// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
use crate::pb::contracts::robonix_system_executor_list_active_plans_client::RobonixSystemExecutorListActivePlansClient;
use crate::pb::executor::{ControlPlanRequest, ControlPlanResponse, ListActivePlansRequest};
use crate::service::VitalsServiceImpl;
use anyhow::{Context, Result, ensure};
use robonix_atlas::{client::AtlasClient, pb::Transport};
use robonix_vitals::component_health::{HEALTH_ERROR, HEALTH_STALE};
use robonix_vitals::health_dependencies::{HealthDependencies, refresh_graph};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, watch};
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_IN_FLIGHT: usize = 8;
const MAX_TRACKED_PLANS: usize = 2048;
const LIST_CONTRACT: &str = "robonix/system/executor/list_active_plans";
const CONTROL_CONTRACT: &str = "robonix/system/executor/control_plan";

/// Control and observation endpoints for the same registered Executor instance.
struct Executor {
    /// Reads accepted nodes, including nodes from an unsealed input stream.
    list: RobonixSystemExecutorListActivePlansClient<Channel>,
    /// Requests plan-scoped cancellation and reports terminal confirmation.
    control: RobonixSystemExecutorControlPlanClient<Channel>,
    /// Atlas allocations retained until the connection is replaced or closed.
    bindings: Vec<String>,
}

/// One matched plan's bounded cancellation retry state.
struct Attempt {
    /// Monotonic time of the last request, used to rate-limit retries.
    last_sent: Option<Instant>,
    /// Whether a control request is currently awaiting its RPC response.
    in_flight: bool,
    /// Whether Executor has confirmed that this plan's owned work terminated.
    completed: bool,
}

/// Keep dependency refresh and cancellation independent of Pilot and model latency.
pub fn spawn(
    atlas: AtlasClient,
    consumer_id: String,
    service: VitalsServiceImpl,
    shutdown: watch::Receiver<bool>,
) {
    let dependencies = service.dependencies();
    let changed = service.snapshot_changed();
    tokio::spawn(refresh_dependencies(
        atlas.clone(),
        consumer_id.clone(),
        Arc::clone(&dependencies),
        Arc::clone(&changed),
        service.clone(),
        shutdown.clone(),
    ));
    tokio::spawn(run(
        atlas,
        consumer_id,
        service,
        dependencies,
        changed,
        shutdown,
    ));
}

/// Refresh control inputs without blocking the fast snapshot-to-cancel loop.
async fn refresh_dependencies(
    mut atlas: AtlasClient,
    consumer_id: String,
    dependencies: Arc<RwLock<HealthDependencies>>,
    changed: Arc<Notify>,
    service: VitalsServiceImpl,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    let mut topology_at = None;
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tick.tick() => {}
        }
        if topology_at.is_none_or(|last: Instant| last.elapsed() >= Duration::from_secs(60)) {
            let body = robonix_vitals::soma_body::fetch_yaml(&mut atlas, &consumer_id).await;
            let mut state = dependencies.write().await;
            match body.and_then(|yaml| state.update_topology(&yaml)) {
                Ok(()) => topology_at = Some(Instant::now()),
                Err(error) => {
                    state.invalidate_topology();
                    log::debug!("[vitals/stop] body bindings unavailable: {error:#}");
                }
            }
        }
        if let Err(error) = refresh_graph(&atlas, &dependencies).await {
            log::warn!(
                "[vitals/stop] dependency refresh failed; using retained evidence: {error:#}"
            );
        }
        service.publish_dependency_report().await;
        changed.notify_one();
    }
}

/// Use one Executor's active table and control capability together, never mixed instances.
async fn connect_executor(atlas: &mut AtlasClient, consumer_id: &str) -> Result<Executor> {
    let capabilities = tokio::time::timeout(
        RPC_TIMEOUT,
        atlas.flatten_capabilities(LIST_CONTRACT, "", Transport::Grpc),
    )
    .await
    .context("Executor discovery timed out")??;
    ensure!(
        capabilities.len() == 1,
        "expected one Executor active table, found {}",
        capabilities.len()
    );
    let provider_id = &capabilities[0].provider_id;
    let mut bindings = Vec::new();
    let result = async {
        let list = bind(
            atlas,
            consumer_id,
            provider_id,
            LIST_CONTRACT,
            &mut bindings,
        )
        .await?;
        let control = bind(
            atlas,
            consumer_id,
            provider_id,
            CONTROL_CONTRACT,
            &mut bindings,
        )
        .await?;
        Ok::<_, anyhow::Error>((list, control))
    }
    .await;
    match result {
        Ok((list, control)) => Ok(Executor {
            list: RobonixSystemExecutorListActivePlansClient::new(list),
            control: RobonixSystemExecutorControlPlanClient::new(control),
            bindings,
        }),
        Err(error) => {
            disconnect(atlas, bindings).await;
            Err(error)
        }
    }
}

/// Record each Atlas allocation before dialing so failed dials can release it.
async fn bind(
    atlas: &mut AtlasClient,
    consumer_id: &str,
    provider_id: &str,
    contract: &str,
    bindings: &mut Vec<String>,
) -> Result<Channel> {
    let (channel_id, endpoint, _) = tokio::time::timeout(
        RPC_TIMEOUT,
        atlas.connect_capability(consumer_id, provider_id, contract, Transport::Grpc),
    )
    .await
    .context("Executor capability binding timed out")??;
    bindings.push(channel_id);
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };
    Ok(Endpoint::new(endpoint)?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect()
        .await?)
}

/// Bound Atlas cleanup so reconnecting control cannot wait forever on bookkeeping.
async fn disconnect(atlas: &mut AtlasClient, bindings: Vec<String>) {
    for binding in bindings {
        match tokio::time::timeout(RPC_TIMEOUT, atlas.disconnect_capability(&binding)).await {
            Ok(Ok(_)) => {}
            result => log::warn!("[vitals/stop] binding cleanup failed for {binding}: {result:?}"),
        }
    }
}

/// Keep stop requests pending across recovery; recovery never restarts a plan.
async fn run(
    mut atlas: AtlasClient,
    consumer_id: String,
    service: VitalsServiceImpl,
    dependencies: Arc<RwLock<HealthDependencies>>,
    changed: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut executor = None;
    let mut attempts: BTreeMap<String, Attempt> = BTreeMap::new();
    let mut tasks: JoinSet<(String, Result<ControlPlanResponse>)> = JoinSet::new();
    let mut connect_at = Instant::now() - RETRY_INTERVAL;
    let mut incomplete = false;
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Ok((plan_id, result)) => record_result(&mut attempts, &plan_id, result),
                    Err(error) => log::error!("[vitals/stop] cancellation worker failed: {error}"),
                }
                continue;
            }
            _ = changed.notified() => {}
            _ = tick.tick() => {}
        }
        let snapshot = service.latest_snapshot().await;
        dependencies.write().await.observe_health(&snapshot);
        if !snapshot
            .components
            .iter()
            .any(|component| matches!(component.health, HEALTH_ERROR | HEALTH_STALE))
            && attempts.is_empty()
        {
            continue;
        }
        if executor.is_none() {
            if connect_at.elapsed() < RETRY_INTERVAL {
                continue;
            }
            connect_at = Instant::now();
            match connect_executor(&mut atlas, &consumer_id).await {
                Ok(connected) => executor = Some(connected),
                Err(error) => {
                    log::warn!(
                        "[vitals/stop] cannot inspect/control Executor; stop unconfirmed: {error:#}"
                    );
                    continue;
                }
            }
        }
        let connected = executor.as_mut().unwrap();
        let plans = tokio::time::timeout(
            RPC_TIMEOUT,
            connected.list.list_active_plans(ListActivePlansRequest {}),
        )
        .await;
        let raw = match plans {
            Ok(Ok(response)) if response.get_ref().success => response.into_inner().plans_json,
            result => {
                service.update_active_plans(None).await;
                log::warn!("[vitals/stop] active-plan query failed; stop unconfirmed: {result:?}");
                let stale = executor.take().unwrap();
                disconnect(&mut atlas, stale.bindings).await;
                continue;
            }
        };
        service.update_active_plans(Some(raw.clone())).await;
        service.publish_dependency_report().await;
        let targets = dependencies.read().await.stop_targets(&snapshot, &raw);
        if targets.incomplete && !incomplete {
            log::warn!(
                "[vitals/stop] dependency coverage is incomplete; stopping only confirmed matches"
            );
        }
        incomplete = targets.incomplete;
        let active = match active_ids(&raw) {
            Ok(active) => active,
            Err(error) => {
                log::warn!("[vitals/stop] cannot verify active plans: {error:#}");
                continue;
            }
        };
        attempts.retain(|id, attempt| !attempt.completed || active.contains(id));
        for plan_id in targets.plan_ids {
            if attempts.len() >= MAX_TRACKED_PLANS && !attempts.contains_key(&plan_id) {
                log::error!(
                    "[vitals/stop] pending-control limit reached; cannot track plan={plan_id}; stop unconfirmed"
                );
                continue;
            }
            attempts.entry(plan_id).or_insert(Attempt {
                last_sent: None,
                in_flight: false,
                completed: false,
            });
        }
        let mut ordered: Vec<_> = attempts.iter_mut().collect();
        ordered.sort_by_key(|(_, attempt)| attempt.last_sent);
        for (plan_id, attempt) in ordered {
            if tasks.len() >= MAX_IN_FLIGHT {
                break;
            }
            if attempt.in_flight
                || attempt.completed
                || attempt
                    .last_sent
                    .is_some_and(|last| last.elapsed() < RETRY_INTERVAL)
            {
                continue;
            }
            attempt.in_flight = true;
            attempt.last_sent = Some(Instant::now());
            let plan_id = plan_id.clone();
            let client = connected.control.clone();
            log::warn!("[vitals/stop] requesting targeted cancel plan={plan_id}");
            tasks.spawn(async move {
                let result = cancel(client, &plan_id).await;
                (plan_id, result)
            });
        }
    }
    tasks.shutdown().await;
    if let Some(executor) = executor {
        disconnect(&mut atlas, executor.bindings).await;
    }
}

/// Parse the authoritative table without mistaking a malformed response for an empty table.
fn active_ids(raw: &str) -> Result<BTreeSet<String>> {
    ensure!(
        raw.len() <= 2 * 1024 * 1024,
        "Executor table exceeds size limit"
    );
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let plans = value
        .get("plans")
        .and_then(serde_json::Value::as_array)
        .context("Executor response has no plans array")?;
    ensure!(
        plans.len() <= MAX_TRACKED_PLANS,
        "Executor table exceeds plan limit"
    );
    plans
        .iter()
        .map(|plan| {
            plan.get("plan_id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty() && id.len() <= 512)
                .map(str::to_owned)
                .context("Executor returned an invalid plan id")
        })
        .collect()
}

/// Completion concerns Executor work; remote cancellation errors remain visible.
async fn cancel(
    mut client: RobonixSystemExecutorControlPlanClient<Channel>,
    plan_id: &str,
) -> Result<ControlPlanResponse> {
    Ok(tokio::time::timeout(
        RPC_TIMEOUT,
        client.control_plan(ControlPlanRequest {
            action: "cancel".into(),
            plan_id: plan_id.into(),
            wait_ms: 500,
        }),
    )
    .await
    .context("Executor cancel timed out")??
    .into_inner())
}

/// Retry rejected or pending controls without claiming that a device physically stopped.
fn record_result(
    attempts: &mut BTreeMap<String, Attempt>,
    plan_id: &str,
    result: Result<ControlPlanResponse>,
) {
    let Some(attempt) = attempts.get_mut(plan_id) else {
        return;
    };
    attempt.in_flight = false;
    match result {
        Ok(response) if response.success && response.completed && response.error.is_empty() => {
            attempt.completed = true;
            log::warn!(
                "[vitals/stop] Executor cancellation completed plan={plan_id}: {}",
                response.message
            );
        }
        Ok(response) => log::warn!(
            "[vitals/stop] stop unconfirmed plan={plan_id}, accepted={}, completed={}, error={}, message={}",
            response.success,
            response.completed,
            response.error,
            response.message,
        ),
        Err(error) => log::error!("[vitals/stop] stop unconfirmed plan={plan_id}: {error:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::contracts::robonix_system_executor_control_plan_server::{
        RobonixSystemExecutorControlPlan, RobonixSystemExecutorControlPlanServer,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::{Request, Response, Status};

    /// Executor fixture that acknowledges cancellation before confirming completion.
    #[derive(Clone)]
    struct PendingControl {
        /// Counts real wire requests so the second attempt can report completion.
        requests: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl RobonixSystemExecutorControlPlan for PendingControl {
        async fn control_plan(
            &self,
            request: Request<ControlPlanRequest>,
        ) -> Result<Response<ControlPlanResponse>, Status> {
            let request = request.into_inner();
            assert_eq!(request.action, "cancel");
            assert_eq!(request.plan_id, "streaming-plan");
            assert_eq!(request.wait_ms, 500);
            let completed = self.requests.fetch_add(1, Ordering::SeqCst) > 0;
            Ok(Response::new(ControlPlanResponse {
                success: true,
                completed,
                message: if completed {
                    "terminated"
                } else {
                    "cancellation pending"
                }
                .into(),
                error: String::new(),
            }))
        }
    }

    #[tokio::test]
    async fn simplified_control_rpc_retries_accepted_but_unconfirmed_stops() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connections, incoming) = tokio::sync::mpsc::channel(4);
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                if connections
                    .send(Ok::<_, std::io::Error>(stream))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let requests = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RobonixSystemExecutorControlPlanServer::new(
                    PendingControl {
                        requests: Arc::clone(&requests),
                    },
                ))
                .serve_with_incoming(tokio_stream::wrappers::ReceiverStream::new(incoming)),
        );
        let client = RobonixSystemExecutorControlPlanClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let mut attempts = BTreeMap::from([(
            "streaming-plan".into(),
            Attempt {
                last_sent: None,
                in_flight: true,
                completed: false,
            },
        )]);
        record_result(
            &mut attempts,
            "streaming-plan",
            cancel(client.clone(), "streaming-plan").await,
        );
        assert!(!attempts["streaming-plan"].completed);
        assert!(!attempts["streaming-plan"].in_flight);
        attempts.get_mut("streaming-plan").unwrap().in_flight = true;
        record_result(
            &mut attempts,
            "streaming-plan",
            cancel(client, "streaming-plan").await,
        );
        assert!(attempts["streaming-plan"].completed);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
        accept.abort();
    }

    #[test]
    /// Invalid tables cannot release pending controls as though every plan finished.
    fn rejects_invalid_active_table() {
        assert!(active_ids("{}").is_err());
        assert!(active_ids(r#"{"plans":[{}]}"#).is_err());
        assert!(active_ids(r#"{"plans":[]}"#).unwrap().is_empty());
    }

    #[test]
    /// A local completion with remote errors must still be reported as unconfirmed.
    fn failed_remote_cancel_is_not_confirmed() {
        let mut attempts = BTreeMap::from([(
            "affected".into(),
            Attempt {
                last_sent: None,
                in_flight: true,
                completed: false,
            },
        )]);
        record_result(
            &mut attempts,
            "affected",
            Ok(ControlPlanResponse {
                success: false,
                completed: true,
                error: "remote cancel failed".into(),
                ..Default::default()
            }),
        );
        assert!(!attempts["affected"].completed);
        assert!(!attempts["affected"].in_flight);
        record_result(
            &mut attempts,
            "affected",
            Ok(ControlPlanResponse {
                success: true,
                completed: true,
                ..Default::default()
            }),
        );
        assert!(attempts["affected"].completed);
    }
}
