// SPDX-License-Identifier: MulanPSL-2.0
//! Active RTDL plans: registration, state snapshots, async calls, and cancellation.
//! Builtin query/cancel adapters remain here for the unchanged dispatch module.

use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, CapabilityCallResult, Plan, RtdlNode};
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use robonix_scribe::warn;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

const DEFAULT_CANCEL_WAIT_MS: u64 = 5_000;
const ASYNC_CANCEL_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ACTIVE_PLANS: usize = 256;

/// Shared active-plan state used for progress queries and cancellation.
#[derive(Clone, Default)]
pub struct PlanRuntime {
    /// Active executions keyed by plan ID, shared across RPC handlers and tasks.
    inner: Arc<Mutex<HashMap<String, PlanRun>>>,
    /// Wakes waiters when cancellation is requested or a completed plan is removed.
    changed: Arc<tokio::sync::Notify>,
}

/// Identifies an in-flight asynchronous capability call so it can be canceled.
#[derive(Clone)]
pub struct RunningAsyncCall {
    /// Original capability call ID used to track the call within its plan.
    pub call_id: String,
    /// Provider responsible for the asynchronous execution.
    pub provider_id: String,
    /// Contract ID used to request cancellation from the provider.
    pub cancel_contract: String,
    /// Provider-issued execution ID sent to the cancel contract.
    pub run_id: String,
}

/// Mutable progress and cancellation state retained for one active plan.
struct PlanRun {
    /// Durable cancellation signal observed by dispatches and the streaming scheduler.
    cancellation: watch::Sender<bool>,
    /// Calls still available for cancellation, keyed by capability call ID.
    running_async: HashMap<String, RunningAsyncCall>,
    /// Call IDs whose exact-run cancellation and terminal confirmation are in progress.
    cancelling: HashSet<String>,
    /// Remote failures retained with their owned calls until a later retry succeeds.
    cancel_errors: HashMap<String, String>,
    /// Whether input, dispatch tasks, and verifiers have finished draining locally.
    local_complete: bool,
    /// Plan-level description (the root node's description = the overall task),
    /// so `get_all_plans` can tell the LLM what each running plan is.
    description: String,
    /// Static op list snapshotted at register time (arena order), so
    /// `get_plan_status` can report op_ids/descriptions the LLM can target.
    ops: Vec<OpMeta>,
    /// op_id → latest RTDL node state observed during execution. Absent = the
    /// op has not started yet (reported as `pending`).
    op_state: HashMap<String, u32>,
}

/// One op's static identity, snapshotted from the plan arena at register time.
#[derive(Clone)]
struct OpMeta {
    /// Capability call identity connecting this node to an unconfirmed remote run.
    call_id: String,
    /// Stable node identity used in progress queries and state updates.
    op_id: String,
    /// Human-readable task performed by this node.
    description: String,
    /// RTDL node kind distinguishing sequence, parallel, and capability calls.
    node_kind: u32,
    /// Provider targeted by a capability call; empty for nodes without a call.
    provider_id: String,
    /// Contract targeted by a capability call; empty for nodes without a call.
    contract_id: String,
}

impl From<&RtdlNode> for OpMeta {
    fn from(node: &RtdlNode) -> Self {
        Self {
            call_id: node
                .call
                .as_ref()
                .map(|call| call.call_id.clone())
                .unwrap_or_default(),
            op_id: node.op_id.clone(),
            description: node.description.clone(),
            node_kind: node.node_kind,
            provider_id: node
                .call
                .as_ref()
                .map(|call| call.provider_id.clone())
                .unwrap_or_default(),
            contract_id: node
                .call
                .as_ref()
                .map(|call| call.contract_id.clone())
                .unwrap_or_default(),
        }
    }
}

/// Local drain and exact-run terminal confirmation reported by a cancellation control.
pub(crate) struct CancelOutcome {
    /// Whether local work drained and all owned asynchronous runs reached terminal state.
    pub(crate) completed: bool,
    /// Human-readable scope and completion information for the control response.
    pub(crate) message: String,
    /// Provider cancellation failures; an empty value does not imply completion.
    pub(crate) error: String,
}

/// JSON arguments selecting a plan to cancel and how long to await completion.
#[derive(Deserialize)]
struct CancelPlanArgs {
    /// ID of the RTDL plan to cancel.
    plan_id: String,
    /// Optional completion wait in milliseconds; omission uses the default wait.
    wait_ms: Option<u64>,
}

impl PlanRuntime {
    /// Reserve a plan for either execution RPC without replacing an active execution.
    pub async fn register_plan(&self, plan_id: &str) -> Result<(), String> {
        let mut plans = self.inner.lock().await;
        if plans.contains_key(plan_id) {
            return Err(format!("plan {plan_id} is already active"));
        }
        if plans.len() >= MAX_ACTIVE_PLANS {
            return Err("active plan limit reached; resolve outstanding cancellations".into());
        }
        plans.insert(
            plan_id.into(),
            PlanRun {
                cancellation: watch::channel(false).0,
                running_async: HashMap::new(),
                cancelling: HashSet::new(),
                cancel_errors: HashMap::new(),
                local_complete: false,
                description: String::new(),
                ops: Vec::new(),
                op_state: HashMap::new(),
            },
        );
        Ok(())
    }

    /// Append metadata atomically with the cancellation check; the first node is the root.
    pub async fn record_stream_node(&self, plan_id: &str, node: &RtdlNode) -> bool {
        let mut plans = self.inner.lock().await;
        let Some(run) = plans.get_mut(plan_id) else {
            return false;
        };
        if *run.cancellation.borrow() {
            return false;
        }
        if run.ops.is_empty() {
            run.description = node.description.clone();
        }
        run.ops.push(OpMeta::from(node));
        true
    }

    /// Snapshot a plan's op list (arena order) so `get_plan_status` can report
    /// the op_ids/descriptions the LLM may target. Called right after
    /// `register_plan`. No-op if the plan is not in the active table.
    pub async fn record_plan_ops(&self, plan: &Plan) {
        let ops: Vec<OpMeta> = plan.nodes.iter().map(OpMeta::from).collect();
        let description = plan
            .nodes
            .get(plan.root_index as usize)
            .map(|n| n.description.clone())
            .unwrap_or_default();
        if let Some(run) = self.inner.lock().await.get_mut(&plan.plan_id) {
            run.ops = ops;
            run.description = description;
        }
    }

    /// Record the latest observed RTDL state for an op. Called by the execution
    /// loop / async poller alongside every node_state event it streams, so
    /// `get_plan_status` reflects live progress. No-op for unknown plans.
    pub async fn record_op_state(&self, plan_id: &str, op_id: &str, state: u32) {
        if let Some(run) = self.inner.lock().await.get_mut(plan_id) {
            run.op_state.insert(op_id.to_string(), state);
        }
    }

    /// Apply the `get_plan_status` builtin: return the plan's ops as JSON, each
    /// with `op_id`, `kind`, `description`, and current `state`.
    /// Errors when the plan is not in the active table (stale/wrong id, or
    /// already finished) — it is a query,
    /// so "not found" is an error; use `get_all_plans` to check liveness.
    pub async fn get_plan_status_builtin(&self, call: &CapabilityCall) -> CapabilityCallResult {
        /// JSON arguments selecting the active plan whose progress is requested.
        #[derive(Deserialize)]
        struct Args {
            /// ID of the active RTDL plan to inspect.
            plan_id: String,
        }
        let args: Args = match serde_json::from_str(&call.args_json) {
            Ok(a) => a,
            Err(e) => return error_result(call, format!("invalid get_plan_status args: {e}")),
        };
        let plans = self.inner.lock().await;
        let Some(run) = plans.get(&args.plan_id) else {
            // A query for a specific plan that is not in the active table is an
            // error (the id is stale or wrong) — unlike cancel
            // commands, which are idempotent "ensure stopped" no-ops. Use
            // get_all_plans to check whether a plan is still running.
            return error_result(
                call,
                format!(
                    "get_plan_status: no active RTDL plan '{}' (it never existed or already finished/cancelled); call get_all_plans to list running plans",
                    args.plan_id
                ),
            );
        };
        let ops: Vec<serde_json::Value> = run
            .ops
            .iter()
            .map(|op| {
                let state = observed_state(run, op);
                serde_json::json!({
                    "op_id": op.op_id,
                    "kind": kind_label(op.node_kind),
                    "description": op.description,
                    "provider_id": op.provider_id,
                    "contract_id": op.contract_id,
                    "state": state,
                })
            })
            .collect();
        ok_result(
            call,
            serde_json::json!({
                "plan_id": args.plan_id,
                "running": true,
                "cancelled": *run.cancellation.borrow(),
                "ops": ops,
            })
            .to_string(),
        )
    }

    /// Apply the `get_all_plans` builtin: list every active RTDL plan with its
    /// op count and cancelled flag. Lets the LLM discover which plans are
    /// running before inspecting one with `get_plan_status`. Takes no args.
    pub async fn get_all_plans_builtin(&self, call: &CapabilityCall) -> CapabilityCallResult {
        ok_result(call, self.active_plans_json().await)
    }

    /// Return Executor's authoritative active-plan table for control-plane
    /// observers. Reading this snapshot does not register a query plan.
    pub async fn active_plans_json(&self) -> String {
        let plans = self.inner.lock().await;
        let mut entries: Vec<(&String, &PlanRun)> = plans.iter().collect();
        entries.sort_by_key(|(id, _)| id.parse::<u64>().unwrap_or(u64::MAX));
        let list: Vec<serde_json::Value> = entries
            .iter()
            .map(|(id, run)| {
                serde_json::json!({
                    "plan_id": id,
                    "description": run.description,
                    "op_count": run.ops.len(),
                    "cancelled": *run.cancellation.borrow(),
                    "ops": run.ops.iter().map(|op| serde_json::json!({
                        "op_id": op.op_id,
                        "kind": kind_label(op.node_kind),
                        "description": op.description,
                        "provider_id": op.provider_id,
                        "contract_id": op.contract_id,
                        "state": observed_state(run, op),
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::json!({ "count": list.len(), "plans": list }).to_string()
    }

    /// Finish local scheduling without losing ownership of an unconfirmed remote run.
    pub async fn complete_plan(&self, plan_id: &str) -> bool {
        let mut plans = self.inner.lock().await;
        let cancelled = if let Some(run) = plans.get_mut(plan_id) {
            run.local_complete = true;
            *run.cancellation.borrow()
        } else {
            false
        };
        remove_finished_plan(&mut plans, plan_id);
        self.changed.notify_waiters();
        cancelled
    }

    /// Return whether a registered plan has been cancelled.
    pub async fn is_cancelled(&self, plan_id: &str) -> bool {
        self.inner
            .lock()
            .await
            .get(plan_id)
            .is_some_and(|run| *run.cancellation.borrow())
    }

    /// Observe cancellation even if it was requested before this waiter subscribed.
    pub async fn wait_for_cancellation(&self, plan_id: &str) {
        let receiver = self
            .inner
            .lock()
            .await
            .get(plan_id)
            .map(|run| run.cancellation.subscribe());
        match receiver {
            Some(mut receiver) => {
                let _ = receiver.wait_for(|cancelled| *cancelled).await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    /// Retain the initial response's exact run, including a response racing cancellation.
    pub async fn register_or_cancel_async_call(
        &self,
        plan_id: &str,
        call: RunningAsyncCall,
        self_provider_id: &str,
        atlas: &mut AtlasClient,
    ) -> bool {
        let cancel = {
            let mut plans = self.inner.lock().await;
            let Some(run) = plans.get_mut(plan_id) else {
                warn!("[executor] async run arrived without its owning plan {plan_id}");
                return false;
            };
            let missing_id = call.run_id.trim().is_empty();
            run.running_async.insert(call.call_id.clone(), call);
            if missing_id {
                run.cancellation.send_replace(true);
            }
            *run.cancellation.borrow()
        };
        if cancel {
            self.cancel_plan_control(plan_id, 0, self_provider_id, atlas)
                .await;
        }
        !cancel
    }

    /// Remove an async call once it reaches a terminal state.
    pub async fn unregister_async_call(&self, plan_id: &str, call_id: &str) {
        if let Some(run) = self.inner.lock().await.get_mut(plan_id)
            && !run.cancelling.contains(call_id)
        {
            run.running_async.remove(call_id);
            run.cancel_errors.remove(call_id);
        }
    }

    /// Seal execution once without turning scheduler cleanup into a cancellation retry.
    pub(crate) async fn cancel_execution(
        &self,
        plan_id: &str,
        consumer_id: &str,
        atlas: &AtlasClient,
    ) {
        let calls = {
            let mut plans = self.inner.lock().await;
            let Some(run) = plans.get_mut(plan_id) else {
                return;
            };
            if *run.cancellation.borrow() {
                return;
            }
            claim_cancels(run)
        };
        self.launch_cancels(plan_id, calls, consumer_id, atlas);
        self.changed.notify_waiters();
    }

    /// Apply the `cancel_plan` builtin.
    ///
    /// Synchronous dispatch stops locally; asynchronous completion additionally
    /// requires terminal status for every owned run.
    pub async fn cancel_plan_builtin(
        &self,
        call: &CapabilityCall,
        self_provider_id: &str,
        atlas: &mut AtlasClient,
    ) -> CapabilityCallResult {
        let args: CancelPlanArgs = match serde_json::from_str(&call.args_json) {
            Ok(args) => args,
            Err(e) => return error_result(call, format!("invalid cancel_plan args: {e}")),
        };
        let outcome = self
            .cancel_plan_control(
                &args.plan_id,
                args.wait_ms.unwrap_or(DEFAULT_CANCEL_WAIT_MS),
                self_provider_id,
                atlas,
            )
            .await;
        CapabilityCallResult {
            call_id: call.call_id.clone(),
            provider_id: call.provider_id.clone(),
            contract_id: call.contract_id.clone(),
            success: outcome.error.is_empty(),
            output: outcome.message,
            error: outcome.error,
        }
    }

    /// Out-of-band target cancellation used by Pilot meta ops and the legacy
    /// builtin wrapper. Returns whether the plan reached terminal state before
    /// the deadline plus a stable, idempotent message.
    pub async fn cancel_plan_control(
        &self,
        plan_id: &str,
        wait_ms: u64,
        self_provider_id: &str,
        atlas: &mut AtlasClient,
    ) -> CancelOutcome {
        let snapshot = match self.begin_cancel(plan_id).await {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return CancelOutcome {
                    completed: true,
                    message: format!(
                        "RTDL plan '{plan_id}' is not running (already finished or cancelled); nothing to cancel."
                    ),
                    error: String::new(),
                };
            }
        };
        let attempts = snapshot.len();
        self.launch_cancels(plan_id, snapshot, self_provider_id, atlas);
        let (completed, error) = self
            .wait_until_all_inactive(&[plan_id.to_string()], wait_ms)
            .await;
        CancelOutcome {
            completed,
            message: format!(
                "Cancellation requested for RTDL plan '{plan_id}'; completed={completed}, async_cancel_attempts={attempts}. Synchronous calls stop locally only; physical stop is unconfirmed."
            ),
            error,
        }
    }

    /// Cancel every active plan and make best-effort cancel calls for running async work.
    pub async fn cancel_all_plans(&self, self_provider_id: &str, atlas: &mut AtlasClient) -> bool {
        let (_, outcome) = self
            .cancel_all_plans_except(self_provider_id, atlas, None, DEFAULT_CANCEL_WAIT_MS)
            .await;
        outcome.completed
    }

    /// Cancel every active plan except an optional control plan and wait for
    /// the targets to leave the active table.
    pub async fn cancel_all_plans_except(
        &self,
        self_provider_id: &str,
        atlas: &mut AtlasClient,
        except_plan_id: Option<&str>,
        wait_ms: u64,
    ) -> (usize, CancelOutcome) {
        let snapshot = self.begin_cancel_all(except_plan_id).await;
        let plan_ids: Vec<_> = snapshot.iter().map(|(id, _)| id.clone()).collect();
        let target_count = plan_ids.len();
        for (plan_id, calls) in snapshot {
            self.launch_cancels(&plan_id, calls, self_provider_id, atlas);
        }
        let (completed, error) = self.wait_until_all_inactive(&plan_ids, wait_ms).await;
        (
            target_count,
            CancelOutcome {
                completed,
                message: format!(
                    "Cancellation requested for all RTDL plans; target_count={target_count}, completed={completed}. Synchronous calls stop locally only; physical stop is unconfirmed."
                ),
                error,
            },
        )
    }

    /// Mark a plan cancelled and return the state needed by `cancel_plan`.
    ///
    /// This pure runtime step is separated from atlas/MCP cancel calls so tests
    /// can cover unknown-plan and cancellation-state behavior without a live
    /// atlas server.
    async fn begin_cancel(&self, plan_id: &str) -> Result<Vec<RunningAsyncCall>, String> {
        let mut plans = self.inner.lock().await;
        let Some(run) = plans.get_mut(plan_id) else {
            return Err(format!("unknown plan_id '{plan_id}'"));
        };
        let calls = claim_cancels(run);
        self.changed.notify_waiters();
        Ok(calls)
    }

    /// Claim asynchronous cancellations atomically across all selected plans.
    async fn begin_cancel_all(
        &self,
        except_plan_id: Option<&str>,
    ) -> Vec<(String, Vec<RunningAsyncCall>)> {
        let mut plans = self.inner.lock().await;
        let mut snapshots = Vec::new();
        for (plan_id, run) in plans.iter_mut() {
            if except_plan_id == Some(plan_id.as_str()) {
                continue;
            }
            snapshots.push((plan_id.clone(), claim_cancels(run)));
        }
        self.changed.notify_waiters();
        snapshots
    }

    /// Independent owned tasks survive a disconnected control RPC and retain failed handles.
    fn launch_cancels(
        &self,
        plan_id: &str,
        calls: Vec<RunningAsyncCall>,
        consumer_id: &str,
        atlas: &AtlasClient,
    ) {
        for running in calls {
            let runtime = self.clone();
            let plan_id = plan_id.to_string();
            let consumer_id = consumer_id.to_string();
            let mut atlas = atlas.clone();
            tokio::spawn(async move {
                let result = cancel_async_call(&consumer_id, &running, &mut atlas).await;
                let mut plans = runtime.inner.lock().await;
                if let Some(run) = plans.get_mut(&plan_id) {
                    run.cancelling.remove(&running.call_id);
                    match result {
                        Ok(()) => {
                            run.running_async.remove(&running.call_id);
                            run.cancel_errors.remove(&running.call_id);
                        }
                        Err(error) => {
                            let error = format!(
                                "{}: {error:#}; physical stop is unconfirmed",
                                running.call_id
                            );
                            warn!("[executor] {error}");
                            run.cancel_errors.insert(running.call_id.clone(), error);
                        }
                    }
                }
                remove_finished_plan(&mut plans, &plan_id);
                runtime.changed.notify_waiters();
            });
        }
    }

    /// Register notification before reading durable state to avoid lost wakeups.
    async fn wait_until_all_inactive(&self, plan_ids: &[String], wait_ms: u64) -> (bool, String) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (completed, error) = {
                let plans = self.inner.lock().await;
                let errors: Vec<_> = plan_ids
                    .iter()
                    .filter_map(|id| plans.get(id))
                    .flat_map(|run| run.cancel_errors.values().cloned())
                    .collect();
                (
                    plan_ids.iter().all(|id| !plans.contains_key(id)),
                    errors.join("; "),
                )
            };
            if completed || !error.is_empty() || tokio::time::Instant::now() >= deadline {
                return (completed, error);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return (false, error);
            }
        }
    }
}

/// Keep unconfirmed owned calls visible to dependency matching after local cancellation.
fn observed_state(run: &PlanRun, op: &OpMeta) -> &'static str {
    if *run.cancellation.borrow() && run.running_async.contains_key(&op.call_id) {
        "running"
    } else {
        run.op_state
            .get(&op.op_id)
            .map(|state| state_label(*state))
            .unwrap_or("pending")
    }
}

/// Claim work atomically without removing the only copy of a provider's run ID.
fn claim_cancels(run: &mut PlanRun) -> Vec<RunningAsyncCall> {
    run.cancellation.send_replace(true);
    run.running_async
        .values()
        .filter_map(|call| {
            if !run.cancelling.insert(call.call_id.clone()) {
                return None;
            }
            run.cancel_errors.remove(&call.call_id);
            Some(call.clone())
        })
        .collect()
}

/// Remove active state only after both local drain and remote terminal confirmation.
fn remove_finished_plan(plans: &mut HashMap<String, PlanRun>, plan_id: &str) {
    if plans.get(plan_id).is_some_and(|run| {
        run.local_complete
            && run.running_async.is_empty()
            && run.cancelling.is_empty()
            && run.cancel_errors.is_empty()
    }) {
        plans.remove(plan_id);
    }
}

/// Call a provider's async cancel contract through atlas/MCP.
///
/// Cancellation acceptance alone cannot release ownership; poll the exact run
/// to a valid terminal state within the same bounded deadline.
async fn cancel_async_call(
    consumer_id: &str,
    running: &RunningAsyncCall,
    atlas: &mut AtlasClient,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !running.run_id.trim().is_empty(),
        "async initial response has no run_id; refusing unscoped cancellation"
    );
    let args = serde_json::json!({ "run_id": running.run_id }).to_string();
    let cancel_call = CapabilityCall {
        call_id: format!("cancel-{}", uuid::Uuid::new_v4()),
        provider_id: running.provider_id.clone(),
        contract_id: running.cancel_contract.clone(),
        args_json: args,
    };
    let deadline = tokio::time::Instant::now() + ASYNC_CANCEL_TIMEOUT;
    let (channel_id, endpoint, _) = tokio::time::timeout_at(
        deadline,
        atlas.connect_capability(
            consumer_id,
            &running.provider_id,
            &running.cancel_contract,
            atlas_pb::Transport::Mcp,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("async cancel discovery timed out"))??;
    let channel = crate::dispatch::CapabilityChannel::new(atlas, channel_id);
    let result = tokio::time::timeout_at(
        deadline,
        crate::dispatch::mcp::execute(&cancel_call, &endpoint),
    )
    .await;
    let result = result.map_err(|_| anyhow::anyhow!("async cancel RPC timed out"))?;
    let cancel_error = if !result.success {
        Some(result.error)
    } else {
        serde_json::from_str::<serde_json::Value>(&result.output)
            .ok()
            .filter(|payload| {
                ["accepted", "ok", "success"].iter().any(|field| {
                    payload.get(*field).and_then(serde_json::Value::as_bool) == Some(false)
                })
            })
            .map(|_| format!("async cancel rejected: {}", result.output))
    };
    let contract = running
        .cancel_contract
        .strip_suffix("/cancel")
        .ok_or_else(|| anyhow::anyhow!("async cancel has no status counterpart"))?;
    let mut last_state = None;
    let confirmation = tokio::time::timeout_at(deadline, async {
        channel.release().await;
        loop {
            let output = crate::dispatch::async_poll::poll_status(
                consumer_id,
                &running.provider_id,
                &format!("{contract}/status"),
                &running.run_id,
                atlas,
            )
            .await?;
            let (state, _) =
                crate::dispatch::async_poll::parse_run_status(&output, &running.run_id)?;
            last_state = Some(state);
            if crate::rtdl_events::is_terminal_state(state) {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "async cancel status confirmation timed out; last status={}",
            last_state.map(state_label).unwrap_or("unavailable")
        ))
    });
    match (cancel_error, confirmation) {
        (_, Ok(())) => Ok(()),
        (Some(cancel_error), Err(status_error)) => {
            anyhow::bail!("{cancel_error}; status confirmation failed: {status_error:#}")
        }
        (None, Err(status_error)) => Err(status_error),
    }
}

/// Build a failed capability result for a builtin.
fn error_result(call: &CapabilityCall, error: String) -> CapabilityCallResult {
    CapabilityCallResult {
        call_id: call.call_id.clone(),
        provider_id: call.provider_id.clone(),
        contract_id: call.contract_id.clone(),
        success: false,
        output: String::new(),
        error,
    }
}

/// Human-readable label for an RTDL node state (for `get_plan_status` JSON).
fn state_label(state: u32) -> &'static str {
    match RtdlNodeStateEnum::try_from(state as i32) {
        Ok(RtdlNodeStateEnum::Pending) => "pending",
        Ok(RtdlNodeStateEnum::Running) => "running",
        Ok(RtdlNodeStateEnum::Succeeded) => "succeeded",
        Ok(RtdlNodeStateEnum::Failed) => "failed",
        Ok(RtdlNodeStateEnum::Canceled) => "canceled",
        Ok(RtdlNodeStateEnum::Timeout) => "timeout",
        Ok(RtdlNodeStateEnum::Paused) => "paused",
        Ok(RtdlNodeStateEnum::Verifying) => "verifying",
        _ => "unknown",
    }
}

/// Label an RTDL node kind (0=sequence, 1=parallel, 2=do).
fn kind_label(node_kind: u32) -> &'static str {
    match node_kind {
        0 => "sequence",
        1 => "parallel",
        2 => "do",
        _ => "unknown",
    }
}

/// Build a successful capability result for a builtin.
fn ok_result(call: &CapabilityCall, output: String) -> CapabilityCallResult {
    CapabilityCallResult {
        call_id: call.call_id.clone(),
        provider_id: call.provider_id.clone(),
        contract_id: call.contract_id.clone(),
        success: true,
        output,
        error: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_cancel_unknown_plan_fails() {
        let runtime = PlanRuntime::default();
        let err = match runtime.begin_cancel("missing").await {
            Ok(_) => panic!("missing plan should not cancel"),
            Err(err) => err,
        };
        assert!(err.contains("unknown plan_id"));
    }

    #[tokio::test]
    async fn registered_plan_starts_uncancelled_then_flips() {
        let runtime = PlanRuntime::default();
        runtime.register_plan("p").await.unwrap();
        assert!(!runtime.is_cancelled("p").await);

        runtime.begin_cancel("p").await.unwrap();

        assert!(runtime.is_cancelled("p").await);
    }

    #[tokio::test]
    async fn begin_cancel_retains_owned_calls_and_deduplicates_inflight_attempts() {
        let runtime = PlanRuntime::default();
        runtime.register_plan("p").await.unwrap();
        {
            let mut plans = runtime.inner.lock().await;
            plans
                .get_mut("p")
                .unwrap()
                .running_async
                .insert("c".into(), running_call("c"));
        }

        let snapshot = runtime.begin_cancel("p").await.unwrap();

        assert_eq!(snapshot.len(), 1);
        assert!(runtime.begin_cancel("p").await.unwrap().is_empty());
        let plans = runtime.inner.lock().await;
        assert_eq!(plans.get("p").unwrap().running_async.len(), 1);
    }

    #[tokio::test]
    async fn begin_cancel_all_marks_every_plan() {
        let runtime = PlanRuntime::default();
        runtime.register_plan("p1").await.unwrap();
        runtime.register_plan("p2").await.unwrap();
        let snapshot = runtime.begin_cancel_all(None).await;

        assert!(snapshot.iter().all(|(_, calls)| calls.is_empty()));
        assert!(runtime.is_cancelled("p1").await);
        assert!(runtime.is_cancelled("p2").await);
    }

    #[tokio::test]
    async fn begin_cancel_all_excludes_control_plan() {
        let runtime = PlanRuntime::default();
        runtime.register_plan("task").await.unwrap();
        runtime.register_plan("control").await.unwrap();

        let snapshot = runtime.begin_cancel_all(Some("control")).await;

        assert_eq!(
            snapshot
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["task"]
        );
        assert!(runtime.is_cancelled("task").await);
        assert!(!runtime.is_cancelled("control").await);
    }

    fn running_call(call_id: &str) -> RunningAsyncCall {
        RunningAsyncCall {
            call_id: call_id.into(),
            provider_id: "provider".into(),
            cancel_contract: "robonix/test/cancel".into(),
            run_id: "r".into(),
        }
    }

    fn status_call(args_json: &str) -> CapabilityCall {
        CapabilityCall {
            call_id: "c".into(),
            provider_id: "executor".into(),
            contract_id: "robonix/system/executor/builtin/get_plan_status".into(),
            args_json: args_json.into(),
        }
    }

    /// Status queries retain node identity and progress after cancellation controls are simplified.
    #[tokio::test]
    async fn get_plan_status_reports_ops_states() {
        use crate::pb::pilot::{Plan, RtdlNode};
        let runtime = PlanRuntime::default();
        runtime.register_plan("p").await.unwrap();
        let plan = Plan {
            plan_id: "p".into(),
            session_id: "s".into(),
            round: 0,
            root_index: 0,
            nodes: vec![
                RtdlNode {
                    node_kind: 0,
                    children: vec![1],
                    call: None,
                    op_id: "1".into(),
                    description: "sequence".into(),
                },
                RtdlNode {
                    node_kind: 2,
                    children: vec![],
                    call: Some(CapabilityCall {
                        call_id: "camera-1".into(),
                        provider_id: "realsense_camera".into(),
                        contract_id: "robonix/primitive/camera/capture".into(),
                        args_json: "{}".into(),
                    }),
                    op_id: "2".into(),
                    description: "take snapshot".into(),
                },
            ],
        };
        runtime.record_plan_ops(&plan).await;
        runtime
            .record_op_state("p", "2", RtdlNodeStateEnum::Running as u32)
            .await;
        let r = runtime
            .get_plan_status_builtin(&status_call(r#"{"plan_id":"p"}"#))
            .await;
        assert!(r.success, "{}", r.error);
        // op 2 is running; op 1 has not started (pending).
        assert!(r.output.contains(r#""op_id":"2""#));
        assert!(r.output.contains(r#""state":"running""#));
        assert!(r.output.contains(r#""state":"pending""#));
        assert!(r.output.contains(r#""running":true"#));
        assert!(r.output.contains("realsense_camera"));
        assert!(r.output.contains("robonix/primitive/camera/capture"));
    }

    #[tokio::test]
    async fn get_all_plans_lists_active_plans() {
        let runtime = PlanRuntime::default();
        let empty = runtime.get_all_plans_builtin(&status_call("{}")).await;
        assert!(empty.success);
        assert!(empty.output.contains(r#""count":0"#));

        use crate::pb::pilot::{Plan, RtdlNode};
        runtime.register_plan("p1").await.unwrap();
        runtime
            .record_plan_ops(&Plan {
                plan_id: "p1".into(),
                session_id: "s".into(),
                round: 0,
                root_index: 0,
                nodes: vec![RtdlNode {
                    node_kind: 0,
                    children: vec![],
                    call: None,
                    op_id: "1".into(),
                    description: "drive to the kitchen".into(),
                }],
            })
            .await;
        runtime.register_plan("p2").await.unwrap();
        let r = runtime.get_all_plans_builtin(&status_call("{}")).await;
        assert!(r.success);
        assert!(r.output.contains(r#""count":2"#));
        assert!(r.output.contains(r#""plan_id":"p1""#));
        assert!(r.output.contains(r#""plan_id":"p2""#));
        // p1's plan-level description (root node) is surfaced for the LLM.
        assert!(r.output.contains(r#""description":"drive to the kitchen""#));
        let snapshot = runtime.active_plans_json().await;
        assert!(snapshot.contains(r#""plan_id":"p1""#));
        assert!(snapshot.contains(r#""ops""#));
    }

    #[tokio::test]
    async fn get_plan_status_unknown_plan_errors() {
        let runtime = PlanRuntime::default();
        let r = runtime
            .get_plan_status_builtin(&status_call(r#"{"plan_id":"missing"}"#))
            .await;
        assert!(!r.success);
        assert!(r.error.contains("no active RTDL plan"));
    }

    #[tokio::test]
    async fn cancellation_waiters_observe_early_and_late_signals_per_plan() {
        let runtime = PlanRuntime::default();
        runtime.register_plan("target").await.unwrap();
        runtime.register_plan("other").await.unwrap();
        let waiting = runtime.clone();
        let waiter = tokio::spawn(async move {
            waiting.wait_for_cancellation("target").await;
        });
        tokio::task::yield_now().await;
        runtime.begin_cancel("target").await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.wait_for_cancellation("target"),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                runtime.wait_for_cancellation("other"),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn unresolved_ownership_blocks_id_reuse_and_capacity_never_evicts_it() {
        let runtime = PlanRuntime::default();
        for index in 0..MAX_ACTIVE_PLANS {
            runtime.register_plan(&index.to_string()).await.unwrap();
        }
        {
            let mut plans = runtime.inner.lock().await;
            plans
                .get_mut("0")
                .unwrap()
                .running_async
                .insert("c".into(), running_call("c"));
        }
        runtime.complete_plan("0").await;
        assert!(runtime.register_plan("0").await.is_err());
        assert!(runtime.register_plan("overflow").await.is_err());
        assert_eq!(
            runtime
                .inner
                .lock()
                .await
                .get("0")
                .unwrap()
                .running_async
                .len(),
            1
        );
        runtime.complete_plan("1").await;
        runtime.register_plan("replacement").await.unwrap();
    }
}
