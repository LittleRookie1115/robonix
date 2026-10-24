// SPDX-License-Identifier: MulanPSL-2.0
//! Ownership of a single Pilot → Executor input/output stream during generation.
use crate::health_dependencies::HealthDependencies;
use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
use crate::pb::contracts::robonix_system_executor_execute_stream_client::RobonixSystemExecutorExecuteStreamClient;
use crate::pb::executor::{ControlPlanRequest, ExecuteStreamInput, RtdlEvent};
use crate::pb::pilot::Plan;
use crate::rtdl_stream::{
    EXECUTE_STREAM_CLOSE, EXECUTE_STREAM_END, EXECUTE_STREAM_NODE, EXECUTE_STREAM_START,
};
use crate::service::TurnSteers;
use anyhow::{Result, ensure};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Streaming, transport::Channel};

/// A submission boundary that requires canceling or revisiting this response.
pub(crate) enum SubmissionInterrupt {
    Stale,
    Failed,
    Steer(crate::pb::pilot::Task),
    Blocked(String),
}

/// Admission context checked before each streamed node is submitted.
pub(crate) struct SubmissionGuard<'a> {
    /// Executor connection receiving this response's tree.
    pub client: &'a mut RobonixSystemExecutorExecuteStreamClient<Channel>,
    /// Calls already running in other trees and unavailable for redispatch.
    pub forbidden: &'a HashSet<String>,
    /// Execution changes that can invalidate a reply before submission.
    pub revision: &'a AtomicU64,
    /// Execution revision observed when this planning request began.
    pub expected_revision: u64,
    /// Vitals-computed provider/contract policy refreshed by the health monitor.
    pub dependencies: &'a RwLock<HealthDependencies>,
    /// Prioritized health updates and ordered user input checked between nodes.
    pub steers: &'a mut TurnSteers,
}

/// One submitted tree's input, observed output, and exact accepted node arena.
#[derive(Default)]
pub(crate) struct StreamExecution {
    /// Open incremental input; dropped after sealing or requesting cancellation.
    input: Option<mpsc::Sender<ExecuteStreamInput>>,
    /// Execution output retained for the session driver after generation ends.
    pub(crate) output: Option<Streaming<RtdlEvent>>,
    /// Events received during generation and awaiting session processing.
    pub(crate) events: VecDeque<RtdlEvent>,
    /// Only the nodes actually submitted to Executor, with their assigned ids.
    pub(crate) plan: Plan,
    /// Operators already closed, indexed by their accepted arena position.
    closed: Vec<bool>,
    /// Whether Executor has reported plan_complete.
    pub(crate) complete: bool,
    /// Whether output was lost before terminal execution evidence arrived.
    pub(crate) transport_failed: bool,
}

impl StreamExecution {
    /// Recheck execution failure and revision at each submission boundary.
    /// Steering and cancellation are handled by the generation loop after submission returns.
    pub(crate) async fn submit(
        &mut self,
        messages: Vec<ExecuteStreamInput>,
        guard: SubmissionGuard<'_>,
    ) -> Result<Option<SubmissionInterrupt>> {
        for message in messages {
            if let Ok(task) = guard.steers.try_recv() {
                return Ok(Some(SubmissionInterrupt::Steer(task)));
            }
            if self.plan.nodes.is_empty()
                && guard.revision.load(Ordering::Acquire) != guard.expected_revision
            {
                return Ok(Some(SubmissionInterrupt::Stale));
            }
            // Poll ready Executor events before sending the next node in a large text delta.
            loop {
                tokio::select! {
                    biased;
                    event = self.next(), if self.output.is_some() => {
                        match event? {
                            Some(event) => { if self.remember(event) { return Ok(Some(SubmissionInterrupt::Failed)); } },
                            None => anyhow::bail!("Executor EOF while submitting nodes"),
                        }
                    }
                    _ = std::future::ready(()) => break,
                }
            }
            if message.message_kind == EXECUTE_STREAM_START {
                self.start(message, guard.client).await?;
                continue;
            }
            let input = self
                .input
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("stream is closed"))?;
            let permit = input.reserve().await?;
            let dependencies = guard.dependencies.read().await;
            if let Ok(task) = guard.steers.try_recv() {
                return Ok(Some(SubmissionInterrupt::Steer(task)));
            }
            // Check after backpressure, and hold the read lock only for synchronous enqueue.
            if let Some(call) = self
                .plan
                .nodes
                .iter()
                .filter_map(|node| node.call.as_ref())
                .chain(message.node.iter().filter_map(|node| node.call.as_ref()))
                .find(|call| dependencies.blocks_call(&call.provider_id, &call.contract_id))
            {
                return Ok(Some(SubmissionInterrupt::Blocked(format!(
                    "Vitals blocked this plan: {} on {} requires hardware that is \
                     ERROR, STALE, or unverified. Preserve unrelated work and wait for verified recovery.",
                    call.contract_id, call.provider_id
                ))));
            }
            if let Some(call) = message.node.as_ref().and_then(|node| node.call.as_ref()) {
                ensure!(
                    !guard
                        .forbidden
                        .contains(&crate::planner::call_signature(call)),
                    "capability call is already in flight"
                );
            }
            permit.send(message.clone());
            drop(dependencies);
            self.record(message);
        }
        Ok(None)
    }

    /// Open one execution stream without preemptively sealing its tree.
    async fn start(
        &mut self,
        message: ExecuteStreamInput,
        client: &mut RobonixSystemExecutorExecuteStreamClient<Channel>,
    ) -> Result<()> {
        ensure!(self.input.is_none(), "stream already started");
        let (tx, rx) = mpsc::channel(1024);
        tx.send(message.clone()).await?;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.execute_stream(ReceiverStream::new(rx)),
        )
        .await??;
        self.output = Some(response.into_inner());
        self.input = Some(tx);
        self.plan = Plan {
            plan_id: message.plan_id,
            session_id: message.session_id,
            round: message.round,
            ..Default::default()
        };
        Ok(())
    }

    /// Mirror only messages actually enqueued for Executor.
    fn record(&mut self, message: ExecuteStreamInput) {
        match message.message_kind {
            EXECUTE_STREAM_NODE => {
                let node = message.node.expect("parser NODE");
                if message.has_parent {
                    self.plan.nodes[message.parent_index as usize]
                        .children
                        .push(message.node_index);
                }
                self.closed.push(node.node_kind == 2);
                self.plan.nodes.push(node);
            }
            EXECUTE_STREAM_CLOSE => self.closed[message.node_index as usize] = true,
            _ => {}
        }
    }

    /// Receive one execution event while token generation is still active.
    pub(crate) async fn next(&mut self) -> Result<Option<RtdlEvent>, tonic::Status> {
        match &mut self.output {
            Some(stream) => stream.message().await,
            None if self.plan.plan_id.is_empty() => std::future::pending().await,
            None => Ok(None),
        }
    }

    /// Remember events until the final prefix Plan has been published to the forest.
    pub(crate) fn remember(&mut self, event: RtdlEvent) -> bool {
        let failed = event
            .node_state
            .as_ref()
            .is_some_and(|n| matches!(n.state, 3..=5));
        if event.plan_complete.is_some() {
            self.complete = true;
        }
        self.events.push_back(event);
        failed
    }

    /// Close only submitted operators, then END and half-close the request transport.
    pub(crate) async fn seal(
        &mut self,
        guard: SubmissionGuard<'_>,
    ) -> Result<Option<SubmissionInterrupt>> {
        let mut messages = Vec::new();
        for index in (0..self.closed.len()).rev() {
            if !self.closed[index] {
                messages.push(ExecuteStreamInput {
                    message_kind: EXECUTE_STREAM_CLOSE,
                    node_index: index as u32,
                    ..Default::default()
                });
            }
        }
        messages.push(ExecuteStreamInput {
            message_kind: EXECUTE_STREAM_END,
            ..Default::default()
        });
        let interrupt = self.submit(messages, guard).await?;
        if interrupt.is_none() {
            self.input.take();
        }
        Ok(interrupt)
    }

    /// Drain terminal events after an invalid response or execution failure, never resending input.
    pub(crate) async fn drain(&mut self) -> Result<()> {
        while self.output.is_some() && !self.complete {
            match self.next().await {
                Ok(Some(event)) => {
                    self.remember(event);
                }
                Ok(None) => {
                    self.transport_failed = true;
                    anyhow::bail!("Executor EOF before plan_complete");
                }
                Err(error) => {
                    self.transport_failed = true;
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    /// Request cancellation before closing input, without sending CLOSE or END
    /// that could release more execution. Retain observed results for history.
    pub(crate) async fn cancel(
        &mut self,
        control: &mut RobonixSystemExecutorControlPlanClient<Channel>,
    ) -> Result<()> {
        if self.plan.plan_id.is_empty() {
            return Ok(());
        }
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(6),
            control.control_plan(ControlPlanRequest {
                action: "cancel".into(),
                plan_id: self.plan.plan_id.clone(),
                wait_ms: 5000,
            }),
        )
        .await;
        // Also close input when the control RPC fails, so Executor observes EOF.
        self.input.take();
        let response = response??.into_inner();
        ensure!(
            response.success,
            "Executor cancellation failed: {}",
            response.error
        );
        if !response.completed {
            robonix_scribe::warn!(
                "[pilot/stream] cancellation wait timed out: {}",
                response.message
            );
        }
        // Read already produced events; synchronous calls may still be finishing after timeout.
        match tokio::time::timeout(std::time::Duration::from_secs(1), self.drain()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                robonix_scribe::warn!("[pilot/stream] cancellation drain failed: {error:#}")
            }
            Err(error) => {
                robonix_scribe::warn!("[pilot/stream] cancellation drain timed out: {error}")
            }
        }
        if self.complete || self.transport_failed {
            self.output.take();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::contracts::robonix_system_executor_control_plan_server::{
        RobonixSystemExecutorControlPlan, RobonixSystemExecutorControlPlanServer,
    };
    use crate::pb::contracts::robonix_system_executor_execute_stream_server::{
        RobonixSystemExecutorExecuteStream, RobonixSystemExecutorExecuteStreamServer,
    };
    use crate::pb::executor::{ControlPlanResponse, RtdlPlanComplete};
    use crate::pb::pilot::Task;
    use crate::pb::vitals::{ComponentHealth, VitalsSnapshot};
    use crate::rtdl_stream::RtdlIncrementalParser;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::{Mutex, watch};

    fn fault_snapshot() -> VitalsSnapshot {
        VitalsSnapshot {
            components: vec![ComponentHealth {
                name: "body/lidar".into(),
                health: crate::health_dependencies::component_health::HEALTH_STALE,
                ..Default::default()
            }],
            dependency_report_json: json!({
                "status":"verified",
                "blocked_calls":[{"provider_id":"nav","component":"body/lidar",
                    "component_contracts":{"navigation/navigate":"body/lidar"}}]
            })
            .to_string(),
            ..Default::default()
        }
    }

    fn turn_inputs() -> (mpsc::Sender<Task>, watch::Sender<Option<Task>>, TurnSteers) {
        let (tasks, receiver) = mpsc::channel(4);
        let (health, watcher) = watch::channel(None);
        (tasks, health, TurnSteers::new(receiver, watcher))
    }

    fn two_call_response() -> RtdlIncrementalParser {
        let mut parser = RtdlIncrementalParser::new(
            crate::planner::CapabilityTargetMap::from([
                ("safe".into(), ("memory".into(), "memory/search".into())),
                ("move".into(), ("nav".into(), "navigation/navigate".into())),
            ]),
            Arc::new(AtomicU64::new(0)),
            "session".into(),
            0,
        );
        parser.push(
            r#"{"content":"working","message_updates":[],"rtdl_parent_task_id":"T1","rtdl_description":"work","rtdl":{"op":"sequence","children":[{"op":"do","cap":"safe","args":{}},{"op":"do","cap":"move","args":{}}]},"task_updates":[]}"#,
        ).unwrap();
        parser
    }

    /// A single model delta cannot bypass a fault observed while waiting to send its next node.
    #[tokio::test]
    async fn fault_between_do_nodes_in_one_delta_blocks_tail_and_keeps_exact_prefix() {
        let dependencies = RwLock::new(HealthDependencies::default());
        let (_, _health, mut steers) = turn_inputs();
        let revision = AtomicU64::new(0);
        let forbidden = HashSet::new();
        let mut client = RobonixSystemExecutorExecuteStreamClient::new(
            Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        );
        let mut parser = two_call_response();
        let messages = parser
            .take_messages()
            .into_iter()
            .filter(|message| message.message_kind != EXECUTE_STREAM_START)
            .collect();
        let (input, mut receiver) = mpsc::channel(1);
        let mut live = StreamExecution {
            input: Some(input),
            plan: Plan {
                plan_id: parser.plan.plan_id.clone(),
                session_id: parser.plan.session_id.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let submit = live.submit(
            messages,
            SubmissionGuard {
                client: &mut client,
                forbidden: &forbidden,
                revision: &revision,
                expected_revision: 0,
                dependencies: &dependencies,
                steers: &mut steers,
            },
        );
        let inject = async {
            let root = receiver.recv().await.unwrap();
            assert_eq!(
                root.node.as_ref().unwrap().node_kind,
                crate::planner::RTDL_SEQUENCE
            );
            while receiver.is_empty() {
                tokio::task::yield_now().await;
            }
            dependencies.write().await.observe_health(&fault_snapshot());
            let first = receiver.recv().await.unwrap();
            assert_eq!(
                first
                    .node
                    .as_ref()
                    .unwrap()
                    .call
                    .as_ref()
                    .unwrap()
                    .provider_id,
                "memory"
            );
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(submit, inject)
        })
        .await
        .unwrap();
        let Some(SubmissionInterrupt::Blocked(message)) = result.unwrap() else {
            panic!("fault must reject the next node");
        };
        assert!(message.contains("Vitals blocked this plan"));
        assert_eq!(live.plan.nodes.len(), 2);
        assert_eq!(live.plan.nodes[0].children, [1]);
        assert_eq!(live.plan.nodes[1], parser.plan.nodes[1]);
        assert!(live.input.is_some());
        assert!(!live.closed[0]);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    /// A fault after the last node still prevents CLOSE and END from releasing more work.
    #[tokio::test]
    async fn fault_before_sealing_does_not_close_or_end_the_submitted_prefix() {
        let mut parser = two_call_response();
        let messages = parser
            .take_messages()
            .into_iter()
            .filter(|message| message.message_kind == EXECUTE_STREAM_NODE)
            .collect();
        let dependencies = RwLock::new(HealthDependencies::default());
        let (_tasks, _health, mut steers) = turn_inputs();
        let forbidden = HashSet::new();
        let revision = AtomicU64::new(0);
        let mut client = RobonixSystemExecutorExecuteStreamClient::new(
            Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        );
        let (input, mut receiver) = mpsc::channel(8);
        let mut live = StreamExecution {
            input: Some(input),
            plan: Plan {
                plan_id: parser.plan.plan_id.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            live.submit(
                messages,
                SubmissionGuard {
                    client: &mut client,
                    forbidden: &forbidden,
                    revision: &revision,
                    expected_revision: 0,
                    dependencies: &dependencies,
                    steers: &mut steers,
                }
            )
            .await
            .unwrap()
            .is_none()
        );
        for _ in 0..3 {
            assert_eq!(
                receiver.recv().await.unwrap().message_kind,
                EXECUTE_STREAM_NODE
            );
        }
        dependencies.write().await.observe_health(&fault_snapshot());
        let interrupt = live
            .seal(SubmissionGuard {
                client: &mut client,
                forbidden: &forbidden,
                revision: &revision,
                expected_revision: 0,
                dependencies: &dependencies,
                steers: &mut steers,
            })
            .await
            .unwrap();
        assert!(matches!(interrupt, Some(SubmissionInterrupt::Blocked(_))));
        assert!(live.input.is_some());
        assert!(!live.closed[0]);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    /// Existing faults reject unsafe work while unrelated calls retain the upstream wire protocol.
    #[tokio::test]
    async fn healthy_and_faulted_batches_keep_exact_admitted_arenas() {
        for fault in [false, true] {
            let mut parser = two_call_response();
            let messages = parser
                .take_messages()
                .into_iter()
                .filter(|message| message.message_kind != EXECUTE_STREAM_START)
                .collect();
            parser.finish().unwrap();
            let mut policy = HealthDependencies::default();
            if fault {
                policy.observe_health(&fault_snapshot());
            }
            let dependencies = RwLock::new(policy);
            let (_tasks, _health, mut steers) = turn_inputs();
            let forbidden = HashSet::new();
            let revision = AtomicU64::new(0);
            let mut client = RobonixSystemExecutorExecuteStreamClient::new(
                Channel::from_static("http://127.0.0.1:9").connect_lazy(),
            );
            let (input, mut receiver) = mpsc::channel(8);
            let mut live = StreamExecution {
                input: Some(input),
                plan: Plan {
                    plan_id: parser.plan.plan_id.clone(),
                    session_id: parser.plan.session_id.clone(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let result = live
                .submit(
                    messages,
                    SubmissionGuard {
                        client: &mut client,
                        forbidden: &forbidden,
                        revision: &revision,
                        expected_revision: 0,
                        dependencies: &dependencies,
                        steers: &mut steers,
                    },
                )
                .await
                .unwrap();
            if fault {
                assert!(matches!(result, Some(SubmissionInterrupt::Blocked(_))));
                assert_eq!(live.plan.nodes.len(), 2);
                assert_eq!(live.plan.nodes[0].children, [1]);
                assert_eq!(live.plan.nodes[1], parser.plan.nodes[1]);
                assert_eq!(
                    receiver.recv().await.unwrap().message_kind,
                    EXECUTE_STREAM_NODE
                );
                assert_eq!(
                    receiver
                        .recv()
                        .await
                        .unwrap()
                        .node
                        .unwrap()
                        .call
                        .unwrap()
                        .provider_id,
                    "memory"
                );
                assert!(matches!(
                    receiver.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            } else {
                assert!(result.is_none());
                assert_eq!(live.plan, parser.plan);
                assert!(
                    live.seal(SubmissionGuard {
                        client: &mut client,
                        forbidden: &forbidden,
                        revision: &revision,
                        expected_revision: 0,
                        dependencies: &dependencies,
                        steers: &mut steers,
                    })
                    .await
                    .unwrap()
                    .is_none()
                );
                let mut kinds = Vec::new();
                while let Some(message) = receiver.recv().await {
                    kinds.push(message.message_kind);
                }
                assert_eq!(
                    kinds,
                    [
                        EXECUTE_STREAM_NODE,
                        EXECUTE_STREAM_NODE,
                        EXECUTE_STREAM_NODE,
                        EXECUTE_STREAM_CLOSE,
                        EXECUTE_STREAM_END
                    ]
                );
            }
        }
    }

    /// Health wins over queued user input even within a single submitted text delta.
    #[tokio::test]
    async fn health_steer_interrupts_before_the_next_node_without_filing_user_work() {
        let (tasks, health, mut steers) = turn_inputs();
        tasks
            .send(Task {
                text: "user steer".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        health
            .send(Some(Task {
                text: "fault".into(),
                context_json: r#"{"system_vitals_alert":true}"#.into(),
                ..Default::default()
            }))
            .unwrap();
        let dependencies = RwLock::new(HealthDependencies::default());
        let forbidden = HashSet::new();
        let revision = AtomicU64::new(0);
        let mut client = RobonixSystemExecutorExecuteStreamClient::new(
            Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        );
        let mut parser = two_call_response();
        let mut live = StreamExecution::default();
        let interrupt = live
            .submit(
                parser.take_messages(),
                SubmissionGuard {
                    client: &mut client,
                    forbidden: &forbidden,
                    revision: &revision,
                    expected_revision: 0,
                    dependencies: &dependencies,
                    steers: &mut steers,
                },
            )
            .await
            .unwrap();
        let Some(SubmissionInterrupt::Steer(task)) = interrupt else {
            panic!("health notification must take priority");
        };
        assert_eq!(task.text, "fault");
        assert!(live.plan.nodes.is_empty());
        assert_eq!(steers.try_recv().unwrap().text, "user steer");
    }

    /// Control endpoint that verifies input stays open until cancellation arrives.
    struct CancelControl {
        /// Input receiver used to detect premature CLOSE, END, or EOF.
        input: Arc<Mutex<mpsc::Receiver<ExecuteStreamInput>>>,
        /// Whether to simulate a failed cancellation RPC.
        reject: bool,
        /// Whether cancellation has already finished all accepted work.
        completed: bool,
    }

    #[tonic::async_trait]
    impl RobonixSystemExecutorControlPlan for CancelControl {
        /// Check ordering at the RPC boundary, then accept or reject cancellation.
        async fn control_plan(
            &self,
            request: tonic::Request<ControlPlanRequest>,
        ) -> Result<tonic::Response<ControlPlanResponse>, tonic::Status> {
            let request = request.into_inner();
            assert_eq!(request.action, "cancel");
            assert_eq!(request.plan_id, "partial");
            assert!(matches!(
                self.input.lock().await.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            if self.reject {
                return Err(tonic::Status::unavailable("control unavailable"));
            }
            Ok(tonic::Response::new(ControlPlanResponse {
                success: true,
                completed: self.completed,
                ..Default::default()
            }))
        }
    }

    /// Neither successful nor failed cancellation may seal a partial plan first.
    #[tokio::test]
    async fn cancellation_precedes_input_closure_even_when_control_fails() {
        for reject in [false, true] {
            let (tx, rx) = mpsc::channel(8);
            let input = Arc::new(Mutex::new(rx));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let stream = listener.accept().await.map(|(stream, _)| stream);
                Some((stream, listener))
            });
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(RobonixSystemExecutorControlPlanServer::new(CancelControl {
                        input: Arc::clone(&input),
                        reject,
                        completed: true,
                    }))
                    .serve_with_incoming(incoming),
            );
            let mut control =
                RobonixSystemExecutorControlPlanClient::connect(format!("http://{address}"))
                    .await
                    .unwrap();
            let mut live = StreamExecution {
                input: Some(tx),
                plan: Plan {
                    plan_id: "partial".into(),
                    ..Default::default()
                },
                closed: vec![false],
                ..Default::default()
            };

            let result = live.cancel(&mut control).await;

            assert_eq!(result.is_err(), reject);
            assert!(live.input.is_none());
            assert!(matches!(
                input.lock().await.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ));
            server.abort();
        }
    }

    /// An accepted synchronous call that reports completion after the drain window.
    struct DeferredExecution {
        /// Releases the terminal event only after cancellation has returned.
        finish: Arc<tokio::sync::Notify>,
    }

    #[tonic::async_trait]
    impl RobonixSystemExecutorExecuteStream for DeferredExecution {
        type ExecuteStreamStream = ReceiverStream<Result<RtdlEvent, tonic::Status>>;

        async fn execute_stream(
            &self,
            _request: tonic::Request<Streaming<ExecuteStreamInput>>,
        ) -> Result<tonic::Response<Self::ExecuteStreamStream>, tonic::Status> {
            let (tx, rx) = mpsc::channel(1);
            let finish = Arc::clone(&self.finish);
            tokio::spawn(async move {
                finish.notified().await;
                let _ = tx
                    .send(Ok(RtdlEvent {
                        event_kind: 2,
                        plan_complete: Some(RtdlPlanComplete {
                            plan_id: "partial".into(),
                            any_failed: false,
                        }),
                        ..Default::default()
                    }))
                    .await;
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test]
    async fn cancellation_drain_timeout_keeps_output_for_the_session() {
        let (input, rx) = mpsc::channel(8);
        let finish = Arc::new(tokio::sync::Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures_util::stream::unfold(listener, |listener| async {
            let stream = listener.accept().await.map(|(stream, _)| stream);
            Some((stream, listener))
        });
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RobonixSystemExecutorControlPlanServer::new(CancelControl {
                    input: Arc::new(Mutex::new(rx)),
                    reject: false,
                    completed: false,
                }))
                .add_service(RobonixSystemExecutorExecuteStreamServer::new(
                    DeferredExecution {
                        finish: Arc::clone(&finish),
                    },
                ))
                .serve_with_incoming(incoming),
        );
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = RobonixSystemExecutorExecuteStreamClient::new(channel.clone());
        let (_, request) = mpsc::channel(1);
        let output = client
            .execute_stream(ReceiverStream::new(request))
            .await
            .unwrap()
            .into_inner();
        let mut live = StreamExecution {
            input: Some(input),
            output: Some(output),
            plan: Plan {
                plan_id: "partial".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        live.cancel(&mut RobonixSystemExecutorControlPlanClient::new(channel))
            .await
            .unwrap();

        assert!(!live.complete);
        assert!(!live.transport_failed);
        assert!(live.output.is_some());
        finish.notify_one();
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), live.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.plan_complete.unwrap().plan_id, "partial");
        server.abort();
    }

    /// A transport already removed during cleanup must report EOF to the forest driver.
    #[tokio::test]
    async fn removed_output_does_not_hang_forest_handoff() {
        let mut live = StreamExecution {
            plan: Plan {
                plan_id: "disconnected".into(),
                ..Default::default()
            },
            transport_failed: true,
            ..Default::default()
        };
        let result = tokio::time::timeout(std::time::Duration::from_millis(50), live.next())
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_none());
    }
}
