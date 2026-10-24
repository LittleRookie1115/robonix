// SPDX-License-Identifier: MulanPSL-2.0
//! Cancellation regressions through real Executor gRPC, Atlas, and HTTP MCP.

use crate::active_plans::RunningAsyncCall;
use crate::executor_lifecycle;
use crate::grpc_server::ExecutorServiceImpl;
use crate::lifecycle::{
    CMD_ACTIVATE, CMD_INIT, CMD_SHUTDOWN, SHARED_DRIVER_CONTRACT, call_startup_driver,
    connect_startup_driver,
};
use crate::pb::contracts::robonix_lifecycle_driver_server::RobonixLifecycleDriverServer;
use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
use crate::pb::contracts::robonix_system_executor_control_plan_server::RobonixSystemExecutorControlPlanServer;
use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::contracts::robonix_system_executor_execute_server::RobonixSystemExecutorExecuteServer;
use crate::pb::contracts::robonix_system_executor_execute_stream_client::RobonixSystemExecutorExecuteStreamClient;
use crate::pb::contracts::robonix_system_executor_execute_stream_server::RobonixSystemExecutorExecuteStreamServer;
use crate::pb::executor::{ControlPlanRequest, ControlPlanResponse, ExecuteStreamInput, RtdlEvent};
use crate::pb::lifecycle::DriverRequest;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, Plan, RtdlNode};
use crate::plan_input::{
    EXECUTE_STREAM_CLOSE, EXECUTE_STREAM_END, EXECUTE_STREAM_NODE, EXECUTE_STREAM_START,
};
use crate::verification::VerificationPolicy;
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use robonix_atlas::service::{AtlasRegistry, AtlasService};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Channel;
use tonic::{Code, Streaming};

/// A provider request whose response is controlled by a cancellation regression.
struct McpRequest {
    /// Tool name and arguments delivered by the production MCP client.
    params: serde_json::Value,
    /// Releases the pending provider call with an actual MCP result.
    reply: oneshot::Sender<serde_json::Value>,
}

/// Isolated network services and their owned background tasks for one regression.
struct Environment {
    /// Executor handlers used to inspect authoritative ownership between RPCs.
    service: ExecutorServiceImpl,
    /// Real gRPC transport shared by execution and cancellation clients.
    channel: Channel,
    /// Socket address used for lifecycle Driver requests.
    address: SocketAddr,
    /// Pending HTTP MCP tool calls waiting for a test-controlled response.
    requests: mpsc::Receiver<McpRequest>,
    /// Services aborted when the test ends, including assertions that panic.
    servers: Vec<JoinHandle<()>>,
}

impl Drop for Environment {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

fn mcp_result(output: &str, is_error: bool) -> serde_json::Value {
    serde_json::json!({
        "content": [{"type": "text", "text": output}], "isError": is_error,
    })
}

async fn mcp_connection(stream: tokio::net::TcpStream, requests: mpsc::Sender<McpRequest>) {
    let mut stream = BufReader::new(stream);
    let mut request_line = String::new();
    stream.read_line(&mut request_line).await.unwrap();
    let mut content_length = 0;
    loop {
        let mut header = String::new();
        stream.read_line(&mut header).await.unwrap();
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse::<usize>().unwrap();
        }
    }
    let (status, body) = if request_line.starts_with("POST ") {
        let mut body = vec![0; content_length];
        stream.read_exact(&mut body).await.unwrap();
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if request["method"] == "notifications/initialized" {
            ("202 Accepted", String::new())
        } else {
            let result = if request["method"] == "initialize" {
                serde_json::json!({
                    "protocolVersion": request["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "cancellation-test", "version": "1"},
                })
            } else {
                assert_eq!(request["method"], "tools/call");
                let (reply, response) = oneshot::channel();
                if requests
                    .send(McpRequest {
                        params: request["params"].clone(),
                        reply,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                let Ok(result) = response.await else {
                    return;
                };
                result
            };
            (
                "200 OK",
                serde_json::json!({
                    "jsonrpc": "2.0", "id": request["id"], "result": result,
                })
                .to_string(),
            )
        }
    } else {
        ("405 Method Not Allowed", String::new())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn nodes(plan_id: &str, contracts: &[&str]) -> Vec<RtdlNode> {
    let mut nodes = vec![RtdlNode {
        node_kind: 0,
        op_id: "root".into(),
        description: "ordered work".into(),
        ..Default::default()
    }];
    for (index, contract) in contracts.iter().enumerate() {
        nodes.push(RtdlNode {
            node_kind: 2,
            op_id: format!("op_{index}"),
            description: contract.to_string(),
            call: Some(CapabilityCall {
                call_id: format!("{plan_id}:{index}"),
                provider_id: "provider".into(),
                contract_id: format!("robonix/test/{contract}"),
                args_json: serde_json::json!({"plan_id": plan_id}).to_string(),
            }),
            ..Default::default()
        });
    }
    nodes
}

impl Environment {
    async fn start(asynchronous: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let (requests, receiver) = mpsc::channel(32);
        let mcp = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                connections.spawn(mcp_connection(stream, requests.clone()));
            }
        });
        let atlas_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let atlas_address = atlas_listener.local_addr().unwrap();
        let atlas_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(atlas_pb::atlas_server::AtlasServer::new(AtlasService::new(
                    Arc::new(AtlasRegistry::default()),
                )))
                .serve_with_incoming(TcpListenerStream::new(atlas_listener))
                .await
                .unwrap();
        });
        let mut atlas = AtlasClient::connect_with_retry(
            format!("http://{atlas_address}"),
            50,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        atlas
            .register_service("provider", "robonix/test", "")
            .await
            .unwrap();
        atlas
            .register_service("executor", "robonix/system/executor", "")
            .await
            .unwrap();
        let mut contracts = vec!["work", "next"];
        if asynchronous {
            contracts.extend(["work/status", "work/cancel"]);
        }
        for contract in contracts {
            atlas
                .declare_capability(
                    "provider",
                    &format!("robonix/test/{contract}"),
                    atlas_pb::Transport::Mcp,
                    &endpoint,
                    robonix_atlas::client::mcp_params("{}"),
                )
                .await
                .unwrap();
        }
        let service = ExecutorServiceImpl::new(
            atlas.clone(),
            "executor".into(),
            Arc::new(VerificationPolicy::new(false, Vec::new())),
        );
        let executor_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = executor_listener.local_addr().unwrap();
        atlas
            .declare_capability(
                "executor",
                SHARED_DRIVER_CONTRACT,
                atlas_pb::Transport::Grpc,
                &address.to_string(),
                robonix_atlas::client::grpc_params(
                    "capabilities/lifecycle/driver.v1.toml",
                    "robonix.contracts.RobonixLifecycleDriver",
                    "/robonix.contracts.RobonixLifecycleDriver/Driver",
                ),
            )
            .await
            .unwrap();
        let lifecycle = executor_lifecycle(atlas, "executor".into(), service.clone());
        let shutdown = lifecycle.subscribe_shutdown();
        let serving = service.clone();
        let executor = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RobonixLifecycleDriverServer::new(lifecycle))
                .add_service(RobonixSystemExecutorExecuteServer::new(serving.clone()))
                .add_service(RobonixSystemExecutorExecuteStreamServer::new(
                    serving.clone(),
                ))
                .add_service(RobonixSystemExecutorControlPlanServer::new(serving))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(executor_listener),
                    crate::lifecycle::wait_for_driver_shutdown(shutdown),
                )
                .await
                .unwrap();
        });
        let mut driver = connect_startup_driver(&address.to_string()).await.unwrap();
        assert_eq!(
            call_startup_driver(&mut driver, CMD_INIT).await.unwrap(),
            "inactive"
        );
        assert_eq!(
            call_startup_driver(&mut driver, CMD_ACTIVATE)
                .await
                .unwrap(),
            "active"
        );
        drop(driver);
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            service,
            channel,
            address,
            requests: receiver,
            servers: vec![mcp, atlas_server, executor],
        }
    }

    async fn open(
        &self,
        plan_id: &str,
        contracts: &[&str],
        end: bool,
    ) -> (mpsc::Sender<ExecuteStreamInput>, Streaming<RtdlEvent>) {
        let (sender, receiver) = mpsc::channel(16);
        sender
            .send(ExecuteStreamInput {
                message_kind: EXECUTE_STREAM_START,
                plan_id: plan_id.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        for (index, node) in nodes(plan_id, contracts).into_iter().enumerate() {
            sender
                .send(ExecuteStreamInput {
                    message_kind: EXECUTE_STREAM_NODE,
                    node_index: index as u32,
                    has_parent: index != 0,
                    node: Some(node),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        if end {
            sender
                .send(ExecuteStreamInput {
                    message_kind: EXECUTE_STREAM_CLOSE,
                    ..Default::default()
                })
                .await
                .unwrap();
            sender
                .send(ExecuteStreamInput {
                    message_kind: EXECUTE_STREAM_END,
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let response = RobonixSystemExecutorExecuteStreamClient::new(self.channel.clone())
            .execute_stream(ReceiverStream::new(receiver))
            .await
            .unwrap()
            .into_inner();
        (sender, response)
    }

    async fn request(&mut self, tool: &str, run_id: Option<&str>) -> McpRequest {
        let request = tokio::time::timeout(Duration::from_secs(4), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.params["name"], tool);
        if let Some(run_id) = run_id {
            assert_eq!(request.params["arguments"]["run_id"], run_id);
        }
        request
    }

    fn cancel(&self, plan_id: &str, wait_ms: u64) -> JoinHandle<ControlPlanResponse> {
        let channel = self.channel.clone();
        let request = ControlPlanRequest {
            action: "cancel".into(),
            plan_id: plan_id.into(),
            wait_ms,
        };
        tokio::spawn(async move {
            RobonixSystemExecutorControlPlanClient::new(channel)
                .control_plan(request)
                .await
                .unwrap()
                .into_inner()
        })
    }

    async fn confirm(&mut self, run_id: &str, state: &str) {
        self.request("status", Some(run_id))
            .await
            .reply
            .send(mcp_result(
                &serde_json::json!({"known": true, "run_id": run_id, "state": state}).to_string(),
                false,
            ))
            .unwrap();
    }

    async fn unknown(&mut self, run_id: &str) {
        self.request("status", Some(run_id))
            .await
            .reply
            .send(mcp_result(r#"{"known":false,"state":"CANCELED"}"#, false))
            .unwrap();
    }

    async fn active(&self) -> serde_json::Value {
        serde_json::from_str(&self.service.runtime.active_plans_json().await).unwrap()
    }

    async fn wait_cancelled(&self, plan_id: &str) {
        tokio::time::timeout(
            Duration::from_secs(1),
            self.service.runtime.wait_for_cancellation(plan_id),
        )
        .await
        .unwrap();
    }
}

async fn events(mut stream: Streaming<RtdlEvent>) -> Vec<RtdlEvent> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut events = Vec::new();
        while let Some(event) = stream.message().await.unwrap() {
            events.push(event);
        }
        events
    })
    .await
    .unwrap()
}

async fn cancel_response(cancel: JoinHandle<ControlPlanResponse>) -> ControlPlanResponse {
    tokio::time::timeout(Duration::from_secs(4), cancel)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn streaming_cancel_interrupts_hung_sync_and_does_not_cancel_another_plan() {
    let mut env = Environment::start(false).await;
    let (target_input, target) = env.open("target", &["work"], false).await;
    let hung = env.request("work", None).await;
    assert_eq!(hung.params["arguments"]["plan_id"], "target");
    let (_other_input, other) = env.open("other", &["work"], true).await;
    let other_call = env.request("work", None).await;
    assert_eq!(other_call.params["arguments"]["plan_id"], "other");
    let response = cancel_response(env.cancel("target", 500)).await;
    assert!(response.success, "{}", response.error);
    assert!(response.completed);
    assert!(response.message.contains("physical stop is unconfirmed"));
    let _ = target_input
        .send(ExecuteStreamInput {
            message_kind: EXECUTE_STREAM_NODE,
            node_index: 2,
            has_parent: true,
            node: nodes("late", &["next"]).pop(),
            ..Default::default()
        })
        .await;
    let target_events = events(target).await;
    assert!(
        target_events.iter().any(|event| event
            .node_state
            .as_ref()
            .is_some_and(
                |state| state.node_index == 1 && state.state == RtdlNodeStateEnum::Canceled as u32,
            ))
    );
    assert!(
        target_events
            .last()
            .unwrap()
            .plan_complete
            .as_ref()
            .unwrap()
            .any_failed
    );
    assert!(!env.service.runtime.is_cancelled("other").await);
    assert!(env.requests.try_recv().is_err());
    other_call.reply.send(mcp_result("done", false)).unwrap();
    assert!(
        !events(other)
            .await
            .last()
            .unwrap()
            .plan_complete
            .as_ref()
            .unwrap()
            .any_failed
    );
    drop(hung);
}

#[tokio::test]
async fn streaming_cancel_interrupts_hung_status_and_releases_atlas_channels() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work", "next"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let hung = env.request("status", Some("owned")).await;
    let cancel = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(r#"{"accepted":true}"#, false))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    let response = cancel_response(cancel).await;
    assert!(response.success, "{}", response.error);
    assert!(response.completed);
    assert!(
        events(stream)
            .await
            .last()
            .unwrap()
            .plan_complete
            .as_ref()
            .unwrap()
            .any_failed
    );
    assert!(env.requests.try_recv().is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = env
                .service
                .atlas
                .inner()
                .inspect_atlas(atlas_pb::InspectAtlasRequest {})
                .await
                .unwrap()
                .into_inner();
            let snapshot: serde_json::Value = serde_json::from_str(&snapshot.json).unwrap();
            if snapshot["channels"].as_object().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(hung);
}

#[tokio::test]
async fn late_async_start_retains_failed_cancellation_and_running_dependency_evidence() {
    for (output, is_error) in [
        ("refused", true),
        (r#"{"accepted":false,"detail":"refused"}"#, false),
        (r#"{"ok":false,"message":"refused"}"#, false),
        (r#"{"success":false,"detail":"refused"}"#, false),
    ] {
        let mut env = Environment::start(true).await;
        let (_input, stream) = env.open("target", &["work"], false).await;
        let initial = env.request("work", None).await;
        let pending = env.cancel("target", 50);
        env.wait_cancelled("target").await;
        let response = cancel_response(pending).await;
        assert!(response.success, "{}", response.error);
        assert!(!response.completed);
        assert_eq!(env.active().await["count"], 1);
        let cancel = env.cancel("target", 500);
        initial
            .reply
            .send(mcp_result(r#"{"run_id":"late"}"#, false))
            .unwrap();
        env.request("cancel", Some("late"))
            .await
            .reply
            .send(mcp_result(output, is_error))
            .unwrap();
        env.unknown("late").await;
        let response = cancel_response(cancel).await;
        assert!(!response.success);
        assert!(!response.completed);
        assert!(response.error.contains("refused"), "{}", response.error);
        assert!(
            response
                .error
                .contains("does not identify the requested run")
        );
        events(stream).await;
        let active = env.active().await;
        assert_eq!(active["count"], 1);
        assert_eq!(active["plans"][0]["cancelled"], true);
        assert_eq!(active["plans"][0]["ops"][1]["state"], "running");
        assert_eq!(active["plans"][0]["ops"][1]["provider_id"], "provider");
        let mut duplicate_nodes = nodes("target", &["work"]);
        duplicate_nodes[0].children = vec![1];
        let error = RobonixSystemExecutorExecuteClient::new(env.channel.clone())
            .execute(Plan {
                plan_id: "target".into(),
                nodes: duplicate_nodes,
                ..Default::default()
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::InvalidArgument);
        let retry = env.cancel("target", 500);
        env.request("cancel", Some("late"))
            .await
            .reply
            .send(mcp_result("accepted", false))
            .unwrap();
        env.confirm("late", "CANCELED").await;
        let response = cancel_response(retry).await;
        assert!(response.success, "{}", response.error);
        assert!(response.completed);
        assert_eq!(env.active().await["count"], 0);
        assert!(env.requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn rejected_cancel_payload_still_confirms_an_already_terminal_run() {
    for state in ["SUCCEEDED", "FAILED", "CANCELED", "TIMEOUT"] {
        let mut env = Environment::start(true).await;
        let (_input, stream) = env.open("target", &["work"], true).await;
        env.request("work", None)
            .await
            .reply
            .send(mcp_result(r#"{"run_id":"owned"}"#, false))
            .unwrap();
        let cancel = env.cancel("target", 500);
        env.request("cancel", Some("owned"))
            .await
            .reply
            .send(mcp_result(
                r#"{"accepted":false,"detail":"run is already terminal"}"#,
                false,
            ))
            .unwrap();
        env.confirm("owned", state).await;
        let response = cancel_response(cancel).await;
        assert!(response.success && response.completed, "{}", response.error);
        assert!(response.error.is_empty());
        events(stream).await;
        assert_eq!(env.active().await["count"], 0);
        assert!(env.requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn mcp_cancel_error_retains_both_failures_then_terminal_retry_allows_shutdown() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let cancel = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result("cancel refused", true))
        .unwrap();
    env.request("status", Some("owned"))
        .await
        .reply
        .send(mcp_result("status unavailable", true))
        .unwrap();
    let response = cancel_response(cancel).await;
    assert!(!response.success && !response.completed);
    assert!(
        response.error.contains("cancel refused"),
        "{}",
        response.error
    );
    assert!(
        response.error.contains("status unavailable"),
        "{}",
        response.error
    );
    events(stream).await;
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
    let retry = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result("run is already canceled", true))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    let response = cancel_response(retry).await;
    assert!(response.success && response.completed, "{}", response.error);
    assert!(response.error.is_empty());
    assert_eq!(env.active().await["count"], 0);
    let mut driver = connect_startup_driver(&env.address.to_string())
        .await
        .unwrap();
    assert_eq!(
        call_startup_driver(&mut driver, CMD_SHUTDOWN)
            .await
            .unwrap(),
        "terminated"
    );
}

#[tokio::test]
async fn refused_cancel_keeps_pending_state_within_the_original_two_second_budget() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let started = tokio::time::Instant::now();
    let mut cancel = env.cancel("target", 3_000);
    let request = env.request("cancel", Some("owned")).await;
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    request
        .reply
        .send(mcp_result(
            r#"{"accepted":false,"detail":"cancel refused"}"#,
            false,
        ))
        .unwrap();
    let mut polls = 0;
    let response = tokio::time::timeout_at(started + Duration::from_secs(3), async {
        loop {
            tokio::select! {
                response = &mut cancel => break response.unwrap(),
                Some(status) = env.requests.recv() => {
                    assert_eq!(status.params["name"], "status");
                    assert_eq!(status.params["arguments"]["run_id"], "owned");
                    let _ = status.reply.send(mcp_result(r#"{"known":true,"state":"RUNNING"}"#, false));
                    polls += 1;
                }
            }
        }
    })
    .await
    .expect("status confirmation must not start a new two-second budget");
    assert!(polls > 0);
    assert!(!response.success && !response.completed);
    for expected in [
        "cancel refused",
        "status confirmation timed out",
        "last status=running",
    ] {
        assert!(response.error.contains(expected), "{}", response.error);
    }
    events(stream).await;
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
    let retry = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(r#"{"accepted":false}"#, false))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    assert!(cancel_response(retry).await.completed);
    assert_eq!(env.active().await["count"], 0);
}

#[tokio::test]
async fn accepted_cancel_is_not_terminal_and_unknown_status_keeps_owned_run() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let cancel = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(r#"{"accepted":true}"#, false))
        .unwrap();
    env.confirm("owned", "RUNNING").await;
    let unknown = env.request("status", Some("owned")).await;
    unknown
        .reply
        .send(mcp_result(r#"{"known":false,"state":"CANCELED"}"#, false))
        .unwrap();
    let response = cancel_response(cancel).await;
    assert!(!response.success);
    assert!(!response.completed);
    events(stream).await;
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
    let retry = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result("accepted", false))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    assert!(cancel_response(retry).await.completed);
    assert_eq!(env.active().await["count"], 0);
}

#[tokio::test]
async fn accepted_but_running_cancellation_times_out_without_losing_ownership() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let mut cancel = env.cancel("target", 3_000);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(r#"{"accepted":true}"#, false))
        .unwrap();
    let mut polls = 0;
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                response = &mut cancel => break response.unwrap(),
                Some(status) = env.requests.recv() => {
                    assert_eq!(status.params["name"], "status");
                    assert_eq!(status.params["arguments"]["run_id"], "owned");
                    let _ = status.reply.send(mcp_result(r#"{"known":true,"state":"RUNNING"}"#, false));
                    polls += 1;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(polls > 0);
    assert!(!response.success && !response.completed);
    assert!(response.error.contains("status confirmation timed out"));
    events(stream).await;
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
    let retry = env.cancel("target", 500);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result("accepted", false))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    assert!(cancel_response(retry).await.completed);
}

#[tokio::test]
async fn input_eof_does_not_confirm_remote_stop_or_forget_the_run() {
    let mut env = Environment::start(true).await;
    let (input, mut stream) = env.open("target", &["work"], false).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let hung_status = env.request("status", Some("owned")).await;
    drop(input);
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(
            r#"{"accepted":false,"detail":"refused"}"#,
            false,
        ))
        .unwrap();
    env.unknown("owned").await;
    let error = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match stream.message().await {
                Err(error) => break error,
                Ok(Some(_)) => {}
                Ok(None) => panic!("EOF before END must report invalid_argument"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(env.active().await["count"], 1);
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
    let (remote, retry) = tokio::time::timeout(Duration::from_secs(4), async {
        let mut retry = env.cancel("target", 500);
        loop {
            tokio::select! {
                Some(remote) = env.requests.recv() => break (remote, retry),
                response = &mut retry => {
                    assert!(!response.unwrap().completed);
                    retry = env.cancel("target", 500);
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(remote.params["name"], "cancel");
    assert_eq!(remote.params["arguments"]["run_id"], "owned");
    remote.reply.send(mcp_result("accepted", false)).unwrap();
    env.confirm("owned", "CANCELED").await;
    assert!(cancel_response(retry).await.completed);
    assert_eq!(env.active().await["count"], 0);
    drop(hung_status);
}

#[tokio::test]
async fn concurrent_cancels_bound_hung_rpc_and_retry_only_the_unconfirmed_handle() {
    let mut env = Environment::start(true).await;
    env.service.runtime.register_plan("target").await.unwrap();
    for run_id in ["slow", "fast"] {
        env.service
            .runtime
            .register_or_cancel_async_call(
                "target",
                RunningAsyncCall {
                    call_id: run_id.into(),
                    provider_id: "provider".into(),
                    cancel_contract: "robonix/test/work/cancel".into(),
                    run_id: run_id.into(),
                },
                "executor",
                &mut env.service.atlas.clone(),
            )
            .await;
    }
    let cancel = env.cancel("target", 3_000);
    let first = env.request("cancel", None).await;
    let second = env.request("cancel", None).await;
    let (hung, fast) = if first.params["arguments"]["run_id"] == "slow" {
        (first, second)
    } else {
        (second, first)
    };
    fast.reply.send(mcp_result("accepted", false)).unwrap();
    env.confirm("fast", "CANCELED").await;
    env.service.runtime.complete_plan("target").await;
    let other = cancel_response(env.cancel("unrelated", 500)).await;
    assert!(other.success && other.completed);
    let response = cancel_response(cancel).await;
    assert!(!response.success && !response.completed);
    assert!(response.error.contains("slow"));
    assert!(!response.error.contains("fast"));
    let retry = env.cancel("target", 500);
    env.request("cancel", Some("slow"))
        .await
        .reply
        .send(mcp_result("accepted", false))
        .unwrap();
    env.confirm("slow", "CANCELED").await;
    assert!(cancel_response(retry).await.completed);
    assert!(env.requests.try_recv().is_err());
    drop(hung);
}

#[tokio::test]
async fn disconnected_control_request_does_not_abandon_owned_cancellation() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let cancel = env.cancel("target", 5_000);
    let remote = env.request("cancel", Some("owned")).await;
    cancel.abort();
    remote.reply.send(mcp_result("accepted", false)).unwrap();
    env.confirm("owned", "CANCELED").await;
    events(stream).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while env.active().await["count"] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_run_id_never_falls_back_to_the_providers_latest_run() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], true).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"accepted":true}"#, false))
        .unwrap();
    events(stream).await;
    let response = cancel_response(env.cancel("target", 500)).await;
    assert!(!response.success && !response.completed);
    assert!(response.error.contains("no run_id"));
    assert!(env.requests.try_recv().is_err());
    assert_eq!(env.active().await["plans"][0]["ops"][1]["state"], "running");
}

#[tokio::test]
async fn driver_shutdown_rejects_new_streams_and_retries_failed_remote_drain() {
    let mut env = Environment::start(true).await;
    let (_input, stream) = env.open("target", &["work"], false).await;
    env.request("work", None)
        .await
        .reply
        .send(mcp_result(r#"{"run_id":"owned"}"#, false))
        .unwrap();
    let mut driver = connect_startup_driver(&env.address.to_string())
        .await
        .unwrap();
    let shutting_down = tokio::spawn(async move {
        driver
            .driver(DriverRequest {
                command: CMD_SHUTDOWN,
                config_json: String::new(),
            })
            .await
            .unwrap()
            .into_inner()
    });
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result(
            r#"{"accepted":false,"detail":"refused"}"#,
            false,
        ))
        .unwrap();
    env.unknown("owned").await;
    let response = shutting_down.await.unwrap();
    assert!(!response.ok);
    assert_eq!(response.state, "error");
    let provider = env
        .service
        .atlas
        .clone()
        .query(
            atlas_pb::Kind::Service,
            "executor",
            "",
            "",
            atlas_pb::Transport::Unspecified,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(provider.state, atlas_pb::LifecycleState::StateActive as i32);
    events(stream).await;
    assert_eq!(env.active().await["count"], 1);
    let mut client = RobonixSystemExecutorExecuteStreamClient::new(env.channel.clone());
    let error = client
        .execute_stream(tokio_stream::iter(vec![ExecuteStreamInput {
            message_kind: EXECUTE_STREAM_START,
            plan_id: "new".into(),
            ..Default::default()
        }]))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::Unavailable);
    let mut complete = nodes("new", &["work"]);
    complete[0].children = vec![1];
    let error = RobonixSystemExecutorExecuteClient::new(env.channel.clone())
        .execute(Plan {
            plan_id: "new".into(),
            nodes: complete,
            ..Default::default()
        })
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::Unavailable);
    let mut driver = connect_startup_driver(&env.address.to_string())
        .await
        .unwrap();
    let retry = tokio::spawn(async move {
        driver
            .driver(DriverRequest {
                command: CMD_SHUTDOWN,
                config_json: String::new(),
            })
            .await
            .unwrap()
            .into_inner()
    });
    env.request("cancel", Some("owned"))
        .await
        .reply
        .send(mcp_result("accepted", false))
        .unwrap();
    env.confirm("owned", "CANCELED").await;
    let response = retry.await.unwrap();
    assert!(response.ok, "{}", response.error);
    assert_eq!(response.state, "terminated");
    assert_eq!(env.active().await["count"], 0);
}

#[tokio::test]
async fn start_without_nodes_can_be_cancelled_before_end() {
    let env = Environment::start(false).await;
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ExecuteStreamInput {
            message_kind: EXECUTE_STREAM_START,
            plan_id: "empty".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let stream = RobonixSystemExecutorExecuteStreamClient::new(env.channel.clone())
        .execute_stream(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    let response = cancel_response(env.cancel("empty", 500)).await;
    assert!(response.success && response.completed);
    assert!(
        events(stream)
            .await
            .last()
            .unwrap()
            .plan_complete
            .as_ref()
            .unwrap()
            .any_failed
    );
    assert_eq!(env.active().await["count"], 0);
    drop(sender);
}

#[tokio::test]
async fn shutdown_interrupts_an_execution_stream_waiting_for_start() {
    let env = Environment::start(false).await;
    let (sender, receiver) = mpsc::channel(4);
    let channel = env.channel.clone();
    let request = tokio::spawn(async move {
        RobonixSystemExecutorExecuteStreamClient::new(channel)
            .execute_stream(ReceiverStream::new(receiver))
            .await
    });
    tokio::task::yield_now().await;
    assert!(env.service.shutdown().await);
    let error = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::Unavailable);
    drop(sender);
}
