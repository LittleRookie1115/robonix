// SPDX-License-Identifier: MulanPSL-2.0
// Author: wheatfox <wheatfox17@icloud.com>
//
// robonix-executor — capability-call dispatch runtime.
// On startup executor:
//   1. Connects to atlas, registers as `com.robonix.system.executor`.
//   2. Declares its gRPC Execute and CancelAllPlans capabilities.
//   3. Declares built-in capabilities under `robonix/system/executor/builtin/<op>`
//      so pilot's atlas-driven discovery surfaces them to the LLM as plain
//      capabilities. Calls hitting these contracts short-circuit to in-process
//      handlers in `dispatch::builtin` — no MCP loopback.
//   4. Serves Execute on `listen`. Per-call dispatch resolves provider via
//      `ConnectCapability(provider_id, contract_id, MCP)` on atlas.

mod config;
mod dispatch;
mod pb;
mod plan_runtime;
mod rtdl_wire;
mod service;
mod verification;

use anyhow::{Context, Result};
use clap::Parser;
use config::{Args, EXECUTOR_NAMESPACE, ExecutorConfig};
use dispatch::builtin::BUILTINS;
use pb::contracts::robonix_lifecycle_driver_client::RobonixLifecycleDriverClient;
use pb::contracts::robonix_lifecycle_driver_server::{
    RobonixLifecycleDriver, RobonixLifecycleDriverServer,
};
use pb::contracts::robonix_system_executor_cancel_all_plans_server::RobonixSystemExecutorCancelAllPlansServer;
use pb::contracts::robonix_system_executor_control_plan_server::RobonixSystemExecutorControlPlanServer;
use pb::contracts::robonix_system_executor_execute_server::RobonixSystemExecutorExecuteServer;
use pb::contracts::robonix_system_executor_get_health_server::RobonixSystemExecutorGetHealthServer;
use pb::contracts::robonix_system_executor_list_active_plans_server::RobonixSystemExecutorListActivePlansServer;
use pb::lifecycle::{DriverRequest, DriverResponse};
use robonix_atlas::client::{self as atlas_client, AtlasClient};
use robonix_atlas::pb as atlas_pb;
use robonix_scribe::{info, warn};
use service::ExecutorServiceImpl;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

const SHARED_DRIVER_CONTRACT: &str = "robonix/lifecycle/driver";
const CMD_INIT: u32 = 0;
const CMD_ACTIVATE: u32 = 1;
const CMD_DEACTIVATE: u32 = 2;
const CMD_SHUTDOWN: u32 = 3;

#[derive(Clone)]
struct SystemLifecycleDriver {
    atlas: AtlasClient,
    provider_id: String,
    executor: ExecutorServiceImpl,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl SystemLifecycleDriver {
    /// Share lifecycle control and shutdown signaling with the executor service.
    fn new(atlas: AtlasClient, provider_id: String, executor: ExecutorServiceImpl) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            atlas,
            provider_id,
            executor,
            shutdown_tx,
        }
    }

    fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Publish legal lifecycle transitions and drain accepted work on shutdown.
    async fn transition(&self, command: u32) -> Result<&'static str> {
        let (state, label) = match command {
            CMD_INIT | CMD_DEACTIVATE => (atlas_pb::LifecycleState::StateInactive, "inactive"),
            CMD_ACTIVATE => (atlas_pb::LifecycleState::StateActive, "active"),
            CMD_SHUTDOWN => (atlas_pb::LifecycleState::StateTerminated, "terminated"),
            _ => anyhow::bail!("unknown lifecycle command code {command}"),
        };
        if command == CMD_SHUTDOWN && !self.executor.shutdown().await {
            anyhow::bail!("Executor plans did not finish cancellation before shutdown");
        }
        let mut atlas = self.atlas.clone();
        atlas
            .set_lifecycle_state(&self.provider_id, state, "")
            .await
            .context("publish Executor lifecycle state")?;
        if command == CMD_SHUTDOWN {
            self.shutdown_tx.send_replace(true);
        }
        Ok(label)
    }
}

#[tonic::async_trait]
impl RobonixLifecycleDriver for SystemLifecycleDriver {
    /// Return transition failures through the shared Driver response envelope.
    async fn driver(
        &self,
        request: Request<DriverRequest>,
    ) -> std::result::Result<Response<DriverResponse>, Status> {
        let response = match self.transition(request.into_inner().command).await {
            Ok(state) => DriverResponse {
                ok: true,
                state: state.to_string(),
                error: String::new(),
            },
            Err(error) => DriverResponse {
                ok: false,
                state: "error".to_string(),
                error: format!("{error:#}"),
            },
        };
        Ok(Response::new(response))
    }
}

/// Observe shutdown even when it was requested before the waiter started.
async fn wait_for_driver_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

/// Dial unspecified bind addresses through the matching loopback address.
fn startup_driver_endpoint(listen_addr: std::net::SocketAddr) -> String {
    let ip = match listen_addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        }
        ip => ip,
    };
    format!(
        "http://{}",
        std::net::SocketAddr::new(ip, listen_addr.port())
    )
}

/// Bound retries to the local server's startup window.
async fn connect_startup_driver(
    endpoint: &str,
) -> Result<RobonixLifecycleDriverClient<tonic::transport::Channel>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match RobonixLifecycleDriverClient::connect(endpoint.to_string()).await {
            Ok(client) => return Ok(client),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error).context("connect Executor lifecycle Driver"),
        }
    }
}

/// Perform one real startup Driver RPC and reject unsuccessful transitions.
async fn call_startup_driver(
    driver: &mut RobonixLifecycleDriverClient<tonic::transport::Channel>,
    command: u32,
) -> Result<String> {
    let response = driver
        .driver(DriverRequest {
            command,
            config_json: "{}".to_string(),
        })
        .await?
        .into_inner();
    if !response.ok {
        anyhow::bail!("Executor lifecycle Driver failed: {}", response.error);
    }
    Ok(response.state)
}

#[tokio::main]
async fn main() -> Result<()> {
    let parsed = Args::parse();
    // Apply the manifest's per-component `log:` level (delivered inside
    // --config-json) to scribe's file sink before the first log line.
    robonix_scribe::init_from_config("executor", parsed.config_json.as_deref());
    info!("robonix-executor starting");

    let cfg = ExecutorConfig::resolve(parsed)?;

    info!("connecting to atlas at {}", cfg.atlas_endpoint);
    let mut atlas =
        AtlasClient::connect_with_retry(&cfg.atlas_endpoint, 10, Duration::from_secs(2))
            .await
            .context("connect to atlas")?;

    atlas
        .register_service(&cfg.id, EXECUTOR_NAMESPACE, "")
        .await?;
    info!("registered as '{}' under '{EXECUTOR_NAMESPACE}'", cfg.id);

    let listen_addr: std::net::SocketAddr = cfg
        .listen
        .parse()
        .with_context(|| format!("invalid executor listen address '{}'", cfg.listen))?;
    let advertised = match listen_addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            format!("127.0.0.1:{}", listen_addr.port())
        }
        _ => listen_addr.to_string(),
    };

    // Execute RPC: pilot → executor for plan dispatch.
    atlas
        .declare_capability(
            &cfg.id,
            "robonix/system/executor/execute",
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/system/executor/execute.v1.toml",
                "robonix.contracts.RobonixSystemExecutorExecute",
                "/robonix.contracts.RobonixSystemExecutorExecute/Execute",
            ),
        )
        .await?;

    // Out-of-band RTDL meta operations. These never enter PlanRuntime as a
    // new plan, so canceling work cannot create a self-referential cancel tree.
    atlas
        .declare_capability(
            &cfg.id,
            "robonix/system/executor/control_plan",
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/system/executor/control_plan.v1.toml",
                "robonix.contracts.RobonixSystemExecutorControlPlan",
                "/robonix.contracts.RobonixSystemExecutorControlPlan/ControlPlan",
            ),
        )
        .await?;

    // Read-only control path for clients and observability. Polling it must not
    // create an RTDL query plan of its own.
    atlas
        .declare_capability(
            &cfg.id,
            "robonix/system/executor/list_active_plans",
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/system/executor/list_active_plans.v1.toml",
                "robonix.contracts.RobonixSystemExecutorListActivePlans",
                "/robonix.contracts.RobonixSystemExecutorListActivePlans/ListActivePlans",
            ),
        )
        .await?;

    // CancelAllPlans RPC: control path for cancelling every active RTDL plan.
    atlas
        .declare_capability(
            &cfg.id,
            "robonix/system/executor/cancel_all_plans",
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/system/executor/cancel_all_plans.v1.toml",
                "robonix.contracts.RobonixSystemExecutorCancelAllPlans",
                "/robonix.contracts.RobonixSystemExecutorCancelAllPlans/CancelAll",
            ),
        )
        .await?;

    // Module health RPC: Vitals polls this for system-module health.
    atlas
        .declare_capability(
            &cfg.id,
            "robonix/system/executor/get_health",
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/system/executor/get_health.toml",
                "robonix.contracts.RobonixSystemExecutorGetHealth",
                "/robonix.contracts.RobonixSystemExecutorGetHealth/GetModuleHealth",
            ),
        )
        .await?;

    // Built-in capabilities: declared as MCP-transport capabilities so pilot's
    // catalog discovery sees them like any user MCP provider. The endpoint is a
    // sentinel — dispatch never dials it; calls hitting these contracts hit
    // the provider_id == self short-circuit in `dispatch::dispatch`.
    let builtin_endpoint = format!("internal://{}/builtin", cfg.id);
    for spec in BUILTINS {
        let contract_id = format!("{EXECUTOR_NAMESPACE}/builtin/{}", spec.op);
        atlas
            .declare_capability_with_description(
                &cfg.id,
                &contract_id,
                atlas_pb::Transport::Mcp,
                &builtin_endpoint,
                atlas_client::mcp_params(spec.input_schema_json),
                spec.description,
            )
            .await
            .with_context(|| format!("declare builtin '{}'", contract_id))?;
    }
    info!(
        "declared executor gRPC capabilities + {} builtin capabilities at {advertised}",
        BUILTINS.len()
    );

    atlas
        .declare_capability(
            &cfg.id,
            SHARED_DRIVER_CONTRACT,
            atlas_pb::Transport::Grpc,
            &advertised,
            atlas_client::grpc_params(
                "capabilities/lifecycle/driver.v1.toml",
                "robonix.contracts.RobonixLifecycleDriver",
                "/robonix.contracts.RobonixLifecycleDriver/Driver",
            ),
        )
        .await?;

    // Atlas evicts providers after ~60s without a heartbeat. Send one every
    // 20s so we stay registered for the lifetime of the process.
    {
        let mut hb = atlas.clone();
        let provider_id = cfg.id.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(20));
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = hb.heartbeat(&provider_id).await {
                    warn!("heartbeat failed: {e:#}");
                }
            }
        });
    }

    let verification = Arc::new(verification::VerificationPolicy::new(
        cfg.verification.overlap,
        cfg.verification.rules,
    ));
    info!(
        "loaded {} executor verification rule(s)",
        verification.len()
    );
    let svc = ExecutorServiceImpl::new(atlas.clone(), cfg.id.clone(), verification);
    let lifecycle = SystemLifecycleDriver::new(atlas, cfg.id.clone(), svc.clone());
    let shutdown = lifecycle.subscribe_shutdown();
    info!("executor gRPC on {listen_addr}");
    let mut server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RobonixLifecycleDriverServer::new(lifecycle))
            .add_service(RobonixSystemExecutorExecuteServer::new(svc.clone()))
            .add_service(RobonixSystemExecutorCancelAllPlansServer::new(svc.clone()))
            .add_service(RobonixSystemExecutorControlPlanServer::new(svc.clone()))
            .add_service(RobonixSystemExecutorListActivePlansServer::new(svc.clone()))
            .add_service(RobonixSystemExecutorGetHealthServer::new(svc))
            .serve_with_shutdown(listen_addr, wait_for_driver_shutdown(shutdown))
            .await
    });
    let startup_endpoint = startup_driver_endpoint(listen_addr);
    let mut startup_driver = tokio::select! {
        client = connect_startup_driver(&startup_endpoint) => client?,
        result = &mut server_task => {
            result.context("join Executor gRPC server")?
                .context("Executor gRPC server failed before readiness")?;
            anyhow::bail!("Executor gRPC server stopped before readiness");
        }
    };
    call_startup_driver(&mut startup_driver, CMD_INIT)
        .await
        .context("initialize Executor lifecycle")?;
    call_startup_driver(&mut startup_driver, CMD_ACTIVATE)
        .await
        .context("activate Executor lifecycle")?;
    drop(startup_driver);
    info!("robonix-executor ready on {listen_addr}");

    server_task
        .await
        .context("join Executor gRPC server")?
        .context("executor gRPC server failed")?;

    Ok(())
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use robonix_atlas::service::{AtlasRegistry, serve_atlas};

    fn reserve_address() -> std::net::SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    #[test]
    /// Startup uses loopback when the server binds all IPv4 or IPv6 interfaces.
    fn startup_driver_dials_loopback_for_unspecified_addresses() {
        assert_eq!(
            startup_driver_endpoint("0.0.0.0:50061".parse().unwrap()),
            "http://127.0.0.1:50061"
        );
        assert_eq!(
            startup_driver_endpoint("[::]:50061".parse().unwrap()),
            "http://[::1]:50061"
        );
    }

    #[tokio::test]
    /// Exercise legal and rejected Driver transitions against a real Atlas.
    async fn shared_driver_rpc_publishes_lifecycle_and_shuts_down_server() {
        let atlas_addr = reserve_address();
        let atlas_server =
            tokio::spawn(serve_atlas(Arc::new(AtlasRegistry::default()), atlas_addr));
        let mut atlas = AtlasClient::connect_with_retry(
            format!("http://{atlas_addr}"),
            50,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let provider_id = "executor-driver-test";
        atlas
            .register_service(provider_id, EXECUTOR_NAMESPACE, "")
            .await
            .unwrap();
        let driver_addr = reserve_address();
        atlas
            .declare_capability(
                provider_id,
                SHARED_DRIVER_CONTRACT,
                atlas_pb::Transport::Grpc,
                &driver_addr.to_string(),
                atlas_client::grpc_params(
                    "capabilities/lifecycle/driver.v1.toml",
                    "robonix.contracts.RobonixLifecycleDriver",
                    "/robonix.contracts.RobonixLifecycleDriver/Driver",
                ),
            )
            .await
            .unwrap();
        let service = ExecutorServiceImpl::new(
            atlas.clone(),
            provider_id.to_string(),
            Arc::new(verification::VerificationPolicy::new(false, Vec::new())),
        );
        let lifecycle = SystemLifecycleDriver::new(atlas.clone(), provider_id.to_string(), service);
        let shutdown = lifecycle.subscribe_shutdown();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RobonixLifecycleDriverServer::new(lifecycle))
                .serve_with_shutdown(driver_addr, wait_for_driver_shutdown(shutdown)),
        );
        let mut client = connect_startup_driver(&startup_driver_endpoint(driver_addr))
            .await
            .unwrap();
        assert!(call_startup_driver(&mut client, 99).await.is_err());
        assert!(
            call_startup_driver(&mut client, CMD_ACTIVATE)
                .await
                .is_err()
        );
        assert_eq!(
            call_startup_driver(&mut client, CMD_INIT).await.unwrap(),
            "inactive"
        );
        assert_eq!(
            call_startup_driver(&mut client, CMD_ACTIVATE)
                .await
                .unwrap(),
            "active"
        );
        let providers = atlas
            .query(
                atlas_pb::Kind::Service,
                provider_id,
                "",
                "",
                atlas_pb::Transport::Unspecified,
            )
            .await
            .unwrap();
        assert_eq!(
            providers[0].state,
            atlas_pb::LifecycleState::StateActive as i32
        );
        assert_eq!(
            call_startup_driver(&mut client, CMD_DEACTIVATE)
                .await
                .unwrap(),
            "inactive"
        );
        assert_eq!(
            call_startup_driver(&mut client, CMD_ACTIVATE)
                .await
                .unwrap(),
            "active"
        );
        assert_eq!(
            call_startup_driver(&mut client, CMD_SHUTDOWN)
                .await
                .unwrap(),
            "terminated"
        );
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        atlas_server.abort();
    }
}
