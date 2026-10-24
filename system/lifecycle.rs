// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_lifecycle_driver_client::RobonixLifecycleDriverClient;
use crate::pb::contracts::robonix_lifecycle_driver_server::RobonixLifecycleDriver;
use crate::pb::lifecycle::{DriverRequest, DriverResponse};
use anyhow::{Context, Result};
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

pub(crate) const SHARED_DRIVER_CONTRACT: &str = "robonix/lifecycle/driver";
pub(crate) const CMD_INIT: u32 = 0;
pub(crate) const CMD_ACTIVATE: u32 = 1;
pub(crate) const CMD_DEACTIVATE: u32 = 2;
pub(crate) const CMD_SHUTDOWN: u32 = 3;

type ShutdownCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct SystemLifecycleDriver {
    atlas: AtlasClient,
    provider_id: String,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    on_shutdown: Option<ShutdownCallback>,
}

impl SystemLifecycleDriver {
    pub(crate) fn new(atlas: AtlasClient, provider_id: String) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            atlas,
            provider_id,
            shutdown_tx,
            on_shutdown: None,
        }
    }

    #[allow(dead_code, reason = "Only Executor installs a shutdown callback.")]
    pub(crate) fn with_shutdown<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_shutdown = Some(Arc::new(move || Box::pin(callback())));
        self
    }

    pub(crate) async fn transition(&self, command: u32) -> Result<&'static str> {
        let (state, label) = match command {
            CMD_INIT | CMD_DEACTIVATE => (atlas_pb::LifecycleState::StateInactive, "inactive"),
            CMD_ACTIVATE => (atlas_pb::LifecycleState::StateActive, "active"),
            CMD_SHUTDOWN => (atlas_pb::LifecycleState::StateTerminated, "terminated"),
            _ => anyhow::bail!("unknown lifecycle command code {command}"),
        };
        if command == CMD_SHUTDOWN
            && let Some(callback) = &self.on_shutdown
        {
            callback().await?;
        }
        let mut atlas = self.atlas.clone();
        atlas
            .set_lifecycle_state(&self.provider_id, state, "")
            .await
            .with_context(|| format!("publish lifecycle state for '{}'", self.provider_id))?;
        if command == CMD_SHUTDOWN {
            self.shutdown_tx.send_replace(true);
        }
        Ok(label)
    }

    pub(crate) fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

#[tonic::async_trait]
impl RobonixLifecycleDriver for SystemLifecycleDriver {
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

pub(crate) async fn wait_for_driver_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

pub(crate) fn startup_driver_endpoint(listen_addr: std::net::SocketAddr) -> String {
    let ip = match listen_addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        }
        ip => ip,
    };
    std::net::SocketAddr::new(ip, listen_addr.port()).to_string()
}

pub(crate) async fn connect_startup_driver(
    endpoint: &str,
) -> Result<RobonixLifecycleDriverClient<tonic::transport::Channel>> {
    let endpoint = if endpoint.starts_with("http") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match RobonixLifecycleDriverClient::connect(endpoint.clone()).await {
            Ok(client) => return Ok(client),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error).context("connect startup lifecycle Driver"),
        }
    }
}

pub(crate) async fn call_startup_driver(
    driver: &mut RobonixLifecycleDriverClient<tonic::transport::Channel>,
    command: u32,
) -> Result<String> {
    let response = driver
        .driver(DriverRequest {
            command,
            config_json: "{}".to_string(),
        })
        .await
        .context("call startup lifecycle Driver")?
        .into_inner();
    if !response.ok {
        anyhow::bail!("startup lifecycle Driver failed: {}", response.error);
    }
    Ok(response.state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::contracts::robonix_lifecycle_driver_server::RobonixLifecycleDriverServer;
    use robonix_atlas::service::{AtlasRegistry, serve_atlas};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn failed_shutdown_keeps_state_and_waiters_available_for_retry() {
        let atlas_addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let atlas_server =
            tokio::spawn(serve_atlas(Arc::new(AtlasRegistry::default()), atlas_addr));
        let mut atlas = AtlasClient::connect_with_retry(
            format!("http://{atlas_addr}"),
            50,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
        let provider_id = "shutdown-retry-test";
        atlas
            .register_service(provider_id, "robonix/system/executor", "")
            .await
            .unwrap();
        let drained = Arc::new(AtomicBool::new(false));
        let callback_drained = Arc::clone(&drained);
        let driver = SystemLifecycleDriver::new(atlas.clone(), provider_id.to_string())
            .with_shutdown(move || {
                let drained = Arc::clone(&callback_drained);
                async move {
                    anyhow::ensure!(drained.load(Ordering::SeqCst), "drain incomplete");
                    Ok(())
                }
            });
        let shutdown = driver.subscribe_shutdown();
        let driver_addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RobonixLifecycleDriverServer::new(driver.clone()))
                .serve_with_shutdown(driver_addr, wait_for_driver_shutdown(shutdown.clone())),
        );
        let mut client = connect_startup_driver(&startup_driver_endpoint(driver_addr))
            .await
            .unwrap();
        atlas
            .declare_capability(
                provider_id,
                SHARED_DRIVER_CONTRACT,
                atlas_pb::Transport::Grpc,
                &driver_addr.to_string(),
                robonix_atlas::client::grpc_params(
                    "capabilities/lifecycle/driver.v1.toml",
                    "robonix.contracts.RobonixLifecycleDriver",
                    "/robonix.contracts.RobonixLifecycleDriver/Driver",
                ),
            )
            .await
            .unwrap();
        call_startup_driver(&mut client, CMD_INIT).await.unwrap();
        call_startup_driver(&mut client, CMD_ACTIVATE)
            .await
            .unwrap();
        let response = client
            .driver(DriverRequest {
                command: CMD_SHUTDOWN,
                config_json: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!response.ok);
        assert!(response.error.contains("drain incomplete"));
        assert!(!*shutdown.borrow());
        assert!(!shutdown.has_changed().unwrap());
        assert!(!server.is_finished());
        let provider = atlas
            .query(
                atlas_pb::Kind::Service,
                provider_id,
                "",
                "",
                atlas_pb::Transport::Unspecified,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(provider.state, atlas_pb::LifecycleState::StateActive as i32);

        drained.store(true, Ordering::SeqCst);
        let response = client
            .driver(DriverRequest {
                command: CMD_SHUTDOWN,
                config_json: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(response.ok, "{}", response.error);
        assert_eq!(response.state, "terminated");
        assert!(*shutdown.borrow());
        tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_driver_shutdown(driver.subscribe_shutdown()),
        )
        .await
        .expect("late subscribers must observe shutdown");
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server must shut down after the successful retry")
            .unwrap()
            .unwrap();
        atlas_server.abort();
    }
}
