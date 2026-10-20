// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_system_soma_get_yaml_client::RobonixSystemSomaGetYamlClient;
use crate::pb::soma::GetYamlRequest;
use anyhow::{Context, Result};
use log::warn;
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb::Transport;
use std::time::Duration;
use tonic::transport::Endpoint;

const GET_YAML_CONTRACT: &str = "robonix/system/soma/get_yaml";

/// Retain the binding before dialing so connection and lookup failures release it.
pub async fn fetch_yaml(atlas: &mut AtlasClient, consumer_id: &str) -> Result<String> {
    let timeout = Duration::from_secs(2);
    let capabilities = tokio::time::timeout(
        timeout,
        atlas.flatten_capabilities(GET_YAML_CONTRACT, "", Transport::Grpc),
    )
    .await
    .context("Soma body discovery timed out")?
    .context("discover Soma get_yaml")?;
    let capability = capabilities
        .first()
        .context("no provider offers Soma get_yaml over gRPC")?;
    if capabilities.len() > 1 {
        warn!(
            "[pilot/soma] multiple get_yaml providers; picking first ('{}')",
            capability.provider_id
        );
    }
    let (channel_id, endpoint, _) = tokio::time::timeout(
        timeout,
        atlas.connect_capability(
            consumer_id,
            &capability.provider_id,
            GET_YAML_CONTRACT,
            Transport::Grpc,
        ),
    )
    .await
    .context("Soma body binding timed out")?
    .context("bind Soma get_yaml")?;
    let result = tokio::time::timeout(timeout, async {
        let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint
        } else {
            format!("http://{endpoint}")
        };
        let channel = Endpoint::new(endpoint)
            .context("invalid Soma body endpoint")?
            .connect_timeout(timeout)
            .connect()
            .await
            .context("connect Soma body endpoint")?;
        let mut client = RobonixSystemSomaGetYamlClient::new(channel);
        let response = client
            .get_yaml(GetYamlRequest {
                robot_id: String::new(),
            })
            .await
            .context("call Soma get_yaml")?
            .into_inner();
        Ok::<_, anyhow::Error>(response.yaml_text)
    })
    .await
    .context("Soma body lookup timed out")
    .and_then(|result| result);
    match tokio::time::timeout(timeout, atlas.disconnect_capability(&channel_id)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => warn!("[pilot/soma] body channel cleanup failed: {error:#}"),
        Err(error) => warn!("[pilot/soma] body channel cleanup timed out: {error}"),
    }
    result
}
