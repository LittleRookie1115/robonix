// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_system_soma_get_yaml_client::RobonixSystemSomaGetYamlClient;
use crate::pb::soma::GetYamlRequest;
use anyhow::{Context, Result, ensure};
use robonix_atlas::{client::AtlasClient, pb::Transport};
use std::time::Duration;
use tonic::transport::Endpoint;

/// Retain the Atlas binding before dialing and release it on every RPC outcome.
pub async fn fetch_yaml(atlas: &mut AtlasClient, consumer_id: &str) -> Result<String> {
    let timeout = Duration::from_secs(2);
    let capabilities = tokio::time::timeout(
        timeout,
        atlas.flatten_capabilities("robonix/system/soma/get_yaml", "", Transport::Grpc),
    )
    .await
    .context("Soma body discovery timed out")??;
    ensure!(
        capabilities.len() == 1,
        "expected one Soma body provider, found {}",
        capabilities.len()
    );
    let capability = &capabilities[0];
    let (channel_id, endpoint, _) = tokio::time::timeout(
        timeout,
        atlas.connect_capability(
            consumer_id,
            &capability.provider_id,
            "robonix/system/soma/get_yaml",
            Transport::Grpc,
        ),
    )
    .await
    .context("Soma body binding timed out")??;
    let result = tokio::time::timeout(timeout, async {
        let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint
        } else {
            format!("http://{endpoint}")
        };
        let channel = Endpoint::new(endpoint)?
            .connect_timeout(timeout)
            .connect()
            .await?;
        let response = RobonixSystemSomaGetYamlClient::new(channel)
            .get_yaml(GetYamlRequest {
                robot_id: String::new(),
            })
            .await?
            .into_inner();
        Ok::<_, anyhow::Error>(response.yaml_text)
    })
    .await
    .context("Soma body lookup timed out")
    .and_then(|result| result);
    match tokio::time::timeout(timeout, atlas.disconnect_capability(&channel_id)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => log::warn!("Soma body channel cleanup failed: {error:#}"),
        Err(error) => log::warn!("Soma body channel cleanup timed out: {error}"),
    }
    result
}
