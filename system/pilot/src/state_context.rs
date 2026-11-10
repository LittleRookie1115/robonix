// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::executor::rtdl_event::RtdlEventEnum;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, Plan, RtdlNode};
use crate::planner::ExecutorConn;
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use serde_json::{Value, json};
use std::time::Duration;
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;

const RTDL_DO: u32 = 2;
const SCENE_CONTEXT: &str = "robonix/system/scene/get_robot_context";
const SCENE_REGIONS: &str = "robonix/system/scene/list_regions";

pub async fn collect(
    executor: &ExecutorConn,
    atlas: &mut AtlasClient,
    caps: &[(String, atlas_pb::Capability)],
) -> String {
    let query = |contract: &'static str| {
        let provider = caps
            .iter()
            .find(|(_, capability)| capability.contract_id == contract)
            .map(|(provider_id, _)| provider_id.clone());
        let graph = executor.graph.clone();
        async move {
            match provider {
                Some(provider_id) => query_scene(graph, provider_id, contract).await,
                None => {
                    json!({"available": false, "error": format!("{contract} is not registered")})
                }
            }
        }
    };
    let (scene, regions) = tokio::join!(query(SCENE_CONTEXT), query(SCENE_REGIONS));
    let providers = match atlas
        .query_capabilities("", "", atlas_pb::Transport::Unspecified)
        .await
    {
        Ok(rows) => rows
            .into_iter()
            .filter(|provider| {
                provider.capabilities.iter().any(|capability| {
                    let id = capability.contract_id.as_str();
                    id.contains("/navigation/")
                        || id.contains("/scene/")
                        || id.contains("/arm/")
                        || id.contains("/chassis/")
                        || id.contains("/skill/")
                })
            })
            .map(|provider| {
                json!({
                    "provider_id": provider.id,
                    "state": lifecycle_name(provider.state),
                    "detail": provider.state_detail,
                })
            })
            .collect::<Vec<_>>(),
        Err(error) => vec![json!({"available": false, "error": error.to_string()})],
    };
    format!(
        "\n\n## Current environment and provider state\n\
         Refreshed immediately before this planning round. Scene owns map pose, \
         room membership, areas, and nearby objects; `regions` are the map's regions \
         and points of interest as Scene's list_regions returns them, without their \
         geometry. Atlas state only indicates \
         provider availability; current task progress remains in the in-flight RTDL \
         tree block. Missing or stale state means unknown.\n\n{}\n",
        serde_json::to_string(&json!({
            "scene": scene,
            "regions": named(&regions),
            "provider_availability": providers,
        }))
        .unwrap_or_else(|_| "{}".into())
    )
}

/// What `list_regions` returned, cut to what choosing a destination needs:
/// a deployment's map can hold many polygons, and every round would resend them.
fn named(listed: &Value) -> Value {
    let Some(regions) = listed["state"]["regions"].as_array() else {
        return listed.clone();
    };
    let named = |region: &Value| {
        let mut named = json!({"id": region["id"], "kind": region["kind"], "name": region["name"]});
        if region["stale"] == true {
            named["stale"] = true.into();
        }
        named
    };
    regions.iter().map(named).collect()
}

async fn query_scene(
    mut graph: RobonixSystemExecutorExecuteClient<Channel>,
    provider_id: String,
    contract: &str,
) -> Value {
    let plan_id = format!("state-prefetch-{}", Uuid::new_v4());
    let plan = Plan {
        plan_id: plan_id.clone(),
        session_id: "pilot-state-prefetch".into(),
        round: 0,
        nodes: vec![RtdlNode {
            node_kind: RTDL_DO,
            children: Vec::new(),
            call: Some(CapabilityCall {
                call_id: format!("{plan_id}:0"),
                provider_id,
                contract_id: contract.into(),
                args_json: "{}".into(),
            }),
            op_id: "scene_snapshot".into(),
            description: format!("Read {contract} before planning"),
        }],
        root_index: 0,
    };
    let query = async {
        let mut stream = graph
            .execute(Request::new(plan))
            .await
            .map_err(|error| error.to_string())?
            .into_inner();
        while let Some(event) = stream.message().await.map_err(|error| error.to_string())? {
            if event.event_kind != RtdlEventEnum::NodeState as u32 {
                continue;
            }
            let Some(state) = event.node_state else {
                continue;
            };
            if state.state == RtdlNodeStateEnum::Succeeded as u32 {
                let output = state
                    .leaf_result
                    .map(|result| result.output)
                    .unwrap_or(state.operator_detail);
                return Ok(serde_json::from_str(&output).unwrap_or_else(|_| json!({"raw": output})));
            }
            if matches!(
                RtdlNodeStateEnum::try_from(state.state as i32),
                Ok(RtdlNodeStateEnum::Failed
                    | RtdlNodeStateEnum::Canceled
                    | RtdlNodeStateEnum::Timeout)
            ) {
                return Err(state.operator_detail);
            }
        }
        Err("Scene query ended without a terminal result".into())
    };
    match tokio::time::timeout(Duration::from_secs(3), query).await {
        Ok(Ok(value)) => json!({"available": true, "state": value}),
        Ok(Err(error)) => json!({"available": false, "error": error}),
        Err(_) => json!({"available": false, "error": "Scene query timed out after 3 seconds"}),
    }
}

fn lifecycle_name(state: i32) -> &'static str {
    match atlas_pb::LifecycleState::try_from(state) {
        Ok(atlas_pb::LifecycleState::StateRegistered) => "registered",
        Ok(atlas_pb::LifecycleState::StateInactive) => "inactive",
        Ok(atlas_pb::LifecycleState::StateActive) => "active",
        Ok(atlas_pb::LifecycleState::StateError) => "error",
        Ok(atlas_pb::LifecycleState::StateTerminated) => "terminated",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{lifecycle_name, named};
    use robonix_atlas::pb::LifecycleState;
    use serde_json::json;

    #[test]
    fn listed_regions_keep_their_identity_and_lose_their_geometry() {
        let listed = json!({"available": true, "state": {"map_id": "m", "regions": [
            {"id": "scene.region.a", "kind": "region", "name": "kitchen",
             "points_xy": [0.0, 0.0, 1.0, 0.0, 1.0, 1.0], "stale": false},
            {"id": "scene.poi.b", "kind": "poi", "name": "sink", "points_xy": [0.5, 0.5],
             "stale": true, "stale_reason": "map epoch changed"},
        ]}});
        assert_eq!(
            named(&listed),
            json!([
                {"id": "scene.region.a", "kind": "region", "name": "kitchen"},
                {"id": "scene.poi.b", "kind": "poi", "name": "sink", "stale": true},
            ])
        );
        let unavailable = json!({"available": false, "error": "timed out"});
        assert_eq!(named(&unavailable), unavailable);
    }

    #[test]
    fn lifecycle_labels_are_stable() {
        assert_eq!(lifecycle_name(LifecycleState::StateActive as i32), "active");
        assert_eq!(lifecycle_name(LifecycleState::StateError as i32), "error");
    }
}
