// SPDX-License-Identifier: MulanPSL-2.0

use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::executor::rtdl_event::RtdlEventEnum;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, Plan, RtdlNode};
use crate::planner::ExecutorConn;
use futures_util::future::join_all;
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::Duration;
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;

const RTDL_DO: u32 = 2;
const SCENE_CONTEXT: &str = "robonix/system/scene/get_robot_context";
const SCENE_REGIONS: &str = "robonix/system/scene/list_regions";
const SCENE_FIND: &str = "robonix/system/scene/find";
const SCENE_GOAL: &str = "robonix/system/scene/goal_region";

/// What goal_region answered for the regions of one map. A map's regions do
/// not move, so an answer is kept for as long as Scene reports that map.
#[derive(Default)]
struct Goals {
    /// The map the answers are for, as list_regions names it.
    map_id: String,
    /// goal_region's answer by region id.
    by_region: HashMap<String, Value>,
    /// Whether a task is asking for the regions still missing.
    asking: bool,
}

static GOALS: LazyLock<Mutex<Goals>> = LazyLock::new(Mutex::default);

fn goals() -> std::sync::MutexGuard<'static, Goals> {
    GOALS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The state a planner otherwise spends rounds asking for, as two prompt
/// blocks: what changes from round to round (where the robot is and the
/// objects Scene knows, by region), and the map's regions with a
/// navigate-ready goal each, which a caller shows only when it changes.
/// Without `view`, only where the robot is, and no map block.
pub async fn collect(
    executor: &ExecutorConn,
    atlas: &mut AtlasClient,
    caps: &[(String, atlas_pb::Capability)],
    view: bool,
) -> (String, String) {
    let query = |contract: &'static str, args: Value| {
        let provider = caps
            .iter()
            .find(|(_, capability)| capability.contract_id == contract)
            .map(|(provider_id, _)| provider_id.clone());
        let graph = executor.graph.clone();
        async move {
            match provider {
                Some(provider_id) => query_scene(graph, provider_id, contract, args).await,
                None => {
                    json!({"available": false, "error": format!("{contract} is not registered")})
                }
            }
        }
    };
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
    if !view {
        let scene = query(SCENE_CONTEXT, json!({})).await;
        let state = json!({"scene": scene, "provider_availability": providers});
        return (environment(&state, ""), String::new());
    }
    let (scene, regions) = tokio::join!(
        query(SCENE_CONTEXT, json!({})),
        query(SCENE_REGIONS, json!({}))
    );
    let listed = regions["state"]["regions"].as_array();
    let listed = listed.map(Vec::as_slice).unwrap_or_default();
    let rooms: Vec<&str> = listed
        .iter()
        .filter(|region| region["kind"] == "region")
        .filter_map(|region| region["name"].as_str())
        .collect();
    // An empty region matches every object, which shows the ones outside all regions.
    let find = |room: &str| {
        let args =
            json!({"label": "", "region": room, "relation": "", "anchor": "", "settle_s": 0.0});
        query(SCENE_FIND, args)
    };
    let (everything, by_room) =
        tokio::join!(find(""), join_all(rooms.iter().map(|room| find(room))));
    // Scene answers goal_region one call at a time and a map has dozens of
    // regions, so they are asked for in the background, once, and a round
    // shows the answers that have come in.
    let known = {
        let map_id = regions["state"]["map_id"].as_str().unwrap_or_default();
        let mut cache = goals();
        if cache.map_id != map_id {
            *cache = Goals {
                map_id: map_id.into(),
                ..Default::default()
            };
        }
        let ids = listed.iter().filter_map(|region| region["id"].as_str());
        let missing: Vec<String> = ids
            .filter(|id| !cache.by_region.contains_key(*id))
            .map(String::from)
            .collect();
        let provider = caps
            .iter()
            .find(|(_, capability)| capability.contract_id == SCENE_GOAL);
        if let (false, false, Some((provider_id, _))) = (cache.asking, missing.is_empty(), provider)
        {
            cache.asking = true;
            let (graph, provider_id, map_id) = (
                executor.graph.clone(),
                provider_id.clone(),
                map_id.to_string(),
            );
            tokio::spawn(async move {
                for id in missing {
                    let args = json!({"region_id": id});
                    let goal =
                        query_scene(graph.clone(), provider_id.clone(), SCENE_GOAL, args).await;
                    let mut cache = goals();
                    if goal["available"] == true && cache.map_id == map_id {
                        cache.by_region.insert(id, rounded(&goal["state"]));
                    }
                }
                goals().asking = false;
            });
        }
        cache.by_region.clone()
    };
    let state = json!({
        "scene": scene,
        "objects": by_region(&everything, &rooms, &by_room),
        "provider_availability": providers,
    });
    let objects = "`objects` are the ids and labels of every object Scene currently knows, by \
                   the region it is in, as Scene's find returns them: call find only for what \
                   they do not show, or with `settle_s` in a tree that has just moved the \
                   robot, because perception takes a few seconds to add what the camera then \
                   sees. ";
    let map = format!(
        "\n\n## Map regions\n\
         The map's regions and points of interest as Scene's list_regions returns \
         them, without their geometry. A region's `goal` is what goal_region answered \
         for it on this map, ready to pass to navigate, and is absent where goal_region \
         found no reachable pose or has not answered yet. Call list_regions or \
         goal_region only for what this does not show. Shown again only when it \
         changes.\n\n{}\n",
        named(&regions, &known)
    );
    (environment(&state, objects), map)
}

/// The environment block around `state`, with `objects` saying what its objects are.
fn environment(state: &Value, objects: &str) -> String {
    format!(
        "\n\n## Current environment and provider state\n\
         Refreshed immediately before this planning round. Scene owns map pose, \
         room membership, areas, and nearby objects. {objects}Atlas state only indicates \
         provider availability; current task progress remains in the in-flight RTDL \
         tree block. Missing or stale state means unknown.\n\n{}\n",
        serde_json::to_string(state).unwrap_or_else(|_| "{}".into())
    )
}

/// `value` with its numbers to the millimetre, which a prompt has no use beyond.
fn rounded(value: &Value) -> Value {
    match value {
        Value::Number(number) if number.is_f64() => {
            let rounded = (number.as_f64().unwrap_or_default() * 1000.0).round() / 1000.0;
            json!(rounded)
        }
        Value::Array(items) => items.iter().map(rounded).collect(),
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| (key.clone(), rounded(value)))
            .collect::<serde_json::Map<_, _>>()
            .into(),
        other => other.clone(),
    }
}

/// What `list_regions` returned, cut to what choosing a destination needs,
/// with each region's goal: a deployment's map can hold many polygons, and
/// every round would resend them.
fn named(listed: &Value, goals: &HashMap<String, Value>) -> Value {
    let Some(regions) = listed["state"]["regions"].as_array() else {
        return listed.clone();
    };
    let named = |region: &Value| {
        let mut named = json!({"id": region["id"], "kind": region["kind"], "name": region["name"]});
        if region["stale"] == true {
            named["stale"] = true.into();
        }
        if let Some(goal) = region["id"].as_str().and_then(|id| goals.get(id))
            && goal["reachable"] == true
        {
            named["goal"] = goal["goal"].clone();
        }
        named
    };
    regions.iter().map(named).collect()
}

/// The ids and labels of the objects `find` returned for each of `rooms`, and
/// under "elsewhere" those of `everything` in none of them.
fn by_region(everything: &Value, rooms: &[&str], by_room: &[Value]) -> Value {
    let objects = |found: &Value| -> Option<Vec<Value>> {
        let objects = found["state"]["objects"].as_array()?;
        let known = objects.iter().filter(|object| object["label"] != "robot");
        Some(
            known
                .map(|object| json!({"id": object["id"], "label": object["label"]}))
                .collect(),
        )
    };
    let Some(mut elsewhere) = objects(everything) else {
        return everything.clone();
    };
    let mut regions = serde_json::Map::new();
    for (room, found) in rooms.iter().zip(by_room) {
        let inside = objects(found).unwrap_or_default();
        elsewhere.retain(|object| !inside.contains(object));
        regions.insert(room.to_string(), inside.into());
    }
    regions.insert("elsewhere".into(), elsewhere.into());
    regions.into()
}

async fn query_scene(
    mut graph: RobonixSystemExecutorExecuteClient<Channel>,
    provider_id: String,
    contract: &str,
    args: Value,
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
                args_json: args.to_string(),
            }),
            op_id: "scene_snapshot".into(),
            description: format!("Read {contract} before planning"),
            ..Default::default()
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
    use super::{by_region, lifecycle_name, named, rounded};
    use robonix_atlas::pb::LifecycleState;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn lifecycle_labels_are_stable() {
        assert_eq!(lifecycle_name(LifecycleState::StateActive as i32), "active");
        assert_eq!(lifecycle_name(LifecycleState::StateError as i32), "error");
    }

    #[test]
    fn listed_regions_keep_their_identity_and_gain_a_reachable_goal() {
        let listed = json!({"available": true, "state": {"map_id": "m", "regions": [
            {"id": "scene.region.a", "kind": "region", "name": "kitchen",
             "points_xy": [0.0, 0.0, 1.0, 0.0, 1.0, 1.0], "stale": false},
            {"id": "scene.poi.b", "kind": "poi", "name": "sink", "points_xy": [0.5, 0.5],
             "stale": true, "stale_reason": "map epoch changed"},
        ]}});
        let goals = HashMap::from([
            (
                "scene.region.a".to_string(),
                json!({"reachable": true, "x": 0.5, "goal": {"header": {"frame_id": "map"}}}),
            ),
            (
                "scene.poi.b".to_string(),
                json!({"reachable": false, "goal": {"header": {"frame_id": ""}}}),
            ),
        ]);
        assert_eq!(
            named(&listed, &goals),
            json!([
                {"id": "scene.region.a", "kind": "region", "name": "kitchen",
                 "goal": {"header": {"frame_id": "map"}}},
                {"id": "scene.poi.b", "kind": "poi", "name": "sink", "stale": true},
            ])
        );
        let unavailable = json!({"available": false, "error": "timed out"});
        assert_eq!(named(&unavailable, &goals), unavailable);
    }

    #[test]
    fn numbers_are_kept_to_the_millimetre() {
        let goal = json!({"reachable": true, "x": 1.1954571252531956, "n": 3,
                          "goal": {"pose": {"orientation": [0.0, -0.7071080798594735]}}});
        assert_eq!(
            rounded(&goal),
            json!({"reachable": true, "x": 1.195, "n": 3,
                   "goal": {"pose": {"orientation": [0.0, -0.707]}}})
        );
    }

    #[test]
    fn objects_are_listed_under_the_region_find_returned_them_for() {
        let found =
            |objects: serde_json::Value| json!({"available": true, "state": {"objects": objects}});
        let object = |id: &str, label: &str| json!({"id": id, "label": label, "x": 1.0});
        let everything = found(json!([
            object("o1", "cup"),
            object("o2", "robot"),
            object("o3", "sofa"),
            object("o4", "bowl")
        ]));
        let kitchen = found(json!([object("o4", "bowl"), object("o1", "cup")]));
        assert_eq!(
            by_region(
                &everything,
                &["kitchen", "bedroom"],
                &[kitchen, found(json!([]))]
            ),
            json!({
                "kitchen": [{"id": "o4", "label": "bowl"}, {"id": "o1", "label": "cup"}],
                "bedroom": [],
                "elsewhere": [{"id": "o3", "label": "sofa"}],
            })
        );
        let unavailable = json!({"available": false, "error": "timed out"});
        assert_eq!(by_region(&unavailable, &[], &[]), unavailable);
    }
}
