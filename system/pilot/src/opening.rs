//! The opening move. A task's first plan takes several seconds to write, and
//! most tasks begin by going somewhere. With a decision model, Pilot asks it
//! which of the map's places the task begins by going to and, if it is sure,
//! sends the robot there while the plan is written. With `handoff_moves` it does
//! the same at every later round its own planning request serves, asking where
//! the task's next step goes given the steps done so far. Navigation follows the
//! newest goal, so the plan's first navigate takes the moving robot over when
//! it runs. The move is stopped if the plan's first step that moves or uses
//! the robot is anything else, if that navigate's tree ends without it having
//! run (it may sit under an `if`), or when the turn ends.

use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::executor::ControlPlanRequest;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNodeState};
use crate::planner::CapabilityTargetMap;
use crate::speculation::{decide, own_plan};
use robonix_scribe::info;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

/// Least probability the decision model must give a place for the robot to
/// head there, unless the task's `tau_open` says otherwise. A right move saves
/// the few seconds the plan takes; a wrong one costs about twice that, driving
/// off and back, so a move pays from about two chances in three. In perf15 its
/// picks were right 7 times in 7 at 0.56 to 0.78; not calibrated.
pub(crate) const TAU_OPEN: f64 = 0.65;

/// The same for a move at a later round, unless the task's `tau_handoff` says
/// otherwise. The decision model is less sure mid-task: in a smoke its picks of
/// the instruction point were right four times in five at 0.43 to 0.81, and a
/// round waited 7 to 13 s for the plan's navigate.
pub(crate) const TAU_HANDOFF: f64 = 0.5;

/// Longest the decision model may take for the move to be worth starting: the
/// plan's first step usually comes about six seconds after the task.
const ANSWER_WITHIN: Duration = Duration::from_secs(3);

const WHERE: &str = "The robot has just been given this task and is planning it. Which of \
                     these places does the task begin by going to? If its first move is not to \
                     go to one of them, or which one is unclear, pick the last option.";

const NOWHERE: &str = "The task does not begin by going to one of these places, or which one is \
                       unclear.";

const WHERE_NEXT: &str = "The robot is carrying out this task; `done` lists the steps it has run \
                          so far, with their results, `said` what the robot last told the user \
                          (often what it means to do next), and `robot_in` where it is. It is \
                          planning what to do next. Which of these places, objects or people does \
                          its next step go to? If its next move is not to go to one of them, or \
                          which one is unclear, pick the last option.";

const NOWHERE_NEXT: &str = "The next step does not go to one of these places, or which one is \
                            unclear.";

/// Steps of `done` the decision model is shown: the latest, which say where the
/// task stands.
const DONE_SHOWN: usize = 12;

/// Objects and people offered as where a later round's next step goes: those
/// the task names, people first.
const OBJECTS_SHOWN: usize = 8;

const GOAL_NEAR: &str = "/scene/goal_near";

/// How close a place's goal or an object must be to the robot for a later
/// round not to offer it: the robot is there already, and its next step there
/// asks or acts rather than drives (in a smoke, moves toward a person the robot
/// stood beside were stopped by the plan's ask_name).
const THERE_M: f64 = 2.0;

/// Where a later round stands, for the decision model to tell where its next
/// step goes.
pub(crate) struct Handoff {
    /// The steps the turn has run, with their results.
    pub(crate) done: Vec<Value>,
    /// What the robot last told the user, which often says what it means to do next.
    pub(crate) said: String,
    /// The region the robot is in, as Scene's robot context names it.
    pub(crate) robot_in: Value,
    /// Where the robot is on the map, if Scene knows.
    pub(crate) at: Option<(f64, f64)>,
    /// Objects and people the robot may go to next, each as how the question
    /// names it and its Scene id.
    pub(crate) objects: Vec<(String, String)>,
}

/// Where a move heads.
enum Goal {
    /// A map place's goal pose, as goal_region gave it.
    Place(Value),
    /// A Scene object or person, whose goal goal_near gives when the move starts.
    Object(String),
}

const NAVIGATE: &str = "/navigation/navigate";

/// What the id of the plan a move runs as begins with.
pub(crate) const PLAN_ID: &str = "opening-";

/// How far the opening move has got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// The decision model has not answered yet.
    Asking,
    /// The robot was sent toward the place.
    Moving,
    /// The turn has decided about the move; nothing more is sent.
    Closed,
}

/// The robot heading for where the task begins while its first plan is written.
pub(crate) struct Opening {
    /// The plan Pilot runs the move as, which a stop cancels.
    plan_id: String,
    /// Shared with the task that asks and moves, so a move is never sent after
    /// the turn has decided about it.
    phase: Arc<Mutex<Phase>>,
    /// Asks the decision model, then runs the move.
    task: JoinHandle<()>,
    /// Executor's control entry, to stop the move.
    control: RobonixSystemExecutorControlPlanClient<Channel>,
    /// The task's read-only contracts: a plan step calling one leaves the move running.
    reads: HashSet<String>,
    /// The step expected to take the move over, as its plan's id and its node
    /// index, once the turn has dispatched one.
    by: Option<(String, u32)>,
    /// Whether that step ran, so the move is not stopped.
    taken_over: bool,
}

impl Opening {
    /// Ask where `task` begins among `places` (the state view's map places, each
    /// with the goal goal_region gave it), or at a later round (`handoff`) where
    /// its next step goes among those and the objects and people it names, and
    /// move there if the decision model picks one with at least `tau`. `None`
    /// without a navigate capability or anywhere to go.
    #[allow(clippy::too_many_arguments)] // Each is a different part of the round it starts in.
    pub(crate) fn start(
        task: &str,
        handoff: Option<Handoff>,
        places: &Value,
        targets: &CapabilityTargetMap,
        graph: RobonixSystemExecutorExecuteClient<Channel>,
        control: RobonixSystemExecutorControlPlanClient<Channel>,
        reads: HashSet<String>,
        tau: f64,
    ) -> Option<Self> {
        let navigate = targets
            .iter()
            .find(|(_, (_, contract))| contract.ends_with(NAVIGATE))
            .map(|(name, _)| name.clone())?;
        let goal_near = targets
            .iter()
            .find(|(_, (_, contract))| contract.ends_with(GOAL_NEAR))
            .map(|(name, _)| name.clone());
        let at = handoff.as_ref().and_then(|handoff| handoff.at);
        let mut places: Vec<(String, Goal)> = places
            .as_array()?
            .iter()
            .filter(|place| place["goal"].is_object())
            .filter(|place| !there(at, &place["goal"]["pose"]["position"]))
            // Mid-task, only places the task names: a choice among every place
            // on the map spread the decision model's confidence thin.
            .filter(|place| {
                handoff.is_none() || place["name"].as_str().is_some_and(|name| names(task, name))
            })
            .filter_map(|place| {
                let name = place["name"].as_str()?;
                Some((format!("Go to {name}."), Goal::Place(place["goal"].clone())))
            })
            .collect();
        if goal_near.is_some()
            && let Some(handoff) = &handoff
        {
            let objects = handoff.objects.iter().take(OBJECTS_SHOWN);
            places.extend(
                objects.map(|(named, id)| (format!("Go to {named}."), Goal::Object(id.clone()))),
            );
        }
        if places.is_empty() {
            return None;
        }
        let names: Vec<&str> = places.iter().map(|(name, _)| name.as_str()).collect();
        let (state, question) = ask(task, handoff.as_ref(), &names);
        let round = handoff.as_ref().map_or(0, |handoff| handoff.done.len());
        let plan_id = format!("{PLAN_ID}{}", uuid::Uuid::new_v4());
        let phase = Arc::new(Mutex::new(Phase::Asking));
        let (targets, shared, id) = (targets.clone(), phase.clone(), plan_id.clone());
        let task = tokio::spawn(async move {
            let asked = Instant::now();
            let Ok(pick) = decide("opening", state, question, |_| tau).await else {
                return;
            };
            let Some((name, goal)) = pick.parse::<usize>().ok().and_then(|i| places.get(i)) else {
                return;
            };
            if asked.elapsed() > ANSWER_WITHIN {
                return;
            }
            let reply = toward(&navigate, goal_near.as_deref(), name, goal);
            let Some(plan) = reply.and_then(|reply| own_plan(&targets, &reply, id.clone())) else {
                return;
            };
            {
                let mut phase = shared.lock().unwrap_or_else(PoisonError::into_inner);
                if *phase != Phase::Asking {
                    return;
                }
                *phase = Phase::Moving;
            }
            info!(
                "[pilot/segue] {}",
                json!({"event": "opening", "plan_id": id, "place": name, "steps_done": round,
                       "answered_ms": asked.elapsed().as_millis() as u64})
            );
            if let Ok(events) = graph.clone().execute(tonic::Request::new(plan)).await {
                let mut events = events.into_inner();
                while let Ok(Some(_)) = events.message().await {}
            }
        });
        Some(Self {
            plan_id,
            phase,
            task,
            control,
            reads,
            by: None,
            taken_over: false,
        })
    }

    /// The move once the turn has dispatched `plan` as far as it has: stopped
    /// if the first step that moves or uses the robot does anything but
    /// navigate, and otherwise left running, for that navigate to take over
    /// when it starts.
    pub(crate) fn follow(mut self, plan: &Plan) -> Option<Self> {
        if self.by.is_some() {
            return Some(self);
        }
        let mut calls = plan.nodes.iter().enumerate();
        let first = calls.find_map(|(index, node)| {
            let contract = node.call.as_ref()?.contract_id.as_str();
            (!self.reads.contains(contract) && engages(contract)).then_some((index, contract))
        });
        match first {
            None => Some(self),
            Some((index, contract)) if contract.ends_with(NAVIGATE) => {
                self.by = Some((plan.plan_id.clone(), index as u32));
                Some(self)
            }
            Some(_) => None,
        }
    }

    /// The move once the tree `plan_id` reports `node`'s state, or has ended
    /// if `node` is `None`: taken over once the step expected to take it over
    /// has run, and stopped if that step fails or its tree ends without it
    /// having run, as a navigate under an `if` whose condition fails does.
    /// Executor reports a call only once it has ended, so that is when a move
    /// counts as taken over; by then navigation has long followed the new goal.
    pub(crate) fn observe(mut self, plan_id: &str, node: Option<&RtdlNodeState>) -> Option<Self> {
        let Some((plan, index)) = &self.by else {
            return Some(self);
        };
        let failed = [
            RtdlNodeStateEnum::Failed,
            RtdlNodeStateEnum::Canceled,
            RtdlNodeStateEnum::Timeout,
        ];
        match node {
            _ if plan != plan_id => Some(self),
            Some(node) if node.node_index != *index => Some(self),
            Some(node) if node.state == RtdlNodeStateEnum::Pending as u32 => Some(self),
            Some(node) => {
                self.taken_over = !failed.map(|state| state as u32).contains(&node.state);
                None
            }
            None => None,
        }
    }
}

/// What the decision model is shown and asked: where `task` begins among the
/// options `names`, or at a later round (`handoff`) where its next step goes.
fn ask(task: &str, handoff: Option<&Handoff>, names: &[&str]) -> (Value, Value) {
    let mut criteria: serde_json::Map<String, Value> = names
        .iter()
        .enumerate()
        .map(|(index, name)| (index.to_string(), (*name).into()))
        .collect();
    let (instructions, nowhere, state) = match handoff {
        None => (WHERE, NOWHERE, json!({"task": task})),
        Some(handoff) => {
            let done = &handoff.done;
            let latest = &done[done.len().saturating_sub(DONE_SHOWN)..];
            let state = json!({"task": task, "done": latest, "said": handoff.said,
                               "robot_in": handoff.robot_in});
            (WHERE_NEXT, NOWHERE_NEXT, state)
        }
    };
    criteria.insert("none".into(), nowhere.into());
    let question = json!({"type": "choice", "instructions": instructions, "criteria": criteria});
    (state, question)
}

/// Whether a step of `contract` moves the robot or uses its body or voice, which
/// a step such as a shell command or a memory lookup does not.
fn engages(contract: &str) -> bool {
    ["/primitive/", "/skill/", "/navigation/", "/speech/"]
        .iter()
        .any(|part| contract.contains(part))
}

/// The reply whose tree sends the robot toward `goal`, the option `name`'s, with
/// `navigate`: a place's goal at once, an object's or person's through
/// `goal_near` first, if that finds it reachable.
fn toward(navigate: &str, goal_near: Option<&str>, name: &str, goal: &Goal) -> Option<Value> {
    let description = format!("{} while the plan is written", name.trim_end_matches('.'));
    let go = |goal: Value| {
        json!({"op": "do", "op_id": 0, "cap": navigate, "description": description,
               "args": {"goal": goal}})
    };
    let children = match goal {
        Goal::Place(goal) => vec![go(goal.clone())],
        Goal::Object(id) => vec![
            json!({"op": "do", "op_id": 0, "cap": goal_near?, "description": "Find where to stand",
                   "args": {"object_id": id}, "bind": "near"}),
            json!({"op": "if", "op_id": 0, "description": "Go only if reachable",
                   "condition": {"op": "eq", "args": [{"var": "near", "path": ["reachable"]}, true]},
                   "then": go(json!({"var": "near", "path": ["goal"]}))}),
        ],
    };
    Some(
        json!({"content": "", "rtdl_description": description, "rtdl": {
        "op": "sequence", "op_id": 0, "description": description, "children": children}}),
    )
}

/// Words a place's name shares with every kind of place, which do not tell one
/// place from another.
const GENERIC: [&str; 5] = ["table", "room", "point", "area", "the"];

/// Whether `task` names the place `name`: its whole name, or a word of it that
/// is not GENERIC (so "the trash" names the kitchen trash bin).
fn names(task: &str, name: &str) -> bool {
    let words = |text: &str| -> String { text.to_lowercase().replace(['_', '-'], " ") };
    let (task, name) = (words(task), words(name));
    let said: HashSet<&str> = task.split(|c: char| !c.is_alphanumeric()).collect();
    task.contains(&name)
        || name
            .split_whitespace()
            .any(|word| !GENERIC.contains(&word) && said.contains(word))
}

/// Whether `position` (x, y on the map) lies within THERE_M of the robot at `at`.
fn there(at: Option<(f64, f64)>, position: &Value) -> bool {
    let (Some((x, y)), Some(px), Some(py)) = (at, position["x"].as_f64(), position["y"].as_f64())
    else {
        return false;
    };
    (px - x).hypot(py - y) < THERE_M
}

/// Objects and people a later round's next step may go to: people, then the
/// objects whose label the task names, each as the question names it (with its
/// region from the state view's `objects`) and its Scene id; none the robot at
/// `at` is already beside, by Scene's `poses`, and one per name, the nearest:
/// options the question names alike split the decision model's confidence.
pub(crate) fn named_objects(
    task: &str,
    objects: &Value,
    poses: &Value,
    at: Option<(f64, f64)>,
) -> Vec<(String, String)> {
    let pose = |id: &str| {
        let poses = poses.as_array().into_iter().flatten();
        poses.into_iter().find(|pose| pose["id"] == id)
    };
    let beside = |id: &str| pose(id).is_some_and(|pose| there(at, pose));
    // How far the robot has to go, unknown last.
    let away = |id: &str| -> f64 {
        let xy = pose(id).and_then(|pose| Some((pose["x"].as_f64()?, pose["y"].as_f64()?)));
        match (at, xy) {
            (Some((x, y)), Some((px, py))) => (px - x).hypot(py - y),
            _ => f64::MAX,
        }
    };
    let words = |text: &str| -> String { text.to_lowercase().replace(['_', '-'], " ") };
    let task = words(task);
    let mut found: Vec<(bool, f64, String, String)> = Vec::new();
    for (region, listed) in objects.as_object().into_iter().flatten() {
        for object in listed.as_array().into_iter().flatten() {
            let (Some(label), Some(id)) = (object["label"].as_str(), object["id"].as_str()) else {
                continue;
            };
            let person = label == "person";
            if person || task.contains(&words(label)) {
                let named = format!("the {} in {region}", words(label));
                found.push((!person, away(id), named, id.to_string()));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
    // One per name, the nearest; none if the robot stands beside it, since Scene may keep the one it just reached
    // twice and a further record named alike is likelier that one than another.
    let mut named: HashSet<String> = HashSet::new();
    found
        .into_iter()
        .filter(|(_, _, name, _)| named.insert(name.clone()))
        .filter(|(_, _, _, id)| !beside(id))
        .map(|(_, _, name, id)| (name, id))
        .collect()
}

impl Drop for Opening {
    fn drop(&mut self) {
        let was = std::mem::replace(
            &mut *self.phase.lock().unwrap_or_else(PoisonError::into_inner),
            Phase::Closed,
        );
        let ended = match was {
            Phase::Asking => {
                self.task.abort();
                "not sent"
            }
            Phase::Moving if self.taken_over => "taken over",
            Phase::Moving => {
                let mut control = self.control.clone();
                let plan_id = self.plan_id.clone();
                tokio::spawn(async move {
                    let request = ControlPlanRequest {
                        action: "cancel".into(),
                        plan_id,
                        wait_ms: 2_000,
                    };
                    let _ = control.control_plan(tonic::Request::new(request)).await;
                });
                "stopped"
            }
            Phase::Closed => return,
        };
        info!(
            "[pilot/segue] {}",
            json!({"event": "opening_end", "plan_id": self.plan_id, "ended": ended})
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{Opening, Phase};
    use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
    use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
    use crate::pb::pilot::{CapabilityCall, Plan, RtdlNode, RtdlNodeState};
    use crate::planner::{CapabilityTargetMap, RTDL_DO, RTDL_IF, RTDL_SEQUENCE};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[test]
    fn a_later_round_asks_where_the_next_step_goes_given_the_latest_steps() {
        let handoff = super::Handoff {
            done: (0..15).map(|i| json!({"step": i})).collect(),
            said: "I'll take it to the kitchen.".into(),
            robot_in: "bedroom".into(),
            at: None,
            objects: Vec::new(),
        };
        let (state, question) = super::ask("t", Some(&handoff), &["Go to kitchen."]);
        assert_eq!(
            state["done"].as_array().map(Vec::len),
            Some(super::DONE_SHOWN)
        );
        assert_eq!(state["done"][0]["step"], 3);
        assert_eq!(state["said"], "I'll take it to the kitchen.");
        assert_eq!(question["instructions"], super::WHERE_NEXT);
        assert_eq!(question["criteria"]["0"], "Go to kitchen.");
        let (first, opening) = super::ask("t", None, &["Go to kitchen."]);
        assert!(first.get("done").is_none());
        assert_eq!(opening["instructions"], super::WHERE);
    }

    #[test]
    fn a_later_round_offers_only_places_the_task_names() {
        let task = "Put the cup in the trash, then come back to the instruction point";
        assert!(super::names(task, "kitchen trash bin"));
        assert!(super::names(task, "instruction point"));
        assert!(!super::names(task, "coffee table"));
        assert!(!super::names(task, "living room"));
    }

    #[test]
    fn a_later_round_may_head_for_people_and_objects_the_task_names() {
        let objects = json!({
            "kitchen": [{"id": "o1", "label": "red_bull"}, {"id": "o2", "label": "cup"},
                        {"id": "p1", "label": "person"}],
            "elsewhere": []
        });
        let poses = json!([{"id": "o2", "x": 0.5, "y": 0.0}, {"id": "p2", "x": 1.0, "y": 1.0}]);
        let named = super::named_objects(
            "Bring me a red bull from the kitchen",
            &objects,
            &poses,
            None,
        );
        assert_eq!(
            named,
            [
                ("the person in kitchen".to_string(), "p1".to_string()),
                ("the red bull in kitchen".to_string(), "o1".to_string())
            ]
        );
        // Two people in one room are offered once, the nearer.
        let two = json!({"kitchen": [{"id": "far", "label": "person"}, {"id": "near", "label": "person"}]});
        let at = json!([{"id": "far", "x": 9.0, "y": 0.0}, {"id": "near", "x": 4.0, "y": 0.0}]);
        assert_eq!(
            super::named_objects("t", &two, &at, Some((0.0, 0.0))),
            [("the person in kitchen".to_string(), "near".to_string())]
        );
        // A person the robot stands beside is not offered.
        let beside = json!({"bedroom": [{"id": "p2", "label": "person"}]});
        assert!(super::named_objects("t", &beside, &poses, Some((0.0, 0.0))).is_empty());
        // Nor anyone else in that room: a further person there is likelier the same one recorded twice.
        let twice = json!({"kitchen": [{"id": "here", "label": "person"}, {"id": "there", "label": "person"}]});
        let at = json!([{"id": "here", "x": 0.5, "y": 0.0}, {"id": "there", "x": 4.0, "y": 0.0}]);
        assert!(super::named_objects("t", &twice, &at, Some((0.0, 0.0))).is_empty());
        let cup = super::Goal::Object("o2".into());
        let tree =
            &super::toward("nav", Some("near"), "Go to the cup.", &cup).expect("goal_near")["rtdl"];
        assert_eq!(tree["children"][0]["args"]["object_id"], "o2");
        assert_eq!(tree["children"][1]["then"]["args"]["goal"]["var"], "near");
        assert!(super::toward("nav", None, "x", &cup).is_none());
    }

    /// A move under way, whose stop goes nowhere.
    fn moving() -> Opening {
        let nowhere = tonic::transport::Channel::from_static("http://[::1]:1").connect_lazy();
        Opening {
            plan_id: "opening-1".into(),
            phase: Arc::new(Mutex::new(Phase::Moving)),
            task: tokio::spawn(async {}),
            control: RobonixSystemExecutorControlPlanClient::new(nowhere),
            reads: ["robonix/system/scene/goal_near".to_string()].into(),
            by: None,
            taken_over: false,
        }
    }

    /// goal_near, then navigate and pick only if the goal is reachable.
    fn approach_if_reachable() -> Plan {
        let node = |node_kind: u32, children: &[u32], contract: &str| RtdlNode {
            node_kind,
            children: children.to_vec(),
            call: (!contract.is_empty()).then(|| CapabilityCall {
                contract_id: contract.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Plan {
            plan_id: "p".into(),
            nodes: vec![
                node(RTDL_SEQUENCE, &[1, 2], ""),
                node(RTDL_DO, &[], "robonix/system/scene/goal_near"),
                node(RTDL_IF, &[3], ""),
                node(RTDL_SEQUENCE, &[4, 5], ""),
                node(RTDL_DO, &[], "robonix/service/navigation/navigate"),
                node(RTDL_DO, &[], "robonix/skill/manipulation/pick"),
            ],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn the_move_counts_as_taken_over_only_once_the_plans_navigate_has_run() {
        let state = |node_index: u32, state: RtdlNodeStateEnum| RtdlNodeState {
            node_index,
            state: state as u32,
            ..Default::default()
        };
        let followed = || moving().follow(&approach_if_reachable()).unwrap();
        assert_eq!(followed().by, Some(("p".into(), 4)));
        // Other trees and other steps say nothing about it.
        let open = followed();
        let open = open.observe("q", None).unwrap();
        let open = open
            .observe("p", Some(&state(1, RtdlNodeStateEnum::Succeeded)))
            .unwrap();
        let open = open
            .observe("p", Some(&state(4, RtdlNodeStateEnum::Pending)))
            .unwrap();
        // Its first report settles the move, as does its tree ending without one.
        let verifying = state(4, RtdlNodeStateEnum::Verifying);
        assert!(open.observe("p", Some(&verifying)).is_none());
        assert!(followed().observe("p", None).is_none());
        // A plan that grasps first stops the move at once.
        let mut grasp = approach_if_reachable();
        grasp.nodes.swap(4, 5);
        assert!(moving().follow(&grasp).is_none());
    }

    #[test]
    fn only_steps_that_move_or_use_the_robot_decide_about_the_move() {
        assert!(super::engages("robonix/skill/manipulation/pick"));
        assert!(super::engages("robonix/service/speech/speak"));
        assert!(super::engages("robonix/service/navigation/navigate"));
        assert!(super::engages("robonix/primitive/camera/snapshot"));
        assert!(!super::engages(
            "robonix/system/executor/builtin/run_command"
        ));
        assert!(!super::engages("robonix/service/memory/search"));
        assert!(!super::engages("robonix/system/scene/goal_region"));
    }

    #[test]
    fn the_move_is_a_plan_with_one_navigate_to_the_goal() {
        let mut targets = CapabilityTargetMap::new();
        targets.insert(
            "navigation_navigate".into(),
            ("nav2".into(), "robonix/service/navigation/navigate".into()),
        );
        let goal = json!({"header": {"frame_id": "map"}, "pose": {"position": {"x": 1.0}}});
        let place = super::Goal::Place(goal.clone());
        let reply = super::toward("navigation_navigate", None, "Go to kitchen.", &place);
        let plan =
            crate::speculation::own_plan(&targets, &reply.expect("a place"), "opening-1".into())
                .expect("the move parses");
        let calls: Vec<_> = plan
            .nodes
            .iter()
            .filter(|n| n.node_kind == RTDL_DO)
            .collect();
        assert_eq!(calls.len(), 1);
        let call = calls[0].call.as_ref().expect("a call");
        assert_eq!(call.contract_id, "robonix/service/navigation/navigate");
        assert_eq!(call.call_id, "opening-1:0");
        let args: serde_json::Value = serde_json::from_str(&call.args_json).unwrap();
        assert_eq!(args["goal"], goal);
    }

    #[test]
    fn a_move_to_an_object_finds_where_to_stand_first() {
        let mut targets = CapabilityTargetMap::new();
        targets.insert(
            "navigation_navigate".into(),
            ("nav2".into(), "robonix/service/navigation/navigate".into()),
        );
        targets.insert(
            "scene_goal_near".into(),
            ("scene".into(), "robonix/system/scene/goal_near".into()),
        );
        let cup = super::Goal::Object("scene.object.7".into());
        let reply = super::toward(
            "navigation_navigate",
            Some("scene_goal_near"),
            "Go to the cup.",
            &cup,
        );
        let plan =
            crate::speculation::own_plan(&targets, &reply.expect("goal_near"), "opening-2".into())
                .expect("the move parses");
        let contracts: Vec<&str> = plan
            .nodes
            .iter()
            .filter_map(|n| Some(n.call.as_ref()?.contract_id.as_str()))
            .collect();
        assert_eq!(
            contracts,
            [
                "robonix/system/scene/goal_near",
                "robonix/service/navigation/navigate"
            ]
        );
        assert!(plan.nodes.iter().any(|n| n.node_kind == RTDL_IF));
    }
}
