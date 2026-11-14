//! The opening move. A task's first plan takes several seconds to write, and
//! most tasks begin by going somewhere. With a decision model, Pilot asks it
//! which of the map's places the task begins by going to and, if it is sure,
//! sends the robot there while the plan is written. Navigation follows the
//! newest goal, so the plan's first navigate takes the moving robot over; a
//! plan whose first step that moves or uses the robot is anything else stops
//! it, and so does the end of the turn.

use crate::pb::contracts::robonix_system_executor_control_plan_client::RobonixSystemExecutorControlPlanClient;
use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::executor::ControlPlanRequest;
use crate::pb::pilot::Plan;
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

/// Longest the decision model may take for the move to be worth starting: the
/// plan's first step usually comes about six seconds after the task.
const ANSWER_WITHIN: Duration = Duration::from_secs(3);

const WHERE: &str = "The robot has just been given this task and is planning it. Which of \
                     these places does the task begin by going to? If its first move is not to \
                     go to one of them, or which one is unclear, pick the last option.";

const NOWHERE: &str = "The task does not begin by going to one of these places, or which one is \
                       unclear.";

const NAVIGATE: &str = "/navigation/navigate";

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
    /// Whether a plan's navigate took the move over, so it is not stopped.
    taken_over: bool,
}

impl Opening {
    /// Ask where `task` begins among `places` (the state view's map places, each
    /// with the goal goal_region gave it) and move there if the decision model
    /// picks one with at least `tau`. `None` without a navigate capability or a
    /// place to go.
    pub(crate) fn start(
        task: &str,
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
        let places: Vec<(String, Value)> = places
            .as_array()?
            .iter()
            .filter(|place| place["goal"].is_object())
            .filter_map(|place| Some((place["name"].as_str()?.to_string(), place["goal"].clone())))
            .collect();
        if places.is_empty() {
            return None;
        }
        let mut criteria: serde_json::Map<String, Value> = places
            .iter()
            .enumerate()
            .map(|(index, (name, _))| (index.to_string(), format!("Go to {name}.").into()))
            .collect();
        criteria.insert("none".into(), NOWHERE.into());
        let question = json!({"type": "choice", "instructions": WHERE, "criteria": criteria});
        let state = json!({"task": task});
        let plan_id = format!("opening-{}", uuid::Uuid::new_v4());
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
            let Some(plan) = own_plan(&targets, &toward(&navigate, name, goal), id.clone()) else {
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
                json!({"event": "opening", "plan_id": id, "place": name,
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
            taken_over: false,
        })
    }

    /// The move once the turn has dispatched `plan` as far as it has: taken
    /// over if the first step that moves or uses the robot navigates, stopped if
    /// it does anything else, and still running until there is one.
    pub(crate) fn follow(mut self, plan: &Plan) -> Option<Self> {
        let calls = plan.nodes.iter().filter_map(|node| node.call.as_ref());
        let Some(first) = calls
            .map(|call| call.contract_id.as_str())
            .find(|contract| !self.reads.contains(*contract) && engages(contract))
        else {
            return Some(self);
        };
        self.taken_over = first.ends_with(NAVIGATE);
        None
    }
}

/// Whether a step of `contract` moves the robot or uses its body or voice, which
/// a step such as a shell command or a memory lookup does not.
fn engages(contract: &str) -> bool {
    ["/primitive/", "/skill/", "/navigation/", "/speech/"]
        .iter()
        .any(|part| contract.contains(part))
}

/// The reply whose tree sends the robot to `goal`, the place `name`'s, with `navigate`.
fn toward(navigate: &str, name: &str, goal: &Value) -> Value {
    let description = format!("Head for {name} while the plan is written");
    json!({"content": "", "rtdl_description": description, "rtdl": {
        "op": "sequence", "op_id": 1, "description": description,
        "children": [{"op": "do", "op_id": 2, "cap": navigate, "description": description,
                      "args": {"goal": goal}}]}})
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
    use crate::planner::{CapabilityTargetMap, RTDL_DO};
    use serde_json::json;

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
        let reply = super::toward("navigation_navigate", "kitchen", &goal);
        let plan = crate::speculation::own_plan(&targets, &reply, "opening-1".into())
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
}
