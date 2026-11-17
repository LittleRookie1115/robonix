// SPDX-License-Identifier: MulanPSL-2.0

//! The read set while a tree runs. A prepared reply is checked against Scene's
//! objects when it is used, but a tree that goes to an object and then picks it
//! up does both in one tree, and a fresh plan is written from the state its
//! planning round began with: either can be stale by the time a step that acts
//! on the world runs. So each dispatched tree of a speculating task is watched:
//! once a second Pilot reads Scene's list, and once an object the tree relies
//! on has dropped out of it on two readings in a row, the tree is stopped before
//! its remaining steps and the planner is told why.
//!
//! A tree relies on the objects whose ids its steps name, and on those a
//! finished step returned whose output a step still to run reads through
//! `{"var": ...}`, directly or through other finished steps. A step still to
//! act that names a class (`{"object": "cornflakes"}`) relies on the objects of
//! that class the tree knew of, once Scene lists none of that class and no step
//! still to run searches for it. An object Scene no longer lists is not gone if
//! Scene lists one of its class where it was: Scene folds duplicate records of
//! one object into one, retiring all ids but one.
//!
//! Only a tree with a step left that does more than read (the task's
//! `read_only`) is stopped, and only until one of its steps beyond reading,
//! moving the robot or speaking has succeeded: from then on the tree has
//! changed the world itself, and an object it took is expected to leave its
//! place.

use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNodeState};
use crate::planner::RTDL_DO;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// How far from where an object was another of its class may be listed and
/// still stand for it: Scene folds records of one class within 0.35 m.
const SAME_M: f64 = 0.5;

/// What Scene says of one object, as the watch compares it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Listing {
    /// Its class.
    pub(crate) label: String,
    /// Its map position in metres, where the source gives one.
    pub(crate) at: Option<(f64, f64)>,
}

/// Each object in `value` with an id and a label, by id: Scene's find output,
/// a step's output, or the state view's objects by region (which have no
/// positions).
pub(crate) fn listings(value: &Value) -> HashMap<String, Listing> {
    fn collect(value: &Value, out: &mut HashMap<String, Listing>) {
        match value {
            Value::Object(fields) => {
                let id = fields.get("id").and_then(Value::as_str);
                let label = fields.get("label").and_then(Value::as_str);
                if let (Some(id), Some(label)) = (id, label) {
                    let at = fields
                        .get("x")
                        .and_then(Value::as_f64)
                        .zip(fields.get("y").and_then(Value::as_f64));
                    out.insert(
                        id.into(),
                        Listing {
                            label: label.into(),
                            at,
                        },
                    );
                }
                fields.values().for_each(|v| collect(v, out));
            }
            Value::Array(items) => items.iter().for_each(|v| collect(v, out)),
            _ => {}
        }
    }
    let mut out = HashMap::new();
    collect(value, &mut out);
    out
}

/// The names `args` reads through `{"var": "<name>", ...}`.
fn vars(args: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = args;
    while let Some(at) = rest.find("\"var\":") {
        rest = rest[at + 6..].trim_start();
        let Some(name) = rest.strip_prefix('"') else {
            continue;
        };
        let Some(end) = name.find('"') else { break };
        names.push(&name[..end]);
        rest = &name[end..];
    }
    names
}

/// One step of a watched tree.
struct Step {
    /// Its node index in the tree.
    index: u32,
    /// Its op_id.
    op_id: String,
    /// The name it binds its output to, which a later step's `{"var": ...}` reads.
    bind: String,
    /// Its capability's contract id.
    contract: String,
    /// Its arguments as dispatched.
    args: String,
}

/// A dispatched tree's read set, checked against Scene's list while it runs.
pub(crate) struct Watch {
    /// The tree.
    pub(crate) plan_id: String,
    /// Its call steps.
    steps: Vec<Step>,
    /// The objects Scene listed, by region, in the state the tree was written from.
    then: Value,
    /// Every object the tree could know of, with where it was last listed: from
    /// the state it was written from, its finished steps' outputs and the readings.
    known: HashMap<String, Listing>,
    /// Raw outputs of its succeeded steps by op_id.
    outputs: HashMap<String, String>,
    /// Node indices of its steps that have ended.
    ended: HashSet<u32>,
    /// Whether a step that may move objects has succeeded, which ends the watch.
    acted: bool,
    /// The task's read-only capabilities, by contract id.
    reads: Arc<HashSet<String>>,
    /// Objects found gone at the last reading, which the next must confirm.
    gone: Vec<String>,
}

impl Watch {
    /// Watch `plan`, written from the objects in `then`, Scene's objects by
    /// region, which Scene's list `listed` places.
    pub(crate) fn new(
        plan: &Plan,
        then: Value,
        listed: HashMap<String, Listing>,
        reads: Arc<HashSet<String>>,
    ) -> Self {
        let steps = plan
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.node_kind == RTDL_DO);
        let steps = steps
            .filter_map(|(index, node)| {
                let call = node.call.as_ref()?;
                Some(Step {
                    index: index as u32,
                    op_id: node.op_id.clone(),
                    bind: node.bind.clone(),
                    contract: call.contract_id.clone(),
                    args: call.args_json.clone(),
                })
            })
            .collect();
        let mut known = listings(&then);
        known.extend(listed);
        Self {
            plan_id: plan.plan_id.clone(),
            steps,
            then,
            known,
            outputs: HashMap::new(),
            ended: HashSet::new(),
            acted: false,
            reads,
            gone: Vec::new(),
        }
    }

    /// Take in a state of one of the tree's nodes.
    pub(crate) fn progressed(&mut self, node: &RtdlNodeState) {
        let terminal = [
            RtdlNodeStateEnum::Succeeded,
            RtdlNodeStateEnum::Failed,
            RtdlNodeStateEnum::Canceled,
            RtdlNodeStateEnum::Timeout,
        ];
        if !terminal.iter().any(|state| node.state == *state as u32) {
            return;
        }
        self.ended.insert(node.node_index);
        let Some(step) = self.steps.iter().find(|step| step.index == node.node_index) else {
            return;
        };
        if node.state != RtdlNodeStateEnum::Succeeded as u32 {
            return;
        }
        if let Some(result) = &node.leaf_result {
            let output = serde_json::from_str(&result.output).unwrap_or_default();
            self.known.extend(listings(&output));
            self.outputs
                .insert(step.op_id.clone(), result.output.clone());
        }
        self.acted |= !self.reads.contains(&step.contract)
            && !["/navigation/", "/speech/"]
                .iter()
                .any(|kind| step.contract.contains(kind));
    }

    /// Whether a change could still stop the tree: it has not acted on the
    /// world itself and has a step left that does more than read.
    pub(crate) fn armed(&self) -> bool {
        !self.acted && !self.left().is_empty()
    }

    /// The op_ids of the steps that have not ended and do more than read.
    pub(crate) fn left(&self) -> Vec<&str> {
        let left = self.unended();
        let acting = left.filter(|step| !self.reads.contains(&step.contract));
        acting.map(|step| step.op_id.as_str()).collect()
    }

    fn unended(&self) -> impl Iterator<Item = &Step> {
        self.steps
            .iter()
            .filter(|step| !self.ended.contains(&step.index))
    }

    /// The objects the tree relies on that `listed`, Scene's list now, lacks at
    /// this reading and lacked at the one before, each as "<id> (<label> in
    /// <region> when the tree was written)"; nothing unless [`Watch::armed`].
    pub(crate) fn check(&mut self, listed: &HashMap<String, Listing>) -> Vec<String> {
        if !self.armed() {
            self.gone.clear();
            return Vec::new();
        }
        for (id, listing) in listed {
            if self.known.contains_key(id) {
                self.known.insert(id.clone(), listing.clone());
            }
        }
        let text: String = self.steps.iter().map(|step| step.args.as_str()).collect();
        let mut relied: HashSet<String> = self
            .known
            .keys()
            .filter(|id| crate::speculation::names(&text, id))
            .cloned()
            .collect();
        // The finished steps whose outputs what is still to run reads, followed back.
        let mut needed: HashSet<&str> = self.unended().flat_map(|step| vars(&step.args)).collect();
        loop {
            let more: Vec<&str> = self
                .steps
                .iter()
                .filter(|step| needed.contains(step.bind.as_str()))
                .flat_map(|step| vars(&step.args))
                .filter(|name| !needed.contains(name))
                .collect();
            if more.is_empty() {
                break;
            }
            needed.extend(more);
        }
        for step in self
            .steps
            .iter()
            .filter(|step| needed.contains(step.bind.as_str()))
        {
            if let Some(output) = self.outputs.get(&step.op_id) {
                relied.extend(
                    listings(&serde_json::from_str(output).unwrap_or_default()).into_keys(),
                );
            }
        }
        let names_class = |step: &Step, class: &str| {
            [class.to_string(), class.replace('_', " ")]
                .iter()
                .any(|form| step.args.contains(&format!("\"{form}\"")))
        };
        let classes: HashSet<&str> = self
            .known
            .values()
            .map(|listing| listing.label.as_str())
            .collect();
        for class in classes {
            let named = |read: bool| {
                self.unended().any(|step| {
                    self.reads.contains(&step.contract) == read && names_class(step, class)
                })
            };
            if named(false)
                && !named(true)
                && !listed.values().any(|listing| listing.label == class)
            {
                let of_class = self
                    .known
                    .iter()
                    .filter(|(_, listing)| listing.label == class);
                relied.extend(of_class.map(|(id, _)| id.clone()));
            }
        }
        let stands_in = |id: &String| {
            let Some(Listing {
                label,
                at: Some((x, y)),
            }) = self.known.get(id)
            else {
                return false;
            };
            listed.values().any(|listing| {
                listing.label == *label
                    && listing
                        .at
                        .is_some_and(|(lx, ly)| (lx - x).hypot(ly - y) <= SAME_M)
            })
        };
        let mut gone: Vec<String> = relied
            .into_iter()
            .filter(|id| !listed.contains_key(id) && !stands_in(id))
            .collect();
        gone.sort();
        let confirmed: Vec<String> = gone
            .iter()
            .filter(|id| self.gone.contains(id))
            .cloned()
            .collect();
        self.gone = gone;
        let region = crate::speculation::placed(&self.then);
        confirmed
            .into_iter()
            .map(|id| {
                let label = self
                    .known
                    .get(&id)
                    .map_or("object", |listing| listing.label.as_str());
                match region.get(id.as_str()) {
                    Some(region) => format!("{id} ({label} in {region} when the tree was written)"),
                    None => format!("{id} ({label}, which an earlier step found)"),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Listing, Watch};
    use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
    use crate::pb::pilot::{CapabilityCall, CapabilityCallResult, Plan, RtdlNode, RtdlNodeState};
    use crate::planner::{RTDL_DO, RTDL_SEQUENCE};
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    const FIND: &str = "robonix/system/scene/find";
    const GOAL_NEAR: &str = "robonix/system/scene/goal_near";
    const NAVIGATE: &str = "robonix/service/navigation/navigate";
    const PICK: &str = "robonix/skill/manipulation/pick";

    /// A sequence of calls, each (contract, bind, args).
    fn tree(calls: &[(&str, &str, serde_json::Value)]) -> Plan {
        let root = RtdlNode {
            node_kind: RTDL_SEQUENCE,
            children: (1..=calls.len() as u32).collect(),
            ..Default::default()
        };
        let steps = calls
            .iter()
            .enumerate()
            .map(|(i, (contract, bind, args))| RtdlNode {
                node_kind: RTDL_DO,
                op_id: format!("p-{}", i + 1),
                bind: bind.to_string(),
                call: Some(CapabilityCall {
                    contract_id: contract.to_string(),
                    args_json: args.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            });
        Plan {
            plan_id: "p".into(),
            nodes: std::iter::once(root).chain(steps).collect(),
            ..Default::default()
        }
    }

    fn succeeded(node_index: u32, output: serde_json::Value) -> RtdlNodeState {
        RtdlNodeState {
            plan_id: "p".into(),
            node_index,
            state: RtdlNodeStateEnum::Succeeded as u32,
            leaf_result: Some(CapabilityCallResult {
                output: output.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Scene's list: each (id, label, x).
    fn listed(objects: &[(&str, &str, f64)]) -> HashMap<String, Listing> {
        let listing = |label: &str, x: f64| Listing {
            label: label.into(),
            at: Some((x, 0.0)),
        };
        objects
            .iter()
            .map(|(id, label, x)| (id.to_string(), listing(label, *x)))
            .collect()
    }

    /// The cornflakes o1 at x 1 and the sofa o2 at x 5, as the tree was written.
    fn watch(plan: &Plan) -> Watch {
        let then = json!({"living room": [{"id": "o1", "label": "cornflakes"}, {"id": "o2", "label": "sofa"}]});
        let reads = [FIND, GOAL_NEAR].map(String::from);
        let at = listed(&[("o1", "cornflakes", 1.0), ("o2", "sofa", 5.0)]);
        Watch::new(plan, then, at, Arc::new(HashSet::from(reads)))
    }

    #[test]
    fn a_named_object_gone_on_two_readings_stops_the_tree_before_it_acts() {
        let plan = tree(&[
            (GOAL_NEAR, "g", json!({"object_id": "o1"})),
            (
                NAVIGATE,
                "",
                json!({"goal": {"var": "g", "path": ["goal"]}}),
            ),
            (PICK, "", json!({"object": "cornflakes"})),
        ]);
        let mut watch = watch(&plan);
        watch.progressed(&succeeded(1, json!({"reachable": true})));
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        assert!(
            watch
                .check(&listed(&[("o1", "cornflakes", 1.0), ("o2", "sofa", 5.0)]))
                .is_empty()
        );
        // A flicker is not enough; a second reading confirms.
        assert!(watch.check(&sofa).is_empty());
        assert!(
            watch
                .check(&listed(&[("o1", "cornflakes", 1.0)]))
                .is_empty()
        );
        assert!(watch.check(&sofa).is_empty());
        assert_eq!(
            watch.check(&sofa),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
        assert_eq!(watch.left(), ["p-2", "p-3"]);
        // An object the tree does not name does not count.
        let mut other = self::watch(&plan);
        let cornflakes = listed(&[("o1", "cornflakes", 1.0)]);
        other.check(&cornflakes);
        assert!(other.check(&cornflakes).is_empty());
    }

    #[test]
    fn an_id_scene_folded_into_another_of_its_class_where_it_was_is_not_gone() {
        let plan = tree(&[
            (GOAL_NEAR, "", json!({"object_id": "o1"})),
            (PICK, "", json!({})),
        ]);
        let mut watch = watch(&plan);
        let merged = listed(&[("o7", "cornflakes", 1.2)]);
        watch.check(&merged);
        assert!(watch.check(&merged).is_empty());
        // One of its class across the room does not stand for it.
        let far = listed(&[("o7", "cornflakes", 3.0)]);
        watch.check(&far);
        assert_eq!(watch.check(&far).len(), 1);
    }

    #[test]
    fn an_object_a_finished_step_found_counts_while_a_later_step_reads_it() {
        let plan = tree(&[
            (FIND, "f", json!({"label": "cup"})),
            (
                GOAL_NEAR,
                "g",
                json!({"object_id": {"var": "f", "path": ["objects", 0, "id"]}}),
            ),
            (
                NAVIGATE,
                "",
                json!({"goal": {"var": "g", "path": ["goal"]}}),
            ),
        ]);
        let mut watch = watch(&plan);
        let cup = json!({"objects": [{"id": "o9", "label": "cup", "x": 8.0, "y": 0.0}]});
        watch.progressed(&succeeded(1, cup));
        watch.progressed(&succeeded(2, json!({"reachable": true, "goal": {}})));
        let none = listed(&[("o1", "cornflakes", 1.0)]);
        watch.check(&none);
        assert_eq!(
            watch.check(&none),
            ["o9 (cup, which an earlier step found)"]
        );
    }

    #[test]
    fn a_step_naming_a_class_relies_on_it_once_scene_lists_none() {
        let pick = tree(&[(PICK, "", json!({"object": "cornflakes"}))]);
        let mut watch = watch(&pick);
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        watch.check(&sofa);
        assert_eq!(
            watch.check(&sofa),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
        // Another of the class listed anywhere, or a search for it still to run, keeps it.
        let mut elsewhere = self::watch(&pick);
        let other = listed(&[("o5", "cornflakes", 9.0)]);
        elsewhere.check(&other);
        assert!(elsewhere.check(&other).is_empty());
        let search = tree(&[
            (FIND, "", json!({"label": "cornflakes"})),
            (PICK, "", json!({"object": "cornflakes"})),
        ]);
        let mut searching = self::watch(&search);
        searching.check(&sofa);
        assert!(searching.check(&sofa).is_empty());
    }

    #[test]
    fn nothing_stops_a_tree_with_only_reads_left_or_that_has_acted_itself() {
        let reads = tree(&[(FIND, "", json!({"label": "cornflakes", "anchor": "o1"}))]);
        let mut reading = watch(&reads);
        reading.check(&HashMap::new());
        assert!(reading.check(&HashMap::new()).is_empty());
        let fetch = tree(&[
            (PICK, "", json!({"object_id": "o1"})),
            (NAVIGATE, "", json!({"goal": {}})),
        ]);
        let mut fetching = watch(&fetch);
        fetching.progressed(&succeeded(1, json!({"ok": true})));
        fetching.check(&HashMap::new());
        assert!(fetching.check(&HashMap::new()).is_empty());
    }
}
