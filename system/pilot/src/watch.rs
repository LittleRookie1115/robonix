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
//! still to run searches for it before the step that acts on it. An object Scene no longer lists is not gone if
//! Scene lists one of its class where it was, within 0.5 m beyond the object's
//! own extent: Scene folds duplicate records of one object into one, retiring
//! all ids but one, and a person's records spread over their body.
//!
//! A navigation still to run relies on its goal, written in its arguments or
//! read from a finished step (goal_region's or goal_near's): Scene's
//! goal_clear checks it against the map and Navigation's live costmap, and a
//! goal not clear on two readings stops the tree. Where the goal was a
//! region's, the goal the state view keeps for the region is dropped so that
//! it is asked for again.
//!
//! Only a tree with a step left that does more than read (the task's
//! `read_only`) is stopped. Once a step beyond reading, moving the robot or
//! speaking has succeeded, in this tree or an earlier one of the turn, what it
//! acted on (what it names, by id or class, or read from earlier steps) is no
//! longer watched: an object the robot took is expected to leave its place,
//! while the person it is to be handed to is still relied on.

use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNodeState};
use crate::planner::RTDL_DO;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

/// How far from where an object was another of its class may be listed and
/// still stand for it, beyond the object's own extent: Scene folds records of
/// one class within 0.35 m, and a person's records spread over their body.
const SAME_M: f64 = 0.5;

/// What one goal is keyed by in goal_clear's answers: its position to the centimetre.
pub(crate) fn goal_key(goal: &Value) -> Option<String> {
    position(goal).map(|(x, y)| format!("{x:.2},{y:.2}"))
}

/// The map position of a navigation goal, a PoseStamped.
pub(crate) fn position(goal: &Value) -> Option<(f64, f64)> {
    let at = &goal["pose"]["position"];
    Some((at["x"].as_f64()?, at["y"].as_f64()?))
}

/// What Scene says of one object, as the watch compares it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Listing {
    /// Its class.
    pub(crate) label: String,
    /// Its map position in metres, where the source gives one.
    pub(crate) at: Option<(f64, f64)>,
    /// The larger of its horizontal extents in metres, 0 where unknown.
    pub(crate) size: f64,
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
                    let number = |key: &str| fields.get(key).and_then(Value::as_f64);
                    let at = number("x").zip(number("y"));
                    let size = number("size_x")
                        .unwrap_or(0.0)
                        .max(number("size_y").unwrap_or(0.0));
                    out.insert(
                        id.into(),
                        Listing {
                            label: label.into(),
                            at,
                            size,
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

/// Whether `args` names the class `class`, as `"cornflakes"` or `"red bull"`.
fn names_class(args: &str, class: &str) -> bool {
    [class.to_string(), class.replace('_', " ")]
        .iter()
        .any(|form| args.contains(&format!("\"{form}\"")))
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
    /// What this turn's own steps acted on, in this tree or an earlier one: the
    /// ids, and the classes, named by a succeeded step that does more than read,
    /// move the robot or speak. An object the robot took is expected to leave its
    /// place; shared by the watches of one turn.
    taken: Arc<Mutex<HashSet<String>>>,
    /// The task's read-only capabilities, by contract id.
    reads: Arc<HashSet<String>>,
    /// Objects found gone at the last reading, which the next must confirm.
    gone: Vec<String>,
    /// Each region's name and goal as the state view showed them, by region id.
    places: HashMap<String, (String, (f64, f64))>,
    /// The goals (by [`goal_key`]) goal_clear found not clear at the last
    /// reading, which the next must confirm.
    blocked: Vec<String>,
}

impl Watch {
    /// Watch `plan`, written from the objects in `then`, Scene's objects by
    /// region, which Scene's list `listed` places, and the regions in `places`,
    /// the state view's; `taken` is what the turn's steps have acted on.
    pub(crate) fn new(
        plan: &Plan,
        then: Value,
        listed: HashMap<String, Listing>,
        places: &Value,
        reads: Arc<HashSet<String>>,
        taken: Arc<Mutex<HashSet<String>>>,
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
            taken,
            places: places
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|place| {
                    let name = place["name"].as_str().unwrap_or_default();
                    Some((
                        place["id"].as_str()?.into(),
                        (name.into(), position(&place["goal"])?),
                    ))
                })
                .collect(),
            blocked: Vec::new(),
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
        if self.acts(step) {
            let acted = self.acted_on(step);
            self.taken
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(acted);
        }
    }

    /// Whether `step` may move what it names: it does more than read, move the
    /// robot or speak.
    fn acts(&self, step: &Step) -> bool {
        !self.reads.contains(&step.contract)
            && !["/navigation/", "/speech/"]
                .iter()
                .any(|kind| step.contract.contains(kind))
    }

    /// What `step` acts on: the objects it names, by id or class, and those it
    /// reads from earlier steps.
    fn acted_on(&self, step: &Step) -> HashSet<String> {
        let mut acted = self.found(std::iter::once(step));
        for (id, listing) in &self.known {
            if crate::speculation::names(&step.args, id) {
                acted.insert(id.clone());
            }
            if names_class(&step.args, &listing.label) {
                acted.insert(listing.label.clone());
            }
        }
        acted
    }

    /// Whether the read set could stop the tree at all: it has a step that does
    /// more than read.
    pub(crate) fn guarded(&self) -> bool {
        self.steps
            .iter()
            .any(|step| !self.reads.contains(&step.contract))
    }

    /// Whether a change could still stop the tree: it has a step left that does
    /// more than read.
    pub(crate) fn armed(&self) -> bool {
        !self.left().is_empty()
    }

    /// The op_ids of the steps that have not ended and do more than read.
    pub(crate) fn left(&self) -> Vec<&str> {
        let left = self.unended();
        let acting = left.filter(|step| !self.reads.contains(&step.contract));
        acting.map(|step| step.op_id.as_str()).collect()
    }

    /// The goal each navigation still to run heads for, where it can be read:
    /// written in its arguments, or the part of a finished step's output it
    /// reads (goal_region's or goal_near's `goal`). Each as (op_id, the goal, a
    /// PoseStamped, and the region whose goal it is, if any: the region a
    /// goal_region step was asked for, or one whose goal the state view showed).
    fn destinations(&self) -> Vec<(&str, Value, Option<&str>)> {
        let navigations = self
            .unended()
            .filter(|step| step.contract.ends_with("navigation/navigate"));
        let mut out = Vec::new();
        for step in navigations {
            let args: Value = serde_json::from_str(&step.args).unwrap_or_default();
            let written = &args["goal"];
            let (goal, region) = match written["var"].as_str() {
                Some(name) => {
                    let Some(source) = self.steps.iter().find(|s| s.bind == name) else {
                        continue;
                    };
                    let Some(output) = self.outputs.get(&source.op_id) else {
                        continue;
                    };
                    let output: Value = serde_json::from_str(output).unwrap_or_default();
                    let path = written["path"].as_array().into_iter().flatten();
                    let goal = path.fold(&output, |value, key| match key {
                        Value::String(key) => &value[key.as_str()],
                        Value::Number(index) => &value[index.as_u64().unwrap_or(0) as usize],
                        _ => &Value::Null,
                    });
                    let asked: Value = serde_json::from_str(&source.args).unwrap_or_default();
                    let region = match source.contract.ends_with("scene/goal_region") {
                        true => asked["region_id"]
                            .as_str()
                            .and_then(|id| self.places.get_key_value(id))
                            .map(|(id, _)| id.as_str()),
                        false => None,
                    };
                    (goal.clone(), region)
                }
                None => {
                    let region = position(written).and_then(|(x, y)| {
                        self.places
                            .iter()
                            .find(|(_, (_, (gx, gy)))| (gx - x).hypot(gy - y) < 0.05)
                            .map(|(region, _)| region.as_str())
                    });
                    (written.clone(), region)
                }
            };
            if position(&goal).is_some() {
                out.push((step.op_id.as_str(), goal, region));
            }
        }
        out
    }

    /// The goals the navigations still to run head for, for goal_clear.
    pub(crate) fn goals(&self) -> Vec<Value> {
        let mut goals: Vec<Value> = self
            .destinations()
            .into_iter()
            .map(|(_, goal, _)| goal)
            .collect();
        goals.dedup();
        goals
    }

    /// The regions whose goal a navigation still to run heads for.
    pub(crate) fn regions(&self) -> Vec<String> {
        let mut regions: Vec<String> = self
            .destinations()
            .into_iter()
            .filter_map(|(_, _, region)| region.map(String::from))
            .collect();
        regions.dedup();
        regions
    }

    /// The objects returned by the finished steps whose outputs `steps` read,
    /// directly or through other finished steps.
    fn found<'a>(&'a self, steps: impl Iterator<Item = &'a Step>) -> HashSet<String> {
        let mut needed: HashSet<&str> = steps.flat_map(|step| vars(&step.args)).collect();
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
        let reached = self
            .steps
            .iter()
            .filter(|step| needed.contains(step.bind.as_str()));
        let outputs = reached.filter_map(|step| self.outputs.get(&step.op_id));
        outputs
            .flat_map(|output| {
                listings(&serde_json::from_str(output).unwrap_or_default()).into_keys()
            })
            .collect()
    }

    fn unended(&self) -> impl Iterator<Item = &Step> {
        self.steps
            .iter()
            .filter(|step| !self.ended.contains(&step.index))
    }

    /// What the tree relies on that has changed at this reading and the one
    /// before: each object that `listed`, Scene's list now, lacks, as "<id>
    /// (<label> in <region> when the tree was written)", and each goal a
    /// navigation still to run heads for that `clear`, goal_clear's answers now
    /// by [`goal_key`] (the reason where a goal is not clear), finds not clear;
    /// nothing unless [`Watch::armed`].
    pub(crate) fn check(
        &mut self,
        listed: &HashMap<String, Listing>,
        clear: &HashMap<String, Option<String>>,
    ) -> Vec<String> {
        self.read(listed, clear, true)
    }

    /// [`Watch::check`] of objects only, on one reading, for the steps from node
    /// `first` on, about to be sent after nodes that have reported `states`.
    pub(crate) fn check_unsent<'a>(
        &mut self,
        first: u32,
        states: impl IntoIterator<Item = &'a RtdlNodeState>,
        listed: &HashMap<String, Listing>,
    ) -> Vec<String> {
        // What a sent step not ended acts on may be moving now; this watch's
        // record of it is not the turn's. No step about to be sent is running yet.
        let taken = self
            .taken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        self.taken = Arc::new(Mutex::new(taken));
        states.into_iter().for_each(|state| self.progressed(state));
        let sent = self
            .unended()
            .filter(|step| step.index < first && self.acts(step));
        let moving: Vec<String> = sent.flat_map(|step| self.acted_on(step)).collect();
        self.taken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(moving);
        self.read(listed, &HashMap::new(), false);
        self.read(listed, &HashMap::new(), false)
    }

    /// [`Watch::check`], where `running` says whether the first step not ended
    /// may be running.
    fn read(
        &mut self,
        listed: &HashMap<String, Listing>,
        clear: &HashMap<String, Option<String>>,
        running: bool,
    ) -> Vec<String> {
        if !self.armed() {
            self.gone.clear();
            self.blocked.clear();
            return Vec::new();
        }
        let mut blocked = Vec::new();
        let mut places = Vec::new();
        for (op_id, goal, region) in self.destinations() {
            let Some(key) = goal_key(&goal) else { continue };
            let Some(Some(reason)) = clear.get(&key) else {
                continue;
            };
            if self.blocked.contains(&key) {
                let (x, y) = position(&goal).unwrap_or_default();
                let place = region
                    .and_then(|region| self.places.get(region))
                    .map_or(String::new(), |(name, _)| format!(" in {name}"));
                places.push(format!(
                    "the goal{place} at ({x:.2}, {y:.2}) that {op_id} navigates to, which is no \
                     longer clear: {reason}"
                ));
            }
            blocked.push(key);
        }
        self.blocked = blocked;
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
        relied.extend(self.found(self.unended()));
        let classes: HashSet<&str> = self
            .known
            .values()
            .map(|listing| listing.label.as_str())
            .collect();
        for class in classes {
            // A search for the class before the step that acts on it lets the
            // tree find another; one after it, checking a place, does not.
            let first = self.unended().find(|step| names_class(&step.args, class));
            if first.is_some_and(|step| !self.reads.contains(&step.contract))
                && !listed.values().any(|listing| listing.label == class)
            {
                let of_class = self
                    .known
                    .iter()
                    .filter(|(_, listing)| listing.label == class);
                relied.extend(of_class.map(|(id, _)| id.clone()));
            }
        }
        // What the turn's own steps acted on is expected to leave its place, and
        // so is what the step running now acts on: Executor does not report a
        // call starting, the first step not ended is running when `running`, and
        // a pick lifts its object off before it ends.
        let mut taken = self
            .taken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let first = self
            .unended()
            .next()
            .filter(|step| running && self.acts(step));
        if let Some(step) = first {
            taken.extend(self.acted_on(step));
        }
        relied.retain(|id| {
            let class = self.known.get(id).map(|listing| listing.label.as_str());
            !taken.contains(id) && !class.is_some_and(|class| taken.contains(class))
        });
        let stands_in = |id: &String| {
            let Some(Listing {
                label,
                at: Some((x, y)),
                size,
            }) = self.known.get(id)
            else {
                return false;
            };
            listed.values().any(|listing| {
                listing.label == *label
                    && listing
                        .at
                        .is_some_and(|(lx, ly)| (lx - x).hypot(ly - y) <= SAME_M + size)
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
            .chain(places)
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
    const PLACE: &str = "robonix/skill/manipulation/place";

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
            size: 0.0,
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
        Watch::new(
            plan,
            then,
            at,
            &json!([]),
            Arc::new(HashSet::from(reads)),
            Arc::default(),
        )
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
                .check(
                    &listed(&[("o1", "cornflakes", 1.0), ("o2", "sofa", 5.0)]),
                    &HashMap::new()
                )
                .is_empty()
        );
        // A flicker is not enough; a second reading confirms.
        assert!(watch.check(&sofa, &HashMap::new()).is_empty());
        assert!(
            watch
                .check(&listed(&[("o1", "cornflakes", 1.0)]), &HashMap::new())
                .is_empty()
        );
        assert!(watch.check(&sofa, &HashMap::new()).is_empty());
        assert_eq!(
            watch.check(&sofa, &HashMap::new()),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
        assert_eq!(watch.left(), ["p-2", "p-3"]);
        // An object the tree does not name does not count.
        let mut other = self::watch(&plan);
        let cornflakes = listed(&[("o1", "cornflakes", 1.0)]);
        other.check(&cornflakes, &HashMap::new());
        assert!(other.check(&cornflakes, &HashMap::new()).is_empty());
    }

    #[test]
    fn one_reading_stops_a_step_about_to_be_sent() {
        // Nodes 0 and 1, the root and the navigation, are sent; the pick is not.
        let plan = tree(&[
            (NAVIGATE, "", json!({"goal": [1.0, 0.0]})),
            (PICK, "", json!({"object_id": "o1"})),
        ]);
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        assert_eq!(
            watch(&plan).check_unsent(2, [], &sofa),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
        let cornflakes = listed(&[("o1", "cornflakes", 1.0)]);
        assert!(watch(&plan).check_unsent(2, [], &cornflakes).is_empty());
    }

    #[test]
    fn a_step_about_to_be_sent_relies_on_what_a_sent_step_found() {
        let plan = tree(&[
            (FIND, "f", json!({"label": "cup"})),
            (
                PICK,
                "",
                json!({"object_id": {"var": "f", "path": ["objects", 0, "id"]}}),
            ),
        ]);
        let cup = json!({"objects": [{"id": "o9", "label": "cup", "x": 8.0, "y": 0.0}]});
        let none = listed(&[("o1", "cornflakes", 1.0)]);
        assert_eq!(
            watch(&plan).check_unsent(2, [&succeeded(1, cup)], &none),
            ["o9 (cup, which an earlier step found)"]
        );
    }

    #[test]
    fn what_a_sent_step_picks_up_may_leave_its_place() {
        let plan = tree(&[
            (PICK, "", json!({"object_id": "o1"})),
            (PLACE, "", json!({"object_id": "o1", "on": "o2"})),
        ]);
        let taken = Arc::default();
        let mut placing = Watch::new(
            &plan,
            json!({"living room": [{"id": "o1", "label": "cornflakes"}, {"id": "o2", "label": "sofa"}]}),
            listed(&[("o1", "cornflakes", 1.0), ("o2", "sofa", 5.0)]),
            &json!([]),
            Arc::new(HashSet::from([FIND.to_string()])),
            Arc::clone(&taken),
        );
        assert!(
            placing
                .check_unsent(2, [], &listed(&[("o2", "sofa", 5.0)]))
                .is_empty()
        );
        // The turn's record of what its steps acted on waits for them to end.
        assert!(taken.lock().unwrap().is_empty());
    }

    #[test]
    fn an_id_scene_folded_into_another_of_its_class_where_it_was_is_not_gone() {
        let plan = tree(&[
            (GOAL_NEAR, "", json!({"object_id": "o1"})),
            (PICK, "", json!({})),
        ]);
        let mut watch = watch(&plan);
        let merged = listed(&[("o7", "cornflakes", 1.2)]);
        watch.check(&merged, &HashMap::new());
        assert!(watch.check(&merged, &HashMap::new()).is_empty());
        // One of its class across the room does not stand for it.
        let far = listed(&[("o7", "cornflakes", 3.0)]);
        watch.check(&far, &HashMap::new());
        assert_eq!(watch.check(&far, &HashMap::new()).len(), 1);
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
        watch.check(&none, &HashMap::new());
        assert_eq!(
            watch.check(&none, &HashMap::new()),
            ["o9 (cup, which an earlier step found)"]
        );
    }

    #[test]
    fn a_step_naming_a_class_relies_on_it_once_scene_lists_none() {
        let pick = tree(&[
            (NAVIGATE, "", json!({"goal": {}})),
            (PICK, "", json!({"object": "cornflakes"})),
        ]);
        let mut watch = watch(&pick);
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        watch.check(&sofa, &HashMap::new());
        assert_eq!(
            watch.check(&sofa, &HashMap::new()),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
        // Another of the class listed anywhere, or a search for it before the
        // pick, keeps it; a search after the pick does not.
        let mut elsewhere = self::watch(&pick);
        let other = listed(&[("o5", "cornflakes", 9.0)]);
        elsewhere.check(&other, &HashMap::new());
        assert!(elsewhere.check(&other, &HashMap::new()).is_empty());
        let search = tree(&[
            (FIND, "", json!({"label": "cornflakes"})),
            (PICK, "", json!({"object": "cornflakes"})),
        ]);
        let mut searching = self::watch(&search);
        searching.check(&sofa, &HashMap::new());
        assert!(searching.check(&sofa, &HashMap::new()).is_empty());
        let verify = tree(&[
            (NAVIGATE, "", json!({"goal": {}})),
            (PICK, "", json!({"object": "cornflakes"})),
            (PLACE, "", json!({"target": "sofa"})),
            (FIND, "", json!({"label": "cornflakes", "anchor": "sofa"})),
        ]);
        let mut verifying = self::watch(&verify);
        verifying.check(&sofa, &HashMap::new());
        assert_eq!(
            verifying.check(&sofa, &HashMap::new()),
            ["o1 (cornflakes in living room when the tree was written)"]
        );
    }

    #[test]
    fn what_an_earlier_tree_of_the_turn_picked_up_is_not_gone() {
        let taken: Arc<std::sync::Mutex<HashSet<String>>> = Arc::default();
        let then = json!({"living room": [{"id": "o1", "label": "cornflakes"}, {"id": "o2", "label": "sofa"}]});
        let reads = Arc::new(HashSet::from([FIND, GOAL_NEAR].map(String::from)));
        let at = listed(&[("o1", "cornflakes", 1.0), ("o2", "sofa", 5.0)]);
        let pick = tree(&[(PICK, "", json!({"object": "cornflakes"}))]);
        let mut picking = Watch::new(
            &pick,
            then.clone(),
            at.clone(),
            &json!([]),
            reads.clone(),
            taken.clone(),
        );
        picking.progressed(&succeeded(1, json!({"ok": true})));
        // The next tree places what the robot holds, which Scene no longer sees where it was.
        let place = tree(&[
            (NAVIGATE, "", json!({"to": "o2"})),
            (PLACE, "", json!({"object_id": "o1", "on": "o2"})),
        ]);
        let mut placing = Watch::new(&place, then, at, &json!([]), reads, taken);
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        placing.check(&sofa, &HashMap::new());
        assert!(placing.check(&sofa, &HashMap::new()).is_empty());
        placing.check(&HashMap::new(), &HashMap::new());
        assert_eq!(
            placing.check(&HashMap::new(), &HashMap::new()),
            ["o2 (sofa in living room when the tree was written)"]
        );
    }

    #[test]
    fn what_the_running_pick_lifts_is_not_gone() {
        // The pick has not ended, so it is running, and its cornflakes leave their place.
        let plan = tree(&[
            (PICK, "", json!({"object": "cornflakes"})),
            (NAVIGATE, "", json!({"to": "o2"})),
        ]);
        let mut watch = watch(&plan);
        let sofa = listed(&[("o2", "sofa", 5.0)]);
        watch.check(&sofa, &HashMap::new());
        assert!(watch.check(&sofa, &HashMap::new()).is_empty());
    }

    #[test]
    fn only_reads_left_or_what_the_tree_took_itself_stops_nothing() {
        let reads = tree(&[(FIND, "", json!({"label": "cornflakes", "anchor": "o1"}))]);
        let mut reading = watch(&reads);
        reading.check(&HashMap::new(), &HashMap::new());
        assert!(reading.check(&HashMap::new(), &HashMap::new()).is_empty());
        // After the pick, the cornflakes it took leave the watch; the sofa it still goes to does not.
        let fetch = tree(&[
            (PICK, "", json!({"object_id": "o1"})),
            (NAVIGATE, "", json!({"to": "o2"})),
        ]);
        let mut fetching = watch(&fetch);
        fetching.progressed(&succeeded(1, json!({"ok": true})));
        fetching.check(&HashMap::new(), &HashMap::new());
        assert_eq!(
            fetching.check(&HashMap::new(), &HashMap::new()),
            ["o2 (sofa in living room when the tree was written)"]
        );
    }

    #[test]
    fn a_goal_the_robot_heads_for_that_goal_clear_finds_not_clear_twice_stops_it() {
        let places = json!([{"id": "r1", "name": "kitchen", "goal": {"pose": {"position": {"x": 1.0, "y": 2.0}}}}]);
        let plan = tree(&[
            (
                "robonix/system/scene/goal_region",
                "g",
                json!({"region_id": "r1"}),
            ),
            (
                NAVIGATE,
                "",
                json!({"goal": {"var": "g", "path": ["goal"]}}),
            ),
        ]);
        let reads = Arc::new(HashSet::from(
            [FIND, GOAL_NEAR, "robonix/system/scene/goal_region"].map(String::from),
        ));
        let mut watch = Watch::new(
            &plan,
            json!({}),
            HashMap::new(),
            &places,
            reads.clone(),
            Arc::default(),
        );
        let goal =
            json!({"header": {"frame_id": "map"}, "pose": {"position": {"x": 1.0, "y": 2.0}}});
        watch.progressed(&succeeded(
            1,
            json!({"reachable": true, "x": 1.0, "y": 2.0, "goal": goal}),
        ));
        assert_eq!(watch.goals(), [goal]);
        assert_eq!(watch.regions(), ["r1"]);
        let answer = |reason: Option<&str>| {
            HashMap::from([("1.00,2.00".to_string(), reason.map(String::from))])
        };
        assert!(watch.check(&HashMap::new(), &answer(None)).is_empty());
        let marked = answer(Some("Navigation's costmap marks it occupied"));
        assert!(watch.check(&HashMap::new(), &marked).is_empty());
        assert_eq!(
            watch.check(&HashMap::new(), &marked),
            [
                "the goal in kitchen at (1.00, 2.00) that p-2 navigates to, which is no longer clear: \
              Navigation's costmap marks it occupied"
            ]
        );
        // A goal written out that the state view showed for a region counts as that region's.
        let literal = tree(&[(
            NAVIGATE,
            "",
            json!({"goal": {"pose": {"position": {"x": 1.0, "y": 2.0}}}}),
        )]);
        let mut going = Watch::new(
            &literal,
            json!({}),
            HashMap::new(),
            &places,
            reads,
            Arc::default(),
        );
        assert_eq!(going.regions(), ["r1"]);
        going.check(&HashMap::new(), &marked);
        assert_eq!(going.check(&HashMap::new(), &marked).len(), 1);
    }

    #[test]
    fn another_record_of_a_person_within_their_extent_stands_for_them() {
        let then = json!({"living room": [{"id": "h1", "label": "person"}]});
        let at = HashMap::from([(
            "h1".to_string(),
            Listing {
                label: "person".into(),
                at: Some((0.0, 0.0)),
                size: 0.6,
            },
        )]);
        let plan = tree(&[
            (GOAL_NEAR, "", json!({"object_id": "h1"})),
            (NAVIGATE, "", json!({})),
        ]);
        let reads = Arc::new(HashSet::from([FIND, GOAL_NEAR].map(String::from)));
        let mut watch = Watch::new(&plan, then, at, &json!([]), reads, Arc::default());
        let moved = |x: f64| {
            HashMap::from([(
                "h9".to_string(),
                Listing {
                    label: "person".into(),
                    at: Some((x, 0.0)),
                    size: 0.6,
                },
            )])
        };
        watch.check(&moved(1.0), &HashMap::new());
        assert!(watch.check(&moved(1.0), &HashMap::new()).is_empty());
        watch.check(&moved(2.0), &HashMap::new());
        assert_eq!(watch.check(&moved(2.0), &HashMap::new()).len(), 1);
    }
}
