//! Commands as clauses ("Look for a pepsi in the kitchen | then get it | and
//! bring it to Charlie in the kitchen"), and which clause each step of a run
//! served, so that a kept run's steps for one clause can serve that clause of
//! another command.
//!
//! A step goes to the clause whose entities it names, or whose kind of action
//! makes that kind of step (a pick: a take), going forward only: a clause keeps
//! its steps until the step that ends its own work has ended well (a take's
//! pick, a find's find of something other than a person), and a step the
//! current clause cannot make, but a later one does, moves on early (the
//! planner skipped a step). Reading the date may come first and goes to the
//! clause that says it. A last drive back to where the task began that no
//! clause asks for is the task's return.

use super::{HOME_M, Point, SAME_GOAL_M, Segment, compared, contains_word, distance, nodes, norm};
use serde_json::Value;
use std::collections::HashMap;

/// Words that join clauses before an action.
const JOINS: &[&str] = &["then", "and"];

/// The word a clause begins with, as [`compared`] makes phrasings one ("look
/// for" a find, "say hello to" a greeting), with the kind of action it is.
const ACTIONS: &[(&str, Action)] = &[
    ("go", Action::Go),
    ("find", Action::Find),
    ("take", Action::Take),
    ("pick", Action::Take),
    ("give", Action::Give),
    ("put", Action::Put),
    ("place", Action::Put),
    ("throw", Action::Put),
    ("meet", Action::Meet),
    ("greet", Action::Meet),
    ("say", Action::Say),
    ("answer", Action::Say),
    ("describe", Action::Say),
    ("count", Action::Say),
    ("follow", Action::Follow),
    ("guide", Action::Follow),
    ("lead", Action::Follow),
];

/// Words of a clause that are not entities: relations, pronouns, questions.
const NOT_NAMES: &[&str] = &[
    "a", "the", "it", "them", "me", "you", "your", "to", "from", "in", "into", "on", "onto", "at",
    "of", "for", "with", "up", "is", "what", "which", "how", "many", "there", "are",
];

/// The kinds of step that act, rather than read.
const ACTS: &[&str] = &[
    "navigate",
    "find",
    "pick",
    "place",
    "ask_name",
    "speak",
    "run_command",
    "follow",
    "guide",
];

/// The kind of action a clause asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    /// Go somewhere.
    Go,
    /// Look for an object or a person.
    Find,
    /// Pick an object up, perhaps from a place it names.
    Take,
    /// Hand an object over: to the operator or to someone it names.
    Give,
    /// Put an object down somewhere, the trash included.
    Put,
    /// Meet or greet someone.
    Meet,
    /// Say or tell something, or answer about what is somewhere.
    Say,
    /// Follow or guide someone.
    Follow,
}

impl Action {
    /// The kinds of step this action makes without naming one of its entities.
    fn makes(self, kind: &str) -> bool {
        let kinds: &[&str] = match self {
            Self::Go => &["navigate"],
            Self::Find => &["navigate", "find", "snapshot", "depth_snapshot"],
            Self::Take => &["goal_near", "pick"],
            Self::Give => &["place", "ask_name", "goal_near"],
            Self::Put => &["place"],
            Self::Meet => &["find", "goal_near", "ask_name", "speak"],
            Self::Say => &["speak", "run_command", "snapshot", "depth_snapshot", "find"],
            Self::Follow => &["follow", "guide", "navigate"],
        };
        kinds.contains(&kind)
    }

    /// The kind of step that ends this action's own work.
    fn ends_with(self) -> &'static str {
        match self {
            Self::Go => "navigate",
            Self::Find => "find",
            Self::Take => "pick",
            Self::Give | Self::Put => "place",
            Self::Meet => "ask_name",
            Self::Say => "speak",
            Self::Follow => "follow",
        }
    }
}

/// One clause of a command.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Clause {
    /// Its words as the command has them.
    pub(super) text: String,
    /// The kind of action it asks for.
    pub(super) action: Action,
    /// The entities it names (runs of words that are not actions or relations), lower case.
    names: Vec<String>,
    /// Whether it hands something to the operator ("to me").
    to_me: bool,
}

impl Clause {
    /// The entities it names.
    pub(super) fn names(&self) -> &[String] {
        &self.names
    }

    /// The kind of step that ends its own work.
    pub(super) fn ends_with(&self) -> &'static str {
        self.action.ends_with()
    }

    /// Whether `step`, of this clause, is the one that ends its own work: of
    /// its own last kind, ended well, a find not of a person (unless it looks
    /// for one), a place on what it names.
    fn ended_by(&self, step: &Step) -> bool {
        let named = |text: &str| self.names.iter().any(|name| contains_word(text, name));
        step.ok
            && self.action.ends_with() == step.kind
            && match self.action {
                Action::Find => step.label != "person" || self.names.iter().any(|n| n == "person"),
                Action::Give | Action::Put => {
                    self.to_me || self.names.is_empty() || named(&step.text)
                }
                _ => true,
            }
    }
}

/// The spans that differ between a kept clause and a new one of the same
/// kind of action ([`super::align`]).
pub(super) fn align_clause(kept: &Clause, new: &Clause) -> Option<Vec<(String, String)>> {
    if kept.action != new.action {
        return None;
    }
    super::align(&kept.text, &new.text)
}

/// The command a task's text ends with; the text before it sets the scene.
pub(super) fn command_line(text: &str) -> &str {
    text.trim().rsplit('\n').next().unwrap_or_default()
}

/// The command a task's text ends with, split into clauses where "then" or
/// "and" comes before an action.
pub(super) fn split(text: &str) -> Vec<Clause> {
    let (words, tokens) = compared(command_line(text));
    let action_of = |word: &str| ACTIONS.iter().find(|(w, _)| *w == word).map(|(_, a)| *a);
    let mut cuts = vec![0];
    for at in 1..tokens.len() {
        let joined = JOINS.contains(&tokens[at - 1].0.as_str());
        if joined && action_of(&tokens[at].0).is_some() && at > *cuts.last().unwrap_or(&0) + 1 {
            cuts.push(at - 1);
        }
    }
    cuts.push(tokens.len());
    let mut out = Vec::new();
    for pair in cuts.windows(2) {
        let mut part = &tokens[pair[0]..pair[1]];
        if part
            .first()
            .is_some_and(|(w, _)| JOINS.contains(&w.as_str()))
        {
            part = &part[1..];
        }
        let Some(action) = part.first().and_then(|(w, _)| action_of(w)) else {
            continue;
        };
        let mut names = Vec::new();
        let mut run: Vec<&str> = Vec::new();
        for (word, _) in &part[1..] {
            if NOT_NAMES.contains(&word.as_str()) || action_of(word).is_some() || word.is_empty() {
                if !run.is_empty() {
                    names.push(run.join(" "));
                    run.clear();
                }
            } else {
                run.push(word);
            }
        }
        if !run.is_empty() {
            names.push(run.join(" "));
        }
        let lower: Vec<&str> = part.iter().map(|(w, _)| w.as_str()).collect();
        let range = part[0].1.start..part[part.len() - 1].1.end;
        out.push(Clause {
            text: words[range].join(" "),
            action,
            to_me: lower.windows(2).any(|w| w == ["to", "me"])
                || lower.get(1).is_some_and(|w| *w == "me"),
            names,
        });
    }
    out
}

/// One step of a tree, as attribution sees it.
#[derive(Debug)]
pub(super) struct Step {
    /// Its capability's last name (navigate, find, pick, ...).
    pub(super) kind: String,
    /// Its arguments' text.
    text: String,
    /// What a find looks for, lower case.
    label: String,
    /// The names of the places its navigation goal is at, if it has one.
    at: Vec<String>,
    /// Whether it drives back to where the task began.
    home: bool,
    /// Whether it ended well (a find: found something).
    pub(super) ok: bool,
    /// Whether it ran at all (it ended well, found nothing or failed).
    ran: bool,
}

/// One step of a run with the clause it served.
#[derive(Debug)]
pub(super) struct Owned {
    /// The tree it is in, an index into the run.
    pub(super) segment: usize,
    /// The top-level step of the tree it is in.
    pub(super) child: usize,
    /// The clause it served: an index; the number of clauses for the return.
    pub(super) clause: usize,
    /// Whether it went to its clause out of the command's order (reading the date first).
    aside: bool,
    /// The step.
    pub(super) step: Step,
}

/// The clause each step of a run served, in the order the steps are numbered.
/// `places` are the map's places with their goals; `start` where the task began.
pub(super) fn owners(
    clauses: &[Clause],
    segments: &[Segment],
    places: &HashMap<String, Point>,
    start: Option<Point>,
) -> Vec<Owned> {
    let ret = clauses.len();
    let mut out: Vec<Owned> = Vec::new();
    let mut fallback = Vec::new(); // each step's clause if it is not the return
    if clauses.is_empty() {
        return out;
    }
    let mut cur = 0;
    let mut done = vec![false; clauses.len()];
    for (s, segment) in segments.iter().enumerate() {
        let Ok(reply) = serde_json::from_str::<Value>(&segment.reply) else {
            continue;
        };
        let mut at = 0;
        for (c, child) in top_level(&reply["rtdl"]).into_iter().enumerate() {
            for node in crate::speculation::do_nodes(child) {
                let step = read(node, segment, at, places, start);
                at += 1;
                let (owner, aside);
                (owner, cur, aside) = step_clause(clauses, cur, &mut done, &step);
                let home = step.home && !clauses[cur..].iter().any(|c| c.to_me);
                fallback.push(owner);
                out.push(Owned {
                    segment: s,
                    child: c,
                    clause: if home { ret } else { owner },
                    aside,
                    step,
                });
            }
        }
    }
    // Only a drive back that no other step follows is the return.
    if let Some(last) = out.iter().rposition(|o| o.clause != ret) {
        for (owned, own) in out.iter_mut().zip(fallback).take(last + 1) {
            owned.clause = own;
        }
    }
    out
}

/// A tree's top-level steps: a sequence root's children, else the root.
pub(super) fn top_level(tree: &Value) -> Vec<&Value> {
    match (tree["op"].as_str(), tree["children"].as_array()) {
        (Some("sequence"), Some(children)) => children.iter().collect(),
        _ if tree.is_object() => vec![tree],
        _ => Vec::new(),
    }
}

/// The kinds of the steps of top-level steps `children` of tree `segment` that act.
pub(super) fn acting_kinds(segment: &Segment, children: &[usize]) -> Vec<String> {
    let Ok(reply) = serde_json::from_str::<Value>(&segment.reply) else {
        return Vec::new();
    };
    let mut at = 0;
    let mut out = Vec::new();
    for (c, child) in top_level(&reply["rtdl"]).into_iter().enumerate() {
        for _ in crate::speculation::do_nodes(child) {
            let kind = kind_of(segment, at);
            at += 1;
            if children.contains(&c) && ACTS.contains(&kind.as_str()) {
                out.push(kind);
            }
        }
    }
    out
}

fn kind_of(segment: &Segment, at: usize) -> String {
    let contract = segment
        .contracts
        .get(at)
        .map(String::as_str)
        .unwrap_or_default();
    contract.rsplit('/').next().unwrap_or_default().to_string()
}

fn read(
    node: &Value,
    segment: &Segment,
    at: usize,
    places: &HashMap<String, Point>,
    start: Option<Point>,
) -> Step {
    let goal = crate::watch::position(&node["args"]["goal"]);
    let near = |g: Point| {
        let near = places
            .iter()
            .filter(|(_, p)| distance(**p, g) <= SAME_GOAL_M);
        near.map(|(name, _)| name.clone()).collect()
    };
    let text = nodes(&node["args"])
        .into_iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    Step {
        kind: kind_of(segment, at),
        text,
        label: node["args"]["label"].as_str().map(norm).unwrap_or_default(),
        at: goal.map(near).unwrap_or_default(),
        home: goal.is_some_and(|g| start.is_some_and(|s| distance(s, g) <= HOME_M)),
        ok: segment.signature.get(at).is_some_and(|s| s == "ok"),
        ran: segment
            .signature
            .get(at)
            .is_some_and(|s| s == "ok" || s == "empty" || s == "failed"),
    }
}

/// The clause a step goes to, the clause now after it, and whether it went
/// there out of the command's order; given the clause now and which clauses'
/// own work is done. A step the clause now neither names nor makes, naming an
/// earlier clause whose work is not done, goes back to it (a find tried again
/// after a later step was cancelled); a step that never ran (cancelled,
/// skipped) moves nothing.
fn step_clause(
    clauses: &[Clause],
    cur: usize,
    done: &mut [bool],
    step: &Step,
) -> (usize, usize, bool) {
    let names = |k: usize| {
        clauses[k].names.iter().any(|name| {
            contains_word(&step.text, name)
                || step.at.iter().any(|place| contains_word(place, name))
        }) || (step.home && clauses[k].to_me)
    };
    let makes = |k: usize| clauses[k].action.makes(&step.kind) || (step.home && clauses[k].to_me);
    let ends = |k: usize| clauses[k].action.ends_with() == step.kind;
    if step.kind == "run_command"
        && let Some(k) = (0..clauses.len()).find(|&k| clauses[k].action == Action::Say)
    {
        return (k, cur, k != cur);
    }
    let ahead: Vec<usize> = (cur + 1..clauses.len())
        .filter(|&k| names(k) || makes(k))
        .collect();
    let back = (0..cur).rev().find(|&k| !done[k] && names(k));
    let mut next = cur;
    if !names(cur)
        && !makes(cur)
        && let Some(k) = back
    {
        next = k;
    } else if !done[cur] {
        let mine = names(cur) || makes(cur) || ends(cur);
        if !mine && let Some(k) = (cur + 1..clauses.len()).find(|&k| makes(k)) {
            next = k;
        }
    } else if !ends(cur) && cur + 1 < clauses.len() && (names(cur + 1) || makes(cur + 1)) {
        next = cur + 1;
    } else if ends(cur) || makes(cur) {
        // Its own kind of step again: another person asked, another look.
    } else if let Some(&k) = ahead.first().filter(|_| !names(cur)) {
        next = k;
    } else if cur + 1 < clauses.len() && clauses[cur + 1].action != Action::Say && !names(cur) {
        next = cur + 1;
    }
    if !step.ran {
        return (next, cur, false);
    }
    if clauses[next].ended_by(step) {
        done[next] = true;
    }
    (next, next, false)
}

/// Which clauses a run has done: those a step of theirs ended
/// ([`Clause::ended_by`]; a meeting only once the person it names answered),
/// and those the run has gone past (a step of a later clause ended well, a
/// reading of the date aside); the return once the robot's last drive went
/// back where the task began.
pub(super) fn done(
    clauses: &[Clause],
    segments: &[Segment],
    places: &HashMap<String, Point>,
    start: Option<Point>,
) -> Vec<bool> {
    let owned = owners(clauses, segments, places, start);
    let mut out = vec![false; clauses.len() + 1];
    for o in &owned {
        let Some(clause) = clauses.get(o.clause) else {
            continue;
        };
        let heard = &segments[o.segment].heard;
        let met = clause.action != Action::Meet
            || heard
                .iter()
                .any(|h| clause.names.iter().any(|name| h.eq_ignore_ascii_case(name)));
        out[o.clause] |= clause.ended_by(&o.step) && met;
    }
    if let Some(furthest) = owned
        .iter()
        .filter(|o| o.step.ok && !o.aside)
        .map(|o| o.clause)
        .max()
    {
        out.iter_mut().take(furthest).for_each(|d| *d = true);
    }
    let last_drive = owned.iter().rev().find(|o| o.step.kind == "navigate");
    out[clauses.len()] = last_drive.is_some_and(|o| o.step.ok && o.step.home);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Where each top-level step of each tree went: the clause all its steps
    /// served, or `None` where they served more than one.
    fn attribute(
        clauses: &[Clause],
        segments: &[Segment],
        places: &HashMap<String, Point>,
        start: Option<Point>,
    ) -> Vec<Vec<Option<usize>>> {
        let owned = owners(clauses, segments, places, start);
        segments
            .iter()
            .enumerate()
            .map(|(s, segment)| {
                let Ok(reply) = serde_json::from_str::<Value>(&segment.reply) else {
                    return Vec::new();
                };
                (0..top_level(&reply["rtdl"]).len())
                    .map(|c| {
                        let mut ks = owned
                            .iter()
                            .filter(|o| o.segment == s && o.child == c)
                            .map(|o| o.clause);
                        let first = ks.next()?;
                        ks.all(|k| k == first).then_some(first)
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn a_command_splits_where_an_action_follows_then_or_and() {
        let text = "You are at the instruction point and gives you this command:\n\
                    Look for a pepsi in the kitchen then get it and bring it to Charlie in the kitchen";
        let clauses = split(text);
        let texts: Vec<&str> = clauses.iter().map(|c| c.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                "Look for a pepsi in the kitchen",
                "get it",
                "bring it to Charlie in the kitchen"
            ]
        );
        assert_eq!(clauses[0].names, ["pepsi", "kitchen"]);
        assert_eq!(clauses[2].names, ["charlie", "kitchen"]);
        let me = split("Get a red bull from the cabinet and give it to me");
        assert_eq!(me[0].names, ["red bull", "cabinet"]);
        assert!(me[1].to_me && me[1].action == Action::Give);
        assert_eq!(
            split("Tell me what is the biggest object on the coffee table").len(),
            1
        );
    }

    /// A kept run of "Find a cup in the kitchen then take it and give it to me".
    fn fetch() -> (Vec<Clause>, Vec<Segment>, HashMap<String, Point>) {
        let goal = |x: f64, y: f64| json!({"pose": {"position": {"x": x, "y": y}}});
        let tree =
            |children: Value| json!({"rtdl": {"op": "sequence", "children": children}}).to_string();
        let segments = vec![
            Segment {
                reply: tree(json!([
                    {"op": "do", "cap": "nav", "args": {"goal": goal(1.0, 1.0)}},
                    {"op": "do", "cap": "find", "args": {"label": "cup", "region": "kitchen"}},
                ])),
                contracts: vec!["x/navigate".into(), "x/find".into()],
                signature: vec!["ok".into(), "ok".into()],
                ..Default::default()
            },
            Segment {
                reply: tree(json!([
                    {"op": "do", "cap": "near", "args": {"object_id": "scene.object.1"}, "bind": "g"},
                    {"op": "do", "cap": "nav", "args": {"goal": {"var": "g", "path": ["goal"]}}},
                    {"op": "do", "cap": "pick", "args": {"object": "cup"}},
                    {"op": "do", "cap": "nav", "args": {"goal": goal(0.0, 0.0)}},
                    {"op": "do", "cap": "place", "args": {"target": "adel"}},
                ])),
                contracts: [
                    "x/goal_near",
                    "x/navigate",
                    "x/pick",
                    "x/navigate",
                    "x/place",
                ]
                .map(String::from)
                .into(),
                signature: vec!["ok".into(); 5],
                ..Default::default()
            },
        ];
        let places = HashMap::from([("kitchen".to_string(), (1.0, 1.0))]);
        let clauses = split("Find a cup in the kitchen then take it and give it to me");
        (clauses, segments, places)
    }

    #[test]
    fn each_top_level_step_goes_to_the_clause_it_served() {
        let (clauses, segments, places) = fetch();
        let parts = attribute(&clauses, &segments, &places, Some((0.0, 0.0)));
        assert_eq!(parts[0], [Some(0), Some(0)]);
        assert_eq!(parts[1], [Some(1), Some(1), Some(1), Some(2), Some(2)]);
        // Handing it over at the instruction point brought the robot back too.
        assert_eq!(
            done(&clauses, &segments, &places, Some((0.0, 0.0))),
            [true, true, true, true]
        );
    }

    #[test]
    fn a_clause_is_done_by_its_step_wherever_the_tree_nests_it() {
        let (clauses, mut segments, places) = fetch();
        // The take and the delivery in one top-level step, under conditions.
        segments[1].reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "near", "args": {"object_id": "scene.object.1"}, "bind": "g"},
            {"op": "if", "condition": true, "then": {"op": "sequence", "children": [
                {"op": "do", "cap": "nav", "args": {"goal": {"var": "g", "path": ["goal"]}}},
                {"op": "do", "cap": "pick", "args": {"object": "cup"}},
                {"op": "if", "condition": true, "then": {"op": "sequence", "children": [
                    {"op": "do", "cap": "nav", "args": {"goal": {"pose": {"position": {"x": 0.0, "y": 0.0}}}}},
                    {"op": "do", "cap": "place", "args": {"target": "adel"}},
                ]}},
            ]}},
        ]}})
        .to_string();
        let parts = attribute(&clauses, &segments, &places, Some((0.0, 0.0)));
        assert_eq!(parts[1], [Some(1), None]);
        assert_eq!(
            done(&clauses, &segments, &places, Some((0.0, 0.0))),
            [true, true, true, true]
        );
    }

    #[test]
    fn phrasings_of_a_greeting_are_a_meeting() {
        let clauses = split("Say hello to Jules in the living room and tell the day of the week");
        assert_eq!(clauses[0].action, Action::Meet);
        assert_eq!(clauses[0].names, ["jules", "living room"]);
        assert_eq!(clauses[1].action, Action::Say);
    }

    #[test]
    fn progress_counts_what_ended_well_and_what_the_run_went_past() {
        let (clauses, mut segments, places) = fetch();
        // The cup not found, only a person: the find is not done.
        segments[0].reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "find", "args": {"label": "cup", "region": "kitchen"}},
            {"op": "do", "cap": "find", "args": {"label": "person", "region": "kitchen"}},
        ]}})
        .to_string();
        segments[0].signature = vec!["empty".into(), "ok".into()];
        segments[0].contracts = vec!["x/find".into(), "x/find".into()];
        let first = &segments[..1];
        assert_eq!(done(&clauses, first, &places, Some((0.0, 0.0))), [false; 4]);
        // A pick that failed, then one that worked: the take is done.
        segments[1].reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "pick", "args": {"object": "cup"}},
            {"op": "do", "cap": "near", "args": {"object_id": "scene.object.1"}},
            {"op": "do", "cap": "pick", "args": {"object": "cup"}},
        ]}})
        .to_string();
        segments[1].contracts = ["x/pick", "x/goal_near", "x/pick"].map(String::from).into();
        segments[1].signature = vec!["failed".into(), "ok".into(), "ok".into()];
        let progress = done(&clauses, &segments, &places, Some((0.0, 0.0)));
        // The find went past by the take; the delivery not begun.
        assert_eq!(progress, [true, true, false, false]);
    }

    #[test]
    fn a_place_on_another_table_does_not_put_it_where_the_clause_says() {
        let (_, mut segments, places) = fetch();
        let clauses = split("Find a cup in the kitchen then take it and place it on the sofa");
        segments[1].reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "pick", "args": {"object": "cup"}},
            {"op": "do", "cap": "place", "args": {"target": "dinner table"}},
        ]}})
        .to_string();
        segments[1].contracts = ["x/pick", "x/place"].map(String::from).into();
        segments[1].signature = vec!["ok".into(), "ok".into()];
        let progress = done(&clauses, &segments, &places, Some((0.0, 0.0)));
        assert_eq!(progress, [true, true, false, false]);
    }

    #[test]
    fn a_take_tried_again_after_cancelled_steps_is_still_the_take() {
        let goal = |x: f64, y: f64| json!({"pose": {"position": {"x": x, "y": y}}});
        let tree = |steps: Value, contracts: &[&str], ended: &[&str]| Segment {
            reply: json!({"rtdl": {"op": "sequence", "children": steps}}).to_string(),
            contracts: contracts.iter().map(|c| format!("x/{c}")).collect(),
            signature: ended.iter().map(|e| e.to_string()).collect(),
            ..Default::default()
        };
        let segments = vec![
            tree(
                json!([{"op": "do", "args": {"goal": goal(1.0, 1.0)}},
                    {"op": "do", "args": {"label": "soju", "anchor": "cabinet"}}]),
                &["navigate", "find"],
                &["ok", "empty"],
            ),
            tree(
                json!([{"op": "do", "args": {"object": "soju"}},
                    {"op": "do", "args": {"goal": goal(5.0, 5.0)}},
                    {"op": "do", "args": {"target": "bedside table"}},
                    {"op": "do", "args": {"goal": goal(0.0, 0.0)}}]),
                &["pick", "navigate", "place", "navigate"],
                &["failed", "canceled", "canceled", "canceled"],
            ),
            tree(
                json!([{"op": "do", "args": {"goal": goal(1.0, 1.0)}},
                    {"op": "do", "args": {"label": "soju", "anchor": "cabinet"}}]),
                &["navigate", "find"],
                &["ok", "ok"],
            ),
            tree(
                json!([{"op": "do", "args": {"object": "soju"}}]),
                &["pick"],
                &["failed"],
            ),
        ];
        let places = HashMap::from([
            ("cabinet".to_string(), (1.0, 1.0)),
            ("bedside table".to_string(), (5.0, 5.0)),
        ]);
        let clauses = split("Get a soju from the cabinet and place it on the bedside table");
        let progress = done(&clauses, &segments, &places, Some((0.0, 0.0)));
        assert_eq!(progress, [false, false, false]);
    }

    #[test]
    fn a_drive_back_no_clause_asks_for_is_the_return() {
        let (_, mut segments, places) = fetch();
        let clauses = split("Find a cup in the kitchen then take it and throw it in the trash");
        segments[1].reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "pick", "args": {"object": "cup"}},
            {"op": "do", "cap": "place", "args": {"target": "kitchen trash bin"}},
            {"op": "do", "cap": "nav", "args": {"goal": {"pose": {"position": {"x": 0.0, "y": 0.0}}}}},
        ]}})
        .to_string();
        segments[1].contracts = ["x/pick", "x/place", "x/navigate"].map(String::from).into();
        segments[1].signature = vec!["ok".into(); 3];
        let parts = attribute(&clauses, &segments, &places, Some((0.0, 0.0)));
        assert_eq!(parts[1], [Some(1), Some(2), Some(3)]);
    }
}
