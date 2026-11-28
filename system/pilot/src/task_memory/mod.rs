//! Task memory (`"task_memory"` in a task's `context_json`; the README says
//! what it keeps and when a kept reply serves). A kept reply holds what its run
//! found out: the objects it met, goals worked out from places, values it then
//! said. Each is bound to this run or refuses the reply, since a stale one is a
//! wrong action or a wrong answer.

use crate::experience::distance;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNodeState};
use crate::planner::{ExecutorConn, RTDL_DO};
use crate::state_context::{self, SceneProviders};
use crate::tasks::{DONE, TaskTable};
use crate::watch::position;
use robonix_scribe::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

mod clauses;
use clauses::Clause;

/// A point on the map.
type Point = (f64, f64);
/// Spans of a kept command and what a new one has in their place.
type Spans = Vec<(String, String)>;

/// At most this many spans may differ between a kept command and a new one.
const MAX_SPANS: usize = 3;
/// A differing span has at most this many words on either side.
const MAX_SPAN_WORDS: usize = 4;
/// Kept runs per command; older ones are dropped.
const KEEP: usize = 3;
/// Kept runs in all; the oldest are dropped.
const MAX_RUNS: usize = 200;
/// A navigation goal this close to a place's goal goes to that place.
const SAME_GOAL_M: f64 = 0.3;
/// A navigation goal this close to where the kept task began returns there.
const HOME_M: f64 = 0.5;
/// Words a differing span may not hold: a changed action or relation is
/// another task, not the same one with other entities.
const NOT_ENTITIES: &[&str] = &[
    "a",
    "an",
    "the",
    "it",
    "me",
    "him",
    "her",
    "them",
    "and",
    "then",
    "to",
    "from",
    "in",
    "into",
    "on",
    "onto",
    "at",
    "of",
    "for",
    "with",
    "take",
    "grasp",
    "get",
    "fetch",
    "bring",
    "give",
    "deliver",
    "put",
    "place",
    "throw",
    "find",
    "locate",
    "look",
    "meet",
    "say",
    "tell",
    "greet",
    "introduce",
    "follow",
    "guide",
    "lead",
    "go",
    "navigate",
    "count",
    "answer",
    "ask",
    "salute",
    "describe",
];
/// Keys of arguments whose text is said to a person.
const SAID: &[&str] = &["text", "message", "say", "speech", "utterance"];
/// Words a count may be said in ("one" is left out: "the red one").
const NUMBERS: &[&str] = &[
    "zero", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
    "twelve",
];
/// Keys whose values name parts of the tree rather than the world, which
/// spans are not put into.
const STRUCTURE: &[&str] = &["op", "cap", "bind", "var", "op_id"];

/// Serialises saving across a process's turns, each of which reads the store,
/// adds its run and writes it back.
static SAVING: Mutex<()> = Mutex::new(());

/// Which memory a task uses.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// One kept run's replies, as long as each tree ends as it did there and
    /// without a failure.
    Paths,
    /// Any kept run's next reply where its trees so far ran the same steps as
    /// this run's and ended as they did, failures included.
    Programs,
    /// As [`Mode::Programs`], and where no kept run serves the whole command,
    /// the steps a kept run made for a clause serve that clause of this one.
    Clauses,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Paths => "paths",
            Self::Programs => "programs",
            Self::Clauses => "clauses",
        }
    }
}

/// One tree a task dispatched, as kept.
#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq)]
struct Segment {
    /// The reply that dispatched it, as replayed.
    reply: String,
    /// Its `do` steps' op_ids, in node order.
    op_ids: Vec<String>,
    /// Their capabilities.
    contracts: Vec<String>,
    /// How each `do` step ended: ok, empty (a find that found nothing), failed,
    /// canceled or skipped; empty until the tree ends.
    signature: Vec<String>,
    /// Names its steps heard (`name` fields): a replay must hear the same, and
    /// a later reply saying one would repeat it from this run.
    heard: Vec<String>,
    /// Labels its steps found, which a later reply saying them would repeat.
    found: Vec<String>,
    /// Scene object ids the state view or earlier steps had listed, with labels.
    labels: HashMap<String, String>,
    /// Place and region names (lower case) with their goal on the map.
    places: HashMap<String, Point>,
    /// Where the robot was on the map when the tree went out, if the state view knew.
    robot: Option<Point>,
    /// Whether the reply came from memory.
    #[serde(skip)]
    reused: bool,
    /// Its plan's id and the node indices of its `do` steps.
    #[serde(skip)]
    plan: Option<(String, Vec<u32>)>,
}

/// A finished task's trees, as kept.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Episode {
    /// The command, as the task's first message gave it.
    command: String,
    /// Its trees, in the order they went out.
    segments: Vec<Segment>,
    /// When it was kept, in Unix seconds, so that runs found broken afterwards
    /// can be told apart.
    when: f64,
}

/// The kept runs of one mode, as saved in the sessions root.
#[derive(Serialize, Deserialize, Default)]
struct Store {
    /// Oldest first.
    episodes: Vec<Episode>,
}

/// A kept run that can serve this command, with the spans to put in.
#[derive(Clone, Debug)]
struct Aligned {
    /// The kept run.
    episode: Episode,
    /// Each span of its command and what this command has there instead.
    spans: Vec<(String, String)>,
}

/// What a served tree is made from.
struct Source<'a> {
    /// The kept run.
    episode: &'a Episode,
    /// The kept tree it comes from, an index into the run's segments.
    segment: usize,
    /// The reply to bind: the kept tree's, or a tree of its steps for one clause.
    reply: Value,
    /// The spans to put in.
    spans: Vec<(String, String)>,
    /// Whether it is the kept tree whole, which may name earlier kept steps.
    whole: bool,
}

/// A kept run's steps for one clause of its command, which can serve that
/// clause of another command.
#[derive(Clone, Debug)]
struct Program {
    /// The kept run, an index into `TaskMemory::kept`.
    episode: usize,
    /// The clause of its command, an index; the number of its clauses for its return.
    clause: usize,
    /// Its steps, tree by tree: each tree's index in the run and the top-level
    /// steps of it that served the clause. Each part is served as a tree of its
    /// own, the next only if the last ended as it did in the kept run.
    parts: Vec<(usize, Vec<usize>)>,
}

/// A task's memory while its turn runs.
pub(crate) struct TaskMemory {
    /// Which memory the task uses.
    mode: Mode,
    /// The task's command, which this run is kept under.
    command: String,
    /// The task the command's message is filed under, which served replies name.
    task_id: String,
    /// The session's user messages when the turn began; another one stops memory.
    messages: usize,
    /// The decision model's threshold for picking an object (`tau_choice`).
    tau: f64,
    /// The kept runs that can serve this command, best first.
    aligned: Vec<Aligned>,
    /// This run's trees so far.
    run: Vec<Segment>,
    /// Every step's output this turn, by op_id.
    outputs: HashMap<String, String>,
    /// The object picked this turn for a kept label (its id and label), so
    /// that every tree takes the same one.
    picks: HashMap<String, (String, String)>,
    /// The kept runs' Scene object ids bound this turn to this run's (keyed by
    /// the kept run and the id), so that every tree binds them alike and no two
    /// of one kept run become one.
    bound: HashMap<String, String>,
    /// Every kept run, for their clauses ([`Mode::Clauses`]; else none).
    kept: Vec<Episode>,
    /// The kept runs' steps for their clauses, which can serve clauses of this command.
    programs: Vec<Program>,
    /// Whether a reply was served and not dispatched yet.
    serving: bool,
    /// Whether the reply served last was a kept run's whole tree.
    served_whole: bool,
    /// Why memory stopped serving this run, once it has.
    stopped: Option<String>,
    /// Why memory stopped serving whole kept runs, where it may still serve clauses.
    whole_stopped: Option<String>,
    /// The clauses of this command memory refused this turn, logged once each.
    refused: HashSet<usize>,
    /// The programs served this turn, not served again.
    tried: HashSet<usize>,
    /// The clauses a program was served for this turn: another is not, for
    /// what the first left undone is the planner's.
    served_clauses: HashSet<usize>,
    /// The program being served part by part: its index, its next part, the
    /// clause of this command it serves, and its spans.
    in_clause: Option<(usize, usize, usize, Spans)>,
    /// The clause of this command the reply served last serves, until Scene's check passes it.
    serving_clause: Option<usize>,
    /// Where this run's robot was when the task began, if the state view knew.
    began_at: Option<Point>,
}

fn log(event: Value) {
    info!(
        "[pilot/segue] {}",
        json!({"event": "task_memory", "detail": event})
    );
}

impl TaskMemory {
    /// The memory a task's `context_json` asks for, if any, with the kept runs
    /// that can serve `command`, best first: the same command, then fewer
    /// differing spans, then the newest. `tasks` has the command's message.
    pub(crate) fn of(context_json: &str, command: &str, tasks: &TaskTable) -> Option<Self> {
        let context: Value = serde_json::from_str(context_json).unwrap_or_default();
        let mode = match context["task_memory"].as_str()? {
            "paths" => Mode::Paths,
            "programs" => Mode::Programs,
            "clauses" => Mode::Clauses,
            _ => return None,
        };
        let store = load(&path(mode)).unwrap_or_else(|why| {
            warn!("[pilot/segue] task memory not read: {why}");
            Store::default()
        });
        let kept = store.episodes.len();
        let all = if mode == Mode::Clauses {
            store.episodes.clone()
        } else {
            Vec::new()
        };
        let programs = programs(&all);
        let mut aligned: Vec<(usize, Aligned)> = store
            .episodes
            .into_iter()
            .enumerate()
            .filter_map(|(at, episode)| {
                let spans = align(&episode.command, command)?;
                handled(&episode, &spans).then_some((at, Aligned { episode, spans }))
            })
            .collect();
        aligned.sort_by_key(|(at, a)| (a.spans.len(), std::cmp::Reverse(*at)));
        let aligned: Vec<Aligned> = aligned.into_iter().map(|(_, a)| a).collect();
        log(json!({
            "what": "lookup", "mode": mode.label(), "kept": kept,
            "aligned": aligned.iter().map(|a| json!({"spans": a.spans, "segments": a.episode.segments.len()}))
                .collect::<Vec<_>>(),
            "clause_programs": programs.len(),
        }));
        Some(Self {
            mode,
            command: command.to_string(),
            task_id: tasks
                .messages
                .last()
                .map(|m| m.task.clone())
                .unwrap_or_default(),
            messages: tasks.messages.len(),
            tau: context["tau_choice"]
                .as_f64()
                .unwrap_or(crate::speculation::TAU_CHOICE),
            aligned,
            run: Vec::new(),
            outputs: HashMap::new(),
            picks: HashMap::new(),
            bound: HashMap::new(),
            kept: all,
            programs,
            serving: false,
            served_whole: false,
            stopped: None,
            whole_stopped: None,
            refused: HashSet::new(),
            tried: HashSet::new(),
            served_clauses: HashSet::new(),
            in_clause: None,
            serving_clause: None,
            began_at: None,
        })
    }

    /// Every step's output this turn, by op_id, for filling in references.
    pub(crate) fn outputs(&self) -> &HashMap<String, String> {
        &self.outputs
    }

    fn stop(&mut self, why: String) {
        if self.stopped.is_none() {
            log(json!({"what": "stop", "tree": self.run.len(), "why": why}));
            self.stopped = Some(why);
        }
    }

    /// Memory serves no more whole kept runs this turn; with [`Mode::Clauses`]
    /// it may still serve clauses.
    fn stop_whole(&mut self, why: String) {
        if self.mode != Mode::Clauses {
            return self.stop(why);
        }
        if self.whole_stopped.is_none() {
            log(json!({"what": "stop", "tree": self.run.len(), "why": why, "clauses_go_on": true}));
            self.whole_stopped = Some(why);
        }
    }

    /// Whether kept run `a`'s first trees ran the steps this run's did and,
    /// where this run's have ended, ended as they did and heard the same
    /// names, the command's spans put in (another person answering is another
    /// run).
    fn fits(&self, a: &Aligned) -> bool {
        let heard = |names: &[String], spans: &[(String, String)]| {
            let mut heard: Vec<String> = names
                .iter()
                .map(|name| replace_spans(name, spans).to_lowercase())
                .collect();
            heard.sort();
            heard
        };
        a.episode.segments.len() >= self.run.len()
            && a.episode.segments.iter().zip(&self.run).all(|(kept, now)| {
                kept.contracts == now.contracts
                    && (now.signature.is_empty()
                        || kept.signature == now.signature
                            && heard(&kept.heard, &a.spans) == heard(&now.heard, &[]))
            })
    }

    /// The kept runs that fit this one and have a tree after it; for
    /// [`Mode::Paths`] only the best, and only while no step failed.
    fn candidates(&self) -> Vec<usize> {
        let goes_on = |a: &Aligned| a.episode.segments.len() > self.run.len() && self.fits(a);
        match self.mode {
            Mode::Paths => {
                let clean = self
                    .run
                    .iter()
                    .all(|s| !s.signature.iter().any(|step| step == "failed"));
                let best = self.aligned.first().filter(|a| clean && goes_on(a));
                best.map(|_| vec![0]).unwrap_or_default()
            }
            Mode::Programs | Mode::Clauses => (0..self.aligned.len())
                .filter(|&at| goes_on(&self.aligned[at]))
                .collect(),
        }
    }

    /// Whether memory may serve the round after the tree just dispatched if it
    /// succeeds, so that no reply need be prepared for its success.
    pub(crate) fn expects_more(&self) -> bool {
        let whole = self.stopped.is_none() && self.whole_stopped.is_none();
        if !whole || !self.run.last().is_some_and(|s| s.reused) {
            return false;
        }
        let k = self.run.len();
        self.candidates().first().is_some_and(|&at| {
            let episode = &self.aligned[at].episode;
            // The kept run went on after a tree that ended well, as this one is asked to.
            let well = episode.segments[k - 1]
                .signature
                .iter()
                .all(|step| step == "ok" || step == "empty");
            let next = serde_json::from_str(&episode.segments[k].reply);
            well && next.is_ok_and(|next| stale(episode, k, &next).is_empty())
        })
    }

    /// The reply memory serves for the next tree, if it can, with a note for
    /// the planner where Scene's check cut it ([`premises_hold`]). `None`
    /// leaves the round to the planner; none while the task is closed. `tasks`
    /// are the session's tasks now.
    pub(crate) async fn reply(
        &mut self,
        state: &Value,
        tasks: &TaskTable,
        executor: &ExecutorConn,
        scene: Option<&SceneProviders>,
    ) -> Option<(String, Option<String>)> {
        // A served reply may have closed the task: the rest is the planner's to say.
        if !tasks.is_open(&self.task_id) {
            return None;
        }
        let reply = self.next(state, tasks.messages.len()).await?;
        match premises_hold(reply, executor, scene).await {
            Ok(checked) => {
                self.served_clauses.extend(self.serving_clause.take());
                Some(checked)
            }
            Err(why) => {
                self.serving_clause = None;
                self.in_clause = None;
                self.serving = false;
                if self.served_whole {
                    self.stop_whole(why);
                } else {
                    log(json!({"what": "refuse", "tree": self.run.len(), "why": why}));
                }
                None
            }
        }
    }

    /// The next tree memory serves, bound to this run, before Scene's check: a
    /// whole kept run's next tree, else ([`Mode::Clauses`]) the steps a kept run
    /// made for the clause this run is at.
    async fn next(&mut self, state: &Value, messages: usize) -> Option<Value> {
        if std::mem::take(&mut self.serving) {
            // Its replay failed or its round was cut short: served again, it would be again.
            self.stop("the reply served last was not dispatched".into());
        }
        if messages != self.messages {
            self.stop("a new message came".into());
        }
        if self.stopped.is_some() {
            return None;
        }
        if self.run.is_empty() || self.began_at.is_none() {
            // Where the state view does not know the robot's pose, a task begins
            // at the instruction point.
            let point = places_listed(&state["places"])
                .get("instruction point")
                .copied();
            self.began_at = self.began_at.or(robot_at(state)).or(point);
        }
        if self
            .run
            .last()
            .is_some_and(|last| last.signature.is_empty())
        {
            return None;
        }
        if self.whole_stopped.is_none() && !self.aligned.is_empty() {
            if let Some(reply) = self.next_whole(state).await {
                return Some(reply);
            }
        } else if self.whole_stopped.is_none() {
            self.stop_whole("no kept run of this command".into());
        }
        if self.mode == Mode::Clauses && self.stopped.is_none() {
            return self.next_clause(state).await;
        }
        None
    }

    /// The next tree of the best kept run that fits this one.
    async fn next_whole(&mut self, state: &Value) -> Option<Value> {
        let k = self.run.len();
        if let Some(last) = self.run.last()
            && !last.reused
        {
            self.stop_whole(format!("tree {} was the planner's", k - 1));
            return None;
        }
        let Some(&at) = self.candidates().first() else {
            let ended_here = self
                .aligned
                .iter()
                .any(|a| a.episode.segments.len() == k && self.fits(a));
            let why = match self.mode {
                _ if ended_here => "the kept run ended here",
                Mode::Paths => "the kept run does not go on this way",
                Mode::Programs | Mode::Clauses => "no kept run went on from here",
            };
            self.stop_whole(why.into());
            return None;
        };
        let aligned = self.aligned[at].clone();
        let kept = serde_json::from_str(&aligned.episode.segments[k].reply).unwrap_or_default();
        let source = Source {
            episode: &aligned.episode,
            segment: k,
            reply: kept,
            spans: aligned.spans.clone(),
            whole: true,
        };
        match self.instantiate(&source, state).await {
            Ok((reply, bound)) => {
                log(json!({
                    "what": "serve", "tree": k, "aligned": at, "spans": aligned.spans,
                    "bound": bound, "steps": aligned.episode.segments[k].op_ids.len(),
                }));
                self.serving = true;
                self.served_whole = true;
                Some(reply)
            }
            Err(why) => {
                self.stop_whole(format!("tree {k}: {why}"));
                None
            }
        }
    }

    /// The steps a kept run made for the first clause this run has not done
    /// (the return once every clause is), as a tree of their own: the next part
    /// of the program being served if its last part ended, and heard, as it did
    /// in the kept run; else the best program not served yet, from its first
    /// part this run has not already done.
    async fn next_clause(&mut self, state: &Value) -> Option<Value> {
        let clauses = clauses::split(&self.command);
        if clauses.is_empty() {
            return None;
        }
        if let Some((p, part, at, spans)) = self.in_clause.take() {
            let program = self.programs[p].clone();
            let (s, steps) = &program.parts[part - 1];
            let kept = &self.kept[program.episode].segments[*s];
            let last = self.run.last()?;
            let heard = |names: &[String], spans: &[(String, String)]| {
                let mut heard: Vec<String> = names
                    .iter()
                    .map(|name| replace_spans(name, spans).to_lowercase())
                    .collect();
                heard.sort();
                heard
            };
            let same = last.signature == part_signature(kept, steps)
                && heard(&kept.heard, &spans) == heard(&last.heard, &[]);
            if last.reused && same {
                if let Some(reply) = self.serve_part(&clauses, p, part, at, spans, state).await {
                    return Some(reply);
                }
            } else {
                log(json!({"what": "stop", "tree": self.run.len(), "program": p,
                    "why": "the part served last did not end as in the kept run"}));
            }
        }
        let mut places: HashMap<String, Point> =
            self.run.iter().flat_map(|s| s.places.clone()).collect();
        places.extend(places_listed(&state["places"]));
        let done = clauses::done(&clauses, &self.run, &places, self.began_at);
        let at = done.iter().position(|d| !d)?;
        if self.served_clauses.contains(&at) {
            return None;
        }
        // What this run already did for the clause, which a program need not do again.
        let owned = clauses::owners(&clauses, &self.run, &places, self.began_at);
        let did: Vec<&str> = owned
            .iter()
            .filter(|o| o.clause == at && o.step.ok)
            .map(|o| o.step.kind.as_str())
            .collect();
        let ending = clauses.get(at).map(Clause::ends_with);
        let mut tried = Vec::new();
        for (p, spans) in self.programs_for(&clauses, at) {
            if !self.tried.insert(p) {
                continue;
            }
            let program = &self.programs[p];
            let kept = &self.kept[program.episode];
            let first = program.parts.iter().position(|(s, steps)| {
                let kinds = clauses::acting_kinds(&kept.segments[*s], steps);
                kinds.iter().any(|k| Some(k.as_str()) == ending)
                    || !kinds.iter().all(|k| did.contains(&k.as_str()))
            });
            let Some(first) = first else {
                continue;
            };
            match self.serve_part(&clauses, p, first, at, spans, state).await {
                Some(reply) => return Some(reply),
                None => tried.push(p),
            }
        }
        if self.refused.insert(at) {
            log(json!({"what": "refuse", "tree": self.run.len(), "clause": at, "programs": tried}));
        }
        None
    }

    /// Part `part` of program `p` bound to this run as a tree, if it can serve.
    async fn serve_part(
        &mut self,
        clauses: &[Clause],
        p: usize,
        part: usize,
        at: usize,
        spans: Vec<(String, String)>,
        state: &Value,
    ) -> Option<Value> {
        let program = self.programs[p].clone();
        let episode = self.kept[program.episode].clone();
        let (segment, steps) = &program.parts[part];
        let kept: Value = serde_json::from_str(&episode.segments[*segment].reply).ok()?;
        let top = clauses::top_level(&kept["rtdl"]);
        let children: Vec<Value> = steps.iter().map(|&c| top[c].clone()).collect();
        let reply = json!({
            "content": "",
            "rtdl": {"op": "sequence", "children": children},
            "task_updates": [],
        });
        let source = Source {
            episode: &episode,
            segment: *segment,
            reply,
            spans: spans.clone(),
            whole: false,
        };
        match self.instantiate(&source, state).await {
            Ok((mut reply, bound)) => {
                // What it says is this command's clause, not the kept one's.
                let text = clauses.get(at).map_or("going back", |c| c.text.as_str());
                reply["content"] = json!(format!("Next: {text}."));
                reply["rtdl_description"] = json!(text);
                reply["rtdl"]["description"] = json!(text);
                log(json!({
                    "what": "serve", "tree": self.run.len(), "program": p, "part": part,
                    "clause": at, "text": text, "kept_run": program.episode,
                    "kept_clause": program.clause, "spans": spans, "bound": bound,
                    "steps": steps.len(),
                }));
                if part + 1 < program.parts.len() {
                    self.in_clause = Some((p, part + 1, at, spans));
                }
                self.serving_clause = Some(at);
                self.serving = true;
                self.served_whole = false;
                Some(reply)
            }
            Err(why) => {
                log(
                    json!({"what": "refuse", "tree": self.run.len(), "program": p, "part": part, "why": why}),
                );
                None
            }
        }
    }

    /// The kept programs that may serve clause `at` of this command (the
    /// return for `clauses.len()`), with the spans to put in, best first: fewer
    /// spans, then newer. The spans are those of every clause of the kept
    /// command that aligns, at the same distance, with one of this command
    /// around `at`: a step may name what an earlier clause did ("it").
    fn programs_for(&self, clauses: &[Clause], at: usize) -> Vec<(usize, Vec<(String, String)>)> {
        let mut out: Vec<(usize, usize, usize, Spans)> = Vec::new();
        for (p, program) in self.programs.iter().enumerate() {
            let kept = clauses::split(&self.kept[program.episode].command);
            let ret = program.clause == kept.len();
            if ret != (at == clauses.len()) {
                continue;
            }
            let episode = &self.kept[program.episode];
            let mut own = Vec::new();
            if !ret {
                match clauses::align_clause(&kept[program.clause], &clauses[at]) {
                    Some(spans) if handled(episode, &spans) => own = spans,
                    _ => continue,
                }
            }
            // Clauses before it, back while they align; but not against what the
            // clause served keeps or renames otherwise, nor two renamings of a word.
            let mut context: Spans = Vec::new();
            let (mut i, mut j) = (program.clause.min(kept.len()), at.min(clauses.len()));
            while i > 0 && j > 0 {
                (i, j) = (i - 1, j - 1);
                match clauses::align_clause(&kept[i], &clauses[j]) {
                    Some(more) => context.extend(more),
                    None => break,
                }
            }
            let served = kept.get(program.clause).map_or("", |c| c.text.as_str());
            let clashes: Vec<String> = context
                .iter()
                .filter(|(old, new)| context.iter().any(|(o, n)| o == old && n != new))
                .map(|(old, _)| old.clone())
                .collect();
            context.retain(|(old, new)| {
                let kept_here =
                    contains_word(served, old) && !own.contains(&(old.clone(), new.clone()));
                !clashes.contains(old) && !kept_here
            });
            let mut spans = own.clone();
            spans.extend(context);
            spans.sort();
            spans.dedup();
            out.push((own.len(), usize::MAX - program.episode, p, spans));
        }
        out.sort_by_key(|(n, newer, ..)| (*n, *newer));
        out.into_iter().map(|(_, _, p, s)| (p, s)).collect()
    }

    /// `source`'s tree for this run, with the bindings made, for the record; or
    /// why it cannot serve. Spans go into its text, with the rooms of renamed
    /// places ([`with_rooms`]); goals and place ids follow renamed places
    /// ([`goals`], [`place_ids`]); Scene objects are bound to this run's
    /// ([`TaskMemory::objects`]); a reference to a kept step names this run's
    /// step in its place; task ids become this task's.
    async fn instantiate(
        &mut self,
        source: &Source<'_>,
        state: &Value,
    ) -> Result<(Value, Value), String> {
        let (episode, kept) = (source.episode, &source.reply);
        let stale = stale(episode, source.segment, kept);
        if !stale.is_empty() {
            return Err(format!(
                "says what the kept run found out then: {}",
                stale.join(", ")
            ));
        }
        let (spans, rooms) = with_rooms(&source.spans, state, &episode.command, &self.command);
        // What the command names, without the scene its text first sets.
        let said = clauses::command_line(&self.command).to_string();
        let mut reply = kept.clone();
        put_in(&mut reply, &spans);
        if !source.whole {
            own_names(episode, &reply["rtdl"], &spans, &said)?;
        }
        let mut bound = serde_json::Map::new();
        let places = Places::of(episode, state, &spans, rooms);
        let start = (episode.segments[0].robot.or(self.began_at), self.began_at);
        let moves = goals(&places, &reply["rtdl"], &spans, &said, start, &mut bound)?;
        if places.changed
            && let Some(region) = regions(&reply["rtdl"])
                .into_iter()
                .find(|region| !places.named(&said, region))
        {
            return Err(format!(
                "searches the {region}, which this command does not name"
            ));
        }
        let mut ids = place_ids(&places, &reply["rtdl"], &spans, &said, &mut bound)?;
        let (objects, relabel) = self
            .objects(source, &spans, &places, state, &mut bound)
            .await?;
        ids.extend(objects);
        let ops: HashMap<String, String> = if source.whole {
            let kept_trees = episode.segments.iter().zip(&self.run);
            kept_trees
                .flat_map(|(kept, now)| kept.op_ids.iter().cloned().zip(now.op_ids.iter().cloned()))
                .collect()
        } else {
            HashMap::new()
        };
        let binds: HashSet<String> = crate::speculation::do_nodes(&reply["rtdl"])
            .iter()
            .filter_map(|node| node["bind"].as_str().map(String::from))
            .collect();
        rewrite(&mut reply["rtdl"], &ids, &ops, &moves, &binds)?;
        let picked = relabel.as_ref().map(|(_, now)| now.clone());
        if let Some((chosen, now)) = relabel {
            put_in(&mut reply, &[(chosen.clone(), now.clone())]);
            bound.insert(chosen, json!(now));
        }
        if !source.whole {
            // A kept clause's object is this command's only where the command
            // names it or the decision model picked it: the kept planner may have
            // chosen it for a kind this command does not ask for.
            let mut now = labels_listed(&state["objects"]);
            for output in self.outputs.values() {
                now.extend(labels_listed(
                    &serde_json::from_str(output).unwrap_or_default(),
                ));
            }
            let foreign = labels_named(&reply, &now).into_iter().find(|label| {
                label != "person" && !contains_word(&said, label) && picked.as_ref() != Some(label)
            });
            if let Some(label) = foreign {
                return Err(format!(
                    "acts on the {label}, which this command does not name"
                ));
            }
        }
        // Only the updates the kept planner made to the task its tree served.
        let kept_task = &kept["rtdl_parent_task_id"];
        if let Some(updates) = reply["task_updates"].as_array_mut() {
            updates.retain(|u| u["task_id"] == *kept_task || u["task_id"] == "new");
            updates
                .iter_mut()
                .for_each(|u| u["task_id"] = json!(self.task_id));
        }
        reply["rtdl_parent_task_id"] = json!(self.task_id);
        reply["message_updates"] = json!([]);
        Ok((reply, Value::Object(bound)))
    }

    /// Each Scene object id tree `kept` names, bound to this run's object: one
    /// bound so earlier this turn as before; the kept object itself where it is
    /// still listed as what the command names (not a person, where the command
    /// names another); else one of the label the spans give ([`bind_label`]),
    /// or, for a label the kept planner chose for a kind of object the command
    /// now names otherwise, the one the decision model picks, with that label
    /// and the picked object's to put in. No two kept ids become one.
    async fn objects(
        &mut self,
        source: &Source<'_>,
        spans: &[(String, String)],
        places: &Places<'_>,
        state: &Value,
        bound: &mut serde_json::Map<String, Value>,
    ) -> Result<(HashMap<String, String>, Option<(String, String)>), String> {
        let (episode, kept) = (source.episode, &source.reply);
        let labels = &episode.segments[source.segment].labels;
        let listed = labels_listed(&state["objects"]);
        let found_now: HashMap<String, String> = self
            .outputs
            .values()
            .flat_map(|o| labels_listed(&serde_json::from_str(o).unwrap_or_default()))
            .collect();
        // Kinds the command names otherwise that no label the kept run acted on holds.
        let acted: HashSet<String> = episode
            .segments
            .iter()
            .flat_map(|s| {
                labels_named(
                    &serde_json::from_str(&s.reply).unwrap_or_default(),
                    &s.labels,
                )
            })
            .collect();
        let kinds: Vec<&(String, String)> = spans
            .iter()
            .filter(|(old, _)| {
                !places.hold(old)
                    && !names_person(old)
                    && !acted.iter().any(|label| contains_word(label, old))
            })
            .collect();
        let mut chosen: Vec<String> = labels_named(kept, labels)
            .into_iter()
            .filter(|label| {
                !kinds.is_empty()
                    && chosen_for_kind(label, &episode.command)
                    && replace_spans(label, spans) == *label
                    && !places.hold(label)
            })
            .collect();
        chosen.sort();
        let pick = match (chosen.as_slice(), kinds.as_slice()) {
            ([], _) => None,
            ([label], [(_, new)]) => Some((label.clone(), self.pick(label, new, state).await?)),
            _ => {
                let new: Vec<&str> = kinds.iter().map(|(_, new)| new.as_str()).collect();
                return Err(format!(
                    "cannot tell which of {} the command's {} stands for",
                    chosen.join(", "),
                    new.join(", ")
                ));
            }
        };
        let person_changed = spans.iter().any(|(old, _)| names_person(old));
        // Ids are bound per kept run: two kept runs may know one object by two ids.
        let run_key = format!("{}|{}|", episode.when, episode.command);
        let mut ids = HashMap::new();
        for id in ids_in(&kept["rtdl"]) {
            if ids.contains_key(&id) {
                continue;
            }
            let key = format!("{run_key}{id}");
            let now = match self.bound.get(&key) {
                Some(now) => now.clone(),
                None => {
                    let label = norm_label(
                        labels
                            .get(&id)
                            .ok_or_else(|| format!("{id} has no kept label"))?,
                    );
                    let renamed = replace_spans(&label, spans);
                    let still = listed.get(&id).or(found_now.get(&id));
                    let now = match &pick {
                        Some((chosen, (picked, _))) if *chosen == label => picked.clone(),
                        _ if renamed == label
                            && !(label == "person" && person_changed)
                            && still.is_some_and(|l| norm_label(l) == label) =>
                        {
                            id.clone()
                        }
                        _ => bind_label(&renamed, &found_now, &state["objects"])?,
                    };
                    let mine = self.bound.iter().filter(|(k, _)| k.starts_with(&run_key));
                    if let Some((other, _)) = mine.into_iter().find(|(_, n)| **n == now) {
                        let other = other.trim_start_matches(&run_key);
                        return Err(format!("{id} and {other} would both be {now}"));
                    }
                    self.bound.insert(key, now.clone());
                    now
                }
            };
            bound.insert(id.clone(), json!(now));
            ids.insert(id, now);
        }
        Ok((ids, pick.map(|(chosen, (_, now))| (chosen, now))))
    }

    /// The object of kind `new` the decision model picks for the kept label
    /// `label`, with its label now; asked once a turn.
    async fn pick(
        &mut self,
        label: &str,
        new: &str,
        state: &Value,
    ) -> Result<(String, String), String> {
        if let Some(picked) = self.picks.get(label) {
            return Ok(picked.clone());
        }
        let wanted = [new.to_string()];
        let objects = &state["objects"];
        let picks = crate::speculation::choose_objects(
            "task-memory",
            &self.command,
            self.tau,
            &wanted,
            objects,
        )
        .await?;
        let id = picks
            .get(new)
            .cloned()
            .ok_or_else(|| format!("no {new} was picked"))?;
        let now = labels_listed(objects)
            .get(&id)
            .map(|l| norm_label(l))
            .ok_or_else(|| format!("{id} is not listed"))?;
        self.picks
            .insert(label.to_string(), (id.clone(), now.clone()));
        Ok((id, now))
    }

    /// A tree went out from `reply`, under the state view `state`.
    pub(crate) fn dispatched(&mut self, plan: &Plan, reply: &str, state: &Value) {
        let mut labels = labels_listed(&state["objects"]);
        for output in self.outputs.values() {
            labels.extend(labels_listed(
                &serde_json::from_str(output).unwrap_or_default(),
            ));
        }
        let dos: Vec<(u32, &crate::pb::pilot::RtdlNode)> = plan
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.node_kind == RTDL_DO)
            .map(|(index, node)| (index as u32, node))
            .collect();
        let reused = std::mem::take(&mut self.serving);
        self.run.push(Segment {
            reply: reply.to_string(),
            op_ids: dos.iter().map(|(_, n)| n.op_id.clone()).collect(),
            contracts: dos
                .iter()
                .map(|(_, n)| {
                    n.call
                        .as_ref()
                        .map(|c| c.contract_id.clone())
                        .unwrap_or_default()
                })
                .collect(),
            signature: Vec::new(),
            heard: Vec::new(),
            found: Vec::new(),
            labels,
            places: places_listed(&state["places"]),
            robot: robot_at(state),
            reused,
            plan: Some((plan.plan_id.clone(), dos.iter().map(|(i, _)| *i).collect())),
        });
    }

    /// The tree `plan_id` ended with `results`.
    pub(crate) fn ended(&mut self, plan_id: &str, results: &[RtdlNodeState]) {
        let Some(at) = self.run.iter().position(|s| {
            s.signature.is_empty() && s.plan.as_ref().is_some_and(|(p, _)| p == plan_id)
        }) else {
            return;
        };
        let segment = &mut self.run[at];
        let indices = segment
            .plan
            .as_ref()
            .map(|(_, indices)| indices.clone())
            .unwrap_or_default();
        segment.signature = signature(&indices, &segment.contracts, results);
        for (index, op_id) in indices.iter().zip(&segment.op_ids) {
            let result = results.iter().find(|r| r.node_index == *index);
            if let Some(result) = result
                && result.state == RtdlNodeStateEnum::Succeeded as u32
                && let Some(leaf) = &result.leaf_result
            {
                segment.heard.extend(values_of(&leaf.output, "name"));
                segment.found.extend(values_of(&leaf.output, "label"));
                self.outputs.insert(op_id.clone(), leaf.output.clone());
            }
        }
        for values in [&mut segment.heard, &mut segment.found] {
            values.sort();
            values.dedup();
        }
        log(json!({
            "what": "ended", "tree": at, "plan_id": plan_id, "reused": segment.reused,
            "signature": segment.signature,
        }));
    }

    /// The turn ended: keep this run if every tree ended and the one task they
    /// were all filed under is done.
    pub(crate) fn finish(self, tasks: &TaskTable) {
        let owner_of = |s: &Segment| {
            let (plan_id, _) = s.plan.as_ref()?;
            tasks.task_of_tree(plan_id)
        };
        let owner = self.run.first().and_then(owner_of);
        // A message mid-turn may have changed what the task is.
        let steered = tasks.messages.len() != self.messages;
        let done = owner
            .is_some_and(|id| tasks.tasks.iter().any(|t| t.id == id && t.status == DONE))
            && self.run.iter().all(|s| owner_of(s) == owner)
            && !steered;
        let complete = self.run.iter().all(|s| !s.signature.is_empty());
        let (trees, reused) = (self.run.len(), self.run.iter().filter(|s| s.reused).count());
        let kept = if done && complete {
            let when = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0.0, |since| since.as_secs_f64());
            let episode = Episode {
                command: self.command,
                segments: self.run,
                when,
            };
            keep(&path(self.mode), episode).unwrap_or_else(|why| {
                warn!("[pilot/segue] task memory not saved: {why}");
                false
            })
        } else {
            false
        };
        log(json!({
            "what": "finish", "done": done, "complete": complete, "kept": kept, "trees": trees,
            "reused": reused, "stopped": self.stopped,
        }));
    }
}

/// Each kept run's steps for each clause of its command (and its return): the
/// top-level steps that served the clause, tree by tree, each part naming no
/// step outside it, every step of it ended well (a find may have found
/// nothing). A clause that shares a top-level step with another has none: its
/// steps there would be served without the others they came with.
fn programs(episodes: &[Episode]) -> Vec<Program> {
    let mut out = Vec::new();
    for (e, episode) in episodes.iter().enumerate() {
        let clauses = clauses::split(&episode.command);
        let places: HashMap<String, Point> = episode
            .segments
            .iter()
            .flat_map(|s| s.places.clone())
            .collect();
        let start = episode.segments[0]
            .robot
            .or_else(|| places.get("instruction point").copied());
        let owned = clauses::owners(&clauses, &episode.segments, &places, start);
        for clause in 0..=clauses.len() {
            let mut at: Vec<(usize, usize)> = owned
                .iter()
                .filter(|o| o.clause == clause)
                .map(|o| (o.segment, o.child))
                .collect();
            at.dedup();
            let shared = at.iter().any(|&(s, c)| {
                owned
                    .iter()
                    .any(|o| o.segment == s && o.child == c && o.clause != clause)
            });
            if shared {
                continue;
            }
            let mut parts: Vec<(usize, Vec<usize>)> = Vec::new();
            for (s, c) in at {
                match parts.last_mut() {
                    Some((last, steps)) if *last == s => steps.push(c),
                    _ => parts.push((s, vec![c])),
                }
            }
            if !parts.is_empty()
                && parts
                    .iter()
                    .all(|(s, steps)| serves_alone(&episode.segments[*s], steps))
            {
                out.push(Program {
                    episode: e,
                    clause,
                    parts,
                });
            }
        }
    }
    out
}

/// How top-level steps `steps` of tree `segment` ended, step by step.
fn part_signature(segment: &Segment, steps: &[usize]) -> Vec<String> {
    let Ok(reply) = serde_json::from_str::<Value>(&segment.reply) else {
        return Vec::new();
    };
    let mut at = 0;
    let mut out = Vec::new();
    for (c, child) in clauses::top_level(&reply["rtdl"]).into_iter().enumerate() {
        let n = crate::speculation::do_nodes(child).len();
        if steps.contains(&c) {
            out.extend(segment.signature.iter().skip(at).take(n).cloned());
        }
        at += n;
    }
    out
}

/// Whether top-level steps `steps` of kept tree `segment` ended well and name
/// no step outside them.
fn serves_alone(segment: &Segment, steps: &[usize]) -> bool {
    let Ok(reply) = serde_json::from_str::<Value>(&segment.reply) else {
        return false;
    };
    let top = clauses::top_level(&reply["rtdl"]);
    let mut at = 0;
    let mut binds = HashSet::new();
    let mut names = Vec::new();
    for (c, child) in top.into_iter().enumerate() {
        let dos = crate::speculation::do_nodes(child);
        if steps.contains(&c) {
            let ended = segment.signature[at.min(segment.signature.len())..]
                .iter()
                .take(dos.len());
            if ended.clone().count() < dos.len()
                || ended.into_iter().any(|e| e != "ok" && e != "empty")
            {
                return false;
            }
            binds.extend(
                dos.iter()
                    .filter_map(|n| n["bind"].as_str().map(String::from)),
            );
            names.extend(nodes(child).into_iter().filter_map(|v| {
                let fields = v.as_object()?;
                crate::expression::is_reference(fields)
                    .then(|| fields.get("var")?.as_str().map(String::from))?
            }));
        }
        at += dos.len();
    }
    names.iter().all(|name| binds.contains(name))
}

/// `spans` with, for each place they rename, the room it stands in (`rooms_of`
/// in the state view) renamed to the room the new place stands in: a kept
/// planner searched the room a table stands in. Not where the command names
/// the old room, nor where another place the kept or this command names, and
/// spans keep, stands in it, nor where two renamed places' rooms disagree.
/// Also the rooms put in this way, which a served tree may name though the
/// command does not.
fn with_rooms(
    spans: &[(String, String)],
    state: &Value,
    kept: &str,
    command: &str,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut out = spans.to_vec();
    let Some(room_of) = state["rooms_of"].as_object() else {
        return (out, Vec::new());
    };
    let (kept, command) = (clauses::command_line(kept), clauses::command_line(command));
    let named = |place: &str| contains_word(kept, place) || contains_word(command, place);
    let mut moved: Vec<(String, String)> = Vec::new();
    for (place, room) in room_of {
        let renamed = replace_spans(place, spans).to_lowercase();
        let (Some(room), Some(new)) =
            (room.as_str(), room_of.get(&renamed).and_then(Value::as_str))
        else {
            continue;
        };
        if renamed != *place && new != room {
            moved.push((room.to_string(), new.to_string()));
        }
    }
    let mut rooms = Vec::new();
    for (room, new) in &moved {
        let clash = moved.iter().any(|(r, n)| r == room && n != new);
        let stays = room_of.iter().any(|(place, r)| {
            r.as_str() == Some(room) && replace_spans(place, spans) == *place && named(place)
        });
        let taken = out.iter().any(|(old, _)| old.eq_ignore_ascii_case(room));
        if !clash && !stays && !taken && !contains_word(command, room) {
            out.push((room.clone(), new.clone()));
            rooms.push(new.clone());
        }
    }
    (out, rooms)
}

/// Why a tree of kept steps for one clause cannot serve: it names an entity
/// of the kept command that the spans do not rename and this command does not
/// name (what "it" was, a room the kept command went to).
fn own_names(
    episode: &Episode,
    tree: &Value,
    spans: &[(String, String)],
    command: &str,
) -> Result<(), String> {
    let mut text = Vec::new();
    world_text(tree, &mut text);
    let text = text.join(" ");
    for name in clauses::split(&episode.command)
        .iter()
        .flat_map(Clause::names)
    {
        let kept = replace_spans(name, spans) == *name;
        if kept && contains_word(&text, name) && !contains_word(command, name) {
            return Err(format!("names the {name} of the kept command"));
        }
    }
    Ok(())
}

/// The string values of `value` its steps act on: [`put_in`]'s, without the
/// planner's descriptions of them.
fn world_text<'a>(value: &'a Value, out: &mut Vec<&'a str>) {
    match value {
        Value::String(text) if !text.starts_with("scene.") => out.push(text),
        Value::Object(fields) if !crate::expression::is_reference(fields) => {
            for (key, value) in fields {
                if !STRUCTURE.contains(&key.as_str()) && key != "description" {
                    world_text(value, out);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|v| world_text(v, out)),
        _ => {}
    }
}

fn path(mode: Mode) -> PathBuf {
    crate::transcript::sessions_root().join(format!("task_memory-{}.json", mode.label()))
}

/// The store at `path`; empty if there is none.
fn load(path: &Path) -> Result<Store, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Store::default()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Add `episode` to the store at `path`, at most [`KEEP`] runs per command
/// and [`MAX_RUNS`] in all, the oldest dropped; whether it was added. A
/// read-only store (a memory frozen for comparison) is left as it is, and so
/// is one that cannot be read, rather than replaced by this run alone.
fn keep(path: &Path, episode: Episode) -> Result<bool, String> {
    let _saving = SAVING.lock().unwrap_or_else(PoisonError::into_inner);
    if std::fs::metadata(path).is_ok_and(|m| m.permissions().readonly()) {
        return Ok(false);
    }
    let mut store = load(path)?;
    let same = |e: &Episode| e.command == episode.command;
    while store.episodes.iter().filter(|e| same(e)).count() >= KEEP {
        if let Some(oldest) = store.episodes.iter().position(same) {
            store.episodes.remove(oldest);
        }
    }
    store.episodes.push(episode);
    let over = store.episodes.len().saturating_sub(MAX_RUNS);
    store.episodes.drain(..over);
    let text = serde_json::to_string(&store).map_err(|e| e.to_string())?;
    let partial = path.with_extension("json.partial");
    std::fs::write(&partial, text)
        .and_then(|()| std::fs::rename(&partial, path))
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(true)
}

/// How each `do` step of a tree ended.
fn signature(indices: &[u32], contracts: &[String], results: &[RtdlNodeState]) -> Vec<String> {
    indices
        .iter()
        .zip(contracts)
        .map(|(index, contract)| {
            let Some(result) = results.iter().find(|r| r.node_index == *index) else {
                return "skipped".into();
            };
            match RtdlNodeStateEnum::try_from(result.state as i32) {
                Ok(RtdlNodeStateEnum::Succeeded) => {
                    let output = result.leaf_result.as_ref().map(|l| l.output.as_str());
                    let found: Value = output
                        .and_then(|o| serde_json::from_str(o).ok())
                        .unwrap_or_default();
                    let empty = found["objects"].as_array().is_some_and(Vec::is_empty);
                    if contract.ends_with("/find") && empty {
                        "empty".into()
                    } else {
                        "ok".into()
                    }
                }
                Ok(RtdlNodeStateEnum::Failed | RtdlNodeStateEnum::Timeout) => "failed".into(),
                Ok(RtdlNodeStateEnum::Canceled) => "canceled".into(),
                _ => "skipped".into(),
            }
        })
        .collect()
}

/// Every value in `value`, itself included.
fn nodes(value: &Value) -> Vec<&Value> {
    let mut out = Vec::new();
    let mut walk = vec![value];
    while let Some(v) = walk.pop() {
        out.push(v);
        match v {
            Value::Object(fields) => walk.extend(fields.values()),
            Value::Array(items) => walk.extend(items),
            _ => {}
        }
    }
    out
}

/// The values of the fields `key` in a step's output: names heard, labels found.
fn values_of(output: &str, key: &str) -> Vec<String> {
    let output: Value = serde_json::from_str(output).unwrap_or_default();
    nodes(&output)
        .into_iter()
        .filter_map(|v| v.get(key)?.as_str())
        .map(|value| value.trim().to_string())
        .filter(|value| value.len() >= 3)
        .collect()
}

/// What tree `k` of `episode`, `reply`, says that the kept run found out
/// before it ([`stale_said`]).
fn stale(episode: &Episode, k: usize, reply: &Value) -> Vec<String> {
    let earlier = episode.segments[..k]
        .iter()
        .flat_map(|s| s.heard.iter().chain(&s.found));
    stale_said(
        reply,
        &episode.segments[k].labels,
        earlier,
        &episode.command,
    )
}

/// Scene object ids with their labels, from a state view's objects by region
/// or a find's output.
fn labels_listed(value: &Value) -> HashMap<String, String> {
    nodes(value)
        .into_iter()
        .filter_map(Value::as_object)
        .filter_map(|fields| {
            let id = fields.get("id")?.as_str()?;
            let label = fields.get("label")?.as_str()?;
            id.starts_with("scene.object.")
                .then(|| (id.to_string(), label.to_string()))
        })
        .collect()
}

/// The state view's places and regions that have a goal, by lower-case name.
fn places_listed(places: &Value) -> HashMap<String, Point> {
    let listed = places.as_array().into_iter().flatten();
    listed
        .filter_map(|place| {
            let name = place["name"].as_str()?.to_lowercase();
            Some((name, position(&place["goal"])?))
        })
        .collect()
}

/// The places a kept tree may refer to.
struct Places<'a> {
    /// Goals by lower-case name: the kept run's state views' (a first round's
    /// may show none yet), which hold for every tree, and this round's.
    goals: HashMap<String, Point>,
    /// This round's state view places, each with its id, name and goal.
    listed: &'a Value,
    /// Whether the command changes a place, after which the kept planner may
    /// have worked out a goal or a region from the old one (the room a table
    /// stands in) that renaming cannot change: only what the command names may
    /// stay.
    changed: bool,
    /// Rooms the spans put in for renamed places' rooms ([`with_rooms`]), which
    /// may stay though the command does not name them.
    rooms: Vec<String>,
    /// The goals the kept run's trees drove to, for a place this round has no goal for yet.
    kept_goals: Vec<Value>,
    /// Each point of interest's room (`rooms_of` in the state view).
    room_of: HashMap<String, String>,
}

impl<'a> Places<'a> {
    fn of(
        episode: &Episode,
        state: &'a Value,
        spans: &[(String, String)],
        rooms: Vec<String>,
    ) -> Self {
        let mut goals: HashMap<String, Point> = episode
            .segments
            .iter()
            .flat_map(|s| s.places.clone())
            .collect();
        goals.extend(places_listed(&state["places"]));
        let kept_goals = episode
            .segments
            .iter()
            .filter_map(|s| serde_json::from_str::<Value>(&s.reply).ok())
            .flat_map(|reply| literal_goals(&reply["rtdl"]))
            .collect();
        let room_of = state["rooms_of"].as_object().into_iter().flatten();
        let room_of = room_of
            .filter_map(|(place, room)| Some((place.clone(), room.as_str()?.to_string())))
            .collect();
        let mut places = Self {
            goals,
            listed: &state["places"],
            changed: false,
            rooms,
            kept_goals,
            room_of,
        };
        places.changed = spans.iter().any(|(old, _)| places.hold(old));
        places
    }

    /// Whether a served tree may name the place `name`: the command names it,
    /// or it is the room of a place the command names or of a renamed one.
    fn named(&self, command: &str, name: &str) -> bool {
        let in_named = self
            .room_of
            .iter()
            .any(|(place, room)| room.eq_ignore_ascii_case(name) && contains_word(command, place));
        contains_word(command, name)
            || in_named
            || self.rooms.iter().any(|r| r.eq_ignore_ascii_case(name))
    }

    /// The goal for the place `name`: this round's, else one a kept tree drove
    /// to at that place (on the same map), which a first round may need.
    fn goal_for(&self, name: &str) -> Option<&Value> {
        let listed = self
            .listed_with("name", Some(name))
            .map(|place| &place["goal"]);
        listed
            .into_iter()
            .find(|goal| position(goal).is_some())
            .or_else(|| {
                let at = *self.goals.get(&name.to_lowercase())?;
                let mut kept = self.kept_goals.iter();
                kept.find(|goal| position(goal).is_some_and(|g| distance(g, at) <= SAME_GOAL_M))
            })
    }

    /// Whether `word` is a place's name or a word of one.
    fn hold(&self, word: &str) -> bool {
        let listed = self
            .listed_with("name", None)
            .filter_map(|p| p["name"].as_str());
        let mut names = self.goals.keys().map(String::as_str).chain(listed);
        names.any(|name| contains_word(name, word))
    }

    /// This round's places whose field `key` is `value` (any, without one).
    fn listed_with<'b>(
        &self,
        key: &'b str,
        value: Option<&'b str>,
    ) -> impl Iterator<Item = &'a Value> + 'b
    where
        'a: 'b,
    {
        let listed = self.listed.as_array().into_iter().flatten();
        listed.filter(move |place| match value {
            Some(value) => place[key]
                .as_str()
                .is_some_and(|v| v.eq_ignore_ascii_case(value)),
            None => true,
        })
    }
}

/// Where tree `tree`'s navigation goals go now: one at a renamed place to the
/// new place's goal, one where the kept task began (`start`: then and now) to
/// where this one did; why not, for a goal at no place (worked out from where
/// a person or an object then was), or, where a place changed, at one the
/// command does not name.
fn goals(
    places: &Places,
    tree: &Value,
    spans: &[(String, String)],
    command: &str,
    start: (Option<Point>, Option<Point>),
    bound: &mut serde_json::Map<String, Value>,
) -> Result<Vec<(Point, Value)>, String> {
    let mut moves = Vec::new();
    for goal in literal_goals(tree) {
        let Some(at) = position(&goal) else {
            continue;
        };
        let near: Vec<&String> = places
            .goals
            .iter()
            .filter(|(_, p)| distance(**p, at) <= SAME_GOAL_M)
            .map(|(name, _)| name)
            .collect();
        let renamed = near.iter().find_map(|name| {
            let new = replace_spans(name, spans);
            (new != **name).then(|| ((*name).clone(), new))
        });
        if let Some((name, new)) = renamed {
            let to = places
                .goal_for(&new)
                .ok_or_else(|| format!("no goal for {new} in the state view"))?;
            bound.insert(name, json!(new));
            moves.push((at, to.clone()));
        } else if start.0.is_some_and(|kept| distance(kept, at) <= HOME_M) {
            if let (Some(kept), Some(now)) = start
                && distance(kept, now) > SAME_GOAL_M
            {
                let mut to = goal.clone();
                to["pose"]["position"]["x"] = json!(now.0);
                to["pose"]["position"]["y"] = json!(now.1);
                moves.push((at, to));
            }
        } else if near.is_empty() {
            return Err(format!(
                "goes to ({:.2}, {:.2}), which is no place on the map",
                at.0, at.1
            ));
        } else if places.changed && !near.iter().any(|name| places.named(command, name)) {
            return Err(format!(
                "goes to the {}, which this command does not name",
                near[0]
            ));
        }
    }
    Ok(moves)
}

/// The place and region ids (`scene.poi.*`, `scene.region.*`) tree `tree`
/// names, bound to this round's: a renamed place's to the new place's; why
/// not, where a place changed, for one this round does not list or the
/// command does not name.
fn place_ids(
    places: &Places,
    tree: &Value,
    spans: &[(String, String)],
    command: &str,
    bound: &mut serde_json::Map<String, Value>,
) -> Result<HashMap<String, String>, String> {
    let mut ids = HashMap::new();
    let named = nodes(tree).into_iter().filter_map(Value::as_str);
    for id in named.filter(|s| s.starts_with("scene.poi.") || s.starts_with("scene.region.")) {
        let name = places
            .listed_with("id", Some(id))
            .find_map(|place| place["name"].as_str());
        let Some(name) = name else {
            if places.changed {
                return Err(format!("names {id}, which the state view does not list"));
            }
            continue;
        };
        let new = replace_spans(name, spans);
        if new != name {
            let now = places
                .listed_with("name", Some(&new))
                .find_map(|place| place["id"].as_str())
                .ok_or_else(|| format!("no {new} in the state view"))?;
            bound.insert(id.to_string(), json!(now));
            ids.insert(id.to_string(), now.to_string());
        } else if places.changed && !places.named(command, name) {
            return Err(format!(
                "acts at the {name}, which this command does not name"
            ));
        }
    }
    Ok(ids)
}

fn norm(word: &str) -> String {
    word.trim_matches(|c: char| !c.is_alphanumeric())
        .to_lowercase()
}

/// A label as compared: lower case, with spaces for underscores.
fn norm_label(label: &str) -> String {
    label.to_lowercase().replace('_', " ")
}

/// Phrasings of one action that commands use interchangeably, each with the
/// word it is compared as.
const SAME_ACTION: &[(&[&str], &str)] = &[
    (&["look", "for"], "find"),
    (&["locate"], "find"),
    (&["say", "hello", "to"], "greet"),
    (&["introduce", "yourself", "to"], "greet"),
    (&["salute"], "greet"),
    (&["get"], "take"),
    (&["grasp"], "take"),
    (&["fetch"], "take"),
    (&["bring"], "give"),
    (&["deliver"], "give"),
    (&["navigate"], "go"),
    (&["put"], "place"),
    (&["tell"], "say"),
    (&["an"], "a"),
];

/// `text`'s words as compared: lower case, without punctuation, with
/// [`SAME_ACTION`]'s phrasings made one; each with the range of `text`'s
/// words it stands for.
fn compared(text: &str) -> (Vec<&str>, Vec<(String, std::ops::Range<usize>)>) {
    let words: Vec<&str> = text.split_whitespace().collect();
    let normed: Vec<String> = words.iter().map(|w| norm(w)).collect();
    let mut out = Vec::new();
    let mut at = 0;
    while at < words.len() {
        let phrase = SAME_ACTION.iter().find(|(phrase, _)| {
            normed.len() >= at + phrase.len()
                && phrase.iter().zip(&normed[at..]).all(|(p, w)| p == w)
        });
        match phrase {
            Some((phrase, word)) => {
                out.push((word.to_string(), at..at + phrase.len()));
                at += phrase.len();
            }
            None => {
                out.push((normed[at].clone(), at..at + 1));
                at += 1;
            }
        }
    }
    (words, out)
}

/// The spans that differ between a kept command and a new one, if they differ
/// only in at most [`MAX_SPANS`] spans of at most [`MAX_SPAN_WORDS`] words on
/// either side, with every other word the same up to [`SAME_ACTION`]'s
/// phrasings: `[(kept span, new span)]`. The same command differs in none.
fn align(kept: &str, new: &str) -> Option<Vec<(String, String)>> {
    let (kept_words, ka) = compared(kept);
    let (new_words, kb) = compared(new);
    let na: Vec<&String> = ka.iter().map(|(w, _)| w).collect();
    let nb: Vec<&String> = kb.iter().map(|(w, _)| w).collect();
    let words =
        |side: &[(String, std::ops::Range<usize>)], all: &[&str], from: usize, to: usize| {
            let range = side[from].1.start..side[to - 1].1.end;
            all[range].join(" ")
        };
    // Longest common subsequence of the normalized words.
    let mut lcs = vec![vec![0usize; nb.len() + 1]; na.len() + 1];
    for i in (0..na.len()).rev() {
        for j in (0..nb.len()).rev() {
            lcs[i][j] = if na[i] == nb[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut spans = Vec::new();
    while i < na.len() || j < nb.len() {
        if i < na.len() && j < nb.len() && na[i] == nb[j] {
            i += 1;
            j += 1;
            continue;
        }
        let (i0, j0) = (i, j);
        while (i < na.len() || j < nb.len()) && !(i < na.len() && j < nb.len() && na[i] == nb[j]) {
            if j >= nb.len() || (i < na.len() && lcs[i + 1][j] >= lcs[i][j + 1]) {
                i += 1;
            } else {
                j += 1;
            }
        }
        if i == i0 || j == j0 || i - i0 > MAX_SPAN_WORDS || j - j0 > MAX_SPAN_WORDS {
            return None;
        }
        if na[i0..i]
            .iter()
            .chain(&nb[j0..j])
            .any(|w| NOT_ENTITIES.contains(&w.as_str()))
        {
            return None;
        }
        let clean = |words: String| {
            words
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_string()
        };
        spans.push((
            clean(words(&ka, &kept_words, i0, i)),
            clean(words(&kb, &new_words, j0, j)),
        ));
    }
    (spans.len() <= MAX_SPANS).then_some(spans)
}

/// The byte ranges where `word` occurs in `text` as whole words, ignoring
/// ASCII case (which keeps byte offsets where full lowercasing would not).
fn occurrences(text: &str, word: &str) -> Vec<(usize, usize)> {
    let lower = text.to_ascii_lowercase();
    let word = word.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    // Only ASCII letters and digits make words: a curly quote ends one.
    let boundary = |at: usize| at >= bytes.len() || !bytes[at].is_ascii_alphanumeric();
    let mut out = Vec::new();
    let mut from = 0;
    while !word.is_empty()
        && let Some(found) = lower[from..].find(&word)
    {
        let (start, end) = (from + found, from + found + word.len());
        if (start == 0 || boundary(start - 1)) && boundary(end) {
            out.push((start, end));
        }
        from = end;
    }
    out
}

/// Whether `text` holds `word` as whole words, or with underscores for spaces.
fn contains_word(text: &str, word: &str) -> bool {
    !occurrences(text, word).is_empty() || !occurrences(text, &word.replace(' ', "_")).is_empty()
}

/// `text` with every whole-word occurrence of each span's first side, or of it
/// with underscores for spaces, replaced by its second side, all at once (so
/// two spans may trade places) and keeping a leading capital.
fn replace_spans(text: &str, spans: &[(String, String)]) -> String {
    let mut hits: Vec<(usize, usize, String)> = Vec::new();
    for (old, new) in spans {
        for (form, replacement) in [
            (old.clone(), new.clone()),
            (old.replace(' ', "_"), new.replace(' ', "_")),
        ] {
            let found = occurrences(text, &form).into_iter();
            hits.extend(found.map(|(start, end)| (start, end, replacement.clone())));
        }
    }
    hits.sort_by_key(|(start, end, _)| (*start, std::cmp::Reverse(*end)));
    let mut out = String::new();
    let mut at = 0;
    for (start, end, replacement) in hits {
        if start < at {
            continue;
        }
        out.push_str(&text[at..start]);
        let mut chars = replacement.chars();
        match chars.next() {
            Some(first) if text[start..].starts_with(char::is_uppercase) => {
                out.extend(first.to_uppercase());
                out.push_str(chars.as_str());
            }
            _ => out.push_str(&replacement),
        }
        at = end;
    }
    out.push_str(&text[at..]);
    out
}

/// `spans` put into every string value of `value` that says something about
/// the world: not into keys, Scene ids, references, numbers or the names of
/// the tree's own parts ([`STRUCTURE`]).
fn put_in(value: &mut Value, spans: &[(String, String)]) {
    match value {
        Value::String(text) if !text.starts_with("scene.") => {
            *text = replace_spans(text, spans);
        }
        Value::Object(fields) if !crate::expression::is_reference(fields) => {
            for (key, value) in fields.iter_mut() {
                if !STRUCTURE.contains(&key.as_str()) {
                    put_in(value, spans);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| put_in(v, spans)),
        _ => {}
    }
}

/// Whether a span names a person: commands capitalise only names, and their
/// first word, an action, cannot be in a span.
fn names_person(span: &str) -> bool {
    span.starts_with(char::is_uppercase)
}

/// Whether a label is one the kept planner chose for a kind of object the
/// command named, rather than one the command named itself.
fn chosen_for_kind(label: &str, command: &str) -> bool {
    label != "person" && !contains_word(command, label)
}

/// Whether each span names what reuse can put in: a place the kept run's
/// state views showed, a person, or an object the kept run acted on (one whose
/// label holds the span, or one its planner chose for a kind the command
/// named, which the decision model picks anew). Another word (a team's name
/// for its country) would change what the replies mean.
fn handled(episode: &Episode, spans: &[(String, String)]) -> bool {
    let places: HashSet<&String> = episode
        .segments
        .iter()
        .flat_map(|s| s.places.keys())
        .collect();
    let objects: HashSet<String> = episode
        .segments
        .iter()
        .flat_map(|s| {
            let reply = serde_json::from_str(&s.reply).unwrap_or_default();
            labels_named(&reply, &s.labels)
        })
        .collect();
    spans.iter().all(|(old, _)| {
        names_person(old)
            || places.iter().any(|place| contains_word(place, old))
            || objects
                .iter()
                .any(|label| contains_word(label, old) || chosen_for_kind(label, &episode.command))
    })
}

/// The labels of the objects a reply's tree acts on, as compared: those of
/// the Scene ids it names (`labels`) and its `label` and `object` arguments.
fn labels_named(reply: &Value, labels: &HashMap<String, String>) -> HashSet<String> {
    let mut named = HashSet::new();
    for v in nodes(&reply["rtdl"]) {
        match v {
            Value::String(id) => named.extend(labels.get(id).map(|l| norm_label(l))),
            Value::Object(fields) => named.extend(
                ["label", "object"]
                    .into_iter()
                    .filter_map(|key| fields.get(key)?.as_str())
                    .filter(|label| !label.starts_with("scene.object."))
                    .map(norm_label),
            ),
            _ => {}
        }
    }
    named
}

/// What a reply says: its `content` and its steps' [`SAID`] text.
fn said_text(reply: &Value) -> String {
    let steps = nodes(&reply["rtdl"])
        .into_iter()
        .filter_map(Value::as_object)
        .flat_map(|fields| {
            SAID.iter()
                .filter_map(move |key| fields.get(*key)?.as_str())
        });
    let said: Vec<&str> = reply["content"].as_str().into_iter().chain(steps).collect();
    said.join(" ")
}

/// What `reply` says that its run found out rather than was told: a number
/// the command does not hold (a time read, a count), or a name heard or label
/// found by an earlier step (`observed`) that the reply does not act on.
/// Another run would say it stale.
fn stale_said<'a>(
    reply: &Value,
    labels: &HashMap<String, String>,
    observed: impl Iterator<Item = &'a String>,
    command: &str,
) -> Vec<String> {
    let said = said_text(reply);
    let acted = labels_named(reply, labels);
    let numbers = said
        .split(|c: char| !(c.is_alphanumeric() || c == ':'))
        .map(|token| token.trim_matches(':'))
        .filter(|token| {
            token.contains(|c: char| c.is_ascii_digit())
                || NUMBERS.contains(&token.to_ascii_lowercase().as_str())
        })
        .map(String::from);
    // "person" is what every meeting finds, not a finding.
    let found = observed
        .map(|value| norm_label(value))
        .filter(|value| value != "person" && !acted.contains(value) && contains_word(&said, value));
    let mut stale: Vec<String> = numbers
        .chain(found)
        .filter(|value| !contains_word(command, value))
        .collect();
    stale.sort();
    stale.dedup();
    stale
}

/// The navigation goals written in a tree.
fn literal_goals(tree: &Value) -> Vec<Value> {
    let goals = nodes(tree).into_iter().filter_map(|v| v.get("goal"));
    goals
        .filter(|goal| position(goal).is_some())
        .cloned()
        .collect()
}

/// The regions a tree's steps search or act in (`region` arguments).
fn regions(tree: &Value) -> Vec<String> {
    let named = nodes(tree)
        .into_iter()
        .filter_map(|v| v.get("region")?.as_str());
    named
        .filter(|region| !region.is_empty())
        .map(String::from)
        .collect()
}

/// The Scene object ids a tree names.
fn ids_in(tree: &Value) -> Vec<String> {
    let strings = nodes(tree).into_iter().filter_map(Value::as_str);
    strings
        .filter(|s| s.starts_with("scene.object."))
        .map(String::from)
        .collect()
}

/// The robot's position on the map in a state view, if known.
fn robot_at(state: &Value) -> Option<Point> {
    let robot = &state["scene"]["state"];
    (robot["pose_known"] == true).then_some((robot["x"].as_f64()?, robot["y"].as_f64()?))
}

/// This run's object labelled `label` (as compared): one its steps returned
/// (any of several objects, which were found where the tree looked, but not
/// of several people: the kept run met one, which cannot be told), else one
/// the state view lists in `objects` ([`one_room`]).
fn bind_label(
    label: &str,
    found_now: &HashMap<String, String>,
    objects: &Value,
) -> Result<String, String> {
    let of = |pool: &HashMap<String, String>| {
        let mut ids: Vec<String> = pool
            .iter()
            .filter(|(_, l)| norm_label(l) == label)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort();
        ids
    };
    match of(found_now).as_slice() {
        [id] => return Ok(id.clone()),
        [id, ..] if label != "person" => return Ok(id.clone()),
        [] => {}
        found => {
            let n = found.len();
            return Err(format!("{n} people could be the one the kept run met"));
        }
    }
    match of(&labels_listed(objects)).as_slice() {
        [id] => Ok(id.clone()),
        [] => Err(format!("no {label} is listed now")),
        listed if label != "person" => one_room(objects, listed).ok_or_else(|| {
            format!(
                "{} objects labelled {label} are listed in different rooms",
                listed.len()
            )
        }),
        listed => Err(format!(
            "{} people could be the one the kept run met",
            listed.len()
        )),
    }
}

/// Of the objects `ids` the state view lists by room in `objects`, the one
/// last seen, if all of those in a named room are in one (Scene keeps one
/// object twice at times); none if they are in several, which the kept run's
/// was cannot be told.
fn one_room(objects: &Value, ids: &[String]) -> Option<String> {
    let rooms = objects.as_object()?;
    let mut listed: Vec<(&String, &Value)> = rooms
        .iter()
        .filter(|(room, _)| *room != "elsewhere")
        .flat_map(|(room, listed)| {
            listed
                .as_array()
                .into_iter()
                .flatten()
                .map(move |o| (room, o))
        })
        .filter(|(_, o)| {
            o["id"]
                .as_str()
                .is_some_and(|id| ids.iter().any(|i| i == id))
        })
        .collect();
    let room = listed.first()?.0;
    if listed.iter().any(|(r, _)| *r != room) {
        return None;
    }
    listed.sort_by_key(|(_, o)| {
        let seen = o["seen_s_ago"].as_u64().unwrap_or(u64::MAX);
        (seen, o["id"].as_str().map(String::from))
    });
    listed.first()?.1["id"].as_str().map(String::from)
}

/// Bind ids, rename references to kept steps and move goals in a kept tree.
fn rewrite(
    value: &mut Value,
    ids: &HashMap<String, String>,
    ops: &HashMap<String, String>,
    moves: &[(Point, Value)],
    binds: &HashSet<String>,
) -> Result<(), String> {
    match value {
        Value::String(s) => {
            if let Some(id) = ids.get(s.as_str()) {
                *s = id.clone();
            }
        }
        Value::Object(fields) if crate::expression::is_reference(fields) => {
            if let Some(name) = fields.get("var").and_then(Value::as_str)
                && !binds.contains(name)
            {
                match ops.get(name) {
                    Some(now) => {
                        fields.insert("var".into(), json!(now));
                    }
                    None if crate::rtdl_stream::is_op_id(name) => {
                        return Err(format!("names step {name}, which this run did not run"));
                    }
                    None => {}
                }
            }
        }
        Value::Object(fields) => {
            if let Some(at) = fields.get("goal").and_then(position)
                && let Some((_, now)) = moves.iter().find(|(kept, _)| distance(*kept, at) < 1e-9)
            {
                fields.insert("goal".into(), now.clone());
            }
            for v in fields.values_mut() {
                rewrite(v, ids, ops, moves, binds)?;
            }
        }
        Value::Array(items) => {
            for v in items {
                rewrite(v, ids, ops, moves, binds)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// A fact one top-level step of a reply relies on.
#[derive(Debug, PartialEq)]
enum Premise {
    /// A navigation goal written in the reply.
    Goal(Value),
    /// A Scene object the reply names.
    Object(String),
}

/// What each top-level step of `reply`'s tree relies on, by the step's index
/// (a tree whose root is not a sequence is one step).
fn premises(reply: &Value) -> Vec<(usize, Premise)> {
    let tree = &reply["rtdl"];
    let steps: Vec<&Value> = match (tree["op"].as_str(), tree["children"].as_array()) {
        (Some("sequence"), Some(children)) => children.iter().collect(),
        _ => vec![tree],
    };
    let mut out = Vec::new();
    for (at, step) in steps.into_iter().enumerate() {
        for v in nodes(step) {
            match v {
                Value::String(id) if id.starts_with("scene.object.") => {
                    out.push((at, Premise::Object(id.clone())));
                }
                _ => out.extend(
                    v.get("goal")
                        .filter(|goal| position(goal).is_some())
                        .map(|goal| (at, Premise::Goal(goal.clone()))),
                ),
            }
        }
    }
    out
}

/// `reply` with its tree cut before top-level step `at`; `None` if nothing
/// would be left.
fn cut(mut reply: Value, at: usize) -> Option<String> {
    if at == 0 {
        return None;
    }
    reply["rtdl"]["children"].as_array_mut()?.truncate(at);
    Some(reply.to_string())
}

/// `reply` cut before its first top-level step whose navigation goal Scene no
/// longer finds clear or whose object Scene no longer lists, with a note for
/// the planner saying so; why not, if nothing would be left. Unchecked where
/// Scene does not answer.
async fn premises_hold(
    reply: Value,
    executor: &ExecutorConn,
    scene: Option<&SceneProviders>,
) -> Result<(String, Option<String>), String> {
    let Some(scene) = scene else {
        return Ok((reply.to_string(), None));
    };
    let listed = state_context::listed(executor, &scene.find).await;
    for (at, premise) in premises(&reply) {
        let why = match premise {
            Premise::Goal(goal) => match &scene.goal_clear {
                Some(provider) => state_context::goal_clear(executor, provider, &goal)
                    .await
                    .flatten()
                    .map(|reason| format!("its goal is not clear ({reason})")),
                None => None,
            },
            Premise::Object(id) => listed
                .as_ref()
                .filter(|listed| !listed.contains_key(&id))
                .map(|_| format!("{id} is no longer listed")),
        };
        if let Some(why) = why {
            let why = format!("kept step {at}: {why}");
            log(json!({"what": "cut", "at": at, "why": why}));
            let kept = cut(reply, at).ok_or_else(|| why.clone())?;
            let note = format!(
                "Pilot reused a kept plan but ran only its first {at} top-level step(s): {why}; plan the rest."
            );
            return Ok((kept, Some(note)));
        }
    }
    Ok((reply.to_string(), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::pilot::{CapabilityCall, CapabilityCallResult, RtdlNode};

    fn spans(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    /// A memory serving `command` from one kept run of `kept` with `segments`.
    fn memory(kept: &str, command: &str, segments: Vec<Segment>) -> TaskMemory {
        let episode = Episode {
            command: kept.into(),
            segments,
            when: 0.0,
        };
        TaskMemory {
            mode: Mode::Programs,
            command: command.into(),
            task_id: "T2".into(),
            messages: 1,
            tau: 0.7,
            aligned: vec![Aligned {
                spans: align(kept, command).expect("the commands align"),
                episode,
            }],
            run: Vec::new(),
            outputs: HashMap::new(),
            picks: HashMap::new(),
            bound: HashMap::new(),
            kept: Vec::new(),
            programs: Vec::new(),
            serving: false,
            served_whole: false,
            stopped: None,
            whole_stopped: None,
            refused: HashSet::new(),
            tried: HashSet::new(),
            served_clauses: HashSet::new(),
            in_clause: None,
            serving_clause: None,
            began_at: None,
        }
    }

    /// The next tree of the memory's one kept run for this run, bound as
    /// `next_whole` binds it.
    async fn bind(memory: &mut TaskMemory, state: &Value) -> Result<(Value, Value), String> {
        let aligned = memory.aligned[0].clone();
        let k = memory.run.len();
        let source = Source {
            episode: &aligned.episode,
            segment: k,
            reply: serde_json::from_str(&aligned.episode.segments[k].reply).unwrap(),
            spans: aligned.spans.clone(),
            whole: true,
        };
        memory.instantiate(&source, state).await
    }

    fn goal(x: f64, y: f64) -> Value {
        json!({"pose": {"position": {"x": x, "y": y}}})
    }

    fn at(reply: &Value, step: usize) -> &Value {
        &reply["rtdl"]["children"][step]["args"]
    }

    #[test]
    fn commands_align_only_where_spans_differ() {
        let kept = "Take a peach from the dinner table and give it to Charlie in the kitchen";
        assert_eq!(align(kept, kept), Some(vec![]));
        let found = align(
            kept,
            "Take a bowl from the shelf and give it to Paris in the kitchen",
        );
        assert_eq!(
            found.unwrap(),
            spans(&[
                ("peach", "bowl"),
                ("dinner table", "shelf"),
                ("Charlie", "Paris")
            ])
        );
        // Another verb or relation is another task.
        let thrown = "Throw a peach from the dinner table and give it to Charlie in the kitchen";
        assert!(align(kept, thrown).is_none());
        assert!(align("Put it on the sofa", "Put it in the sofa").is_none());
        assert!(align(kept, "Take a peach from the dinner table").is_none());
        let longer = "Bring me a coke from the kitchen please now";
        assert!(align("Bring me a coke", longer).is_none());
    }

    #[test]
    fn phrasings_of_one_action_align() {
        assert_eq!(
            align(
                "Get a red bull from the cabinet and give it to me",
                "Take a milk from the cabinet and give it to me"
            ),
            Some(spans(&[("red bull", "milk")]))
        );
        assert_eq!(
            align(
                "Find a mangosteen in the kitchen then grasp it and put it on the tv stand",
                "Find a mangosteen in the kitchen then grasp it and place it on the shelf"
            ),
            Some(spans(&[("tv stand", "shelf")]))
        );
        assert_eq!(
            align(
                "Locate a dish in the kitchen then get it and bring it to me",
                "Look for a cornflakes in the living room then take it and give it to me"
            ),
            Some(spans(&[("dish", "cornflakes"), ("kitchen", "living room")]))
        );
    }

    #[test]
    fn spans_go_into_what_strings_say_at_once() {
        // Two places trading roles are put in at once.
        let swap = spans(&[("dinner table", "shelf"), ("shelf", "dinner table")]);
        assert_eq!(
            replace_spans("Take it from the dinner table to the shelf", &swap),
            "Take it from the shelf to the dinner table"
        );
        assert_eq!(
            replace_spans("tablet table", &spans(&[("table", "shelf")])),
            "tablet shelf"
        );
        // Lowercasing that changes byte lengths does not shift what is replaced.
        assert_eq!(
            replace_spans("İİİİ, Robin", &spans(&[("robin", "Paris")])), // i18n-ok: multibyte fixture
            "İİİİ, Paris" // i18n-ok: multibyte fixture
        );
        let mut reply = json!({
            "content": "Red bull here",
            "rtdl": {"op": "do", "cap": "object", "bind": "object",
                "args": {"object_id": "scene.object.0048", "label": "red_bull", "x": 2.0,
                    "text": {"var": "ab12-2", "path": ["object"]}}},
        });
        put_in(
            &mut reply,
            &spans(&[("object", "coke"), ("2", "3"), ("red bull", "orange juice")]),
        );
        assert_eq!(
            reply,
            json!({
                "content": "Orange juice here",
                "rtdl": {"op": "do", "cap": "object", "bind": "object",
                    "args": {"object_id": "scene.object.0048", "label": "orange_juice", "x": 2.0,
                        "text": {"var": "ab12-2", "path": ["object"]}}},
            })
        );
    }

    #[tokio::test]
    async fn renamed_places_and_labels_bind_without_asking() {
        let reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": goal(1.0, 1.0)}},
            {"op": "do", "cap": "pick", "args": {"object": "red_bell_pepper", "id": "scene.object.0001"}},
            {"op": "do", "cap": "navigate", "args": {"goal": goal(5.0, 5.0)}},
        ]}});
        let segment = Segment {
            reply: reply.to_string(),
            labels: HashMap::from([("scene.object.0001".into(), "red_bell_pepper".into())]),
            places: HashMap::from([
                ("dinner table".into(), (1.0, 1.0)),
                ("bedside table".into(), (5.0, 5.0)),
            ]),
            ..Default::default()
        };
        let state = json!({
            "objects": {"kitchen": [{"id": "scene.object.0002", "label": "yellow_bell_pepper"}]},
            "places": [
                {"name": "dinner table", "goal": goal(1.0, 1.0)},
                {"name": "bedside table", "goal": goal(5.0, 5.0)},
            ],
        });
        let mut memory = memory(
            "Grasp a red bell pepper from the dinner table and place it on the bedside table",
            "Grasp a yellow bell pepper from the bedside table and place it on the dinner table",
            vec![segment],
        );
        let (reply, _) = bind(&mut memory, &state).await.unwrap();
        assert_eq!(at(&reply, 0)["goal"]["pose"]["position"]["x"], 5.0);
        assert_eq!(at(&reply, 2)["goal"]["pose"]["position"]["x"], 1.0);
        assert_eq!(at(&reply, 1)["object"], "yellow_bell_pepper");
        assert_eq!(at(&reply, 1)["id"], "scene.object.0002");
    }

    /// The kept run's second tree, after a first that ran step ab12-3.
    fn delivery() -> Vec<Segment> {
        let reply = json!({
            "content": "I'll bring the sponge.",
            "message_updates": [{"message_id": "m1", "parent_task_id": "new"}],
            "rtdl_parent_task_id": "T1",
            "task_updates": [{"task_id": "T1", "status": "done"}],
            "rtdl": {"op": "sequence", "children": [
                {"op": "do", "cap": "goal_near", "args": {"object_id": "scene.object.0048"}, "bind": "approach"},
                {"op": "do", "cap": "navigate", "args": {"goal": {"var": "approach", "path": ["goal"]}}},
                {"op": "do", "cap": "pick", "args": {"object": "sponge"}},
                {"op": "do", "cap": "navigate", "args": {"goal": goal(0.7, -6.08)}},
                {"op": "do", "cap": "speak", "args": {"text": {"var": "ab12-3", "path": ["text"]}}},
            ]},
        });
        let first = Segment {
            op_ids: vec!["ab12-3".into()],
            contracts: vec!["x/find".into()],
            signature: vec!["ok".into()],
            ..Default::default()
        };
        let second = Segment {
            reply: reply.to_string(),
            labels: HashMap::from([("scene.object.0048".into(), "sponge".into())]),
            places: HashMap::from([("kitchen".into(), (0.7, -6.08))]),
            ..Default::default()
        };
        vec![first, second]
    }

    /// A memory whose run has run the first tree of [`delivery`] as step cd34-9.
    fn delivering(command: &str) -> TaskMemory {
        let mut memory = memory(
            "Find a sponge in the bedroom and bring it to the kitchen",
            command,
            delivery(),
        );
        memory.run.push(Segment {
            op_ids: vec!["cd34-9".into()],
            contracts: vec!["x/find".into()],
            signature: vec!["ok".into()],
            reused: true,
            ..Default::default()
        });
        memory.outputs.insert(
            "cd34-9".into(),
            json!({"objects": [{"id": "scene.object.0099", "label": "sponge"}]}).to_string(),
        );
        memory
    }

    #[tokio::test]
    async fn a_kept_reply_binds_to_this_run() {
        let state = json!({
            "objects": {"living room": [{"id": "scene.object.0102", "label": "sponge"}]},
            "places": [{"name": "Living room", "goal": goal(-3.0, -2.0)}],
        });
        let mut memory = delivering("Find a sponge in the bedroom and bring it to the living room");
        let (reply, bound) = bind(&mut memory, &state).await.unwrap();
        // The sponge this run found, not the kept one or one merely listed.
        assert_eq!(at(&reply, 0)["object_id"], "scene.object.0099");
        assert_eq!(bound["scene.object.0048"], "scene.object.0099");
        assert_eq!(at(&reply, 1)["goal"]["var"], "approach");
        assert_eq!(at(&reply, 3)["goal"]["pose"]["position"]["x"], -3.0);
        assert_eq!(at(&reply, 4)["text"]["var"], "cd34-9");
        // This task's ids, and no messages refiled.
        assert_eq!(reply["rtdl_parent_task_id"], "T2");
        assert_eq!(reply["task_updates"][0]["task_id"], "T2");
        assert_eq!(reply["message_updates"], json!([]));
        // A reference to a step this run did not run refuses the reply.
        memory.run[0].op_ids = vec![];
        let refused = bind(&mut memory, &state).await;
        assert!(refused.unwrap_err().contains("did not run"));
        // A place the state view has no goal for refuses it too.
        let mut memory = delivering("Find a sponge in the bedroom and bring it to the garage");
        assert!(bind(&mut memory, &state).await.is_err());
    }

    /// A kept tree that drives to the kitchen's goal and searches the kitchen,
    /// with the kitchen trash bin among the places the state view then showed.
    fn kitchen_search() -> Segment {
        let reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": goal(1.0, 1.0)}},
            {"op": "do", "cap": "find", "args": {"label": "cup", "region": "kitchen"}},
        ]}});
        Segment {
            reply: reply.to_string(),
            places: HashMap::from([
                ("kitchen".into(), (1.0, 1.0)),
                ("kitchen trash bin".into(), (2.0, 2.0)),
                ("dinner table".into(), (1.5, 1.5)),
            ]),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn only_places_the_reply_goes_to_are_renamed() {
        let state = json!({"objects": {}, "places": [
            {"name": "living room", "goal": goal(-3.0, -2.0)},
        ]});
        let mut memory = memory(
            "Locate a cup in the kitchen then take it and give it to me",
            "Locate a cup in the living room then take it and give it to me",
            vec![kitchen_search()],
        );
        let (reply, _) = bind(&mut memory, &state).await.unwrap();
        assert_eq!(at(&reply, 0)["goal"]["pose"]["position"]["x"], -3.0);
        assert_eq!(at(&reply, 1)["region"], "living room");
    }

    #[tokio::test]
    async fn what_the_kept_planner_worked_out_from_an_old_place_refuses_the_reply() {
        // The kept command named the dinner table; its tree went to the kitchen,
        // where the table stands, which renaming the table cannot change.
        let state = json!({"objects": {}, "places": [
            {"name": "bedside table", "goal": goal(5.0, 5.0)},
        ]});
        let kept = "Grasp a cup from the dinner table and place it on the bedside table";
        let swapped = "Grasp a cup from the bedside table and place it on the dinner table";
        let mut memory = memory(kept, swapped, vec![kitchen_search()]);
        let err = bind(&mut memory, &state).await.unwrap_err();
        assert!(err.contains("does not name"), "{err}");
        // Another object only: the kitchen the kept planner worked out still holds.
        let bowl = "Grasp a bowl from the dinner table and place it on the bedside table";
        let mut memory = super::tests::memory(kept, bowl, vec![kitchen_search()]);
        assert!(bind(&mut memory, &state).await.is_ok());
    }

    #[tokio::test]
    async fn goals_go_to_places_or_where_the_task_began() {
        let reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": goal(3.0, 3.0)}},
            {"op": "do", "cap": "navigate", "args": {"goal": goal(0.1, 0.0)}},
        ]}});
        let segment = Segment {
            reply: reply.to_string(),
            robot: Some((0.0, 0.0)),
            ..Default::default()
        };
        let command = "Meet Robin in the bedroom";
        let state = json!({"objects": {}, "places": [{"name": "bedroom", "goal": goal(3.0, 3.0)}],
            "scene": {"state": {"pose_known": true, "x": 2.0, "y": -1.0}}});
        let mut memory = memory(command, command, vec![segment.clone()]);
        memory.began_at = robot_at(&state);
        let (reply, _) = bind(&mut memory, &state).await.unwrap();
        // The way back goes to where this run began.
        assert_eq!(at(&reply, 1)["goal"]["pose"]["position"]["x"], 2.0);
        assert_eq!(at(&reply, 0)["goal"]["pose"]["position"]["x"], 3.0);
        // A goal at no place was worked out from where someone then stood.
        let state = json!({"objects": {}, "places": []});
        let mut memory = super::tests::memory(command, command, vec![segment]);
        let err = bind(&mut memory, &state).await.unwrap_err();
        assert!(err.contains("no place"), "{err}");
    }

    #[tokio::test]
    async fn objects_the_command_does_not_rename_keep_their_label() {
        let reply = json!({"content": "I'll put the coke on the dinner table.",
        "rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "pick", "args": {"object": "coke", "object_id": "scene.object.1"}},
            {"op": "do", "cap": "place", "args": {"target_id": "scene.object.2"}},
        ]}});
        let segment = Segment {
            reply: reply.to_string(),
            labels: HashMap::from([
                ("scene.object.1".into(), "coke".into()),
                ("scene.object.2".into(), "dinner_table".into()),
            ]),
            ..Default::default()
        };
        let state = json!({"objects": {"kitchen": [
            {"id": "scene.object.7", "label": "apple"},
            {"id": "scene.object.8", "label": "dinner_table"},
        ]}, "places": []});
        let mut memory = memory(
            "Put a drink on the dinner table",
            "Put a fruit on the dinner table",
            vec![segment.clone()],
        );
        // The apple the decision model picked earlier this turn.
        memory
            .picks
            .insert("coke".into(), ("scene.object.7".into(), "apple".into()));
        let (reply, _) = bind(&mut memory, &state).await.unwrap();
        assert_eq!(at(&reply, 0)["object_id"], "scene.object.7");
        assert_eq!(at(&reply, 0)["object"], "apple");
        assert_eq!(at(&reply, 1)["target_id"], "scene.object.8");
        assert_eq!(reply["content"], "I'll put the apple on the dinner table.");
        // Two objects the kept planner chose: which the fruit stands for cannot be told.
        let mut two = segment;
        two.labels.insert("scene.object.2".into(), "glass".into());
        let mut memory = super::tests::memory(
            "Put a drink on the dinner table",
            "Put a fruit on the dinner table",
            vec![two],
        );
        let err = bind(&mut memory, &state).await.unwrap_err();
        assert!(err.contains("cannot tell"), "{err}");
    }

    #[test]
    fn only_spans_naming_places_people_or_objects_are_reused() {
        let segment = Segment {
            reply: json!({"rtdl": {"op": "do", "cap": "pick", "args": {"object": "red_bull"}}})
                .to_string(),
            places: HashMap::from([("cabinet".into(), (1.0, 1.0))]),
            ..Default::default()
        };
        let episode = Episode {
            command: "Get a red bull from the cabinet and give it to Robin".into(),
            segments: vec![segment],
            when: 0.0,
        };
        assert!(handled(
            &episode,
            &spans(&[
                ("red bull", "milk"),
                ("cabinet", "shelf"),
                ("Robin", "Paris")
            ])
        ));
        let speech = Episode {
            command: "Tell me your team's name".into(),
            segments: vec![Segment {
                reply: json!({"rtdl": {"op": "do", "cap": "speak", "args": {"text": "We are T"}}})
                    .to_string(),
                ..Default::default()
            }],
            when: 0.0,
        };
        assert!(!handled(&speech, &spans(&[("name", "country")])));
    }

    #[test]
    fn what_the_kept_run_found_out_is_not_said_again() {
        let command = "Meet Robin in the bedroom and say the time";
        let none = HashMap::new();
        let stale = |reply: Value, observed: &[&str]| {
            let observed: Vec<String> = observed.iter().map(|o| o.to_string()).collect();
            stale_said(&reply, &none, observed.iter(), command)
        };
        let time = json!({"content": "Telling Robin the time.", "rtdl": {"op": "do", "cap": "speak",
            "args": {"text": "Robin, it is 03:24 CST."}}});
        assert_eq!(stale(time, &[]), ["03:24"]);
        let count = json!({"content": "There are three people in the bedroom.", "rtdl": {}});
        assert_eq!(stale(count, &["person"]), ["three"]);
        // A name heard counts; one the command asked about does not.
        let heard = json!({"content": "I found Simone, not Robin.", "rtdl": {}});
        assert_eq!(stale(heard.clone(), &["Simone", "Robin"]), ["simone"]);
        // A label found that the reply does not act on is an answer from then.
        let answer = json!({"content": "The biggest object is the cereal box.", "rtdl": {}});
        assert_eq!(stale(answer, &["cereal_box"]), ["cereal box"]);
        let acted = json!({"content": "I'll pick the cereal box.",
            "rtdl": {"op": "do", "cap": "pick", "args": {"object": "cereal_box"}}});
        assert!(stale(acted, &["cereal_box"]).is_empty());
        // A reply that reads the time again says nothing stale.
        let fresh = json!({"content": "", "rtdl": {"op": "do", "cap": "speak",
            "args": {"text": {"var": "clock", "path": ["stdout"]}}}});
        assert!(stale(fresh, &["03:24"]).is_empty());
        let meeting = json!({"content": "I'll approach the person found.", "rtdl": {}});
        assert!(stale(meeting, &["person"]).is_empty());
    }

    #[tokio::test]
    async fn an_object_no_longer_listed_refuses_the_reply() {
        let state = json!({"objects": {"kitchen": []}, "places": []});
        let command = "Find a sponge in the bedroom and bring it to the kitchen";
        let mut memory = delivering(command);
        memory.outputs.clear();
        let err = bind(&mut memory, &state).await.unwrap_err();
        assert!(err.contains("no sponge"), "{err}");
    }

    #[test]
    fn steps_end_as_ok_empty_failed_or_skipped() {
        let result = |index: u32, state: RtdlNodeStateEnum, output: &str| RtdlNodeState {
            node_index: index,
            state: state as u32,
            leaf_result: Some(CapabilityCallResult {
                output: output.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let contracts: Vec<String> = ["x/navigate", "x/find", "x/pick", "x/place"]
            .map(String::from)
            .into();
        let results = vec![
            result(1, RtdlNodeStateEnum::Succeeded, "{}"),
            result(2, RtdlNodeStateEnum::Succeeded, r#"{"objects": []}"#),
            result(3, RtdlNodeStateEnum::Failed, ""),
        ];
        assert_eq!(
            signature(&[1, 2, 3, 4], &contracts, &results),
            ["ok", "empty", "failed", "skipped"]
        );
    }

    #[test]
    fn premises_by_top_level_step_and_cuts() {
        let reply: Value = serde_json::from_str(&delivery()[1].reply).unwrap();
        let found = premises(&reply);
        assert!(found.contains(&(0, Premise::Object("scene.object.0048".into()))));
        let goal_at_3 = found
            .iter()
            .any(|(at, p)| *at == 3 && matches!(p, Premise::Goal(_)));
        assert!(goal_at_3);
        let kept: Value = serde_json::from_str(&cut(reply.clone(), 3).unwrap()).unwrap();
        assert_eq!(kept["rtdl"]["children"].as_array().unwrap().len(), 3);
        assert!(cut(reply, 0).is_none());
    }

    /// A one-step plan of `contract`.
    fn plan(plan_id: &str, op_id: &str, contract: &str) -> Plan {
        Plan {
            plan_id: plan_id.into(),
            nodes: vec![RtdlNode {
                node_kind: RTDL_DO,
                op_id: op_id.into(),
                call: Some(CapabilityCall {
                    contract_id: contract.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn succeeded(output: &str) -> Vec<RtdlNodeState> {
        vec![RtdlNodeState {
            node_index: 0,
            state: RtdlNodeStateEnum::Succeeded as u32,
            leaf_result: Some(CapabilityCallResult {
                output: output.into(),
                ..Default::default()
            }),
            ..Default::default()
        }]
    }

    #[tokio::test]
    async fn a_turn_is_served_tree_by_tree_until_it_departs_from_the_kept_run() {
        let tree = |cap: &str| {
            json!({"content": "", "rtdl": {"op": "do", "cap": cap, "args": {}}}).to_string()
        };
        let kept = |cap: &str| Segment {
            reply: tree(cap),
            contracts: vec![format!("x/{cap}")],
            op_ids: vec!["ab12-1".into()],
            signature: vec!["ok".into()],
            heard: vec!["Simone".into()],
            ..Default::default()
        };
        let command = "Bring me a coke";
        let state = json!({"objects": {}, "places": []});
        let fresh = || memory(command, command, vec![kept("find"), kept("speak")]);

        // A reply served and never dispatched is not served again.
        let mut memory = fresh();
        assert!(memory.next(&state, 1).await.is_some());
        assert!(memory.next(&state, 1).await.is_none());
        assert!(
            memory
                .stopped
                .as_deref()
                .unwrap()
                .contains("not dispatched")
        );

        // Dispatched and ended as kept, the next tree is served.
        let mut memory = fresh();
        let first = memory.next(&state, 1).await.unwrap();
        memory.dispatched(&plan("p1", "cd34-1", "x/find"), &first.to_string(), &state);
        assert!(memory.expects_more());
        memory.ended("p1", &succeeded(r#"{"name": "Simone"}"#));
        assert_eq!(memory.run[0].heard, ["Simone"]);
        assert!(memory.next(&state, 1).await.is_some());

        // Another person answering is another run.
        let mut memory = fresh();
        let first = memory.next(&state, 1).await.unwrap();
        memory.dispatched(&plan("p1", "cd34-1", "x/find"), &first.to_string(), &state);
        memory.ended("p1", &succeeded(r#"{"name": "Paris"}"#));
        assert!(memory.next(&state, 1).await.is_none());

        // A new message stops it.
        let mut memory = fresh();
        let first = memory.next(&state, 1).await.unwrap();
        memory.dispatched(&plan("p1", "cd34-1", "x/find"), &first.to_string(), &state);
        memory.ended("p1", &succeeded(r#"{"name": "Simone"}"#));
        assert!(memory.next(&state, 2).await.is_none());

        // So does a tree that ran other steps than the kept one.
        let mut memory = fresh();
        let first = memory.next(&state, 1).await.unwrap();
        memory.dispatched(
            &plan("p1", "cd34-1", "x/navigate"),
            &first.to_string(),
            &state,
        );
        assert!(!memory.expects_more());
        memory.ended("p1", &succeeded(r#"{"name": "Simone"}"#));
        assert!(memory.next(&state, 1).await.is_none());
    }

    #[tokio::test]
    async fn place_ids_follow_the_command_and_objects_stay_who_they_were() {
        let reply = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "goal_region", "args": {"region_id": "scene.poi.k"}},
            {"op": "do", "cap": "goal_near", "args": {"object_id": "scene.object.5"}},
            {"op": "do", "cap": "goal_near", "args": {"object_id": "scene.object.6"}},
        ]}});
        let segment = Segment {
            reply: reply.to_string(),
            labels: HashMap::from([
                ("scene.object.5".into(), "person".into()),
                ("scene.object.6".into(), "person".into()),
            ]),
            ..Default::default()
        };
        let state = json!({"places": [
            {"id": "scene.poi.k", "name": "kitchen"},
            {"id": "scene.poi.b", "name": "bedroom"},
        ],
        "objects": {"bedroom": [
            {"id": "scene.object.5", "label": "person"},
            {"id": "scene.object.6", "label": "person"},
        ]}});
        // The same command: the people the kept run met, where Scene still lists them.
        let command = "Meet Paris in the kitchen";
        let mut memory = memory(command, command, vec![segment.clone()]);
        let (served, _) = bind(&mut memory, &state).await.unwrap();
        assert_eq!(at(&served, 0)["region_id"], "scene.poi.k");
        assert_eq!(at(&served, 2)["object_id"], "scene.object.6");
        // Another person in another room: the bedroom's id, and which of two
        // people Robin is cannot be told.
        let mut memory = super::tests::memory(command, "Meet Robin in the bedroom", vec![segment]);
        let err = bind(&mut memory, &state).await.unwrap_err();
        assert!(err.contains("people"), "{err}");
        let aligned = memory.aligned[0].clone();
        let places = Places::of(&aligned.episode, &state, &aligned.spans, Vec::new());
        let ids = place_ids(
            &places,
            &reply["rtdl"],
            &aligned.spans,
            "Meet Robin in the bedroom",
            &mut serde_json::Map::new(),
        );
        assert_eq!(ids.unwrap()["scene.poi.k"], "scene.poi.b");
    }

    #[test]
    fn several_objects_of_a_label_bind_only_where_they_can_be_told() {
        let listed = json!({
            "kitchen": [{"id": "scene.object.1", "label": "pringles", "restored": true}],
            "bedroom": [{"id": "scene.object.2", "label": "pringles", "seen_s_ago": 3}],
        });
        let found = HashMap::from([("scene.object.2".to_string(), "pringles".to_string())]);
        assert_eq!(
            bind_label("pringles", &found, &listed).unwrap(),
            "scene.object.2"
        );
        assert!(bind_label("pringles", &HashMap::new(), &listed).is_err());
        // One object Scene keeps twice in one room: the record last seen.
        let twice = json!({
            "kitchen": [
                {"id": "scene.object.1", "label": "pringles", "restored": true},
                {"id": "scene.object.2", "label": "pringles", "seen_s_ago": 3},
            ],
            "elsewhere": [],
        });
        assert_eq!(
            bind_label("pringles", &HashMap::new(), &twice).unwrap(),
            "scene.object.2"
        );
    }

    #[test]
    fn no_success_reply_is_skipped_for_a_tree_memory_will_refuse() {
        let said = |text: &str| {
            json!({"content": text, "rtdl": {"op": "do", "cap": "speak", "args": {}}}).to_string()
        };
        let tree = |reply: String| Segment {
            reply,
            contracts: vec!["x/speak".into()],
            op_ids: vec!["ab12-1".into()],
            signature: vec!["ok".into()],
            ..Default::default()
        };
        let command = "Say the time";
        let mut memory = memory(
            command,
            command,
            vec![tree(said("Reading the clock.")), tree(said("It is 3:24."))],
        );
        memory.run.push(Segment {
            contracts: vec!["x/speak".into()],
            reused: true,
            ..Default::default()
        });
        assert!(!memory.expects_more());
    }

    #[tokio::test]
    async fn the_rooms_of_traded_places_trade_too() {
        // The kept planner looked for the peach in the kitchen, where the dinner
        // table stands; with the tables traded it looks in the bedroom.
        let kept_goal = |x: f64, y: f64| json!({"header": {"frame_id": "map"}, "pose": {"position": {"x": x, "y": y}, "orientation": {"w": 1.0}}});
        let first = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": kept_goal(1.0, 1.0)}},
            {"op": "do", "cap": "find", "args": {"label": "peach", "region": "kitchen", "anchor": "dinner table"}},
        ]}});
        let second = json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": kept_goal(5.0, 5.0)}},
            {"op": "do", "cap": "place", "args": {"target": "bedside table"}},
        ]}});
        let places = HashMap::from([
            ("dinner table".to_string(), (1.0, 1.0)),
            ("bedside table".to_string(), (5.0, 5.0)),
        ]);
        let tree = |reply: &Value| Segment {
            reply: reply.to_string(),
            places: places.clone(),
            ..Default::default()
        };
        // A first round: no goal has come in for either table yet.
        let state = json!({"objects": {}, "places": [{"name": "dinner table"}, {"name": "bedside table"}],
            "rooms_of": {"dinner table": "kitchen", "bedside table": "bedroom"}});
        let mut memory = memory(
            "Grasp a peach from the dinner table and place it on the bedside table",
            "Grasp a peach from the bedside table and place it on the dinner table",
            vec![tree(&first), tree(&second)],
        );
        let (served, _) = bind(&mut memory, &state).await.unwrap();
        assert_eq!(at(&served, 1)["region"], "bedroom");
        assert_eq!(at(&served, 1)["anchor"], "bedside table");
        // The bedside table's goal as the kept run drove to it, orientation and all.
        assert_eq!(at(&served, 0)["goal"], kept_goal(5.0, 5.0));
    }

    /// A clause memory over one kept run of "Find a cup in the kitchen then
    /// take it and throw it in the trash".
    fn clause_memory(command: &str) -> TaskMemory {
        let goal = |x: f64, y: f64| json!({"pose": {"position": {"x": x, "y": y}}});
        let tree =
            |children: Value| json!({"rtdl": {"op": "sequence", "children": children}}).to_string();
        let places = HashMap::from([
            ("kitchen".to_string(), (1.0, 1.0)),
            ("kitchen trash bin".to_string(), (2.0, 2.0)),
        ]);
        let segments = vec![
            Segment {
                reply: tree(json!([
                    {"op": "do", "cap": "navigate", "args": {"goal": goal(1.0, 1.0)}},
                    {"op": "do", "cap": "find", "args": {"label": "cup", "region": "kitchen"}},
                ])),
                contracts: vec!["x/navigate".into(), "x/find".into()],
                signature: vec!["ok".into(), "ok".into()],
                places: places.clone(),
                robot: Some((0.0, 0.0)),
                ..Default::default()
            },
            Segment {
                reply: tree(json!([
                    {"op": "do", "cap": "goal_near", "args": {"object_id": "scene.object.1"}, "bind": "g"},
                    {"op": "do", "cap": "navigate", "args": {"goal": {"var": "g", "path": ["goal"]}}},
                    {"op": "do", "cap": "pick", "args": {"object": "cup"}},
                    {"op": "do", "cap": "navigate", "args": {"goal": goal(2.0, 2.0)}},
                    {"op": "do", "cap": "place", "args": {"target": "kitchen trash bin"}},
                    {"op": "do", "cap": "navigate", "args": {"goal": goal(0.0, 0.0)}},
                ])),
                contracts: [
                    "x/goal_near",
                    "x/navigate",
                    "x/pick",
                    "x/navigate",
                    "x/place",
                    "x/navigate",
                ]
                .map(String::from)
                .into(),
                signature: vec!["ok".into(); 6],
                labels: HashMap::from([("scene.object.1".into(), "cup".into())]),
                places,
                ..Default::default()
            },
        ];
        let kept = vec![Episode {
            command: "Find a cup in the kitchen then take it and throw it in the trash".into(),
            segments,
            when: 0.0,
        }];
        let mut memory = memory(command, command, Vec::new());
        memory.mode = Mode::Clauses;
        memory.aligned.clear();
        memory.programs = programs(&kept);
        memory.kept = kept;
        memory
    }

    #[tokio::test]
    async fn a_clause_of_another_command_serves_with_what_its_clauses_name() {
        let command = "Find a bowl in the living room then take it and give it to me";
        let mut memory = clause_memory(command);
        let state = json!({"objects": {"living room": [{"id": "scene.object.9", "label": "bowl"}]},
            "places": [{"name": "living room", "goal": goal(-3.0, -2.0)}]});
        let first = memory.next(&state, 1).await.unwrap();
        assert_eq!(at(&first, 0)["goal"]["pose"]["position"]["x"], -3.0);
        assert_eq!(at(&first, 1)["label"], "bowl");
        assert_eq!(at(&first, 1)["region"], "living room");
        let mut first_plan = plan("p1", "cd34-1", "x/navigate");
        first_plan.nodes.push(RtdlNode {
            node_kind: RTDL_DO,
            op_id: "cd34-2".into(),
            call: Some(CapabilityCall {
                contract_id: "x/find".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        memory.dispatched(&first_plan, &first.to_string(), &state);
        let found = json!({"objects": [{"id": "scene.object.9", "label": "bowl"}]}).to_string();
        let mut results = succeeded("{}");
        results.extend(succeeded(&found).into_iter().map(|mut r| {
            r.node_index = 1;
            r
        }));
        memory.ended("p1", &results);
        // "take it": the kept take, "it" the bowl the first clause named.
        let second = memory.next(&state, 1).await.unwrap();
        assert_eq!(at(&second, 0)["object_id"], "scene.object.9");
        assert_eq!(at(&second, 2)["object"], "bowl");
        assert_eq!(second["rtdl"]["children"].as_array().unwrap().len(), 3);
        // Handing it to the operator no kept run did.
        let mut take = plan("p2", "cd34-3", "x/goal_near");
        for (op, contract) in [("cd34-4", "x/navigate"), ("cd34-5", "x/pick")] {
            take.nodes.push(RtdlNode {
                node_kind: RTDL_DO,
                op_id: op.into(),
                call: Some(CapabilityCall {
                    contract_id: contract.into(),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        memory.dispatched(&take, &second.to_string(), &state);
        let ok = (0..3)
            .flat_map(|i| {
                succeeded("{}").into_iter().map(move |mut r| {
                    r.node_index = i;
                    r
                })
            })
            .collect::<Vec<_>>();
        memory.ended("p2", &ok);
        assert!(memory.next(&state, 1).await.is_none());
    }

    #[test]
    fn a_room_stays_where_a_place_the_command_keeps_stands_in_it() {
        let state = json!({"rooms_of": {"dinner table": "kitchen", "cooking table": "kitchen",
            "bedside table": "bedroom"}});
        let kept = "Grasp a peach from the dinner table and place it on the cooking table";
        let new = "Grasp a peach from the dinner table and place it on the bedside table";
        let spans = align(kept, new).unwrap();
        let (with, rooms) = with_rooms(&spans, &state, kept, new);
        assert_eq!(with, spans);
        assert!(rooms.is_empty());
        // Traded tables trade their rooms.
        let traded = "Grasp a peach from the bedside table and place it on the dinner table";
        let kept = "Grasp a peach from the dinner table and place it on the bedside table";
        let (with, _) = with_rooms(&align(kept, traded).unwrap(), &state, kept, traded);
        assert!(with.contains(&("kitchen".to_string(), "bedroom".to_string())));
        assert!(with.contains(&("bedroom".to_string(), "kitchen".to_string())));
    }

    #[tokio::test]
    async fn a_clause_says_this_commands_words_and_acts_only_on_its_objects() {
        let command = "Find a bowl in the living room then take it and give it to me";
        let mut memory = clause_memory(command);
        let state = json!({"objects": {"living room": [{"id": "scene.object.9", "label": "bowl"}]},
            "places": [{"name": "living room", "goal": goal(-3.0, -2.0)}]});
        let first = memory.next(&state, 1).await.unwrap();
        assert_eq!(first["content"], "Next: Find a bowl in the living room.");
        // "take it" with nothing that says what "it" is: the kept cup is not this command's.
        let mut memory = clause_memory("Go to the living room then take it and give it to me");
        let tree = |c: &str| {
            json!({"rtdl": {"op": "sequence", "children": [
            {"op": "do", "cap": "navigate", "args": {"goal": goal(-3.0, -2.0)}}]}, "content": c})
            .to_string()
        };
        let mut plan = plan("p0", "cd34-0", "x/navigate");
        plan.plan_id = "p0".into();
        memory.dispatched(&plan, &tree(""), &state);
        memory.ended("p0", &succeeded("{}"));
        assert!(memory.next(&state, 1).await.is_none());
    }

    #[test]
    fn a_clause_naming_what_another_kept_clause_named_is_not_reused() {
        let episode = Episode {
            command: "Find a cup in the kitchen then take it".into(),
            segments: Vec::new(),
            when: 0.0,
        };
        let tree = json!({"op": "do", "cap": "pick", "args": {"object": "cup"}});
        let command = "Go to the kitchen then take it";
        assert!(own_names(&episode, &tree, &[], command).is_err());
        let spans = spans(&[("cup", "bowl")]);
        let mut renamed = tree.clone();
        put_in(&mut renamed, &spans);
        assert!(own_names(&episode, &renamed, &spans, command).is_ok());
    }

    #[test]
    fn a_store_that_cannot_be_read_or_is_read_only_is_left_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("task_memory-paths.json");
        let episode = |command: &str| Episode {
            command: command.into(),
            segments: vec![],
            when: 0.0,
        };
        for _ in 0..KEEP + 1 {
            assert!(keep(&path, episode("a")).unwrap());
        }
        assert!(keep(&path, episode("b")).unwrap());
        let store = load(&path).unwrap();
        assert_eq!(store.episodes.len(), KEEP + 1);
        std::fs::write(&path, "{").unwrap();
        assert!(keep(&path, episode("c")).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{");
        std::fs::write(&path, r#"{"episodes": []}"#).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).unwrap();
        assert!(!keep(&path, episode("c")).unwrap());
        assert_eq!(load(&path).unwrap().episodes.len(), 0);
    }
}
