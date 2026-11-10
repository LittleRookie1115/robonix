//! Speculative planning. While a tree runs, Pilot asks the model in the
//! background what it would reply once the tree reports, once per likely
//! outcome. When the tree ends with one of those outcomes, the prepared reply
//! stands in for the planning request, so the robot does not stop to wait for
//! it. Nothing speculative is dispatched before the real outcome is known.
//!
//! With a TypeSafe decision model configured (`TYPESAFE_BASE_URL`), each
//! outcome is prepared for as up to three situations, each saying what the
//! failed step's error or the steps' outputs would show, with a reply each. The
//! decision model picks the one the real results show, unless one situation
//! with an empty `when` covers the whole outcome; any other prepared reply that
//! dispatches nothing is used only if the decision model finds it supported by
//! the step results. The questions start as soon as the tree ends.
//!
//! When to prepare follows from how long things take, which Pilot learns as
//! it runs: a tree expected to end before a reply could be written is not
//! prepared for while it runs, and with `lookahead` above 1 in `context_json`,
//! a success reply that would dispatch such a tree has that tree's reply
//! prepared ahead, while the tree before it still runs.
//!
//! Capabilities a task lists under `read_only` in `context_json` change
//! nothing, so a tree of them may run before its turn: a reply that would
//! dispatch one has it run at once, and the reply after it is prepared from
//! what it returned and used only if it returns the same in its turn. A
//! situation whose reply only reads is used whatever the decision model's
//! confidence, since a wrong pick costs one lookup.

use crate::pb::contracts::robonix_system_executor_execute_client::RobonixSystemExecutorExecuteClient;
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, Plan, RtdlNode, RtdlNodeState, Task};
use crate::planner::{CapabilityTargetMap, RTDL_DO, RTDL_SEQUENCE};
use crate::rtdl_stream::{RtdlIncrementalParser, next_op_id};
use crate::vlm::{Message, ReplyShape, VlmClient, VlmStreamItem};
use anyhow::{Result, bail};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use robonix_scribe::{info, warn};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

/// Requests per tree. The provider serves about four concurrent requests on one
/// context without slowing each down; more would queue behind the success branch.
const MAX_BRANCHES: usize = 4;

/// Background requests in flight at once; a request prepared ahead waits here
/// for a branch to finish or to be dropped.
static SLOTS: Semaphore = Semaphore::const_new(MAX_BRANCHES);

/// How long things have taken in this Pilot process, which decides whether a
/// reply can be prepared before a tree ends.
#[derive(Default)]
struct Timings {
    /// The longest successful call of each capability, by the name a reply calls it.
    calls: HashMap<String, Duration>,
    /// About how long a request takes before its reply text begins.
    first_text: Option<Duration>,
    /// About how long a prepared reply takes to write in full.
    reply: Option<Duration>,
}

impl Timings {
    /// How long a tree calling these capabilities is expected to run, once
    /// each has been seen to finish.
    fn expected<'a>(&self, capabilities: impl IntoIterator<Item = &'a str>) -> Option<Duration> {
        capabilities
            .into_iter()
            .map(|capability| self.calls.get(capability))
            .sum()
    }
}

static TIMINGS: LazyLock<Mutex<Timings>> = LazyLock::new(Mutex::default);

fn timings() -> std::sync::MutexGuard<'static, Timings> {
    TIMINGS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The name a reply calls this capability by.
fn called(call: &CapabilityCall) -> String {
    format!(
        "{}.{}",
        call.provider_id,
        crate::discovery::llm_name(&call.contract_id)
    )
}

/// Record that this call succeeded after `time`.
pub(crate) fn took(call: &CapabilityCall, time: Duration) {
    let mut timings = timings();
    let longest = timings.calls.entry(called(call)).or_default();
    *longest = time.max(*longest);
}

/// What a prepared reply asks for instead of a plan when it cannot be written
/// without the real outputs. It must fit the reply schema, which providers enforce.
const NEEDS_RESULTS: &str = "NEEDS_RESULTS";

/// Thresholds picked for the local Qwen3.5-9B decision model at a 2% target
/// error rate, on the Segue offline calibration set plus the questions it was
/// asked in the M3 runs and their counterfactuals. A task sets its own with
/// `tau_choice` / `tau_done` in `context_json`.
const TAU_CHOICE: f64 = 0.9411;
const TAU_DONE: f64 = 0.834;

/// The questions, worded as in the calibration set the thresholds come from.
const CHOICE: &str = "The plan prepared a next move for each situation below. Using only the \
                      step results in State, which situation is what actually happened? If none \
                      of them clearly matches, pick the last option.";
const NONE_OF_THEM: &str = "以上情况都不符合，或者从结果里看不出是哪一种";
const DONE: &str = "Do the step results in State support every claim in the final reply, and \
                    does that reply finish everything the task asked for?";

/// The decision model, if `TYPESAFE_BASE_URL` names one.
static DECISION_MODEL: LazyLock<Option<DecisionModel>> = LazyLock::new(DecisionModel::from_env);

/// A TypeSafe `/v1/systemone` endpoint: it answers a question about a state
/// with probabilities, in a fraction of a planning request's time.
struct DecisionModel {
    /// Client reused across questions.
    http: reqwest::Client,
    /// The systemone endpoint (`TYPESAFE_BASE_URL`).
    url: String,
    /// Model name sent with every question (`TYPESAFE_MODEL`).
    model: String,
    /// Bearer key (`TYPESAFE_API_KEY`); never logged.
    key: Option<String>,
}

impl DecisionModel {
    fn from_env() -> Option<Self> {
        let var = |name| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        Some(Self {
            http: reqwest::Client::new(),
            url: var("TYPESAFE_BASE_URL")?,
            model: var("TYPESAFE_MODEL").unwrap_or_else(|| "jev-latest".into()),
            key: var("TYPESAFE_API_KEY"),
        })
    }

    /// Ask one question about `state` and return its answer object.
    async fn ask(&self, state: &Value, question: &Value) -> Result<Value> {
        let body = json!({"model": self.model, "state": state, "questions": {"q": question}});
        // The planning request runs alongside the question, so a late answer
        // costs nothing; past this, that request has usually answered first.
        let mut request = self
            .http
            .post(&self.url)
            .timeout(Duration::from_secs(5))
            .json(&body);
        if let Some(key) = &self.key {
            request = request.bearer_auth(key);
        }
        let mut answer: Value = request.send().await?.error_for_status()?.json().await?;
        Ok(answer["answers"]["q"].take())
    }
}

/// How a tree ended, in the terms a prepared reply was written for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The tree's root succeeded.
    Succeeded,
    /// The step with this op_id failed after every earlier step succeeded.
    Failed(String),
}

impl Outcome {
    fn label(&self) -> String {
        match self {
            Self::Succeeded => "succeeded".into(),
            Self::Failed(op_id) => format!("failed:{op_id}"),
        }
    }
}

/// One background request and the outcome it assumed.
pub(crate) struct Branch {
    /// The outcome the reply was written for.
    outcome: Outcome,
    /// The reply once generated, or once settled by [`Speculation::settle`];
    /// otherwise why there is none.
    reply: JoinHandle<Result<Prepared, String>>,
    /// What was prepared ahead for the tree the resolved reply dispatches.
    next: Option<Box<Ahead>>,
}

/// A prepared reply and what was prepared beyond it.
struct Prepared {
    /// The reply as written, or once settled, the one reply to use.
    text: String,
    /// For each reply in `text`, in order, what was prepared ahead for the
    /// tree it dispatches.
    next: Vec<Option<Ahead>>,
    /// What the steps of the tree this reply follows returned when they were
    /// run ahead, in step order, if the reply was written from that.
    read: Option<Vec<Value>>,
}

/// What lets Pilot run a prepared tree of read-only calls before its turn.
pub(crate) struct Reader {
    /// The catalog a reply's capability names resolve against.
    pub(crate) targets: CapabilityTargetMap,
    /// Executor's entry for the plans Pilot runs for itself.
    pub(crate) graph: RobonixSystemExecutorExecuteClient<Channel>,
}

/// A reply being prepared for the success of a tree that is not dispatched yet.
struct Ahead {
    /// That tree's steps in order: the op_id the request named each by, and
    /// the capability it calls.
    steps: Vec<(String, String)>,
    /// The request.
    branch: Branch,
    /// When the request was sent.
    started: Instant,
}

impl Drop for Branch {
    fn drop(&mut self) {
        self.reply.abort();
    }
}

impl Branch {
    /// Wait for the reply, or why there is none.
    async fn reply(&mut self) -> Result<Prepared, String> {
        (&mut self.reply)
            .await
            .unwrap_or_else(|_| Err("request failed".into()))
    }

    /// Whether the reply is settled, so waiting for it costs nothing.
    pub(crate) fn is_settled(&self) -> bool {
        self.reply.is_finished()
    }

    /// The settled reply to use in place of the planning request, or why there
    /// is none. `dry_run` admits it against the live catalog without dispatching it.
    pub(crate) async fn resolve(
        &mut self,
        mut dry_run: RtdlIncrementalParser,
    ) -> Result<String, String> {
        let Prepared {
            text: reply,
            mut next,
            ..
        } = self.reply().await?;
        match dry_run.push(&reply).and_then(|()| dry_run.finish()) {
            Ok((_, None)) => {
                self.next = next.pop().flatten().map(Box::new);
                Ok(reply)
            }
            Ok((_, Some(_))) => Err("reply is a plan-control op".into()),
            Err(error) => Err(format!("reply rejected: {error:#}")),
        }
    }
}

/// Replies prepared for the planning round that one running tree's report will start.
pub(crate) struct Speculation {
    /// The tree the replies were written for.
    pub(crate) plan_id: String,
    /// Steps a failure hypothesis can name, in execution order, as node index
    /// and op_id. Only a tree that runs its steps one after another has them,
    /// because only there does "step k failed" fix what else ran.
    steps: Vec<(u32, String)>,
    /// The tree's nodes by index, to name and describe the steps it reports.
    nodes: Vec<RtdlNode>,
    /// Requests not yet taken, one per outcome.
    branches: Vec<Branch>,
    /// User messages the task table held at the start; a steer adds one, and
    /// a reply prepared before it would ignore it.
    pub(crate) messages_seen: usize,
    /// How the tree ended, once its completion arrived; `None` inside when the
    /// ending matches no outcome a reply could be written for.
    ended: Option<Option<Outcome>>,
    /// Raw outputs of the tree's succeeded steps by op_id, for `$ref` binding.
    outputs: HashMap<String, String>,
    /// The tree's finished steps as the decision model reads them.
    report: Vec<Value>,
    /// The user's words for the task, as the decision model reads them.
    task: String,
    /// Least confidence the decision model must give a situation for its reply
    /// to be used.
    tau_choice: f64,
    /// Least probability the decision model must give that a reply which
    /// dispatches nothing is supported by the results and finishes the task.
    tau_done: f64,
    /// When the requests were sent.
    started: Instant,
    /// For a speculation prepared ahead, the tree's steps as its request named
    /// them, until [`Speculation::adopt`] meets the tree.
    ahead: Vec<(String, String)>,
    /// Op_ids that requests prepared ahead used for steps not dispatched yet,
    /// with the op_ids those steps got.
    renames: Vec<(String, String)>,
    /// The task's read-only capabilities, by the name after the provider's.
    reads: Arc<HashSet<String>>,
}

/// The `do` steps of a tree that run one after another under its root, as node
/// index and op_id. Only there does "step k failed" fix what else ran.
fn steps_in_order(plan: &Plan) -> Vec<(u32, String)> {
    let is_step = |index: &u32| {
        plan.nodes
            .get(*index as usize)
            .is_some_and(|node| node.node_kind == RTDL_DO)
    };
    let root = &plan.nodes[0];
    if root.node_kind == RTDL_DO {
        vec![(0, root.op_id.clone())]
    } else if root.node_kind == RTDL_SEQUENCE && root.children.iter().all(is_step) {
        root.children
            .iter()
            .map(|&child| (child, plan.nodes[child as usize].op_id.clone()))
            .collect()
    } else {
        Vec::new()
    }
}

/// A tree's capability calls, in node order.
fn calls(plan: &Plan) -> impl Iterator<Item = (&RtdlNode, &CapabilityCall)> {
    plan.nodes
        .iter()
        .filter(|node| node.node_kind == RTDL_DO)
        .filter_map(|node| Some((node, node.call.as_ref()?)))
}

impl Speculation {
    /// Send one request per outcome: success, then failure of each step in order.
    /// `prefix` is the request the next planning round would send, up to where
    /// the tree's results would go. `None` if the tree is expected to end
    /// before a prepared reply could answer sooner than that round's request.
    pub(crate) fn start(
        vlm: &VlmClient,
        plan: &Plan,
        prefix: Vec<Message>,
        task: &Task,
        messages_seen: usize,
        idle_timeout: Duration,
        reader: Reader,
    ) -> Option<Self> {
        let names: Vec<String> = calls(plan).map(|(_, call)| called(call)).collect();
        {
            let timings = timings();
            if let Some(run) = timings.expected(names.iter().map(String::as_str))
                && let (Some(first_text), Some(reply)) = (timings.first_text, timings.reply)
                && run + first_text < reply
            {
                info!(
                    "[pilot/segue] {}",
                    json!({
                        "event": "skip",
                        "plan_id": plan.plan_id,
                        "expected_ms": run.as_millis() as u64,
                        "first_text_ms": first_text.as_millis() as u64,
                        "reply_ms": reply.as_millis() as u64,
                    })
                );
                return None;
            }
        }
        let steps = steps_in_order(plan);
        let outcomes = std::iter::once(Outcome::Succeeded)
            .chain(
                steps
                    .iter()
                    .map(|(_, op_id)| Outcome::Failed(op_id.clone())),
            )
            .take(MAX_BRANCHES);
        // Only a decision model reading the real results can choose between situations.
        let situations = DECISION_MODEL.is_some();
        let context: Value = serde_json::from_str(&task.context_json).unwrap_or_default();
        let listed = context["read_only"].as_array();
        let contracts = listed.into_iter().flatten().filter_map(Value::as_str);
        let reads: Arc<HashSet<String>> =
            Arc::new(contracts.map(crate::discovery::llm_name).collect());
        let request = Arc::new(Request {
            vlm: vlm.clone(),
            shape: if situations {
                ReplyShape::Situations
            } else {
                ReplyShape::RtdlEnvelope
            },
            plan_id: plan.plan_id.clone(),
            idle_timeout,
            reads: reads.clone(),
            reader,
        });
        let tau =
            |key: &str, default: f64| context.get(key).and_then(Value::as_f64).unwrap_or(default);
        let (tau_choice, tau_done) = (tau("tau_choice", TAU_CHOICE), tau("tau_done", TAU_DONE));
        let lookahead = context
            .get("lookahead")
            .and_then(Value::as_u64)
            .unwrap_or(1);
        let tree = format!(
            "RTDL plan {} ({:?})",
            plan.plan_id, plan.nodes[0].description
        );
        let listed: String = plan
            .nodes
            .iter()
            .filter(|node| node.node_kind == RTDL_DO)
            .map(|node| format!("\n- op_id={}: {}", node.op_id, node.description))
            .collect();
        let started = Instant::now();
        let branches = outcomes
            .map(|outcome| {
                let mut messages = prefix.clone();
                messages.push(Message::user(&hypothesis(
                    &tree, &listed, &outcome, situations, false,
                )));
                // Failures are rare, so only the success path is prepared further along.
                let ahead = if outcome == Outcome::Succeeded {
                    lookahead.saturating_sub(1)
                } else {
                    0
                };
                let reply =
                    tokio::spawn(prepare(request.clone(), messages, outcome.label(), ahead));
                Branch {
                    outcome,
                    reply,
                    next: None,
                }
            })
            .collect::<Vec<_>>();
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "start",
                "plan_id": plan.plan_id,
                "branches": branches.iter().map(|b| b.outcome.label()).collect::<Vec<_>>(),
                "decision_model": DECISION_MODEL.as_ref().map(|m| &m.model),
                "tau_choice": tau_choice,
                "tau_done": tau_done,
                "lookahead": lookahead,
            })
        );
        Some(Self {
            plan_id: plan.plan_id.clone(),
            steps,
            nodes: plan.nodes.clone(),
            branches,
            messages_seen,
            ended: None,
            outputs: HashMap::new(),
            report: Vec::new(),
            task: task.text.clone(),
            tau_choice,
            tau_done,
            started,
            ahead: Vec::new(),
            renames: Vec::new(),
            reads,
        })
    }

    /// Meet the tree a speculation prepared ahead was written for. `None` if
    /// `plan` is not that tree, or a user message arrived since.
    pub(crate) fn adopt(mut self, plan: &Plan, messages: usize) -> Option<Self> {
        self.plan_id = plan.plan_id.clone();
        let met: Vec<(String, String)> = calls(plan)
            .map(|(node, call)| (node.op_id.clone(), called(call)))
            .collect();
        if met.len() != self.ahead.len() || met.iter().zip(&self.ahead).any(|(a, b)| a.1 != b.1) {
            self.discard("not the tree prepared for");
            return None;
        }
        if messages != self.messages_seen {
            self.discard("user message arrived");
            return None;
        }
        let made_up = self.ahead.drain(..).map(|(op_id, _)| op_id);
        self.renames
            .extend(made_up.zip(met.into_iter().map(|(op_id, _)| op_id)));
        self.nodes = plan.nodes.clone();
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "start",
                "plan_id": self.plan_id,
                "branches": ["succeeded"],
                "ahead_ms": self.started.elapsed().as_millis() as u64,
                "ready": self.branches.iter().all(|b| b.reply.is_finished()),
            })
        );
        Some(self)
    }

    /// Drop the reply prepared for the failure of a step that has succeeded,
    /// which frees its request for one prepared ahead.
    pub(crate) fn progressed(&mut self, plan_id: &str, node: &RtdlNodeState) {
        if plan_id == self.plan_id
            && node.state == RtdlNodeStateEnum::Succeeded as u32
            && let Some(step) = self.nodes.get(node.node_index as usize)
        {
            let failed = Outcome::Failed(step.op_id.clone());
            self.branches.retain(|branch| branch.outcome != failed);
        }
    }

    /// Record how the tree ended, from its completion.
    pub(crate) fn ended(
        &mut self,
        plan_id: &str,
        results: &[RtdlNodeState],
        any_failed: bool,
        canceled: bool,
    ) {
        if plan_id != self.plan_id {
            return;
        }
        let succeeded = RtdlNodeStateEnum::Succeeded as u32;
        let failed = |s: u32| {
            s == RtdlNodeStateEnum::Failed as u32 || s == RtdlNodeStateEnum::Timeout as u32
        };
        let mut leaves: Vec<(&RtdlNodeState, &RtdlNode)> = results
            .iter()
            .filter_map(|r| {
                let node = self.nodes.get(r.node_index as usize)?;
                (node.node_kind == RTDL_DO).then_some((r, node))
            })
            .collect();
        leaves.sort_by_key(|(r, _)| r.node_index);
        // A speculation prepared ahead already holds the outputs of the trees before this one.
        self.outputs.extend(
            leaves
                .iter()
                .filter(|(r, _)| r.state == succeeded)
                .filter_map(|(r, node)| {
                    Some((node.op_id.clone(), r.leaf_result.as_ref()?.output.clone()))
                }),
        );
        let parse = |text: &str| serde_json::from_str(text).unwrap_or_else(|_| Value::from(text));
        self.report = leaves
            .iter()
            .filter(|(r, _)| r.state == succeeded || failed(r.state))
            .map(|(r, node)| {
                let call = node.call.clone().unwrap_or_default();
                let leaf = r.leaf_result.clone().unwrap_or_default();
                let mut step = json!({
                    "op": node.op_id,
                    "capability": call.contract_id,
                    "args": parse(&call.args_json),
                });
                if r.state == succeeded {
                    step["status"] = "succeeded".into();
                    // The decision model reads text only, and one image's base64
                    // alone exceeds its input limit.
                    step["output"] = if crate::history::is_image_output(&leaf.output) {
                        "[image, not shown]".into()
                    } else {
                        parse(&leaf.output)
                    };
                } else {
                    step["status"] = "failed".into();
                    step["error"] = if leaf.error.is_empty() {
                        leaf.output
                    } else {
                        leaf.error
                    }
                    .into();
                }
                step
            })
            .collect();
        let state = |index: u32| {
            results
                .iter()
                .find(|r| r.node_index == index)
                .map(|r| r.state)
        };
        let outcome = if canceled {
            None
        } else if !any_failed {
            Some(Outcome::Succeeded)
        } else {
            self.steps
                .iter()
                .find(|(index, _)| state(*index) != Some(succeeded))
                .filter(|(index, _)| state(*index).is_some_and(failed))
                .map(|(_, op_id)| Outcome::Failed(op_id.clone()))
        };
        self.ended = Some(outcome);
    }

    /// Take the branch written for the outcome the tree actually had.
    fn take_branch(&mut self) -> Result<Branch, &'static str> {
        let outcome = match &self.ended {
            None => return Err("tree has not ended"),
            Some(None) => return Err("outcome not speculated"),
            Some(Some(outcome)) => outcome,
        };
        let index = self
            .branches
            .iter()
            .position(|b| &b.outcome == outcome)
            .ok_or("outcome not speculated")?;
        Ok(self.branches.swap_remove(index))
    }

    /// Outputs of the tree's succeeded steps, for binding `$ref`s that name them.
    pub(crate) fn outputs(&self) -> &HashMap<String, String> {
        &self.outputs
    }

    /// The tree's finished steps, as the decision model reads them.
    pub(crate) fn report(&self) -> &[Value] {
        &self.report
    }

    /// Take the branch for the outcome the tree actually had and settle its
    /// reply in the background, so the decision model answers while the round
    /// gathers its context. `steps` holds every step the turn has reported.
    pub(crate) fn settle(&mut self, steps: Vec<Value>) -> Result<Branch, &'static str> {
        let mut written = self.take_branch()?;
        let outcome = written.outcome.clone();
        let (plan_id, task) = (self.plan_id.clone(), self.task.clone());
        let (tau_choice, tau_done) = (self.tau_choice, self.tau_done);
        let renames = self.renames.clone();
        let (reads, own) = (self.reads.clone(), self.report.clone());
        let reply = tokio::spawn(async move {
            let Prepared {
                text: mut reply,
                mut next,
                read,
            } = written.reply().await?;
            if let Some(read) = read {
                let now = own.iter().map(|step| &step["output"]);
                if read.len() != own.len() || !read.iter().zip(now).all(|(a, b)| same(a, b)) {
                    return Err("results differ from those read ahead".into());
                }
            }
            let mut state = json!({"task": task, "steps": steps});
            let mut picked = false;
            if DECISION_MODEL.is_some() {
                let index;
                (reply, index, picked) =
                    situation(&plan_id, &reply, state.clone(), tau_choice, &reads).await?;
                next = next.into_iter().skip(index).take(1).collect();
            }
            let reply = renamed(reply, &renames);
            if needs_results(&reply) {
                return Err("reply needs results".into());
            }
            // A situation's reply was written for the results the decision model just matched;
            // asking again whether the results support it only adds a second, noisier answer.
            if DECISION_MODEL.is_some() && !picked {
                let mut envelope: Value =
                    serde_json::from_str(&reply).map_err(|e| format!("reply rejected: {e}"))?;
                if !dispatches(&envelope["rtdl"]) {
                    state["final_reply"] = envelope["content"].take();
                    let question = json!({"type": "noul", "instructions": DONE});
                    decide(&plan_id, state, question, |_| tau_done).await?;
                }
            }
            Ok(Prepared {
                text: reply,
                next,
                read: None,
            })
        });
        Ok(Branch {
            outcome,
            reply,
            next: None,
        })
    }

    /// Log why the prepared replies go unused; dropping them aborts their requests.
    pub(crate) fn discard(self, reason: &str) {
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "discard",
                "plan_id": self.plan_id,
                "reason": reason,
                "outcome": self.ended.clone().flatten().map(|o| o.label()),
                "unfinished": self.branches.iter().filter(|b| !b.reply.is_finished()).count(),
            })
        );
    }

    /// Log that a prepared reply replaces this planning request, and hand on
    /// what was prepared ahead for the tree that reply dispatches.
    pub(crate) fn commit(self, branch: &mut Branch, waited: Duration) -> Option<Self> {
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "commit",
                "plan_id": self.plan_id,
                "outcome": branch.outcome.label(),
                "waited_ms": waited.as_millis() as u64,
                "since_start_ms": self.started.elapsed().as_millis() as u64,
                "unfinished": self.branches.iter().filter(|b| !b.reply.is_finished()).count(),
            })
        );
        let ahead = branch.next.take()?;
        Some(Self {
            plan_id: String::new(),
            steps: Vec::new(),
            nodes: Vec::new(),
            branches: vec![ahead.branch],
            ended: None,
            report: Vec::new(),
            started: ahead.started,
            ahead: ahead.steps,
            ..self
        })
    }
}

/// `reply` with each op_id a request prepared ahead made up for a step
/// replaced by the op_id that step got.
fn renamed(mut reply: String, renames: &[(String, String)]) -> String {
    for (made_up, op_id) in renames {
        for end in ['#', '"'] {
            reply = reply.replace(&format!("\"{made_up}{end}"), &format!("\"{op_id}{end}"));
        }
    }
    reply
}

/// The reply prepared for the situation the decision model finds in `state`,
/// that situation's index, and whether the model picked it: a lone situation
/// with an empty `when` was written for the whole outcome and needs no pick.
async fn situation(
    plan_id: &str,
    reply: &str,
    state: Value,
    tau: f64,
    reads: &HashSet<String>,
) -> Result<(String, usize, bool), String> {
    let mut reply: Value = serde_json::from_str(reply).map_err(|e| format!("situations: {e}"))?;
    let situations = reply
        .get_mut("situations")
        .and_then(Value::as_array_mut)
        .filter(|situations| !situations.is_empty())
        .ok_or("situations: none given")?;
    if let [only] = situations.as_mut_slice()
        && only["when"]
            .as_str()
            .is_some_and(|when| when.trim().is_empty())
    {
        return Ok((only["reply"].take().to_string(), 0, false));
    }
    let mut criteria: serde_json::Map<String, Value> = situations
        .iter()
        .enumerate()
        .map(|(index, situation)| (index.to_string(), situation["when"].clone()))
        .collect();
    criteria.insert("none".into(), NONE_OF_THEM.into());
    let question = json!({"type": "choice", "instructions": CHOICE, "criteria": criteria});
    // A reply that only reads cannot do harm, so it needs no confidence.
    let free: Vec<bool> = situations
        .iter()
        .map(|situation| reads_only(&situation["reply"]["rtdl"], reads))
        .collect();
    let needed = |key: &str| match key.parse::<usize>().ok().and_then(|index| free.get(index)) {
        Some(true) => 0.0,
        _ => tau,
    };
    let pick = decide(plan_id, state, question, needed).await?;
    let (index, situation) = pick
        .parse::<usize>()
        .ok()
        .and_then(|index| Some((index, situations.get_mut(index)?)))
        .ok_or_else(|| format!("decision model picked unknown situation {pick}"))?;
    Ok((situation["reply"].take().to_string(), index, true))
}

/// Ask the decision model one question, log it as a `decide` event, and
/// return the option it picked if that is not "none" and has at least the
/// probability `tau` asks of that option. A yes/no question picks "yes" with
/// p(yes).
async fn decide(
    plan_id: &str,
    state: Value,
    question: Value,
    tau: impl Fn(&str) -> f64,
) -> Result<String, String> {
    let model = DECISION_MODEL.as_ref().ok_or("no decision model")?;
    let asked = Instant::now();
    let answer = model
        .ask(&state, &question)
        .await
        .map_err(|error| format!("decision model failed: {error:#}"));
    let pick = answer
        .as_ref()
        .ok()
        .and_then(|a| match a["type"].as_str()? {
            "noul" => Some(("yes", a["noul"].as_f64()?)),
            // Hosted models report a confidence rescaled by the option count; the
            // thresholds are calibrated on the probability itself.
            "choice" => {
                let key = a["choice"].as_str()?;
                Some((key, a["probabilities"][key].as_f64()?))
            }
            _ => None,
        });
    let accepted = pick
        .filter(|&(key, p)| key != "none" && p >= tau(key))
        .map(|(key, _)| key.to_string());
    info!(
        "[pilot/segue] {}",
        json!({
            "event": "decide",
            "plan_id": plan_id,
            "latency_ms": asked.elapsed().as_millis() as u64,
            "tau": pick.map(|(key, _)| tau(key)),
            "accept": accepted.is_some(),
            "answer": answer.as_ref().ok(),
            "error": answer.as_ref().err(),
            "state": state,
            "question": question,
        })
    );
    accepted.ok_or_else(|| match (&answer, pick) {
        (Err(error), _) => error.clone(),
        (Ok(_), None) => "decision model answer unreadable".into(),
        (Ok(_), Some((key, p))) => format!("decision model: {key} p={p:.3}, tau={}", tau(key)),
    })
}

/// Whether a prepared reply's RTDL tree dispatches any step.
fn dispatches(node: &Value) -> bool {
    node["op"] == "do"
        || node["children"]
            .as_array()
            .is_some_and(|children| children.iter().any(dispatches))
}

/// Whether a prepared reply declined to plan without the real outputs.
fn needs_results(reply: &str) -> bool {
    reply.contains(NEEDS_RESULTS)
}

/// A prepared reply as the stream a planning request would have returned.
pub(crate) fn replay(reply: String) -> BoxStream<'static, Result<VlmStreamItem>> {
    futures_util::stream::iter([Ok(VlmStreamItem::TextDelta(reply))]).boxed()
}

/// The user message that states one outcome of `tree` as already reported,
/// with its steps `listed`; with `situations`, it asks for a reply per
/// situation the outcome could show. With `returned`, `listed` gives what each
/// step returned when it was run ahead, so nothing about them is left to guess.
fn hypothesis(
    tree: &str,
    listed: &str,
    outcome: &Outcome,
    situations: bool,
    returned: bool,
) -> String {
    let ended = match outcome {
        Outcome::Succeeded => "finished successfully".to_string(),
        Outcome::Failed(op_id) => format!(
            "ended because step op_id={op_id} failed (its error is not known yet); the steps \
             before it succeeded and the steps after it did not run"
        ),
    };
    let assume_nothing = "Do not assume any output value, and do not state one in `content`.";
    let beyond_listed = "Beyond the outputs listed, do not assume any output value, and do not \
                         state one in `content`.";
    let (ask, assume) = match outcome {
        _ if !situations => (
            "Write the reply you would give once this outcome is reported.",
            if returned {
                beyond_listed
            } else {
                assume_nothing
            },
        ),
        Outcome::Succeeded if returned => (
            "Answer {\"situations\": [{\"when\": \"\", \"reply\": ...}]}, where `reply` is the \
             complete reply you would give once these outputs are reported.",
            beyond_listed,
        ),
        Outcome::Failed(_) => (
            "Name one to three situations that could have caused this failure and call for \
             different next moves. Answer {\"situations\": [{\"when\": ..., \"reply\": ...}]}: \
             `when` says in one sentence what the failed step's error message would show in that \
             situation, and `reply` is the complete reply you would give once that failure is \
             reported. If the next move is the same whatever the error shows, give one situation \
             with `when` empty.",
            assume_nothing,
        ),
        Outcome::Succeeded => (
            "Name one to three situations that the steps' outputs could show and that call for \
             different next moves. Answer {\"situations\": [{\"when\": ..., \"reply\": ...}]}: \
             `when` says in one sentence what the outputs would show in that situation, and \
             `reply` is the complete reply you would give once those outputs are reported. A \
             `when` may rest only on what the outputs say as text, never on what an image \
             shows: the situation is picked without seeing images. If the next move is the same \
             whatever the outputs show, give one situation with `when` empty.",
            "Beyond what a situation's `when` says, do not assume any output value, and do not \
             state one in its reply's `content`.",
        ),
    };
    let known = if returned {
        "Each step is listed with what it returned when Pilot ran it just now; Pilot uses this \
         reply only if the steps return the same when the plan runs in its turn, so plan on \
         them as listed. The live state above was captured before these plans ran."
    } else {
        "The steps' outputs are not known yet, and the live state above was captured before \
         this plan ran."
    };
    format!(
        "Pilot speculation request (an assumption, not a new user message): assume {tree} has \
         just {ended}. Its steps:{listed}\n\
         {known} {ask} {assume} Where the next step depends on an output of these \
         steps, pass it with {{\"$ref\": \"<op_id>#/<field>\"}} and decide with `check` inside \
         `fallback`. If that cannot express what the next step needs, reply with `content` \
         exactly \"{NEEDS_RESULTS}\" and an empty sequence. Pilot uses a reply only if the real \
         outcome matches its assumption."
    )
}

/// What the requests of one speculation share.
struct Request {
    /// The planning model.
    vlm: VlmClient,
    /// The shape every reply is asked in.
    shape: ReplyShape,
    /// The running tree the speculation started from, which its log records name.
    plan_id: String,
    /// Longest wait for the stream to open or to send its next item.
    idle_timeout: Duration,
    /// The task's read-only capabilities, by the name after the provider's.
    reads: Arc<HashSet<String>>,
    /// Runs a prepared tree of those capabilities before its turn.
    reader: Reader,
}

/// Whether a reply's tree calls something and only capabilities in `reads`.
fn reads_only(tree: &Value, reads: &HashSet<String>) -> bool {
    let nodes = do_nodes(tree);
    let read = |node: &&Value| {
        let name = node["cap"].as_str().and_then(|cap| cap.rsplit_once('.'));
        name.is_some_and(|(_, name)| reads.contains(name))
    };
    !nodes.is_empty() && nodes.iter().all(read)
}

/// Whether two outputs say the same, allowing numbers the drift of a repeated measurement.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (
                a.as_f64().unwrap_or(f64::NAN),
                b.as_f64().unwrap_or(f64::NAN),
            );
            (a - b).abs() <= 0.1_f64.max(0.2 * a.abs().max(b.abs()))
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| same(a, b)))
        }
        _ => a == b,
    }
}

/// Run a reply's tree now and return what each of its steps returned, in step
/// order. `None` unless every step succeeded with text: a reply that names an
/// output not known yet does not parse, and an image cannot be compared later.
async fn read_ahead(reader: &Reader, reply: &Value) -> Option<Vec<Value>> {
    let mut parser = RtdlIncrementalParser::new(
        reader.targets.clone(),
        Default::default(),
        "pilot-read-ahead".into(),
        0,
    );
    parser.push(&reply.to_string()).ok()?;
    let (_, None) = parser.finish().ok()? else {
        return None;
    };
    let mut plan = parser.plan;
    plan.plan_id = format!("read-ahead-{}", uuid::Uuid::new_v4());
    let calls = plan.nodes.iter_mut().filter_map(|node| node.call.as_mut());
    for (index, call) in calls.enumerate() {
        call.call_id = format!("{}:{index}", plan.plan_id);
    }
    let steps: Vec<u32> = (0..plan.nodes.len() as u32)
        .filter(|&index| plan.nodes[index as usize].node_kind == RTDL_DO)
        .collect();
    let mut graph = reader.graph.clone();
    let run = async move {
        let mut events = graph
            .execute(tonic::Request::new(plan))
            .await
            .ok()?
            .into_inner();
        let mut returned = HashMap::new();
        while let Some(event) = events.message().await.ok()? {
            let Some(node) = event.node_state else {
                continue;
            };
            let state = RtdlNodeStateEnum::try_from(node.state as i32).ok()?;
            match (state, node.leaf_result) {
                (RtdlNodeStateEnum::Succeeded, Some(result)) => {
                    if crate::history::is_image_output(&result.output) {
                        return None;
                    }
                    let output = serde_json::from_str(&result.output)
                        .unwrap_or_else(|_| Value::from(result.output));
                    returned.insert(node.node_index, output);
                }
                (
                    RtdlNodeStateEnum::Failed
                    | RtdlNodeStateEnum::Canceled
                    | RtdlNodeStateEnum::Timeout,
                    _,
                ) => return None,
                _ => {}
            }
        }
        Some(returned)
    };
    let mut returned = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .ok()??;
    steps.iter().map(|index| returned.remove(index)).collect()
}

/// Write the reply for the outcome `label` names, then, while `ahead` trees
/// further are allowed, the replies after the trees that reply would dispatch.
fn prepare(
    request: Arc<Request>,
    messages: Vec<Message>,
    label: String,
    ahead: u64,
) -> BoxFuture<'static, Result<Prepared, String>> {
    async move {
        let text = generate(&request, messages.clone(), &label)
            .await
            .map_err(|error| {
                warn!(
                    "[pilot/segue] plan_id={} {label} request failed: {error:#}",
                    request.plan_id
                );
                "request failed".to_string()
            })?;
        let next = match ahead {
            0 => Vec::new(),
            _ => look_ahead(&request, messages, &text, &label, ahead - 1),
        };
        Ok(Prepared {
            text,
            next,
            read: None,
        })
    }
    .boxed()
}

/// The `do` nodes of a reply's RTDL tree, in the order its steps are numbered.
fn do_nodes(node: &Value) -> Vec<&Value> {
    if node["op"] == "do" {
        return vec![node];
    }
    let children = node["children"].as_array();
    children.into_iter().flatten().flat_map(do_nodes).collect()
}

/// For each reply in `text`, start preparing the reply after the tree it
/// dispatches, if that tree is expected to end before a reply could be written
/// while it runs. `messages` is the request `text` answered.
fn look_ahead(
    request: &Arc<Request>,
    messages: Vec<Message>,
    text: &str,
    label: &str,
    ahead: u64,
) -> Vec<Option<Ahead>> {
    let Ok(written) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    // Without a decision model the reply is one envelope rather than situations.
    let replies: Vec<(&Value, &Value)> = match written["situations"].as_array() {
        Some(situations) => situations
            .iter()
            .map(|s| (&s["when"], &s["reply"]))
            .collect(),
        None => vec![(&Value::Null, &written)],
    };
    let prepare_for = |(index, (when, reply)): (usize, (&Value, &Value))| {
        let nodes = do_nodes(&reply["rtdl"]);
        let names = nodes.iter().map(|node| node["cap"].as_str().unwrap_or(""));
        if nodes.is_empty() {
            return None;
        }
        let (run, write) = {
            let timings = timings();
            (timings.expected(names.clone()), timings.reply)
        };
        let short = run.zip(write).is_some_and(|(run, write)| run < write);
        // A tree that only reads is short whatever was timed, and can run now.
        let readable = reads_only(&reply["rtdl"], &request.reads);
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "ahead",
                "plan_id": request.plan_id,
                "outcome": format!("{label}/{index}"),
                "expected_ms": run.map(|run| run.as_millis() as u64),
                "reply_ms": write.map(|write| write.as_millis() as u64),
                "reads_only": readable,
                "sent": short || readable,
            })
        );
        if !(short || readable) {
            return None;
        }
        let steps: Vec<(String, String)> = names.map(|name| (next_op_id(), name.into())).collect();
        let described: Vec<String> = steps
            .iter()
            .zip(&nodes)
            .map(|((op_id, _), node)| {
                let description = node["description"].as_str().unwrap_or("");
                format!("\n- op_id={op_id}: {description}")
            })
            .collect();
        let plan = reply["rtdl"]["description"].as_str().unwrap_or("");
        let tree = match when.as_str().map(str::trim).filter(|when| !when.is_empty()) {
            Some(when) => format!(
                "the outputs showed the situation {when:?}, Pilot used that situation's reply \
                 above, and the RTDL plan it dispatched ({plan:?})"
            ),
            None => {
                format!("Pilot used the reply above, and the RTDL plan it dispatched ({plan:?})")
            }
        };
        let mut messages = messages.clone();
        messages.push(Message::assistant(text));
        let (request, reply, label) = (request.clone(), reply.clone(), format!("{label}/{index}"));
        let reply = tokio::spawn(async move {
            let read = match readable {
                true => read_ahead(&request.reader, &reply).await,
                false => None,
            };
            if readable {
                info!(
                    "[pilot/segue] {}",
                    json!({"event": "read", "plan_id": request.plan_id, "outcome": label,
                           "returned": read.is_some()})
                );
            }
            let listed: String = match &read {
                Some(read) => described
                    .iter()
                    .zip(read)
                    .map(|(step, output)| format!("{step}; returned {output}"))
                    .collect(),
                None => described.concat(),
            };
            let ask = hypothesis(
                &tree,
                &listed,
                &Outcome::Succeeded,
                DECISION_MODEL.is_some(),
                read.is_some(),
            );
            messages.push(Message::user(&ask));
            let prepared = prepare(request, messages, label, ahead).await?;
            Ok(Prepared { read, ..prepared })
        });
        Some(Ahead {
            steps,
            branch: Branch {
                outcome: Outcome::Succeeded,
                reply,
                next: None,
            },
            started: Instant::now(),
        })
    };
    replies.into_iter().enumerate().map(prepare_for).collect()
}

/// Run one background request to completion and return its text.
async fn generate(request: &Request, messages: Vec<Message>, outcome: &str) -> Result<String> {
    let Request {
        vlm,
        shape,
        plan_id,
        idle_timeout,
        ..
    } = request;
    let idle_timeout = *idle_timeout;
    let _slot = SLOTS.acquire().await?;
    let started = Instant::now();
    let mut stream = tokio::time::timeout(
        idle_timeout,
        vlm.chat_stream(&messages, &[], Some(vlm.prompt_cache_key()), *shape),
    )
    .await??;
    let mut text = String::new();
    let mut first_token = None;
    let mut usage = None;
    while let Some(item) = tokio::time::timeout(idle_timeout, stream.next()).await? {
        match item? {
            VlmStreamItem::TextDelta(delta) => {
                first_token.get_or_insert_with(|| started.elapsed());
                text.push_str(&delta);
            }
            VlmStreamItem::Usage(value) => usage = Some(value),
            VlmStreamItem::ToolCall(call) => bail!("tool call {}", call.function.name),
            VlmStreamItem::ReasoningDelta(_) | VlmStreamItem::Finish => {}
        }
    }
    // A reply that declines to plan is quick to write and says nothing about one that plans.
    if !needs_results(&text) {
        let smooth = |seen: &mut Option<Duration>, new: Duration| {
            *seen = Some(seen.map_or(new, |old| old.mul_f64(0.7) + new.mul_f64(0.3)));
        };
        let mut timings = timings();
        if let Some(first) = first_token {
            smooth(&mut timings.first_text, first);
        }
        smooth(&mut timings.reply, started.elapsed());
    }
    info!(
        "[pilot/segue] {}",
        json!({
            "event": "ready",
            "plan_id": plan_id,
            "outcome": outcome,
            "depth": outcome.matches('/').count() + 1,
            "latency_ms": started.elapsed().as_millis() as u64,
            "first_token_ms": first_token.map(|t| t.as_millis() as u64),
            "input_tokens": usage.as_ref().map(|u| u.prompt_tokens),
            "cached_input_tokens": usage.as_ref().and_then(|u| u.cached_tokens),
            "output_tokens": usage.as_ref().map(|u| u.completion_tokens),
            "needs_results": needs_results(&text),
        })
    );
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::{Branch, Outcome, Prepared, Speculation};
    use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
    use crate::pb::pilot::{CapabilityCall, CapabilityCallResult, Plan, RtdlNode, RtdlNodeState};
    use crate::planner::{RTDL_DO, RTDL_SEQUENCE};
    use std::collections::HashMap;
    use std::time::Instant;

    /// A sequence `0` of three `do` steps `1..=3`, with one branch per outcome.
    fn three_steps() -> Speculation {
        let op = |index: u32| (index, format!("p-{index}"));
        let outcomes = [
            Outcome::Succeeded,
            Outcome::Failed("p-1".into()),
            Outcome::Failed("p-2".into()),
            Outcome::Failed("p-3".into()),
        ];
        let step = |index: u32| RtdlNode {
            node_kind: RTDL_DO,
            op_id: format!("p-{index}"),
            call: Some(CapabilityCall {
                contract_id: "robonix/service/map/get_pose".into(),
                args_json: "{}".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let root = RtdlNode {
            node_kind: RTDL_SEQUENCE,
            children: vec![1, 2, 3],
            ..Default::default()
        };
        Speculation {
            plan_id: "p".into(),
            steps: (1..=3).map(op).collect(),
            nodes: std::iter::once(root).chain((1..=3).map(step)).collect(),
            branches: outcomes
                .into_iter()
                .map(|outcome| Branch {
                    reply: tokio::spawn(std::future::ready(Ok(Prepared {
                        text: outcome.label(),
                        next: Vec::new(),
                        read: None,
                    }))),
                    outcome,
                    next: None,
                })
                .collect(),
            messages_seen: 1,
            ended: None,
            outputs: HashMap::new(),
            report: Vec::new(),
            task: String::new(),
            tau_choice: super::TAU_CHOICE,
            tau_done: super::TAU_DONE,
            started: Instant::now(),
            ahead: Vec::new(),
            renames: Vec::new(),
            reads: Default::default(),
        }
    }

    fn state(node_index: u32, state: RtdlNodeStateEnum) -> RtdlNodeState {
        RtdlNodeState {
            plan_id: "p".into(),
            node_index,
            state: state as u32,
            leaf_result: Some(CapabilityCallResult {
                output: format!(r#"{{"step":{node_index}}}"#),
                error: if state == RtdlNodeStateEnum::Succeeded {
                    String::new()
                } else {
                    format!("step {node_index} failed")
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn the_branch_for_the_real_outcome_is_taken() {
        use RtdlNodeStateEnum::{Canceled, Failed, Succeeded, Timeout};
        let cases = [
            (
                vec![
                    (0, Succeeded),
                    (1, Succeeded),
                    (2, Succeeded),
                    (3, Succeeded),
                ],
                false,
                "succeeded",
                &["p-1", "p-2", "p-3"][..],
            ),
            (
                vec![(0, Failed), (1, Succeeded), (2, Failed), (3, Canceled)],
                true,
                "failed:p-2",
                &["p-1"],
            ),
            (vec![(0, Failed), (1, Timeout)], true, "failed:p-1", &[]),
        ];
        for (states, any_failed, expected, finished) in cases {
            let mut spec = three_steps();
            let results: Vec<_> = states.into_iter().map(|(i, s)| state(i, s)).collect();
            spec.ended("p", &results, any_failed, false);
            let mut branch = spec.take_branch().unwrap();
            assert_eq!(branch.reply().await.unwrap().text, expected);
            let mut outputs: Vec<_> = spec.outputs().keys().map(String::as_str).collect();
            outputs.sort();
            assert_eq!(outputs, finished);
        }
    }

    #[test]
    fn the_report_lists_finished_steps_in_plan_order() {
        use RtdlNodeStateEnum::{Canceled, Failed, Succeeded};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let mut spec = three_steps();
        let results = [
            state(3, Canceled),
            state(2, Failed),
            state(0, Failed),
            state(1, Succeeded),
        ];
        spec.ended("p", &results, true, false);
        let report = serde_json::to_value(spec.report()).unwrap();
        assert_eq!(
            report,
            serde_json::json!([
                {"op": "p-1", "capability": "robonix/service/map/get_pose", "args": {},
                 "status": "succeeded", "output": {"step": 1}},
                {"op": "p-2", "capability": "robonix/service/map/get_pose", "args": {},
                 "status": "failed", "error": "step 2 failed"},
            ])
        );
    }

    #[test]
    fn the_report_leaves_image_data_out() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let mut spec = three_steps();
        let mut snapshot = state(1, RtdlNodeStateEnum::Succeeded);
        snapshot.leaf_result.as_mut().unwrap().output =
            r#"{"width": 640, "height": 480, "encoding": "jpeg", "data": "/9j/4AAQ"}"#.into();
        spec.ended("p", &[snapshot], false, false);
        assert_eq!(spec.report()[0]["output"], "[image, not shown]");
    }

    #[test]
    fn endings_no_reply_was_written_for_take_no_branch() {
        use RtdlNodeStateEnum::{Canceled, Failed, Succeeded};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let mut spec = three_steps();
        spec.ended("other", &[], false, false);
        assert_eq!(spec.take_branch().err(), Some("tree has not ended"));
        spec.ended("p", &[state(0, Canceled)], false, true);
        assert_eq!(spec.take_branch().err(), Some("outcome not speculated"));
        // A step that never ran cannot be the one that failed.
        let results = [state(0, Failed), state(1, Succeeded), state(2, Canceled)];
        spec.ended("p", &results, true, false);
        assert_eq!(spec.take_branch().err(), Some("outcome not speculated"));
    }

    #[tokio::test]
    async fn a_lone_situation_without_when_is_used_without_asking() {
        let reply = r#"{"situations": [{"when": " ", "reply": {"content": "x"}}]}"#;
        let settled =
            super::situation("p", reply, serde_json::json!({}), 1.0, &Default::default()).await;
        assert_eq!(settled, Ok((r#"{"content":"x"}"#.to_string(), 0, false)));
    }

    #[test]
    fn with_a_decision_model_every_outcome_asks_for_situations() {
        for outcome in [Outcome::Succeeded, Outcome::Failed("p-0".into())] {
            assert!(
                super::hypothesis("plan p", "", &outcome, true, false).contains(r#"{"situations""#)
            );
            assert!(
                !super::hypothesis("plan p", "", &outcome, false, false).contains("situations")
            );
        }
        let read = super::hypothesis("plan p", "", &Outcome::Succeeded, true, true);
        assert!(read.contains(r#"{"when": "", "reply": ...}"#) && !read.contains("not known yet"));
    }

    #[test]
    fn a_tree_reads_only_if_every_call_is_a_listed_capability() {
        use serde_json::json;
        let reads = ["scene_find".to_string()].into();
        let call = |cap: &str| json!({"op": "do", "cap": cap});
        let tree = |calls: Vec<serde_json::Value>| json!({"op": "sequence", "children": calls});
        assert!(super::reads_only(
            &tree(vec![call("scene.scene_find"), call("b.scene_find")]),
            &reads
        ));
        assert!(!super::reads_only(
            &tree(vec![call("scene.scene_find"), call("arm.arm_pick")]),
            &reads
        ));
        assert!(!super::reads_only(&tree(vec![]), &reads));
    }

    #[test]
    fn outputs_are_the_same_up_to_the_drift_of_a_repeated_measurement() {
        use serde_json::json;
        let seen = json!({"objects": [{"id": "o1", "x": 1.52, "last_seen_unix": 1794297111.4, "count": 40}], "detail": ""});
        let again = json!({"objects": [{"id": "o1", "x": 1.49, "last_seen_unix": 1794297150.9, "count": 44}], "detail": ""});
        assert!(super::same(&seen, &again));
        assert!(!super::same(
            &seen,
            &json!({"objects": [], "detail": "no object matches"})
        ));
        let moved = json!({"objects": [{"id": "o1", "x": 2.9, "last_seen_unix": 1794297150.9, "count": 44}], "detail": ""});
        assert!(!super::same(&seen, &moved));
        let other = json!({"objects": [{"id": "o2", "x": 1.52, "last_seen_unix": 1794297111.4, "count": 40}], "detail": ""});
        assert!(!super::same(&seen, &other));
    }

    #[tokio::test]
    async fn a_reply_written_from_results_read_ahead_needs_the_same_results() {
        use RtdlNodeStateEnum::Succeeded;
        let written = |read: Vec<serde_json::Value>| {
            let mut spec = three_steps();
            spec.branches[0].reply = tokio::spawn(std::future::ready(Ok(Prepared {
                text: r#"{"content": "x"}"#.into(),
                next: Vec::new(),
                read: Some(read),
            })));
            let results: Vec<_> = (0..=3).map(|index| state(index, Succeeded)).collect();
            spec.ended("p", &results, false, false);
            spec.settle(Vec::new()).unwrap()
        };
        let step = |index: u32| serde_json::json!({"step": index});
        let reply = written(vec![step(1), step(2), step(3)]).reply().await;
        assert_eq!(reply.unwrap().text, r#"{"content": "x"}"#);
        let changed = written(vec![step(1), step(2), step(9)]).reply().await;
        assert_eq!(
            changed.err().unwrap(),
            "results differ from those read ahead"
        );
        assert!(written(vec![step(1)]).reply().await.is_err());
    }

    #[test]
    fn a_tree_is_expected_to_take_what_its_calls_took_once_all_were_seen() {
        let seconds = std::time::Duration::from_secs;
        let timings = super::Timings {
            calls: HashMap::from([("a".into(), seconds(1)), ("b".into(), seconds(30))]),
            ..Default::default()
        };
        assert_eq!(timings.expected(["a", "b", "a"]), Some(seconds(32)));
        assert_eq!(timings.expected(["a", "never seen"]), None);
    }

    #[tokio::test]
    async fn a_step_that_succeeded_has_no_failure_left_to_prepare_for() {
        let mut spec = three_steps();
        spec.progressed("other", &state(1, RtdlNodeStateEnum::Succeeded));
        spec.progressed("p", &state(2, RtdlNodeStateEnum::Running));
        assert_eq!(spec.branches.len(), 4);
        spec.progressed("p", &state(1, RtdlNodeStateEnum::Succeeded));
        let left: Vec<_> = spec.branches.iter().map(|b| b.outcome.label()).collect();
        assert_eq!(left, ["succeeded", "failed:p-2", "failed:p-3"]);
    }

    /// The reply a test prepares ahead: two calls under a fallback, then a third.
    fn nested_reply() -> serde_json::Value {
        let call = |name: &str| {
            serde_json::json!({"op": "do", "op_id": 0, "description": name,
                               "cap": format!("x.t_{name}"), "args": {}})
        };
        serde_json::json!({"content": "", "message_updates": [], "rtdl_parent_task_id": null,
        "rtdl_description": "d", "task_updates": [],
        "rtdl": {"op": "sequence", "op_id": 0, "description": "d", "children": [
            {"op": "fallback", "op_id": 0, "description": "f",
             "children": [call("a"), call("b")]},
            call("c"),
        ]}})
    }

    /// `nested_reply` as the planner parses it when it is dispatched.
    fn nested_plan() -> Plan {
        let targets = ["a", "b", "c"].map(|n| (format!("x.t_{n}"), ("x".into(), format!("t/{n}"))));
        let mut parser = crate::rtdl_stream::RtdlIncrementalParser::new(
            HashMap::from(targets),
            Default::default(),
            "session".into(),
            0,
        );
        parser.push(&nested_reply().to_string()).unwrap();
        parser.finish().unwrap();
        parser.plan
    }

    #[tokio::test]
    async fn a_speculation_prepared_ahead_meets_only_the_tree_it_named() {
        let reply = nested_reply();
        let named: Vec<(String, String)> = super::do_nodes(&reply["rtdl"])
            .iter()
            .zip(["m-1", "m-2", "m-3"])
            .map(|(node, op_id)| (op_id.into(), node["cap"].as_str().unwrap().into()))
            .collect();
        let ahead = |named: &[(String, String)]| Speculation {
            plan_id: String::new(),
            nodes: Vec::new(),
            ahead: named.to_vec(),
            ..three_steps()
        };
        let plan = nested_plan();
        let met = ahead(&named).adopt(&plan, 1).unwrap();
        let got: Vec<&str> = super::calls(&plan)
            .map(|(node, _)| node.op_id.as_str())
            .collect();
        let renames: Vec<(&str, &str)> = met
            .renames
            .iter()
            .map(|(made_up, op_id)| (made_up.as_str(), op_id.as_str()))
            .collect();
        assert_eq!(renames, [("m-1", got[0]), ("m-2", got[1]), ("m-3", got[2])]);
        assert_eq!(met.plan_id, plan.plan_id);

        assert!(ahead(&named).adopt(&plan, 2).is_none());
        assert!(ahead(&named[..2]).adopt(&plan, 1).is_none());
        let mut other = named.clone();
        other.swap(0, 2);
        assert!(ahead(&other).adopt(&plan, 1).is_none());
    }

    #[test]
    fn a_made_up_op_id_is_replaced_only_where_it_is_the_whole_id() {
        let reply = r#"{"a":{"$ref":"m-1#/x"},"b":{"$ref":"m-1"},"c":{"$ref":"m-10#/x"}}"#;
        let renames = [("m-1".to_string(), "r-7".to_string())];
        assert_eq!(
            super::renamed(reply.into(), &renames),
            r#"{"a":{"$ref":"r-7#/x"},"b":{"$ref":"r-7"},"c":{"$ref":"m-10#/x"}}"#
        );
    }

    #[test]
    fn a_tree_dispatches_if_any_node_is_a_do() {
        use serde_json::json;
        let nested =
            json!({"op": "sequence", "children": [{"op": "fallback", "children": [{"op": "do"}]}]});
        assert!(super::dispatches(&nested));
        assert!(!super::dispatches(
            &json!({"op": "sequence", "children": []})
        ));
        assert!(!super::dispatches(&serde_json::Value::Null));
    }
}
