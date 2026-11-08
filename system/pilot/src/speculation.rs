//! Speculative planning. While a tree runs, Pilot asks the model in the
//! background what it would reply once the tree reports, once per likely
//! outcome. When the tree ends with one of those outcomes, the prepared reply
//! stands in for the planning request, so the robot does not stop to wait for
//! it. Nothing speculative is dispatched before the real outcome is known.
//!
//! With a TypeSafe decision model configured (`TYPESAFE_BASE_URL`), a failure
//! is prepared for as up to three situations with a reply each, and the
//! decision model picks the one the real error shows, unless one situation with
//! an empty `when` covers any failure; any other prepared reply that dispatches
//! nothing is used only if the decision model finds it supported by the step
//! results. The questions start as soon as the tree ends.

use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNode, RtdlNodeState, Task};
use crate::planner::{RTDL_DO, RTDL_SEQUENCE};
use crate::rtdl_stream::RtdlIncrementalParser;
use crate::vlm::{Message, ReplyShape, VlmClient, VlmStreamItem};
use anyhow::{Result, bail};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use robonix_scribe::{info, warn};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Requests per tree. The provider serves about four concurrent requests on one
/// context without slowing each down; more would queue behind the success branch.
const MAX_BRANCHES: usize = 4;

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

    /// Whether the reply is prepared per failure situation, which only a
    /// decision model reading the real error can choose between.
    fn has_situations(&self) -> bool {
        matches!(self, Self::Failed(_)) && DECISION_MODEL.is_some()
    }
}

/// One background request and the outcome it assumed.
pub(crate) struct Branch {
    /// The outcome the reply was written for.
    outcome: Outcome,
    /// The reply text once generated, or once settled by [`Speculation::settle`];
    /// otherwise why there is none.
    reply: JoinHandle<Result<String, String>>,
}

impl Drop for Branch {
    fn drop(&mut self) {
        self.reply.abort();
    }
}

impl Branch {
    /// Wait for the reply, or why there is none.
    async fn reply(&mut self) -> Result<String, String> {
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
        let reply = self.reply().await?;
        match dry_run.push(&reply).and_then(|()| dry_run.finish()) {
            Ok((_, None)) => Ok(reply),
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
    /// Least confidence the decision model must give a failure situation for
    /// its reply to be used.
    tau_choice: f64,
    /// Least probability the decision model must give that a reply which
    /// dispatches nothing is supported by the results and finishes the task.
    tau_done: f64,
    /// When the requests were sent.
    started: Instant,
}

impl Speculation {
    /// Send one request per outcome: success, then failure of each step in order.
    /// `prefix` is the request the next planning round would send, up to where
    /// the tree's results would go.
    pub(crate) fn start(
        vlm: &VlmClient,
        plan: &Plan,
        prefix: Vec<Message>,
        task: &Task,
        messages_seen: usize,
        idle_timeout: Duration,
    ) -> Self {
        let is_step = |index: &u32| {
            plan.nodes
                .get(*index as usize)
                .is_some_and(|node| node.node_kind == RTDL_DO)
        };
        let root = &plan.nodes[0];
        let steps: Vec<(u32, String)> = if root.node_kind == RTDL_DO {
            vec![(0, root.op_id.clone())]
        } else if root.node_kind == RTDL_SEQUENCE && root.children.iter().all(is_step) {
            root.children
                .iter()
                .map(|&child| (child, plan.nodes[child as usize].op_id.clone()))
                .collect()
        } else {
            Vec::new()
        };
        let outcomes = std::iter::once(Outcome::Succeeded)
            .chain(
                steps
                    .iter()
                    .map(|(_, op_id)| Outcome::Failed(op_id.clone())),
            )
            .take(MAX_BRANCHES);
        let started = Instant::now();
        let branches = outcomes
            .map(|outcome| {
                let mut messages = prefix.clone();
                messages.push(Message::user(&hypothesis(plan, &outcome)));
                let shape = if outcome.has_situations() {
                    ReplyShape::Situations
                } else {
                    ReplyShape::RtdlEnvelope
                };
                let (vlm, plan_id, label) = (vlm.clone(), plan.plan_id.clone(), outcome.label());
                let reply = tokio::spawn(async move {
                    generate(vlm, messages, shape, &plan_id, &label, idle_timeout)
                        .await
                        .map_err(|error| {
                            warn!(
                                "[pilot/segue] plan_id={plan_id} {label} request failed: {error:#}"
                            );
                            "request failed".to_string()
                        })
                });
                Branch { outcome, reply }
            })
            .collect::<Vec<_>>();
        let context: Value = serde_json::from_str(&task.context_json).unwrap_or_default();
        let tau =
            |key: &str, default: f64| context.get(key).and_then(Value::as_f64).unwrap_or(default);
        let (tau_choice, tau_done) = (tau("tau_choice", TAU_CHOICE), tau("tau_done", TAU_DONE));
        info!(
            "[pilot/segue] {}",
            json!({
                "event": "start",
                "plan_id": plan.plan_id,
                "branches": branches.iter().map(|b| b.outcome.label()).collect::<Vec<_>>(),
                "decision_model": DECISION_MODEL.as_ref().map(|m| &m.model),
                "tau_choice": tau_choice,
                "tau_done": tau_done,
            })
        );
        Self {
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
        self.outputs = leaves
            .iter()
            .filter(|(r, _)| r.state == succeeded)
            .filter_map(|(r, node)| {
                Some((node.op_id.clone(), r.leaf_result.as_ref()?.output.clone()))
            })
            .collect();
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
        let reply = tokio::spawn(async move {
            let mut reply = written.reply().await?;
            let mut state = json!({"task": task, "steps": steps});
            let mut picked = false;
            if written.outcome.has_situations() {
                (reply, picked) = situation(&plan_id, &reply, state.clone(), tau_choice).await?;
            }
            if needs_results(&reply) {
                return Err("reply needs results".into());
            }
            // A situation's reply was written for the error the decision model just matched;
            // asking again whether the results support it only adds a second, noisier answer.
            if DECISION_MODEL.is_some() && !picked {
                let mut envelope: Value =
                    serde_json::from_str(&reply).map_err(|e| format!("reply rejected: {e}"))?;
                if !dispatches(&envelope["rtdl"]) {
                    state["final_reply"] = envelope["content"].take();
                    let question = json!({"type": "noul", "instructions": DONE});
                    decide(&plan_id, state, question, tau_done).await?;
                }
            }
            Ok(reply)
        });
        Ok(Branch { outcome, reply })
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

    /// Log that a prepared reply replaces this planning request.
    pub(crate) fn commit(self, branch: &Branch, waited: Duration) {
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
    }
}

/// The reply prepared for the failure situation the decision model finds in
/// `state`, and whether it picked one: a lone situation with an empty `when`
/// was written for any failure and needs no pick.
async fn situation(
    plan_id: &str,
    reply: &str,
    state: Value,
    tau: f64,
) -> Result<(String, bool), String> {
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
        return Ok((only["reply"].take().to_string(), false));
    }
    let mut criteria: serde_json::Map<String, Value> = situations
        .iter()
        .enumerate()
        .map(|(index, situation)| (index.to_string(), situation["when"].clone()))
        .collect();
    criteria.insert("none".into(), NONE_OF_THEM.into());
    let question = json!({"type": "choice", "instructions": CHOICE, "criteria": criteria});
    let pick = decide(plan_id, state, question, tau).await?;
    let situation = pick
        .parse::<usize>()
        .ok()
        .and_then(|index| situations.get_mut(index))
        .ok_or_else(|| format!("decision model picked unknown situation {pick}"))?;
    Ok((situation["reply"].take().to_string(), true))
}

/// Ask the decision model one question, log it as a `decide` event, and
/// return the option it picked if that is not "none" and has at least
/// probability `tau`. A yes/no question picks "yes" with p(yes).
async fn decide(plan_id: &str, state: Value, question: Value, tau: f64) -> Result<String, String> {
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
        .filter(|&(key, p)| key != "none" && p >= tau)
        .map(|(key, _)| key.to_string());
    info!(
        "[pilot/segue] {}",
        json!({
            "event": "decide",
            "plan_id": plan_id,
            "latency_ms": asked.elapsed().as_millis() as u64,
            "tau": tau,
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
        (Ok(_), Some((key, p))) => format!("decision model: {key} p={p:.3}, tau={tau}"),
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

/// The user message that states one outcome as already reported.
fn hypothesis(plan: &Plan, outcome: &Outcome) -> String {
    let steps: String = plan
        .nodes
        .iter()
        .filter(|node| node.node_kind == RTDL_DO)
        .map(|node| format!("\n- op_id={}: {}", node.op_id, node.description))
        .collect();
    let ended = match outcome {
        Outcome::Succeeded => "finished successfully".to_string(),
        Outcome::Failed(op_id) => format!(
            "ended because step op_id={op_id} failed (its error is not known yet); the steps \
             before it succeeded and the steps after it did not run"
        ),
    };
    let ask = if outcome.has_situations() {
        "Name one to three situations that could have caused this failure and call for different \
         next moves. Answer {\"situations\": [{\"when\": ..., \"reply\": ...}]}: `when` says in one \
         sentence what the failed step's error message would show in that situation, and `reply` \
         is the complete reply you would give once that failure is reported. If the next move \
         is the same whatever the error shows, give one situation with `when` empty."
    } else {
        "Write the reply you would give once this outcome is reported."
    };
    format!(
        "Pilot speculation request (an assumption, not a new user message): assume RTDL plan \
         {plan_id} ({description:?}) has just {ended}. Its steps:{steps}\n\
         The steps' outputs are not known yet, and the live state above was captured before \
         this plan ran. {ask} Do not assume any output value, and do not state one in \
         `content`. Where the next step depends on an output of these steps, pass it with \
         {{\"$ref\": \"<op_id>#/<field>\"}} and decide with `check` inside `fallback`. If that \
         cannot express what the next step needs, reply with `content` exactly \
         \"{NEEDS_RESULTS}\" and an empty sequence. Pilot uses a reply only if the real outcome \
         matches its assumption.",
        plan_id = plan.plan_id,
        description = plan.nodes[0].description,
    )
}

/// Run one background request to completion and return its text.
async fn generate(
    vlm: VlmClient,
    messages: Vec<Message>,
    shape: ReplyShape,
    plan_id: &str,
    outcome: &str,
    idle_timeout: Duration,
) -> Result<String> {
    let started = Instant::now();
    let mut stream = tokio::time::timeout(
        idle_timeout,
        vlm.chat_stream(&messages, &[], Some(vlm.prompt_cache_key()), shape),
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
    info!(
        "[pilot/segue] {}",
        json!({
            "event": "ready",
            "plan_id": plan_id,
            "outcome": outcome,
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
    use super::{Branch, Outcome, Speculation};
    use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
    use crate::pb::pilot::{CapabilityCall, CapabilityCallResult, RtdlNode, RtdlNodeState};
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
                    reply: tokio::spawn(std::future::ready(Ok(outcome.label()))),
                    outcome,
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
            assert_eq!(branch.reply().await.as_deref(), Ok(expected));
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
        let settled = super::situation("p", reply, serde_json::json!({}), 1.0).await;
        assert_eq!(settled, Ok((r#"{"content":"x"}"#.to_string(), false)));
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
