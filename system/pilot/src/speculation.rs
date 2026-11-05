//! Speculative planning. While a tree runs, Pilot asks the model in the
//! background what it would reply once the tree reports, once per likely
//! outcome. When the tree ends with one of those outcomes, the prepared reply
//! stands in for the planning request, so the robot does not stop to wait for
//! it. Nothing speculative is dispatched before the real outcome is known.

use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{Plan, RtdlNodeState};
use crate::planner::{RTDL_DO, RTDL_SEQUENCE};
use crate::vlm::{Message, ReplyShape, VlmClient, VlmStreamItem};
use anyhow::{Result, bail};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use robonix_scribe::{info, warn};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Requests per tree. The provider serves about four concurrent requests on one
/// context without slowing each down; more would queue behind the success branch.
const MAX_BRANCHES: usize = 4;

/// What a prepared reply asks for instead of a plan when it cannot be written
/// without the real outputs. It must fit the reply schema, which providers enforce.
const NEEDS_RESULTS: &str = "NEEDS_RESULTS";

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
    /// The reply text, once generated.
    reply: JoinHandle<Result<String>>,
}

impl Drop for Branch {
    fn drop(&mut self) {
        self.reply.abort();
    }
}

impl Branch {
    /// Whether the request has finished, so waiting for its reply costs nothing.
    pub(crate) fn is_ready(&self) -> bool {
        self.reply.is_finished()
    }

    /// Wait for the reply. A request that failed or was aborted yields none.
    pub(crate) async fn reply(&mut self) -> Option<String> {
        (&mut self.reply).await.ok()?.ok()
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
    /// op_id of every `do` node by node index, to key the outputs of finished steps.
    ops: HashMap<u32, String>,
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
        messages_seen: usize,
        idle_timeout: Duration,
    ) -> Self {
        let ops: HashMap<u32, String> = plan
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.node_kind == RTDL_DO)
            .map(|(index, node)| (index as u32, node.op_id.clone()))
            .collect();
        let root = &plan.nodes[0];
        let steps: Vec<(u32, String)> = if root.node_kind == RTDL_DO {
            vec![(0, root.op_id.clone())]
        } else if root.node_kind == RTDL_SEQUENCE
            && root.children.iter().all(|&child| ops.contains_key(&child))
        {
            root.children
                .iter()
                .map(|&child| (child, ops[&child].clone()))
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
                let (vlm, plan_id, label) = (vlm.clone(), plan.plan_id.clone(), outcome.label());
                let reply = tokio::spawn(async move {
                    generate(vlm, messages, &plan_id, &label, idle_timeout)
                        .await
                        .inspect_err(|error| {
                            warn!(
                                "[pilot/segue] plan_id={plan_id} {label} request failed: {error:#}"
                            )
                        })
                });
                Branch { outcome, reply }
            })
            .collect::<Vec<_>>();
        info!(
            "[pilot/segue] {}",
            serde_json::json!({
                "event": "start",
                "plan_id": plan.plan_id,
                "branches": branches.iter().map(|b| b.outcome.label()).collect::<Vec<_>>(),
            })
        );
        Self {
            plan_id: plan.plan_id.clone(),
            steps,
            ops,
            branches,
            messages_seen,
            ended: None,
            outputs: HashMap::new(),
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
        self.outputs = results
            .iter()
            .filter(|r| r.state == succeeded)
            .filter_map(|r| {
                let op_id = self.ops.get(&r.node_index)?;
                Some((op_id.clone(), r.leaf_result.as_ref()?.output.clone()))
            })
            .collect();
        let state = |index: u32| {
            results
                .iter()
                .find(|r| r.node_index == index)
                .map(|r| r.state)
        };
        let failed = |s: u32| {
            s == RtdlNodeStateEnum::Failed as u32 || s == RtdlNodeStateEnum::Timeout as u32
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
    pub(crate) fn take_branch(&mut self) -> Result<Branch, &'static str> {
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

    /// Log why the prepared replies go unused; dropping them aborts their requests.
    pub(crate) fn discard(self, reason: &str) {
        info!(
            "[pilot/segue] {}",
            serde_json::json!({
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
            serde_json::json!({
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

/// Whether a prepared reply declined to plan without the real outputs.
pub(crate) fn needs_results(reply: &str) -> bool {
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
    format!(
        "Pilot speculation request (an assumption, not a new user message): assume RTDL plan \
         {plan_id} ({description:?}) has just {ended}. Its steps:{steps}\n\
         The steps' outputs are not known yet, and the live state above was captured before \
         this plan ran. Write the reply you would give once this outcome is reported. Do not \
         assume any output value, and do not state one in `content`. Where the next step \
         depends on an output of these steps, pass it with {{\"$ref\": \"<op_id>#/<field>\"}} \
         and decide with `check` inside `fallback`. If that cannot express what the next step \
         needs, reply with `content` exactly \"{NEEDS_RESULTS}\" and an empty sequence. Pilot \
         uses your reply only if the real outcome matches this assumption.",
        plan_id = plan.plan_id,
        description = plan.nodes[0].description,
    )
}

/// Run one background request to completion and return its text.
async fn generate(
    vlm: VlmClient,
    messages: Vec<Message>,
    plan_id: &str,
    outcome: &str,
    idle_timeout: Duration,
) -> Result<String> {
    let started = Instant::now();
    let mut stream = tokio::time::timeout(
        idle_timeout,
        vlm.chat_stream(
            &messages,
            &[],
            Some(vlm.prompt_cache_key()),
            ReplyShape::RtdlEnvelope,
        ),
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
        serde_json::json!({
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
    use crate::pb::pilot::{CapabilityCallResult, RtdlNodeState};
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
        Speculation {
            plan_id: "p".into(),
            steps: (1..=3).map(op).collect(),
            ops: (1..=3).map(op).collect(),
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
            assert_eq!(branch.reply().await.as_deref(), Some(expected));
            let mut outputs: Vec<_> = spec.outputs().keys().map(String::as_str).collect();
            outputs.sort();
            assert_eq!(outputs, finished);
        }
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
}
