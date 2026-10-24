// SPDX-License-Identifier: MulanPSL-2.0
// Poll async MCP capabilities via `<contract_id>/status` until terminal state.

use robonix_scribe::warn;
use std::time::Duration;

use crate::dispatch::{self, CapabilityChannel, async_registry::AsyncGroup};
use crate::pb::pilot::rtdl_node_state::RtdlNodeStateEnum;
use crate::pb::pilot::{CapabilityCall, CapabilityCallResult};
use crate::plan_runtime::{PlanRuntime, RunningAsyncCall};
use crate::rtdl_wire::{self, NodeEventContext};
use robonix_atlas::client::AtlasClient;
use robonix_atlas::pb as atlas_pb;
use tokio::time;

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Dispatch an async cap and poll status every 2s. The caller owns the single
/// terminal event so successful work can be verified before it is published.
/// Only polling is interruptible; initial dispatch must yield its run_id.
pub async fn run_until_terminal(
    call: &CapabilityCall,
    group: &AsyncGroup,
    self_provider_id: &str,
    atlas: &mut AtlasClient,
    node: &NodeEventContext,
    runtime: &PlanRuntime,
) -> (CapabilityCallResult, u32) {
    if runtime.is_cancelled(&node.plan_id).await {
        return (
            canceled_result(call, "plan canceled before async dispatch"),
            RtdlNodeStateEnum::Canceled as u32,
        );
    }
    let initial = dispatch::dispatch(call, self_provider_id, atlas, runtime, &node.plan_id).await;
    if !initial.success {
        return (initial, RtdlNodeStateEnum::Failed as u32);
    }

    let run_id = extract_run_id(&initial.output);
    let accepted = runtime
        .register_or_cancel_async_call(
            &node.plan_id,
            RunningAsyncCall {
                call_id: call.call_id.clone(),
                provider_id: call.provider_id.clone(),
                cancel_contract: group.cancel_contract.clone(),
                run_id: run_id.clone(),
            },
            self_provider_id,
            atlas,
        )
        .await;
    if !accepted {
        let result = canceled_result(call, "plan was cancelled before async polling began");
        return (result, RtdlNodeStateEnum::Canceled as u32);
    }

    let mut interval = time::interval(POLL_INTERVAL);
    interval.tick().await;

    loop {
        let status_out = tokio::select! {
            biased;
            _ = runtime.wait_for_cancellation(&node.plan_id) => None,
            result = async {
                interval.tick().await;
                poll_status(
                    self_provider_id,
                    &call.provider_id,
                    &group.status_contract,
                    &run_id,
                    atlas,
                ).await
            } => Some(result),
        };
        let Some(status_out) = status_out else {
            let result = canceled_result(call, "plan was cancelled; physical stop is unconfirmed");
            return (result, RtdlNodeStateEnum::Canceled as u32);
        };
        let status_out = match status_out {
            Ok(s) => s,
            Err(e) => {
                let error = format!("status poll failed for {}: {e:#}", call.contract_id);
                warn!("[executor] {error}");
                let result = failed_result(call, &error);
                runtime
                    .cancel_execution(&node.plan_id, self_provider_id, atlas)
                    .await;
                return (result, RtdlNodeStateEnum::Failed as u32);
            }
        };

        let (state, detail) = match parse_run_status(&status_out, &run_id) {
            Ok(status) => status,
            Err(error) => {
                runtime
                    .cancel_execution(&node.plan_id, self_provider_id, atlas)
                    .await;
                return (
                    failed_result(call, &format!("status poll failed: {error:#}")),
                    RtdlNodeStateEnum::Failed as u32,
                );
            }
        };
        if rtdl_wire::is_terminal_state(state) {
            let result = terminal_result(call, state, &detail, &status_out);
            runtime
                .unregister_async_call(&node.plan_id, &call.call_id)
                .await;
            return (result, state);
        }
        // Record only live states here. The caller records the final state
        // after optional verification has completed.
        runtime
            .record_op_state(&node.plan_id, &node.op_id, state)
            .await;
    }
}

/// Own the status binding through its RPC so dropping a hung poll releases it.
pub(crate) async fn poll_status(
    consumer_id: &str,
    provider_id: &str,
    status_contract: &str,
    run_id: &str,
    atlas: &mut AtlasClient,
) -> anyhow::Result<String> {
    anyhow::ensure!(!run_id.trim().is_empty(), "status requires an exact run_id");
    let args = serde_json::json!({ "run_id": run_id }).to_string();
    let status_call = CapabilityCall {
        call_id: format!("status-{}", uuid::Uuid::new_v4()),
        provider_id: provider_id.to_string(),
        contract_id: status_contract.to_string(),
        args_json: args,
    };
    let (channel_id, endpoint, _) = atlas
        .connect_capability(
            consumer_id,
            provider_id,
            status_contract,
            atlas_pb::Transport::Mcp,
        )
        .await?;
    let channel = CapabilityChannel::new(atlas, channel_id);
    let result = crate::dispatch::mcp::execute(&status_call, &endpoint).await;
    channel.release().await;
    if result.success {
        Ok(result.output)
    } else {
        anyhow::bail!("{}", result.error)
    }
}

/// Read `run_id` from the async cap's initial MCP response JSON.
pub fn extract_run_id(output: &str) -> String {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(output) else {
        return String::new();
    };
    v.get("run_id")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Validate status evidence before allowing it to release an owned execution.
pub(crate) fn parse_run_status(output: &str, run_id: &str) -> anyhow::Result<(u32, String)> {
    let payload: serde_json::Value = serde_json::from_str(output)?;
    if let Some(known) = payload.get("known") {
        anyhow::ensure!(
            known.as_bool() == Some(true),
            "status does not identify the requested run"
        );
    }
    if let Some(returned_id) = payload.get("run_id") {
        anyhow::ensure!(
            returned_id.as_str() == Some(run_id),
            "status returned a different run_id"
        );
    }
    let state = payload
        .get("state")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_state_name)
        .ok_or_else(|| {
            anyhow::anyhow!("status response missing required 'state' or has unknown state")
        })?;
    let detail = payload
        .get("detail")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok((state, detail))
}

/// Convert status response state names into RTDL node state constants.
pub fn parse_state_name(s: &str) -> Option<u32> {
    match s.to_uppercase().as_str() {
        "PENDING" => Some(RtdlNodeStateEnum::Pending as u32),
        "RUNNING" => Some(RtdlNodeStateEnum::Running as u32),
        "SUCCEEDED" => Some(RtdlNodeStateEnum::Succeeded as u32),
        "FAILED" => Some(RtdlNodeStateEnum::Failed as u32),
        "CANCELED" | "CANCELLED" => Some(RtdlNodeStateEnum::Canceled as u32),
        "TIMEOUT" => Some(RtdlNodeStateEnum::Timeout as u32),
        "PAUSED" => Some(RtdlNodeStateEnum::Paused as u32),
        _ => None,
    }
}

fn terminal_result(
    call: &CapabilityCall,
    state: u32,
    detail: &str,
    raw: &str,
) -> CapabilityCallResult {
    let success = state == RtdlNodeStateEnum::Succeeded as u32;
    CapabilityCallResult {
        call_id: call.call_id.clone(),
        provider_id: call.provider_id.clone(),
        contract_id: call.contract_id.clone(),
        success,
        output: if success {
            raw.to_string()
        } else {
            String::new()
        },
        error: if success {
            String::new()
        } else {
            detail.to_string()
        },
    }
}

fn canceled_result(call: &CapabilityCall, error: &str) -> CapabilityCallResult {
    failed_result(call, error)
}

/// Build a failed capability result for async control-plane failures.
/// The original call identity is preserved so the terminal node event still
/// correlates with the user-requested async capability, not the status poll.
fn failed_result(call: &CapabilityCall, error: &str) -> CapabilityCallResult {
    CapabilityCallResult {
        call_id: call.call_id.clone(),
        provider_id: call.provider_id.clone(),
        contract_id: call.contract_id.clone(),
        success: false,
        output: String::new(),
        error: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_run_id_from_response() {
        assert_eq!(extract_run_id(r#"{"run_id":"r1","accepted":true}"#), "r1");
        assert_eq!(extract_run_id(r#"{"goal_id":"g1"}"#), "");
    }

    #[test]
    fn parse_status_requires_state_field() {
        let (s, d) = parse_run_status(r#"{"state":"SUCCEEDED","detail":"done"}"#, "r1").unwrap();
        assert_eq!(s, RtdlNodeStateEnum::Succeeded as u32);
        assert_eq!(d, "done");
    }

    #[test]
    fn parse_status_missing_state_fails() {
        let error = parse_run_status(r#"{"known":true,"terminal":false}"#, "r1").unwrap_err();
        assert!(error.to_string().contains("missing required 'state'"));
    }

    #[test]
    fn unknown_or_mismatched_runs_never_confirm_terminal_state() {
        for output in [
            r#"{"known":false,"state":"CANCELED"}"#,
            r#"{"known":true,"run_id":"other","state":"CANCELED"}"#,
            r#"{"state":"invalid"}"#,
            "not json",
        ] {
            assert!(parse_run_status(output, "owned").is_err(), "{output}");
        }
    }
}
