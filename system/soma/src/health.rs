// SPDX-License-Identifier: MulanPSL-2.0
//
// Soma health data collector — discovers health primitives via Atlas,
// consumes their gRPC HealthState streams, aggregates data into
// SomaHealthSnapshot, and broadcasts to subscribers (Vitals).

use crate::pb::soma::{
    ActuatorState, ComponentStatus, FaultState, SafetyState, Scalar, SomaHealthSnapshot,
};
use crate::service::SomaService;
use anyhow::{Context, Result};
use robonix_atlas::client::AtlasClient;
use robonix_scribe::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

const SCHEMA_VERSION: u32 = 1;
const QUALITY_VALID: u32 = 0;
const KIND_BODY: u32 = 1;
const KIND_ARM: u32 = 2;
const KIND_JOINT: u32 = 4;
const KIND_GRIPPER: u32 = 6;
const OP_ACTIVE: u32 = 4;
const SAFETY_NORMAL: u32 = 1;
const SAFETY_FAULT: u32 = 5;
const SAFETY_ESTOP: u32 = 4;
const FAULT_ERROR: u32 = 2;

/// Start health data collection: discover primitives via Atlas,
/// consume their gRPC HealthState streams, aggregate into
/// SomaHealthSnapshot, and publish through the Soma service. Returns false
/// when no health primitive is available so the caller can start its generic
/// runtime-state fallback instead.
pub async fn start_health_collector(
    mut atlas: AtlasClient,
    body_id: String,
    arm_model: String,
    service: Arc<SomaService>,
) -> Result<bool> {
    use robonix_atlas::pb as atlas_pb;

    info!("[soma-health] discovering health primitives...");

    // Discover providers implementing robonix/primitive/health/stream.
    let capabilities = atlas
        .flatten_capabilities(
            "robonix/primitive/health/stream",
            "",
            atlas_pb::Transport::Grpc,
        )
        .await
        .context("discover health primitives")?;

    if capabilities.is_empty() {
        info!("[soma-health] no health primitives found; using runtime-state fallback");
        return Ok(false);
    }

    // Deduplicate by provider_id.
    let mut seen = std::collections::HashSet::new();
    let providers: Vec<_> = capabilities
        .into_iter()
        .filter(|c| seen.insert(c.provider_id.clone()))
        .collect();

    info!(
        "[soma-health] found {} health primitive(s)",
        providers.len()
    );

    let latest = Arc::new(Mutex::new(HashMap::new()));
    let sequence = Arc::new(AtomicU64::new(0));

    for cap in providers {
        let provider_id = cap.provider_id.clone();
        let body_id = body_id.clone();
        let arm_model = arm_model.clone();
        let mut atlas = atlas.clone();
        let latest = Arc::clone(&latest);
        let sequence = Arc::clone(&sequence);
        let service = Arc::clone(&service);

        tokio::spawn(async move {
            loop {
                let result = consume_primitive_stream(
                    &mut atlas,
                    &provider_id,
                    body_id.clone(),
                    arm_model.clone(),
                    Arc::clone(&service),
                    Arc::clone(&latest),
                    Arc::clone(&sequence),
                )
                .await;
                match result {
                    Ok(()) => robonix_scribe::warn!(
                        "[soma-health] primitive '{}' stream ended; reconnecting",
                        provider_id
                    ),
                    Err(e) => robonix_scribe::warn!(
                        "[soma-health] primitive '{}' stream failed: {e:#}; reconnecting",
                        provider_id
                    ),
                }
                let disconnected_snapshot = {
                    let mut states = latest.lock().await;
                    states.remove(&provider_id).map(|_| {
                        merged_health_snapshot(
                            &states,
                            &body_id,
                            &arm_model,
                            sequence.fetch_add(1, Ordering::Relaxed) + 1,
                        )
                    })
                };
                if let Some(snapshot) = disconnected_snapshot {
                    service.publish_snapshot(snapshot).await;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    Ok(true)
}

/// Connect to one health primitive's StreamHealthState gRPC and forward data.
async fn consume_primitive_stream(
    atlas: &mut AtlasClient,
    provider_id: &str,
    body_id: String,
    arm_model: String,
    service: Arc<SomaService>,
    latest: Arc<Mutex<HashMap<String, crate::pb::health::HealthState>>>,
    sequence: Arc<AtomicU64>,
) -> Result<()> {
    use crate::pb::contracts::robonix_primitive_health_stream_client::RobonixPrimitiveHealthStreamClient;
    use crate::pb::health::StreamHealthStateRequest;
    use robonix_atlas::pb as atlas_pb;

    // Get the endpoint from Atlas.
    let (_channel_id, endpoint_str, _params) = atlas
        .connect_capability(
            "soma",
            provider_id,
            "robonix/primitive/health/stream",
            atlas_pb::Transport::Grpc,
        )
        .await
        .with_context(|| format!("connect to health primitive '{provider_id}'"))?;

    let normalized = if endpoint_str.starts_with("http") {
        endpoint_str.clone()
    } else {
        format!("http://{endpoint_str}")
    };

    info!(
        "[soma-health] connected to health primitive '{}' at {}",
        provider_id, normalized
    );

    let channel = Channel::from_shared(normalized.clone())
        .context("invalid endpoint")?
        .connect()
        .await
        .context("dial health primitive")?;

    let mut client = RobonixPrimitiveHealthStreamClient::new(channel);
    let mut stream = client
        .stream_health_state(StreamHealthStateRequest {})
        .await
        .context("open StreamHealthState")?
        .into_inner();

    loop {
        let result = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .with_context(|| format!("health stream from '{provider_id}' timed out"))?;
        let Some(result) = result else {
            break;
        };
        match result {
            Ok(health_state) => {
                let snapshot = {
                    let mut states = latest.lock().await;
                    states.insert(provider_id.to_string(), health_state);
                    merged_health_snapshot(
                        &states,
                        &body_id,
                        &arm_model,
                        sequence.fetch_add(1, Ordering::Relaxed) + 1,
                    )
                };
                service.publish_snapshot(snapshot).await;
            }
            Err(e) => {
                robonix_scribe::warn!("[soma-health] stream error from '{}': {e:#}", provider_id);
                break;
            }
        }
    }

    Ok(())
}

/// Convert a HealthState frame from the primitive into a SomaHealthSnapshot.
fn health_state_to_snapshot(
    state: &crate::pb::health::HealthState,
    body_id: &str,
    arm_model: &str,
    seq: u64,
) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();

    // Build component tree: root + arm + joints.
    let mut components = vec![
        component("body", "", KIND_BODY, "Piper Robot", "base_link", "piper"),
        component(
            "body/arm",
            "body",
            KIND_ARM,
            "Piper arm",
            "arm_base_link",
            arm_model,
        ),
    ];
    for joint_idx in 1..=6 {
        components.push(component(
            &format!("body/arm/joint_{joint_idx}"),
            "body/arm",
            KIND_JOINT,
            &format!("joint_{joint_idx}"),
            &format!("joint_{joint_idx}"),
            arm_model,
        ));
    }
    components.push(component(
        "body/arm/gripper",
        "body/arm",
        KIND_GRIPPER,
        "gripper",
        "gripper_base",
        "piper_gripper",
    ));

    // Parse sensor readings into per-joint data.
    let mut joint_temps = [-1.0f64; 6];
    let mut joint_driver_temps = [-1.0f64; 6];
    let mut joint_voltages = [-1.0f64; 6];
    let mut joint_currents = [-1.0f64; 6];
    let mut joint_errors = [0u32; 6];
    let mut joint_enabled = [false; 6];
    let mut joint_communication = [false; 6];
    let mut arm_state: u32 = SAFETY_NORMAL;
    let mut arm_error = 0u32;
    let mut gripper_error = 0u32;
    let mut gripper_enabled = false;
    let mut gripper_homed = false;
    let mut gripper_opening_m = 0.0f64;

    for reading in &state.readings {
        let name = &reading.name;
        // Parse joint index from name like "body/arm/joint_N/..."
        if let Some(joint_idx) = parse_joint_index(name) {
            if name.ends_with("/motor_temp") {
                joint_temps[joint_idx] = reading.temp_c as f64;
            } else if name.ends_with("/driver_temp") {
                joint_driver_temps[joint_idx] = reading.temp_c as f64;
            } else if name.ends_with("/voltage") {
                joint_voltages[joint_idx] = reading.voltage as f64;
                joint_currents[joint_idx] = reading.current_a as f64;
            } else if name.ends_with("/error") {
                joint_errors[joint_idx] = reading.current_a as u32;
            } else if name.ends_with("/enabled") {
                joint_enabled[joint_idx] = reading.current_a >= 0.5;
            } else if name.ends_with("/communication_ok") {
                joint_communication[joint_idx] = reading.current_a >= 0.5;
            }
        } else if name == "body/arm/state" {
            let raw = reading.current_a as u32;
            arm_state = match raw {
                0 => SAFETY_NORMAL,
                2 => SAFETY_ESTOP,
                _ => SAFETY_FAULT,
            };
        } else if name == "body/arm/error" {
            arm_error = reading.current_a as u32;
        } else if name == "body/arm/gripper/error" {
            gripper_error = reading.current_a as u32;
        } else if name == "body/arm/gripper/enabled" {
            gripper_enabled = reading.current_a >= 0.5;
        } else if name == "body/arm/gripper/homed" {
            gripper_homed = reading.current_a >= 0.5;
        } else if name == "body/arm/gripper/opening_m" {
            gripper_opening_m = reading.current_a as f64;
        }
    }

    // Build actuators from parsed joint data.
    let actuators: Vec<ActuatorState> = (0..6)
        .map(|i| {
            let joint_idx = (i + 1) as u32;
            ActuatorState {
                component_id: format!("body/arm/joint_{joint_idx}"),
                joint_name: format!("joint_{joint_idx}"),
                position: Some(scalar(0.0, "rad")),
                velocity: Some(scalar(0.0, "rad/s")),
                effort: Some(scalar(0.0, "Nm")),
                current: (joint_currents[i] >= 0.0).then(|| scalar(joint_currents[i], "A")),
                voltage: (joint_voltages[i] >= 0.0).then(|| scalar(joint_voltages[i], "V")),
                motor_temp: (joint_temps[i] >= 0.0).then(|| scalar(joint_temps[i], "degC")),
                driver_temp: (joint_driver_temps[i] >= 0.0)
                    .then(|| scalar(joint_driver_temps[i], "degC")),
                torque_enabled: joint_enabled[i],
                brake_engaged: false,
                communication_ok: joint_communication[i],
                vendor_mode: 0,
                vendor_error_code: joint_errors[i],
                status_flags: joint_errors[i],
            }
        })
        .collect();
    let mut actuators = actuators;
    actuators.push(ActuatorState {
        component_id: "body/arm/gripper".to_string(),
        joint_name: "gripper".to_string(),
        position: Some(scalar(gripper_opening_m, "m")),
        velocity: Some(scalar(0.0, "m/s")),
        effort: None,
        current: None,
        voltage: None,
        motor_temp: None,
        driver_temp: None,
        torque_enabled: gripper_enabled,
        brake_engaged: false,
        communication_ok: joint_communication.iter().all(|ok| *ok),
        vendor_mode: if gripper_homed { 1 } else { 0 },
        vendor_error_code: gripper_error,
        status_flags: gripper_error,
    });

    // Build faults from non-zero error codes.
    let mut faults = Vec::new();
    if arm_error != 0 {
        faults.push(FaultState {
            component_id: "body/arm".to_string(),
            fault_id: "piper_controller_fault".to_string(),
            severity: FAULT_ERROR,
            active: true,
            clearable: true,
            onset_ts_ns: now_ns,
            vendor_code: arm_error,
            vendor_code_text: format!("0x{arm_error:X}"),
            message: format!("Piper controller error_code=0x{arm_error:X}"),
            attributes: vec![],
            vendor_raw_json: String::new(),
        });
    }
    for (i, err) in joint_errors.iter().enumerate() {
        if *err != 0 {
            faults.push(FaultState {
                component_id: format!("body/arm/joint_{}", i + 1),
                fault_id: "piper_foc_fault".to_string(),
                severity: FAULT_ERROR,
                active: true,
                clearable: true,
                onset_ts_ns: now_ns,
                vendor_code: *err,
                vendor_code_text: format!("0x{err:02X}"),
                message: format!("joint_{} foc_status=0x{err:02X}", i + 1),
                attributes: vec![],
                vendor_raw_json: String::new(),
            });
        }
    }
    if gripper_error != 0 {
        faults.push(FaultState {
            component_id: "body/arm/gripper".to_string(),
            fault_id: "piper_gripper_fault".to_string(),
            severity: FAULT_ERROR,
            active: true,
            clearable: true,
            onset_ts_ns: now_ns,
            vendor_code: gripper_error,
            vendor_code_text: format!("0x{gripper_error:02X}"),
            message: format!("gripper status=0x{gripper_error:02X}"),
            attributes: vec![],
            vendor_raw_json: String::new(),
        });
    }

    let communication_ok = joint_communication.iter().all(|ok| *ok);
    let motion_allowed = arm_state == SAFETY_NORMAL && communication_ok;
    let motor_power_allowed = motion_allowed && joint_enabled.iter().all(|enabled| *enabled);

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body_id.to_string(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators,
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed,
            motor_power_allowed,
            aggregate_state: arm_state,
            detail: String::new(),
        }),
        safety_endpoints: vec![],
        faults,
        metrics: vec![],
    }
}

/// Merge the latest frame from every discovered health provider into one body.
fn merged_health_snapshot(
    states: &HashMap<String, crate::pb::health::HealthState>,
    body_id: &str,
    arm_model: &str,
    seq: u64,
) -> SomaHealthSnapshot {
    if states.is_empty() {
        return empty_snapshot(body_id, arm_model, seq);
    }

    let now_ns = chrono_now_ns();
    let mut components = vec![component(
        "body",
        "",
        KIND_BODY,
        "Dual Piper robot",
        "dual_piper_base",
        body_id,
    )];
    let mut actuators = Vec::new();
    let mut faults = Vec::new();
    let mut safety_endpoints = Vec::new();
    let mut motion_allowed = true;
    let mut motor_power_allowed = true;
    let mut aggregate_state = SAFETY_NORMAL;

    let mut providers: Vec<_> = states.iter().collect();
    providers.sort_by(|left, right| left.0.cmp(right.0));
    for (provider_id, state) in providers {
        let side = provider_side(provider_id);
        let arm_id = format!("body/{side}_arm");
        let mut snapshot = health_state_to_snapshot(state, body_id, arm_model, seq);

        for mut item in snapshot.components.drain(..) {
            if item.id == "body" {
                continue;
            }
            item.id = namespace_path(&item.id, &arm_id);
            item.parent_id = namespace_path(&item.parent_id, &arm_id);
            item.frame_id = namespace_frame(&item.frame_id, side);
            if item.id == arm_id {
                item.name = format!("{} Piper arm", title_case(side));
            }
            components.push(item);
        }
        for mut actuator in snapshot.actuators.drain(..) {
            actuator.component_id = namespace_path(&actuator.component_id, &arm_id);
            actuator.joint_name = namespace_frame(&actuator.joint_name, side);
            actuators.push(actuator);
        }
        for mut fault in snapshot.faults.drain(..) {
            fault.component_id = namespace_path(&fault.component_id, &arm_id);
            fault.fault_id = format!("{side}_{}", fault.fault_id);
            faults.push(fault);
        }
        for mut endpoint in snapshot.safety_endpoints.drain(..) {
            endpoint.name = format!("{side}_{}", endpoint.name);
            safety_endpoints.push(endpoint);
        }
        if let Some(safety) = snapshot.safety {
            motion_allowed &= safety.motion_allowed;
            motor_power_allowed &= safety.motor_power_allowed;
            if safety.aggregate_state == SAFETY_ESTOP {
                aggregate_state = SAFETY_ESTOP;
            } else if safety.aggregate_state == SAFETY_FAULT && aggregate_state != SAFETY_ESTOP {
                aggregate_state = SAFETY_FAULT;
            }
        }
    }

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body_id.to_string(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators,
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed,
            motor_power_allowed,
            aggregate_state,
            detail: format!("{} Piper health provider(s) aggregated", states.len()),
        }),
        safety_endpoints,
        faults,
        metrics: vec![],
    }
}

fn provider_side(provider_id: &str) -> &str {
    let normalized = provider_id.to_ascii_lowercase();
    if normalized.contains("right") {
        "right"
    } else if normalized.contains("left") {
        "left"
    } else {
        "arm"
    }
}

fn namespace_path(path: &str, arm_id: &str) -> String {
    if path == "body" || path.is_empty() {
        return path.to_string();
    }
    path.replacen("body/arm", arm_id, 1)
}

fn namespace_frame(frame: &str, side: &str) -> String {
    match frame {
        "arm_base_link" => format!("{side}_base_link"),
        "gripper_base" | "gripper" => format!("{side}_gripper_base"),
        value if value.starts_with("joint_") => {
            format!("{side}_joint{}", value.trim_start_matches("joint_"))
        }
        value => value.to_string(),
    }
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Build an empty snapshot (used when no health primitives are available).
fn empty_snapshot(body_id: &str, arm_model: &str, seq: u64) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();
    let components = vec![component(
        "body",
        "",
        KIND_BODY,
        "Piper Robot",
        "base_link",
        arm_model,
    )];

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body_id.to_string(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators: vec![],
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed: false,
            motor_power_allowed: false,
            aggregate_state: SAFETY_FAULT,
            detail: "no health primitive connected".to_string(),
        }),
        safety_endpoints: vec![],
        faults: vec![],
        metrics: vec![],
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn component(
    id: &str,
    parent_id: &str,
    kind: u32,
    name: &str,
    frame_id: &str,
    model: &str,
) -> ComponentStatus {
    ComponentStatus {
        id: id.to_string(),
        parent_id: parent_id.to_string(),
        kind,
        name: name.to_string(),
        frame_id: frame_id.to_string(),
        model: model.to_string(),
        serial: String::new(),
        health: 4, // HEALTH_UNKNOWN
        operational_state: OP_ACTIVE,
        present: true,
        online: true,
        detail: String::new(),
    }
}

fn scalar(value: f64, unit: &str) -> Scalar {
    Scalar {
        value,
        unit: unit.to_string(),
        quality: QUALITY_VALID,
    }
}

/// Parse "body/arm/joint_N/..." → Some(N-1) (0-indexed), or None.
fn parse_joint_index(name: &str) -> Option<usize> {
    let prefix = "body/arm/joint_";
    let rest = name.strip_prefix(prefix)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let idx: usize = digits.parse().ok()?;
    if !(1..=6).contains(&idx) {
        return None;
    }
    Some(idx - 1)
}

fn chrono_now_ns() -> i64 {
    // Use a simple monotonic approach; avoids pulling in chrono just for this.
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    start.elapsed().as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_joint_index_valid() {
        assert_eq!(parse_joint_index("body/arm/joint_1/motor_temp"), Some(0));
        assert_eq!(parse_joint_index("body/arm/joint_6/enabled"), Some(5));
        assert_eq!(parse_joint_index("body/arm/joint_3/error"), Some(2));
    }

    #[test]
    fn parse_joint_index_invalid() {
        assert_eq!(parse_joint_index("body/arm/joint_7/motor_temp"), None);
        assert_eq!(parse_joint_index("body/arm/joint_0/motor_temp"), None);
        assert_eq!(parse_joint_index("body/leg/joint_1/motor_temp"), None);
        assert_eq!(parse_joint_index("random_string"), None);
    }

    #[test]
    fn merge_namespaces_left_and_right_provider_components() {
        let states = HashMap::from([
            (
                "left_piper".to_string(),
                crate::pb::health::HealthState::default(),
            ),
            (
                "right_piper".to_string(),
                crate::pb::health::HealthState::default(),
            ),
        ]);

        let snapshot = merged_health_snapshot(&states, "dual_piper", "piper", 7);
        let component_ids: std::collections::HashSet<_> = snapshot
            .components
            .iter()
            .map(|component| component.id.as_str())
            .collect();

        assert!(component_ids.contains("body/left_arm/joint_1"));
        assert!(component_ids.contains("body/left_arm/gripper"));
        assert!(component_ids.contains("body/right_arm/joint_6"));
        assert!(component_ids.contains("body/right_arm/gripper"));
        assert_eq!(snapshot.actuators.len(), 14);
    }

    #[test]
    fn controller_error_becomes_an_arm_fault() {
        let state = crate::pb::health::HealthState {
            readings: vec![crate::pb::health::SensorReading {
                name: "body/arm/error".to_string(),
                current_a: 7.0,
                ..Default::default()
            }],
            ..Default::default()
        };

        let snapshot = health_state_to_snapshot(&state, "dual_piper", "piper", 1);

        assert!(snapshot.faults.iter().any(|fault| {
            fault.component_id == "body/arm"
                && fault.fault_id == "piper_controller_fault"
                && fault.vendor_code == 7
        }));
    }
}
