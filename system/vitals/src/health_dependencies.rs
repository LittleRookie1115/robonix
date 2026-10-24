// SPDX-License-Identifier: MulanPSL-2.0

use crate::component_health::{HEALTH_ERROR, HEALTH_OK, HEALTH_STALE};
use crate::pb::vitals::VitalsSnapshot;
use anyhow::{Context, Result, ensure};
use robonix_atlas::{client::AtlasClient, pb as atlas_pb};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const RETENTION: Duration = Duration::from_secs(300);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ENTRIES: usize = 2048;
const MAX_RESULTS: usize = 64;
const MAX_TRACE: usize = 16;
const MAX_REPORT_BYTES: usize = 24 * 1024;

impl VitalsSnapshot {
    fn components(&self) -> impl ExactSizeIterator<Item = (&str, u32)> {
        self.components
            .iter()
            .map(|component| (component.name.as_str(), component.health))
    }

    fn has_bodies(&self) -> bool {
        !self.bodies.is_empty()
    }

    fn body_alert(&self) -> bool {
        self.bodies
            .iter()
            .any(|body| body.state != 0 || !body.message.is_empty())
    }
}

/// Matched active plans and whether their dependency coverage is incomplete.
pub struct StopTargets {
    /// Exact Executor plan ids whose accepted nodes depend on severe faults.
    pub plan_ids: Vec<String>,
    /// Missing, stale, or truncated evidence prevents certifying full coverage.
    pub incomplete: bool,
}

#[derive(Clone, Debug, Deserialize, Hash)]
/// One recorded functional input from a consumer to a capability provider.
struct Dependency {
    /// Registered instance consuming the capability.
    consumer_id: String,
    /// Registered instance supplying the capability.
    provider_id: String,
    /// Exact input contract, used to respect component export boundaries.
    contract_id: String,
}

#[derive(Default, Deserialize)]
/// The channel portion of Atlas's inspection response.
struct Inspection {
    /// Active connection records keyed by Atlas's allocation id.
    channels: BTreeMap<String, Dependency>,
}

#[derive(Clone)]
/// Last observed provider identity and lifecycle evidence.
struct Provider {
    /// Atlas kind distinguishing primitive, service and skill instances.
    kind: i32,
    /// Last observed lifecycle state of the instance.
    state: i32,
    /// Namespace used to exclude monitoring and control connections.
    namespace: String,
    /// Monotonic observation time for bounded evidence retention.
    last_seen: Instant,
}

#[derive(Clone, Default)]
/// Component-owned provider contracts declared in Soma's body description.
struct Export {
    /// Deployment instance that implements these component capabilities.
    provider_id: String,
    /// Exact contracts owned by this component.
    contracts: BTreeSet<String>,
}

#[derive(Clone, Hash)]
/// A fault's origin and the input edge explaining one affected provider.
struct Evidence {
    /// Body component from which this impact path originates.
    component: String,
    /// Previous input edge on a transitive path, absent for the direct owner.
    predecessor: Option<Dependency>,
    /// Directly affected owner contracts; absent for a transitive consumer.
    contracts: Option<BTreeMap<String, String>>,
}

#[derive(Default)]
/// Bounded topology and dependency evidence used for health impact assessment.
pub struct HealthDependencies {
    /// Soma component paths and their exact provider bindings.
    exports: BTreeMap<String, Vec<Export>>,
    /// Current and retained provider metadata.
    providers: BTreeMap<String, Provider>,
    /// Input edges with their last observation time.
    channels: BTreeMap<(String, String, String), (Dependency, Instant)>,
    /// Edges present in the latest successful Atlas inspection.
    observed_channels: BTreeSet<(String, String, String)>,
    /// Providers present and not terminated in the latest successful query.
    present_providers: BTreeSet<String>,
    /// Fault-relevant providers retained until recovery is observed.
    pinned_providers: BTreeSet<String>,
    /// Whether the latest dependency query completed and passed validation.
    graph_verified: bool,
    /// Whether the latest Soma description supplied valid component bindings.
    topology_verified: bool,
}

impl HealthDependencies {
    pub fn invalidate_topology(&mut self) {
        self.topology_verified = false;
    }

    /// Pin fault-relevant records until health recovers, while keeping entry bounds.
    pub fn observe_health(&mut self, snapshot: &VitalsSnapshot) {
        self.pinned_providers = self
            .affected(snapshot, Instant::now())
            .0
            .into_keys()
            .collect();
    }

    /// Replace body bindings only after the complete Soma description parses.
    pub fn update_topology(&mut self, yaml: &str) -> Result<()> {
        self.topology_verified = false;
        let document: serde_yaml::Value = serde_yaml::from_str(yaml)?;
        let robot = document.get("robot").context("Soma YAML has no robot")?;
        let components = robot
            .get("components")
            .and_then(serde_yaml::Value::as_sequence)
            .context("Soma YAML has no component list")?;
        let mut exports = BTreeMap::new();
        collect_exports(components, "body", &mut exports)?;
        ensure!(
            exports.values().map(Vec::len).sum::<usize>() <= MAX_ENTRIES,
            "Soma binding count exceeds the limit"
        );
        self.exports = exports;
        self.topology_verified = true;
        Ok(())
    }

    /// Parse existing InspectAtlas data and retain edges lost with a provider.
    fn update_graph(
        &mut self,
        providers: Vec<atlas_pb::CapabilityProvider>,
        raw: &str,
        now: Instant,
    ) -> Result<()> {
        self.graph_verified = false;
        ensure!(
            raw.len() <= 2 * 1024 * 1024,
            "Atlas inspection exceeds the size limit"
        );
        let value: Value = serde_json::from_str(raw)?;
        ensure!(
            value.get("channels").is_some_and(Value::is_object),
            "Atlas channel data is unavailable"
        );
        let inspection: Inspection = serde_json::from_value(value)?;
        ensure!(
            providers.len() <= MAX_ENTRIES && inspection.channels.len() <= MAX_ENTRIES,
            "Atlas dependency data exceeds the entry limit"
        );
        let present: BTreeSet<_> = providers
            .iter()
            .filter(|provider| provider.state != atlas_pb::LifecycleState::StateTerminated as i32)
            .map(|provider| provider.id.clone())
            .collect();
        let known_consumers: BTreeMap<_, _> = self
            .providers
            .iter()
            .map(|(id, provider)| (id.as_str(), provider.namespace.as_str()))
            .chain(
                providers
                    .iter()
                    .map(|provider| (provider.id.as_str(), provider.namespace.as_str())),
            )
            .collect();
        let channels: Vec<_> = inspection
            .channels
            .into_values()
            .filter(|edge| {
                edge.contract_id != "robonix/lifecycle/driver"
                    && !is_control_path(&edge.contract_id)
                    && known_consumers
                        .get(edge.consumer_id.as_str())
                        .is_some_and(|namespace| !is_control_path(namespace))
            })
            .collect();
        let mut current = BTreeSet::new();
        for edge in &channels {
            ensure!(
                [&edge.consumer_id, &edge.provider_id, &edge.contract_id]
                    .iter()
                    .all(|value| !value.is_empty() && value.len() <= 512),
                "Atlas returned an invalid channel"
            );
            current.insert(edge_key(edge));
        }
        let mut retained_channels = self.channels.clone();
        let mut known_providers = self.providers.clone();
        retained_channels.retain(|key, (edge, last_seen)| {
            self.pinned_providers.contains(&edge.provider_id)
                || self.pinned_providers.contains(&edge.consumer_id)
                || (now.duration_since(*last_seen) <= RETENTION
                    && (current.contains(key)
                        || !present.contains(&edge.provider_id)
                        || !present.contains(&edge.consumer_id)))
        });
        for edge in channels {
            retained_channels.insert(edge_key(&edge), (edge, now));
        }
        known_providers.retain(|id, provider| {
            now.duration_since(provider.last_seen) <= RETENTION
                || self.pinned_providers.contains(id)
        });
        for provider in providers {
            known_providers.insert(
                provider.id,
                Provider {
                    kind: provider.kind,
                    state: provider.state,
                    namespace: provider.namespace,
                    last_seen: now,
                },
            );
        }
        ensure!(
            retained_channels.len() <= MAX_ENTRIES && known_providers.len() <= MAX_ENTRIES,
            "Retained Atlas dependency data exceeds the entry limit"
        );
        self.channels = retained_channels;
        self.providers = known_providers;
        self.observed_channels = current;
        self.present_providers = present;
        self.graph_verified = true;
        Ok(())
    }

    /// Resolve derived signals through their declared component or nearest owning parent.
    fn owners(&self, signal: &str) -> Option<(String, &[Export])> {
        let mut component = signal;
        while !self.exports.contains_key(component) {
            component = component.rsplit_once('/')?.0;
        }
        let original = component.to_string();
        loop {
            let exports = self.exports.get(component)?;
            if !exports.is_empty() {
                return Some((original, exports));
            }
            component = component.rsplit_once('/')?.0;
        }
    }

    /// Follow reverse consumer edges once per provider, keeping one causal path.
    fn affected(
        &self,
        snapshot: &VitalsSnapshot,
        now: Instant,
    ) -> (BTreeMap<String, Evidence>, Vec<String>) {
        self.trace(snapshot, now, false)
    }

    /// Share dependency traversal while excluding warnings from automatic stops.
    fn trace(
        &self,
        snapshot: &VitalsSnapshot,
        now: Instant,
        severe_only: bool,
    ) -> (BTreeMap<String, Evidence>, Vec<String>) {
        let mut affected = BTreeMap::new();
        let mut unmapped = BTreeSet::new();
        let mut queue = VecDeque::new();
        let mut consumers: BTreeMap<&str, Vec<&Dependency>> = BTreeMap::new();
        for (edge, last_seen) in self.channels.values() {
            if now.duration_since(*last_seen) <= RETENTION
                || self.pinned_providers.contains(&edge.provider_id)
                || self.pinned_providers.contains(&edge.consumer_id)
            {
                consumers.entry(&edge.provider_id).or_default().push(edge);
            }
        }
        for (name, _) in snapshot
            .components()
            .filter(|(_, health)| {
                if severe_only {
                    matches!(*health, HEALTH_ERROR | HEALTH_STALE)
                } else {
                    *health != HEALTH_OK
                }
            })
            .take(MAX_ENTRIES)
        {
            let Some((component, exports)) = self.owners(name) else {
                unmapped.insert(name.to_string());
                continue;
            };
            for export in exports {
                if export.contracts.is_empty() {
                    unmapped.insert(name.to_string());
                    continue;
                }
                let evidence = affected
                    .entry(export.provider_id.clone())
                    .or_insert_with(|| {
                        queue.push_back(export.provider_id.clone());
                        Evidence {
                            component: component.clone(),
                            predecessor: None,
                            contracts: Some(BTreeMap::new()),
                        }
                    });
                evidence.contracts.as_mut().unwrap().extend(
                    export
                        .contracts
                        .iter()
                        .map(|contract| (contract.clone(), component.clone())),
                );
            }
        }
        while let Some(provider_id) = queue.pop_front() {
            let evidence = affected[&provider_id].clone();
            for edge in consumers.get(provider_id.as_str()).into_iter().flatten() {
                if evidence
                    .contracts
                    .as_ref()
                    .is_some_and(|contracts| !contracts.contains_key(&edge.contract_id))
                {
                    continue;
                }
                let component = evidence
                    .contracts
                    .as_ref()
                    .and_then(|contracts| contracts.get(&edge.contract_id))
                    .unwrap_or(&evidence.component)
                    .clone();
                if affected.contains_key(&edge.consumer_id) {
                    let replace_predecessor =
                        !would_cycle(&affected, &edge.consumer_id, &edge.provider_id);
                    let existing = affected.get_mut(&edge.consumer_id).unwrap();
                    if existing.contracts.take().is_some() {
                        if replace_predecessor {
                            existing.component = component;
                            existing.predecessor = Some((*edge).clone());
                        }
                        queue.push_back(edge.consumer_id.clone());
                    }
                    continue;
                }
                affected.insert(
                    edge.consumer_id.clone(),
                    Evidence {
                        component,
                        predecessor: Some((*edge).clone()),
                        contracts: None,
                    },
                );
                queue.push_back(edge.consumer_id.clone());
            }
        }
        (affected, unmapped.into_iter().collect())
    }

    /// Keep model interrupts stable when only timestamps or alarm wording change.
    pub fn alert_key(&self, snapshot: &VitalsSnapshot) -> String {
        let (affected, unmapped) = self.affected(snapshot, Instant::now());
        let mut hasher = DefaultHasher::new();
        (self.graph_verified, self.topology_verified).hash(&mut hasher);
        for binding in &affected {
            binding.hash(&mut hasher);
        }
        unmapped.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Automatic control uses all bounded matches, independently of prompt clipping.
    pub fn stop_targets(&self, snapshot: &VitalsSnapshot, plans: &str) -> StopTargets {
        let (affected, unmapped) = self.trace(snapshot, Instant::now(), true);
        let (candidates, status, truncated) = match_plans(Some(plans), &affected, MAX_ENTRIES);
        StopTargets {
            plan_ids: candidates
                .into_iter()
                .filter_map(|candidate| candidate["plan_id"].as_str().map(str::to_owned))
                .collect(),
            incomplete: !self.graph_verified
                || !self.topology_verified
                || affected
                    .keys()
                    .any(|id| !self.present_providers.contains(id))
                || !unmapped.is_empty()
                || status != "verified"
                || truncated
                || snapshot.components().len() > MAX_ENTRIES,
        }
    }

    /// Match active executable steps to known owner contracts and transitive consumers.
    pub fn report(&self, snapshot: Option<&VitalsSnapshot>, plans: Option<&str>) -> Value {
        let Some(snapshot) = snapshot else {
            return json!({"status":"unavailable", "reason":"Vitals has no verified hardware snapshot"});
        };
        if snapshot.components().len() == 0 && !snapshot.has_bodies() {
            return json!({"status":"unavailable", "reason":"Vitals has not reported hardware components"});
        }
        let now = Instant::now();
        let (affected, unmapped) = self.affected(snapshot, now);
        let unobserved_bindings: Vec<_> = affected
            .iter()
            .filter(|(provider_id, evidence)| {
                evidence.predecessor.is_none()
                    && !self.providers.get(*provider_id).is_some_and(|provider| {
                        now.duration_since(provider.last_seen) <= RETENTION
                            || self.pinned_providers.contains(*provider_id)
                    })
            })
            .map(|(provider_id, _)| provider_id.clone())
            .collect();
        let mut dependent = Vec::new();
        let mut cached_evidence = false;
        let mut path_truncated = false;
        for (provider_id, evidence) in affected.iter().take(MAX_RESULTS) {
            let provider = self.providers.get(provider_id).filter(|provider| {
                now.duration_since(provider.last_seen) <= RETENTION
                    || self.pinned_providers.contains(provider_id)
            });
            let (trace, truncated) = evidence_path(&affected, provider_id);
            let cached = !self.graph_verified
                || !self.present_providers.contains(provider_id)
                || trace
                    .iter()
                    .any(|edge| !self.observed_channels.contains(&edge_key(edge)));
            cached_evidence |= cached;
            path_truncated |= truncated;
            let path: Vec<_> = trace.iter().map(|edge| json!({
                "consumer_id": edge.consumer_id, "provider_id": edge.provider_id, "contract_id": edge.contract_id,
            })).collect();
            dependent.push(json!({
                "provider_id": provider_id,
                "kind": provider.map(|provider| kind_label(provider.kind)).unwrap_or("unknown"),
                "lifecycle_state": provider.and_then(|provider| atlas_pb::LifecycleState::try_from(provider.state).ok()).map(|state| state.as_str_name()),
                "component": evidence.component,
                "evidence": if cached { "last_known" } else { "current" },
                "path": path,
                "component_contracts": evidence.contracts,
            }));
        }
        let (candidate_plans, plan_status, plan_truncated) =
            match_plans(plans, &affected, MAX_RESULTS);
        let unscoped_body_alert = affected.is_empty() && snapshot.body_alert();
        bounded_report(json!({
            "status": if self.graph_verified && self.topology_verified && !cached_evidence
                && unmapped.is_empty() && unobserved_bindings.is_empty() && !unscoped_body_alert
                { "verified" } else { "last_known_or_incomplete" },
            "dependent_providers": dependent,
            "unmapped_components": unmapped,
            "unobserved_provider_bindings": unobserved_bindings,
            "unscoped_body_alert": unscoped_body_alert,
            "candidate_plans": candidate_plans,
            "executor_status": plan_status,
            "truncated": affected.len() > MAX_RESULTS || plan_truncated || path_truncated
                || snapshot.components().len() > MAX_ENTRIES,
        }))
    }

    pub fn snapshot_report(&self, snapshot: &VitalsSnapshot, plans: Option<&str>) -> String {
        let mut report = self.report(Some(snapshot), plans);
        let blocked = self.trace(snapshot, Instant::now(), true).0;
        report["blocked_calls"] = json!(
            blocked
                .iter()
                .flat_map(|(provider_id, evidence)| {
                    let mut owners: BTreeMap<String, Option<BTreeMap<String, String>>> = BTreeMap::new();
                    if let Some(contracts) = &evidence.contracts {
                        for (contract, component) in contracts {
                            owners.entry(component.clone()).or_insert_with(|| Some(BTreeMap::new()))
                                .as_mut().unwrap().insert(contract.clone(), component.clone());
                        }
                    } else {
                        owners.insert(evidence.component.clone(), None);
                    }
                    owners.into_iter().map(|(component, contracts)| json!({
                        "provider_id":provider_id, "component":component, "component_contracts":contracts
                    })).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        );
        report["alert_key"] = json!(self.alert_key(snapshot));
        report["blocked_calls_truncated"] = json!(
            snapshot
                .components()
                .filter(|(_, health)| matches!(*health, HEALTH_ERROR | HEALTH_STALE))
                .count()
                > MAX_ENTRIES
        );
        let encoded = report.to_string();
        if encoded.len() <= 2 * 1024 * 1024 {
            encoded
        } else {
            json!({"status":"unavailable","truncated":true,"reason":"dependency report exceeds the size limit"}).to_string()
        }
    }
}

/// Fetch graph data off the cache lock so slow Atlas calls cannot delay alerts.
pub async fn refresh_graph(
    atlas: &AtlasClient,
    cache: &Arc<RwLock<HealthDependencies>>,
) -> Result<()> {
    let mut wire = atlas.inner();
    let mut discovery = atlas.clone();
    let result = tokio::time::timeout(RPC_TIMEOUT, async {
        tokio::try_join!(
            async {
                wire.inspect_atlas(atlas_pb::InspectAtlasRequest {})
                    .await
                    .map(|response| response.into_inner().json)
                    .map_err(anyhow::Error::from)
            },
            discovery.query(
                atlas_pb::Kind::Unspecified,
                "",
                "",
                "",
                atlas_pb::Transport::Unspecified
            ),
        )
    })
    .await
    .context("Atlas dependency query timed out")
    .and_then(|result| result);
    let mut state = cache.write().await;
    match result {
        Ok((raw, providers)) => state.update_graph(providers, &raw, Instant::now()),
        Err(error) => {
            state.graph_verified = false;
            Err(error)
        }
    }
}

fn edge_key(edge: &Dependency) -> (String, String, String) {
    (
        edge.consumer_id.clone(),
        edge.provider_id.clone(),
        edge.contract_id.clone(),
    )
}

/// Orchestration observes business calls without making its own independent jobs depend on them.
fn is_control_path(path: &str) -> bool {
    ["atlas", "executor", "pilot", "vitals", "soma"]
        .into_iter()
        .any(|name| {
            path.strip_prefix("robonix/system/")
                .and_then(|suffix| suffix.strip_prefix(name))
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'))
        })
}

/// Keep one predecessor per provider instead of cloning every complete path.
fn evidence_path<'a>(
    affected: &'a BTreeMap<String, Evidence>,
    provider_id: &str,
) -> (Vec<&'a Dependency>, bool) {
    let mut current = provider_id;
    let mut visited = BTreeSet::new();
    let mut path = Vec::new();
    while let Some(edge) = affected
        .get(current)
        .and_then(|evidence| evidence.predecessor.as_ref())
    {
        if path.len() == MAX_TRACE || !visited.insert(current) {
            path.reverse();
            return (path, true);
        }
        path.push(edge);
        current = &edge.provider_id;
    }
    path.reverse();
    (path, false)
}

/// Avoid replacing a seed's explanation with a circular predecessor chain.
fn would_cycle(
    affected: &BTreeMap<String, Evidence>,
    consumer_id: &str,
    provider_id: &str,
) -> bool {
    let mut current = provider_id;
    for _ in 0..=affected.len() {
        if current == consumer_id {
            return true;
        }
        match affected
            .get(current)
            .and_then(|evidence| evidence.predecessor.as_ref())
        {
            Some(edge) => current = &edge.provider_id,
            None => return false,
        }
    }
    true
}

/// Limit model-facing evidence and explicitly mark omitted rows or steps.
fn bounded_report(mut report: Value) -> Value {
    while report.to_string().len() > MAX_REPORT_BYTES {
        report["truncated"] = json!(true);
        let removed = [
            "dependent_providers",
            "unobserved_provider_bindings",
            "unmapped_components",
        ]
        .into_iter()
        .any(|key| {
            report[key]
                .as_array_mut()
                .is_some_and(|items| items.pop().is_some())
        });
        if removed {
            continue;
        }
        let candidates = report["candidate_plans"].as_array_mut().unwrap();
        if candidates.iter_mut().any(|candidate| {
            candidate["steps"]
                .as_array_mut()
                .is_some_and(|steps| steps.pop().is_some())
        }) {
            continue;
        }
        if candidates.pop().is_none() {
            break;
        }
    }
    report
}

/// Parse declared component paths without guessing deployment-specific provider aliases.
fn collect_exports(
    components: &[serde_yaml::Value],
    parent: &str,
    result: &mut BTreeMap<String, Vec<Export>>,
) -> Result<()> {
    ensure!(
        parent.matches('/').count() < 32,
        "Soma component nesting exceeds the limit"
    );
    for component in components {
        let id = component
            .get("id")
            .and_then(serde_yaml::Value::as_str)
            .context("Soma component has no id")?;
        ensure!(
            !id.is_empty() && id.len() <= 512,
            "Soma component id is invalid"
        );
        let path = if id.starts_with("body/") {
            id.to_string()
        } else {
            format!("{parent}/{id}")
        };
        ensure!(
            result.len() < MAX_ENTRIES,
            "Soma component count exceeds the limit"
        );
        let mut exports = Vec::new();
        if let Some(rows) = component
            .get("exports")
            .and_then(serde_yaml::Value::as_sequence)
        {
            for row in rows {
                let provider_id = row
                    .get("provider_id")
                    .and_then(serde_yaml::Value::as_str)
                    .context("Soma export has no provider id")?;
                ensure!(
                    !provider_id.is_empty() && provider_id.len() <= 512,
                    "Soma provider id is invalid"
                );
                let contracts = row
                    .get("capabilities")
                    .and_then(serde_yaml::Value::as_sequence)
                    .into_iter()
                    .flatten()
                    .filter_map(|capability| {
                        capability.get("path").and_then(serde_yaml::Value::as_str)
                    })
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>();
                ensure!(
                    contracts.len() <= MAX_RESULTS
                        && contracts
                            .iter()
                            .all(|contract| !contract.is_empty() && contract.len() <= 512),
                    "Soma capability bindings exceed the limit"
                );
                exports.push(Export {
                    provider_id: provider_id.into(),
                    contracts,
                });
            }
        }
        ensure!(
            result.insert(path.clone(), exports).is_none(),
            "Soma component path is duplicated"
        );
        if let Some(children) = component
            .get("components")
            .and_then(serde_yaml::Value::as_sequence)
        {
            collect_exports(children, &path, result)?;
        }
    }
    Ok(())
}

/// Use Atlas kinds rather than deriving component roles from provider names.
fn kind_label(kind: i32) -> &'static str {
    match atlas_pb::Kind::try_from(kind) {
        Ok(atlas_pb::Kind::Primitive) => "primitive",
        Ok(atlas_pb::Kind::Service) => "service",
        Ok(atlas_pb::Kind::Skill) => "skill",
        _ => "unknown",
    }
}

/// Omitted or malformed executable steps remain unverified rather than safe.
fn match_plans(
    plans: Option<&str>,
    affected: &BTreeMap<String, Evidence>,
    result_limit: usize,
) -> (Vec<Value>, &'static str, bool) {
    let parsed = plans
        .filter(|raw| raw.len() <= 2 * 1024 * 1024)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    let Some(plans) = parsed
        .as_ref()
        .and_then(|value| value.get("plans"))
        .and_then(Value::as_array)
    else {
        return (Vec::new(), "unavailable", false);
    };
    let mut candidates = Vec::new();
    let mut incomplete = plans.len() > MAX_ENTRIES;
    let mut truncated = plans.len() > MAX_ENTRIES;
    for plan in plans.iter().take(MAX_ENTRIES) {
        let Some(plan_id) = plan.get("plan_id").and_then(Value::as_str) else {
            incomplete = true;
            continue;
        };
        let Some(operations) = plan.get("ops").and_then(Value::as_array) else {
            incomplete = true;
            continue;
        };
        let steps: Vec<_> = operations
            .iter()
            .filter_map(|operation| {
                let provider_id = operation
                    .get("provider_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let contract_id = operation
                    .get("contract_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if provider_id.is_empty() || contract_id.is_empty() {
                    if operation.get("kind").and_then(Value::as_str) == Some("do") {
                        incomplete = true;
                    }
                    return None;
                }
                let Some(state) = operation.get("state").and_then(Value::as_str) else {
                    incomplete = true;
                    return None;
                };
                if !matches!(state, "pending" | "running" | "paused" | "verifying") {
                    if !matches!(
                        state,
                        "succeeded" | "failed" | "canceled" | "cancelled" | "timeout" | "skipped"
                    ) {
                        incomplete = true;
                    }
                    return None;
                }
                let evidence = affected.get(provider_id)?;
                if evidence
                    .contracts
                    .as_ref()
                    .is_some_and(|contracts| !contracts.contains_key(contract_id))
                {
                    return None;
                }
                let component = evidence
                    .contracts
                    .as_ref()
                    .and_then(|contracts| contracts.get(contract_id))
                    .unwrap_or(&evidence.component);
                Some(
                    json!({"op_id": operation.get("op_id"), "provider_id": provider_id,
                        "contract_id": contract_id, "component": component}),
                )
            })
            .collect();
        if !steps.is_empty() {
            truncated |= steps.len() > MAX_RESULTS;
            candidates.push(json!({"plan_id": plan_id, "steps": steps.into_iter().take(MAX_RESULTS).collect::<Vec<_>>()}));
        }
    }
    candidates.sort_by(|left, right| left["plan_id"].as_str().cmp(&right["plan_id"].as_str()));
    truncated |= candidates.len() > result_limit;
    candidates.truncate(result_limit);
    (
        candidates,
        if incomplete { "partial" } else { "verified" },
        truncated,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::vitals::{ComponentHealth, VitalsSnapshot};

    const LIDAR: &str = "robonix/primitive/lidar/lidar";
    const CAMERA: &str = "robonix/primitive/camera/rgb";
    const MAP: &str = "robonix/service/map/occupancy_grid";
    const NAV: &str = "robonix/service/navigation/navigate";
    const YAML: &str = "
robot:
  components:
    - id: laser
      exports:
        - provider_id: laser
          capabilities:
            - path: robonix/primitive/lidar/lidar
    - id: camera
      exports:
        - provider_id: camera
          capabilities:
            - path: robonix/primitive/camera/rgb
";

    /// Build typed records independently of Atlas's debug provider JSON.
    fn providers() -> Vec<atlas_pb::CapabilityProvider> {
        [
            ("laser", atlas_pb::Kind::Primitive),
            ("camera", atlas_pb::Kind::Primitive),
            ("mapping", atlas_pb::Kind::Service),
            ("navigation", atlas_pb::Kind::Service),
            ("explore", atlas_pb::Kind::Skill),
            ("memory", atlas_pb::Kind::Service),
        ]
        .into_iter()
        .map(|(id, kind)| atlas_pb::CapabilityProvider {
            id: id.into(),
            kind: kind as i32,
            state: atlas_pb::LifecycleState::StateActive as i32,
            ..Default::default()
        })
        .collect()
    }

    /// Give the laser a two-hop consumer chain and the camera an unrelated one.
    fn channels() -> Value {
        json!({"channels": {
            "one": {"consumer_id":"mapping","provider_id":"laser","contract_id":LIDAR},
            "two": {"consumer_id":"navigation","provider_id":"mapping","contract_id":MAP},
            "three": {"consumer_id":"explore","provider_id":"navigation","contract_id":NAV},
            "four": {"consumer_id":"viewer","provider_id":"camera","contract_id":CAMERA}
        }})
    }

    /// Represent one failed device alongside a still healthy camera.
    fn snapshot() -> VitalsSnapshot {
        VitalsSnapshot {
            components: vec![
                ComponentHealth {
                    name: "body/laser".into(),
                    health: 2,
                    ..Default::default()
                },
                ComponentHealth {
                    name: "body/camera".into(),
                    health: 0,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    /// The active table includes unrelated work and an already completed laser step.
    fn plans() -> String {
        json!({"plans":[
            {"plan_id":"exploration","ops":[{"provider_id":"explore","contract_id":"robonix/skill/explore/explore","op_id":"start","state":"running"}]},
            {"plan_id":"memory","ops":[{"provider_id":"memory","contract_id":"robonix/service/memory/search","op_id":"lookup","state":"running"}]},
            {"plan_id":"camera","ops":[{"provider_id":"camera","contract_id":CAMERA,"op_id":"observe","state":"pending"}]},
            {"plan_id":"completed-laser","ops":[{"provider_id":"laser","contract_id":LIDAR,"op_id":"scan","state":"succeeded"}]}
        ]}).to_string()
    }

    #[test]
    /// Reverse reachability yields causal services and only the affected active plan.
    fn traces_services_skills_and_preserves_unrelated_plans() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let report = state.report(Some(&snapshot()), Some(&plans()));
        assert_eq!(report["candidate_plans"][0]["plan_id"], "exploration");
        assert_eq!(report["candidate_plans"].as_array().unwrap().len(), 1);
        assert_eq!(report["dependent_providers"].as_array().unwrap().len(), 4);
        assert!(
            report["dependent_providers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|provider| provider["provider_id"] == "explore"
                    && provider["path"].as_array().unwrap().len() == 3)
        );
    }

    #[test]
    fn streamed_nodes_become_stop_targets_before_the_plan_is_sealed() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let mut active = json!({"plans":[{"plan_id":"streaming-plan","cancelled":false,"ops":[
            {"op_id":"root","kind":"sequence","state":"running","provider_id":"","contract_id":""}
        ]}]});
        assert!(
            state
                .stop_targets(&snapshot(), &active.to_string())
                .plan_ids
                .is_empty()
        );
        active["plans"][0]["ops"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "op_id":"later-do","kind":"do","state":"pending",
                "provider_id":"explore","contract_id":"robonix/skill/explore/explore"
            }));
        assert_eq!(
            state
                .stop_targets(&snapshot(), &active.to_string())
                .plan_ids,
            ["streaming-plan"]
        );
        active["plans"][0]["cancelled"] = json!(true);
        assert_eq!(
            state
                .stop_targets(&snapshot(), &active.to_string())
                .plan_ids,
            ["streaming-plan"]
        );
    }

    #[test]
    /// A disappeared provider retains its last observed edge for fault-time tracing.
    fn provider_eviction_keeps_fault_evidence_but_closed_live_channels_do_not() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        let now = Instant::now();
        state
            .update_graph(providers(), &channels().to_string(), now)
            .unwrap();
        let without_laser: Vec<_> = providers()
            .into_iter()
            .filter(|provider| provider.id != "laser")
            .collect();
        let mut current = channels();
        current["channels"].as_object_mut().unwrap().remove("one");
        state
            .update_graph(
                without_laser,
                &current.to_string(),
                now + Duration::from_secs(1),
            )
            .unwrap();
        let report = state.report(Some(&snapshot()), Some(&plans()));
        assert_eq!(report["candidate_plans"][0]["plan_id"], "exploration");
        assert!(
            report["dependent_providers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|provider| provider["provider_id"] == "mapping"
                    && provider["evidence"] == "last_known")
        );
        state
            .update_graph(
                providers(),
                &current.to_string(),
                now + Duration::from_secs(2),
            )
            .unwrap();
        assert!(
            state.report(Some(&snapshot()), Some(&plans()))["candidate_plans"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    /// Missing declarations remain explicit and never fall back to fuzzy provider names.
    fn unmapped_component_and_missing_executor_data_are_not_all_clear() {
        let state = HealthDependencies::default();
        let report = state.report(Some(&snapshot()), None);
        assert_eq!(report["unmapped_components"], json!(["body/laser"]));
        assert_eq!(report["executor_status"], "unavailable");
        assert_eq!(report["status"], "last_known_or_incomplete");
    }

    #[test]
    /// Malformed inspection does not replace previously validated dependencies.
    fn rejects_missing_channel_schema_without_losing_previous_graph() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        assert!(
            state
                .update_graph(providers(), "{}", Instant::now())
                .is_err()
        );
        assert_eq!(
            state.report(Some(&snapshot()), Some(&plans()))["candidate_plans"][0]["plan_id"],
            "exploration"
        );
    }

    #[test]
    /// Independent signal wording changes do not create repeated model interrupts.
    fn stable_alert_key_ignores_detail_and_timestamp() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let original = snapshot();
        let mut changed = original.clone();
        changed.ts_ns = 99;
        changed.components[0].detail = "updated alarm reading".into();
        assert_eq!(state.alert_key(&original), state.alert_key(&changed));
    }

    #[test]
    /// Atlas retains terminated records after removing their incoming channels.
    fn heartbeat_termination_retains_missing_dependency_edges() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        let now = Instant::now();
        state
            .update_graph(providers(), &channels().to_string(), now)
            .unwrap();
        let mut terminated = providers();
        terminated[0].state = atlas_pb::LifecycleState::StateTerminated as i32;
        let mut current = channels();
        current["channels"].as_object_mut().unwrap().remove("one");
        state
            .update_graph(
                terminated,
                &current.to_string(),
                now + Duration::from_secs(1),
            )
            .unwrap();
        let report = state.report(Some(&snapshot()), Some(&plans()));
        assert_eq!(report["candidate_plans"][0]["plan_id"], "exploration");
        assert_eq!(report["status"], "last_known_or_incomplete");
        let targets = state.stop_targets(&snapshot(), &plans());
        assert_eq!(targets.plan_ids, ["exploration"]);
        assert!(targets.incomplete);
    }

    #[test]
    /// Different devices served by one provider keep their direct contract scopes.
    fn shared_provider_does_not_imply_every_device_contract_failed() {
        let mut state = HealthDependencies::default();
        state
            .update_topology(&YAML.replace("provider_id: laser", "provider_id: camera"))
            .unwrap();
        let mut current = channels();
        current["channels"]["one"]["provider_id"] = json!("camera");
        state
            .update_graph(providers(), &current.to_string(), Instant::now())
            .unwrap();
        let report = state.report(Some(&snapshot()), Some(&plans()));
        assert_eq!(report["candidate_plans"].as_array().unwrap().len(), 1);
        assert_eq!(report["candidate_plans"][0]["plan_id"], "exploration");
        assert!(
            !report["dependent_providers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|provider| provider["provider_id"] == "viewer")
        );
    }

    #[test]
    /// Incoming failed inputs widen a seeded consumer once, without cycling forever.
    fn seeded_consumer_widens_and_cycles_keep_bounded_evidence() {
        let mut state = HealthDependencies::default();
        state
            .update_topology(&YAML.replace("provider_id: camera", "provider_id: mapping"))
            .unwrap();
        let mut current = channels();
        current["channels"]["cycle"] =
            json!({"consumer_id":"laser","provider_id":"navigation","contract_id":NAV});
        state
            .update_graph(providers(), &current.to_string(), Instant::now())
            .unwrap();
        let mut failed = snapshot();
        failed.components[1].health = 2;
        let report = state.report(Some(&failed), Some(&plans()));
        assert_eq!(report["candidate_plans"][0]["plan_id"], "exploration");
        let mapping = report["dependent_providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|provider| provider["provider_id"] == "mapping")
            .unwrap();
        assert_eq!(mapping["component"], "body/laser");
        assert_eq!(mapping["path"][0]["contract_id"], LIDAR);
    }

    #[test]
    /// Ongoing faults pin lost bindings until a positive recovery releases them.
    fn persistent_fault_keeps_bounded_records_until_recovery() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        let now = Instant::now();
        state
            .update_graph(providers(), &channels().to_string(), now)
            .unwrap();
        state.observe_health(&snapshot());
        let mut current = channels();
        current["channels"].as_object_mut().unwrap().remove("one");
        let remaining = providers()
            .into_iter()
            .filter(|provider| provider.id != "laser")
            .collect::<Vec<_>>();
        state
            .update_graph(
                remaining.clone(),
                &current.to_string(),
                now + RETENTION + Duration::from_secs(1),
            )
            .unwrap();
        assert!(
            state
                .affected(&snapshot(), now + RETENTION + Duration::from_secs(1))
                .0
                .contains_key("explore")
        );
        let mut recovered = snapshot();
        recovered
            .components
            .iter_mut()
            .for_each(|component| component.health = 0);
        state.observe_health(&recovered);
        state
            .update_graph(
                remaining,
                &current.to_string(),
                now + RETENTION + Duration::from_secs(2),
            )
            .unwrap();
        assert!(
            !state
                .affected(&snapshot(), now + RETENTION + Duration::from_secs(2))
                .0
                .contains_key("explore")
        );
    }

    #[test]
    /// A consumer disconnecting its broken input cannot erase an ongoing stop scope.
    fn fault_scope_survives_closed_channels_until_recovery() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        state.observe_health(&snapshot());
        state
            .update_graph(providers(), r#"{"channels":{}}"#, Instant::now())
            .unwrap();
        assert_eq!(
            state.stop_targets(&snapshot(), &plans()).plan_ids,
            ["exploration"]
        );
        let mut healthy = snapshot();
        healthy.components[0].health = 0;
        state.observe_health(&healthy);
        state
            .update_graph(providers(), r#"{"channels":{}}"#, Instant::now())
            .unwrap();
        let report: Value =
            serde_json::from_str(&state.snapshot_report(&snapshot(), None)).unwrap();
        assert!(
            !report["blocked_calls"]
                .as_array()
                .unwrap()
                .iter()
                .any(|provider| provider["provider_id"] == "explore")
        );
    }

    #[test]
    /// Descendant controls inherit a declared actuator owner without name heuristics.
    fn derived_joint_signal_inherits_the_parent_binding() {
        let mut state = HealthDependencies::default();
        state
            .update_topology(
                "
robot:
  components:
    - id: base
      exports:
        - provider_id: drive
          capabilities: [{path: robonix/primitive/chassis/move}]
      components: [{id: wheel, exports: []}]
",
            )
            .unwrap();
        let failed = VitalsSnapshot {
            components: vec![ComponentHealth {
                name: "body/base/wheel/communication_ok".into(),
                health: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        let (affected, unmapped) = state.affected(&failed, Instant::now());
        assert_eq!(affected["drive"].component, "body/base/wheel");
        assert!(unmapped.is_empty());
    }

    #[test]
    /// A valid active-table envelope does not certify malformed or opaque plan steps.
    fn malformed_plan_steps_are_partial_and_large_reports_are_bounded() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let incomplete = json!({"plans":[{"plan_id":"opaque"}]}).to_string();
        assert_eq!(
            state.report(Some(&snapshot()), Some(&incomplete))["executor_status"],
            "partial"
        );
        let large = json!({"plans": (0..MAX_RESULTS).map(|index| json!({
            "plan_id": format!("candidate-{index}-{}", "x".repeat(256)),
            "ops": (0..8).map(|step| json!({"provider_id":"explore",
                "contract_id":"robonix/skill/explore/explore", "op_id":format!("{step}-{}", "y".repeat(256)),
                "state":"running"})).collect::<Vec<_>>()
        })).collect::<Vec<_>>()}).to_string();
        let report = state.report(Some(&snapshot()), Some(&large));
        assert_eq!(report["truncated"], true);
        assert!(report.to_string().len() <= MAX_REPORT_BYTES);
    }

    #[test]
    /// Lifecycle bookkeeping and non-provider CLI callers are not functional inputs.
    fn lifecycle_and_unregistered_consumers_do_not_expand_fault_scope() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        let mut current = channels();
        current["channels"]["driver"] = json!({"consumer_id":"memory","provider_id":"mapping",
            "contract_id":"robonix/lifecycle/driver"});
        current["channels"]["cli"] = json!({"consumer_id":"rbnx-cli/deploy","provider_id":"mapping",
            "contract_id":MAP});
        current["channels"]["monitor"] = json!({"consumer_id":"memory","provider_id":"explore",
            "contract_id":"robonix/system/executor/list_active_plans"});
        state
            .update_graph(providers(), &current.to_string(), Instant::now())
            .unwrap();
        let affected = state.affected(&snapshot(), Instant::now()).0;
        assert!(!affected.contains_key("memory"));
        assert!(!affected.contains_key("rbnx-cli/deploy"));
        assert!(affected.contains_key("explore"));
    }

    #[test]
    /// Warnings and unknown startup samples do not request automatic stops.
    fn automatic_stop_requires_error_or_stale() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let mut health = snapshot();
        for severity in [0, 1, 4] {
            health.components[0].health = severity;
            assert!(state.stop_targets(&health, &plans()).plan_ids.is_empty());
        }
        for severity in [2, 3] {
            health.components[0].health = severity;
            assert_eq!(
                state.stop_targets(&health, &plans()).plan_ids,
                ["exploration"]
            );
            let report: Value =
                serde_json::from_str(&state.snapshot_report(&health, None)).unwrap();
            let blocked = report["blocked_calls"].as_array().unwrap();
            assert!(
                blocked
                    .iter()
                    .any(|provider| provider["provider_id"] == "explore")
            );
            assert!(
                !blocked
                    .iter()
                    .any(|provider| provider["provider_id"] == "memory")
            );
        }
    }

    #[test]
    fn severe_signals_are_selected_before_the_entry_limit() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let mut health = VitalsSnapshot {
            components: (0..MAX_ENTRIES)
                .map(|index| ComponentHealth {
                    name: format!("body/healthy_{index}"),
                    health: HEALTH_OK,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        health.components.push(snapshot().components[0].clone());
        assert_eq!(
            state.stop_targets(&health, &plans()).plan_ids,
            ["exploration"]
        );
    }

    #[test]
    /// Polling a failed job does not make unrelated Executor builtins dependent on it.
    fn executor_monitoring_does_not_cancel_independent_builtins() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        let mut registered = providers();
        registered.push(atlas_pb::CapabilityProvider {
            id: "executor-instance".into(),
            namespace: "robonix/system/executor".into(),
            ..Default::default()
        });
        let mut current = channels();
        current["channels"]["poll"] = json!({"consumer_id":"executor-instance", "provider_id":"explore",
            "contract_id":"robonix/skill/explore/explore/status"});
        current["channels"]["scene"] = json!({"consumer_id":"memory", "provider_id":"mapping",
            "contract_id":"robonix/system/scene/get_scene_graph"});
        state
            .update_graph(registered, &current.to_string(), Instant::now())
            .unwrap();
        let active = json!({"plans": [
            {"plan_id":"builtin", "ops":[{"provider_id":"executor-instance",
                "contract_id":"robonix/system/executor/builtin/read_file", "state":"running"}]},
            {"plan_id":"scene-consumer", "ops":[{"provider_id":"memory",
                "contract_id":"robonix/service/memory/search", "state":"running"}]}
        ]})
        .to_string();
        assert_eq!(
            state.stop_targets(&snapshot(), &active).plan_ids,
            ["scene-consumer"]
        );
    }

    #[test]
    /// A prompt's row budget must not leave later affected plans running.
    fn stop_selection_is_not_clipped_to_prompt_limit() {
        let mut state = HealthDependencies::default();
        state.update_topology(YAML).unwrap();
        state
            .update_graph(providers(), &channels().to_string(), Instant::now())
            .unwrap();
        let many = json!({"plans": (0..100).map(|index| json!({
            "plan_id": format!("affected-{index}"),
            "ops": [{"provider_id":"explore", "contract_id":"robonix/skill/explore/explore", "state":"running"}]
        })).collect::<Vec<_>>()}).to_string();
        assert_eq!(state.stop_targets(&snapshot(), &many).plan_ids.len(), 100);
        assert_eq!(
            state.report(Some(&snapshot()), Some(&many))["candidate_plans"]
                .as_array()
                .unwrap()
                .len(),
            MAX_RESULTS
        );
    }
}
