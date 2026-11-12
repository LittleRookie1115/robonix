// SPDX-License-Identifier: MulanPSL-2.0
//! How long capability calls take, learned from the calls Pilot has seen and
//! kept across restarts, so that speculation can tell how long a tree will run.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::time::Duration;

/// Recent durations kept per capability; older ones are dropped.
const KEPT: usize = 64;

/// Recent navigations kept for the line that times one by its distance.
const MOVES_KEPT: usize = 256;

/// The calls Pilot has seen, as [`Experience::estimate`] reads them.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Experience {
    /// Seconds each recent successful call of a capability took, newest last,
    /// by the name a reply calls the capability.
    calls: HashMap<String, VecDeque<f64>>,
    /// Recent navigations, as the straight-line metres between where the robot
    /// was and where it stopped, and the seconds they took.
    moves: VecDeque<(f64, f64)>,
    /// Where the robot was last known to be, which the next navigation's
    /// distance is measured from; not kept across restarts.
    #[serde(skip)]
    position: Option<(f64, f64)>,
}

/// Whether a reply's name for a capability is the navigation one.
fn navigates(capability: &str) -> bool {
    capability.ends_with("navigation_navigate")
}

/// The map coordinates a navigation's arguments name, when they are literal.
fn goal(args: &Value) -> Option<(f64, f64)> {
    let at = &args["goal"]["pose"]["position"];
    Some((at["x"].as_f64()?, at["y"].as_f64()?))
}

/// Where a navigation's output says the robot stopped, from the
/// `last_pose=(x,y)` its detail reports.
fn stopped(output: &str) -> Option<(f64, f64)> {
    let (_, rest) = output.split_once("last_pose=(")?;
    let (x, rest) = rest.split_once(',')?;
    let (y, _) = rest.split_once(')')?;
    Some((x.trim().parse().ok()?, y.trim().parse().ok()?))
}

fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
    (a.0 - b.0).hypot(a.1 - b.1)
}

fn push<T>(kept: &mut VecDeque<T>, item: T, limit: usize) {
    if kept.len() == limit {
        kept.pop_front();
    }
    kept.push_back(item);
}

impl Experience {
    /// The experience saved at `path`; none if there is none or it does not parse.
    pub(crate) fn load(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// The experience as saved, for writing without holding the lock it lives under.
    pub(crate) fn saved(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Record that a call of `capability` succeeded after `time` with `output`;
    /// a navigation also moves the robot to where it stopped.
    pub(crate) fn record(&mut self, capability: &str, time: Duration, output: &str) {
        let seconds = time.as_secs_f64();
        let kept = self.calls.entry(capability.into()).or_default();
        push(kept, seconds, KEPT);
        if navigates(capability)
            && let Some(end) = stopped(output)
        {
            if let Some(start) = self.position {
                push(&mut self.moves, (distance(start, end), seconds), MOVES_KEPT);
            }
            self.position = Some(end);
        }
    }

    /// The robot is at `at`, as the state view says at the start of a round.
    pub(crate) fn locate(&mut self, at: (f64, f64)) {
        self.position = Some(at);
    }

    /// Where the robot was last known to be.
    pub(crate) fn position(&self) -> Option<(f64, f64)> {
        self.position
    }

    /// The median of a capability's recent durations.
    fn typical(&self, capability: &str) -> Option<f64> {
        let mut seen: Vec<f64> = self.calls.get(capability)?.iter().copied().collect();
        seen.sort_by(f64::total_cmp);
        seen.get(seen.len() / 2).copied()
    }

    /// Seconds a navigation over `metres` is expected to take, from a line
    /// fitted to the recent navigations; none until they span some distances.
    fn travel(&self, metres: f64) -> Option<f64> {
        let n = self.moves.len() as f64;
        if n < 8.0 {
            return None;
        }
        let (md, mt) = self
            .moves
            .iter()
            .fold((0.0, 0.0), |(d, t), (dm, ts)| (d + dm / n, t + ts / n));
        let spread: f64 = self.moves.iter().map(|(d, _)| (d - md).powi(2)).sum();
        if spread < 1.0 {
            return None;
        }
        let slope: f64 = self
            .moves
            .iter()
            .map(|(d, t)| (d - md) * (t - mt))
            .sum::<f64>()
            / spread;
        Some((mt + slope * (metres - md)).max(0.0))
    }

    /// How long `steps`, a capability and its arguments each, take run one
    /// after another from `from`, and where they leave the robot. A navigation
    /// to literal coordinates is timed by its distance when the robot's start
    /// is known, any other step by its capability's typical duration; the time
    /// is unknown if a step's capability was never seen to succeed.
    pub(crate) fn estimate<'a>(
        &self,
        steps: impl IntoIterator<Item = (&'a str, &'a Value)>,
        from: Option<(f64, f64)>,
    ) -> (Option<Duration>, Option<(f64, f64)>) {
        let mut at = from;
        let mut total = Some(0.0);
        for (capability, args) in steps {
            let seconds = if navigates(capability) {
                let to = goal(args);
                let by_distance = to
                    .zip(at)
                    .and_then(|(to, at)| self.travel(distance(at, to)));
                at = to;
                by_distance.or_else(|| self.typical(capability))
            } else {
                self.typical(capability)
            };
            total = total.zip(seconds).map(|(total, seconds)| total + seconds);
        }
        (total.map(Duration::from_secs_f64), at)
    }
}

#[cfg(test)]
mod tests {
    use super::Experience;
    use serde_json::json;
    use std::time::Duration;

    const NAV: &str = "nav2.navigation_navigate";

    fn arrived(x: f64, y: f64) -> String {
        format!(r#"{{"detail":"succeeded; distance_remaining=0.2m last_pose=({x},{y})"}}"#)
    }

    #[test]
    fn a_navigation_is_timed_by_its_distance_once_navigations_span_some() {
        let mut seen = Experience::default();
        seen.locate((0.0, 0.0));
        // 5 s plus 6 s a metre, back and forth along the x axis.
        for (i, x) in [1.0, 4.0, 2.0, 6.0, 3.0, 8.0, 1.0, 5.0, 2.0]
            .into_iter()
            .enumerate()
        {
            let from = seen.position().unwrap().0;
            let seconds = 5.0 + 6.0 * (x - from).abs();
            seen.record(NAV, Duration::from_secs_f64(seconds), &arrived(x, 0.0));
            assert_eq!(seen.position(), Some((x, 0.0)), "after move {i}");
        }
        seen.record("scene.scene_find", Duration::from_secs(4), "{}");
        let to = |x: f64| json!({"goal": {"pose": {"position": {"x": x, "y": 0.0}}}});
        let (near, far, find) = (to(3.0), to(12.0), json!({}));
        let steps = [(NAV, &near), ("scene.scene_find", &find), (NAV, &far)];
        let (time, end) = seen.estimate(steps, Some((2.0, 0.0)));
        // 11 s for one metre, 4 s for the search, 59 s for nine metres.
        assert!((time.unwrap().as_secs_f64() - 74.0).abs() < 0.5);
        assert_eq!(end, Some((12.0, 0.0)));
        // Without a start, a navigation counts as its capability's typical one.
        let unknown = seen.estimate([(NAV, &near)], None).0.unwrap().as_secs_f64();
        assert!(unknown > 11.0);
        assert_eq!(seen.estimate([("never.seen", &find)], None).0, None);
    }

    #[test]
    fn experience_outlives_the_process_but_the_robots_position_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("experience.json");
        let mut seen = Experience::default();
        seen.locate((1.0, 1.0));
        seen.record("scene.scene_find", Duration::from_secs(3), "{}");
        std::fs::write(&path, seen.saved()).unwrap();
        let loaded = Experience::load(&path);
        assert_eq!(loaded.typical("scene.scene_find"), Some(3.0));
        assert_eq!(loaded.position(), None);
        assert_eq!(
            Experience::load(&dir.path().join("missing")).typical("x"),
            None
        );
    }
}
