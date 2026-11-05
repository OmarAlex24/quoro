// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's reading of the edge's backend health (ADR 0143,
//! `node-agent-protocol.md` section 5.3).
//!
//! The primary stores no edge backend health (ADR 0143): it keeps the newest
//! report of each edge machine in memory and turns a change of an app's
//! rotation state into `app.degraded`, `app.down` or `app.up`. `app.degraded`
//! opens no incident; `app.down` with `check: edge_health` does, like any
//! other `app.down` (D-0032, ADR 0143). The transition is a pure function of
//! the state kept so far and the newest report, so it is tested without a
//! session or a database.

use quoro_proto::wire::{EdgeBackendStatus, EdgeOutReason};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// What the primary needs from an `apps` row to read a report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppFacts {
    /// The app's project, for the emitted events.
    pub project_id: String,
    /// `apps.replicas`.
    pub replicas: i64,
}

/// The edge's health view of one app, kept in memory between reports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum AppHealth {
    /// Every instance is in rotation, or the app was never reported.
    #[default]
    Up,
    /// Some instances are out of rotation and at least one is in.
    Degraded {
        /// The instances out of rotation, by VM id.
        out: BTreeSet<String>,
        /// The earliest `since` of the instances that are out: when the app
        /// became degraded.
        since: String,
    },
    /// Every instance is out of rotation.
    Down {
        /// The earliest `since` of the instances that are out: when the app
        /// went down.
        since: String,
    },
}

/// One event a report produces, before its project is attached.
#[derive(Clone, Debug, PartialEq)]
pub struct AppEvent {
    /// `app.degraded`, `app.down` or `app.up`.
    pub kind: &'static str,
    /// The app the event is about.
    pub app_id: String,
    /// The `data` of the event, shaped per `events.schema.json`.
    pub data: Value,
}

/// Reads one report against the state kept so far and returns the events it
/// causes. The state is updated in place.
///
/// A backend that is out of rotation and names no reason is read as a failed
/// health check, the reason the primary can assume. An app with no row (it was
/// deleted while the report was in flight) is ignored.
pub fn apply(
    state: &mut BTreeMap<String, AppHealth>,
    edge_node_id: &str,
    report: &[EdgeBackendStatus],
    apps: &BTreeMap<String, AppFacts>,
) -> Vec<AppEvent> {
    let mut by_app: BTreeMap<&str, Vec<&EdgeBackendStatus>> = BTreeMap::new();
    for entry in report {
        by_app.entry(entry.app_id.as_str()).or_default().push(entry);
    }

    let mut events = Vec::new();
    for (app_id, entries) in by_app {
        let Some(facts) = apps.get(app_id) else {
            continue;
        };
        // Only an app with more than one replica is served by the edge; a
        // report still naming one that was scaled down is left alone, since
        // `app.degraded` requires `replicas` of at least 2.
        if facts.replicas < 2 {
            continue;
        }
        let total = entries.len();
        let out: Vec<&EdgeBackendStatus> = entries
            .iter()
            .copied()
            .filter(|entry| !entry.in_rotation)
            .collect();
        let previous = state.get(app_id).cloned().unwrap_or_default();

        if out.is_empty() {
            match &previous {
                AppHealth::Degraded { since, .. } => events.push(AppEvent {
                    kind: "app.up",
                    app_id: app_id.to_string(),
                    data: json!({
                        "app_id": app_id,
                        "from": "degraded",
                        "degraded_since": since,
                    }),
                }),
                AppHealth::Down { since } => events.push(AppEvent {
                    kind: "app.up",
                    app_id: app_id.to_string(),
                    data: {
                        json!({
                            "app_id": app_id,
                            "from": "down",
                            "down_since": since,
                        })
                    },
                }),
                AppHealth::Up => {}
            }
            state.insert(app_id.to_string(), AppHealth::Up);
            continue;
        }

        let ids: BTreeSet<String> = out.iter().map(|entry| entry.vm_id.clone()).collect();
        if out.len() == total {
            if !matches!(previous, AppHealth::Down { .. }) {
                let since = earliest(&out);
                let mut data = json!({
                    "app_id": app_id,
                    "node_id": edge_node_id,
                    "check": "edge_health",
                    "failures": out.len(),
                });
                if let Some(error) = out.iter().find_map(|entry| entry.last_error.clone()) {
                    data["last_error"] = Value::from(error);
                }
                events.push(AppEvent {
                    kind: "app.down",
                    app_id: app_id.to_string(),
                    data,
                });
                state.insert(app_id.to_string(), AppHealth::Down { since });
            }
            continue;
        }

        let already = matches!(
            &previous,
            AppHealth::Degraded { out: previous, .. } if *previous == ids
        );
        if !already {
            let since = earliest(&out);
            events.push(AppEvent {
                kind: "app.degraded",
                app_id: app_id.to_string(),
                data: json!({
                    "app_id": app_id,
                    "edge_node_id": edge_node_id,
                    "replicas": facts.replicas,
                    "in_rotation": total - out.len(),
                    "out_of_rotation": out
                        .iter()
                        .map(|entry| json!({
                            "vm_id": entry.vm_id,
                            "node_id": entry.node_id,
                            "since": entry.since,
                            "reason": reason(entry.reason),
                        }))
                        .collect::<Vec<_>>(),
                }),
            });
            state.insert(app_id.to_string(), AppHealth::Degraded { out: ids, since });
        }
    }
    events
}

/// The earliest `since` of the instances out of rotation. `out` is never
/// empty; an empty slice would answer with an empty time, which no report
/// carries.
fn earliest(out: &[&EdgeBackendStatus]) -> String {
    out.iter()
        .map(|entry| entry.since.as_str())
        .min()
        .unwrap_or_default()
        .to_string()
}

/// The wire name of an out-of-rotation reason (`events.schema.json`).
fn reason(reason: Option<EdgeOutReason>) -> &'static str {
    match reason {
        Some(EdgeOutReason::ConnectFailed) => "connect_failed",
        Some(EdgeOutReason::HealthCheck) | None => "health_check",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend entry of an edge report.
    fn entry(vm_id: &str, in_rotation: bool, since: &str) -> EdgeBackendStatus {
        EdgeBackendStatus {
            app_id: "app_0000000000000000000000001".to_string(),
            vm_id: vm_id.to_string(),
            node_id: format!("nod_{}", &vm_id[3..]),
            in_rotation,
            since: since.to_string(),
            reason: (!in_rotation).then_some(EdgeOutReason::HealthCheck),
            last_error: (!in_rotation).then(|| "connection refused".to_string()),
        }
    }

    /// The facts of the one app these reports are about.
    fn facts() -> BTreeMap<String, AppFacts> {
        BTreeMap::from([(
            "app_0000000000000000000000001".to_string(),
            AppFacts {
                project_id: "prj_0000000000000000000000001".to_string(),
                replicas: 2,
            },
        )])
    }

    #[test]
    fn everything_in_rotation_emits_nothing() {
        let mut state = BTreeMap::new();
        let report = vec![
            entry("vm_0000000000000000000000001", true, "2026-11-04T10:00:00Z"),
            entry("vm_0000000000000000000000002", true, "2026-11-04T10:00:00Z"),
        ];
        let events = apply(
            &mut state,
            "nod_000000000000000000000000E",
            &report,
            &facts(),
        );
        assert!(events.is_empty());
        assert_eq!(
            state.get("app_0000000000000000000000001"),
            Some(&AppHealth::Up)
        );
    }

    #[test]
    fn one_of_two_out_is_degraded_and_the_same_set_does_not_repeat() {
        let mut state = BTreeMap::new();
        let edge = "nod_000000000000000000000000E";
        let report = vec![
            entry("vm_0000000000000000000000001", true, "2026-11-04T10:00:00Z"),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:00:05Z",
            ),
        ];
        let events = apply(&mut state, edge, &report, &facts());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "app.degraded");
        assert_eq!(events[0].data["edge_node_id"], edge);
        assert_eq!(events[0].data["replicas"], 2);
        assert_eq!(events[0].data["in_rotation"], 1);
        assert_eq!(
            events[0].data["out_of_rotation"][0]["vm_id"],
            "vm_0000000000000000000000002"
        );
        assert_eq!(
            events[0].data["out_of_rotation"][0]["reason"],
            "health_check"
        );

        assert!(apply(&mut state, edge, &report, &facts()).is_empty());
    }

    #[test]
    fn a_changed_out_set_is_degraded_again() {
        let mut state = BTreeMap::new();
        let edge = "nod_000000000000000000000000E";
        let first = vec![
            entry("vm_0000000000000000000000001", true, "2026-11-04T10:00:00Z"),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:00:05Z",
            ),
        ];
        assert_eq!(apply(&mut state, edge, &first, &facts()).len(), 1);

        let second = vec![
            entry(
                "vm_0000000000000000000000001",
                false,
                "2026-11-04T10:00:09Z",
            ),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:00:05Z",
            ),
            entry("vm_0000000000000000000000003", true, "2026-11-04T10:00:00Z"),
        ];
        let events = apply(&mut state, edge, &second, &facts());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "app.degraded");
        assert_eq!(events[0].data["in_rotation"], 1);
        assert_eq!(
            events[0].data["out_of_rotation"].as_array().unwrap().len(),
            2
        );
    }

    #[test]
    fn all_out_is_down_only_once() {
        let mut state = BTreeMap::new();
        let edge = "nod_000000000000000000000000E";
        let report = vec![
            entry(
                "vm_0000000000000000000000001",
                false,
                "2026-11-04T10:00:05Z",
            ),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:00:06Z",
            ),
        ];
        let events = apply(&mut state, edge, &report, &facts());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "app.down");
        assert_eq!(events[0].data["node_id"], edge);
        assert_eq!(events[0].data["check"], "edge_health");
        assert_eq!(events[0].data["failures"], 2);
        assert_eq!(events[0].data["last_error"], "connection refused");
        assert!(apply(&mut state, edge, &report, &facts()).is_empty());
        assert!(matches!(
            state.get("app_0000000000000000000000001"),
            Some(AppHealth::Down { .. })
        ));
    }

    #[test]
    fn everything_back_in_rotation_is_up_from_down_then_from_degraded() {
        let mut state = BTreeMap::new();
        let edge = "nod_000000000000000000000000E";
        let all_out = vec![
            entry(
                "vm_0000000000000000000000001",
                false,
                "2026-11-04T10:00:05Z",
            ),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:00:06Z",
            ),
        ];
        assert_eq!(apply(&mut state, edge, &all_out, &facts()).len(), 1);

        let all_in = vec![
            entry("vm_0000000000000000000000001", true, "2026-11-04T10:02:00Z"),
            entry("vm_0000000000000000000000002", true, "2026-11-04T10:02:00Z"),
        ];
        let events = apply(&mut state, edge, &all_in, &facts());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "app.up");
        assert_eq!(events[0].data["from"], "down");
        assert_eq!(events[0].data["down_since"], "2026-11-04T10:00:05Z");

        let one_out = vec![
            entry("vm_0000000000000000000000001", true, "2026-11-04T10:02:00Z"),
            entry(
                "vm_0000000000000000000000002",
                false,
                "2026-11-04T10:03:00Z",
            ),
        ];
        // A fresh state, so the degraded-to-up path is read on its own.
        let mut state = BTreeMap::new();
        assert_eq!(apply(&mut state, edge, &one_out, &facts()).len(), 1);
        let events = apply(&mut state, edge, &all_in, &facts());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "app.up");
        assert_eq!(events[0].data["from"], "degraded");
        assert_eq!(events[0].data["degraded_since"], "2026-11-04T10:03:00Z");
    }
}
