// SPDX-License-Identifier: AGPL-3.0-only
//! The edge's health-aware backend rotation (`node-agent-protocol.md` section
//! 6.8; ADR 0143).
//!
//! [`EdgeManager`] watches the verified snapshot like the mesh manager. For
//! every backend of `spec.edge.routes` it keeps a rotation state: a backend
//! starts in rotation, a probe of `health_check.path` runs every `interval_s`
//! with `timeout_s`, `failures` consecutive failed probes take it out and the
//! same number of consecutive passes bring it back. A connect failure reported
//! by the proxy's [`BackendObserver`] takes it out at once; the probes bring it
//! back.
//!
//! The manager pushes the in-rotation backends to the [`IngressSupervisor`] as
//! edge routes after every snapshot and every rotation change, so a health
//! decision takes effect without waiting for the primary. A backend that
//! leaves the rotation is also handed to [`IngressSupervisor::backend_down`],
//! which drops its pooled connections and cancels the requests still waiting
//! on it, so none of them waits for the response timeout. A backend that
//! leaves the spec (a deployment) is not: its requests finish normally. Its decisions never
//! change replicated state; they reach the primary only through
//! `status_report.edge_backends`. Every change of that list is published on
//! [`EdgeManager::changes`], so the session reports it at once instead of at
//! the next periodic report (section 5.3).

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quoro_ingress::proxy::BackendObserver;
use quoro_ingress::{RetryPolicy, Route, RouteTable, TlsMode};
use quoro_proto::wire::{
    EdgeBackendStatus, EdgeOutReason, EdgeRoute, HealthCheck, SpecEdge, SpecTunnel,
};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::health::{self, HealthTarget};
use crate::ingress::{EdgeRoutes, IngressSupervisor, RouteTables, tls_mode};
use crate::reconcile::Snapshot;
use crate::util;

/// Response-header timeout of every edge request (section 6.8).
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound of `last_error` in `status_report.edge_backends` (section 5.3).
pub const LAST_ERROR_MAX: usize = 500;

/// The key the edge keeps rotation state under: one app instance.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BackendKey {
    app_id: String,
    vm_id: String,
}

impl BackendKey {
    fn new(app_id: &str, vm_id: &str) -> Self {
        Self {
            app_id: app_id.to_string(),
            vm_id: vm_id.to_string(),
        }
    }
}

/// How one edge route is probed, with the contract defaults applied.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProbeSpec {
    path: String,
    interval: Duration,
    timeout: Duration,
    failures: u32,
}

impl ProbeSpec {
    fn from_check(check: &HealthCheck) -> Self {
        let path = check
            .path
            .clone()
            .unwrap_or_else(|| health::DEFAULT_PATH.to_owned());
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        Self {
            path,
            interval: Duration::from_secs(u64::from(
                check
                    .interval_s
                    .unwrap_or(health::DEFAULT_INTERVAL_S)
                    .max(1),
            )),
            timeout: Duration::from_secs(u64::from(
                check.timeout_s.unwrap_or(health::DEFAULT_TIMEOUT_S).max(1),
            )),
            failures: check.failures.unwrap_or(health::DEFAULT_FAILURES).max(1),
        }
    }
}

/// The snapshot parts of one backend, besides its key.
struct BackendInfo {
    node_id: String,
    addr: SocketAddr,
    probe: ProbeSpec,
}

/// One backend's rotation state.
#[derive(Clone, Debug)]
struct BackendRotation {
    app_id: String,
    vm_id: String,
    node_id: String,
    addr: SocketAddr,
    probe: ProbeSpec,
    in_rotation: bool,
    /// RFC 3339 of the last change, or when the backend was first seen.
    since: String,
    /// Set while out of rotation.
    reason: Option<EdgeOutReason>,
    last_error: Option<String>,
    /// Consecutive failed probes.
    failures: u32,
    /// Consecutive passed probes while out of rotation.
    passes: u32,
    /// When the next probe is due.
    next_probe: Instant,
}

impl BackendRotation {
    fn new(key: &BackendKey, info: &BackendInfo, now: &str) -> Self {
        Self {
            app_id: key.app_id.clone(),
            vm_id: key.vm_id.clone(),
            node_id: info.node_id.clone(),
            addr: info.addr,
            probe: info.probe.clone(),
            // A backend the edge has not probed yet starts in rotation.
            in_rotation: true,
            since: now.to_string(),
            reason: None,
            last_error: None,
            failures: 0,
            passes: 0,
            next_probe: Instant::now() + info.probe.interval,
        }
    }

    /// The probe target, or `None` when the address is not IPv4.
    fn target(&self) -> Option<HealthTarget> {
        let SocketAddr::V4(addr) = self.addr else {
            return None;
        };
        Some(HealthTarget {
            ip: *addr.ip(),
            port: addr.port(),
            path: self.probe.path.clone(),
            interval: self.probe.interval,
            timeout: self.probe.timeout,
            failures: self.probe.failures,
        })
    }

    /// Records a passed probe. Returns whether the rotation changed.
    fn record_pass(&mut self, now: &str, failures: u32) -> bool {
        self.failures = 0;
        if self.in_rotation {
            self.passes = 0;
            return false;
        }
        self.passes = self.passes.saturating_add(1);
        if self.passes < failures {
            return false;
        }
        self.passes = 0;
        self.in_rotation = true;
        self.since = now.to_string();
        self.reason = None;
        self.last_error = None;
        true
    }

    /// Records a failed probe. Returns whether the rotation changed.
    fn record_failure(&mut self, now: &str, failures: u32, reason: &str) -> bool {
        self.passes = 0;
        self.last_error = Some(truncate(reason));
        if !self.in_rotation {
            return false;
        }
        self.failures = self.failures.saturating_add(1);
        if self.failures < failures {
            return false;
        }
        self.failures = 0;
        self.in_rotation = false;
        self.since = now.to_string();
        self.reason = Some(EdgeOutReason::HealthCheck);
        true
    }

    /// Takes the backend out at once after the proxy could not connect or the
    /// connection was reset. Returns whether the rotation changed.
    fn connect_failed(&mut self, now: &str, error: &str) -> bool {
        self.last_error = Some(truncate(error));
        self.passes = 0;
        if !self.in_rotation {
            return false;
        }
        self.failures = 0;
        self.in_rotation = false;
        self.since = now.to_string();
        self.reason = Some(EdgeOutReason::ConnectFailed);
        true
    }

    fn status(&self) -> EdgeBackendStatus {
        EdgeBackendStatus {
            app_id: self.app_id.clone(),
            vm_id: self.vm_id.clone(),
            node_id: self.node_id.clone(),
            in_rotation: self.in_rotation,
            since: self.since.clone(),
            reason: self.reason.filter(|_| !self.in_rotation),
            last_error: self.last_error.clone(),
        }
    }
}

/// One line, at most [`LAST_ERROR_MAX`] characters.
fn truncate(message: &str) -> String {
    let line = message.lines().next().unwrap_or_default();
    line.chars().take(LAST_ERROR_MAX).collect()
}

/// The manager's current view of the snapshot and the rotation state.
#[derive(Default)]
struct EdgeState {
    routes: Vec<EdgeRoute>,
    tunnel: Option<SpecTunnel>,
    rotations: BTreeMap<BackendKey, BackendRotation>,
}

impl EdgeState {
    /// Replaces the routes and rotates the state map with them. A backend that
    /// stays keeps its rotation state and `since`; a new one starts in
    /// rotation; one that left the spec is forgotten.
    fn replace(&mut self, edge: Option<&SpecEdge>, now: &str) {
        let mut wanted: BTreeMap<BackendKey, BackendInfo> = BTreeMap::new();
        if let Some(edge) = edge {
            for route in &edge.routes {
                let probe = ProbeSpec::from_check(&route.health_check);
                for backend in &route.backends {
                    let Ok(ip) = backend.address.parse::<Ipv4Addr>() else {
                        warn!(
                            vm_id = %backend.vm_id,
                            address = %backend.address,
                            "edge backend address is not an IPv4 address; not served"
                        );
                        continue;
                    };
                    wanted.insert(
                        BackendKey::new(&route.app_id, &backend.vm_id),
                        BackendInfo {
                            node_id: backend.node_id.clone(),
                            addr: SocketAddr::from((ip, backend.port)),
                            probe: probe.clone(),
                        },
                    );
                }
            }
        }
        let mut rotations = BTreeMap::new();
        for (key, info) in wanted {
            let rotation = match self.rotations.remove(&key) {
                Some(mut rotation) => {
                    rotation.node_id = info.node_id;
                    rotation.addr = info.addr;
                    rotation.probe = info.probe;
                    rotation
                }
                None => BackendRotation::new(&key, &info, now),
            };
            rotations.insert(key, rotation);
        }
        self.rotations = rotations;
        self.routes = edge.map_or_else(Vec::new, |edge| edge.routes.clone());
        self.tunnel = edge.and_then(|edge| edge.tunnel.clone());
    }

    /// Pushes the in-rotation backends as proxy routes.
    fn tables(&self) -> RouteTables {
        let mut tunnel = Vec::new();
        let mut acme = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for route in &self.routes {
            let Some(mode) = tls_mode(&route.tls_mode) else {
                warn!(
                    hostname = %route.hostname,
                    tls_mode = %route.tls_mode,
                    "edge route has an unknown tls_mode; not served"
                );
                continue;
            };
            let hostname = route.hostname.to_ascii_lowercase();
            if !seen.insert(hostname.clone()) {
                warn!(%hostname, "edge route appears more than once; keeping the first");
                continue;
            }
            let backends: Vec<SocketAddr> = route
                .backends
                .iter()
                .filter_map(|backend| {
                    let key = BackendKey::new(&route.app_id, &backend.vm_id);
                    self.rotations
                        .get(&key)
                        .filter(|rotation| rotation.in_rotation)
                        .map(|rotation| rotation.addr)
                })
                .collect();
            let route = Route {
                hostname,
                backends,
                tls: mode,
                retry: RetryPolicy::IdempotentOnce,
                response_timeout: Some(RESPONSE_TIMEOUT),
            };
            match mode {
                TlsMode::Tunnel => tunnel.push(route),
                TlsMode::Acme => acme.push(route),
            }
        }
        let table = |routes| {
            RouteTable::new(routes).unwrap_or_else(|e| {
                // Individual routes were already validated; a collision that
                // still got here is a bug, and an empty table is the safe answer.
                error!(error = %e, "could not build an edge route table");
                RouteTable::default()
            })
        };
        RouteTables {
            tunnel: table(tunnel),
            acme: table(acme),
        }
    }

    /// `status_report.edge_backends` for the current view, ordered by app and
    /// instance.
    fn statuses(&self) -> Vec<EdgeBackendStatus> {
        self.rotations
            .values()
            .map(BackendRotation::status)
            .collect()
    }

    /// The earliest probe deadline, or `None` when there is no backend.
    fn next_deadline(&self) -> Option<Instant> {
        self.rotations
            .values()
            .map(|rotation| rotation.next_probe)
            .min()
    }
}

/// Probes one backend once. `Ok` means a 2xx or 3xx status; `Err` is the
/// one-line reason a `status_report` carries.
async fn probe_one(target: &HealthTarget) -> Result<(), String> {
    match health::probe(target).await {
        Ok(status) if health::is_passing(status) => Ok(()),
        Ok(status) => Err(format!("HTTP {status}")),
        Err(error) => Err(error),
    }
}

/// Keeps the edge's route table equal to the snapshot and the probe results.
pub struct EdgeManager {
    supervisor: Arc<IngressSupervisor>,
    state: Mutex<EdgeState>,
    /// The newest snapshot revision whose `spec.edge` was handed to the
    /// supervisor; 0 before the first snapshot.
    applied: AtomicU64,
    /// The current `status_report.edge_backends`, replaced under the state
    /// lock after every change, so its receivers wake only when the report
    /// would differ.
    reported: watch::Sender<Vec<EdgeBackendStatus>>,
}

impl EdgeManager {
    /// A manager that pushes its routes to `supervisor`.
    #[must_use]
    pub fn new(supervisor: Arc<IngressSupervisor>) -> Self {
        Self {
            supervisor,
            state: Mutex::new(EdgeState::default()),
            applied: AtomicU64::new(0),
            reported: watch::channel(Vec::new()).0,
        }
    }

    /// The newest snapshot revision this manager applied (section 6.8). The
    /// reconciler records a revision with `spec.edge` as applied only once
    /// this reaches it.
    #[must_use]
    pub fn applied_rev(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// The current `status_report.edge_backends`. Empty while the snapshot has
    /// no `spec.edge` or before the first snapshot.
    #[must_use]
    pub fn statuses(&self) -> Vec<EdgeBackendStatus> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .statuses()
    }

    /// A receiver that changes whenever [`statuses`](Self::statuses) does: a
    /// backend left or joined the rotation, its `last_error` changed, or a
    /// snapshot added or removed backends. A report sent on that change is
    /// what section 5.3 calls a report "on any observed change".
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<Vec<EdgeBackendStatus>> {
        self.reported.subscribe()
    }

    /// Publishes `state`'s statuses when they differ from the last ones.
    /// Called with the state lock held, so the published lists follow the
    /// same order as the changes.
    fn publish(&self, state: &EdgeState) {
        let statuses = state.statuses();
        self.reported.send_if_modified(|current| {
            if *current == statuses {
                return false;
            }
            *current = statuses;
            true
        });
    }

    /// Watches the snapshot, probes the backends and pushes the tables until
    /// `stop` resolves. A snapshot without `spec.edge` serves nothing and
    /// keeps no backend.
    pub async fn run(
        self: Arc<Self>,
        mut desired: watch::Receiver<Option<Snapshot>>,
        stop: impl Future<Output = ()> + Send,
    ) {
        let mut stop = std::pin::pin!(stop);
        let current = desired.borrow_and_update().clone();
        self.sync(current.as_ref());

        loop {
            let deadline = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next_deadline();
            tokio::select! {
                () = &mut stop => break,
                changed = desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let current = desired.borrow_and_update().clone();
                    self.sync(current.as_ref());
                }
                () = wait_until(deadline) => self.probe_due().await,
            }
        }
    }

    /// One new snapshot: rebuild the rotation map and push the tables.
    fn sync(&self, snapshot: Option<&Snapshot>) {
        let now = util::now_rfc3339();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.replace(
            snapshot.and_then(|snapshot| snapshot.state.spec.edge.as_ref()),
            &now,
        );
        self.publish(&state);
        let tables = state.tables();
        let tunnel = state.tunnel.clone();
        drop(state);
        self.supervisor.update_edge(EdgeRoutes { tables, tunnel });
        if let Some(snapshot) = snapshot {
            self.applied.fetch_max(snapshot.rev, Ordering::AcqRel);
        }
    }

    /// Probes every backend whose deadline has passed, concurrently, and
    /// pushes the tables when a rotation changed.
    async fn probe_due(&self) {
        let due: Vec<(BackendKey, HealthTarget)> = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            state
                .rotations
                .iter()
                .filter(|(_, rotation)| rotation.next_probe <= now)
                .filter_map(|(key, rotation)| rotation.target().map(|target| (key.clone(), target)))
                .collect()
        };
        if due.is_empty() {
            return;
        }
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            for (key, target) in &due {
                if let Some(rotation) = state.rotations.get_mut(key) {
                    rotation.next_probe = now + target.interval;
                }
            }
        }

        let results = futures_util::future::join_all(
            due.into_iter()
                .map(|(key, target)| async move { (key, probe_one(&target).await) }),
        )
        .await;

        let now = util::now_rfc3339();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut changed = false;
        let mut gone: Vec<SocketAddr> = Vec::new();
        for (key, result) in results {
            let Some(rotation) = state.rotations.get_mut(&key) else {
                continue;
            };
            let failures = rotation.probe.failures;
            let flipped = match result {
                Ok(()) => rotation.record_pass(&now, failures),
                Err(error) => rotation.record_failure(&now, failures, &error),
            };
            if flipped && !rotation.in_rotation {
                gone.push(rotation.addr);
            }
            changed |= flipped;
        }
        // A failed probe changes `last_error` even when the backend stays in
        // rotation; the report carries that too.
        self.publish(&state);
        if changed {
            let tables = state.tables();
            let tunnel = state.tunnel.clone();
            drop(state);
            self.supervisor.update_edge(EdgeRoutes { tables, tunnel });
            for backend in gone {
                self.supervisor.backend_down(backend);
            }
        }
    }
}

impl BackendObserver for EdgeManager {
    fn connect_failed(&self, hostname: &str, backend: SocketAddr, error: &str) {
        let now = util::now_rfc3339();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys: Vec<BackendKey> = state
            .routes
            .iter()
            .filter(|route| route.hostname.eq_ignore_ascii_case(hostname))
            .flat_map(|route| {
                route
                    .backends
                    .iter()
                    .map(|backend| BackendKey::new(&route.app_id, &backend.vm_id))
            })
            .filter(|key| {
                state
                    .rotations
                    .get(key)
                    .is_some_and(|rotation| rotation.addr == backend)
            })
            .collect();
        let mut changed = false;
        for key in keys {
            if let Some(rotation) = state.rotations.get_mut(&key) {
                changed |= rotation.connect_failed(&now, error);
            }
        }
        self.publish(&state);
        if changed {
            let tables = state.tables();
            let tunnel = state.tunnel.clone();
            drop(state);
            self.supervisor.update_edge(EdgeRoutes { tables, tunnel });
            self.supervisor.backend_down(backend);
        }
    }
}

/// Waits for `deadline`, or forever when there is none.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use quoro_proto::wire::{EdgeBackend, HealthCheck};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn probe_spec(failures: u32) -> ProbeSpec {
        ProbeSpec {
            path: "/health".to_string(),
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(1),
            failures,
        }
    }

    fn rotation(failures: u32) -> BackendRotation {
        let key = BackendKey::new("app_1", "vm_1");
        let info = BackendInfo {
            node_id: "nod_1".to_string(),
            addr: "10.42.0.2:8080".parse().unwrap(),
            probe: probe_spec(failures),
        };
        BackendRotation::new(&key, &info, "2026-11-04T10:00:00Z")
    }

    fn edge_route(hostname: &str, backends: Vec<EdgeBackend>) -> EdgeRoute {
        EdgeRoute {
            hostname: hostname.to_string(),
            domain_id: "dom_1".to_string(),
            app_id: "app_1".to_string(),
            tls_mode: "acme".to_string(),
            health_check: HealthCheck {
                path: Some("/health".to_string()),
                interval_s: Some(1),
                timeout_s: Some(1),
                failures: Some(3),
                start_period_s: None,
                restart: None,
            },
            backends,
            draining_until: None,
        }
    }

    fn backend(vm_id: &str, address: &str) -> EdgeBackend {
        EdgeBackend {
            vm_id: vm_id.to_string(),
            node_id: "nod_1".to_string(),
            address: address.to_string(),
            port: 8080,
        }
    }

    #[test]
    fn failures_and_passes_use_the_same_threshold() {
        let mut rotation = rotation(3);
        assert!(rotation.in_rotation);
        assert!(!rotation.record_failure("2026-11-04T10:00:01Z", 3, "boom"));
        assert!(!rotation.record_failure("2026-11-04T10:00:02Z", 3, "boom"));
        assert!(rotation.in_rotation);
        assert!(rotation.record_failure("2026-11-04T10:00:03Z", 3, "boom"));
        assert!(!rotation.in_rotation);
        assert_eq!(rotation.reason, Some(EdgeOutReason::HealthCheck));
        assert_eq!(rotation.since, "2026-11-04T10:00:03Z");
        assert_eq!(rotation.last_error.as_deref(), Some("boom"));

        // It returns only after the same number of consecutive passes.
        assert!(!rotation.record_pass("2026-11-04T10:00:04Z", 3));
        assert!(!rotation.record_pass("2026-11-04T10:00:05Z", 3));
        assert!(rotation.record_pass("2026-11-04T10:00:06Z", 3));
        assert!(rotation.in_rotation);
        assert_eq!(rotation.reason, None);
        assert_eq!(rotation.last_error, None);
        assert_eq!(rotation.since, "2026-11-04T10:00:06Z");
    }

    #[test]
    fn a_failed_probe_between_passes_resets_the_count() {
        let mut rotation = rotation(2);
        assert!(!rotation.record_failure("t1", 2, "down"));
        assert!(rotation.record_failure("t2", 2, "down"));
        assert!(!rotation.in_rotation);
        assert!(!rotation.record_pass("t3", 2));
        assert!(!rotation.record_failure("t4", 2, "down"));
        assert!(!rotation.record_pass("t5", 2));
        assert!(rotation.record_pass("t6", 2));
        assert!(rotation.in_rotation);
    }

    #[test]
    fn a_connect_failure_takes_it_out_at_once_and_probes_bring_it_back() {
        let mut rotation = rotation(3);
        assert!(rotation.connect_failed("2026-11-04T11:00:00Z", "connection refused"));
        assert!(!rotation.in_rotation);
        assert_eq!(rotation.reason, Some(EdgeOutReason::ConnectFailed));
        assert_eq!(rotation.since, "2026-11-04T11:00:00Z");
        assert_eq!(rotation.last_error.as_deref(), Some("connection refused"));

        // A probe pass is needed; the connect failure reset the counter.
        assert!(!rotation.record_pass("2026-11-04T11:00:01Z", 3));
        assert!(!rotation.record_pass("2026-11-04T11:00:02Z", 3));
        assert!(rotation.record_pass("2026-11-04T11:00:03Z", 3));
        assert!(rotation.in_rotation);
        assert_eq!(rotation.reason, None);
    }

    #[test]
    fn a_connect_failure_while_out_does_not_change_since() {
        let mut rotation = rotation(1);
        assert!(rotation.record_failure("t1", 1, "HTTP 500"));
        assert!(!rotation.in_rotation);
        assert!(!rotation.connect_failed("t2", "reset"));
        assert_eq!(rotation.since, "t1");
        assert_eq!(rotation.last_error.as_deref(), Some("reset"));
    }

    #[test]
    fn backends_that_leave_the_spec_are_forgotten() {
        let mut state = EdgeState::default();
        let first = edge_route(
            "a.example.com",
            vec![backend("vm_1", "10.42.0.2"), backend("vm_2", "10.42.0.3")],
        );
        let edge = SpecEdge {
            routes: vec![first],
            tunnel: None,
        };
        state.replace(Some(&edge), "t1");
        assert_eq!(state.rotations.len(), 2);
        assert_eq!(state.statuses().len(), 2);

        let edge = SpecEdge {
            routes: vec![edge_route(
                "a.example.com",
                vec![backend("vm_2", "10.42.0.3")],
            )],
            tunnel: None,
        };
        state.replace(Some(&edge), "t2");
        assert_eq!(state.rotations.len(), 1);
        assert!(
            state
                .rotations
                .contains_key(&BackendKey::new("app_1", "vm_2"))
        );

        state.replace(None, "t3");
        assert!(state.rotations.is_empty());
        assert!(state.statuses().is_empty());
    }

    #[test]
    fn tables_hold_only_the_backends_in_rotation_with_edge_policy() {
        let mut state = EdgeState::default();
        let edge = SpecEdge {
            routes: vec![edge_route(
                "A.Example.com",
                vec![backend("vm_1", "10.42.0.2"), backend("vm_2", "10.42.0.3")],
            )],
            tunnel: None,
        };
        state.replace(Some(&edge), "t1");
        let key = BackendKey::new("app_1", "vm_2");
        state
            .rotations
            .get_mut(&key)
            .unwrap()
            .record_failure("t2", 1, "down");

        let tables = state.tables();
        let route = tables
            .acme
            .get("a.example.com")
            .expect("the route is served");
        assert_eq!(route.backends, vec!["10.42.0.2:8080".parse().unwrap()]);
        assert_eq!(route.retry, RetryPolicy::IdempotentOnce);
        assert_eq!(route.response_timeout, Some(RESPONSE_TIMEOUT));
        assert!(tables.tunnel.is_empty());
    }

    #[test]
    fn statuses_carry_rotation_reason_and_last_error() {
        let mut state = EdgeState::default();
        let edge = SpecEdge {
            routes: vec![edge_route(
                "a.example.com",
                vec![backend("vm_1", "10.42.0.2"), backend("vm_2", "10.42.0.3")],
            )],
            tunnel: None,
        };
        state.replace(Some(&edge), "2026-11-04T10:00:00Z");
        let key = BackendKey::new("app_1", "vm_1");
        state
            .rotations
            .get_mut(&key)
            .unwrap()
            .connect_failed("2026-11-04T10:01:00Z", "connection refused");

        let statuses = state.statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].app_id, "app_1");
        assert_eq!(statuses[0].vm_id, "vm_1");
        assert_eq!(statuses[0].node_id, "nod_1");
        assert!(!statuses[0].in_rotation);
        assert_eq!(statuses[0].since, "2026-11-04T10:01:00Z");
        assert_eq!(statuses[0].reason, Some(EdgeOutReason::ConnectFailed));
        assert_eq!(
            statuses[0].last_error.as_deref(),
            Some("connection refused")
        );
        assert!(statuses[1].in_rotation);
        assert_eq!(statuses[1].since, "2026-11-04T10:00:00Z");
        assert_eq!(statuses[1].reason, None);
    }

    #[test]
    fn last_error_is_one_line_of_at_most_500_characters() {
        let long = "x".repeat(600);
        assert_eq!(truncate(&long).chars().count(), LAST_ERROR_MAX);
        assert_eq!(truncate("first line\nsecond line"), "first line");
        assert_eq!(truncate("one line"), "one line");
    }

    #[tokio::test]
    async fn probing_a_local_server_passes_on_2xx_and_fails_on_5xx_or_a_closed_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for status in [204, 503] {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 512];
                let _ = socket.read(&mut buffer).await;
                let _ = socket
                    .write_all(
                        format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    )
                    .await;
            }
        });
        let target = HealthTarget {
            ip: Ipv4Addr::LOCALHOST,
            port,
            path: "/health".to_string(),
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(1),
            failures: 1,
        };
        assert_eq!(probe_one(&target).await, Ok(()));
        assert_eq!(probe_one(&target).await, Err("HTTP 503".to_string()));
        server.await.unwrap();
        assert!(probe_one(&target).await.is_err());
    }

    #[test]
    fn probe_spec_applies_the_contract_defaults() {
        let spec = ProbeSpec::from_check(&HealthCheck {
            path: None,
            interval_s: None,
            timeout_s: None,
            failures: None,
            start_period_s: None,
            restart: None,
        });
        assert_eq!(spec.path, "/");
        assert_eq!(
            spec.interval,
            Duration::from_secs(health::DEFAULT_INTERVAL_S.into())
        );
        assert_eq!(
            spec.timeout,
            Duration::from_secs(health::DEFAULT_TIMEOUT_S.into())
        );
        assert_eq!(spec.failures, health::DEFAULT_FAILURES);

        let spec = ProbeSpec::from_check(&HealthCheck {
            path: Some("live".to_string()),
            interval_s: Some(0),
            timeout_s: Some(7),
            failures: Some(0),
            start_period_s: None,
            restart: None,
        });
        assert_eq!(spec.path, "/live");
        assert_eq!(spec.interval, Duration::from_secs(1));
        assert_eq!(spec.timeout, Duration::from_secs(7));
        assert_eq!(spec.failures, 1);
    }
}
