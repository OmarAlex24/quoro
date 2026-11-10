// SPDX-License-Identifier: AGPL-3.0-only
//! [`BuildManager`]: runs `spec.builds` on this node (section 6.6).
//!
//! The manager is the node's whole build loop. It starts the oldest `queued`
//! build of the current snapshot when no other build runs (one at a time per
//! node, ADR 0118), turns the builder's events into the row the status report
//! reads, imports a success with `prepare_layout`, and serves the logs through
//! [`BuildsHandle`]. It is independent of the reconciler's other steps: a build
//! never blocks a VM, and a later apply run never restarts one.
//!
//! Two rules the contract fixes:
//!
//! - A build the primary marked `cancelled`, or left out of the snapshot,
//!   stops; a queued one is reported `cancelled` without ever running.
//! - An agent restart fails a running build with `node agent restarted`; builds
//!   are never resumed. A leftover builder VM (`bld_`) is removed at start-up.
//!
//! A `node capacity: ` failure from the builder is not a failure: the build
//! goes back to `queued` and is retried on a later pass.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use quoro_engine::{Engine, StopMode, VmState};
use quoro_proto::wire::{
    BuildPhase, BuildState, BuildStatus, FailedStep, LogLine, LogStream, Spec, SpecBuild,
};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::AgentError;
use crate::reconcile::{Snapshot, Subnet};
use crate::store::{BuildRecord, NodeStore};
use crate::util;

use super::log::{BuildLogs, StoredBuildLine};
use super::{
    BuildEvent, BuildFailure, BuildOutput, BuildSource, BuildSpec, Builder, BuilderNet, Frontend,
    MAX_ERROR_CHARS, MAX_EXCERPT_CHARS, MAX_EXCERPT_LINES, truncate_chars,
};

/// How often a pass runs when nothing else wakes the manager (section 6.2 step
/// 5: on each snapshot and every five seconds).
const DEFAULT_TICK: Duration = Duration::from_secs(5);
/// How often an open `build_logs` stream asks whether its build ended.
const FOLLOW_POLL: Duration = Duration::from_millis(100);
/// Lines sent in one `build_logs` batch.
const FOLLOW_BATCH: usize = 200;
/// The most lines one `build_logs_query` returns (section 5.17).
pub const MAX_QUERY_LIMIT: usize = 1_000;
/// How many builds of one app keep their log and row (section 6.6).
pub const KEPT_BUILDS_PER_APP: usize = 20;
/// The error prefix a builder uses for "the node has no capacity for this
/// build right now" (ADR 0118).
pub const CAPACITY_PREFIX: &str = "node capacity: ";
/// The first wait before a build the node had no capacity for is retried.
const WAIT_BASE: Duration = Duration::from_secs(5);
/// The longest wait between two attempts of a waiting build.
const WAIT_MAX: Duration = Duration::from_secs(60);
/// How often a build that is still waiting repeats its waiting line.
const WAIT_NOTICE: Duration = Duration::from_secs(10 * 60);

/// Why a build log request could not be answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildsError {
    /// The node has no build with that id (section 5.15, `not_found`).
    NotFound,
    /// The request itself was malformed.
    BadRequest(String),
    /// Reading the node's store or a log file failed.
    Store(String),
}

impl fmt::Display for BuildsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("this node has no such build"),
            Self::BadRequest(reason) => f.write_str(reason),
            Self::Store(reason) => write!(f, "could not read the build log: {reason}"),
        }
    }
}

impl std::error::Error for BuildsError {}

/// Where a `build_logs` stream starts (section 5.15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildStart {
    /// Replay the last `n` lines, then follow.
    Tail(usize),
    /// Replay the lines after this `seq`, then follow.
    After(u64),
}

/// A build that is running on this node.
struct Running {
    build_id: String,
    app_id: String,
    cancel: watch::Sender<bool>,
}

/// Why a build task stopped.
struct Done {
    build_id: String,
    /// Set when the node had no capacity: the reason after
    /// [`CAPACITY_PREFIX`]. The row went back to `queued` and the run
    /// directory was kept for the next attempt.
    capacity: Option<String>,
}

/// A build waiting for node capacity. Kept in memory only: a snapshot may
/// retry it at once, and a restart simply runs a queued build again.
struct Waiting {
    /// How many attempts were refused, which sets the next back-off.
    attempts: u32,
    /// The earliest time the next attempt may start.
    next_attempt: Instant,
    /// When the first refusal happened, for the `still waiting` line.
    first_at: Instant,
    /// When the waiting line was last appended.
    last_notice: Instant,
    /// The most recent capacity reason.
    reason: String,
}

/// Runs the node's builds. See the module documentation.
pub struct BuildManager<E: Engine, B: Builder> {
    engine: Arc<E>,
    builder: Arc<B>,
    store: NodeStore,
    logs: Arc<BuildLogs>,
    statuses: watch::Sender<Vec<BuildStatus>>,
    /// Bumped whenever a row changes, so the manager recomputes the status
    /// report without polling.
    changes: watch::Sender<u64>,
    running: Option<Running>,
    /// Builds whose last attempt the node had no capacity for, by build id.
    waiting: HashMap<String, Waiting>,
    current: Option<Snapshot>,
    tick: Duration,
    wait_base: Duration,
    wait_max: Duration,
    /// The image-inventory transport, when the agent runs one (section 5.26).
    inventory: Option<crate::inventory::InventoryHub>,
}

impl<E: Engine + 'static, B: Builder + 'static> BuildManager<E, B> {
    /// Builds the manager and the handle the session and the binary share.
    #[must_use]
    pub fn new(engine: Arc<E>, builder: Arc<B>, store: NodeStore) -> (Self, BuildsHandle) {
        let logs = Arc::new(BuildLogs::new(store.data_dir().join("builds").join("logs")));
        let (statuses, statuses_rx) = watch::channel(Vec::new());
        let (changes, _) = watch::channel(0u64);
        let manager = Self {
            engine,
            builder,
            store: store.clone(),
            logs: Arc::clone(&logs),
            statuses,
            changes,
            running: None,
            waiting: HashMap::new(),
            current: None,
            tick: DEFAULT_TICK,
            wait_base: WAIT_BASE,
            wait_max: WAIT_MAX,
            inventory: None,
        };
        let handle = BuildsHandle {
            store,
            logs,
            statuses: statuses_rx,
        };
        (manager, handle)
    }

    /// Replaces the pass interval; tests shorten it.
    #[must_use]
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    /// Replaces the capacity retry back-off; tests shorten it.
    #[must_use]
    pub fn with_wait_backoff(mut self, base: Duration, max: Duration) -> Self {
        self.wait_base = base;
        self.wait_max = max;
        self
    }

    /// Attaches the inventory transport: every image this manager imports
    /// queues its inventory for the session (section 5.26).
    #[must_use]
    pub fn with_inventory(mut self, inventory: crate::inventory::InventoryHub) -> Self {
        self.inventory = Some(inventory);
        self
    }

    /// The wait before the given refused attempt is retried: the base, then
    /// doubled per attempt up to the cap.
    fn wait_backoff(&self, attempts: u32) -> Duration {
        capacity_wait(self.wait_base, self.wait_max, attempts)
    }

    /// Fails what a previous agent left running, removes leftover builder VMs,
    /// then runs passes until `shutdown` resolves.
    pub async fn run(
        mut self,
        mut snapshots: watch::Receiver<Option<Snapshot>>,
        shutdown: impl Future<Output = ()> + Send,
    ) {
        let mut shutdown = std::pin::pin!(shutdown);
        self.startup().await;
        self.current = snapshots.borrow_and_update().clone();

        let mut changes = self.changes.subscribe();
        let (done_tx, mut done_rx) = mpsc::channel::<Done>(8);
        let mut ticker = tokio::time::interval(self.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        self.pass(&done_tx, false).await;
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.current = snapshots.borrow_and_update().clone();
                    // A VM may have been deleted, so a waiting build gets to
                    // try again at once rather than on its back-off.
                    self.pass(&done_tx, true).await;
                }
                _ = ticker.tick() => {
                    self.pass(&done_tx, false).await;
                }
                changed = changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.refresh_statuses().await;
                }
                done = done_rx.recv() => {
                    let mut start_next = true;
                    if let Some(done) = done {
                        if self.running.as_ref().is_some_and(|running| running.build_id == done.build_id) {
                            self.running = None;
                        }
                        if let Some(reason) = done.capacity {
                            // The node had no capacity: keep the row queued,
                            // record the back-off and write the one waiting
                            // line. The next attempt comes from a tick or a
                            // snapshot, never in a tight loop with this one.
                            self.on_capacity(&done.build_id, reason).await;
                            start_next = false;
                        } else {
                            // It started and ended, so it is no longer waiting.
                            self.waiting.remove(&done.build_id);
                        }
                    }
                    if start_next {
                        // A finished build frees the node for the next one now
                        // rather than at the next tick.
                        self.pass(&done_tx, false).await;
                    } else {
                        self.refresh_statuses().await;
                    }
                }
            }
        }
        info!("build manager stopped");
    }

    /// Fails every build a previous agent left non-terminal, then removes every
    /// leftover builder VM (section 6.6).
    async fn startup(&mut self) {
        let rows = match self.store.builds().await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "could not read the build rows at start-up");
                Vec::new()
            }
        };
        for mut row in rows {
            // Only a build that was running fails (section 6.6); one still
            // queued never started and simply runs on a later pass.
            if row.status.is_terminal() || row.status == BuildState::Queued {
                continue;
            }
            // Its address reservation may outlive a VM the engine never
            // created (the agent stopped between the two).
            if let Err(e) = self.store.forget_vm(&row.build_id).await {
                warn!(build_id = %row.build_id, error = %e, "could not release a builder address");
            }
            let phase = phase_of(row.status);
            row.status = BuildState::Failed;
            row.failed_phase = Some(phase);
            row.error = Some("node agent restarted".to_owned());
            row.finished_at = Some(now());
            // The line before the terminal row, as in `complete`.
            let line = format!("build failed in {}: node agent restarted", phase.as_str());
            self.finish_log(&row, &line).await;
            if let Err(e) = self.store.save_build(&row).await {
                warn!(build_id = %row.build_id, error = %e, "could not record a failed build");
            }
        }

        match self.engine.list().await {
            Ok(vms) => {
                for info in vms {
                    let build_id = info.id.0.clone();
                    if !build_id.starts_with("bld_") {
                        continue;
                    }
                    if matches!(
                        info.state,
                        VmState::Running | VmState::Starting | VmState::Stopping
                    ) && let Err(e) = self
                        .engine
                        .stop(
                            &info.id,
                            StopMode::Force {
                                within: crate::FORCE_WITHIN,
                            },
                        )
                        .await
                    {
                        warn!(build_id = %build_id, error = ?e, "could not force-stop a leftover builder VM");
                    }
                    match self.engine.delete(&info.id).await {
                        Ok(()) => {
                            if let Err(e) = self.store.forget_vm(&build_id).await {
                                warn!(build_id = %build_id, error = %e, "could not forget a leftover builder VM");
                            }
                            info!(build_id = %build_id, "removed a leftover builder VM");
                        }
                        Err(e) => {
                            warn!(build_id = %build_id, error = ?e, "could not delete a leftover builder VM");
                        }
                    }
                    self.builder.cleanup(&build_id).await;
                }
            }
            Err(e) => warn!(error = ?e, "could not list the engine's VMs at start-up"),
        }
        self.refresh_statuses().await;
    }

    /// One pass: rows for new builds, cancels, the next build, cache cleanup
    /// and the status report (section 6.2 step 5). `retry_now` lets a fresh
    /// snapshot bypass the capacity back-off.
    async fn pass(&mut self, done: &mpsc::Sender<Done>, retry_now: bool) {
        let Some(snapshot) = self.current.clone() else {
            self.refresh_statuses().await;
            return;
        };
        let spec = &snapshot.state.spec;
        let subnet = spec
            .network
            .as_ref()
            .and_then(|network| Subnet::parse(&network.subnet, &network.gateway));
        let desired: BTreeSet<String> = spec
            .builds
            .iter()
            .map(|build| build.build_id.clone())
            .collect();

        self.cleanup_caches(spec).await;

        for build in &spec.builds {
            if let Err(e) = self.ensure_row(build).await {
                warn!(build_id = %build.build_id, error = %e, "could not create a build row");
            }
        }
        let mut apps: BTreeSet<String> = BTreeSet::new();
        for build in &spec.builds {
            apps.insert(build.app_id.clone());
        }
        for app_id in apps {
            self.prune_app(&app_id, &desired).await;
        }

        // A build the primary left out is not run any more.
        let rows = self.store.builds().await.unwrap_or_default();
        for row in rows {
            if row.status.is_terminal() || desired.contains(&row.build_id) {
                continue;
            }
            if self.is_running(&row.build_id) {
                self.signal_cancel(&row.build_id);
            } else {
                self.cancel_build(&row).await;
            }
            self.waiting.remove(&row.build_id);
        }

        // A build the primary marked cancelled stops, or is reported without
        // ever running.
        for build in &spec.builds {
            let Ok(Some(row)) = self.store.build(&build.build_id).await else {
                continue;
            };
            if row.status.is_terminal() || !build.cancelled {
                continue;
            }
            if self.is_running(&build.build_id) {
                self.signal_cancel(&build.build_id);
            } else {
                self.cancel_build(&row).await;
            }
            self.waiting.remove(&build.build_id);
        }

        // A waiting build repeats its line at most every ten minutes.
        self.note_waiting().await;

        // One build at a time per node (ADR 0118): the oldest `queued` one in
        // spec order. A waiting one keeps the node otherwise free, and starts
        // again when its back-off expired or a snapshot said to retry.
        if self.running.is_none() {
            for build in &spec.builds {
                if build.cancelled {
                    continue;
                }
                let Ok(Some(row)) = self.store.build(&build.build_id).await else {
                    continue;
                };
                if row.status != BuildState::Queued {
                    continue;
                }
                let waiting = self.waiting.get(&build.build_id);
                let due =
                    retry_now || waiting.is_none_or(|wait| Instant::now() >= wait.next_attempt);
                if !due {
                    break;
                }
                match subnet {
                    Some(subnet) => {
                        self.start(build, row, &subnet, done, waiting.is_some())
                            .await;
                    }
                    None => debug!(
                        subnet = ?spec.network.as_ref().map(|network| &network.subnet),
                        "the desired network is not a usable subnet; no build starts"
                    ),
                }
                break;
            }
        }

        self.refresh_statuses().await;
    }

    /// Records a capacity refusal and, on the first one, appends the waiting
    /// line to the build log with an `info!` so the reason is visible.
    async fn on_capacity(&mut self, build_id: &str, reason: String) {
        let now = Instant::now();
        if let Some(attempts) = self
            .waiting
            .get(build_id)
            .map(|waiting| waiting.attempts.saturating_add(1))
        {
            let backoff = self.wait_backoff(attempts);
            if let Some(waiting) = self.waiting.get_mut(build_id) {
                waiting.attempts = attempts;
                waiting.next_attempt = now + backoff;
                waiting.reason = reason;
            }
            return;
        }
        info!(
            build_id,
            reason, "the node has no capacity for the build; it stays queued"
        );
        let row = match self.store.build(build_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return,
            Err(e) => {
                warn!(build_id, error = %e, "could not read a waiting build's row");
                return;
            }
        };
        let line = format!("waiting for node capacity: {reason}");
        if let Err(e) = self
            .logs
            .append(&row.app_id, &row.build_id, LogStream::Stdout, None, &line)
            .await
        {
            warn!(build_id, error = %e, "could not append to a build log");
        }
        self.waiting.insert(
            build_id.to_owned(),
            Waiting {
                attempts: 1,
                next_attempt: now + self.wait_backoff(1),
                first_at: now,
                last_notice: now,
                reason,
            },
        );
    }

    /// Appends `still waiting for node capacity after N min` to every waiting
    /// build whose last such line is [`WAIT_NOTICE`] old.
    async fn note_waiting(&mut self) {
        let now = Instant::now();
        let mut notices = Vec::new();
        for (build_id, waiting) in &mut self.waiting {
            if now.saturating_duration_since(waiting.last_notice) >= WAIT_NOTICE {
                waiting.last_notice = now;
                let minutes = now.saturating_duration_since(waiting.first_at).as_secs() / 60;
                notices.push((build_id.clone(), minutes, waiting.reason.clone()));
            }
        }
        for (build_id, minutes, reason) in notices {
            let row = match self.store.build(&build_id).await {
                Ok(Some(row)) => row,
                Ok(None) => continue,
                Err(e) => {
                    warn!(build_id, error = %e, "could not read a waiting build's row");
                    continue;
                }
            };
            let line = format!("still waiting for node capacity after {minutes} min: {reason}");
            if let Err(e) = self
                .logs
                .append(&row.app_id, &row.build_id, LogStream::Stdout, None, &line)
                .await
            {
                warn!(build_id, error = %e, "could not append to a build log");
            }
        }
    }

    /// Creates a build's row the first time the snapshot names it.
    async fn ensure_row(&self, build: &SpecBuild) -> Result<(), AgentError> {
        if self.store.build(&build.build_id).await?.is_some() {
            return Ok(());
        }
        let log_path = self.logs.path(&build.app_id, &build.build_id)?;
        let row = BuildRecord {
            build_id: build.build_id.clone(),
            app_id: build.app_id.clone(),
            status: BuildState::Queued,
            failed_phase: None,
            commit: None,
            image_digest: None,
            error: None,
            failed_step: None,
            log_path: Some(log_path.display().to_string()),
            created_at: now(),
            started_at: None,
            finished_at: None,
        };
        self.store.save_build(&row).await
    }

    /// Keeps the last [`KEPT_BUILDS_PER_APP`] terminal builds of one app and
    /// deletes the older logs with their rows. A build the snapshot still names
    /// is never removed, because its status is what the primary is waiting for.
    async fn prune_app(&self, app_id: &str, desired: &BTreeSet<String>) {
        let rows = match self.store.builds_for_app(app_id).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(app_id, error = %e, "could not read an app's build rows");
                return;
            }
        };
        let terminal: Vec<&BuildRecord> = rows
            .iter()
            .filter(|row| row.status.is_terminal() && !desired.contains(&row.build_id))
            .collect();
        if terminal.len() <= KEPT_BUILDS_PER_APP {
            return;
        }
        let excess = terminal.len() - KEPT_BUILDS_PER_APP;
        for row in terminal.into_iter().take(excess) {
            if let Err(e) = self.logs.remove_build(&row.app_id, &row.build_id).await {
                warn!(build_id = %row.build_id, error = %e, "could not delete an old build log");
            }
            if let Err(e) = self.store.delete_build(&row.build_id).await {
                warn!(build_id = %row.build_id, error = %e, "could not delete an old build row");
            }
        }
    }

    /// Removes the cache disk, logs and rows of every app that is in neither
    /// `spec.vms` nor `spec.builds` (section 6.6), unless one of its builds
    /// still runs.
    async fn cleanup_caches(&mut self, spec: &Spec) {
        let mut apps: BTreeSet<String> = BTreeSet::new();
        let cache_dir = self.store.data_dir().join("builds").join("cache");
        match tokio::fs::read_dir(&cache_dir).await {
            Ok(mut entries) => {
                // The builder keeps one `<app_id>.ext4` per app; anything
                // else in the directory is not a cache disk.
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Some(app_id) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_suffix(".ext4"))
                        .filter(|app_id| app_id.starts_with("app_"))
                    {
                        apps.insert(app_id.to_owned());
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!(error = %e, "could not read the build cache directory"),
        }
        if let Ok(rows) = self.store.builds().await {
            for row in rows {
                apps.insert(row.app_id);
            }
        }

        let desired: HashSet<&str> = spec
            .vms
            .iter()
            .filter_map(|vm| vm.app_id.as_deref())
            .chain(spec.builds.iter().map(|build| build.app_id.as_str()))
            .collect();
        let running_app = self.running.as_ref().map(|running| running.app_id.clone());

        for app_id in apps {
            if desired.contains(app_id.as_str()) {
                continue;
            }
            if running_app.as_deref() == Some(app_id.as_str()) {
                continue;
            }
            info!(
                app_id,
                "removing the build cache of an app that left the node"
            );
            self.builder.remove_cache(&app_id).await;
            if let Err(e) = self.logs.remove_app(&app_id).await {
                warn!(app_id, error = %e, "could not delete an app's build logs");
            }
            if let Err(e) = self.store.delete_builds_for_app(&app_id).await {
                warn!(app_id, error = %e, "could not delete an app's build rows");
            }
        }
    }

    fn is_running(&self, build_id: &str) -> bool {
        self.running
            .as_ref()
            .is_some_and(|running| running.build_id == build_id)
    }

    fn signal_cancel(&self, build_id: &str) {
        if let Some(running) = self.running.as_ref()
            && running.build_id == build_id
        {
            // The builder stops its VM and returns a cancelled failure; a
            // receiver that is already gone is not an error.
            let _ = running.cancel.send(true);
        }
    }

    /// Reports a queued build `cancelled` without running it.
    async fn cancel_build(&mut self, row: &BuildRecord) {
        self.builder.cleanup(&row.build_id).await;
        if let Err(e) = self.store.forget_vm(&row.build_id).await {
            warn!(build_id = %row.build_id, error = %e, "could not release a cancelled build's address");
        }
        let mut row = row.clone();
        row.status = BuildState::Cancelled;
        row.finished_at = Some(now());
        if let Err(e) = self.store.save_build(&row).await {
            warn!(build_id = %row.build_id, error = %e, "could not record a cancelled build");
        }
        self.finish_log(&row, "build cancelled").await;
    }

    /// Starts one build: reserves its address, builds the [`BuildSpec`] and
    /// spawns the task that runs it.
    async fn start(
        &mut self,
        build: &SpecBuild,
        row: BuildRecord,
        subnet: &Subnet,
        done: &mpsc::Sender<Done>,
        waiting: bool,
    ) {
        let used: HashSet<Ipv4Addr> = self
            .store
            .used_ips()
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();
        let ip = subnet.allocate_high(&used);
        if let Err(e) = self.store.record_created(&build.build_id, ip, 0).await {
            warn!(build_id = %build.build_id, error = %e, "could not reserve a builder address");
            return;
        }
        let spec = BuildSpec {
            build_id: build.build_id.clone(),
            app_id: build.app_id.clone(),
            source: BuildSource::Git {
                url: build.repo_url.clone(),
                git_ref: build.git_ref.clone(),
            },
            frontend: Frontend::Dockerfile {
                path: build.dockerfile_path.clone(),
                target: build.target.clone(),
            },
            context_dir: build.context_dir.clone(),
            build_args: build.build_args.clone(),
            secrets: build.secrets.clone(),
            limits: build.limits,
            refresh_base: build.refresh_base,
            net: BuilderNet {
                ip,
                gateway: subnet.gateway,
            },
        };
        let (cancel, cancel_rx) = watch::channel(false);
        info!(build_id = %build.build_id, app_id = %build.app_id, %ip, "starting a build");
        let task = BuildTask {
            engine: Arc::clone(&self.engine),
            builder: Arc::clone(&self.builder),
            store: self.store.clone(),
            logs: Arc::clone(&self.logs),
            changes: self.changes.clone(),
            done: done.clone(),
            inventory: self.inventory.clone(),
        };
        tokio::spawn(task.run(row, spec, cancel_rx, waiting));
        self.running = Some(Running {
            build_id: build.build_id.clone(),
            app_id: build.app_id.clone(),
            cancel,
        });
    }

    /// Recomputes `status_report.builds`: one entry per build in the current
    /// `spec.builds`, sorted by build id (section 5.3).
    async fn refresh_statuses(&self) {
        let mut statuses = Vec::new();
        if let Some(snapshot) = &self.current {
            let mut builds: Vec<&SpecBuild> = snapshot.state.spec.builds.iter().collect();
            builds.sort_by(|a, b| a.build_id.cmp(&b.build_id));
            for build in builds {
                match self.store.build(&build.build_id).await {
                    Ok(Some(row)) => statuses.push(row.to_status()),
                    Ok(None) => statuses.push(BuildStatus {
                        build_id: build.build_id.clone(),
                        status: BuildState::Queued,
                        commit: None,
                        image_digest: None,
                        failed_phase: None,
                        error: None,
                        failed_step: None,
                        started_at: None,
                        finished_at: None,
                    }),
                    Err(e) => {
                        warn!(build_id = %build.build_id, error = %e, "could not read a build row");
                    }
                }
            }
        }
        if *self.statuses.borrow() != statuses {
            self.statuses.send_replace(statuses);
        }
    }

    /// Appends the terminal line to a build's log and closes it.
    async fn finish_log(&self, row: &BuildRecord, line: &str) {
        if let Err(e) = self
            .logs
            .append(&row.app_id, &row.build_id, LogStream::Stdout, None, line)
            .await
        {
            warn!(build_id = %row.build_id, error = %e, "could not append to a build log");
        }
        if let Err(e) = self.logs.finish(&row.app_id, &row.build_id).await {
            warn!(build_id = %row.build_id, error = %e, "could not close a build log");
        }
    }
}

/// One build's task: it owns the row while the build runs and reports the
/// outcome.
struct BuildTask<E: Engine, B: Builder> {
    engine: Arc<E>,
    builder: Arc<B>,
    store: NodeStore,
    logs: Arc<BuildLogs>,
    changes: watch::Sender<u64>,
    done: mpsc::Sender<Done>,
    inventory: Option<crate::inventory::InventoryHub>,
}

impl<E: Engine, B: Builder> BuildTask<E, B> {
    /// Runs one build from `queued` to a terminal state (or back to `queued`
    /// when the node has no capacity). `waiting` is true for a retry of a
    /// build that is already waiting: it leaves `queued` only when the builder
    /// reports that its VM exists.
    async fn run(
        self,
        mut row: BuildRecord,
        spec: BuildSpec,
        cancel: watch::Receiver<bool>,
        waiting: bool,
    ) {
        if !waiting {
            row.status = BuildState::Preparing;
            if row.started_at.is_none() {
                row.started_at = Some(now());
            }
            self.save(&row).await;
        }

        let (events_tx, mut events) = mpsc::channel(256);
        let build = self.builder.build(&spec, events_tx, cancel);
        tokio::pin!(build);
        let mut events_open = true;
        let outcome = loop {
            tokio::select! {
                outcome = &mut build => break outcome,
                event = async {
                    if events_open {
                        events.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match event {
                        Some(event) => self.on_event(&mut row, event).await,
                        None => events_open = false,
                    }
                }
            }
        };
        // Everything the builder queued before returning still reaches the
        // log and the row.
        while let Ok(event) = events.try_recv() {
            self.on_event(&mut row, event).await;
        }

        match outcome {
            Ok(output) => self.import(&mut row, output).await,
            Err(failure) if failure.cancelled => {
                row.status = BuildState::Cancelled;
                self.complete(&mut row, "build cancelled").await;
            }
            Err(failure) if failure.error.starts_with(CAPACITY_PREFIX) => {
                let reason = failure
                    .error
                    .strip_prefix(CAPACITY_PREFIX)
                    .unwrap_or_default()
                    .to_owned();
                row.status = BuildState::Queued;
                row.started_at = None;
                self.save(&row).await;
                // Keep the run directory and the prepared source: only the
                // address reservation belongs to this attempt.
                self.forget_address(&row.build_id).await;
                let _ = self
                    .done
                    .send(Done {
                        build_id: row.build_id.clone(),
                        capacity: Some(reason),
                    })
                    .await;
            }
            Err(failure) => self.fail(&mut row, failure).await,
        }
    }

    /// Applies one builder event to the row and the log.
    async fn on_event(&self, row: &mut BuildRecord, event: BuildEvent) {
        match event {
            BuildEvent::Phase(phase) => {
                row.status = state_of(phase);
                if row.started_at.is_none() {
                    row.started_at = Some(now());
                }
                self.save(row).await;
            }
            BuildEvent::Commit(commit) => {
                row.commit = Some(commit);
                self.save(row).await;
            }
            BuildEvent::Line {
                stream,
                vertex,
                text,
            } => {
                if let Err(e) = self
                    .logs
                    .append(&row.app_id, &row.build_id, stream, vertex, &text)
                    .await
                {
                    warn!(build_id = %row.build_id, error = %e, "could not append to a build log");
                }
            }
        }
    }

    /// Imports a successful build's layout (section 6.6, `importing`).
    async fn import(&self, row: &mut BuildRecord, output: BuildOutput) {
        row.status = BuildState::Importing;
        self.save(row).await;
        let layout_digest = output.layout.manifest_digest.clone();
        match self.engine.prepare_layout(&output.layout).await {
            Ok(prepared) if prepared.digest == layout_digest => {
                if let Some(hub) = &self.inventory {
                    hub.notify_prepared(prepared.digest.clone());
                }
                row.status = BuildState::Succeeded;
                row.image_digest = Some(prepared.digest.0.clone());
                let line = format!("build succeeded: {}", prepared.digest.0);
                self.complete(row, &line).await;
            }
            Ok(prepared) => {
                let error = format!(
                    "the prepared image is {}, not the layout's {}",
                    prepared.digest.0, layout_digest.0
                );
                self.fail(
                    row,
                    BuildFailure {
                        phase: BuildPhase::Importing,
                        error,
                        step: None,
                        cancelled: false,
                    },
                )
                .await;
            }
            Err(e) => {
                self.fail(
                    row,
                    BuildFailure {
                        phase: BuildPhase::Importing,
                        error: e.to_string(),
                        step: None,
                        cancelled: false,
                    },
                )
                .await;
            }
        }
    }

    /// Records a failure: phase, bounded reason and bounded failing step, then
    /// the terminal line.
    async fn fail(&self, row: &mut BuildRecord, failure: BuildFailure) {
        row.status = BuildState::Failed;
        row.failed_phase = Some(failure.phase);
        row.error = Some(truncate_chars(&failure.error, MAX_ERROR_CHARS));
        row.failed_step = failure.step.map(sanitize_step);
        let reason = row.error.clone().unwrap_or_default();
        let line = format!("build failed in {}: {reason}", failure.phase.as_str());
        self.complete(row, &line).await;
    }

    /// Ends a build: `finished_at`, the terminal log line, the cleanup and the
    /// signal that the next one may start.
    async fn complete(&self, row: &mut BuildRecord, line: &str) {
        row.finished_at = Some(now());
        // Clean up before the terminal row is visible, so a caller that sees
        // the build end already knows its ephemeral parts are gone.
        self.release(&row.build_id).await;
        // The terminal line goes into the file before the row turns terminal:
        // a `build_logs` follow ends once it reads a terminal row and drains
        // the file, so a line written after the row would never reach it
        // (section 5.15).
        if let Err(e) = self
            .logs
            .append(&row.app_id, &row.build_id, LogStream::Stdout, None, line)
            .await
        {
            warn!(build_id = %row.build_id, error = %e, "could not append to a build log");
        }
        if let Err(e) = self.logs.finish(&row.app_id, &row.build_id).await {
            warn!(build_id = %row.build_id, error = %e, "could not close a build log");
        }
        self.save(row).await;
        let _ = self
            .done
            .send(Done {
                build_id: row.build_id.clone(),
                capacity: None,
            })
            .await;
    }

    /// Removes the ephemeral parts and releases the builder's address.
    async fn release(&self, build_id: &str) {
        self.builder.cleanup(build_id).await;
        self.forget_address(build_id).await;
    }

    /// Releases only the builder's address reservation, keeping the run
    /// directory for the next capacity retry.
    async fn forget_address(&self, build_id: &str) {
        if let Err(e) = self.store.forget_vm(build_id).await {
            warn!(build_id, error = %e, "could not release a builder address");
        }
    }

    async fn save(&self, row: &BuildRecord) {
        match self.store.save_build(row).await {
            Ok(()) => {
                self.changes
                    .send_modify(|generation| *generation = generation.wrapping_add(1));
            }
            Err(e) => warn!(build_id = %row.build_id, error = %e, "could not record a build row"),
        }
    }
}

/// The handle the session and the binary use to read build status and logs.
#[derive(Clone)]
pub struct BuildsHandle {
    store: NodeStore,
    logs: Arc<BuildLogs>,
    statuses: watch::Receiver<Vec<BuildStatus>>,
}

impl BuildsHandle {
    /// A receiver of the current `status_report.builds`, updated on every
    /// change.
    #[must_use]
    pub fn statuses(&self) -> watch::Receiver<Vec<BuildStatus>> {
        self.statuses.clone()
    }

    /// The current entries, without waiting.
    #[must_use]
    pub fn current(&self) -> Vec<BuildStatus> {
        self.statuses.borrow().clone()
    }

    /// Whether this node has the build at all.
    ///
    /// # Errors
    ///
    /// [`BuildsError::Store`] when the row cannot be read.
    pub async fn has_build(&self, build_id: &str) -> Result<bool, BuildsError> {
        match self.store.build(build_id).await {
            Ok(row) => Ok(row.is_some()),
            Err(e) => Err(BuildsError::Store(e.to_string())),
        }
    }

    /// The lines after `after_seq`, oldest first, at most `limit` (at most
    /// [`MAX_QUERY_LIMIT`]) (section 5.17).
    ///
    /// # Errors
    ///
    /// [`BuildsError::NotFound`] when the node has no such build, and
    /// [`BuildsError::Store`] when its log cannot be read.
    pub async fn query(
        &self,
        build_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<LogLine>, BuildsError> {
        let row = self.row(build_id).await?;
        let limit = limit.min(MAX_QUERY_LIMIT);
        let lines = self
            .logs
            .lines_after(&row.app_id, build_id, after_seq, limit)
            .await
            .map_err(|e| BuildsError::Store(e.to_string()))?;
        Ok(lines.into_iter().map(to_wire).collect())
    }

    /// Follows a build's log: the replay according to `start`, then every new
    /// line. The receiver ends once the build is terminal and every line was
    /// sent (section 5.15), and never yields a line twice.
    ///
    /// # Errors
    ///
    /// [`BuildsError::NotFound`] when the node has no such build.
    pub async fn follow(
        &self,
        build_id: &str,
        start: BuildStart,
    ) -> Result<mpsc::Receiver<Vec<LogLine>>, BuildsError> {
        let row = self.row(build_id).await?;
        // Subscribe before the replay: a line written between the replay and
        // the live part is then either in the replay or in the channel.
        let live = self.logs.subscribe();
        let (tx, rx) = mpsc::channel(8);
        let store = self.store.clone();
        let logs = Arc::clone(&self.logs);
        let build_id = build_id.to_owned();
        tokio::spawn(async move {
            follow_task(store, logs, row.app_id, build_id, start, live, tx).await;
        });
        Ok(rx)
    }

    async fn row(&self, build_id: &str) -> Result<BuildRecord, BuildsError> {
        match self.store.build(build_id).await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(BuildsError::NotFound),
            Err(e) => Err(BuildsError::Store(e.to_string())),
        }
    }
}

/// Reads a build log for one stream. It ends when the build is terminal and
/// the file has nothing newer to send.
async fn follow_task(
    store: NodeStore,
    logs: Arc<BuildLogs>,
    app_id: String,
    build_id: String,
    start: BuildStart,
    mut live: broadcast::Receiver<StoredBuildLine>,
    tx: mpsc::Sender<Vec<LogLine>>,
) {
    let mut all = match logs.lines_after(&app_id, &build_id, 0, usize::MAX).await {
        Ok(lines) => lines,
        Err(e) => {
            warn!(build_id, error = %e, "could not read a build log");
            Vec::new()
        }
    };
    let newest = all.last().map_or(0, |line| line.seq);
    let replay: Vec<StoredBuildLine> = match start {
        BuildStart::Tail(n) => all.split_off(all.len().saturating_sub(n)),
        BuildStart::After(seq) => all.into_iter().filter(|line| line.seq > seq).collect(),
    };
    let mut last = newest;
    let mut batch: Vec<LogLine> = Vec::new();
    for line in replay {
        batch.push(to_wire(line));
    }
    if !flush(&tx, &mut batch).await {
        return;
    }

    let mut ticker = tokio::time::interval(FOLLOW_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            received = live.recv() => {
                match received {
                    Ok(line) if line.build_id == build_id && line.seq > last => {
                        last = line.seq;
                        batch.push(to_wire(line));
                        if batch.len() >= FOLLOW_BATCH && !flush(&tx, &mut batch).await {
                            return;
                        }
                    }
                    Ok(_) => {}
                    // A lagged reader catches up from the file below, and the
                    // next tick sends what is missing.
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = ticker.tick() => {
                if !flush(&tx, &mut batch).await {
                    return;
                }
                let terminal = match store.build(&build_id).await {
                    Ok(Some(row)) => row.status.is_terminal(),
                    Ok(None) => true,
                    Err(e) => {
                        warn!(build_id, error = %e, "could not read a build row");
                        false
                    }
                };
                if terminal {
                    // The file is the source of truth: read whatever the live
                    // channel did not deliver before ending.
                    match logs.lines_after(&app_id, &build_id, last, usize::MAX).await {
                        Ok(lines) => {
                            for line in lines {
                                batch.push(to_wire(line));
                            }
                        }
                        Err(e) => warn!(build_id, error = %e, "could not read a build log"),
                    }
                    flush(&tx, &mut batch).await;
                    break;
                }
            }
        }
    }
}

/// Sends one batch. Returns `false` when the reader is gone.
async fn flush(tx: &mpsc::Sender<Vec<LogLine>>, batch: &mut Vec<LogLine>) -> bool {
    if batch.is_empty() {
        return true;
    }
    let lines = std::mem::take(batch);
    tx.send(lines).await.is_ok()
}

/// The wire shape of one stored build line: `seq`, `vertex` and no `vm_id`
/// (section 5.7).
fn to_wire(line: StoredBuildLine) -> LogLine {
    LogLine {
        time: util::rfc3339_nanos(line.time_ns),
        stream: line.stream,
        line: line.line,
        vm_id: None,
        seq: Some(line.seq),
        vertex: line.vertex,
    }
}

/// Bounds a failing step from the untrusted builder output (section 5.3).
fn sanitize_step(step: FailedStep) -> FailedStep {
    FailedStep {
        vertex: step.vertex,
        name: truncate_chars(&step.name, MAX_EXCERPT_CHARS),
        error: truncate_chars(&step.error, MAX_ERROR_CHARS),
        dockerfile_line: step.dockerfile_line,
        excerpt: step
            .excerpt
            .into_iter()
            .take(MAX_EXCERPT_LINES)
            .map(|line| truncate_chars(&line, MAX_EXCERPT_CHARS))
            .collect(),
    }
}

/// The state a phase event reports.
fn state_of(phase: BuildPhase) -> BuildState {
    match phase {
        BuildPhase::Preparing => BuildState::Preparing,
        BuildPhase::Building => BuildState::Building,
        BuildPhase::Exporting => BuildState::Exporting,
        BuildPhase::Importing => BuildState::Importing,
    }
}

/// The phase a non-terminal state was in, for the restart failure.
fn phase_of(state: BuildState) -> BuildPhase {
    match state {
        BuildState::Queued | BuildState::Preparing => BuildPhase::Preparing,
        BuildState::Building => BuildPhase::Building,
        BuildState::Exporting => BuildPhase::Exporting,
        BuildState::Importing => BuildPhase::Importing,
        // Neither queued nor terminal rows reach here.
        BuildState::Succeeded | BuildState::Failed | BuildState::Cancelled => BuildPhase::Preparing,
    }
}

fn now() -> String {
    util::rfc3339_nanos(util::now_ns())
}

/// The wait before the attempt after `attempts` refusals: `base`, doubled
/// per refusal, never above `max`.
fn capacity_wait(base: Duration, max: Duration, attempts: u32) -> Duration {
    let doublings = attempts.saturating_sub(1).min(20);
    base.saturating_mul(1u32 << doublings).min(max)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    use quoro_engine::{EngineError, FakeEngine, FakeOp};
    use quoro_proto::wire::{BuildLimits, DesiredState, Spec, SpecNetwork};
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::build::buildkit::{
        BuilderConfig, DEFAULT_FETCH_TIMEOUT, DockerfileBuilder, find_git, pinned_image,
    };
    use crate::build::fake::{FakeBuilder, FakeOutcome};
    use crate::build::log::BuildLogs;
    use crate::reconcile::Snapshot;

    const APP: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E";
    const BUILD: &str = "bld_01JC9ZA2B3C4D5E6F7G8H9J0KM";
    const WAIT: Duration = Duration::from_secs(10);

    /// A unique temporary directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "quoro-manager-{}-{}-{}",
                std::process::id(),
                util::now_ns(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("a temporary directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn spec_build() -> SpecBuild {
        SpecBuild {
            build_id: BUILD.to_owned(),
            app_id: APP.to_owned(),
            project_id: "prj_1".to_owned(),
            repo_url: "https://github.com/example/blog.git".to_owned(),
            git_ref: "main".to_owned(),
            dockerfile_path: "Dockerfile".to_owned(),
            context_dir: ".".to_owned(),
            target: None,
            build_args: BTreeMap::new(),
            secrets: BTreeMap::new(),
            limits: BuildLimits::default(),
            cancelled: false,
            refresh_base: false,
        }
    }

    fn snapshot(rev: u64, builds: Vec<SpecBuild>) -> Snapshot {
        Snapshot {
            rev,
            state: DesiredState {
                rev,
                issued_at: "2026-11-04T00:00:00Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms: Vec::new(),
                    routes: Vec::new(),
                    network: Some(SpecNetwork {
                        subnet: "10.42.1.0/24".to_owned(),
                        gateway: "10.42.1.1".to_owned(),
                    }),
                    ingress: None,
                    builds,
                    mesh: None,
                    edge: None,
                    volumes: Vec::new(),
                    backups: Vec::new(),
                    backup_repository: None,
                    votes: None,
                    witness: None,
                    quarantine_deletes: Vec::new(),
                },
                signature: String::new(),
            },
        }
    }

    async fn stop_future(mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            if stop.changed().await.is_err() {
                return;
            }
        }
    }

    /// A running manager over a scratch store, its handle and its snapshot
    /// channel.
    struct Harness {
        dir: TempDir,
        builder: Arc<FakeBuilder>,
        store: NodeStore,
        handle: BuildsHandle,
        snapshots: watch::Sender<Option<Snapshot>>,
        stop: watch::Sender<bool>,
        task: JoinHandle<()>,
    }

    impl Harness {
        async fn new(builder: FakeBuilder, engine: Arc<FakeEngine>) -> Self {
            let dir = TempDir::new();
            let store = NodeStore::open(dir.path())
                .await
                .expect("open the node store");
            let builder = Arc::new(builder);
            let (manager, handle) =
                BuildManager::new(Arc::clone(&engine), Arc::clone(&builder), store.clone());
            let manager = manager
                .with_tick(Duration::from_millis(5))
                .with_wait_backoff(Duration::from_millis(100), Duration::from_millis(400));
            let (snapshots, snapshots_rx) = watch::channel(None);
            let (stop, stop_rx) = watch::channel(false);
            let task = tokio::spawn(manager.run(snapshots_rx, stop_future(stop_rx)));
            Self {
                dir,
                builder,
                store,
                handle,
                snapshots,
                stop,
                task,
            }
        }

        fn publish(&self, rev: u64, builds: Vec<SpecBuild>) {
            self.snapshots.send_replace(Some(snapshot(rev, builds)));
        }

        async fn wait_builds(&self, count: usize) {
            let waited = tokio::time::timeout(WAIT, async {
                while self.builder.build_count() < count {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await;
            assert!(waited.is_ok(), "the builder never ran {count} build(s)");
        }

        async fn wait_terminal(&self, build_id: &str) -> BuildRecord {
            let found = tokio::time::timeout(WAIT, async {
                loop {
                    if let Ok(Some(row)) = self.store.build(build_id).await
                        && row.status.is_terminal()
                    {
                        return row;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            found.unwrap_or_else(|_| panic!("build {build_id} never ended"))
        }

        async fn log_lines(&self, build_id: &str) -> Vec<String> {
            let logs = BuildLogs::new(self.dir.path().join("builds").join("logs"));
            logs.lines_after(APP, build_id, 0, usize::MAX)
                .await
                .expect("read the build log")
                .into_iter()
                .map(|line| line.line)
                .collect()
        }

        async fn shutdown(self) {
            let _ = self.stop.send(true);
            let finished = tokio::time::timeout(WAIT, self.task).await;
            assert!(finished.is_ok(), "the build manager did not stop");
        }
    }

    #[test]
    fn capacity_backoff_doubles_and_caps() {
        let waits: Vec<u64> = (1..=6)
            .map(|attempts| capacity_wait(WAIT_BASE, WAIT_MAX, attempts).as_secs())
            .collect();
        assert_eq!(waits, vec![5, 10, 20, 40, 60, 60]);
    }

    #[tokio::test]
    async fn a_waiting_build_stays_queued_and_logs_one_waiting_line() {
        let builder = FakeBuilder::new().with_delay(Duration::ZERO);
        for _ in 0..4 {
            builder.script(BUILD, FakeOutcome::CapacityOnce("sha256:bb".to_owned()));
        }
        let harness = Harness::new(builder, Arc::new(FakeEngine::new())).await;
        harness.publish(1, vec![spec_build()]);

        // The first retry starts while the row is still queued: the manager
        // must not flip it to `preparing` on an attempt that only waits.
        harness.wait_builds(2).await;
        let row = harness.store.build(BUILD).await.unwrap().unwrap();
        assert_eq!(row.status, BuildState::Queued);
        assert!(row.started_at.is_none(), "a waiting build never started");
        assert!(
            harness
                .handle
                .current()
                .iter()
                .any(|status| status.build_id == BUILD && status.status == BuildState::Queued),
            "the status report keeps it queued too"
        );

        let row = harness.wait_terminal(BUILD).await;
        assert_eq!(row.status, BuildState::Succeeded);
        // The terminal row is saved just before its last log line.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let lines = harness.log_lines(BUILD).await;
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("waiting for node capacity: "))
                .count(),
            1,
            "one waiting line for the whole wait: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .all(|line| !line.starts_with("preparing the builder image")),
            "a waiting retry writes no preparing line: {lines:?}"
        );
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.as_str() == "fetching the source")
                .count(),
            1,
            "the successful attempt writes the fetch line once: {lines:?}"
        );
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn a_build_runs_once_capacity_frees_and_logs_the_normal_lines_once() {
        let builder = FakeBuilder::new().with_delay(Duration::ZERO);
        builder.script(BUILD, FakeOutcome::CapacityOnce("sha256:bb".to_owned()));
        let harness = Harness::new(builder, Arc::new(FakeEngine::new())).await;
        harness.publish(1, vec![spec_build()]);

        let row = harness.wait_terminal(BUILD).await;
        assert_eq!(row.status, BuildState::Succeeded);
        assert_eq!(row.image_digest.as_deref(), Some("sha256:bb"));
        // The terminal row is saved just before its last log line.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let lines = harness.log_lines(BUILD).await;
        for line in [
            "fetching the source",
            "#1 [1/1] RUN echo built",
            "writing the OCI layout",
        ] {
            assert_eq!(
                lines
                    .iter()
                    .filter(|candidate| candidate.as_str() == line)
                    .count(),
                1,
                "{line} appears once: {lines:?}"
            );
        }
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("build succeeded:"))
                .count(),
            1
        );
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.starts_with("waiting for node capacity: "))
                .count(),
            1
        );
        harness.shutdown().await;
    }

    /// A local Git repository the real builder can fetch from, and its URL.
    fn git_fixture() -> Option<(TempDir, String)> {
        let git = find_git()?;
        let dir = TempDir::new();
        let work = dir.path().join("repo");
        std::fs::create_dir_all(&work).expect("a work tree");
        std::fs::write(work.join("Dockerfile"), b"FROM scratch\n").expect("a Dockerfile");
        let run = |args: &[&str]| {
            let output = Command::new(&git)
                .args([
                    "-c",
                    "user.name=Quoro Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                ])
                .arg("-C")
                .arg(&work)
                .args(args)
                .output()
                .expect("git runs");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(&["init", "--quiet", "--initial-branch=main"]);
        run(&["add", "--all"]);
        run(&["commit", "--quiet", "-m", "initial"]);
        Some((dir, format!("file://{}", work.display())))
    }

    #[tokio::test]
    async fn cancelling_a_waiting_build_removes_its_run_directory() {
        let Some((fixture, url)) = git_fixture() else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let dir = TempDir::new();
        let store = NodeStore::open(dir.path())
            .await
            .expect("open the node store");
        let engine = Arc::new(FakeEngine::new());
        engine.fail_next(
            FakeOp::Create,
            EngineError::ResourceExhausted("no room for another VM".to_owned()),
        );
        let builds_dir = dir.path().join("builds");
        let builder = Arc::new(DockerfileBuilder::new(
            Arc::clone(&engine),
            BuilderConfig {
                builds_dir: builds_dir.clone(),
                git: find_git(),
                image: pinned_image(),
                fetch_timeout: DEFAULT_FETCH_TIMEOUT,
            },
        ));
        let (manager, handle) =
            BuildManager::new(Arc::clone(&engine), Arc::clone(&builder), store.clone());
        // A long back-off keeps the build waiting until the cancel arrives.
        let manager = manager
            .with_tick(Duration::from_millis(5))
            .with_wait_backoff(Duration::from_secs(60), Duration::from_secs(60));
        let (snapshots, snapshots_rx) = watch::channel(None);
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(manager.run(snapshots_rx, stop_future(stop_rx)));

        let mut build = spec_build();
        build.repo_url = url;
        snapshots.send_replace(Some(snapshot(1, vec![build.clone()])));

        let run_dir = builds_dir.join("run").join(BUILD);
        let waited = tokio::time::timeout(WAIT, async {
            loop {
                // The manager creates the run directory before it writes
                // the source disk, so a slow runner can see the directory
                // alone; wait for both (issue #376).
                if let Ok(Some(row)) = store.build(BUILD).await
                    && row.status == BuildState::Queued
                    && run_dir.is_dir()
                    && run_dir.join("src.disk").is_file()
                    && row.started_at.is_none()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            waited.is_ok(),
            "the build never waited with a run directory and its source disk"
        );

        let mut cancelled = build;
        cancelled.cancelled = true;
        snapshots.send_replace(Some(snapshot(2, vec![cancelled])));

        let ended = tokio::time::timeout(WAIT, async {
            loop {
                if let Ok(Some(row)) = store.build(BUILD).await
                    && row.status == BuildState::Cancelled
                {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the waiting build is cancelled");
        assert_eq!(ended.status, BuildState::Cancelled);
        assert!(!run_dir.exists(), "cancel removes the run directory");
        assert!(
            store
                .used_ips()
                .await
                .expect("the address table")
                .is_empty(),
            "the builder address is released"
        );
        assert!(!handle.current().is_empty());
        let _ = stop.send(true);
        let _ = tokio::time::timeout(WAIT, task).await;
        drop(fixture);
    }
}
