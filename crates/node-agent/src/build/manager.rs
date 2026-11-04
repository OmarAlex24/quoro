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

use std::collections::{BTreeSet, HashSet};
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
    current: Option<Snapshot>,
    tick: Duration,
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
            current: None,
            tick: DEFAULT_TICK,
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
        let (done_tx, mut done_rx) = mpsc::channel::<String>(8);
        let mut ticker = tokio::time::interval(self.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        self.pass(&done_tx).await;
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.current = snapshots.borrow_and_update().clone();
                    self.pass(&done_tx).await;
                }
                _ = ticker.tick() => {
                    self.pass(&done_tx).await;
                }
                changed = changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.refresh_statuses().await;
                }
                done = done_rx.recv() => {
                    let mut start_next = true;
                    if let Some(build_id) = done {
                        if self.running.as_ref().is_some_and(|running| running.build_id == build_id) {
                            self.running = None;
                        }
                        // A build that went back to `queued` (the node had no
                        // capacity) is retried on the next tick, never in a
                        // tight loop with the pass that just ran it.
                        if let Ok(Some(row)) = self.store.build(&build_id).await
                            && row.status == BuildState::Queued
                        {
                            start_next = false;
                        }
                    }
                    if start_next {
                        // A finished build frees the node for the next one now
                        // rather than at the next tick.
                        self.pass(&done_tx).await;
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
            if let Err(e) = self.store.save_build(&row).await {
                warn!(build_id = %row.build_id, error = %e, "could not record a failed build");
            }
            let line = format!("build failed in {}: node agent restarted", phase.as_str());
            self.finish_log(&row, &line).await;
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
                    ) && let Err(e) = self.engine.stop(&info.id, StopMode::Force).await
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
    /// and the status report (section 6.2 step 5).
    async fn pass(&mut self, done: &mpsc::Sender<String>) {
        let Some(snapshot) = self.current.clone() else {
            self.refresh_statuses().await;
            return;
        };
        let spec = &snapshot.state.spec;
        let subnet = Subnet::parse(&spec.network.subnet, &spec.network.gateway);
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
        }

        // One build at a time per node (ADR 0118): the oldest `queued` one in
        // spec order.
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
                match subnet {
                    Some(subnet) => self.start(build, row, &subnet, done).await,
                    None => debug!(
                        subnet = %spec.network.subnet,
                        "the desired network is not a usable subnet; no build starts"
                    ),
                }
                break;
            }
        }

        self.refresh_statuses().await;
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
        done: &mpsc::Sender<String>,
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
        };
        tokio::spawn(task.run(row, spec, cancel_rx));
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
    done: mpsc::Sender<String>,
}

impl<E: Engine, B: Builder> BuildTask<E, B> {
    /// Runs one build from `queued` to a terminal state (or back to `queued`
    /// when the node has no capacity).
    async fn run(self, mut row: BuildRecord, spec: BuildSpec, cancel: watch::Receiver<bool>) {
        row.status = BuildState::Preparing;
        if row.started_at.is_none() {
            row.started_at = Some(now());
        }
        self.save(&row).await;

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
                debug!(build_id = %row.build_id, "the node has no capacity for the build; it stays queued");
                row.status = BuildState::Queued;
                row.started_at = None;
                self.save(&row).await;
                self.release(&row.build_id).await;
                let _ = self.done.send(row.build_id.clone()).await;
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
        self.save(row).await;
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
        self.release(&row.build_id).await;
        let _ = self.done.send(row.build_id.clone()).await;
    }

    /// Removes the ephemeral parts and releases the builder's address.
    async fn release(&self, build_id: &str) {
        self.builder.cleanup(build_id).await;
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
