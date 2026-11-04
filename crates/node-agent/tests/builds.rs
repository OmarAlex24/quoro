// SPDX-License-Identifier: AGPL-3.0-only
//! The build manager, its log store and its handle
//! (`node-agent-protocol.md` section 6.6, ADR 0117, ADR 0118, ADR 0119).
//!
//! Every test drives a real [`NodeStore`] in a scratch directory and the
//! in-memory engine and builder, so the loop, the rows, the log files and the
//! status report are exercised together.

mod agent_common;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use agent_common::{GATEWAY, TempDir, snapshot_with, spec_build};
use quoro_engine::{
    ConsoleMode, Engine, FakeEngine, ImageDigest, ImageRef, InitConfig, MacAddr, NetAttach, VmId,
    VmSpec,
};
use quoro_node_agent::build::fake::{DEFAULT_DIGEST, FakeBuilder, FakeOutcome};
use quoro_node_agent::build::log::BuildLogs;
use quoro_node_agent::build::manager::{
    BuildManager, BuildStart, BuildsHandle, KEPT_BUILDS_PER_APP,
};
use quoro_node_agent::store::{BuildRecord, NodeStore};
use quoro_proto::wire::{BuildLimits, BuildPhase, BuildState, BuildStatus, FailedStep, LogStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const APP: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E";
const BUILD: &str = "bld_01JC9ZA2B3C4D5E6F7G8H9J0KM";
const WAIT: Duration = Duration::from_secs(10);

/// A manager over a scratch data directory, its handle and a way to publish
/// snapshots.
struct Harness {
    dir: TempDir,
    builder: Arc<FakeBuilder>,
    store: NodeStore,
    handle: BuildsHandle,
    snapshots: watch::Sender<Option<quoro_node_agent::reconcile::Snapshot>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Harness {
    /// A harness with a fresh engine and store.
    async fn new(builder: FakeBuilder) -> Self {
        let dir = TempDir::new();
        let store = NodeStore::open(dir.path()).await.expect("open the store");
        let engine = Arc::new(FakeEngine::new());
        Self::with(dir, engine, store, builder).await
    }

    /// A harness over an existing directory, engine and store, as an agent
    /// restart would open them.
    async fn with(
        dir: TempDir,
        engine: Arc<FakeEngine>,
        store: NodeStore,
        builder: FakeBuilder,
    ) -> Self {
        let builder = Arc::new(builder);
        let (manager, handle) =
            BuildManager::new(Arc::clone(&engine), Arc::clone(&builder), store.clone());
        let manager = manager.with_tick(Duration::from_millis(10));
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

    /// Publishes one snapshot with no VMs.
    fn publish(&self, rev: u64, builds: Vec<quoro_proto::wire::SpecBuild>) {
        self.publish_with(rev, Vec::new(), builds);
    }

    /// Publishes one snapshot.
    fn publish_with(
        &self,
        rev: u64,
        vms: Vec<quoro_proto::wire::SpecVm>,
        builds: Vec<quoro_proto::wire::SpecBuild>,
    ) {
        self.snapshots
            .send_replace(Some(snapshot_with(rev, vms, builds)));
    }

    /// Waits until the handle reports the build in `want`.
    async fn wait_status(&self, build_id: &str, want: BuildState) -> BuildStatus {
        let found = tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self
                    .handle
                    .current()
                    .into_iter()
                    .find(|status| status.build_id == build_id)
                    && status.status == want
                {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        found.unwrap_or_else(|_| {
            panic!(
                "build {build_id} never reached {:?}; statuses are {:?}",
                want,
                self.handle.current()
            )
        })
    }

    /// Waits until the stored row reports `want`.
    async fn wait_row(&self, build_id: &str, want: BuildState) -> BuildRecord {
        let found = tokio::time::timeout(WAIT, async {
            loop {
                if let Ok(Some(row)) = self.store.build(build_id).await
                    && row.status == want
                {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        found.unwrap_or_else(|_| panic!("build {build_id} never became {want:?}"))
    }

    /// Waits until the builder was asked to run at least `count` builds.
    async fn wait_builds(&self, count: usize) {
        let waited = tokio::time::timeout(WAIT, async {
            while self.builder.build_count() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(waited.is_ok(), "the builder never ran {count} build(s)");
    }

    async fn shutdown(self) {
        let _ = self.stop.send(true);
        let finished = tokio::time::timeout(WAIT, self.task).await;
        assert!(finished.is_ok(), "the build manager did not stop");
    }
}

/// The shutdown future the manager waits on.
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

fn terminal_row(build_id: &str, app_id: &str, seconds: u32) -> BuildRecord {
    BuildRecord {
        build_id: build_id.to_owned(),
        app_id: app_id.to_owned(),
        status: BuildState::Succeeded,
        failed_phase: None,
        commit: Some("c".repeat(40)),
        image_digest: Some(DEFAULT_DIGEST.to_owned()),
        error: None,
        failed_step: None,
        log_path: None,
        created_at: format!("2026-11-01T00:00:{seconds:02}.000000000Z"),
        started_at: Some("2026-11-01T00:00:00.000000000Z".to_owned()),
        finished_at: Some("2026-11-01T00:01:00.000000000Z".to_owned()),
    }
}

#[tokio::test]
async fn a_build_runs_and_succeeds() {
    let harness = Harness::new(FakeBuilder::new()).await;
    let mut build = spec_build(BUILD, APP);
    build.build_args.insert("EXTRA".to_owned(), "1".to_owned());
    harness.publish(1, vec![build]);

    let status = harness.wait_status(BUILD, BuildState::Succeeded).await;
    assert_eq!(status.image_digest.as_deref(), Some(DEFAULT_DIGEST));
    assert_eq!(status.commit.as_deref(), Some("c".repeat(40).as_str()));
    assert!(status.started_at.is_some());
    assert!(status.finished_at.is_some());
    assert!(status.failed_phase.is_none());

    let row = harness.store.build(BUILD).await.unwrap().unwrap();
    assert_eq!(row.status, BuildState::Succeeded);
    assert_eq!(row.app_id, APP);
    assert_eq!(row.image_digest.as_deref(), Some(DEFAULT_DIGEST));
    assert!(row.log_path.is_some());

    // The spec the builder got carries the snapshot's fields and an address
    // from the snapshot's subnet.
    let specs = harness.builder.specs();
    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.build_id, BUILD);
    assert_eq!(spec.app_id, APP);
    assert_eq!(spec.context_dir, ".");
    assert_eq!(spec.limits, BuildLimits::default());
    assert_eq!(
        spec.secrets.get("token").map(String::as_str),
        Some("s3cret")
    );
    assert_eq!(spec.net.gateway, GATEWAY.parse::<Ipv4Addr>().unwrap());
    assert!(spec.net.ip.to_string().starts_with("10.42.1."));
    assert_eq!(
        specs[0].build_args.get("EXTRA").map(String::as_str),
        Some("1")
    );

    // The address was released and the ephemeral parts cleaned up.
    assert!(harness.builder.cleanups().contains(&BUILD.to_owned()));
    assert!(
        !harness
            .store
            .used_ips()
            .await
            .unwrap()
            .contains(&spec.net.ip)
    );

    // The log carries the builder's lines plus one terminal line.
    let lines = harness
        .handle
        .query(BUILD, 0, 1_000)
        .await
        .expect("the build has a log");
    assert!(lines.len() >= 3, "{lines:?}");
    assert!(lines.iter().all(|line| line.vm_id.is_none()));
    assert_eq!(lines[0].seq, Some(1));
    for pair in lines.windows(2) {
        assert_eq!(pair[1].seq, Some(pair[0].seq.unwrap() + 1), "gap-free");
    }
    assert_eq!(
        lines.last().unwrap().line,
        format!("build succeeded: {DEFAULT_DIGEST}")
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn two_builds_run_one_after_the_other() {
    let harness = Harness::new(FakeBuilder::new()).await;
    let first = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KA", APP);
    let second = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", APP);
    harness.publish(1, vec![first, second]);

    harness
        .wait_status("bld_01JC9ZA2B3C4D5E6F7G8H9J0KA", BuildState::Succeeded)
        .await;
    harness
        .wait_status("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", BuildState::Succeeded)
        .await;
    let order: Vec<String> = harness
        .builder
        .specs()
        .into_iter()
        .map(|spec| spec.build_id)
        .collect();
    assert_eq!(
        order,
        vec![
            "bld_01JC9ZA2B3C4D5E6F7G8H9J0KA".to_owned(),
            "bld_01JC9ZA2B3C4D5E6F7G8H9J0KB".to_owned()
        ],
        "the builds run in spec order, one at a time"
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn a_failure_carries_the_failed_step() {
    let builder = FakeBuilder::new();
    builder.script(
        BUILD,
        FakeOutcome::Fail {
            phase: BuildPhase::Building,
            error: "process \"/bin/sh -c npm run build\" did not complete successfully".to_owned(),
            step: Some(FailedStep {
                vertex: 6,
                name: "[3/4] RUN npm run build".to_owned(),
                error: "exit code 1".to_owned(),
                dockerfile_line: Some(9),
                excerpt: vec![
                    "#6 0.1 > build".to_owned(),
                    "#6 ERROR: exit code 1".to_owned(),
                ],
            }),
        },
    );
    let harness = Harness::new(builder).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);

    let status = harness.wait_status(BUILD, BuildState::Failed).await;
    assert_eq!(status.failed_phase, Some(BuildPhase::Building));
    assert!(
        status.error.as_deref().unwrap().contains("npm run build"),
        "{status:?}"
    );
    let step = status.failed_step.expect("the failing step is reported");
    assert_eq!(step.vertex, 6);
    assert_eq!(step.name, "[3/4] RUN npm run build");
    assert_eq!(step.dockerfile_line, Some(9));
    assert_eq!(step.excerpt.len(), 2);

    let lines = harness.handle.query(BUILD, 0, 1_000).await.unwrap();
    assert!(
        lines
            .last()
            .unwrap()
            .line
            .starts_with("build failed in building: "),
        "{:?}",
        lines.last()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn a_running_build_stops_when_the_primary_cancels_it() {
    let builder = FakeBuilder::new();
    builder.script(BUILD, FakeOutcome::Hang);
    let harness = Harness::new(builder).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    harness.wait_builds(1).await;

    let mut cancelled = spec_build(BUILD, APP);
    cancelled.cancelled = true;
    harness.publish(2, vec![cancelled]);

    harness.wait_status(BUILD, BuildState::Cancelled).await;
    let row = harness.store.build(BUILD).await.unwrap().unwrap();
    assert_eq!(row.status, BuildState::Cancelled);
    assert!(row.finished_at.is_some());
    assert!(harness.builder.cleanups().contains(&BUILD.to_owned()));
    harness.shutdown().await;
}

#[tokio::test]
async fn a_queued_build_is_reported_cancelled_without_running() {
    let builder = FakeBuilder::new();
    builder.script("bld_01JC9ZA2B3C4D5E6F7G8H9J0KA", FakeOutcome::Hang);
    let harness = Harness::new(builder).await;
    let running = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KA", APP);
    let queued = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", APP);
    harness.publish(1, vec![running, queued]);
    harness.wait_builds(1).await;
    harness
        .wait_row("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", BuildState::Queued)
        .await;

    let running = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KA", APP);
    let mut cancelled = spec_build("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", APP);
    cancelled.cancelled = true;
    harness.publish(2, vec![running, cancelled]);

    let status = harness
        .wait_status("bld_01JC9ZA2B3C4D5E6F7G8H9J0KB", BuildState::Cancelled)
        .await;
    assert!(status.started_at.is_none(), "it never left queued");
    assert_eq!(
        harness.builder.build_count(),
        1,
        "the cancelled build never ran"
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn a_build_that_leaves_the_snapshot_is_cancelled() {
    let builder = FakeBuilder::new();
    builder.script(BUILD, FakeOutcome::Hang);
    let harness = Harness::new(builder).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    harness.wait_builds(1).await;

    // The app keeps a VM, so only the build leaves the spec: the manager must
    // stop it rather than let the app's rows be removed.
    let mut vm = agent_common::spec_vm("vm_1", "prj_1", quoro_proto::wire::DesiredStatus::Running);
    vm.app_id = Some(APP.to_owned());
    harness.publish_with(2, vec![vm], Vec::new());

    let row = harness.wait_row(BUILD, BuildState::Cancelled).await;
    assert!(row.finished_at.is_some());
    assert!(harness.builder.cleanups().contains(&BUILD.to_owned()));
    harness.shutdown().await;
}

#[tokio::test]
async fn capacity_keeps_a_build_queued_and_it_runs_later() {
    let builder = FakeBuilder::new();
    builder.script(BUILD, FakeOutcome::CapacityOnce("sha256:bb".to_owned()));
    let harness = Harness::new(builder).await;
    let mut statuses = harness.handle.statuses();
    harness.publish(1, vec![spec_build(BUILD, APP)]);

    let final_status = tokio::time::timeout(WAIT, async {
        loop {
            let _ = statuses.changed().await;
            if let Some(status) = statuses
                .borrow()
                .iter()
                .find(|status| status.build_id == BUILD)
                && status.status.is_terminal()
            {
                return status.clone();
            }
        }
    })
    .await
    .expect("the retried build ended");

    assert_eq!(final_status.status, BuildState::Succeeded);
    assert_eq!(final_status.image_digest.as_deref(), Some("sha256:bb"));
    assert!(
        harness.builder.build_count() >= 2,
        "the build was retried after the capacity failure"
    );
    assert!(
        harness.builder.build_count() <= 3,
        "a capacity failure must not spin the node: {} runs",
        harness.builder.build_count()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn a_restart_fails_a_running_build_and_removes_its_builder_vm() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let mut row = terminal_row(BUILD, APP, 1);
    row.status = BuildState::Building;
    row.finished_at = None;
    store.save_build(&row).await.unwrap();
    store
        .record_created(BUILD, Ipv4Addr::new(10, 42, 1, 30), 0)
        .await
        .unwrap();

    let engine = Arc::new(FakeEngine::new());
    let digest = ImageDigest("sha256:builder".to_owned());
    engine
        .prepare_image(&ImageRef {
            reference: "builder".to_owned(),
            digest: digest.clone(),
        })
        .await
        .unwrap();
    let spec = VmSpec {
        id: VmId(BUILD.to_owned()),
        vcpus: 2,
        memory_mb: 2048,
        rootfs: digest,
        disk_gb: 1,
        volumes: Vec::new(),
        net: NetAttach {
            ip: Ipv4Addr::new(10, 42, 1, 30),
            gateway: GATEWAY.parse().unwrap(),
            mac: MacAddr([0x52, 0x55, 10, 42, 1, 30]),
            isolation_group: None,
            egress_only: true,
        },
        init: InitConfig {
            entrypoint: Vec::new(),
            cmd: Vec::new(),
            env: Vec::new(),
            user: None,
            workdir: None,
        },
        console: ConsoleMode::Off,
        serial: quoro_engine::SerialMode::Off,
        vsock: None,
        work_disks: Vec::new(),
    };
    engine.create(&spec).await.unwrap();
    engine.start(&VmId(BUILD.to_owned())).await.unwrap();

    let harness = Harness::with(dir, Arc::clone(&engine), store, FakeBuilder::new()).await;
    let row = harness.wait_row(BUILD, BuildState::Failed).await;
    assert_eq!(row.failed_phase, Some(BuildPhase::Building));
    assert_eq!(row.error.as_deref(), Some("node agent restarted"));
    assert!(row.finished_at.is_some());

    let removed = tokio::time::timeout(WAIT, async {
        loop {
            let left: Vec<String> = engine
                .list()
                .await
                .unwrap()
                .into_iter()
                .map(|vm| vm.id.0)
                .collect();
            if !left.iter().any(|id| id.starts_with("bld_")) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        removed.is_ok(),
        "the leftover builder VM was not removed: {:?}",
        engine.list().await.unwrap()
    );
    let cleaned = tokio::time::timeout(WAIT, async {
        while !harness.builder.cleanups().contains(&BUILD.to_owned()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(cleaned.is_ok(), "the leftover build was not cleaned up");
    assert!(
        !harness
            .store
            .used_ips()
            .await
            .unwrap()
            .contains(&Ipv4Addr::new(10, 42, 1, 30))
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn an_app_that_leaves_the_snapshot_loses_its_cache_and_logs() {
    let harness = Harness::new(FakeBuilder::new()).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    harness.wait_status(BUILD, BuildState::Succeeded).await;
    let logs = BuildLogs::new(harness.dir.path().join("builds").join("logs"));
    assert!(logs.exists(APP, BUILD));

    harness.publish(2, Vec::new());
    let removed = tokio::time::timeout(WAIT, async {
        while !harness.builder.cache_removals().contains(&APP.to_owned()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(removed.is_ok(), "the cache was never removed");
    assert!(!logs.exists(APP, BUILD));
    assert!(harness.store.builds().await.unwrap().is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn a_build_still_queued_at_restart_runs_instead_of_failing() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let mut row = terminal_row(BUILD, APP, 0);
    row.status = BuildState::Queued;
    row.commit = None;
    row.image_digest = None;
    row.failed_phase = None;
    row.error = None;
    row.started_at = None;
    row.finished_at = None;
    store.save_build(&row).await.unwrap();

    let harness = Harness::with(dir, Arc::new(FakeEngine::new()), store, FakeBuilder::new()).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    let status = harness.wait_status(BUILD, BuildState::Succeeded).await;
    assert!(status.error.is_none());
    harness.shutdown().await;
}

#[tokio::test]
async fn a_cache_disk_without_rows_is_removed_by_its_app_id() {
    // A cache disk left by an app whose rows are already gone is found from
    // its `<app_id>.ext4` file name.
    let dir = TempDir::new();
    let cache = dir.path().join("builds").join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(cache.join(format!("{APP}.ext4")), b"").unwrap();
    std::fs::write(cache.join("notes.txt"), b"").unwrap();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let harness = Harness::with(dir, Arc::new(FakeEngine::new()), store, FakeBuilder::new()).await;

    harness.publish(1, Vec::new());
    let removed = tokio::time::timeout(WAIT, async {
        while !harness.builder.cache_removals().contains(&APP.to_owned()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(removed.is_ok(), "the cache was never removed");
    assert!(
        harness
            .builder
            .cache_removals()
            .iter()
            .all(|app_id| app_id == APP),
        "only app ids reach remove_cache: {:?}",
        harness.builder.cache_removals()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn only_the_last_twenty_builds_of_an_app_are_kept() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let logs = BuildLogs::new(dir.path().join("builds").join("logs"));
    let total = KEPT_BUILDS_PER_APP + 1;
    for index in 0..total {
        let build_id = format!("bld_old_{index:02}");
        store
            .save_build(&terminal_row(&build_id, APP, index as u32))
            .await
            .unwrap();
        logs.append(APP, &build_id, LogStream::Stdout, None, "old")
            .await
            .unwrap();
    }

    let harness = Harness::with(dir, Arc::new(FakeEngine::new()), store, FakeBuilder::new()).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    harness.wait_status(BUILD, BuildState::Succeeded).await;

    let rows = harness.store.builds_for_app(APP).await.unwrap();
    assert_eq!(
        rows.len(),
        KEPT_BUILDS_PER_APP + 1,
        "twenty terminal builds plus the new one"
    );
    assert!(
        !rows.iter().any(|row| row.build_id == "bld_old_00"),
        "the oldest build was pruned"
    );
    assert!(!logs.exists(APP, "bld_old_00"));
    harness.shutdown().await;
}

#[tokio::test]
async fn the_log_cap_leaves_exactly_one_marker() {
    let dir = TempDir::new();
    let logs = BuildLogs::new(dir.path().join("builds").join("logs"));
    let line = "x".repeat(16 * 1024);
    let mut appended = 0usize;
    for _ in 0..520 {
        if logs
            .append(APP, BUILD, LogStream::Stdout, None, &line)
            .await
            .unwrap()
            .is_some()
        {
            appended += 1;
        }
    }
    assert!(appended < 520, "the cap dropped nothing");
    logs.finish(APP, BUILD).await.unwrap();
    logs.finish(APP, BUILD).await.unwrap();

    let lines = logs.lines_after(APP, BUILD, 0, usize::MAX).await.unwrap();
    assert_eq!(lines.len(), appended + 1);
    let marker = lines.last().unwrap();
    assert_eq!(
        marker.line,
        format!("log truncated: {} more lines dropped", 520 - appended)
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.line.starts_with("log truncated:"))
            .count(),
        1
    );
}

#[tokio::test]
async fn query_and_follow_agree_without_gaps_or_repeats() {
    let builder = FakeBuilder::new().with_delay(Duration::from_millis(20));
    let harness = Harness::new(builder).await;
    harness.publish(1, vec![spec_build(BUILD, APP)]);
    harness.wait_row(BUILD, BuildState::Preparing).await;

    // Follow while the build runs: the replay plus every live line, in order.
    let mut live = harness
        .handle
        .follow(BUILD, BuildStart::Tail(0))
        .await
        .expect("the build exists");
    let mut lines = Vec::new();
    while let Some(batch) = live.recv().await {
        lines.extend(batch);
    }
    assert!(!lines.is_empty(), "the live stream sent no line");
    assert_eq!(lines[0].seq, Some(1));
    for pair in lines.windows(2) {
        assert_eq!(
            pair[1].seq,
            Some(pair[0].seq.unwrap() + 1),
            "the stream has a gap or a repeat"
        );
    }
    assert_eq!(
        lines.last().unwrap().line,
        format!("build succeeded: {DEFAULT_DIGEST}"),
        "the stream ended after the terminal line"
    );

    // A terminal build: the query and both replay shapes see the same lines.
    let queried = harness.handle.query(BUILD, 0, 1_000).await.unwrap();
    assert_eq!(queried.len(), lines.len());
    let mut after = harness
        .handle
        .follow(BUILD, BuildStart::After(0))
        .await
        .unwrap();
    let mut replayed = Vec::new();
    while let Some(batch) = after.recv().await {
        replayed.extend(batch);
    }
    assert_eq!(replayed.len(), queried.len());

    // `tail` replays only the last line and ends at once.
    let mut tail = harness
        .handle
        .follow(BUILD, BuildStart::Tail(1))
        .await
        .unwrap();
    let mut tailed = Vec::new();
    while let Some(batch) = tail.recv().await {
        tailed.extend(batch);
    }
    assert_eq!(tailed.len(), 1);
    assert_eq!(tailed[0].seq, queried.last().unwrap().seq);

    assert!(matches!(
        harness
            .handle
            .follow("bld_missing", BuildStart::Tail(0))
            .await,
        Err(quoro_node_agent::build::manager::BuildsError::NotFound)
    ));
    harness.shutdown().await;
}

#[tokio::test]
async fn statuses_cover_only_the_current_spec_and_are_sorted() {
    let builder = FakeBuilder::new().with_delay(Duration::from_millis(20));
    builder.script("bld_z", FakeOutcome::Hang);
    let harness = Harness::new(builder).await;
    harness.publish(
        1,
        vec![spec_build("bld_z", APP), spec_build("bld_a", "app_other")],
    );
    let status = harness.wait_status("bld_z", BuildState::Preparing).await;
    let statuses = harness.handle.current();
    assert_eq!(statuses.len(), 2);
    assert_eq!(
        statuses
            .iter()
            .map(|status| status.build_id.as_str())
            .collect::<Vec<_>>(),
        vec!["bld_a", "bld_z"],
        "sorted by build id"
    );
    assert_eq!(
        statuses
            .iter()
            .find(|status| status.build_id == "bld_a")
            .unwrap()
            .status,
        BuildState::Queued
    );
    assert_eq!(status.status, BuildState::Preparing);

    // A build that leaves the spec leaves the report too.
    harness.publish(2, Vec::new());
    let emptied = tokio::time::timeout(WAIT, async {
        loop {
            if harness.handle.current().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(emptied.is_ok(), "the report still names a removed build");
    harness.shutdown().await;
}

/// A row nobody reports, so the unused-field lint stays quiet on helpers.
#[tokio::test]
async fn build_rows_round_trip_through_the_store() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let mut row = terminal_row(BUILD, APP, 3);
    row.failed_phase = Some(BuildPhase::Exporting);
    row.error = Some("timeout".to_owned());
    row.failed_step = Some(FailedStep {
        vertex: 2,
        name: "export".to_owned(),
        error: "timeout".to_owned(),
        dockerfile_line: None,
        excerpt: Vec::new(),
    });
    store.save_build(&row).await.unwrap();
    assert_eq!(store.build(BUILD).await.unwrap(), Some(row.clone()));
    assert_eq!(store.builds().await.unwrap(), vec![row.clone()]);
    assert_eq!(store.builds_for_app(APP).await.unwrap(), vec![row.clone()]);
    let status = row.to_status();
    assert_eq!(status.build_id, BUILD);
    assert_eq!(status.failed_phase, Some(BuildPhase::Exporting));

    store.delete_build(BUILD).await.unwrap();
    assert!(store.build(BUILD).await.unwrap().is_none());
}
