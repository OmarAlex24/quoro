// SPDX-License-Identifier: AGPL-3.0-only
//! A returning source's hand-over (section 9): the node was the volume's
//! source, lost the lease while it was cut off, and comes back as its
//! target, still holding the volume as its own.

use std::os::unix::fs::{FileExt as _, MetadataExt as _};
use std::path::Path;

use quoro_engine::{
    ChangedRanges, ConsoleMode, ImageDigest, ImageRef, InitConfig, MacAddr, NetAttach, SerialMode,
    VmId, VmSpec, VolumeAttach, VolumeId, VolumeSnapshot,
};
use quoro_engine::{FakeOp, FakeRanges};
use quoro_proto::wire::{DesiredStatus, QuarantineStatus, SpecVm, SpecVolume};

use super::*;
use crate::kept::{self, KeepPurpose};
use crate::store::{HandOverMark, VolumeQuarantineRecord};
use crate::warm::quarantine::Quarantines;
use crate::warm::quarantine::tests::{assert_primary_accepts, assert_rfc3339, plain_sha256};
use crate::warm::receiver::handover::{HandOver, REFUSALS_BEFORE_ERROR};

fn vol() -> VolumeId {
    VolumeId(VOL.to_owned())
}

/// The node's own row of the volume, from when it was the source.
fn own_row(state: VolumeState) -> VolumeRecord {
    VolumeRecord {
        volume_id: VOL.to_owned(),
        app_id: APP.to_owned(),
        size_gb: 1,
        state,
        error: None,
        allocated_bytes: None,
    }
}

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open")
        .write_all_at(bytes, offset)
        .expect("write");
}

fn read_at(path: &Path, offset: u64, length: usize) -> Vec<u8> {
    let mut buf = vec![0_u8; length];
    std::fs::File::open(path)
        .expect("open")
        .read_exact_at(&mut buf, offset)
        .expect("read");
    buf
}

/// Moves the entry's listener to a free port, so a pass binds it and no
/// port another test holds turns into an error on the board.
fn free_port(h: &Harness) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe")
        .local_addr()
        .expect("addr")
        .port();
    h.set_warm(|warm| warm.port = port);
}

/// The returning source as the harness finds it: the live file with data
/// and the node's own row.
async fn returning_source(h: &Harness) -> PathBuf {
    free_port(h);
    h.engine.set_changed_ranges_mode(FakeRanges::Content);
    // The start-up sweep removes what the store does not record.
    kept::start_tracking(&h.store, Some(&BTreeSet::new()))
        .await
        .expect("tracking");
    let info = h
        .engine
        .ensure_volume(&vol(), 1)
        .await
        .expect("the live volume");
    assert_eq!(info.path, h.volume_path);
    write_at(&info.path, 0, &[7_u8; 4096]);
    write_at(&info.path, 3 * CELL + 100, &pattern(100_000, 2));
    h.store
        .save_volume(&own_row(VolumeState::Ready))
        .await
        .expect("own row");
    // The warm row each pass adds for a target entry.
    h.store
        .add_warm_target(VOL, APP, 1)
        .await
        .expect("warm row");
    info.path
}

/// Keeps a snapshot of the live file, as the source kept its base and the
/// snapshot it was shipping.
async fn keep_snapshot(h: &Harness, purpose: KeepPurpose, seq: u64) -> VolumeSnapshot {
    let snapshot = h.engine.snapshot_volume(&vol()).await.expect("snapshot");
    kept::keep(&h.store, APP, &snapshot, purpose, Some(seq), None)
        .await
        .expect("keep");
    snapshot
}

/// A write the target never got.
fn diverge(path: &Path) {
    write_at(path, 7 * CELL + 512, &pattern(8192, 4));
}

async fn quarantined(h: &Harness) -> Vec<VolumeQuarantineRecord> {
    h.store.volume_quarantines().await.expect("records")
}

/// The purposes of the snapshots the node keeps of the volume, sorted.
async fn kept_purposes(h: &Harness) -> Vec<String> {
    let mut purposes: Vec<String> = h
        .store
        .kept_snapshots_of(VOL)
        .await
        .expect("kept")
        .into_iter()
        .map(|record| record.purpose)
        .collect();
    purposes.sort();
    purposes
}

/// The volume is an ordinary warm copy now: no file, no own row, no
/// complete copy, no error.
async fn assert_handed_over(h: &Harness) {
    assert!(!h.volume_path.exists(), "the live file is gone");
    assert!(
        h.store.volume(VOL).await.expect("store").is_none(),
        "the own row is gone"
    );
    let row = h.row().await;
    assert_eq!(row.complete_seq, None);
    assert!(row.pending.is_none());
    assert!(row.error.is_none(), "{:?}", row.error);
}

/// The quarantine list once `ready` accepts it. The bound covers a hash of
/// the whole 1 GiB copy in a debug build on a busy machine.
async fn quarantines_where(
    statuses: &mut watch::Receiver<Vec<QuarantineStatus>>,
    ready: impl FnMut(&Vec<QuarantineStatus>) -> bool,
) -> Vec<QuarantineStatus> {
    tokio::time::timeout(Duration::from_secs(600), statuses.wait_for(ready))
        .await
        .expect("the list is published")
        .expect("the list is open")
        .clone()
}

/// The plain SHA-256 of a file, off the test's runtime.
async fn sha256_of_file(path: &Path) -> String {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || plain_sha256(&path))
        .await
        .expect("hash")
}

async fn hand_over(h: &Harness) -> HandOver {
    h.receiver.inner.hand_over(VOL).await
}

/// One pass, with the test's listeners: a listener a pass bound stays bound
/// until the test ends.
async fn pass(h: &Harness, listeners: &mut BTreeMap<SocketAddr, JoinHandle<()>>) {
    h.receiver.pass(listeners).await;
}

#[tokio::test]
async fn a_returning_source_with_no_change_since_the_base_hands_its_volume_over() {
    let h = Harness::new("handover-unchanged").await;
    let mut listeners = BTreeMap::new();
    returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    let shipping = keep_snapshot(&h, KeepPurpose::WarmShipping, 2).await;
    let snapshots = h.engine.volume_snapshots().len();

    // Before the hand-over a stream is refused as `busy`: the volume is the
    // node's own, and the source sends again later. That is no error.
    let model = first_model();
    h.post(
        stream(&header(5, 1, None), &model, &model.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("busy", None);
    assert!(h.status().error.is_none(), "{:?}", h.status());

    pass(&h, &mut listeners).await;
    assert_handed_over(&h).await;
    assert!(quarantined(&h).await.is_empty());
    assert!(h.receiver.quarantine_statuses().borrow().is_empty());
    assert_eq!(
        h.engine.volume_snapshots().len(),
        snapshots,
        "no copy taken"
    );
    assert!(h.status().error.is_none(), "{:?}", h.status());
    // The source's snapshots stay until a complete stream lands.
    assert_eq!(kept_purposes(&h).await, vec!["warm_base", "warm_shipping"]);
    assert!(base.location.exists() && shipping.location.exists());

    // A second pass finds nothing to do.
    pass(&h, &mut listeners).await;
    assert_eq!(h.engine.volume_snapshots().len(), snapshots);
    assert!(quarantined(&h).await.is_empty());

    // The next stream is a full copy, from empty; then the source's
    // snapshots go.
    let model = ship_first(&h).await;
    h.assert_copy(&model, Some(1), &model.touched).await;
    assert!(kept_purposes(&h).await.is_empty());
    assert!(!base.location.exists() && !shipping.location.exists());
    assert!(h.engine.volume_snapshots().is_empty());
    assert!(h.status().error.is_none());
}

#[tokio::test]
async fn a_returning_source_with_writes_since_the_base_keeps_them_in_quarantine() {
    let h = Harness::new("handover-changed").await;
    let mut listeners = BTreeMap::new();
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let mut statuses = h.receiver.quarantine_statuses();
    let gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;

    pass(&h, &mut listeners).await;
    assert_handed_over(&h).await;
    let records = quarantined(&h).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = records[0].clone();
    assert_eq!(
        (record.volume_id.as_str(), record.app_id.as_str()),
        (VOL, APP)
    );
    assert!(record.sha256.is_none());
    assert_rfc3339(&record.created_at);
    assert_eq!(
        record.size_bytes,
        std::fs::metadata(&record.path).expect("the copy").blocks() * 512,
        "the bytes the copy uses on disk"
    );
    // The copy holds the live data, the divergent write included.
    assert_eq!(read_at(&record.path, 0, 4096), vec![7_u8; 4096]);
    assert_eq!(
        read_at(&record.path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
    );
    // Its kept snapshot is the copy, under its own purpose.
    assert_eq!(kept_purposes(&h).await, vec!["quarantine", "warm_base"]);
    let kept = h
        .store
        .kept_snapshot(&record.snapshot_id)
        .await
        .expect("store")
        .expect("the copy's kept snapshot");
    assert_eq!(kept.purpose, "quarantine");
    assert_eq!(kept.snapshot.location, record.path);
    assert_eq!(kept.seq, None);

    // Reported at once, without its hash, in a form the primary accepts.
    let listed = statuses.borrow_and_update().clone();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].quarantine_id, record.quarantine_id);
    assert_eq!(listed[0].size_bytes, record.size_bytes);
    assert!(listed[0].sha256.is_none());
    assert_primary_accepts(&listed[0]);

    // A second pass takes no second copy.
    pass(&h, &mut listeners).await;
    assert_eq!(quarantined(&h).await.len(), 1);

    // The hash is what `sha256sum` prints for the copy, holes included.
    drop(gate);
    let hashed = quarantines_where(&mut statuses, |list| {
        list.first().is_some_and(|entry| entry.sha256.is_some())
    })
    .await;
    let expected = sha256_of_file(&record.path).await;
    assert_eq!(hashed[0].sha256.as_deref(), Some(expected.as_str()));
    assert_primary_accepts(&hashed[0]);
    assert_eq!(
        quarantined(&h).await[0].sha256.as_deref(),
        Some(expected.as_str())
    );

    // A full copy then completes. The base goes; the quarantined copy stays.
    let model = ship_first(&h).await;
    h.assert_copy(&model, Some(1), &model.touched).await;
    assert_eq!(kept_purposes(&h).await, vec!["quarantine"]);
    assert!(!base.location.exists());
    assert!(record.path.exists());
    assert_eq!(statuses.borrow().len(), 1);
}

/// Section 9 step 1 under its binding interim rule: only a comparison with
/// the base that is not `full` and finds no range lets the file go unkept.
#[tokio::test]
async fn every_case_but_a_proved_unchanged_file_is_quarantined() {
    #[derive(Debug, Clone, Copy)]
    enum Case {
        MatchesShippingOnly,
        NoBase,
        Full,
        EngineError,
        /// The engine refuses the base, and not the live file alone.
        UnusableBase,
        /// The base is recorded but the engine no longer has it.
        LostBase,
    }
    for case in [
        Case::MatchesShippingOnly,
        Case::NoBase,
        Case::Full,
        Case::EngineError,
        Case::UnusableBase,
        Case::LostBase,
    ] {
        let h = Harness::new(&format!("handover-rule-{case:?}")).await;
        let path = returning_source(&h).await;
        let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
        match case {
            Case::MatchesShippingOnly => {
                keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
                diverge(&path);
                keep_snapshot(&h, KeepPurpose::WarmShipping, 2).await;
            }
            Case::NoBase => {
                keep_snapshot(&h, KeepPurpose::WarmShipping, 2).await;
            }
            Case::Full => {
                keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
                // No range, but `full`: the extent maps could not be
                // compared, which proves nothing.
                h.engine.push_changed_ranges(ChangedRanges {
                    ranges: Vec::new(),
                    full: true,
                    extent_count: 0,
                });
            }
            Case::EngineError => {
                keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
                h.engine.fail_next(
                    FakeOp::VolumeChangedRanges,
                    EngineError::Io("injected".to_owned()),
                );
            }
            Case::UnusableBase => {
                keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
                h.engine.fail_next(
                    FakeOp::VolumeChangedRanges,
                    EngineError::Conflict("the base is outside the volume directory".to_owned()),
                );
            }
            Case::LostBase => {
                let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
                diverge(&path);
                h.engine
                    .delete_volume_snapshot(&base)
                    .await
                    .expect("lose the base");
            }
        }
        let outcome = hand_over(&h).await;
        assert!(
            matches!(outcome, HandOver::Done(Some(_))),
            "{case:?}: {outcome:?}"
        );
        let records = quarantined(&h).await;
        assert_eq!(records.len(), 1, "{case:?}");
        if matches!(case, Case::UnusableBase | Case::LostBase) {
            assert!(
                records[0].reason.contains("base cannot be compared"),
                "{case:?}: {}",
                records[0].reason
            );
        }
        // The copy holds what the live file held.
        assert_eq!(read_at(&records[0].path, 0, 4096), vec![7_u8; 4096]);
        assert_handed_over(&h).await;
    }
}

/// An engine VM that attaches the volume, or none.
async fn engine_vm(engine: &FakeEngine, vm_id: &str, attach: bool) -> VmId {
    let digest = ImageDigest("sha256:old".to_owned());
    engine
        .prepare_image(&ImageRef {
            reference: "old".to_owned(),
            digest: digest.clone(),
        })
        .await
        .expect("image");
    let id = VmId(vm_id.to_owned());
    engine
        .create(&VmSpec {
            id: id.clone(),
            vcpus: 1,
            memory_mb: 128,
            rootfs: digest,
            disk_gb: 1,
            volumes: if attach {
                vec![VolumeAttach {
                    volume_id: vol(),
                    mount_path: "/data".to_owned(),
                    serial: "quoro-vol-0".to_owned(),
                }]
            } else {
                Vec::new()
            },
            net: NetAttach {
                ip: std::net::Ipv4Addr::new(10, 42, 1, 2),
                gateway: std::net::Ipv4Addr::new(10, 42, 1, 1),
                mac: MacAddr([0x52, 0x55, 10, 42, 1, 2]),
                isolation_group: None,
                egress_only: false,
            },
            init: InitConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
            },
            console: ConsoleMode::Off,
            serial: SerialMode::Off,
            vsock: None,
            work_disks: Vec::new(),
        })
        .await
        .expect("create");
    id
}

/// The app's VM as the warm node's spec lists it: `standby`, with the
/// volume.
fn standby_vm(vm_id: &str) -> SpecVm {
    SpecVm {
        vm_id: vm_id.to_owned(),
        project_id: "prj_01JD0WARM0000000000000001".to_owned(),
        app_id: Some(APP.to_owned()),
        deployment_id: None,
        name: "app".to_owned(),
        image: "registry.example/app:1".to_owned(),
        image_digest: "sha256:old".to_owned(),
        vcpus: 1,
        memory_mb: 128,
        disk_gb: 1,
        port: None,
        env: BTreeMap::new(),
        health_check: None,
        desired_status: DesiredStatus::Stopped,
        standby: true,
        restart_generation: 0,
        stateful: true,
        lease_id: Some(LEASE.to_owned()),
        fencing_token: Some(5),
        volumes: vec![SpecVolume {
            volume_id: VOL.to_owned(),
            serial: "quoro-vol-0".to_owned(),
            mount_path: "/data".to_owned(),
        }],
        build_id: None,
    }
}

#[tokio::test]
async fn a_vm_that_still_attaches_the_volume_holds_the_hand_over_without_an_error() {
    let h = Harness::new("handover-vm").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let snapshots = h.engine.volume_snapshots().len();

    // A VM the engine holds with the volume: the engine refuses to compare,
    // with the base and without it.
    let attaching = engine_vm(&h.engine, "vm_01JD0WARM0000000000000002", true).await;
    assert!(
        matches!(hand_over(&h).await, HandOver::Refused(ref reason) if reason.contains("attached")),
    );
    assert!(h.row().await.error.is_none());
    h.engine.delete(&attaching).await.expect("delete");

    // The spec's VM of the volume, the old one the reconciler has not
    // deleted yet: waited for even where the engine's comparison would not
    // see it.
    let old_id = "vm_01JD0WARM0000000000000001";
    let old = engine_vm(&h.engine, old_id, false).await;
    h.desired.send_modify(|current| {
        let snapshot = current.as_mut().expect("a snapshot");
        snapshot.rev += 1;
        snapshot.state.rev = snapshot.rev;
        snapshot.state.spec.vms = vec![standby_vm(old_id)];
    });
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(h.receiver.clone().run(async {
        let _ = stopped.await;
    }));
    // Many passes: nothing changes, and nothing is reported as an error.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(h.volume_path.exists());
    assert!(h.store.volume(VOL).await.expect("store").is_some());
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(
        h.engine.volume_snapshots().len(),
        snapshots,
        "no copy taken"
    );
    assert!(h.row().await.error.is_none());
    assert!(h.status().error.is_none(), "{:?}", h.status());
    // A stream meanwhile is refused as `busy`, without an error: the source
    // keeps its base and sends again.
    let model = first_model();
    h.post(
        stream(&header(5, 1, None), &model, &model.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("busy", None);
    assert!(h.row().await.error.is_none());
    assert!(h.status().error.is_none(), "{:?}", h.status());
    assert!(h.volume_path.exists());
    assert!(h.engine.staged_volumes().is_empty());

    // Once the old VM is gone, a pass of the running receiver hands the
    // volume over.
    h.engine.delete(&old).await.expect("delete");
    wait_for("the hand-over", || async {
        h.store.volume(VOL).await.expect("store").is_none()
    })
    .await;
    assert_handed_over(&h).await;
    assert_eq!(quarantined(&h).await.len(), 1);

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the receiver stops")
        .expect("the receiver task");
}

async fn sqlx_connect(path: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(path))
        .await
        .expect("a second connection")
}

/// What the node finds at its next start: the engine's sweep with the rule
/// the store gives.
async fn restart_sweep(h: &Harness) {
    let rule = kept::sweep_rule(&h._scratch.0).await;
    assert!(
        matches!(rule, quoro_engine::SnapshotSweep::Keep(_)),
        "{rule:?}"
    );
    h.engine.start_up_sweep(&rule).expect("sweep");
}

#[tokio::test]
async fn a_crash_at_any_step_leaves_exactly_one_copy_and_the_hand_over_ends() {
    let h = Harness::new("handover-crash").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let db = sqlx_connect(&h._scratch.0.join("node.db")).await;

    // 1. After the clone, before its records: the clone is not recorded, and
    //    it stays, since a failed commit may still come back at a start.
    sqlx::query(
        "CREATE TRIGGER no_quarantine BEFORE INSERT ON quarantine \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(
        kept_purposes(&h).await,
        vec!["warm_base"],
        "nothing recorded"
    );
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(h.volume_path.exists());
    assert!(h.row().await.error.is_some(), "the failure is reported");
    let unrecorded: Vec<VolumeSnapshot> = h
        .engine
        .volume_snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.snapshot_id != base.snapshot_id)
        .collect();
    assert_eq!(unrecorded.len(), 1);
    assert!(unrecorded[0].location.exists());
    sqlx::query("DROP TRIGGER no_quarantine")
        .execute(&db)
        .await
        .expect("drop trigger");
    // The next start's sweep removes the unrecorded clone and keeps the base.
    restart_sweep(&h).await;
    assert!(!unrecorded[0].location.exists());
    assert!(base.location.exists());
    assert_eq!(h.engine.volume_snapshots().len(), 1);

    // 2. After the records, before the file is deleted.
    h.engine
        .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    let records = quarantined(&h).await;
    assert_eq!(records.len(), 1);
    let copy = records[0].clone();
    assert!(h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(h.volume_path.exists());

    // 3. After the file is deleted, before the row goes.
    sqlx::query(
        "CREATE TRIGGER keep_row BEFORE DELETE ON volumes \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_eq!(quarantined(&h).await, vec![copy.clone()], "no second copy");
    assert!(!h.volume_path.exists());
    assert!(h.store.volume_handing_over(VOL).await.expect("store"));
    sqlx::query("DROP TRIGGER keep_row")
        .execute(&db)
        .await
        .expect("drop trigger");

    // The next run ends the hand-over with the same single copy, also as
    // a reopened store reads it.
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    let reopened = NodeStore::open(&h._scratch.0).await.expect("store");
    assert_eq!(
        reopened.volume_quarantines().await.expect("records"),
        vec![copy.clone()]
    );
    // A start-up sweep keeps the quarantined copy and the base.
    restart_sweep(&h).await;
    assert!(copy.path.exists());
    assert!(base.location.exists());
    assert_eq!(hand_over(&h).await, HandOver::Nothing);
}

#[tokio::test]
async fn a_crash_after_an_unchanged_volume_is_marked_takes_no_copy() {
    let h = Harness::new("handover-crash-unchanged").await;
    returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    h.engine
        .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert!(h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(h.volume_path.exists());
    // The next run skips the comparison and ends the hand-over.
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(h.engine.volume_snapshots().len(), 1, "the base only");
}

#[tokio::test]
async fn an_operator_s_delete_removes_the_copy_and_its_records_once() {
    let h = Harness::new("handover-delete").await;
    let mut listeners = BTreeMap::new();
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let mut statuses = h.receiver.quarantine_statuses();
    let gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    pass(&h, &mut listeners).await;
    let records = quarantined(&h).await;
    assert_eq!(records.len(), 1);
    let copy = records[0].clone();
    assert_eq!(statuses.borrow_and_update().len(), 1);

    // The hash starts; the delete comes while it runs.
    drop(gate);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let unknown = "qrn_01JD0WARM0000000000000000Z".to_owned();
    h.desired.send_modify(|current| {
        let snapshot = current.as_mut().expect("a snapshot");
        snapshot.rev += 1;
        snapshot.state.rev = snapshot.rev;
        snapshot.state.spec.quarantine_deletes = vec![unknown.clone(), copy.quarantine_id.clone()];
    });
    pass(&h, &mut listeners).await;
    assert!(quarantined(&h).await.is_empty());
    assert!(!copy.path.exists(), "the copy is deleted");
    assert_eq!(kept_purposes(&h).await, vec!["warm_base"]);
    assert!(base.location.exists());
    quarantines_where(&mut statuses, Vec::is_empty).await;

    // The stopped hash records nothing and lists nothing again.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(quarantined(&h).await.is_empty());
    assert!(statuses.borrow().is_empty());

    // The same ids again, and after a restart: nothing more happens.
    pass(&h, &mut listeners).await;
    let (restarted, restarted_statuses) = Quarantines::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        Duration::from_millis(50),
    );
    restarted.refresh().await.expect("the records");
    restarted
        .delete(&[unknown, copy.quarantine_id.clone()])
        .await;
    assert!(restarted_statuses.borrow().is_empty());
    assert_eq!(kept_purposes(&h).await, vec!["warm_base"]);
    assert!(base.location.exists());
}

#[tokio::test]
async fn a_restart_resumes_the_hash_of_a_copy_without_one() {
    let h = Harness::new("handover-rehash").await;
    let mut listeners = BTreeMap::new();
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    // The first run never hashes.
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    pass(&h, &mut listeners).await;
    let copy = quarantined(&h).await[0].clone();
    assert!(copy.sha256.is_none());
    // A shorter copy hashes quickly; it keeps its data and its holes.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&copy.path)
        .expect("open")
        .set_len(8 * CELL)
        .expect("shorten");

    // At the next start the list holds the copy before the session's first
    // report, without a hash.
    let (board, _) = StatusBoard::new();
    let starting = WarmReceiver::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        h.desired.subscribe(),
        VolumeLocks::new(),
        board,
        ReceiverOptions::quick(),
    );
    let first_report = starting.quarantine_statuses();
    starting.load_quarantines().await.expect("the records");
    assert_eq!(first_report.borrow().len(), 1);
    assert_eq!(first_report.borrow()[0].quarantine_id, copy.quarantine_id);
    assert!(first_report.borrow()[0].sha256.is_none());
    drop(starting);

    // The node starts again over the same store: the hash resumes.
    let (restarted, mut statuses) = Quarantines::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        Duration::from_millis(50),
    );
    restarted.refresh().await.expect("the records");
    let listed = quarantines_where(&mut statuses, |list| {
        list.first().is_some_and(|entry| entry.sha256.is_some())
    })
    .await;
    let expected = sha256_of_file(&copy.path).await;
    assert_eq!(listed[0].quarantine_id, copy.quarantine_id);
    assert_eq!(listed[0].sha256.as_deref(), Some(expected.as_str()));
    assert_primary_accepts(&listed[0]);
    assert_eq!(
        quarantined(&h).await[0].sha256.as_deref(),
        Some(expected.as_str())
    );
}

#[tokio::test]
async fn a_volume_the_node_no_longer_holds_as_its_own_runs_no_hand_over() {
    let h = Harness::new("handover-none").await;
    let mut listeners = BTreeMap::new();
    free_port(&h);
    // No own row at all: a warm copy like any other.
    let model = ship_first(&h).await;
    let snapshots = h.engine.volume_snapshots().len();
    assert_eq!(hand_over(&h).await, HandOver::Nothing);
    pass(&h, &mut listeners).await;
    h.assert_copy(&model, Some(1), &model.touched).await;

    // An own row in state `deleted`: the node no longer holds the volume.
    h.store
        .save_volume(&own_row(VolumeState::Deleted))
        .await
        .expect("row");
    assert_eq!(hand_over(&h).await, HandOver::Nothing);
    pass(&h, &mut listeners).await;
    h.assert_copy(&model, Some(1), &model.touched).await;
    assert!(h.store.volume(VOL).await.expect("store").is_some());
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(h.engine.volume_snapshots().len(), snapshots);
}

#[tokio::test]
async fn an_engine_that_keeps_refusing_the_hand_over_is_reported_after_a_bound() {
    let h = Harness::new("handover-refused").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    // A VM the desired state does not list, which the engine holds with the
    // volume: only the engine's comparison sees it.
    let attaching = engine_vm(&h.engine, "vm_01JD0WARM0000000000000002", true).await;
    for run in 1..REFUSALS_BEFORE_ERROR {
        assert!(
            matches!(hand_over(&h).await, HandOver::Refused(_)),
            "run {run}"
        );
        assert!(h.row().await.error.is_none(), "run {run}");
    }
    // From the bound on, the operator sees why.
    assert!(matches!(hand_over(&h).await, HandOver::Refused(_)));
    let error = h.row().await.error.expect("the refusal is reported");
    assert!(error.contains("refused"), "{error}");
    assert!(h.volume_path.exists());
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(quarantined(&h).await.is_empty());

    // Once the engine lets go, the hand-over ends and the error goes.
    h.engine.delete(&attaching).await.expect("delete");
    assert!(matches!(hand_over(&h).await, HandOver::Done(Some(_))));
    assert_handed_over(&h).await;
    assert_eq!(quarantined(&h).await.len(), 1);
}

#[tokio::test]
async fn an_own_row_without_a_live_file_hands_over_only_where_another_file_shows_the_directory() {
    // The base on disk shows the volume directory to be the real one: the
    // live file is gone, and there is nothing to keep.
    let h = Harness::new("handover-gone-evident").await;
    returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    let snapshots = h.engine.volume_snapshots().len();
    h.engine.delete_volume(&vol()).await.expect("delete");
    assert!(h.store.volume(VOL).await.expect("store").is_some());
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(h.engine.volume_snapshots().len(), snapshots);

    // No other file of the volume: the directory may not be the real one,
    // as after a state disk that did not mount. Nothing changes, and the
    // error says why.
    let h = Harness::new("handover-gone-unproved").await;
    returning_source(&h).await;
    h.engine.delete_volume(&vol()).await.expect("delete");
    for _ in 0..2 {
        assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
        assert!(
            h.store.volume(VOL).await.expect("store").is_some(),
            "the row stays"
        );
        assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
        let error = h.row().await.error.expect("an error is shown");
        assert!(error.contains("may not be the real one"), "{error}");
    }
    assert!(quarantined(&h).await.is_empty());

    // A snapshot that answers `NotFound` while the live file is there is
    // not taken for a missing file: nothing goes, and the next run keeps it.
    let h = Harness::new("handover-gone-snapshot").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    h.engine
        .fail_next(FakeOp::SnapshotVolume, EngineError::NotFound);
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert!(h.volume_path.exists());
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(h.store.volume(VOL).await.expect("store").is_some());
    assert!(matches!(hand_over(&h).await, HandOver::Done(Some(_))));
    assert_eq!(quarantined(&h).await.len(), 1);
    assert_handed_over(&h).await;
}

#[tokio::test]
async fn a_copy_whose_record_failed_is_recorded_from_the_same_clone() {
    let h = Harness::new("handover-record-failed").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let db = sqlx_connect(&h._scratch.0.join("node.db")).await;
    sqlx::query(
        "CREATE TRIGGER no_quarantine BEFORE INSERT ON quarantine \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    // The clone stays: the commit could still come back at a start.
    let clones: Vec<VolumeSnapshot> = h
        .engine
        .volume_snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.snapshot_id != base.snapshot_id)
        .collect();
    assert_eq!(clones.len(), 1);
    assert!(clones[0].location.exists());
    assert!(quarantined(&h).await.is_empty());
    assert!(h.volume_path.exists());
    sqlx::query("DROP TRIGGER no_quarantine")
        .execute(&db)
        .await
        .expect("drop trigger");

    // The next run records that clone; it takes no second one.
    assert!(matches!(hand_over(&h).await, HandOver::Done(Some(_))));
    let records = quarantined(&h).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].path, clones[0].location);
    assert_eq!(records[0].snapshot_id, clones[0].snapshot_id);
    assert_eq!(
        h.engine.volume_snapshots().len(),
        2,
        "the base and the copy"
    );
    assert_eq!(
        read_at(&records[0].path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
    );
    assert_handed_over(&h).await;
}

/// A copy recorded with the mark standing for it, whose live file was not
/// deleted.
async fn marked_copy(h: &Harness) -> VolumeQuarantineRecord {
    h.engine
        .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
    assert!(matches!(hand_over(h).await, HandOver::Failed(_)));
    let copy = quarantined(h).await.last().expect("a copy").clone();
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(HandOverMark::Copy(copy.quarantine_id.clone()))
    );
    copy
}

/// The live file still holds the divergent write.
fn assert_live_kept(h: &Harness) {
    assert!(h.volume_path.exists(), "the live file stays");
    assert_eq!(
        read_at(&h.volume_path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
    );
}

/// Moves `paths` out of the volume directory, as a state disk that did not
/// mount; returns where each went.
fn move_away(h: &Harness, paths: &[&Path]) -> Vec<(PathBuf, PathBuf)> {
    let away = h._scratch.0.join("away");
    std::fs::create_dir_all(&away).expect("away");
    paths
        .iter()
        .enumerate()
        .map(|(n, path)| {
            let to = away.join(n.to_string());
            std::fs::rename(path, &to).expect("move away");
            (path.to_path_buf(), to)
        })
        .collect()
}

fn move_back(moved: &[(PathBuf, PathBuf)]) {
    for (path, away) in moved {
        std::fs::rename(away, path).expect("move back");
    }
}

#[tokio::test]
async fn a_marked_copy_whose_clone_is_missing_is_kept_and_the_file_compared_again() {
    let h = Harness::new("handover-lost-clone").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let mut statuses = h.receiver.quarantine_statuses();
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let lost = marked_copy(&h).await;
    assert_eq!(statuses.borrow_and_update().len(), 1);
    std::fs::remove_file(&lost.path).expect("remove the clone");
    // The base on disk shows the volume directory to be the real one.
    assert!(base.location.exists());
    let db = sqlx_connect(&h._scratch.0.join("node.db")).await;

    // The mark cannot be cleared: it stays, and so do the record and the
    // file.
    sqlx::query(
        "CREATE TRIGGER keep_mark BEFORE UPDATE OF handing_over ON volumes \
         WHEN NEW.handing_over = 0 BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_live_kept(&h);
    assert_eq!(quarantined(&h).await, vec![lost.clone()]);
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(HandOverMark::Copy(lost.quarantine_id.clone()))
    );
    sqlx::query("DROP TRIGGER keep_mark")
        .execute(&db)
        .await
        .expect("drop trigger");

    // The marked run keeps the live file: the mark goes, and the record
    // stays, still listed without a hash.
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_live_kept(&h);
    assert_eq!(
        quarantined(&h).await,
        vec![lost.clone()],
        "nothing forgotten"
    );
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        None
    );
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(h.row().await.error.is_some(), "the operator sees why");
    let listed = statuses.borrow_and_update().clone();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].quarantine_id, lost.quarantine_id);
    assert!(listed[0].sha256.is_none());

    // The next run compares again and keeps one new copy beside it.
    let HandOver::Done(Some(id)) = hand_over(&h).await else {
        panic!("the hand-over ends");
    };
    assert_ne!(id, lost.quarantine_id);
    let records = quarantined(&h).await;
    assert_eq!(
        records
            .iter()
            .map(|record| record.quarantine_id.as_str())
            .collect::<Vec<_>>(),
        vec![lost.quarantine_id.as_str(), id.as_str()]
    );
    assert_eq!(
        read_at(&records[1].path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
    );
    assert_handed_over(&h).await;
}

#[tokio::test]
async fn a_missing_clone_with_no_other_file_of_the_volume_changes_nothing_until_the_files_are_back()
{
    let h = Harness::new("handover-unmounted").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let copy = marked_copy(&h).await;
    let snapshots = h.engine.volume_snapshots().len();

    // The state disk did not mount: the volume directory is empty, while
    // the store and the engine still name every file.
    let moved = move_away(&h, &[&path, &copy.path, &base.location]);
    for _ in 0..3 {
        assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
        assert_eq!(
            quarantined(&h).await,
            vec![copy.clone()],
            "nothing is forgotten"
        );
        assert!(
            h.store.volume(VOL).await.expect("store").is_some(),
            "the row stays"
        );
        assert_eq!(
            h.store.volume_hand_over_mark(VOL).await.expect("store"),
            Some(HandOverMark::Copy(copy.quarantine_id.clone())),
            "the mark stands"
        );
        let error = h.row().await.error.expect("an error is shown");
        assert!(error.contains("may not be the real one"), "{error}");
    }
    assert_eq!(kept_purposes(&h).await, vec!["quarantine", "warm_base"]);
    assert_eq!(h.engine.volume_snapshots().len(), snapshots);

    // Mounted again: the hand-over ends with exactly the original copy.
    move_back(&moved);
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_eq!(quarantined(&h).await, vec![copy.clone()]);
    assert!(copy.path.exists());
    assert_eq!(read_at(&copy.path, 7 * CELL + 512, 8192), pattern(8192, 4));
    assert_eq!(h.engine.volume_snapshots().len(), snapshots, "no new copy");
    assert_handed_over(&h).await;
}

#[tokio::test]
async fn a_missing_clone_of_an_older_copy_leaves_the_newer_copy_s_mark_standing() {
    let h = Harness::new("handover-older-clone").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let older = marked_copy(&h).await;
    // The node ran the volume again before that hand-over ended; a later
    // one took a newer copy.
    h.store.clear_volume_handing_over(VOL).await.expect("clear");
    let newer = marked_copy(&h).await;
    assert_ne!(newer.quarantine_id, older.quarantine_id);
    std::fs::remove_file(&older.path).expect("remove the older clone");
    let snapshots = h.engine.volume_snapshots().len();

    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_eq!(quarantined(&h).await, vec![older.clone(), newer.clone()]);
    assert!(newer.path.exists());
    assert_eq!(
        h.engine.volume_snapshots().len(),
        snapshots,
        "no extra copy"
    );
    assert_handed_over(&h).await;
}

#[tokio::test]
async fn a_marked_copy_an_operator_deleted_has_the_file_compared_again() {
    let h = Harness::new("handover-deleted-marked").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let copy = marked_copy(&h).await;
    h.receiver
        .inner
        .quarantines
        .delete(std::slice::from_ref(&copy.quarantine_id))
        .await;
    assert!(quarantined(&h).await.is_empty());
    assert!(!copy.path.exists());

    // The live file is all that keeps those writes now: it stays, and the
    // mark goes.
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_live_kept(&h);
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        None
    );
    let HandOver::Done(Some(id)) = hand_over(&h).await else {
        panic!("the hand-over ends");
    };
    assert_ne!(id, copy.quarantine_id);
    assert_eq!(quarantined(&h).await.len(), 1);
    assert_handed_over(&h).await;
}

#[tokio::test]
async fn a_quarantine_row_that_cannot_be_read_whole_is_still_deleted() {
    let h = Harness::new("handover-bad-row").await;
    returning_source(&h).await;
    let clone = h.engine.snapshot_volume(&vol()).await.expect("clone");
    kept::keep(&h.store, APP, &clone, KeepPurpose::Quarantine, None, None)
        .await
        .expect("keep");
    let db = sqlx_connect(&h._scratch.0.join("node.db")).await;
    let id = crate::util::quarantine_id();
    // No `volume_id`: the row cannot be read back whole.
    sqlx::query(
        "INSERT INTO quarantine (kind, path, quarantined_at, reason, quarantine_id, snapshot_id) \
         VALUES ('volume', ?, '2026-11-10T09:00:00Z', 'changed since the base', ?, ?)",
    )
    .bind(clone.location.display().to_string())
    .bind(&id)
    .bind(&clone.snapshot_id)
    .execute(&db)
    .await
    .expect("row");
    assert!(quarantined(&h).await.is_empty(), "left out of the list");

    h.receiver
        .inner
        .quarantines
        .delete(std::slice::from_ref(&id))
        .await;
    assert!(!clone.location.exists(), "the clone is deleted");
    assert_eq!(
        h.store
            .kept_snapshot(&clone.snapshot_id)
            .await
            .expect("store"),
        None
    );
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM quarantine")
        .fetch_one(&db)
        .await
        .expect("count");
    assert_eq!(left, 0);
}

#[tokio::test]
async fn a_failed_hash_is_tried_again_without_a_restart() {
    let h = Harness::new("handover-hash-retry").await;
    let mut listeners = BTreeMap::new();
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let quarantines = h.receiver.inner.quarantines.clone();
    let gate = quarantines.inner.hash_gate.lock().await;
    quarantines
        .inner
        .fail_hashes
        .store(1, std::sync::atomic::Ordering::SeqCst);
    pass(&h, &mut listeners).await;
    let copy = quarantined(&h).await[0].clone();
    // A shorter copy hashes quickly; it keeps its data and its holes.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&copy.path)
        .expect("open")
        .set_len(8 * CELL)
        .expect("shorten");

    // The first hash fails. Its wait is recorded before another hash of
    // the copy may start.
    drop(gate);
    wait_for("the failed hash to end", || async {
        !quarantines.hashing(&copy.quarantine_id)
    })
    .await;
    assert!(quarantines.hash_failed(&copy.quarantine_id));
    assert!(quarantined(&h).await[0].sha256.is_none());

    // Later passes hash it again.
    let mut statuses = h.receiver.quarantine_statuses();
    let expected = sha256_of_file(&copy.path).await;
    tokio::time::timeout(Duration::from_secs(600), async {
        while quarantined(&h).await[0].sha256.is_none() {
            pass(&h, &mut listeners).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the hash is tried again");
    assert_eq!(
        quarantined(&h).await[0].sha256.as_deref(),
        Some(expected.as_str())
    );
    let listed = quarantines_where(&mut statuses, |list| {
        list.first().is_some_and(|entry| entry.sha256.is_some())
    })
    .await;
    assert_eq!(listed[0].sha256.as_deref(), Some(expected.as_str()));
    assert!(!quarantines.hash_failed(&copy.quarantine_id));

    // A hash started from a list read before the hash was recorded reads the
    // record again and hashes nothing.
    quarantines
        .inner
        .fail_hashes
        .store(1, std::sync::atomic::Ordering::SeqCst);
    quarantines.hash(&copy);
    wait_for("the stale hash to end", || async {
        !quarantines.hashing(&copy.quarantine_id)
    })
    .await;
    assert_eq!(
        quarantines
            .inner
            .fail_hashes
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no second hash ran"
    );
    assert!(!quarantines.hash_failed(&copy.quarantine_id));
    assert_eq!(
        quarantined(&h).await[0].sha256.as_deref(),
        Some(expected.as_str())
    );
}
