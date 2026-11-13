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
use quoro_proto::wire::{DesiredStatus, QuarantineStatus, SpecNetwork, SpecVm, SpecVolume};

use super::*;
use crate::kept::{self, KeepPurpose};
use crate::lease::LeaseView;
use crate::reconcile::{ReconcileConfig, ReconcileOptions, Reconciler};
use crate::store::{HandOverMark, VolumeQuarantineRecord};
use crate::warm::quarantine::Quarantines;
use crate::warm::quarantine::tests::{assert_primary_accepts, assert_rfc3339, plain_sha256};
use crate::warm::receiver::handover::{HandOver, REFUSALS_BEFORE_ERROR};
use crate::warm::sender::{SenderOptions, WarmSender};

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

impl Harness {
    /// The rev of the current desired state.
    fn rev(&self) -> u64 {
        self.desired.borrow().as_ref().expect("a snapshot").rev
    }
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
    // The warm row and the returning flag each pass writes for a target
    // entry of a volume the node holds as its own.
    assert!(
        h.store
            .add_return_pending_target(VOL, APP, 1, h.rev())
            .await
            .expect("warm row")
    );
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
async fn a_marked_unchanged_or_gone_run_changes_nothing_until_the_volume_directory_is_back() {
    for gone in [false, true] {
        let h = Harness::new(if gone {
            "handover-marked-gone-unmounted"
        } else {
            "handover-marked-unchanged-unmounted"
        })
        .await;
        let path = returning_source(&h).await;
        let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
        if gone {
            h.engine.delete_volume(&vol()).await.expect("delete");
        }
        // The run that writes the mark fails to delete the live file.
        h.engine
            .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
        assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
        let mark = if gone {
            HandOverMark::Gone
        } else {
            HandOverMark::Unchanged
        };
        assert_eq!(
            h.store.volume_hand_over_mark(VOL).await.expect("store"),
            Some(mark.clone())
        );

        // The state disk did not mount: the volume directory is empty, while
        // the store and the engine still name every file. Neither the base
        // nor the live file shows the directory: the engine answers
        // `NotFound` for the live file it no longer finds.
        let mut paths = vec![base.location.as_path()];
        if !gone {
            paths.push(path.as_path());
        }
        let moved = move_away(&h, &paths);
        for _ in 0..2 {
            assert!(
                matches!(hand_over(&h).await, HandOver::Failed(_)),
                "gone {gone}"
            );
            assert!(
                h.store.volume(VOL).await.expect("store").is_some(),
                "gone {gone}: the row stays"
            );
            assert_eq!(
                h.store.volume_hand_over_mark(VOL).await.expect("store"),
                Some(mark.clone()),
                "gone {gone}: the mark stands"
            );
            let error = h.row().await.error.expect("an error is shown");
            assert!(error.contains("may not be the real one"), "{error}");
        }
        // The engine was not asked to delete the live file.
        assert_eq!(
            h.engine.volumes().iter().any(|(id, _)| *id == vol()),
            !gone,
            "gone {gone}"
        );

        // Mounted again: the hand-over ends.
        move_back(&moved);
        assert_eq!(hand_over(&h).await, HandOver::Done(None), "gone {gone}");
        assert_handed_over(&h).await;
        assert!(quarantined(&h).await.is_empty());
    }
}

/// A marked run that fails with nothing changed: the row and the mark stay,
/// and so does the live file when there is one.
async fn assert_marked_run_changes_nothing(h: &Harness, mark: &HandOverMark, what: &str) {
    let outcome = hand_over(h).await;
    assert!(
        matches!(&outcome, HandOver::Failed(reason) if reason.contains("may not be the real one")),
        "{what}: {outcome:?}"
    );
    assert!(
        h.store.volume(VOL).await.expect("store").is_some(),
        "{what}: the row stays"
    );
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(mark.clone()),
        "{what}: the mark stands"
    );
    let error = h.row().await.error.expect("an error is shown");
    assert!(error.contains("may not be the real one"), "{what}: {error}");
}

/// A marked run the engine refuses, as while a VM it holds attaches the
/// live file: nothing changes, the live file stays, and the refusal adds no
/// error of its own, as any refusal until it lasts
/// [`REFUSALS_BEFORE_ERROR`] passes. Returns the refusal's reason.
async fn assert_marked_run_refused(h: &Harness, mark: &HandOverMark, what: &str) -> String {
    let error = h.row().await.error;
    let HandOver::Refused(reason) = hand_over(h).await else {
        panic!("{what}: the run is refused");
    };
    assert!(
        h.store.volume(VOL).await.expect("store").is_some(),
        "{what}: the row stays"
    );
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(mark.clone()),
        "{what}: the mark stands"
    );
    assert!(h.volume_path.exists(), "{what}: the live file stays");
    assert_eq!(h.row().await.error, error, "{what}: no error of its own");
    reason
}

#[tokio::test]
async fn a_file_marked_unchanged_whose_base_is_lost_goes_on_the_evidence_of_the_live_file() {
    let h = Harness::new("handover-unchanged-lost-base").await;
    returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    // The run that writes the mark fails to delete the live file.
    h.engine
        .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    let mark = HandOverMark::Unchanged;
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(mark.clone())
    );
    // The base is lost out of band (disk damage, removed by hand); the
    // store and the engine still name it. No other file of the volume is
    // left, so only the live file can show the directory.
    std::fs::remove_file(&base.location).expect("lose the base");

    // An engine that neither finds nor misses the live file shows nothing.
    // A VM it holds attaches the file, which only the engine sees: the run
    // waits, as for any VM that attaches the volume, with no error yet.
    let attaching = engine_vm(&h.engine, "vm_01JD0WARM0000000000000002", true).await;
    for _ in 0..2 {
        let reason = assert_marked_run_refused(&h, &mark, "a VM attaches the live file").await;
        assert!(reason.contains("attached"), "{reason}");
    }
    h.engine.delete(&attaching).await.expect("delete");
    // It cannot read the file: the run fails, and says why.
    h.engine.fail_next(
        FakeOp::VolumeChangedRanges,
        EngineError::Io("injected".to_owned()),
    );
    assert_marked_run_changes_nothing(&h, &mark, "an I/O error").await;
    assert!(h.volume_path.exists());

    // The engine finds the live file in its volume directory: that shows
    // the directory to be the real one, and the file the mark proved
    // unchanged goes.
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    assert!(quarantined(&h).await.is_empty());
}

/// A `gone` mark was written because the live file was missing; the delete
/// or the row's drop then failed, and the live file was put back since, as
/// by an operator. Nothing proved it unchanged, so it is compared again
/// before anything goes: the marked run deletes nothing and clears the
/// mark, and the next run keeps the file as a quarantined copy. That holds
/// whether the base is still there or was lost too.
#[tokio::test]
async fn a_live_file_back_after_a_gone_mark_is_compared_again_before_anything_goes() {
    for lose_base in [true, false] {
        let what = format!("base lost {lose_base}");
        let h = Harness::new(if lose_base {
            "handover-gone-back-lost-base"
        } else {
            "handover-gone-back"
        })
        .await;
        let path = returning_source(&h).await;
        let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
        let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
        h.engine.delete_volume(&vol()).await.expect("delete");
        // The run that writes the mark fails before the row goes.
        h.engine
            .fail_next(FakeOp::DeleteVolume, EngineError::Io("injected".to_owned()));
        assert!(matches!(hand_over(&h).await, HandOver::Failed(_)), "{what}");
        let mark = HandOverMark::Gone;
        assert_eq!(
            h.store.volume_hand_over_mark(VOL).await.expect("store"),
            Some(mark.clone()),
            "{what}"
        );
        if lose_base {
            std::fs::remove_file(&base.location).expect("lose the base");
        }
        // A live file with data the target never got is put back.
        h.engine
            .ensure_volume(&vol(), 1)
            .await
            .expect("the file is back");
        write_at(&path, 0, &[9_u8; 4096]);

        // A VM the engine holds attaches the file: the run waits, and the
        // mark stands.
        let attaching = engine_vm(&h.engine, "vm_01JD0WARM0000000000000002", true).await;
        let reason = assert_marked_run_refused(&h, &mark, &what).await;
        assert!(reason.contains("attached"), "{what}: {reason}");
        h.engine.delete(&attaching).await.expect("delete");

        // The engine finds the file: nothing is deleted, and the mark goes.
        let outcome = hand_over(&h).await;
        assert!(
            matches!(&outcome, HandOver::Failed(reason) if reason.contains("compares the live file again")),
            "{what}: {outcome:?}"
        );
        assert_eq!(
            h.store.volume_hand_over_mark(VOL).await.expect("store"),
            None,
            "{what}: the mark goes"
        );
        assert!(
            h.store.volume(VOL).await.expect("store").is_some(),
            "{what}: the row stays"
        );
        assert_eq!(
            read_at(&path, 0, 4096),
            vec![9_u8; 4096],
            "{what}: the file stays"
        );
        assert!(quarantined(&h).await.is_empty(), "{what}");
        let error = h.row().await.error.expect("an error is shown");
        assert!(error.contains("compares the live file again"), "{error}");

        // The next run compares the file and keeps it as a quarantined copy
        // before it goes.
        let HandOver::Done(Some(id)) = hand_over(&h).await else {
            panic!("{what}: the hand-over ends with a copy");
        };
        let records = quarantined(&h).await;
        assert_eq!(records.len(), 1, "{what}: {records:?}");
        assert_eq!(records[0].quarantine_id, id, "{what}");
        assert_eq!(
            read_at(&records[0].path, 0, 4096),
            vec![9_u8; 4096],
            "{what}: the copy holds the put-back data"
        );
        assert_handed_over(&h).await;
    }
}

#[tokio::test]
async fn a_marked_copy_whose_row_cannot_be_read_whole_still_hands_over() {
    let h = Harness::new("handover-marked-unreadable").await;
    let path = returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let copy = marked_copy(&h).await;
    sqlx::query("UPDATE quarantine SET app_id = NULL WHERE quarantine_id = ?")
        .bind(&copy.quarantine_id)
        .execute(h.store.pool())
        .await
        .expect("damage the row");
    assert!(
        h.store
            .volume_quarantine(&copy.quarantine_id)
            .await
            .is_err(),
        "the row cannot be read whole"
    );

    // The marked run reads only the clone's path: the clone is there, so
    // the live file goes and the copy keeps its writes.
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    assert!(copy.path.exists());
    assert_eq!(read_at(&copy.path, 7 * CELL + 512, 8192), pattern(8192, 4));
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
        h.locks.clone(),
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
        h.locks.clone(),
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
    // error says why. The live file is lost through the engine, or removed
    // by hand while the engine still names the volume; either way the
    // engine answers `NotFound` for it, and a missing live file shows
    // nothing of the directory it is missing from (issue #762 case 1(b)).
    // Only the volume's own files count as evidence: whether another
    // volume's live file or kept snapshot may count too awaits a ruling
    // (#762), so this case keeps refusing until a file is back.
    for by_hand in [false, true] {
        let h = Harness::new(if by_hand {
            "handover-gone-unproved-by-hand"
        } else {
            "handover-gone-unproved"
        })
        .await;
        let path = returning_source(&h).await;
        if by_hand {
            std::fs::remove_file(&path).expect("remove the live file");
            assert!(
                h.engine.volumes().iter().any(|(id, _)| *id == vol()),
                "the engine still names the volume"
            );
        } else {
            h.engine.delete_volume(&vol()).await.expect("delete");
        }
        for _ in 0..2 {
            assert!(
                matches!(hand_over(&h).await, HandOver::Failed(_)),
                "by hand {by_hand}"
            );
            assert!(
                h.store.volume(VOL).await.expect("store").is_some(),
                "by hand {by_hand}: the row stays"
            );
            assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
            let error = h.row().await.error.expect("an error is shown");
            assert!(
                error.contains("may not be the real one"),
                "by hand {by_hand}: {error}"
            );
        }
        assert!(quarantined(&h).await.is_empty());
        assert!(h.engine.volume_snapshots().is_empty(), "no copy taken");
    }

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
    // Only the file goes: the fake engine still names the clone. On a real
    // node the engine would answer `NotFound` for it, and the hash would
    // forget the record (section 9 step 2); the record stays here because
    // the hash gate keeps the hash from running.
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

/// Issue #762 case 1(a): no base is kept, so once the copy's clone is
/// removed by hand the live file is the only file of the volume left. It
/// shows the volume directory to be the real one: the mark goes, and the
/// next run takes a new copy, where every run used to fail.
#[tokio::test]
async fn with_no_kept_base_the_live_file_shows_the_directory_once_the_clone_is_missing() {
    let h = Harness::new("handover-no-base-live").await;
    let path = returning_source(&h).await;
    diverge(&path);
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    // The live file is quarantined, and its delete fails once.
    let copy = marked_copy(&h).await;
    assert_eq!(kept_purposes(&h).await, vec!["quarantine"], "no base");
    std::fs::remove_file(&copy.path).expect("remove the clone");
    let mark = HandOverMark::Copy(copy.quarantine_id.clone());

    // An engine that neither finds nor misses the live file shows nothing.
    // A refusal holds the run without an error of its own; any other error
    // fails it.
    h.engine.fail_next(
        FakeOp::VolumeChangedRanges,
        EngineError::Conflict("injected".to_owned()),
    );
    let reason = assert_marked_run_refused(&h, &mark, "a refusal").await;
    assert!(reason.contains("injected"), "{reason}");
    assert_live_kept(&h);
    assert_eq!(quarantined(&h).await, vec![copy.clone()]);
    h.engine.fail_next(
        FakeOp::VolumeChangedRanges,
        EngineError::Io("injected".to_owned()),
    );
    assert_marked_run_changes_nothing(&h, &mark, "an I/O error").await;
    assert_live_kept(&h);
    assert_eq!(quarantined(&h).await, vec![copy.clone()]);

    // The engine finds the live file: the mark goes, the record stays, and
    // the live file stays to keep its writes.
    let outcome = hand_over(&h).await;
    assert!(
        matches!(&outcome, HandOver::Failed(reason) if reason.contains("compares the live file again")),
        "{outcome:?}"
    );
    assert_live_kept(&h);
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        None
    );
    assert_eq!(
        quarantined(&h).await,
        vec![copy.clone()],
        "nothing forgotten"
    );

    // The next run compares again and keeps one new copy beside it.
    let HandOver::Done(Some(id)) = hand_over(&h).await else {
        panic!("the hand-over ends");
    };
    assert_ne!(id, copy.quarantine_id);
    let records = quarantined(&h).await;
    assert_eq!(
        records
            .iter()
            .map(|record| record.quarantine_id.as_str())
            .collect::<Vec<_>>(),
        vec![copy.quarantine_id.as_str(), id.as_str()]
    );
    assert_eq!(
        read_at(&records[1].path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
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

// A `deleted` entry reaches a returning source only through the stale-copy
// rule of section 5.11 (ADR 0267), which never removes a volume of the
// node's own: the node keeps its file, its row and its kept snapshots, and
// only the warm record goes.

/// The old VM of the app, on the engine and in the spec as `standby`.
const OLD_VM: &str = "vm_01JD0WARM0000000000000001";

/// The node's reconciler, on the harness's engine, store and locks.
fn node_reconciler(h: &Harness) -> Reconciler<FakeEngine> {
    let (reconciler, _observed) = Reconciler::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        ReconcileConfig {
            bridge: "quoro0".to_owned(),
            dns: Vec::new(),
        },
        ReconcileOptions {
            tick: Duration::from_millis(10),
            stop_timeout: Duration::from_millis(20),
            backoff_base: Duration::from_millis(1),
            backoff_max: Duration::from_millis(8),
            healthy_after: Duration::from_secs(60),
        },
    );
    reconciler.with_warm_locks(h.locks.clone())
}

/// The node's sender, on the harness's engine, store and locks.
fn node_sender(h: &Harness) -> WarmSender<FakeEngine> {
    let (board, _) = StatusBoard::new();
    WarmSender::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        h.desired.subscribe(),
        LeaseView::default(),
        h.locks.clone(),
        board,
        SenderOptions::quick(),
    )
}

/// A new revision with `vms` and `volumes`, and a network so the reconciler
/// applies it; returns it.
fn send_spec(h: &Harness, vms: Vec<SpecVm>, volumes: Vec<SpecVolumeFile>) -> Snapshot {
    h.desired.send_modify(|current| {
        let snapshot = current.as_mut().expect("a snapshot");
        snapshot.rev += 1;
        snapshot.state.rev = snapshot.rev;
        snapshot.state.spec.vms = vms;
        snapshot.state.spec.volumes = volumes;
        snapshot.state.spec.network = Some(SpecNetwork {
            subnet: "10.42.1.0/24".to_owned(),
            gateway: "10.42.1.1".to_owned(),
        });
    });
    h.desired.borrow().clone().expect("a snapshot")
}

/// A returning source before its hand-over finished.
struct Returning {
    /// Its `present` target entry.
    target: SpecVolumeFile,
    base: VolumeSnapshot,
    shipping: VolumeSnapshot,
    /// The old VM, which holds the hand-over back.
    old: VmId,
    /// The live file's bytes where a divergent write goes.
    live: Vec<u8>,
}

/// The returning source as the failover leaves it: its own row and live
/// file, holding a write the target lacks when `diverged`, the kept base and
/// shipping snapshots, and the old VM. A pass records the target, flags the
/// own row and reports the copy; the hand-over waits for the VM.
async fn returning_before_its_hand_over(
    h: &Harness,
    listeners: &mut BTreeMap<SocketAddr, JoinHandle<()>>,
    diverged: bool,
) -> Returning {
    let path = returning_source(h).await;
    // The pass writes the flag, not the setup.
    h.store
        .clear_volume_return_pending(VOL)
        .await
        .expect("clear");
    let base = keep_snapshot(h, KeepPurpose::WarmBase, 1).await;
    let shipping = keep_snapshot(h, KeepPurpose::WarmShipping, 2).await;
    if diverged {
        diverge(&path);
    }
    let old = engine_vm(&h.engine, OLD_VM, false).await;
    let target = h
        .desired
        .borrow()
        .as_ref()
        .expect("a snapshot")
        .state
        .spec
        .volumes[0]
        .clone();
    send_spec(h, vec![standby_vm(OLD_VM)], vec![target.clone()]);
    pass(h, listeners).await;
    assert!(h.store.volume_return_pending(VOL).await.expect("store"));
    assert!(h.volume_path.exists(), "the hand-over waits for the old VM");
    assert_eq!(h.status().volume_id, VOL, "the copy is reported");
    assert!(h.status().error.is_none(), "{:?}", h.status());
    let live = read_at(&path, 7 * CELL + 512, 8192);
    Returning {
        target,
        base,
        shipping,
        old,
        live,
    }
}

/// The stale-copy rule's `deleted` entry, without `warm`, in the revision
/// that also drops the old VM, which the engine no longer holds.
async fn deleted_entry_arrives(h: &Harness, returning: &Returning) -> Snapshot {
    h.engine.delete(&returning.old).await.expect("delete");
    send_spec(
        h,
        Vec::new(),
        vec![SpecVolumeFile {
            state: SpecVolumeState::Deleted,
            warm: None,
            ..returning.target.clone()
        }],
    )
}

/// The file with its bytes, the own row, the flag and the kept snapshots are
/// all still there, and no copy was taken.
async fn assert_own_volume_kept(h: &Harness, returning: &Returning, what: &str) {
    assert!(h.volume_path.exists(), "{what}: the live file stays");
    assert_eq!(
        read_at(&h.volume_path, 7 * CELL + 512, 8192),
        returning.live,
        "{what}: with its bytes"
    );
    let row = h.store.volume(VOL).await.expect("store");
    assert_eq!(
        row.map(|row| row.state),
        Some(VolumeState::Ready),
        "{what}: the own row stays"
    );
    assert!(
        h.store.volume_return_pending(VOL).await.expect("store"),
        "{what}"
    );
    assert_eq!(
        kept_purposes(h).await,
        vec!["warm_base", "warm_shipping"],
        "{what}: the kept snapshots stay"
    );
    assert!(
        returning.base.location.exists() && returning.shipping.location.exists(),
        "{what}"
    );
    assert!(quarantined(h).await.is_empty(), "{what}");
    assert!(
        !h.store.volume_handing_over(VOL).await.expect("store"),
        "{what}"
    );
}

/// The state the reconciler reports for the volume in `status_report.volumes`.
fn reported(reconciler: &Reconciler<FakeEngine>) -> Option<VolumeState> {
    reconciler
        .volume_statuses()
        .borrow()
        .iter()
        .find(|status| status.volume_id == VOL)
        .map(|status| status.state)
}

/// No VM exists or was stopped: nothing touched a VM for the volume.
async fn assert_no_vm_call(h: &Harness, what: &str) {
    assert!(
        h.engine.list().await.expect("list").is_empty(),
        "{what}: no VM"
    );
    assert!(h.engine.stop_calls().is_empty(), "{what}: no stop");
}

/// All three cases of the stale-copy rule reach the node as the same
/// `deleted` entry without `warm`, with the old VM gone from `spec.vms`: the
/// app has another warm node, it left `warm`, or it or its volume is being
/// deleted.
#[tokio::test]
async fn a_deleted_entry_keeps_a_returning_source_s_own_volume() {
    for trigger in [
        "the app has another warm node",
        "the app left warm",
        "the app or the volume is being deleted",
    ] {
        let tag: String = trigger.split(' ').skip(2).collect::<Vec<_>>().join("-");
        let h = Harness::new(&format!("returning-deleted-{tag}")).await;
        let mut listeners = BTreeMap::new();
        let returning = returning_before_its_hand_over(&h, &mut listeners, true).await;
        let mut reconciler = node_reconciler(&h);
        let sender = node_sender(&h);
        let deleted = deleted_entry_arrives(&h, &returning).await;

        // The reconciler first, while the warm row still stands; then the
        // receiver and the sender; then the reconciler again.
        reconciler.reconcile_once(Some(&deleted)).await;
        pass(&h, &mut listeners).await;
        sender.pass_once().await;
        reconciler.reconcile_once(Some(&deleted)).await;
        assert_own_volume_kept(&h, &returning, trigger).await;
        assert!(h.row().await.removed, "{trigger}: the warm row is removed");
        assert!(
            h.statuses.borrow().is_empty(),
            "{trigger}: the copy is no longer reported: {:?}",
            h.statuses.borrow()
        );
        assert_eq!(
            reported(&reconciler),
            Some(VolumeState::Ready),
            "{trigger}: reported with its state"
        );
        assert_eq!(
            h.store.applied_rev().await.expect("store"),
            deleted.rev,
            "{trigger}: the revision is not held"
        );
        assert_no_vm_call(&h, trigger).await;

        // The entry leaves the spec: the warm row is forgotten, and the file
        // is an orphan the node keeps.
        let gone = send_spec(&h, Vec::new(), Vec::new());
        pass(&h, &mut listeners).await;
        sender.pass_once().await;
        reconciler.reconcile_once(Some(&gone)).await;
        assert_own_volume_kept(&h, &returning, trigger).await;
        assert!(h.store.warm_target(VOL).await.expect("store").is_none());
        assert_eq!(
            reported(&reconciler),
            Some(VolumeState::Orphaned),
            "{trigger}"
        );
        assert!(h.statuses.borrow().is_empty(), "{trigger}");
        assert_no_vm_call(&h, trigger).await;
    }
}

/// An older revision `rev`, in which the node runs the volume as its
/// source, with no VM.
fn older_source_revision(h: &Harness, returning: &Returning, rev: u64) -> Snapshot {
    let mut older = h.desired.borrow().clone().expect("a snapshot");
    older.rev = rev;
    older.state.rev = rev;
    older.state.spec.vms = Vec::new();
    older.state.spec.volumes = vec![SpecVolumeFile {
        warm: returning.target.warm.clone().map(|warm| SpecWarm {
            role: WarmRole::Source,
            ..warm
        }),
        ..returning.target.clone()
    }];
    older
}

/// A reconciler pass on an older revision leaves a hand-over's mark alone
/// while the flag names a newer one, with the flag and the warm row: the
/// next hand-over finds the mark and takes no second copy.
#[tokio::test]
async fn a_reconciler_pass_on_an_older_revision_leaves_the_hand_over_mark() {
    let h = Harness::new("returning-older-rev-mark").await;
    let mut listeners = BTreeMap::new();
    let returning = returning_before_its_hand_over(&h, &mut listeners, true).await;
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let mut reconciler = node_reconciler(&h);
    let named = h.rev();
    h.engine.delete(&returning.old).await.expect("delete");
    // A hand-over on N quarantined the file and marked the row, and stopped
    // before it ended.
    let copy = marked_copy(&h).await;

    let older = older_source_revision(&h, &returning, named - 1);
    reconciler.reconcile_once(Some(&older)).await;
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        Some(HandOverMark::Copy(copy.quarantine_id.clone())),
        "the mark stays"
    );
    assert_eq!(
        h.store.volume_return_pending_rev(VOL).await.expect("store"),
        Some(named),
        "the flag stays"
    );
    assert!(h.store.warm_target(VOL).await.expect("store").is_some());
    assert_ne!(h.store.applied_rev().await.expect("store"), older.rev);

    // The next hand-over goes on from the mark: no second copy.
    assert_eq!(hand_over(&h).await, HandOver::Done(None));
    assert_handed_over(&h).await;
    assert_eq!(quarantined(&h).await, vec![copy]);
}

/// A reconciler pass on an older revision, where the node still runs the
/// volume as its source, does not clear a flag a newer revision set: the
/// volume waits for a newer revision, and the `deleted` entry that follows
/// keeps the file and the base.
#[tokio::test]
async fn a_reconciler_pass_on_an_older_revision_leaves_the_flag() {
    let h = Harness::new("returning-older-rev").await;
    let mut listeners = BTreeMap::new();
    let returning = returning_before_its_hand_over(&h, &mut listeners, true).await;
    let mut reconciler = node_reconciler(&h);
    let sender = node_sender(&h);
    let named = h.rev();
    assert_eq!(
        h.store.volume_return_pending_rev(VOL).await.expect("store"),
        Some(named)
    );
    h.engine.delete(&returning.old).await.expect("delete");

    let older = older_source_revision(&h, &returning, named - 1);
    reconciler.reconcile_once(Some(&older)).await;
    assert!(h.store.volume_return_pending(VOL).await.expect("store"));
    assert!(h.store.warm_target(VOL).await.expect("store").is_some());
    assert_ne!(
        h.store.applied_rev().await.expect("store"),
        older.rev,
        "the stale revision is not applied"
    );

    // N + 1: the stale-copy rule's `deleted` entry.
    let deleted = send_spec(
        &h,
        Vec::new(),
        vec![SpecVolumeFile {
            state: SpecVolumeState::Deleted,
            warm: None,
            ..returning.target.clone()
        }],
    );
    reconciler.reconcile_once(Some(&deleted)).await;
    pass(&h, &mut listeners).await;
    sender.pass_once().await;
    reconciler.reconcile_once(Some(&deleted)).await;
    assert_own_volume_kept(&h, &returning, "older revision").await;
    assert_eq!(reported(&reconciler), Some(VolumeState::Ready));
    assert_eq!(h.store.applied_rev().await.expect("store"), deleted.rev);
}

/// A reconciler pass on the `deleted` revision that runs only after the
/// receiver forgot its warm row reads the flag fresh, and keeps the file.
#[tokio::test]
async fn a_reconciler_pass_behind_the_receiver_still_keeps_the_file() {
    let h = Harness::new("returning-lagging").await;
    let mut listeners = BTreeMap::new();
    let returning = returning_before_its_hand_over(&h, &mut listeners, true).await;
    let mut reconciler = node_reconciler(&h);
    let deleted = deleted_entry_arrives(&h, &returning).await;
    pass(&h, &mut listeners).await;
    assert!(h.row().await.removed);
    send_spec(&h, Vec::new(), Vec::new());
    pass(&h, &mut listeners).await;
    assert!(h.store.warm_target(VOL).await.expect("store").is_none());

    reconciler.reconcile_once(Some(&deleted)).await;
    assert_own_volume_kept(&h, &returning, "lagging").await;
    assert_eq!(reported(&reconciler), Some(VolumeState::Ready));
    assert_eq!(h.store.applied_rev().await.expect("store"), deleted.rev);
    assert_no_vm_call(&h, "lagging").await;
}

/// Named the volume's target again after a `deleted` entry and the orphan
/// that followed, the node runs the hand-over against the base it kept: an
/// unchanged file goes, a changed one is kept as a quarantined copy.
#[tokio::test]
async fn named_the_target_again_the_hand_over_compares_with_the_kept_base() {
    for diverged in [false, true] {
        let what = if diverged { "changed" } else { "unchanged" };
        let h = Harness::new(&format!("returning-again-{what}")).await;
        let mut listeners = BTreeMap::new();
        let returning = returning_before_its_hand_over(&h, &mut listeners, diverged).await;
        let mut reconciler = node_reconciler(&h);
        let deleted = deleted_entry_arrives(&h, &returning).await;
        reconciler.reconcile_once(Some(&deleted)).await;
        pass(&h, &mut listeners).await;
        let gone = send_spec(&h, Vec::new(), Vec::new());
        pass(&h, &mut listeners).await;
        reconciler.reconcile_once(Some(&gone)).await;
        assert_eq!(reported(&reconciler), Some(VolumeState::Orphaned), "{what}");
        assert_own_volume_kept(&h, &returning, what).await;
        let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;

        let again = send_spec(&h, Vec::new(), vec![returning.target.clone()]);
        reconciler.reconcile_once(Some(&again)).await;
        pass(&h, &mut listeners).await;
        assert_handed_over(&h).await;
        let records = quarantined(&h).await;
        if diverged {
            assert_eq!(records.len(), 1, "{records:?}");
            assert_eq!(
                read_at(&records[0].path, 7 * CELL + 512, 8192),
                pattern(8192, 4),
                "the copy keeps the divergent write"
            );
            assert_eq!(
                kept_purposes(&h).await,
                vec!["quarantine", "warm_base", "warm_shipping"]
            );
        } else {
            assert!(records.is_empty(), "{records:?}");
            assert_eq!(kept_purposes(&h).await, vec!["warm_base", "warm_shipping"]);
        }
        assert!(h.status().error.is_none(), "{what}: {:?}", h.status());
        reconciler.reconcile_once(Some(&again)).await;
        assert_eq!(reported(&reconciler), None, "{what}: a warm copy now");
        assert_no_vm_call(&h, what).await;
    }
}

/// The flag is durable before `status_report.warm` lists the target: a
/// publish that finds the own row without it leaves the entry out, and a
/// pass that cannot write it neither reports the target nor hands it over.
#[tokio::test]
async fn the_target_is_reported_only_once_the_returning_flag_is_durable() {
    let h = Harness::new("returning-flag-order").await;
    let mut listeners = BTreeMap::new();
    returning_source(&h).await;
    keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    // The warm row as a stream's record leaves it, without the flag.
    h.store
        .clear_volume_return_pending(VOL)
        .await
        .expect("clear");
    let snapshots = h.engine.volume_snapshots().len();
    let model = first_model();
    h.post(
        stream(&header(5, 1, None), &model, &model.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("busy", None);
    assert!(h.statuses.borrow().is_empty(), "{:?}", h.statuses.borrow());

    // The flag cannot be written.
    sqlx::query(
        "CREATE TRIGGER no_return_pending BEFORE UPDATE OF return_pending ON volumes \
         BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(h.store.pool())
    .await
    .expect("trigger");
    pass(&h, &mut listeners).await;
    assert!(!h.store.volume_return_pending(VOL).await.expect("store"));
    assert!(h.volume_path.exists(), "no hand-over");
    assert!(h.store.volume(VOL).await.expect("store").is_some());
    assert!(!h.store.volume_handing_over(VOL).await.expect("store"));
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(h.engine.volume_snapshots().len(), snapshots);
    assert!(h.statuses.borrow().is_empty(), "{:?}", h.statuses.borrow());

    // Once it can be written, the pass writes it before it publishes: while
    // the old VM holds the hand-over back, the target is reported flagged.
    sqlx::query("DROP TRIGGER no_return_pending")
        .execute(h.store.pool())
        .await
        .expect("drop trigger");
    let old = engine_vm(&h.engine, OLD_VM, false).await;
    let target = h
        .desired
        .borrow()
        .as_ref()
        .expect("a snapshot")
        .state
        .spec
        .volumes[0]
        .clone();
    send_spec(&h, vec![standby_vm(OLD_VM)], vec![target.clone()]);
    pass(&h, &mut listeners).await;
    assert!(h.store.volume_return_pending(VOL).await.expect("store"));
    assert_eq!(h.status().volume_id, VOL);
    assert!(h.volume_path.exists());

    // And the hand-over runs once the VM is gone.
    h.engine.delete(&old).await.expect("delete");
    send_spec(&h, Vec::new(), vec![target]);
    pass(&h, &mut listeners).await;
    assert_handed_over(&h).await;
    assert_eq!(h.status().volume_id, VOL);
}

// Section 9 step 2: the record of a copy whose clone is gone before it was
// hashed is forgotten only when the engine answers `NotFound` for the clone,
// another kept snapshot of the volume answers it or, when none does, the
// engine finds the volume's live file, and one open handle of the clone's
// directory shows the clone missing and that snapshot or live file there.

#[tokio::test]
async fn an_unhashed_copy_whose_clone_is_gone_is_forgotten() {
    let h = Harness::new("handover-clone-gone").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let quarantines = h.receiver.inner.quarantines.clone();
    let mut statuses = h.receiver.quarantine_statuses();
    let gate = quarantines.inner.hash_gate.lock().await;
    let lost = marked_copy(&h).await;
    assert_eq!(statuses.borrow_and_update().len(), 1);
    let clone = h
        .store
        .kept_snapshot(&lost.snapshot_id)
        .await
        .expect("store")
        .expect("kept");
    // The engine no longer has the clone; the base still answers it.
    h.engine
        .delete_volume_snapshot(&clone.snapshot)
        .await
        .expect("delete the clone");

    drop(gate);
    quarantines_where(&mut statuses, Vec::is_empty).await;
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(
        h.store
            .kept_snapshot(&lost.snapshot_id)
            .await
            .expect("store"),
        None
    );
    assert!(!quarantines.hash_failed(&lost.quarantine_id));
    // No file is touched.
    assert!(base.location.exists());
    assert!(path.exists());

    // The mark stood for the copy: the next run clears it and compares the
    // live file again.
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        None
    );
}

#[tokio::test]
async fn a_lost_clone_keeps_its_record_while_the_engine_cannot_tell_or_no_other_file_answers() {
    let h = Harness::new("handover-clone-unproved").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let quarantines = h.receiver.inner.quarantines.clone();
    let gate = quarantines.inner.hash_gate.lock().await;
    let lost = marked_copy(&h).await;
    let clone = h
        .store
        .kept_snapshot(&lost.snapshot_id)
        .await
        .expect("store")
        .expect("kept");
    h.engine
        .delete_volume_snapshot(&clone.snapshot)
        .await
        .expect("delete the clone");
    let kept_as_is = |h: &Harness| {
        let store = h.store.clone();
        let lost = lost.clone();
        async move {
            assert_eq!(
                store.volume_quarantines().await.expect("records"),
                vec![lost.clone()]
            );
            assert!(
                store
                    .kept_snapshot(&lost.snapshot_id)
                    .await
                    .expect("store")
                    .is_some()
            );
        }
    };

    // A pool that does not mount answers `Unsupported` for the clone.
    h.engine.fail_next(
        FakeOp::VolumeChangedRanges,
        EngineError::Unsupported("the volume pool does not mount".to_owned()),
    );
    drop(gate);
    wait_for("the hash to fail", || async {
        quarantines.hash_failed(&lost.quarantine_id)
    })
    .await;
    kept_as_is(&h).await;

    // No other file of the volume answers the engine: no other kept
    // snapshot, and the live file is missing while the engine still names
    // the volume. The directory is not shown to be the real one.
    h.engine
        .delete_volume_snapshot(&base)
        .await
        .expect("delete the base");
    let moved = move_away(&h, &[&path]);
    quarantines.hash(&lost);
    wait_for("the second hash to end", || async {
        !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert!(quarantines.hash_failed(&lost.quarantine_id));
    kept_as_is(&h).await;
    move_back(&moved);
    assert!(path.exists());
}

#[tokio::test]
async fn a_clone_the_engine_still_names_keeps_its_record() {
    let h = Harness::new("handover-clone-named").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let quarantines = h.receiver.inner.quarantines.clone();
    let gate = quarantines.inner.hash_gate.lock().await;
    let lost = marked_copy(&h).await;
    // The file is gone, but the engine still names the clone.
    std::fs::remove_file(&lost.path).expect("remove the clone");
    drop(gate);
    wait_for("the hash to fail", || async {
        quarantines.hash_failed(&lost.quarantine_id)
    })
    .await;
    assert_eq!(quarantined(&h).await, vec![lost.clone()]);
    assert!(
        h.store
            .kept_snapshot(&lost.snapshot_id)
            .await
            .expect("store")
            .is_some()
    );
    assert!(base.location.exists());
}

#[tokio::test]
async fn a_lost_clone_is_checked_through_one_open_volume_directory() {
    let h = Harness::new("handover-clone-one-dir").await;
    let path = returning_source(&h).await;
    let base = keep_snapshot(&h, KeepPurpose::WarmBase, 1).await;
    diverge(&path);
    let quarantines = h.receiver.inner.quarantines.clone();
    // The hash waits; each check below runs on its own.
    let _gate = quarantines.inner.hash_gate.lock().await;
    let lost = marked_copy(&h).await;
    let clone = h
        .store
        .kept_snapshot(&lost.snapshot_id)
        .await
        .expect("store")
        .expect("kept");
    // The engine answers `NotFound` for the clone from here on.
    h.engine
        .delete_volume_snapshot(&clone.snapshot)
        .await
        .expect("delete the clone");
    let check = || quarantines.inner.forget_lost_clone(&lost.quarantine_id);
    let between = |run: Box<dyn FnOnce() + Send>| {
        *quarantines.inner.between_checks.lock().expect("hook") = Some(run);
    };
    let kept_because = |reason: Result<(), String>, why: &str| {
        let reason = reason.expect_err("the record stays");
        assert!(reason.contains(why), "{reason}");
    };
    let kept_as_is = || {
        let (h, lost) = (&h, &lost);
        async move {
            assert_eq!(quarantined(h).await, vec![lost.clone()]);
            assert!(
                h.store
                    .kept_snapshot(&lost.snapshot_id)
                    .await
                    .expect("store")
                    .is_some()
            );
        }
    };

    // The clone's file is in the volume directory.
    std::fs::write(&lost.path, b"clone").expect("write the clone");
    kept_because(check().await, "the clone's file is in the volume directory");
    kept_as_is().await;
    std::fs::remove_file(&lost.path).expect("remove the clone");

    // The clone's file appears after the engine's lookups.
    let appears = lost.path.clone();
    between(Box::new(move || {
        std::fs::write(&appears, b"clone").expect("write the clone");
    }));
    kept_because(check().await, "the clone's file is in the volume directory");
    kept_as_is().await;
    std::fs::remove_file(&lost.path).expect("remove the clone");

    // The other kept snapshot vanishes after it answered the engine.
    let aside = base.location.with_extension("aside");
    let (from, to) = (base.location.clone(), aside.clone());
    between(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the base aside");
    }));
    kept_because(check().await, "cannot read the other kept snapshot");
    kept_as_is().await;
    std::fs::rename(&aside, &base.location).expect("put the base back");

    // The other kept snapshot is a symlink in the volume directory.
    std::fs::rename(&base.location, &aside).expect("move the base aside");
    std::os::unix::fs::symlink(&aside, &base.location).expect("link the base");
    kept_because(
        check().await,
        "the other kept snapshot is not a regular file",
    );
    kept_as_is().await;
    std::fs::remove_file(&base.location).expect("remove the link");
    std::fs::rename(&aside, &base.location).expect("put the base back");

    // Right after the volume directory is opened, it is moved aside and a
    // new one holding the clone takes its path. The reads go through the
    // open directory, where the clone is missing and the other kept snapshot
    // is a regular file beside it: the record is forgotten, and no file is
    // touched. Reads by path would see the clone and keep it.
    let dir = lost.path.parent().expect("a directory").to_path_buf();
    let moved = dir.with_extension("moved");
    let (from, to, clone_path) = (dir.clone(), moved.clone(), lost.path.clone());
    *quarantines.inner.after_open.lock().expect("hook") = Some(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the directory aside");
        std::fs::create_dir(&from).expect("a new directory");
        std::fs::write(&clone_path, b"clone").expect("write the clone");
    }));
    assert_eq!(check().await, Ok(()));
    std::fs::remove_dir_all(&dir).expect("remove the new directory");
    std::fs::rename(&moved, &dir).expect("put the directory back");
    assert!(quarantined(&h).await.is_empty());
    assert_eq!(
        h.store
            .kept_snapshot(&lost.snapshot_id)
            .await
            .expect("store"),
        None
    );
    assert!(base.location.exists());
    assert!(path.exists());
}

/// A copy kept from a returning source with no kept base, whose clone the
/// engine no longer has: the volume's live file is the only other file of
/// the volume. The caller holds the hash gate, so the copy's hash waits.
async fn lost_clone_beside_the_live_file(h: &Harness) -> VolumeQuarantineRecord {
    let path = returning_source(h).await;
    diverge(&path);
    let lost = marked_copy(h).await;
    assert_eq!(kept_purposes(h).await, vec!["quarantine"], "no base");
    let clone = h
        .store
        .kept_snapshot(&lost.snapshot_id)
        .await
        .expect("store")
        .expect("kept");
    // The engine answers `NotFound` for the clone from here on.
    h.engine
        .delete_volume_snapshot(&clone.snapshot)
        .await
        .expect("delete the clone");
    assert!(!lost.path.exists());
    lost
}

/// The copy's record and its kept snapshot are still there, as they were.
async fn assert_record_kept(h: &Harness, lost: &VolumeQuarantineRecord, what: &str) {
    assert_eq!(quarantined(h).await, vec![lost.clone()], "{what}");
    assert!(
        h.store
            .kept_snapshot(&lost.snapshot_id)
            .await
            .expect("store")
            .is_some(),
        "{what}"
    );
}

/// The copy's record and its kept snapshot are gone.
async fn assert_record_forgotten(h: &Harness, lost: &VolumeQuarantineRecord) {
    assert!(quarantined(h).await.is_empty());
    assert_eq!(
        h.store
            .kept_snapshot(&lost.snapshot_id)
            .await
            .expect("store"),
        None
    );
}

/// Issue #762: with no other kept snapshot of the volume, the live file the
/// engine finds shows the directory to be the real one, so an unhashed copy
/// whose clone is gone is forgotten, where its record used to stay until
/// another kept file appeared. A missing live file shows nothing.
#[tokio::test]
async fn with_no_kept_base_the_live_file_lets_a_lost_clone_s_record_go() {
    let h = Harness::new("handover-clone-gone-live").await;
    let quarantines = h.receiver.inner.quarantines.clone();
    let mut statuses = h.receiver.quarantine_statuses();
    let gate = quarantines.inner.hash_gate.lock().await;
    let lost = lost_clone_beside_the_live_file(&h).await;
    assert_eq!(statuses.borrow_and_update().len(), 1);

    // The live file is missing too, while the engine still names the
    // volume: the record stays, and the hash waits to be tried again.
    let moved = move_away(&h, &[&h.volume_path]);
    drop(gate);
    wait_for("the hash to fail", || async {
        quarantines.hash_failed(&lost.quarantine_id) && !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert_record_kept(&h, &lost, "a missing live file").await;
    let reason = quarantines
        .inner
        .forget_lost_clone(&lost.quarantine_id)
        .await
        .expect_err("the record stays");
    assert!(reason.contains("live file is missing"), "{reason}");
    assert_record_kept(&h, &lost, "a missing live file").await;

    // The live file is back: the record and its kept snapshot go, and no
    // file is touched.
    move_back(&moved);
    quarantines.hash(&lost);
    quarantines_where(&mut statuses, Vec::is_empty).await;
    assert_record_forgotten(&h, &lost).await;
    wait_for("the hash to end", || async {
        !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert!(!quarantines.hash_failed(&lost.quarantine_id));
    assert_live_kept(&h);

    // The mark stood for the copy: the next run clears it and keeps the
    // live file, and the one after takes a new copy and ends the hand-over.
    let _gate = quarantines.inner.hash_gate.lock().await;
    assert!(matches!(hand_over(&h).await, HandOver::Failed(_)));
    assert_eq!(
        h.store.volume_hand_over_mark(VOL).await.expect("store"),
        None
    );
    assert_live_kept(&h);
    let HandOver::Done(Some(id)) = hand_over(&h).await else {
        panic!("the hand-over ends");
    };
    assert_ne!(id, lost.quarantine_id);
    let records = quarantined(&h).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        read_at(&records[0].path, 7 * CELL + 512, 8192),
        pattern(8192, 4)
    );
    assert_handed_over(&h).await;
}

/// A VM the engine holds that attaches the live file keeps the engine from
/// checking the file: not now. The record stays, its hash waits as any
/// failed hash does and does not start again before that, and the record
/// goes once the VM is gone.
#[tokio::test]
async fn a_live_file_a_vm_attaches_keeps_a_lost_clone_s_record_for_now() {
    let h = Harness::new("handover-clone-gone-attached").await;
    // The receiver's own hash of the copy never runs here.
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let lost = lost_clone_beside_the_live_file(&h).await;
    let attaching = engine_vm(&h.engine, "vm_01JD0WARM0000000000000002", true).await;
    // The copies on the same engine and store, with a failed hash waiting
    // an hour before it is tried again.
    let (quarantines, _statuses) = Quarantines::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        h.locks.clone(),
        Duration::from_secs(3600),
    );

    quarantines.hash(&lost);
    wait_for("the hash to fail", || async {
        quarantines.hash_failed(&lost.quarantine_id) && !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert_record_kept(&h, &lost, "a VM attaches the live file").await;
    let reason = quarantines
        .inner
        .forget_lost_clone(&lost.quarantine_id)
        .await
        .expect_err("the record stays");
    assert!(
        reason.contains("would not check the volume's live file now"),
        "{reason}"
    );
    assert!(reason.contains("attached"), "{reason}");
    assert_live_kept(&h);

    // No loop: a pass before the wait ends starts no hash of the copy.
    {
        let _held = quarantines.inner.hash_gate.lock().await;
        quarantines.refresh().await.expect("refresh");
        assert!(!quarantines.hashing(&lost.quarantine_id), "the hash waits");
    }
    assert!(quarantines.hash_failed(&lost.quarantine_id));
    assert_record_kept(&h, &lost, "before the wait ends").await;

    // Once the VM is gone, the next try forgets the record.
    h.engine.delete(&attaching).await.expect("delete the VM");
    quarantines.hash(&lost);
    wait_for("the record to go", || async {
        quarantined(&h).await.is_empty() && !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert_record_forgotten(&h, &lost).await;
    assert!(!quarantines.hash_failed(&lost.quarantine_id));
    assert_live_kept(&h);
}

/// A record with a SHA-256 waits for `quarantine_deletes`, also with its
/// clone gone and the live file there for the engine to find: the primary
/// recorded the copy, and only an operator deletes it.
#[tokio::test]
async fn a_hashed_copy_whose_clone_is_gone_waits_for_its_delete_beside_the_live_file() {
    let h = Harness::new("handover-clone-gone-hashed").await;
    let quarantines = h.receiver.inner.quarantines.clone();
    let gate = quarantines.inner.hash_gate.lock().await;
    let lost = lost_clone_beside_the_live_file(&h).await;
    let sha256 = "ab".repeat(32);
    assert!(
        h.store
            .set_quarantine_sha256(&lost.quarantine_id, &sha256)
            .await
            .expect("store")
    );
    let hashed = VolumeQuarantineRecord {
        sha256: Some(sha256),
        ..lost.clone()
    };

    // The hash the hand-over started reads the record again and hashes
    // nothing, and a pass starts no hash of it.
    drop(gate);
    wait_for("the hash to end", || async {
        !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    quarantines.refresh().await.expect("refresh");
    assert!(!quarantines.hashing(&lost.quarantine_id));
    assert!(!quarantines.hash_failed(&lost.quarantine_id));
    assert_record_kept(&h, &hashed, "a hashed copy").await;

    // Asked directly, the forget path keeps it too, and so does the store.
    let reason = quarantines
        .inner
        .forget_lost_clone(&lost.quarantine_id)
        .await
        .expect_err("the record stays");
    assert!(reason.contains("hashed"), "{reason}");
    assert!(
        !h.store
            .forget_unhashed_volume_quarantine(&lost.quarantine_id, &lost.snapshot_id)
            .await
            .expect("store")
    );
    assert_record_kept(&h, &hashed, "a hashed copy").await;
    assert_live_kept(&h);

    // An operator's delete removes it.
    quarantines
        .delete(std::slice::from_ref(&lost.quarantine_id))
        .await;
    assert_record_forgotten(&h, &lost).await;
    assert_live_kept(&h);
}

/// The live file the engine found is read through the same open handle of
/// the clone's directory as the clone's absence, as another kept snapshot
/// is.
#[tokio::test]
async fn a_lost_clone_s_live_file_is_read_through_one_open_volume_directory() {
    let h = Harness::new("handover-clone-live-one-dir").await;
    let quarantines = h.receiver.inner.quarantines.clone();
    // The hash waits; each check below runs on its own.
    let _gate = quarantines.inner.hash_gate.lock().await;
    let lost = lost_clone_beside_the_live_file(&h).await;
    let live = h.volume_path.clone();
    let check = || quarantines.inner.forget_lost_clone(&lost.quarantine_id);
    let between = |run: Box<dyn FnOnce() + Send>| {
        *quarantines.inner.between_checks.lock().expect("hook") = Some(run);
    };
    let kept_because = |reason: Result<(), String>, why: &str| {
        let reason = reason.expect_err("the record stays");
        assert!(reason.contains(why), "{reason}");
    };

    // The live file vanishes after the engine found it.
    let aside = live.with_extension("aside");
    let (from, to) = (live.clone(), aside.clone());
    between(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the live file aside");
    }));
    kept_because(check().await, "cannot read the volume's live file");
    assert_record_kept(&h, &lost, "the live file vanished").await;
    std::fs::rename(&aside, &live).expect("put the live file back");

    // The live file is a symlink by the time the directory is read.
    let (from, to) = (live.clone(), aside.clone());
    between(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the live file aside");
        std::os::unix::fs::symlink(&to, &from).expect("link the live file");
    }));
    kept_because(
        check().await,
        "the volume's live file is not a regular file",
    );
    assert_record_kept(&h, &lost, "a symlink").await;
    std::fs::remove_file(&live).expect("remove the link");
    std::fs::rename(&aside, &live).expect("put the live file back");

    // The clone's file appears after the engine's lookups.
    let appears = lost.path.clone();
    between(Box::new(move || {
        std::fs::write(&appears, b"clone").expect("write the clone");
    }));
    kept_because(check().await, "the clone's file is in the volume directory");
    assert_record_kept(&h, &lost, "the clone is back").await;
    std::fs::remove_file(&lost.path).expect("remove the clone");

    // The volume directory unmounts after the engine found the live file:
    // an empty directory takes its path, and it shows no live file.
    let dir = live.parent().expect("a directory").to_path_buf();
    let moved = dir.with_extension("moved");
    let (from, to) = (dir.clone(), moved.clone());
    between(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the directory aside");
        std::fs::create_dir(&from).expect("an empty directory");
    }));
    kept_because(check().await, "cannot read the volume's live file");
    std::fs::remove_dir(&dir).expect("remove the empty directory");
    std::fs::rename(&moved, &dir).expect("put the directory back");
    assert_record_kept(&h, &lost, "an unmounted directory").await;

    // Right after the volume directory is opened, it is moved aside and a
    // new one holding the clone and no live file takes its path. The reads
    // go through the open directory, where the clone is missing and the live
    // file is a regular file: the record is forgotten, and no file is
    // touched. Reads by path would see the clone and keep it.
    let (from, to, clone_path) = (dir.clone(), moved.clone(), lost.path.clone());
    *quarantines.inner.after_open.lock().expect("hook") = Some(Box::new(move || {
        std::fs::rename(&from, &to).expect("move the directory aside");
        std::fs::create_dir(&from).expect("a new directory");
        std::fs::write(&clone_path, b"clone").expect("write the clone");
    }));
    assert_eq!(check().await, Ok(()));
    std::fs::remove_dir_all(&dir).expect("remove the new directory");
    std::fs::rename(&moved, &dir).expect("put the directory back");
    assert_record_forgotten(&h, &lost).await;
    assert_live_kept(&h);
}

/// The forget path asks the engine about the volume only under the volume's
/// lock, and only tries it: while a stream, a hand-over or the reconciler
/// holds it, the record stays without the engine being asked, and the hash
/// waits as any failed hash does instead of waiting for the lock. Once the
/// lock is free, the next try forgets the record.
#[tokio::test]
async fn a_lost_clone_s_record_stays_while_another_holds_the_volume_s_lock() {
    let h = Harness::new("handover-clone-gone-locked").await;
    // The receiver's own hash of the copy never runs here.
    let _gate = h.receiver.inner.quarantines.inner.hash_gate.lock().await;
    let lost = lost_clone_beside_the_live_file(&h).await;
    // The copies on the same engine, store and locks, with a failed hash
    // waiting an hour before it is tried again.
    let (quarantines, _statuses) = Quarantines::new(
        Arc::clone(&h.engine),
        h.store.clone(),
        h.locks.clone(),
        Duration::from_secs(3600),
    );
    // Answered by the first engine lookup the forget path makes.
    h.engine.fail_next(
        FakeOp::VolumeChangedRanges,
        EngineError::Io("the first lookup".to_owned()),
    );

    let held = h.locks.try_lock(VOL).expect("the volume's lock");
    quarantines.hash(&lost);
    wait_for("the hash to fail", || async {
        quarantines.hash_failed(&lost.quarantine_id) && !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert_record_kept(&h, &lost, "the volume's lock is held").await;
    let reason = quarantines
        .inner
        .forget_lost_clone(&lost.quarantine_id)
        .await
        .expect_err("the record stays");
    assert!(reason.contains("holds its lock"), "{reason}");
    assert_live_kept(&h);

    // No loop: a pass before the wait ends starts no hash of the copy.
    {
        let _held_gate = quarantines.inner.hash_gate.lock().await;
        quarantines.refresh().await.expect("refresh");
        assert!(!quarantines.hashing(&lost.quarantine_id), "the hash waits");
    }
    assert!(quarantines.hash_failed(&lost.quarantine_id));
    assert_record_kept(&h, &lost, "before the wait ends").await;

    // The lock is free: the engine is asked from here on. Its first answer
    // is the failure queued before the lock was taken, so no lookup ran
    // while the lock was held.
    drop(held);
    let reason = quarantines
        .inner
        .forget_lost_clone(&lost.quarantine_id)
        .await
        .expect_err("the queued failure");
    assert!(reason.contains("the first lookup"), "{reason}");
    assert_record_kept(&h, &lost, "the queued failure").await;

    // The next try forgets the record, and leaves the lock free.
    quarantines.hash(&lost);
    wait_for("the record to go", || async {
        quarantined(&h).await.is_empty() && !quarantines.hashing(&lost.quarantine_id)
    })
    .await;
    assert_record_forgotten(&h, &lost).await;
    assert!(!quarantines.hash_failed(&lost.quarantine_id));
    assert!(h.locks.try_lock(VOL).is_some(), "the lock is released");
    assert_live_kept(&h);
}
