// SPDX-License-Identifier: AGPL-3.0-only
//! The warm sender against the real warm receiver over loopback, each on its
//! own fake engine with real files. After every cycle the test checks what
//! the target holds, what the source keeps, and that a copy the target
//! confirmed equals the source's snapshot.

use std::net::SocketAddr;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quoro_backup::cells::{CELL, CellHashes};
use quoro_engine::{
    ByteRange, Engine as _, EngineError, FakeEngine, FakeOp, FakeRanges, SnapshotMethod, VolumeId,
};
use quoro_proto::wire::{
    DesiredState, Spec, SpecMesh, SpecVolumeFile, SpecVolumeState, SpecWarm, WarmRole, WarmStatus,
};
use quoro_proto::{LeaseGrant, LeaseId, LeaseMode, ResourceType};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::{CycleOutcome, SenderOptions, WarmSender, produce};
use crate::kept::{self, KeepPurpose};
use crate::lease::{LeaseTable, LeaseView};
use crate::reconcile::Snapshot;
use crate::store::{KeptSnapshotRecord, NodeStore};
use crate::warm::frame::{Decoder, Frame, RefuseReason, Sequence, StreamHeader};
use crate::warm::frame::{Manifest, encode};
use crate::warm::receiver::{ReceiverOptions, WarmReceiver};
use crate::warm::{StatusBoard, VolumeLocks, hash_copy};

const VOL: &str = "vol_01JD0WARM0000000000000002";
const APP: &str = "app_01JD0WARM0000000000000002";
const LEASE: &str = "lse_01JD0WARM0000000000000002";
const GIB: u64 = 1 << 30;
const SIZE: u64 = GIB;
const LAST_CELL: u64 = SIZE / CELL - 1;
const TOKEN: u64 = 5;

/// The target's timings here: `quick()`, but with the production frame
/// timeout. No test here stalls a stream, so the timeout checks nothing in
/// these tests; at 500 ms, a loaded machine's file IO on the source reaches
/// it between two frames and the target drops a sound stream as
/// `bad_frame`. The receiver's own tests stall streams and keep 500 ms.
fn target_options() -> ReceiverOptions {
    ReceiverOptions {
        frame_timeout: ReceiverOptions::default().frame_timeout,
        ..ReceiverOptions::quick()
    }
}

/// The source's timings here: `quick()` cycles, with the production stall
/// timeout, for the same reason. No test here makes the target stop
/// reading, and the target's store writes can pause its reads for seconds
/// on a loaded disk.
fn source_options() -> SenderOptions {
    SenderOptions {
        stall_timeout: SenderOptions::default().stall_timeout,
        ..SenderOptions::quick()
    }
}

/// A scratch directory, removed on drop: under `QUORO_TEST_REFLINK_DIR`
/// when it is set, so holes and clones run on the filesystem warm copies
/// live on; under the workspace `target/` otherwise.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let root = std::env::var_os("QUORO_TEST_REFLINK_DIR").map_or_else(
            || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            PathBuf::from,
        );
        let dir = root
            .join("warm-sender-tests")
            .join(format!("{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("volumes")).expect("scratch dir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn snapshot(rev: u64, volumes: Vec<SpecVolumeFile>) -> Snapshot {
    Snapshot {
        rev,
        state: DesiredState {
            rev,
            issued_at: "2026-11-06T10:00:00Z".to_owned(),
            term: 1,
            spec: Spec {
                mesh: Some(SpecMesh {
                    address: "127.0.0.1/16".to_owned(),
                    listen_port: 51820,
                    peers: Vec::new(),
                    allow: Vec::new(),
                }),
                volumes,
                ..Spec::default()
            },
            signature: String::new(),
        },
    }
}

fn warm_entry(role: WarmRole, port: u16, token: u64) -> SpecVolumeFile {
    SpecVolumeFile {
        volume_id: VOL.to_owned(),
        app_id: APP.to_owned(),
        size_gb: 1,
        state: SpecVolumeState::Present,
        restore: None,
        warm: Some(SpecWarm {
            role,
            peer_node_id: "nod_01JD0WARM0000000000000009".to_owned(),
            peer_address: "127.0.0.1".to_owned(),
            port,
            interval_s: 30,
            lease_id: LEASE.to_owned(),
            fencing_token: token,
        }),
    }
}

/// A lease table where this node holds the app's lease with `token`.
fn lease_view(token: u64) -> LeaseView {
    let mut table = LeaseTable::new();
    table
        .on_grant(
            &LeaseGrant {
                lease_id: LeaseId(LEASE.to_owned()),
                resource_type: ResourceType::App,
                resource_id: APP.to_owned(),
                mode: LeaseMode::Local,
                fencing_token: token,
                duration_ms: None,
                granted_at: "2026-11-06T10:00:00Z".to_owned(),
            },
            None,
            tokio::time::Instant::now(),
        )
        .expect("a valid local grant");
    LeaseView::new(table, false)
}

fn set_entry(desired: &watch::Sender<Option<Snapshot>>, change: impl FnOnce(&mut SpecVolumeFile)) {
    desired.send_modify(|current| {
        let snapshot = current.as_mut().expect("a snapshot");
        snapshot.rev += 1;
        snapshot.state.rev = snapshot.rev;
        change(&mut snapshot.state.spec.volumes[0]);
    });
}

/// The root of a file's cell hashes.
fn root_of(path: &Path) -> String {
    hash_copy(&std::fs::File::open(path).expect("open"), SIZE)
        .expect("hash")
        .root_hex()
}

/// The target: a warm receiver serving on a loopback listener.
struct Target {
    _scratch: Scratch,
    engine: Arc<FakeEngine>,
    store: NodeStore,
    desired: watch::Sender<Option<Snapshot>>,
    locks: VolumeLocks,
    addr: SocketAddr,
    server: JoinHandle<()>,
    copy: PathBuf,
}

impl Drop for Target {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Target {
    async fn new(tag: &str) -> Self {
        let scratch = Scratch::new(&format!("{tag}-target"));
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(scratch.0.join("volumes"));
        engine.set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let (desired, desired_rx) = watch::channel(Some(snapshot(
            1,
            vec![warm_entry(WarmRole::Target, 0, TOKEN)],
        )));
        let locks = VolumeLocks::new();
        let (board, _statuses) = StatusBoard::new();
        let receiver = WarmReceiver::new(
            Arc::clone(&engine),
            store.clone(),
            desired_rx,
            locks.clone(),
            board,
            target_options(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move { receiver.serve(listener).await });
        let copy = scratch.0.join("volumes").join(VOL);
        Self {
            _scratch: scratch,
            engine,
            store,
            desired,
            locks,
            addr,
            server,
            copy,
        }
    }

    async fn complete_seq(&self) -> Option<u64> {
        self.store
            .warm_target(VOL)
            .await
            .expect("store")
            .and_then(|row| row.complete_seq)
    }
}

/// What a proxy between the two saw of each request.
#[derive(Default)]
struct Seen {
    /// The stream header of each request, in order.
    headers: Vec<StreamHeader>,
    /// The bytes of each request body, chunk framing included.
    bytes: Vec<u64>,
}

/// A loopback proxy in front of the target that records the stream headers
/// and can drop the target's answers.
struct Proxy {
    addr: SocketAddr,
    seen: Arc<Mutex<Seen>>,
    drop_answers: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    async fn new(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let seen = Arc::new(Mutex::new(Seen::default()));
        let drop_answers = Arc::new(AtomicBool::new(false));
        let (seen_task, drop_task) = (Arc::clone(&seen), Arc::clone(&drop_answers));
        let task = tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                let seen = Arc::clone(&seen_task);
                let drop_answers = Arc::clone(&drop_task);
                tokio::spawn(async move { relay(inbound, target, seen, drop_answers).await });
            }
        });
        Self {
            addr,
            seen,
            drop_answers,
            task,
        }
    }

    fn headers(&self) -> Vec<StreamHeader> {
        self.seen.lock().expect("seen").headers.clone()
    }

    fn bytes(&self) -> Vec<u64> {
        self.seen.lock().expect("seen").bytes.clone()
    }
}

/// Relays one connection. The request goes through as it comes; the answer
/// is read whole and then sent on, or dropped with the connection.
async fn relay(
    inbound: TcpStream,
    target: SocketAddr,
    seen: Arc<Mutex<Seen>>,
    drop_answers: Arc<AtomicBool>,
) {
    let Ok(outbound) = TcpStream::connect(target).await else {
        return;
    };
    let (mut in_read, mut in_write) = inbound.into_split();
    let (mut out_read, mut out_write) = outbound.into_split();
    let index = {
        let mut seen = seen.lock().expect("seen");
        seen.bytes.push(0);
        seen.bytes.len() - 1
    };
    let up_seen = Arc::clone(&seen);
    let up = tokio::spawn(async move {
        let mut head: Vec<u8> = Vec::new();
        let mut parsed = false;
        let mut buf = vec![0_u8; 64 << 10];
        loop {
            let n = match in_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            up_seen.lock().expect("seen").bytes[index] += n as u64;
            if !parsed && head.len() < 1 << 20 {
                head.extend_from_slice(&buf[..n]);
                if let Some(header) = first_header(&head) {
                    up_seen.lock().expect("seen").headers.push(header);
                    parsed = true;
                }
            }
            if out_write.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        let _ = out_write.shutdown().await;
    });
    let mut answer = Vec::new();
    let _ = out_read.read_to_end(&mut answer).await;
    if !drop_answers.load(Ordering::SeqCst) {
        let _ = in_write.write_all(&answer).await;
        let _ = in_write.shutdown().await;
    }
    up.abort();
}

/// The stream header in the first chunk of a request.
fn first_header(raw: &[u8]) -> Option<StreamHeader> {
    let head_end = raw.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
    let rest = &raw[head_end..];
    let line_end = rest.windows(2).position(|window| window == b"\r\n")?;
    let length = usize::from_str_radix(std::str::from_utf8(&rest[..line_end]).ok()?, 16).ok()?;
    let chunk = rest.get(line_end + 2..line_end + 2 + length)?;
    let mut decoder = Decoder::new();
    decoder.push(chunk);
    match decoder.next_frame() {
        Ok(Some(Frame::Header(header))) => Some(header),
        _ => None,
    }
}

/// The source: one warm volume with real data, and its sender.
struct Source {
    scratch: Scratch,
    engine: Arc<FakeEngine>,
    store: NodeStore,
    desired: watch::Sender<Option<Snapshot>>,
    leases: LeaseView,
    locks: VolumeLocks,
    board: StatusBoard,
    statuses: watch::Receiver<Vec<WarmStatus>>,
    sender: WarmSender<FakeEngine>,
    volume: PathBuf,
}

impl Source {
    async fn new(tag: &str, port: u16, leases: LeaseView) -> Self {
        let scratch = Scratch::new(&format!("{tag}-source"));
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(scratch.0.join("volumes"));
        engine.set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
        engine.set_changed_ranges_mode(FakeRanges::Content);
        engine
            .ensure_volume(&VolumeId(VOL.to_owned()), 1)
            .await
            .expect("the source's volume");
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let (desired, desired_rx) = watch::channel(Some(snapshot(
            1,
            vec![warm_entry(WarmRole::Source, port, TOKEN)],
        )));
        let locks = VolumeLocks::new();
        let (board, statuses) = StatusBoard::new();
        let sender = WarmSender::new(
            Arc::clone(&engine),
            store.clone(),
            desired_rx,
            leases.clone(),
            locks.clone(),
            board.clone(),
            source_options(),
        );
        let volume = scratch.0.join("volumes").join(VOL);
        Self {
            scratch,
            engine,
            store,
            desired,
            leases,
            locks,
            board,
            statuses,
            sender,
            volume,
        }
    }

    /// A new sender on the same engine and store, as after a restart.
    fn restart(&mut self) {
        self.sender = WarmSender::new(
            Arc::clone(&self.engine),
            self.store.clone(),
            self.desired.subscribe(),
            self.leases.clone(),
            self.locks.clone(),
            self.board.clone(),
            source_options(),
        );
    }

    fn write(&self, offset: u64, bytes: &[u8]) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.volume)
            .expect("the volume");
        file.write_all_at(bytes, offset).expect("write");
    }

    async fn cycle(&self) -> CycleOutcome {
        self.sender.cycle_once(VOL).await
    }

    /// The warm snapshots the store keeps, as `(purpose, seq)` by seq.
    async fn kept(&self) -> Vec<(String, Option<u64>)> {
        let mut kept: Vec<(String, Option<u64>)> = self
            .records()
            .await
            .into_iter()
            .map(|record| (record.purpose, record.seq))
            .collect();
        kept.sort_by_key(|(_, seq)| *seq);
        kept
    }

    async fn records(&self) -> Vec<KeptSnapshotRecord> {
        self.store.kept_snapshots().await.expect("kept snapshots")
    }

    /// The source's base: its record, which must carry its cell hashes,
    /// and the root of its file.
    async fn base(&self) -> KeptSnapshotRecord {
        let records = self.records().await;
        let bases: Vec<&KeptSnapshotRecord> = records
            .iter()
            .filter(|record| record.purpose == KeepPurpose::WarmBase.as_str())
            .collect();
        assert_eq!(bases.len(), 1, "one base: {records:?}");
        bases[0].clone()
    }

    fn status(&self) -> WarmStatus {
        let statuses = self.statuses.borrow();
        assert_eq!(statuses.len(), 1, "one status entry: {statuses:?}");
        statuses[0].clone()
    }

    fn snapshot_count(&self) -> usize {
        self.engine.volume_snapshots().len()
    }
}

fn base_entry(purpose: KeepPurpose, seq: u64) -> (String, Option<u64>) {
    (purpose.as_str().to_owned(), Some(seq))
}

/// The target's copy is the source's base, byte for byte by cell hash, and
/// the base's recorded hashes say so too.
async fn assert_shipped(source: &Source, target: &Target, seq: u64) {
    assert_eq!(target.complete_seq().await, Some(seq), "the target's copy");
    let base = source.base().await;
    assert_eq!(base.seq, Some(seq), "the source's base");
    let target_root = root_of(&target.copy);
    assert_eq!(
        root_of(&base.snapshot.location),
        target_root,
        "the copy is the base"
    );
    let recorded =
        CellHashes::from_bytes(base.cells.as_deref().expect("base cells"), SIZE).expect("cells");
    assert_eq!(
        recorded.root_hex(),
        target_root,
        "the base's recorded cells"
    );
    assert!(
        target.engine.staged_volumes().is_empty(),
        "no staging file left"
    );
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).expect("small").wrapping_add(seed))
        .collect()
}

/// Data at the start, across a cell boundary, a whole cell and the end.
fn write_first(source: &Source) {
    source.write(0, &[7_u8; 4096]);
    source.write(3 * CELL - 100, &pattern(300_000, 1));
    source.write(5 * CELL, &pattern(CELL as usize, 2));
    source.write(LAST_CELL * CELL + CELL - 10, &[1_u8; 10]);
}

/// A partial change, zeros over part of a cell and over a whole cell, and a
/// new cell.
fn write_second(source: &Source) {
    source.write(3 * CELL + 50, &pattern(1000, 3));
    source.write(100, &[0_u8; 1000]);
    source.write(5 * CELL, &vec![0_u8; CELL as usize]);
    source.write(8 * CELL, &pattern(CELL as usize, 4));
}

#[tokio::test]
async fn cycles_ship_a_full_copy_then_only_what_changed() {
    let target = Target::new("full-then-incremental").await;
    let proxy = Proxy::new(target.addr).await;
    let source = Source::new(
        "full-then-incremental",
        proxy.addr.port(),
        lease_view(TOKEN),
    )
    .await;

    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));
    assert_shipped(&source, &target, 1).await;
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1, "only the base is kept");
    let status = source.status();
    assert_eq!(status.role, WarmRole::Source);
    assert_eq!(status.snapshot_seq, Some(1));
    assert!(status.snapshot_taken_at.is_some());
    assert!(status.completed_at.is_some());
    assert!(status.extent_count.is_some());
    assert!(status.error.is_none());

    write_second(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 2)]
    );
    assert_eq!(source.snapshot_count(), 1);

    // Nothing changed: an empty incremental stream still moves the copy on.
    assert_eq!(source.cycle().await, CycleOutcome::Complete(3));
    assert_shipped(&source, &target, 3).await;

    let headers = proxy.headers();
    let bases: Vec<Option<u64>> = headers.iter().map(|header| header.base_seq).collect();
    assert_eq!(
        bases,
        vec![None, Some(1), Some(2)],
        "a full copy, then increments"
    );
    for header in &headers {
        assert_eq!(header.fencing_token, TOKEN);
        assert_eq!(header.lease_id, LEASE);
        assert_eq!(header.size_bytes, SIZE);
    }
    let bytes = proxy.bytes();
    assert!(
        bytes[1] < 4 * CELL,
        "the increment ships what changed: {bytes:?}"
    );
    assert!(
        bytes[2] < 64 << 10,
        "an empty increment is small: {bytes:?}"
    );
}

#[tokio::test]
async fn no_snapshot_is_taken_without_the_lease_or_on_a_pausing_node() {
    let target = Target::new("no-lease").await;
    let port = target.addr.port();
    for leases in [LeaseView::default(), lease_view(TOKEN - 1)] {
        let source = Source::new("no-lease", port, leases).await;
        write_first(&source);
        let CycleOutcome::Failed(error) = source.cycle().await else {
            panic!("a failed cycle");
        };
        assert!(error.contains("lease"), "{error}");
        assert_eq!(source.snapshot_count(), 0, "no snapshot taken");
        assert!(source.kept().await.is_empty());
        assert!(
            source
                .status()
                .error
                .is_some_and(|error| error.contains("lease"))
        );
    }
    assert_eq!(target.complete_seq().await, None);

    let source = Source::new("pausing", port, lease_view(TOKEN)).await;
    source
        .engine
        .set_snapshot_method(SnapshotMethod::PauseCopy, Duration::ZERO);
    let CycleOutcome::Failed(error) = source.cycle().await else {
        panic!("a failed cycle");
    };
    assert!(error.contains("reflink"), "{error}");
    assert!(source.kept().await.is_empty());
    // No snapshot was ever taken: the engine's next one is its first.
    source
        .engine
        .set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
    let first = source
        .engine
        .snapshot_volume(&VolumeId(VOL.to_owned()))
        .await
        .expect("snapshot");
    assert_eq!(first.snapshot_id, "snap-1", "the cycle took no snapshot");
}

#[tokio::test]
async fn a_refusal_that_names_the_base_keeps_it() {
    let target = Target::new("stale").await;
    let source = Source::new("stale", target.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    // The target moved on to a higher token: this source's stream is stale.
    set_entry(&target.desired, |entry| {
        entry.warm.as_mut().expect("warm").fencing_token = TOKEN + 1;
    });
    write_second(&source);
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::StaleToken, Some(1))
    );
    assert_eq!(
        target.complete_seq().await,
        Some(1),
        "the copy did not change"
    );
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(
        source.snapshot_count(),
        1,
        "the refused snapshot is deleted"
    );
    let status = source.status();
    assert_eq!(status.snapshot_seq, Some(1));
    assert!(
        status
            .error
            .is_some_and(|error| error.contains("stale_token"))
    );

    // Another lease: refused at the header, while the source still writes.
    source.write(16 * CELL, &pattern(48 * CELL as usize, 9));
    set_entry(&target.desired, |entry| {
        let warm = entry.warm.as_mut().expect("warm");
        warm.fencing_token = TOKEN;
        warm.lease_id = "lse_01JD0WARM00000000000000XX".to_owned();
    });
    let started = std::time::Instant::now();
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::WrongLease, Some(1))
    );
    assert!(started.elapsed() < Duration::from_secs(15));
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1);

    // Back to the right lease: the next cycle ships from the base.
    set_entry(&target.desired, |entry| {
        entry.warm.as_mut().expect("warm").lease_id = LEASE.to_owned();
    });
    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
    assert!(
        source.status().error.is_none(),
        "a complete cycle clears the error"
    );
}

#[tokio::test]
async fn a_lost_answer_deletes_the_snapshot_and_the_next_answer_says_what_the_target_holds() {
    let target = Target::new("lost-answer").await;
    let proxy = Proxy::new(target.addr).await;
    let mut source = Source::new("lost-answer", proxy.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    // The target swaps seq 2 in, but its answer never arrives: a failed
    // cycle keeps the base and deletes `next` (section 6.13).
    write_second(&source);
    proxy.drop_answers.store(true, Ordering::SeqCst);
    let CycleOutcome::Failed(error) = source.cycle().await else {
        panic!("a failed cycle");
    };
    assert!(error.contains("without an answer"), "{error}");
    assert_eq!(target.complete_seq().await, Some(2));
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1);
    proxy.drop_answers.store(false, Ordering::SeqCst);

    // The target is busy with another stream: only `next` goes.
    let lock = target.locks.try_lock(VOL).expect("the target's lock");
    source.write(9 * CELL, &pattern(1000, 5));
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::Busy, Some(2))
    );
    drop(lock);
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1);

    // After a restart the next answer names seq 2, which this node no
    // longer keeps: everything goes, and the cycle after ships a full copy.
    source.restart();
    source.sender.restore().await;
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::BaseMismatch, Some(2))
    );
    assert!(source.kept().await.is_empty());
    assert_eq!(source.snapshot_count(), 0);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(3));
    assert_shipped(&source, &target, 3).await;

    let bases: Vec<(u64, Option<u64>)> = proxy
        .headers()
        .iter()
        .map(|header| (header.snapshot_seq, header.base_seq))
        .collect();
    assert_eq!(
        bases,
        vec![
            (1, None),
            (2, Some(1)),
            // A number whose snapshot is gone is used again.
            (2, Some(1)),
            (2, Some(1)),
            (3, None),
        ]
    );
}

#[tokio::test]
async fn a_snapshot_the_target_confirmed_survives_a_restart_without_its_answer() {
    let target = Target::new("confirmed-restart").await;
    let proxy = Proxy::new(target.addr).await;
    let mut source = Source::new("confirmed-restart", proxy.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    // The target confirms seq 2, but the node cannot record its move to the
    // base, and then restarts: what it learned in memory is gone.
    let db = sqlx_connect(&source.scratch.0.join("node.db")).await;
    sqlx::query(
        "CREATE TRIGGER no_warm_base BEFORE UPDATE ON kept_snapshots \
         WHEN NEW.purpose = 'warm_base' BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");
    write_second(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    sqlx::query("DROP TRIGGER no_warm_base")
        .execute(&db)
        .await
        .expect("drop trigger");
    assert_eq!(
        source.kept().await,
        vec![
            base_entry(KeepPurpose::WarmBase, 1),
            base_entry(KeepPurpose::WarmShipping, 2)
        ]
    );
    source.restart();
    source.sender.restore().await;
    assert_eq!(source.kept().await.len(), 2, "both survive the restart");

    // The next answer names seq 2, which becomes the base, and the cycle
    // after ships from it.
    source.write(9 * CELL, &pattern(1000, 5));
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::BaseMismatch, Some(2))
    );
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 2)]
    );
    assert_eq!(source.cycle().await, CycleOutcome::Complete(3));
    assert_shipped(&source, &target, 3).await;
    let bases: Vec<(u64, Option<u64>)> = proxy
        .headers()
        .iter()
        .map(|header| (header.snapshot_seq, header.base_seq))
        .collect();
    assert_eq!(
        bases,
        vec![(1, None), (2, Some(1)), (3, Some(1)), (3, Some(2))]
    );
}

#[tokio::test]
async fn a_failure_on_the_target_deletes_the_snapshot_and_the_next_cycle_ships_from_the_base() {
    let target = Target::new("target-fails").await;
    let source = Source::new("target-fails", target.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    write_second(&source);
    target.engine.fail_next(
        FakeOp::CommitStagedVolume,
        EngineError::Conflict("injected".to_owned()),
    );
    let CycleOutcome::Failed(error) = source.cycle().await else {
        panic!("a failed cycle");
    };
    assert!(error.contains("500"), "{error}");
    // The target answers before it resolves its copy, which hashes the whole
    // volume, and holds the volume's lock until then; a cycle that came
    // before would be refused `busy`.
    drop(
        tokio::time::timeout(Duration::from_secs(300), target.locks.lock(VOL))
            .await
            .expect("the target releases the volume's lock"),
    );
    let row = target
        .store
        .warm_target(VOL)
        .await
        .expect("store")
        .expect("the row");
    assert!(row.pending.is_none(), "the target resolved its copy");
    assert_eq!(row.complete_seq, Some(1));
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1);

    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 2)]
    );
    assert_eq!(source.snapshot_count(), 1);
}

#[tokio::test]
async fn a_lease_lost_before_the_manifest_never_completes_the_stream() {
    let target = Target::new("lease-lost").await;
    let source = Source::new("lease-lost", target.addr.port(), lease_view(TOKEN)).await;
    let (tx, rx) = mpsc::channel(4);
    let header = StreamHeader {
        app_id: APP.to_owned(),
        volume_id: VOL.to_owned(),
        lease_id: LEASE.to_owned(),
        fencing_token: TOKEN,
        snapshot_seq: 1,
        base_seq: None,
        taken_at: "2026-11-06T10:00:00Z".to_owned(),
        size_bytes: SIZE,
    };
    let empty = CellHashes::zeroed(SIZE).expect("cells");
    for (bytes, last) in [
        (encode(&Frame::Header(header)), false),
        (
            encode(&Frame::Manifest(Manifest {
                block_count: 0,
                root_hash: empty.root_hex(),
            })),
            true,
        ),
    ] {
        tx.send(super::Chunk { bytes, last }).await.expect("queue");
    }
    drop(tx);
    let lost = || false;
    let shipped = source
        .sender
        .inner
        .post("127.0.0.1".parse().expect("ip"), target.addr, rx, &lost)
        .await;
    let Some(super::Shipped::Unanswered(error)) = shipped else {
        panic!("no answer");
    };
    assert!(error.contains("lease"), "{error}");
    wait_for("the target to drop the stream", || async {
        target.locks.try_lock(VOL).is_some()
    })
    .await;
    assert_eq!(target.complete_seq().await, None, "no copy");
    assert!(target.engine.staged_volumes().is_empty());
    assert!(!target.copy.exists());
}

#[tokio::test]
async fn a_wrong_root_starts_over_with_a_full_copy() {
    let target = Target::new("wrong-root").await;
    let proxy = Proxy::new(target.addr).await;
    let source = Source::new("wrong-root", proxy.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    // The base's recorded cell hashes no longer match the target's copy.
    let mut base = source.base().await;
    base.cells = Some(CellHashes::zeroed(SIZE).expect("cells").to_bytes());
    source.store.save_kept_snapshot(&base).await.expect("save");
    write_second(&source);
    assert_eq!(
        source.cycle().await,
        CycleOutcome::Refused(RefuseReason::RootMismatch, Some(1))
    );
    assert!(source.kept().await.is_empty(), "nothing is trusted");
    assert_eq!(source.snapshot_count(), 0);
    assert_eq!(target.complete_seq().await, Some(1));

    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
    let bases: Vec<Option<u64>> = proxy
        .headers()
        .iter()
        .map(|header| header.base_seq)
        .collect();
    assert_eq!(bases, vec![None, Some(1), None]);
}

#[tokio::test]
async fn a_bad_frame_refusal_keeps_the_base_the_target_still_holds() {
    let target = Target::new("bad-frame").await;
    let source = Source::new("bad-frame", target.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));

    // A stream the target refused as malformed did not change its copy.
    write_second(&source);
    let next = source
        .engine
        .snapshot_volume(&VolumeId(VOL.to_owned()))
        .await
        .expect("snapshot");
    kept::keep(
        &source.store,
        APP,
        &next,
        KeepPurpose::WarmShipping,
        Some(2),
        None,
    )
    .await
    .expect("keep");
    let records = source.records().await;
    let next_record = records
        .iter()
        .find(|record| record.seq == Some(2))
        .cloned()
        .expect("next");
    let kept_set = super::KeptSet::from_records(
        records
            .into_iter()
            .filter(|record| record.seq != Some(2))
            .collect(),
    );
    let header = StreamHeader {
        app_id: APP.to_owned(),
        volume_id: VOL.to_owned(),
        lease_id: LEASE.to_owned(),
        fencing_token: TOKEN,
        snapshot_seq: 2,
        base_seq: Some(1),
        taken_at: "2026-11-06T10:00:00Z".to_owned(),
        size_bytes: SIZE,
    };
    let answer = super::Answer {
        status: super::AnswerStatus::Refused,
        snapshot_seq: Some(1),
        reason: Some(RefuseReason::BadFrame),
    };
    let outcome = source
        .sender
        .inner
        .settle(
            VOL,
            &header,
            &next_record,
            &kept_set,
            super::Shipped::Answer(answer),
            None,
        )
        .await;
    assert_eq!(
        outcome,
        CycleOutcome::Refused(RefuseReason::BadFrame, Some(1))
    );
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmBase, 1)]
    );
    assert_eq!(source.snapshot_count(), 1);

    // The next cycle ships from the base.
    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
}

#[tokio::test]
async fn a_failed_save_of_the_new_base_is_recorded_at_the_next_cycle() {
    let target = Target::new("failed-save").await;
    let proxy = Proxy::new(target.addr).await;
    let source = Source::new("failed-save", proxy.addr.port(), lease_view(TOKEN)).await;
    let db = sqlx_connect(&source.scratch.0.join("node.db")).await;
    sqlx::query(
        "CREATE TRIGGER no_warm_base BEFORE UPDATE ON kept_snapshots \
         WHEN NEW.purpose = 'warm_base' BEGIN SELECT RAISE(ABORT, 'injected'); END",
    )
    .execute(&db)
    .await
    .expect("trigger");

    write_first(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(1));
    assert_eq!(target.complete_seq().await, Some(1));
    assert_eq!(
        source.kept().await,
        vec![base_entry(KeepPurpose::WarmShipping, 1)],
        "the confirmed snapshot stays kept"
    );
    assert_eq!(source.status().snapshot_seq, Some(1));

    sqlx::query("DROP TRIGGER no_warm_base")
        .execute(&db)
        .await
        .expect("drop trigger");
    write_second(&source);
    assert_eq!(source.cycle().await, CycleOutcome::Complete(2));
    assert_shipped(&source, &target, 2).await;
    let bases: Vec<Option<u64>> = proxy
        .headers()
        .iter()
        .map(|header| header.base_seq)
        .collect();
    assert_eq!(
        bases,
        vec![None, Some(1)],
        "shipped from the confirmed snapshot"
    );
}

async fn sqlx_connect(path: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(path))
        .await
        .expect("a second connection")
}

#[tokio::test]
async fn a_restart_keeps_the_newest_base_and_the_newest_candidate_above_it() {
    let target = Target::new("restart-rule").await;
    let source = Source::new("restart-rule", target.addr.port(), lease_view(TOKEN)).await;
    let id = VolumeId(VOL.to_owned());
    // Two bases (a crash between recording the new one and dropping the old
    // one), a candidate at or below the base, and two above it.
    let mut taken = Vec::new();
    for (purpose, seq) in [
        (KeepPurpose::WarmBase, 1),
        (KeepPurpose::WarmBase, 3),
        (KeepPurpose::WarmShipping, 3),
        (KeepPurpose::WarmShipping, 4),
        (KeepPurpose::WarmShipping, 6),
    ] {
        let snapshot = source.engine.snapshot_volume(&id).await.expect("snapshot");
        kept::keep(&source.store, APP, &snapshot, purpose, Some(seq), None)
            .await
            .expect("keep");
        taken.push(snapshot.snapshot_id);
    }
    source.sender.restore().await;
    assert_eq!(
        source.kept().await,
        vec![
            base_entry(KeepPurpose::WarmBase, 3),
            base_entry(KeepPurpose::WarmShipping, 6)
        ]
    );
    let left: Vec<String> = source
        .engine
        .volume_snapshots()
        .into_iter()
        .map(|snapshot| snapshot.snapshot_id)
        .collect();
    assert_eq!(left.len(), 2);
    assert!(
        left.contains(&taken[1]) && left.contains(&taken[4]),
        "{left:?}"
    );
    source.sender.inner.publish();
    assert_eq!(source.status().snapshot_seq, Some(3));
}

#[tokio::test]
async fn a_volume_that_stops_being_warm_keeps_nothing_and_a_new_target_keeps_all() {
    let target = Target::new("retired").await;
    let source = Source::new("retired", target.addr.port(), lease_view(TOKEN)).await;
    write_first(&source);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn(source.sender.clone().run(async move {
        let _ = stopped.await;
    }));
    wait_for("a first copy", || async {
        target.complete_seq().await.is_some()
    })
    .await;

    // Turned into the target: the source's snapshots stay for the return
    // path, and the node reports no source entry.
    set_entry(&source.desired, |entry| {
        entry.warm.as_mut().expect("warm").role = WarmRole::Target;
    });
    wait_for("no source entry", || async {
        source.statuses.borrow().is_empty()
    })
    .await;
    // A cycle already under way may still finish: the lease gates its
    // manifest, not the role (section 6.13), and the primary revokes the
    // lease before it changes the role. A cycle holds the volume's lock from
    // start to end, so taking the lock waits for it, however slow the
    // filesystem makes its copies; every cycle after it sees the new role.
    drop(source.locks.lock(VOL).await);
    let kept = source.kept().await;
    assert!(!kept.is_empty(), "the snapshots stay");
    let shipped = target.complete_seq().await;
    let mut taken: Vec<String> = source
        .engine
        .volume_snapshots()
        .into_iter()
        .map(|snapshot| snapshot.snapshot_id)
        .collect();
    taken.sort();
    // Three cycle intervals: no cycle starts, so no snapshot is taken and
    // nothing new reaches the target.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(target.complete_seq().await, shipped, "no cycle runs");
    let mut now: Vec<String> = source
        .engine
        .volume_snapshots()
        .into_iter()
        .map(|snapshot| snapshot.snapshot_id)
        .collect();
    now.sort();
    assert_eq!(now, taken, "no snapshot is taken");

    // No longer warm: every snapshot goes.
    set_entry(&source.desired, |entry| entry.warm = None);
    wait_for("no kept snapshot", || async {
        source.kept().await.is_empty()
    })
    .await;
    assert_eq!(source.snapshot_count(), 0);

    // Warm again, then deleted.
    set_entry(&source.desired, |entry| {
        *entry = warm_entry(WarmRole::Source, target.addr.port(), TOKEN);
    });
    wait_for("a base again", || async { !source.kept().await.is_empty() }).await;
    set_entry(&source.desired, |entry| {
        entry.state = SpecVolumeState::Deleted
    });
    wait_for("no kept snapshot", || async {
        source.kept().await.is_empty()
    })
    .await;
    assert_eq!(source.snapshot_count(), 0);

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the sender stops")
        .expect("the sender task");
}

async fn wait_for<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs the producer over `ranges` of `file` and returns the frames it sent,
/// the cell hashes it computed and the most it read between two frames.
fn produce_frames(
    file: &Path,
    ranges: &[(u64, u64)],
    base: Option<CellHashes>,
) -> (Vec<Frame>, CellHashes, u64) {
    let header = StreamHeader {
        app_id: APP.to_owned(),
        volume_id: VOL.to_owned(),
        lease_id: LEASE.to_owned(),
        fencing_token: TOKEN,
        snapshot_seq: 2,
        base_seq: base.as_ref().map(|_| 1),
        taken_at: "2026-11-06T10:00:00Z".to_owned(),
        size_bytes: SIZE,
    };
    let ranges: Vec<ByteRange> = ranges
        .iter()
        .map(|(offset, length)| ByteRange {
            offset: *offset,
            length: *length,
        })
        .collect();
    let (tx, mut rx) = mpsc::channel(1 << 16);
    let produced = produce(file, &header, &ranges, base, &tx).expect("produce");
    drop(tx);
    let mut decoder = Decoder::new();
    let mut last = false;
    while let Ok(chunk) = rx.try_recv() {
        assert!(!last, "nothing after the manifest");
        last = chunk.last;
        decoder.push(&chunk.bytes);
    }
    assert!(last, "the manifest ends the stream");
    let mut frames = Vec::new();
    while let Some(frame) = decoder.next_frame().expect("a valid frame") {
        frames.push(frame);
    }
    // The order rules the target enforces.
    let mut order = Sequence::new(SIZE).expect("a sequence");
    for frame in &frames {
        match frame {
            Frame::Header(_) => {}
            Frame::Block { offset, data, .. } => order
                .block(*offset, data.len() as u64)
                .expect("a block in order"),
            Frame::Zero { offset, length } => {
                order.zero(*offset, *length).expect("a zero range in order");
            }
            Frame::Manifest(manifest) => order.manifest(manifest).expect("the manifest's count"),
        }
    }
    (frames, produced.cells, produced.max_read_between_frames)
}

#[test]
fn the_producer_cuts_blocks_at_cells_and_hashes_what_it_ships() {
    let scratch = Scratch::new("produce");
    let path = scratch.0.join("snapshot");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("create");
    file.set_len(SIZE).expect("size");
    let base_root;
    let base_cells;
    {
        file.write_all_at(&pattern(3 * CELL as usize, 1), 2 * CELL)
            .expect("write");
        file.write_all_at(&[3_u8; 10], LAST_CELL * CELL + CELL - 10)
            .expect("write");
        base_cells = hash_copy(&file, SIZE).expect("hash");
        base_root = base_cells.root_hex();
    }

    // A full copy of every range: the cells are the file's.
    let (frames, cells, _) = produce_frames(&path, &[(0, SIZE)], None);
    assert_eq!(cells.root_hex(), base_root);
    assert!(matches!(frames.first(), Some(Frame::Header(_))));
    let Some(Frame::Manifest(manifest)) = frames.last() else {
        panic!("a manifest");
    };
    assert_eq!(manifest.root_hash, base_root);
    assert_eq!(manifest.block_count, frames.len() as u64 - 2);
    for frame in &frames {
        if let Frame::Block { offset, data, .. } = frame {
            assert_eq!(
                offset / CELL,
                (offset + data.len() as u64 - 1) / CELL,
                "inside one cell"
            );
            assert!(
                data.iter().any(|byte| *byte != 0),
                "zeros go as zero ranges"
            );
        }
    }

    // Changes: zeros inside a cell, over a whole cell and across cells, a
    // partial block, and ranges wider than the change.
    file.write_all_at(&[0_u8; 5000], 2 * CELL + 100)
        .expect("write");
    file.write_all_at(&vec![0_u8; CELL as usize], 3 * CELL)
        .expect("write");
    file.write_all_at(&vec![0_u8; 2000], 5 * CELL - 1000)
        .expect("write");
    file.write_all_at(&pattern(700, 8), 9 * CELL + 3)
        .expect("write");
    let changed = [
        (2 * CELL, 8192),
        (3 * CELL, CELL),
        (5 * CELL - 4096, 8192),
        (9 * CELL, 4096),
        (20 * CELL, 3 * CELL),
    ];
    let (frames, cells, _) = produce_frames(&path, &changed, Some(base_cells));
    let expected = hash_copy(&file, SIZE).expect("hash").root_hex();
    assert_ne!(expected, base_root);
    assert_eq!(
        cells.root_hex(),
        expected,
        "the base's cells with the changed ones"
    );
    let zeros: Vec<(u64, u64)> = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Zero { offset, length } => Some((*offset, *length)),
            _ => None,
        })
        .collect();
    assert!(zeros.contains(&(3 * CELL, CELL)), "a zero cell: {zeros:?}");
    assert!(zeros.contains(&(20 * CELL, 3 * CELL)), "a hole: {zeros:?}");
}

#[test]
fn the_producer_never_reads_long_between_two_frames() {
    // The target drops a stream that sends nothing for a while. Many cells
    // covered in part, and a long run of zeros over former data, must not
    // become one long read before the next frame.
    let scratch = Scratch::new("produce-gaps");
    let path = scratch.0.join("snapshot");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("create");
    file.set_len(SIZE).expect("size");
    let mut changed = Vec::new();
    for cell in 0..200 {
        file.write_all_at(&pattern(64, 1), cell * CELL + 1)
            .expect("write");
        changed.push((cell * CELL, 4096));
    }
    file.write_all_at(&vec![0_u8; 128 << 20], 300 * CELL)
        .expect("write zeros");
    changed.push((300 * CELL, 128 << 20));
    let base = CellHashes::zeroed(SIZE).expect("cells");

    let (frames, cells, max_read) = produce_frames(&path, &changed, Some(base));
    assert_eq!(
        cells.root_hex(),
        hash_copy(&file, SIZE).expect("hash").root_hex()
    );
    assert!(
        max_read <= super::ZERO_FLUSH_BYTES + 4 * CELL,
        "read {max_read} bytes between two frames"
    );
    let zeros: u64 = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Zero { offset, length } if *offset >= 300 * CELL => Some(*length),
            _ => None,
        })
        .sum();
    assert_eq!(zeros, 128 << 20, "every zero is shipped");
}
