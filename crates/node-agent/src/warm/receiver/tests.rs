// SPDX-License-Identifier: AGPL-3.0-only
//! The warm receiver against a fake engine with real files: a test plays the
//! source with a model of its volume and checks, after every stream, that the
//! copy on disk is exactly the last complete snapshot the store names.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use quoro_backup::cells::{CELL, CellHash, CellHashes, hash_cell};
use quoro_engine::{Engine as _, EngineError, FakeEngine, FakeOp, SnapshotMethod};
use quoro_proto::wire::{
    DesiredState, Spec, SpecMesh, SpecVolumeFile, SpecVolumeState, SpecWarm, VolumeState, WarmRole,
    WarmStatus,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{ReceiverOptions, WarmReceiver};
use crate::reconcile::Snapshot;
use crate::store::{NodeStore, PendingCommit, VolumeRecord};
use crate::warm::frame::{Frame, Manifest, StreamHeader, encode};
use crate::warm::{StatusBoard, VolumeLocks};

const VOL: &str = "vol_01JD0WARM0000000000000001";
const APP: &str = "app_01JD0WARM0000000000000001";
const LEASE: &str = "lse_01JD0WARM0000000000000001";
const GIB: u64 = 1 << 30;
const SIZE: u64 = GIB;
const LAST_CELL: u64 = SIZE / CELL - 1;

/// A scratch directory under the workspace `target/`, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    /// Under `QUORO_TEST_REFLINK_DIR` when it is set, so holes and punched
    /// ranges run on the filesystem warm copies live on; under the
    /// workspace `target/` otherwise.
    fn new(tag: &str) -> Self {
        let root = std::env::var_os("QUORO_TEST_REFLINK_DIR").map_or_else(
            || std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            PathBuf::from,
        );
        let dir = root
            .join("warm-receiver-tests")
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

/// The source's volume: the cells that are not all zeros with their hashes,
/// and every cell any version of it ever wrote.
#[derive(Clone, Default)]
struct Model {
    cells: BTreeMap<u64, (Vec<u8>, CellHash)>,
    touched: BTreeSet<u64>,
}

impl Model {
    fn cell(&self, cell: u64) -> Vec<u8> {
        self.cells
            .get(&cell)
            .map_or_else(|| vec![0; CELL as usize], |(data, _)| data.clone())
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) {
        let mut at = offset;
        let mut rest = bytes;
        while !rest.is_empty() {
            let cell = at / CELL;
            let inside = usize::try_from(at % CELL).expect("small");
            let n = rest.len().min(CELL as usize - inside);
            let mut data = self.cell(cell);
            data[inside..inside + n].copy_from_slice(&rest[..n]);
            if data.iter().all(|byte| *byte == 0) {
                self.cells.remove(&cell);
            } else {
                let hash = hash_cell(&data);
                self.cells.insert(cell, (data, hash));
            }
            self.touched.insert(cell);
            at += n as u64;
            rest = &rest[n..];
        }
    }

    fn bytes(&self, offset: u64, length: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(usize::try_from(length).expect("small"));
        let mut at = offset;
        while at < offset + length {
            let cell = at / CELL;
            let inside = usize::try_from(at % CELL).expect("small");
            let n = usize::try_from((offset + length - at).min(CELL - at % CELL)).expect("small");
            match self.cells.get(&cell) {
                Some((data, _)) => out.extend_from_slice(&data[inside..inside + n]),
                None => out.resize(out.len() + n, 0),
            }
            at += n as u64;
        }
        out
    }

    fn hashes(&self) -> CellHashes {
        let mut hashes = CellHashes::zeroed(SIZE).expect("cells");
        for (cell, (_, hash)) in &self.cells {
            hashes.set(*cell, *hash).expect("in range");
        }
        hashes
    }

    fn root(&self) -> String {
        self.hashes().root_hex()
    }

    /// Every range that is not zeros, cell by cell: what a full copy ships.
    fn allocated(&self) -> Vec<(u64, u64)> {
        self.cells.keys().map(|cell| (cell * CELL, CELL)).collect()
    }
}

/// Sorts and merges ranges, as the engine reports changed ranges.
fn merged(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (offset, length) in ranges {
        match out.last_mut() {
            Some((last, last_length)) if *last + *last_length >= offset => {
                let end = (*last + *last_length).max(offset + length);
                *last_length = end - *last;
            }
            _ => out.push((offset, length)),
        }
    }
    out
}

/// The frames after the header: blocks cut at cell boundaries, runs of zeros
/// as one zero range, and the manifest.
fn body_frames(model: &Model, ranges: &[(u64, u64)]) -> (Vec<Frame>, Manifest) {
    let mut frames: Vec<Frame> = Vec::new();
    for (offset, length) in merged(ranges.to_vec()) {
        let mut at = offset;
        while at < offset + length {
            let n = (offset + length - at).min(CELL - at % CELL);
            let data = model.bytes(at, n);
            if data.iter().all(|byte| *byte == 0) {
                match frames.last_mut() {
                    Some(Frame::Zero {
                        offset: zero,
                        length: zero_length,
                    }) if *zero + *zero_length == at => *zero_length += n,
                    _ => frames.push(Frame::Zero {
                        offset: at,
                        length: n,
                    }),
                }
            } else {
                frames.push(Frame::Block {
                    offset: at,
                    blake3: hash_cell(&data),
                    data,
                });
            }
            at += n;
        }
    }
    let manifest = Manifest {
        block_count: frames.len() as u64,
        root_hash: model.root(),
    };
    (frames, manifest)
}

fn header(token: u64, seq: u64, base: Option<u64>) -> StreamHeader {
    StreamHeader {
        app_id: APP.to_owned(),
        volume_id: VOL.to_owned(),
        lease_id: LEASE.to_owned(),
        fencing_token: token,
        snapshot_seq: seq,
        base_seq: base,
        taken_at: format!("2026-11-06T10:00:{:02}Z", seq % 60),
        size_bytes: SIZE,
    }
}

/// A whole stream, encoded.
fn stream(header: &StreamHeader, model: &Model, ranges: &[(u64, u64)]) -> Vec<Vec<u8>> {
    let (frames, manifest) = body_frames(model, ranges);
    let mut out = vec![encode(&Frame::Header(header.clone()))];
    out.extend(frames.iter().map(encode));
    out.push(encode(&Frame::Manifest(manifest)));
    out
}

/// How the test source ends its request body.
#[derive(Clone, Copy)]
enum End {
    /// The last chunk.
    Complete,
    /// The connection's write side closes without the last chunk.
    Cut,
    /// The source stops sending and waits.
    Stall,
}

/// What came back: the status code and the body, or nothing.
#[derive(Debug)]
struct Reply {
    status: u16,
    body: String,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        assert_eq!(self.status, 200, "an answer, not a failure: {}", self.body);
        serde_json::from_str(&self.body).expect("a JSON answer")
    }

    fn assert_complete(&self, seq: u64) {
        let answer = self.json();
        assert_eq!(answer["status"], "complete", "{answer}");
        assert_eq!(answer["snapshot_seq"], seq, "{answer}");
        assert!(answer.get("reason").is_none(), "{answer}");
    }

    fn assert_refused(&self, reason: &str, seq: Option<u64>) {
        let answer = self.json();
        assert_eq!(answer["status"], "refused", "{answer}");
        assert_eq!(answer["reason"], reason, "{answer}");
        assert_eq!(
            answer["snapshot_seq"],
            seq.map_or(serde_json::Value::Null, serde_json::Value::from),
            "{answer}"
        );
    }
}

/// Posts `chunks` as a chunked request body, one chunk per frame, and reads
/// the reply while it writes, as the source does.
async fn post_to(addr: SocketAddr, path: &str, chunks: Vec<Vec<u8>>, end: End) -> Option<Reply> {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut read, mut write) = stream.into_split();
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: target\r\ncontent-type: application/octet-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n"
    );
    let writer = tokio::spawn(async move {
        let mut out = head.into_bytes();
        for chunk in chunks {
            out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            out.extend_from_slice(&chunk);
            out.extend_from_slice(b"\r\n");
            // Write in pieces so a refusal can arrive while the body is
            // still going out.
            if out.len() >= 256 << 10 {
                if write.write_all(&out).await.is_err() {
                    return write;
                }
                out.clear();
            }
        }
        if write.write_all(&out).await.is_err() {
            return write;
        }
        match end {
            End::Complete => {
                let _ = write.write_all(b"0\r\n\r\n").await;
            }
            End::Cut => {
                let _ = write.shutdown().await;
            }
            End::Stall => {}
        }
        write
    });
    let mut raw = Vec::new();
    let read_all = tokio::time::timeout(Duration::from_secs(20), read.read_to_end(&mut raw)).await;
    let _write = writer.await.expect("writer");
    if !matches!(read_all, Ok(Ok(_))) || raw.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n")?;
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    Some(Reply {
        status,
        body: body.to_owned(),
    })
}

/// A warm target with one volume entry, its receiver serving on a loopback
/// listener.
struct Harness {
    _scratch: Scratch,
    engine: Arc<FakeEngine>,
    store: NodeStore,
    desired: watch::Sender<Option<Snapshot>>,
    locks: VolumeLocks,
    statuses: watch::Receiver<Vec<WarmStatus>>,
    receiver: WarmReceiver<FakeEngine>,
    addr: SocketAddr,
    server: JoinHandle<()>,
    volume_path: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn entry(token: u64) -> SpecVolumeFile {
    SpecVolumeFile {
        volume_id: VOL.to_owned(),
        app_id: APP.to_owned(),
        size_gb: 1,
        state: SpecVolumeState::Present,
        restore: None,
        warm: Some(SpecWarm {
            role: WarmRole::Target,
            peer_node_id: "nod_01JD0WARM0000000000000001".to_owned(),
            peer_address: "127.0.0.1".to_owned(),
            port: 51822,
            interval_s: 30,
            lease_id: LEASE.to_owned(),
            fencing_token: token,
        }),
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

impl Harness {
    async fn new(tag: &str) -> Self {
        let scratch = Scratch::new(tag);
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(scratch.0.join("volumes"));
        engine.set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let (desired, desired_rx) = watch::channel(Some(snapshot(1, vec![entry(5)])));
        let locks = VolumeLocks::new();
        let (board, statuses) = StatusBoard::new();
        let receiver = WarmReceiver::new(
            Arc::clone(&engine),
            store.clone(),
            desired_rx,
            locks.clone(),
            board,
            ReceiverOptions::quick(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let serving = receiver.clone();
        let server = tokio::spawn(async move { serving.serve(listener).await });
        let volume_path = scratch.0.join("volumes").join(VOL);
        Self {
            _scratch: scratch,
            engine,
            store,
            desired,
            locks,
            statuses,
            receiver,
            addr,
            server,
            volume_path,
        }
    }

    async fn post(&self, chunks: Vec<Vec<u8>>, end: End) -> Option<Reply> {
        post_to(self.addr, super::REPLICATE_PATH, chunks, end).await
    }

    /// Changes the volume entry in a new desired-state revision.
    fn set_entry(&self, change: impl FnOnce(&mut SpecVolumeFile)) {
        self.desired.send_modify(|current| {
            let snapshot = current.as_mut().expect("a snapshot");
            snapshot.rev += 1;
            snapshot.state.rev = snapshot.rev;
            change(&mut snapshot.state.spec.volumes[0]);
        });
    }

    fn set_warm(&self, change: impl FnOnce(&mut SpecWarm)) {
        self.set_entry(|entry| change(entry.warm.as_mut().expect("warm")));
    }

    async fn row(&self) -> crate::store::WarmTargetRecord {
        self.store
            .warm_target(VOL)
            .await
            .expect("store")
            .expect("a warm target row")
    }

    /// The copy on disk matches `expected` in every cell `cells` names, the
    /// store names `seq` as complete with nothing pending, and no staging
    /// file is left.
    async fn assert_copy(&self, expected: &Model, seq: Option<u64>, cells: &BTreeSet<u64>) {
        let row = self.row().await;
        assert_eq!(row.complete_seq, seq, "complete copy");
        assert!(row.pending.is_none(), "nothing pending");
        assert!(
            self.engine.staged_volumes().is_empty(),
            "no staging file left"
        );
        if seq.is_none() {
            return;
        }
        let recorded = CellHashes::from_bytes(row.cells.as_deref().expect("cells"), SIZE)
            .expect("recorded cells");
        assert_eq!(recorded.root_hex(), expected.root(), "recorded cell hashes");
        let file = std::fs::File::open(&self.volume_path).expect("the copy");
        let mut checked = cells.clone();
        checked.extend([0, LAST_CELL]);
        for cell in checked {
            let mut buf = vec![0_u8; CELL as usize];
            std::os::unix::fs::FileExt::read_exact_at(&file, &mut buf, cell * CELL)
                .expect("read a cell");
            assert!(buf == expected.cell(cell), "cell {cell} of the copy");
        }
    }

    fn status(&self) -> WarmStatus {
        let statuses = self.statuses.borrow();
        assert_eq!(statuses.len(), 1, "one status entry: {statuses:?}");
        statuses[0].clone()
    }
}

/// A volume with data at the start, in the middle of a cell and in the last
/// cell.
fn first_model() -> Model {
    let mut model = Model::default();
    model.write(0, &[7_u8; 4096]);
    model.write(3 * CELL + 100, &vec![9_u8; 300_000]);
    model.write(5 * CELL, &pattern(CELL as usize, 1));
    model.write(LAST_CELL * CELL + CELL - 10, &[1_u8; 10]);
    model
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).expect("small").wrapping_add(seed))
        .collect()
}

/// Ships `first_model` as a full copy, seq 1, token 5.
async fn ship_first(h: &Harness) -> Model {
    let model = first_model();
    let reply = h
        .post(
            stream(&header(5, 1, None), &model, &model.allocated()),
            End::Complete,
        )
        .await
        .expect("a reply");
    reply.assert_complete(1);
    model
}

#[tokio::test]
async fn a_full_copy_then_an_incremental_one_become_the_complete_copy() {
    let h = Harness::new("full-then-incremental").await;
    let first = ship_first(&h).await;
    h.assert_copy(&first, Some(1), &first.touched).await;
    let status = h.status();
    assert_eq!(status.role, WarmRole::Target);
    assert_eq!(status.snapshot_seq, Some(1));
    assert_eq!(
        status.snapshot_taken_at.as_deref(),
        Some("2026-11-06T10:00:01Z")
    );
    assert!(status.completed_at.is_some());
    assert!(status.error.is_none());

    // The incremental copy: a partial block, zeros over part of a cell and
    // over a whole cell, and a new whole cell.
    let mut second = first.clone();
    let mut changed = Vec::new();
    for (offset, bytes) in [
        (3 * CELL + 50, pattern(1000, 3)),
        (100, vec![0_u8; 1000]),
        (5 * CELL, vec![0_u8; CELL as usize]),
        (8 * CELL, pattern(CELL as usize, 4)),
        (9 * CELL + CELL - 5, pattern(10, 5)),
    ] {
        second.write(offset, &bytes);
        changed.push((offset, bytes.len() as u64));
    }
    let reply = h
        .post(
            stream(&header(5, 2, Some(1)), &second, &changed),
            End::Complete,
        )
        .await
        .expect("a reply");
    reply.assert_complete(2);
    h.assert_copy(&second, Some(2), &second.touched).await;
    assert_eq!(h.status().snapshot_seq, Some(2));
}

#[tokio::test]
async fn a_stale_token_is_refused_recorded_and_reported() {
    let h = Harness::new("stale-token").await;
    let first = ship_first(&h).await;

    // Below the entry's token.
    let mut newer = first.clone();
    newer.write(0, &[1_u8; 10]);
    let reply = h
        .post(
            stream(&header(4, 2, Some(1)), &newer, &[(0, 10)]),
            End::Complete,
        )
        .await
        .expect("a reply");
    reply.assert_refused("stale_token", Some(1));
    let row = h.row().await;
    assert_eq!(row.rejected_token, Some(4));
    assert!(row.rejected_at.is_some());
    let status = h.status();
    assert_eq!(status.rejected_token, Some(4));
    assert!(status.rejected_at.is_some());
    h.assert_copy(&first, Some(1), &newer.touched).await;

    // A higher token than the entry's is accepted and becomes the highest
    // seen; one between the two is then stale although the entry allows it.
    h.post(
        stream(&header(7, 2, Some(1)), &newer, &[(0, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(2);
    assert_eq!(h.row().await.highest_token, 7);
    let mut third = newer.clone();
    third.write(CELL, &[2_u8; 10]);
    h.post(
        stream(&header(6, 3, Some(2)), &third, &[(CELL, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("stale_token", Some(2));
    assert_eq!(h.row().await.rejected_token, Some(6));
    h.assert_copy(&newer, Some(2), &third.touched).await;
}

#[tokio::test]
async fn every_check_refuses_before_anything_is_written() {
    let h = Harness::new("checks").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(0, &[1_u8; 10]);
    let ranges = [(0, 10)];

    let cases: Vec<(&str, StreamHeader, &str, Option<u64>)> = vec![
        (
            "another volume",
            StreamHeader {
                volume_id: "vol_01JD0WARM0000000000000009".to_owned(),
                ..header(5, 2, Some(1))
            },
            "not_target",
            None,
        ),
        (
            "another app",
            StreamHeader {
                app_id: "app_01JD0WARM0000000000000009".to_owned(),
                ..header(5, 2, Some(1))
            },
            "not_target",
            Some(1),
        ),
        (
            "another lease",
            StreamHeader {
                lease_id: "lse_01JD0WARM0000000000000009".to_owned(),
                ..header(5, 2, Some(1))
            },
            "wrong_lease",
            Some(1),
        ),
        (
            "a base it does not hold",
            header(5, 3, Some(2)),
            "base_mismatch",
            Some(1),
        ),
        (
            "a seq not above the base",
            header(5, 1, Some(1)),
            "not_newer",
            Some(1),
        ),
        (
            "a full copy not above the complete one",
            header(5, 1, None),
            "not_newer",
            Some(1),
        ),
        (
            "another size",
            StreamHeader {
                size_bytes: 2 * GIB,
                ..header(5, 2, Some(1))
            },
            "bad_frame",
            Some(1),
        ),
    ];
    for (what, header, reason, seq) in cases {
        let reply = h
            .post(stream(&header, &newer, &ranges), End::Complete)
            .await
            .unwrap_or_else(|| panic!("a reply for {what}"));
        let answer = reply.json();
        assert_eq!(answer["status"], "refused", "{what}: {answer}");
        assert_eq!(answer["reason"], reason, "{what}: {answer}");
        assert_eq!(
            answer["snapshot_seq"],
            seq.map_or(serde_json::Value::Null, serde_json::Value::from),
            "{what}: {answer}"
        );
        h.assert_copy(&first, Some(1), &newer.touched).await;
    }

    // A stream from another address than the entry's peer.
    h.set_warm(|warm| warm.peer_address = "127.0.0.2".to_owned());
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &ranges),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("not_target", Some(1));
    // A node that is no longer the target.
    h.set_warm(|warm| {
        warm.peer_address = "127.0.0.1".to_owned();
        warm.role = WarmRole::Source;
    });
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &ranges),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("not_target", Some(1));
    h.assert_copy(&first, Some(1), &newer.touched).await;
    // The highest token never moved for a refused stream.
    assert_eq!(h.row().await.highest_token, 5);
}

#[tokio::test]
async fn a_body_that_does_not_start_with_a_header_or_hits_another_path_is_refused() {
    let h = Harness::new("no-header").await;
    let first = first_model();
    let (frames, _) = body_frames(&first, &first.allocated());
    let reply = h
        .post(vec![encode(&frames[0])], End::Complete)
        .await
        .expect("a reply");
    reply.assert_refused("bad_frame", None);
    let reply = post_to(h.addr, "/v1/other", Vec::new(), End::Complete)
        .await
        .expect("a reply");
    assert_eq!(reply.status, 404);
    assert!(h.engine.staged_volumes().is_empty());
}

#[tokio::test]
async fn a_second_stream_of_a_volume_is_busy() {
    let h = Harness::new("busy").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(0, &[1_u8; 10]);
    let held = h.locks.try_lock(VOL).expect("free");
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &[(0, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("busy", Some(1));
    drop(held);
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &[(0, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(2);

    // Two real streams: the first stalls with the lock held, the second is
    // busy, and the stalled one never replaces the copy.
    let mut third = newer.clone();
    third.write(2 * CELL, &[3_u8; 10]);
    let mut frames = stream(&header(5, 3, Some(2)), &third, &[(2 * CELL, 10)]);
    frames.pop();
    let addr = h.addr;
    let stalled = tokio::spawn(post_to(addr, super::REPLICATE_PATH, frames, End::Stall));
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.post(
        stream(&header(5, 3, Some(2)), &third, &[(2 * CELL, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("busy", Some(2));
    let _ = stalled.await;
    h.assert_copy(&newer, Some(2), &third.touched).await;
}

/// How a test source breaks a stream.
#[derive(Clone, Copy, Debug)]
enum Break {
    CutBeforeManifest,
    StallBeforeManifest,
    BadBlockHash,
    WrongRoot,
    WrongCount,
    OutOfOrder,
    SecondHeader,
    DataAfterManifest,
    BlockAcrossCells,
    BeyondTheVolume,
}

/// A root hash that differs from `root` in its first character.
fn other_root(root: &str) -> String {
    let first = if root.starts_with('0') { '1' } else { '0' };
    format!("{first}{}", &root[1..])
}

/// A stream of `newer` over `base`, broken as `how`; the reason it should be
/// refused with, or `None` when no answer is expected.
fn broken(
    how: Break,
    head: &StreamHeader,
    newer: &Model,
    ranges: &[(u64, u64)],
) -> (Vec<Vec<u8>>, End, Option<&'static str>) {
    let (mut frames, mut manifest) = body_frames(newer, ranges);
    assert!(frames.len() >= 2, "the broken streams need two frames");
    let mut end = End::Complete;
    let mut reason = Some("bad_frame");
    let mut tail: Vec<Vec<u8>> = Vec::new();
    let mut with_manifest = true;
    match how {
        Break::CutBeforeManifest => {
            with_manifest = false;
            end = End::Cut;
            reason = None;
        }
        Break::StallBeforeManifest => {
            with_manifest = false;
            end = End::Stall;
        }
        Break::BadBlockHash => {
            let block = frames.iter_mut().find_map(|frame| match frame {
                Frame::Block { blake3, .. } => Some(blake3),
                _ => None,
            });
            match block {
                Some(block) => block[0] ^= 1,
                // Zeros only: break the root instead.
                None => {
                    manifest.root_hash = other_root(&manifest.root_hash);
                    reason = Some("root_mismatch");
                }
            }
        }
        Break::WrongRoot => {
            manifest.root_hash = other_root(&manifest.root_hash);
            reason = Some("root_mismatch");
        }
        Break::WrongCount => manifest.block_count += 1,
        Break::OutOfOrder => frames.swap(0, 1),
        Break::SecondHeader => frames.insert(1, Frame::Header(head.clone())),
        Break::DataAfterManifest => tail.push(encode(&frames[0])),
        Break::BlockAcrossCells => frames.push(Frame::Block {
            offset: 20 * CELL - 10,
            blake3: hash_cell(&[0_u8; 20]),
            data: vec![0_u8; 20],
        }),
        Break::BeyondTheVolume => frames.push(Frame::Zero {
            offset: SIZE - CELL,
            length: 2 * CELL,
        }),
    }
    if matches!(how, Break::BlockAcrossCells | Break::BeyondTheVolume) {
        manifest.block_count = frames.len() as u64;
    }
    let mut out = vec![encode(&Frame::Header(head.clone()))];
    out.extend(frames.iter().map(encode));
    if with_manifest {
        out.push(encode(&Frame::Manifest(manifest)));
    }
    out.extend(tail);
    (out, end, reason)
}

#[tokio::test]
async fn a_partial_or_broken_transfer_never_replaces_the_complete_copy() {
    let h = Harness::new("broken").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(3 * CELL + 10, &pattern(5000, 7));
    newer.write(6 * CELL, &pattern(CELL as usize, 8));
    newer.write(7 * CELL, &[0_u8; 100]);
    let ranges = [(3 * CELL + 10, 5000), (6 * CELL, CELL), (7 * CELL, 100)];
    for how in [
        Break::CutBeforeManifest,
        Break::StallBeforeManifest,
        Break::BadBlockHash,
        Break::WrongRoot,
        Break::WrongCount,
        Break::OutOfOrder,
        Break::SecondHeader,
        Break::DataAfterManifest,
        Break::BlockAcrossCells,
        Break::BeyondTheVolume,
    ] {
        let (chunks, end, reason) = broken(how, &header(5, 2, Some(1)), &newer, &ranges);
        let reply = h.post(chunks, end).await;
        if let Some(reason) = reason {
            reply
                .unwrap_or_else(|| panic!("a reply for {how:?}"))
                .assert_refused(reason, Some(1));
        }
        // The stream's lock is released once the receiver is done with it.
        let mut freed = false;
        for _ in 0..100 {
            if h.locks.try_lock(VOL).is_some() {
                freed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(freed, "{how:?}: the volume lock is released");
        h.assert_copy(&first, Some(1), &newer.touched).await;
        assert!(
            h.row().await.error.is_some(),
            "{how:?}: the error is recorded"
        );
    }
    // The source's next cycle from the same base still lands.
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &ranges),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(2);
    h.assert_copy(&newer, Some(2), &newer.touched).await;
    assert!(
        h.row().await.error.is_none(),
        "a complete copy clears the error"
    );
}

#[tokio::test]
async fn a_full_copy_cut_short_leaves_no_copy_at_all() {
    let h = Harness::new("first-cut").await;
    let model = first_model();
    let (chunks, end, _) = broken(
        Break::CutBeforeManifest,
        &header(5, 1, None),
        &model,
        &model.allocated(),
    );
    let _ = h.post(chunks, end).await;
    for _ in 0..100 {
        if h.locks.try_lock(VOL).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    h.assert_copy(&model, None, &model.touched).await;
    assert!(!h.volume_path.exists(), "no file before a complete stream");
    assert!(h.engine.volumes().is_empty());
}

#[tokio::test]
async fn a_refusal_reaches_a_source_that_is_still_writing() {
    let h = Harness::new("refuse-early").await;
    let first = ship_first(&h).await;
    // 24 MiB of blocks behind a stale header: the answer comes after the
    // header, while the source is still writing.
    let mut newer = first.clone();
    let mut ranges = Vec::new();
    for cell in 10..34 {
        newer.write(
            cell * CELL,
            &pattern(CELL as usize, u8::try_from(cell).expect("small")),
        );
        ranges.push((cell * CELL, CELL));
    }
    let reply = h
        .post(
            stream(&header(3, 2, Some(1)), &newer, &ranges),
            End::Complete,
        )
        .await
        .expect("the refusal arrives");
    reply.assert_refused("stale_token", Some(1));
    h.assert_copy(&first, Some(1), &newer.touched).await;
}

#[tokio::test]
async fn a_stream_the_desired_state_overtook_is_not_swapped_in() {
    let h = Harness::new("overtaken").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(0, &[1_u8; 10]);

    for (change, reason) in [(0, "stale_token"), (1, "wrong_lease"), (2, "not_target")] {
        // The stream stalls after its blocks; the desired state moves; the
        // rest arrives.
        let chunks = stream(&header(5, 2, Some(1)), &newer, &[(0, 10)]);
        let (first_part, rest) = chunks.split_at(chunks.len() - 1);
        let stream = TcpStream::connect(h.addr).await.expect("connect");
        let (mut read, mut write) = stream.into_split();
        let mut out = format!(
            "POST {} HTTP/1.1\r\nhost: target\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
            super::REPLICATE_PATH
        )
        .into_bytes();
        for chunk in first_part {
            out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            out.extend_from_slice(chunk);
            out.extend_from_slice(b"\r\n");
        }
        write.write_all(&out).await.expect("write");
        tokio::time::sleep(Duration::from_millis(100)).await;
        match change {
            0 => h.set_warm(|warm| warm.fencing_token = 9),
            1 => h.set_warm(|warm| warm.lease_id = "lse_01JD0WARM0000000000000002".to_owned()),
            _ => h.set_warm(|warm| warm.role = WarmRole::Source),
        }
        let mut out = Vec::new();
        for chunk in rest {
            out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            out.extend_from_slice(chunk);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b"0\r\n\r\n");
        write.write_all(&out).await.expect("write");
        let mut raw = String::new();
        read.read_to_string(&mut raw).await.expect("read");
        let (_, body) = raw.split_once("\r\n\r\n").expect("a reply");
        let answer: serde_json::Value = serde_json::from_str(body).expect("json");
        assert_eq!(answer["reason"], reason, "{answer}");
        h.assert_copy(&first, Some(1), &newer.touched).await;
        h.set_warm(|warm| {
            warm.fencing_token = 5;
            warm.lease_id = LEASE.to_owned();
            warm.role = WarmRole::Target;
        });
    }
}

#[tokio::test]
async fn a_failed_swap_keeps_the_copy_the_store_names() {
    let h = Harness::new("failed-swap").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(CELL, &[4_u8; 4096]);
    h.engine.fail_next(
        FakeOp::CommitStagedVolume,
        EngineError::Io("the disk is full".to_owned()),
    );
    let reply = h
        .post(
            stream(&header(5, 2, Some(1)), &newer, &[(CELL, 4096)]),
            End::Complete,
        )
        .await
        .expect("a reply");
    assert_eq!(reply.status, 500, "{reply:?}");
    assert!(reply.body.contains("the disk is full"), "{reply:?}");
    h.assert_copy(&first, Some(1), &newer.touched).await;
    assert!(h.status().error.is_some());
    // The next cycle from the same base lands.
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &[(CELL, 4096)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(2);
    h.assert_copy(&newer, Some(2), &newer.touched).await;
}

/// Writes `model`'s touched cells straight into the volume file, as a swap
/// the store did not record yet would have.
fn write_file(h: &Harness, model: &Model) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&h.volume_path)
        .expect("the copy");
    for cell in &model.touched {
        std::os::unix::fs::FileExt::write_all_at(&file, &model.cell(*cell), cell * CELL)
            .expect("write a cell");
    }
}

async fn leave_pending(h: &Harness, seq: u64, model: &Model) {
    let mut row = h.row().await;
    row.pending = Some(PendingCommit {
        seq,
        taken_at: "2026-11-06T10:00:59Z".to_owned(),
        root: model.root(),
        cells: model.hashes().to_bytes(),
    });
    h.store.save_warm_target(&row).await.expect("save");
}

#[tokio::test]
async fn a_copy_left_pending_is_resolved_by_its_hash() {
    let h = Harness::new("pending").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(2 * CELL, &[6_u8; 4096]);

    // The swap happened and the record did not: the pending copy is adopted.
    write_file(&h, &newer);
    leave_pending(&h, 2, &newer).await;
    h.receiver.recover_pending().await;
    h.assert_copy(&newer, Some(2), &newer.touched).await;

    // The swap did not happen: the complete copy stays.
    let mut third = newer.clone();
    third.write(4 * CELL, &[8_u8; 4096]);
    leave_pending(&h, 3, &third).await;
    h.receiver.recover_pending().await;
    h.assert_copy(&newer, Some(2), &third.touched).await;

    // The file is neither: no copy, so the next stream is a full one.
    let mut other = newer.clone();
    other.write(11 * CELL, &[9_u8; 4096]);
    write_file(&h, &other);
    leave_pending(&h, 3, &third).await;
    h.receiver.recover_pending().await;
    h.assert_copy(&other, None, &other.touched).await;
    h.post(
        stream(&header(5, 3, Some(2)), &third, &[(4 * CELL, 4096)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("base_mismatch", None);
    h.post(
        stream(&header(5, 3, None), &third, &third.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(3);
    // A full copy over an older file: every cell the old file had and the
    // new one does not must read as zeros.
    h.assert_copy(&third, Some(3), &other.touched).await;
}

#[tokio::test]
async fn a_stream_resolves_a_copy_left_pending_before_its_checks() {
    let h = Harness::new("pending-stream").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(2 * CELL, &[6_u8; 4096]);
    write_file(&h, &newer);
    leave_pending(&h, 2, &newer).await;
    // The source got no answer for seq 2 and ships seq 3 from base 2, which
    // the target holds once it resolved the pending copy.
    let mut third = newer.clone();
    third.write(3 * CELL, &[1_u8; 10]);
    h.post(
        stream(&header(5, 3, Some(2)), &third, &[(3 * CELL, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(3);
    h.assert_copy(&third, Some(3), &third.touched).await;
}

#[tokio::test]
async fn the_running_receiver_listens_reports_and_removes_a_deleted_copy() {
    let scratch = Scratch::new("run");
    let engine = Arc::new(FakeEngine::new());
    engine.set_volume_dir(scratch.0.join("volumes"));
    engine.set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
    let store = NodeStore::open(&scratch.0).await.expect("store");
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe");
        probe.local_addr().expect("addr").port()
    };
    let mut target = entry(5);
    target.warm.as_mut().expect("warm").port = port;
    let (desired, desired_rx) = watch::channel(Some(snapshot(1, vec![target.clone()])));
    let (board, statuses) = StatusBoard::new();
    let receiver = WarmReceiver::new(
        Arc::clone(&engine),
        store.clone(),
        desired_rx,
        VolumeLocks::new(),
        board,
        ReceiverOptions::quick(),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(receiver.run(async {
        let _ = stopped.await;
    }));

    // Before any stream: an entry with no copy, and a target row.
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let mut listening = false;
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            listening = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(listening, "the receiver listens on the mesh address");
    let status = statuses.borrow().clone();
    assert_eq!(status.len(), 1, "{status:?}");
    assert_eq!(status[0].snapshot_seq, None);
    assert!(status[0].error.is_none(), "{status:?}");
    assert!(store.warm_target(VOL).await.expect("store").is_some());

    let model = first_model();
    post_to(
        addr,
        super::REPLICATE_PATH,
        stream(&header(5, 1, None), &model, &model.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(1);
    assert_eq!(statuses.borrow()[0].snapshot_seq, Some(1));
    assert_eq!(engine.volumes().len(), 1);

    // A `deleted` entry removes the copy and marks the row removed, which
    // keeps the reconciler off the entry, and no status remains.
    target.state = SpecVolumeState::Deleted;
    desired.send_replace(Some(snapshot(2, vec![target])));
    let mut removed = false;
    for _ in 0..100 {
        if store
            .warm_target(VOL)
            .await
            .expect("store")
            .is_some_and(|row| row.removed && row.complete_seq.is_none())
        {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(removed, "the row is marked removed");
    assert!(engine.volumes().is_empty(), "the copy is removed");
    assert!(!scratch.0.join("volumes").join(VOL).exists());
    for _ in 0..100 {
        if statuses.borrow().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(statuses.borrow().is_empty());
    // The listener closed with the entry.
    let mut closed = false;
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_err() {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(closed, "the listener closes");

    // Once the entry leaves the spec, the row is forgotten.
    desired.send_replace(Some(snapshot(3, Vec::new())));
    let mut forgotten = false;
    for _ in 0..100 {
        if store.warm_target(VOL).await.expect("store").is_none() {
            forgotten = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(forgotten, "the row is forgotten");

    let _ = stop.send(());
    task.await.expect("the receiver stops");
}

#[tokio::test]
async fn a_listener_that_cannot_bind_is_reported() {
    let scratch = Scratch::new("bind");
    let engine = Arc::new(FakeEngine::new());
    engine.set_volume_dir(scratch.0.join("volumes"));
    engine.set_snapshot_method(SnapshotMethod::Reflink, Duration::ZERO);
    let store = NodeStore::open(&scratch.0).await.expect("store");
    let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("taken");
    let mut target = entry(5);
    target.warm.as_mut().expect("warm").port = taken.local_addr().expect("addr").port();
    let (_desired, desired_rx) = watch::channel(Some(snapshot(1, vec![target])));
    let (board, statuses) = StatusBoard::new();
    let receiver = WarmReceiver::new(
        engine,
        store,
        desired_rx,
        VolumeLocks::new(),
        board,
        ReceiverOptions::quick(),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(receiver.run(async {
        let _ = stopped.await;
    }));
    let mut reported = None;
    for _ in 0..100 {
        if let Some(error) = statuses.borrow().first().and_then(|s| s.error.clone()) {
            reported = Some(error);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let error = reported.expect("the bind error is reported");
    assert!(error.contains("cannot listen"), "{error}");
    let _ = stop.send(());
    task.await.expect("stops");
}

#[tokio::test]
async fn the_nodes_own_volume_never_takes_a_stream() {
    // A node that turns from source into target holds the volume as its
    // own, with data the copy does not have. No stream writes that file
    // until it is handed over.
    let h = Harness::new("own-volume").await;
    let info = h
        .engine
        .ensure_volume(&quoro_engine::VolumeId(VOL.to_owned()), 1)
        .await
        .expect("the live volume");
    std::os::unix::fs::FileExt::write_all_at(
        &std::fs::OpenOptions::new()
            .write(true)
            .open(&info.path)
            .expect("open"),
        &[0xAB_u8; 4096],
        CELL,
    )
    .expect("write");
    let own = |state| VolumeRecord {
        volume_id: VOL.to_owned(),
        app_id: APP.to_owned(),
        size_gb: 1,
        state,
        error: None,
        allocated_bytes: None,
    };
    h.store
        .save_volume(&own(VolumeState::Ready))
        .await
        .expect("row");
    let model = first_model();
    let reply = h
        .post(
            stream(&header(5, 1, None), &model, &model.allocated()),
            End::Complete,
        )
        .await
        .expect("a reply");
    assert_eq!(reply.status, 500, "{reply:?}");
    assert!(reply.body.contains("as its own"), "{reply:?}");
    let mut live = Model::default();
    live.write(CELL, &[0xAB_u8; 4096]);
    let file = std::fs::File::open(&h.volume_path).expect("the live file");
    let mut buf = vec![0_u8; CELL as usize];
    std::os::unix::fs::FileExt::read_exact_at(&file, &mut buf, CELL).expect("read");
    assert!(buf == live.cell(1), "the live data is untouched");
    assert!(h.engine.staged_volumes().is_empty());
    assert_eq!(h.row().await.complete_seq, None);
    assert!(h.status().error.is_some_and(|e| e.contains("as its own")));

    // Handed over: the next full copy lands over it.
    h.store
        .save_volume(&own(VolumeState::Deleted))
        .await
        .expect("row");
    h.post(
        stream(&header(5, 1, None), &model, &model.allocated()),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(1);
    let mut cells = model.touched.clone();
    cells.insert(1);
    h.assert_copy(&model, Some(1), &cells).await;
}

#[tokio::test]
async fn an_incremental_copy_without_recorded_cell_hashes_hashes_the_copy_once() {
    let h = Harness::new("no-cells").await;
    let first = ship_first(&h).await;
    let mut row = h.row().await;
    row.cells = None;
    h.store.save_warm_target(&row).await.expect("save");
    let mut newer = first.clone();
    newer.write(2 * CELL + 7, &[5_u8; 100]);
    // A stream that fails still leaves the hashes it computed recorded.
    let (chunks, end, _) = broken(
        Break::WrongRoot,
        &header(5, 2, Some(1)),
        &newer,
        &[(2 * CELL + 7, 100), (3 * CELL, 10)],
    );
    h.post(chunks, end)
        .await
        .expect("a reply")
        .assert_refused("root_mismatch", Some(1));
    let cells = h.row().await.cells.expect("the computed hashes are kept");
    assert_eq!(
        CellHashes::from_bytes(&cells, SIZE)
            .expect("cells")
            .root_hex(),
        first.root()
    );
    h.post(
        stream(&header(5, 2, Some(1)), &newer, &[(2 * CELL + 7, 100)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(2);
    h.assert_copy(&newer, Some(2), &newer.touched).await;
}

/// Writes a stream's head and `chunks`, without the last chunk, and returns
/// the connection.
async fn open_stream(addr: SocketAddr, chunks: &[Vec<u8>]) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut out = format!(
        "POST {} HTTP/1.1\r\nhost: target\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
        super::REPLICATE_PATH
    )
    .into_bytes();
    for chunk in chunks {
        out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    stream.write_all(&out).await.expect("write");
    stream
}

async fn wait_unlocked(h: &Harness) {
    for _ in 0..250 {
        if h.locks.try_lock(VOL).is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the volume lock is never released");
}

#[tokio::test]
async fn a_source_that_closes_after_its_last_frame_does_not_cut_the_swap() {
    // Hyper drops a request's handler when the client closes the connection.
    // A stream whose body is complete still lands, and nothing it staged is
    // left to block the next one.
    let h = Harness::new("early-close").await;
    let first = ship_first(&h).await;
    // Partial blocks in many cells, so the hashing at the manifest takes a
    // while and the close lands in the middle of it.
    let mut newer = first.clone();
    let mut ranges = Vec::new();
    for cell in 40..120 {
        newer.write(
            cell * CELL + 1,
            &[u8::try_from(cell % 200).expect("small"); 64],
        );
        ranges.push((cell * CELL + 1, 64));
    }
    let mut chunks = stream(&header(5, 2, Some(1)), &newer, &ranges);
    let last = chunks.pop().expect("the manifest");
    let mut connection = open_stream(h.addr, &chunks).await;
    let mut out = format!("{:x}\r\n", last.len()).into_bytes();
    out.extend_from_slice(&last);
    out.extend_from_slice(b"\r\n0\r\n\r\n");
    connection.write_all(&out).await.expect("write");
    drop(connection);
    // The stream ends one way or the other: swapped in, or dropped with an
    // error. Either way the store names the copy on disk.
    let mut ended = false;
    for _ in 0..250 {
        let row = h.row().await;
        if row.complete_seq == Some(2) || row.error.is_some() {
            ended = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ended, "the stream ends");
    wait_unlocked(&h).await;
    h.assert_copy(&newer, Some(2), &newer.touched).await;

    let mut third = newer.clone();
    third.write(0, &[3_u8; 10]);
    h.post(
        stream(&header(5, 3, Some(2)), &third, &[(0, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_complete(3);
    h.assert_copy(&third, Some(3), &third.touched).await;
}

#[tokio::test]
async fn a_stale_stream_refused_while_another_runs_stays_recorded() {
    let h = Harness::new("stale-during").await;
    let first = ship_first(&h).await;
    let mut newer = first.clone();
    newer.write(0, &[1_u8; 10]);
    let mut chunks = stream(&header(5, 2, Some(1)), &newer, &[(0, 10)]);
    let last = chunks.pop().expect("the manifest");
    let mut running = open_stream(h.addr, &chunks).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.post(
        stream(&header(4, 2, Some(1)), &newer, &[(0, 10)]),
        End::Complete,
    )
    .await
    .expect("a reply")
    .assert_refused("stale_token", Some(1));
    let mut out = format!("{:x}\r\n", last.len()).into_bytes();
    out.extend_from_slice(&last);
    out.extend_from_slice(b"\r\n0\r\n\r\n");
    running.write_all(&out).await.expect("write");
    let mut raw = String::new();
    running.read_to_string(&mut raw).await.expect("read");
    assert!(raw.contains("\"complete\""), "{raw}");
    let row = h.row().await;
    assert_eq!(row.complete_seq, Some(2));
    assert_eq!(row.rejected_token, Some(4), "the rejection is kept");
    assert_eq!(h.status().rejected_token, Some(4));
}

/// A small deterministic generator for the state-machine test.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Random cycles of a source against the receiver: complete streams, broken
/// ones, stale ones, failed swaps, lost bases and token raises. After every
/// step the copy on disk is the last complete snapshot the store names.
#[tokio::test]
async fn random_cycles_keep_the_copy_equal_to_the_last_complete_snapshot() {
    for (round, seed) in [0x9E37_79B9_7F4A_7C15_u64, 0xD1B5_4A32_D192_ED03]
        .into_iter()
        .enumerate()
    {
        let h = Harness::new(&format!("random-{round}")).await;
        let mut rng = Rng(seed);
        // The target's complete copy, as the test expects it.
        let mut complete: Option<(u64, Model)> = None;
        // The source: its volume, its base, its snapshot numbers, its token
        // and what changed since its base.
        let mut source = Model::default();
        let mut base: Option<u64> = None;
        let mut seq = 0_u64;
        let mut token = 5_u64;
        let mut since_base: Vec<(u64, u64)> = Vec::new();
        for step in 0..24 {
            for _ in 0..=rng.below(3) {
                let cell = if rng.below(8) == 0 {
                    LAST_CELL
                } else {
                    rng.below(16)
                };
                let offset = cell * CELL + rng.below(CELL);
                // Mostly small writes; now and then one across cells.
                let longest = if rng.below(6) == 0 {
                    2 * CELL
                } else {
                    CELL / 8
                };
                let length = (1 + rng.below(longest)).min(SIZE - offset);
                let len = usize::try_from(length).expect("small");
                let data = if rng.below(3) == 0 {
                    vec![0_u8; len]
                } else {
                    pattern(len, u8::try_from(rng.below(250)).expect("small"))
                };
                source.write(offset, &data);
                since_base.push((offset, length));
            }
            // The source numbers its snapshot one above the target's newest
            // complete copy (section 6.13).
            seq = seq.max(complete.as_ref().map_or(0, |(seq, _)| *seq)) + 1;
            let (head, ranges) = match base {
                Some(base_seq) => (header(token, seq, Some(base_seq)), since_base.clone()),
                None => (header(token, seq, None), source.allocated()),
            };
            let action = rng.below(10);
            // Shown when the test fails.
            eprintln!(
                "round {round} step {step}: action {action}, seq {seq}, base {base:?}, complete {:?}",
                complete.as_ref().map(|(seq, _)| *seq)
            );
            let completed = match action {
                0 | 1 => {
                    let how = [
                        Break::CutBeforeManifest,
                        Break::BadBlockHash,
                        Break::WrongRoot,
                        Break::WrongCount,
                    ][usize::try_from(rng.below(4)).expect("small")];
                    let mut ranges = ranges.clone();
                    ranges.extend([(0, CELL), (SIZE - CELL, CELL)]);
                    let (chunks, end, _) = broken(how, &head, &source, &ranges);
                    let _ = h.post(chunks, end).await;
                    false
                }
                2 => {
                    let stale = StreamHeader {
                        fencing_token: token - 1,
                        ..head.clone()
                    };
                    h.post(stream(&stale, &source, &ranges), End::Complete)
                        .await
                        .expect("a reply")
                        .assert_refused("stale_token", complete.as_ref().map(|(seq, _)| *seq));
                    false
                }
                3 => {
                    h.engine.fail_next(
                        FakeOp::CommitStagedVolume,
                        EngineError::Io("injected".to_owned()),
                    );
                    let reply = h.post(stream(&head, &source, &ranges), End::Complete).await;
                    assert_eq!(
                        reply.expect("a reply").status,
                        500,
                        "seed {seed:x} step {step}"
                    );
                    false
                }
                4 => {
                    token += 1;
                    h.set_warm(|warm| warm.fencing_token = token);
                    let head = StreamHeader {
                        fencing_token: token,
                        ..head.clone()
                    };
                    h.post(stream(&head, &source, &ranges), End::Complete)
                        .await
                        .expect("a reply")
                        .assert_complete(seq);
                    true
                }
                _ => {
                    h.post(stream(&head, &source, &ranges), End::Complete)
                        .await
                        .expect("a reply")
                        .assert_complete(seq);
                    true
                }
            };
            for _ in 0..100 {
                if h.locks.try_lock(VOL).is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if completed {
                complete = Some((seq, source.clone()));
                base = Some(seq);
                since_base.clear();
            } else if rng.below(4) == 0 {
                // The source lost its base to a crash: its next cycle is a
                // full copy.
                base = None;
            }
            match &complete {
                Some((seq, model)) => h.assert_copy(model, Some(*seq), &source.touched).await,
                None => h.assert_copy(&source, None, &source.touched).await,
            }
        }
    }
}
