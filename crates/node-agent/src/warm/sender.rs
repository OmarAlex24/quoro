// SPDX-License-Identifier: AGPL-3.0-only
//! The source side of warm replication (`node-agent-protocol.md` section
//! 6.13, ADR 0173).
//!
//! The node that holds a warm app's lease snapshots the volume every
//! `interval_s` and ships what changed since the newest copy the target
//! confirmed (its **base**) to the target's mesh address. One cycle:
//!
//! 1. Only while this node holds the app's lease in state `held` with the
//!    entry's token (section 8.6); otherwise nothing is snapshotted.
//! 2. The engine takes the snapshot `next` (reflink only, ADR 0172), and the
//!    store records it at once, before anything relies on it (section 12,
//!    engine-trait.md section 3.16).
//! 3. The engine compares `next` with the base (`volume_changed_ranges`);
//!    without a base, or when it cannot compare, every allocated range of
//!    `next` is shipped as a full copy.
//! 4. The ranges go out as blocks and zero ranges, the cell hashes of `next`
//!    are those of the base with the cells the ranges touched hashed again,
//!    and the manifest carries their root.
//! 5. The lease again: before the connection, and before the manifest, so
//!    a node that lost the lease never completes a stream.
//! 6. The answer settles the snapshots the node keeps:
//!    - `complete`: `next` becomes the base, then every other kept snapshot
//!      of the volume is deleted (the engine deletes the file before the
//!      store forgets it);
//!    - `busy`: the target is still applying an earlier stream of this
//!      node, so only `next` goes;
//!    - `root_mismatch`: the base and the target's copy disagree on content,
//!      so every kept snapshot goes and the next cycle ships a full copy;
//!    - any other refusal names the target's copy: the kept snapshot with
//!      that number becomes the base, and every other goes;
//!    - no answer, or a failure on the target: a failed cycle keeps the base
//!      and deletes `next` (section 6.13). If the target did swap `next` in,
//!      its next answer names a number this node no longer keeps, and the
//!      cycle after ships a full copy.
//!
//!    When the new base cannot be recorded after `complete`, `next` stays
//!    recorded as the snapshot in flight, and the next cycle records it as
//!    the base first.
//!
//! A restart keeps the newest base, and the newest snapshot in flight
//! numbered above it, which the target may hold: its next answer names it
//! if it does ([`WarmSender::restore`]).
//!
//! Snapshot numbers (ADR 0210): `next` is numbered one above the target's
//! newest copy this node knows of, and also above every snapshot it keeps,
//! so one number never names two of its snapshots and an answer's number
//! tells them apart. The two differ only while another snapshot is kept
//! above the target's copy (one a restart left in flight, a confirmed one
//! not yet recorded as the base, or one whose delete failed): the next
//! number then skips. A refused number is used again once its snapshot is
//! gone. The target accepts both (section 6.13).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::FileExt as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use quoro_backup::cells::{CELL, CellHashes, hash_cell, zero_cell_hash};
use quoro_engine::{ByteRange, Engine, RangesOf, SnapshotMethod, VolumeId};
use quoro_proto::wire::{Spec, SpecVolumeFile, SpecVolumeState, SpecWarm, WarmRole, WarmStatus};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpSocket;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::frame::{Answer, AnswerStatus, Frame, Manifest, RefuseReason, StreamHeader, encode};
use super::receiver::REPLICATE_PATH;
use super::{StatusBoard, VolumeLocks, data_segments, hash_copy, is_zero};
use crate::LeaseView;
use crate::kept::{self, KeepPurpose};
use crate::reconcile::Snapshot;
use crate::store::{KeptSnapshotRecord, NodeStore};
use crate::util;

/// Bytes in one gibibyte, the unit of `size_gb`.
const GIB: u64 = 1 << 30;
/// The board part this side fills.
const PART: &str = "source";
/// The longest `error` in `status_report.warm` (section 5.3).
const ERROR_CHARS: usize = 500;
/// The bounds of `interval_s` (section 5.11).
const INTERVAL_MIN_S: u32 = 5;
const INTERVAL_MAX_S: u32 = 3_600;
/// Frames the reader may run ahead of the connection.
const CHANNEL_FRAMES: usize = 8;
/// The largest answer read back.
const ANSWER_MAX: usize = 64 << 10;
/// A run of zeros read from the snapshot is sent once it reaches this many
/// bytes read, so the target never waits on a long scan for its next frame.
pub const ZERO_FLUSH_BYTES: u64 = 16 << 20;

/// Timings; tests shorten them.
#[derive(Clone, Debug)]
pub struct SenderOptions {
    /// How long a connection to the target may take.
    pub connect_timeout: Duration,
    /// How long one write to the target may block before the stream is
    /// dropped.
    pub stall_timeout: Duration,
    /// After the manifest, how long the target may take to answer: it
    /// hashes the cells the stream touched, syncs and renames the copy.
    pub answer_timeout: Duration,
    /// How often the desired state is read again for new sources.
    pub pass_interval: Duration,
    /// Replaces every entry's `interval_s`; tests use it.
    pub interval_override: Option<Duration>,
}

impl Default for SenderOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            stall_timeout: Duration::from_secs(30),
            answer_timeout: Duration::from_secs(600),
            pass_interval: Duration::from_secs(5),
            interval_override: None,
        }
    }
}

impl SenderOptions {
    /// Short timings for tests.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            stall_timeout: Duration::from_secs(2),
            answer_timeout: Duration::from_secs(20),
            pass_interval: Duration::from_millis(50),
            interval_override: Some(Duration::from_millis(100)),
        }
    }
}

/// How one cycle ended, for the caller and the tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Nothing was shipped: this node is not the source, or may not ship.
    Skipped(String),
    /// The target holds the snapshot with this number.
    Complete(u64),
    /// The target refused the stream; its newest copy.
    Refused(RefuseReason, Option<u64>),
    /// The cycle failed before or without an answer.
    Failed(String),
}

/// The source side of every warm volume this node runs.
pub struct WarmSender<E: Engine> {
    inner: Arc<Inner<E>>,
}

impl<E: Engine> Clone for WarmSender<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    desired: watch::Receiver<Option<Snapshot>>,
    leases: LeaseView,
    locks: VolumeLocks,
    board: StatusBoard,
    options: SenderOptions,
    /// What each source volume last learned, by volume id.
    states: StdMutex<BTreeMap<String, SourceState>>,
    /// One stream at a time per target address.
    peers: StdMutex<HashMap<IpAddr, Arc<Semaphore>>>,
}

/// What a source volume reports (section 5.3).
#[derive(Clone, Debug, Default)]
struct SourceState {
    /// The target's newest complete copy, from its last answer.
    confirmed: Option<u64>,
    /// When this node took that snapshot, when it is one of its own.
    taken_at: Option<String>,
    /// When the target's confirmation arrived.
    completed_at: Option<String>,
    /// Extents of the newest snapshot taken.
    extent_count: Option<u64>,
    error: Option<String>,
}

/// The snapshots a source volume keeps.
#[derive(Debug, Default)]
struct KeptSet {
    /// The newest copy the target confirmed.
    base: Option<KeptSnapshotRecord>,
    /// A snapshot in flight the target may hold: one a restart cut short,
    /// or one the target confirmed whose move to the base was not recorded.
    candidate: Option<KeptSnapshotRecord>,
    /// Every warm snapshot of the volume the store records.
    all: Vec<KeptSnapshotRecord>,
}

impl KeptSet {
    fn from_records(records: Vec<KeptSnapshotRecord>) -> Self {
        let newest = |purpose: KeepPurpose| {
            records
                .iter()
                .filter(|record| record.purpose == purpose.as_str())
                .max_by_key(|record| record.seq)
                .cloned()
        };
        let base = newest(KeepPurpose::WarmBase);
        let candidate = newest(KeepPurpose::WarmShipping)
            .filter(|candidate| candidate.seq > base.as_ref().and_then(|base| base.seq));
        Self {
            base,
            candidate,
            all: records,
        }
    }

    fn highest_seq(&self) -> Option<u64> {
        self.all.iter().filter_map(|record| record.seq).max()
    }
}

/// What came back from a stream.
enum Shipped {
    /// The target's JSON answer.
    Answer(Answer),
    /// No answer, or a failure on the target (`500`).
    Unanswered(String),
}

/// What the stream's reader built.
struct Produced {
    /// The cell hashes of `next`.
    cells: CellHashes,
    /// The most bytes it read from the snapshot between two frames.
    max_read_between_frames: u64,
}

/// One piece of the request body.
struct Chunk {
    bytes: Vec<u8>,
    /// The manifest, after which the body ends.
    last: bool,
}

impl<E: Engine + 'static> WarmSender<E> {
    /// Builds the sender. `desired` is the verified desired state; the sender
    /// reads its `spec.volumes[].warm` and `spec.mesh`.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        desired: watch::Receiver<Option<Snapshot>>,
        leases: LeaseView,
        locks: VolumeLocks,
        board: StatusBoard,
        options: SenderOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine,
                store,
                desired,
                leases,
                locks,
                board,
                options,
                states: StdMutex::new(BTreeMap::new()),
                peers: StdMutex::new(HashMap::new()),
            }),
        }
    }

    /// Settles what a restart left, then runs one cycle loop per source
    /// volume until `shutdown` resolves.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) {
        let mut shutdown = std::pin::pin!(shutdown);
        self.restore().await;
        let mut desired = self.inner.desired.clone();
        let (stop, stopped) = watch::channel(false);
        let mut loops: HashMap<String, JoinHandle<()>> = HashMap::new();
        let mut tick = tokio::time::interval(self.inner.options.pass_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            self.pass(&mut loops, &stopped).await;
            tokio::select! {
                () = &mut shutdown => break,
                changed = desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = tick.tick() => {}
            }
        }
        let _ = stop.send(true);
        for (_, task) in loops {
            task.abort();
        }
        info!("warm sender stopped");
    }

    /// One cycle of `volume_id` now, under its lock. The cycle loop calls
    /// it every interval; a test calls it directly.
    pub async fn cycle_once(&self, volume_id: &str) -> CycleOutcome {
        let outcome = self.inner.cycle(volume_id).await;
        self.inner.publish();
        outcome
    }

    /// Settles the snapshots a restart left (section 12): per volume, the
    /// newest base stays and older ones go, and so does the newest snapshot
    /// in flight numbered above the base, which the target may hold. A crash
    /// between a `complete` answer and the base's move leaves exactly that:
    /// two bases, or a snapshot in flight the target holds, which the next
    /// answer names.
    pub async fn restore(&self) {
        let volumes = match self.inner.warm_volumes().await {
            Ok(volumes) => volumes,
            Err(e) => {
                warn!(error = %e, "could not read the kept warm snapshots");
                return;
            }
        };
        for volume_id in volumes {
            let _guard = self.inner.locks.lock(&volume_id).await;
            let set = match self.inner.warm_records(&volume_id).await {
                Ok(records) => KeptSet::from_records(records),
                Err(e) => {
                    warn!(volume_id, error = %e, "could not read the kept warm snapshots");
                    continue;
                }
            };
            let keep: BTreeSet<&str> = [&set.base, &set.candidate]
                .into_iter()
                .flatten()
                .map(|record| record.snapshot.snapshot_id.as_str())
                .collect();
            for record in &set.all {
                if !keep.contains(record.snapshot.snapshot_id.as_str()) {
                    self.inner.release(record).await;
                }
            }
            if let Some(base) = &set.base {
                let mut states = self.inner.lock_states();
                let state = states.entry(volume_id.clone()).or_default();
                state.confirmed = base.seq;
                state.taken_at = Some(util::rfc3339(base.snapshot.created_at));
            }
        }
    }

    async fn pass(
        &self,
        loops: &mut HashMap<String, JoinHandle<()>>,
        stopped: &watch::Receiver<bool>,
    ) {
        loops.retain(|_, task| !task.is_finished());
        let spec = self
            .inner
            .desired
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.state.spec.clone());
        let Some(spec) = spec else {
            self.inner.publish();
            return;
        };
        for (file, _) in sources(&spec) {
            if loops.contains_key(&file.volume_id) {
                continue;
            }
            let sender = self.clone();
            let volume_id = file.volume_id.clone();
            let stopped = stopped.clone();
            loops.insert(
                file.volume_id.clone(),
                tokio::spawn(async move { sender.volume_loop(volume_id, stopped).await }),
            );
        }
        // A volume that is deleted, or no longer warm, keeps nothing. A
        // source that became the target keeps its snapshots, and so does a
        // volume the spec no longer names: the return path decides about
        // them (section 9).
        let sourced: BTreeSet<String> = sources(&spec)
            .into_iter()
            .map(|(file, _)| file.volume_id.clone())
            .collect();
        self.inner
            .lock_states()
            .retain(|volume_id, _| sourced.contains(volume_id) || loops.contains_key(volume_id));
        match self.inner.warm_volumes().await {
            Ok(kept) => {
                for file in &spec.volumes {
                    let retired = match file.state {
                        SpecVolumeState::Deleted => true,
                        SpecVolumeState::Present => file.warm.is_none(),
                    };
                    if retired
                        && kept.contains(&file.volume_id)
                        && !loops.contains_key(&file.volume_id)
                    {
                        self.inner.release_volume(&file.volume_id).await;
                    }
                }
            }
            Err(e) => warn!(error = %e, "could not read the kept warm snapshots"),
        }
        self.inner.publish();
    }

    /// Cycles one volume every interval while it is a source here.
    async fn volume_loop(&self, volume_id: String, mut stopped: watch::Receiver<bool>) {
        while let Some(interval) = self.inner.interval_of(&volume_id) {
            let started = Instant::now();
            let outcome = self.cycle_once(&volume_id).await;
            debug!(volume_id, ?outcome, "warm cycle");
            let wait = interval.saturating_sub(started.elapsed());
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                _ = stopped.changed() => break,
            }
        }
    }
}

impl<E: Engine + 'static> Inner<E> {
    fn lock_states(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, SourceState>> {
        self.states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The `present` entry this node is the source of for `volume_id`, and
    /// this node's mesh address.
    fn source_entry(&self, volume_id: &str) -> Option<(SpecVolumeFile, SpecWarm, Option<IpAddr>)> {
        let desired = self.desired.borrow();
        let spec = &desired.as_ref()?.state.spec;
        let mesh = mesh_ip(spec);
        sources(spec)
            .into_iter()
            .find(|(file, _)| file.volume_id == volume_id)
            .map(|(file, warm)| (file.clone(), warm.clone(), mesh))
    }

    fn interval_of(&self, volume_id: &str) -> Option<Duration> {
        let (_, warm, _) = self.source_entry(volume_id)?;
        Some(self.options.interval_override.unwrap_or_else(|| {
            Duration::from_secs(u64::from(
                warm.interval_s.clamp(INTERVAL_MIN_S, INTERVAL_MAX_S),
            ))
        }))
    }

    /// The warm snapshots the store records of one volume.
    async fn warm_records(&self, volume_id: &str) -> Result<Vec<KeptSnapshotRecord>, String> {
        Ok(self
            .store
            .kept_snapshots_of(volume_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|record| is_warm(&record.purpose))
            .collect())
    }

    /// The volumes with a kept warm snapshot, read without the cell hashes.
    async fn warm_volumes(&self) -> Result<BTreeSet<String>, String> {
        Ok(self
            .store
            .kept_snapshot_purposes()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|(_, purpose)| is_warm(purpose))
            .map(|(volume_id, _)| volume_id)
            .collect())
    }

    /// Deletes one kept snapshot, the file before the record (section 12).
    /// A refused delete keeps both and is tried again later.
    async fn release(&self, record: &KeptSnapshotRecord) {
        if let Err(e) = kept::release(&*self.engine, &self.store, &record.snapshot).await {
            warn!(
                volume_id = %record.snapshot.volume_id.0,
                snapshot_id = %record.snapshot.snapshot_id,
                error = %e,
                "could not delete a kept warm snapshot"
            );
        }
    }

    /// Deletes every kept snapshot of a volume that is no longer warm here.
    async fn release_volume(&self, volume_id: &str) {
        let _guard = self.locks.lock(volume_id).await;
        let records = match self.warm_records(volume_id).await {
            Ok(records) => records,
            Err(e) => {
                warn!(volume_id, error = %e, "could not read the kept warm snapshots");
                return;
            }
        };
        for record in &records {
            self.release(record).await;
        }
        debug!(
            volume_id,
            "deleted the kept snapshots of a volume that is no longer warm"
        );
    }

    fn fail(&self, volume_id: &str, error: String) -> CycleOutcome {
        warn!(volume_id, error = %error, "a warm cycle failed");
        self.lock_states()
            .entry(volume_id.to_owned())
            .or_default()
            .error = Some(util::one_line(&error, ERROR_CHARS));
        CycleOutcome::Failed(error)
    }

    /// One cycle (see the module documentation).
    async fn cycle(&self, volume_id: &str) -> CycleOutcome {
        if self.source_entry(volume_id).is_none() {
            return CycleOutcome::Skipped("this node is not the volume's source".to_owned());
        }
        let _guard = self.locks.lock(volume_id).await;
        // Read again under the lock: the desired state may have moved.
        let Some((file, warm, mesh)) = self.source_entry(volume_id) else {
            return CycleOutcome::Skipped("this node is not the volume's source".to_owned());
        };
        let Some(local) = mesh else {
            return self.fail(volume_id, "this node has no mesh address yet".to_owned());
        };
        let Some(peer) = peer_ip(&warm.peer_address) else {
            return self.fail(
                volume_id,
                format!(
                    "the target's address {} is not an IP address",
                    warm.peer_address
                ),
            );
        };
        if !self
            .leases
            .holds_app_lease(&file.app_id, warm.fencing_token)
        {
            return self.fail(
                volume_id,
                format!(
                    "this node does not hold the app's lease with token {}",
                    warm.fencing_token
                ),
            );
        }
        // A node that snapshots by pausing and copying is never a warm source
        // (ADR 0172): it would pause the app every interval for nothing.
        if self.engine.capabilities().await.volume_snapshot_method != Some(SnapshotMethod::Reflink)
        {
            return self.fail(
                volume_id,
                "this node does not snapshot volumes with reflink, so it cannot be a warm source"
                    .to_owned(),
            );
        }
        let mut kept_set = match self.warm_records(volume_id).await {
            Ok(records) => KeptSet::from_records(records),
            Err(e) => return self.fail(volume_id, format!("cannot read the kept snapshots: {e}")),
        };
        let confirmed = self
            .lock_states()
            .get(volume_id)
            .and_then(|state| state.confirmed);
        // The target confirmed the candidate, but its move to the base was
        // not recorded: record it now, then drop the rest.
        if let Some(candidate) = kept_set
            .candidate
            .clone()
            .filter(|candidate| confirmed.is_some() && candidate.seq == confirmed)
        {
            let mut base = candidate;
            base.purpose = KeepPurpose::WarmBase.as_str().to_owned();
            if let Err(e) = self.store.save_kept_snapshot(&base).await {
                return self.fail(volume_id, format!("cannot record the new base: {e}"));
            }
            for record in &kept_set.all {
                if record.snapshot.snapshot_id != base.snapshot.snapshot_id {
                    self.release(record).await;
                }
            }
            kept_set = KeptSet::from_records(vec![base]);
        }
        let seq = confirmed.max(kept_set.highest_seq()).unwrap_or(0) + 1;
        let size = u64::from(file.size_gb) * GIB;
        let id = VolumeId(volume_id.to_owned());

        // Take `next` and record it before anything relies on it.
        let next = match self.engine.snapshot_volume(&id).await {
            Ok(next) => next,
            Err(e) => return self.fail(volume_id, format!("cannot snapshot the volume: {e}")),
        };
        if next.method != SnapshotMethod::Reflink || next.size_bytes != size {
            let reason = if next.method == SnapshotMethod::Reflink {
                format!("the volume is {} bytes, not {size}", next.size_bytes)
            } else {
                "the engine took a snapshot by pausing and copying, so it cannot be shipped"
                    .to_owned()
            };
            if let Err(e) = self.engine.delete_volume_snapshot(&next).await {
                warn!(volume_id, error = %e, "could not delete a snapshot it cannot ship");
            }
            return self.fail(volume_id, reason);
        }
        if let Err(e) = kept::keep(
            &self.store,
            &file.app_id,
            &next,
            KeepPurpose::WarmShipping,
            Some(seq),
            None,
        )
        .await
        {
            if let Err(de) = self.engine.delete_volume_snapshot(&next).await {
                warn!(volume_id, error = %de, "could not delete an unrecorded snapshot");
            }
            return self.fail(volume_id, format!("cannot record the snapshot: {e}"));
        }
        let next_record = KeptSnapshotRecord {
            app_id: file.app_id.clone(),
            snapshot: next.clone(),
            purpose: KeepPurpose::WarmShipping.as_str().to_owned(),
            seq: Some(seq),
            cells: None,
        };

        // What changed since the base, or everything.
        let newer = RangesOf::Snapshot(next.clone());
        let base = kept_set.base.clone();
        let mut compared = match &base {
            Some(base) => self
                .engine
                .volume_changed_ranges(&newer, Some(&base.snapshot))
                .await
                .map_err(|e| {
                    warn!(volume_id, error = %e, "cannot compare with the base; shipping a full copy");
                })
                .ok(),
            None => None,
        };
        if compared.is_none() {
            compared = match self.engine.volume_changed_ranges(&newer, None).await {
                Ok(ranges) => Some(ranges),
                Err(e) => {
                    self.release(&next_record).await;
                    return self.fail(volume_id, format!("cannot list the snapshot's ranges: {e}"));
                }
            };
        }
        let Some(changed) = compared else {
            self.release(&next_record).await;
            return self.fail(volume_id, "no ranges to ship".to_owned());
        };
        let incremental_from = base.as_ref().filter(|_| !changed.full);
        let base_cells = match incremental_from {
            Some(base) => match self.base_cells(base, size).await {
                Ok(cells) => Some(cells),
                Err(e) => {
                    self.release(&next_record).await;
                    return self.fail(volume_id, format!("cannot hash the base: {e}"));
                }
            },
            None => None,
        };
        self.lock_states()
            .entry(volume_id.to_owned())
            .or_default()
            .extent_count = Some(changed.extent_count);
        let header = StreamHeader {
            app_id: file.app_id.clone(),
            volume_id: volume_id.to_owned(),
            lease_id: warm.lease_id.clone(),
            fencing_token: warm.fencing_token,
            snapshot_seq: seq,
            base_seq: incremental_from.and_then(|base| base.seq),
            taken_at: util::rfc3339(next.created_at),
            size_bytes: size,
        };

        // One stream at a time per target, then the lease again, right
        // before the write leaves the node.
        let permit = self.peer_permit(peer).acquire_owned().await;
        if !self
            .leases
            .holds_app_lease(&file.app_id, warm.fencing_token)
        {
            drop(permit);
            self.release(&next_record).await;
            return self.fail(
                volume_id,
                format!(
                    "this node no longer holds the app's lease with token {}",
                    warm.fencing_token
                ),
            );
        }
        let target = SocketAddr::new(peer, warm.port);
        let still_held = || {
            self.leases
                .holds_app_lease(&file.app_id, warm.fencing_token)
        };
        let (shipped, cells) = self
            .ship(
                local,
                target,
                &header,
                next.location.clone(),
                changed.ranges,
                base_cells,
                &still_held,
            )
            .await;
        drop(permit);
        self.settle(volume_id, &header, &next_record, &kept_set, shipped, cells)
            .await
    }

    /// The cell hashes of the base: recorded, or read once from the base
    /// snapshot and recorded.
    async fn base_cells(&self, base: &KeptSnapshotRecord, size: u64) -> Result<CellHashes, String> {
        if let Some(cells) = base
            .cells
            .as_deref()
            .and_then(|bytes| CellHashes::from_bytes(bytes, size).ok())
        {
            return Ok(cells);
        }
        let path = base.snapshot.location.clone();
        let cells = tokio::task::spawn_blocking(move || hash_copy(&File::open(&path)?, size))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let mut record = base.clone();
        record.cells = Some(cells.to_bytes());
        if let Err(e) = self.store.save_kept_snapshot(&record).await {
            warn!(snapshot_id = %base.snapshot.snapshot_id, error = %e, "could not record the base's cell hashes");
        }
        Ok(cells)
    }

    fn peer_permit(&self, peer: IpAddr) -> Arc<Semaphore> {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            peers
                .entry(peer)
                .or_insert_with(|| Arc::new(Semaphore::new(1))),
        )
    }

    /// Settles the kept snapshots by the stream's outcome (see the module
    /// documentation).
    async fn settle(
        &self,
        volume_id: &str,
        header: &StreamHeader,
        next: &KeptSnapshotRecord,
        kept_set: &KeptSet,
        shipped: Shipped,
        cells: Option<CellHashes>,
    ) -> CycleOutcome {
        let seq = header.snapshot_seq;
        let answer = match shipped {
            Shipped::Answer(answer) => answer,
            // A failed cycle keeps the base and deletes `next` (section 6.13),
            // even when the target may have swapped it in: its next answer
            // names that copy, and with no snapshot of that number here the
            // cycle after ships a full copy.
            Shipped::Unanswered(error) => {
                self.release(next).await;
                return self.fail(volume_id, error);
            }
        };

        if answer.status == AnswerStatus::Complete && answer.snapshot_seq == Some(seq) {
            let mut base = next.clone();
            base.purpose = KeepPurpose::WarmBase.as_str().to_owned();
            base.cells = cells.as_ref().map(CellHashes::to_bytes);
            match self.store.save_kept_snapshot(&base).await {
                Ok(()) => {
                    for record in &kept_set.all {
                        self.release(record).await;
                    }
                }
                // `next` stays recorded as the candidate, and the next cycle
                // records it as the base.
                Err(e) => warn!(volume_id, error = %e, "could not record the new base"),
            }
            let mut states = self.lock_states();
            let state = states.entry(volume_id.to_owned()).or_default();
            state.confirmed = Some(seq);
            state.taken_at = Some(header.taken_at.clone());
            state.completed_at = Some(util::now_rfc3339());
            state.error = None;
            info!(
                volume_id,
                snapshot_seq = seq,
                "the target holds a new warm copy"
            );
            return CycleOutcome::Complete(seq);
        }

        let Some(reason) = answer
            .reason
            .filter(|_| answer.status == AnswerStatus::Refused)
        else {
            // `complete` for another number: not an answer this protocol
            // gives. A failed cycle.
            self.release(next).await;
            return self.fail(
                volume_id,
                format!(
                    "the target answered complete for snapshot {:?}, not {seq}",
                    answer.snapshot_seq
                ),
            );
        };
        let held = answer.snapshot_seq;
        let error = format!(
            "the target refused snapshot {seq}: {} (it holds {})",
            reason.as_str(),
            held.map_or_else(|| "no copy".to_owned(), |held| format!("snapshot {held}"))
        );
        match reason {
            // An earlier stream of this node is still being applied: what
            // the target holds is about to change, so nothing moves.
            RefuseReason::Busy => {
                self.release(next).await;
            }
            // The base and the target's copy disagree on content: shipping
            // from the base again would fail the same way, so start over
            // with a full copy.
            RefuseReason::RootMismatch => {
                self.release(next).await;
                for record in &kept_set.all {
                    self.release(record).await;
                }
                self.learn(volume_id, held, None);
            }
            // The answer names the target's copy, which the stream did not
            // change (`bad_frame` included): rebase on it if this node keeps
            // it, otherwise the next cycle ships a full copy.
            _ => {
                self.release(next).await;
                let named = kept_set
                    .all
                    .iter()
                    .find(|record| held.is_some() && record.seq == held)
                    .cloned();
                let mut rebased = None;
                if let Some(named) = named {
                    if named.purpose == KeepPurpose::WarmBase.as_str() {
                        // Already the base: saving the record read at the
                        // start of the cycle would drop cell hashes recorded
                        // since.
                        rebased = Some(named);
                    } else {
                        let mut base = named;
                        base.purpose = KeepPurpose::WarmBase.as_str().to_owned();
                        match self.store.save_kept_snapshot(&base).await {
                            Ok(()) => rebased = Some(base),
                            Err(e) => warn!(volume_id, error = %e, "could not record the new base"),
                        }
                    }
                }
                for record in &kept_set.all {
                    let keep = rebased.as_ref().is_some_and(|base| {
                        base.snapshot.snapshot_id == record.snapshot.snapshot_id
                    });
                    if !keep {
                        self.release(record).await;
                    }
                }
                self.learn(volume_id, held, rebased.as_ref());
            }
        }
        self.fail(volume_id, error);
        CycleOutcome::Refused(reason, held)
    }

    /// Records the target's copy an answer named.
    fn learn(&self, volume_id: &str, held: Option<u64>, base: Option<&KeptSnapshotRecord>) {
        let mut states = self.lock_states();
        let state = states.entry(volume_id.to_owned()).or_default();
        if state.confirmed != held {
            state.completed_at = None;
        }
        state.confirmed = held;
        state.taken_at = base.map(|base| util::rfc3339(base.snapshot.created_at));
    }

    /// Streams `next` to the target and returns what came back, with the
    /// cell hashes of `next` when the whole stream was built.
    #[allow(clippy::too_many_arguments)]
    async fn ship(
        &self,
        local: IpAddr,
        target: SocketAddr,
        header: &StreamHeader,
        location: PathBuf,
        ranges: Vec<ByteRange>,
        base_cells: Option<CellHashes>,
        still_held: &(dyn Fn() -> bool + Sync),
    ) -> (Shipped, Option<CellHashes>) {
        let (tx, rx) = mpsc::channel::<Chunk>(CHANNEL_FRAMES);
        let stream_header = header.clone();
        let producer = tokio::task::spawn_blocking(move || {
            produce(&location, &stream_header, &ranges, base_cells, &tx)
        });
        let shipped = self.post(local, target, rx, still_held).await;
        let produced = producer
            .await
            .map_err(|e| format!("the stream's reader failed: {e}"))
            .and_then(|produced| produced);
        match (shipped, produced) {
            (Some(shipped), Ok(produced)) => {
                debug!(
                    volume_id = %header.volume_id,
                    snapshot_seq = header.snapshot_seq,
                    max_read_between_frames = produced.max_read_between_frames,
                    "warm stream built"
                );
                (shipped, Some(produced.cells))
            }
            (Some(shipped), Err(_)) => (shipped, None),
            // The reader stopped before the manifest: its error is the cause.
            (None, Err(error)) => (Shipped::Unanswered(error), None),
            (None, Ok(_)) => (
                Shipped::Unanswered("the stream ended before its manifest".to_owned()),
                None,
            ),
        }
    }

    /// One `POST /v1/replicate` with the chunks of `rx` as its chunked body,
    /// from this node's mesh address. The last chunk, the manifest, goes out
    /// only while `still_held` answers true. Returns `None` when the reader
    /// ended the body early (its own error says why). A stream that does not
    /// end with its manifest is dropped without the last chunk, so the
    /// target drops it too.
    async fn post(
        &self,
        local: IpAddr,
        target: SocketAddr,
        mut rx: mpsc::Receiver<Chunk>,
        still_held: &(dyn Fn() -> bool + Sync),
    ) -> Option<Shipped> {
        let unanswered = |error: String| Some(Shipped::Unanswered(error));
        let socket = match if target.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        } {
            Ok(socket) => socket,
            Err(e) => return unanswered(format!("cannot open a socket: {e}")),
        };
        if let Err(e) = socket.bind(SocketAddr::new(local, 0)) {
            return unanswered(format!("cannot use the mesh address {local}: {e}"));
        }
        let stream = match tokio::time::timeout(
            self.options.connect_timeout,
            socket.connect(target),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                return unanswered(format!("cannot reach the target at {target}: {e}"));
            }
            Err(_) => {
                return unanswered(format!(
                    "the target at {target} did not accept the connection"
                ));
            }
        };
        let (read, mut write) = stream.into_split();
        let (answer_tx, mut answer_rx) = oneshot::channel();
        let reader = tokio::spawn(async move {
            let _ = answer_tx.send(read_answer(read).await);
        });
        let stall = self.options.stall_timeout;
        let head = format!(
            "POST {REPLICATE_PATH} HTTP/1.1\r\nhost: {target}\r\ncontent-type: application/octet-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n"
        );
        let mut early = None;
        let wrote = 'body: {
            if let Err(e) = write_with(stall, &mut write, head.as_bytes()).await {
                break 'body Err(e);
            }
            loop {
                tokio::select! {
                    biased;
                    answer = &mut answer_rx => {
                        // The target answered before the body ended: a
                        // refusal. Stop writing.
                        early = Some(answer);
                        break 'body Ok(());
                    }
                    chunk = rx.recv() => {
                        let Some(chunk) = chunk else {
                            reader.abort();
                            return None;
                        };
                        if chunk.last && !still_held() {
                            // The lease went while the stream was being
                            // sent: the target never gets the manifest.
                            reader.abort();
                            return unanswered("this node lost the app's lease before the manifest".to_owned(),
                            );
                        }
                        let mut out = format!("{:x}\r\n", chunk.bytes.len()).into_bytes();
                        out.extend_from_slice(&chunk.bytes);
                        out.extend_from_slice(b"\r\n");
                        if chunk.last {
                            out.extend_from_slice(b"0\r\n\r\n");
                        }
                        if let Err(e) = write_with(stall, &mut write, &out).await {
                            break 'body Err(e);
                        }
                        if chunk.last {
                            break 'body Ok(());
                        }
                    }
                }
            }
        };
        // Whatever the reader still has is not sent.
        drop(rx);
        let answer = match early {
            Some(answer) => answer.unwrap_or_else(|_| Err("the answer was lost".to_owned())),
            None => {
                if let Err(error) = wrote {
                    // The target may have answered before the write failed.
                    match tokio::time::timeout(Duration::from_millis(200), &mut answer_rx).await {
                        Ok(Ok(Ok(answer))) => Ok(answer),
                        _ => {
                            reader.abort();
                            return unanswered(error);
                        }
                    }
                } else {
                    match tokio::time::timeout(self.options.answer_timeout, &mut answer_rx).await {
                        Ok(Ok(answer)) => answer,
                        Ok(Err(_)) => Err("the answer was lost".to_owned()),
                        Err(_) => {
                            reader.abort();
                            return unanswered(format!(
                                "the target did not answer within {:?}",
                                self.options.answer_timeout
                            ));
                        }
                    }
                }
            }
        };
        drop(write);
        match answer {
            Ok((200, body)) => match serde_json::from_slice::<Answer>(&body) {
                Ok(answer) => Some(Shipped::Answer(answer)),
                Err(e) => unanswered(format!("the target's answer is not valid: {e}")),
            },
            Ok((status, body)) => unanswered(format!(
                "the target failed ({status}): {}",
                util::one_line(&String::from_utf8_lossy(&body), 300)
            )),
            Err(error) => unanswered(error),
        }
    }

    /// Publishes one `status_report.warm` entry per volume this node is the
    /// source of.
    fn publish(&self) {
        let spec = self
            .desired
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.state.spec.clone());
        let Some(spec) = spec else {
            self.board.set(PART, Vec::new());
            return;
        };
        let states = self.lock_states().clone();
        let entries = sources(&spec)
            .into_iter()
            .map(|(file, _)| {
                let state = states.get(&file.volume_id).cloned().unwrap_or_default();
                WarmStatus {
                    volume_id: file.volume_id.clone(),
                    app_id: file.app_id.clone(),
                    role: WarmRole::Source,
                    snapshot_seq: state.confirmed,
                    snapshot_taken_at: state.confirmed.and(state.taken_at),
                    completed_at: state.confirmed.and(state.completed_at),
                    allocated_bytes: None,
                    extent_count: state.extent_count,
                    frozen: None,
                    rejected_token: None,
                    rejected_at: None,
                    error: state.error,
                }
            })
            .collect();
        self.board.set(PART, entries);
    }
}

/// Whether a kept snapshot's purpose is one of the warm sender's.
fn is_warm(purpose: &str) -> bool {
    purpose == KeepPurpose::WarmBase.as_str() || purpose == KeepPurpose::WarmShipping.as_str()
}

/// The `present` entries of `spec.volumes` this node is the source of.
pub fn sources(spec: &Spec) -> Vec<(&SpecVolumeFile, &SpecWarm)> {
    spec.volumes
        .iter()
        .filter(|file| file.state == SpecVolumeState::Present)
        .filter_map(|file| {
            file.warm
                .as_ref()
                .filter(|warm| warm.role == WarmRole::Source)
                .map(|warm| (file, warm))
        })
        .collect()
}

fn mesh_ip(spec: &Spec) -> Option<IpAddr> {
    spec.mesh.as_ref().and_then(|mesh| peer_ip(&mesh.address))
}

fn peer_ip(address: &str) -> Option<IpAddr> {
    address.split('/').next()?.parse().ok()
}

async fn write_with(
    stall: Duration,
    write: &mut tokio::net::tcp::OwnedWriteHalf,
    bytes: &[u8],
) -> Result<(), String> {
    match tokio::time::timeout(stall, write.write_all(bytes)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("the connection to the target broke: {e}")),
        Err(_) => Err(format!("the target read nothing for {stall:?}")),
    }
}

/// Reads one HTTP/1.1 answer: its status code and body, as far as its
/// `content-length` says, or to the end of the connection.
async fn read_answer(mut read: OwnedReadHalf) -> Result<(u16, Vec<u8>), String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let head_end = buf.windows(4).position(|window| window == b"\r\n\r\n");
        if let Some(end) = head_end {
            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
            let status: u16 = head
                .split_whitespace()
                .nth(1)
                .and_then(|code| code.parse().ok())
                .ok_or_else(|| "the target's answer has no status".to_owned())?;
            let length = head.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            });
            let body_start = end + 4;
            loop {
                let have = buf.len() - body_start;
                if length.is_some_and(|length| have >= length) {
                    let length = length.unwrap_or(have);
                    return Ok((status, buf[body_start..body_start + length].to_vec()));
                }
                let n = read
                    .read(&mut chunk)
                    .await
                    .map_err(|e| format!("cannot read the target's answer: {e}"))?;
                if n == 0 {
                    return Ok((status, buf[body_start..].to_vec()));
                }
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > ANSWER_MAX {
                    return Err("the target's answer is too long".to_owned());
                }
            }
        }
        let n = read
            .read(&mut chunk)
            .await
            .map_err(|e| format!("cannot read the target's answer: {e}"))?;
        if n == 0 {
            return Err("the target closed the connection without an answer".to_owned());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > ANSWER_MAX {
            return Err("the target's answer is too long".to_owned());
        }
    }
}

/// Sorted, merged and cut at `size`.
fn normalize(ranges: &[ByteRange], size: u64) -> Vec<(u64, u64)> {
    let mut spans: Vec<(u64, u64)> = ranges
        .iter()
        .filter_map(|range| {
            let start = range.offset.min(size);
            let end = range.offset.saturating_add(range.length).min(size);
            (start < end).then_some((start, end))
        })
        .collect();
    spans.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in spans {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// Builds the stream of `ranges` of the snapshot at `location` and hands
/// its frames to `tx`: the header, blocks inside one cell each, runs of
/// zeros as zero ranges, and the manifest with the root of the cell hashes
/// (those of the base, or zeros for a full copy, with every cell a range
/// touched hashed again). Returns those cell hashes. Blocking IO.
///
/// The target drops a stream that sends nothing for a while, so the reads
/// between two frames stay short: a run of zeros read from the file is sent
/// every [`ZERO_FLUSH_BYTES`], and a cell a range covers in part is hashed
/// again as soon as the stream has passed it, not all at the end.
fn produce(
    location: &std::path::Path,
    header: &StreamHeader,
    ranges: &[ByteRange],
    base_cells: Option<CellHashes>,
    tx: &mpsc::Sender<Chunk>,
) -> Result<Produced, String> {
    let size = header.size_bytes;
    let file = File::open(location).map_err(|e| format!("cannot open the snapshot: {e}"))?;
    let cells = match base_cells {
        Some(cells) => cells,
        None => CellHashes::zeroed(size).map_err(|e| e.to_string())?,
    };
    let mut stream = Stream {
        file: &file,
        tx,
        cells,
        dirty: BTreeSet::new(),
        zeros: None,
        count: 0,
        read_since_frame: 0,
        max_read_between_frames: 0,
    };
    stream.send(&Frame::Header(header.clone()), false)?;
    let mut buf = vec![0_u8; usize::try_from(CELL).map_err(|e| e.to_string())?];
    for (start, end) in normalize(ranges, size) {
        let segments = data_segments(&file, start, end)
            .map_err(|e| format!("cannot read the snapshot: {e}"))?;
        // Holes between the data read as zeros.
        let mut pieces: Vec<(u64, u64, bool)> = Vec::new();
        let mut at = start;
        for (data_start, data_end) in segments {
            if at < data_start {
                pieces.push((at, data_start, false));
            }
            pieces.push((data_start, data_end, true));
            at = data_end;
        }
        if at < end {
            pieces.push((at, end, false));
        }
        for (piece_start, piece_end, data) in pieces {
            if !data {
                stream.rehash_before(piece_start / CELL)?;
                stream.zeros(piece_start, piece_end)?;
                continue;
            }
            let mut offset = piece_start;
            while offset < piece_end {
                stream.rehash_before(offset / CELL)?;
                let stop = piece_end.min((offset / CELL + 1) * CELL);
                let length = usize::try_from(stop - offset).map_err(|e| e.to_string())?;
                let bytes = &mut buf[..length];
                file.read_exact_at(bytes, offset)
                    .map_err(|e| format!("cannot read the snapshot: {e}"))?;
                stream.read_since_frame += stop - offset;
                if is_zero(bytes) {
                    stream.zeros(offset, stop)?;
                } else {
                    stream.flush_zeros()?;
                    let blake3 = hash_cell(bytes);
                    if offset.is_multiple_of(CELL) && stop - offset == CELL {
                        stream
                            .cells
                            .set(offset / CELL, blake3)
                            .map_err(|e| e.to_string())?;
                    } else {
                        stream.dirty.insert(offset / CELL);
                    }
                    stream.send(
                        &Frame::Block {
                            offset,
                            blake3,
                            data: bytes.to_vec(),
                        },
                        false,
                    )?;
                    stream.count += 1;
                }
                offset = stop;
            }
        }
    }
    stream.flush_zeros()?;
    stream.rehash_before(u64::MAX)?;
    let manifest = Manifest {
        block_count: stream.count,
        root_hash: stream.cells.root_hex(),
    };
    stream.send(&Frame::Manifest(manifest), true)?;
    Ok(Produced {
        cells: stream.cells,
        max_read_between_frames: stream.max_read_between_frames,
    })
}

/// What [`produce`] keeps while it builds a stream.
struct Stream<'a> {
    file: &'a File,
    tx: &'a mpsc::Sender<Chunk>,
    cells: CellHashes,
    /// Cells a range covers in part, hashed again once the stream is past
    /// them.
    dirty: BTreeSet<u64>,
    /// A zero range waiting for the next piece, which may extend it.
    zeros: Option<(u64, u64)>,
    /// Blocks and zero ranges sent.
    count: u64,
    read_since_frame: u64,
    max_read_between_frames: u64,
}

impl Stream<'_> {
    fn send(&mut self, frame: &Frame, last: bool) -> Result<(), String> {
        self.tx
            .blocking_send(Chunk {
                bytes: encode(frame),
                last,
            })
            .map_err(|_| "the stream was closed".to_owned())?;
        self.max_read_between_frames = self.max_read_between_frames.max(self.read_since_frame);
        self.read_since_frame = 0;
        Ok(())
    }

    fn flush_zeros(&mut self) -> Result<(), String> {
        if let Some((offset, end)) = self.zeros.take() {
            self.send(
                &Frame::Zero {
                    offset,
                    length: end - offset,
                },
                false,
            )?;
            self.count += 1;
        }
        Ok(())
    }

    /// A piece that reads as zeros: it extends the waiting zero range when
    /// it follows it, and the range goes out once enough was read for it.
    fn zeros(&mut self, start: u64, end: u64) -> Result<(), String> {
        note_zeros(&mut self.cells, &mut self.dirty, start, end)?;
        match self.zeros {
            Some((zero_start, zero_end)) if zero_end == start => {
                self.zeros = Some((zero_start, end));
            }
            _ => {
                self.flush_zeros()?;
                self.zeros = Some((start, end));
            }
        }
        if self.read_since_frame >= ZERO_FLUSH_BYTES {
            self.flush_zeros()?;
        }
        Ok(())
    }

    /// Hashes again the cells below `cell` that a range covered in part: the
    /// ranges are sorted, so nothing later reaches them. Stops once the
    /// connection is gone.
    fn rehash_before(&mut self, cell: u64) -> Result<(), String> {
        if self.tx.is_closed() {
            return Err("the stream was closed".to_owned());
        }
        let later = self.dirty.split_off(&cell);
        let passed = std::mem::replace(&mut self.dirty, later);
        if passed.is_empty() {
            return Ok(());
        }
        self.read_since_frame += passed.len() as u64 * CELL;
        let mut reader = self.file;
        self.cells
            .rehash(&mut reader, passed)
            .map_err(|e| format!("cannot hash the snapshot: {e}"))
    }
}

/// The cells of a range of zeros: those it covers whole hash as zeros, the
/// ones at its ends are hashed again from the file.
fn note_zeros(
    cells: &mut CellHashes,
    dirty: &mut BTreeSet<u64>,
    start: u64,
    end: u64,
) -> Result<(), String> {
    for cell in start.div_ceil(CELL)..end / CELL {
        cells
            .set(cell, zero_cell_hash())
            .map_err(|e| e.to_string())?;
    }
    if !start.is_multiple_of(CELL) {
        dirty.insert(start / CELL);
    }
    if !end.is_multiple_of(CELL) {
        dirty.insert(end / CELL);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
