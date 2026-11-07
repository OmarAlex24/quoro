// SPDX-License-Identifier: AGPL-3.0-only
//! The target side of warm replication (`node-agent-protocol.md` section
//! 6.13, ADR 0173).
//!
//! The warm node listens on its own mesh address, on the port of each
//! `spec.volumes[].warm` entry it is the target of, and takes one stream per
//! `POST /v1/replicate`. Before it writes anything it checks the stream:
//!
//! - it is the target of the volume, for the same app, and the connection
//!   comes from the entry's `peer_address` (`not_target`);
//! - the stream names the entry's lease (`wrong_lease`);
//! - its token is at least the entry's token and the highest token it has
//!   accepted for the volume (`stale_token`, recorded and reported);
//! - no other stream of the volume is open and no copy of it is being
//!   resolved (`busy`, below);
//! - `base_seq` is null or names its complete copy (`base_mismatch`);
//! - `snapshot_seq` is above `base_seq`, and for a full copy above its
//!   complete copy (`not_newer`).
//!
//! Then it writes the stream into a staging file the engine made (an empty
//! sparse file, or a reflink clone of its complete copy), checks each block's
//! hash, updates the cell hashes of the cells the stream touched, and
//! compares the root hash at the manifest (`bad_frame`, `root_mismatch`).
//! Only a stream that passed every check is swapped in, in one rename by the
//! engine, so its copy is always one complete snapshot of the source.
//!
//! **Before the swap** it checks again that it is still the target with the
//! same lease and that no higher token arrived, because a stream can outlive
//! the desired state it started under. Then it writes the copy it is about
//! to swap in to its store (`pending`), swaps, and records the new copy. A
//! crash or an error between the swap and that record leaves the store
//! naming the copy it may hold. Resolving it hashes the volume file once and
//! keeps whichever copy it matches; a copy that matches neither is treated
//! as no copy, and the source ships a full one. The hash reads the whole
//! volume, which can take longer than a source waits, so it never runs
//! while a stream or a pass waits for it. It runs in a task that holds the
//! volume's lock, and a stream that arrives meanwhile is refused `busy`:
//! - after a failed swap, once the source has its answer;
//! - when a stream finds the copy pending, which is refused `busy` too;
//! - at start-up, while the listeners bind and the streams of other volumes
//!   are served.
//!
//! The copy is never created except from a complete stream, and it is
//! reported in `status_report.warm`, never in `status_report.volumes`. A
//! copy the node is no longer the target of stays reported until it is
//! removed, so the primary can always send its `deleted` entry (ADR 0267).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::FileExt as _;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt as _, Full};
use hyper::body::{Body, Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use quoro_backup::cells::{CELL, CellHashes, hash_cell, zero_cell_hash};
use quoro_engine::{Engine, EngineError, StageFrom, StagedVolume, VolumeId};
use quoro_proto::wire::{
    Spec, SpecVolumeFile, SpecVolumeState, SpecWarm, VolumeState, WarmRole, WarmStatus,
};
use tokio::net::TcpListener;
use tokio::sync::{OwnedMutexGuard, Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::frame::{Answer, AnswerStatus, Decoder, Frame, RefuseReason, Sequence, StreamHeader};
use super::{StatusBoard, VolumeLocks};
use crate::reconcile::Snapshot;
use crate::store::{NodeStore, PendingCommit, WarmTargetRecord};
use crate::util;

/// The path a stream is posted to (section 6.13).
pub const REPLICATE_PATH: &str = "/v1/replicate";
/// Bytes in one gibibyte, the unit of `size_gb`.
const GIB: u64 = 1 << 30;
/// Connections one listener serves at once. The source opens one per
/// volume and cycle; the bound only stops a peer from opening thousands.
const MAX_CONNECTIONS: usize = 16;
/// The board part this side fills.
const PART: &str = "target";
/// The longest `error` in `status_report.warm` (section 5.3).
const ERROR_CHARS: usize = 500;
/// How the error of a copy that could not be resolved starts.
const UNRESOLVED: &str = "could not resolve the copy left pending";

/// Timings; tests shorten them.
#[derive(Clone, Debug)]
pub struct ReceiverOptions {
    /// How long a stream may send nothing before it is dropped.
    pub frame_timeout: Duration,
    /// After a refusal, how long the rest of the body is read and thrown
    /// away, so the answer reaches the source before the connection closes.
    pub drain_timeout: Duration,
    /// After a refusal, how many bytes of the rest are read at most.
    pub drain_bytes: u64,
    /// How often a listener that could not bind is tried again.
    pub bind_retry: Duration,
}

impl Default for ReceiverOptions {
    fn default() -> Self {
        Self {
            frame_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(5),
            drain_bytes: 64 << 20,
            bind_retry: Duration::from_secs(5),
        }
    }
}

impl ReceiverOptions {
    /// Short timings for tests.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            frame_timeout: Duration::from_millis(500),
            drain_timeout: Duration::from_secs(2),
            drain_bytes: 64 << 20,
            bind_retry: Duration::from_millis(50),
        }
    }
}

/// The target side of every warm volume this node holds a copy of.
pub struct WarmReceiver<E: Engine> {
    inner: Arc<Inner<E>>,
}

impl<E: Engine> Clone for WarmReceiver<E> {
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
    locks: VolumeLocks,
    board: StatusBoard,
    options: ReceiverOptions,
    /// Listeners that could not bind, with why.
    bind_errors: StdMutex<BTreeMap<SocketAddr, String>>,
    connections: Arc<Semaphore>,
    /// Staging files a failed discard or a panic left, by volume id. The
    /// next staging of the volume removes its leftover first, under the
    /// volume's lock; without that, the engine refuses every new staging
    /// file of the volume until the agent restarts.
    leftovers: StdMutex<HashMap<String, StagedVolume>>,
    /// Held from reading the rows to setting the entries, so a publish that
    /// read older rows never overwrites a newer one's entries.
    publishing: tokio::sync::Mutex<()>,
    /// Volumes whose copy a task removes once their lock is free.
    deleting: StdMutex<BTreeSet<String>>,
    /// Held by a test to keep a pending copy's resolution from starting.
    #[cfg(test)]
    recovery_gate: tokio::sync::Mutex<()>,
    /// Notified once the start-up has listed the pending copies.
    #[cfg(test)]
    pending_listed: tokio::sync::Notify,
}

/// A staging file of a stream or a recovery. Dropped before it is committed
/// or discarded, it is remembered as a leftover.
struct Staging<'a, E: Engine> {
    inner: &'a Inner<E>,
    staged: Option<StagedVolume>,
}

impl<E: Engine> Staging<'_, E> {
    fn get(&self) -> &StagedVolume {
        self.staged
            .as_ref()
            .expect("a staging file is present until it is committed or discarded")
    }

    /// The engine swapped the file in; there is nothing left to remove.
    fn committed(mut self) {
        self.staged = None;
    }
}

impl<E: Engine + 'static> Staging<'_, E> {
    async fn discard(mut self) {
        if let Some(staged) = self.staged.take() {
            self.inner.discard(&staged).await;
        }
    }
}

impl<E: Engine> Drop for Staging<'_, E> {
    fn drop(&mut self) {
        if let Some(staged) = self.staged.take() {
            self.inner
                .leftovers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(staged.volume_id.0.clone(), staged);
        }
    }
}

/// What a stream gets back.
enum Outcome {
    /// The JSON answer of section 6.13.
    Answer(Answer),
    /// A failure on this node, not a check: answered `500` with one line,
    /// so the source counts a failed cycle.
    Failed(String),
}

/// Why a stream was not accepted.
enum Reject {
    Refuse(RefuseReason, Option<u64>),
    Failed(String),
}

/// A stream that passed every check and holds its volume's lock.
struct Accepted {
    _guard: OwnedMutexGuard<()>,
    record: WarmTargetRecord,
    size_gb: u32,
}

/// Why writing a stream stopped.
enum StreamFailure {
    Refuse(RefuseReason, String),
    Failed(String),
}

impl<E: Engine + 'static> WarmReceiver<E> {
    /// Builds the receiver. `desired` is the verified desired state; the
    /// receiver reads its `spec.volumes[].warm` and `spec.mesh`.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        desired: watch::Receiver<Option<Snapshot>>,
        locks: VolumeLocks,
        board: StatusBoard,
        options: ReceiverOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine,
                store,
                desired,
                locks,
                board,
                options,
                bind_errors: StdMutex::new(BTreeMap::new()),
                connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
                leftovers: StdMutex::new(HashMap::new()),
                publishing: tokio::sync::Mutex::new(()),
                deleting: StdMutex::new(BTreeSet::new()),
                #[cfg(test)]
                recovery_gate: tokio::sync::Mutex::new(()),
                #[cfg(test)]
                pending_listed: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Keeps the listeners, the target rows and the status entries in step
    /// with the desired state until `shutdown` resolves. The copies a crash
    /// left pending are resolved meanwhile, in a task of their own.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) {
        let mut shutdown = std::pin::pin!(shutdown);
        // Not awaited: the hashes would hold back every listener. Nothing
        // aborts the task; an agent that stops during a hash leaves the copy
        // pending, and the next start or stream resolves it.
        let recovering = self.clone();
        tokio::spawn(async move { recovering.recover_pending().await });
        let mut desired = self.inner.desired.clone();
        let mut listeners: BTreeMap<SocketAddr, JoinHandle<()>> = BTreeMap::new();
        let mut tick = tokio::time::interval(self.inner.options.bind_retry);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            self.pass(&mut listeners).await;
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
        for (_, task) in listeners {
            task.abort();
        }
        info!("warm receiver stopped");
    }

    /// Serves streams on `listener` until the task is aborted. The node's
    /// own listeners come from [`WarmReceiver::run`]; a test passes one on a
    /// loopback address.
    pub async fn serve(&self, listener: TcpListener) {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, "could not accept a replication connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&self.inner.connections).try_acquire_owned() else {
                debug!(%remote, "too many replication connections; closing one");
                continue;
            };
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let _permit = permit;
                let timeout = inner.options.frame_timeout;
                let service = hyper::service::service_fn(move |request| {
                    let inner = Arc::clone(&inner);
                    async move {
                        // Its own task: hyper drops the service future when
                        // the source closes the connection, which must not
                        // cut a stream between its checks and its swap. The
                        // task is never aborted or timed out: dropped during
                        // `commit_staged_volume`, it would release the lock
                        // while the engine's rename still runs, and the
                        // next stream's staging file could be renamed over
                        // the copy half written.
                        let task =
                            tokio::spawn(async move { inner.handle(request, remote.ip()).await });
                        Ok::<_, Infallible>(task.await.unwrap_or_else(|e| {
                            warn!(%remote, error = %e, "a replication stream's task failed");
                            plain(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "the stream's task failed",
                            )
                        }))
                    }
                });
                let mut builder = hyper::server::conn::http1::Builder::new();
                builder
                    .timer(TokioTimer::new())
                    .header_read_timeout(timeout)
                    .keep_alive(false);
                if let Err(e) = builder
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    debug!(%remote, error = %e, "replication connection ended");
                }
            });
        }
    }

    /// One pass: records the volumes this node is the target of, removes the
    /// copies of `deleted` entries, opens and closes listeners, and publishes
    /// the status entries.
    async fn pass(&self, listeners: &mut BTreeMap<SocketAddr, JoinHandle<()>>) {
        let spec = self
            .inner
            .desired
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.state.spec.clone());
        let Some(spec) = spec else {
            self.inner.publish().await;
            return;
        };
        let targets = targets(&spec);
        for (file, _) in &targets {
            if let Err(e) = self
                .inner
                .store
                .add_warm_target(&file.volume_id, &file.app_id, file.size_gb)
                .await
            {
                warn!(volume_id = %file.volume_id, error = %e, "could not record a warm target");
            }
        }
        for file in &spec.volumes {
            if file.state == SpecVolumeState::Deleted {
                self.inner.delete_copy(&file.volume_id).await;
            }
        }
        self.inner.forget_removed(&spec).await;

        let wanted: BTreeSet<SocketAddr> = match mesh_ip(&spec) {
            Some(ip) => targets
                .iter()
                .map(|(_, warm)| SocketAddr::new(ip, warm.port))
                .collect(),
            None => BTreeSet::new(),
        };
        listeners.retain(|addr, task| {
            let keep = wanted.contains(addr) && !task.is_finished();
            if !keep {
                task.abort();
            }
            keep
        });
        {
            let mut errors = self
                .inner
                .bind_errors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            errors.retain(|addr, _| wanted.contains(addr));
        }
        for addr in wanted {
            if listeners.contains_key(&addr) {
                continue;
            }
            match TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!(%addr, "warm replication listener ready");
                    self.inner
                        .bind_errors
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&addr);
                    let receiver = self.clone();
                    listeners.insert(
                        addr,
                        tokio::spawn(async move { receiver.serve(listener).await }),
                    );
                }
                Err(e) => {
                    // The mesh interface may not have its address yet; the
                    // next pass tries again.
                    debug!(%addr, error = %e, "cannot listen for warm replication yet");
                    self.inner
                        .bind_errors
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(addr, format!("cannot listen on {addr}: {e}"));
                }
            }
        }
        self.inner.publish().await;
    }

    /// Resolves every copy a crash left pending (see the module
    /// documentation), one at a time.
    pub async fn recover_pending(&self) {
        let rows = match self.inner.store.warm_targets().await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "could not read the warm targets");
                return;
            }
        };
        let pending: Vec<String> = rows
            .into_iter()
            .filter(|row| row.pending.is_some())
            .map(|row| row.volume_id)
            .collect();
        #[cfg(test)]
        self.inner.pending_listed.notify_one();
        for volume_id in pending {
            let guard = self.inner.locks.lock(&volume_id).await;
            // Read again under the lock: a stream may have resolved the copy,
            // and swapped a newer one in, while this waited for it.
            let row = match self.inner.store.warm_target(&volume_id).await {
                Ok(Some(row)) if row.pending.is_some() && !row.removed => row,
                Ok(_) => continue,
                Err(e) => {
                    warn!(volume_id, error = %e, "could not read a warm target");
                    continue;
                }
            };
            // A copy about to be deleted needs no hash.
            if self.inner.entry_state(&volume_id) == Some(SpecVolumeState::Deleted) {
                continue;
            }
            // The node holds the volume as its own (it turned from target
            // into source): the file is the live volume, which no stream
            // writes and nothing hashes here. Its warm record waits for the
            // hand-over of section 9.
            match self.inner.own_volume(&volume_id).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(e) => {
                    warn!(volume_id, error = %e, "could not read the volume row");
                    continue;
                }
            }
            self.inner.resolve(guard, row).await;
        }
    }
}

impl<E: Engine + 'static> Inner<E> {
    /// Answers one request.
    async fn handle(
        self: &Arc<Self>,
        request: Request<Incoming>,
        remote: IpAddr,
    ) -> Response<Full<Bytes>> {
        if request.method() != Method::POST || request.uri().path() != REPLICATE_PATH {
            let reader = FrameReader::new(request.into_body(), self.options.frame_timeout);
            tokio::spawn(reader.drain(self.options.drain_timeout, self.options.drain_bytes));
            return plain(StatusCode::NOT_FOUND, "not found");
        }
        let mut reader = FrameReader::new(request.into_body(), self.options.frame_timeout);
        let outcome = match reader.next().await {
            Ok(Frame::Header(header)) => self.receive(&header, remote, &mut reader).await,
            Ok(_) => Outcome::Answer(refused(RefuseReason::BadFrame, None)),
            Err(e) => {
                debug!(%remote, error = %e, "a replication stream without a header");
                Outcome::Answer(refused(RefuseReason::BadFrame, None))
            }
        };
        if !reader.ended {
            // A refusal arrives while the source may still be writing; read
            // the rest for a while, so the connection does not close under
            // the answer before the source has read it.
            let (timeout, bytes) = (self.options.drain_timeout, self.options.drain_bytes);
            tokio::spawn(reader.drain(timeout, bytes));
        }
        match outcome {
            Outcome::Answer(answer) => {
                let body = serde_json::to_vec(&answer).unwrap_or_default();
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap_or_else(|_| plain(StatusCode::INTERNAL_SERVER_ERROR, "answer"))
            }
            Outcome::Failed(reason) => plain(StatusCode::INTERNAL_SERVER_ERROR, &reason),
        }
    }

    /// Checks one stream, applies it and answers.
    async fn receive<B>(
        self: &Arc<Self>,
        header: &StreamHeader,
        remote: IpAddr,
        reader: &mut FrameReader<B>,
    ) -> Outcome
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        let outcome = match self.accept(header, remote).await {
            Ok(accepted) => self.apply(header, remote, accepted, reader).await,
            Err(Reject::Refuse(reason, seq)) => {
                info!(
                    volume_id = %header.volume_id,
                    snapshot_seq = header.snapshot_seq,
                    reason = reason.as_str(),
                    "refused a warm stream"
                );
                Outcome::Answer(refused(reason, seq))
            }
            Err(Reject::Failed(reason)) => Outcome::Failed(reason),
        };
        self.publish().await;
        outcome
    }

    /// The `spec.volumes` entry this node is the target of for `volume_id`.
    fn target_entry(&self, volume_id: &str) -> Option<(SpecVolumeFile, SpecWarm)> {
        let desired = self.desired.borrow();
        let spec = &desired.as_ref()?.state.spec;
        targets(spec)
            .into_iter()
            .find(|(file, _)| file.volume_id == volume_id)
            .map(|(file, warm)| (file.clone(), warm.clone()))
    }

    /// The checks that need no lock: the target, the peer, the lease and the
    /// token. Returns the entry's size and the complete copy's seq.
    async fn check_entry(
        &self,
        header: &StreamHeader,
        remote: IpAddr,
    ) -> Result<(u32, Option<u64>), Reject> {
        let row = self
            .store
            .warm_target(&header.volume_id)
            .await
            .map_err(|e| Reject::Failed(e.to_string()))?;
        let seq = row.as_ref().and_then(|row| row.complete_seq);
        let refuse = |reason| Reject::Refuse(reason, seq);
        let Some((file, warm)) = self.target_entry(&header.volume_id) else {
            return Err(refuse(RefuseReason::NotTarget));
        };
        if file.app_id != header.app_id || peer_ip(&warm.peer_address) != Some(remote) {
            return Err(refuse(RefuseReason::NotTarget));
        }
        if warm.lease_id != header.lease_id {
            return Err(refuse(RefuseReason::WrongLease));
        }
        let highest = row
            .as_ref()
            .map_or(0, |row| row.highest_token)
            .max(warm.fencing_token);
        if header.fencing_token < highest {
            self.record_stale(&file, header.fencing_token).await;
            return Err(refuse(RefuseReason::StaleToken));
        }
        Ok((file.size_gb, seq))
    }

    /// Records a stream refused as stale (section 6.13).
    async fn record_stale(&self, file: &SpecVolumeFile, token: u64) {
        let at = util::now_rfc3339();
        let recorded = async {
            self.store
                .add_warm_target(&file.volume_id, &file.app_id, file.size_gb)
                .await?;
            self.store
                .record_warm_rejection(&file.volume_id, token, &at)
                .await
        };
        if let Err(e) = recorded.await {
            warn!(volume_id = %file.volume_id, error = %e, "could not record a stale stream");
        }
    }

    /// The state of the desired state's entry for `volume_id`, if it names
    /// the volume.
    fn entry_state(&self, volume_id: &str) -> Option<SpecVolumeState> {
        let desired = self.desired.borrow();
        desired
            .as_ref()?
            .state
            .spec
            .volumes
            .iter()
            .find(|file| file.volume_id == volume_id)
            .map(|file| file.state)
    }

    /// Every check of section 6.13, then the volume's lock.
    async fn accept(
        self: &Arc<Self>,
        header: &StreamHeader,
        remote: IpAddr,
    ) -> Result<Accepted, Reject> {
        let (size_gb, seq) = self.check_entry(header, remote).await?;
        if header.size_bytes != u64::from(size_gb) * GIB {
            return Err(Reject::Refuse(RefuseReason::BadFrame, seq));
        }
        let Some(guard) = self.locks.try_lock(&header.volume_id) else {
            return Err(Reject::Refuse(RefuseReason::Busy, seq));
        };
        // Under the lock: the copy cannot change, so the row is current.
        let file = self
            .target_entry(&header.volume_id)
            .map(|(file, _)| file)
            .ok_or(Reject::Refuse(RefuseReason::NotTarget, seq))?;
        self.store
            .add_warm_target(&file.volume_id, &file.app_id, file.size_gb)
            .await
            .map_err(|e| Reject::Failed(e.to_string()))?;
        if self
            .own_volume(&header.volume_id)
            .await
            .map_err(Reject::Failed)?
        {
            let reason = "this node holds the volume as its own; it takes no stream until \
                          the volume is handed over as the warm copy"
                .to_owned();
            self.set_error(&header.volume_id, Some(&reason)).await;
            return Err(Reject::Failed(reason));
        }
        let mut record = self
            .store
            .warm_target(&header.volume_id)
            .await
            .map_err(|e| Reject::Failed(e.to_string()))?
            .ok_or_else(|| Reject::Failed("the warm target row is gone".to_owned()))?;
        let seq = record.complete_seq;
        if record.pending.is_some() {
            // Resolving the copy reads the whole volume, longer than the
            // source waits for its next frame: it runs in a task that keeps
            // the lock, and the source tries again on its next cycle.
            info!(volume_id = %header.volume_id, "a stream found the copy pending; resolving it first");
            self.resolve_in_task(guard, record);
            return Err(Reject::Refuse(RefuseReason::Busy, seq));
        }
        if header.fencing_token < record.highest_token {
            self.record_stale(&file, header.fencing_token).await;
            return Err(Reject::Refuse(RefuseReason::StaleToken, seq));
        }
        match header.base_seq {
            Some(base) if Some(base) != record.complete_seq => {
                return Err(Reject::Refuse(RefuseReason::BaseMismatch, seq));
            }
            Some(base) if header.snapshot_seq <= base => {
                return Err(Reject::Refuse(RefuseReason::NotNewer, seq));
            }
            None if record
                .complete_seq
                .is_some_and(|complete| header.snapshot_seq <= complete) =>
            {
                return Err(Reject::Refuse(RefuseReason::NotNewer, seq));
            }
            _ => {}
        }
        // The token is the highest seen from the moment the stream is
        // accepted, before anything is written (section 8.6).
        self.store
            .raise_warm_token(&header.volume_id, header.fencing_token)
            .await
            .map_err(|e| Reject::Failed(e.to_string()))?;
        record.highest_token = record.highest_token.max(header.fencing_token);
        Ok(Accepted {
            _guard: guard,
            record,
            size_gb,
        })
    }

    /// Writes an accepted stream into a staging file and swaps it in.
    async fn apply<B>(
        self: &Arc<Self>,
        header: &StreamHeader,
        remote: IpAddr,
        accepted: Accepted,
        reader: &mut FrameReader<B>,
    ) -> Outcome
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        let Accepted {
            _guard: guard,
            mut record,
            size_gb,
        } = accepted;
        let id = VolumeId(header.volume_id.clone());
        let from = if header.base_seq.is_some() {
            StageFrom::Volume
        } else {
            StageFrom::Empty
        };
        let staging = match self.stage(&id, size_gb, &from).await {
            Ok(staging) => staging,
            Err(e) => {
                let reason = format!("could not stage the copy: {e}");
                self.set_error(&header.volume_id, Some(&reason)).await;
                return Outcome::Failed(reason);
            }
        };
        let cells = match self
            .write_stream(header, &record, staging.get(), reader)
            .await
        {
            Ok(cells) => cells,
            Err(failure) => {
                staging.discard().await;
                return match failure {
                    StreamFailure::Refuse(reason, detail) => {
                        info!(
                            volume_id = %header.volume_id,
                            snapshot_seq = header.snapshot_seq,
                            reason = reason.as_str(),
                            detail = %detail,
                            "dropped a warm stream; the complete copy is unchanged"
                        );
                        self.set_error(&header.volume_id, Some(&detail)).await;
                        Outcome::Answer(refused(reason, record.complete_seq))
                    }
                    StreamFailure::Failed(reason) => {
                        self.set_error(&header.volume_id, Some(&reason)).await;
                        Outcome::Failed(reason)
                    }
                };
            }
        };

        // The desired state may have moved while the stream ran: only a
        // stream this node would still accept is swapped in.
        if let Err(reject) = self.still_accepts(header, remote).await {
            staging.discard().await;
            return match reject {
                Reject::Refuse(reason, _) => Outcome::Answer(refused(reason, record.complete_seq)),
                Reject::Failed(reason) => Outcome::Failed(reason),
            };
        }

        // Write ahead: the copy about to be swapped in, so a crash between
        // the swap and its record is resolved by hashing the file once.
        record.pending = Some(PendingCommit {
            seq: header.snapshot_seq,
            taken_at: header.taken_at.clone(),
            root: cells.root_hex(),
            cells: cells.to_bytes(),
        });
        if let Err(e) = self.store.save_warm_target(&record).await {
            staging.discard().await;
            return Outcome::Failed(format!("could not record the copy before the swap: {e}"));
        }
        match self.engine.commit_staged_volume(staging.get()).await {
            Ok(info) => {
                staging.committed();
                record.complete_seq = Some(header.snapshot_seq);
                record.taken_at = Some(header.taken_at.clone());
                record.completed_at = Some(util::now_rfc3339());
                record.allocated_bytes = Some(info.allocated_bytes);
                record.cells = Some(cells.to_bytes());
                record.pending = None;
                // Twice: an answer `complete` the store does not record makes
                // the source's next cycle a full copy.
                let mut saved = self.store.save_warm_target(&record).await;
                if saved.is_err() {
                    saved = self.store.save_warm_target(&record).await;
                }
                if let Err(e) = saved {
                    // The pending copy is still recorded: the next stream or
                    // start finds it by its hash.
                    warn!(volume_id = %header.volume_id, error = %e, "could not record a swapped-in copy");
                }
                self.set_error(&header.volume_id, None).await;
                info!(
                    volume_id = %header.volume_id,
                    snapshot_seq = header.snapshot_seq,
                    "a warm copy is complete"
                );
                Outcome::Answer(Answer {
                    status: AnswerStatus::Complete,
                    snapshot_seq: Some(header.snapshot_seq),
                    reason: None,
                })
            }
            Err(e) => {
                // The volume file is the old copy or the new one (section
                // 3.22); which one is found by its hash, which reads the
                // whole volume. The source gets its answer now, not after
                // that read; the copy is resolved after the answer, still
                // under the volume's lock, so a stream that comes before it
                // is done is refused `busy`.
                staging.discard().await;
                let reason = format!("could not swap the copy in: {e}");
                self.set_error(&header.volume_id, Some(&reason)).await;
                self.resolve_in_task(guard, record);
                Outcome::Failed(reason)
            }
        }
    }

    /// Resolves a pending copy in a task of its own, which holds the
    /// volume's lock until the copy is resolved and published. Nothing
    /// aborts the task; an agent that stops during the hash leaves the copy
    /// pending, and the next start or stream resolves it.
    fn resolve_in_task(self: &Arc<Self>, guard: OwnedMutexGuard<()>, record: WarmTargetRecord) {
        let inner = Arc::clone(self);
        tokio::spawn(async move { inner.resolve(guard, record).await });
    }

    /// Resolves the pending copy of `record`, whose lock `guard` holds,
    /// publishes the result, and only then releases the lock.
    async fn resolve(&self, guard: OwnedMutexGuard<()>, mut record: WarmTargetRecord) {
        #[cfg(test)]
        let gate = self.recovery_gate.lock().await;
        match self.recover(&mut record).await {
            Ok(()) => {
                // Only an earlier resolution's error goes: a failed swap's
                // stays until a stream lands. Read from the store, since a
                // failed swap set its error after `record` was read.
                let unresolved = self
                    .store
                    .warm_target(&record.volume_id)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|row| row.error)
                    .is_some_and(|error| error.starts_with(UNRESOLVED));
                if unresolved {
                    self.set_error(&record.volume_id, None).await;
                }
            }
            Err(e) => {
                warn!(volume_id = %record.volume_id, error = %e, "could not resolve a warm copy left pending");
                // The copy stays pending and the next stream tries again;
                // the status says why the copy does not move.
                let reason = format!("{UNRESOLVED}: {e}");
                self.set_error(&record.volume_id, Some(&reason)).await;
            }
        }
        #[cfg(test)]
        drop(gate);
        self.publish().await;
        // Only now. A stream that came before was refused `busy`; what takes
        // the lock next (a `deleted` entry's removal, this node's sender)
        // finds the copy resolved and published.
        drop(guard);
    }

    /// The checks of [`Inner::check_entry`] again, against the desired state
    /// and the token as they are now.
    async fn still_accepts(&self, header: &StreamHeader, remote: IpAddr) -> Result<(), Reject> {
        let (size_gb, _) = self.check_entry(header, remote).await?;
        if u64::from(size_gb) * GIB == header.size_bytes {
            Ok(())
        } else {
            Err(Reject::Refuse(RefuseReason::BadFrame, None))
        }
    }

    /// Reads the frames after the header into `staged` and returns the cell
    /// hashes of the result, whose root matched the manifest's.
    async fn write_stream<B>(
        &self,
        header: &StreamHeader,
        record: &WarmTargetRecord,
        staged: &StagedVolume,
        reader: &mut FrameReader<B>,
    ) -> Result<CellHashes, StreamFailure>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        let bad = |detail: String| StreamFailure::Refuse(RefuseReason::BadFrame, detail);
        let failed = |detail: String| StreamFailure::Failed(detail);
        let size = header.size_bytes;
        let full = header.base_seq.is_none();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&staged.location)
            .map(Arc::new)
            .map_err(|e| failed(format!("cannot open the staging file: {e}")))?;
        let mut cells = if full {
            CellHashes::zeroed(size).map_err(|e| bad(e.to_string()))?
        } else {
            match record
                .cells
                .as_deref()
                .map(|bytes| CellHashes::from_bytes(bytes, size))
            {
                Some(Ok(cells)) => cells,
                // Lost or mismatched: the staging file is a clone of the
                // complete copy, so hash it once and keep the result.
                _ => {
                    let cells = hash_file(Arc::clone(&file), size)
                        .await
                        .map_err(|e| failed(format!("cannot hash the complete copy: {e}")))?;
                    if let Some(complete) = record.complete_seq
                        && let Err(e) = self
                            .store
                            .set_warm_cells(&record.volume_id, complete, &cells.to_bytes())
                            .await
                    {
                        warn!(volume_id = %record.volume_id, error = %e, "could not record the copy's cell hashes");
                    }
                    cells
                }
            }
        };
        let mut sequence = Sequence::new(size).map_err(|e| bad(e.to_string()))?;
        // Cells written in part, hashed again from the file at the manifest.
        let mut dirty: BTreeSet<u64> = BTreeSet::new();
        loop {
            match reader.next().await.map_err(bad)? {
                Frame::Header(_) => return Err(bad("a second header".to_owned())),
                Frame::Block {
                    offset,
                    blake3,
                    data,
                } => {
                    let length = data.len() as u64;
                    sequence
                        .block(offset, length)
                        .map_err(|e| bad(e.to_string()))?;
                    if hash_cell(&data) != blake3 {
                        return Err(bad(format!(
                            "the block at {offset} does not match its hash"
                        )));
                    }
                    write_at(Arc::clone(&file), offset, data)
                        .await
                        .map_err(|e| failed(format!("cannot write the staging file: {e}")))?;
                    if offset.is_multiple_of(CELL) && length == CELL {
                        // A block that covers its cell carries the cell's
                        // hash already.
                        cells
                            .set(offset / CELL, blake3)
                            .map_err(|e| failed(e.to_string()))?;
                    } else {
                        dirty.insert(offset / CELL);
                    }
                }
                Frame::Zero { offset, length } => {
                    sequence
                        .zero(offset, length)
                        .map_err(|e| bad(e.to_string()))?;
                    if !full {
                        // A full copy starts as an empty sparse file, which
                        // reads as zeros already.
                        zero_range(Arc::clone(&file), offset, length)
                            .await
                            .map_err(|e| failed(format!("cannot zero the staging file: {e}")))?;
                    }
                    let end = offset + length;
                    for cell in offset.div_ceil(CELL)..end / CELL {
                        cells
                            .set(cell, zero_cell_hash())
                            .map_err(|e| failed(e.to_string()))?;
                    }
                    if !offset.is_multiple_of(CELL) {
                        dirty.insert(offset / CELL);
                    }
                    if !end.is_multiple_of(CELL) {
                        dirty.insert(end / CELL);
                    }
                }
                Frame::Manifest(manifest) => {
                    sequence
                        .manifest(&manifest)
                        .map_err(|e| bad(e.to_string()))?;
                    reader.expect_end().await.map_err(bad)?;
                    let file = Arc::clone(&file);
                    let cells = tokio::task::spawn_blocking(move || {
                        let mut handle = &*file;
                        cells.rehash(&mut handle, dirty).map(|()| cells)
                    })
                    .await
                    .map_err(|e| failed(format!("the hashing task failed: {e}")))?
                    .map_err(|e| failed(format!("cannot hash the staging file: {e}")))?;
                    if cells.root_hex() != manifest.root_hash {
                        return Err(StreamFailure::Refuse(
                            RefuseReason::RootMismatch,
                            format!(
                                "the copy's root hash {} is not the manifest's {}",
                                cells.root_hex(),
                                manifest.root_hash
                            ),
                        ));
                    }
                    return Ok(cells);
                }
            }
        }
    }

    /// Resolves a copy left pending: hashes the volume file once and keeps
    /// the copy it matches, the pending one or the complete one, or no copy.
    /// The caller holds the volume's lock.
    async fn recover(&self, record: &mut WarmTargetRecord) -> Result<(), String> {
        let Some(pending) = record.pending.clone() else {
            return Ok(());
        };
        let id = VolumeId(record.volume_id.clone());
        let size = u64::from(record.size_gb) * GIB;
        // The engine owns the volume file; a reflink clone of it is read
        // instead, so nothing here depends on where the file lives.
        let found = match self.stage(&id, record.size_gb, &StageFrom::Volume).await {
            Ok(staging) => {
                let hashed = match std::fs::File::open(&staging.get().location) {
                    Ok(file) => hash_file(Arc::new(file), size).await,
                    Err(e) => Err(e),
                };
                staging.discard().await;
                Some(hashed.map_err(|e| format!("cannot hash the warm copy: {e}"))?)
            }
            Err(EngineError::NotFound) => None,
            Err(e) => return Err(format!("cannot read the warm copy: {e}")),
        };
        let known_root = record
            .cells
            .as_deref()
            .and_then(|bytes| CellHashes::from_bytes(bytes, size).ok())
            .map(|cells| cells.root_hex());
        match found {
            // Checked first: when the two copies hold the same bytes, the
            // one the source still holds as its base is the one to name.
            Some(cells)
                if record.complete_seq.is_some() && known_root == Some(cells.root_hex()) =>
            {
                info!(volume_id = %record.volume_id, "the swap did not happen; the complete copy is unchanged");
            }
            Some(cells) if cells.root_hex() == pending.root => {
                info!(volume_id = %record.volume_id, snapshot_seq = pending.seq, "the copy swapped in before a crash is complete");
                record.complete_seq = Some(pending.seq);
                record.taken_at = Some(pending.taken_at.clone());
                record.completed_at = Some(util::now_rfc3339());
                record.cells = Some(cells.to_bytes());
            }
            Some(_) => {
                warn!(volume_id = %record.volume_id, "the warm copy matches no recorded copy; the next stream must be a full copy");
                record.complete_seq = None;
                record.taken_at = None;
                record.completed_at = None;
                record.cells = None;
            }
            None => {
                record.complete_seq = None;
                record.taken_at = None;
                record.completed_at = None;
                record.cells = None;
            }
        }
        record.pending = None;
        self.store
            .save_warm_target(record)
            .await
            .map_err(|e| e.to_string())
    }

    /// Removes the copy of a `deleted` entry this node holds as a target,
    /// then forgets it. While a stream or a resolution holds the volume, a
    /// task waits for its lock and removes the copy then, so the pass never
    /// waits for a hash of the volume. A removal that fails is tried again
    /// at a later pass: the node keeps reporting the copy until it is
    /// removed, so the primary keeps sending the entry (ADR 0267).
    async fn delete_copy(self: &Arc<Self>, volume_id: &str) {
        if !self.holds_copy(volume_id).await {
            return;
        }
        if let Some(guard) = self.locks.try_lock(volume_id) {
            self.remove_if_deleted(volume_id, guard).await;
            return;
        }
        let waiting = self
            .deleting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(volume_id.to_owned());
        if !waiting {
            return;
        }
        debug!(
            volume_id,
            "the warm copy is busy; removing it once it is free"
        );
        let inner = Arc::clone(self);
        let volume_id = volume_id.to_owned();
        tokio::spawn(async move {
            let guard = inner.locks.lock(&volume_id).await;
            inner.remove_if_deleted(&volume_id, guard).await;
            inner
                .deleting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&volume_id);
            // The copy leaves the report now, not at the next pass.
            inner.publish().await;
        });
    }

    /// Removes the copy of `volume_id`, whose lock `guard` holds, unless its
    /// entry is `present` again: this node may be the target again, or the
    /// source, whose file the reconciler adopts. Only a volume still
    /// `deleted`, or no longer named, goes.
    async fn remove_if_deleted(&self, volume_id: &str, guard: OwnedMutexGuard<()>) {
        if self.entry_state(volume_id) != Some(SpecVolumeState::Present)
            && self.holds_copy(volume_id).await
        {
            self.remove_copy(volume_id, guard).await;
        }
    }

    /// Whether the node has a warm target row for `volume_id` not yet
    /// marked removed.
    async fn holds_copy(&self, volume_id: &str) -> bool {
        match self.store.warm_target(volume_id).await {
            Ok(row) => row.is_some_and(|row| !row.removed),
            Err(e) => {
                warn!(volume_id, error = %e, "could not read a warm target");
                false
            }
        }
    }

    /// Removes the copy of `volume_id`, whose lock `_guard` holds, and marks
    /// its row removed.
    async fn remove_copy(&self, volume_id: &str, _guard: OwnedMutexGuard<()>) {
        let own = match self.own_volume(volume_id).await {
            Ok(own) => own,
            Err(e) => {
                warn!(volume_id, error = %e, "could not read the volume row");
                return;
            }
        };
        // The node's own volume file is the reconciler's to remove; only the
        // warm record goes.
        let deleted = if own {
            Ok(())
        } else {
            self.engine
                .delete_volume(&VolumeId(volume_id.to_owned()))
                .await
        };
        match deleted {
            Ok(()) | Err(EngineError::NotFound) => {
                if let Err(e) = self.store.mark_warm_removed(volume_id).await {
                    warn!(volume_id, error = %e, "could not record a removed warm copy");
                } else {
                    info!(volume_id, "removed a deleted warm copy");
                }
            }
            Err(e) => warn!(volume_id, error = %e, "could not remove a deleted warm copy"),
        }
    }

    /// Whether the node holds `volume_id` as a volume of its own: the
    /// reconciler created or keeps its file (it was the source, or the volume
    /// is a plain one). No stream ever writes that file: a node that turns
    /// from source into target first keeps the data its file holds, then
    /// hands the file over (the return path, section 9).
    async fn own_volume(&self, volume_id: &str) -> Result<bool, String> {
        self.store
            .volume(volume_id)
            .await
            .map(|row| row.is_some_and(|row| row.state != VolumeState::Deleted))
            .map_err(|e| e.to_string())
    }

    /// Forgets the rows of removed copies whose volume `spec.volumes` no
    /// longer names.
    async fn forget_removed(&self, spec: &Spec) {
        let rows = match self.store.warm_target_summaries().await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "could not read the warm targets");
                return;
            }
        };
        for row in rows.into_iter().filter(|row| row.removed) {
            if spec
                .volumes
                .iter()
                .any(|file| file.volume_id == row.volume_id)
            {
                continue;
            }
            let _guard = self.locks.lock(&row.volume_id).await;
            if let Err(e) = self.store.forget_warm_target(&row.volume_id).await {
                warn!(volume_id = %row.volume_id, error = %e, "could not forget a removed warm copy");
            }
        }
    }

    /// A staging file for `id`, after removing a leftover one. The caller
    /// holds the volume's lock.
    async fn stage(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &StageFrom,
    ) -> Result<Staging<'_, E>, EngineError> {
        let leftover = self
            .leftovers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id.0);
        if let Some(leftover) = leftover
            && let Err(e) = self.engine.discard_staged_volume(&leftover).await
        {
            self.leftovers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.0.clone(), leftover);
            return Err(e);
        }
        let staged = self.engine.stage_volume(id, size_gb, from).await?;
        Ok(Staging {
            inner: self,
            staged: Some(staged),
        })
    }

    /// Removes a staging file; one the engine could not remove is removed
    /// again before the volume's next staging file.
    async fn discard(&self, staged: &StagedVolume) {
        if let Err(e) = self.engine.discard_staged_volume(staged).await {
            warn!(volume_id = %staged.volume_id.0, error = %e, "could not remove a staging file");
            self.leftovers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(staged.volume_id.0.clone(), staged.clone());
        }
    }

    async fn set_error(&self, volume_id: &str, error: Option<&str>) {
        let error = error.map(|error| util::one_line(error, ERROR_CHARS));
        if let Err(e) = self.store.set_warm_error(volume_id, error.as_deref()).await {
            warn!(volume_id, error = %e, "could not record a warm error");
        }
    }

    /// Publishes this node's `status_report.warm` entries as a target
    /// (section 5.3):
    /// - one per volume `spec.volumes` names as a `present` target entry;
    /// - one per copy the node still holds that the spec does not name so: a
    ///   copy whose app left `warm`, got another warm node, was deleted or is
    ///   moving, and one whose removal failed. It is reported until it is
    ///   removed, so the primary keeps sending its `deleted` entry, also
    ///   after its own restart (ADR 0267). A row with no complete copy is
    ///   reported too: a failed swap may have left a file of unknown content.
    ///
    /// A volume the node holds as its own is never reported the second way:
    /// its file is that volume, not a warm copy, and a `deleted` entry for it
    /// would reach the reconciler, which would remove it (section 9).
    async fn publish(&self) {
        let _publishing = self.publishing.lock().await;
        let spec = self
            .desired
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.state.spec.clone());
        let Some(spec) = spec else {
            self.board.set(PART, Vec::new());
            return;
        };
        let rows: BTreeMap<String, WarmTargetRecord> =
            match self.store.warm_target_summaries().await {
                Ok(rows) => rows
                    .into_iter()
                    .map(|row| (row.volume_id.clone(), row))
                    .collect(),
                Err(e) => {
                    warn!(error = %e, "could not read the warm targets");
                    return;
                }
            };
        let own: BTreeSet<String> = match self.store.volumes().await {
            Ok(volumes) => volumes
                .into_iter()
                .filter(|volume| volume.state != VolumeState::Deleted)
                .map(|volume| volume.volume_id)
                .collect(),
            Err(e) => {
                warn!(error = %e, "could not read the volume rows");
                return;
            }
        };
        let ip = mesh_ip(&spec);
        let bind_errors = self
            .bind_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let named = targets(&spec);
        let mut entries: Vec<WarmStatus> = named
            .iter()
            .map(|(file, warm)| {
                let listen_error = match ip {
                    None => Some("this node has no mesh address yet".to_owned()),
                    Some(ip) => bind_errors.get(&SocketAddr::new(ip, warm.port)).cloned(),
                };
                target_status(
                    &file.volume_id,
                    &file.app_id,
                    rows.get(&file.volume_id),
                    listen_error,
                )
            })
            .collect();
        let held = rows.values().filter(|row| {
            !row.removed
                && !own.contains(&row.volume_id)
                && !named
                    .iter()
                    .any(|(file, _)| file.volume_id == row.volume_id)
        });
        entries.extend(held.map(|row| target_status(&row.volume_id, &row.app_id, Some(row), None)));
        self.board.set(PART, entries);
    }
}

/// The `status_report.warm` entry of a copy this node holds as a target, from
/// its row when it has one.
fn target_status(
    volume_id: &str,
    app_id: &str,
    row: Option<&WarmTargetRecord>,
    listen_error: Option<String>,
) -> WarmStatus {
    let complete = row.and_then(|row| row.complete_seq);
    WarmStatus {
        volume_id: volume_id.to_owned(),
        app_id: app_id.to_owned(),
        role: WarmRole::Target,
        snapshot_seq: complete,
        snapshot_taken_at: complete.and(row.and_then(|row| row.taken_at.clone())),
        completed_at: complete.and(row.and_then(|row| row.completed_at.clone())),
        allocated_bytes: complete.and(row.and_then(|row| row.allocated_bytes)),
        extent_count: None,
        frozen: None,
        rejected_token: row.and_then(|row| row.rejected_token),
        rejected_at: row.and_then(|row| row.rejected_at.clone()),
        error: row
            .and_then(|row| row.error.clone())
            .or(listen_error)
            .map(|error| util::one_line(&error, ERROR_CHARS)),
    }
}

/// The `present` entries of `spec.volumes` this node is the target of.
pub fn targets(spec: &Spec) -> Vec<(&SpecVolumeFile, &SpecWarm)> {
    spec.volumes
        .iter()
        .filter(|file| file.state == SpecVolumeState::Present)
        .filter_map(|file| {
            file.warm
                .as_ref()
                .filter(|warm| warm.role == WarmRole::Target)
                .map(|warm| (file, warm))
        })
        .collect()
}

/// This node's mesh address, without its prefix length.
fn mesh_ip(spec: &Spec) -> Option<IpAddr> {
    spec.mesh.as_ref().and_then(|mesh| peer_ip(&mesh.address))
}

/// An address that may carry a prefix length, as an IP address.
fn peer_ip(address: &str) -> Option<IpAddr> {
    address.split('/').next()?.parse().ok()
}

fn refused(reason: RefuseReason, snapshot_seq: Option<u64>) -> Answer {
    Answer {
        status: AnswerStatus::Refused,
        snapshot_seq,
        reason: Some(reason),
    }
}

fn plain(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(util::one_line(text, ERROR_CHARS))));
    *response.status_mut() = status;
    response
}

/// The cell hashes of a file of `size` bytes, off the async threads.
async fn hash_file(file: Arc<std::fs::File>, size: u64) -> std::io::Result<CellHashes> {
    tokio::task::spawn_blocking(move || super::hash_copy(&file, size))
        .await
        .map_err(std::io::Error::other)?
}

async fn write_at(file: Arc<std::fs::File>, offset: u64, data: Vec<u8>) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || file.write_all_at(&data, offset))
        .await
        .map_err(std::io::Error::other)?
}

/// Makes `offset..offset + length` read as zeros: a punched hole where the
/// filesystem has them, written zeros otherwise.
async fn zero_range(file: Arc<std::fs::File>, offset: u64, length: u64) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        #[cfg(target_os = "linux")]
        {
            use rustix::fs::FallocateFlags;
            if rustix::fs::fallocate(
                &*file,
                FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
                offset,
                length,
            )
            .is_ok()
            {
                return Ok(());
            }
        }
        let zeros = vec![0_u8; CELL as usize];
        let mut at = offset;
        let end = offset + length;
        while at < end {
            let n = (end - at).min(CELL);
            file.write_all_at(&zeros[..usize::try_from(n).unwrap_or(zeros.len())], at)?;
            at += n;
        }
        Ok(())
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Reads frames from a request body, with a timeout on each read.
struct FrameReader<B> {
    body: B,
    decoder: Decoder,
    timeout: Duration,
    /// The body has no more data.
    ended: bool,
}

impl<B> FrameReader<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    fn new(body: B, timeout: Duration) -> Self {
        Self {
            body,
            decoder: Decoder::new(),
            timeout,
            ended: false,
        }
    }

    /// The next frame. Every failure is one line: a malformed frame, a body
    /// that ended or broke off, or one that sent nothing for the timeout.
    async fn next(&mut self) -> Result<Frame, String> {
        loop {
            if let Some(frame) = self.decoder.next_frame().map_err(|e| e.to_string())? {
                return Ok(frame);
            }
            if self.ended {
                return Err("the stream ended before its manifest".to_owned());
            }
            self.pull().await?;
        }
    }

    /// The body ends right after the manifest.
    async fn expect_end(&mut self) -> Result<(), String> {
        loop {
            if self.decoder.pending() > 0 {
                return Err("data after the manifest".to_owned());
            }
            if self.ended {
                return Ok(());
            }
            self.pull().await?;
        }
    }

    async fn pull(&mut self) -> Result<(), String> {
        match tokio::time::timeout(self.timeout, self.body.frame()).await {
            Err(_) => Err(format!("the stream sent nothing for {:?}", self.timeout)),
            Ok(None) => {
                self.ended = true;
                Ok(())
            }
            Ok(Some(Err(e))) => Err(format!("the stream broke off: {e}")),
            Ok(Some(Ok(frame))) => {
                if let Ok(data) = frame.into_data() {
                    self.decoder.push(&data);
                }
                Ok(())
            }
        }
    }

    /// Reads and throws away the rest of the body, for at most `timeout` and
    /// `max_bytes`.
    async fn drain(mut self, timeout: Duration, max_bytes: u64) {
        let deadline = Instant::now() + timeout;
        let mut read = 0_u64;
        while read < max_bytes {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match tokio::time::timeout(left, self.body.frame()).await {
                Ok(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        read += data.len() as u64;
                    }
                }
                _ => break,
            }
        }
    }
}

#[cfg(test)]
mod tests;
