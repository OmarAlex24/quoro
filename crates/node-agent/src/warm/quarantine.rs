// SPDX-License-Identifier: AGPL-3.0-only
//! A returning source's quarantined copies of a volume
//! (`node-agent-protocol.md` sections 5.3, 5.11, 9 and 12, ADR 0176, ADR
//! 0215).
//!
//! The warm receiver's hand-over ([`super::receiver`]) keeps a live volume
//! file it cannot prove unchanged as a reflink clone, recorded in the store
//! with a `qrn_` id. From then on this module owns the copy:
//!
//! - **`status_report.quarantines`.** One entry per copy the store records,
//!   with `sha256` only once it is known. The list is read again on every
//!   pass of the receiver and published when it changed: when a copy is
//!   added, gets its hash, or goes, and after a read that failed. The session
//!   reports each new list.
//! - **The SHA-256.** A blocking task hashes the clone's bytes, exactly what
//!   `sha256sum` prints for the file: holes are hashed as zeros without being
//!   read. No volume lock is held meanwhile. Every pass starts the hash of a
//!   copy without one that is not being hashed; one whose hash failed waits
//!   first, longer after each failure. A hash reads the record again before
//!   it reads the clone, and records its result before another may start, so
//!   a copy is hashed once. A copy deleted during its hash stops it, and the
//!   hash gives up quietly. A copy whose clone is missing stays recorded and
//!   reported without a hash; its hash fails and waits like any other, and
//!   completes once the file is back, unless its clone is shown to be gone
//!   (below).
//! - **A clone gone before its hash** (section 9 step 2). When the hash of a
//!   copy finds no file, its record is forgotten, with its kept snapshot and
//!   without touching any file, only when all three hold: the engine
//!   answers `NotFound` for the clone as a snapshot of the volume (a pool
//!   that does not mount answers `Unsupported`), and its path is missing;
//!   another kept snapshot of the volume answers the engine, so the
//!   directory is the real one; and the record still has no SHA-256, so the
//!   primary never recorded the copy. The path checks read one open handle
//!   of the clone's directory: the clone is missing, the other kept snapshot
//!   is a regular file there, and the clone is still missing. Otherwise the
//!   record stays with a warning that says why, and the hash is tried again.
//!   The next report no longer lists a forgotten copy.
//! - **`spec.quarantine_deletes`.** Each id the node holds is deleted: the
//!   clone first, through the engine, then its kept snapshot and its record
//!   in one transaction. Only the record's id and `snapshot_id` are read for
//!   it, so a row that cannot be read whole is deleted too. An id the node
//!   does not hold is ignored, and a delete that failed is tried again with
//!   the next pass. Nothing else forgets a record but the case above: a file
//!   lookup that came back empty proves nothing alone, since a volume
//!   directory that did not mount looks exactly like it (section 5.11,
//!   [`crate::kept`]).

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use quoro_engine::{Engine, EngineError, RangesOf};
use quoro_proto::wire::QuarantineStatus;
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::{ZERO_CELL, data_segments};
use crate::store::{NodeStore, VolumeQuarantineRecord};
use crate::util;

/// The longest wait before a failed hash is tried again.
const HASH_RETRY_MAX: Duration = Duration::from_secs(30 * 60);

/// The quarantined copies of volumes this node keeps.
pub struct Quarantines<E: Engine> {
    pub(super) inner: Arc<Inner<E>>,
}

impl<E: Engine> Clone for Quarantines<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub(super) struct Inner<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    tx: watch::Sender<Vec<QuarantineStatus>>,
    /// Held from reading the records to publishing them, so an older read
    /// never overwrites a newer list.
    publishing: tokio::sync::Mutex<()>,
    /// The copies being hashed, each with the flag that stops its hash.
    hashing: StdMutex<HashMap<String, Arc<AtomicBool>>>,
    /// The copies whose hash failed: when the next try may start, and how
    /// long the one after it waits.
    retry_at: StdMutex<HashMap<String, (Instant, Duration)>>,
    /// The wait after a first failed hash.
    retry_first: Duration,
    /// Held by a test to keep a hash from starting.
    #[cfg(test)]
    pub(super) hash_gate: tokio::sync::Mutex<()>,
    /// How many of the next hashes a test makes fail.
    #[cfg(test)]
    pub(super) fail_hashes: std::sync::atomic::AtomicUsize,
    /// What a test runs after the engine's lookups of a lost clone, before
    /// the volume directory is read.
    #[cfg(test)]
    pub(super) between_checks: StdMutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl<E: Engine> Drop for Inner<E> {
    /// Stops every hash still running: its result has nowhere to go.
    fn drop(&mut self) {
        let hashing = self
            .hashing
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for cancel in hashing.values() {
            cancel.store(true, Ordering::Relaxed);
        }
    }
}

impl<E: Engine + 'static> Quarantines<E> {
    /// The copies and the list the session reports. A failed hash is tried
    /// again after `retry_first`, then after twice as long each time, up to
    /// 30 minutes.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        retry_first: Duration,
    ) -> (Self, watch::Receiver<Vec<QuarantineStatus>>) {
        let (tx, rx) = watch::channel(Vec::new());
        (
            Self {
                inner: Arc::new(Inner {
                    engine,
                    store,
                    tx,
                    publishing: tokio::sync::Mutex::new(()),
                    hashing: StdMutex::new(HashMap::new()),
                    retry_at: StdMutex::new(HashMap::new()),
                    retry_first,
                    #[cfg(test)]
                    hash_gate: tokio::sync::Mutex::new(()),
                    #[cfg(test)]
                    fail_hashes: std::sync::atomic::AtomicUsize::new(0),
                    #[cfg(test)]
                    between_checks: StdMutex::new(None),
                }),
            },
            rx,
        )
    }

    /// The list `status_report.quarantines` carries.
    #[must_use]
    pub fn statuses(&self) -> watch::Receiver<Vec<QuarantineStatus>> {
        self.inner.tx.subscribe()
    }

    /// Each pass, and at start: publishes the recorded copies when the list
    /// changed, and starts the hash of every copy without one that is not
    /// being hashed and whose last failed hash waited long enough.
    ///
    /// # Errors
    ///
    /// The store's error when the records cannot be read; the list stays as
    /// it was.
    pub async fn refresh(&self) -> Result<(), String> {
        let records = self.inner.publish().await?;
        let now = Instant::now();
        {
            let mut retry_at = self.lock_retry();
            retry_at.retain(|id, _| records.iter().any(|record| &record.quarantine_id == id));
        }
        for record in records.iter().filter(|record| record.sha256.is_none()) {
            let due = self
                .lock_retry()
                .get(&record.quarantine_id)
                .is_none_or(|(at, _)| *at <= now);
            if due {
                self.hash(record);
            }
        }
        Ok(())
    }

    /// Publishes one entry per recorded copy, oldest first, when the list
    /// changed.
    ///
    /// # Errors
    ///
    /// The store's error when the records cannot be read; the list stays as
    /// it was.
    pub async fn publish(&self) -> Result<(), String> {
        self.inner.publish().await.map(|_| ())
    }

    /// A copy the hand-over just recorded: it is reported at once, without
    /// its hash, and hashed in the background.
    pub async fn added(&self, record: &VolumeQuarantineRecord) {
        if let Err(e) = self.publish().await {
            warn!(error = %e, "could not read the quarantined copies; the next pass reports them");
        }
        self.hash(record);
    }

    /// Deletes each copy of `ids` the node holds (`spec.quarantine_deletes`,
    /// section 5.11): the clone first, through the engine, which answers
    /// `Ok` for a missing one, then its kept snapshot and its record in one
    /// transaction. Only the record's `snapshot_id` is read, so a row that
    /// cannot be read whole still goes. An id the node does not hold is
    /// ignored. A copy whose clone the engine would not delete stays
    /// recorded and reported, and the next pass tries again.
    pub async fn delete(&self, ids: &[String]) {
        let mut changed = false;
        for id in ids {
            let snapshot_id = match self.inner.store.volume_quarantine_snapshot_id(id).await {
                Ok(Some(snapshot_id)) => snapshot_id,
                Ok(None) => continue,
                Err(e) => {
                    warn!(quarantine_id = %id, error = %e, "could not read a quarantined copy");
                    continue;
                }
            };
            match self.remove(id, &snapshot_id).await {
                Ok(()) => {
                    info!(
                        quarantine_id = %id,
                        snapshot_id,
                        "deleted a quarantined copy an operator deleted"
                    );
                    changed = true;
                }
                Err(e) => {
                    warn!(quarantine_id = %id, error = %e, "could not delete a quarantined copy; trying again with the next pass");
                }
            }
        }
        if changed && let Err(e) = self.publish().await {
            warn!(error = %e, "could not read the quarantined copies; the next pass reports them");
        }
    }

    /// Removes one copy: stops its hash, deletes the clone through the
    /// engine, then forgets its kept snapshot and its record in one
    /// transaction.
    async fn remove(&self, id: &str, snapshot_id: &str) -> Result<(), String> {
        // Its hash has nothing left to read.
        if let Some(cancel) = self.lock_hashing().remove(id) {
            cancel.store(true, Ordering::Relaxed);
        }
        self.lock_retry().remove(id);
        match self.inner.store.kept_snapshot(snapshot_id).await {
            Ok(Some(kept)) => self
                .inner
                .engine
                .delete_volume_snapshot(&kept.snapshot)
                .await
                .map_err(|e| format!("cannot delete the clone: {e}"))?,
            // Recorded together and forgotten together, so this is not
            // expected; the record still goes.
            Ok(None) => warn!(
                quarantine_id = %id,
                "a quarantined copy has no kept snapshot; forgetting its record"
            ),
            Err(e) => return Err(format!("cannot read the copy's kept snapshot: {e}")),
        }
        self.inner
            .store
            .forget_volume_quarantine(id, snapshot_id)
            .await
            .map_err(|e| format!("cannot forget the copy's records: {e}"))
    }

    fn lock_hashing(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<AtomicBool>>> {
        self.inner
            .hashing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_retry(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Instant, Duration)>> {
        self.inner.lock_retry()
    }

    /// Whether the last hash of `id` failed and waits to be tried again.
    #[cfg(test)]
    pub(super) fn hash_failed(&self, id: &str) -> bool {
        self.lock_retry().contains_key(id)
    }

    /// Whether a hash of `id` runs.
    #[cfg(test)]
    pub(super) fn hashing(&self, id: &str) -> bool {
        self.lock_hashing().contains_key(id)
    }

    /// Hashes one copy in the background, unless it is being hashed or its
    /// record has a hash by the time the task reads it.
    pub(super) fn hash(&self, record: &VolumeQuarantineRecord) {
        let cancel = Arc::new(AtomicBool::new(false));
        {
            let mut hashing = self.lock_hashing();
            if hashing.contains_key(&record.quarantine_id) {
                return;
            }
            hashing.insert(record.quarantine_id.clone(), Arc::clone(&cancel));
        }
        // Weak: a hash never keeps the agent's state alive, and dropping it
        // stops the hash ([`Inner`]'s `Drop`).
        let weak: Weak<Inner<E>> = Arc::downgrade(&self.inner);
        let id = record.quarantine_id.clone();
        let volume_id = record.volume_id.clone();
        let snapshot_id = record.snapshot_id.clone();
        let path = record.path.clone();
        tokio::spawn(async move {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            #[cfg(test)]
            let gate = inner.hash_gate.lock().await;
            // Read again while the copy is this task's to hash: a hash that
            // ended since the list was read left its result in the store.
            let pending = matches!(
                inner.store.volume_quarantine(&id).await,
                Ok(Some(ref record)) if record.sha256.is_none()
            );
            if !pending {
                inner.release_hash(&id, &cancel);
                debug!(quarantine_id = %id, "a quarantined copy has its hash, or is gone");
                return;
            }
            #[cfg(test)]
            let injected = {
                drop(gate);
                inner
                    .fail_hashes
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            };
            #[cfg(not(test))]
            let injected = false;
            drop(inner);
            let hashed = if injected {
                Ok(Err(std::io::Error::other("injected")))
            } else {
                let flag = Arc::clone(&cancel);
                tokio::task::spawn_blocking(move || sha256_of(&path, &flag)).await
            };
            let Some(inner) = weak.upgrade() else {
                return;
            };
            // The result, or the wait before the next try, is recorded
            // before the copy can be hashed again.
            let recorded = match hashed {
                Ok(Ok(Some(sha256))) => match inner.store.set_quarantine_sha256(&id, &sha256).await
                {
                    Ok(true) => {
                        inner.lock_retry().remove(&id);
                        info!(quarantine_id = %id, sha256 = %sha256, "hashed a quarantined copy");
                        true
                    }
                    // Deleted meanwhile.
                    Ok(false) => {
                        debug!(quarantine_id = %id, "a quarantined copy went during its hash");
                        false
                    }
                    Err(e) => {
                        let wait = inner.failed(&id);
                        warn!(quarantine_id = %id, error = %e, retry_in = ?wait, "could not record a quarantined copy's hash");
                        false
                    }
                },
                Ok(Ok(None)) => {
                    debug!(quarantine_id = %id, "the hash of a deleted quarantined copy stopped");
                    false
                }
                // Deleted before its hash, a clone shown to be gone, whose
                // record goes, or a clone missing while its record stays: the
                // last is tried again, and hashed once the file is back. Until
                // then the primary does not record the copy.
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    match inner.forget_lost_clone(&id).await {
                        Ok(true) => {
                            inner.lock_retry().remove(&id);
                            warn!(
                                quarantine_id = %id,
                                volume_id = %volume_id,
                                snapshot_id = %snapshot_id,
                                "the clone of a quarantined copy is gone before it was hashed; its record and kept snapshot are forgotten"
                            );
                            true
                        }
                        Ok(false) => {
                            let wait = inner.failed(&id);
                            warn!(quarantine_id = %id, retry_in = ?wait, "a quarantined copy has no file to hash; the primary records it only once it is hashed");
                            false
                        }
                        Err(e) => {
                            let wait = inner.failed(&id);
                            warn!(quarantine_id = %id, error = %e, retry_in = ?wait, "a quarantined copy has no file to hash, and whether its clone is gone cannot be told");
                            false
                        }
                    }
                }
                Ok(Err(e)) => {
                    let wait = inner.failed(&id);
                    warn!(quarantine_id = %id, error = %e, retry_in = ?wait, "could not hash a quarantined copy");
                    false
                }
                Err(e) => {
                    let wait = inner.failed(&id);
                    warn!(quarantine_id = %id, error = %e, retry_in = ?wait, "the hash of a quarantined copy failed");
                    false
                }
            };
            inner.release_hash(&id, &cancel);
            if recorded && let Err(e) = inner.publish().await {
                warn!(error = %e, "could not read the quarantined copies; the next pass reports them");
            }
        });
    }
}

impl<E: Engine> Inner<E> {
    /// Forgets the record of copy `id`, with its kept snapshot, when its
    /// clone is gone before it was hashed (section 9 step 2): the engine
    /// answers `NotFound` for the clone, another kept snapshot of the volume
    /// answers it, and the record has no SHA-256. Touches no file. Returns
    /// whether the record went.
    ///
    /// The engine's answers are then checked against one open handle of the
    /// clone's directory: the clone is missing from it, the other kept
    /// snapshot is a regular file in it, and the clone is still missing. A
    /// volume directory that mounts or unmounts meanwhile cannot split those
    /// three reads. A copy with no other kept snapshot in that directory is
    /// never forgotten, and each record kept for a reason other than a hash
    /// gets a warning that names it.
    pub(super) async fn forget_lost_clone(&self, id: &str) -> Result<bool, String> {
        let record = match self.store.volume_quarantine(id).await {
            Ok(Some(record)) if record.sha256.is_none() => record,
            // Deleted, or hashed meanwhile.
            Ok(_) => return Ok(false),
            Err(e) => return Err(format!("cannot read the record: {e}")),
        };
        let kept = match self.store.kept_snapshot(&record.snapshot_id).await {
            Ok(Some(kept)) => kept,
            // Recorded together, so not expected: nothing to look the clone
            // up by.
            Ok(None) => return Ok(false),
            Err(e) => return Err(format!("cannot read the kept snapshot: {e}")),
        };
        let kept_because = |reason: &str| -> Result<bool, String> {
            warn!(
                quarantine_id = %id,
                volume_id = %record.volume_id,
                reason,
                "the clone of an unhashed quarantined copy is not shown to be gone; its record stays"
            );
            Ok(false)
        };
        match self
            .engine
            .volume_changed_ranges(&RangesOf::Snapshot(kept.snapshot), None)
            .await
        {
            Err(EngineError::NotFound) => {}
            Ok(_) => return kept_because("the engine still names the clone"),
            // `Unsupported` from a pool that does not mount among them.
            Err(e) => return kept_because(&format!("the engine cannot look the clone up: {e}")),
        }
        let Some(other) = present_volume_file(
            self.engine.as_ref(),
            &self.store,
            &record.volume_id,
            Some(&record.snapshot_id),
        )
        .await?
        else {
            return kept_because("no other kept snapshot of the volume answers the engine");
        };
        #[cfg(test)]
        if let Some(between) = self
            .between_checks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            between();
        }
        if let Err(reason) = clone_missing_beside(&record.path, &other) {
            return kept_because(&reason);
        }
        self.store
            .forget_unhashed_volume_quarantine(id, &record.snapshot_id)
            .await
            .map_err(|e| format!("cannot forget the record: {e}"))
    }

    /// Reads the records and publishes their entries when they changed.
    /// Returns the records read.
    async fn publish(&self) -> Result<Vec<VolumeQuarantineRecord>, String> {
        let _publishing = self.publishing.lock().await;
        let records = self
            .store
            .volume_quarantines()
            .await
            .map_err(|e| format!("cannot read the quarantined copies: {e}"))?;
        let statuses: Vec<QuarantineStatus> = records.iter().map(status).collect();
        self.tx.send_if_modified(|current| {
            if *current == statuses {
                false
            } else {
                *current = statuses;
                true
            }
        });
        Ok(records)
    }

    fn lock_retry(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Instant, Duration)>> {
        self.retry_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Ends the hash of `id` that `cancel` stops, unless a delete ended it
    /// first; another hash of the copy may start from then on.
    fn release_hash(&self, id: &str, cancel: &Arc<AtomicBool>) {
        let mut hashing = self
            .hashing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if hashing
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, cancel))
        {
            hashing.remove(id);
        }
    }

    /// Records a failed hash of `id`; returns how long its next try waits.
    fn failed(&self, id: &str) -> Duration {
        let mut retry_at = self.lock_retry();
        let wait = retry_at.get(id).map_or(self.retry_first, |(_, wait)| *wait);
        retry_at.insert(
            id.to_owned(),
            (Instant::now() + wait, (wait * 2).min(HASH_RETRY_MAX)),
        );
        wait
    }
}

/// Whether a file the node records for `volume_id`, other than the kept
/// snapshot `except`, is present: a kept snapshot of the volume that is on
/// disk and that the engine answers for. It shows the volume directory to be
/// the real one (section 9).
///
/// # Errors
///
/// The store's error when the kept snapshots cannot be read.
pub(super) async fn volume_file_present<E: Engine>(
    engine: &E,
    store: &NodeStore,
    volume_id: &str,
    except: Option<&str>,
) -> Result<bool, String> {
    Ok(present_volume_file(engine, store, volume_id, except)
        .await?
        .is_some())
}

/// The location of the file [`volume_file_present`] finds, if any.
///
/// # Errors
///
/// The store's error when the kept snapshots cannot be read.
async fn present_volume_file<E: Engine>(
    engine: &E,
    store: &NodeStore,
    volume_id: &str,
    except: Option<&str>,
) -> Result<Option<std::path::PathBuf>, String> {
    let kept = store
        .kept_snapshots_of(volume_id)
        .await
        .map_err(|e| format!("cannot read the kept snapshots: {e}"))?;
    for kept in kept {
        if Some(kept.snapshot.snapshot_id.as_str()) == except
            || std::fs::symlink_metadata(&kept.snapshot.location).is_err()
        {
            continue;
        }
        let location = kept.snapshot.location.clone();
        if engine
            .volume_changed_ranges(&RangesOf::Snapshot(kept.snapshot), None)
            .await
            .is_ok()
        {
            return Ok(Some(location));
        }
    }
    Ok(None)
}

/// Reads, through one handle of the clone's directory opened with
/// `O_DIRECTORY | O_NOFOLLOW`, that `clone` is missing from it, that `other`
/// is a regular file in it, and that `clone` is still missing. The error says
/// which read failed.
fn clone_missing_beside(clone: &Path, other: &Path) -> Result<(), String> {
    use rustix::fs::{AtFlags, FileType, Mode, OFlags, open, statat};
    let Some(dir) = clone.parent() else {
        return Err("the clone's path names no directory".to_owned());
    };
    if other.parent() != Some(dir) {
        return Err("no other kept snapshot of the volume is in the clone's directory".to_owned());
    }
    let (Some(clone_name), Some(other_name)) = (clone.file_name(), other.file_name()) else {
        return Err("the clone's or the other kept snapshot's path names no file".to_owned());
    };
    let dir = open(
        dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| format!("cannot open the volume directory: {e}"))?;
    let clone_missing = || match statat(&dir, clone_name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => Ok(()),
        Ok(_) => Err("the clone's file is in the volume directory".to_owned()),
        Err(e) => Err(format!("cannot read the clone's file: {e}")),
    };
    clone_missing()?;
    match statat(&dir, other_name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile => {}
        Ok(_) => return Err("the other kept snapshot is not a regular file".to_owned()),
        Err(e) => return Err(format!("cannot read the other kept snapshot: {e}")),
    }
    clone_missing()
}

/// The `status_report.quarantines` entry of a recorded copy (section 5.3).
#[must_use]
pub fn status(record: &VolumeQuarantineRecord) -> QuarantineStatus {
    QuarantineStatus {
        quarantine_id: record.quarantine_id.clone(),
        volume_id: record.volume_id.clone(),
        app_id: record.app_id.clone(),
        created_at: record.created_at.clone(),
        size_bytes: record.size_bytes,
        sha256: record.sha256.clone(),
    }
}

/// Bytes read or hashed between two looks at the stop flag.
const CHUNK: usize = 1 << 20;

/// The lowercase hex SHA-256 of the file at `path`, as `sha256sum` prints
/// it, or `None` once `cancel` is set. The holes that `SEEK_DATA` and
/// `SEEK_HOLE` report are hashed as zeros without being read. Blocking IO.
///
/// # Errors
///
/// An I/O error from the file, `NotFound` when it is gone.
pub fn sha256_of(path: &Path, cancel: &AtomicBool) -> std::io::Result<Option<String>> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; CHUNK];
    let mut at = 0_u64;
    let segments = data_segments(&file, 0, length)?;
    for (start, stop) in segments
        .into_iter()
        .chain(std::iter::once((length, length)))
    {
        // The hole before the segment.
        while at < start {
            if cancel.load(Ordering::Relaxed) {
                return Ok(None);
            }
            let n = usize::try_from((start - at).min(CHUNK as u64)).unwrap_or(CHUNK);
            hasher.update(&ZERO_CELL[..n]);
            at += n as u64;
        }
        while at < stop {
            if cancel.load(Ordering::Relaxed) {
                return Ok(None);
            }
            let n = usize::try_from((stop - at).min(CHUNK as u64)).unwrap_or(CHUNK);
            file.read_exact_at(&mut buffer[..n], at)?;
            hasher.update(&buffer[..n]);
            at += n as u64;
        }
    }
    Ok(Some(util::hex(&hasher.finalize())))
}

#[cfg(test)]
pub(super) mod tests {
    use std::io::Read as _;
    use std::os::unix::fs::FileExt as _;
    use std::sync::atomic::AtomicBool;

    use quoro_proto::wire::{NodeMessage, QuarantineStatus, RawFrame};
    use sha2::{Digest as _, Sha256};

    use super::sha256_of;
    use crate::store::VolumeQuarantineRecord;

    /// A scratch directory under the workspace `target/`, removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target")
                .join("quarantine-tests")
                .join(format!("{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The plain SHA-256 of every byte of the file, holes read as the zeros
    /// the filesystem returns: what `sha256sum` prints.
    pub(crate) fn plain_sha256(path: &std::path::Path) -> String {
        let mut file = std::fs::File::open(path).expect("open");
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            let n = file.read(&mut buffer).expect("read");
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        crate::util::hex(&hasher.finalize())
    }

    #[test]
    fn the_hash_of_a_sparse_file_is_the_plain_hash_of_its_bytes() {
        let scratch = Scratch::new("sparse");
        let path = scratch.0.join("copy");
        let file = std::fs::File::create(&path).expect("create");
        // Holes at the start, between the data and at the end; data that
        // crosses a chunk boundary, and data that ends the file.
        file.set_len(9 << 20).expect("size");
        file.write_all_at(&[7_u8; 5000], (1 << 20) - 100)
            .expect("write");
        file.write_all_at(&[1_u8; 3], 5 << 20).expect("write");
        let never = AtomicBool::new(false);
        let hashed = sha256_of(&path, &never)
            .expect("hash")
            .expect("not stopped");
        assert_eq!(hashed, plain_sha256(&path));
        assert_eq!(hashed.len(), 64);
        assert!(
            hashed
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        );

        file.write_all_at(&[2_u8; 10], (9 << 20) - 10)
            .expect("write");
        assert_eq!(
            sha256_of(&path, &never)
                .expect("hash")
                .expect("not stopped"),
            plain_sha256(&path),
            "data at the very end"
        );
        // An empty file is the hash of nothing, as `sha256sum` prints it.
        let empty = scratch.0.join("empty");
        std::fs::File::create(&empty).expect("create");
        assert_eq!(
            sha256_of(&empty, &never)
                .expect("hash")
                .expect("not stopped"),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn a_stopped_hash_gives_up_and_a_missing_file_is_not_found() {
        let scratch = Scratch::new("stopped");
        let path = scratch.0.join("copy");
        std::fs::File::create(&path)
            .expect("create")
            .set_len(4 << 20)
            .expect("size");
        let stopped = AtomicBool::new(true);
        assert_eq!(sha256_of(&path, &stopped).expect("no error"), None);
        let gone = sha256_of(&scratch.0.join("gone"), &AtomicBool::new(false))
            .expect_err("a missing file");
        assert_eq!(gone.kind(), std::io::ErrorKind::NotFound);
    }

    /// What the primary checks of each entry before it records one
    /// (`control-plane` `quarantines::flaw`): one entry it cannot read makes
    /// it drop the whole report.
    pub(crate) fn assert_primary_accepts(entry: &QuarantineStatus) {
        const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let rest = entry
            .quarantine_id
            .strip_prefix("qrn_")
            .unwrap_or_else(|| panic!("a qrn_ id: {entry:?}"));
        assert_eq!(rest.len(), 26, "{entry:?}");
        assert!(rest.bytes().all(|b| CROCKFORD.contains(&b)), "{entry:?}");
        if let Some(sha256) = &entry.sha256 {
            assert_eq!(sha256.len(), 64, "{entry:?}");
            assert!(
                sha256
                    .bytes()
                    .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
                "{entry:?}"
            );
        }
        assert_rfc3339(&entry.created_at);
        assert!(entry.size_bytes < (1 << 53), "{entry:?}");
        assert!(entry.volume_id.starts_with("vol_"), "{entry:?}");
        assert!(entry.app_id.starts_with("app_"), "{entry:?}");

        // The whole report decodes as the primary decodes it.
        let report = serde_json::json!({
            "applied_rev": 1,
            "vms": [],
            "routes": [],
            "capacity": {"cpus": 1, "memory_mb": 1, "disk_gb": 1},
            "allocated": {"cpus": 0, "memory_mb": 0, "disk_gb": 0},
            "disk_free_gb": 1.0,
            "quarantines": [serde_json::to_value(entry).expect("serialize")],
        });
        let text = serde_json::json!({
            "v": 1,
            "type": "status_report",
            "id": "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
            "body": report,
        })
        .to_string();
        let raw = RawFrame::parse(&text).expect("a frame");
        match NodeMessage::from_raw(&raw).expect("the primary decodes it") {
            NodeMessage::StatusReport(decoded) => {
                assert_eq!(decoded.quarantines, vec![entry.clone()]);
            }
            other => panic!("not a status report: {other:?}"),
        }
    }

    /// RFC 3339 as the primary parses it: a full date, `T`, a full time and
    /// `Z` or an offset.
    pub(crate) fn assert_rfc3339(text: &str) {
        let bytes = text.as_bytes();
        let digits = |range: std::ops::Range<usize>| {
            bytes
                .get(range)
                .is_some_and(|part| part.iter().all(u8::is_ascii_digit))
        };
        assert!(
            digits(0..4)
                && bytes.get(4) == Some(&b'-')
                && digits(5..7)
                && bytes.get(7) == Some(&b'-')
                && digits(8..10)
                && bytes.get(10) == Some(&b'T')
                && digits(11..13)
                && bytes.get(13) == Some(&b':')
                && digits(14..16)
                && bytes.get(16) == Some(&b':')
                && digits(17..19)
                && (text.ends_with('Z') || text.contains('+')),
            "{text} is not RFC 3339"
        );
        assert!(
            crate::util::parse_rfc3339_nanos(text).is_some(),
            "{text} is not RFC 3339"
        );
    }

    #[test]
    fn every_entry_passes_the_primary_s_checks_with_and_without_its_hash() {
        let mut record = VolumeQuarantineRecord {
            quarantine_id: crate::util::quarantine_id(),
            volume_id: "vol_01JD0WARM0000000000000001".to_owned(),
            app_id: "app_01JD0WARM0000000000000001".to_owned(),
            created_at: crate::util::now_rfc3339(),
            size_bytes: 1 << 30,
            path: std::path::PathBuf::from("/volumes/vol.snap-1"),
            snapshot_id: "snap-1".to_owned(),
            reason: "test".to_owned(),
            sha256: None,
        };
        let pending = super::status(&record);
        assert!(pending.sha256.is_none());
        let value = serde_json::to_value(&pending).expect("serialize");
        assert!(value.get("sha256").is_none(), "absent, not null: {value}");
        assert_primary_accepts(&pending);
        record.sha256 = Some("ab".repeat(32));
        assert_primary_accepts(&super::status(&record));
        // Many ids, each of the right form.
        for _ in 0..200 {
            record.quarantine_id = crate::util::quarantine_id();
            assert_primary_accepts(&super::status(&record));
        }
    }
}
