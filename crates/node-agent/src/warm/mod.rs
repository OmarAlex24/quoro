// SPDX-License-Identifier: AGPL-3.0-only
//! Warm replication (`node-agent-protocol.md` section 6.13, ADR 0172 to ADR
//! 0177).
//!
//! The node that holds a warm app's lease is the **source**: it snapshots the
//! volume every interval and ships what changed. The warm node is the
//! **target**: it holds the last complete copy. [`sender`] is the source
//! side, [`receiver`] the target side; [`frame`] is the stream both sides
//! speak. A source that comes back as the target hands its volume over to
//! the receiver first, keeping a live file it cannot prove unchanged as a
//! quarantined copy ([`quarantine`], section 9).
//!
//! Each volume has one lock ([`VolumeLocks`]): a stream holds it from its
//! checks to its answer, and anything else that replaces or removes the warm
//! copy (deleting it, keeping it as the base when the node becomes the
//! source, [`keep_copy_as_base`]) takes it first, so it never runs in the
//! middle of a stream.

pub mod frame;
pub mod quarantine;
pub mod receiver;
pub mod sender;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::os::unix::fs::FileExt as _;
use std::sync::{Arc, Mutex as StdMutex};

use quoro_backup::cells::{CELL, CellHashes, hash_cell, zero_cell_hash};
use quoro_engine::{Engine, VolumeId};
use quoro_proto::wire::WarmStatus;
use rustix::fs::SeekFrom;
use rustix::io::Errno;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};
use tracing::{info, warn};

use crate::kept::{self, KeepPurpose};
use crate::store::NodeStore;

/// One async lock per warm volume, by volume id.
#[derive(Clone, Default)]
pub struct VolumeLocks {
    locks: Arc<StdMutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl VolumeLocks {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_of(&self, volume_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(locks.entry(volume_id.to_owned()).or_default())
    }

    /// The volume's lock, or `None` while someone else holds it: a second
    /// stream of a volume, or one that arrives while its copy is resolved,
    /// is refused `busy` (section 6.13).
    #[must_use]
    pub fn try_lock(&self, volume_id: &str) -> Option<OwnedMutexGuard<()>> {
        self.lock_of(volume_id).try_lock_owned().ok()
    }

    /// Waits for the volume's lock.
    pub async fn lock(&self, volume_id: &str) -> OwnedMutexGuard<()> {
        self.lock_of(volume_id).lock_owned().await
    }
}

/// What [`keep_copy_as_base`] did with a volume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyAsBase {
    /// The node holds no warm copy of the volume, so there is nothing to
    /// keep: the volume is a plain one.
    NoCopyRow,
    /// The copy is kept as the source's base and its row is gone, so the
    /// volume file is the node's own from now on.
    Kept,
    /// The copy was left pending by a crash or a failed swap and is not
    /// resolved yet (section 6.13, "Applying"). The warm receiver resolves
    /// it; the next pass tries again.
    Resolving,
    /// A step failed, with this reason; the next pass tries again.
    Failed(String),
    /// The node holds no complete copy, so it has nothing to start the app
    /// on: its copy never completed, or it was removed.
    NoCompleteCopy,
}

/// Keeps the warm copy of `volume_id` as the source's base, for a warm node
/// that a failover or a switchover makes the source (section 6.13,
/// "Failover", ADR 0175).
///
/// The copy file is already the volume file, so the reconciler adopts it as
/// the node's own volume. Before it does, and so before any VM writes to it,
/// this takes a snapshot of the copy, numbered with the copy's
/// `snapshot_seq` and carrying its cell hashes, and records it as the warm
/// base ([`kept::keep`]). The node then numbers its own snapshots after that
/// copy, and the base is the common point the returning old source resyncs
/// from (section 9). Last, the copy's row goes: the file is no longer the
/// warm receiver's.
///
/// A crash between the steps is safe. A snapshot with no record is swept at
/// the next start, and a recorded base of that number is not taken again.
/// Once the row is gone, the volume file holds the copy, which
/// `ensure_volume` keeps.
///
/// The caller holds the volume's lock, so no stream or resolution of the
/// copy runs meanwhile.
pub async fn keep_copy_as_base<E: Engine>(
    engine: &E,
    store: &NodeStore,
    volume_id: &str,
) -> CopyAsBase {
    let record = match store.warm_target(volume_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return CopyAsBase::NoCopyRow,
        Err(e) => return CopyAsBase::Failed(format!("cannot read the warm copy: {e}")),
    };
    if record.pending.is_some() {
        return CopyAsBase::Resolving;
    }
    let Some(seq) = record.complete_seq.filter(|_| !record.removed) else {
        return CopyAsBase::NoCompleteCopy;
    };
    let kept_already = match store.kept_snapshots_of(volume_id).await {
        // The number alone is not enough: a base the node kept as an
        // earlier source can carry the same number for other data.
        Ok(records) => records.iter().any(|kept| {
            kept.purpose == KeepPurpose::WarmBase.as_str()
                && kept.seq == Some(seq)
                && kept.cells == record.cells
        }),
        Err(e) => return CopyAsBase::Failed(format!("cannot read the kept snapshots: {e}")),
    };
    if !kept_already {
        let snapshot = match engine
            .snapshot_volume(&VolumeId(volume_id.to_owned()))
            .await
        {
            Ok(snapshot) => snapshot,
            Err(e) => return CopyAsBase::Failed(format!("cannot snapshot the warm copy: {e}")),
        };
        if let Err(e) = kept::keep(
            store,
            &record.app_id,
            &snapshot,
            KeepPurpose::WarmBase,
            Some(seq),
            record.cells.clone(),
        )
        .await
        {
            if let Err(de) = engine.delete_volume_snapshot(&snapshot).await {
                warn!(volume_id, error = %de, "could not delete an unrecorded snapshot of the warm copy");
            }
            return CopyAsBase::Failed(format!("cannot record the base: {e}"));
        }
        info!(
            volume_id,
            seq, "kept the warm copy as the base before starting the app on it"
        );
    }
    if let Err(e) = store.forget_warm_target(volume_id).await {
        return CopyAsBase::Failed(format!("cannot forget the warm copy's row: {e}"));
    }
    CopyAsBase::Kept
}

/// What `status_report.warm` reports (section 5.3): the target side's
/// entries and, from the sender, the source side's, merged.
#[derive(Clone)]
pub struct StatusBoard {
    parts: Arc<StdMutex<BTreeMap<&'static str, Vec<WarmStatus>>>>,
    tx: Arc<watch::Sender<Vec<WarmStatus>>>,
}

impl StatusBoard {
    /// The board and the receiver the session reads.
    #[must_use]
    pub fn new() -> (Self, watch::Receiver<Vec<WarmStatus>>) {
        let (tx, rx) = watch::channel(Vec::new());
        (
            Self {
                parts: Arc::new(StdMutex::new(BTreeMap::new())),
                tx: Arc::new(tx),
            },
            rx,
        )
    }

    /// Replaces one side's entries and publishes the merged list, sorted by
    /// volume id and role.
    pub fn set(&self, part: &'static str, entries: Vec<WarmStatus>) {
        let mut parts = self
            .parts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        parts.insert(part, entries);
        let mut merged: Vec<WarmStatus> = parts.values().flatten().cloned().collect();
        merged.sort_by(|a, b| {
            (
                a.volume_id.as_str(),
                a.role == quoro_proto::wire::WarmRole::Target,
            )
                .cmp(&(
                    b.volume_id.as_str(),
                    b.role == quoro_proto::wire::WarmRole::Target,
                ))
        });
        self.tx.send_if_modified(|current| {
            if *current == merged {
                false
            } else {
                *current = merged;
                true
            }
        });
    }
}

/// The data segments of `file` inside `start..end`, from `SEEK_DATA` and
/// `SEEK_HOLE`, sorted; what lies between them reads as zeros. A filesystem
/// that cannot tell reports everything as data. Blocking IO.
///
/// # Errors
///
/// An I/O error from the file.
pub fn data_segments(file: &File, start: u64, end: u64) -> std::io::Result<Vec<(u64, u64)>> {
    let mut segments = Vec::new();
    let mut offset = start;
    while offset < end {
        let data = match rustix::fs::seek(file, SeekFrom::Data(offset)) {
            Ok(data) => data,
            Err(Errno::NXIO) => break,
            Err(e) => return Err(e.into()),
        };
        if data >= end {
            break;
        }
        let stop = match rustix::fs::seek(file, SeekFrom::Hole(data)) {
            Ok(hole) if hole > data => hole.min(end),
            // A hole at the data offset is not the truth; read the rest.
            Ok(_) | Err(Errno::NXIO) => end,
            Err(e) => return Err(e.into()),
        };
        segments.push((data, stop));
        offset = stop;
    }
    Ok(segments)
}

/// One cell of zeros to compare with.
static ZERO_CELL: [u8; CELL as usize] = [0; CELL as usize];

/// Whether `bytes` are all zeros. One slice comparison per cell, which is a
/// `memcmp`: many times faster than a byte-by-byte scan, above all in a debug
/// build.
pub fn is_zero(bytes: &[u8]) -> bool {
    bytes
        .chunks(ZERO_CELL.len())
        .all(|chunk| chunk == &ZERO_CELL[..chunk.len()])
}

/// The cell hashes of a copy of `size` bytes, reading only the cells its
/// data reaches ([`data_segments`]): a hole reads as zeros, so a cell no
/// data reaches has the zero cell's hash, and so does a cell of zeros, which
/// is not hashed again. A filesystem that reports holes as data then costs a
/// read of the copy, not a hash of every byte of it. Blocking IO.
///
/// # Errors
///
/// An I/O error from the file, and `UnexpectedEof` when it is shorter than
/// `size`.
pub fn hash_copy(file: &File, size: u64) -> std::io::Result<CellHashes> {
    let mut cells = CellHashes::zeroed(size)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // A file shorter than the copy is not the copy, holes or not.
    if file.metadata()?.len() < size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("the copy is shorter than {size} bytes"),
        ));
    }
    let mut with_data: BTreeSet<u64> = BTreeSet::new();
    for (start, stop) in data_segments(file, 0, size)? {
        with_data.extend(start / CELL..stop.div_ceil(CELL));
    }
    let mut buf = vec![0_u8; ZERO_CELL.len()];
    for cell in with_data {
        file.read_exact_at(&mut buf, cell * CELL)?;
        let hash = if is_zero(&buf) {
            zero_cell_hash()
        } else {
            hash_cell(&buf)
        };
        cells
            .set(cell, hash)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    }
    Ok(cells)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt as _;

    use quoro_backup::cells::{CELL, CellHashes};

    use super::{VolumeLocks, hash_copy};

    #[test]
    fn hashing_only_the_data_of_a_sparse_copy_matches_hashing_every_cell() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .join("warm-hash-tests")
            .join(std::process::id().to_string());
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("copy");
        let size = 64 * CELL;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");
        file.set_len(size).expect("size");
        assert_eq!(
            hash_copy(&file, size).expect("hash").root_hex(),
            CellHashes::zeroed(size).expect("cells").root_hex(),
            "an empty sparse file is all zero cells"
        );
        file.write_all_at(&[1_u8; 10], 0).expect("write");
        file.write_all_at(&[2_u8; 5000], 7 * CELL - 100)
            .expect("write");
        file.write_all_at(&[3_u8; 1], size - 1).expect("write");
        // Written zeros are data, and hash like a hole.
        file.write_all_at(&vec![0_u8; CELL as usize], 20 * CELL)
            .expect("write");
        let mut reader = &file;
        let every = CellHashes::compute(&mut reader, size).expect("every cell");
        assert!(
            hash_copy(&file, size).expect("hash") == every,
            "the same hashes"
        );

        file.set_len(size - CELL).expect("shorter");
        assert!(
            hash_copy(&file, size).is_err(),
            "a short file is not the copy"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_held_volume_lock_refuses_a_second_holder_until_it_is_released() {
        let locks = VolumeLocks::new();
        let first = locks.try_lock("vol_a").expect("free");
        assert!(locks.try_lock("vol_a").is_none(), "held");
        assert!(locks.try_lock("vol_b").is_some(), "another volume is free");
        drop(first);
        assert!(locks.try_lock("vol_a").is_some(), "released");
    }
}
