// SPDX-License-Identifier: AGPL-3.0-only
//! Warm replication (`node-agent-protocol.md` section 6.13, ADR 0172 to ADR
//! 0177).
//!
//! The node that holds a warm app's lease is the **source**: it snapshots the
//! volume every interval and ships what changed. The warm node is the
//! **target**: it holds the last complete copy. [`sender`] is the source
//! side, [`receiver`] the target side; [`frame`] is the stream both sides
//! speak.
//!
//! Each volume has one lock ([`VolumeLocks`]): a stream holds it from its
//! checks to its answer, and anything else that replaces or removes the warm
//! copy (deleting it, keeping it as the base when the node becomes the
//! source) takes it first, so it never runs in the middle of a stream.

pub mod frame;
pub mod receiver;
pub mod sender;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::sync::{Arc, Mutex as StdMutex};

use quoro_backup::cells::{CELL, CellHashes};
use quoro_proto::wire::WarmStatus;
use rustix::fs::SeekFrom;
use rustix::io::Errno;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

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
    /// stream of a volume is refused `busy` (section 6.13).
    #[must_use]
    pub fn try_lock(&self, volume_id: &str) -> Option<OwnedMutexGuard<()>> {
        self.lock_of(volume_id).try_lock_owned().ok()
    }

    /// Waits for the volume's lock.
    pub async fn lock(&self, volume_id: &str) -> OwnedMutexGuard<()> {
        self.lock_of(volume_id).lock_owned().await
    }
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

/// The cell hashes of a copy of `size` bytes, reading only the cells its
/// data reaches ([`data_segments`]): a hole reads as zeros, so a cell no
/// data reaches has the zero cell's hash. Blocking IO.
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
    let mut reader = file;
    cells.rehash(&mut reader, with_data)?;
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
