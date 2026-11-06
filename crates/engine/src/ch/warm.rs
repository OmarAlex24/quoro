// SPDX-License-Identifier: AGPL-3.0-only
//! Warm volumes: changed ranges and staging (engine-trait.md §3.22, ADR 0172,
//! ADR 0173).
//!
//! `volume_changed_ranges` reads the extent maps of two reflink clones of one
//! volume file with `FS_IOC_FIEMAP` and hands them to
//! [`crate::extents::changed_ranges`]. Both files stay open for the whole
//! comparison, so the blocks the two maps name cannot be freed and reused in
//! between. When there is no base, or the maps cannot be compared (another
//! filesystem, FIEMAP unsupported, a map that does not read as one), the
//! answer is every allocated range of the newer file, from `SEEK_DATA` and
//! `SEEK_HOLE`, with `full: true`.
//!
//! The staging file a target writes a received copy into is
//! `<volume_dir>/<volume id>.staging`, next to the volume file so the commit
//! is one rename on one filesystem. A crash leaves at most that file, which the
//! start-up sweep removes ([`super::volume::remove_leftovers`]).

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use rustix::fs::SeekFrom;
use rustix::io::Errno;

use super::engine::{Inner, create_work_disk_file, io_error, lock};
use super::pool::VolumePool;
use super::volume::{
    METADATA_MARGIN, SNAPSHOT_SUFFIX, available_bytes, reflink, sync_file, validate_volume_id,
    volume_info, volume_path,
};
use crate::ch::state;
use crate::extents::{self, Extent};
use crate::{
    ByteRange, ChangedRanges, EngineError, RangesOf, SnapshotMethod, StageFrom, StagedVolume, VmId,
    VolumeId, VolumeInfo, VolumeSnapshot,
};

/// The suffix of a volume's staging file (§3.22).
pub(super) const STAGING_SUFFIX: &str = ".staging";

/// The staging file of one volume, `<volume_dir>/<volume id>.staging`.
pub(super) fn staging_path(dir: &Path, id: &VolumeId) -> PathBuf {
    dir.join(format!("{}{STAGING_SUFFIX}", id.0))
}

/// Checks that `snapshot` names a snapshot file of its own volume inside the
/// volume directory, so no call reads or clones a file elsewhere.
///
/// # Errors
///
/// `Conflict` for an invalid volume id, a location outside `dir`, or a file
/// name that is not `<volume id>.….snapshot`.
pub(super) fn check_snapshot_file(
    dir: &Path,
    snapshot: &VolumeSnapshot,
) -> Result<(), EngineError> {
    validate_volume_id(&snapshot.volume_id)?;
    let name = snapshot
        .location
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let prefix = format!("{}.", snapshot.volume_id.0);
    if snapshot.location.parent() != Some(dir)
        || !name.starts_with(&prefix)
        || !name.ends_with(SNAPSHOT_SUFFIX)
    {
        return Err(EngineError::Conflict(format!(
            "{} is not a snapshot of volume {}",
            snapshot.location.display(),
            snapshot.volume_id.0
        )));
    }
    Ok(())
}

/// Opens an existing regular file for reading.
///
/// # Errors
///
/// `NotFound` for a missing file, `Conflict` for anything but a regular file.
fn open_regular(path: &Path) -> Result<File, EngineError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(EngineError::Conflict(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::NotFound);
        }
        Err(err) => return Err(io_error(path, &err)),
    }
    File::open(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            EngineError::NotFound
        } else {
            io_error(path, &err)
        }
    })
}

/// True when a FIEMAP error means the filesystem or the request is not
/// supported, which falls back to a full copy rather than failing the cycle.
fn fiemap_unsupported(err: &std::io::Error) -> bool {
    matches!(
        Errno::from_io_error(err),
        Some(Errno::OPNOTSUPP | Errno::NOTTY | Errno::INVAL | Errno::BADR)
    )
}

/// The extent map of `file`, read with `FIEMAP_FLAG_SYNC` so dirty data is
/// written and mapped first; `None` when the filesystem does not report one.
fn read_extent_map(file: &File, path: &Path) -> Result<Option<Vec<Extent>>, EngineError> {
    let mut map = Vec::new();
    for extent in fiemap::Fiemap::with_flags(file, fiemap::FiemapFlags::SYNC) {
        match extent {
            Ok(extent) => map.push(Extent::from_fiemap(
                extent.fe_logical,
                extent.fe_physical,
                extent.fe_length,
                extent.fe_flags.bits(),
            )),
            Err(err) if fiemap_unsupported(&err) => return Ok(None),
            Err(err) => return Err(io_error(path, &err)),
        }
    }
    Ok(Some(map))
}

/// Every allocated range of `file`, from `SEEK_DATA` and `SEEK_HOLE`, cut at
/// `size` (the full-copy fallback of §3.22).
fn allocated_ranges(file: &File, path: &Path, size: u64) -> Result<Vec<ByteRange>, EngineError> {
    let mut ranges = Vec::new();
    let mut offset = 0_u64;
    while offset < size {
        let data = match rustix::fs::seek(file, SeekFrom::Data(offset)) {
            Ok(data) => data,
            Err(Errno::NXIO) => break,
            Err(err) => return Err(io_error(path, &err.into())),
        };
        if data >= size {
            break;
        }
        let end = match rustix::fs::seek(file, SeekFrom::Hole(data)) {
            Ok(hole) => hole.min(size),
            Err(Errno::NXIO) => size,
            Err(err) => return Err(io_error(path, &err.into())),
        };
        if end <= data {
            // A filesystem that reports a hole at the data offset is not
            // telling the truth; treat the rest of the file as data.
            ranges.push(ByteRange {
                offset: data,
                length: size - data,
            });
            break;
        }
        ranges.push(ByteRange {
            offset: data,
            length: end - data,
        });
        offset = end;
    }
    Ok(ranges)
}

/// The comparison of §3.22, on blocking IO.
///
/// The newer file is opened first and the base second, and both stay open
/// until the comparison is done.
pub(super) fn changed_ranges_blocking(
    newer: &Path,
    base: Option<&Path>,
) -> Result<ChangedRanges, EngineError> {
    let newer_file = open_regular(newer)?;
    let newer_meta = newer_file.metadata().map_err(|err| io_error(newer, &err))?;
    let size = newer_meta.len();
    let newer_map = read_extent_map(&newer_file, newer)?;

    let compared = match base {
        None => None,
        Some(base) => {
            let base_file = open_regular(base)?;
            let base_meta = base_file.metadata().map_err(|err| io_error(base, &err))?;
            // Physical addresses mean the same only on one filesystem, and a
            // base of another length is not a clone of this file.
            if base_meta.dev() != newer_meta.dev() || base_meta.len() != size {
                None
            } else {
                let base_map = read_extent_map(&base_file, base)?;
                match (&newer_map, base_map) {
                    (Some(newer_map), Some(base_map)) => {
                        extents::changed_ranges(newer_map, &base_map, size)
                    }
                    _ => None,
                }
            }
        }
    };

    match compared {
        Some(ranges) => Ok(ChangedRanges {
            ranges,
            full: false,
            extent_count: newer_map.as_ref().map_or(0, |map| map.len() as u64),
        }),
        None => {
            let allocated = allocated_ranges(&newer_file, newer, size)?;
            let extent_count = newer_map
                .as_ref()
                .map_or(allocated.len() as u64, |map| map.len() as u64);
            Ok(ChangedRanges {
                ranges: extents::round_and_merge(allocated, size),
                full: true,
                extent_count,
            })
        }
    }
}

/// Creates the staging file at `location`: an empty sparse file of `size_gb`
/// GiB, or a reflink clone of `source` (§3.22).
///
/// A staging file that is already there is `Conflict` and is left alone; a
/// file this call created and could not sync is removed again, so a failed
/// call leaves nothing that would block the next one.
pub(super) fn create_staging(
    location: &Path,
    size_gb: u32,
    source: Option<&Path>,
) -> Result<(), EngineError> {
    let exists = || EngineError::Conflict(format!("{} already exists", location.display()));
    match source {
        None => {
            if !create_work_disk_file(location, size_gb)? {
                return Err(exists());
            }
            sync_file(location).inspect_err(|_| {
                let _ = std::fs::remove_file(location);
            })
        }
        Some(source) => {
            if std::fs::symlink_metadata(location).is_ok() {
                return Err(exists());
            }
            // `reflink` creates the clone with `create_new` and removes it
            // again when the clone or its sync fails.
            reflink(source, location)
        }
    }
}

/// Syncs the staging file, renames it over the volume file in one step and
/// syncs the directory, so after a crash the volume file is the old copy or
/// the new one (§3.22).
///
/// An error from the directory sync comes after the rename: the volume file
/// already is the new copy, though a power cut could still bring back the old
/// one. A caller treats any error of a commit as "the copy is one of the two"
/// and does not trust what it recorded about the old copy.
pub(super) fn commit_staging(location: &Path, volume: &Path) -> Result<(), EngineError> {
    sync_file(location)?;
    std::fs::rename(location, volume).map_err(|err| io_error(volume, &err))?;
    state::sync_dir(volume)
}

/// A file of `expected` bytes at `path`.
///
/// # Errors
///
/// `NotFound` when it is missing, `Conflict` when it is not a regular file or
/// has another length.
fn check_length(path: &Path, expected: u64) -> Result<(), EngineError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == expected => Ok(()),
        Ok(metadata) if metadata.file_type().is_file() => Err(EngineError::Conflict(format!(
            "{} is {} bytes, not {expected}",
            path.display(),
            metadata.len()
        ))),
        Ok(_) => Err(EngineError::Conflict(format!(
            "{} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(EngineError::NotFound),
        Err(err) => Err(io_error(path, &err)),
    }
}

/// Holds a volume file against `create` while a warm call reads or replaces
/// it: the path sits in `inflight_disks` under a holder id no VM can have, so
/// a concurrent create that would attach it gets `Conflict`, and the hold
/// itself was refused while a VM that exists attaches the volume.
///
/// The hold owns its handle on the map, so it can move into the blocking
/// task that reads or replaces the file: a caller that drops the call's
/// future does not release the hold while that task still runs.
pub(super) struct VolumeHold {
    inflight: Arc<StdMutex<HashMap<PathBuf, VmId>>>,
    holder: VmId,
    path: PathBuf,
}

impl Drop for VolumeHold {
    fn drop(&mut self) {
        let mut inflight = lock(&self.inflight);
        if inflight.get(&self.path) == Some(&self.holder) {
            inflight.remove(&self.path);
        }
    }
}

impl Inner {
    /// The VM that exists and attaches `id`, if any (§3.20, §3.22).
    pub(super) fn vm_attaching(&self, id: &VolumeId) -> Option<VmId> {
        self.entries().into_iter().find_map(|(vm_id, entry)| {
            let record = entry.spec()?;
            record
                .spec
                .volumes
                .iter()
                .any(|volume| volume.volume_id == id.0)
                .then_some(vm_id)
        })
    }

    /// Takes a [`VolumeHold`] on the file of `id` at `path`.
    ///
    /// # Errors
    ///
    /// `Conflict` while a VM that exists attaches the volume, or a create or
    /// another warm call holds the path.
    pub(super) fn hold_unattached(
        &self,
        id: &VolumeId,
        path: &Path,
    ) -> Result<VolumeHold, EngineError> {
        let holder = VmId(format!("warm:{}", id.0));
        {
            let mut inflight = lock(&self.inflight_disks);
            if let Some(owner) = inflight.get(path) {
                return Err(EngineError::Conflict(format!(
                    "volume {} is being attached to VM {} or is in use",
                    id.0, owner.0
                )));
            }
            inflight.insert(path.to_path_buf(), holder.clone());
        }
        let hold = VolumeHold {
            inflight: Arc::clone(&self.inflight_disks),
            holder,
            path: path.to_path_buf(),
        };
        if let Some(vm_id) = self.vm_attaching(id) {
            return Err(EngineError::Conflict(format!(
                "volume {} is attached to VM {}",
                id.0, vm_id.0
            )));
        }
        Ok(hold)
    }

    /// The ranges of `newer` that may differ from `base` (§3.22).
    pub(super) async fn volume_changed_ranges(
        &self,
        newer: &RangesOf,
        base: Option<&VolumeSnapshot>,
    ) -> Result<ChangedRanges, EngineError> {
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let (volume_id, newer_path) = match newer {
            RangesOf::Snapshot(snapshot) => {
                check_snapshot_file(&dir, snapshot)?;
                (snapshot.volume_id.clone(), snapshot.location.clone())
            }
            RangesOf::Volume(id) => {
                validate_volume_id(id)?;
                (id.clone(), volume_path(&dir, id))
            }
        };
        let base_path = match base {
            Some(base) if base.volume_id != volume_id => {
                return Err(EngineError::Conflict(format!(
                    "base snapshot {} is of volume {}, not {}",
                    base.snapshot_id, base.volume_id.0, volume_id.0
                )));
            }
            Some(base) => {
                check_snapshot_file(&dir, base)?;
                Some(base.location.clone())
            }
            None => None,
        };
        // The live file could change under the comparison if a VM used it.
        let hold = match newer {
            RangesOf::Volume(id) => Some(self.hold_unattached(id, &newer_path)?),
            RangesOf::Snapshot(_) => None,
        };
        tokio::task::spawn_blocking(move || {
            let _hold = hold;
            changed_ranges_blocking(&newer_path, base_path.as_deref())
        })
        .await
        .map_err(|err| EngineError::Io(format!("changed-ranges task failed: {err}")))?
    }

    /// Creates the staging file of one volume (§3.22).
    pub(super) async fn stage_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &StageFrom,
    ) -> Result<StagedVolume, EngineError> {
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let expected = u64::from(size_gb) << 30;
        let location = staging_path(&dir, id);
        match std::fs::symlink_metadata(&location) {
            Ok(_) => {
                return Err(EngineError::Conflict(format!(
                    "volume {} already has a staging file",
                    id.0
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error(&location, &err)),
        }
        let source = match from {
            StageFrom::Empty => None,
            StageFrom::Volume => Some(volume_path(&dir, id)),
            StageFrom::Snapshot(snapshot) => {
                if &snapshot.volume_id != id {
                    return Err(EngineError::Conflict(format!(
                        "snapshot {} is of volume {}, not {}",
                        snapshot.snapshot_id, snapshot.volume_id.0, id.0
                    )));
                }
                check_snapshot_file(&dir, snapshot)?;
                Some(snapshot.location.clone())
            }
        };
        if let Some(source) = &source {
            check_length(source, expected)?;
            if self.volume_method != SnapshotMethod::Reflink {
                return Err(EngineError::Unsupported(
                    "a staging clone needs reflink; this node has no warm volumes (ADR 0172)"
                        .to_owned(),
                ));
            }
        }

        // The pool must hold the copy and its staging file. A target never
        // runs `ensure_volume`, so the first copy sizes the pool here (§3.16).
        let _growing = match &self.pool {
            VolumePool::Mounted { .. } => Some(self.pool_grow.lock().await),
            VolumePool::Absent | VolumePool::Broken { .. } => None,
        };
        // An empty staging file receives a full copy next to the volume's,
        // so it needs a volume's room; a clone shares the blocks it copies.
        let needs_room = matches!(from, StageFrom::Empty) || !volume_path(&dir, id).exists();
        self.grow_pool_for(if needs_room { expected } else { 0 })
            .await?;
        let space = self.space_dir();
        let available = available_bytes(&space)?;
        if available < METADATA_MARGIN {
            return Err(EngineError::ResourceExhausted(format!(
                "{} has {available} bytes free, below the {METADATA_MARGIN} byte metadata margin",
                space.display()
            )));
        }

        let target = location.clone();
        tokio::task::spawn_blocking(move || create_staging(&target, size_gb, source.as_deref()))
            .await
            .map_err(|err| EngineError::Io(format!("staging task failed: {err}")))??;
        Ok(StagedVolume {
            volume_id: id.clone(),
            size_bytes: expected,
            location,
        })
    }

    /// Renames a staging file over its volume file (§3.22).
    pub(super) async fn commit_staged_volume(
        &self,
        staged: &StagedVolume,
    ) -> Result<VolumeInfo, EngineError> {
        let id = &staged.volume_id;
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let location = staging_path(&dir, id);
        if staged.location != location {
            return Err(EngineError::Conflict(format!(
                "{} is not the staging file of volume {}",
                staged.location.display(),
                id.0
            )));
        }
        let size_gb = u32::try_from(staged.size_bytes >> 30)
            .ok()
            .filter(|size_gb| u64::from(*size_gb) << 30 == staged.size_bytes)
            .ok_or_else(|| {
                EngineError::Conflict(format!(
                    "a staging file of {} bytes is not a whole number of GiB",
                    staged.size_bytes
                ))
            })?;
        check_length(&location, staged.size_bytes)?;
        let path = volume_path(&dir, id);
        let hold = self.hold_unattached(id, &path)?;
        let (from, to) = (location.clone(), path.clone());
        tokio::task::spawn_blocking(move || {
            let _hold = hold;
            commit_staging(&from, &to)
        })
        .await
        .map_err(|err| EngineError::Io(format!("commit task failed: {err}")))??;
        volume_info(id, size_gb, path)
    }

    /// Removes a staging file; a missing one is `Ok` (§3.22).
    pub(super) async fn discard_staged_volume(
        &self,
        staged: &StagedVolume,
    ) -> Result<(), EngineError> {
        let id = &staged.volume_id;
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let location = staging_path(&self.volumes_dir(), id);
        if staged.location != location {
            return Err(EngineError::Conflict(format!(
                "{} is not the staging file of volume {}",
                staged.location.display(),
                id.0
            )));
        }
        match std::fs::remove_file(&location) {
            Ok(()) => state::sync_dir(&location),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(&location, &err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    use super::{
        changed_ranges_blocking, check_snapshot_file, commit_staging, create_staging, staging_path,
    };
    use crate::ch::volume::{probe_method, reflink, volume_path};
    use crate::{ByteRange, EngineError, SnapshotMethod, VolumeId, VolumeSnapshot};

    const MIB: u64 = 1 << 20;
    const BLOCK: u64 = 4096;

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .join("warm-tests")
            .join(format!("{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// A directory that reflinks: `QUORO_TEST_REFLINK_DIR` when set (an XFS
    /// or btrfs mount), otherwise the scratch directory when it happens to
    /// reflink; `None` skips the test.
    fn reflink_dir(tag: &str) -> Option<PathBuf> {
        let dir = match std::env::var_os("QUORO_TEST_REFLINK_DIR") {
            Some(root) => {
                let dir = PathBuf::from(root).join(format!("ch-warm-{tag}-{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).expect("reflink dir");
                dir
            }
            None => scratch_dir(tag),
        };
        if probe_method(&dir) == SnapshotMethod::Reflink {
            Some(dir)
        } else {
            eprintln!(
                "skipping: {} does not reflink; set QUORO_TEST_REFLINK_DIR to an XFS or btrfs directory",
                dir.display()
            );
            let _ = std::fs::remove_dir_all(&dir);
            None
        }
    }

    fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        file.write_all(bytes).expect("write");
        file.sync_all().expect("sync");
    }

    fn read_all(path: &Path) -> Vec<u8> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .expect("open")
            .read_to_end(&mut bytes)
            .expect("read");
        bytes
    }

    fn sparse(path: &Path, size: u64) {
        std::fs::File::create(path)
            .and_then(|file| file.set_len(size))
            .expect("sparse file");
    }

    fn covers(ranges: &[ByteRange], offset: u64, length: u64) -> bool {
        (offset / BLOCK..(offset + length).div_ceil(BLOCK)).all(|block| {
            ranges.iter().any(|range| {
                range.offset <= block * BLOCK && block * BLOCK < range.offset + range.length
            })
        })
    }

    /// Copies the bytes of `ranges` from `from` into `to`, as a target applies
    /// a stream.
    fn apply(from: &Path, to: &Path, ranges: &[ByteRange]) {
        let mut source = std::fs::File::open(from).expect("open source");
        let mut target = std::fs::OpenOptions::new()
            .write(true)
            .open(to)
            .expect("open target");
        for range in ranges {
            let mut buffer = vec![0_u8; usize::try_from(range.length).expect("length")];
            source.seek(SeekFrom::Start(range.offset)).expect("seek");
            source.read_exact(&mut buffer).expect("read");
            target.seek(SeekFrom::Start(range.offset)).expect("seek");
            target.write_all(&buffer).expect("write");
        }
        target.sync_all().expect("sync");
    }

    /// A deterministic byte source for the test data.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn bytes(&mut self, length: usize) -> Vec<u8> {
            (0..length).map(|_| self.next() as u8).collect()
        }
    }

    #[test]
    fn without_a_base_every_allocated_range_is_reported_as_full() {
        // A filesystem without SEEK_HOLE support reports the whole file as
        // data, which is still a correct full copy; only a known reflink
        // filesystem is held to reporting just the data.
        let (dir, holes_known) = match reflink_dir("full") {
            Some(dir) => (dir, true),
            None => (scratch_dir("full"), false),
        };
        let file = dir.join("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF");
        sparse(&file, 16 * MIB);
        write_at(&file, 0, b"start");
        write_at(&file, 8 * MIB + 10, b"middle");
        let ranges = changed_ranges_blocking(&file, None).expect("ranges");
        assert!(ranges.full);
        assert!(covers(&ranges.ranges, 0, 5));
        assert!(covers(&ranges.ranges, 8 * MIB + 10, 6));
        let reported: u64 = ranges.ranges.iter().map(|range| range.length).sum();
        assert!(
            !holes_known || reported < 8 * MIB,
            "a sparse file reports its data, not its holes: {reported} bytes"
        );
        for range in &ranges.ranges {
            assert_eq!(range.offset % BLOCK, 0);
            assert_eq!(range.length % BLOCK, 0);
            assert!(range.offset + range.length <= 16 * MIB);
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_missing_file_is_not_found() {
        let dir = scratch_dir("missing");
        assert_eq!(
            changed_ranges_blocking(&dir.join("absent"), None),
            Err(EngineError::NotFound)
        );
        let file = dir.join("present");
        sparse(&file, MIB);
        assert_eq!(
            changed_ranges_blocking(&file, Some(&dir.join("absent"))),
            Err(EngineError::NotFound)
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_base_of_another_length_falls_back_to_a_full_copy() {
        let dir = scratch_dir("length");
        let newer = dir.join("newer");
        let base = dir.join("base");
        sparse(&newer, 2 * MIB);
        sparse(&base, MIB);
        write_at(&newer, 0, b"data");
        let ranges = changed_ranges_blocking(&newer, Some(&base)).expect("ranges");
        assert!(ranges.full);
        assert!(covers(&ranges.ranges, 0, 4));
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// The S1 procedure of `docs/research/m5b-warm-and-leases.md` at a small
    /// size: random writes to a live file, a clone per cycle, the changed
    /// ranges against the previous clone applied to a target copy, and the
    /// target compared byte for byte with the clone after every cycle.
    #[test]
    fn reflinked_snapshots_differ_only_where_the_file_was_written() {
        let Some(dir) = reflink_dir("cycles") else {
            return;
        };
        const SIZE: u64 = 64 * MIB;
        let mut rng = Rng(0x5157_4152_4d5f_0001);
        let live = dir.join("live");
        sparse(&live, SIZE);
        for chunk in 0..SIZE / MIB {
            if chunk % 3 != 0 {
                write_at(&live, chunk * MIB, &rng.bytes(MIB as usize));
            }
        }

        let mut base = dir.join("snap-0");
        reflink(&live, &base).expect("clone");
        let target = dir.join("target");
        reflink(&base, &target).expect("target copy");

        for cycle in 1..=20_u64 {
            let mut written = Vec::new();
            if cycle != 7 {
                for _ in 0..(rng.next() % 40) {
                    let offset = rng.next() % (SIZE - 70_000);
                    let length = match rng.next() % 10 {
                        0..=5 => 4096,
                        6..=8 => 1 + rng.next() % 65_536,
                        _ => 65_536,
                    };
                    write_at(&live, offset, &rng.bytes(length as usize));
                    written.push((offset, length));
                }
            }
            let next = dir.join(format!("snap-{cycle}"));
            reflink(&live, &next).expect("clone");
            let ranges = changed_ranges_blocking(&next, Some(&base)).expect("ranges");
            assert!(!ranges.full, "cycle {cycle}: the maps compare");
            assert!(ranges.extent_count > 0);
            for (offset, length) in &written {
                assert!(
                    covers(&ranges.ranges, *offset, *length),
                    "cycle {cycle}: the write at {offset} ({length} bytes) was missed"
                );
            }
            let written_blocks: u64 = written
                .iter()
                .map(|(offset, length)| (offset + length).div_ceil(BLOCK) - offset / BLOCK)
                .sum();
            let reported: u64 = ranges.ranges.iter().map(|range| range.length / BLOCK).sum();
            assert!(
                reported <= written_blocks + 64,
                "cycle {cycle}: {reported} blocks reported for {written_blocks} written"
            );
            if cycle == 7 {
                assert!(ranges.ranges.is_empty(), "an idle cycle changes nothing");
            }

            apply(&next, &target, &ranges.ranges);
            assert!(
                read_all(&target) == read_all(&next),
                "cycle {cycle}: the target differs from the snapshot"
            );
            std::fs::remove_file(&base).expect("drop the old base");
            base = next;
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn the_live_file_reports_writes_that_are_not_synced_yet() {
        // A buffered write into blocks the base shares stays in the page
        // cache until writeback; FIEMAP_FLAG_SYNC must flush it, or the map
        // still shows the shared block and the write is missed.
        let Some(dir) = reflink_dir("unsynced") else {
            return;
        };
        let live = dir.join("live");
        sparse(&live, 16 * MIB);
        write_at(&live, 0, &[7_u8; 4 * MIB as usize]);
        let base = dir.join("base");
        reflink(&live, &base).expect("clone");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&live)
            .expect("open");
        file.seek(SeekFrom::Start(MIB + 100)).expect("seek");
        file.write_all(b"not synced yet").expect("write");
        drop(file);
        let ranges = changed_ranges_blocking(&live, Some(&base)).expect("ranges");
        assert!(!ranges.full, "the maps compare");
        assert!(
            covers(&ranges.ranges, MIB + 100, 14),
            "the unsynced write is reported: {:?}",
            ranges.ranges
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_staging_file_that_is_already_there_is_left_alone() {
        let dir = scratch_dir("staging-exists");
        let id = VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string());
        let staging = staging_path(&dir, &id);
        std::fs::write(&staging, b"another stage").expect("staging file");
        let source = volume_path(&dir, &id);
        sparse(&source, 1 << 30);
        for from in [None, Some(source.as_path())] {
            assert!(
                matches!(
                    create_staging(&staging, 1, from),
                    Err(EngineError::Conflict(_))
                ),
                "{from:?}"
            );
            assert_eq!(
                std::fs::read(&staging).expect("read"),
                b"another stage",
                "the other stage's file is untouched"
            );
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn staging_starts_empty_or_from_a_clone_and_commits_in_one_rename() {
        let dir = scratch_dir("staging");
        let id = VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string());
        let staging = staging_path(&dir, &id);
        assert_eq!(
            staging.file_name().and_then(|name| name.to_str()),
            Some("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.staging")
        );
        create_staging(&staging, 1, None).expect("empty staging file");
        assert_eq!(std::fs::metadata(&staging).expect("staging").len(), 1 << 30);
        assert!(
            matches!(
                create_staging(&staging, 1, None),
                Err(EngineError::Conflict(_))
            ),
            "a second staging file is refused"
        );
        write_at(&staging, 4096, b"copy one");
        let volume = dir.join(&id.0);
        commit_staging(&staging, &volume).expect("commit");
        assert!(!staging.exists());
        assert_eq!(&read_all(&volume)[4096..4104], b"copy one");

        if probe_method(&dir) == SnapshotMethod::Reflink {
            create_staging(&staging, 1, Some(&volume)).expect("clone the copy");
            assert_eq!(&read_all(&staging)[4096..4104], b"copy one");
            write_at(&staging, 4096, b"copy two");
            assert_eq!(
                &read_all(&volume)[4096..4104],
                b"copy one",
                "the copy is unchanged before the commit"
            );
            commit_staging(&staging, &volume).expect("commit");
            assert_eq!(&read_all(&volume)[4096..4104], b"copy two");
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_snapshot_must_be_a_snapshot_file_of_its_own_volume() {
        let dir = PathBuf::from("/var/lib/quoro/engine/volumes");
        let id = VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string());
        let snapshot = |location: PathBuf, volume_id: &VolumeId| VolumeSnapshot {
            volume_id: volume_id.clone(),
            snapshot_id: "s".to_string(),
            created_at: SystemTime::now(),
            size_bytes: 1 << 30,
            location,
            method: SnapshotMethod::Reflink,
            pause: std::time::Duration::ZERO,
        };
        let good = dir.join(format!("{}.19a2b3c4d5e-0.snapshot", id.0));
        assert_eq!(
            check_snapshot_file(&dir, &snapshot(good.clone(), &id)),
            Ok(())
        );
        let legacy = dir.join(format!("{}.snapshot", id.0));
        assert_eq!(check_snapshot_file(&dir, &snapshot(legacy, &id)), Ok(()));
        for location in [
            PathBuf::from("/etc/passwd"),
            dir.join(&id.0),
            dir.join(format!("{}.staging", id.0)),
            dir.join("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EG.1-0.snapshot"),
            PathBuf::from("/tmp").join(format!("{}.1-0.snapshot", id.0)),
        ] {
            assert!(
                matches!(
                    check_snapshot_file(&dir, &snapshot(location.clone(), &id)),
                    Err(EngineError::Conflict(_))
                ),
                "{} is not a snapshot of the volume",
                location.display()
            );
        }
        let bad_id = VolumeId("../vol".to_string());
        assert!(matches!(
            check_snapshot_file(&dir, &snapshot(good, &bad_id)),
            Err(EngineError::Conflict(_))
        ));
    }
}
