// SPDX-License-Identifier: AGPL-3.0-only
//! Local volumes and their snapshots (engine-trait.md §3.16, §3.20; ADR 0145,
//! ADR 0156).
//!
//! A volume is one sparse raw file, named by the volume id, in
//! `<state_dir>/volumes/`, owned by root and never mounted on the host. The
//! engine checks and formats it with `mkfs.ext4` on the file itself, attaches
//! it as a writable virtio-blk disk and snapshots it by `FICLONE` when the
//! node's filesystem reflinks, or by pausing the using VM, copying the sparse
//! file and resuming it otherwise. The method is probed once at start-up, in
//! the volume directory itself or inside the loop-mounted XFS volume pool
//! [`super::pool`] puts there on a filesystem that does not reflink.
//!
//! [`Inner`]'s volume methods live here so `engine.rs` stays a lifecycle
//! module; they share its locks and its record view through the
//! `pub(super)` items there.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use rustix::fs::SeekFrom;
use rustix::io::Errno;

use super::api::ApiClient;
use super::engine::{Inner, VmEntry, create_work_disk_file, io_error, lock};
use super::pool::VolumePool;
use super::vmm::ProcessId;
use crate::ch::state;
use crate::ch::tools;
use crate::{EngineError, SnapshotMethod, VmId, VmSpec, VolumeId, VolumeInfo, VolumeSnapshot};

/// Free bytes the volume directory keeps for the filesystem's metadata: a
/// sparse file itself needs almost nothing up front, but `mkfs.ext4` writes
/// inode and journal metadata into it (contract §3.20).
pub(super) const METADATA_MARGIN: u64 = 64 * 1024 * 1024;

/// Bytes copied per `copy_file_range` call. Large enough to keep the syscall
/// count low, small enough that a resume never waits on one huge call.
const COPY_CHUNK: u64 = 4 * 1024 * 1024;

/// The suffix of every snapshot file (contract §3.16). From M5b a volume may
/// have several, `<volume id>.<unique part>.snapshot`; M5a named its one
/// snapshot `<volume id>.snapshot`.
pub(super) const SNAPSHOT_SUFFIX: &str = ".snapshot";

/// Snapshots taken by this process, so two in one millisecond get different
/// names (§3.16).
static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The directory that holds every volume file.
pub(super) fn volume_dir(inner: &Inner) -> PathBuf {
    inner.config.state_dir.join("volumes")
}

/// The file of one volume, `<volume_dir>/<volume id>` (contract §3.20).
pub(super) fn volume_path(dir: &Path, id: &VolumeId) -> PathBuf {
    dir.join(&id.0)
}

/// The one snapshot file M5a gave a volume, `<volume_dir>/<volume id>.snapshot`.
pub(super) fn legacy_snapshot_path(dir: &Path, id: &VolumeId) -> PathBuf {
    dir.join(format!("{}{SNAPSHOT_SUFFIX}", id.0))
}

/// A new snapshot file name for one volume that no file in `dir` has:
/// `<volume id>.<milliseconds since the epoch, hex>-<counter, hex>.snapshot`.
/// The time keeps names apart across restarts; the counter within one.
pub(super) fn new_snapshot_path(dir: &Path, id: &VolumeId) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    loop {
        let count = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("{}.{millis:x}-{count:x}{SNAPSHOT_SUFFIX}", id.0));
        if !path.exists() {
            return path;
        }
    }
}

/// `vol_` followed by 26 Crockford base32 characters (the ULID alphabet).
///
/// # Errors
///
/// `Conflict("invalid volume id …")` for anything else, so a hostile id can
/// never name a path outside the volume directory.
pub(super) fn validate_volume_id(id: &VolumeId) -> Result<(), EngineError> {
    let text = id.0.as_str();
    let invalid =
        |reason: &str| EngineError::Conflict(format!("invalid volume id {:?}: {reason}", text));
    let Some(ulid) = text.strip_prefix("vol_") else {
        return Err(invalid("it does not start with 'vol_'"));
    };
    if ulid.len() != 26 {
        return Err(invalid("the ULID is not 26 characters"));
    }
    if !ulid.bytes().all(is_crockford) {
        return Err(invalid(
            "the ULID may only contain 0-9 and A-Z without I, L, O and U",
        ));
    }
    Ok(())
}

/// True for one Crockford base32 character: `[0-9A-HJKMNP-TV-Z]`.
fn is_crockford(byte: u8) -> bool {
    matches!(
        byte,
        b'0'..=b'9' | b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z'
    )
}

/// The serials a volume may carry: `quoro-vol-<n>` (contract §3.6).
fn is_volume_serial(serial: &str) -> bool {
    match serial.strip_prefix("quoro-vol-") {
        Some(number) => !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()),
        None => false,
    }
}

/// Checks the volume attachments of one spec (contract §3.6).
///
/// Every serial is `quoro-vol-<n>` and no two volumes of the spec share one.
///
/// # Errors
///
/// `Conflict` naming the volume and the rule it breaks.
pub(super) fn validate_volume_attaches(spec: &VmSpec) -> Result<(), EngineError> {
    for (index, volume) in spec.volumes.iter().enumerate() {
        let label = format!("volume {index} ({})", volume.volume_id.0);
        if !is_volume_serial(&volume.serial) {
            return Err(EngineError::Conflict(format!(
                "VM {}: {label} serial must be quoro-vol-<n>",
                spec.id.0
            )));
        }
        for other in &spec.volumes[..index] {
            if other.serial == volume.serial {
                return Err(EngineError::Conflict(format!(
                    "VM {}: two volumes use the serial {}",
                    spec.id.0, volume.serial
                )));
            }
            if other.volume_id == volume.volume_id {
                return Err(EngineError::Conflict(format!(
                    "VM {}: volume {} is attached twice",
                    spec.id.0, volume.volume_id.0
                )));
            }
        }
    }
    Ok(())
}

/// Probes whether the volume directory reflinks with `FICLONE` (contract
/// §3.16): a scratch source, a clone into a second scratch file, both removed.
///
/// Any error at any step is `PauseCopy`, including a directory that cannot be
/// written: the probe's only question is whether the clone succeeded.
pub(super) fn probe_method(dir: &Path) -> SnapshotMethod {
    let source = dir.join(format!(".probe-{}-source", std::process::id()));
    let clone = dir.join(format!(".probe-{}-clone", std::process::id()));
    let cloned = probe_clone(&source, &clone).is_ok();
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&clone);
    if cloned {
        SnapshotMethod::Reflink
    } else {
        SnapshotMethod::PauseCopy
    }
}

/// The file work of [`probe_method`].
fn probe_clone(source: &Path, clone: &Path) -> std::io::Result<()> {
    let _ = std::fs::remove_file(source);
    let _ = std::fs::remove_file(clone);
    let mut file = File::create(source)?;
    file.write_all(b"quoro")?;
    let src = File::open(source)?;
    let dst = File::create(clone)?;
    rustix::fs::ioctl_ficlone(&dst, &src).map_err(std::io::Error::from)
}

/// Removes the snapshot, staging and temporary files a crash left in the
/// volume directory (§3.16: snapshots left by a crash are removed at start;
/// §3.22: so are staging files), and returns the file names of the snapshots
/// it left. Volume files themselves are never touched.
///
/// A snapshot whose file name `sweep` keeps stays: from M5b the node agent
/// keeps the snapshots its store records, a warm base or a quarantined copy,
/// across a restart (§3.16). The one M5a snapshot name,
/// `<volume id>.snapshot`, goes under every rule: from M5b nothing creates or
/// records it, so such a file is always a backup cut short before M5b.
///
/// The names are `None` when the directory or one of its entries could not
/// be read: the node agent must never take a listing it did not get whole
/// for "nothing is left".
pub(super) fn remove_leftovers(
    dir: &Path,
    sweep: &crate::SnapshotSweep,
) -> Option<std::collections::BTreeSet<String>> {
    let mut kept = std::collections::BTreeSet::new();
    let mut whole = true;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return None;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            whole = false;
            continue;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let leftover = if let Some(stem) = name.strip_suffix(SNAPSHOT_SUFFIX) {
            // A volume id holds no dot, so a stem without one is the M5a
            // name; a newer one carries `.<time>-<counter>`.
            let legacy = !stem.contains('.');
            legacy || !sweep.keeps(name)
        } else {
            name.ends_with(super::warm::STAGING_SUFFIX) || name.ends_with(".tmp")
        };
        if leftover {
            if std::fs::remove_file(entry.path()).is_err() && name.ends_with(SNAPSHOT_SUFFIX) {
                // A snapshot that could not be removed is still there.
                kept.insert(name.to_owned());
            }
        } else if name.ends_with(SNAPSHOT_SUFFIX) {
            kept.insert(name.to_owned());
        }
    }
    whole.then_some(kept)
}

/// Free bytes available to the volume directory.
pub(super) fn available_bytes(dir: &Path) -> Result<u64, EngineError> {
    let stat = rustix::fs::statvfs(dir).map_err(|err| io_error(dir, &err.into()))?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

/// The [`VolumeInfo`] of the file at `path`.
pub(super) fn volume_info(
    id: &VolumeId,
    size_gb: u32,
    path: PathBuf,
) -> Result<VolumeInfo, EngineError> {
    let metadata = std::fs::metadata(&path).map_err(|err| io_error(&path, &err))?;
    Ok(VolumeInfo {
        volume_id: id.clone(),
        size_gb,
        allocated_bytes: metadata.blocks().saturating_mul(512),
        path,
    })
}

/// Clones `source` into a new `clone` with `FICLONE` (contract §3.16).
pub(super) fn reflink(source: &Path, clone: &Path) -> Result<(), EngineError> {
    let src = File::open(source).map_err(|err| io_error(source, &err))?;
    let dst = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(clone)
        .map_err(|err| io_error(clone, &err))?;
    if let Err(err) = rustix::fs::ioctl_ficlone(&dst, &src) {
        drop(dst);
        let _ = std::fs::remove_file(clone);
        return Err(io_error(source, &err.into()));
    }
    if let Err(err) = rustix::fs::fsync(&dst) {
        drop(dst);
        let _ = std::fs::remove_file(clone);
        return Err(io_error(clone, &err.into()));
    }
    Ok(())
}

/// Copies `source` into a new `clone`, keeping its holes (contract §3.16).
///
/// `SEEK_DATA` and `SEEK_HOLE` walk the data extents; each one is copied with
/// `copy_file_range` in [`COPY_CHUNK`] pieces. `ENXIO` from `SEEK_DATA` means
/// no data is left. The clone gets the source's length and is fsynced.
fn copy_sparse(source: &Path, clone: &Path) -> Result<u64, EngineError> {
    let src = File::open(source).map_err(|err| io_error(source, &err))?;
    let length = src.metadata().map_err(|err| io_error(source, &err))?.len();
    let dst = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(clone)
        .map_err(|err| io_error(clone, &err))?;

    let mut offset = 0_u64;
    loop {
        let data = match rustix::fs::seek(&src, SeekFrom::Data(offset)) {
            Ok(data) => data,
            Err(Errno::NXIO) => break,
            Err(err) => return Err(io_error(source, &err.into())),
        };
        if data >= length {
            break;
        }
        let end = match rustix::fs::seek(&src, SeekFrom::Hole(data)) {
            Ok(hole) => hole.min(length),
            Err(Errno::NXIO) => length,
            Err(err) => return Err(io_error(source, &err.into())),
        };
        let mut read_at = data;
        let mut write_at = data;
        while read_at < end {
            let want =
                usize::try_from((end - read_at).min(COPY_CHUNK)).unwrap_or(COPY_CHUNK as usize);
            match rustix::fs::copy_file_range(
                &src,
                Some(&mut read_at),
                &dst,
                Some(&mut write_at),
                want,
            ) {
                Ok(0) => break,
                Ok(_) => {}
                // A filesystem that cannot clone (overlayfs in a container,
                // for example) gets the same extent with plain reads and
                // writes; the hole before it stays a hole.
                Err(Errno::XDEV | Errno::OPNOTSUPP | Errno::NOSYS | Errno::INVAL) => {
                    copy_extent(&src, &dst, read_at, end, source, clone)?;
                    break;
                }
                Err(err) => return Err(io_error(source, &err.into())),
            }
        }
        offset = end;
    }

    dst.set_len(length).map_err(|err| io_error(clone, &err))?;
    rustix::fs::fsync(&dst).map_err(|err| io_error(clone, &err.into()))?;
    Ok(length)
}

/// Copies `from..to` from `src` to `dst` with ordinary reads and writes.
fn copy_extent(
    src: &File,
    dst: &File,
    from: u64,
    to: u64,
    source: &Path,
    clone: &Path,
) -> Result<(), EngineError> {
    use std::io::{Read as _, Seek as _};

    let mut reader = src;
    reader
        .seek(std::io::SeekFrom::Start(from))
        .map_err(|err| io_error(source, &err))?;
    let mut writer = dst;
    writer
        .seek(std::io::SeekFrom::Start(from))
        .map_err(|err| io_error(clone, &err))?;
    let mut remaining = to - from;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = reader
            .read(&mut buffer[..want])
            .map_err(|err| io_error(source, &err))?;
        if read == 0 {
            break;
        }
        writer
            .write_all(&buffer[..read])
            .map_err(|err| io_error(clone, &err))?;
        remaining -= read as u64;
    }
    Ok(())
}

/// Imports a restored file that lives on another filesystem (§3.20).
///
/// Copies `from` into `temp`, a file in the volume directory, keeping its
/// holes; [`copy_sparse`] sets its length and syncs it. The temporary file is
/// then renamed onto `path`, the directory entry is synced and `from` is
/// removed. Any failure removes the temporary file and leaves `from` for the
/// caller, so the node agent can retry the import.
fn import_by_copy(from: &Path, temp: &Path, path: &Path) -> Result<(), EngineError> {
    let _ = std::fs::remove_file(temp);
    let copied = copy_sparse(from, temp).map(|_| ());
    let imported = copied.and_then(|()| {
        std::fs::rename(temp, path).map_err(|err| io_error(path, &err))?;
        state::sync_dir(path)
    });
    if let Err(err) = imported {
        let _ = std::fs::remove_file(temp);
        return Err(err);
    }
    std::fs::remove_file(from).map_err(|err| io_error(from, &err))
}

/// fsyncs the temporary file before the rename that publishes it.
pub(super) fn sync_file(path: &Path) -> Result<(), EngineError> {
    let file = File::open(path).map_err(|err| io_error(path, &err))?;
    file.sync_all().map_err(|err| io_error(path, &err))
}

/// The reservation `create` holds on the volume files it attaches (contract
/// §3.6). It shares `inflight_disks` so a concurrent create of another VM
/// cannot attach the same file.
pub(super) struct VolumeReservation<'a> {
    inner: &'a Inner,
    id: VmId,
    paths: Vec<PathBuf>,
}

impl Drop for VolumeReservation<'_> {
    fn drop(&mut self) {
        let mut inflight = lock(&self.inner.inflight_disks);
        for path in &self.paths {
            if inflight.get(path) == Some(&self.id) {
                inflight.remove(path);
            }
        }
    }
}

impl Inner {
    /// The directory volume files live in.
    pub(super) fn volumes_dir(&self) -> PathBuf {
        volume_dir(self)
    }

    /// The filesystem whose free space bounds new volume data (§3.16): the
    /// host's when the pool holds the data, the volume directory otherwise.
    pub(super) fn space_dir(&self) -> PathBuf {
        match &self.pool {
            VolumePool::Mounted { .. } => self.config.state_dir.clone(),
            VolumePool::Absent | VolumePool::Broken { .. } => self.volumes_dir(),
        }
    }

    /// Reserves every volume file of `spec`, refusing a missing file, a file
    /// another VM attaches, and a concurrent create that holds it (§3.6).
    ///
    /// The caller validated the spec, including the serials, first.
    pub(super) fn reserve_volumes(
        &self,
        spec: &VmSpec,
    ) -> Result<VolumeReservation<'_>, EngineError> {
        if spec.volumes.is_empty() {
            return Ok(VolumeReservation {
                inner: self,
                id: spec.id.clone(),
                paths: Vec::new(),
            });
        }
        // A VM with volumes cannot be created while the pool is unusable: the
        // nodes underneath it would be used by mistake (§3.16).
        self.volume_pool_error()?;
        validate_volume_attaches(spec)?;
        let dir = self.volumes_dir();
        let mut paths = Vec::with_capacity(spec.volumes.len());
        for volume in &spec.volumes {
            validate_volume_id(&volume.volume_id)?;
            let path = volume_path(&dir, &volume.volume_id);
            match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => {
                    return Err(EngineError::Conflict(format!(
                        "volume {} exists and is not a regular file",
                        volume.volume_id.0
                    )));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(EngineError::NotFound);
                }
                Err(err) => return Err(io_error(&path, &err)),
            }
            for (other_id, other) in self.entries() {
                if other_id == spec.id {
                    continue;
                }
                let Some(record) = other.spec() else {
                    continue;
                };
                if record
                    .spec
                    .volumes
                    .iter()
                    .any(|other| other.volume_id == volume.volume_id.0)
                {
                    return Err(EngineError::Conflict(format!(
                        "volume {} is already attached to VM {}",
                        volume.volume_id.0, other_id.0
                    )));
                }
            }
            paths.push(path);
        }
        {
            let mut inflight = lock(&self.inflight_disks);
            for path in &paths {
                if let Some(owner) = inflight.get(path)
                    && owner != &spec.id
                {
                    return Err(EngineError::Conflict(format!(
                        "volume {} is being attached to VM {}",
                        path.file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        owner.0
                    )));
                }
            }
            for path in &paths {
                inflight.insert(path.clone(), spec.id.clone());
            }
        }
        Ok(VolumeReservation {
            inner: self,
            id: spec.id.clone(),
            paths,
        })
    }

    /// Creates one sparse volume file and formats it as ext4 (§3.20).
    pub(super) async fn ensure_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
    ) -> Result<VolumeInfo, EngineError> {
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let path = volume_path(&dir, id);
        let expected = u64::from(size_gb) << 30;

        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.file_type().is_file() {
                    return Err(EngineError::Conflict(format!(
                        "volume {} exists and is not a regular file",
                        id.0
                    )));
                }
                if metadata.len() != expected {
                    return Err(EngineError::Conflict(format!(
                        "volume {} exists with {} bytes, not {size_gb} GiB",
                        id.0,
                        metadata.len()
                    )));
                }
                return volume_info(id, size_gb, path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error(&path, &err)),
        }

        // The pool must be able to hold the new volume and one snapshot of it
        // before the file is created, and the lock keeps a concurrent create
        // from sizing the pool without this volume (§3.16).
        let _growing = match &self.pool {
            VolumePool::Mounted { .. } => Some(self.pool_grow.lock().await),
            VolumePool::Absent | VolumePool::Broken { .. } => None,
        };
        self.grow_pool_for(expected).await?;

        let space = self.space_dir();
        let available = available_bytes(&space)?;
        if available < METADATA_MARGIN {
            return Err(EngineError::ResourceExhausted(format!(
                "{} has {available} bytes free, below the {METADATA_MARGIN} byte metadata margin",
                space.display()
            )));
        }

        // A crash between the create and the rename leaves only this file; the
        // next call replaces it.
        let temp = dir.join(format!("{}.tmp", id.0));
        let _ = std::fs::remove_file(&temp);
        let create = temp.clone();
        let created = tokio::task::spawn_blocking(move || create_work_disk_file(&create, size_gb))
            .await
            .map_err(|err| EngineError::Io(format!("volume task failed: {err}")))??;
        if !created {
            let _ = std::fs::remove_file(&temp);
            return Err(EngineError::Conflict(format!(
                "volume {} was created concurrently",
                id.0
            )));
        }

        let temp_text = temp.to_str().ok_or_else(|| {
            EngineError::Conflict(format!("volume path {} is not valid UTF-8", temp.display()))
        })?;
        let formatted = tools::run(
            &self.tools.mkfs_ext4,
            &[
                "-q",
                "-F",
                "-E",
                "nodiscard,lazy_itable_init=1,lazy_journal_init=1",
                temp_text,
            ],
        )
        .await;
        if let Err(err) = formatted {
            let _ = std::fs::remove_file(&temp);
            return Err(err);
        }

        let sync = temp.clone();
        tokio::task::spawn_blocking(move || sync_file(&sync))
            .await
            .map_err(|err| EngineError::Io(format!("volume task failed: {err}")))??;
        std::fs::rename(&temp, &path).map_err(|err| io_error(&path, &err))?;
        state::sync_dir(&path)?;
        volume_info(id, size_gb, path)
    }

    /// Moves a restored raw file into place as one volume (§3.20).
    ///
    /// A plain rename when the file is on the volume directory's filesystem;
    /// otherwise (the pool is another filesystem) the file is copied with
    /// [`copy_sparse`], which keeps its holes, and the copy is renamed into
    /// place before the given file is removed.
    pub(super) async fn import_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &Path,
    ) -> Result<VolumeInfo, EngineError> {
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let path = volume_path(&dir, id);
        if path.exists() {
            return Err(EngineError::Conflict(format!(
                "volume {} already exists",
                id.0
            )));
        }
        let metadata = std::fs::symlink_metadata(from).map_err(|_| {
            EngineError::Conflict(format!("{} is not a readable file", from.display()))
        })?;
        let expected = u64::from(size_gb) << 30;
        if !metadata.file_type().is_file() {
            return Err(EngineError::Conflict(format!(
                "{} is not a regular file",
                from.display()
            )));
        }
        if metadata.len() != expected {
            return Err(EngineError::Conflict(format!(
                "{} is {} bytes, not {size_gb} GiB",
                from.display(),
                metadata.len()
            )));
        }
        // The pool grows for the restored volume before it is moved in
        // (§3.16); the lock keeps a concurrent create from racing the size.
        let _growing = match &self.pool {
            VolumePool::Mounted { .. } => Some(self.pool_grow.lock().await),
            VolumePool::Absent | VolumePool::Broken { .. } => None,
        };
        self.grow_pool_for(expected).await?;
        let temp = dir.join(format!("{}.tmp", id.0));
        match std::fs::rename(from, &path) {
            Ok(()) => state::sync_dir(&path)?,
            // The node agent stages its restores outside the pool, so the
            // rename crosses a filesystem: copy, sync, rename, then remove
            // the staged file (§3.20).
            Err(err) if err.raw_os_error() == Some(rustix::io::Errno::XDEV.raw_os_error()) => {
                let source = from.to_path_buf();
                let copy = temp.clone();
                let target = path.clone();
                tokio::task::spawn_blocking(move || import_by_copy(&source, &copy, &target))
                    .await
                    .map_err(|err| EngineError::Io(format!("volume task failed: {err}")))??;
            }
            Err(err) => return Err(io_error(&path, &err)),
        }
        volume_info(id, size_gb, path)
    }

    /// Removes one volume file, unless a VM that exists attaches it (§3.20).
    ///
    /// A leftover snapshot of the volume goes with it; a missing file is `Ok`.
    pub(super) async fn delete_volume(&self, id: &VolumeId) -> Result<(), EngineError> {
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let path = volume_path(&dir, id);
        if let Some(vm_id) = self.vm_attaching(id) {
            return Err(EngineError::Conflict(format!(
                "volume {} is attached to VM {}",
                id.0, vm_id.0
            )));
        }
        if let Some(owner) = lock(&self.inflight_disks).get(&path) {
            return Err(EngineError::Conflict(format!(
                "volume {} is being attached to VM {}",
                id.0, owner.0
            )));
        }
        match std::fs::remove_file(&path) {
            Ok(()) => state::sync_dir(&path)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error(&path, &err)),
        }
        // M5a's single snapshot name and a staging file only: from M5b the
        // node agent deletes the snapshots it took, and keeps some on purpose
        // (a warm base, a quarantined copy), so no pattern of snapshot names
        // is removed here (§3.16). A staging file belongs to the volume alone.
        for leftover in [
            legacy_snapshot_path(&dir, id),
            super::warm::staging_path(&dir, id),
        ] {
            if let Err(err) = std::fs::remove_file(&leftover)
                && err.kind() != std::io::ErrorKind::NotFound
            {
                return Err(io_error(&leftover, &err));
            }
        }
        Ok(())
    }

    /// Takes a snapshot of one volume (§3.16, ADR 0146). From M5b a volume may
    /// have several at once, each with its own file name.
    ///
    /// `Reflink` clones the file. `PauseCopy` pauses the VM that attaches the
    /// volume when it is running, copies the sparse file and always resumes the
    /// VM, whatever the copy's outcome (see [`Inner::pause_copy`]).
    pub(super) async fn snapshot_volume(
        &self,
        id: &VolumeId,
    ) -> Result<VolumeSnapshot, EngineError> {
        validate_volume_id(id)?;
        self.volume_pool_error()?;
        let dir = self.volumes_dir();
        let path = volume_path(&dir, id);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => metadata,
            Ok(_) => {
                return Err(EngineError::Conflict(format!(
                    "volume {} exists and is not a regular file",
                    id.0
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(EngineError::NotFound);
            }
            Err(err) => return Err(io_error(&path, &err)),
        };
        let location = new_snapshot_path(&dir, id);

        let length = metadata.len();
        let allocated = metadata.blocks().saturating_mul(512);
        let method = self.volume_method;
        let mut pause = Duration::ZERO;
        let mut copied = None;
        match method {
            SnapshotMethod::Reflink => {
                let source = path.clone();
                let clone = location.clone();
                tokio::task::spawn_blocking(move || reflink(&source, &clone))
                    .await
                    .map_err(|err| EngineError::Io(format!("snapshot task failed: {err}")))??;
            }
            SnapshotMethod::PauseCopy => {
                let space = self.space_dir();
                let available = available_bytes(&space)?;
                if available < allocated {
                    return Err(EngineError::ResourceExhausted(format!(
                        "volume {} uses {allocated} bytes and only {available} are free",
                        id.0
                    )));
                }
                let (paused, file) = self.pause_copy(id, &path, &location).await?;
                pause = paused;
                copied = Some(file);
            }
        }

        // The new name must survive a power cut as well as the data, or a
        // snapshot the node agent recorded could vanish with its directory
        // entry (§3.16).
        if let Err(err) = state::sync_dir(&location) {
            let _ = std::fs::remove_file(&location);
            return Err(err);
        }
        let snapshot_id = location
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(file) = copied {
            file.keep();
        }
        Ok(VolumeSnapshot {
            volume_id: id.clone(),
            snapshot_id,
            created_at: SystemTime::now(),
            size_bytes: length,
            location,
            method,
            pause,
        })
    }

    /// Removes one snapshot file; a missing file is `Ok` (§3.16).
    pub(super) async fn delete_volume_snapshot(
        &self,
        snapshot: &VolumeSnapshot,
    ) -> Result<(), EngineError> {
        self.volume_pool_error()?;
        // With several snapshots per volume, a record that names another
        // volume's file must not delete it (§3.16).
        super::warm::check_snapshot_file(&self.volumes_dir(), snapshot)?;
        match std::fs::remove_file(&snapshot.location) {
            Ok(()) => state::sync_dir(&snapshot.location),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(&snapshot.location, &err)),
        }
    }

    /// The `PauseCopy` snapshot of the volume file `path` into `location`
    /// (§3.16): see [`spawn_pause_copy`].
    async fn pause_copy(
        &self,
        id: &VolumeId,
        path: &Path,
        location: &Path,
    ) -> Result<(Duration, SnapshotFile), EngineError> {
        spawn_pause_copy(
            self.vms_for_volume(id),
            id.clone(),
            path.to_owned(),
            location.to_owned(),
            copy_sparse,
        )
        .await
    }

    /// The VMs whose spec attaches `id`, whatever their state (§3.16).
    fn vms_for_volume(&self, id: &VolumeId) -> Vec<Arc<VmEntry>> {
        self.entries()
            .into_iter()
            .filter_map(|(vm_id, entry)| {
                let (_, record) = self.entry_with_spec(&vm_id).ok()?;
                record
                    .spec
                    .volumes
                    .iter()
                    .any(|volume| volume.volume_id == id.0)
                    .then_some(entry)
            })
            .collect()
    }
}

/// A snapshot file a `PauseCopy` made, removed when this is dropped unless
/// [`Self::keep`] took it: a snapshot whose caller gave up is not left behind.
pub(super) struct SnapshotFile(Option<PathBuf>);

impl SnapshotFile {
    /// The caller has the snapshot; the file stays.
    pub(super) fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Runs [`pause_and_copy`] on a task of its own. A caller that drops the
/// returned future does not leave the VM paused: the task carries the copy on
/// to the resume. The snapshot file comes with a [`SnapshotFile`], so one
/// nobody received is removed (§3.16).
pub(super) fn spawn_pause_copy<F>(
    vms: Vec<Arc<VmEntry>>,
    volume: VolumeId,
    source: PathBuf,
    clone: PathBuf,
    copy: F,
) -> impl Future<Output = Result<(Duration, SnapshotFile), EngineError>> + Send
where
    F: FnOnce(&Path, &Path) -> Result<u64, EngineError> + Send + 'static,
{
    let (sent, received) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = pause_and_copy(&vms, &volume, &source, &clone, copy)
            .await
            .map(|pause| (pause, SnapshotFile(Some(clone))));
        // A result nobody receives is dropped here, its file with it.
        let _ = sent.send(result);
    });
    async move {
        received
            .await
            .map_err(|_| EngineError::Io("snapshot task failed".to_owned()))?
    }
}

/// The VM a `PauseCopy` snapshot paused: which VMM process, and since when.
struct PausedVm {
    entry: Arc<VmEntry>,
    process: ProcessId,
    since: Instant,
}

/// Pauses the one of `vms` that runs, copies `source` into `clone` with
/// `copy` and resumes the VM whatever the copy's outcome (§3.16). Returns how
/// long the VM was paused, zero when none ran and the volume was copied
/// without a pause.
///
/// The VM's operation lock is held for the pause and for the resume, and the
/// VM's state is read under it, so neither interleaves with a start, a stop
/// or a delete of the VM, and one under way is waited out first (§4). It is
/// not held during the copy, which can take a minute, so a stop of the paused
/// VM, a fence's included, is not held up by it. The resume is for the VMM
/// process that was paused only: when that process is gone, the copy is not
/// kept, as the VM may have started again on the volume during it. While the
/// VM is paused, its directory records the paused process, so an engine that
/// restarts meanwhile resumes it ([`resume_left_paused`]).
pub(super) async fn pause_and_copy<F>(
    vms: &[Arc<VmEntry>],
    volume: &VolumeId,
    source: &Path,
    clone: &Path,
    copy: F,
) -> Result<Duration, EngineError>
where
    F: FnOnce(&Path, &Path) -> Result<u64, EngineError> + Send + 'static,
{
    let paused = pause_running(vms, volume).await?;
    let (from, to) = (source.to_owned(), clone.to_owned());
    // No `?` before the resume: a failed or panicked copy task must still
    // resume the VM.
    let copied = tokio::task::spawn_blocking(move || copy(&from, &to))
        .await
        .map_err(|err| EngineError::Io(format!("snapshot task failed: {err}")))
        .and_then(|result| result);
    let mut pause = Duration::ZERO;
    if let Some(paused) = paused {
        if let Err(err) = resume_paused(&paused, volume).await {
            let _ = std::fs::remove_file(clone);
            return Err(err);
        }
        // The window from before the pause to after the resume.
        pause = paused.since.elapsed();
    }
    if let Err(err) = copied {
        let _ = std::fs::remove_file(clone);
        return Err(err);
    }
    Ok(pause)
}

/// Pauses the first of `vms` whose guest runs, under its operation lock.
///
/// A VM another caller paused is `Conflict`: its freeze is not this
/// snapshot's, and it could end during the copy. A VM whose VMM does not say
/// its state is `Hypervisor`, since the copy could not be made consistent. A
/// forced stop of the VM that waits for the lock meanwhile gets it at once,
/// and the snapshot is `Conflict` (§4, ADR 0243).
async fn pause_running(
    vms: &[Arc<VmEntry>],
    volume: &VolumeId,
) -> Result<Option<PausedVm>, EngineError> {
    for entry in vms {
        let _op = entry.op.lock().await;
        if !entry.paths.dir.exists() || !entry.booted.load(Ordering::SeqCst) {
            continue;
        }
        let Some(vmm) = entry.live_vmm() else {
            continue;
        };
        let process = vmm.id();
        let since = tokio::select! {
            since = pause_if_running(entry, process, volume) => since?,
            () = entry.forced_stop_waiting() => {
                return Err(EngineError::Conflict(format!(
                    "a forced stop of the VM using volume {} came before the snapshot's pause",
                    volume.0
                )));
            }
        };
        let Some(since) = since else {
            continue;
        };
        return Ok(Some(PausedVm {
            entry: Arc::clone(entry),
            process,
            since,
        }));
    }
    Ok(None)
}

/// Pauses the VM if its VMM reports it running, and returns when the pause
/// began; `None` when it is not running. The paused process is recorded
/// before the pause and the record removed when the pause did not take
/// effect.
async fn pause_if_running(
    entry: &VmEntry,
    process: ProcessId,
    volume: &VolumeId,
) -> Result<Option<Instant>, EngineError> {
    let api = ApiClient::new(&entry.paths.api_socket).with_timeout(SNAPSHOT_API_TIMEOUT);
    let document = api.info().await.map_err(|err| {
        EngineError::Hypervisor(format!(
            "cloud-hypervisor did not report the VM using volume {}: {err}",
            volume.0
        ))
    })?;
    match document.get("state").and_then(serde_json::Value::as_str) {
        Some("Running") => {}
        Some("Paused") => {
            return Err(EngineError::Conflict(format!(
                "the VM using volume {} is paused by another caller",
                volume.0
            )));
        }
        _ => return Ok(None),
    }
    // Recorded first: a pause the engine cannot record is not made, since a
    // restart during the copy would leave it in place.
    state::write_json_atomic(&entry.paths.snapshot_paused, &process)?;
    let since = Instant::now();
    if let Err(err) = api.pause().await {
        // The request may have reached the VMM and only its answer been lost;
        // nobody else could pause the VM under its lock.
        if !is_paused(&api).await || api.resume().await.is_ok() {
            let _ = state::remove_marker(&entry.paths.snapshot_paused);
        }
        return Err(EngineError::Hypervisor(format!(
            "cloud-hypervisor did not pause the VM using volume {}: {err}",
            volume.0
        )));
    }
    Ok(Some(since))
}

/// Resumes the VM `paused` stopped, under its operation lock, if its VMM is
/// still the process that was paused. A resume that fails, or a VMM that is
/// gone, is `Hypervisor`, and the VM is left for the node agent's crash
/// handling (§3.16). A forced stop of the VM that waits for the lock meanwhile
/// gets it at once and ends the VM, which is then not resumed (ADR 0243).
async fn resume_paused(paused: &PausedVm, volume: &VolumeId) -> Result<(), EngineError> {
    let entry = &paused.entry;
    let stopped = || {
        EngineError::Hypervisor(format!(
            "the VM using volume {} stopped while it was paused for the copy",
            volume.0
        ))
    };
    let _op = entry.op.lock().await;
    if !entry
        .live_vmm()
        .is_some_and(|vmm| vmm.id() == paused.process)
    {
        let _ = state::remove_marker(&entry.paths.snapshot_paused);
        return Err(stopped());
    }
    let api = ApiClient::new(&entry.paths.api_socket).with_timeout(SNAPSHOT_API_TIMEOUT);
    tokio::select! {
        resumed = api.resume() => {
            resumed.map_err(|err| {
                EngineError::Hypervisor(format!(
                    "cloud-hypervisor did not resume the VM using volume {}: {err}",
                    volume.0
                ))
            })?;
            let _ = state::remove_marker(&entry.paths.snapshot_paused);
            Ok(())
        }
        () = entry.forced_stop_waiting() => Err(stopped()),
    }
}

/// Resumes a VM that a `PauseCopy` snapshot paused before the engine last
/// stopped, once the engine has adopted its VMM again (§3.16): the record
/// names the paused process, and only that process is resumed. A record of a
/// process that is gone is removed.
pub(super) async fn resume_left_paused(entry: &VmEntry) {
    let Ok(Some(process)) = state::read_json::<ProcessId>(&entry.paths.snapshot_paused) else {
        return;
    };
    let _op = entry.op.lock().await;
    if !entry.live_vmm().is_some_and(|vmm| vmm.id() == process) {
        let _ = state::remove_marker(&entry.paths.snapshot_paused);
        return;
    }
    let api = ApiClient::new(&entry.paths.api_socket).with_timeout(SNAPSHOT_API_TIMEOUT);
    if !is_paused(&api).await {
        let _ = state::remove_marker(&entry.paths.snapshot_paused);
    } else if api.resume().await.is_ok() {
        eprintln!(
            "quoro-engine: resumed VM {}, which a volume snapshot paused before the engine restarted",
            entry.id.0
        );
        let _ = state::remove_marker(&entry.paths.snapshot_paused);
    } else {
        eprintln!(
            "quoro-engine: could not resume VM {}, which a volume snapshot paused before the engine restarted",
            entry.id.0
        );
    }
}

/// Whether the VMM reports its VM paused.
async fn is_paused(api: &ApiClient) -> bool {
    api.info().await.is_ok_and(|document| {
        document.get("state").and_then(serde_json::Value::as_str) == Some("Paused")
    })
}

/// How long one pause or resume may take. A paused VM makes no progress, so
/// the call fails rather than leaving it paused forever.
const SNAPSHOT_API_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
    use std::path::{Path, PathBuf};

    use super::{
        copy_sparse, import_by_copy, is_crockford, is_volume_serial, legacy_snapshot_path,
        new_snapshot_path, probe_method, remove_leftovers, validate_volume_attaches,
        validate_volume_id, volume_path,
    };
    use crate::{
        EngineError, ImageDigest, InitConfig, MacAddr, NetAttach, SerialMode, SnapshotMethod, VmId,
        VmSpec, VolumeAttach, VolumeId,
    };

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .join("volume-tests")
            .join(format!("{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn volume_id(text: &str) -> VolumeId {
        VolumeId(text.to_string())
    }

    fn spec(volumes: Vec<VolumeAttach>) -> VmSpec {
        VmSpec {
            id: VmId("vm_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string()),
            vcpus: 1,
            memory_mb: 256,
            rootfs: ImageDigest("sha256:aa".to_string()),
            disk_gb: 1,
            volumes,
            net: NetAttach {
                ip: "10.42.1.7".parse().expect("ip"),
                gateway: "10.42.1.1".parse().expect("gateway"),
                mac: MacAddr([0x02, 0, 0, 0, 0, 1]),
                isolation_group: None,
                egress_only: false,
            },
            init: InitConfig {
                entrypoint: vec!["/bin/sh".to_string()],
                cmd: vec!["-c".to_string(), "true".to_string()],
                env: Vec::new(),
                user: None,
                workdir: None,
            },
            console: crate::ConsoleMode::Off,
            serial: SerialMode::Off,
            vsock: None,
            work_disks: Vec::new(),
        }
    }

    fn attach(id: &str, serial: &str) -> VolumeAttach {
        VolumeAttach {
            volume_id: volume_id(id),
            mount_path: "/data".to_string(),
            serial: serial.to_string(),
        }
    }

    #[test]
    fn volume_ids_are_ulids() {
        let good = "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF";
        assert!(validate_volume_id(&volume_id(good)).is_ok());
        for bad in [
            "",
            "vol_",
            "vol_01hx8z4q2m9w7t3yv6b1c0d5ef",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5E",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EFF",
            "../vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EI",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EL",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EO",
            "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EU",
        ] {
            match validate_volume_id(&volume_id(bad)) {
                Err(EngineError::Conflict(reason)) => {
                    assert!(reason.starts_with("invalid volume id "), "{reason}");
                }
                other => panic!("{bad:?} should be refused, got {other:?}"),
            }
        }
        assert!(is_crockford(b'Z') && !is_crockford(b'U'));
    }

    #[test]
    fn volume_serials_are_numbered() {
        assert!(is_volume_serial("quoro-vol-0"));
        assert!(is_volume_serial("quoro-vol-12"));
        for bad in ["quoro-vol-", "quoro-vol-x", "quoro-volume-1", "QUORO-VOL-0"] {
            assert!(!is_volume_serial(bad), "{bad:?}");
        }
    }

    #[test]
    fn volume_attachments_need_unique_numbered_serials() {
        let good = "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF";
        let other = "vol_01HX8Z4Q2M9W7T3YV6B1C0D5EG";
        validate_volume_attaches(&spec(vec![
            attach(good, "quoro-vol-0"),
            attach(other, "quoro-vol-1"),
        ]))
        .expect("valid attachments");

        assert!(matches!(
            validate_volume_attaches(&spec(vec![attach(good, "vol0")])),
            Err(EngineError::Conflict(reason)) if reason.contains("quoro-vol-<n>")
        ));
        assert!(matches!(
            validate_volume_attaches(&spec(vec![
                attach(good, "quoro-vol-0"),
                attach(other, "quoro-vol-0"),
            ])),
            Err(EngineError::Conflict(reason)) if reason.contains("serial")
        ));
        assert!(matches!(
            validate_volume_attaches(&spec(vec![
                attach(good, "quoro-vol-0"),
                attach(good, "quoro-vol-1"),
            ])),
            Err(EngineError::Conflict(reason)) if reason.contains("twice")
        ));
    }

    #[test]
    fn volume_paths_use_the_directory_and_the_snapshot_suffix() {
        let dir = Path::new("/var/lib/quoro/volumes");
        let id = volume_id("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF");
        assert_eq!(
            volume_path(dir, &id),
            PathBuf::from("/var/lib/quoro/volumes/vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF")
        );
        assert_eq!(
            legacy_snapshot_path(dir, &id),
            PathBuf::from("/var/lib/quoro/volumes/vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.snapshot")
        );
    }

    #[test]
    fn every_new_snapshot_of_a_volume_gets_its_own_name() {
        let dir = scratch_dir("names");
        let id = volume_id("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF");
        let first = new_snapshot_path(&dir, &id);
        std::fs::write(&first, b"x").expect("file");
        let second = new_snapshot_path(&dir, &id);
        assert_ne!(first, second);
        for path in [&first, &second] {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("name");
            assert!(
                name.starts_with("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF."),
                "{name}"
            );
            assert!(name.ends_with(".snapshot"), "{name}");
            assert_eq!(path.parent(), Some(dir.as_path()));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn probe_marks_a_plain_directory_pause_copy_or_reflink() {
        let dir = scratch_dir("probe");
        let method = probe_method(&dir);
        assert!(matches!(
            method,
            SnapshotMethod::Reflink | SnapshotMethod::PauseCopy
        ));
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("read the probe directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sparse_copy_keeps_holes_and_copies_every_data_extent() {
        let dir = scratch_dir("sparse");
        let source = dir.join("source.img");
        let clone = dir.join("clone.img");
        const LENGTH: u64 = 16 * 1024 * 1024;
        let mut file = std::fs::File::create(&source).expect("create");
        file.set_len(LENGTH).expect("set length");
        file.seek(SeekFrom::Start(0)).expect("seek");
        file.write_all(&[7_u8; 4096]).expect("write head");
        file.seek(SeekFrom::Start(8 * 1024 * 1024))
            .expect("seek middle");
        file.write_all(&[9_u8; 8192]).expect("write middle");
        file.seek(SeekFrom::Start(LENGTH - 4096))
            .expect("seek tail");
        file.write_all(&[5_u8; 4096]).expect("write tail");
        file.sync_all().expect("sync");

        let copied = copy_sparse(&source, &clone).expect("copy");
        assert_eq!(copied, LENGTH);
        let mut read = std::fs::File::open(&clone).expect("open clone");
        assert_eq!(read.metadata().expect("metadata").len(), LENGTH);
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).expect("read clone");
        assert_eq!(bytes.len() as u64, LENGTH);
        assert!(bytes[..4096].iter().all(|byte| *byte == 7));
        assert!(bytes[4096..8 * 1024 * 1024].iter().all(|byte| *byte == 0));
        assert!(
            bytes[8 * 1024 * 1024..8 * 1024 * 1024 + 8192]
                .iter()
                .all(|byte| *byte == 9)
        );
        assert!(
            bytes[8 * 1024 * 1024 + 8192..LENGTH as usize - 4096]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(
            bytes[LENGTH as usize - 4096..]
                .iter()
                .all(|byte| *byte == 5)
        );

        use std::os::unix::fs::MetadataExt as _;
        let source_allocated = std::fs::metadata(&source).expect("metadata").blocks() * 512;
        let allocated = std::fs::metadata(&clone).expect("metadata").blocks() * 512;
        // A filesystem that does not report holes (overlayfs in a container,
        // for example) stores the source fully allocated; then the clone is
        // only required to match. Where the source is sparse, so is the clone.
        if source_allocated < LENGTH / 2 {
            assert!(
                allocated < LENGTH / 2,
                "the clone is not sparse: {allocated} bytes allocated"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_by_copy_keeps_the_holes_and_takes_the_given_file() {
        let dir = scratch_dir("import");
        let from = dir.join("restored.img");
        let temp = dir.join("vol_01JC9ZC2D3E4F5G6H7J8K9M0NP.tmp");
        let path = dir.join("vol_01JC9ZC2D3E4F5G6H7J8K9M0NP");
        const LENGTH: u64 = 8 * 1024 * 1024;
        let mut file = std::fs::File::create(&from).expect("create");
        file.set_len(LENGTH).expect("set length");
        file.seek(SeekFrom::Start(0)).expect("seek");
        file.write_all(&[7_u8; 4096]).expect("write head");
        file.seek(SeekFrom::Start(LENGTH - 4096))
            .expect("seek tail");
        file.write_all(&[5_u8; 4096]).expect("write tail");
        file.sync_all().expect("sync");
        // A crash left a temporary file behind; the import replaces it.
        std::fs::write(&temp, b"stale").expect("stale temp");

        import_by_copy(&from, &temp, &path).expect("copy import");
        assert!(!from.exists(), "the given file is removed");
        assert!(!temp.exists(), "the temporary file is renamed away");
        let metadata = std::fs::metadata(&path).expect("volume file");
        assert_eq!(metadata.len(), LENGTH);
        let mut read = std::fs::File::open(&path).expect("open");
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).expect("read");
        assert!(bytes[..4096].iter().all(|byte| *byte == 7));
        assert!(
            bytes[4096..LENGTH as usize - 4096]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(
            bytes[LENGTH as usize - 4096..]
                .iter()
                .all(|byte| *byte == 5)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_by_copy_leaves_the_given_file_when_the_rename_fails() {
        let dir = scratch_dir("import-fail");
        let from = dir.join("restored.img");
        let temp = dir.join("volume.tmp");
        let path = dir.join("volume");
        std::fs::write(&from, b"quoro").expect("source");
        std::fs::create_dir(&path).expect("a directory is in the way");

        let err = import_by_copy(&from, &temp, &path).expect_err("rename onto a directory fails");
        assert!(matches!(err, EngineError::Io(_)), "{err:?}");
        assert!(from.exists(), "the given file is left for the caller");
        assert!(!temp.exists(), "no temporary file is left behind");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftovers_are_removed_and_volumes_are_kept() {
        let dir = scratch_dir("leftovers");
        let id = "vol_01JC9ZC2D3E4F5G6H7J8K9M0NP";
        let leftovers = [
            format!("{id}.snapshot"),
            format!("{id}.19a2b3c4d5e-3.snapshot"),
            format!("{id}.staging"),
            format!("{id}.tmp"),
        ];
        for name in std::iter::once(id.to_string()).chain(leftovers.iter().cloned()) {
            std::fs::write(dir.join(&name), b"x").expect("file");
        }
        let kept = remove_leftovers(&dir, &crate::SnapshotSweep::Keep(BTreeSet::new()));
        assert_eq!(kept, Some(BTreeSet::new()));
        assert!(dir.join(id).exists(), "the volume file stays");
        for name in &leftovers {
            assert!(!dir.join(name).exists(), "{name} is removed");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_sweep_keeps_the_snapshots_the_node_agent_records() {
        let dir = scratch_dir("kept");
        let id = "vol_01JC9ZC2D3E4F5G6H7J8K9M0NP";
        let kept = format!("{id}.19a2b3c4d5e-3.snapshot");
        let unrecorded = format!("{id}.19a2b3c4d5f-4.snapshot");
        let others = [format!("{id}.staging"), format!("{id}.tmp")];
        let write_all = || {
            for name in [id.to_string(), kept.clone(), unrecorded.clone()]
                .iter()
                .chain(&others)
            {
                std::fs::write(dir.join(name), b"x").expect("file");
            }
        };

        write_all();
        let rule = crate::SnapshotSweep::Keep([kept.clone()].into_iter().collect());
        let left = remove_leftovers(&dir, &rule);
        assert_eq!(
            left,
            Some([kept.clone()].into_iter().collect::<BTreeSet<_>>())
        );
        assert!(dir.join(id).exists(), "the volume file stays");
        assert!(dir.join(&kept).exists(), "a recorded snapshot stays");
        assert!(!dir.join(&unrecorded).exists(), "an unrecorded one goes");
        for name in &others {
            assert!(!dir.join(name).exists(), "{name} is removed");
        }

        // A node agent that cannot read its records keeps every snapshot,
        // but never a staging or temporary file.
        write_all();
        let legacy = format!("{id}.snapshot");
        std::fs::write(dir.join(&legacy), b"x").expect("file");
        let left = remove_leftovers(&dir, &crate::SnapshotSweep::KeepAll);
        assert_eq!(
            left,
            Some(
                [kept.clone(), unrecorded.clone()]
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            ),
            "the sweep reports what it left"
        );
        assert!(dir.join(&kept).exists() && dir.join(&unrecorded).exists());
        for name in &others {
            assert!(!dir.join(name).exists(), "{name} is removed");
        }
        // The M5a name goes under every rule: nothing from M5b records it.
        assert!(!dir.join(&legacy).exists(), "the M5a snapshot name goes");

        // A directory it cannot read reports nothing, never an empty set.
        assert_eq!(
            remove_leftovers(&dir.join("missing"), &crate::SnapshotSweep::KeepAll),
            None
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The sweep matches the names the engine gives its snapshots, so a
    /// snapshot named by [`new_snapshot_path`] and kept by its file name
    /// survives, and one that is not kept goes.
    #[test]
    fn the_sweep_matches_the_names_the_engine_gives_snapshots() {
        let dir = scratch_dir("names");
        let id = VolumeId("vol_01JC9ZC2D3E4F5G6H7J8K9M0NP".to_string());
        std::fs::write(volume_path(&dir, &id), b"live").expect("volume");
        let recorded = new_snapshot_path(&dir, &id);
        let unrecorded = new_snapshot_path(&dir, &id);
        for path in [&recorded, &unrecorded] {
            copy_sparse(&volume_path(&dir, &id), path).expect("snapshot");
        }
        let name = |path: &Path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .expect("a file name")
                .to_owned()
        };
        let rule = crate::SnapshotSweep::Keep([name(&recorded)].into_iter().collect());
        let left = remove_leftovers(&dir, &rule);
        assert_eq!(
            left,
            Some([name(&recorded)].into_iter().collect::<BTreeSet<_>>())
        );
        assert!(recorded.exists(), "the recorded snapshot stays");
        assert!(!unrecorded.exists(), "the other one goes");
        assert!(volume_path(&dir, &id).exists(), "the volume file stays");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
