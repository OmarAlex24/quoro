// SPDX-License-Identifier: AGPL-3.0-only
//! Local volumes and their snapshots (engine-trait.md §3.16, §3.20; ADR 0145,
//! ADR 0146).
//!
//! A volume is one sparse raw file, named by the volume id, in
//! `<state_dir>/volumes/`, owned by root and never mounted on the host. The
//! engine checks and formats it with `mkfs.ext4` on the file itself, attaches
//! it as a writable virtio-blk disk and snapshots it by `FICLONE` when the
//! node's filesystem reflinks, or by pausing the using VM, copying the sparse
//! file and resuming it otherwise. The method is probed once at start-up.
//!
//! [`Inner`]'s volume methods live here so `engine.rs` stays a lifecycle
//! module; they share its locks and its record view through the
//! `pub(super)` items there.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rustix::fs::SeekFrom;
use rustix::io::Errno;

use super::api::ApiClient;
use super::engine::{Inner, create_work_disk_file, io_error, lock};
use crate::ch::state;
use crate::ch::tools;
use crate::{
    EngineError, SnapshotMethod, VmId, VmSpec, VmState, VolumeId, VolumeInfo, VolumeSnapshot,
};

/// Free bytes the volume directory keeps for the filesystem's metadata: a
/// sparse file itself needs almost nothing up front, but `mkfs.ext4` writes
/// inode and journal metadata into it (contract §3.20).
const METADATA_MARGIN: u64 = 64 * 1024 * 1024;

/// Bytes copied per `copy_file_range` call. Large enough to keep the syscall
/// count low, small enough that a resume never waits on one huge call.
const COPY_CHUNK: u64 = 4 * 1024 * 1024;

/// The suffix of the one snapshot file a volume may have (contract §3.16).
const SNAPSHOT_SUFFIX: &str = ".snapshot";

/// The directory that holds every volume file.
pub(super) fn volume_dir(inner: &Inner) -> PathBuf {
    inner.config.state_dir.join("volumes")
}

/// The file of one volume, `<volume_dir>/<volume id>` (contract §3.20).
pub(super) fn volume_path(dir: &Path, id: &VolumeId) -> PathBuf {
    dir.join(&id.0)
}

/// The snapshot file of one volume, `<volume_dir>/<volume id>.snapshot`.
pub(super) fn snapshot_path(dir: &Path, id: &VolumeId) -> PathBuf {
    dir.join(format!("{}{SNAPSHOT_SUFFIX}", id.0))
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

/// Removes the snapshot and temporary files a crash left in the volume
/// directory (§3.16: snapshots left by a crash are removed at start). Volume
/// files themselves are never touched.
pub(super) fn remove_leftovers(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(SNAPSHOT_SUFFIX) || name.ends_with(".tmp") {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Free bytes available to the volume directory.
fn available_bytes(dir: &Path) -> Result<u64, EngineError> {
    let stat = rustix::fs::statvfs(dir).map_err(|err| io_error(dir, &err.into()))?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

/// The [`VolumeInfo`] of the file at `path`.
fn volume_info(id: &VolumeId, size_gb: u32, path: PathBuf) -> Result<VolumeInfo, EngineError> {
    let metadata = std::fs::metadata(&path).map_err(|err| io_error(&path, &err))?;
    Ok(VolumeInfo {
        volume_id: id.clone(),
        size_gb,
        allocated_bytes: metadata.blocks().saturating_mul(512),
        path,
    })
}

/// Clones `source` into a new `clone` with `FICLONE` (contract §3.16).
fn reflink(source: &Path, clone: &Path) -> Result<(), EngineError> {
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
    rustix::fs::fsync(&dst).map_err(|err| io_error(clone, &err.into()))?;
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

/// fsyncs the temporary file before the rename that publishes it.
fn sync_file(path: &Path) -> Result<(), EngineError> {
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

        let available = available_bytes(&dir)?;
        if available < METADATA_MARGIN {
            return Err(EngineError::ResourceExhausted(format!(
                "{} has {available} bytes free, below the {METADATA_MARGIN} byte metadata margin",
                dir.display()
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
    pub(super) async fn import_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &Path,
    ) -> Result<VolumeInfo, EngineError> {
        validate_volume_id(id)?;
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
        std::fs::rename(from, &path).map_err(|err| {
            if err.raw_os_error() == Some(rustix::io::Errno::XDEV.raw_os_error()) {
                EngineError::Io(format!(
                    "{} and {} are on different filesystems: {err}",
                    from.display(),
                    path.display()
                ))
            } else {
                io_error(&path, &err)
            }
        })?;
        state::sync_dir(&path)?;
        volume_info(id, size_gb, path)
    }

    /// Removes one volume file, unless a VM that exists attaches it (§3.20).
    ///
    /// A leftover snapshot of the volume goes with it; a missing file is `Ok`.
    pub(super) async fn delete_volume(&self, id: &VolumeId) -> Result<(), EngineError> {
        validate_volume_id(id)?;
        let dir = self.volumes_dir();
        let path = volume_path(&dir, id);
        for (vm_id, entry) in self.entries() {
            let Some(record) = entry.spec() else {
                continue;
            };
            if record
                .spec
                .volumes
                .iter()
                .any(|volume| volume.volume_id == id.0)
            {
                return Err(EngineError::Conflict(format!(
                    "volume {} is attached to VM {}",
                    id.0, vm_id.0
                )));
            }
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
        let snapshot = snapshot_path(&dir, id);
        if let Err(err) = std::fs::remove_file(&snapshot)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(io_error(&snapshot, &err));
        }
        Ok(())
    }

    /// Takes the one snapshot a volume may have (§3.16, ADR 0146).
    ///
    /// `Reflink` clones the file. `PauseCopy` pauses the VM that attaches the
    /// volume when it is running, copies the sparse file and always resumes the
    /// VM, whatever the copy's outcome.
    pub(super) async fn snapshot_volume(
        &self,
        id: &VolumeId,
    ) -> Result<VolumeSnapshot, EngineError> {
        validate_volume_id(id)?;
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
        let location = snapshot_path(&dir, id);
        if location.exists() {
            return Err(EngineError::Conflict(format!(
                "volume {} already has a snapshot",
                id.0
            )));
        }

        let length = metadata.len();
        let allocated = metadata.blocks().saturating_mul(512);
        let method = self.volume_method;
        let mut pause = Duration::ZERO;
        match method {
            SnapshotMethod::Reflink => {
                let source = path.clone();
                let clone = location.clone();
                tokio::task::spawn_blocking(move || reflink(&source, &clone))
                    .await
                    .map_err(|err| EngineError::Io(format!("snapshot task failed: {err}")))??;
            }
            SnapshotMethod::PauseCopy => {
                let available = available_bytes(&dir)?;
                if available < allocated {
                    return Err(EngineError::ResourceExhausted(format!(
                        "volume {} uses {allocated} bytes and only {available} are free",
                        id.0
                    )));
                }
                let running = self.running_vm_for_volume(id).await;
                let api = running.as_ref().map(|entry| {
                    ApiClient::new(&entry.paths.api_socket).with_timeout(SNAPSHOT_API_TIMEOUT)
                });
                let started = Instant::now();
                if let Some(api) = &api
                    && let Err(err) = api.pause().await
                {
                    return Err(EngineError::Hypervisor(format!(
                        "cloud-hypervisor did not pause the VM using volume {}: {err}",
                        id.0
                    )));
                }
                let source = path.clone();
                let clone = location.clone();
                // No `?` before the resume: a failed or panicked copy task
                // must still resume the VM.
                let copied = tokio::task::spawn_blocking(move || copy_sparse(&source, &clone))
                    .await
                    .map_err(|err| EngineError::Io(format!("snapshot task failed: {err}")))
                    .and_then(|result| result);
                if api.is_some() {
                    // Resume whatever the copy's outcome; the pause is the
                    // window from before the pause to after the resume.
                    if let Some(api) = &api
                        && let Err(err) = api.resume().await
                    {
                        let _ = std::fs::remove_file(&location);
                        return Err(EngineError::Hypervisor(format!(
                            "cloud-hypervisor did not resume the VM using volume {}: {err}",
                            id.0
                        )));
                    }
                    pause = started.elapsed();
                }
                if let Err(err) = copied {
                    let _ = std::fs::remove_file(&location);
                    return Err(err);
                }
            }
        }

        Ok(VolumeSnapshot {
            volume_id: id.clone(),
            snapshot_id: format!("{}{SNAPSHOT_SUFFIX}", id.0),
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
        let dir = self.volumes_dir();
        let is_snapshot_file = snapshot
            .location
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(SNAPSHOT_SUFFIX));
        if snapshot.location.parent() != Some(dir.as_path()) || !is_snapshot_file {
            return Err(EngineError::Conflict(format!(
                "snapshot {} is not inside the volume directory",
                snapshot.location.display()
            )));
        }
        match std::fs::remove_file(&snapshot.location) {
            Ok(()) => state::sync_dir(&snapshot.location),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(&snapshot.location, &err)),
        }
    }

    /// The running VM that attaches `id`, if any (§3.16).
    async fn running_vm_for_volume(&self, id: &VolumeId) -> Option<Arc<super::engine::VmEntry>> {
        for (vm_id, entry) in self.entries() {
            let Ok((_, record)) = self.entry_with_spec(&vm_id) else {
                continue;
            };
            if !record
                .spec
                .volumes
                .iter()
                .any(|volume| volume.volume_id == id.0)
            {
                continue;
            }
            if matches!(self.inspect(&vm_id).await, Ok(info) if info.state == VmState::Running) {
                return Some(entry);
            }
        }
        None
    }
}

/// How long one pause or resume may take. A paused VM makes no progress, so
/// the call fails rather than leaving it paused forever.
const SNAPSHOT_API_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
    use std::path::{Path, PathBuf};

    use super::{
        copy_sparse, is_crockford, is_volume_serial, probe_method, remove_leftovers, snapshot_path,
        validate_volume_attaches, validate_volume_id, volume_path,
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
            snapshot_path(dir, &id),
            PathBuf::from("/var/lib/quoro/volumes/vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF.snapshot")
        );
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
    fn leftovers_are_removed_and_volumes_are_kept() {
        let dir = scratch_dir("leftovers");
        let id = "vol_01JC9ZC2D3E4F5G6H7J8K9M0NP";
        for name in [
            id.to_string(),
            format!("{id}.snapshot"),
            format!("{id}.tmp"),
        ] {
            std::fs::write(dir.join(&name), b"x").expect("file");
        }
        remove_leftovers(&dir);
        assert!(dir.join(id).exists(), "the volume file stays");
        assert!(!dir.join(format!("{id}.snapshot")).exists());
        assert!(!dir.join(format!("{id}.tmp")).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
