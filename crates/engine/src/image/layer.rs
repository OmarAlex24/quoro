// SPDX-License-Identifier: AGPL-3.0-only
//! Applying one OCI layer inside a staging root.
//!
//! Every path operation goes through the staging root's directory descriptor:
//! the parent of an entry is opened with
//! `openat2(…, RESOLVE_IN_ROOT | RESOLVE_NO_MAGICLINKS)`, so an absolute
//! symlink (`var/run -> /run`) resolves *inside* the root and a `..` in a
//! symlink target is clamped at the root. The final component is always
//! handled with a `*at` call on that parent descriptor and never followed
//! (`O_NOFOLLOW`, `AT_SYMLINK_NOFOLLOW`). No `std::fs` path is ever built by
//! joining an entry name to the root (engine-trait.md §3.4).
//!
//! A layer is applied in two passes, as OCI requires: whiteouts first (against
//! what the lower layers left), then every other entry.

use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, Dir, FileType, Gid, Mode, OFlags, ResolveFlags, Timespec, Timestamps, Uid, XattrFlags,
    fchmod, fchown, fsetxattr, futimens, linkat, makedev, mkdirat, mknodat, openat, openat2,
    statat, symlinkat, unlinkat, utimensat,
};
use rustix::io::Errno;
use rustix::process::geteuid;
use sha2::{Digest, Sha256};

use crate::EngineError;
use crate::image::hex;

/// Uncompressed OCI layer.
pub const OCI_LAYER_TAR: &str = "application/vnd.oci.image.layer.v1.tar";
/// gzip-compressed OCI layer.
pub const OCI_LAYER_TAR_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
/// Uncompressed Docker layer.
pub const DOCKER_LAYER_TAR: &str = "application/vnd.docker.image.rootfs.diff.tar";
/// gzip-compressed Docker layer.
pub const DOCKER_LAYER_TAR_GZIP: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";
/// zstd-compressed OCI layer (recognised only to reject it by name).
pub const OCI_LAYER_TAR_ZSTD: &str = "application/vnd.oci.image.layer.v1.tar+zstd";
/// zstd-compressed Docker layer (recognised only to reject it by name).
pub const DOCKER_LAYER_TAR_ZSTD: &str = "application/vnd.docker.image.rootfs.diff.tar.zstd";

/// How a layer blob is compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// The blob is a plain tar archive.
    None,
    /// The blob is a gzip-compressed tar archive.
    Gzip,
}

/// Maps a layer media type to its compression.
///
/// # Errors
///
/// `Image` naming the media type. zstd gets a message of its own because it is
/// a known gap, everything else is unsupported or non-distributable.
pub fn compression_for(media_type: &str) -> Result<Compression, EngineError> {
    let base = media_type.split(';').next().unwrap_or(media_type).trim();
    match base {
        OCI_LAYER_TAR | DOCKER_LAYER_TAR => Ok(Compression::None),
        OCI_LAYER_TAR_GZIP | DOCKER_LAYER_TAR_GZIP => Ok(Compression::Gzip),
        OCI_LAYER_TAR_ZSTD | DOCKER_LAYER_TAR_ZSTD => Err(EngineError::Image(format!(
            "zstd-compressed layers are not supported yet (media type {media_type})"
        ))),
        other => Err(EngineError::Image(format!(
            "unsupported layer media type {other:?}"
        ))),
    }
}

/// Counters for one applied layer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LayerStats {
    /// Non-whiteout entries seen in the layer.
    pub entries: usize,
    /// `.wh.<name>` markers applied.
    pub whiteouts: usize,
    /// `.wh..wh..opq` markers applied.
    pub opaque_dirs: usize,
    /// Character and block devices skipped because the engine is not root.
    pub skipped_devices: usize,
    /// `SCHILY.xattr.*` attributes skipped because ownership is not preserved.
    pub skipped_xattrs: usize,
}

impl LayerStats {
    /// Adds another layer's counters into these.
    pub fn merge(&mut self, other: Self) {
        self.entries += other.entries;
        self.whiteouts += other.whiteouts;
        self.opaque_dirs += other.opaque_dirs;
        self.skipped_devices += other.skipped_devices;
        self.skipped_xattrs += other.skipped_xattrs;
    }
}

type LayerReader = Box<dyn Read>;

/// Applies one layer blob on top of whatever the lower layers left in `root`.
///
/// This is synchronous and blocking; the caller runs it on
/// `tokio::task::spawn_blocking`.
///
/// # Errors
///
/// `Image` for a malformed entry path, a missing hard-link target or a
/// `diff_id` mismatch; `Io` for filesystem failures.
pub fn apply_layer(
    root: &Path,
    blob: &Path,
    compression: Compression,
    diff_id: &str,
    preserve_ownership: bool,
) -> Result<LayerStats, EngineError> {
    let root = Root::open(root, preserve_ownership)?;
    let mut stats = LayerStats::default();

    // Pass 1: whiteouts, hashing the decompressed stream on the way.
    {
        let reader = HashingReader::new(open_decoder(blob, compression)?);
        let mut archive = tar::Archive::new(reader);
        {
            let entries = archive.entries().map_err(|err| read_error(blob, &err))?;
            for entry in entries {
                let entry = entry.map_err(|err| read_error(blob, &err))?;
                let path = entry.path_bytes().into_owned();
                let components = entry_components(&path)?;
                let Some(name) = components.last() else {
                    continue;
                };
                let bytes = name.as_bytes();
                let parent_components = &components[..components.len() - 1];
                if bytes == b".wh..wh..opq" {
                    if let Some(parent) = root.open_dirs(parent_components, false)? {
                        root.remove_children(&parent)?;
                        stats.opaque_dirs += 1;
                    }
                } else if let Some(victim) = bytes.strip_prefix(b".wh.") {
                    if victim.is_empty() {
                        return Err(EngineError::Image(format!(
                            "malformed whiteout entry {:?}",
                            String::from_utf8_lossy(&path)
                        )));
                    }
                    if let Some(parent) = root.open_dirs(parent_components, false)? {
                        root.remove(&parent, OsStr::from_bytes(victim))?;
                        stats.whiteouts += 1;
                    }
                }
            }
        }
        // The diff_id covers the whole decompressed stream: read the padding
        // after the tar end marker too.
        let mut reader = archive.into_inner();
        io::copy(&mut reader, &mut io::sink()).map_err(|err| read_error(blob, &err))?;
        let got = reader.finish();
        if got != diff_id {
            return Err(EngineError::Image(format!(
                "layer diff_id mismatch: expected {diff_id}, got {got}"
            )));
        }
    }

    // Pass 2: everything that is not a whiteout.
    let mut deferred: Vec<(Vec<OsString>, Meta)> = Vec::new();
    {
        let mut archive = tar::Archive::new(open_decoder(blob, compression)?);
        let entries = archive.entries().map_err(|err| read_error(blob, &err))?;
        for entry in entries {
            let mut entry = entry.map_err(|err| read_error(blob, &err))?;
            root.apply_entry(&mut entry, &mut stats, &mut deferred)?;
        }
    }
    root.apply_deferred(deferred)?;

    Ok(stats)
}

/// Metadata carried by a tar header that the engine applies.
#[derive(Clone, Copy, Debug)]
struct Meta {
    mode: u16,
    uid: u32,
    gid: u32,
    mtime: i64,
}

impl Meta {
    fn of(header: &tar::Header) -> Result<Self, EngineError> {
        let mode = header
            .mode()
            .map_err(|err| EngineError::Image(format!("layer entry has no mode: {err}")))?;
        let uid = header
            .uid()
            .map_err(|err| EngineError::Image(format!("layer entry has no uid: {err}")))?;
        let gid = header
            .gid()
            .map_err(|err| EngineError::Image(format!("layer entry has no gid: {err}")))?;
        let mtime = header
            .mtime()
            .map_err(|err| EngineError::Image(format!("layer entry has no mtime: {err}")))?;
        Ok(Self {
            mode: (mode & 0o7777) as u16,
            uid: uid as u32,
            gid: gid as u32,
            mtime: mtime as i64,
        })
    }

    fn mode(self) -> Mode {
        Mode::from(u32::from(self.mode))
    }

    fn timestamps(self) -> Timestamps {
        let time = Timespec {
            tv_sec: self.mtime,
            tv_nsec: 0,
        };
        Timestamps {
            last_access: time,
            last_modification: time,
        }
    }
}

/// Splits a tar entry path into in-root components.
///
/// A leading `/` and `.` components are dropped, `..` is clamped at the root,
/// empty names and NUL bytes are refused. The result is empty only for the
/// root entry itself (`./`).
fn entry_components(path: &[u8]) -> Result<Vec<OsString>, EngineError> {
    let mut components: Vec<OsString> = Vec::new();
    for raw in path.split(|byte| *byte == b'/') {
        if raw.is_empty() || raw == b"." {
            continue;
        }
        if raw.contains(&0) {
            return Err(EngineError::Image(format!(
                "layer entry path {:?} contains a NUL byte",
                String::from_utf8_lossy(path)
            )));
        }
        if raw == b".." {
            components.pop();
            continue;
        }
        components.push(OsStr::from_bytes(raw).to_owned());
    }
    Ok(components)
}

/// The staging directory, opened once, plus the ownership policy.
struct Root {
    fd: OwnedFd,
    preserve_ownership: bool,
}

impl Root {
    fn open(path: &Path, preserve_ownership: bool) -> Result<Self, EngineError> {
        let fd = openat(
            rustix::fs::CWD,
            path,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|err| io_error(path, err))?;
        Ok(Self {
            fd,
            preserve_ownership,
        })
    }

    /// A fresh `O_PATH` descriptor for the root itself.
    fn root_fd(&self) -> Result<OwnedFd, EngineError> {
        openat2(
            &self.fd,
            ".",
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            resolve_flags(),
        )
        .map_err(|err| io_error(Path::new("."), err))
    }

    /// Opens `components` relative to the root, resolving symlinks inside it.
    fn open_relative(&self, components: &[OsString]) -> Result<OwnedFd, Errno> {
        openat2(
            &self.fd,
            relative_path(components),
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            resolve_flags(),
        )
    }

    /// Opens an already resolved directory with a real descriptor, so
    /// `fchmod`/`fchown`/`futimens` work on it.
    fn open_real(&self, components: &[OsString]) -> Result<OwnedFd, EngineError> {
        openat2(
            &self.fd,
            relative_path(components),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            resolve_flags(),
        )
        .map_err(|err| io_error(&relative_path(components), err))
    }

    /// Resolves the parent directory of an entry, creating missing
    /// components (mode 0755, owner 0:0) on the way when `create` is set.
    ///
    /// Returns `Ok(None)` when a component is missing and `create` is false.
    fn open_dirs(
        &self,
        components: &[OsString],
        create: bool,
    ) -> Result<Option<OwnedFd>, EngineError> {
        let mut fd = self.root_fd()?;
        for (index, component) in components.iter().enumerate() {
            let prefix = &components[..=index];
            match self.open_relative(prefix) {
                Ok(opened) => fd = opened,
                Err(err) if err == Errno::NOENT => {
                    if !create {
                        return Ok(None);
                    }
                    mkdirat(&fd, component, Mode::from(0o755u32))
                        .map_err(|err| io_error(&relative_path(prefix), err))?;
                    // The umask may have masked mkdirat's mode.
                    rustix::fs::chmodat(&fd, component, Mode::from(0o755u32), AtFlags::empty())
                        .map_err(|err| io_error(&relative_path(prefix), err))?;
                    if self.preserve_ownership {
                        rustix::fs::chownat(
                            &fd,
                            component,
                            Some(Uid::from_raw(0)),
                            Some(Gid::from_raw(0)),
                            AtFlags::SYMLINK_NOFOLLOW,
                        )
                        .map_err(|err| io_error(&relative_path(prefix), err))?;
                    }
                    fd = self
                        .open_relative(prefix)
                        .map_err(|err| io_error(&relative_path(prefix), err))?;
                }
                Err(err) if err == Errno::NOTDIR => {
                    return Err(EngineError::Image(format!(
                        "layer entry parent {} is not a directory",
                        relative_path(prefix).display()
                    )));
                }
                Err(err) => return Err(io_error(&relative_path(prefix), err)),
            }
        }
        Ok(Some(fd))
    }

    /// Deletes `name` under `parent` without following symlinks; a directory
    /// is removed with its whole subtree. A missing entry is fine.
    fn remove(&self, parent: &OwnedFd, name: &OsStr) -> Result<(), EngineError> {
        let stat = match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return Ok(()),
            Err(err) => return Err(io_error(Path::new(name), err)),
        };
        if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
            let fd = openat(
                parent,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|err| io_error(Path::new(name), err))?;
            self.remove_children(&fd)?;
            unlinkat(parent, name, AtFlags::REMOVEDIR)
                .map_err(|err| io_error(Path::new(name), err))?;
        } else {
            unlinkat(parent, name, AtFlags::empty())
                .map_err(|err| io_error(Path::new(name), err))?;
        }
        Ok(())
    }

    /// Deletes every child of `dir`, keeping the directory itself. `dir` may be
    /// an `O_PATH` descriptor, so a readable one is opened first.
    fn remove_children(&self, dir: &OwnedFd) -> Result<(), EngineError> {
        let readable = openat(
            dir,
            ".",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|err| io_error(Path::new("."), err))?;
        let mut names: Vec<CString> = Vec::new();
        {
            let mut reader =
                Dir::read_from(&readable).map_err(|err| io_error(Path::new("."), err))?;
            while let Some(entry) = reader.read() {
                let entry = entry.map_err(|err| io_error(Path::new("."), err))?;
                let name = entry.file_name();
                if name.to_bytes() == b"." || name.to_bytes() == b".." {
                    continue;
                }
                names.push(name.to_owned());
            }
        }
        for name in names {
            self.remove(dir, OsStr::from_bytes(name.to_bytes()))?;
        }
        Ok(())
    }

    /// Applies one non-whiteout entry.
    fn apply_entry(
        &self,
        entry: &mut tar::Entry<'_, LayerReader>,
        stats: &mut LayerStats,
        deferred: &mut Vec<(Vec<OsString>, Meta)>,
    ) -> Result<(), EngineError> {
        let path = entry.path_bytes().into_owned();
        let components = entry_components(&path)?;
        let xattrs = pax_xattrs(entry)?;
        let header = entry.header();
        let kind = header.entry_type();
        let meta = Meta::of(header)?;
        let link_target = entry.link_name_bytes().map(|target| target.into_owned());

        if components.is_empty() {
            // The root entry itself only refreshes the root's metadata.
            deferred.push((Vec::new(), meta));
            return Ok(());
        }
        let name = components
            .last()
            .cloned()
            .ok_or_else(|| EngineError::Image("layer entry has an empty name".into()))?;
        if name.as_bytes().starts_with(b".wh.") {
            // Whiteouts were applied in pass 1.
            return Ok(());
        }

        // A device node that this process may not create leaves the lower
        // layer's entry alone.
        if matches!(kind, tar::EntryType::Char | tar::EntryType::Block)
            && geteuid() != Uid::from_raw(0)
        {
            stats.entries += 1;
            stats.skipped_devices += 1;
            return Ok(());
        }

        let parent_components = components[..components.len() - 1].to_vec();
        let parent = self
            .open_dirs(&parent_components, true)?
            .ok_or_else(|| EngineError::Image("layer entry parent disappeared".into()))?;

        stats.entries += 1;

        if kind.is_dir() {
            let existing = statat(&parent, &name, AtFlags::SYMLINK_NOFOLLOW).ok();
            let existing_is_dir = existing
                .is_some_and(|stat| FileType::from_raw_mode(stat.st_mode) == FileType::Directory);
            if !existing_is_dir {
                self.remove(&parent, &name)?;
                mkdirat(&parent, &name, Mode::from(0o700u32))
                    .map_err(|err| io_error(Path::new(&name), err))?;
            }
            if self.preserve_ownership {
                self.chown(&parent, &name, meta)?;
            }
            deferred.push((components, meta));
            return Ok(());
        }

        // A non-directory replaces whatever is there, directories included.
        self.remove(&parent, &name)?;

        match kind {
            // The tar crate expands GNU sparse entries on read.
            tar::EntryType::Regular | tar::EntryType::Continuous | tar::EntryType::GNUSparse => {
                self.write_file(&parent, &name, entry, &meta, &xattrs, stats)?;
            }
            tar::EntryType::Symlink => {
                let target = link_target.ok_or_else(|| {
                    EngineError::Image(format!("symlink {} has no target", name.to_string_lossy()))
                })?;
                symlinkat(OsStr::from_bytes(&target), &parent, &name)
                    .map_err(|err| io_error(Path::new(&name), err))?;
                if self.preserve_ownership {
                    self.chown(&parent, &name, meta)?;
                }
                utimensat(
                    &parent,
                    &name,
                    &meta.timestamps(),
                    AtFlags::SYMLINK_NOFOLLOW,
                )
                .map_err(|err| io_error(Path::new(&name), err))?;
            }
            tar::EntryType::Link => {
                self.hard_link(&parent, &name, link_target.as_deref())?;
            }
            tar::EntryType::Fifo => {
                mknodat(&parent, &name, FileType::Fifo, Mode::from(0o600u32), 0)
                    .map_err(|err| io_error(Path::new(&name), err))?;
                self.finish_special(&parent, &name, meta)?;
            }
            tar::EntryType::Char | tar::EntryType::Block => {
                let file_type = if kind == tar::EntryType::Char {
                    FileType::CharacterDevice
                } else {
                    FileType::BlockDevice
                };
                let major = header.device_major().ok().flatten().unwrap_or(0);
                let minor = header.device_minor().ok().flatten().unwrap_or(0);
                mknodat(
                    &parent,
                    &name,
                    file_type,
                    Mode::from(0o600u32),
                    makedev(major, minor),
                )
                .map_err(|err| io_error(Path::new(&name), err))?;
                self.finish_special(&parent, &name, meta)?;
            }
            _ => {
                // Unknown entry types (sockets, sparse markers, ...) carry no
                // content this engine can reproduce.
                stats.entries -= 1;
            }
        }
        Ok(())
    }

    /// Links `name` in `parent` to an already applied entry, resolved inside
    /// the root so a symlinked parent works.
    fn hard_link(
        &self,
        parent: &OwnedFd,
        name: &OsStr,
        target: Option<&[u8]>,
    ) -> Result<(), EngineError> {
        let target = target.ok_or_else(|| {
            EngineError::Image(format!(
                "hard link {} has no target",
                name.to_string_lossy()
            ))
        })?;
        let target_components = entry_components(target)?;
        let (target_name, target_parent_components) =
            target_components.split_last().ok_or_else(|| {
                EngineError::Image(format!(
                    "hard link {} points at the root",
                    name.to_string_lossy()
                ))
            })?;
        let missing = || {
            EngineError::Image(format!(
                "hard link {} points at missing {:?}",
                name.to_string_lossy(),
                String::from_utf8_lossy(target)
            ))
        };
        let target_parent = self
            .open_dirs(target_parent_components, false)?
            .ok_or_else(missing)?;
        linkat(target_parent, target_name, parent, name, AtFlags::empty()).map_err(|err| {
            if err == Errno::NOENT {
                missing()
            } else {
                io_error(Path::new(name), err)
            }
        })
    }

    /// Creates a regular file, copies its data and applies its metadata.
    fn write_file(
        &self,
        parent: &OwnedFd,
        name: &OsStr,
        entry: &mut tar::Entry<'_, LayerReader>,
        meta: &Meta,
        xattrs: &[(String, Vec<u8>)],
        stats: &mut LayerStats,
    ) -> Result<(), EngineError> {
        let fd = openat(
            parent,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from(0o600u32),
        )
        .map_err(|err| io_error(Path::new(name), err))?;
        let mut file = File::from(fd);
        io::copy(entry, &mut file).map_err(|err| read_error(Path::new(name), &err))?;
        io::Write::flush(&mut file).map_err(|err| read_error(Path::new(name), &err))?;

        // Ownership first, so setuid/setgid bits survive the mode change.
        if self.preserve_ownership {
            fchown(
                &file,
                Some(Uid::from_raw(meta.uid)),
                Some(Gid::from_raw(meta.gid)),
            )
            .map_err(|err| io_error(Path::new(name), err))?;
        }
        fchmod(&file, meta.mode()).map_err(|err| io_error(Path::new(name), err))?;

        for (key, value) in xattrs {
            let Some(attribute) = key.strip_prefix("SCHILY.xattr.") else {
                continue;
            };
            if !self.preserve_ownership {
                stats.skipped_xattrs += 1;
                continue;
            }
            fsetxattr(&file, attribute, value, XattrFlags::empty())
                .map_err(|err| io_error(Path::new(name), err))?;
        }

        futimens(&file, &meta.timestamps()).map_err(|err| io_error(Path::new(name), err))?;
        Ok(())
    }

    /// Applies ownership, mode and mtime to a freshly created FIFO or device.
    fn finish_special(
        &self,
        parent: &OwnedFd,
        name: &OsStr,
        meta: Meta,
    ) -> Result<(), EngineError> {
        if self.preserve_ownership {
            self.chown(parent, name, meta)?;
        }
        rustix::fs::chmodat(parent, name, meta.mode(), AtFlags::empty())
            .map_err(|err| io_error(Path::new(name), err))?;
        utimensat(parent, name, &meta.timestamps(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|err| io_error(Path::new(name), err))?;
        Ok(())
    }

    fn chown(&self, parent: &OwnedFd, name: &OsStr, meta: Meta) -> Result<(), EngineError> {
        rustix::fs::chownat(
            parent,
            name,
            Some(Uid::from_raw(meta.uid)),
            Some(Gid::from_raw(meta.gid)),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|err| io_error(Path::new(name), err))
    }

    /// Applies the deferred directory metadata, deepest first, so a 0555
    /// directory still accepted its children while the layer was applied.
    fn apply_deferred(&self, mut deferred: Vec<(Vec<OsString>, Meta)>) -> Result<(), EngineError> {
        deferred.sort_by_key(|(components, _)| std::cmp::Reverse(components.len()));
        for (components, meta) in deferred {
            let fd = self.open_real(&components)?;
            if self.preserve_ownership {
                fchown(
                    &fd,
                    Some(Uid::from_raw(meta.uid)),
                    Some(Gid::from_raw(meta.gid)),
                )
                .map_err(|err| io_error(&relative_path(&components), err))?;
            }
            fchmod(&fd, meta.mode()).map_err(|err| io_error(&relative_path(&components), err))?;
            futimens(&fd, &meta.timestamps())
                .map_err(|err| io_error(&relative_path(&components), err))?;
        }
        Ok(())
    }
}

/// The `SCHILY.xattr.*` entries of a pax header, as (key, value) pairs.
fn pax_xattrs(
    entry: &mut tar::Entry<'_, LayerReader>,
) -> Result<Vec<(String, Vec<u8>)>, EngineError> {
    let extensions = entry
        .pax_extensions()
        .map_err(|err| read_error(Path::new(""), &err))?;
    let Some(extensions) = extensions else {
        return Ok(Vec::new());
    };
    let mut xattrs = Vec::new();
    for extension in extensions {
        let extension = extension
            .map_err(|err| EngineError::Image(format!("malformed pax extension: {err}")))?;
        if let Ok(key) = extension.key() {
            xattrs.push((key.to_owned(), extension.value_bytes().to_vec()));
        }
    }
    Ok(xattrs)
}

fn resolve_flags() -> ResolveFlags {
    ResolveFlags::IN_ROOT | ResolveFlags::NO_MAGICLINKS
}

fn relative_path(components: &[OsString]) -> PathBuf {
    if components.is_empty() {
        return PathBuf::from(".");
    }
    let mut path = PathBuf::new();
    for component in components {
        path.push(component);
    }
    path
}

/// A reader that hashes every byte it hands out.
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> String {
        format!("sha256:{}", hex(&self.hasher.finalize()))
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

fn open_decoder(path: &Path, compression: Compression) -> Result<LayerReader, EngineError> {
    let file = File::open(path).map_err(|err| {
        io_error(
            path,
            Errno::from_raw_os_error(err.raw_os_error().unwrap_or(0)),
        )
    })?;
    let reader: LayerReader = match compression {
        Compression::None => Box::new(io::BufReader::new(file)),
        Compression::Gzip => Box::new(GzDecoder::new(io::BufReader::new(file))),
    };
    Ok(reader)
}

fn io_error(path: &Path, err: Errno) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

fn read_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;

    fn setup(label: &str) -> (PathBuf, PathBuf) {
        let base = crate::image::cache::tests_support::temp_dir(label);
        let root = base.join("root");
        std::fs::create_dir(&root).expect("staging root");
        (base, root)
    }

    fn digest_of(bytes: &[u8]) -> String {
        format!("sha256:{}", hex(&Sha256::digest(bytes)))
    }

    fn tar(add: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        add(&mut builder);
        builder.into_inner().expect("tar")
    }

    fn header(kind: tar::EntryType, mode: u32) -> tar::Header {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_mode(mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(1_700_000_000);
        header.set_size(0);
        header
    }

    fn file(builder: &mut tar::Builder<Vec<u8>>, path: &str, data: &[u8]) {
        let mut header = header(tar::EntryType::Regular, 0o644);
        header.set_size(data.len() as u64);
        header.set_cksum();
        builder.append_data(&mut header, path, data).expect("file");
    }

    fn dir(builder: &mut tar::Builder<Vec<u8>>, path: &str, mode: u32) {
        let mut header = header(tar::EntryType::Directory, mode);
        header.set_cksum();
        builder
            .append_data(&mut header, path, io::empty())
            .expect("dir");
    }

    fn symlink(builder: &mut tar::Builder<Vec<u8>>, path: &str, target: &str) {
        let mut header = header(tar::EntryType::Symlink, 0o777);
        header.set_cksum();
        builder
            .append_link(&mut header, path, target)
            .expect("symlink");
    }

    fn hardlink(builder: &mut tar::Builder<Vec<u8>>, path: &str, target: &str) {
        let mut header = header(tar::EntryType::Link, 0o644);
        header.set_cksum();
        builder
            .append_link(&mut header, path, target)
            .expect("hard link");
    }

    fn whiteout(builder: &mut tar::Builder<Vec<u8>>, path: &str) {
        file(builder, path, b"");
    }

    /// Applies `bytes` as a plain tar layer.
    fn apply(base: &Path, root: &Path, bytes: &[u8]) -> Result<LayerStats, EngineError> {
        let blob = base.join("layer.tar");
        std::fs::write(&blob, bytes).expect("layer blob");
        apply_layer(root, &blob, Compression::None, &digest_of(bytes), false)
    }

    #[test]
    fn an_absolute_symlink_parent_resolves_inside_the_root() {
        let (base, root) = setup("layer-abs-symlink");
        let layer = tar(|builder| {
            dir(builder, "var/", 0o755);
            symlink(builder, "var/run", "/run");
            dir(builder, "run/", 0o755);
            file(builder, "var/run/app.pid", b"42");
        });
        apply(&base, &root, &layer).expect("apply");

        assert!(
            std::fs::symlink_metadata(root.join("var/run"))
                .expect("symlink")
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(root.join("run/app.pid")).expect("pid"), b"42");
        assert!(
            !base.join("run").exists(),
            "must not escape the staging root"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_dotdot_symlink_cannot_escape_the_root() {
        let (base, root) = setup("layer-dotdot");
        let layer = tar(|builder| {
            symlink(builder, "evil", "../../..");
            dir(builder, "etc/", 0o755);
            file(builder, "evil/etc/x", b"inside");
        });
        apply(&base, &root, &layer).expect("apply");
        assert_eq!(
            std::fs::read(root.join("etc/x")).expect("inside"),
            b"inside"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_hardlink_through_a_symlinked_parent_is_linked_inside_the_root() {
        let (base, root) = setup("layer-hardlink");
        let layer = tar(|builder| {
            dir(builder, "real/", 0o755);
            file(builder, "real/a", b"data");
            symlink(builder, "link", "real");
            hardlink(builder, "link/b", "real/a");
        });
        apply(&base, &root, &layer).expect("apply");
        let a = std::fs::metadata(root.join("real/a")).expect("a");
        let b = std::fs::metadata(root.join("link/b")).expect("b");
        assert_eq!(a.ino(), b.ino());
        assert_eq!(std::fs::read(root.join("link/b")).expect("b"), b"data");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_missing_hardlink_target_is_an_image_error() {
        let (base, root) = setup("layer-hardlink-missing");
        let layer = tar(|builder| {
            dir(builder, "d/", 0o755);
            hardlink(builder, "d/b", "d/missing");
        });
        let err = apply(&base, &root, &layer).expect_err("missing target");
        assert!(matches!(err, EngineError::Image(_)), "{err}");
        assert!(err.to_string().contains("missing"), "{err}");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_whiteout_removes_a_file_from_the_lower_layer() {
        let (base, root) = setup("layer-whiteout-file");
        apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "d/", 0o755);
                file(builder, "d/a", b"a");
                file(builder, "d/b", b"b");
            }),
        )
        .expect("lower layer");
        let stats = apply(
            &base,
            &root,
            &tar(|builder| {
                whiteout(builder, "d/.wh.a");
            }),
        )
        .expect("whiteout");
        assert_eq!(stats.whiteouts, 1);
        assert!(!root.join("d/a").exists());
        assert!(root.join("d/b").exists());
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_whiteout_of_a_directory_never_follows_a_symlink_out_of_the_root() {
        let outside = crate::image::cache::tests_support::temp_dir("layer-outside");
        let outside_file = outside.join("keep.txt");
        std::fs::write(&outside_file, b"outside").expect("outside file");
        let (base, root) = setup("layer-whiteout-dir");

        let escape = outside.to_str().expect("utf-8 temp path").to_owned();
        apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "d/", 0o755);
                symlink(builder, "d/escape", &escape);
                file(builder, "d/f", b"x");
            }),
        )
        .expect("lower layer");
        let stats = apply(
            &base,
            &root,
            &tar(|builder| {
                whiteout(builder, ".wh.d");
            }),
        )
        .expect("whiteout");
        assert_eq!(stats.whiteouts, 1);
        assert!(!root.join("d").exists());
        assert_eq!(
            std::fs::read(&outside_file).expect("outside survives"),
            b"outside"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
        std::fs::remove_dir_all(&outside).expect("cleanup");
    }

    #[test]
    fn an_opaque_directory_drops_lower_entries_and_keeps_this_layer() {
        let (base, root) = setup("layer-opaque");
        apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "opq/", 0o755);
                file(builder, "opq/old", b"old");
            }),
        )
        .expect("lower layer");
        let stats = apply(
            &base,
            &root,
            &tar(|builder| {
                whiteout(builder, "opq/.wh..wh..opq");
                file(builder, "opq/new", b"new");
            }),
        )
        .expect("opaque");
        assert_eq!(stats.opaque_dirs, 1);
        assert!(!root.join("opq/old").exists());
        assert_eq!(std::fs::read(root.join("opq/new")).expect("new"), b"new");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_directory_replaces_a_symlink() {
        let (base, root) = setup("layer-dir-over-symlink");
        apply(
            &base,
            &root,
            &tar(|builder| {
                file(builder, "target", b"target");
                symlink(builder, "link", "target");
            }),
        )
        .expect("lower layer");
        apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "link/", 0o755);
                file(builder, "link/inside", b"inside");
            }),
        )
        .expect("dir over symlink");
        assert!(std::fs::metadata(root.join("link")).expect("dir").is_dir());
        assert_eq!(
            std::fs::read(root.join("link/inside")).expect("inside"),
            b"inside"
        );
        assert_eq!(
            std::fs::read(root.join("target")).expect("target"),
            b"target"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_file_replaces_a_directory() {
        let (base, root) = setup("layer-file-over-dir");
        apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "x/", 0o755);
                file(builder, "x/y", b"y");
            }),
        )
        .expect("lower layer");
        apply(
            &base,
            &root,
            &tar(|builder| {
                file(builder, "x", b"flat");
            }),
        )
        .expect("file over dir");
        assert_eq!(std::fs::read(root.join("x")).expect("x"), b"flat");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_symlink_replaces_a_file() {
        let (base, root) = setup("layer-symlink-over-file");
        apply(
            &base,
            &root,
            &tar(|builder| {
                file(builder, "s", b"file");
            }),
        )
        .expect("lower layer");
        apply(
            &base,
            &root,
            &tar(|builder| {
                symlink(builder, "s", "/nowhere");
            }),
        )
        .expect("symlink over file");
        let metadata = std::fs::symlink_metadata(root.join("s")).expect("s");
        assert!(metadata.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(root.join("s")).expect("target"),
            PathBuf::from("/nowhere")
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_read_only_directory_still_accepts_its_children() {
        let (base, root) = setup("layer-read-only-dir");
        let stats = apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "ro/", 0o555);
                file(builder, "ro/child", b"child");
            }),
        )
        .expect("apply");
        assert_eq!(stats.entries, 2);
        let mode = std::fs::metadata(root.join("ro"))
            .expect("ro")
            .permissions()
            .mode();
        assert_eq!(mode & 0o7777, 0o555);
        assert_eq!(
            std::fs::read(root.join("ro/child")).expect("child"),
            b"child"
        );
        std::fs::set_permissions(root.join("ro"), std::fs::Permissions::from_mode(0o755))
            .expect("chmod");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_root_entry_only_updates_the_root_metadata() {
        let (base, root) = setup("layer-root-entry");
        let stats = apply(
            &base,
            &root,
            &tar(|builder| {
                dir(builder, "./", 0o755);
                file(builder, "./a", b"a");
            }),
        )
        .expect("apply");
        assert_eq!(stats.entries, 1);
        assert_eq!(std::fs::read(root.join("a")).expect("a"), b"a");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_gzip_layer_applies() {
        use flate2::Compression as GzLevel;
        use flate2::write::GzEncoder;
        use std::io::Write as _;

        let (base, root) = setup("layer-gzip");
        let plain = tar(|builder| {
            dir(builder, "etc/", 0o755);
            file(builder, "etc/os-release", b"ID=quoro\n");
        });
        let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
        encoder.write_all(&plain).expect("gzip");
        let compressed = encoder.finish().expect("gzip");
        let blob = base.join("layer.tar.gz");
        std::fs::write(&blob, &compressed).expect("blob");
        // The diff_id is the digest of the decompressed stream.
        apply_layer(&root, &blob, Compression::Gzip, &digest_of(&plain), false).expect("apply");
        assert_eq!(
            std::fs::read(root.join("etc/os-release")).expect("os-release"),
            b"ID=quoro\n"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn a_diff_id_mismatch_is_rejected() {
        let (base, root) = setup("layer-diff-id");
        let plain = tar(|builder| {
            file(builder, "a", b"a");
        });
        let blob = base.join("layer.tar");
        std::fs::write(&blob, &plain).expect("blob");
        let err = apply_layer(&root, &blob, Compression::None, &digest_of(b"other"), false)
            .expect_err("mismatch");
        assert!(matches!(err, EngineError::Image(_)), "{err}");
        assert!(err.to_string().contains("diff_id mismatch"), "{err}");
        std::fs::remove_dir_all(&base).expect("cleanup");
    }

    #[test]
    fn zstd_layers_are_rejected_by_name() {
        let err = compression_for(OCI_LAYER_TAR_ZSTD).expect_err("zstd");
        assert_eq!(
            err.to_string(),
            "image error: zstd-compressed layers are not supported yet (media type application/vnd.oci.image.layer.v1.tar+zstd)"
        );
        assert!(compression_for(DOCKER_LAYER_TAR_ZSTD).is_err());
    }

    #[test]
    fn known_media_types_map_to_their_compression() {
        assert_eq!(
            compression_for(OCI_LAYER_TAR).expect("oci"),
            Compression::None
        );
        assert_eq!(
            compression_for(OCI_LAYER_TAR_GZIP).expect("oci gzip"),
            Compression::Gzip
        );
        assert_eq!(
            compression_for(DOCKER_LAYER_TAR).expect("docker"),
            Compression::None
        );
        assert_eq!(
            compression_for(DOCKER_LAYER_TAR_GZIP).expect("docker gzip"),
            Compression::Gzip
        );
        assert_eq!(
            compression_for("application/vnd.oci.image.layer.v1.tar+gzip; charset=utf-8")
                .expect("parameterised"),
            Compression::Gzip
        );
        let err = compression_for("application/vnd.oci.image.layer.nondistributable.v1.tar+gzip")
            .expect_err("foreign");
        assert!(
            err.to_string().contains("unsupported layer media type"),
            "{err}"
        );
    }

    #[test]
    fn entry_paths_are_clamped_to_the_root() {
        assert!(entry_components(b"./").expect("root").is_empty());
        assert!(entry_components(b"/").expect("root").is_empty());
        assert_eq!(
            entry_components(b"//a/./b/../c").expect("path"),
            vec![OsString::from("a"), OsString::from("c"),]
        );
        assert!(entry_components(b"a/../../../b").expect("clamped") == vec![OsString::from("b")]);
        assert!(entry_components(b"a\0b").is_err());
    }

    #[test]
    fn a_whiteout_with_an_empty_name_is_refused() {
        let (base, root) = setup("layer-whiteout-empty");
        let layer = tar(|builder| {
            file(builder, ".wh.", b"");
        });
        assert!(apply(&base, &root, &layer).is_err());
        std::fs::remove_dir_all(&base).expect("cleanup");
    }
}
