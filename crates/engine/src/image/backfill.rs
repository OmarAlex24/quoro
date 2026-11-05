// SPDX-License-Identifier: AGPL-3.0-only
//! Inventories of images cached before M5a (engine-trait.md §3.21,
//! node-agent-protocol.md §6.11, ADR 0163).
//!
//! An image prepared by an earlier release has its `rootfs.ext4` and
//! `image.json` but no `inventory.json`, and a second preparation of its
//! digest is a cache hit, so nothing would ever inventory it. Here the engine
//! reads the root disk instead: it mounts `rootfs.ext4` read-only through a
//! loop device (`ro,noload,nodev,nosuid,noexec`, type `ext4`, never probed),
//! runs the same parsers on the mounted tree through the same in-root
//! [`Tree`](crate::inventory::Tree) as a fresh preparation, unmounts it and
//! writes the inventory next to the image.
//!
//! The root disk was built by the engine's own `mkfs.ext4 -d` and is attached
//! read-only to every VM, so its metadata was never written by a guest. It is
//! never written here either (`ro` makes the loop device read-only and
//! `noload` skips any journal replay), and nothing on it is executed.
//!
//! One image is mounted at a time, on one mount point of the cache,
//! [`MOUNT_DIR`]. A mount an interrupted agent left there is unmounted before
//! the next one. On a node the agent runs in its own mount namespace, so the
//! mount is invisible on the host; when it is not, the mount point is still
//! the engine's own directory.

use std::path::Path;

use crate::image::cache;
use crate::{EngineError, ImageDigest, ImageInventory};

/// The mount point, below the cache root. Its name is neither a digest nor a
/// scratch directory, so neither the cache nor the leftover sweep touches it.
pub(crate) const MOUNT_DIR: &str = ".mnt-inventory";

/// The mount options: read-only, no journal replay, and no device node,
/// setuid bit or executable honoured from the image.
pub(crate) const MOUNT_OPTIONS: &str = "ro,noload,nodev,nosuid,noexec,loop";

/// Mounts a root disk read-only and unmounts it. Blocking.
pub(crate) trait RootMount: Send + Sync {
    /// Mounts `rootfs` read-only at `at`.
    fn mount(&self, rootfs: &Path, at: &Path) -> Result<(), EngineError>;
    /// Unmounts `at`.
    fn unmount(&self, at: &Path) -> Result<(), EngineError>;
    /// True when something is mounted at `at`.
    fn is_mounted(&self, at: &Path) -> bool;
}

/// The host's `mount` and `umount`, with a loop device.
pub(crate) struct LoopMount;

impl RootMount for LoopMount {
    fn mount(&self, rootfs: &Path, at: &Path) -> Result<(), EngineError> {
        let mount = crate::ch::tools::find_optional("mount").ok_or_else(|| {
            EngineError::Unsupported("mount not found; install util-linux".to_owned())
        })?;
        let rootfs = utf8(rootfs)?;
        let at = utf8(at)?;
        crate::ch::tools::run_blocking(&mount, &["-t", "ext4", "-o", MOUNT_OPTIONS, rootfs, at])
            .map(|_| ())
    }

    fn unmount(&self, at: &Path) -> Result<(), EngineError> {
        let umount = crate::ch::tools::find_optional("umount").ok_or_else(|| {
            EngineError::Unsupported("umount not found; install util-linux".to_owned())
        })?;
        crate::ch::tools::run_blocking(&umount, &[utf8(at)?]).map(|_| ())
    }

    fn is_mounted(&self, at: &Path) -> bool {
        is_mount_point(at)
    }
}

/// True when `path` is a directory on another filesystem than its parent:
/// a loop-mounted ext4 always is.
fn is_mount_point(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    let Some(parent) = path.parent() else {
        return false;
    };
    match (std::fs::metadata(path), std::fs::metadata(parent)) {
        (Ok(own), Ok(parent)) => own.is_dir() && own.dev() != parent.dev(),
        _ => false,
    }
}

fn utf8(path: &Path) -> Result<&str, EngineError> {
    path.to_str()
        .ok_or_else(|| EngineError::Io(format!("{} is not UTF-8", path.display())))
}

/// The committed cache entries with a root disk and no inventory, sorted.
/// Reads only the cache directory.
pub(crate) fn uninventoried(root: &Path) -> Result<Vec<ImageDigest>, EngineError> {
    let entries = std::fs::read_dir(root)
        .map_err(|err| EngineError::Io(format!("{}: {err}", root.display())))?;
    let mut digests = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_digest_hex(name) {
            continue;
        }
        let dir = entry.path();
        if dir.join("image.json").is_file()
            && dir.join("rootfs.ext4").is_file()
            && !dir.join(cache::INVENTORY_FILE).exists()
        {
            digests.push(ImageDigest(format!("sha256:{name}")));
        }
    }
    digests.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(digests)
}

/// 64 lowercase hex digits: the name of a committed cache entry.
fn is_digest_hex(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Inventories the cache entry `<root>/<hex>` from its `rootfs.ext4` and
/// writes `inventory.json` next to it. The caller holds the digest's lock and
/// has checked that the entry is committed and has no inventory.
///
/// A root disk that does not mount is an error and writes nothing, so the
/// next start tries again (§3.21). A mounted tree that cannot be read gives
/// the `inventory_failed` inventory, as a fresh preparation does, and that
/// one is kept.
pub(crate) fn inventory_from_root_disk(
    root: &Path,
    hex: &str,
    digest: ImageDigest,
    mounter: &dyn RootMount,
) -> Result<ImageInventory, EngineError> {
    let entry = root.join(hex);
    let rootfs = entry.join("rootfs.ext4");
    let at = root.join(MOUNT_DIR);
    clear_stale_mount(&at, mounter)?;
    std::fs::create_dir_all(&at)
        .map_err(|err| EngineError::Io(format!("{}: {err}", at.display())))?;

    mounter.mount(&rootfs, &at)?;
    let inventory = super::inventory_of(&at, digest);
    // An unmount that fails leaves the mount for the next call to clear; the
    // inventory read from it is complete either way.
    let _ = mounter.unmount(&at);
    cache::write_inventory_atomic(&entry, &inventory)?;
    Ok(inventory)
}

/// Unmounts whatever an interrupted agent left mounted at `at`.
fn clear_stale_mount(at: &Path, mounter: &dyn RootMount) -> Result<(), EngineError> {
    if !mounter.is_mounted(at) {
        return Ok(());
    }
    mounter.unmount(at)?;
    if mounter.is_mounted(at) {
        return Err(EngineError::Io(format!(
            "{} is still a mount point after umount",
            at.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use super::RootMount;
    use crate::EngineError;

    /// A mounter for unprivileged tests: "mounting" copies a fixture tree to
    /// the mount point, "unmounting" empties it again.
    pub(crate) struct CopyMount {
        pub(crate) fixture: PathBuf,
        pub(crate) mounted: Mutex<HashSet<PathBuf>>,
        pub(crate) mounts: Mutex<Vec<PathBuf>>,
        pub(crate) unmounts: Mutex<usize>,
        pub(crate) fail: Mutex<Option<EngineError>>,
    }

    impl CopyMount {
        pub(crate) fn new(fixture: &Path) -> Self {
            Self {
                fixture: fixture.to_path_buf(),
                mounted: Mutex::new(HashSet::new()),
                mounts: Mutex::new(Vec::new()),
                unmounts: Mutex::new(0),
                fail: Mutex::new(None),
            }
        }
    }

    impl RootMount for CopyMount {
        fn mount(&self, rootfs: &Path, at: &Path) -> Result<(), EngineError> {
            if let Some(err) = self.fail.lock().unwrap().take() {
                return Err(err);
            }
            assert!(
                self.mounted.lock().unwrap().insert(at.to_path_buf()),
                "mounted twice on {}",
                at.display()
            );
            self.mounts.lock().unwrap().push(rootfs.to_path_buf());
            copy_tree(&self.fixture, at);
            Ok(())
        }

        fn unmount(&self, at: &Path) -> Result<(), EngineError> {
            *self.unmounts.lock().unwrap() += 1;
            self.mounted.lock().unwrap().remove(at);
            for entry in std::fs::read_dir(at).expect("mount dir").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(&path).expect("empty mount");
                } else {
                    std::fs::remove_file(&path).expect("empty mount");
                }
            }
            Ok(())
        }

        fn is_mounted(&self, at: &Path) -> bool {
            self.mounted.lock().unwrap().contains(at)
        }
    }

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("dir");
        for entry in std::fs::read_dir(from).expect("fixture").flatten() {
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).expect("copy");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_names_are_64_lowercase_hex() {
        assert!(is_digest_hex(&"ab".repeat(32)));
        assert!(!is_digest_hex(&"AB".repeat(32)));
        assert!(!is_digest_hex(&"ab".repeat(31)));
        assert!(!is_digest_hex(MOUNT_DIR));
    }

    #[test]
    fn the_mount_options_are_read_only_and_inert() {
        for option in ["ro", "noload", "nodev", "nosuid", "noexec", "loop"] {
            assert!(MOUNT_OPTIONS.split(',').any(|o| o == option), "{option}");
        }
        assert!(!MOUNT_OPTIONS.split(',').any(|o| o == "rw"));
    }

    #[test]
    fn a_plain_directory_is_not_a_mount_point() {
        let dir = crate::image::cache::tests_support::temp_dir("backfill-mountpoint");
        let child = dir.join("child");
        std::fs::create_dir_all(&child).expect("child");
        assert!(!is_mount_point(&child));
        assert!(!is_mount_point(&dir.join("missing")));
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
