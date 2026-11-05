// SPDX-License-Identifier: AGPL-3.0-only
//! On-disk layout of the prepared-image cache.
//!
//! ```text
//! <root>/<64hex>/rootfs.ext4
//! <root>/<64hex>/image.json
//! <root>/<64hex>/inventory.json     (from M5a; absent for older entries)
//! <root>/.tmp-<64hex>-<pid>-<counter>/{blobs,staging,rootfs.ext4,image.json}
//! ```
//!
//! A conversion writes into its scratch directory and the `rename` into
//! `<root>/<64hex>` is the commit point, so a crash never leaves a half-built
//! entry that `cached()` would trust.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::image::registry::digest_hex;
use crate::{
    EngineError, ImageConfig, ImageDigest, ImageInventory, ImageRef, InventoryPackage,
    PreparedImage,
};

/// Scratch directories all share this prefix.
const TMP_PREFIX: &str = ".tmp-";

/// `image.json`: what `cached()` needs without the registry.
#[derive(Debug, Deserialize, Serialize)]
struct CachedEntry {
    digest: String,
    reference: String,
    size_bytes: u64,
    entrypoint: Vec<String>,
    cmd: Vec<String>,
    env: Vec<(String, String)>,
    user: Option<String>,
    workdir: Option<String>,
    exposed_ports: Vec<u16>,
}

impl CachedEntry {
    fn new(image: &ImageRef, config: &ImageConfig, size_bytes: u64) -> Self {
        Self {
            digest: image.digest.0.clone(),
            reference: image.reference.clone(),
            size_bytes,
            entrypoint: config.entrypoint.clone(),
            cmd: config.cmd.clone(),
            env: config.env.clone(),
            user: config.user.clone(),
            workdir: config.workdir.clone(),
            exposed_ports: config.exposed_ports.clone(),
        }
    }

    fn config(&self) -> ImageConfig {
        ImageConfig {
            entrypoint: self.entrypoint.clone(),
            cmd: self.cmd.clone(),
            env: self.env.clone(),
            user: self.user.clone(),
            workdir: self.workdir.clone(),
            exposed_ports: self.exposed_ports.clone(),
        }
    }
}

/// Removes scratch directories left behind by an interrupted run.
pub(crate) fn remove_leftovers(root: &Path) -> Result<(), EngineError> {
    let entries = std::fs::read_dir(root).map_err(|err| io_error(root, &err))?;
    for entry in entries {
        let entry = entry.map_err(|err| io_error(root, &err))?;
        let name = entry.file_name();
        if name.as_encoded_bytes().starts_with(TMP_PREFIX.as_bytes()) {
            let path = entry.path();
            std::fs::remove_dir_all(&path).map_err(|err| io_error(&path, &err))?;
        }
    }
    Ok(())
}

/// A fresh scratch directory for one conversion.
pub(crate) fn tmp_dir(root: &Path, hex: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    root.join(format!(
        "{TMP_PREFIX}{hex}-{}-{counter}",
        std::process::id()
    ))
}

/// Writes `<dir>/image.json`.
pub(crate) fn write_entry(
    dir: &Path,
    image: &ImageRef,
    config: &ImageConfig,
    size_bytes: u64,
) -> Result<(), EngineError> {
    let entry = CachedEntry::new(image, config, size_bytes);
    let bytes = serde_json::to_vec_pretty(&entry)
        .map_err(|err| EngineError::Io(format!("serializing image metadata: {err}")))?;
    let path = dir.join("image.json");
    std::fs::write(&path, bytes).map_err(|err| io_error(&path, &err))
}

/// Reads a cache entry, or `None` when none is committed for `digest`.
pub(crate) fn read(
    root: &Path,
    digest: &ImageDigest,
) -> Result<Option<PreparedImage>, EngineError> {
    let hex = digest_hex(&digest.0)?;
    let dir = root.join(hex);
    let metadata_path = dir.join("image.json");
    let bytes = match std::fs::read(&metadata_path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_error(&metadata_path, &err)),
    };
    let entry: CachedEntry = serde_json::from_slice(&bytes).map_err(|err| {
        EngineError::Image(format!(
            "cache entry {} is malformed: {err}",
            metadata_path.display()
        ))
    })?;
    let rootfs = dir.join("rootfs.ext4");
    let metadata = match std::fs::metadata(&rootfs) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_error(&rootfs, &err)),
    };
    let rootfs_path = if rootfs.is_absolute() {
        rootfs
    } else {
        std::path::absolute(&rootfs).map_err(|err| io_error(&rootfs, &err))?
    };
    let config = entry.config();
    Ok(Some(PreparedImage {
        digest: ImageDigest(entry.digest),
        rootfs_path,
        size_bytes: metadata.len(),
        config,
    }))
}

/// `inventory.json`: the image's package inventory (engine-trait.md §3.21).
#[derive(Debug, Deserialize, Serialize)]
struct CachedInventory {
    digest: String,
    os: Option<(String, String)>,
    ecosystems: Vec<String>,
    not_inventoried: Vec<String>,
    packages: Vec<CachedPackage>,
    inventoried_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CachedPackage {
    ecosystem: String,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_name: Option<String>,
    version: String,
    purl: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

/// Writes `<dir>/inventory.json`.
pub(crate) fn write_inventory(dir: &Path, inventory: &ImageInventory) -> Result<(), EngineError> {
    let cached = CachedInventory {
        digest: inventory.digest.0.clone(),
        os: inventory.os.clone(),
        ecosystems: inventory.ecosystems.clone(),
        not_inventoried: inventory.not_inventoried.clone(),
        packages: inventory
            .packages
            .iter()
            .map(|p| CachedPackage {
                ecosystem: p.ecosystem.clone(),
                name: p.name.clone(),
                source_name: p.source_name.clone(),
                version: p.version.clone(),
                purl: p.purl.clone(),
                path: p.path.clone(),
            })
            .collect(),
        inventoried_at: inventory.inventoried_at.clone(),
    };
    let bytes = serde_json::to_vec(&cached)
        .map_err(|err| EngineError::Io(format!("serializing the image inventory: {err}")))?;
    let path = dir.join("inventory.json");
    std::fs::write(&path, bytes).map_err(|err| io_error(&path, &err))
}

/// Reads the committed inventory of `digest`: `NotFound` when the image is
/// not cached or was cached before inventories existed (§3.21).
pub(crate) fn read_inventory(
    root: &Path,
    digest: &ImageDigest,
) -> Result<ImageInventory, EngineError> {
    let hex = digest_hex(&digest.0)?;
    let dir = root.join(hex);
    if !dir.join("image.json").exists() {
        return Err(EngineError::NotFound);
    }
    let path = dir.join("inventory.json");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::NotFound);
        }
        Err(err) => return Err(io_error(&path, &err)),
    };
    let cached: CachedInventory = serde_json::from_slice(&bytes).map_err(|err| {
        EngineError::Image(format!("inventory {} is malformed: {err}", path.display()))
    })?;
    Ok(ImageInventory {
        digest: ImageDigest(cached.digest),
        os: cached.os,
        ecosystems: cached.ecosystems,
        not_inventoried: cached.not_inventoried,
        packages: cached
            .packages
            .into_iter()
            .map(|p| InventoryPackage {
                ecosystem: p.ecosystem,
                name: p.name,
                source_name: p.source_name,
                version: p.version,
                purl: p.purl,
                path: p.path,
            })
            .collect(),
        inventoried_at: cached.inventoried_at,
    })
}

/// Commits `tmp` as `<root>/<hex>` by renaming it into place.
pub(crate) fn commit(root: &Path, tmp: &Path, hex: &str) -> Result<(), EngineError> {
    let target = root.join(hex);
    if target.exists() {
        // A concurrent process committed the same digest first.
        std::fs::remove_dir_all(tmp).map_err(|err| io_error(tmp, &err))?;
        return Ok(());
    }
    std::fs::rename(tmp, &target).map_err(|err| io_error(tmp, &err))
}

fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

/// Helpers for tests in this module tree.
#[cfg(test)]
pub(crate) mod tests_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique, empty directory below the system temporary directory.
    pub(crate) fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quoro-image-{label}-{}-{counter}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("temp dir");
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    fn image() -> ImageRef {
        ImageRef {
            reference: "docker.io/library/alpine:3.22".into(),
            digest: ImageDigest(format!("sha256:{HEX}")),
        }
    }

    fn config() -> ImageConfig {
        ImageConfig {
            entrypoint: vec!["/bin/sh".into()],
            cmd: vec!["-c".into(), "true".into()],
            env: vec![("PATH".into(), "/usr/bin".into())],
            user: Some("1000:1000".into()),
            workdir: Some("/srv".into()),
            exposed_ports: vec![80, 443],
        }
    }

    fn entry(root: &Path) {
        let tmp = tmp_dir(root, HEX);
        std::fs::create_dir_all(&tmp).expect("tmp");
        std::fs::write(tmp.join("rootfs.ext4"), vec![7u8; 8192]).expect("rootfs");
        write_entry(&tmp, &image(), &config(), 8192).expect("image.json");
        commit(root, &tmp, HEX).expect("commit");
    }

    #[test]
    fn a_committed_entry_round_trips() {
        let root = tests_support::temp_dir("cache-round-trip");
        entry(&root);
        let prepared = read(&root, &image().digest)
            .expect("read")
            .expect("present");
        assert_eq!(prepared.digest, image().digest);
        assert_eq!(prepared.size_bytes, 8192);
        assert_eq!(prepared.config, config());
        assert!(prepared.rootfs_path.is_absolute());
        assert!(prepared.rootfs_path.exists());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn a_missing_entry_is_none() {
        let root = tests_support::temp_dir("cache-missing");
        assert!(read(&root, &image().digest).expect("read").is_none());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn an_entry_without_a_rootfs_is_not_served() {
        let root = tests_support::temp_dir("cache-no-rootfs");
        entry(&root);
        std::fs::remove_file(root.join(HEX).join("rootfs.ext4")).expect("remove");
        assert!(read(&root, &image().digest).expect("read").is_none());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn malformed_metadata_is_an_image_error() {
        let root = tests_support::temp_dir("cache-malformed");
        let dir = root.join(HEX);
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::write(dir.join("image.json"), b"{not json").expect("write");
        assert!(matches!(
            read(&root, &image().digest),
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn leftovers_are_removed_and_the_committed_entry_is_kept() {
        let root = tests_support::temp_dir("cache-leftovers");
        entry(&root);
        let stale = root.join(format!("{}.tmp-{HEX}-999-0", ""));
        std::fs::create_dir_all(stale.join("blobs")).expect("stale");
        remove_leftovers(&root).expect("cleanup leftovers");
        assert!(!stale.exists());
        assert!(read(&root, &image().digest).expect("read").is_some());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn scratch_directories_are_unique_per_call() {
        let root = tests_support::temp_dir("cache-tmp");
        let first = tmp_dir(&root, HEX);
        let second = tmp_dir(&root, HEX);
        assert_ne!(first, second);
        assert!(
            first
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".tmp-") && name.contains(HEX))
        );
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}
