// SPDX-License-Identifier: AGPL-3.0-only
//! `prepare_image` and `remove_image` (engine-trait.md §3.4, §3.5; D-0061).
//!
//! Pull an OCI image by digest, verify every byte against its digest, apply
//! the layers inside a staging root with in-root path resolution
//! (`openat2(RESOLVE_IN_ROOT)`), build a read-only ext4 root disk with
//! `mkfs.ext4 -d`, record the image's [`ImageConfig`], and cache the result by
//! digest.
//!
//! The engine caches by digest, so a caller that only has the reference the
//! user typed ([`resolve_digest`]) can ask the registry what it points at
//! before preparing it.
//!
//! Only anonymous registry authentication is implemented. Private registries
//! are a later item; a registry that requires credentials fails with the
//! registry's own message.
//!
//! INTERFACE FROZEN BY THE SUB-ORCHESTRATOR: `ImageStore`, `ImageStoreConfig`
//! and their public methods are what the Cloud Hypervisor backend calls. Keep
//! the names and signatures; everything else in this module is yours.
//!
//! [`ImageConfig`]: crate::ImageConfig

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::{EngineError, ImageDigest, ImageInventory, ImageRef, PreparedImage};

pub(crate) mod cache;
pub(crate) mod ext4;
pub(crate) mod layer;
pub(crate) mod layout;
pub(crate) mod oci;
pub(crate) mod registry;
pub(crate) mod tree;

/// Lowercase hexadecimal encoding of `bytes`.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

/// Resolves an image reference to the digest the store caches by.
///
/// A reference that already pins a digest (`name@sha256:…`) resolves offline:
/// the digest is validated and used as is. A tag or a bare name asks the
/// registry for the manifest digest it currently points at, which is how a
/// control plane that stores `ghcr.io/example/app:latest` still gets a stable
/// cache key.
///
/// # Errors
///
/// `Image` when the reference or its digest is malformed, or when the registry
/// cannot be reached or answers with a digest other than `sha256`.
pub async fn resolve_digest(reference: &str) -> Result<ImageDigest, EngineError> {
    registry::resolve_digest(reference).await
}

/// Where the store keeps its files and which tools it runs.
#[derive(Clone, Debug)]
pub struct ImageStoreConfig {
    /// Cache root, for example `/var/lib/quoro/engine/images`. One directory
    /// per digest lives below it, plus a scratch area for in-flight work.
    pub root: PathBuf,
    /// Absolute path of `mkfs.ext4` (e2fsprogs >= 1.43 for `-d`).
    pub mkfs_ext4: PathBuf,
    /// Copy layer ownership (uid, gid) into the rootfs. Production runs as
    /// root with `true`; unprivileged tests set `false`.
    pub preserve_ownership: bool,
}

/// The node's prepared-image cache.
pub struct ImageStore {
    config: ImageStoreConfig,
    /// One async mutex per digest, so concurrent `prepare` calls for the same
    /// image do one pull. The outer mutex is never held across an `.await`.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ImageStore {
    /// Opens (and creates) the cache root.
    ///
    /// Leftover scratch directories from an interrupted run are removed.
    ///
    /// # Errors
    ///
    /// `Io` when the root cannot be created.
    pub fn new(config: ImageStoreConfig) -> Result<Self, EngineError> {
        std::fs::create_dir_all(&config.root).map_err(|err| io_error(&config.root, &err))?;
        cache::remove_leftovers(&config.root)?;
        Ok(Self {
            config,
            locks: Mutex::new(HashMap::new()),
        })
    }

    /// The cached image for `digest`, without network access.
    ///
    /// # Errors
    ///
    /// `Io` or `Image` when a cache entry exists but cannot be read.
    pub fn cached(&self, digest: &ImageDigest) -> Result<Option<PreparedImage>, EngineError> {
        cache::read(&self.config.root, digest)
    }

    /// Pulls, verifies, converts and caches `image` (§3.4). Idempotent by
    /// digest; a cached digest returns without network access. Concurrent
    /// calls for the same digest are serialized.
    ///
    /// # Errors
    ///
    /// `Image` for a malformed reference or digest, a digest mismatch, an
    /// unsupported layer or a registry error; `Io` for local failures.
    pub async fn prepare(&self, image: &ImageRef) -> Result<PreparedImage, EngineError> {
        let hex = registry::digest_hex(&image.digest.0)?;
        if let Some(cached) = self.cached(&image.digest)? {
            return Ok(cached);
        }
        let lock = self.lock_for(hex);
        let _guard = lock.lock().await;
        // Another caller may have finished while we waited for the lock.
        if let Some(cached) = self.cached(&image.digest)? {
            return Ok(cached);
        }
        self.convert_and_commit(image, hex).await
    }

    /// The cached inventory of `digest`, without network access (§3.21).
    ///
    /// # Errors
    ///
    /// `NotFound` when the image is not cached or was cached before
    /// inventories existed; `Image` or `Io` when the file cannot be read.
    pub async fn inventory(&self, digest: &ImageDigest) -> Result<ImageInventory, EngineError> {
        let root = self.config.root.clone();
        let digest = digest.clone();
        tokio::task::spawn_blocking(move || cache::read_inventory(&root, &digest))
            .await
            .map_err(|err| EngineError::Io(format!("inventory read task failed: {err}")))?
    }

    /// Removes a cached image. A missing digest is `Ok`. The caller checks
    /// that no VM uses it (§3.5).
    ///
    /// # Errors
    ///
    /// `Image` for a malformed digest, `Io` when the files cannot be removed.
    pub async fn remove(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        let hex = registry::digest_hex(&digest.0)?;
        let dir = self.config.root.join(hex);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(&dir, &err)),
        }
    }

    /// One mutex per digest; the map lock is released before returning.
    fn lock_for(&self, hex: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = match self.locks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        locks
            .entry(hex.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Does the work in a scratch directory and renames it into place.
    async fn convert_and_commit(
        &self,
        image: &ImageRef,
        hex: &str,
    ) -> Result<PreparedImage, EngineError> {
        let tmp = cache::tmp_dir(&self.config.root, hex);
        let converted = convert(&self.config, image, &tmp).await;
        if let Err(err) = converted {
            remove_dir(&tmp).await;
            return Err(err);
        }
        if let Err(err) = cache::commit(&self.config.root, &tmp, hex) {
            remove_dir(&tmp).await;
            return Err(err);
        }
        self.cached(&image.digest)?.ok_or_else(|| {
            EngineError::Io(format!(
                "cache entry for {} disappeared after commit",
                image.digest.0
            ))
        })
    }
}

/// Pull, apply every layer, build the ext4 image and record the metadata, all
/// inside `tmp`.
async fn convert(
    config: &ImageStoreConfig,
    image: &ImageRef,
    tmp: &Path,
) -> Result<(), EngineError> {
    let blobs = tmp.join("blobs");
    tokio::fs::create_dir_all(&blobs)
        .await
        .map_err(|err| io_error(&blobs, &err))?;

    let pulled = registry::pull(image, &blobs).await?;
    finalize(config, image, &pulled.config, pulled.layers, tmp).await
}

/// The shared tail of every conversion: apply `layers` to a staging tree,
/// build the ext4 root disk, write `image.json` and drop the scratch data.
///
/// `tmp` holds the layer blobs under `tmp/blobs` and becomes the cache entry,
/// so the caller only has to have produced the blobs and their `diff_ids`.
pub(crate) async fn finalize(
    config: &ImageStoreConfig,
    image: &ImageRef,
    image_config: &crate::ImageConfig,
    layers: Vec<registry::PulledLayer>,
    tmp: &Path,
) -> Result<(), EngineError> {
    let staging = tmp.join("staging");
    tokio::fs::create_dir_all(&staging)
        .await
        .map_err(|err| io_error(&staging, &err))?;

    let mut stats = layer::LayerStats::default();
    for (index, blob) in layers.iter().enumerate() {
        let root = staging.clone();
        let path = blob.path.clone();
        let diff_id = blob.diff_id.clone();
        let compression = blob.compression;
        let preserve_ownership = config.preserve_ownership;
        let applied = tokio::task::spawn_blocking(move || {
            layer::apply_layer(&root, &path, compression, &diff_id, preserve_ownership)
        })
        .await
        .map_err(|err| EngineError::Io(format!("layer {index} conversion task failed: {err}")))?;
        stats.merge(applied?);
    }
    let _ = stats;

    // The package inventory, from the tree that is about to become the root
    // disk (§3.21, ADR 0151). It never fails the preparation: a tree that
    // cannot be opened gives an inventory that says so, and an inventory that
    // cannot be written leaves the image without one (reported `pending`).
    let tree_root = staging.clone();
    let digest = image.digest.clone();
    let inventory = tokio::task::spawn_blocking(move || inventory_of(&tree_root, digest)).await;
    if let Ok(inventory) = inventory {
        let _ = cache::write_inventory(tmp, &inventory);
    }

    let rootfs = tmp.join("rootfs.ext4");
    let mkfs = config.mkfs_ext4.clone();
    let source = staging.clone();
    let target = rootfs.clone();
    let size_bytes = tokio::task::spawn_blocking(move || ext4::build(&mkfs, &source, &target))
        .await
        .map_err(|err| EngineError::Io(format!("ext4 build task failed: {err}")))??;

    cache::write_entry(tmp, image, image_config, size_bytes)?;

    // Only the finished artifacts remain before the rename.
    remove_dir(&staging).await;
    remove_dir(&tmp.join("blobs")).await;
    Ok(())
}

/// Inventories the unpacked tree at `root`.
fn inventory_of(root: &Path, digest: ImageDigest) -> ImageInventory {
    let now = tree::rfc3339_now();
    match tree::RootTree::open(root) {
        Ok(tree) => crate::inventory::build(&tree, digest, now),
        Err(_) => ImageInventory {
            digest,
            os: None,
            ecosystems: Vec::new(),
            not_inventoried: vec!["inventory_failed".to_owned()],
            packages: Vec::new(),
            inventoried_at: now,
        },
    }
}

/// Removes a scratch directory, ignoring a missing one.
async fn remove_dir(path: &Path) {
    let _ = tokio::fs::remove_dir_all(path).await;
}

fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(root: &Path) -> ImageStore {
        ImageStore::new(ImageStoreConfig {
            root: root.to_path_buf(),
            mkfs_ext4: PathBuf::from("/bin/true"),
            preserve_ownership: false,
        })
        .expect("store")
    }

    fn digest(hex: &str) -> ImageDigest {
        ImageDigest(format!("sha256:{hex}"))
    }

    const HEX: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

    #[test]
    fn new_removes_leftover_scratch_directories() {
        let root = crate::image::cache::tests_support::temp_dir("store-new");
        let leftover = root.join(format!(".tmp-{HEX}-1-0"));
        std::fs::create_dir_all(leftover.join("blobs")).expect("leftover");
        let store = store(&root);
        assert!(!leftover.exists());
        drop(store);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn cached_round_trips_a_committed_entry() {
        let root = crate::image::cache::tests_support::temp_dir("store-cached");
        let store = store(&root);
        let image = ImageRef {
            reference: "docker.io/library/alpine:3.22".into(),
            digest: digest(HEX),
        };
        let config = crate::ImageConfig {
            entrypoint: vec!["/bin/sh".into()],
            cmd: Vec::new(),
            env: vec![("PATH".into(), "/usr/bin".into())],
            user: None,
            workdir: Some("/".into()),
            exposed_ports: vec![80],
        };

        let tmp = cache::tmp_dir(&root, HEX);
        std::fs::create_dir_all(&tmp).expect("tmp");
        std::fs::write(tmp.join("rootfs.ext4"), vec![0u8; 4096]).expect("rootfs");
        cache::write_entry(&tmp, &image, &config, 4096).expect("image.json");
        cache::commit(&root, &tmp, HEX).expect("commit");

        let cached = store
            .cached(&image.digest)
            .expect("cached")
            .expect("present");
        assert_eq!(cached.digest, image.digest);
        assert_eq!(cached.size_bytes, 4096);
        assert_eq!(cached.config, config);
        assert!(cached.rootfs_path.is_absolute());
        assert!(cached.rootfs_path.ends_with("rootfs.ext4"));
        assert!(cached.rootfs_path.exists());

        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn cached_is_none_without_an_entry() {
        let root = crate::image::cache::tests_support::temp_dir("store-missing");
        let store = store(&root);
        assert!(store.cached(&digest(HEX)).expect("cached").is_none());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn cached_rejects_a_malformed_digest() {
        let root = crate::image::cache::tests_support::temp_dir("store-bad-digest");
        let store = store(&root);
        assert!(matches!(
            store.cached(&digest("nope")),
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn resolve_digest_uses_a_pinned_digest_without_network() {
        let reference = format!("docker.io/library/alpine@sha256:{HEX}");
        assert_eq!(
            resolve_digest(&reference).await.expect("pinned digest"),
            digest(HEX)
        );
    }

    #[tokio::test]
    async fn resolve_digest_rejects_a_malformed_pin_without_network() {
        for reference in [
            "docker.io/library/alpine@sha256:beef".to_string(),
            "docker.io/library/alpine@sha512:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".to_string(),
            String::new(),
        ] {
            assert!(
                matches!(
                    resolve_digest(&reference).await,
                    Err(EngineError::Image(_))
                ),
                "{reference:?} should be refused before any network use"
            );
        }
    }

    #[tokio::test]
    async fn prepare_rejects_a_malformed_digest_before_any_network_use() {
        let root = crate::image::cache::tests_support::temp_dir("store-prepare-bad");
        let store = store(&root);
        let image = ImageRef {
            reference: "docker.io/library/alpine:3.22".into(),
            digest: digest("deadbeef"),
        };
        assert!(matches!(
            store.prepare(&image).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn prepare_serves_a_cached_digest_without_network() {
        let root = crate::image::cache::tests_support::temp_dir("store-prepare-hit");
        let store = store(&root);
        let image = ImageRef {
            reference: "docker.io/library/alpine:3.22".into(),
            digest: digest(HEX),
        };
        let tmp = cache::tmp_dir(&root, HEX);
        std::fs::create_dir_all(&tmp).expect("tmp");
        std::fs::write(tmp.join("rootfs.ext4"), b"rootfs").expect("rootfs");
        cache::write_entry(
            &tmp,
            &image,
            &crate::ImageConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
                exposed_ports: Vec::new(),
            },
            6,
        )
        .expect("image.json");
        cache::commit(&root, &tmp, HEX).expect("commit");
        // A reference the parser would reject proves no network path ran.
        let prepared = store.prepare(&image).await.expect("cached prepare");
        assert_eq!(prepared.size_bytes, 6);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn remove_deletes_an_entry_and_tolerates_a_missing_one() {
        let root = crate::image::cache::tests_support::temp_dir("store-remove");
        let store = store(&root);
        let image = ImageRef {
            reference: "docker.io/library/alpine:3.22".into(),
            digest: digest(HEX),
        };
        let tmp = cache::tmp_dir(&root, HEX);
        std::fs::create_dir_all(&tmp).expect("tmp");
        std::fs::write(tmp.join("rootfs.ext4"), b"rootfs").expect("rootfs");
        cache::write_entry(
            &tmp,
            &image,
            &crate::ImageConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
                exposed_ports: Vec::new(),
            },
            6,
        )
        .expect("image.json");
        cache::commit(&root, &tmp, HEX).expect("commit");
        assert!(store.cached(&image.digest).expect("cached").is_some());

        store.remove(&image.digest).await.expect("remove");
        assert!(store.cached(&image.digest).expect("cached").is_none());
        store.remove(&image.digest).await.expect("remove again");
        assert!(matches!(
            store.remove(&digest("zz")).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}

/// Live registry tests. They are `#[ignore]` and additionally require
/// `QUORO_ENGINE_NET_TESTS=1`, so a normal `cargo test` never touches the
/// network. Run them as root with a cache root on disk, for example:
///
/// ```text
/// sudo -E env "PATH=$PATH" QUORO_ENGINE_NET_TESTS=1 \
///   QUORO_IMAGE_TEST_ROOT=/var/tmp/quoro-images \
///   QUORO_PRESERVE_OWNERSHIP=1 \
///   cargo test -p quoro-engine image::live_tests -- --ignored
/// ```
#[cfg(test)]
mod live_tests {
    use super::*;
    use std::time::SystemTime;

    /// Digest of `docker.io/library/alpine:3.22` (multi-platform index).
    const ALPINE_INDEX: &str =
        "sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f";
    /// Digest of `docker.io/nginxinc/nginx-unprivileged:1.29-alpine`.
    const NGINX_INDEX: &str =
        "sha256:0c79d56aee561a1d81c63f00eee5fb5fe29279560cdc55e91425133104c7fbe6";
    /// Digest of `docker.io/library/debian:13-slim`.
    const DEBIAN_INDEX: &str =
        "sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4";

    fn enabled() -> bool {
        std::env::var("QUORO_ENGINE_NET_TESTS").as_deref() == Ok("1")
    }

    fn test_root(label: &str) -> PathBuf {
        let base = std::env::var_os("QUORO_IMAGE_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!("quoro-live-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    fn store(root: &Path) -> ImageStore {
        let mkfs = std::env::var_os("QUORO_MKFS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("mkfs.ext4"));
        let preserve_ownership = std::env::var("QUORO_PRESERVE_OWNERSHIP").as_deref() == Ok("1");
        ImageStore::new(ImageStoreConfig {
            root: root.to_path_buf(),
            mkfs_ext4: mkfs,
            preserve_ownership,
        })
        .expect("store")
    }

    fn image(reference: &str, digest: &str) -> ImageRef {
        ImageRef {
            reference: reference.into(),
            digest: ImageDigest(digest.into()),
        }
    }

    fn mtime(path: &Path) -> SystemTime {
        std::fs::metadata(path)
            .expect("stat")
            .modified()
            .expect("mtime")
    }

    /// Removes the scratch cache root unless the caller wants to inspect the
    /// converted rootfs afterwards (`QUORO_KEEP_SCRATCH=1`).
    fn cleanup(root: &Path) {
        if std::env::var("QUORO_KEEP_SCRATCH").as_deref() == Ok("1") {
            return;
        }
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "network: set QUORO_ENGINE_NET_TESTS=1"]
    async fn alpine_by_digest_and_a_second_prepare_from_cache() {
        if !enabled() {
            return;
        }
        let root = test_root("alpine");
        let store = store(&root);
        let image = image("docker.io/library/alpine:3.22", ALPINE_INDEX);

        let first = store.prepare(&image).await.expect("first prepare");
        assert!(first.size_bytes > 0);
        let inventory = store.inventory(&image.digest).await.expect("inventory");
        assert_eq!(inventory.ecosystems, vec!["Alpine:v3.22".to_owned()]);
        assert!(
            inventory
                .packages
                .iter()
                .any(|p| p.name == "libcrypto3" && p.source_name.as_deref() == Some("openssl"))
        );
        assert_eq!(first.digest, image.digest);
        let stamp = mtime(&first.rootfs_path);

        let second = store.prepare(&image).await.expect("second prepare");
        assert_eq!(second, first, "the second prepare must come from cache");
        assert_eq!(mtime(&first.rootfs_path), stamp, "cache must not rebuild");

        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .expect("read root")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "scratch directories left behind");
        cleanup(&root);
    }

    #[tokio::test]
    #[ignore = "network: set QUORO_ENGINE_NET_TESTS=1"]
    async fn nginx_unprivileged_by_digest() {
        if !enabled() {
            return;
        }
        let root = test_root("nginx");
        let store = store(&root);
        let prepared = store
            .prepare(&image(
                "docker.io/nginxinc/nginx-unprivileged:1.29-alpine",
                NGINX_INDEX,
            ))
            .await
            .expect("prepare");
        assert!(prepared.size_bytes > 0);
        assert_eq!(prepared.config.exposed_ports, vec![8080]);
        cleanup(&root);
    }

    #[tokio::test]
    #[ignore = "network: set QUORO_ENGINE_NET_TESTS=1"]
    async fn debian_with_an_absolute_symlink_in_the_layers() {
        if !enabled() {
            return;
        }
        let root = test_root("debian");
        let store = store(&root);
        let prepared = store
            .prepare(&image("docker.io/library/debian:13-slim", DEBIAN_INDEX))
            .await
            .expect("prepare");
        assert!(prepared.size_bytes > 0);
        // `/var/run -> /run` must have been resolved inside the root; the
        // conversion succeeding already proves no entry escaped it.
        let inventory = store.inventory(&prepared.digest).await.expect("inventory");
        assert_eq!(
            inventory.os.as_ref().map(|os| os.0.as_str()),
            Some("debian")
        );
        assert!(inventory.ecosystems.contains(&"Debian:13".to_owned()));
        assert!(
            inventory
                .packages
                .iter()
                .any(|p| p.source_name.as_deref() == Some("openssl")),
            "{:?}",
            inventory.packages
        );
        assert!(
            inventory.packages.len() > 50,
            "{}",
            inventory.packages.len()
        );
        cleanup(&root);
    }
}
