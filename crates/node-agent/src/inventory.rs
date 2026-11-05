// SPDX-License-Identifier: AGPL-3.0-only
//! The node's image-inventory transport (`node-agent-protocol.md` sections
//! 5.26 and 6.11; ADR 0150).
//!
//! [`InventoryHub`] is shared by the reconciler, the build manager and the
//! session. The first two announce every image they prepared; the session
//! drains the resulting queue into `image_inventory` parts and reports the
//! inventory hashes in `status_report.images`. An inventory that cannot be
//! read never fails the caller: it is logged and skipped.
//!
//! The engine is type-erased here for the same reason as in
//! [`crate::interactive`]: the session is not generic over the backend.
//!
//! [`spawn_backfill`] covers images the node already held from a release
//! before M5a, which have no inventory: once after the agent starts, in the
//! background, the engine inventories each of them from its root disk and the
//! hub announces it exactly like a freshly prepared image (section 6.11,
//! `engine-trait.md` section 3.21, ADR 0163).

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use quoro_engine::{Engine, EngineError, ImageDigest, ImageInventory};
use quoro_proto::inventory::{InventoryObject, sha256_hex, split};
use quoro_proto::wire::{
    ImageInventoryBody, ImageStatus, InventoryOs, InventoryPackageWire, MAX_MESSAGE_BYTES,
};
use tokio::sync::Notify;
use tracing::{info, warn};

/// Room left below [`MAX_MESSAGE_BYTES`] for the envelope and ids.
const FRAME_HEADROOM: usize = 16 * 1024;
/// Most inventory hashes remembered before the cache is trimmed.
const CACHE_ENTRIES: usize = 512;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The one engine call inventories need, erased so the session stays
/// non-generic.
trait InventoryEngine: Send + Sync {
    fn image_inventory<'a>(
        &'a self,
        digest: &'a ImageDigest,
    ) -> BoxFuture<'a, Result<ImageInventory, EngineError>>;
}

impl<E: Engine + 'static> InventoryEngine for E {
    fn image_inventory<'a>(
        &'a self,
        digest: &'a ImageDigest,
    ) -> BoxFuture<'a, Result<ImageInventory, EngineError>> {
        Box::pin(Engine::image_inventory(self, digest))
    }
}

/// The image had no inventory to resend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotFound;

struct Inner {
    engine: Arc<dyn InventoryEngine>,
    /// Images waiting for the session to send their inventory.
    queue: Mutex<VecDeque<ImageDigest>>,
    /// Images prepared since this agent started.
    prepared: Mutex<HashSet<String>>,
    /// Digest to its inventory hash; `None` when the engine has no inventory.
    cache: Mutex<HashMap<String, Option<String>>>,
    notify: Notify,
}

/// The node-wide inventory transport, shared by every loop.
#[derive(Clone)]
pub struct InventoryHub {
    inner: Arc<Inner>,
}

impl InventoryHub {
    /// Wraps the engine the node runs.
    #[must_use]
    pub fn new<E: Engine + 'static>(engine: Arc<E>) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine,
                queue: Mutex::new(VecDeque::new()),
                prepared: Mutex::new(HashSet::new()),
                cache: Mutex::new(HashMap::new()),
                notify: Notify::new(),
            }),
        }
    }

    /// Records that an image was just prepared and queues its inventory.
    pub fn notify_prepared(&self, digest: ImageDigest) {
        // A freshly prepared image may now have an inventory where the cache
        // remembered `None` for an unprepared spec digest.
        self.inner.cache.lock().unwrap().remove(&digest.0);
        self.inner.prepared.lock().unwrap().insert(digest.0.clone());
        self.inner.queue.lock().unwrap().push_back(digest);
        self.inner.notify.notify_one();
    }

    /// Queues an image's inventory again at the primary's request.
    ///
    /// # Errors
    ///
    /// [`NotFound`] when the engine has no inventory for the image.
    pub async fn request_resend(&self, digest: &str) -> Result<(), NotFound> {
        let digest = ImageDigest(digest.to_owned());
        match self.inner.engine.image_inventory(&digest).await {
            Ok(_) => {
                self.inner.queue.lock().unwrap().push_back(digest);
                self.inner.notify.notify_one();
                Ok(())
            }
            Err(_) => Err(NotFound),
        }
    }

    /// Waits until an inventory was queued or a resend was requested.
    pub async fn notified(&self) {
        self.inner.notify.notified().await;
    }

    /// The `status_report.images` entries (section 5.3): every image in the
    /// current spec plus every image prepared since start that the engine
    /// still holds, sorted and deduplicated.
    pub async fn images(&self, spec_digests: &[String]) -> Vec<ImageStatus> {
        let spec: HashSet<&str> = spec_digests.iter().map(String::as_str).collect();
        let mut digests: BTreeSet<String> = spec_digests.iter().cloned().collect();
        digests.extend(self.inner.prepared.lock().unwrap().iter().cloned());

        let mut images = Vec::with_capacity(digests.len());
        for digest in digests {
            let in_spec = spec.contains(digest.as_str());
            let sha = self.sha(&digest).await;
            if sha.is_none() && !in_spec {
                // Prepared since start but the engine no longer has it.
                continue;
            }
            images.push(ImageStatus {
                image_digest: digest,
                inventory_sha256: sha,
            });
        }
        images
    }

    /// Takes the next queued image's inventory as its wire parts, or `None`
    /// when nothing is queued. An inventory that cannot be read or split is
    /// logged and skipped, so one bad image never holds up the rest. The
    /// session drains the queue with this.
    pub async fn next_parts(&self) -> Option<Vec<ImageInventoryBody>> {
        loop {
            let digest = self.inner.queue.lock().unwrap().pop_front()?;
            if let Some(parts) = self.parts_of(&digest).await {
                return Some(parts);
            }
        }
    }

    /// One image's inventory as wire parts; `None` when it cannot be sent.
    async fn parts_of(&self, digest: &ImageDigest) -> Option<Vec<ImageInventoryBody>> {
        let inventory = match self.inner.engine.image_inventory(digest).await {
            Ok(inventory) => inventory,
            Err(e) => {
                warn!(digest = %digest.0, error = %e, "could not read an image inventory; skipping it");
                return None;
            }
        };
        let Some(object) = to_object(inventory) else {
            warn!(digest = %digest.0, "an image inventory is not canonical; skipping it");
            return None;
        };
        let sha = sha256_hex(&object);
        let id = crate::util::message_id();
        match split(
            &object,
            &sha,
            &id,
            MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM),
        ) {
            Ok(parts) => {
                self.remember(&digest.0, Some(sha));
                Some(parts)
            }
            Err(e) => {
                warn!(digest = %digest.0, error = %e, "could not split an image inventory; skipping it");
                None
            }
        }
    }

    /// The inventory hash for one image, cached. `None` when the engine has no
    /// inventory (an image prepared before M5a, or not prepared yet).
    async fn sha(&self, digest: &str) -> Option<String> {
        if let Some(cached) = self.inner.cache.lock().unwrap().get(digest) {
            return cached.clone();
        }
        let sha = match self
            .inner
            .engine
            .image_inventory(&ImageDigest(digest.to_owned()))
            .await
        {
            Ok(inventory) => to_object(inventory).map(|object| sha256_hex(&object)),
            Err(EngineError::NotFound) => None,
            Err(e) => {
                warn!(digest, error = %e, "could not read an image inventory");
                None
            }
        };
        self.remember(digest, sha.clone());
        sha
    }

    fn remember(&self, digest: &str, sha: Option<String>) {
        let mut cache = self.inner.cache.lock().unwrap();
        if cache.len() >= CACHE_ENTRIES
            && !cache.contains_key(digest)
            && let Some(oldest) = cache.keys().next().cloned()
        {
            cache.remove(&oldest);
        }
        cache.insert(digest.to_owned(), sha);
    }
}

/// What one backfill pass did, by digest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackfillReport {
    /// Images inventoried from their root disk and queued for sending.
    pub inventoried: Vec<String>,
    /// Images the engine could not inventory; the next start tries again.
    pub failed: Vec<String>,
}

/// Starts the backfill pass in the background and returns at once, so the
/// node's start, its VMs and its preparations never wait for it.
pub fn spawn_backfill<E: Engine + 'static>(
    engine: Arc<E>,
    hub: InventoryHub,
) -> tokio::task::JoinHandle<BackfillReport> {
    tokio::spawn(async move { backfill(engine.as_ref(), &hub).await })
}

/// Inventories every cached image that has none, one at a time, and queues
/// each inventory like a freshly prepared image's (section 6.11). An image
/// that fails is logged and skipped; it keeps no inventory, so the next
/// start tries again.
pub async fn backfill<E: Engine>(engine: &E, hub: &InventoryHub) -> BackfillReport {
    let mut report = BackfillReport::default();
    let digests = match engine.uninventoried_images().await {
        Ok(digests) => digests,
        Err(e) => {
            warn!(error = %e, "could not list the cached images without an inventory");
            return report;
        }
    };
    if digests.is_empty() {
        return report;
    }
    info!(
        images = digests.len(),
        "inventorying cached images prepared before M5a from their root disks"
    );
    for digest in digests {
        match engine.inventory_cached_image(&digest).await {
            Ok(inventory) => {
                info!(
                    digest = %digest.0,
                    packages = inventory.packages.len(),
                    "inventoried a cached image from its root disk"
                );
                report.inventoried.push(digest.0.clone());
                hub.notify_prepared(digest);
            }
            Err(e) => {
                warn!(
                    digest = %digest.0,
                    error = %e,
                    "could not inventory a cached image; it stays without an inventory until the next start"
                );
                report.failed.push(digest.0);
            }
        }
    }
    info!(
        inventoried = report.inventoried.len(),
        failed = report.failed.len(),
        "finished inventorying cached images"
    );
    report
}

/// Converts the engine's inventory to the object whose canonical bytes are
/// hashed (section 6.11).
fn to_object(inventory: ImageInventory) -> Option<InventoryObject> {
    Some(InventoryObject {
        image_digest: inventory.digest.0,
        inventoried_at: inventory.inventoried_at,
        os: inventory.os.map(|(id, version_id)| InventoryOs {
            id,
            version_id: (!version_id.is_empty()).then_some(version_id),
        }),
        ecosystems: inventory.ecosystems,
        not_inventoried: inventory.not_inventoried,
        package_count: u64::try_from(inventory.packages.len()).ok()?,
        packages: inventory
            .packages
            .into_iter()
            .map(|package| InventoryPackageWire {
                ecosystem: package.ecosystem,
                name: package.name,
                source_name: package.source_name,
                version: package.version,
                purl: package.purl,
                path: package.path,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_engine::{FakeEngine, FakeOp, ImageRef, InventoryPackage};

    const OLD: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const OLDER: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    const NEW: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";

    fn digest(text: &str) -> ImageDigest {
        ImageDigest(text.to_owned())
    }

    fn inventory(text: &str) -> ImageInventory {
        ImageInventory {
            digest: digest(text),
            os: Some(("alpine".to_owned(), "3.22.1".to_owned())),
            ecosystems: vec!["Alpine:v3.22".to_owned()],
            not_inventoried: Vec::new(),
            packages: vec![InventoryPackage {
                ecosystem: "Alpine:v3.22".to_owned(),
                name: "libcrypto3".to_owned(),
                source_name: Some("openssl".to_owned()),
                version: "3.5.1-r0".to_owned(),
                purl: "pkg:apk/alpine/libcrypto3@3.5.1-r0".to_owned(),
                path: None,
            }],
            inventoried_at: "2026-11-05T06:09:41Z".to_owned(),
        }
    }

    /// A fake holding `OLD` and `OLDER` as images prepared before M5a and
    /// `NEW` as one prepared with its inventory.
    async fn engine() -> Arc<FakeEngine> {
        let engine = Arc::new(FakeEngine::new());
        for text in [OLD, OLDER, NEW] {
            let image = ImageRef {
                reference: "docker.io/library/alpine:3.22".to_owned(),
                digest: digest(text),
            };
            Engine::prepare_image(&*engine, &image).await.unwrap();
        }
        engine.clear_inventory(&digest(OLD));
        engine.clear_inventory(&digest(OLDER));
        engine.set_inventory(inventory(NEW));
        engine.set_backfill_inventory(inventory(OLD));
        engine.set_backfill_inventory(inventory(OLDER));
        engine
    }

    async fn drained(hub: &InventoryHub) -> Vec<String> {
        let mut sent = Vec::new();
        while let Some(parts) = hub.next_parts().await {
            sent.push(parts[0].image_digest.clone());
        }
        sent
    }

    #[tokio::test]
    async fn a_cached_image_without_an_inventory_is_inventoried_once_and_queued() {
        let engine = engine().await;
        let hub = InventoryHub::new(Arc::clone(&engine));

        let report = backfill(engine.as_ref(), &hub).await;
        assert_eq!(report.inventoried, vec![OLD.to_owned(), OLDER.to_owned()]);
        assert!(report.failed.is_empty());
        assert_eq!(engine.backfilled(), vec![digest(OLD), digest(OLDER)]);
        assert_eq!(drained(&hub).await, vec![OLD.to_owned(), OLDER.to_owned()]);

        // A second pass finds nothing left to inventory.
        let again = backfill(engine.as_ref(), &hub).await;
        assert_eq!(again, BackfillReport::default());
        assert_eq!(engine.backfilled().len(), 2);
        assert!(drained(&hub).await.is_empty());
    }

    #[tokio::test]
    async fn an_image_that_already_has_an_inventory_is_skipped() {
        let engine = engine().await;
        let hub = InventoryHub::new(Arc::clone(&engine));
        let report = backfill(engine.as_ref(), &hub).await;
        assert!(!report.inventoried.contains(&NEW.to_owned()));
        assert!(!engine.backfilled().contains(&digest(NEW)));
    }

    #[tokio::test]
    async fn a_failure_is_logged_and_the_pass_goes_on() {
        let engine = engine().await;
        engine.fail_next(
            FakeOp::InventoryCachedImage,
            EngineError::Io("mount: wrong fs type".to_owned()),
        );
        let hub = InventoryHub::new(Arc::clone(&engine));

        let report = backfill(engine.as_ref(), &hub).await;
        assert_eq!(report.failed, vec![OLD.to_owned()]);
        assert_eq!(report.inventoried, vec![OLDER.to_owned()]);
        assert_eq!(drained(&hub).await, vec![OLDER.to_owned()]);
        // The failed image keeps no inventory, so the next start retries it.
        assert_eq!(
            Engine::uninventoried_images(engine.as_ref()).await.unwrap(),
            vec![digest(OLD)]
        );
        let next_start = backfill(engine.as_ref(), &hub).await;
        assert_eq!(next_start.inventoried, vec![OLD.to_owned()]);
    }

    #[tokio::test]
    async fn the_pass_runs_in_the_background() {
        let engine = engine().await;
        let hub = InventoryHub::new(Arc::clone(&engine));
        // Spawning returns before the pass ran: the caller's next step (the
        // session, the reconciler) does not wait for it.
        let task = spawn_backfill(Arc::clone(&engine), hub.clone());
        let report = task.await.expect("backfill task");
        assert_eq!(report.inventoried.len(), 2);
    }

    #[tokio::test]
    async fn an_engine_without_cached_images_does_nothing() {
        let engine = Arc::new(FakeEngine::new());
        let hub = InventoryHub::new(Arc::clone(&engine));
        assert_eq!(
            backfill(engine.as_ref(), &hub).await,
            BackfillReport::default()
        );
    }
}
