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
use tracing::warn;

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
