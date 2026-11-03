// SPDX-License-Identifier: AGPL-3.0-only
//! Quoro's control plane: the HTTP API, authentication and authorization,
//! the SQLite change log and state tables, events and the audit chain
//! (D-0012, D-0015, D-0031).
//!
//! In M1 the primary is the only node. From M2 nodes dial out and run the
//! node-agent session ([`session`]); the control plane changes desired state
//! and tells the hub, which signs a full snapshot and pushes it to the node's
//! live session. Observed state arrives back through [`driver`].

pub mod api;
pub mod audit;
pub mod auth;
pub mod changelog;
pub mod crypto;
pub mod driver;
pub mod error;
pub mod events;
pub mod idempotency;
pub mod keys;
/// RFC 8785 canonical JSON, shared with the node agent through `quoro-proto`.
pub use quoro_proto::jcs;
pub mod openapi;
pub mod request;
pub mod session;
pub mod store;

use crate::keys::ClusterKeys;
use crate::session::NodeHub;
use axum::http::HeaderMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

/// The node bridge every VM is attached to (`spec.network`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeNetwork {
    /// Subnet as a CIDR string, for example `10.42.1.0/24`.
    pub subnet: String,
    /// The node's address on that bridge, for example `10.42.1.1`.
    pub gateway: String,
}

/// Static server settings.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// `/var/lib/quoro` in production; holds the database, the TLS material,
    /// the setup token and the cluster signing key.
    pub data_dir: PathBuf,
    /// Whether the session cookie carries `Secure`. True behind the TLS
    /// listener; tests over plain HTTP turn it off.
    pub secure_cookies: bool,
    /// Base URL nodes dial, for example `https://primary.example:8443`. When
    /// `None` the URL is derived from the request's `Host` header.
    pub public_url: Option<String>,
    /// SHA-256 of the listener's certificate DER, exactly as the binary
    /// prints it: `sha256:<64 lowercase hex>`.
    pub tls_fingerprint: String,
    /// The node bridge handed to every node in its desired state.
    pub node_network: NodeNetwork,
    /// Heartbeat interval announced in `welcome` (section 4.3).
    pub heartbeat_interval_ms: u64,
    /// Session timeout without any frame (section 4.3).
    pub session_timeout_ms: u64,
}

impl ServerConfig {
    /// The URL nodes dial: [`ServerConfig::public_url`], or `https://<Host>`.
    #[must_use]
    pub fn primary_url(&self, headers: &HeaderMap) -> String {
        if let Some(url) = &self.public_url {
            return url.clone();
        }
        let host = headers
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .filter(|host| !host.is_empty())
            .unwrap_or("localhost");
        format!("https://{host}")
    }
}

/// Turns an image reference into the content digest the engine caches by.
///
/// The control plane stores whatever reference the user typed; the node needs
/// the digest so a tag that moves never silently changes a running VM.
/// [`ReferenceAsDigest`] treats the reference itself as the digest (tests and
/// the fake engine); the binary plugs a registry-backed resolver.
pub trait DigestResolver: Send + Sync {
    /// Resolves `reference` to a digest.
    fn resolve<'a>(
        &'a self,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;
}

/// Returns the reference unchanged, as its own digest.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReferenceAsDigest;

impl DigestResolver for ReferenceAsDigest {
    fn resolve<'a>(
        &'a self,
        reference: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async move { Ok(reference.to_string()) })
    }
}

/// Shared state of every handler.
#[derive(Clone)]
pub struct AppState {
    pub store: store::Store,
    pub config: Arc<ServerConfig>,
    pub idempotency: idempotency::Cache,
    /// The cluster data-encryption keyring (D-0050); every `*_enc` column is
    /// encrypted and decrypted through it.
    pub data_key: Arc<crypto::DataKey>,
    /// The live node-agent sessions (`node-agent-protocol.md` section 3).
    pub hub: NodeHub,
    /// The cluster signing key, loaded at start (ADR 0050).
    pub keys: Arc<ClusterKeys>,
    /// `cls_` id of this cluster, from `meta`.
    pub cluster_id: String,
}

impl AppState {
    /// Opens the state: loads or generates the cluster key, completes `meta`
    /// and builds the session hub.
    ///
    /// # Errors
    ///
    /// A message for the operator when the signing key cannot be loaded or
    /// `meta` cannot be completed.
    pub async fn open(
        store: store::Store,
        mut config: ServerConfig,
        resolver: Arc<dyn DigestResolver>,
        data_key: Arc<crypto::DataKey>,
    ) -> Result<Self, String> {
        if config.heartbeat_interval_ms == 0 {
            config.heartbeat_interval_ms = quoro_proto::wire::HEARTBEAT_INTERVAL_MS;
        }
        if config.session_timeout_ms == 0 {
            config.session_timeout_ms = quoro_proto::wire::SESSION_TIMEOUT_MS;
        }
        let keys = Arc::new(
            ClusterKeys::load_or_generate(&config.data_dir)
                .map_err(|e| format!("cannot load the cluster signing key: {e}"))?,
        );
        let cluster_id = keys::ensure_cluster_meta(&store)
            .await
            .map_err(|e| format!("cannot read the cluster identity: {e:?}"))?;
        let config = Arc::new(config);
        let hub = NodeHub::new(
            store.clone(),
            Arc::clone(&config),
            Arc::clone(&keys),
            cluster_id.clone(),
            resolver,
        );
        hub.reset_stale_connections().await?;
        Ok(Self {
            store,
            config,
            idempotency: idempotency::Cache::default(),
            data_key,
            hub,
            keys,
            cluster_id,
        })
    }

    /// Tells the session hub that `vm_id` needs reconciling. The rebuild runs
    /// in the background and never blocks the request.
    pub fn notify_driver(&self, vm_id: &str) {
        self.hub.vm_changed(vm_id);
    }
}

/// The API router with the request-id layer. The panel's static assets are
/// mounted by the binary beside it.
pub fn router(state: AppState) -> axum::Router {
    api::routes()
        .with_state(state)
        .layer(axum::middleware::from_fn(request::request_id))
}
