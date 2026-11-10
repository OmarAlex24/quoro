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
pub mod app_env;
pub mod audit;
pub mod audit_export;
pub mod auth;
pub mod authority;
pub mod authz;
pub mod backups;
pub mod build;
pub mod changelog;
pub mod crypto;
pub mod deploy;
pub mod dispatcher;
pub mod driver;
/// Reading the edge's backend health (ADR 0143).
pub(crate) mod edge_health;
pub mod error;
pub mod events;
/// Moving a `warm` app's lease to its warm node (`failoverApp`, ADR 0175).
pub(crate) mod failover;
pub(crate) mod fences;
pub mod idempotency;
pub mod interactive;
pub mod keys;
/// RFC 8785 canonical JSON, shared with the node agent through `quoro-proto`.
pub use quoro_proto::jcs;
pub mod leases;
pub mod openapi;
pub mod primary_status;
/// Quarantined copies of a volume: recording, refusals and deletion (ADR
/// 0176).
pub(crate) mod quarantines;
pub mod request;
pub(crate) mod scheduler;
pub mod service;
pub mod session;
pub mod store;
pub(crate) mod subnet;
pub mod support;
pub mod sweeper;
pub mod tunnel;
pub mod voters;
pub mod vulns;
pub(crate) mod warm;

/// Whether this control plane was compiled with the dev-only
/// `dev-warm-policy` feature (ADR 0223, ADR 0277). Until the last warm PR
/// merges, only such a test-lab build accepts `ha_policy` `warm`, and it runs
/// everything of the warm policy merged so far: the warm node's choice, its
/// settings and its cost. Every other build refuses `warm` as before. It is
/// read from this crate's own `cfg`, so it is true however the feature was
/// turned on, including by cargo's feature unification; `quoro version`
/// reports it and CI fails a default or release binary that does. The last
/// warm PR removes it with the feature.
pub const DEV_WARM_POLICY: bool = cfg!(feature = "dev-warm-policy");

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
    /// The configured guest network. The oldest node with the `node` role
    /// keeps it; every other node gets its own `/24` (ADR 0144).
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
    /// Per-user sliding window of `POST /api/v1/auth/password` attempts
    /// (permission-model.md section 12).
    pub password_rate_limit: auth::rate_limit::RateLimiter,
    /// The cluster data-encryption keyring (D-0050); every `*_enc` column is
    /// encrypted and decrypted through it.
    pub data_key: Arc<crypto::DataKey>,
    /// The live node-agent sessions (`node-agent-protocol.md` section 3).
    pub hub: NodeHub,
    /// The Cloudflare Tunnel manager (ADR 0094, 0095).
    pub tunnel: tunnel::TunnelManager,
    /// The cluster signing key, loaded at start (ADR 0050).
    pub keys: Arc<ClusterKeys>,
    /// `cls_` id of this cluster, from `meta`.
    pub cluster_id: String,
    /// The in-memory tickets and attach points of interactive sessions
    /// (ADR 0114).
    pub interactive: interactive::Interactive,
    /// The backup runtime: the bucket factory (production `S3Client`, tests a
    /// `MemoryStore`), the garbage-collection flag and the last sweep time.
    pub backups: backups::Backups,
    /// The primary's vulnerability data: the cache file next to the database
    /// and the loop that keeps it fresh (ADR 0150). The loop only runs once
    /// the binary calls [`vulns::Vulns::spawn`].
    pub vulns: vulns::Vulns,
    /// The apps whose failover is running, in memory only (`failoverApp`
    /// `in_progress`).
    pub(crate) failovers: failover::Running,
}

impl AppState {
    /// Opens the state: loads or generates the cluster key, takes the
    /// pre-upgrade backup when a migration is pending, completes `meta`, moves
    /// the primary status as `quoro restore` asked, and builds the session hub.
    ///
    /// # Errors
    ///
    /// A message for the operator when the signing key cannot be loaded, the
    /// pre-upgrade backup fails, the migrations cannot be applied or `meta`
    /// cannot be completed.
    pub async fn open(
        store: store::Store,
        config: ServerConfig,
        resolver: Arc<dyn DigestResolver>,
        data_key: Arc<crypto::DataKey>,
    ) -> Result<Self, String> {
        Self::open_with_upgrade(
            store,
            config,
            resolver,
            data_key,
            backups::Backups::s3(),
            false,
        )
        .await
    }

    /// [`AppState::open`], with the backup runtime the caller already built and
    /// the operator's `--skip-upgrade-backup`.
    ///
    /// `store` may be unmigrated: the pending range is read, the pre-upgrade
    /// backup runs (or the skip is honoured), the migrations are applied and
    /// the backup is recorded, all before the rest of the start
    /// (`backup-format.md` section 7.2, "Before an upgrade").
    ///
    /// # Errors
    ///
    /// As [`AppState::open`].
    pub async fn open_with_upgrade(
        store: store::Store,
        mut config: ServerConfig,
        resolver: Arc<dyn DigestResolver>,
        data_key: Arc<crypto::DataKey>,
        backups: backups::Backups,
        skip_upgrade_backup: bool,
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
        // The backup needs the keys and the data key, and must run before any
        // migration touches the database.
        backups::control_plane::upgrade_before_start(
            &store,
            &config.data_dir,
            &keys,
            &data_key,
            &backups,
            skip_upgrade_backup,
        )
        .await
        .map_err(|reason| {
            format!(
                "the pre-upgrade backup failed: {reason}; start once with \
                 --skip-upgrade-backup to migrate without it"
            )
        })?;
        let cluster_id = keys::ensure_cluster_meta(&store)
            .await
            .map_err(|e| format!("cannot read the cluster identity: {e:?}"))?;
        // After `quoro restore` onto another host, the node on this host takes
        // the primary status before any session or request sees it
        // (`backup-format.md` section 8.2).
        primary_status::apply_marker(&store, &config.data_dir).await?;
        // Nodes enrolled before the per-node subnet existed get theirs before
        // any session can ask for a desired state (ADR 0144).
        subnet::backfill(&store, &config.node_network).await?;
        let config = Arc::new(config);
        let hub = NodeHub::new(
            store.clone(),
            Arc::clone(&config),
            Arc::clone(&keys),
            cluster_id.clone(),
            resolver,
            Arc::clone(&data_key),
            backups.clone(),
        );
        hub.reset_stale_connections().await?;
        hub.seed_lease_gates().await?;
        // Section 8.4, "Grants of a marked row": every step-4 row's grant is
        // pending from the start until its holder's next session start (ADR
        // 0295), and the held grants go out once replicated and supported.
        hub.rebuild_held_grants().await?;
        hub.spawn_voter_watch();
        hub.spawn_held_grants();
        let vulns = vulns::Vulns::open(&config.data_dir, store.clone()).await?;
        hub.set_vulns(&vulns);
        let tunnel = tunnel::TunnelManager::new(store.clone(), Arc::clone(&data_key), hub.clone());
        hub.set_tunnel_manager(&tunnel);
        tunnel.start();
        let state = Self {
            store,
            config,
            idempotency: idempotency::Cache::default(),
            password_rate_limit: auth::rate_limit::RateLimiter::default(),
            data_key,
            hub,
            tunnel,
            keys,
            cluster_id,
            interactive: interactive::Interactive::default(),
            backups,
            vulns,
            failovers: failover::Running::default(),
        };
        // Every session the previous process left open ends with
        // `primary_restarted` before the listener opens.
        interactive::close_orphans(&state).await?;
        sweeper::spawn(state.clone());
        vulns::findings::spawn(state.clone());
        audit_export::spawn(state.clone());
        backups::dispatch::spawn(state.clone());
        session::serving::spawn(state.clone());
        dispatcher::start(state.clone()).await?;
        Ok(state)
    }

    /// Points the Cloudflare clients of this process at other endpoints. The
    /// tests use it to reach a local mock; production keeps the public API.
    #[must_use]
    pub fn with_cloudflare_endpoints(
        self,
        api_base: impl Into<String>,
        doh_base: impl Into<String>,
    ) -> Self {
        self.hub
            .set_cloudflare_endpoints(tunnel::CloudflareEndpoints {
                api_base: api_base.into(),
                doh_base: doh_base.into(),
            });
        self
    }

    /// Installs the source of the `welcome.primary_urls` list a node receives
    /// (section 5.10). The binary installs the host's advertised addresses;
    /// without a provider the list is empty and the field is left off.
    #[must_use]
    pub fn with_advertised_urls(
        self,
        provider: impl Fn() -> Vec<String> + Send + Sync + 'static,
    ) -> Self {
        self.hub.set_advertised_urls(std::sync::Arc::new(provider));
        self
    }

    /// Tells the session hub that `vm_id` needs reconciling. The rebuild runs
    /// in the background and never blocks the request.
    pub fn notify_driver(&self, vm_id: &str) {
        self.hub.vm_changed(vm_id);
    }
}

/// The API router with the request-id layer and the request body limit
/// (README "Request body limit", ADR 0306). The panel's static assets are
/// mounted by the binary beside it.
///
/// The limit runs after routing, on every route of [`api::routes`], so it
/// knows the operation it refuses; the `DefaultBodyLimit` keeps the
/// extractors behind it to the same limit.
pub fn router(state: AppState) -> axum::Router {
    api::routes()
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request::limit_body,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(
            request::MAX_BODY_BYTES,
        ))
        .with_state(state)
        .layer(axum::middleware::from_fn(request::request_id))
}
