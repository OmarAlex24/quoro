//! Quoro's control plane: the HTTP API, authentication and authorization,
//! the SQLite change log and state tables, events and the audit chain
//! (D-0012, D-0015, D-0031).
//!
//! In M1 the primary is the only node. The engine is not called from here:
//! the control plane changes desired state and notifies the local node
//! driver through [`AppState::driver`]; the driver reports observed state back
//! through [`driver`]. From M2 the node-agent session replaces the in-process
//! driver without changing the services.

pub mod api;
pub mod audit;
pub mod auth;
pub mod changelog;
pub mod crypto;
pub mod driver;
pub mod error;
pub mod events;
pub mod idempotency;
/// RFC 8785 canonical JSON, shared with the node agent through `quoro-proto`.
pub use quoro_proto::jcs;
pub mod openapi;
pub mod request;
pub mod store;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Static server settings.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// `/var/lib/quoro` in production; holds the database, the TLS material
    /// and the setup token.
    pub data_dir: PathBuf,
    /// Whether the session cookie carries `Secure`. True behind the TLS
    /// listener; tests over plain HTTP turn it off.
    pub secure_cookies: bool,
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
    /// Notified with a `vm_` id whenever a VM's desired state changes.
    pub driver: Option<mpsc::UnboundedSender<String>>,
}

impl AppState {
    #[must_use]
    pub fn new(
        store: store::Store,
        config: ServerConfig,
        driver: Option<mpsc::UnboundedSender<String>>,
        data_key: Arc<crypto::DataKey>,
    ) -> Self {
        Self {
            store,
            config: Arc::new(config),
            idempotency: idempotency::Cache::default(),
            data_key,
            driver,
        }
    }

    /// Tells the local node driver that `vm_id` needs reconciling.
    pub fn notify_driver(&self, vm_id: &str) {
        if let Some(driver) = &self.driver {
            let _ = driver.send(vm_id.to_string());
        }
    }
}

/// The API router with the request-id layer. The panel's static assets are
/// mounted by the binary beside it.
pub fn router(state: AppState) -> axum::Router {
    api::routes()
        .with_state(state)
        .layer(axum::middleware::from_fn(request::request_id))
}
