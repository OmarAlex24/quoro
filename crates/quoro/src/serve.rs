//! `quoro serve`: the HTTPS listener of D-0054 and the local node driver.
//!
//! One listener serves the API, the embedded panel (and, from later milestones, MCP)
//! on a self-signed certificate. The engine is chosen here, with a `match` over
//! [`EngineKind`], because the [`Engine`] trait is not object safe: adding the
//! Cloud Hypervisor backend is one more arm of that match and nothing else.

use crate::config::{Config, EngineKind};
use crate::local_node;
use crate::tls;
use axum_server::Handle;
use quoro_control_plane::store::Store;
use quoro_control_plane::{AppState, ServerConfig, router};
use quoro_engine::{Engine, FakeEngine};
use std::net::TcpListener;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

/// Runs the server until SIGTERM or Ctrl-C. Returns the process exit code.
///
/// # Errors
///
/// A message for the operator when the data directory, the database, the TLS
/// material or the listener cannot be brought up.
pub async fn run(config: Config) -> Result<i32, String> {
    crate::config::ensure_data_dir(&config.data_dir)
        .map_err(|e| format!("cannot create {}: {e}", config.data_dir.display()))?;

    let database = config.data_dir.join("quoro.db");
    let store = Store::open(&database)
        .await
        .map_err(|e| format!("cannot open {}: {e}", database.display()))?;
    let material = tls::load_or_generate(&config.data_dir, &config.node_name, config.listen)
        .map_err(|e| format!("cannot load the TLS material: {e}"))?;

    match config.engine {
        // The fake engine has no hypervisor and no privileges; it is what the
        // end-to-end test and first-run experiments use.
        EngineKind::Fake => serve_with(FakeEngine::new(), config, store, material).await,
        EngineKind::CloudHypervisor => {
            eprintln!(
                "the cloud-hypervisor engine is not built into this binary yet; set engine = \"fake\""
            );
            Ok(2)
        }
    }
}

/// Serves the control plane with `engine` behind the driver.
async fn serve_with<E: Engine + 'static>(
    engine: E,
    config: Config,
    store: Store,
    material: tls::Material,
) -> Result<i32, String> {
    let (tx, rx) = mpsc::unbounded_channel();
    let state = AppState::new(
        store.clone(),
        ServerConfig {
            data_dir: config.data_dir.clone(),
            // The listener is TLS, so the session cookie carries `Secure`.
            secure_cookies: true,
        },
        Some(tx),
    );

    let (node_id, driver) = local_node::start(engine, store.clone(), rx, config.node_name.clone())
        .await
        .map_err(|e| format!("cannot start the local node driver: {e:?}"))?;
    let owner_exists = owner_exists(&store)
        .await
        .map_err(|e| format!("cannot read the user table: {e}"))?;

    // Binding first gives the real port when `listen` names port 0, which the
    // test relies on and which also makes the log line truthful.
    let listener = TcpListener::bind(config.listen)
        .map_err(|e| format!("cannot bind {}: {e}", config.listen))?;
    let address = listener
        .local_addr()
        .map_err(|e| format!("cannot read the bound address: {e}"))?;
    let url = format!("https://{address}");

    let handle = Handle::new();
    tokio::spawn(shutdown_on_signal(handle.clone()));

    // D-0054's operator-facing start lines, then the first-run instruction.
    info!("listening on {url}");
    info!("tls fingerprint: {}", material.fingerprint);
    info!("engine: {}", config.engine.as_str());
    info!("node: {node_id}");
    if !owner_exists {
        info!("no owner yet: run 'quoro setup-token' and open {url}/");
    }

    axum_server::from_tcp_rustls(
        listener,
        axum_server::tls_rustls::RustlsConfig::from_config(material.config),
    )
    .handle(handle)
    .serve(
        router(state)
            .fallback(crate::panel::serve)
            .into_make_service(),
    )
    .await
    .map_err(|e| format!("the listener stopped: {e}"))?;

    driver.abort();
    Ok(0)
}

/// Whether the cluster already has a user, which is what makes `setup-token`
/// refuse (D-0054).
///
/// # Errors
///
/// A database error.
pub async fn owner_exists(store: &Store) -> Result<bool, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM users LIMIT 1")
        .fetch_optional(store.pool())
        .await?;
    Ok(row.is_some())
}

/// Shuts the listener down on SIGTERM (systemd) or Ctrl-C.
async fn shutdown_on_signal(handle: Handle) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "cannot listen for SIGTERM"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("received Ctrl-C; shutting down"),
        () = terminate => info!("received SIGTERM; shutting down"),
    }
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}
