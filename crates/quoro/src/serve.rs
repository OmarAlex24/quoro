//! `quoro serve`: the HTTPS listener of D-0054 and the local node driver.
//!
//! One listener serves the API, the embedded panel (and, from later milestones,
//! MCP) on a self-signed certificate. The engine is chosen here, with a `match`
//! over [`EngineKind`], because the [`Engine`] trait is not object safe: each
//! backend is one arm of that match and nothing else.
//!
//! The Cloud Hypervisor arm is the Linux node (ADR 0007, ADR 0009): it builds
//! the backend from the `[cloud_hypervisor]` table, refuses to serve on a host
//! that cannot run guests, and then hands the same driver the real engine and
//! the registry-backed image resolver. Every refusal exits with
//! [`EX_CONFIG`] so systemd stops restarting a node that will never boot.

use crate::config::{Config, EngineKind};
use crate::local_node::{self, ImageResolver, NodeNetworkConfig, ReferenceAsDigest};
use crate::tls;
use axum_server::Handle;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::store::Store;
use quoro_control_plane::{AppState, ServerConfig, router};
use quoro_engine::{Engine, FakeEngine};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

/// The exit status of a configuration the process cannot recover from
/// (`EX_CONFIG`, sysexits.h).
///
/// The systemd unit turns this into `RestartPreventExitStatus=78`, so a node
/// without KVM or with missing artifacts stops instead of looping.
pub const EX_CONFIG: i32 = 78;

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
    // D-0050: the cluster data-encryption key is generated on first start,
    // before the listener opens, so a node never serves before it can encrypt.
    let data_key = DataKey::load_or_create(&config.data_dir)
        .map_err(|e| format!("cannot load the data-encryption key: {e}"))?;
    info!("data-encryption key ready: {}", data_key.key_id_hex());
    let data_key = Arc::new(data_key);

    // Both engines attach VMs to the same node network; only the config table
    // differs, so the driver takes the same shape either way.
    let network = NodeNetworkConfig::from(&config.cloud_hypervisor);

    match config.engine {
        // The fake engine has no hypervisor and no privileges; it is what the
        // end-to-end test and first-run experiments use. The reference stands
        // in for the digest, which is what that engine caches by.
        EngineKind::Fake => {
            serve_with(
                FakeEngine::new(),
                ReferenceAsDigest,
                network,
                config,
                store,
                material,
                data_key,
            )
            .await
        }
        EngineKind::CloudHypervisor => {
            serve_cloud_hypervisor(config, network, store, material, data_key).await
        }
    }
}

/// Builds the Cloud Hypervisor backend, checks the host and serves on it.
///
/// The host check is the engine's own `probe_host`, not a second copy of it:
/// the backend already knows where KVM, the CPU flags and the kernel release
/// live, and a duplicated check would drift from it.
#[cfg(target_os = "linux")]
async fn serve_cloud_hypervisor(
    config: Config,
    network: NodeNetworkConfig,
    store: Store,
    material: tls::Material,
    data_key: Arc<DataKey>,
) -> Result<i32, String> {
    use quoro_engine::ch::{ChConfig, ChEngine};
    use tracing::warn;

    let ch = &config.cloud_hypervisor;
    let dns = ch.dns();
    let mut ch_config = ChConfig::new(
        ch.state_dir(&config.data_dir),
        ch.artifacts_dir.clone(),
        ch.init_binary.clone(),
    );
    ch_config.dns.clone_from(&dns);

    // A pin mismatch, a missing `nft` or a broken artifact lands here; the
    // engine's message already names the cause, so it is passed through.
    let engine = match ChEngine::new(ch_config).await {
        Ok(engine) => engine,
        Err(e) => {
            eprintln!("cannot start the cloud-hypervisor engine: {e}");
            return Ok(EX_CONFIG);
        }
    };

    let capabilities = engine.capabilities().await;
    info!(version = %capabilities.version, "cloud-hypervisor engine ready");

    let probe = match engine.probe_host().await {
        Ok(probe) => probe,
        Err(e) => {
            eprintln!("cannot probe this host for the cloud-hypervisor engine: {e}");
            return Ok(EX_CONFIG);
        }
    };
    if !probe.kvm_available {
        eprintln!(
            "/dev/kvm is missing or not accessible: the cloud-hypervisor engine needs KVM (ADR 0009)"
        );
        return Ok(EX_CONFIG);
    }
    if !(probe.cpu_vmx || probe.cpu_svm) {
        eprintln!("the CPU exposes neither vmx nor svm: enable hardware or nested virtualization");
        return Ok(EX_CONFIG);
    }
    if !probe.kernel_supported {
        eprintln!(
            "kernel {} is older than the engine's supported minimum (6.8)",
            probe.kernel_release
        );
        return Ok(EX_CONFIG);
    }

    if dns.is_empty() {
        // The engine still starts; guests just cannot resolve names.
        warn!("no DNS server found: guests get an empty resolv.conf and cannot resolve names");
    } else {
        let servers = dns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        info!(servers = %servers, "guest DNS servers");
    }

    serve_with(
        engine,
        local_node::RegistryResolver,
        network,
        config,
        store,
        material,
        data_key,
    )
    .await
}

/// The Cloud Hypervisor backend is Linux-only; on any other platform the
/// configuration is accepted but cannot be served.
#[cfg(not(target_os = "linux"))]
async fn serve_cloud_hypervisor(
    _config: Config,
    _network: NodeNetworkConfig,
    _store: Store,
    _material: tls::Material,
    _data_key: Arc<DataKey>,
) -> Result<i32, String> {
    eprintln!("the cloud-hypervisor engine runs on Linux only");
    Ok(EX_CONFIG)
}

/// Serves the control plane with `engine` behind the driver.
async fn serve_with<E: Engine + 'static, R: ImageResolver>(
    engine: E,
    resolver: R,
    network: NodeNetworkConfig,
    config: Config,
    store: Store,
    material: tls::Material,
    data_key: Arc<DataKey>,
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
        data_key,
    );

    let (node_id, driver) = local_node::start(
        engine,
        resolver,
        network,
        store.clone(),
        rx,
        config.node_name.clone(),
    )
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
