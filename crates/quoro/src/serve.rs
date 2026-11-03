//! `quoro serve`: the HTTPS listener of D-0054 and, on a single host, the
//! primary's own node agent.
//!
//! One listener serves the API, the embedded panel and MCP (`/mcp`) on a
//! self-signed certificate. Nodes dial the same listener for their
//! session (`node-agent-protocol.md`), so the control plane never touches a
//! hypervisor itself: it signs desired state and the node agents run it.
//!
//! With `[node] local = true` the process also runs a node agent that enrolls
//! with this listener over loopback through the same join token and session
//! as any other node (one code path for every node). The privilege split
//! (ADR 0089) turns that off and runs the agent as its own service.

use crate::config::{Config, EngineKind};
use crate::node::{self, EX_CONFIG};
use crate::tls;
use axum_server::Handle;
use quoro_control_plane::api::enroll::ensure_local_join;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::store::Store;
use quoro_control_plane::{
    AppState, DigestResolver, NodeNetwork, ReferenceAsDigest, ServerConfig, router,
};
use quoro_proto::wire::{HEARTBEAT_INTERVAL_MS, SESSION_TIMEOUT_MS};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

/// How long the local node agent gets to stop after the listener did.
const NODE_STOP_GRACE: Duration = Duration::from_secs(15);

/// Runs the server until SIGTERM or Ctrl-C. Returns the process exit code.
///
/// # Errors
///
/// A message for the operator when the data directory, the database, the TLS
/// material or the listener cannot be brought up.
pub async fn run(config: Config) -> Result<i32, String> {
    // The control plane's webhook clients use reqwest's no-provider rustls
    // feature, so the process must install one before `AppState::open` starts
    // the dispatcher; `ring` is the provider ADR 0080 allows. The listener
    // and the node agent pass ring explicitly and do not depend on this.
    let _ = rustls::crypto::ring::default_provider().install_default();

    crate::config::ensure_data_dir(&config.data_dir)
        .map_err(|e| format!("cannot create {}: {e}", config.data_dir.display()))?;

    let Some(resolver) = resolver_for(config.engine) else {
        eprintln!("the cloud-hypervisor engine runs on Linux only");
        return Ok(EX_CONFIG);
    };

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

    let subnet = config.cloud_hypervisor.subnet;
    let state = AppState::open(
        store.clone(),
        ServerConfig {
            data_dir: config.data_dir.clone(),
            // The listener is TLS, so the session cookie carries `Secure`.
            secure_cookies: true,
            public_url: config.public_url.clone(),
            tls_fingerprint: material.fingerprint.clone(),
            // Every node gets the subnet of this configuration in M2; a
            // per-node subnet arrives with the overlay network (ADR 0063).
            node_network: NodeNetwork {
                subnet: subnet.to_string(),
                gateway: subnet.gateway().to_string(),
            },
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
            session_timeout_ms: SESSION_TIMEOUT_MS,
        },
        resolver,
        Arc::new(data_key),
    )
    .await?;
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

    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = Handle::new();
    tokio::spawn(shutdown_on_signal(handle.clone(), stop_tx));

    // D-0054's operator-facing start lines, then the first-run instruction.
    info!("listening on {url}");
    info!("tls fingerprint: {}", material.fingerprint);
    info!("cluster: {}", state.cluster_id);
    if !owner_exists {
        info!("no owner yet: run 'quoro setup-token' and open {url}/");
    }

    // The primary's own node agent reads this join file and dials the listener
    // over loopback like any other node. It is written until the primary holds
    // a node credential, whether the agent runs in this process
    // (`[node] local`) or as its own service.
    if !primary_enrolled(&store)
        .await
        .map_err(|e| format!("cannot read the node credentials: {e}"))?
    {
        let loopback = format!("https://{}", loopback(address));
        ensure_local_join(&config.data_dir, &loopback, &material.fingerprint)
            .map_err(|e| format!("cannot write the local node's join file: {e}"))?;
    }
    let local_node = if config.node.local {
        info!(
            engine = config.engine.as_str(),
            "starting the local node agent"
        );
        let node_config = config.clone();
        let mut node_stop = stop_rx;
        Some(tokio::spawn(async move {
            let stopped = async move {
                let _ = node_stop.wait_for(|stop| *stop).await;
            };
            node::run(&node_config, stopped).await
        }))
    } else {
        None
    };

    let server = axum_server::from_tcp_rustls(
        listener,
        axum_server::tls_rustls::RustlsConfig::from_config(material.config),
    )
    .handle(handle.clone())
    .serve(
        router(state.clone())
            .merge(quoro_mcp::router(state))
            .fallback(crate::panel::serve)
            .into_make_service(),
    );

    let Some(mut local_node) = local_node else {
        server
            .await
            .map_err(|e| format!("the listener stopped: {e}"))?;
        return Ok(0);
    };

    // A local node that cannot run (no KVM, a broken artifact) stops the
    // whole process with its exit code, so systemd sees the refusal instead
    // of a primary that silently places VMs nowhere.
    tokio::pin!(server);
    tokio::select! {
        served = &mut server => {
            served.map_err(|e| format!("the listener stopped: {e}"))?;
            match tokio::time::timeout(NODE_STOP_GRACE, &mut local_node).await {
                Ok(Ok(Ok(code))) => Ok(code),
                Ok(Ok(Err(e))) => Err(e),
                Ok(Err(e)) => Err(format!("the local node agent panicked: {e}")),
                Err(_) => {
                    local_node.abort();
                    error!("the local node agent did not stop in time");
                    Ok(0)
                }
            }
        }
        finished = &mut local_node => {
            handle.graceful_shutdown(Some(Duration::from_secs(10)));
            let _ = server.await;
            match finished {
                Ok(Ok(code)) => Ok(code),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(format!("the local node agent panicked: {e}")),
            }
        }
    }
}

/// The image resolver of `engine`, or `None` when the engine cannot run on
/// this platform.
///
/// The fake engine caches by whatever string it is given, so the reference
/// stands in for the digest and the end-to-end test stays offline.
fn resolver_for(engine: EngineKind) -> Option<Arc<dyn DigestResolver>> {
    match engine {
        EngineKind::Fake => Some(Arc::new(ReferenceAsDigest)),
        #[cfg(target_os = "linux")]
        EngineKind::CloudHypervisor => Some(Arc::new(RegistryResolver)),
        #[cfg(not(target_os = "linux"))]
        EngineKind::CloudHypervisor => None,
    }
}

/// Resolves a reference against its OCI registry: a digest that is already
/// pinned is used as is, a tag is turned into the manifest digest it points
/// at. The primary resolves once, so every node runs the same bytes.
#[cfg(target_os = "linux")]
struct RegistryResolver;

#[cfg(target_os = "linux")]
impl DigestResolver for RegistryResolver {
    fn resolve<'a>(
        &'a self,
        reference: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
    {
        Box::pin(async move {
            quoro_engine::image::resolve_digest(reference)
                .await
                .map(|digest| digest.0)
                .map_err(|e| e.to_string())
        })
    }
}

/// The address the local node agent dials: the bound address, with an
/// unspecified host (`0.0.0.0`, `::`) replaced by loopback.
fn loopback(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, address.port())
}

/// Whether the primary's own node agent already enrolled: a primary node row
/// with a credential that is not revoked.
async fn primary_enrolled(store: &Store) -> Result<bool, sqlx::Error> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT c.node_id FROM node_credentials c JOIN nodes n ON n.id = c.node_id \
         WHERE n.cp_status = 'primary' AND c.revoked_at IS NULL LIMIT 1",
    )
    .fetch_optional(store.pool())
    .await?;
    Ok(row.is_some())
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

/// Resolves on SIGTERM (systemd) or Ctrl-C.
pub async fn signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => error!(error = %e, "cannot listen for SIGTERM"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("received Ctrl-C; shutting down"),
        () = terminate => info!("received SIGTERM; shutting down"),
    }
}

/// Shuts the listener and the local node agent down on a signal.
async fn shutdown_on_signal(handle: Handle, stop: watch::Sender<bool>) {
    signal().await;
    let _ = stop.send(true);
    handle.graceful_shutdown(Some(Duration::from_secs(10)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unspecified_listen_address_is_dialled_on_loopback() {
        let any: SocketAddr = "0.0.0.0:8443".parse().unwrap();
        assert_eq!(loopback(any), "127.0.0.1:8443".parse().unwrap());
        let any6: SocketAddr = "[::]:8443".parse().unwrap();
        assert_eq!(loopback(any6), "[::1]:8443".parse().unwrap());
        let fixed: SocketAddr = "10.0.0.5:9000".parse().unwrap();
        assert_eq!(loopback(fixed), fixed);
    }
}
