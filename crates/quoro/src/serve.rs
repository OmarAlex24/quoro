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
//!
//! A primary also opens its local control socket, `/run/quoro/control.sock`,
//! for `quoro cluster maintenance` (`node-agent-protocol.md` section 8.11,
//! [`crate::control`]). A demoted primary opens none.

use crate::config::{Config, EngineKind};
use crate::node::{self, EX_CONFIG};
use crate::tls;
use axum_server::Handle;
use quoro_control_plane::api::enroll::ensure_local_join;
use quoro_control_plane::backups::Backups;
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
use tracing::{error, info, warn};

/// How long the local node agent gets to stop after the listener did.
const NODE_STOP_GRACE: Duration = Duration::from_secs(15);

/// Runs the server until SIGTERM or Ctrl-C. Returns the process exit code.
///
/// `skip_upgrade_backup` is the operator's `--skip-upgrade-backup`: with it, a
/// pending migration runs without the pre-upgrade backup
/// (`backup-format.md` section 7.2, "Before an upgrade").
///
/// # Errors
///
/// A message for the operator when the data directory, the database, the TLS
/// material or the listener cannot be brought up.
/// Set by the integration tests, which run without network: the vulnerability
/// data loop is not started.
const NO_VULN_DOWNLOAD: &str = "QUORO_TEST_NO_VULN_DOWNLOAD";

pub async fn run(config: Config, skip_upgrade_backup: bool) -> Result<i32, String> {
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
    // Unmigrated: the upgrade step inside `AppState::open_with_upgrade` backs
    // the database up before it applies any migration.
    let store = Store::open_unmigrated(&database)
        .await
        .map_err(|e| format!("cannot open {}: {e}", database.display()))?;
    // Section 7.4: a primary that learned of a higher term stays demoted
    // across a restart. Read only, before any start-time write.
    let demoted_by_term = store
        .demoted_by_term()
        .await
        .map_err(|e| format!("cannot read {}: {e}", database.display()))?;
    if let Some(term) = demoted_by_term {
        store.pool().close().await;
        return run_demoted(config, &term).await;
    }
    let material = tls::load_or_generate(&config.data_dir, &config.node_name, config.listen)
        .map_err(|e| format!("cannot load the TLS material: {e}"))?;
    // D-0050: the cluster data-encryption key is generated on first start,
    // before the listener opens, so a node never serves before it can encrypt.
    let data_key = DataKey::load_or_create(&config.data_dir)
        .map_err(|e| format!("cannot load the data-encryption key: {e}"))?;
    info!("data-encryption key ready: {}", data_key.key_id_hex());

    let subnet = config.cloud_hypervisor.subnet;
    let state = AppState::open_with_upgrade(
        store.clone(),
        ServerConfig {
            data_dir: config.data_dir.clone(),
            // The listener is TLS, so the session cookie carries `Secure`.
            secure_cookies: true,
            public_url: config.public_url.clone(),
            tls_fingerprint: material.fingerprint.clone(),
            tls: material.identity.clone(),
            // The configured guest network. The oldest node with the `node`
            // role keeps it; every other node gets its own /24 of
            // 10.42.0.0/16 (ADR 0144).
            node_network: NodeNetwork {
                subnet: subnet.to_string(),
                gateway: subnet.gateway().to_string(),
            },
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
            session_timeout_ms: SESSION_TIMEOUT_MS,
        },
        resolver,
        Arc::new(data_key),
        Backups::s3(),
        skip_upgrade_backup,
    )
    .await?;
    // The vulnerability data loop downloads from OSV and CISA. The binary's
    // own integration test runs without network and turns it off.
    if std::env::var_os(NO_VULN_DOWNLOAD).is_none() {
        state.vulns.spawn();
    }
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

    // Section 5.10: the other addresses a node may dial when this one fails.
    // Built after binding, so a `listen` port of 0 advertises the real port.
    let advertised = crate::advertise::AdvertisedUrls::discover(&config, address).await;
    let state = {
        let advertised = advertised.clone();
        state.with_advertised_urls(move || advertised.urls())
    };

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
    // Section 8.11: `quoro cluster maintenance` reaches this primary over its
    // local control socket. Without it the primary serves on, and the command
    // fails, which the installer reports and goes past. After a window, the
    // stored lease durations go back to `D` once the longer gates passed.
    let control = match crate::control::listen(&crate::control::socket_path(), state.hub.clone()) {
        Ok(control) => Some(control),
        Err(e) => {
            warn!(
                "no control socket, so 'quoro cluster maintenance' cannot reach this primary: {e}"
            );
            None
        }
    };
    state.hub.spawn_duration_lowering();
    let local_node = start_local_node(&config, stop_rx);
    let served = serve_until_stopped(
        listener,
        material.config,
        handle,
        service(state),
        local_node,
    )
    .await;
    drop(control);
    // A demotion whose record is still being retried gets one last attempt,
    // so a clean stop does not come back as a primary
    // (`node-agent-protocol.md` section 7.4).
    store.record_demotion_before_exit().await;
    served
}

/// `meta.demoted_by_term` of the database at `database`, read without any
/// write or migration, or `None` when there is no database yet. The other
/// commands that write `quoro.db` refuse on a demoted host with it
/// (`node-agent-protocol.md` section 7.4).
///
/// # Errors
///
/// A one-line message when the database cannot be read.
pub async fn demoted_by_term(database: &std::path::Path) -> Result<Option<String>, String> {
    if !database.is_file() {
        return Ok(None);
    }
    let store = Store::open_unmigrated(database)
        .await
        .map_err(|e| format!("cannot open {}: {e}", database.display()))?;
    let term = store
        .demoted_by_term()
        .await
        .map_err(|e| format!("cannot read {}: {e}", database.display()));
    store.pool().close().await;
    term
}

/// The whole service of a primary: the API, MCP and the panel as the
/// fallback, with the demotion guard as the outermost layer, so a primary
/// that demotes itself answers every request with `503` `not_primary`
/// before any route or authentication runs (`node-agent-protocol.md`
/// section 7.4).
fn service(state: AppState) -> axum::Router {
    let store = state.store.clone();
    quoro_control_plane::demotion::guard(
        router(state.clone())
            .merge(quoro_mcp::router(state))
            .fallback(crate::panel::serve),
        store,
    )
}

/// Runs a primary that a higher term demoted (`node-agent-protocol.md`
/// section 7.4, ADR 0230): `meta.demoted_by_term` is set. It writes nothing:
/// no upgrade backup, no migration, no first-start backfill or seal, no key,
/// no TLS material and no join file, and it starts none of the control
/// plane's loops. It loads the TLS material it has and answers every request
/// with `503` `not_primary`. The host's own node agent still runs, so it
/// reaches the new primary.
async fn run_demoted(config: Config, term: &str) -> Result<i32, String> {
    let material = tls::load_existing(&config.data_dir).map_err(|e| {
        format!("cannot load the TLS material of this demoted primary, and none is generated: {e}")
    })?;
    let listener = TcpListener::bind(config.listen)
        .map_err(|e| format!("cannot bind {}: {e}", config.listen))?;
    let address = listener
        .local_addr()
        .map_err(|e| format!("cannot read the bound address: {e}"))?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = Handle::new();
    tokio::spawn(shutdown_on_signal(handle.clone(), stop_tx));

    info!("listening on https://{address}");
    info!("tls fingerprint: {}", material.fingerprint);
    warn!(
        "this host is a demoted primary at term {term}: it answers every request with 503 not_primary"
    );
    let local_node = start_local_node(&config, stop_rx);
    serve_until_stopped(
        listener,
        material.config,
        handle,
        quoro_control_plane::demotion::responder(),
        local_node,
    )
    .await
}

/// Starts the in-process node agent when `[node] local` is set.
fn start_local_node(
    config: &Config,
    stop_rx: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<Result<i32, String>>> {
    if !config.node.local {
        return None;
    }
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
}

/// Serves `service` on `listener` until a signal, or until the local node
/// agent stops, and returns the exit code.
async fn serve_until_stopped(
    listener: TcpListener,
    tls: Arc<rustls::ServerConfig>,
    handle: Handle,
    service: axum::Router,
    local_node: Option<tokio::task::JoinHandle<Result<i32, String>>>,
) -> Result<i32, String> {
    let server = axum_server::from_tcp_rustls(
        listener,
        axum_server::tls_rustls::RustlsConfig::from_config(tls),
    )
    .handle(handle.clone())
    .serve(service.into_make_service_with_connect_info::<SocketAddr>());

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
/// unspecified host (`0.0.0.0`, `::`) replaced by loopback. A promoted
/// primary's agent dials the same address (`promote::prepare`).
pub(crate) fn loopback(address: SocketAddr) -> SocketAddr {
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

    /// A running primary that demotes itself answers every route of the
    /// service `quoro serve` composes with `503` `not_primary`: the API, MCP,
    /// the panel and its assets (`node-agent-protocol.md` section 7.4).
    #[tokio::test]
    async fn the_composed_service_answers_not_primary_once_demoted() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let data_dir = std::env::temp_dir().join(format!(
            "quoro-serve-demotion-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let material = tls::load_or_generate(&data_dir, "primary", listen).unwrap();
        let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
        let state = AppState::open(
            store,
            ServerConfig {
                data_dir: data_dir.clone(),
                secure_cookies: false,
                public_url: None,
                tls_fingerprint: material.fingerprint.clone(),
                tls: material.identity.clone(),
                node_network: NodeNetwork {
                    subnet: "10.42.1.0/24".to_string(),
                    gateway: "10.42.1.1".to_string(),
                },
                heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
                session_timeout_ms: SESSION_TIMEOUT_MS,
            },
            Arc::new(ReferenceAsDigest),
            Arc::new(DataKey::ephemeral()),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = service(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let mcp = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });

        let health = client
            .get(format!("{base}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), 200);
        let before = client
            .post(format!("{base}/mcp"))
            .json(&mcp)
            .send()
            .await
            .unwrap();
        assert_ne!(before.status(), 503, "MCP answers before the demotion");

        state.hub.stale_term(Some(5)).await;
        for (what, request) in [
            ("health", client.get(format!("{base}/api/v1/health"))),
            ("an API route", client.get(format!("{base}/api/v1/nodes"))),
            ("MCP", client.post(format!("{base}/mcp")).json(&mcp)),
            ("the panel root", client.get(format!("{base}/"))),
            ("a panel asset", client.get(format!("{base}/assets/app.js"))),
            ("a panel page", client.get(format!("{base}/apps"))),
        ] {
            let res = request.send().await.unwrap();
            assert_eq!(res.status(), 503, "{what}");
            let content_type = res
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            assert_eq!(content_type, "application/problem+json", "{what}");
            let body: serde_json::Value = res.json().await.unwrap();
            assert_eq!(body["code"], "not_primary", "{what}: {body}");
        }
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
