//! Shared harness for the HTTP integration tests: a control plane on an
//! ephemeral local port over plain HTTP (TLS is the binary's concern and is
//! tested there), a temporary data directory and a cookie-carrying client.

#![allow(dead_code)]

pub mod log;
pub mod machine;
pub mod replay;

use quoro_control_plane::auth::setup_token;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::custody::TlsIdentity;
use quoro_control_plane::store::{Store, random_secret};
use quoro_control_plane::{
    AppState, DigestResolver, NodeNetwork, ReferenceAsDigest, ServerConfig, router,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const OWNER_EMAIL: &str = "owner@example.com";
pub const OWNER_PASSWORD: &str = "correct horse battery staple";

pub const TLS_FINGERPRINT: &str = concat!(
    "sha256:",
    "0000000000000000000000000000000000000000000000000000000000000000"
);

/// A data directory under the system temp directory, removed with everything
/// in it when the last clone drops, including when the test panics.
///
/// Every [`Server`] holds a clone of its directory, and so does a server
/// restarted on it, so the directory outlives a server that is dropped and
/// reopened. A test that stops every server and opens the directory again
/// keeps its own clone from [`TestDir::new`] until it ends. Dropping a
/// `Server` does not stop its tasks: they run on until the test's runtime
/// ends, and find the directory gone once its last clone dropped.
#[derive(Clone)]
pub struct TestDir(Arc<DirGuard>);

struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        // A file written into the directory while it is being removed, as by
        // a database connection of a task still running, fails the removal;
        // a later try, after a short wait, removes that file too.
        for attempt in 0..3_u32 {
            match std::fs::remove_dir_all(&self.0) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    if attempt == 2 {
                        eprintln!("cannot remove the test directory {}: {e}", self.0.display());
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10 << attempt));
                }
                _ => return,
            }
        }
    }
}

impl TestDir {
    /// Creates `<temp dir>/<prefix>-<random>`.
    pub fn new(prefix: &str) -> TestDir {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", &random_secret()[..16]));
        std::fs::create_dir_all(&path).unwrap();
        TestDir(Arc::new(DirGuard(path)))
    }

    pub fn path(&self) -> &Path {
        &self.0.0
    }
}

impl std::ops::Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        self.path()
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

pub struct Server {
    pub base: String,
    pub data_dir: PathBuf,
    pub state: AppState,
    /// Removes `data_dir` once this server, every server restarted on it and
    /// the test's own [`TestDir`] are dropped.
    dir: TestDir,
}

impl Server {
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// The `host:port` the test listener bound.
    pub fn address(&self) -> String {
        self.base.trim_start_matches("http://").to_string()
    }

    /// A client with its own cookie jar.
    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .unwrap()
    }

    /// Issues a setup token the way `quoro setup-token` does.
    pub fn setup_token(&self) -> String {
        setup_token::issue(&self.data_dir).unwrap()
    }

    /// Runs setup and logs in; returns a client holding the owner session.
    pub async fn owner(&self) -> reqwest::Client {
        let token = self.setup_token();
        let client = self.client();
        let res = client
            .post(self.url("/api/v1/setup"))
            .header("Quoro-Setup-Token", token)
            .json(&json!({ "email": OWNER_EMAIL, "name": "Owner", "password": OWNER_PASSWORD }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "setup: {}", res.text().await.unwrap());
        self.login(&client, OWNER_EMAIL, OWNER_PASSWORD).await;
        client
    }

    pub async fn login(&self, client: &reqwest::Client, email: &str, password: &str) {
        let res = client
            .post(self.url("/api/v1/auth/login"))
            .json(&json!({ "email": email, "password": password }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "login: {}", res.text().await.unwrap());
    }

    /// Inserts a user directly, for role tests (no user-management API in M1).
    pub async fn insert_user(&self, email: &str, password: &str, role: &str) -> String {
        let id = quoro_types::new_id(quoro_types::Prefix::User);
        let hash = quoro_control_plane::auth::password::hash(password).unwrap();
        sqlx::query("INSERT INTO users (id, email, name, role, password_hash, disabled, created_at) VALUES (?, ?, ?, ?, ?, 0, ?)")
            .bind(&id)
            .bind(email)
            .bind(email)
            .bind(role)
            .bind(hash)
            .bind(quoro_control_plane::store::now_rfc3339())
            .execute(self.state.store.pool())
            .await
            .unwrap();
        id
    }

    /// Every audit record, oldest first, as (operation, result, result_detail).
    pub async fn audit(&self) -> Vec<(String, String, Option<String>)> {
        sqlx::query_as("SELECT operation, result, result_detail FROM audit_log ORDER BY seq")
            .fetch_all(self.state.store.pool())
            .await
            .unwrap()
    }
}

/// The primary's TLS pair as `quoro serve` keeps it, `<data_dir>/tls/cert.pem`
/// and `key.pem`: a self-signed pair from rcgen written on first use and read
/// back after, so a restart on the same directory keeps its certificate.
pub fn tls_identity(data_dir: &Path) -> TlsIdentity {
    let dir = data_dir.join("tls");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    if !cert_path.exists() {
        std::fs::create_dir_all(&dir).unwrap();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    }
    TlsIdentity::from_pem(
        std::fs::read_to_string(&cert_path).unwrap(),
        std::fs::read_to_string(&key_path).unwrap(),
    )
    .unwrap()
}

/// The router the harness serves: the API behind the demotion guard, as
/// `quoro serve` wraps its whole service (`node-agent-protocol.md` section
/// 7.4).
pub fn served(state: &AppState) -> axum::Router {
    quoro_control_plane::demotion::guard(router(state.clone()), state.store.clone())
}

/// Starts a control plane on 127.0.0.1 with an empty database.
pub async fn start() -> Server {
    start_inner(200, 800, None, false).await
}

/// Starts a control plane on an existing data directory, reusing its key
/// material.
///
/// The restart tests stop one process and start another on the same directory.
/// The data-encryption key must outlive the process, as `keys/data.key` does in
/// production: a fresh key would leave stored webhook secrets undecryptable.
/// Such a test keeps a clone of `data_dir` while no server is running.
pub async fn start_in(data_dir: TestDir) -> Server {
    let data_key = Arc::new(DataKey::load_or_create(&data_dir).unwrap());
    start_at(data_dir, 200, 800, None, data_key, false).await
}

/// [`start_in`] with the patient node sessions of
/// [`start_with_patient_node_session`]: every key the primary holds is a file
/// in `data_dir`, as in production, so a test can read the key files.
pub async fn start_patient_in(data_dir: TestDir) -> Server {
    let data_key = Arc::new(DataKey::load_or_create(&data_dir).unwrap());
    start_at(data_dir, 200, 30_000, None, data_key, false).await
}

/// Starts a control plane whose node sessions outlive the time a test spends
/// talking to the API between frames: the heartbeat interval stays at 200 ms
/// (the node-session tests assert it in `welcome`) but a node that sends
/// nothing is only dropped after thirty seconds. Use this for every node test
/// that is not itself about the session timeout.
pub async fn start_with_patient_node_session() -> Server {
    start_inner(200, 30_000, None, false).await
}

/// Like [`start_with_patient_node_session`], but the router sees each
/// connection's peer address, as the binary's does. The hub then records the
/// source address of every node session, which the mesh endpoints come from
/// (`node-agent-protocol.md` section 6.9, ADR 0144, ADR 0266).
pub async fn start_with_peer_addresses() -> Server {
    start_inner(200, 30_000, None, true).await
}

/// Starts a control plane whose Cloudflare clients reach a local mock, with a
/// session timeout long enough for a test to poll the API while a node stays
/// connected.
pub async fn start_with_cloudflare(api_base: &str, doh_base: &str) -> Server {
    start_inner(
        5_000,
        60_000,
        Some((api_base.to_string(), doh_base.to_string())),
        false,
    )
    .await
}

async fn start_inner(
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    cloudflare: Option<(String, String)>,
    peer_addresses: bool,
) -> Server {
    start_at(
        TestDir::new("quoro-test"),
        heartbeat_interval_ms,
        session_timeout_ms,
        cloudflare,
        Arc::new(DataKey::ephemeral()),
        peer_addresses,
    )
    .await
}

async fn start_at(
    dir: TestDir,
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    cloudflare: Option<(String, String)>,
    data_key: Arc<DataKey>,
    peer_addresses: bool,
) -> Server {
    // reqwest may carry the binary's no-provider rustls feature in a workspace
    // build; an earlier test having installed it already is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let data_dir = dir.to_path_buf();
    std::fs::create_dir_all(&data_dir).unwrap();
    let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
    let mut state = AppState::open(
        store,
        ServerConfig {
            data_dir: data_dir.clone(),
            secure_cookies: false,
            public_url: Some("https://primary.example:8443".to_string()),
            tls_fingerprint: TLS_FINGERPRINT.to_string(),
            tls: tls_identity(&data_dir),
            node_network: NodeNetwork {
                subnet: "10.42.1.0/24".to_string(),
                gateway: "10.42.1.1".to_string(),
            },
            heartbeat_interval_ms,
            session_timeout_ms,
        },
        Arc::new(ReferenceAsDigest),
        data_key,
    )
    .await
    .unwrap();
    if let Some((api_base, doh_base)) = cloudflare {
        state = state.with_cloudflare_endpoints(api_base, doh_base);
    }
    let app = served(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        if peer_addresses {
            let app = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
            axum::serve(listener, app).await.unwrap();
        } else {
            axum::serve(listener, app).await.unwrap();
        }
    });
    Server {
        base,
        data_dir,
        state,
        dir,
    }
}

/// Serves `state`, which a test opened itself on `dir` (as on a data
/// directory a restore installed), as the other starts serve theirs. `dir` is
/// removed once this server and every other clone of it drop.
pub async fn serve_in(dir: TestDir, state: AppState) -> Server {
    let app = served(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir: dir.to_path_buf(),
        state,
        dir,
    }
}

/// Starts a second control-plane process on `server`'s database, as after a
/// restart: a new `AppState` with empty in-memory caches that resolves image
/// references with `resolver`. Stop the first process's node sessions first.
pub async fn restart_with_resolver(server: &Server, resolver: Arc<dyn DigestResolver>) -> Server {
    let state = AppState::open(
        server.state.store.clone(),
        (*server.state.config).clone(),
        resolver,
        Arc::clone(&server.state.data_key),
    )
    .await
    .unwrap();
    let app = served(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir: server.data_dir.clone(),
        state,
        dir: server.dir.clone(),
    }
}

/// [`restart_with_resolver`] with [`ReferenceAsDigest`], except that the TLS
/// pair is read again from `<data_dir>/tls/` with [`tls_identity`], as
/// `quoro serve` reads it at every start: a test that replaced the files
/// restarts on the new pair. Stop the first process's node sessions first.
pub async fn restart_reloading_tls(server: &Server) -> Server {
    let mut config = (*server.state.config).clone();
    config.tls = tls_identity(&server.data_dir);
    let state = AppState::open(
        server.state.store.clone(),
        config,
        Arc::new(ReferenceAsDigest),
        Arc::clone(&server.state.data_key),
    )
    .await
    .unwrap();
    let app = served(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir: server.data_dir.clone(),
        state,
        dir: server.dir.clone(),
    }
}

/// The problem body's `code` and `reason`.
pub async fn problem(res: reqwest::Response) -> (u16, String, Option<String>) {
    let status = res.status().as_u16();
    let ct = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(ct, "application/problem+json", "status {status}");
    let body: Value = res.json().await.unwrap();
    (
        status,
        body["code"].as_str().unwrap_or_default().to_string(),
        body["reason"].as_str().map(str::to_string),
    )
}

/// Records that every current voter acknowledged the current voter-set
/// version, as a session of each would (`node-agent-protocol.md` section
/// 8.10 rule 2), so the next enrollment of a voter is not refused while the
/// earlier voters have no session. Called by the enroll helpers of tests
/// that are not about rule 2; the tests of rule 2 never call it.
pub async fn settle_voters(server: &Server) {
    server.state.hub.ack_voter_set().await;
}
