//! Shared harness for the HTTP integration tests: a control plane on an
//! ephemeral local port over plain HTTP (TLS is the binary's concern and is
//! tested there), a temporary data directory and a cookie-carrying client.

#![allow(dead_code)]

use quoro_control_plane::auth::setup_token;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::store::{Store, random_secret};
use quoro_control_plane::{
    AppState, DigestResolver, NodeNetwork, ReferenceAsDigest, ServerConfig, router,
};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;

pub const OWNER_EMAIL: &str = "owner@example.com";
pub const OWNER_PASSWORD: &str = "correct horse battery staple";

pub const TLS_FINGERPRINT: &str = concat!(
    "sha256:",
    "0000000000000000000000000000000000000000000000000000000000000000"
);

pub struct Server {
    pub base: String,
    pub data_dir: PathBuf,
    pub state: AppState,
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

/// Starts a control plane on 127.0.0.1 with an empty database.
pub async fn start() -> Server {
    start_inner(200, 800, None).await
}

/// Starts a control plane on an existing data directory, reusing its key
/// material.
///
/// The restart tests stop one process and start another on the same directory.
/// The data-encryption key must outlive the process, as `keys/data.key` does in
/// production: a fresh key would leave stored webhook secrets undecryptable.
pub async fn start_in(data_dir: PathBuf) -> Server {
    let data_key = Arc::new(DataKey::load_or_create(&data_dir).unwrap());
    start_at(data_dir, 200, 800, None, data_key).await
}

/// Starts a control plane whose node sessions outlive the time a test spends
/// talking to the API between frames: the heartbeat interval stays at 200 ms
/// (the node-session tests assert it in `welcome`) but a node that sends
/// nothing is only dropped after thirty seconds. Use this for every node test
/// that is not itself about the session timeout.
pub async fn start_with_patient_node_session() -> Server {
    start_inner(200, 30_000, None).await
}

/// Starts a control plane whose Cloudflare clients reach a local mock, with a
/// session timeout long enough for a test to poll the API while a node stays
/// connected.
pub async fn start_with_cloudflare(api_base: &str, doh_base: &str) -> Server {
    start_inner(
        5_000,
        60_000,
        Some((api_base.to_string(), doh_base.to_string())),
    )
    .await
}

async fn start_inner(
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    cloudflare: Option<(String, String)>,
) -> Server {
    let data_dir = std::env::temp_dir().join(format!("quoro-test-{}", &random_secret()[..16]));
    start_at(
        data_dir,
        heartbeat_interval_ms,
        session_timeout_ms,
        cloudflare,
        Arc::new(DataKey::ephemeral()),
    )
    .await
}

async fn start_at(
    data_dir: PathBuf,
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    cloudflare: Option<(String, String)>,
    data_key: Arc<DataKey>,
) -> Server {
    // reqwest may carry the binary's no-provider rustls feature in a workspace
    // build; an earlier test having installed it already is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
    std::fs::create_dir_all(&data_dir).unwrap();
    let store = Store::open(&data_dir.join("quoro.db")).await.unwrap();
    let mut state = AppState::open(
        store,
        ServerConfig {
            data_dir: data_dir.clone(),
            secure_cookies: false,
            public_url: Some("https://primary.example:8443".to_string()),
            tls_fingerprint: TLS_FINGERPRINT.to_string(),
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
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir,
        state,
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
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base,
        data_dir: server.data_dir.clone(),
        state,
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
