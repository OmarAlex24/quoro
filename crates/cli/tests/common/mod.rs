//! An in-process primary for the client's end-to-end tests.
//!
//! The control plane's router behind a TLS listener with a fresh self-signed
//! certificate, the way `quoro serve` serves it, so the client is tested over
//! real TLS against the pin it must check. A second, plain HTTP listener on
//! the same state is for fixtures only (the first owner's setup), so the tests
//! need no TLS client of their own. No engine runs: the API answers from its
//! database.

#![allow(dead_code)]

use axum::extract::Request;
use axum::middleware::{self, Next};
use quoro_cli::Environment;
use quoro_control_plane::auth::setup_token;
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::store::{Store, random_secret, sha256_hex};
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig, router};
use rustls::pki_types::PrivatePkcs8KeyDer;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Primary {
    /// `https://127.0.0.1:<port>`, the TLS listener the client talks to.
    pub url: String,
    /// `sha256:<hex>` of the listener's certificate.
    pub fingerprint: String,
    pub state: AppState,
    /// A scratch directory: the database, and homes for the client.
    pub dir: PathBuf,
    /// Requests that reached the TLS listener's router.
    requests: Arc<AtomicUsize>,
    plain: String,
}

/// A client run: its exit code and what it printed.
pub struct Output {
    pub code: u8,
    pub stdout: String,
    pub stderr: String,
}

pub async fn start() -> Primary {
    // The fixture client is reqwest without a provider of its own.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!("quoro-cli-test-{}", &random_secret()[..16]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("quoro.db")).await.unwrap();

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert = certified.cert.der().clone();
    let fingerprint = format!("sha256:{}", sha256_hex(&cert));
    let key = PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key.into())
    .unwrap();

    let state = AppState::open(
        store,
        ServerConfig {
            data_dir: dir.clone(),
            secure_cookies: true,
            public_url: None,
            tls_fingerprint: fingerprint.clone(),
            node_network: NodeNetwork {
                subnet: "10.42.1.0/24".to_string(),
                gateway: "10.42.1.1".to_string(),
            },
            heartbeat_interval_ms: 200,
            session_timeout_ms: 800,
        },
        Arc::new(ReferenceAsDigest),
        Arc::new(DataKey::ephemeral()),
    )
    .await
    .unwrap();

    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let counted =
        router(state.clone()).layer(middleware::from_fn(move |request: Request, next: Next| {
            counter.fetch_add(1, Ordering::SeqCst);
            next.run(request)
        }));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let server = axum_server::from_tcp_rustls(
        listener,
        axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(tls)),
    );
    tokio::spawn(server.serve(counted.into_make_service_with_connect_info::<SocketAddr>()));

    let plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plain = format!("http://{}", plain_listener.local_addr().unwrap());
    let fixtures = router(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(plain_listener, fixtures).await });

    Primary {
        url,
        fingerprint,
        state,
        dir,
        requests,
        plain,
    }
}

impl Primary {
    /// `http://127.0.0.1:<port>`, the plain fixture listener, where a test's
    /// fake node enrolls and opens its session.
    pub fn plain_url(&self) -> &str {
        &self.plain
    }

    /// How many requests reached the TLS listener.
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// Creates the first owner and signs it in on the plain fixture listener;
    /// returns the owner's id and the `quoro_session=...` cookie pair.
    pub async fn owner_session(&self) -> (String, String) {
        let setup = setup_token::issue(&self.dir).unwrap();
        let client = reqwest::Client::new();
        let body = r#"{"email":"owner@example.com","name":"Owner","password":"correct horse battery staple"}"#;
        let response = client
            .post(format!("{}/api/v1/setup", self.plain))
            .header("Quoro-Setup-Token", setup)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "setup");
        let user: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        let owner_id = user["id"].as_str().unwrap().to_string();
        let response = client
            .post(format!("{}/api/v1/auth/login", self.plain))
            .header("Content-Type", "application/json")
            .body(r#"{"email":"owner@example.com","password":"correct horse battery staple"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "login");
        let cookie = response
            .headers()
            .get_all("set-cookie")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .find_map(|value| {
                value
                    .split(';')
                    .next()
                    .filter(|pair| pair.starts_with("quoro_session="))
            })
            .expect("a session cookie")
            .to_string();
        (owner_id, cookie)
    }

    /// A request with the session `cookie` on the plain fixture listener; the
    /// status and the JSON body (`Value::Null` when there is none).
    pub async fn with_session(
        &self,
        method: reqwest::Method,
        path: &str,
        cookie: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = reqwest::Client::new()
            .request(method, format!("{}{path}", self.plain))
            .header("Cookie", cookie);
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Creates the first owner and a personal API token (`tok_...`) for it,
    /// inserted directly as `crates/mcp/tests/auth.rs` does, so a test that is
    /// not about tokens does not depend on the tokens API. Returns the owner's
    /// id and the token.
    pub async fn owner_token(&self) -> (String, String) {
        let setup = setup_token::issue(&self.dir).unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/setup", self.plain))
            .header("Quoro-Setup-Token", setup)
            .header("Content-Type", "application/json")
            .body(r#"{"email":"owner@example.com","name":"Owner","password":"correct horse battery staple"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "setup");
        let user: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        let owner_id = user["id"].as_str().unwrap().to_string();

        let secret = format!("tok_{}", "C".repeat(40));
        let hash = sha256_hex(secret.as_bytes());
        sqlx::query(
            "INSERT INTO api_tokens \
             (id, user_id, name, token_hash, expires_at, last_used_at, created_at, revoked_at) \
             VALUES (?, ?, ?, ?, NULL, NULL, '2026-01-01T00:00:00.000Z', NULL)",
        )
        .bind(format!("tok_{}", &hash[..26]))
        .bind(&owner_id)
        .bind("cli test")
        .bind(&hash)
        .execute(self.state.store.pool())
        .await
        .unwrap();
        (owner_id, secret)
    }

    /// A fresh `$XDG_CONFIG_HOME` whose `config.toml` defines the context
    /// `lab` for this primary, pinned, and makes it current.
    pub fn config_home(&self, name: &str) -> PathBuf {
        let home = self.dir.join(name);
        std::fs::create_dir_all(home.join("quoro")).unwrap();
        std::fs::write(
            home.join("quoro/config.toml"),
            format!(
                "current = \"lab\"\n\n[contexts.lab]\nurl = \"{}\"\nfingerprint = \"{}\"\n",
                self.url, self.fingerprint
            ),
        )
        .unwrap();
        home
    }

    /// An environment that selects the context `lab` in `home`.
    pub fn context_environment(&self, home: &Path) -> Environment {
        Environment::new([("XDG_CONFIG_HOME", home.display().to_string())], false)
    }

    /// An ad-hoc context for this primary from `QUORO_URL` and
    /// `QUORO_FINGERPRINT`, plus `extra`.
    pub fn ad_hoc_environment(&self, extra: &[(&str, &str)]) -> Environment {
        let mut vars = vec![
            (
                "XDG_CONFIG_HOME".to_string(),
                self.dir.join("empty-home").display().to_string(),
            ),
            ("QUORO_URL".to_string(), self.url.clone()),
            ("QUORO_FINGERPRINT".to_string(), self.fingerprint.clone()),
        ];
        vars.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
        );
        Environment::new(vars, false)
    }

    /// A request with a bearer token on the plain fixture listener; the
    /// status and the JSON body (`Value::Null` when there is none).
    pub async fn fixture(
        &self,
        method: reqwest::Method,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = reqwest::Client::new()
            .request(method, format!("{}{path}", self.plain))
            .header("Authorization", format!("Bearer {token}"));
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn fixture_ok(&self, path: &str, token: &str, body: Value) -> Value {
        let (status, body) = self
            .fixture(reqwest::Method::POST, path, token, Some(body))
            .await;
        assert!((200..300).contains(&status), "POST {path}: {status} {body}");
        body
    }

    /// Registers the local node and creates the project `demo` with the app
    /// `web`, as the owner; returns the app's id.
    pub async fn app(&self, owner_token: &str) -> String {
        driver::register_local_node(
            &self.state.store,
            &LocalNode {
                name: "test-node".to_string(),
                capacity: Capacity {
                    cpus: 8,
                    memory_mb: 16_384,
                    disk_gb: 100,
                },
                kvm: Kvm {
                    available: true,
                    kernel: "6.8.0".to_string(),
                    nested: Some(true),
                },
                engine_version: "test-engine".to_string(),
                agent_version: "test-agent".to_string(),
            },
        )
        .await
        .unwrap();
        let project = self
            .fixture_ok(
                "/api/v1/projects",
                owner_token,
                json!({ "slug": "demo", "name": "Demo" }),
            )
            .await;
        let app = self
            .fixture_ok(
                "/api/v1/apps",
                owner_token,
                json!({
                    "project_id": project["id"],
                    "name": "web",
                    "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
                    "port": 8080,
                }),
            )
            .await;
        app["id"].as_str().unwrap().to_string()
    }

    /// Creates the agent `name` owned by `owner_id` and a key for it; returns
    /// the agent's id and the key.
    pub async fn agent(&self, owner_token: &str, owner_id: &str, name: &str) -> (String, String) {
        let agent = self
            .fixture_ok(
                "/api/v1/agents",
                owner_token,
                json!({ "name": name, "model": "test-model", "owner_user_id": owner_id }),
            )
            .await;
        let agent_id = agent["id"].as_str().unwrap().to_string();
        let key = self
            .fixture_ok(
                &format!("/api/v1/agents/{agent_id}/keys"),
                owner_token,
                json!({}),
            )
            .await;
        (agent_id, key["key"].as_str().unwrap().to_string())
    }

    /// Grants `scopes` on `selector` to the agent for an hour.
    pub async fn grant(&self, owner_token: &str, agent_id: &str, scopes: &[&str], selector: Value) {
        self.fixture_ok(
            "/api/v1/grants",
            owner_token,
            json!({
                "agent_id": agent_id,
                "scopes": scopes,
                "selector": selector,
                "ttl_s": 3600,
                "reason": "tests",
            }),
        )
        .await;
    }

    /// One integer from a SQL query, such as a count.
    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(self.state.store.pool())
            .await
            .unwrap()
    }

    /// How many access tokens the primary minted.
    pub async fn mints(&self) -> i64 {
        self.count("SELECT COUNT(*) FROM access_tokens").await
    }

    /// The newest audit record's justification, actor type and operation.
    pub async fn last_actor(&self) -> (Option<String>, String, String) {
        sqlx::query_as(
            "SELECT justification, actor_type, operation FROM audit_log ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(self.state.store.pool())
        .await
        .unwrap()
    }

    /// The newest audit record's justification, channel and operation.
    pub async fn last_audit(&self) -> (Option<String>, String, String) {
        sqlx::query_as(
            "SELECT justification, channel, operation FROM audit_log ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(self.state.store.pool())
        .await
        .unwrap()
    }
}

/// Writes `credentials.toml` in `home` with `token` for `lab`, at `mode`.
pub fn write_credentials(home: &Path, token: &str, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    let path = home.join("quoro/credentials.toml");
    std::fs::write(&path, format!("[contexts.lab]\ntoken = \"{token}\"\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// Writes `key` to `dir/name` at `mode`; returns the path.
pub fn key_file(dir: &Path, name: &str, key: &str, mode: u32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, format!("{key}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    path
}

/// A file's permission bits.
pub fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

/// Runs the client with `args` in `environment`.
pub async fn quoro(args: &[&str], environment: &Environment) -> Output {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let code = quoro_cli::run_with(
        args.iter().map(ToString::to_string).collect(),
        environment,
        &mut stdout,
        &mut stderr,
    )
    .await;
    Output {
        code,
        stdout: String::from_utf8(stdout).unwrap(),
        stderr: String::from_utf8(stderr).unwrap(),
    }
}
