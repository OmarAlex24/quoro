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
use quoro_control_plane::store::{Store, random_secret, sha256_hex};
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig, router};
use rustls::pki_types::PrivatePkcs8KeyDer;
use serde_json::Value;
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
    /// How many requests reached the TLS listener.
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// Creates the first owner and a personal API token (`tok_...`) for it,
    /// inserted directly as `crates/mcp/tests/auth.rs` does: the tokens API
    /// is not there yet. Returns the owner's id and the token.
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
