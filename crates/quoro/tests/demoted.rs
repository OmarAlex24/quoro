//! A primary that learned of a higher term starts demoted
//! (`node-agent-protocol.md` section 7.4, ADR 0230, issue #728).
//!
//! The data directory holds a database whose `meta` has `demoted_by_term`.
//! `quoro serve` then writes nothing: no migration, no backup, no key, no
//! TLS material and no join file. It answers every request with `503`
//! `not_primary`, and it refuses to start when its TLS material is missing.

use quoro_control_plane::store::{Store, sha256_hex};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

/// A data directory that is removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("quoro-{label}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create the temporary data directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The configuration the server reads: loopback on an ephemeral port, the
/// fake engine, and the default local node agent beside the control plane.
fn write_config(dir: &Path) -> PathBuf {
    let path = dir.join("quoro.toml");
    let body = format!(
        "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \"test-node\"\n\n[node]\ndata_dir = \"{}\"\n",
        dir.join("cp").display(),
        dir.join("node").display(),
    );
    std::fs::write(&path, body).expect("write quoro.toml");
    path
}

/// A control-plane data directory whose database a higher term demoted at
/// term 5, with the TLS material of its listener when `tls`.
async fn demoted_data_dir(cp: &Path, tls: bool) {
    std::fs::create_dir_all(cp).unwrap();
    let store = Store::open(&cp.join("quoro.db")).await.unwrap();
    sqlx::query("INSERT INTO meta (key, value) VALUES ('demoted_by_term', '5')")
        .execute(store.pool())
        .await
        .unwrap();
    // Every page in the main file now: the pool's connections close on their
    // own threads, after `close` returns, and the checkpoint they would run
    // then must not move the file the test sums.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    if tls {
        let dir = cp.join("tls");
        std::fs::create_dir_all(&dir).unwrap();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        std::fs::write(dir.join("key.pem"), certified.key_pair.serialize_pem()).unwrap();
        std::fs::write(dir.join("cert.pem"), certified.cert.pem()).unwrap();
    }
}

/// A control-plane data directory demoted at term 5 whose database is still
/// at the base schema, as on a host upgraded to a binary with migrations to
/// apply: a demoted start must not apply them.
async fn demoted_data_dir_with_pending_migrations(cp: &Path) {
    std::fs::create_dir_all(cp).unwrap();
    let store = Store::open_unmigrated(&cp.join("quoro.db")).await.unwrap();
    sqlx::raw_sql(include_str!("../../../docs/contracts/db-schema.sql"))
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO meta (key, value) VALUES ('demoted_by_term', '5')")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(store.pending_migrations().await.unwrap().is_some());
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    let dir = cp.join("tls");
    std::fs::create_dir_all(&dir).unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    std::fs::write(dir.join("key.pem"), certified.key_pair.serialize_pem()).unwrap();
    std::fs::write(dir.join("cert.pem"), certified.cert.pem()).unwrap();
}

/// Every path under `dir`, relative to it, except SQLite's side files, which
/// any reader creates.
fn entries(dir: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap().display().to_string();
            if relative.ends_with("-wal") || relative.ends_with("-shm") {
                continue;
            }
            if path.is_dir() {
                walk(root, &path, out);
            }
            out.insert(relative);
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

fn checksum(path: &Path) -> String {
    sha256_hex(&std::fs::read(path).expect("read the database file"))
}

/// Asserts the `503` `not_primary` problem with its request id.
async fn assert_not_primary(response: reqwest::Response, what: &str) {
    assert_eq!(response.status().as_u16(), 503, "{what}");
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    assert_eq!(
        header("content-type").as_deref(),
        Some("application/problem+json"),
        "{what}"
    );
    assert!(
        header("quoro-request-id").is_some_and(|id| id.starts_with("req_")),
        "{what}"
    );
    let body: serde_json::Value = response.json().await.expect("a JSON problem");
    assert_eq!(body["code"], "not_primary", "{what}: {body}");
    assert_eq!(body["status"], 503, "{what}: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_demoted_primary_starts_without_writing_and_answers_not_primary() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("the ring provider installs once per process");
    let dir = TempDir::new("demoted");
    let config = write_config(dir.path());
    let cp = dir.path().join("cp");
    demoted_data_dir(&cp, true).await;
    let database = cp.join("quoro.db");
    let sum_before = checksum(&database);
    let entries_before = entries(&cp);

    let mut server = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .env("QUORO_TEST_NO_VULN_DOWNLOAD", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let stderr = server.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    let mut log: Vec<String> = Vec::new();
    let port = timeout(Duration::from_secs(30), async {
        let mut port = None;
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Some(rest) = line.split("listening on https://").nth(1) {
                        let address = rest.split_whitespace().next().expect("the address");
                        port = address
                            .rsplit(':')
                            .next()
                            .and_then(|p| p.parse::<u16>().ok());
                    }
                    let demoted = line.contains("demoted primary at term 5")
                        && line.contains("quoro node rejoin");
                    log.push(line);
                    if demoted {
                        return port.expect("the address is logged first");
                    }
                }
                Ok(None) => panic!("the server exited:\n{}", log.join("\n")),
                Err(e) => panic!("cannot read the server's stderr: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the server did not start demoted within 30 s:\n{}",
            log.join("\n")
        )
    });
    // Keep draining stderr so the server never blocks on a full pipe.
    let drained = tokio::spawn(async move {
        let mut rest = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            rest.push(line);
        }
        rest
    });

    let base = format!("https://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build the HTTPS client");
    assert_not_primary(
        client
            .get(format!("{base}/api/v1/nodes"))
            .send()
            .await
            .unwrap(),
        "an API route",
    )
    .await;
    assert_not_primary(
        client
            .post(format!("{base}/api/v1/setup"))
            .json(&serde_json::json!({ "email": "a@example.com", "name": "a", "password": "b" }))
            .send()
            .await
            .unwrap(),
        "setup",
    )
    .await;
    assert_not_primary(
        client.get(format!("{base}/")).send().await.unwrap(),
        "the panel root",
    )
    .await;
    assert_not_primary(
        client
            .get(format!("{base}/assets/index.js"))
            .send()
            .await
            .unwrap(),
        "a panel asset",
    )
    .await;
    assert_not_primary(
        client
            .post(format!("{base}/mcp"))
            .header("Authorization", "Bearer atk_unknown")
            .json(&serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .send()
            .await
            .unwrap(),
        "MCP",
    )
    .await;
    assert_not_primary(
        client
            .get(format!("{base}/api/v1/nodes/session"))
            .header("Authorization", "Bearer a-credential-this-host-never-saw")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await
            .unwrap(),
        "the node session upgrade",
    )
    .await;

    server.kill().await.expect("stop the server");
    let _ = server.wait().await;
    let rest = drained.await.unwrap_or_default();
    log.extend(rest);

    assert_eq!(
        checksum(&database),
        sum_before,
        "the database file is byte-for-byte unchanged:\n{}",
        log.join("\n")
    );
    let wal = cp.join("quoro.db-wal");
    if wal.exists() {
        assert_eq!(
            std::fs::metadata(&wal).unwrap().len(),
            0,
            "nothing was written to the write-ahead log"
        );
    }
    assert_eq!(
        entries(&cp),
        entries_before,
        "no file is created in the data directory: no key, no join file, no cache"
    );
    assert!(
        !cp.join(quoro_control_plane::api::enroll::LOCAL_JOIN_FILE)
            .exists()
    );
    assert!(
        log.iter()
            .any(|line| line.contains("starting the local node agent")),
        "the host's node agent still runs:\n{}",
        log.join("\n")
    );
    assert!(
        !log.iter()
            .any(|line| line.contains("webhook dispatcher resumes")),
        "no loop of the control plane started:\n{}",
        log.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_demoted_primary_without_its_tls_material_refuses_to_start() {
    let dir = TempDir::new("demoted-no-tls");
    let config = write_config(dir.path());
    let cp = dir.path().join("cp");
    demoted_data_dir(&cp, false).await;
    let sum_before = checksum(&cp.join("quoro.db"));

    let output = timeout(
        Duration::from_secs(30),
        Command::new(env!("CARGO_BIN_EXE_quoro"))
            .arg("serve")
            .arg("--config")
            .arg(&config)
            .env("QUORO_TEST_NO_VULN_DOWNLOAD", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("the server refuses within 30 s")
    .expect("run quoro serve");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "a refusal: {stderr}");
    assert!(stderr.contains("TLS material"), "{stderr}");
    assert!(!cp.join("tls").exists(), "no TLS material is generated");
    assert!(!cp.join("keys").exists(), "no key is generated");
    assert_eq!(checksum(&cp.join("quoro.db")), sum_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_demoted_primary_with_pending_migrations_neither_migrates_nor_backs_up() {
    let dir = TempDir::new("demoted-pending");
    let config = write_config(dir.path());
    let cp = dir.path().join("cp");
    demoted_data_dir_with_pending_migrations(&cp).await;
    let database = cp.join("quoro.db");
    let sum_before = checksum(&database);
    let entries_before = entries(&cp);

    let mut server = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .env("QUORO_TEST_NO_VULN_DOWNLOAD", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let stderr = server.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    let mut log: Vec<String> = Vec::new();
    let demoted = timeout(Duration::from_secs(30), async {
        while let Ok(Some(line)) = lines.next_line().await {
            let demoted = line.contains("demoted primary at term 5");
            log.push(line);
            if demoted {
                return true;
            }
        }
        false
    })
    .await;
    server.kill().await.expect("stop the server");
    let _ = server.wait().await;
    assert_eq!(
        demoted,
        Ok(true),
        "not started demoted:\n{}",
        log.join("\n")
    );

    assert_eq!(checksum(&database), sum_before, "no migration ran");
    assert_eq!(
        entries(&cp),
        entries_before,
        "no upgrade backup file, no key"
    );
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .read_only(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let version: String = sqlx::query_scalar("SELECT value FROM meta WHERE key = 'schema_version'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(version, "1", "schema_version is unchanged");
    pool.close().await;
}
