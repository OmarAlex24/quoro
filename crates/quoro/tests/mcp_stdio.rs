//! End-to-end test of `quoro mcp stdio` (ADR 0054, issue #124).
//!
//! A real `quoro serve` is the primary: setup, an owner, an agent with a key
//! and a `cluster:read` grant. `quoro mcp stdio` then runs with that key in a
//! `0600` file and the primary's certificate fingerprint, and the test speaks
//! newline-delimited JSON-RPC on its stdin: `initialize`, the `initialized`
//! notification, `tools/list` and a `get_cluster` call. The audit log must
//! show the call as the agent's, on the `mcp` channel, because the relay only
//! forwards to the primary's `/mcp` and adds nothing of its own (ADR 0031).
//!
//! The failure cases need no session: a missing, shared or malformed key
//! file, and a fingerprint that does not match the primary, all end the
//! process with a message on stderr before it reads stdin.

use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::timeout;

const EMAIL: &str = "owner@example.test";
const PASSWORD: &str = "correct horse battery staple";
const WAIT: Duration = Duration::from_secs(30);

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
        std::fs::create_dir_all(&path).expect("create the temporary directory");
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

/// Writes `content` to `dir/name` with `mode`.
fn write_file(dir: &Path, name: &str, content: &str, mode: u32) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write the file");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    path
}

/// A running `quoro serve` and what a client needs to reach it.
struct Primary {
    _process: Child,
    base: String,
    fingerprint: String,
    config: PathBuf,
}

/// Starts `quoro serve` on an ephemeral port with the fake engine and reads
/// the address and the certificate fingerprint from its start log.
async fn start_primary(dir: &Path) -> Primary {
    let config = dir.join("quoro.toml");
    std::fs::write(
        &config,
        format!(
            "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \"mcp-node\"\n\n[node]\ndata_dir = \"{}\"\n",
            dir.join("cp").display(),
            dir.join("node").display(),
        ),
    )
    .expect("write quoro.toml");
    let mut process = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");
    let mut lines = BufReader::new(process.stderr.take().expect("stderr is piped")).lines();
    let mut log = Vec::new();
    let (port, fingerprint) = timeout(WAIT, async {
        let mut port = None;
        loop {
            let Some(line) = lines.next_line().await.expect("read the server's stderr") else {
                panic!("the server exited before it listened:\n{}", log.join("\n"));
            };
            if let Some(rest) = line.split("listening on https://").nth(1) {
                let address = rest.split_whitespace().next().expect("the address");
                port = address
                    .rsplit(':')
                    .next()
                    .and_then(|p| p.parse::<u16>().ok());
            }
            if let Some(rest) = line.split("tls fingerprint: ").nth(1) {
                let fingerprint = rest.split_whitespace().next().expect("a fingerprint");
                return (
                    port.expect("the address is logged first"),
                    fingerprint.to_string(),
                );
            }
            log.push(line);
        }
    })
    .await
    .expect("the server logged its address and fingerprint");
    // Keep draining stderr so the server never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    Primary {
        _process: process,
        base: format!("https://127.0.0.1:{port}"),
        fingerprint,
        config,
    }
}

/// Asserts the status and returns the JSON body.
async fn json_body(response: reqwest::Response, expected: u16) -> Value {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status.as_u16(),
        expected,
        "expected {expected}, got {status}: {body}"
    );
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"))
}

/// Runs setup and logs the owner in; returns the session client and the
/// owner's user id.
async fn owner(primary: &Primary) -> (reqwest::Client, String) {
    let issued = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args(["setup-token", "--config"])
        .arg(&primary.config)
        .output()
        .await
        .expect("run setup-token");
    assert!(
        issued.status.success(),
        "setup-token: {}",
        String::from_utf8_lossy(&issued.stderr)
    );
    let token = String::from_utf8_lossy(&issued.stdout).trim().to_string();
    // The test only sets the primary up; the relay under test pins the
    // certificate itself.
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build the HTTPS client");
    let base = &primary.base;
    let response = client
        .post(format!("{base}/api/v1/setup"))
        .header("Quoro-Setup-Token", token)
        .json(&json!({ "email": EMAIL, "name": "Owner", "password": PASSWORD }))
        .send()
        .await
        .expect("POST /api/v1/setup");
    let user = json_body(response, 201).await;
    let response = client
        .post(format!("{base}/api/v1/auth/login"))
        .json(&json!({ "email": EMAIL, "password": PASSWORD }))
        .send()
        .await
        .expect("POST /api/v1/auth/login");
    json_body(response, 200).await;
    (
        client,
        user["id"].as_str().expect("the owner id").to_string(),
    )
}

/// POSTs JSON as the owner and returns the 201 body.
async fn create(client: &reqwest::Client, url: String, body: Value) -> Value {
    let response = client.post(url).json(&body).send().await.expect("POST");
    json_body(response, 201).await
}

/// Spawns `quoro mcp stdio` with piped stdio.
fn spawn_relay(primary: &Primary, key_file: &Path, fingerprint: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args([
            "mcp",
            "stdio",
            "--url",
            &primary.base,
            "--fingerprint",
            fingerprint,
        ])
        .arg("--key-file")
        .arg(key_file)
        .args(["--justification", "end-to-end test of quoro mcp stdio"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro mcp stdio")
}

/// The relay's stdout. The relay forwards messages concurrently, so answers
/// may arrive in any order (JSON-RPC allows it); ones read while looking for
/// another id are kept for later.
struct Answers {
    lines: Lines<BufReader<ChildStdout>>,
    early: Vec<Value>,
}

impl Answers {
    /// The JSON-RPC response with `id`.
    async fn response(&mut self, id: i64) -> Value {
        if let Some(index) = self.early.iter().position(|m| m["id"] == id) {
            return self.early.remove(index);
        }
        timeout(WAIT, async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .expect("read the relay's stdout")
                    .expect("the relay closed stdout before answering");
                let message: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|e| panic!("stdout carries only JSON-RPC ({e}): {line}"));
                if message["id"] == id {
                    return message;
                }
                self.early.push(message);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no response for id {id}"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stdio_session_lists_tools_and_reads_through_the_primary() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = TempDir::new("mcp-stdio");
    let primary = start_primary(dir.path()).await;
    let (client, owner_id) = owner(&primary).await;
    let base = &primary.base;

    let agent = create(
        &client,
        format!("{base}/api/v1/agents"),
        json!({ "name": "local-client", "model": "test-model", "owner_user_id": owner_id }),
    )
    .await;
    let agent_id = agent["id"].as_str().expect("the agent id").to_string();
    let key = create(
        &client,
        format!("{base}/api/v1/agents/{agent_id}/keys"),
        json!({}),
    )
    .await;
    let key = key["key"].as_str().expect("the one-time key").to_string();
    create(
        &client,
        format!("{base}/api/v1/grants"),
        json!({
            "agent_id": agent_id,
            "scopes": ["cluster:read"],
            "selector": { "all": true },
            "ttl_s": 3600,
            "reason": "stdio test",
        }),
    )
    .await;
    let key_file = write_file(dir.path(), "agent.key", &format!("{key}\n"), 0o600);

    let mut relay = spawn_relay(&primary, &key_file, &primary.fingerprint);
    let mut stdin = relay.stdin.take().expect("stdin is piped");
    let mut stdout = Answers {
        lines: BufReader::new(relay.stdout.take().expect("stdout is piped")).lines(),
        early: Vec::new(),
    };
    let stderr = relay.stderr.take().expect("stderr is piped");

    let send = |message: Value| format!("{message}\n");
    stdin
        .write_all(
            send(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "stdio-test", "version": "0" },
                },
            }))
            .as_bytes(),
        )
        .await
        .expect("write initialize");
    let initialized = stdout.response(1).await;
    assert_eq!(
        initialized["result"]["serverInfo"]["name"], "quoro",
        "the primary's MCP server answered: {initialized}"
    );

    let rest = [
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "get_cluster", "arguments": {} },
        }),
    ];
    for message in rest {
        stdin
            .write_all(send(message).as_bytes())
            .await
            .expect("write a message");
    }

    let listed = stdout.response(2).await;
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("tools/list returned tools: {listed}"))
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    for expected in ["get_cluster", "get_app", "restart_app", "list_audit"] {
        assert!(names.contains(&expected), "{expected} is listed: {names:?}");
    }
    assert!(
        !names.iter().any(|n| n.contains("decide")),
        "no approval:decide tool: {names:?}"
    );

    let called = stdout.response(3).await;
    assert_ne!(
        called["result"]["isError"], true,
        "get_cluster succeeded: {called}"
    );
    let cluster = &called["result"]["structuredContent"];
    assert!(
        cluster["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("cls_")),
        "get_cluster returned the cluster: {called}"
    );

    // Closing stdin ends the session cleanly.
    drop(stdin);
    let status = timeout(WAIT, relay.wait())
        .await
        .expect("the relay exits")
        .expect("wait");
    assert!(status.success(), "the relay exits 0 at end of input");
    let mut stderr_text = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut BufReader::new(stderr), &mut stderr_text)
        .await
        .expect("read stderr");
    assert!(!stderr_text.contains(&key), "the agent key is never logged");

    // The primary audited the call as the agent's, on the MCP channel: the
    // relay sent it to `/mcp` and built nothing itself.
    let response = client
        .get(format!("{base}/api/v1/audit?actor_id={agent_id}"))
        .send()
        .await
        .expect("GET /api/v1/audit");
    let audit = json_body(response, 200).await;
    let records = audit["items"].as_array().expect("audit items");
    let call = records
        .iter()
        .find(|r| r["operation"] == "get_cluster")
        .unwrap_or_else(|| panic!("get_cluster is audited: {audit}"));
    assert_eq!(call["channel"], "mcp", "{call}");
    assert_eq!(call["result"], "ok", "{call}");
    assert_eq!(call["actor_id"], agent_id, "{call}");
    let mint = records
        .iter()
        .find(|r| r["operation"] == "POST /api/v1/auth/agent-token")
        .unwrap_or_else(|| panic!("the mint is audited: {audit}"));
    assert_eq!(
        mint["justification"], "end-to-end test of quoro mcp stdio",
        "the mint carries --justification: {mint}"
    );

    // A pin that does not match this primary fails before stdin is read.
    let wrong = format!("sha256:{}", "0".repeat(64));
    let output = spawn_relay(&primary, &key_file, &wrong)
        .wait_with_output()
        .await
        .expect("run the relay");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("fingerprint"),
        "the pin mismatch is named: {stderr}"
    );
    assert!(output.stdout.is_empty(), "nothing is written on stdout");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_shared_or_malformed_key_file_fails_clearly() {
    let dir = TempDir::new("mcp-key");
    let run = |key_file: PathBuf| async move {
        let output = Command::new(env!("CARGO_BIN_EXE_quoro"))
            .args([
                "mcp",
                "stdio",
                // Nothing listens here: the key file is checked first.
                "--url",
                "https://127.0.0.1:9",
                "--fingerprint",
                &format!("sha256:{}", "a".repeat(64)),
            ])
            .arg("--key-file")
            .arg(key_file)
            .stdin(Stdio::null())
            .output()
            .await
            .expect("run quoro mcp stdio");
        assert_eq!(output.status.code(), Some(1), "the relay exits 1");
        assert!(output.stdout.is_empty(), "nothing is written on stdout");
        String::from_utf8_lossy(&output.stderr).into_owned()
    };

    let missing = dir.path().join("absent.key");
    let stderr = run(missing.clone()).await;
    assert!(
        stderr.contains("does not exist") && stderr.contains(&missing.display().to_string()),
        "a missing file is named: {stderr}"
    );

    let secret = "akey_01JC9Z6Q5W8N3K4T7R2M1XHB0D";
    let shared = write_file(dir.path(), "shared.key", secret, 0o644);
    let stderr = run(shared).await;
    assert!(
        stderr.contains("chmod 600"),
        "a readable file is refused: {stderr}"
    );
    assert!(
        !stderr.contains(secret),
        "the key is never echoed: {stderr}"
    );

    let unreadable = write_file(dir.path(), "unreadable.key", secret, 0o000);
    // Root reads a mode-000 file anyway; the case only exists for other users.
    if std::fs::read(&unreadable).is_err() {
        let stderr = run(unreadable).await;
        assert!(
            stderr.contains("cannot read the agent key file"),
            "{stderr}"
        );
    }

    let malformed = write_file(dir.path(), "malformed.key", "not a key\n", 0o600);
    let stderr = run(malformed).await;
    assert!(stderr.contains("does not hold an agent key"), "{stderr}");

    // The key is never taken from the command line.
    let output = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args([
            "mcp",
            "stdio",
            "--url",
            "https://127.0.0.1:9",
            "--key",
            secret,
        ])
        .output()
        .await
        .expect("run quoro mcp stdio");
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unknown option is a usage error"
    );
}
