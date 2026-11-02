//! End-to-end test of the `quoro` binary (D-0054, issue #26).
//!
//! The test drives the real binary over real HTTPS: a temporary data
//! directory, `listen = "127.0.0.1:0"`, the port read from the start log, then
//! setup, login, one node, one project and one VM through its whole lifecycle
//! on the fake engine. Nothing here is mocked, so a failure points at the
//! binary, the TLS material or the control plane's handlers rather than at a
//! harness.

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::Command;
use tokio::time::{Instant, sleep, timeout};

const EMAIL: &str = "owner@example.test";
const NAME: &str = "Test Owner";
/// Twelve characters, the minimum the setup request accepts.
const PASSWORD: &str = "correct horse battery staple";
/// How long the fake engine may take to report `running` or `stopped`.
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Writes the configuration the server and `setup-token` both read.
fn write_config(dir: &Path) -> PathBuf {
    let path = dir.join("quoro.toml");
    let body = format!(
        "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \"test-node\"\n",
        dir.display()
    );
    std::fs::write(&path, body).expect("write quoro.toml");
    path
}

/// Runs the binary with `args` and `--config <path>`.
async fn run_with_config(args: &[&str], config: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args(args)
        .arg("--config")
        .arg(config)
        .output()
        .await
        .expect("run the quoro binary")
}

/// Asserts the response status and returns the parsed JSON body.
async fn json(response: reqwest::Response, expected: u16) -> Value {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status.as_u16(),
        expected,
        "expected {expected}, got {status}: {body}"
    );
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"))
}

/// One VM by id.
async fn get_vm(client: &reqwest::Client, base: &str, vm_id: &str) -> Value {
    let response = client
        .get(format!("{base}/api/v1/vms/{vm_id}"))
        .send()
        .await
        .expect("GET one VM");
    json(response, 200).await
}

/// Polls one VM until it reports `want`, which is what the fake engine does a
/// few hundred milliseconds after the node reconciles the change.
async fn wait_for_observed(client: &reqwest::Client, base: &str, vm_id: &str, want: &str) -> Value {
    let deadline = Instant::now() + LIFECYCLE_TIMEOUT;
    loop {
        let vm = get_vm(client, base, vm_id).await;
        if vm["observed_status"] == want {
            return vm;
        }
        assert!(
            Instant::now() < deadline,
            "the VM never reached {want}; last record: {vm}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_binary_serves_the_whole_lifecycle_over_https() {
    // reqwest's no-provider TLS feature leaves the provider to the process;
    // `ring` is the one D-0080 allows.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("the ring provider installs once per process");

    let dir = TempDir::new("serve");
    let config = write_config(dir.path());

    // `quoro version` needs no server and no configuration.
    let version = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("version")
        .output()
        .await
        .expect("run quoro version");
    assert!(version.status.success(), "quoro version must exit 0");
    let version = String::from_utf8_lossy(&version.stdout);
    assert!(
        version.trim().starts_with("quoro "),
        "version prints the crate version: {version}"
    );

    let mut server = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");

    let stderr = server.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    // Kept so a server that dies before it listens explains itself.
    let mut log: Vec<String> = Vec::new();
    let port = timeout(Duration::from_secs(30), async {
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Some(rest) = line.split("listening on https://").nth(1) {
                        let address = rest.split_whitespace().next().expect("the address");
                        return address
                            .rsplit(':')
                            .next()
                            .expect("a port")
                            .parse::<u16>()
                            .expect("a numeric port");
                    }
                    log.push(line);
                }
                Ok(None) => panic!("the server exited before it listened:\n{}", log.join("\n")),
                Err(e) => panic!("cannot read the server's stderr: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the server logged no listening address within 30s:\n{}",
            log.join("\n")
        )
    });

    // Keep draining stderr so a chatty server never blocks on a full pipe.
    tokio::spawn(async move { while matches!(lines.next_line().await, Ok(Some(_))) {} });

    let base = format!("https://127.0.0.1:{port}");

    // ---- setup token -----------------------------------------------------
    let issued = run_with_config(&["setup-token"], &config).await;
    assert!(
        issued.status.success(),
        "setup-token exits 0: {}",
        String::from_utf8_lossy(&issued.stderr)
    );
    let token = String::from_utf8_lossy(&issued.stdout).trim().to_string();
    assert!(token.starts_with("qst_"), "a token was printed: {token}");

    // The session cookie the panel and the API share; the certificate is
    // self-signed, so the test pins nothing and accepts it instead.
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build the HTTPS client");

    // ---- setup and login -------------------------------------------------
    let response = client
        .post(format!("{base}/api/v1/setup"))
        .header("Quoro-Setup-Token", &token)
        .json(&json!({ "email": EMAIL, "name": NAME, "password": PASSWORD }))
        .send()
        .await
        .expect("POST /api/v1/setup");
    let owner = json(response, 201).await;
    assert_eq!(owner["role"], "owner", "setup creates the owner: {owner}");

    // A token cannot be issued once an owner exists: it could never be used.
    let second = run_with_config(&["setup-token"], &config).await;
    assert_eq!(
        second.status.code(),
        Some(1),
        "a second setup-token is refused"
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("an owner already exists"),
        "the refusal says why: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    let response = client
        .post(format!("{base}/api/v1/auth/login"))
        .json(&json!({ "email": EMAIL, "password": PASSWORD }))
        .send()
        .await
        .expect("POST /api/v1/auth/login");
    let me = json(response, 200).await;
    assert_eq!(me["type"], "user", "login answers the identity: {me}");

    // ---- the primary node registered itself ------------------------------
    let response = client
        .get(format!("{base}/api/v1/nodes"))
        .send()
        .await
        .expect("GET /api/v1/nodes");
    let nodes = json(response, 200).await;
    let node = &nodes["items"][0];
    assert_eq!(
        nodes["items"].as_array().map(Vec::len),
        Some(1),
        "the primary is the only node in M1: {nodes}"
    );
    assert_eq!(node["cp_status"], "primary", "the node: {node}");
    assert_eq!(node["status"], "connected", "the node: {node}");
    let node_id = node["id"].as_str().expect("the node id").to_string();

    // ---- a project and a VM on that node --------------------------------
    let response = client
        .post(format!("{base}/api/v1/projects"))
        .json(&json!({ "slug": "smoke", "name": "Smoke" }))
        .send()
        .await
        .expect("POST /api/v1/projects");
    let project = json(response, 201).await;
    assert_eq!(project["slug"], "smoke", "the project: {project}");
    let project_id = project["id"].as_str().expect("the project id").to_string();

    let response = client
        .post(format!("{base}/api/v1/vms"))
        .json(&json!({
            "project_id": project_id,
            "node_id": node_id,
            "name": "smoke",
            "vcpus": 1,
            "memory_mb": 256,
            "disk_gb": 1,
            "image": "ghcr.io/quoro/hello:latest",
        }))
        .send()
        .await
        .expect("POST /api/v1/vms");
    let vm = json(response, 201).await;
    let vm_id = vm["id"].as_str().expect("the VM id").to_string();
    assert!(vm_id.starts_with("vm_"), "a vm_ id: {vm_id}");
    assert_eq!(vm["desired_status"], "running", "the VM: {vm}");
    assert_eq!(vm["node_id"], node_id, "placed on the primary: {vm}");

    // ---- the fake engine brings it up ------------------------------------
    let running = wait_for_observed(&client, &base, &vm_id, "running").await;
    assert!(
        running["ip"]
            .as_str()
            .is_some_and(|ip| ip.starts_with("10.42.0.")),
        "the node reports the address it handed out: {running}"
    );

    // ---- stop, start, delete ---------------------------------------------
    let response = client
        .post(format!("{base}/api/v1/vms/{vm_id}/stop"))
        .send()
        .await
        .expect("POST stop");
    let vm = json(response, 202).await;
    assert_eq!(vm["desired_status"], "stopped", "the VM: {vm}");
    wait_for_observed(&client, &base, &vm_id, "stopped").await;

    let response = client
        .post(format!("{base}/api/v1/vms/{vm_id}/start"))
        .send()
        .await
        .expect("POST start");
    let vm = json(response, 202).await;
    assert_eq!(vm["desired_status"], "running", "the VM: {vm}");
    wait_for_observed(&client, &base, &vm_id, "running").await;

    let response = client
        .delete(format!("{base}/api/v1/vms/{vm_id}"))
        .send()
        .await
        .expect("DELETE the VM");
    let vm = json(response, 202).await;
    assert_eq!(vm["id"], vm_id, "the deleted record: {vm}");

    // The row goes away and the driver removes the VM from the engine.
    let deadline = Instant::now() + LIFECYCLE_TIMEOUT;
    loop {
        let response = client
            .get(format!("{base}/api/v1/vms"))
            .send()
            .await
            .expect("GET /api/v1/vms");
        let list = json(response, 200).await;
        let empty = list["items"].as_array().is_some_and(Vec::is_empty);
        if empty {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the deleted VM is still listed: {list}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    server.kill().await.expect("stop the server");
    let _ = server.wait().await;
}
