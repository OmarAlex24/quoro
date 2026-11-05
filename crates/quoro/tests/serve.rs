//! End-to-end test of the `quoro` binary (D-0054, issue #26).
//!
//! The test drives the real binary over real HTTPS: a temporary data
//! directory, `listen = "127.0.0.1:0"`, the port read from the start log, then
//! setup, login, the primary's own node agent, one project and one VM through
//! its whole lifecycle on the fake engine, and finally a second machine that
//! joins with a join token, runs `quoro node` and runs a VM of its own. Along
//! the way a webhook points at a receiver the test runs, so the binary's own
//! dispatcher and test delivery must reach it: the test process installs a
//! rustls crypto provider for its client, but the spawned binary has to
//! install its own. Nothing here is mocked, so a failure points at the
//! binary, the TLS material or the control plane's handlers rather than at a
//! harness.

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::mpsc;
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

/// Writes the configuration the server and `setup-token` both read. The node
/// agent keeps its own data directory beside the control plane's.
fn write_config(dir: &Path, node_name: &str) -> PathBuf {
    let path = dir.join("quoro.toml");
    let body = format!(
        "listen = \"127.0.0.1:0\"\ndata_dir = \"{}\"\nengine = \"fake\"\nnode_name = \"{node_name}\"\n\n[node]\ndata_dir = \"{}\"\n",
        dir.join("cp").display(),
        dir.join("node").display(),
    );
    std::fs::write(&path, body).expect("write quoro.toml");
    path
}

/// Polls the node list until `name` is `connected`, and returns that node.
async fn wait_for_node(client: &reqwest::Client, base: &str, name: &str) -> Value {
    let deadline = Instant::now() + LIFECYCLE_TIMEOUT;
    loop {
        let response = client
            .get(format!("{base}/api/v1/nodes"))
            .send()
            .await
            .expect("GET /api/v1/nodes");
        let nodes = json(response, 200).await;
        let found = nodes["items"]
            .as_array()
            .and_then(|items| items.iter().find(|node| node["name"] == name))
            .cloned();
        if let Some(node) = found
            && node["status"] == "connected"
        {
            return node;
        }
        assert!(
            Instant::now() < deadline,
            "node {name} did not connect: {nodes}"
        );
        sleep(Duration::from_millis(100)).await;
    }
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

/// One request a webhook receiver got: lowercased header names, and the body.
struct Received {
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Received {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A plain-HTTP webhook receiver on loopback. It answers every request with
/// 200 and hands it to the returned channel. Webhook URLs accept plain HTTP
/// only for `localhost`, so the caller builds the URL from the port.
async fn start_receiver() -> (u16, mpsc::UnboundedReceiver<Received>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the webhook receiver");
    let port = listener
        .local_addr()
        .expect("the receiver's address")
        .port();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Some(received) = read_request(stream).await {
                    let _ = tx.send(received);
                }
            });
        }
    });
    (port, rx)
}

/// Reads one HTTP/1.1 request with a `Content-Length` body and answers 200.
async fn read_request(mut stream: TcpStream) -> Option<Received> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let head_end = loop {
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let headers: Vec<(String, String)> = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let length = headers
        .iter()
        .find(|(key, _)| key == "content-length")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buffer.split_off(head_end + 4);
    while body.len() < length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .ok()?;
    let _ = stream.shutdown().await;
    Some(Received { headers, body })
}

#[tokio::test(flavor = "multi_thread")]
async fn the_binary_serves_the_whole_lifecycle_over_https() {
    // reqwest's no-provider TLS feature leaves the provider to the process;
    // `ring` is the one D-0080 allows.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("the ring provider installs once per process");

    let dir = TempDir::new("serve");
    let config = write_config(dir.path(), "test-node");

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
        .env("QUORO_TEST_NO_VULN_DOWNLOAD", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro serve");

    let stderr = server.stderr.take().expect("stderr is piped");
    let mut lines = BufReader::new(stderr).lines();
    // Kept so a server that dies before it listens explains itself.
    let mut log: Vec<String> = Vec::new();
    let (port, fingerprint) = timeout(Duration::from_secs(30), async {
        let mut port = None;
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Some(rest) = line.split("listening on https://").nth(1) {
                        let address = rest.split_whitespace().next().expect("the address");
                        port = Some(
                            address
                                .rsplit(':')
                                .next()
                                .expect("a port")
                                .parse::<u16>()
                                .expect("a numeric port"),
                        );
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
                Ok(None) => panic!("the server exited before it listened:\n{}", log.join("\n")),
                Err(e) => panic!("cannot read the server's stderr: {e}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the server logged no listening address and fingerprint within 30s:\n{}",
            log.join("\n")
        )
    });

    // Keep draining stderr so a chatty server never blocks on a full pipe,
    // and keep the lines: a background task that panics only says so there.
    let server_log = std::sync::Arc::new(std::sync::Mutex::new(log));
    let drained = tokio::spawn({
        let server_log = std::sync::Arc::clone(&server_log);
        async move {
            while let Ok(Some(line)) = lines.next_line().await {
                server_log.lock().expect("the log lock").push(line);
            }
        }
    });
    let server_log_text = || server_log.lock().expect("the log lock").join("\n");

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

    // ---- a webhook pointed at a receiver this test runs ------------------
    let (receiver_port, mut deliveries) = start_receiver().await;
    let response = client
        .post(format!("{base}/api/v1/webhooks"))
        .json(&json!({
            "name": "smoke receiver",
            "url": format!("http://localhost:{receiver_port}/hook"),
            "event_types": ["vm.started"],
            // An empty selector matches nothing (openapi.yaml, Selector).
            "selector": { "all": true },
        }))
        .send()
        .await
        .expect("POST /api/v1/webhooks");
    let webhook = json(response, 201).await;
    let webhook_id = webhook["id"].as_str().expect("the webhook id").to_string();

    // ---- the primary's own node agent enrolled over loopback -------------
    let node = wait_for_node(&client, &base, "test-node").await;
    assert_eq!(node["cp_status"], "primary", "the node: {node}");
    let node_id = node["id"].as_str().expect("the node id").to_string();
    assert!(
        !dir.path().join("cp/local-node-join.json").exists(),
        "the local join file is consumed by the enrollment"
    );

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

    // ---- the dispatcher delivers the vm.started event --------------------
    // The binary's own HTTP client sends it, so this fails if `quoro serve`
    // never installed a crypto provider for its rustls client.
    let Ok(delivered) = timeout(LIFECYCLE_TIMEOUT, deliveries.recv()).await else {
        let attempts = client
            .get(format!("{base}/api/v1/webhooks/{webhook_id}/deliveries"))
            .send()
            .await
            .expect("GET the webhook's deliveries");
        let attempts = json(attempts, 200).await;
        let events = client
            .get(format!("{base}/api/v1/events"))
            .send()
            .await
            .expect("GET /api/v1/events");
        let events = json(events, 200).await;
        panic!(
            "the dispatcher delivered no webhook within the timeout\n\
             deliveries: {attempts}\nevents: {events}\nserver log:\n{}",
            server_log_text()
        );
    };
    let delivered = delivered.expect("the receiver is still running");
    assert_eq!(
        delivered.header("quoro-event-type"),
        Some("vm.started"),
        "the delivered event type: {:?}",
        delivered.headers
    );
    assert!(
        delivered
            .header("quoro-signature")
            .is_some_and(|signature| signature.starts_with("t=") && signature.contains(",v1=")),
        "the delivery is signed: {:?}",
        delivered.headers
    );
    let envelope: Value =
        serde_json::from_slice(&delivered.body).expect("the delivery body is JSON");
    assert_eq!(
        envelope["data"]["vm_id"], vm_id,
        "the event names the VM: {envelope}"
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

    // ---- a test delivery goes out from the request handler --------------
    // It resends the newest event, now `vm.stopped`, which this webhook's
    // filter kept the dispatcher from delivering.
    let response = client
        .post(format!("{base}/api/v1/webhooks/{webhook_id}/test"))
        .send()
        .await
        .expect("POST the test delivery");
    assert_eq!(
        response.status().as_u16(),
        202,
        "the test delivery failed; server log:\n{}",
        server_log_text()
    );
    let attempt = json(response, 202).await;
    assert_eq!(
        attempt["status"], "delivered",
        "the test delivery reached the receiver: {attempt}"
    );
    let tested = timeout(LIFECYCLE_TIMEOUT, deliveries.recv())
        .await
        .expect("the receiver saw no test delivery")
        .expect("the receiver is still running");
    assert_eq!(
        tested.header("quoro-delivery-id"),
        attempt["id"].as_str(),
        "the receiver got the recorded test delivery: {:?}",
        tested.headers
    );

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

    // ---- a second machine joins with a join token -----------------------
    let response = client
        .post(format!("{base}/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .expect("POST /api/v1/nodes/join-tokens");
    let created = json(response, 201).await;
    let join_token = created["token"]
        .as_str()
        .expect("the join token")
        .to_string();
    assert!(join_token.starts_with("njt_"), "a join token: {created}");

    let second_dir = TempDir::new("node");
    let second = write_config(second_dir.path(), "second-node");
    let joined = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args([
            "join",
            &base,
            "--token",
            &join_token,
            "--fingerprint",
            &fingerprint,
        ])
        .arg("--config")
        .arg(&second)
        .output()
        .await
        .expect("run quoro join");
    assert!(
        joined.status.success(),
        "quoro join exits 0: {}",
        String::from_utf8_lossy(&joined.stderr)
    );
    assert!(
        String::from_utf8_lossy(&joined.stdout).contains("enrolled as nod_"),
        "join prints the node id: {}",
        String::from_utf8_lossy(&joined.stdout)
    );

    // The token is single use.
    let again = run_with_config(
        &[
            "join",
            &base,
            "--token",
            &join_token,
            "--fingerprint",
            &fingerprint,
        ],
        &write_config(TempDir::new("again").path(), "third-node"),
    )
    .await;
    assert!(!again.status.success(), "a used join token is refused");

    let mut agent = Command::new(env!("CARGO_BIN_EXE_quoro"))
        .arg("node")
        .arg("--config")
        .arg(&second)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn quoro node");

    let second_node = wait_for_node(&client, &base, "second-node").await;
    assert!(
        second_node["cp_status"].is_null(),
        "a joined node is not a primary: {second_node}"
    );
    let second_id = second_node["id"].as_str().expect("the node id").to_string();

    let response = client
        .post(format!("{base}/api/v1/vms"))
        .json(&json!({
            "project_id": project_id,
            "node_id": second_id,
            "name": "remote",
            "vcpus": 1,
            "memory_mb": 256,
            "disk_gb": 1,
            "image": "ghcr.io/quoro/hello:latest",
        }))
        .send()
        .await
        .expect("POST /api/v1/vms on the second node");
    let vm = json(response, 201).await;
    let remote_id = vm["id"].as_str().expect("the VM id").to_string();
    assert_eq!(vm["node_id"], second_id, "placed on the second node: {vm}");
    wait_for_observed(&client, &base, &remote_id, "running").await;

    agent.kill().await.expect("stop the node agent");
    let _ = agent.wait().await;
    server.kill().await.expect("stop the server");
    let _ = server.wait().await;
    drained.await.expect("the stderr drain task");
    let log = server_log_text();
    assert!(!log.contains("panicked"), "the server panicked:\n{log}");
}
