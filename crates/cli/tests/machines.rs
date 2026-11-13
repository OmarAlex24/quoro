//! `vm`, `ssh`, `incident` and `audit`, end to end against an in-process
//! primary.
//!
//! No engine runs: a created VM stays `creating`, and start, stop and delete
//! only set what the primary wants. `ssh` and `vm console` reach a fake node:
//! the test enrolls a node on the primary's fixture listener, holds its
//! session open with heartbeats and answers the primary's `stream_open` the
//! way a node agent relays a guest (node-agent-protocol.md), so the ticket,
//! the WebSocket upgrade over the pinned TLS listener and the framing are
//! real. Incidents are written with SQL, as a detection would open them.

mod common;

use base64::Engine as _;
use common::{Output, Primary, key_file, quoro, start, write_credentials};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_cli::{Environment, exit};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity, Kvm, NodeMessage};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

const IMAGE: &str =
    "ghcr.io/example/box@sha256:1111111111111111111111111111111111111111111111111111111111111111";

/// A primary with the project `demo`, its app `web` and the local node
/// `test-node`, a human's context `lab` with the owner's token, and the
/// folder `web/` linked to the app.
struct Lab {
    primary: Primary,
    owner_id: String,
    owner_token: String,
    app_id: String,
    home: PathBuf,
    web: PathBuf,
}

async fn lab() -> Lab {
    let primary = start().await;
    let (owner_id, owner_token) = primary.owner_token().await;
    let app_id = primary.app(&owner_token).await;
    let home = primary.config_home("home");
    write_credentials(&home, &owner_token, 0o600);
    let lab = Lab {
        web: primary.dir.join("web"),
        primary,
        owner_id,
        owner_token,
        app_id,
        home,
    };
    std::fs::create_dir_all(&lab.web).unwrap();
    lab.ok(&["link", "demo", "web"]).await;
    lab
}

impl Lab {
    fn environment(&self) -> Environment {
        self.primary
            .context_environment(&self.home)
            .with_cwd(&self.web)
    }

    /// The human's environment on a terminal that types `keys`.
    fn terminal(&self, keys: &[u8]) -> Environment {
        Environment::new([("XDG_CONFIG_HOME", self.home.display().to_string())], true)
            .with_cwd(&self.web)
            .with_stdin(keys.to_vec(), true)
    }

    async fn run(&self, args: &[&str]) -> Output {
        quoro(args, &self.environment()).await
    }

    async fn ok(&self, args: &[&str]) -> Output {
        let output = self.run(args).await;
        assert_eq!(output.code, exit::OK, "{args:?}: {}", output.stderr);
        output
    }

    async fn json(&self, args: &[&str]) -> Value {
        let output = self.ok(&[args, &["--json"]].concat()).await;
        serde_json::from_str(&output.stdout)
            .unwrap_or_else(|e| panic!("{args:?}: {e}: {}", output.stdout))
    }

    async fn get(&self, path: &str) -> Value {
        let (status, body) = self
            .primary
            .fixture(reqwest::Method::GET, path, &self.owner_token, None)
            .await;
        assert_eq!(status, 200, "GET {path}: {body}");
        body
    }

    /// The status of a GET as the owner.
    async fn status(&self, path: &str) -> u16 {
        self.primary
            .fixture(reqwest::Method::GET, path, &self.owner_token, None)
            .await
            .0
    }

    /// An agent with `scopes` (and the reads its link needs) on the project
    /// `demo`, logged in to its own context `lab` and linked to `web`;
    /// returns its id and environment.
    async fn agent(&self, name: &str, scopes: &[&str]) -> (String, Environment) {
        let (agent_id, key) = self
            .primary
            .agent(&self.owner_token, &self.owner_id, name)
            .await;
        let mut scopes = scopes.to_vec();
        for read in ["project:read", "app:read"] {
            if !scopes.contains(&read) {
                scopes.push(read);
            }
        }
        self.primary
            .grant(
                &self.owner_token,
                &agent_id,
                &scopes,
                json!({ "projects": ["demo"] }),
            )
            .await;
        let agent_home = self.primary.dir.join(format!("{name}-home"));
        let key_path = key_file(&agent_home.join("keys"), "agent.key", &key, 0o600);
        let cwd = self.primary.dir.join(format!("{name}-work"));
        std::fs::create_dir_all(&cwd).unwrap();
        let environment = Environment::new(
            [
                (
                    "XDG_CONFIG_HOME",
                    agent_home.join("config").display().to_string(),
                ),
                (
                    "XDG_RUNTIME_DIR",
                    agent_home.join("run").display().to_string(),
                ),
            ],
            true,
        )
        .with_cwd(&cwd)
        .with_stdin(Vec::new(), true);
        let key_path = key_path.display().to_string();
        let login = quoro(
            &[
                "--context",
                "lab",
                "login",
                "--agent",
                "--key-file",
                &key_path,
                "--url",
                &self.primary.url,
                "--fingerprint",
                &self.primary.fingerprint,
            ],
            &environment,
        )
        .await;
        assert_eq!(login.code, exit::OK, "{}", login.stderr);
        let link = quoro(&["link", "demo", "web"], &environment).await;
        assert_eq!(link.code, exit::OK, "{}", link.stderr);
        (agent_id, environment)
    }

    /// A VM row of the app `web` (or a standalone one) on `node_id`, as the
    /// primary records an instance it placed; returns its id.
    async fn vm_row(&self, name: &str, app: bool, node_id: &str, observed: &str) -> String {
        let project = self.get("/api/v1/projects").await["items"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let id = format!("vm_{}", ulid_like(name));
        sqlx::query(
            "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
             image, desired_status, observed_status, ip, restart_generation, restart_count, \
             last_exit_code, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 1, 256, 1, ?, \
             'running', ?, NULL, 0, 0, NULL, '2026-11-07T10:00:00.000Z', \
             '2026-11-07T10:00:00.000Z')",
        )
        .bind(&id)
        .bind(&project)
        .bind(app.then_some(self.app_id.as_str()))
        .bind(node_id)
        .bind(name)
        .bind(IMAGE)
        .bind(observed)
        .execute(self.primary.state.store.pool())
        .await
        .unwrap();
        id
    }
}

/// 26 characters of Crockford base32 made from `seed`, for a fixture id.
fn ulid_like(seed: &str) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let mut value: u128 = seed.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u128::from(b)).wrapping_mul(0x100_0000_01b3)
    });
    (0..26)
        .map(|_| {
            let c = ALPHABET[(value % 32) as usize] as char;
            value /= 32;
            c
        })
        .collect()
}

/// Approves the newest pending approval other than `except`, as the owner.
async fn approve(lab: &Lab, except: &str) -> String {
    let pool = lab.primary.state.store.pool();
    for _ in 0..200 {
        let pending: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM approvals WHERE status = 'pending' AND id <> ? \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(except)
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some((id,)) = pending {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let (status, body) = lab
                .primary
                .fixture(
                    reqwest::Method::POST,
                    &format!("/api/v1/approvals/{id}/approve"),
                    &lab.owner_token,
                    Some(json!({ "note": "approved by the test" })),
                )
                .await;
            assert_eq!(status, 200, "approve: {body}");
            return id;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no pending approval");
}

// ---------------------------------------------------------------------------
// A fake node: enrolled on the fixture listener, its session held open with
// heartbeats; the test reads the primary's frames and answers them.
// ---------------------------------------------------------------------------

static MESSAGES: AtomicU64 = AtomicU64::new(1);

fn msg_id() -> String {
    let n = MESSAGES.fetch_add(1, Ordering::SeqCst);
    wire::msg_id((u128::from(n) << 64 | 0x5eed).to_be_bytes())
}

fn frame(kind: &str, body: Value) -> Value {
    json!({ "v": 1, "type": kind, "id": msg_id(), "body": body })
}

fn now() -> String {
    quoro_control_plane::store::now_rfc3339()
}

struct Node {
    id: String,
    out: mpsc::UnboundedSender<Value>,
    incoming: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Node {
    async fn enroll(lab: &Lab, name: &str) -> Self {
        // The registered local node is a voter without a session: record its
        // acknowledgement, so this second voter passes rule 2 of
        // node-agent-protocol.md section 8.10.
        lab.primary.state.hub.ack_voter_set().await;
        let (status, token) = lab
            .primary
            .fixture(
                reqwest::Method::POST,
                "/api/v1/nodes/join-tokens",
                &lab.owner_token,
                Some(json!({})),
            )
            .await;
        assert_eq!(status, 201, "join token: {token}");
        let key = sign::signing_key_from_seed(&[5u8; 32]);
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/nodes/enroll", lab.primary.plain_url()))
            .json(&json!({
                "join_token": token["token"],
                "name": name,
                "public_key": sign::public_key_to_base64(&key.verifying_key()),
                "roles": ["node"],
                "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
                "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        let enrolled: Value = response.json().await.unwrap();
        let id = enrolled["node_id"].as_str().unwrap().to_string();
        let credential = enrolled["credential"].as_str().unwrap().to_string();

        let address = lab
            .primary
            .plain_url()
            .trim_start_matches("http://")
            .to_string();
        let mut request = format!("ws://{address}/api/v1/nodes/session")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {credential}").parse().unwrap(),
        );
        let stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        let (mut socket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .expect("the node session upgrade");
        let hello = NodeMessage::Hello(wire::Hello {
            node_id: id.clone(),
            agent_version: "test-agent".to_string(),
            engine_version: "test-engine".to_string(),
            boot_id: "boot-1".to_string(),
            stored_rev: 0,
            applied_rev: 0,
            log_seq: 0,
            log_hash: "0".repeat(64),
            leases: Vec::new(),
            capacity: Capacity {
                cpus: 8,
                memory_mb: 16_384,
                disk_gb: 100,
            },
            allocated: Capacity::default(),
            kvm: Kvm {
                available: true,
                kernel: "6.9.0".to_string(),
                nested: Some(true),
            },
            dropped_events: 0,
            mesh_public_key: None,
            volume_snapshot: None,
            custody_public_key: None,
            mesh_rules: None,
            control_plane_urls: Vec::new(),
        });
        socket
            .send(Message::Text(
                hello.to_frame(&msg_id(), None).to_text().into(),
            ))
            .await
            .unwrap();
        let (out, mut out_rx) = mpsc::unbounded_channel::<Value>();
        let (in_tx, incoming) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut beat = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    message = socket.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(value) = serde_json::from_str::<Value>(text.as_str())
                                && in_tx.send(value).is_err()
                            {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                        _ => {}
                    },
                    value = out_rx.recv() => match value {
                        Some(value) => {
                            if socket.send(Message::Text(value.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                    _ = beat.tick() => {
                        let beat = frame("heartbeat", json!({ "time": now() }));
                        if socket.send(Message::Text(beat.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let mut node = Self {
            id,
            out,
            incoming,
            task,
        };
        node.next("welcome").await;
        node
    }

    fn send(&self, value: Value) {
        let _ = self.out.send(value);
    }

    /// The next frame of `kind` from the primary.
    async fn next(&mut self, kind: &str) -> Value {
        loop {
            let value = tokio::time::timeout(Duration::from_secs(10), self.incoming.recv())
                .await
                .unwrap_or_else(|_| panic!("a {kind} frame within 10 s"))
                .expect("the node session is open");
            if value["type"] == kind {
                return value;
            }
        }
    }

    /// Guest output on `stream_id`.
    fn data(&self, stream_id: &str, seq: u64, bytes: &[u8]) {
        self.send(frame(
            "stream_data",
            json!({
                "stream_id": stream_id,
                "seq": seq,
                "data": base64::engine::general_purpose::STANDARD.encode(bytes),
            }),
        ));
    }
}

/// The bytes of a `stream_input` frame.
fn input_of(frame: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(frame["body"]["data"].as_str().unwrap_or_default())
        .unwrap()
}

/// The `session.end` params of the newest session, once written.
async fn session_end(lab: &Lab) -> Value {
    for _ in 0..200 {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT params FROM audit_log WHERE operation = 'session.end' ORDER BY seq DESC LIMIT 1",
        )
        .fetch_optional(lab.primary.state.store.pool())
        .await
        .unwrap();
        if let Some((params,)) = row {
            return serde_json::from_str(&params).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no session.end");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vms_are_created_listed_shown_started_and_stopped() {
    let lab = lab().await;
    let output = lab.ok(&["vm", "list"]).await;
    assert!(
        output.stdout.starts_with("The project Demo has no VMs"),
        "{}",
        output.stdout
    );

    let create = [
        "vm",
        "create",
        "box",
        "--image",
        IMAGE,
        "--node",
        "test-node",
        "--vcpus",
        "2",
        "--memory",
        "512",
    ];
    let output = lab.ok(&create).await;
    assert!(
        output.stdout.starts_with("Created the VM box (vm_"),
        "{}",
        output.stdout
    );
    let vm = lab.json(&["vm", "show", "box"]).await;
    let id = vm["id"].as_str().unwrap().to_string();
    assert_eq!(
        (&vm["vcpus"], &vm["memory_mb"], &vm["disk_gb"], &vm["image"]),
        (&json!(2), &json!(512), &json!(1), &json!(IMAGE))
    );
    let output = lab.ok(&["vm", "list"]).await;
    let header = output.stdout.lines().next().unwrap();
    assert!(header.starts_with("NAME"), "{}", output.stdout);
    assert!(output.stdout.contains(&id), "{}", output.stdout);
    let output = lab.ok(&["vm", "show", &id]).await;
    assert!(output.stdout.contains("Name:     box"), "{}", output.stdout);
    assert!(
        output
            .stdout
            .contains("2 vCPUs, 512 MiB memory, 1 GiB disk"),
        "{}",
        output.stdout
    );

    let output = lab.ok(&["vm", "stop", "box"]).await;
    assert!(
        output.stdout.starts_with("Stopping the VM box"),
        "{}",
        output.stdout
    );
    assert_eq!(
        lab.get(&format!("/api/v1/vms/{id}")).await["desired_status"],
        "stopped"
    );
    let output = lab.ok(&["vm", "start", "box"]).await;
    assert!(
        output.stdout.starts_with("Starting the VM box"),
        "{}",
        output.stdout
    );
    assert_eq!(
        lab.get(&format!("/api/v1/vms/{id}")).await["desired_status"],
        "running"
    );

    // --disk, a node by id, and a name two VMs share.
    let node_id = vm["node_id"].as_str().unwrap();
    let second = lab
        .json(
            &[
                &create[..5],
                &[
                    "--node", node_id, "--vcpus", "1", "--memory", "256", "--disk", "3",
                ],
            ]
            .concat(),
        )
        .await;
    assert_eq!(second["disk_gb"], 3);
    let output = lab.run(&["vm", "show", "box"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("matches 2 VMs"), "{}", output.stderr);
    let output = lab.run(&["vm", "show", "nothing"]).await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
    let output = lab
        .run(&[
            "vm",
            "create",
            "x",
            "--image",
            IMAGE,
            "--node",
            "no-such-node",
            "--vcpus",
            "1",
            "--memory",
            "1",
        ])
        .await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vm_is_deleted_by_a_human_and_by_an_agent_with_an_approval() {
    let lab = lab().await;
    let node = lab.get("/api/v1/nodes").await["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let first = lab.vm_row("old", false, &node, "running").await;
    let second = lab.vm_row("older", false, &node, "running").await;

    // Without a terminal and without --yes nothing is sent.
    let requests = lab.primary.requests();
    let output = lab.run(&["vm", "delete", "old"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("--yes"), "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests);
    // On a terminal the name is typed; a wrong one deletes nothing.
    let output = quoro(&["vm", "delete", "old"], &lab.terminal(b"olden\n")).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("nothing was deleted"),
        "{}",
        output.stderr
    );
    assert_eq!(
        lab.get(&format!("/api/v1/vms/{first}")).await["desired_status"],
        "running"
    );
    let output = quoro(&["vm", "delete", "old"], &lab.terminal(b"old\n")).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output.stdout.starts_with("Deleted the VM old"),
        "{}",
        output.stdout
    );
    assert_eq!(
        lab.status(&format!("/api/v1/vms/{first}")).await,
        404,
        "the VM record is gone"
    );

    // An agent: a reason first, then exit 3, then an approval.
    let (_, agent) = lab
        .agent(
            "operator",
            &[
                "project:read",
                "vm:read",
                "approval:create",
                "approval:read",
                "event:read",
            ],
        )
        .await;
    let requests = lab.primary.requests();
    let output = quoro(&["vm", "delete", &second], &agent).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("--reason"), "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    let delete = ["vm", "delete", second.as_str(), "--reason", "retire it"];
    let output = quoro(&delete, &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("scope vm:delete on vm {second}")),
        "{}",
        output.stderr
    );
    let output = quoro(&[&delete[..], &["--request-approval"]].concat(), &agent).await;
    assert_eq!(output.code, exit::APPROVAL_REQUIRED, "{}", output.stderr);
    let filed = output.stdout.trim().to_string();
    assert!(filed.starts_with("apr_"), "{}", output.stdout);
    let wait = [&delete[..], &["--request-approval", "--wait"]].concat();
    let (output, approved) = tokio::join!(quoro(&wait, &agent), approve(&lab, &filed));
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output
            .stderr
            .contains(&format!("approval {approved} approved")),
        "{}",
        output.stderr
    );
    assert!(
        output.stdout.starts_with("Deleted the VM older"),
        "{}",
        output.stdout
    );
    assert_eq!(
        lab.status(&format!("/api/v1/vms/{second}")).await,
        404,
        "the VM record is gone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_is_refused_before_anything_is_sent() {
    let lab = lab().await;
    let (_, agent) = lab
        .agent(
            "watcher",
            &["project:read", "vm:read", "vm:console", "incident:read"],
        )
        .await;
    let vm = format!("vm_{}", "0".repeat(26));
    let incident = format!("inc_{}", "0".repeat(26));
    let requests = lab.primary.requests();
    let mints = lab.primary.mints().await;
    for args in [
        &[
            "vm",
            "create",
            "box",
            "--image",
            IMAGE,
            "--node",
            "test-node",
            "--vcpus",
            "1",
            "--memory",
            "256",
        ][..],
        &["vm", "start", &vm],
        &["vm", "stop", &vm],
        &["vm", "delete", &vm],
        &["vm", "console", &vm],
        &["incident", "resolve", &incident],
    ] {
        let output = quoro(args, &agent).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
        assert!(
            output.stderr.contains("--reason"),
            "{args:?}: {}",
            output.stderr
        );
    }
    // A shell is human-only: refused even with a reason.
    let output = quoro(&["ssh", "--reason", "look around"], &agent).await;
    assert_eq!(output.code, exit::FORBIDDEN, "{}", output.stderr);
    assert!(output.stderr.contains("human_only"), "{}", output.stderr);
    // A user by email needs a human-only read.
    let output = quoro(&["audit", "list", "--actor", "owner@example.com"], &agent).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(output.stderr.contains("usr_"), "{}", output.stderr);
    assert_eq!(lab.primary.requests(), requests, "nothing was sent");
    assert_eq!(lab.primary.mints().await, mints, "not even a mint");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ssh_runs_a_shell_over_the_session_websocket() {
    let lab = lab().await;
    let mut node = Node::enroll(&lab, "node-shell").await;
    let vm = lab.vm_row("web", true, &node.id, "running").await;

    // Without a terminal nothing is sent.
    let requests = lab.primary.requests();
    let output = lab.run(&["ssh"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);
    assert!(
        output.stderr.contains("needs a terminal"),
        "{}",
        output.stderr
    );
    assert_eq!(lab.primary.requests(), requests);

    // The shell: output, typed input, the program's exit.
    let environment = lab.terminal(b"echo hi\n");
    let shell = quoro(&["ssh"], &environment);
    let node_side = async {
        let open = node.next("stream_open").await;
        assert_eq!(open["body"]["kind"], "terminal");
        assert_eq!(open["body"]["params"]["vm_id"], vm);
        assert_eq!(open["body"]["params"]["rows"], 24);
        assert_eq!(open["body"]["params"]["cols"], 80);
        let stream = open["body"]["stream_id"].as_str().unwrap().to_string();
        node.data(&stream, 1, b"$ ");
        let input = node.next("stream_input").await;
        assert_eq!(input_of(&input), b"echo hi\n");
        node.data(&stream, 2, b"hi\n");
        node.send(frame(
            "stream_end",
            json!({ "stream_id": stream, "reason": "completed", "exit_code": 0 }),
        ));
    };
    let (output, ()) = tokio::join!(shell, node_side);
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert_eq!(output.stdout, "$ hi\n");
    assert!(
        output.stderr.contains("a shell in web"),
        "{}",
        output.stderr
    );
    let end = session_end(&lab).await;
    assert_eq!(
        (&end["kind"], &end["close_reason"]),
        (&json!("terminal"), &json!("exited"))
    );

    // Ctrl-] closes the session from this side; a non-zero exit is exit 1.
    let detach = lab.terminal(b"ls\x1d");
    let shell = quoro(&["ssh"], &detach);
    let node_side = async {
        let open = node.next("stream_open").await;
        let stream = open["body"]["stream_id"].as_str().unwrap().to_string();
        node.data(&stream, 1, b"$ ");
        let input = node.next("stream_input").await;
        assert_eq!(input_of(&input), b"ls");
        node.next("stream_close").await;
    };
    let (output, ()) = tokio::join!(shell, node_side);
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    assert!(
        output.stderr.contains("closed the session"),
        "{}",
        output.stderr
    );
    assert_eq!(
        session_end_reason(&lab, "ended_by_client").await,
        "ended_by_client"
    );

    let silent = lab.terminal(b"");
    let shell = quoro(&["ssh"], &silent);
    let node_side = async {
        let open = node.next("stream_open").await;
        let stream = open["body"]["stream_id"].as_str().unwrap().to_string();
        node.data(&stream, 1, b"bye\n");
        node.send(frame(
            "stream_end",
            json!({ "stream_id": stream, "reason": "completed", "exit_code": 3 }),
        ));
    };
    let (output, ()) = tokio::join!(shell, node_side);
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert!(
        output.stderr.contains("exited with code 3"),
        "{}",
        output.stderr
    );
}

/// Waits for a `session.end` whose `close_reason` is `reason`.
async fn session_end_reason(lab: &Lab, reason: &str) -> String {
    for _ in 0..200 {
        let end = session_end(lab).await;
        if end["close_reason"] == reason {
            return reason.to_string();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no session.end with {reason}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vm_console_follows_the_serial_console_and_sends_nothing() {
    let lab = lab().await;
    let mut node = Node::enroll(&lab, "node-console").await;
    let vm = lab.vm_row("boxed", false, &node.id, "running").await;
    let (_, agent) = lab
        .agent("reader", &["project:read", "vm:read", "vm:console"])
        .await;

    let console = quoro(
        &["vm", "console", "boxed", "--reason", "read the boot log"],
        &agent,
    );
    let node_side = async {
        let open = node.next("stream_open").await;
        assert_eq!(open["body"]["kind"], "console");
        assert_eq!(open["body"]["params"]["vm_id"], vm);
        let stream = open["body"]["stream_id"].as_str().unwrap().to_string();
        node.data(&stream, 1, b"[    0.000000] Linux version 6.9\n");
        // The session is attached once the primary acks the replay.
        node.next("stream_ack").await;
        node.data(&stream, 2, b"quoro-init: ready\n");
        node.next("stream_ack").await;
        // Someone ends it: the client reports why, having sent nothing.
        let session: (String,) =
            sqlx::query_as("SELECT id FROM interactive_sessions WHERE ended_at IS NULL LIMIT 1")
                .fetch_one(lab.primary.state.store.pool())
                .await
                .unwrap();
        let (status, body) = lab
            .primary
            .fixture(
                reqwest::Method::DELETE,
                &format!("/api/v1/interactive-sessions/{}", session.0),
                &lab.owner_token,
                None,
            )
            .await;
        assert_eq!(status, 204, "{body}");
    };
    let (output, ()) = tokio::join!(console, node_side);
    assert_eq!(output.code, exit::OTHER, "{}", output.stderr);
    assert_eq!(
        output.stdout,
        "[    0.000000] Linux version 6.9\nquoro-init: ready\n"
    );
    assert!(
        output.stderr.contains("ended_by_request"),
        "{}",
        output.stderr
    );
    let end = session_end(&lab).await;
    assert_eq!(end["kind"], "console");
    assert_eq!(end["bytes_in"], 0, "a console sends nothing: {end}");
    let open: (Option<String>, String) = sqlx::query_as(
        "SELECT justification, actor_type FROM audit_log \
         WHERE operation = 'POST /api/v1/vms/{vm_id}/console-sessions' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(lab.primary.state.store.pool())
    .await
    .unwrap();
    assert_eq!(
        open,
        (Some("read the boot log".to_string()), "agent".to_string())
    );

    // A node without a session is unreachable (exit 7) before any WebSocket.
    drop(node);
    let mut unreachable = 0;
    for _ in 0..40 {
        let output = lab.run(&["vm", "console", &vm]).await;
        if output.code == exit::UNREACHABLE {
            unreachable += 1;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        unreachable, 1,
        "the console of a disconnected node is exit 7"
    );
}

/// Writes an open incident on the app `web` and the event that opened it.
async fn incident(lab: &Lab, n: u32, title: &str) -> String {
    let pool = lab.primary.state.store.pool();
    let project: (String,) = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(&lab.app_id)
        .fetch_one(pool)
        .await
        .unwrap();
    let event = format!("evt_{}", ulid_like(&format!("event-{n}")));
    let id = format!("inc_{}", ulid_like(&format!("incident-{n}")));
    let subject = json!({ "type": "app", "id": lab.app_id }).to_string();
    sqlx::query(
        "INSERT INTO events (id, seq, type, time, source, subject, project_id, incident_id, \
         request_id, actor, data) VALUES (?, (SELECT IFNULL(MAX(seq), 0) + 1 FROM events), \
         'app.down', ?, '{\"kind\":\"node\"}', ?, ?, NULL, NULL, NULL, '{}')",
    )
    .bind(&event)
    .bind(now())
    .bind(&subject)
    .bind(&project.0)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO incidents (id, status, title, trigger_event_id, subject, project_id, opened_at) \
         VALUES (?, 'open', ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(title)
    .bind(&event)
    .bind(&subject)
    .bind(&project.0)
    .bind(now())
    .execute(pool)
    .await
    .unwrap();
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incidents_are_listed_shown_and_resolved() {
    let lab = lab().await;
    let output = lab.ok(&["incident", "list"]).await;
    assert_eq!(output.stdout, "No incidents.\n");
    let id = incident(&lab, 1, "web is down").await;
    let output = lab.ok(&["incident", "list"]).await;
    assert!(output.stdout.starts_with("ID"), "{}", output.stdout);
    assert!(output.stdout.contains(&id), "{}", output.stdout);
    assert!(output.stdout.contains("web is down"), "{}", output.stdout);
    let output = lab.ok(&["incident", "show", &id]).await;
    assert!(
        output.stdout.contains("Trigger:  app.down evt_"),
        "{}",
        output.stdout
    );
    assert!(
        output.stdout.contains("Grants:   none"),
        "{}",
        output.stdout
    );
    let output = lab.run(&["incident", "show", "web"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);

    let output = lab
        .ok(&[
            "incident",
            "resolve",
            &id,
            "--note",
            "restarted the node",
            "--reason",
            "fixed",
        ])
        .await;
    assert!(
        output
            .stdout
            .starts_with(&format!("Resolved the incident {id}")),
        "{}",
        output.stdout
    );
    let resolved = lab.get(&format!("/api/v1/incidents/{id}")).await;
    assert_eq!(resolved["status"], "resolved");
    assert_eq!(resolved["resolution_note"], "restarted the node");
    let output = lab.ok(&["incident", "list", "--status", "open"]).await;
    assert_eq!(output.stdout, "No open incidents.\n");
    let output = lab.run(&["incident", "resolve", &id]).await;
    assert_eq!(output.code, exit::CONFLICT, "{}", output.stderr);

    // An agent resolves with a reason, which is audited.
    let other = incident(&lab, 2, "web is down again").await;
    let (_, agent) = lab
        .agent("responder", &["incident:read", "incident:resolve"])
        .await;
    let output = quoro(
        &["incident", "resolve", &other, "--reason", "it is back"],
        &agent,
    )
    .await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);
    let (justification, actor, operation) = lab.primary.last_actor().await;
    assert_eq!(
        (justification.as_deref(), actor.as_str(), operation.as_str()),
        (
            Some("it is back"),
            "agent",
            "POST /api/v1/incidents/{incident_id}/resolve"
        )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_list_filters_by_actor_and_since() {
    let lab = lab().await;
    let (agent_id, agent) = lab.agent("auditor", &["project:read", "app:read"]).await;
    let output = quoro(&["app", "list"], &agent).await;
    assert_eq!(output.code, exit::OK, "{}", output.stderr);

    let output = lab.ok(&["audit", "list"]).await;
    let header = output.stdout.lines().next().unwrap();
    assert!(header.starts_with("SEQ"), "{}", output.stdout);
    assert!(
        output.stdout.contains(&format!("user {}", lab.owner_id)),
        "{}",
        output.stdout
    );

    // An agent by id and by name.
    for actor in [agent_id.as_str(), "auditor"] {
        let page = lab.json(&["audit", "list", "--actor", actor]).await;
        let items = page["items"].as_array().unwrap();
        assert!(!items.is_empty(), "{actor}");
        assert!(
            items.iter().all(|r| r["actor_id"] == agent_id.as_str()),
            "{actor}: {page}"
        );
    }
    // A user by email (a human may read users).
    let page = lab
        .json(&["audit", "list", "--actor", "owner@example.com"])
        .await;
    let items = page["items"].as_array().unwrap();
    assert!(!items.is_empty());
    assert!(
        items.iter().all(|r| r["actor_id"] == lab.owner_id.as_str()),
        "{page}"
    );
    let output = lab.run(&["audit", "list", "--actor", "nobody"]).await;
    assert_eq!(output.code, exit::NOT_FOUND, "{}", output.stderr);

    // --since: an old time keeps every record, a duration ago only the newest.
    let total = lab.primary.count("SELECT COUNT(*) FROM audit_log").await;
    assert!(total >= 4, "{total}");
    let all = lab
        .json(&["audit", "list", "--since", "2000-01-01T00:00:00Z"])
        .await;
    assert_eq!(all["items"][0]["seq"], 1, "{all}");
    // Records a second later than every earlier one: the search finds the
    // first of them (`--since` has the precision of a second).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    lab.ok(&[
        "vm",
        "create",
        "late",
        "--image",
        IMAGE,
        "--node",
        "test-node",
        "--vcpus",
        "1",
        "--memory",
        "64",
    ])
    .await;
    let (seq, time): (i64, String) =
        sqlx::query_as("SELECT seq, time FROM audit_log WHERE seq > ? ORDER BY seq LIMIT 1")
            .bind(total)
            .fetch_one(lab.primary.state.store.pool())
            .await
            .unwrap();
    let recent = lab.json(&["audit", "list", "--since", &time]).await;
    assert_eq!(recent["items"][0]["seq"], seq, "{recent}");
    let recent = lab.json(&["audit", "list", "--since", "10m"]).await;
    assert_eq!(recent["items"][0]["seq"], 1, "{recent}");
    let future = lab
        .ok(&["audit", "list", "--since", "2999-01-01T00:00:00Z"])
        .await;
    assert!(
        future
            .stdout
            .starts_with("No audit records since 2999-01-01"),
        "{}",
        future.stdout
    );
    let output = lab.run(&["audit", "list", "--since", "yesterday"]).await;
    assert_eq!(output.code, exit::USAGE, "{}", output.stderr);

    // Without --since: the 100 newest records, the newest at the bottom.
    filler(&lab, 150).await;
    let last = lab.primary.count("SELECT MAX(seq) FROM audit_log").await;
    let page = lab.json(&["audit", "list"]).await;
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 100, "{page}");
    assert_eq!(items[0]["seq"], last - 99);
    assert_eq!(items[99]["seq"], last);
    assert_eq!(page["next_cursor"], Value::Null);
    let output = lab.ok(&["audit", "list"]).await;
    let lines: Vec<&str> = output.stdout.lines().collect();
    assert!(
        lines[1].starts_with(&(last - 99).to_string()),
        "{}",
        output.stdout
    );
    assert!(
        lines[100].starts_with(&last.to_string()),
        "{}",
        output.stdout
    );
    assert_eq!(
        lines.last(),
        Some(&"No newer records."),
        "{}",
        output.stdout
    );
    // --since still reads forward one page and names the next.
    let output = lab
        .ok(&["audit", "list", "--since", "2000-01-01T00:00:00Z"])
        .await;
    assert!(
        output
            .stdout
            .contains("quoro api GET '/api/v1/audit?limit=100&after_seq=0&cursor="),
        "{}",
        output.stdout
    );
    // --actor without --since: the actor's newest records among the newest
    // 1000, here older than the 150 newest of the log.
    let page = lab.json(&["audit", "list", "--actor", &agent_id]).await;
    let items = page["items"].as_array().unwrap();
    assert!(!items.is_empty(), "{page}");
    assert!(items.iter().all(|r| r["actor_id"] == agent_id.as_str()));
    filler(&lab, 1000).await;
    let output = lab.ok(&["audit", "list", "--actor", &agent_id]).await;
    assert_eq!(
        output.stdout,
        format!(
            "No audit records of {agent_id} among the newest 1000; --since T looks further back.\n"
        )
    );
    let page = lab
        .json(&[
            "audit",
            "list",
            "--actor",
            &agent_id,
            "--since",
            "2000-01-01T00:00:00Z",
        ])
        .await;
    assert!(!page["items"].as_array().unwrap().is_empty(), "{page}");
}

/// Appends `n` records of a system actor, as a busy log would hold; each
/// links to the previous hash, as the table's trigger requires.
async fn filler(lab: &Lab, n: i64) {
    let mut tx = lab.primary.state.store.pool().begin().await.unwrap();
    for _ in 0..n {
        sqlx::query(
            "INSERT INTO audit_log (seq, time, actor_type, actor_id, channel, operation, params, \
             request_id, result, prev_hash, hash) VALUES ((SELECT MAX(seq) + 1 FROM audit_log), \
             ?, 'system', 'filler', 'system', 'test.filler', '{}', 'req_filler', 'ok', \
             (SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1), ?)",
        )
        .bind(now())
        .bind("0".repeat(64))
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn the_new_commands_need_a_context() {
    let primary = start().await;
    let empty = primary.dir.join("nowhere");
    std::fs::create_dir_all(&empty).unwrap();
    let environment = Environment::new([("XDG_CONFIG_HOME", empty.display().to_string())], false)
        .with_cwd(&empty);
    for args in [
        &["vm", "list"][..],
        &["vm", "console", "box"],
        &["incident", "list"],
        &["audit", "list"],
        &["ssh"],
    ] {
        let output = quoro(args, &environment).await;
        assert_eq!(output.code, exit::USAGE, "{args:?}: {}", output.stderr);
        assert!(
            output.stderr.contains("no context is selected"),
            "{args:?}: {}",
            output.stderr
        );
    }
    assert_eq!(primary.requests(), 0);
}
