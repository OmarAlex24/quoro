//! Metrics (`getNodeMetrics`, `streamNodeMetrics`, `getAppMetrics`,
//! `streamAppMetrics`) over real HTTP, SSE and the node-agent WebSocket
//! (`node-agent-protocol.md` section 5.17).

mod common;

use common::{Server, problem, start, start_with_cloudflare};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::store::{now_rfc3339, sha256_hex};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[7u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
}

fn enroll_body(token: &str, name: &str) -> Value {
    json!({
        "join_token": token,
        "name": name,
        "public_key": public_key(),
        "roles": ["node"],
        "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
        "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
    })
}

async fn create_token(server: &Server, owner: &reqwest::Client) -> Value {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    res.json().await.unwrap()
}

struct Enrolled {
    node_id: String,
    credential: String,
}

async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    let token = create_token(server, owner).await;
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&enroll_body(token["token"].as_str().unwrap(), name))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "enroll: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    Enrolled {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

fn hello(node_id: &str) -> NodeMessage {
    NodeMessage::Hello(wire::Hello {
        node_id: node_id.to_string(),
        agent_version: "test-agent".to_string(),
        engine_version: "test-engine".to_string(),
        boot_id: "boot-1".to_string(),
        stored_rev: 0,
        applied_rev: 0,
        log_seq: 0,
        log_hash: "0".repeat(64),
        leases: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        mesh_public_key: None,
    })
}

async fn connect(server: &Server, credential: &str) -> Socket {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status(), 101);
    socket
}

async fn send_text(socket: &mut Socket, text: String) {
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

async fn send(socket: &mut Socket, message: NodeMessage) {
    send_text(socket, message.to_frame("msg_test_send", None).to_text()).await;
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("a frame within 3 s")
            .expect("the socket is open")
            .expect("a valid websocket frame");
        match message {
            WsMessage::Text(text) => {
                return serde_json::from_str(text.as_str()).expect("JSON frame");
            }
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

/// Sends `hello` and consumes `welcome` and the first `desired_state`.
async fn open(server: &Server, node: &Enrolled) -> (Socket, Value) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let welcome = recv_type(&mut socket, "welcome").await;
    assert_eq!(welcome["body"]["primary_node_id"], node.node_id);
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
}

/// Replays one `command_result` for the command the handler sent.
async fn answer_command(socket: &mut Socket, command: &Value, body: Value) {
    let id = command["id"].as_str().unwrap();
    send_text(
        socket,
        json!({
            "v": 1,
            "type": "command_result",
            "id": "msg_test_result",
            "re": id,
            "body": body
        })
        .to_string(),
    )
    .await;
}

/// Sends one `stream_data` metrics report for `stream_id`.
async fn stream_metrics(socket: &mut Socket, stream_id: &str, seq: u64, metrics: Value) {
    send_text(
        socket,
        json!({
            "v": 1,
            "type": "stream_data",
            "id": format!("msg_test_data_{seq}"),
            "body": { "stream_id": stream_id, "seq": seq, "metrics": metrics }
        })
        .to_string(),
    )
    .await;
}

/// Ends one stream.
async fn end_stream(socket: &mut Socket, stream_id: &str, reason: &str) {
    send_text(
        socket,
        json!({
            "v": 1,
            "type": "stream_end",
            "id": "msg_test_end",
            "body": { "stream_id": stream_id, "reason": reason }
        })
        .to_string(),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const APP_ID: &str = "app_00000000000000000000000001";

fn vm_id(n: u32) -> String {
    format!("vm_{n:026}")
}

struct Fixture {
    node: Enrolled,
    project_id: String,
    app_id: String,
    /// Two instances on `node`, in ascending id order.
    vm_ids: Vec<String>,
}

/// Creates a project, an app and two of its VMs on one enrolled node. Rows go
/// directly into the store because the apps API is written in parallel.
async fn fixture(server: &Server, owner: &reqwest::Client, name: &str) -> Fixture {
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": format!("metrics-{name}"), "name": "Metrics Demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap().to_string();
    let node = enrolled(server, owner, name).await;
    let now = now_rfc3339();
    let app_id = APP_ID.to_string();

    sqlx::query(
        "INSERT INTO apps (id, project_id, name, image, port, health_check, resources, placement, \
         status, created_at, updated_at) \
         VALUES (?, ?, 'web', ?, 8080, ?, ?, '{\"node_ids\":[]}', 'deploying', ?, ?)",
    )
    .bind(&app_id)
    .bind(&project_id)
    .bind("ghcr.io/example/web:1")
    .bind(r#"{"path":"/healthz","interval_s":10,"timeout_s":2,"failures":3}"#)
    .bind(r#"{"vcpus":2,"memory_mb":512,"disk_gb":4}"#)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();

    let mut vm_ids = Vec::new();
    for n in [1, 2] {
        let id = vm_id(n);
        add_vm(server, &project_id, &app_id, &node.node_id, &id).await;
        vm_ids.push(id);
    }

    Fixture {
        node,
        project_id,
        app_id,
        vm_ids,
    }
}

/// Inserts one running VM of the app on `node_id`.
async fn add_vm(server: &Server, project_id: &str, app_id: &str, node_id: &str, id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'web', 2, 512, 4, ?, 'running', 'creating', NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(id)
    .bind(project_id)
    .bind(app_id)
    .bind(node_id)
    .bind("ghcr.io/example/web:1")
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// A bearer token for the owner, for requests that hold a streaming body open.
async fn owner_token(server: &Server) -> String {
    let (user_id,): (String,) = sqlx::query_as("SELECT id FROM users ORDER BY created_at LIMIT 1")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let secret = format!("tok_{}", "A".repeat(26));
    let id = quoro_types::new_id(quoro_types::Prefix::User);
    sqlx::query(
        "INSERT INTO api_tokens (id, user_id, name, token_hash, created_at) VALUES (?, ?, 'sse', ?, ?)",
    )
    .bind(id)
    .bind(user_id)
    .bind(sha256_hex(secret.as_bytes()))
    .bind(now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    secret
}

// ---------------------------------------------------------------------------
// The raw SSE client
// ---------------------------------------------------------------------------

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Reads the SSE body of one request over a raw HTTP/1.0 connection, which
/// carries no chunked framing (it ends when the connection closes).
struct Sse {
    stream: TcpStream,
    buffer: Vec<u8>,
    headers_checked: bool,
}

impl Sse {
    async fn open(server: &Server, path: &str, token: &str) -> Self {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        let request = format!(
            "GET {path} HTTP/1.0\r\nHost: {}\r\nAuthorization: Bearer {token}\r\n\
             Accept: text/event-stream\r\n\r\n",
            server.address()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        Self {
            stream,
            buffer: Vec::new(),
            headers_checked: false,
        }
    }

    /// Reads and checks the response headers once.
    async fn read_headers(&mut self) {
        loop {
            if let Some(position) = find(&self.buffer, b"\r\n\r\n") {
                let headers =
                    String::from_utf8_lossy(&self.buffer[..position]).to_ascii_lowercase();
                assert!(
                    headers.starts_with("http/1.1 200") || headers.starts_with("http/1.0 200"),
                    "{headers}"
                );
                assert!(headers.contains("text/event-stream"), "{headers}");
                assert!(headers.contains("cache-control: no-cache"), "{headers}");
                self.buffer.drain(..position + 4);
                self.headers_checked = true;
                return;
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "the response headers ended early");
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// The next complete SSE frame (`field: value` lines without the final
    /// blank line).
    async fn next_frame(&mut self) -> Option<String> {
        if !self.headers_checked {
            self.read_headers().await;
        }
        loop {
            if let Some(position) = find(&self.buffer, b"\n\n") {
                let frame = String::from_utf8_lossy(&self.buffer[..position]).to_string();
                self.buffer.drain(..position + 2);
                return Some(frame);
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// The next frame that is not a comment/keep-alive line.
    async fn next_event(&mut self) -> String {
        loop {
            let frame = timeout(Duration::from_secs(3), self.next_frame())
                .await
                .expect("an SSE frame within 3 s")
                .expect("the stream is open");
            if frame.lines().any(|line| line.starts_with("event:")) {
                return frame;
            }
        }
    }
}

/// One `field: value` of an SSE frame.
fn field(frame: &str, name: &str) -> Option<String> {
    frame.lines().find_map(|line| {
        line.strip_prefix(&format!("{name}:"))
            .map(|value| value.trim_start().to_string())
    })
}

// ---------------------------------------------------------------------------
// Fixtures for the agent and viewer callers
// ---------------------------------------------------------------------------

async fn owner_id(owner: &reqwest::Client, server: &Server) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    me["user"]["id"].as_str().unwrap().to_string()
}

/// An agent access token whose agent holds no grant at all.
async fn unprivileged_agent_token(owner: &reqwest::Client, server: &Server) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "metrics-reader",
            "model": "test-model",
            "owner_user_id": owner_id(owner, server).await,
            "description": "an agent without a grant",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create agent: {}",
        res.text().await.unwrap()
    );
    let agent: Value = res.json().await.unwrap();
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create key: {}",
        res.text().await.unwrap()
    );
    let key: Value = res.json().await.unwrap();
    let key = key["key"].as_str().unwrap().to_string();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "token: {}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

const VIEWER_PASSWORD: &str = "viewer-password";

async fn create_viewer(owner: &reqwest::Client, server: &Server) -> reqwest::Client {
    let res = owner
        .post(server.url("/api/v1/users"))
        .json(&json!({
            "email": "viewer@example.com",
            "name": "Viewer",
            "role": "viewer",
            "password": VIEWER_PASSWORD,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create user: {}",
        res.text().await.unwrap()
    );
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", VIEWER_PASSWORD)
        .await;
    viewer
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_node_metrics_read_returns_the_nodes_samples() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "node-read").await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let client = owner.clone();
    let url = server.url(&format!("/api/v1/nodes/{}/metrics", fixture.node.node_id));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });

    let command = recv_type(&mut socket, "command").await;
    assert_eq!(command["body"]["kind"], "metrics_query");
    assert_eq!(command["body"]["params"], json!({ "node": true }));
    answer_command(
        &mut socket,
        &command,
        json!({
            "status": "ok",
            "metrics": {
                "interval_s": 5,
                "node": [
                    {
                        "time": "2026-11-04T10:15:30Z",
                        "cpu_usage": 0.12,
                        "cpu_count": 8,
                        "memory_total_bytes": 17_179_869_184_u64,
                        "memory_used_bytes": 6_442_450_944_u64
                    },
                    {
                        "time": "2026-11-04T10:15:35Z",
                        "cpu_usage": 0.5,
                        "cpu_count": 8,
                        "memory_total_bytes": 17_179_869_184_u64,
                        "memory_used_bytes": 6_600_000_000_u64
                    }
                ]
            }
        }),
    )
    .await;

    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["node_id"], fixture.node.node_id);
    assert_eq!(body["interval_s"], 5);
    let samples = body["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2, "{body}");
    assert_eq!(samples[0]["time"], "2026-11-04T10:15:30Z");
    assert_eq!(samples[0]["cpu_usage"], 0.12);
    assert_eq!(samples[0]["cpu_count"], 8);
    assert_eq!(samples[1]["cpu_usage"], 0.5);
}

#[tokio::test]
async fn an_unknown_or_malformed_node_is_not_found() {
    let server = start().await;
    let owner = server.owner().await;

    for node_id in ["nod_00000000000000000000000001", "node_bad"] {
        let (status, code, _) = problem(
            owner
                .get(server.url(&format!("/api/v1/nodes/{node_id}/metrics")))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{node_id}");
    }
}

#[tokio::test]
async fn metrics_without_a_live_session_are_node_unreachable() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "unreachable").await;

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/nodes/{}/metrics", fixture.node.node_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!(
                "/api/v1/nodes/{}/metrics/stream",
                fixture.node.node_id
            )))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/apps/{}/metrics", fixture.app_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));
}

#[tokio::test]
async fn app_metrics_group_vms_per_node_and_leave_out_a_missing_vm() {
    // Two live sessions must stay open long enough for both commands; the
    // default test server times a session out after 800 ms.
    let server = start_with_cloudflare("http://127.0.0.1:9", "http://127.0.0.1:9/dns-query").await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "grouped").await;

    // A second node with a third instance of the same app.
    let node_b = enrolled(&server, &owner, "node-metrics-b").await;
    let third = vm_id(3);
    add_vm(
        &server,
        &fixture.project_id,
        &fixture.app_id,
        &node_b.node_id,
        &third,
    )
    .await;

    let (mut socket_a, _) = open(&server, &fixture.node).await;
    let (mut socket_b, _) = open(&server, &node_b).await;

    let client = owner.clone();
    let url = server.url(&format!("/api/v1/apps/{}/metrics", fixture.app_id));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });

    let command_a = recv_type(&mut socket_a, "command").await;
    assert_eq!(command_a["body"]["kind"], "metrics_query");
    assert_eq!(
        command_a["body"]["params"]["vm_ids"],
        json!([fixture.vm_ids[0], fixture.vm_ids[1]])
    );

    // The handler asks one node at a time, in node-id order: answer node A
    // before node B's command is sent.
    answer_command(
        &mut socket_a,
        &command_a,
        json!({
            "status": "ok",
            "metrics": {
                "interval_s": 5,
                "vms": [{
                    "vm_id": fixture.vm_ids[0],
                    "samples": [{
                        "time": "2026-11-04T10:15:30Z",
                        "cpu_usage": 0.04,
                        "vcpus": 2,
                        "memory_rss_bytes": 201_326_592_u64,
                        "memory_bytes": 536_870_912_u64
                    }]
                }]
            }
        }),
    )
    .await;

    let command_b = recv_type(&mut socket_b, "command").await;
    assert_eq!(
        command_b["body"]["params"]["vm_ids"],
        json!([third.clone()])
    );

    answer_command(
        &mut socket_b,
        &command_b,
        json!({
            "status": "ok",
            "metrics": {
                "interval_s": 5,
                "vms": [{
                    "vm_id": third,
                    "samples": [{
                        "time": "2026-11-04T10:15:30Z",
                        "cpu_usage": 0.2,
                        "vcpus": 2,
                        "memory_rss_bytes": 300_000_000_u64,
                        "memory_bytes": 536_870_912_u64
                    }]
                }]
            }
        }),
    )
    .await;

    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["app_id"], fixture.app_id);
    assert_eq!(body["interval_s"], 5);
    let instances = body["instances"].as_array().unwrap();
    assert_eq!(instances.len(), 3, "{body}");
    assert_eq!(instances[0]["vm_id"], fixture.vm_ids[0]);
    assert_eq!(instances[0]["node_id"], fixture.node.node_id);
    assert_eq!(instances[0]["samples"].as_array().unwrap().len(), 1);
    assert_eq!(instances[1]["vm_id"], fixture.vm_ids[1]);
    assert_eq!(
        instances[1]["samples"].as_array().unwrap().len(),
        0,
        "a VM the node left out keeps an empty sample list"
    );
    assert_eq!(instances[2]["vm_id"], third);
    assert_eq!(instances[2]["node_id"], node_b.node_id);
    assert_eq!(instances[2]["samples"][0]["cpu_usage"], 0.2);
}

#[tokio::test]
async fn a_viewer_can_read_node_and_app_metrics() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "viewer").await;
    // Creating and logging in the viewer hashes twice with Argon2id, which can
    // outlast the test server's short session timeout; do it before the node
    // connects.
    let viewer = create_viewer(&owner, &server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let client = viewer.clone();
    let url = server.url(&format!("/api/v1/nodes/{}/metrics", fixture.node.node_id));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });
    let command = recv_type(&mut socket, "command").await;
    answer_command(
        &mut socket,
        &command,
        json!({ "status": "ok", "metrics": { "interval_s": 5, "node": [] } }),
    )
    .await;
    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);

    let client = viewer.clone();
    let url = server.url(&format!("/api/v1/apps/{}/metrics", fixture.app_id));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });
    let command = recv_type(&mut socket, "command").await;
    answer_command(
        &mut socket,
        &command,
        json!({ "status": "ok", "metrics": { "interval_s": 5, "vms": [] } }),
    )
    .await;
    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn an_agent_without_a_grant_is_forbidden() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "agent").await;
    let (mut socket, _) = open(&server, &fixture.node).await;
    let token = unprivileged_agent_token(&owner, &server).await;

    let response = reqwest::Client::new()
        .get(server.url(&format!("/api/v1/nodes/{}/metrics", fixture.node.node_id)))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "tests read metrics")
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(response).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    // No command reached the node.
    let extra = timeout(
        Duration::from_millis(150),
        recv_type(&mut socket, "command"),
    )
    .await;
    assert!(extra.is_err(), "a refused call must not reach the node");
}

#[tokio::test]
async fn stream_node_metrics_frames_samples_and_closes_on_disconnect() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "node-stream").await;
    let token = owner_token(&server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/nodes/{}/metrics/stream", fixture.node.node_id),
        &token,
    )
    .await;

    let open_frame = recv_type(&mut socket, "stream_open").await;
    assert_eq!(open_frame["body"]["kind"], "metrics");
    assert_eq!(open_frame["body"]["params"], json!({ "node": true }));
    let stream_id = open_frame["body"]["stream_id"]
        .as_str()
        .unwrap()
        .to_string();

    stream_metrics(
        &mut socket,
        &stream_id,
        1,
        json!({
            "interval_s": 5,
            "node": [{
                "time": "2026-11-04T10:15:30Z",
                "cpu_usage": 0.12,
                "cpu_count": 8,
                "memory_total_bytes": 17_179_869_184_u64,
                "memory_used_bytes": 6_442_450_944_u64
            }]
        }),
    )
    .await;

    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "event").as_deref(), Some("sample"));
    assert_eq!(
        field(&frame, "id"),
        None,
        "metric samples carry no id: {frame}"
    );
    let sample: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(
        sample,
        json!({
            "time": "2026-11-04T10:15:30Z",
            "cpu_usage": 0.12,
            "cpu_count": 8,
            "memory_total_bytes": 17_179_869_184_u64,
            "memory_used_bytes": 6_442_450_944_u64
        })
    );

    // The consumer goes away: the node must be told to stop producing.
    drop(sse);
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream_metrics(
        &mut socket,
        &stream_id,
        2,
        json!({ "interval_s": 5, "node": [] }),
    )
    .await;
    let close = recv_type(&mut socket, "stream_close").await;
    assert_eq!(close["body"]["stream_id"], stream_id);
}

#[tokio::test]
async fn a_node_stream_ending_emits_an_error_event_and_ends_the_body() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "node-stream-end").await;
    let token = owner_token(&server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/nodes/{}/metrics/stream", fixture.node.node_id),
        &token,
    )
    .await;
    let open_frame = recv_type(&mut socket, "stream_open").await;
    let stream_id = open_frame["body"]["stream_id"]
        .as_str()
        .unwrap()
        .to_string();

    end_stream(&mut socket, &stream_id, "completed").await;
    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "event").as_deref(), Some("error"));
    let data: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(data["code"], "node_unreachable");
    assert_eq!(data["status"], 504);

    let next = timeout(Duration::from_secs(3), sse.next_frame())
        .await
        .unwrap();
    assert!(next.is_none(), "the body ended after the error event");
}

#[tokio::test]
async fn stream_app_metrics_frames_carry_vm_and_node_ids() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner, "app-stream").await;
    let token = owner_token(&server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/apps/{}/metrics/stream", fixture.app_id),
        &token,
    )
    .await;

    let open_frame = recv_type(&mut socket, "stream_open").await;
    assert_eq!(open_frame["body"]["kind"], "metrics");
    assert_eq!(
        open_frame["body"]["params"]["vm_ids"],
        json!([fixture.vm_ids[0], fixture.vm_ids[1]])
    );
    let stream_id = open_frame["body"]["stream_id"]
        .as_str()
        .unwrap()
        .to_string();

    stream_metrics(
        &mut socket,
        &stream_id,
        1,
        json!({
            "interval_s": 5,
            "vms": [{
                "vm_id": fixture.vm_ids[0],
                "samples": [
                    {
                        "time": "2026-11-04T10:15:30Z",
                        "cpu_usage": 0.04,
                        "vcpus": 2,
                        "memory_rss_bytes": 201_326_592_u64,
                        "memory_bytes": 536_870_912_u64
                    },
                    {
                        "time": "2026-11-04T10:15:35Z",
                        "cpu_usage": 0.08,
                        "vcpus": 2,
                        "memory_rss_bytes": 210_000_000_u64,
                        "memory_bytes": 536_870_912_u64
                    }
                ]
            }]
        }),
    )
    .await;

    for cpu_usage in [0.04, 0.08] {
        let frame = sse.next_event().await;
        assert_eq!(field(&frame, "event").as_deref(), Some("sample"));
        assert_eq!(field(&frame, "id"), None, "{frame}");
        let sample: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
        assert_eq!(sample["vm_id"], fixture.vm_ids[0]);
        assert_eq!(sample["node_id"], fixture.node.node_id);
        assert_eq!(sample["cpu_usage"], cpu_usage);
        assert_eq!(sample["vcpus"], 2);
        assert_eq!(sample["memory_bytes"], 536_870_912_u64);
    }

    // A node stream ending closes the merged body with an error event.
    end_stream(&mut socket, &stream_id, "completed").await;
    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "event").as_deref(), Some("error"));
    let data: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(data["code"], "node_unreachable");
    let next = timeout(Duration::from_secs(3), sse.next_frame())
        .await
        .unwrap();
    assert!(next.is_none(), "the body ended after the error event");
}

#[tokio::test]
async fn an_unknown_app_is_not_found_and_an_app_without_vms_ends_at_once() {
    let server = start().await;
    let owner = server.owner().await;

    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/apps/app_unknown/metrics"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    let fixture = fixture(&server, &owner, "empty-app").await;
    // Remove both VMs: the app has no instance left to sample.
    sqlx::query("DELETE FROM vms WHERE app_id = ?")
        .bind(&fixture.app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let response = owner
        .get(server.url(&format!("/api/v1/apps/{}/metrics", fixture.app_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["interval_s"], 5);
    assert_eq!(body["instances"], json!([]));

    let token = owner_token(&server).await;
    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/apps/{}/metrics/stream", fixture.app_id),
        &token,
    )
    .await;
    sse.read_headers().await;
    let next = timeout(Duration::from_secs(3), sse.next_frame())
        .await
        .unwrap();
    assert!(
        next.is_none(),
        "an app without instances ends its body at once"
    );
}
