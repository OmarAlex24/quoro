//! Logs (`getAppLogs`, `streamAppLogs`, `getDeploymentLogs`) and the
//! app-VM desired state that feeds them, over real HTTP, SSE and the
//! node-agent WebSocket (`node-agent-protocol.md` sections 5.7, 5.11, 5.15
//! and 5.17).

mod common;

use common::{Server, problem, start};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::app_env;
use quoro_control_plane::store::{now_rfc3339, sha256_hex};
use quoro_proto::sign;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Health, Kvm as WireKvm, NodeMessage, ObservedStatus,
    StatusReport, VmStatus,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const NODE_NAME: &str = "node-logs";

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
        volume_snapshot: None,
        custody_public_key: None,
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

/// Sends one `stream_data` batch for `stream_id`.
async fn stream_lines(socket: &mut Socket, stream_id: &str, seq: u64, lines: Value) {
    send_text(
        socket,
        json!({
            "v": 1,
            "type": "stream_data",
            "id": format!("msg_test_data_{seq}"),
            "body": { "stream_id": stream_id, "seq": seq, "lines": lines }
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

fn app_id() -> String {
    "app_00000000000000000000000001".to_string()
}

fn deployment_id() -> String {
    "dep_00000000000000000000000001".to_string()
}

fn domain_id() -> String {
    "dom_00000000000000000000000001".to_string()
}

struct Fixture {
    node: Enrolled,
    app_id: String,
    vm_id: String,
    deployment_id: String,
}

/// Creates a project, an app with one VM on a node, the deployment linked to
/// that VM, an encrypted environment value and a domain. Rows go directly into
/// the store because the apps API is written in parallel.
async fn fixture(server: &Server, owner: &reqwest::Client) -> Fixture {
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "logs-demo", "name": "Logs Demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap().to_string();
    let node = enrolled(server, owner, NODE_NAME).await;
    let app_id = app_id();
    let deployment_id = deployment_id();
    let vm_id = quoro_types::new_id(quoro_types::Prefix::Vm);
    let now = now_rfc3339();

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

    sqlx::query(
        "INSERT INTO deployments (id, app_id, image, status, \"trigger\", created_by_type, \
         created_by_id, created_at) \
         VALUES (?, ?, ?, 'pending', 'api', 'user', 'usr_test', ?)",
    )
    .bind(&deployment_id)
    .bind(&app_id)
    .bind("ghcr.io/example/web:1")
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'web', 2, 512, 4, ?, 'running', 'creating', NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(&vm_id)
    .bind(&project_id)
    .bind(&app_id)
    .bind(&node.node_id)
    .bind("ghcr.io/example/web:1")
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();

    let blob = app_env::encrypt_value(
        &server.state.data_key,
        &app_id,
        "DATABASE_URL",
        "postgres://web@10.42.1.5/web",
    );
    sqlx::query("INSERT INTO app_env (app_id, name, value_enc, updated_at) VALUES (?, ?, ?, ?)")
        .bind(&app_id)
        .bind("DATABASE_URL")
        .bind(blob)
        .bind(&now)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO domains (id, app_id, hostname, tls_mode, cert_status, created_at) \
         VALUES (?, ?, 'web.example.com', 'acme', 'pending', ?)",
    )
    .bind(domain_id())
    .bind(&app_id)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();

    Fixture {
        node,
        app_id,
        vm_id,
        deployment_id,
    }
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
    /// Writes the request; the response headers are read by [`Sse::next_frame`]
    /// so a handler that fills a resume gap is never deadlocked by a client
    /// that has not read yet.
    async fn open(server: &Server, path: &str, token: &str, last_event: Option<&str>) -> Self {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        let mut request = format!(
            "GET {path} HTTP/1.0\r\nHost: {}\r\nAuthorization: Bearer {token}\r\n\
             Accept: text/event-stream\r\n",
            server.address()
        );
        if let Some(last_event) = last_event {
            request.push_str(&format!("Last-Event-ID: {last_event}\r\n"));
        }
        request.push_str("\r\n");
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

fn ns(time: &str) -> i128 {
    quoro_control_plane::api::logs::line_ns(time).expect("a valid time")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_snapshot_carries_the_app_vm_env_port_deployment_and_route() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let (_socket, snapshot) = open(&server, &fixture.node).await;

    let spec = &snapshot["body"]["spec"];
    let vms = spec["vms"].as_array().unwrap();
    assert_eq!(vms.len(), 1);
    let vm = &vms[0];
    assert_eq!(vm["vm_id"], fixture.vm_id);
    assert_eq!(vm["app_id"], fixture.app_id);
    assert_eq!(vm["port"], 8080);
    assert_eq!(vm["deployment_id"], fixture.deployment_id);
    assert_eq!(
        vm["env"]["DATABASE_URL"], "postgres://web@10.42.1.5/web",
        "the desired state carries the decrypted value"
    );
    assert_eq!(vm["health_check"]["path"], "/healthz");
    assert_eq!(vm["health_check"]["failures"], 3);

    let routes = spec["routes"].as_array().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0]["hostname"], "web.example.com");
    assert_eq!(routes[0]["app_id"], fixture.app_id);
    assert_eq!(routes[0]["port"], 8080);
    assert_eq!(routes[0]["tls_mode"], "acme");
    assert_eq!(routes[0]["vm_ids"], json!([fixture.vm_id]));
}

#[tokio::test]
async fn get_app_logs_asks_the_node_and_keeps_the_tail() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let client = owner.clone();
    let url = server.url(&format!("/api/v1/apps/{}/logs?tail=2", fixture.app_id));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });

    let command = recv_type(&mut socket, "command").await;
    assert_eq!(command["body"]["kind"], "logs_query");
    assert_eq!(command["body"]["params"]["app_id"], fixture.app_id);
    assert_eq!(command["body"]["params"]["tail"], 2);
    let lines = json!([
        {
            "time": "2026-11-02T10:00:00.000000001Z",
            "stream": "stdout",
            "line": "one",
            "vm_id": fixture.vm_id
        },
        {
            "time": "2026-11-02T10:00:00.000000002Z",
            "stream": "stderr",
            "line": "two",
            "vm_id": fixture.vm_id
        },
        {
            "time": "2026-11-02T10:00:00.000000003Z",
            "stream": "stdout",
            "line": "three"
        }
    ]);
    answer_command(
        &mut socket,
        &command,
        json!({ "status": "ok", "lines": lines }),
    )
    .await;

    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "{body}");
    assert_eq!(items[0]["line"], "two");
    assert_eq!(items[0]["stream"], "stderr");
    assert_eq!(items[1]["line"], "three");
    // A line without `vm_id` falls back to the app's only VM.
    assert_eq!(items[1]["vm_id"], fixture.vm_id);
    assert_eq!(items[1]["time"], "2026-11-02T10:00:00.000000003Z");
}

#[tokio::test]
async fn app_logs_without_a_live_session_are_node_unreachable() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/apps/{}/logs", fixture.app_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/apps/{}/logs/stream", fixture.app_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!(
                "/api/v1/deployments/{}/logs",
                fixture.deployment_id
            )))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));
}

#[tokio::test]
async fn stream_app_logs_frames_lines_and_closes_on_disconnect() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let token = owner_token(&server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/apps/{}/logs/stream?tail=0", fixture.app_id),
        &token,
        None,
    )
    .await;

    let open_frame = recv_type(&mut socket, "stream_open").await;
    assert_eq!(open_frame["body"]["kind"], "app_logs");
    assert_eq!(open_frame["body"]["params"]["app_id"], fixture.app_id);
    let stream_id = open_frame["body"]["stream_id"]
        .as_str()
        .unwrap()
        .to_string();

    let first = "2026-11-02T10:00:00.100000000Z";
    stream_lines(
        &mut socket,
        &stream_id,
        1,
        json!([{ "time": first, "stream": "stdout", "line": "hello", "vm_id": fixture.vm_id }]),
    )
    .await;

    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "event").as_deref(), Some("log"));
    assert_eq!(field(&frame, "id"), Some(ns(first).to_string()));
    let entry: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(entry["line"], "hello");
    assert_eq!(entry["vm_id"], fixture.vm_id);
    assert_eq!(entry["stream"], "stdout");

    // The consumer goes away: the node must be told to stop producing.
    drop(sse);
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream_lines(
        &mut socket,
        &stream_id,
        2,
        json!([{
            "time": "2026-11-02T10:00:00.200000000Z",
            "stream": "stdout",
            "line": "after the client left"
        }]),
    )
    .await;
    let close = recv_type(&mut socket, "stream_close").await;
    assert_eq!(close["body"]["stream_id"], stream_id);
}

#[tokio::test]
async fn last_event_id_resumes_without_repeating_a_line() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let token = owner_token(&server).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let seen = "2026-11-02T10:00:00.100000000Z";
    let history = "2026-11-02T10:00:00.200000000Z";
    let live = "2026-11-02T10:00:00.300000000Z";
    let mut sse = Sse::open(
        &server,
        &format!("/api/v1/apps/{}/logs/stream", fixture.app_id),
        &token,
        Some(&ns(seen).to_string()),
    )
    .await;

    // The live stream opens first, then the gap query is filled from history.
    let open_frame = recv_type(&mut socket, "stream_open").await;
    let stream_id = open_frame["body"]["stream_id"]
        .as_str()
        .unwrap()
        .to_string();
    let command = recv_type(&mut socket, "command").await;
    assert_eq!(command["body"]["kind"], "logs_query");
    assert_eq!(
        command["body"]["params"]["since"],
        "2026-11-02T10:00:00.100000000Z"
    );
    answer_command(
        &mut socket,
        &command,
        json!({
            "status": "ok",
            "lines": [
                { "time": seen, "stream": "stdout", "line": "already seen", "vm_id": fixture.vm_id },
                { "time": history, "stream": "stdout", "line": "history", "vm_id": fixture.vm_id }
            ]
        }),
    )
    .await;

    // Live data: the history line again (a duplicate) and a new one.
    stream_lines(
        &mut socket,
        &stream_id,
        1,
        json!([
            { "time": history, "stream": "stdout", "line": "history", "vm_id": fixture.vm_id },
            { "time": live, "stream": "stdout", "line": "live", "vm_id": fixture.vm_id }
        ]),
    )
    .await;

    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "id"), Some(ns(history).to_string()));
    let entry: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(entry["line"], "history");

    let frame = sse.next_event().await;
    assert_eq!(field(&frame, "id"), Some(ns(live).to_string()));
    let entry: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
    assert_eq!(entry["line"], "live");

    // No third copy of the history line.
    let extra = timeout(Duration::from_millis(150), sse.next_event()).await;
    assert!(extra.is_err(), "the duplicate line was not emitted again");
}

#[tokio::test]
async fn deployment_logs_merge_the_four_phases_in_time_order() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let (mut socket, _) = open(&server, &fixture.node).await;

    let client = owner.clone();
    let url = server.url(&format!(
        "/api/v1/deployments/{}/logs",
        fixture.deployment_id
    ));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });

    let mut phases = std::collections::BTreeMap::new();
    for _ in 0..4 {
        let open_frame = recv_type(&mut socket, "stream_open").await;
        assert_eq!(open_frame["body"]["kind"], "deployment_logs");
        assert_eq!(
            open_frame["body"]["params"]["deployment_id"],
            fixture.deployment_id
        );
        let phase = open_frame["body"]["params"]["phase"]
            .as_str()
            .unwrap()
            .to_string();
        let stream_id = open_frame["body"]["stream_id"]
            .as_str()
            .unwrap()
            .to_string();
        phases.insert(phase, stream_id);
    }
    assert_eq!(
        phases.keys().cloned().collect::<Vec<_>>(),
        ["boot", "health", "pull", "rootfs"]
    );

    let lines = [
        (
            "pull",
            "2026-11-02T10:00:00.400000000Z",
            "pulling ghcr.io/example/web:1",
        ),
        (
            "rootfs",
            "2026-11-02T10:00:00.300000000Z",
            "preparing the rootfs",
        ),
        ("boot", "2026-11-02T10:00:00.100000000Z", "booting"),
        (
            "health",
            "2026-11-02T10:00:00.200000000Z",
            "health check passed",
        ),
    ];
    for (phase, time, message) in lines {
        let stream_id = phases.get(phase).unwrap();
        stream_lines(
            &mut socket,
            stream_id,
            1,
            json!([{ "time": time, "stream": "stdout", "line": message }]),
        )
        .await;
        end_stream(&mut socket, stream_id, "completed").await;
    }

    let response = http.await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 4, "{body}");
    let messages: Vec<&str> = items
        .iter()
        .map(|item| item["message"].as_str().unwrap())
        .collect();
    assert_eq!(
        messages,
        [
            "booting",
            "health check passed",
            "preparing the rootfs",
            "pulling ghcr.io/example/web:1"
        ]
    );
    assert_eq!(items[0]["phase"], "boot");
    assert_eq!(items[3]["phase"], "pull");
}

#[tokio::test]
async fn an_instance_of_another_app_is_a_validation_failure() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;

    let (status, code, _) = problem(
        owner
            .get(server.url(&format!(
                "/api/v1/apps/{}/logs?instance=vm_00000000000000000000000001",
                fixture.app_id
            )))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/apps/app_unknown/logs"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

/// The fake node reports a VM running; the session must still hand the status
/// to the deployment hook without failing.
#[tokio::test]
async fn a_status_report_for_an_app_vm_is_accepted() {
    let server = start().await;
    let owner = server.owner().await;
    let fixture = fixture(&server, &owner).await;
    let (mut socket, snapshot) = open(&server, &fixture.node).await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();

    send(
        &mut socket,
        NodeMessage::StatusReport(StatusReport {
            applied_rev: rev,
            vms: vec![VmStatus {
                vm_id: fixture.vm_id.clone(),
                observed_status: ObservedStatus::Running,
                health: Health::Passing,
                ip: Some("10.42.1.9".to_string()),
                restart_count: 0,
                last_exit_code: None,
            }],
            routes: Vec::new(),
            capacity: WireCapacity {
                cpus: 8,
                memory_mb: 16_384,
                disk_gb: 100,
            },
            allocated: WireCapacity {
                cpus: 2,
                memory_mb: 512,
                disk_gb: 4,
            },
            disk_free_gb: 90.0,
            builds: Vec::new(),
            edge_backends: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: Vec::new(),
            volumes: Vec::new(),
        }),
    )
    .await;

    for _ in 0..100 {
        let (status, ip): (String, Option<String>) =
            sqlx::query_as("SELECT observed_status, ip FROM vms WHERE id = ?")
                .bind(&fixture.vm_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        if status == "running" {
            assert_eq!(ip.as_deref(), Some("10.42.1.9"));
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the status report was not applied");
}
