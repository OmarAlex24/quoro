//! Interactive sessions over real HTTP and WebSocket with a fake node session:
//! the web terminal, the read-only console, single-use tickets, the relay with
//! credit flow control, re-checks, `session.end` and transcript fetch
//! (`permission-model.md` sections 7.5 and 7.6, `audit-log-format.md` section
//! 3.3, `node-agent-protocol.md` sections 5.7, 5.8, 5.15, 5.16, 5.24 and 5.25).

mod common;

use base64::Engine as _;
use common::{Server, problem, start};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::interactive::Timings;
use quoro_control_plane::store::{now_rfc3339, sha256_hex};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage};
use serde_json::{Value, json};
use std::future::Future;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;
type Client = reqwest::Client;

const PASSWORD: &str = "a role password";
const ORIGIN: &str = "https://primary.example:8443";
const APP_OPERATION: &str = "POST /api/v1/apps/{app_id}/terminal-sessions";
const CONSOLE_OPERATION: &str = "POST /api/v1/vms/{vm_id}/console-sessions";

fn new_msg() -> String {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).expect("random");
    wire::msg_id(random)
}

fn frame(kind: &str, body: Value) -> Value {
    json!({ "v": 1, "type": kind, "id": new_msg(), "body": body })
}

fn stream_data(stream_id: &str, seq: u64, data: &[u8]) -> Value {
    frame(
        "stream_data",
        json!({
            "stream_id": stream_id,
            "seq": seq,
            "data": base64::engine::general_purpose::STANDARD.encode(data),
        }),
    )
}

fn stream_end(stream_id: &str, reason: &str, extra: Value) -> Value {
    let mut body = json!({ "stream_id": stream_id, "reason": reason });
    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            body[key] = value;
        }
    }
    frame("stream_end", body)
}

fn command_error(open: &Value, code: &str, message: &str) -> Value {
    json!({
        "v": 1,
        "type": "command_result",
        "id": new_msg(),
        "re": open["id"],
        "body": {
            "status": "error",
            "error": { "code": code, "message": message }
        }
    })
}

// ---------------------------------------------------------------------------
// The fake node session
// ---------------------------------------------------------------------------

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[7u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
}

struct Enrolled {
    node_id: String,
    credential: String,
}

async fn enroll(server: &Server, owner: &Client, name: &str) -> Enrolled {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"].as_str().unwrap(),
            "name": name,
            "public_key": public_key(),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
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
    })
}

/// A live node session whose frames the test reads and writes directly, kept
/// alive by a heartbeat pump.
struct Node {
    out: mpsc::UnboundedSender<Value>,
    incoming: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn connect(server: &Server, node: &Enrolled) -> Self {
        let url = format!("ws://{}/api/v1/nodes/session", server.address());
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", node.credential).parse().unwrap(),
        );
        let stream = TcpStream::connect(server.address()).await.unwrap();
        let (mut socket, response) = tokio_tungstenite::client_async(request, stream)
            .await
            .expect("node websocket upgrade");
        assert_eq!(response.status(), 101);
        socket
            .send(WsMessage::Text(
                hello(&node.node_id)
                    .to_frame(&new_msg(), None)
                    .to_text()
                    .into(),
            ))
            .await
            .unwrap();

        let (out, out_rx): (mpsc::UnboundedSender<Value>, mpsc::UnboundedReceiver<Value>) =
            mpsc::unbounded_channel();
        let (in_tx, incoming) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut out_rx = out_rx;
            let mut beat = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    message = socket.next() => match message {
                        Some(Ok(WsMessage::Text(text))) => {
                            let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
                                continue;
                            };
                            if in_tx.send(value).is_err() {
                                break;
                            }
                        }
                        Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                        _ => {}
                    },
                    value = out_rx.recv() => match value {
                        Some(value) if value["type"] == "__close" => {
                            let _ = socket.send(WsMessage::Close(None)).await;
                            break;
                        }
                        Some(value) => {
                            if socket
                                .send(WsMessage::Text(value.to_string().into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        None => break,
                    },
                    _ = beat.tick() => {
                        let beat = frame("heartbeat", json!({ "time": now_rfc3339() }));
                        if socket.send(WsMessage::Text(beat.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let mut node = Self {
            out,
            incoming,
            task,
        };
        node.next_type("welcome").await;
        node.next_type("desired_state").await;
        node
    }

    fn send(&self, value: Value) {
        let _ = self.out.send(value);
    }

    async fn next_frame(&mut self) -> Value {
        timeout(Duration::from_secs(3), self.incoming.recv())
            .await
            .expect("a node frame within 3 s")
            .expect("the node session is open")
    }

    async fn next_type(&mut self, kind: &str) -> Value {
        loop {
            let value = self.next_frame().await;
            if value["type"] == kind {
                return value;
            }
        }
    }

    /// Reads the next `stream_open` and answers it with `answer`.
    async fn expect_open(&mut self) -> Value {
        self.next_type("stream_open").await
    }

    fn disconnect(&self) {
        self.send(json!({ "type": "__close" }));
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    server: Server,
    owner: Client,
    operator: Client,
    operator2: Client,
    viewer: Client,
    node: Node,
    node_id: String,
    project_id: String,
    app_id: String,
    vm_id: String,
}

fn image() -> &'static str {
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000"
}

async fn role_client(server: &Server, name: &str, role: &str) -> Client {
    let email = format!("{name}@example.com");
    server.insert_user(&email, PASSWORD, role).await;
    let client = server.client();
    server.login(&client, &email, PASSWORD).await;
    client
}

async fn fixture(server: Server) -> Fixture {
    server.state.interactive.set_timings(Timings {
        terminal_open_wait: Duration::from_millis(150),
        console_open_wait: Duration::from_millis(80),
        recheck: Duration::from_millis(250),
        idle: Duration::from_secs(60),
        max_duration: Duration::from_secs(3600),
        ticket_ttl: Duration::from_secs(30),
        tick: Duration::from_millis(25),
        stream_close_wait: Duration::from_millis(150),
    });
    let owner = server.owner().await;
    let operator = role_client(&server, "operator", "operator").await;
    let operator2 = role_client(&server, "operator2", "operator").await;
    let viewer = role_client(&server, "viewer", "viewer").await;
    let node = enroll(&server, &owner, "node-interactive").await;
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "interactive", "name": "Interactive" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap().to_string();
    let app_id = format!("app_{}", &quoro_types::new_id(quoro_types::Prefix::Vm)[3..]);
    let vm_id = quoro_types::new_id(quoro_types::Prefix::Vm);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO apps (id, project_id, name, image, port, health_check, resources, placement, \
         status, created_at, updated_at) \
         VALUES (?, ?, 'web', ?, 8080, ?, ?, '{\"node_ids\":[]}', 'running', ?, ?)",
    )
    .bind(&app_id)
    .bind(&project_id)
    .bind(image())
    .bind(r#"{"path":"/healthz","interval_s":10,"timeout_s":2,"failures":3}"#)
    .bind(r#"{"vcpus":2,"memory_mb":512,"disk_gb":4}"#)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    insert_vm(
        &server,
        &vm_id,
        &project_id,
        Some(&app_id),
        &node.node_id,
        "running",
    )
    .await;
    let socket = Node::connect(&server, &node).await;
    Fixture {
        server,
        owner,
        operator,
        operator2,
        viewer,
        node: socket,
        node_id: node.node_id,
        project_id,
        app_id,
        vm_id,
    }
}

async fn insert_vm(
    server: &Server,
    vm_id: &str,
    project_id: &str,
    app_id: Option<&str>,
    node_id: &str,
    observed: &str,
) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'web', 2, 512, 4, ?, 'running', ?, NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(vm_id)
    .bind(project_id)
    .bind(app_id)
    .bind(node_id)
    .bind(image())
    .bind(observed)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// HTTP and WebSocket helpers
// ---------------------------------------------------------------------------

async fn terminal(
    server: &Server,
    client: &Client,
    app_id: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(server.url(&format!("/api/v1/apps/{app_id}/terminal-sessions")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn console(server: &Server, client: &Client, vm_id: &str, body: Value) -> reqwest::Response {
    client
        .post(server.url(&format!("/api/v1/vms/{vm_id}/console-sessions")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn ticket_of(res: reqwest::Response) -> Value {
    assert_eq!(res.status(), 201, "open: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// Polls an open request and answers its `stream_open` concurrently.
async fn open_and_answer<F>(
    node: &mut Node,
    request: F,
    answer: impl FnOnce(&Value) -> Value,
) -> (reqwest::Response, Value)
where
    F: Future<Output = reqwest::Response>,
{
    let mut open = None;
    let (response, ()) = tokio::join!(request, async {
        let frame = node.expect_open().await;
        node.send(answer(&frame));
        open = Some(frame);
    });
    (response, open.expect("the node saw the stream_open"))
}

async fn ws_upgrade(
    server: &Server,
    session_id: &str,
    ticket: &str,
    origin: Option<&str>,
) -> Result<Socket, u16> {
    let url = format!(
        "ws://{}/api/v1/interactive-sessions/{session_id}/ws?ticket={ticket}",
        server.address()
    );
    let mut request = url.into_client_request().unwrap();
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("Origin", origin.parse().unwrap());
    }
    let stream = TcpStream::connect(server.address()).await.unwrap();
    match tokio_tungstenite::client_async(request, stream).await {
        Ok((socket, response)) => {
            assert_eq!(response.status(), 101);
            Ok(socket)
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Err(response.status().as_u16())
        }
        Err(error) => panic!("websocket upgrade failed: {error}"),
    }
}

async fn ws_upgrade_ok(server: &Server, ticket: &Value, origin: Option<&str>) -> Socket {
    ws_upgrade(
        server,
        ticket["id"].as_str().unwrap(),
        ticket["ticket"].as_str().unwrap(),
        origin,
    )
    .await
    .expect("the upgrade succeeds")
}

async fn ws_next(socket: &mut Socket) -> WsMessage {
    // Generous bound: under a loaded CI runner a frame can take longer than
    // a few seconds; a healthy run never waits this long.
    timeout(Duration::from_secs(30), socket.next())
        .await
        .expect("a websocket frame within 30 s")
        .expect("the socket is open")
        .expect("a valid websocket frame")
}

/// Reads until a text control message of `kind` (`exit`, `closed`, `error`).
async fn ws_control(socket: &mut Socket, kind: &str) -> Value {
    loop {
        match ws_next(socket).await {
            WsMessage::Text(text) => {
                let value: Value = serde_json::from_str(text.as_str()).unwrap();
                if value["type"] == kind {
                    return value;
                }
            }
            WsMessage::Close(_) => panic!("the socket closed before a {kind} control"),
            _ => {}
        }
    }
}

async fn ws_binary(socket: &mut Socket) -> Vec<u8> {
    loop {
        match ws_next(socket).await {
            WsMessage::Binary(bytes) => return bytes.to_vec(),
            WsMessage::Close(_) => panic!("the socket closed before binary data"),
            _ => {}
        }
    }
}

async fn ws_send_binary(socket: &mut Socket, bytes: &[u8]) {
    socket
        .send(WsMessage::Binary(bytes.to_vec().into()))
        .await
        .unwrap();
}

async fn ws_send_text(socket: &mut Socket, value: Value) {
    socket
        .send(WsMessage::Text(value.to_string().into()))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

async fn session_row(
    server: &Server,
    id: &str,
) -> Option<(Option<String>, Option<i64>, Option<String>)> {
    sqlx::query_as(
        "SELECT ended_at, end_audit_seq, connected_at FROM interactive_sessions WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(server.state.store.pool())
    .await
    .unwrap()
}

async fn audit_of(
    server: &Server,
    operation: &str,
    target_id: &str,
) -> Vec<(String, Option<String>, String, String)> {
    sqlx::query_as(
        "SELECT result, result_detail, params, actor_id FROM audit_log \
         WHERE operation = ? AND target_id = ? ORDER BY seq",
    )
    .bind(operation)
    .bind(target_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

async fn wait_closed(socket: &mut Socket, reason: &str) -> Value {
    let closed = ws_control(socket, "closed").await;
    assert_eq!(closed["reason"], reason);
    closed
}

/// Polls for the `session.end` record of `target_id` (an actor writes it once
/// it has the node's `stream_end`, or its bounded wait elapsed) and returns
/// the record's params.
async fn end_params(server: &Server, target_id: &str) -> Value {
    for _ in 0..100 {
        let ends = audit_of(server, "session.end", target_id).await;
        if let Some((_, _, params, _)) = ends.last() {
            return serde_json::from_str(params).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no session.end within 2.5 s");
}

fn interrupted() -> Value {
    json!({ "rows": 24, "cols": 80 })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn owner_opens_a_terminal_relays_bytes_and_writes_session_end() {
    let mut f = fixture(start().await).await;
    let (res, open) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| stream_data(open["body"]["stream_id"].as_str().unwrap(), 1, b"ready\n"),
    )
    .await;
    assert_eq!(open["body"]["kind"], "terminal");
    assert_eq!(open["body"]["params"]["vm_id"], f.vm_id);
    assert_eq!(open["body"]["params"]["rows"], 24);
    assert_eq!(open["body"]["params"]["argv"], json!(["/bin/sh"]));
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    assert!(session_id.starts_with("trm_"), "{session_id}");
    assert_eq!(ticket["kind"], "terminal");
    assert_eq!(ticket["vm_id"], f.vm_id);
    assert_eq!(ticket["ticket"].as_str().unwrap().len(), 43);
    assert_eq!(
        ticket["websocket_path"],
        format!("/api/v1/interactive-sessions/{session_id}/ws")
    );
    assert!(ticket.get("expires_at").is_some());

    // The open record carries the resolved command, the session id and never
    // the ticket.
    let rows = audit_of(&f.server, APP_OPERATION, &f.app_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "ok");
    let params: Value = serde_json::from_str(&rows[0].2).unwrap();
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["command"], json!(["/bin/sh"]));
    assert!(params.get("ticket").is_none());
    let (_, _, connected_at) = session_row(&f.server, &session_id).await.unwrap();
    assert!(connected_at.is_none());

    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    // Data produced before the WebSocket attached was buffered, not acked.
    assert_eq!(ws_binary(&mut socket).await, b"ready\n");
    let stream_id = open["body"]["stream_id"].as_str().unwrap();
    let ack = f.node.next_type("stream_ack").await;
    assert_eq!(ack["body"]["stream_id"], stream_id);
    assert_eq!(ack["body"]["acked_bytes"], 6);

    // Input and resize become gap-free stream_input frames.
    ws_send_binary(&mut socket, b"ls\n").await;
    let input = f.node.next_type("stream_input").await;
    assert_eq!(input["body"]["seq"], 1);
    assert_eq!(
        input["body"]["data"],
        base64::engine::general_purpose::STANDARD.encode(b"ls\n")
    );
    ws_send_text(&mut socket, json!({"type":"resize","rows":50,"cols":132})).await;
    let resize = f.node.next_type("stream_input").await;
    assert_eq!(resize["body"]["seq"], 2);
    assert_eq!(resize["body"]["resize"], json!({"rows":50,"cols":132}));

    // Live output reaches the WebSocket.
    f.node.send(stream_data(stream_id, 2, b"output\n"));
    assert_eq!(ws_binary(&mut socket).await, b"output\n");

    // The program exits.
    f.node.send(frame(
        "stream_end",
        json!({
            "stream_id": stream_id,
            "reason": "completed",
            "exit_code": 3,
            "transcript_sha256": "a".repeat(64),
            "transcript_bytes": 42
        }),
    ));
    let exit = ws_control(&mut socket, "exit").await;
    assert_eq!(exit["code"], 3);
    wait_closed(&mut socket, "exited").await;

    let (ended_at, end_audit_seq, connected_at) =
        session_row(&f.server, &session_id).await.unwrap();
    assert!(ended_at.is_some());
    assert!(end_audit_seq.is_some());
    assert!(connected_at.is_some());
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0].0, "ok");
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["kind"], "terminal");
    assert_eq!(params["close_reason"], "exited");
    assert_eq!(params["exit_code"], 3);
    assert_eq!(params["connected"], true);
    assert_eq!(params["bytes_in"], 3);
    assert_eq!(params["bytes_out"], 13);
    assert_eq!(params["transcript_sha256"], "a".repeat(64));
    assert_eq!(params["transcript_bytes"], 42);
    assert!(params.get("ended_by").is_none());
}

#[tokio::test]
async fn viewer_is_refused_and_no_row_exists() {
    let f = fixture(start().await).await;
    let (status, code, reason) =
        problem(terminal(&f.server, &f.viewer, &f.app_id, interrupted()).await).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("scope_missing"))
    );
    let rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM interactive_sessions")
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows.0, 0);
}

async fn owner_id(owner: &Client, server: &Server) -> String {
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

async fn create_agent(owner: &Client, server: &Server, owner_user_id: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "shell-agent",
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "tests"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    body["id"].as_str().unwrap().to_string()
}

async fn access_token(owner: &Client, server: &Server, agent_id: &str) -> String {
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    let key = body["key"].as_str().unwrap();
    let res = Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

async fn create_grant(
    owner: &Client,
    server: &Server,
    agent_id: &str,
    scopes: &[&str],
    selector: Value,
) -> Value {
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": scopes,
            "selector": selector,
            "ttl_s": 3600,
            "reason": "tests"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "grant: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

async fn grant_uses(server: &Server, grant_id: &str) -> i64 {
    sqlx::query_scalar("SELECT uses FROM grants WHERE id = ?")
        .bind(grant_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn an_agent_is_human_only_for_a_terminal_and_the_refusal_is_audited() {
    let f = fixture(start().await).await;
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    let token = access_token(&f.owner, &f.server, &agent_id).await;
    let res = Client::new()
        .post(
            f.server
                .url(&format!("/api/v1/apps/{}/terminal-sessions", f.app_id)),
        )
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "look at it")
        .json(&interrupted())
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("human_only"))
    );
    let rows = audit_of(&f.server, APP_OPERATION, &f.app_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "refused");
    assert_eq!(rows[0].1.as_deref(), Some("human_only"));
    assert_eq!(rows[0].3, agent_id);
}

#[tokio::test]
async fn an_agent_console_needs_a_justification_and_consumes_one_grant_use() {
    let mut f = fixture(start().await).await;
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    let grant = create_grant(
        &f.owner,
        &f.server,
        &agent_id,
        &["vm:console"],
        json!({ "apps": [&f.app_id] }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let token = access_token(&f.owner, &f.server, &agent_id).await;

    let res = Client::new()
        .post(
            f.server
                .url(&format!("/api/v1/vms/{}/console-sessions", f.vm_id)),
        )
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert_eq!(grant_uses(&f.server, &grant_id).await, 0);

    let (res, open) = open_and_answer(
        &mut f.node,
        async {
            Client::new()
                .post(
                    f.server
                        .url(&format!("/api/v1/vms/{}/console-sessions", f.vm_id)),
                )
                .header("Authorization", format!("Bearer {token}"))
                .header("Quoro-Justification", "read the boot output")
                .json(&json!({ "tail_bytes": 4096 }))
                .send()
                .await
                .unwrap()
        },
        |open| stream_data(open["body"]["stream_id"].as_str().unwrap(), 1, b"boot\n"),
    )
    .await;
    assert_eq!(open["body"]["kind"], "console");
    assert_eq!(open["body"]["params"]["tail_bytes"], 4096);
    let opened = audit_of(&f.server, CONSOLE_OPERATION, &f.vm_id).await;
    // The 422 without a justification wrote an error record; the open follows.
    assert_eq!(opened.len(), 2);
    assert_eq!(opened.last().unwrap().0, "ok");
    let ticket = ticket_of(res).await;
    assert_eq!(grant_uses(&f.server, &grant_id).await, 1);
    let mut socket = ws_upgrade_ok(&f.server, &ticket, None).await;
    assert_eq!(ws_binary(&mut socket).await, b"boot\n");
    // The re-check loop consumes no grant use.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(grant_uses(&f.server, &grant_id).await, 1);
    let stream_id = open["body"]["stream_id"].as_str().unwrap();
    f.node.send(stream_end(stream_id, "vm_stopped", json!({})));
    wait_closed(&mut socket, "vm_stopped").await;
}

#[tokio::test]
async fn a_node_without_a_session_is_unreachable() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enroll(&server, &owner, "node-offline").await;
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({"slug":"offline","name":"Offline"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap().to_string();
    let app_id = format!("app_{}", &quoro_types::new_id(quoro_types::Prefix::Vm)[3..]);
    let vm_id = quoro_types::new_id(quoro_types::Prefix::Vm);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO apps (id, project_id, name, image, port, health_check, resources, placement, status, created_at, updated_at) \
         VALUES (?, ?, 'web', ?, 8080, ?, ?, '{\"node_ids\":[]}', 'running', ?, ?)",
    )
    .bind(&app_id)
    .bind(&project_id)
    .bind(image())
    .bind(r#"{"path":"/healthz","interval_s":10,"timeout_s":2,"failures":3}"#)
    .bind(r#"{"vcpus":2,"memory_mb":512,"disk_gb":4}"#)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    insert_vm(
        &server,
        &vm_id,
        &project_id,
        Some(&app_id),
        &node.node_id,
        "running",
    )
    .await;
    let (status, code, _) = problem(terminal(&server, &owner, &app_id, interrupted()).await).await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));
    let rows = audit_of(&server, APP_OPERATION, &app_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "error");
    assert_eq!(rows[0].1.as_deref(), Some("node_unreachable"));
}

#[tokio::test]
async fn a_failed_node_open_is_a_conflict_and_creates_no_session() {
    let mut f = fixture(start().await).await;
    let (res, _) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| command_error(open, "not_available", "the VM was started before M4"),
    )
    .await;
    assert_eq!(res.status(), 409);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["code"], "conflict");
    assert!(body["detail"].as_str().unwrap().contains("M4"));
    let rows = audit_of(&f.server, APP_OPERATION, &f.app_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "error");
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM interactive_sessions")
        .fetch_one(f.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
async fn tickets_are_single_use_for_one_session_only() {
    let f = fixture(start().await).await;
    let first = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let second = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    // A ticket for another session is never accepted.
    let status = ws_upgrade(
        &f.server,
        second["id"].as_str().unwrap(),
        first["ticket"].as_str().unwrap(),
        Some(ORIGIN),
    )
    .await
    .expect_err("a foreign ticket");
    assert_eq!(status, 401);
    let mut socket = ws_upgrade_ok(&f.server, &first, Some(ORIGIN)).await;
    // The same ticket again on the session that already has its socket.
    let status = ws_upgrade(
        &f.server,
        first["id"].as_str().unwrap(),
        first["ticket"].as_str().unwrap(),
        Some(ORIGIN),
    )
    .await
    .expect_err("a second websocket");
    assert_eq!(status, 409);
    // The other session's ticket was not consumed by the wrong-session call.
    let mut other = ws_upgrade_ok(&f.server, &second, Some(ORIGIN)).await;
    let _ = (&mut socket, &mut other);
}

#[tokio::test]
async fn an_expired_ticket_is_refused_and_closes_the_session() {
    let f = fixture(start().await).await;
    f.server.state.interactive.set_timings(Timings {
        ticket_ttl: Duration::from_millis(250),
        ..Timings::default()
    });
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let status = ws_upgrade(
        &f.server,
        ticket["id"].as_str().unwrap(),
        ticket["ticket"].as_str().unwrap(),
        Some(ORIGIN),
    )
    .await
    .expect_err("an expired ticket");
    assert_eq!(status, 401);
    let session_id = ticket["id"].as_str().unwrap();
    let (ended_at, end_audit_seq, _) = session_row(&f.server, session_id).await.unwrap();
    assert!(ended_at.is_some());
    assert!(end_audit_seq.is_some());
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "ticket_expired");
    assert_eq!(params["connected"], false);
}

#[tokio::test]
async fn origin_rules_and_a_spent_ticket() {
    let f = fixture(start().await).await;
    // A ticket minted with the panel session cookie needs an Origin.
    let cookie_ticket =
        ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let status = ws_upgrade(
        &f.server,
        cookie_ticket["id"].as_str().unwrap(),
        cookie_ticket["ticket"].as_str().unwrap(),
        None,
    )
    .await
    .expect_err("a cookie ticket needs an Origin");
    assert_eq!(status, 403);
    // A mismatch is refused and burns the ticket; a replay is then unknown.
    let other = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let status = ws_upgrade(
        &f.server,
        other["id"].as_str().unwrap(),
        other["ticket"].as_str().unwrap(),
        Some("https://evil.example"),
    )
    .await
    .expect_err("a cross-site origin");
    assert_eq!(status, 403);
    let status = ws_upgrade(
        &f.server,
        other["id"].as_str().unwrap(),
        other["ticket"].as_str().unwrap(),
        Some(ORIGIN),
    )
    .await
    .expect_err("a spent ticket");
    assert_eq!(status, 401);
}

#[tokio::test]
async fn a_console_refuses_input_and_carries_no_transcript() {
    let mut f = fixture(start().await).await;
    let (res, open) = open_and_answer(
        &mut f.node,
        console(&f.server, &f.owner, &f.vm_id, json!({})),
        |open| {
            stream_data(
                open["body"]["stream_id"].as_str().unwrap(),
                1,
                b"linux boot\n",
            )
        },
    )
    .await;
    let ticket = ticket_of(res).await;
    assert_eq!(ticket["kind"], "console");
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    assert_eq!(ws_binary(&mut socket).await, b"linux boot\n");
    ws_send_binary(&mut socket, b"whoami\n").await;
    let error = ws_control(&mut socket, "error").await;
    assert!(error["message"].as_str().unwrap().contains("read-only"));
    // No stream_input reached the node.
    let stream_id = open["body"]["stream_id"].as_str().unwrap();
    f.node.send(stream_end(stream_id, "vm_stopped", json!({})));
    wait_closed(&mut socket, "vm_stopped").await;
    let session_id = ticket["id"].as_str().unwrap();
    let ends = audit_of(&f.server, "session.end", &f.vm_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["bytes_in"], 0);
    assert!(params.get("transcript_sha256").is_none());
}

#[tokio::test]
async fn delete_is_allowed_for_the_opener_and_an_owner_and_refused_otherwise() {
    let f = fixture(start().await).await;
    let first = ticket_of(terminal(&f.server, &f.operator, &f.app_id, interrupted()).await).await;
    let mut socket = ws_upgrade_ok(&f.server, &first, Some(ORIGIN)).await;
    let session_id = first["id"].as_str().unwrap().to_string();

    // Another operator may not end it; the refusal is audited.
    let res = f
        .operator2
        .delete(
            f.server
                .url(&format!("/api/v1/interactive-sessions/{session_id}")),
        )
        .send()
        .await
        .unwrap();
    let (status, _, reason) = problem(res).await;
    assert_eq!((status, reason.as_deref()), (403, Some("scope_missing")));
    let rows = audit_of(
        &f.server,
        "DELETE /api/v1/interactive-sessions/{session_id}",
        &f.app_id,
    )
    .await;
    assert_eq!(rows.last().unwrap().0, "refused");

    // The owner may.
    let res = f
        .owner
        .delete(
            f.server
                .url(&format!("/api/v1/interactive-sessions/{session_id}")),
        )
        .header("Quoro-Justification", "closing it")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let request_id = res
        .headers()
        .get("quoro-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    wait_closed(&mut socket, "ended_by_request").await;
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    assert_eq!(ends.len(), 1);
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "ended_by_request");
    let owner_user = owner_id(&f.owner, &f.server).await;
    let opener = owner_id(&f.operator, &f.server).await;
    assert_eq!(params["ended_by"], json!(owner_user));
    // The record repeats the opener's accountability fields, not the caller's.
    assert_eq!(
        rows_actor(&f.server, "session.end", &f.app_id).await,
        opener
    );
    // The end record repeats the DELETE's request id.
    let end_request: (String,) = sqlx::query_as(
        "SELECT request_id FROM audit_log WHERE operation = 'session.end' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(f.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(end_request.0, request_id);

    // A second DELETE finds nothing.
    let res = f
        .owner
        .delete(
            f.server
                .url(&format!("/api/v1/interactive-sessions/{session_id}")),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);

    // The opener may end its own session.
    let second = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let id = second["id"].as_str().unwrap();
    let res = f
        .owner
        .delete(f.server.url(&format!("/api/v1/interactive-sessions/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204, "{}", res.text().await.unwrap());
}

#[tokio::test]
async fn ending_a_terminal_by_request_records_the_transcript_the_node_reports() {
    let mut f = fixture(start().await).await;
    let (res, open) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| stream_data(open["body"]["stream_id"].as_str().unwrap(), 1, b"$ \n"),
    )
    .await;
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    assert_eq!(ws_binary(&mut socket).await, b"$ \n");
    let stream_id = open["body"]["stream_id"].as_str().unwrap().to_string();
    let sha = "b".repeat(64);

    // The primary asks the node to stop the stream and waits for the
    // `stream_end` that carries the transcript it then records.
    let (res, ()) = tokio::join!(
        async {
            f.owner
                .delete(
                    f.server
                        .url(&format!("/api/v1/interactive-sessions/{session_id}")),
                )
                .send()
                .await
                .unwrap()
        },
        async {
            let close = f.node.next_type("stream_close").await;
            assert_eq!(close["body"]["stream_id"], stream_id);
            f.node.send(stream_end(
                &stream_id,
                "cancelled",
                json!({ "transcript_sha256": sha.clone(), "transcript_bytes": 11 }),
            ));
        }
    );
    assert_eq!(res.status(), 204);
    wait_closed(&mut socket, "ended_by_request").await;

    let params = end_params(&f.server, &f.app_id).await;
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["close_reason"], "ended_by_request");
    assert_eq!(params["transcript_sha256"], sha);
    assert_eq!(params["transcript_bytes"], 11);
}

#[tokio::test]
async fn a_client_that_closes_its_socket_still_records_the_transcript() {
    let mut f = fixture(start().await).await;
    let (res, open) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| stream_data(open["body"]["stream_id"].as_str().unwrap(), 1, b"$ \n"),
    )
    .await;
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    assert_eq!(ws_binary(&mut socket).await, b"$ \n");
    let stream_id = open["body"]["stream_id"].as_str().unwrap().to_string();
    let sha = "c".repeat(64);

    socket.send(WsMessage::Close(None)).await.unwrap();
    let close = f.node.next_type("stream_close").await;
    assert_eq!(close["body"]["stream_id"], stream_id);
    f.node.send(stream_end(
        &stream_id,
        "cancelled",
        json!({ "transcript_sha256": sha.clone(), "transcript_bytes": 23 }),
    ));

    let params = end_params(&f.server, &f.app_id).await;
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["close_reason"], "ended_by_client");
    assert_eq!(params["transcript_sha256"], sha);
    assert_eq!(params["transcript_bytes"], 23);
}

#[tokio::test]
async fn a_node_that_never_answers_stream_close_does_not_hang_session_end() {
    let mut f = fixture(start().await).await;
    f.server.state.interactive.set_timings(Timings {
        terminal_open_wait: Duration::from_millis(150),
        recheck: Duration::from_secs(5),
        tick: Duration::from_millis(25),
        stream_close_wait: Duration::from_millis(150),
        ..Timings::default()
    });
    let (res, open) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| stream_data(open["body"]["stream_id"].as_str().unwrap(), 1, b"$ \n"),
    )
    .await;
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    assert_eq!(ws_binary(&mut socket).await, b"$ \n");
    let stream_id = open["body"]["stream_id"].as_str().unwrap().to_string();

    let started = std::time::Instant::now();
    let res = f
        .owner
        .delete(
            f.server
                .url(&format!("/api/v1/interactive-sessions/{session_id}")),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the close waited for a StreamEnd that never came"
    );
    // The node did get the request; it just does not answer it.
    let close = f.node.next_type("stream_close").await;
    assert_eq!(close["body"]["stream_id"], stream_id);
    wait_closed(&mut socket, "ended_by_request").await;

    let params = end_params(&f.server, &f.app_id).await;
    assert_eq!(params["session_id"], session_id);
    assert_eq!(params["close_reason"], "ended_by_request");
    assert!(params.get("transcript_sha256").is_none());
    assert!(params.get("transcript_bytes").is_none());
}

async fn rows_actor(server: &Server, operation: &str, target_id: &str) -> String {
    let row: (String,) = sqlx::query_as(
        "SELECT actor_id FROM audit_log WHERE operation = ? AND target_id = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(operation)
    .bind(target_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    row.0
}

#[tokio::test]
async fn idle_timeout_and_max_duration_close_a_session() {
    let f = fixture(start().await).await;
    f.server.state.interactive.set_timings(Timings {
        idle: Duration::from_millis(250),
        max_duration: Duration::from_secs(3600),
        tick: Duration::from_millis(25),
        recheck: Duration::from_secs(5),
        stream_close_wait: Duration::from_millis(150),
        ..Timings::default()
    });
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    wait_closed(&mut socket, "idle_timeout").await;
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "idle_timeout");

    f.server.state.interactive.set_timings(Timings {
        idle: Duration::from_secs(60),
        max_duration: Duration::from_millis(300),
        tick: Duration::from_millis(25),
        recheck: Duration::from_secs(5),
        stream_close_wait: Duration::from_millis(150),
        ..Timings::default()
    });
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    wait_closed(&mut socket, "max_duration").await;
}

#[tokio::test]
async fn a_credential_that_expires_closes_the_session() {
    let f = fixture(start().await).await;
    f.server.state.interactive.set_timings(Timings {
        recheck: Duration::from_millis(100),
        tick: Duration::from_millis(25),
        stream_close_wait: Duration::from_millis(150),
        ..Timings::default()
    });
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    sqlx::query("UPDATE sessions SET expires_at = ?")
        .bind(now_rfc3339())
        .execute(f.server.state.store.pool())
        .await
        .unwrap();
    wait_closed(&mut socket, "credential_expired").await;
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "credential_expired");
}

#[tokio::test]
async fn revoking_the_grant_closes_an_agent_console() {
    let f = fixture(start().await).await;
    f.server.state.interactive.set_timings(Timings {
        recheck: Duration::from_millis(100),
        tick: Duration::from_millis(25),
        ..Timings::default()
    });
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    let grant = create_grant(
        &f.owner,
        &f.server,
        &agent_id,
        &["vm:console"],
        json!({ "all": true }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let token = access_token(&f.owner, &f.server, &agent_id).await;
    let res = Client::new()
        .post(
            f.server
                .url(&format!("/api/v1/vms/{}/console-sessions", f.vm_id)),
        )
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "read the boot output")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let ticket = ticket_of(res).await;
    let mut socket = ws_upgrade_ok(&f.server, &ticket, None).await;
    let res = f
        .owner
        .post(f.server.url(&format!("/api/v1/grants/{grant_id}/revoke")))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "revoke: {}", res.text().await.unwrap());
    wait_closed(&mut socket, "access_revoked").await;
    let ends = audit_of(&f.server, "session.end", &f.vm_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "access_revoked");
    assert_eq!(ends[0].1, None);
}

#[tokio::test]
async fn a_node_disconnect_is_recorded_as_an_error() {
    let f = fixture(start().await).await;
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    f.node.disconnect();
    wait_closed(&mut socket, "node_disconnected").await;
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    let params: Value = serde_json::from_str(&ends[0].2).unwrap();
    assert_eq!(params["close_reason"], "node_disconnected");
    assert_eq!(ends[0].0, "error");
    assert_eq!(ends[0].1.as_deref(), Some("node_unreachable"));
}

#[tokio::test]
async fn orphan_sessions_are_closed_on_startup() {
    let f = fixture(start().await).await;
    let ticket = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    // Pretend the row came from a previous process: open, not ended.
    sqlx::query("UPDATE interactive_sessions SET connected_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&session_id)
        .execute(f.server.state.store.pool())
        .await
        .unwrap();
    // A second start-up over the same database closes it.
    let store = quoro_control_plane::store::Store::open(&f.server.data_dir.join("quoro.db"))
        .await
        .unwrap();
    let config = (*f.server.state.config).clone();
    let _state = quoro_control_plane::AppState::open(
        store,
        config,
        std::sync::Arc::new(quoro_control_plane::ReferenceAsDigest),
        std::sync::Arc::new(quoro_control_plane::crypto::DataKey::ephemeral()),
    )
    .await
    .unwrap();
    let (ended_at, end_audit_seq, _) = session_row(&f.server, &session_id).await.unwrap();
    assert!(ended_at.is_some());
    assert!(end_audit_seq.is_some());
    let ends = audit_of(&f.server, "session.end", &f.app_id).await;
    let params: Value = serde_json::from_str(&ends.last().unwrap().2).unwrap();
    assert_eq!(params["close_reason"], "primary_restarted");
    assert_eq!(params["connected"], true);
    assert_eq!(params["bytes_in"], 0);
    assert_eq!(params["bytes_out"], 0);
}

#[tokio::test]
async fn transcript_fetch_relays_the_file_and_is_audited() {
    let mut f = fixture(start().await).await;
    let (res, open) = open_and_answer(
        &mut f.node,
        terminal(&f.server, &f.owner, &f.app_id, interrupted()),
        |open| {
            stream_data(
                open["body"]["stream_id"].as_str().unwrap(),
                1,
                b"$ echo hi\nhi\n",
            )
        },
    )
    .await;
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    let mut socket = ws_upgrade_ok(&f.server, &ticket, Some(ORIGIN)).await;
    assert_eq!(ws_binary(&mut socket).await, b"$ echo hi\nhi\n");
    let stream_id = open["body"]["stream_id"].as_str().unwrap();
    let transcript = b"{\"version\":2}\n[0.0,\"o\",\"hi\\n\"]\n";
    f.node.send(stream_end(
        stream_id,
        "completed",
        json!({
            "exit_code": 0,
            "transcript_sha256": sha256_hex(transcript),
            "transcript_bytes": transcript.len(),
        }),
    ));
    ws_control(&mut socket, "exit").await;
    wait_closed(&mut socket, "exited").await;

    // A terminal's transcript is streamed from the node as asciicast.
    let handle = tokio::spawn({
        let client = f.owner.clone();
        let url = f.server.url(&format!(
            "/api/v1/interactive-sessions/{session_id}/transcript"
        ));
        async move { client.get(url).send().await.unwrap() }
    });
    let open = f.node.next_type("stream_open").await;
    assert_eq!(open["body"]["kind"], "transcript");
    assert_eq!(open["body"]["params"]["session_id"], session_id);
    let id = open["body"]["stream_id"].as_str().unwrap();
    f.node.send(stream_data(id, 1, transcript));
    f.node.send(stream_end(id, "completed", json!({})));
    let res = handle.await.unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers().get("content-type").unwrap(),
        "application/x-asciicast"
    );
    assert_eq!(res.bytes().await.unwrap().as_ref(), transcript);
    let audit = audit_of(
        &f.server,
        "GET /api/v1/interactive-sessions/{session_id}/transcript",
        &f.vm_id,
    )
    .await;
    assert_eq!(audit.last().unwrap().0, "ok");
    let params: Value = serde_json::from_str(&audit.last().unwrap().2).unwrap();
    assert_eq!(params["session_id"], session_id);

    // An agent may not read a transcript, even with `audit:read`.
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    create_grant(
        &f.owner,
        &f.server,
        &agent_id,
        &["audit:read"],
        json!({ "all": true }),
    )
    .await;
    let token = access_token(&f.owner, &f.server, &agent_id).await;
    let res = Client::new()
        .get(f.server.url(&format!(
            "/api/v1/interactive-sessions/{session_id}/transcript"
        )))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("human_only"))
    );

    // A console session has no transcript.
    let console_ticket = ticket_of(console(&f.server, &f.owner, &f.vm_id, json!({})).await).await;
    let console_id = console_ticket["id"].as_str().unwrap();
    let res = f
        .owner
        .get(f.server.url(&format!(
            "/api/v1/interactive-sessions/{console_id}/transcript"
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
}

#[tokio::test]
async fn bodies_are_validated_like_the_schema() {
    let f = fixture(start().await).await;
    for body in [
        json!({ "rows": 0, "cols": 80 }),
        json!({ "rows": 24, "cols": 1001 }),
        json!({ "rows": 24, "cols": 80, "command": [] }),
        json!({ "rows": 24, "cols": 80, "command": vec!["x"; 65] }),
        json!({ "rows": 24, "cols": 80, "command": [""] }),
        json!({ "rows": 24, "cols": 80, "user": "u".repeat(65) }),
        json!({ "rows": 24, "cols": 80, "vm_id": "vm_bad" }),
    ] {
        let res = f
            .owner
            .post(
                f.server
                    .url(&format!("/api/v1/apps/{}/terminal-sessions", f.app_id)),
            )
            .json(&body)
            .send()
            .await
            .unwrap();
        let (status, code, _) = problem(res).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{body}"
        );
    }
    // A VM of another app is not found.
    let other_app = format!("app_{}", &quoro_types::new_id(quoro_types::Prefix::Vm)[3..]);
    let other_vm = quoro_types::new_id(quoro_types::Prefix::Vm);
    insert_vm(
        &f.server,
        &other_vm,
        &f.project_id,
        None,
        &f.node_id,
        "running",
    )
    .await;
    let res = terminal(
        &f.server,
        &f.owner,
        &f.app_id,
        json!({ "rows": 24, "cols": 80, "vm_id": other_vm }),
    )
    .await;
    assert_eq!(res.status(), 404);
    // No running instance: conflict.
    sqlx::query("UPDATE vms SET observed_status = 'stopped' WHERE app_id = ?")
        .bind(&f.app_id)
        .execute(f.server.state.store.pool())
        .await
        .unwrap();
    let res = terminal(&f.server, &f.owner, &f.app_id, interrupted()).await;
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    let _ = other_app;
}

#[tokio::test]
async fn an_agent_ends_its_console_only_with_a_justification() {
    let f = fixture(start().await).await;
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    create_grant(
        &f.owner,
        &f.server,
        &agent_id,
        &["vm:console"],
        json!({ "all": true }),
    )
    .await;
    let token = access_token(&f.owner, &f.server, &agent_id).await;
    let agent = Client::new();
    let res = agent
        .post(
            f.server
                .url(&format!("/api/v1/vms/{}/console-sessions", f.vm_id)),
        )
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "read the boot output")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let ticket = ticket_of(res).await;
    let session_id = ticket["id"].as_str().unwrap().to_string();
    let mut socket = ws_upgrade_ok(&f.server, &ticket, None).await;
    let url = f
        .server
        .url(&format!("/api/v1/interactive-sessions/{session_id}"));

    // Without a justification the agent's DELETE is refused and audited.
    let res = agent
        .delete(url.clone())
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
    let rows = audit_of(
        &f.server,
        "DELETE /api/v1/interactive-sessions/{session_id}",
        &f.vm_id,
    )
    .await;
    assert_eq!(rows.last().unwrap().0, "error");

    // With one it ends the session, and session.end carries the reason.
    let res = agent
        .delete(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "done reading")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    wait_closed(&mut socket, "ended_by_request").await;
    let justification: (Option<String>,) = sqlx::query_as(
        "SELECT justification FROM audit_log WHERE operation = 'session.end' AND target_id = ?",
    )
    .bind(&f.vm_id)
    .fetch_one(f.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(justification.0.as_deref(), Some("done reading"));
}

#[tokio::test]
async fn an_agent_transcript_fetch_is_refused_naming_the_vm() {
    let f = fixture(start().await).await;
    let first = ticket_of(terminal(&f.server, &f.owner, &f.app_id, interrupted()).await).await;
    let session_id = first["id"].as_str().unwrap().to_string();
    let owner = owner_id(&f.owner, &f.server).await;
    let agent_id = create_agent(&f.owner, &f.server, &owner).await;
    create_grant(
        &f.owner,
        &f.server,
        &agent_id,
        &["audit:read"],
        json!({ "all": true }),
    )
    .await;
    let token = access_token(&f.owner, &f.server, &agent_id).await;
    let res = Client::new()
        .get(f.server.url(&format!(
            "/api/v1/interactive-sessions/{session_id}/transcript"
        )))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let (status, _, reason) = problem(res).await;
    assert_eq!((status, reason.as_deref()), (403, Some("human_only")));
    let vm_id = first["vm_id"].as_str().unwrap();
    let rows = audit_of(
        &f.server,
        "GET /api/v1/interactive-sessions/{session_id}/transcript",
        vm_id,
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "refused");
}
