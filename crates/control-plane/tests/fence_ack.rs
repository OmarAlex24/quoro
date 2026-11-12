//! Failed fences and their acknowledgement (`node-agent-protocol.md` sections
//! 4.2 rule 4, 5.17 and 8.5; D-0074, D-0079; issue #391), and declaring a node
//! lost (section 8.3, ADR 0183, ADR 0202; issue #734), over real HTTP and
//! WebSocket with a fake node.

mod common;

use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::session::Fault;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, Kvm as WireKvm, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorBody, ErrorCode};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";
const UNKNOWN_LEASE: &str = "lse_01JC9Z6Q5W8N3K4T7R2M1XHB0D";
const OTHER_LEASE: &str = "lse_01JC9Z6Q5W8N3K4T7R2M1XHB0E";
const SECOND_LEASE: &str = "lse_01JC9Z6Q5W8N3K4T7R2M1XHB0F";
const SECOND_APP: &str = "app_01JC9Z6Q5W8N3K4T7R2M1XHB0F";

struct Enrolled {
    node_id: String,
    credential: String,
}

fn hello(node_id: &str, leases: Vec<wire::HelloLease>) -> NodeMessage {
    NodeMessage::Hello(wire::Hello {
        node_id: node_id.to_string(),
        agent_version: "test-agent".to_string(),
        engine_version: "test-engine".to_string(),
        boot_id: "boot-1".to_string(),
        stored_rev: 0,
        applied_rev: 0,
        log_seq: 0,
        log_hash: "0".repeat(64),
        leases,
        mesh_public_key: None,
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        volume_snapshot: Some(wire::SnapshotMethod::Reflink),
        custody_public_key: None,
        mesh_rules: None,
    })
}

fn hello_lease(lease_id: &str, app_id: &str, token: u64, state: &str) -> wire::HelloLease {
    wire::HelloLease {
        lease_id: lease_id.to_string(),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: app_id.to_string(),
        mode: quoro_proto::LeaseMode::Local,
        fencing_token: token,
        state: state.to_string(),
    }
}

async fn enrolled_node(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    // Section 8.10 rule 2: these tests enroll voters back to back.
    common::settle_voters(server).await;
    let token: Value = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"].as_str().unwrap(),
            "name": name,
            "public_key": quoro_proto::sign::public_key_to_base64(
                &quoro_proto::sign::signing_key_from_seed(&[3u8; 32]).verifying_key()
            ),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
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

async fn send(socket: &mut Socket, message: NodeMessage) {
    reply(socket, message, None).await;
}

async fn reply(socket: &mut Socket, message: NodeMessage, re: Option<&str>) {
    let text = message
        .to_frame(&format!("msg_{}", ulid_like()), re)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

/// A fresh 26-character suffix, so every frame of the fake node has its own id.
fn ulid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "01JC9Z6Q5W8N3K4T7R2M{:06}",
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

async fn heartbeat(socket: &mut Socket) {
    send(
        socket,
        NodeMessage::Heartbeat(Heartbeat {
            time: "2026-11-02T10:15:32Z".to_string(),
            busy: None,
        }),
    )
    .await;
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(30), socket.next())
            .await
            .expect("a frame within 30 s")
            .expect("the socket is open")
            .expect("a valid websocket frame");
        match message {
            WsMessage::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

/// Reads frames until one of each `wanted` type was seen, and returns them in
/// arrival order. Earlier frames of other types are kept in the result.
async fn recv_until(socket: &mut Socket, wanted: &[&str]) -> Vec<Value> {
    let mut remaining: Vec<&str> = wanted.to_vec();
    let mut seen = Vec::new();
    while !remaining.is_empty() {
        let frame = recv_frame(socket).await;
        let kind = frame["type"].as_str().unwrap_or_default().to_string();
        if let Some(index) = remaining.iter().position(|wanted| *wanted == kind) {
            remaining.remove(index);
        }
        seen.push(frame);
    }
    seen
}

fn of_type<'a>(frames: &'a [Value], kind: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == kind)
        .collect()
}

fn ok_result() -> NodeMessage {
    NodeMessage::CommandResult(CommandResult {
        status: CommandStatus::Ok,
        detail: None,
        error: None,
        lines: None,
        metrics: None,
        snapshot_seq: None,
    })
}

fn fence_failed_result() -> NodeMessage {
    NodeMessage::CommandResult(CommandResult {
        status: CommandStatus::Error,
        detail: Some("vm_x may still run".to_string()),
        error: Some(ErrorBody {
            code: ErrorCode::FenceFailed,
            message: "the forced stop could not be confirmed".to_string(),
        }),
        lines: None,
        metrics: None,
        snapshot_seq: None,
    })
}

fn fence_failed_event(node_id: &str, lease_id: &str, app_id: &str, vm_id: &str) -> NodeMessage {
    NodeMessage::Event(wire::EventBody {
        schema_version: 1,
        id: format!("evt_{}", ulid_like()),
        kind: "node.fence_failed".to_string(),
        time: "2026-11-06T10:15:32.000000000Z".to_string(),
        source: json!({ "kind": "node", "node_id": node_id }),
        subject: json!({ "type": "lease", "id": lease_id }),
        project_id: None,
        incident_id: None,
        request_id: None,
        actor: Some(json!({ "type": "node", "id": node_id })),
        data: json!({
            "node_id": node_id,
            "app_ids": [app_id],
            "lease_ids": [lease_id],
            "vm_ids": [vm_id],
            "reason": "lease_revoked",
            "error": "vm_x: the VMM did not exit",
        }),
    })
}

fn status_report(applied_rev: u64, volumes: Vec<VolumeStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev,
        vms: Vec::new(),
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        disk_free_gb: 90.0,
        builds: Vec::new(),
        edge_backends: Vec::new(),
        backups: Vec::new(),
        images: Vec::new(),
        volumes,
        warm: Vec::new(),
        quarantines: Vec::new(),
    })
}

async fn get_node(owner: &reqwest::Client, server: &Server, node_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

/// Polls the node until `ready` holds, sending heartbeats meanwhile.
async fn wait_for_node(
    owner: &reqwest::Client,
    server: &Server,
    socket: Option<&mut Socket>,
    node_id: &str,
    what: &str,
    ready: impl Fn(&Value) -> bool,
) -> Value {
    let mut socket = socket;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let node = get_node(owner, server, node_id).await;
        if ready(&node) {
            return node;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node never showed {what}: {node}"
        );
        if let Some(socket) = socket.as_deref_mut() {
            heartbeat(socket).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn fence_ack_pending(server: &Server, node_id: &str) -> Option<Value> {
    sqlx::query_scalar::<_, Option<String>>("SELECT fence_ack_pending FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
        .map(|text| serde_json::from_str(&text).unwrap())
}

async fn wait_for_no_pending_ack(server: &Server, socket: &mut Socket, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while fence_ack_pending(server, node_id).await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fence_ack_pending was never cleared"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The change log's newest `seq`, 0 for an empty log.
async fn log_head(server: &Server) -> i64 {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    head.unwrap_or(0)
}

/// Asserts that `node`'s `log_lag` is the newest `seq` when the response was
/// built minus its `log_seq`: above `before`, read before the call that wrote,
/// and at most `after` (node-agent-protocol.md section 7.1, ADR 0301).
fn assert_log_lag(node: &Value, before: i64, after: i64) {
    let reported =
        node["log_seq"].as_i64().expect("log_seq") + node["log_lag"].as_i64().expect("log_lag");
    assert!(
        before < reported && reported <= after,
        "{before} < {reported} <= {after}: {node}"
    );
    // Every `Node` response carries `custody` too (section 7.2, "Status"):
    // this machine has the `node` role and reported no custody key.
    assert_eq!(node.get("custody"), Some(&Value::from("none")), "{node}");
}

async fn lease_row(server: &Server, lease_id: &str) -> (i64, Option<String>, String) {
    sqlx::query_as("SELECT fencing_token, revoked_at, holder_node_id FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// A node, connected, holding the `local` lease of a stateful app whose
/// volume is ready. Returns the socket, the app, its VM and its lease.
async fn node_with_lease(
    server: &Server,
    owner: &reqwest::Client,
) -> (Enrolled, Socket, String, String, String) {
    let node = enrolled_node(server, owner, "node-alpha").await;
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, Vec::new())).await;
    recv_until(&mut socket, &["welcome", "desired_state"]).await;
    heartbeat(&mut socket).await;

    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project["id"],
            "name": "web",
            "image": IMAGE,
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let app_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let (vm_id,): (String,) = sqlx::query_as("SELECT id FROM vms WHERE app_id = ?")
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    recv_until(&mut socket, &["desired_state"]).await;
    heartbeat(&mut socket).await;

    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let lease_id = of_type(&frames, "lease_grant")[0]["body"]["lease_id"]
        .as_str()
        .unwrap()
        .to_string();
    let rev = of_type(&frames, "desired_state")[0]["body"]["rev"]
        .as_u64()
        .unwrap();
    send(
        &mut socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id,
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
    )
    .await;
    heartbeat(&mut socket).await;
    (node, socket, app_id, vm_id, lease_id)
}

async fn agent_token(server: &Server, owner: &reqwest::Client) -> String {
    let owner_user_id = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "fence-operator",
            "model": "test-model",
            "owner_user_id": owner_user_id,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key: Value = owner
        .post(server.url(&format!(
            "/api/v1/agents/{}/keys",
            agent["id"].as_str().unwrap()
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token: Value = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

async fn ack(
    client: &reqwest::Client,
    server: &Server,
    node_id: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(server.url(&format!("/api/v1/nodes/{node_id}/fence-ack")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_failed_fence_is_listed_and_a_human_acknowledgement_reaches_the_node() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, app_id, vm_id, lease_id) = node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();

    // Nothing to acknowledge yet.
    let (status, code, _) = problem(
        ack(
            &owner,
            &server,
            &node_id,
            json!({ "lease_ids": [lease_id] }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // Step 1: `node.fence_failed` lists the lease, app and VM on the node.
    send(
        &mut socket,
        fence_failed_event(&node_id, &lease_id, &app_id, &vm_id),
    )
    .await;
    let listed = wait_for_node(
        &owner,
        &server,
        Some(&mut socket),
        &node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;
    let failure = &listed["fence_failure"];
    assert_eq!(failure["lease_ids"], json!([lease_id]), "{listed}");
    assert_eq!(failure["app_ids"], json!([app_id]));
    assert_eq!(failure["vm_ids"], json!([vm_id]));
    assert!(!failure["detected_at"].as_str().unwrap().is_empty());

    // While listed, the lease is granted to no node: a new session of its
    // holder gets its desired state but no `lease_grant`.
    drop(socket);
    wait_for_node(&owner, &server, None, &node_id, "disconnected", |node| {
        node["status"] != "connected"
    })
    .await;
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(&node_id, vec![hello_lease(&lease_id, &app_id, 1, "held")]),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "desired_state"]).await;
    assert!(of_type(&frames, "lease_grant").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    heartbeat(&mut socket).await;

    // Only a human acknowledges: an agent is refused with `human_only`.
    let token = agent_token(&server, &owner).await;
    let res = reqwest::Client::new()
        .post(server.url(&format!("/api/v1/nodes/{node_id}/fence-ack")))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "try to clear a fence")
        .json(&json!({ "lease_ids": [lease_id] }))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));

    // The list must be exactly the outstanding one, and well formed.
    for (body, expected) in [
        (json!({ "lease_ids": [UNKNOWN_LEASE] }), (409, "conflict")),
        (
            json!({ "lease_ids": [lease_id, UNKNOWN_LEASE] }),
            (409, "conflict"),
        ),
        (json!({ "lease_ids": [] }), (422, "validation_failed")),
        (
            json!({ "lease_ids": [lease_id, lease_id] }),
            (422, "validation_failed"),
        ),
        (
            json!({ "lease_ids": ["app_01JC9Z6Q5W8N3K4T7R2M1XHB0D"] }),
            (422, "validation_failed"),
        ),
    ] {
        let (status, code, _) = problem(ack(&owner, &server, &node_id, body.clone()).await).await;
        assert_eq!((status, code.as_str()), expected, "{body}");
    }
    let (status, code, _) = problem(
        ack(
            &owner,
            &server,
            "nod_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
            json!({ "lease_ids": [lease_id] }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
    assert_eq!(lease_row(&server, &lease_id).await.0, 1, "nothing changed");

    // Steps 2 and 3: the acknowledgement bumps the token, records the lease
    // revoked, clears `fence_failure` and queues the `fence_ack`.
    let before = log_head(&server).await;
    let res = ack(
        &owner,
        &server,
        &node_id,
        json!({ "lease_ids": [lease_id] }),
    )
    .await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    assert!(body["fence_failure"].is_null(), "{body}");
    assert_eq!(body["id"], node_id);
    // The returned node carries its change-log lag, as every node read does
    // (node-agent-protocol.md section 7.1, ADR 0301).
    assert_log_lag(&body, before, log_head(&server).await);
    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 2);
    assert!(revoked_at.is_some());
    assert_eq!(holder, node_id, "the node stays named as the last holder");

    // Step 4: the connected node gets `command` kind `fence_ack` at once;
    // its `ok` clears `fence_ack_pending`.
    let frames = recv_until(&mut socket, &["command"]).await;
    let command = of_type(&frames, "command")[0];
    assert_eq!(command["body"]["kind"], "fence_ack", "{command}");
    assert_eq!(command["body"]["params"]["lease_ids"], json!([lease_id]));
    // ADR 0245: with the token after the increment, so the node clears only
    // the failed fences below it.
    assert_eq!(
        command["body"]["params"]["fencing_tokens"],
        json!({ lease_id.as_str(): 2 })
    );
    let pending = fence_ack_pending(&server, &node_id).await.expect("pending");
    assert_eq!(pending["fencing_tokens"], json!({ lease_id.as_str(): 2 }));
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    wait_for_no_pending_ack(&server, &mut socket, &node_id).await;

    // One audit record of the call, by the human, with the list.
    let (result, params): (String, String) = sqlx::query_as(
        "SELECT result, params FROM audit_log \
         WHERE operation = 'POST /api/v1/nodes/{node_id}/fence-ack' AND result = 'ok'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "ok");
    let params: Value = serde_json::from_str(&params).unwrap();
    assert_eq!(params["lease_ids"], json!([lease_id]));
}

/// A listed failure on a node that is gone, then acknowledged: returns the
/// node, its app, VM and lease, with `fence_ack_pending` set.
async fn acked_while_away(server: &Server, owner: &reqwest::Client) -> (Enrolled, String, String) {
    let (node, mut socket, app_id, vm_id, lease_id) = node_with_lease(server, owner).await;
    let node_id = node.node_id.clone();
    send(
        &mut socket,
        fence_failed_event(&node_id, &lease_id, &app_id, &vm_id),
    )
    .await;
    wait_for_node(
        owner,
        server,
        Some(&mut socket),
        &node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;
    drop(socket);
    wait_for_node(owner, server, None, &node_id, "disconnected", |node| {
        node["status"] != "connected"
    })
    .await;
    let res = ack(owner, server, &node_id, json!({ "lease_ids": [lease_id] })).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    assert!(fence_ack_pending(server, &node_id).await.is_some());
    (node, app_id, lease_id)
}

/// Reads frames for `wait` and fails on any of the `forbidden` types.
async fn none_of(socket: &mut Socket, forbidden: &[&str], wait: Duration) {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return;
        }
        match timeout(left, socket.next()).await {
            Err(_) => return,
            Ok(Some(Ok(WsMessage::Text(text)))) => {
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                let kind = frame["type"].as_str().unwrap_or_default();
                assert!(!forbidden.contains(&kind), "unexpected {kind}: {frame}");
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("the socket ended: {other:?}"),
        }
    }
}

/// Section 4.2 rules 3 to 5: the stale-lease revoke goes first, the pending
/// `fence_ack` only once it is answered, and the desired state only once the
/// `fence_ack` is answered.
#[tokio::test]
async fn a_pending_fence_ack_goes_out_once_the_revokes_are_answered() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = acked_while_away(&server, &owner).await;
    let node_id = node.node_id.clone();

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "revoked")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    assert_eq!(revoke["body"]["lease_id"], lease_id);
    assert_eq!(revoke["body"]["fencing_token"], 2);
    assert!(of_type(&frames, "command").is_empty(), "{frames:?}");
    // Nothing else while the revoke is unanswered.
    heartbeat(&mut socket).await;
    none_of(
        &mut socket,
        &["command", "desired_state"],
        Duration::from_millis(500),
    )
    .await;

    // The node's fence at the new token stops everything: then the
    // acknowledgement goes out, and the desired state waits for its answer.
    reply(
        &mut socket,
        ok_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    let frames = recv_until(&mut socket, &["command"]).await;
    assert!(of_type(&frames, "desired_state").is_empty(), "{frames:?}");
    let command = of_type(&frames, "command")[0].clone();
    assert_eq!(command["body"]["kind"], "fence_ack");
    assert_eq!(command["body"]["params"]["lease_ids"], json!([lease_id]));
    assert_eq!(
        command["body"]["params"]["fencing_tokens"],
        json!({ lease_id.as_str(): 2 }),
        "the token the acknowledgement stored, as the revoke carried"
    );
    heartbeat(&mut socket).await;
    none_of(&mut socket, &["desired_state"], Duration::from_millis(500)).await;
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    recv_until(&mut socket, &["desired_state"]).await;
    wait_for_no_pending_ack(&server, &mut socket, &node_id).await;
    let node = get_node(&owner, &server, &node_id).await;
    assert!(node["fence_failure"].is_null(), "{node}");
}

/// The blocker case of the review: after the operator checked, the node's
/// fence at the new token fails again. That failure is listed, the
/// acknowledgement no longer covers the lease, and no `fence_ack` reaches the
/// node, so it stays failed.
#[tokio::test]
async fn a_revoke_that_fails_again_after_the_ack_keeps_the_lease_listed() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = acked_while_away(&server, &owner).await;
    let node_id = node.node_id.clone();

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "revoked")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    reply(
        &mut socket,
        fence_failed_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    // With nothing left to acknowledge, the desired state follows at once,
    // and no `fence_ack` is sent.
    let frames = recv_until(&mut socket, &["desired_state"]).await;
    assert!(of_type(&frames, "command").is_empty(), "{frames:?}");
    heartbeat(&mut socket).await;
    none_of(&mut socket, &["command"], Duration::from_millis(500)).await;
    assert!(fence_ack_pending(&server, &node_id).await.is_none());
    let listed = get_node(&owner, &server, &node_id).await;
    assert_eq!(
        listed["fence_failure"]["lease_ids"],
        json!([lease_id]),
        "{listed}"
    );
}

/// The desired state waits for the answer to the `fence_ack` (section 4.2
/// rules 4 and 5), and a refusal is an answer: a node that answers it with an
/// `error` frame, or with the `command_result` error an agent without
/// `fence_ack` sends, still gets its desired state. The acknowledgement stays
/// pending, is not sent again on that session, and goes out again at the next.
#[tokio::test]
async fn a_refused_fence_ack_lets_the_desired_state_out_and_is_sent_again() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = acked_while_away(&server, &owner).await;
    let node_id = node.node_id.clone();

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "revoked")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    reply(
        &mut socket,
        ok_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    let frames = recv_until(&mut socket, &["command"]).await;
    assert!(of_type(&frames, "desired_state").is_empty(), "{frames:?}");
    let command = of_type(&frames, "command")[0].clone();
    assert_eq!(command["body"]["kind"], "fence_ack");
    heartbeat(&mut socket).await;
    none_of(&mut socket, &["desired_state"], Duration::from_millis(300)).await;
    reply(
        &mut socket,
        NodeMessage::Error(quoro_proto::ProtocolError {
            code: ErrorCode::BadMessage,
            message: "the fence_ack could not be read".to_string(),
            fatal: false,
            term: None,
        }),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    let frames = recv_until(&mut socket, &["desired_state"]).await;
    assert!(of_type(&frames, "command").is_empty(), "{frames:?}");
    heartbeat(&mut socket).await;
    none_of(&mut socket, &["command"], Duration::from_millis(500)).await;
    assert!(fence_ack_pending(&server, &node_id).await.is_some());

    // The next session sends it again; an agent that does not know the
    // command kind refuses it with a `command_result` error.
    drop(socket);
    wait_for_node(&owner, &server, None, &node_id, "disconnected", |node| {
        node["status"] != "connected"
    })
    .await;
    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node_id, Vec::new())).await;
    let frames = recv_until(&mut socket, &["welcome", "command"]).await;
    assert!(of_type(&frames, "desired_state").is_empty(), "{frames:?}");
    let command = of_type(&frames, "command")[0].clone();
    assert_eq!(command["body"]["kind"], "fence_ack");
    reply(
        &mut socket,
        NodeMessage::CommandResult(CommandResult {
            status: CommandStatus::Error,
            detail: Some("command kind \"fence_ack\" arrives in a later milestone".to_string()),
            error: Some(ErrorBody {
                code: ErrorCode::BadMessage,
                message: "this command kind is not implemented yet".to_string(),
            }),
            lines: None,
            metrics: None,
            snapshot_seq: None,
        }),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    recv_until(&mut socket, &["desired_state"]).await;
    heartbeat(&mut socket).await;
    assert!(fence_ack_pending(&server, &node_id).await.is_some());
}

/// An acknowledgement leaves alone a listed lease that another node holds by
/// now: that node's grant already carries a higher token. The failed node is
/// still told, so it clears its own record.
#[tokio::test]
async fn an_ack_leaves_a_lease_another_node_holds_alone() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, app_id, vm_id, lease_id) = node_with_lease(&server, &owner).await;
    let other = enrolled_node(&server, &owner, "node-beta").await;
    send(
        &mut socket,
        fence_failed_event(&node.node_id, &lease_id, &app_id, &vm_id),
    )
    .await;
    wait_for_node(
        &owner,
        &server,
        Some(&mut socket),
        &node.node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;
    // The lease moved on meanwhile (the restore race of #392).
    sqlx::query("UPDATE leases SET holder_node_id = ?, fencing_token = 5 WHERE id = ?")
        .bind(&other.node_id)
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let res = ack(
        &owner,
        &server,
        &node.node_id,
        json!({ "lease_ids": [lease_id] }),
    )
    .await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!(
        (token, revoked_at, holder),
        (5, None, other.node_id.clone())
    );
    let command = of_type(&recv_until(&mut socket, &["command"]).await, "command")[0].clone();
    assert_eq!(command["body"]["params"]["lease_ids"], json!([lease_id]));
    // ADR 0247: one above the other node's token, so this node clears a
    // failed fence it recorded at that token.
    assert_eq!(
        command["body"]["params"]["fencing_tokens"],
        json!({ lease_id.as_str(): 6 })
    );
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    wait_for_no_pending_ack(&server, &mut socket, &node.node_id).await;
}

#[tokio::test]
async fn a_revoke_answered_fence_failed_lists_its_lease() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;

    // The node claims a lease the primary does not know: it is revoked first,
    // and the node cannot confirm the stop.
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node.node_id,
            vec![hello_lease(
                UNKNOWN_LEASE,
                "app_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
                3,
                "held",
            )],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0];
    assert_eq!(revoke["body"]["lease_id"], UNKNOWN_LEASE);
    reply(
        &mut socket,
        fence_failed_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    // Without a pending acknowledgement the desired state follows the revoke.
    recv_until(&mut socket, &["desired_state"]).await;
    let listed = wait_for_node(
        &owner,
        &server,
        Some(&mut socket),
        &node.node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;
    assert_eq!(
        listed["fence_failure"]["lease_ids"],
        json!([UNKNOWN_LEASE]),
        "{listed}"
    );

    // The acknowledgement of a lease with no row still clears the failure.
    let res = ack(
        &owner,
        &server,
        &node.node_id,
        json!({ "lease_ids": [UNKNOWN_LEASE] }),
    )
    .await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let frames = recv_until(&mut socket, &["command"]).await;
    let command = of_type(&frames, "command")[0];
    assert_eq!(command["body"]["kind"], "fence_ack");
    // P-0273: a lease with no row gets the highest token the change log
    // carries, so the node clears every failed fence of it.
    assert_eq!(
        command["body"]["params"],
        json!({
            "lease_ids": [UNKNOWN_LEASE],
            "fencing_tokens": { UNKNOWN_LEASE: 9_007_199_254_740_991u64 },
        }),
        "{command}"
    );
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    wait_for_no_pending_ack(&server, &mut socket, &node.node_id).await;
}

/// ADR 0245: an acknowledgement a primary before ADR 0245 recorded has no
/// tokens. It goes out without `fencing_tokens`, as that primary sent it, and
/// the node's `ok` clears it. An acknowledgement recorded since, with a
/// token, follows in a `fence_ack` of its own with its token, so the first
/// one never clears it by lease id.
#[tokio::test]
async fn an_acknowledgement_recorded_without_tokens_goes_out_without_them() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = acked_while_away(&server, &owner).await;
    let node_id = node.node_id.clone();
    sqlx::query("UPDATE nodes SET fence_ack_pending = ? WHERE id = ?")
        .bind(
            json!({
                "acked_at": "2026-11-01T00:00:00Z",
                "lease_ids": [lease_id, OTHER_LEASE],
                "fencing_tokens": { OTHER_LEASE: 9 },
            })
            .to_string(),
        )
        .bind(&node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "revoked")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    reply(
        &mut socket,
        ok_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    let frames = recv_until(&mut socket, &["command"]).await;
    let command = of_type(&frames, "command")[0].clone();
    assert_eq!(command["body"]["kind"], "fence_ack");
    assert_eq!(
        command["body"]["params"],
        json!({ "lease_ids": [lease_id] }),
        "{command}"
    );
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    let frames = recv_until(&mut socket, &["command"]).await;
    let command = of_type(&frames, "command")[0].clone();
    assert_eq!(
        command["body"]["params"],
        json!({ "lease_ids": [OTHER_LEASE], "fencing_tokens": { OTHER_LEASE: 9 } }),
        "{command}"
    );
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    wait_for_no_pending_ack(&server, &mut socket, &node_id).await;
}

/// `declareNodeLost` (section 8.3, ADR 0183, ADR 0202).
async fn declare_lost(
    client: &reqwest::Client,
    server: &Server,
    node_id: &str,
) -> reqwest::Response {
    client
        .post(server.url(&format!("/api/v1/nodes/{node_id}/declare-lost")))
        .send()
        .await
        .unwrap()
}

/// The `lost_at` the node row holds.
async fn lost_at(server: &Server, node_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT lost_at FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Waits until the primary no longer has a session of `node_id`.
async fn wait_disconnected(server: &Server, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while server.state.hub.is_connected(node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never ended"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `(result, scope, params, change_seq, actor_type, target_type, target_id)`
/// of each `declareNodeLost` record, oldest first.
type DeclareRecord = (
    String,
    Option<String>,
    String,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
);

async fn declare_records(server: &Server) -> Vec<DeclareRecord> {
    sqlx::query_as(
        "SELECT result, scope, params, change_seq, actor_type, target_type, target_id \
         FROM audit_log WHERE operation = 'POST /api/v1/nodes/{node_id}/declare-lost' \
         ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

/// `(seq, entity_type, entity_id, payload)` of the lease and node entries
/// written after `after`.
async fn entries_after(server: &Server, after: i64) -> Vec<(i64, String, String, Value)> {
    let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT seq, entity_type, entity_id, payload FROM change_log \
         WHERE seq > ? AND entity_type IN ('lease', 'node') ORDER BY seq",
    )
    .bind(after)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, kind, id, payload)| (seq, kind, id, serde_json::from_str(&payload).unwrap()))
        .collect()
}

/// Section 8.3: each answer of `declareNodeLost`, in the order the checks
/// run. A connected node is refused with `conflict`, an agent with
/// `human_only`, an unknown node with `not_found` and a removed one with
/// `conflict`, and none of them changes anything. A disconnected node is
/// declared lost: in one transaction the token of its lease goes up by one
/// and the row is recorded revoked with the node still its holder, `lost_at`
/// is set, the change log carries both, and the audit record names the
/// node's entry. The answer is the node as `getNode` returns it. A repeat
/// changes nothing, answers `200`, and is recorded with no `change_seq`.
#[tokio::test]
async fn declaring_a_node_lost_revokes_its_leases_and_answers_each_case() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, socket, _app_id, _vm_id, lease_id) = node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();

    // Connected: the primary revokes over the session instead.
    let (status, code, reason) = problem(declare_lost(&owner, &server, &node_id).await).await;
    assert_eq!((status, code.as_str(), reason), (409, "conflict", None));

    drop(socket);
    wait_disconnected(&server, &node_id).await;
    // A lease another node holds: declaring this node lost leaves it alone
    // (section 8.3 touches the leases the node holds; ADR 0246).
    let other = enrolled_node(&server, &owner, "node-beta").await;
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, granted_at) \
         VALUES (?, 'app', 'app_01JC9Z6Q5W8N3K4T7R2M1XHB0E', ?, 'local', 7, ?)",
    )
    .bind(OTHER_LEASE)
    .bind(&other.node_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();

    // Human-only, before anything else is checked.
    let token = agent_token(&server, &owner).await;
    let res = reqwest::Client::new()
        .post(server.url(&format!("/api/v1/nodes/{node_id}/declare-lost")))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "try to declare a node lost")
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));

    for unknown in ["nod_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "not-a-node"] {
        let (status, code, _) = problem(declare_lost(&owner, &server, unknown).await).await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{unknown}");
    }
    let (token, revoked_at, _) = lease_row(&server, &lease_id).await;
    assert_eq!((token, revoked_at), (1, None), "nothing changed");
    assert_eq!(lost_at(&server, &node_id).await, None);

    // Declared lost.
    let before = log_head(&server).await;
    let res = declare_lost(&owner, &server, &node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    let after = log_head(&server).await;
    assert_eq!(body["id"], node_id);
    assert_eq!(body["status"], "disconnected", "the node stays enrolled");
    assert_eq!(body["roles"], json!(["node"]), "and keeps its vote");
    assert!(body["fence_failure"].is_null(), "{body}");
    let declared = body["lost_at"].as_str().expect("lost_at").to_string();
    assert_log_lag(&body, before, after);
    // The same shape as `getNode`.
    let read = get_node(&owner, &server, &node_id).await;
    assert_eq!(read["lost_at"], body["lost_at"]);
    let keys = |node: &Value| -> Vec<String> {
        let mut keys: Vec<String> = node.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    };
    assert_eq!(keys(&read), keys(&body));

    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 2, "the token goes up by one");
    let revoked_at = revoked_at.expect("recorded revoked in the primary's view");
    assert!(revoked_at <= declared, "{revoked_at} <= {declared}");
    assert_eq!(holder, node_id, "the node stays named as the last holder");
    assert_eq!(
        lease_row(&server, OTHER_LEASE).await,
        (7, None, other.node_id.clone()),
        "a lease another node holds is left as it is"
    );

    let entries = entries_after(&server, before).await;
    assert_eq!(entries.len(), 2, "{entries:?}");
    let (_, kind, id, payload) = &entries[0];
    assert_eq!((kind.as_str(), id.as_str()), ("lease", lease_id.as_str()));
    assert_eq!(payload["fencing_token"], 2, "{payload}");
    assert_eq!(payload["revoked_at"], json!(revoked_at));
    let (node_seq, kind, id, payload) = &entries[1];
    assert_eq!((kind.as_str(), id.as_str()), ("node", node_id.as_str()));
    assert_eq!(payload["lost_at"], json!(declared), "{payload}");

    let records = declare_records(&server).await;
    let ok: Vec<&DeclareRecord> = records.iter().filter(|r| r.0 == "ok").collect();
    assert_eq!(ok.len(), 1, "{records:?}");
    let (_, scope, params, change_seq, actor_type, target_type, target_id) = ok[0];
    assert_eq!(scope.as_deref(), Some("node:declare_lost"));
    assert_eq!(serde_json::from_str::<Value>(params).unwrap(), json!({}));
    assert_eq!(*change_seq, Some(*node_seq));
    assert_eq!(actor_type, "user");
    assert_eq!(target_type.as_deref(), Some("node"));
    assert_eq!(target_id.as_deref(), Some(node_id.as_str()));
    // Every refusal is recorded too: the connected node, the agent and the
    // two unknown nodes.
    assert_eq!(records.len(), 5, "{records:?}");

    // A repeat changes nothing and answers the node.
    let before = log_head(&server).await;
    let res = declare_lost(&owner, &server, &node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let again: Value = res.json().await.unwrap();
    assert_eq!(again["lost_at"], json!(declared));
    assert_eq!(
        lease_row(&server, &lease_id).await.0,
        2,
        "no second increment"
    );
    assert!(entries_after(&server, before).await.is_empty());
    let records = declare_records(&server).await;
    let last = records.last().unwrap();
    assert_eq!((last.0.as_str(), last.3), ("ok", None), "{records:?}");

    // A removed node is refused.
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, reason) = problem(declare_lost(&owner, &server, &node_id).await).await;
    assert_eq!((status, code.as_str(), reason), (409, "conflict", None));
}

/// A node that is disconnected and declared lost: returns the node, its app
/// and its lease, revoked at token 2.
async fn declared_lost(server: &Server, owner: &reqwest::Client) -> (Enrolled, String, String) {
    let (node, socket, app_id, _vm_id, lease_id) = node_with_lease(server, owner).await;
    drop(socket);
    wait_disconnected(server, &node.node_id).await;
    let res = declare_lost(owner, server, &node.node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    assert_eq!(lease_row(server, &lease_id).await.0, 2);
    (node, app_id, lease_id)
}

/// `(params, target_id, change_seq)` of each `lease.revoke` record.
async fn lease_revoke_records(server: &Server) -> Vec<(Value, Option<String>, Option<i64>)> {
    let rows: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
        "SELECT params, target_id, change_seq FROM audit_log \
         WHERE operation = 'lease.revoke' AND actor_type = 'system' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(params, target, seq)| (serde_json::from_str(&params).unwrap(), target, seq))
        .collect()
}

/// Waits until the node row's `lost_at` is null, sending heartbeats.
async fn wait_lost_at_cleared(server: &Server, socket: &mut Socket, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while lost_at(server, node_id).await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "lost_at was never cleared"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sections 8.3 and 9 step 2: a node declared lost that returns listing its
/// lease at the old token gets it revoked with reason `declared_lost` first,
/// right after the `welcome`, at the primary's token, and the revoke is
/// recorded as a `lease.revoke` system record. While it is connected it
/// cannot be declared lost again. `lost_at` stays until the revoke is
/// answered, then it is cleared and the change log carries it. The lease
/// stays revoked with the node as its last holder: nothing grants it back.
#[tokio::test]
async fn a_returning_lost_node_gets_its_leases_revoked_first_and_lost_at_cleared() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = declared_lost(&server, &owner).await;
    let node_id = node.node_id.clone();

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(&node_id, vec![hello_lease(&lease_id, &app_id, 1, "held")]),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let kinds: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["welcome", "lease_revoke"], "before anything else");
    let revoke = &frames[1];
    assert_eq!(revoke["body"]["lease_id"], lease_id);
    assert_eq!(revoke["body"]["fencing_token"], 2);
    assert_eq!(revoke["body"]["reason"], "declared_lost", "{revoke}");
    let records = lease_revoke_records(&server).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        records[0].0,
        json!({ "node_id": node_id, "fencing_token": 2, "reason": "declared_lost" })
    );
    assert_eq!(records[0].1.as_deref(), Some(app_id.as_str()));

    // Lost and connected: the connected check answers.
    let (status, code, _) = problem(declare_lost(&owner, &server, &node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    assert!(
        lost_at(&server, &node_id).await.is_some(),
        "not answered yet"
    );

    let before = log_head(&server).await;
    reply(
        &mut socket,
        ok_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    wait_lost_at_cleared(&server, &mut socket, &node_id).await;
    let entries = entries_after(&server, before).await;
    let cleared: Vec<_> = entries
        .iter()
        .filter(|(_, kind, id, payload)| {
            kind == "node" && id == &node_id && payload.get("lost_at") == Some(&Value::Null)
        })
        .collect();
    assert_eq!(cleared.len(), 1, "{entries:?}");
    let node = get_node(&owner, &server, &node_id).await;
    assert!(node["lost_at"].is_null(), "{node}");
    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!((token, holder.as_str()), (2, node_id.as_str()));
    assert!(revoked_at.is_some(), "the lease stays revoked");
    none_of(&mut socket, &["lease_grant"], Duration::from_millis(300)).await;
}

/// Waits up to 10 s for the primary to end the session, and returns every
/// frame it sent until then.
async fn frames_until_closed(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let message = timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("the session ends within 10 s");
        match message {
            Some(Ok(WsMessage::Text(text))) => {
                frames.push(serde_json::from_str(text.as_str()).unwrap());
            }
            Some(Ok(WsMessage::Close(_)) | Err(_)) | None => return frames,
            Some(Ok(_)) => {}
        }
    }
}

/// Sections 8.3 and 9 step 2, read with `Node.lost_at` ("cleared once the
/// node returns and has answered the revokes of those leases"). Three
/// edges of the return:
///
/// - a node that lists none of its leases still gets its declared lease
///   revoked with reason `declared_lost`, recorded, and `lost_at` stays
///   until it answered;
/// - a node declared lost with no lease has nothing to answer, so `lost_at`
///   is cleared at its session start;
/// - a revoke answered `fence_failed` is answered all the same: the failure
///   is listed (section 8.5), which keeps the lease ungrantable until an
///   operator acknowledges it, and `lost_at` is cleared.
#[tokio::test]
async fn a_returning_lost_node_clears_lost_at_without_listings_or_after_a_failed_fence() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = declared_lost(&server, &owner).await;
    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, Vec::new())).await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let kinds: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["welcome", "lease_revoke"], "before anything else");
    let revoke = &frames[1];
    assert_eq!(revoke["body"]["lease_id"], lease_id);
    assert_eq!(revoke["body"]["fencing_token"], 2);
    assert_eq!(revoke["body"]["reason"], "declared_lost", "{revoke}");
    let records = lease_revoke_records(&server).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        records[0].0,
        json!({ "node_id": node.node_id, "fencing_token": 2, "reason": "declared_lost" })
    );
    assert_eq!(records[0].1.as_deref(), Some(app_id.as_str()));
    heartbeat(&mut socket).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        lost_at(&server, &node.node_id).await.is_some(),
        "not answered yet"
    );
    reply(
        &mut socket,
        ok_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    wait_lost_at_cleared(&server, &mut socket, &node.node_id).await;

    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let res = declare_lost(&owner, &server, &node.node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, Vec::new())).await;
    let frames = recv_until(&mut socket, &["welcome", "desired_state"]).await;
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    wait_lost_at_cleared(&server, &mut socket, &node.node_id).await;
    assert!(lease_revoke_records(&server).await.is_empty());

    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = declared_lost(&server, &owner).await;
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node.node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "held")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    assert_eq!(revoke["body"]["reason"], "declared_lost");
    reply(
        &mut socket,
        fence_failed_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    wait_lost_at_cleared(&server, &mut socket, &node.node_id).await;
    let node = wait_for_node(
        &owner,
        &server,
        Some(&mut socket),
        &node.node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;
    assert_eq!(node["fence_failure"]["lease_ids"], json!([lease_id]));
}

/// Sections 8.3 and 9 step 2 with a partial listing: the node held two
/// leases when it was declared lost and returns listing one. The listed one
/// is revoked first, then the other, both with reason `declared_lost`, each
/// with its `lease.revoke` record naming the app of its row. `lost_at`
/// stays until both are answered.
#[tokio::test]
async fn a_returning_lost_node_that_lists_some_of_its_leases_gets_all_revoked() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, socket, app_id, _vm_id, lease_id) = node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, granted_at) VALUES (?, 'app', ?, ?, 'local', 3, ?)",
    )
    .bind(SECOND_LEASE)
    .bind(SECOND_APP)
    .bind(&node_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    drop(socket);
    wait_disconnected(&server, &node_id).await;
    let res = declare_lost(&owner, &server, &node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    assert_eq!(lease_row(&server, &lease_id).await.0, 2);
    assert_eq!(lease_row(&server, SECOND_LEASE).await.0, 4);

    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(&node_id, vec![hello_lease(&lease_id, &app_id, 1, "held")]),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let mut frames = frames;
    frames.push(recv_frame(&mut socket).await);
    let kinds: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
    assert_eq!(
        kinds,
        ["welcome", "lease_revoke", "lease_revoke"],
        "the listed one, then the other, before anything else"
    );
    let (first, second) = (&frames[1]["body"], &frames[2]["body"]);
    assert_eq!(
        (
            &first["lease_id"],
            &first["fencing_token"],
            &first["reason"]
        ),
        (&json!(lease_id), &json!(2), &json!("declared_lost"))
    );
    assert_eq!(
        (
            &second["lease_id"],
            &second["fencing_token"],
            &second["reason"]
        ),
        (&json!(SECOND_LEASE), &json!(4), &json!("declared_lost"))
    );
    let records = lease_revoke_records(&server).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(
        records[0],
        (
            json!({ "node_id": node_id, "fencing_token": 2, "reason": "declared_lost" }),
            Some(app_id.clone()),
            None
        )
    );
    assert_eq!(
        records[1],
        (
            json!({ "node_id": node_id, "fencing_token": 4, "reason": "declared_lost" }),
            Some(SECOND_APP.to_string()),
            None
        )
    );

    reply(&mut socket, ok_result(), frames[1]["id"].as_str()).await;
    heartbeat(&mut socket).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        lost_at(&server, &node_id).await.is_some(),
        "one revoke is still unanswered"
    );
    reply(&mut socket, ok_result(), frames[2]["id"].as_str()).await;
    wait_lost_at_cleared(&server, &mut socket, &node_id).await;
}

/// Audit-log-format.md section 3.7: the `lease.revoke` record of a
/// `declared_lost` revoke takes its app from the lease row. A node declared
/// lost lists three leases with no row: one under an app that exists, which
/// becomes the record's target; one under a well-formed `app_` id with no
/// app; and one under an id that is no app's, oversized. Each is revoked,
/// and the last two records have no target rather than what the node sent.
#[tokio::test]
async fn a_declared_lost_revoke_of_a_lease_with_no_row_records_only_an_app_that_exists() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = declared_lost(&server, &owner).await;
    let bogus = format!("not-an-app-{}", "x".repeat(4096));
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node.node_id,
            vec![
                hello_lease(OTHER_LEASE, &app_id, 1, "held"),
                hello_lease(SECOND_LEASE, SECOND_APP, 1, "held"),
                hello_lease(UNKNOWN_LEASE, &bogus, 1, "held"),
            ],
        ),
    )
    .await;
    let mut frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    while of_type(&frames, "lease_revoke").len() < 4 {
        frames.push(recv_frame(&mut socket).await);
    }
    let revokes: Vec<&Value> = of_type(&frames, "lease_revoke");
    let ids: Vec<&Value> = revokes.iter().map(|r| &r["body"]["lease_id"]).collect();
    assert_eq!(
        ids,
        [
            &json!(OTHER_LEASE),
            &json!(SECOND_LEASE),
            &json!(UNKNOWN_LEASE),
            &json!(lease_id)
        ],
        "{frames:?}"
    );
    assert!(
        revokes
            .iter()
            .all(|r| r["body"]["reason"] == "declared_lost")
    );
    let rows: Vec<(Option<String>, Option<String>, String)> = sqlx::query_as(
        "SELECT target_type, target_id, params FROM audit_log \
         WHERE operation = 'lease.revoke' AND actor_type = 'system' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    let targets: Vec<(Option<&str>, Option<&str>)> = rows
        .iter()
        .map(|(kind, id, _)| (kind.as_deref(), id.as_deref()))
        .collect();
    assert_eq!(
        targets,
        [
            (Some("app"), Some(app_id.as_str())),
            (None, None),
            (None, None),
            (Some("app"), Some(app_id.as_str())),
        ],
        "{rows:?}"
    );
    assert!(
        rows.iter()
            .all(|(_, _, params)| !params.contains("not-an-app") && !params.contains(SECOND_APP)),
        "{rows:?}"
    );
}

/// The write that clears `lost_at` can fail. After the `welcome`, the
/// session then ends with a non-fatal `bad_message`; at the session start,
/// when no revoke is to be answered, it ends before its `welcome`, as an
/// incomplete pass does. `lost_at` stays set either way, and the next
/// session clears it.
#[tokio::test]
async fn a_failed_clear_of_lost_at_ends_the_session() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id) = declared_lost(&server, &owner).await;
    let node_id = node.node_id.clone();

    server.state.hub.inject_fault(Fault::ClearLostAt {
        node_id: node_id.clone(),
    });
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(&node_id, vec![hello_lease(&lease_id, &app_id, 1, "held")]),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    reply(&mut socket, ok_result(), revoke["id"].as_str()).await;
    let frames = frames_until_closed(&mut socket).await;
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["code"], "bad_message", "{frames:?}");
    assert_eq!(errors[0]["body"]["fatal"], false, "{frames:?}");
    wait_disconnected(&server, &node_id).await;
    assert!(lost_at(&server, &node_id).await.is_some(), "not cleared");

    // Released at token 2 by that answer, so nothing is to be answered: the
    // clear runs at the session start.
    server.state.hub.inject_fault(Fault::ClearLostAt {
        node_id: node_id.clone(),
    });
    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node_id, Vec::new())).await;
    let frames = frames_until_closed(&mut socket).await;
    assert!(of_type(&frames, "welcome").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["fatal"], false, "{frames:?}");
    wait_disconnected(&server, &node_id).await;
    assert!(lost_at(&server, &node_id).await.is_some(), "not cleared");

    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node_id, Vec::new())).await;
    let frames = recv_until(&mut socket, &["welcome", "desired_state"]).await;
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    wait_lost_at_cleared(&server, &mut socket, &node_id).await;
}

/// When the write that clears `lost_at` fails, the answer that triggered it
/// unlocks nothing: no `fence_ack` and no desired state follow it, only the
/// non-fatal `bad_message` that ends the session (section 4.2 rules 3 to 5).
/// The node has a pending `fence_ack` for one lease, so the session start
/// waits for the revokes before sending it, and holds a second lease that
/// the declaration revoked. The `declared_lost` revoke of that second lease
/// is answered with a `command_result`, then, on a second server, with an
/// `error`; either answer is the last one `lost_at` waits for.
#[tokio::test]
async fn a_failed_clear_of_lost_at_sends_nothing_after_the_answer() {
    for refused in [false, true] {
        let case = if refused { "error" } else { "command_result" };
        let server = start_with_patient_node_session().await;
        let owner = server.owner().await;
        let (node, _app_id, _lease_id) = acked_while_away(&server, &owner).await;
        let node_id = node.node_id.clone();
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, granted_at) VALUES (?, 'app', ?, ?, 'local', 3, ?)",
        )
        .bind(SECOND_LEASE)
        .bind(SECOND_APP)
        .bind(&node_id)
        .bind(quoro_control_plane::store::now_rfc3339())
        .execute(server.state.store.pool())
        .await
        .unwrap();
        let res = declare_lost(&owner, &server, &node_id).await;
        assert_eq!(res.status(), 200, "{case}: {}", res.text().await.unwrap());

        server.state.hub.inject_fault(Fault::ClearLostAt {
            node_id: node_id.clone(),
        });
        let mut socket = connect(&server, &node.credential).await;
        send(&mut socket, hello(&node_id, Vec::new())).await;
        let frames = recv_until(&mut socket, &["welcome", "lease_revoke"]).await;
        let revoke = of_type(&frames, "lease_revoke")[0].clone();
        assert_eq!(revoke["body"]["lease_id"], SECOND_LEASE, "{case}: {revoke}");
        assert_eq!(revoke["body"]["reason"], "declared_lost", "{case}");
        let answer = if refused {
            NodeMessage::Error(quoro_proto::ProtocolError {
                code: ErrorCode::BadMessage,
                message: "the revoke could not be read".to_string(),
                fatal: false,
                term: None,
            })
        } else {
            ok_result()
        };
        reply(&mut socket, answer, revoke["id"].as_str()).await;
        let frames = frames_until_closed(&mut socket).await;
        let kinds: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["error"], "{case}: {frames:?}");
        assert_eq!(frames[0]["body"]["code"], "bad_message", "{case}");
        assert_eq!(frames[0]["body"]["fatal"], false, "{case}");
        wait_disconnected(&server, &node_id).await;
        assert!(lost_at(&server, &node_id).await.is_some(), "{case}");
        assert!(
            fence_ack_pending(&server, &node_id).await.is_some(),
            "{case}: never delivered"
        );
    }
}
