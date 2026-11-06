//! Failed fences and their acknowledgement (`node-agent-protocol.md` sections
//! 4.2 rule 4, 5.17 and 8.5; D-0074, D-0079; issue #391), over real HTTP and
//! WebSocket with a fake node.

mod common;

use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
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
    assert!(fence_ack_pending(&server, &node_id).await.is_some());
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

#[tokio::test]
async fn a_pending_fence_ack_goes_out_after_the_revokes_and_before_the_desired_state() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, app_id, vm_id, lease_id) = node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    send(
        &mut socket,
        fence_failed_event(&node_id, &lease_id, &app_id, &vm_id),
    )
    .await;
    wait_for_node(
        &owner,
        &server,
        Some(&mut socket),
        &node_id,
        "a fence_failure",
        |node| !node["fence_failure"].is_null(),
    )
    .await;

    // The node is gone when the operator acknowledges, so the command waits.
    drop(socket);
    wait_for_node(&owner, &server, None, &node_id, "disconnected", |node| {
        node["status"] != "connected"
    })
    .await;
    let res = ack(
        &owner,
        &server,
        &node_id,
        json!({ "lease_ids": [lease_id] }),
    )
    .await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(fence_ack_pending(&server, &node_id).await.is_some());

    // Section 4.2: the stale lease is revoked first, then `fence_ack`, and the
    // desired state waits for its answer.
    let mut socket = connect(&server, &node.credential).await;
    send(
        &mut socket,
        hello(
            &node_id,
            vec![hello_lease(&lease_id, &app_id, 1, "revoked")],
        ),
    )
    .await;
    let frames = recv_until(&mut socket, &["welcome", "lease_revoke", "command"]).await;
    let kinds: Vec<&str> = frames
        .iter()
        .map(|frame| frame["type"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["welcome", "lease_revoke", "command"], "{frames:?}");
    let revoke = &frames[1];
    assert_eq!(revoke["body"]["lease_id"], lease_id);
    assert_eq!(revoke["body"]["fencing_token"], 2);
    let command = &frames[2];
    assert_eq!(command["body"]["kind"], "fence_ack");
    assert_eq!(command["body"]["params"]["lease_ids"], json!([lease_id]));

    // The node answers the revoke `fence_failed` (its failed fence is still
    // recorded): that predates the acknowledgement, so the lease is not
    // listed again.
    reply(
        &mut socket,
        fence_failed_result(),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    // No desired state before the `fence_ack` is answered.
    heartbeat(&mut socket).await;
    let quiet = timeout(Duration::from_millis(500), socket.next()).await;
    if let Ok(Some(Ok(WsMessage::Text(text)))) = quiet {
        let frame: Value = serde_json::from_str(text.as_str()).unwrap();
        assert_ne!(frame["type"], "desired_state", "{frame}");
    }
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
    reply(
        &mut socket,
        ok_result(),
        Some(command["id"].as_str().unwrap()),
    )
    .await;
    wait_for_no_pending_ack(&server, &mut socket, &node.node_id).await;
}
