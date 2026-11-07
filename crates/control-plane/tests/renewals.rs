//! How the primary answers `lease_renew` (`node-agent-protocol.md` sections
//! 5.5, 8.4 and 8.8; D-0256 part 2; issue #603), over real HTTP and WebSocket
//! with a fake node. No build counts voter support yet, so the tests that
//! need an answer set their own.

mod common;

use common::{Server, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::authority::VoterSupport;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, Kvm as WireKvm, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, LeaseId, LeaseRenew};
use serde_json::{Value, json};
use std::sync::Arc;
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

/// A majority of voters supports every duration.
#[derive(Debug)]
struct EverySupport;

impl VoterSupport for EverySupport {
    fn supports(&self, _duration_ms: u64) -> bool {
        true
    }
}

struct Enrolled {
    node_id: String,
    credential: String,
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

/// Sends `message` and returns its id.
async fn send(socket: &mut Socket, message: NodeMessage, re: Option<&str>) -> String {
    let id = format!("msg_{}", ulid_like());
    let text = message.to_frame(&id, re).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
    id
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
            time: "2026-11-07T10:15:32Z".to_string(),
            busy: None,
        }),
        None,
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

/// Every frame the primary sends before it answers two pings in a row: the
/// session handles messages in order, so whatever a renewal before them
/// caused is in the result.
async fn frames_until_settled(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    for _ in 0..2 {
        let id = send(socket, NodeMessage::Ping, None).await;
        loop {
            let frame = recv_frame(socket).await;
            if frame["type"] == "pong" && frame["re"] == id.as_str() {
                break;
            }
            frames.push(frame);
        }
    }
    frames
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
    })
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

fn renew(lease_id: &str, token: u64) -> NodeMessage {
    NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: token,
    })
}

async fn run(server: &Server, sql: &str, lease_id: &str) {
    sqlx::query(sql)
        .bind(lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

async fn stored_duration(server: &Server, lease_id: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT duration_ms FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

async fn change_log_entries(server: &Server, lease_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM change_log WHERE entity_id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// A node, connected, holding the lease of a stateful app whose volume is
/// ready, made `expiring` for `D` in the row, as a first `expiring` grant
/// would leave it. Returns the node, its socket, the lease and its token.
async fn node_with_expiring_lease(
    server: &Server,
    owner: &reqwest::Client,
) -> (Enrolled, Socket, String, u64) {
    let node = enrolled_node(server, owner, "node-alpha").await;
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id), None).await;
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
    let grant = &of_type(&frames, "lease_grant")[0]["body"];
    let lease_id = grant["lease_id"].as_str().unwrap().to_string();
    let token = grant["fencing_token"].as_u64().unwrap();
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
        None,
    )
    .await;
    frames_until_settled(&mut socket).await;
    run(
        server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    (node, socket, lease_id, token)
}

/// Section 5.5: a renewal of the node's `expiring` lease at its token is
/// answered with a `lease_grant` at that token for `D`, `re` set, while a
/// majority of voters supports it. The answer counts toward the holder's gate
/// and writes nothing, unless the stored duration is shorter than `D`.
#[tokio::test]
async fn a_renewal_is_answered_at_its_token_while_voters_support_it() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, lease_id, token) = node_with_expiring_lease(&server, &owner).await;
    server.state.hub.set_voter_support(Arc::new(EverySupport));
    let entries = change_log_entries(&server, &lease_id).await;

    let id = send(&mut socket, renew(&lease_id, token), None).await;
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["re"], id.as_str());
    assert_eq!(grant["body"]["lease_id"], lease_id.as_str());
    assert_eq!(grant["body"]["fencing_token"], token);
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    assert!(of_type(&frames, "lease_revoke").is_empty());
    let authority = server.state.hub.authority();
    assert!(!authority.passed(&lease_id), "the answer opens a gate");
    assert!(
        authority
            .view(&lease_id)
            .and_then(|view| view.renewed_at)
            .is_some()
    );
    assert_eq!(
        change_log_entries(&server, &lease_id).await,
        entries,
        "a renewal writes nothing"
    );

    // A stored duration shorter than the answer's is raised first, so a
    // restarted primary counts the answer's (ADR 0203).
    run(
        &server,
        "UPDATE leases SET duration_ms = 3000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let id = send(&mut socket, renew(&lease_id, token), None).await;
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert_eq!(of_type(&frames, "lease_grant")[0]["re"], id.as_str());
    assert_eq!(stored_duration(&server, &lease_id).await, Some(15_000));
}

/// Section 8.8: without majority support the primary leaves the renewal
/// unanswered, records no gate, and keeps no raise of the stored duration.
/// No build counts support yet. A lease the node no longer holds is revoked
/// all the same.
#[tokio::test]
async fn without_voter_support_no_renewal_is_answered() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, lease_id, token) = node_with_expiring_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET duration_ms = 3000 WHERE id = ?",
        &lease_id,
    )
    .await;

    send(&mut socket, renew(&lease_id, token), None).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(of_type(&frames, "lease_grant").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    assert!(server.state.hub.authority().passed(&lease_id));
    assert_eq!(server.state.hub.authority().view(&lease_id), None);
    assert_eq!(
        stored_duration(&server, &lease_id).await,
        Some(3_000),
        "the refused answer's raise rolls back"
    );

    send(&mut socket, renew(UNKNOWN_LEASE, 4), None).await;
    let frames = recv_until(&mut socket, &["lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0];
    assert_eq!(revoke["body"]["lease_id"], UNKNOWN_LEASE);
    assert_eq!(revoke["body"]["fencing_token"], 4);
    assert!(of_type(&frames, "lease_grant").is_empty(), "{frames:?}");
}

/// D-0256 part 2: the primary does not answer a renewal of a lease whose row
/// is `local`, with support or without. Nor one above the row's token, one
/// of a lease listed in any node's failed fence (section 8.5), or one the
/// authority refuses because it keeps another holder's gate.
#[tokio::test]
async fn a_renewal_the_row_does_not_match_gets_no_answer() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, lease_id, token) = node_with_expiring_lease(&server, &owner).await;
    let other = enrolled_node(&server, &owner, "node-beta").await;
    server.state.hub.set_voter_support(Arc::new(EverySupport));

    run(
        &server,
        "UPDATE leases SET mode = 'local', duration_ms = NULL WHERE id = ?",
        &lease_id,
    )
    .await;
    send(&mut socket, renew(&lease_id, token), None).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(
        of_type(&frames, "lease_grant").is_empty(),
        "local: {frames:?}"
    );
    assert!(
        of_type(&frames, "lease_revoke").is_empty(),
        "local: {frames:?}"
    );

    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    send(&mut socket, renew(&lease_id, token + 1), None).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(
        of_type(&frames, "lease_grant").is_empty(),
        "higher: {frames:?}"
    );
    assert!(
        of_type(&frames, "lease_revoke").is_empty(),
        "higher: {frames:?}"
    );

    let failure = json!({
        "detected_at": "2026-11-07T10:00:00Z",
        "lease_ids": [lease_id],
        "app_ids": [],
        "vm_ids": [],
    })
    .to_string();
    // Listed in another node's failed fence: the lease goes to no node until
    // an operator acknowledges it (section 8.5).
    sqlx::query("UPDATE nodes SET fence_failure = ? WHERE id = ?")
        .bind(&failure)
        .bind(&other.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    send(&mut socket, renew(&lease_id, token), None).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(
        of_type(&frames, "lease_grant").is_empty(),
        "listed: {frames:?}"
    );
    assert!(
        of_type(&frames, "lease_revoke").is_empty(),
        "listed: {frames:?}"
    );
    assert!(server.state.hub.authority().passed(&lease_id));
    sqlx::query("UPDATE nodes SET fence_failure = NULL WHERE id = ?")
        .bind(&other.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // The authority keeps another holder's gate for the lease, so it refuses
    // the answer.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &other.node_id, Some(15_000));
    send(&mut socket, renew(&lease_id, token), None).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(
        of_type(&frames, "lease_grant").is_empty(),
        "not holder: {frames:?}"
    );
    assert!(
        of_type(&frames, "lease_revoke").is_empty(),
        "not holder: {frames:?}"
    );
    assert_eq!(server.state.hub.authority().view(&lease_id), None);

    // Once the row's holder holds the gate again, the renewal is answered.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(15_000));
    let id = send(&mut socket, renew(&lease_id, token), None).await;
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert_eq!(of_type(&frames, "lease_grant")[0]["re"], id.as_str());
}

/// Section 5.5: a renewal of a lease the node no longer holds is answered
/// with `lease_revoke`, with or without support, at a token the node
/// accepts: a lower token than the row's, no row, another holder, a revoked
/// row.
#[tokio::test]
async fn a_renewal_of_a_lease_the_node_no_longer_holds_is_revoked() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, lease_id, token) = node_with_expiring_lease(&server, &owner).await;
    let other = enrolled_node(&server, &owner, "node-beta").await;
    server.state.hub.set_voter_support(Arc::new(EverySupport));

    let expect_revoke = |frames: &[Value], lease: &str, token: u64, reason: &str| {
        assert!(of_type(frames, "lease_grant").is_empty(), "{frames:?}");
        let revokes = of_type(frames, "lease_revoke");
        assert_eq!(revokes.len(), 1, "{frames:?}");
        assert_eq!(revokes[0]["body"]["lease_id"], lease);
        assert_eq!(revokes[0]["body"]["fencing_token"], token);
        assert_eq!(revokes[0]["body"]["reason"], reason);
        revokes[0]["id"].as_str().unwrap().to_string()
    };

    // The row moved on to a higher token.
    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_id,
    )
    .await;
    send(&mut socket, renew(&lease_id, token), None).await;
    let mut frames = recv_until(&mut socket, &["lease_revoke"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    let revoke = expect_revoke(&frames, &lease_id, token + 1, "superseded");
    send(&mut socket, ok_result(), Some(&revoke)).await;

    // Another node holds it, at the node's token.
    sqlx::query("UPDATE leases SET holder_node_id = ?, fencing_token = ? WHERE id = ?")
        .bind(&other.node_id)
        .bind(i64::try_from(token).unwrap())
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    send(&mut socket, renew(&lease_id, token + 2), None).await;
    let mut frames = recv_until(&mut socket, &["lease_revoke"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    let revoke = expect_revoke(&frames, &lease_id, token + 2, "moved");
    send(&mut socket, ok_result(), Some(&revoke)).await;

    // The row is revoked.
    sqlx::query(
        "UPDATE leases SET holder_node_id = ?, revoked_at = '2026-11-07T10:00:00Z' WHERE id = ?",
    )
    .bind(&node.node_id)
    .bind(&lease_id)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    send(&mut socket, renew(&lease_id, token), None).await;
    let mut frames = recv_until(&mut socket, &["lease_revoke"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    let revoke = expect_revoke(&frames, &lease_id, token, "deleted");
    send(&mut socket, ok_result(), Some(&revoke)).await;

    // No row at all.
    send(&mut socket, renew(UNKNOWN_LEASE, 4), None).await;
    let mut frames = recv_until(&mut socket, &["lease_revoke"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    let revoke = expect_revoke(&frames, UNKNOWN_LEASE, 4, "moved");
    send(&mut socket, ok_result(), Some(&revoke)).await;
    assert!(server.state.hub.authority().passed(&lease_id));
}
