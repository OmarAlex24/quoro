//! The primary gives a fenced lease back to its node from `hello`
//! (`node-agent-protocol.md` section 8.5, "Regrant from `hello`"; ADR 0281;
//! issue #622), over real HTTP and WebSocket with a fake node.

mod common;

use common::{Server, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, Kvm as WireKvm, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorCode, ProtocolError};
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

async fn run(server: &Server, sql: &str, lease_id: &str) {
    sqlx::query(sql)
        .bind(lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

/// A node, connected, holding the `local` lease of a stateful app whose
/// volume is ready. Returns the node, the app, the lease and its token.
async fn node_with_lease(
    server: &Server,
    owner: &reqwest::Client,
) -> (Enrolled, String, String, u64) {
    let node = enrolled_node(server, owner, "node-alpha").await;
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, Vec::new()), None).await;
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
    socket.close(None).await.unwrap();
    (node, app_id, lease_id, token)
}

/// A new session of `node` whose `hello` lists `lease_id` of `app_id` at
/// `token` in `state`. Returns the socket and every frame up to the end of
/// the session start.
async fn reconnect(
    server: &Server,
    node: &Enrolled,
    app_id: &str,
    lease_id: &str,
    token: u64,
    state: &str,
) -> (Socket, Vec<Value>) {
    let mut socket = connect(server, &node.credential).await;
    let listed = wire::HelloLease {
        lease_id: lease_id.to_string(),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: app_id.to_string(),
        mode: quoro_proto::LeaseMode::Local,
        fencing_token: token,
        state: state.to_string(),
    };
    send(&mut socket, hello(&node.node_id, vec![listed]), None).await;
    let mut frames = recv_until(&mut socket, &["welcome", "desired_state"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    (socket, frames)
}

/// The grants of `lease_id` among `frames`, by token.
fn grants_of(frames: &[Value], lease_id: &str) -> Vec<u64> {
    of_type(frames, "lease_grant")
        .into_iter()
        .filter(|frame| frame["body"]["lease_id"] == lease_id)
        .map(|frame| frame["body"]["fencing_token"].as_u64().unwrap())
        .collect()
}

async fn row(server: &Server, lease_id: &str) -> (i64, Option<String>, String) {
    sqlx::query_as("SELECT fencing_token, revoked_at, mode FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Section 8.5: a lease the `hello` lists `expired` at the row's token is
/// given back at the next token, `local`, with the row written first; the
/// same-token grant the node would refuse is not sent.
#[tokio::test]
async fn a_lease_listed_expired_is_given_back_at_a_higher_token() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;

    let (_socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 1], "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["body"]["mode"], "local");
    assert_eq!(
        row(&server, &lease_id).await,
        (i64::try_from(token + 1).unwrap(), None, "local".to_string())
    );
}

/// A lease listed `expired` below the row's token is revoked first (section
/// 5.1), then given back above the row's token; the node holds the grant
/// back until it has answered the revoke.
#[tokio::test]
async fn an_expired_lease_below_the_rows_token_is_revoked_then_given_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_id,
    )
    .await;

    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    let revoke = frames
        .iter()
        .position(|frame| frame["type"] == "lease_revoke")
        .expect("a revoke");
    let grant = frames
        .iter()
        .position(|frame| frame["type"] == "lease_grant")
        .expect("a grant");
    assert!(revoke < grant, "{frames:?}");
    assert_eq!(frames[revoke]["body"]["fencing_token"], token + 1);
    assert_eq!(frames[revoke]["body"]["reason"], "superseded");
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 2]);

    // The node answers the revoke, then takes the grant: nothing more goes
    // out for the lease.
    let revoke_id = frames[revoke]["id"].as_str().unwrap().to_string();
    send(&mut socket, ok_result(), Some(&revoke_id)).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (i64::try_from(token + 2).unwrap(), None, "local".to_string())
    );
}

/// ADR 0281: a lease the node lists `revoked` at or above the row's token is
/// never given back, and gets no grant at all. One listed `revoked` below
/// the row's token is revoked (section 5.1), and not given back either.
#[tokio::test]
async fn a_lease_listed_revoked_is_never_given_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;

    for listed in [token, token + 1] {
        let (socket, frames) =
            reconnect(&server, &node, &app_id, &lease_id, listed, "revoked").await;
        assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
        assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
        drop(socket);
    }
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token).unwrap()
    );

    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_id,
    )
    .await;
    let (_socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "revoked").await;
    let revokes = of_type(&frames, "lease_revoke");
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["reason"], "superseded");
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token + 1).unwrap()
    );
}

/// The row must give the lease to the node, and the node must have reported
/// no failed fence of it: a revoked row, another holder, a lease in a
/// node's `fence_failure` or in this node's `fence_ack_pending`, or a node
/// token above the row's, is not given back.
#[tokio::test]
async fn a_lease_the_row_does_not_give_the_node_is_not_given_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    let other = enrolled_node(&server, &owner, "node-beta").await;
    let pool = server.state.store.pool();

    let check = |frames: &[Value], case: &str| {
        let higher: Vec<u64> = grants_of(frames, &lease_id)
            .into_iter()
            .filter(|granted| *granted > token)
            .collect();
        assert!(higher.is_empty(), "{case}: {frames:?}");
    };

    // Revoked in the primary's view, as a declaration of loss leaves it.
    run(
        &server,
        "UPDATE leases SET revoked_at = '2026-11-09T10:00:00Z' WHERE id = ?",
        &lease_id,
    )
    .await;
    let (socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    check(&frames, "revoked row");
    drop(socket);
    run(
        &server,
        "UPDATE leases SET revoked_at = NULL WHERE id = ?",
        &lease_id,
    )
    .await;

    // Another holder.
    sqlx::query("UPDATE leases SET holder_node_id = ? WHERE id = ?")
        .bind(&other.node_id)
        .bind(&lease_id)
        .execute(pool)
        .await
        .unwrap();
    let (socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    check(&frames, "another holder");
    drop(socket);
    sqlx::query("UPDATE leases SET holder_node_id = ? WHERE id = ?")
        .bind(&node.node_id)
        .bind(&lease_id)
        .execute(pool)
        .await
        .unwrap();

    // Listed in another node's failed fence.
    let failure = json!({
        "detected_at": "2026-11-09T10:00:00Z",
        "lease_ids": [lease_id],
        "app_ids": [app_id],
        "vm_ids": [],
    })
    .to_string();
    sqlx::query("UPDATE nodes SET fence_failure = ? WHERE id = ?")
        .bind(&failure)
        .bind(&other.node_id)
        .execute(pool)
        .await
        .unwrap();
    let (socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    check(&frames, "fence_failure");
    drop(socket);
    sqlx::query("UPDATE nodes SET fence_failure = NULL WHERE id = ?")
        .bind(&other.node_id)
        .execute(pool)
        .await
        .unwrap();

    // In this node's pending acknowledgement, whose delivery the session
    // start waits for: the node refuses it, so it stays pending.
    let pending = json!({
        "acked_at": "2026-11-09T10:00:00Z",
        "lease_ids": [lease_id],
        "fencing_tokens": { lease_id.clone(): token },
    })
    .to_string();
    sqlx::query("UPDATE nodes SET fence_ack_pending = ? WHERE id = ?")
        .bind(&pending)
        .bind(&node.node_id)
        .execute(pool)
        .await
        .unwrap();
    let mut socket = connect(&server, &node.credential).await;
    let listed = wire::HelloLease {
        lease_id: lease_id.clone(),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: app_id.clone(),
        mode: quoro_proto::LeaseMode::Local,
        fencing_token: token,
        state: "expired".to_string(),
    };
    send(&mut socket, hello(&node.node_id, vec![listed]), None).await;
    let frames = recv_until(&mut socket, &["welcome", "command"]).await;
    let command = of_type(&frames, "command")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    send(
        &mut socket,
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "refused".to_string(),
            fatal: false,
        }),
        Some(&command),
    )
    .await;
    let mut frames = recv_until(&mut socket, &["desired_state"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    check(&frames, "fence_ack_pending");
    drop(socket);
    sqlx::query("UPDATE nodes SET fence_ack_pending = NULL WHERE id = ?")
        .bind(&node.node_id)
        .execute(pool)
        .await
        .unwrap();

    // A node token above the row's.
    let (_socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token + 1, "expired").await;
    check(&frames, "a higher node token");
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token).unwrap()
    );
}

/// Section 8.5: a same-token `local` grant the node refuses with
/// `bad_message`, on a session whose `hello` listed the lease `held` at that
/// token, means the lease expired on its way: it is given back. Another
/// refusal code gives nothing back.
#[tokio::test]
async fn a_refused_same_token_grant_gives_the_lease_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;

    let refuse = |code: ErrorCode| {
        NodeMessage::Error(ProtocolError {
            code,
            message: "the lease is not held".to_string(),
            fatal: false,
        })
    };

    let (mut socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "held").await;
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["body"]["fencing_token"], token);
    let grant_id = grant["id"].as_str().unwrap().to_string();
    send(&mut socket, refuse(ErrorCode::NotFound), Some(&grant_id)).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    drop(socket);

    let (mut socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "held").await;
    let grant_id = of_type(&frames, "lease_grant")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    send(&mut socket, refuse(ErrorCode::BadMessage), Some(&grant_id)).await;
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 1]);
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token + 1).unwrap()
    );
}

/// Section 8.4: an `expiring` row's lease is given back only once its gate
/// has passed, on the same session.
#[tokio::test]
async fn an_expiring_rows_regrant_waits_for_its_gate() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 3000 WHERE id = ?",
        &lease_id,
    )
    .await;
    // 3 s and two margins of 1 s from now.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(3_000));
    let seeded = tokio::time::Instant::now();

    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert!(seeded.elapsed() >= Duration::from_millis(4_900));
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 1]);
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["body"]["mode"], "local");
    assert_eq!(
        row(&server, &lease_id).await,
        (i64::try_from(token + 1).unwrap(), None, "local".to_string())
    );
}

/// A `warm` app's lease is given back `expiring`, whose first grant is not
/// built yet (#604): it is not given back for now.
#[tokio::test]
async fn a_warm_apps_lease_is_not_given_back_yet() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE apps SET ha_policy = 'warm' WHERE id = ?",
        &app_id,
    )
    .await;

    let (_socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token).unwrap()
    );
}

/// A regrant that waits for a gate decides again once the gate has passed:
/// a row that moved to another holder, went to a higher token, was revoked,
/// or whose lease another node's failed fence lists, is not given back.
#[tokio::test]
async fn a_waiting_regrant_gives_nothing_back_once_the_row_changed() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    let other = enrolled_node(&server, &owner, "node-beta").await;
    let pool = server.state.store.pool();
    let failure = json!({
        "detected_at": "2026-11-09T10:00:00Z",
        "lease_ids": [lease_id],
        "app_ids": [app_id],
        "vm_ids": [],
    })
    .to_string();
    let changes: [(&str, &str); 4] = [
        (
            "another holder",
            "UPDATE leases SET holder_node_id = ? WHERE id = ?",
        ),
        (
            "a higher token",
            "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        ),
        (
            "revoked",
            "UPDATE leases SET revoked_at = '2026-11-09T10:00:00Z' WHERE id = ?",
        ),
        (
            "a failed fence",
            "UPDATE nodes SET fence_failure = ? WHERE id = ?",
        ),
    ];
    for (case, change) in changes {
        sqlx::query(
            "UPDATE leases SET mode = 'expiring', duration_ms = 3000, holder_node_id = ?, \
             fencing_token = ?, revoked_at = NULL WHERE id = ?",
        )
        .bind(&node.node_id)
        .bind(i64::try_from(token).unwrap())
        .bind(&lease_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("UPDATE nodes SET fence_failure = NULL")
            .execute(pool)
            .await
            .unwrap();
        server
            .state
            .hub
            .authority()
            .seed(&lease_id, &node.node_id, Some(3_000));
        let (mut socket, frames) =
            reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
        assert!(
            grants_of(&frames, &lease_id).is_empty(),
            "{case}: {frames:?}"
        );

        let query = sqlx::query(change);
        let query = match case {
            "another holder" => query.bind(&other.node_id).bind(&lease_id),
            "a failed fence" => query.bind(&failure).bind(&other.node_id),
            _ => query.bind(&lease_id),
        };
        query.execute(pool).await.unwrap();
        // The gate passes 5 s after the seed; wait past it.
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        let frames = frames_until_settled(&mut socket).await;
        assert!(
            grants_of(&frames, &lease_id).is_empty(),
            "{case}: {frames:?}"
        );
        drop(socket);
    }
}

/// A refused same-token grant gives nothing back when a `lease_revoke` of
/// the lease went out on the same session, here the revoke of a renewal
/// that crossed the row's newer token.
#[tokio::test]
async fn a_refused_grant_after_a_revoke_on_the_session_gives_nothing_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_id,
    )
    .await;

    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token + 1, "held").await;
    let grant_id = of_type(&frames, "lease_grant")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    send(
        &mut socket,
        NodeMessage::LeaseRenew(quoro_proto::LeaseRenew {
            lease_id: quoro_proto::LeaseId(lease_id.clone()),
            fencing_token: token,
        }),
        None,
    )
    .await;
    let frames = recv_until(&mut socket, &["lease_revoke"]).await;
    let revoke_id = of_type(&frames, "lease_revoke")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    send(&mut socket, ok_result(), Some(&revoke_id)).await;
    send(
        &mut socket,
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "the lease is not held".to_string(),
            fatal: false,
        }),
        Some(&grant_id),
    )
    .await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token + 1).unwrap()
    );
}
