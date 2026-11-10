//! The primary gives a fenced lease back to its node from `hello`
//! (`node-agent-protocol.md` section 8.5, "Regrant from `hello`"; ADR 0281;
//! issue #622), over real HTTP and WebSocket with a fake node.

mod common;

use common::{Server, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::authority::{NoVoterSupport, VoterSupport};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, Kvm as WireKvm, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorCode, LeaseId, LeaseRenew, ProtocolError};
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
    })
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
        quarantines: Vec::new(),
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

/// Waits for a `desired_state` whose VM of `lease_id` carries `token`,
/// looking first among `frames`: a stateful VM starts only when its spec's
/// token matches the lease's (section 6.4).
async fn await_spec_token(socket: &mut Socket, frames: &[Value], lease_id: &str, token: u64) {
    let carries = |frame: &Value| {
        frame["type"] == "desired_state"
            && frame["body"]["spec"]["vms"].as_array().is_some_and(|vms| {
                vms.iter()
                    .any(|vm| vm["lease_id"] == lease_id && vm["fencing_token"] == token)
            })
    };
    if frames.iter().any(carries) {
        return;
    }
    while !carries(&recv_frame(socket).await) {}
}

/// A majority of voters supports every duration.
#[derive(Debug)]
struct EverySupport;

impl VoterSupport for EverySupport {
    fn supports(&self, _duration_ms: u64) -> bool {
        true
    }
}

/// Whether a majority of voters supports this primary from now on.
fn support(server: &Server, supported: bool) {
    let support: Arc<dyn VoterSupport> = if supported {
        Arc::new(EverySupport)
    } else {
        Arc::new(NoVoterSupport)
    };
    server.state.hub.set_voter_support(support);
}

async fn set_policy(server: &Server, app_id: &str, policy: &str) {
    sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
        .bind(policy)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

fn renew(lease_id: &str, token: u64) -> NodeMessage {
    NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: token,
    })
}

async fn row(server: &Server, lease_id: &str) -> (i64, Option<String>, String) {
    sqlx::query_as("SELECT fencing_token, revoked_at, mode FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// The row as (token, mode, `duration_ms`, unsettled).
async fn marked_row(server: &Server, lease_id: &str) -> (i64, String, Option<i64>, i64) {
    sqlx::query_as("SELECT fencing_token, mode, duration_ms, unsettled FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// `grant_pending` and `local_pending` of the app's lease (section 8.4,
/// "Grants of a marked row" and "The mark").
async fn pending(server: &Server, owner: &reqwest::Client, app_id: &str) -> (bool, bool) {
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        app["lease"]["grant_pending"].as_bool().unwrap(),
        app["lease"]["local_pending"].as_bool().unwrap(),
    )
}

/// Every frame the primary sends past the held-grant task's tick, up to two
/// pongs: an attempt that could have sent a held grant has run.
async fn frames_after_a_tick(socket: &mut Socket) -> Vec<Value> {
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    frames_until_settled(socket).await
}

/// Section 8.5: a lease the `hello` lists `expired` at the row's token is
/// given back at the next token, `local`, with the row written first; the
/// same-token grant the node would refuse is not sent. A desired state
/// carrying the new token follows, so the app's VM can start.
#[tokio::test]
async fn a_lease_listed_expired_is_given_back_at_a_higher_token() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;

    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 1], "{frames:?}");
    assert!(of_type(&frames, "lease_revoke").is_empty(), "{frames:?}");
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["body"]["mode"], "local");
    assert_eq!(
        row(&server, &lease_id).await,
        (i64::try_from(token + 1).unwrap(), None, "local".to_string())
    );
    await_spec_token(&mut socket, &frames, &lease_id, token + 1).await;
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
    let settled = frames_until_settled(&mut socket).await;
    assert!(grants_of(&settled, &lease_id).is_empty(), "{settled:?}");
    assert!(of_type(&settled, "lease_revoke").is_empty(), "{settled:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (i64::try_from(token + 2).unwrap(), None, "local".to_string())
    );
    let mut seen = frames;
    seen.extend(settled);
    await_spec_token(&mut socket, &seen, &lease_id, token + 2).await;
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
            term: None,
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
/// `bad_message`, on a session whose `hello` listed the lease `held` or
/// `uncertain` at that token, means the lease expired on its way: it is
/// given back. Another refusal code gives nothing back, and an `expiring`
/// row gets no same-token grant to refuse.
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
            term: None,
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
    await_spec_token(&mut socket, &frames, &lease_id, token + 1).await;
    drop(socket);

    // Listed `uncertain` at the row's token: the same.
    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token + 1, "uncertain").await;
    let grant_id = of_type(&frames, "lease_grant")[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    send(&mut socket, refuse(ErrorCode::BadMessage), Some(&grant_id)).await;
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 2]);
    drop(socket);

    // An `expiring` row listed `held` gets no same-token grant at all
    // (section 8.4), so there is nothing to refuse, and nothing is given
    // back.
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token + 2, "held").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let frames = frames_until_settled(&mut socket).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await.0,
        i64::try_from(token + 2).unwrap()
    );
}

/// Section 8.4: an `expiring` row's lease is given back only once its gate
/// has passed, on the same session. A `cold` app's `local` regrant over that
/// row sets the row's unsettled mark ("The mark"), so once the gate has
/// passed it is written `local` with the mark at the next token and its
/// grant held until the write is replicated (section 8.4, "Grants of a
/// marked row"; issue #703); the node does not follow the log here, so
/// nothing goes out. The row is then set back by hand to an unmarked
/// `expiring` row at that token, and a `warm` app with voter support gets
/// the lease back `expiring` once the gate has passed. The row stores `D`;
/// the seed counts the gate at 3 s, the test's shortcut for a gate that has
/// run most of its course. Before #703 the `cold` regrant wrote nothing.
#[tokio::test]
async fn an_expiring_rows_regrant_waits_for_its_gate() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    // 3 s and two margins of 1 s from now.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(3_000));
    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token, "expired").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    tokio::time::sleep(Duration::from_millis(5_500)).await;
    let frames = frames_after_a_tick(&mut socket).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let marked = i64::try_from(token + 1).unwrap();
    assert_eq!(
        marked_row(&server, &lease_id).await,
        (marked, "local".to_string(), None, 1),
        "written `local` with the mark"
    );
    assert_eq!(pending(&server, &owner, &app_id).await, (true, true));
    drop(socket);

    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000, unsettled = 0 WHERE id = ?",
        &lease_id,
    )
    .await;
    set_policy(&server, &app_id, "warm").await;
    support(&server, true);
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(3_000));
    let seeded = tokio::time::Instant::now();
    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, token + 1, "expired").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert!(seeded.elapsed() >= Duration::from_millis(4_900));
    assert_eq!(grants_of(&frames, &lease_id), vec![token + 2]);
    assert_eq!(
        of_type(&frames, "lease_grant")[0]["body"]["mode"],
        "expiring"
    );
    assert_eq!(
        row(&server, &lease_id).await,
        (
            i64::try_from(token + 2).unwrap(),
            None,
            "expiring".to_string()
        )
    );
    await_spec_token(&mut socket, &frames, &lease_id, token + 2).await;
}

/// The regrant has the mode the app's HA policy wants (D-0047, #604): a
/// `cold` app's lease comes back `local`, with or without voter support, and
/// a `warm` app's `expiring` with duration `D` while voters support it, over
/// a row that stores `D`. The node then renews at once (section 8.4, "First
/// grant"), and the primary answers that renewal. A regrant that must wait
/// is written with the mark at the next token and its grant held, and
/// nothing goes out on that session (section 8.4, "Grants of a marked row";
/// issue #703): a `warm` or `hot` app's `expiring` regrant over the `local`
/// row that stores no duration raises it (ADR 0268), with or without
/// support. After each, the row is set back by hand to the unmarked `local`
/// row it was, so the next case starts from it. Before #703 those regrants
/// wrote nothing.
#[tokio::test]
async fn the_regrant_has_the_mode_the_policy_wants() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;

    let mut current = token;
    for (policy, supported) in [("cold", true), ("cold", false)] {
        set_policy(&server, &app_id, policy).await;
        support(&server, supported);
        let (socket, frames) =
            reconnect(&server, &node, &app_id, &lease_id, current, "expired").await;
        assert_eq!(
            grants_of(&frames, &lease_id),
            vec![current + 1],
            "{policy}: {frames:?}"
        );
        let grant = of_type(&frames, "lease_grant")[0];
        assert_eq!(grant["body"]["mode"], "local", "{policy}");
        assert!(grant["body"].get("duration_ms").is_none(), "{policy}");
        current += 1;
        assert_eq!(
            row(&server, &lease_id).await,
            (i64::try_from(current).unwrap(), None, "local".to_string())
        );
        drop(socket);
    }

    // Without support, and with support over the `local` row that stores no
    // duration: a raise, written with the mark and held.
    for (policy, supported) in [("warm", false), ("hot", false), ("warm", true)] {
        set_policy(&server, &app_id, policy).await;
        support(&server, supported);
        let (mut socket, frames) =
            reconnect(&server, &node, &app_id, &lease_id, current, "expired").await;
        let mut frames = frames;
        frames.extend(frames_after_a_tick(&mut socket).await);
        assert!(
            grants_of(&frames, &lease_id).is_empty(),
            "{policy}: {frames:?}"
        );
        assert_eq!(
            marked_row(&server, &lease_id).await,
            (
                i64::try_from(current + 1).unwrap(),
                "expiring".to_string(),
                Some(15_000),
                1
            ),
            "{policy}, supported {supported}: written with the mark"
        );
        assert_eq!(
            pending(&server, &owner, &app_id).await,
            (true, false),
            "{policy}"
        );
        drop(socket);
        sqlx::query(
            "UPDATE leases SET fencing_token = ?, mode = 'local', duration_ms = NULL, \
             unsettled = 0 WHERE id = ?",
        )
        .bind(i64::try_from(current).unwrap())
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    }

    // Over a row that stores `D`, `expiring`. The new holder's gate then
    // holds every later new token for `D + 2M`, so this case comes last.
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let (mut socket, frames) =
        reconnect(&server, &node, &app_id, &lease_id, current, "expired").await;
    assert_eq!(
        grants_of(&frames, &lease_id),
        vec![current + 1],
        "{frames:?}"
    );
    let grant = of_type(&frames, "lease_grant")[0];
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    current += 1;
    assert_eq!(
        row(&server, &lease_id).await,
        (
            i64::try_from(current).unwrap(),
            None,
            "expiring".to_string()
        )
    );
    let stored: Option<i64> = sqlx::query_scalar("SELECT duration_ms FROM leases WHERE id = ?")
        .bind(&lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(stored, Some(15_000));
    // The gate is the new holder's: a new token waits for it.
    assert!(!server.state.hub.authority().passed(&lease_id));
    await_spec_token(&mut socket, &frames, &lease_id, current).await;

    let renewal = send(&mut socket, renew(&lease_id, current), None).await;
    let frames = frames_until_settled(&mut socket).await;
    let answers: Vec<&Value> = of_type(&frames, "lease_grant")
        .into_iter()
        .filter(|frame| frame["re"] == renewal.as_str())
        .collect();
    assert_eq!(answers.len(), 1, "{frames:?}");
    assert_eq!(answers[0]["body"]["fencing_token"], current);
    assert_eq!(answers[0]["body"]["mode"], "expiring");
}

/// A new session of `node` whose `hello` lists no lease. Returns the socket
/// and every frame up to the end of the session start.
async fn session_listing_nothing(server: &Server, node: &Enrolled) -> (Socket, Vec<Value>) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, Vec::new()), None).await;
    let mut frames = recv_until(&mut socket, &["welcome", "desired_state"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    (socket, frames)
}

/// Section 8.4: an `expiring` lease the holder's `hello` lists `held` or
/// `uncertain` at the row's token gets no grant at a session start: the
/// node renews it, and a same-token `expiring` grant without `re` has no row
/// in the node state machine. A holder that does not list the lease gets its
/// first grant again, without `re`, only when the row carries no unsettled
/// mark, while voters support the primary (section 8.8), and once the
/// lease's gate has passed, as for a grant with a new token: while the gate
/// is open, on the same session once it has passed. The authority records
/// it, so the holder's gate covers it, and the node's renewal is answered.
#[tokio::test]
async fn an_expiring_lease_is_granted_again_only_to_a_holder_that_lacks_it() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    set_policy(&server, &app_id, "warm").await;
    let passed = || server.state.hub.authority().passed(&lease_id);

    support(&server, true);
    for state in ["held", "uncertain"] {
        let (socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, state).await;
        assert!(
            grants_of(&frames, &lease_id).is_empty(),
            "{state}: {frames:?}"
        );
        drop(socket);
    }
    assert!(passed(), "nothing was recorded");

    support(&server, false);
    let (socket, frames) = session_listing_nothing(&server, &node).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert!(passed(), "nothing was recorded");
    drop(socket);

    support(&server, true);
    run(
        &server,
        "UPDATE leases SET unsettled = 1 WHERE id = ?",
        &lease_id,
    )
    .await;
    let (socket, frames) = session_listing_nothing(&server, &node).await;
    assert!(
        grants_of(&frames, &lease_id).is_empty(),
        "marked: {frames:?}"
    );
    assert!(passed(), "nothing was recorded");
    drop(socket);
    run(
        &server,
        "UPDATE leases SET unsettled = 0 WHERE id = ?",
        &lease_id,
    )
    .await;

    // A gate still open, 3 s and two margins of 1 s from now: nothing at
    // the session start, and the grant on the same session once the gate
    // has passed.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(3_000));
    let seeded = tokio::time::Instant::now();
    let (mut socket, frames) = session_listing_nothing(&server, &node).await;
    assert!(
        grants_of(&frames, &lease_id).is_empty(),
        "gate open: {frames:?}"
    );
    let frames = recv_until(&mut socket, &["lease_grant"]).await;
    assert!(seeded.elapsed() >= Duration::from_millis(4_900));
    assert_eq!(grants_of(&frames, &lease_id), vec![token], "{frames:?}");
    let grant = of_type(&frames, "lease_grant")
        .into_iter()
        .find(|frame| frame["body"]["lease_id"] == lease_id.as_str())
        .unwrap();
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    assert!(grant.get("re").is_none_or(Value::is_null), "{grant}");
    assert!(!passed(), "the holder's gate covers the grant");

    let renewal = send(&mut socket, renew(&lease_id, token), None).await;
    let frames = frames_until_settled(&mut socket).await;
    let answers: Vec<&Value> = of_type(&frames, "lease_grant")
        .into_iter()
        .filter(|frame| frame["re"] == renewal.as_str())
        .collect();
    assert_eq!(answers.len(), 1, "{frames:?}");
    assert_eq!(answers[0]["body"]["fencing_token"], token);
    drop(socket);

    // Once the gate has passed, a session start sends it at once.
    server
        .state
        .hub
        .authority()
        .seed(&lease_id, &node.node_id, Some(3_000));
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    let (socket, frames) = session_listing_nothing(&server, &node).await;
    assert_eq!(grants_of(&frames, &lease_id), vec![token], "{frames:?}");
    drop(socket);
}

/// Section 8.4: an `expiring` lease the holder's `hello` lists at a token
/// below the row's gets the stale-lease revoke at a session start, and no
/// grant at the row's token, which the node would refuse (section 5.12),
/// although voters support the primary and the lease's gate has passed.
#[tokio::test]
async fn an_expiring_lease_listed_below_the_rows_token_gets_no_grant() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000, \
         fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_id,
    )
    .await;
    set_policy(&server, &app_id, "warm").await;
    support(&server, true);
    assert!(server.state.hub.authority().passed(&lease_id));

    let (socket, frames) = reconnect(&server, &node, &app_id, &lease_id, token, "held").await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert!(
        of_type(&frames, "lease_revoke")
            .iter()
            .any(|frame| frame["body"]["lease_id"] == lease_id.as_str()),
        "{frames:?}"
    );
    assert!(
        server.state.hub.authority().passed(&lease_id),
        "nothing was recorded"
    );
    drop(socket);
}

/// A regrant that waits for a gate decides again once the gate has passed:
/// a row that moved to another holder, went to a higher token, was revoked,
/// or whose lease another node's failed fence lists, is not given back. The
/// app is `warm` with voter support, so the regrant would be `expiring` and
/// waits for the gate rather than for the unsettled mark.
#[tokio::test]
async fn a_waiting_regrant_gives_nothing_back_once_the_row_changed() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, app_id, lease_id, token) = node_with_lease(&server, &owner).await;
    set_policy(&server, &app_id, "warm").await;
    support(&server, true);
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
            "UPDATE leases SET mode = 'expiring', duration_ms = 15000, holder_node_id = ?, \
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
        let changed = row(&server, &lease_id).await;
        // The gate passes 5 s after the seed; wait past it.
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        let frames = frames_until_settled(&mut socket).await;
        assert!(
            grants_of(&frames, &lease_id).is_empty(),
            "{case}: {frames:?}"
        );
        // Nothing was written either.
        assert_eq!(row(&server, &lease_id).await, changed, "{case}");
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
            term: None,
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
