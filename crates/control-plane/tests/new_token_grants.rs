//! A new token for an app's lease, through `createVolume`: its mode, the
//! previous holder's gate, and ADR 0258 on the primary's side
//! (`node-agent-protocol.md` sections 8.4 and 8.8; issue #604), over real
//! HTTP and WebSocket with a fake node.

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

/// A majority of voters supports every duration.
#[derive(Debug)]
struct EverySupport;

impl VoterSupport for EverySupport {
    fn supports(&self, _duration_ms: u64) -> bool {
        true
    }
}

fn renew(lease_id: &str, token: u64) -> NodeMessage {
    NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: token,
    })
}

/// Whether a majority of voters supports this primary from now on.
fn set_support(server: &Server, supported: bool) {
    let support: Arc<dyn VoterSupport> = if supported {
        Arc::new(EverySupport)
    } else {
        Arc::new(quoro_control_plane::authority::NoVoterSupport)
    };
    server.state.hub.set_voter_support(support);
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

/// A connected node and a project.
async fn connected_node(server: &Server, owner: &reqwest::Client) -> (Enrolled, Socket, String) {
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
    let project_id = project["id"].as_str().unwrap().to_string();
    (node, socket, project_id)
}

/// An app named `name` with `ha_policy` set to `policy` in the store, as the
/// test-lab build's `updateApp` would after its volume exists (#632). The API
/// takes `warm` only for an app with a volume, and this test creates one.
async fn app_with_policy(
    server: &Server,
    owner: &reqwest::Client,
    project_id: &str,
    name: &str,
    policy: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": name,
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
    sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
        .bind(policy)
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    app_id
}

async fn create_volume(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap()
}

/// The `detail` of a `409 conflict` problem.
async fn conflict_detail(res: reqwest::Response) -> String {
    assert_eq!(res.status(), 409);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["code"], "conflict", "{body}");
    body["detail"].as_str().unwrap_or_default().to_string()
}

/// Creates the app's volume and returns its id and the grant the node got.
async fn volume_and_grant(
    server: &Server,
    owner: &reqwest::Client,
    socket: &mut Socket,
    app_id: &str,
) -> (String, Value) {
    let res = create_volume(server, owner, app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let frames = recv_until(socket, &["lease_grant"]).await;
    let grant = frames
        .into_iter()
        .find(|frame| frame["type"] == "lease_grant")
        .unwrap();
    (volume_id, grant)
}

/// Deletes the volume and returns the id of the revoke the node got.
async fn delete_volume(
    server: &Server,
    owner: &reqwest::Client,
    socket: &mut Socket,
    volume_id: &str,
) -> String {
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let frames = recv_until(socket, &["lease_revoke"]).await;
    let revoke = frames
        .into_iter()
        .find(|frame| frame["type"] == "lease_revoke")
        .unwrap();
    assert_eq!(revoke["body"]["reason"], "deleted");
    revoke["id"].as_str().unwrap().to_string()
}

async fn mode_and_duration(server: &Server, lease_id: &str) -> (String, Option<i64>) {
    sqlx::query_as("SELECT mode, duration_ms FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Gives the app a released lease whose row reads `expiring` for `D`: a
/// first volume while the app's policy is `none`, deleted, its revoke
/// answered, then the row set as an `expiring` lease's reads. The authority
/// keeps no gate for it, as once its gate has passed. Then sets the app's
/// policy to `policy`. Returns the lease id.
async fn released_expiring_lease(
    server: &Server,
    owner: &reqwest::Client,
    socket: &mut Socket,
    app_id: &str,
    policy: &str,
) -> String {
    let set_policy = |policy: &str| {
        sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
            .bind(policy.to_string())
            .bind(app_id.to_string())
            .execute(server.state.store.pool())
    };
    set_policy("none").await.unwrap();
    let (volume_id, grant) = volume_and_grant(server, owner, socket, app_id).await;
    let lease_id = grant["body"]["lease_id"].as_str().unwrap().to_string();
    let revoke_id = delete_volume(server, owner, socket, &volume_id).await;
    send(socket, ok_result(), Some(&revoke_id)).await;
    frames_until_settled(socket).await;
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    set_policy(policy).await.unwrap();
    lease_id
}

/// #604, section 8.4 "First grant": a `cold` app's lease is `local`, with
/// or without voter support. A `warm` or `hot` app's new token is `expiring`
/// for `D` while a majority of voters supports `D`, over a previous row that
/// already stores `D` (ADR 0268), with the row written first, and the node's
/// first renewal is answered.
#[tokio::test]
async fn a_new_volume_gets_the_mode_the_policy_wants() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, project_id) = connected_node(&server, &owner).await;

    for (name, supported) in [("web-cold", true), ("web-cold-alone", false)] {
        set_support(&server, supported);
        let app_id = app_with_policy(&server, &owner, &project_id, name, "cold").await;
        let (_, grant) = volume_and_grant(&server, &owner, &mut socket, &app_id).await;
        assert_eq!(grant["body"]["mode"], "local", "{name}: {grant}");
        assert_eq!(grant["body"]["fencing_token"], 1, "{name}");
        assert!(grant["body"].get("duration_ms").is_none(), "{name}");
        let lease_id = grant["body"]["lease_id"].as_str().unwrap();
        assert_eq!(
            mode_and_duration(&server, lease_id).await,
            ("local".to_string(), None),
            "{name}"
        );
    }

    set_support(&server, true);
    for (name, policy) in [("web-warm", "warm"), ("web-hot", "hot")] {
        let app_id = app_with_policy(&server, &owner, &project_id, name, policy).await;
        let lease_id = released_expiring_lease(&server, &owner, &mut socket, &app_id, policy).await;
        let (_, grant) = volume_and_grant(&server, &owner, &mut socket, &app_id).await;
        assert_eq!(grant["body"]["mode"], "expiring", "{name}: {grant}");
        assert_eq!(grant["body"]["fencing_token"], 2, "{name}");
        assert_eq!(grant["body"]["duration_ms"], 15_000, "{name}");
        assert_eq!(
            mode_and_duration(&server, &lease_id).await,
            ("expiring".to_string(), Some(15_000)),
            "{name}"
        );
        let renewal = send(&mut socket, renew(&lease_id, 2), None).await;
        let frames = frames_until_settled(&mut socket).await;
        let answers: Vec<&Value> = frames
            .iter()
            .filter(|frame| frame["type"] == "lease_grant" && frame["re"] == renewal.as_str())
            .collect();
        assert_eq!(answers.len(), 1, "{name}: {frames:?}");
    }
}

/// Section 8.4: a `warm` or `hot` app's first volume has no previous lease
/// row. Without voter support its new token would be `local`, a write that
/// sets the row's unsettled mark ("The mark", ADR 0285). With support it
/// would be `expiring` for `D` over a row that stored no duration, a raise
/// (ADR 0268). Either grant goes out only once its write is replicated over
/// the machines with the `node` role, which this build cannot count, so
/// `createVolume` answers `conflict` and creates nothing.
#[tokio::test]
async fn a_warm_apps_first_volume_creates_nothing_while_its_grant_is_held() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, project_id) = connected_node(&server, &owner).await;

    for (name, policy, supported) in [
        ("web-warm-alone", "warm", false),
        ("web-hot-alone", "hot", false),
        ("web-warm", "warm", true),
        ("web-hot", "hot", true),
    ] {
        set_support(&server, supported);
        let app_id = app_with_policy(&server, &owner, &project_id, name, policy).await;
        let detail = conflict_detail(create_volume(&server, &owner, &app_id).await).await;
        assert!(detail.contains("cannot be granted yet"), "{name}: {detail}");
        let (volumes, leases): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM volumes WHERE app_id = ?1), \
             (SELECT COUNT(*) FROM leases WHERE resource_id = ?1)",
        )
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        assert_eq!((volumes, leases), (0, 0), "{name}: nothing was written");
        let frames = frames_until_settled(&mut socket).await;
        assert!(
            !frames.iter().any(|frame| frame["type"] == "lease_grant"),
            "{name}: {frames:?}"
        );
    }
}

/// Section 8.4, #604: a new token waits until the previous `expiring`
/// holder's gate has passed. `createVolume` answers `conflict` meanwhile and
/// writes nothing; once the gate has passed, a `warm` app with voter support
/// gets the next token `expiring` over a row that stores `D`. Three new
/// tokens stay held after the gate too, because each goes out only once its
/// write is replicated: an `expiring` one over a row that stores less than
/// `D` (a raise, ADR 0268), a `local` one over the row that reads
/// `expiring`, for a `none` app ("The mark"), and a `warm` app's without
/// support ("The mark", ADR 0285). The seed counts the gate at 3 s, the
/// test's shortcut for a gate that has run most of its course.
#[tokio::test]
async fn a_new_volume_waits_for_the_previous_expiring_holders_gate() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, project_id) = connected_node(&server, &owner).await;
    for (name, policy, supported, stored, mode) in [
        ("web-warm", "warm", true, 15_000, Some("expiring")),
        ("web-warm-short", "warm", true, 3_000, None),
        ("web-none", "none", false, 15_000, None),
        ("web-warm-alone", "warm", false, 15_000, None),
    ] {
        set_support(&server, supported);
        let app_id = app_with_policy(&server, &owner, &project_id, name, "none").await;
        let (volume_id, grant) = volume_and_grant(&server, &owner, &mut socket, &app_id).await;
        let lease_id = grant["body"]["lease_id"].as_str().unwrap().to_string();
        let revoke_id = delete_volume(&server, &owner, &mut socket, &volume_id).await;
        send(&mut socket, ok_result(), Some(&revoke_id)).await;
        frames_until_settled(&mut socket).await;
        sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
            .bind(policy)
            .bind(&app_id)
            .execute(server.state.store.pool())
            .await
            .unwrap();

        // The lease was `expiring`: its gate passes 5 s after the seed (3 s
        // and two margins of 1 s).
        sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = ? WHERE id = ?")
            .bind(stored)
            .bind(&lease_id)
            .execute(server.state.store.pool())
            .await
            .unwrap();
        server
            .state
            .hub
            .authority()
            .seed(&lease_id, &node.node_id, Some(3_000));
        let passes = tokio::time::Instant::now() + Duration::from_millis(5_000);
        let refused = if mode.is_some() {
            "gate is open"
        } else {
            "cannot be granted yet"
        };
        refused_unwritten(&server, &owner, name, &app_id, &lease_id, refused).await;
        assert!(
            tokio::time::Instant::now() < passes,
            "{name}: the test ran too slowly"
        );

        tokio::time::sleep_until(passes).await;
        if let Some(mode) = mode {
            let (_, grant) = volume_and_grant(&server, &owner, &mut socket, &app_id).await;
            assert_eq!(grant["body"]["fencing_token"], 2, "{name}");
            assert_eq!(grant["body"]["mode"], mode, "{name}");
        } else {
            refused_unwritten(&server, &owner, name, &app_id, &lease_id, refused).await;
        }
    }
}

/// `createVolume` answers `conflict` with `detail` containing `refused`, and
/// writes nothing: the lease is still the deleted volume's, at token 1 and
/// `expiring`, and only that volume exists.
async fn refused_unwritten(
    server: &Server,
    owner: &reqwest::Client,
    name: &str,
    app_id: &str,
    lease_id: &str,
    refused: &str,
) {
    let detail = conflict_detail(create_volume(server, owner, app_id).await).await;
    assert!(detail.contains(refused), "{name}: {detail}");
    let (token, stored): (i64, String) =
        sqlx::query_as("SELECT fencing_token, mode FROM leases WHERE id = ?")
            .bind(lease_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(
        (token, stored.as_str()),
        (1, "expiring"),
        "{name}: nothing was written"
    );
    let volumes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM volumes WHERE app_id = ?")
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(volumes, 1, "{name}: only the deleted volume");
}

/// ADR 0258 on the primary's side (#604): a new token reaches the node after
/// the revoke of the app's previous lease on its session, so the node takes
/// it once it has answered that revoke, with the lease `revoked`, and the
/// grant's mode is free. A `warm` app with voter support gets the new token
/// `expiring` while the revoke is not answered yet. The previous row reads
/// `expiring` for `D`, so the new token raises nothing (ADR 0268); the
/// authority keeps no gate for it here, as once its gate has passed.
#[tokio::test]
async fn a_new_token_follows_the_revoke_of_the_previous_lease() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, project_id) = connected_node(&server, &owner).await;
    let app_id = app_with_policy(&server, &owner, &project_id, "web", "none").await;
    let (volume_id, first) = volume_and_grant(&server, &owner, &mut socket, &app_id).await;
    assert_eq!(first["body"]["mode"], "local");
    sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(first["body"]["lease_id"].as_str().unwrap())
        .execute(server.state.store.pool())
        .await
        .unwrap();
    set_support(&server, true);

    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let frames = recv_until(&mut socket, &["lease_revoke", "lease_grant"]).await;
    let revoke = frames
        .iter()
        .position(|frame| frame["type"] == "lease_revoke")
        .unwrap();
    let grant = frames
        .iter()
        .position(|frame| frame["type"] == "lease_grant")
        .unwrap();
    assert!(revoke < grant, "{frames:?}");
    assert_eq!(frames[revoke]["body"]["fencing_token"], 1);
    assert_eq!(frames[grant]["body"]["fencing_token"], 2);
    assert_eq!(frames[grant]["body"]["mode"], "expiring", "{frames:?}");
}

/// ADR 0258 and section 8.4: a volume that failed leaves the app's lease
/// unrevoked, so the node still holds it `local`. A new volume's token on
/// that node keeps `local`, and no revoke goes out. For a `warm` app, even
/// with voter support, that `local` write would set the row's unsettled mark
/// (ADR 0285), so `createVolume` answers `conflict` and writes nothing.
#[tokio::test]
async fn a_new_token_to_a_node_that_still_holds_the_lease_stays_local() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (_node, mut socket, project_id) = connected_node(&server, &owner).await;
    let app_id = app_with_policy(&server, &owner, &project_id, "web", "none").await;
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let rev = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap()["body"]["rev"]
        .as_u64()
        .unwrap();
    send(
        &mut socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Failed,
                allocated_bytes: None,
                error: Some("the disk is full".to_string()),
            }],
        ),
        None,
    )
    .await;
    frames_until_settled(&mut socket).await;
    let (state, revoked): (String, Option<String>) = sqlx::query_as(
        "SELECT v.state, l.revoked_at FROM volumes v JOIN leases l ON l.resource_id = v.app_id \
         WHERE v.id = ?",
    )
    .bind(&volume_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!((state.as_str(), revoked), ("failed", None));

    sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    set_support(&server, true);
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let detail = conflict_detail(create_volume(&server, &owner, &app_id).await).await;
    assert!(detail.contains("cannot be granted yet"), "{detail}");
    let (token, revoked): (i64, Option<String>) =
        sqlx::query_as("SELECT fencing_token, revoked_at FROM leases WHERE resource_id = ?")
            .bind(&app_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!((token, revoked), (1, None), "nothing was written");

    sqlx::query("UPDATE apps SET ha_policy = 'none' WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let mut frames = recv_until(&mut socket, &["lease_grant"]).await;
    frames.extend(frames_until_settled(&mut socket).await);
    assert!(
        !frames.iter().any(|frame| frame["type"] == "lease_revoke"),
        "{frames:?}"
    );
    let grant = frames
        .iter()
        .find(|frame| frame["type"] == "lease_grant")
        .unwrap();
    assert_eq!(grant["body"]["fencing_token"], 2);
    assert_eq!(grant["body"]["mode"], "local", "{grant}");
}
