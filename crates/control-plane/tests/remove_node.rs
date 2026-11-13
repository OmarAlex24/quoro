// SPDX-License-Identifier: AGPL-3.0-only
//! `removeNode` (`DELETE /api/v1/nodes/{node_id}`, `node-agent-protocol.md`
//! section 10; issue #740), over real HTTP and WebSocket with a fake node:
//! the refusals, a node that is not connected (its tokens go up as when it
//! is declared lost), a connected node (its leases are revoked over its
//! session and every answer awaited, then its session ends with
//! `node_removed`), the fences that are not confirmed, and the voter-set
//! gate (section 8.10, ADR 0321 item 4).

mod common;

use common::machine::{self, Machine};
use common::{Server, start_with_patient_node_session};
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
const OTHER_LEASE: &str = "lse_01JC9Z6Q5W8N3K4T7R2M1XHB0E";
const QUARANTINE: &str = "qrn_01JC9Z6Q5W8N3K4T7R2M1XHB0Q";
const OPERATION: &str = "DELETE /api/v1/nodes/{node_id}";

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
        custody_public_key: None,
        mesh_rules: None,
    })
}

async fn enrolled_node(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    let node = machine::enroll(server, owner, name, &["node"]).await;
    Enrolled {
        node_id: node.node_id,
        credential: node.credential,
    }
}

/// Opens a session WebSocket with `credential`; the upgrade must pass.
async fn connect(server: &Server, credential: &str) -> Socket {
    let (socket, status) = dial(server, credential).await;
    assert_eq!(status, 101);
    socket.expect("websocket upgrade")
}

/// Dials a session with `credential`: the socket when the upgrade passed,
/// and the HTTP status of the answer.
async fn dial(server: &Server, credential: &str) -> (Option<Socket>, u16) {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    match tokio_tungstenite::client_async(request, stream).await {
        Ok((socket, response)) => (Some(socket), response.status().as_u16()),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            (None, response.status().as_u16())
        }
        Err(e) => panic!("dial: {e}"),
    }
}

async fn send(socket: &mut Socket, message: NodeMessage) {
    reply(socket, message, None).await;
}

async fn reply(socket: &mut Socket, message: NodeMessage, re: Option<&str>) {
    let text = message
        .to_frame(&format!("msg_{}", machine::ulid_like()), re)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

async fn heartbeat(socket: &mut Socket) {
    send(
        socket,
        NodeMessage::Heartbeat(Heartbeat {
            time: "2026-11-12T10:15:32Z".to_string(),
            busy: None,
        }),
    )
    .await;
}

/// The next text frame, or `None` once the primary closed the socket.
async fn next_frame(socket: &mut Socket) -> Option<Value> {
    loop {
        let message = timeout(Duration::from_secs(30), socket.next())
            .await
            .expect("a frame within 30 s");
        match message {
            Some(Ok(WsMessage::Text(text))) => {
                return Some(serde_json::from_str(text.as_str()).unwrap());
            }
            Some(Ok(WsMessage::Close(_)) | Err(_)) | None => return None,
            Some(Ok(_)) => {}
        }
    }
}

async fn recv_frame(socket: &mut Socket) -> Value {
    next_frame(socket).await.expect("the socket is open")
}

/// Reads frames until one of each `wanted` type was seen, and returns them
/// in arrival order.
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

/// `removeNode` of `node_id` by `client`.
async fn remove(client: &reqwest::Client, server: &Server, node_id: &str) -> reqwest::Response {
    client
        .delete(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap()
}

/// The status, code, reason and detail of a problem answer.
async fn problem(res: reqwest::Response) -> (u16, String, Option<String>, String) {
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    (
        status,
        body["code"].as_str().unwrap_or_default().to_string(),
        body["reason"].as_str().map(str::to_string),
        body["detail"].as_str().unwrap_or_default().to_string(),
    )
}

async fn node_status(server: &Server, node_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// `(fencing_token, revoked_at, holder_node_id)` of a lease row.
async fn lease_row(server: &Server, lease_id: &str) -> (i64, Option<String>, String) {
    sqlx::query_as("SELECT fencing_token, revoked_at, holder_node_id FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

async fn log_head(server: &Server) -> i64 {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
        .unwrap_or(0)
}

/// `(seq, entity_type, entity_id, op, payload)` of the change-log entries
/// other than audit records written after `after`.
async fn entries_after(server: &Server, after: i64) -> Vec<(i64, String, String, String, Value)> {
    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, entity_type, entity_id, op, payload FROM change_log \
         WHERE seq > ? AND entity_type <> 'audit' ORDER BY seq",
    )
    .bind(after)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, kind, id, op, payload)| {
            (seq, kind, id, op, serde_json::from_str(&payload).unwrap())
        })
        .collect()
}

/// `(result, scope, params, change_seq, actor_type, target_type, target_id)`
/// of each `removeNode` record, oldest first.
type RemoveRecord = (
    String,
    Option<String>,
    String,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
);

async fn remove_records(server: &Server) -> Vec<RemoveRecord> {
    sqlx::query_as(
        "SELECT result, scope, params, change_seq, actor_type, target_type, target_id \
         FROM audit_log WHERE operation = ? ORDER BY seq",
    )
    .bind(OPERATION)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

/// `(params, target_id, change_seq)` of each `lease.revoke` record.
async fn lease_revoke_records(server: &Server) -> Vec<(Value, Option<String>, Option<i64>)> {
    let rows: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
        "SELECT params, target_id, change_seq FROM audit_log \
         WHERE operation = 'lease.revoke' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(params, target, seq)| (serde_json::from_str(&params).unwrap(), target, seq))
        .collect()
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

/// A node, connected, holding the `local` lease of a stateful app whose
/// volume is ready. Returns the node, its socket, the app, its VM, its
/// volume and its lease.
async fn node_with_lease(
    server: &Server,
    owner: &reqwest::Client,
) -> (Enrolled, Socket, String, String, String, String) {
    let node = enrolled_node(server, owner, "node-alpha").await;
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
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
                volume_id: volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
    )
    .await;
    heartbeat(&mut socket).await;
    (node, socket, app_id, vm_id, volume_id, lease_id)
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
            "name": "node-remover",
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

/// A quarantined copy the node keeps of `volume_id`, and a custody seal made
/// for it, as the primary records them (sections 7.2 and 9).
async fn records_of(server: &Server, node_id: &str, app_id: &str, volume_id: &str) {
    let now = quoro_control_plane::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO volume_quarantines (id, volume_id, app_id, node_id, size_bytes, sha256, \
         created_at) VALUES (?, ?, ?, ?, 4096, ?, ?)",
    )
    .bind(QUARANTINE)
    .bind(volume_id)
    .bind(app_id)
    .bind(node_id)
    .bind("a".repeat(64))
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO node_key_custody (node_id, key_kind, key_id, sealed, created_at) \
         VALUES (?, 'signing', ?, X'00', ?)",
    )
    .bind(node_id)
    .bind("b".repeat(64))
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

async fn count(server: &Server, sql: &str, node_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

async fn quarantines_of(server: &Server, node_id: &str) -> i64 {
    count(
        server,
        "SELECT COUNT(*) FROM volume_quarantines WHERE node_id = ?",
        node_id,
    )
    .await
}

async fn seals_of(server: &Server, node_id: &str) -> i64 {
    count(
        server,
        "SELECT COUNT(*) FROM node_key_custody WHERE node_id = ?",
        node_id,
    )
    .await
}

/// Section 10 for a node that is not connected, with each refusal first:
///
/// - an agent is refused `human_only` (ADR 0030), an unknown node is
///   `not_found`, a node with the primary status and a node that hosts a
///   running instance are `conflict`, and none of them changes anything;
/// - an instance a disconnected node last reported `running` reads
///   `unknown` (issue #342) and does not refuse the removal;
/// - the removal raises the token of the lease the node holds exactly as
///   declaring it lost does, leaves a lease another node holds alone, turns
///   the node `removed` in one entry, drops its quarantine records and
///   deletes its custody seals in the same transaction, and answers `202`
///   with the node; the audit record names the removal entry;
/// - a repeat is `conflict`, and the node's credential no longer opens a
///   session.
#[tokio::test]
async fn removing_a_disconnected_node_raises_its_tokens_and_drops_its_records() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, socket, app_id, vm_id, volume_id, lease_id) = node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    records_of(&server, &node_id, &app_id, &volume_id).await;
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

    // Human-only, before anything else is checked, also with a
    // justification.
    let token = agent_token(&server, &owner).await;
    let res = reqwest::Client::new()
        .delete(server.url(&format!("/api/v1/nodes/{node_id}")))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "try to remove a node")
        .send()
        .await
        .unwrap();
    let (status, code, reason, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));

    for unknown in ["nod_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "not-a-node"] {
        let (status, code, _, _) = problem(remove(&owner, &server, unknown).await).await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{unknown}");
    }

    // The node that holds the primary status is refused.
    sqlx::query("UPDATE nodes SET cp_status = 'primary' WHERE id = ?")
        .bind(&other.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, _, detail) = problem(remove(&owner, &server, &other.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("primary"), "{detail}");
    sqlx::query("UPDATE nodes SET cp_status = NULL WHERE id = ?")
        .bind(&other.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // A running instance on the connected node is refused.
    sqlx::query("UPDATE vms SET observed_status = 'running' WHERE id = ?")
        .bind(&vm_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, _, detail) = problem(remove(&owner, &server, &node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("running instances"), "{detail}");
    assert_eq!(lease_row(&server, &lease_id).await.0, 1, "nothing changed");
    assert_eq!(node_status(&server, &node_id).await, "connected");
    assert_eq!(quarantines_of(&server, &node_id).await, 1);
    assert_eq!(seals_of(&server, &node_id).await, 1);

    // Once the node is gone, its instance reads `unknown`, not `running`.
    drop(socket);
    wait_disconnected(&server, &node_id).await;
    assert_eq!(node_status(&server, &node_id).await, "disconnected");

    // Section 8.10 rule 2, which the fake node does not acknowledge itself.
    common::settle_voters(&server).await;
    let before = log_head(&server).await;
    let res = remove(&owner, &server, &node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["id"], node_id);
    assert_eq!(body["status"], "removed", "{body}");
    assert_eq!(body["roles"], json!(["node"]), "{body}");
    assert!(body.get("custody").is_some(), "{body}");
    assert_eq!(node_status(&server, &node_id).await, "removed");

    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 2, "the token goes up by one, as declaring it lost");
    assert!(
        revoked_at.is_some(),
        "recorded revoked in the primary's view"
    );
    assert_eq!(holder, node_id, "the node stays named as the last holder");
    assert_eq!(
        lease_row(&server, OTHER_LEASE).await,
        (7, None, other.node_id.clone()),
        "a lease another node holds is left as it is"
    );
    assert_eq!(quarantines_of(&server, &node_id).await, 0);
    assert_eq!(seals_of(&server, &node_id).await, 0);

    // One transaction: the lease, then the removal entry, then the records.
    let entries = entries_after(&server, before).await;
    let kinds: Vec<(&str, &str, &str)> = entries
        .iter()
        .map(|(_, kind, id, op, _)| (kind.as_str(), id.as_str(), op.as_str()))
        .collect();
    assert_eq!(
        kinds[..3],
        [
            ("lease", lease_id.as_str(), "update"),
            ("node", node_id.as_str(), "update"),
            ("volume_quarantine", QUARANTINE, "delete"),
        ],
        "{kinds:?}"
    );
    assert_eq!(kinds.len(), 4, "{kinds:?}");
    assert_eq!(kinds[3].0, "node_key_custody", "{kinds:?}");
    assert_eq!(kinds[3].2, "delete", "{kinds:?}");
    let (removal_seq, _, _, _, payload) = &entries[1];
    assert_eq!(payload["status"], "removed", "{payload}");
    assert_eq!(payload["roles"], json!(["node"]), "{payload}");
    assert_eq!(entries[0].4["fencing_token"], 2);

    let records = remove_records(&server).await;
    let ok: Vec<&RemoveRecord> = records.iter().filter(|r| r.0 == "ok").collect();
    assert_eq!(ok.len(), 1, "{records:?}");
    let (_, scope, params, change_seq, actor_type, target_type, target_id) = ok[0];
    assert_eq!(scope.as_deref(), Some("node:remove"));
    assert_eq!(
        serde_json::from_str::<Value>(params).unwrap(),
        json!({ "node_id": node_id })
    );
    assert_eq!(*change_seq, Some(*removal_seq));
    assert_eq!(actor_type, "user");
    assert_eq!(target_type.as_deref(), Some("node"));
    assert_eq!(target_id.as_deref(), Some(node_id.as_str()));
    // Every refusal is recorded too: the agent, the two unknown nodes, the
    // primary's node and the running instance.
    assert_eq!(records.len(), 6, "{records:?}");
    assert!(
        records[..5]
            .iter()
            .all(|record| record.0 != "ok" && record.3.is_none()),
        "{records:?}"
    );

    // A repeat changes nothing.
    let before = log_head(&server).await;
    let (status, code, _, detail) = problem(remove(&owner, &server, &node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("already removed"), "{detail}");
    assert!(entries_after(&server, before).await.is_empty());

    // The node stops dialing: its credential opens no session.
    let (socket, status) = dial(&server, &node.credential).await;
    assert!(socket.is_none());
    assert_eq!(status, 403);
}

/// Section 10 for a connected node: the primary first revokes its lease in
/// its own view and sends `lease_revoke`, and the node is not removed while
/// the revoke waits for its answer. Once the node answers `ok`, it turns
/// `removed`, its session ends with the fatal `error` `node_removed` and
/// nothing after it, and the call answers `202`. The token does not move:
/// the node released the lease at it. The revoke is recorded as a
/// `lease.revoke` system record with its reason.
#[tokio::test]
async fn removing_a_connected_node_revokes_its_leases_and_ends_its_session() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, app_id, _vm_id, _volume_id, lease_id) =
        node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    common::settle_voters(&server).await;

    let call = tokio::spawn({
        let owner = owner.clone();
        let url = server.url(&format!("/api/v1/nodes/{node_id}"));
        async move { owner.delete(url).send().await.unwrap() }
    });
    let frames = recv_until(&mut socket, &["lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    assert_eq!(revoke["body"]["lease_id"], lease_id.as_str());
    assert_eq!(revoke["body"]["fencing_token"], 1);
    assert_eq!(revoke["body"]["reason"], "declared_lost");
    let (token, revoked_at, _) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 1);
    assert!(revoked_at.is_some(), "revoked in the primary's view first");

    // Unanswered: nothing is removed yet.
    heartbeat(&mut socket).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!call.is_finished(), "the call waits for the answer");
    assert_eq!(node_status(&server, &node_id).await, "connected");

    reply(&mut socket, ok_result(), revoke["id"].as_str()).await;
    let mut last = None;
    while let Some(frame) = next_frame(&mut socket).await {
        last = Some(frame);
    }
    let last = last.expect("a last frame");
    assert_eq!(last["type"], "error", "{last}");
    assert_eq!(last["body"]["code"], "node_removed", "{last}");
    assert_eq!(last["body"]["fatal"], true, "{last}");

    let res = call.await.unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["status"], "removed", "{body}");
    assert_eq!(node_status(&server, &node_id).await, "removed");
    let (token, revoked_at, holder) = lease_row(&server, &lease_id).await;
    assert_eq!((token, holder.as_str()), (1, node_id.as_str()));
    assert!(revoked_at.is_some());
    wait_disconnected(&server, &node_id).await;
    assert_eq!(
        node_status(&server, &node_id).await,
        "removed",
        "the session's end does not write the row back"
    );

    let records = lease_revoke_records(&server).await;
    let (params, target, change_seq) = records.last().expect("a lease.revoke record");
    assert_eq!(
        params,
        &json!({ "node_id": node_id, "fencing_token": 1, "reason": "declared_lost" })
    );
    assert_eq!(target.as_deref(), Some(app_id.as_str()));
    assert!(change_seq.is_some());
    let ok: Vec<RemoveRecord> = remove_records(&server)
        .await
        .into_iter()
        .filter(|r| r.0 == "ok")
        .collect();
    assert_eq!(ok.len(), 1);
    assert!(ok[0].3.is_some());

    let (socket, status) = dial(&server, &node.credential).await;
    assert!(socket.is_none());
    assert_eq!(status, 403);
}

/// A connected node whose fence is not confirmed is not removed (section
/// 8.5): a revoke answered `fence_failed` answers `conflict`, the lease
/// stays revoked and listed in `fence_failure`, and the node stays
/// `connected` with its session. Nothing releases the lease.
#[tokio::test]
async fn a_failed_fence_removes_nothing() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, _app_id, _vm_id, _volume_id, lease_id) =
        node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    common::settle_voters(&server).await;

    let call = tokio::spawn({
        let owner = owner.clone();
        let url = server.url(&format!("/api/v1/nodes/{node_id}"));
        async move { owner.delete(url).send().await.unwrap() }
    });
    let frames = recv_until(&mut socket, &["lease_revoke"]).await;
    let revoke = of_type(&frames, "lease_revoke")[0].clone();
    reply(&mut socket, fence_failed_result(), revoke["id"].as_str()).await;
    let res = call.await.unwrap();
    let (status, code, _, detail) = problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("fence_failed"), "{detail}");
    assert!(detail.contains("stay revoked"), "{detail}");

    assert_eq!(node_status(&server, &node_id).await, "connected");
    assert!(server.state.hub.is_connected(&node_id));
    let (token, revoked_at, _) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 1);
    assert!(revoked_at.is_some());
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert!(
        failure.is_some_and(|failure| failure.contains(&lease_id)),
        "the lease is listed until an operator acknowledges it"
    );
    let records = remove_records(&server).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].0, "error");
    assert_eq!(records[0].3, None);
    heartbeat(&mut socket).await;
}

/// A connected node whose session ends before it answers its revoke is not
/// removed: the call answers `conflict` and the lease stays revoked with its
/// token. The node is then not connected, so a retry removes it as section
/// 10 removes a disconnected node, exactly as declaring it lost does: only
/// the leases it holds unrevoked have their token raised, so this row keeps
/// its own (`declareNodeLost` leaves a revoked row as it is too).
#[tokio::test]
async fn a_session_that_ends_before_its_answer_removes_nothing() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let (node, mut socket, _app_id, _vm_id, _volume_id, lease_id) =
        node_with_lease(&server, &owner).await;
    let node_id = node.node_id.clone();
    common::settle_voters(&server).await;

    let call = tokio::spawn({
        let owner = owner.clone();
        let url = server.url(&format!("/api/v1/nodes/{node_id}"));
        async move { owner.delete(url).send().await.unwrap() }
    });
    recv_until(&mut socket, &["lease_revoke"]).await;
    drop(socket);
    let res = call.await.unwrap();
    let (status, code, _, detail) = problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("session ended"), "{detail}");
    wait_disconnected(&server, &node_id).await;
    assert_eq!(node_status(&server, &node_id).await, "disconnected");
    let (token, revoked_at, _) = lease_row(&server, &lease_id).await;
    assert_eq!(token, 1);
    assert!(revoked_at.is_some());

    let res = remove(&owner, &server, &node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    assert_eq!(node_status(&server, &node_id).await, "removed");
    assert_eq!(lease_row(&server, &lease_id).await.0, 1);
}

/// A session that has attached but not said `hello` yet counts as
/// connected: the node holds no lease, so nothing is revoked, and once the
/// removal commits the session ends with `node_removed`, its `hello`
/// recorded nowhere.
#[tokio::test]
async fn a_session_that_has_not_said_hello_ends_with_node_removed() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let mut socket = connect(&server, &node.credential).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !server.state.hub.is_connected(&node.node_id) {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    common::settle_voters(&server).await;
    let res = remove(&owner, &server, &node.node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let frame = recv_frame(&mut socket).await;
    assert_eq!(frame["type"], "error", "{frame}");
    assert_eq!(frame["body"]["code"], "node_removed", "{frame}");
    assert_eq!(frame["body"]["fatal"], true, "{frame}");
    assert!(next_frame(&mut socket).await.is_none(), "nothing after it");
    assert_eq!(node_status(&server, &node.node_id).await, "removed");
}

/// The newest `seq` of a `node` entry, the voter-set version once a removal
/// moved it.
async fn last_node_entry(server: &Server, node_id: &str) -> u64 {
    let seq: i64 = sqlx::query_scalar(
        "SELECT MAX(seq) FROM change_log WHERE entity_type = 'node' AND entity_id = ?",
    )
    .bind(node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    u64::try_from(seq).unwrap()
}

/// Reads `machine`'s frames until its newest `spec.votes` names `count`
/// voters, and until the primary handled its acknowledgement.
async fn holds_voters(machine: &mut Machine, count: usize) {
    while machine.spec["votes"]["voters"]
        .as_array()
        .map_or(0, Vec::len)
        != count
    {
        machine.until("desired_state").await;
    }
    machine.settle().await;
}

/// Section 8.10 for a removal, every acknowledgement from a real session:
/// removing a voter answers `conflict` while a majority of the current
/// voters has not acknowledged the current version (rule 2), and passes once
/// it has. It is not refused for leaving fewer than three voters while an
/// `expiring` lease exists, and needs no `force`. Its removal entry is the
/// new voter-set version, and the voters' next `spec.votes` no longer names
/// it.
#[tokio::test]
async fn removing_a_voter_follows_rule_2_and_never_the_three_voter_guard() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = machine::enroll(&server, &owner, "node-a", &["node"]).await;
    let mut machine_a = Machine::connect(&server, &a).await;
    machine_a.settle().await;
    let b = machine::enroll(&server, &owner, "node-b", &["node"]).await;
    let mut machine_b = Machine::connect(&server, &b).await;
    holds_voters(&mut machine_a, 2).await;
    holds_voters(&mut machine_b, 2).await;
    let c = machine::enroll(&server, &owner, "node-c", &["node"]).await;
    // An `expiring` lease, which the roles operation's guard would count.
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, duration_ms, granted_at) \
         VALUES (?, 'app', 'app_01JC9Z6Q5W8N3K4T7R2M1XHB0E', ?, 'expiring', 1, 15000, ?)",
    )
    .bind(OTHER_LEASE)
    .bind(&a.node_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();

    // C's enrollment moved the version; neither A nor B holds it yet.
    let (status, code, _, detail) = problem(remove(&owner, &server, &c.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("majority"), "{detail}");
    assert_eq!(node_status(&server, &c.node_id).await, "pending");

    holds_voters(&mut machine_a, 3).await;
    holds_voters(&mut machine_b, 3).await;
    let res = remove(&owner, &server, &c.node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let removal = last_node_entry(&server, &c.node_id).await;

    // Two voters are left, with an `expiring` lease: no guard, no `force`.
    holds_voters(&mut machine_a, 2).await;
    assert_eq!(machine_a.version(), removal, "the removal entry");
    let voters: Vec<&str> = machine_a.spec["votes"]["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|voter| voter["node_id"].as_str().unwrap())
        .collect();
    assert!(!voters.contains(&c.node_id.as_str()), "{voters:?}");

    // The next voter change waits for the voters to hold that version.
    let (status, code, _, detail) = problem(remove(&owner, &server, &b.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("majority"), "{detail}");
    assert_eq!(node_status(&server, &b.node_id).await, "connected");
}

/// ADR 0321 item 4: while the `nodes` rows and the change log disagree on a
/// machine that never connects again, every voter change waits, the removal
/// of another voter included, and removing that machine is the one change
/// the gate lets through: its removal entry ends the disagreement and moves
/// the version, and the next voter change passes once the voters hold it.
#[tokio::test]
async fn removing_the_machine_the_rows_and_the_log_disagree_on_repairs_the_gate() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = machine::enroll(&server, &owner, "node-a", &["node"]).await;
    let mut machine_a = Machine::connect(&server, &a).await;
    machine_a.settle().await;
    let b = machine::enroll(&server, &owner, "node-b", &["node"]).await;
    let mut machine_b = Machine::connect(&server, &b).await;
    holds_voters(&mut machine_a, 2).await;
    holds_voters(&mut machine_b, 2).await;
    let x = machine::enroll(&server, &owner, "edge-x", &["edge", "witness"]).await;
    holds_voters(&mut machine_a, 3).await;
    holds_voters(&mut machine_b, 3).await;
    let version = machine_a.version();

    // X's row loses its vote with no change-log entry: the rows name {A, B},
    // the log {A, B, X}.
    sqlx::query("UPDATE nodes SET roles = '[\"edge\"]' WHERE id = ?")
        .bind(&x.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // Another voter's removal waits.
    let (status, code, _, detail) = problem(remove(&owner, &server, &b.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("disagree"), "{detail}");
    assert_eq!(node_status(&server, &b.node_id).await, "connected");

    // X's removal passes, and its entry is the new version.
    let res = remove(&owner, &server, &x.node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let removal = last_node_entry(&server, &x.node_id).await;
    assert!(removal > version);
    while machine_a.version() != removal {
        machine_a.until("desired_state").await;
    }
    holds_voters(&mut machine_a, 2).await;
    holds_voters(&mut machine_b, 2).await;
    assert_eq!(machine_b.version(), removal);

    // The rows and the log agree again: the next voter change passes.
    let res = remove(&owner, &server, &b.node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
}

/// ADR 0321 item 4, a row turned `removed` with no change-log entry: the
/// log still counts the machine as a voter, so every voter change waits.
/// Removing it again is not refused as already removed: it writes the
/// removal entry, which ends the disagreement and moves the version. A
/// removed machine the log agrees on stays refused.
#[tokio::test]
async fn removing_again_a_machine_removed_without_its_entry_writes_the_entry() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = machine::enroll(&server, &owner, "node-a", &["node"]).await;
    let mut machine_a = Machine::connect(&server, &a).await;
    machine_a.settle().await;
    let b = machine::enroll(&server, &owner, "node-b", &["node"]).await;
    let mut machine_b = Machine::connect(&server, &b).await;
    holds_voters(&mut machine_a, 2).await;
    holds_voters(&mut machine_b, 2).await;
    let x = machine::enroll(&server, &owner, "edge-x", &["edge", "witness"]).await;
    holds_voters(&mut machine_a, 3).await;
    holds_voters(&mut machine_b, 3).await;

    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&x.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, _, detail) = problem(remove(&owner, &server, &b.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("disagree"), "{detail}");

    let before = last_node_entry(&server, &x.node_id).await;
    let res = remove(&owner, &server, &x.node_id).await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let removal = last_node_entry(&server, &x.node_id).await;
    assert!(removal > before);
    while machine_a.version() != removal {
        machine_a.until("desired_state").await;
    }
    holds_voters(&mut machine_a, 2).await;
    holds_voters(&mut machine_b, 2).await;

    // Now the log agrees: a third removal of X is refused and writes nothing.
    let (status, code, _, detail) = problem(remove(&owner, &server, &x.node_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{detail}");
    assert!(detail.contains("already removed"), "{detail}");
    assert_eq!(last_node_entry(&server, &x.node_id).await, removal);
}
