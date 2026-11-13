//! The primary's side of quarantined copies of a volume (node-agent-protocol.md
//! sections 5.3, 5.11, 9 and 10; `openapi.yaml` `deleteVolumeQuarantine`;
//! ADR 0176, ADR 0215; issue #732), over real HTTP and WebSocket.
//!
//! No node reports a copy yet, so fake nodes send `status_report.quarantines`.
//! The volume rows are written directly, as `tests/failover.rs` does: the
//! volume lives on one node (the holder) and the copies are reported by
//! another (the returning source).

mod common;

use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Health, Kvm as WireKvm, NodeMessage, ObservedStatus,
    QuarantineStatus, StatusReport, VmStatus, VolumeState as WireVolumeState, VolumeStatus,
};
use serde_json::{Value, json};
use sqlx::Row as _;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/db@sha256:0000000000000000000000000000000000000000000000000000000000000000";
/// A standalone VM on the returning node, for the rest of a report.
const VM_ID: &str = "vm_0000000000000000000000000Q";
const UNKNOWN_VOLUME: &str = "vol_0000000000000000000000000Z";
const UNKNOWN_APP: &str = "app_0000000000000000000000000Z";
const CREATED_AT: &str = "2026-11-09T10:00:00Z";
const EARLIER: &str = "2026-11-09T09:00:00Z";
const SIZE: u64 = 1 << 20;
const DELETE_OPERATION: &str = "DELETE /api/v1/volumes/{volume_id}/quarantines/{quarantine_id}";
/// The `detail` of `deleteVolume`'s `has_quarantines` for a recorded copy.
const VOLUME_ROW_DETAIL: &str =
    "the volume keeps a quarantined copy: a human deletes it first (deleteVolumeQuarantine)";
/// The `detail` of `deleteApp`'s `has_quarantines` for a recorded copy.
const APP_ROW_DETAIL: &str = "a volume of the app keeps a quarantined copy: a human deletes it \
                              first (deleteVolumeQuarantine)";

/// A fresh six-digit suffix, so every frame and row id is distinct.
fn next_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{:06}", NEXT.fetch_add(1, Ordering::SeqCst))
}

/// A well-formed `qrn_` id.
fn qrn(n: u32) -> String {
    format!("qrn_01JC9Z6Q5W8N3K4T7R2M{n:06}")
}

/// A 64-character hash made of one hex digit.
fn sha(digit: char) -> String {
    digit.to_string().repeat(64)
}

fn copy(
    id: &str,
    volume_id: &str,
    app_id: &str,
    created_at: &str,
    sha256: Option<String>,
) -> QuarantineStatus {
    QuarantineStatus {
        quarantine_id: id.to_string(),
        volume_id: volume_id.to_string(),
        app_id: app_id.to_string(),
        created_at: created_at.to_string(),
        size_bytes: SIZE,
        sha256,
    }
}

fn report(
    vms: Vec<VmStatus>,
    volumes: Vec<VolumeStatus>,
    quarantines: Vec<QuarantineStatus>,
) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev: 0,
        vms,
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
        volumes,
        backups: Vec::new(),
        images: Vec::new(),
        warm: Vec::new(),
        quarantines,
    })
}

struct Enrolled {
    node_id: String,
    credential: String,
}

/// Enrolls a node whose identity key comes from `seed`.
async fn enroll(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> Enrolled {
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
                &quoro_proto::sign::signing_key_from_seed(&[seed; 32]).verifying_key()
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
        control_plane_urls: Vec::new(),
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

/// Sends `message` and returns its id.
async fn send(socket: &mut Socket, message: NodeMessage) -> String {
    let id = format!("msg_01JC9Z6Q5W8N3K4T7R2M{}", next_suffix());
    let text = message.to_frame(&id, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
    id
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

/// The first frame of type `kind`; earlier frames are skipped.
async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

/// The `spec` of the first `desired_state` that `wanted` accepts; earlier
/// frames are skipped.
async fn spec_where(socket: &mut Socket, wanted: impl Fn(&Value) -> bool) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == "desired_state" && wanted(&frame["body"]["spec"]) {
            return frame["body"]["spec"].clone();
        }
    }
}

/// Every frame the primary sends before it answers two pings in a row: the
/// session handles messages in order, so whatever an earlier message caused
/// is done, and in the result.
async fn frames_until_settled(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    for _ in 0..2 {
        let id = send(socket, NodeMessage::Ping).await;
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

/// Sends a report of `quarantines` only, waits until it is applied and
/// returns the frames the primary sent meanwhile.
async fn report_copies(socket: &mut Socket, quarantines: Vec<QuarantineStatus>) -> Vec<Value> {
    send(socket, report(Vec::new(), Vec::new(), quarantines)).await;
    frames_until_settled(socket).await
}

/// The `spec` of the first `desired_state` that `wanted` accepts, among
/// `frames` already read and then the frames that follow.
async fn spec_in(
    socket: &mut Socket,
    frames: Vec<Value>,
    wanted: impl Fn(&Value) -> bool,
) -> Value {
    if let Some(frame) = frames
        .into_iter()
        .find(|frame| frame["type"] == "desired_state" && wanted(&frame["body"]["spec"]))
    {
        return frame["body"]["spec"].clone();
    }
    spec_where(socket, wanted).await
}

/// Connects, says hello and takes the first snapshot, which has no
/// `quarantine_deletes` key: the node has no copy to delete.
async fn open(server: &Server, node: &Enrolled) -> Socket {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let frame = recv_type(&mut socket, "desired_state").await;
    assert!(
        frame["body"]["spec"].get("quarantine_deletes").is_none(),
        "{frame}"
    );
    socket
}

async fn create_app(
    server: &Server,
    owner: &reqwest::Client,
    project_id: &str,
    name: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// A `ready` volume of `app_id` on `node_id`, written directly.
async fn insert_volume(server: &Server, app_id: &str, node_id: &str) -> String {
    let id = format!("vol_01JC9Z6Q5W8N3K4T7R2M{}", next_suffix());
    let now = quoro_control_plane::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
         allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
         VALUES (?, ?, ?, 2, '/data', 'ready', NULL, NULL, NULL, ?, ?)",
    )
    .bind(&id)
    .bind(app_id)
    .bind(node_id)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    id
}

/// One app with a `ready` volume on the holder, and two connected nodes: the
/// holder and the returning source, which reports the copies.
struct Setup {
    server: Server,
    owner: reqwest::Client,
    project_id: String,
    app_id: String,
    volume_id: String,
    holder: Enrolled,
    returning: Enrolled,
    on_holder: Socket,
    on_returning: Socket,
}

impl Setup {
    async fn new() -> Self {
        let server = start_with_patient_node_session().await;
        let owner = server.owner().await;
        let holder = enroll(&server, &owner, "holder", 3).await;
        let returning = enroll(&server, &owner, "returning", 4).await;
        let on_holder = open(&server, &holder).await;
        let on_returning = open(&server, &returning).await;
        let project: Value = owner
            .post(server.url("/api/v1/projects"))
            .json(&json!({ "slug": "quarantine", "name": "quarantine" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let project_id = project["id"].as_str().unwrap().to_string();
        let app_id = create_app(&server, &owner, &project_id, "db").await;
        let volume_id = insert_volume(&server, &app_id, &holder.node_id).await;
        Self {
            server,
            owner,
            project_id,
            app_id,
            volume_id,
            holder,
            returning,
            on_holder,
            on_returning,
        }
    }

    /// A copy of the setup's volume.
    fn copy(&self, id: &str, created_at: &str, sha256: Option<String>) -> QuarantineStatus {
        copy(id, &self.volume_id, &self.app_id, created_at, sha256)
    }

    /// The returning node reports `id` with its hash, which records it.
    async fn record(&mut self, id: &str) {
        let entry = self.copy(id, CREATED_AT, Some(sha('a')));
        report_copies(&mut self.on_returning, vec![entry]).await;
        assert!(
            copies(&self.server).await.iter().any(|row| row.0 == id),
            "{id} is recorded"
        );
    }

    async fn delete_copy(&self, volume_id: &str, id: &str) -> reqwest::Response {
        delete_copy(&self.owner, &self.server, volume_id, id).await
    }

    async fn get_volume(&self) -> Value {
        let res = self
            .owner
            .get(
                self.server
                    .url(&format!("/api/v1/volumes/{}", self.volume_id)),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        res.json().await.unwrap()
    }

    async fn deleting_at(&self, id: &str) -> Option<String> {
        copies(&self.server)
            .await
            .into_iter()
            .find(|row| row.0 == id)
            .unwrap_or_else(|| panic!("{id} has a row"))
            .7
    }
}

async fn delete_copy(
    client: &reqwest::Client,
    server: &Server,
    volume_id: &str,
    id: &str,
) -> reqwest::Response {
    client
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}/quarantines/{id}")))
        .send()
        .await
        .unwrap()
}

/// `(id, volume_id, app_id, node_id, size_bytes, sha256, created_at,
/// deleting_at)` of every `volume_quarantines` row, by id.
type CopyRow = (
    String,
    String,
    String,
    String,
    i64,
    String,
    String,
    Option<String>,
);

async fn copies(server: &Server) -> Vec<CopyRow> {
    sqlx::query_as(
        "SELECT id, volume_id, app_id, node_id, size_bytes, sha256, created_at, deleting_at \
         FROM volume_quarantines ORDER BY id",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

async fn count(server: &Server, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// The change-log entries of one copy: `(seq, op, actor_type, actor_id,
/// payload)`.
async fn log_of(server: &Server, id: &str) -> Vec<(i64, String, String, String, Value)> {
    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, op, actor_type, actor_id, payload FROM change_log \
         WHERE entity_type = 'volume_quarantine' AND entity_id = ? ORDER BY seq",
    )
    .bind(id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, op, kind, actor, payload)| {
            (
                seq,
                op,
                kind,
                actor,
                serde_json::from_str(&payload).unwrap(),
            )
        })
        .collect()
}

/// The audit records of `operation`, oldest first.
async fn audit_of(server: &Server, operation: &str) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT actor_type, actor_id, channel, scope, target_type, target_id, project_id, \
         params, result, result_detail, change_seq FROM audit_log WHERE operation = ? \
         ORDER BY seq",
    )
    .bind(operation)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|row| {
            json!({
                "actor_type": row.get::<String, _>("actor_type"),
                "actor_id": row.get::<String, _>("actor_id"),
                "channel": row.get::<String, _>("channel"),
                "scope": row.get::<Option<String>, _>("scope"),
                "target_type": row.get::<Option<String>, _>("target_type"),
                "target_id": row.get::<Option<String>, _>("target_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "params": serde_json::from_str::<Value>(&row.get::<String, _>("params")).unwrap(),
                "result": row.get::<String, _>("result"),
                "result_detail": row.get::<Option<String>, _>("result_detail"),
                "change_seq": row.get::<Option<i64>, _>("change_seq"),
            })
        })
        .collect()
}

/// `(source, subject, project_id, actor, data)` of an event row.
type EventRow = (String, String, Option<String>, Option<String>, String);

/// The `volume.quarantined` events, oldest first.
async fn events(server: &Server) -> Vec<Value> {
    let rows: Vec<EventRow> = sqlx::query_as(
        "SELECT source, subject, project_id, actor, data FROM events \
         WHERE type = 'volume.quarantined' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(source, subject, project_id, actor, data)| {
            json!({
                "source": serde_json::from_str::<Value>(&source).unwrap(),
                "subject": serde_json::from_str::<Value>(&subject).unwrap(),
                "project_id": project_id,
                "actor": actor,
                "data": serde_json::from_str::<Value>(&data).unwrap(),
            })
        })
        .collect()
}

/// How many records, events and change-log creates the copies have.
async fn written(server: &Server) -> (usize, i64, i64, i64) {
    (
        copies(server).await.len(),
        count(
            server,
            "SELECT count(*) FROM audit_log WHERE operation = 'volume.quarantine'",
        )
        .await,
        count(
            server,
            "SELECT count(*) FROM events WHERE type = 'volume.quarantined'",
        )
        .await,
        count(
            server,
            "SELECT count(*) FROM change_log WHERE entity_type = 'volume_quarantine' \
             AND op = 'create'",
        )
        .await,
    )
}

/// An agent of the setup's project that holds an approved `app:delete`
/// approval for the setup's volume, and a grant written directly that
/// carries `app:delete_quarantine`, which the grant API never issues.
async fn approved_agent(s: &Setup) -> String {
    let server = &s.server;
    let owner = &s.owner;
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
            "name": "copy-operator",
            "model": "test-model",
            "owner_user_id": owner_user_id,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": ["app:read", "approval:create"],
            "selector": { "projects": ["quarantine"] },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "grant: {}", res.text().await.unwrap());
    let key: Value = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
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
    let token = token["access_token"].as_str().unwrap().to_string();

    let res = reqwest::Client::new()
        .post(server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "scope": "app:delete",
            "target": { "type": "volume", "id": s.volume_id },
            "operation": "DELETE /api/v1/volumes/{volume_id}",
            "params": { "volume_id": s.volume_id },
            "justification": "the volume is finished",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "approval: {}", res.text().await.unwrap());
    let approval_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = owner
        .post(server.url(&format!("/api/v1/approvals/{approval_id}/approve")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());

    sqlx::query(
        "INSERT INTO grants (id, agent_id, kind, scopes, selector, expires_at, max_uses, uses, \
         status, incident_id, approval_id, on_behalf_of, created_by_type, created_by_id, reason, \
         created_at, revoked_at, revoked_by) \
         VALUES (?, ?, 'standard', ?, ?, '2999-01-01T00:00:00.000Z', NULL, 0, 'active', NULL, \
         NULL, ?, 'user', ?, 'tests', ?, NULL, NULL)",
    )
    .bind(quoro_types::new_id(quoro_types::Prefix::Grant))
    .bind(&agent_id)
    .bind(json!(["app:delete_quarantine"]).to_string())
    .bind(json!({ "apps": [&s.app_id] }).to_string())
    .bind(&owner_user_id)
    .bind(&owner_user_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    token
}

/// Test 1: the first report with a hash records the copy: its row, its
/// change-log `create`, the `volume.quarantine` audit record and the
/// `volume.quarantined` event, and every read of the volume lists it as
/// `kept`, oldest first. The volume's own change-log payload still leaves
/// `quarantines` out.
#[tokio::test]
async fn a_report_with_a_hash_records_the_copy() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;

    assert_eq!(
        copies(&s.server).await,
        [(
            id.clone(),
            s.volume_id.clone(),
            s.app_id.clone(),
            s.returning.node_id.clone(),
            i64::try_from(SIZE).unwrap(),
            sha('a'),
            CREATED_AT.to_string(),
            None,
        )]
    );

    let log = log_of(&s.server, &id).await;
    assert_eq!(log.len(), 1, "{log:?}");
    let (seq, op, actor_type, actor_id, payload) = &log[0];
    assert_eq!(
        (op.as_str(), actor_type.as_str(), actor_id.as_str()),
        ("create", "system", "volumes")
    );
    assert_eq!(
        payload,
        &json!({
            "id": id,
            "volume_id": s.volume_id,
            "app_id": s.app_id,
            "node_id": s.returning.node_id,
            "size_bytes": SIZE,
            "sha256": sha('a'),
            "created_at": CREATED_AT,
            "deleting_at": null,
        })
    );

    let facts = json!({
        "app_id": s.app_id,
        "volume_id": s.volume_id,
        "node_id": s.returning.node_id,
        "quarantine_id": id,
        "size_bytes": SIZE,
        "sha256": sha('a'),
    });
    assert_eq!(
        audit_of(&s.server, "volume.quarantine").await,
        [json!({
            "actor_type": "system",
            "actor_id": "volumes",
            "channel": "system",
            "scope": null,
            "target_type": "volume",
            "target_id": s.volume_id,
            "project_id": null,
            "params": facts,
            "result": "ok",
            "result_detail": null,
            "change_seq": seq,
        })]
    );
    assert_eq!(
        events(&s.server).await,
        [json!({
            "source": { "kind": "control_plane" },
            "subject": { "type": "volume", "id": s.volume_id },
            "project_id": s.project_id,
            "actor": null,
            "data": facts,
        })]
    );

    let kept = json!([{
        "id": id,
        "node_id": s.returning.node_id,
        "size_bytes": SIZE,
        "sha256": sha('a'),
        "state": "kept",
        "created_at": CREATED_AT,
    }]);
    assert_eq!(s.get_volume().await["quarantines"], kept);
    let list: Value = s
        .owner
        .get(s.server.url(&format!("/api/v1/apps/{}/volumes", s.app_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"][0]["quarantines"], kept, "{list}");

    // A second, older copy is listed first.
    let older = qrn(2);
    let entries = vec![
        s.copy(&id, CREATED_AT, Some(sha('a'))),
        s.copy(&older, EARLIER, Some(sha('b'))),
    ];
    report_copies(&mut s.on_returning, entries).await;
    let listed: Vec<String> = s.get_volume().await["quarantines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|copy| copy["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(listed, [older, id]);

    // The volume's own change-log payload leaves the copies out.
    send(
        &mut s.on_holder,
        report(
            Vec::new(),
            vec![VolumeStatus {
                volume_id: s.volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
            Vec::new(),
        ),
    )
    .await;
    frames_until_settled(&mut s.on_holder).await;
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM change_log WHERE entity_type = 'volume' AND entity_id = ? \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(&s.volume_id)
    .fetch_one(s.server.state.store.pool())
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["allocated_bytes"], 4096, "{payload}");
    assert!(payload.get("quarantines").is_none(), "{payload}");
}

/// Test 2: the same report three times records the copy once, and another
/// node naming it writes nothing either.
#[tokio::test]
async fn repeated_reports_write_once() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    for _ in 0..3 {
        let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
        report_copies(&mut s.on_returning, vec![entry]).await;
        assert_eq!(written(&s.server).await, (1, 1, 1, 1));
    }
    // Another node naming the copy is logged and ignored.
    let entry = s.copy(&id, EARLIER, Some(sha('b')));
    report_copies(&mut s.on_holder, vec![entry]).await;
    assert_eq!(written(&s.server).await, (1, 1, 1, 1));
    let rows = copies(&s.server).await;
    assert_eq!(
        (rows[0].3.as_str(), rows[0].5.as_str(), rows[0].6.as_str()),
        (s.returning.node_id.as_str(), sha('a').as_str(), CREATED_AT)
    );
    assert_eq!(log_of(&s.server, &id).await.len(), 1);
}

/// Test 3: a copy listed without its hash is not recorded until a later
/// report carries it.
#[tokio::test]
async fn a_copy_without_its_hash_waits_for_it() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    for _ in 0..2 {
        let entry = s.copy(&id, CREATED_AT, None);
        report_copies(&mut s.on_returning, vec![entry]).await;
        assert_eq!(written(&s.server).await, (0, 0, 0, 0));
        assert!(
            s.get_volume().await["quarantines"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    let entry = s.copy(&id, CREATED_AT, Some(sha('c')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(written(&s.server).await, (1, 1, 1, 1));
    assert_eq!(copies(&s.server).await[0].5, sha('c'));
}

/// Test 4: each kind of bad entry is skipped, while a good entry and the rest
/// of the same report still apply.
#[tokio::test]
async fn bad_entries_are_skipped_and_the_rest_of_the_report_applies() {
    let mut s = Setup::new().await;
    let other_app = create_app(&s.server, &s.owner, &s.project_id, "other").await;
    let now = quoro_control_plane::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
         image, desired_status, observed_status, ip, restart_generation, restart_count, \
         last_exit_code, created_at, updated_at) \
         VALUES (?, ?, NULL, ?, 'side', 1, 512, 4, ?, 'stopped', 'creating', NULL, 0, 0, NULL, \
         ?, ?)",
    )
    .bind(VM_ID)
    .bind(&s.project_id)
    .bind(&s.returning.node_id)
    .bind(IMAGE)
    .bind(&now)
    .bind(&now)
    .execute(s.server.state.store.pool())
    .await
    .unwrap();

    let good = qrn(15);
    let entries = vec![
        s.copy("qrn_not-an-id", CREATED_AT, Some(sha('a'))),
        s.copy(&qrn(11), CREATED_AT, Some(sha('A'))),
        s.copy(&qrn(12), CREATED_AT, Some("a".repeat(63))),
        s.copy(&qrn(13), CREATED_AT, Some(format!("{}g", "a".repeat(63)))),
        // A malformed `volume_id`, also one far too long for an audit
        // record, and a malformed `app_id` far too long for one, are bad
        // entries. The `app_id` one names an unknown volume, so only the id
        // check keeps it out of an audit record.
        copy(
            &qrn(20),
            &format!("vol_{}", "A".repeat(5000)),
            &s.app_id,
            CREATED_AT,
            Some(sha('a')),
        ),
        copy(
            &qrn(21),
            "vol_not-an-id",
            &s.app_id,
            CREATED_AT,
            Some(sha('a')),
        ),
        copy(
            &qrn(22),
            UNKNOWN_VOLUME,
            &format!("app_{}", "A".repeat(5000)),
            CREATED_AT,
            Some(sha('a')),
        ),
        // A copy of an unknown volume is not a bad entry: it is a copy of a
        // volume that is gone (test 8).
        copy(
            &qrn(16),
            &s.volume_id,
            &other_app,
            CREATED_AT,
            Some(sha('a')),
        ),
        copy(
            &qrn(17),
            &s.volume_id,
            UNKNOWN_APP,
            CREATED_AT,
            Some(sha('a')),
        ),
        s.copy(&qrn(18), "yesterday", Some(sha('a'))),
        s.copy(&qrn(19), "2026-11-09", Some(sha('a'))),
        s.copy(&good, CREATED_AT, Some(sha('d'))),
    ];
    let vm = VmStatus {
        vm_id: VM_ID.to_string(),
        observed_status: ObservedStatus::Stopped,
        health: Health::Unknown,
        ip: None,
        restart_count: 0,
        last_exit_code: None,
    };
    // `applied_rev` is applied after the copies, the VM status before them.
    let NodeMessage::StatusReport(mut body) = report(vec![vm], Vec::new(), entries) else {
        unreachable!("report builds a status_report");
    };
    body.applied_rev = 1;
    send(&mut s.on_returning, NodeMessage::StatusReport(body)).await;
    frames_until_settled(&mut s.on_returning).await;

    let rows = copies(&s.server).await;
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        [good.as_str()]
    );
    assert_eq!(written(&s.server).await, (1, 1, 1, 1));
    let observed: String = sqlx::query_scalar("SELECT observed_status FROM vms WHERE id = ?")
        .bind(VM_ID)
        .fetch_one(s.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(observed, "stopped", "the VM status of the report applied");
    let applied_rev: i64 = sqlx::query_scalar("SELECT applied_rev FROM nodes WHERE id = ?")
        .bind(&s.returning.node_id)
        .fetch_one(s.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(applied_rev, 1, "the applied_rev of the report applied");
}

/// Test 5: `deleteVolume` and `deleteApp` answer `409 has_quarantines` and
/// change nothing while a copy is recorded; without one they behave as
/// before.
#[tokio::test]
async fn deletes_are_refused_while_a_copy_is_recorded() {
    let mut s = Setup::new().await;
    s.record(&qrn(1)).await;
    let pool = s.server.state.store.pool();
    let volume_log = || {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM change_log WHERE entity_type = 'volume' AND entity_id = ?",
        )
        .bind(&s.volume_id)
        .fetch_one(pool)
    };
    let before = volume_log().await.unwrap();

    let volume_path = format!("/api/v1/volumes/{}", s.volume_id);
    let app_path = format!("/api/v1/apps/{}", s.app_id);
    assert_eq!(
        refused(&s, &volume_path, "recorded").await,
        VOLUME_ROW_DETAIL
    );
    assert_eq!(refused(&s, &app_path, "recorded").await, APP_ROW_DETAIL);

    // Nothing changed: the app, the volume and the copy are as they were.
    assert_eq!(s.get_volume().await["state"], "ready");
    assert_eq!(volume_log().await.unwrap(), before);
    let apps: i64 = sqlx::query_scalar("SELECT count(*) FROM apps WHERE id = ?")
        .bind(&s.app_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(apps, 1);
    assert_eq!(copies(&s.server).await.len(), 1);
    let refusals = audit_of(&s.server, "DELETE /api/v1/volumes/{volume_id}").await;
    assert_eq!(refusals.len(), 1);
    assert_eq!(refusals[0]["result_detail"], "has_quarantines");

    // Without a copy, both calls go through as before.
    let plain = create_app(&s.server, &s.owner, &s.project_id, "plain").await;
    let plain_volume = insert_volume(&s.server, &plain, &s.holder.node_id).await;
    let res = s
        .owner
        .delete(s.server.url(&format!("/api/v1/volumes/{plain_volume}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let res = s
        .owner
        .delete(s.server.url(&format!("/api/v1/apps/{plain}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
}

/// Test 6: a human deletes a copy; the copy reads `deleting`, its node's
/// next snapshot names it in `spec.quarantine_deletes` and no other node's
/// snapshot changes. The record stays while the node lists the copy and
/// goes, with a change-log `delete`, once it does not.
#[tokio::test]
async fn a_human_deletes_a_copy_and_the_record_goes_once_the_node_drops_it() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    s.record(&id).await;
    frames_until_settled(&mut s.on_holder).await;

    let res = s.delete_copy(&s.volume_id, &id).await;
    assert_eq!(res.status(), 202);
    let volume: Value = res.json().await.unwrap();
    assert_eq!(volume["id"], s.volume_id);
    assert_eq!(volume["quarantines"][0]["id"], id.as_str());
    assert_eq!(volume["quarantines"][0]["state"], "deleting");
    assert_eq!(s.get_volume().await["quarantines"][0]["state"], "deleting");
    let deleting_at = s.deleting_at(&id).await.expect("deleting_at is set");

    let log = log_of(&s.server, &id).await;
    assert_eq!(log.len(), 2, "{log:?}");
    let (update_seq, op, actor_type, _, payload) = &log[1];
    assert_eq!((op.as_str(), actor_type.as_str()), ("update", "user"));
    assert_eq!(payload, &json!({ "id": id, "deleting_at": deleting_at }));
    let calls = audit_of(&s.server, DELETE_OPERATION).await;
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0]["actor_type"], "user");
    assert_eq!(calls[0]["scope"], "app:delete_quarantine");
    assert_eq!(calls[0]["target_type"], "volume");
    assert_eq!(calls[0]["target_id"], s.volume_id);
    assert_eq!(calls[0]["project_id"], s.project_id);
    assert_eq!(calls[0]["params"], json!({ "quarantine_id": id }));
    assert_eq!(calls[0]["result"], "ok");
    assert_eq!(calls[0]["change_seq"], json!(update_seq));

    let spec = spec_where(&mut s.on_returning, |spec| {
        spec.get("quarantine_deletes").is_some()
    })
    .await;
    assert_eq!(spec["quarantine_deletes"], json!([id]));
    let frames = frames_until_settled(&mut s.on_holder).await;
    assert!(
        !frames.iter().any(|frame| frame["type"] == "desired_state"),
        "the holder's snapshot does not change: {frames:?}"
    );

    // A second call answers again and changes nothing.
    let res = s.delete_copy(&s.volume_id, &id).await;
    assert_eq!(res.status(), 202);
    assert_eq!(
        res.json::<Value>().await.unwrap()["quarantines"][0]["state"],
        "deleting"
    );
    assert_eq!(
        s.deleting_at(&id).await.as_deref(),
        Some(deleting_at.as_str())
    );
    assert_eq!(log_of(&s.server, &id).await.len(), 2);
    let calls = audit_of(&s.server, DELETE_OPERATION).await;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        (&calls[1]["result"], &calls[1]["change_seq"]),
        (&json!("ok"), &Value::Null)
    );

    // While the node still lists the copy, the record stays.
    let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert!(s.deleting_at(&id).await.is_some());

    // Once it does not, the record goes and the id leaves the spec.
    let frames = report_copies(&mut s.on_returning, Vec::new()).await;
    assert!(copies(&s.server).await.is_empty());
    let log = log_of(&s.server, &id).await;
    assert_eq!(log.len(), 3, "{log:?}");
    assert_eq!(log[2].1, "delete");
    assert_eq!(log[2].4["id"], id.as_str());
    let spec = spec_in(&mut s.on_returning, frames, |spec| {
        spec.get("quarantine_deletes").is_none()
    })
    .await;
    assert!(spec.get("quarantine_deletes").is_none(), "{spec}");
    assert!(
        s.get_volume().await["quarantines"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // Unknown, malformed or mismatched ids answer 404.
    let other_app = create_app(&s.server, &s.owner, &s.project_id, "other").await;
    let other_volume = insert_volume(&s.server, &other_app, &s.holder.node_id).await;
    let elsewhere = qrn(2);
    let entry = copy(
        &elsewhere,
        &other_volume,
        &other_app,
        CREATED_AT,
        Some(sha('e')),
    );
    report_copies(&mut s.on_returning, vec![entry]).await;
    for (volume_id, copy_id) in [
        (s.volume_id.as_str(), qrn(9)),
        (s.volume_id.as_str(), "qrn_x".to_string()),
        (s.volume_id.as_str(), elsewhere.clone()),
        (UNKNOWN_VOLUME, elsewhere.clone()),
        ("vol_x", elsewhere.clone()),
    ] {
        let (status, code, _) = problem(s.delete_copy(volume_id, &copy_id).await).await;
        assert_eq!(
            (status, code.as_str()),
            (404, "not_found"),
            "{volume_id} {copy_id}"
        );
    }
    assert!(s.deleting_at(&elsewhere).await.is_none(), "still kept");
}

/// Test 6, the agent: an agent is refused with `human_only` even with an
/// approval for the volume and a grant naming the scope, and the copy stays
/// `kept`. The approval still cannot delete the volume past the copy.
#[tokio::test]
async fn an_agent_never_deletes_a_copy_even_with_an_approval() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    s.record(&id).await;
    let token = approved_agent(&s).await;

    let res = reqwest::Client::new()
        .delete(
            s.server
                .url(&format!("/api/v1/volumes/{}/quarantines/{id}", s.volume_id)),
        )
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "clean up the copy")
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("human_only"))
    );
    assert!(s.deleting_at(&id).await.is_none(), "the copy stays kept");
    assert_eq!(s.get_volume().await["quarantines"][0]["state"], "kept");
    assert_eq!(log_of(&s.server, &id).await.len(), 1);
    let calls = audit_of(&s.server, DELETE_OPERATION).await;
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(
        (
            &calls[0]["actor_type"],
            &calls[0]["result"],
            &calls[0]["result_detail"]
        ),
        (&json!("agent"), &json!("refused"), &json!("human_only"))
    );
    let frames = frames_until_settled(&mut s.on_returning).await;
    assert!(
        !frames.iter().any(|frame| frame["type"] == "desired_state"),
        "{frames:?}"
    );

    // The approved delete of the volume is refused too.
    let res = reqwest::Client::new()
        .delete(s.server.url(&format!("/api/v1/volumes/{}", s.volume_id)))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "approved cleanup")
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (409, "conflict", Some("has_quarantines"))
    );
    assert_eq!(s.get_volume().await["state"], "ready");
}

/// Test 7: a `kept` record stays when its node's report leaves it out, and a
/// `deleting` one stays when another node's report does.
#[tokio::test]
async fn only_its_own_node_drops_a_deleting_record() {
    let mut s = Setup::new().await;
    let kept = qrn(1);
    let deleting = qrn(2);
    s.record(&kept).await;
    s.record(&deleting).await;
    assert_eq!(s.delete_copy(&s.volume_id, &deleting).await.status(), 202);

    // The returning node lists only the deleting copy: the kept one stays.
    let entry = s.copy(&deleting, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    // Another node lists nothing: the returning node's deleting copy stays.
    report_copies(&mut s.on_holder, Vec::new()).await;
    let rows = copies(&s.server).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(s.deleting_at(&kept).await.is_none());
    assert!(s.deleting_at(&deleting).await.is_some());

    // A report without the field leaves out both: only the deleting one goes.
    report_copies(&mut s.on_returning, Vec::new()).await;
    let rows = copies(&s.server).await;
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        [kept.as_str()]
    );
    assert_eq!(log_of(&s.server, &kept).await.len(), 1);
}

/// Test 8: a copy of a volume that is gone or `deleting` gets its audit
/// record and its event, once per `quarantine_id`, and no row (section 9,
/// ADR 0316). The volume's node can still report it `deleted` and the row
/// goes, with the rest of that report applied; a later report of the copy,
/// once the volume is gone, writes nothing more. A copy of a volume never
/// known is recorded the same way, and a copy that names another app than
/// its volume's is still skipped.
#[tokio::test]
async fn a_copy_of_a_volume_that_is_gone_gets_its_record_and_event_once() {
    let mut s = Setup::new().await;
    let other_app = create_app(&s.server, &s.owner, &s.project_id, "other").await;
    let other_volume = insert_volume(&s.server, &other_app, &s.holder.node_id).await;
    let res = s
        .owner
        .delete(s.server.url(&format!("/api/v1/volumes/{}", s.volume_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    assert_eq!(s.get_volume().await["state"], "deleting");

    // A copy that names another app than its volume's is a bad entry, and
    // one without its hash waits for it.
    let id = qrn(1);
    let wrong_app = copy(
        &qrn(2),
        &s.volume_id,
        &other_app,
        CREATED_AT,
        Some(sha('b')),
    );
    let unhashed = s.copy(&qrn(3), CREATED_AT, None);
    report_copies(&mut s.on_returning, vec![wrong_app, unhashed]).await;
    assert_eq!(written(&s.server).await, (0, 0, 0, 0));

    let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(
        written(&s.server).await,
        (0, 1, 1, 0),
        "a record and an event, no row"
    );
    let facts = json!({
        "app_id": s.app_id,
        "volume_id": s.volume_id,
        "node_id": s.returning.node_id,
        "quarantine_id": id,
        "size_bytes": SIZE,
        "sha256": sha('a'),
    });
    assert_eq!(
        audit_of(&s.server, "volume.quarantine").await,
        [json!({
            "actor_type": "system",
            "actor_id": "volumes",
            "channel": "system",
            "scope": null,
            "target_type": "volume",
            "target_id": s.volume_id,
            "project_id": null,
            "params": facts,
            "result": "ok",
            "result_detail": null,
            "change_seq": null,
        })]
    );
    let events = events(&s.server).await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0]["subject"],
        json!({ "type": "volume", "id": s.volume_id })
    );
    assert_eq!(events[0]["project_id"], json!(s.project_id));
    assert_eq!(events[0]["data"], facts);
    // Not listed in the volume, so `deleteVolumeQuarantine` cannot reach it.
    assert_eq!(s.get_volume().await["quarantines"], json!([]));

    // The same copy again, while the volume still reads `deleting`.
    let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(
        written(&s.server).await,
        (0, 1, 1, 0),
        "once per quarantine_id"
    );

    // The holder removed the file and reports it, with its other volume.
    send(
        &mut s.on_holder,
        report(
            Vec::new(),
            vec![
                VolumeStatus {
                    volume_id: s.volume_id.clone(),
                    state: WireVolumeState::Deleted,
                    allocated_bytes: None,
                    error: None,
                },
                VolumeStatus {
                    volume_id: other_volume.clone(),
                    state: WireVolumeState::Ready,
                    allocated_bytes: Some(4096),
                    error: None,
                },
            ],
            Vec::new(),
        ),
    )
    .await;
    frames_until_settled(&mut s.on_holder).await;
    let pool = s.server.state.store.pool();
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM volumes WHERE id = ?")
        .bind(&s.volume_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(left, 0, "the deleted volume's row went");
    let allocated: Option<i64> =
        sqlx::query_scalar("SELECT allocated_bytes FROM volumes WHERE id = ?")
            .bind(&other_volume)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        allocated,
        Some(4096),
        "the other volume of the report applied"
    );

    // Gone now: the copy is still recorded once only.
    let entry = s.copy(&id, CREATED_AT, Some(sha('a')));
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(written(&s.server).await, (0, 1, 1, 0));

    // A copy of a volume the primary never knew gets its own record.
    let unknown_volume = "vol_01JC9Z6Q5W8N3K4T7R2M999999";
    let stranger = qrn(4);
    let entry = copy(
        &stranger,
        unknown_volume,
        &s.app_id,
        CREATED_AT,
        Some(sha('c')),
    );
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(written(&s.server).await, (0, 2, 2, 0));
    let records = audit_of(&s.server, "volume.quarantine").await;
    assert_eq!(records[1]["target_id"], unknown_volume);
    assert_eq!(records[1]["params"]["quarantine_id"], json!(stranger));
}

/// Test 8b: the once-per-`quarantine_id` rule holds across a restart of the
/// primary, which keeps nothing of it in memory: the audit record is found.
#[tokio::test]
async fn a_gone_volume_copy_is_not_recorded_again_after_a_restart() {
    let Setup {
        server,
        owner,
        app_id,
        volume_id,
        returning,
        on_holder,
        mut on_returning,
        ..
    } = Setup::new().await;
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let id = qrn(1);
    let entry = copy(&id, &volume_id, &app_id, CREATED_AT, Some(sha('a')));
    report_copies(&mut on_returning, vec![entry.clone()]).await;
    assert_eq!(written(&server).await, (0, 1, 1, 0));

    drop(on_returning);
    drop(on_holder);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while server.state.hub.is_connected(&returning.node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never ended"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let restarted = common::restart_reloading_tls(&server).await;
    let mut on_returning = open(&restarted, &returning).await;
    report_copies(&mut on_returning, vec![entry]).await;
    assert_eq!(written(&restarted).await, (0, 1, 1, 0));
}

/// Test 9: a node with no copy to delete gets a spec without the
/// `quarantine_deletes` key, whether it keeps no copy (each `open`) or only
/// `kept` ones, and the node of a `deleting` copy gets the key.
#[tokio::test]
async fn a_node_with_no_copy_to_delete_gets_no_quarantine_deletes_key() {
    let mut s = Setup::new().await;
    s.record(&qrn(1)).await;
    let theirs = qrn(2);
    let entry = s.copy(&theirs, CREATED_AT, Some(sha('b')));
    report_copies(&mut s.on_holder, vec![entry]).await;
    assert_eq!(s.delete_copy(&s.volume_id, &theirs).await.status(), 202);
    let spec = spec_where(&mut s.on_holder, |spec| {
        spec.get("quarantine_deletes").is_some()
    })
    .await;
    assert_eq!(spec["quarantine_deletes"], json!([theirs]));

    // The returning node keeps a `kept` copy only: a new session's first
    // snapshot has no key (`open` checks it).
    let Setup {
        server,
        returning,
        on_returning,
        ..
    } = s;
    drop(on_returning);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while server.state.hub.is_connected(&returning.node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut again = open(&server, &returning).await;
    frames_until_settled(&mut again).await;
}

/// Waits until the primary no longer sees `node_id`'s session.
async fn disconnected(server: &Server, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while server.state.hub.is_connected(node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Asserts that `DELETE path` answers `409 has_quarantines` and returns the
/// problem's `detail`.
async fn refused(s: &Setup, path: &str, case: &str) -> String {
    let res = s.owner.delete(s.server.url(path)).send().await.unwrap();
    let status = res.status().as_u16();
    let content_type = res
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        (
            status,
            content_type.as_str(),
            body["code"].as_str(),
            body["reason"].as_str()
        ),
        (
            409,
            "application/problem+json",
            Some("conflict"),
            Some("has_quarantines")
        ),
        "{case}: DELETE {path}: {body}"
    );
    body["detail"].as_str().unwrap_or_default().to_string()
}

/// The `detail` of `has_quarantines` for a copy `node_id` lists that has no
/// row yet.
fn listed_detail(node_id: &str, id: &str) -> String {
    format!(
        "the quarantined copy {id} on node {node_id} is not recorded yet because its node is \
         still hashing it: once it is recorded, a human deletes it first \
         (deleteVolumeQuarantine)"
    )
}

/// Asserts that `DELETE path` answers `202`.
async fn accepted(s: &Setup, path: &str, case: &str) {
    let res = s.owner.delete(s.server.url(path)).send().await.unwrap();
    assert_eq!(
        res.status(),
        202,
        "{case}: DELETE {path}: {}",
        res.text().await.unwrap()
    );
}

/// Test 10: a copy a node lists without its hash blocks `deleteVolume` and
/// `deleteApp` before any row exists, with a detail that names the node and
/// the copy; a disconnect keeps the refusal, and a later report without the
/// copy lets the deletes through.
#[tokio::test]
async fn a_listed_copy_blocks_deletes_before_it_is_recorded() {
    let mut s = Setup::new().await;
    let id = qrn(1);
    let entry = s.copy(&id, CREATED_AT, None);
    report_copies(&mut s.on_returning, vec![entry]).await;
    assert_eq!(written(&s.server).await, (0, 0, 0, 0), "no row yet");

    let volume_path = format!("/api/v1/volumes/{}", s.volume_id);
    let app_path = format!("/api/v1/apps/{}", s.app_id);
    let detail = listed_detail(&s.returning.node_id, &id);
    for case in ["listed", "listed, node disconnected"] {
        assert_eq!(refused(&s, &volume_path, case).await, detail, "{case}");
        assert_eq!(refused(&s, &app_path, case).await, detail, "{case}");
        assert_eq!(s.get_volume().await["state"], "ready", "{case}");
        if case == "listed" {
            let _ = s.on_returning.close(None).await;
            disconnected(&s.server, &s.returning.node_id).await;
        }
    }

    // The node comes back and no longer lists the copy.
    s.on_returning = open(&s.server, &s.returning).await;
    report_copies(&mut s.on_returning, Vec::new()).await;
    accepted(&s, &volume_path, "no longer listed").await;
    accepted(&s, &app_path, "no longer listed").await;
    assert_eq!(written(&s.server).await, (0, 0, 0, 0));
}

/// Test 11: a `size_bytes` above 2^53 - 1, which the change log cannot
/// carry, is a bad entry. The good entries of the same report are recorded,
/// one at the bound included, and a `deleting` copy of that node is still
/// dropped once no report lists it.
#[tokio::test]
async fn an_oversized_copy_is_skipped_and_the_rest_still_applies() {
    let mut s = Setup::new().await;
    let deleting = qrn(1);
    s.record(&deleting).await;
    assert_eq!(s.delete_copy(&s.volume_id, &deleting).await.status(), 202);

    let mut oversized = s.copy(&qrn(2), CREATED_AT, Some(sha('b')));
    oversized.size_bytes = 1 << 53;
    let good = qrn(3);
    let mut at_bound = s.copy(&qrn(4), CREATED_AT, Some(sha('d')));
    at_bound.size_bytes = (1 << 53) - 1;
    let entries = vec![
        s.copy(&deleting, CREATED_AT, Some(sha('a'))),
        oversized.clone(),
        s.copy(&good, CREATED_AT, Some(sha('c'))),
        at_bound,
    ];
    report_copies(&mut s.on_returning, entries).await;
    let rows = copies(&s.server).await;
    assert_eq!(
        rows.iter()
            .map(|row| (row.0.as_str(), row.4, row.7.is_some()))
            .collect::<Vec<_>>(),
        [
            (deleting.as_str(), i64::try_from(SIZE).unwrap(), true),
            (good.as_str(), i64::try_from(SIZE).unwrap(), false),
            (qrn(4).as_str(), (1 << 53) - 1, false),
        ]
    );
    assert_eq!(written(&s.server).await, (3, 3, 3, 3));

    // The next report lists only the oversized copy: the deleting record
    // goes, and the kept ones stay.
    report_copies(&mut s.on_returning, vec![oversized]).await;
    let rows = copies(&s.server).await;
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        [good.as_str(), qrn(4).as_str()]
    );
    let log = log_of(&s.server, &deleting).await;
    assert_eq!(log.last().map(|entry| entry.1.as_str()), Some("delete"));
}

/// Test 12: a copy listed without its hash for a volume that turned
/// `deleting` first blocks neither `deleteApp`, though the app has another
/// live volume, nor a repeated `deleteVolume`. A well-formed unhashed copy
/// of a live volume in the same report still blocks both.
#[tokio::test]
async fn a_listed_copy_of_a_deleting_volume_blocks_nothing() {
    let mut s = Setup::new().await;
    let volume_path = format!("/api/v1/volumes/{}", s.volume_id);
    accepted(&s, &volume_path, "before any copy").await;
    assert_eq!(s.get_volume().await["state"], "deleting");
    let live_volume = insert_volume(&s.server, &s.app_id, &s.holder.node_id).await;
    let other_app = create_app(&s.server, &s.owner, &s.project_id, "other").await;
    let other_volume = insert_volume(&s.server, &other_app, &s.holder.node_id).await;

    let entries = vec![
        s.copy(&qrn(1), CREATED_AT, None),
        copy(&qrn(2), &other_volume, &other_app, CREATED_AT, None),
    ];
    report_copies(&mut s.on_returning, entries).await;
    let detail = listed_detail(&s.returning.node_id, &qrn(2));
    let other_volume_path = format!("/api/v1/volumes/{other_volume}");
    let other_app_path = format!("/api/v1/apps/{other_app}");
    assert_eq!(refused(&s, &other_volume_path, "control").await, detail);
    assert_eq!(refused(&s, &other_app_path, "control").await, detail);

    // The repeated delete meets the usual conflict, not `has_quarantines`.
    let res = s
        .owner
        .delete(s.server.url(&volume_path))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (409, "conflict", None)
    );
    let live_state: String = sqlx::query_scalar("SELECT state FROM volumes WHERE id = ?")
        .bind(&live_volume)
        .fetch_one(s.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(live_state, "ready");
    accepted(&s, &format!("/api/v1/apps/{}", s.app_id), "deleting volume").await;
    assert_eq!(written(&s.server).await, (0, 0, 0, 0));
}

/// Test 13: an unhashed copy that could never get a row, with a `created_at`
/// that is not an RFC 3339 date-time or a `size_bytes` above 2^53 - 1,
/// blocks neither `deleteVolume` nor `deleteApp` of its live volume and app.
/// A well-formed unhashed copy in the same report still blocks both.
#[tokio::test]
async fn an_unhashed_copy_that_could_never_be_recorded_blocks_nothing() {
    let mut s = Setup::new().await;
    // For each flaw, one app whose volume is deleted and one deleted whole,
    // each volume `ready` and of its own app.
    let mut entries = Vec::new();
    let mut paths = Vec::new();
    for (n, name) in [
        (1, "datevolume"),
        (2, "dateapp"),
        (3, "sizevolume"),
        (4, "sizeapp"),
    ] {
        let app_id = create_app(&s.server, &s.owner, &s.project_id, name).await;
        let volume_id = insert_volume(&s.server, &app_id, &s.holder.node_id).await;
        let mut entry = copy(&qrn(n), &volume_id, &app_id, CREATED_AT, None);
        if n <= 2 {
            entry.created_at = "2026-11-09".to_string();
        } else {
            entry.size_bytes = 1 << 53;
        }
        entries.push(entry);
        paths.push(if n % 2 == 1 {
            format!("/api/v1/volumes/{volume_id}")
        } else {
            format!("/api/v1/apps/{app_id}")
        });
    }
    let control = qrn(5);
    entries.push(s.copy(&control, CREATED_AT, None));
    report_copies(&mut s.on_returning, entries).await;

    let detail = listed_detail(&s.returning.node_id, &control);
    let volume_path = format!("/api/v1/volumes/{}", s.volume_id);
    let app_path = format!("/api/v1/apps/{}", s.app_id);
    assert_eq!(refused(&s, &volume_path, "control").await, detail);
    assert_eq!(refused(&s, &app_path, "control").await, detail);
    for (path, case) in paths.iter().zip([
        "a malformed created_at, deleteVolume",
        "a malformed created_at, deleteApp",
        "an oversized size_bytes, deleteVolume",
        "an oversized size_bytes, deleteApp",
    ]) {
        accepted(&s, path, case).await;
    }
    assert_eq!(written(&s.server).await, (0, 0, 0, 0));
}

/// Test 14: hashed copies recorded without a row block no delete while the
/// node keeps listing them: a copy of a `deleting` volume blocks neither
/// `deleteVolume` of its app's new live volume nor `deleteApp` of an app
/// with a live volume, and a copy of a volume the primary never knew blocks
/// no `deleteApp` either. A repeated `deleteVolume` of the `deleting` volume
/// meets the usual conflict, not `has_quarantines`.
#[tokio::test]
async fn copies_recorded_without_a_row_block_no_delete() {
    let mut s = Setup::new().await;
    let volume_path = format!("/api/v1/volumes/{}", s.volume_id);
    accepted(&s, &volume_path, "before any copy").await;
    let live_volume = insert_volume(&s.server, &s.app_id, &s.holder.node_id).await;
    let other_app = create_app(&s.server, &s.owner, &s.project_id, "other").await;
    let other_volume = insert_volume(&s.server, &other_app, &s.holder.node_id).await;
    accepted(
        &s,
        &format!("/api/v1/volumes/{other_volume}"),
        "other volume",
    )
    .await;
    insert_volume(&s.server, &other_app, &s.holder.node_id).await;

    let entries = vec![
        s.copy(&qrn(1), CREATED_AT, Some(sha('a'))),
        copy(
            &qrn(2),
            &other_volume,
            &other_app,
            CREATED_AT,
            Some(sha('b')),
        ),
        copy(
            &qrn(3),
            "vol_01JC9Z6Q5W8N3K4T7R2M999998",
            &other_app,
            CREATED_AT,
            Some(sha('c')),
        ),
    ];
    report_copies(&mut s.on_returning, entries.clone()).await;
    assert_eq!(
        written(&s.server).await,
        (0, 3, 3, 0),
        "three records, no row"
    );
    // The node keeps listing them.
    report_copies(&mut s.on_returning, entries).await;
    assert_eq!(written(&s.server).await, (0, 3, 3, 0));

    let res = s
        .owner
        .delete(s.server.url(&volume_path))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (409, "conflict", None)
    );
    accepted(
        &s,
        &format!("/api/v1/volumes/{live_volume}"),
        "deleteVolume",
    )
    .await;
    accepted(&s, &format!("/api/v1/apps/{other_app}"), "deleteApp").await;
    assert_eq!(written(&s.server).await, (0, 3, 3, 0));
}
