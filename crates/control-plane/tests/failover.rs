//! `failoverApp` on the primary (`openapi.yaml` `failoverApp`,
//! node-agent-protocol.md sections 6.13 and 8.4, ADR 0175): its refusals,
//! the move of a disconnected source's lease, and the revoke of a connected
//! source, over real HTTP and WebSocket with fake nodes.
//!
//! `updateApp` does not accept `ha_policy: warm` in a default build, so the
//! tests write the warm app's rows directly, as `tests/warm.rs` does.

mod common;

use base64::Engine as _;
use common::{Server, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::authority::VoterSupport;
use quoro_control_plane::session::Fault;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage, StatusReport, WarmRole, WarmStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorBody, ErrorCode};
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
    "ghcr.io/example/db@sha256:0000000000000000000000000000000000000000000000000000000000000000";
const VM_ID: &str = "vm_0000000000000000000000000W";
const VOLUME_ID: &str = "vol_0000000000000000000000000W";
const LEASE_ID: &str = "lse_0000000000000000000000000W";
const SNAPSHOT_TAKEN_AT: &str = "2026-11-09T10:00:00Z";

struct Enrolled {
    node_id: String,
    credential: String,
}

/// Enrolls a node whose identity key comes from `seed`.
async fn enroll(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> Enrolled {
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

/// `hello` with a mesh key, so the primary gives the node a mesh address,
/// and `reflink` snapshots, so the node can be either side of a warm pair.
fn hello(node_id: &str, mesh_seed: u8) -> NodeMessage {
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
        mesh_public_key: Some(base64::engine::general_purpose::STANDARD.encode([mesh_seed; 32])),
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

/// A fresh 26-character suffix, so every frame of the fake nodes has its own
/// id.
fn ulid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "01JC9Z6Q5W8N3K4T7R2M{:06}",
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
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
/// is in the result.
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

/// Asserts that no frame of type `kind` reached the node so far.
async fn none_of(socket: &mut Socket, kind: &str, case: &str) {
    let frames = frames_until_settled(socket).await;
    assert!(
        !frames.iter().any(|frame| frame["type"] == kind),
        "{case}: {frames:?}"
    );
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
fn set_support(server: &Server, supported: bool) {
    let support: Arc<dyn VoterSupport> = if supported {
        Arc::new(EverySupport)
    } else {
        Arc::new(quoro_control_plane::authority::NoVoterSupport)
    };
    server.state.hub.set_voter_support(support);
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
        detail: None,
        error: Some(ErrorBody {
            code: ErrorCode::FenceFailed,
            message: "a VM did not stop".to_string(),
        }),
        lines: None,
        metrics: None,
    })
}

/// The warm node's report: a complete copy of the app's volume, snapshot 3.
fn copy_report(app_id: &str) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev: 0,
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
        warm: vec![WarmStatus {
            volume_id: VOLUME_ID.to_string(),
            app_id: app_id.to_string(),
            role: WarmRole::Target,
            snapshot_seq: Some(3),
            snapshot_taken_at: Some(SNAPSHOT_TAKEN_AT.to_string()),
            completed_at: Some(SNAPSHOT_TAKEN_AT.to_string()),
            allocated_bytes: None,
            extent_count: None,
            frozen: None,
            rejected_token: None,
            rejected_at: None,
            error: None,
        }],
        volumes: Vec::new(),
        quarantines: Vec::new(),
    })
}

/// A `warm` app whose source holds a `local` lease with token 4, and both
/// nodes connected. The warm node has reported nothing yet, the lease row
/// still reads `local` and the primary holds no support: each test turns
/// what it needs on, after both sessions started, so no session start sees
/// an `expiring` row.
struct Setup {
    server: Server,
    owner: reqwest::Client,
    app_id: String,
    source: Enrolled,
    warm: Enrolled,
    on_source: Option<Socket>,
    on_warm: Option<Socket>,
}

impl Setup {
    async fn new() -> Self {
        let server = start_with_patient_node_session().await;
        let owner = server.owner().await;
        let source = enroll(&server, &owner, "source", 3).await;
        let warm = enroll(&server, &owner, "warm", 4).await;
        let mut on_source = connect(&server, &source.credential).await;
        send(&mut on_source, hello(&source.node_id, 1), None).await;
        recv_type(&mut on_source, "desired_state").await;
        let mut on_warm = connect(&server, &warm.credential).await;
        send(&mut on_warm, hello(&warm.node_id, 2), None).await;
        recv_type(&mut on_warm, "desired_state").await;

        let project: Value = owner
            .post(server.url("/api/v1/projects"))
            .json(&json!({ "slug": "warm", "name": "warm" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let project_id = project["id"].as_str().unwrap().to_string();
        let res = owner
            .post(server.url("/api/v1/apps"))
            .json(&json!({ "project_id": project_id, "name": "db", "image": IMAGE, "port": 8080 }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
        let app_id = res.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        let pool = server.state.store.pool();
        let now = quoro_control_plane::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
             image, desired_status, observed_status, ip, restart_generation, restart_count, \
             last_exit_code, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 'db-1', 1, 512, 4, ?, 'running', 'running', NULL, 0, 0, NULL, \
             ?, ?)",
        )
        .bind(VM_ID)
        .bind(&project_id)
        .bind(&app_id)
        .bind(&source.node_id)
        .bind(IMAGE)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
             allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
             VALUES (?, ?, ?, 2, '/data', 'ready', NULL, NULL, NULL, ?, ?)",
        )
        .bind(VOLUME_ID)
        .bind(&app_id)
        .bind(&source.node_id)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, granted_at) VALUES (?, 'app', ?, ?, 'local', 4, ?)",
        )
        .bind(LEASE_ID)
        .bind(&app_id)
        .bind(&source.node_id)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE apps SET stateful = 1, ha_policy = 'warm', warm_node_id = ? WHERE id = ?",
        )
        .bind(&warm.node_id)
        .bind(&app_id)
        .execute(pool)
        .await
        .unwrap();
        Self {
            server,
            owner,
            app_id,
            source,
            warm,
            on_source: Some(on_source),
            on_warm: Some(on_warm),
        }
    }

    fn on_source(&mut self) -> &mut Socket {
        self.on_source.as_mut().expect("the source is connected")
    }

    fn on_warm(&mut self) -> &mut Socket {
        self.on_warm.as_mut().expect("the warm node is connected")
    }

    async fn sql(&self, sql: &str) {
        sqlx::query(sql)
            .bind(LEASE_ID)
            .execute(self.server.state.store.pool())
            .await
            .unwrap();
    }

    /// The lease row reads `expiring` for `D`, written after the sessions
    /// started, so the primary keeps no gate for it.
    async fn expiring(&self) {
        self.sql("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
            .await;
    }

    /// The warm node reports its complete copy, and the primary applied it.
    async fn report_copy(&mut self) {
        let app_id = self.app_id.clone();
        let socket = self.on_warm();
        send(socket, copy_report(&app_id), None).await;
        frames_until_settled(socket).await;
    }

    /// Everything a failover needs, with the source connected: the row reads
    /// `expiring` for `D`, the warm node reported its copy, and the primary
    /// holds support.
    async fn ready(&mut self) {
        self.expiring().await;
        self.report_copy().await;
        set_support(&self.server, true);
    }

    /// Ends the source's session and waits until the primary sees it gone.
    async fn disconnect_source(&mut self) {
        drop(self.on_source.take());
        disconnected(&self.server, &self.source.node_id).await;
    }

    fn call(&self, node_id: &str) -> tokio::task::JoinHandle<reqwest::Response> {
        let request = self
            .owner
            .post(
                self.server
                    .url(&format!("/api/v1/apps/{}/failover", self.app_id)),
            )
            .json(&json!({ "node_id": node_id }));
        tokio::spawn(async move { request.send().await.unwrap() })
    }

    async fn failover(&self) -> reqwest::Response {
        self.call(&self.warm.node_id).await.unwrap()
    }

    /// Nothing moved: the lease row still names the source at token 4, with
    /// `revoked_at` set exactly when `revoked`; the volume and the VM are on
    /// the source; the warm node is unchanged; no `app.failed_over`.
    async fn unmoved(&self, revoked: bool, case: &str) {
        let pool = self.server.state.store.pool();
        let (holder, token, revoked_at): (String, i64, Option<String>) =
            sqlx::query_as("SELECT holder_node_id, fencing_token, revoked_at FROM leases")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(holder, self.source.node_id, "{case}: lease holder");
        assert_eq!(token, 4, "{case}: lease token");
        assert_eq!(revoked_at.is_some(), revoked, "{case}: revoked_at");
        let volume_node: String = sqlx::query_scalar("SELECT node_id FROM volumes WHERE id = ?")
            .bind(VOLUME_ID)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(volume_node, self.source.node_id, "{case}: volume node");
        let vm_node: String = sqlx::query_scalar("SELECT node_id FROM vms WHERE id = ?")
            .bind(VM_ID)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(vm_node, self.source.node_id, "{case}: VM node");
        let (warm_node_id, pinned): (Option<String>, i64) =
            sqlx::query_as("SELECT warm_node_id, warm_node_pinned FROM apps WHERE id = ?")
                .bind(&self.app_id)
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(
            (warm_node_id.as_deref(), pinned),
            (Some(self.warm.node_id.as_str()), 0),
            "{case}: warm node"
        );
        let events: i64 =
            sqlx::query_scalar("SELECT count(*) FROM events WHERE type = 'app.failed_over'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(events, 0, "{case}: app.failed_over");
    }

    /// A failover with the source connected: the source answers its revoke
    /// `ok`, and the call's answer.
    async fn connected_failover(&mut self) -> reqwest::Response {
        let call = self.call(&self.warm.node_id);
        let revoke = recv_type(self.on_source(), "lease_revoke").await;
        let revoke_id = revoke["id"].as_str().unwrap().to_string();
        send(self.on_source(), ok_result(), Some(&revoke_id)).await;
        call.await.unwrap()
    }

    /// The app's `lease` as `getApp` shows it.
    async fn lease_view(&self) -> Value {
        let app: Value = self
            .owner
            .get(self.server.url(&format!("/api/v1/apps/{}", self.app_id)))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        app["lease"].clone()
    }

    /// `lease`, a 202's `App.lease`, and `getApp`'s, show the grant to the
    /// warm node waiting at token 5 with the mark set, on both nodes, which
    /// follow no log (ADR 0292).
    async fn waiting(&self, lease: &Value) {
        let mut both = vec![self.source.node_id.clone(), self.warm.node_id.clone()];
        both.sort();
        for lease in [lease.clone(), self.lease_view().await] {
            assert_eq!(
                lease["holder_node_id"],
                self.warm.node_id.as_str(),
                "{lease}"
            );
            assert_eq!(lease["fencing_token"], 5, "{lease}");
            assert_eq!(lease["unsettled"], true, "{lease}");
            assert_eq!(lease["grant_pending"], true, "{lease}");
            let mut waiting_on: Vec<String> = lease["grant_waiting_on"]
                .as_array()
                .unwrap_or_else(|| panic!("{lease}"))
                .iter()
                .map(|id| id.as_str().unwrap().to_string())
                .collect();
            waiting_on.sort();
            assert_eq!(waiting_on, both, "{lease}");
        }
    }

    /// A new session of `node` that follows the change log from its start,
    /// with the mesh key of `mesh_seed`, listing no lease.
    async fn follower(&self, node: &Enrolled, mesh_seed: u8) -> common::machine::Machine {
        let mut hello = common::machine::hello_body(&node.node_id);
        hello.mesh_public_key =
            Some(base64::engine::general_purpose::STANDARD.encode([mesh_seed; 32]));
        let node = common::machine::Node {
            node_id: node.node_id.clone(),
            credential: node.credential.clone(),
        };
        common::machine::Machine::connect_with(&self.server, &node, hello, true, true).await
    }

    /// The lease names the warm node at token 5, `expiring` for `D`, with
    /// the unsettled mark set: its grant waits (ADR 0292, ADR 0303 Q2).
    async fn moved_marked(&self) {
        self.moved().await;
        let unsettled: i64 = sqlx::query_scalar("SELECT unsettled FROM leases")
            .fetch_one(self.server.state.store.pool())
            .await
            .unwrap();
        assert_eq!(unsettled, 1);
    }

    /// The lease names the warm node at token 5, `expiring` for `D`.
    async fn moved(&self) {
        let (holder, token, mode, duration, revoked_at): (
            String,
            i64,
            String,
            Option<i64>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT holder_node_id, fencing_token, mode, duration_ms, revoked_at FROM leases",
        )
        .fetch_one(self.server.state.store.pool())
        .await
        .unwrap();
        assert_eq!(
            (holder.as_str(), token, mode.as_str(), duration, revoked_at),
            (
                self.warm.node_id.as_str(),
                5,
                "expiring",
                Some(15_000),
                None
            )
        );
    }
}

/// `(operation, result, scope, params, change_seq)` of an audit record.
type AuditRow = (String, String, Option<String>, String, Option<i64>);

/// Waits until the primary no longer has a session of `node_id`.
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

/// The problem body of a refusal, with its status, code and reason checked.
async fn refused(res: reqwest::Response, status: u16, code: &str, reason: Option<&str>) -> Value {
    assert_eq!(res.status().as_u16(), status);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["code"], code, "{body}");
    assert_eq!(body["reason"].as_str(), reason, "{body}");
    body
}

/// The `lease_grant` the warm node got, after its earlier frames.
async fn grant_of(socket: &mut Socket) -> Value {
    recv_type(socket, "lease_grant").await
}

/// Every refusal before the revoke, in the contract's order, moves nothing
/// and sends no revoke: `not_warm`, `validation_failed`, `no_copy`,
/// `fence_failed` (the warm node's own failed fence), `no_support` without
/// voter support, and `node_disconnected`.
#[tokio::test]
async fn the_refusals_move_nothing() {
    let mut s = Setup::new().await;
    s.expiring().await;
    let pool = s.server.state.store.pool().clone();

    sqlx::query("UPDATE apps SET ha_policy = 'none' WHERE id = ?")
        .bind(&s.app_id)
        .execute(&pool)
        .await
        .unwrap();
    refused(s.failover().await, 409, "conflict", Some("not_warm")).await;
    s.unmoved(false, "not_warm").await;
    sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
        .bind(&s.app_id)
        .execute(&pool)
        .await
        .unwrap();

    let source_id = s.source.node_id.clone();
    let res = s.call(&source_id).await.unwrap();
    refused(res, 422, "validation_failed", None).await;
    s.unmoved(false, "validation_failed").await;

    refused(s.failover().await, 409, "conflict", Some("no_copy")).await;
    s.unmoved(false, "no_copy").await;

    s.report_copy().await;
    let failure = json!({
        "detected_at": "2026-11-09T10:00:00Z",
        "lease_ids": ["lse_0000000000000000000000000X"],
        "app_ids": [],
        "vm_ids": [],
    })
    .to_string();
    sqlx::query("UPDATE nodes SET fence_failure = ? WHERE id = ?")
        .bind(&failure)
        .bind(&s.warm.node_id)
        .execute(&pool)
        .await
        .unwrap();
    refused(s.failover().await, 409, "conflict", Some("fence_failed")).await;
    s.unmoved(false, "fence_failed").await;
    sqlx::query("UPDATE nodes SET fence_failure = NULL WHERE id = ?")
        .bind(&s.warm.node_id)
        .execute(&pool)
        .await
        .unwrap();

    let body = refused(s.failover().await, 409, "conflict", Some("no_support")).await;
    assert!(
        body["detail"].as_str().unwrap().contains("majority"),
        "{body}"
    );
    s.unmoved(false, "no_support").await;

    drop(s.on_warm.take());
    disconnected(&s.server, &s.warm.node_id).await;
    set_support(&s.server, true);
    refused(
        s.failover().await,
        409,
        "conflict",
        Some("node_disconnected"),
    )
    .await;
    s.unmoved(false, "node_disconnected").await;

    none_of(s.on_source(), "lease_revoke", "every refusal").await;
    let records: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log \
         WHERE operation = 'POST /api/v1/apps/{app_id}/failover' ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(records.len(), 6, "{records:?}");
    assert!(
        records.iter().all(|(result, _)| result == "error"),
        "{records:?}"
    );
}

/// Section 8.4: a source that is not connected may still hold an
/// `expiring` lease until its gate has passed, so the lease does not move.
#[tokio::test]
async fn a_disconnected_source_holds_the_lease_until_its_gate_passed() {
    let mut s = Setup::new().await;
    s.ready().await;
    s.disconnect_source().await;
    // The gate passes 5 s from now (3 s and two margins of 1 s).
    s.server
        .state
        .hub
        .authority()
        .seed(LEASE_ID, &s.source.node_id, Some(3_000));
    let body = refused(s.failover().await, 409, "conflict", Some("lease_held")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.contains("gate"), "{detail}");
    assert!(
        !detail.contains("stays revoked"),
        "nothing was revoked: {detail}"
    );
    s.unmoved(false, "lease_held").await;
    none_of(s.on_warm(), "lease_grant", "lease_held").await;
}

/// Section 8.4: a source that is not connected holds a `local` lease until
/// it answers a revoke or is declared lost, so the lease does not move.
/// Once it is declared lost it no longer holds it: the grant raises the
/// stored duration of a `local` row, so the change writes the row marked,
/// answers `202`, and holds the grant (ADR 0292, ADR 0303 Q2). The source,
/// declared lost but not removed, stays in the required set, so the grant
/// waits on it.
#[tokio::test]
async fn a_disconnected_sources_local_lease_is_held_until_it_is_declared_lost() {
    let mut s = Setup::new().await;
    s.report_copy().await;
    set_support(&s.server, true);
    s.disconnect_source().await;

    let body = refused(s.failover().await, 409, "conflict", Some("lease_held")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.contains("local"), "{detail}");
    assert!(detail.contains("declared lost"), "{detail}");
    s.unmoved(false, "local row, source not lost").await;

    sqlx::query("UPDATE nodes SET lost_at = ? WHERE id = ?")
        .bind(quoro_control_plane::store::now_rfc3339())
        .bind(&s.source.node_id)
        .execute(s.server.state.store.pool())
        .await
        .unwrap();
    let res = s.failover().await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    s.waiting(&app["lease"]).await;
    s.moved_marked().await;
    none_of(s.on_warm(), "lease_grant", "local row").await;
}

/// ADR 0292, ADR 0303. With support, a failover whose grant must wait for
/// its write to be replicated over the machines with the `node` role goes
/// on: the connected source gets its revoke, and once it answered, the
/// change writes the row naming the warm node at token 5 with the mark set,
/// moves the volume and turns the standby VM `running` in the warm node's
/// desired state, and the call answers `202` with `grant_pending` true and
/// `grant_waiting_on` naming both nodes, which follow no log. No grant goes
/// out, and `app.failed_over` carries `grant_pending: true`. Each case is a
/// grant that must wait: a row that reads `local` (a raise of the stored
/// duration) and a marked row that reads `expiring` for `D`.
#[tokio::test]
async fn a_failover_whose_grant_waits_answers_202_and_holds_the_grant() {
    for marked in [false, true] {
        let case = if marked { "marked row" } else { "local row" };
        let mut s = Setup::new().await;
        s.report_copy().await;
        set_support(&s.server, true);
        if marked {
            s.expiring().await;
            s.sql("UPDATE leases SET unsettled = 1 WHERE id = ?").await;
        }
        let res = s.connected_failover().await;
        assert_eq!(res.status(), 202, "{case}: {}", res.text().await.unwrap());
        let app: Value = res.json().await.unwrap();
        assert_eq!(
            app["placement"]["node_ids"],
            json!([s.warm.node_id]),
            "{case}"
        );
        s.waiting(&app["lease"]).await;
        s.moved_marked().await;
        let warm_id = s.warm.node_id.clone();
        assert_eq!(
            vm_and_app(&s).await,
            (warm_id, "running".to_string(), "deploying".to_string()),
            "{case}"
        );
        let (data,): (String,) =
            sqlx::query_as("SELECT data FROM events WHERE type = 'app.failed_over'")
                .fetch_one(s.server.state.store.pool())
                .await
                .unwrap();
        let data: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(data["grant_pending"], true, "{case}: {data}");
        assert_eq!(data["fencing_token"], 5, "{case}: {data}");

        // The standby VM is `running` in the warm node's desired state under
        // the new token, and no grant reached the warm node.
        let mut frames = Vec::new();
        loop {
            let frame = recv_frame(s.on_warm()).await;
            let runs = frame["type"] == "desired_state"
                && frame["body"]["spec"]["vms"].as_array().is_some_and(|vms| {
                    vms.iter().any(|vm| {
                        vm["vm_id"] == VM_ID
                            && vm["desired_status"] == "running"
                            && vm["fencing_token"] == 5
                    })
                });
            frames.push(frame);
            if runs {
                break;
            }
        }
        assert!(
            !frames.iter().any(|frame| frame["type"] == "lease_grant"),
            "{case}: {frames:?}"
        );
        none_of(s.on_warm(), "lease_grant", case).await;
    }
}

/// ADR 0292: while the grant to the warm node waits, a second failover of
/// the app answers `in_progress`, checked before `node_id`: the first call
/// made the old source the app's warm node, so the same `node_id` would
/// otherwise be `validation_failed`. Nothing moves again. Then both nodes
/// follow the change log and the primary holds support, and the held grant
/// goes out to the warm node, once, at token 5 for `D`; `grant_pending`
/// turns false and the row stays marked. A failover then no longer answers
/// `in_progress`.
#[tokio::test]
async fn a_held_failover_grant_answers_in_progress_then_goes_out_once_replicated() {
    let mut s = Setup::new().await;
    s.report_copy().await;
    set_support(&s.server, true);
    let res = s.connected_failover().await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());

    for node_id in [s.warm.node_id.clone(), s.source.node_id.clone()] {
        let body = refused(
            s.call(&node_id).await.unwrap(),
            409,
            "conflict",
            Some("in_progress"),
        )
        .await;
        assert!(
            body["detail"].as_str().unwrap().contains("grant_pending"),
            "{body}"
        );
    }
    s.moved_marked().await;
    let revokes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'lease.revoke'")
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(revokes, 1, "no second revoke");

    // Both nodes come back following the change log.
    drop(s.on_source.take());
    drop(s.on_warm.take());
    disconnected(&s.server, &s.source.node_id).await;
    disconnected(&s.server, &s.warm.node_id).await;
    let mut on_source = s.follower(&s.source, 1).await;
    let mut on_warm = s.follower(&s.warm, 2).await;
    let head = common::log::log_head(&s.server).await;
    let mut seen = on_warm.start.clone();
    for _ in 0..50 {
        if on_warm.log.0 >= head && on_source.log.0 >= head {
            break;
        }
        seen.extend(on_warm.settle().await);
        on_source.settle().await;
    }
    assert!(on_warm.log.0 >= head && on_source.log.0 >= head);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !seen.iter().any(|frame| frame["type"] == "lease_grant") {
        assert!(tokio::time::Instant::now() < deadline, "{seen:?}");
        seen.extend(on_warm.settle().await);
    }
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    seen.extend(on_warm.settle().await);
    let grants: Vec<&Value> = seen
        .iter()
        .filter(|frame| frame["type"] == "lease_grant")
        .collect();
    assert_eq!(grants.len(), 1, "{seen:?}");
    assert_eq!(grants[0]["body"]["lease_id"], LEASE_ID);
    assert_eq!(grants[0]["body"]["fencing_token"], 5);
    assert_eq!(grants[0]["body"]["mode"], "expiring");
    assert_eq!(grants[0]["body"]["duration_ms"], 15_000);

    let lease = s.lease_view().await;
    assert_eq!(lease["grant_pending"], false, "{lease}");
    assert_eq!(lease["grant_waiting_on"], json!([]), "{lease}");
    assert_eq!(lease["unsettled"], true, "{lease}");
    let res = s.call(&s.source.node_id).await.unwrap();
    assert_ne!(
        res.json::<Value>().await.unwrap()["reason"],
        "in_progress",
        "the grant went out"
    );
}

/// A source that is not connected, with an unmarked `expiring` row for `D`
/// whose gate has passed (the primary keeps none for it): the lease, the
/// volume and the instance move to the warm node, the old source becomes the
/// warm node, and the warm node gets its grant and its new desired state.
#[tokio::test]
async fn a_disconnected_sources_lease_moves_to_the_warm_node() {
    let mut s = Setup::new().await;
    s.ready().await;
    s.disconnect_source().await;

    let res = s.failover().await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    assert_eq!(app["id"], s.app_id.as_str());
    assert_eq!(app["status"], "deploying");
    assert_eq!(app["placement"]["node_ids"], json!([s.warm.node_id]));
    assert_eq!(app["lease"]["holder_node_id"], s.warm.node_id.as_str());
    assert_eq!(app["lease"]["fencing_token"], 5);
    assert_eq!(app["warm"]["node_id"], s.source.node_id.as_str());
    s.moved().await;

    let warm_id = s.warm.node_id.clone();
    let source_id = s.source.node_id.clone();
    let grant = grant_of(s.on_warm()).await;
    assert_eq!(grant["body"]["lease_id"], LEASE_ID);
    assert_eq!(grant["body"]["fencing_token"], 5);
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);

    let pool = s.server.state.store.pool().clone();
    let volume_node: String = sqlx::query_scalar("SELECT node_id FROM volumes WHERE id = ?")
        .bind(VOLUME_ID)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(volume_node, warm_id);
    let (vm_node, desired): (String, String) =
        sqlx::query_as("SELECT node_id, desired_status FROM vms WHERE id = ?")
            .bind(VM_ID)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (vm_node.as_str(), desired.as_str()),
        (warm_id.as_str(), "running")
    );
    let (warm_node_id, pinned, placement): (Option<String>, i64, String) =
        sqlx::query_as("SELECT warm_node_id, warm_node_pinned, placement FROM apps WHERE id = ?")
            .bind(&s.app_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(warm_node_id.as_deref(), Some(source_id.as_str()));
    assert_eq!(pinned, 0);
    assert_eq!(
        serde_json::from_str::<Value>(&placement).unwrap(),
        json!({ "node_ids": [warm_id] })
    );

    let (data,): (String,) =
        sqlx::query_as("SELECT data FROM events WHERE type = 'app.failed_over'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&data).unwrap(),
        json!({
            "app_id": s.app_id,
            "from_node_id": source_id,
            "to_node_id": warm_id,
            "fencing_token": 5,
            "snapshot_seq": 3,
            "snapshot_taken_at": SNAPSHOT_TAKEN_AT,
            "grant_pending": false,
        })
    );

    let records: Vec<AuditRow> = sqlx::query_as(
        "SELECT operation, result, scope, params, change_seq FROM audit_log \
         WHERE operation IN ('POST /api/v1/apps/{app_id}/failover', 'lease.grant', \
         'lease.revoke') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(records.len(), 2, "no revoke went out: {records:?}");
    let (operation, result, scope, params, change_seq) = &records[0];
    assert_eq!((operation.as_str(), result.as_str()), ("lease.grant", "ok"));
    assert_eq!(*scope, None);
    assert_eq!(
        serde_json::from_str::<Value>(params).unwrap(),
        json!({
            "node_id": warm_id,
            "fencing_token": 5,
            "mode": "expiring",
            "duration_ms": 15_000,
        })
    );
    assert!(change_seq.is_some());
    let (operation, result, scope, params, _) = &records[1];
    assert_eq!(
        (operation.as_str(), result.as_str(), scope.as_deref()),
        (
            "POST /api/v1/apps/{app_id}/failover",
            "ok",
            Some("app:failover")
        )
    );
    assert_eq!(
        serde_json::from_str::<Value>(params).unwrap(),
        json!({ "node_id": warm_id })
    );

    let (payload,): (String,) = sqlx::query_as(
        "SELECT payload FROM change_log WHERE entity_type = 'app' AND entity_id = ? \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(&s.app_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["id"], s.app_id.as_str());
    assert_eq!(payload["name"], "db");
    assert_eq!(payload["image"], IMAGE);
    assert_eq!(payload["placement"], json!({ "node_ids": [warm_id] }));
    assert_eq!(payload["status"], "deploying");
    assert_eq!(payload["warm"], Value::Null);
    assert_eq!(payload["warm_node_id"], source_id.as_str());
    assert_eq!(payload["warm_node_pinned"], false);
    assert_eq!(payload["warm_interval_s"], 30);

    // Section 6.13: the warm node is the source now, and runs the app under
    // the new token.
    let spec = spec_where(s.on_warm(), |spec| {
        spec["volumes"][0]["warm"]["role"] == "source"
    })
    .await;
    assert_eq!(spec["volumes"][0]["volume_id"], VOLUME_ID);
    assert_eq!(spec["volumes"][0]["state"], "present");
    assert_eq!(
        spec["volumes"][0]["warm"]["peer_node_id"],
        source_id.as_str()
    );
    assert_eq!(spec["volumes"][0]["warm"]["fencing_token"], 5);
    let vm = &spec["vms"][0];
    assert_eq!(vm["vm_id"], VM_ID);
    assert_eq!(vm["desired_status"], "running");
    assert_ne!(vm["standby"], true, "absent means false: {vm}");
    assert_eq!(vm["lease_id"], LEASE_ID);
    assert_eq!(vm["fencing_token"], 5);
}

/// The revoke of the lease's payloads in the change log, oldest first.
async fn lease_payloads(server: &Server) -> Vec<Value> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT payload FROM change_log WHERE entity_type = 'lease' AND entity_id = ? \
         ORDER BY seq",
    )
    .bind(LEASE_ID)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(payload,)| serde_json::from_str(&payload).unwrap())
        .collect()
}

/// A connected source gets the revoke first (reason `moved`), and the warm
/// node gets its grant only once the source answered it `ok`. The lease row
/// was revoked at token 4, then granted at token 5, each with its system
/// audit record.
#[tokio::test]
async fn a_connected_source_answers_the_revoke_before_the_lease_moves() {
    let mut s = Setup::new().await;
    s.ready().await;
    let call = s.call(&s.warm.node_id);

    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    assert_eq!(revoke["body"]["lease_id"], LEASE_ID);
    assert_eq!(revoke["body"]["fencing_token"], 4);
    assert_eq!(revoke["body"]["reason"], "moved");
    tokio::time::sleep(Duration::from_millis(300)).await;
    none_of(s.on_warm(), "lease_grant", "before the answer").await;
    assert!(!call.is_finished(), "the call waits for the answer");
    s.unmoved(true, "before the answer").await;

    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;
    let res = call.await.unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    s.moved().await;
    let grant = grant_of(s.on_warm()).await;
    assert_eq!(grant["body"]["fencing_token"], 5);
    assert_eq!(grant["body"]["mode"], "expiring");

    let payloads = lease_payloads(&s.server).await;
    let revoked = &payloads[payloads.len() - 2];
    assert_eq!(revoked["fencing_token"], 4);
    assert_eq!(revoked["holder_node_id"], s.source.node_id.as_str());
    assert!(revoked["revoked_at"].is_string(), "{revoked}");
    let granted = &payloads[payloads.len() - 1];
    assert_eq!(granted["fencing_token"], 5);
    assert_eq!(granted["holder_node_id"], s.warm.node_id.as_str());
    assert_eq!(granted["revoked_at"], Value::Null);

    let records: Vec<(String, String)> = sqlx::query_as(
        "SELECT operation, params FROM audit_log \
         WHERE operation IN ('lease.revoke', 'lease.grant') ORDER BY seq",
    )
    .fetch_all(s.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[0].0, "lease.revoke");
    assert_eq!(
        serde_json::from_str::<Value>(&records[0].1).unwrap(),
        json!({
            "node_id": s.source.node_id,
            "fencing_token": 4,
            "reason": "moved",
        })
    );
    assert_eq!(records[1].0, "lease.grant");
}

/// Section 8.4: once the source answered, the grant still waits for the
/// lease's gate. The call stays open until it passed, then answers `202`.
#[tokio::test]
async fn a_connected_sources_lease_moves_once_its_gate_passed() {
    let mut s = Setup::new().await;
    s.ready().await;
    // The gate passes 5 s from now (3 s and two margins of 1 s).
    s.server
        .state
        .hub
        .authority()
        .seed(LEASE_ID, &s.source.node_id, Some(3_000));
    let passes = tokio::time::Instant::now() + Duration::from_millis(5_000);
    let call = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;

    let res = call.await.unwrap();
    assert!(
        tokio::time::Instant::now() >= passes,
        "the call answered before the gate passed"
    );
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    s.moved().await;
    assert_eq!(grant_of(s.on_warm()).await["body"]["fencing_token"], 5);
}

/// Section 8.5: a revoke answered `fence_failed` keeps the lease where it is
/// until an operator acknowledges the failure.
#[tokio::test]
async fn a_revoke_answered_fence_failed_moves_nothing() {
    let mut s = Setup::new().await;
    s.ready().await;
    let call = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), fence_failed_result(), Some(&revoke_id)).await;

    let body = refused(call.await.unwrap(), 409, "conflict", Some("fence_failed")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(
        detail.contains("stays revoked and its app stopped"),
        "{detail}"
    );
    s.unmoved(true, "fence_failed").await;
    none_of(s.on_warm(), "lease_grant", "fence_failed").await;
}

/// `openapi.yaml` `failoverApp`: `fence_failed` when "the warm node has a
/// failed fence that no operator acknowledged", recorded or not (section
/// 8.5 step 1). The warm node answers the revoke of a lease no row knows
/// `fence_failed`, and the primary cannot record it. The call refuses with
/// `fence_failed` naming the warm node, before `no_support`, and nothing
/// moves.
#[tokio::test]
async fn an_unrecorded_failed_fence_on_the_warm_node_refuses() {
    let mut s = Setup::new().await;
    s.expiring().await;
    drop(s.on_warm.take());
    disconnected(&s.server, &s.warm.node_id).await;

    s.server.state.hub.inject_fault(Fault::FailureRecord {
        node_id: s.warm.node_id.clone(),
    });
    let NodeMessage::Hello(mut body) = hello(&s.warm.node_id, 2) else {
        unreachable!("hello builds a hello");
    };
    body.leases = vec![wire::HelloLease {
        lease_id: "lse_0000000000000000000000000Z".to_string(),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: "app_0000000000000000000000000Z".to_string(),
        mode: quoro_proto::LeaseMode::Local,
        fencing_token: 1,
        state: "held".to_string(),
    }];
    let mut on_warm = connect(&s.server, &s.warm.credential).await;
    send(&mut on_warm, NodeMessage::Hello(body), None).await;
    let revoke = recv_type(&mut on_warm, "lease_revoke").await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(&mut on_warm, fence_failed_result(), Some(&revoke_id)).await;
    frames_until_settled(&mut on_warm).await;
    s.on_warm = Some(on_warm);
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&s.warm.node_id)
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(failure, None, "the failed fence was not recorded");
    s.report_copy().await;

    let body = refused(s.failover().await, 409, "conflict", Some("fence_failed")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.contains("warm node"), "{detail}");
    s.unmoved(false, "unrecorded failed fence on the warm node")
        .await;
    none_of(s.on_source(), "lease_revoke", "fence_failed").await;
}

/// Section 8.5, issue #392: a revoke answered `fence_failed` that the
/// primary could not record stays unanswered. Once the source's session
/// ends, the lease still does not move, though its gate has passed: the
/// source's VMs may still run.
#[tokio::test]
async fn an_unrecorded_fence_failure_holds_the_lease_after_the_source_leaves() {
    let mut s = Setup::new().await;
    s.ready().await;
    let pool = s.server.state.store.pool().clone();
    sqlx::query(
        "CREATE TRIGGER test_refuse_fence_failure BEFORE UPDATE OF fence_failure ON nodes \
         BEGIN SELECT RAISE(ABORT, 'test: the failed fence is not recorded'); END",
    )
    .execute(&pool)
    .await
    .unwrap();
    let call = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), fence_failed_result(), Some(&revoke_id)).await;
    // The session handles messages in order: the answer is handled once
    // the pings are answered.
    frames_until_settled(s.on_source()).await;
    assert!(
        !call.is_finished(),
        "the call waits while the source is connected"
    );
    s.disconnect_source().await;

    let body = refused(call.await.unwrap(), 409, "conflict", Some("fence_failed")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.contains("could not record"), "{detail}");
    assert!(
        detail.contains("stays revoked and its app stopped"),
        "{detail}"
    );
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&s.source.node_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(failure, None, "the failed fence was not recorded");
    s.unmoved(true, "unrecorded fence failure").await;
    none_of(s.on_warm(), "lease_grant", "unrecorded fence failure").await;
}

/// The change checks the warm node again: one whose session ended while the
/// source held its revoke unanswered gets no lease, though the source then
/// answers `ok`. The lease stays revoked at the source's token, and nothing
/// moves.
#[tokio::test]
async fn a_warm_node_that_leaves_during_the_wait_gets_no_lease() {
    let mut s = Setup::new().await;
    s.ready().await;
    let call = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    drop(s.on_warm.take());
    disconnected(&s.server, &s.warm.node_id).await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;

    refused(
        call.await.unwrap(),
        409,
        "conflict",
        Some("node_disconnected"),
    )
    .await;
    s.unmoved(true, "warm node left").await;
    none_of(s.on_source(), "lease_grant", "warm node left").await;
    let grants: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'lease.grant'")
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(grants, 0, "no grant was written");
}

/// A source whose session ends before it answered may still hold the lease
/// until its gate has passed, so the call answers `lease_held`.
#[tokio::test]
async fn a_source_that_leaves_before_answering_holds_the_lease() {
    let mut s = Setup::new().await;
    s.ready().await;
    // A gate that has not passed: the holder may still run the app.
    s.server
        .state
        .hub
        .authority()
        .seed(LEASE_ID, &s.source.node_id, Some(15_000));
    let call = s.call(&s.warm.node_id);
    recv_type(s.on_source(), "lease_revoke").await;
    drop(s.on_source.take());

    let body = refused(call.await.unwrap(), 409, "conflict", Some("lease_held")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.contains("not connected"), "{detail}");
    assert!(
        detail.contains("stays revoked and its app stopped"),
        "{detail}"
    );
    s.unmoved(true, "session lost").await;
    none_of(s.on_warm(), "lease_grant", "session lost").await;
}

/// A second failover of the app while the first waits for its revoke's
/// answer is refused with `in_progress` at once, before the source answers;
/// the first then completes.
#[tokio::test]
async fn a_second_failover_while_the_first_runs_is_in_progress() {
    let mut s = Setup::new().await;
    s.ready().await;
    let first = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;

    // A second call that went on would wait for the same answer: this
    // client gives up after 2 s.
    let res = s
        .owner
        .post(s.server.url(&format!("/api/v1/apps/{}/failover", s.app_id)))
        .timeout(Duration::from_secs(2))
        .json(&json!({ "node_id": s.warm.node_id }))
        .send()
        .await
        .expect("the second call answers while the revoke is unanswered");
    refused(res, 409, "conflict", Some("in_progress")).await;
    assert!(!first.is_finished(), "the first call still waits");
    s.unmoved(true, "while the first waits").await;

    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;
    let res = first.await.unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    s.moved().await;
}

/// The app's VM row as `(node_id, desired_status)`, and the app's status.
async fn vm_and_app(s: &Setup) -> (String, String, String) {
    let pool = s.server.state.store.pool();
    let (node_id, desired): (String, String) =
        sqlx::query_as("SELECT node_id, desired_status FROM vms WHERE id = ?")
            .bind(VM_ID)
            .fetch_one(pool)
            .await
            .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM apps WHERE id = ?")
        .bind(&s.app_id)
        .fetch_one(pool)
        .await
        .unwrap();
    (node_id, desired, status)
}

/// Support that lapses after the source answered its revoke: the grant is
/// held, so the call answers `no_support` and the lease stays revoked. The
/// revoke stopped the app, so the source's next desired state names its VM
/// `stopped`, not `running` without its lease. With support back, a retry
/// goes straight to the grant, with no second revoke, and the VM runs on
/// the warm node.
#[tokio::test]
async fn support_lost_after_the_revoke_answers_no_support_and_a_retry_grants() {
    let mut s = Setup::new().await;
    s.ready().await;
    let call = s.call(&s.warm.node_id);
    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    set_support(&s.server, false);
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;

    let body = refused(call.await.unwrap(), 409, "conflict", Some("no_support")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(
        detail.contains("stays revoked and its app stopped"),
        "{detail}"
    );
    s.unmoved(true, "support lost").await;
    none_of(s.on_warm(), "lease_grant", "support lost").await;
    let spec = spec_where(s.on_source(), |spec| {
        spec["vms"]
            .as_array()
            .is_some_and(|vms| vms.iter().any(|vm| vm["vm_id"] == VM_ID))
    })
    .await;
    let vm = spec["vms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vm| vm["vm_id"] == VM_ID)
        .unwrap();
    assert_eq!(vm["desired_status"], "stopped", "{vm}");
    let source_id = s.source.node_id.clone();
    assert_eq!(
        vm_and_app(&s).await,
        (source_id, "stopped".to_string(), "stopped".to_string())
    );

    set_support(&s.server, true);
    let res = s.failover().await;
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    s.moved().await;
    let warm_id = s.warm.node_id.clone();
    assert_eq!(
        vm_and_app(&s).await,
        (warm_id, "running".to_string(), "deploying".to_string())
    );
    assert_eq!(grant_of(s.on_warm()).await["body"]["fencing_token"], 5);
    none_of(s.on_source(), "lease_revoke", "the retry").await;
    let revokes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'lease.revoke'")
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(revokes, 1);
}

/// ADR 0303 Q3, `openapi.yaml` `failoverApp`: support lapses after the
/// change's support check and before its write, so the grant would wait
/// for it. The call answers `no_support` and moves nothing: the lease stays
/// revoked at the source's token, unmarked, no grant goes out and none is
/// held.
#[tokio::test]
async fn support_lost_at_the_write_answers_no_support_and_moves_nothing() {
    let mut s = Setup::new().await;
    s.ready().await;
    s.server
        .state
        .hub
        .inject_fault(Fault::FailoverSupportLapse {
            app_id: s.app_id.clone(),
        });

    let res = s.connected_failover().await;
    let body = refused(res, 409, "conflict", Some("no_support")).await;
    let detail = body["detail"].as_str().unwrap();
    assert!(
        detail.contains("stays revoked and its app stopped"),
        "{detail}"
    );
    s.unmoved(true, "support lost at the write").await;
    let unsettled: i64 = sqlx::query_scalar("SELECT unsettled FROM leases")
        .fetch_one(s.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(unsettled, 0);
    let lease = s.lease_view().await;
    assert_eq!(lease["grant_pending"], false, "{lease}");
    none_of(s.on_warm(), "lease_grant", "support lost at the write").await;
}

/// A client that gives up while the source holds its revoke unanswered does
/// not drop the failover: once the source answers, the lease moves, and a
/// retry with the same `Idempotency-Key` answers the stored `202`.
#[tokio::test]
async fn a_client_that_gives_up_does_not_drop_the_failover() {
    let mut s = Setup::new().await;
    s.ready().await;
    let url = s.server.url(&format!("/api/v1/apps/{}/failover", s.app_id));
    let body = json!({ "node_id": s.warm.node_id });
    let gave_up = s
        .owner
        .post(&url)
        .header("Idempotency-Key", "failover-1")
        .timeout(Duration::from_secs(1))
        .json(&body)
        .send()
        .await
        .expect_err("the client gives up after 1 s");
    assert!(gave_up.is_timeout(), "{gave_up}");

    let revoke = recv_type(s.on_source(), "lease_revoke").await;
    let revoke_id = revoke["id"].as_str().unwrap().to_string();
    send(s.on_source(), ok_result(), Some(&revoke_id)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let holder: String = sqlx::query_scalar("SELECT holder_node_id FROM leases WHERE id = ?")
            .bind(LEASE_ID)
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
        if holder == s.warm.node_id {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lease never moved"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    s.moved().await;
    assert_eq!(grant_of(s.on_warm()).await["body"]["fencing_token"], 5);

    let res = s
        .owner
        .post(&url)
        .header("Idempotency-Key", "failover-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    assert_eq!(app["lease"]["holder_node_id"], s.warm.node_id.as_str());
    assert_eq!(app["lease"]["fencing_token"], 5);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM audit_log WHERE operation = 'lease.grant'), \
         (SELECT count(*) FROM audit_log \
          WHERE operation = 'POST /api/v1/apps/{app_id}/failover' AND result = 'ok' \
          AND change_seq IS NOT NULL)",
    )
    .fetch_one(s.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(counts, (1, 1), "one failover ran, the retry replayed it");
}

async fn owner_user_id(owner: &reqwest::Client, server: &Server) -> String {
    owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_agent(
    owner: &reqwest::Client,
    server: &Server,
    owner_user_id: &str,
    name: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": name,
            "model": "test-model",
            "owner_user_id": owner_user_id,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create agent: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn agent_token(owner: &reqwest::Client, server: &Server, agent_id: &str) -> String {
    let key: Value = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key = key["key"].as_str().unwrap().to_string();
    let token: Value = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

/// `app:failover` is destructive: an agent needs an approval grant on the
/// app with `node_id` in its params (ADR 0030).
#[tokio::test]
async fn an_agent_needs_an_approval_to_fail_over() {
    let mut s = Setup::new().await;
    s.ready().await;
    s.disconnect_source().await;
    let owner_user_id = owner_user_id(&s.owner, &s.server).await;
    let agent_id = create_agent(&s.owner, &s.server, &owner_user_id, "failer").await;
    let res = s
        .owner
        .post(s.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": ["approval:create"],
            "selector": { "all": true },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "grant: {}", res.text().await.unwrap());
    let token = agent_token(&s.owner, &s.server, &agent_id).await;
    let call = || {
        reqwest::Client::new()
            .post(s.server.url(&format!("/api/v1/apps/{}/failover", s.app_id)))
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "the source node is lost")
            .json(&json!({ "node_id": s.warm.node_id }))
            .send()
    };

    let (status, code, _) = common::problem(call().await.unwrap()).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));
    s.unmoved(false, "approval_required").await;

    let res = reqwest::Client::new()
        .post(s.server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "scope": "app:failover",
            "target": { "type": "app", "id": s.app_id },
            "operation": "POST /api/v1/apps/{app_id}/failover",
            "params": { "node_id": s.warm.node_id },
            "justification": "the source node is lost",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "approval: {}", res.text().await.unwrap());
    let approval_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = s
        .owner
        .post(
            s.server
                .url(&format!("/api/v1/approvals/{approval_id}/approve")),
        )
        .json(&json!({ "note": "approved for the test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());

    let res = call().await.unwrap();
    assert_eq!(res.status(), 202, "failover: {}", res.text().await.unwrap());
    s.moved().await;
    let (approval, grant): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT approval_id, grant_id FROM audit_log \
         WHERE operation = 'POST /api/v1/apps/{app_id}/failover' AND result = 'ok'",
    )
    .fetch_one(s.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(approval.as_deref(), Some(approval_id.as_str()));
    assert!(grant.is_some());
}
