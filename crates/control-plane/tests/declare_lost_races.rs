//! Declaring a node lost against the node's return (`node-agent-protocol.md`
//! sections 4.2, 8.3 and 9; ADR 0183, ADR 0202; issue #761): a `hello` that
//! meets `declareNodeLost`, in both orders, and a node that is cut off,
//! declared lost while it may still run its app, and returns after the app
//! moved. Over real HTTP and WebSocket with fake nodes, and a `MemoryStore`
//! as the bucket of the backup whose restore moves the app.
//!
//! Each test that compares a declaration with a report of the node waits
//! until the primary's clock is past the earlier of the two, so no
//! assertion here rests on how two times taken in the same millisecond
//! compare.

mod common;

use common::machine::{Node, Socket, dial, enroll, hello_body, ok_result, ulid_like};
use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_backup::{BackupWriter, Chunker, FORMAT, Manifest, RepoKey, SourceKind, layout};
use quoro_control_plane::backups::{Bucket, BucketFactory, BucketStore, RepositoryTarget};
use quoro_control_plane::deploy;
use quoro_control_plane::store::now_rfc3339;
use quoro_proto::wire::{
    self, BackupState, Capacity as WireCapacity, Health, Heartbeat, NodeMessage, ObservedStatus,
    StatusReport, VmStatus, VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_providers::object_store::{MemoryStore, ObjectInfo, ObjectStore, ObjectStoreError};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";
const PREFIX: &str = "declare-lost-test";
const DECLARE_LOST: &str = "POST /api/v1/nodes/{node_id}/declare-lost";

// ---------------------------------------------------------------------------
// The in-memory bucket
// ---------------------------------------------------------------------------

struct MemoryBuckets(Arc<MemoryStore>);

impl BucketStore for MemoryBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.0.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.list(prefix))
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.delete(key))
    }
}

impl BucketFactory for MemoryBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(MemoryBuckets(Arc::clone(&self.0))))
    }
}

// ---------------------------------------------------------------------------
// The fake nodes
// ---------------------------------------------------------------------------

/// Sends `message` with an id of its own, answering `re` when it is set.
async fn send(socket: &mut Socket, message: NodeMessage, re: Option<&str>) {
    let text = message
        .to_frame(&format!("msg_{}", ulid_like()), re)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

/// The `hello` of a node that still runs the app under `lease_id`: it lists
/// the lease `held` in `local` mode at `token`.
fn hello_holding(node_id: &str, lease_id: &str, app_id: &str, token: i64) -> NodeMessage {
    let mut hello = hello_body(node_id);
    hello.leases = vec![wire::HelloLease {
        lease_id: lease_id.to_string(),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: app_id.to_string(),
        mode: quoro_proto::LeaseMode::Local,
        fencing_token: u64::try_from(token).unwrap(),
        state: "held".to_string(),
    }];
    NodeMessage::Hello(hello)
}

async fn heartbeat(socket: &mut Socket) {
    send(
        socket,
        NodeMessage::Heartbeat(Heartbeat {
            time: "2026-11-02T10:15:32Z".to_string(),
            busy: None,
        }),
        None,
    )
    .await;
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
        warm: Vec::new(),
        volumes,
        quarantines: Vec::new(),
    })
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

/// Reads frames until one of each `wanted` type was seen, and returns them
/// in arrival order, with the frames of other types between them.
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

/// Every frame up to and including the first of type `kind`, in arrival
/// order.
async fn frames_through(socket: &mut Socket, kind: &str) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(socket).await;
        let last = frame["type"] == kind;
        frames.push(frame);
        if last {
            return frames;
        }
    }
}

/// Every frame the primary sends before the `pong`s of two `ping`s. The
/// session handles a node's messages in order, so whatever an earlier
/// message caused is among them.
async fn frames_until_settled(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    for _ in 0..2 {
        let id = format!("msg_{}", ulid_like());
        let text = NodeMessage::Ping.to_frame(&id, None).to_text();
        socket.send(WsMessage::Text(text.into())).await.unwrap();
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

/// Asserts that nothing reaches `socket` within `wait`.
async fn silent(socket: &mut Socket, wait: Duration, what: &str) {
    if let Ok(frame) = timeout(wait, socket.next()).await {
        panic!("{what}, yet a frame arrived: {frame:?}");
    }
}

fn kinds(frames: &[Value]) -> Vec<&str> {
    frames
        .iter()
        .map(|frame| frame["type"].as_str().unwrap_or_default())
        .collect()
}

/// The entry of `vm_id` in a `desired_state` frame's `spec.vms`, if any.
fn names_vm<'a>(frame: &'a Value, vm_id: &str) -> Option<&'a Value> {
    frame["body"]["spec"]["vms"]
        .as_array()?
        .iter()
        .find(|vm| vm["vm_id"] == vm_id)
}

/// Whether a `desired_state` frame lists a VM bound to `lease_id`.
fn binds_lease(frame: &Value, lease_id: &str) -> bool {
    frame["body"]["spec"]["vms"]
        .as_array()
        .is_some_and(|vms| vms.iter().any(|vm| vm["lease_id"] == lease_id))
}

// ---------------------------------------------------------------------------
// The primary's rows
// ---------------------------------------------------------------------------

/// The lease row's `(fencing_token, revoked_at, holder_node_id)`.
async fn lease_row(server: &Server, lease_id: &str) -> (i64, Option<String>, String) {
    sqlx::query_as("SELECT fencing_token, revoked_at, holder_node_id FROM leases WHERE id = ?")
        .bind(lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// The node row's `(status, lost_at, last_seen_at)`.
async fn node_row(server: &Server, node_id: &str) -> (String, Option<String>, Option<String>) {
    sqlx::query_as("SELECT status, lost_at, last_seen_at FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// The change log's newest `seq`, 0 for an empty log.
async fn log_head(server: &Server) -> i64 {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    head.unwrap_or(0)
}

/// `(entity_type, payload)` of each lease and node entry written after
/// `after`, oldest first.
async fn entries_after(server: &Server, after: i64) -> Vec<(String, Value)> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT entity_type, payload FROM change_log \
         WHERE seq > ? AND entity_type IN ('lease', 'node') ORDER BY seq",
    )
    .bind(after)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(kind, payload)| (kind, serde_json::from_str(&payload).unwrap()))
        .collect()
}

/// The `result` of each `declareNodeLost` audit record, oldest first.
async fn declare_results(server: &Server) -> Vec<String> {
    sqlx::query_scalar("SELECT result FROM audit_log WHERE operation = ? ORDER BY seq")
        .bind(DECLARE_LOST)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap()
}

/// `declareNodeLost` (section 8.3, ADR 0183).
async fn declare_lost(
    owner: &reqwest::Client,
    server: &Server,
    node_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/nodes/{node_id}/declare-lost")))
        .send()
        .await
        .unwrap()
}

/// Declares `node_id` lost once the primary's clock is past the node's last
/// report, and returns the `lost_at` it answers.
async fn declare_lost_after_its_last_report(
    owner: &reqwest::Client,
    server: &Server,
    node_id: &str,
) -> String {
    let (_, _, seen) = node_row(server, node_id).await;
    wait_past(&seen.expect("the node reported before")).await;
    let res = declare_lost(owner, server, node_id).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let node: Value = res.json().await.unwrap();
    let declared = node["lost_at"].as_str().expect("lost_at").to_string();
    assert!(
        node_row(server, node_id).await.2.as_deref() < Some(declared.as_str()),
        "declared after the node's last report"
    );
    declared
}

/// Waits until the primary's clock reads past `instant`, a time it wrote
/// with millisecond resolution, so the next time it takes is later.
async fn wait_past(instant: &str) {
    while now_rfc3339().as_str() <= instant {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Closes `socket`, the session of `node_id`, and waits until the primary
/// has no session of the node and its row reads `disconnected`.
async fn cut_off(server: &Server, socket: &mut Socket, node_id: &str) {
    socket.close(None).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while server.state.hub.is_connected(node_id)
        || node_row(server, node_id).await.0 != "disconnected"
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never ended"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits until the primary has a session of `node_id`.
async fn wait_connected(server: &Server, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !server.state.hub.is_connected(node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never started"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until the node row's `lost_at` is unset, sending heartbeats.
async fn wait_lost_at_cleared(server: &Server, socket: &mut Socket, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while node_row(server, node_id).await.1.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "lost_at was never cleared"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_volume(
    owner: &reqwest::Client,
    server: &Server,
    socket: &mut Socket,
    volume_id: &str,
    state: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let volume = get_volume(owner, server, volume_id).await;
        if volume["state"] == state {
            return volume;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "volume {volume_id} did not become {state}: {volume}"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn get_volume(owner: &reqwest::Client, server: &Server, volume_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

// ---------------------------------------------------------------------------
// A stateful app, and a restore that moves it
// ---------------------------------------------------------------------------

/// A connected node running a stateful app: a deployed instance the primary
/// sees `running`, and a `ready` volume under the `local` lease its
/// placement granted the node (section 8.3, "Granted at placement").
struct Stateful {
    server: Server,
    owner: reqwest::Client,
    node: Node,
    socket: Socket,
    app_id: String,
    volume_id: String,
    vm_id: String,
    lease_id: String,
    token: i64,
}

async fn stateful_app() -> Stateful {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enroll(&server, &owner, "node-alpha", &["node"]).await;
    let mut socket = dial(&server, &node).await;
    send(
        &mut socket,
        NodeMessage::Hello(hello_body(&node.node_id)),
        None,
    )
    .await;
    recv_type(&mut socket, "desired_state").await;
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
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
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
    recv_type(&mut socket, "desired_state").await;
    heartbeat(&mut socket).await;
    deploy::on_vm_observed(
        &server.state.store,
        &node.node_id,
        &VmStatus {
            vm_id: vm_id.clone(),
            observed_status: ObservedStatus::Running,
            health: Health::Passing,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        },
    )
    .await
    .unwrap();
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
    let rev = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .expect("the snapshot that carries the volume")["body"]["rev"]
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
        None,
    )
    .await;
    wait_for_volume(&owner, &server, &mut socket, &volume_id, "ready").await;
    let (lease_id, token): (String, i64) = sqlx::query_as(
        "SELECT id, fencing_token FROM leases WHERE resource_type = 'app' AND resource_id = ?",
    )
    .bind(&app_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    Stateful {
        server,
        owner,
        node,
        socket,
        app_id,
        volume_id,
        vm_id,
        lease_id,
        token,
    }
}

/// A stateful app, a backup repository on an in-memory bucket, and a second
/// connected node, the target of the restore.
struct Setup {
    app: Stateful,
    target: Node,
    on_target: Socket,
    memory: Arc<MemoryStore>,
    repo_id: String,
    repo_key: RepoKey,
}

async fn setup() -> Setup {
    let app = stateful_app().await;
    let memory = Arc::new(MemoryStore::new());
    app.server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let (repo_id, repo_key) = create_repository(&app.owner, &app.server).await;
    let target = enroll(&app.server, &app.owner, "node-beta", &["node"]).await;
    let mut on_target = dial(&app.server, &target).await;
    send(
        &mut on_target,
        NodeMessage::Hello(hello_body(&target.node_id)),
        None,
    )
    .await;
    recv_type(&mut on_target, "desired_state").await;
    Setup {
        app,
        target,
        on_target,
        memory,
        repo_id,
        repo_key,
    }
}

async fn create_repository(owner: &reqwest::Client, server: &Server) -> (String, RepoKey) {
    let res = owner
        .post(server.url("/api/v1/providers"))
        .json(&json!({
            "kind": "object_store",
            "name": "backups",
            "config": {
                "endpoint": "https://s3.example.test",
                "bucket": "quoro",
                "access_key_id": "AKIATEST",
                "region": "auto"
            },
            "secret": "secret-access-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let provider: Value = res.json().await.unwrap();
    let res = owner
        .post(server.url("/api/v1/backup-repositories"))
        .json(&json!({
            "name": "primary",
            "provider_id": provider["id"],
            "prefix": PREFIX,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let repository: Value = res.json().await.unwrap();
    let repo_id = repository["id"].as_str().unwrap().to_string();
    let repo_key = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT repo_key_enc FROM backup_repositories WHERE id = ?",
    )
    .bind(&repo_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let secret = server
        .state
        .data_key
        .decrypt("backup_repositories.repo_key_enc", &repo_id, &repo_key)
        .unwrap();
    (repo_id, RepoKey::from_bytes(decode_hex32(secret.expose())))
}

fn decode_hex32(text: &str) -> [u8; 32] {
    assert_eq!(text.len(), 64, "the stored repository key is 64 hex");
    let mut bytes = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let value = |byte: u8| (byte as char).to_digit(16).unwrap() as u8;
        bytes[index] = (value(pair[0]) << 4) | value(pair[1]);
    }
    bytes
}

/// Writes the chunks and the manifest of a backup of `data` to the bucket.
/// Returns the manifest's key and digest, its logical size and its number
/// of chunks.
async fn write_manifest(setup: &Setup, backup_id: &str, data: &[u8]) -> (String, String, u64, u64) {
    let header = Manifest {
        format: FORMAT,
        backup_id: backup_id.to_string(),
        cluster_id: setup.app.server.state.cluster_id.clone(),
        repository_id: setup.repo_id.clone(),
        source_kind: SourceKind::Volume,
        source_id: setup.app.app_id.clone(),
        volume_id: Some(setup.app.volume_id.clone()),
        fencing_token: Some(1),
        snapshot_method: Some("reflink".to_string()),
        logical_bytes: 0,
        chunker: Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: "test".to_string(),
        created_at: "2026-11-05T03:00:00Z".to_string(),
    };
    let root = layout::root(PREFIX, &setup.app.server.state.cluster_id);
    let mut writer = BackupWriter::new(setup.repo_key.source_keys(&setup.app.app_id), header);
    let mut objects = Vec::new();
    for chunk in quoro_backup::chunks(data) {
        let (id, object, _) = writer.push(chunk.unwrap());
        objects.push((id, object));
    }
    let (manifest, object, digest) = writer.finish().unwrap();
    for (id, chunk) in objects {
        setup
            .memory
            .put_new(&format!("{root}{}", layout::chunk(&id)), chunk, None)
            .await
            .unwrap();
    }
    let key = layout::manifest(&setup.app.app_id, backup_id);
    setup
        .memory
        .put_new(&format!("{root}{key}"), object, None)
        .await
        .unwrap();
    (
        key,
        digest,
        manifest.logical_bytes,
        manifest.chunks.len() as u64,
    )
}

/// Queues a backup of the app, reports it `succeeded` with a real manifest
/// and returns its id once the row is `succeeded`.
async fn run_backup(setup: &mut Setup, data: &[u8]) -> String {
    let res = setup
        .app
        .owner
        .post(
            setup
                .app
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let backup_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let snapshot = recv_type(&mut setup.app.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let (key, digest, logical_bytes, chunk_count) = write_manifest(setup, &backup_id, data).await;
    let NodeMessage::StatusReport(mut report) = status_report(rev, Vec::new()) else {
        unreachable!("status_report builds a status report")
    };
    report.backups.push(wire::BackupStatus {
        backup_id: backup_id.clone(),
        status: BackupState::Succeeded,
        snapshot_method: Some(wire::SnapshotMethod::Reflink),
        pause_ms: Some(0),
        guest_flush: None,
        manifest_key: Some(key),
        manifest_digest: Some(digest),
        logical_bytes: Some(logical_bytes),
        uploaded_bytes: Some(0),
        chunk_count: Some(chunk_count),
        error: None,
        started_at: None,
        finished_at: None,
    });
    send(
        &mut setup.app.socket,
        NodeMessage::StatusReport(report),
        None,
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let backup: Value = setup
            .app
            .owner
            .get(
                setup
                    .app
                    .server
                    .url(&format!("/api/v1/backups/{backup_id}")),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if backup["status"] == "succeeded" {
            return backup_id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "backup {backup_id} did not succeed: {backup}"
        );
        heartbeat(&mut setup.app.socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A restore of the app onto the target node, up to the target's snapshot
/// that carries the new volume.
struct Moving {
    volume_id: String,
    rev: u64,
}

/// Starts the restore while the old node is connected, as `restoreBackup`
/// requires (section 8.3). The old node gets the restore's `moved` revoke
/// and does not answer it.
async fn start_moving(setup: &mut Setup, byte: u8) -> Moving {
    let backup_id = run_backup(setup, &vec![byte; 64 * 1024]).await;
    let res = setup
        .app
        .owner
        .post(
            setup
                .app
                .server
                .url(&format!("/api/v1/backups/{backup_id}/restore")),
        )
        .json(&json!({ "node_id": setup.target.node_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let revoke = recv_type(&mut setup.app.socket, "lease_revoke").await;
    assert_eq!(revoke["body"]["lease_id"], setup.app.lease_id.as_str());
    assert_eq!(revoke["body"]["fencing_token"], json!(setup.app.token));
    assert_eq!(revoke["body"]["reason"], "moved");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no snapshot carried the restored volume"
        );
        let frame = recv_frame(&mut setup.on_target).await;
        let carries = frame["type"] == "desired_state"
            && frame["body"]["spec"]["volumes"]
                .as_array()
                .is_some_and(|volumes| volumes.iter().any(|v| v["volume_id"] == volume_id));
        if carries {
            return Moving {
                volume_id,
                rev: frame["body"]["rev"].as_u64().unwrap(),
            };
        }
    }
}

/// The target's report that the restored volume is `ready`.
fn restored(moving: &Moving) -> NodeMessage {
    status_report(
        moving.rev,
        vec![VolumeStatus {
            volume_id: moving.volume_id.clone(),
            state: WireVolumeState::Ready,
            allocated_bytes: Some(8192),
            error: None,
        }],
    )
}

/// The target reports the restored volume `ready` and gets nothing: the
/// volume stays `restoring`, and the lease stays revoked on the old node at
/// its token.
async fn ready_waits(setup: &mut Setup, moving: &Moving) {
    send(&mut setup.on_target, restored(moving), None).await;
    let frames = frames_until_settled(&mut setup.on_target).await;
    assert!(
        !kinds(&frames).contains(&"lease_grant"),
        "nothing grants the target the lease: {frames:?}"
    );
    let volume = get_volume(&setup.app.owner, &setup.app.server, &moving.volume_id).await;
    assert_eq!(volume["state"], "restoring", "{volume}");
    let (token, revoked_at, holder) = lease_row(&setup.app.server, &setup.app.lease_id).await;
    assert_eq!(
        (token, holder.as_str()),
        (setup.app.token, setup.app.node.node_id.as_str()),
        "the lease did not move"
    );
    assert!(revoked_at.is_some());
}

/// The target reports the restored volume `ready` and the restore finishes:
/// the target gets the lease with a token above the old one, and a desired
/// state that runs the app's VM under it. Returns the new token.
async fn ready_grants(setup: &mut Setup, moving: &Moving) -> i64 {
    send(&mut setup.on_target, restored(moving), None).await;
    wait_for_volume(
        &setup.app.owner,
        &setup.app.server,
        &mut setup.on_target,
        &moving.volume_id,
        "ready",
    )
    .await;
    let (token, revoked_at, holder) = lease_row(&setup.app.server, &setup.app.lease_id).await;
    assert_eq!(holder, setup.target.node_id);
    assert!(token > setup.app.token, "{token} > {}", setup.app.token);
    assert!(revoked_at.is_none());
    let (mut granted, mut runs) = (false, false);
    while !(granted && runs) {
        let frame = recv_frame(&mut setup.on_target).await;
        if frame["type"] == "lease_grant" {
            assert_eq!(frame["body"]["lease_id"], setup.app.lease_id.as_str());
            assert_eq!(frame["body"]["fencing_token"], json!(token));
            granted = true;
        }
        if frame["type"] == "desired_state"
            && names_vm(&frame, &setup.app.vm_id).is_some_and(|vm| {
                vm["desired_status"] == "running" && vm["fencing_token"] == json!(token)
            })
        {
            runs = true;
        }
    }
    token
}

// ---------------------------------------------------------------------------
// A declaration racing a reconnect
// ---------------------------------------------------------------------------

/// Issue #761, a declaration racing the node's reconnect, the declaration
/// first (sections 4.2 rule 3, 8.3 and 9 step 2). A restore onto another
/// node waits for the old node's answer to its `moved` revoke, and the old
/// node is cut off before it answers. It is declared lost, then dials again
/// and says `hello`, listing the lease at its token. Its session start sends
/// the `welcome`, the `declared_lost` revoke of that lease, and only then
/// the `desired_state`, which binds no VM to the lease. While the revoke is
/// unanswered the node keeps `lost_at` and nothing grants the lease
/// elsewhere: the target's `ready` report leaves the restore waiting. The
/// node's `ok` clears `lost_at` and releases the lease, so the target's next
/// report finishes the restore with a higher token, and the old node gets
/// neither a grant nor the VM back.
///
/// The two are serial here: the declaration commits before the node's new
/// session is upgraded. The interleaving in which the session is upgraded
/// after the declaration's connected check and before its commit, so that
/// the `hello`'s write waits behind the declaration's, has no point a test
/// can hold without a hook in the declaration's transaction.
#[tokio::test]
async fn a_declaration_before_the_hello_is_revoked_first_and_frees_nothing_until_answered() {
    let mut setup = setup().await;
    let moving = start_moving(&mut setup, 21).await;
    let node_id = setup.app.node.node_id.clone();
    let lease_id = setup.app.lease_id.clone();
    let token = setup.app.token;
    cut_off(&setup.app.server, &mut setup.app.socket, &node_id).await;
    // Neither connected nor declared lost: the restore waits.
    ready_waits(&mut setup, &moving).await;

    let declared =
        declare_lost_after_its_last_report(&setup.app.owner, &setup.app.server, &node_id).await;
    let (row_token, revoked_at, holder) = lease_row(&setup.app.server, &lease_id).await;
    // The declaration raises the row the restore already revoked by one,
    // as every lease of the node (ADR 0202, #948); the checks below compare
    // with the row's token.
    assert_eq!(row_token, token + 1);
    assert_eq!(holder, node_id);
    assert!(revoked_at.is_some());
    setup.app.token = row_token;

    // The node returns once the declaration has committed.
    wait_past(&declared).await;
    let mut socket = dial(&setup.app.server, &setup.app.node).await;
    send(
        &mut socket,
        hello_holding(&node_id, &lease_id, &setup.app.app_id, token),
        None,
    )
    .await;
    let frames = frames_through(&mut socket, "desired_state").await;
    assert_eq!(
        kinds(&frames),
        ["welcome", "lease_revoke", "desired_state"],
        "the revoke before anything else: {frames:?}"
    );
    let revoke = frames[1].clone();
    assert_eq!(revoke["body"]["lease_id"], lease_id.as_str());
    assert_eq!(
        revoke["body"]["fencing_token"].as_i64(),
        Some(row_token),
        "the token the declaration raised the row to: {revoke}"
    );
    assert_eq!(revoke["body"]["reason"], "declared_lost", "{revoke}");
    assert!(
        names_vm(&frames[2], &setup.app.vm_id).is_none() && !binds_lease(&frames[2], &lease_id),
        "neither the moved app's VM nor a VM under the revoked lease: {}",
        frames[2]["body"]["spec"]["vms"]
    );
    setup.app.socket = socket;
    let (status, lost_at, seen) = node_row(&setup.app.server, &node_id).await;
    assert_eq!(status, "connected");
    assert_eq!(lost_at.as_deref(), Some(declared.as_str()), "not answered");
    assert!(
        seen.as_deref() > Some(declared.as_str()),
        "the hello is recorded after the declaration: {seen:?}"
    );

    // Connected, and the revoke unanswered: nothing moves the lease.
    ready_waits(&mut setup, &moving).await;
    let frames = frames_until_settled(&mut setup.app.socket).await;
    assert!(!kinds(&frames).contains(&"lease_grant"), "{frames:?}");
    assert_eq!(
        node_row(&setup.app.server, &node_id).await.1.as_deref(),
        Some(declared.as_str())
    );

    send(
        &mut setup.app.socket,
        ok_result(None),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    wait_lost_at_cleared(&setup.app.server, &mut setup.app.socket, &node_id).await;
    ready_grants(&mut setup, &moving).await;
    let frames = frames_until_settled(&mut setup.app.socket).await;
    assert!(
        frames.iter().all(|frame| frame["type"] != "lease_grant"
            && names_vm(frame, &setup.app.vm_id).is_none()
            && !binds_lease(frame, &lease_id)),
        "nothing gives the old node the lease or the VM back: {frames:?}"
    );
}

/// Issue #761, a declaration racing the node's reconnect, the `hello` first
/// (section 8.3: refused with `409 conflict` while the node is connected,
/// since the primary then revokes over the session). The node holds the
/// `local` lease of a running app and is cut off. Twice a declaration meets
/// its new session before the database shows the node connected:
///
/// - the session is upgraded and the node has not said `hello` yet, so its
///   row still reads `disconnected`;
/// - the node said `hello`, and the session start's write waits on the
///   writer, which the test holds, as does a declaration sent after it.
///   The writer serves them in arrival order, so the `hello` commits first
///   and the declaration finds the node connected.
///
/// Both are refused and write nothing: the lease keeps its token and stays
/// unrevoked, `lost_at` stays unset, the change log gets no lease entry and
/// no node entry with `lost_at`, and each refusal is audited. The session
/// then starts as any does: no revoke, and a desired state that runs the
/// VM at the lease's token.
#[tokio::test]
async fn a_hello_before_the_declaration_refuses_it_and_nothing_is_written() {
    let mut app = stateful_app().await;
    let node_id = app.node.node_id.clone();
    cut_off(&app.server, &mut app.socket, &node_id).await;
    let before = log_head(&app.server).await;

    // Upgraded, no `hello` yet.
    let mut socket = dial(&app.server, &app.node).await;
    wait_connected(&app.server, &node_id).await;
    assert_eq!(node_row(&app.server, &node_id).await.0, "disconnected");
    let (status, code, reason) =
        problem(declare_lost(&app.owner, &app.server, &node_id).await).await;
    assert_eq!((status, code.as_str(), reason), (409, "conflict", None));

    // The `hello` and a declaration both wait on the writer.
    let gate = app.server.state.store.begin_write().await.unwrap();
    send(
        &mut socket,
        hello_holding(&node_id, &app.lease_id, &app.app_id, app.token),
        None,
    )
    .await;
    silent(
        &mut socket,
        Duration::from_millis(300),
        "the hello's write waits on the writer",
    )
    .await;
    let declaration = {
        let request = app.owner.post(
            app.server
                .url(&format!("/api/v1/nodes/{node_id}/declare-lost")),
        );
        tokio::spawn(async move { request.send().await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !declaration.is_finished(),
        "the declaration waits on the writer too"
    );
    let (status, lost_at, _) = node_row(&app.server, &node_id).await;
    assert_eq!((status.as_str(), lost_at), ("disconnected", None));
    gate.commit().await.unwrap();
    let (status, code, reason) = problem(declaration.await.unwrap()).await;
    assert_eq!((status, code.as_str(), reason), (409, "conflict", None));

    let mut frames = frames_through(&mut socket, "desired_state").await;
    assert_eq!(kinds(&frames)[0], "welcome", "{frames:?}");
    let desired = frames.last().unwrap().clone();
    let vm = names_vm(&desired, &app.vm_id).expect("the node keeps its VM");
    assert_eq!(vm["lease_id"], app.lease_id.as_str(), "{vm}");
    assert_eq!(vm["fencing_token"], json!(app.token), "{vm}");
    assert_eq!(vm["desired_status"], "running", "{vm}");
    frames.extend(frames_until_settled(&mut socket).await);
    assert!(!kinds(&frames).contains(&"lease_revoke"), "{frames:?}");

    assert_eq!(
        lease_row(&app.server, &app.lease_id).await,
        (app.token, None, node_id.clone()),
        "the lease is unchanged"
    );
    let (status, lost_at, _) = node_row(&app.server, &node_id).await;
    assert_eq!((status.as_str(), lost_at), ("connected", None));
    let entries = entries_after(&app.server, before).await;
    assert!(
        entries
            .iter()
            .all(|(kind, payload)| kind == "node" && payload["lost_at"].is_null()),
        "only the session start's node entries: {entries:?}"
    );
    assert_eq!(declare_results(&app.server).await, ["error", "error"]);
}

// ---------------------------------------------------------------------------
// A partition, end to end
// ---------------------------------------------------------------------------

/// Issue #761, an end-to-end partition (sections 4.2 rule 3, 8.3 and 9). A
/// node runs a stateful app under its `local` lease. A restore onto another
/// node starts while the node is connected, as `restoreBackup` requires, and
/// the node is cut off before it answers the restore's `moved` revoke, so as
/// far as the primary knows it still runs the app. It is declared lost, and
/// the restore finishes: the target gets the lease with a higher token and
/// runs the app's VM under it. The partition then heals, and the old node
/// returns still listing the lease `held` at its old token. On its socket
/// come, in this order, the `welcome`, the `declared_lost` revoke of the
/// lease, at a token it accepts (section 5.13), and only then the
/// `desired_state`, which names neither the moved VM nor any VM under the
/// lease. Once it answers, `lost_at` is cleared and nothing gives it the
/// lease or the VM back. The target keeps the lease and is sent nothing
/// about it.
#[tokio::test]
async fn a_partitioned_node_declared_lost_returns_to_its_revoke_and_not_the_moved_app() {
    let mut setup = setup().await;
    let node_id = setup.app.node.node_id.clone();
    let lease_id = setup.app.lease_id.clone();
    let vm_id = setup.app.vm_id.clone();
    let old_token = setup.app.token;
    let moving = start_moving(&mut setup, 22).await;
    cut_off(&setup.app.server, &mut setup.app.socket, &node_id).await;
    ready_waits(&mut setup, &moving).await;

    let declared =
        declare_lost_after_its_last_report(&setup.app.owner, &setup.app.server, &node_id).await;
    let new_token = ready_grants(&mut setup, &moving).await;
    let (vm_node,): (String,) = sqlx::query_as("SELECT node_id FROM vms WHERE id = ?")
        .bind(&vm_id)
        .fetch_one(setup.app.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(vm_node, setup.target.node_id, "the VM moved");

    let mut socket = dial(&setup.app.server, &setup.app.node).await;
    send(
        &mut socket,
        hello_holding(&node_id, &lease_id, &setup.app.app_id, old_token),
        None,
    )
    .await;
    let frames = frames_through(&mut socket, "desired_state").await;
    assert_eq!(
        kinds(&frames),
        ["welcome", "lease_revoke", "desired_state"],
        "the revoke before anything else: {frames:?}"
    );
    let revoke = &frames[1];
    assert_eq!(revoke["body"]["lease_id"], lease_id.as_str());
    assert_eq!(revoke["body"]["reason"], "declared_lost", "{revoke}");
    let revoke_token = revoke["body"]["fencing_token"].as_i64().unwrap();
    assert!(
        revoke_token >= old_token,
        "at least the token the node holds: {revoke}"
    );
    let desired = &frames[2];
    assert!(
        names_vm(desired, &vm_id).is_none() && !binds_lease(desired, &lease_id),
        "the old node is not given the moved app: {}",
        desired["body"]["spec"]["vms"]
    );
    assert_eq!(
        node_row(&setup.app.server, &node_id).await.1.as_deref(),
        Some(declared.as_str()),
        "not answered"
    );

    send(
        &mut socket,
        ok_result(None),
        Some(revoke["id"].as_str().unwrap()),
    )
    .await;
    wait_lost_at_cleared(&setup.app.server, &mut socket, &node_id).await;
    let frames = frames_until_settled(&mut socket).await;
    assert!(
        frames.iter().all(|frame| frame["type"] != "lease_grant"
            && names_vm(frame, &vm_id).is_none()
            && !binds_lease(frame, &lease_id)),
        "nothing gives the old node the lease or the VM back: {frames:?}"
    );

    assert_eq!(
        lease_row(&setup.app.server, &lease_id).await,
        (new_token, None, setup.target.node_id.clone()),
        "the target keeps the lease"
    );
    let frames = frames_until_settled(&mut setup.on_target).await;
    assert!(!kinds(&frames).contains(&"lease_revoke"), "{frames:?}");
}
