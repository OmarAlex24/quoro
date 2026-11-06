//! Volume restore onto another node, `ha_policy: cold` and `App.ha_cost`
//! (`openapi.yaml` `restoreBackup`, `AppHaCost`; `backup-format.md` section
//! 8.1; ADR 0147, ADR 0020), over real HTTP and a node WebSocket, with a
//! `MemoryStore` as the bucket.

mod common;

use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_backup::{BackupWriter, Chunker, FORMAT, Manifest, RepoKey, SourceKind, layout};
use quoro_control_plane::backups::{Bucket, BucketFactory, BucketStore, RepositoryTarget};
use quoro_control_plane::deploy;
use quoro_proto::wire::{
    self, BackupState, Capacity as WireCapacity, Health, Heartbeat, Kvm as WireKvm, NodeMessage,
    ObservedStatus, StatusReport, VmStatus, VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_providers::object_store::{MemoryStore, ObjectInfo, ObjectStore, ObjectStoreError};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
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
const PREFIX: &str = "restore-test";

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
// Setup
// ---------------------------------------------------------------------------

async fn create_project(owner: &reqwest::Client, server: &Server, slug: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": slug, "name": slug }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create project: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn create_app(
    owner: &reqwest::Client,
    server: &Server,
    project_id: &str,
    name: &str,
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
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn get_app(owner: &reqwest::Client, server: &Server, app_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "get app: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

async fn deploy_app(owner: &reqwest::Client, server: &Server, app_id: &str) -> String {
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "create deployment: {}",
        res.text().await.unwrap()
    );
    sqlx::query_as::<_, (String,)>("SELECT id FROM vms WHERE app_id = ? ORDER BY created_at DESC")
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
        .0
}

async fn create_volume(
    owner: &reqwest::Client,
    server: &Server,
    app_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap()
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
    assert_eq!(
        res.status(),
        201,
        "create provider: {}",
        res.text().await.unwrap()
    );
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
    assert_eq!(
        res.status(),
        201,
        "create repository: {}",
        res.text().await.unwrap()
    );
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

fn root(server: &Server) -> String {
    layout::root(PREFIX, &server.state.cluster_id)
}

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

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

async fn send(socket: &mut Socket, message: NodeMessage) {
    let text = message
        .to_frame("msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D", None)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
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

async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

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

async fn open(server: &Server, node: &Enrolled) -> (Socket, Value) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let welcome = recv_type(&mut socket, "welcome").await;
    assert_eq!(welcome["body"]["cluster_id"], server.state.cluster_id);
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
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

/// A connected node with an app, a running instance and a `ready` volume.
struct Stateful {
    server: Server,
    owner: reqwest::Client,
    socket: Socket,
    node: Enrolled,
    app_id: String,
    volume_id: String,
    vm_id: String,
}

async fn stateful_app() -> Stateful {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let vm_id = deploy_app(&owner, &server, &app_id).await;
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

    let res = create_volume(&owner, &server, &app_id).await;
    assert_eq!(res.status(), 201, "volume: {}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let snapshot = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .expect("the snapshot that carries the volume");
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
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
    wait_for_volume(&owner, &server, &mut socket, &volume_id, "ready").await;
    Stateful {
        server,
        owner,
        socket,
        node,
        app_id,
        volume_id,
        vm_id,
    }
}

/// A stateful app plus a repository and a second connected node.
struct Setup {
    state: Stateful,
    socket_b: Socket,
    node_b: Enrolled,
    memory: Arc<MemoryStore>,
    repo_id: String,
    repo_key: RepoKey,
}

async fn setup() -> Setup {
    setup_with_state(stateful_app().await).await
}

/// Adds a repository and a second connected node to a stateful app.
async fn setup_with_state(state: Stateful) -> Setup {
    let memory = Arc::new(MemoryStore::new());
    state
        .server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let (repo_id, repo_key) = create_repository(&state.owner, &state.server).await;
    let node_b = enrolled_node(&state.server, &state.owner, "node-beta").await;
    let (socket_b, _) = open(&state.server, &node_b).await;
    Setup {
        state,
        socket_b,
        node_b,
        memory,
        repo_id,
        repo_key,
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
        let res = owner
            .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let volume: Value = res.json().await.unwrap();
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

/// Waits for the first `desired_state` that `check` accepts.
async fn desired_state_until(socket: &mut Socket, check: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no desired_state matched"
        );
        let frame = recv_frame(socket).await;
        if frame["type"] == "desired_state" && check(&frame) {
            return frame;
        }
    }
}

/// The entry of `vm_id` in a `desired_state` frame's `spec.vms`, if any.
fn names_vm<'a>(frame: &'a Value, vm_id: &str) -> Option<&'a Value> {
    frame["body"]["spec"]["vms"]
        .as_array()?
        .iter()
        .find(|vm| vm["vm_id"] == vm_id)
}

/// A `status_report` that carries one VM's observed status.
fn vm_report(applied_rev: u64, vm_id: &str, observed_status: ObservedStatus) -> NodeMessage {
    let NodeMessage::StatusReport(mut report) = status_report(applied_rev, Vec::new()) else {
        unreachable!("status_report builds a status report")
    };
    report.vms.push(VmStatus {
        vm_id: vm_id.to_string(),
        observed_status,
        health: Health::Unknown,
        ip: None,
        restart_count: 0,
        last_exit_code: None,
    });
    NodeMessage::StatusReport(report)
}

/// Waits until the VM row's `(node_id, observed_status)` satisfies `check`.
async fn wait_for_vm_row(
    server: &Server,
    vm_id: &str,
    check: impl Fn(&(String, String)) -> bool,
) -> (String, String) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let row: (String, String) =
            sqlx::query_as("SELECT node_id, observed_status FROM vms WHERE id = ?")
                .bind(vm_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        if check(&row) {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the VM row stayed at {row:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Waits for the first `desired_state` that carries `volume_id` in
/// `spec.volumes`.
async fn desired_state_with_volume(socket: &mut Socket, volume_id: &str) -> Value {
    desired_state_until(socket, |frame| {
        frame["body"]["spec"]["volumes"]
            .as_array()
            .is_some_and(|volumes| volumes.iter().any(|v| v["volume_id"] == volume_id))
    })
    .await
}

fn manifest_header(setup: &Setup, backup_id: &str) -> Manifest {
    Manifest {
        format: FORMAT,
        backup_id: backup_id.to_string(),
        cluster_id: setup.state.server.state.cluster_id.clone(),
        repository_id: setup.repo_id.clone(),
        source_kind: SourceKind::Volume,
        source_id: setup.state.app_id.clone(),
        volume_id: Some(setup.state.volume_id.clone()),
        fencing_token: Some(1),
        snapshot_method: Some("reflink".to_string()),
        logical_bytes: 0,
        chunker: Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: "test".to_string(),
        created_at: "2026-11-05T03:00:00Z".to_string(),
    }
}

async fn write_manifest(setup: &Setup, backup_id: &str, data: &[u8]) -> (String, String, u64, u64) {
    let keys = setup.repo_key.source_keys(&setup.state.app_id);
    let header = manifest_header(setup, backup_id);
    let mut writer = BackupWriter::new(keys, header);
    let mut objects = Vec::new();
    for chunk in quoro_backup::chunks(data) {
        let (id, object, _) = writer.push(chunk.unwrap());
        objects.push((id, object));
    }
    let (manifest, object, digest) = writer.finish().unwrap();
    for (id, chunk) in objects {
        setup
            .memory
            .put_new(
                &format!("{}{}", root(&setup.state.server), layout::chunk(&id)),
                chunk,
                None,
            )
            .await
            .unwrap();
    }
    let key = layout::manifest(&setup.state.app_id, backup_id);
    setup
        .memory
        .put_new(&format!("{}{key}", root(&setup.state.server)), object, None)
        .await
        .unwrap();
    (
        key,
        digest,
        manifest.logical_bytes,
        manifest.chunks.len() as u64,
    )
}

/// Queues a backup, reports it `running` and `succeeded` with a real manifest
/// and returns the backup id once the row is `succeeded`.
async fn run_backup(setup: &mut Setup, data: &[u8]) -> String {
    let res = setup
        .state
        .owner
        .post(
            setup
                .state
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.state.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "create backup: {}",
        res.text().await.unwrap()
    );
    let backup: Value = res.json().await.unwrap();
    let backup_id = backup["id"].as_str().unwrap().to_string();

    let snapshot = recv_type(&mut setup.state.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let (key, digest, logical_bytes, chunk_count) = write_manifest(setup, &backup_id, data).await;
    send(
        &mut setup.state.socket,
        NodeMessage::StatusReport(StatusReport {
            applied_rev: rev,
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
            backups: vec![wire::BackupStatus {
                backup_id: backup_id.clone(),
                status: BackupState::Succeeded,
                snapshot_method: Some(wire::SnapshotMethod::Reflink),
                pause_ms: Some(0),
                manifest_key: Some(key),
                manifest_digest: Some(digest),
                logical_bytes: Some(logical_bytes),
                uploaded_bytes: Some(0),
                chunk_count: Some(chunk_count),
                error: None,
                started_at: None,
                finished_at: None,
            }],
            volumes: Vec::new(),
            images: Vec::new(),
        }),
    )
    .await;
    wait_for_backup(setup, &backup_id).await;
    backup_id
}

async fn wait_for_backup(setup: &mut Setup, backup_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = setup
            .state
            .owner
            .get(
                setup
                    .state
                    .server
                    .url(&format!("/api/v1/backups/{backup_id}")),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let backup: Value = res.json().await.unwrap();
        if backup["status"] == "succeeded" {
            return backup;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "backup {backup_id} did not succeed: {backup}"
        );
        heartbeat(&mut setup.state.socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn restore(
    owner: &reqwest::Client,
    server: &Server,
    backup_id: &str,
    node_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/backups/{backup_id}/restore")))
        .json(&json!({ "node_id": node_id }))
        .send()
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_restore_moves_the_app_and_the_ready_report_finishes_it() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![7u8; 300 * 1024]).await;
    let (manifest_key, manifest_digest, logical_bytes): (String, String, i64) = sqlx::query_as(
        "SELECT manifest_key, manifest_digest, logical_bytes FROM backups WHERE id = ?",
    )
    .bind(&backup_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();
    let old_token: i64 = sqlx::query_scalar(
        "SELECT fencing_token FROM leases WHERE resource_type = 'app' AND resource_id = ?",
    )
    .bind(&setup.state.app_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();

    let res = restore(
        &setup.state.owner,
        &setup.state.server,
        &backup_id,
        &setup.node_b.node_id,
    )
    .await;
    assert_eq!(res.status(), 202, "restore: {}", res.text().await.unwrap());
    let new_volume: Value = res.json().await.unwrap();
    let new_volume_id = new_volume["id"].as_str().unwrap().to_string();
    assert_eq!(new_volume["state"], "restoring");
    assert_eq!(new_volume["node_id"], setup.node_b.node_id);
    assert_eq!(new_volume["app_id"], setup.state.app_id);
    assert_eq!(new_volume["restored_from_backup_id"], backup_id);
    assert_eq!(new_volume["mount_path"], "/data");
    assert_eq!(new_volume["size_gb"], 5);

    // The old volume is detached and the app is stopped.
    let old: Value = setup
        .state
        .owner
        .get(
            setup
                .state
                .server
                .url(&format!("/api/v1/volumes/{}", setup.state.volume_id)),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(old["state"], "detached");
    let app = get_app(&setup.state.owner, &setup.state.server, &setup.state.app_id).await;
    assert_eq!(app["status"], "stopped");

    // The lease on the old node was revoked with `moved`.
    let revoked: Option<String> = sqlx::query_scalar(
        "SELECT revoked_at FROM leases WHERE resource_type = 'app' AND resource_id = ?",
    )
    .bind(&setup.state.app_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();
    assert!(revoked.is_some(), "the old lease is revoked");
    let revoke = recv_type(&mut setup.state.socket, "lease_revoke").await;
    assert_eq!(revoke["body"]["reason"], "moved");

    // The first snapshot after the revoke no longer names the app's VM, so
    // the old node deletes it. Kept in without the detached volume, it would
    // be recreated stopped (section 6.2 step 3, issue #287).
    let snapshot = recv_type(&mut setup.state.socket, "desired_state").await;
    let old_rev = snapshot["body"]["rev"].as_u64().unwrap();
    assert!(
        names_vm(&snapshot, &setup.state.vm_id).is_none(),
        "the old node's snapshot still names the moved VM: {}",
        snapshot["body"]["spec"]["vms"]
    );
    assert!(
        snapshot["body"]["spec"]["routes"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "no route points at the moved VM"
    );

    // The old node deletes it. The row stays on the old node until the
    // restore is ready and records what the node reported.
    send(
        &mut setup.state.socket,
        vm_report(old_rev, &setup.state.vm_id, ObservedStatus::Deleted),
    )
    .await;
    let (vm_node, observed) = wait_for_vm_row(&setup.state.server, &setup.state.vm_id, |row| {
        row.1 == "deleted"
    })
    .await;
    assert_eq!(vm_node, setup.state.node.node_id);
    assert_eq!(observed, "deleted");

    // The target node's snapshot carries the restore object.
    let snapshot = desired_state_with_volume(&mut setup.socket_b, &new_volume_id).await;
    let entry = snapshot["body"]["spec"]["volumes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|volume| volume["volume_id"] == new_volume_id)
        .unwrap()
        .clone();
    assert_eq!(entry["state"], "present");
    let restore = &entry["restore"];
    assert_eq!(restore["backup_id"], backup_id);
    assert_eq!(restore["manifest_key"], manifest_key);
    assert_eq!(restore["manifest_digest"], manifest_digest);
    assert_eq!(restore["logical_bytes"], logical_bytes);
    let keys = setup.repo_key.source_keys(&setup.state.app_id);
    assert_eq!(restore["id_key"], base64_of(keys.id_key.as_bytes()));
    assert_eq!(restore["data_key"], base64_of(keys.data_key.as_bytes()));
    let repository = &snapshot["body"]["spec"]["backup_repository"];
    assert_eq!(repository["repository_id"], setup.repo_id);
    assert_eq!(repository["root"], root(&setup.state.server));

    // The target node reports the volume ready.
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut setup.socket_b,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: new_volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(8192),
                error: None,
            }],
        ),
    )
    .await;
    wait_for_volume(
        &setup.state.owner,
        &setup.state.server,
        &mut setup.socket_b,
        &new_volume_id,
        "ready",
    )
    .await;

    // The new lease is on the target node with a strictly higher token.
    let (holder, token, revoked): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT holder_node_id, fencing_token, revoked_at FROM leases \
         WHERE resource_type = 'app' AND resource_id = ?",
    )
    .bind(&setup.state.app_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(holder, setup.node_b.node_id);
    assert!(token > old_token, "{token} > {old_token}");
    assert!(revoked.is_none());
    let frames = recv_until(&mut setup.socket_b, &["lease_grant", "desired_state"]).await;
    let granted = frames
        .iter()
        .find(|frame| frame["type"] == "lease_grant")
        .expect("the target node receives the grant");
    assert_eq!(granted["body"]["fencing_token"], json!(token));

    // The target node's snapshot names the VM, running, with the new volume
    // and the new token.
    let vm_id = setup.state.vm_id.clone();
    let runs_there = |frame: &Value| {
        frame["type"] == "desired_state"
            && names_vm(frame, &vm_id).is_some_and(|vm| vm["desired_status"] == "running")
    };
    let target_snapshot = match frames.iter().find(|frame| runs_there(frame)) {
        Some(frame) => frame.clone(),
        None => desired_state_until(&mut setup.socket_b, &runs_there).await,
    };
    let moved = names_vm(&target_snapshot, &vm_id).unwrap();
    assert_eq!(moved["stateful"], true);
    assert_eq!(moved["fencing_token"], json!(token));
    assert_eq!(moved["volumes"][0]["volume_id"], new_volume_id);

    // Nothing gives the VM back to the old node.
    heartbeat(&mut setup.state.socket).await;
    while let Ok(Some(Ok(message))) =
        timeout(Duration::from_millis(500), setup.state.socket.next()).await
    {
        if let WsMessage::Text(text) = message {
            let frame: Value = serde_json::from_str(text.as_str()).unwrap();
            if frame["type"] == "desired_state" {
                assert!(
                    names_vm(&frame, &setup.state.vm_id).is_none(),
                    "the old node got the moved VM back"
                );
            }
        }
    }

    // Placement is pinned to the target node and the app runs there.
    let app = get_app(&setup.state.owner, &setup.state.server, &setup.state.app_id).await;
    assert_eq!(app["placement"]["node_ids"], json!([setup.node_b.node_id]));
    assert_eq!(app["stateful"], true);
    let desired: String = sqlx::query_scalar("SELECT desired_status FROM vms WHERE id = ?")
        .bind(&setup.state.vm_id)
        .fetch_one(setup.state.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(desired, "running");
    let (vm_node, observed): (String, String) =
        sqlx::query_as("SELECT node_id, observed_status FROM vms WHERE id = ?")
            .bind(&setup.state.vm_id)
            .fetch_one(setup.state.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(vm_node, setup.node_b.node_id);
    assert_eq!(observed, "creating");
    let ready = setup
        .state
        .owner
        .get(
            setup
                .state
                .server
                .url(&format!("/api/v1/volumes/{new_volume_id}")),
        )
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(ready["allocated_bytes"], 8192);

    // One `restore.succeeded`, naming the new volume and the new token.
    let data: String =
        sqlx::query_scalar("SELECT data FROM events WHERE type = 'restore.succeeded'")
            .fetch_one(setup.state.server.state.store.pool())
            .await
            .unwrap();
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(data["backup_id"], backup_id);
    assert_eq!(data["app_id"], setup.state.app_id);
    assert_eq!(data["volume_id"], new_volume_id);
    assert_eq!(data["node_id"], setup.node_b.node_id);
    assert_eq!(data["fencing_token"], token);
    let grants: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE operation = 'lease.grant' AND target_id = ?",
    )
    .bind(&setup.state.app_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(grants, 1);
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn a_failed_restore_leaves_the_app_stopped_without_a_lease() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![3u8; 128 * 1024]).await;
    let res = restore(
        &setup.state.owner,
        &setup.state.server,
        &backup_id,
        &setup.node_b.node_id,
    )
    .await;
    assert_eq!(res.status(), 202);
    let new_volume: Value = res.json().await.unwrap();
    let new_volume_id = new_volume["id"].as_str().unwrap().to_string();
    // While the restore runs, the app's VM has left its old node.
    recv_type(&mut setup.state.socket, "lease_revoke").await;
    let during = recv_type(&mut setup.state.socket, "desired_state").await;
    assert!(names_vm(&during, &setup.state.vm_id).is_none());
    let snapshot = desired_state_with_volume(&mut setup.socket_b, &new_volume_id).await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut setup.socket_b,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: new_volume_id.clone(),
                state: WireVolumeState::Failed,
                allocated_bytes: None,
                error: Some("the manifest does not verify".to_string()),
            }],
        ),
    )
    .await;
    let volume = wait_for_volume(
        &setup.state.owner,
        &setup.state.server,
        &mut setup.socket_b,
        &new_volume_id,
        "failed",
    )
    .await;
    assert_eq!(volume["error"], "the manifest does not verify");

    let app = get_app(&setup.state.owner, &setup.state.server, &setup.state.app_id).await;
    assert_eq!(app["status"], "stopped");
    let unrevoked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM leases WHERE resource_type = 'app' AND resource_id = ? \
         AND revoked_at IS NULL",
    )
    .bind(&setup.state.app_id)
    .fetch_one(setup.state.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(unrevoked, 0, "the app has no lease");

    // The app stays on its old node: that node gets its VM back at once,
    // stopped and without a volume.
    let vm_id = setup.state.vm_id.clone();
    let after = desired_state_until(&mut setup.state.socket, |frame| {
        names_vm(frame, &vm_id).is_some()
    })
    .await;
    let back = names_vm(&after, &vm_id).unwrap();
    assert_eq!(back["desired_status"], "stopped");
    assert_eq!(back["volumes"], json!([]));

    let data: String = sqlx::query_scalar("SELECT data FROM events WHERE type = 'restore.failed'")
        .fetch_one(setup.state.server.state.store.pool())
        .await
        .unwrap();
    let data: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(data["volume_id"], new_volume_id);
    assert_eq!(data["node_id"], setup.node_b.node_id);
    assert_eq!(data["error"], "the manifest does not verify");
}

#[tokio::test]
async fn a_restore_refuses_backups_that_cannot_be_used() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![9u8; 64 * 1024]).await;
    let pool = setup.state.server.state.store.pool();

    // A pruned backup cannot be restored.
    sqlx::query("UPDATE backups SET pruned_at = ? WHERE id = ?")
        .bind(quoro_control_plane::store::now_rfc3339())
        .bind(&backup_id)
        .execute(pool)
        .await
        .unwrap();
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &backup_id,
            &setup.node_b.node_id,
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // A backup that did not succeed.
    let res = setup
        .state
        .owner
        .post(
            setup
                .state
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.state.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "create backup: {}",
        res.text().await.unwrap()
    );
    let queued_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &queued_id,
            &setup.node_b.node_id,
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // A control-plane backup is restored offline.
    let now = quoro_control_plane::store::now_rfc3339();
    let control_id = format!(
        "bkp_{}",
        &quoro_types::new_id(quoro_types::Prefix::Request)[4..]
    );
    sqlx::query(
        "INSERT INTO backups (id, repository_id, source_kind, app_id, volume_id, node_id, status, \
         \"trigger\", fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes, \
         chunk_count, manifest_key, manifest_digest, error, created_by_type, created_by_id, \
         created_at, started_at, finished_at, pruned_at) \
         VALUES (?, ?, 'control_plane', NULL, NULL, ?, 'succeeded', 'api', NULL, NULL, NULL, 0, 0, \
         0, 'manifests/cluster/x.json', ?, NULL, 'system', 'backups', ?, NULL, NULL, NULL)",
    )
    .bind(&control_id)
    .bind(&setup.repo_id)
    .bind(&setup.state.node.node_id)
    .bind("a".repeat(64))
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &control_id,
            &setup.node_b.node_id,
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // An unknown node is a validation failure.
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &backup_id,
            "nod_00000000000000000000000000",
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn a_restore_is_refused_while_another_runs_or_the_old_node_is_gone() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![5u8; 64 * 1024]).await;
    let res = restore(
        &setup.state.owner,
        &setup.state.server,
        &backup_id,
        &setup.node_b.node_id,
    )
    .await;
    assert_eq!(res.status(), 202, "restore: {}", res.text().await.unwrap());
    // A second restore waits for the first one.
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &backup_id,
            &setup.node_b.node_id,
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // Restoring while the volume's node is disconnected is refused.
    let mut setup = setup_with_state(stateful_app().await).await;
    let backup_id = run_backup(&mut setup, &vec![6u8; 64 * 1024]).await;
    setup.state.socket.close(None).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while setup
        .state
        .server
        .state
        .hub
        .is_connected(&setup.state.node.node_id)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session did not drop"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (status, code, _) = problem(
        restore(
            &setup.state.owner,
            &setup.state.server,
            &backup_id,
            &setup.node_b.node_id,
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

// ---------------------------------------------------------------------------
// Approval
// ---------------------------------------------------------------------------

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

#[tokio::test]
async fn an_agent_needs_an_approval_to_restore() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![4u8; 64 * 1024]).await;
    let owner_user_id = owner_user_id(&setup.state.owner, &setup.state.server).await;
    let agent_id = create_agent(
        &setup.state.owner,
        &setup.state.server,
        &owner_user_id,
        "restorer",
    )
    .await;
    let res = setup
        .state
        .owner
        .post(setup.state.server.url("/api/v1/grants"))
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
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
    let token = agent_token(&setup.state.owner, &setup.state.server, &agent_id).await;
    let call = || {
        reqwest::Client::new()
            .post(
                setup
                    .state
                    .server
                    .url(&format!("/api/v1/backups/{backup_id}/restore")),
            )
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "restore the app's data")
            .json(&json!({ "node_id": setup.node_b.node_id }))
            .send()
    };

    let (status, code, _) = problem(call().await.unwrap()).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));

    // The agent files a request naming the backup and the target node.
    let params = json!({ "node_id": setup.node_b.node_id });
    let res = reqwest::Client::new()
        .post(setup.state.server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "scope": "backup:restore",
            "target": { "type": "backup", "id": backup_id },
            "operation": "POST /api/v1/backups/{backup_id}/restore",
            "params": params,
            "justification": "restore the app's data",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "file approval: {}",
        res.text().await.unwrap()
    );
    let approval: Value = res.json().await.unwrap();
    let approval_id = approval["id"].as_str().unwrap().to_string();
    assert_eq!(approval["target"]["type"], "backup");
    assert_eq!(approval["target"]["id"], backup_id);

    // A human approves, which creates the one-use approval grant.
    let res = setup
        .state
        .owner
        .post(
            setup
                .state
                .server
                .url(&format!("/api/v1/approvals/{approval_id}/approve")),
        )
        .json(&json!({ "note": "approved for the test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());
    let grant: Option<(String,)> = sqlx::query_as(
        "SELECT approval_id FROM grants WHERE agent_id = ? AND kind = 'approval' \
         AND status = 'active'",
    )
    .bind(&agent_id)
    .fetch_optional(setup.state.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(
        grant.map(|row| row.0).as_deref(),
        Some(approval_id.as_str())
    );

    // The same call with the same params now runs.
    let res = call().await.unwrap();
    assert_eq!(res.status(), 202, "restore: {}", res.text().await.unwrap());
}

#[tokio::test]
async fn backup_read_follows_the_app_of_the_backup() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![2u8; 32 * 1024]).await;
    let owner_user_id = owner_user_id(&setup.state.owner, &setup.state.server).await;

    // Another app in the same project, for the negative selector.
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM apps WHERE id = ?")
        .bind(&setup.state.app_id)
        .fetch_one(setup.state.server.state.store.pool())
        .await
        .unwrap();
    let other_app = create_app(
        &setup.state.owner,
        &setup.state.server,
        &project_id,
        "other",
    )
    .await;

    let agent_id = create_agent(
        &setup.state.owner,
        &setup.state.server,
        &owner_user_id,
        "restorer",
    )
    .await;
    let res = setup
        .state
        .owner
        .post(setup.state.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": ["backup:read"],
            "selector": { "apps": [setup.state.app_id] },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
    let token = agent_token(&setup.state.owner, &setup.state.server, &agent_id).await;
    let get = |token: String| {
        let url = setup
            .state
            .server
            .url(&format!("/api/v1/backups/{backup_id}"));
        async move {
            reqwest::Client::new()
                .get(url)
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap()
        }
    };

    // The backup belongs to the granted app.
    let res = get(token.clone()).await;
    assert_eq!(
        res.status(),
        200,
        "get backup: {}",
        res.text().await.unwrap()
    );
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["id"], backup_id);
    assert_eq!(body["app_id"], setup.state.app_id);

    // A grant that names only another app sees nothing.
    let other_agent = create_agent(
        &setup.state.owner,
        &setup.state.server,
        &owner_user_id,
        "other-reader",
    )
    .await;
    let res = setup
        .state
        .owner
        .post(setup.state.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": other_agent,
            "scopes": ["backup:read"],
            "selector": { "apps": [other_app] },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
    let other_token = agent_token(&setup.state.owner, &setup.state.server, &other_agent).await;
    let (status, code, _) = problem(get(other_token).await).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

// ---------------------------------------------------------------------------
// `ha_policy: cold` and `ha_cost`
// ---------------------------------------------------------------------------

async fn patch_app(
    owner: &reqwest::Client,
    server: &Server,
    app_id: &str,
    body: Value,
) -> reqwest::Response {
    owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn cold_needs_a_stateful_app_and_a_repository() {
    let setup = setup().await;
    let res = patch_app(
        &setup.state.owner,
        &setup.state.server,
        &setup.state.app_id,
        json!({ "ha_policy": "cold" }),
    )
    .await;
    assert_eq!(res.status(), 200, "cold: {}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    assert_eq!(app["ha_policy"], "cold");

    let (status, code, _) = problem(
        patch_app(
            &setup.state.owner,
            &setup.state.server,
            &setup.state.app_id,
            json!({ "ha_policy": "warm" }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // A stateless app cannot be `cold`.
    let project_id = create_project(&setup.state.owner, &setup.state.server, "other").await;
    let stateless = create_app(
        &setup.state.owner,
        &setup.state.server,
        &project_id,
        "stateless",
    )
    .await;
    let (status, code, _) = problem(
        patch_app(
            &setup.state.owner,
            &setup.state.server,
            &stateless,
            json!({ "ha_policy": "cold" }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // A stateful app without a repository cannot be `cold`.
    let no_repo = stateful_app().await;
    let (status, code, _) = problem(
        patch_app(
            &no_repo.owner,
            &no_repo.server,
            &no_repo.app_id,
            json!({ "ha_policy": "cold" }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn ha_cost_prices_every_policy() {
    let mut setup = setup().await;
    let app_id = setup.state.app_id.clone();
    let mut app = get_app(&setup.state.owner, &setup.state.server, &app_id).await;
    let cost = app["ha_cost"].clone();
    assert_eq!(cost["snapshot_method"], "reflink");
    let policies = cost["policies"].as_array().unwrap();
    assert_eq!(policies.len(), 4);
    let order: Vec<&str> = policies
        .iter()
        .map(|policy| policy["policy"].as_str().unwrap())
        .collect();
    assert_eq!(order, ["none", "cold", "warm", "hot"]);
    assert_eq!(policies[0]["storage_bytes"], 0);
    assert_eq!(
        policies[0]["reserved"],
        json!({ "vcpus": 0, "memory_mb": 0, "disk_gb": 0 })
    );
    assert!(policies[0]["restore_estimate_s"].is_null());
    assert!(policies[0]["pause_estimate_ms"].is_null());
    // Before the first backup the volume's allocated bytes stand in.
    assert_eq!(policies[1]["available"], true);
    assert_eq!(policies[1]["storage_bytes"], 4096);
    assert!(policies[1]["restore_estimate_s"].is_null());
    assert_eq!(policies[1]["pause_estimate_ms"], 0);
    assert_eq!(policies[2]["available"], false);
    assert_eq!(policies[2]["unavailable_reason"], "not_in_this_release");
    assert_eq!(
        policies[2]["reserved"],
        json!({ "vcpus": 1, "memory_mb": 256, "disk_gb": 1 })
    );
    assert!(policies[2]["storage_bytes"].is_null());
    assert_eq!(policies[3]["unavailable_reason"], "not_in_this_release");

    // A `pause_copy` node prices the pause a backup measured.
    let backup_id = run_backup(&mut setup, &vec![8u8; 300 * 1024]).await;
    let pool = setup.state.server.state.store.pool();
    sqlx::query("UPDATE backups SET uploaded_bytes = 1200, pause_ms = 137 WHERE id = ?")
        .bind(&backup_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE nodes SET volume_snapshot = 'pause_copy' WHERE id = ?")
        .bind(&setup.state.node.node_id)
        .execute(pool)
        .await
        .unwrap();
    app = get_app(&setup.state.owner, &setup.state.server, &app_id).await;
    let cost = app["ha_cost"].clone();
    assert_eq!(cost["snapshot_method"], "pause_copy");
    assert_eq!(cost["policies"][1]["pause_estimate_ms"], 137);
    assert_eq!(cost["policies"][1]["storage_bytes"], 1200);
    let logical_bytes: i64 = sqlx::query_scalar("SELECT logical_bytes FROM backups WHERE id = ?")
        .bind(&backup_id)
        .fetch_one(pool)
        .await
        .unwrap();
    let rate = 50 * 1024 * 1024;
    assert_eq!(
        cost["policies"][1]["restore_estimate_s"],
        (logical_bytes + rate - 1) / rate
    );

    // `storage_bytes` adds up every kept backup.
    let now = quoro_control_plane::store::now_rfc3339();
    let second = format!(
        "bkp_{}",
        &quoro_types::new_id(quoro_types::Prefix::Request)[4..]
    );
    sqlx::query(
        "INSERT INTO backups (id, repository_id, source_kind, app_id, volume_id, node_id, status, \
         \"trigger\", fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes, \
         chunk_count, manifest_key, manifest_digest, error, created_by_type, created_by_id, \
         created_at, started_at, finished_at, pruned_at) \
         VALUES (?, ?, 'volume', ?, ?, ?, 'succeeded', 'api', 1, 'reflink', 0, 2048, 800, 1, \
         'manifests/x.json', ?, NULL, 'user', 'usr_x', ?, NULL, NULL, NULL)",
    )
    .bind(&second)
    .bind(&setup.repo_id)
    .bind(&app_id)
    .bind(&setup.state.volume_id)
    .bind(&setup.state.node.node_id)
    .bind("b".repeat(64))
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    app = get_app(&setup.state.owner, &setup.state.server, &app_id).await;
    assert_eq!(app["ha_cost"]["policies"][1]["storage_bytes"], 2000);

    // Switching to `cold` changes nothing in the prices themselves.
    let before = app["ha_cost"].clone();
    let res = patch_app(
        &setup.state.owner,
        &setup.state.server,
        &app_id,
        json!({ "ha_policy": "cold" }),
    )
    .await;
    assert_eq!(res.status(), 200, "cold: {}", res.text().await.unwrap());
    app = get_app(&setup.state.owner, &setup.state.server, &app_id).await;
    assert_eq!(app["ha_policy"], "cold");
    assert_eq!(app["ha_cost"]["policies"], before["policies"]);
    assert_eq!(app["ha_cost"]["snapshot_method"], before["snapshot_method"]);

    // A stateless app still gets a cost, with `cold` unavailable.
    let project_id = create_project(&setup.state.owner, &setup.state.server, "plain").await;
    let stateless = create_app(
        &setup.state.owner,
        &setup.state.server,
        &project_id,
        "plain",
    )
    .await;
    let app = get_app(&setup.state.owner, &setup.state.server, &stateless).await;
    let cost = app["ha_cost"].clone();
    assert!(cost["snapshot_method"].is_null());
    assert_eq!(cost["policies"][0]["available"], true);
    assert_eq!(cost["policies"][1]["available"], false);
    assert_eq!(cost["policies"][1]["unavailable_reason"], "stateless");
    assert_eq!(cost["policies"][1]["storage_bytes"], 0);
    assert!(cost["policies"][1]["restore_estimate_s"].is_null());
    assert!(cost["policies"][1]["pause_estimate_ms"].is_null());
}

#[tokio::test]
async fn deleting_the_last_volume_makes_a_cold_app_none() {
    let mut setup = setup().await;
    let app_id = setup.state.app_id.clone();
    let volume_id = setup.state.volume_id.clone();
    let res = patch_app(
        &setup.state.owner,
        &setup.state.server,
        &app_id,
        json!({ "ha_policy": "cold" }),
    )
    .await;
    assert_eq!(res.status(), 200, "cold: {}", res.text().await.unwrap());

    let res = setup
        .state
        .owner
        .post(
            setup
                .state
                .server
                .url(&format!("/api/v1/apps/{app_id}/stop")),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "stop: {}", res.text().await.unwrap());
    let res = setup
        .state
        .owner
        .delete(
            setup
                .state
                .server
                .url(&format!("/api/v1/volumes/{volume_id}")),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());

    // The node confirms the file is gone, which makes the app stateless.
    let snapshot = recv_type(&mut setup.state.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut setup.state.socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Deleted,
                allocated_bytes: None,
                error: None,
            }],
        ),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let app = get_app(&setup.state.owner, &setup.state.server, &app_id).await;
        if app["stateful"] == false {
            assert_eq!(app["ha_policy"], "none");
            assert_eq!(app["stateful"], false);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the app stayed stateful: {app}"
        );
        heartbeat(&mut setup.state.socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
