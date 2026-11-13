//! Backup repositories, volume backups, seals, retention and garbage
//! collection (`backup-format.md`, ADR 0147, issue #233), over real HTTP and a
//! node WebSocket, with a `MemoryStore` as the bucket.

mod common;

use common::replay;
use common::{Server, problem, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_backup::{
    BackupWriter, ChunkId, Chunker, FORMAT, Manifest, RecoveryKey, RepoConfig, RepoKey, SourceKind,
    layout, unwrap_repo_key, wrap_repo_key,
};
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";
const PREFIX: &str = "backups-test";

// ---------------------------------------------------------------------------
// The in-memory bucket
// ---------------------------------------------------------------------------

/// A `BucketFactory` over one `MemoryStore`.
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

/// A factory whose bucket refuses the seal put only, so the node's chunks and
/// manifest land and the seal alone fails (`backup-format.md` section 7.1,
/// step 5).
struct SealFailBuckets(Arc<MemoryStore>);

impl BucketStore for SealFailBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move {
            if key.contains("/seals/") {
                return Err(ObjectStoreError::Transport(
                    "the seal put is down".to_string(),
                ));
            }
            self.0.put_new(key, body, None).await.map(|_| ())
        })
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

impl BucketFactory for SealFailBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(SealFailBuckets(Arc::clone(&self.0))))
    }
}

/// A store that reports the keys named in `young` as written now, so the
/// 48-hour grace of the sweep can be tested on a store without a clock.
struct AgedBuckets {
    memory: Arc<MemoryStore>,
    young: Vec<String>,
}

impl BucketStore for AgedBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.memory.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.memory.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        let young = self.young.clone();
        Box::pin(async move {
            let mut objects = self.memory.list(prefix).await?;
            let now = quoro_control_plane::store::now_rfc3339();
            for object in &mut objects {
                if young.contains(&object.key) {
                    object.last_modified.clone_from(&now);
                }
            }
            Ok(objects)
        })
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.memory.delete(key))
    }
}

impl BucketFactory for AgedBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(AgedBuckets {
            memory: Arc::clone(&self.memory),
            young: self.young.clone(),
        }))
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

/// Creates an `object_store` provider and the cluster's repository. Returns
/// the repository id, the recovery key text and the repository key.
async fn create_repository(owner: &reqwest::Client, server: &Server) -> (String, String, RepoKey) {
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
    let recovery = repository["recovery_key"].as_str().unwrap().to_string();
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
    (
        repo_id,
        recovery,
        RepoKey::from_bytes(decode_hex32(secret.expose())),
    )
}

fn decode_hex32(text: &str) -> [u8; 32] {
    assert_eq!(text.len(), 64, "the stored repository key is 64 hex");
    let mut bytes = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let high = (pair[0] as char).to_digit(16).unwrap() as u8;
        let low = (pair[1] as char).to_digit(16).unwrap() as u8;
        bytes[index] = (high << 4) | low;
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
        custody_public_key: None,
        mesh_rules: None,
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

fn status_report(
    applied_rev: u64,
    volumes: Vec<VolumeStatus>,
    backups: Vec<wire::BackupStatus>,
) -> NodeMessage {
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
        backups,
        volumes,
        images: Vec::new(),
        warm: Vec::new(),
        quarantines: Vec::new(),
    })
}

async fn wait_for_backup(
    owner: &reqwest::Client,
    server: &Server,
    socket: &mut Socket,
    backup_id: &str,
    status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/backups/{backup_id}")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let backup: Value = res.json().await.unwrap();
        if backup["status"] == status {
            return backup;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "backup {backup_id} did not become {status}: {backup}"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A control plane with a live node, an app, a ready volume and a repository.
struct Setup {
    server: Server,
    owner: reqwest::Client,
    socket: Socket,
    memory: Arc<MemoryStore>,
    app_id: String,
    volume_id: String,
    repo_id: String,
    repo_key: RepoKey,
    /// The recovery key `createBackupRepository` answered once.
    recovery: String,
}

async fn setup() -> Setup {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let vm_id = deploy_app(&owner, &server, &app_id).await;
    let deployment = recv_type(&mut socket, "desired_state").await;
    heartbeat(&mut socket).await;
    deploy::on_vm_observed(
        &server.state.store,
        &node.node_id,
        &VmStatus {
            vm_id,
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
        .unwrap();
    let rev = snapshot["body"]["rev"]
        .as_u64()
        .unwrap_or_else(|| deployment["body"]["rev"].as_u64().unwrap());
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
            Vec::new(),
        ),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap();
        let volume: Value = res.json().await.unwrap();
        if volume["state"] == "ready" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "volume not ready");
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let (repo_id, recovery, repo_key) = create_repository(&owner, &server).await;
    Setup {
        server,
        owner,
        socket,
        memory,
        app_id,
        volume_id,
        repo_id,
        repo_key,
        recovery,
    }
}

fn manifest_header(setup: &Setup, backup_id: &str, cluster_id: &str, created_at: &str) -> Manifest {
    Manifest {
        format: FORMAT,
        backup_id: backup_id.to_string(),
        cluster_id: cluster_id.to_string(),
        repository_id: setup.repo_id.clone(),
        source_kind: SourceKind::Volume,
        source_id: setup.app_id.clone(),
        volume_id: Some(setup.volume_id.clone()),
        fencing_token: Some(1),
        snapshot_method: Some("reflink".to_string()),
        logical_bytes: 0,
        chunker: Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: "test".to_string(),
        created_at: created_at.to_string(),
    }
}

/// Writes a real encrypted manifest for `backup_id` into the store and returns
/// its key, digest, logical size and chunk count.
async fn write_manifest(setup: &Setup, backup_id: &str, data: &[u8]) -> (String, String, u64, u64) {
    let keys = setup.repo_key.source_keys(&setup.app_id);
    let header = manifest_header(
        setup,
        backup_id,
        &setup.server.state.cluster_id,
        "2026-11-05T03:00:00Z",
    );
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
                &format!("{}{}", root(&setup.server), layout::chunk(&id)),
                chunk,
                None,
            )
            .await
            .unwrap();
    }
    let key = layout::manifest(&setup.app_id, backup_id);
    setup
        .memory
        .put_new(&format!("{}{key}", root(&setup.server)), object, None)
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
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
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

    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let (key, digest, logical_bytes, chunk_count) = write_manifest(setup, &backup_id, data).await;
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
                backup_id: backup_id.clone(),
                status: BackupState::Running,
                snapshot_method: Some(wire::SnapshotMethod::Reflink),
                pause_ms: Some(0),
                guest_flush: None,
                manifest_key: None,
                manifest_digest: None,
                logical_bytes: None,
                uploaded_bytes: None,
                chunk_count: None,
                error: None,
                started_at: Some(quoro_control_plane::store::now_rfc3339()),
                finished_at: None,
            }],
        ),
    )
    .await;
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
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
                started_at: Some(quoro_control_plane::store::now_rfc3339()),
                finished_at: Some(quoro_control_plane::store::now_rfc3339()),
            }],
        ),
    )
    .await;
    wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &backup_id,
        "succeeded",
    )
    .await;
    backup_id
}

// ---------------------------------------------------------------------------
// Repository
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_recovery_key_is_shown_once_and_opens_the_config_object() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    let (repo_id, recovery_text, repo_key) = create_repository(&owner, &server).await;

    assert!(repo_id.starts_with("brp_"));
    assert!(recovery_text.starts_with("qrk1-"), "{recovery_text}");
    assert_eq!(recovery_text.len(), 5 + 52 + 12);
    let fingerprint: String =
        sqlx::query_scalar("SELECT recovery_key_fingerprint FROM backup_repositories WHERE id = ?")
            .bind(&repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(fingerprint.len(), 16);

    // The config object holds the repository key wrapped under the recovery
    // key, and the same key opens it.
    let config_key = format!("{}config-1.json", root(&server));
    let bytes = memory.get(&config_key).await.unwrap();
    let config: RepoConfig = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(config.repository_id, repo_id);
    assert_eq!(config.cluster_id, server.state.cluster_id);
    assert_eq!(config.recovery_key_fingerprint, fingerprint);
    assert_eq!(
        config.cluster_public_key,
        server.state.keys.public_key_base64()
    );
    let recovery = RecoveryKey::parse(&recovery_text).unwrap();
    let opened = unwrap_repo_key(&config.wrapped_repo_key, &recovery, &repo_id).unwrap();
    assert_eq!(opened.as_bytes(), repo_key.as_bytes());
    // Both derivations of a source key agree.
    assert_eq!(
        opened.source_keys("app_x").id_key.as_bytes(),
        repo_key.source_keys("app_x").id_key.as_bytes()
    );

    // One repository per cluster.
    let provider_id: String =
        sqlx::query_scalar("SELECT provider_id FROM backup_repositories WHERE id = ?")
            .bind(&repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/backup-repositories"))
            .json(&json!({ "name": "second", "provider_id": provider_id }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // The key appears in no change-log row, audit row or event.
    let repo_hex: String = repo_key
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let payloads: Vec<String> = sqlx::query_scalar("SELECT payload FROM change_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    let mut texts = payloads;
    let audits: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT params, result_detail FROM audit_log")
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    texts.extend(
        audits
            .into_iter()
            .map(|(params, detail)| format!("{params}{detail:?}")),
    );
    let events: Vec<String> = sqlx::query_scalar("SELECT data FROM events")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    texts.extend(events);
    for text in texts {
        assert!(
            !text.contains(&recovery_text),
            "the recovery key leaked: {text}"
        );
        assert!(
            !text.contains(&repo_hex),
            "the repository key leaked: {text}"
        );
    }

    // GET never returns a key either.
    let res = owner
        .get(server.url(&format!("/api/v1/backup-repositories/{repo_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body.get("recovery_key").is_none());
    assert_eq!(body["recovery_key_fingerprint"], fingerprint);

    // An agent is refused with `human_only`, before the one-repository rule.
    let owner_user_id: String = owner
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
            "name": "backup-admin",
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
    let token = token["access_token"].as_str().unwrap().to_string();
    let (status, code, reason) = problem(
        reqwest::Client::new()
            .post(server.url("/api/v1/backup-repositories"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "try to manage backups")
            .json(&json!({ "name": "agent", "provider_id": provider_id }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
}

#[tokio::test]
async fn rotating_the_recovery_key_writes_config_2() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    let (repo_id, old_text, _repo_key) = create_repository(&owner, &server).await;
    let old_fingerprint: String =
        sqlx::query_scalar("SELECT recovery_key_fingerprint FROM backup_repositories WHERE id = ?")
            .bind(&repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();

    let res = owner
        .post(server.url(&format!(
            "/api/v1/backup-repositories/{repo_id}/recovery-key"
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "rotate: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    let new_text = body["recovery_key"].as_str().unwrap().to_string();
    assert_ne!(new_text, old_text);
    assert_ne!(
        body["recovery_key_fingerprint"].as_str().unwrap(),
        old_fingerprint
    );

    let config_key = format!("{}config-2.json", root(&server));
    let bytes = memory.get(&config_key).await.unwrap();
    let config: RepoConfig = serde_json::from_slice(&bytes).unwrap();
    let new_key = RecoveryKey::parse(&new_text).unwrap();
    let opened = unwrap_repo_key(&config.wrapped_repo_key, &new_key, &repo_id).unwrap();
    assert_eq!(
        opened.as_bytes(),
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT repo_key_enc FROM backup_repositories WHERE id = ?"
        )
        .bind(&repo_id)
        .fetch_one(server.state.store.pool())
        .await
        .map(|blob| {
            let secret = server
                .state
                .data_key
                .decrypt("backup_repositories.repo_key_enc", &repo_id, &blob)
                .unwrap();
            decode_hex32(secret.expose())
        })
        .unwrap()
        .as_slice()
    );
    let old_key = RecoveryKey::parse(&old_text).unwrap();
    assert!(unwrap_repo_key(&config.wrapped_repo_key, &old_key, &repo_id).is_err());
}

/// Section 7.3 step 4 (ADR 0297): the change-log entry of a provider created
/// over the API rebuilds its row when replayed into an empty database, its
/// settings included as `config_json`. The backup repository the provider
/// serves is not replayed here: `repo_key_enc` stays out of the log, and a
/// promoted primary gets the repository key back from the recovery key
/// (ADR 0322).
#[tokio::test]
async fn a_provider_entry_rebuilds_its_row() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    create_repository(&owner, &server).await;

    let rebuilt = replay::rebuild(&server, &["provider"]).await;
    replay::assert_rebuilt(
        &server,
        &rebuilt,
        "providers",
        &[
            "id",
            "kind",
            "name",
            "config_json",
            "secret_enc",
            "created_by",
            "created_at",
            "rotated_at",
        ],
        1,
    )
    .await;
}

// ---------------------------------------------------------------------------
// A backup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_volume_backup_reaches_the_node_and_the_primary_seals_it() {
    let mut setup = setup().await;
    let res = setup
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "create: {}", res.text().await.unwrap());
    let backup: Value = res.json().await.unwrap();
    let backup_id = backup["id"].as_str().unwrap().to_string();
    assert_eq!(backup["status"], "queued");
    assert_eq!(backup["trigger"], "api");
    assert_eq!(backup["source_kind"], "volume");

    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let spec = &snapshot["body"]["spec"];
    let jobs = spec["backups"].as_array().unwrap();
    assert_eq!(jobs.len(), 1, "{snapshot}");
    assert_eq!(jobs[0]["backup_id"], backup_id);
    assert_eq!(jobs[0]["app_id"], setup.app_id);
    assert_eq!(jobs[0]["volume_id"], setup.volume_id);
    assert_eq!(jobs[0]["fencing_token"], 1);
    let keys = setup.repo_key.source_keys(&setup.app_id);
    assert_eq!(jobs[0]["id_key"], base64_of(keys.id_key.as_bytes()));
    assert_eq!(jobs[0]["data_key"], base64_of(keys.data_key.as_bytes()));
    let repository = &spec["backup_repository"];
    assert_eq!(repository["repository_id"], setup.repo_id);
    assert_eq!(repository["root"], root(&setup.server));
    assert_eq!(repository["bucket"], "quoro");
    assert_eq!(repository["secret_access_key"], "secret-access-key");

    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let data = vec![7u8; 300 * 1024];
    let (key, digest, logical_bytes, chunk_count) = write_manifest(&setup, &backup_id, &data).await;
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
                backup_id: backup_id.clone(),
                status: BackupState::Running,
                snapshot_method: Some(wire::SnapshotMethod::Reflink),
                pause_ms: Some(0),
                guest_flush: None,
                manifest_key: None,
                manifest_digest: None,
                logical_bytes: None,
                uploaded_bytes: None,
                chunk_count: None,
                error: None,
                started_at: Some(quoro_control_plane::store::now_rfc3339()),
                finished_at: None,
            }],
        ),
    )
    .await;
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
                backup_id: backup_id.clone(),
                status: BackupState::Succeeded,
                snapshot_method: Some(wire::SnapshotMethod::Reflink),
                pause_ms: Some(0),
                guest_flush: None,
                manifest_key: Some(key.clone()),
                manifest_digest: Some(digest.clone()),
                logical_bytes: Some(logical_bytes),
                uploaded_bytes: Some(0),
                chunk_count: Some(chunk_count),
                error: None,
                started_at: Some(quoro_control_plane::store::now_rfc3339()),
                finished_at: Some(quoro_control_plane::store::now_rfc3339()),
            }],
        ),
    )
    .await;
    let done = wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &backup_id,
        "succeeded",
    )
    .await;
    assert_eq!(done["logical_bytes"], logical_bytes);
    assert_eq!(done["chunk_count"], chunk_count);
    assert_eq!(done["manifest_digest"], Value::Null);
    assert_eq!(done["snapshot_method"], "reflink");

    // The seal verifies with the cluster public key and names the digest.
    let seal_key = format!(
        "{}{}",
        root(&setup.server),
        layout::seal(&setup.app_id, &backup_id)
    );
    let bytes = setup.memory.get(&seal_key).await.unwrap();
    let seal: quoro_backup::Seal = serde_json::from_slice(&bytes).unwrap();
    quoro_backup::verify_seal(
        &seal,
        &setup.server.state.keys.signing_key().verifying_key(),
    )
    .unwrap();
    assert_eq!(seal.manifest_digest, digest);
    assert_eq!(seal.manifest_key, key);
    assert_eq!(seal.chunk_count, chunk_count);

    let emitted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE type = 'backup.succeeded'")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(emitted, 1);

    // The job leaves desired state and the repository with it.
    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let spec = &snapshot["body"]["spec"];
    assert!(
        spec.get("backups").is_none() || spec["backups"].as_array().is_none_or(Vec::is_empty),
        "{snapshot}"
    );
    assert!(spec.get("backup_repository").is_none(), "{snapshot}");
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// ---------------------------------------------------------------------------
// A primary without the repository key
// ---------------------------------------------------------------------------

/// Empties `repo_key_enc`, which is how a promoted primary rebuilds the row
/// from the change log (`node-agent-protocol.md` section 7.3, step 4), and
/// returns the blob it held.
async fn drop_repository_key(server: &Server, repo_id: &str) -> Vec<u8> {
    let blob: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert!(!blob.is_empty());
    sqlx::query("UPDATE backup_repositories SET repo_key_enc = X'' WHERE id = ?")
        .bind(repo_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    blob
}

/// Asserts a `409 conflict` that names the absent key, and returns its detail.
async fn key_absent_conflict(res: reqwest::Response) -> String {
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["code"], "conflict", "{body}");
    let detail = body["detail"].as_str().unwrap_or_default().to_string();
    assert!(
        detail.contains("does not hold the backup repository key"),
        "{body}"
    );
    detail
}

/// The audit records of calls refused with `conflict`.
async fn conflicts(server: &Server) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE result = 'error' AND result_detail = 'conflict'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
}

/// Every key of the in-memory bucket, sorted.
async fn bucket_keys(memory: &MemoryStore) -> Vec<String> {
    let mut keys: Vec<String> = memory
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    keys.sort();
    keys
}

/// `node-agent-protocol.md` section 7.3, step 4 (ADR 0322): until the key is
/// back on this primary, the repository shows `repository_key_present: false`,
/// `createBackup`, `restoreBackup` and `rotateBackupRecoveryKey` answer
/// `conflict`, the pre-upgrade backup fails, and garbage collection does not
/// run. Nothing in the bucket changes.
#[tokio::test]
async fn a_primary_without_the_repository_key_refuses_backup_work() {
    let mut setup = setup().await;
    let backup_id = run_backup(&mut setup, &vec![5u8; 300 * 1024]).await;
    let node_id: String = sqlx::query_scalar("SELECT node_id FROM backups WHERE id = ?")
        .bind(&backup_id)
        .fetch_one(setup.server.state.store.pool())
        .await
        .unwrap();
    let res = setup
        .owner
        .get(
            setup
                .server
                .url(&format!("/api/v1/backup-repositories/{}", setup.repo_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.json::<Value>().await.unwrap()["repository_key_present"],
        true
    );

    drop_repository_key(&setup.server, &setup.repo_id).await;
    let fingerprint: String =
        sqlx::query_scalar("SELECT recovery_key_fingerprint FROM backup_repositories WHERE id = ?")
            .bind(&setup.repo_id)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    // A dead chunk a sweep with the key would delete
    // (`a_sweep_deletes_only_dead_old_chunks`).
    let dead_key = format!(
        "{}{}",
        root(&setup.server),
        layout::chunk(&ChunkId([0xaa; 32]))
    );
    setup
        .memory
        .put_new(&dead_key, b"dead".to_vec(), None)
        .await
        .unwrap();
    let objects = bucket_keys(&setup.memory).await;
    let backups: i64 = sqlx::query_scalar("SELECT count(*) FROM backups")
        .fetch_one(setup.server.state.store.pool())
        .await
        .unwrap();
    let conflicts_before = conflicts(&setup.server).await;

    // The repository shows the absence, read alone and in the list.
    let res = setup
        .owner
        .get(
            setup
                .server
                .url(&format!("/api/v1/backup-repositories/{}", setup.repo_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.json::<Value>().await.unwrap()["repository_key_present"],
        false
    );
    let res = setup
        .owner
        .get(setup.server.url("/api/v1/backup-repositories"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    assert_eq!(list["items"][0]["id"], setup.repo_id.as_str());
    assert_eq!(list["items"][0]["repository_key_present"], false);

    // `createBackup` queues nothing.
    key_absent_conflict(
        setup
            .owner
            .post(
                setup
                    .server
                    .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
            )
            .send()
            .await
            .unwrap(),
    )
    .await;
    // `restoreBackup` of a succeeded backup onto its own, connected node
    // starts nothing.
    key_absent_conflict(
        setup
            .owner
            .post(
                setup
                    .server
                    .url(&format!("/api/v1/backups/{backup_id}/restore")),
            )
            .json(&json!({ "node_id": node_id }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let restoring: i64 = sqlx::query_scalar("SELECT count(*) FROM volumes WHERE state <> 'ready'")
        .fetch_one(setup.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(restoring, 0);
    // `rotateBackupRecoveryKey` writes no config object and keeps the key.
    key_absent_conflict(
        setup
            .owner
            .post(setup.server.url(&format!(
                "/api/v1/backup-repositories/{}/recovery-key",
                setup.repo_id
            )))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let after: String =
        sqlx::query_scalar("SELECT recovery_key_fingerprint FROM backup_repositories WHERE id = ?")
            .bind(&setup.repo_id)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(after, fingerprint);
    assert_eq!(
        setup
            .memory
            .get(&format!("{}config-2.json", root(&setup.server)))
            .await,
        Err(ObjectStoreError::NotFound)
    );
    assert_eq!(
        conflicts(&setup.server).await,
        conflicts_before + 3,
        "each refusal is audited"
    );

    // The pre-upgrade backup fails before it touches the bucket
    // (`backup-format.md` section 7.2 step 4).
    let upgrade = quoro_control_plane::backups::control_plane::backup_before_upgrade(
        &setup.server.state.store,
        &setup.server.data_dir,
        &setup.server.state.keys,
        &setup.server.state.data_key,
        &setup.server.state.backups,
        time::OffsetDateTime::now_utc(),
    )
    .await;
    assert_eq!(upgrade.err().as_deref(), Some("repository key absent"));

    // Garbage collection does not run: no error, nothing deleted, no record.
    let counts = quoro_control_plane::backups::prune::sweep(
        &setup.server.state.store,
        &setup.server.state.data_key,
        &setup.server.state.keys,
        &setup.server.state.backups,
        &setup.repo_id,
        time::OffsetDateTime::now_utc() + time::Duration::days(3),
    )
    .await
    .unwrap();
    assert_eq!(
        counts,
        quoro_control_plane::backups::prune::GcCounts::default()
    );
    assert!(!setup.server.state.backups.is_sweeping());
    let sweeps: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'backup.gc'")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(sweeps, 0);

    assert_eq!(bucket_keys(&setup.memory).await, objects);
    let now_backups: i64 = sqlx::query_scalar("SELECT count(*) FROM backups")
        .fetch_one(setup.server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(now_backups, backups);
}

/// The `(source_kind, status, error)` of every scheduled backup, oldest first.
async fn scheduled_rows(server: &Server) -> Vec<(String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT source_kind, status, error FROM backups WHERE \"trigger\" = 'schedule' \
         ORDER BY created_at, source_kind",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

/// `node-agent-protocol.md` section 7.3, step 4: without the repository key, a
/// scheduled backup of a volume or of the control-plane database is recorded
/// `failed` at once with `repository key absent` and emits `backup.failed`.
/// It counts as the last attempt, so the next interval tries again, and once
/// the key is back the next interval takes a real backup.
#[tokio::test]
async fn a_scheduled_backup_without_the_repository_key_fails_at_once() {
    let setup = setup().await;
    let pool = setup.server.state.store.pool();
    sqlx::query("UPDATE apps SET ha_policy = 'cold', backup_schedule = ? WHERE id = ?")
        .bind(r#"{"interval_h":1,"keep":2}"#)
        .bind(&setup.app_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE backup_repositories SET control_plane_schedule = ? WHERE id = ?")
        .bind(r#"{"interval_h":6,"keep":7}"#)
        .bind(&setup.repo_id)
        .execute(pool)
        .await
        .unwrap();
    // The control-plane schedule backs up from the primary's node, and this
    // harness enrolls node-alpha without that status: name it the primary.
    sqlx::query(
        "UPDATE nodes SET cp_status = 'primary' \
         WHERE id = (SELECT node_id FROM volumes WHERE id = ?)",
    )
    .bind(&setup.volume_id)
    .execute(pool)
    .await
    .unwrap();
    let blob = drop_repository_key(&setup.server, &setup.repo_id).await;
    let objects = bucket_keys(&setup.memory).await;
    let absent = Some("repository key absent".to_string());
    let failed = |kind: &str| ("failed".to_string(), kind.to_string(), absent.clone());
    let shape = |rows: Vec<(String, String, Option<String>)>| {
        rows.into_iter()
            .map(|(kind, status, error)| (status, kind, error))
            .collect::<Vec<_>>()
    };
    let state = &setup.server.state;
    let now = time::OffsetDateTime::now_utc();

    // Both sources are due: each is recorded failed at once and no node hears
    // of a job.
    let tick = quoro_control_plane::backups::dispatch::run(state, now)
        .await
        .unwrap();
    assert_eq!(tick.queued, 0);
    assert_eq!(
        shape(scheduled_rows(&setup.server).await),
        vec![failed("control_plane"), failed("volume")]
    );
    let ids: Vec<(String, String)> =
        sqlx::query_as("SELECT id, source_kind FROM backups ORDER BY source_kind")
            .fetch_all(pool)
            .await
            .unwrap();
    for (backup_id, source_kind) in &ids {
        let data: String = sqlx::query_scalar(
            "SELECT data FROM events WHERE type = 'backup.failed' \
             AND json_extract(subject, '$.id') = ?",
        )
        .bind(backup_id)
        .fetch_one(pool)
        .await
        .unwrap();
        let data: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(data["backup_id"], backup_id.as_str());
        assert_eq!(data["repository_id"], setup.repo_id.as_str());
        assert_eq!(data["source_kind"], source_kind.as_str());
        assert_eq!(data["trigger"], "schedule");
        assert_eq!(data["error"], "repository key absent");
        let records: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT operation, result, result_detail FROM audit_log WHERE target_id = ? \
             ORDER BY seq",
        )
        .bind(backup_id)
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(
            records,
            vec![
                ("backup.dispatch".to_string(), "ok".to_string(), None),
                (
                    "backup.seal".to_string(),
                    "error".to_string(),
                    absent.clone()
                ),
            ]
        );
    }
    let volume_backup = &ids.iter().find(|(_, kind)| kind == "volume").unwrap().0;
    let res = setup
        .owner
        .get(
            setup
                .server
                .url(&format!("/api/v1/backups/{volume_backup}")),
        )
        .send()
        .await
        .unwrap();
    let backup: Value = res.json().await.unwrap();
    assert_eq!(backup["status"], "failed");
    assert_eq!(backup["error"], "repository key absent");

    // Not again before the next interval.
    quoro_control_plane::backups::dispatch::run(state, now + time::Duration::minutes(30))
        .await
        .unwrap();
    assert_eq!(scheduled_rows(&setup.server).await.len(), 2);
    // The volume's next interval tries again, and fails again.
    quoro_control_plane::backups::dispatch::run(state, now + time::Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(
        shape(scheduled_rows(&setup.server).await),
        vec![failed("control_plane"), failed("volume"), failed("volume")]
    );
    // So does the control plane's.
    quoro_control_plane::backups::dispatch::run(state, now + time::Duration::hours(6))
        .await
        .unwrap();
    assert_eq!(
        shape(scheduled_rows(&setup.server).await),
        vec![
            failed("control_plane"),
            failed("volume"),
            failed("volume"),
            failed("control_plane"),
            failed("volume"),
        ]
    );
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE type = 'backup.failed'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(events, 5);
    // Failed at once by the schedule, not by a task that started: no
    // key-absent row ever ran.
    let started: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM backups WHERE \"trigger\" = 'schedule' AND started_at IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(started, 0);
    assert_eq!(bucket_keys(&setup.memory).await, objects);

    // With the key back, the volume's next interval queues a real backup.
    sqlx::query("UPDATE backup_repositories SET repo_key_enc = ? WHERE id = ?")
        .bind(&blob)
        .bind(&setup.repo_id)
        .execute(pool)
        .await
        .unwrap();
    let tick = quoro_control_plane::backups::dispatch::run(state, now + time::Duration::hours(7))
        .await
        .unwrap();
    assert_eq!(tick.queued, 1);
    let rows = scheduled_rows(&setup.server).await;
    assert_eq!(
        rows.last().unwrap(),
        &("volume".to_string(), "queued".to_string(), None)
    );
}

/// The `backup_id` of every job in a desired-state frame.
fn spec_jobs(snapshot: &Value) -> Vec<String> {
    snapshot["body"]["spec"]["backups"]
        .as_array()
        .map(|jobs| {
            jobs.iter()
                .map(|job| job["backup_id"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The next desired-state frame that `wanted` accepts.
async fn recv_spec(socket: &mut Socket, wanted: impl Fn(&Value) -> bool) -> Value {
    loop {
        let snapshot = recv_type(socket, "desired_state").await;
        if wanted(&snapshot) {
            return snapshot;
        }
    }
}

/// A `status_report.backups[]` entry with `status`, and the manifest's key,
/// digest, logical size and chunk count when one is given.
fn reported_backup(
    backup_id: &str,
    status: BackupState,
    manifest: Option<(String, String, u64, u64)>,
) -> wire::BackupStatus {
    let (manifest_key, manifest_digest, logical_bytes, chunk_count) = match manifest {
        Some((key, digest, logical, chunks)) => {
            (Some(key), Some(digest), Some(logical), Some(chunks))
        }
        None => (None, None, None, None),
    };
    let done = matches!(status, BackupState::Succeeded | BackupState::Failed);
    wire::BackupStatus {
        backup_id: backup_id.to_string(),
        status,
        snapshot_method: Some(wire::SnapshotMethod::Reflink),
        pause_ms: Some(0),
        guest_flush: None,
        manifest_key,
        manifest_digest,
        logical_bytes,
        uploaded_bytes: logical_bytes.map(|_| 0),
        chunk_count,
        error: None,
        started_at: Some(quoro_control_plane::store::now_rfc3339()),
        finished_at: done.then(quoro_control_plane::store::now_rfc3339),
    }
}

/// `node-agent-protocol.md` section 7.3, step 4: a volume backup still
/// `queued` or `running` while this primary does not hold the repository key,
/// as a promoted primary inherits one, leaves its node's desired state, which
/// is still built and sent with everything else. The next dispatch tick
/// records the backup `failed` with `repository key absent`, with the event and
/// records of any failure, and a later report of it from the node changes
/// nothing. Once the key is restored, a new backup's job carries its source
/// keys again.
#[tokio::test]
async fn a_backup_in_progress_without_the_repository_key_leaves_the_desired_state_and_fails() {
    let mut setup = setup().await;
    let pool = setup.server.state.store.pool().clone();
    let (node_id, vm_id): (String, String) = sqlx::query_as(
        "SELECT v.node_id, m.id FROM volumes v JOIN vms m ON m.app_id = v.app_id WHERE v.id = ?",
    )
    .bind(&setup.volume_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let absent = "repository key absent";
    let now = time::OffsetDateTime::now_utc();

    // First a backup its node reported `running`, then one still `queued`,
    // each accepted while this primary held the key.
    for (round, running) in [(1u64, true), (2, false)] {
        let res = setup
            .owner
            .post(
                setup
                    .server
                    .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202, "create: {}", res.text().await.unwrap());
        let backup_id = res.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let snapshot = recv_spec(&mut setup.socket, |snapshot| {
            spec_jobs(snapshot) == [backup_id.clone()]
        })
        .await;
        let rev = snapshot["body"]["rev"].as_u64().unwrap();
        if running {
            send(
                &mut setup.socket,
                status_report(
                    rev,
                    Vec::new(),
                    vec![reported_backup(&backup_id, BackupState::Running, None)],
                ),
            )
            .await;
            wait_for_backup(
                &setup.owner,
                &setup.server,
                &mut setup.socket,
                &backup_id,
                "running",
            )
            .await;
        }
        let blob = drop_repository_key(&setup.server, &setup.repo_id).await;

        // The node's next desired state has neither the job nor the
        // repository, and still has the app's VM and volume.
        setup.server.state.hub.node_changed(&node_id);
        let snapshot =
            recv_spec(&mut setup.socket, |snapshot| spec_jobs(snapshot).is_empty()).await;
        let spec = &snapshot["body"]["spec"];
        assert!(spec.get("backup_repository").is_none(), "{snapshot}");
        assert!(
            spec["vms"]
                .as_array()
                .unwrap()
                .iter()
                .any(|vm| vm["vm_id"] == vm_id.as_str()),
            "{snapshot}"
        );
        assert!(
            spec["volumes"].as_array().unwrap().iter().any(|volume| {
                volume["volume_id"] == setup.volume_id.as_str() && volume["state"] == "present"
            }),
            "{snapshot}"
        );
        let status: String = sqlx::query_scalar("SELECT status FROM backups WHERE id = ?")
            .bind(&backup_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, if running { "running" } else { "queued" });

        // The tick records it failed, as any failure: the row, its
        // change-log entry, the `backup.failed` event and the `backup.seal`
        // record.
        let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, now)
            .await
            .unwrap();
        assert_eq!((tick.key_absent, tick.queued, tick.timed_out), (1, 0, 0));
        let failed = wait_for_backup(
            &setup.owner,
            &setup.server,
            &mut setup.socket,
            &backup_id,
            "failed",
        )
        .await;
        assert_eq!(failed["error"], absent);
        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM change_log WHERE entity_type = 'backup' AND entity_id = ? \
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(&backup_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["status"], "failed");
        assert_eq!(payload["error"], absent);
        let data: String = sqlx::query_scalar(
            "SELECT data FROM events WHERE type = 'backup.failed' \
             AND json_extract(subject, '$.id') = ?",
        )
        .bind(&backup_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let data: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(data["backup_id"], backup_id.as_str());
        assert_eq!(data["trigger"], "api");
        assert_eq!(data["error"], absent);
        let records: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM audit_log WHERE target_id = ? AND operation = 'backup.seal' \
             AND result = 'error' AND result_detail = ?",
        )
        .bind(&backup_id)
        .bind(absent)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(records, 1);
        let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, now)
            .await
            .unwrap();
        assert_eq!(tick.key_absent, 0);

        // A later report of the backup changes nothing, even a `succeeded`
        // one with a real manifest. The volume report sent after it is
        // applied only once the backup report was.
        let manifest = write_manifest(&setup, &backup_id, &vec![9u8; 64 * 1024]).await;
        send(
            &mut setup.socket,
            status_report(
                rev,
                Vec::new(),
                vec![reported_backup(
                    &backup_id,
                    BackupState::Succeeded,
                    Some(manifest),
                )],
            ),
        )
        .await;
        let allocated = 4096 * (round + 1);
        send(
            &mut setup.socket,
            status_report(
                rev,
                vec![VolumeStatus {
                    volume_id: setup.volume_id.clone(),
                    state: WireVolumeState::Ready,
                    allocated_bytes: Some(allocated),
                    error: None,
                }],
                Vec::new(),
            ),
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let applied: Option<i64> =
                sqlx::query_scalar("SELECT allocated_bytes FROM volumes WHERE id = ?")
                    .bind(&setup.volume_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            if applied == Some(i64::try_from(allocated).unwrap()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the volume report was not applied"
            );
            heartbeat(&mut setup.socket).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let row: (String, Option<String>) =
            sqlx::query_as("SELECT status, error FROM backups WHERE id = ?")
                .bind(&backup_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row, ("failed".to_string(), Some(absent.to_string())));
        assert_eq!(
            setup
                .memory
                .get(&format!(
                    "{}{}",
                    root(&setup.server),
                    layout::seal(&setup.app_id, &backup_id)
                ))
                .await,
            Err(ObjectStoreError::NotFound)
        );
        let succeeded: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM events WHERE type = 'backup.succeeded' \
             AND json_extract(subject, '$.id') = ?",
        )
        .bind(&backup_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(succeeded, 0);

        // The key comes back by SQL for the second round.
        if round == 1 {
            sqlx::query("UPDATE backup_repositories SET repo_key_enc = ? WHERE id = ?")
                .bind(&blob)
                .bind(&setup.repo_id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    // Restored from the recovery key, the key gives a new backup's job its
    // source keys and the repository entry again.
    let res =
        restore_repository_key(&setup.owner, &setup.server, &setup.repo_id, &setup.recovery).await;
    assert_eq!(res.status(), 200, "restore: {}", res.text().await.unwrap());
    let res = setup
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "create: {}", res.text().await.unwrap());
    let backup_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let snapshot = recv_spec(&mut setup.socket, |snapshot| {
        spec_jobs(snapshot) == [backup_id.clone()]
    })
    .await;
    let spec = &snapshot["body"]["spec"];
    let keys = setup.repo_key.source_keys(&setup.app_id);
    assert_eq!(
        spec["backups"][0]["id_key"],
        base64_of(keys.id_key.as_bytes())
    );
    assert_eq!(
        spec["backups"][0]["data_key"],
        base64_of(keys.data_key.as_bytes())
    );
    assert_eq!(
        spec["backup_repository"]["repository_id"],
        setup.repo_id.as_str()
    );
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, now)
        .await
        .unwrap();
    assert_eq!(tick.key_absent, 0);
    let status: String = sqlx::query_scalar("SELECT status FROM backups WHERE id = ?")
        .bind(&backup_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "queued");
}

// ---------------------------------------------------------------------------
// Restoring the repository key (ADR 0322)
// ---------------------------------------------------------------------------

/// The audit operation of `restoreBackupRepositoryKey`.
const RESTORE_KEY_OPERATION: &str =
    "POST /api/v1/backup-repositories/{repository_id}/repository-key";

/// A `MemoryStore` bucket that counts its listings and reads, and can refuse
/// every listing or every read, so a test sees whether and how the bucket was
/// read. With [`CountingBuckets::write_first`], its next read first writes
/// `repo_key_enc`, as a concurrent call that restored the key would.
#[derive(Clone)]
struct CountingBuckets {
    memory: Arc<MemoryStore>,
    lists: Arc<AtomicUsize>,
    gets: Arc<AtomicUsize>,
    list_fails: Arc<AtomicBool>,
    get_fails: Arc<AtomicBool>,
    written_first: Arc<Mutex<Option<KeyWrittenFirst>>>,
}

/// A `repo_key_enc` that lands while a `restoreBackupRepositoryKey` call
/// reads the bucket.
struct KeyWrittenFirst {
    pool: sqlx::SqlitePool,
    repo_id: String,
    blob: Vec<u8>,
}

impl CountingBuckets {
    fn new(memory: Arc<MemoryStore>) -> Self {
        Self {
            memory,
            lists: Arc::new(AtomicUsize::new(0)),
            gets: Arc::new(AtomicUsize::new(0)),
            list_fails: Arc::new(AtomicBool::new(false)),
            get_fails: Arc::new(AtomicBool::new(false)),
            written_first: Arc::new(Mutex::new(None)),
        }
    }

    /// Before the next read returns, `blob` is written into `repo_id`'s
    /// `repo_key_enc`, straight into the database and with no change-log
    /// entry.
    fn write_first(&self, server: &Server, repo_id: &str, blob: Vec<u8>) {
        *self.written_first.lock().unwrap() = Some(KeyWrittenFirst {
            pool: server.state.store.pool().clone(),
            repo_id: repo_id.to_string(),
            blob,
        });
    }

    /// `(listings, reads)` so far.
    fn reads(&self) -> (usize, usize) {
        (
            self.lists.load(Ordering::SeqCst),
            self.gets.load(Ordering::SeqCst),
        )
    }
}

impl BucketStore for CountingBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.memory.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let fails = self.get_fails.load(Ordering::SeqCst);
        let written_first = self.written_first.lock().unwrap().take();
        Box::pin(async move {
            if let Some(write) = written_first {
                sqlx::query("UPDATE backup_repositories SET repo_key_enc = ? WHERE id = ?")
                    .bind(&write.blob)
                    .bind(&write.repo_id)
                    .execute(&write.pool)
                    .await
                    .unwrap();
            }
            if fails {
                return Err(ObjectStoreError::Denied("reads are refused".to_string()));
            }
            self.memory.get(key).await
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        let fails = self.list_fails.load(Ordering::SeqCst);
        Box::pin(async move {
            if fails {
                return Err(ObjectStoreError::Transport(
                    "the bucket is down".to_string(),
                ));
            }
            self.memory.list(prefix).await
        })
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.memory.delete(key))
    }
}

impl BucketFactory for CountingBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(self.clone()))
    }
}

/// `restoreBackupRepositoryKey` with `body`, as `client` sends it.
async fn restore_repository_key_with(
    client: &reqwest::Client,
    server: &Server,
    repo_id: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(server.url(&format!(
            "/api/v1/backup-repositories/{repo_id}/repository-key"
        )))
        .json(&body)
        .send()
        .await
        .unwrap()
}

/// `restoreBackupRepositoryKey` with `body` as raw bytes, and `content_type`
/// when one is given.
async fn restore_repository_key_raw(
    client: &reqwest::Client,
    server: &Server,
    repo_id: &str,
    content_type: Option<&str>,
    body: String,
) -> reqwest::Response {
    let mut request = client.post(server.url(&format!(
        "/api/v1/backup-repositories/{repo_id}/repository-key"
    )));
    if let Some(content_type) = content_type {
        request = request.header("Content-Type", content_type);
    }
    request.body(body).send().await.unwrap()
}

/// `restoreBackupRepositoryKey` with `recovery_key`.
async fn restore_repository_key(
    client: &reqwest::Client,
    server: &Server,
    repo_id: &str,
    recovery_key: &str,
) -> reqwest::Response {
    restore_repository_key_with(
        client,
        server,
        repo_id,
        json!({ "recovery_key": recovery_key }),
    )
    .await
}

/// The status, `code` and whole body of a problem answer.
async fn problem_text(res: reqwest::Response) -> (u16, String, String) {
    let status = res.status().as_u16();
    let text = res.text().await.unwrap();
    let body: Value = serde_json::from_str(&text).unwrap();
    (
        status,
        body["code"].as_str().unwrap_or_default().to_string(),
        text,
    )
}

/// The 32 bytes `repo_key_enc` holds.
async fn held_repository_key(server: &Server, repo_id: &str) -> [u8; 32] {
    let blob: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    let secret = server
        .state
        .data_key
        .decrypt("backup_repositories.repo_key_enc", repo_id, &blob)
        .unwrap();
    decode_hex32(secret.expose())
}

/// Whether `repo_key_enc` is empty.
async fn key_is_absent(server: &Server, repo_id: &str) -> bool {
    let length: i64 =
        sqlx::query_scalar("SELECT length(repo_key_enc) FROM backup_repositories WHERE id = ?")
            .bind(repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    length == 0
}

/// The newest change-log `seq`.
async fn log_head(server: &Server) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// `(entity_type, entity_id)` of every change-log entry after `seq`.
async fn log_entries_after(server: &Server, seq: i64) -> Vec<(String, String)> {
    sqlx::query_as("SELECT entity_type, entity_id FROM change_log WHERE seq > ? ORDER BY seq")
        .bind(seq)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap()
}

/// One audit record of `restoreBackupRepositoryKey`.
#[derive(Debug)]
struct RestoreKeyRecord {
    actor_type: String,
    result: String,
    result_detail: Option<String>,
    scope: Option<String>,
    target_type: Option<String>,
    target_id: Option<String>,
    params: String,
    change_seq: Option<i64>,
}

/// Every audit record of `restoreBackupRepositoryKey`, oldest first.
async fn restore_key_records(server: &Server) -> Vec<RestoreKeyRecord> {
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<i64>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT actor_type, result, result_detail, scope, target_type, target_id, params, \
         change_seq FROM audit_log WHERE operation = ? ORDER BY seq",
    )
    .bind(RESTORE_KEY_OPERATION)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(
            |(
                actor_type,
                result,
                result_detail,
                scope,
                target_type,
                target_id,
                params,
                change_seq,
            )| {
                RestoreKeyRecord {
                    actor_type,
                    result,
                    result_detail,
                    scope,
                    target_type,
                    target_id,
                    params,
                    change_seq,
                }
            },
        )
        .collect()
}

/// Asserts that no change-log payload, audit record, event or answer in
/// `texts` holds any of `recovery_keys`, in upper or lower case, or the
/// repository key, in hex or in standard base64.
async fn assert_no_key_anywhere(
    server: &Server,
    recovery_keys: &[&str],
    repo_key: &RepoKey,
    texts: &[String],
) {
    let repo_hex: String = repo_key
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let repo_base64 = base64_of(repo_key.as_bytes());
    let recovery_forms: Vec<String> = recovery_keys
        .iter()
        .flat_map(|key| [key.to_string(), key.to_lowercase()])
        .collect();
    let pool = server.state.store.pool();
    let mut all: Vec<String> = texts.to_vec();
    all.extend(
        sqlx::query_scalar::<_, String>("SELECT payload FROM change_log")
            .fetch_all(pool)
            .await
            .unwrap(),
    );
    let audits: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT params, result_detail, justification FROM audit_log")
            .fetch_all(pool)
            .await
            .unwrap();
    all.extend(
        audits
            .into_iter()
            .map(|(params, detail, justification)| format!("{params}{detail:?}{justification:?}")),
    );
    all.extend(
        sqlx::query_scalar::<_, String>("SELECT data FROM events")
            .fetch_all(pool)
            .await
            .unwrap(),
    );
    for text in &all {
        for recovery in &recovery_forms {
            assert!(!text.contains(recovery), "a recovery key leaked: {text}");
        }
        assert!(
            !text.contains(&repo_hex) && !text.contains(&repo_base64),
            "the repository key leaked: {text}"
        );
    }
}

/// An access token of a new agent the owner owns.
async fn agent_token(owner: &reqwest::Client, server: &Server) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "backup-keys",
            "model": "test-model",
            "owner_user_id": me["user"]["id"],
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

/// `backup-format.md` section 4, "Restoring the key on a primary" (ADR 0322):
/// on a primary without the key, the owner's recovery key puts the same
/// repository key back on this primary. The call appends no change-log entry
/// but the audit record's own, whose `params` are
/// `{"recovery_key": "[redacted]"}`. Backups run again, and a second call
/// answers `conflict`.
#[tokio::test]
async fn the_recovery_key_restores_the_repository_key_and_backups_run_again() {
    let mut setup = setup().await;
    let recovery = setup.recovery.clone();
    drop_repository_key(&setup.server, &setup.repo_id).await;
    key_absent_conflict(
        setup
            .owner
            .post(
                setup
                    .server
                    .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
            )
            .send()
            .await
            .unwrap(),
    )
    .await;
    let mut texts = Vec::new();

    let head = log_head(&setup.server).await;
    let res = restore_repository_key(&setup.owner, &setup.server, &setup.repo_id, &recovery).await;
    let status = res.status();
    let text = res.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    texts.push(text);
    assert_eq!(body["id"], setup.repo_id.as_str());
    assert_eq!(body["repository_key_present"], true);
    assert!(body.get("recovery_key").is_none(), "{body}");

    // The same key, wrapped under the data key on this primary.
    assert_eq!(
        &held_repository_key(&setup.server, &setup.repo_id).await,
        setup.repo_key.as_bytes()
    );
    // One change-log entry: the audit record's own. No entry for the row.
    let entries = log_entries_after(&setup.server, head).await;
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].0, "audit", "{entries:?}");
    let records = restore_key_records(&setup.server).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.actor_type, "user");
    assert_eq!(record.result, "ok");
    assert_eq!(record.result_detail, None);
    assert_eq!(record.scope.as_deref(), Some("backup:manage"));
    assert_eq!(record.target_type.as_deref(), Some("backup_repository"));
    assert_eq!(record.target_id.as_deref(), Some(setup.repo_id.as_str()));
    assert_eq!(
        serde_json::from_str::<Value>(&record.params).unwrap(),
        json!({ "recovery_key": "[redacted]" })
    );
    assert_eq!(record.change_seq, None);

    let res = setup
        .owner
        .get(
            setup
                .server
                .url(&format!("/api/v1/backup-repositories/{}", setup.repo_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.json::<Value>().await.unwrap()["repository_key_present"],
        true
    );

    // `createBackup` is accepted again and the backup seals.
    run_backup(&mut setup, &vec![7u8; 300 * 1024]).await;

    // A second call: this primary holds the key.
    let blob: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(&setup.repo_id)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    let (status, code, text) = problem_text(
        restore_repository_key(&setup.owner, &setup.server, &setup.repo_id, &recovery).await,
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{text}");
    texts.push(text);
    let after: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(&setup.repo_id)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(after, blob);
    let records = restore_key_records(&setup.server).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[1].result, "error");
    assert_eq!(records[1].result_detail.as_deref(), Some("conflict"));
    assert_eq!(
        serde_json::from_str::<Value>(&records[1].params).unwrap(),
        json!({ "recovery_key": "[redacted]" })
    );

    assert_no_key_anywhere(&setup.server, &[&recovery], &setup.repo_key, &texts).await;
}

/// Step 1 is checked before the bucket is read. A malformed key, a key that
/// matches the fingerprint but opens no config object, a bucket that cannot
/// be listed and an unknown repository are refused too. Until the right call,
/// the key stays absent, only audit records enter the change log, and each
/// call is audited with the recovery key redacted.
#[tokio::test]
async fn restoring_the_repository_key_refuses_what_does_not_open_it() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    let buckets = CountingBuckets::new(Arc::clone(&memory));
    server.state.backups.set_factory(Arc::new(buckets.clone()));
    let owner = server.owner().await;
    let (repo_id, recovery, repo_key) = create_repository(&owner, &server).await;
    drop_repository_key(&server, &repo_id).await;
    let head = log_head(&server).await;
    let mut texts = Vec::new();
    let mut audited = 0;

    // A well-formed key of another repository: its fingerprint differs, and
    // the bucket is not read.
    let other = RecoveryKey::generate().to_text();
    let reads = buckets.reads();
    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &other).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    assert!(text.contains("fingerprint"), "{text}");
    assert_eq!(buckets.reads(), reads, "the bucket was read");
    texts.push(text);
    audited += 1;

    // Keys that do not match the request pattern or have nonzero padding
    // bits (`backup-format.md` section 3), and bodies without a key.
    let malformed = [
        recovery.to_lowercase(),
        recovery[..recovery.len() - 1].to_string(),
        recovery.replace('-', ""),
        format!("qrk1-Z{}", &recovery[6..]),
        format!(" {recovery}"),
        String::new(),
    ];
    for bad in &malformed {
        let (status, code, text) =
            problem_text(restore_repository_key(&owner, &server, &repo_id, bad).await).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{bad}: {text}"
        );
        texts.push(text);
        audited += 1;
    }
    // Bodies that are not a JSON object with a `recovery_key` string. Each
    // answer has the same fixed detail, which quotes nothing of the body:
    // a bare JSON string (`jq -R .`) or an array holds the key itself. As on
    // every operation, a body that is not read as the request is refused
    // before the call is authorized, so it has no audit record.
    let fixed = "the body must be a JSON object with a recovery_key string, \
                 sent as application/json";
    let json_bodies = [
        json!({}),
        json!({ "recovery_key": 5 }),
        json!({ "recovery_key": [recovery] }),
        json!({ "recovery": recovery }),
        // The key as a field name.
        Value::Object(serde_json::Map::from_iter([(
            recovery.clone(),
            json!("recovery_key"),
        )])),
        json!(recovery),
        json!([recovery]),
        json!([recovery, recovery]),
    ];
    let mut answers = Vec::new();
    for body in json_bodies {
        let shown = body.to_string();
        answers.push((
            shown,
            restore_repository_key_with(&owner, &server, &repo_id, body).await,
        ));
    }
    for (content_type, body) in [
        // Not JSON at all: the key as plain text, or cut short.
        (Some("application/json"), recovery.clone()),
        (Some("application/json"), format!("\"{recovery}")),
        (
            Some("application/json"),
            format!("{{\"recovery_key\": \"{recovery}"),
        ),
        // A JSON body sent without its media type.
        (
            Some("text/plain"),
            json!({ "recovery_key": recovery }).to_string(),
        ),
        (None, json!(recovery).to_string()),
        // The same key twice in one object.
        (
            Some("application/json"),
            format!("{{\"recovery_key\": \"{recovery}\", \"recovery_key\": \"{recovery}\"}}"),
        ),
    ] {
        let shown = format!("{content_type:?} {body}");
        answers.push((
            shown,
            restore_repository_key_raw(&owner, &server, &repo_id, content_type, body).await,
        ));
    }
    for (shown, res) in answers {
        let (status, code, text) = problem_text(res).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{shown}: {text}"
        );
        let problem: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(problem["detail"], fixed, "{shown}: {text}");
        assert!(!text.contains(&recovery[5..]), "{shown}: {text}");
        texts.push(text);
    }
    assert_eq!(buckets.reads(), reads, "the bucket was read");
    assert_eq!(
        restore_key_records(&server).await.len(),
        audited,
        "a refused body was audited"
    );

    // The fingerprint matches, but config-1 holds a repository key wrapped
    // under another key.
    let config_key = format!("{}config-1.json", root(&server));
    let original = memory.get(&config_key).await.unwrap();
    let mut config: RepoConfig = serde_json::from_slice(&original).unwrap();
    config.wrapped_repo_key =
        wrap_repo_key(&RepoKey::generate(), &RecoveryKey::generate(), &repo_id);
    memory
        .put_overwrite(&config_key, serde_json::to_vec(&config).unwrap())
        .await
        .unwrap();
    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &recovery).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    assert!(text.contains("opens no config object"), "{text}");
    assert_eq!(buckets.reads(), (reads.0 + 1, reads.1 + 1));
    texts.push(text);
    audited += 1;

    // No config object at all.
    memory.delete(&config_key).await.unwrap();
    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &recovery).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    assert!(text.contains("opens no config object"), "{text}");
    texts.push(text);
    audited += 1;

    // The bucket cannot be listed.
    memory
        .put_overwrite(&config_key, original.clone())
        .await
        .unwrap();
    buckets.list_fails.store(true, Ordering::SeqCst);
    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &recovery).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    assert!(text.contains("cannot read the bucket"), "{text}");
    texts.push(text);
    audited += 1;
    buckets.list_fails.store(false, Ordering::SeqCst);

    // The listing names config-1, but it cannot be read: the bucket is not
    // readable, which is not a key that opens nothing.
    buckets.get_fails.store(true, Ordering::SeqCst);
    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &recovery).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    let problem: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        problem["detail"],
        "cannot read the bucket: a config object could not be read: denied: reads are refused",
        "{text}"
    );
    texts.push(text);
    audited += 1;
    buckets.get_fails.store(false, Ordering::SeqCst);

    // Unknown repositories.
    for unknown in ["brp_01JCA0A2B3C4D5E6F7G8H9J0KM", "brp_nope"] {
        assert_ne!(unknown, repo_id);
        let (status, code, text) =
            problem_text(restore_repository_key(&owner, &server, unknown, &recovery).await).await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{text}");
        texts.push(text);
        audited += 1;
    }

    // Nothing changed: the key is absent, and only audit records were
    // appended to the change log.
    assert!(key_is_absent(&server, &repo_id).await);
    let entries = log_entries_after(&server, head).await;
    assert_eq!(entries.len(), audited, "{entries:?}");
    assert!(
        entries.iter().all(|(kind, _)| kind == "audit"),
        "{entries:?}"
    );
    let records = restore_key_records(&server).await;
    assert_eq!(records.len(), audited, "{records:?}");
    for record in &records {
        assert_eq!(record.result, "error", "{record:?}");
        assert_eq!(record.scope.as_deref(), Some("backup:manage"));
        assert_eq!(
            serde_json::from_str::<Value>(&record.params).unwrap(),
            json!({ "recovery_key": "[redacted]" })
        );
    }

    // With the config object back and the bucket readable, the same key
    // restores the repository key.
    let res = restore_repository_key(&owner, &server, &repo_id, &recovery).await;
    let status = res.status();
    let text = res.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    texts.push(text);
    assert_eq!(
        &held_repository_key(&server, &repo_id).await,
        repo_key.as_bytes()
    );

    assert_no_key_anywhere(&server, &[&recovery, &other], &repo_key, &texts).await;
}

/// Step 3's write is guarded: when another call writes `repo_key_enc` after
/// this one found it empty, here while it reads config-1, this call answers
/// `409 conflict`. The key the other call wrote stays as it was, and the one
/// change-log entry is this call's audit record, an `error` with the
/// recovery key redacted.
#[tokio::test]
async fn a_key_written_while_the_call_reads_the_bucket_answers_conflict() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    let buckets = CountingBuckets::new(Arc::clone(&memory));
    server.state.backups.set_factory(Arc::new(buckets.clone()));
    let owner = server.owner().await;
    let (repo_id, recovery, repo_key) = create_repository(&owner, &server).await;
    let blob = drop_repository_key(&server, &repo_id).await;
    buckets.write_first(&server, &repo_id, blob.clone());
    let head = log_head(&server).await;
    let reads = buckets.reads();

    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &recovery).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"), "{text}");
    // The call found the key absent and read the bucket: the guarded write,
    // not the first check, refused it.
    assert_eq!(buckets.reads(), (reads.0 + 1, reads.1 + 1));
    assert!(buckets.written_first.lock().unwrap().is_none());

    let after: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(&repo_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(after, blob, "the key written first was replaced");

    let entries = log_entries_after(&server, head).await;
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].0, "audit", "{entries:?}");
    let records = restore_key_records(&server).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.actor_type, "user");
    assert_eq!(record.result, "error");
    assert_eq!(record.result_detail.as_deref(), Some("conflict"));
    assert_eq!(record.scope.as_deref(), Some("backup:manage"));
    assert_eq!(record.target_type.as_deref(), Some("backup_repository"));
    assert_eq!(record.target_id.as_deref(), Some(repo_id.as_str()));
    assert_eq!(
        serde_json::from_str::<Value>(&record.params).unwrap(),
        json!({ "recovery_key": "[redacted]" })
    );
    assert_eq!(record.change_seq, None);

    assert_no_key_anywhere(&server, &[&recovery], &repo_key, &[text]).await;
}

/// `backup:manage` is human-only and owner-only (permission-model.md
/// sections 3 and 4.2): an agent is refused with `human_only`, an operator
/// with `forbidden`. Neither reads the bucket, and the key stays absent.
#[tokio::test]
async fn only_an_owner_restores_the_repository_key() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    let buckets = CountingBuckets::new(Arc::clone(&memory));
    server.state.backups.set_factory(Arc::new(buckets.clone()));
    let owner = server.owner().await;
    let (repo_id, recovery, repo_key) = create_repository(&owner, &server).await;
    drop_repository_key(&server, &repo_id).await;
    let reads = buckets.reads();
    let mut texts = Vec::new();

    let token = agent_token(&owner, &server).await;
    let res = reqwest::Client::new()
        .post(server.url(&format!(
            "/api/v1/backup-repositories/{repo_id}/repository-key"
        )))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "restore the backup key")
        .json(&json!({ "recovery_key": recovery }))
        .send()
        .await
        .unwrap();
    let (status, code, text) = problem_text(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"), "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["reason"], "human_only", "{text}");
    texts.push(text);

    server
        .insert_user("operator@example.com", "operator password one", "operator")
        .await;
    let operator = server.client();
    server
        .login(&operator, "operator@example.com", "operator password one")
        .await;
    let (status, code, text) =
        problem_text(restore_repository_key(&operator, &server, &repo_id, &recovery).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"), "{text}");
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["reason"], "scope_missing", "{text}");
    texts.push(text);

    assert_eq!(buckets.reads(), reads, "the bucket was read");
    assert!(key_is_absent(&server, &repo_id).await);
    let records = restore_key_records(&server).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[0].actor_type, "agent");
    assert_eq!(records[1].actor_type, "user");
    for record in &records {
        assert_eq!(record.result, "refused", "{record:?}");
        assert_eq!(
            serde_json::from_str::<Value>(&record.params).unwrap(),
            json!({ "recovery_key": "[redacted]" })
        );
    }
    assert_eq!(records[0].result_detail.as_deref(), Some("human_only"));
    assert_eq!(records[1].result_detail.as_deref(), Some("scope_missing"));

    assert_no_key_anywhere(&server, &[&recovery], &repo_key, &texts).await;
}

/// After `rotateBackupRecoveryKey` wrote config-2, the test removes the older
/// config to check the fingerprint refusal: the new recovery key restores the
/// key and the old one is refused by its fingerprint. The restored key
/// rotates again: config-3 opens to the same repository key.
#[tokio::test]
async fn the_rotated_recovery_key_restores_the_repository_key() {
    let server = start_with_patient_node_session().await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    let (repo_id, old, repo_key) = create_repository(&owner, &server).await;
    let rotate = |client: reqwest::Client, url: String| async move {
        let res = client.post(url).send().await.unwrap();
        assert_eq!(res.status(), 200, "rotate: {}", res.text().await.unwrap());
        res.json::<Value>().await.unwrap()["recovery_key"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let rotate_url = server.url(&format!(
        "/api/v1/backup-repositories/{repo_id}/recovery-key"
    ));
    let new = rotate(owner.clone(), rotate_url.clone()).await;
    memory
        .delete(&format!("{}config-1.json", root(&server)))
        .await
        .unwrap();
    drop_repository_key(&server, &repo_id).await;
    let mut texts = Vec::new();

    let (status, code, text) =
        problem_text(restore_repository_key(&owner, &server, &repo_id, &old).await).await;
    assert_eq!(
        (status, code.as_str()),
        (422, "validation_failed"),
        "{text}"
    );
    assert!(text.contains("fingerprint"), "{text}");
    texts.push(text);
    assert!(key_is_absent(&server, &repo_id).await);

    let res = restore_repository_key(&owner, &server, &repo_id, &new).await;
    let status = res.status();
    let text = res.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    texts.push(text);
    assert_eq!(
        &held_repository_key(&server, &repo_id).await,
        repo_key.as_bytes()
    );

    let newer = rotate(owner.clone(), rotate_url).await;
    let bytes = memory
        .get(&format!("{}config-3.json", root(&server)))
        .await
        .unwrap();
    let config: RepoConfig = serde_json::from_slice(&bytes).unwrap();
    let opened = unwrap_repo_key(
        &config.wrapped_repo_key,
        &RecoveryKey::parse(&newer).unwrap(),
        &repo_id,
    )
    .unwrap();
    assert_eq!(opened.as_bytes(), repo_key.as_bytes());

    assert_no_key_anywhere(&server, &[&old, &new], &repo_key, &texts).await;
}

/// `audit-log-format.md` section 4: `recovery_key` is redacted at any depth,
/// and the request's `Debug` prints no key.
#[test]
fn a_recovery_key_is_redacted_in_the_audit_and_in_debug_output() {
    let key = RecoveryKey::generate().to_text();
    let redacted = quoro_control_plane::audit::redact(&json!({
        "recovery_key": key,
        "nested": [{ "recovery_key": key }],
        "name": "kept",
    }));
    assert_eq!(
        redacted,
        json!({
            "recovery_key": "[redacted]",
            "nested": [{ "recovery_key": "[redacted]" }],
            "name": "kept",
        })
    );
    let request: quoro_control_plane::api::backups::RestoreBackupRepositoryKeyRequest =
        serde_json::from_value(json!({ "recovery_key": key })).unwrap();
    let printed = format!("{request:?}");
    assert!(!printed.contains(&key), "{printed}");
    assert_eq!(request.recovery_key.expose(), key);
    // Only an object is the request: an array, which a derived
    // `Deserialize` reads into a struct, and a bare string are not.
    type Request = quoro_control_plane::api::backups::RestoreBackupRepositoryKeyRequest;
    assert!(serde_json::from_value::<Request>(json!([key])).is_err());
    assert!(serde_json::from_value::<Request>(json!(key)).is_err());
    assert!(serde_json::from_str::<Request>(&format!("[\"{key}\"]")).is_err());
    let extra: Request =
        serde_json::from_value(json!({ "recovery_key": key, "note": [1, { "a": null }] })).unwrap();
    assert_eq!(extra.recovery_key.expose(), key);
    let parsed = RecoveryKey::parse(&key).unwrap();
    assert!(!format!("{parsed:?}").contains(&key));
}

#[tokio::test]
async fn a_moved_lease_fails_the_seal_and_a_node_failure_emits_an_event() {
    let mut setup = setup().await;
    let res = setup
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let backup_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    // The app's lease moves while the backup runs.
    sqlx::query("UPDATE leases SET fencing_token = 2 WHERE resource_id = ?")
        .bind(&setup.app_id)
        .execute(setup.server.state.store.pool())
        .await
        .unwrap();
    let data = vec![3u8; 128 * 1024];
    let (key, digest, logical_bytes, chunk_count) = write_manifest(&setup, &backup_id, &data).await;
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
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
            }],
        ),
    )
    .await;
    let failed = wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &backup_id,
        "failed",
    )
    .await;
    assert_eq!(failed["error"], "lease_mismatch");
    let seal_key = format!(
        "{}{}",
        root(&setup.server),
        layout::seal(&setup.app_id, &backup_id)
    );
    assert_eq!(
        setup.memory.get(&seal_key).await,
        Err(ObjectStoreError::NotFound)
    );

    // A node that reports `failed` ends the backup with its reason.
    let res = setup
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let second = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
                backup_id: second.clone(),
                status: BackupState::Failed,
                snapshot_method: None,
                pause_ms: None,
                guest_flush: None,
                manifest_key: None,
                manifest_digest: None,
                logical_bytes: None,
                uploaded_bytes: None,
                chunk_count: None,
                error: Some("the snapshot could not be read".to_string()),
                started_at: None,
                finished_at: None,
            }],
        ),
    )
    .await;
    let failed = wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &second,
        "failed",
    )
    .await;
    assert_eq!(failed["error"], "the snapshot could not be read");
    let emitted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM events WHERE type = 'backup.failed'")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(emitted, 2);
}

#[tokio::test]
async fn a_refused_seal_put_fails_a_node_backup_at_once() {
    let mut setup = setup().await;
    let res = setup
        .owner
        .post(
            setup
                .server
                .url(&format!("/api/v1/apps/{}/backups", setup.app_id)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let backup_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();

    // The node's objects land; only the primary's seal put is refused.
    let data = vec![5u8; 128 * 1024];
    let (key, digest, logical_bytes, chunk_count) = write_manifest(&setup, &backup_id, &data).await;
    setup
        .server
        .state
        .backups
        .set_factory(Arc::new(SealFailBuckets(Arc::clone(&setup.memory))));
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
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
            }],
        ),
    )
    .await;

    let failed = wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &backup_id,
        "failed",
    )
    .await;
    let error = failed["error"].as_str().expect("a one-line reason");
    assert!(!error.is_empty() && !error.contains('\n'), "{error:?}");
    assert!(error.starts_with("cannot write the seal:"), "{error}");
    assert_eq!(failed["manifest_digest"], Value::Null);

    let (result, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log WHERE operation = 'backup.seal' \
         AND target_id = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&backup_id)
    .fetch_one(setup.server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "error");
    assert_eq!(reason.as_deref(), Some(error));
}

#[tokio::test]
async fn a_backup_past_24_hours_fails_with_timeout_and_the_schedule_waits() {
    let mut setup = setup().await;
    // The app is `cold` and its schedule counts from its last succeeded backup.
    sqlx::query("UPDATE apps SET ha_policy = 'cold', backup_schedule = ? WHERE id = ?")
        .bind(r#"{"interval_h":1,"keep":2}"#)
        .bind(&setup.app_id)
        .execute(setup.server.state.store.pool())
        .await
        .unwrap();

    let now = time::OffsetDateTime::now_utc();
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, now)
        .await
        .unwrap();
    assert_eq!(tick.queued, 1);
    let row: (String, String) =
        sqlx::query_as("SELECT id, \"trigger\" FROM backups ORDER BY created_at DESC LIMIT 1")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(row.1, "schedule");
    let dispatched: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'backup.dispatch'")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(dispatched, 1);

    // A backup not terminal after 24 hours becomes `failed` with `timeout`.
    let later = now + time::Duration::hours(25);
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, later)
        .await
        .unwrap();
    assert_eq!(tick.timed_out, 1);
    let failed = wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &row.0,
        "failed",
    )
    .await;
    assert_eq!(failed["error"], "timeout");

    // The same tick that timed the first one out queued a retry, because the
    // next interval had already passed; it stays active, so no second retry.
    let retry: String =
        sqlx::query_scalar("SELECT id FROM backups WHERE id <> ? ORDER BY created_at DESC LIMIT 1")
            .bind(&row.0)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, later)
        .await
        .unwrap();
    assert_eq!(tick.queued, 0);

    // A failed scheduled backup is not retried before the next interval. Mark
    // the retry failed through the node, then tick shortly after.
    let snapshot = recv_type(&mut setup.socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut setup.socket,
        status_report(
            rev,
            Vec::new(),
            vec![wire::BackupStatus {
                backup_id: retry.clone(),
                status: BackupState::Failed,
                snapshot_method: None,
                pause_ms: None,
                guest_flush: None,
                manifest_key: None,
                manifest_digest: None,
                logical_bytes: None,
                uploaded_bytes: None,
                chunk_count: None,
                error: Some("the snapshot could not be read".to_string()),
                started_at: None,
                finished_at: None,
            }],
        ),
    )
    .await;
    wait_for_backup(
        &setup.owner,
        &setup.server,
        &mut setup.socket,
        &retry,
        "failed",
    )
    .await;
    let soon = later + time::Duration::minutes(30);
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, soon)
        .await
        .unwrap();
    assert_eq!(tick.queued, 0);
    let next = later + time::Duration::hours(2);
    let tick = quoro_control_plane::backups::dispatch::run(&setup.server.state, next)
        .await
        .unwrap();
    assert_eq!(tick.queued, 1);
}

// ---------------------------------------------------------------------------
// Retention and garbage collection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pruning_keeps_two_and_removes_the_oldest_seal_and_manifest() {
    let mut setup = setup().await;
    sqlx::query("UPDATE apps SET backup_schedule = ? WHERE id = ?")
        .bind(r#"{"interval_h":24,"keep":2}"#)
        .bind(&setup.app_id)
        .execute(setup.server.state.store.pool())
        .await
        .unwrap();

    let mut ids = Vec::new();
    for index in 0..3u8 {
        let data = vec![index.wrapping_add(11); 200 * 1024];
        ids.push(run_backup(&mut setup, &data).await);
    }
    let oldest = &ids[0];
    // Pruning runs after the third backup's row is `succeeded`, so the
    // test waits for its commit, which sets `pruned_at` last.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let pruned_at: Option<String> =
            sqlx::query_scalar("SELECT pruned_at FROM backups WHERE id = ?")
                .bind(oldest)
                .fetch_one(setup.server.state.store.pool())
                .await
                .unwrap();
        if pruned_at.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the oldest backup is pruned"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let old_seal = format!(
        "{}{}",
        root(&setup.server),
        layout::seal(&setup.app_id, oldest)
    );
    assert_eq!(
        setup.memory.get(&old_seal).await,
        Err(ObjectStoreError::NotFound)
    );
    let old_manifest = format!(
        "{}{}",
        root(&setup.server),
        layout::manifest(&setup.app_id, oldest)
    );
    assert_eq!(
        setup.memory.get(&old_manifest).await,
        Err(ObjectStoreError::NotFound)
    );
    for kept in &ids[1..] {
        let seal = format!(
            "{}{}",
            root(&setup.server),
            layout::seal(&setup.app_id, kept)
        );
        assert!(setup.memory.get(&seal).await.is_ok(), "{kept} is kept");
        let row: Option<String> = sqlx::query_scalar("SELECT pruned_at FROM backups WHERE id = ?")
            .bind(kept)
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
        assert!(row.is_none());
    }
    let prunes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE operation = 'backup.prune'")
            .fetch_one(setup.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(prunes, 1);
}

#[tokio::test]
async fn a_sweep_deletes_only_dead_old_chunks() {
    let mut setup = setup().await;
    let data = vec![9u8; 400 * 1024];
    let backup_id = run_backup(&mut setup, &data).await;

    // The manifest lists the live chunk ids.
    let keys = setup.repo_key.source_keys(&setup.app_id);
    let manifest_key = format!(
        "{}{}",
        root(&setup.server),
        layout::manifest(&setup.app_id, &backup_id)
    );
    let object = setup.memory.get(&manifest_key).await.unwrap();
    let seal_bytes = setup
        .memory
        .get(&format!(
            "{}{}",
            root(&setup.server),
            layout::seal(&setup.app_id, &backup_id)
        ))
        .await
        .unwrap();
    let seal: quoro_backup::Seal = serde_json::from_slice(&seal_bytes).unwrap();
    let manifest = quoro_backup::open_manifest(
        &keys,
        &backup_id,
        &setup.app_id,
        &seal.manifest_digest,
        &object,
    )
    .unwrap();
    let live: Vec<String> = manifest
        .chunks
        .iter()
        .map(|chunk| chunk.id.clone())
        .collect();

    let dead = ChunkId([0xaa; 32]);
    let dead_key = format!("{}{}", root(&setup.server), layout::chunk(&dead));
    setup
        .memory
        .put_new(&dead_key, b"dead".to_vec(), None)
        .await
        .unwrap();
    let young = ChunkId([0xbb; 32]);
    let young_key = format!("{}{}", root(&setup.server), layout::chunk(&young));
    setup
        .memory
        .put_new(&young_key, b"young".to_vec(), None)
        .await
        .unwrap();

    // The store has no clock: `AgedBuckets` dates the young key as fresh.
    setup
        .server
        .state
        .backups
        .set_factory(Arc::new(AgedBuckets {
            memory: Arc::clone(&setup.memory),
            young: vec![young_key.clone()],
        }));
    let counts = quoro_control_plane::backups::prune::sweep(
        &setup.server.state.store,
        &setup.server.state.data_key,
        &setup.server.state.keys,
        &setup.server.state.backups,
        &setup.repo_id,
        time::OffsetDateTime::now_utc(),
    )
    .await
    .unwrap();
    assert_eq!(counts.chunks_deleted, 1);
    assert_eq!(counts.manifests_deleted, 0);
    assert_eq!(
        setup.memory.get(&dead_key).await,
        Err(ObjectStoreError::NotFound)
    );
    assert!(
        setup.memory.get(&young_key).await.is_ok(),
        "young chunk kept"
    );
    for id in &live {
        let key = format!(
            "{}{}",
            root(&setup.server),
            layout::chunk(&ChunkId::from_hex(id).unwrap())
        );
        assert!(setup.memory.get(&key).await.is_ok(), "live chunk {id} kept");
    }
    let last_gc: String = sqlx::query_scalar(
        "SELECT params FROM audit_log WHERE operation = 'backup.gc' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(setup.server.state.store.pool())
    .await
    .unwrap();
    let last_gc: Value = serde_json::from_str(&last_gc).unwrap();
    assert_eq!(last_gc["chunks_deleted"], 1);
    assert_eq!(last_gc["manifests_deleted"], 0);
}

/// A demoted primary deletes nothing (`node-agent-protocol.md` section 7.4):
/// a sweep that runs after the demotion leaves a dead chunk in place.
#[tokio::test]
async fn a_sweep_after_the_demotion_deletes_nothing() {
    let setup = setup().await;
    let dead = ChunkId([0xaa; 32]);
    let dead_key = format!("{}{}", root(&setup.server), layout::chunk(&dead));
    setup
        .memory
        .put_new(&dead_key, b"dead".to_vec(), None)
        .await
        .unwrap();
    setup.server.state.hub.stale_term(Some(5)).await;

    let counts = quoro_control_plane::backups::prune::sweep(
        &setup.server.state.store,
        &setup.server.state.data_key,
        &setup.server.state.keys,
        &setup.server.state.backups,
        &setup.repo_id,
        time::OffsetDateTime::now_utc(),
    )
    .await
    .unwrap();
    assert_eq!(counts.chunks_deleted, 0);
    assert!(
        setup.memory.get(&dead_key).await.is_ok(),
        "the dead chunk stays"
    );
}
