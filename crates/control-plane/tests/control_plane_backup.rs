//! The control-plane database backup (`backup-format.md` section 7.2,
//! ADR 0148), with a `MemoryStore` as the bucket: the schedule, `VACUUM INTO`,
//! the chunks and manifest the restore reads, the seal, retention and the
//! failure path.

mod common;

use common::{Server, start, start_in};
use quoro_backup::{
    ChunkId, RepoKey, Restorer, Seal, SourceKind, layout, open_manifest, sign_seal, verify_seal,
};
use quoro_control_plane::backups::control_plane::{
    backup_before_upgrade, queue_if_due, record_upgrade, remove_leftovers, resolve_orphaned,
    run_control_plane_backup, upgrade_before_start,
};
use quoro_control_plane::backups::prune;
use quoro_control_plane::backups::{Backups, Bucket, BucketFactory, BucketStore, RepositoryTarget};
use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::error::ApiError;
use quoro_control_plane::keys::ClusterKeys;
use quoro_control_plane::store::Store;
use quoro_control_plane::{AppState, NodeNetwork, ReferenceAsDigest, ServerConfig};
use quoro_providers::object_store::{MemoryStore, ObjectInfo, ObjectStore, ObjectStoreError};
use serde_json::{Value, json};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const PREFIX: &str = "cp-backup-test";
const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

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

/// A factory that cannot open a bucket, for the failure path.
struct BrokenBuckets;

impl BucketFactory for BrokenBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Err(ApiError::internal(&"the bucket factory is down"))
    }
}

/// A factory whose bucket refuses the seal put only, so the chunks and the
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

/// A factory whose bucket demotes the primary as the manifest lands: the
/// demotion comes while the upload runs, after its first check
/// (`node-agent-protocol.md` section 7.4).
struct DemotingBuckets {
    bucket: Arc<MemoryStore>,
    store: Store,
}

impl BucketStore for DemotingBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move {
            let put = self.bucket.put_new(key, body, None).await.map(|_| ());
            if key.contains("manifests/") {
                let _ = self.store.demote(Some(5)).await;
            }
            put
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.bucket.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.bucket.list(prefix))
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.bucket.delete(key))
    }
}

impl BucketFactory for DemotingBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(DemotingBuckets {
            bucket: Arc::clone(&self.bucket),
            store: self.store.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

fn local_node() -> LocalNode {
    LocalNode {
        name: "test-node".to_string(),
        capacity: Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        kvm: Kvm {
            available: true,
            kernel: "6.8.0".to_string(),
            nested: Some(true),
        },
        engine_version: "test-engine".to_string(),
        agent_version: "test-agent".to_string(),
    }
}

async fn setup(bucket: &Arc<MemoryStore>) -> (Server, reqwest::Client, String, String) {
    let server = start().await;
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(bucket))));
    let owner = server.owner().await;
    let node_id = driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
    let project_id = create_project(&owner, &server, "demo").await;
    (server, owner, node_id, project_id)
}

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

/// Creates the bucket provider and the repository with `control_plane_schedule`.
async fn create_repository(
    owner: &reqwest::Client,
    server: &Server,
    interval_h: i64,
    keep: i64,
) -> String {
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
            "control_plane_schedule": { "interval_h": interval_h, "keep": keep },
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
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn at(text: &str) -> OffsetDateTime {
    OffsetDateTime::parse(text, &Rfc3339).unwrap()
}

fn root(server: &Server) -> String {
    layout::root(PREFIX, &server.state.cluster_id)
}

/// The repository key the primary stores under the cluster DEK.
async fn repo_key(server: &Server, repository_id: &str) -> RepoKey {
    let encrypted: Vec<u8> =
        sqlx::query_scalar("SELECT repo_key_enc FROM backup_repositories WHERE id = ?")
            .bind(repository_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    let secret = server
        .state
        .data_key
        .decrypt(
            "backup_repositories.repo_key_enc",
            repository_id,
            &encrypted,
        )
        .unwrap();
    let text = secret.expose();
    assert_eq!(text.len(), 64, "the stored repository key is 64 hex");
    let mut bytes = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let value = |byte: u8| (byte as char).to_digit(16).unwrap() as u8;
        bytes[index] = (value(pair[0]) << 4) | value(pair[1]);
    }
    RepoKey::from_bytes(bytes)
}

/// One `backups` row, reduced to the columns these tests assert on.
struct Row {
    status: String,
    manifest_key: Option<String>,
    manifest_digest: Option<String>,
    logical_bytes: Option<i64>,
    uploaded_bytes: Option<i64>,
    chunk_count: Option<i64>,
    error: Option<String>,
    created_by_type: String,
    created_by_id: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    pruned_at: Option<String>,
}

async fn row(server: &Server, backup_id: &str) -> Row {
    let row = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT status, manifest_key, manifest_digest, logical_bytes, uploaded_bytes, \
         chunk_count, error, created_by_type, created_by_id, started_at, finished_at, pruned_at \
         FROM backups WHERE id = ?",
    )
    .bind(backup_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    Row {
        status: row.0,
        manifest_key: row.1,
        manifest_digest: row.2,
        logical_bytes: row.3,
        uploaded_bytes: row.4,
        chunk_count: row.5,
        error: row.6,
        created_by_type: row.7,
        created_by_id: row.8,
        started_at: row.9,
        finished_at: row.10,
        pruned_at: row.11,
    }
}

/// The `cp-backup-*.sqlite` files of the data directory.
fn leftover_files(data_dir: &Path) -> Vec<String> {
    std::fs::read_dir(data_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .filter(|name| name.starts_with("cp-backup-") && name.ends_with(".sqlite"))
        .collect()
}

/// Reads one repository object.
async fn object(bucket: &MemoryStore, key: &str) -> Vec<u8> {
    bucket.get(key).await.unwrap()
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_scheduled_backup_uploads_a_restorable_database_and_seals_it() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_app(&owner, &server, &project_id, "api").await;
    let repository_id = create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now)
        .await
        .unwrap()
        .expect("a first backup is due");
    assert_eq!(queued.source_kind.as_str(), "control_plane");
    assert_eq!(queued.status.as_str(), "queued");
    assert_eq!(queued.trigger.as_str(), "schedule");
    assert_eq!(queued.node_id, node_id);
    assert_eq!(queued.repository_id, repository_id);
    assert!(queued.app_id.is_none() && queued.volume_id.is_none());
    let queued_row = row(&server, &queued.id).await;
    assert_eq!(queued_row.created_by_type, "system");
    assert_eq!(queued_row.created_by_id, "backups");
    // The dispatch record names the backup (`audit-log-format.md` section 3.5).
    let (result,): (String,) = sqlx::query_as(
        "SELECT result FROM audit_log WHERE operation = 'backup.dispatch' AND target_id = ?",
    )
    .bind(&queued.id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "ok");

    run_control_plane_backup(&server.state, &queued.id, now)
        .await
        .unwrap();

    let after = row(&server, &queued.id).await;
    assert_eq!(after.status, "succeeded", "{:?}", after.error);
    let cluster_id = server.state.cluster_id.clone();
    let manifest_key = layout::manifest(&cluster_id, &queued.id);
    assert_eq!(after.manifest_key.as_deref(), Some(manifest_key.as_str()));
    let digest = after.manifest_digest.clone().unwrap();
    assert_eq!(digest.len(), 64);
    assert!(after.started_at.is_some() && after.finished_at.is_some());
    assert!(after.logical_bytes.unwrap() > 0);
    assert!(after.chunk_count.unwrap() > 0);
    assert!(after.uploaded_bytes.unwrap() > 0);
    assert!(after.pruned_at.is_none());
    assert!(after.error.is_none());

    // The seal verifies with the cluster public key and repeats the row.
    let seal_key = format!("{}{}", root(&server), layout::seal(&cluster_id, &queued.id));
    let seal: Seal = serde_json::from_slice(&object(&bucket, &seal_key).await).unwrap();
    verify_seal(&seal, &server.state.keys.signing_key().verifying_key()).unwrap();
    assert_eq!(seal.source_kind, SourceKind::ControlPlane);
    assert_eq!(seal.source_id, cluster_id);
    assert_eq!(seal.manifest_key, manifest_key);
    assert_eq!(seal.manifest_digest, digest);
    assert_eq!(seal.logical_bytes, after.logical_bytes.unwrap() as u64);
    assert_eq!(seal.chunk_count, after.chunk_count.unwrap() as u64);
    assert!(seal.volume_id.is_none() && seal.fencing_token.is_none());

    // The manifest opens with the source keys of the cluster id.
    let keys = repo_key(&server, &repository_id)
        .await
        .source_keys(&cluster_id);
    let manifest_object = object(&bucket, &format!("{}{manifest_key}", root(&server))).await;
    let manifest = open_manifest(&keys, &queued.id, &cluster_id, &digest, &manifest_object)
        .expect("the manifest opens with the cluster's source keys");
    assert_eq!(manifest.source_kind, SourceKind::ControlPlane);
    assert_eq!(manifest.source_id, cluster_id);
    assert_eq!(manifest.logical_bytes, after.logical_bytes.unwrap() as u64);
    assert_eq!(
        manifest.chunks.len() as i64,
        after.chunk_count.unwrap(),
        "the manifest lists every chunk"
    );

    // Section 8.1: the chunks rebuild the file, which SQLite accepts.
    let restored = server.data_dir.join("restored.sqlite");
    let mut restorer = Restorer::create(keys, &restored, manifest.logical_bytes).unwrap();
    for entry in &manifest.chunks {
        let id = ChunkId::from_hex(&entry.id).unwrap();
        let bytes = object(&bucket, &format!("{}{}", root(&server), layout::chunk(&id))).await;
        restorer.apply(entry, &bytes).unwrap();
    }
    restorer.finish().unwrap();

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&restored)
        .read_only(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(integrity, vec!["ok".to_string()]);
    let (apps,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM apps")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(apps, 2, "the restored database holds the app rows");
    pool.close().await;
    std::fs::remove_file(&restored).unwrap();

    assert!(
        leftover_files(&server.data_dir).is_empty(),
        "the temporary database is gone"
    );
}

// ---------------------------------------------------------------------------
// The schedule
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_backup_inside_the_interval_is_not_due_again() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let first = queue_if_due(&server.state, now).await.unwrap().unwrap();
    run_control_plane_backup(&server.state, &first.id, now)
        .await
        .unwrap();

    // 30 minutes later the interval has not passed, and a queued row would
    // have blocked it too.
    assert!(
        queue_if_due(&server.state, now + time::Duration::minutes(30))
            .await
            .unwrap()
            .is_none()
    );
    // One interval after the last succeeded backup it is due again.
    let second = queue_if_due(&server.state, now + time::Duration::hours(1))
        .await
        .unwrap()
        .expect("due after the interval");
    assert_ne!(second.id, first.id);
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retention_keeps_two_and_removes_the_oldest_seal_and_manifest() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 2).await;
    let cluster_id = server.state.cluster_id.clone();

    let mut ids = Vec::new();
    for step in 0..3 {
        let now = at("2026-11-05T02:00:00Z") + time::Duration::hours(2 * step);
        let backup = queue_if_due(&server.state, now)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("backup {step} is due"));
        run_control_plane_backup(&server.state, &backup.id, now)
            .await
            .unwrap();
        assert_eq!(row(&server, &backup.id).await.status, "succeeded");
        ids.push(backup.id);
    }

    // Section 9: the oldest lost its seal first, then its manifest.
    let oldest = row(&server, &ids[0]).await;
    assert!(oldest.pruned_at.is_some(), "the oldest is pruned");
    assert!(row(&server, &ids[1]).await.pruned_at.is_none());
    assert!(row(&server, &ids[2]).await.pruned_at.is_none());
    assert!(
        bucket
            .get(&format!(
                "{}{}",
                root(&server),
                layout::seal(&cluster_id, &ids[0])
            ))
            .await
            .is_err(),
        "the oldest seal is deleted"
    );
    assert!(
        bucket
            .get(&format!(
                "{}{}",
                root(&server),
                layout::manifest(&cluster_id, &ids[0])
            ))
            .await
            .is_err(),
        "the oldest manifest is deleted"
    );
    assert!(
        bucket
            .get(&format!(
                "{}{}",
                root(&server),
                layout::seal(&cluster_id, &ids[2])
            ))
            .await
            .is_ok(),
        "the newest seal stays"
    );
    let (operation,): (String,) = sqlx::query_as(
        "SELECT operation FROM audit_log WHERE target_id = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&ids[0])
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(operation, "backup.prune");
}

// ---------------------------------------------------------------------------
// The failure path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_bucket_fails_the_row_and_leaves_no_temporary_file() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();
    server.state.backups.set_factory(Arc::new(BrokenBuckets));

    assert!(
        run_control_plane_backup(&server.state, &queued.id, now)
            .await
            .is_err()
    );

    let failed = row(&server, &queued.id).await;
    assert_eq!(failed.status, "failed");
    let error = failed.error.clone().expect("a one-line error");
    assert!(!error.is_empty() && !error.contains('\n'), "{error:?}");
    assert!(failed.started_at.is_some() && failed.finished_at.is_some());
    assert!(failed.manifest_key.is_none());
    assert!(leftover_files(&server.data_dir).is_empty());

    let (result, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log WHERE operation = 'backup.seal' \
         AND target_id = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&queued.id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "error");
    assert_eq!(reason.as_deref(), Some(error.as_str()));
}

/// A primary demoted while its backup uploads writes no seal: the upload
/// that was running reaches the seal after the demotion and stops there.
#[tokio::test]
async fn an_upload_running_at_the_demotion_writes_no_seal() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;
    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();
    server.state.backups.set_factory(Arc::new(DemotingBuckets {
        bucket: Arc::clone(&bucket),
        store: server.state.store.clone(),
    }));

    // The run ends without a seal; recording its end is refused as well.
    let _ = run_control_plane_backup(&server.state, &queued.id, now).await;

    assert!(server.state.store.is_demoted());
    let root = root(&server);
    let manifest = format!(
        "{root}{}",
        layout::manifest(&server.state.cluster_id, &queued.id)
    );
    assert!(
        !object(&bucket, &manifest).await.is_empty(),
        "the upload ran up to its manifest"
    );
    let seals = bucket.list(&format!("{root}seals/")).await.unwrap();
    assert!(seals.is_empty(), "no seal after the demotion: {seals:?}");
    assert_eq!(row(&server, &queued.id).await.status, "running");
}

#[tokio::test]
async fn startup_removes_a_temporary_file_a_crash_left_behind() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, _owner, _node_id, _project_id) = setup(&bucket).await;
    let left = server
        .data_dir
        .join("cp-backup-bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K.sqlite");
    std::fs::write(&left, b"half a database").unwrap();
    let kept = server.data_dir.join("quoro.db");
    assert!(kept.exists());

    remove_leftovers(&server.data_dir).unwrap();

    assert!(!left.exists(), "the leftover is removed");
    assert!(kept.exists(), "nothing else is touched");
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

async fn owner_id(owner: &reqwest::Client, server: &Server) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    me["user"]["id"].as_str().unwrap().to_string()
}

async fn create_agent(
    owner: &reqwest::Client,
    server: &Server,
    name: &str,
    owner_user_id: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": name,
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "a test agent",
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

async fn token_for(owner: &reqwest::Client, server: &Server, agent_id: &str) -> String {
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create key: {}",
        res.text().await.unwrap()
    );
    let key: Value = res.json().await.unwrap();
    let key = key["key"].as_str().unwrap().to_string();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "mint token: {}",
        res.text().await.unwrap()
    );
    let token: Value = res.json().await.unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

async fn create_grant(owner: &reqwest::Client, server: &Server, body: Value) -> Value {
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

async fn list_as_agent(server: &Server, token: &str) -> Value {
    let res = reqwest::Client::new()
        .get(server.url(LIST_FIRST_PAGE))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "list backups: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

/// The first page of `listBackups`, without a cursor.
const LIST_FIRST_PAGE: &str = "/api/v1/backups";

#[tokio::test]
async fn a_selector_that_is_not_all_does_not_list_control_plane_backups() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();

    // A human owner sees every row.
    let listed: Value = owner
        .get(server.url(LIST_FIRST_PAGE))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == queued.id),
        "the owner sees the control-plane backup: {listed}"
    );

    // An agent grant that names a project is not `{all: true}`
    // (`openapi.yaml` `listBackups`).
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        json!({
            "agent_id": agent_id,
            "scopes": ["backup:read"],
            "selector": { "projects": ["demo"] },
            "ttl_s": 3600,
            "reason": "tests",
        }),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;
    let listed = list_as_agent(&server, &token).await;
    assert!(
        listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["source_kind"] != "control_plane"),
        "a project selector never lists the control-plane backup"
    );
}

// ---------------------------------------------------------------------------
// Before an upgrade
// ---------------------------------------------------------------------------

/// The newest shipped schema version.
fn shipped() -> usize {
    quoro_control_plane::store::Store::shipped_schema_version()
}

/// The migration the next release ships: this test's stand-in, so no fake
/// migration goes into `docs/contracts/migrations/`. It moves the schema one
/// version past the newest shipped one, whatever that is.
fn extra_migrations() -> &'static [&'static str] {
    static EXTRA: std::sync::OnceLock<&'static [&'static str]> = std::sync::OnceLock::new();
    EXTRA.get_or_init(|| {
        let migration: &'static str = Box::leak(
            format!(
                "CREATE TABLE upgrade_probe (id INTEGER PRIMARY KEY);\n\
                 UPDATE meta SET value = '{}' WHERE key = 'schema_version';",
                shipped() + 1
            )
            .into_boxed_str(),
        );
        Box::leak(vec![migration].into_boxed_slice())
    })
}

/// A database the previous release left behind: the newest shipped schema, a
/// data directory that persists its keys, a repository and a primary node.
async fn previous_release(bucket: &Arc<MemoryStore>, keep: i64) -> Server {
    let dir = std::env::temp_dir().join(format!(
        "quoro-upgrade-{}",
        &quoro_control_plane::store::random_secret()[..16]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let server = start_in(dir).await;
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(bucket))));
    let owner = server.owner().await;
    driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
    create_repository(&owner, &server, 1, keep).await;
    server
}

/// The previous release's database, reopened unmigrated with one more
/// migration, plus the key material a later binary loads from the same
/// directory.
async fn upgrade_parts(server: &Server) -> (Store, Arc<DataKey>, Arc<ClusterKeys>) {
    let database = server.data_dir.join("quoro.db");
    let store = Store::open_unmigrated(&database)
        .await
        .unwrap()
        .with_extra_migrations(extra_migrations());
    let data_key = Arc::new(DataKey::load_or_create(&server.data_dir).unwrap());
    let keys = Arc::new(ClusterKeys::load_or_generate(&server.data_dir).unwrap());
    (store, data_key, keys)
}

/// The settings `common::start_in` uses, for a test that builds `AppState`
/// itself.
fn server_config(data_dir: &Path) -> ServerConfig {
    ServerConfig {
        data_dir: data_dir.to_path_buf(),
        secure_cookies: false,
        public_url: Some("https://primary.example:8443".to_string()),
        tls_fingerprint: common::TLS_FINGERPRINT.to_string(),
        tls: common::tls_identity(data_dir),
        node_network: NodeNetwork {
            subnet: "10.42.1.0/24".to_string(),
            gateway: "10.42.1.1".to_string(),
        },
        heartbeat_interval_ms: 200,
        session_timeout_ms: 30_000,
    }
}

async fn schema_version(pool: &sqlx::SqlitePool) -> String {
    sqlx::query_scalar("SELECT value FROM meta WHERE key = 'schema_version'")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn table_exists(pool: &sqlx::SqlitePool, name: &str) -> bool {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap();
    count == 1
}

async fn repository_id(server: &Server) -> String {
    sqlx::query_scalar("SELECT id FROM backup_repositories LIMIT 1")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Restores a sealed backup the way `quoro restore` does (section 8.2) and
/// returns the read-only pool over the restored file and its path.
async fn restore_seal(
    server: &Server,
    bucket: &MemoryStore,
    backup_id: &str,
    repository_id: &str,
) -> (sqlx::SqlitePool, PathBuf) {
    let cluster_id = server.state.cluster_id.clone();
    let seal_key = format!("{}{}", root(server), layout::seal(&cluster_id, backup_id));
    let seal: Seal = serde_json::from_slice(&object(bucket, &seal_key).await).unwrap();
    verify_seal(&seal, &server.state.keys.signing_key().verifying_key()).unwrap();
    let keys = repo_key(server, repository_id)
        .await
        .source_keys(&cluster_id);
    let manifest_object = object(bucket, &format!("{}{}", root(server), seal.manifest_key)).await;
    let manifest = open_manifest(
        &keys,
        backup_id,
        &cluster_id,
        &seal.manifest_digest,
        &manifest_object,
    )
    .expect("the manifest opens with the cluster's source keys");
    let path = server.data_dir.join(format!("restored-{backup_id}.sqlite"));
    let mut restorer = Restorer::create(keys, &path, manifest.logical_bytes).unwrap();
    for entry in &manifest.chunks {
        let id = ChunkId::from_hex(&entry.id).unwrap();
        let bytes = object(bucket, &format!("{}{}", root(server), layout::chunk(&id))).await;
        restorer.apply(entry, &bytes).unwrap();
    }
    restorer.finish().unwrap();

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .read_only(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    (pool, path)
}

#[tokio::test]
async fn a_pending_migration_backs_up_first_then_records_the_upgrade_backup() {
    let bucket = Arc::new(MemoryStore::new());
    let previous = previous_release(&bucket, 7).await;
    let repository_id = repository_id(&previous).await;
    let (store, data_key, keys) = upgrade_parts(&previous).await;
    assert_eq!(
        store.pending_migrations().await.unwrap(),
        Some((shipped(), shipped() + 1))
    );

    let runtime = Backups::s3();
    runtime.set_factory(Arc::new(MemoryBuckets(Arc::clone(&bucket))));
    upgrade_before_start(
        &store,
        &previous.data_dir,
        &keys,
        &data_key,
        &runtime,
        false,
    )
    .await
    .unwrap();

    // The live database is migrated, with the extra migration's effect.
    assert_eq!(
        schema_version(store.pool()).await,
        (shipped() + 1).to_string()
    );
    assert!(table_exists(store.pool(), "upgrade_probe").await);

    // One `upgrade` row, `succeeded`, with the counts the seal carries.
    use sqlx::Row as _;
    let row = sqlx::query("SELECT * FROM backups WHERE \"trigger\" = 'upgrade'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let backup_id: String = row.try_get("id").unwrap();
    let cluster_id = previous.state.cluster_id.clone();
    assert_eq!(row.try_get::<String, _>("status").unwrap(), "succeeded");
    assert_eq!(
        row.try_get::<String, _>("source_kind").unwrap(),
        "control_plane"
    );
    assert_eq!(
        row.try_get::<String, _>("created_by_type").unwrap(),
        "system"
    );
    assert_eq!(
        row.try_get::<String, _>("created_by_id").unwrap(),
        "backups"
    );
    let (node_id,): (String,) = sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>("node_id").unwrap(), node_id);
    assert_eq!(
        row.try_get::<String, _>("repository_id").unwrap(),
        repository_id
    );
    assert_eq!(
        row.try_get::<String, _>("manifest_key").unwrap(),
        layout::manifest(&cluster_id, &backup_id)
    );
    assert_eq!(
        row.try_get::<String, _>("manifest_digest").unwrap().len(),
        64
    );
    assert!(row.try_get::<i64, _>("logical_bytes").unwrap() > 0);
    assert!(row.try_get::<i64, _>("uploaded_bytes").unwrap() > 0);
    assert!(row.try_get::<i64, _>("chunk_count").unwrap() > 0);
    assert!(
        row.try_get::<Option<String>, _>("pruned_at")
            .unwrap()
            .is_none()
    );
    assert!(row.try_get::<Option<String>, _>("error").unwrap().is_none());

    // Both system records name the backup.
    let operations: Vec<String> =
        sqlx::query_scalar("SELECT operation FROM audit_log WHERE target_id = ? ORDER BY seq")
            .bind(&backup_id)
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert!(
        operations.contains(&"backup.dispatch".to_string()),
        "{operations:?}"
    );
    assert!(
        operations.contains(&"backup.seal".to_string()),
        "{operations:?}"
    );
    let results: Vec<String> = sqlx::query_scalar(
        "SELECT result FROM audit_log WHERE target_id = ? AND operation IN ('backup.dispatch', 'backup.seal')",
    )
    .bind(&backup_id)
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(results, vec!["ok".to_string(), "ok".to_string()]);

    // The seal verifies, and the restored database is the one before the
    // migration: the extra migration's effect is absent there.
    let (restored, path) = restore_seal(&previous, &bucket, &backup_id, &repository_id).await;
    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&restored)
        .await
        .unwrap();
    assert_eq!(integrity, vec!["ok".to_string()]);
    assert_eq!(
        schema_version(&restored).await,
        shipped().to_string(),
        "the backup is the database before the migration"
    );
    assert!(!table_exists(&restored, "upgrade_probe").await);
    restored.close().await;
    std::fs::remove_file(&path).unwrap();

    assert!(
        leftover_files(&previous.data_dir).is_empty(),
        "the temporary database is gone"
    );
}

#[tokio::test]
async fn a_failed_pre_upgrade_backup_stops_the_start_and_leaves_the_database_alone() {
    let bucket = Arc::new(MemoryStore::new());
    let previous = previous_release(&bucket, 7).await;
    let (store, data_key, _keys) = upgrade_parts(&previous).await;
    let runtime = Backups::s3();
    runtime.set_factory(Arc::new(BrokenBuckets));

    let error = match AppState::open_with_upgrade(
        store.clone(),
        server_config(&previous.data_dir),
        Arc::new(ReferenceAsDigest),
        data_key,
        runtime,
        false,
    )
    .await
    {
        Ok(_) => panic!("the start must fail when the backup cannot run"),
        Err(error) => error,
    };
    assert!(
        error.starts_with("the pre-upgrade backup failed: "),
        "{error}"
    );
    assert!(
        error.ends_with("; start once with --skip-upgrade-backup to migrate without it"),
        "{error}"
    );
    assert!(!error.contains('\n'), "one line: {error:?}");

    // No migration ran, no row exists and no temporary file is left.
    assert_eq!(schema_version(store.pool()).await, shipped().to_string());
    assert!(!table_exists(store.pool(), "upgrade_probe").await);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    assert!(leftover_files(&previous.data_dir).is_empty());
}

#[tokio::test]
async fn the_skip_flag_migrates_without_a_backup_and_records_the_skip() {
    let bucket = Arc::new(MemoryStore::new());
    let previous = previous_release(&bucket, 7).await;
    let repository_id = repository_id(&previous).await;
    let (store, data_key, keys) = upgrade_parts(&previous).await;
    let runtime = Backups::s3();
    // A factory that cannot open a bucket: the skip must upload nothing.
    runtime.set_factory(Arc::new(BrokenBuckets));

    upgrade_before_start(&store, &previous.data_dir, &keys, &data_key, &runtime, true)
        .await
        .unwrap();

    assert_eq!(
        schema_version(store.pool()).await,
        (shipped() + 1).to_string()
    );
    assert!(table_exists(store.pool(), "upgrade_probe").await);
    let backups: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(backups, 0, "no backup was taken");

    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT actor_id, target_type, target_id, params FROM audit_log \
         WHERE operation = 'backup.upgrade_skipped'",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "one record: {rows:?}");
    let (actor, target_type, target_id, params) = &rows[0];
    assert_eq!(actor, "backups");
    assert_eq!(target_type, "backup_repository");
    assert_eq!(target_id.as_deref(), Some(repository_id.as_str()));
    let params: Value = serde_json::from_str(params).unwrap();
    assert_eq!(params["from_schema_version"], shipped());
    assert_eq!(params["to_schema_version"], shipped() + 1);
}

#[tokio::test]
async fn a_start_with_nothing_pending_takes_no_backup_with_or_without_the_flag() {
    let dir = std::env::temp_dir().join(format!(
        "quoro-upgrade-{}",
        &quoro_control_plane::store::random_secret()[..16]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("quoro.db");
    let store = Store::open(&path).await.unwrap();
    let data_key = Arc::new(DataKey::load_or_create(&dir).unwrap());
    let keys = Arc::new(ClusterKeys::load_or_generate(&dir).unwrap());
    let runtime = Backups::s3();
    runtime.set_factory(Arc::new(BrokenBuckets));

    for skip in [false, true] {
        upgrade_before_start(&store, &dir, &keys, &data_key, &runtime, skip)
            .await
            .unwrap();
    }

    let backups: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(backups, 0);
    let skipped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE operation = 'backup.upgrade_skipped'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(skipped, 0, "the flag with nothing pending skips nothing");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn pending_migrations_with_no_repository_migrate_without_a_backup() {
    for skip in [false, true] {
        no_repository_upgrade(skip).await;
    }
}

/// One start of a database with a pending migration and no repository, with
/// or without the operator's flag: it migrates and writes no backup.
async fn no_repository_upgrade(skip: bool) {
    let dir = std::env::temp_dir().join(format!(
        "quoro-upgrade-{}",
        &quoro_control_plane::store::random_secret()[..16]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("quoro.db");
    // The previous release's database: newest shipped schema, no repository.
    drop(Store::open(&path).await.unwrap());
    let store = Store::open_unmigrated(&path)
        .await
        .unwrap()
        .with_extra_migrations(extra_migrations());
    assert!(store.pending_migrations().await.unwrap().is_some());
    let data_key = Arc::new(DataKey::load_or_create(&dir).unwrap());
    let keys = Arc::new(ClusterKeys::load_or_generate(&dir).unwrap());
    let runtime = Backups::s3();
    runtime.set_factory(Arc::new(MemoryBuckets(Arc::new(MemoryStore::new()))));

    upgrade_before_start(&store, &dir, &keys, &data_key, &runtime, skip)
        .await
        .unwrap();
    // The flag with no repository skips nothing and writes no record.
    let skipped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE operation = 'backup.upgrade_skipped'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(skipped, 0);

    assert_eq!(
        schema_version(store.pool()).await,
        (shipped() + 1).to_string()
    );
    assert!(table_exists(store.pool(), "upgrade_probe").await);
    let backups: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(backups, 0, "there is nothing to write to");
    std::fs::remove_dir_all(&dir).unwrap();
}

// ---------------------------------------------------------------------------
// Retention of upgrade backups (ADR 0158)
// ---------------------------------------------------------------------------

/// One scheduled control-plane backup at `now`, through the usual path.
async fn scheduled_backup(server: &Server, now: OffsetDateTime) -> String {
    let backup = queue_if_due(&server.state, now)
        .await
        .unwrap()
        .expect("a backup is due");
    run_control_plane_backup(&server.state, &backup.id, now)
        .await
        .unwrap();
    backup.id
}

/// One upgrade backup at `now`, recorded as the upgrade step records it.
async fn upgrade_backup(server: &Server, now: OffsetDateTime) -> String {
    let sealed = backup_before_upgrade(
        &server.state.store,
        &server.data_dir,
        &server.state.keys,
        &server.state.data_key,
        &server.state.backups,
        now,
    )
    .await
    .unwrap()
    .expect("a repository and a primary node exist");
    record_upgrade(
        &server.state.store,
        &server.state.keys,
        &server.state.data_key,
        &server.state.backups,
        &sealed,
        now,
    )
    .await
    .unwrap();
    // The ids order by time, and a test creates backups faster than a
    // millisecond.
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    sealed.backup_id
}

async fn pruned_at(server: &Server, backup_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT pruned_at FROM backups WHERE id = ?")
        .bind(backup_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn retention_counts_scheduled_and_upgrade_backups_apart() {
    let bucket = Arc::new(MemoryStore::new());
    let server = previous_release(&bucket, 2).await;

    let start = at("2026-11-05T02:00:00Z");
    let mut scheduled = Vec::new();
    for step in 0..3 {
        scheduled.push(scheduled_backup(&server, start + time::Duration::hours(2 * step)).await);
    }
    let mut upgrades = Vec::new();
    for step in 0..2 {
        upgrades.push(upgrade_backup(&server, start + time::Duration::hours(6 + step)).await);
    }

    // `keep: 2` prunes only the scheduled backups.
    assert!(
        pruned_at(&server, &scheduled[0]).await.is_some(),
        "the oldest scheduled backup is pruned"
    );
    assert!(pruned_at(&server, &scheduled[1]).await.is_none());
    assert!(pruned_at(&server, &scheduled[2]).await.is_none());
    // Upgrade backups are counted on their own, and two are under the three
    // the contract keeps.
    for id in &upgrades {
        assert!(
            pruned_at(&server, id).await.is_none(),
            "upgrade backup {id} stays"
        );
    }
}

#[tokio::test]
async fn four_upgrade_backups_prune_only_the_oldest_upgrade_one() {
    let bucket = Arc::new(MemoryStore::new());
    let server = previous_release(&bucket, 2).await;

    let start = at("2026-11-05T02:00:00Z");
    let mut scheduled = Vec::new();
    for step in 0..2 {
        scheduled.push(scheduled_backup(&server, start + time::Duration::hours(2 * step)).await);
    }
    let mut upgrades = Vec::new();
    for step in 0..4 {
        upgrades.push(upgrade_backup(&server, start + time::Duration::hours(6 + step)).await);
    }

    assert!(
        pruned_at(&server, &upgrades[0]).await.is_some(),
        "the oldest upgrade backup is pruned"
    );
    for id in &upgrades[1..] {
        assert!(pruned_at(&server, id).await.is_none(), "{id} stays");
    }
    for id in &scheduled {
        assert!(
            pruned_at(&server, id).await.is_none(),
            "an upgrade backup never prunes a scheduled one"
        );
    }
}

#[tokio::test]
async fn a_sweep_keeps_the_chunks_of_the_kept_upgrade_backups() {
    let bucket = Arc::new(MemoryStore::new());
    let server = previous_release(&bucket, 7).await;
    let repository_id = repository_id(&server).await;
    let cluster_id = server.state.cluster_id.clone();

    let start = at("2026-11-05T02:00:00Z");
    let mut upgrades = Vec::new();
    for step in 0..4 {
        upgrades.push(upgrade_backup(&server, start + time::Duration::hours(step)).await);
    }
    assert!(pruned_at(&server, &upgrades[0]).await.is_some());

    // A chunk no manifest references, written long enough ago for the sweep.
    let dead = format!("{}chunks/deadbeef", root(&server));
    bucket.put_new(&dead, vec![1u8; 64], None).await.unwrap();

    // 49 hours on: everything is older than the 48-hour grace, so the sweep
    // deletes what no seal references.
    let later = time::OffsetDateTime::now_utc() + time::Duration::hours(49);
    let counts = prune::sweep(
        &server.state.store,
        &server.state.data_key,
        &server.state.keys,
        &server.state.backups,
        &repository_id,
        later,
    )
    .await
    .unwrap();
    assert!(counts.chunks_deleted >= 1, "{counts:?}");
    assert!(bucket.get(&dead).await.is_err(), "the dead chunk is gone");

    // The kept upgrade backup still restores: its seal and every chunk of its
    // manifest survived the sweep.
    let kept = &upgrades[3];
    let (restored, path) = restore_seal(&server, &bucket, kept, &repository_id).await;
    let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&restored)
        .await
        .unwrap();
    assert_eq!(integrity, vec!["ok".to_string()]);
    restored.close().await;
    std::fs::remove_file(&path).unwrap();
    assert!(
        bucket
            .get(&format!(
                "{}{}",
                root(&server),
                layout::seal(&cluster_id, kept)
            ))
            .await
            .is_ok(),
        "the kept seal is there"
    );
}

// ---------------------------------------------------------------------------
// The seal put
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_seal_put_fails_the_scheduled_backup_at_once() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();
    server
        .state
        .backups
        .set_factory(Arc::new(SealFailBuckets(Arc::clone(&bucket))));

    // A seal that cannot be written fails the row itself; the run has nothing
    // left to report.
    run_control_plane_backup(&server.state, &queued.id, now)
        .await
        .unwrap();

    let failed = row(&server, &queued.id).await;
    assert_eq!(failed.status, "failed");
    let error = failed.error.clone().expect("a one-line error");
    assert!(!error.is_empty() && !error.contains('\n'), "{error:?}");
    assert!(error.starts_with("cannot write the seal:"), "{error}");
    assert!(failed.manifest_key.is_none());
    assert!(leftover_files(&server.data_dir).is_empty());

    let (result, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log WHERE operation = 'backup.seal' \
         AND target_id = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&queued.id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "error");
    assert_eq!(reason.as_deref(), Some(error.as_str()));
}

// ---------------------------------------------------------------------------
// Rows left behind at start (`backup-format.md` section 7.2)
// ---------------------------------------------------------------------------

/// A factory whose bucket fails every read of a seal with a transport error,
/// not a not-found.
struct SealGetFailBuckets(Arc<MemoryStore>);

impl BucketStore for SealGetFailBuckets {
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
        Box::pin(async move {
            if key.contains("/seals/") {
                return Err(ObjectStoreError::Transport(
                    "the seal get is down".to_string(),
                ));
            }
            self.0.get(key).await
        })
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

impl BucketFactory for SealGetFailBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(SealGetFailBuckets(Arc::clone(&self.0))))
    }
}

/// Puts the row back as `VACUUM INTO` copied it: `running`, with none of the
/// fields the seal step writes.
async fn set_running(server: &Server, backup_id: &str) {
    sqlx::query(
        "UPDATE backups SET status = 'running', manifest_key = NULL, manifest_digest = NULL, \
         logical_bytes = NULL, uploaded_bytes = NULL, chunk_count = NULL, finished_at = NULL \
         WHERE id = ?",
    )
    .bind(backup_id)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// The repository key of the seal of `backup_id`.
fn seal_key(server: &Server, backup_id: &str) -> String {
    format!(
        "{}{}",
        root(server),
        layout::seal(&server.state.cluster_id, backup_id)
    )
}

/// The `result` and `result_detail` of every `backup.seal` record of a backup,
/// oldest first.
async fn seal_records(server: &Server, backup_id: &str) -> Vec<(String, Option<String>)> {
    sqlx::query_as(
        "SELECT result, result_detail FROM audit_log WHERE operation = 'backup.seal' \
         AND target_id = ? ORDER BY seq",
    )
    .bind(backup_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

/// How many events of `kind` name `backup_id`.
async fn event_count(server: &Server, kind: &str, backup_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM events WHERE type = ? AND json_extract(subject, '$.id') = ?",
    )
    .bind(kind)
    .bind(backup_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
}

/// A sealed control-plane backup whose row a restored database lists as
/// `running`, and its seal.
async fn restored_from_backup(
    server: &Server,
    bucket: &MemoryStore,
    now: OffsetDateTime,
) -> (String, Seal) {
    let backup_id = scheduled_backup(server, now).await;
    set_running(server, &backup_id).await;
    let seal: Seal =
        serde_json::from_slice(&object(bucket, &seal_key(server, &backup_id)).await).unwrap();
    (backup_id, seal)
}

#[tokio::test]
async fn a_running_row_whose_seal_exists_is_recorded_succeeded_from_the_seal() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let (backup_id, seal) = restored_from_backup(&server, &bucket, now).await;
    let started_at = row(&server, &backup_id).await.started_at;
    assert!(started_at.is_some());
    let changes_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM change_log WHERE entity_type = 'backup' AND entity_id = ?",
    )
    .bind(&backup_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    // The bug: the row blocks the schedule.
    assert!(
        queue_if_due(&server.state, now + time::Duration::hours(2))
            .await
            .unwrap()
            .is_none(),
        "a running row blocks the schedule"
    );

    resolve_orphaned(&server.state).await.unwrap();

    let after = row(&server, &backup_id).await;
    assert_eq!(after.status, "succeeded", "{:?}", after.error);
    assert_eq!(
        after.manifest_key.as_deref(),
        Some(seal.manifest_key.as_str())
    );
    assert_eq!(
        after.manifest_key.as_deref(),
        Some(layout::manifest(&server.state.cluster_id, &backup_id).as_str())
    );
    assert_eq!(
        after.manifest_digest.as_deref(),
        Some(seal.manifest_digest.as_str())
    );
    assert_eq!(after.logical_bytes, Some(seal.logical_bytes as i64));
    assert_eq!(after.chunk_count, Some(seal.chunk_count as i64));
    assert_eq!(after.uploaded_bytes, None, "uploaded_bytes stays null");
    assert_eq!(after.finished_at.as_deref(), Some(seal.sealed_at.as_str()));
    assert_eq!(after.started_at, started_at, "started_at is unchanged");
    assert!(after.error.is_none());

    // The records of a sealed backup: one more change-log entry, a second
    // `backup.seal` with `ok`, and the event.
    let changes_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM change_log WHERE entity_type = 'backup' AND entity_id = ?",
    )
    .bind(&backup_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(changes_after, changes_before + 1);
    let records = seal_records(&server, &backup_id).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[1], ("ok".to_string(), None));
    assert_eq!(
        event_count(&server, "backup.succeeded", &backup_id).await,
        2
    );

    // The seal object was not written again.
    let stored: Seal =
        serde_json::from_slice(&object(&bucket, &seal_key(&server, &backup_id)).await).unwrap();
    assert_eq!(stored, seal);

    // The schedule counts from the resolved row.
    assert!(
        queue_if_due(&server.state, now + time::Duration::minutes(30))
            .await
            .unwrap()
            .is_none(),
        "inside the interval nothing is due"
    );
    let next = queue_if_due(&server.state, now + time::Duration::hours(2))
        .await
        .unwrap()
        .expect("due once the interval passed");
    assert_ne!(next.id, backup_id);
}

#[tokio::test]
async fn a_running_row_without_a_seal_fails_as_interrupted() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();
    // The primary crashed after it marked the row running.
    sqlx::query("UPDATE backups SET status = 'running', started_at = ? WHERE id = ?")
        .bind("2026-11-05T02:00:00.000Z")
        .bind(&queued.id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    resolve_orphaned(&server.state).await.unwrap();

    let failed = row(&server, &queued.id).await;
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error.as_deref(), Some("interrupted"));
    assert!(failed.finished_at.is_some() && failed.manifest_key.is_none());
    assert_eq!(
        seal_records(&server, &queued.id).await,
        vec![("error".to_string(), Some("interrupted".to_string()))]
    );
    assert_eq!(event_count(&server, "backup.failed", &queued.id).await, 1);
    assert!(
        queue_if_due(&server.state, now + time::Duration::hours(1))
            .await
            .unwrap()
            .is_some(),
        "the failed row no longer blocks the schedule"
    );
}

#[tokio::test]
async fn a_seal_signed_by_another_key_fails_the_row() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let (backup_id, mut seal) = restored_from_backup(&server, &bucket, now).await;
    let other_dir = server.data_dir.join("other-cluster");
    let other = ClusterKeys::load_or_generate(&other_dir).unwrap();
    sign_seal(&mut seal, other.signing_key()).unwrap();
    let key = seal_key(&server, &backup_id);
    bucket.delete(&key).await.unwrap();
    bucket
        .put_new(&key, serde_json::to_vec(&seal).unwrap(), None)
        .await
        .unwrap();

    resolve_orphaned(&server.state).await.unwrap();

    let failed = row(&server, &backup_id).await;
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error.as_deref(), Some("the seal does not verify"));
    assert!(failed.manifest_key.is_none());
    let records = seal_records(&server, &backup_id).await;
    assert_eq!(
        records.last(),
        Some(&(
            "error".to_string(),
            Some("the seal does not verify".to_string())
        ))
    );
}

#[tokio::test]
async fn a_valid_seal_of_another_backup_fails_the_row() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let first = scheduled_backup(&server, now).await;
    let (second, _) = restored_from_backup(&server, &bucket, now + time::Duration::hours(2)).await;
    // The second backup's seal key holds the first backup's seal, which
    // verifies with the cluster key but names another backup.
    let key = seal_key(&server, &second);
    let foreign = object(&bucket, &seal_key(&server, &first)).await;
    bucket.delete(&key).await.unwrap();
    bucket.put_new(&key, foreign, None).await.unwrap();

    resolve_orphaned(&server.state).await.unwrap();

    let failed = row(&server, &second).await;
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error.as_deref(), Some("the seal does not verify"));
    assert_eq!(row(&server, &first).await.status, "succeeded");
}

#[tokio::test]
async fn a_bucket_that_cannot_be_read_leaves_the_row_running() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, _node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    create_repository(&owner, &server, 1, 7).await;

    let now = at("2026-11-05T02:00:00Z");
    let (backup_id, _seal) = restored_from_backup(&server, &bucket, now).await;
    let records_before = seal_records(&server, &backup_id).await;
    server
        .state
        .backups
        .set_factory(Arc::new(SealGetFailBuckets(Arc::clone(&bucket))));

    resolve_orphaned(&server.state).await.unwrap();

    let after = row(&server, &backup_id).await;
    assert_eq!(after.status, "running");
    assert!(after.error.is_none() && after.finished_at.is_none());
    assert_eq!(seal_records(&server, &backup_id).await, records_before);

    // A factory that cannot open the bucket leaves it too.
    server.state.backups.set_factory(Arc::new(BrokenBuckets));
    resolve_orphaned(&server.state).await.unwrap();
    assert_eq!(row(&server, &backup_id).await.status, "running");
}

#[tokio::test]
async fn a_running_volume_backup_is_left_alone() {
    let bucket = Arc::new(MemoryStore::new());
    let (server, owner, node_id, project_id) = setup(&bucket).await;
    create_app(&owner, &server, &project_id, "web").await;
    let repository_id = create_repository(&owner, &server, 1, 7).await;

    let volume_backup = "bkp_01JC9Z1A2B3C4D5E6F7G8H9J0V";
    sqlx::query(
        "INSERT INTO backups (id, repository_id, source_kind, app_id, volume_id, node_id, status, \
         \"trigger\", fencing_token, created_by_type, created_by_id, created_at, started_at) \
         VALUES (?, ?, 'volume', 'app_01JC9Z1A2B3C4D5E6F7G8H9J0A', 'vol_01JC9Z1A2B3C4D5E6F7G8H9J0B', \
         ?, 'running', 'schedule', 1, 'system', 'backups', ?, ?)",
    )
    .bind(volume_backup)
    .bind(&repository_id)
    .bind(&node_id)
    .bind("2026-11-05T01:00:00.000Z")
    .bind("2026-11-05T01:00:01.000Z")
    .execute(server.state.store.pool())
    .await
    .unwrap();
    // A queued control-plane row in the same start shows the resolver ran.
    let now = at("2026-11-05T02:00:00Z");
    let queued = queue_if_due(&server.state, now).await.unwrap().unwrap();

    resolve_orphaned(&server.state).await.unwrap();

    let failed = row(&server, &queued.id).await;
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error.as_deref(), Some("interrupted"));
    let volume = row(&server, volume_backup).await;
    assert_eq!(volume.status, "running");
    assert!(volume.error.is_none() && volume.finished_at.is_none());
    assert!(seal_records(&server, volume_backup).await.is_empty());
}
