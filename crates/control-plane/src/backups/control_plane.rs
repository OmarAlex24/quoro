// SPDX-License-Identifier: AGPL-3.0-only
//! The control-plane database backup (`backup-format.md` section 7.2,
//! ADR 0148).
//!
//! The primary runs `VACUUM INTO` into a temporary file in its data directory,
//! outside any open transaction, and feeds that file through the same chunk,
//! upload and seal steps a node runs for a volume (`backup-format.md` sections
//! 5, 6.1 and 7.1 step 4), with the cluster id as `source_id`. It runs on the
//! repository's `control_plane_schedule`; nothing here notifies a node,
//! because the primary does the work itself.
//!
//! The temporary file is removed on every path, success or error: [`TempBackup`]
//! drops it. A crash can still leave one behind, so [`remove_leftovers`] sweeps
//! the data directory when the backup loop starts. The backup row a crash left
//! `queued` or `running`, or that a database restored from that very backup
//! lists as `running`, is resolved from its seal object before the loop's
//! first tick ([`resolve_orphaned`]); one whose bucket cannot be read stays
//! and times out after 24 hours through [`super::seal::time_out`].
//!
//! Before an upgrade the same work runs against the old schema, before the
//! pending migrations, with `trigger: upgrade`
//! ([`backup_before_upgrade`], [`upgrade_before_start`]). It writes no row then:
//! the seal carries what [`record_upgrade`] writes into the migrated database.

use crate::AppState;
use crate::api::backups::{
    BACKUP_COLUMNS, Backup, BackupRecord, BackupSourceKind, BackupStatus, BackupTrigger,
    backup_from_row, backup_payload,
};
use crate::changelog::{self, Actor, Op};
use crate::crypto::DataKey;
use crate::error::ApiError;
use crate::keys::ClusterKeys;
use crate::store::Store;
use quoro_backup::{BackupWriter, Chunker, Manifest, SourceKeys, SourceKind, chunks, layout};
use quoro_proto::wire;
use quoro_providers::object_store::ObjectStoreError;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

use super::{Backups, Bucket, RepositoryTarget, dispatch, object_key, repo, seal, timestamp};

/// Prefix of the temporary database file in the data directory.
const TEMP_PREFIX: &str = "cp-backup-";
/// Suffix of the temporary database file in the data directory.
const TEMP_SUFFIX: &str = ".sqlite";

/// Queues a control-plane backup when the repository's schedule says one is
/// due, and returns the `queued` row as the API shows it
/// (`backup-format.md` section 7.2).
///
/// Due means: a repository exists, no control-plane backup is `queued` or
/// `running`, and the newest `succeeded` or `failed` one is older than
/// `control_plane_schedule.interval_h`, or none exists. A primary with no node
/// row logs a warning and queues nothing.
///
/// While this primary does not hold the repository key, a due backup is
/// recorded `failed` at once with [`super::KEY_ABSENT`], and the row returned
/// is that failed one; the next interval tries again
/// (`node-agent-protocol.md` section 7.3, step 4).
///
/// # Errors
///
/// A database error.
pub async fn queue_if_due(
    state: &AppState,
    now: OffsetDateTime,
) -> Result<Option<Backup>, ApiError> {
    let Some(repository) = repo::any(&state.store).await? else {
        return Ok(None);
    };
    if active(&state.store, &repository.id).await? {
        return Ok(None);
    }
    if let Some(last) = last_terminal(&state.store, &repository.id).await? {
        let interval = time::Duration::hours(repository.control_plane_schedule.interval_h);
        if now < last + interval {
            return Ok(None);
        }
    }
    let Some(node_id) = primary_node(&state.store).await? else {
        tracing::warn!("no primary node; skipping the control-plane backup");
        return Ok(None);
    };
    let created_at = timestamp(now);
    let new = dispatch::NewBackup {
        repository_id: &repository.id,
        source_kind: BackupSourceKind::ControlPlane,
        app_id: None,
        volume_id: None,
        node_id: &node_id,
        fencing_token: None,
        trigger: BackupTrigger::Schedule,
        created_by_type: "system",
        created_by_id: "backups",
        created_at: &created_at,
        project_id: None,
        request: None,
        dispatch_audit: true,
    };
    if !repository.holds_key() {
        let failed = dispatch::queue_failed(&state.store, new, super::KEY_ABSENT).await?;
        tracing::warn!(backup_id = %failed.id, reason = super::KEY_ABSENT, "the control-plane backup failed at once");
        return Ok(Some(failed.api()));
    }
    let backup = dispatch::queue(&state.store, new).await?;
    Ok(Some(backup.api()))
}

/// Takes, uploads and seals one queued control-plane backup
/// (`backup-format.md` section 7.2). Tests call it directly; the dispatch loop
/// spawns it.
///
/// # Errors
///
/// A database error, or an internal error holding the one-line reason the row
/// was failed with.
pub async fn run_control_plane_backup(
    state: &AppState,
    backup_id: &str,
    now: OffsetDateTime,
) -> Result<(), ApiError> {
    let backup = load(&state.store, backup_id).await?;
    if backup.source_kind != BackupSourceKind::ControlPlane {
        return Err(ApiError::internal(&"backup is not a control-plane backup"));
    }
    let node_id = backup.node_id.clone();
    let outcome = async {
        if !mark_running(&state.store, &backup, now).await? {
            return Ok(());
        }
        run(state, &backup, now).await
    }
    .await;
    match outcome {
        Ok(()) => Ok(()),
        Err(reason) => {
            let reason = one_line(&reason);
            seal::fail(&state.store, backup_id, &node_id, &reason).await?;
            Err(ApiError::internal(&reason))
        }
    }
}

/// Resolves every control-plane backup left `queued` or `running` when the
/// primary starts (`backup-format.md` section 7.2, "Rows left behind at
/// start").
///
/// The primary takes a control-plane backup inside its own process, so such a
/// row was interrupted, or the database was restored from that very backup.
/// Each row is resolved from `seals/<cluster_id>/<backup_id>.json`:
///
/// - a seal that verifies with the cluster signing key and names this backup,
///   its repository and the cluster records the backup `succeeded` from the
///   seal, with `uploaded_bytes` null and without writing the seal again;
/// - no seal object records it `failed` with `interrupted`;
/// - a seal that does not verify or names another backup, repository, cluster
///   or source records it `failed` with `the seal does not verify`;
/// - a repository or bucket that cannot be read leaves the row as it is, and
///   the 24-hour timeout still applies.
///
/// Volume backups are never touched. Resolving runs no retention.
///
/// # Errors
///
/// A database error.
pub async fn resolve_orphaned(state: &AppState) -> Result<(), ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups \
         WHERE source_kind = 'control_plane' AND status IN ('queued', 'running') ORDER BY id"
    ))
    .fetch_all(state.store.pool())
    .await?;
    for row in &rows {
        let backup = backup_from_row(row)?;
        resolve_one(state, &backup).await?;
    }
    Ok(())
}

/// What the seal object of a row left behind says.
enum Orphan {
    /// The seal verifies and names this backup.
    Sealed(Box<quoro_backup::Seal>),
    /// No seal object exists.
    Missing,
    /// The seal object exists but is not this backup's valid seal.
    Invalid,
}

/// Resolves one row of [`resolve_orphaned`].
async fn resolve_one(state: &AppState, backup: &BackupRecord) -> Result<(), ApiError> {
    let orphan = match read_orphan_seal(state, backup).await {
        Ok(orphan) => orphan,
        Err(reason) => {
            // Section 7.2: a bucket that cannot be read leaves the row; the
            // 24-hour timeout still applies.
            tracing::warn!(backup_id = %backup.id, reason = %reason, "cannot resolve a control-plane backup left behind; it stays");
            return Ok(());
        }
    };
    match orphan {
        Orphan::Missing => {
            tracing::info!(backup_id = %backup.id, "a control-plane backup left behind has no seal; failing it");
            seal::fail(&state.store, &backup.id, &backup.node_id, "interrupted").await?;
        }
        Orphan::Invalid => {
            tracing::warn!(backup_id = %backup.id, "the seal of a control-plane backup left behind does not verify; failing it");
            seal::fail(
                &state.store,
                &backup.id,
                &backup.node_id,
                "the seal does not verify",
            )
            .await?;
        }
        Orphan::Sealed(sealed) => {
            let sealed = *sealed;
            tracing::info!(backup_id = %backup.id, "a control-plane backup left behind is sealed; recording it");
            seal::record_succeeded(
                &state.store,
                &backup.id,
                seal::Sealed {
                    snapshot_method: None,
                    pause_ms: backup.pause_ms,
                    logical_bytes: i64::try_from(sealed.logical_bytes).ok(),
                    uploaded_bytes: None,
                    chunk_count: i64::try_from(sealed.chunk_count).ok(),
                    manifest_key: sealed.manifest_key,
                    manifest_digest: sealed.manifest_digest,
                    finished_at: sealed.sealed_at,
                },
            )
            .await?;
        }
    }
    Ok(())
}

/// Reads and checks the seal object of a row left behind. Every error is a
/// repository or bucket that cannot be read, as a one-line reason.
async fn read_orphan_seal(state: &AppState, backup: &BackupRecord) -> Result<Orphan, String> {
    let repository = repo::by_id(&state.store, &backup.repository_id)
        .await
        .map_err(|error| reason_of(&error))?
        .ok_or_else(|| "the backup's repository is missing".to_string())?;
    let target = repo::target(&state.store, &state.data_key, &repository)
        .await
        .map_err(|error| reason_of(&error))?;
    let bucket = state
        .backups
        .open_bucket(&target)
        .map_err(|error| reason_of(&error))?;
    let cluster_id = &state.cluster_id;
    let key = object_key(&target.root, &layout::seal(cluster_id, &backup.id));
    let bytes = match bucket.get(&key).await {
        Ok(bytes) => bytes,
        Err(ObjectStoreError::NotFound) => return Ok(Orphan::Missing),
        Err(error) => return Err(format!("cannot read the seal: {error}")),
    };
    let Ok(sealed) = serde_json::from_slice::<quoro_backup::Seal>(&bytes) else {
        return Ok(Orphan::Invalid);
    };
    let verifies =
        quoro_backup::verify_seal(&sealed, &state.keys.signing_key().verifying_key()).is_ok();
    if !verifies
        || sealed.backup_id != backup.id
        || &sealed.cluster_id != cluster_id
        || sealed.repository_id != backup.repository_id
        || sealed.source_kind != SourceKind::ControlPlane
        || &sealed.source_id != cluster_id
        || sealed.manifest_key != layout::manifest(cluster_id, &backup.id)
    {
        return Ok(Orphan::Invalid);
    }
    Ok(Orphan::Sealed(Box::new(sealed)))
}

/// Removes the temporary files a crashed backup left in `data_dir`.
///
/// # Errors
///
/// An I/O error from the directory listing or one removal.
pub fn remove_leftovers(data_dir: &Path) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(TEMP_PREFIX)
            && name.ends_with(TEMP_SUFFIX)
            && entry.file_type()?.is_file()
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// A pre-upgrade backup that is sealed but not yet recorded.
///
/// The row can only be written after the migrations ran, so the values the row
/// needs travel with the seal (`backup-format.md` section 7.2, "Before an
/// upgrade").
#[derive(Clone, Debug)]
pub struct SealedUpgrade {
    /// The id the backup was sealed under, generated before it ran.
    pub backup_id: String,
    /// The repository the backup went to.
    pub repository_id: String,
    /// The primary node that took it, when the row is written.
    pub node_id: String,
    /// When the backup started, as `backups.created_at`.
    pub created_at: String,
    /// When the seal was written.
    pub sealed_at: String,
    /// Repository-relative key of the manifest object.
    pub manifest_key: String,
    /// BLAKE3 digest of the manifest object's bytes.
    pub manifest_digest: String,
    /// Size of the vacuumed database.
    pub logical_bytes: u64,
    /// Bytes of new chunk objects this backup uploaded.
    pub uploaded_bytes: u64,
    /// Chunks the manifest lists.
    pub chunk_count: u64,
}

/// Takes the pre-upgrade backup against the old schema, before the pending
/// migrations run (`backup-format.md` section 7.2, "Before an upgrade").
///
/// It writes nothing to the database: the seal carries what [`record_upgrade`]
/// needs afterwards. It returns `None` when there is no repository to write to
/// or no primary node to record the row against, and the caller migrates
/// without a backup then.
///
/// # Errors
///
/// A one-line reason for any failed step; no migration runs and the temporary
/// file is removed. [`super::KEY_ABSENT`] while this primary does not hold the
/// repository key, after a promotion, before the bucket is touched: the
/// operator migrates with `--skip-upgrade-backup` (`backup-format.md`
/// section 7.2 step 4, ADR 0322).
pub async fn backup_before_upgrade(
    store: &Store,
    data_dir: &Path,
    keys: &ClusterKeys,
    data_key: &DataKey,
    runtime: &Backups,
    now: OffsetDateTime,
) -> Result<Option<SealedUpgrade>, String> {
    let repository = repo::any_before_upgrade(store)
        .await
        .map_err(|error| reason_of(&error))?;
    let Some(repository) = repository else {
        tracing::info!("migrating without a pre-upgrade backup: no backup repository");
        return Ok(None);
    };
    if !repository.holds_key() {
        return Err(super::KEY_ABSENT.to_string());
    }
    // `queue_if_due` queues nothing without a primary node, and a `backups` row
    // needs one (`node_id` is a foreign key).
    let Some(node_id) = primary_node(store)
        .await
        .map_err(|error| reason_of(&error))?
    else {
        tracing::warn!("migrating without a pre-upgrade backup: no primary node");
        return Ok(None);
    };
    if let Err(error) = remove_leftovers(data_dir) {
        return Err(format!("could not remove a leftover file: {error}"));
    }
    let target = repo::target(store, data_key, &repository)
        .await
        .map_err(|error| reason_of(&error))?;
    let bucket = runtime
        .open_bucket(&target)
        .map_err(|error| reason_of(&error))?;
    let cluster_id = repo::cluster_id(store)
        .await
        .map_err(|error| reason_of(&error))?;
    let repo_key = super::decrypt_repo_key(data_key, &repository.id, &repository.repo_key_enc)
        .map_err(|error| reason_of(&error))?;
    let backup_id = crate::deploy::new_id_with_prefix("bkp");
    let uploaded = upload_database(
        store,
        data_dir,
        &bucket,
        &target,
        &cluster_id,
        &repository.id,
        &backup_id,
        repo_key.source_keys(&cluster_id),
        now,
    )
    .await?;
    let created_at = timestamp(now);
    let seal = quoro_backup::Seal {
        format: quoro_backup::FORMAT,
        backup_id: backup_id.clone(),
        cluster_id: cluster_id.clone(),
        repository_id: repository.id.clone(),
        source_kind: SourceKind::ControlPlane,
        source_id: cluster_id.clone(),
        volume_id: None,
        fencing_token: None,
        manifest_key: uploaded.manifest_key.clone(),
        manifest_digest: uploaded.manifest_digest.clone(),
        logical_bytes: uploaded.logical_bytes,
        chunk_count: uploaded.chunk_count,
        created_at: created_at.clone(),
        sealed_at: created_at.clone(),
        signature: String::new(),
    };
    if store.is_demoted() {
        return Err(crate::store::DEMOTED_REFUSAL.to_string());
    }
    match seal::put_seal(&bucket, &target, keys, seal).await {
        Ok(()) => {}
        Err(seal::SealError::Failed(reason)) => return Err(reason),
        Err(seal::SealError::Retry(_)) => {
            return Err("the seal object could not be read back".to_string());
        }
    }
    tracing::info!(backup_id, "the pre-upgrade backup is sealed");
    Ok(Some(SealedUpgrade {
        backup_id,
        repository_id: repository.id,
        node_id,
        created_at: created_at.clone(),
        sealed_at: created_at,
        manifest_key: uploaded.manifest_key,
        manifest_digest: uploaded.manifest_digest,
        logical_bytes: uploaded.logical_bytes,
        uploaded_bytes: uploaded.uploaded_bytes,
        chunk_count: uploaded.chunk_count,
    }))
}

/// Records a sealed pre-upgrade backup `succeeded` in the migrated database,
/// with its change-log entry, its `backup.dispatch` and `backup.seal` records,
/// and runs retention (`backup-format.md` section 7.2 step 3).
///
/// # Errors
///
/// A database error.
pub async fn record_upgrade(
    store: &Store,
    keys: &ClusterKeys,
    data_key: &DataKey,
    runtime: &Backups,
    sealed: &SealedUpgrade,
    now: OffsetDateTime,
) -> Result<(), ApiError> {
    let backup = BackupRecord {
        id: sealed.backup_id.clone(),
        repository_id: sealed.repository_id.clone(),
        source_kind: BackupSourceKind::ControlPlane,
        app_id: None,
        volume_id: None,
        node_id: sealed.node_id.clone(),
        status: BackupStatus::Succeeded,
        trigger: BackupTrigger::Upgrade,
        fencing_token: None,
        snapshot_method: None,
        pause_ms: None,
        logical_bytes: i64::try_from(sealed.logical_bytes).ok(),
        uploaded_bytes: i64::try_from(sealed.uploaded_bytes).ok(),
        chunk_count: i64::try_from(sealed.chunk_count).ok(),
        manifest_key: Some(sealed.manifest_key.clone()),
        manifest_digest: Some(sealed.manifest_digest.clone()),
        error: None,
        created_by_type: "system".to_string(),
        created_by_id: "backups".to_string(),
        created_at: sealed.created_at.clone(),
        started_at: Some(sealed.created_at.clone()),
        finished_at: Some(sealed.sealed_at.clone()),
        pruned_at: None,
    };
    let mut w = store.begin_write().await?;
    sqlx::query(
        "INSERT INTO backups (id, repository_id, source_kind, app_id, volume_id, node_id, status, \
         \"trigger\", fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes, \
         chunk_count, manifest_key, manifest_digest, error, created_by_type, created_by_id, \
         created_at, started_at, finished_at, pruned_at) \
         VALUES (?, ?, 'control_plane', NULL, NULL, ?, 'succeeded', 'upgrade', NULL, NULL, NULL, \
         ?, ?, ?, ?, ?, NULL, 'system', 'backups', ?, ?, ?, NULL)",
    )
    .bind(&backup.id)
    .bind(&backup.repository_id)
    .bind(&backup.node_id)
    .bind(backup.logical_bytes)
    .bind(backup.uploaded_bytes)
    .bind(backup.chunk_count)
    .bind(&backup.manifest_key)
    .bind(&backup.manifest_digest)
    .bind(&backup.created_at)
    .bind(&backup.started_at)
    .bind(&backup.finished_at)
    .execute(&mut *w.tx)
    .await?;
    let seq = changelog::append(
        &mut w.tx,
        &Actor::system("backups"),
        "backup",
        &backup.id,
        Op::Create,
        backup_payload(&backup),
    )
    .await?;
    let record = super::system_record(
        "backup.dispatch",
        "backup",
        &backup.id,
        json!({ "app_id": Value::Null, "volume_id": Value::Null, "trigger": "upgrade" }),
        crate::audit::Outcome::Ok,
        None,
        Some(seq),
    );
    crate::audit::append(&mut w.tx, &record).await?;
    let record = super::system_record(
        "backup.seal",
        "backup",
        &backup.id,
        json!({}),
        crate::audit::Outcome::Ok,
        None,
        Some(seq),
    );
    crate::audit::append(&mut w.tx, &record).await?;
    w.commit().await?;

    // Retention runs only after the seal exists (`backup-format.md` section 9),
    // with the upgrade rule of section 7.2.
    let Some(repository) = repo::by_id(store, &backup.repository_id).await? else {
        return Ok(());
    };
    let target = repo::target(store, data_key, &repository).await?;
    let bucket = runtime.open_bucket(&target)?;
    if let Err(error) = super::prune::after_seal(
        store,
        data_key,
        keys,
        runtime,
        &repository,
        &target,
        &bucket,
        &backup,
        now,
    )
    .await
    {
        tracing::warn!(backup_id = %backup.id, error = ?error, "backup pruning failed");
    }
    Ok(())
}

/// Records that the operator migrated without the backup
/// (`backup.upgrade_skipped`, `audit-log-format.md` section 3.5).
///
/// # Errors
///
/// A database error.
async fn record_skip(
    store: &Store,
    repository_id: &str,
    from: usize,
    to: usize,
) -> Result<(), ApiError> {
    let mut w = store.begin_write().await?;
    let record = super::system_record(
        "backup.upgrade_skipped",
        "backup_repository",
        repository_id,
        json!({ "from_schema_version": from, "to_schema_version": to }),
        crate::audit::Outcome::Ok,
        None,
        None,
    );
    crate::audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(())
}

/// The whole pre-upgrade step of `backup-format.md` section 7.2, "Before an
/// upgrade": when migrations are pending, back the database up (or honour the
/// operator's skip), migrate, and record the result.
///
/// A start with no pending migration does nothing, with or without the flag.
///
/// # Errors
///
/// A one-line reason. No migration ran when the backup failed, and the
/// database is left as it was.
pub async fn upgrade_before_start(
    store: &Store,
    data_dir: &Path,
    keys: &ClusterKeys,
    data_key: &DataKey,
    runtime: &Backups,
    skip: bool,
) -> Result<(), String> {
    let pending = store
        .pending_migrations()
        .await
        .map_err(|error| error.to_string())?;
    let Some((from, to)) = pending else {
        // A new database gets its schema here, and a migrated one nothing:
        // there is no backup to take either way (section 7.2 step 1).
        store.migrate().await.map_err(|error| error.to_string())?;
        return Ok(());
    };
    let repository = repo::any_before_upgrade(store)
        .await
        .map_err(|error| reason_of(&error))?;
    let Some(repository) = repository else {
        tracing::info!(
            from,
            to,
            "migrating without a pre-upgrade backup: no repository"
        );
        store.migrate().await.map_err(|error| error.to_string())?;
        return Ok(());
    };
    if skip {
        store.migrate().await.map_err(|error| error.to_string())?;
        record_skip(store, &repository.id, from, to)
            .await
            .map_err(|error| reason_of(&error))?;
        tracing::warn!(
            from,
            to,
            repository_id = %repository.id,
            "migrated without a pre-upgrade backup: --skip-upgrade-backup"
        );
        return Ok(());
    }
    let now = OffsetDateTime::now_utc();
    let sealed = backup_before_upgrade(store, data_dir, keys, data_key, runtime, now).await?;
    store.migrate().await.map_err(|error| error.to_string())?;
    if let Some(sealed) = sealed {
        record_upgrade(store, keys, data_key, runtime, &sealed, now)
            .await
            .map_err(|error| reason_of(&error))?;
    }
    Ok(())
}

/// The work of one backup, up to the seal. Every failure is a one-line reason.
async fn run(state: &AppState, backup: &BackupRecord, now: OffsetDateTime) -> Result<(), String> {
    let repository = repo::by_id(&state.store, &backup.repository_id)
        .await
        .map_err(|error| reason_of(&error))?
        .ok_or_else(|| "the backup's repository is missing".to_string())?;
    // A backup queued before the key went absent fails with the same reason
    // as one the schedule fails at once (`node-agent-protocol.md` section
    // 7.3, step 4).
    if !repository.holds_key() {
        return Err(super::KEY_ABSENT.to_string());
    }
    let target = repo::target(&state.store, &state.data_key, &repository)
        .await
        .map_err(|error| reason_of(&error))?;
    let bucket = state
        .backups
        .open_bucket(&target)
        .map_err(|error| reason_of(&error))?;
    // Section 7.2: the primary holds `repo_key` and does the work itself.
    let repo_key =
        super::decrypt_repo_key(&state.data_key, &repository.id, &repository.repo_key_enc)
            .map_err(|error| reason_of(&error))?;
    let source_id = state.cluster_id.clone();
    let keys = repo_key.source_keys(&source_id);

    let uploaded = upload_database(
        &state.store,
        &state.config.data_dir,
        &bucket,
        &target,
        &source_id,
        &repository.id,
        &backup.id,
        keys,
        now,
    )
    .await?;

    // Section 7.2 step 5: the existing seal path signs and prunes, exactly as
    // for a node's report.
    let entry = wire::BackupStatus {
        backup_id: backup.id.clone(),
        status: wire::BackupState::Succeeded,
        snapshot_method: None,
        pause_ms: None,
        guest_flush: None,
        manifest_key: Some(uploaded.manifest_key),
        manifest_digest: Some(uploaded.manifest_digest),
        logical_bytes: Some(uploaded.logical_bytes),
        uploaded_bytes: Some(uploaded.uploaded_bytes),
        chunk_count: Some(uploaded.chunk_count),
        error: None,
        started_at: Some(timestamp(now)),
        finished_at: Some(timestamp(now)),
    };
    seal::seal(
        &state.store,
        &state.data_key,
        &state.keys,
        &source_id,
        &state.backups,
        backup,
        &entry,
    )
    .await
    .map_err(|error| reason_of(&error))?;
    Ok(())
}

/// The manifest, digest and counts of one uploaded database.
#[derive(Clone, Debug)]
pub(crate) struct Uploaded {
    /// Repository-relative key of the manifest object.
    pub manifest_key: String,
    /// BLAKE3 digest of the manifest object's bytes.
    pub manifest_digest: String,
    /// Size of the vacuumed database.
    pub logical_bytes: u64,
    /// Bytes of new chunk objects this backup uploaded.
    pub uploaded_bytes: u64,
    /// Chunks the manifest lists.
    pub chunk_count: u64,
}

/// Vacuums the database into a temporary file in `data_dir`, chunks it,
/// uploads the chunks and writes the manifest (`backup-format.md` sections 5,
/// 6.1 and 7.2). Both the scheduled and the pre-upgrade backup run it.
///
/// The temporary file is removed on every path, success or error.
///
/// # Errors
///
/// A one-line reason for any failed step.
#[allow(clippy::too_many_arguments)]
async fn upload_database(
    store: &Store,
    data_dir: &Path,
    bucket: &Bucket,
    target: &RepositoryTarget,
    source_id: &str,
    repository_id: &str,
    backup_id: &str,
    keys: SourceKeys,
    now: OffsetDateTime,
) -> Result<Uploaded, String> {
    // `VACUUM INTO` does not go through the writer gate, so a demoted
    // primary, which uploads nothing more, is checked here
    // (`node-agent-protocol.md` section 7.4).
    if store.is_demoted() {
        return Err(crate::store::DEMOTED_REFUSAL.to_string());
    }
    let path = temp_path(data_dir, backup_id)?;
    let quote_free = path
        .to_str()
        .ok_or_else(|| "the data directory path is not UTF-8".to_string())?
        .to_string();
    // A file with this name can only be a leftover of an earlier run of this
    // backup: the id is unique.
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("could not remove a leftover file: {error}")),
    }
    let guard = TempBackup::new(path.clone());

    // Outside any transaction, on a pool connection (ADR 0148).
    let vacuum = format!("VACUUM INTO '{quote_free}'");
    sqlx::query(&vacuum)
        .execute(store.pool())
        .await
        .map_err(|error| format!("could not vacuum the database: {error}"))?;

    let file = std::fs::File::open(&path)
        .map_err(|error| format!("could not open the temporary database: {error}"))?;
    let header = Manifest {
        format: quoro_backup::FORMAT,
        backup_id: backup_id.to_string(),
        cluster_id: source_id.to_string(),
        repository_id: repository_id.to_string(),
        source_kind: SourceKind::ControlPlane,
        source_id: source_id.to_string(),
        volume_id: None,
        fencing_token: None,
        snapshot_method: None,
        logical_bytes: 0,
        chunker: Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: env!("CARGO_PKG_VERSION").to_owned(),
        created_at: timestamp(now),
    };
    let mut writer = BackupWriter::new(keys, header);
    let mut uploaded: u64 = 0;
    // Section 5: all-zero chunks are uploaded like any other, and a `412`
    // means the repository already holds the chunk.
    for chunk in chunks(file) {
        let chunk =
            chunk.map_err(|error| format!("could not read the temporary database: {error}"))?;
        let (id, object, _zero) = writer.push(chunk);
        let size = object.len() as u64;
        let key = object_key(&target.root, &layout::chunk(&id));
        // A primary demoted during the upload uploads no further chunk.
        if store.is_demoted() {
            return Err(crate::store::DEMOTED_REFUSAL.to_string());
        }
        match bucket.put_new(&key, object).await {
            Ok(()) => uploaded = uploaded.saturating_add(size),
            Err(ObjectStoreError::AlreadyExists) => {}
            Err(error) => return Err(format!("could not upload a chunk: {error}")),
        }
    }

    let (manifest, object, digest) = writer
        .finish()
        .map_err(|error| format!("could not write the manifest: {error}"))?;
    let manifest_key = layout::manifest(source_id, backup_id);
    let key = object_key(&target.root, &manifest_key);
    // The manifest commits the uploaded chunks; a primary demoted during the
    // upload writes it no more.
    if store.is_demoted() {
        return Err(crate::store::DEMOTED_REFUSAL.to_string());
    }
    match bucket.put_new(&key, object).await {
        Ok(()) => {}
        Err(ObjectStoreError::AlreadyExists) => {
            return Err("the manifest object already exists".to_string());
        }
        Err(error) => return Err(format!("could not upload the manifest: {error}")),
    }
    // The temporary file has done its work before the seal.
    drop(guard);
    Ok(Uploaded {
        manifest_key,
        manifest_digest: digest,
        logical_bytes: manifest.logical_bytes,
        uploaded_bytes: uploaded,
        chunk_count: manifest.chunks.len() as u64,
    })
}

/// Marks the row `running` with its start time, its change-log entry in the
/// same transaction. Returns whether the work should run: a row that is
/// already terminal does nothing.
async fn mark_running(
    store: &Store,
    backup: &BackupRecord,
    now: OffsetDateTime,
) -> Result<bool, String> {
    if backup.status.is_terminal() {
        return Ok(false);
    }
    let mut w = store
        .begin_write()
        .await
        .map_err(|error| error.to_string())?;
    let mut after = backup.clone();
    after.status = BackupStatus::Running;
    after.started_at = Some(timestamp(now));
    sqlx::query("UPDATE backups SET status = 'running', started_at = ? WHERE id = ?")
        .bind(&after.started_at)
        .bind(&after.id)
        .execute(&mut *w.tx)
        .await
        .map_err(|error| error.to_string())?;
    changelog::append(
        &mut w.tx,
        &Actor::system("backups"),
        "backup",
        &after.id,
        Op::Update,
        backup_payload(&after),
    )
    .await
    .map_err(|error| error.to_string())?;
    w.commit().await.map_err(|error| error.to_string())?;
    Ok(true)
}

/// One `backups` row by id.
async fn load(store: &Store, backup_id: &str) -> Result<BackupRecord, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
    ))
    .bind(backup_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Err(ApiError::not_found("backup"));
    };
    backup_from_row(&row)
}

/// Whether a control-plane backup of the repository is non-terminal.
async fn active(store: &Store, repository_id: &str) -> Result<bool, ApiError> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM backups WHERE repository_id = ? AND source_kind = 'control_plane' \
         AND status IN ('queued', 'running') LIMIT 1",
    )
    .bind(repository_id)
    .fetch_optional(store.pool())
    .await?;
    Ok(row.is_some())
}

/// The creation time of the newest terminal control-plane backup.
async fn last_terminal(
    store: &Store,
    repository_id: &str,
) -> Result<Option<OffsetDateTime>, ApiError> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT MAX(created_at) FROM backups WHERE repository_id = ? \
         AND source_kind = 'control_plane' AND status IN ('succeeded', 'failed')",
    )
    .bind(repository_id)
    .fetch_optional(store.pool())
    .await?;
    Ok(row
        .and_then(|row| row.0)
        .and_then(|text| super::parse_time(&text)))
}

/// The node row that carries `cp_status = 'primary'`.
async fn primary_node(store: &Store) -> Result<Option<String>, ApiError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary'")
        .fetch_optional(store.pool())
        .await?;
    Ok(row.map(|row| row.0))
}

/// `<data_dir>/cp-backup-<backup_id>.sqlite`, refused when it holds a quote
/// that would break the `VACUUM INTO` literal.
fn temp_path(data_dir: &Path, backup_id: &str) -> Result<PathBuf, String> {
    let path = data_dir.join(format!("{TEMP_PREFIX}{backup_id}{TEMP_SUFFIX}"));
    let text = path
        .to_str()
        .ok_or_else(|| "the data directory path is not UTF-8".to_string())?;
    if text.contains('\'') {
        return Err("the temporary path holds a quote".to_string());
    }
    Ok(path)
}

/// Removes the temporary database however the backup ends.
struct TempBackup {
    path: PathBuf,
}

impl TempBackup {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempBackup {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(path = %self.path.display(), error = %error, "could not remove the temporary database");
            }
        }
    }
}

/// One short phrase: the first line, at most 2 000 characters.
fn one_line(text: &str) -> String {
    text.lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(2000)
        .collect()
}

/// The reason an `ApiError` carries, for the failed row and its
/// `backup.seal` record. The generic internal detail is used when it has none.
fn reason_of(error: &ApiError) -> String {
    error.detail.clone().unwrap_or_else(|| error.audit_detail())
}
