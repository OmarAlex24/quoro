// SPDX-License-Identifier: AGPL-3.0-only
//! Applying `status_report.backups` and writing the seal
//! (`backup-format.md` sections 6.2 and 7.1, `node-agent-protocol.md`
//! section 5.3).
//!
//! A backup becomes real only here: the primary checks the lease, the manifest
//! key and digest the node reported, signs a [`Seal`] with the cluster signing
//! key and writes `seals/<source_id>/<backup_id>.json` with
//! `If-None-Match: *`. A failed put records the backup `failed` at once.
//! Only after the put does the row turn `succeeded`, the `backup.succeeded` event is
//! emitted and retention runs.

use crate::api::apps::SnapshotMethod;
use crate::api::backups::{
    BACKUP_COLUMNS, BackupRecord, BackupSourceKind, BackupStatus, backup_from_row, backup_payload,
};
use crate::changelog::{self, Op};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::keys::ClusterKeys;
use crate::store::{Store, now_rfc3339};
use quoro_backup::{FORMAT, Seal, layout, sign_seal, verify_seal};
use quoro_proto::wire;
use serde_json::{Value, json};
use sqlx::Row;

use super::{
    BACKUP_TIMEOUT, Backups, Bucket, RepositoryTarget, object_key, parse_time, repo, time_after,
};

/// Applies one report's backups of `node_id`. Returns whether the node's
/// desired state changed (a job left the `queued`/`running` set), so the
/// caller can resend the snapshot.
///
/// # Errors
///
/// A database error, or an internal error when the seal cannot be built or
/// signed. Object-store failures never fail the report.
pub(crate) async fn apply_status(
    store: &Store,
    data_key: &crate::crypto::DataKey,
    keys: &ClusterKeys,
    cluster_id: &str,
    runtime: &Backups,
    node_id: &str,
    reported: &[wire::BackupStatus],
) -> Result<bool, ApiError> {
    let mut desired_changed = false;
    for entry in reported {
        let row = sqlx::query(&format!(
            "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
        ))
        .bind(&entry.backup_id)
        .fetch_optional(store.pool())
        .await?;
        let Some(row) = row else {
            continue;
        };
        let before = backup_from_row(&row)?;
        if before.node_id != node_id || before.status.is_terminal() {
            continue;
        }
        match entry.status {
            wire::BackupState::Queued => {}
            wire::BackupState::Running => {
                update_running(store, &before, entry).await?;
            }
            wire::BackupState::Failed => {
                let reason = entry
                    .error
                    .clone()
                    .unwrap_or_else(|| "the node reported the backup failed".to_string());
                if fail(store, &before.id, node_id, &reason).await? {
                    desired_changed = true;
                }
            }
            wire::BackupState::Succeeded => {
                if seal(store, data_key, keys, cluster_id, runtime, &before, entry).await? {
                    desired_changed = true;
                }
            }
        }
    }
    Ok(desired_changed)
}

/// Records a `running` report: the snapshot method, the pause and the start
/// time. Every other field stays as it was.
async fn update_running(
    store: &Store,
    before: &BackupRecord,
    entry: &wire::BackupStatus,
) -> Result<(), ApiError> {
    let mut after = before.clone();
    after.status = BackupStatus::Running;
    after.snapshot_method = entry.snapshot_method.map(snapshot_method);
    after.pause_ms = entry.pause_ms.and_then(|ms| i64::try_from(ms).ok());
    after.started_at = Some(
        entry
            .started_at
            .clone()
            .unwrap_or_else(|| before.started_at.clone().unwrap_or_else(now_rfc3339)),
    );
    if after.status == before.status
        && after.snapshot_method == before.snapshot_method
        && after.pause_ms == before.pause_ms
        && after.started_at == before.started_at
    {
        return Ok(());
    }
    let mut w = store.begin_write().await?;
    sqlx::query(
        "UPDATE backups SET status = 'running', snapshot_method = ?, pause_ms = ?, started_at = ? \
         WHERE id = ?",
    )
    .bind(after.snapshot_method.map(snapshot_name))
    .bind(after.pause_ms)
    .bind(&after.started_at)
    .bind(&after.id)
    .execute(&mut *w.tx)
    .await?;
    changelog::append(
        &mut w.tx,
        &changelog::Actor::system("backups"),
        "backup",
        &after.id,
        Op::Update,
        backup_payload(&after),
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// Records a backup `failed` with `reason`, emits `backup.failed` and writes
/// the `backup.seal` system record with `error`. Returns whether the job left
/// the active set (it did unless the row was already terminal).
pub(crate) async fn fail(
    store: &Store,
    backup_id: &str,
    node_id: &str,
    reason: &str,
) -> Result<bool, ApiError> {
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
    ))
    .bind(backup_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let before = backup_from_row(&row)?;
    if before.node_id != node_id || before.status.is_terminal() {
        return Ok(false);
    }
    let mut after = before.clone();
    let reason = one_line(reason);
    after.status = BackupStatus::Failed;
    after.error = Some(reason.clone());
    after.finished_at = Some(now_rfc3339());
    sqlx::query("UPDATE backups SET status = 'failed', error = ?, finished_at = ? WHERE id = ?")
        .bind(&after.error)
        .bind(&after.finished_at)
        .bind(&after.id)
        .execute(&mut *w.tx)
        .await?;
    let seq = changelog::append(
        &mut w.tx,
        &changelog::Actor::system("backups"),
        "backup",
        &after.id,
        Op::Update,
        backup_payload(&after),
    )
    .await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "backup.failed",
            source: Source::ControlPlane,
            subject_type: "backup",
            subject_id: &after.id,
            project_id: None,
            request_id: None,
            actor: None,
            data: event_data(&after, Some(Value::String(reason.clone()))),
        },
    )
    .await?;
    let record = super::system_record(
        "backup.seal",
        "backup",
        &after.id,
        json!({}),
        crate::audit::Outcome::Error,
        Some(reason),
        Some(seq),
    );
    crate::audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(true)
}

/// Seals a reported `succeeded` backup. Returns whether the row left the
/// active set; `false` also covers a retryable object-store failure.
pub(crate) async fn seal(
    store: &Store,
    data_key: &crate::crypto::DataKey,
    keys: &ClusterKeys,
    cluster_id: &str,
    runtime: &Backups,
    before: &BackupRecord,
    entry: &wire::BackupStatus,
) -> Result<bool, ApiError> {
    let source_id = match before.source_kind {
        BackupSourceKind::Volume => before
            .app_id
            .clone()
            .unwrap_or_else(|| String::from("unknown")),
        BackupSourceKind::ControlPlane => cluster_id.to_string(),
    };
    let Some(manifest_key) = entry.manifest_key.clone() else {
        return fail(store, &before.id, &before.node_id, "bad manifest report").await;
    };
    let Some(manifest_digest) = entry.manifest_digest.clone() else {
        return fail(store, &before.id, &before.node_id, "bad manifest report").await;
    };
    if manifest_key != layout::manifest(&source_id, &before.id) {
        return fail(store, &before.id, &before.node_id, "bad manifest report").await;
    }
    if !is_lower_hex_64(&manifest_digest) {
        return fail(store, &before.id, &before.node_id, "bad manifest report").await;
    }
    // The reported token must still be the app's current one
    // (`backup-format.md` section 6.2).
    if before.source_kind == BackupSourceKind::Volume {
        let app_id = before.app_id.as_deref().unwrap_or_default();
        let token: Option<(i64,)> = sqlx::query_as(
            "SELECT fencing_token FROM leases \
             WHERE resource_type = 'app' AND resource_id = ? AND revoked_at IS NULL",
        )
        .bind(app_id)
        .fetch_optional(store.pool())
        .await?;
        if token.map(|row| row.0) != before.fencing_token {
            return fail(store, &before.id, &before.node_id, "lease_mismatch").await;
        }
    }

    let repository = repo::by_id(store, &before.repository_id)
        .await?
        .ok_or_else(|| ApiError::internal(&"the backup's repository is missing"))?;
    let target = repo::target(store, data_key, &repository).await?;
    let bucket = runtime.open_bucket(&target)?;

    let now = now_rfc3339();
    let seal = Seal {
        format: FORMAT,
        backup_id: before.id.clone(),
        cluster_id: cluster_id.to_string(),
        repository_id: repository.id.clone(),
        source_kind: source_kind(before.source_kind),
        source_id: source_id.clone(),
        volume_id: before.volume_id.clone(),
        fencing_token: before.fencing_token.and_then(|t| u64::try_from(t).ok()),
        manifest_key: manifest_key.clone(),
        manifest_digest: manifest_digest.clone(),
        logical_bytes: entry.logical_bytes.unwrap_or(0),
        chunk_count: entry.chunk_count.unwrap_or(0),
        created_at: before.created_at.clone(),
        sealed_at: now.clone(),
        signature: String::new(),
    };
    match put_seal(&bucket, &target, keys, seal).await {
        Ok(()) => {}
        Err(SealError::Failed(reason)) => {
            // A seal that cannot be written fails the backup at once
            // (`backup-format.md` section 7.1, step 5); a scheduled one runs
            // again at its next interval.
            tracing::warn!(backup_id = %before.id, reason = %reason, "cannot write the seal");
            return fail(store, &before.id, &before.node_id, &reason).await;
        }
        Err(SealError::Retry(error)) => return Err(error),
    }

    let sealed = Sealed {
        snapshot_method: entry.snapshot_method.map(snapshot_method),
        pause_ms: entry.pause_ms.and_then(|ms| i64::try_from(ms).ok()),
        logical_bytes: entry.logical_bytes.and_then(|n| i64::try_from(n).ok()),
        uploaded_bytes: entry.uploaded_bytes.and_then(|n| i64::try_from(n).ok()),
        chunk_count: entry.chunk_count.and_then(|n| i64::try_from(n).ok()),
        manifest_key,
        manifest_digest,
        finished_at: now,
    };
    let Some(after) = record_succeeded(store, &before.id, sealed).await? else {
        return Ok(false);
    };

    // Retention runs only after the seal exists (`backup-format.md` section 9).
    if let Err(error) = super::prune::after_seal(
        store,
        data_key,
        keys,
        runtime,
        &repository,
        &target,
        &bucket,
        &after,
        time::OffsetDateTime::now_utc(),
    )
    .await
    {
        tracing::warn!(backup_id = %after.id, error = ?error, "backup pruning failed");
    }
    Ok(true)
}

/// What a written seal records on its row.
pub(crate) struct Sealed {
    /// The snapshot method; `None` keeps the row's.
    pub snapshot_method: Option<SnapshotMethod>,
    /// The pause, replacing the row's.
    pub pause_ms: Option<i64>,
    /// Size of the file the manifest's chunks cover.
    pub logical_bytes: Option<i64>,
    /// Bytes of new chunk objects; `None` when they are not known.
    pub uploaded_bytes: Option<i64>,
    /// Chunks the manifest lists.
    pub chunk_count: Option<i64>,
    /// Repository-relative key of the manifest object.
    pub manifest_key: String,
    /// BLAKE3 digest of the manifest object's bytes.
    pub manifest_digest: String,
    /// When the seal was written.
    pub finished_at: String,
}

/// Records a backup whose seal object exists `succeeded`, with its change-log
/// entry, the `backup.succeeded` event and the `backup.seal` system record
/// with `ok`. It writes no object and runs no retention.
///
/// Returns the recorded row, or `None` when the row is gone or already
/// terminal.
///
/// # Errors
///
/// A database error.
pub(crate) async fn record_succeeded(
    store: &Store,
    backup_id: &str,
    sealed: Sealed,
) -> Result<Option<BackupRecord>, ApiError> {
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
    ))
    .bind(backup_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let fresh = backup_from_row(&row)?;
    if fresh.status.is_terminal() {
        return Ok(None);
    }
    let mut after = fresh;
    after.status = BackupStatus::Succeeded;
    after.snapshot_method = sealed.snapshot_method.or(after.snapshot_method);
    after.pause_ms = sealed.pause_ms;
    after.logical_bytes = sealed.logical_bytes;
    after.uploaded_bytes = sealed.uploaded_bytes;
    after.chunk_count = sealed.chunk_count;
    after.manifest_key = Some(sealed.manifest_key);
    after.manifest_digest = Some(sealed.manifest_digest);
    after.finished_at = Some(sealed.finished_at);
    sqlx::query(
        "UPDATE backups SET status = 'succeeded', snapshot_method = ?, pause_ms = ?, \
         logical_bytes = ?, uploaded_bytes = ?, chunk_count = ?, manifest_key = ?, \
         manifest_digest = ?, finished_at = ? WHERE id = ?",
    )
    .bind(after.snapshot_method.map(snapshot_name))
    .bind(after.pause_ms)
    .bind(after.logical_bytes)
    .bind(after.uploaded_bytes)
    .bind(after.chunk_count)
    .bind(&after.manifest_key)
    .bind(&after.manifest_digest)
    .bind(&after.finished_at)
    .bind(&after.id)
    .execute(&mut *w.tx)
    .await?;
    let seq = changelog::append(
        &mut w.tx,
        &changelog::Actor::system("backups"),
        "backup",
        &after.id,
        Op::Update,
        backup_payload(&after),
    )
    .await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "backup.succeeded",
            source: Source::ControlPlane,
            subject_type: "backup",
            subject_id: &after.id,
            project_id: None,
            request_id: None,
            actor: None,
            data: event_data(&after, None),
        },
    )
    .await?;
    let record = super::system_record(
        "backup.seal",
        "backup",
        &after.id,
        json!({}),
        crate::audit::Outcome::Ok,
        None,
        Some(seq),
    );
    crate::audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(Some(after))
}

/// Why the seal object could not be written.
pub(crate) enum SealError {
    /// The bucket refused the put, or the stored object is not this backup's:
    /// the backup fails at once with this one-line reason.
    Failed(String),
    /// A retry after the row update failed could not read the existing object
    /// back: the backup stays as it was for a later retry.
    Retry(ApiError),
}

/// Signs `seal` and writes `seals/<source_id>/<backup_id>.json` with
/// `If-None-Match: *` (`backup-format.md` section 6.2).
///
/// The pre-upgrade backup writes its seal through here too, before the row
/// exists (`backup-format.md` section 7.2, "Before an upgrade").
///
/// # Errors
///
/// A [`SealError::Failed`] for a refused put or a stored object that is not
/// this backup's, a [`SealError::Retry`] when a retry cannot read the existing
/// object back.
pub(crate) async fn put_seal(
    bucket: &Bucket,
    target: &RepositoryTarget,
    keys: &ClusterKeys,
    mut seal: Seal,
) -> Result<(), SealError> {
    sign_seal(&mut seal, keys.signing_key())
        .map_err(|error| SealError::Failed(format!("cannot sign the seal: {error}")))?;
    let bytes = serde_json::to_vec(&seal)
        .map_err(|error| SealError::Failed(format!("the seal does not serialize: {error}")))?;
    let key = object_key(
        &target.root,
        &layout::seal(&seal.source_id, &seal.backup_id),
    );
    match bucket.put_new(&key, bytes).await {
        Ok(()) => Ok(()),
        Err(quoro_providers::object_store::ObjectStoreError::AlreadyExists) => {
            // A retry after the row update failed: accept the stored seal only
            // if it is really this backup's.
            let stored = bucket.get(&key).await.map_err(|error| {
                tracing::warn!(backup_id = %seal.backup_id, error = %error, "cannot read the existing seal back");
                SealError::Retry(ApiError::internal(&error))
            })?;
            let stored: Seal = serde_json::from_slice(&stored).map_err(|error| {
                SealError::Retry(ApiError::internal(&format!("the stored seal: {error}")))
            })?;
            if verify_seal(&stored, &keys.signing_key().verifying_key()).is_err()
                || stored.backup_id != seal.backup_id
                || stored.manifest_digest != seal.manifest_digest
            {
                return Err(SealError::Failed(
                    "the seal object does not verify".to_string(),
                ));
            }
            Ok(())
        }
        Err(error) => Err(SealError::Failed(format!("cannot write the seal: {error}"))),
    }
}

/// A backup that never reached a terminal status 24 hours after it was
/// created is `failed` with `timeout` (`backup-format.md` section 7.1).
///
/// # Errors
///
/// A database error.
pub(crate) async fn time_out(
    store: &Store,
    now: time::OffsetDateTime,
) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, node_id, created_at FROM backups \
         WHERE status IN ('queued', 'running')",
    )
    .fetch_all(store.pool())
    .await?;
    let mut changed = Vec::new();
    for row in &rows {
        let backup_id: String = row.try_get("id")?;
        let node_id: String = row.try_get("node_id")?;
        let created_at: String = row.try_get("created_at")?;
        let Some(created) = parse_time(&created_at) else {
            continue;
        };
        if now >= time_after(created, BACKUP_TIMEOUT)
            && fail(store, &backup_id, &node_id, "timeout").await?
        {
            changed.push(node_id);
        }
    }
    Ok(changed)
}

/// The `backup.succeeded`/`backup.failed` payload (`events.schema.json`).
fn event_data(backup: &BackupRecord, error: Option<Value>) -> Value {
    let mut data = json!({
        "backup_id": backup.id,
        "repository_id": backup.repository_id,
        "source_kind": backup.source_kind.as_str(),
        "trigger": backup.trigger.as_str(),
    });
    if let Some(app_id) = &backup.app_id {
        data["app_id"] = Value::from(app_id.clone());
    }
    if let Some(volume_id) = &backup.volume_id {
        data["volume_id"] = Value::from(volume_id.clone());
    }
    data["node_id"] = Value::from(backup.node_id.clone());
    match error {
        Some(error) => {
            data["error"] = error;
        }
        None => {
            data["logical_bytes"] = Value::from(backup.logical_bytes.unwrap_or(0));
            data["uploaded_bytes"] = Value::from(backup.uploaded_bytes.unwrap_or(0));
            if let Some(method) = backup.snapshot_method {
                data["snapshot_method"] = Value::from(snapshot_name(method));
            }
            if let Some(pause_ms) = backup.pause_ms {
                data["pause_ms"] = Value::from(pause_ms.max(0));
            }
        }
    }
    data
}

/// The stored and wire form of a snapshot method.
fn snapshot_name(method: SnapshotMethod) -> &'static str {
    match method {
        SnapshotMethod::Reflink => "reflink",
        SnapshotMethod::PauseCopy => "pause_copy",
    }
}

fn snapshot_method(method: wire::SnapshotMethod) -> SnapshotMethod {
    match method {
        wire::SnapshotMethod::Reflink => SnapshotMethod::Reflink,
        wire::SnapshotMethod::PauseCopy => SnapshotMethod::PauseCopy,
    }
}

fn source_kind(kind: BackupSourceKind) -> quoro_backup::SourceKind {
    match kind {
        BackupSourceKind::Volume => quoro_backup::SourceKind::Volume,
        BackupSourceKind::ControlPlane => quoro_backup::SourceKind::ControlPlane,
    }
}

/// 64 lowercase hex characters (`backup-format.md` section 6.1).
fn is_lower_hex_64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
