// SPDX-License-Identifier: AGPL-3.0-only
//! Retention and garbage collection (`backup-format.md` section 9).
//!
//! After a backup is sealed, the oldest succeeded backups of that source
//! beyond its `keep` count lose their seal first, then their manifest, and
//! only then get `pruned_at`: a backup without a seal cannot be restored, and
//! the record stays. Control-plane backups are pruned per trigger: the
//! schedule's `keep` counts the scheduled ones, and upgrade backups are kept
//! three at a time (`backup-format.md` sections 7.2 and 9, ADR 0158). The
//! mark-and-sweep then removes what no sealed backup
//! references: manifests without a seal older than 48 hours and chunks no live
//! manifest lists. It runs at most once a day, never while an object listing
//! fails and never while a backup of the repository is queued or running.

use crate::api::apps::BackupSchedule;
use crate::api::backups::{
    BACKUP_COLUMNS, BackupRecord, BackupSourceKind, BackupTrigger, backup_from_row, backup_payload,
};
use crate::changelog::{self, Op};
use crate::error::ApiError;
use crate::keys::ClusterKeys;
use crate::store::{Store, now_rfc3339};
use quoro_backup::layout;
use serde_json::json;

use super::{
    Backups, Bucket, GC_GRACE, GC_PERIOD, RepositoryTarget, decrypt_repo_key, object_key,
    parse_time, repo, time_after,
};

/// How many upgrade backups retention keeps (`backup-format.md` section 7.2,
/// ADR 0158). They are counted apart from the scheduled ones, so
/// `control_plane_schedule.keep` never touches them.
pub const UPGRADE_KEEP: i64 = 3;

/// What one sweep removed, for the `backup.gc` audit record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcCounts {
    /// Manifests without a seal older than 48 hours.
    pub manifests_deleted: u64,
    /// Chunks no live manifest references, older than 48 hours.
    pub chunks_deleted: u64,
}

/// Prunes the source's oldest succeeded backups beyond `keep`, then runs a
/// garbage-collection sweep when one is due.
///
/// # Errors
///
/// A database error. A delete the bucket refuses stops pruning; it does not
/// fail the seal that already committed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn after_seal(
    store: &Store,
    data_key: &crate::crypto::DataKey,
    keys: &ClusterKeys,
    runtime: &Backups,
    repository: &repo::Repository,
    target: &RepositoryTarget,
    bucket: &Bucket,
    backup: &BackupRecord,
    now: time::OffsetDateTime,
) -> Result<(), ApiError> {
    let keep = keep_for(store, repository, backup).await?;
    let control_plane = backup.source_kind == BackupSourceKind::ControlPlane;
    // A control-plane backup prunes only its own trigger's backups: the
    // schedule's `keep` counts the scheduled ones, and upgrade backups are
    // counted on their own (`backup-format.md` section 7.2, ADR 0158).
    let rows = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups \
         WHERE repository_id = ? AND source_kind = ? AND status = 'succeeded' \
           AND pruned_at IS NULL AND {} AND (? = 0 OR \"trigger\" = ?) \
         ORDER BY id DESC",
        source_clause()
    ))
    .bind(&repository.id)
    .bind(backup.source_kind.as_str())
    .bind(backup.app_id.as_deref().unwrap_or_default())
    .bind(control_plane)
    .bind(i64::from(control_plane))
    .bind(backup.trigger.as_str())
    .fetch_all(store.pool())
    .await?;
    let kept: Vec<BackupRecord> = rows
        .iter()
        .map(backup_from_row)
        .collect::<Result<Vec<_>, _>>()?;

    let mut pruned = 0usize;
    for stale in kept.iter().skip(usize::try_from(keep).unwrap_or(0)) {
        let source_id = match stale.source_kind {
            BackupSourceKind::Volume => stale.app_id.clone().unwrap_or_default(),
            BackupSourceKind::ControlPlane => repo::cluster_id(store).await?,
        };
        let seal_key = object_key(&target.root, &layout::seal(&source_id, &stale.id));
        if let Err(error) = bucket.delete(&seal_key).await {
            tracing::warn!(backup_id = %stale.id, error = %error, "cannot delete the seal; pruning stops");
            return Ok(());
        }
        let manifest_key = stale
            .manifest_key
            .clone()
            .unwrap_or_else(|| layout::manifest(&source_id, &stale.id));
        if let Err(error) = bucket
            .delete(&object_key(&target.root, &manifest_key))
            .await
        {
            tracing::warn!(backup_id = %stale.id, error = %error, "cannot delete the manifest; pruning stops");
            return Ok(());
        }
        let mut w = store.begin_write().await?;
        let pruned_at = now_rfc3339();
        sqlx::query("UPDATE backups SET pruned_at = ? WHERE id = ?")
            .bind(&pruned_at)
            .bind(&stale.id)
            .execute(&mut *w.tx)
            .await?;
        let mut after = stale.clone();
        after.pruned_at = Some(pruned_at);
        let seq = changelog::append(
            &mut w.tx,
            &changelog::Actor::system("backups"),
            "backup",
            &after.id,
            Op::Update,
            backup_payload(&after),
        )
        .await?;
        let record = super::system_record(
            "backup.prune",
            "backup",
            &after.id,
            json!({}),
            crate::audit::Outcome::Ok,
            None,
            Some(seq),
        );
        crate::audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        pruned += 1;
    }

    // A sweep at most once a day, and only after a pruning (`backup-format.md`
    // section 9). A seal that pruned nothing does not start one: while it
    // runs, no backup of the repository can be queued.
    if pruned > 0
        && runtime
            .last_sweep()
            .is_none_or(|last| now >= time_after(last, GC_PERIOD))
    {
        match sweep(store, data_key, keys, runtime, &repository.id, now).await {
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(repository_id = %repository.id, error = ?error, "backup gc failed")
            }
        }
    }
    Ok(())
}

/// The `keep` of the source's schedule: the app's `backup_schedule` for a
/// volume, the repository's `control_plane_schedule` for the scheduled
/// control-plane backups, [`UPGRADE_KEEP`] for an upgrade backup.
async fn keep_for(
    store: &Store,
    repository: &repo::Repository,
    backup: &BackupRecord,
) -> Result<i64, ApiError> {
    if backup.source_kind == BackupSourceKind::ControlPlane {
        return Ok(if backup.trigger == BackupTrigger::Upgrade {
            UPGRADE_KEEP
        } else {
            repository.control_plane_schedule.keep
        });
    }
    let Some(app_id) = &backup.app_id else {
        return Ok(BackupSchedule::default().keep);
    };
    let row: Option<(String,)> = sqlx::query_as("SELECT backup_schedule FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(store.pool())
        .await?;
    match row {
        Some((text,)) => Ok(repo::parse_schedule(&text)?.keep),
        // A deleted app keeps its backups and its default retention.
        None => Ok(BackupSchedule::default().keep),
    }
}

/// The sweep of `backup-format.md` section 9.
///
/// Lists the seals, verifies them, reads and decrypts their manifests to fix
/// the live chunk ids, then deletes stale manifests without a seal and stale
/// non-live chunks. Any failed listing stops the sweep without deleting
/// anything.
///
/// # Errors
///
/// A database error for the repository row or the audit record. A bucket
/// failure is returned as zero counts: nothing is deleted.
pub async fn sweep(
    store: &Store,
    data_key: &crate::crypto::DataKey,
    keys: &ClusterKeys,
    runtime: &Backups,
    repository_id: &str,
    now: time::OffsetDateTime,
) -> Result<GcCounts, ApiError> {
    let Some(repository) = repo::by_id(store, repository_id).await? else {
        return Err(ApiError::not_found("backup repository"));
    };
    let active: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM backups WHERE repository_id = ? AND status IN ('queued', 'running') LIMIT 1",
    )
    .bind(repository_id)
    .fetch_optional(store.pool())
    .await?;
    if active.is_some() {
        return Ok(GcCounts::default());
    }
    let _guard = runtime.begin_sweep();
    let target = repo::target(store, data_key, &repository).await?;
    let bucket = runtime.open_bucket(&target)?;
    let repo_key = decrypt_repo_key(data_key, &repository.id, &repository.repo_key_enc)?;

    let seal_objects = match bucket.list(&object_key(&target.root, "seals/")).await {
        Ok(objects) => objects,
        Err(error) => {
            tracing::warn!(repository_id, error = %error, "cannot list the seals; the sweep stops");
            return Ok(GcCounts::default());
        }
    };
    let mut seals = Vec::new();
    let mut live_sources: Vec<(String, String)> = Vec::new();
    for object in &seal_objects {
        // A seal that cannot be read must stop the sweep: skipping it would
        // treat a sealed backup's chunks as dead and delete them. Only a seal
        // that was read and is not a valid seal (garbage, or not signed by
        // this cluster) seals nothing.
        let bytes = match bucket.get(&object.key).await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(key = %object.key, error = %error, "cannot read a seal; the sweep stops");
                return Ok(GcCounts::default());
            }
        };
        let Ok(seal) = serde_json::from_slice::<quoro_backup::Seal>(&bytes) else {
            tracing::warn!(key = %object.key, "an object under seals/ is not a seal; it seals nothing");
            continue;
        };
        if quoro_backup::verify_seal(&seal, &keys.signing_key().verifying_key()).is_err() {
            tracing::warn!(key = %object.key, "a seal does not verify; it seals nothing");
            continue;
        }
        live_sources.push((seal.source_id.clone(), seal.backup_id.clone()));
        seals.push(seal);
    }

    let mut live_chunks: std::collections::HashSet<String> = std::collections::HashSet::new();
    for seal in &seals {
        let key = object_key(&target.root, &seal.manifest_key);
        let Ok(object) = bucket.get(&key).await else {
            tracing::warn!(backup_id = %seal.backup_id, "cannot read a sealed manifest; the sweep stops");
            return Ok(GcCounts::default());
        };
        let source_keys = repo_key.source_keys(&seal.source_id);
        let Ok(manifest) = quoro_backup::open_manifest(
            &source_keys,
            &seal.backup_id,
            &seal.source_id,
            &seal.manifest_digest,
            &object,
        ) else {
            tracing::warn!(backup_id = %seal.backup_id, "a sealed manifest does not verify; the sweep stops");
            return Ok(GcCounts::default());
        };
        live_chunks.extend(manifest.chunks.into_iter().map(|chunk| chunk.id));
    }

    let mut counts = GcCounts::default();
    let manifest_objects = match bucket.list(&object_key(&target.root, "manifests/")).await {
        Ok(objects) => objects,
        Err(error) => {
            tracing::warn!(repository_id, error = %error, "cannot list the manifests; the sweep stops");
            return Ok(GcCounts::default());
        }
    };
    for object in &manifest_objects {
        let Some(relative) = super::relative_key(&target.root, &object.key) else {
            continue;
        };
        let Some((source_id, backup_id)) = parse_manifest_key(relative) else {
            continue;
        };
        if live_sources
            .iter()
            .any(|(s, b)| s == source_id && b == backup_id)
        {
            continue;
        }
        if !older_than(&object.last_modified, now, GC_GRACE) {
            continue;
        }
        match bucket.delete(&object.key).await {
            Ok(()) => counts.manifests_deleted += 1,
            Err(error) => {
                tracing::warn!(key = %object.key, error = %error, "cannot delete a stale manifest");
            }
        }
    }

    let chunk_objects = match bucket.list(&object_key(&target.root, "chunks/")).await {
        Ok(objects) => objects,
        Err(error) => {
            tracing::warn!(repository_id, error = %error, "cannot list the chunks; the sweep stops");
            return Ok(GcCounts::default());
        }
    };
    for object in &chunk_objects {
        let Some(id) = object.key.rsplit('/').next() else {
            continue;
        };
        if live_chunks.contains(id) {
            continue;
        }
        if !older_than(&object.last_modified, now, GC_GRACE) {
            continue;
        }
        match bucket.delete(&object.key).await {
            Ok(()) => counts.chunks_deleted += 1,
            Err(error) => {
                tracing::warn!(key = %object.key, error = %error, "cannot delete a dead chunk");
            }
        }
    }

    {
        let mut w = store.begin_write().await?;
        let record = super::system_record(
            "backup.gc",
            "backup_repository",
            &repository.id,
            json!({
                "manifests_deleted": counts.manifests_deleted,
                "chunks_deleted": counts.chunks_deleted,
            }),
            crate::audit::Outcome::Ok,
            None,
            None,
        );
        crate::audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
    }
    runtime.note_sweep(now);
    Ok(counts)
}

/// `manifests/<source_id>/<backup_id>`.
fn parse_manifest_key(relative: &str) -> Option<(&str, &str)> {
    let rest = relative.strip_prefix("manifests/")?;
    let (source_id, backup_id) = rest.split_once('/')?;
    (!source_id.is_empty() && !backup_id.is_empty() && !backup_id.contains('/'))
        .then_some((source_id, backup_id))
}

/// Whether a listing's `last_modified` is older than `grace`. A timestamp the
/// store does not give in RFC 3339 counts as young: the sweep never deletes
/// what it cannot date.
fn older_than(last_modified: &str, now: time::OffsetDateTime, grace: std::time::Duration) -> bool {
    parse_time(last_modified).is_some_and(|written| now >= time_after(written, grace))
}

/// The SQL fragment that selects one source's rows. The first placeholder is
/// the app id and the second a boolean for the control-plane source.
const fn source_clause() -> &'static str {
    "((source_kind = 'volume' AND app_id = ?) OR (source_kind = 'control_plane' AND ?))"
}
