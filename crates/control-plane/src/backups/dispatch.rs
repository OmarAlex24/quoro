// SPDX-License-Identifier: AGPL-3.0-only
//! The backup schedule and its timeout loop (`backup-format.md` section 7.1).
//!
//! Every 60 seconds the loop queues the scheduled volume backups that are due
//! and fails the ones that stayed non-terminal for 24 hours. A `cold` app with
//! a `ready` volume and a repository gets a `trigger: schedule` backup when its
//! last succeeded backup is older than `backup_schedule.interval_h`, or when
//! it has none. A failed scheduled backup waits for the next interval; it is
//! not retried sooner, because the interval counts from the last *succeeded*
//! one. The control-plane database has its own schedule and work
//! ([`super::control_plane`]); the loop only queues it.

use crate::AppState;
use crate::api::apps::BackupSchedule;
use crate::api::backups::{BackupRecord, BackupSourceKind, BackupStatus, BackupTrigger};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::request::CallCtx;
use crate::store::Store;
use serde_json::{Value, json};
use sqlx::Row;
use std::time::Duration;

use super::{repo, seal};

/// How often the dispatch loop runs.
pub const DISPATCH_INTERVAL: Duration = Duration::from_secs(60);

/// Starts the dispatch loop. Errors are logged, never propagated: a failed
/// tick is retried on the next one.
pub fn spawn(state: AppState) {
    // A crash mid-backup leaves its temporary database behind
    // (`backup-format.md` section 7.2): the file goes now, and the row is
    // resolved below, before the first tick.
    if let Err(error) = super::control_plane::remove_leftovers(&state.config.data_dir) {
        tracing::warn!(error = ?error, "could not remove a leftover control-plane backup file");
    }
    tokio::spawn(async move {
        // A control-plane backup left `queued` or `running` would block its
        // schedule; it is resolved from its seal object before the first
        // `queue_if_due` (`backup-format.md` section 7.2, "Rows left behind at
        // start"). The bucket I/O runs here, so it does not delay the start.
        if let Err(error) = super::control_plane::resolve_orphaned(&state).await {
            tracing::warn!(error = ?error, "could not resolve the control-plane backups left behind");
        }
        let mut ticker = tokio::time::interval(DISPATCH_INTERVAL);
        loop {
            ticker.tick().await;
            // A demoted primary queues and takes no backup
            // (`node-agent-protocol.md` section 7.4).
            if state.store.stops_for_demotion("the backup dispatch loop") {
                break;
            }
            if let Err(error) = run(&state, time::OffsetDateTime::now_utc()).await
                && !state.store.is_demoted()
            {
                tracing::warn!(error = ?error, "backup dispatch failed");
            }
        }
    });
}

/// What one tick of [`run`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tick {
    /// Scheduled backups queued.
    pub queued: usize,
    /// Backups that timed out and were failed.
    pub timed_out: usize,
}

/// One tick of the schedule and timeout loop at `now`. The tests call it with
/// a fake time.
///
/// # Errors
///
/// A database error. Object-store work never runs here.
pub async fn run(state: &AppState, now: time::OffsetDateTime) -> Result<Tick, ApiError> {
    let mut tick = Tick::default();
    // A sweep dispatches no backup (`backup-format.md` section 9).
    if state.backups.is_sweeping() {
        return Ok(tick);
    }
    for node_id in seal::time_out(&state.store, now).await? {
        tick.timed_out += 1;
        state.hub.node_changed(&node_id);
    }
    for node_id in scheduled_due(&state.store, now).await? {
        tick.queued += 1;
        state.hub.node_changed(&node_id);
    }
    // The control-plane source queues its own backup; no node is notified
    // (`backup-format.md` section 7.2).
    for node_id in dispatch_control_plane(state, now).await? {
        tick.queued += 1;
        state.hub.node_changed(&node_id);
    }
    Ok(tick)
}

/// The nodes whose `cold` apps had a scheduled backup queued.
async fn scheduled_due(store: &Store, now: time::OffsetDateTime) -> Result<Vec<String>, ApiError> {
    let Some(repository) = repo::any(store).await? else {
        return Ok(Vec::new());
    };
    let rows = sqlx::query(
        "SELECT a.id AS app_id, a.backup_schedule, a.project_id, v.id AS volume_id, v.node_id \
         FROM apps a JOIN volumes v ON v.app_id = a.id \
         WHERE a.ha_policy = 'cold' AND v.state = 'ready' ORDER BY a.id",
    )
    .fetch_all(store.pool())
    .await?;
    let mut notified = Vec::new();
    for row in &rows {
        let app_id: String = row.try_get("app_id")?;
        let schedule_json: String = row.try_get("backup_schedule")?;
        let schedule: BackupSchedule = repo::parse_schedule(&schedule_json)?;
        let volume_id: String = row.try_get("volume_id")?;
        let node_id: String = row.try_get("node_id")?;
        let project_id: Option<String> = row.try_get("project_id")?;

        let active: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM backups WHERE volume_id = ? AND status IN ('queued', 'running') LIMIT 1",
        )
        .bind(&volume_id)
        .fetch_optional(store.pool())
        .await?;
        if active.is_some() {
            continue;
        }
        // The interval counts from the last succeeded backup; a failed
        // scheduled one waits for the same interval, never a shorter one
        // (`backup-format.md` section 7.1).
        let last: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT MAX(created_at) FROM backups \
             WHERE volume_id = ? AND status IN ('succeeded', 'failed')",
        )
        .bind(&volume_id)
        .fetch_optional(store.pool())
        .await?;
        let last = last
            .and_then(|row| row.0)
            .and_then(|text| super::parse_time(&text));
        let due = match last {
            None => true,
            Some(last) => {
                let interval = time::Duration::hours(schedule.interval_h);
                now >= last + interval
            }
        };
        if !due {
            continue;
        }
        let Some(lease) = current_lease(store, &app_id).await? else {
            continue;
        };
        let backup = queue(
            store,
            NewBackup {
                repository_id: &repository.id,
                source_kind: BackupSourceKind::Volume,
                app_id: Some(&app_id),
                volume_id: Some(&volume_id),
                node_id: &node_id,
                fencing_token: Some(lease),
                trigger: BackupTrigger::Schedule,
                created_by_type: "system",
                created_by_id: "backups",
                created_at: &super::timestamp(now),
                project_id: project_id.as_deref(),
                request: None,
                dispatch_audit: true,
            },
        )
        .await?;
        let _ = backup;
        notified.push(node_id);
    }
    Ok(notified)
}

/// A row to insert into `backups`, with what the audit trail needs.
pub(crate) struct NewBackup<'a> {
    pub repository_id: &'a str,
    pub source_kind: BackupSourceKind,
    pub app_id: Option<&'a str>,
    pub volume_id: Option<&'a str>,
    pub node_id: &'a str,
    pub fencing_token: Option<i64>,
    pub trigger: BackupTrigger,
    pub created_by_type: &'a str,
    pub created_by_id: &'a str,
    /// The request or tick time, as `backups.created_at`.
    pub created_at: &'a str,
    pub project_id: Option<&'a str>,
    /// The API call that asked for the backup: its `ok` audit record is
    /// written in the same transaction.
    pub request: Option<(&'a CallCtx, &'a Value)>,
    /// A scheduled backup writes the `backup.dispatch` system record.
    pub dispatch_audit: bool,
}

/// Inserts the `queued` row, its change-log entry and its audit record in one
/// transaction.
///
/// # Errors
///
/// `conflict` when the volume already has a non-terminal backup (the unique
/// index also refuses it), or a database error.
pub(crate) async fn queue(store: &Store, new: NewBackup<'_>) -> Result<BackupRecord, ApiError> {
    let backup = BackupRecord {
        id: crate::deploy::new_id_with_prefix("bkp"),
        repository_id: new.repository_id.to_string(),
        source_kind: new.source_kind,
        app_id: new.app_id.map(str::to_string),
        volume_id: new.volume_id.map(str::to_string),
        node_id: new.node_id.to_string(),
        status: BackupStatus::Queued,
        trigger: new.trigger,
        fencing_token: new.fencing_token,
        snapshot_method: None,
        pause_ms: None,
        logical_bytes: None,
        uploaded_bytes: None,
        chunk_count: None,
        manifest_key: None,
        manifest_digest: None,
        error: None,
        created_by_type: new.created_by_type.to_string(),
        created_by_id: new.created_by_id.to_string(),
        created_at: new.created_at.to_string(),
        started_at: None,
        finished_at: None,
        pruned_at: None,
    };
    let mut w = store.begin_write().await?;
    if let Some(volume_id) = new.volume_id {
        let active: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM backups WHERE volume_id = ? AND status IN ('queued', 'running') LIMIT 1",
        )
        .bind(volume_id)
        .fetch_optional(&mut *w.tx)
        .await?;
        if active.is_some() {
            return Err(ApiError::conflict(
                "the volume already has a backup that is not terminal",
            ));
        }
    }
    let actor = match new.request {
        Some((ctx, _)) => ctx.identity.actor(),
        None => Actor::system("backups"),
    };
    sqlx::query(
        "INSERT INTO backups (id, repository_id, source_kind, app_id, volume_id, node_id, status, \
         \"trigger\", fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes, \
         chunk_count, manifest_key, manifest_digest, error, created_by_type, created_by_id, \
         created_at, started_at, finished_at, pruned_at) \
         VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, ?, ?, ?, NULL, NULL, NULL)",
    )
    .bind(&backup.id)
    .bind(&backup.repository_id)
    .bind(backup.source_kind.as_str())
    .bind(&backup.app_id)
    .bind(&backup.volume_id)
    .bind(&backup.node_id)
    .bind(backup.trigger.as_str())
    .bind(backup.fencing_token)
    .bind(new.created_by_type)
    .bind(new.created_by_id)
    .bind(&backup.created_at)
    .execute(&mut *w.tx)
    .await
    .map_err(|error| {
        if error
            .as_database_error()
            .is_some_and(|error| error.is_unique_violation())
        {
            ApiError::conflict("the volume already has a backup that is not terminal")
        } else {
            ApiError::from(error)
        }
    })?;
    let seq = changelog::append(
        &mut w.tx,
        &actor,
        "backup",
        &backup.id,
        Op::Create,
        crate::api::backups::backup_payload(&backup),
    )
    .await?;
    if let Some((ctx, params)) = new.request {
        let target = new.app_id.map_or_else(
            || crate::api::backups::backup_target(&backup.id),
            crate::api::apps::app_target,
        );
        ctx.append_ok(
            &mut w.tx,
            Some(quoro_types::Scope::BackupWrite),
            Some(&target),
            new.project_id,
            params,
            Some(seq),
        )
        .await?;
    } else if new.dispatch_audit {
        let record = super::system_record(
            "backup.dispatch",
            "backup",
            &backup.id,
            json!({ "app_id": backup.app_id, "volume_id": backup.volume_id, "trigger": backup.trigger.as_str() }),
            crate::audit::Outcome::Ok,
            None,
            Some(seq),
        );
        crate::audit::append(&mut w.tx, &record).await?;
    }
    w.commit().await?;
    Ok(backup)
}

/// The fencing token of the app's current `local` lease.
///
/// # Errors
///
/// A database error.
pub(crate) async fn current_lease(store: &Store, app_id: &str) -> Result<Option<i64>, ApiError> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT fencing_token FROM leases \
         WHERE resource_type = 'app' AND resource_id = ? ORDER BY id LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(store.pool())
    .await?;
    Ok(row.map(|row| row.0))
}

/// The control-plane source's schedule (`backup-format.md` section 7.2,
/// ADR 0148). The primary does the work itself, so no node is notified and the
/// returned list is always empty.
async fn dispatch_control_plane(
    state: &AppState,
    now: time::OffsetDateTime,
) -> Result<Vec<String>, ApiError> {
    let Some(backup) = super::control_plane::queue_if_due(state, now).await? else {
        return Ok(Vec::new());
    };
    let backup_id = backup.id;
    let state = state.clone();
    // Its own task, so the 60 s loop never waits on a vacuum and an upload.
    tokio::spawn(async move {
        if let Err(error) =
            super::control_plane::run_control_plane_backup(&state, &backup_id, now).await
        {
            tracing::warn!(backup_id = %backup_id, error = ?error, "control-plane backup failed");
        }
    });
    Ok(Vec::new())
}
