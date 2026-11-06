// SPDX-License-Identifier: AGPL-3.0-only
//! Restoring a volume backup onto a node and moving the app there
//! (`openapi.yaml` `restoreBackup`, `backup-format.md` section 8.1,
//! `node-agent-protocol.md` sections 5.11, 6.7 and 8.3, ADR 0147).
//!
//! The primary accepts the restore in one transaction: it detaches the volume
//! being replaced (the unique index allows one attached volume per app),
//! inserts the new volume in `restoring` on the target node, stops the app and
//! revokes its `local` lease with reason `moved`. From then on, a node other
//! than the target no longer finds the app's instances in its desired state
//! and deletes them (`session::desired`, issue #287). The target node
//! downloads, verifies and writes the snapshot; when it reports the volume
//! `ready`, [`apply_status`] grants the node a new `local` lease with a higher
//! fencing token, pins the app's placement there, moves the instances there
//! and starts the app. A reported `failed` leaves the app stopped with no
//! lease, and its instances back in the desired state of the node they are on.

use crate::AppState;
use crate::api::apps::{APP_COLUMNS, AppPlacement, AppStatus, app_from_row};
use crate::api::backups::{
    BACKUP_COLUMNS, BackupRecord, BackupSourceKind, BackupStatus, backup_from_row, backup_target,
};
use crate::api::vms::{VM_COLUMNS, vm_from_row};
use crate::api::volumes::{VOLUME_COLUMNS, Volume, VolumeState};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::leases::{self, Lease};
use crate::request::CallCtx;
use crate::store::{Store, now_rfc3339};
use quoro_proto::wire;
use quoro_proto::{LeaseRevoke, RevokeReason};
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};

/// A gibibyte, for the size a restored backup needs when its volume is gone.
const GIB: i64 = 1024 * 1024 * 1024;

/// Body of `restoreBackup` (`openapi.yaml` `RestoreBackupRequest`).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[schema(
    title = "RestoreBackupRequest",
    description = "Where to restore a volume backup."
)]
pub struct RestoreBackupRequest {
    /// Connected node with the `node` role that gets the new volume and then
    /// runs the app. It may be the volume's current node.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
}

/// A restore the primary accepted, with what the nodes must hear.
pub(crate) struct AcceptedRestore {
    /// The new volume, in `restoring`.
    pub volume: Volume,
    /// The node that held the replaced volume, when there was one.
    pub old_node_id: Option<String>,
    /// The revoke the old holder must receive (reason `moved`).
    pub revoke: Option<LeaseRevoke>,
    /// The nodes that held the app's instances. Each must hear a snapshot
    /// with the instance stopped, or without it when the restore moves the
    /// app to another node.
    pub vm_node_ids: Vec<String>,
}

/// The restores a status report moved, with the leases the nodes must hear.
#[derive(Default)]
pub(crate) struct AppliedRestores {
    /// Whether a node's desired state changed.
    pub desired_changed: bool,
    /// New `local` leases, one per restore that became `ready`.
    pub grants: Vec<Lease>,
    /// Other nodes whose desired state changed: the nodes the app's instances
    /// moved away from (a `ready` restore), so they delete them, and the
    /// nodes the instances stayed on (a `failed` restore), so they get them
    /// back.
    pub other_node_ids: Vec<String>,
}

impl AppliedRestores {
    fn merge(&mut self, other: Self) {
        self.desired_changed |= other.desired_changed;
        self.grants.extend(other.grants);
        self.other_node_ids.extend(other.other_node_ids);
    }
}

/// Accepts `restoreBackup` after the authorization check: one transaction
/// detaches the old volume, creates the new one and moves the app there
/// (`openapi.yaml` `restoreBackup`).
///
/// # Errors
///
/// `not_found` for an unknown backup, `validation_failed` for a `node_id` that
/// is not a node with the `node` role, `conflict` for every refusal of the
/// contract, or a database error.
pub(crate) async fn accept(
    state: &AppState,
    ctx: &CallCtx,
    backup_id: &str,
    body: &RestoreBackupRequest,
    params: &Value,
) -> Result<AcceptedRestore, ApiError> {
    if !crate::deploy::is_id_with_prefix("bkp", backup_id) {
        return Err(ApiError::not_found("backup"));
    }
    target_node(state, &body.node_id).await?;
    let backup_row = sqlx::query(&format!(
        "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
    ))
    .bind(backup_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("backup"))?;
    let backup = backup_from_row(&backup_row)?;
    refuse_unrestorable(state, &backup).await?;
    let app_id = backup
        .app_id
        .clone()
        .ok_or_else(|| ApiError::conflict("the backup has no app"))?;
    let app_row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(&app_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::conflict("the backup's app is gone"))?;
    let app = app_from_row(&app_row)?;

    let old = old_volume(state, &app_id).await?;
    let (size_gb, mount_path) = match &old {
        Some(volume) => (volume.size_gb, volume.mount_path.clone()),
        None => replaced_volume_shape(state, &backup).await?,
    };

    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    // 1. The unique index allows one attached volume per app, so the old one
    //    leaves `creating`/`ready` first (`migration 0006`).
    let old_node_id = match &old {
        Some(volume) => {
            let mut after = volume.clone();
            after.state = VolumeState::Detached;
            after.updated_at = now.clone();
            sqlx::query("UPDATE volumes SET state = 'detached', updated_at = ? WHERE id = ?")
                .bind(&after.updated_at)
                .bind(&after.id)
                .execute(&mut *w.tx)
                .await?;
            changelog::append(
                &mut w.tx,
                &ctx.identity.actor(),
                "volume",
                &after.id,
                Op::Update,
                after.payload()?,
            )
            .await?;
            Some(volume.node_id.clone())
        }
        None => None,
    };

    // 2. The new volume restores into a file the node writes.
    let volume = Volume {
        id: crate::deploy::new_id_with_prefix("vol"),
        app_id: app_id.clone(),
        node_id: body.node_id.clone(),
        size_gb,
        mount_path,
        state: VolumeState::Restoring,
        allocated_bytes: None,
        restored_from_backup_id: Some(backup.id.clone()),
        error: None,
        quarantines: Vec::new(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    sqlx::query(
        "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
         allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, 'restoring', NULL, ?, NULL, ?, ?)",
    )
    .bind(&volume.id)
    .bind(&volume.app_id)
    .bind(&volume.node_id)
    .bind(volume.size_gb)
    .bind(&volume.mount_path)
    .bind(&volume.restored_from_backup_id)
    .bind(&volume.created_at)
    .bind(&volume.updated_at)
    .execute(&mut *w.tx)
    .await?;
    let change_seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "volume",
        &volume.id,
        Op::Create,
        volume.payload()?,
    )
    .await?;

    // 3. The app stops, the way `stopApp` does. On a restore onto another
    //    node its instances also leave their node's desired state now, while
    //    the new volume is `restoring` there (`session::desired`).
    let vm_node_ids = stop_app(&mut w.tx, &app_id, &ctx.identity.actor()).await?;

    // 4. The old holder's `local` lease ends with the move (section 8.3).
    let revoke = leases::revoke_app_lease(&mut w.tx, &app_id, &ctx.identity.actor())
        .await?
        .map(|lease| lease.revoke(RevokeReason::Moved));

    // 5. The call itself.
    ctx.append_ok(
        &mut w.tx,
        Some(quoro_types::Scope::BackupRestore),
        Some(&backup_target(&backup.id)),
        Some(&app.project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(AcceptedRestore {
        volume,
        old_node_id,
        revoke,
        vm_node_ids,
    })
}

/// Applies the volume states a restoring node reported (section 5.3). Runs
/// before [`crate::service::volumes::apply_status`], which then sees the row
/// already at its new state and writes nothing.
///
/// # Errors
///
/// A database error.
pub(crate) async fn apply_status(
    store: &Store,
    node_id: &str,
    volumes: &[wire::VolumeStatus],
) -> Result<AppliedRestores, ApiError> {
    let mut applied = AppliedRestores::default();
    for reported in volumes {
        let row = sqlx::query(&format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
        ))
        .bind(&reported.volume_id)
        .fetch_optional(store.pool())
        .await?;
        let Some(row) = row else {
            continue;
        };
        let volume = crate::api::volumes::volume_from_row(&row)?;
        if volume.node_id != node_id
            || volume.state != VolumeState::Restoring
            || volume.restored_from_backup_id.is_none()
        {
            continue;
        }
        match reported.state {
            wire::VolumeState::Ready => {
                let outcome = restore_ready(store, node_id, &volume, reported).await?;
                applied.merge(outcome);
            }
            wire::VolumeState::Failed => {
                let vm_node_ids = restore_failed(store, node_id, &volume, reported).await?;
                applied.desired_changed = true;
                applied.other_node_ids.extend(vm_node_ids);
            }
            _ => {}
        }
    }
    Ok(applied)
}

/// The `ready` transition: the row, the new lease, the placement, the app and
/// the event, in one transaction (section 8.3, `events.schema.json`).
///
/// A repeated report finds the row already `ready` and changes nothing, so the
/// transition is applied once.
async fn restore_ready(
    store: &Store,
    node_id: &str,
    before: &Volume,
    reported: &wire::VolumeStatus,
) -> Result<AppliedRestores, ApiError> {
    let actor = Actor::system("leases");
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
    ))
    .bind(&before.id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::internal(&"the restoring volume disappeared"))?;
    let mut current = crate::api::volumes::volume_from_row(&row)?;
    if current.state != VolumeState::Restoring {
        return Ok(AppliedRestores::default());
    }
    current.state = VolumeState::Ready;
    current.allocated_bytes = reported
        .allocated_bytes
        .map(|bytes| i64::try_from(bytes).unwrap_or(i64::MAX))
        .or(current.allocated_bytes);
    current.error = None;
    current.updated_at = now_rfc3339();
    sqlx::query(
        "UPDATE volumes SET state = 'ready', allocated_bytes = ?, error = NULL, updated_at = ? \
         WHERE id = ?",
    )
    .bind(current.allocated_bytes)
    .bind(&current.updated_at)
    .bind(&current.id)
    .execute(&mut *w.tx)
    .await?;
    let volume_seq = changelog::append(
        &mut w.tx,
        &actor,
        "volume",
        &current.id,
        Op::Update,
        current.payload()?,
    )
    .await?;

    // `grant_app_lease` takes the app's last token, revoked or not, and adds
    // one, so the target node's token is strictly higher than the old one
    // (section 8.6).
    let lease = leases::grant_app_lease(&mut w.tx, &current.app_id, node_id, &actor).await?;

    // Placement follows the volume, as `createVolume` pins it (ADR 0145); the
    // app's instance moves with it.
    let app_row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(&current.app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::internal(&"the restored app disappeared"))?;
    let mut app = app_from_row(&app_row)?;
    app.stateful = true;
    app.placement = AppPlacement {
        node_ids: vec![node_id.to_string()],
    };
    app.status = AppStatus::Deploying;
    app.updated_at = now_rfc3339();
    let placement = serde_json::to_string(&app.placement)
        .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
    sqlx::query(
        "UPDATE apps SET stateful = 1, placement = ?, status = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&placement)
    .bind(app.status.as_str())
    .bind(&app.updated_at)
    .bind(&app.id)
    .execute(&mut *w.tx)
    .await?;
    changelog::append(
        &mut w.tx,
        &actor,
        "app",
        &app.id,
        Op::Update,
        serde_json::to_value(&app)
            .map_err(|error| ApiError::internal(&format!("app payload: {error}")))?,
    )
    .await?;
    let other_node_ids = move_vms(&mut w.tx, &app.id, node_id, &actor).await?;

    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "restore.succeeded",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: &app.id,
            project_id: Some(&app.project_id),
            request_id: None,
            actor: None,
            data: json!({
                "backup_id": current.restored_from_backup_id,
                "app_id": app.id,
                "volume_id": current.id,
                "node_id": node_id,
                "fencing_token": lease.fencing_token,
            }),
        },
    )
    .await?;
    // audit-log-format.md section 3.5: the `lease.grant` system record.
    crate::audit::append(
        &mut w.tx,
        &system_record(
            "lease.grant",
            &app.id,
            json!({ "node_id": node_id, "fencing_token": lease.fencing_token }),
            Some(volume_seq),
        ),
    )
    .await?;
    w.commit().await?;
    Ok(AppliedRestores {
        desired_changed: true,
        grants: vec![lease],
        other_node_ids,
    })
}

/// The `failed` transition: the row keeps the node's reason and the event says
/// why (`backup-format.md` section 8.1). The app stays stopped, without a lease.
/// Returns the nodes that hold the app's instances: with no volume `restoring`
/// any more, the instances are back in those nodes' desired state, stopped.
async fn restore_failed(
    store: &Store,
    node_id: &str,
    before: &Volume,
    reported: &wire::VolumeStatus,
) -> Result<Vec<String>, ApiError> {
    let actor = Actor::system("volumes");
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
    ))
    .bind(&before.id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::internal(&"the restoring volume disappeared"))?;
    let mut current = crate::api::volumes::volume_from_row(&row)?;
    if current.state != VolumeState::Restoring {
        return Ok(Vec::new());
    }
    let reason = reported
        .error
        .clone()
        .unwrap_or_else(|| "the node reported the restore failed".to_string())
        .chars()
        .take(2000)
        .collect::<String>();
    current.state = VolumeState::Failed;
    current.error = Some(reason.clone());
    current.updated_at = now_rfc3339();
    sqlx::query("UPDATE volumes SET state = 'failed', error = ?, updated_at = ? WHERE id = ?")
        .bind(&current.error)
        .bind(&current.updated_at)
        .bind(&current.id)
        .execute(&mut *w.tx)
        .await?;
    changelog::append(
        &mut w.tx,
        &actor,
        "volume",
        &current.id,
        Op::Update,
        current.payload()?,
    )
    .await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "restore.failed",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: &current.app_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({
                "backup_id": current.restored_from_backup_id,
                "app_id": current.app_id,
                "volume_id": current.id,
                "node_id": node_id,
                "error": reason,
            }),
        },
    )
    .await?;
    let vm_node_ids: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT node_id FROM vms WHERE app_id = ? ORDER BY node_id")
            .bind(&current.app_id)
            .fetch_all(&mut *w.tx)
            .await?;
    w.commit().await?;
    Ok(vm_node_ids)
}

/// The refusals of `openapi.yaml` `restoreBackup` that read the backup, the
/// repository and the node holding the volume.
async fn refuse_unrestorable(state: &AppState, backup: &BackupRecord) -> Result<(), ApiError> {
    if backup.pruned_at.is_some() {
        return Err(ApiError::conflict("the backup was pruned"));
    }
    if backup.status != BackupStatus::Succeeded {
        return Err(ApiError::conflict("the backup is not succeeded"));
    }
    if backup.source_kind == BackupSourceKind::ControlPlane {
        return Err(ApiError::conflict(
            "a control-plane backup is restored offline",
        ));
    }
    if backup.manifest_key.is_none() || backup.manifest_digest.is_none() {
        return Err(ApiError::conflict("the backup has no manifest"));
    }
    if crate::backups::repo::any(&state.store).await?.is_none() {
        return Err(ApiError::conflict("the cluster has no backup repository"));
    }
    let app_id = backup.app_id.as_deref().unwrap_or_default();
    if let Some(volume) = old_volume(state, app_id).await? {
        if volume.state == VolumeState::Restoring {
            return Err(ApiError::conflict(
                "the app already has a restore in progress",
            ));
        }
        if !state.hub.is_connected(&volume.node_id) {
            return Err(ApiError::conflict("the volume's node is not connected"));
        }
    }
    Ok(())
}

/// Checks that `node_id` is a node with the `node` role and no failed fence.
async fn target_node(state: &AppState, node_id: &str) -> Result<(), ApiError> {
    let row = sqlx::query("SELECT roles, fence_failure FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::validation("node_id is not a node with the node role"))?;
    let roles: String = row.try_get("roles")?;
    let roles: Vec<String> = serde_json::from_str(&roles).unwrap_or_default();
    if !roles.iter().any(|role| role == "node") {
        return Err(ApiError::validation(
            "node_id is not a node with the node role",
        ));
    }
    let fence_failure: Option<String> = row.try_get("fence_failure")?;
    if fence_failure.is_some() {
        return Err(ApiError::conflict("the target node has a fence failure"));
    }
    Ok(())
}

/// The app's attached volume, newest first.
async fn old_volume(state: &AppState, app_id: &str) -> Result<Option<Volume>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes \
         WHERE app_id = ? AND state IN ('creating', 'ready', 'restoring') \
         ORDER BY id DESC LIMIT 1"
    ))
    .bind(app_id)
    .fetch_optional(state.store.pool())
    .await?;
    row.as_ref()
        .map(crate::api::volumes::volume_from_row)
        .transpose()
}

/// Size and mount path of the new volume when no attached volume is left: the
/// size follows the backup's logical bytes, the mount path comes from the
/// backup's volume row when retention still has it.
async fn replaced_volume_shape(
    state: &AppState,
    backup: &BackupRecord,
) -> Result<(i64, String), ApiError> {
    let Some(volume_id) = &backup.volume_id else {
        return Err(ApiError::conflict(
            "the backup's volume is gone and its mount path is unknown",
        ));
    };
    let mount_path: Option<(String,)> =
        sqlx::query_as("SELECT mount_path FROM volumes WHERE id = ?")
            .bind(volume_id)
            .fetch_optional(state.store.pool())
            .await?;
    let Some((mount_path,)) = mount_path else {
        return Err(ApiError::conflict(
            "the backup's volume is gone and its mount path is unknown",
        ));
    };
    let logical_bytes = backup.logical_bytes.unwrap_or(0).max(0);
    let size_gb = (logical_bytes.saturating_add(GIB - 1) / GIB).clamp(1, 1024);
    Ok((size_gb, mount_path))
}

/// Stops every instance of `app_id` and its status, as `stopApp` does, and
/// returns the nodes that hold the instances, each once.
async fn stop_app(
    conn: &mut SqliteConnection,
    app_id: &str,
    actor: &Actor,
) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query(&format!("SELECT {VM_COLUMNS} FROM vms WHERE app_id = ?"))
        .bind(app_id)
        .fetch_all(&mut *conn)
        .await?;
    let mut node_ids = Vec::new();
    for row in &rows {
        let mut vm = vm_from_row(row)?;
        if !node_ids.contains(&vm.node_id) {
            node_ids.push(vm.node_id.clone());
        }
        if vm.desired_status == crate::api::vms::DesiredStatus::Stopped {
            continue;
        }
        vm.desired_status = crate::api::vms::DesiredStatus::Stopped;
        vm.updated_at = now_rfc3339();
        sqlx::query("UPDATE vms SET desired_status = 'stopped', updated_at = ? WHERE id = ?")
            .bind(&vm.updated_at)
            .bind(&vm.id)
            .execute(&mut *conn)
            .await?;
        changelog::append(
            conn,
            actor,
            "vm",
            &vm.id,
            Op::Update,
            serde_json::to_value(&vm)
                .map_err(|error| ApiError::internal(&format!("vm payload: {error}")))?,
        )
        .await?;
    }
    let row = sqlx::query("SELECT status FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(row) = row {
        let status: String = row.try_get("status")?;
        if status != AppStatus::Stopped.as_str() {
            let now = now_rfc3339();
            sqlx::query("UPDATE apps SET status = 'stopped', updated_at = ? WHERE id = ?")
                .bind(&now)
                .bind(app_id)
                .execute(&mut *conn)
                .await?;
            changelog::append(
                conn,
                actor,
                "app",
                app_id,
                Op::Update,
                json!({ "id": app_id, "status": "stopped", "updated_at": now }),
            )
            .await?;
        }
    }
    Ok(node_ids)
}

/// Moves the app's instances to `node_id` and asks them to run there. Returns
/// the other nodes the instances left, each once: their desired state no
/// longer names the instances, and they must hear it.
async fn move_vms(
    conn: &mut SqliteConnection,
    app_id: &str,
    node_id: &str,
    actor: &Actor,
) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query(&format!("SELECT {VM_COLUMNS} FROM vms WHERE app_id = ?"))
        .bind(app_id)
        .fetch_all(&mut *conn)
        .await?;
    let mut left_node_ids = Vec::new();
    for row in &rows {
        let mut vm = vm_from_row(row)?;
        if vm.node_id != node_id && !left_node_ids.contains(&vm.node_id) {
            left_node_ids.push(vm.node_id.clone());
        }
        vm.node_id = node_id.to_string();
        vm.desired_status = crate::api::vms::DesiredStatus::Running;
        vm.observed_status = crate::api::vms::ObservedStatus::Creating;
        vm.updated_at = now_rfc3339();
        sqlx::query(
            "UPDATE vms SET node_id = ?, desired_status = 'running', \
             observed_status = 'creating', updated_at = ? WHERE id = ?",
        )
        .bind(&vm.node_id)
        .bind(&vm.updated_at)
        .bind(&vm.id)
        .execute(&mut *conn)
        .await?;
        changelog::append(
            conn,
            actor,
            "vm",
            &vm.id,
            Op::Update,
            serde_json::to_value(&vm)
                .map_err(|error| ApiError::internal(&format!("vm payload: {error}")))?,
        )
        .await?;
    }
    Ok(left_node_ids)
}

/// An `audit-log-format.md` section 3.5 system record of the `leases` actor.
fn system_record(
    operation: &str,
    app_id: &str,
    params: Value,
    change_seq: Option<i64>,
) -> crate::audit::Record {
    crate::audit::Record {
        actor: Actor::system("leases"),
        agent_model: None,
        on_behalf_of: None,
        channel: crate::audit::Channel::System,
        operation: operation.to_string(),
        scope: None,
        target_type: Some("app".to_string()),
        target_id: Some(app_id.to_string()),
        project_id: None,
        params,
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        result: crate::audit::Outcome::Ok,
        result_detail: None,
        change_seq,
    }
}
