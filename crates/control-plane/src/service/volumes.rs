// SPDX-License-Identifier: AGPL-3.0-only
//! Volume operations (`/api/v1/apps/{app_id}/volumes`, `/api/v1/volumes*`;
//! ADR 0145).
//!
//! A volume is a sparse file the node creates from `spec.volumes` and reports
//! back in `status_report.volumes` (node-agent-protocol.md sections 5.3, 5.11
//! and 6.7). This module owns the row's lifecycle: `creating` when the API
//! records it, `ready` or `failed` from what the node reports, `deleting`
//! once a delete is accepted, and gone when the node confirms the file is
//! removed.
//!
//! Creating the app's first volume is what makes the app stateful: the app's
//! placement is pinned to the volume's node and the app gets that node's
//! `local` lease, which the VM spec echoes (section 8.3).

use crate::AppState;
use crate::api::apps::{APP_COLUMNS, AppPlacement, app_from_row, app_payload, app_target};
use crate::api::volumes::{
    CreateVolumeRequest, VOLUME_COLUMNS, Volume, VolumeList, VolumeState, volume_from_row,
    volume_target,
};
use crate::changelog::{self, Op};
use crate::deploy;
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::leases::{self, Lease};
use crate::request::{CallCtx, Resource};
use crate::store::{Store, now_rfc3339};
use quoro_proto::wire;
use quoro_types::Scope;
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// The volume [`create_volume`] wrote, with what the node needs to hear.
pub(crate) struct CreatedVolume {
    /// The row, in `creating`.
    pub volume: Volume,
    /// Node that holds the file.
    pub node_id: String,
    /// The app's `local` lease, granted to that node.
    pub lease: Lease,
}

/// The volume a delete was accepted for, with the lease it released.
pub(crate) struct DeletedVolume {
    /// The row, in `deleting`.
    pub volume: Volume,
    /// Node that holds the file.
    pub node_id: String,
    /// The revoke the holder must receive, when the volume was attached.
    pub revoke: Option<quoro_proto::LeaseRevoke>,
}

/// Whether `path` is a usable mount path (`openapi.yaml`
/// `CreateVolumeRequest.mount_path`, ADR 0145).
///
/// # Errors
///
/// `validation_failed` with the rule the path breaks.
pub(crate) fn validate_mount_path(path: &str) -> Result<(), ApiError> {
    if !(2..=255).contains(&path.len()) {
        return Err(ApiError::validation(
            "mount_path must be 2 to 255 characters long",
        ));
    }
    if !path.starts_with('/') {
        return Err(ApiError::validation("mount_path must be absolute"));
    }
    if !path
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
    {
        return Err(ApiError::validation(
            "mount_path may only hold letters, digits, `.`, `_`, `-` and `/`",
        ));
    }
    let mut segments = path.split('/');
    segments.next();
    let mut count = 0;
    for segment in segments {
        if segment.is_empty() {
            return Err(ApiError::validation("mount_path has an empty segment"));
        }
        if segment == "." || segment == ".." {
            return Err(ApiError::validation("mount_path has a `.` or `..` segment"));
        }
        count += 1;
    }
    if count == 0 {
        return Err(ApiError::validation("mount_path must not be /"));
    }
    for reserved in ["/proc", "/sys", "/dev"] {
        if path == reserved || path.starts_with(&format!("{reserved}/")) {
            return Err(ApiError::validation(format!(
                "mount_path must not be under {reserved}"
            )));
        }
    }
    Ok(())
}

/// The static validation of a `createVolume` body.
fn validate(body: &CreateVolumeRequest) -> Result<(), ApiError> {
    if !(1..=1024).contains(&body.size_gb) {
        return Err(ApiError::validation("size_gb is outside 1-1024"));
    }
    validate_mount_path(&body.mount_path)
}

/// Volumes of one app, newest first.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed app
/// id, or a database error.
pub(crate) async fn list_volumes(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
) -> Result<VolumeList, ApiError> {
    let params = json!({ "app_id": app_id });
    let target = app_target(app_id);
    let access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<VolumeList, ApiError> = async {
        if !deploy::is_id_with_prefix("app", app_id) {
            return Err(ApiError::not_found("app"));
        }
        let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
            .bind(app_id)
            .fetch_optional(state.store.pool())
            .await?;
        let Some((project_id,)) = project else {
            return Err(ApiError::not_found("app"));
        };
        let project_id = project_id.clone();
        let rows = sqlx::query(&format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE app_id = ? ORDER BY id DESC"
        ))
        .bind(app_id)
        .fetch_all(state.store.pool())
        .await?;
        let volumes = rows
            .iter()
            .map(volume_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let entries: Vec<(Volume, String)> = volumes
            .into_iter()
            .map(|volume| (volume, project_id.clone()))
            .collect();
        let entries = access
            .filter(state, entries, |(volume, project_id)| Resource {
                kind: "volume",
                id: &volume.id,
                app_id: Some(&volume.app_id),
                project_id: Some(project_id),
                node_id: Some(&volume.node_id),
            })
            .await?;
        let items = entries.into_iter().map(|(volume, _)| volume).collect();
        Ok(VolumeList { items })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One volume with its state and node.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
pub(crate) async fn get_volume(
    state: &AppState,
    ctx: &CallCtx,
    volume_id: &str,
) -> Result<Volume, ApiError> {
    let params = json!({ "volume_id": volume_id });
    let target = volume_target(volume_id);
    let mut access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<Volume, ApiError> = async {
        if !deploy::is_id_with_prefix("vol", volume_id) {
            return Err(ApiError::not_found("volume"));
        }
        let row = sqlx::query(&format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
        ))
        .bind(volume_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("volume"))?;
        let volume = volume_from_row(&row)?;
        let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
            .bind(&volume.app_id)
            .fetch_optional(state.store.pool())
            .await?;
        access.set_project(project.map(|row| row.0).as_deref());
        Ok(volume)
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Creates the app's local volume, makes the app stateful and grants the
/// node the app's lease with a new token, all in one write transaction
/// ([`leases::grant_app_lease`]).
///
/// # Errors
///
/// `not_found` for an unknown app, `conflict` when the app already has an
/// attached volume, its node is not connected, the previous holder's gate
/// is still open (section 8.4), or the grant would carry the lease row's
/// unsettled mark ([`leases::NewGrant::Held`]), `validation_failed` for a
/// bad body or a size larger than the node's free disk, or a database error.
pub(crate) async fn create_volume(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    body: &CreateVolumeRequest,
    params: &Value,
) -> Result<CreatedVolume, ApiError> {
    validate(body)?;
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("app"))?;
    let mut app = app_from_row(&row)?;
    let attached: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM volumes WHERE app_id = ? AND state IN ('creating', 'ready', 'restoring')",
    )
    .bind(app_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    if attached.is_some() {
        return Err(ApiError::conflict(
            "the app already has a volume; an app has one in M5a",
        ));
    }
    // A volume attaches to one instance, so only a single-replica app may
    // get one (ADR 0142, ADR 0145).
    if app.replicas > 1 {
        return Err(ApiError::conflict(
            "the app runs more than one replica; a volume needs exactly one",
        ));
    }

    // The volume lives where the app's instance runs; an app with no
    // instance gets the node the scheduler picks (ADR 0145).
    let instance_node: Option<(String,)> = sqlx::query_as(
        "SELECT node_id FROM vms WHERE app_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    let node_id = match instance_node {
        Some((node_id,)) => node_id,
        None => deploy::pick_node(&mut w.tx, &app.placement).await?,
    };
    let node_status: Option<(String,)> = sqlx::query_as("SELECT status FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if node_status.map(|row| row.0).as_deref() != Some("connected") {
        return Err(ApiError::conflict(
            "the node that would hold the volume is not connected",
        ));
    }
    if let Some(free) = state.hub.disk_free_gb(&node_id)
        && body.size_gb as f64 > free
    {
        return Err(ApiError::validation(format!(
            "size_gb {} is larger than the node's free disk",
            body.size_gb
        )));
    }

    let now = now_rfc3339();
    let volume = Volume {
        id: deploy::new_id_with_prefix("vol"),
        app_id: app_id.to_string(),
        node_id: node_id.clone(),
        size_gb: body.size_gb,
        mount_path: body.mount_path.clone(),
        state: VolumeState::Creating,
        allocated_bytes: None,
        restored_from_backup_id: None,
        error: None,
        quarantines: Vec::new(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    sqlx::query(
        "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
         allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, 'creating', NULL, NULL, NULL, ?, ?)",
    )
    .bind(&volume.id)
    .bind(&volume.app_id)
    .bind(&volume.node_id)
    .bind(volume.size_gb)
    .bind(&volume.mount_path)
    .bind(&volume.created_at)
    .bind(&volume.updated_at)
    .execute(&mut *w.tx)
    .await?;
    let actor = ctx.identity.actor();
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "volume",
        &volume.id,
        Op::Create,
        volume.payload()?,
    )
    .await?;

    // The app is stateful exactly when it has a volume, and it may run only
    // on the node that holds it (section 8.3, ADR 0145).
    app.stateful = true;
    app.placement = AppPlacement {
        node_ids: vec![node_id.clone()],
    };
    app.updated_at = now;
    let placement = serde_json::to_string(&app.placement)
        .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
    sqlx::query("UPDATE apps SET stateful = 1, placement = ?, updated_at = ? WHERE id = ?")
        .bind(&placement)
        .bind(&app.updated_at)
        .bind(app_id)
        .execute(&mut *w.tx)
        .await?;
    let payload = app_payload(&mut w.tx, &app).await?;
    changelog::append(&mut w.tx, &actor, "app", app_id, Op::Update, payload).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "app.updated",
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: app_id,
            project_id: Some(&app.project_id),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data: json!({ "app_id": app_id, "changed": ["stateful", "placement"] }),
        },
    )
    .await?;

    // Deleting the app's volume revoked its previous lease, so the node took
    // that revoke before this grant and its mode is free (ADR 0258). A
    // volume that failed leaves the lease unrevoked: the node still holds it,
    // so a new token keeps the mode it holds, `local` (section 8.4).
    let keep_local = leases::app_lease(&mut w.tx, app_id)
        .await?
        .is_some_and(|previous| {
            !previous.is_revoked() && previous.mode == "local" && previous.holder_node_id == node_id
        });
    let lease = match leases::grant_app_lease(
        &mut w.tx,
        state.hub.authority(),
        app_id,
        &node_id,
        &actor,
        keep_local,
    )
    .await?
    {
        leases::NewGrant::Granted(lease) => *lease,
        leases::NewGrant::GateOpen(remaining) => {
            return Err(ApiError::conflict(format!(
                "the app's previous lease holder may still run it for {} s more (its gate is \
                 open); retry then",
                remaining.as_secs() + 1
            )));
        }
        // Section 8.4 ("Grants of a marked row", ADR 0268) says the grant
        // waits. This build cannot count what it waits for, so nothing is
        // created.
        leases::NewGrant::Held => {
            return Err(ApiError::conflict(
                "the app's lease cannot be granted yet: this grant needs majority voter support \
                 and its lease row on every node first, which this build does not track yet",
            ));
        }
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppUpdate),
        Some(&app_target(app_id)),
        Some(&app.project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(CreatedVolume {
        volume,
        node_id,
        lease,
    })
}

/// Accepts a delete: the row turns `deleting` and the node removes the file
/// (section 6.7). An attached volume releases the app's `local` lease.
///
/// # Errors
///
/// `not_found` for an unknown or malformed id, `conflict` while the app is
/// running or the volume is already deleting, or a database error.
pub(crate) async fn delete_volume(
    state: &AppState,
    ctx: &CallCtx,
    volume_id: &str,
    params: &Value,
) -> Result<DeletedVolume, ApiError> {
    if !deploy::is_id_with_prefix("vol", volume_id) {
        return Err(ApiError::not_found("volume"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
    ))
    .bind(volume_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("volume"))?;
    let mut volume = volume_from_row(&row)?;
    let app: Option<(String, String)> =
        sqlx::query_as("SELECT status, project_id FROM apps WHERE id = ?")
            .bind(&volume.app_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    let Some((app_status, project_id)) = app else {
        return Err(ApiError::not_found("volume"));
    };
    if app_status == "running"
        && !matches!(volume.state, VolumeState::Detached | VolumeState::Failed)
    {
        return Err(ApiError::conflict(
            "the app is running: stop it before deleting its volume",
        ));
    }
    if volume.state == VolumeState::Deleting {
        return Err(ApiError::conflict("the volume is already deleting"));
    }

    let attached = volume.state.is_attached();
    volume.state = VolumeState::Deleting;
    volume.updated_at = now_rfc3339();
    sqlx::query("UPDATE volumes SET state = 'deleting', updated_at = ? WHERE id = ?")
        .bind(&volume.updated_at)
        .bind(&volume.id)
        .execute(&mut *w.tx)
        .await?;
    let actor = ctx.identity.actor();
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "volume",
        &volume.id,
        Op::Update,
        volume.payload()?,
    )
    .await?;
    // Section 8.3, "Released at deletion": the attached volume's lease ends
    // with the volume.
    let revoke = if attached {
        leases::revoke_app_lease(&mut w.tx, &volume.app_id, &actor)
            .await?
            .map(|lease| lease.revoke(quoro_proto::RevokeReason::Deleted))
    } else {
        None
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDelete),
        Some(&volume_target(volume_id)),
        Some(&project_id),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(DeletedVolume {
        volume,
        node_id: row_node_id(&row)?,
        revoke,
    })
}

/// The node of a row selected with [`VOLUME_COLUMNS`].
fn row_node_id(row: &sqlx::sqlite::SqliteRow) -> Result<String, ApiError> {
    use sqlx::Row as _;
    Ok(row.try_get("node_id")?)
}

/// Applies `status_report.volumes` (section 5.3) from `node_id`. Returns
/// whether the node's desired state changed, so the caller can resend it.
///
/// `deleted` removes the row and, when it was the app's last attached volume,
/// makes the app stateless and unpins its placement. `orphaned` is ignored:
/// the protocol never deletes a file by absence (section 6.7).
///
/// # Errors
///
/// A database error.
pub(crate) async fn apply_status(
    store: &Store,
    node_id: &str,
    volumes: &[wire::VolumeStatus],
) -> Result<bool, ApiError> {
    if volumes.is_empty() {
        return Ok(false);
    }
    let mut w = store.begin_write().await?;
    let actor = crate::changelog::Actor::system("volumes");
    let mut desired_changed = false;
    for reported in volumes {
        let row = sqlx::query(&format!(
            "SELECT {VOLUME_COLUMNS} FROM volumes WHERE id = ?"
        ))
        .bind(&reported.volume_id)
        .fetch_optional(&mut *w.tx)
        .await?;
        let Some(row) = row else {
            continue;
        };
        if row_node_id(&row)? != node_id {
            continue;
        }
        let before = volume_from_row(&row)?;
        if before.state == VolumeState::Detached {
            continue;
        }
        match reported.state {
            wire::VolumeState::Orphaned => {
                tracing::debug!(
                    volume_id = %before.id,
                    node_id,
                    "the node holds a volume file the desired state does not name"
                );
            }
            wire::VolumeState::Deleted => {
                sqlx::query("DELETE FROM volumes WHERE id = ?")
                    .bind(&before.id)
                    .execute(&mut *w.tx)
                    .await?;
                changelog::append(
                    &mut w.tx,
                    &actor,
                    "volume",
                    &before.id,
                    Op::Delete,
                    before.payload()?,
                )
                .await?;
                clear_stateful(&mut w.tx, &before.app_id, &actor).await?;
                desired_changed = true;
            }
            reported_state => {
                if before.state == VolumeState::Deleting {
                    // A report that raced the delete must not resurrect it.
                    continue;
                }
                if reported_state == wire::VolumeState::Ready
                    && before.state == VolumeState::Restoring
                    && before.restored_from_backup_id.is_some()
                {
                    // A restore's volume becomes `ready` only through
                    // `backups::restore`, together with its lease; one still
                    // `restoring` waits for that lease (issue #392).
                    continue;
                }
                let after_state = match reported_state {
                    wire::VolumeState::Creating => VolumeState::Creating,
                    wire::VolumeState::Ready => VolumeState::Ready,
                    wire::VolumeState::Restoring => VolumeState::Restoring,
                    wire::VolumeState::Failed => VolumeState::Failed,
                    wire::VolumeState::Deleted | wire::VolumeState::Orphaned => continue,
                };
                let allocated_bytes = reported
                    .allocated_bytes
                    .map(|bytes| i64::try_from(bytes).unwrap_or(i64::MAX));
                let error = reported
                    .error
                    .as_ref()
                    .map(|error| error.chars().take(2000).collect::<String>());
                let mut after = before.clone();
                after.state = after_state;
                after.allocated_bytes = allocated_bytes.or(before.allocated_bytes);
                if after_state == VolumeState::Failed {
                    after.error = error;
                }
                if after == before {
                    continue;
                }
                after.updated_at = now_rfc3339();
                sqlx::query(
                    "UPDATE volumes SET state = ?, allocated_bytes = ?, error = ?, updated_at = ? \
                     WHERE id = ?",
                )
                .bind(after.state.as_str())
                .bind(after.allocated_bytes)
                .bind(&after.error)
                .bind(&after.updated_at)
                .bind(&after.id)
                .execute(&mut *w.tx)
                .await?;
                changelog::append(
                    &mut w.tx,
                    &actor,
                    "volume",
                    &after.id,
                    Op::Update,
                    after.payload()?,
                )
                .await?;
                if after.state == VolumeState::Failed {
                    // A failed volume is not attached: an app left with only
                    // detached or failed volumes is stateless (migration 0006).
                    clear_stateful(&mut w.tx, &before.app_id, &actor).await?;
                }
            }
        }
    }
    w.commit().await?;
    Ok(desired_changed)
}

/// Makes `app_id` stateless and unpins its placement when no attached volume
/// is left (`openapi.yaml` `deleteVolume`). Writes the app row and its
/// change-log entry only when something changed.
pub(crate) async fn clear_stateful(
    conn: &mut SqliteConnection,
    app_id: &str,
    actor: &crate::changelog::Actor,
) -> Result<bool, ApiError> {
    let attached: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM volumes WHERE app_id = ? AND state IN ('creating', 'ready', 'restoring')",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if attached.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let mut app = app_from_row(&row)?;
    if !app.stateful && app.placement.node_ids.is_empty() {
        return Ok(false);
    }
    app.stateful = false;
    app.placement = AppPlacement::default();
    // The contract allows `cold` only for a stateful app (`openapi.yaml`
    // `App.ha_policy`), so the last volume's deletion makes it `none` again.
    if app.ha_policy == crate::api::apps::HaPolicy::Cold {
        app.ha_policy = crate::api::apps::HaPolicy::None;
    }
    app.updated_at = now_rfc3339();
    let placement = serde_json::to_string(&app.placement)
        .map_err(|error| ApiError::internal(&format!("placement: {error}")))?;
    sqlx::query(
        "UPDATE apps SET stateful = 0, placement = ?, ha_policy = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&placement)
    .bind(app.ha_policy.as_str())
    .bind(&app.updated_at)
    .bind(app_id)
    .execute(&mut *conn)
    .await?;
    let payload = app_payload(conn, &app).await?;
    changelog::append(conn, actor, "app", app_id, Op::Update, payload).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mount_path_follows_the_schema() {
        for path in ["/data", "/var/lib/app", "/a_b/c-d.e", "/srv/1"] {
            assert!(validate_mount_path(path).is_ok(), "{path} is accepted");
        }
        for path in [
            "",
            "/",
            "data",
            "/data/",
            "//data",
            "/data//x",
            "/./data",
            "/data/..",
            "/proc",
            "/proc/cpuinfo",
            "/sys",
            "/sys/kernel",
            "/dev",
            "/dev/shm",
            "/data x",
            "/d\u{e9}ta",
            "relative/path",
        ] {
            assert!(validate_mount_path(path).is_err(), "{path:?} is refused");
        }
        let long = format!("/{}", "a".repeat(255));
        assert!(validate_mount_path(&long).is_err(), "over 255 characters");
    }
}
