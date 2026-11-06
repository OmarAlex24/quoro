// SPDX-License-Identifier: AGPL-3.0-only
//! Deployments: an app's rollout as desired state, and its progress from what
//! the node reports.
//!
//! # Model
//!
//! - A deployment of an app with one replica creates **one new VM row** for
//!   the app (`vms.app_id` set), placed on a node, because a VM's image digest
//!   is create-time only on the node
//!   (`crates/node-agent/src/reconcile.rs`). An app with more replicas starts
//!   one new instance per node, one node at a time, gated on the previous new
//!   instance's health (`crate::scheduler`, ADR 0142).
//! - **VM to deployment link.** `vms` has no `deployment_id` column. The link
//!   is `vms.app_id = deployments.app_id AND vms.created_at =
//!   deployments.created_at`: the deployment and its VM are written in the
//!   same write transaction with the same `created_at`, and that value is made
//!   strictly greater than every earlier deployment of the app (bumped by one
//!   millisecond on a tie), so the link is unique by construction.
//!   [`deployments_for_node`] is the only reader of the rule.
//! - When every new instance is observed `running` with health `passing`, the
//!   deployment `succeeded`, becomes the app's `current_deployment_id`, and
//!   the app's older VMs are deleted. A failure deletes every VM of the
//!   deployment and leaves the previous deployment's VMs running.
//! - **Stateful apps.** A stateful app's volume is attached to one VM at a
//!   time (ADR 0145), so a deployment of a stateful app deletes the app's
//!   previous VM rows in the same transaction that writes the new one: the
//!   node stops and deletes the old VM before it starts the new one. A
//!   stateful redeploy therefore has a short downtime, and a redeploy that
//!   fails leaves no instance running — the previous VM is already gone. A
//!   stateful app has exactly one instance: `replicas` above 1 is refused
//!   while it has a volume (ADR 0142, ADR 0145).
//!
//! Every function that changes desired state returns the `vm_` ids whose
//! desired state changed; the caller (the session or a handler) passes each
//! to `NodeHub::vm_changed` so the node gets a new snapshot.

use crate::api::apps::{AppPlacement, AppResources, AppStatus};
use crate::api::deployments::{
    CreatedByType, DEPLOYMENT_COLUMNS, Deployment, DeploymentPhase, DeploymentStatus,
    DeploymentTrigger, deployment_from_row,
};
use crate::changelog::{self, Actor, ActorType, Op};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::store::{Store, now_rfc3339};
use quoro_proto::wire;
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};
use std::collections::{HashMap, HashSet};

/// Crockford base32, the alphabet of a ULID (`README` "Identifiers").
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The subsystem recorded as the actor of observation-driven changes.
fn deploy_actor() -> Actor {
    Actor::system("deploy")
}

/// Mints an `app_`, `dep_` or `dom_` id.
///
/// `quoro_types::Prefix` does not list the app id prefixes yet, so a freshly
/// minted ULID gets its prefix swapped. The construction stays the one every
/// other id uses (`README` "Identifiers"): 48 bits of milliseconds followed by
/// 80 random bits, so ids sort by creation time.
#[must_use]
pub(crate) fn new_id_with_prefix(prefix: &str) -> String {
    let minted = quoro_types::new_id(quoro_types::Prefix::Request);
    let ulid = minted.strip_prefix("req_").unwrap_or(&minted);
    format!("{prefix}_{ulid}")
}

/// Whether `id` is `<prefix>_` followed by 26 Crockford base32 characters, the
/// pattern every id parameter in `openapi.yaml` uses.
#[must_use]
pub(crate) fn is_id_with_prefix(prefix: &str, id: &str) -> bool {
    let Some(rest) = id
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('_'))
    else {
        return false;
    };
    rest.len() == 26 && rest.bytes().all(|b| CROCKFORD.contains(&b))
}

/// `t` in the fixed-width form of [`now_rfc3339`], so stored timestamps compare
/// correctly as strings. The formatter is local because `store`'s is private.
fn format_rfc3339(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

/// `last` plus one millisecond, in the fixed-width stored form; `None` when
/// `last` is not a stored timestamp.
fn next_millisecond(last: &str) -> Option<String> {
    let parsed =
        time::OffsetDateTime::parse(last, &time::format_description::well_known::Rfc3339).ok()?;
    Some(format_rfc3339(parsed + time::Duration::milliseconds(1)))
}

/// The deployment and the VM row [`create_deployment`] wrote.
pub(crate) struct CreatedDeployment {
    /// The `pending` deployment.
    pub deployment: Deployment,
    /// Project of the app, for the audit record.
    pub project_id: String,
    /// Change-log `seq` of the deployment's create entry.
    pub change_seq: i64,
    /// The first new VM row of the deployment (its only one when the app has
    /// one replica).
    pub vm_id: String,
    /// Node the VM is placed on.
    pub node_id: String,
    /// Ids of the superseded deployments' VMs, deleted in the same transaction.
    pub retired_vm_ids: Vec<String>,
}

/// Where a deployment is placed and the digest it already knows.
///
/// A deployment started by a build knows both: the image never leaves the
/// node that built it, so there is no registry resolution (ADR 0118).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DeploymentTarget<'a> {
    /// Node to place the deployment on; `None` lets the scheduler pick.
    pub node_id: Option<&'a str>,
    /// Digest the reference is known to have, when it is already resolved.
    pub image_digest: Option<&'a str>,
}

/// The app fields a deployment needs, read inside the write transaction.
struct AppSnapshot {
    project_id: String,
    name: String,
    status: AppStatus,
    stateful: bool,
    restart_generation: i64,
    resources: AppResources,
    placement: AppPlacement,
    replicas: i64,
}

/// Loads the fields of `app_id` a deployment needs.
async fn load_app(conn: &mut SqliteConnection, app_id: &str) -> Result<AppSnapshot, ApiError> {
    let row = sqlx::query(
        "SELECT project_id, name, status, stateful, restart_generation, resources, placement, \
         replicas \
         FROM apps WHERE id = ?",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| ApiError::not_found("app"))?;
    let status: String = row.try_get("status")?;
    let status = AppStatus::parse(&status)
        .ok_or_else(|| ApiError::internal(&format!("unknown app status {status:?}")))?;
    let resources: String = row.try_get("resources")?;
    let resources = serde_json::from_str(&resources)
        .map_err(|error| ApiError::internal(&format!("stored resources: {error}")))?;
    let placement: String = row.try_get("placement")?;
    let placement = serde_json::from_str(&placement)
        .map_err(|error| ApiError::internal(&format!("stored placement: {error}")))?;
    Ok(AppSnapshot {
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        status,
        stateful: row.try_get("stateful")?,
        restart_generation: row.try_get("restart_generation")?,
        resources,
        placement,
        replicas: row.try_get("replicas")?,
    })
}

/// The fields every VM row of an app instance carries.
struct NewVm<'a> {
    project_id: &'a str,
    app_id: &'a str,
    node_id: &'a str,
    name: &'a str,
    vcpus: i64,
    memory_mb: i64,
    disk_gb: i64,
    image: &'a str,
    restart_generation: i64,
    /// `created_at` of the deployment the VM is linked to.
    created_at: &'a str,
}

/// Inserts one app instance row and its change-log entry.
async fn insert_app_vm(
    conn: &mut SqliteConnection,
    vm: &NewVm<'_>,
    actor: &Actor,
    now: &str,
) -> Result<String, ApiError> {
    let vm_id = new_id_with_prefix("vm");
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 0, NULL, ?, ?)",
    )
    .bind(&vm_id)
    .bind(vm.project_id)
    .bind(vm.app_id)
    .bind(vm.node_id)
    .bind(vm.name)
    .bind(vm.vcpus)
    .bind(vm.memory_mb)
    .bind(vm.disk_gb)
    .bind(vm.image)
    .bind("running")
    .bind("creating")
    .bind(vm.restart_generation)
    .bind(vm.created_at)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    changelog::append(
        conn,
        actor,
        "vm",
        &vm_id,
        Op::Create,
        json!({
            "id": vm_id,
            "project_id": vm.project_id,
            "app_id": vm.app_id,
            "node_id": vm.node_id,
            "name": vm.name,
            "vcpus": vm.vcpus,
            "memory_mb": vm.memory_mb,
            "disk_gb": vm.disk_gb,
            "image": vm.image,
            "desired_status": "running",
            "observed_status": "creating",
            "restart_generation": vm.restart_generation,
            "created_at": vm.created_at,
            "updated_at": now,
        }),
    )
    .await?;
    Ok(vm_id)
}

/// One VM row linked to a deployment by the link rule in the module
/// documentation.
struct LinkedVm {
    id: String,
    node_id: String,
    desired_status: String,
    observed_status: String,
}

/// The VM rows of `app_id` linked to the deployment written at `created_at`.
async fn linked_vms(
    conn: &mut SqliteConnection,
    app_id: &str,
    created_at: &str,
) -> Result<Vec<LinkedVm>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, node_id, desired_status, observed_status FROM vms \
         WHERE app_id = ? AND created_at = ? ORDER BY id",
    )
    .bind(app_id)
    .bind(created_at)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(LinkedVm {
                id: row.try_get("id")?,
                node_id: row.try_get("node_id")?,
                desired_status: row.try_get("desired_status")?,
                observed_status: row.try_get("observed_status")?,
            })
        })
        .collect()
}

/// Deletes every VM row linked to the deployment and change-logs each one,
/// returning the deleted ids. The previous deployment's VMs are untouched.
async fn delete_linked_vms(
    conn: &mut SqliteConnection,
    app_id: &str,
    created_at: &str,
) -> Result<Vec<String>, ApiError> {
    let mut deleted = Vec::new();
    for vm in linked_vms(conn, app_id, created_at).await? {
        sqlx::query("DELETE FROM vms WHERE id = ?")
            .bind(&vm.id)
            .execute(&mut *conn)
            .await?;
        changelog::append(conn, &deploy_actor(), "vm", &vm.id, Op::Delete, json!({})).await?;
        deleted.push(vm.id);
    }
    Ok(deleted)
}

/// The node a deployment is placed on: the first of `placement.node_ids` that
/// is not removed, else the first `connected` node by id.
pub(crate) async fn pick_node(
    conn: &mut SqliteConnection,
    placement: &AppPlacement,
) -> Result<String, ApiError> {
    for node_id in &placement.node_ids {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT id FROM nodes WHERE id = ? AND status != 'removed'")
                .bind(node_id)
                .fetch_optional(&mut *conn)
                .await?;
        if row.is_some() {
            return Ok(node_id.clone());
        }
    }
    let row: Option<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE status = 'connected' ORDER BY id LIMIT 1")
            .fetch_optional(&mut *conn)
            .await?;
    row.map(|(id,)| id)
        .ok_or_else(|| ApiError::validation("no node available"))
}

/// The creator kind of a deployment, from the change-log actor.
pub(crate) const fn creator_type(actor: &Actor) -> CreatedByType {
    match actor.kind {
        ActorType::User => CreatedByType::User,
        ActorType::Agent => CreatedByType::Agent,
        ActorType::Node | ActorType::System => CreatedByType::System,
    }
}

/// The change-log payload of one deployment.
fn deployment_payload(deployment: &Deployment) -> Result<Value, ApiError> {
    serde_json::to_value(deployment)
        .map_err(|error| ApiError::internal(&format!("deployment payload: {error}")))
}

/// Appends the change-log entry of a deployment change.
async fn log_deployment(
    conn: &mut SqliteConnection,
    deployment: &Deployment,
    op: Op,
) -> Result<i64, ApiError> {
    Ok(changelog::append(
        conn,
        &deploy_actor(),
        "deployment",
        &deployment.id,
        op,
        deployment_payload(deployment)?,
    )
    .await?)
}

/// Writes a deployment row's mutable fields and its change-log entry, only when
/// a value actually changed.
async fn save_deployment(
    conn: &mut SqliteConnection,
    before: &Deployment,
    after: &Deployment,
) -> Result<bool, ApiError> {
    if before == after {
        return Ok(false);
    }
    sqlx::query(
        "UPDATE deployments SET status = ?, phase = ?, error = ?, image_digest = ?, finished_at = ? \
         WHERE id = ?",
    )
    .bind(after.status.as_str())
    .bind(after.phase.map(DeploymentPhase::as_str))
    .bind(&after.error)
    .bind(&after.image_digest)
    .bind(&after.finished_at)
    .bind(&after.id)
    .execute(&mut *conn)
    .await?;
    log_deployment(conn, after, Op::Update).await?;
    Ok(true)
}

/// Starts a deployment: supersedes the app's in-progress deployments and their
/// VMs, inserts the new deployment and its VM row, and moves the app to
/// `deploying`, all in the caller's write transaction.
///
/// # Errors
///
/// `not_found` for an unknown app, `validation_failed` when no node is
/// available, or a database error.
pub(crate) async fn create_deployment(
    conn: &mut SqliteConnection,
    app_id: &str,
    image: &str,
    trigger: DeploymentTrigger,
    actor: &Actor,
) -> Result<CreatedDeployment, ApiError> {
    create_deployment_with(
        conn,
        app_id,
        image,
        trigger,
        actor,
        &DeploymentTarget::default(),
        true,
    )
    .await
}

/// Starts a deployment with a target and a digest the caller already knows,
/// and optionally without its two events so the caller emits them in its own
/// order (a build emits `build.succeeded` before `deployment.started`).
///
/// # Errors
///
/// The same as [`create_deployment`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_deployment_with(
    conn: &mut SqliteConnection,
    app_id: &str,
    image: &str,
    trigger: DeploymentTrigger,
    actor: &Actor,
    target: &DeploymentTarget<'_>,
    emit_events: bool,
) -> Result<CreatedDeployment, ApiError> {
    let app = load_app(conn, app_id).await?;
    // An app with one instance keeps the historical placement. An app with
    // more starts one instance on the first node of the scheduler's order and
    // adds the rest of the nodes as each new instance passes its health check
    // (ADR 0142).
    let node_id = if app.replicas > 1 {
        let nodes =
            crate::scheduler::place_instances(conn, app_id, &app.placement, app.replicas).await?;
        nodes
            .first()
            .cloned()
            .ok_or_else(|| ApiError::validation("no node available"))?
    } else {
        match target.node_id {
            Some(node_id) => node_id.to_string(),
            None => pick_node(conn, &app.placement).await?,
        }
    };
    let now = now_rfc3339();

    // The link rule needs a strictly increasing `created_at` per app.
    let last: Option<(String,)> =
        sqlx::query_as("SELECT MAX(created_at) FROM deployments WHERE app_id = ?")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let created_at = match last.map(|(last,)| last).filter(|last| *last >= now) {
        Some(last) => next_millisecond(&last)
            .ok_or_else(|| ApiError::internal(&format!("stored deployment time {last:?}")))?,
        None => now.clone(),
    };

    // Supersede every in-progress deployment and delete its VM row, so the
    // node stops converging to the old rollout.
    let mut retired_vm_ids = Vec::new();
    for (deployment_id,) in sqlx::query_as::<_, (String,)>(
        "SELECT id FROM deployments WHERE app_id = ? AND status IN ('pending', 'preparing', 'starting')",
    )
    .bind(app_id)
    .fetch_all(&mut *conn)
    .await?
    {
        // Read the row back to keep the change-log payload faithful.
        let row = sqlx::query(&format!(
            "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE id = ?"
        ))
        .bind(&deployment_id)
        .fetch_one(&mut *conn)
        .await?;
        let before = deployment_from_row(&row)?;
        let mut superseded = before.clone();
        superseded.status = DeploymentStatus::Superseded;
        superseded.finished_at = Some(now.clone());
        save_deployment(conn, &before, &superseded).await?;

        let vms: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM vms WHERE app_id = ? AND created_at = ?")
                .bind(app_id)
                .bind(&before.created_at)
                .fetch_all(&mut *conn)
                .await?;
        for (vm_id,) in vms {
            sqlx::query("DELETE FROM vms WHERE id = ?")
                .bind(&vm_id)
                .execute(&mut *conn)
                .await?;
            changelog::append(conn, &deploy_actor(), "vm", &vm_id, Op::Delete, json!({}))
                .await?;
            retired_vm_ids.push(vm_id);
        }
    }

    let deployment = Deployment {
        id: new_id_with_prefix("dep"),
        app_id: app_id.to_string(),
        image: image.to_string(),
        image_digest: target.image_digest.map(str::to_string),
        status: DeploymentStatus::Pending,
        phase: None,
        error: None,
        trigger,
        created_by_type: creator_type(actor),
        created_by_id: actor.id.clone(),
        created_at: created_at.clone(),
        finished_at: None,
    };
    sqlx::query(
        "INSERT INTO deployments (id, app_id, image, image_digest, status, phase, error, \
         \"trigger\", created_by_type, created_by_id, created_at, finished_at) \
         VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL)",
    )
    .bind(&deployment.id)
    .bind(&deployment.app_id)
    .bind(&deployment.image)
    .bind(&deployment.image_digest)
    .bind(deployment.status.as_str())
    .bind(deployment.trigger.as_str())
    .bind(deployment.created_by_type.as_str())
    .bind(&deployment.created_by_id)
    .bind(&deployment.created_at)
    .execute(&mut *conn)
    .await?;
    let change_seq = log_deployment(conn, &deployment, Op::Create).await?;

    let vm_id = insert_app_vm(
        conn,
        &NewVm {
            project_id: &app.project_id,
            app_id,
            node_id: &node_id,
            name: &app.name,
            vcpus: app.resources.vcpus,
            memory_mb: app.resources.memory_mb,
            disk_gb: app.resources.disk_gb,
            image,
            restart_generation: app.restart_generation,
            created_at: &created_at,
        },
        actor,
        &now,
    )
    .await?;

    // A stateful app's volume attaches to one VM at a time (ADR 0145), so
    // every older VM of the app leaves the desired state in this same
    // transaction: the node stops and deletes it before it starts the new
    // one. A stateful redeploy therefore has a short downtime, and a failed
    // one leaves no instance running.
    if app.stateful {
        let older: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM vms WHERE app_id = ? AND id != ?")
                .bind(app_id)
                .bind(&vm_id)
                .fetch_all(&mut *conn)
                .await?;
        for (older_id,) in older {
            sqlx::query("DELETE FROM vms WHERE id = ?")
                .bind(&older_id)
                .execute(&mut *conn)
                .await?;
            changelog::append(
                conn,
                &deploy_actor(),
                "vm",
                &older_id,
                Op::Delete,
                json!({}),
            )
            .await?;
            retired_vm_ids.push(older_id);
        }
    }

    let created = CreatedDeployment {
        deployment,
        project_id: app.project_id,
        change_seq,
        vm_id,
        node_id,
        retired_vm_ids,
    };
    if emit_events {
        emit_created_events(conn, &created, actor).await?;
    }

    if app.status != AppStatus::Deploying {
        set_app_status(conn, app_id, AppStatus::Deploying, &now).await?;
    }

    Ok(created)
}

/// Emits the `deployment.started` and `vm.created` events of a new deployment.
///
/// A caller that wants `build.succeeded` before `deployment.started` creates
/// the deployment without its events and emits them here itself (ADR 0117).
///
/// # Errors
///
/// A database error.
pub(crate) async fn emit_created_events(
    conn: &mut SqliteConnection,
    created: &CreatedDeployment,
    actor: &Actor,
) -> Result<(), ApiError> {
    events::emit(
        conn,
        NewEvent {
            kind: "deployment.started",
            source: Source::ControlPlane,
            subject_type: "deployment",
            subject_id: &created.deployment.id,
            project_id: Some(&created.project_id),
            request_id: None,
            actor: Some(actor),
            data: json!({
                "deployment_id": created.deployment.id,
                "app_id": created.deployment.app_id,
                "image": created.deployment.image,
                "trigger": created.deployment.trigger.as_str(),
            }),
        },
    )
    .await?;
    events::emit(
        conn,
        NewEvent {
            kind: "vm.created",
            source: Source::ControlPlane,
            subject_type: "vm",
            subject_id: &created.vm_id,
            project_id: Some(&created.project_id),
            request_id: None,
            actor: Some(actor),
            data: json!({
                "vm_id": created.vm_id,
                "node_id": created.node_id,
                "app_id": created.deployment.app_id,
            }),
        },
    )
    .await?;
    Ok(())
}

/// Writes `apps.status` and its change-log entry when it changes.
async fn set_app_status(
    conn: &mut SqliteConnection,
    app_id: &str,
    status: AppStatus,
    now: &str,
) -> Result<(), ApiError> {
    sqlx::query("UPDATE apps SET status = ?, updated_at = ? WHERE id = ?")
        .bind(status.as_str())
        .bind(now)
        .bind(app_id)
        .execute(&mut *conn)
        .await?;
    changelog::append(
        conn,
        &deploy_actor(),
        "app",
        app_id,
        Op::Update,
        json!({ "id": app_id, "status": status.as_str(), "updated_at": now }),
    )
    .await?;
    Ok(())
}

/// Recomputes `apps.status` from the app's deployments and its current
/// deployment's VM, writing and change-logging only when the value changes.
async fn recompute_app_status(
    conn: &mut SqliteConnection,
    app_id: &str,
    observation: Option<(&str, wire::ObservedStatus, wire::Health)>,
) -> Result<(), ApiError> {
    let row = sqlx::query("SELECT status, current_deployment_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let current: String = row.try_get("status")?;
    let current = AppStatus::parse(&current)
        .ok_or_else(|| ApiError::internal(&format!("unknown app status {current:?}")))?;
    let current_deployment_id: Option<String> = row.try_get("current_deployment_id")?;

    let in_progress: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM deployments WHERE app_id = ? AND status IN ('pending', 'preparing', 'starting') LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;

    let mut target = current;
    if in_progress.is_some() {
        target = AppStatus::Deploying;
    } else if let Some(deployment_id) = current_deployment_id {
        let created: Option<(String,)> =
            sqlx::query_as("SELECT created_at FROM deployments WHERE id = ?")
                .bind(&deployment_id)
                .fetch_optional(&mut *conn)
                .await?;
        if let Some((created_at,)) = created {
            let vms = linked_vms(conn, app_id, &created_at).await?;
            if vms.len() == 1 {
                // One instance keeps the historical rules exactly.
                let vm = &vms[0];
                let (observed, health) = match observation {
                    Some((seen_id, observed, health)) if seen_id == vm.id => {
                        (format!("{observed:?}").to_lowercase(), Some(health))
                    }
                    _ => (vm.observed_status.clone(), None),
                };
                if vm.desired_status == "stopped" {
                    target = AppStatus::Stopped;
                } else if observed == "running" && health == Some(wire::Health::Passing) {
                    target = AppStatus::Running;
                } else if observed == "crashed"
                    || health == Some(wire::Health::Failing)
                    || observed == "deleted"
                {
                    target = AppStatus::Down;
                }
            } else if !vms.is_empty() {
                // Several instances: stopped only when every one is desired
                // stopped, running only when every one runs (the observed one
                // also with health passing), down when none runs, degraded in
                // between (ADR 0142).
                if vms.iter().all(|vm| vm.desired_status == "stopped") {
                    target = AppStatus::Stopped;
                } else {
                    let mut running = 0;
                    let mut all_running = true;
                    for vm in &vms {
                        let (observed, health) = match observation {
                            Some((seen_id, observed, health)) if seen_id == vm.id => {
                                (format!("{observed:?}").to_lowercase(), Some(health))
                            }
                            _ => (vm.observed_status.clone(), None),
                        };
                        if observed == "running" {
                            running += 1;
                            // Only the observed instance carries health in
                            // this pass; as with one instance it has to pass.
                            if health.is_some_and(|health| health != wire::Health::Passing) {
                                all_running = false;
                            }
                        } else {
                            all_running = false;
                        }
                    }
                    target = if all_running {
                        AppStatus::Running
                    } else if running == 0 {
                        AppStatus::Down
                    } else {
                        AppStatus::Degraded
                    };
                }
            }
        }
    }

    if target == current {
        return Ok(());
    }
    set_app_status(conn, app_id, target, &now_rfc3339()).await
}

/// The app a VM row belongs to, with the fields a transition needs.
struct AppVm {
    app_id: String,
    created_at: String,
}

/// Reads the app id and `created_at` of VM `vm_id` placed on `node_id`.
async fn app_vm_on_node(
    conn: &mut SqliteConnection,
    node_id: &str,
    vm_id: &str,
) -> Result<Option<AppVm>, ApiError> {
    let row = sqlx::query("SELECT app_id, node_id, created_at FROM vms WHERE id = ?")
        .bind(vm_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let placed: String = row.try_get("node_id")?;
    if placed != node_id {
        return Ok(None);
    }
    let app_id: Option<String> = row.try_get("app_id")?;
    let created_at: String = row.try_get("created_at")?;
    Ok(app_id.map(|app_id| AppVm { app_id, created_at }))
}

/// The in-progress deployment linked to `app_id` at `created_at`.
async fn linked_deployment(
    conn: &mut SqliteConnection,
    app_id: &str,
    created_at: &str,
) -> Result<Option<Deployment>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE app_id = ? AND created_at = ?"
    ))
    .bind(app_id)
    .bind(created_at)
    .fetch_optional(&mut *conn)
    .await?;
    row.as_ref().map(deployment_from_row).transpose()
}

/// The deployment that created an app VM.
#[derive(Clone, Debug)]
pub struct VmDeployment {
    /// Its `dep_` id.
    pub id: String,
    /// The digest it recorded: the image's, once a snapshot first named its
    /// instance ([`on_spec_sent`]), or the build's. A VM keeps it for life.
    pub image_digest: Option<String>,
}

/// `vm_` id to the deployment that created it, for every app VM placed on
/// `node_id`, following the link rule in the module documentation.
/// Standalone VMs are absent.
///
/// # Errors
///
/// A database error.
pub async fn deployments_for_node(
    store: &Store,
    node_id: &str,
) -> Result<HashMap<String, VmDeployment>, sqlx::Error> {
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT v.id, d.id, d.image_digest FROM vms v \
         JOIN deployments d ON d.app_id = v.app_id AND d.created_at = v.created_at \
         WHERE v.node_id = ? AND v.app_id IS NOT NULL",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|(vm_id, id, image_digest)| (vm_id, VmDeployment { id, image_digest }))
        .collect())
}

/// The session sent a snapshot to `node_id` that holds these app VMs, each
/// with the digest it was given (`(vm_id, image_digest)`). A `pending`
/// deployment of such a VM becomes `preparing` and records the digest.
///
/// # Errors
///
/// A database error.
pub async fn on_spec_sent(
    store: &Store,
    node_id: &str,
    vms: &[(String, String)],
) -> Result<(), ApiError> {
    if vms.is_empty() {
        return Ok(());
    }
    let mut w = store.begin_write().await?;
    for (vm_id, image_digest) in vms {
        let Some(app_vm) = app_vm_on_node(&mut w.tx, node_id, vm_id).await? else {
            continue;
        };
        let Some(before) = linked_deployment(&mut w.tx, &app_vm.app_id, &app_vm.created_at).await?
        else {
            continue;
        };
        if before.status != DeploymentStatus::Pending {
            continue;
        }
        let mut after = before.clone();
        after.status = DeploymentStatus::Preparing;
        after.phase = Some(DeploymentPhase::Pull);
        after.image_digest = Some(image_digest.clone());
        save_deployment(&mut w.tx, &before, &after).await?;
    }
    w.commit().await?;
    Ok(())
}

/// One VM entry of a `status_report` from `node_id`, for a VM that row
/// `vms` places on that node. Moves the VM's deployment through `preparing`
/// (`creating`), `starting` (`starting`) and `succeeded` (`running` with
/// health `passing`), or `failed` (`crashed` while still in progress), and
/// keeps `apps.status` in step. Returns the VMs whose desired state changed:
/// the instances a success retires, the next instance a passing rollout
/// creates, and the instances a failure deletes.
///
/// # Errors
///
/// A database error.
pub async fn on_vm_observed(
    store: &Store,
    node_id: &str,
    vm: &wire::VmStatus,
) -> Result<Vec<String>, ApiError> {
    let mut w = store.begin_write().await?;
    let Some(app_vm) = app_vm_on_node(&mut w.tx, node_id, &vm.vm_id).await? else {
        w.commit().await?;
        return Ok(Vec::new());
    };
    let Some(before) = linked_deployment(&mut w.tx, &app_vm.app_id, &app_vm.created_at).await?
    else {
        // Not a deployment VM (or its deployment is gone): still keep the app
        // status in step with what the node reports.
        recompute_app_status(
            &mut w.tx,
            &app_vm.app_id,
            Some((&vm.vm_id, vm.observed_status, vm.health)),
        )
        .await?;
        w.commit().await?;
        return Ok(Vec::new());
    };

    let mut changed = Vec::new();
    let mut after = before.clone();
    let now = now_rfc3339();
    let mut emit: Option<(&'static str, Value, Source)> = None;
    let in_progress = before.status.in_progress();
    if in_progress {
        match vm.observed_status {
            wire::ObservedStatus::Creating => {
                if before.status == DeploymentStatus::Pending {
                    after.status = DeploymentStatus::Preparing;
                    after.phase = Some(DeploymentPhase::Pull);
                }
            }
            wire::ObservedStatus::Starting => {
                after.status = DeploymentStatus::Starting;
                after.phase = Some(DeploymentPhase::Boot);
            }
            wire::ObservedStatus::Running if vm.health == wire::Health::Passing => {
                // The new instance of this node passed. With more replicas
                // wanted than this deployment has instances, the next node
                // starts now, next to the old instances; otherwise every
                // instance has to be running before the deployment succeeds
                // (ADR 0142).
                let replicas = app_replicas(&mut w.tx, &app_vm.app_id).await?;
                let linked = linked_vms(&mut w.tx, &app_vm.app_id, &before.created_at).await?;
                if (linked.len() as i64) < replicas {
                    match next_instance_node(&mut w.tx, &app_vm.app_id, &linked).await? {
                        Some(next) => {
                            let seed =
                                first_instance(&mut w.tx, &app_vm.app_id, &before.created_at)
                                    .await?;
                            let new_id = insert_app_vm(
                                &mut w.tx,
                                &NewVm {
                                    project_id: &seed.project_id,
                                    app_id: &app_vm.app_id,
                                    node_id: &next,
                                    name: &seed.name,
                                    vcpus: seed.vcpus,
                                    memory_mb: seed.memory_mb,
                                    disk_gb: seed.disk_gb,
                                    image: &seed.image,
                                    restart_generation: seed.restart_generation,
                                    created_at: &before.created_at,
                                },
                                &deploy_actor(),
                                &now,
                            )
                            .await?;
                            changed.push(new_id);
                            after.status = DeploymentStatus::Starting;
                            after.phase = Some(DeploymentPhase::Boot);
                        }
                        None => {
                            let error = "no eligible node for the next instance".to_string();
                            after.status = DeploymentStatus::Failed;
                            after.phase = Some(DeploymentPhase::Boot);
                            after.error = Some(error.clone());
                            after.finished_at = Some(now.clone());
                            emit = Some((
                                "deployment.failed",
                                json!({
                                    "deployment_id": after.id,
                                    "app_id": after.app_id,
                                    "phase": "boot",
                                    "error": error,
                                }),
                                Source::ControlPlane,
                            ));
                        }
                    }
                } else if all_linked_running(&linked, &vm.vm_id) {
                    after.status = DeploymentStatus::Succeeded;
                    after.phase = None;
                    after.finished_at = Some(now.clone());
                    let mut data = json!({
                        "deployment_id": after.id,
                        "app_id": after.app_id,
                    });
                    if let Some(digest) = &after.image_digest {
                        data["image_digest"] = json!(digest);
                    }
                    emit = Some((
                        "deployment.succeeded",
                        data,
                        Source::Node(node_id.to_string()),
                    ));
                } else {
                    // Another instance is not running yet: stay in progress.
                    after.status = DeploymentStatus::Starting;
                    after.phase = Some(DeploymentPhase::Boot);
                }
            }
            wire::ObservedStatus::Crashed => {
                after.status = DeploymentStatus::Failed;
                after.phase = Some(DeploymentPhase::Boot);
                after.error = Some(match vm.last_exit_code {
                    Some(code) => format!("the VM exited with code {code}"),
                    None => "the VM crashed".to_string(),
                });
                after.finished_at = Some(now.clone());
                emit = Some((
                    "deployment.failed",
                    json!({
                        "deployment_id": after.id,
                        "app_id": after.app_id,
                        "phase": "boot",
                        "error": after.error,
                    }),
                    Source::Node(node_id.to_string()),
                ));
            }
            _ => {}
        }
    }
    let succeeded = after.status == DeploymentStatus::Succeeded && in_progress;
    let failed = after.status == DeploymentStatus::Failed && in_progress;
    save_deployment(&mut w.tx, &before, &after).await?;
    if let Some((kind, data, source)) = emit {
        events::emit(
            &mut w.tx,
            NewEvent {
                kind,
                source,
                subject_type: "deployment",
                subject_id: &after.id,
                project_id: None,
                request_id: None,
                actor: None,
                data,
            },
        )
        .await?;
    }

    if failed {
        // Every instance this deployment started goes away; the previous
        // deployment's instances keep running (ADR 0142).
        changed.extend(delete_linked_vms(&mut w.tx, &app_vm.app_id, &before.created_at).await?);
    }

    if succeeded {
        // The new instances are the app's current ones; their image wins and
        // every VM not linked to this deployment is retired in the same
        // transaction.
        let row = sqlx::query("SELECT current_deployment_id, image FROM apps WHERE id = ?")
            .bind(&app_vm.app_id)
            .fetch_one(&mut *w.tx)
            .await?;
        let old_current: Option<String> = row.try_get("current_deployment_id")?;
        let old_image: String = row.try_get("image")?;
        if old_current.as_deref() != Some(after.id.as_str()) || old_image != after.image {
            sqlx::query(
                "UPDATE apps SET current_deployment_id = ?, image = ?, updated_at = ? WHERE id = ?",
            )
            .bind(&after.id)
            .bind(&after.image)
            .bind(&now)
            .bind(&app_vm.app_id)
            .execute(&mut *w.tx)
            .await?;
            changelog::append(
                &mut w.tx,
                &deploy_actor(),
                "app",
                &app_vm.app_id,
                Op::Update,
                json!({
                    "id": app_vm.app_id,
                    "current_deployment_id": after.id,
                    "image": after.image,
                    "updated_at": now,
                }),
            )
            .await?;
        }

        let others: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM vms WHERE app_id = ? AND created_at != ?")
                .bind(&app_vm.app_id)
                .bind(&before.created_at)
                .fetch_all(&mut *w.tx)
                .await?;
        for (other_id,) in others {
            sqlx::query("DELETE FROM vms WHERE id = ?")
                .bind(&other_id)
                .execute(&mut *w.tx)
                .await?;
            changelog::append(
                &mut w.tx,
                &deploy_actor(),
                "vm",
                &other_id,
                Op::Delete,
                json!({}),
            )
            .await?;
            changed.push(other_id);
        }

        // A scale-down during the rollout leaves more instances than wanted;
        // the extras go in the same order as in `scale`.
        let replicas = app_replicas(&mut w.tx, &app_vm.app_id).await?;
        let mut linked = linked_vms(&mut w.tx, &app_vm.app_id, &before.created_at).await?;
        if linked.len() as i64 > replicas {
            removal_order(&mut w.tx, &app_vm.app_id, &mut linked).await?;
            let extras = usize::try_from((linked.len() as i64 - replicas).max(0)).unwrap_or(0);
            for extra in linked.into_iter().take(extras) {
                sqlx::query("DELETE FROM vms WHERE id = ?")
                    .bind(&extra.id)
                    .execute(&mut *w.tx)
                    .await?;
                changelog::append(
                    &mut w.tx,
                    &deploy_actor(),
                    "vm",
                    &extra.id,
                    Op::Delete,
                    json!({}),
                )
                .await?;
                changed.push(extra.id);
            }
            report_unserved_tunnel_domains(&mut w.tx, &app_vm.app_id).await?;
        }
    }

    if failed {
        app_status_after_failure(&mut w.tx, &app_vm.app_id).await?;
    }
    recompute_app_status(
        &mut w.tx,
        &app_vm.app_id,
        Some((&vm.vm_id, vm.observed_status, vm.health)),
    )
    .await?;
    w.commit().await?;
    Ok(changed)
}

/// The replica count stored for `app_id`.
async fn app_replicas(conn: &mut SqliteConnection, app_id: &str) -> Result<i64, ApiError> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT replicas FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(replicas,)| replicas).unwrap_or(1))
}

/// The placement stored for `app_id`.
async fn app_placement(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<AppPlacement, ApiError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT placement FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some((placement,)) = row else {
        return Ok(AppPlacement::default());
    };
    serde_json::from_str(&placement)
        .map_err(|error| ApiError::internal(&format!("stored placement: {error}")))
}

/// A template for the next instance of a rollout, copied from the first VM
/// this deployment created.
struct InstanceSeed {
    project_id: String,
    name: String,
    vcpus: i64,
    memory_mb: i64,
    disk_gb: i64,
    image: String,
    restart_generation: i64,
}

/// Reads the fields the next instance of a deployment copies from its first
/// instance.
async fn first_instance(
    conn: &mut SqliteConnection,
    app_id: &str,
    created_at: &str,
) -> Result<InstanceSeed, ApiError> {
    let row = sqlx::query(
        "SELECT project_id, name, vcpus, memory_mb, disk_gb, image, restart_generation FROM vms \
         WHERE app_id = ? AND created_at = ? ORDER BY id LIMIT 1",
    )
    .bind(app_id)
    .bind(created_at)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| ApiError::internal(&"the deployment has no instance"))?;
    Ok(InstanceSeed {
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        vcpus: row.try_get("vcpus")?,
        memory_mb: row.try_get("memory_mb")?,
        disk_gb: row.try_get("disk_gb")?,
        image: row.try_get("image")?,
        restart_generation: row.try_get("restart_generation")?,
    })
}

/// The next node of the scheduler's order that has no VM of the deployment,
/// or `None` when every candidate already has one.
async fn next_instance_node(
    conn: &mut SqliteConnection,
    app_id: &str,
    linked: &[LinkedVm],
) -> Result<Option<String>, ApiError> {
    let placement = app_placement(conn, app_id).await?;
    let used: HashSet<String> = linked.iter().map(|vm| vm.node_id.clone()).collect();
    let candidates = crate::scheduler::candidate_nodes(conn, app_id, &placement).await?;
    Ok(candidates.into_iter().find(|node| !used.contains(node)))
}

/// Whether every VM of the deployment is `running`, counting the VM being
/// observed as running.
fn all_linked_running(linked: &[LinkedVm], observed_id: &str) -> bool {
    !linked.is_empty()
        && linked
            .iter()
            .all(|vm| vm.id == observed_id || vm.observed_status == "running")
}

/// Truncates a node-supplied failure message to the stored bound.
fn failure_error(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .unwrap_or("the deployment failed")
        .chars()
        .take(2000)
        .collect()
}

/// App status after a failed deployment deleted every VM linked to it.
async fn app_status_after_failure(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<(), ApiError> {
    let remaining: Option<(String,)> =
        sqlx::query_as("SELECT id FROM vms WHERE app_id = ? LIMIT 1")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    if remaining.is_some() {
        return recompute_app_status(conn, app_id, None).await;
    }
    let row = sqlx::query("SELECT status, current_deployment_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let current: String = row.try_get("status")?;
    let current = AppStatus::parse(&current)
        .ok_or_else(|| ApiError::internal(&format!("unknown app status {current:?}")))?;
    let current_deployment_id: Option<String> = row.try_get("current_deployment_id")?;
    let target = if current_deployment_id.is_some() {
        AppStatus::Down
    } else {
        AppStatus::Stopped
    };
    if target != current {
        set_app_status(conn, app_id, target, &now_rfc3339()).await?;
    }
    Ok(())
}

/// A node-originated `deployment.failed` event from `node_id` (its `data`
/// object: `deployment_id`, `app_id`, `phase`, `error`). Accepted only when
/// one of the deployment's instances is placed on `node_id` and the
/// deployment is still in progress; anything else is ignored. Returns the VMs
/// whose desired state changed.
///
/// # Errors
///
/// A database error.
pub async fn on_node_deployment_failed(
    store: &Store,
    node_id: &str,
    data: &Value,
) -> Result<Vec<String>, ApiError> {
    let Some(deployment_id) = data.get("deployment_id").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if !is_id_with_prefix("dep", deployment_id) {
        return Ok(Vec::new());
    }
    let mut w = store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE id = ?"
    ))
    .bind(deployment_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    let Some(row) = row else {
        w.commit().await?;
        return Ok(Vec::new());
    };
    let before = deployment_from_row(&row)?;
    if !before.status.in_progress() {
        w.commit().await?;
        return Ok(Vec::new());
    }
    // The event is accepted when any instance of the deployment runs on the
    // reporting node.
    let linked = linked_vms(&mut w.tx, &before.app_id, &before.created_at).await?;
    if !linked.iter().any(|vm| vm.node_id == node_id) {
        w.commit().await?;
        return Ok(Vec::new());
    }
    let Some(phase) = data
        .get("phase")
        .and_then(Value::as_str)
        .and_then(DeploymentPhase::parse)
    else {
        w.commit().await?;
        return Ok(Vec::new());
    };

    let mut after = before.clone();
    after.status = DeploymentStatus::Failed;
    after.phase = Some(phase);
    after.error = Some(failure_error(data.get("error")));
    after.finished_at = Some(now_rfc3339());
    save_deployment(&mut w.tx, &before, &after).await?;
    // The node's event is already stored by the session; emitting another
    // `deployment.failed` here would duplicate it.

    let deleted = delete_linked_vms(&mut w.tx, &before.app_id, &before.created_at).await?;
    app_status_after_failure(&mut w.tx, &before.app_id).await?;
    w.commit().await?;
    Ok(deleted)
}

/// The primary could not resolve the image of app VM `vm_id` within its retry
/// budget: its in-progress deployment fails in phase `pull` with `error`, and
/// every VM row linked to that deployment is deleted. Returns the VMs whose
/// desired state changed.
///
/// # Errors
///
/// A database error.
pub async fn on_image_unresolvable(
    store: &Store,
    vm_id: &str,
    error: &str,
) -> Result<Vec<String>, ApiError> {
    if !is_id_with_prefix("vm", vm_id) {
        return Ok(Vec::new());
    }
    let mut w = store.begin_write().await?;
    let row = sqlx::query("SELECT app_id, created_at FROM vms WHERE id = ?")
        .bind(vm_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    let Some(row) = row else {
        w.commit().await?;
        return Ok(Vec::new());
    };
    let app_id: Option<String> = row.try_get("app_id")?;
    let Some(app_id) = app_id else {
        w.commit().await?;
        return Ok(Vec::new());
    };
    let created_at: String = row.try_get("created_at")?;
    let Some(before) = linked_deployment(&mut w.tx, &app_id, &created_at).await? else {
        w.commit().await?;
        return Ok(Vec::new());
    };
    if !before.status.in_progress() {
        w.commit().await?;
        return Ok(Vec::new());
    }
    let mut after = before.clone();
    after.status = DeploymentStatus::Failed;
    after.phase = Some(DeploymentPhase::Pull);
    after.error = Some(error.chars().take(2000).collect());
    after.finished_at = Some(now_rfc3339());
    save_deployment(&mut w.tx, &before, &after).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "deployment.failed",
            source: Source::ControlPlane,
            subject_type: "deployment",
            subject_id: &after.id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({
                "deployment_id": after.id,
                "app_id": after.app_id,
                "phase": DeploymentPhase::Pull.as_str(),
                "error": after.error,
            }),
        },
    )
    .await?;

    let deleted = delete_linked_vms(&mut w.tx, &app_id, &created_at).await?;
    app_status_after_failure(&mut w.tx, &app_id).await?;
    w.commit().await?;
    Ok(deleted)
}

/// Applies a new replica count without a deployment (ADR 0142).
///
/// With no rollout in progress and a current deployment, more replicas add
/// VM rows linked to it on scheduler nodes that have none, and fewer delete
/// instances in [`removal_order`]: those on nodes with a tunnel are kept when
/// the app has `tunnel` domains, otherwise the highest node ids go. A
/// scale-down that still leaves those domains without a tunnel reports them
/// ([`report_unserved_tunnel_domains`]). When the eligible nodes cannot hold
/// the new count the function writes nothing and returns `validation_failed`,
/// so the caller's whole transaction fails. A rollout in progress, or an app
/// without a current deployment, is left alone: the rollout and the next
/// deploy read `apps.replicas` themselves. Returns the changed VM ids.
///
/// # Errors
///
/// `validation_failed` when the scheduler is short of nodes, or a database
/// error.
pub(crate) async fn scale(
    conn: &mut SqliteConnection,
    app_id: &str,
    replicas: i64,
) -> Result<Vec<String>, ApiError> {
    let row = sqlx::query("SELECT current_deployment_id, placement FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(Vec::new());
    };
    let current_deployment_id: Option<String> = row.try_get("current_deployment_id")?;
    let Some(current_deployment_id) = current_deployment_id else {
        return Ok(Vec::new());
    };
    let in_progress: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM deployments WHERE app_id = ? AND status IN ('pending', 'preparing', 'starting') LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if in_progress.is_some() {
        return Ok(Vec::new());
    }
    let created: Option<(String,)> =
        sqlx::query_as("SELECT created_at FROM deployments WHERE id = ? AND app_id = ?")
            .bind(&current_deployment_id)
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((created_at,)) = created else {
        return Ok(Vec::new());
    };

    let linked = linked_vms(conn, app_id, &created_at).await?;
    let count = linked.len() as i64;
    if replicas == count {
        return Ok(Vec::new());
    }

    if replicas > count {
        let placement: String = row.try_get("placement")?;
        let placement: AppPlacement = serde_json::from_str(&placement)
            .map_err(|error| ApiError::internal(&format!("stored placement: {error}")))?;
        let used: HashSet<String> = linked.iter().map(|vm| vm.node_id.clone()).collect();
        let available: Vec<String> = crate::scheduler::candidate_nodes(conn, app_id, &placement)
            .await?
            .into_iter()
            .filter(|node| !used.contains(node))
            .collect();
        let needed = usize::try_from(replicas - count).unwrap_or(usize::MAX);
        if available.len() < needed {
            return Err(ApiError::validation(format!(
                "replicas {replicas} is above the {} eligible nodes",
                count as usize + available.len()
            )));
        }
        let seed = first_instance(conn, app_id, &created_at).await?;
        let now = now_rfc3339();
        let mut added = Vec::new();
        for node_id in available.into_iter().take(needed) {
            let vm_id = insert_app_vm(
                conn,
                &NewVm {
                    project_id: &seed.project_id,
                    app_id,
                    node_id: &node_id,
                    name: &seed.name,
                    vcpus: seed.vcpus,
                    memory_mb: seed.memory_mb,
                    disk_gb: seed.disk_gb,
                    image: &seed.image,
                    restart_generation: seed.restart_generation,
                    created_at: &created_at,
                },
                &deploy_actor(),
                &now,
            )
            .await?;
            added.push(vm_id);
        }
        return Ok(added);
    }

    // Fewer replicas: see `removal_order` for which instances go.
    let mut linked = linked;
    removal_order(conn, app_id, &mut linked).await?;
    let extras = usize::try_from(count - replicas).unwrap_or(usize::MAX);
    let mut removed = Vec::new();
    for vm in linked.into_iter().take(extras) {
        sqlx::query("DELETE FROM vms WHERE id = ?")
            .bind(&vm.id)
            .execute(&mut *conn)
            .await?;
        changelog::append(conn, &deploy_actor(), "vm", &vm.id, Op::Delete, json!({})).await?;
        removed.push(vm.id);
    }
    report_unserved_tunnel_domains(conn, app_id).await?;
    Ok(removed)
}

/// Orders an app's instances so the ones a scale-down removes come first.
///
/// An app with one replica has its `tunnel` hostnames served by the tunnel of
/// the node that runs it, so when the app has `tunnel` domains the instances
/// on nodes mapped to a tunnel (`node_tunnels`) are kept before the others.
/// Within each group the instances on the highest node ids go first, as they
/// always did.
async fn removal_order(
    conn: &mut SqliteConnection,
    app_id: &str,
    linked: &mut [LinkedVm],
) -> Result<(), ApiError> {
    let has_tunnel_domain: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM domains WHERE app_id = ? AND tls_mode = 'tunnel' LIMIT 1")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let tunnel_nodes: HashSet<String> = if has_tunnel_domain.is_some() {
        sqlx::query_as::<_, (String,)>("SELECT node_id FROM node_tunnels")
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(|(node_id,)| node_id)
            .collect()
    } else {
        HashSet::new()
    };
    sort_for_removal(linked, &tunnel_nodes);
    Ok(())
}

/// The pure part of [`removal_order`]: instances on nodes outside
/// `tunnel_nodes` first, then by node id, highest first.
fn sort_for_removal(linked: &mut [LinkedVm], tunnel_nodes: &HashSet<String>) {
    linked.sort_by(|a, b| {
        let a_keep = tunnel_nodes.contains(&a.node_id);
        let b_keep = tunnel_nodes.contains(&b.node_id);
        a_keep.cmp(&b_keep).then_with(|| b.node_id.cmp(&a.node_id))
    });
}

/// After a scale-down, reports every `tunnel` domain that no tunnel serves
/// any more: the app has one replica, at least one instance meant to run,
/// and none of them on a node mapped to a tunnel. Each such domain gets a
/// `certificate.failed` event with `tls_mode` `tunnel`, the only per-domain
/// failure signal the contract has, so the change is never silent.
async fn report_unserved_tunnel_domains(
    conn: &mut SqliteConnection,
    app_id: &str,
) -> Result<(), ApiError> {
    let app: Option<(i64, String)> =
        sqlx::query_as("SELECT replicas, project_id FROM apps WHERE id = ?")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((replicas, project_id)) = app else {
        return Ok(());
    };
    // With more replicas the edge serves the hostnames from its own tunnel.
    if replicas != 1 {
        return Ok(());
    }
    let (running, on_tunnel): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(t.node_id) FROM vms v \
         LEFT JOIN node_tunnels t ON t.node_id = v.node_id \
         WHERE v.app_id = ? AND v.desired_status = 'running'",
    )
    .bind(app_id)
    .fetch_one(&mut *conn)
    .await?;
    if running == 0 || on_tunnel > 0 {
        return Ok(());
    }
    let domains: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, hostname FROM domains WHERE app_id = ? AND tls_mode = 'tunnel' \
         ORDER BY hostname",
    )
    .bind(app_id)
    .fetch_all(&mut *conn)
    .await?;
    for (domain_id, hostname) in domains {
        tracing::warn!(
            app_id,
            hostname,
            "no instance of the app runs on a node with a tunnel; the hostname is not served"
        );
        events::emit(
            conn,
            NewEvent {
                kind: "certificate.failed",
                source: Source::ControlPlane,
                subject_type: "domain",
                subject_id: &domain_id,
                project_id: Some(&project_id),
                request_id: None,
                actor: None,
                data: json!({
                    "domain_id": domain_id,
                    "hostname": hostname,
                    "tls_mode": "tunnel",
                    "error": "no instance of the app runs on a node with a tunnel, \
                              so no tunnel serves this hostname",
                }),
            },
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{self, Capacity, Kvm, LocalNode};
    use crate::store::Store;

    fn local_node() -> LocalNode {
        LocalNode {
            name: "test-node".to_string(),
            capacity: Capacity {
                cpus: 8,
                memory_mb: 16_384,
                disk_gb: 100,
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

    /// A store with one project, one connected node and one stopped app.
    async fn setup() -> (Store, String, String, String) {
        let store = Store::open_in_memory().await.unwrap();
        let node_id = driver::register_local_node(&store, &local_node())
            .await
            .unwrap();
        let project_id = new_id_with_prefix("prj");
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, 'demo', 'Demo', ?, ?)",
        )
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        let app_id = new_id_with_prefix("app");
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, stateful, ha_policy, image, port, health_check, \
             resources, placement, status, current_deployment_id, restart_generation, created_at, \
             updated_at) VALUES (?, ?, 'web', 0, 'none', 'img@sha256:1', 8080, \
             '{\"path\":\"/\",\"interval_s\":10,\"timeout_s\":2,\"failures\":3}', \
             '{\"vcpus\":1,\"memory_mb\":256,\"disk_gb\":1}', '{\"node_ids\":[]}', 'stopped', \
             NULL, 0, ?, ?)",
        )
        .bind(&app_id)
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        (store, project_id, node_id, app_id)
    }

    fn actor() -> Actor {
        Actor::system("test")
    }

    fn status(vm_id: &str, observed: wire::ObservedStatus, health: wire::Health) -> wire::VmStatus {
        wire::VmStatus {
            vm_id: vm_id.to_string(),
            observed_status: observed,
            health,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        }
    }

    async fn deployment_row(store: &Store, id: &str) -> Deployment {
        let row = sqlx::query(&format!(
            "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE id = ?"
        ))
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        deployment_from_row(&row).unwrap()
    }

    async fn app_row(store: &Store, id: &str) -> (String, Option<String>) {
        sqlx::query_as("SELECT status, current_deployment_id FROM apps WHERE id = ?")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    async fn deployment_changes(store: &Store, id: &str) -> i64 {
        let (n,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM change_log WHERE entity_type = 'deployment' AND entity_id = ?",
        )
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        n
    }

    #[test]
    fn minted_ids_keep_the_contract_prefix_and_length() {
        for prefix in ["app", "dep", "dom"] {
            let id = new_id_with_prefix(prefix);
            assert!(is_id_with_prefix(prefix, &id), "{id}");
            assert!(!is_id_with_prefix("vm", &id), "{id}");
        }
    }

    #[test]
    fn a_bumped_timestamp_is_fixed_width_and_strictly_later() {
        let last = "2026-11-02T10:00:00.999Z";
        let bumped = next_millisecond(last).unwrap();
        assert_eq!(bumped, "2026-11-02T10:00:01.000Z");
        assert_eq!(bumped.len(), 24);
    }

    #[tokio::test]
    async fn observations_drive_a_deployment_to_success_and_retire_the_old_vm() {
        let (store, _project_id, node_id, app_id) = setup().await;

        // A first deployment succeeds and becomes current.
        let mut w = store.begin_write().await.unwrap();
        let first = create_deployment(
            &mut w.tx,
            &app_id,
            "img@sha256:1",
            DeploymentTrigger::Api,
            &actor(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();
        on_spec_sent(
            &store,
            &node_id,
            &[(first.vm_id.clone(), "img@sha256:1".to_string())],
        )
        .await
        .unwrap();
        assert_eq!(
            deployment_row(&store, &first.deployment.id).await.status,
            DeploymentStatus::Preparing
        );
        on_vm_observed(
            &store,
            &node_id,
            &status(
                &first.vm_id,
                wire::ObservedStatus::Running,
                wire::Health::Passing,
            ),
        )
        .await
        .unwrap();
        let (app_status, current) = app_row(&store, &app_id).await;
        assert_eq!(app_status, "running");
        assert_eq!(current.as_deref(), Some(first.deployment.id.as_str()));

        // A second deployment moves through creating → starting → running.
        let mut w = store.begin_write().await.unwrap();
        let second = create_deployment(
            &mut w.tx,
            &app_id,
            "img@sha256:2",
            DeploymentTrigger::Redeploy,
            &actor(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();
        assert_eq!(app_row(&store, &app_id).await.0, "deploying");

        on_vm_observed(
            &store,
            &node_id,
            &status(
                &second.vm_id,
                wire::ObservedStatus::Creating,
                wire::Health::Unknown,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            deployment_row(&store, &second.deployment.id).await.status,
            DeploymentStatus::Preparing
        );
        on_vm_observed(
            &store,
            &node_id,
            &status(
                &second.vm_id,
                wire::ObservedStatus::Starting,
                wire::Health::Unknown,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            deployment_row(&store, &second.deployment.id).await.status,
            DeploymentStatus::Starting
        );

        let retired = on_vm_observed(
            &store,
            &node_id,
            &status(
                &second.vm_id,
                wire::ObservedStatus::Running,
                wire::Health::Passing,
            ),
        )
        .await
        .unwrap();
        assert_eq!(retired, vec![first.vm_id.clone()]);
        let deployment = deployment_row(&store, &second.deployment.id).await;
        assert_eq!(deployment.status, DeploymentStatus::Succeeded);
        assert!(deployment.finished_at.is_some());
        let (app_status, current) = app_row(&store, &app_id).await;
        assert_eq!(app_status, "running");
        assert_eq!(current.as_deref(), Some(second.deployment.id.as_str()));
        let vms: Vec<(String,)> = sqlx::query_as("SELECT id FROM vms WHERE app_id = ?")
            .bind(&app_id)
            .fetch_all(store.pool())
            .await
            .unwrap();
        assert_eq!(vms, vec![(second.vm_id.clone(),)]);
        let (events,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM events WHERE type = 'deployment.succeeded'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert!(events >= 1);

        // A repeated observation writes nothing new.
        let before = deployment_changes(&store, &second.deployment.id).await;
        on_vm_observed(
            &store,
            &node_id,
            &status(
                &second.vm_id,
                wire::ObservedStatus::Running,
                wire::Health::Passing,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            deployment_changes(&store, &second.deployment.id).await,
            before,
            "a repeated observation appends no change-log entry"
        );
    }

    #[tokio::test]
    async fn a_crash_fails_the_deployment() {
        let (store, _project_id, node_id, app_id) = setup().await;
        let mut w = store.begin_write().await.unwrap();
        let created = create_deployment(
            &mut w.tx,
            &app_id,
            "img@sha256:1",
            DeploymentTrigger::Api,
            &actor(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();

        let mut crashed = status(
            &created.vm_id,
            wire::ObservedStatus::Crashed,
            wire::Health::Unknown,
        );
        crashed.last_exit_code = Some(3);
        on_vm_observed(&store, &node_id, &crashed).await.unwrap();

        let deployment = deployment_row(&store, &created.deployment.id).await;
        assert_eq!(deployment.status, DeploymentStatus::Failed);
        assert_eq!(deployment.phase, Some(DeploymentPhase::Boot));
        assert_eq!(
            deployment.error.as_deref(),
            Some("the VM exited with code 3")
        );
        assert!(deployment.finished_at.is_some());
        let (events,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM events WHERE type = 'deployment.failed'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn a_node_failure_from_another_node_is_ignored() {
        let (store, _project_id, node_id, app_id) = setup().await;
        let mut w = store.begin_write().await.unwrap();
        let created = create_deployment(
            &mut w.tx,
            &app_id,
            "img@sha256:1",
            DeploymentTrigger::Api,
            &actor(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();

        let data = json!({
            "deployment_id": created.deployment.id,
            "app_id": app_id,
            "phase": "pull",
            "error": "pull failed",
        });
        let changed = on_node_deployment_failed(&store, "nod_00000000000000000000000000", &data)
            .await
            .unwrap();
        assert!(changed.is_empty());
        assert_eq!(
            deployment_row(&store, &created.deployment.id).await.status,
            DeploymentStatus::Pending
        );
        let (vms,): (i64,) = sqlx::query_as("SELECT count(*) FROM vms WHERE app_id = ?")
            .bind(&app_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(vms, 1);

        // The right node fails it; the VM row is deleted and no second event
        // is emitted (the node event is already stored by the session).
        let changed = on_node_deployment_failed(&store, &node_id, &data)
            .await
            .unwrap();
        assert_eq!(changed, vec![created.vm_id.clone()]);
        let deployment = deployment_row(&store, &created.deployment.id).await;
        assert_eq!(deployment.status, DeploymentStatus::Failed);
        assert_eq!(deployment.phase, Some(DeploymentPhase::Pull));
        let (failed_events,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM events WHERE type = 'deployment.failed'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(failed_events, 0);
        let (vms,): (i64,) = sqlx::query_as("SELECT count(*) FROM vms WHERE app_id = ?")
            .bind(&app_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(vms, 0);
    }

    #[tokio::test]
    async fn an_unresolvable_image_fails_the_pull_and_removes_the_vm() {
        let (store, _project_id, _node_id, app_id) = setup().await;
        let mut w = store.begin_write().await.unwrap();
        let created = create_deployment(
            &mut w.tx,
            &app_id,
            "img@sha256:1",
            DeploymentTrigger::Api,
            &actor(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();

        let changed = on_image_unresolvable(&store, &created.vm_id, "manifest unknown")
            .await
            .unwrap();
        assert_eq!(changed, vec![created.vm_id.clone()]);
        let deployment = deployment_row(&store, &created.deployment.id).await;
        assert_eq!(deployment.status, DeploymentStatus::Failed);
        assert_eq!(deployment.phase, Some(DeploymentPhase::Pull));
        assert_eq!(deployment.error.as_deref(), Some("manifest unknown"));
        let (events,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM events WHERE type = 'deployment.failed'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(events, 1);
    }

    fn linked(id: &str, node_id: &str) -> LinkedVm {
        LinkedVm {
            id: id.to_string(),
            node_id: node_id.to_string(),
            desired_status: "running".to_string(),
            observed_status: "running".to_string(),
        }
    }

    fn removal_ids(linked: &mut [LinkedVm], tunnel_nodes: &[&str]) -> Vec<String> {
        let tunnel_nodes: HashSet<String> = tunnel_nodes.iter().map(ToString::to_string).collect();
        sort_for_removal(linked, &tunnel_nodes);
        linked.iter().map(|vm| vm.id.clone()).collect()
    }

    #[test]
    fn a_scale_down_removes_instances_on_nodes_without_a_tunnel_first() {
        let mut vms = [
            linked("vm_a", "nod_a"),
            linked("vm_b", "nod_b"),
            linked("vm_c", "nod_c"),
        ];
        // No tunnel in play: the highest node ids go first, as before.
        assert_eq!(removal_ids(&mut vms, &[]), ["vm_c", "vm_b", "vm_a"]);
        // The node with a tunnel keeps its instance; the others keep the old
        // order among themselves.
        assert_eq!(removal_ids(&mut vms, &["nod_c"]), ["vm_b", "vm_a", "vm_c"]);
        assert_eq!(
            removal_ids(&mut vms, &["nod_a", "nod_c"]),
            ["vm_b", "vm_c", "vm_a"]
        );
    }
}
