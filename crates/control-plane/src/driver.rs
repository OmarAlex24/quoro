//! The control-plane side of the in-process node driver (M1).
//!
//! In M1 the primary is the only node and there is no node-agent session yet
//! (that is M2). The binary runs a local driver that owns the engine; this
//! module is everything it may ask of the control plane:
//!
//! - [`register_local_node`] records the primary itself as a node at start.
//! - [`desired_vm`] / [`vms_on_node`] read desired state. A VM whose row is
//!   gone was deleted: the driver stops and deletes it on the engine, then
//!   calls [`report_deleted`].
//! - [`report_observed`] writes what the engine reports back, as the node's
//!   identity, through the change log, emitting `vm.started`, `vm.stopped` and
//!   `vm.crashed` on the matching transitions.
//!
//! From M2 the node-agent session calls the same functions with the remote
//! node's reports, so the services do not change.

use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, ActorType, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, NewEvent, Source};
use crate::store::{Store, now_rfc3339};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::SqliteConnection;

/// Capacity triple (`NodeCapacity`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacity {
    pub cpus: u32,
    pub memory_mb: u64,
    pub disk_gb: u64,
}

/// KVM probe result (`KvmInfo`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kvm {
    pub available: bool,
    pub kernel: String,
    pub nested: Option<bool>,
}

/// What the local driver knows about the machine it runs on.
#[derive(Clone, Debug)]
pub struct LocalNode {
    pub name: String,
    pub capacity: Capacity,
    pub kvm: Kvm,
    pub engine_version: String,
    pub agent_version: String,
}

/// VM `desired_status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesiredStatus {
    Running,
    Stopped,
}

/// VM `observed_status` (README "Status enums").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedStatus {
    Creating,
    Stopped,
    Starting,
    Running,
    Stopping,
    Crashed,
    Deleted,
    Unknown,
}

impl ObservedStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Crashed => "crashed",
            Self::Deleted => "deleted",
            Self::Unknown => "unknown",
        }
    }
}

/// The desired state of one VM, as the driver needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredVm {
    pub id: String,
    pub node_id: String,
    pub vcpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub image: String,
    pub desired_status: DesiredStatus,
    pub observed_status: ObservedStatus,
    /// Address last reported for the VM, so a restarted driver keeps it.
    pub ip: Option<String>,
    pub restart_generation: i64,
}

/// One observation of a VM by the node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub status: ObservedStatus,
    pub ip: Option<String>,
    pub exit_code: Option<i64>,
    pub restart_count: Option<i64>,
}

/// Records the primary itself as a node, or refreshes it on later starts.
/// Returns its `nod_` id. The first start inserts a node with `cp_status`
/// `primary`, `status` `connected`, roles `["node"]` and emits `node.enrolled`
/// and `node.connected`; a later start updates versions, capacity and KVM and
/// sets `connected`. Both go through the change log and the audit log as the
/// `system` actor.
///
/// # Errors
///
/// A database error.
pub async fn register_local_node(store: &Store, node: &LocalNode) -> Result<String, ApiError> {
    let mut w = store.begin_write().await?;
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary'")
            .fetch_optional(&mut *w.tx)
            .await?;
    let actor = Actor::system("local-node");
    let now = now_rfc3339();
    let capacity = json!({
        "cpus": node.capacity.cpus,
        "memory_mb": node.capacity.memory_mb,
        "disk_gb": node.capacity.disk_gb,
    });
    let kvm = json!({
        "available": node.kvm.available,
        "kernel": node.kvm.kernel,
        "nested": node.kvm.nested,
    });

    let (id, seq) = if let Some((id,)) = existing {
        let allocated = allocated_capacity(&mut w.tx, &id).await?;
        sqlx::query(
            "UPDATE nodes SET name = ?, capacity = ?, allocated = ?, kvm = ?, engine_version = ?, \
             agent_version = ?, status = 'connected', last_seen_at = ? WHERE id = ?",
        )
        .bind(&node.name)
        .bind(capacity.to_string())
        .bind(allocated.to_string())
        .bind(kvm.to_string())
        .bind(&node.engine_version)
        .bind(&node.agent_version)
        .bind(&now)
        .bind(&id)
        .execute(&mut *w.tx)
        .await?;
        let payload = node_payload(&mut w.tx, &id).await?;
        let seq = changelog::append(&mut w.tx, &actor, "node", &id, Op::Update, payload).await?;
        (id, seq)
    } else {
        let id = new_id(Prefix::Node);
        let allocated = json!({ "cpus": 0, "memory_mb": 0, "disk_gb": 0 });
        // The node keypair and its base64 public key arrive with enrollment in
        // M2; until then the primary is trusted by being in-process.
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at) \
             VALUES (?, ?, ?, 'primary', 'connected', '', NULL, 0, '{}', ?, ?, ?, ?, ?, 0, 0, 0, ?, ?)",
        )
        .bind(&id)
        .bind(&node.name)
        .bind(json!(["node"]).to_string())
        .bind(capacity.to_string())
        .bind(allocated.to_string())
        .bind(kvm.to_string())
        .bind(&node.engine_version)
        .bind(&node.agent_version)
        .bind(&now)
        .bind(&now)
        .execute(&mut *w.tx)
        .await?;
        let payload = node_payload(&mut w.tx, &id).await?;
        let seq = changelog::append(&mut w.tx, &actor, "node", &id, Op::Create, payload).await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "node.enrolled",
                source: Source::ControlPlane,
                subject_type: "node",
                subject_id: &id,
                project_id: None,
                request_id: None,
                actor: None,
                data: json!({ "node_id": id, "name": node.name, "roles": ["node"], "address": null }),
            },
        )
        .await?;
        (id, seq)
    };

    let record = system_record(
        actor,
        "register_local_node",
        Some(Target {
            kind: "node".to_string(),
            id: id.clone(),
        }),
        json!({ "node_id": id, "name": node.name }),
        Some(seq),
    );
    audit::append(&mut w.tx, &record).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "node.connected",
            source: Source::ControlPlane,
            subject_type: "node",
            subject_id: &id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({ "node_id": id, "agent_version": node.agent_version, "log_seq": 0 }),
        },
    )
    .await?;
    w.commit().await?;
    Ok(id)
}

/// The desired state of `vm_id`, or `None` if the VM row does not exist
/// (never created, or deleted).
///
/// # Errors
///
/// A database error.
pub async fn desired_vm(store: &Store, vm_id: &str) -> Result<Option<DesiredVm>, ApiError> {
    let row = sqlx::query(
        "SELECT id, node_id, vcpus, memory_mb, disk_gb, image, desired_status, observed_status, \
         ip, restart_generation FROM vms WHERE id = ?",
    )
    .bind(vm_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let vcpus: i64 = row.try_get("vcpus")?;
    let memory_mb: i64 = row.try_get("memory_mb")?;
    let disk_gb: i64 = row.try_get("disk_gb")?;
    let desired: String = row.try_get("desired_status")?;
    let observed: String = row.try_get("observed_status")?;
    Ok(Some(DesiredVm {
        id: row.try_get("id")?,
        node_id: row.try_get("node_id")?,
        vcpus: u32::try_from(vcpus).map_err(|e| ApiError::internal(&format!("vms.vcpus: {e}")))?,
        memory_mb: u32::try_from(memory_mb)
            .map_err(|e| ApiError::internal(&format!("vms.memory_mb: {e}")))?,
        disk_gb: u32::try_from(disk_gb)
            .map_err(|e| ApiError::internal(&format!("vms.disk_gb: {e}")))?,
        image: row.try_get("image")?,
        desired_status: if desired == "running" {
            DesiredStatus::Running
        } else {
            DesiredStatus::Stopped
        },
        observed_status: parse_observed(&observed),
        ip: row.try_get("ip")?,
        restart_generation: row.try_get("restart_generation")?,
    }))
}

/// Ids of every VM row placed on `node_id`, for the resync at driver start.
///
/// # Errors
///
/// A database error.
pub async fn vms_on_node(store: &Store, node_id: &str) -> Result<Vec<String>, ApiError> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM vms WHERE node_id = ? ORDER BY id")
        .bind(node_id)
        .fetch_all(store.pool())
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Writes an observation of `vm_id` by `node_id` (actor `node`). A no-op when
/// nothing changed or the row is gone. On a change it updates
/// `observed_status`, `ip`, `last_exit_code`, `restart_count` and
/// `updated_at`, appends one change-log entry, and emits `vm.started` on
/// entering `running`, `vm.stopped` on entering `stopped` from `stopping` or
/// `running`, and `vm.crashed` on entering `crashed`.
///
/// # Errors
///
/// A database error.
pub async fn report_observed(
    store: &Store,
    node_id: &str,
    vm_id: &str,
    observation: &Observation,
) -> Result<(), ApiError> {
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    let mut w = store.begin_write().await?;
    let row = sqlx::query(
        "SELECT observed_status, ip, last_exit_code, restart_count, project_id, app_id \
         FROM vms WHERE id = ? AND node_id = ?",
    )
    .bind(vm_id)
    .bind(node_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let old_status: String = row.try_get("observed_status")?;
    let old_ip: Option<String> = row.try_get("ip")?;
    let old_exit: Option<i64> = row.try_get("last_exit_code")?;
    let old_restart: i64 = row.try_get("restart_count")?;
    let project_id: Option<String> = row.try_get("project_id")?;
    let app_id: Option<String> = row.try_get("app_id")?;

    let new_status = observation.status.as_str().to_string();
    // A missing field means "nothing reported this time", not "clear it".
    let new_ip = observation.ip.clone().or_else(|| old_ip.clone());
    let new_exit = observation.exit_code.or(old_exit);
    let new_restart = observation.restart_count.unwrap_or(old_restart);
    if new_status == old_status
        && new_ip == old_ip
        && new_exit == old_exit
        && new_restart == old_restart
    {
        return Ok(());
    }

    let now = now_rfc3339();
    sqlx::query(
        "UPDATE vms SET observed_status = ?, ip = ?, last_exit_code = ?, restart_count = ?, \
         updated_at = ? WHERE id = ?",
    )
    .bind(&new_status)
    .bind(&new_ip)
    .bind(new_exit)
    .bind(new_restart)
    .bind(&now)
    .bind(vm_id)
    .execute(&mut *w.tx)
    .await?;
    let payload = vm_payload(&mut w.tx, vm_id).await?;
    changelog::append(&mut w.tx, &actor, "vm", vm_id, Op::Update, payload).await?;

    let source = || Source::Node(node_id.to_string());
    if new_status == "running" && old_status != "running" {
        let mut data = json!({ "vm_id": vm_id, "node_id": node_id });
        if let Some(ip) = &new_ip {
            data["ip"] = Value::from(ip.clone());
        }
        if let Some(app_id) = &app_id {
            data["app_id"] = Value::from(app_id.clone());
        }
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "vm.started",
                source: source(),
                subject_type: "vm",
                subject_id: vm_id,
                project_id: project_id.as_deref(),
                request_id: None,
                actor: None,
                data,
            },
        )
        .await?;
    }
    if new_status == "stopped" && matches!(old_status.as_str(), "stopping" | "running") {
        let mut data = json!({ "vm_id": vm_id, "node_id": node_id });
        if let Some(exit_code) = new_exit {
            data["exit_code"] = Value::from(exit_code);
        }
        if let Some(app_id) = &app_id {
            data["app_id"] = Value::from(app_id.clone());
        }
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "vm.stopped",
                source: source(),
                subject_type: "vm",
                subject_id: vm_id,
                project_id: project_id.as_deref(),
                request_id: None,
                actor: None,
                data,
            },
        )
        .await?;
    }
    if new_status == "crashed" && old_status != "crashed" {
        let mut data = json!({
            "vm_id": vm_id,
            "node_id": node_id,
            "restart_count": new_restart.max(0),
        });
        if let Some(exit_code) = new_exit {
            data["exit_code"] = Value::from(exit_code);
        }
        if let Some(app_id) = &app_id {
            data["app_id"] = Value::from(app_id.clone());
        }
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "vm.crashed",
                source: source(),
                subject_type: "vm",
                subject_id: vm_id,
                project_id: project_id.as_deref(),
                request_id: None,
                actor: None,
                data,
            },
        )
        .await?;
    }
    w.commit().await?;
    Ok(())
}

/// Records that the node removed a deleted VM from the engine: emits
/// `vm.deleted` (actor `node`). The VM row is already gone.
///
/// # Errors
///
/// A database error.
pub async fn report_deleted(store: &Store, node_id: &str, vm_id: &str) -> Result<(), ApiError> {
    let mut w = store.begin_write().await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "vm.deleted",
            source: Source::Node(node_id.to_string()),
            subject_type: "vm",
            subject_id: vm_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({ "vm_id": vm_id, "node_id": node_id }),
        },
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// Maps `vms.observed_status` to [`ObservedStatus`]; an unknown value reads
/// as [`ObservedStatus::Unknown`].
fn parse_observed(status: &str) -> ObservedStatus {
    match status {
        "creating" => ObservedStatus::Creating,
        "stopped" => ObservedStatus::Stopped,
        "starting" => ObservedStatus::Starting,
        "running" => ObservedStatus::Running,
        "stopping" => ObservedStatus::Stopping,
        "crashed" => ObservedStatus::Crashed,
        "deleted" => ObservedStatus::Deleted,
        _ => ObservedStatus::Unknown,
    }
}

/// The audit record of a system-initiated driver call.
fn system_record(
    actor: Actor,
    operation: &str,
    target: Option<Target>,
    params: Value,
    change_seq: Option<i64>,
) -> Record {
    Record {
        actor,
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: operation.to_string(),
        scope: None,
        target_type: target.as_ref().map(|t| t.kind.clone()),
        target_id: target.as_ref().map(|t| t.id.clone()),
        project_id: None,
        params,
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: new_id(Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq,
    }
}

/// Parses a `nodes` JSON column of the payload into a JSON value.
fn json_value(text: String) -> Result<Value, ApiError> {
    serde_json::from_str(&text).map_err(|e| ApiError::internal(&format!("node JSON column: {e}")))
}

/// Parses a nullable `nodes` JSON column of the payload.
fn optional_json_value(text: Option<String>) -> Result<Value, ApiError> {
    match text {
        None => Ok(Value::Null),
        Some(text) => json_value(text),
    }
}

/// The full `nodes` row as JSON, the change-log payload of a node change.
async fn node_payload(conn: &mut SqliteConnection, id: &str) -> Result<Value, ApiError> {
    let row = sqlx::query("SELECT * FROM nodes WHERE id = ?")
        .bind(id)
        .fetch_one(&mut *conn)
        .await?;
    Ok(json!({
        "id": row.try_get::<String, _>("id")?,
        "name": row.try_get::<String, _>("name")?,
        "roles": json_value(row.try_get("roles")?)?,
        "cp_status": row.try_get::<Option<String>, _>("cp_status")?,
        "status": row.try_get::<String, _>("status")?,
        "public_key": row.try_get::<String, _>("public_key")?,
        "address": row.try_get::<Option<String>, _>("address")?,
        "behind_nat": row.try_get::<i64, _>("behind_nat")?,
        "labels": json_value(row.try_get("labels")?)?,
        "capacity": json_value(row.try_get("capacity")?)?,
        "allocated": json_value(row.try_get("allocated")?)?,
        "kvm": json_value(row.try_get("kvm")?)?,
        "engine_version": row.try_get::<Option<String>, _>("engine_version")?,
        "agent_version": row.try_get::<Option<String>, _>("agent_version")?,
        "desired_rev": row.try_get::<i64, _>("desired_rev")?,
        "applied_rev": row.try_get::<i64, _>("applied_rev")?,
        "log_seq": row.try_get::<i64, _>("log_seq")?,
        "last_seen_at": row.try_get::<Option<String>, _>("last_seen_at")?,
        "created_at": row.try_get::<String, _>("created_at")?,
        "fence_failure": optional_json_value(row.try_get("fence_failure")?)?,
        "fence_ack_pending": optional_json_value(row.try_get("fence_ack_pending")?)?,
    }))
}

/// The full `vms` row as JSON, the change-log payload of a VM change.
async fn vm_payload(conn: &mut SqliteConnection, id: &str) -> Result<Value, ApiError> {
    let row = sqlx::query("SELECT * FROM vms WHERE id = ?")
        .bind(id)
        .fetch_one(&mut *conn)
        .await?;
    Ok(json!({
        "id": row.try_get::<String, _>("id")?,
        "project_id": row.try_get::<String, _>("project_id")?,
        "app_id": row.try_get::<Option<String>, _>("app_id")?,
        "node_id": row.try_get::<String, _>("node_id")?,
        "name": row.try_get::<String, _>("name")?,
        "vcpus": row.try_get::<i64, _>("vcpus")?,
        "memory_mb": row.try_get::<i64, _>("memory_mb")?,
        "disk_gb": row.try_get::<i64, _>("disk_gb")?,
        "image": row.try_get::<String, _>("image")?,
        "desired_status": row.try_get::<String, _>("desired_status")?,
        "observed_status": row.try_get::<String, _>("observed_status")?,
        "ip": row.try_get::<Option<String>, _>("ip")?,
        "restart_generation": row.try_get::<i64, _>("restart_generation")?,
        "restart_count": row.try_get::<i64, _>("restart_count")?,
        "last_exit_code": row.try_get::<Option<i64>, _>("last_exit_code")?,
        "created_at": row.try_get::<String, _>("created_at")?,
        "updated_at": row.try_get::<String, _>("updated_at")?,
    }))
}

/// Sum of the `vcpus`, `memory_mb` and `disk_gb` of the VMs placed on a node.
async fn allocated_capacity(conn: &mut SqliteConnection, node_id: &str) -> Result<Value, ApiError> {
    let (cpus, memory_mb, disk_gb): (i64, i64, i64) = sqlx::query_as(
        "SELECT IFNULL(SUM(vcpus), 0), IFNULL(SUM(memory_mb), 0), IFNULL(SUM(disk_gb), 0) \
         FROM vms WHERE node_id = ?",
    )
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(json!({ "cpus": cpus, "memory_mb": memory_mb, "disk_gb": disk_gb }))
}
