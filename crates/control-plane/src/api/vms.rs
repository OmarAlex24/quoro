//! `/api/v1/vms`: standalone microVMs (issue #26).
//!
//! A VM is a row in `vms` (desired state) that the local node driver converges
//! to on the engine. The endpoints here only change desired state and tell the
//! driver (`AppState::notify_driver`); observed state arrives through
//! [`crate::driver::report_observed`].

use crate::AppState;
use crate::api::page::{self, PageQuery};
use crate::audit::{self, Outcome};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, NewEvent, Source};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::now_rfc3339;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// Audit/MCP operation string of `createVm`; also its idempotency key scope.
const CREATE_VM: &str = "POST /api/v1/vms";

/// Columns of [`Vm`], in the order [`vm_from_row`] expects them.
const VM_COLUMNS: &str = "id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
     desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
     created_at, updated_at";

/// `vms.desired_status` (`Vm.desired_status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DesiredStatus {
    Running,
    Stopped,
}

impl DesiredStatus {
    fn parse(value: &str) -> Result<Self, ApiError> {
        match value {
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            other => Err(ApiError::internal(&format!(
                "unknown desired_status {other:?}"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
        }
    }
}

/// `vms.observed_status` (`Vm.observed_status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
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
    fn parse(value: &str) -> Result<Self, ApiError> {
        match value {
            "creating" => Ok(Self::Creating),
            "stopped" => Ok(Self::Stopped),
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "stopping" => Ok(Self::Stopping),
            "crashed" => Ok(Self::Crashed),
            "deleted" => Ok(Self::Deleted),
            "unknown" => Ok(Self::Unknown),
            other => Err(ApiError::internal(&format!(
                "unknown observed_status {other:?}"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
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

/// One microVM (`openapi.yaml` `Vm`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Vm")]
pub struct Vm {
    /// VM id.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Project the VM belongs to.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: String,
    /// App whose instance this VM is, or null for a standalone VM.
    #[serde(default)]
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$", required = true)]
    pub app_id: Option<String>,
    /// Node the VM is placed on.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// VM name.
    pub name: String,
    /// Number of virtual CPUs.
    #[schema(minimum = 1)]
    pub vcpus: i64,
    /// Memory in mebibytes.
    #[schema(minimum = 1)]
    pub memory_mb: i64,
    /// Root disk in gibibytes.
    #[schema(minimum = 0)]
    pub disk_gb: i64,
    /// OCI reference with digest, the rootfs the VM boots.
    pub image: String,
    /// Status the control plane wants.
    #[schema(inline)]
    pub desired_status: DesiredStatus,
    /// Status the node last reported.
    #[schema(inline)]
    pub observed_status: ObservedStatus,
    /// Private address of the VM on the node bridge.
    pub ip: Option<String>,
    /// Counter bumped by a restart; the node restarts the VM when it changes.
    #[schema(minimum = 0)]
    pub restart_generation: i64,
    /// Number of restarts the node has performed.
    #[schema(minimum = 0)]
    pub restart_count: i64,
    /// Exit code of the last VM process, or null.
    pub last_exit_code: Option<i64>,
    /// When the VM was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the VM record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// One page of VMs (`openapi.yaml` `VmList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(title = "VmList")]
pub struct VmList {
    pub items: Vec<Vm>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// Body of `createVm` (`openapi.yaml` `CreateVmRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateVmRequest")]
pub struct CreateVmRequest {
    /// Project the VM belongs to.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: String,
    /// App whose instance the VM is. Omit for a standalone VM.
    #[serde(default)]
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: Option<String>,
    /// Node to place the VM on.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// VM name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Number of virtual CPUs.
    #[schema(minimum = 1)]
    pub vcpus: i64,
    /// Memory in mebibytes.
    #[schema(minimum = 1)]
    pub memory_mb: i64,
    /// Root disk in gibibytes.
    #[schema(minimum = 0)]
    pub disk_gb: i64,
    /// OCI reference the VM boots.
    pub image: String,
}

/// Query of `listVms`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListVmsQuery {
    /// Only VMs of this project.
    #[param(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// Only VMs placed on this node.
    #[param(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: Option<String>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListVmsQuery {
    fn page(&self) -> PageQuery {
        PageQuery {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Rows of `vms` as [`Vm`].
fn vm_from_row(row: &SqliteRow) -> Result<Vm, ApiError> {
    let desired: String = row.get("desired_status");
    let observed: String = row.get("observed_status");
    Ok(Vm {
        id: row.get("id"),
        project_id: row.get("project_id"),
        app_id: row.get("app_id"),
        node_id: row.get("node_id"),
        name: row.get("name"),
        vcpus: row.get("vcpus"),
        memory_mb: row.get("memory_mb"),
        disk_gb: row.get("disk_gb"),
        image: row.get("image"),
        desired_status: DesiredStatus::parse(&desired)?,
        observed_status: ObservedStatus::parse(&observed)?,
        ip: row.get("ip"),
        restart_generation: row.get("restart_generation"),
        restart_count: row.get("restart_count"),
        last_exit_code: row.get("last_exit_code"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn vm_target(vm_id: &str) -> Target {
    Target {
        kind: "vm".to_string(),
        id: vm_id.to_string(),
    }
}

fn body_value<T: Serialize>(body: &T) -> Value {
    serde_json::to_value(body).unwrap_or(Value::Null)
}

/// The `Idempotency-Key` of the call, when present and well-formed.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ApiError::validation("Idempotency-Key is not valid ASCII"))?;
    if key.is_empty() {
        return Ok(None);
    }
    if key.chars().count() > 128 {
        return Err(ApiError::validation(
            "Idempotency-Key is longer than 128 characters",
        ));
    }
    Ok(Some(key.to_string()))
}

/// The project id of the call when it names an existing project. Audit rows
/// carry a foreign key to `projects`, so an unknown id is omitted.
async fn known_project(state: &AppState, project_id: Option<&str>) -> Option<String> {
    let id = project_id?;
    if !is_id(Prefix::Project, id) {
        return None;
    }
    sqlx::query_as::<_, (String,)>("SELECT id FROM projects WHERE id = ?")
        .bind(id)
        .fetch_optional(state.store.pool())
        .await
        .ok()
        .flatten()
        .map(|row| row.0)
}

/// Validates a `createVm` body.
fn validate_create(body: &CreateVmRequest) -> Result<(), ApiError> {
    if !is_id(Prefix::Project, &body.project_id) {
        return Err(ApiError::validation("project_id is not a valid project id"));
    }
    if !is_id(Prefix::Node, &body.node_id) {
        return Err(ApiError::validation("node_id is not a valid node id"));
    }
    // M1 has no apps (they arrive with the app workstream), so an instance of
    // an app cannot be created yet; `Vm.app_id` stays null.
    if body.app_id.is_some() {
        return Err(ApiError::validation(
            "app_id is not supported: no app exists in M1",
        ));
    }
    let name_len = body.name.chars().count();
    if !(1..=200).contains(&name_len) {
        return Err(ApiError::validation(format!(
            "name has {name_len} characters, expected 1-200"
        )));
    }
    // The engine takes a `u8` count of vCPUs.
    if !(1..=255).contains(&body.vcpus) {
        return Err(ApiError::validation(format!(
            "vcpus {} is outside 1-255",
            body.vcpus
        )));
    }
    if body.memory_mb < 1 {
        return Err(ApiError::validation("memory_mb must be at least 1"));
    }
    // The contract (`CreateVmRequest.disk_gb`) allows 0 but `vms.disk_gb` has
    // `CHECK (disk_gb >= 1)`, so 0 is refused until Q-0088 is decided.
    if body.disk_gb < 1 {
        return Err(ApiError::validation("disk_gb must be at least 1"));
    }
    if body.image.trim().is_empty() {
        return Err(ApiError::validation("image must not be empty"));
    }
    Ok(())
}

/// `GET /api/v1/vms`.
///
/// # Errors
///
/// `unauthorized`/`forbidden` without `vm:read`, `validation_failed` for a
/// malformed filter or page bound, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/vms",
    tag = "vms",
    operation_id = "listVms",
    extensions(("x-quoro-scope" = json!("vm:read"))),
    params(ListVmsQuery),
    responses(
        (status = 200, description = "One page of VMs.", body = VmList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_vms(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Query(query): Query<ListVmsQuery>,
) -> Result<Json<VmList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, "GET /api/v1/vms", &headers);
    ctx.authorize_read(Scope::VmRead)?;

    if let Some(project_id) = &query.project_id
        && !is_id(Prefix::Project, project_id)
    {
        return Err(ApiError::validation("project_id is not a valid project id"));
    }
    if let Some(node_id) = &query.node_id
        && !is_id(Prefix::Node, node_id)
    {
        return Err(ApiError::validation("node_id is not a valid node id"));
    }

    let page_query = query.page();
    let limit = page_query.limit()?;
    let after = page_query.after();

    let mut sql = format!("SELECT {VM_COLUMNS} FROM vms WHERE id > ?");
    if query.project_id.is_some() {
        sql.push_str(" AND project_id = ?");
    }
    if query.node_id.is_some() {
        sql.push_str(" AND node_id = ?");
    }
    sql.push_str(" ORDER BY id LIMIT ?");

    let mut statement = sqlx::query(&sql).bind(after);
    if let Some(project_id) = &query.project_id {
        statement = statement.bind(project_id);
    }
    if let Some(node_id) = &query.node_id {
        statement = statement.bind(node_id);
    }
    let rows = statement
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
    let vms = rows
        .iter()
        .map(vm_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let (items, next_cursor) = page::finish(vms, limit, |vm| vm.id.clone());
    Ok(Json(VmList { items, next_cursor }))
}

/// `GET /api/v1/vms/{vm_id}`.
///
/// # Errors
///
/// `unauthorized`/`forbidden` without `vm:read`, `not_found` for an unknown or
/// malformed id, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/vms/{vm_id}",
    tag = "vms",
    operation_id = "getVm",
    extensions(("x-quoro-scope" = json!("vm:read"))),
    params(
        ("vm_id" = String, Path, description = "VM id (`vm_...`).",
            pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The VM.", body = Vm,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_vm(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(vm_id): Path<String>,
) -> Result<Json<Vm>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, "GET /api/v1/vms/{vm_id}", &headers);
    ctx.authorize_read(Scope::VmRead)?;

    if !is_id(Prefix::Vm, &vm_id) {
        return Err(ApiError::not_found("vm"));
    }
    let row = sqlx::query(&format!("SELECT {VM_COLUMNS} FROM vms WHERE id = ?"))
        .bind(&vm_id)
        .fetch_optional(state.store.pool())
        .await?;
    match row {
        Some(row) => Ok(Json(vm_from_row(&row)?)),
        None => Err(ApiError::not_found("vm")),
    }
}

/// `POST /api/v1/vms`.
///
/// # Errors
///
/// `forbidden` without `vm:create`, `conflict` on an idempotency clash,
/// `validation_failed` for a bad body or an unknown project/node, or a
/// database error.
#[utoipa::path(
    post,
    path = "/api/v1/vms",
    tag = "vms",
    operation_id = "createVm",
    extensions(("x-quoro-scope" = json!("vm:create"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = CreateVmRequest, content_type = "application/json",
        description = "Placement, size and image of the VM."),
    responses(
        (status = 201, description = "The VM record was created.", body = Vm,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_vm(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    ApiJson(body): ApiJson<CreateVmRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CREATE_VM, &headers);
    let params = body_value(&body);
    let requested_project = if is_id(Prefix::Project, &body.project_id) {
        Some(body.project_id.clone())
    } else {
        None
    };
    let audit_project = known_project(&state, requested_project.as_deref()).await;

    ctx.authorize(
        &state,
        Scope::VmCreate,
        None,
        audit_project.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(&ctx.identity.user_id, CREATE_VM, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, body) => {
                let id = body["id"].as_str().unwrap_or_default();
                ctx.audit_replay(
                    &state,
                    Scope::VmCreate,
                    &vm_target(id),
                    body["project_id"].as_str(),
                    &params,
                )
                .await?;
                return Ok((status, Json(body)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::VmCreate),
                    None,
                    audit_project.as_deref(),
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match insert_vm(&state, &ctx, &body, &params).await {
        Ok(vm) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    &ctx.identity.user_id,
                    CREATE_VM,
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&vm).unwrap_or(Value::Null),
                );
            }
            state.notify_driver(&vm.id);
            Ok((StatusCode::CREATED, Json(vm)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(&ctx.identity.user_id, CREATE_VM, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::VmCreate),
                None,
                audit_project.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Validates and inserts the VM, with its change-log entry, event and audit
/// record, in one transaction.
async fn insert_vm(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateVmRequest,
    params: &Value,
) -> Result<Vm, ApiError> {
    validate_create(body)?;
    let now = now_rfc3339();
    let vm = Vm {
        id: new_id(Prefix::Vm),
        project_id: body.project_id.clone(),
        app_id: None,
        node_id: body.node_id.clone(),
        name: body.name.clone(),
        vcpus: body.vcpus,
        memory_mb: body.memory_mb,
        disk_gb: body.disk_gb,
        image: body.image.clone(),
        desired_status: DesiredStatus::Running,
        observed_status: ObservedStatus::Creating,
        ip: None,
        restart_generation: 0,
        restart_count: 0,
        last_exit_code: None,
        created_at: now.clone(),
        updated_at: now,
    };

    let mut w = state.store.begin_write().await?;

    let project: Option<(String,)> = sqlx::query_as("SELECT id FROM projects WHERE id = ?")
        .bind(&vm.project_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if project.is_none() {
        return Err(ApiError::validation("project_id does not exist"));
    }
    let node: Option<(String,)> = sqlx::query_as("SELECT status FROM nodes WHERE id = ?")
        .bind(&vm.node_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    match node.as_ref().map(|(status,)| status.as_str()) {
        None => return Err(ApiError::validation("node_id does not exist")),
        Some("removed") => return Err(ApiError::validation("node_id is removed")),
        Some(_) => {}
    }

    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&vm.id)
    .bind(&vm.project_id)
    .bind(&vm.app_id)
    .bind(&vm.node_id)
    .bind(&vm.name)
    .bind(vm.vcpus)
    .bind(vm.memory_mb)
    .bind(vm.disk_gb)
    .bind(&vm.image)
    .bind(vm.desired_status.as_str())
    .bind(ObservedStatus::Creating.as_str())
    .bind(&vm.ip)
    .bind(vm.restart_generation)
    .bind(vm.restart_count)
    .bind(vm.last_exit_code)
    .bind(&vm.created_at)
    .bind(&vm.updated_at)
    .execute(&mut *w.tx)
    .await?;

    let actor = ctx.identity.actor();
    let payload = serde_json::to_value(&vm).map_err(|e| ApiError::internal(&e))?;
    let change_seq =
        changelog::append(&mut w.tx, &actor, "vm", &vm.id, Op::Create, payload).await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "vm.created",
            source: Source::ControlPlane,
            subject_type: "vm",
            subject_id: &vm.id,
            project_id: Some(&vm.project_id),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data: json!({ "vm_id": vm.id, "node_id": vm.node_id }),
        },
    )
    .await?;

    let target = vm_target(&vm.id);
    let record = ctx.record(
        Some(Scope::VmCreate),
        Some(&target),
        Some(&vm.project_id),
        params,
        Outcome::Ok,
        None,
        Some(change_seq),
    );
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(vm)
}

/// `deleteVm`: removes the desired-state row; the node deletes the VM and its
/// disks and reports `vm.deleted` later.
///
/// # Errors
///
/// `forbidden` without `vm:delete`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    delete,
    path = "/api/v1/vms/{vm_id}",
    tag = "vms",
    operation_id = "deleteVm",
    extensions(("x-quoro-scope" = json!("vm:delete"))),
    params(
        ("vm_id" = String, Path, description = "VM id (`vm_...`).",
            pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Deletion accepted; the observed status follows.", body = Vm,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::ApprovalRequired),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn delete_vm(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(vm_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "DELETE /api/v1/vms/{vm_id}",
        &headers,
    );
    let params = json!({ "vm_id": vm_id });
    let target = vm_target(&vm_id);
    ctx.authorize(&state, Scope::VmDelete, Some(&target), None, &params)
        .await?;

    match delete_vm_tx(&state, &ctx, &vm_id, &params).await {
        Ok(vm) => {
            state.notify_driver(&vm.id);
            Ok((StatusCode::ACCEPTED, Json(vm)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::VmDelete),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

async fn delete_vm_tx(
    state: &AppState,
    ctx: &CallCtx,
    vm_id: &str,
    params: &Value,
) -> Result<Vm, ApiError> {
    if !is_id(Prefix::Vm, vm_id) {
        return Err(ApiError::not_found("vm"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {VM_COLUMNS} FROM vms WHERE id = ?"))
        .bind(vm_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    let Some(row) = row else {
        return Err(ApiError::not_found("vm"));
    };
    let mut vm = vm_from_row(&row)?;

    sqlx::query("DELETE FROM vms WHERE id = ?")
        .bind(vm_id)
        .execute(&mut *w.tx)
        .await?;

    let actor = ctx.identity.actor();
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "vm",
        &vm.id,
        Op::Delete,
        // node-agent-protocol.md section 9: `{}` is the payload of a delete.
        json!({}),
    )
    .await?;
    let target = vm_target(&vm.id);
    let record = ctx.record(
        Some(Scope::VmDelete),
        Some(&target),
        Some(&vm.project_id),
        params,
        Outcome::Ok,
        None,
        Some(change_seq),
    );
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;

    // The response is the record as it was, with the desired status the
    // deletion implies.
    vm.desired_status = DesiredStatus::Stopped;
    Ok(vm)
}

/// Sets `desired_status` and records one change-log entry when it changes.
async fn set_desired(
    state: &AppState,
    ctx: &CallCtx,
    vm_id: &str,
    desired: DesiredStatus,
    scope: Scope,
    params: &Value,
) -> Result<Vm, ApiError> {
    if !is_id(Prefix::Vm, vm_id) {
        return Err(ApiError::not_found("vm"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {VM_COLUMNS} FROM vms WHERE id = ?"))
        .bind(vm_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    let Some(row) = row else {
        return Err(ApiError::not_found("vm"));
    };
    let mut vm = vm_from_row(&row)?;
    let target = vm_target(&vm.id);

    if vm.desired_status == desired {
        // No state change, so no change-log entry; the call is still audited
        // as ok and the driver is notified again.
        let record = ctx.record(
            Some(scope),
            Some(&target),
            Some(&vm.project_id),
            params,
            Outcome::Ok,
            None,
            None,
        );
        audit::append(&mut w.tx, &record).await?;
        w.commit().await?;
        return Ok(vm);
    }

    vm.desired_status = desired;
    vm.updated_at = now_rfc3339();
    sqlx::query("UPDATE vms SET desired_status = ?, updated_at = ? WHERE id = ?")
        .bind(desired.as_str())
        .bind(&vm.updated_at)
        .bind(&vm.id)
        .execute(&mut *w.tx)
        .await?;

    let actor = ctx.identity.actor();
    let payload = serde_json::to_value(&vm).map_err(|e| ApiError::internal(&e))?;
    let change_seq =
        changelog::append(&mut w.tx, &actor, "vm", &vm.id, Op::Update, payload).await?;
    let record = ctx.record(
        Some(scope),
        Some(&target),
        Some(&vm.project_id),
        params,
        Outcome::Ok,
        None,
        Some(change_seq),
    );
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(vm)
}

/// `POST /api/v1/vms/{vm_id}/start`.
///
/// # Errors
///
/// `forbidden` without `vm:start`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/vms/{vm_id}/start",
    tag = "vms",
    operation_id = "startVm",
    extensions(("x-quoro-scope" = json!("vm:start"))),
    params(
        ("vm_id" = String, Path, description = "VM id (`vm_...`).",
            pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Start accepted; the desired status is `running`.", body = Vm,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn start_vm(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(vm_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/vms/{vm_id}/start",
        &headers,
    );
    let params = json!({ "vm_id": vm_id });
    let target = vm_target(&vm_id);
    ctx.authorize(&state, Scope::VmStart, Some(&target), None, &params)
        .await?;

    match set_desired(
        &state,
        &ctx,
        &vm_id,
        DesiredStatus::Running,
        Scope::VmStart,
        &params,
    )
    .await
    {
        Ok(vm) => {
            state.notify_driver(&vm.id);
            Ok((StatusCode::ACCEPTED, Json(vm)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::VmStart),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// `POST /api/v1/vms/{vm_id}/stop`.
///
/// # Errors
///
/// `forbidden` without `vm:stop`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/vms/{vm_id}/stop",
    tag = "vms",
    operation_id = "stopVm",
    extensions(("x-quoro-scope" = json!("vm:stop"))),
    params(
        ("vm_id" = String, Path, description = "VM id (`vm_...`).",
            pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Stop accepted; the desired status is `stopped`.", body = Vm,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn stop_vm(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(vm_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/vms/{vm_id}/stop",
        &headers,
    );
    let params = json!({ "vm_id": vm_id });
    let target = vm_target(&vm_id);
    ctx.authorize(&state, Scope::VmStop, Some(&target), None, &params)
        .await?;

    match set_desired(
        &state,
        &ctx,
        &vm_id,
        DesiredStatus::Stopped,
        Scope::VmStop,
        &params,
    )
    .await
    {
        Ok(vm) => {
            state.notify_driver(&vm.id);
            Ok((StatusCode::ACCEPTED, Json(vm)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::VmStop),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}
