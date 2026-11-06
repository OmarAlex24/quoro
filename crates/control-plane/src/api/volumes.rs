// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/volumes` and `/api/v1/volumes*`: the local volumes
//! of stateful apps (ADR 0145).
//!
//! A volume is a sparse file on one node's data disk, attached to the app's
//! microVM and mounted by the guest at `mount_path` (ADR 0145). The row is
//! replicated state, written through the change log like a deployment; the
//! file itself never leaves its node. [`routes`] mounts the four volume
//! operations and [`VolumesDoc`] carries their OpenAPI paths and schemas.

use crate::AppState;
use crate::api::apps::{app_target, idempotency_key};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use quoro_proto::wire::ControlMessage;
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::ToSchema;

/// The audit and idempotency name of `createVolume`.
pub(crate) const CREATE_VOLUME: &str = "POST /api/v1/apps/{app_id}/volumes";

/// Columns of [`Volume`], in the order [`volume_from_row`] expects them.
pub(crate) const VOLUME_COLUMNS: &str = "id, app_id, node_id, size_gb, mount_path, state, \
     allocated_bytes, restored_from_backup_id, error, created_at, updated_at";

/// `volumes.state` (`Volume.state`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolumeState {
    Creating,
    Ready,
    Restoring,
    Failed,
    Detached,
    Deleting,
}

impl VolumeState {
    /// The wire form stored in `volumes.state`.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Restoring => "restoring",
            Self::Failed => "failed",
            Self::Detached => "detached",
            Self::Deleting => "deleting",
        }
    }

    /// Parses `volumes.state`; `None` is an internal inconsistency.
    #[must_use]
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "creating" => Some(Self::Creating),
            "ready" => Some(Self::Ready),
            "restoring" => Some(Self::Restoring),
            "failed" => Some(Self::Failed),
            "detached" => Some(Self::Detached),
            "deleting" => Some(Self::Deleting),
            _ => None,
        }
    }

    /// Whether the volume is attached to the app: the states in which it
    /// counts against the one-volume-per-app rule (`openapi.yaml` `Volume`).
    #[must_use]
    pub(crate) const fn is_attached(self) -> bool {
        matches!(self, Self::Creating | Self::Ready | Self::Restoring)
    }
}

/// Local persistent disk of a stateful app (`openapi.yaml` `Volume`, ADR
/// 0145).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[schema(
    title = "Volume",
    description = "Local persistent disk of a stateful app: a sparse file on one node's data disk, attached to the app's microVM as a virtio-blk device and mounted by the guest init at `mount_path` (ADR 0145)."
)]
pub struct Volume {
    /// Volume id.
    #[schema(pattern = "^vol_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// App that owns the volume.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Node holding the data.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Size in gibibytes.
    #[schema(minimum = 1, maximum = 1024)]
    pub size_gb: i64,
    /// Absolute mount path inside the app.
    pub mount_path: String,
    /// Volume state.
    #[schema(inline)]
    pub state: VolumeState,
    /// Bytes the sparse file uses on the node's disk, as last reported; null
    /// before the first report.
    #[schema(minimum = 0)]
    pub allocated_bytes: Option<i64>,
    /// The backup this volume was restored from, or null for a volume created
    /// empty.
    #[schema(pattern = "^bkp_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub restored_from_backup_id: Option<String>,
    /// One-line reason, in `failed`.
    #[schema(max_length = 2000)]
    pub error: Option<String>,
    /// From M5b: the quarantined copies of this volume, oldest first (ADR
    /// 0176). Always empty until warm volumes ship.
    #[serde(default)]
    #[schema(required = true)]
    pub quarantines: Vec<VolumeQuarantine>,
    /// When the volume was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the volume record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

impl Volume {
    /// The change-log payload of the row. The quarantined copies are rows of
    /// their own (`volume_quarantines`), so they are not part of it.
    ///
    /// # Errors
    ///
    /// An internal error when the row does not serialize.
    pub(crate) fn payload(&self) -> Result<Value, ApiError> {
        let mut payload = serde_json::to_value(self)
            .map_err(|error| ApiError::internal(&format!("volume payload: {error}")))?;
        if let Some(object) = payload.as_object_mut() {
            object.remove("quarantines");
        }
        Ok(payload)
    }
}

/// State of a quarantined copy (`openapi.yaml` `VolumeQuarantine.state`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolumeQuarantineState {
    Kept,
    Deleting,
}

/// A quarantined copy of a volume (`openapi.yaml` `VolumeQuarantine`, ADR
/// 0176): the divergent writes a node kept after a failover.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[schema(
    title = "VolumeQuarantine",
    description = "A quarantined copy of a volume: the live file of a node that lost the app's lease to a failover, kept on that node's disk (ADR 0176)."
)]
pub struct VolumeQuarantine {
    /// Quarantined copy id.
    #[schema(pattern = "^qrn_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// The node whose disk holds the copy.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Bytes the copy uses on that node's disk.
    #[schema(minimum = 0)]
    pub size_bytes: i64,
    /// SHA-256 of the copy's content, as the node computed it.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub sha256: String,
    /// `kept` or `deleting`.
    #[schema(inline)]
    pub state: VolumeQuarantineState,
    /// When the node took the copy.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// The volumes of one app, newest first (`openapi.yaml` `VolumeList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "VolumeList",
    description = "The volumes of one app, newest first."
)]
pub struct VolumeList {
    pub items: Vec<Volume>,
}

/// Body of `createVolume` (`openapi.yaml` `CreateVolumeRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "CreateVolumeRequest",
    description = "Create a local volume for an app (ADR 0145)."
)]
pub struct CreateVolumeRequest {
    /// Size in gibibytes. The file is sparse; the size counts against the
    /// node's disk.
    #[schema(minimum = 1, maximum = 1024)]
    pub size_gb: i64,
    /// Absolute mount path inside the app's root filesystem.
    #[schema(min_length = 2, max_length = 255, pattern = "^/[A-Za-z0-9._/-]+$")]
    pub mount_path: String,
}

/// Builds a [`Volume`] from a `volumes` row.
///
/// # Errors
///
/// An internal error for a row whose state is not one of the contract's.
pub(crate) fn volume_from_row(row: &SqliteRow) -> Result<Volume, ApiError> {
    let state: String = row.try_get("state")?;
    let state = VolumeState::parse(&state)
        .ok_or_else(|| ApiError::internal(&format!("unknown volume state {state:?}")))?;
    Ok(Volume {
        id: row.try_get("id")?,
        app_id: row.try_get("app_id")?,
        node_id: row.try_get("node_id")?,
        size_gb: row.try_get("size_gb")?,
        mount_path: row.try_get("mount_path")?,
        state,
        allocated_bytes: row.try_get("allocated_bytes")?,
        restored_from_backup_id: row.try_get("restored_from_backup_id")?,
        error: row.try_get("error")?,
        quarantines: Vec::new(),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The audit target of a volume operation.
#[must_use]
pub(crate) fn volume_target(volume_id: &str) -> Target {
    Target {
        kind: "volume".to_string(),
        id: volume_id.to_string(),
    }
}

/// The parameters of a `createVolume` call, as the audit log records them.
pub(crate) fn create_params(app_id: &str, body: &CreateVolumeRequest) -> Value {
    let mut params = Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    params.insert("size_gb".to_string(), json!(body.size_gb));
    params.insert("mount_path".to_string(), json!(body.mount_path));
    Value::Object(params)
}

/// The project of a volume's app, or `None` when either does not exist.
pub(crate) async fn volume_project(state: &AppState, volume_id: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>(
        "SELECT a.project_id FROM volumes v JOIN apps a ON a.id = v.app_id WHERE v.id = ?",
    )
    .bind(volume_id)
    .fetch_optional(state.store.pool())
    .await
    .ok()
    .flatten()
    .map(|row| row.0)
}

/// The project of an app, or `None` when it does not exist.
pub(crate) async fn app_project(state: &AppState, app_id: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await
        .ok()
        .flatten()
        .map(|row| row.0)
}

/// Lists the volumes of one app, newest first.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed app
/// id, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/volumes",
    tag = "volumes",
    operation_id = "listAppVolumes",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The app's volumes.", body = VolumeList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_app_volumes(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Json<VolumeList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/volumes",
        &headers,
    );
    let list = crate::service::volumes::list_volumes(&state, &ctx, &app_id).await?;
    Ok(Json(list))
}

/// Creates a local volume for an app, which makes it stateful (ADR 0145).
///
/// # Errors
///
/// `forbidden` without `app:update`, `not_found` for an unknown or malformed
/// app id, `conflict` when the app already has an attached volume or its node
/// is not connected, `validation_failed` for a bad `mount_path` or a size
/// larger than the node's free disk, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/volumes",
    tag = "volumes",
    operation_id = "createVolume",
    extensions(("x-quoro-scope" = json!("app:update"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second volume (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = CreateVolumeRequest, content_type = "application/json",
        description = "Size and mount path."),
    responses(
        (status = 201, description = "The volume was recorded in `creating`; it turns `ready` once its node reports the file.", body = Volume,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_volume(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(body): ApiJson<CreateVolumeRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CREATE_VOLUME, &headers);
    let params = create_params(&app_id, &body);
    let target = app_target(&app_id);
    let project_id = app_project(&state, &app_id).await;
    ctx.authorize(
        &state,
        Scope::AppUpdate,
        Some(&target),
        project_id.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_VOLUME, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let volume: Volume = serde_json::from_value(stored)
                    .map_err(|error| ApiError::internal(&format!("replayed volume: {error}")))?;
                ctx.audit_replay(
                    &state,
                    Scope::AppUpdate,
                    &target,
                    project_id.as_deref(),
                    &params,
                )
                .await?;
                return Ok((status, Json(volume)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::AppUpdate),
                    Some(&target),
                    project_id.as_deref(),
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match crate::service::volumes::create_volume(&state, &ctx, &app_id, &body, &params).await {
        Ok(created) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_VOLUME,
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&created.volume).unwrap_or(Value::Null),
                );
            }
            // The lease reaches the node before the next snapshot, so a
            // stateful VM in it finds the lease held (section 6.4).
            state.hub.send_control(
                &created.node_id,
                &ControlMessage::LeaseGrant(created.lease.grant()),
            );
            state.hub.node_changed(&created.node_id);
            Ok((StatusCode::CREATED, Json(created.volume)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_VOLUME, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::AppUpdate),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Reads one volume.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/volumes/{volume_id}",
    tag = "volumes",
    operation_id = "getVolume",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("volume_id" = String, Path, description = "Volume id (`vol_...`).",
            pattern = "^vol_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The volume.", body = Volume,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_volume(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(volume_id): Path<String>,
) -> Result<Json<Volume>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/volumes/{volume_id}",
        &headers,
    );
    let volume = crate::service::volumes::get_volume(&state, &ctx, &volume_id).await?;
    Ok(Json(volume))
}

/// Deletes a volume and its data. A human with `app:delete` calls it directly;
/// an agent reaches it through an approval grant (permission-model.md
/// section 9), otherwise the call is refused with `approval_required`.
///
/// # Errors
///
/// `forbidden` without `app:delete` or with `approval_required` for an agent,
/// `not_found` for an unknown or malformed id, `conflict` while the app is
/// running, or a database error.
#[utoipa::path(
    delete,
    path = "/api/v1/volumes/{volume_id}",
    tag = "volumes",
    operation_id = "deleteVolume",
    extensions(("x-quoro-scope" = json!("app:delete"))),
    params(
        ("volume_id" = String, Path, description = "Volume id (`vol_...`).",
            pattern = "^vol_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "Deletion accepted; the volume is `deleting` until its node removes the file.", body = Volume,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::ApprovalRequired),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn delete_volume(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(volume_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "DELETE /api/v1/volumes/{volume_id}",
        &headers,
    );
    let params = json!({ "volume_id": volume_id });
    let target = volume_target(&volume_id);
    let project_id = volume_project(&state, &volume_id).await;
    ctx.authorize(
        &state,
        Scope::AppDelete,
        Some(&target),
        project_id.as_deref(),
        &params,
    )
    .await?;

    match crate::service::volumes::delete_volume(&state, &ctx, &volume_id, &params).await {
        Ok(deleted) => {
            // Section 4.1: the revoke reaches the node before the snapshot
            // that drops the volume from `spec.volumes`.
            if let Some(revoke) = &deleted.revoke {
                state.hub.send_control(
                    &deleted.node_id,
                    &ControlMessage::LeaseRevoke(revoke.clone()),
                );
            }
            state.hub.node_changed(&deleted.node_id);
            Ok((StatusCode::ACCEPTED, Json(deleted.volume)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::AppDelete),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Routes of the volume operations.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/apps/{app_id}/volumes",
            get(list_app_volumes).post(create_volume),
        )
        .route(
            "/api/v1/volumes/{volume_id}",
            get(get_volume).delete(delete_volume),
        )
}

/// OpenAPI paths and schemas of the volume operations.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(list_app_volumes, create_volume, get_volume, delete_volume),
    components(
        schemas(Volume, VolumeQuarantine, VolumeList, CreateVolumeRequest),
        responses(
            crate::openapi::Unauthenticated,
            crate::openapi::Forbidden,
            crate::openapi::ApprovalRequired,
            crate::openapi::NotFound,
            crate::openapi::Conflict,
            crate::openapi::ValidationFailed,
            crate::openapi::NotPrimary,
            crate::openapi::RateLimited,
        )
    )
)]
pub struct VolumesDoc;
