// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/deployments` and `/api/v1/deployments/{deployment_id}`.
//!
//! A deployment is the rollout record of an image to one app. The desired-state
//! changes and the progress transitions live in [`crate::deploy`]; these
//! handlers read and start deployments.

use crate::api::page::PageQuery;
use crate::error::ApiError;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// Columns of [`Deployment`], in the order [`deployment_from_row`] expects them.
pub(crate) const DEPLOYMENT_COLUMNS: &str = "id, app_id, image, image_digest, status, phase, error, \"trigger\", created_by_type, \
     created_by_id, created_at, finished_at";

/// `deployments.status` (`Deployment.status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatus {
    Pending,
    Preparing,
    Starting,
    Succeeded,
    Failed,
    Superseded,
}

impl DeploymentStatus {
    /// The wire form stored in `deployments.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Preparing => "preparing",
            Self::Starting => "starting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }

    /// Whether the deployment is still making progress.
    #[must_use]
    pub const fn in_progress(self) -> bool {
        matches!(self, Self::Pending | Self::Preparing | Self::Starting)
    }

    /// Parses `deployments.status`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "preparing" => Some(Self::Preparing),
            "starting" => Some(Self::Starting),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }
}

/// `deployments.phase` (`Deployment.phase`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentPhase {
    Pull,
    Rootfs,
    Boot,
    Health,
    Build,
}

impl DeploymentPhase {
    /// The wire form stored in `deployments.phase`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pull => "pull",
            Self::Rootfs => "rootfs",
            Self::Boot => "boot",
            Self::Health => "health",
            Self::Build => "build",
        }
    }

    /// Parses a phase reported by a node.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pull" => Some(Self::Pull),
            "rootfs" => Some(Self::Rootfs),
            "boot" => Some(Self::Boot),
            "health" => Some(Self::Health),
            "build" => Some(Self::Build),
            _ => None,
        }
    }
}

/// `deployments.trigger` (`Deployment.trigger`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentTrigger {
    Api,
    Redeploy,
    Rollback,
    Build,
}

impl DeploymentTrigger {
    /// The wire form stored in `deployments.trigger`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Redeploy => "redeploy",
            Self::Rollback => "rollback",
            Self::Build => "build",
        }
    }
}

/// `deployments.created_by_type` (`Deployment.created_by_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CreatedByType {
    User,
    Agent,
    System,
}

impl CreatedByType {
    /// The wire form stored in `deployments.created_by_type`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::System => "system",
        }
    }

    /// Parses `deployments.created_by_type`; `None` is an internal
    /// inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "agent" => Some(Self::Agent),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

/// One rollout of an image to an app (`openapi.yaml` `Deployment`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "Deployment",
    description = "One rollout of an image to an app. `phase` is the current or failing phase; a build reports `build` (M4)."
)]
pub struct Deployment {
    /// Deployment id.
    #[schema(pattern = "^dep_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// App the deployment rolls out.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Image reference as requested.
    pub image: String,
    /// Digest the reference resolved to, once known.
    #[schema(required = false)]
    pub image_digest: Option<String>,
    /// Deployment status.
    #[schema(inline)]
    pub status: DeploymentStatus,
    /// Current or failing phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(inline, required = false)]
    pub phase: Option<DeploymentPhase>,
    /// Failure message when `status` is `failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    pub error: Option<String>,
    /// What started the deployment.
    #[schema(inline)]
    pub trigger: DeploymentTrigger,
    /// Kind of identity that started the deployment.
    #[schema(inline)]
    pub created_by_type: CreatedByType,
    /// Id of the identity that started the deployment.
    pub created_by_id: String,
    /// When the deployment started.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the deployment reached a terminal status.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    pub finished_at: Option<String>,
}

/// One page of deployments, newest first (`openapi.yaml` `DeploymentList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "DeploymentList",
    description = "One page of deployments, newest first."
)]
pub struct DeploymentList {
    pub items: Vec<Deployment>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// Body of `createDeployment` (`openapi.yaml` `CreateDeploymentRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "CreateDeploymentRequest",
    description = "Roll out a new image to an app."
)]
pub struct CreateDeploymentRequest {
    /// OCI reference to deploy.
    pub image: String,
}

/// Query of `listAppDeployments`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListDeploymentsQuery {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListDeploymentsQuery {
    /// `limit` and `cursor` in the shared page shape.
    #[must_use]
    pub fn page(&self) -> PageQuery {
        PageQuery {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Builds a [`Deployment`] from a `deployments` row.
///
/// # Errors
///
/// An internal error for a status, phase or creator kind outside the contract.
pub(crate) fn deployment_from_row(row: &SqliteRow) -> Result<Deployment, ApiError> {
    let status: String = row.try_get("status")?;
    let status = DeploymentStatus::parse(&status)
        .ok_or_else(|| ApiError::internal(&format!("unknown deployment status {status:?}")))?;
    let phase: Option<String> = row.try_get("phase")?;
    let phase =
        match phase {
            Some(phase) => Some(DeploymentPhase::parse(&phase).ok_or_else(|| {
                ApiError::internal(&format!("unknown deployment phase {phase:?}"))
            })?),
            None => None,
        };
    let created_by: String = row.try_get("created_by_type")?;
    let created_by_type = CreatedByType::parse(&created_by)
        .ok_or_else(|| ApiError::internal(&format!("unknown creator type {created_by:?}")))?;
    let trigger: String = row.try_get("trigger")?;
    let trigger = parse_trigger(&trigger)
        .ok_or_else(|| ApiError::internal(&format!("unknown deployment trigger {trigger:?}")))?;
    Ok(Deployment {
        id: row.try_get("id")?,
        app_id: row.try_get("app_id")?,
        image: row.try_get("image")?,
        image_digest: row.try_get("image_digest")?,
        status,
        phase,
        error: row.try_get("error")?,
        trigger,
        created_by_type,
        created_by_id: row.try_get("created_by_id")?,
        created_at: row.try_get("created_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn parse_trigger(value: &str) -> Option<DeploymentTrigger> {
    match value {
        "api" => Some(DeploymentTrigger::Api),
        "redeploy" => Some(DeploymentTrigger::Redeploy),
        "rollback" => Some(DeploymentTrigger::Rollback),
        "build" => Some(DeploymentTrigger::Build),
        _ => None,
    }
}

use crate::AppState;

use crate::deploy;
use crate::error::Target;
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use quoro_types::Scope;
use serde_json::{Value, json};

/// Deployments of one app, newest first.
///
/// The cursor is the last id of the previous page and means "ids smaller than
/// the cursor", because the page is ordered `id DESC` (ULIDs sort by time).
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed
/// app id, `validation_failed` for a bad page bound, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/deployments",
    tag = "deployments",
    operation_id = "listAppDeployments",
    extensions(
        ("x-quoro-scope" = json!("app:read")),
        ("x-quoro-mcp-tool" = json!("list_deployments")),
    ),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ListDeploymentsQuery,
    ),
    responses(
        (status = 200, description = "One page of deployments.", body = DeploymentList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_app_deployments(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(query): Query<ListDeploymentsQuery>,
) -> Result<Json<DeploymentList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/deployments",
        &headers,
    );
    let list = crate::service::deployments::list_deployments(&state, &ctx, &app_id, &query).await?;
    Ok(Json(list))
}

/// Starts a deployment of a new image. A deployment that is still in progress
/// is superseded.
///
/// # Errors
///
/// `forbidden` without `app:deploy`, `not_found` for an unknown or malformed
/// app id, `conflict` on an idempotency clash, `validation_failed` for a bad
/// body or when no node is available, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/deployments",
    tag = "deployments",
    operation_id = "createDeployment",
    extensions(("x-quoro-scope" = json!("app:deploy"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
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
    request_body(content = CreateDeploymentRequest, content_type = "application/json",
        description = "The image to deploy."),
    responses(
        (status = 202, description = "The deployment was accepted and is now `pending`.", body = Deployment,
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
pub async fn create_deployment(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(body): ApiJson<CreateDeploymentRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/deployments",
        &headers,
    );
    let params = json!({ "app_id": app_id, "image": body.image });
    ctx.authorize(&state, Scope::AppDeploy, None, None, &params)
        .await?;
    let key = crate::api::apps::idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state.idempotency.begin(
            ctx.identity.id(),
            "POST /api/v1/apps/{app_id}/deployments",
            key,
            &params,
        ) {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let deployment: Deployment = serde_json::from_value(stored).map_err(|error| {
                    ApiError::internal(&format!("replayed deployment: {error}"))
                })?;
                let target = Target {
                    kind: "deployment".to_string(),
                    id: deployment.id.clone(),
                };
                ctx.audit_replay(&state, Scope::AppDeploy, &target, None, &params)
                    .await?;
                return Ok((status, Json(deployment)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(&state, Some(Scope::AppDeploy), None, None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    match create_deployment_tx(&state, &ctx, &app_id, &body.image, &params).await {
        Ok(created) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    "POST /api/v1/apps/{app_id}/deployments",
                    key,
                    StatusCode::ACCEPTED,
                    serde_json::to_value(&created.deployment).unwrap_or(Value::Null),
                );
            }
            for vm_id in std::iter::once(&created.vm_id).chain(created.retired_vm_ids.iter()) {
                state.notify_driver(vm_id);
            }
            Ok((StatusCode::ACCEPTED, Json(created.deployment)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state.idempotency.abandon(
                    ctx.identity.id(),
                    "POST /api/v1/apps/{app_id}/deployments",
                    key,
                );
            }
            ctx.audit_error(&state, Some(Scope::AppDeploy), None, None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn create_deployment_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    image: &str,
    params: &Value,
) -> Result<deploy::CreatedDeployment, ApiError> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let source: Option<(Option<String>,)> = sqlx::query_as("SELECT source FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    let Some((source,)) = source else {
        return Err(ApiError::not_found("app"));
    };
    // An app with a Git source deploys the image its build produced, with
    // `trigger: build`; an image given here would not be built (ADR 0117).
    if source.is_some() {
        return Err(ApiError::conflict(
            "the app builds its image from a Git source; start a build instead",
        ));
    }
    let created = deploy::create_deployment(
        &mut w.tx,
        app_id,
        image,
        DeploymentTrigger::Api,
        &ctx.identity.actor(),
    )
    .await?;
    let target = Target {
        kind: "deployment".to_string(),
        id: created.deployment.id.clone(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDeploy),
        Some(&target),
        Some(&created.project_id),
        params,
        Some(created.change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(created)
}

/// One deployment with its status, phase, resolved digest and timestamps.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/deployments/{deployment_id}",
    tag = "deployments",
    operation_id = "getDeployment",
    extensions(
        ("x-quoro-scope" = json!("app:read")),
        ("x-quoro-mcp-tool" = json!("get_deployment")),
    ),
    params(
        ("deployment_id" = String, Path, description = "Deployment id (`dep_...`).",
            pattern = "^dep_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The deployment.", body = Deployment,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_deployment(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(deployment_id): Path<String>,
) -> Result<Json<Deployment>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/deployments/{deployment_id}",
        &headers,
    );
    let deployment =
        crate::service::deployments::get_deployment(&state, &ctx, &deployment_id).await?;
    Ok(Json(deployment))
}
