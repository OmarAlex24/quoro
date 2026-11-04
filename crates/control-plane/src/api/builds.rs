// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/builds` and `/api/v1/builds*`: the builds of an
//! app's Git source, their records and their logs (ADR 0117, ADR 0118,
//! ADR 0119).
//!
//! A build row is replicated state, written through the change log like a
//! deployment; its log stays on the node that ran it and is read on demand
//! (ADR 0014). [`routes`] mounts the six build operations and [`BuildsDoc`]
//! carries their OpenAPI paths and schemas.

use crate::AppState;
use crate::api::apps::{AppSource, app_target, idempotency_key};
use crate::api::deployments::CreatedByType;
use crate::api::logs::{LogStreamName, NODE_TIMEOUT, hub_error};
use crate::api::page::PageQuery;
use crate::build::{self, BUILD_COLUMNS};
use crate::deploy;
use crate::error::{ApiError, Problem, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::session::{HubError, NodeStream, StreamEvent};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::stream::{self, Stream};
use quoro_proto::wire;
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;
use utoipa::{IntoParams, OpenApi, ToSchema};

/// How often the live build stream looks at the build's row.
const STATUS_POLL: Duration = Duration::from_secs(1);

/// How often the SSE response keeps a quiet connection alive.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// Routes of the build operations.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/apps/{app_id}/builds",
            get(list_app_builds).post(start_build),
        )
        .route("/api/v1/builds/{build_id}", get(get_build))
        .route("/api/v1/builds/{build_id}/cancel", post(cancel_build))
        .route("/api/v1/builds/{build_id}/logs", get(get_build_logs))
        .route(
            "/api/v1/builds/{build_id}/logs/stream",
            get(stream_build_logs),
        )
}

// ---------------------------------------------------------------------------
// Response shapes (`openapi.yaml` `Build`, `BuildList`, ...)
// ---------------------------------------------------------------------------

/// `builds.status` (`Build.status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    Queued,
    Preparing,
    Building,
    Exporting,
    Importing,
    Succeeded,
    Failed,
    Cancelled,
}

impl BuildStatus {
    /// The wire form stored in `builds.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Preparing => "preparing",
            Self::Building => "building",
            Self::Exporting => "exporting",
            Self::Importing => "importing",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the build is finished: `succeeded`, `failed` or `cancelled`.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Parses `builds.status`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "preparing" => Some(Self::Preparing),
            "building" => Some(Self::Building),
            "exporting" => Some(Self::Exporting),
            "importing" => Some(Self::Importing),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// The phase a build is in, for a failed or cancelled build.
    #[must_use]
    pub fn phase(self) -> Option<BuildPhase> {
        match self {
            Self::Preparing => Some(BuildPhase::Preparing),
            Self::Building => Some(BuildPhase::Building),
            Self::Exporting => Some(BuildPhase::Exporting),
            Self::Importing => Some(BuildPhase::Importing),
            Self::Queued | Self::Succeeded | Self::Failed | Self::Cancelled => None,
        }
    }
}

/// `builds.failed_phase` (`Build.failed_phase`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BuildPhase {
    Preparing,
    Building,
    Exporting,
    Importing,
}

impl BuildPhase {
    /// The wire form stored in `builds.failed_phase`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Building => "building",
            Self::Exporting => "exporting",
            Self::Importing => "importing",
        }
    }

    /// Parses a phase a node reported.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "preparing" => Some(Self::Preparing),
            "building" => Some(Self::Building),
            "exporting" => Some(Self::Exporting),
            "importing" => Some(Self::Importing),
            _ => None,
        }
    }
}

impl From<wire::BuildPhase> for BuildPhase {
    fn from(value: wire::BuildPhase) -> Self {
        match value {
            wire::BuildPhase::Preparing => Self::Preparing,
            wire::BuildPhase::Building => Self::Building,
            wire::BuildPhase::Exporting => Self::Exporting,
            wire::BuildPhase::Importing => Self::Importing,
        }
    }
}

/// The BuildKit step a build failed in (`openapi.yaml` `BuildFailedStep`).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "BuildFailedStep",
    description = "The BuildKit step that failed, with a few lines of its log and the Dockerfile excerpt."
)]
pub struct BuildFailedStep {
    /// BuildKit's step number.
    #[schema(minimum = 0)]
    pub vertex: i64,
    /// The step's name, for example `[3/4] RUN npm ci`.
    pub name: String,
    /// BuildKit's error for the step.
    pub error: String,
    /// Line of the Dockerfile the step comes from, when BuildKit names it.
    #[serde(default)]
    #[schema(minimum = 1, required = true)]
    pub dockerfile_line: Option<i64>,
    /// The step's last log lines and the Dockerfile excerpt.
    #[schema(max_items = 20)]
    pub excerpt: Vec<String>,
}

/// What started a build (`Build.trigger`). Only an explicit start in M4.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BuildTrigger {
    Api,
}

impl BuildTrigger {
    /// The wire form stored in `builds.trigger`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
        }
    }

    /// Parses `builds.trigger`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "api" => Some(Self::Api),
            _ => None,
        }
    }
}

/// One image build of an app's Git source (`openapi.yaml` `Build`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "Build",
    description = "One image build of an app's Git source, run on the node that runs the app in an ephemeral builder microVM (ADR 0117, ADR 0118)."
)]
pub struct Build {
    /// Build id.
    #[schema(pattern = "^bld_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// App the build belongs to.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Node that runs the build and keeps its log.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Snapshot of the source the build started from.
    pub source: AppSource,
    /// Build status.
    #[schema(inline)]
    pub status: BuildStatus,
    /// Commit the ref resolved to, once the checkout finished.
    #[serde(default)]
    #[schema(pattern = "^[0-9a-f]{40}$", required = true)]
    pub commit: Option<String>,
    /// The built image's reference, on success.
    #[serde(default)]
    #[schema(required = true)]
    pub image: Option<String>,
    /// Manifest digest of the built image, on success.
    #[serde(default)]
    #[schema(required = true)]
    pub image_digest: Option<String>,
    /// Phase the build failed in; null unless `status` is `failed`.
    #[serde(default)]
    #[schema(inline, required = true)]
    pub failed_phase: Option<BuildPhase>,
    /// One-line failure reason when `status` is `failed`.
    #[serde(default)]
    #[schema(required = true)]
    pub error: Option<String>,
    /// The failing step, when the build failed in a build step.
    #[serde(default)]
    #[schema(required = true)]
    pub failed_step: Option<BuildFailedStep>,
    /// The deployment the build started, on success.
    #[serde(default)]
    #[schema(pattern = "^dep_[0-9A-HJKMNP-TV-Z]{26}$", required = true)]
    pub deployment_id: Option<String>,
    /// What started the build.
    #[schema(inline)]
    pub trigger: BuildTrigger,
    /// Kind of identity that started the build.
    #[schema(inline)]
    pub created_by_type: CreatedByType,
    /// Id of the identity that started the build.
    pub created_by_id: String,
    /// When the build was queued.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the node started it.
    #[serde(default)]
    #[schema(format = "date-time", required = true)]
    pub started_at: Option<String>,
    /// When it reached a terminal status.
    #[serde(default)]
    #[schema(format = "date-time", required = true)]
    pub finished_at: Option<String>,
}

/// One page of builds, newest first (`openapi.yaml` `BuildList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(title = "BuildList", description = "One page of builds, newest first.")]
pub struct BuildList {
    pub items: Vec<Build>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// Body of `startBuild` (`openapi.yaml` `StartBuildRequest`).
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "StartBuildRequest",
    description = "Start a build of the app's Git source. Every field is optional."
)]
pub struct StartBuildRequest {
    /// Build this branch, tag or commit instead of the source's `ref`.
    #[serde(rename = "ref", default)]
    #[schema(value_type = String, min_length = 1, max_length = 255, required = false)]
    pub git_ref: Option<String>,
}

/// One line of a build's log (`openapi.yaml` `BuildLogLine`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "BuildLogLine",
    description = "One line of a build's log, as kept on the node that ran it (ADR 0119)."
)]
pub struct BuildLogLine {
    /// Position in the build's log, gap-free from 1.
    #[schema(minimum = 1)]
    pub seq: i64,
    /// When the node recorded the line.
    #[schema(format = "date-time")]
    pub time: String,
    /// Which stream the line came from.
    pub stream: LogStreamName,
    /// BuildKit step the line belongs to, when it names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 0, required = false)]
    pub vertex: Option<i64>,
    /// The line, without its trailing newline.
    pub line: String,
}

/// One page of a build's log, oldest first (`openapi.yaml` `BuildLogList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "BuildLogList",
    description = "One page of a build's log, oldest first."
)]
pub struct BuildLogList {
    pub items: Vec<BuildLogLine>,
    /// Cursor for the next page, or null when no more lines exist yet.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// Query of `listAppBuilds`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListBuildsQuery {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListBuildsQuery {
    /// `limit` and `cursor` in the shared page shape.
    #[must_use]
    pub fn page(&self) -> PageQuery {
        PageQuery {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Query of `getBuildLogs`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct BuildLogsQuery {
    /// Maximum number of lines to return.
    #[param(minimum = 1, maximum = 1000, default = 500)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

/// Query of `streamBuildLogs`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct BuildLogsStreamQuery {
    /// Start with the last `tail` lines instead of the whole log.
    #[param(minimum = 0, maximum = 5000)]
    pub tail: Option<i64>,
}

// ---------------------------------------------------------------------------
// The audit and idempotency names of the operations
// ---------------------------------------------------------------------------

const LIST_BUILDS: &str = "GET /api/v1/apps/{app_id}/builds";
const START_BUILD: &str = "POST /api/v1/apps/{app_id}/builds";
const GET_BUILD: &str = "GET /api/v1/builds/{build_id}";
const CANCEL_BUILD: &str = "POST /api/v1/builds/{build_id}/cancel";
const GET_BUILD_LOGS: &str = "GET /api/v1/builds/{build_id}/logs";
const STREAM_BUILD_LOGS: &str = "GET /api/v1/builds/{build_id}/logs/stream";

/// The audit target of a build operation.
fn build_target(build_id: &str) -> Target {
    Target {
        kind: "build".to_string(),
        id: build_id.to_string(),
    }
}

/// The project of an app, or `None` when the app does not exist.
async fn project_of_app(state: &AppState, app_id: &str) -> Option<String> {
    if !deploy::is_id_with_prefix("app", app_id) {
        return None;
    }
    sqlx::query_as::<_, (String,)>("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await
        .ok()
        .flatten()
        .map(|row| row.0)
}

/// The parameters of a build operation, as the audit log records them.
fn start_params(app_id: &str, git_ref: Option<&str>) -> Value {
    let mut params = Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(git_ref) = git_ref {
        params.insert("ref".to_string(), json!(git_ref));
    }
    Value::Object(params)
}

// ---------------------------------------------------------------------------
// listAppBuilds
// ---------------------------------------------------------------------------

/// Builds of one app, newest first.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed
/// app id, `validation_failed` for a bad page bound, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/builds",
    tag = "builds",
    operation_id = "listAppBuilds",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ListBuildsQuery,
    ),
    responses(
        (status = 200, description = "One page of builds.", body = BuildList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_app_builds(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(query): Query<ListBuildsQuery>,
) -> Result<Json<BuildList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, LIST_BUILDS, &headers);
    let list = crate::service::builds::list_builds(&state, &ctx, &app_id, &query).await?;
    Ok(Json(list))
}

// ---------------------------------------------------------------------------
// startBuild
// ---------------------------------------------------------------------------

/// Queues a build of the app's Git source on the node that runs the app, or
/// on the node the scheduler picks.
///
/// # Errors
///
/// `forbidden` without `app:deploy`, `not_found` for an unknown or malformed
/// app id, `conflict` when the app has no Git source or a build that is not
/// terminal, `validation_failed` for a bad body or when no node is available,
/// or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/builds",
    tag = "builds",
    operation_id = "startBuild",
    extensions(("x-quoro-scope" = json!("app:deploy"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of starting a second build (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body = StartBuildRequest,
    responses(
        (status = 202, description = "The build was accepted and is `queued`.", body = Build,
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
pub async fn start_build(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    body: Option<ApiJson<StartBuildRequest>>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, START_BUILD, &headers);
    let git_ref = body.and_then(|ApiJson(body)| body.git_ref);
    let params = start_params(&app_id, git_ref.as_deref());
    let target = app_target(&app_id);
    let project_id = project_of_app(&state, &app_id).await;
    ctx.authorize(
        &state,
        Scope::AppDeploy,
        Some(&target),
        project_id.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), START_BUILD, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let build: Build = serde_json::from_value(stored)
                    .map_err(|error| ApiError::internal(&format!("replayed build: {error}")))?;
                ctx.audit_replay(
                    &state,
                    Scope::AppDeploy,
                    &target,
                    project_id.as_deref(),
                    &params,
                )
                .await?;
                return Ok((status, Json(build)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::AppDeploy),
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

    match start_build_tx(&state, &ctx, &app_id, git_ref.as_deref(), &params).await {
        Ok((build, _change_seq)) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    START_BUILD,
                    key,
                    StatusCode::ACCEPTED,
                    serde_json::to_value(&build).unwrap_or(Value::Null),
                );
            }
            if !build.status.is_terminal() {
                state.hub.node_changed(&build.node_id);
            }
            Ok((StatusCode::ACCEPTED, Json(build)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), START_BUILD, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::AppDeploy),
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

/// Starts the build, its change-log entry, its events and the audit record in
/// one transaction.
async fn start_build_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    git_ref: Option<&str>,
    params: &Value,
) -> Result<(Build, i64), ApiError> {
    let mut w = state.store.begin_write().await?;
    let started = build::start_build(&mut w.tx, app_id, git_ref, &ctx.identity.actor()).await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDeploy),
        Some(&app_target(app_id)),
        Some(&started.project_id),
        params,
        Some(started.change_seq),
    )
    .await?;
    w.commit().await?;
    Ok((started.build, started.change_seq))
}

// ---------------------------------------------------------------------------
// getBuild
// ---------------------------------------------------------------------------

/// One build with its status, resolved commit, failing step and the
/// deployment it started.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/builds/{build_id}",
    tag = "builds",
    operation_id = "getBuild",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("build_id" = String, Path, description = "Build id (`bld_...`).",
            pattern = "^bld_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The build.", body = Build,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_build(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(build_id): Path<String>,
) -> Result<Json<Build>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, GET_BUILD, &headers);
    let build = crate::service::builds::get_build(&state, &ctx, &build_id).await?;
    Ok(Json(build))
}

// ---------------------------------------------------------------------------
// cancelBuild
// ---------------------------------------------------------------------------

/// Asks the node to stop a build; the answer is the build as recorded.
///
/// # Errors
///
/// `forbidden` without `app:deploy`, `not_found` for an unknown or malformed
/// id, `conflict` when the build already finished, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/builds/{build_id}/cancel",
    tag = "builds",
    operation_id = "cancelBuild",
    extensions(("x-quoro-scope" = json!("app:deploy"))),
    params(
        ("build_id" = String, Path, description = "Build id (`bld_...`).",
            pattern = "^bld_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "The cancel was recorded and sent to the node.", body = Build,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn cancel_build(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(build_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CANCEL_BUILD, &headers);
    let params = json!({ "build_id": build_id });
    let target = build_target(&build_id);
    ctx.authorize(&state, Scope::AppDeploy, Some(&target), None, &params)
        .await?;

    match cancel_build_tx(&state, &ctx, &build_id, &params).await {
        Ok((build, project_id, notify)) => {
            if notify {
                state.hub.node_changed(&build.node_id);
            }
            let _ = project_id;
            Ok((StatusCode::ACCEPTED, Json(build)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::AppDeploy),
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

async fn cancel_build_tx(
    state: &AppState,
    ctx: &CallCtx,
    build_id: &str,
    params: &Value,
) -> Result<(Build, String, bool), ApiError> {
    let mut w = state.store.begin_write().await?;
    let cancelled = build::cancel_build(&mut w.tx, build_id, &ctx.identity.actor()).await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppDeploy),
        Some(&build_target(build_id)),
        Some(&cancelled.project_id),
        params,
        cancelled.change_seq,
    )
    .await?;
    w.commit().await?;
    Ok((cancelled.build, cancelled.project_id, cancelled.notify))
}

// ---------------------------------------------------------------------------
// getBuildLogs
// ---------------------------------------------------------------------------

/// One page of a build's log, oldest first, read from the node that ran it.
///
/// # Errors
///
/// `forbidden` without `app:logs`, `not_found` for an unknown build or a node
/// that no longer keeps the log, `validation_failed` for a bad page bound or
/// cursor, `node_unreachable` when the node does not answer, or a database
/// error.
#[utoipa::path(
    get,
    path = "/api/v1/builds/{build_id}/logs",
    tag = "builds",
    operation_id = "getBuildLogs",
    extensions(("x-quoro-scope" = json!("app:logs"))),
    params(
        ("build_id" = String, Path, description = "Build id (`bld_...`).",
            pattern = "^bld_[0-9A-HJKMNP-TV-Z]{26}$"),
        BuildLogsQuery,
    ),
    responses(
        (status = 200, description = "One page of log lines.", body = BuildLogList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn get_build_logs(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(build_id): Path<String>,
    Query(query): Query<BuildLogsQuery>,
) -> Result<Json<BuildLogList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, GET_BUILD_LOGS, &headers);
    let mut params = Map::new();
    params.insert("build_id".to_string(), json!(build_id));
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    let target = build_target(&build_id);
    let mut access = ctx
        .authorize_read(
            &state,
            Scope::AppLogs,
            Some(&target),
            &Value::Object(params),
        )
        .await?;
    let result: Result<BuildLogList, ApiError> = async {
        let (build, project_id) = require_build(&state, &build_id).await?;
        access.set_project(Some(&project_id));
        let limit = match query.limit {
            None => 500,
            Some(limit) if (1..=1000).contains(&limit) => limit,
            Some(limit) => {
                return Err(ApiError::validation(format!(
                    "limit {limit} is outside 1-1000"
                )));
            }
        };
        let after_seq = match &query.cursor {
            None => 0,
            Some(cursor) => cursor.trim().parse::<i64>().map_err(|_| {
                ApiError::validation("cursor is not a position in this build's log")
            })?,
        };
        if !state.hub.is_connected(&build.node_id) {
            return Err(hub_error(HubError::NotConnected));
        }
        let answer = state
            .hub
            .command(
                &build.node_id,
                "build_logs_query",
                json!({ "build_id": build_id, "after_seq": after_seq, "limit": limit }),
                NODE_TIMEOUT,
            )
            .await
            .map_err(hub_error)?;
        if answer.status == quoro_proto::CommandStatus::Error {
            if command_is_not_found(&answer) {
                return Err(ApiError::not_found("build log"));
            }
            return Err(crate::api::logs::command_error(answer));
        }
        let items: Vec<BuildLogLine> = answer
            .lines
            .unwrap_or_default()
            .iter()
            .filter_map(log_line)
            .collect();
        let next_cursor = match items.last() {
            Some(last) if items.len() == usize::try_from(limit).unwrap_or(0) => {
                Some(last.seq.to_string())
            }
            Some(last) if !build.status.is_terminal() => Some(last.seq.to_string()),
            Some(_) => None,
            None if !build.status.is_terminal() => Some(after_seq.to_string()),
            None => None,
        };
        Ok(BuildLogList { items, next_cursor })
    }
    .await;
    ctx.finish_read(&state, access, result).await.map(Json)
}

/// The build and its app's project, or `not_found`.
async fn require_build(state: &AppState, build_id: &str) -> Result<(Build, String), ApiError> {
    if !deploy::is_id_with_prefix("bld", build_id) {
        return Err(ApiError::not_found("build"));
    }
    let row = sqlx::query(&format!("SELECT {BUILD_COLUMNS} FROM builds WHERE id = ?"))
        .bind(build_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("build"))?;
    let build = crate::build::build_from_row(&row)?;
    let project_id: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(&build.app_id)
        .fetch_optional(state.store.pool())
        .await?;
    let Some((project_id,)) = project_id else {
        return Err(ApiError::not_found("app"));
    };
    Ok((build, project_id))
}

/// One line of a build's log, or `None` when the node did not number it.
fn log_line(line: &wire::LogLine) -> Option<BuildLogLine> {
    Some(BuildLogLine {
        seq: i64::try_from(line.seq?).ok().filter(|seq| *seq >= 1)?,
        time: line.time.clone(),
        stream: line.stream.into(),
        vertex: line.vertex.map(i64::from),
        line: line.line.clone(),
    })
}

/// Whether a failed `command_result` means the node does not have the build
/// (section 5.18, `not_found`).
fn command_is_not_found(answer: &quoro_proto::CommandResult) -> bool {
    answer
        .error
        .as_ref()
        .is_some_and(|error| error.code == quoro_proto::ErrorCode::NotFound)
}

// ---------------------------------------------------------------------------
// streamBuildLogs
// ---------------------------------------------------------------------------

/// Server-sent events of a build's log, read from the node on demand.
///
/// # Errors
///
/// `forbidden` without `app:logs`, `not_found` for an unknown build,
/// `validation_failed` for a bad `tail` or `Last-Event-ID`,
/// `node_unreachable` when the node has no live session, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/builds/{build_id}/logs/stream",
    tag = "builds",
    operation_id = "streamBuildLogs",
    extensions(("x-quoro-scope" = json!("app:logs"))),
    params(
        ("build_id" = String, Path, description = "Build id (`bld_...`).",
            pattern = "^bld_[0-9A-HJKMNP-TV-Z]{26}$"),
        BuildLogsStreamQuery,
        ("Last-Event-ID" = Option<String>, Header,
            description = "Last line `seq` the client received; the stream resumes after it.",
            nullable = false),
    ),
    responses(
        (status = 200, description = "The stream started; events follow until the build is terminal or the client disconnects.",
            content_type = "text/event-stream", body = String,
            headers(
                ("Quoro-Request-Id" = String, description = "Correlation id of this call."),
                ("Cache-Control" = String, description = "Always `no-cache`; the stream must not be cached."),
            )),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn stream_build_logs(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(build_id): Path<String>,
    Query(query): Query<BuildLogsStreamQuery>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, STREAM_BUILD_LOGS, &headers);
    let mut params = Map::new();
    params.insert("build_id".to_string(), json!(build_id));
    if let Some(tail) = query.tail {
        params.insert("tail".to_string(), json!(tail));
    }
    let target = build_target(&build_id);
    let mut access = ctx
        .authorize_read(
            &state,
            Scope::AppLogs,
            Some(&target),
            &Value::Object(params),
        )
        .await?;
    let result: Result<Response, ApiError> = async {
        let (build, project_id) = require_build(&state, &build_id).await?;
        access.set_project(Some(&project_id));
        let mut stream_params = Map::new();
        stream_params.insert("build_id".to_string(), json!(build_id));
        match headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
            Some(last) => {
                let after_seq = last
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| ApiError::validation("Last-Event-ID is not a line number"))?;
                stream_params.insert("after_seq".to_string(), json!(after_seq));
            }
            None => {
                let tail = match query.tail {
                    None => 0,
                    Some(tail) if (0..=5000).contains(&tail) => tail,
                    Some(tail) => {
                        return Err(ApiError::validation(format!(
                            "tail {tail} is outside 0-5000"
                        )));
                    }
                };
                stream_params.insert("tail".to_string(), json!(tail));
            }
        }
        if !state.hub.is_connected(&build.node_id) {
            return Err(hub_error(HubError::NotConnected));
        }
        let stream = state
            .hub
            .open_stream(&build.node_id, "build_logs", Value::Object(stream_params))
            .map_err(hub_error)?;
        let feed = BuildFeed {
            state: state.clone(),
            build_id: build.id.clone(),
            stream,
            status: build.status,
            ticker: tokio::time::interval(STATUS_POLL),
            queue: VecDeque::new(),
            done: false,
        };
        Ok(sse(stream::unfold(feed, |mut feed| async move {
            feed.next_event()
                .await
                .map(|event| (Ok::<_, Infallible>(event), feed))
        })))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// The live build log: the node's lines, the build's status changes and the
/// end of the stream.
struct BuildFeed {
    state: AppState,
    build_id: String,
    stream: NodeStream,
    status: BuildStatus,
    ticker: tokio::time::Interval,
    queue: VecDeque<Event>,
    done: bool,
}

impl BuildFeed {
    /// The next SSE frame, or `None` once the stream ended.
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some(event) = self.queue.pop_front() {
                return Some(event);
            }
            if self.done {
                return None;
            }
            tokio::select! {
                event = self.stream.next() => match event {
                    None => {
                        self.done = true;
                        self.queue.push_back(end_event());
                    }
                    Some(StreamEvent::Lines(lines)) => {
                        for line in &lines {
                            if let Some(line) = log_line(line) {
                                self.queue.push_back(line_event(&line));
                            }
                        }
                    }
                    // A build-log stream never carries samples or terminal
                    // bytes.
                    Some(
                        StreamEvent::Metrics(_)
                        | StreamEvent::Data(_)
                        | StreamEvent::SessionEnd { .. },
                    ) => {}
                    Some(StreamEvent::End { reason, message }) => {
                        self.refresh_status().await;
                        self.done = true;
                        if reason == "completed" {
                            self.queue.push_back(end_event());
                        } else if reason == "error" {
                            self.queue.push_back(error_event(&message));
                        } else {
                            self.queue.push_back(end_event());
                        }
                    }
                },
                _ = self.ticker.tick() => {
                    self.refresh_status().await;
                }
            }
        }
    }

    /// Reads the build's row and queues a `status` frame when the status
    /// changed.
    async fn refresh_status(&mut self) {
        let row = sqlx::query(&format!("SELECT {BUILD_COLUMNS} FROM builds WHERE id = ?"))
            .bind(&self.build_id)
            .fetch_optional(self.state.store.pool())
            .await;
        let Ok(Some(row)) = row else {
            return;
        };
        let Ok(build) = crate::build::build_from_row(&row) else {
            return;
        };
        if build.status == self.status {
            return;
        }
        self.status = build.status;
        self.queue.push_back(status_event(&build));
    }
}

/// One `log` frame with `id` set to the line's `seq`.
fn line_event(line: &BuildLogLine) -> Event {
    let data = serde_json::to_string(line).unwrap_or_default();
    Event::default()
        .id(line.seq.to_string())
        .event("log")
        .data(data)
}

/// One `status` frame carrying the build.
fn status_event(build: &Build) -> Event {
    let data = serde_json::to_string(build).unwrap_or_default();
    Event::default().event("status").data(data)
}

/// The `end` frame, sent once the node ended the stream `completed`.
fn end_event() -> Event {
    Event::default().event("end").data("{}")
}

/// An `error` frame carrying a `node_unreachable` `Problem`.
fn error_event(message: &str) -> Event {
    let problem = Problem {
        kind: "about:blank".to_string(),
        title: "Node unreachable".to_string(),
        status: 504,
        detail: Some(if message.is_empty() {
            "the node reported an error".to_string()
        } else {
            message.to_string()
        }),
        instance: None,
        code: crate::error::Code::NodeUnreachable,
        reason: None,
        scope: None,
        target: None,
    };
    let data = serde_json::to_string(&problem).unwrap_or_default();
    Event::default().event("error").data(data)
}

/// The SSE response with `Cache-Control: no-cache` and a keep-alive. The
/// `Quoro-Request-Id` header is added by the request-id middleware.
fn sse<S>(body: S) -> Response
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    let mut response = Sse::new(body)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

// ---------------------------------------------------------------------------
// OpenAPI document
// ---------------------------------------------------------------------------

/// OpenAPI paths and schemas of the build operations.
#[derive(OpenApi)]
#[openapi(
    paths(
        list_app_builds,
        start_build,
        get_build,
        cancel_build,
        get_build_logs,
        stream_build_logs,
    ),
    components(
        schemas(
            Build,
            BuildList,
            BuildStatus,
            BuildPhase,
            BuildFailedStep,
            BuildTrigger,
            BuildLogLine,
            BuildLogList,
            StartBuildRequest,
        ),
        responses(
            crate::openapi::Unauthenticated,
            crate::openapi::Forbidden,
            crate::openapi::NotFound,
            crate::openapi::Conflict,
            crate::openapi::ValidationFailed,
            crate::openapi::NotPrimary,
            crate::openapi::RateLimited,
            crate::api::logs::NodeUnreachable,
        )
    )
)]
pub struct BuildsDoc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_log_line_without_a_position_is_dropped() {
        let line = wire::LogLine {
            time: "2026-11-02T10:00:00Z".to_string(),
            stream: wire::LogStream::Stdout,
            line: "hello".to_string(),
            vm_id: None,
            seq: None,
            vertex: None,
        };
        assert!(log_line(&line).is_none());
        let mut numbered = line.clone();
        numbered.seq = Some(3);
        numbered.vertex = Some(6);
        let numbered = log_line(&numbered).expect("numbered");
        assert_eq!(numbered.seq, 3);
        assert_eq!(numbered.vertex, Some(6));
    }

    #[test]
    fn a_not_found_answer_is_read_from_its_code() {
        let answer = |code| quoro_proto::CommandResult {
            status: quoro_proto::CommandStatus::Error,
            detail: None,
            error: Some(quoro_proto::ErrorBody {
                code,
                message: "this node has no such build".to_string(),
            }),
            lines: None,
            metrics: None,
        };
        assert!(command_is_not_found(&answer(
            quoro_proto::ErrorCode::NotFound
        )));
        assert!(!command_is_not_found(&answer(
            quoro_proto::ErrorCode::BadMessage
        )));
    }
}
