//! Just-in-time approvals: `/api/v1/approvals*` (permission-model.md
//! section 9, D-0030).
//!
//! A destructive action is off by default for an agent. The agent files a
//! request naming the scope, the target, the operation and the exact params;
//! a human with `approval:decide` approves or denies it. On approve the
//! system issues one short-lived grant (`kind` `approval`, `max_uses` 1,
//! TTL 10 minutes) scoped to that one target and params hash.

use crate::AppState;
use crate::api::auth::ScopeSchema;
use crate::error::Target;
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

/// The operation string recorded in the audit log and used as the idempotency
/// scope.
pub const CREATE_OPERATION: &str = "POST /api/v1/approvals";

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// A request for just-in-time approval of a destructive action (D-0030).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Approval")]
pub struct Approval {
    /// Approval request id.
    #[schema(pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Agent that filed the request.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: String,
    /// The destructive scope the request asks for.
    #[schema(value_type = ScopeSchema)]
    pub scope: String,
    /// The resource the destructive call addresses.
    #[schema(value_type = Target)]
    pub target: Value,
    /// Endpoint template or MCP tool name the agent intends to call.
    pub operation: String,
    /// The intended call parameters.
    #[schema(value_type = Object)]
    pub params: Value,
    /// SHA-256 hex of the canonical params.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub params_hash: String,
    /// The agent's stated reason.
    pub justification: String,
    /// Incident the request belongs to, if any.
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
    /// Approval status.
    #[schema(inline)]
    pub status: ApprovalStatus,
    /// Expiry of the request; default 1 hour after filing.
    #[schema(format = "date-time")]
    pub expires_at: String,
    /// Human who decided the request.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub decided_by: Option<String>,
    /// When the request was decided.
    pub decided_at: Option<String>,
    /// Note the deciding human left.
    pub decision_note: Option<String>,
    /// Approval grant issued on approve.
    #[schema(pattern = "^grt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub grant_id: Option<String>,
    /// When the request was filed.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// One page of approval requests.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "ApprovalList")]
pub struct ApprovalList {
    pub items: Vec<Approval>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Approval status.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
    Expired,
}

impl ApprovalStatus {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::Expired => "expired",
        }
    }
}

/// File an approval request. Agents only: a human does not need approval for
/// a destructive action (D-0030).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateApprovalRequest")]
pub struct CreateApprovalRequest {
    /// The destructive scope the agent asks for.
    #[schema(value_type = ScopeSchema)]
    pub scope: String,
    /// The resource the destructive call addresses.
    #[schema(value_type = Target)]
    pub target: Value,
    /// Endpoint template or MCP tool name the agent intends to call.
    pub operation: String,
    /// The intended call parameters.
    #[schema(value_type = Object)]
    pub params: Value,
    /// Why the agent needs the exception.
    #[schema(min_length = 1, max_length = 2000)]
    pub justification: String,
    /// Incident the request belongs to, if any.
    #[serde(default)]
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
}

/// Approve or deny a request, optionally with a note.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "DecisionRequest")]
pub struct DecisionRequest {
    /// Note recorded with the decision.
    #[serde(default)]
    #[schema(max_length = 2000, nullable = false)]
    pub note: Option<String>,
}

/// `status` filter of [`ListApprovalsQuery`].
#[derive(Clone, Copy, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalStatusFilter {
    Pending,
    Approved,
    Denied,
    Expired,
}

impl ApprovalStatusFilter {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::Expired => "expired",
        }
    }
}

/// `status` filter plus `limit` and `cursor`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListApprovalsQuery {
    /// Only requests in this status.
    #[param(inline)]
    pub status: Option<ApprovalStatusFilter>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListApprovalsQuery {
    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, crate::error::ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(crate::error::ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    /// The cursor, or the empty string, which sorts before every id.
    #[must_use]
    pub fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// Target of an approval operation: the request itself.
#[must_use]
pub fn approval_target(approval_id: &str) -> Target {
    Target {
        kind: "approval".into(),
        id: approval_id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Requests waiting for a human decision and the ones already decided.
#[utoipa::path(
    get,
    path = "/api/v1/approvals",
    tag = "approvals",
    operation_id = "listApprovals",
    extensions(("x-quoro-scope" = json!("approval:read"))),
    params(ListApprovalsQuery),
    responses(
        (status = 200, description = "One page of approval requests.", body = ApprovalList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `approval:read`, `validation_failed` for a bad filter,
/// or a database error.
pub async fn list_approvals(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(query): Query<ListApprovalsQuery>,
) -> Result<Json<ApprovalList>, crate::error::ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/approvals", &headers);
    let list = crate::service::approvals::list_approvals(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// File an approval request for one destructive call.
#[utoipa::path(
    post,
    path = "/api/v1/approvals",
    tag = "approvals",
    operation_id = "createApproval",
    extensions(
        ("x-quoro-scope" = json!("approval:create")),
        ("x-quoro-mcp-tool" = json!("request_approval")),
    ),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    request_body = CreateApprovalRequest,
    responses(
        (status = 201, description = "The request was filed and is `pending`.", body = Approval,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `validation_failed` for a body the contract refuses, `forbidden` without
/// `approval:create`, `not_found` for an unknown target or incident,
/// `conflict` for a reused idempotency key, or a database error.
pub async fn create_approval(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateApprovalRequest>,
) -> Result<(StatusCode, Json<Approval>), crate::error::ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let approval = crate::service::approvals::create_approval(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(approval)))
}

/// One approval request with its status, decision and grant.
#[utoipa::path(
    get,
    path = "/api/v1/approvals/{approval_id}",
    tag = "approvals",
    operation_id = "getApproval",
    extensions(
        ("x-quoro-scope" = json!("approval:read")),
        ("x-quoro-mcp-tool" = json!("get_approval")),
    ),
    params(
        ("approval_id" = String, Path, description = "Approval request id (`apr_...`).", pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The approval request.", body = Approval,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `approval:read`, `not_found` for an unknown or
/// malformed id, or a database error.
pub async fn get_approval(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(approval_id): Path<String>,
) -> Result<Json<Approval>, crate::error::ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/approvals/{approval_id}",
        &headers,
    );
    let approval = crate::service::approvals::get_approval(&state, &ctx, &approval_id).await?;
    Ok(Json(approval))
}

/// Approve a request and issue its one-use grant.
#[utoipa::path(
    post,
    path = "/api/v1/approvals/{approval_id}/approve",
    tag = "approvals",
    operation_id = "approveApproval",
    extensions(("x-quoro-scope" = json!("approval:decide"))),
    params(
        ("approval_id" = String, Path, description = "Approval request id (`apr_...`).", pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = DecisionRequest,
    responses(
        (status = 200, description = "The approved request with its `grant_id`.", body = Approval,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `approval:decide` (agents get `human_only`),
/// `not_found` for an unknown id, `conflict` when the request is not pending
/// or has expired, `validation_failed` for a note over 2000 characters, or a
/// database error.
pub async fn approve_approval(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(approval_id): Path<String>,
    body: Option<ApiJson<DecisionRequest>>,
) -> Result<Json<Approval>, crate::error::ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "POST /api/v1/approvals/{approval_id}/approve",
        &headers,
    );
    let body = body.map(|ApiJson(body)| body);
    let approval =
        crate::service::approvals::decide(&state, &ctx, &approval_id, body, true).await?;
    Ok(Json(approval))
}

/// Deny a request; no grant is issued.
#[utoipa::path(
    post,
    path = "/api/v1/approvals/{approval_id}/deny",
    tag = "approvals",
    operation_id = "denyApproval",
    extensions(("x-quoro-scope" = json!("approval:decide"))),
    params(
        ("approval_id" = String, Path, description = "Approval request id (`apr_...`).", pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = DecisionRequest,
    responses(
        (status = 200, description = "The denied request; its status is `denied`.", body = Approval,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `approval:decide` (agents get `human_only`),
/// `not_found` for an unknown id, `conflict` when the request is not pending
/// or has expired, `validation_failed` for a note over 2000 characters, or a
/// database error.
pub async fn deny_approval(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(approval_id): Path<String>,
    body: Option<ApiJson<DecisionRequest>>,
) -> Result<Json<Approval>, crate::error::ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "POST /api/v1/approvals/{approval_id}/deny",
        &headers,
    );
    let body = body.map(|ApiJson(body)| body);
    let approval =
        crate::service::approvals::decide(&state, &ctx, &approval_id, body, false).await?;
    Ok(Json(approval))
}
