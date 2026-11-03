//! Grants: `/api/v1/grants*` (permission-model.md section 6, D-0030).
//!
//! A grant is the only carrier of agent authority: a scope set, a resource
//! selector, a mandatory expiry and an optional maximum number of uses.
//! `grant:write` is human-only (section 4.2), so creating and revoking a grant
//! authorizes through [`CallCtx::authorize_human`]; reading one uses
//! [`CallCtx::authorize_read`] and audits an agent's read.

use crate::AppState;
use crate::api::auth::{
    GrantCreator, GrantKind, GrantStatus, ScopeSchema, Selector as SelectorSchema,
};
use crate::authz;
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

/// The operation string recorded in the audit log and used as the idempotency
/// scope.
pub const CREATE_OPERATION: &str = "POST /api/v1/grants";

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// Scope plus resource selector plus a mandatory expiry (D-0030). An incident
/// grant is scoped to one incident; an approval grant is the extra grant a
/// human approval issues.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Grant")]
pub struct Grant {
    /// Grant id.
    #[schema(pattern = "^grt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Agent the grant authorizes.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: String,
    /// What the grant is for.
    #[schema(inline)]
    pub kind: GrantKind,
    /// Incident the grant is scoped to.
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
    /// Approval request that produced the grant.
    #[schema(pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub approval_id: Option<String>,
    /// Scopes granted. Listed explicitly; no wildcards.
    #[schema(value_type = Vec<ScopeSchema>)]
    pub scopes: Vec<String>,
    /// Where the scopes apply.
    #[schema(value_type = SelectorSchema)]
    pub selector: authz::Selector,
    /// Expiry; a TTL is mandatory.
    #[schema(format = "date-time")]
    pub expires_at: String,
    /// Maximum number of uses, or null when unlimited until expiry.
    #[schema(minimum = 1)]
    pub max_uses: Option<i64>,
    /// Uses consumed so far.
    #[schema(minimum = 0)]
    pub uses: i64,
    /// Grant status.
    #[schema(inline)]
    pub status: GrantStatus,
    /// Human who authorized the grant.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub on_behalf_of: String,
    /// Kind of identity that created the grant.
    #[schema(inline)]
    pub created_by_type: GrantCreator,
    /// Id of the identity that created the grant.
    pub created_by_id: String,
    /// Why the grant was issued.
    pub reason: String,
    /// When the grant was issued.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the grant was revoked.
    pub revoked_at: Option<String>,
    /// Id of the identity that revoked the grant.
    pub revoked_by: Option<String>,
}

/// One page of grants.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "GrantList")]
pub struct GrantList {
    pub items: Vec<Grant>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Only `standard` is accepted here; incident and approval grants are issued
/// by the system.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum CreateGrantKind {
    #[default]
    Standard,
}

/// Create a standard grant. `expires_at` or `ttl_s` is required: grants always
/// expire.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateGrantRequest")]
pub struct CreateGrantRequest {
    /// Agent to authorize.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: String,
    /// Only `standard` is accepted here.
    #[serde(default)]
    #[schema(inline, default = "standard")]
    pub kind: CreateGrantKind,
    /// Scopes to grant.
    #[schema(value_type = Vec<ScopeSchema>)]
    pub scopes: Vec<String>,
    /// Where the scopes apply.
    #[schema(value_type = SelectorSchema)]
    pub selector: Value,
    /// Absolute expiry. Either this or `ttl_s` is required.
    #[serde(default)]
    #[schema(format = "date-time", nullable = false)]
    pub expires_at: Option<String>,
    /// Lifetime in seconds. Either this or `expires_at` is required.
    #[serde(default)]
    #[schema(minimum = 1, nullable = false)]
    pub ttl_s: Option<i64>,
    /// Optional maximum number of uses.
    #[serde(default)]
    #[schema(minimum = 1, nullable = false)]
    pub max_uses: Option<i64>,
    /// Why the agent needs this grant.
    pub reason: String,
}

/// `status` filter of [`ListGrantsQuery`].
#[derive(Clone, Copy, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum GrantStatusFilter {
    Active,
    Expired,
    Revoked,
    Exhausted,
}

impl GrantStatusFilter {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::Exhausted => "exhausted",
        }
    }
}

/// `kind` filter of [`ListGrantsQuery`].
#[derive(Clone, Copy, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum GrantKindFilter {
    Standard,
    Incident,
    Approval,
}

impl GrantKindFilter {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Incident => "incident",
            Self::Approval => "approval",
        }
    }
}

/// `agent_id`, `status` and `kind` filters plus `limit` and `cursor`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListGrantsQuery {
    /// Only grants of this agent.
    #[param(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: Option<String>,
    /// Only grants in this status.
    #[param(inline)]
    pub status: Option<GrantStatusFilter>,
    /// Only grants of this kind.
    #[param(inline)]
    pub kind: Option<GrantKindFilter>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListGrantsQuery {
    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
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

/// Target of a grant operation.
#[must_use]
pub fn grant_target(grant_id: &str) -> Target {
    Target {
        kind: "grant".into(),
        id: grant_id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Grants with their scopes, selector, expiry, uses and status.
#[utoipa::path(
    get,
    path = "/api/v1/grants",
    tag = "grants",
    operation_id = "listGrants",
    extensions(("x-quoro-scope" = json!("grant:read"))),
    params(ListGrantsQuery),
    responses(
        (status = 200, description = "One page of grants.", body = GrantList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `grant:read`, `validation_failed` for a bad filter, or
/// a database error.
pub async fn list_grants(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(query): Query<ListGrantsQuery>,
) -> Result<Json<GrantList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/grants", &headers);
    let list = crate::service::grants::list_grants(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// Issues a standard grant: scopes, a resource selector and a mandatory
/// expiry, optionally with `max_uses`.
#[utoipa::path(
    post,
    path = "/api/v1/grants",
    tag = "grants",
    operation_id = "createGrant",
    extensions(("x-quoro-scope" = json!("grant:write"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    request_body = CreateGrantRequest,
    responses(
        (status = 201, description = "The grant was issued and is `active`.", body = Grant,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `validation_failed` for a scope, selector or expiry the contract refuses,
/// `forbidden` without `grant:write` (agents get `human_only`), `not_found` for
/// an unknown agent, or a database error.
pub async fn create_grant(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateGrantRequest>,
) -> Result<(StatusCode, Json<Grant>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let grant = crate::service::grants::create_grant(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(grant)))
}

/// One grant with its remaining uses and expiry.
#[utoipa::path(
    get,
    path = "/api/v1/grants/{grant_id}",
    tag = "grants",
    operation_id = "getGrant",
    extensions(("x-quoro-scope" = json!("grant:read"))),
    params(
        ("grant_id" = String, Path, description = "Grant id (`grt_...`).", pattern = "^grt_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The grant.", body = Grant,
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
/// `forbidden` without `grant:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_grant(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(grant_id): Path<String>,
) -> Result<Json<Grant>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/grants/{grant_id}",
        &headers,
    );
    let grant = crate::service::grants::get_grant(&state, &ctx, &grant_id).await?;
    Ok(Json(grant))
}

/// Revokes a grant immediately: the agent's next call is refused with
/// `grant_revoked`.
#[utoipa::path(
    post,
    path = "/api/v1/grants/{grant_id}/revoke",
    tag = "grants",
    operation_id = "revokeGrant",
    extensions(("x-quoro-scope" = json!("grant:write"))),
    params(
        ("grant_id" = String, Path, description = "Grant id (`grt_...`).", pattern = "^grt_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    responses(
        (status = 200, description = "The revoked grant; its status is `revoked`.", body = Grant,
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
/// `forbidden` without `grant:write` (agents get `human_only`), `not_found` for
/// an unknown id, `conflict` when the grant is not active, or a database
/// error.
pub async fn revoke_grant(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(grant_id): Path<String>,
) -> Result<Json<Grant>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "POST /api/v1/grants/{grant_id}/revoke",
        &headers,
    );
    let grant = crate::service::grants::revoke_grant(&state, &ctx, &grant_id).await?;
    Ok(Json(grant))
}
