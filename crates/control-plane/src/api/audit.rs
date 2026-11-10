//! Audit reads: `/api/v1/audit`, `/api/v1/audit/{seq}` and
//! `POST /api/v1/audit/verify` (`audit-log-format.md` sections 2.4, 6).
//!
//! The log is append-only and hash-chained; nothing here writes a record
//! except the verification run, which the audit contract requires (section 6).

use crate::AppState;
use crate::auth::Identity;
use crate::error::ApiError;
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// Kind of identity behind a call (`AuditRecord.actor_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AuditActorType {
    User,
    Agent,
    Node,
    System,
}

/// Channel the call came through (`AuditRecord.channel`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AuditChannel {
    Api,
    Panel,
    Mcp,
    Cli,
    System,
}

/// Outcome of the call (`AuditRecord.result`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AuditOutcome {
    Ok,
    Refused,
    Error,
}

/// One record of the append-only audit log (D-0031). Optional absent fields
/// are omitted from the JSON, as the hash covers the serialized object.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "AuditRecord")]
pub struct AuditRecord {
    /// Gap-free sequence number of the audit log.
    #[schema(minimum = 1)]
    pub seq: i64,
    /// When the call was recorded.
    #[schema(format = "date-time")]
    pub time: String,
    /// Kind of identity behind the call.
    #[schema(inline)]
    pub actor_type: AuditActorType,
    /// Id of the identity.
    pub actor_id: String,
    /// Model of the agent, when the actor is an agent.
    pub agent_model: Option<String>,
    /// Human the actor acted for.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub on_behalf_of: Option<String>,
    /// Channel the call came through.
    #[schema(inline)]
    pub channel: AuditChannel,
    /// Endpoint or MCP tool name.
    pub operation: String,
    /// Scope the call needed, when it needed one.
    pub scope: Option<String>,
    /// Kind of the addressed resource.
    pub target_type: Option<String>,
    /// Id of the addressed resource.
    pub target_id: Option<String>,
    /// Project of the target, when it has one.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// Call parameters, with secrets redacted; `{}` when the record keeps
    /// their digest instead, or when the request body was above the limit.
    #[schema(value_type = Object)]
    pub params: Value,
    /// SHA-256 of the redacted parameters' text (`audit-log-format.md`
    /// section 4.1), present only when `params` is `{}` in their place.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub params_sha256: Option<String>,
    /// Size in bytes of the text `params_sha256` covers; present exactly when
    /// `params_sha256` is.
    #[schema(minimum = 0)]
    pub params_bytes: Option<i64>,
    /// The agent's stated reason, from `Quoro-Justification`.
    pub justification: Option<String>,
    /// Event that triggered the call.
    #[schema(pattern = "^evt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub trigger_event_id: Option<String>,
    /// Incident the call belongs to.
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
    /// Grant that authorized the call.
    #[schema(pattern = "^grt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub grant_id: Option<String>,
    /// Approval request behind the call.
    #[schema(pattern = "^apr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub approval_id: Option<String>,
    /// Correlation id of the call.
    #[schema(pattern = "^req_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub request_id: String,
    /// Outcome of the call.
    #[schema(inline)]
    pub result: AuditOutcome,
    /// Refusal reason or error code.
    pub result_detail: Option<String>,
    /// Change-log seq the action produced, if any.
    pub change_seq: Option<i64>,
    /// Hash of the previous record.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub prev_hash: String,
    /// Hash of this record.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub hash: String,
}

/// One page of audit records in ascending `seq` order, or descending with
/// `order=desc`.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AuditList")]
pub struct AuditList {
    pub items: Vec<AuditRecord>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Verify a range of the audit chain; both bounds default to the whole log.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "AuditVerificationRequest")]
pub struct AuditVerificationRequest {
    /// First record to verify.
    #[serde(default)]
    #[schema(minimum = 1, nullable = false)]
    pub from_seq: Option<i64>,
    /// Last record to verify.
    #[serde(default)]
    #[schema(minimum = 1, nullable = false)]
    pub to_seq: Option<i64>,
}

/// Result of verifying the hash chain.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AuditVerificationResult")]
pub struct AuditVerificationResult {
    /// Whether every checked link matched.
    pub ok: bool,
    /// Number of records verified.
    #[schema(minimum = 0)]
    pub checked: i64,
    /// First record verified.
    #[schema(minimum = 1)]
    pub first_seq: i64,
    /// Last record verified.
    #[schema(minimum = 1)]
    pub last_seq: i64,
    /// Hash of the last record in the log.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub head_hash: String,
    /// First record whose link does not match.
    pub broken_at_seq: Option<i64>,
    /// Why the link does not match.
    pub reason: Option<String>,
}

/// Schema of the `result` query parameter, inline as the frozen contract
/// writes it (a `$ref` there would be a contract difference).
#[must_use]
pub fn audit_result_param() -> utoipa::openapi::schema::Object {
    utoipa::openapi::schema::ObjectBuilder::new()
        .schema_type(utoipa::openapi::schema::Type::String)
        .enum_values(Some(["ok", "refused", "error"]))
        .build()
}

/// Schema of the `order` query parameter, inline with its default as the
/// frozen contract writes it (ADR 0283).
#[must_use]
pub fn audit_order_param() -> utoipa::openapi::schema::Object {
    utoipa::openapi::schema::ObjectBuilder::new()
        .schema_type(utoipa::openapi::schema::Type::String)
        .enum_values(Some(["asc", "desc"]))
        .default(Some(serde_json::json!("asc")))
        .build()
}

/// Filters and paging of `listAudit` (`openapi.yaml` section 2.4).
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListAuditQuery {
    /// Only records of this actor (user, agent or node id).
    pub actor_id: Option<String>,
    /// Only records about this resource id.
    pub target_id: Option<String>,
    /// Only records with this result.
    #[param(schema_with = audit_result_param)]
    pub result: Option<String>,
    /// Only records that belong to this incident.
    #[param(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
    /// Order of the page by `seq`, `asc` (oldest first) or `desc` (newest
    /// first).
    #[param(schema_with = audit_order_param)]
    pub order: Option<String>,
    /// Return only entries whose `seq` is greater than this value.
    #[param(minimum = 0)]
    pub after_seq: Option<i64>,
    /// Return only entries whose `seq` is less than this value.
    #[param(minimum = 1)]
    pub before_seq: Option<i64>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// The audit records in ascending `seq` order (D-0031), or newest first with
/// `order=desc` (ADR 0283).
#[utoipa::path(
    get,
    path = "/api/v1/audit",
    tag = "audit",
    operation_id = "listAudit",
    extensions(
        ("x-quoro-scope" = json!("audit:read")),
        ("x-quoro-mcp-tool" = json!("list_audit")),
    ),
    params(ListAuditQuery),
    responses(
        (status = 200, description = "One page of audit records.", body = AuditList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `audit:read`, `validation_failed` for a bad limit,
/// cursor, order or bound, or a database error.
pub async fn list_audit(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(query): Query<ListAuditQuery>,
) -> Result<Json<AuditList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/audit", &headers);
    let list = crate::service::audit::list_audit(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// One record by sequence number, with its hashes.
#[utoipa::path(
    get,
    path = "/api/v1/audit/{seq}",
    tag = "audit",
    operation_id = "getAuditRecord",
    extensions(("x-quoro-scope" = json!("audit:read"))),
    params(
        ("seq" = i64, Path, description = "Sequence number of an audit record.", minimum = 1),
    ),
    responses(
        (status = 200, description = "The audit record.", body = AuditRecord,
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
/// `forbidden` without `audit:read`, `not_found` for an unknown `seq` or one
/// the agent's selector does not cover, or a database error.
pub async fn get_audit_record(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(seq): Path<i64>,
) -> Result<Json<AuditRecord>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/audit/{seq}", &headers);
    let record = crate::service::audit::get_audit_record(&state, &ctx, seq).await?;
    Ok(Json(record))
}

/// Recomputes the hash links of a range of the audit log.
#[utoipa::path(
    post,
    path = "/api/v1/audit/verify",
    tag = "audit",
    operation_id = "verifyAudit",
    extensions(("x-quoro-scope" = json!("audit:read"))),
    params(
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031).",
            nullable = false, min_length = 1, max_length = 2000),
    ),
    request_body(content = AuditVerificationRequest, content_type = "application/json",
        description = "Optional range to verify; the whole log by default."),
    responses(
        (status = 200, description = "The verification result.", body = AuditVerificationResult,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `audit:read`, `validation_failed` for an empty or
/// reversed range, or a database error.
pub async fn verify_audit(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    body: Option<ApiJson<AuditVerificationRequest>>,
) -> Result<Json<AuditVerificationResult>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "POST /api/v1/audit/verify", &headers);
    let body = body.map(|ApiJson(body)| body);
    let result = crate::service::audit::verify_audit(&state, &ctx, body).await?;
    Ok(Json(result))
}
