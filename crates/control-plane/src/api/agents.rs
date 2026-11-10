//! Agents: `/api/v1/agents*` and `POST /api/v1/auth/agent-token`.
//!
//! An agent is an AI identity owned by a human (D-0029). The agent key is a
//! credential that only mints short-lived access tokens (D-0030); the secret
//! is shown once and only its SHA-256 hash is stored. `agent:write` is
//! human-only (permission-model.md section 4.2), so every mutating handler
//! authorizes through [`CallCtx::authorize_human`].
//!
//! `deleteAgent` is pending a contract decision: the contract says grants and
//! audit records must stay, but `db-schema.sql` cascades grants on agent
//! delete, so the operation is deliberately not implemented yet.

use crate::AppState;
use crate::auth::{Identity, authenticate_agent_key};
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use quoro_types::{Prefix, new_id};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Operation strings recorded in the audit log and used as idempotency scopes.
pub const CREATE_OPERATION: &str = "POST /api/v1/agents";
pub const CREATE_KEY_OPERATION: &str = "POST /api/v1/agents/{agent_id}/keys";
pub const TOKEN_OPERATION: &str = "POST /api/v1/auth/agent-token";

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// An AI identity (D-0029).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "Agent")]
pub struct Agent {
    /// Agent id.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Agent name, unique in the cluster.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Free-text model identifier the agent runs.
    pub model: String,
    /// Human who owns the agent.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub owner_user_id: String,
    /// What the agent is for.
    pub description: Option<String>,
    /// A disabled agent cannot mint access tokens.
    pub disabled: bool,
    /// When the agent was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the agent record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// One page of agents.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AgentList")]
pub struct AgentList {
    pub items: Vec<Agent>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Create an agent. It starts with no grants.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateAgentRequest")]
pub struct CreateAgentRequest {
    /// Unique agent name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Free-text model identifier.
    pub model: String,
    /// Human owner of the agent.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub owner_user_id: String,
    /// What the agent is for.
    #[schema(max_length = 2000, nullable = false)]
    pub description: Option<String>,
}

/// Change an agent. Absent fields stay unchanged.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "UpdateAgentRequest")]
pub struct UpdateAgentRequest {
    /// New unique agent name.
    #[schema(min_length = 1, max_length = 200, nullable = false)]
    pub name: Option<String>,
    /// New model identifier.
    #[schema(nullable = false)]
    pub model: Option<String>,
    /// New human owner.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false)]
    pub owner_user_id: Option<String>,
    /// New description.
    #[schema(max_length = 2000, nullable = false)]
    pub description: Option<String>,
    /// Disable or re-enable the agent.
    #[schema(nullable = false)]
    pub disabled: Option<bool>,
}

/// A long-lived agent credential. It authorizes nothing by itself; it only
/// mints short-lived access tokens (D-0030).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "AgentKey")]
pub struct AgentKey {
    /// Agent key id.
    #[schema(pattern = "^akey_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Agent the key belongs to.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: String,
    /// When the key was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the key was revoked.
    pub revoked_at: Option<String>,
    /// When the key last minted an access token.
    pub last_used_at: Option<String>,
}

/// A new agent key with its secret. The secret is shown once and cannot be
/// retrieved again.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentKeyCreated {
    #[serde(flatten)]
    pub key_info: AgentKey,
    /// The key secret. Shown only in this response.
    pub key: String,
}

impl utoipa::PartialSchema for AgentKeyCreated {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::Ref;
        use utoipa::openapi::schema::{AllOfBuilder, ObjectBuilder, Type};
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .title(Some("AgentKeyCreated".to_string()))
            .item(Ref::from_schema_name("AgentKey"))
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Object)
                    .property(
                        "key",
                        ObjectBuilder::new()
                            .schema_type(Type::String)
                            .read_only(Some(true)),
                    )
                    .required("key"),
            )
            .into()
    }
}

impl utoipa::ToSchema for AgentKeyCreated {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("AgentKeyCreated")
    }
}

/// One page of an agent's keys, newest first, revoked keys included. A key
/// never carries its secret here.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AgentKeyList")]
pub struct AgentKeyList {
    pub items: Vec<AgentKey>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// A short-lived agent access token minted from an agent key.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AccessTokenResponse")]
pub struct AccessTokenResponse {
    /// The access token. It is shown only in this response.
    #[schema(pattern = "^atk_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub access_token: String,
    /// Expiry, 5 minutes after issue.
    #[schema(format = "date-time")]
    pub expires_at: String,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AgentPage {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    cursor: Option<String>,
}

impl AgentPage {
    fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

pub(crate) type AgentRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    i64,
    String,
    String,
);

/// Builds an [`Agent`] from a `agents` row tuple.
pub(crate) fn agent_from_tuple(row: AgentRow) -> Agent {
    let (id, name, model, owner_user_id, description, disabled, created_at, updated_at) = row;
    Agent {
        id,
        name,
        model,
        owner_user_id,
        description,
        disabled: disabled != 0,
        created_at,
        updated_at,
    }
}

/// Target of an agent operation.
#[must_use]
pub fn agent_target(agent_id: &str) -> Target {
    Target {
        kind: "agent".into(),
        id: agent_id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Every AI identity with its model, owner and `disabled` flag (D-0029).
#[utoipa::path(
    get,
    path = "/api/v1/agents",
    tag = "agents",
    operation_id = "listAgents",
    extensions(("x-quoro-scope" = json!("agent:read"))),
    params(AgentPage),
    responses(
        (status = 200, description = "One page of agents.", body = AgentList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `agent:read`, or a database error.
pub async fn list_agents(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<AgentPage>,
) -> Result<Json<AgentList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/agents", &headers);
    let list =
        crate::service::agents::list_agents(&state, &ctx, page.limit()?, page.after()).await?;
    Ok(Json(list))
}

/// Creates an AI identity owned by a human.
#[utoipa::path(
    post,
    path = "/api/v1/agents",
    tag = "agents",
    operation_id = "createAgent",
    extensions(("x-quoro-scope" = json!("agent:write"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = CreateAgentRequest,
    responses(
        (status = 201, description = "The agent was created.", body = Agent,
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
/// `validation_failed` for a bad name, description or owner, `forbidden`
/// without `agent:write` (agents get `human_only`), `conflict` for a duplicate
/// name, or a database error.
pub async fn create_agent(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateAgentRequest>,
) -> Result<(StatusCode, Json<Agent>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let agent = crate::service::agents::create_agent(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(agent)))
}

/// One agent with its model, owner and `disabled` flag.
#[utoipa::path(
    get,
    path = "/api/v1/agents/{agent_id}",
    tag = "agents",
    operation_id = "getAgent",
    extensions(("x-quoro-scope" = json!("agent:read"))),
    params(
        ("agent_id" = String, Path, description = "Agent id (`agt_...`).", pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The agent.", body = Agent,
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
/// `forbidden` without `agent:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_agent(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(agent_id): Path<String>,
) -> Result<Json<Agent>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/agents/{agent_id}",
        &headers,
    );
    let agent = crate::service::agents::get_agent(&state, &ctx, &agent_id).await?;
    Ok(Json(agent))
}

/// Changes name, model, owner or description, and sets `disabled`.
#[utoipa::path(
    patch,
    path = "/api/v1/agents/{agent_id}",
    tag = "agents",
    operation_id = "updateAgent",
    extensions(("x-quoro-scope" = json!("agent:write"))),
    params(
        ("agent_id" = String, Path, description = "Agent id (`agt_...`).", pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = UpdateAgentRequest,
    responses(
        (status = 200, description = "The updated agent.", body = Agent,
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
///
/// # Errors
///
/// `validation_failed` for a bad name, description or owner, `forbidden`
/// without `agent:write` (agents get `human_only`), `not_found` for an unknown
/// id, `conflict` for a duplicate name, or a database error.
pub async fn update_agent(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(agent_id): Path<String>,
    ApiJson(body): ApiJson<UpdateAgentRequest>,
) -> Result<Json<Agent>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "PATCH /api/v1/agents/{agent_id}",
        &headers,
    );
    let agent = crate::service::agents::update_agent(&state, &ctx, &agent_id, &body).await?;
    Ok(Json(agent))
}

/// The agent's keys, newest first, revoked keys included. Secrets are never
/// returned: a key's secret is shown once, by `createAgentKey`.
#[utoipa::path(
    get,
    path = "/api/v1/agents/{agent_id}/keys",
    tag = "agents",
    operation_id = "listAgentKeys",
    extensions(("x-quoro-scope" = json!("agent:read"))),
    params(
        ("agent_id" = String, Path, description = "Agent id (`agt_...`).", pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$"),
        AgentPage,
    ),
    responses(
        (status = 200, description = "One page of the agent's keys.", body = AgentKeyList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `agent:read`, `not_found` for an unknown or malformed
/// agent id, `validation_failed` for a bad page, or a database error.
pub async fn list_agent_keys(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(agent_id): Path<String>,
    Query(page): Query<AgentPage>,
) -> Result<Json<AgentKeyList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/agents/{agent_id}/keys",
        &headers,
    );
    let list = crate::service::agents::list_agent_keys(
        &state,
        &ctx,
        &agent_id,
        page.limit()?,
        page.after(),
    )
    .await?;
    Ok(Json(list))
}

/// Creates an agent key and returns its secret once.
#[utoipa::path(
    post,
    path = "/api/v1/agents/{agent_id}/keys",
    tag = "agents",
    operation_id = "createAgentKey",
    extensions(("x-quoro-scope" = json!("agent:write"))),
    params(
        ("agent_id" = String, Path, description = "Agent id (`agt_...`).", pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    responses(
        (status = 201, description = "The key was created; the secret is in this response only.", body = AgentKeyCreated,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `agent:write` (agents get `human_only`), `not_found`
/// for an unknown agent, or a database error.
pub async fn create_agent_key(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(agent_id): Path<String>,
) -> Result<(StatusCode, Json<AgentKeyCreated>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_KEY_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let created = crate::service::agents::create_agent_key(&state, &ctx, &agent_id, key).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

/// Revokes one agent key. Access tokens already minted from it stay valid
/// until they expire, at most 5 minutes (D-0030). Repeating the call is
/// idempotent: an already-revoked key still answers 204.
#[utoipa::path(
    delete,
    path = "/api/v1/agents/{agent_id}/keys/{key_id}",
    tag = "agents",
    operation_id = "revokeAgentKey",
    extensions(("x-quoro-scope" = json!("agent:write"))),
    params(
        ("agent_id" = String, Path, description = "Agent id (`agt_...`).", pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("key_id" = String, Path, description = "Agent key id (`akey_...`).", pattern = "^akey_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    responses(
        (status = 204, description = "The key was revoked.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `agent:write` (agents get `human_only`), `not_found`
/// for an unknown key or a key of another agent, or a database error.
pub async fn revoke_agent_key(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path((agent_id, key_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "DELETE /api/v1/agents/{agent_id}/keys/{key_id}",
        &headers,
    );
    crate::service::agents::revoke_agent_key(&state, &ctx, &agent_id, &key_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Mint a short-lived agent access token from an agent key.
#[utoipa::path(
    post,
    path = "/api/v1/auth/agent-token",
    tag = "auth",
    operation_id = "createAgentToken",
    security(("agentKey" = [])),
    extensions(("x-quoro-scope" = json!("authenticated"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    responses(
        (status = 200, description = "A new access token.", body = AccessTokenResponse,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `unauthenticated` for an unknown or revoked key (or any non-`akey_`
/// bearer), `forbidden` with `agent_disabled` for a disabled agent,
/// `validation_failed` without `Quoro-Justification`, or a database error.
pub async fn create_agent_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
) -> Result<Json<AccessTokenResponse>, ApiError> {
    // The agent key is accepted on this endpoint only, so authentication does
    // not go through the `Identity` extractor.
    let holder = authenticate_agent_key(&state, &headers).await?;
    // The credential names the access-token row the mint is about to create.
    let token_id = new_id(Prefix::AccessToken);
    let identity = holder.identity(token_id.clone());
    let ctx = CallCtx::new(identity, request_id, TOKEN_OPERATION, &headers);
    // `Idempotency-Key` is accepted by the contract but deliberately ignored:
    // each call mints a fresh token and is audited, so a replay would otherwise
    // hand back a token whose audit trail no longer matches the call.
    let token =
        crate::service::agents::create_agent_token(&state, &ctx, &holder.key_id, &token_id).await?;
    Ok(Json(token))
}
