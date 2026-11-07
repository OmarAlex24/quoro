//! Personal API tokens: `/api/v1/tokens*` (`openapi.yaml` `listTokens`,
//! `createToken`, `revokeToken`).
//!
//! A token belongs to the human who created it and authenticates as that
//! human (`Authorization: Bearer tok_...`, resolved in [`crate::auth`]). The
//! secret is shown once, by `createToken`; only its SHA-256 hash is stored.
//! The three operations are `x-quoro-scope: human`: any human may manage
//! their own tokens whatever their role, and an agent is refused with
//! `forbidden` / `human_only` (permission-model.md section 2), so it can never
//! obtain a credential that escapes its grants.

use crate::AppState;
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Operation strings recorded in the audit log and used as idempotency scopes.
pub const LIST_OPERATION: &str = "GET /api/v1/tokens";
pub const CREATE_OPERATION: &str = "POST /api/v1/tokens";
pub const REVOKE_OPERATION: &str = "DELETE /api/v1/tokens/{token_id}";

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// A personal API token of a human. Only its hash is stored; the secret is
/// shown once on creation.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "ApiToken")]
pub struct ApiToken {
    /// Token id.
    #[schema(pattern = "^tok_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// User the token belongs to.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub user_id: String,
    /// Label of the token.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Expiry of the token, or null when it does not expire.
    pub expires_at: Option<String>,
    /// When the token was last used.
    pub last_used_at: Option<String>,
    /// When the token was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the token was revoked.
    pub revoked_at: Option<String>,
}

/// One page of API tokens.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "ApiTokenList")]
pub struct ApiTokenList {
    pub items: Vec<ApiToken>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// The new token together with its secret. The secret is shown once and
/// cannot be retrieved again.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiTokenCreated {
    #[serde(flatten)]
    pub token_info: ApiToken,
    /// The token secret. Shown only in this response.
    pub token: String,
}

impl utoipa::PartialSchema for ApiTokenCreated {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::Ref;
        use utoipa::openapi::schema::{AllOfBuilder, ObjectBuilder, Type};
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .title(Some("ApiTokenCreated".to_string()))
            .item(Ref::from_schema_name("ApiToken"))
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Object)
                    .property(
                        "token",
                        ObjectBuilder::new()
                            .schema_type(Type::String)
                            .read_only(Some(true)),
                    )
                    .required("token"),
            )
            .into()
    }
}

impl utoipa::ToSchema for ApiTokenCreated {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("ApiTokenCreated")
    }
}

/// Create a personal API token.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateTokenRequest")]
pub struct CreateTokenRequest {
    /// Label of the token.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Optional expiry.
    pub expires_at: Option<String>,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TokenPage {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    cursor: Option<String>,
}

impl TokenPage {
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

/// Target of a token operation: the human the tokens belong to. The audit
/// contract has no `api_token` target type (audit-log-format.md section 2),
/// so, like `changePassword`, the record names the user and carries the
/// token id in `params`.
#[must_use]
pub fn token_owner_target(user_id: &str) -> Target {
    Target {
        kind: "user".into(),
        id: user_id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Personal API tokens of the authenticated human, newest first, revoked and
/// expired tokens included. The secret is never returned.
#[utoipa::path(
    get,
    path = "/api/v1/tokens",
    tag = "tokens",
    operation_id = "listTokens",
    extensions(("x-quoro-scope" = json!("human"))),
    params(TokenPage),
    responses(
        (status = 200, description = "One page of the caller's tokens.", body = ApiTokenList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` with `human_only` for an agent, `validation_failed` for a bad
/// page, or a database error.
pub async fn list_tokens(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<TokenPage>,
) -> Result<Json<ApiTokenList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, LIST_OPERATION, &headers);
    let list =
        crate::service::tokens::list_tokens(&state, &ctx, page.limit(), page.after()).await?;
    Ok(Json(list))
}

/// Creates a token for the authenticated human and returns its secret once.
#[utoipa::path(
    post,
    path = "/api/v1/tokens",
    tag = "tokens",
    operation_id = "createToken",
    extensions(("x-quoro-scope" = json!("human"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
    ),
    request_body(content = CreateTokenRequest, description = "Label and optional expiry of the token."),
    responses(
        (status = 201, description = "The token was created; the secret is in this response only.", body = ApiTokenCreated,
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
/// `forbidden` with `human_only` for an agent, `validation_failed` for a bad
/// name or expiry, `conflict` for a reused `Idempotency-Key`, or a database
/// error.
pub async fn create_token(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateTokenRequest>,
) -> Result<(StatusCode, Json<ApiTokenCreated>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let created = crate::service::tokens::create_token(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

/// Revokes one of the caller's tokens, effective on the next request. A token
/// of another user answers `not_found`, as an unknown id does.
#[utoipa::path(
    delete,
    path = "/api/v1/tokens/{token_id}",
    tag = "tokens",
    operation_id = "revokeToken",
    extensions(("x-quoro-scope" = json!("human"))),
    params(
        ("token_id" = String, Path, description = "API token id (`tok_...`).", pattern = "^tok_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 204, description = "The token was revoked.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
    )
)]
///
/// # Errors
///
/// `forbidden` with `human_only` for an agent, `not_found` for an unknown or
/// malformed id or a token of another user, or a database error.
pub async fn revoke_token(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(token_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(identity, request_id, REVOKE_OPERATION, &headers);
    crate::service::tokens::revoke_token(&state, &ctx, &token_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
