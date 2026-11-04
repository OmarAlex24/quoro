//! Users: `GET /api/v1/users` and the human-user management calls.
//! `user:read` and `user:write` are human-only (permission-model.md section
//! 4.2), so an agent is refused with `forbidden` and `reason: human_only`
//! (D-0030).

use crate::AppState;
use crate::api::auth::User;
use crate::auth::Identity;
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Operation strings recorded in the audit log and used as idempotency scopes.
pub const CREATE_OPERATION: &str = "POST /api/v1/users";
pub const UPDATE_OPERATION: &str = "PATCH /api/v1/users/{user_id}";
pub const DELETE_OPERATION: &str = "DELETE /api/v1/users/{user_id}";

/// One of the fixed role bundles, as sent in a create or update request.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum UserRoleRequest {
    Owner,
    Operator,
    Viewer,
}

impl UserRoleRequest {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Operator => "operator",
            Self::Viewer => "viewer",
        }
    }

    #[must_use]
    pub fn role(self) -> quoro_types::Role {
        match self {
            Self::Owner => quoro_types::Role::Owner,
            Self::Operator => quoro_types::Role::Operator,
            Self::Viewer => quoro_types::Role::Viewer,
        }
    }
}

/// Create a human user. `Debug` leaves the password out.
#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateUserRequest")]
pub struct CreateUserRequest {
    /// Unique email.
    #[schema(format = "email")]
    pub email: String,
    /// Display name.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Fixed scope bundle.
    #[schema(inline)]
    pub role: UserRoleRequest,
    /// Initial password. Stored as an Argon2id hash.
    #[schema(format = "password", min_length = 12, write_only)]
    pub password: String,
    /// Whether the user starts disabled.
    #[serde(default)]
    #[schema(default = false, required = false)]
    pub disabled: bool,
}

/// Change a user. Absent fields stay unchanged. `Debug` leaves the password
/// out.
#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(title = "UpdateUserRequest")]
pub struct UpdateUserRequest {
    /// New unique email.
    #[schema(format = "email", nullable = false)]
    pub email: Option<String>,
    /// New display name.
    #[schema(min_length = 1, max_length = 200, nullable = false)]
    pub name: Option<String>,
    /// New fixed scope bundle.
    #[schema(inline, nullable = false)]
    pub role: Option<UserRoleRequest>,
    /// Whether the user is disabled.
    #[schema(nullable = false)]
    pub disabled: Option<bool>,
    /// New password. Stored as an Argon2id hash.
    #[schema(format = "password", min_length = 12, write_only, nullable = false)]
    pub password: Option<String>,
}

impl std::fmt::Debug for CreateUserRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateUserRequest")
            .field("email", &self.email)
            .field("name", &self.name)
            .field("role", &self.role)
            .field("password", &"<redacted>")
            .field("disabled", &self.disabled)
            .finish()
    }
}

impl std::fmt::Debug for UpdateUserRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateUserRequest")
            .field("email", &self.email)
            .field("name", &self.name)
            .field("role", &self.role)
            .field("disabled", &self.disabled)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Target of a user operation.
#[must_use]
pub fn user_target(user_id: &str) -> Target {
    Target {
        kind: "user".into(),
        id: user_id.to_string(),
    }
}

/// One page of users.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "UserList")]
pub struct UserList {
    pub items: Vec<User>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UserPage {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    cursor: Option<String>,
}

impl UserPage {
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

/// Every human identity with its role, so a reader can turn a user id into a
/// name. `user:read` is human-only, so an agent is refused with
/// `reason: human_only`.
#[utoipa::path(
    get,
    path = "/api/v1/users",
    tag = "users",
    operation_id = "listUsers",
    extensions(("x-quoro-scope" = json!("user:read"))),
    params(UserPage),
    responses(
        (status = 200, description = "One page of users.", body = UserList,
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
/// `forbidden` without `user:read` (agents get `human_only`),
/// `validation_failed` for a bad page, or a database error.
pub async fn list_users(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<UserPage>,
) -> Result<Json<UserList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/users", &headers);
    let list = crate::service::users::list_users(&state, &ctx, page.limit()?, page.after()).await?;
    Ok(Json(list))
}

/// Creates a human with one of the fixed role bundles.
#[utoipa::path(
    post,
    path = "/api/v1/users",
    tag = "users",
    operation_id = "createUser",
    extensions(("x-quoro-scope" = json!("user:write"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "The user was created.", body = User,
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
/// `validation_failed` for a bad email, name, role or password, `forbidden`
/// without `user:write` (agents get `human_only`), `conflict` for a duplicate
/// email or a reused `Idempotency-Key`, or a database error.
pub async fn create_user(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateUserRequest>,
) -> Result<(StatusCode, Json<User>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let user = crate::service::users::create_user(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(user)))
}

/// Changes name, email, role, password or the `disabled` flag.
#[utoipa::path(
    patch,
    path = "/api/v1/users/{user_id}",
    tag = "users",
    operation_id = "updateUser",
    extensions(("x-quoro-scope" = json!("user:write"))),
    params(
        ("user_id" = String, Path, description = "User id (`usr_...`).", pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = UpdateUserRequest,
    responses(
        (status = 200, description = "The updated user.", body = User,
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
/// `validation_failed` for a bad email, name or password, `forbidden` without
/// `user:write` (agents get `human_only`), `not_found` for an unknown or
/// malformed id, `conflict` for a duplicate email or for demoting or disabling
/// the last enabled owner, or a database error.
pub async fn update_user(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(user_id): Path<String>,
    ApiJson(body): ApiJson<UpdateUserRequest>,
) -> Result<Json<User>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, UPDATE_OPERATION, &headers);
    let user = crate::service::users::update_user(&state, &ctx, &user_id, &body).await?;
    Ok(Json(user))
}

/// Deletes a user and revokes its sessions and tokens. The last remaining
/// owner cannot be deleted.
#[utoipa::path(
    delete,
    path = "/api/v1/users/{user_id}",
    tag = "users",
    operation_id = "deleteUser",
    extensions(("x-quoro-scope" = json!("user:write"))),
    params(
        ("user_id" = String, Path, description = "User id (`usr_...`).", pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    responses(
        (status = 204, description = "The user was deleted.",
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
/// `forbidden` without `user:write` (agents get `human_only`), `not_found` for
/// an unknown or malformed id, `conflict` for the last enabled owner or for a
/// user still referenced by agents, grants or other records, or a database
/// error.
pub async fn delete_user(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(user_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(identity, request_id, DELETE_OPERATION, &headers);
    crate::service::users::delete_user(&state, &ctx, &user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
