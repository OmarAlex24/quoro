//! Users: `GET /api/v1/users`. `user:read` is human-only
//! (permission-model.md section 4.2), so an agent is refused with `forbidden`
//! and `reason: human_only` (D-0030).

use crate::AppState;
use crate::api::auth::User;
use crate::error::ApiError;
use crate::request::{CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

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
