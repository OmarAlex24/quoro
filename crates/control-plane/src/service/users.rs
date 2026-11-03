//! User operations (`/api/v1/users`). `user:read` is human-only
//! (permission-model.md sections 3 and 4.2).

use crate::AppState;
use crate::api::auth::user_from;
use crate::api::users::UserList;
use crate::error::ApiError;
use crate::request::CallCtx;
use quoro_types::Scope;
use serde_json::json;
use sqlx::Row;

/// Lists human users, one page at a time, ascending by id.
///
/// # Errors
///
/// `forbidden` without `user:read` (agents get `human_only`), or a database
/// error.
pub async fn list_users(
    state: &AppState,
    ctx: &CallCtx,
    limit: i64,
    after: &str,
) -> Result<UserList, ApiError> {
    let params = json!({ "limit": limit });
    // `user:read` is human-only: an agent is refused before any row is read.
    ctx.authorize_human(state, Scope::UserRead, None, None, &params)
        .await?;
    let rows = sqlx::query(
        "SELECT id, email, name, role, disabled, created_at FROM users \
         WHERE id > ? ORDER BY id LIMIT ?",
    )
    .bind(after)
    .bind(limit + 1)
    .fetch_all(state.store.pool())
    .await?;
    let users = rows
        .iter()
        .map(|row| {
            user_from((
                row.try_get("id")?,
                row.try_get("email")?,
                row.try_get("name")?,
                row.try_get("role")?,
                row.try_get("disabled")?,
                row.try_get("created_at")?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (items, next_cursor) = crate::api::page::finish(users, limit, |user| user.id.clone());
    Ok(UserList { items, next_cursor })
}
