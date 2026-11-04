//! User operations (`/api/v1/users`). `user:read` and `user:write` are
//! human-only (permission-model.md sections 3 and 4.2).

use crate::AppState;
use crate::api::auth::{User, UserRow, user_from, valid_email};
use crate::api::users::{
    CREATE_OPERATION, CreateUserRequest, UpdateUserRequest, UserList, UserRoleRequest, user_target,
};
use crate::auth::{Credential, password};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::CallCtx;
use crate::store::{now_rfc3339, sha256_hex};
use quoro_types::{Prefix, Role, Scope, is_id, new_id};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;

const LAST_OWNER_UPDATE: &str = "the last enabled owner cannot be demoted or disabled";
const LAST_OWNER_DELETE: &str = "The last remaining owner cannot be deleted";
const USER_REFERENCED: &str =
    "the user is referenced by agents, grants or other records; disable it instead";

/// The shape stored in the idempotency cache: a [`User`] and nothing else. It
/// exists because [`User`] is a response type and is not deserialized.
#[derive(Deserialize)]
struct ReplayedUser {
    id: String,
    email: String,
    name: String,
    role: UserRoleRequest,
    disabled: bool,
    created_at: String,
}

impl From<ReplayedUser> for User {
    fn from(user: ReplayedUser) -> Self {
        Self {
            id: user.id,
            email: user.email,
            name: user.name,
            role: user.role.role().into(),
            disabled: user.disabled,
            created_at: user.created_at,
        }
    }
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if !(1..=200).contains(&name.chars().count()) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    Ok(())
}

fn validate_email(email: &str) -> Result<(), ApiError> {
    if !valid_email(email) {
        return Err(ApiError::validation("email is not a valid address"));
    }
    Ok(())
}

fn validate_password(password: &str) -> Result<(), ApiError> {
    if password.chars().count() < 12 {
        return Err(ApiError::validation(
            "password must be at least 12 characters",
        ));
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String, ApiError> {
    password::hash(password).map_err(|e| ApiError::internal(&e))
}

/// The replicated `users` row, exactly as the change log carries it: the
/// password hash is included so replaying the log rebuilds the table.
fn user_payload(user: &User, password_hash: &str) -> Value {
    json!({
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "role": user.role,
        "password_hash": password_hash,
        "disabled": i64::from(user.disabled),
        "created_at": user.created_at,
    })
}

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

/// Creates a human user. `user:write` is human-only, so an agent caller is
/// refused with `reason: human_only` and the refusal is audited.
///
/// # Errors
///
/// `validation_failed` for a bad email, name or password, `forbidden` without
/// `user:write`, `conflict` for a duplicate email, or a database error.
pub async fn create_user(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateUserRequest,
    key: Option<String>,
) -> Result<User, ApiError> {
    let name = body.name.trim();
    validate_name(name)?;
    validate_email(&body.email)?;
    validate_password(&body.password)?;
    let role = body.role.role();
    let params = json!({
        "email": body.email,
        "name": name,
        "role": body.role.as_str(),
        "disabled": body.disabled,
        "password": body.password,
    });
    ctx.authorize_human(state, Scope::UserWrite, None, None, &params)
        .await?;

    // The idempotency fingerprint carries a hash of the password, never the
    // password itself, so two calls with the same key but a different
    // password are a conflict rather than a silent replay.
    let fingerprint = json!({
        "email": body.email,
        "name": name,
        "role": body.role.as_str(),
        "disabled": body.disabled,
        "password": sha256_hex(body.password.as_bytes()),
    });
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_OPERATION, key, &fingerprint)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let user: User = serde_json::from_value::<ReplayedUser>(stored)
                    .map(|user| user.into())
                    .map_err(|e| ApiError::internal(&format!("replayed user: {e}")))?;
                let target = user_target(&user.id);
                ctx.audit_replay(state, Scope::UserWrite, &target, None, &params)
                    .await?;
                return Ok(user);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(state, Some(Scope::UserWrite), None, None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    match insert_user(state, ctx, body, name, role, &params).await {
        Ok(user) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&user)
                    .map_err(|e| ApiError::internal(&format!("user serialization: {e}")))?;
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(user)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_OPERATION, key);
            }
            ctx.audit_error(state, Some(Scope::UserWrite), None, None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn insert_user(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateUserRequest,
    name: &str,
    role: Role,
    params: &Value,
) -> Result<User, ApiError> {
    // Argon2id runs before the writer gate so it never blocks other writes.
    let password_hash = hash_password(&body.password)?;
    let mut w = state.store.begin_write().await?;
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM users WHERE email = ?")
        .bind(&body.email)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!("email {} is taken", body.email)));
    }
    let id = new_id(Prefix::User);
    let created_at = now_rfc3339();
    sqlx::query(
        "INSERT INTO users (id, email, name, role, password_hash, disabled, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&body.email)
    .bind(name)
    .bind(role.as_str())
    .bind(&password_hash)
    .bind(i64::from(body.disabled))
    .bind(&created_at)
    .execute(&mut *w.tx)
    .await?;
    let user = User {
        id: id.clone(),
        email: body.email.clone(),
        name: name.to_string(),
        role: role.into(),
        disabled: body.disabled,
        created_at,
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "user",
        &id,
        Op::Create,
        user_payload(&user, &password_hash),
    )
    .await?;
    let target = user_target(&id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::UserWrite),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(user)
}

/// Changes a user's fields; absent fields stay unchanged.
///
/// # Errors
///
/// `validation_failed` for a bad email, name or password, `forbidden` without
/// `user:write`, `not_found` for an unknown id, `conflict` for a duplicate
/// email or for demoting or disabling the last enabled owner, or a database
/// error.
pub async fn update_user(
    state: &AppState,
    ctx: &CallCtx,
    user_id: &str,
    body: &UpdateUserRequest,
) -> Result<User, ApiError> {
    if let Some(name) = &body.name {
        validate_name(name.trim())?;
    }
    if let Some(email) = &body.email {
        validate_email(email)?;
    }
    if let Some(password) = &body.password {
        validate_password(password)?;
    }
    let mut params = json!({
        "user_id": user_id,
        "email": body.email,
        "name": body.name,
        "role": body.role.map(UserRoleRequest::as_str),
        "disabled": body.disabled,
        "password": body.password,
    });
    // Only the fields that were sent: an absent password must not read as
    // `"[redacted]"` (a password change) in the audit record.
    if let Value::Object(map) = &mut params {
        map.retain(|_, value| !value.is_null());
    }
    let target = user_target(user_id);
    ctx.authorize_human(state, Scope::UserWrite, Some(&target), None, &params)
        .await?;

    match update_user_tx(state, ctx, user_id, body, &params, &target).await {
        Ok(user) => Ok(user),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::UserWrite),
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

async fn update_user_tx(
    state: &AppState,
    ctx: &CallCtx,
    user_id: &str,
    body: &UpdateUserRequest,
    params: &Value,
    target: &Target,
) -> Result<User, ApiError> {
    if !is_id(Prefix::User, user_id) {
        return Err(ApiError::not_found("user"));
    }
    // Hashing is CPU work; it runs before the writer gate.
    let new_hash = match &body.password {
        Some(password) => Some(hash_password(password)?),
        None => None,
    };
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(
        "SELECT id, email, name, role, password_hash, disabled, created_at FROM users WHERE id = ?",
    )
    .bind(user_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("user"))?;
    let current: UserRow = (
        row.try_get("id")?,
        row.try_get("email")?,
        row.try_get("name")?,
        row.try_get("role")?,
        row.try_get("disabled")?,
        row.try_get("created_at")?,
    );
    let current_hash: String = row.try_get("password_hash")?;
    let current_role: String = row.try_get("role")?;
    let current_disabled: i64 = row.try_get("disabled")?;

    let email = body.email.clone().unwrap_or_else(|| current.1.clone());
    let name = body
        .name
        .as_deref()
        .map(str::trim)
        .unwrap_or(&current.2)
        .to_string();
    let role = match body.role {
        Some(role) => role.role(),
        None => Role::parse(&current_role)
            .ok_or_else(|| ApiError::internal(&format!("user {user_id} has unknown role")))?,
    };
    let disabled = body.disabled.unwrap_or(current_disabled != 0);
    let password_hash = new_hash.clone().unwrap_or(current_hash);

    if email != current.1 {
        let taken: Option<(String,)> =
            sqlx::query_as("SELECT id FROM users WHERE email = ? AND id <> ?")
                .bind(&email)
                .bind(user_id)
                .fetch_optional(&mut *w.tx)
                .await?;
        if taken.is_some() {
            return Err(ApiError::conflict(format!("email {email} is taken")));
        }
    }

    // The last enabled owner keeps the cluster manageable: it cannot be
    // demoted or disabled.
    if current_role == Role::Owner.as_str()
        && current_disabled == 0
        && (role != Role::Owner || disabled)
    {
        let (others,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM users WHERE role = 'owner' AND disabled = 0 AND id <> ?",
        )
        .bind(user_id)
        .fetch_one(&mut *w.tx)
        .await?;
        if others == 0 {
            return Err(ApiError::conflict(LAST_OWNER_UPDATE));
        }
    }

    sqlx::query(
        "UPDATE users SET email = ?, name = ?, role = ?, password_hash = ?, disabled = ? \
         WHERE id = ?",
    )
    .bind(&email)
    .bind(&name)
    .bind(role.as_str())
    .bind(&password_hash)
    .bind(i64::from(disabled))
    .bind(user_id)
    .execute(&mut *w.tx)
    .await?;

    // A changed password or a disable forces a fresh login. `sessions` is
    // LOCAL: no change-log entry. When the caller changes their own password
    // their current session survives, so they are not logged out mid-call.
    let password_changed = new_hash.is_some();
    if password_changed || disabled {
        let keep = if password_changed && !disabled {
            match (&ctx.identity.credential, ctx.identity.user()) {
                (Credential::Session { session_id }, Some(user)) if user.user_id == user_id => {
                    Some(session_id)
                }
                _ => None,
            }
        } else {
            None
        };
        match keep {
            Some(session_id) => {
                sqlx::query("DELETE FROM sessions WHERE user_id = ? AND id <> ?")
                    .bind(user_id)
                    .bind(session_id)
                    .execute(&mut *w.tx)
                    .await?;
            }
            None => {
                sqlx::query("DELETE FROM sessions WHERE user_id = ?")
                    .bind(user_id)
                    .execute(&mut *w.tx)
                    .await?;
            }
        }
    }

    let updated = User {
        id: current.0,
        email,
        name,
        role: role.into(),
        disabled,
        created_at: current.5,
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "user",
        &updated.id,
        Op::Update,
        user_payload(&updated, &password_hash),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::UserWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(updated)
}

/// Deletes a user. Sessions and API tokens cascade; a user still referenced by
/// an agent, a grant or another record is refused, because foreign keys are on.
///
/// # Errors
///
/// `forbidden` without `user:write`, `not_found` for an unknown id, `conflict`
/// for the last enabled owner or a referenced user, or a database error.
pub async fn delete_user(state: &AppState, ctx: &CallCtx, user_id: &str) -> Result<(), ApiError> {
    let params = json!({ "user_id": user_id });
    let target = user_target(user_id);
    ctx.authorize_human(state, Scope::UserWrite, Some(&target), None, &params)
        .await?;

    match delete_user_tx(state, ctx, user_id, &params, &target).await {
        Ok(()) => Ok(()),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::UserWrite),
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

async fn delete_user_tx(
    state: &AppState,
    ctx: &CallCtx,
    user_id: &str,
    params: &Value,
    target: &Target,
) -> Result<(), ApiError> {
    if !is_id(Prefix::User, user_id) {
        return Err(ApiError::not_found("user"));
    }
    let mut w = state.store.begin_write().await?;
    let row: Option<(String, i64)> =
        sqlx::query_as("SELECT role, disabled FROM users WHERE id = ?")
            .bind(user_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    let Some((role, disabled)) = row else {
        return Err(ApiError::not_found("user"));
    };
    if role == Role::Owner.as_str() && disabled == 0 {
        let (owners,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM users WHERE role = 'owner' AND disabled = 0")
                .fetch_one(&mut *w.tx)
                .await?;
        if owners <= 1 {
            return Err(ApiError::conflict(LAST_OWNER_DELETE));
        }
    }
    match sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(user_id)
        .execute(&mut *w.tx)
        .await
    {
        Ok(_) => {}
        Err(sqlx::Error::Database(db))
            if db.kind() == sqlx::error::ErrorKind::ForeignKeyViolation =>
        {
            return Err(ApiError::conflict(USER_REFERENCED));
        }
        Err(error) => return Err(error.into()),
    }
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "user",
        user_id,
        Op::Delete,
        json!({ "id": user_id }),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::UserWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(())
}
