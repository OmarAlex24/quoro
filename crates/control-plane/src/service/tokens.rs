//! Personal API token operations (`/api/v1/tokens*`). Human-only
//! (`x-quoro-scope: human`): every human manages their own tokens whatever
//! their role, and an agent is refused with `forbidden` / `human_only`
//! (permission-model.md section 2, D-0029, D-0030).
//!
//! There is no scope behind these operations, so the audit records carry no
//! `scope`; their target is the calling user and the token id is in `params`.
//! A human's list is a read and is not recorded; an agent's refused attempt
//! is, on all three operations (audit-log-format.md section 3).

use crate::AppState;
use crate::api::tokens::{
    ApiToken, ApiTokenCreated, ApiTokenList, CREATE_OPERATION, CreateTokenRequest,
    token_owner_target,
};
use crate::audit::{self, Outcome};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Reason, Target};
use crate::idempotency::Begin;
use crate::request::CallCtx;
use crate::store::{now_rfc3339, random_secret, sha256_hex};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::Row;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The `tok_` prefix of a token id and of its secret. `random_secret` is 64
/// lowercase hex characters, so the secret is `tok_<64 hex>` and never equal
/// to the `tok_` id (a ULID), the same shape as an agent key (`akey_`).
const TOKEN_PREFIX: &str = "tok_";

/// Crockford base32, the alphabet of the ULID in an id.
const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Whether `id` matches the contract's `TokenId` pattern.
fn is_token_id(id: &str) -> bool {
    id.strip_prefix(TOKEN_PREFIX)
        .is_some_and(|rest| rest.len() == 26 && rest.bytes().all(|b| CROCKFORD.contains(&b)))
}

/// The store's fixed-width UTC form (`YYYY-MM-DDTHH:MM:SS.mmmZ`), so the
/// string comparison `expires_at > now` in authentication orders correctly.
fn fixed_rfc3339(t: OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    let len = name.chars().count();
    if !(1..=200).contains(&len) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    Ok(())
}

/// The requested expiry in the store's form: an RFC 3339 date-time in the
/// future, or none for a token that does not expire.
fn expiry(expires_at: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(text) = expires_at else {
        return Ok(None);
    };
    let parsed = OffsetDateTime::parse(text, &Rfc3339)
        .map_err(|_| ApiError::validation("expires_at is not an RFC 3339 date-time"))?;
    if parsed <= OffsetDateTime::now_utc() {
        return Err(ApiError::validation("expires_at is in the past"));
    }
    Ok(Some(fixed_rfc3339(parsed)))
}

/// The calling human's id, or the audited `human_only` refusal for an agent.
async fn caller(state: &AppState, ctx: &CallCtx, params: &Value) -> Result<String, ApiError> {
    if let Some(user) = ctx.identity.user() {
        return Ok(user.user_id.clone());
    }
    let error = ApiError::forbidden(Reason::HumanOnly, None);
    // `audit_error` records a `forbidden` as `refused` with the reason.
    ctx.audit_error(state, None, None, None, params, &error)
        .await?;
    Err(error)
}

fn token_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ApiToken, sqlx::Error> {
    Ok(ApiToken {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        name: row.try_get("name")?,
        expires_at: row.try_get("expires_at")?,
        last_used_at: row.try_get("last_used_at")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

const TOKEN_COLUMNS: &str = "id, user_id, name, expires_at, last_used_at, created_at, revoked_at";

/// The change-log payload: the row without `token_hash`. The hash is a
/// credential, so it stays out of the replicated log, as for `agent_key`
/// and `node_join_token`.
fn token_payload(token: &ApiToken) -> Value {
    json!({
        "id": token.id,
        "user_id": token.user_id,
        "name": token.name,
        "expires_at": token.expires_at,
        "last_used_at": token.last_used_at,
        "created_at": token.created_at,
        "revoked_at": token.revoked_at,
    })
}

/// Lists the caller's own tokens, newest first, revoked and expired ones
/// included. A token never carries its secret or hash here. `limit` is the
/// already-parsed page size, so an agent with a bad page is still refused
/// (and audited) as `human_only` first.
///
/// # Errors
///
/// `forbidden` with `human_only` for an agent, `validation_failed` for a bad
/// limit, or a database error.
pub async fn list_tokens(
    state: &AppState,
    ctx: &CallCtx,
    limit: Result<i64, ApiError>,
    after: &str,
) -> Result<ApiTokenList, ApiError> {
    let user_id = caller(state, ctx, &json!({})).await?;
    let limit = limit?;
    // Newest first; ids are ULIDs, monotonic within the primary (#541), so
    // descending id is descending creation.
    let mut sql = format!("SELECT {TOKEN_COLUMNS} FROM api_tokens WHERE user_id = ?");
    if !after.is_empty() {
        sql.push_str(" AND id < ?");
    }
    sql.push_str(" ORDER BY id DESC LIMIT ?");
    let mut statement = sqlx::query(&sql).bind(&user_id);
    if !after.is_empty() {
        statement = statement.bind(after);
    }
    let rows = statement
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
    let tokens = rows
        .iter()
        .map(token_from_row)
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    let (items, next_cursor) = crate::api::page::finish(tokens, limit, |t| t.id.clone());
    Ok(ApiTokenList { items, next_cursor })
}

/// Records an idempotent replay: `ok`, no change-log link.
async fn audit_replay(
    state: &AppState,
    ctx: &CallCtx,
    target: &Target,
    params: &Value,
) -> Result<(), ApiError> {
    let record = ctx.record(None, Some(target), None, params, Outcome::Ok, None, None);
    let mut w = state.store.begin_write().await?;
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(())
}

/// Creates a token for the caller and returns its secret once.
///
/// # Errors
///
/// `forbidden` with `human_only` for an agent, `validation_failed` for a bad
/// name or an expiry that is malformed or not in the future, `conflict` for a
/// reused `Idempotency-Key`, or a database error.
pub async fn create_token(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateTokenRequest,
    key: Option<String>,
) -> Result<ApiTokenCreated, ApiError> {
    let name = body.name.trim();
    let params = json!({ "name": name, "expires_at": body.expires_at });
    let user_id = caller(state, ctx, &params).await?;
    let target = token_owner_target(&user_id);

    let checked = validate_name(name).and_then(|()| expiry(body.expires_at.as_deref()));
    let expires_at = match checked {
        Ok(expires_at) => expires_at,
        Err(error) => {
            ctx.audit_error(state, None, Some(&target), None, &params, &error)
                .await?;
            return Err(error);
        }
    };

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let created: ApiTokenCreated = serde_json::from_value(stored)
                    .map_err(|e| ApiError::internal(&format!("replayed api token: {e}")))?;
                audit_replay(state, ctx, &target, &params).await?;
                return Ok(created);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(state, None, Some(&target), None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    match insert_token(state, ctx, &user_id, name, expires_at, &target, &params).await {
        Ok(created) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&created)
                    .map_err(|e| ApiError::internal(&format!("api token serialization: {e}")))?;
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(created)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_OPERATION, key);
            }
            ctx.audit_error(state, None, Some(&target), None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn insert_token(
    state: &AppState,
    ctx: &CallCtx,
    user_id: &str,
    name: &str,
    expires_at: Option<String>,
    target: &Target,
    params: &Value,
) -> Result<ApiTokenCreated, ApiError> {
    let id = new_id(Prefix::ApiToken);
    let secret = format!("{TOKEN_PREFIX}{}", random_secret());
    debug_assert_ne!(id, secret, "a token secret is never its id");
    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "INSERT INTO api_tokens (id, user_id, name, token_hash, expires_at, last_used_at, created_at, revoked_at) \
         VALUES (?, ?, ?, ?, ?, NULL, ?, NULL)",
    )
    .bind(&id)
    .bind(user_id)
    .bind(name)
    .bind(sha256_hex(secret.as_bytes()))
    .bind(&expires_at)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let token_info = ApiToken {
        id: id.clone(),
        user_id: user_id.to_string(),
        name: name.to_string(),
        expires_at,
        last_used_at: None,
        created_at: now,
        revoked_at: None,
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "api_token",
        &id,
        Op::Create,
        token_payload(&token_info),
    )
    .await?;
    // The new id goes into the record so it can be paired with its revoke.
    let mut params = params.clone();
    params["token_id"] = json!(id);
    ctx.append_ok(&mut w.tx, None, Some(target), None, &params, Some(seq))
        .await?;
    w.commit().await?;
    Ok(ApiTokenCreated {
        token_info,
        token: secret,
    })
}

/// Revokes one of the caller's tokens; the next request with it is
/// `unauthenticated`, and an interactive session it opened closes at its next
/// re-check. Repeating the call on an already-revoked token is a no-op that
/// still answers 204 and records the call.
///
/// # Errors
///
/// `forbidden` with `human_only` for an agent, `not_found` for an unknown or
/// malformed id or a token of another user, or a database error.
pub async fn revoke_token(state: &AppState, ctx: &CallCtx, token_id: &str) -> Result<(), ApiError> {
    let params = json!({ "token_id": token_id });
    let user_id = caller(state, ctx, &params).await?;
    let target = token_owner_target(&user_id);
    match revoke_token_tx(state, ctx, &user_id, token_id, &target, &params).await {
        Ok(()) => Ok(()),
        Err(error) => {
            ctx.audit_error(state, None, Some(&target), None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn revoke_token_tx(
    state: &AppState,
    ctx: &CallCtx,
    user_id: &str,
    token_id: &str,
    target: &Target,
    params: &Value,
) -> Result<(), ApiError> {
    if !is_token_id(token_id) {
        return Err(ApiError::not_found("api token"));
    }
    let mut w = state.store.begin_write().await?;
    // Scoped to the caller: another user's token is indistinguishable from
    // an unknown one.
    let row = sqlx::query(&format!(
        "SELECT {TOKEN_COLUMNS} FROM api_tokens WHERE id = ? AND user_id = ?"
    ))
    .bind(token_id)
    .bind(user_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("api token"))?;
    let token = token_from_row(&row)?;
    if token.revoked_at.is_some() {
        // Idempotent: nothing changed, so there is no change-log entry.
        ctx.append_ok(&mut w.tx, None, Some(target), None, params, None)
            .await?;
        w.commit().await?;
        return Ok(());
    }
    let now = now_rfc3339();
    sqlx::query("UPDATE api_tokens SET revoked_at = ? WHERE id = ?")
        .bind(&now)
        .bind(token_id)
        .execute(&mut *w.tx)
        .await?;
    let updated = ApiToken {
        revoked_at: Some(now),
        ..token
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "api_token",
        token_id,
        Op::Update,
        token_payload(&updated),
    )
    .await?;
    ctx.append_ok(&mut w.tx, None, Some(target), None, params, Some(seq))
        .await?;
    w.commit().await?;
    Ok(())
}
