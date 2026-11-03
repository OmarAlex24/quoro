//! Agent operations (`/api/v1/agents*`, `POST /api/v1/auth/agent-token`).
//! Human-only except the token mint, which authenticates with an agent key
//! (D-0029, D-0030).

use crate::AppState;
use crate::api::agents::{
    AccessTokenResponse, Agent, AgentKey, AgentKeyCreated, AgentKeyList, AgentList,
    CREATE_KEY_OPERATION, CREATE_OPERATION, CreateAgentRequest, UpdateAgentRequest,
    agent_from_tuple, agent_target,
};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Reason, Target};
use crate::idempotency::Begin;
use crate::request::CallCtx;
use crate::store::{now_rfc3339, random_secret, rfc3339_in, sha256_hex};
use quoro_types::{Prefix, Scope, is_id, new_id, new_secret};
use serde_json::{Value, json};
use sqlx::Row;

/// The `akey_` secret prefix. `random_secret` is 64 lowercase hex characters,
/// so the secret is `akey_<64 hex>` and never equal to the `akey_` id (which
/// is a ULID).
const AGENT_KEY_PREFIX: &str = "akey_";

fn validate_name(name: &str) -> Result<(), ApiError> {
    let len = name.chars().count();
    if !(1..=200).contains(&len) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    Ok(())
}

fn validate_description(description: Option<&str>) -> Result<(), ApiError> {
    if description.is_some_and(|d| d.chars().count() > 2000) {
        return Err(ApiError::validation(
            "description must be at most 2000 characters",
        ));
    }
    Ok(())
}

fn validate_owner(owner_user_id: &str) -> Result<(), ApiError> {
    if !is_id(Prefix::User, owner_user_id) {
        return Err(ApiError::validation("owner_user_id must be a usr_ id"));
    }
    Ok(())
}

/// Lists every agent, one page at a time (permission-model.md section 5.1:
/// agents read through `authorize_read`; the read is audited).
///
/// # Errors
///
/// `forbidden` without `agent:read`, or a database error.
pub async fn list_agents(
    state: &AppState,
    ctx: &CallCtx,
    limit: i64,
    after: &str,
) -> Result<AgentList, ApiError> {
    let access = ctx
        .authorize_read(state, Scope::AgentRead, None, &json!({}))
        .await?;
    let result: Result<_, ApiError> = async {
        if access.reads_nothing() {
            return Ok(AgentList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let rows = sqlx::query(
            "SELECT id, name, model, owner_user_id, description, disabled, created_at, updated_at \
             FROM agents WHERE id > ? ORDER BY id LIMIT ?",
        )
        .bind(after)
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
        let agents = rows
            .iter()
            .map(|row| -> Result<Agent, sqlx::Error> {
                Ok(agent_from_tuple((
                    row.try_get("id")?,
                    row.try_get("name")?,
                    row.try_get("model")?,
                    row.try_get("owner_user_id")?,
                    row.try_get("description")?,
                    row.try_get("disabled")?,
                    row.try_get("created_at")?,
                    row.try_get("updated_at")?,
                )))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        let (items, next_cursor) =
            crate::api::page::finish(agents, limit, |agent| agent.id.clone());
        Ok(AgentList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Creates an agent. `agent:write` is human-only, so an agent caller is
/// refused with `reason: human_only` and the refusal is audited.
///
/// # Errors
///
/// `validation_failed` for a bad name, description or owner, `forbidden`
/// without `agent:write`, `conflict` for a duplicate name, or a database
/// error.
pub async fn create_agent(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateAgentRequest,
    key: Option<String>,
) -> Result<Agent, ApiError> {
    let name = body.name.trim();
    validate_name(name)?;
    validate_description(body.description.as_deref())?;
    validate_owner(&body.owner_user_id)?;
    let params = json!({
        "name": name,
        "model": body.model,
        "owner_user_id": body.owner_user_id,
        "description": body.description,
    });
    let target: Option<Target> = None;
    ctx.authorize_human(state, Scope::AgentWrite, target.as_ref(), None, &params)
        .await?;

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let agent: Agent = serde_json::from_value(stored)
                    .map_err(|e| ApiError::internal(&format!("replayed agent: {e}")))?;
                let target = agent_target(&agent.id);
                ctx.audit_replay(state, Scope::AgentWrite, &target, None, &params)
                    .await?;
                return Ok(agent);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(state, Some(Scope::AgentWrite), None, None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    match insert_agent(state, ctx, body, name, &params).await {
        Ok(agent) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&agent)
                    .map_err(|e| ApiError::internal(&format!("agent serialization: {e}")))?;
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(agent)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_OPERATION, key);
            }
            ctx.audit_error(state, Some(Scope::AgentWrite), None, None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn insert_agent(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateAgentRequest,
    name: &str,
    params: &Value,
) -> Result<Agent, ApiError> {
    let mut w = state.store.begin_write().await?;
    let owner: Option<(String,)> = sqlx::query_as("SELECT id FROM users WHERE id = ?")
        .bind(&body.owner_user_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if owner.is_none() {
        return Err(ApiError::validation(format!(
            "owner_user_id {} is not an existing user",
            body.owner_user_id
        )));
    }
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE name = ?")
        .bind(name)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!("name {name} is taken")));
    }
    let id = new_id(Prefix::Agent);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO agents (id, name, model, owner_user_id, description, disabled, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
    )
    .bind(&id)
    .bind(name)
    .bind(&body.model)
    .bind(&body.owner_user_id)
    .bind(&body.description)
    .bind(&now)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let agent = Agent {
        id: id.clone(),
        name: name.to_string(),
        model: body.model.clone(),
        owner_user_id: body.owner_user_id.clone(),
        description: body.description.clone(),
        disabled: false,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "agent",
        &id,
        Op::Create,
        agent_payload(&agent),
    )
    .await?;
    let target = agent_target(&id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AgentWrite),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(agent)
}

fn agent_payload(agent: &Agent) -> Value {
    json!({
        "id": agent.id,
        "name": agent.name,
        "model": agent.model,
        "owner_user_id": agent.owner_user_id,
        "description": agent.description,
        "disabled": i64::from(agent.disabled),
        "created_at": agent.created_at,
        "updated_at": agent.updated_at,
    })
}

/// One agent by id.
///
/// # Errors
///
/// `forbidden` without `agent:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_agent(state: &AppState, ctx: &CallCtx, agent_id: &str) -> Result<Agent, ApiError> {
    let target = agent_target(agent_id);
    let access = ctx
        .authorize_read(state, Scope::AgentRead, Some(&target), &json!({}))
        .await?;
    let result: Result<_, ApiError> = async {
        if !is_id(Prefix::Agent, agent_id) {
            return Err(ApiError::not_found("agent"));
        }
        let row = sqlx::query(
            "SELECT id, name, model, owner_user_id, description, disabled, created_at, updated_at \
             FROM agents WHERE id = ?",
        )
        .bind(agent_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("agent"))?;
        Ok(agent_from_tuple((
            row.try_get("id")?,
            row.try_get("name")?,
            row.try_get("model")?,
            row.try_get("owner_user_id")?,
            row.try_get("description")?,
            row.try_get("disabled")?,
            row.try_get("created_at")?,
            row.try_get("updated_at")?,
        )))
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Changes an agent's fields; absent fields stay unchanged.
///
/// # Errors
///
/// `validation_failed` for a bad name, description or owner, `forbidden`
/// without `agent:write`, `not_found` for an unknown id, `conflict` for a
/// duplicate name, or a database error.
pub async fn update_agent(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    body: &UpdateAgentRequest,
) -> Result<Agent, ApiError> {
    if let Some(name) = &body.name {
        validate_name(name)?;
    }
    validate_description(body.description.as_deref())?;
    if let Some(owner) = &body.owner_user_id {
        validate_owner(owner)?;
    }
    let params = json!({
        "agent_id": agent_id,
        "name": body.name,
        "model": body.model,
        "owner_user_id": body.owner_user_id,
        "description": body.description,
        "disabled": body.disabled,
    });
    let target = agent_target(agent_id);
    ctx.authorize_human(state, Scope::AgentWrite, Some(&target), None, &params)
        .await?;

    match update_agent_tx(state, ctx, agent_id, body, &params, &target).await {
        Ok(agent) => Ok(agent),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AgentWrite),
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

async fn update_agent_tx(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    body: &UpdateAgentRequest,
    params: &Value,
    target: &Target,
) -> Result<Agent, ApiError> {
    if !is_id(Prefix::Agent, agent_id) {
        return Err(ApiError::not_found("agent"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(
        "SELECT id, name, model, owner_user_id, description, disabled, created_at, updated_at \
         FROM agents WHERE id = ?",
    )
    .bind(agent_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("agent"))?;
    let current = agent_from_tuple((
        row.try_get("id")?,
        row.try_get("name")?,
        row.try_get("model")?,
        row.try_get("owner_user_id")?,
        row.try_get("description")?,
        row.try_get("disabled")?,
        row.try_get("created_at")?,
        row.try_get("updated_at")?,
    ));
    let name = body.name.as_deref().map(str::trim).unwrap_or(&current.name);
    let owner_user_id = body
        .owner_user_id
        .as_deref()
        .unwrap_or(&current.owner_user_id);
    if body.owner_user_id.is_some() {
        let owner: Option<(String,)> = sqlx::query_as("SELECT id FROM users WHERE id = ?")
            .bind(owner_user_id)
            .fetch_optional(&mut *w.tx)
            .await?;
        if owner.is_none() {
            return Err(ApiError::validation(format!(
                "owner_user_id {owner_user_id} is not an existing user"
            )));
        }
    }
    if name != current.name {
        let taken: Option<(String,)> =
            sqlx::query_as("SELECT id FROM agents WHERE name = ? AND id <> ?")
                .bind(name)
                .bind(agent_id)
                .fetch_optional(&mut *w.tx)
                .await?;
        if taken.is_some() {
            return Err(ApiError::conflict(format!("name {name} is taken")));
        }
    }
    let updated = Agent {
        id: current.id,
        name: name.to_string(),
        model: body.model.clone().unwrap_or(current.model),
        owner_user_id: owner_user_id.to_string(),
        description: body.description.clone().or(current.description),
        disabled: body.disabled.unwrap_or(current.disabled),
        created_at: current.created_at,
        updated_at: now_rfc3339(),
    };
    sqlx::query(
        "UPDATE agents SET name = ?, model = ?, owner_user_id = ?, description = ?, disabled = ?, updated_at = ? \
         WHERE id = ?",
    )
    .bind(&updated.name)
    .bind(&updated.model)
    .bind(&updated.owner_user_id)
    .bind(&updated.description)
    .bind(i64::from(updated.disabled))
    .bind(&updated.updated_at)
    .bind(&updated.id)
    .execute(&mut *w.tx)
    .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "agent",
        &updated.id,
        Op::Update,
        agent_payload(&updated),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AgentWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(updated)
}

/// Lists one agent's keys, newest first, revoked keys included. The secret and
/// its hash are never part of the response.
///
/// # Errors
///
/// `forbidden` without `agent:read`, `not_found` for an unknown or malformed
/// agent id, or a database error.
pub async fn list_agent_keys(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    limit: i64,
    after: &str,
) -> Result<AgentKeyList, ApiError> {
    let params = json!({ "agent_id": agent_id, "limit": limit });
    let target = agent_target(agent_id);
    let access = ctx
        .authorize_read(state, Scope::AgentRead, Some(&target), &params)
        .await?;
    let result: Result<_, ApiError> = async {
        if !is_id(Prefix::Agent, agent_id) {
            return Err(ApiError::not_found("agent"));
        }
        let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE id = ?")
            .bind(agent_id)
            .fetch_optional(state.store.pool())
            .await?;
        if exists.is_none() {
            return Err(ApiError::not_found("agent"));
        }
        // Newest first; ids are ULIDs, so descending id is descending creation.
        let mut sql = String::from(
            "SELECT id, agent_id, created_at, revoked_at, last_used_at FROM agent_keys \
             WHERE agent_id = ?",
        );
        if !after.is_empty() {
            sql.push_str(" AND id < ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        let mut statement = sqlx::query(&sql).bind(agent_id);
        if !after.is_empty() {
            statement = statement.bind(after);
        }
        let rows = statement
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let keys = rows
            .iter()
            .map(|row| -> Result<AgentKey, sqlx::Error> {
                Ok(AgentKey {
                    id: row.try_get("id")?,
                    agent_id: row.try_get("agent_id")?,
                    created_at: row.try_get("created_at")?,
                    revoked_at: row.try_get("revoked_at")?,
                    last_used_at: row.try_get("last_used_at")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        let (items, next_cursor) = crate::api::page::finish(keys, limit, |key| key.id.clone());
        Ok(AgentKeyList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Creates a key for an agent and returns its secret once.
///
/// # Errors
///
/// `forbidden` without `agent:write`, `not_found` for an unknown agent, or a
/// database error.
pub async fn create_agent_key(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    key: Option<String>,
) -> Result<AgentKeyCreated, ApiError> {
    let params = json!({ "agent_id": agent_id });
    let target = agent_target(agent_id);
    ctx.authorize_human(state, Scope::AgentWrite, Some(&target), None, &params)
        .await?;

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_KEY_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let created: AgentKeyCreated = serde_json::from_value(stored)
                    .map_err(|e| ApiError::internal(&format!("replayed agent key: {e}")))?;
                ctx.audit_replay(state, Scope::AgentWrite, &target, None, &params)
                    .await?;
                return Ok(created);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    state,
                    Some(Scope::AgentWrite),
                    Some(&target),
                    None,
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match insert_agent_key(state, ctx, agent_id, &target, &params).await {
        Ok(created) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&created)
                    .map_err(|e| ApiError::internal(&format!("agent key serialization: {e}")))?;
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_KEY_OPERATION,
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
                    .abandon(ctx.identity.id(), CREATE_KEY_OPERATION, key);
            }
            ctx.audit_error(
                state,
                Some(Scope::AgentWrite),
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

async fn insert_agent_key(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    target: &Target,
    params: &Value,
) -> Result<AgentKeyCreated, ApiError> {
    if !is_id(Prefix::Agent, agent_id) {
        return Err(ApiError::not_found("agent"));
    }
    let mut w = state.store.begin_write().await?;
    let agent: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE id = ?")
        .bind(agent_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if agent.is_none() {
        return Err(ApiError::not_found("agent"));
    }
    let id = new_id(Prefix::AgentKey);
    let secret = format!("{AGENT_KEY_PREFIX}{}", random_secret());
    debug_assert_ne!(id, secret, "an agent key secret is never its id");
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO agent_keys (id, agent_id, key_hash, created_at, revoked_at, last_used_at) \
         VALUES (?, ?, ?, ?, NULL, NULL)",
    )
    .bind(&id)
    .bind(agent_id)
    .bind(sha256_hex(secret.as_bytes()))
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let key_info = AgentKey {
        id: id.clone(),
        agent_id: agent_id.to_string(),
        created_at: now,
        revoked_at: None,
        last_used_at: None,
    };
    // agent_keys is replicated, but the hash is a credential: the change log
    // carries the row without `key_hash`, exactly like `node_join_tokens`.
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "agent_key",
        &id,
        Op::Create,
        agent_key_payload(&key_info),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AgentWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(AgentKeyCreated {
        key_info,
        key: secret,
    })
}

fn agent_key_payload(key: &AgentKey) -> Value {
    json!({
        "id": key.id,
        "agent_id": key.agent_id,
        "created_at": key.created_at,
        "revoked_at": key.revoked_at,
        "last_used_at": key.last_used_at,
    })
}

/// Revokes an agent key. Repeating the call on an already-revoked key is a
/// no-op that still answers 204 and records the call.
///
/// # Errors
///
/// `forbidden` without `agent:write`, `not_found` for an unknown key or a key
/// of another agent, or a database error.
pub async fn revoke_agent_key(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    key_id: &str,
) -> Result<(), ApiError> {
    let params = json!({ "agent_id": agent_id, "key_id": key_id });
    let target = agent_target(agent_id);
    ctx.authorize_human(state, Scope::AgentWrite, Some(&target), None, &params)
        .await?;

    match revoke_agent_key_tx(state, ctx, agent_id, key_id, &target, &params).await {
        Ok(()) => Ok(()),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AgentWrite),
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

async fn revoke_agent_key_tx(
    state: &AppState,
    ctx: &CallCtx,
    agent_id: &str,
    key_id: &str,
    target: &Target,
    params: &Value,
) -> Result<(), ApiError> {
    if !is_id(Prefix::Agent, agent_id) || !is_id(Prefix::AgentKey, key_id) {
        return Err(ApiError::not_found("agent key"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(
        "SELECT id, agent_id, created_at, revoked_at, last_used_at FROM agent_keys \
         WHERE id = ? AND agent_id = ?",
    )
    .bind(key_id)
    .bind(agent_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("agent key"))?;
    let key = AgentKey {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        last_used_at: row.try_get("last_used_at")?,
    };
    if key.revoked_at.is_some() {
        // Idempotent: nothing changed, so there is no change-log entry.
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::AgentWrite),
            Some(target),
            None,
            params,
            None,
        )
        .await?;
        w.commit().await?;
        return Ok(());
    }
    let now = now_rfc3339();
    sqlx::query("UPDATE agent_keys SET revoked_at = ? WHERE id = ?")
        .bind(&now)
        .bind(key_id)
        .execute(&mut *w.tx)
        .await?;
    let updated = AgentKey {
        revoked_at: Some(now),
        ..key
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "agent_key",
        key_id,
        Op::Update,
        agent_key_payload(&updated),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AgentWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// Mints a 5-minute access token for an agent key. The token is local: only
/// its hash is stored and no change-log entry is written for it.
///
/// # Errors
///
/// `forbidden` with `agent_disabled` for a disabled agent, `validation_failed`
/// without a justification, or a database error.
pub async fn create_agent_token(
    state: &AppState,
    ctx: &CallCtx,
    key_id: &str,
    token_id: &str,
) -> Result<AccessTokenResponse, ApiError> {
    let agent = ctx
        .identity
        .agent()
        .ok_or_else(|| ApiError::internal(&"agent-token called without an agent identity"))?;
    let target = agent_target(&agent.agent_id);
    let params = json!({});
    if agent.disabled {
        let error = ApiError::forbidden(Reason::AgentDisabled, None);
        ctx.audit_error(state, None, Some(&target), None, &params, &error)
            .await?;
        return Err(error);
    }
    if ctx.justification.is_none() {
        let error = ApiError::validation("Quoro-Justification is required for an agent call");
        ctx.audit_error(state, None, Some(&target), None, &params, &error)
            .await?;
        return Err(error);
    }
    let token = new_secret(Prefix::AccessToken);
    let expires_at = rfc3339_in(crate::auth::ACCESS_TOKEN_TTL_SECONDS);
    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    // The key and the agent are checked again under the write lock: a key
    // revoked or an agent disabled since authentication mints nothing.
    let live: Option<(String,)> = sqlx::query_as(
        "SELECT k.id FROM agent_keys k JOIN agents a ON a.id = k.agent_id \
         WHERE k.id = ? AND k.agent_id = ? AND k.revoked_at IS NULL AND a.disabled = 0",
    )
    .bind(key_id)
    .bind(&agent.agent_id)
    .fetch_optional(&mut *w.tx)
    .await?;
    if live.is_none() {
        return Err(ApiError::unauthenticated());
    }
    sqlx::query(
        "INSERT INTO access_tokens (id, agent_id, key_id, token_hash, expires_at, created_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(token_id)
    .bind(&agent.agent_id)
    .bind(key_id)
    .bind(sha256_hex(token.as_bytes()))
    .bind(&expires_at)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let row = sqlx::query(
        "SELECT id, agent_id, created_at, revoked_at, last_used_at FROM agent_keys WHERE id = ?",
    )
    .bind(key_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(ApiError::unauthenticated)?;
    let key = AgentKey {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        last_used_at: row.try_get("last_used_at")?,
    };
    sqlx::query("UPDATE agent_keys SET last_used_at = ? WHERE id = ?")
        .bind(&now)
        .bind(key_id)
        .execute(&mut *w.tx)
        .await?;
    let updated = AgentKey {
        last_used_at: Some(now),
        ..key
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "agent_key",
        key_id,
        Op::Update,
        agent_key_payload(&updated),
    )
    .await?;
    ctx.append_ok(&mut w.tx, None, Some(&target), None, &params, Some(seq))
        .await?;
    w.commit().await?;
    Ok(AccessTokenResponse {
        access_token: token,
        expires_at,
    })
}
