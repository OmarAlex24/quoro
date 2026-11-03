//! Grant operations (`/api/v1/grants*`, permission-model.md section 6).
//!
//! A grant is the only carrier of agent authority (D-0030). Creating and
//! revoking one is human-only (`grant:write`, section 4.2); reading one is
//! audited for an agent like any other read. [`insert_grant`] is the one
//! insertion path, shared with the incident and approval flows, so every grant
//! has the same change-log and event shape.

use crate::AppState;
use crate::api::auth::{GrantCreator, GrantKind, GrantStatus};
use crate::api::grants::{
    CREATE_OPERATION, CreateGrantRequest, Grant, GrantList, ListGrantsQuery, grant_target,
};
use crate::authz::Selector;
use crate::changelog::{self, Actor, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, NewEvent, Source};
use crate::idempotency::Begin;
use crate::request::CallCtx;
use crate::store::{now_rfc3339, rfc3339_in};
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Every column of [`Grant`], in the order [`grant_from_row`] reads them.
const GRANT_COLUMNS: &str = "id, agent_id, kind, incident_id, approval_id, scopes, selector, \
     expires_at, max_uses, uses, status, on_behalf_of, created_by_type, created_by_id, reason, \
     created_at, revoked_at, revoked_by";

/// One grant to insert, as the incident, approval and standard flows describe
/// it. Validation is the caller's job; this only stores what it is given.
pub(crate) struct NewGrant {
    pub agent_id: String,
    /// `standard`, `incident` or `approval`.
    pub kind: &'static str,
    pub scopes: Vec<Scope>,
    pub selector: Selector,
    /// Fixed-width RFC 3339 UTC, so string comparison orders correctly.
    pub expires_at: String,
    pub max_uses: Option<i64>,
    pub incident_id: Option<String>,
    pub approval_id: Option<String>,
    pub on_behalf_of: String,
    /// `user` or `system`.
    pub created_by_type: &'static str,
    pub created_by_id: String,
    pub reason: String,
}

fn kind_from_str(value: &str) -> Result<GrantKind, ApiError> {
    match value {
        "standard" => Ok(GrantKind::Standard),
        "incident" => Ok(GrantKind::Incident),
        "approval" => Ok(GrantKind::Approval),
        other => Err(ApiError::internal(&format!("unknown grant kind {other}"))),
    }
}

fn status_from_str(value: &str) -> Result<GrantStatus, ApiError> {
    match value {
        "active" => Ok(GrantStatus::Active),
        "expired" => Ok(GrantStatus::Expired),
        "revoked" => Ok(GrantStatus::Revoked),
        "exhausted" => Ok(GrantStatus::Exhausted),
        other => Err(ApiError::internal(&format!("unknown grant status {other}"))),
    }
}

fn creator_from_str(value: &str) -> Result<GrantCreator, ApiError> {
    match value {
        "user" => Ok(GrantCreator::User),
        "system" => Ok(GrantCreator::System),
        other => Err(ApiError::internal(&format!(
            "unknown grant creator {other}"
        ))),
    }
}

/// The wire form of a [`GrantStatus`].
#[must_use]
pub(crate) fn status_str(status: GrantStatus) -> &'static str {
    match status {
        GrantStatus::Active => "active",
        GrantStatus::Expired => "expired",
        GrantStatus::Revoked => "revoked",
        GrantStatus::Exhausted => "exhausted",
    }
}

fn kind_str(kind: GrantKind) -> &'static str {
    match kind {
        GrantKind::Standard => "standard",
        GrantKind::Incident => "incident",
        GrantKind::Approval => "approval",
    }
}

fn creator_str(creator: GrantCreator) -> &'static str {
    match creator {
        GrantCreator::User => "user",
        GrantCreator::System => "system",
    }
}

/// The status a grant reads as now (section 6.2): a stored `active` grant past
/// its TTL is `expired`, and one out of uses is `exhausted`. `expired` and
/// `revoked` are terminal and stay as stored.
fn effective_status(
    stored: &str,
    expires_at: &str,
    uses: i64,
    max_uses: Option<i64>,
    now: &str,
) -> String {
    if stored == "active" {
        if expires_at <= now {
            return "expired".to_string();
        }
        if max_uses.is_some_and(|max| uses >= max) {
            return "exhausted".to_string();
        }
    }
    stored.to_string()
}

/// Builds a [`Grant`] from a row selected with [`GRANT_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns or enum values do not parse.
fn grant_from_row(row: &sqlx::sqlite::SqliteRow, now: &str) -> Result<Grant, ApiError> {
    let scopes: String = row.try_get("scopes")?;
    let selector: String = row.try_get("selector")?;
    let uses: i64 = row.try_get("uses")?;
    let max_uses: Option<i64> = row.try_get("max_uses")?;
    let expires_at: String = row.try_get("expires_at")?;
    let status: String = row.try_get("status")?;
    Ok(Grant {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        kind: kind_from_str(row.try_get("kind")?)?,
        incident_id: row.try_get("incident_id")?,
        approval_id: row.try_get("approval_id")?,
        scopes: serde_json::from_str(&scopes)
            .map_err(|e| ApiError::internal(&format!("grant scopes: {e}")))?,
        // A stored selector the type cannot read matches nothing.
        selector: serde_json::from_str(&selector).unwrap_or_default(),
        expires_at: expires_at.clone(),
        max_uses,
        uses,
        status: status_from_str(&effective_status(&status, &expires_at, uses, max_uses, now))?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        created_by_type: creator_from_str(row.try_get("created_by_type")?)?,
        created_by_id: row.try_get("created_by_id")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        revoked_by: row.try_get("revoked_by")?,
    })
}

/// The change-log payload of a grant: the whole row, so a replica rebuilds the
/// table by replaying the log (D-0013).
pub(crate) fn grant_payload(grant: &Grant) -> Value {
    json!({
        "id": grant.id,
        "agent_id": grant.agent_id,
        "kind": kind_str(grant.kind),
        "incident_id": grant.incident_id,
        "approval_id": grant.approval_id,
        "scopes": grant.scopes,
        "selector": grant.selector,
        "expires_at": grant.expires_at,
        "max_uses": grant.max_uses,
        "uses": grant.uses,
        "status": status_str(grant.status),
        "on_behalf_of": grant.on_behalf_of,
        "created_by_type": creator_str(grant.created_by_type),
        "created_by_id": grant.created_by_id,
        "reason": grant.reason,
        "created_at": grant.created_at,
        "revoked_at": grant.revoked_at,
        "revoked_by": grant.revoked_by,
    })
}

/// The `grant.created` payload (`events.schema.json` `data_grant_created`).
fn created_event(kind: &str, grant: &Grant) -> Value {
    let mut data = json!({
        "grant_id": grant.id,
        "agent_id": grant.agent_id,
        "kind": kind,
        "scopes": grant.scopes,
        "selector": grant.selector,
        "expires_at": grant.expires_at,
        "on_behalf_of": grant.on_behalf_of,
    });
    if let Some(max_uses) = grant.max_uses {
        data["max_uses"] = json!(max_uses);
    }
    if let Some(incident_id) = &grant.incident_id {
        data["incident_id"] = json!(incident_id);
    }
    if let Some(approval_id) = &grant.approval_id {
        data["approval_id"] = json!(approval_id);
    }
    data
}

/// Inserts one grant, appends its change-log entry and emits `grant.created`,
/// all in the caller's transaction. It does not validate and does not audit:
/// the caller does both, so the incident and approval flows (later tasks) can
/// reuse it unchanged. Returns the grant and the change-log `seq`.
///
/// # Errors
///
/// A database error.
pub(crate) async fn insert_grant(
    conn: &mut SqliteConnection,
    actor: &Actor,
    request_id: Option<&str>,
    new: NewGrant,
) -> Result<(Grant, i64), ApiError> {
    let id = new_id(Prefix::Grant);
    let now = now_rfc3339();
    let scopes: Vec<String> = new.scopes.iter().map(|s| s.as_str().to_string()).collect();
    let scopes_json = serde_json::to_string(&scopes)
        .map_err(|e| ApiError::internal(&format!("grant scopes: {e}")))?;
    let selector_json = serde_json::to_string(&new.selector)
        .map_err(|e| ApiError::internal(&format!("grant selector: {e}")))?;
    sqlx::query(
        "INSERT INTO grants (id, agent_id, kind, scopes, selector, expires_at, max_uses, uses, status, \
         incident_id, approval_id, on_behalf_of, created_by_type, created_by_id, reason, created_at, \
         revoked_at, revoked_by) \
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'active', ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
    )
    .bind(&id)
    .bind(&new.agent_id)
    .bind(new.kind)
    .bind(&scopes_json)
    .bind(&selector_json)
    .bind(&new.expires_at)
    .bind(new.max_uses)
    .bind(&new.incident_id)
    .bind(&new.approval_id)
    .bind(&new.on_behalf_of)
    .bind(new.created_by_type)
    .bind(&new.created_by_id)
    .bind(&new.reason)
    .bind(&now)
    .execute(&mut *conn)
    .await?;
    let grant = Grant {
        id: id.clone(),
        agent_id: new.agent_id,
        kind: kind_from_str(new.kind)?,
        incident_id: new.incident_id,
        approval_id: new.approval_id,
        scopes,
        selector: new.selector,
        expires_at: new.expires_at,
        max_uses: new.max_uses,
        uses: 0,
        status: GrantStatus::Active,
        on_behalf_of: new.on_behalf_of,
        created_by_type: creator_from_str(new.created_by_type)?,
        created_by_id: new.created_by_id,
        reason: new.reason,
        created_at: now,
        revoked_at: None,
        revoked_by: None,
    };
    let seq =
        changelog::append(conn, actor, "grant", &id, Op::Create, grant_payload(&grant)).await?;
    events::emit(
        conn,
        NewEvent {
            kind: "grant.created",
            source: Source::ControlPlane,
            subject_type: "grant",
            subject_id: &id,
            project_id: None,
            request_id,
            actor: Some(actor),
            data: created_event(new.kind, &grant),
        },
    )
    .await?;
    Ok((grant, seq))
}

async fn read_grant(state: &AppState, grant_id: &str) -> Result<Grant, ApiError> {
    let row = sqlx::query(&format!("SELECT {GRANT_COLUMNS} FROM grants WHERE id = ?"))
        .bind(grant_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("grant"))?;
    grant_from_row(&row, &now_rfc3339())
}

/// The fixed-width UTC form of a parsed timestamp, the same shape
/// [`now_rfc3339`] produces so stored timestamps compare correctly as strings.
fn fixed_rfc3339(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
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

fn valid_slug(slug: &str) -> bool {
    (1..=40).contains(&slug.len())
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Section 6.6: everything the grant endpoint refuses with `validation_failed`.
/// Returns the parsed scopes, the selector and the normalized `expires_at`.
fn validate(body: &CreateGrantRequest) -> Result<(Vec<Scope>, Selector, String), ApiError> {
    let mut scopes = Vec::with_capacity(body.scopes.len());
    for raw in &body.scopes {
        if raw.contains('*') {
            return Err(ApiError::validation(format!(
                "wildcard scope {raw} is not allowed; list scopes explicitly"
            )));
        }
        let scope = Scope::parse(raw)
            .ok_or_else(|| ApiError::validation(format!("unknown scope {raw}")))?;
        if scope.is_human_only() {
            return Err(ApiError::validation(format!(
                "scope {raw} is human-only and cannot be granted to an agent"
            )));
        }
        if scope.is_destructive() {
            return Err(ApiError::validation(format!(
                "scope {raw} is destructive: a standard grant cannot carry it, ask for approval instead"
            )));
        }
        scopes.push(scope);
    }

    let selector: Selector = serde_json::from_value(body.selector.clone())
        .map_err(|e| ApiError::validation(format!("selector is not the documented shape: {e}")))?;
    for slug in selector.projects.iter().flatten() {
        if !valid_slug(slug) {
            return Err(ApiError::validation(format!(
                "selector project {slug:?} is not a valid project slug"
            )));
        }
    }
    for app_id in selector.apps.iter().flatten() {
        if !crate::deploy::is_id_with_prefix("app", app_id) {
            return Err(ApiError::validation(format!(
                "selector app {app_id:?} is not a valid app id"
            )));
        }
    }
    for node_id in selector.nodes.iter().flatten() {
        if !is_id(Prefix::Node, node_id) {
            return Err(ApiError::validation(format!(
                "selector node {node_id:?} is not a valid node id"
            )));
        }
    }

    let expires_at = match (&body.expires_at, body.ttl_s) {
        (Some(_), Some(_)) => {
            return Err(ApiError::validation(
                "give either expires_at or ttl_s, not both",
            ));
        }
        (None, None) => {
            return Err(ApiError::validation(
                "a grant must expire: give expires_at or ttl_s",
            ));
        }
        (Some(at), None) => {
            let parsed = OffsetDateTime::parse(at, &Rfc3339).map_err(|e| {
                ApiError::validation(format!("expires_at is not an RFC 3339 timestamp: {e}"))
            })?;
            if parsed <= OffsetDateTime::now_utc() {
                return Err(ApiError::validation("expires_at is in the past"));
            }
            fixed_rfc3339(parsed)
        }
        (None, Some(ttl_s)) => {
            if ttl_s < 1 {
                return Err(ApiError::validation("ttl_s must be at least 1"));
            }
            rfc3339_in(ttl_s)
        }
    };

    if body.max_uses.is_some_and(|max| max < 1) {
        return Err(ApiError::validation("max_uses must be at least 1"));
    }
    if body.reason.trim().is_empty() {
        return Err(ApiError::validation(
            "reason must say why the agent needs the grant",
        ));
    }

    Ok((scopes, selector, expires_at))
}

/// Creates a standard grant (section 6.6). `grant:write` is human-only, so an
/// agent caller is refused with `human_only` before the body is looked at.
///
/// # Errors
///
/// `validation_failed` for a scope, selector or expiry the contract refuses,
/// `forbidden` without `grant:write`, `not_found` for an unknown agent,
/// `conflict` for a reused idempotency key, or a database error.
pub async fn create_grant(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateGrantRequest,
    key: Option<String>,
) -> Result<Grant, ApiError> {
    let params = serde_json::to_value(body)
        .map_err(|e| ApiError::internal(&format!("grant params: {e}")))?;
    ctx.authorize_human(state, Scope::GrantWrite, None, None, &params)
        .await?;

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let id = stored["id"]
                    .as_str()
                    .ok_or_else(|| ApiError::internal(&"replayed grant has no id"))?
                    .to_string();
                let grant = read_grant(state, &id).await?;
                ctx.audit_replay(state, Scope::GrantWrite, &grant_target(&id), None, &params)
                    .await?;
                return Ok(grant);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(state, Some(Scope::GrantWrite), None, None, &params, &error)
                    .await?;
                return Err(error);
            }
        }
    }

    let validated = match validate(body) {
        Ok(validated) => validated,
        Err(error) => {
            ctx.audit_error(state, Some(Scope::GrantWrite), None, None, &params, &error)
                .await?;
            return Err(error);
        }
    };

    match insert_grant_tx(state, ctx, body, validated, &params).await {
        Ok(grant) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&grant)
                    .map_err(|e| ApiError::internal(&format!("grant serialization: {e}")))?;
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(grant)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_OPERATION, key);
            }
            ctx.audit_error(state, Some(Scope::GrantWrite), None, None, &params, &error)
                .await?;
            Err(error)
        }
    }
}

async fn insert_grant_tx(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateGrantRequest,
    validated: (Vec<Scope>, Selector, String),
    params: &Value,
) -> Result<Grant, ApiError> {
    let (scopes, selector, expires_at) = validated;
    let mut w = state.store.begin_write().await?;
    let agent: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE id = ?")
        .bind(&body.agent_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if agent.is_none() {
        return Err(ApiError::not_found("agent"));
    }
    let new = NewGrant {
        agent_id: body.agent_id.clone(),
        kind: "standard",
        scopes,
        selector,
        expires_at,
        max_uses: body.max_uses,
        incident_id: None,
        approval_id: None,
        on_behalf_of: ctx.identity.id().to_string(),
        created_by_type: "user",
        created_by_id: ctx.identity.id().to_string(),
        reason: body.reason.trim().to_string(),
    };
    let (grant, seq) =
        insert_grant(&mut w.tx, &ctx.identity.actor(), Some(&ctx.request_id), new).await?;
    let target = grant_target(&grant.id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::GrantWrite),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(grant)
}

/// Lists grants one page at a time. The `status` filter uses the effective
/// status, so a stored `active` grant past its TTL is found as `expired`.
///
/// # Errors
///
/// `forbidden` without `grant:read`, `validation_failed` for a bad filter, or a
/// database error.
pub async fn list_grants(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListGrantsQuery,
) -> Result<GrantList, ApiError> {
    let params = json!({
        "agent_id": query.agent_id,
        "status": query.status.map(|s| s.as_str()),
        "kind": query.kind.map(|k| k.as_str()),
    });
    let access = ctx
        .authorize_read(state, Scope::GrantRead, None, &params)
        .await?;
    let result: Result<GrantList, ApiError> = async {
        if let Some(agent_id) = &query.agent_id
            && !is_id(Prefix::Agent, agent_id)
        {
            return Err(ApiError::validation("agent_id is not a valid agent id"));
        }
        let limit = query.limit()?;
        if access.reads_nothing() {
            return Ok(GrantList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let mut sql = format!("SELECT {GRANT_COLUMNS} FROM grants WHERE id > ?");
        if query.agent_id.is_some() {
            sql.push_str(" AND agent_id = ?");
        }
        if query.kind.is_some() {
            sql.push_str(" AND kind = ?");
        }
        sql.push_str(" ORDER BY id");
        let mut statement = sqlx::query(&sql).bind(query.after());
        if let Some(agent_id) = &query.agent_id {
            statement = statement.bind(agent_id);
        }
        if let Some(kind) = query.kind {
            statement = statement.bind(kind.as_str());
        }
        let rows = statement.fetch_all(state.store.pool()).await?;
        let now = now_rfc3339();
        let grants = rows
            .iter()
            .map(|row| grant_from_row(row, &now))
            .collect::<Result<Vec<_>, _>>()?;
        // The effective status is not a column, so filter before paginating:
        // the cursor then comes from the filtered rows and stays correct.
        let filtered: Vec<Grant> = grants
            .into_iter()
            .filter(|grant| {
                query
                    .status
                    .is_none_or(|status| status_str(grant.status) == status.as_str())
            })
            .collect();
        let (items, next_cursor) =
            crate::api::page::finish(filtered, limit, |grant| grant.id.clone());
        Ok(GrantList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One grant by id.
///
/// # Errors
///
/// `forbidden` without `grant:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_grant(state: &AppState, ctx: &CallCtx, grant_id: &str) -> Result<Grant, ApiError> {
    let params = json!({ "grant_id": grant_id });
    let target = grant_target(grant_id);
    let access = ctx
        .authorize_read(state, Scope::GrantRead, Some(&target), &params)
        .await?;
    let result: Result<Grant, ApiError> = async {
        if !is_id(Prefix::Grant, grant_id) {
            return Err(ApiError::not_found("grant"));
        }
        read_grant(state, grant_id).await
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Revokes a grant. Revocation is checked on every request, so the agent's
/// next call is refused with `grant_revoked` (section 6.5).
///
/// # Errors
///
/// `forbidden` without `grant:write` (agents get `human_only`), `not_found` for
/// an unknown id, `conflict` when the grant is not active, or a database error.
pub async fn revoke_grant(
    state: &AppState,
    ctx: &CallCtx,
    grant_id: &str,
) -> Result<Grant, ApiError> {
    let params = json!({ "grant_id": grant_id });
    let target = grant_target(grant_id);
    ctx.authorize_human(state, Scope::GrantWrite, Some(&target), None, &params)
        .await?;
    match revoke_grant_tx(state, ctx, grant_id, &target, &params).await {
        Ok(grant) => Ok(grant),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::GrantWrite),
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

async fn revoke_grant_tx(
    state: &AppState,
    ctx: &CallCtx,
    grant_id: &str,
    target: &Target,
    params: &Value,
) -> Result<Grant, ApiError> {
    if !is_id(Prefix::Grant, grant_id) {
        return Err(ApiError::not_found("grant"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!("SELECT {GRANT_COLUMNS} FROM grants WHERE id = ?"))
        .bind(grant_id)
        .fetch_optional(&mut *w.tx)
        .await?
        .ok_or_else(|| ApiError::not_found("grant"))?;
    let now = now_rfc3339();
    let mut grant = grant_from_row(&row, &now)?;
    if status_str(grant.status) != "active" {
        return Err(ApiError::conflict(format!(
            "grant {grant_id} is {} and cannot be revoked",
            status_str(grant.status)
        )));
    }
    let revoked_by = ctx.identity.id().to_string();
    let revoked_at = now;
    sqlx::query(
        "UPDATE grants SET status = 'revoked', revoked_at = ?, revoked_by = ? WHERE id = ?",
    )
    .bind(&revoked_at)
    .bind(&revoked_by)
    .bind(grant_id)
    .execute(&mut *w.tx)
    .await?;
    grant.status = GrantStatus::Revoked;
    grant.revoked_at = Some(revoked_at.clone());
    grant.revoked_by = Some(revoked_by.clone());
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "grant",
        grant_id,
        Op::Update,
        grant_payload(&grant),
    )
    .await?;
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "grant.revoked",
            source: Source::ControlPlane,
            subject_type: "grant",
            subject_id: grant_id,
            project_id: None,
            request_id: Some(&ctx.request_id),
            actor: None,
            data: json!({
                "grant_id": grant_id,
                "agent_id": grant.agent_id,
                "revoked_by": revoked_by,
            }),
        },
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::GrantWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(grant)
}

/// Revokes every active grant issued for an incident, as the system, in the
/// caller's transaction (section 8.3). `revoked_by` stays NULL because no human
/// did it; the event names the grant's `on_behalf_of`, the human the incident
/// grant borrowed its authority from. The caller audits the resolution.
///
/// # Errors
///
/// A database error.
pub(crate) async fn revoke_for_incident(
    conn: &mut SqliteConnection,
    incident_id: &str,
    request_id: Option<&str>,
) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query("SELECT id, agent_id, on_behalf_of FROM grants WHERE incident_id = ? AND status = 'active' ORDER BY id")
        .bind(incident_id)
        .fetch_all(&mut *conn)
        .await?;
    let actor = Actor::system("grants");
    let now = now_rfc3339();
    let mut revoked = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.try_get("id")?;
        let agent_id: String = row.try_get("agent_id")?;
        let on_behalf_of: String = row.try_get("on_behalf_of")?;
        sqlx::query(
            "UPDATE grants SET status = 'revoked', revoked_at = ?, revoked_by = NULL WHERE id = ?",
        )
        .bind(&now)
        .bind(&id)
        .execute(&mut *conn)
        .await?;
        changelog::append(
            conn,
            &actor,
            "grant",
            &id,
            Op::Update,
            json!({
                "id": id,
                "status": "revoked",
                "revoked_at": now,
                "revoked_by": Value::Null,
            }),
        )
        .await?;
        events::emit(
            conn,
            NewEvent {
                kind: "grant.revoked",
                source: Source::ControlPlane,
                subject_type: "grant",
                subject_id: &id,
                project_id: None,
                request_id,
                actor: Some(&actor),
                data: json!({
                    "grant_id": id,
                    "agent_id": agent_id,
                    "revoked_by": on_behalf_of,
                    "reason": "incident resolved",
                }),
            },
        )
        .await?;
        revoked.push(id);
    }
    Ok(revoked)
}

/// Marks every stored `active` grant whose TTL has passed as `expired`, with a
/// change-log entry and `grant.expired` (`reason: ttl`) for each, in one
/// transaction. The grant sweeper calls it every 15 s; it returns how many
/// grants it expired.
///
/// # Errors
///
/// A database error.
pub async fn expire_due(state: &AppState) -> Result<usize, ApiError> {
    let mut w = state.store.begin_write().await?;
    let now = now_rfc3339();
    let rows = sqlx::query(
        "SELECT id, agent_id FROM grants WHERE status = 'active' AND expires_at <= ? ORDER BY id",
    )
    .bind(&now)
    .fetch_all(&mut *w.tx)
    .await?;
    let actor = Actor::system("grants");
    let mut expired = 0;
    for row in rows {
        let id: String = row.try_get("id")?;
        let agent_id: String = row.try_get("agent_id")?;
        sqlx::query("UPDATE grants SET status = 'expired' WHERE id = ?")
            .bind(&id)
            .execute(&mut *w.tx)
            .await?;
        changelog::append(
            &mut w.tx,
            &actor,
            "grant",
            &id,
            Op::Update,
            json!({ "id": id, "status": "expired" }),
        )
        .await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "grant.expired",
                source: Source::ControlPlane,
                subject_type: "grant",
                subject_id: &id,
                project_id: None,
                request_id: None,
                actor: Some(&actor),
                data: json!({
                    "grant_id": id,
                    "agent_id": agent_id,
                    "reason": "ttl",
                }),
            },
        )
        .await?;
        expired += 1;
    }
    w.commit().await?;
    Ok(expired)
}

/// The grants that authorize something right now: `active`, not past their
/// TTL and with uses left. `GET /api/v1/auth/me` lists them for an agent.
///
/// # Errors
///
/// A database error.
pub async fn active_grants_for_agent(
    state: &AppState,
    agent_id: &str,
) -> Result<Vec<Grant>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {GRANT_COLUMNS} FROM grants WHERE agent_id = ? ORDER BY id"
    ))
    .bind(agent_id)
    .fetch_all(state.store.pool())
    .await?;
    let now = now_rfc3339();
    let grants = rows
        .iter()
        .map(|row| grant_from_row(row, &now))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(grants
        .into_iter()
        .filter(|grant| status_str(grant.status) == "active")
        .collect())
}
