//! Approval operations (`/api/v1/approvals*`, permission-model.md section 9).
//!
//! A destructive scope is unreachable through a standing grant: an agent that
//! needs one files a request naming the scope, the target, the operation and
//! the exact params. A human with `approval:decide` approves it, and the
//! approval issues exactly one short-lived grant (`kind` `approval`,
//! `max_uses` 1, TTL 10 minutes) scoped to that target. The grant matches the
//! call only when the target and the params hash agree, so approving a
//! harmless call cannot be turned into a different one (D-0030).

use crate::AppState;
use crate::api::approvals::{
    Approval, ApprovalList, ApprovalStatus, ApprovalStatusFilter, CREATE_OPERATION,
    CreateApprovalRequest, DecisionRequest, ListApprovalsQuery, approval_target,
};
use crate::audit::{self, Outcome};
use crate::authz::{self, Selector};
use crate::changelog::{self, Actor, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, NewEvent, Source};
use crate::idempotency::Begin;
use crate::request::{CallCtx, Resource};
use crate::service::grants::{self, NewGrant};
use crate::store::{now_rfc3339, rfc3339_in};
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};

/// Every column of [`Approval`], in the order [`approval_from_row`] reads them.
const APPROVAL_COLUMNS: &str = "id, agent_id, scope, target, operation, params, params_hash, \
     justification, incident_id, status, expires_at, decided_by, decided_at, decision_note, \
     grant_id, created_at";

fn status_from_str(value: &str) -> Result<ApprovalStatus, ApiError> {
    match value {
        "pending" => Ok(ApprovalStatus::Pending),
        "approved" => Ok(ApprovalStatus::Approved),
        "denied" => Ok(ApprovalStatus::Denied),
        "expired" => Ok(ApprovalStatus::Expired),
        other => Err(ApiError::internal(&format!(
            "unknown approval status {other}"
        ))),
    }
}

/// Builds an [`Approval`] from a row selected with [`APPROVAL_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns or enum values do not parse.
fn approval_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Approval, ApiError> {
    let target: String = row.try_get("target")?;
    let params: String = row.try_get("params")?;
    Ok(Approval {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        scope: row.try_get("scope")?,
        target: serde_json::from_str(&target)
            .map_err(|e| ApiError::internal(&format!("approval target: {e}")))?,
        operation: row.try_get("operation")?,
        params: serde_json::from_str(&params)
            .map_err(|e| ApiError::internal(&format!("approval params: {e}")))?,
        params_hash: row.try_get("params_hash")?,
        justification: row.try_get("justification")?,
        incident_id: row.try_get("incident_id")?,
        status: status_from_str(row.try_get("status")?)?,
        expires_at: row.try_get("expires_at")?,
        decided_by: row.try_get("decided_by")?,
        decided_at: row.try_get("decided_at")?,
        decision_note: row.try_get("decision_note")?,
        grant_id: row.try_get("grant_id")?,
        created_at: row.try_get("created_at")?,
    })
}

/// The change-log payload of an approval: the whole row, so a replica rebuilds
/// the table by replaying the log (D-0013).
fn approval_payload(approval: &Approval) -> Value {
    json!({
        "id": approval.id,
        "agent_id": approval.agent_id,
        "scope": approval.scope,
        "target": approval.target,
        "operation": approval.operation,
        "params": approval.params,
        "params_hash": approval.params_hash,
        "justification": approval.justification,
        "incident_id": approval.incident_id,
        "status": approval.status.as_str(),
        "expires_at": approval.expires_at,
        "decided_by": approval.decided_by,
        "decided_at": approval.decided_at,
        "decision_note": approval.decision_note,
        "grant_id": approval.grant_id,
        "created_at": approval.created_at,
    })
}

async fn read_approval(state: &AppState, approval_id: &str) -> Result<Approval, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {APPROVAL_COLUMNS} FROM approvals WHERE id = ?"
    ))
    .bind(approval_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("approval"))?;
    approval_from_row(&row)
}

/// Reads the `{type, id}` target the way the body and the stored column carry
/// it. `error::Target` is a wire type without `Deserialize`, so the JSON is
/// read field by field.
fn target_of(value: &Value) -> Target {
    Target {
        kind: value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        id: value
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    }
}

/// The `prj_` id the approval target belongs to, recorded on the event and
/// the audit record when known.
async fn target_project_id(
    conn: &mut SqliteConnection,
    target: &Target,
) -> Result<Option<String>, ApiError> {
    let row: Option<(String,)> =
        match target.kind.as_str() {
            "app" => {
                sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(&mut *conn)
                    .await?
            }
            "vm" => {
                sqlx::query_as("SELECT project_id FROM vms WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(&mut *conn)
                    .await?
            }
            "volume" => sqlx::query_as(
                "SELECT a.project_id FROM volumes v JOIN apps a ON a.id = v.app_id WHERE v.id = ?",
            )
            .bind(&target.id)
            .fetch_optional(&mut *conn)
            .await?,
            "backup" => sqlx::query_as(
                "SELECT a.project_id FROM backups b JOIN apps a ON a.id = b.app_id WHERE b.id = ?",
            )
            .bind(&target.id)
            .fetch_optional(&mut *conn)
            .await?,
            "project" => {
                sqlx::query_as("SELECT id FROM projects WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(&mut *conn)
                    .await?
            }
            _ => None,
        };
    Ok(row.map(|r| r.0))
}

/// Whether the target names a row that exists.
async fn target_exists(state: &AppState, target: &Target) -> Result<bool, ApiError> {
    let sql = match target.kind.as_str() {
        "app" => "SELECT id FROM apps WHERE id = ?",
        "vm" => "SELECT id FROM vms WHERE id = ?",
        "volume" => "SELECT id FROM volumes WHERE id = ?",
        "backup" => "SELECT id FROM backups WHERE id = ?",
        "project" => "SELECT id FROM projects WHERE id = ?",
        _ => return Ok(false),
    };
    let row: Option<(String,)> = sqlx::query_as(sql)
        .bind(&target.id)
        .fetch_optional(state.store.pool())
        .await?;
    Ok(row.is_some())
}

/// Everything the approval endpoint refuses with `validation_failed`
/// (section 9, step 3): one destructive scope, a known target, a non-empty
/// operation, an object of params, a justification and a known incident.
async fn validate(state: &AppState, body: &CreateApprovalRequest) -> Result<Scope, ApiError> {
    let scope = Scope::parse(&body.scope)
        .ok_or_else(|| ApiError::validation(format!("unknown scope {}", body.scope)))?;
    if scope.is_human_only() {
        return Err(ApiError::validation(format!(
            "scope {} is human-only: an approval grant can never carry it",
            body.scope
        )));
    }
    if !scope.is_destructive() {
        return Err(ApiError::validation(format!(
            "scope {} is not destructive: an agent does not need approval for it",
            body.scope
        )));
    }
    let target = target_of(&body.target);
    if !matches!(
        target.kind.as_str(),
        "app" | "vm" | "project" | "volume" | "backup"
    ) {
        return Err(ApiError::validation(format!(
            "target.type {:?} is not app, vm, project, volume or backup",
            target.kind
        )));
    }
    if target.id.is_empty() {
        return Err(ApiError::validation("target.id is required"));
    }
    if !target_exists(state, &target).await? {
        return Err(ApiError::not_found(&target.kind));
    }
    if body.operation.trim().is_empty() {
        return Err(ApiError::validation(
            "operation must name the call the request is for",
        ));
    }
    if !body.params.is_object() {
        return Err(ApiError::validation("params must be a JSON object"));
    }
    let length = body.justification.chars().count();
    if !(1..=2000).contains(&length) {
        return Err(ApiError::validation(
            "justification must be between 1 and 2000 characters",
        ));
    }
    if let Some(incident_id) = &body.incident_id {
        let row: Option<(String,)> = sqlx::query_as("SELECT id FROM incidents WHERE id = ?")
            .bind(incident_id)
            .fetch_optional(state.store.pool())
            .await?;
        if row.is_none() {
            return Err(ApiError::not_found("incident"));
        }
    }
    Ok(scope)
}

/// Files an approval request (section 9, step 3). `approval:create` is the
/// agent's own scope: it does not let the agent do anything destructive, it
/// lets it ask. A human that holds `approval:create` is told it needs none.
///
/// # Errors
///
/// `validation_failed` for a body the contract refuses, `forbidden` without
/// `approval:create`, `not_found` for an unknown target or incident,
/// `conflict` for a reused idempotency key, or a database error.
pub async fn create_approval(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateApprovalRequest,
    key: Option<String>,
) -> Result<Approval, ApiError> {
    let params = serde_json::to_value(body)
        .map_err(|e| ApiError::internal(&format!("approval params: {e}")))?;
    let requested_target = target_of(&body.target);

    // The body's justification is the agent's reason for the call the approval
    // is for; when the `Quoro-Justification` header is absent (the MCP tool
    // passes it as an argument instead), it is the justification the mutating
    // call records. The body's incident_id falls back the same way.
    let mut effective = ctx.clone();
    if effective.justification.is_none() {
        effective.justification = Some(body.justification.clone());
    }
    if effective.incident_id.is_none() {
        effective.incident_id = body.incident_id.clone();
    }

    // A human's role decides `approval:create`; holding it means the human can
    // already do the destructive call themselves.
    if effective.identity.is_human() {
        effective
            .authorize(
                state,
                Scope::ApprovalCreate,
                Some(&requested_target),
                None,
                &params,
            )
            .await?;
        let error = ApiError::validation("a human does not need approval");
        effective
            .audit_error(
                state,
                Some(Scope::ApprovalCreate),
                Some(&requested_target),
                None,
                &params,
                &error,
            )
            .await?;
        return Err(error);
    }

    // The agent's `approval:create` grant must cover the target it asks about.
    effective
        .authorize(
            state,
            Scope::ApprovalCreate,
            Some(&requested_target),
            None,
            &params,
        )
        .await?;

    let scope = match validate(state, body).await {
        Ok(scope) => scope,
        Err(error) => {
            effective
                .audit_error(
                    state,
                    Some(Scope::ApprovalCreate),
                    Some(&requested_target),
                    None,
                    &params,
                    &error,
                )
                .await?;
            return Err(error);
        }
    };

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(effective.identity.id(), CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let id = stored["id"]
                    .as_str()
                    .ok_or_else(|| ApiError::internal(&"replayed approval has no id"))?
                    .to_string();
                let approval = read_approval(state, &id).await?;
                effective
                    .audit_replay(
                        state,
                        Scope::ApprovalCreate,
                        &approval_target(&id),
                        None,
                        &params,
                    )
                    .await?;
                return Ok(approval);
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                effective
                    .audit_error(
                        state,
                        Some(Scope::ApprovalCreate),
                        Some(&requested_target),
                        None,
                        &params,
                        &error,
                    )
                    .await?;
                return Err(error);
            }
        }
    }

    match insert_approval_tx(state, &effective, body, scope, &params).await {
        Ok(approval) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&approval)
                    .map_err(|e| ApiError::internal(&format!("approval serialization: {e}")))?;
                state.idempotency.finish(
                    effective.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(approval)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(effective.identity.id(), CREATE_OPERATION, key);
            }
            effective
                .audit_error(
                    state,
                    Some(Scope::ApprovalCreate),
                    Some(&requested_target),
                    None,
                    &params,
                    &error,
                )
                .await?;
            Err(error)
        }
    }
}

async fn insert_approval_tx(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateApprovalRequest,
    scope: Scope,
    params: &Value,
) -> Result<Approval, ApiError> {
    let id = new_id(Prefix::Approval);
    let now = now_rfc3339();
    let expires_at = rfc3339_in(3600);
    let target_json = serde_json::to_string(&body.target)
        .map_err(|e| ApiError::internal(&format!("approval target: {e}")))?;
    let target = target_of(&body.target);
    let params_json = serde_json::to_string(&body.params)
        .map_err(|e| ApiError::internal(&format!("approval params: {e}")))?;
    let params_hash = authz::params_hash(&body.params)?;
    let justification = body.justification.trim().to_string();
    let mut w = state.store.begin_write().await?;
    let project_id = target_project_id(&mut w.tx, &target).await?;
    sqlx::query(
        "INSERT INTO approvals (id, agent_id, scope, target, operation, params, params_hash, \
         justification, incident_id, status, expires_at, decided_by, decided_at, decision_note, \
         grant_id, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL, NULL, ?)",
    )
    .bind(&id)
    .bind(ctx.identity.id())
    .bind(scope.as_str())
    .bind(&target_json)
    .bind(body.operation.trim())
    .bind(&params_json)
    .bind(&params_hash)
    .bind(&justification)
    .bind(&body.incident_id)
    .bind(&expires_at)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let approval = Approval {
        id: id.clone(),
        agent_id: ctx.identity.id().to_string(),
        scope: scope.as_str().to_string(),
        target: body.target.clone(),
        operation: body.operation.trim().to_string(),
        params: body.params.clone(),
        params_hash,
        justification,
        incident_id: body.incident_id.clone(),
        status: ApprovalStatus::Pending,
        expires_at: expires_at.clone(),
        decided_by: None,
        decided_at: None,
        decision_note: None,
        grant_id: None,
        created_at: now,
    };
    let actor = ctx.identity.actor();
    let seq = changelog::append(
        &mut w.tx,
        &actor,
        "approval",
        &id,
        Op::Create,
        approval_payload(&approval),
    )
    .await?;
    let mut data = json!({
        "approval_id": approval.id,
        "agent_id": approval.agent_id,
        "scope": approval.scope,
        "target": approval.target,
        "operation": approval.operation,
        "justification": approval.justification,
        "expires_at": approval.expires_at,
    });
    if let Some(incident_id) = &approval.incident_id {
        data["incident_id"] = json!(incident_id);
    }
    events::emit(
        &mut w.tx,
        NewEvent {
            kind: "approval.requested",
            source: Source::ControlPlane,
            subject_type: "approval",
            subject_id: &id,
            project_id: project_id.as_deref(),
            request_id: Some(&ctx.request_id),
            actor: Some(&actor),
            data,
        },
    )
    .await?;
    let target = approval_target(&id);
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ApprovalCreate),
        Some(&target),
        project_id.as_deref(),
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(approval)
}

/// What an approval row is, for selector matching (section 5: an approval
/// matches through its target).
struct TargetInfo {
    kind: String,
    id: String,
    app_id: Option<String>,
    project_id: Option<String>,
    node_id: Option<String>,
}

/// Resolves the app, project and node an approval target belongs to.
async fn describe(state: &AppState, approval: &Approval) -> Result<TargetInfo, ApiError> {
    let target = target_of(&approval.target);
    let mut info = TargetInfo {
        kind: target.kind.clone(),
        id: target.id.clone(),
        app_id: None,
        project_id: None,
        node_id: None,
    };
    match target.kind.as_str() {
        "app" => {
            let row: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
                .bind(&target.id)
                .fetch_optional(state.store.pool())
                .await?;
            info.app_id = Some(target.id.clone());
            info.project_id = row.map(|r| r.0);
        }
        "vm" => {
            let row: Option<(Option<String>, String, String)> =
                sqlx::query_as("SELECT app_id, project_id, node_id FROM vms WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(state.store.pool())
                    .await?;
            if let Some((app_id, project_id, node_id)) = row {
                info.app_id = app_id;
                info.project_id = Some(project_id);
                info.node_id = Some(node_id);
            }
        }
        // A volume matches through the app that owns it (permission-model.md
        // section 5, "Matching per target type").
        "volume" => {
            let row: Option<(String, String, String)> = sqlx::query_as(
                "SELECT v.app_id, a.project_id, v.node_id FROM volumes v \
                 JOIN apps a ON a.id = v.app_id WHERE v.id = ?",
            )
            .bind(&target.id)
            .fetch_optional(state.store.pool())
            .await?;
            if let Some((app_id, project_id, node_id)) = row {
                info.app_id = Some(app_id);
                info.project_id = Some(project_id);
                info.node_id = Some(node_id);
            }
        }
        // A backup of a volume matches through its app too; a control-plane
        // backup carries no app, so only `{"all": true}` matches it.
        "backup" => {
            let row: Option<(Option<String>, Option<String>, String)> = sqlx::query_as(
                "SELECT b.app_id, a.project_id, b.node_id FROM backups b \
                 LEFT JOIN apps a ON a.id = b.app_id WHERE b.id = ?",
            )
            .bind(&target.id)
            .fetch_optional(state.store.pool())
            .await?;
            if let Some((app_id, project_id, node_id)) = row {
                info.app_id = app_id;
                info.project_id = project_id;
                info.node_id = Some(node_id);
            }
        }
        "project" => info.project_id = Some(target.id.clone()),
        _ => {}
    }
    Ok(info)
}

/// Lists approval requests one page at a time.
///
/// # Errors
///
/// `forbidden` without `approval:read`, `validation_failed` for a bad filter,
/// or a database error.
pub async fn list_approvals(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListApprovalsQuery,
) -> Result<ApprovalList, ApiError> {
    let params = json!({ "status": query.status.map(ApprovalStatusFilter::as_str) });
    let access = ctx
        .authorize_read(state, Scope::ApprovalRead, None, &params)
        .await?;
    let result: Result<ApprovalList, ApiError> = async {
        let limit = query.limit()?;
        if access.reads_nothing() {
            return Ok(ApprovalList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let mut sql = format!("SELECT {APPROVAL_COLUMNS} FROM approvals WHERE id > ?");
        if query.status.is_some() {
            sql.push_str(" AND status = ?");
        }
        sql.push_str(" ORDER BY id LIMIT ?");
        let mut statement = sqlx::query(&sql).bind(query.after());
        if let Some(status) = query.status {
            statement = statement.bind(status.as_str());
        }
        let rows = statement
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let approvals = rows
            .iter()
            .map(approval_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_cursor) =
            crate::api::page::finish(approvals, limit, |approval| approval.id.clone());
        let mut described = Vec::with_capacity(items.len());
        for approval in items {
            let info = describe(state, &approval).await?;
            described.push((approval, info));
        }
        let described = access
            .filter(state, described, |(_, info)| Resource {
                kind: &info.kind,
                id: &info.id,
                app_id: info.app_id.as_deref(),
                project_id: info.project_id.as_deref(),
                node_id: info.node_id.as_deref(),
            })
            .await?;
        let items = described
            .into_iter()
            .map(|(approval, _)| approval)
            .collect();
        Ok(ApprovalList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One approval request by id.
///
/// # Errors
///
/// `forbidden` without `approval:read`, `not_found` for an unknown or
/// malformed id, or a database error.
pub async fn get_approval(
    state: &AppState,
    ctx: &CallCtx,
    approval_id: &str,
) -> Result<Approval, ApiError> {
    let params = json!({ "approval_id": approval_id });
    let target = approval_target(approval_id);
    let access = ctx
        .authorize_read(state, Scope::ApprovalRead, Some(&target), &params)
        .await?;
    let result: Result<Approval, ApiError> = async {
        if !is_id(Prefix::Approval, approval_id) {
            return Err(ApiError::not_found("approval"));
        }
        read_approval(state, approval_id).await
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Approves or denies a pending request (section 9, steps 4-6). Deciding is
/// human-only, so an agent is refused with `human_only`.
///
/// # Errors
///
/// `forbidden` without `approval:decide` (agents get `human_only`),
/// `not_found` for an unknown id, `conflict` when the request is not pending
/// or has expired, or a database error.
pub async fn decide(
    state: &AppState,
    ctx: &CallCtx,
    approval_id: &str,
    body: Option<DecisionRequest>,
    approve: bool,
) -> Result<Approval, ApiError> {
    let params = serde_json::to_value(&body)
        .map_err(|e| ApiError::internal(&format!("decision params: {e}")))?;
    let target = approval_target(approval_id);
    ctx.authorize_human(state, Scope::ApprovalDecide, Some(&target), None, &params)
        .await?;
    if let Some(note) = body.as_ref().and_then(|b| b.note.as_ref())
        && note.chars().count() > 2000
    {
        let error = ApiError::validation("note must be at most 2000 characters");
        ctx.audit_error(
            state,
            Some(Scope::ApprovalDecide),
            Some(&target),
            None,
            &params,
            &error,
        )
        .await?;
        return Err(error);
    }
    match decide_tx(
        state,
        ctx,
        approval_id,
        body.as_ref(),
        approve,
        &target,
        &params,
    )
    .await
    {
        Ok(approval) => Ok(approval),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::ApprovalDecide),
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

/// The selector of an approval grant: the target only (section 9, step 5).
/// Section 7.2 step (g) additionally pins the exact target and params hash on
/// every destructive call, so this selector narrows the grant to the
/// target's resource without widening what it may delete.
async fn selector_for_target(
    conn: &mut SqliteConnection,
    target: &Target,
) -> Result<Selector, ApiError> {
    let mut selector = Selector::default();
    match target.kind.as_str() {
        "app" => selector.apps = Some(vec![target.id.clone()]),
        "project" => {
            let row: (String,) = sqlx::query_as("SELECT slug FROM projects WHERE id = ?")
                .bind(&target.id)
                .fetch_optional(&mut *conn)
                .await?
                .ok_or_else(|| ApiError::not_found("project"))?;
            selector.projects = Some(vec![row.0]);
        }
        "vm" => {
            let row: (Option<String>, String) =
                sqlx::query_as("SELECT app_id, node_id FROM vms WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(&mut *conn)
                    .await?
                    .ok_or_else(|| ApiError::not_found("vm"))?;
            match row {
                (Some(app_id), _) => selector.apps = Some(vec![app_id]),
                (None, node_id) => selector.nodes = Some(vec![node_id]),
            }
        }
        // A volume is scoped to its app, like an `app` target.
        "volume" => {
            let row: (String,) = sqlx::query_as("SELECT app_id FROM volumes WHERE id = ?")
                .bind(&target.id)
                .fetch_optional(&mut *conn)
                .await?
                .ok_or_else(|| ApiError::not_found("volume"))?;
            selector.apps = Some(vec![row.0]);
        }
        // A backup of a volume is scoped to its app; the control-plane
        // database is restored only offline, never through an approval.
        "backup" => {
            let row: (Option<String>, String) =
                sqlx::query_as("SELECT app_id, source_kind FROM backups WHERE id = ?")
                    .bind(&target.id)
                    .fetch_optional(&mut *conn)
                    .await?
                    .ok_or_else(|| ApiError::not_found("backup"))?;
            let (app_id, source_kind) = row;
            if source_kind == "control_plane" {
                return Err(ApiError::validation(
                    "a control-plane backup is restored only offline",
                ));
            }
            let app_id = app_id.ok_or_else(|| ApiError::validation("the backup's app is gone"))?;
            selector.apps = Some(vec![app_id]);
        }
        _ => {
            return Err(ApiError::validation(
                "approval target type is not supported",
            ));
        }
    }
    Ok(selector)
}

async fn decide_tx(
    state: &AppState,
    ctx: &CallCtx,
    approval_id: &str,
    body: Option<&DecisionRequest>,
    approve: bool,
    target: &Target,
    params: &Value,
) -> Result<Approval, ApiError> {
    if !is_id(Prefix::Approval, approval_id) {
        return Err(ApiError::not_found("approval"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {APPROVAL_COLUMNS} FROM approvals WHERE id = ?"
    ))
    .bind(approval_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("approval"))?;
    let mut approval = approval_from_row(&row)?;
    let now = now_rfc3339();
    if approval.status != ApprovalStatus::Pending {
        return Err(ApiError::conflict(format!(
            "approval {approval_id} is {} and cannot be decided again",
            approval.status.as_str()
        )));
    }
    if approval.expires_at <= now {
        return Err(ApiError::conflict(format!(
            "approval {approval_id} has expired"
        )));
    }
    let project_id = target_project_id(&mut w.tx, &target_of(&approval.target)).await?;
    let decider = ctx.identity.id().to_string();
    let note = body.and_then(|b| b.note.clone());
    let actor = ctx.identity.actor();
    let scope = Scope::parse(&approval.scope)
        .ok_or_else(|| ApiError::internal(&format!("unknown scope {}", approval.scope)))?;
    let (seq, grant_id) = if approve {
        let selector = selector_for_target(&mut w.tx, &target_of(&approval.target)).await?;
        let new = NewGrant {
            agent_id: approval.agent_id.clone(),
            kind: "approval",
            scopes: vec![scope],
            selector,
            expires_at: rfc3339_in(600),
            max_uses: Some(1),
            incident_id: approval.incident_id.clone(),
            approval_id: Some(approval.id.clone()),
            on_behalf_of: decider.clone(),
            created_by_type: "user",
            created_by_id: decider.clone(),
            reason: format!("approval {approval_id}"),
        };
        let (grant, _) =
            grants::insert_grant(&mut w.tx, &actor, Some(&ctx.request_id), new).await?;
        sqlx::query(
            "UPDATE approvals SET status = 'approved', decided_by = ?, decided_at = ?, \
             decision_note = ?, grant_id = ? WHERE id = ?",
        )
        .bind(&decider)
        .bind(&now)
        .bind(&note)
        .bind(&grant.id)
        .bind(approval_id)
        .execute(&mut *w.tx)
        .await?;
        approval.status = ApprovalStatus::Approved;
        approval.decided_by = Some(decider.clone());
        approval.decided_at = Some(now.clone());
        approval.decision_note = note.clone();
        approval.grant_id = Some(grant.id.clone());
        let seq = changelog::append(
            &mut w.tx,
            &actor,
            "approval",
            approval_id,
            Op::Update,
            approval_payload(&approval),
        )
        .await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "approval.approved",
                source: Source::ControlPlane,
                subject_type: "approval",
                subject_id: approval_id,
                project_id: project_id.as_deref(),
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data: json!({
                    "approval_id": approval.id,
                    "agent_id": approval.agent_id,
                    "scope": approval.scope,
                    "decided_by": decider,
                    "grant_id": grant.id,
                }),
            },
        )
        .await?;
        (seq, Some(grant.id))
    } else {
        sqlx::query(
            "UPDATE approvals SET status = 'denied', decided_by = ?, decided_at = ?, \
             decision_note = ? WHERE id = ?",
        )
        .bind(&decider)
        .bind(&now)
        .bind(&note)
        .bind(approval_id)
        .execute(&mut *w.tx)
        .await?;
        approval.status = ApprovalStatus::Denied;
        approval.decided_by = Some(decider.clone());
        approval.decided_at = Some(now.clone());
        approval.decision_note = note.clone();
        let seq = changelog::append(
            &mut w.tx,
            &actor,
            "approval",
            approval_id,
            Op::Update,
            approval_payload(&approval),
        )
        .await?;
        let mut data = json!({
            "approval_id": approval.id,
            "agent_id": approval.agent_id,
            "scope": approval.scope,
            "decided_by": decider,
        });
        if let Some(note) = &note {
            data["decision_note"] = json!(note);
        }
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "approval.denied",
                source: Source::ControlPlane,
                subject_type: "approval",
                subject_id: approval_id,
                project_id: project_id.as_deref(),
                request_id: Some(&ctx.request_id),
                actor: Some(&actor),
                data,
            },
        )
        .await?;
        (seq, None)
    };
    // The decision is a human action; the accountability fields name the
    // deciding human on the approval record itself (section 11).
    let mut record = ctx.record(
        Some(Scope::ApprovalDecide),
        Some(target),
        project_id.as_deref(),
        params,
        Outcome::Ok,
        None,
        Some(seq),
    );
    record.on_behalf_of = Some(decider);
    record.approval_id = Some(approval.id.clone());
    record.grant_id = grant_id;
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(approval)
}

/// Marks every pending approval whose TTL has passed as `expired`, with a
/// change-log entry and `approval.expired` for each, in one transaction. The
/// sweeper calls it every 15 s; a late decision then answers `conflict`.
///
/// # Errors
///
/// A database error.
pub async fn expire_due(state: &AppState) -> Result<usize, ApiError> {
    let mut w = state.store.begin_write().await?;
    let now = now_rfc3339();
    let rows = sqlx::query(&format!(
        "SELECT {APPROVAL_COLUMNS} FROM approvals WHERE status = 'pending' AND expires_at <= ? \
         ORDER BY id"
    ))
    .bind(&now)
    .fetch_all(&mut *w.tx)
    .await?;
    let actor = Actor::system("approvals");
    let mut expired = 0;
    for row in rows {
        let mut approval = approval_from_row(&row)?;
        sqlx::query("UPDATE approvals SET status = 'expired' WHERE id = ?")
            .bind(&approval.id)
            .execute(&mut *w.tx)
            .await?;
        approval.status = ApprovalStatus::Expired;
        changelog::append(
            &mut w.tx,
            &actor,
            "approval",
            &approval.id,
            Op::Update,
            approval_payload(&approval),
        )
        .await?;
        events::emit(
            &mut w.tx,
            NewEvent {
                kind: "approval.expired",
                source: Source::ControlPlane,
                subject_type: "approval",
                subject_id: &approval.id,
                project_id: None,
                request_id: None,
                actor: Some(&actor),
                data: json!({
                    "approval_id": approval.id,
                    "agent_id": approval.agent_id,
                    "scope": approval.scope,
                }),
            },
        )
        .await?;
        expired += 1;
    }
    w.commit().await?;
    Ok(expired)
}
