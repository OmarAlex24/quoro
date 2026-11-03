//! Incident operations (`/api/v1/incidents*`) and the detection hook that
//! opens and resolves them (permission-model.md sections 8.2 and 8.3, D-0032).
//!
//! A detection opens an incident with the subject it is about; `app.down` from
//! a node opens one for that app, and `app.up` resolves it as the system, in
//! the same transaction that stores the node event. Resolving an incident
//! revokes its incident grants and emits `grant.revoked` for each.

use crate::AppState;
use crate::api::auth::{GrantCreator, GrantKind, GrantStatus};
use crate::api::events::Subject;
use crate::api::grants::Grant;
use crate::api::incidents::{
    Incident, IncidentDetail, IncidentList, IncidentResolver, IncidentStatus, ListIncidentsQuery,
    incident_target,
};
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::error::{ApiError, Target};
use crate::events::{self, StoredEvent};
use crate::request::{CallCtx, Resource};
use crate::service::events as event_service;
use crate::service::grants;
use crate::store::now_rfc3339;
use quoro_proto::wire;
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};

const INCIDENT_COLUMNS: &str = "id, status, title, trigger_event_id, subject, project_id, \
     opened_at, resolved_at, resolved_by_type, resolved_by_id, resolution_note";

const GRANT_COLUMNS: &str = "id, agent_id, kind, incident_id, approval_id, scopes, selector, \
     expires_at, max_uses, uses, status, on_behalf_of, created_by_type, created_by_id, reason, \
     created_at, revoked_at, revoked_by";

fn subject_of(json: &str) -> Result<Subject, ApiError> {
    let value: Value = serde_json::from_str(json)
        .map_err(|e| ApiError::internal(&format!("incident subject: {e}")))?;
    Ok(Subject {
        kind: value["type"].as_str().unwrap_or_default().to_string(),
        id: value["id"].as_str().unwrap_or_default().to_string(),
    })
}

/// One [`Incident`] from a row selected with [`INCIDENT_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON or enum columns do not parse.
fn incident_from_row(row: &SqliteRow) -> Result<Incident, ApiError> {
    let status: String = row.try_get("status")?;
    let resolved_by_type: Option<String> = row.try_get("resolved_by_type")?;
    let subject: String = row.try_get("subject")?;
    Ok(Incident {
        id: row.try_get("id")?,
        status: IncidentStatus::parse(&status)?,
        title: row.try_get("title")?,
        trigger_event_id: row.try_get("trigger_event_id")?,
        subject: subject_of(&subject)?,
        project_id: row.try_get("project_id")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        resolved_by_type: resolved_by_type
            .map(|value| IncidentResolver::parse(&value))
            .transpose()?,
        resolved_by_id: row.try_get("resolved_by_id")?,
        resolution_note: row.try_get("resolution_note")?,
    })
}

/// The change-log payload of an incident: the whole row, so a replica rebuilds
/// the table by replaying the log (D-0013).
fn incident_payload(incident: &Incident) -> Value {
    json!({
        "id": incident.id,
        "status": incident.status.as_str(),
        "title": incident.title,
        "trigger_event_id": incident.trigger_event_id,
        "subject": { "type": incident.subject.kind, "id": incident.subject.id },
        "project_id": incident.project_id,
        "opened_at": incident.opened_at,
        "resolved_at": incident.resolved_at,
        "resolved_by_type": incident.resolved_by_type.map(IncidentResolver::as_str),
        "resolved_by_id": incident.resolved_by_id,
        "resolution_note": incident.resolution_note,
    })
}

fn grant_kind(value: &str) -> Result<GrantKind, ApiError> {
    match value {
        "standard" => Ok(GrantKind::Standard),
        "incident" => Ok(GrantKind::Incident),
        "approval" => Ok(GrantKind::Approval),
        other => Err(ApiError::internal(&format!("unknown grant kind {other}"))),
    }
}

fn grant_status(value: &str) -> Result<GrantStatus, ApiError> {
    match value {
        "active" => Ok(GrantStatus::Active),
        "expired" => Ok(GrantStatus::Expired),
        "revoked" => Ok(GrantStatus::Revoked),
        "exhausted" => Ok(GrantStatus::Exhausted),
        other => Err(ApiError::internal(&format!("unknown grant status {other}"))),
    }
}

fn grant_creator(value: &str) -> Result<GrantCreator, ApiError> {
    match value {
        "user" => Ok(GrantCreator::User),
        "system" => Ok(GrantCreator::System),
        other => Err(ApiError::internal(&format!(
            "unknown grant creator {other}"
        ))),
    }
}

fn stored_grant(row: &SqliteRow) -> Result<Grant, ApiError> {
    let scopes: String = row.try_get("scopes")?;
    let selector: String = row.try_get("selector")?;
    Ok(Grant {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        kind: grant_kind(row.try_get("kind")?)?,
        incident_id: row.try_get("incident_id")?,
        approval_id: row.try_get("approval_id")?,
        scopes: serde_json::from_str(&scopes)
            .map_err(|e| ApiError::internal(&format!("grant scopes: {e}")))?,
        selector: serde_json::from_str(&selector).unwrap_or_default(),
        expires_at: row.try_get("expires_at")?,
        max_uses: row.try_get("max_uses")?,
        uses: row.try_get("uses")?,
        status: grant_status(row.try_get("status")?)?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        created_by_type: grant_creator(row.try_get("created_by_type")?)?,
        created_by_id: row.try_get("created_by_id")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        revoked_by: row.try_get("revoked_by")?,
    })
}

/// The grants issued for an incident, in id order.
///
/// # Errors
///
/// A database error, or a row whose JSON or enum columns do not parse.
async fn grants_for_incident(
    conn: &mut SqliteConnection,
    incident_id: &str,
) -> Result<Vec<Grant>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {GRANT_COLUMNS} FROM grants WHERE incident_id = ? ORDER BY id"
    ))
    .bind(incident_id)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(stored_grant).collect()
}

/// Reads one incident by id.
///
/// # Errors
///
/// A database error, or an internal error for a row that does not parse.
async fn read_incident(state: &AppState, incident_id: &str) -> Result<Option<Incident>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE id = ?"
    ))
    .bind(incident_id)
    .fetch_optional(state.store.pool())
    .await?;
    row.as_ref().map(incident_from_row).transpose()
}

/// The `Resource` of an incident, for selector filtering: its subject's app,
/// node or project plus the incident's `project_id`.
#[must_use]
pub fn resource_of(incident: &Incident) -> Resource<'_> {
    Resource {
        kind: "incident",
        id: &incident.id,
        app_id: (incident.subject.kind == "app").then_some(incident.subject.id.as_str()),
        project_id: incident.project_id.as_deref(),
        node_id: (incident.subject.kind == "node").then_some(incident.subject.id.as_str()),
    }
}

/// Lists incidents, newest first. An agent's page is filtered by its grants'
/// selectors on the incident subject (permission-model.md section 5.1).
///
/// # Errors
///
/// `forbidden` without `incident:read`, `validation_failed` for a bad filter,
/// or a database error.
pub async fn list_incidents(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListIncidentsQuery,
) -> Result<IncidentList, ApiError> {
    let params = json!({
        "status": query.status(),
        "limit": query.limit,
    });
    let access = ctx
        .authorize_read(state, Scope::IncidentRead, None, &params)
        .await?;
    let result: Result<IncidentList, ApiError> = async {
        let limit = query.limit()?;
        if access.reads_nothing() {
            return Ok(IncidentList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let mut sql = format!("SELECT {INCIDENT_COLUMNS} FROM incidents WHERE 1 = 1");
        if query.cursor.is_some() {
            sql.push_str(" AND id < ?");
        }
        if query.status().is_some() {
            sql.push_str(" AND status = ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        let mut statement = sqlx::query(&sql);
        if let Some(cursor) = &query.cursor {
            statement = statement.bind(cursor);
        }
        if let Some(status) = query.status() {
            statement = statement.bind(status);
        }
        let rows = statement
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let incidents = rows
            .iter()
            .map(incident_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        // The cursor comes from the unfiltered rows, so a filtered page never
        // ends the listing early.
        let (items, next_cursor) = crate::api::page::finish(incidents, limit, |i| i.id.clone());
        let items = access.filter(state, items, resource_of).await?;
        Ok(IncidentList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One incident with its trigger event and incident grants.
///
/// # Errors
///
/// `forbidden` without `incident:read`, `not_found` for an unknown or
/// malformed id, or a database error.
pub async fn get_incident(
    state: &AppState,
    ctx: &CallCtx,
    incident_id: &str,
) -> Result<IncidentDetail, ApiError> {
    let params = json!({ "incident_id": incident_id });
    let target = incident_target(incident_id);
    let access = ctx
        .authorize_read(state, Scope::IncidentRead, Some(&target), &params)
        .await?;
    let result: Result<IncidentDetail, ApiError> = async {
        if !is_id(Prefix::Incident, incident_id) {
            return Err(ApiError::not_found("incident"));
        }
        let incident = read_incident(state, incident_id)
            .await?
            .ok_or_else(|| ApiError::not_found("incident"))?;
        let trigger_event = event_service::read_event(state, &incident.trigger_event_id)
            .await?
            .ok_or_else(|| ApiError::internal(&"the incident's trigger event is missing"))?;
        let mut conn = state.store.pool().acquire().await?;
        let grants = grants_for_incident(&mut conn, incident_id).await?;
        Ok(IncidentDetail {
            incident,
            trigger_event,
            grants,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Resolves an incident and revokes its incident grants (section 8.3).
///
/// # Errors
///
/// `forbidden` without `incident:resolve`, `not_found` for an unknown id,
/// `conflict` when it is already resolved, or a database error.
pub async fn resolve_incident(
    state: &AppState,
    ctx: &CallCtx,
    incident_id: &str,
    resolution_note: Option<String>,
) -> Result<Incident, ApiError> {
    let params = json!({
        "incident_id": incident_id,
        "resolution_note": resolution_note,
    });
    let target = incident_target(incident_id);
    ctx.authorize(state, Scope::IncidentResolve, Some(&target), None, &params)
        .await?;
    match resolve_tx(state, ctx, incident_id, resolution_note, &target, &params).await {
        Ok(incident) => Ok(incident),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::IncidentResolve),
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

async fn resolve_tx(
    state: &AppState,
    ctx: &CallCtx,
    incident_id: &str,
    resolution_note: Option<String>,
    target: &Target,
    params: &Value,
) -> Result<Incident, ApiError> {
    if !is_id(Prefix::Incident, incident_id) {
        return Err(ApiError::not_found("incident"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE id = ?"
    ))
    .bind(incident_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("incident"))?;
    let mut incident = incident_from_row(&row)?;
    if incident.status == IncidentStatus::Resolved {
        return Err(ApiError::conflict(format!(
            "incident {incident_id} is already resolved"
        )));
    }

    let actor = ctx.identity.actor();
    let resolved_by_type = match actor.kind.as_str() {
        "agent" => IncidentResolver::Agent,
        "system" => IncidentResolver::System,
        _ => IncidentResolver::User,
    };
    let now = now_rfc3339();
    incident.status = IncidentStatus::Resolved;
    incident.resolved_at = Some(now.clone());
    incident.resolved_by_type = Some(resolved_by_type);
    incident.resolved_by_id = Some(actor.id.clone());
    incident.resolution_note = resolution_note.clone();
    sqlx::query(
        "UPDATE incidents SET status = 'resolved', resolved_at = ?, resolved_by_type = ?, \
         resolved_by_id = ?, resolution_note = ? WHERE id = ?",
    )
    .bind(&now)
    .bind(resolved_by_type.as_str())
    .bind(&actor.id)
    .bind(&resolution_note)
    .bind(incident_id)
    .execute(&mut *w.tx)
    .await?;
    let change_seq = changelog::append(
        &mut w.tx,
        &actor,
        "incident",
        incident_id,
        Op::Update,
        incident_payload(&incident),
    )
    .await?;

    grants::revoke_for_incident(&mut w.tx, incident_id, Some(&ctx.request_id)).await?;
    emit_resolved(
        &mut w.tx,
        &incident,
        resolved_by_type,
        &actor.id,
        resolution_note.as_deref(),
        Some(&ctx.request_id),
        &actor,
    )
    .await?;

    ctx.append_ok(
        &mut w.tx,
        Some(Scope::IncidentResolve),
        Some(target),
        incident.project_id.as_deref(),
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok(incident)
}

/// Emits `incident.resolved` for `incident` in the caller's transaction.
async fn emit_resolved(
    conn: &mut SqliteConnection,
    incident: &Incident,
    resolved_by_type: IncidentResolver,
    resolved_by_id: &str,
    resolution_note: Option<&str>,
    request_id: Option<&str>,
    actor: &Actor,
) -> Result<(), ApiError> {
    let id = new_id(Prefix::Event);
    let time = now_rfc3339();
    let mut data = json!({
        "incident_id": incident.id,
        "resolved_by_type": resolved_by_type.as_str(),
        "resolved_by_id": resolved_by_id,
    });
    if let Some(note) = resolution_note {
        data["resolution_note"] = json!(note);
    }
    events::insert(
        conn,
        StoredEvent {
            id: &id,
            time: &time,
            kind: "incident.resolved",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "incident", "id": incident.id }),
            project_id: incident.project_id.as_deref(),
            incident_id: Some(&incident.id),
            request_id,
            actor: Some(json!({ "type": actor.kind.as_str(), "id": actor.id })),
            data,
            change_actor: actor.clone(),
        },
    )
    .await?;
    Ok(())
}

/// The audit record of one system incident action.
fn system_record(
    operation: &str,
    incident: &Incident,
    trigger_event_id: Option<&str>,
    params: Value,
    request_id: String,
    change_seq: i64,
) -> Record {
    Record {
        actor: Actor::system("incidents"),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: operation.to_string(),
        scope: None,
        target_type: Some("incident".to_string()),
        target_id: Some(incident.id.clone()),
        project_id: incident.project_id.clone(),
        params,
        justification: None,
        trigger_event_id: trigger_event_id.map(str::to_string),
        incident_id: Some(incident.id.clone()),
        grant_id: None,
        approval_id: None,
        request_id,
        result: Outcome::Ok,
        result_detail: None,
        change_seq: Some(change_seq),
    }
}

/// The `request_id` of a system action: the node event's, or a fresh one.
fn system_request_id(event: &wire::EventBody) -> String {
    event
        .request_id
        .clone()
        .unwrap_or_else(|| new_id(Prefix::Request))
}

/// Applies a stored node event to the incident table, in the caller's
/// transaction: `app.down` opens an incident for the app when none is open,
/// and `app.up` resolves the open one as the system. Every other event is
/// ignored.
///
/// # Errors
///
/// A database error, or an inconsistent stored row.
pub(crate) async fn apply_node_event(
    conn: &mut SqliteConnection,
    event: &wire::EventBody,
) -> Result<(), ApiError> {
    match event.kind.as_str() {
        "app.down" => open_for_app_down(conn, event).await,
        "app.up" => resolve_for_app_up(conn, event).await,
        _ => Ok(()),
    }
}

/// The subject id of an event whose subject is an app.
fn app_subject(event: &wire::EventBody) -> Option<&str> {
    (event.subject["type"].as_str() == Some("app"))
        .then(|| event.subject["id"].as_str())
        .flatten()
}

/// Opens one incident for an `app.down` event, unless an open one already
/// exists for that app (section 8.2 step 1 and 2).
async fn open_for_app_down(
    conn: &mut SqliteConnection,
    event: &wire::EventBody,
) -> Result<(), ApiError> {
    let Some(app_id) = app_subject(event) else {
        return Ok(());
    };
    let existing: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM incidents WHERE status = 'open' \
         AND json_extract(subject, '$.type') = 'app' AND json_extract(subject, '$.id') = ?",
    )
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    if existing.is_some() {
        return Ok(());
    }
    let app: Option<(String, String)> =
        sqlx::query_as("SELECT name, project_id FROM apps WHERE id = ?")
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?;
    let name = app
        .as_ref()
        .map_or_else(|| app_id.to_string(), |(name, _)| name.clone());
    let project_id = event
        .project_id
        .clone()
        .or_else(|| app.as_ref().map(|(_, project_id)| project_id.clone()));
    let incident = Incident {
        id: new_id(Prefix::Incident),
        status: IncidentStatus::Open,
        title: format!("app {name} is down"),
        trigger_event_id: event.id.clone(),
        subject: Subject {
            kind: "app".to_string(),
            id: app_id.to_string(),
        },
        project_id: project_id.clone(),
        opened_at: now_rfc3339(),
        resolved_at: None,
        resolved_by_type: None,
        resolved_by_id: None,
        resolution_note: None,
    };
    sqlx::query(
        "INSERT INTO incidents (id, status, title, trigger_event_id, subject, project_id, \
         opened_at, resolved_at, resolved_by_type, resolved_by_id, resolution_note) \
         VALUES (?, 'open', ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)",
    )
    .bind(&incident.id)
    .bind(&incident.title)
    .bind(&incident.trigger_event_id)
    .bind(json!({ "type": "app", "id": app_id }).to_string())
    .bind(&incident.project_id)
    .bind(&incident.opened_at)
    .execute(&mut *conn)
    .await?;
    let change_seq = changelog::append(
        conn,
        &Actor::system("incidents"),
        "incident",
        &incident.id,
        Op::Create,
        incident_payload(&incident),
    )
    .await?;

    let request_id = system_request_id(event);
    let event_id = new_id(Prefix::Event);
    let time = now_rfc3339();
    let actor = Actor::system("incidents");
    events::insert(
        conn,
        StoredEvent {
            id: &event_id,
            time: &time,
            kind: "incident.opened",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "incident", "id": incident.id }),
            project_id: incident.project_id.as_deref(),
            incident_id: Some(&incident.id),
            request_id: Some(&request_id),
            actor: Some(json!({ "type": "system", "id": "incidents" })),
            data: json!({
                "incident_id": incident.id,
                "title": incident.title,
                "trigger_event_id": incident.trigger_event_id,
                "subject": { "type": "app", "id": app_id },
            }),
            change_actor: actor.clone(),
        },
    )
    .await?;
    audit::append(
        conn,
        &system_record(
            "incident.open",
            &incident,
            Some(&event.id),
            json!({
                "incident_id": incident.id,
                "subject": { "type": "app", "id": app_id },
            }),
            request_id,
            change_seq,
        ),
    )
    .await?;
    Ok(())
}

/// Resolves the open incident of an `app.up` event as the system (section 8.3).
async fn resolve_for_app_up(
    conn: &mut SqliteConnection,
    event: &wire::EventBody,
) -> Result<(), ApiError> {
    let Some(app_id) = app_subject(event) else {
        return Ok(());
    };
    let row = sqlx::query(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE status = 'open' \
         AND json_extract(subject, '$.type') = 'app' AND json_extract(subject, '$.id') = ? \
         ORDER BY id LIMIT 1"
    ))
    .bind(app_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let mut incident = incident_from_row(&row)?;
    let actor = Actor::system("incidents");
    let now = now_rfc3339();
    incident.status = IncidentStatus::Resolved;
    incident.resolved_at = Some(now.clone());
    incident.resolved_by_type = Some(IncidentResolver::System);
    incident.resolved_by_id = Some("incidents".to_string());
    incident.resolution_note = Some("the app came back up".to_string());
    sqlx::query(
        "UPDATE incidents SET status = 'resolved', resolved_at = ?, resolved_by_type = 'system', \
         resolved_by_id = 'incidents', resolution_note = ? WHERE id = ?",
    )
    .bind(&now)
    .bind(&incident.resolution_note)
    .bind(&incident.id)
    .execute(&mut *conn)
    .await?;
    let change_seq = changelog::append(
        conn,
        &actor,
        "incident",
        &incident.id,
        Op::Update,
        incident_payload(&incident),
    )
    .await?;
    let request_id = system_request_id(event);
    grants::revoke_for_incident(conn, &incident.id, Some(&request_id)).await?;
    emit_resolved(
        conn,
        &incident,
        IncidentResolver::System,
        "incidents",
        incident.resolution_note.as_deref(),
        Some(&request_id),
        &actor,
    )
    .await?;
    audit::append(
        conn,
        &system_record(
            "incident.resolve",
            &incident,
            Some(&incident.trigger_event_id),
            json!({ "incident_id": incident.id, "reason": "app.up" }),
            request_id,
            change_seq,
        ),
    )
    .await?;
    Ok(())
}
