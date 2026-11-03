//! Event reads (`GET /api/v1/events`, `/api/v1/events/stream`).
//!
//! Events are append-only (`db-schema.sql`): this module only reads them. A
//! human reads everything their role covers; an agent's page is filtered by its
//! grants' selectors on the event subject and `project_id`
//! (permission-model.md section 5.1), and its read is audited like any other.

use crate::AppState;
use crate::api::events::{
    Event, EventActor, EventActorKind, EventList, EventSource, EventSourceKind, ListEventsQuery,
    Subject,
};
use crate::api::page;
use crate::authz;
use crate::error::ApiError;
use crate::request::{CallCtx, ReadFilter};
use quoro_types::Scope;
use serde_json::{Value, json};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use std::collections::HashMap;

/// The filters a page or the stream applies to every read.
#[derive(Clone, Debug, Default)]
pub struct EventFilters {
    /// Event types to keep; empty means every type.
    pub types: Vec<String>,
    /// Only events whose subject id is this one.
    pub subject_id: Option<String>,
}

const EVENT_COLUMNS: &str =
    "id, seq, type, time, source, subject, project_id, incident_id, request_id, actor, data";

/// Builds one [`Event`] from a row selected with [`EVENT_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns do not parse.
fn event_from_row(row: &SqliteRow) -> Result<Event, ApiError> {
    let source: String = row.try_get("source")?;
    let subject: String = row.try_get("subject")?;
    let actor: Option<String> = row.try_get("actor")?;
    let data: String = row.try_get("data")?;
    let source: Value = serde_json::from_str(&source)
        .map_err(|e| ApiError::internal(&format!("event source: {e}")))?;
    let subject: Value = serde_json::from_str(&subject)
        .map_err(|e| ApiError::internal(&format!("event subject: {e}")))?;
    let actor: Option<Value> = actor
        .map(|actor| serde_json::from_str(&actor))
        .transpose()
        .map_err(|e| ApiError::internal(&format!("event actor: {e}")))?;
    let data: Value =
        serde_json::from_str(&data).map_err(|e| ApiError::internal(&format!("event data: {e}")))?;

    let kind = match source["kind"].as_str() {
        Some("node") => EventSourceKind::Node,
        _ => EventSourceKind::ControlPlane,
    };
    let actor = match actor {
        None | Some(Value::Null) => None,
        Some(actor) => Some(EventActor {
            kind: match actor["type"].as_str() {
                Some("user") => EventActorKind::User,
                Some("agent") => EventActorKind::Agent,
                Some("node") => EventActorKind::Node,
                _ => EventActorKind::System,
            },
            id: actor["id"].as_str().unwrap_or_default().to_string(),
        }),
    };

    Ok(Event {
        id: row.try_get("id")?,
        seq: row.try_get("seq")?,
        kind: row.try_get("type")?,
        time: row.try_get("time")?,
        source: EventSource {
            kind,
            node_id: source["node_id"].as_str().map(str::to_string),
        },
        subject: Subject {
            kind: subject["type"].as_str().unwrap_or_default().to_string(),
            id: subject["id"].as_str().unwrap_or_default().to_string(),
        },
        project_id: row.try_get("project_id")?,
        incident_id: row.try_get("incident_id")?,
        request_id: row.try_get("request_id")?,
        actor,
        data,
    })
}

/// Events after `after_seq` in ascending `seq` order, at most `limit` of them.
///
/// # Errors
///
/// A database error, or a row whose JSON columns do not parse.
pub(crate) async fn query(
    state: &AppState,
    filters: &EventFilters,
    after_seq: i64,
    limit: i64,
) -> Result<Vec<Event>, ApiError> {
    let mut conn = state.store.pool().acquire().await?;
    query_conn(&mut conn, filters, after_seq, limit).await
}

/// [`query`] on the caller's connection, so the stream and the page share one
/// statement builder.
pub(crate) async fn query_conn(
    conn: &mut SqliteConnection,
    filters: &EventFilters,
    after_seq: i64,
    limit: i64,
) -> Result<Vec<Event>, ApiError> {
    let mut sql = format!("SELECT {EVENT_COLUMNS} FROM events WHERE seq > ?");
    if !filters.types.is_empty() {
        let placeholders = vec!["?"; filters.types.len()].join(", ");
        sql.push_str(&format!(" AND type IN ({placeholders})"));
    }
    if filters.subject_id.is_some() {
        sql.push_str(" AND json_extract(subject, '$.id') = ?");
    }
    sql.push_str(" ORDER BY seq ASC LIMIT ?");
    let mut statement = sqlx::query(&sql).bind(after_seq);
    for kind in &filters.types {
        statement = statement.bind(kind);
    }
    if let Some(subject_id) = &filters.subject_id {
        statement = statement.bind(subject_id);
    }
    let rows = statement.bind(limit).fetch_all(conn).await?;
    rows.iter().map(event_from_row).collect()
}

/// One event by its `evt_` id.
///
/// # Errors
///
/// A database error, or an internal error for a row whose JSON columns do not
/// parse.
pub(crate) async fn read_event(
    state: &AppState,
    event_id: &str,
) -> Result<Option<Event>, ApiError> {
    let row = sqlx::query(&format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = ?"))
        .bind(event_id)
        .fetch_optional(state.store.pool())
        .await?;
    row.as_ref().map(event_from_row).transpose()
}

/// The current head of the event log: the `seq` a live stream starts after.
///
/// # Errors
///
/// A database error.
pub(crate) async fn head_seq(state: &AppState) -> Result<i64, ApiError> {
    let (seq,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(state.store.pool())
        .await?;
    Ok(seq)
}

/// Keeps the rows an agent's selectors cover (permission-model.md section
/// 5.1). Humans and `{"all": true}` selectors see every row; `{}` sees none.
///
/// # Errors
///
/// A database error while resolving project slugs.
pub(crate) async fn filter_events(
    state: &AppState,
    filter: &ReadFilter,
    items: Vec<Event>,
) -> Result<Vec<Event>, ApiError> {
    let selectors = match filter {
        ReadFilter::All => return Ok(items),
        ReadFilter::Selectors(selectors) => selectors,
    };
    let mut slugs: HashMap<String, Option<String>> = HashMap::new();
    let mut kept = Vec::with_capacity(items.len());
    for item in items {
        let resource = crate::api::events::resource_of(&item);
        let mut resolved = authz::ResolvedResource::default();
        if let Some(app_id) = resource.app_id {
            resolved.apps.insert(app_id.to_string());
        }
        if let Some(node_id) = resource.node_id {
            resolved.nodes.insert(node_id.to_string());
        }
        if let Some(project_id) = resource.project_id {
            if !slugs.contains_key(project_id) {
                let slug: Option<(String,)> =
                    sqlx::query_as("SELECT slug FROM projects WHERE id = ?")
                        .bind(project_id)
                        .fetch_optional(state.store.pool())
                        .await?;
                slugs.insert(project_id.to_string(), slug.map(|s| s.0));
            }
            if let Some(Some(slug)) = slugs.get(project_id) {
                resolved.project_slugs.insert(slug.clone());
            }
        }
        if selectors.iter().any(|s| s.matches(&resolved)) {
            kept.push(item);
        }
    }
    Ok(kept)
}

/// Lists one page of events in ascending `seq` order.
///
/// # Errors
///
/// `forbidden` without `event:read`, `validation_failed` for a bad filter or
/// cursor, or a database error.
pub async fn list_events(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListEventsQuery,
) -> Result<EventList, ApiError> {
    let params = json!({
        "type": query.types,
        "subject_id": query.subject_id,
        "after_seq": query.after_seq,
    });
    let access = ctx
        .authorize_read(state, Scope::EventRead, None, &params)
        .await?;
    let result: Result<EventList, ApiError> = async {
        let limit = query.limit()?;
        let after = query.after()?;
        if access.reads_nothing() {
            return Ok(EventList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let rows = query_page(state, &query.filters(), after, limit).await?;
        // The cursor comes from the unfiltered rows, so a filtered page never
        // ends the listing early (see `ReadAccess::filter`).
        let (items, next_cursor) = page::finish(rows, limit, |event| event.seq.to_string());
        let items = access
            .filter(state, items, crate::api::events::resource_of)
            .await?;
        Ok(EventList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One page plus one row, so the caller knows whether a next page exists.
async fn query_page(
    state: &AppState,
    filters: &EventFilters,
    after: i64,
    limit: i64,
) -> Result<Vec<Event>, ApiError> {
    let mut conn = state.store.pool().acquire().await?;
    query_conn(&mut conn, filters, after, limit + 1).await
}
