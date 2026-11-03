//! Event emission into the `events` table (`events.schema.json`).
//!
//! Each event is also a change-log entry of `entity_type` `event`, so replicas
//! hold the event log too (`db-schema.sql` header).

use crate::changelog::{self, Actor, AppendError, Op};
use crate::store::now_rfc3339;
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// Where the event came from (`$defs/Source`).
#[derive(Clone, Debug)]
pub enum Source {
    ControlPlane,
    Node(String),
}

/// One event before it is stored. `data` must match the schema for `kind`.
#[derive(Clone, Debug)]
pub struct NewEvent<'a> {
    /// Event type, for example `vm.started`.
    pub kind: &'a str,
    pub source: Source,
    /// Subject resource kind and id (`$defs/Subject`).
    pub subject_type: &'a str,
    pub subject_id: &'a str,
    pub project_id: Option<&'a str>,
    pub request_id: Option<&'a str>,
    /// `None` for events produced by observation only.
    pub actor: Option<&'a Actor>,
    pub data: Value,
}

/// One event with every field already in its stored form. Node events arrive
/// as this shape (`node-agent-protocol.md` section 5.4): their id, time,
/// source, subject and actor are the node's, and the primary only assigns the
/// `seq`.
#[derive(Clone, Debug)]
pub struct StoredEvent<'a> {
    /// `evt_` id, as the node generated it.
    pub id: &'a str,
    /// RFC 3339 time of the event.
    pub time: &'a str,
    /// Event type.
    pub kind: &'a str,
    /// `{kind, node_id?}`.
    pub source: Value,
    /// `{type, id}`.
    pub subject: Value,
    pub project_id: Option<&'a str>,
    pub incident_id: Option<&'a str>,
    pub request_id: Option<&'a str>,
    /// `{type, id}`, or `None` for observation-only events.
    pub actor: Option<Value>,
    pub data: Value,
    /// The actor of the change-log entry that carries the event.
    pub change_actor: Actor,
}

/// Stores `event` and returns its `evt_` id. Runs in the caller's
/// transaction, so an event exists only if the change that caused it does.
///
/// # Errors
///
/// A database error.
pub async fn emit(conn: &mut SqliteConnection, event: NewEvent<'_>) -> Result<String, AppendError> {
    let id = new_id(Prefix::Event);
    let time = now_rfc3339();
    let source = match &event.source {
        Source::ControlPlane => json!({ "kind": "control_plane" }),
        Source::Node(node_id) => json!({ "kind": "node", "node_id": node_id }),
    };
    let subject = json!({ "type": event.subject_type, "id": event.subject_id });
    let actor = event
        .actor
        .map(|a| json!({ "type": a.kind.as_str(), "id": a.id }));
    let change_actor = event
        .actor
        .cloned()
        .unwrap_or_else(|| Actor::system("events"));
    insert(
        conn,
        StoredEvent {
            id: &id,
            time: &time,
            kind: event.kind,
            source,
            subject,
            project_id: event.project_id,
            incident_id: None,
            request_id: event.request_id,
            actor,
            data: event.data,
            change_actor,
        },
    )
    .await?;
    Ok(id)
}

/// Inserts an event that already has its id and time, as a node event does.
/// The `seq` is assigned here; a repeated id is the caller's deduplication
/// problem (the session ignores it before calling this).
///
/// # Errors
///
/// A database error.
pub async fn insert(
    conn: &mut SqliteConnection,
    event: StoredEvent<'_>,
) -> Result<(), AppendError> {
    let (last,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(&mut *conn)
        .await?;
    let seq = last + 1;
    sqlx::query(
        "INSERT INTO events (id, seq, type, time, source, subject, project_id, incident_id, request_id, actor, data) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(event.id)
    .bind(seq)
    .bind(event.kind)
    .bind(event.time)
    .bind(event.source.to_string())
    .bind(event.subject.to_string())
    .bind(event.project_id)
    .bind(event.incident_id)
    .bind(event.request_id)
    .bind(event.actor.as_ref().map(Value::to_string))
    .bind(event.data.to_string())
    .execute(&mut *conn)
    .await?;

    let mut envelope = json!({
        "schema_version": 1, "id": event.id, "seq": seq, "type": event.kind,
        "time": event.time, "source": event.source, "subject": event.subject,
        "data": event.data,
    });
    if let Some(project_id) = event.project_id {
        envelope["project_id"] = Value::from(project_id);
    }
    if let Some(incident_id) = event.incident_id {
        envelope["incident_id"] = Value::from(incident_id);
    }
    if let Some(request_id) = event.request_id {
        envelope["request_id"] = Value::from(request_id);
    }
    if let Some(actor) = event.actor {
        envelope["actor"] = actor;
    }
    changelog::append(
        conn,
        &event.change_actor,
        "event",
        event.id,
        Op::Create,
        envelope,
    )
    .await?;
    Ok(())
}
