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

/// Stores `event` and returns its `evt_` id. Runs in the caller's
/// transaction, so an event exists only if the change that caused it does.
///
/// # Errors
///
/// A database error.
pub async fn emit(conn: &mut SqliteConnection, event: NewEvent<'_>) -> Result<String, AppendError> {
    let (last,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(&mut *conn)
        .await?;
    let seq = last + 1;
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

    sqlx::query(
        "INSERT INTO events (id, seq, type, time, source, subject, project_id, incident_id, request_id, actor, data) \
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
    )
    .bind(&id)
    .bind(seq)
    .bind(event.kind)
    .bind(&time)
    .bind(source.to_string())
    .bind(subject.to_string())
    .bind(event.project_id)
    .bind(event.request_id)
    .bind(actor.as_ref().map(Value::to_string))
    .bind(event.data.to_string())
    .execute(&mut *conn)
    .await?;

    let mut envelope = json!({
        "schema_version": 1, "id": id, "seq": seq, "type": event.kind, "time": time,
        "source": source, "subject": subject, "data": event.data,
    });
    if let Some(project_id) = event.project_id {
        envelope["project_id"] = Value::from(project_id);
    }
    if let Some(request_id) = event.request_id {
        envelope["request_id"] = Value::from(request_id);
    }
    if let Some(actor) = actor {
        envelope["actor"] = actor;
    }
    let change_actor = event
        .actor
        .cloned()
        .unwrap_or_else(|| Actor::system("events"));
    changelog::append(conn, &change_actor, "event", &id, Op::Create, envelope).await?;
    Ok(id)
}
