//! The audit log (`audit-log-format.md`, D-0031).
//!
//! One record per action, written in the action's own transaction, chained by
//! `SHA-256(JCS(record without hash))` with absent optional fields omitted.
//! Each record is also appended to the change log as `entity_type` `audit`, so
//! it reaches replicas through the verified change-log stream (section 3.2).

use crate::changelog::{self, Actor, AppendError, Op};
use crate::jcs;
use crate::store::{GENESIS_HASH, now_rfc3339, sha256_hex};
use serde_json::{Map, Value};
use sqlx::SqliteConnection;

/// How the action arrived (`audit_log.channel`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Api,
    Panel,
    Mcp,
    Cli,
    System,
}

impl Channel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Panel => "panel",
            Self::Mcp => "mcp",
            Self::Cli => "cli",
            Self::System => "system",
        }
    }
}

/// Outcome of the call (`audit_log.result`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Refused,
    Error,
}

impl Outcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Refused => "refused",
            Self::Error => "error",
        }
    }
}

/// One audit record before it is chained. Optional fields that are `None`
/// are omitted from the hashed object (section 2).
#[derive(Clone, Debug)]
pub struct Record {
    pub actor: Actor,
    pub agent_model: Option<String>,
    pub on_behalf_of: Option<String>,
    pub channel: Channel,
    /// `POST /api/v1/vms` style, with path templates, or an MCP tool name.
    pub operation: String,
    pub scope: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub project_id: Option<String>,
    /// Request parameters; [`append`] redacts them (section 4).
    pub params: Value,
    pub justification: Option<String>,
    pub trigger_event_id: Option<String>,
    pub incident_id: Option<String>,
    pub grant_id: Option<String>,
    pub approval_id: Option<String>,
    pub request_id: String,
    pub result: Outcome,
    pub result_detail: Option<String>,
    pub change_seq: Option<i64>,
}

/// Keys whose values are replaced at any depth (section 4).
const REDACTED_KEYS: [&str; 6] = ["value", "values", "secret", "password", "token", "key"];

/// Replaces the values of the redacted keys by `"[redacted]"`, at any depth.
#[must_use]
pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    if REDACTED_KEYS.contains(&k.as_str()) {
                        (k.clone(), Value::String("[redacted]".to_string()))
                    } else {
                        (k.clone(), redact(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

fn put(map: &mut Map<String, Value>, key: &str, value: Option<&String>) {
    if let Some(v) = value {
        map.insert(key.to_string(), Value::String(v.clone()));
    }
}

/// Appends `record` and returns its audit `seq`. Must run in the same write
/// transaction as the change it describes (section 3.1).
///
/// # Errors
///
/// A database error, or parameters JCS cannot canonicalize.
pub async fn append(conn: &mut SqliteConnection, record: &Record) -> Result<i64, AppendError> {
    let last: Option<(i64, String)> =
        sqlx::query_as("SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (seq, prev_hash) = match last {
        Some((seq, hash)) => (seq + 1, hash),
        None => (1, GENESIS_HASH.to_string()),
    };
    let time = now_rfc3339();
    let params = redact(&record.params);

    let mut object = Map::new();
    object.insert("seq".into(), Value::from(seq));
    object.insert("time".into(), Value::String(time.clone()));
    object.insert("actor_type".into(), Value::from(record.actor.kind.as_str()));
    object.insert("actor_id".into(), Value::String(record.actor.id.clone()));
    put(&mut object, "agent_model", record.agent_model.as_ref());
    put(&mut object, "on_behalf_of", record.on_behalf_of.as_ref());
    object.insert("channel".into(), Value::from(record.channel.as_str()));
    object.insert("operation".into(), Value::String(record.operation.clone()));
    put(&mut object, "scope", record.scope.as_ref());
    put(&mut object, "target_type", record.target_type.as_ref());
    put(&mut object, "target_id", record.target_id.as_ref());
    put(&mut object, "project_id", record.project_id.as_ref());
    object.insert("params".into(), params.clone());
    put(&mut object, "justification", record.justification.as_ref());
    put(
        &mut object,
        "trigger_event_id",
        record.trigger_event_id.as_ref(),
    );
    put(&mut object, "incident_id", record.incident_id.as_ref());
    put(&mut object, "grant_id", record.grant_id.as_ref());
    put(&mut object, "approval_id", record.approval_id.as_ref());
    object.insert(
        "request_id".into(),
        Value::String(record.request_id.clone()),
    );
    object.insert("result".into(), Value::from(record.result.as_str()));
    put(&mut object, "result_detail", record.result_detail.as_ref());
    if let Some(change_seq) = record.change_seq {
        object.insert("change_seq".into(), Value::from(change_seq));
    }
    object.insert("prev_hash".into(), Value::String(prev_hash.clone()));

    let canonical =
        jcs::canonicalize(&Value::Object(object.clone())).map_err(AppendError::NotCanonical)?;
    let hash = sha256_hex(canonical.as_bytes());
    let params_text = jcs::canonicalize(&params).map_err(AppendError::NotCanonical)?;

    sqlx::query(
        "INSERT INTO audit_log (seq, time, actor_type, actor_id, agent_model, on_behalf_of, channel, operation, scope, \
         target_type, target_id, project_id, params, justification, trigger_event_id, incident_id, grant_id, approval_id, \
         request_id, result, result_detail, change_seq, prev_hash, hash) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(seq)
    .bind(&time)
    .bind(record.actor.kind.as_str())
    .bind(&record.actor.id)
    .bind(&record.agent_model)
    .bind(&record.on_behalf_of)
    .bind(record.channel.as_str())
    .bind(&record.operation)
    .bind(&record.scope)
    .bind(&record.target_type)
    .bind(&record.target_id)
    .bind(&record.project_id)
    .bind(&params_text)
    .bind(&record.justification)
    .bind(&record.trigger_event_id)
    .bind(&record.incident_id)
    .bind(&record.grant_id)
    .bind(&record.approval_id)
    .bind(&record.request_id)
    .bind(record.result.as_str())
    .bind(&record.result_detail)
    .bind(record.change_seq)
    .bind(&prev_hash)
    .bind(&hash)
    .execute(&mut *conn)
    .await?;

    // Section 3.2: the record travels to replicas as a change-log entry.
    object.insert("hash".into(), Value::String(hash));
    changelog::append(
        conn,
        &record.actor,
        "audit",
        &seq.to_string(),
        Op::Create,
        Value::Object(object),
    )
    .await?;
    Ok(seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ActorType;
    use crate::store::Store;
    use serde_json::json;

    fn record(result: Outcome) -> Record {
        Record {
            actor: Actor {
                kind: ActorType::User,
                id: "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into(),
            },
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::Panel,
            operation: "POST /api/v1/setup".into(),
            scope: None,
            target_type: Some("user".into()),
            target_id: Some("usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into()),
            project_id: None,
            params: json!({ "email": "a@example.com", "password": "hunter2hunter2", "nested": [{ "token": "x" }] }),
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: "req_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into(),
            result,
            result_detail: None,
            change_seq: None,
        }
    }

    #[tokio::test]
    async fn records_chain_redact_and_replicate() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        assert_eq!(append(&mut w.tx, &record(Outcome::Ok)).await.unwrap(), 1);
        assert_eq!(
            append(&mut w.tx, &record(Outcome::Refused)).await.unwrap(),
            2
        );
        w.commit().await.unwrap();

        let rows: Vec<(i64, String, String, String)> =
            sqlx::query_as("SELECT seq, params, prev_hash, hash FROM audit_log ORDER BY seq")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(rows[0].2, GENESIS_HASH);
        assert_eq!(rows[1].2, rows[0].3);
        assert!(!rows[0].1.contains("hunter2"));
        assert!(rows[0].1.contains("[redacted]"));
        assert!(!rows[0].1.contains("\"x\""));

        let audit_entries: (i64,) =
            sqlx::query_as("SELECT count(*) FROM change_log WHERE entity_type = 'audit'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(audit_entries.0, 2);
    }
}
