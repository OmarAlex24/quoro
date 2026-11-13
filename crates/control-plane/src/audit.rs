//! The audit log (`audit-log-format.md`, D-0031).
//!
//! One record per action, written in the action's own transaction, chained by
//! `SHA-256(JCS(record without hash))` with absent optional fields omitted.
//! Each record is also appended to the change log as `entity_type` `audit`, so
//! it reaches replicas through the verified change-log stream (section 3.2).
//! When the record's parameters would make that entry one the primary may not
//! write, the record keeps their digest and size instead (section 4.1, ADR
//! 0306), so the audit write never fails because of its parameters.

use crate::changelog::{self, Actor, AppendError, Op, StoredEntry};
use crate::jcs;
use crate::store::{GENESIS_HASH, now_rfc3339, sha256_hex};
use serde_json::{Map, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};

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
const REDACTED_KEYS: [&str; 9] = [
    "value",
    "values",
    "secret",
    "password",
    "current_password",
    "new_password",
    "token",
    "key",
    "recovery_key",
];

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

/// Every column of `audit_log`, in the shape [`fields_from_row`] reads.
pub const COLUMNS: &str = "seq, time, actor_type, actor_id, agent_model, on_behalf_of, channel, operation, scope, \
     target_type, target_id, project_id, params, params_sha256, params_bytes, justification, trigger_event_id, \
     incident_id, grant_id, approval_id, request_id, result, result_detail, change_seq, prev_hash, hash";

/// The fields of one audit record, exactly as [`append`] serializes them into
/// the hashed object: absent optional fields omitted, `params` already
/// redacted and in its stored canonical form, integers as integers.
///
/// [`append`] and [`fields_from_row`] both build this type, so the object
/// written to the database and the object re-hashed by a verifier come from
/// one place and cannot drift (section 5).
#[derive(Clone, Debug)]
pub struct HashedFields {
    pub seq: i64,
    pub time: String,
    pub actor_type: String,
    pub actor_id: String,
    pub agent_model: Option<String>,
    pub on_behalf_of: Option<String>,
    pub channel: String,
    pub operation: String,
    pub scope: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub project_id: Option<String>,
    pub params: Value,
    /// The digest of the redacted parameters, when `params` is `{}` in their
    /// place (section 4.1).
    pub params_sha256: Option<String>,
    /// The size of the text `params_sha256` covers; set exactly when it is.
    pub params_bytes: Option<i64>,
    pub justification: Option<String>,
    pub trigger_event_id: Option<String>,
    pub incident_id: Option<String>,
    pub grant_id: Option<String>,
    pub approval_id: Option<String>,
    pub request_id: String,
    pub result: String,
    pub result_detail: Option<String>,
    pub change_seq: Option<i64>,
    pub prev_hash: String,
}

impl HashedFields {
    /// The record object the hash covers: every field except `hash`, with
    /// absent optional fields left out rather than set to null (section 2).
    #[must_use]
    pub fn object(&self) -> Map<String, Value> {
        let mut object = Map::new();
        object.insert("seq".into(), Value::from(self.seq));
        object.insert("time".into(), Value::String(self.time.clone()));
        object.insert("actor_type".into(), Value::String(self.actor_type.clone()));
        object.insert("actor_id".into(), Value::String(self.actor_id.clone()));
        put(&mut object, "agent_model", self.agent_model.as_ref());
        put(&mut object, "on_behalf_of", self.on_behalf_of.as_ref());
        object.insert("channel".into(), Value::String(self.channel.clone()));
        object.insert("operation".into(), Value::String(self.operation.clone()));
        put(&mut object, "scope", self.scope.as_ref());
        put(&mut object, "target_type", self.target_type.as_ref());
        put(&mut object, "target_id", self.target_id.as_ref());
        put(&mut object, "project_id", self.project_id.as_ref());
        object.insert("params".into(), self.params.clone());
        // Section 4.1: both are part of the hashed record when set, and a
        // record without them hashes as it did before they existed.
        put(&mut object, "params_sha256", self.params_sha256.as_ref());
        if let Some(params_bytes) = self.params_bytes {
            object.insert("params_bytes".into(), Value::from(params_bytes));
        }
        put(&mut object, "justification", self.justification.as_ref());
        put(
            &mut object,
            "trigger_event_id",
            self.trigger_event_id.as_ref(),
        );
        put(&mut object, "incident_id", self.incident_id.as_ref());
        put(&mut object, "grant_id", self.grant_id.as_ref());
        put(&mut object, "approval_id", self.approval_id.as_ref());
        object.insert("request_id".into(), Value::String(self.request_id.clone()));
        object.insert("result".into(), Value::String(self.result.clone()));
        put(&mut object, "result_detail", self.result_detail.as_ref());
        if let Some(change_seq) = self.change_seq {
            object.insert("change_seq".into(), Value::from(change_seq));
        }
        object.insert("prev_hash".into(), Value::String(self.prev_hash.clone()));
        object
    }

    /// [`HashedFields::object`] as a standalone value.
    #[must_use]
    pub fn value(&self) -> Value {
        Value::Object(self.object())
    }

    /// The complete record, `hash` included: what the API returns and what an
    /// exported JSON Lines segment carries (section 8.1).
    #[must_use]
    pub fn value_with_hash(&self, hash: &str) -> Value {
        let mut object = self.object();
        object.insert("hash".into(), Value::String(hash.to_string()));
        Value::Object(object)
    }

    /// `SHA-256(JCS(object))`, lowercase hex (section 5).
    ///
    /// # Errors
    ///
    /// A value JCS cannot canonicalize.
    pub fn hash(&self) -> Result<String, AppendError> {
        let canonical = jcs::canonicalize(&self.value()).map_err(AppendError::NotCanonical)?;
        Ok(sha256_hex(canonical.as_bytes()))
    }
}

/// Rebuilds [`HashedFields`] from one stored `audit_log` row, with `params`
/// parsed back from its canonical JSON text. This is the inverse of the object
/// [`append`] wrote, field for field.
///
/// # Errors
///
/// A missing or mistyped column, or `params` that does not parse as JSON.
pub fn fields_from_row(row: &SqliteRow) -> Result<HashedFields, sqlx::Error> {
    let params_text: String = row.try_get("params")?;
    let params = serde_json::from_str(&params_text)
        .map_err(|e| sqlx::Error::Protocol(format!("audit_log.params is not JSON: {e}")))?;
    Ok(HashedFields {
        seq: row.try_get("seq")?,
        time: row.try_get("time")?,
        actor_type: row.try_get("actor_type")?,
        actor_id: row.try_get("actor_id")?,
        agent_model: row.try_get("agent_model")?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        channel: row.try_get("channel")?,
        operation: row.try_get("operation")?,
        scope: row.try_get("scope")?,
        target_type: row.try_get("target_type")?,
        target_id: row.try_get("target_id")?,
        project_id: row.try_get("project_id")?,
        params,
        params_sha256: row.try_get("params_sha256")?,
        params_bytes: row.try_get("params_bytes")?,
        justification: row.try_get("justification")?,
        trigger_event_id: row.try_get("trigger_event_id")?,
        incident_id: row.try_get("incident_id")?,
        grant_id: row.try_get("grant_id")?,
        approval_id: row.try_get("approval_id")?,
        request_id: row.try_get("request_id")?,
        result: row.try_get("result")?,
        result_detail: row.try_get("result_detail")?,
        change_seq: row.try_get("change_seq")?,
        prev_hash: row.try_get("prev_hash")?,
    })
}

/// The complete stored record as JSON: [`fields_from_row`] plus the stored
/// `hash`. Optional absent fields are omitted, as the contract requires.
///
/// # Errors
///
/// A missing or mistyped column, or `params` that does not parse as JSON.
pub fn record_value_from_row(row: &SqliteRow) -> Result<Value, sqlx::Error> {
    let fields = fields_from_row(row)?;
    let hash: String = row.try_get("hash")?;
    Ok(fields.value_with_hash(&hash))
}

/// One stored record as a line of an exported segment (section 8.1), without
/// the newline: the complete record in RFC 8785 form, with `params` written
/// as the text stored for them.
///
/// `quoro audit verify --file` then reads each line's `params` from the same
/// text `--db` reads from the row, so for every record the line reads back
/// exactly as the stored record does, and verifies offline (section 6.1)
/// exactly when the stored record verifies. Writing `params` in the RFC 8785
/// form of the value read back from that text instead can give another text:
/// a float can read back as a number whose RFC 8785 form differs from the
/// stored one (`72096104873994100000` reads back as a number written
/// `72096104873994090000`).
///
/// # Errors
///
/// A missing or mistyped column, `params` that does not parse as JSON, or a
/// field with no RFC 8785 form.
pub fn export_line(row: &SqliteRow) -> Result<String, sqlx::Error> {
    let params_text: String = row.try_get("params")?;
    let record = record_value_from_row(row)?;
    let Value::Object(members) = record else {
        return Err(sqlx::Error::Protocol(
            "an audit record is a JSON object".into(),
        ));
    };
    // RFC 8785 orders members by the UTF-16 code units of their names; the
    // record's names are ASCII, so their byte order is that order.
    let mut names: Vec<&String> = members.keys().collect();
    names.sort();
    let mut line = String::from("{");
    for (index, name) in names.into_iter().enumerate() {
        if index > 0 {
            line.push(',');
        }
        line.push_str(&canonical(&Value::String(name.clone()))?);
        line.push(':');
        if name == "params" {
            line.push_str(&params_text);
        } else {
            line.push_str(&canonical(&members[name])?);
        }
    }
    line.push('}');
    Ok(line)
}

/// [`jcs::canonicalize`] with its error as a row error, for [`export_line`].
fn canonical(value: &Value) -> Result<String, sqlx::Error> {
    jcs::canonicalize(value)
        .map_err(|e| sqlx::Error::Protocol(format!("audit record field: {}", e.0)))
}

/// Appends `record` and returns its audit `seq`. Must run in the same write
/// transaction as the change it describes (section 3.1).
///
/// The record's change-log entry (section 3.2) is built and checked before
/// anything is written, with the same check `changelog::append` makes
/// ([`changelog::prepare`]). When the entry, built with the redacted
/// parameters, is one the primary may not write (ADR 0302), or the parameters
/// have no RFC 8785 form at all, the record keeps `params` `{}` with
/// `params_sha256` and `params_bytes` instead (section 4.1, ADR 0306). There
/// is no other threshold: parameters that fit and verify are kept.
///
/// # Errors
///
/// A database error. A record that keeps the digest has no field left that
/// could make its entry one a node cannot receive; should it still be one,
/// that error is returned.
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
    let mut fields = HashedFields {
        seq,
        time: time.clone(),
        actor_type: record.actor.kind.as_str().to_string(),
        actor_id: record.actor.id.clone(),
        agent_model: record.agent_model.clone(),
        on_behalf_of: record.on_behalf_of.clone(),
        channel: record.channel.as_str().to_string(),
        operation: record.operation.clone(),
        scope: record.scope.clone(),
        target_type: record.target_type.clone(),
        target_id: record.target_id.clone(),
        project_id: record.project_id.clone(),
        params: redact(&record.params),
        params_sha256: None,
        params_bytes: None,
        justification: record.justification.clone(),
        trigger_event_id: record.trigger_event_id.clone(),
        incident_id: record.incident_id.clone(),
        grant_id: record.grant_id.clone(),
        approval_id: record.approval_id.clone(),
        request_id: record.request_id.clone(),
        result: record.result.as_str().to_string(),
        result_detail: record.result_detail.clone(),
        change_seq: record.change_seq,
        prev_hash: prev_hash.clone(),
    };
    let (hash, entry) = match carried(conn, &record.actor, &fields).await {
        Ok(carried) => carried,
        Err(error) if kept_as_digest(&error) => {
            // Section 4.1: the text is the redacted parameters as the primary
            // read them, in their RFC 8785 form with exact integers beyond
            // 2^53 written out; it exists for every value.
            let text = jcs::canonicalize_lenient(&fields.params);
            fields.params = Value::Object(Map::new());
            fields.params_sha256 = Some(sha256_hex(text.as_bytes()));
            fields.params_bytes = Some(i64::try_from(text.len()).unwrap_or(i64::MAX));
            // With `params` `{}` the entry carries no number but `seq`,
            // `change_seq` and `params_bytes`, integers far below 2^53, so it
            // has an RFC 8785 form that reads back, and parses on a node, as
            // itself. Its other fields are strings: ids, hashes, times and
            // operation templates the server writes, and the request's path
            // and headers (target ids, the justification, the incident id),
            // which the HTTP server bounds to its header limit, well under
            // the 1 MiB message limit even with every character escaped. So a
            // node can receive the entry alone and verify it, and the audit
            // write does not fail because of its parameters. Were that ever
            // untrue, the error is returned rather than a record written
            // without its entry.
            carried(conn, &record.actor, &fields).await?
        }
        Err(error) => return Err(error),
    };
    // `carried` hashed these fields, so they have an RFC 8785 form.
    let params_text = jcs::canonicalize(&fields.params).map_err(AppendError::NotCanonical)?;

    sqlx::query(
        "INSERT INTO audit_log (seq, time, actor_type, actor_id, agent_model, on_behalf_of, channel, operation, scope, \
         target_type, target_id, project_id, params, params_sha256, params_bytes, justification, trigger_event_id, \
         incident_id, grant_id, approval_id, request_id, result, result_detail, change_seq, prev_hash, hash) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    .bind(&fields.params_sha256)
    .bind(fields.params_bytes)
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

    // Section 3.2: the record travels to replicas as the change-log entry
    // `carried` built and checked.
    changelog::insert(conn, &entry).await?;
    Ok(seq)
}

/// The record's `hash` (section 5) and the change-log entry that carries the
/// record with that hash (section 3.2), as `changelog::append` would write it
/// next, once [`changelog::prepare`] has shown a node can receive it alone and
/// verify it.
///
/// # Errors
///
/// [`AppendError::NotCanonical`] for a record with no RFC 8785 form, the
/// errors of [`changelog::prepare`], or a database error.
async fn carried(
    conn: &mut SqliteConnection,
    actor: &Actor,
    fields: &HashedFields,
) -> Result<(String, StoredEntry), AppendError> {
    let hash = fields.hash()?;
    let entry = changelog::prepare(
        conn,
        actor,
        "audit",
        &fields.seq.to_string(),
        Op::Create,
        fields.value_with_hash(&hash),
    )
    .await?;
    Ok((hash, entry))
}

/// Whether [`carried`] refused the record for what it carries, so that the
/// record keeps its parameters as a digest (section 4.1): it has no RFC 8785
/// form, or its entry is one the primary may not write (ADR 0302). A database
/// error is not one.
fn kept_as_digest(error: &AppendError) -> bool {
    matches!(
        error,
        AppendError::NotCanonical(_)
            | AppendError::Unverifiable(_)
            | AppendError::FrameTooLarge { .. }
    )
}

/// Why a link of the chain does not verify (section 6).
pub const SEQ_GAP: &str = "seq_gap";
pub const HASH_MISMATCH: &str = "hash_mismatch";
pub const PREV_MISMATCH: &str = "prev_mismatch";

/// Outcome of [`verify_records`], the fields of `AuditVerificationResult`
/// (section 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verification {
    /// True only if every record in the walk verified.
    pub ok: bool,
    /// Records verified before the walk stopped.
    pub checked: i64,
    /// First record walked.
    pub first_seq: i64,
    /// Last record in the walked range (its `seq`), even after a failure, so
    /// `head_hash` names the range's end (section 6).
    pub last_seq: i64,
    /// `hash` of `last_seq`; the seed when the range holds no record.
    pub head_hash: String,
    /// First record that failed, absent when `ok`.
    pub broken_at_seq: Option<i64>,
    /// [`SEQ_GAP`], [`HASH_MISMATCH`] or [`PREV_MISMATCH`], absent when `ok`.
    pub reason: Option<&'static str>,
    /// The hash the chain implied at the broken record (for the
    /// `audit.chain_broken` event).
    pub expected_hash: Option<String>,
    /// The hash the record actually carried (for the event).
    pub found_hash: Option<String>,
}

/// Re-walks a chain of record objects exactly as section 6 specifies. Each
/// item is a complete record including `hash`; `first_seq` is the seq the walk
/// expects to start at, and `seed_prev_hash` the link record `first_seq - 1`
/// implies (64 zeros for a chain that starts at 1).
///
/// The iterator is consumed to its end even after a failure, so `last_seq` and
/// `head_hash` name the end of the walked range as the contract's failure
/// example does; only `checked` stops at the first broken record.
#[must_use]
pub fn verify_records(
    records: impl IntoIterator<Item = Value>,
    first_seq: i64,
    seed_prev_hash: &str,
) -> Verification {
    let mut prev = seed_prev_hash.to_string();
    let mut expect_seq = first_seq;
    let mut checked = 0;
    let mut last_seq = first_seq;
    let mut head_hash = seed_prev_hash.to_string();
    let mut failure: Option<(&'static str, i64, String, String)> = None;

    for record in records {
        let seq = record
            .get("seq")
            .and_then(Value::as_i64)
            .unwrap_or(expect_seq);
        let stored_hash = record
            .get("hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let stored_prev = record
            .get("prev_hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        last_seq = seq;
        head_hash.clone_from(&stored_hash);
        if failure.is_some() {
            continue;
        }
        if seq != expect_seq {
            failure = Some((SEQ_GAP, seq, prev.clone(), stored_prev));
            continue;
        }
        let mut object = record;
        if let Value::Object(map) = &mut object {
            map.remove("hash");
        }
        // A record JCS cannot canonicalize cannot match its stored hash
        // either; hash the raw JSON so the event still carries 64 hex chars.
        let computed = match jcs::canonicalize(&object) {
            Ok(canonical) => sha256_hex(canonical.as_bytes()),
            Err(_) => sha256_hex(object.to_string().as_bytes()),
        };
        if computed != stored_hash {
            failure = Some((HASH_MISMATCH, seq, computed, stored_hash));
            continue;
        }
        if stored_prev != prev {
            failure = Some((PREV_MISMATCH, seq, prev.clone(), stored_prev));
            continue;
        }
        prev = stored_hash;
        expect_seq += 1;
        checked += 1;
    }

    let ok = failure.is_none();
    let (reason, broken_at_seq, expected_hash, found_hash) = match failure {
        Some((reason, seq, expected, found)) => {
            (Some(reason), Some(seq), Some(expected), Some(found))
        }
        None => (None, None, None, None),
    };
    Verification {
        ok,
        checked,
        first_seq,
        last_seq,
        head_hash,
        broken_at_seq,
        reason,
        expected_hash,
        found_hash,
    }
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
            params: json!({
                "email": "a@example.com",
                "password": "hunter2hunter2",
                "current_password": "old-secret-value",
                "new_password": "new-secret-value",
                "nested": [{ "token": "x" }],
            }),
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
        assert!(!rows[0].1.contains("old-secret-value"));
        assert!(!rows[0].1.contains("new-secret-value"));
        assert!(rows[0].1.contains("[redacted]"));
        assert!(!rows[0].1.contains("\"x\""));

        let audit_entries: (i64,) =
            sqlx::query_as("SELECT count(*) FROM change_log WHERE entity_type = 'audit'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(audit_entries.0, 2);
    }

    async fn stored_records(store: &Store) -> Vec<Value> {
        let rows = sqlx::query(&format!("SELECT {COLUMNS} FROM audit_log ORDER BY seq"))
            .fetch_all(store.pool())
            .await
            .unwrap();
        rows.iter()
            .map(|row| record_value_from_row(row).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn verify_accepts_a_clean_chain_and_names_the_edited_record() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        for result in [Outcome::Ok, Outcome::Refused, Outcome::Ok] {
            append(&mut w.tx, &record(result)).await.unwrap();
        }
        w.commit().await.unwrap();

        let clean = verify_records(stored_records(&store).await, 1, GENESIS_HASH);
        assert!(clean.ok, "{clean:?}");
        assert_eq!(clean.checked, 3);
        assert_eq!(clean.first_seq, 1);
        assert_eq!(clean.last_seq, 3);
        assert_eq!(clean.reason, None);

        // The append-only triggers would refuse an edit, which is exactly the
        // contract; a tampered copy drops them first (section 7).
        for trigger in [
            "audit_log_append_only_update",
            "audit_log_append_only_delete",
        ] {
            sqlx::query(&format!("DROP TRIGGER {trigger}"))
                .execute(store.pool())
                .await
                .unwrap();
        }
        sqlx::query("UPDATE audit_log SET justification = 'edited after the fact' WHERE seq = 2")
            .execute(store.pool())
            .await
            .unwrap();
        let edited = verify_records(stored_records(&store).await, 1, GENESIS_HASH);
        assert!(!edited.ok);
        assert_eq!(edited.checked, 1);
        assert_eq!(edited.broken_at_seq, Some(2));
        assert_eq!(edited.reason, Some(HASH_MISMATCH));
        assert_eq!(edited.last_seq, 3);

        // Re-hashing the edited record isolates the chain link: the object is
        // self-consistent, but its `prev_hash` no longer names record 1.
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM audit_log WHERE seq = 2"))
            .fetch_one(store.pool())
            .await
            .unwrap();
        let mut fields = fields_from_row(&row).unwrap();
        fields.justification = None;
        fields.prev_hash = "1".repeat(64);
        let hash = fields.hash().unwrap();
        sqlx::query(
            "UPDATE audit_log SET justification = NULL, prev_hash = ?, hash = ? WHERE seq = 2",
        )
        .bind(&fields.prev_hash)
        .bind(&hash)
        .execute(store.pool())
        .await
        .unwrap();
        let relinked = verify_records(stored_records(&store).await, 1, GENESIS_HASH);
        assert!(!relinked.ok);
        assert_eq!(relinked.broken_at_seq, Some(2));
        assert_eq!(relinked.reason, Some(PREV_MISMATCH));
        assert_eq!(
            relinked.expected_hash,
            Some(fields_of_seq_1_hash(&store).await)
        );
        assert_eq!(relinked.found_hash, Some("1".repeat(64)));
    }

    async fn fields_of_seq_1_hash(store: &Store) -> String {
        let (hash,): (String,) = sqlx::query_as("SELECT hash FROM audit_log WHERE seq = 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
        hash
    }

    /// Lowercase hex SHA-256, computed here rather than through the code
    /// under test.
    fn sha256(text: &str) -> String {
        use sha2::Digest as _;
        sha2::Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// Request parameters exactly as the primary reads them from a body.
    fn read(body: &str) -> Value {
        serde_json::from_str(body).unwrap()
    }

    /// Appends one refused record carrying `params` after an ordinary one,
    /// commits, and returns the stored record.
    async fn append_with(store: &Store, params: Value) -> Value {
        let mut w = store.begin_write().await.unwrap();
        append(&mut w.tx, &record(Outcome::Ok)).await.unwrap();
        let mut with_params = record(Outcome::Refused);
        with_params.params = params;
        let seq = append(&mut w.tx, &with_params).await.unwrap();
        w.commit().await.unwrap();
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM audit_log WHERE seq = ?"))
            .bind(seq)
            .fetch_one(store.pool())
            .await
            .unwrap();
        record_value_from_row(&row).unwrap()
    }

    /// The record's change-log entry (section 3.2), parsed from its stored
    /// payload.
    async fn carried_payload(store: &Store, seq: i64) -> Value {
        let (payload,): (String,) = sqlx::query_as(
            "SELECT payload FROM change_log WHERE entity_type = 'audit' AND entity_id = ?",
        )
        .bind(seq.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
        serde_json::from_str(&payload).unwrap()
    }

    /// A record that keeps the digest of `text` (section 4.1): `params` is
    /// `{}`, the two fields name `text`, the stored record hashes to its
    /// `hash`, its change-log entry was written with it, and the stored chain
    /// verifies offline, as `quoro audit verify` walks it, and so does the
    /// change log.
    async fn assert_kept_as_digest(store: &Store, stored: &Value, text: &str) {
        assert_eq!(stored["params"], json!({}), "{stored}");
        assert_eq!(stored["params_sha256"], json!(sha256(text)), "{text}");
        assert_eq!(stored["params_bytes"], json!(text.len()), "{text}");

        let mut unhashed = stored.clone();
        let hash = unhashed.as_object_mut().unwrap().remove("hash").unwrap();
        assert_eq!(
            hash,
            json!(sha256(&jcs::canonicalize(&unhashed).unwrap())),
            "the stored record hashes to its hash"
        );

        let seq = stored["seq"].as_i64().unwrap();
        assert_eq!(&carried_payload(store, seq).await, stored);

        let chain = verify_records(stored_records(store).await, 1, GENESIS_HASH);
        assert!(chain.ok, "{chain:?}");
        assert_eq!(chain.checked, seq);
        let mut conn = store.pool().acquire().await.unwrap();
        assert_eq!(changelog::verify(&mut conn).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_whole_number_float_above_2_53_keeps_its_digest() {
        // `1e16` is read as a float; RFC 8785 writes it as an integer that
        // reads back above 2^53 (section 4.1, first case).
        let store = Store::open_in_memory().await.unwrap();
        let stored = append_with(&store, read(r#"{"n": 1e16}"#)).await;
        assert_eq!(stored["params_bytes"], json!(23));
        assert_kept_as_digest(&store, &stored, r#"{"n":10000000000000000}"#).await;
    }

    #[tokio::test]
    async fn an_exact_integer_beyond_2_53_keeps_its_digest() {
        // Read exactly, it has no RFC 8785 form (section 4.1, second case);
        // the text writes it out.
        let store = Store::open_in_memory().await.unwrap();
        let stored = append_with(&store, read(r#"{"n": 18446744073709551615}"#)).await;
        assert_eq!(stored["params_bytes"], json!(26));
        assert_kept_as_digest(&store, &stored, r#"{"n":18446744073709551615}"#).await;
    }

    #[tokio::test]
    async fn a_float_a_node_reads_as_another_number_keeps_its_digest() {
        // A node that parses the entry reads it back as another number
        // (section 4.1, third case); the text is the RFC 8785 form of the
        // number the primary read.
        let store = Store::open_in_memory().await.unwrap();
        let params = read(r#"{"x": 18983552425072.563}"#);
        let text = jcs::canonicalize(&params).unwrap();
        assert_eq!(text, r#"{"x":18983552425072.563}"#);
        let stored = append_with(&store, params).await;
        assert_kept_as_digest(&store, &stored, &text).await;
    }

    #[tokio::test]
    async fn parameters_near_1_mib_keep_their_redacted_digest() {
        // Alone in a `log_entries` message the entry would pass the 1 MiB
        // limit. The digest covers the parameters after redaction.
        let store = Store::open_in_memory().await.unwrap();
        let blob = "x".repeat(1_048_000);
        let stored = append_with(
            &store,
            json!({ "blob": blob, "password": "hunter2hunter2" }),
        )
        .await;
        let text = format!(r#"{{"blob":"{blob}","password":"[redacted]"}}"#);
        assert_kept_as_digest(&store, &stored, &text).await;
        assert!(!stored.to_string().contains("hunter2"));
    }

    #[tokio::test]
    async fn ordinary_parameters_are_kept_and_hash_as_before() {
        // Several hundred KiB, safe integers and short floats: the entry fits
        // and verifies, so the parameters are kept whatever their size.
        let store = Store::open_in_memory().await.unwrap();
        let params = json!({
            "blob": "y".repeat(400_000),
            "big": 9_007_199_254_740_991_i64,
            "score": 9.8,
            "whole": 10.0,
            "password": "hunter2hunter2",
        });
        let stored = append_with(&store, params.clone()).await;
        let mut redacted = params;
        redacted["password"] = json!("[redacted]");
        // Stored as `10`, `10.0` reads back as an integer; compare the forms.
        assert_eq!(
            jcs::canonicalize(&stored["params"]).unwrap(),
            jcs::canonicalize(&redacted).unwrap()
        );
        assert!(stored.get("params_sha256").is_none(), "{stored}");
        assert!(stored.get("params_bytes").is_none(), "{stored}");

        // The hash covers the fields a record had before the digest existed,
        // and no other.
        let before = json!({
            "seq": stored["seq"], "time": stored["time"], "actor_type": "user",
            "actor_id": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "channel": "panel",
            "operation": "POST /api/v1/setup", "target_type": "user",
            "target_id": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "params": redacted,
            "request_id": "req_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "result": "refused",
            "prev_hash": stored["prev_hash"],
        });
        assert_eq!(
            stored["hash"],
            json!(sha256(&jcs::canonicalize(&before).unwrap()))
        );
        let seq = stored["seq"].as_i64().unwrap();
        assert_eq!(carried_payload(&store, seq).await, stored);
        let chain = verify_records(stored_records(&store).await, 1, GENESIS_HASH);
        assert!(chain.ok, "{chain:?}");
    }
}
