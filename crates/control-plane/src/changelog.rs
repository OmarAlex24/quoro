//! The numbered, hash-chained change log (D-0013, D-0015).
//!
//! Every state change appends one entry in the same transaction that updates
//! the state tables. `hash` is `SHA-256(JCS(entry without hash))`, the same
//! construction as the audit log (`audit-log-format.md` section 5). The
//! schema's triggers enforce `seq` contiguity and the `prev_hash` link.

use crate::jcs;
use crate::store::{GENESIS_HASH, now_rfc3339, sha256_hex};
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// `term` is 1 until elections exist (D-0016).
pub const TERM: i64 = 1;

/// Kind of identity behind a change (`change_log.actor_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorType {
    User,
    Agent,
    Node,
    System,
}

impl ActorType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Node => "node",
            Self::System => "system",
        }
    }
}

/// Who caused a change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Actor {
    pub kind: ActorType,
    /// `usr_`, `agt_`, `nod_` id, or the subsystem name for `system`.
    pub id: String,
}

impl Actor {
    #[must_use]
    pub fn system(subsystem: &str) -> Self {
        Self {
            kind: ActorType::System,
            id: subsystem.to_string(),
        }
    }
}

/// The operation on the entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Create,
    Update,
    Delete,
}

impl Op {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Error appending to a hash chain.
#[derive(Debug)]
pub enum AppendError {
    Db(sqlx::Error),
    NotCanonical(jcs::NotCanonical),
}

impl From<sqlx::Error> for AppendError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "database: {e}"),
            Self::NotCanonical(e) => write!(f, "not canonical: {}", e.0),
        }
    }
}

impl std::error::Error for AppendError {}

/// Appends one entry and returns its `seq`. Must run inside the caller's
/// write transaction, together with the state-table change it describes.
///
/// # Errors
///
/// A database error, or a payload with a value JCS cannot canonicalize.
pub async fn append(
    conn: &mut SqliteConnection,
    actor: &Actor,
    entity_type: &str,
    entity_id: &str,
    op: Op,
    payload: Value,
) -> Result<i64, AppendError> {
    let last: Option<(i64, String)> =
        sqlx::query_as("SELECT seq, hash FROM change_log ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (seq, prev_hash) = match last {
        Some((seq, hash)) => (seq + 1, hash),
        None => (1, GENESIS_HASH.to_string()),
    };
    let time = now_rfc3339();
    let entry = json!({
        "seq": seq,
        "term": TERM,
        "time": time,
        "actor_type": actor.kind.as_str(),
        "actor_id": actor.id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "op": op.as_str(),
        "payload": payload,
        "prev_hash": prev_hash,
    });
    let canonical = jcs::canonicalize(&entry).map_err(AppendError::NotCanonical)?;
    let hash = sha256_hex(canonical.as_bytes());
    let payload_text = jcs::canonicalize(&entry["payload"]).map_err(AppendError::NotCanonical)?;
    sqlx::query(
        "INSERT INTO change_log (seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, prev_hash, hash) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(seq)
    .bind(TERM)
    .bind(&time)
    .bind(actor.kind.as_str())
    .bind(&actor.id)
    .bind(entity_type)
    .bind(entity_id)
    .bind(op.as_str())
    .bind(&payload_text)
    .bind(&prev_hash)
    .bind(&hash)
    .execute(&mut *conn)
    .await?;
    Ok(seq)
}

/// Re-hashes the whole change log from `seq` 1 and returns the first `seq`
/// whose `prev_hash` or `hash` does not match, or `None` when it is intact.
///
/// # Errors
///
/// A database error, or a stored payload that is not valid JSON.
pub async fn verify(conn: &mut SqliteConnection) -> Result<Option<i64>, AppendError> {
    type Row = (
        i64,
        i64,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, prev_hash, hash \
         FROM change_log ORDER BY seq",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut expected_prev = GENESIS_HASH.to_string();
    for (seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, prev, hash) in
        rows
    {
        let payload: Value = serde_json::from_str(&payload)
            .map_err(|e| AppendError::NotCanonical(jcs::NotCanonical(e.to_string())))?;
        let entry = json!({
            "seq": seq, "term": term, "time": time, "actor_type": actor_type,
            "actor_id": actor_id, "entity_type": entity_type, "entity_id": entity_id,
            "op": op, "payload": payload, "prev_hash": prev,
        });
        let canonical = jcs::canonicalize(&entry).map_err(AppendError::NotCanonical)?;
        if prev != expected_prev || sha256_hex(canonical.as_bytes()) != hash {
            return Ok(Some(seq));
        }
        expected_prev = hash;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[tokio::test]
    async fn entries_chain_and_verify() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let actor = Actor::system("test");
        let a = append(
            &mut w.tx,
            &actor,
            "vm",
            "vm_1",
            Op::Create,
            json!({"b": 1, "a": "x"}),
        )
        .await
        .unwrap();
        let b = append(&mut w.tx, &actor, "vm", "vm_1", Op::Delete, json!({}))
            .await
            .unwrap();
        assert_eq!((a, b), (1, 2));
        assert_eq!(verify(&mut w.tx).await.unwrap(), None);
        w.commit().await.unwrap();

        // Tampering is detected at the exact seq. The append-only trigger
        // blocks UPDATE, so tamper by dropping the trigger first, as an
        // attacker with file access could.
        let mut conn = store.pool().acquire().await.unwrap();
        sqlx::query("DROP TRIGGER change_log_append_only_update")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("UPDATE change_log SET payload = '{\"a\":\"y\",\"b\":1}' WHERE seq = 1")
            .execute(&mut *conn)
            .await
            .unwrap();
        assert_eq!(verify(&mut conn).await.unwrap(), Some(1));
    }
}
