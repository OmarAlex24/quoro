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

/// The columns of a `change_log` row, in [`Row`] order.
const COLUMNS: &str =
    "seq, term, time, actor_type, actor_id, entity_type, entity_id, op, payload, prev_hash, hash";

/// One `change_log` row as `sqlx` reads it, in [`COLUMNS`] order.
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

/// One stored `change_log` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredEntry {
    pub seq: i64,
    pub term: i64,
    pub time: String,
    pub actor_type: String,
    pub actor_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub op: String,
    /// The payload's stored canonical text.
    pub payload: String,
    pub prev_hash: String,
    pub hash: String,
}

impl From<Row> for StoredEntry {
    fn from(row: Row) -> Self {
        let (
            seq,
            term,
            time,
            actor_type,
            actor_id,
            entity_type,
            entity_id,
            op,
            payload,
            prev,
            hash,
        ) = row;
        Self {
            seq,
            term,
            time,
            actor_type,
            actor_id,
            entity_type,
            entity_id,
            op,
            payload,
            prev_hash: prev,
            hash,
        }
    }
}

impl StoredEntry {
    /// The entry without `hash`, with `payload` parsed from its stored text:
    /// what `hash` covers. [`verify`] and change-log follow both build it
    /// here, so the entry a node verifies is the one the primary verifies.
    ///
    /// # Errors
    ///
    /// A stored payload that is not valid JSON.
    pub(crate) fn unhashed(&self) -> Result<Value, AppendError> {
        let payload: Value = serde_json::from_str(&self.payload)
            .map_err(|e| AppendError::NotCanonical(jcs::NotCanonical(e.to_string())))?;
        Ok(json!({
            "seq": self.seq, "term": self.term, "time": self.time, "actor_type": self.actor_type,
            "actor_id": self.actor_id, "entity_type": self.entity_type, "entity_id": self.entity_id,
            "op": self.op, "payload": payload, "prev_hash": self.prev_hash,
        }))
    }

    /// The entry as `log_entries` carries it: the 11 change-log fields of
    /// `node-agent-protocol.md` section 5.14, [`StoredEntry::unhashed`] with
    /// the stored `hash`.
    ///
    /// # Errors
    ///
    /// A stored payload that is not valid JSON, or an entry without an RFC
    /// 8785 form. The node verifies each entry over that form (section 7.1),
    /// so it could not verify such an entry: a whole-number float above 2^53
    /// in a payload is stored as a plain integer, and parsing it again gives
    /// an integer that has no canonical form.
    pub(crate) fn wire_entry(&self) -> Result<Value, AppendError> {
        let mut entry = self.unhashed()?;
        jcs::canonicalize(&entry).map_err(AppendError::NotCanonical)?;
        if let Value::Object(fields) = &mut entry {
            fields.insert("hash".to_string(), Value::String(self.hash.clone()));
        }
        Ok(entry)
    }
}

/// The newest `seq` (0 for an empty log), and up to `limit` entries after
/// `after` in `seq` order, none above that newest `seq`: the next
/// `log_entries` of a follower at `after` (`node-agent-protocol.md` section
/// 5.14). The newest `seq` is read first, so it is never below the last entry
/// returned.
///
/// # Errors
///
/// A database error.
pub(crate) async fn read_after(
    conn: &mut SqliteConnection,
    after: i64,
    limit: usize,
) -> Result<(i64, Vec<StoredEntry>), sqlx::Error> {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(&mut *conn)
        .await?;
    let head = head.unwrap_or(0);
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM change_log WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?"
    ))
    .bind(after)
    .bind(head)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *conn)
    .await?;
    Ok((head, rows.into_iter().map(StoredEntry::from).collect()))
}

/// The stored `hash` of the entry at `seq`, or `None` when the log holds no
/// such entry.
///
/// # Errors
///
/// A database error.
pub(crate) async fn hash_at(
    conn: &mut SqliteConnection,
    seq: i64,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT hash FROM change_log WHERE seq = ?")
        .bind(seq)
        .fetch_optional(conn)
        .await
}

/// Re-hashes the whole change log from `seq` 1 and returns the first `seq`
/// whose `prev_hash` or `hash` does not match, or `None` when it is intact.
///
/// # Errors
///
/// A database error, or a stored payload that is not valid JSON.
pub async fn verify(conn: &mut SqliteConnection) -> Result<Option<i64>, AppendError> {
    let rows: Vec<Row> = sqlx::query_as(&format!("SELECT {COLUMNS} FROM change_log ORDER BY seq"))
        .fetch_all(&mut *conn)
        .await?;
    let mut expected_prev = GENESIS_HASH.to_string();
    for entry in rows.into_iter().map(StoredEntry::from) {
        let canonical = jcs::canonicalize(&entry.unhashed()?).map_err(AppendError::NotCanonical)?;
        if entry.prev_hash != expected_prev || sha256_hex(canonical.as_bytes()) != entry.hash {
            return Ok(Some(entry.seq));
        }
        expected_prev = entry.hash;
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

    #[tokio::test]
    async fn stored_entries_rebuild_the_hashed_entry() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let actor = Actor::system("test");
        for (i, payload) in [
            json!({"name": "blog", "score": 9.8, "whole": 10.0}),
            json!({"nested": {"b": [1, 2], "a": "\u{e9}"}}),
            json!({}),
        ]
        .into_iter()
        .enumerate()
        {
            append(
                &mut w.tx,
                &actor,
                "vm",
                &format!("vm_{i}"),
                Op::Update,
                payload,
            )
            .await
            .unwrap();
        }
        let (head, entries) = read_after(&mut w.tx, 1, 500).await.unwrap();
        assert_eq!(head, 3);
        assert_eq!(
            entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
            [2, 3]
        );
        let (_, all) = read_after(&mut w.tx, 0, 2).await.unwrap();
        assert_eq!(all.len(), 2, "the limit holds");
        for entry in &all {
            let wire = entry.wire_entry().unwrap();
            assert_eq!(wire.as_object().unwrap().len(), 11, "{wire}");
            let mut unhashed = wire.clone();
            let hash = unhashed.as_object_mut().unwrap().remove("hash").unwrap();
            let canonical = jcs::canonicalize(&unhashed).unwrap();
            assert_eq!(hash, json!(sha256_hex(canonical.as_bytes())));
            assert_eq!(
                hash_at(&mut w.tx, entry.seq).await.unwrap(),
                Some(entry.hash.clone())
            );
        }
        assert_eq!(hash_at(&mut w.tx, 4).await.unwrap(), None);
        w.commit().await.unwrap();
    }

    #[test]
    fn an_entry_whose_payload_does_not_canonicalize_again_is_refused() {
        // A whole-number float above 2^53 is stored as a plain integer, which
        // parses back as an integer RFC 8785 refuses.
        let entry = StoredEntry {
            seq: 1,
            term: 1,
            time: "2026-11-02T10:15:31.000Z".to_string(),
            actor_type: "system".to_string(),
            actor_id: "test".to_string(),
            entity_type: "vm".to_string(),
            entity_id: "vm_1".to_string(),
            op: "update".to_string(),
            payload: "{\"n\":9007199254740993}".to_string(),
            prev_hash: GENESIS_HASH.to_string(),
            hash: GENESIS_HASH.to_string(),
        };
        assert!(entry.unhashed().is_ok());
        assert!(matches!(
            entry.wire_entry(),
            Err(AppendError::NotCanonical(_))
        ));
    }
}
