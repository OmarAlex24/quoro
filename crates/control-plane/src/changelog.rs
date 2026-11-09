//! The numbered, hash-chained change log (D-0013, D-0015).
//!
//! Every state change appends one entry in the same transaction that updates
//! the state tables. `hash` is `SHA-256(JCS(entry without hash))`, the same
//! construction as the audit log (`audit-log-format.md` section 5). The
//! schema's triggers enforce `seq` contiguity and the `prev_hash` link.
//!
//! An entry a node could not receive alone and verify is never written
//! (`node-agent-protocol.md` section 7.1, "Entries a node can receive", ADR
//! 0302): [`append`] refuses it.

use crate::jcs;
use crate::store::{GENESIS_HASH, now_rfc3339, sha256_hex};
use quoro_proto::wire::{self, ControlMessage};
use serde_json::{Value, json};
use sqlx::SqliteConnection;

/// `term` is 1 until elections exist (D-0016).
pub const TERM: i64 = 1;

/// The highest `seq` the change log carries, 2^53 - 1
/// (`node-agent-protocol.md` section 7.1).
pub(crate) const MAX_SEQ: u64 = (1 << 53) - 1;

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
    /// The change-log payload is not a JSON object, the only payload a node
    /// accepts (`node-agent-protocol.md` sections 5.14 and 7.1, "Entries a
    /// node can receive", ADR 0302).
    PayloadNotAnObject,
    /// A node could not verify the change-log entry: with its payload read
    /// back from the text the primary stores, it has no RFC 8785 form; or, as
    /// a node parses it from the `log_entries` message that carries it alone,
    /// it has none or does not hash to its `hash` (`node-agent-protocol.md`
    /// section 7.1, "Entries a node can receive", ADR 0302). The text says
    /// which.
    Unverifiable(String),
    /// The `log_entries` message that carries the change-log entry alone
    /// takes `bytes` bytes (`alone_frame_bytes`), above the 1 MiB message
    /// limit (`node-agent-protocol.md` sections 3.2 and 7.1, "Entries a node
    /// can receive", ADR 0302).
    FrameTooLarge {
        bytes: usize,
    },
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
            Self::PayloadNotAnObject => write!(f, "the change-log payload is not a JSON object"),
            Self::Unverifiable(reason) => {
                write!(f, "a node could not verify the change-log entry: {reason}")
            }
            Self::FrameTooLarge { bytes } => write!(
                f,
                "a log_entries message holding the change-log entry alone takes {bytes} bytes, above the {} byte message limit",
                wire::MAX_MESSAGE_BYTES
            ),
        }
    }
}

impl std::error::Error for AppendError {}

/// Appends one entry and returns its `seq`. Must run inside the caller's
/// write transaction, together with the state-table change it describes.
///
/// The entry is built from the payload exactly as it reads back from the
/// canonical text stored for it, the way `StoredEntry::unhashed` and
/// [`verify`] rebuild it, and hashed in that form. For the payloads Quoro
/// writes, whose only floats are scores with one decimal, the read-back form
/// is the original canonical form, so their hash is the one an entry had
/// before this check (ADR 0302).
///
/// An entry a node could not receive alone and verify is refused before
/// anything is written (`node-agent-protocol.md` section 7.1, "Entries a
/// node can receive", ADR 0302): one whose payload is not a JSON object; one
/// whose read-back form has no RFC 8785 form, such as a whole-number float
/// above 2^53, which is stored as a plain integer and reads back as one RFC
/// 8785 refuses; one whose `log_entries` message alone (`alone_frame_bytes`)
/// exceeds the 1 MiB message limit (section 3.2); and one that a node,
/// parsing that message, would not find hashing to its `hash`
/// (`StoredEntry::receivable_hash`). The caller's write then fails like any
/// other write error: its transaction is dropped, and the change log and its
/// head watch stay as they were.
///
/// # Errors
///
/// A database error, a payload with a value JCS cannot canonicalize,
/// [`AppendError::PayloadNotAnObject`], [`AppendError::Unverifiable`] or
/// [`AppendError::FrameTooLarge`].
pub async fn append(
    conn: &mut SqliteConnection,
    actor: &Actor,
    entity_type: &str,
    entity_id: &str,
    op: Op,
    payload: Value,
) -> Result<i64, AppendError> {
    if !payload.is_object() {
        return Err(AppendError::PayloadNotAnObject);
    }
    let last: Option<(i64, String)> =
        sqlx::query_as("SELECT seq, hash FROM change_log ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut *conn)
            .await?;
    let (seq, prev_hash) = match last {
        Some((seq, hash)) => (seq + 1, hash),
        None => (1, GENESIS_HASH.to_string()),
    };
    let mut entry = StoredEntry {
        seq,
        term: TERM,
        time: now_rfc3339(),
        actor_type: actor.kind.as_str().to_string(),
        actor_id: actor.id.clone(),
        entity_type: entity_type.to_string(),
        entity_id: entity_id.to_string(),
        op: op.as_str().to_string(),
        payload: jcs::canonicalize(&payload).map_err(AppendError::NotCanonical)?,
        prev_hash,
        hash: String::new(),
    };
    entry.hash = entry.receivable_hash()?;
    sqlx::query(&format!(
        "INSERT INTO change_log ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ))
    .bind(entry.seq)
    .bind(entry.term)
    .bind(&entry.time)
    .bind(&entry.actor_type)
    .bind(&entry.actor_id)
    .bind(&entry.entity_type)
    .bind(&entry.entity_id)
    .bind(&entry.op)
    .bind(&entry.payload)
    .bind(&entry.prev_hash)
    .bind(&entry.hash)
    .execute(&mut *conn)
    .await?;
    Ok(seq)
}

/// The `log_entries` frame (`node-agent-protocol.md` section 5.14), as
/// change-log follow sends it.
pub(crate) fn log_entries_frame(
    msg_id: &str,
    from_seq: u64,
    head_seq: u64,
    entries: Vec<Value>,
) -> String {
    ControlMessage::LogEntries(wire::LogEntries {
        from_seq,
        entries,
        head_seq: Some(head_seq),
    })
    .to_frame(msg_id, None)
    .to_text()
}

/// The [`log_entries_frame`] that carries `entry`, the entry at `seq` as
/// [`StoredEntry::wire_entry`] builds it, alone, in the longest envelope
/// follow can send it in: a message id as long as every `msg_` id
/// ([`wire::msg_id`]), `from_seq` at `seq`, `head_seq` at [`MAX_SEQ`], and no
/// `re`. The envelope adds `v`, `type` and `id` around `body`, and no other
/// field, no time among them. The frame follow sends for this entry alone is
/// therefore never larger.
fn alone_frame(seq: u64, entry: &Value) -> String {
    log_entries_frame(&wire::msg_id([0; 16]), seq, MAX_SEQ, vec![entry.clone()])
}

/// The size in bytes of [`alone_frame`].
///
/// [`append`] refuses an entry this puts above the 1 MiB message limit, and
/// follow sends a first entry alone when this is within it
/// (`node-agent-protocol.md` section 7.1, "Entries a node can receive", ADR
/// 0302).
pub(crate) fn alone_frame_bytes(seq: u64, entry: &Value) -> usize {
    alone_frame(seq, entry).len()
}

/// Checks the entry in `frame`, a [`log_entries_frame`] of one entry, the way
/// a node verifies it (`verify_batch` in `quoro-node-agent`): the frame text
/// parsed back as a node parses it, its `body.entries[0]` without `hash` in
/// RFC 8785 form, and the SHA-256 of that equal to `hash`. [`append`] writes
/// only an entry a node can verify (`node-agent-protocol.md` section 7.1,
/// "Entries a node can receive", ADR 0302).
///
/// The primary hashes the entry as it reads back from its stored text, but a
/// node parses it a second time, from the frame text. `serde_json` parses
/// floats without `float_roundtrip`, so a float can come back from that text
/// as another number.
///
/// # Errors
///
/// Why a node could not verify the entry.
fn node_verifies(frame: &str, hash: &str) -> Result<(), String> {
    let raw = wire::RawFrame::parse(frame).map_err(|e| format!("its frame does not parse: {e}"))?;
    let entry = match ControlMessage::from_raw(&raw) {
        Ok(ControlMessage::LogEntries(body)) => body.entries.into_iter().next(),
        Ok(other) => return Err(format!("its frame reads back as {}", other.kind())),
        Err(e) => return Err(format!("its frame does not read back: {e}")),
    };
    let Some(Value::Object(mut fields)) = entry else {
        return Err("its frame carries no entry object".to_string());
    };
    fields.remove("hash");
    let canonical = jcs::canonicalize(&Value::Object(fields))
        .map_err(|e| format!("as a node parses it, it has no RFC 8785 form: {}", e.0))?;
    if sha256_hex(canonical.as_bytes()) == hash {
        Ok(())
    } else {
        Err("as a node parses it, it does not hash to its hash".to_string())
    }
}

/// `unhashed` with `hash` added: the 11 fields of an entry on the wire.
fn with_hash(mut unhashed: Value, hash: &str) -> Value {
    if let Value::Object(fields) = &mut unhashed {
        fields.insert("hash".to_string(), Value::String(hash.to_string()));
    }
    unhashed
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
    /// what `hash` covers. [`append`] hashes it, and [`verify`] and
    /// change-log follow build it here too, so the entry follow sends is the
    /// one the primary hashed and verifies (ADR 0302). A node parses it once
    /// more, from the frame text; [`append`] also checks that it hashes the
    /// same there (`StoredEntry::receivable_hash`).
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
        let entry = self.unhashed()?;
        jcs::canonicalize(&entry).map_err(AppendError::NotCanonical)?;
        Ok(with_hash(entry, &self.hash))
    }

    /// The `hash` [`append`] stores for this entry, once a node is shown to
    /// be able to receive the entry alone and verify it
    /// (`node-agent-protocol.md` section 7.1, "Entries a node can receive",
    /// ADR 0302). The `hash` this entry holds is not read.
    ///
    /// The hash covers [`StoredEntry::unhashed`], the entry with its payload
    /// as read back from the stored text. The entry with that hash then goes
    /// into [`alone_frame`], which must be within the 1 MiB message limit,
    /// and a node that parses that frame must find an entry that hashes to it
    /// ([`node_verifies`]).
    ///
    /// # Errors
    ///
    /// [`AppendError::Unverifiable`] or [`AppendError::FrameTooLarge`]; a
    /// stored payload that is not valid JSON.
    fn receivable_hash(&self) -> Result<String, AppendError> {
        let unhashed = self.unhashed()?;
        let canonical = jcs::canonicalize(&unhashed).map_err(|e| {
            AppendError::Unverifiable(format!("as read back, it has no RFC 8785 form: {}", e.0))
        })?;
        let hash = sha256_hex(canonical.as_bytes());
        let frame = alone_frame(
            u64::try_from(self.seq).unwrap_or(MAX_SEQ),
            &with_hash(unhashed, &hash),
        );
        if frame.len() > wire::MAX_MESSAGE_BYTES {
            return Err(AppendError::FrameTooLarge { bytes: frame.len() });
        }
        node_verifies(&frame, &hash).map_err(AppendError::Unverifiable)?;
        Ok(hash)
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

    /// The newest `seq` and the number of rows of the change log.
    async fn log_state(conn: &mut SqliteConnection) -> (i64, i64) {
        sqlx::query_as("SELECT COALESCE(MAX(seq), 0), COUNT(*) FROM change_log")
            .fetch_one(conn)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_entry_whose_frame_alone_passes_the_message_limit_is_refused() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let actor = Actor::system("test");
        // Nine entries first, so the two below take two-digit seqs: a
        // measure with the wrong from_seq or head_seq is then off by bytes.
        let start = log_state(&mut w.tx).await.0;
        for i in 0..9 {
            append(
                &mut w.tx,
                &actor,
                "vm",
                &format!("vm_{i}"),
                Op::Update,
                json!({}),
            )
            .await
            .unwrap();
        }
        let seq = start + 10;
        assert!((10..=98).contains(&seq), "{seq}");
        // The worst-case envelope, written out: a 30-character msg_ id,
        // from_seq at the entry's seq, head_seq 2^53 - 1 and no re. The
        // payload as read back: 10.0 is stored, and so read back, as 10.
        let frame_of = |seq: i64, blob: &str| {
            let entry = json!({
                "seq": seq, "term": 1, "time": "2026-11-09T10:15:31.000Z",
                "actor_type": "system", "actor_id": "test", "entity_type": "vm",
                "entity_id": "vm_big", "op": "update",
                "payload": {"blob": blob, "whole": 10}, "prev_hash": "a".repeat(64),
                "hash": "b".repeat(64),
            });
            ControlMessage::LogEntries(wire::LogEntries {
                from_seq: u64::try_from(seq).unwrap(),
                entries: vec![entry],
                head_seq: Some(9_007_199_254_740_991),
            })
            .to_frame("msg_01JC9ZN1Q6C8D0F2H4K6N8Q0S2", None)
            .to_text()
            .len()
        };
        let room = wire::MAX_MESSAGE_BYTES - frame_of(seq, "");
        let payload = |len: usize| json!({"blob": "x".repeat(len), "whole": 10.0});

        // Exactly at the limit: accepted, and its frame is that size.
        let at = append(&mut w.tx, &actor, "vm", "vm_big", Op::Update, payload(room))
            .await
            .unwrap();
        assert_eq!(at, seq);
        let (_, rows) = read_after(&mut w.tx, seq - 1, 1).await.unwrap();
        let wire_entry = rows[0].wire_entry().unwrap();
        let sent = ControlMessage::LogEntries(wire::LogEntries {
            from_seq: u64::try_from(seq).unwrap(),
            entries: vec![wire_entry.clone()],
            head_seq: Some(9_007_199_254_740_991),
        })
        .to_frame("msg_01JC9ZN1Q6C8D0F2H4K6N8Q0S2", None)
        .to_text();
        assert_eq!(sent.len(), wire::MAX_MESSAGE_BYTES);
        assert_eq!(
            alone_frame_bytes(u64::try_from(seq).unwrap(), &wire_entry),
            wire::MAX_MESSAGE_BYTES
        );

        // One byte more: refused with the size, and nothing written.
        let refused = append(
            &mut w.tx,
            &actor,
            "vm",
            "vm_big",
            Op::Update,
            payload(room + 1),
        )
        .await;
        assert_eq!(
            frame_of(seq + 1, &"x".repeat(room + 1)),
            wire::MAX_MESSAGE_BYTES + 1
        );
        match refused {
            Err(AppendError::FrameTooLarge { bytes }) => {
                assert_eq!(bytes, wire::MAX_MESSAGE_BYTES + 1);
            }
            other => panic!("not refused for its size: {other:?}"),
        }
        assert_eq!(log_state(&mut w.tx).await, (seq, seq));
        assert_eq!(verify(&mut w.tx).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_whole_number_float_above_2_53_is_refused() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let payload = json!({"n": 1e16});
        // The caller's value has a canonical form; the integer it is stored
        // as, and reads back as, has none.
        assert_eq!(
            jcs::canonicalize(&payload).unwrap(),
            "{\"n\":10000000000000000}"
        );
        let before = log_state(&mut w.tx).await;
        let refused = append(
            &mut w.tx,
            &Actor::system("test"),
            "vm",
            "vm_1",
            Op::Update,
            payload,
        )
        .await;
        assert!(
            matches!(&refused, Err(AppendError::Unverifiable(reason)) if reason.starts_with("as read back")),
            "{refused:?}"
        );
        assert_eq!(log_state(&mut w.tx).await, before);
    }

    #[tokio::test]
    async fn a_float_a_node_parses_back_as_another_number_is_refused() {
        // Found by a search over 17-digit floats: serde_json, without
        // float_roundtrip, reads the stored text back as one number, and a
        // node reads that number's text in the frame back as another.
        let payload = json!({"x": 18_983_552_425_072.563});
        assert_eq!(
            jcs::canonicalize(&payload).unwrap(),
            "{\"x\":18983552425072.563}"
        );
        let read_back: Value = serde_json::from_str("{\"x\":18983552425072.563}").unwrap();
        let on_the_node: Value =
            serde_json::from_str(&serde_json::to_string(&read_back).unwrap()).unwrap();
        assert_ne!(
            read_back, on_the_node,
            "this float now parses back the same: pick another for this test"
        );
        // Both forms have a canonical form, so only the node's hash differs.
        assert!(jcs::canonicalize(&read_back).is_ok());
        assert!(jcs::canonicalize(&on_the_node).is_ok());

        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let before = log_state(&mut w.tx).await;
        let refused = append(
            &mut w.tx,
            &Actor::system("test"),
            "vm",
            "vm_1",
            Op::Update,
            payload,
        )
        .await;
        assert!(
            matches!(&refused, Err(AppendError::Unverifiable(reason)) if reason.contains("does not hash")),
            "{refused:?}"
        );
        assert_eq!(log_state(&mut w.tx).await, before);
    }

    #[tokio::test]
    async fn a_payload_that_is_not_an_object_is_refused() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let before = log_state(&mut w.tx).await;
        for payload in [json!([]), json!("x"), json!(1), json!(null), json!(true)] {
            let refused = append(
                &mut w.tx,
                &Actor::system("test"),
                "vm",
                "vm_1",
                Op::Update,
                payload,
            )
            .await;
            assert!(
                matches!(refused, Err(AppendError::PayloadNotAnObject)),
                "{refused:?}"
            );
        }
        assert_eq!(log_state(&mut w.tx).await, before);
    }

    #[test]
    fn the_message_limit_holds_at_a_long_seq() {
        // A 13-digit seq, measured without writing that many rows: the
        // measure must take from_seq at the entry's own seq.
        let seq: i64 = 1_234_567_890_123;
        let entry = |len: usize| StoredEntry {
            seq,
            term: TERM,
            time: "2026-11-09T10:15:31.000Z".to_string(),
            actor_type: "system".to_string(),
            actor_id: "test".to_string(),
            entity_type: "vm".to_string(),
            entity_id: "vm_big".to_string(),
            op: "update".to_string(),
            payload: format!("{{\"blob\":\"{}\"}}", "x".repeat(len)),
            prev_hash: "a".repeat(64),
            hash: String::new(),
        };
        // The worst-case envelope, written out: a 30-character msg_ id,
        // from_seq at the entry's seq, head_seq 2^53 - 1 and no re.
        let frame_of = |len: usize| {
            let sent = json!({
                "seq": 1_234_567_890_123_u64, "term": 1, "time": "2026-11-09T10:15:31.000Z",
                "actor_type": "system", "actor_id": "test", "entity_type": "vm",
                "entity_id": "vm_big", "op": "update", "payload": {"blob": "x".repeat(len)},
                "prev_hash": "a".repeat(64), "hash": "b".repeat(64),
            });
            ControlMessage::LogEntries(wire::LogEntries {
                from_seq: 1_234_567_890_123,
                entries: vec![sent],
                head_seq: Some(9_007_199_254_740_991),
            })
            .to_frame("msg_01JC9ZN1Q6C8D0F2H4K6N8Q0S2", None)
            .to_text()
            .len()
        };
        let at = wire::MAX_MESSAGE_BYTES - frame_of(0);
        assert_eq!(frame_of(at), wire::MAX_MESSAGE_BYTES);

        // Exactly at the limit: accepted, with the hash verify recomputes.
        let hash = entry(at).receivable_hash().unwrap();
        let canonical = jcs::canonicalize(&entry(at).unhashed().unwrap()).unwrap();
        assert_eq!(hash, sha256_hex(canonical.as_bytes()));

        // One byte more: refused with the size.
        match entry(at + 1).receivable_hash() {
            Err(AppendError::FrameTooLarge { bytes }) => {
                assert_eq!(bytes, wire::MAX_MESSAGE_BYTES + 1);
            }
            other => panic!("not refused for its size: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_refused_append_leaves_the_log_and_the_head_watch() {
        let store = Store::open_in_memory().await.unwrap();
        let actor = Actor::system("test");
        let mut w = store.begin_write().await.unwrap();
        append(&mut w.tx, &actor, "vm", "vm_1", Op::Create, json!({}))
            .await
            .unwrap();
        w.commit().await.unwrap();
        let mut head = store.change_log_head();
        let before = {
            let mut conn = store.pool().acquire().await.unwrap();
            log_state(&mut conn).await
        };
        assert_eq!(*head.borrow_and_update(), u64::try_from(before.0).unwrap());

        // A write that appends one entry, then one append refuses: the error
        // drops the transaction, as `?` in any caller does.
        let mut w = store.begin_write().await.unwrap();
        append(&mut w.tx, &actor, "vm", "vm_2", Op::Create, json!({}))
            .await
            .unwrap();
        let refused = append(
            &mut w.tx,
            &actor,
            "vm",
            "vm_2",
            Op::Update,
            json!({"blob": "x".repeat(wire::MAX_MESSAGE_BYTES)}),
        )
        .await;
        assert!(
            matches!(refused, Err(AppendError::FrameTooLarge { .. })),
            "{refused:?}"
        );
        // The refused entry wrote no row, even before the rollback.
        assert_eq!(log_state(&mut w.tx).await, (before.0 + 1, before.1 + 1));
        drop(w);

        let mut conn = store.pool().acquire().await.unwrap();
        assert_eq!(log_state(&mut conn).await, before);
        assert!(!head.has_changed().unwrap());
        assert_eq!(
            *store.change_log_head().borrow(),
            u64::try_from(before.0).unwrap()
        );
    }

    #[tokio::test]
    async fn an_ordinary_payload_keeps_the_hash_verify_recomputes() {
        let store = Store::open_in_memory().await.unwrap();
        let mut w = store.begin_write().await.unwrap();
        let actor = Actor::system("test");
        let payloads = [
            json!({"name": "blog", "score": 9.8, "whole": 10.0, "zero": -0.0}),
            json!({"nested": {"b": [1, 2, null, true], "a": "\u{e9}\n\"q\""}}),
            json!({"big": 9_007_199_254_740_991_i64, "neg": -9_007_199_254_740_991_i64}),
            json!({}),
        ];
        let start = log_state(&mut w.tx).await.0;
        for payload in &payloads {
            append(&mut w.tx, &actor, "vm", "vm_1", Op::Update, payload.clone())
                .await
                .unwrap();
        }
        let (_, rows) = read_after(&mut w.tx, start, 500).await.unwrap();
        assert_eq!(rows.len(), payloads.len());
        for (row, payload) in rows.iter().zip(&payloads) {
            // What verify recomputes from the stored row.
            let recomputed = sha256_hex(
                jcs::canonicalize(&row.unhashed().unwrap())
                    .unwrap()
                    .as_bytes(),
            );
            assert_eq!(row.hash, recomputed, "seq {}", row.seq);
            // The hash the entry had before append hashed the read-back
            // form: the same, over the caller's payload.
            let before = json!({
                "seq": row.seq, "term": TERM, "time": row.time, "actor_type": "system",
                "actor_id": "test", "entity_type": "vm", "entity_id": "vm_1",
                "op": "update", "payload": payload, "prev_hash": row.prev_hash,
            });
            assert_eq!(
                row.hash,
                sha256_hex(jcs::canonicalize(&before).unwrap().as_bytes()),
                "seq {}",
                row.seq
            );
        }
        assert_eq!(verify(&mut w.tx).await.unwrap(), None);
    }
}
