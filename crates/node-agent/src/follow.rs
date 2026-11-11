// SPDX-License-Identifier: AGPL-3.0-only
//! Change-log follow, the node's side (`node-agent-protocol.md` sections 5.14
//! and 7.1, ADR 0179, ADR 0227).
//!
//! The primary sends the change log in `log_entries` messages; the node checks
//! each one against the copy it holds with [`verify_batch`], stores the
//! entries ([`crate::store::NodeStore::store_log_batch`]) and acknowledges the
//! highest with one `log_ack` (section 5.6). A message that does not verify is
//! a primary bug: the node stores nothing from it and answers `bad_message`.
//!
//! Verification is strict on purpose. A message must start right after the
//! stored position, its entries must be numbered without a gap, and every
//! entry must link to the hash before it and hash to its own `hash`, so no
//! path stores an entry twice, out of order, or after a broken link (D-0013).
//!
//! The node also keeps the mark of section 7.1 (ADR 0261): the highest `seq`
//! it has acknowledged, in the file [`crate::identity::LOG_MARK_FILE`] outside
//! `node.db`, so a log that went back is noticed even when the store did not
//! survive. The mark is raised before every `log_ack` and `hello` that
//! reports a higher `seq` ([`raise_mark`]). It moves down only while the
//! node refollows after a quarantine: to [`MAX_SEQ`] before the copy is moved
//! aside, then to the `head_seq` it refollows to ([`lower_mark`], ADR 0280).
//! [`recover`] finishes, before every `hello`, what an agent that stopped
//! between those writes left (ADR 0285).

use std::path::Path;

use quoro_proto::jcs;
use quoro_proto::wire::LogEntries;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use tracing::warn;

use crate::AgentError;
use crate::identity;
use crate::store::{FreezeChecked, LogQuarantine, NodeStore, Refollowing};
use crate::util;

/// The highest `seq` the change log carries, 2^53 - 1 (section 7.1
/// "Refollowing"): RFC 8785 refuses larger integers (`quoro_proto::jcs`),
/// and the control plane's fence token for an unknown lease uses the same
/// bound. A quarantine writes the mark as this value before it moves the
/// copy aside (ADR 0261), so no copy is at or above the mark until the node
/// learns the `head_seq` it refollows to.
pub const MAX_SEQ: u64 = 9_007_199_254_740_991;

/// Reads the change-log mark from `data_dir` (section 7.1 "The node's mark",
/// section 12, ADR 0261): `None` when there is no mark file, which is the
/// case before the node first acknowledged an entry, and on an edge-only or
/// witness-only machine, which never follows.
///
/// `quoro cluster promote` reads it to refuse a log below the mark (section
/// 7.3), as [`NodeStore::log_below_mark`] does; the agent reads it before
/// each write of the mark.
///
/// # Errors
///
/// [`AgentError::Io`] when the file exists but cannot be read, and
/// [`AgentError::Store`] when it does not hold a decimal `seq`. The session
/// then ends before `hello`, since it can neither report nor guard its log.
pub fn read_mark(data_dir: &Path) -> Result<Option<u64>, AgentError> {
    let path = data_dir.join(identity::LOG_MARK_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(AgentError::io("read the change-log mark", e)),
    };
    text.trim().parse::<u64>().map(Some).map_err(|_| {
        AgentError::Store(format!(
            "{} holds {text:?}, which is not a change-log seq",
            path.display()
        ))
    })
}

/// Whether a copy of the change log whose newest `seq` is `log_seq` is below
/// the change-log mark `mark` (section 7.1 "The node's mark", ADR 0261): no
/// log is below a missing mark. [`NodeStore::log_below_mark`] and
/// [`crate::freeze::promotion_reads`] apply it to the values they read, so
/// the rule lives in one place and each read happens once.
#[must_use]
pub fn below_mark(log_seq: u64, mark: Option<u64>) -> bool {
    log_seq < mark.unwrap_or(0)
}

/// Writes the change-log mark as `seq`, `0600`, through
/// [`identity::write_private`], which syncs the file and the directory, so
/// the mark is durable before the `log_ack` or `hello` it covers goes out
/// (section 7.1). The write runs off the async runtime. Only a quarantine
/// writes it directly, as [`MAX_SEQ`], under the write lock of
/// [`NodeStore::mark_quarantine`]; every other write goes through
/// [`raise_mark`] or [`lower_mark`].
///
/// # Errors
///
/// [`AgentError::Io`] when the file cannot be written and synced.
pub(crate) async fn write_mark(data_dir: &Path, seq: u64) -> Result<(), AgentError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        identity::write_private(&dir, identity::LOG_MARK_FILE, seq.to_string().as_bytes())
    })
    .await
    .map_err(|e| AgentError::Store(format!("the change-log mark writer stopped: {e}")))?
}

/// Raises the change-log mark to `seq` when `seq` is above it, and never
/// lowers it (section 7.1 "The node's mark", ADR 0261). A missing mark
/// counts as 0, so `seq` 0 writes nothing. Called before every `log_ack` and
/// `hello`; while the node refollows below its head, the mark is that head
/// and nothing is written.
///
/// # Errors
///
/// The errors of [`read_mark`] and of the write.
pub(crate) async fn raise_mark(data_dir: &Path, seq: u64) -> Result<(), AgentError> {
    if seq > read_mark(data_dir)?.unwrap_or(0) {
        write_mark(data_dir, seq).await?;
    }
    Ok(())
}

/// Lowers the change-log mark to `seq` when it is above it, and never raises
/// it: the node learned the `head_seq` it refollows to (section 7.1
/// "Refollowing", ADR 0280). A missing mark is written as `seq` too: since
/// [`raise_mark`] counts a missing mark as 0, the ack of a batch below the
/// head would otherwise write a mark below it. Returns whether it wrote.
///
/// # Errors
///
/// The errors of [`read_mark`] and of the write.
pub(crate) async fn lower_mark(data_dir: &Path, seq: u64) -> Result<bool, AgentError> {
    if read_mark(data_dir)?.is_none_or(|mark| mark > seq) {
        write_mark(data_dir, seq).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Finishes what an agent that stopped during a quarantine or a refollowing
/// left (section 7.1 "Refollowing", ADR 0261, ADR 0280, ADR 0285). The
/// session runs it before every `hello`. It is idempotent: once it has run,
/// a second run writes nothing.
///
/// - **The mark is [`MAX_SEQ`], whatever state is recorded:** a quarantine
///   wrote it and the agent may have stopped before it recorded
///   [`Refollowing::HeadUnknown`] (ADR 0285). With no state recorded, that
///   is the only reading. With [`Refollowing::Head`] `h` recorded there are
///   two: a second `log_diverged` came while the node refollowed to `h`, so
///   the stored copy is diverged and must be moved aside; or the agent
///   recorded `h` and stopped before lowering the mark. They read the same,
///   so it takes the first: it records [`Refollowing::HeadUnknown`] and goes
///   on as below. The second loses nothing by it, since nothing was stored
///   since the quarantine and the node relearns the head from the next
///   `log_entries`, whose `head_seq` from the same primary is never below
///   `h`.
/// - **The state is [`Refollowing::HeadUnknown`]:** the stored copy is moved
///   aside ([`NodeStore::quarantine_log`]; an empty copy needs nothing). The
///   session stores no entry in that state, so every entry the copy holds
///   predates the quarantine, which stopped before it moved the copy.
/// - **The state is [`Refollowing::Head`] `h` and the mark is above `h` or
///   missing:** the mark is lowered to `h` ([`lower_mark`]).
///
/// While a promotion froze the agent (section 7.3 "The freeze"), the
/// refollowing state is not written: [`NodeStore::set_refollowing`] reads
/// the freeze in its own transaction, and when it refuses, this stops there
/// and returns [`FreezeChecked::Frozen`], so the session sends no `hello`.
/// A copy is not moved aside either: [`NodeStore::quarantine_log`] returns
/// [`LogQuarantine::Frozen`], and this stops there and returns
/// [`FreezeChecked::Frozen`] too. Otherwise it returns
/// [`FreezeChecked::Written`], also when there was nothing to write.
///
/// # Errors
///
/// The errors of the store, of [`read_mark`] and of the writes. The session
/// then sends no `hello`: it would report a copy that must first be moved
/// aside, or a mark it could not keep.
pub(crate) async fn recover(store: &NodeStore) -> Result<FreezeChecked, AgentError> {
    let data_dir = store.data_dir();
    let mut state = store.refollowing().await?;
    if read_mark(data_dir)? == Some(MAX_SEQ) && state != Some(Refollowing::HeadUnknown) {
        warn!(
            state = ?state,
            "the change-log mark is at its quarantine value with the head not unknown; finishing the quarantine"
        );
        if store.set_refollowing(Refollowing::HeadUnknown).await? == FreezeChecked::Frozen {
            return Ok(FreezeChecked::Frozen);
        }
        state = Some(Refollowing::HeadUnknown);
    }
    match state {
        Some(Refollowing::HeadUnknown) => {
            let reason = "refollowing: a quarantine that had not moved the copy aside";
            match store.quarantine_log(reason).await? {
                LogQuarantine::Moved(path) => warn!(
                    path = %path.display(),
                    "a change-log copy stored before an unfinished quarantine is quarantined"
                ),
                LogQuarantine::Empty => {}
                LogQuarantine::Frozen => return Ok(FreezeChecked::Frozen),
            }
        }
        Some(Refollowing::Head(head)) => {
            // A freeze that committed meanwhile does not hold this back: it
            // is the lowered mark of a refollowing agent, one of the two mark
            // writes `crate::freeze` allows after a freeze. The mark here is
            // never `MAX_SEQ`, which the arm above turned into "head unknown".
            let lowered = lower_mark(data_dir, head).await?;
            if lowered {
                warn!(
                    head_seq = head,
                    "the change-log mark was above the head the node refollows to, or missing; lowered to it"
                );
            }
        }
        None => {}
    }
    Ok(FreezeChecked::Written)
}

/// `prev_hash` of `seq` 1, and `log_hash` while `log_seq` is 0 (sections 5.1
/// and 5.14).
pub const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The fields of a change-log entry, exactly as section 5.14 lists them (the
/// columns of `change_log` in `db-schema.sql`).
pub const ENTRY_FIELDS: [&str; 11] = [
    "seq",
    "term",
    "time",
    "actor_type",
    "actor_id",
    "entity_type",
    "entity_id",
    "op",
    "payload",
    "prev_hash",
    "hash",
];

/// One verified change-log entry, as the node stores it (section 12
/// `change_log`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// Gap-free from 1.
    pub seq: u64,
    /// The primary's term when it wrote the entry.
    pub term: u64,
    /// When the primary wrote it, RFC 3339.
    pub time: String,
    /// `user`, `agent`, `node` or `system`.
    pub actor_type: String,
    /// The actor's id, or the subsystem for `system`.
    pub actor_id: String,
    /// The changed entity's type.
    pub entity_type: String,
    /// The changed entity's id.
    pub entity_id: String,
    /// `create`, `update` or `delete`.
    pub op: String,
    /// The RFC 8785 canonical text of the payload object, the form the
    /// primary stores it in.
    pub payload: String,
    /// The `hash` of the entry before it; [`ZERO_HASH`] for `seq` 1.
    pub prev_hash: String,
    /// Hex SHA-256 over the RFC 8785 serialization of the entry without
    /// `hash`.
    pub hash: String,
}

impl LogEntry {
    /// The entry as the JSON object of section 5.14, with its 11 fields.
    ///
    /// # Errors
    ///
    /// A stored payload that is not JSON, which only a tampered store holds.
    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        let payload: Value = serde_json::from_str(&self.payload)?;
        Ok(serde_json::json!({
            "seq": self.seq,
            "term": self.term,
            "time": self.time,
            "actor_type": self.actor_type,
            "actor_id": self.actor_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "op": self.op,
            "payload": payload,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }))
    }
}

/// Why a `log_entries` message does not verify (section 7.1). The node
/// stores nothing from it and answers `bad_message`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// `entries` is empty.
    Empty,
    /// `from_seq` is not the stored position plus one: a resend of stored
    /// entries, or a gap after them.
    FromSeq { from_seq: u64, expected: u64 },
    /// Entry `index` is not a JSON object.
    NotAnObject { index: usize },
    /// Entry `index` lacks a field of section 5.14.
    MissingField { index: usize, field: &'static str },
    /// Entry `index` has a field section 5.14 does not list.
    ExtraField { index: usize, field: String },
    /// A field of entry `index` has the wrong JSON type or form.
    BadField { index: usize, field: &'static str },
    /// Entry `index` is not numbered `from_seq + index`.
    Seq {
        index: usize,
        seq: u64,
        expected: u64,
    },
    /// The entry at `seq` does not link to the hash before it.
    PrevHash { seq: u64 },
    /// The entry at `seq` holds a value RFC 8785 cannot serialize.
    NotCanonical { seq: u64 },
    /// The entry at `seq` does not hash to its `hash`.
    Hash { seq: u64 },
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "log_entries has no entries"),
            Self::FromSeq { from_seq, expected } => write!(
                f,
                "log_entries starts at seq {from_seq}, but the node holds the log up to seq {}",
                expected.saturating_sub(1)
            ),
            Self::NotAnObject { index } => write!(f, "entry {index} is not an object"),
            Self::MissingField { index, field } => {
                write!(f, "entry {index} has no field {field:?}")
            }
            Self::ExtraField { index, field } => {
                write!(f, "entry {index} has the unknown field {field:?}")
            }
            Self::BadField { index, field } => {
                write!(f, "entry {index} has a malformed field {field:?}")
            }
            Self::Seq {
                index,
                seq,
                expected,
            } => write!(f, "entry {index} has seq {seq}, expected {expected}"),
            Self::PrevHash { seq } => {
                write!(
                    f,
                    "the prev_hash of seq {seq} does not match the hash before it"
                )
            }
            Self::NotCanonical { seq } => {
                write!(f, "seq {seq} has a value with no RFC 8785 serialization")
            }
            Self::Hash { seq } => write!(f, "seq {seq} does not hash to its hash"),
        }
    }
}

impl std::error::Error for Rejection {}

/// Verifies one `log_entries` message against the stored position
/// `(pos_seq, pos_hash)` (section 7.1), and returns its entries in the form
/// the node stores them.
///
/// The message is valid only when `entries` is non-empty and `from_seq` is
/// `pos_seq + 1`, and each entry, in order, is an object with exactly the 11
/// fields of section 5.14 of the right JSON types, is numbered
/// `from_seq + index`, carries a `prev_hash` equal to the hash before it
/// (`pos_hash` for the first), and hashes to its `hash`. Each entry is
/// checked in that order, and the first failure is returned.
///
/// # Errors
///
/// The first [`Rejection`] found.
pub fn verify_batch(
    pos_seq: u64,
    pos_hash: &str,
    message: &LogEntries,
) -> Result<Vec<LogEntry>, Rejection> {
    if message.entries.is_empty() {
        return Err(Rejection::Empty);
    }
    let expected = pos_seq.saturating_add(1);
    if message.from_seq != expected {
        return Err(Rejection::FromSeq {
            from_seq: message.from_seq,
            expected,
        });
    }
    let mut verified: Vec<LogEntry> = Vec::with_capacity(message.entries.len());
    let mut prev_hash = pos_hash.to_owned();
    for (index, value) in message.entries.iter().enumerate() {
        let Value::Object(object) = value else {
            return Err(Rejection::NotAnObject { index });
        };
        let entry = shape(index, object)?;
        let expected = message
            .from_seq
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
        if entry.seq != expected {
            return Err(Rejection::Seq {
                index,
                seq: entry.seq,
                expected,
            });
        }
        if entry.prev_hash != prev_hash {
            return Err(Rejection::PrevHash { seq: entry.seq });
        }
        let mut unhashed = object.clone();
        unhashed.remove("hash");
        let canonical = jcs::canonicalize(&Value::Object(unhashed))
            .map_err(|_| Rejection::NotCanonical { seq: entry.seq })?;
        if entry_hash(canonical.as_bytes()) != entry.hash {
            return Err(Rejection::Hash { seq: entry.seq });
        }
        prev_hash.clone_from(&entry.hash);
        verified.push(entry);
    }
    Ok(verified)
}

/// Lowercase hex SHA-256 of `bytes`, the form `hash` takes.
fn entry_hash(bytes: &[u8]) -> String {
    util::hex(&Sha256::digest(bytes))
}

/// Checks that entry `index` has exactly the fields of section 5.14 with the
/// right JSON types, and reads it.
fn shape(index: usize, object: &Map<String, Value>) -> Result<LogEntry, Rejection> {
    for field in ENTRY_FIELDS {
        if !object.contains_key(field) {
            return Err(Rejection::MissingField { index, field });
        }
    }
    if let Some(extra) = object
        .keys()
        .find(|key| !ENTRY_FIELDS.contains(&key.as_str()))
    {
        return Err(Rejection::ExtraField {
            index,
            field: extra.clone(),
        });
    }
    let integer = |field: &'static str| {
        object[field]
            .as_u64()
            .ok_or(Rejection::BadField { index, field })
    };
    let string = |field: &'static str| {
        object[field]
            .as_str()
            .map(str::to_owned)
            .ok_or(Rejection::BadField { index, field })
    };
    let hex = |field: &'static str| {
        string(field).and_then(|text| {
            if is_lower_hex_64(&text) {
                Ok(text)
            } else {
                Err(Rejection::BadField { index, field })
            }
        })
    };
    let seq = integer("seq")?;
    let term = integer("term")?;
    let time = string("time")?;
    let actor_type = string("actor_type")?;
    let actor_id = string("actor_id")?;
    let entity_type = string("entity_type")?;
    let entity_id = string("entity_id")?;
    let op = string("op")?;
    if !matches!(op.as_str(), "create" | "update" | "delete") {
        return Err(Rejection::BadField { index, field: "op" });
    }
    let payload = match &object["payload"] {
        payload @ Value::Object(_) => {
            jcs::canonicalize(payload).map_err(|_| Rejection::NotCanonical { seq })?
        }
        _ => {
            return Err(Rejection::BadField {
                index,
                field: "payload",
            });
        }
    };
    Ok(LogEntry {
        seq,
        term,
        time,
        actor_type,
        actor_id,
        entity_type,
        entity_id,
        op,
        payload,
        prev_hash: hex("prev_hash")?,
        hash: hex("hash")?,
    })
}

/// Whether `text` is 64 lowercase hex digits.
fn is_lower_hex_64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Builds valid chains the way the primary's `changelog::append` does, for
/// the tests of this module and of the store.
#[cfg(test)]
pub(crate) mod testing {
    use super::{ZERO_HASH, entry_hash};
    use quoro_proto::jcs;
    use quoro_proto::wire::LogEntries;
    use serde_json::{Value, json};

    /// `count` entries from `from_seq`, the first linking to `prev_hash`.
    /// `salt` goes into every payload, so two chains from the same position
    /// differ.
    pub(crate) fn chain(from_seq: u64, prev_hash: &str, count: u64, salt: &str) -> Vec<Value> {
        let mut prev = prev_hash.to_owned();
        (from_seq..from_seq + count)
            .map(|seq| {
                let mut entry = json!({
                    "seq": seq,
                    "term": 1,
                    "time": "2026-11-02T10:15:31Z",
                    "actor_type": "agent",
                    "actor_id": "agt_1",
                    "entity_type": "app",
                    "entity_id": format!("app_{seq}"),
                    "op": if seq == 1 { "create" } else { "update" },
                    "payload": { "name": format!("blog-{seq}"), "salt": salt, "port": 8080, "ratio": 9.8 },
                    "prev_hash": prev,
                });
                let hash = entry_hash(jcs::canonicalize(&entry).unwrap().as_bytes());
                entry["hash"] = json!(hash);
                prev = hash;
                entry
            })
            .collect()
    }

    /// `chain(1, ZERO_HASH, count, salt)`.
    pub(crate) fn genesis(count: u64, salt: &str) -> Vec<Value> {
        chain(1, ZERO_HASH, count, salt)
    }

    /// A `log_entries` body over `entries`, with `from_seq` the first
    /// entry's `seq` (1 when empty).
    pub(crate) fn message(entries: Vec<Value>) -> LogEntries {
        let from_seq = entries
            .first()
            .and_then(|entry| entry["seq"].as_u64())
            .unwrap_or(1);
        LogEntries {
            from_seq,
            entries,
            head_seq: None,
        }
    }

    /// Recomputes `entry`'s `hash` over its current fields, so a test can
    /// change one field and leave every other check passing.
    pub(crate) fn rehash(entry: &mut Value) {
        let mut unhashed = entry.clone();
        unhashed.as_object_mut().unwrap().remove("hash");
        entry["hash"] = json!(entry_hash(jcs::canonicalize(&unhashed).unwrap().as_bytes()));
    }

    /// A change-log entry without `hash`, in RFC 8785 form, written out: two
    /// floats whose text an approximate parser reads back as other numbers
    /// (ADR 0315). The primary's tests hold the same text (`FLOAT_ENTRY` in
    /// `quoro-control-plane`'s `changelog::tests`), so a node and the primary
    /// re-hash one entry. Its hash is the SHA-256 of this text.
    pub(crate) const FLOAT_ENTRY: &str = r#"{"actor_id":"test","actor_type":"system","entity_id":"vm_1","entity_type":"vm","op":"update","payload":{"x":1548.2162860866163,"y":18983552425072.563},"prev_hash":"0000000000000000000000000000000000000000000000000000000000000000","seq":1,"term":1,"time":"2026-11-10T10:15:31.000Z"}"#;

    /// An entry a development build since #723 wrote before floats parsed
    /// exactly (ADR 0315), without `hash`, with the payload text it stored:
    /// the RFC 8785 text of the double a request's `1548.2162860866162` read
    /// as.
    pub(crate) const STALE_STORED: &str = r#"{"actor_id":"test","actor_type":"system","entity_id":"vm_1","entity_type":"vm","op":"update","payload":{"x":1548.2162860866163},"prev_hash":"0000000000000000000000000000000000000000000000000000000000000000","seq":1,"term":1,"time":"2026-11-10T10:15:31.000Z"}"#;

    /// [`STALE_STORED`] as that build hashed it: with the payload as its
    /// parser read the stored text back, the neighbouring double.
    pub(crate) const STALE_HASHED: &str = r#"{"actor_id":"test","actor_type":"system","entity_id":"vm_1","entity_type":"vm","op":"update","payload":{"x":1548.2162860866165},"prev_hash":"0000000000000000000000000000000000000000000000000000000000000000","seq":1,"term":1,"time":"2026-11-10T10:15:31.000Z"}"#;

    /// Lowercase hex SHA-256 of `text`.
    pub(crate) fn sha256(text: &str) -> String {
        use sha2::{Digest as _, Sha256};
        crate::util::hex(&Sha256::digest(text.as_bytes()))
    }

    /// The entry `unhashed`, read from its stored text as the primary reads
    /// it, with `hash`, sent alone in a `log_entries` frame as the primary
    /// writes it, and parsed back from that frame's text as the node parses
    /// it.
    pub(crate) fn received(unhashed: &str, hash: &str) -> LogEntries {
        use quoro_proto::wire::{ControlMessage, RawFrame};
        let mut entry: Value = serde_json::from_str(unhashed).unwrap();
        entry["hash"] = json!(hash);
        let text = ControlMessage::LogEntries(LogEntries {
            from_seq: entry["seq"].as_u64().unwrap(),
            entries: vec![entry],
            head_seq: Some(1),
        })
        .to_frame("msg_01JC9ZN1Q6C8D0F2H4K6N8Q0S2", None)
        .to_text();
        match ControlMessage::from_raw(&RawFrame::parse(&text).unwrap()).unwrap() {
            ControlMessage::LogEntries(body) => body,
            other => panic!("not log_entries: {}", other.kind()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{chain, genesis, message, rehash};
    use super::*;
    use serde_json::json;

    fn hash_of(entry: &Value) -> String {
        entry["hash"].as_str().unwrap().to_owned()
    }

    /// The hash construction matches one computed outside this crate:
    /// SHA-256 over the RFC 8785 text of this entry without `hash`, as
    /// Python's `json.dumps(sort_keys=True, separators=(",", ":"))` writes it
    /// for these ASCII fields.
    #[test]
    fn a_fixed_entry_verifies_against_an_independently_computed_hash() {
        let entry = json!({
            "seq": 1, "term": 1, "time": "2026-11-02T10:15:31Z",
            "actor_type": "system", "actor_id": "test",
            "entity_type": "app", "entity_id": "app_1", "op": "create",
            "payload": {"name": "blog", "id": "app_1"},
            "prev_hash": ZERO_HASH,
            "hash": "0c2187b1728ea24cde3d7dad5bd6dd7968cb709096f1a8d3535196c9c1b00a3d",
        });
        let verified = verify_batch(0, ZERO_HASH, &message(vec![entry])).unwrap();
        assert_eq!(verified[0].payload, r#"{"id":"app_1","name":"blog"}"#);
        assert_eq!(verified[0].op, "create");
    }

    #[test]
    fn a_first_batch_from_seq_1_is_accepted() {
        let entries = genesis(3, "a");
        let verified = verify_batch(0, ZERO_HASH, &message(entries.clone())).unwrap();
        assert_eq!(verified.len(), 3);
        for (stored, sent) in verified.iter().zip(&entries) {
            assert_eq!(&stored.to_value().unwrap(), sent);
        }
        assert_eq!(verified[0].prev_hash, ZERO_HASH);
        assert_eq!(verified[2].seq, 3);
        assert_eq!(verified[2].hash, hash_of(&entries[2]));
    }

    #[test]
    fn a_second_batch_chained_on_the_first_is_accepted() {
        let first = genesis(3, "a");
        let head = hash_of(&first[2]);
        let second = chain(4, &head, 2, "a");
        let verified = verify_batch(3, &head, &message(second.clone())).unwrap();
        assert_eq!(verified.iter().map(|e| e.seq).collect::<Vec<_>>(), [4, 5]);
        assert_eq!(verified[0].prev_hash, head);
        assert_eq!(verified[1].hash, hash_of(&second[1]));
    }

    #[test]
    fn a_wrong_from_seq_is_rejected() {
        // `from_seq` and the entries skip seq 1.
        let mut body = message(chain(2, ZERO_HASH, 2, "a"));
        assert_eq!(
            verify_batch(0, ZERO_HASH, &body),
            Err(Rejection::FromSeq {
                from_seq: 2,
                expected: 1
            })
        );
        // `from_seq` is right but the first entry is not numbered by it.
        body.from_seq = 1;
        assert_eq!(
            verify_batch(0, ZERO_HASH, &body),
            Err(Rejection::Seq {
                index: 0,
                seq: 2,
                expected: 1
            })
        );
    }

    #[test]
    fn a_resend_from_a_stored_seq_is_rejected() {
        let first = genesis(3, "a");
        let head = hash_of(&first[2]);
        // The node holds 1..=3; the primary sends 3..=4 again.
        let resend = vec![first[2].clone(), chain(4, &head, 1, "a").remove(0)];
        assert_eq!(
            verify_batch(3, &head, &message(resend)),
            Err(Rejection::FromSeq {
                from_seq: 3,
                expected: 4
            })
        );
        // So is the whole first batch again.
        assert_eq!(
            verify_batch(3, &head, &message(first)),
            Err(Rejection::FromSeq {
                from_seq: 1,
                expected: 4
            })
        );
    }

    #[test]
    fn a_gap_inside_the_batch_is_rejected() {
        let mut entries = genesis(4, "a");
        entries.remove(2);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::Seq {
                index: 2,
                seq: 4,
                expected: 3
            })
        );
    }

    #[test]
    fn a_broken_hash_is_rejected() {
        let mut entries = genesis(3, "a");
        entries[1]["payload"]["name"] = json!("tampered");
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::Hash { seq: 2 })
        );

        // A well-formed hash of some other entry fails the same way.
        let mut entries = genesis(1, "a");
        entries[0]["hash"] = json!(hash_of(&genesis(1, "b")[0]));
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::Hash { seq: 1 })
        );
    }

    #[test]
    fn a_broken_prev_hash_of_the_first_entry_is_rejected() {
        // Against an empty copy: the first entry must link to 64 zeros. Its
        // hash is recomputed, so only the link is wrong.
        let mut entries = genesis(2, "a");
        entries[0]["prev_hash"] = json!("1".repeat(64));
        rehash(&mut entries[0]);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::PrevHash { seq: 1 })
        );

        // Against a stored copy: a second batch that chains on another head.
        let first = genesis(3, "a");
        let other = genesis(3, "b");
        let second = chain(4, &hash_of(&other[2]), 2, "a");
        assert_eq!(
            verify_batch(3, &hash_of(&first[2]), &message(second)),
            Err(Rejection::PrevHash { seq: 4 })
        );
    }

    #[test]
    fn a_broken_prev_hash_of_a_middle_entry_is_rejected() {
        let mut entries = genesis(4, "a");
        entries[2]["prev_hash"] = json!(hash_of(&entries[0]));
        rehash(&mut entries[2]);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::PrevHash { seq: 3 })
        );
    }

    #[test]
    fn an_extra_field_is_rejected() {
        let mut entries = genesis(2, "a");
        entries[1]["note"] = json!("x");
        rehash(&mut entries[1]);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::ExtraField {
                index: 1,
                field: "note".to_owned()
            })
        );
    }

    #[test]
    fn a_missing_field_is_rejected() {
        let mut entries = genesis(2, "a");
        entries[0].as_object_mut().unwrap().remove("term");
        rehash(&mut entries[0]);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::MissingField {
                index: 0,
                field: "term"
            })
        );
    }

    #[test]
    fn a_field_of_the_wrong_type_is_rejected() {
        for (field, value) in [
            ("seq", json!("1")),
            ("term", json!(1.5)),
            ("time", json!(1)),
            ("op", json!("upsert")),
            ("payload", json!([])),
            ("prev_hash", json!("00")),
        ] {
            let mut entries = genesis(1, "a");
            entries[0][field] = value;
            rehash(&mut entries[0]);
            assert_eq!(
                verify_batch(0, ZERO_HASH, &message(entries)),
                Err(Rejection::BadField { index: 0, field }),
                "{field}"
            );
        }
    }

    #[test]
    fn an_uppercase_hex_hash_is_rejected() {
        let mut entries = genesis(2, "a");
        let upper = hash_of(&entries[1]).to_uppercase();
        assert!(upper.bytes().any(|byte| byte.is_ascii_uppercase()));
        entries[1]["hash"] = json!(upper);
        assert_eq!(
            verify_batch(0, ZERO_HASH, &message(entries)),
            Err(Rejection::BadField {
                index: 1,
                field: "hash"
            })
        );
    }

    /// ADR 0315: the primary sends an entry a development build since #723
    /// hashed over a float's best-effort read-back as it stored it. Read
    /// exactly, it does not hash to its hash, so a node that does not hold
    /// it refuses the message (`bad_message`) and stores nothing. The same
    /// message, read with the best-effort parser, verified.
    #[test]
    fn an_entry_hashed_over_a_best_effort_read_back_is_refused() {
        use super::testing::{STALE_HASHED, STALE_STORED, received, sha256};
        assert_ne!(
            "1548.2162860866163".parse::<f64>().unwrap(),
            "1548.2162860866165".parse::<f64>().unwrap()
        );
        let body = received(STALE_STORED, &sha256(STALE_HASHED));
        assert_eq!(
            verify_batch(0, ZERO_HASH, &body),
            Err(Rejection::Hash { seq: 1 })
        );
    }

    #[test]
    fn empty_entries_are_rejected() {
        let body = LogEntries {
            from_seq: 1,
            entries: Vec::new(),
            head_seq: Some(0),
        };
        assert_eq!(verify_batch(0, ZERO_HASH, &body), Err(Rejection::Empty));
    }
}
