// SPDX-License-Identifier: AGPL-3.0-only
//! The restored primary's first start (`backup-format.md` section 8.2, "The
//! first write" and "Detecting the first start"; ADR 0280, ADR 0285).
//!
//! `quoro restore` writes `<data_dir>/restore-pending` ([`MARKER_FILE`])
//! before it installs the database. The marker ([`Marker`]) names that
//! database by its change log's head: the head entry's `seq` and hash. It
//! also says whether `--accept-loss` was given, with the statement.
//!
//! At each start, the primary reads the marker in two steps:
//!
//! 1. Before anything else of the start, and so before the pre-upgrade
//!    backup and the migrations (section 7.2), [`check`] reads the marker,
//!    `meta.restore_marker`, the change log's head and, when the first write
//!    is due, `primary-node`, all read-only, and decides what the marker
//!    asks for ([`Pending`]). Every refusal of the start comes from this
//!    step, so a refused start has written nothing, migrated nothing and
//!    sealed no pre-upgrade backup, and the next start takes that backup and
//!    records it.
//! 2. Once the migrations ran, and before any other change-log write,
//!    [`apply`] carries the decision out. Only a database error stops the
//!    start there. No migration and nothing the start runs before [`apply`]
//!    writes the change log, so the head [`check`] read still holds.
//!
//! What the marker asks for:
//!
//! - **It matches `meta.restore_marker`:** the first write already
//!   committed. The marker is deleted and nothing is written again.
//! - **It matches the database's log head:** this is the first start after
//!   the restore, and the first write is made from it. In one transaction it
//!   marks every `expiring` lease row unsettled (node-agent-protocol.md
//!   section 8.4, "The mark", [`crate::leases::mark_expiring_rows`]), moves
//!   the primary status when `primary-node` asks for it
//!   ([`crate::primary_status::move_in`], `cluster.primary_moved`), and
//!   records the marker's head in `meta.restore_marker`. `meta` is not in the
//!   log, so that record is local. Once the transaction commits,
//!   `primary-node` and the marker are deleted.
//! - **Otherwise:** the marker belongs to another database. It is logged
//!   and deleted, and nothing is written from it. A marker that cannot be
//!   read or parsed names no database this primary can match, so it takes
//!   this path too, and the log says why.
//!
//! Without a marker, and after a marker that names another database,
//! `primary-node` is applied as before ([`crate::primary_status::apply_marker`]).
//!
//! `--accept-loss` is not built yet: its part of the first write (the
//! removals, the token raises, the seal deletions and `cluster.restored`)
//! comes later in issue #838. A marker that asks for it and names this
//! database stops the start in [`check`], before the migrations and with
//! nothing written, and stays, so the restore is not taken as done without
//! that part.

use std::path::Path;

use serde_json::{Map, Value, json};
use sqlx::SqliteConnection;

use crate::changelog::Actor;
use crate::primary_status::{self, File, Moved};
use crate::store::{GENESIS_HASH, Store};

/// The marker `quoro restore` writes into the data directory, next to the
/// database and to `primary-node`.
pub const MARKER_FILE: &str = "restore-pending";

/// The `meta` key that records the head of the marker whose first write
/// committed (migration 0013). It is local and never in the change log.
pub const META_KEY: &str = "restore_marker";

/// The system actor of the first write's entries, as of the primary status
/// move it may carry (`audit-log-format.md` section 3.5).
const ACTOR: &str = "cluster";

/// The largest `seq` a marker may name: entries are numbered within the
/// IEEE-754 safe range, as every integer of the change log is.
const MAX_SEQ: i64 = (1 << 53) - 1;

/// The newest entry of a change log: its `seq` and `hash`. An empty log has
/// head `seq` 0 and the genesis hash, the `prev_hash` of its first entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub seq: i64,
    pub hash: String,
}

impl Head {
    /// The JSON members that name this head, in the marker and in
    /// `meta.restore_marker`.
    fn members(&self) -> Map<String, Value> {
        let mut members = Map::new();
        members.insert("head_seq".to_string(), json!(self.seq));
        members.insert("head_hash".to_string(), json!(self.hash));
        members
    }

    /// The text of `meta.restore_marker`: `{"head_hash":…,"head_seq":…}` in
    /// RFC 8785 form.
    fn meta_value(&self) -> String {
        crate::jcs::canonicalize_lenient(&Value::Object(self.members()))
    }
}

/// The head of the change log on `conn`.
///
/// # Errors
///
/// A database error, such as a database without a `change_log` table.
pub async fn log_head(conn: &mut SqliteConnection) -> Result<Head, sqlx::Error> {
    let last: Option<(i64, String)> =
        sqlx::query_as("SELECT seq, hash FROM change_log ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut *conn)
            .await?;
    Ok(match last {
        Some((seq, hash)) => Head { seq, hash },
        None => Head {
            seq: 0,
            hash: GENESIS_HASH.to_string(),
        },
    })
}

/// The `restore-pending` marker.
///
/// The contract fixes what it holds, not how. Its file is one JSON object
/// with exactly these four members, written in RFC 8785 form and followed by
/// a newline:
///
/// ```json
/// {"accept_loss":false,"head_hash":"<64 lowercase hex>","head_seq":42,"statement":null}
/// ```
///
/// `head_seq` is an integer from 0 to 2^53 − 1, `head_hash` 64 lowercase hex
/// digits, and `statement` a non-blank string when `accept_loss` is true and
/// `null` when it is false. [`Marker::parse`] refuses anything else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    /// The restored log's head.
    pub head: Head,
    /// Whether `quoro restore` was given `--accept-loss`.
    pub accept_loss: bool,
    /// The operator's statement, given with `--accept-loss`.
    pub statement: Option<String>,
}

impl Marker {
    /// The marker of a restore without `--accept-loss`.
    #[must_use]
    pub fn plain(head: Head) -> Self {
        Self {
            head,
            accept_loss: false,
            statement: None,
        }
    }

    /// The file's bytes: the object in RFC 8785 form and a newline.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut members = self.head.members();
        members.insert("accept_loss".to_string(), json!(self.accept_loss));
        members.insert(
            "statement".to_string(),
            self.statement.as_deref().map_or(Value::Null, Value::from),
        );
        let mut text = crate::jcs::canonicalize_lenient(&Value::Object(members));
        text.push('\n');
        text.into_bytes()
    }

    /// Reads a marker file's bytes strictly (see [`Marker`]).
    ///
    /// # Errors
    ///
    /// A one-line reason when the bytes are not that object.
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|error| format!("it is not JSON: {error}"))?;
        let Value::Object(members) = value else {
            return Err("it is not a JSON object".to_string());
        };
        const MEMBERS: [&str; 4] = ["accept_loss", "head_hash", "head_seq", "statement"];
        if members.len() != MEMBERS.len() || !MEMBERS.iter().all(|key| members.contains_key(*key)) {
            return Err(
                "it must hold exactly accept_loss, head_hash, head_seq and statement".to_string(),
            );
        }
        let seq = members["head_seq"]
            .as_i64()
            .filter(|seq| (0..=MAX_SEQ).contains(seq))
            .ok_or("head_seq is not an integer from 0 to 2^53 - 1")?;
        let hash = members["head_hash"]
            .as_str()
            .filter(|hash| is_hash(hash))
            .ok_or("head_hash is not 64 lowercase hex digits")?;
        let accept_loss = members["accept_loss"]
            .as_bool()
            .ok_or("accept_loss is not true or false")?;
        let statement = match (&members["statement"], accept_loss) {
            (Value::Null, false) => None,
            (Value::String(statement), true) if !statement.trim().is_empty() => {
                Some(statement.clone())
            }
            (_, false) => return Err("statement must be null without accept_loss".to_string()),
            (_, true) => {
                return Err("accept_loss needs a statement that is not blank".to_string());
            }
        };
        Ok(Self {
            head: Head {
                seq,
                hash: hash.to_string(),
            },
            accept_loss,
            statement,
        })
    }
}

fn is_hash(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// What [`check`] decided from the marker before the migrations, for
/// [`apply`] to carry out after them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub enum Pending {
    /// No marker.
    NoMarker,
    /// The marker matched `meta.restore_marker`: its first write committed.
    AlreadyWritten {
        /// The head the marker names.
        head: Head,
    },
    /// The marker names this database's log head: the first write is due.
    FirstWrite {
        /// The marker.
        marker: Marker,
        /// The node `primary-node` names, trimmed; `None` without the file.
        node_id: Option<String>,
    },
    /// The marker names another log head, or cannot be read or parsed.
    OtherDatabase {
        /// Why, for the log.
        why: String,
    },
}

/// What [`apply`] did with the marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    /// No marker: `primary-node`, if any, was applied as before.
    NoMarker,
    /// The marker matched `meta.restore_marker`: deleted, nothing written.
    AlreadyWritten,
    /// The first write committed; it marked `marked` lease rows and moved
    /// the primary status when `moved`.
    FirstWrite { marked: usize, moved: bool },
    /// The marker named another log head, or could not be read or parsed:
    /// deleted (a failure to delete it is logged), nothing written from it.
    OtherDatabase,
}

/// The first step of the restored primary's first start (see the module
/// documentation): reads the marker and decides what it asks for. It runs
/// before anything else of the start, before the pre-upgrade backup and the
/// migrations, and writes nothing: no file, no row.
///
/// It reads `meta.restore_marker` and the change log's head from the
/// database as it is, unmigrated. Both tables exist from the first schema,
/// and no migration writes the log; a database without them yet (a new one)
/// has no `restore_marker` and an empty log.
///
/// # Errors
///
/// A one-line reason that stops the start, with nothing written, nothing
/// migrated and the marker and `primary-node` left in place: a marker of this
/// database that asks for `--accept-loss`, which is not built yet; a
/// `primary-node` file that cannot be read when the first write is due; or a
/// database error.
pub async fn check(store: &Store, data_dir: &Path) -> Result<Pending, String> {
    let path = data_dir.join(MARKER_FILE);
    let marker = match std::fs::read(&path) {
        Ok(bytes) => match Marker::parse(&bytes) {
            Ok(marker) => marker,
            Err(reason) => {
                return Ok(Pending::OtherDatabase {
                    why: format!("it is not a marker quoro restore writes: {reason}"),
                });
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Pending::NoMarker);
        }
        Err(error) => {
            return Ok(Pending::OtherDatabase {
                why: format!("it cannot be read: {error}"),
            });
        }
    };

    let (consumed, head) = read_database(store).await.map_err(|error| {
        format!(
            "cannot read the database before the restored primary's first write: {error}; \
             nothing was written"
        )
    })?;
    if consumed.as_deref() == Some(marker.head.meta_value().as_str()) {
        return Ok(Pending::AlreadyWritten { head: marker.head });
    }
    if head != marker.head {
        return Ok(Pending::OtherDatabase {
            why: format!(
                "it names the change log head at seq {} with hash {}, and this database's head \
                 is seq {} with hash {}",
                marker.head.seq, marker.head.hash, head.seq, head.hash
            ),
        });
    }

    if marker.accept_loss {
        return Err(format!(
            "{} asks for a restore with --accept-loss, whose first write (removing the other \
             nodes, raising their lease tokens, deleting their custody seals and recording \
             cluster.restored) this version does not make yet; the start stops before its \
             migrations, and nothing was written",
            path.display()
        ));
    }
    let node_id = match primary_status::read_file(data_dir) {
        File::Missing => None,
        File::Text(text) => Some(text.trim().to_string()),
        File::Unreadable(error) => {
            return Err(format!(
                "cannot read {}, left by quoro restore: {error}; the restored primary's first \
                 write must carry its move, so give it the database file's owner; the start \
                 stops before its migrations, and nothing was written",
                data_dir.join(primary_status::MARKER_FILE).display()
            ));
        }
    };
    Ok(Pending::FirstWrite { marker, node_id })
}

/// `meta.restore_marker` and the change log's head, read in one read
/// transaction from the database as it is, migrated or not.
async fn read_database(store: &Store) -> Result<(Option<String>, Head), sqlx::Error> {
    let mut tx = store.pool().begin().await?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('meta', 'change_log')",
    )
    .fetch_all(&mut *tx)
    .await?;
    let has = |name: &str| tables.iter().any(|table| table == name);
    let consumed: Option<String> = if has("meta") {
        sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
            .bind(META_KEY)
            .fetch_optional(&mut *tx)
            .await?
    } else {
        None
    };
    let head = if has("change_log") {
        log_head(&mut tx).await?
    } else {
        Head {
            seq: 0,
            hash: GENESIS_HASH.to_string(),
        }
    };
    tx.rollback().await?;
    Ok((consumed, head))
}

/// The second step of the restored primary's first start (see the module
/// documentation): carries out what [`check`] decided. It runs once the
/// migrations ran and before any other change-log write of the start.
///
/// # Errors
///
/// A one-line reason for a database error, which stops the start. The first
/// write is then not committed, so none of it is written, and the marker and
/// `primary-node` stay for the next start.
pub async fn apply(store: &Store, data_dir: &Path, pending: Pending) -> Result<Applied, String> {
    let path = data_dir.join(MARKER_FILE);
    let (marker, node_id) = match pending {
        Pending::NoMarker => {
            primary_status::apply_marker(store, data_dir).await?;
            return Ok(Applied::NoMarker);
        }
        Pending::AlreadyWritten { head } => {
            // The first write moved the status if `primary-node` asked for
            // it; a file left by a crash after that commit is not applied
            // again.
            if !matches!(primary_status::read_file(data_dir), File::Missing) {
                primary_status::remove_file(data_dir);
            }
            tracing::info!(
                head_seq = head.seq,
                "the restored primary's first write already committed; deleting the restore-pending marker"
            );
            remove(&path);
            return Ok(Applied::AlreadyWritten);
        }
        Pending::OtherDatabase { why } => {
            tracing::warn!(
                path = %path.display(),
                why = %why,
                "the restore-pending marker belongs to another database; deleting it without a first write"
            );
            remove(&path);
            primary_status::apply_marker(store, data_dir).await?;
            return Ok(Applied::OtherDatabase);
        }
        Pending::FirstWrite { marker, node_id } => (marker, node_id),
    };

    let mut w = store
        .begin_write()
        .await
        .map_err(|error| format!("cannot begin the restored primary's first write: {error}"))?;
    let actor = Actor::system(ACTOR);
    let marked = crate::leases::mark_expiring_rows(&mut w.tx, &actor)
        .await
        .map_err(|error| {
            format!(
                "cannot mark the expiring lease rows: {}",
                error
                    .detail
                    .unwrap_or_else(|| error.code.as_str().to_string())
            )
        })?;
    let moved = match &node_id {
        Some(node_id) => Some(primary_status::move_in(&mut w.tx, node_id).await?),
        None => None,
    };
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES (?, ?) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(META_KEY)
    .bind(marker.head.meta_value())
    .execute(&mut *w.tx)
    .await
    .map_err(|error| format!("cannot record meta.{META_KEY}: {error}"))?;
    w.commit()
        .await
        .map_err(|error| format!("cannot commit the restored primary's first write: {error}"))?;

    if let (Some(node_id), Some(moved)) = (&node_id, &moved) {
        primary_status::log_moved(node_id, moved);
        primary_status::remove_file(data_dir);
    }
    tracing::info!(
        head_seq = marker.head.seq,
        marked = marked.len(),
        "made the restored primary's first write: every expiring lease row is marked unsettled"
    );
    remove(&path);
    Ok(Applied::FirstWrite {
        marked: marked.len(),
        moved: matches!(moved, Some(Moved::Yes { .. })),
    })
}

/// Deletes the marker; a failure is logged. A marker left behind this way
/// matches `meta.restore_marker` or another database at the next start, so
/// it is never applied twice.
fn remove(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        tracing::warn!(
            path = %path.display(),
            error = %error,
            "cannot delete the restore-pending marker left by quoro restore"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{Head, Marker};

    fn head() -> Head {
        Head {
            seq: 42,
            hash: "ab".repeat(32),
        }
    }

    #[test]
    fn the_file_is_one_canonical_object_and_a_newline() {
        let bytes = Marker::plain(head()).to_bytes();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            format!(
                "{{\"accept_loss\":false,\"head_hash\":\"{}\",\"head_seq\":42,\"statement\":null}}\n",
                "ab".repeat(32)
            )
        );
        assert_eq!(Marker::parse(&bytes).unwrap(), Marker::plain(head()));
        let forced = Marker {
            head: head(),
            accept_loss: true,
            statement: Some("no other node survives".to_string()),
        };
        assert_eq!(Marker::parse(&forced.to_bytes()).unwrap(), forced);
    }

    #[test]
    fn anything_else_is_refused() {
        let hash = "ab".repeat(32);
        for text in [
            String::new(),
            "[]".to_string(),
            format!(r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":false}}"#),
            format!(
                r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":false,"statement":null,"x":1}}"#
            ),
            format!(
                r#"{{"head_seq":-1,"head_hash":"{hash}","accept_loss":false,"statement":null}}"#
            ),
            format!(
                r#"{{"head_seq":4.5,"head_hash":"{hash}","accept_loss":false,"statement":null}}"#
            ),
            format!(
                r#"{{"head_seq":9007199254740992,"head_hash":"{hash}","accept_loss":false,"statement":null}}"#
            ),
            format!(
                r#"{{"head_seq":"42","head_hash":"{hash}","accept_loss":false,"statement":null}}"#
            ),
            format!(
                r#"{{"head_seq":42,"head_hash":"{}","accept_loss":false,"statement":null}}"#,
                "AB".repeat(32)
            ),
            r#"{"head_seq":42,"head_hash":"abab","accept_loss":false,"statement":null}"#
                .to_string(),
            format!(r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":0,"statement":null}}"#),
            format!(
                r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":false,"statement":"x"}}"#
            ),
            format!(
                r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":true,"statement":null}}"#
            ),
            format!(r#"{{"head_seq":42,"head_hash":"{hash}","accept_loss":true,"statement":" "}}"#),
        ] {
            assert!(Marker::parse(text.as_bytes()).is_err(), "{text}");
        }
    }
}
