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
//!   sets the restored primary's term first (below, "The term"), then
//!   marks every `expiring` lease row unsettled (node-agent-protocol.md
//!   section 8.4, "The mark", [`crate::leases::mark_expiring_rows`]), makes
//!   the removals of `--accept-loss` when the marker asks for them (below),
//!   moves the primary status when `primary-node` asks for it
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
//! **`--accept-loss` (ADR 0261, ADR 0280).** The operator's statement says
//! that no other machine with the `node` role survives. So the same first
//! write also removes every machine with the `node` role that the restored
//! database holds, not `removed`, other than the new host's node: the node
//! `primary-node` names, the one `quoro restore` picked. With no
//! `primary-node`, the new host has no node of the restored database, and
//! every such machine is removed ([`to_remove`]). For each one, in this order
//! ([`remove_the_lost`]):
//!
//! - the token of each lease row that names it goes up by one and the row is
//!   recorded revoked, as declaring the node lost does (node-agent-protocol.md
//!   section 8.3): the rows `declareNodeLost` raises
//!   ([`crate::api::nodes::raise_lost_leases`], ADR 0202), a revoked one
//!   included, except a row already revoked at the token cap, also when the
//!   machine was declared lost before and holds a lease granted since. The
//!   write raises the tokens and does not declare the machine lost: it sets
//!   no `lost_at`;
//! - it turns `removed`, with its node entry, the whole row, as `removeNode`
//!   writes a removal (section 10), so the voter set and the required set of
//!   section 8.4 read it removed from that `seq` on. The same row write
//!   clears its `cp_status`, so no removed machine keeps the primary status.
//!   With `primary-node`, the write reads which node held the status before
//!   the removals, and the move that follows names it as `from_node_id` in
//!   `cluster.primary_moved`. The move writes no second entry for the old
//!   primary, whose removal entry already reads no `cp_status`;
//! - its custody seals are deleted (section 7.2,
//!   [`crate::custody::delete_seals`]) and the records of the quarantined
//!   copies it keeps are dropped ([`crate::quarantines::drop_node_copies`]),
//!   as a removal does (section 10).
//!
//! When the new host's node does not hold the primary status after the
//! write, because no `primary-node` file is present or it names a node the
//! primary cannot use (one the database does not hold, a `removed` one, or
//! text that is no `nod_` id, [`crate::primary_status::read_named`]), the
//! write then clears `cp_status` on every machine that still holds it,
//! whatever its roles, each row with its node entry ([`clear_cp_status`]), so
//! no machine holds the primary status and the next local enrollment of the
//! new host's agent takes it, and adopts no old row. A machine the write
//! removed reads no `cp_status` in its removal entry already and is not
//! written again. The clear records no `cluster.primary_moved`;
//! `cluster.restored` stands for it. When the new host's node holds the
//! status after the write, because it held it already or the move gives it,
//! nothing more follows.
//!
//! Then the write records `cluster.restored` (`audit-log-format.md` section
//! 3.9), before the move's `cluster.primary_moved`, so it is the first record
//! of the write. That write may remove more than one voter at once and runs
//! no gate: it is exempt from node-agent-protocol.md section 8.10 rules 1 and
//! 2.
//!
//! Until that enrollment no node holds the primary status. Nothing this
//! module or the start after it runs needs a holder: the move names a null
//! `from_node_id` when none held it, the start writes the local join file
//! while no holder has a credential, and the local enrollment then makes the
//! new host's node the holder.
//!
//! **The term (`backup-format.md` section 8.2, "Term"; node-agent-protocol.md
//! section 8.8; ADR 0261, ADR 0280).** The first write starts the restored
//! primary at one above the highest term it knows of: the restored log's
//! terms, the term the restored database holds (`meta.term`, the term of the
//! primary the backup was taken from), and the highest term this host's node
//! agent stored (`node_meta.highest_term` in its store, section 12), from
//! the store in [`NODE_DATA_DIR`] opened read-only, as `quoro restore` reads
//! it. Its first statement sets `meta.term`, so every entry of the write, and
//! `cluster.restored`, carries the new term. [`check`] reads the same terms
//! and stops the start, before the migrations and with nothing written, when
//! the agent's store exists and its term cannot be read, or when that term
//! would pass the term ceiling, 2^53 - 1: a stated limit, as no restore can
//! take a higher term. The third source the contract names, the terms in vote
//! `check` answers the restored primary receives, is not read here: nothing
//! in this start sends a vote `check`.
//!
//! After `--accept-loss`, [`apply`] also lets a higher term start the
//! primary again until it sends its first `lease_grant`
//! ([`Store::restarts_on_a_higher_term`]): a `stale_term`, or a voter's
//! `bad_message` that carries its `term`, then raises `meta.term` to one
//! above that term instead of demoting it (node-agent-protocol.md section
//! 8.8, [`Store::restart_term`]). A restart that would pass the ceiling
//! demotes it. A plain restore does not: it demotes on `stale_term`, and a
//! voter's `bad_message` leaves it without that voter's support.
//!
//! The token cap (node-agent-protocol.md section 8.6, ADR 0325): [`check`]
//! reads every lease that write would raise and stops the start, before the
//! migrations and with nothing written, when one is already at the cap
//! ([`leases_at_cap`]), naming each with its token. `quoro restore
//! --accept-loss` makes the same check on the restored database before it
//! installs anything. The raise itself refuses at the cap too, as a backstop,
//! and then the first write is not committed.

use std::path::Path;

use serde_json::{Map, Value, json};
use sqlx::SqliteConnection;

use crate::audit::{Channel, Outcome, Record};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::primary_status::{self, File, Moved};
use crate::store::{GENESIS_HASH, MAX_TERM, Store, TermError, read_term};

/// The marker `quoro restore` writes into the data directory, next to the
/// database and to `primary-node`.
pub const MARKER_FILE: &str = "restore-pending";

/// The data directory of this host's node agent, where the restored
/// primary's first start reads the term the agent stored: the directory of
/// the identity file `backup-format.md` section 8.2 names, and the one
/// `quoro restore` reads the agent's voters from.
pub const NODE_DATA_DIR: &str = "/var/lib/quoro-node";

/// The node agent's store inside [`NODE_DATA_DIR`] (node-agent-protocol.md
/// section 12).
const NODE_STORE_FILE: &str = "node.db";

/// The `node_meta` key of the node agent's store that holds the highest term
/// the machine has seen (node-agent-protocol.md sections 8.8 and 12).
const HIGHEST_TERM_KEY: &str = "highest_term";

/// The `meta` key that records the head of the marker whose first write
/// committed (migration 0013). It is local and never in the change log.
pub const META_KEY: &str = "restore_marker";

/// The system actor of the first write's entries, as of the primary status
/// move it may carry (`audit-log-format.md` section 3.5), and of
/// `cluster.restored` (section 3.9).
const ACTOR: &str = "cluster";

/// The system record of a first write made after `--accept-loss`
/// (`audit-log-format.md` section 3.9).
pub const RESTORED_OPERATION: &str = "cluster.restored";

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
        /// Whether the marker asks for `--accept-loss`. The process that
        /// committed the first write stopped before it deleted the marker,
        /// so before it served: it sent no `lease_grant`.
        accept_loss: bool,
    },
    /// The marker names this database's log head: the first write is due.
    FirstWrite {
        /// The marker.
        marker: Marker,
        /// The node `primary-node` names, trimmed; `None` without the file.
        node_id: Option<String>,
        /// The highest term this host's node agent stored; `None` without
        /// its store or a stored term.
        agent_term: Option<u64>,
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
    /// The first write committed; it marked `marked` lease rows, removed the
    /// machines `removed` lists (only after `--accept-loss`, which also
    /// recorded `cluster.restored`), and moved the primary status when
    /// `moved`.
    FirstWrite {
        marked: usize,
        removed: Vec<String>,
        moved: bool,
    },
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
/// It reads the term this host's node agent stored from the agent's store in
/// [`NODE_DATA_DIR`] ([`check_with_node_data_dir`]).
///
/// # Errors
///
/// As [`check_with_node_data_dir`].
pub async fn check(store: &Store, data_dir: &Path) -> Result<Pending, String> {
    check_with_node_data_dir(store, data_dir, Path::new(NODE_DATA_DIR)).await
}

/// [`check`], reading the term this host's node agent stored from the
/// agent's store in `node_data_dir`, read-only, when the first write is due.
///
/// # Errors
///
/// A one-line reason that stops the start, with nothing written, nothing
/// migrated and the marker and `primary-node` left in place: a
/// `primary-node` file that cannot be read when the first write is due; an
/// agent store that exists and whose term cannot be read; a first term that
/// would pass the term ceiling (node-agent-protocol.md section 8.8), which
/// stops every later start the same way; after `--accept-loss`, a lease of a
/// machine to remove whose token is already at the cap (`token_exhausted`,
/// node-agent-protocol.md section 8.6), which stops every later start the
/// same way until an operator resolves it; or a database error.
pub async fn check_with_node_data_dir(
    store: &Store,
    data_dir: &Path,
    node_data_dir: &Path,
) -> Result<Pending, String> {
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
        return Ok(Pending::AlreadyWritten {
            head: marker.head,
            accept_loss: marker.accept_loss,
        });
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
    let stops = "the start stops before its migrations, and nothing was written";
    let agent_term = agent_term(node_data_dir).await.map_err(|reason| {
        format!(
            "{reason}; the restored primary starts above the term this host's node agent \
             stored (backup-format.md section 8.2), so {stops}"
        )
    })?;
    let mut tx = store.pool().begin().await.map_err(|error| {
        format!(
            "cannot read the database before the restored primary's first write: {error}; {stops}"
        )
    })?;
    let first = first_term(&mut tx, agent_term)
        .await
        .map_err(|reason| format!("{reason}; {stops}"))?;
    tx.rollback().await.map_err(|error| {
        format!("cannot end the read before the restored primary's first write: {error}; {stops}")
    })?;
    if let Err(reason) = first {
        return Err(format!(
            "{} asks for the restored primary's first write: {reason}; {stops}, and every later \
             start stops the same way: no restore can take a higher term (node-agent-protocol.md \
             section 8.8)",
            path.display()
        ));
    }
    if marker.accept_loss {
        check_accept_loss(store, &path, node_id.as_deref()).await?;
    }
    Ok(Pending::FirstWrite {
        marker,
        node_id,
        agent_term,
    })
}

/// The restored primary's first term (`backup-format.md` section 8.2,
/// "Term"), read on `conn`: one above the highest of the change log's terms,
/// `meta.term` and `agent_term`. A `meta.term` that holds no term
/// ([`read_term`]) is logged and left out: the first write replaces it. A
/// database without a `change_log` or `meta` table yet has no term there.
///
/// Gives `Ok(Err(reason))` when that term would pass the term ceiling,
/// 2^53 - 1 (node-agent-protocol.md section 8.8).
///
/// # Errors
///
/// A one-line reason for a database error.
async fn first_term(
    conn: &mut SqliteConnection,
    agent_term: Option<u64>,
) -> Result<Result<i64, String>, String> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('meta', 'change_log')",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|error| format!("cannot read the database's tables: {error}"))?;
    let has = |name: &str| tables.iter().any(|table| table == name);
    let log_term: Option<i64> = if has("change_log") {
        sqlx::query_scalar("SELECT MAX(term) FROM change_log")
            .fetch_one(&mut *conn)
            .await
            .map_err(|error| format!("cannot read the change log's terms: {error}"))?
    } else {
        None
    };
    let meta_term = if has("meta") {
        match read_term(&mut *conn).await {
            Ok(term) => Some(term),
            Err(TermError::Database(error)) => {
                return Err(format!("cannot read meta.term: {error}"));
            }
            Err(TermError::Bad(value)) => {
                tracing::warn!(
                    meta_term = %value,
                    "the restored meta.term holds no term; the first write replaces it"
                );
                None
            }
        }
    } else {
        None
    };
    let agent = agent_term.map(|term| i64::try_from(term).unwrap_or(i64::MAX));
    let highest = [log_term, meta_term, agent]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0);
    Ok(highest
        .checked_add(1)
        .filter(|term| *term <= MAX_TERM)
        .ok_or_else(|| {
            format!(
                "its first term, one above {highest} (the highest of the restored log's terms \
                 {log_term:?}, meta.term {meta_term:?} and the term this host's node agent \
                 stored {agent_term:?}), would pass the term ceiling, {MAX_TERM}"
            )
        }))
}

/// The highest term this host's node agent stored, `node_meta.highest_term`
/// of its store in `node_data_dir` (node-agent-protocol.md sections 8.8 and
/// 12), opened read-only. No store, or a store without that key, gives
/// `None`.
///
/// # Errors
///
/// A one-line reason when the store exists but its term cannot be read, or
/// is not a plain decimal integer.
async fn agent_term(node_data_dir: &Path) -> Result<Option<u64>, String> {
    use sqlx::ConnectOptions as _;
    let path = node_data_dir.join(NODE_STORE_FILE);
    let unreadable = |reason: String| {
        format!(
            "cannot read the term this host's node agent stored in {}: {reason}",
            path.display()
        )
    };
    let exists = path
        .try_exists()
        .map_err(|error| unreadable(error.to_string()))?;
    if !exists {
        return Ok(None);
    }
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .read_only(true)
        .connect()
        .await
        .map_err(|error| unreadable(error.to_string()))?;
    let stored: Result<Option<String>, sqlx::Error> =
        sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
            .bind(HIGHEST_TERM_KEY)
            .fetch_optional(&mut conn)
            .await;
    if let Err(error) = sqlx::Connection::close(conn).await {
        tracing::debug!(%error, "cannot close this host's node agent store after reading its term");
    }
    let Some(text) = stored.map_err(|error| unreadable(error.to_string()))? else {
        return Ok(None);
    };
    let term = (!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| text.parse::<u64>().ok())
        .flatten()
        .ok_or_else(|| unreadable(format!("{HIGHEST_TERM_KEY} {text:?} is not a term")))?;
    Ok(Some(term))
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
/// A one-line reason for a database error, which stops the start, for a
/// first term above the term ceiling, or, after `--accept-loss`, for a lease
/// to raise at the token cap; the last two are the backstops of what
/// [`check`] already refuses. The first write is then not committed, so none
/// of it is written, and the marker and `primary-node` stay for the next
/// start.
pub async fn apply(store: &Store, data_dir: &Path, pending: Pending) -> Result<Applied, String> {
    let path = data_dir.join(MARKER_FILE);
    let (marker, node_id, agent_term) = match pending {
        Pending::NoMarker => {
            primary_status::apply_marker(store, data_dir).await?;
            return Ok(Applied::NoMarker);
        }
        Pending::AlreadyWritten { head, accept_loss } => {
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
            // The process that committed it stopped before this deletion,
            // so before it served: it sent no `lease_grant`.
            if accept_loss {
                store.allow_term_restarts();
            }
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
        Pending::FirstWrite {
            marker,
            node_id,
            agent_term,
        } => (marker, node_id, agent_term),
    };

    let mut w = store
        .begin_write()
        .await
        .map_err(|error| format!("cannot begin the restored primary's first write: {error}"))?;
    // The restored primary's term, before anything else of the write, so
    // every entry and record of the write carries it (`backup-format.md`
    // section 8.2, "Term"). The terms are read again inside the write; the
    // ceiling here is the backstop of what [`check`] already refuses.
    let term = first_term(&mut w.tx, agent_term).await?.map_err(|reason| {
        format!("the restored primary's first write: {reason}; nothing was written")
    })?;
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES ('term', ?) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(term.to_string())
    .execute(&mut *w.tx)
    .await
    .map_err(|error| format!("cannot write the restored primary's term to meta.term: {error}"))?;
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
    // With `--accept-loss`, the node that holds the primary status before the
    // removals, which clear it: the move's `cluster.primary_moved` names it as
    // `from_node_id`. Without it nothing clears the status before the move,
    // which names the node that holds it, as `move_primary` does.
    let held_before: Option<String> = match &node_id {
        Some(_) if marker.accept_loss => sqlx::query_scalar(
            "SELECT id FROM nodes WHERE cp_status = 'primary' ORDER BY id LIMIT 1",
        )
        .fetch_optional(&mut *w.tx)
        .await
        .map_err(|error| format!("cannot read the primary node: {error}"))?,
        _ => None,
    };
    let (removed, cleared) = match (marker.accept_loss, marker.statement.as_deref()) {
        (true, Some(statement)) => {
            remove_the_lost(
                &mut w.tx,
                &actor,
                &marker.head,
                node_id.as_deref(),
                statement,
            )
            .await?
        }
        _ => (Vec::new(), false),
    };
    // After `cluster.restored`, so that record is the write's first.
    let moved = match &node_id {
        Some(node_id) => {
            Some(primary_status::move_in(&mut w.tx, node_id, held_before.as_deref()).await?)
        }
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
        match moved {
            // The status did not stay where it was: the write cleared it.
            Moved::No(reason) if cleared => tracing::warn!(
                node_id = node_id.as_str(),
                reason = reason,
                "the primary-node file left by quoro restore was not used; the --accept-loss first write cleared the primary status on every machine"
            ),
            _ => primary_status::log_moved(node_id, moved),
        }
        primary_status::remove_file(data_dir);
    }
    tracing::info!(
        head_seq = marker.head.seq,
        marked = marked.len(),
        term,
        "made the restored primary's first write: it starts at this term, and every expiring lease row is marked unsettled"
    );
    if marker.accept_loss {
        // Until its first `lease_grant`, a higher term starts it again
        // instead of demoting it (node-agent-protocol.md section 8.8).
        store.allow_term_restarts();
        tracing::warn!(
            removed_node_ids = ?removed,
            "the restore was made with --accept-loss: its first write removed every other machine with the node role and recorded cluster.restored"
        );
    }
    if cleared {
        tracing::warn!(
            "no machine holds the primary status: the next local enrollment of this host's node agent takes it"
        );
    }
    remove(&path);
    Ok(Applied::FirstWrite {
        marked: marked.len(),
        removed,
        moved: matches!(moved, Some(Moved::Yes { .. })),
    })
}

/// The `--accept-loss` part of the first write ([module docs](self)), in the
/// first write's transaction `conn`, after the mark: removes every machine
/// with the `node` role that the database holds, not `removed`, other than
/// `host_node`, the node `primary-node` names; when `host_node` will not hold
/// the primary status after the write (none, or a node the primary cannot
/// use, [`primary_status::read_named`]), clears `cp_status` on every machine
/// that still holds it ([`clear_cp_status`]); then records `cluster.restored`
/// with the operator's `statement`. Returns the removed ids, by id, and
/// whether it cleared `cp_status` on every machine.
///
/// `cluster.restored`'s `params.term` is the term of the first write, the
/// restored primary's first term, which the write set in `meta.term` before
/// anything else (`audit-log-format.md` section 3.9). A later restart above a
/// higher term leaves the record as it is. Its `change_seq` is the removal
/// entry of the last machine removed, or, with none removed, the newest entry
/// the write appended before it, if any.
///
/// # Errors
///
/// A one-line reason after which the caller drops the transaction, so
/// nothing is written: a lease at the token cap (`token_exhausted`), a
/// `meta.term` or `meta.cluster_id` that cannot be read, or a database
/// error.
async fn remove_the_lost(
    conn: &mut SqliteConnection,
    actor: &Actor,
    head: &Head,
    host_node: Option<&str>,
    statement: &str,
) -> Result<(Vec<String>, bool), String> {
    let term = read_term(&mut *conn)
        .await
        .map_err(|error| format!("cannot read the term for {RESTORED_OPERATION}: {error}"))?;
    let cluster_id: Option<String> =
        sqlx::query_scalar("SELECT value FROM meta WHERE key = 'cluster_id'")
            .fetch_optional(&mut *conn)
            .await
            .map_err(|error| format!("cannot read meta.cluster_id: {error}"))?;
    let Some(cluster_id) = cluster_id else {
        return Err(format!(
            "the database names no cluster (meta.cluster_id) for {RESTORED_OPERATION}"
        ));
    };
    let lost = to_remove(&mut *conn, host_node).await?;

    let mut last_removal = None;
    for node_id in &lost {
        // The rows declaring the node lost raises (section 8.3, ADR 0202),
        // revoked ones below the cap included, without its early return for
        // a node already declared lost: a lease granted after that
        // declaration is raised too. No `lost_at` is set.
        let raise =
            format!("cannot raise the lease tokens of node {node_id}, which --accept-loss removes");
        crate::api::nodes::raise_lost_leases(conn, node_id, actor)
            .await
            .map_err(|error| api_reason(&raise, error))?;
        sqlx::query("UPDATE nodes SET status = 'removed', cp_status = NULL WHERE id = ?")
            .bind(node_id)
            .execute(&mut *conn)
            .await
            .map_err(|error| format!("cannot remove node {node_id}: {error}"))?;
        let payload = crate::driver::node_payload(conn, node_id)
            .await
            .map_err(|error| api_reason(&format!("cannot read node {node_id}"), error))?;
        last_removal = Some(
            changelog::append(conn, actor, "node", node_id, Op::Update, payload)
                .await
                .map_err(|error| format!("cannot log the removal of node {node_id}: {error}"))?,
        );
        crate::custody::delete_seals(conn, node_id).await?;
        crate::quarantines::drop_node_copies(conn, actor, node_id)
            .await
            .map_err(|error| {
                api_reason(
                    &format!("cannot drop the quarantined copies of node {node_id}"),
                    error,
                )
            })?;
    }
    // When the new host's node does not hold the primary status after the
    // write, no machine keeps it, whatever its roles (`backup-format.md`
    // section 8.2): with no `primary-node`, or with one that names a node the
    // primary cannot use, which the move that follows leaves as it is. The
    // next local enrollment of the new host's agent then takes it, and adopts
    // no old row. The host's node is never removed above, so it reads the
    // same now as when the move runs. A machine removed above reads no
    // `cp_status` already, in its removal entry, and is not written again.
    // The clear records no `cluster.primary_moved`: `cluster.restored` below
    // stands for it (`audit-log-format.md` section 3.9).
    let host_holds = match host_node {
        Some(node_id) => primary_status::read_named(&mut *conn, node_id)
            .await?
            .holds_after_move(),
        None => false,
    };
    if !host_holds {
        clear_cp_status(conn, actor).await?;
    }

    let change_seq = match last_removal {
        Some(seq) => Some(seq),
        None => {
            let newest: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
                .fetch_one(&mut *conn)
                .await
                .map_err(|error| format!("cannot read the change log's head: {error}"))?;
            newest.filter(|seq| *seq > head.seq)
        }
    };
    let record = Record {
        actor: actor.clone(),
        agent_model: None,
        on_behalf_of: None,
        channel: Channel::System,
        operation: RESTORED_OPERATION.to_string(),
        scope: None,
        target_type: Some("cluster".to_string()),
        target_id: Some(cluster_id),
        project_id: None,
        params: json!({
            "removed_node_ids": lost,
            "term": term,
        }),
        justification: Some(statement.to_string()),
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        result: Outcome::Ok,
        result_detail: None,
        change_seq,
    };
    crate::audit::append(&mut *conn, &record)
        .await
        .map_err(|error| format!("cannot record {RESTORED_OPERATION}: {error}"))?;
    Ok((lost, !host_holds))
}

/// Clears `cp_status` on every machine that still holds it, whatever its
/// roles, each row with its whole-row node entry, as a move of the primary
/// status clears it ([`primary_status::move_in`]). Run on the first write's
/// transaction `conn` after `--accept-loss`'s removals when the new host's
/// node will not hold the status after the write: no `primary-node`, or one
/// that names a node the primary cannot use. It writes no audit record.
///
/// # Errors
///
/// A one-line reason for a database error, after which the caller drops the
/// transaction, so nothing is written.
async fn clear_cp_status(conn: &mut SqliteConnection, actor: &Actor) -> Result<(), String> {
    let holders: Vec<String> =
        sqlx::query_scalar("SELECT id FROM nodes WHERE cp_status IS NOT NULL ORDER BY id")
            .fetch_all(&mut *conn)
            .await
            .map_err(|error| format!("cannot read the nodes that hold a cp_status: {error}"))?;
    for node_id in &holders {
        sqlx::query("UPDATE nodes SET cp_status = NULL WHERE id = ?")
            .bind(node_id)
            .execute(&mut *conn)
            .await
            .map_err(|error| format!("cannot clear the cp_status of node {node_id}: {error}"))?;
        let payload = crate::driver::node_payload(conn, node_id)
            .await
            .map_err(|error| api_reason(&format!("cannot read node {node_id}"), error))?;
        changelog::append(conn, actor, "node", node_id, Op::Update, payload)
            .await
            .map_err(|error| {
                format!("cannot log the cleared cp_status of node {node_id}: {error}")
            })?;
    }
    Ok(())
}

/// The machines the `--accept-loss` part of the first write removes, read
/// on `conn`: every machine with the `node` role that the database holds,
/// not `removed`, other than `host_node`, the new host's node. Sorted by id.
/// `quoro restore --accept-loss` reads the same list from the restored
/// database before it installs it.
///
/// # Errors
///
/// A one-line reason when the nodes cannot be read or a node's roles are
/// not a JSON list of strings.
pub async fn to_remove(
    conn: &mut SqliteConnection,
    host_node: Option<&str>,
) -> Result<Vec<String>, String> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, roles FROM nodes WHERE status <> 'removed' ORDER BY id")
            .fetch_all(&mut *conn)
            .await
            .map_err(|error| format!("cannot read the nodes to remove: {error}"))?;
    let mut lost = Vec::new();
    for (node_id, roles) in rows {
        let roles: Vec<String> = serde_json::from_str(&roles)
            .map_err(|error| format!("cannot read the roles of node {node_id}: {error}"))?;
        if Some(node_id.as_str()) != host_node && roles.iter().any(|role| role == "node") {
            lost.push(node_id);
        }
    }
    Ok(lost)
}

/// Every lease the first write would raise for the machines `node_ids`
/// that is already at the token cap (node-agent-protocol.md section 8.6,
/// "The cap"; ADR 0325), read on `conn`, as `(lease id, token)` sorted by
/// machine, then lease id. These are the rows declaring a node lost raises
/// ([`crate::api::nodes::raise_lost_leases`]) whose token
/// [`crate::leases::bump_tokens`] refuses to raise: the unrevoked rows at
/// the cap. A revoked row is raised only below the cap, so it reaches at
/// most the cap and never refuses. Only columns of the first schema are
/// read, so it reads an unmigrated database too.
///
/// # Errors
///
/// A database error.
pub async fn leases_at_cap(
    conn: &mut SqliteConnection,
    node_ids: &[String],
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let mut at_cap = Vec::new();
    for node_id in node_ids {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT id, fencing_token FROM leases \
             WHERE holder_node_id = ? AND revoked_at IS NULL AND fencing_token >= ? ORDER BY id",
        )
        .bind(node_id)
        .bind(crate::leases::MAX_TOKEN)
        .fetch_all(&mut *conn)
        .await?;
        at_cap.extend(rows);
    }
    Ok(at_cap)
}

/// The refusal of a raise past the token cap, naming each `(lease id,
/// token)` of `at_cap`, worded as every other path that refuses at the cap
/// ([`crate::leases::token_exhausted_detail`]).
#[must_use]
pub fn token_cap_refusal(at_cap: &[(String, i64)]) -> String {
    let named: Vec<(&str, i64)> = at_cap
        .iter()
        .map(|(lease_id, token)| (lease_id.as_str(), *token))
        .collect();
    crate::leases::token_exhausted_detail(&named)
}

/// What [`check`] reads, read-only, for a marker of this database that asks
/// for `--accept-loss`: that no lease the first write would raise is already
/// at the token cap. `marker_path` names the marker in the reason;
/// `host_node` is the node `primary-node` names.
async fn check_accept_loss(
    store: &Store,
    marker_path: &Path,
    host_node: Option<&str>,
) -> Result<(), String> {
    let stops = "the start stops before its migrations, and nothing was written";
    let mut tx = store.pool().begin().await.map_err(|error| {
        format!(
            "cannot read the database before the restored primary's first write: {error}; {stops}"
        )
    })?;
    let lost = to_remove(&mut tx, host_node)
        .await
        .map_err(|reason| format!("{reason}; {stops}"))?;
    let at_cap = leases_at_cap(&mut tx, &lost)
        .await
        .map_err(|error| format!("cannot read the lease tokens to raise: {error}; {stops}"))?;
    tx.rollback().await.map_err(|error| {
        format!("cannot end the read before the restored primary's first write: {error}; {stops}")
    })?;
    if !at_cap.is_empty() {
        return Err(format!(
            "{} asks for a restore with --accept-loss, whose first write raises the lease tokens \
             of the machines it removes: {}; {stops}, and every later start stops the same way \
             until an operator resolves it",
            marker_path.display(),
            token_cap_refusal(&at_cap)
        ));
    }
    Ok(())
}

/// `what`, an [`ApiError`]'s detail (or its code), and that the first write
/// wrote nothing: the caller drops the transaction.
fn api_reason(what: &str, error: ApiError) -> String {
    let reason = error
        .detail
        .unwrap_or_else(|| error.code.as_str().to_string());
    format!("{what}: {reason}; nothing was written")
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
