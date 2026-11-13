// SPDX-License-Identifier: AGPL-3.0-only
//! The database a promotion rebuilds from the followed change log
//! (`node-agent-protocol.md` section 7.3 step 4; ADR 0228, ADR 0229, ADR
//! 0286, ADR 0297, ADR 0317, ADR 0322).
//!
//! [`build`] writes it into a new file:
//!
//! 1. It creates the schema of this binary, `db-schema.sql` and every
//!    migration, through the same migration path [`Store`] uses.
//! 2. It folds the entries per entity ([`super::fold`]) and stops on the
//!    fold's errors.
//! 3. It applies the schema's `ON DELETE CASCADE` deletes itself
//!    ([`Cascaded`]): a row the primary removed with its parent has no entry
//!    of its own. A row goes with a parent deleted after its own last
//!    `create`, when it pointed at that parent at the time of the delete; a
//!    parent created again later under the same id does not bring back the
//!    rows deleted before it.
//! 4. It inserts the final rows in one transaction with foreign keys
//!    deferred, decoding each column as its writer encodes it in the payload
//!    ([`insert_rows`]).
//! 5. It copies every entry into `change_log` in `seq` order, unchanged.
//! 6. It writes `meta`: `cluster_id` from the agent's identity, `term` the
//!    promotion's term, `schema_version` this binary's, and `webhook_cursor`
//!    the `seq` of the newest event in the rebuilt event log (0 when it holds
//!    none). No other key: `meta` is not in the log.
//! 7. Tables marked LOCAL start empty, and `backup_repositories.repo_key_enc`
//!    is an empty BLOB (section 7.1).
//! 8. It verifies the change-log chain and the audit chain on the new file
//!    (`audit-log-format.md` section 6) and stops on a break.
//!
//! The file is built under a temporary name next to the target and linked
//! into place only once everything above holds, so a build that stops leaves
//! nothing at the target path, and one that finds the path taken refuses.
//!
//! # How a payload's values become columns
//!
//! A payload carries each column under the column's name, as its writer
//! encodes it today:
//!
//! - an `INTEGER` column takes a JSON integer, or a boolean as 0 or 1;
//! - a `REAL` column takes a JSON number;
//! - a `TEXT` column takes a JSON string as it stands, an object or an array
//!   as its JSON text (RFC 8785, the form the audit log stores `params` in),
//!   or an integer as its decimal text;
//! - a `BLOB` column takes a string in its writer's encoding
//!   ([`BLOB_ENCODINGS`]): lowercase hex for the `*_enc` ciphertexts and the
//!   tunnel run token, standard base64 for a custody seal (migration 0012);
//! - JSON `null` is SQL `NULL`.
//!
//! A value that does not fit its column stops the build with an error that
//! names the entity, the `seq` of the entry that wrote the value and the
//! column ([`BuildError::Column`]).

use super::{Cell, Deletion, Dropped, Entry, FoldError, Folded, KeyMismatch, Schema, TABLES};
use crate::audit;
use crate::changelog;
use crate::store::{GENESIS_HASH, MAX_TERM, Store, random_secret};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Map, Value};
use sqlx::{Row as _, SqliteConnection};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// One followed change-log entry as the node stores it
/// (`node-agent-protocol.md` sections 5.14 and 12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowedEntry {
    pub seq: u64,
    pub term: u64,
    pub time: String,
    pub actor_type: String,
    pub actor_id: String,
    pub entity_type: String,
    pub entity_id: String,
    /// `create`, `update` or `delete`.
    pub op: String,
    /// The payload object's RFC 8785 text, as the primary stores it.
    pub payload: String,
    pub prev_hash: String,
    pub hash: String,
}

impl FollowedEntry {
    /// The entry as the fold reads it.
    #[must_use]
    pub fn as_entry(&self) -> Entry<'_> {
        Entry {
            seq: self.seq,
            entity_type: &self.entity_type,
            entity_id: &self.entity_id,
            op: &self.op,
            payload: &self.payload,
        }
    }
}

/// How a `BLOB` column travels in a change-log payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobEncoding {
    /// Hex of the bytes; the writers write it lowercase.
    Hex,
    /// Standard base64 with padding.
    Base64,
}

/// Each `BLOB` column a payload carries, and its writer's encoding. The one
/// other `BLOB` column of a replicated table, the wrapped repository key, is
/// in no payload ([`super::Cell::EmptyBlob`]).
pub const BLOB_ENCODINGS: [(&str, &str, BlobEncoding); 5] = [
    // `api::apps`: the variable's ciphertext, `hex(value_enc)`.
    ("app_env", "value_enc", BlobEncoding::Hex),
    // `custody::encode_sealed`: migration 0012 says base64.
    ("node_key_custody", "sealed", BlobEncoding::Base64),
    // `tunnel` and `api::nodes`: the run token's ciphertext, or null.
    ("node_tunnels", "token_enc", BlobEncoding::Hex),
    // `api::providers`: the credential's ciphertext.
    ("providers", "secret_enc", BlobEncoding::Hex),
    // `service::webhooks`: the signing secret's ciphertext.
    ("webhooks", "secret_enc", BlobEncoding::Hex),
];

/// The encoding of `table.column` in a payload, or `None` for a `BLOB`
/// column no writer puts in one.
#[must_use]
pub fn blob_encoding(table: &str, column: &str) -> Option<BlobEncoding> {
    BLOB_ENCODINGS
        .iter()
        .find(|(t, c, _)| *t == table && *c == column)
        .map(|(_, _, encoding)| *encoding)
}

/// A row the schema's `ON DELETE CASCADE` removed with a deleted parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cascaded {
    pub table: &'static str,
    pub entity_id: String,
    /// The `seq` of the `create` the removed row started from.
    pub created_seq: u64,
    /// The `delete` the row went with: its own parent's, or an ancestor's.
    pub with: Deletion,
}

/// What [`build`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built {
    /// The newest `seq` of the copied change log.
    pub head_seq: u64,
    /// The rows of every table the change log carries, empty ones included.
    pub rows: BTreeMap<&'static str, u64>,
    /// `meta.schema_version`: this binary's.
    pub schema_version: usize,
    /// `meta.webhook_cursor`: the newest event's `seq`, 0 for none.
    pub webhook_cursor: u64,
    /// The rows the cascades removed, in the order of their deletes.
    pub cascaded: Vec<Cascaded>,
    /// The `create`s whose key columns are not their `entity_id`
    /// ([`super::Folded::mismatches`]).
    pub mismatches: Vec<KeyMismatch>,
    /// The rows made only of updates that the fold left out
    /// ([`super::Folded::dropped`]).
    pub dropped: Vec<Dropped>,
}

/// Why [`build`] stopped. Nothing is left at the target path.
#[derive(Debug)]
pub enum BuildError {
    /// Something already exists at the target path.
    Exists(PathBuf),
    /// The followed log is empty.
    EmptyLog,
    /// The term is not one from 1 to 2^53 - 1.
    BadTerm(u64),
    /// The cluster id is empty.
    NoClusterId,
    /// The fold stopped (ADR 0317).
    Fold(FoldError),
    /// A value of the row of `entity_type` `entity_id` does not fit its
    /// column; `seq` is the entry that wrote it, or the row's `create` when
    /// the `create` lacked a column that has neither a default nor NULL.
    Column {
        entity_type: String,
        entity_id: String,
        seq: u64,
        table: &'static str,
        column: String,
        reason: String,
    },
    /// The database refused the row of `entity_type` `entity_id`, whose
    /// `create` is at `seq`.
    Insert {
        entity_type: String,
        entity_id: String,
        seq: u64,
        table: &'static str,
        error: String,
    },
    /// A rebuilt row references a row the rebuilt database lacks.
    ForeignKey {
        table: String,
        entity_id: Option<String>,
        /// The `seq` of the row's `create`, when the row is one the fold
        /// returned.
        seq: Option<u64>,
        columns: String,
        parent: String,
    },
    /// A schema `ON DELETE CASCADE` that does not reference its parent's
    /// key, which a `delete` entry, naming only the parent's key, cannot
    /// apply.
    CascadeKey {
        child: &'static str,
        parent: &'static str,
    },
    /// The change-log chain breaks at `seq`.
    ChangeLog {
        seq: u64,
        reason: String,
    },
    /// The audit chain breaks at the record `seq` (`audit-log-format.md`
    /// section 6), for `reason`: `seq_gap`, `hash_mismatch` or
    /// `prev_mismatch`.
    AuditChain {
        seq: i64,
        reason: &'static str,
    },
    /// The `-wal` file of the new database still holds pages after the
    /// last connection closed, so the database file alone is not the whole
    /// database.
    Unflushed(PathBuf),
    Database(sqlx::Error),
    Io(std::io::Error),
}

impl From<sqlx::Error> for BuildError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<std::io::Error> for BuildError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<FoldError> for BuildError {
    fn from(error: FoldError) -> Self {
        Self::Fold(error)
    }
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exists(path) => write!(f, "{} already exists", path.display()),
            Self::EmptyLog => write!(f, "the followed change log is empty"),
            Self::BadTerm(term) => write!(f, "the term {term} is not one from 1 to {MAX_TERM}"),
            Self::NoClusterId => write!(f, "the cluster id is empty"),
            Self::Fold(error) => error.fmt(f),
            Self::Column {
                entity_type,
                entity_id,
                seq,
                table,
                column,
                reason,
            } => write!(
                f,
                "the {entity_type} {entity_id}: the entry at seq {seq} cannot fill {table}.{column}: {reason}"
            ),
            Self::Insert {
                entity_type,
                entity_id,
                seq,
                table,
                error,
            } => write!(
                f,
                "the {entity_type} {entity_id}, created at seq {seq}, cannot be inserted into {table}: {error}"
            ),
            Self::ForeignKey {
                table,
                entity_id,
                seq,
                columns,
                parent,
            } => {
                write!(f, "a row of {table}")?;
                if let Some(entity_id) = entity_id {
                    write!(f, ", {entity_id}")?;
                }
                if let Some(seq) = seq {
                    write!(f, ", created at seq {seq},")?;
                }
                write!(
                    f,
                    " references {parent} through {columns}, and the rebuilt {parent} has no such row"
                )
            }
            Self::CascadeKey { child, parent } => write!(
                f,
                "the ON DELETE CASCADE of {child} references columns of {parent} other than its key"
            ),
            Self::ChangeLog { seq, reason } => {
                write!(f, "the change-log chain breaks at seq {seq}: {reason}")
            }
            Self::AuditChain { seq, reason } => {
                write!(f, "the audit chain breaks at record {seq}: {reason}")
            }
            Self::Unflushed(path) => write!(
                f,
                "{} still holds pages of the new database after it closed",
                path.display()
            ),
            Self::Database(error) => write!(f, "database: {error}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for BuildError {}

/// Builds the database a promotion at `term` rebuilds from `entries`, the
/// agent's followed change log in `seq` order, into a new file at `path`
/// (module documentation). `cluster_id` is the agent's identity's.
///
/// # Errors
///
/// [`BuildError`]; the file at `path` is then not created.
pub async fn build(
    entries: &[FollowedEntry],
    cluster_id: &str,
    term: u64,
    path: &Path,
) -> Result<Built, BuildError> {
    if entries.is_empty() {
        return Err(BuildError::EmptyLog);
    }
    if !(1..=MAX_TERM.unsigned_abs()).contains(&term) {
        return Err(BuildError::BadTerm(term));
    }
    if cluster_id.is_empty() {
        return Err(BuildError::NoClusterId);
    }
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(BuildError::Exists(path.to_path_buf()));
    }
    let temporary = temporary_path(path)?;
    create_new(&temporary)?;
    let built = match write(entries, cluster_id, term, &temporary).await {
        Ok(built) => built,
        Err(error) => {
            remove_database(&temporary);
            return Err(error);
        }
    };
    if let Err(error) = settle(&temporary, path) {
        remove_database(&temporary);
        return Err(error);
    }
    Ok(built)
}

/// A name next to `path` that no other build takes:
/// `.<file name>.rebuild-<random hex>`.
fn temporary_path(path: &Path) -> Result<PathBuf, BuildError> {
    let name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} names no file", path.display()),
        )
    })?;
    let mut temporary = std::ffi::OsString::from(".");
    temporary.push(name);
    temporary.push(format!(".rebuild-{}", &random_secret()[..16]));
    Ok(path.with_file_name(temporary))
}

/// Creates an empty file at `path`, refusing one that exists, readable by
/// its owner only. SQLite takes an empty file as a new database.
fn create_new(path: &Path) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path).map(drop)
}

/// The `-wal` and `-shm` files SQLite keeps next to `path`.
fn sidecars(path: &Path) -> [PathBuf; 2] {
    ["-wal", "-shm"].map(|suffix| {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        PathBuf::from(name)
    })
}

/// Removes the database at `path` and its sidecars, ignoring what is not
/// there.
fn remove_database(path: &Path) {
    for file in std::iter::once(path.to_path_buf()).chain(sidecars(path)) {
        if let Err(error) = std::fs::remove_file(&file)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, path = %file.display(), "cannot remove a stopped rebuild");
        }
    }
}

/// Moves the closed database at `temporary` to `path`: it checks that no
/// `-wal` page is left, removes the empty sidecars, syncs the file, links
/// it at `path`, which fails when `path` exists, removes the temporary
/// name and syncs the directory. When a step after the link fails, it
/// removes the link it made, so the build's error leaves no file at `path`.
fn settle(temporary: &Path, path: &Path) -> Result<(), BuildError> {
    let [wal, shm] = sidecars(temporary);
    match std::fs::metadata(&wal) {
        Ok(meta) if meta.len() > 0 => return Err(BuildError::Unflushed(wal)),
        Ok(_) | Err(_) => {}
    }
    for sidecar in [wal, shm] {
        match std::fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    std::fs::File::open(temporary)?.sync_all()?;
    match std::fs::hard_link(temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(BuildError::Exists(path.to_path_buf()));
        }
        Err(error) => return Err(error.into()),
    }
    let linked = std::fs::remove_file(temporary).and_then(|()| {
        match path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
            Some(dir) => std::fs::File::open(dir)?.sync_all(),
            None => Ok(()),
        }
    });
    if let Err(error) = linked {
        if let Err(unlink) = std::fs::remove_file(path) {
            tracing::warn!(error = %unlink, path = %path.display(), "cannot remove a stopped rebuild");
        }
        return Err(error.into());
    }
    Ok(())
}

/// Writes the database into the empty file at `path` and closes it.
async fn write(
    entries: &[FollowedEntry],
    cluster_id: &str,
    term: u64,
    path: &Path,
) -> Result<Built, BuildError> {
    let store = Store::open(path).await?;
    let built = fill(&store, entries, cluster_id, term).await;
    if built.is_ok() {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(store.pool())
            .await?;
    }
    store.pool().close().await;
    built
}

/// Steps 2 to 8 of the module documentation, into `store`, a new database
/// with this binary's schema.
async fn fill(
    store: &Store,
    followed: &[FollowedEntry],
    cluster_id: &str,
    term: u64,
) -> Result<Built, BuildError> {
    let entries: Vec<Entry<'_>> = followed.iter().map(FollowedEntry::as_entry).collect();
    let mut w = store.begin_write().await?;
    sqlx::query("PRAGMA defer_foreign_keys = ON")
        .execute(&mut *w.tx)
        .await?;
    let schema = Schema::read(&mut w.tx).await?;
    let mut folded = super::fold(&schema, entries.iter().copied())?;
    let foreign = cascades(&mut w.tx, &schema).await?;
    let cascaded = cascade(&schema, &foreign, &entries);
    for removed in &cascaded {
        if let Some(rows) = folded.tables.get_mut(removed.table)
            && rows
                .get(&removed.entity_id)
                .is_some_and(|row| row.created_seq == removed.created_seq)
        {
            rows.remove(&removed.entity_id);
        }
    }
    insert_rows(&mut w.tx, &schema, &folded, &entries).await?;
    let head_seq = copy_change_log(&mut w.tx, followed).await?;

    let (webhook_cursor,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(&mut *w.tx)
        .await?;
    for (key, value) in [
        ("cluster_id", cluster_id.to_owned()),
        ("term", term.to_string()),
        ("webhook_cursor", webhook_cursor.to_string()),
    ] {
        sqlx::query("INSERT INTO meta (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&mut *w.tx)
            .await?;
    }
    let (schema_version,): (String,) =
        sqlx::query_as("SELECT value FROM meta WHERE key = 'schema_version'")
            .fetch_one(&mut *w.tx)
            .await?;
    check_foreign_keys(&mut w.tx, &schema, &folded).await?;
    let mut rows = BTreeMap::new();
    for (_, table) in TABLES {
        let (count,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{table}\""))
            .fetch_one(&mut *w.tx)
            .await?;
        rows.insert(table, count.unsigned_abs());
    }
    w.commit().await?;

    let mut conn = store.pool().acquire().await?;
    verify_chains(&mut conn).await?;
    Ok(Built {
        head_seq,
        rows,
        schema_version: schema_version.parse().unwrap_or_default(),
        webhook_cursor: webhook_cursor.unsigned_abs(),
        cascaded,
        mismatches: folded.mismatches,
        dropped: folded.dropped,
    })
}

/// Copies `entries` into `change_log` unchanged, in `seq` order, and returns
/// the newest `seq`. Each must follow the one before it, from `seq` 1 and
/// the genesis hash; [`verify_chains`] re-hashes them once written.
async fn copy_change_log(
    conn: &mut SqliteConnection,
    entries: &[FollowedEntry],
) -> Result<u64, BuildError> {
    let mut expected = 1;
    let mut prev_hash = GENESIS_HASH;
    for entry in entries {
        let broken = |reason: &str| BuildError::ChangeLog {
            seq: entry.seq,
            reason: reason.to_owned(),
        };
        if entry.seq != expected {
            return Err(broken(&format!("it comes where seq {expected} belongs")));
        }
        if entry.prev_hash != prev_hash {
            return Err(broken(
                "its prev_hash is not the hash of the entry before it",
            ));
        }
        sqlx::query(
            "INSERT INTO change_log (seq, term, time, actor_type, actor_id, entity_type, \
             entity_id, op, payload, prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(i64::try_from(entry.seq).map_err(|_| broken("seq out of range"))?)
        .bind(i64::try_from(entry.term).map_err(|_| broken("term out of range"))?)
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
        .await
        .map_err(|error| broken(&error.to_string()))?;
        expected += 1;
        prev_hash = &entry.hash;
    }
    Ok(expected - 1)
}

/// Re-hashes the change log from `seq` 1 ([`changelog::verify`]) and walks
/// the audit chain from record 1 ([`audit::verify_records`]).
async fn verify_chains(conn: &mut SqliteConnection) -> Result<(), BuildError> {
    match changelog::verify(conn).await {
        Ok(None) => {}
        Ok(Some(seq)) => {
            return Err(BuildError::ChangeLog {
                seq: seq.unsigned_abs(),
                reason: "its hash or prev_hash does not match".to_owned(),
            });
        }
        Err(error) => {
            return Err(BuildError::ChangeLog {
                seq: 0,
                reason: error.to_string(),
            });
        }
    }
    let rows = sqlx::query(&format!(
        "SELECT {} FROM audit_log ORDER BY seq",
        audit::COLUMNS
    ))
    .fetch_all(&mut *conn)
    .await?;
    let records = rows
        .iter()
        .map(audit::record_value_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let verification = audit::verify_records(records, 1, GENESIS_HASH);
    if !verification.ok {
        return Err(BuildError::AuditChain {
            seq: verification.broken_at_seq.unwrap_or_default(),
            reason: verification.reason.unwrap_or_default(),
        });
    }
    Ok(())
}

/// One `ON DELETE CASCADE` of the schema between two tables the change log
/// carries: deleting a row of `parent` deletes the rows of `child` whose
/// `columns` hold the parent's key, in key order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Cascade {
    child: &'static str,
    columns: Vec<String>,
    parent: &'static str,
}

/// One column of a foreign key, as `pragma_foreign_key_list` reports it.
struct ForeignColumn {
    /// The foreign key's number within its table.
    id: i64,
    /// The parent table.
    table: String,
    /// The child's column.
    from: String,
    /// The parent's column; `None` for the parent's primary key.
    to: Option<String>,
    on_delete: String,
}

/// The `ON DELETE CASCADE` foreign keys among the tables the change log
/// carries, as `pragma_foreign_key_list` reports them. A cascade from or to
/// a LOCAL table is left out: LOCAL tables start empty.
async fn cascades(
    conn: &mut SqliteConnection,
    schema: &Schema,
) -> Result<Vec<Cascade>, BuildError> {
    let mut found = Vec::new();
    for (_, child) in TABLES {
        let rows = sqlx::query(
            "SELECT id, \"table\", \"from\", \"to\", on_delete \
             FROM pragma_foreign_key_list(?) ORDER BY id, seq",
        )
        .bind(child)
        .fetch_all(&mut *conn)
        .await?;
        let mut by_id: BTreeMap<i64, Vec<ForeignColumn>> = BTreeMap::new();
        for row in rows {
            let column = ForeignColumn {
                id: row.try_get(0)?,
                table: row.try_get(1)?,
                from: row.try_get(2)?,
                to: row.try_get(3)?,
                on_delete: row.try_get(4)?,
            };
            by_id.entry(column.id).or_default().push(column);
        }
        for columns in by_id.into_values() {
            if !columns[0].on_delete.eq_ignore_ascii_case("CASCADE") {
                continue;
            }
            let Some(parent) = TABLES
                .iter()
                .map(|(_, table)| *table)
                .find(|table| table.eq_ignore_ascii_case(&columns[0].table))
            else {
                continue;
            };
            let key = schema.key(parent).unwrap_or_default();
            let to: Vec<String> = columns
                .iter()
                .zip(key)
                .map(|(column, key)| column.to.clone().unwrap_or_else(|| key.clone()))
                .collect();
            if columns.len() != key.len() || to != key {
                return Err(BuildError::CascadeKey { child, parent });
            }
            found.push(Cascade {
                child,
                columns: columns.into_iter().map(|column| column.from).collect(),
                parent,
            });
        }
    }
    Ok(found)
}

/// A key column's value as an `entity_id` part: a string as it stands, an
/// integer in decimal. `None` for anything else, which names no row.
fn key_part(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) if number.is_i64() || number.is_u64() => Some(number.to_string()),
        _ => None,
    }
}

/// A row of a cascade's child table while [`cascade`] walks the log.
struct Live {
    /// The `seq` of its last `create`.
    created_seq: u64,
    /// The cascades' columns, each as a key part, as of the entry walked.
    values: BTreeMap<String, Option<String>>,
}

/// Walks `entries` in `seq` order and returns the rows each `delete` would
/// have removed through `cascades`, in order: every row of a child table
/// that holds, at the time of the delete, the deleted row's key in the
/// cascade's columns, and in turn the rows those removals cascade to. A row
/// is tracked from its `create`, so a parent created again later under the
/// same id finds none of the rows removed before it.
fn cascade(schema: &Schema, cascades: &[Cascade], entries: &[Entry<'_>]) -> Vec<Cascaded> {
    let mut columns: BTreeMap<&'static str, BTreeSet<&str>> = BTreeMap::new();
    for cascade in cascades {
        columns
            .entry(cascade.child)
            .or_default()
            .extend(cascade.columns.iter().map(String::as_str));
    }
    let mut live: BTreeMap<&'static str, BTreeMap<String, Live>> = BTreeMap::new();
    let mut removed = Vec::new();
    for entry in entries {
        let Some(table) = super::table_of(entry.entity_type) else {
            continue;
        };
        match entry.op {
            "create" | "update" => {
                let Some(tracked) = columns.get(table) else {
                    continue;
                };
                let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(entry.payload) else {
                    continue;
                };
                let rows = live.entry(table).or_default();
                if entry.op == "create" {
                    let values = tracked
                        .iter()
                        .map(|column| {
                            ((*column).to_owned(), fields.get(*column).and_then(key_part))
                        })
                        .collect();
                    rows.insert(
                        entry.entity_id.to_owned(),
                        Live {
                            created_seq: entry.seq,
                            values,
                        },
                    );
                } else if let Some(row) = rows.get_mut(entry.entity_id) {
                    let key = schema.key(table).unwrap_or_default();
                    for column in tracked {
                        if key.iter().any(|k| k.as_str() == *column) {
                            continue;
                        }
                        if let Some(value) = fields.get(*column) {
                            row.values.insert((*column).to_owned(), key_part(value));
                        }
                    }
                }
            }
            "delete" => {
                if let Some(rows) = live.get_mut(table) {
                    rows.remove(entry.entity_id);
                }
                let with = Deletion {
                    entity_type: entry.entity_type.to_owned(),
                    entity_id: entry.entity_id.to_owned(),
                    seq: entry.seq,
                };
                let mut parents = vec![(table, entry.entity_id.to_owned())];
                while let Some((parent, parent_id)) = parents.pop() {
                    let key = schema.key(parent).unwrap_or_default();
                    let parts: Vec<&str> = if key.len() == 1 {
                        vec![parent_id.as_str()]
                    } else {
                        parent_id.split('/').collect()
                    };
                    for cascade in cascades.iter().filter(|c| c.parent == parent) {
                        if parts.len() != cascade.columns.len() {
                            continue;
                        }
                        let Some(rows) = live.get_mut(cascade.child) else {
                            continue;
                        };
                        let gone: Vec<String> = rows
                            .iter()
                            .filter(|(_, row)| {
                                cascade.columns.iter().zip(&parts).all(|(column, part)| {
                                    row.values.get(column).and_then(Option::as_deref) == Some(*part)
                                })
                            })
                            .map(|(id, _)| id.clone())
                            .collect();
                        for id in gone {
                            if let Some(row) = rows.remove(&id) {
                                removed.push(Cascaded {
                                    table: cascade.child,
                                    entity_id: id.clone(),
                                    created_seq: row.created_seq,
                                    with: with.clone(),
                                });
                                parents.push((cascade.child, id));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    removed
}

/// The entity type whose rows `table` holds.
fn entity_type_of(table: &str) -> &'static str {
    TABLES
        .iter()
        .find(|(_, t)| *t == table)
        .map_or("", |(entity_type, _)| entity_type)
}

/// A value bound to an `INSERT`.
#[derive(Clone, Debug, PartialEq)]
enum Bound {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// `value` as `table.column` stores it, under the encoding its writer gives
/// it in a payload (module documentation).
///
/// # Errors
///
/// Why the value does not fit the column.
fn decode(table: &str, column: &super::Column, value: &Value) -> Result<Bound, String> {
    if value.is_null() {
        return if column.not_null {
            Err("null in a NOT NULL column".to_owned())
        } else {
            Ok(Bound::Null)
        };
    }
    match (column.declared_type.to_ascii_uppercase().as_str(), value) {
        ("INTEGER", Value::Bool(flag)) => Ok(Bound::Int(i64::from(*flag))),
        ("INTEGER", Value::Number(number)) => number
            .as_i64()
            .map(Bound::Int)
            .ok_or_else(|| format!("{number} is not a 64-bit integer")),
        ("REAL", Value::Number(number)) => number
            .as_f64()
            .map(Bound::Real)
            .ok_or_else(|| format!("{number} is not a number")),
        ("TEXT", Value::String(text)) => Ok(Bound::Text(text.clone())),
        ("TEXT", Value::Object(_) | Value::Array(_)) => {
            Ok(Bound::Text(crate::jcs::canonicalize_lenient(value)))
        }
        ("TEXT", Value::Number(number)) if number.is_i64() || number.is_u64() => {
            Ok(Bound::Text(number.to_string()))
        }
        ("BLOB", Value::String(text)) => match blob_encoding(table, &column.name) {
            Some(BlobEncoding::Hex) => hex_decode(text).map(Bound::Blob),
            Some(BlobEncoding::Base64) => STANDARD
                .decode(text)
                .map(Bound::Blob)
                .map_err(|error| format!("not standard base64: {error}")),
            None => Err("no writer puts this BLOB column in a payload".to_owned()),
        },
        (kind, other) => Err(format!("a {kind} column cannot hold {other}")),
    }
}

/// The bytes `text` writes in hex, either case.
fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("hex of odd length".to_owned());
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let digit = |byte: u8| char::from(byte).to_digit(16);
            match (digit(pair[0]), digit(pair[1])) {
                (Some(high), Some(low)) => u8::try_from(high * 16 + low).map_err(|e| e.to_string()),
                _ => Err("not hex".to_owned()),
            }
        })
        .collect()
}

/// The `seq` of the last entry of the row of `entity_type` `entity_id` that
/// wrote `column`, from its `create` at `created_seq` on; `created_seq` when
/// none did.
fn writer_seq(
    entries: &[Entry<'_>],
    entity_type: &str,
    entity_id: &str,
    created_seq: u64,
    column: &str,
) -> u64 {
    entries
        .iter()
        .filter(|entry| {
            entry.seq >= created_seq
                && entry.entity_type == entity_type
                && entry.entity_id == entity_id
                && entry.op != "delete"
        })
        .filter(|entry| {
            serde_json::from_str::<Map<String, Value>>(entry.payload)
                .is_ok_and(|fields| fields.contains_key(column))
        })
        .map(|entry| entry.seq)
        .next_back()
        .unwrap_or(created_seq)
}

/// Inserts every row of `folded` through `conn`, a database with the schema
/// `schema` was read from, in the order of the `create`s the rows start
/// from, decoding each column as its writer encodes it in the payload
/// (module documentation). A column the `create` lacked is left to the
/// column's default; the wrapped repository key is an empty BLOB.
///
/// `entries` are the entries `folded` comes from, for the `seq` an error
/// names. Foreign keys are the caller's: [`build`] defers them, a test may
/// turn them off.
///
/// # Errors
///
/// [`BuildError::Column`] for a value that does not fit its column, or a
/// column the `create` lacked that has neither a default nor NULL, and
/// [`BuildError::Insert`] for a row the database refuses.
pub async fn insert_rows(
    conn: &mut SqliteConnection,
    schema: &Schema,
    folded: &Folded,
    entries: &[Entry<'_>],
) -> Result<(), BuildError> {
    let mut rows: Vec<(u64, &'static str, &String, &super::FoldedRow)> = folded
        .tables
        .iter()
        .flat_map(|(table, rows)| {
            rows.iter()
                .map(|(entity_id, row)| (row.created_seq, *table, entity_id, row))
        })
        .collect();
    rows.sort_by_key(|(created_seq, ..)| *created_seq);
    for (created_seq, table, entity_id, row) in rows {
        let entity_type = entity_type_of(table);
        let column_error = |column: &str, seq: u64, reason: String| BuildError::Column {
            entity_type: entity_type.to_owned(),
            entity_id: entity_id.clone(),
            seq,
            table,
            column: column.to_owned(),
            reason,
        };
        let mut names = Vec::new();
        let mut slots = Vec::new();
        let mut values = Vec::new();
        for column in schema.columns(table).unwrap_or_default() {
            match row.cells.get(&column.name) {
                None => {}
                Some(Cell::Default) => {
                    if column.not_null && column.default.is_none() {
                        return Err(column_error(
                            &column.name,
                            created_seq,
                            "the create lacks it, and the column has no default and is NOT NULL"
                                .to_owned(),
                        ));
                    }
                }
                Some(Cell::EmptyBlob) => {
                    names.push(format!("\"{}\"", column.name));
                    slots.push("X''");
                }
                Some(Cell::Value(value)) => {
                    let bound = decode(table, column, value).map_err(|reason| {
                        let seq =
                            writer_seq(entries, entity_type, entity_id, created_seq, &column.name);
                        column_error(&column.name, seq, reason)
                    })?;
                    names.push(format!("\"{}\"", column.name));
                    slots.push("?");
                    values.push(bound);
                }
            }
        }
        let sql = if names.is_empty() {
            format!("INSERT INTO \"{table}\" DEFAULT VALUES")
        } else {
            format!(
                "INSERT INTO \"{table}\" ({}) VALUES ({})",
                names.join(", "),
                slots.join(", ")
            )
        };
        let mut query = sqlx::query(&sql);
        for value in values {
            query = match value {
                Bound::Null => query.bind(None::<String>),
                Bound::Int(int) => query.bind(int),
                Bound::Real(real) => query.bind(real),
                Bound::Text(text) => query.bind(text),
                Bound::Blob(blob) => query.bind(blob),
            };
        }
        query
            .execute(&mut *conn)
            .await
            .map_err(|error| BuildError::Insert {
                entity_type: entity_type.to_owned(),
                entity_id: entity_id.clone(),
                seq: created_seq,
                table,
                error: error.to_string(),
            })?;
    }
    Ok(())
}

/// Stops with the first row `PRAGMA foreign_key_check` reports, named by
/// its key and the `seq` of its `create`.
async fn check_foreign_keys(
    conn: &mut SqliteConnection,
    schema: &Schema,
    folded: &Folded,
) -> Result<(), BuildError> {
    let violation: Option<(String, Option<i64>, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_optional(&mut *conn)
            .await?;
    let Some((table, rowid, parent, fk_id)) = violation else {
        return Ok(());
    };
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT \"from\" FROM pragma_foreign_key_list(?) WHERE id = ? ORDER BY seq",
    )
    .bind(&table)
    .bind(fk_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut entity_id = None;
    if let (Some(rowid), Some(key)) = (rowid, schema.key(&table)) {
        let select = key
            .iter()
            .map(|column| format!("CAST(\"{column}\" AS TEXT)"))
            .collect::<Vec<_>>()
            .join(", ");
        if let Ok(row) = sqlx::query(&format!("SELECT {select} FROM \"{table}\" WHERE rowid = ?"))
            .bind(rowid)
            .fetch_one(&mut *conn)
            .await
        {
            let parts: Vec<String> = (0..key.len())
                .map(|i| row.try_get::<String, _>(i).unwrap_or_default())
                .collect();
            entity_id = Some(parts.join("/"));
        }
    }
    let seq = entity_id.as_ref().and_then(|id| {
        folded
            .tables
            .iter()
            .find(|(name, _)| **name == table)
            .and_then(|(_, rows)| rows.get(id))
            .map(|row| row.created_seq)
    });
    Err(BuildError::ForeignKey {
        table,
        entity_id,
        seq,
        columns: columns.join(", "),
        parent,
    })
}

#[cfg(test)]
mod tests;
