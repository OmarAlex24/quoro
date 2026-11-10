//! The SQLite store: schema bootstrap, the single-writer gate, time and hash
//! helpers shared by the change log and the audit log.
//!
//! The schema is the frozen contract `docs/contracts/db-schema.sql` plus the
//! numbered migrations in `docs/contracts/migrations/`, compiled into the
//! binary so the database and the contract cannot drift (D-0015).

use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Sqlite, SqliteConnection, SqlitePool, Transaction};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

const BASE_SCHEMA: &str = include_str!("../../../docs/contracts/db-schema.sql");

/// Migrations in order; migration `n` moves `schema_version` from `n` to `n + 1`.
const MIGRATIONS: &[&str] = &[
    include_str!("../../../docs/contracts/migrations/0001-node-fence-failure.sql"),
    include_str!("../../../docs/contracts/migrations/0002-node-credentials.sql"),
    include_str!("../../../docs/contracts/migrations/0003-provider-references.sql"),
    include_str!("../../../docs/contracts/migrations/0004-webhook-test-deliveries.sql"),
    include_str!("../../../docs/contracts/migrations/0005-builds-and-interactive-sessions.sql"),
    include_str!("../../../docs/contracts/migrations/0006-volumes-and-backups.sql"),
    include_str!("../../../docs/contracts/migrations/0007-replicas-edge-and-mesh.sql"),
    include_str!("../../../docs/contracts/migrations/0008-vulnerabilities.sql"),
    include_str!(
        "../../../docs/contracts/migrations/0009-domain-serving-status-and-volume-sweep.sql"
    ),
    include_str!("../../../docs/contracts/migrations/0010-node-lost-at.sql"),
    include_str!("../../../docs/contracts/migrations/0011-warm-copies-and-quarantines.sql"),
    include_str!("../../../docs/contracts/migrations/0012-custody-and-promotion.sql"),
    include_str!("../../../docs/contracts/migrations/0013-lease-unsettled.sql"),
    include_str!("../../../docs/contracts/migrations/0014-audit-params-digest.sql"),
];

/// 64 zeros: `prev_hash` of the first entry of a chain.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The control-plane database.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
    migrations: Arc<[&'static str]>,
    /// The newest change-log `seq`, raised after each commit
    /// ([`Store::change_log_head`]).
    head: Arc<watch::Sender<u64>>,
}

/// A write transaction holding the single-writer gate. Dropping it without
/// [`WriteTx::commit`] rolls back.
pub struct WriteTx {
    pub tx: Transaction<'static, Sqlite>,
    head: Arc<watch::Sender<u64>>,
    _gate: OwnedMutexGuard<()>,
}

impl WriteTx {
    /// Commits, raises the change log's head watch to the newest `seq` this
    /// commit leaves ([`Store::change_log_head`]), and releases the writer
    /// gate.
    ///
    /// The newest `seq` is read inside the transaction, which holds SQLite's
    /// write lock, so it is exactly what the commit leaves; the watch is raised
    /// only once the commit succeeded, and before the gate is released, so
    /// the watch rises in commit order. A failed read is logged and leaves the
    /// watch where it was: the commit itself is not refused, and the next
    /// commit raises the watch past it.
    ///
    /// # Errors
    ///
    /// The database error from `COMMIT`.
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        let head = head_of(&mut self.tx).await;
        self.tx.commit().await?;
        match head {
            Ok(seq) => raise(&self.head, seq),
            Err(error) => {
                tracing::warn!(%error, "cannot read the change log's newest seq after a write");
            }
        }
        Ok(())
    }
}

impl Store {
    /// Opens (creating if needed) the database at `path` and brings the schema
    /// to the newest version.
    ///
    /// # Errors
    ///
    /// Any database error, or a database newer than this binary.
    pub async fn open(path: &Path) -> Result<Self, sqlx::Error> {
        let store = Self::open_unmigrated(path).await?;
        store.migrate().await?;
        Ok(store)
    }

    /// Opens (creating if needed) the database at `path` without applying any
    /// migration: the caller reads [`Store::pending_migrations`], takes the
    /// pre-upgrade backup, and migrates afterwards (`backup-format.md`
    /// section 7.2, "Before an upgrade").
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn open_unmigrated(path: &Path) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        Self::connect(pool).await
    }

    /// An in-memory database for tests: one shared connection, same schema.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn open_in_memory() -> Result<Self, sqlx::Error> {
        let store = Self::open_in_memory_unmigrated().await?;
        store.migrate().await?;
        Ok(store)
    }

    /// [`Store::open_in_memory`] without the migration, for a test that needs a
    /// pending range.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn open_in_memory_unmigrated() -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?.foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await?;
        Self::connect(pool).await
    }

    /// The newest schema version this binary ships, for tests that stand in
    /// the next release's migration.
    #[doc(hidden)]
    #[must_use]
    pub fn shipped_schema_version() -> usize {
        MIGRATIONS.len() + 1
    }

    /// Appends migrations after the ones this binary ships. A test-only seam:
    /// it gives a database a pending range without adding a fake migration to
    /// `docs/contracts/migrations/`.
    #[doc(hidden)]
    #[must_use]
    pub fn with_extra_migrations(self, extra: &'static [&'static str]) -> Self {
        let mut migrations: Vec<&'static str> = self.migrations.to_vec();
        migrations.extend_from_slice(extra);
        Self {
            migrations: migrations.into(),
            ..self
        }
    }

    /// The migrations still to apply as `(from, to)`, or `None` when there are
    /// none or the database is new (`backup-format.md` section 7.2, "Before an
    /// upgrade").
    ///
    /// # Errors
    ///
    /// Any database error, or a stored `schema_version` this binary cannot
    /// read.
    pub async fn pending_migrations(&self) -> Result<Option<(usize, usize)>, sqlx::Error> {
        let has_meta: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'")
                .fetch_optional(&self.pool)
                .await?;
        if has_meta.is_none() {
            return Ok(None);
        }
        let mut conn = self.pool.acquire().await?;
        let version = read_version(&mut conn).await?;
        let newest = self.migrations.len() + 1;
        if version == 0 || version > newest {
            return Err(unsupported_version(version, newest));
        }
        if version == newest {
            return Ok(None);
        }
        Ok(Some((version, newest)))
    }

    /// Wraps `pool`, seeding the change log's head watch from the newest
    /// `seq` it holds (0 for a database without a change log yet).
    async fn connect(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        let has_log: Option<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'change_log'",
        )
        .fetch_optional(&pool)
        .await?;
        let head = if has_log.is_some() {
            head_of(&mut *pool.acquire().await?).await?
        } else {
            0
        };
        Ok(Self {
            pool,
            writer: Arc::new(Mutex::new(())),
            migrations: Arc::from(MIGRATIONS),
            head: Arc::new(watch::channel(head).0),
        })
    }

    /// A watch of the change log's newest `seq`. It is seeded when the store
    /// opens and raised after every [`WriteTx::commit`], and it only ever
    /// grows. Node sessions wait on it to send new entries as they are written
    /// instead of polling the database (`node-agent-protocol.md` section 7.1,
    /// ADR 0227).
    ///
    /// Only writes through [`Store::begin_write`] raise it. Another process
    /// that appends to the same file, such as `quoro vuln import`, is seen at
    /// this process's next commit.
    #[must_use]
    pub fn change_log_head(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }

    /// Applies the pending migrations in one write transaction. A new database
    /// gets the base schema first.
    ///
    /// # Errors
    ///
    /// Any database error, or a database newer than this binary.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut w = self.begin_write().await?;
        let has_meta: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'")
                .fetch_optional(&mut *w.tx)
                .await?;
        if has_meta.is_none() {
            sqlx::raw_sql(BASE_SCHEMA).execute(&mut *w.tx).await?;
        }
        let version = read_version(&mut w.tx).await?;
        let newest = self.migrations.len() + 1;
        if version == 0 || version > newest {
            return Err(unsupported_version(version, newest));
        }
        for migration in &self.migrations[version - 1..] {
            sqlx::raw_sql(migration).execute(&mut *w.tx).await?;
        }
        w.commit().await
    }

    /// The read pool. Reads never take the writer gate.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Starts a write transaction. Writes are serialized through one gate so
    /// that `seq` assignment in the change log and the audit log never races
    /// (D-0015: one writer).
    ///
    /// The transaction starts with `BEGIN IMMEDIATE`, so it takes SQLite's
    /// write lock at once and waits on `busy_timeout` for a writer that does
    /// not go through the gate. A deferred `BEGIN` would take a read snapshot
    /// at its first `SELECT` and could not upgrade it to a write once another
    /// connection committed in between: SQLite answers `SQLITE_BUSY_SNAPSHOT`
    /// at once, without waiting.
    ///
    /// # Errors
    ///
    /// The database error from `BEGIN IMMEDIATE`.
    pub async fn begin_write(&self) -> Result<WriteTx, sqlx::Error> {
        let gate = Arc::clone(&self.writer).lock_owned().await;
        let tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        Ok(WriteTx {
            tx,
            head: Arc::clone(&self.head),
            _gate: gate,
        })
    }
}

/// The change log's newest `seq` through `conn`, 0 when it is empty.
async fn head_of(conn: &mut SqliteConnection) -> Result<u64, sqlx::Error> {
    let seq: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(conn)
        .await?;
    Ok(seq.map_or(0, |seq| u64::try_from(seq).unwrap_or(0)))
}

/// Raises `head` to `seq` when that is higher, notifying its receivers; a
/// lower or equal `seq` changes nothing, so the watch only ever grows.
fn raise(head: &watch::Sender<u64>, seq: u64) {
    head.send_if_modified(|current| {
        if seq > *current {
            *current = seq;
            true
        } else {
            false
        }
    });
}

/// Reads `meta.schema_version` through `conn`.
async fn read_version(conn: &mut SqliteConnection) -> Result<usize, sqlx::Error> {
    let (version,): (String,) =
        sqlx::query_as("SELECT value FROM meta WHERE key = 'schema_version'")
            .fetch_one(conn)
            .await?;
    version
        .parse()
        .map_err(|_| sqlx::Error::Protocol(format!("bad schema_version {version:?}")))
}

/// The error for a database written by a newer binary.
fn unsupported_version(version: usize, newest: usize) -> sqlx::Error {
    sqlx::Error::Protocol(format!(
        "database schema_version {version} is not supported by this binary (max {newest})"
    ))
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

/// Current time as RFC 3339 UTC with milliseconds, `2026-11-02T10:00:00.000Z`.
/// Fixed width, so stored timestamps compare correctly as strings.
#[must_use]
pub fn now_rfc3339() -> String {
    format_rfc3339(time::OffsetDateTime::now_utc())
}

/// `now + seconds` in the same format as [`now_rfc3339`].
#[must_use]
pub fn rfc3339_in(seconds: i64) -> String {
    format_rfc3339(time::OffsetDateTime::now_utc() + time::Duration::seconds(seconds))
}

pub(crate) fn format_rfc3339(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

/// 32 random bytes as lowercase hex, for session and setup secrets.
///
/// # Panics
///
/// Panics if the operating system's random generator fails.
#[must_use]
pub fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("OS random generator");
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn schema_and_migrations_apply_and_reopen() {
        let dir = std::env::temp_dir().join(format!("quoro-store-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("quoro.db");
        let store = Store::open(&path).await.unwrap();
        let (version,): (String,) =
            sqlx::query_as("SELECT value FROM meta WHERE key = 'schema_version'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(version, (MIGRATIONS.len() + 1).to_string());
        drop(store);
        // Reopening applies nothing twice.
        Store::open(&path).await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_new_database_has_nothing_pending_and_an_extra_migration_shows_up() {
        let dir = std::env::temp_dir().join(format!("quoro-store-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("quoro.db");

        // A new database is not pending anything: the first start creates it
        // (`backup-format.md` section 7.2 step 1).
        let store = Store::open_unmigrated(&path).await.unwrap();
        assert_eq!(store.pending_migrations().await.unwrap(), None);
        store.migrate().await.unwrap();
        assert_eq!(store.pending_migrations().await.unwrap(), None);
        drop(store);

        let migration: &'static str = Box::leak(
            format!(
                "CREATE TABLE probe (id INTEGER PRIMARY KEY); \
                 UPDATE meta SET value = '{}' WHERE key = 'schema_version';",
                MIGRATIONS.len() + 2
            )
            .into_boxed_str(),
        );
        let extra: &'static [&'static str] = Box::leak(vec![migration].into_boxed_slice());
        let store = Store::open_unmigrated(&path)
            .await
            .unwrap()
            .with_extra_migrations(extra);
        assert_eq!(
            store.pending_migrations().await.unwrap(),
            Some((MIGRATIONS.len() + 1, MIGRATIONS.len() + 2))
        );
        store.migrate().await.unwrap();
        assert_eq!(store.pending_migrations().await.unwrap(), None);
        let (version,): (String,) =
            sqlx::query_as("SELECT value FROM meta WHERE key = 'schema_version'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(version, (MIGRATIONS.len() + 2).to_string());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Appends one change-log entry in its own write and commits it.
    async fn append_one(store: &Store, entity_id: &str) {
        let mut w = store.begin_write().await.unwrap();
        crate::changelog::append(
            &mut w.tx,
            &crate::changelog::Actor::system("test"),
            "vm",
            entity_id,
            crate::changelog::Op::Create,
            serde_json::json!({}),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();
    }

    #[tokio::test]
    async fn the_head_watch_is_seeded_at_open_and_rises_only_after_a_commit() {
        let dir = std::env::temp_dir().join(format!("quoro-store-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("quoro.db");
        let store = Store::open(&path).await.unwrap();
        let mut head = store.change_log_head();
        let start = *head.borrow_and_update();

        // Raised after the commit, not before.
        let mut w = store.begin_write().await.unwrap();
        crate::changelog::append(
            &mut w.tx,
            &crate::changelog::Actor::system("test"),
            "vm",
            "vm_1",
            crate::changelog::Op::Create,
            serde_json::json!({}),
        )
        .await
        .unwrap();
        assert!(!head.has_changed().unwrap(), "nothing before the commit");
        w.commit().await.unwrap();
        assert!(head.has_changed().unwrap());
        assert_eq!(*head.borrow_and_update(), start + 1);

        // A rolled-back append and a commit that appends nothing leave it.
        let mut w = store.begin_write().await.unwrap();
        crate::changelog::append(
            &mut w.tx,
            &crate::changelog::Actor::system("test"),
            "vm",
            "vm_2",
            crate::changelog::Op::Create,
            serde_json::json!({}),
        )
        .await
        .unwrap();
        drop(w);
        store.begin_write().await.unwrap().commit().await.unwrap();
        assert!(!head.has_changed().unwrap());

        // Every clone shares the watch.
        append_one(&store.clone(), "vm_3").await;
        assert_eq!(*head.borrow_and_update(), start + 2);

        // It never goes down.
        raise(&store.head, 1);
        assert!(!head.has_changed().unwrap());
        assert_eq!(*head.borrow(), start + 2);

        // A reopened store starts at the newest seq.
        drop(store);
        let store = Store::open(&path).await.unwrap();
        assert_eq!(*store.change_log_head().borrow(), start + 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_unmigrated_new_database_starts_the_head_watch_at_zero() {
        let store = Store::open_in_memory_unmigrated().await.unwrap();
        assert_eq!(*store.change_log_head().borrow(), 0);
        store.migrate().await.unwrap();
        append_one(&store, "vm_1").await;
        assert_eq!(*store.change_log_head().borrow(), 1);
    }

    #[test]
    fn timestamps_are_fixed_width() {
        let now = now_rfc3339();
        assert_eq!(now.len(), 24, "{now}");
        assert!(now.ends_with('Z'));
        assert!(rfc3339_in(3600) > now);
    }
}
