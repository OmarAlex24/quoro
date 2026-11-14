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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, watch};

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
    include_str!("../../../docs/contracts/migrations/0015-node-mesh-rules.sql"),
];

/// 64 zeros: `prev_hash` of the first entry of a chain.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The term ceiling, 2^53 - 1: no machine sends, stores or adopts a higher
/// term (`node-agent-protocol.md` section 8.8).
pub(crate) const MAX_TERM: i64 = (1 << 53) - 1;

/// Why [`read_term`] gives no term.
#[derive(Debug)]
pub(crate) enum TermError {
    /// The database could not be read.
    Database(sqlx::Error),
    /// `meta.term` holds no term: its text is not a plain decimal integer
    /// from 1 to [`MAX_TERM`]. It holds the stored text.
    Bad(String),
}

impl From<sqlx::Error> for TermError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl std::fmt::Display for TermError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(f, "cannot read meta.term: {error}"),
            Self::Bad(value) => {
                write!(f, "meta.term {value:?} is not a term from 1 to {MAX_TERM}")
            }
        }
    }
}

impl std::error::Error for TermError {}

/// The cluster's current term, `meta.term`, read through `executor`. Every
/// reader of the term goes through here, so all of them read the same text
/// the same way: the change log, the `welcome`, `desired_state` and
/// `support_request` a session sends, the cluster API and a demotion.
///
/// A missing `meta.term` reads as 1, the term before the first promotion
/// (`db-schema.sql`, `meta`).
///
/// # Errors
///
/// [`TermError::Database`] for a database error, and [`TermError::Bad`] for
/// a stored value that is not a plain decimal integer from 1 to
/// [`MAX_TERM`]: a sign, a leading zero, a space, a fraction or a value
/// above the ceiling makes it bad (section 8.8).
pub(crate) async fn read_term<'e, E>(executor: E) -> Result<i64, TermError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let stored: Option<String> = sqlx::query_scalar("SELECT value FROM meta WHERE key = 'term'")
        .fetch_optional(executor)
        .await?;
    match stored {
        None => Ok(1),
        Some(value) => parse_term(&value).ok_or(TermError::Bad(value)),
    }
}

/// `value` as a term: a plain decimal integer from 1 to [`MAX_TERM`], digits
/// only, with no leading zero.
fn parse_term(value: &str) -> Option<i64> {
    if value.starts_with('0') || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value
        .parse::<i64>()
        .ok()
        .filter(|term| (1..=MAX_TERM).contains(term))
}

/// The control-plane database.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
    migrations: Arc<[&'static str]>,
    /// The newest change-log `seq`, raised after each commit
    /// ([`Store::change_log_head`]).
    head: Arc<watch::Sender<u64>>,
    /// Whether this primary is demoted (`node-agent-protocol.md` section
    /// 7.4, ADR 0230). Set once, by [`Store::demote`], before it takes the
    /// writer gate and before the `meta` write, and never cleared: from then
    /// on [`Store::begin_write`] and [`WriteTx::commit`] refuse. Readable
    /// without the database.
    demoted: Arc<watch::Sender<bool>>,
    /// The `meta.demoted_by_term` record of the demotion, and its retry
    /// until it is recorded ([`Store::demote`]).
    demotion: Arc<DemotionRecord>,
    /// What [`Store::stops_for_demotion`] stopped, by name: a loop's own stop
    /// signal ([`Store::stopped_for_demotion`]).
    stopped: Arc<watch::Sender<Vec<String>>>,
    /// Test seam: the next demotion waits here, with the flag set and the
    /// gate held, before its `meta` write ([`Store::hold_next_demotion`]).
    demotion_hold: Arc<std::sync::Mutex<Option<Arc<DemotionHold>>>>,
    /// Whether this process is a primary restored with `quoro restore
    /// --accept-loss` that has sent no `lease_grant` yet, so a higher term
    /// starts it again instead of demoting it ([`Store::restarts_on_a_higher_term`]).
    restarts: Arc<AtomicBool>,
}

/// What [`Store::restart_term`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermRestart {
    /// `meta.term` went from `from` to `to`.
    Raised { from: i64, to: i64 },
    /// `meta.term` was already at or above the new term, and stays.
    Kept(i64),
}

/// The first wait of the retry of an unrecorded demotion; it doubles after
/// each failed attempt, up to [`DEMOTION_RETRY_MAX`].
pub const DEMOTION_RETRY_FIRST: Duration = Duration::from_secs(5);

/// The longest wait between two retries of an unrecorded demotion.
pub const DEMOTION_RETRY_MAX: Duration = Duration::from_secs(60);

/// How far the demotion got with `meta.demoted_by_term`. The demoted flag
/// is set before any of it: [`Store::demote`] sets the flag first and keeps
/// `node_term`, and only then does its task take the writer gate to write
/// `meta`. The rest is read and written with the gate held, but the retry
/// task reads `recorded` to stop.
struct DemotionRecord {
    /// Whether `meta.demoted_by_term` is recorded.
    recorded: AtomicBool,
    /// The first `node_term` a `stale_term` carried, kept before the gate is
    /// taken. Every attempt, a later `stale_term`'s, the retry task's and the
    /// one at shutdown, passes this one to [`demotion_term`], but only while
    /// no attempt has chosen a term yet: a chosen `term` (below) beats it.
    /// So an attempt that failed before it chose a term, on a busy
    /// `BEGIN IMMEDIATE`, still leaves the node's term to the next one. A
    /// `stale_term` without a term keeps nothing, so a later one with a term
    /// is kept.
    node_term: std::sync::Mutex<Option<u64>>,
    /// The term the first attempt that could compute one chose. Every later
    /// attempt, the retry task's, a later `stale_term`'s and the one at
    /// shutdown, records this one, whatever `node_term` it passes, so no two
    /// attempts record different terms.
    term: std::sync::Mutex<Option<u64>>,
    /// Whether the retry task was started; one is enough.
    retrying: AtomicBool,
    /// The retry task's first wait, in milliseconds
    /// ([`Store::set_demotion_retry_for_test`]).
    first_retry_ms: AtomicU64,
}

impl Default for DemotionRecord {
    fn default() -> Self {
        Self {
            recorded: AtomicBool::new(false),
            node_term: std::sync::Mutex::new(None),
            term: std::sync::Mutex::new(None),
            retrying: AtomicBool::new(false),
            first_retry_ms: AtomicU64::new(
                u64::try_from(DEMOTION_RETRY_FIRST.as_millis()).unwrap_or(u64::MAX),
            ),
        }
    }
}

/// The error [`Store::begin_write`] and [`WriteTx::commit`] return once the
/// primary is demoted.
pub const DEMOTED_REFUSAL: &str =
    "this primary is demoted: a higher term exists, and it writes nothing more";

/// What the `meta` write of [`Store::demote`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Demotion {
    /// `meta.demoted_by_term` holds this term, so a restart stays demoted.
    Recorded(u64),
    /// The `meta` write failed: this process is demoted, a restart is not.
    NotRecorded,
}

/// The `meta` write of a demotion, on its own task ([`Store::demote`]).
/// Awaiting it gives what the write did: `None` when the demotion was already
/// recorded. Dropping it leaves the write running.
pub struct DemotionTask(tokio::task::JoinHandle<Option<Demotion>>);

impl DemotionTask {
    /// Runs `write` on its own task.
    pub(crate) fn spawn(
        write: impl std::future::Future<Output = Option<Demotion>> + Send + 'static,
    ) -> Self {
        Self(tokio::spawn(write))
    }
}

impl std::future::Future for DemotionTask {
    type Output = Option<Demotion>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx).map(|joined| {
            joined.unwrap_or_else(|error| {
                if error.is_panic() {
                    std::panic::resume_unwind(error.into_panic());
                }
                // Cancelled with the runtime: nothing more to report.
                None
            })
        })
    }
}

/// A test seam that holds the next demotion with the demoted flag set and the
/// writer gate held, before its `meta` write.
#[doc(hidden)]
#[derive(Default)]
pub struct DemotionHold {
    reached: Notify,
    release: Notify,
}

impl DemotionHold {
    /// Waits until the held demotion has set the flag and holds the gate.
    pub async fn reached(&self) {
        self.reached.notified().await;
    }

    /// Lets the held demotion go on to write `meta` and release the gate.
    pub fn release(&self) {
        self.release.notify_one();
    }
}

/// A write transaction holding the single-writer gate. Dropping it without
/// [`WriteTx::commit`] rolls back.
pub struct WriteTx {
    pub tx: Transaction<'static, Sqlite>,
    head: Arc<watch::Sender<u64>>,
    demoted: Arc<watch::Sender<bool>>,
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
    /// Once the primary is demoted it rolls back instead, and the watch stays
    /// where it was: a transaction opened before the demotion commits
    /// nothing after it (section 7.4). The flag is read right before
    /// `COMMIT`, so only a commit already under way when it is set lands.
    ///
    /// # Errors
    ///
    /// [`DEMOTED_REFUSAL`] once the primary is demoted, or the database error
    /// from `COMMIT`.
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        let head = head_of(&mut self.tx).await;
        if *self.demoted.borrow() {
            if let Err(error) = self.tx.rollback().await {
                tracing::warn!(%error, "cannot roll back a write refused for the demotion");
            }
            return Err(sqlx::Error::Protocol(DEMOTED_REFUSAL.to_string()));
        }
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
            demoted: Arc::new(watch::channel(false).0),
            demotion: Arc::new(DemotionRecord::default()),
            stopped: Arc::new(watch::channel(Vec::new()).0),
            demotion_hold: Arc::new(std::sync::Mutex::new(None)),
            restarts: Arc::new(AtomicBool::new(false)),
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

    /// The database's `meta.schema_version`, which `welcome` carries to every
    /// node (`node-agent-protocol.md` section 5.10, ADR 0229).
    ///
    /// # Errors
    ///
    /// Any database error, or a stored value that is not a number.
    pub async fn schema_version(&self) -> Result<usize, sqlx::Error> {
        read_version(&mut *self.pool.acquire().await?).await
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
    /// Once the primary is demoted it refuses and opens no transaction
    /// (section 7.4: a demoted primary writes nothing more). A writer waiting
    /// on the gate stops waiting the moment the flag is set, and is refused
    /// without the gate, so no session or loop waits on the gate after the
    /// demotion. One that holds the gate then is refused at
    /// [`WriteTx::commit`].
    ///
    /// # Errors
    ///
    /// [`DEMOTED_REFUSAL`] once the primary is demoted, or the database error
    /// from `BEGIN IMMEDIATE`.
    pub async fn begin_write(&self) -> Result<WriteTx, sqlx::Error> {
        let mut demoted = self.demoted.subscribe();
        let gate = tokio::select! {
            biased;
            gate = Arc::clone(&self.writer).lock_owned() => gate,
            () = async {
                // The sender lives as long as the store: a closed watch
                // never refuses.
                if demoted.wait_for(|demoted| *demoted).await.is_err() {
                    std::future::pending::<()>().await;
                }
            } => return Err(sqlx::Error::Protocol(DEMOTED_REFUSAL.to_string())),
        };
        if self.is_demoted() {
            return Err(sqlx::Error::Protocol(DEMOTED_REFUSAL.to_string()));
        }
        let tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        Ok(WriteTx {
            tx,
            head: Arc::clone(&self.head),
            demoted: Arc::clone(&self.demoted),
            _gate: gate,
        })
    }

    /// Whether this primary is demoted (`node-agent-protocol.md` section 7.4).
    /// Reads memory only.
    #[must_use]
    pub fn is_demoted(&self) -> bool {
        *self.demoted.borrow()
    }

    /// A watch of the demoted flag: it turns `true` once and stays so. Node
    /// sessions wait on it to end with `not_primary`.
    #[must_use]
    pub fn demoted_watch(&self) -> watch::Receiver<bool> {
        self.demoted.subscribe()
    }

    /// Whether a background loop must stop because the primary is demoted,
    /// logging one line naming `what` when it must. A loop calls it once per
    /// pass and stops on `true`, so it logs at most once. Work that calls
    /// outside (a provider, a bucket) also calls it right before each call,
    /// since the demotion may land while that work runs.
    #[must_use]
    pub fn stops_for_demotion(&self, what: &str) -> bool {
        if self.is_demoted() {
            tracing::info!("{what} stops: this primary is demoted");
            self.stopped.send_if_modified(|stopped| {
                if stopped.iter().any(|name| name == what) {
                    return false;
                }
                stopped.push(what.to_string());
                true
            });
            return true;
        }
        false
    }

    /// Waits until [`Store::stops_for_demotion`] stopped `what`: for a loop,
    /// until its pass after the demotion saw the flag, so it sends and writes
    /// nothing more.
    pub async fn stopped_for_demotion(&self, what: &str) {
        let mut stopped = self.stopped.subscribe();
        let _ = stopped
            .wait_for(|stopped| stopped.iter().any(|name| name == what))
            .await;
    }

    /// `meta.demoted_by_term`, read through the pool: `None` when the key is
    /// absent or the database has no `meta` table yet. Nothing is written.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn demoted_by_term(&self) -> Result<Option<String>, sqlx::Error> {
        let has_meta: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'")
                .fetch_optional(&self.pool)
                .await?;
        if has_meta.is_none() {
            return Ok(None);
        }
        sqlx::query_scalar("SELECT value FROM meta WHERE key = 'demoted_by_term'")
            .fetch_optional(&self.pool)
            .await
    }

    /// Demotes this primary: a machine answered it with `stale_term`, so a
    /// higher term exists (`node-agent-protocol.md` sections 7.4 and 8.8, ADR
    /// 0230).
    ///
    /// It sets the demoted flag and keeps the first `node_term` it is given
    /// ([`DemotionRecord`]) at once, without waiting for anything, so from
    /// the moment the `stale_term` arrives nothing more commits and every
    /// session sends only `not_primary`: a writer waiting on the gate is
    /// refused at [`Store::begin_write`], and one holding it at
    /// [`WriteTx::commit`]. The rest runs on its own task, which it returns,
    /// so the caller, a node session among them, waits for neither the gate
    /// nor SQLite. That task takes the writer gate and, in one transaction,
    /// records `meta.demoted_by_term`: the term an earlier attempt chose,
    /// else the kept `node_term` when it is usable, else `meta.term + 1`
    /// ([`demotion_term`]). Awaited, it gives `None` when the demotion was
    /// already recorded: the first recorded term stays. The contract
    /// gives no rule for a `stale_term` without a usable term; that case is
    /// logged at `error` and the primary is demoted all the same, as section
    /// 8.8 sets no condition. Until the `meta` write commits the process is
    /// demoted but not recorded, the same state as a failed `meta` write.
    ///
    /// A failed `meta` write still sets the flag: this process is demoted,
    /// though a restart would not be. A task then retries the write under the
    /// gate, after [`DEMOTION_RETRY_FIRST`] and doubling up to
    /// [`DEMOTION_RETRY_MAX`], until it is recorded: no later `stale_term` is
    /// to be expected, as every session ends and the guard answers `503`. A
    /// later `stale_term` tries as well. Both record the term the first
    /// attempt chose, and only if no term is recorded yet. That write is the
    /// one write a demoted primary makes: it goes straight to the pool, past
    /// the refusal of [`Store::begin_write`].
    ///
    /// Section 8.8 excepts a primary restored with `quoro restore
    /// --accept-loss` that has sent no `lease_grant` yet, which starts again
    /// above the error's term instead (`backup-format.md` section 8.2, ADR
    /// 0261, [`Store::restart_term`]). The caller decides that before it
    /// calls this; once called, the primary demotes whatever it was.
    pub fn demote(&self, node_term: Option<u64>) -> DemotionTask {
        self.demoted.send_replace(true);
        self.keep_node_term(node_term);
        let store = self.clone();
        DemotionTask::spawn(async move { store.record_demotion_under_gate().await })
    }

    /// The part of [`Store::demote`] that waits: the `meta` write under the
    /// writer gate, and the retry task when it fails.
    async fn record_demotion_under_gate(&self) -> Option<Demotion> {
        let gate = Arc::clone(&self.writer).lock_owned().await;
        if self.demotion.recorded.load(Ordering::SeqCst) {
            return None;
        }
        let hold = self
            .demotion_hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hold) = hold {
            hold.reached.notify_one();
            hold.release.notified().await;
        }
        let demotion = match self.record_demotion(self.kept_node_term()).await {
            Ok(term) => Demotion::Recorded(term),
            Err(error) => {
                tracing::error!(
                    %error,
                    node_term = ?self.kept_node_term(),
                    "cannot record meta.demoted_by_term: this process is demoted, but a restart would not stay demoted until a retry records it"
                );
                Demotion::NotRecorded
            }
        };
        drop(gate);
        if demotion == Demotion::NotRecorded {
            self.retry_demotion();
        }
        Some(demotion)
    }

    /// Keeps `node_term` for every attempt at the `meta` write, unless an
    /// earlier `stale_term` already gave one ([`DemotionRecord`]).
    fn keep_node_term(&self, node_term: Option<u64>) {
        let mut kept = self
            .demotion
            .node_term
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if kept.is_none() {
            *kept = node_term;
        }
    }

    /// The `node_term` kept by [`Store::keep_node_term`].
    fn kept_node_term(&self) -> Option<u64> {
        *self
            .demotion
            .node_term
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records `meta.demoted_by_term` with the writer gate held, and marks it
    /// recorded. The term is the one an earlier attempt chose, or else the one
    /// [`demotion_term`] gives for `node_term`, kept for later attempts. An
    /// existing row is kept: a commit that reported an error but landed keeps
    /// its term. Returns the term the row holds.
    async fn record_demotion(&self, node_term: Option<u64>) -> Result<u64, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let chosen = *self
            .demotion
            .term
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let term = match chosen {
            Some(term) => term,
            None => {
                let term = demotion_term(&mut tx, node_term).await?;
                *self
                    .demotion
                    .term
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(term);
                term
            }
        };
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ('demoted_by_term', ?) \
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(term.to_string())
        .execute(&mut *tx)
        .await?;
        let recorded: String =
            sqlx::query_scalar("SELECT value FROM meta WHERE key = 'demoted_by_term'")
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        self.demotion.recorded.store(true, Ordering::SeqCst);
        Ok(recorded.parse().unwrap_or(term))
    }

    /// Starts the task that retries an unrecorded demotion until it is
    /// recorded ([`Store::demote`]); a second call starts nothing.
    fn retry_demotion(&self) {
        if self.demotion.retrying.swap(true, Ordering::SeqCst) {
            return;
        }
        let store = self.clone();
        tokio::spawn(async move {
            let mut wait =
                Duration::from_millis(store.demotion.first_retry_ms.load(Ordering::SeqCst));
            loop {
                tokio::time::sleep(wait).await;
                let gate = Arc::clone(&store.writer).lock_owned().await;
                if store.demotion.recorded.load(Ordering::SeqCst) {
                    return;
                }
                match store.record_demotion(store.kept_node_term()).await {
                    Ok(term) => {
                        tracing::info!(
                            demoted_by_term = term,
                            "meta.demoted_by_term is recorded: a restart stays demoted"
                        );
                        return;
                    }
                    Err(error) => tracing::error!(
                        %error,
                        retry_in = ?wait.saturating_mul(2).min(DEMOTION_RETRY_MAX),
                        "cannot record meta.demoted_by_term yet; a restart would not stay demoted"
                    ),
                }
                drop(gate);
                wait = wait.saturating_mul(2).min(DEMOTION_RETRY_MAX);
            }
        });
    }

    /// One last attempt at an unrecorded demotion, for a clean shutdown: the
    /// retry task stops with the process. Does nothing when the primary is not
    /// demoted or its demotion is recorded. It records the term an earlier
    /// attempt chose, if one did; otherwise it passes the `node_term` the
    /// first `stale_term` carried, so a first attempt that failed before it
    /// chose a term still leads to the node's term, not `meta.term + 1`.
    pub async fn record_demotion_before_exit(&self) {
        if !self.is_demoted() {
            return;
        }
        let _gate = Arc::clone(&self.writer).lock_owned().await;
        if self.demotion.recorded.load(Ordering::SeqCst) {
            return;
        }
        match self.record_demotion(self.kept_node_term()).await {
            Ok(term) => tracing::info!(
                demoted_by_term = term,
                "meta.demoted_by_term is recorded at shutdown: a restart stays demoted"
            ),
            Err(error) => tracing::error!(
                %error,
                "cannot record meta.demoted_by_term at shutdown: a restart would not stay demoted"
            ),
        }
    }

    /// Test seam: the first wait of the retry of an unrecorded demotion,
    /// instead of [`DEMOTION_RETRY_FIRST`].
    #[doc(hidden)]
    pub fn set_demotion_retry_for_test(&self, first: Duration) {
        self.demotion.first_retry_ms.store(
            u64::try_from(first.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    /// Test seam: holds the next [`Store::demote`] with the flag set and the
    /// writer gate held, before its `meta` write.
    #[doc(hidden)]
    #[must_use]
    pub fn hold_next_demotion(&self) -> Arc<DemotionHold> {
        let hold = Arc::new(DemotionHold::default());
        *self
            .demotion_hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&hold));
        hold
    }

    /// Whether a higher term starts this primary again instead of demoting
    /// it: it was restored with `quoro restore --accept-loss` and has sent no
    /// `lease_grant` yet (`node-agent-protocol.md` sections 7.4 and 8.8,
    /// `backup-format.md` section 8.2, ADR 0261, ADR 0280). Reads memory
    /// only.
    ///
    /// [`crate::restore_pending::apply`] turns it on in the process that
    /// makes the first write after such a restore, before the primary serves,
    /// and the first `lease_grant` a session sends turns it off for good
    /// ([`Store::lease_grant_going_out`]). It lives in memory only: a
    /// process started again, also one that finds the marker of a first write
    /// already committed, finds it off, and demotes on a higher term as any
    /// primary does, the safe side of the rule.
    #[must_use]
    pub fn restarts_on_a_higher_term(&self) -> bool {
        self.restarts.load(Ordering::SeqCst)
    }

    /// Turns [`Store::restarts_on_a_higher_term`] on, for a primary restored
    /// with `--accept-loss` that has not served yet.
    pub(crate) fn allow_term_restarts(&self) {
        self.restarts.store(true, Ordering::SeqCst);
    }

    /// A `lease_grant` is about to go out: from then on a higher term demotes
    /// this primary, as any other (section 7.4). Called before the frame is
    /// sent, so a grant never leaves while the exception still holds. Returns
    /// whether this call ended the exception.
    pub(crate) fn lease_grant_going_out(&self) -> bool {
        self.restarts.swap(false, Ordering::SeqCst)
    }

    /// Starts this primary again at one above `error_term`, the `term` of a
    /// `stale_term`, or of a voter's `bad_message` that answers a
    /// `support_request` (`node-agent-protocol.md` sections 5.18, 5.27 and
    /// 8.8, `backup-format.md` section 8.2): one write, through the writer
    /// gate, that sets `meta.term`. Every later `welcome`, `desired_state`,
    /// `support_request` and change-log entry carries it. `meta.term` is
    /// never lowered: when it already reads that term or a higher one,
    /// nothing is written. The caller checks
    /// [`Store::restarts_on_a_higher_term`] first, when the error arrives.
    ///
    /// # Errors
    ///
    /// A one-line reason, with nothing written: a new term above the term
    /// ceiling, 2^53 - 1 (section 8.8), which the caller checks before
    /// already; a demoted primary ([`DEMOTED_REFUSAL`]); a `meta.term` that
    /// holds no term ([`read_term`]); or a database error. The caller then
    /// demotes the primary.
    pub(crate) async fn restart_term(&self, error_term: u64) -> Result<TermRestart, String> {
        // The caller decides a restart past the ceiling when the error
        // arrives, and demotes instead (`NodeHub::start_again_above`); the
        // refusal below is the backstop of a release build.
        debug_assert!(
            i64::try_from(error_term).is_ok_and(|term| term < MAX_TERM),
            "a restart above term {error_term} passes the term ceiling; the caller demotes instead"
        );
        let to = i64::try_from(error_term)
            .ok()
            .and_then(|term| term.checked_add(1))
            .filter(|term| *term <= MAX_TERM)
            .ok_or_else(|| {
                format!("one above term {error_term} is above the term ceiling, {MAX_TERM}")
            })?;
        let mut w = self
            .begin_write()
            .await
            .map_err(|error| format!("cannot begin the write of the new term: {error}"))?;
        let from = read_term(&mut *w.tx)
            .await
            .map_err(|error| error.to_string())?;
        if to <= from {
            return Ok(TermRestart::Kept(from));
        }
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ('term', ?) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind(to.to_string())
        .execute(&mut *w.tx)
        .await
        .map_err(|error| format!("cannot write meta.term: {error}"))?;
        w.commit()
            .await
            .map_err(|error| format!("cannot commit meta.term: {error}"))?;
        Ok(TermRestart::Raised { from, to })
    }
}

/// The term a demotion records, read through `tx`: `node_term` when it is
/// present and above `meta.term`, or present while `meta.term` holds no term
/// ([`read_term`]). Otherwise `meta.term + 1` (a missing `meta.term` reads as
/// 1), logged at `error` because the `stale_term` carried no usable term.
/// With neither usable there is no term, and nothing is recorded.
///
/// No term above [`MAX_TERM`] is recorded (section 8.8 "The term ceiling"):
/// a `node_term` above it is not usable (a session never passes one: such an
/// `error` is malformed and demotes nothing), and `meta.term + 1` stops at
/// it.
async fn demotion_term(
    tx: &mut Transaction<'_, Sqlite>,
    node_term: Option<u64>,
) -> Result<u64, sqlx::Error> {
    let (current, bad) = match read_term(&mut **tx).await {
        Ok(term) => (Some(term.unsigned_abs()), None),
        Err(TermError::Database(error)) => return Err(error),
        Err(TermError::Bad(value)) => (None, Some(value)),
    };
    if let Some(term) = node_term.filter(|term| *term > quoro_proto::wire::MAX_TERM) {
        tracing::error!(
            node_term = term,
            max_term = quoro_proto::wire::MAX_TERM,
            "the stale_term's term is above the term ceiling; it is not recorded"
        );
    }
    let node_term = node_term.filter(|term| *term <= quoro_proto::wire::MAX_TERM);
    match (node_term, current) {
        (Some(term), Some(current)) if term > current => Ok(term),
        (Some(term), None) => {
            tracing::error!(
                node_term = term,
                meta_term = ?bad,
                "meta.term holds no term; recording the stale_term's term"
            );
            Ok(term)
        }
        (_, Some(current)) => {
            tracing::error!(
                ?node_term,
                meta_term = current,
                "the stale_term carried no term above this primary's; recording meta.term + 1"
            );
            Ok(current.saturating_add(1).min(quoro_proto::wire::MAX_TERM))
        }
        (None, None) => Err(sqlx::Error::Protocol(format!(
            "the stale_term carried no term and meta.term {:?} holds no term: no term to record",
            bad.unwrap_or_default()
        ))),
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

    /// Section 8.8: `meta.term` reads as a plain decimal integer from 1 to
    /// 2^53 - 1, and a missing one as 1. Any other text is no term.
    #[tokio::test]
    async fn meta_term_reads_only_as_a_plain_decimal_term() {
        let store = Store::open_in_memory().await.unwrap();
        let read = || async { read_term(store.pool()).await };
        assert_eq!(read().await.unwrap(), 1, "a missing meta.term");
        for (value, term) in [
            ("1", 1),
            ("7", 7),
            ("10", 10),
            ("9007199254740991", MAX_TERM),
        ] {
            sqlx::query(
                "INSERT INTO meta (key, value) VALUES ('term', ?) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            )
            .bind(value)
            .execute(store.pool())
            .await
            .unwrap();
            assert_eq!(read().await.unwrap(), term, "{value:?}");
        }
        for value in [
            "0",
            "-1",
            "+5",
            "007",
            "05",
            "",
            " 5",
            "5 ",
            "1.5",
            "1e3",
            "seven",
            "9007199254740992",
            "18446744073709551616",
        ] {
            sqlx::query("UPDATE meta SET value = ? WHERE key = 'term'")
                .bind(value)
                .execute(store.pool())
                .await
                .unwrap();
            let refused = read().await;
            assert!(
                matches!(&refused, Err(TermError::Bad(stored)) if stored == value),
                "{value:?}: {refused:?}"
            );
        }
    }

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

    /// Migration 0015: a new database opens at schema 16 with
    /// `nodes.mesh_rules`, and a database at schema 15 migrates to it,
    /// keeping its rows, which read `NULL` there. The column takes no value
    /// below 1.
    #[tokio::test]
    async fn the_store_opens_at_16_and_a_database_at_15_migrates() {
        async fn version(store: &Store) -> String {
            sqlx::query_scalar("SELECT value FROM meta WHERE key = 'schema_version'")
                .fetch_one(store.pool())
                .await
                .unwrap()
        }
        async fn has_mesh_rules(store: &Store) -> bool {
            let columns: Vec<String> =
                sqlx::query_scalar("SELECT name FROM pragma_table_info('nodes')")
                    .fetch_all(store.pool())
                    .await
                    .unwrap();
            columns.iter().any(|column| column == "mesh_rules")
        }
        let dir = std::env::temp_dir().join(format!("quoro-store-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();

        let store = Store::open(&dir.join("new.db")).await.unwrap();
        assert_eq!(Store::shipped_schema_version(), 16);
        assert_eq!(version(&store).await, "16");
        assert!(has_mesh_rules(&store).await);
        drop(store);

        // A database the release before this one left: schema 15.
        let path = dir.join("old.db");
        let mut old = Store::open_unmigrated(&path).await.unwrap();
        old.migrations = Arc::from(&MIGRATIONS[..14]);
        old.migrate().await.unwrap();
        assert_eq!(version(&old).await, "15");
        assert!(!has_mesh_rules(&old).await);
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, status, public_key, behind_nat, labels, \
             capacity, allocated, kvm, created_at) VALUES ('nod_1', 'node-1', '[\"node\"]', \
             'connected', 'key', 0, '{}', '{}', '{}', '{}', '2026-11-10T10:00:00.000Z')",
        )
        .execute(old.pool())
        .await
        .unwrap();
        drop(old);

        let store = Store::open(&path).await.unwrap();
        assert_eq!(version(&store).await, "16");
        let rules: Option<i64> =
            sqlx::query_scalar("SELECT mesh_rules FROM nodes WHERE id = 'nod_1'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(rules, None);
        for (value, accepted) in [(2, true), (1, true), (0, false)] {
            let result = sqlx::query("UPDATE nodes SET mesh_rules = ? WHERE id = 'nod_1'")
                .bind(value)
                .execute(store.pool())
                .await;
            assert_eq!(result.is_ok(), accepted, "{value}");
        }
        drop(store);
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
