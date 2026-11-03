//! The SQLite store: schema bootstrap, the single-writer gate, time and hash
//! helpers shared by the change log and the audit log.
//!
//! The schema is the frozen contract `docs/contracts/db-schema.sql` plus the
//! numbered migrations in `docs/contracts/migrations/`, compiled into the
//! binary so the database and the contract cannot drift (D-0015).

use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard};

const BASE_SCHEMA: &str = include_str!("../../../docs/contracts/db-schema.sql");

/// Migrations in order; migration `n` moves `schema_version` from `n` to `n + 1`.
const MIGRATIONS: &[&str] = &[
    include_str!("../../../docs/contracts/migrations/0001-node-fence-failure.sql"),
    include_str!("../../../docs/contracts/migrations/0002-node-credentials.sql"),
    include_str!("../../../docs/contracts/migrations/0003-provider-references.sql"),
    include_str!("../../../docs/contracts/migrations/0004-webhook-test-deliveries.sql"),
];

/// 64 zeros: `prev_hash` of the first entry of a chain.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The control-plane database.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    writer: Arc<Mutex<()>>,
}

/// A write transaction holding the single-writer gate. Dropping it without
/// [`WriteTx::commit`] rolls back.
pub struct WriteTx {
    pub tx: Transaction<'static, Sqlite>,
    _gate: OwnedMutexGuard<()>,
}

impl WriteTx {
    /// Commits and releases the writer gate.
    ///
    /// # Errors
    ///
    /// The database error from `COMMIT`.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.tx.commit().await
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
        Self::init(pool).await
    }

    /// An in-memory database for tests: one shared connection, same schema.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn open_in_memory() -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?.foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await?;
        Self::init(pool).await
    }

    async fn init(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        let store = Self {
            pool,
            writer: Arc::new(Mutex::new(())),
        };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut w = self.begin_write().await?;
        let has_meta: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'")
                .fetch_optional(&mut *w.tx)
                .await?;
        if has_meta.is_none() {
            sqlx::raw_sql(BASE_SCHEMA).execute(&mut *w.tx).await?;
        }
        let (version,): (String,) =
            sqlx::query_as("SELECT value FROM meta WHERE key = 'schema_version'")
                .fetch_one(&mut *w.tx)
                .await?;
        let version: usize = version
            .parse()
            .map_err(|_| sqlx::Error::Protocol(format!("bad schema_version {version:?}")))?;
        if version == 0 || version > MIGRATIONS.len() + 1 {
            return Err(sqlx::Error::Protocol(format!(
                "database schema_version {version} is not supported by this binary (max {})",
                MIGRATIONS.len() + 1
            )));
        }
        for migration in &MIGRATIONS[version - 1..] {
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
    /// # Errors
    ///
    /// The database error from `BEGIN`.
    pub async fn begin_write(&self) -> Result<WriteTx, sqlx::Error> {
        let gate = Arc::clone(&self.writer).lock_owned().await;
        let tx = self.pool.begin().await?;
        Ok(WriteTx { tx, _gate: gate })
    }
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

fn format_rfc3339(t: time::OffsetDateTime) -> String {
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

    #[test]
    fn timestamps_are_fixed_width() {
        let now = now_rfc3339();
        assert_eq!(now.len(), 24, "{now}");
        assert!(now.ends_with('Z'));
        assert!(rfc3339_in(3600) > now);
    }
}
