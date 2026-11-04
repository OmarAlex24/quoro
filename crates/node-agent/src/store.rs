// SPDX-License-Identifier: AGPL-3.0-only
//! The local node store (`node-agent-protocol.md` section 12).
//!
//! One SQLite file, `<data_dir>/node.db`, owned by root, holding the verified
//! desired state, what was applied per VM, the buffered events and a small
//! metadata table. Nothing here is replicated; the tables are local and may
//! evolve.
//!
//! The whole point of the store is that the node keeps running without the
//! control plane (D-0011): the newest verified snapshot is the source of truth
//! after a restart, and the signature is verified again before it is used, so a
//! tampered store cannot drive the engine.

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use sqlx::Row as _;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::broadcast;

use crate::AgentError;
use crate::localkey::LocalKey;
use crate::logs::CapturedLine;
use crate::util;

/// How many accepted revisions are kept; older ones are pruned after each
/// insert. A node only ever acts on the newest, but keeping a few makes an
/// incident inspectable.
const KEPT_REVISIONS: i64 = 5;

/// The event buffer is bounded at 10 000 entries (D-0011, section 4.3).
pub const EVENT_BUFFER_LIMIT: i64 = 10_000;

/// `key` of `node_meta` holding the newest fully applied revision.
const APPLIED_REV: &str = "applied_rev";
/// `key` of `node_meta` holding the count of events dropped from the buffer.
const DROPPED_EVENTS: &str = "dropped_events";

/// The schema. Every statement is `IF NOT EXISTS`, so opening an existing file
/// is idempotent.
const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS node_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS desired_state (
    rev        INTEGER PRIMARY KEY,
    body       TEXT NOT NULL,
    body_enc   BLOB,
    issued_at  TEXT NOT NULL,
    term       INTEGER NOT NULL,
    stored_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_state (
    vm_id              TEXT PRIMARY KEY,
    ip                 TEXT,
    restart_count      INTEGER NOT NULL DEFAULT 0,
    last_exit_code     INTEGER,
    restart_generation INTEGER NOT NULL DEFAULT 0,
    created            INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS event_buffer (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    evt_id TEXT UNIQUE NOT NULL,
    body   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS log_lines (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    vm_id   TEXT NOT NULL,
    app_id  TEXT,
    time_ns INTEGER NOT NULL,
    stream  TEXT NOT NULL,
    line    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS log_lines_by_vm ON log_lines (vm_id, time_ns);
CREATE INDEX IF NOT EXISTS log_lines_by_app ON log_lines (app_id, time_ns);
CREATE TABLE IF NOT EXISTS deployment_lines (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL,
    phase         TEXT NOT NULL,
    time_ns       INTEGER NOT NULL,
    message       TEXT NOT NULL,
    vm_id         TEXT
);
CREATE INDEX IF NOT EXISTS deployment_lines_by_deployment
    ON deployment_lines (deployment_id, time_ns);
CREATE TABLE IF NOT EXISTS transcripts (
    session_id TEXT PRIMARY KEY,
    vm_id      TEXT NOT NULL,
    path       TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    sha256     TEXT NOT NULL DEFAULT '',
    started_at INTEGER NOT NULL,
    ended_at   INTEGER
);
CREATE INDEX IF NOT EXISTS transcripts_by_start ON transcripts (started_at);
";

/// Capacity of the live log fan-out channel. A stream that falls further
/// behind loses lines and carries on, so a slow reader never blocks capture.
const LOG_BROADCAST_CAPACITY: usize = 1_024;

/// The node's SQLite store.
#[derive(Clone)]
pub struct NodeStore {
    pub(crate) pool: SqlitePool,
    /// Root-owned data directory the store was opened in.
    pub(crate) data_dir: std::path::PathBuf,
    /// The node-local key that seals desired-state bodies.
    pub(crate) key: Arc<LocalKey>,
    /// The strictly increasing node-wide log clock.
    pub(crate) clock: Arc<LogClock>,
    /// Newly captured log lines, for open streams.
    pub(crate) logs_tx: broadcast::Sender<CapturedLine>,
    /// Serializes log appends, so instants are committed and broadcast in
    /// the order they were handed out and a live stream never sees one go
    /// backwards.
    pub(crate) log_gate: Arc<tokio::sync::Mutex<()>>,
}

/// A node-wide monotonic clock in nanoseconds since the Unix epoch.
///
/// The wall clock may repeat a value or step backwards (NTP); the control
/// plane uses these instants as cursors, so every value handed out is strictly
/// greater than the previous one.
pub(crate) struct LogClock {
    last: Mutex<i64>,
}

impl LogClock {
    fn new(last: i64) -> Self {
        Self {
            last: Mutex::new(last),
        }
    }

    /// The next instant: the wall clock when it moved forward, otherwise the
    /// last value plus one.
    fn next(&self) -> i64 {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = util::now_ns();
        let value = if now > *last {
            now
        } else {
            last.saturating_add(1)
        };
        *last = value;
        value
    }
}

/// One VM's applied row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VmRecord {
    /// The address handed to the VM, reused across restarts.
    pub ip: Option<Ipv4Addr>,
    /// Crash restarts recorded since the last healthy reset.
    pub restart_count: u64,
    /// The exit code of the last stop or crash the agent saw.
    pub last_exit_code: Option<i64>,
    /// The `restart_generation` of the spec that was last applied.
    pub restart_generation: u64,
    /// Whether the VM was created by this agent.
    pub created: bool,
}

/// One terminal session's transcript (`audit-log-format.md` section 3.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTranscript {
    /// The `trm_` session id the primary minted.
    pub session_id: String,
    /// The VM the session was attached to.
    pub vm_id: String,
    /// The asciicast file on this node.
    pub path: std::path::PathBuf,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Lowercase hex SHA-256 of the file.
    pub sha256: String,
    /// Unix seconds when the session started.
    pub started_at: i64,
    /// Unix seconds when it ended; `None` while it is open.
    pub ended_at: Option<i64>,
}

/// One buffered event awaiting delivery.
#[derive(Clone, Debug, PartialEq)]
pub struct BufferedEvent {
    /// `seq` of the `event_buffer` row.
    pub seq: i64,
    /// The `evt_` id the primary deduplicates by.
    pub evt_id: String,
    /// The event body, without `seq`.
    pub body: Value,
}

impl NodeStore {
    /// Opens (creating when missing) `<data_dir>/node.db` in WAL mode with
    /// foreign keys on, and creates the tables.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when SQLite cannot open the file or the schema
    /// cannot be created.
    pub async fn open(data_dir: &Path) -> Result<Self, AgentError> {
        let path = data_dir.join("node.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            // Deleted rows (pruned log lines, resealed plaintext bodies) are
            // overwritten, not left in free pages.
            .pragma("secure_delete", "ON")
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(store_error)?;
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(store_error)?;
        migrate(&pool).await?;

        let key = Arc::new(LocalKey::load_or_create(data_dir)?);
        reseal_legacy(&pool, &key).await?;
        let last_log = max_log_instant(&pool).await?;
        let (logs_tx, _) = broadcast::channel(LOG_BROADCAST_CAPACITY);
        Ok(Self {
            pool,
            data_dir: data_dir.to_path_buf(),
            key,
            clock: Arc::new(LogClock::new(last_log)),
            logs_tx,
            log_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// The data directory the store lives in.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The console socket path for `vm_id`, creating `<data_dir>/consoles`
    /// `0700` when it does not exist.
    ///
    /// The path is derived from the VM id alone, so it is reconstructible
    /// after an agent restart, and it stays short for the AF_UNIX limit.
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the directory cannot be created.
    pub fn console_socket(&self, vm_id: &str) -> Result<std::path::PathBuf, AgentError> {
        let dir = self.data_dir.join("consoles");
        if !dir.exists() {
            std::fs::create_dir_all(&dir)
                .map_err(|e| AgentError::io("create the console socket directory", e))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(|e| AgentError::io("secure the console socket directory", e))?;
            }
        }
        Ok(dir.join(format!("{vm_id}.sock")))
    }

    /// Where a VM's serial port socket lives (`engine-trait.md` section 3.19):
    /// next to its console socket, in the same root-only directory. The engine
    /// is the socket's only client (ADR 0116).
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the directory cannot be created.
    pub fn serial_socket(&self, vm_id: &str) -> Result<std::path::PathBuf, AgentError> {
        let console = self.console_socket(vm_id)?;
        Ok(console.with_file_name(format!("{vm_id}.serial.sock")))
    }

    /// Where a VM's hybrid vsock socket lives (`engine-trait.md` section
    /// 3.18), in the same root-only directory. Only the node agent connects to
    /// it, and nothing ever listens on `<socket>_<port>`, so the guest cannot
    /// reach the host over vsock (ADR 0113).
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the directory cannot be created.
    pub fn vsock_socket(&self, vm_id: &str) -> Result<std::path::PathBuf, AgentError> {
        let console = self.console_socket(vm_id)?;
        Ok(console.with_file_name(format!("{vm_id}.vsock.sock")))
    }

    /// The next strictly increasing node-wide log instant, in nanoseconds
    /// since the Unix epoch.
    #[must_use]
    pub fn next_log_time(&self) -> i64 {
        self.clock.next()
    }

    /// Subscribes to newly captured log lines.
    #[must_use]
    pub fn subscribe_logs(&self) -> broadcast::Receiver<CapturedLine> {
        self.logs_tx.subscribe()
    }

    /// The node metadata for `key`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn meta(&self, key: &str) -> Result<Option<String>, AgentError> {
        let row = sqlx::query("SELECT value FROM node_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        match row {
            Some(row) => row
                .try_get::<String, _>("value")
                .map(Some)
                .map_err(store_error),
            None => Ok(None),
        }
    }

    /// Sets one node metadata value.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_meta(&self, key: &str, value: &str) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO node_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// The newest stored revision and its signed body.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a body that is not JSON.
    pub async fn latest_desired(&self) -> Result<Option<(u64, Value)>, AgentError> {
        let row =
            sqlx::query("SELECT rev, body, body_enc FROM desired_state ORDER BY rev DESC LIMIT 1")
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let rev: i64 = row.try_get("rev").map_err(store_error)?;
        let rev = u64::try_from(rev).unwrap_or(0);
        let sealed: Option<Vec<u8>> = row.try_get("body_enc").map_err(store_error)?;
        let body = match sealed {
            Some(blob) => {
                let plaintext = self.key.open(rev, &blob)?;
                String::from_utf8(plaintext).map_err(|_| {
                    AgentError::Store("the stored desired state is not UTF-8".to_owned())
                })?
            }
            // A row written before encryption existed: read it as plaintext.
            None => row.try_get("body").map_err(store_error)?,
        };
        let body = serde_json::from_str::<Value>(&body)
            .map_err(|e| AgentError::Store(format!("stored desired state is not JSON: {e}")))?;
        Ok(Some((rev, body)))
    }

    /// Persists one accepted `desired_state` body atomically.
    ///
    /// The body is the whole signed JSON exactly as received, so the signature
    /// can be verified again after a restart. Old revisions are pruned in the
    /// same transaction, which keeps only the last [`KEPT_REVISIONS`].
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the body cannot be encoded or SQLite fails.
    pub async fn store_desired(&self, rev: u64, body: &Value) -> Result<(), AgentError> {
        let text = serde_json::to_string(body)
            .map_err(|e| AgentError::Store(format!("could not encode desired state: {e}")))?;
        let sealed = self.key.seal(rev, text.as_bytes());
        let issued_at = body
            .get("issued_at")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let term = body.get("term").and_then(Value::as_u64).unwrap_or(0);
        let rev = i64::try_from(rev).unwrap_or(i64::MAX);

        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query(
            "INSERT INTO desired_state (rev, body, body_enc, issued_at, term, stored_at) \
             VALUES (?, '', ?, ?, ?, ?) \
             ON CONFLICT(rev) DO UPDATE SET body = '', body_enc = excluded.body_enc, \
             issued_at = excluded.issued_at, term = excluded.term, \
             stored_at = excluded.stored_at",
        )
        .bind(rev)
        .bind(sealed)
        .bind(issued_at)
        .bind(i64::try_from(term).unwrap_or(i64::MAX))
        .bind(util::now_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        sqlx::query(
            "DELETE FROM desired_state WHERE rev < \
             (SELECT rev FROM desired_state ORDER BY rev DESC LIMIT 1 OFFSET ?)",
        )
        .bind(KEPT_REVISIONS - 1)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        tx.commit().await.map_err(store_error)
    }

    /// The newest revision that was fully reconciled, or 0.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn applied_rev(&self) -> Result<u64, AgentError> {
        Ok(self
            .meta(APPLIED_REV)
            .await?
            .and_then(|text| text.parse().ok())
            .unwrap_or(0))
    }

    /// Records the newest fully reconciled revision.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_applied_rev(&self, rev: u64) -> Result<(), AgentError> {
        self.set_meta(APPLIED_REV, &rev.to_string()).await
    }

    /// How many events the buffer has dropped since the last delivery.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn dropped_events(&self) -> Result<u64, AgentError> {
        Ok(self
            .meta(DROPPED_EVENTS)
            .await?
            .and_then(|text| text.parse().ok())
            .unwrap_or(0))
    }

    /// One VM's applied row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn vm_record(&self, vm_id: &str) -> Result<Option<VmRecord>, AgentError> {
        let row = sqlx::query(
            "SELECT ip, restart_count, last_exit_code, restart_generation, created \
             FROM applied_state WHERE vm_id = ?",
        )
        .bind(vm_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let restart_count: i64 = row.try_get("restart_count").map_err(store_error)?;
        let generation: i64 = row.try_get("restart_generation").map_err(store_error)?;
        let created: i64 = row.try_get("created").map_err(store_error)?;
        let ip: Option<String> = row.try_get("ip").map_err(store_error)?;
        let exit_code: Option<i64> = row.try_get("last_exit_code").map_err(store_error)?;
        Ok(Some(VmRecord {
            ip: ip.and_then(|text| text.parse().ok()),
            restart_count: u64::try_from(restart_count).unwrap_or(0),
            last_exit_code: exit_code,
            restart_generation: u64::try_from(generation).unwrap_or(0),
            created: created != 0,
        }))
    }

    /// Records that the VM was created with `ip` at `restart_generation`.
    /// Restart bookkeeping of an existing row is preserved.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn record_created(
        &self,
        vm_id: &str,
        ip: Ipv4Addr,
        restart_generation: u64,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO applied_state (vm_id, ip, restart_generation, created) \
             VALUES (?, ?, ?, 1) \
             ON CONFLICT(vm_id) DO UPDATE SET ip = excluded.ip, \
             restart_generation = excluded.restart_generation, created = 1",
        )
        .bind(vm_id)
        .bind(ip.to_string())
        .bind(i64::try_from(restart_generation).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Stores or replaces one VM's address.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_vm_ip(&self, vm_id: &str, ip: Ipv4Addr) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO applied_state (vm_id, ip) VALUES (?, ?) \
             ON CONFLICT(vm_id) DO UPDATE SET ip = excluded.ip",
        )
        .bind(vm_id)
        .bind(ip.to_string())
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Forgets a VM entirely, including its address.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn forget_vm(&self, vm_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM applied_state WHERE vm_id = ?")
            .bind(vm_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Every address currently recorded, so the allocator never hands one out
    /// twice.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn used_ips(&self) -> Result<Vec<Ipv4Addr>, AgentError> {
        let rows = sqlx::query("SELECT ip FROM applied_state WHERE ip IS NOT NULL")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        let mut ips = Vec::with_capacity(rows.len());
        for row in rows {
            let text: String = row.try_get("ip").map_err(store_error)?;
            if let Ok(ip) = text.parse() {
                ips.push(ip);
            }
        }
        Ok(ips)
    }

    /// Counts one crash restart and remembers the exit code that preceded it.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn record_restart(
        &self,
        vm_id: &str,
        last_exit_code: Option<i64>,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE applied_state SET restart_count = restart_count + 1, \
             last_exit_code = COALESCE(?, last_exit_code) WHERE vm_id = ?",
        )
        .bind(last_exit_code)
        .bind(vm_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Clears the crash-restart count after a healthy period.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn reset_restarts(&self, vm_id: &str) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE applied_state SET restart_count = 0, last_exit_code = NULL WHERE vm_id = ?",
        )
        .bind(vm_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Remembers the exit code of the VM's last stop without counting it as a
    /// crash.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_exit_code(
        &self,
        vm_id: &str,
        last_exit_code: Option<i64>,
    ) -> Result<(), AgentError> {
        sqlx::query("UPDATE applied_state SET last_exit_code = ? WHERE vm_id = ?")
            .bind(last_exit_code)
            .bind(vm_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Records the `restart_generation` a spec last asked for.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_restart_generation(
        &self,
        vm_id: &str,
        generation: u64,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO applied_state (vm_id, restart_generation) VALUES (?, ?) \
             ON CONFLICT(vm_id) DO UPDATE SET restart_generation = excluded.restart_generation",
        )
        .bind(vm_id)
        .bind(i64::try_from(generation).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Buffers one event, dropping the oldest entries when the buffer is full
    /// and counting each drop (section 6.5).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the body cannot be encoded or SQLite fails.
    pub async fn push_event(&self, evt_id: &str, body: &Value) -> Result<(), AgentError> {
        let text = serde_json::to_string(body)
            .map_err(|e| AgentError::Store(format!("could not encode an event: {e}")))?;
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query("INSERT OR IGNORE INTO event_buffer (evt_id, body) VALUES (?, ?)")
            .bind(evt_id)
            .bind(text)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM event_buffer")
            .fetch_one(&mut *tx)
            .await
            .map_err(store_error)?
            .try_get("n")
            .map_err(store_error)?;
        let excess = count - EVENT_BUFFER_LIMIT;
        if excess > 0 {
            sqlx::query(
                "DELETE FROM event_buffer WHERE seq IN \
                 (SELECT seq FROM event_buffer ORDER BY seq ASC LIMIT ?)",
            )
            .bind(excess)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
            let dropped = sqlx::query("SELECT value FROM node_meta WHERE key = ?")
                .bind(DROPPED_EVENTS)
                .fetch_optional(&mut *tx)
                .await
                .map_err(store_error)?
                .and_then(|row| row.try_get::<String, _>("value").ok())
                .and_then(|text| text.parse::<u64>().ok())
                .unwrap_or(0)
                .saturating_add(u64::try_from(excess).unwrap_or(0));
            sqlx::query(
                "INSERT INTO node_meta (key, value) VALUES (?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(DROPPED_EVENTS)
            .bind(dropped.to_string())
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)
    }

    /// Every buffered event, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unparseable body.
    pub async fn buffered_events(&self) -> Result<Vec<BufferedEvent>, AgentError> {
        let rows = sqlx::query("SELECT seq, evt_id, body FROM event_buffer ORDER BY seq ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.try_get("seq").map_err(store_error)?;
            let evt_id: String = row.try_get("evt_id").map_err(store_error)?;
            let text: String = row.try_get("body").map_err(store_error)?;
            let body = serde_json::from_str::<Value>(&text).map_err(|e| {
                AgentError::Store(format!("buffered event {evt_id} is not JSON: {e}"))
            })?;
            events.push(BufferedEvent { seq, evt_id, body });
        }
        Ok(events)
    }

    /// Removes every buffered event with `seq` at or below `through`, which the
    /// session calls once those frames were written.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn drain_events(&self, through: i64) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM event_buffer WHERE seq <= ?")
            .bind(through)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Records a terminal session's transcript row when its file is created.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn insert_transcript(&self, row: &StoredTranscript) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO transcripts \
             (session_id, vm_id, path, size_bytes, sha256, started_at, ended_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(session_id) DO UPDATE SET vm_id = excluded.vm_id, \
             path = excluded.path, size_bytes = excluded.size_bytes, \
             sha256 = excluded.sha256, started_at = excluded.started_at, \
             ended_at = excluded.ended_at",
        )
        .bind(&row.session_id)
        .bind(&row.vm_id)
        .bind(row.path.to_string_lossy().into_owned())
        .bind(i64::try_from(row.size_bytes).unwrap_or(i64::MAX))
        .bind(&row.sha256)
        .bind(row.started_at)
        .bind(row.ended_at)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Records a transcript's final size, hash and end time.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn finish_transcript(
        &self,
        session_id: &str,
        size_bytes: u64,
        sha256: &str,
        ended_at: i64,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE transcripts SET size_bytes = ?, sha256 = ?, ended_at = ? \
             WHERE session_id = ?",
        )
        .bind(i64::try_from(size_bytes).unwrap_or(i64::MAX))
        .bind(sha256)
        .bind(ended_at)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// One transcript row, when this node holds it.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn transcript(
        &self,
        session_id: &str,
    ) -> Result<Option<StoredTranscript>, AgentError> {
        let row = sqlx::query(
            "SELECT session_id, vm_id, path, size_bytes, sha256, started_at, ended_at \
             FROM transcripts WHERE session_id = ?",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(transcript_row).transpose()
    }

    /// Every transcript row on this node.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn transcripts(&self) -> Result<Vec<StoredTranscript>, AgentError> {
        let rows = sqlx::query(
            "SELECT session_id, vm_id, path, size_bytes, sha256, started_at, ended_at \
             FROM transcripts ORDER BY started_at ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(transcript_row).collect()
    }

    /// Deletes one transcript row; its file is removed by the caller.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn delete_transcript(&self, session_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM transcripts WHERE session_id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// The pool, for a test that wants to inspect a table directly.
    #[doc(hidden)]
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

fn transcript_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredTranscript, AgentError> {
    let size: i64 = row.try_get("size_bytes").map_err(store_error)?;
    let ended: Option<i64> = row.try_get("ended_at").map_err(store_error)?;
    Ok(StoredTranscript {
        session_id: row.try_get("session_id").map_err(store_error)?,
        vm_id: row.try_get("vm_id").map_err(store_error)?,
        path: std::path::PathBuf::from(row.try_get::<String, _>("path").map_err(store_error)?),
        size_bytes: u64::try_from(size).unwrap_or(0),
        sha256: row.try_get("sha256").map_err(store_error)?,
        started_at: row.try_get("started_at").map_err(store_error)?,
        ended_at: ended,
    })
}

fn store_error(error: sqlx::Error) -> AgentError {
    AgentError::Store(error.to_string())
}

/// Adds `desired_state.body_enc` to a database created before bodies were
/// encrypted. New databases get the column from [`SCHEMA`]; existing ones are
/// altered once, and the old plaintext `body` rows keep loading.
async fn migrate(pool: &SqlitePool) -> Result<(), AgentError> {
    let columns = sqlx::query("PRAGMA table_info(desired_state)")
        .fetch_all(pool)
        .await
        .map_err(store_error)?;
    let has_enc = columns.iter().any(|row| {
        row.try_get::<String, _>("name")
            .is_ok_and(|name| name == "body_enc")
    });
    if !has_enc {
        sqlx::query("ALTER TABLE desired_state ADD COLUMN body_enc BLOB")
            .execute(pool)
            .await
            .map_err(store_error)?;
    }
    Ok(())
}

/// Encrypts every desired-state row still stored as plaintext by an older
/// agent, so env values written before the node-local key existed do not
/// stay readable on disk.
async fn reseal_legacy(pool: &SqlitePool, key: &LocalKey) -> Result<(), AgentError> {
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT rev, body FROM desired_state WHERE body_enc IS NULL AND body <> ''")
            .fetch_all(pool)
            .await
            .map_err(store_error)?;
    for (rev, body) in rows {
        let sealed = key.seal(u64::try_from(rev).unwrap_or(0), body.as_bytes());
        sqlx::query("UPDATE desired_state SET body = '', body_enc = ? WHERE rev = ?")
            .bind(sealed)
            .bind(rev)
            .execute(pool)
            .await
            .map_err(store_error)?;
    }
    Ok(())
}

/// The newest log instant already stored, so a restart never reuses one.
async fn max_log_instant(pool: &SqlitePool) -> Result<i64, AgentError> {
    let logs: Option<i64> = sqlx::query_scalar("SELECT MAX(time_ns) FROM log_lines")
        .fetch_one(pool)
        .await
        .map_err(store_error)?;
    let deployments: Option<i64> = sqlx::query_scalar("SELECT MAX(time_ns) FROM deployment_lines")
        .fetch_one(pool)
        .await
        .map_err(store_error)?;
    Ok(logs.unwrap_or(0).max(deployments.unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-node-store-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn the_newest_body_survives_and_old_revisions_are_pruned() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(store.latest_desired().await.unwrap(), None);

        for rev in 1..=8 {
            let body = json!({"rev": rev, "issued_at": "2026-11-02T10:15:31Z", "term": 1, "signature": "x"});
            store.store_desired(rev, &body).await.unwrap();
        }
        let (rev, body) = store.latest_desired().await.unwrap().unwrap();
        assert_eq!(rev, 8);
        assert_eq!(body["signature"], "x");
        let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM desired_state")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .get("n");
        assert_eq!(count, KEPT_REVISIONS);
    }

    #[tokio::test]
    async fn vm_rows_keep_their_ip_and_restart_bookkeeping() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let ip = Ipv4Addr::new(10, 42, 1, 7);
        store.record_created("vm_1", ip, 3).await.unwrap();
        store.record_restart("vm_1", Some(137)).await.unwrap();
        store.record_restart("vm_1", None).await.unwrap();

        let record = store.vm_record("vm_1").await.unwrap().unwrap();
        assert_eq!(record.ip, Some(ip));
        assert_eq!(record.restart_count, 2);
        assert_eq!(record.last_exit_code, Some(137));
        assert_eq!(record.restart_generation, 3);
        assert!(record.created);
        assert_eq!(store.used_ips().await.unwrap(), vec![ip]);

        store.set_restart_generation("vm_1", 4).await.unwrap();
        store.reset_restarts("vm_1").await.unwrap();
        let record = store.vm_record("vm_1").await.unwrap().unwrap();
        assert_eq!(record.restart_generation, 4);
        assert_eq!(record.restart_count, 0);
        assert_eq!(record.last_exit_code, None);

        store.forget_vm("vm_1").await.unwrap();
        assert_eq!(store.vm_record("vm_1").await.unwrap(), None);
        assert!(store.used_ips().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn events_are_drained_in_order() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        store
            .push_event("evt_1", &json!({"id": "evt_1"}))
            .await
            .unwrap();
        store
            .push_event("evt_2", &json!({"id": "evt_2"}))
            .await
            .unwrap();
        let events = store.buffered_events().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].evt_id, "evt_1");
        store.drain_events(events[0].seq).await.unwrap();
        let events = store.buffered_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].evt_id, "evt_2");
        assert_eq!(store.dropped_events().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn applied_rev_round_trips() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(store.applied_rev().await.unwrap(), 0);
        store.set_applied_rev(42).await.unwrap();
        assert_eq!(store.applied_rev().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn legacy_plaintext_rows_still_load() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let body =
            json!({"rev": 3, "issued_at": "2026-11-02T10:15:31Z", "term": 1, "signature": "old"});
        sqlx::query(
            "INSERT INTO desired_state (rev, body, issued_at, term, stored_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(3i64)
        .bind(body.to_string())
        .bind("2026-11-02T10:15:31Z")
        .bind(1i64)
        .bind("2026-11-02T10:15:32Z")
        .execute(store.pool())
        .await
        .unwrap();

        let (rev, loaded) = store.latest_desired().await.unwrap().unwrap();
        assert_eq!(rev, 3);
        assert_eq!(loaded["signature"], "old");

        // The next start reseals it: no plaintext stays on disk.
        drop(store);
        let store = NodeStore::open(&dir.0).await.unwrap();
        let (plain, sealed): (String, Option<Vec<u8>>) =
            sqlx::query_as("SELECT body, body_enc FROM desired_state WHERE rev = 3")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert!(plain.is_empty());
        assert!(sealed.is_some());
        let (_, loaded) = store.latest_desired().await.unwrap().unwrap();
        assert_eq!(loaded["signature"], "old");
    }

    #[tokio::test]
    async fn a_new_row_is_ciphertext_and_a_tampered_one_fails() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let body =
            json!({"rev": 9, "issued_at": "2026-11-02T10:15:31Z", "term": 1, "signature": "x"});
        store.store_desired(9, &body).await.unwrap();

        let row = sqlx::query("SELECT body, body_enc FROM desired_state WHERE rev = 9")
            .fetch_one(store.pool())
            .await
            .unwrap();
        let plaintext: String = row.try_get("body").unwrap();
        assert!(plaintext.is_empty(), "no plaintext is kept for new rows");
        let mut sealed: Vec<u8> = row.try_get("body_enc").unwrap();
        assert!(!sealed.is_empty());

        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        sqlx::query("UPDATE desired_state SET body_enc = ? WHERE rev = 9")
            .bind(sealed)
            .execute(store.pool())
            .await
            .unwrap();
        assert!(matches!(
            store.latest_desired().await,
            Err(AgentError::LocalKey(_))
        ));
    }
}
