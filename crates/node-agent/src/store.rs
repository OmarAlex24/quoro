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

use quoro_proto::wire::{BuildPhase, BuildState, FailedStep, VolumeState};
use quoro_proto::{FenceReason, LeaseId, LeaseMode, ResourceType};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::broadcast;

use crate::AgentError;
use crate::lease::{FenceRecord, LeaseState, StoredLease};
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
CREATE TABLE IF NOT EXISTS builds (
    build_id     TEXT PRIMARY KEY,
    app_id       TEXT NOT NULL,
    status       TEXT NOT NULL,
    failed_phase TEXT,
    [commit]     TEXT,
    image_digest TEXT,
    error        TEXT,
    failed_step  TEXT,
    log_path     TEXT,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    finished_at  TEXT
);
CREATE INDEX IF NOT EXISTS builds_by_app ON builds (app_id, created_at);
CREATE TABLE IF NOT EXISTS volumes (
    volume_id       TEXT PRIMARY KEY,
    app_id          TEXT NOT NULL,
    size_gb         INTEGER NOT NULL,
    state           TEXT NOT NULL,
    error           TEXT,
    allocated_bytes INTEGER
);
CREATE TABLE IF NOT EXISTS leases (
    lease_id      TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    mode          TEXT NOT NULL,
    token         INTEGER NOT NULL,
    state         TEXT NOT NULL,
    duration_ms   INTEGER
);
CREATE TABLE IF NOT EXISTS lease_tokens (
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    token         INTEGER NOT NULL,
    PRIMARY KEY (resource_type, resource_id)
);
CREATE TABLE IF NOT EXISTS fences (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id  TEXT NOT NULL,
    token     INTEGER NOT NULL,
    vm_ids    TEXT NOT NULL,
    at_ns     INTEGER NOT NULL,
    reason    TEXT NOT NULL,
    complete  INTEGER NOT NULL
);
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

/// One build's local row (`node-agent-protocol.md` section 12, ADR 0117).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildRecord {
    /// `bld_<ULID>`.
    pub build_id: String,
    /// `app_<ULID>` the build belongs to; also its cache key.
    pub app_id: String,
    /// Where the build is: `queued` up to a terminal state.
    pub status: BuildState,
    /// The phase it failed in, on `failed`.
    pub failed_phase: Option<BuildPhase>,
    /// Full commit SHA the ref resolved to, once the checkout finished.
    pub commit: Option<String>,
    /// Manifest digest of the built image, on `succeeded`.
    pub image_digest: Option<String>,
    /// One-line reason, on `failed`.
    pub error: Option<String>,
    /// The failing BuildKit step, on `failed`.
    pub failed_step: Option<FailedStep>,
    /// Path of the build's human log file, when it has one.
    pub log_path: Option<String>,
    /// Creation order, an RFC 3339 string with nanoseconds.
    pub created_at: String,
    /// When the node left `queued`.
    pub started_at: Option<String>,
    /// When the build reached a terminal status.
    pub finished_at: Option<String>,
}

impl BuildRecord {
    /// The `status_report.builds[]` entry for this row (section 5.3).
    #[must_use]
    pub fn to_status(&self) -> quoro_proto::wire::BuildStatus {
        quoro_proto::wire::BuildStatus {
            build_id: self.build_id.clone(),
            status: self.status,
            commit: self.commit.clone(),
            image_digest: self.image_digest.clone(),
            failed_phase: self.failed_phase,
            error: self.error.clone(),
            failed_step: self.failed_step.clone(),
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
        }
    }
}

/// One volume row (`node-agent-protocol.md` section 12 `volumes`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeRecord {
    /// `vol_` id; also the name of the file.
    pub volume_id: String,
    /// `app_` id of the owner.
    pub app_id: String,
    /// Size of the file in gibibytes.
    pub size_gb: u32,
    /// The last state the node reported.
    pub state: VolumeState,
    /// One-line reason, on `failed`.
    pub error: Option<String>,
    /// Bytes the sparse file uses, when known.
    pub allocated_bytes: Option<u64>,
}

/// The `node_meta` key holding the node's failed mark (section 12).
const NODE_FAILED: &str = "node_failed";

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

    /// Inserts or replaces one build row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the failed step cannot be encoded or SQLite
    /// fails.
    pub async fn save_build(&self, record: &BuildRecord) -> Result<(), AgentError> {
        let failed_step = match &record.failed_step {
            Some(step) => Some(serde_json::to_string(step).map_err(|e| {
                AgentError::Store(format!("could not encode a build's failed step: {e}"))
            })?),
            None => None,
        };
        sqlx::query(
            "INSERT INTO builds (build_id, app_id, status, failed_phase, \"commit\", image_digest, \
             error, failed_step, log_path, created_at, started_at, finished_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(build_id) DO UPDATE SET app_id = excluded.app_id, \
             status = excluded.status, failed_phase = excluded.failed_phase, \
             \"commit\" = excluded.\"commit\", image_digest = excluded.image_digest, \
             error = excluded.error, failed_step = excluded.failed_step, \
             log_path = excluded.log_path, created_at = excluded.created_at, \
             started_at = excluded.started_at, finished_at = excluded.finished_at",
        )
        .bind(&record.build_id)
        .bind(&record.app_id)
        .bind(record.status.as_str())
        .bind(record.failed_phase.map(BuildPhase::as_str))
        .bind(record.commit.as_deref())
        .bind(record.image_digest.as_deref())
        .bind(record.error.as_deref())
        .bind(failed_step)
        .bind(record.log_path.as_deref())
        .bind(&record.created_at)
        .bind(record.started_at.as_deref())
        .bind(record.finished_at.as_deref())
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// One build row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn build(&self, build_id: &str) -> Result<Option<BuildRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM builds WHERE build_id = ?")
            .bind(build_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(build_record).transpose()
    }

    /// Every build row, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn builds(&self) -> Result<Vec<BuildRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM builds ORDER BY created_at ASC, build_id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(build_record).collect()
    }

    /// Every build row of one app, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn builds_for_app(&self, app_id: &str) -> Result<Vec<BuildRecord>, AgentError> {
        let rows = sqlx::query(
            "SELECT * FROM builds WHERE app_id = ? ORDER BY created_at ASC, build_id ASC",
        )
        .bind(app_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(build_record).collect()
    }

    /// Removes one build row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn delete_build(&self, build_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM builds WHERE build_id = ?")
            .bind(build_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Removes every build row of one app.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn delete_builds_for_app(&self, app_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM builds WHERE app_id = ?")
            .bind(app_id)
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

    /// One volume row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn volume(&self, volume_id: &str) -> Result<Option<VolumeRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM volumes WHERE volume_id = ?")
            .bind(volume_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(volume_record).transpose()
    }

    /// Every volume row, sorted by id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn volumes(&self) -> Result<Vec<VolumeRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM volumes ORDER BY volume_id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(volume_record).collect()
    }

    /// Inserts or replaces one volume row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn save_volume(&self, record: &VolumeRecord) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO volumes (volume_id, app_id, size_gb, state, error, allocated_bytes) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(volume_id) DO UPDATE SET app_id = excluded.app_id, \
             size_gb = excluded.size_gb, state = excluded.state, error = excluded.error, \
             allocated_bytes = excluded.allocated_bytes",
        )
        .bind(&record.volume_id)
        .bind(&record.app_id)
        .bind(i64::from(record.size_gb))
        .bind(volume_state_str(record.state))
        .bind(record.error.as_deref())
        .bind(
            record
                .allocated_bytes
                .map(|bytes| i64::try_from(bytes).unwrap_or(i64::MAX)),
        )
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Removes one volume row; the file is removed by the caller.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn delete_volume(&self, volume_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM volumes WHERE volume_id = ?")
            .bind(volume_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Every lease row, as last saved (section 12 `leases`).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn load_leases(&self) -> Result<Vec<StoredLease>, AgentError> {
        let rows = sqlx::query("SELECT * FROM leases ORDER BY lease_id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(lease_record).collect()
    }

    /// The highest fencing token seen per resource (section 8.6).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn load_highest_tokens(
        &self,
    ) -> Result<Vec<((ResourceType, String), u64)>, AgentError> {
        let rows = sqlx::query("SELECT * FROM lease_tokens")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        let mut tokens = Vec::with_capacity(rows.len());
        for row in rows {
            let resource_type: String = row.try_get("resource_type").map_err(store_error)?;
            let resource_id: String = row.try_get("resource_id").map_err(store_error)?;
            let token: i64 = row.try_get("token").map_err(store_error)?;
            tokens.push((
                (parse_resource_type(&resource_type)?, resource_id),
                u64::try_from(token).unwrap_or(0),
            ));
        }
        Ok(tokens)
    }

    /// Persists one lease row and the highest token for its resource.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a lease field with no
    /// stored form.
    pub async fn save_lease(&self, lease: &StoredLease, highest: u64) -> Result<(), AgentError> {
        let duration_ms = lease
            .duration
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query(
            "INSERT INTO leases (lease_id, resource_type, resource_id, mode, token, state, \
             duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(lease_id) DO UPDATE SET resource_type = excluded.resource_type, \
             resource_id = excluded.resource_id, mode = excluded.mode, token = excluded.token, \
             state = excluded.state, duration_ms = excluded.duration_ms",
        )
        .bind(&lease.lease_id.0)
        .bind(resource_type_str(lease.resource_type))
        .bind(&lease.resource_id)
        .bind(lease_mode_str(lease.mode))
        .bind(i64::try_from(lease.token).unwrap_or(i64::MAX))
        .bind(lease_state_str(lease.state))
        .bind(duration_ms)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        sqlx::query(
            "INSERT INTO lease_tokens (resource_type, resource_id, token) VALUES (?, ?, ?) \
             ON CONFLICT(resource_type, resource_id) DO UPDATE SET token = MAX(token, excluded.token)",
        )
        .bind(resource_type_str(lease.resource_type))
        .bind(&lease.resource_id)
        .bind(i64::try_from(highest).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        tx.commit().await.map_err(store_error)
    }

    /// Records one self-fence (section 8.5 step 2).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unencodable VM list.
    pub async fn record_fence(&self, record: &FenceRecord) -> Result<(), AgentError> {
        let vm_ids: Vec<String> = record.vm_ids.iter().map(|id| id.0.clone()).collect();
        let vm_ids = serde_json::to_string(&vm_ids)
            .map_err(|e| AgentError::Store(format!("could not encode a fence's VMs: {e}")))?;
        let at_ns = record
            .at
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
            });
        sqlx::query(
            "INSERT INTO fences (lease_id, token, vm_ids, at_ns, reason, complete) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&record.lease_id.0)
        .bind(i64::try_from(record.token).unwrap_or(i64::MAX))
        .bind(vm_ids)
        .bind(at_ns)
        .bind(fence_reason_str(record.reason))
        .bind(i64::from(record.complete))
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Every recorded self-fence, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn fences(&self) -> Result<Vec<FenceRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM fences ORDER BY id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(fence_record).collect()
    }

    /// Sets the node's failed mark (section 12 `node_meta`).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_node_failed(&self, failed: bool) -> Result<(), AgentError> {
        self.set_meta(NODE_FAILED, if failed { "1" } else { "0" })
            .await
    }

    /// Whether the node is marked failed (section 12 `node_meta`).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn node_failed(&self) -> Result<bool, AgentError> {
        Ok(self
            .meta(NODE_FAILED)
            .await?
            .is_some_and(|value| value == "1"))
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

/// Reads one `builds` row back into a [`BuildRecord`].
fn build_record(row: sqlx::sqlite::SqliteRow) -> Result<BuildRecord, AgentError> {
    let status: String = row.try_get("status").map_err(store_error)?;
    let failed_phase: Option<String> = row.try_get("failed_phase").map_err(store_error)?;
    let failed_step: Option<String> = row.try_get("failed_step").map_err(store_error)?;
    Ok(BuildRecord {
        build_id: row.try_get("build_id").map_err(store_error)?,
        app_id: row.try_get("app_id").map_err(store_error)?,
        status: parse_build_state(&status)?,
        failed_phase: failed_phase
            .map(|phase| parse_build_phase(&phase))
            .transpose()?,
        commit: row.try_get("commit").map_err(store_error)?,
        image_digest: row.try_get("image_digest").map_err(store_error)?,
        error: row.try_get("error").map_err(store_error)?,
        failed_step: failed_step
            .map(|step| {
                serde_json::from_str(&step).map_err(|e| {
                    AgentError::Store(format!("a stored build failed step is not JSON: {e}"))
                })
            })
            .transpose()?,
        log_path: row.try_get("log_path").map_err(store_error)?,
        created_at: row.try_get("created_at").map_err(store_error)?,
        started_at: row.try_get("started_at").map_err(store_error)?,
        finished_at: row.try_get("finished_at").map_err(store_error)?,
    })
}

fn parse_build_state(text: &str) -> Result<BuildState, AgentError> {
    match text {
        "queued" => Ok(BuildState::Queued),
        "preparing" => Ok(BuildState::Preparing),
        "building" => Ok(BuildState::Building),
        "exporting" => Ok(BuildState::Exporting),
        "importing" => Ok(BuildState::Importing),
        "succeeded" => Ok(BuildState::Succeeded),
        "failed" => Ok(BuildState::Failed),
        "cancelled" => Ok(BuildState::Cancelled),
        other => Err(AgentError::Store(format!(
            "a stored build has an unknown status {other:?}"
        ))),
    }
}

fn parse_build_phase(text: &str) -> Result<BuildPhase, AgentError> {
    match text {
        "preparing" => Ok(BuildPhase::Preparing),
        "building" => Ok(BuildPhase::Building),
        "exporting" => Ok(BuildPhase::Exporting),
        "importing" => Ok(BuildPhase::Importing),
        other => Err(AgentError::Store(format!(
            "a stored build has an unknown phase {other:?}"
        ))),
    }
}

fn volume_state_str(state: VolumeState) -> &'static str {
    match state {
        VolumeState::Creating => "creating",
        VolumeState::Ready => "ready",
        VolumeState::Restoring => "restoring",
        VolumeState::Failed => "failed",
        VolumeState::Deleted => "deleted",
        VolumeState::Orphaned => "orphaned",
    }
}

fn parse_volume_state(text: &str) -> Result<VolumeState, AgentError> {
    match text {
        "creating" => Ok(VolumeState::Creating),
        "ready" => Ok(VolumeState::Ready),
        "restoring" => Ok(VolumeState::Restoring),
        "failed" => Ok(VolumeState::Failed),
        "deleted" => Ok(VolumeState::Deleted),
        "orphaned" => Ok(VolumeState::Orphaned),
        other => Err(AgentError::Store(format!(
            "a stored volume has an unknown state {other:?}"
        ))),
    }
}

fn volume_record(row: sqlx::sqlite::SqliteRow) -> Result<VolumeRecord, AgentError> {
    let state: String = row.try_get("state").map_err(store_error)?;
    let size_gb: i64 = row.try_get("size_gb").map_err(store_error)?;
    let allocated: Option<i64> = row.try_get("allocated_bytes").map_err(store_error)?;
    Ok(VolumeRecord {
        volume_id: row.try_get("volume_id").map_err(store_error)?,
        app_id: row.try_get("app_id").map_err(store_error)?,
        size_gb: u32::try_from(size_gb).unwrap_or(0),
        state: parse_volume_state(&state)?,
        error: row.try_get("error").map_err(store_error)?,
        allocated_bytes: allocated.and_then(|bytes| u64::try_from(bytes).ok()),
    })
}

fn resource_type_str(resource_type: ResourceType) -> &'static str {
    match resource_type {
        ResourceType::App => "app",
        ResourceType::Primary => "primary",
    }
}

fn parse_resource_type(text: &str) -> Result<ResourceType, AgentError> {
    match text {
        "app" => Ok(ResourceType::App),
        "primary" => Ok(ResourceType::Primary),
        other => Err(AgentError::Store(format!(
            "a stored lease has an unknown resource type {other:?}"
        ))),
    }
}

fn lease_mode_str(mode: LeaseMode) -> &'static str {
    match mode {
        LeaseMode::Local => "local",
        LeaseMode::Expiring => "expiring",
    }
}

fn parse_lease_mode(text: &str) -> Result<LeaseMode, AgentError> {
    match text {
        "local" => Ok(LeaseMode::Local),
        "expiring" => Ok(LeaseMode::Expiring),
        other => Err(AgentError::Store(format!(
            "a stored lease has an unknown mode {other:?}"
        ))),
    }
}

fn lease_state_str(state: LeaseState) -> &'static str {
    match state {
        LeaseState::Held => "held",
        LeaseState::Uncertain => "uncertain",
        LeaseState::Expired => "expired",
        LeaseState::Revoked => "revoked",
    }
}

fn parse_lease_state(text: &str) -> Result<LeaseState, AgentError> {
    match text {
        "held" => Ok(LeaseState::Held),
        "uncertain" => Ok(LeaseState::Uncertain),
        "expired" => Ok(LeaseState::Expired),
        "revoked" => Ok(LeaseState::Revoked),
        other => Err(AgentError::Store(format!(
            "a stored lease has an unknown state {other:?}"
        ))),
    }
}

fn lease_record(row: sqlx::sqlite::SqliteRow) -> Result<StoredLease, AgentError> {
    let resource_type: String = row.try_get("resource_type").map_err(store_error)?;
    let mode: String = row.try_get("mode").map_err(store_error)?;
    let state: String = row.try_get("state").map_err(store_error)?;
    let token: i64 = row.try_get("token").map_err(store_error)?;
    let duration_ms: Option<i64> = row.try_get("duration_ms").map_err(store_error)?;
    Ok(StoredLease {
        lease_id: LeaseId(row.try_get("lease_id").map_err(store_error)?),
        resource_type: parse_resource_type(&resource_type)?,
        resource_id: row.try_get("resource_id").map_err(store_error)?,
        mode: parse_lease_mode(&mode)?,
        token: u64::try_from(token).unwrap_or(0),
        state: parse_lease_state(&state)?,
        duration: duration_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .map(Duration::from_millis),
    })
}

fn fence_reason_str(reason: FenceReason) -> &'static str {
    match reason {
        FenceReason::LeaseExpired => "lease_expired",
        FenceReason::LeaseRevoked => "lease_revoked",
    }
}

fn parse_fence_reason(text: &str) -> Result<FenceReason, AgentError> {
    match text {
        "lease_expired" => Ok(FenceReason::LeaseExpired),
        "lease_revoked" => Ok(FenceReason::LeaseRevoked),
        other => Err(AgentError::Store(format!(
            "a stored fence has an unknown reason {other:?}"
        ))),
    }
}

fn fence_record(row: sqlx::sqlite::SqliteRow) -> Result<FenceRecord, AgentError> {
    let vm_ids: String = row.try_get("vm_ids").map_err(store_error)?;
    let vm_ids: Vec<String> = serde_json::from_str(&vm_ids)
        .map_err(|e| AgentError::Store(format!("a stored fence's VM list is not JSON: {e}")))?;
    let token: i64 = row.try_get("token").map_err(store_error)?;
    let at_ns: i64 = row.try_get("at_ns").map_err(store_error)?;
    let reason: String = row.try_get("reason").map_err(store_error)?;
    let complete: i64 = row.try_get("complete").map_err(store_error)?;
    let at = std::time::UNIX_EPOCH + Duration::from_nanos(u64::try_from(at_ns).unwrap_or(0));
    Ok(FenceRecord {
        lease_id: LeaseId(row.try_get("lease_id").map_err(store_error)?),
        token: u64::try_from(token).unwrap_or(0),
        vm_ids: vm_ids.into_iter().map(quoro_engine::VmId).collect(),
        at,
        reason: parse_fence_reason(&reason)?,
        complete: complete != 0,
    })
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
