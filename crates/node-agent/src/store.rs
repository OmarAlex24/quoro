// SPDX-License-Identifier: AGPL-3.0-only
//! The local node store (`node-agent-protocol.md` section 12).
//!
//! One SQLite file, `<data_dir>/node.db`, owned by root, holding the verified
//! desired state, what was applied per VM, the buffered events, the node's
//! verified copy of the change log (`change_log`, section 7.1), the records of
//! quarantined copies (`quarantine`) and a small metadata table. Nothing here
//! is replicated; the tables are local and may evolve. A quarantined copy of
//! the change log itself is a file under `<data_dir>/quarantine/`.
//!
//! The whole point of the store is that the node keeps running without the
//! control plane (D-0011): the newest verified snapshot is the source of truth
//! after a restart, and the signature is verified again before it is used, so a
//! tampered store cannot drive the engine.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quoro_proto::wire::{
    BackupState, BuildPhase, BuildState, FailedStep, SnapshotMethod, VolumeState,
};
use quoro_proto::{FenceReason, LeaseId, LeaseMode, ResourceType};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::{broadcast, watch};

use crate::AgentError;
use crate::follow::{LogEntry, ZERO_HASH};
use crate::lease::{FenceRecord, LeaseState, StoredLease, StoredValidity};
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
/// `key` of `node_meta` holding the primary's newest `seq` as its last
/// `log_entries` reported it (`head_seq`, sections 5.14 and 12).
const LOG_HEAD_SEQ: &str = "log_head_seq";
/// `key` of `node_meta` holding when the node received [`LOG_HEAD_SEQ`], RFC
/// 3339.
const LOG_HEAD_SEQ_AT: &str = "log_head_seq_at";

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
CREATE TABLE IF NOT EXISTS backups (
    backup_id       TEXT PRIMARY KEY,
    app_id          TEXT NOT NULL,
    volume_id       TEXT NOT NULL,
    status          TEXT NOT NULL,
    snapshot_method TEXT,
    pause_ms        INTEGER,
    guest_flush     INTEGER,
    manifest_key    TEXT,
    manifest_digest TEXT,
    logical_bytes   INTEGER,
    uploaded_bytes  INTEGER,
    chunk_count     INTEGER,
    error           TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    snapshot_path   TEXT
);
CREATE TABLE IF NOT EXISTS leases (
    lease_id      TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    mode          TEXT NOT NULL,
    token         INTEGER NOT NULL,
    state         TEXT NOT NULL,
    duration_ms   INTEGER,
    validity_boot_id        TEXT,
    valid_until_boottime_ns INTEGER
);
CREATE TABLE IF NOT EXISTS lease_tokens (
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    token         INTEGER NOT NULL,
    PRIMARY KEY (resource_type, resource_id)
);
CREATE TABLE IF NOT EXISTS kept_snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    volume_id     TEXT NOT NULL,
    app_id        TEXT NOT NULL,
    purpose       TEXT NOT NULL,
    seq           INTEGER,
    location      TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    method        TEXT NOT NULL,
    pause_ms      INTEGER NOT NULL,
    cells         BLOB
);
CREATE TABLE IF NOT EXISTS warm_targets (
    volume_id        TEXT PRIMARY KEY,
    app_id           TEXT NOT NULL,
    size_gb          INTEGER NOT NULL,
    complete_seq     INTEGER,
    taken_at         TEXT,
    completed_at     TEXT,
    allocated_bytes  INTEGER,
    cells            BLOB,
    highest_token    INTEGER NOT NULL DEFAULT 0,
    rejected_token   INTEGER,
    rejected_at      TEXT,
    pending_seq      INTEGER,
    pending_taken_at TEXT,
    pending_root     TEXT,
    pending_cells    BLOB,
    error            TEXT,
    removed          INTEGER NOT NULL DEFAULT 0
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
CREATE TABLE IF NOT EXISTS change_log (
    seq         INTEGER PRIMARY KEY,
    term        INTEGER NOT NULL,
    time        TEXT NOT NULL,
    actor_type  TEXT NOT NULL,
    actor_id    TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    op          TEXT NOT NULL,
    payload     TEXT NOT NULL,
    prev_hash   TEXT NOT NULL,
    hash        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    kind           TEXT NOT NULL,
    path           TEXT NOT NULL,
    quarantined_at TEXT NOT NULL,
    reason         TEXT NOT NULL,
    last_seq       INTEGER,
    last_hash      TEXT
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
    /// The newest revision [`Self::set_applied_rev`] recorded in this
    /// process, so the session reports it without waiting for the periodic
    /// report (section 5.3).
    pub(crate) applied: Arc<watch::Sender<u64>>,
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

/// One snapshot the node keeps across an agent restart (`warm`,
/// `quarantine`, `node-agent-protocol.md` section 12): the engine's
/// descriptor plus why the node keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeptSnapshotRecord {
    /// The app the volume belongs to.
    pub app_id: String,
    /// The engine's descriptor; `snapshot_id` is the key.
    pub snapshot: quoro_engine::VolumeSnapshot,
    /// Why the node keeps it, for example `warm_base`.
    pub purpose: String,
    /// The `snapshot_seq` of the copy it holds, when it has one.
    pub seq: Option<u64>,
    /// The copy's cell hashes, when the node keeps them (section 6.13).
    pub cells: Option<Vec<u8>>,
}

/// The warm copy this node holds as the target of a warm volume
/// (`node-agent-protocol.md` sections 6.13 and 12).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WarmTargetRecord {
    pub volume_id: String,
    pub app_id: String,
    pub size_gb: u32,
    /// The `snapshot_seq` of the complete copy, `None` before the first one
    /// or when the copy on disk is not known to be one.
    pub complete_seq: Option<u64>,
    /// When the source took that copy's snapshot, from its stream header.
    pub taken_at: Option<String>,
    /// When this node swapped that copy in.
    pub completed_at: Option<String>,
    /// Bytes the complete copy uses on disk.
    pub allocated_bytes: Option<u64>,
    /// The complete copy's cell hashes in their stored form, when known.
    pub cells: Option<Vec<u8>>,
    /// The highest fencing token of a stream this node accepted (section
    /// 8.6).
    pub highest_token: u64,
    /// The token of the last stream refused as stale, and when.
    pub rejected_token: Option<u64>,
    pub rejected_at: Option<String>,
    /// A copy about to be swapped in, written before the swap so that a
    /// crash between the swap and its record is resolved at the next start.
    pub pending: Option<PendingCommit>,
    /// One line on the last failed stream.
    pub error: Option<String>,
    /// The copy was removed on a `deleted` entry; the row stays until the
    /// entry leaves `spec.volumes`, so the reconciler keeps leaving the
    /// entry to the warm receiver.
    pub removed: bool,
}

/// A copy the node is about to swap in (see [`WarmTargetRecord::pending`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCommit {
    pub seq: u64,
    pub taken_at: String,
    /// The copy's root hash, 64 lowercase hex characters.
    pub root: String,
    /// Its cell hashes in their stored form.
    pub cells: Vec<u8>,
}

/// One backup job row (`node-agent-protocol.md` section 12 `backups`).
///
/// It holds ids, sizes, times and one-line reasons, never a key or a
/// credential: the job's `id_key` and `data_key`, and the repository's secret,
/// live only in the memory of the task carrying them (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupRecord {
    /// `bkp_` id.
    pub backup_id: String,
    /// `app_` id of the backed-up volume; also the manifest's `source_id`.
    pub app_id: String,
    /// `vol_` id of the volume.
    pub volume_id: String,
    /// The last status the node reported.
    pub status: BackupState,
    /// From `running`: how the snapshot was taken.
    pub snapshot_method: Option<SnapshotMethod>,
    /// From `running`, for `pause_copy`: how long the VM was paused.
    pub pause_ms: Option<u64>,
    /// From `running`: whether the snapshot followed a guest flush, `true`
    /// for `Flushed` or `NoRunningVm` (ADR 0282). `None` for a row from
    /// before `running` or from an agent older than M5b.
    pub guest_flush: Option<bool>,
    /// On `succeeded`: the manifest's key, relative to the repository root.
    pub manifest_key: Option<String>,
    /// On `succeeded`: BLAKE3 of the encrypted manifest object.
    pub manifest_digest: Option<String>,
    /// On `succeeded`: size of the snapshot.
    pub logical_bytes: Option<u64>,
    /// On `succeeded`: bytes of chunk objects this backup created.
    pub uploaded_bytes: Option<u64>,
    /// On `succeeded`: chunks the manifest lists.
    pub chunk_count: Option<u64>,
    /// One-line reason, on `failed`.
    pub error: Option<String>,
    /// When the node left `queued`.
    pub started_at: Option<String>,
    /// When the node finished its part.
    pub finished_at: Option<String>,
    /// Node-local path of the snapshot file, so a restart can delete what a
    /// crash left (`engine-trait.md` section 3.16). Never reported.
    pub snapshot_path: Option<String>,
}

impl BackupRecord {
    /// The `status_report.backups[]` entry for this row (section 5.3).
    #[must_use]
    pub fn to_status(&self) -> quoro_proto::wire::BackupStatus {
        quoro_proto::wire::BackupStatus {
            backup_id: self.backup_id.clone(),
            status: self.status,
            snapshot_method: self.snapshot_method,
            pause_ms: self.pause_ms,
            guest_flush: self.guest_flush,
            manifest_key: self.manifest_key.clone(),
            manifest_digest: self.manifest_digest.clone(),
            logical_bytes: self.logical_bytes,
            uploaded_bytes: self.uploaded_bytes,
            chunk_count: self.chunk_count,
            error: self.error.clone(),
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
        }
    }
}

/// The `node_meta` key holding the node's failed mark (section 12).
const NODE_FAILED: &str = "node_failed";
/// `node_meta` key of the failed fences the failed mark covers: a JSON array
/// of the lease ids whose fence failed and no `fence_ack` cleared yet
/// (section 12 `node_meta`). It keeps the form an agent before ADR 0245
/// reads, so an agent rolled back to one still records a failed fence and
/// buffers its `node.fence_failed`; their tokens are in
/// [`FAILED_FENCE_TOKENS`].
const FAILED_FENCES: &str = "failed_fences";
/// `node_meta` key of the token of each lease in [`FAILED_FENCES`]: a JSON
/// object keyed by lease id, with the highest token a failed fence of the
/// lease was recorded at (section 5.17, ADR 0245). An agent before ADR 0245
/// neither writes nor reads it; the store fills in what such an agent left
/// when it opens.
const FAILED_FENCE_TOKENS: &str = "failed_fence_tokens";

/// A lease whose fence failed and no `fence_ack` cleared yet (section 8.5).
///
/// One entry stands for every failed fence of the lease, at the highest token
/// any of them was recorded at. A `fence_ack` clears the failed fences below
/// its acknowledged token (section 5.17, ADR 0245), so it clears the entry
/// only when that token is above the highest: a failed fence at or above it
/// keeps the lease failed, whatever lower ones the acknowledgement covers.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FailedFence {
    /// The lease whose fence failed.
    pub lease_id: String,
    /// The highest token a failed fence of the lease was recorded at
    /// (section 8.5 step 2).
    pub fencing_token: u64,
}
/// `node_meta` key set when an upgrade built [`FAILED_FENCES`] from the fence
/// records an older agent left, or gave a lease an older agent listed an
/// estimated token: the agent reports those failed fences to the primary
/// once, so the primary lists them for an acknowledgement.
const FAILED_FENCES_UNREPORTED: &str = "failed_fences_unreported";

/// The `node_meta` key holding the kernel boot id the agent last started on
/// (section 12 `host_boot_id`, section 8.4 "Agent restart").
pub const HOST_BOOT_ID: &str = "host_boot_id";

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
            applied: Arc::new(watch::channel(0).0),
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
        self.set_meta(APPLIED_REV, &rev.to_string()).await?;
        self.applied.send_replace(rev);
        Ok(())
    }

    /// A receiver that changes every time [`Self::set_applied_rev`] records a
    /// revision.
    #[must_use]
    pub fn applied_changes(&self) -> watch::Receiver<u64> {
        self.applied.subscribe()
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
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        buffer_event(&mut tx, evt_id, body).await?;
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

    /// Records a snapshot the node keeps, or updates its purpose, `seq` and
    /// cell hashes. A record never moves to another file: saving the same
    /// `snapshot_id` with another location or volume is an error.
    ///
    /// The record is durable when this returns: the write is committed in its
    /// own transaction, so the engine's start-up sweep keeps the file from
    /// the next start on (engine-trait.md section 3.16).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, or when the id is recorded
    /// with another location or volume.
    pub async fn save_kept_snapshot(&self, record: &KeptSnapshotRecord) -> Result<(), AgentError> {
        let snapshot = &record.snapshot;
        let created_at_ms = snapshot
            .created_at
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
            });
        let done = sqlx::query(
            "INSERT INTO kept_snapshots (snapshot_id, volume_id, app_id, purpose, seq, location, \
             size_bytes, created_at_ms, method, pause_ms, cells) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(snapshot_id) DO UPDATE SET app_id = excluded.app_id, \
             purpose = excluded.purpose, seq = excluded.seq, cells = excluded.cells \
             WHERE kept_snapshots.location = excluded.location \
             AND kept_snapshots.volume_id = excluded.volume_id",
        )
        .bind(&snapshot.snapshot_id)
        .bind(&snapshot.volume_id.0)
        .bind(&record.app_id)
        .bind(&record.purpose)
        .bind(record.seq.map(|seq| i64::try_from(seq).unwrap_or(i64::MAX)))
        .bind(snapshot.location.display().to_string())
        .bind(i64::try_from(snapshot.size_bytes).unwrap_or(i64::MAX))
        .bind(created_at_ms)
        .bind(engine_method_str(snapshot.method))
        .bind(i64::try_from(snapshot.pause.as_millis()).unwrap_or(i64::MAX))
        .bind(record.cells.as_deref())
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        if done.rows_affected() == 0 {
            // The sweep keeps a snapshot by its file name, so a record must
            // never move to another file.
            return Err(AgentError::Store(format!(
                "kept snapshot {} is recorded with another file or volume",
                snapshot.snapshot_id
            )));
        }
        Ok(())
    }

    /// Every snapshot the node keeps, ordered by volume and id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn kept_snapshots(&self) -> Result<Vec<KeptSnapshotRecord>, AgentError> {
        let rows =
            sqlx::query("SELECT * FROM kept_snapshots ORDER BY volume_id ASC, snapshot_id ASC")
                .fetch_all(&self.pool)
                .await
                .map_err(store_error)?;
        rows.into_iter().map(kept_snapshot_record).collect()
    }

    /// The snapshots the node keeps of one volume, ordered by id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn kept_snapshots_of(
        &self,
        volume_id: &str,
    ) -> Result<Vec<KeptSnapshotRecord>, AgentError> {
        let rows = sqlx::query(
            "SELECT * FROM kept_snapshots WHERE volume_id = ? ORDER BY snapshot_id ASC",
        )
        .bind(volume_id)
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(kept_snapshot_record).collect()
    }

    /// The `(volume_id, purpose)` pairs of the kept snapshots, without their
    /// cell hashes.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn kept_snapshot_purposes(&self) -> Result<Vec<(String, String)>, AgentError> {
        sqlx::query_as("SELECT DISTINCT volume_id, purpose FROM kept_snapshots ORDER BY volume_id")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)
    }

    /// Forgets one kept snapshot. The caller deletes the file first, so a
    /// record never outlives its reason to keep the file.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn forget_kept_snapshot(&self, snapshot_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM kept_snapshots WHERE snapshot_id = ?")
            .bind(snapshot_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// The warm copy of one volume this node holds as a target.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn warm_target(
        &self,
        volume_id: &str,
    ) -> Result<Option<WarmTargetRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM warm_targets WHERE volume_id = ?")
            .bind(volume_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(warm_target_record).transpose()
    }

    /// Every warm copy this node holds as a target, sorted by volume id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn warm_targets(&self) -> Result<Vec<WarmTargetRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM warm_targets ORDER BY volume_id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(warm_target_record).collect()
    }

    /// Every warm target row without its cell hashes, sorted by volume id:
    /// `cells` and `pending` are always `None`. For the status entries and
    /// the reconciler, which run every few seconds and need neither (the
    /// hashes of a 1 TiB copy are 32 MiB).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn warm_target_summaries(&self) -> Result<Vec<WarmTargetRecord>, AgentError> {
        let rows = sqlx::query(
            "SELECT volume_id, app_id, size_gb, complete_seq, taken_at, completed_at, \
             allocated_bytes, NULL AS cells, highest_token, rejected_token, rejected_at, \
             NULL AS pending_seq, NULL AS pending_taken_at, NULL AS pending_root, \
             NULL AS pending_cells, error, removed \
             FROM warm_targets ORDER BY volume_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter().map(warm_target_record).collect()
    }

    /// Records that this node is the target of a warm volume, unless it is
    /// recorded already; an existing row is left as it is, except that a
    /// removed copy's row is a target again.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn add_warm_target(
        &self,
        volume_id: &str,
        app_id: &str,
        size_gb: u32,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO warm_targets (volume_id, app_id, size_gb) VALUES (?, ?, ?) \
             ON CONFLICT(volume_id) DO UPDATE SET removed = 0 WHERE warm_targets.removed = 1",
        )
        .bind(volume_id)
        .bind(app_id)
        .bind(i64::from(size_gb))
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Writes the copy's state of one warm target row: everything but the
    /// last stale stream, the error and the removed mark, which only their
    /// own calls write, so a stream that holds the lock never erases a
    /// rejection recorded while it ran. Only the holder of the volume's lock
    /// calls it.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn save_warm_target(&self, record: &WarmTargetRecord) -> Result<(), AgentError> {
        let as_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        let pending = record.pending.as_ref();
        sqlx::query(
            "INSERT INTO warm_targets (volume_id, app_id, size_gb, complete_seq, taken_at, \
             completed_at, allocated_bytes, cells, highest_token, rejected_token, rejected_at, \
             pending_seq, pending_taken_at, pending_root, pending_cells, error) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(volume_id) DO UPDATE SET app_id = excluded.app_id, \
             size_gb = excluded.size_gb, complete_seq = excluded.complete_seq, \
             taken_at = excluded.taken_at, completed_at = excluded.completed_at, \
             allocated_bytes = excluded.allocated_bytes, cells = excluded.cells, \
             highest_token = MAX(warm_targets.highest_token, excluded.highest_token), \
             pending_seq = excluded.pending_seq, pending_taken_at = excluded.pending_taken_at, \
             pending_root = excluded.pending_root, pending_cells = excluded.pending_cells",
        )
        .bind(&record.volume_id)
        .bind(&record.app_id)
        .bind(i64::from(record.size_gb))
        .bind(record.complete_seq.map(as_i64))
        .bind(record.taken_at.as_deref())
        .bind(record.completed_at.as_deref())
        .bind(record.allocated_bytes.map(as_i64))
        .bind(record.cells.as_deref())
        .bind(as_i64(record.highest_token))
        .bind(record.rejected_token.map(as_i64))
        .bind(record.rejected_at.as_deref())
        .bind(pending.map(|p| as_i64(p.seq)))
        .bind(pending.map(|p| p.taken_at.as_str()))
        .bind(pending.map(|p| p.root.as_str()))
        .bind(pending.map(|p| p.cells.as_slice()))
        .bind(record.error.as_deref())
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Raises the highest token seen in a stream of a warm volume; a lower
    /// one leaves it as it is (section 8.6).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn raise_warm_token(&self, volume_id: &str, token: u64) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE warm_targets SET highest_token = MAX(highest_token, ?) WHERE volume_id = ?",
        )
        .bind(i64::try_from(token).unwrap_or(i64::MAX))
        .bind(volume_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Records a stream refused as stale (section 6.13).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn record_warm_rejection(
        &self,
        volume_id: &str,
        token: u64,
        at: &str,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE warm_targets SET rejected_token = ?, rejected_at = ? WHERE volume_id = ?",
        )
        .bind(i64::try_from(token).unwrap_or(i64::MAX))
        .bind(at)
        .bind(volume_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Sets or clears the one-line error of a warm target.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_warm_error(
        &self,
        volume_id: &str,
        error: Option<&str>,
    ) -> Result<(), AgentError> {
        sqlx::query("UPDATE warm_targets SET error = ? WHERE volume_id = ?")
            .bind(error)
            .bind(volume_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Records the cell hashes of the complete copy `complete_seq`, unless
    /// the row names another copy by now.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_warm_cells(
        &self,
        volume_id: &str,
        complete_seq: u64,
        cells: &[u8],
    ) -> Result<(), AgentError> {
        sqlx::query("UPDATE warm_targets SET cells = ? WHERE volume_id = ? AND complete_seq = ?")
            .bind(cells)
            .bind(volume_id)
            .bind(i64::try_from(complete_seq).unwrap_or(i64::MAX))
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// Marks a warm copy removed on a `deleted` entry: no copy, nothing
    /// pending, no error. The caller removed the file first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn mark_warm_removed(&self, volume_id: &str) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE warm_targets SET removed = 1, complete_seq = NULL, taken_at = NULL, \
             completed_at = NULL, allocated_bytes = NULL, cells = NULL, pending_seq = NULL, \
             pending_taken_at = NULL, pending_root = NULL, pending_cells = NULL, error = NULL \
             WHERE volume_id = ?",
        )
        .bind(volume_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Forgets a warm target; the caller removed its copy first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn forget_warm_target(&self, volume_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM warm_targets WHERE volume_id = ?")
            .bind(volume_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// One backup job row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn backup(&self, backup_id: &str) -> Result<Option<BackupRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM backups WHERE backup_id = ?")
            .bind(backup_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(backup_record).transpose()
    }

    /// Every backup job row, sorted by id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn backups(&self) -> Result<Vec<BackupRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM backups ORDER BY backup_id ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        rows.into_iter().map(backup_record).collect()
    }

    /// Inserts or replaces one backup job row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn save_backup(&self, record: &BackupRecord) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO backups (backup_id, app_id, volume_id, status, snapshot_method, \
             pause_ms, guest_flush, manifest_key, manifest_digest, logical_bytes, \
             uploaded_bytes, chunk_count, error, started_at, finished_at, snapshot_path) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(backup_id) DO UPDATE SET app_id = excluded.app_id, \
             volume_id = excluded.volume_id, status = excluded.status, \
             snapshot_method = excluded.snapshot_method, pause_ms = excluded.pause_ms, \
             guest_flush = excluded.guest_flush, manifest_key = excluded.manifest_key, \
             manifest_digest = excluded.manifest_digest, \
             logical_bytes = excluded.logical_bytes, uploaded_bytes = excluded.uploaded_bytes, \
             chunk_count = excluded.chunk_count, error = excluded.error, \
             started_at = excluded.started_at, finished_at = excluded.finished_at, \
             snapshot_path = excluded.snapshot_path",
        )
        .bind(&record.backup_id)
        .bind(&record.app_id)
        .bind(&record.volume_id)
        .bind(backup_state_str(record.status))
        .bind(record.snapshot_method.map(snapshot_method_str))
        .bind(record.pause_ms.and_then(|ms| i64::try_from(ms).ok()))
        .bind(record.guest_flush)
        .bind(record.manifest_key.as_deref())
        .bind(record.manifest_digest.as_deref())
        .bind(
            record
                .logical_bytes
                .and_then(|bytes| i64::try_from(bytes).ok()),
        )
        .bind(
            record
                .uploaded_bytes
                .and_then(|bytes| i64::try_from(bytes).ok()),
        )
        .bind(
            record
                .chunk_count
                .and_then(|count| i64::try_from(count).ok()),
        )
        .bind(record.error.as_deref())
        .bind(record.started_at.as_deref())
        .bind(record.finished_at.as_deref())
        .bind(record.snapshot_path.as_deref())
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Removes one backup job row, once its job left `spec.backups`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn delete_backup(&self, backup_id: &str) -> Result<(), AgentError> {
        sqlx::query("DELETE FROM backups WHERE backup_id = ?")
            .bind(backup_id)
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
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        upsert_lease(&mut tx, lease, highest).await?;
        tx.commit().await.map_err(store_error)
    }

    /// Records one self-fence (section 8.5 step 2).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unencodable VM list.
    pub async fn record_fence(&self, record: &FenceRecord) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        insert_fence(&mut tx, record).await?;
        tx.commit().await.map_err(store_error)
    }

    /// Records one finished self-fence together with the lease's row and its
    /// resource's highest token, its event, and the node's failed mark when
    /// the fence could not confirm every stop, in one transaction (section 8.5
    /// steps 2 and 3).
    ///
    /// A restart therefore finds all of them or none of them. With none, the
    /// lease is either still `revoked` or `expired` without a fence record,
    /// which the restore reads as an interrupted fence and runs again, or the
    /// revoke was never answered with success.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unencodable field.
    pub async fn finish_fence(
        &self,
        record: &FenceRecord,
        lease: Option<(&StoredLease, u64)>,
        evt_id: &str,
        event: &Value,
    ) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        if let Some((lease, highest)) = lease {
            upsert_lease(&mut tx, lease, highest).await?;
        }
        insert_fence(&mut tx, record).await?;
        if !record.complete {
            add_failed_fence(&mut tx, &record.lease_id.0, record.token).await?;
        }
        buffer_event(&mut tx, evt_id, event).await?;
        tx.commit().await.map_err(store_error)
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

    /// Marks the node failed for `lease_id`'s failed fence at `token` and
    /// buffers its `node.fence_failed` event, in one transaction: the
    /// fallback when the whole fence record could not be written (section
    /// 8.5). The mark and the list of failed fences always change together,
    /// so a `fence_ack` of other leases, or one below `token`, never clears a
    /// mark this fence set.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unencodable field.
    pub async fn mark_fence_failed(
        &self,
        lease_id: &str,
        token: u64,
        evt_id: &str,
        event: &Value,
    ) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        add_failed_fence(&mut tx, lease_id, token).await?;
        buffer_event(&mut tx, evt_id, event).await?;
        tx.commit().await.map_err(store_error)
    }

    /// Whether an upgrade left failed fences to report to the primary once.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn failed_fences_unreported(&self) -> Result<bool, AgentError> {
        Ok(self.meta(FAILED_FENCES_UNREPORTED).await?.is_some())
    }

    /// Buffers the `node.fence_failed` events that report an upgrade's failed
    /// fences and clears the flag, in one transaction, so they go out once.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unencodable event.
    pub async fn report_failed_fences(&self, events: &[(String, Value)]) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        for (evt_id, event) in events {
            buffer_event(&mut tx, evt_id, event).await?;
        }
        sqlx::query("DELETE FROM node_meta WHERE key = ?")
            .bind(FAILED_FENCES_UNREPORTED)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)
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

    /// The leases whose fence failed and no `fence_ack` cleared yet, each
    /// with the highest token a failed fence of it was recorded at (section 12
    /// `node_meta`).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unreadable list.
    pub async fn failed_fences(&self) -> Result<Vec<FailedFence>, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        let failed = read_failed_fences(&mut tx).await?;
        tx.commit().await.map_err(store_error)?;
        Ok(failed)
    }

    /// Applies a `fence_ack` (section 5.17): clears the failed fences of the
    /// listed leases, and the node's failed mark once none remains, in one
    /// transaction. Returns the failed fences that remain. A listed lease with
    /// no failed fence is not an error.
    ///
    /// With `fencing_tokens` (ADR 0245), a listed lease's failed fences are
    /// cleared only below its acknowledged token: a failed fence at or above
    /// it is one the operator has not seen, and stays. A listed lease missing
    /// from `fencing_tokens` keeps its failed fences; the caller refuses such
    /// an acknowledgement before it gets here. Without `fencing_tokens`, as a
    /// primary before M5b sends it, every failed fence of a listed lease is
    /// cleared.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unreadable list.
    pub async fn ack_fences(
        &self,
        lease_ids: &[String],
        fencing_tokens: Option<&BTreeMap<String, u64>>,
    ) -> Result<Vec<FailedFence>, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        let mut failed = read_failed_fences(&mut tx).await?;
        failed.retain(|fence| !acknowledges(lease_ids, fencing_tokens, fence));
        write_failed_fences(&mut tx, &failed).await?;
        if failed.is_empty() {
            sqlx::query(
                "INSERT INTO node_meta (key, value) VALUES (?, '0') \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(NODE_FAILED)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
        Ok(failed)
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

    /// The newest `(seq, hash)` of the node's copy of the change log, or
    /// `(0, ZERO_HASH)` when it holds none: what `hello` reports as `log_seq`
    /// and `log_hash` (section 5.1), and what the next `log_entries` must
    /// continue (section 7.1).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn log_position(&self) -> Result<(u64, String), AgentError> {
        let newest: Option<(i64, String)> =
            sqlx::query_as("SELECT seq, hash FROM change_log ORDER BY seq DESC LIMIT 1")
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        match newest {
            Some((seq, hash)) => Ok((stored_seq(seq)?, hash)),
            None => Ok((0, ZERO_HASH.to_owned())),
        }
    }

    /// Stores one verified batch of change-log entries (section 7.1) and,
    /// when the message carried one, the primary's `head_seq` with the time it
    /// arrived, in one transaction: all of it is stored, or nothing.
    ///
    /// The batch must continue the stored copy. A `seq` already stored fails
    /// its insert, and once the rows are in, the entry before the batch must
    /// be the one its first `prev_hash` names and none may follow it, so the
    /// copy stays one gap-free chain. The rows go in first, so the
    /// transaction holds the write lock before it reads anything.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the batch does not continue the copy or
    /// SQLite fails; nothing is stored then.
    pub async fn store_log_batch(
        &self,
        entries: &[LogEntry],
        head_seq: Option<u64>,
    ) -> Result<(), AgentError> {
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(());
        };
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        for entry in entries {
            sqlx::query(
                "INSERT INTO change_log (seq, term, time, actor_type, actor_id, entity_type, \
                 entity_id, op, payload, prev_hash, hash) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(seq_column(entry.seq)?)
            .bind(seq_column(entry.term)?)
            .bind(&entry.time)
            .bind(&entry.actor_type)
            .bind(&entry.actor_id)
            .bind(&entry.entity_type)
            .bind(&entry.entity_id)
            .bind(&entry.op)
            .bind(&entry.payload)
            .bind(&entry.prev_hash)
            .bind(&entry.hash)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        let before: Option<String> = if first.seq <= 1 {
            Some(ZERO_HASH.to_owned())
        } else {
            sqlx::query_scalar("SELECT hash FROM change_log WHERE seq = ?")
                .bind(seq_column(first.seq - 1)?)
                .fetch_optional(&mut *tx)
                .await
                .map_err(store_error)?
        };
        let newest: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
            .fetch_one(&mut *tx)
            .await
            .map_err(store_error)?;
        if before.as_deref() != Some(first.prev_hash.as_str())
            || newest != Some(seq_column(last.seq)?)
        {
            return Err(AgentError::Store(format!(
                "change-log entries {} to {} do not continue the stored copy",
                first.seq, last.seq
            )));
        }
        if let Some(head_seq) = head_seq {
            for (key, value) in [
                (LOG_HEAD_SEQ, head_seq.to_string()),
                (LOG_HEAD_SEQ_AT, util::now_rfc3339()),
            ] {
                sqlx::query(
                    "INSERT INTO node_meta (key, value) VALUES (?, ?) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                )
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await
                .map_err(store_error)?;
            }
        }
        tx.commit().await.map_err(store_error)
    }

    /// The primary's newest `seq` as its last `log_entries` reported it, and
    /// when the node received it (section 12 `change_log`); `None` before
    /// any `log_entries` carried a `head_seq`, and after a quarantine.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a value that is not a
    /// number.
    pub async fn log_head(&self) -> Result<Option<(u64, String)>, AgentError> {
        let Some(seq) = self.meta(LOG_HEAD_SEQ).await? else {
            return Ok(None);
        };
        let seq = seq
            .parse::<u64>()
            .map_err(|_| AgentError::Store(format!("the stored head_seq {seq:?} is no number")))?;
        let at = self.meta(LOG_HEAD_SEQ_AT).await?.unwrap_or_default();
        Ok(Some((seq, at)))
    }

    /// Every entry of the node's copy of the change log, in `seq` order.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn log_entries(&self) -> Result<Vec<LogEntry>, AgentError> {
        let rows = sqlx::query(
            "SELECT seq, term, time, actor_type, actor_id, entity_type, entity_id, op, \
             payload, prev_hash, hash FROM change_log ORDER BY seq",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(LogEntry {
                    seq: stored_seq(row.try_get("seq").map_err(store_error)?)?,
                    term: stored_seq(row.try_get("term").map_err(store_error)?)?,
                    time: row.try_get("time").map_err(store_error)?,
                    actor_type: row.try_get("actor_type").map_err(store_error)?,
                    actor_id: row.try_get("actor_id").map_err(store_error)?,
                    entity_type: row.try_get("entity_type").map_err(store_error)?,
                    entity_id: row.try_get("entity_id").map_err(store_error)?,
                    op: row.try_get("op").map_err(store_error)?,
                    payload: row.try_get("payload").map_err(store_error)?,
                    prev_hash: row.try_get("prev_hash").map_err(store_error)?,
                    hash: row.try_get("hash").map_err(store_error)?,
                })
            })
            .collect()
    }

    /// Moves the node's copy of the change log aside after `log_diverged`
    /// (section 7.1): divergent local writes are never silently dropped
    /// (D-0022).
    ///
    /// Every stored entry is written, in `seq` order, as one JSON object per
    /// line with the fields of section 5.14 (see [`log_copy_line`]), to a new
    /// `0600` file under `<data_dir>/quarantine/`, never over an existing one.
    /// The file and the directory are synced, and only then, in one
    /// transaction, the copy is recorded in `quarantine`, the `change_log`
    /// rows it holds are deleted and the primary's `head_seq` is cleared.
    /// Returns the file, or `None` when the copy is empty, which needs no
    /// file, so a second call does nothing.
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the file cannot be written and synced, and
    /// [`AgentError::Store`] on a SQLite failure or when the rows to delete
    /// are not the ones exported. Nothing is deleted then.
    pub async fn quarantine_log(&self, reason: &str) -> Result<Option<PathBuf>, AgentError> {
        let entries = self.log_entries().await?;
        let Some(last) = entries.last() else {
            return Ok(None);
        };
        let last_seq = seq_column(last.seq)?;
        let last_hash = last.hash.clone();
        let exported = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        let dir = self.data_dir.join(QUARANTINE_DIR);
        let stamp = util::rfc3339_nanos(util::now_ns()).replace(['-', ':'], "");
        let path = tokio::task::spawn_blocking(move || write_log_copy(&dir, &stamp, &entries))
            .await
            .map_err(|e| AgentError::Store(format!("the quarantine writer stopped: {e}")))??;

        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query(
            "INSERT INTO quarantine (kind, path, quarantined_at, reason, last_seq, last_hash) \
             VALUES ('change_log', ?, ?, ?, ?, ?)",
        )
        .bind(path.to_string_lossy().into_owned())
        .bind(util::now_rfc3339())
        .bind(reason)
        .bind(last_seq)
        .bind(&last_hash)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        let deleted = sqlx::query("DELETE FROM change_log WHERE seq <= ?")
            .bind(last_seq)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?
            .rows_affected();
        if deleted != exported {
            // Dropping the transaction rolls it back: the copy stays.
            return Err(AgentError::Store(format!(
                "the change log changed while it was quarantined: {exported} entries were \
                 exported, {deleted} would be deleted"
            )));
        }
        sqlx::query("DELETE FROM node_meta WHERE key IN (?, ?)")
            .bind(LOG_HEAD_SEQ)
            .bind(LOG_HEAD_SEQ_AT)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)?;
        Ok(Some(path))
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

/// The directory under the data directory that holds quarantined copies of
/// the change log (section 7.1).
const QUARANTINE_DIR: &str = "quarantine";

/// A change-log `seq` or `term` as SQLite stores it. A verified entry's are
/// below 2^53, since RFC 8785 refuses larger integers.
fn seq_column(value: u64) -> Result<i64, AgentError> {
    i64::try_from(value)
        .map_err(|_| AgentError::Store(format!("change-log number {value} does not fit SQLite")))
}

/// A stored change-log `seq` or `term`, which is never negative.
fn stored_seq(value: i64) -> Result<u64, AgentError> {
    u64::try_from(value)
        .map_err(|_| AgentError::Store(format!("the stored change-log number {value} is negative")))
}

/// Writes `entries` as one JSON object per line ([`log_copy_line`]) to a new
/// file in `dir`, named after `stamp`, and syncs it and `dir` (section 7.1).
/// A file that could not be completed is removed: it is not a copy.
fn write_log_copy(dir: &Path, stamp: &str, entries: &[LogEntry]) -> Result<PathBuf, AgentError> {
    use std::io::Write as _;

    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    match builder.create(dir) {
        Ok(()) => {
            // The new directory's own entry must survive a crash too.
            if let Some(parent) = dir.parent() {
                sync_dir(parent).map_err(|e| AgentError::io("sync the data directory", e))?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(AgentError::io("create the quarantine directory", e)),
    }
    let (path, file) = create_log_copy_file(dir, stamp)
        .map_err(|e| AgentError::io("create the quarantine file", e))?;
    let written = (|| -> std::io::Result<()> {
        let mut out = std::io::BufWriter::new(file);
        for entry in entries {
            let line = log_copy_line(entry).map_err(std::io::Error::other)?;
            out.write_all(line.as_bytes())?;
            out.write_all(b"\n")?;
        }
        out.into_inner()
            .map_err(std::io::IntoInnerError::into_error)?
            .sync_all()?;
        sync_dir(dir)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&path);
        return Err(AgentError::io("write the quarantined change log", e));
    }
    Ok(path)
}

/// One line of a quarantined copy: the entry as the JSON object of section
/// 5.14, members in RFC 8785 order, with the stored canonical payload text
/// spliced in as it is. The payload is never parsed and serialized again: a
/// whole-number float above 2^53 the primary hashed as a plain integer would
/// have no canonical form the second time, and the copy must still be
/// written.
///
/// # Errors
///
/// A stored payload that is not JSON, which only a tampered store holds; the
/// line would not be JSON either.
fn log_copy_line(entry: &LogEntry) -> Result<String, serde_json::Error> {
    serde_json::from_str::<serde::de::IgnoredAny>(&entry.payload)?;
    let text = |value: &str| serde_json::to_string(value);
    // Every key is ASCII, so this order is the UTF-16 order RFC 8785 sorts by.
    Ok(format!(
        "{{\"actor_id\":{},\"actor_type\":{},\"entity_id\":{},\"entity_type\":{},\
         \"hash\":{},\"op\":{},\"payload\":{},\"prev_hash\":{},\"seq\":{},\"term\":{},\
         \"time\":{}}}",
        text(&entry.actor_id)?,
        text(&entry.actor_type)?,
        text(&entry.entity_id)?,
        text(&entry.entity_type)?,
        text(&entry.hash)?,
        text(&entry.op)?,
        entry.payload,
        text(&entry.prev_hash)?,
        entry.seq,
        entry.term,
        text(&entry.time)?,
    ))
}

/// Creates `change_log-<stamp>.jsonl` in `dir`, mode `0600`, or the same name
/// with a counter when it exists: an existing file is never opened.
fn create_log_copy_file(dir: &Path, stamp: &str) -> std::io::Result<(PathBuf, std::fs::File)> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    for attempt in 0..1_000_u32 {
        let name = if attempt == 0 {
            format!("change_log-{stamp}.jsonl")
        } else {
            format!("change_log-{stamp}-{attempt}.jsonl")
        };
        let path = dir.join(name);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "every quarantine file name for this time is taken",
    ))
}

/// Syncs a directory, so the entries created in it survive a crash.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
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

fn backup_state_str(state: BackupState) -> &'static str {
    match state {
        BackupState::Queued => "queued",
        BackupState::Running => "running",
        BackupState::Succeeded => "succeeded",
        BackupState::Failed => "failed",
    }
}

fn parse_backup_state(text: &str) -> Result<BackupState, AgentError> {
    match text {
        "queued" => Ok(BackupState::Queued),
        "running" => Ok(BackupState::Running),
        "succeeded" => Ok(BackupState::Succeeded),
        "failed" => Ok(BackupState::Failed),
        other => Err(AgentError::Store(format!(
            "a stored backup has an unknown status {other:?}"
        ))),
    }
}

fn snapshot_method_str(method: SnapshotMethod) -> &'static str {
    match method {
        SnapshotMethod::Reflink => "reflink",
        SnapshotMethod::PauseCopy => "pause_copy",
    }
}

fn parse_snapshot_method(text: &str) -> Result<SnapshotMethod, AgentError> {
    match text {
        "reflink" => Ok(SnapshotMethod::Reflink),
        "pause_copy" => Ok(SnapshotMethod::PauseCopy),
        other => Err(AgentError::Store(format!(
            "a stored backup has an unknown snapshot method {other:?}"
        ))),
    }
}

fn backup_record(row: sqlx::sqlite::SqliteRow) -> Result<BackupRecord, AgentError> {
    let status: String = row.try_get("status").map_err(store_error)?;
    let method: Option<String> = row.try_get("snapshot_method").map_err(store_error)?;
    let pause_ms: Option<i64> = row.try_get("pause_ms").map_err(store_error)?;
    let guest_flush: Option<bool> = row.try_get("guest_flush").map_err(store_error)?;
    let logical_bytes: Option<i64> = row.try_get("logical_bytes").map_err(store_error)?;
    let uploaded_bytes: Option<i64> = row.try_get("uploaded_bytes").map_err(store_error)?;
    let chunk_count: Option<i64> = row.try_get("chunk_count").map_err(store_error)?;
    Ok(BackupRecord {
        backup_id: row.try_get("backup_id").map_err(store_error)?,
        app_id: row.try_get("app_id").map_err(store_error)?,
        volume_id: row.try_get("volume_id").map_err(store_error)?,
        status: parse_backup_state(&status)?,
        snapshot_method: method.as_deref().map(parse_snapshot_method).transpose()?,
        pause_ms: pause_ms.and_then(|ms| u64::try_from(ms).ok()),
        guest_flush,
        manifest_key: row.try_get("manifest_key").map_err(store_error)?,
        manifest_digest: row.try_get("manifest_digest").map_err(store_error)?,
        logical_bytes: logical_bytes.and_then(|bytes| u64::try_from(bytes).ok()),
        uploaded_bytes: uploaded_bytes.and_then(|bytes| u64::try_from(bytes).ok()),
        chunk_count: chunk_count.and_then(|count| u64::try_from(count).ok()),
        error: row.try_get("error").map_err(store_error)?,
        started_at: row.try_get("started_at").map_err(store_error)?,
        finished_at: row.try_get("finished_at").map_err(store_error)?,
        snapshot_path: row.try_get("snapshot_path").map_err(store_error)?,
    })
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

/// Reads one `kept_snapshots` row back into a [`KeptSnapshotRecord`].
fn kept_snapshot_record(row: sqlx::sqlite::SqliteRow) -> Result<KeptSnapshotRecord, AgentError> {
    let seq: Option<i64> = row.try_get("seq").map_err(store_error)?;
    let size_bytes: i64 = row.try_get("size_bytes").map_err(store_error)?;
    let created_at_ms: i64 = row.try_get("created_at_ms").map_err(store_error)?;
    let pause_ms: i64 = row.try_get("pause_ms").map_err(store_error)?;
    let method: String = row.try_get("method").map_err(store_error)?;
    let location: String = row.try_get("location").map_err(store_error)?;
    let volume_id: String = row.try_get("volume_id").map_err(store_error)?;
    let method = match method.as_str() {
        "reflink" => quoro_engine::SnapshotMethod::Reflink,
        "pause_copy" => quoro_engine::SnapshotMethod::PauseCopy,
        other => {
            return Err(AgentError::Store(format!(
                "unknown snapshot method {other:?}"
            )));
        }
    };
    Ok(KeptSnapshotRecord {
        app_id: row.try_get("app_id").map_err(store_error)?,
        snapshot: quoro_engine::VolumeSnapshot {
            volume_id: quoro_engine::VolumeId(volume_id),
            snapshot_id: row.try_get("snapshot_id").map_err(store_error)?,
            created_at: std::time::UNIX_EPOCH
                + Duration::from_millis(u64::try_from(created_at_ms).unwrap_or(0)),
            size_bytes: u64::try_from(size_bytes).unwrap_or(0),
            location: std::path::PathBuf::from(location),
            method,
            pause: Duration::from_millis(u64::try_from(pause_ms).unwrap_or(0)),
            // The store keeps no flush outcome for a kept snapshot, and
            // nothing reads it from one: only a backup reports it.
            guest_flush: quoro_engine::GuestFlush::Unsupported,
        },
        purpose: row.try_get("purpose").map_err(store_error)?,
        seq: seq.and_then(|seq| u64::try_from(seq).ok()),
        cells: row.try_get("cells").map_err(store_error)?,
    })
}

fn warm_target_record(row: sqlx::sqlite::SqliteRow) -> Result<WarmTargetRecord, AgentError> {
    let u64_of = |name: &str| -> Result<Option<u64>, AgentError> {
        let value: Option<i64> = row.try_get(name).map_err(store_error)?;
        Ok(value.and_then(|value| u64::try_from(value).ok()))
    };
    let size_gb: i64 = row.try_get("size_gb").map_err(store_error)?;
    let pending_seq = u64_of("pending_seq")?;
    let pending_taken_at: Option<String> = row.try_get("pending_taken_at").map_err(store_error)?;
    let pending_root: Option<String> = row.try_get("pending_root").map_err(store_error)?;
    let pending_cells: Option<Vec<u8>> = row.try_get("pending_cells").map_err(store_error)?;
    let pending = match (pending_seq, pending_taken_at, pending_root, pending_cells) {
        (Some(seq), Some(taken_at), Some(root), Some(cells)) => Some(PendingCommit {
            seq,
            taken_at,
            root,
            cells,
        }),
        (None, None, None, None) => None,
        _ => {
            return Err(AgentError::Store(
                "a warm target row holds part of a pending copy".to_owned(),
            ));
        }
    };
    Ok(WarmTargetRecord {
        volume_id: row.try_get("volume_id").map_err(store_error)?,
        app_id: row.try_get("app_id").map_err(store_error)?,
        size_gb: u32::try_from(size_gb).unwrap_or(0),
        complete_seq: u64_of("complete_seq")?,
        taken_at: row.try_get("taken_at").map_err(store_error)?,
        completed_at: row.try_get("completed_at").map_err(store_error)?,
        allocated_bytes: u64_of("allocated_bytes")?,
        cells: row.try_get("cells").map_err(store_error)?,
        highest_token: u64_of("highest_token")?.unwrap_or(0),
        rejected_token: u64_of("rejected_token")?,
        rejected_at: row.try_get("rejected_at").map_err(store_error)?,
        pending,
        error: row.try_get("error").map_err(store_error)?,
        removed: u64_of("removed")?.unwrap_or(0) != 0,
    })
}

fn engine_method_str(method: quoro_engine::SnapshotMethod) -> &'static str {
    match method {
        quoro_engine::SnapshotMethod::Reflink => "reflink",
        quoro_engine::SnapshotMethod::PauseCopy => "pause_copy",
    }
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
    let validity_boot_id: Option<String> = row.try_get("validity_boot_id").map_err(store_error)?;
    let valid_until_ns: Option<i64> = row
        .try_get("valid_until_boottime_ns")
        .map_err(store_error)?;
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
        validity: match (validity_boot_id, valid_until_ns) {
            (Some(boot_id), Some(ns)) => u64::try_from(ns).ok().map(|ns| StoredValidity {
                boot_id,
                until_boottime: Duration::from_nanos(ns),
            }),
            _ => None,
        },
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

/// Inserts or replaces one lease row and raises its resource's highest token
/// inside `tx` (section 12 `leases`, section 8.6).
async fn upsert_lease(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    lease: &StoredLease,
    highest: u64,
) -> Result<(), AgentError> {
    let duration_ms = lease
        .duration
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
    // A validity that does not fit is not stored: the restart then fences
    // the lease, the safe side.
    let validity = lease.validity.as_ref().and_then(|validity| {
        i64::try_from(validity.until_boottime.as_nanos())
            .ok()
            .map(|ns| (validity.boot_id.as_str(), ns))
    });
    sqlx::query(
        "INSERT INTO leases (lease_id, resource_type, resource_id, mode, token, state, \
         duration_ms, validity_boot_id, valid_until_boottime_ns) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(lease_id) DO UPDATE SET resource_type = excluded.resource_type, \
         resource_id = excluded.resource_id, mode = excluded.mode, token = excluded.token, \
         state = excluded.state, duration_ms = excluded.duration_ms, \
         validity_boot_id = excluded.validity_boot_id, \
         valid_until_boottime_ns = excluded.valid_until_boottime_ns",
    )
    .bind(&lease.lease_id.0)
    .bind(resource_type_str(lease.resource_type))
    .bind(&lease.resource_id)
    .bind(lease_mode_str(lease.mode))
    .bind(i64::try_from(lease.token).unwrap_or(i64::MAX))
    .bind(lease_state_str(lease.state))
    .bind(duration_ms)
    .bind(validity.map(|(boot_id, _)| boot_id))
    .bind(validity.map(|(_, ns)| ns))
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    sqlx::query(
        "INSERT INTO lease_tokens (resource_type, resource_id, token) VALUES (?, ?, ?) \
         ON CONFLICT(resource_type, resource_id) DO UPDATE SET token = MAX(token, excluded.token)",
    )
    .bind(resource_type_str(lease.resource_type))
    .bind(&lease.resource_id)
    .bind(i64::try_from(highest).unwrap_or(i64::MAX))
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    Ok(())
}

/// Whether a `fence_ack` of `lease_ids`, with `fencing_tokens` when the
/// primary sent them, clears `fence` (section 5.17, ADR 0245).
#[must_use]
pub fn acknowledges(
    lease_ids: &[String],
    fencing_tokens: Option<&BTreeMap<String, u64>>,
    fence: &FailedFence,
) -> bool {
    if !lease_ids.contains(&fence.lease_id) {
        return false;
    }
    match fencing_tokens {
        None => true,
        Some(tokens) => tokens
            .get(&fence.lease_id)
            .is_some_and(|acked| fence.fencing_token < *acked),
    }
}

/// Sets the failed mark and adds a failed fence of `lease_id` at `token`,
/// inside `tx`. A lease listed already keeps one entry, at the higher token.
async fn add_failed_fence(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    lease_id: &str,
    token: u64,
) -> Result<(), AgentError> {
    sqlx::query(
        "INSERT INTO node_meta (key, value) VALUES (?, '1') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(NODE_FAILED)
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    let mut failed = read_failed_fences(tx).await?;
    match failed.iter_mut().find(|fence| fence.lease_id == lease_id) {
        Some(fence) => fence.fencing_token = fence.fencing_token.max(token),
        None => failed.push(FailedFence {
            lease_id: lease_id.to_owned(),
            fencing_token: token,
        }),
    }
    write_failed_fences(tx, &failed).await
}

/// The failed fences in `node_meta`, inside `tx`. A lease without a stored
/// token, which only an agent before ADR 0245 leaves, takes
/// [`estimated_token`].
async fn read_failed_fences(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Vec<FailedFence>, AgentError> {
    let (lease_ids, tokens) = read_failed_lists(tx).await?;
    let mut failed: Vec<FailedFence> = Vec::with_capacity(lease_ids.len());
    for lease_id in lease_ids {
        if failed.iter().any(|fence| fence.lease_id == lease_id) {
            continue;
        }
        let fencing_token = match tokens.get(&lease_id) {
            Some(token) => *token,
            None => estimated_token(tx, &lease_id).await?,
        };
        failed.push(FailedFence {
            lease_id,
            fencing_token,
        });
    }
    Ok(failed)
}

/// The two `node_meta` values behind the failed fences, as stored.
async fn read_failed_lists(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(Vec<String>, BTreeMap<String, u64>), AgentError> {
    let mut values = Vec::with_capacity(2);
    for key in [FAILED_FENCES, FAILED_FENCE_TOKENS] {
        let text: Option<String> = sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(store_error)?;
        values.push(text);
    }
    let unreadable =
        |e: serde_json::Error| AgentError::Store(format!("could not read the failed fences: {e}"));
    let tokens = values.pop().flatten();
    let lease_ids = values.pop().flatten();
    Ok((
        lease_ids
            .map(|text| serde_json::from_str(&text).map_err(unreadable))
            .transpose()?
            .unwrap_or_default(),
        tokens
            .map(|text| serde_json::from_str(&text).map_err(unreadable))
            .transpose()?
            .unwrap_or_default(),
    ))
}

/// The token of a failed fence of `lease_id` that no token was stored for:
/// the highest of its incomplete fence records and the token the node holds
/// the lease at, which is at least the one a fence of it was recorded at, or
/// 0 when the store has neither. A token above the real one is the safe
/// side: an acknowledgement then clears nothing the real token would keep.
async fn estimated_token(
    conn: &mut sqlx::SqliteConnection,
    lease_id: &str,
) -> Result<u64, AgentError> {
    let recorded = recorded_token(conn, lease_id).await?;
    let held: Option<i64> = sqlx::query_scalar("SELECT token FROM leases WHERE lease_id = ?")
        .bind(lease_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(store_error)?;
    Ok(recorded.max(held.map_or(0, |token| u64::try_from(token).unwrap_or(0))))
}

/// The highest token an incomplete fence record of `lease_id` has, or 0.
async fn recorded_token(
    conn: &mut sqlx::SqliteConnection,
    lease_id: &str,
) -> Result<u64, AgentError> {
    let recorded: Option<i64> =
        sqlx::query_scalar("SELECT MAX(token) FROM fences WHERE lease_id = ? AND complete = 0")
            .bind(lease_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(store_error)?;
    Ok(recorded.map_or(0, |token| u64::try_from(token).unwrap_or(0)))
}

/// Replaces the failed fences in `node_meta`, inside `tx`: the lease ids in
/// the form an agent before ADR 0245 reads, and their tokens beside them.
async fn write_failed_fences(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    failed: &[FailedFence],
) -> Result<(), AgentError> {
    let lease_ids: Vec<&str> = failed.iter().map(|fence| fence.lease_id.as_str()).collect();
    let tokens: BTreeMap<&str, u64> = failed
        .iter()
        .map(|fence| (fence.lease_id.as_str(), fence.fencing_token))
        .collect();
    let encode = |e: serde_json::Error| {
        AgentError::Store(format!("could not encode the failed fences: {e}"))
    };
    for (key, text) in [
        (
            FAILED_FENCES,
            serde_json::to_string(&lease_ids).map_err(encode)?,
        ),
        (
            FAILED_FENCE_TOKENS,
            serde_json::to_string(&tokens).map_err(encode)?,
        ),
    ] {
        sqlx::query(
            "INSERT INTO node_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(text)
        .execute(&mut **tx)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

/// Inserts one fence record inside `tx` (section 8.5 step 2).
async fn insert_fence(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &FenceRecord,
) -> Result<(), AgentError> {
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
    .execute(&mut **tx)
    .await
    .map_err(store_error)
    .map(|_| ())
}

/// Buffers one event inside `tx`, dropping the oldest entries when the buffer
/// is full and counting each drop (section 6.5).
async fn buffer_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    evt_id: &str,
    body: &Value,
) -> Result<(), AgentError> {
    let text = serde_json::to_string(body)
        .map_err(|e| AgentError::Store(format!("could not encode an event: {e}")))?;
    sqlx::query("INSERT OR IGNORE INTO event_buffer (evt_id, body) VALUES (?, ?)")
        .bind(evt_id)
        .bind(text)
        .execute(&mut **tx)
        .await
        .map_err(store_error)?;
    let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM event_buffer")
        .fetch_one(&mut **tx)
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
        .execute(&mut **tx)
        .await
        .map_err(store_error)?;
        let dropped = sqlx::query("SELECT value FROM node_meta WHERE key = ?")
            .bind(DROPPED_EVENTS)
            .fetch_optional(&mut **tx)
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
        .execute(&mut **tx)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

/// Adds the columns newer agents need to a database an older agent created:
/// `desired_state.body_enc` (bodies encrypted), the two validity columns of
/// `leases` and `backups.guest_flush` (M5b, ADR 0282). New databases get them
/// from [`SCHEMA`]; existing ones are altered once, and the old rows keep
/// loading.
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

    // A held `expiring` lease's validity on `CLOCK_BOOTTIME` (section 8.4
    // "Agent restart", ADR 0072); rows written before it read as none.
    let columns = sqlx::query("PRAGMA table_info(leases)")
        .fetch_all(pool)
        .await
        .map_err(store_error)?;
    let names: Vec<String> = columns
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect();
    for (column, kind) in [
        ("validity_boot_id", "TEXT"),
        ("valid_until_boottime_ns", "INTEGER"),
    ] {
        if !names.iter().any(|name| name == column) {
            sqlx::query(&format!("ALTER TABLE leases ADD COLUMN {column} {kind}"))
                .execute(pool)
                .await
                .map_err(store_error)?;
        }
    }

    // Whether a backup's snapshot followed a guest flush (section 5.3, ADR
    // 0282); rows written before it read as unknown.
    let columns = sqlx::query("PRAGMA table_info(backups)")
        .fetch_all(pool)
        .await
        .map_err(store_error)?;
    let has_flush = columns.iter().any(|row| {
        row.try_get::<String, _>("name")
            .is_ok_and(|name| name == "guest_flush")
    });
    if !has_flush {
        sqlx::query("ALTER TABLE backups ADD COLUMN guest_flush INTEGER")
            .execute(pool)
            .await
            .map_err(store_error)?;
    }

    // A node an older agent marked failed has no list of the failed fences
    // the mark covers: they are the incomplete fence records (section 12
    // `node_meta`), so a `fence_ack` can clear them. The older primary never
    // listed them, so the agent reports them once (`node.fence_failed`).
    let failed: Option<String> = sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
        .bind(NODE_FAILED)
        .fetch_optional(pool)
        .await
        .map_err(store_error)?;
    let listed: Option<String> = sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
        .bind(FAILED_FENCES)
        .fetch_optional(pool)
        .await
        .map_err(store_error)?;
    if failed.as_deref() == Some("1") && listed.is_none() {
        // Each with the token of its latest failed fence: tokens only grow,
        // and an older failure of the lease was acknowledged before it.
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT lease_id, MAX(token) FROM fences WHERE complete = 0 \
             GROUP BY lease_id ORDER BY lease_id",
        )
        .fetch_all(pool)
        .await
        .map_err(store_error)?;
        let fences: Vec<FailedFence> = rows
            .into_iter()
            .map(|(lease_id, token)| FailedFence {
                lease_id,
                fencing_token: u64::try_from(token).unwrap_or(0),
            })
            .collect();
        let mut tx = pool.begin().await.map_err(store_error)?;
        write_failed_fences(&mut tx, &fences).await?;
        if !fences.is_empty() {
            sqlx::query("INSERT OR REPLACE INTO node_meta (key, value) VALUES (?, '1')")
                .bind(FAILED_FENCES_UNREPORTED)
                .execute(&mut *tx)
                .await
                .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
    } else if listed.is_some() {
        // The tokens beside the list are brought in line with it. An agent
        // before ADR 0245 wrote the list alone: a lease it listed has no
        // token, and takes `estimated_token`. One it cleared and listed again,
        // after a rollback to it, keeps the token stored before; its new
        // failed fence's record, which is at least as high, wins over that
        // token. A token for a lease no longer listed is dropped.
        //
        // An estimated token can be above the one an acknowledgement the
        // primary already recorded carries: the lease was granted to the node
        // again before that acknowledgement arrived. The acknowledgement then
        // clears nothing, and the primary, which cleared its `fence_failure`,
        // would list nothing more. So the agent reports these failed fences
        // once, as for an older failed mark: the primary lists them again,
        // and an acknowledgement recorded after that carries a higher token.
        let mut conn = pool.acquire().await.map_err(store_error)?;
        let (lease_ids, tokens) = read_failed_lists(&mut conn).await?;
        let mut fences: Vec<FailedFence> = Vec::with_capacity(lease_ids.len());
        let mut estimated = false;
        for lease_id in lease_ids {
            if fences.iter().any(|fence| fence.lease_id == lease_id) {
                continue;
            }
            let fencing_token = match tokens.get(&lease_id) {
                Some(token) => (*token).max(recorded_token(&mut conn, &lease_id).await?),
                None => {
                    estimated = true;
                    estimated_token(&mut conn, &lease_id).await?
                }
            };
            fences.push(FailedFence {
                lease_id,
                fencing_token,
            });
        }
        drop(conn);
        let mut tx = pool.begin().await.map_err(store_error)?;
        write_failed_fences(&mut tx, &fences).await?;
        if estimated {
            sqlx::query("INSERT OR REPLACE INTO node_meta (key, value) VALUES (?, '1')")
                .bind(FAILED_FENCES_UNREPORTED)
                .execute(&mut *tx)
                .await
                .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
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
    use quoro_proto::jcs;
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

    fn running_backup(backup_id: &str, guest_flush: Option<bool>) -> BackupRecord {
        BackupRecord {
            backup_id: backup_id.to_owned(),
            app_id: "app_1".to_owned(),
            volume_id: "vol_1".to_owned(),
            status: BackupState::Running,
            snapshot_method: Some(SnapshotMethod::PauseCopy),
            pause_ms: Some(120),
            guest_flush,
            manifest_key: None,
            manifest_digest: None,
            logical_bytes: None,
            uploaded_bytes: None,
            chunk_count: None,
            error: None,
            started_at: Some("2026-11-09T03:00:00Z".to_owned()),
            finished_at: None,
            snapshot_path: Some("/var/lib/quoro/volumes/vol_1.snap-1".to_owned()),
        }
    }

    /// Section 5.3, ADR 0282: a backup row keeps whether its snapshot followed
    /// a guest flush, `true`, `false` or unknown, and reports it.
    #[tokio::test]
    async fn a_backup_row_keeps_its_guest_flush() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        for (backup_id, guest_flush) in [
            ("bkp_1", Some(true)),
            ("bkp_2", Some(false)),
            ("bkp_3", None),
        ] {
            let record = running_backup(backup_id, guest_flush);
            store.save_backup(&record).await.unwrap();
            let loaded = store.backup(backup_id).await.unwrap().unwrap();
            assert_eq!(loaded, record);
            assert_eq!(loaded.to_status().guest_flush, guest_flush);
        }

        // The upsert replaces it as the row moves on.
        let mut record = running_backup("bkp_3", Some(false));
        store.save_backup(&record).await.unwrap();
        record.status = BackupState::Succeeded;
        record.guest_flush = Some(true);
        store.save_backup(&record).await.unwrap();
        assert_eq!(
            store.backup("bkp_3").await.unwrap().unwrap().guest_flush,
            Some(true)
        );
        let flushes: Vec<Option<bool>> = store
            .backups()
            .await
            .unwrap()
            .iter()
            .map(|row| row.guest_flush)
            .collect();
        assert_eq!(flushes, vec![Some(true), Some(false), Some(true)]);
    }

    /// Section 12 `backups`: a database an agent older than M5b created gains
    /// the `guest_flush` column, its rows load with none, and a new row
    /// stores the value.
    #[tokio::test]
    async fn the_backup_table_of_an_older_agent_gains_the_guest_flush_column() {
        let dir = TempDir::new();
        {
            let options = SqliteConnectOptions::new()
                .filename(dir.0.join("node.db"))
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(options).await.unwrap();
            sqlx::raw_sql(
                "CREATE TABLE backups (backup_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, \
                 volume_id TEXT NOT NULL, status TEXT NOT NULL, snapshot_method TEXT, \
                 pause_ms INTEGER, manifest_key TEXT, manifest_digest TEXT, \
                 logical_bytes INTEGER, uploaded_bytes INTEGER, chunk_count INTEGER, \
                 error TEXT, started_at TEXT, finished_at TEXT, snapshot_path TEXT); \
                 INSERT INTO backups (backup_id, app_id, volume_id, status, \
                 snapshot_method, pause_ms, started_at) VALUES ('bkp_old', 'app_1', \
                 'vol_1', 'running', 'reflink', NULL, '2026-11-05T03:00:00Z');",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }
        let store = NodeStore::open(&dir.0).await.expect("the store opens");
        let old = store.backup("bkp_old").await.unwrap().unwrap();
        assert_eq!(old.status, BackupState::Running);
        assert_eq!(old.snapshot_method, Some(SnapshotMethod::Reflink));
        assert_eq!(old.guest_flush, None);
        assert_eq!(old.to_status().guest_flush, None);

        let new = running_backup("bkp_new", Some(false));
        store.save_backup(&new).await.unwrap();
        assert_eq!(store.backup("bkp_new").await.unwrap(), Some(new));

        // A second start finds the column and alters nothing.
        drop(store);
        let store = NodeStore::open(&dir.0).await.expect("the store reopens");
        assert_eq!(
            store.backup("bkp_new").await.unwrap().unwrap().guest_flush,
            Some(false)
        );
    }

    /// Verified entries `from_seq..from_seq + count`, chained on `prev_hash`.
    fn verified(from_seq: u64, prev_hash: &str, count: u64, salt: &str) -> Vec<LogEntry> {
        let entries = crate::follow::testing::chain(from_seq, prev_hash, count, salt);
        crate::follow::verify_batch(
            from_seq - 1,
            prev_hash,
            &crate::follow::testing::message(entries),
        )
        .unwrap()
    }

    /// The `quarantine` rows, oldest first: kind, path, reason, last seq and
    /// hash.
    async fn quarantine_rows(
        store: &NodeStore,
    ) -> Vec<(String, String, String, Option<i64>, Option<String>)> {
        sqlx::query_as("SELECT kind, path, reason, last_seq, last_hash FROM quarantine ORDER BY id")
            .fetch_all(store.pool())
            .await
            .unwrap()
    }

    /// Section 7.1: a batch is stored whole or not at all. The second batch
    /// continues the copy but repeats seq 4 in its last entry, so its insert
    /// fails after seq 4 and 5 went in; neither stays, nor does its
    /// `head_seq`.
    #[tokio::test]
    async fn a_log_batch_commits_atomically() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let first = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&first, Some(3)).await.unwrap();
        assert_eq!(store.log_entries().await.unwrap(), first);
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 3);

        let mut second = verified(4, &first[2].hash, 2, "a");
        second.push(second[0].clone());
        assert!(store.store_log_batch(&second, Some(9)).await.is_err());
        assert_eq!(
            store.log_position().await.unwrap(),
            (3, first[2].hash.clone())
        );
        assert_eq!(store.log_entries().await.unwrap(), first);
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 3);

        // A batch that does not continue the copy stores nothing either.
        let other = verified(4, &"1".repeat(64), 2, "a");
        assert!(store.store_log_batch(&other, Some(9)).await.is_err());
        let gap = verified(5, &first[2].hash, 1, "a");
        assert!(store.store_log_batch(&gap, Some(9)).await.is_err());
        assert_eq!(store.log_entries().await.unwrap(), first);
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 3);

        // The batch that does continue it goes in, without a `head_seq`.
        let second = verified(4, &first[2].hash, 2, "a");
        store.store_log_batch(&second, None).await.unwrap();
        assert_eq!(store.log_entries().await.unwrap().len(), 5);
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 3);
    }

    /// Section 5.1: the position `hello` reports is the newest stored entry,
    /// 0 and 64 zeros before any, and it survives a reopen.
    #[tokio::test]
    async fn the_log_position_survives_a_reopen() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store.log_position().await.unwrap(),
            (0, ZERO_HASH.to_owned())
        );
        assert_eq!(store.log_head().await.unwrap(), None);
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(7)).await.unwrap();
        drop(store);

        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store.log_position().await.unwrap(),
            (3, entries[2].hash.clone())
        );
        assert_eq!(store.log_entries().await.unwrap(), entries);
        let (head, at) = store.log_head().await.unwrap().unwrap();
        assert_eq!(head, 7);
        assert!(util::parse_rfc3339_nanos(&at).is_some(), "{at}");
    }

    /// Section 7.1: a quarantine writes the copy to a new file, one canonical
    /// entry per line, records it, and empties the log; a second one has
    /// nothing to move and does nothing; the node then follows from seq 1.
    #[tokio::test]
    async fn quarantine_writes_the_copy_records_it_and_empties_the_log() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(3)).await.unwrap();

        let path = store
            .quarantine_log("log_diverged: test")
            .await
            .unwrap()
            .expect("a copy was written");
        assert_eq!(path.parent(), Some(dir.0.join("quarantine").as_path()));
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), entries.len());
        for (line, entry) in lines.iter().zip(&entries) {
            let value: Value = serde_json::from_str(line).unwrap();
            assert_eq!(value, entry.to_value().unwrap());
            assert_eq!(*line, jcs::canonicalize(&value).unwrap());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_eq!(
            quarantine_rows(&store).await,
            vec![(
                "change_log".to_owned(),
                path.to_string_lossy().into_owned(),
                "log_diverged: test".to_owned(),
                Some(3),
                Some(entries[2].hash.clone()),
            )]
        );
        assert!(store.log_entries().await.unwrap().is_empty());
        assert_eq!(
            store.log_position().await.unwrap(),
            (0, ZERO_HASH.to_owned())
        );
        assert_eq!(store.log_head().await.unwrap(), None);

        // Idempotent: nothing left to move, no second file or row.
        assert_eq!(store.quarantine_log("again").await.unwrap(), None);
        assert_eq!(quarantine_rows(&store).await.len(), 1);
        assert_eq!(
            std::fs::read_dir(dir.0.join("quarantine")).unwrap().count(),
            1
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);

        // A fresh chain from seq 1 is accepted after it.
        let fresh = verified(1, ZERO_HASH, 2, "b");
        store.store_log_batch(&fresh, None).await.unwrap();
        assert_eq!(store.log_entries().await.unwrap(), fresh);
    }

    /// A quarantined line carries the stored payload text as it is. A float
    /// the primary hashed as `1e16` is stored as the integer text
    /// `10000000000000000`, which parsed again has no RFC 8785 form; the
    /// copy is still written, and its line parses back to the entry.
    #[tokio::test]
    async fn quarantine_exports_the_stored_payload_text_as_it_is() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut sent = crate::follow::testing::genesis(1, "a").remove(0);
        sent["payload"] = json!({"big": 1e16, "name": "blog"});
        crate::follow::testing::rehash(&mut sent);
        let entries =
            crate::follow::verify_batch(0, ZERO_HASH, &crate::follow::testing::message(vec![sent]))
                .unwrap();
        let stored = r#"{"big":10000000000000000,"name":"blog"}"#;
        assert_eq!(entries[0].payload, stored);
        assert!(jcs::canonicalize(&entries[0].to_value().unwrap()).is_err());
        store.store_log_batch(&entries, None).await.unwrap();

        let path = store.quarantine_log("test").await.unwrap().unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].contains(&format!(r#""payload":{stored}"#)),
            "{}",
            lines[0]
        );
        let value: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(value, entries[0].to_value().unwrap());
        let keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut fields = crate::follow::ENTRY_FIELDS.to_vec();
        fields.sort_unstable();
        assert_eq!(keys, fields);
        assert!(store.log_entries().await.unwrap().is_empty());
    }

    /// A quarantine file is never written over an existing one, even with the
    /// same time in its name.
    #[test]
    fn a_quarantine_file_never_replaces_an_existing_one() {
        let dir = TempDir::new();
        let entries = vec![LogEntry {
            seq: 1,
            term: 1,
            time: "2026-11-02T10:15:31Z".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: "test".to_owned(),
            entity_type: "app".to_owned(),
            entity_id: "app_1".to_owned(),
            op: "create".to_owned(),
            payload: "{}".to_owned(),
            prev_hash: ZERO_HASH.to_owned(),
            hash: "1".repeat(64),
        }];
        let quarantine = dir.0.join("quarantine");
        let first = write_log_copy(&quarantine, "20261109T101530Z", &entries).unwrap();
        let first_text = std::fs::read_to_string(&first).unwrap();
        let second = write_log_copy(&quarantine, "20261109T101530Z", &entries[..0]).unwrap();
        assert_ne!(first, second);
        assert_eq!(std::fs::read_to_string(&first).unwrap(), first_text);
        assert_eq!(std::fs::read_to_string(&second).unwrap(), "");
    }

    /// Section 12: a database an agent without change-log follow created
    /// gains `change_log` and `quarantine` on open, and both work.
    #[tokio::test]
    async fn an_older_database_gains_the_change_log_and_quarantine_tables() {
        let dir = TempDir::new();
        {
            let options = SqliteConnectOptions::new()
                .filename(dir.0.join("node.db"))
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(options).await.unwrap();
            sqlx::raw_sql(
                "CREATE TABLE node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE desired_state (rev INTEGER PRIMARY KEY, body TEXT NOT NULL, \
                 body_enc BLOB, issued_at TEXT NOT NULL, term INTEGER NOT NULL, \
                 stored_at TEXT NOT NULL); \
                 INSERT INTO node_meta (key, value) VALUES ('applied_rev', '4');",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }
        let store = NodeStore::open(&dir.0).await.expect("the store opens");
        assert_eq!(store.applied_rev().await.unwrap(), 4);
        assert_eq!(
            store.log_position().await.unwrap(),
            (0, ZERO_HASH.to_owned())
        );
        let entries = verified(1, ZERO_HASH, 2, "a");
        store.store_log_batch(&entries, Some(2)).await.unwrap();
        assert_eq!(store.log_entries().await.unwrap(), entries);
        assert!(store.quarantine_log("test").await.unwrap().is_some());
        assert_eq!(quarantine_rows(&store).await.len(), 1);

        // A second start finds the tables and keeps the record.
        drop(store);
        let store = NodeStore::open(&dir.0).await.expect("the store reopens");
        assert_eq!(quarantine_rows(&store).await.len(), 1);
    }
}
