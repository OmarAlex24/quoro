// SPDX-License-Identifier: AGPL-3.0-only
//! The local node store (`node-agent-protocol.md` section 12).
//!
//! One SQLite file, `<data_dir>/node.db`, owned by root, holding the verified
//! desired state, what was applied per VM, the buffered events, the node's
//! verified copy of the change log (`change_log`, section 7.1), the records of
//! quarantined copies (`quarantine`) and a small metadata table. Nothing here
//! is replicated; the tables are local and may evolve. A quarantined copy of
//! the change log itself is a file under `<data_dir>/quarantine/`; one of a
//! volume (section 9) is a reflink clone in the engine's volume directory,
//! kept through the start-up sweep by its `kept_snapshots` row.
//!
//! The whole point of the store is that the node keeps running without the
//! control plane (D-0011): the newest verified snapshot is the source of truth
//! after a restart, and the signature is verified again before it is used, so a
//! tampered store cannot drive the engine.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quoro_proto::wire::{
    BackupState, BuildPhase, BuildState, FailedStep, MAX_TERM, SnapshotMethod, VolumeState,
    term_above_ceiling,
};
use quoro_proto::{FenceReason, LeaseId, LeaseMode, ResourceType};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::sync::{broadcast, watch};
use tracing::warn;

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
/// `key` of `node_meta` holding the refollowing state (section 7.1
/// "Refollowing", section 12 `change_log`, ADR 0261, ADR 0280): absent when
/// the node is not refollowing, [`REFOLLOWING_HEAD_UNKNOWN`] from a
/// quarantine until a `log_entries` carries a `head_seq`, and then that
/// `head_seq` in decimal. [`NodeStore::quarantine_log`] never deletes it.
const LOG_REFOLLOWING: &str = "log_refollowing";
/// The value of [`LOG_REFOLLOWING`] for [`Refollowing::HeadUnknown`].
const REFOLLOWING_HEAD_UNKNOWN: &str = "head unknown";

/// Where a node that refollows the change log after a quarantine stands
/// (section 7.1 "Refollowing", ADR 0261, ADR 0280). It is kept in the local
/// store, so it survives an agent crash at any point (section 12), and while
/// it lasts `quoro cluster promote` refuses on this node (section 7.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refollowing {
    /// The log was moved aside and no `log_entries` since carried the
    /// `head_seq` to refollow to. Nothing is stored in this state.
    HeadUnknown,
    /// The `head_seq` of the first `log_entries` after the quarantine: the
    /// node refollows until its copy reaches it, and its mark is this value
    /// meanwhile.
    Head(u64),
}

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
    allocated_bytes INTEGER,
    handing_over    INTEGER NOT NULL DEFAULT 0,
    hand_over_basis TEXT,
    return_pending  INTEGER NOT NULL DEFAULT 0,
    return_pending_rev INTEGER NOT NULL DEFAULT 0
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
    last_hash      TEXT,
    quarantine_id  TEXT,
    volume_id      TEXT,
    app_id         TEXT,
    size_bytes     INTEGER,
    sha256         TEXT,
    snapshot_id    TEXT
);
";

/// The `kind` of a `quarantine` row that records a quarantined copy of a
/// volume (section 9); a change-log copy's is `change_log` (section 7.1).
const VOLUME_KIND: &str = "volume";

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

/// A quarantined copy of a volume the node keeps (`quarantine`,
/// `node-agent-protocol.md` sections 9 and 12, ADR 0176, ADR 0215): a
/// reflink clone of a returning source's live volume file, recorded beside
/// the kept snapshot that keeps the clone through the start-up sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeQuarantineRecord {
    /// `qrn_` id the node generated.
    pub quarantine_id: String,
    /// The volume it is a copy of.
    pub volume_id: String,
    /// The volume's app.
    pub app_id: String,
    /// When the node took the copy, RFC 3339.
    pub created_at: String,
    /// Bytes the clone uses on disk: its allocated blocks.
    pub size_bytes: u64,
    /// Where the clone is.
    pub path: PathBuf,
    /// The clone's `snapshot_id`, the key of its kept snapshot.
    pub snapshot_id: String,
    /// Why the live file was not proved unchanged.
    pub reason: String,
    /// Lowercase hex SHA-256 of the clone's bytes, once computed.
    pub sha256: Option<String>,
}

/// What the mark on the node's own volume row stands for while the volume is
/// handed over to the warm receiver (section 9): why the live file may go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandOverMark {
    /// The live file's writes are kept in this quarantined copy, by its
    /// `qrn_` id.
    Copy(String),
    /// The live file showed no change against the base.
    Unchanged,
    /// The live file is missing from a volume directory that another file
    /// the node records for the volume shows to be the real one.
    Gone,
}

impl HandOverMark {
    /// How the mark is stored.
    fn as_str(&self) -> &str {
        match self {
            Self::Copy(quarantine_id) => quarantine_id,
            Self::Unchanged => "unchanged",
            Self::Gone => "gone",
        }
    }

    /// A stored mark; `None` for anything else.
    fn parse(stored: &str) -> Option<Self> {
        match stored {
            "unchanged" => Some(Self::Unchanged),
            "gone" => Some(Self::Gone),
            id if id.starts_with("qrn_") => Some(Self::Copy(id.to_owned())),
            _ => None,
        }
    }
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

/// The `node_meta` key holding the highest term the machine has seen, voter
/// or not (sections 8.8 "Terms" and 12). Absent until the first one.
pub const HIGHEST_TERM: &str = "highest_term";
/// The `node_meta` key holding a voter's vote: a JSON
/// [`StoredVote`](crate::vote::StoredVote), the candidate it voted for and the
/// term it voted in (sections 8.8, 8.9 and 12). Only the vote listener of
/// section 8.9 sets it.
pub const VOTE: &str = "vote";
/// The `node_meta` key holding a voter's support: a JSON
/// [`StoredSupport`](crate::vote::StoredSupport), the primary it supports with
/// the last `S` and `L`, and its promise and lease horizon on
/// `CLOCK_BOOTTIME` next to the kernel boot id (sections 8.8 "Restarts" and
/// 12). A machine that stops being a voter keeps it (section 6.12).
pub const SUPPORT: &str = "support";
/// The `node_meta` key holding the newest `welcome.schema_version` the node
/// has seen, in decimal (sections 5.10 and 12, ADR 0229). Absent until a
/// `welcome` carries one; it is never lowered.
pub const WELCOME_SCHEMA_VERSION: &str = "welcome_schema_version";
/// The `node_meta` key holding the promotion freeze: present, with the RFC
/// 3339 time it was set, while a promotion on this host froze the agent
/// (section 7.3 "The freeze", section 12, ADR 0261). [`crate::freeze`] sets
/// and clears it from another process.
pub const PROMOTION_FREEZE: &str = "promotion_freeze";

/// What [`NodeStore::store_log_batch`] did with a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchStored {
    /// The batch is stored, and the refollowing state, if any, goes on.
    Stored,
    /// The batch is stored and brought the copy to the head the node
    /// refollowed to, so the same transaction ended the refollowing state
    /// (section 7.1 "Refollowing").
    RefollowingEnded,
    /// Nothing is stored: a promotion on this host froze the agent (section
    /// 7.3 "The freeze"), so the batch must not be acknowledged.
    Frozen,
}

/// What a write that the promotion freeze refuses did
/// ([`NodeStore::set_refollowing`], [`NodeStore::mark_quarantine`], section
/// 7.3 "The freeze", ADR 0261). Each such write reads the freeze inside its
/// own `BEGIN IMMEDIATE` transaction, as [`NodeStore::store_log_batch`] does,
/// so once a freeze has committed it writes nothing.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreezeChecked {
    /// The agent was not frozen, and the write committed.
    Written,
    /// A promotion on this host froze the agent: nothing was written.
    Frozen,
}

/// What [`NodeStore::quarantine_log`] did with the node's copy of the change
/// log (section 7.1).
#[must_use]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogQuarantine {
    /// The copy was moved aside to this file.
    Moved(PathBuf),
    /// The copy is empty, so there was nothing to move.
    Empty,
    /// A promotion on this host froze the agent (section 7.3 "The freeze",
    /// ADR 0261): the copy stays as it stands and no file is left. This is
    /// no failure; the caller stops as for any write the freeze refuses.
    Frozen,
}

/// The connection settings of `<data_dir>/node.db`, shared by every
/// connection to it: the agent's pool and the single connection a promotion
/// opens from its own process to freeze the agent ([`crate::freeze`]).
///
/// The file stays in WAL mode with SQLite's default `NORMAL` locking, never
/// `EXCLUSIVE`, so a second process can open it next to a running agent: its
/// readers never block, and writers from either process take SQLite's write
/// lock in turn, each waiting up to the busy timeout for the other.
pub(crate) fn connect_options(data_dir: &Path) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(data_dir.join("node.db"))
        .journal_mode(SqliteJournalMode::Wal)
        // Every commit reaches the disk before it returns: a voter's
        // promise must survive a power loss before its `support_grant`
        // goes out (section 8.8 "Restarts"), and a freeze before the
        // promotion's first real vote request (section 7.3).
        .synchronous(SqliteSynchronous::Full)
        .foreign_keys(true)
        // Deleted rows (pruned log lines, resealed plaintext bodies) are
        // overwritten, not left in free pages.
        .pragma("secure_delete", "ON")
        .busy_timeout(Duration::from_secs(5))
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
        let options = connect_options(data_dir).create_if_missing(true);
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

    /// The highest term the machine has seen, or `None` before the first
    /// (section 8.8 "Terms").
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a value that is not a
    /// term.
    pub async fn highest_term(&self) -> Result<Option<u64>, AgentError> {
        self.meta(HIGHEST_TERM)
            .await?
            .map(|text| parse_term(&text))
            .transpose()
    }

    /// Stores `term` as the highest term seen when it is above the stored one,
    /// in one statement, so a term is never lowered (section 8.8 "Terms").
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, and for a `term` above
    /// [`MAX_TERM`], which is never stored (section 8.8 "The term ceiling").
    pub async fn raise_term(&self, term: u64) -> Result<(), AgentError> {
        let mut conn = self.pool.acquire().await.map_err(store_error)?;
        raise_term_on(&mut conn, term).await
    }

    /// The newest `welcome.schema_version` the node has seen, or `None` when
    /// no `welcome` carried one (sections 5.10 and 12, ADR 0229). `quoro
    /// cluster promote` refuses when it is above the newest schema version
    /// its binary knows (section 7.3).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a stored value that is
    /// not a version.
    pub async fn welcome_schema_version(&self) -> Result<Option<u32>, AgentError> {
        self.meta(WELCOME_SCHEMA_VERSION)
            .await?
            .map(|text| {
                text.parse::<u32>().map_err(|_| {
                    AgentError::Store(format!(
                        "the stored welcome schema version {text:?} is not a version"
                    ))
                })
            })
            .transpose()
    }

    /// Stores `version` as the newest `welcome.schema_version` seen when it is
    /// above the stored one, in one statement, so it is never lowered
    /// (sections 5.10 and 12).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn raise_welcome_schema_version(&self, version: u32) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO node_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value \
             WHERE CAST(node_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
        )
        .bind(WELCOME_SCHEMA_VERSION)
        .bind(version.to_string())
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Reads a voter's term, vote and support, lets `decide` choose what to
    /// write, and writes it, in one transaction (sections 8.8 and 8.9): the
    /// highest term, the support of a `support_grant` and the vote of a
    /// granted promotion vote.
    ///
    /// The transaction starts with `BEGIN IMMEDIATE`, so it holds SQLite's
    /// write lock from its first read: a vote written by another task either
    /// commits before it, and `decide` sees it, or waits for it, and then
    /// sees the promise it stored. A support is therefore never granted on a
    /// vote state that a vote changed in between. What `decide` returns is
    /// handed back only once the transaction committed, so an answer built
    /// from it goes out after what it promises is durable.
    ///
    /// No term above [`MAX_TERM`] is written (section 8.8 "The term
    /// ceiling"): a write that carries one, as the highest term, the
    /// support's or the vote's, fails before anything is written.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the state cannot be read or parsed, or the
    /// writes or the commit fail, or a term to write is above [`MAX_TERM`];
    /// nothing is written then.
    pub async fn update_vote_state<T>(
        &self,
        decide: impl FnOnce(&crate::vote::VoteState) -> (T, crate::vote::VoteWrite),
    ) -> Result<T, AgentError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        let highest_term = meta_on(&mut tx, HIGHEST_TERM)
            .await?
            .map(|text| parse_term(&text))
            .transpose()?;
        let vote = meta_on(&mut tx, VOTE)
            .await?
            .map(|text| parse_json(VOTE, &text))
            .transpose()?;
        let support = meta_on(&mut tx, SUPPORT)
            .await?
            .map(|text| parse_json(SUPPORT, &text))
            .transpose()?;
        let state = crate::vote::VoteState {
            highest_term,
            vote,
            support,
        };
        let (decided, write) = decide(&state);
        for (what, term) in [
            ("highest", write.term),
            (
                "support's",
                write.support.as_ref().map(|support| support.term),
            ),
            ("vote's", write.vote.as_ref().map(|vote| vote.term)),
        ] {
            if let Some(term) = term.filter(|term| *term > MAX_TERM) {
                return Err(above_ceiling(what, Some(&Value::from(term))));
            }
        }
        if let Some(term) = write.term {
            raise_term_on(&mut tx, term).await?;
        }
        if let Some(support) = &write.support {
            let text = serde_json::to_string(support)
                .map_err(|e| AgentError::Store(format!("could not encode the support: {e}")))?;
            sqlx::query(
                "INSERT INTO node_meta (key, value) VALUES (?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(SUPPORT)
            .bind(text)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        if let Some(vote) = &write.vote {
            let text = serde_json::to_string(vote)
                .map_err(|e| AgentError::Store(format!("could not encode the vote: {e}")))?;
            sqlx::query(
                "INSERT INTO node_meta (key, value) VALUES (?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(VOTE)
            .bind(text)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
        Ok(decided)
    }

    /// The voter's vote, when it voted (section 8.9).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a value that does not
    /// parse.
    pub async fn vote(&self) -> Result<Option<crate::vote::StoredVote>, AgentError> {
        self.meta_json(VOTE).await
    }

    /// The voter's support and its promise and lease horizon (section 8.8).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a value that does not
    /// parse.
    pub async fn support(&self) -> Result<Option<crate::vote::StoredSupport>, AgentError> {
        self.meta_json(SUPPORT).await
    }

    /// Replaces the voter's support. It is durable once this returns, which is
    /// what lets the voter answer `support_grant` (sections 5.28 and 8.8).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, and for a support whose
    /// term is above [`MAX_TERM`], which is never stored (section 8.8 "The
    /// term ceiling").
    pub async fn save_support(
        &self,
        support: &crate::vote::StoredSupport,
    ) -> Result<(), AgentError> {
        if support.term > MAX_TERM {
            return Err(above_ceiling("support's", Some(&Value::from(support.term))));
        }
        let text = serde_json::to_string(support)
            .map_err(|e| AgentError::Store(format!("could not encode the support: {e}")))?;
        self.set_meta(SUPPORT, &text).await
    }

    /// One JSON `node_meta` value.
    async fn meta_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, AgentError> {
        self.meta(key)
            .await?
            .map(|text| parse_json(key, &text))
            .transpose()
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
    /// [`AgentError::Store`] when the body cannot be encoded or SQLite fails,
    /// and for a body whose `term` is above [`MAX_TERM`], which is never
    /// stored (section 8.8 "The term ceiling").
    pub async fn store_desired(&self, rev: u64, body: &Value) -> Result<(), AgentError> {
        if term_above_ceiling(body) {
            return Err(above_ceiling("desired_state's", body.get("term")));
        }
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

    /// Whether the node's own row of `volume_id` is marked as being handed
    /// over to the warm receiver (section 9); `false` without a row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_handing_over(&self, volume_id: &str) -> Result<bool, AgentError> {
        let mark: Option<i64> =
            sqlx::query_scalar("SELECT handing_over FROM volumes WHERE volume_id = ?")
                .bind(volume_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(mark.is_some_and(|mark| mark != 0))
    }

    /// What the mark on the node's own row of `volume_id` stands for
    /// (section 9); `None` without a mark or a row. A mark whose basis
    /// cannot be read stands for nothing: the hand-over compares the file
    /// again.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_hand_over_mark(
        &self,
        volume_id: &str,
    ) -> Result<Option<HandOverMark>, AgentError> {
        let row: Option<(i64, Option<String>)> =
            sqlx::query_as("SELECT handing_over, hand_over_basis FROM volumes WHERE volume_id = ?")
                .bind(volume_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(match row {
            Some((mark, basis)) if mark != 0 => {
                let parsed = basis.as_deref().and_then(HandOverMark::parse);
                if parsed.is_none() {
                    warn!(volume_id, basis = ?basis, "a hand-over mark has no basis that can be read; the volume is compared again");
                }
                parsed
            }
            _ => None,
        })
    }

    /// Marks the node's own row of `volume_id` as being handed over (section
    /// 9), with what the mark stands for: the live file showed no change
    /// against the base, or it is gone from a volume directory shown to be
    /// the real one. A quarantined copy's mark is written with its records
    /// ([`Self::record_volume_quarantine`]). From the mark on, the hand-over
    /// only removes the file and the row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn mark_volume_handing_over(
        &self,
        volume_id: &str,
        mark: &HandOverMark,
    ) -> Result<(), AgentError> {
        sqlx::query("UPDATE volumes SET handing_over = 1, hand_over_basis = ? WHERE volume_id = ?")
            .bind(mark.as_str())
            .bind(volume_id)
            .execute(&self.pool)
            .await
            .map_err(store_error)
            .map(|_| ())
    }

    /// The volumes whose own row is marked as being handed over (section 9).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volumes_handing_over(&self) -> Result<BTreeSet<String>, AgentError> {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT volume_id FROM volumes WHERE handing_over = 1")
                .fetch_all(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(ids.into_iter().collect())
    }

    /// Clears the mark of a hand-over that did not finish on the node's own
    /// row of `volume_id`, and what it stood for (section 9): the node runs
    /// the volume again, so its file may take writes the mark did not see,
    /// or the copy the mark stands for no longer keeps them. A later
    /// hand-over compares the file again.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn clear_volume_handing_over(&self, volume_id: &str) -> Result<(), AgentError> {
        sqlx::query(
            "UPDATE volumes SET handing_over = 0, hand_over_basis = NULL WHERE volume_id = ?",
        )
        .bind(volume_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|_| ())
    }

    /// Ends a hand-over (section 9) in one transaction: drops the node's own
    /// row of `volume_id` when it is marked as being handed over, and leaves
    /// the warm target row with no complete copy, nothing pending and no
    /// error, so the next stream is a full copy. The caller removed the
    /// live file first. Returns whether a marked row was dropped.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; nothing changes then.
    pub async fn finish_hand_over(&self, volume_id: &str) -> Result<bool, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        let dropped = sqlx::query("DELETE FROM volumes WHERE volume_id = ? AND handing_over = 1")
            .bind(volume_id)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?
            .rows_affected();
        sqlx::query(
            "UPDATE warm_targets SET complete_seq = NULL, taken_at = NULL, completed_at = NULL, \
             allocated_bytes = NULL, cells = NULL, pending_seq = NULL, pending_taken_at = NULL, \
             pending_root = NULL, pending_cells = NULL, error = NULL WHERE volume_id = ?",
        )
        .bind(volume_id)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        tx.commit().await.map_err(store_error)?;
        Ok(dropped > 0)
    }

    /// Records that this node is the target of a warm volume it holds as its
    /// own, a returning source (section 9), in one transaction: the warm
    /// target row, as [`Self::add_warm_target`] writes it, and the flag on
    /// the own row that the node is returning, with the desired-state `rev`
    /// that named it the target. That rev is raised, never lowered: only a
    /// newer revision clears the flag ([`Self::end_return_pending`]). Returns
    /// whether an own row, not `deleted`, carries the flag now; nothing is
    /// flagged without one.
    ///
    /// While the flag is set, a `deleted` entry never removes the own file
    /// and the sender keeps the volume's warm snapshots: the stale-copy rule
    /// of section 5.11 never removes a volume of the node's own (ADR 0267).
    /// The flag goes with the row at the end of a hand-over
    /// ([`Self::finish_hand_over`]), or when the node runs the volume again
    /// ([`Self::end_return_pending`]).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; nothing is written then.
    pub async fn add_return_pending_target(
        &self,
        volume_id: &str,
        app_id: &str,
        size_gb: u32,
        rev: u64,
    ) -> Result<bool, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query(
            "INSERT INTO warm_targets (volume_id, app_id, size_gb) VALUES (?, ?, ?) \
             ON CONFLICT(volume_id) DO UPDATE SET removed = 0 WHERE warm_targets.removed = 1",
        )
        .bind(volume_id)
        .bind(app_id)
        .bind(i64::from(size_gb))
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        let flagged = sqlx::query(
            "UPDATE volumes SET return_pending = 1, \
             return_pending_rev = MAX(return_pending_rev, ?) \
             WHERE volume_id = ? AND state <> 'deleted'",
        )
        .bind(i64::try_from(rev).unwrap_or(i64::MAX))
        .bind(volume_id)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?
        .rows_affected();
        tx.commit().await.map_err(store_error)?;
        Ok(flagged > 0)
    }

    /// Whether the node's own row of `volume_id` carries the flag that the
    /// node is returning as the volume's warm target (section 9); `false`
    /// without a row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_return_pending(&self, volume_id: &str) -> Result<bool, AgentError> {
        let flag: Option<i64> =
            sqlx::query_scalar("SELECT return_pending FROM volumes WHERE volume_id = ?")
                .bind(volume_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(flag.is_some_and(|flag| flag != 0))
    }

    /// The node's own volumes that are not `deleted`, each with whether its
    /// row carries the returning flag, read in one statement.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn own_volumes_return_pending(&self) -> Result<BTreeMap<String, bool>, AgentError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT volume_id, return_pending FROM volumes WHERE state <> 'deleted'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .map(|(volume_id, flag)| (volume_id, flag != 0))
            .collect())
    }

    /// The desired-state rev that last named the node the target of
    /// `volume_id` while it holds the volume as its own, when its own row
    /// carries the returning flag (section 9); `None` without the flag or a
    /// row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_return_pending_rev(
        &self,
        volume_id: &str,
    ) -> Result<Option<u64>, AgentError> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT return_pending, return_pending_rev FROM volumes WHERE volume_id = ?",
        )
        .bind(volume_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(row
            .filter(|(flag, _)| *flag != 0)
            .map(|(_, rev)| u64::try_from(rev).unwrap_or(0)))
    }

    /// The node runs `volume_id` again (section 9) under a desired state of
    /// rev `newer_than`: in one transaction, clears the returning flag on
    /// its own row when the flag was set by an older revision, and then
    /// forgets the warm target row beside it, whose copy the hand-over never
    /// took (the store row only, never a file). Returns whether the flag was
    /// cleared; a flag set by `newer_than` or a later revision stays, and so
    /// does the warm row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; nothing changes then.
    pub async fn end_return_pending(
        &self,
        volume_id: &str,
        newer_than: u64,
    ) -> Result<bool, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        let cleared = sqlx::query(
            "UPDATE volumes SET return_pending = 0 \
             WHERE volume_id = ? AND return_pending = 1 AND return_pending_rev < ?",
        )
        .bind(volume_id)
        .bind(i64::try_from(newer_than).unwrap_or(i64::MAX))
        .execute(&mut *tx)
        .await
        .map_err(store_error)?
        .rows_affected()
            > 0;
        if cleared {
            sqlx::query("DELETE FROM warm_targets WHERE volume_id = ?")
                .bind(volume_id)
                .execute(&mut *tx)
                .await
                .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
        Ok(cleared)
    }

    /// Forgets the warm target row of `volume_id` when it holds no copy:
    /// no complete copy and nothing pending. The store row only, never a
    /// file. Returns whether a row went.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn forget_copyless_warm_target(&self, volume_id: &str) -> Result<bool, AgentError> {
        sqlx::query(
            "DELETE FROM warm_targets \
             WHERE volume_id = ? AND complete_seq IS NULL AND pending_seq IS NULL",
        )
        .bind(volume_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)
        .map(|done| done.rows_affected() > 0)
    }

    /// Clears the returning flag whatever revision set it; for the tests.
    #[cfg(test)]
    pub(crate) async fn clear_volume_return_pending(
        &self,
        volume_id: &str,
    ) -> Result<(), AgentError> {
        sqlx::query("UPDATE volumes SET return_pending = 0 WHERE volume_id = ?")
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
        let mut conn = self.pool.acquire().await.map_err(store_error)?;
        upsert_kept_snapshot(&mut conn, record).await
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

    /// One kept snapshot, by its id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn kept_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<KeptSnapshotRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM kept_snapshots WHERE snapshot_id = ?")
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(kept_snapshot_record).transpose()
    }

    /// Records a quarantined copy of a volume (section 9) in one transaction:
    /// the clone's kept snapshot, so the start-up sweep keeps the file, the
    /// `quarantine` row, and the mark on the node's own volume row that it
    /// is being handed over, standing for this copy. A crash before the commit leaves none of the
    /// three, and the sweep removes the unrecorded clone at the next start.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, or when the kept snapshot
    /// is recorded with another file or volume; nothing is written then.
    pub async fn record_volume_quarantine(
        &self,
        kept: &KeptSnapshotRecord,
        record: &VolumeQuarantineRecord,
    ) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        upsert_kept_snapshot(&mut tx, kept).await?;
        sqlx::query(
            "INSERT INTO quarantine (kind, path, quarantined_at, reason, quarantine_id, \
             volume_id, app_id, size_bytes, sha256, snapshot_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(VOLUME_KIND)
        .bind(record.path.display().to_string())
        .bind(&record.created_at)
        .bind(&record.reason)
        .bind(&record.quarantine_id)
        .bind(&record.volume_id)
        .bind(&record.app_id)
        .bind(i64::try_from(record.size_bytes).unwrap_or(i64::MAX))
        .bind(record.sha256.as_deref())
        .bind(&record.snapshot_id)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        sqlx::query("UPDATE volumes SET handing_over = 1, hand_over_basis = ? WHERE volume_id = ?")
            .bind(&record.quarantine_id)
            .bind(&record.volume_id)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)
    }

    /// Every quarantined copy of a volume the node keeps, oldest first. A
    /// row that cannot be read back is left out with a warning, so one bad
    /// row never empties the list `status_report.quarantines` carries.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_quarantines(&self) -> Result<Vec<VolumeQuarantineRecord>, AgentError> {
        let rows = sqlx::query("SELECT * FROM quarantine WHERE kind = ? ORDER BY id ASC")
            .bind(VOLUME_KIND)
            .fetch_all(&self.pool)
            .await
            .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let id: Option<i64> = row.try_get("id").ok();
                match volume_quarantine_record(row) {
                    Ok(record) => Some(record),
                    Err(e) => {
                        warn!(row = ?id, error = %e, "a volume quarantine row cannot be read; it is left out");
                        None
                    }
                }
            })
            .collect())
    }

    /// One quarantined copy of a volume, by its `qrn_` id.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a row it cannot read back.
    pub async fn volume_quarantine(
        &self,
        quarantine_id: &str,
    ) -> Result<Option<VolumeQuarantineRecord>, AgentError> {
        let row = sqlx::query("SELECT * FROM quarantine WHERE kind = ? AND quarantine_id = ?")
            .bind(VOLUME_KIND)
            .bind(quarantine_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_error)?;
        row.map(volume_quarantine_record).transpose()
    }

    /// The clone path of the quarantined copy `quarantine_id`, read alone, so
    /// a marked hand-over can check the clone while the rest of the row
    /// cannot be read whole; `None` without such a row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn volume_quarantine_path(
        &self,
        quarantine_id: &str,
    ) -> Result<Option<PathBuf>, AgentError> {
        let path: Option<String> =
            sqlx::query_scalar("SELECT path FROM quarantine WHERE kind = ? AND quarantine_id = ?")
                .bind(VOLUME_KIND)
                .bind(quarantine_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(store_error)?;
        Ok(path.map(PathBuf::from))
    }

    /// The `snapshot_id` of the quarantined copy `quarantine_id`, read alone,
    /// so a row that cannot be read whole can still be deleted; `None`
    /// without such a row.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, or for a row without a
    /// `snapshot_id`.
    pub async fn volume_quarantine_snapshot_id(
        &self,
        quarantine_id: &str,
    ) -> Result<Option<String>, AgentError> {
        let snapshot_id: Option<Option<String>> = sqlx::query_scalar(
            "SELECT snapshot_id FROM quarantine WHERE kind = ? AND quarantine_id = ?",
        )
        .bind(VOLUME_KIND)
        .bind(quarantine_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        match snapshot_id {
            None => Ok(None),
            Some(Some(snapshot_id)) => Ok(Some(snapshot_id)),
            Some(None) => Err(AgentError::Store(format!(
                "the quarantined copy {quarantine_id} has no snapshot_id"
            ))),
        }
    }

    /// Records the SHA-256 of a quarantined copy that has none yet. Returns
    /// whether a record took it: `false` once the copy was deleted.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn set_quarantine_sha256(
        &self,
        quarantine_id: &str,
        sha256: &str,
    ) -> Result<bool, AgentError> {
        let done = sqlx::query(
            "UPDATE quarantine SET sha256 = ? \
             WHERE kind = ? AND quarantine_id = ? AND sha256 IS NULL",
        )
        .bind(sha256)
        .bind(VOLUME_KIND)
        .bind(quarantine_id)
        .execute(&self.pool)
        .await
        .map_err(store_error)?;
        Ok(done.rows_affected() > 0)
    }

    /// Forgets the record of a quarantined copy whose clone is gone before
    /// it was hashed (section 9 step 2), in one transaction: its `quarantine`
    /// row, only while it has no SHA-256 and still names `snapshot_id`, and
    /// its kept snapshot. No file is touched. Returns whether the record
    /// went: `false` once the copy has a hash, or is gone.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; both records stay then.
    pub async fn forget_unhashed_volume_quarantine(
        &self,
        quarantine_id: &str,
        snapshot_id: &str,
    ) -> Result<bool, AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        let gone = sqlx::query(
            "DELETE FROM quarantine WHERE kind = ? AND quarantine_id = ? AND sha256 IS NULL \
             AND snapshot_id = ?",
        )
        .bind(VOLUME_KIND)
        .bind(quarantine_id)
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        if gone.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query("DELETE FROM kept_snapshots WHERE snapshot_id = ?")
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)?;
        Ok(true)
    }

    /// Forgets a quarantined copy an operator deleted, in one transaction:
    /// its kept snapshot and its `quarantine` row. The caller deleted the
    /// clone first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; both records stay then.
    pub async fn forget_volume_quarantine(
        &self,
        quarantine_id: &str,
        snapshot_id: &str,
    ) -> Result<(), AgentError> {
        let mut tx = self.pool.begin().await.map_err(store_error)?;
        sqlx::query("DELETE FROM kept_snapshots WHERE snapshot_id = ?")
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        sqlx::query("DELETE FROM quarantine WHERE kind = ? AND quarantine_id = ?")
            .bind(VOLUME_KIND)
            .bind(quarantine_id)
            .execute(&mut *tx)
            .await
            .map_err(store_error)?;
        tx.commit().await.map_err(store_error)
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
    /// When the node refollows to [`Refollowing::Head`] `h` and the batch
    /// brings the copy to `h` or past it, the same transaction ends the
    /// refollowing state (section 7.1 "Refollowing", ADR 0261), so the copy
    /// never reaches `h` with the state still recorded, nor the state ends
    /// without it, and the result is [`BatchStored::RefollowingEnded`].
    ///
    /// The transaction starts with `BEGIN IMMEDIATE`, so it holds SQLite's
    /// write lock before its first read, and that first read is the
    /// promotion freeze ([`PROMOTION_FREEZE`], section 7.3 "The freeze",
    /// ADR 0261). When it is set, nothing is stored and the result is
    /// [`BatchStored::Frozen`]. SQLite serializes writers, also across
    /// processes, so once a freeze has committed no later batch is stored,
    /// and a batch that committed first is in the log the promotion reads
    /// after its freeze.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the batch does not continue the copy or
    /// SQLite fails; nothing is stored then.
    pub async fn store_log_batch(
        &self,
        entries: &[LogEntry],
        head_seq: Option<u64>,
    ) -> Result<BatchStored, AgentError> {
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(BatchStored::Stored);
        };
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        if meta_on(&mut tx, PROMOTION_FREEZE).await?.is_some() {
            // Dropping the transaction rolls it back; it wrote nothing.
            return Ok(BatchStored::Frozen);
        }
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
        let refollowing: Option<String> =
            sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
                .bind(LOG_REFOLLOWING)
                .fetch_optional(&mut *tx)
                .await
                .map_err(store_error)?;
        let ended = match refollowing.as_deref().map(parse_refollowing).transpose()? {
            Some(Refollowing::Head(head)) => last.seq >= head,
            Some(Refollowing::HeadUnknown) | None => false,
        };
        if ended {
            sqlx::query("DELETE FROM node_meta WHERE key = ?")
                .bind(LOG_REFOLLOWING)
                .execute(&mut *tx)
                .await
                .map_err(store_error)?;
        }
        tx.commit().await.map_err(store_error)?;
        Ok(if ended {
            BatchStored::RefollowingEnded
        } else {
            BatchStored::Stored
        })
    }

    /// The refollowing state (section 7.1 "Refollowing", section 12
    /// `change_log`, ADR 0261, ADR 0280), or `None` when the node is not
    /// refollowing.
    ///
    /// `quoro cluster promote` reads it and refuses while it is set (section
    /// 7.3); the agent reads it on every `log_entries` and before every
    /// `hello`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or a stored value that is
    /// neither `head unknown` nor a decimal `seq`.
    pub async fn refollowing(&self) -> Result<Option<Refollowing>, AgentError> {
        self.meta(LOG_REFOLLOWING)
            .await?
            .as_deref()
            .map(parse_refollowing)
            .transpose()
    }

    /// Records the refollowing state and commits it (section 7.1
    /// "Refollowing"). Only [`Self::store_log_batch`] ends it.
    ///
    /// The transaction starts with `BEGIN IMMEDIATE` and first reads the
    /// promotion freeze ([`PROMOTION_FREEZE`], section 7.3 "The freeze"), as
    /// [`Self::store_log_batch`] does. When it is set, nothing is written and
    /// the result is [`FreezeChecked::Frozen`], so once a freeze has
    /// committed the refollowing state stays as the promotion read it.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure; nothing is written then.
    pub async fn set_refollowing(&self, state: Refollowing) -> Result<FreezeChecked, AgentError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        if meta_on(&mut tx, PROMOTION_FREEZE).await?.is_some() {
            tx.rollback().await.map_err(store_error)?;
            return Ok(FreezeChecked::Frozen);
        }
        set_refollowing_on(&mut tx, state).await?;
        tx.commit().await.map_err(store_error)?;
        Ok(FreezeChecked::Written)
    }

    /// The first two steps of a quarantine on `log_diverged` (section 7.1
    /// "Refollowing", ADR 0261), unless a promotion froze the agent: the
    /// change-log mark is written as [`crate::follow::MAX_SEQ`] and synced,
    /// then "refollowing, head unknown" is recorded. Moving the copy aside,
    /// the third step, is [`Self::quarantine_log`].
    ///
    /// Both happen while one `BEGIN IMMEDIATE` transaction holds SQLite's
    /// write lock, after its first read, the promotion freeze
    /// ([`PROMOTION_FREEZE`], section 7.3 "The freeze"). When that is set,
    /// the transaction rolls back, the mark file is not touched and the
    /// result is [`FreezeChecked::Frozen`]. Otherwise a freeze from another
    /// process waits on its busy timeout until this commits, so it never
    /// commits between the flag read and the mark: once a freeze has
    /// committed, a quarantine writes nothing. The mark can be written
    /// without the state only when the state write itself fails (see the
    /// errors below).
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the mark cannot be written and synced, and
    /// [`AgentError::Store`] on a SQLite failure. A failure after the mark
    /// was written leaves the mark at [`crate::follow::MAX_SEQ`] with no
    /// state recorded, which [`crate::follow::recover`] finishes before the
    /// next `hello` (ADR 0285).
    pub async fn mark_quarantine(&self) -> Result<FreezeChecked, AgentError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        if meta_on(&mut tx, PROMOTION_FREEZE).await?.is_some() {
            tx.rollback().await.map_err(store_error)?;
            return Ok(FreezeChecked::Frozen);
        }
        crate::follow::write_mark(&self.data_dir, crate::follow::MAX_SEQ).await?;
        set_refollowing_on(&mut tx, Refollowing::HeadUnknown).await?;
        tx.commit().await.map_err(store_error)?;
        Ok(FreezeChecked::Written)
    }

    /// Whether the stored copy of the change log is below the change-log
    /// mark (section 7.1 "The node's mark", ADR 0261): its newest `seq` is
    /// lower than the mark. No log is below a missing mark. Such a log lost
    /// entries the node once acknowledged, or is refollowing toward the
    /// mark, and `quoro cluster promote` refuses on it without
    /// `--accept-loss` (section 7.3). The agent itself does not read it; the
    /// promotion applies the same rule, [`crate::follow::below_mark`], to
    /// the values [`crate::freeze::promotion_reads`] reads.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure, and the errors of
    /// [`crate::follow::read_mark`].
    pub async fn log_below_mark(&self) -> Result<bool, AgentError> {
        let mark = crate::follow::read_mark(&self.data_dir)?;
        let (log_seq, _) = self.log_position().await?;
        Ok(crate::follow::below_mark(log_seq, mark))
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
    /// rows it holds are deleted and the primary's `head_seq` is cleared. The
    /// refollowing state is kept: the caller records it before the copy moves
    /// (section 7.1 "Refollowing", ADR 0261).
    /// Returns [`LogQuarantine::Moved`] with the file, or
    /// [`LogQuarantine::Empty`] when the copy is empty, which needs no file,
    /// so a second call does nothing.
    ///
    /// While a promotion froze the agent the copy stays as it stands (section
    /// 7.3 "The freeze"): the transaction holds the write lock before it
    /// reads the freeze, as [`Self::store_log_batch`] does, and when it is
    /// set the file is removed, nothing changes and the result is
    /// [`LogQuarantine::Frozen`], which is no error. [`Self::mark_quarantine`]
    /// committed the mark and "head unknown" before, so the copy reads as
    /// quarantined meanwhile, and [`crate::follow::recover`] moves it aside
    /// before the first `hello` after the freeze.
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the file cannot be written and synced, and
    /// [`AgentError::Store`] on a SQLite failure or when the rows to delete
    /// are not the ones exported. Nothing is deleted then.
    pub async fn quarantine_log(&self, reason: &str) -> Result<LogQuarantine, AgentError> {
        let entries = self.log_entries().await?;
        let Some(last) = entries.last() else {
            return Ok(LogQuarantine::Empty);
        };
        let last_seq = seq_column(last.seq)?;
        let last_hash = last.hash.clone();
        let exported = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        let dir = self.data_dir.join(QUARANTINE_DIR);
        let stamp = util::rfc3339_nanos(util::now_ns()).replace(['-', ':'], "");
        let path = tokio::task::spawn_blocking(move || write_log_copy(&dir, &stamp, &entries))
            .await
            .map_err(|e| AgentError::Store(format!("the quarantine writer stopped: {e}")))??;

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        if meta_on(&mut tx, PROMOTION_FREEZE).await?.is_some() {
            // Dropping the transaction rolls it back; the file is no copy.
            drop(tx);
            let _ = std::fs::remove_file(&path);
            return Ok(LogQuarantine::Frozen);
        }
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
        Ok(LogQuarantine::Moved(path))
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

/// One `node_meta` value, read on `conn`.
async fn meta_on(
    conn: &mut sqlx::SqliteConnection,
    key: &str,
) -> Result<Option<String>, AgentError> {
    sqlx::query_scalar("SELECT value FROM node_meta WHERE key = ?")
        .bind(key)
        .fetch_optional(conn)
        .await
        .map_err(store_error)
}

/// Records the refollowing state on `conn`, inside the caller's transaction.
async fn set_refollowing_on(
    conn: &mut sqlx::SqliteConnection,
    state: Refollowing,
) -> Result<(), AgentError> {
    let value = match state {
        Refollowing::HeadUnknown => REFOLLOWING_HEAD_UNKNOWN.to_owned(),
        Refollowing::Head(head) => head.to_string(),
    };
    sqlx::query(
        "INSERT INTO node_meta (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(LOG_REFOLLOWING)
    .bind(value)
    .execute(conn)
    .await
    .map_err(store_error)?;
    Ok(())
}

/// Stores `term` as the highest term seen when it is above the stored one,
/// in one statement on `conn`, so a term is never lowered (section 8.8). A
/// term above [`MAX_TERM`] is refused and nothing is written (section 8.8
/// "The term ceiling").
async fn raise_term_on(conn: &mut sqlx::SqliteConnection, term: u64) -> Result<(), AgentError> {
    if term > MAX_TERM {
        return Err(above_ceiling("highest", Some(&Value::from(term))));
    }
    sqlx::query(
        "INSERT INTO node_meta (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value \
         WHERE CAST(node_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
    )
    .bind(HIGHEST_TERM)
    .bind(term.to_string())
    .execute(conn)
    .await
    .map_err(store_error)
    .map(|_| ())
}

/// The refusal to store the `what` term `term`, which is above [`MAX_TERM`]
/// (section 8.8 "The term ceiling").
fn above_ceiling(what: &str, term: Option<&Value>) -> AgentError {
    let term = term.map_or_else(|| "?".to_owned(), Value::to_string);
    AgentError::Store(format!(
        "the {what} term {term} is above the term ceiling {MAX_TERM}; it is not stored"
    ))
}

/// A stored term ([`HIGHEST_TERM`]).
fn parse_term(text: &str) -> Result<u64, AgentError> {
    text.parse::<u64>()
        .map_err(|_| AgentError::Store(format!("the stored term {text:?} is not a term")))
}

/// A JSON `node_meta` value stored under `key`.
fn parse_json<T: serde::de::DeserializeOwned>(key: &str, text: &str) -> Result<T, AgentError> {
    serde_json::from_str(text)
        .map_err(|e| AgentError::Store(format!("the stored {key} does not parse: {e}")))
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

/// A stored [`LOG_REFOLLOWING`] value.
fn parse_refollowing(value: &str) -> Result<Refollowing, AgentError> {
    if value == REFOLLOWING_HEAD_UNKNOWN {
        return Ok(Refollowing::HeadUnknown);
    }
    value.parse::<u64>().map(Refollowing::Head).map_err(|_| {
        AgentError::Store(format!(
            "the stored refollowing state {value:?} is neither {REFOLLOWING_HEAD_UNKNOWN:?} nor a seq"
        ))
    })
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

/// Inserts or updates one kept snapshot on `conn`, for
/// [`NodeStore::save_kept_snapshot`] and the transactions that record one
/// with other rows. A record never moves to another file.
async fn upsert_kept_snapshot(
    conn: &mut sqlx::SqliteConnection,
    record: &KeptSnapshotRecord,
) -> Result<(), AgentError> {
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
    .execute(conn)
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

/// Reads one volume `quarantine` row back into a [`VolumeQuarantineRecord`].
fn volume_quarantine_record(
    row: sqlx::sqlite::SqliteRow,
) -> Result<VolumeQuarantineRecord, AgentError> {
    let text = |name: &str| -> Result<String, AgentError> {
        row.try_get::<Option<String>, _>(name)
            .map_err(store_error)?
            .ok_or_else(|| AgentError::Store(format!("a volume quarantine row has no {name}")))
    };
    let size_bytes: Option<i64> = row.try_get("size_bytes").map_err(store_error)?;
    Ok(VolumeQuarantineRecord {
        quarantine_id: text("quarantine_id")?,
        volume_id: text("volume_id")?,
        app_id: text("app_id")?,
        created_at: text("quarantined_at")?,
        size_bytes: size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
            .unwrap_or(0),
        path: PathBuf::from(text("path")?),
        snapshot_id: text("snapshot_id")?,
        reason: text("reason")?,
        sha256: row.try_get("sha256").map_err(store_error)?,
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
/// `leases`, `backups.guest_flush` (M5b, ADR 0282), `volumes.handing_over`,
/// `volumes.hand_over_basis`, `volumes.return_pending` and
/// `volumes.return_pending_rev`, and the volume columns
/// of `quarantine` with their unique `qrn_` index (M5b, section 9). New databases get the columns from [`SCHEMA`]; existing ones
/// are altered once, and the old rows keep loading.
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

    // A returning source's hand-over (section 9): the mark on its own volume
    // row and what it stands for, and the quarantined copies of a volume beside those of the change
    // log (section 12 `quarantine`). Rows written before read as not handed
    // over, and as change-log copies.
    //
    // The returning flag (section 9) is backfilled once, when its column is
    // new, in the transaction that adds it, so a crash leaves both or
    // neither: an own row that is not `deleted` and has a warm target row,
    // removed or not, was named this node's target, so a node in the middle
    // of a return at the upgrade keeps its file on a `deleted` entry too. Its
    // rev is 0, so the reconciler's next pass that runs the volume clears it.
    // `IMMEDIATE`: a write of another process between the column read and
    // the `ALTER` waits, instead of failing the start.
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(store_error)?;
    let added = add_missing_columns(
        &mut tx,
        "volumes",
        &[
            ("handing_over", "INTEGER NOT NULL DEFAULT 0"),
            ("hand_over_basis", "TEXT"),
            ("return_pending", "INTEGER NOT NULL DEFAULT 0"),
            ("return_pending_rev", "INTEGER NOT NULL DEFAULT 0"),
        ],
    )
    .await?;
    let backfilled: Vec<String> = if added.iter().any(|column| column == "return_pending") {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT volume_id FROM volumes WHERE state <> 'deleted' \
             AND volume_id IN (SELECT volume_id FROM warm_targets) ORDER BY volume_id",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(store_error)?;
        sqlx::query(
            "UPDATE volumes SET return_pending = 1, return_pending_rev = 0 \
             WHERE state <> 'deleted' AND volume_id IN (SELECT volume_id FROM warm_targets)",
        )
        .execute(&mut *tx)
        .await
        .map_err(store_error)?;
        ids
    } else {
        Vec::new()
    };
    tx.commit().await.map_err(store_error)?;
    for volume_id in &backfilled {
        warn!(
            volume_id,
            "the volume is kept as a returning source's volume of this node's own, \
             until the node is named its warm target or runs it again"
        );
    }
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(store_error)?;
    add_missing_columns(
        &mut tx,
        "quarantine",
        &[
            ("quarantine_id", "TEXT"),
            ("volume_id", "TEXT"),
            ("app_id", "TEXT"),
            ("size_bytes", "INTEGER"),
            ("sha256", "TEXT"),
            ("snapshot_id", "TEXT"),
        ],
    )
    .await?;
    tx.commit().await.map_err(store_error)?;
    // After the columns: an older database gains `quarantine_id` only above.
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS quarantine_by_id ON quarantine (quarantine_id) \
         WHERE quarantine_id IS NOT NULL",
    )
    .execute(pool)
    .await
    .map_err(store_error)?;

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

/// Adds each of `columns` (name and declaration) that `table` lacks, on
/// `conn`, so a caller's transaction holds the change, and returns the names
/// it added.
async fn add_missing_columns(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
    columns: &[(&str, &str)],
) -> Result<Vec<String>, AgentError> {
    let present: Vec<String> = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(&mut *conn)
        .await
        .map_err(store_error)?
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect();
    let mut added = Vec::new();
    for (column, kind) in columns {
        if !present.iter().any(|name| name == column) {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"))
                .execute(&mut *conn)
                .await
                .map_err(store_error)?;
            added.push((*column).to_owned());
        }
    }
    Ok(added)
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

    /// Section 8.8 "The term ceiling": no `desired_state` whose term is
    /// above 2^53 - 1 is stored, whatever reached the store.
    #[tokio::test]
    async fn a_desired_state_above_the_term_ceiling_is_not_stored() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        for term in [json!(MAX_TERM + 1), json!(u64::MAX)] {
            let body = json!({"rev": 9, "issued_at": "2026-11-02T10:15:31Z", "term": term, "signature": "x"});
            assert!(store.store_desired(9, &body).await.is_err(), "{term}");
            assert_eq!(store.latest_desired().await.unwrap(), None, "{term}");
        }
        let body = json!({"rev": 9, "issued_at": "2026-11-02T10:15:31Z", "term": MAX_TERM, "signature": "x"});
        store.store_desired(9, &body).await.unwrap();
        assert_eq!(store.latest_desired().await.unwrap().unwrap().0, 9);
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

    /// Sections 5.10 and 12 (ADR 0229): the newest `welcome.schema_version`
    /// is kept, never lowered, and survives a reopen; a stored value that is
    /// not a version is an error, not `None`.
    #[tokio::test]
    async fn the_welcome_schema_version_only_rises() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(store.welcome_schema_version().await.unwrap(), None);
        for (raised, kept) in [(14, 14), (12, 14), (14, 14), (15, 15)] {
            store.raise_welcome_schema_version(raised).await.unwrap();
            assert_eq!(
                store.welcome_schema_version().await.unwrap(),
                Some(kept),
                "after {raised}"
            );
        }
        drop(store);
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(store.welcome_schema_version().await.unwrap(), Some(15));
        store
            .set_meta(WELCOME_SCHEMA_VERSION, "fourteen")
            .await
            .unwrap();
        assert!(store.welcome_schema_version().await.is_err());
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

        let LogQuarantine::Moved(path) = store.quarantine_log("log_diverged: test").await.unwrap()
        else {
            panic!("a copy was written");
        };
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
        assert_eq!(
            store.quarantine_log("again").await.unwrap(),
            LogQuarantine::Empty
        );
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

        let LogQuarantine::Moved(path) = store.quarantine_log("test").await.unwrap() else {
            panic!("a copy was written");
        };
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
        assert!(matches!(
            store.quarantine_log("test").await.unwrap(),
            LogQuarantine::Moved(_)
        ));
        assert_eq!(quarantine_rows(&store).await.len(), 1);

        // A second start finds the tables and keeps the record.
        drop(store);
        let store = NodeStore::open(&dir.0).await.expect("the store reopens");
        assert_eq!(quarantine_rows(&store).await.len(), 1);
    }

    /// Section 9: a database from before the hand-over gains the mark on
    /// the volume rows and the volume columns of `quarantine`. Its rows read
    /// as before: a volume not handed over, a change-log copy that is no
    /// volume's.
    #[tokio::test]
    async fn an_older_database_gains_the_hand_over_mark_and_volume_quarantines() {
        let dir = TempDir::new();
        {
            let options = SqliteConnectOptions::new()
                .filename(dir.0.join("node.db"))
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(options).await.unwrap();
            sqlx::raw_sql(
                "CREATE TABLE node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE volumes (volume_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, \
                 size_gb INTEGER NOT NULL, state TEXT NOT NULL, error TEXT, \
                 allocated_bytes INTEGER); \
                 CREATE TABLE quarantine (id INTEGER PRIMARY KEY AUTOINCREMENT, \
                 kind TEXT NOT NULL, path TEXT NOT NULL, quarantined_at TEXT NOT NULL, \
                 reason TEXT NOT NULL, last_seq INTEGER, last_hash TEXT); \
                 INSERT INTO volumes (volume_id, app_id, size_gb, state) \
                 VALUES ('vol_a', 'app_a', 1, 'ready'); \
                 INSERT INTO quarantine (kind, path, quarantined_at, reason, last_seq) \
                 VALUES ('change_log', '/q/log', '2026-11-01T00:00:00Z', 'fork', 3);",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }
        let store = NodeStore::open(&dir.0).await.expect("the store opens");
        assert!(!store.volume_handing_over("vol_a").await.unwrap());
        assert_eq!(
            store.volume("vol_a").await.unwrap().unwrap().state,
            VolumeState::Ready
        );
        assert!(store.volume_quarantines().await.unwrap().is_empty());
        assert_eq!(quarantine_rows(&store).await.len(), 1);

        let (kept, record) = volume_copy("vol_a", "snap-1");
        store
            .record_volume_quarantine(&kept, &record)
            .await
            .unwrap();
        assert!(store.volume_handing_over("vol_a").await.unwrap());
        assert_eq!(store.volume_quarantines().await.unwrap(), vec![record]);
        assert_eq!(quarantine_rows(&store).await.len(), 2);

        // A second start finds the columns and keeps both.
        drop(store);
        let store = NodeStore::open(&dir.0).await.expect("the store reopens");
        assert!(store.volume_handing_over("vol_a").await.unwrap());
        assert_eq!(store.volume_quarantines().await.unwrap().len(), 1);
        assert_eq!(quarantine_rows(&store).await.len(), 2);
    }

    /// Section 9: a database from before the returning flag gains it once.
    /// An own row that is not `deleted` and has a warm target row, removed or
    /// not, carries it; every other row does not. A later start backfills
    /// nothing again.
    #[tokio::test]
    async fn an_older_database_flags_the_own_volumes_it_was_named_the_target_of() {
        let dir = TempDir::new();
        {
            let options = SqliteConnectOptions::new()
                .filename(dir.0.join("node.db"))
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(options).await.unwrap();
            sqlx::raw_sql(
                "CREATE TABLE node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
                 CREATE TABLE volumes (volume_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, \
                 size_gb INTEGER NOT NULL, state TEXT NOT NULL, error TEXT, \
                 allocated_bytes INTEGER, handing_over INTEGER NOT NULL DEFAULT 0, \
                 hand_over_basis TEXT); \
                 CREATE TABLE warm_targets (volume_id TEXT PRIMARY KEY, \
                 app_id TEXT NOT NULL, size_gb INTEGER NOT NULL, \
                 removed INTEGER NOT NULL DEFAULT 0); \
                 INSERT INTO volumes (volume_id, app_id, size_gb, state) VALUES \
                 ('vol_target', 'app_a', 1, 'ready'), \
                 ('vol_removed', 'app_a', 1, 'failed'), \
                 ('vol_plain', 'app_a', 1, 'ready'), \
                 ('vol_gone', 'app_a', 1, 'deleted'); \
                 INSERT INTO warm_targets (volume_id, app_id, size_gb, removed) VALUES \
                 ('vol_target', 'app_a', 1, 0), ('vol_removed', 'app_a', 1, 1), \
                 ('vol_gone', 'app_a', 1, 0), ('vol_copy', 'app_a', 1, 0);",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }
        let store = NodeStore::open(&dir.0).await.expect("the store opens");
        let flagged = |store: NodeStore| async move {
            let mut out = Vec::new();
            for id in [
                "vol_target",
                "vol_removed",
                "vol_plain",
                "vol_gone",
                "vol_copy",
            ] {
                if store.volume_return_pending(id).await.unwrap() {
                    out.push(id);
                }
            }
            out
        };
        assert_eq!(
            flagged(store.clone()).await,
            vec!["vol_target", "vol_removed"]
        );
        assert_eq!(
            store.own_volumes_return_pending().await.unwrap(),
            BTreeMap::from([
                ("vol_plain".to_owned(), false),
                ("vol_removed".to_owned(), true),
                ("vol_target".to_owned(), true),
            ])
        );
        assert_eq!(
            store.volume("vol_target").await.unwrap().unwrap().state,
            VolumeState::Ready
        );
        assert_eq!(
            store.volume_return_pending_rev("vol_target").await.unwrap(),
            Some(0),
            "any revision that runs the volume ends it"
        );

        // Once: a flag cleared after the upgrade stays cleared.
        store
            .clear_volume_return_pending("vol_target")
            .await
            .unwrap();
        drop(store);
        let store = NodeStore::open(&dir.0).await.expect("the store reopens");
        assert_eq!(flagged(store.clone()).await, vec!["vol_removed"]);
    }

    /// The columns of the returning flag and its backfill are one
    /// transaction: a start that stops between the `ALTER` and the backfill,
    /// here by a backfill that fails, leaves neither, and the next start does
    /// both.
    #[tokio::test]
    async fn a_start_cut_before_the_backfill_adds_no_column_and_the_next_one_backfills() {
        let dir = TempDir::new();
        let options = SqliteConnectOptions::new()
            .filename(dir.0.join("node.db"))
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
             CREATE TABLE volumes (volume_id TEXT PRIMARY KEY, app_id TEXT NOT NULL, \
             size_gb INTEGER NOT NULL, state TEXT NOT NULL, error TEXT, \
             allocated_bytes INTEGER); \
             CREATE TABLE warm_targets (volume_id TEXT PRIMARY KEY, \
             app_id TEXT NOT NULL, size_gb INTEGER NOT NULL, \
             removed INTEGER NOT NULL DEFAULT 0); \
             INSERT INTO volumes (volume_id, app_id, size_gb, state) \
             VALUES ('vol_target', 'app_a', 1, 'ready'); \
             INSERT INTO warm_targets (volume_id, app_id, size_gb) \
             VALUES ('vol_target', 'app_a', 1); \
             CREATE TRIGGER cut BEFORE UPDATE ON volumes \
             BEGIN SELECT RAISE(ABORT, 'the agent stopped'); END;",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert!(NodeStore::open(&dir.0).await.is_err(), "the start stops");
        let columns: Vec<String> = sqlx::query("PRAGMA table_info(volumes)")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .filter_map(|row| row.try_get::<String, _>("name").ok())
            .collect();
        for column in [
            "handing_over",
            "hand_over_basis",
            "return_pending",
            "return_pending_rev",
        ] {
            assert!(!columns.iter().any(|name| name == column), "{column}");
        }

        sqlx::query("DROP TRIGGER cut")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let store = NodeStore::open(&dir.0).await.expect("the store opens");
        assert_eq!(
            store.volume_return_pending_rev("vol_target").await.unwrap(),
            Some(0)
        );
    }

    /// Section 9: the returning flag is written with the warm target row,
    /// only on an own row that is not `deleted`, with the highest rev that
    /// named the node the target; saving the row keeps it, clearing the
    /// hand-over mark keeps it, only a newer revision ends it, with the warm
    /// row, and the end of a hand-over drops it with the row.
    #[tokio::test]
    async fn the_returning_flag_survives_the_row_s_writes_and_goes_with_it() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let row = |state| VolumeRecord {
            volume_id: "vol_a".to_owned(),
            app_id: "app_a".to_owned(),
            size_gb: 1,
            state,
            error: None,
            allocated_bytes: None,
        };
        // No own row: the warm row alone.
        assert!(
            !store
                .add_return_pending_target("vol_a", "app_a", 1, 3)
                .await
                .unwrap()
        );
        assert!(store.warm_target("vol_a").await.unwrap().is_some());
        assert!(!store.volume_return_pending("vol_a").await.unwrap());
        // A `deleted` own row is no volume of the node's own.
        store.save_volume(&row(VolumeState::Deleted)).await.unwrap();
        assert!(
            !store
                .add_return_pending_target("vol_a", "app_a", 1, 3)
                .await
                .unwrap()
        );
        assert!(!store.volume_return_pending("vol_a").await.unwrap());

        store.save_volume(&row(VolumeState::Ready)).await.unwrap();
        store.mark_warm_removed("vol_a").await.unwrap();
        assert!(
            store
                .add_return_pending_target("vol_a", "app_a", 1, 3)
                .await
                .unwrap()
        );
        assert!(store.volume_return_pending("vol_a").await.unwrap());
        assert!(
            !store.warm_target("vol_a").await.unwrap().unwrap().removed,
            "a removed copy's row is a target again"
        );
        store.save_volume(&row(VolumeState::Failed)).await.unwrap();
        assert!(store.volume_return_pending("vol_a").await.unwrap());
        store
            .mark_volume_handing_over("vol_a", &HandOverMark::Unchanged)
            .await
            .unwrap();
        store.clear_volume_handing_over("vol_a").await.unwrap();
        assert!(store.volume_return_pending("vol_a").await.unwrap());
        // The rev that named the node the target is raised, never lowered.
        assert!(
            store
                .add_return_pending_target("vol_a", "app_a", 1, 2)
                .await
                .unwrap()
        );
        assert_eq!(
            store.volume_return_pending_rev("vol_a").await.unwrap(),
            Some(3)
        );
        // Only a newer revision ends it, and the warm row with it.
        assert!(!store.end_return_pending("vol_a", 3).await.unwrap());
        assert!(store.volume_return_pending("vol_a").await.unwrap());
        assert!(store.warm_target("vol_a").await.unwrap().is_some());
        assert!(store.end_return_pending("vol_a", 4).await.unwrap());
        assert!(!store.volume_return_pending("vol_a").await.unwrap());
        assert_eq!(
            store.volume_return_pending_rev("vol_a").await.unwrap(),
            None
        );
        assert!(store.warm_target("vol_a").await.unwrap().is_none());
        assert!(!store.end_return_pending("vol_a", 5).await.unwrap());

        assert!(
            store
                .add_return_pending_target("vol_a", "app_a", 1, 3)
                .await
                .unwrap()
        );
        store
            .mark_volume_handing_over("vol_a", &HandOverMark::Unchanged)
            .await
            .unwrap();
        assert!(store.finish_hand_over("vol_a").await.unwrap());
        assert!(!store.volume_return_pending("vol_a").await.unwrap());
        assert!(store.own_volumes_return_pending().await.unwrap().is_empty());
    }

    /// A quarantined copy of `volume_id` and its kept snapshot.
    fn volume_copy(
        volume_id: &str,
        snapshot_id: &str,
    ) -> (KeptSnapshotRecord, VolumeQuarantineRecord) {
        let location = PathBuf::from(format!("/volumes/{volume_id}.{snapshot_id}"));
        let kept = KeptSnapshotRecord {
            app_id: "app_a".to_owned(),
            snapshot: quoro_engine::VolumeSnapshot {
                volume_id: quoro_engine::VolumeId(volume_id.to_owned()),
                snapshot_id: snapshot_id.to_owned(),
                created_at: std::time::UNIX_EPOCH + Duration::from_millis(1_000),
                size_bytes: 1 << 30,
                location: location.clone(),
                method: quoro_engine::SnapshotMethod::Reflink,
                pause: Duration::ZERO,
                guest_flush: quoro_engine::GuestFlush::Unsupported,
            },
            purpose: "quarantine".to_owned(),
            seq: None,
            cells: None,
        };
        let record = VolumeQuarantineRecord {
            quarantine_id: crate::util::quarantine_id(),
            volume_id: volume_id.to_owned(),
            app_id: "app_a".to_owned(),
            created_at: "2026-11-10T10:00:00Z".to_owned(),
            size_bytes: 8192,
            path: location,
            snapshot_id: snapshot_id.to_owned(),
            reason: "changed since the base".to_owned(),
            sha256: None,
        };
        (kept, record)
    }

    /// Section 9 step 2: only a record without a SHA-256 that still names
    /// the snapshot is forgotten for a lost clone, with its kept snapshot.
    #[tokio::test]
    async fn only_an_unhashed_record_is_forgotten_for_a_lost_clone() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let (kept, record) = volume_copy("vol_a", "snap-1");
        store
            .record_volume_quarantine(&kept, &record)
            .await
            .unwrap();
        let (hashed_kept, hashed) = volume_copy("vol_a", "snap-2");
        store
            .record_volume_quarantine(&hashed_kept, &hashed)
            .await
            .unwrap();
        assert!(
            store
                .set_quarantine_sha256(&hashed.quarantine_id, &"ab".repeat(32))
                .await
                .unwrap()
        );

        // A hashed record stays, and so does its kept snapshot.
        assert!(
            !store
                .forget_unhashed_volume_quarantine(&hashed.quarantine_id, "snap-2")
                .await
                .unwrap()
        );
        assert!(
            store
                .volume_quarantine(&hashed.quarantine_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.kept_snapshot("snap-2").await.unwrap().is_some());
        // Another snapshot than the record's changes nothing.
        assert!(
            !store
                .forget_unhashed_volume_quarantine(&record.quarantine_id, "snap-2")
                .await
                .unwrap()
        );
        assert!(store.kept_snapshot("snap-2").await.unwrap().is_some());

        assert!(
            store
                .forget_unhashed_volume_quarantine(&record.quarantine_id, "snap-1")
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .volume_quarantine(&record.quarantine_id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(store.kept_snapshot("snap-1").await.unwrap(), None);
        // Once gone, nothing more.
        assert!(
            !store
                .forget_unhashed_volume_quarantine(&record.quarantine_id, "snap-1")
                .await
                .unwrap()
        );
        let left: Vec<String> = store
            .volume_quarantines()
            .await
            .unwrap()
            .into_iter()
            .map(|left| left.quarantine_id)
            .collect();
        assert_eq!(left, [hashed.quarantine_id]);
    }

    /// Section 9: a quarantined copy is recorded with its kept snapshot and
    /// the mark in one transaction, gets its hash once, and is forgotten
    /// with its kept snapshot; the end of a hand-over drops a marked row only
    /// and leaves the warm row with no copy.
    #[tokio::test]
    async fn a_volume_quarantine_and_a_hand_over_round_trip() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        store
            .save_volume(&VolumeRecord {
                volume_id: "vol_a".to_owned(),
                app_id: "app_a".to_owned(),
                size_gb: 1,
                state: VolumeState::Ready,
                error: None,
                allocated_bytes: Some(4096),
            })
            .await
            .unwrap();
        store.add_warm_target("vol_a", "app_a", 1).await.unwrap();
        let mut row = store.warm_target("vol_a").await.unwrap().unwrap();
        row.complete_seq = Some(4);
        row.taken_at = Some("2026-11-10T09:00:00Z".to_owned());
        row.error = Some("this node holds the volume as its own".to_owned());
        store.save_warm_target(&row).await.unwrap();

        // Not marked: the end drops nothing.
        assert!(!store.finish_hand_over("vol_a").await.unwrap());
        assert!(store.volume("vol_a").await.unwrap().is_some());

        let (kept, record) = volume_copy("vol_a", "snap-1");
        store
            .record_volume_quarantine(&kept, &record)
            .await
            .unwrap();
        assert!(store.volume_handing_over("vol_a").await.unwrap());
        assert_eq!(
            store.volume_hand_over_mark("vol_a").await.unwrap(),
            Some(HandOverMark::Copy(record.quarantine_id.clone())),
            "the mark stands for the copy"
        );
        assert_eq!(
            store.volumes_handing_over().await.unwrap(),
            BTreeSet::from(["vol_a".to_owned()])
        );
        assert_eq!(
            store
                .volume_quarantine(&record.quarantine_id)
                .await
                .unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store.kept_snapshot("snap-1").await.unwrap(),
            Some(kept.clone())
        );
        // A second record under the same id is refused, and changes nothing.
        let (other_kept, mut other) = volume_copy("vol_a", "snap-2");
        other.quarantine_id = record.quarantine_id.clone();
        assert!(
            store
                .record_volume_quarantine(&other_kept, &other)
                .await
                .is_err()
        );
        assert_eq!(store.kept_snapshot("snap-2").await.unwrap(), None);

        // The hash is recorded once.
        let sha256 = "ab".repeat(32);
        assert!(
            store
                .set_quarantine_sha256(&record.quarantine_id, &sha256)
                .await
                .unwrap()
        );
        assert!(
            !store
                .set_quarantine_sha256(&record.quarantine_id, &"cd".repeat(32))
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .volume_quarantine(&record.quarantine_id)
                .await
                .unwrap()
                .unwrap()
                .sha256,
            Some(sha256)
        );

        // The end: the marked row goes, the warm row has no copy and no error.
        assert!(store.finish_hand_over("vol_a").await.unwrap());
        assert!(store.volume("vol_a").await.unwrap().is_none());
        let row = store.warm_target("vol_a").await.unwrap().unwrap();
        assert_eq!(row.complete_seq, None);
        assert_eq!(row.taken_at, None);
        assert_eq!(row.error, None);
        assert!(store.volumes_handing_over().await.unwrap().is_empty());

        // The operator's delete forgets both records; once more is a no-op.
        store
            .forget_volume_quarantine(&record.quarantine_id, "snap-1")
            .await
            .unwrap();
        assert!(store.volume_quarantines().await.unwrap().is_empty());
        assert_eq!(store.kept_snapshot("snap-1").await.unwrap(), None);
        store
            .forget_volume_quarantine(&record.quarantine_id, "snap-1")
            .await
            .unwrap();
        assert!(
            !store
                .set_quarantine_sha256(&record.quarantine_id, &"ef".repeat(32))
                .await
                .unwrap()
        );

        // A mark on a row the node runs again is cleared.
        store
            .save_volume(&VolumeRecord {
                volume_id: "vol_b".to_owned(),
                app_id: "app_a".to_owned(),
                size_gb: 1,
                state: VolumeState::Ready,
                error: None,
                allocated_bytes: None,
            })
            .await
            .unwrap();
        store
            .mark_volume_handing_over("vol_b", &HandOverMark::Unchanged)
            .await
            .unwrap();
        // Saving the row again keeps the mark.
        store
            .save_volume(&VolumeRecord {
                volume_id: "vol_b".to_owned(),
                app_id: "app_a".to_owned(),
                size_gb: 1,
                state: VolumeState::Ready,
                error: None,
                allocated_bytes: Some(1),
            })
            .await
            .unwrap();
        assert!(store.volume_handing_over("vol_b").await.unwrap());
        assert_eq!(
            store.volume_hand_over_mark("vol_b").await.unwrap(),
            Some(HandOverMark::Unchanged)
        );
        store.clear_volume_handing_over("vol_b").await.unwrap();
        assert!(!store.volume_handing_over("vol_b").await.unwrap());
        assert_eq!(store.volume_hand_over_mark("vol_b").await.unwrap(), None);
        store
            .mark_volume_handing_over("vol_b", &HandOverMark::Gone)
            .await
            .unwrap();
        assert_eq!(
            store.volume_hand_over_mark("vol_b").await.unwrap(),
            Some(HandOverMark::Gone)
        );
        // A mark whose basis cannot be read stands for nothing.
        sqlx::query("UPDATE volumes SET hand_over_basis = NULL WHERE volume_id = 'vol_b'")
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.volume_handing_over("vol_b").await.unwrap());
        assert_eq!(store.volume_hand_over_mark("vol_b").await.unwrap(), None);
    }

    /// One unreadable quarantine row leaves the others listed.
    #[tokio::test]
    async fn an_unreadable_volume_quarantine_row_is_left_out_of_the_list() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        sqlx::query(
            "INSERT INTO quarantine (kind, path, quarantined_at, reason) \
             VALUES (?, '/gone', '2026-11-10T09:00:00Z', 'no id')",
        )
        .bind(VOLUME_KIND)
        .execute(&store.pool)
        .await
        .unwrap();
        let (kept, record) = volume_copy("vol_a", "snap-1");
        store
            .record_volume_quarantine(&kept, &record)
            .await
            .unwrap();
        assert_eq!(
            store.volume_quarantines().await.unwrap(),
            vec![record.clone()]
        );
        // Its clone path read alone, for a marked hand-over.
        assert_eq!(
            store
                .volume_quarantine_path(&record.quarantine_id)
                .await
                .unwrap(),
            Some(record.path.clone())
        );
        assert_eq!(
            store
                .volume_quarantine_path("qrn_01JD0WARM0000000000000000Z")
                .await
                .unwrap(),
            None
        );
        // Its id and snapshot read alone, for a delete.
        assert_eq!(
            store
                .volume_quarantine_snapshot_id(&record.quarantine_id)
                .await
                .unwrap(),
            Some("snap-1".to_owned())
        );
        assert_eq!(
            store
                .volume_quarantine_snapshot_id("qrn_01JD0WARM0000000000000000Z")
                .await
                .unwrap(),
            None
        );
    }

    /// The stored refollowing value, as it is in `node_meta`.
    async fn refollowing_value(store: &NodeStore) -> Option<String> {
        store.meta(LOG_REFOLLOWING).await.unwrap()
    }

    /// Section 7.1 "Refollowing", section 12: the state reads back as it was
    /// set, across a reopen, for any `head_seq` the wire can carry; a value
    /// that is neither form is an error, not "not refollowing".
    #[tokio::test]
    async fn the_refollowing_state_round_trips() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(store.refollowing().await.unwrap(), None);

        assert_eq!(
            store
                .set_refollowing(Refollowing::HeadUnknown)
                .await
                .unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );
        assert_eq!(
            refollowing_value(&store).await.as_deref(),
            Some("head unknown")
        );
        for head in [0, 42, crate::follow::MAX_SEQ, u64::MAX] {
            assert_eq!(
                store
                    .set_refollowing(Refollowing::Head(head))
                    .await
                    .unwrap(),
                FreezeChecked::Written
            );
            assert_eq!(
                store.refollowing().await.unwrap(),
                Some(Refollowing::Head(head))
            );
        }
        assert_eq!(
            store.set_refollowing(Refollowing::Head(42)).await.unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(refollowing_value(&store).await.as_deref(), Some("42"));
        drop(store);

        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(42))
        );
        store.set_meta(LOG_REFOLLOWING, "head").await.unwrap();
        assert!(store.refollowing().await.is_err());
    }

    /// Section 7.1 "Refollowing": a quarantine clears the primary's
    /// `head_seq` but keeps the refollowing state the session recorded
    /// before it, whichever form it has.
    #[tokio::test]
    async fn quarantine_keeps_the_refollowing_state() {
        for state in [Refollowing::HeadUnknown, Refollowing::Head(9)] {
            let dir = TempDir::new();
            let store = NodeStore::open(&dir.0).await.unwrap();
            let entries = verified(1, ZERO_HASH, 3, "a");
            store.store_log_batch(&entries, Some(3)).await.unwrap();
            assert_eq!(
                store.set_refollowing(state).await.unwrap(),
                FreezeChecked::Written
            );

            assert!(matches!(
                store.quarantine_log("test").await.unwrap(),
                LogQuarantine::Moved(_)
            ));
            assert!(store.log_entries().await.unwrap().is_empty());
            assert_eq!(store.log_head().await.unwrap(), None);
            assert_eq!(store.refollowing().await.unwrap(), Some(state));
        }
    }

    /// Section 7.1 "Refollowing": the batch that brings the copy to the head
    /// the node refollows to ends the state in its own transaction. A batch
    /// below the head keeps it, a batch that fails keeps it with nothing
    /// stored, and with the head unknown no batch ends it.
    #[tokio::test]
    async fn the_batch_that_reaches_the_head_ends_refollowing_with_it() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
            FreezeChecked::Written
        );
        let first = verified(1, ZERO_HASH, 3, "a");
        assert_eq!(
            store.store_log_batch(&first, Some(5)).await.unwrap(),
            BatchStored::Stored
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );

        // The batch that would reach the head fails: neither its rows nor
        // the end of the state are kept.
        let mut second = verified(4, &first[2].hash, 2, "a");
        second.push(second[0].clone());
        assert!(store.store_log_batch(&second, Some(5)).await.is_err());
        assert_eq!(store.log_entries().await.unwrap(), first);
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );

        // The same batch, whole, reaches seq 5 and ends the state.
        let second = verified(4, &first[2].hash, 2, "a");
        assert_eq!(
            store.store_log_batch(&second, Some(8)).await.unwrap(),
            BatchStored::RefollowingEnded
        );
        assert_eq!(store.refollowing().await.unwrap(), None);
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 8);

        // A batch past the head ends it too, and with the head unknown none
        // does.
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store
                .set_refollowing(Refollowing::HeadUnknown)
                .await
                .unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(
            store.store_log_batch(&first, Some(2)).await.unwrap(),
            BatchStored::Stored
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );
        assert_eq!(
            store.set_refollowing(Refollowing::Head(2)).await.unwrap(),
            FreezeChecked::Written
        );
        let more = verified(4, &first[2].hash, 1, "a");
        assert_eq!(
            store.store_log_batch(&more, None).await.unwrap(),
            BatchStored::RefollowingEnded
        );
        assert_eq!(store.refollowing().await.unwrap(), None);
    }

    /// Section 7.1 "The node's mark", section 12: no file is no mark, a
    /// decimal `seq` is the mark, and anything else is an error. Whether the
    /// stored copy is below it follows the mark, a missing one included.
    #[tokio::test]
    async fn the_mark_file_is_missing_valid_or_unparsable() {
        use crate::follow::read_mark;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let path = dir.0.join(crate::identity::LOG_MARK_FILE);
        assert_eq!(read_mark(&dir.0).unwrap(), None);
        assert!(!store.log_below_mark().await.unwrap());

        std::fs::write(&path, "3").unwrap();
        assert_eq!(read_mark(&dir.0).unwrap(), Some(3));
        assert!(store.log_below_mark().await.unwrap());
        store
            .store_log_batch(&verified(1, ZERO_HASH, 3, "a"), None)
            .await
            .unwrap();
        assert!(!store.log_below_mark().await.unwrap());
        std::fs::write(&path, "9007199254740991\n").unwrap();
        assert_eq!(read_mark(&dir.0).unwrap(), Some(crate::follow::MAX_SEQ));
        assert!(store.log_below_mark().await.unwrap());

        for text in ["", "x", "-1", "3.0", "18446744073709551616"] {
            std::fs::write(&path, text).unwrap();
            assert!(read_mark(&dir.0).is_err(), "{text:?}");
            assert!(store.log_below_mark().await.is_err(), "{text:?}");
        }
    }

    /// Section 7.1 "The node's mark": the mark is written as the decimal
    /// `seq`, `0600`. A raise never lowers it and a missing mark counts as 0,
    /// so raising to 0 creates no file; a lower never raises it. A lower
    /// writes a missing mark (section 7.1 "Refollowing"), so a later raise
    /// below the head the node refollows to writes nothing.
    #[tokio::test]
    async fn the_mark_is_raised_and_lowered_only_as_asked() {
        use crate::follow::{lower_mark, raise_mark, write_mark};
        let dir = TempDir::new();
        let path = dir.0.join(crate::identity::LOG_MARK_FILE);
        raise_mark(&dir.0, 0).await.unwrap();
        assert!(!path.exists());
        assert!(lower_mark(&dir.0, 5).await.unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "5");
        raise_mark(&dir.0, 3).await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "5");
        assert!(!lower_mark(&dir.0, 5).await.unwrap());
        std::fs::remove_file(&path).unwrap();

        raise_mark(&dir.0, 7).await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "7");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        raise_mark(&dir.0, 5).await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "7");
        assert!(!lower_mark(&dir.0, 9).await.unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "7");
        assert!(lower_mark(&dir.0, 4).await.unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "4");

        write_mark(&dir.0, crate::follow::MAX_SEQ).await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "9007199254740991");
        let names: Vec<String> = std::fs::read_dir(&dir.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("log-mark"))
            .collect();
        assert_eq!(names, ["log-mark"]);
    }
}
