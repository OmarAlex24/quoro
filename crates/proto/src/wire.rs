// SPDX-License-Identifier: AGPL-3.0-only
//! The wire form of the node-agent session (`node-agent-protocol.md`
//! sections 3 and 5).
//!
//! Every frame is one JSON text message: the envelope `{v, type, id, re?,
//! body}`. Decoding happens in two steps so the receiver can answer each kind
//! of failure the way section 3.4 requires:
//!
//! 1. [`RawFrame::parse`] reads the envelope and checks size and `v`
//!    ([`DecodeError::TooLarge`], [`DecodeError::Malformed`],
//!    [`DecodeError::UnsupportedVersion`]).
//! 2. [`NodeMessage::from_raw`] / [`ControlMessage::from_raw`] map `type` to a
//!    typed body ([`DecodeError::UnknownType`], [`DecodeError::BadBody`]).
//!
//! Unknown fields inside a known body are ignored (serde's default), which is
//! the additive-change rule of section 3.4.
//!
//! `desired_state` and `signing_key_rotate` are carried as the raw body
//! [`Value`]: the node verifies the signature over exactly what arrived
//! ([`crate::sign::verify_body`]) before it parses anything
//! ([`DesiredState::from_value`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CommandResult, LeaseGrant, LeaseRenew, LeaseRevoke, ProtocolError};

/// Envelope major version (section 3.3).
pub const VERSION: u64 = 1;

/// Maximum size of one frame in bytes (section 3.2).
pub const MAX_MESSAGE_BYTES: usize = 1 << 20;

/// Default heartbeat interval (section 4.3).
pub const HEARTBEAT_INTERVAL_MS: u64 = 5_000;

/// Default session timeout without any frame (section 4.3).
pub const SESSION_TIMEOUT_MS: u64 = 15_000;

/// Most entries a `welcome.primary_urls` list may carry (section 5.10).
pub const MAX_PRIMARY_URLS: usize = 8;

/// Status report interval (section 4.3).
pub const STATUS_REPORT_INTERVAL_MS: u64 = 30_000;

/// The envelope of every frame, with the body still untyped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawFrame {
    /// Envelope major version.
    pub v: u64,
    /// Message type (`README.md` "Node-agent message types").
    #[serde(rename = "type")]
    pub kind: String,
    /// `msg_` id of this message.
    pub id: String,
    /// `msg_` id of the message this one answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub re: Option<String>,
    /// Message-specific body.
    #[serde(default = "empty_object")]
    pub body: Value,
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Why a frame could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Larger than [`MAX_MESSAGE_BYTES`]: `bad_message`, non-fatal.
    TooLarge(usize),
    /// Not a JSON envelope: `bad_message`, non-fatal.
    Malformed(String),
    /// A major version this side does not implement: `unsupported_version`,
    /// fatal.
    UnsupportedVersion(u64),
    /// A `type` this side does not know: `bad_message`, non-fatal. Carries the
    /// envelope id so the error can set `re`.
    UnknownType { kind: String, id: String },
    /// A known `type` whose body does not have the required shape:
    /// `bad_message`, non-fatal.
    BadBody {
        kind: String,
        id: String,
        reason: String,
    },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge(size) => write!(f, "frame of {size} bytes exceeds the 1 MiB limit"),
            Self::Malformed(reason) => write!(f, "malformed frame: {reason}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported envelope version {v}"),
            Self::UnknownType { kind, .. } => write!(f, "unknown message type {kind:?}"),
            Self::BadBody { kind, reason, .. } => write!(f, "bad {kind} body: {reason}"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl DecodeError {
    /// The error code section 5.18 answers this failure with.
    #[must_use]
    pub fn code(&self) -> crate::ErrorCode {
        match self {
            Self::UnsupportedVersion(_) => crate::ErrorCode::UnsupportedVersion,
            _ => crate::ErrorCode::BadMessage,
        }
    }

    /// Whether the session must end (section 3.4).
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::UnsupportedVersion(_))
    }

    /// The id of the offending message, when the envelope was readable.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        match self {
            Self::UnknownType { id, .. } | Self::BadBody { id, .. } => Some(id),
            _ => None,
        }
    }
}

impl RawFrame {
    /// Reads the envelope of one text frame.
    ///
    /// # Errors
    ///
    /// [`DecodeError::TooLarge`], [`DecodeError::Malformed`] or
    /// [`DecodeError::UnsupportedVersion`].
    pub fn parse(text: &str) -> Result<Self, DecodeError> {
        if text.len() > MAX_MESSAGE_BYTES {
            return Err(DecodeError::TooLarge(text.len()));
        }
        let frame: RawFrame =
            serde_json::from_str(text).map_err(|e| DecodeError::Malformed(e.to_string()))?;
        if frame.v != VERSION {
            return Err(DecodeError::UnsupportedVersion(frame.v));
        }
        if !frame.body.is_object() {
            return Err(DecodeError::Malformed("body is not an object".to_owned()));
        }
        Ok(frame)
    }

    /// The JSON text of this frame.
    #[must_use]
    pub fn to_text(&self) -> String {
        // Serializing a struct of strings, integers and a `Value` cannot fail.
        serde_json::to_string(self).unwrap_or_default()
    }

    fn body_as<T: for<'de> Deserialize<'de>>(&self) -> Result<T, DecodeError> {
        serde_json::from_value(self.body.clone()).map_err(|e| DecodeError::BadBody {
            kind: self.kind.clone(),
            id: self.id.clone(),
            reason: e.to_string(),
        })
    }
}

/// Builds a frame from a type, ids and a serializable body.
#[must_use]
pub fn frame<B: Serialize>(kind: &str, id: &str, re: Option<&str>, body: &B) -> RawFrame {
    RawFrame {
        v: VERSION,
        kind: kind.to_owned(),
        id: id.to_owned(),
        re: re.map(str::to_owned),
        body: serde_json::to_value(body).unwrap_or_else(|_| empty_object()),
    }
}

/// A fresh `msg_` id: the prefix and 26 Crockford base32 characters from the
/// given 16 random bytes. Message ids are message-local and never persisted
/// (section 3.3), so they only have to be unique within a session.
#[must_use]
pub fn msg_id(random: [u8; 16]) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let value = u128::from_be_bytes(random);
    let mut out = String::with_capacity(30);
    out.push_str("msg_");
    for i in (0..26).rev() {
        let index = ((value >> (i * 5)) & 0x1f) as usize;
        out.push(char::from(ALPHABET[index]));
    }
    out
}

// --- shared shapes ----------------------------------------------------------

/// `cpus`, `memory_mb`, `disk_gb` (sections 2.3, 5.1, 5.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub cpus: u32,
    pub memory_mb: u64,
    pub disk_gb: u64,
}

/// The KVM probe (sections 2.3, 5.1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kvm {
    pub available: bool,
    pub kernel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<bool>,
}

/// Which output stream a log line came from (section 5.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// One log line (section 5.7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    /// RFC 3339 time of the line.
    pub time: String,
    pub stream: LogStream,
    /// The line without its trailing newline.
    pub line: String,
    /// The VM that produced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<String>,
    /// Position in a build's log, gap-free from 1; set only on build log
    /// lines (section 5.7, ADR 0119). Added in M4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// The BuildKit step a build log line belongs to, when it names one.
    /// Added in M4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vertex: Option<u32>,
}

// --- node to control plane --------------------------------------------------

/// A lease the node knows, as `hello` reports it (section 5.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloLease {
    pub lease_id: String,
    pub resource_type: crate::ResourceType,
    pub resource_id: String,
    pub mode: crate::LeaseMode,
    pub fencing_token: u64,
    /// `held`, `uncertain`, `expired` or `revoked`.
    pub state: String,
}

/// Body of `hello` (section 5.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub node_id: String,
    pub agent_version: String,
    pub engine_version: String,
    pub boot_id: String,
    pub stored_rev: u64,
    pub applied_rev: u64,
    pub log_seq: u64,
    pub log_hash: String,
    pub leases: Vec<HelloLease>,
    pub capacity: Capacity,
    pub allocated: Capacity,
    pub kvm: Kvm,
    pub dropped_events: u64,
    /// Base64 WireGuard public key of this machine (32 bytes), from M5a
    /// (section 5.1, ADR 0144). The private key never leaves the machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh_public_key: Option<String>,
    /// How this node snapshots a volume (ADR 0146): `reflink` or
    /// `pause_copy`. Added in M5a; absent from older agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_snapshot: Option<SnapshotMethod>,
}

/// How a node snapshots a volume (section 5.1, ADR 0146).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMethod {
    Reflink,
    PauseCopy,
}

/// Body of `heartbeat` (section 5.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub time: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy: Option<bool>,
}

/// `observed_status` of a VM (`README.md` status enums).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObservedStatus {
    Creating,
    Stopped,
    Starting,
    Running,
    Stopping,
    Crashed,
    Deleted,
    Unknown,
}

/// `health` of a VM (section 5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    Passing,
    Failing,
    Unknown,
}

/// One VM in a `status_report` (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmStatus {
    pub vm_id: String,
    pub observed_status: ObservedStatus,
    pub health: Health,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    pub restart_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i64>,
}

/// One route in a `status_report` (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteStatus {
    pub hostname: String,
    /// `pending`, `issued`, `renewing`, `failed` or `not_applicable`.
    pub cert_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_expires_at: Option<String>,
}

/// Body of `status_report` (section 5.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatusReport {
    pub applied_rev: u64,
    pub vms: Vec<VmStatus>,
    pub routes: Vec<RouteStatus>,
    pub capacity: Capacity,
    pub allocated: Capacity,
    pub disk_free_gb: f64,
    /// One entry per build in the node's current `spec.builds` (section 5.3,
    /// ADR 0117). Added in M4; absent when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builds: Vec<BuildStatus>,
    /// On a machine with the `edge` role: one entry per backend of
    /// `spec.edge.routes` (section 5.3, ADR 0143). Added in M5a.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edge_backends: Vec<EdgeBackendStatus>,
    /// One entry per volume of `spec.volumes`, plus one per volume file the
    /// node holds that `spec.volumes` does not name (section 5.3, ADR 0145).
    /// Added in M5a; absent when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeStatus>,
    /// One entry per job of `spec.backups` (section 5.3, ADR 0147). Added in
    /// M5a; absent when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backups: Vec<BackupStatus>,
    /// One entry per prepared image the node holds, with its inventory hash
    /// (section 5.3, ADR 0150). Added in M5a; absent when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ImageStatus>,
    /// One entry per volume of `spec.volumes` that carries `warm`, on the
    /// source and on the target (sections 5.3 and 6.13, ADR 0173). Added in
    /// M5b; absent when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warm: Vec<WarmStatus>,
}

/// One entry of `status_report.warm` (section 5.3, section 6.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmStatus {
    pub volume_id: String,
    pub app_id: String,
    pub role: WarmRole,
    /// On the target: the newest complete copy it holds. On the source: the
    /// newest snapshot the target confirmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// RFC 3339: when the source took that snapshot, from the stream header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_taken_at: Option<String>,
    /// RFC 3339. On the target: when it swapped that copy in. On the source:
    /// when the target's confirmation arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    /// On the target: bytes its complete copy uses on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_bytes: Option<u64>,
    /// On the source: the extents of the newest snapshot it took.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extent_count: Option<u64>,
    /// On the source: true while the app's VM is paused by
    /// `switchover_freeze` (section 5.17).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<bool>,
    /// On the target: the token of the last stream it refused as stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_token: Option<u64>,
    /// RFC 3339: when the target refused that stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_at: Option<String>,
    /// One line on the last failed cycle or stream, at most 500 characters;
    /// absent once a cycle succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One entry of `status_report.images` (section 5.3, ADR 0150).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageStatus {
    pub image_digest: String,
    /// Hex SHA-256 of the image's inventory (section 6.11); absent while the
    /// node has no inventory for the image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_sha256: Option<String>,
}

/// Why an edge took a backend out of rotation (section 5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeOutReason {
    /// Its probe failed `failures` times in a row.
    HealthCheck,
    /// A request could not connect or was reset.
    ConnectFailed,
}

/// One backend of `spec.edge.routes` as the edge sees it (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeBackendStatus {
    pub app_id: String,
    pub vm_id: String,
    pub node_id: String,
    pub in_rotation: bool,
    /// RFC 3339: when `in_rotation` last changed, or when the edge first saw
    /// the backend.
    pub since: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EdgeOutReason>,
    /// At most 500 characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// `state` of a volume entry in `status_report` (section 5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeState {
    Creating,
    Ready,
    Restoring,
    Failed,
    /// The file was removed after a `deleted` entry in `spec.volumes`.
    Deleted,
    /// The node holds the file and `spec.volumes` does not name it.
    Orphaned,
}

/// One entry of `status_report.volumes` (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeStatus {
    pub volume_id: String,
    pub state: VolumeState,
    /// Bytes the sparse file uses on disk; present when the file exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_bytes: Option<u64>,
    /// One-line reason on `failed`, at most 2 000 characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `status` of a backup entry in `status_report` (section 5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupState {
    Queued,
    Running,
    /// The node wrote the manifest; the backup is not sealed yet.
    Succeeded,
    Failed,
}

/// One entry of `status_report.backups` (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupStatus {
    pub backup_id: String,
    pub status: BackupState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_method: Option<SnapshotMethod>,
    /// For `pause_copy`: how long the VM was paused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_ms: Option<u64>,
    /// From `running` (M5b, ADR 0282): `true` when the engine reported
    /// `Flushed` or `NoRunningVm`, `false` for any other outcome, which
    /// leaves the backup crash-consistent only. It never fails the backup.
    /// Absent from an agent older than M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_flush: Option<bool>,
    /// On `succeeded`: the manifest's key, relative to the repository root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    /// On `succeeded`: BLAKE3 of the encrypted manifest, 64 lowercase hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uploaded_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_count: Option<u64>,
    /// One-line reason on `failed`, at most 2 000 characters;
    /// `lease_mismatch` when the job's token is not the node's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// RFC 3339; when the node left `queued`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// RFC 3339; when the node finished its part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// Status of a build (README.md "Status enums", ADR 0117).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Queued,
    Preparing,
    Building,
    Exporting,
    Importing,
    Succeeded,
    Failed,
    Cancelled,
}

impl BuildState {
    /// `succeeded`, `failed` and `cancelled` are terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// The wire name, for example `preparing`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Preparing => "preparing",
            Self::Building => "building",
            Self::Exporting => "exporting",
            Self::Importing => "importing",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// The phase a build failed in (section 5.3, ADR 0117).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildPhase {
    Preparing,
    Building,
    Exporting,
    Importing,
}

impl BuildPhase {
    /// The wire name, for example `building`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Building => "building",
            Self::Exporting => "exporting",
            Self::Importing => "importing",
        }
    }
}

/// The BuildKit step a build failed in (section 5.3, ADR 0119): at most 20
/// excerpt lines of at most 500 characters each.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedStep {
    pub vertex: u32,
    pub name: String,
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dockerfile_line: Option<u32>,
    #[serde(default)]
    pub excerpt: Vec<String>,
}

/// One entry of `status_report.builds` (section 5.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStatus {
    pub build_id: String,
    pub status: BuildState,
    /// Full commit SHA the ref resolved to, once the checkout finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Manifest digest of the built image, on `succeeded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_phase: Option<BuildPhase>,
    /// One-line reason on `failed`, at most 2 000 characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<FailedStep>,
    /// RFC 3339; when the node left `queued`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// RFC 3339; when the build reached a terminal status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// Body of `event`: the event envelope without `seq` (section 5.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBody {
    pub schema_version: u32,
    /// `evt_` id, generated by the node; the primary deduplicates by it.
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub time: String,
    /// `{ "kind": "node", "node_id": <nod_> }`.
    pub source: Value,
    /// `{ "type": ..., "id": ... }`.
    pub subject: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incident_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Value>,
    pub data: Value,
}

/// Body of `log_ack` (section 5.6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogAck {
    pub seq: u64,
    pub hash: String,
}

/// Body of `stream_data` (section 5.7).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamData {
    pub stream_id: String,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lines: Option<Vec<LogLine>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// One sample per array, for a `metrics` stream (section 5.17).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<crate::metrics::MetricsReport>,
}

/// `reason` of `stream_end` (section 5.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamEndReason {
    Completed,
    Error,
    Cancelled,
    /// A `terminal` whose VM stopped, or a `console` whose VM was deleted (M4).
    VmStopped,
    /// A `terminal` whose transcript reached its cap (M4).
    Limit,
}

/// Body of `stream_end` (section 5.8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEnd {
    pub stream_id: String,
    pub reason: StreamEndReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// `terminal` only: the program's exit code, or `128 + signal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// `terminal` only: lowercase hex SHA-256 of the transcript as stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_sha256: Option<String>,
    /// `terminal` only: the transcript's size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_bytes: Option<u64>,
}

/// A node-to-control-plane message with a typed body.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeMessage {
    Hello(Hello),
    Heartbeat(Heartbeat),
    StatusReport(StatusReport),
    Event(EventBody),
    LeaseRenew(LeaseRenew),
    LogAck(LogAck),
    StreamData(StreamData),
    StreamEnd(StreamEnd),
    CommandResult(CommandResult),
    DnsChallenge(DnsChallenge),
    ImageInventory(ImageInventoryBody),
    /// From M5b, a voter's answer to `support_request` (section 5.28).
    SupportGrant(SupportGrant),
    Error(ProtocolError),
    Ping,
    Pong,
}

impl NodeMessage {
    /// The wire `type` of this message.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Hello(_) => "hello",
            Self::Heartbeat(_) => "heartbeat",
            Self::StatusReport(_) => "status_report",
            Self::Event(_) => "event",
            Self::LeaseRenew(_) => "lease_renew",
            Self::LogAck(_) => "log_ack",
            Self::StreamData(_) => "stream_data",
            Self::StreamEnd(_) => "stream_end",
            Self::CommandResult(_) => "command_result",
            Self::DnsChallenge(_) => "dns_challenge",
            Self::ImageInventory(_) => "image_inventory",
            Self::SupportGrant(_) => "support_grant",
            Self::Error(_) => "error",
            Self::Ping => "ping",
            Self::Pong => "pong",
        }
    }

    /// The frame for this message.
    #[must_use]
    pub fn to_frame(&self, id: &str, re: Option<&str>) -> RawFrame {
        let kind = self.kind();
        match self {
            Self::Hello(b) => frame(kind, id, re, b),
            Self::Heartbeat(b) => frame(kind, id, re, b),
            Self::StatusReport(b) => frame(kind, id, re, b),
            Self::Event(b) => frame(kind, id, re, b),
            Self::LeaseRenew(b) => frame(kind, id, re, b),
            Self::LogAck(b) => frame(kind, id, re, b),
            Self::StreamData(b) => frame(kind, id, re, b),
            Self::StreamEnd(b) => frame(kind, id, re, b),
            Self::CommandResult(b) => frame(kind, id, re, b),
            Self::DnsChallenge(b) => frame(kind, id, re, b),
            Self::ImageInventory(b) => frame(kind, id, re, b),
            Self::SupportGrant(b) => frame(kind, id, re, b),
            Self::Error(b) => frame(kind, id, re, b),
            Self::Ping | Self::Pong => frame(kind, id, re, &empty_object()),
        }
    }

    /// Maps a parsed envelope to a typed message.
    ///
    /// # Errors
    ///
    /// [`DecodeError::UnknownType`] or [`DecodeError::BadBody`].
    pub fn from_raw(raw: &RawFrame) -> Result<Self, DecodeError> {
        Ok(match raw.kind.as_str() {
            "hello" => Self::Hello(raw.body_as()?),
            "heartbeat" => Self::Heartbeat(raw.body_as()?),
            "status_report" => Self::StatusReport(raw.body_as()?),
            "event" => Self::Event(raw.body_as()?),
            "lease_renew" => Self::LeaseRenew(raw.body_as()?),
            "log_ack" => Self::LogAck(raw.body_as()?),
            "stream_data" => Self::StreamData(raw.body_as()?),
            "stream_end" => Self::StreamEnd(raw.body_as()?),
            "command_result" => Self::CommandResult(raw.body_as()?),
            "dns_challenge" => Self::DnsChallenge(raw.body_as()?),
            "image_inventory" => Self::ImageInventory(raw.body_as()?),
            "support_grant" => Self::SupportGrant(raw.body_as()?),
            "error" => Self::Error(raw.body_as()?),
            "ping" => Self::Ping,
            "pong" => Self::Pong,
            other => {
                return Err(DecodeError::UnknownType {
                    kind: other.to_owned(),
                    id: raw.id.clone(),
                });
            }
        })
    }
}

// --- control plane to node --------------------------------------------------

/// Body of `welcome` (section 5.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Welcome {
    pub cluster_id: String,
    pub term: u64,
    pub server_time: String,
    pub heartbeat_interval_ms: u64,
    pub session_timeout_ms: u64,
    pub primary_node_id: String,
    /// Other base URLs where this primary's listener can be reached, best
    /// first (section 5.10). Optional: an absent or empty list is valid.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_urls: Vec<String>,
}

/// `desired_status` of a VM.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredStatus {
    Running,
    Stopped,
}

/// `health_check` of a VM (section 5.11; defaults in section 6.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheck {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failures: Option<u32>,
    /// Seconds after a start during which failed probes do not count; absent
    /// means 30 (section 6.3, ADR 0184). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_period_s: Option<u32>,
    /// Whether the node restarts a VM that stays unhealthy; absent means true
    /// (section 6.3, ADR 0184). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<bool>,
}

/// A volume attached to a VM, one entry of `spec.vms[].volumes` (section
/// 5.11; empty until M5a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVolume {
    pub volume_id: String,
    /// The block device serial the guest init finds the disk by,
    /// `quoro-vol-0` for the first. Added in M5a; older snapshots held no
    /// volumes.
    #[serde(default)]
    pub serial: String,
    pub mount_path: String,
}

/// One VM of `spec.vms` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVm {
    pub vm_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub name: String,
    pub image: String,
    pub image_digest: String,
    pub vcpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheck>,
    pub desired_status: DesiredStatus,
    /// On the warm node of a `warm` app: the node prepares the VM's image and
    /// creates nothing else (sections 5.11 and 6.4, ADR 0174). Added in M5b;
    /// absent means false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub standby: bool,
    pub restart_generation: u64,
    pub stateful: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    pub volumes: Vec<SpecVolume>,
    /// Set when the image is a build's output on this node: the node never
    /// pulls it (section 6.6). Added in M4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
}

/// One route of `spec.routes` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecRoute {
    pub hostname: String,
    pub app_id: String,
    pub vm_ids: Vec<String>,
    pub port: u16,
    /// `acme` or `tunnel`.
    pub tls_mode: String,
    /// The domain this route serves (`dom_`), named by the node's
    /// `certificate.*` events. Optional and additive: a primary that predates
    /// it sends none, and an older node ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_id: Option<String>,
    /// RFC 3339 end of the grace window of a `tunnel` route that left this
    /// machine (section 5.11, ADR 0168). Added in M5b; the node serves the
    /// route like any other and never acts on the time itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draining_until: Option<String>,
}

/// `spec.network` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecNetwork {
    /// CIDR of the node bridge.
    pub subnet: String,
    /// Address of the bridge.
    pub gateway: String,
}

/// `spec.mesh` (section 5.11, ADR 0144): this machine's WireGuard interface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecMesh {
    /// This machine's mesh address with prefix length `/16`.
    pub address: String,
    /// UDP port the interface listens on.
    pub listen_port: u16,
    /// The other end of each tunnel: on a node the edge, on the edge every
    /// node (a star in M5a).
    pub peers: Vec<MeshPeer>,
    /// On a node, what the edge may reach through the interface; `[]` on the
    /// edge.
    pub allow: Vec<MeshAllow>,
}

/// One peer of `spec.mesh.peers` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshPeer {
    pub node_id: String,
    /// Base64 WireGuard public key of the peer.
    pub public_key: String,
    /// `host:port` to dial; absent for a peer that dials this machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// CIDRs routed to the peer.
    pub allowed_ips: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistent_keepalive_s: Option<u16>,
}

/// One entry of `spec.mesh.allow` (section 5.11): TCP from `source` to
/// `address:port` is admitted through the mesh interface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshAllow {
    /// The edge's mesh address.
    pub source: String,
    /// A guest address on this node.
    pub address: String,
    pub port: u16,
}

/// `spec.edge` (section 5.11, ADR 0143).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecEdge {
    pub routes: Vec<EdgeRoute>,
    /// The edge's own tunnel, same shape and handling as
    /// `spec.ingress.tunnel`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<SpecTunnel>,
}

/// One route of `spec.edge.routes` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeRoute {
    pub hostname: String,
    pub domain_id: String,
    pub app_id: String,
    /// `acme` or `tunnel`.
    pub tls_mode: String,
    pub health_check: HealthCheck,
    pub backends: Vec<EdgeBackend>,
    /// RFC 3339 end of the grace window of a `tunnel` route whose app went
    /// back to one replica (section 5.11, ADR 0168). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draining_until: Option<String>,
}

/// One instance behind an edge route (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeBackend {
    pub vm_id: String,
    pub node_id: String,
    /// The instance's guest address in its node's subnet.
    pub address: String,
    pub port: u16,
}

/// `spec` of `desired_state` (section 5.11).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub vms: Vec<SpecVm>,
    pub routes: Vec<SpecRoute>,
    /// The node's own bridge subnet (ADR 0144); absent on a machine without
    /// the `node` role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<SpecNetwork>,
    /// Present when the node runs a tunnel (section 5.11, D-0094).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<SpecIngress>,
    /// Builds for this node to run (section 5.11, ADR 0117). Added in M4.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builds: Vec<SpecBuild>,
    /// The machine's WireGuard interface, once it is on the mesh (ADR 0144).
    /// Added in M5a.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<SpecMesh>,
    /// Routes with remote backends, on a machine with the `edge` role (ADR
    /// 0143). Added in M5a.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<SpecEdge>,
    /// Volume files this node holds (section 5.11, ADR 0145). Added in M5a.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<SpecVolumeFile>,
    /// Backup jobs for this node (section 5.11, ADR 0147). Added in M5a.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backups: Vec<SpecBackup>,
    /// The repository the jobs and restores use; present while `backups` or
    /// a `volumes[].restore` is not empty. Added in M5a.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_repository: Option<SpecBackupRepository>,
    /// Present on every voter: a machine with the `node` or `witness` role
    /// (section 6.12, ADR 0170). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub votes: Option<SpecVotes>,
    /// Present on a machine with the `witness` role; its presence is the role
    /// (section 6.12). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub witness: Option<SpecWitness>,
}

/// `spec.votes` (section 6.12): the voter set this machine counts in.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVotes {
    /// The voter-set version: the change-log `seq` of the entry that last
    /// changed the voter set (section 8.10).
    pub version: u64,
    /// TCP port of the vote listener on the voter's mesh address.
    pub port: u16,
    /// Every voter, this machine included.
    pub voters: Vec<SpecVoter>,
}

/// One entry of `spec.votes.voters` (section 6.12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVoter {
    pub node_id: String,
    /// `node`, `witness` and `edge`, as enrolled.
    pub roles: Vec<String>,
    /// The voter's mesh address, or `null` while it has none.
    #[serde(default)]
    pub mesh_address: Option<String>,
}

/// `spec.witness` (section 6.12): an empty object in M5b.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecWitness {}

/// `state` of a `spec.volumes[]` entry (section 5.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpecVolumeState {
    /// The file must exist.
    Present,
    /// The node removes the file once no VM uses it, then reports `deleted`.
    Deleted,
}

/// One entry of `spec.volumes` (section 5.11, ADR 0145).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVolumeFile {
    pub volume_id: String,
    pub app_id: String,
    pub size_gb: u32,
    pub state: SpecVolumeState,
    /// Present only while the volume is `restoring`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore: Option<SpecRestore>,
    /// On both nodes of a `warm` app: this node's part in replicating the
    /// volume (sections 5.11 and 6.13, ADR 0173). Added in M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm: Option<SpecWarm>,
}

/// `role` of `spec.volumes[].warm` (section 5.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmRole {
    /// This node holds the app's lease, runs it and ships snapshots.
    Source,
    /// This node holds the warm copy and receives them.
    Target,
}

/// `spec.volumes[].warm` (section 5.11, section 6.13, ADR 0173).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecWarm {
    pub role: WarmRole,
    /// The other node of the pair.
    pub peer_node_id: String,
    /// The other node's mesh address; the source dials the target's.
    pub peer_address: String,
    /// TCP port of the target's replication listener on its own mesh
    /// address; 51822 unless the operator changed it.
    pub port: u16,
    /// Seconds between two snapshots on the source, 5 to 3 600.
    pub interval_s: u32,
    /// The app's lease.
    pub lease_id: String,
    /// The lease's current token; the target refuses a stream with a lower
    /// one.
    pub fencing_token: u64,
}

/// `spec.volumes[].restore` (section 5.11, backup-format.md section 8.1).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecRestore {
    pub backup_id: String,
    pub manifest_key: String,
    pub manifest_digest: String,
    pub logical_bytes: u64,
    /// Base64 of the app's 32-byte chunk-id key. `Debug` redacts it.
    pub id_key: String,
    /// Base64 of the app's 32-byte chunk-data key. `Debug` redacts it.
    pub data_key: String,
}

impl std::fmt::Debug for SpecRestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecRestore")
            .field("backup_id", &self.backup_id)
            .field("manifest_key", &self.manifest_key)
            .field("manifest_digest", &self.manifest_digest)
            .field("logical_bytes", &self.logical_bytes)
            .field("id_key", &"<redacted>")
            .field("data_key", &"<redacted>")
            .finish()
    }
}

/// One job of `spec.backups` (section 5.11, ADR 0147).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecBackup {
    pub backup_id: String,
    pub app_id: String,
    pub volume_id: String,
    /// The app's VM on this node as the primary placed it; absent when the
    /// app has no VM here. Informational only: the node does not use it to
    /// choose what to pause, since a `pause_copy` snapshot pauses the VM that
    /// attaches the volume (`engine-trait.md` section 3.16).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<String>,
    pub fencing_token: u64,
    /// Base64 of the app's 32-byte chunk-id key. `Debug` redacts it.
    pub id_key: String,
    /// Base64 of the app's 32-byte chunk-data key. `Debug` redacts it.
    pub data_key: String,
}

impl std::fmt::Debug for SpecBackup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecBackup")
            .field("backup_id", &self.backup_id)
            .field("app_id", &self.app_id)
            .field("volume_id", &self.volume_id)
            .field("vm_id", &self.vm_id)
            .field("fencing_token", &self.fencing_token)
            .field("id_key", &"<redacted>")
            .field("data_key", &"<redacted>")
            .finish()
    }
}

/// `spec.backup_repository` (section 5.11): where the node reads and writes
/// chunk and manifest objects. The credential is plaintext only inside the
/// signed snapshot.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecBackupRepository {
    pub repository_id: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// `<prefix>/<cluster_id>`, the prefix every repository key is relative to.
    pub root: String,
    pub access_key_id: String,
    /// The provider's secret, plaintext. `Debug` redacts it; never log it.
    pub secret_access_key: String,
}

impl std::fmt::Debug for SpecBackupRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecBackupRepository")
            .field("repository_id", &self.repository_id)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// `spec.builds[].limits` (section 6.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildLimits {
    pub vcpus: u32,
    pub memory_mb: u32,
    pub timeout_s: u64,
    pub cache_gb: u32,
    pub output_gb: u32,
}

impl Default for BuildLimits {
    /// The contract's defaults (section 6.6).
    fn default() -> Self {
        Self {
            vcpus: 2,
            memory_mb: 2048,
            timeout_s: 1800,
            cache_gb: 10,
            output_gb: 10,
        }
    }
}

/// One build of `spec.builds` (section 5.11, ADR 0117, ADR 0118).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecBuild {
    pub build_id: String,
    pub app_id: String,
    pub project_id: String,
    /// `https://` URL of a public Git repository.
    pub repo_url: String,
    /// Branch, tag or full commit SHA.
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// Relative to `context_dir`.
    pub dockerfile_path: String,
    /// Relative to the repository root; `.` is the root.
    pub context_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub build_args: BTreeMap<String, String>,
    /// Secret id to plaintext value. `Debug` redacts the values; never log them.
    pub secrets: BTreeMap<String, String>,
    pub limits: BuildLimits,
    pub cancelled: bool,
    /// Resolve every base image again from its registry
    /// (`image-resolve-mode=pull`) instead of the builder's cached copy, so a
    /// patched base is picked up (section 5.2, ADR 0150). Added in M5a;
    /// absent means false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub refresh_base: bool,
}

impl std::fmt::Debug for SpecBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secrets: Vec<&String> = self.secrets.keys().collect();
        f.debug_struct("SpecBuild")
            .field("build_id", &self.build_id)
            .field("app_id", &self.app_id)
            .field("project_id", &self.project_id)
            .field("repo_url", &self.repo_url)
            .field("git_ref", &self.git_ref)
            .field("dockerfile_path", &self.dockerfile_path)
            .field("context_dir", &self.context_dir)
            .field("target", &self.target)
            .field("build_args", &self.build_args)
            .field("secrets", &secrets)
            .field("limits", &self.limits)
            .field("cancelled", &self.cancelled)
            .field("refresh_base", &self.refresh_base)
            .finish()
    }
}

/// `spec.ingress` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecIngress {
    /// The node's tunnel, present when the node is mapped to a `tunnel`
    /// provider and the tunnel exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<SpecTunnel>,
}

/// `spec.ingress.tunnel` (section 5.11): only the node's own tunnel id, its
/// run token and the loopback origin port. Never the account credential.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecTunnel {
    pub tunnel_id: String,
    /// The tunnel's run token, plaintext. `Debug` redacts it; never log it.
    pub token: String,
    pub origin_port: u16,
}

impl std::fmt::Debug for SpecTunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecTunnel")
            .field("tunnel_id", &self.tunnel_id)
            .field("token", &"<redacted>")
            .field("origin_port", &self.origin_port)
            .finish()
    }
}

/// `ID` and `VERSION_ID` of an image's os-release (section 5.26).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryOs {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// One package of an `image_inventory` part (section 5.26).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryPackageWire {
    pub ecosystem: String,
    pub name: String,
    /// The source package, when the package database records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    pub version: String,
    pub purl: String,
    /// For a language package, the file it was read from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Body of one `image_inventory` part (section 5.26), node to control plane.
///
/// Every part of one send carries the same `image_digest`, `inventory_id`,
/// `parts` and metadata, and its own slice of `packages`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageInventoryBody {
    pub image_digest: String,
    /// Identity of the send: the `msg_` id of part 1.
    pub inventory_id: String,
    /// 1-based number of this part.
    pub part: u32,
    /// Number of parts of this send, 1 to 8.
    pub parts: u32,
    /// Hex SHA-256 of the whole inventory (section 6.11).
    pub inventory_sha256: String,
    pub inventoried_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<InventoryOs>,
    pub ecosystems: Vec<String>,
    pub not_inventoried: Vec<String>,
    /// Packages in the whole inventory.
    pub package_count: u64,
    /// This part's slice of the packages.
    pub packages: Vec<InventoryPackageWire>,
}

/// `action` of `dns_challenge` (section 5.22).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DnsChallengeAction {
    Set,
    Clear,
}

/// Body of `dns_challenge` (section 5.22), node to control plane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsChallenge {
    pub action: DnsChallengeAction,
    pub hostname: String,
    /// `set` only: the DNS-01 key authorization digest (base64url).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `clear` only: the id from the `dns_challenge_result` of the `set`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
}

/// Body of `dns_challenge_result` (section 5.23), control plane to node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsChallengeResult {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    /// Present when `ok` is false; never a credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Body of `desired_state` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredState {
    pub rev: u64,
    pub issued_at: String,
    pub term: u64,
    pub spec: Spec,
    /// Base64 Ed25519 signature; empty before [`crate::sign::sign_body`].
    #[serde(default)]
    pub signature: String,
}

impl DesiredState {
    /// Parses a body whose signature was already verified.
    ///
    /// # Errors
    ///
    /// The serde error when the body does not have the section 5.11 shape.
    pub fn from_value(body: &Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(body.clone())
    }

    /// The body as JSON, without `signature` when it is empty, ready for
    /// [`crate::sign::sign_body`].
    #[must_use]
    pub fn to_unsigned_value(&self) -> Value {
        let mut value = serde_json::to_value(self).unwrap_or_else(|_| empty_object());
        if let Value::Object(object) = &mut value {
            object.remove("signature");
        }
        value
    }
}

/// Body of `log_entries` (section 5.14). Entries stay JSON: their hash is
/// computed over their canonical form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntries {
    pub from_seq: u64,
    pub entries: Vec<Value>,
    /// From M5b: the `seq` of the primary's newest entry when it sent this
    /// message, so the node knows how far behind it is (section 7.1, ADR
    /// 0227). Absent from a primary older than M5b.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_seq: Option<u64>,
}

/// Body of `stream_open` (section 5.15).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamOpen {
    pub stream_id: String,
    /// `app_logs` or `deployment_logs` in v1.
    pub kind: String,
    pub params: Value,
}

/// Body of `stream_close` (section 5.16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamClose {
    pub stream_id: String,
}

/// A new window size in `stream_input` (section 5.24).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSize {
    /// 1 to 1 000.
    pub rows: u16,
    /// 1 to 1 000.
    pub cols: u16,
}

/// Body of `stream_input` (section 5.24): input for a `terminal` stream. At
/// least one of `data`, `resize` and `eof` is present; the node applies them
/// in that order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInput {
    pub stream_id: String,
    /// Per-stream input sequence, gap-free from 1.
    pub seq: u64,
    /// Base64 bytes for the program's input, at most 64 KiB decoded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resize: Option<WindowSize>,
    /// True to close the program's input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eof: Option<bool>,
}

/// Body of `stream_ack` (section 5.25): credit for a `data` stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAck {
    pub stream_id: String,
    /// Cumulative decoded bytes of the stream's `data` the consumer has taken.
    pub acked_bytes: u64,
}

/// Body of `command` (section 5.17).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    /// `logs_query`, `probe` or `fence_ack` in v1.
    pub kind: String,
    pub params: Value,
}

/// Body of `support_request` (section 5.27): the primary asks a voter for
/// its support (section 8.8, ADR 0169).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportRequest {
    /// The primary's term.
    pub term: u64,
    /// The node holding the primary status, as in `welcome`.
    pub primary_node_id: String,
    /// `S`, the support window asked for; at least [`MIN_SUPPORT_MS`].
    pub support_ms: u64,
    /// `L`, the longest `duration_ms` the primary may grant while this
    /// support lasts.
    pub max_lease_ms: u64,
    /// The voter-set version the primary counts against (section 8.10).
    pub voters_version: u64,
}

/// Body of `support_grant` (section 5.28): a voter's promise, sent once it is
/// stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportGrant {
    /// The term supported, equal to the request's.
    pub term: u64,
    /// The window granted, equal to the request's.
    pub support_ms: u64,
    /// The `L` recorded with the promise, equal to the request's.
    pub max_lease_ms: u64,
    /// The voter-set version of the voter's stored desired state.
    pub voters_version: u64,
}

/// The floor of `support_request.support_ms` (sections 5.27 and 8.8).
pub const MIN_SUPPORT_MS: u64 = 3_000;

/// A control-plane-to-node message with a typed body.
#[derive(Clone, Debug, PartialEq)]
pub enum ControlMessage {
    Welcome(Welcome),
    /// The raw signed body: verify it, then [`DesiredState::from_value`].
    DesiredState(Value),
    LeaseGrant(LeaseGrant),
    LeaseRevoke(LeaseRevoke),
    LogEntries(LogEntries),
    StreamOpen(StreamOpen),
    StreamClose(StreamClose),
    StreamInput(StreamInput),
    StreamAck(StreamAck),
    Command(Command),
    /// The raw signed body (section 5.21).
    SigningKeyRotate(Value),
    DnsChallengeResult(DnsChallengeResult),
    /// From M5b, to a voter (section 5.27).
    SupportRequest(SupportRequest),
    Error(ProtocolError),
    Ping,
    Pong,
}

impl ControlMessage {
    /// The wire `type` of this message.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Welcome(_) => "welcome",
            Self::DesiredState(_) => "desired_state",
            Self::LeaseGrant(_) => "lease_grant",
            Self::LeaseRevoke(_) => "lease_revoke",
            Self::LogEntries(_) => "log_entries",
            Self::StreamOpen(_) => "stream_open",
            Self::StreamClose(_) => "stream_close",
            Self::StreamInput(_) => "stream_input",
            Self::StreamAck(_) => "stream_ack",
            Self::Command(_) => "command",
            Self::SigningKeyRotate(_) => "signing_key_rotate",
            Self::DnsChallengeResult(_) => "dns_challenge_result",
            Self::SupportRequest(_) => "support_request",
            Self::Error(_) => "error",
            Self::Ping => "ping",
            Self::Pong => "pong",
        }
    }

    /// The frame for this message.
    #[must_use]
    pub fn to_frame(&self, id: &str, re: Option<&str>) -> RawFrame {
        let kind = self.kind();
        match self {
            Self::Welcome(b) => frame(kind, id, re, b),
            Self::DesiredState(b) | Self::SigningKeyRotate(b) => frame(kind, id, re, b),
            Self::LeaseGrant(b) => frame(kind, id, re, b),
            Self::LeaseRevoke(b) => frame(kind, id, re, b),
            Self::LogEntries(b) => frame(kind, id, re, b),
            Self::StreamOpen(b) => frame(kind, id, re, b),
            Self::StreamClose(b) => frame(kind, id, re, b),
            Self::StreamInput(b) => frame(kind, id, re, b),
            Self::StreamAck(b) => frame(kind, id, re, b),
            Self::Command(b) => frame(kind, id, re, b),
            Self::DnsChallengeResult(b) => frame(kind, id, re, b),
            Self::SupportRequest(b) => frame(kind, id, re, b),
            Self::Error(b) => frame(kind, id, re, b),
            Self::Ping | Self::Pong => frame(kind, id, re, &empty_object()),
        }
    }

    /// Maps a parsed envelope to a typed message.
    ///
    /// # Errors
    ///
    /// [`DecodeError::UnknownType`] or [`DecodeError::BadBody`].
    pub fn from_raw(raw: &RawFrame) -> Result<Self, DecodeError> {
        Ok(match raw.kind.as_str() {
            "welcome" => Self::Welcome(raw.body_as()?),
            "desired_state" => Self::DesiredState(raw.body.clone()),
            "lease_grant" => Self::LeaseGrant(raw.body_as()?),
            "lease_revoke" => Self::LeaseRevoke(raw.body_as()?),
            "log_entries" => Self::LogEntries(raw.body_as()?),
            "stream_open" => Self::StreamOpen(raw.body_as()?),
            "stream_close" => Self::StreamClose(raw.body_as()?),
            "stream_input" => Self::StreamInput(raw.body_as()?),
            "stream_ack" => Self::StreamAck(raw.body_as()?),
            "command" => Self::Command(raw.body_as()?),
            "signing_key_rotate" => Self::SigningKeyRotate(raw.body.clone()),
            "dns_challenge_result" => Self::DnsChallengeResult(raw.body_as()?),
            "support_request" => Self::SupportRequest(raw.body_as()?),
            "error" => Self::Error(raw.body_as()?),
            "ping" => Self::Ping,
            "pong" => Self::Pong,
            other => {
                return Err(DecodeError::UnknownType {
                    kind: other.to_owned(),
                    id: raw.id.clone(),
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_protocol_hello_example_decodes() {
        let text = r#"{"v":1,"type":"hello","id":"msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D","body":{
            "node_id":"nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP","agent_version":"0.1.0",
            "engine_version":"cloud-hypervisor v54.0","boot_id":"8f1c","stored_rev":412,
            "applied_rev":412,"log_seq":902,"log_hash":"3b1f","leases":[{"lease_id":"lse_1",
            "resource_type":"app","resource_id":"app_1","mode":"local","fencing_token":7,
            "state":"held"}],"capacity":{"cpus":8,"memory_mb":16384,"disk_gb":200},
            "allocated":{"cpus":2,"memory_mb":2048,"disk_gb":20},
            "kvm":{"available":true,"kernel":"6.11.2","nested":true},"dropped_events":0,
            "future_field":"ignored"}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let NodeMessage::Hello(hello) = NodeMessage::from_raw(&raw).unwrap() else {
            panic!("not a hello");
        };
        assert_eq!(hello.stored_rev, 412);
        assert_eq!(hello.leases[0].mode, crate::LeaseMode::Local);
        assert_eq!(hello.kvm.nested, Some(true));
    }

    #[test]
    fn the_dns_challenge_examples_decode_and_round_trip() {
        let text = r#"{"v":1,"type":"dns_challenge","id":"msg_01JC9ZW2A4C6E8G0J2M4P6R8T0",
            "body":{"action":"set","hostname":"blog.example.com",
            "value":"LoqXcYV8q5ONbJQxbmR7SCTNo3tiAXDfowyjxAjEuX0"}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let message = NodeMessage::from_raw(&raw).unwrap();
        let NodeMessage::DnsChallenge(challenge) = &message else {
            panic!("not a dns_challenge");
        };
        assert_eq!(challenge.action, DnsChallengeAction::Set);
        assert_eq!(challenge.record_id, None);
        let again = message.to_frame(&raw.id, None);
        assert_eq!(NodeMessage::from_raw(&again).unwrap(), message);

        let text = r#"{"v":1,"type":"dns_challenge_result","id":"msg_01JC9ZW3B5D7F9H1K3N5Q7S9V1",
            "re":"msg_01JC9ZW2A4C6E8G0J2M4P6R8T0",
            "body":{"ok":true,"record_id":"372e67954025e0ba6aaa6d586b9e0b59"}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let ControlMessage::DnsChallengeResult(result) = ControlMessage::from_raw(&raw).unwrap()
        else {
            panic!("not a dns_challenge_result");
        };
        assert!(result.ok);
        assert_eq!(result.message, None);
    }

    #[test]
    fn a_tunnel_token_never_shows_in_debug_output() {
        let ingress = SpecIngress {
            tunnel: Some(SpecTunnel {
                tunnel_id: "c1744f8b".to_owned(),
                token: "eyJhIjoidG9rZW4ifQ".to_owned(),
                origin_port: 8081,
            }),
        };
        let printed = format!("{ingress:?}");
        assert!(!printed.contains("eyJhIjoidG9rZW4ifQ"), "{printed}");
        let value = serde_json::to_value(&ingress).unwrap();
        assert_eq!(value["tunnel"]["origin_port"], 8081);
        let back: SpecIngress = serde_json::from_value(value).unwrap();
        assert_eq!(back, ingress);
    }

    #[test]
    fn envelope_errors_map_to_the_section_5_18_codes() {
        assert!(matches!(
            RawFrame::parse("not json"),
            Err(DecodeError::Malformed(_))
        ));
        let v2 = RawFrame::parse(r#"{"v":2,"type":"hello","id":"msg_1","body":{}}"#).unwrap_err();
        assert!(v2.is_fatal());
        assert_eq!(v2.code(), crate::ErrorCode::UnsupportedVersion);

        let raw = RawFrame::parse(r#"{"v":1,"type":"teleport","id":"msg_1","body":{}}"#).unwrap();
        let err = NodeMessage::from_raw(&raw).unwrap_err();
        assert!(!err.is_fatal());
        assert_eq!(err.message_id(), Some("msg_1"));

        let raw = RawFrame::parse(r#"{"v":1,"type":"heartbeat","id":"msg_2","body":{}}"#).unwrap();
        assert!(matches!(
            NodeMessage::from_raw(&raw),
            Err(DecodeError::BadBody { .. })
        ));

        let big = format!(
            r#"{{"v":1,"type":"ping","id":"msg_3","body":{{"x":"{}"}}}}"#,
            "a".repeat(MAX_MESSAGE_BYTES)
        );
        assert!(matches!(
            RawFrame::parse(&big),
            Err(DecodeError::TooLarge(_))
        ));
    }

    #[test]
    fn a_welcome_without_primary_urls_parses_and_an_empty_list_is_not_serialized() {
        let text = r#"{"v":1,"type":"welcome","id":"msg_1","body":{"cluster_id":"cls_1","term":1,"server_time":"2026-11-02T10:15:30Z","heartbeat_interval_ms":5000,"session_timeout_ms":15000,"primary_node_id":"nod_1"}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let welcome = match ControlMessage::from_raw(&raw).unwrap() {
            ControlMessage::Welcome(welcome) => welcome,
            other => panic!("not a welcome: {other:?}"),
        };
        assert!(welcome.primary_urls.is_empty());
        let frame = ControlMessage::Welcome(welcome)
            .to_frame("msg_1", None)
            .to_text();
        assert!(!frame.contains("primary_urls"), "{frame}");

        let raw = RawFrame::parse(
            r#"{"v":1,"type":"welcome","id":"msg_2","body":{"cluster_id":"cls_1","term":1,"server_time":"2026-11-02T10:15:30Z","heartbeat_interval_ms":5000,"session_timeout_ms":15000,"primary_node_id":"nod_1","primary_urls":["https://primary.example:8443"]}}"#,
        )
        .unwrap();
        let welcome = match ControlMessage::from_raw(&raw).unwrap() {
            ControlMessage::Welcome(welcome) => welcome,
            other => panic!("not a welcome: {other:?}"),
        };
        assert_eq!(
            welcome.primary_urls,
            vec!["https://primary.example:8443".to_owned()]
        );
    }

    #[test]
    fn log_entries_carry_head_seq_only_when_present() {
        let text = r#"{"v":1,"type":"log_entries","id":"msg_1","body":{"from_seq":903,
            "entries":[{"seq":903}],"head_seq":950}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let ControlMessage::LogEntries(entries) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("not log_entries");
        };
        assert_eq!(entries.from_seq, 903);
        assert_eq!(entries.head_seq, Some(950));

        // A primary older than M5b sends no `head_seq` (section 5.14).
        let raw = RawFrame::parse(
            r#"{"v":1,"type":"log_entries","id":"msg_2","body":{"from_seq":1,"entries":[]}}"#,
        )
        .unwrap();
        let ControlMessage::LogEntries(entries) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("not log_entries");
        };
        assert_eq!(entries.head_seq, None);
        let frame = ControlMessage::LogEntries(entries)
            .to_frame("msg_2", None)
            .to_text();
        assert!(!frame.contains("head_seq"), "{frame}");
    }

    #[test]
    fn frames_round_trip_both_ways() {
        let message = NodeMessage::Heartbeat(Heartbeat {
            time: "2026-11-02T10:15:32Z".to_owned(),
            busy: None,
        });
        let text = message.to_frame("msg_9", Some("msg_8")).to_text();
        let raw = RawFrame::parse(&text).unwrap();
        assert_eq!(raw.re.as_deref(), Some("msg_8"));
        assert_eq!(NodeMessage::from_raw(&raw).unwrap(), message);

        let welcome = ControlMessage::Welcome(Welcome {
            cluster_id: "cls_1".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
            session_timeout_ms: SESSION_TIMEOUT_MS,
            primary_node_id: "nod_1".to_owned(),
            primary_urls: Vec::new(),
        });
        let raw = RawFrame::parse(&welcome.to_frame("msg_1", None).to_text()).unwrap();
        assert_eq!(ControlMessage::from_raw(&raw).unwrap(), welcome);
        assert_eq!(
            RawFrame::parse(&ControlMessage::Ping.to_frame("msg_2", None).to_text())
                .unwrap()
                .body,
            json!({})
        );
    }

    #[test]
    fn a_signed_desired_state_verifies_after_the_wire() {
        let key = crate::sign::signing_key_from_seed(&[5; 32]);
        let state = DesiredState {
            rev: 7,
            issued_at: "2026-11-02T10:15:31Z".to_owned(),
            term: 1,
            spec: Spec {
                vms: vec![SpecVm {
                    vm_id: "vm_1".to_owned(),
                    project_id: "prj_1".to_owned(),
                    app_id: None,
                    deployment_id: None,
                    name: "web".to_owned(),
                    image: "docker.io/library/nginx:1.27".to_owned(),
                    image_digest: "sha256:aa".to_owned(),
                    vcpus: 1,
                    memory_mb: 256,
                    disk_gb: 1,
                    port: Some(80),
                    env: BTreeMap::from([("A".to_owned(), "b".to_owned())]),
                    health_check: None,
                    desired_status: DesiredStatus::Running,
                    standby: false,
                    restart_generation: 0,
                    stateful: false,
                    lease_id: None,
                    fencing_token: None,
                    volumes: Vec::new(),
                    build_id: None,
                }],
                routes: Vec::new(),
                network: Some(SpecNetwork {
                    subnet: "10.42.0.0/24".to_owned(),
                    gateway: "10.42.0.1".to_owned(),
                }),
                ..Spec::default()
            },
            signature: String::new(),
        };
        let mut body = state.to_unsigned_value();
        crate::sign::sign_body(&key, &mut body).unwrap();
        let text = ControlMessage::DesiredState(body)
            .to_frame("msg_4", None)
            .to_text();
        let raw = RawFrame::parse(&text).unwrap();
        let ControlMessage::DesiredState(received) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("not a desired_state");
        };
        crate::sign::verify_body(&key.verifying_key(), &received).unwrap();
        let parsed = DesiredState::from_value(&received).unwrap();
        assert_eq!(parsed.spec, state.spec);
        assert!(!parsed.signature.is_empty());
    }

    #[test]
    fn standby_is_absent_when_false_and_read_when_present() {
        let vm = json!({
            "vm_id": "vm_1", "project_id": "prj_1", "name": "web",
            "image": "docker.io/library/nginx:1.27", "image_digest": "sha256:aa",
            "vcpus": 1, "memory_mb": 256, "disk_gb": 1, "env": {},
            "desired_status": "stopped", "restart_generation": 0,
            "stateful": true, "lease_id": "lse_1", "fencing_token": 3, "volumes": [],
        });
        // A snapshot from a primary that predates `standby` still decodes.
        let old: SpecVm = serde_json::from_value(vm.clone()).unwrap();
        assert!(!old.standby);
        // An absent flag is not serialized, so an older node sees the old shape.
        assert_eq!(serde_json::to_value(&old).unwrap(), vm);

        let mut standby = vm;
        standby["standby"] = json!(true);
        let parsed: SpecVm = serde_json::from_value(standby.clone()).unwrap();
        assert!(parsed.standby);
        assert_eq!(serde_json::to_value(&parsed).unwrap(), standby);
    }

    #[test]
    fn a_backup_guest_flush_is_absent_when_unknown_and_read_when_present() {
        let entry =
            json!({"backup_id": "bkp_1", "status": "running", "snapshot_method": "reflink"});
        // An entry from an agent older than M5b still decodes.
        let old: BackupStatus = serde_json::from_value(entry.clone()).unwrap();
        assert_eq!(old.guest_flush, None);
        // An unknown outcome is not serialized, so the entry keeps the old shape.
        assert_eq!(serde_json::to_value(&old).unwrap(), entry);

        let unflushed = BackupStatus {
            guest_flush: Some(false),
            ..old.clone()
        };
        let value = serde_json::to_value(&unflushed).unwrap();
        assert_eq!(value["guest_flush"], json!(false));
        assert!(
            serde_json::to_string(&unflushed)
                .unwrap()
                .contains(r#""guest_flush":false"#)
        );
        assert_eq!(
            serde_json::from_value::<BackupStatus>(value).unwrap(),
            unflushed
        );

        let flushed = BackupStatus {
            guest_flush: Some(true),
            ..old
        };
        let value = serde_json::to_value(&flushed).unwrap();
        assert_eq!(value["guest_flush"], json!(true));
        assert_eq!(
            serde_json::from_value::<BackupStatus>(value).unwrap(),
            flushed
        );
    }

    #[test]
    fn a_route_domain_id_is_optional_on_the_wire() {
        // A route from a primary that predates `domain_id` still decodes.
        let old: SpecRoute = serde_json::from_value(json!({
            "hostname": "blog.example.com",
            "app_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
            "vm_ids": ["vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F"],
            "port": 8080,
            "tls_mode": "acme",
        }))
        .unwrap();
        assert_eq!(old.domain_id, None);
        // An absent id is not serialized, so an older node sees the old shape.
        let value = serde_json::to_value(&old).unwrap();
        assert!(value.get("domain_id").is_none());

        let new = SpecRoute {
            domain_id: Some("dom_01JC9Z8D5Q7S9V1X3Z5B7D9F1H".to_owned()),
            ..old
        };
        let value = serde_json::to_value(&new).unwrap();
        assert_eq!(value["domain_id"], "dom_01JC9Z8D5Q7S9V1X3Z5B7D9F1H");
        assert_eq!(serde_json::from_value::<SpecRoute>(value).unwrap(), new);
    }

    /// Sections 5.27 and 5.28: the protocol's examples decode and round-trip.
    #[test]
    fn the_support_examples_decode_and_round_trip() {
        let text = r#"{"v":1,"type":"support_request","id":"msg_01JCA0B2C3D4E5F6G7H8J9K0MN",
            "body":{"term":1,"primary_node_id":"nod_01JC9Z0000000000000000000Q",
            "support_ms":15000,"max_lease_ms":15000,"voters_version":3120}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let message = ControlMessage::from_raw(&raw).unwrap();
        assert_eq!(message.kind(), "support_request");
        let ControlMessage::SupportRequest(request) = &message else {
            panic!("not a support_request");
        };
        assert_eq!(
            request,
            &SupportRequest {
                term: 1,
                primary_node_id: "nod_01JC9Z0000000000000000000Q".to_owned(),
                support_ms: 15_000,
                max_lease_ms: 15_000,
                voters_version: 3120,
            }
        );
        let again = RawFrame::parse(&message.to_frame(&raw.id, None).to_text()).unwrap();
        assert_eq!(ControlMessage::from_raw(&again).unwrap(), message);

        let text = r#"{"v":1,"type":"support_grant","id":"msg_01JCA0B3D4E5F6G7H8J9K0MNPQ",
            "re":"msg_01JCA0B2C3D4E5F6G7H8J9K0MN",
            "body":{"term":1,"support_ms":15000,"max_lease_ms":15000,"voters_version":3120}}"#;
        let raw = RawFrame::parse(text).unwrap();
        let message = NodeMessage::from_raw(&raw).unwrap();
        assert_eq!(message.kind(), "support_grant");
        let NodeMessage::SupportGrant(grant) = &message else {
            panic!("not a support_grant");
        };
        assert_eq!(grant.voters_version, 3120);
        let again =
            RawFrame::parse(&message.to_frame(&raw.id, raw.re.as_deref()).to_text()).unwrap();
        assert_eq!(again.re.as_deref(), Some("msg_01JCA0B2C3D4E5F6G7H8J9K0MN"));
        assert_eq!(NodeMessage::from_raw(&again).unwrap(), message);

        // A request missing a required field is a bad body (section 3.4).
        let raw =
            RawFrame::parse(r#"{"v":1,"type":"support_request","id":"msg_1","body":{"term":1}}"#)
                .unwrap();
        assert!(matches!(
            ControlMessage::from_raw(&raw),
            Err(DecodeError::BadBody { .. })
        ));
    }

    /// Section 6.12: `spec.votes` and `spec.witness` as the example shows
    /// them, and a spec from a primary older than M5b without either.
    #[test]
    fn spec_votes_and_witness_are_optional_and_round_trip() {
        let old: Spec = serde_json::from_value(json!({"vms": [], "routes": []})).unwrap();
        assert_eq!((old.votes.as_ref(), old.witness), (None, None));
        let value = serde_json::to_value(&old).unwrap();
        assert!(value.get("votes").is_none() && value.get("witness").is_none());

        let example = json!({
            "vms": [],
            "routes": [],
            "witness": {},
            "votes": {
                "version": 3120,
                "port": 51821,
                "voters": [
                    { "node_id": "nod_01JC9Z0000000000000000000Q", "roles": ["node"], "mesh_address": "10.43.0.2" },
                    { "node_id": "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP", "roles": ["node"], "mesh_address": null },
                    { "node_id": "nod_01JCA0A1B2C3D4E5F6G7H8J9KM", "roles": ["edge", "witness"], "mesh_address": "10.43.0.1" }
                ]
            }
        });
        let spec: Spec = serde_json::from_value(example.clone()).unwrap();
        assert_eq!(spec.witness, Some(SpecWitness {}));
        let votes = spec.votes.as_ref().unwrap();
        assert_eq!((votes.version, votes.port), (3120, 51821));
        assert_eq!(votes.voters.len(), 3);
        assert_eq!(votes.voters[1].mesh_address, None);
        assert_eq!(votes.voters[2].roles, vec!["edge", "witness"]);
        assert_eq!(serde_json::to_value(&spec).unwrap(), example);

        // A voter entry without `mesh_address` reads as null.
        let voter: SpecVoter =
            serde_json::from_value(json!({"node_id": "nod_1", "roles": ["witness"]})).unwrap();
        assert_eq!(voter.mesh_address, None);
    }

    /// Section 5.18: `stale_term` and `not_primary` are new codes, `term` is
    /// carried only when set, and an `error` without it still parses.
    #[test]
    fn an_error_carries_term_only_when_set() {
        let stale = ControlMessage::Error(ProtocolError {
            code: crate::ErrorCode::StaleTerm,
            message: "the welcome's term is lower than the node's".to_owned(),
            fatal: true,
            term: Some(4),
        });
        let raw = RawFrame::parse(&stale.to_frame("msg_1", Some("msg_0")).to_text()).unwrap();
        assert_eq!(raw.body["code"], "stale_term");
        assert_eq!(raw.body["term"], 4);
        assert_eq!(ControlMessage::from_raw(&raw).unwrap(), stale);

        let not_primary = NodeMessage::Error(ProtocolError {
            code: crate::ErrorCode::NotPrimary,
            message: "demoted".to_owned(),
            fatal: true,
            term: None,
        });
        let raw = RawFrame::parse(&not_primary.to_frame("msg_2", None).to_text()).unwrap();
        assert_eq!(raw.body["code"], "not_primary");
        assert!(raw.body.get("term").is_none(), "{}", raw.body);
        assert_eq!(NodeMessage::from_raw(&raw).unwrap(), not_primary);

        let raw = RawFrame::parse(
            r#"{"v":1,"type":"error","id":"msg_3","body":{"code":"bad_signature","message":"x","fatal":false}}"#,
        )
        .unwrap();
        let ControlMessage::Error(error) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("not an error");
        };
        assert_eq!(error.term, None);
    }

    #[test]
    fn msg_ids_have_the_prefix_and_length() {
        let id = msg_id([0xab; 16]);
        assert!(id.starts_with("msg_"));
        assert_eq!(id.len(), 30);
        assert_ne!(id, msg_id([0xac; 16]));
    }

    #[test]
    fn the_protocol_interactive_examples_decode() {
        let input = r#"{"v":1,"type":"stream_input","id":"msg_01JC9ZX4C6E8G0J2M4P6R8T0V2","body":{
            "stream_id":"msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9","seq":3,"data":"dW5hbWUgLWEK"}}"#;
        let raw = RawFrame::parse(input).unwrap();
        let ControlMessage::StreamInput(body) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("expected stream_input");
        };
        assert_eq!(body.seq, 3);
        assert_eq!(body.data.as_deref(), Some("dW5hbWUgLWEK"));
        assert_eq!((body.resize, body.eof), (None, None));

        let resize = r#"{"v":1,"type":"stream_input","id":"msg_1","body":{
            "stream_id":"msg_2","seq":4,"resize":{"rows":50,"cols":132}}}"#;
        let raw = RawFrame::parse(resize).unwrap();
        let ControlMessage::StreamInput(body) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("expected stream_input");
        };
        assert_eq!(
            body.resize,
            Some(WindowSize {
                rows: 50,
                cols: 132
            })
        );

        let ack = r#"{"v":1,"type":"stream_ack","id":"msg_3","body":{
            "stream_id":"msg_2","acked_bytes":196608}}"#;
        let raw = RawFrame::parse(ack).unwrap();
        let message = ControlMessage::from_raw(&raw).unwrap();
        assert_eq!(message.kind(), "stream_ack");
        let ControlMessage::StreamAck(body) = message.clone() else {
            panic!("expected stream_ack");
        };
        assert_eq!(body.acked_bytes, 196_608);
        let back = ControlMessage::from_raw(&message.to_frame("msg_3", None)).unwrap();
        assert_eq!(back, message);

        let end = r#"{"v":1,"type":"stream_end","id":"msg_4","body":{
            "stream_id":"msg_2","reason":"completed","exit_code":0,
            "transcript_sha256":"4f2a9c1e7b3d5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8",
            "transcript_bytes":18234}}"#;
        let raw = RawFrame::parse(end).unwrap();
        let NodeMessage::StreamEnd(body) = NodeMessage::from_raw(&raw).unwrap() else {
            panic!("expected stream_end");
        };
        assert_eq!(body.exit_code, Some(0));
        assert_eq!(body.transcript_bytes, Some(18_234));

        let stopped = StreamEnd {
            stream_id: "msg_2".to_owned(),
            reason: StreamEndReason::VmStopped,
            message: None,
            exit_code: None,
            transcript_sha256: None,
            transcript_bytes: None,
        };
        let value = serde_json::to_value(&stopped).unwrap();
        assert_eq!(value, json!({"stream_id": "msg_2", "reason": "vm_stopped"}));
        assert_eq!(
            serde_json::to_value(StreamEndReason::Limit).unwrap(),
            json!("limit")
        );
        assert_eq!(
            serde_json::to_value(crate::ErrorCode::NotAvailable).unwrap(),
            json!("not_available")
        );
    }
}
