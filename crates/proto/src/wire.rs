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
#[serde(rename_all = "lowercase")]
pub enum StreamEndReason {
    Completed,
    Error,
    Cancelled,
}

/// Body of `stream_end` (section 5.8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEnd {
    pub stream_id: String,
    pub reason: StreamEndReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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
}

/// A volume attached to a VM (section 5.11; empty until M5a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecVolume {
    pub volume_id: String,
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
    pub restart_generation: u64,
    pub stateful: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    pub volumes: Vec<SpecVolume>,
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
}

/// `spec.network` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecNetwork {
    /// CIDR of the node bridge.
    pub subnet: String,
    /// Address of the bridge.
    pub gateway: String,
}

/// `spec` of `desired_state` (section 5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub vms: Vec<SpecVm>,
    pub routes: Vec<SpecRoute>,
    pub network: SpecNetwork,
    /// Present when the node runs a tunnel (section 5.11, D-0094).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<SpecIngress>,
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

/// Body of `command` (section 5.17).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    /// `logs_query`, `probe` or `fence_ack` in v1.
    pub kind: String,
    pub params: Value,
}

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
    Command(Command),
    /// The raw signed body (section 5.21).
    SigningKeyRotate(Value),
    DnsChallengeResult(DnsChallengeResult),
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
            Self::Command(_) => "command",
            Self::SigningKeyRotate(_) => "signing_key_rotate",
            Self::DnsChallengeResult(_) => "dns_challenge_result",
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
            Self::Command(b) => frame(kind, id, re, b),
            Self::DnsChallengeResult(b) => frame(kind, id, re, b),
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
            "command" => Self::Command(raw.body_as()?),
            "signing_key_rotate" => Self::SigningKeyRotate(raw.body.clone()),
            "dns_challenge_result" => Self::DnsChallengeResult(raw.body_as()?),
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
                    restart_generation: 0,
                    stateful: false,
                    lease_id: None,
                    fencing_token: None,
                    volumes: Vec::new(),
                }],
                routes: Vec::new(),
                network: SpecNetwork {
                    subnet: "10.42.0.0/24".to_owned(),
                    gateway: "10.42.0.1".to_owned(),
                },
                ingress: None,
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

    #[test]
    fn msg_ids_have_the_prefix_and_length() {
        let id = msg_id([0xab; 16]);
        assert!(id.starts_with("msg_"));
        assert_eq!(id.len(), 30);
        assert_ne!(id, msg_id([0xac; 16]));
    }
}
