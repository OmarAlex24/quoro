// SPDX-License-Identifier: AGPL-3.0-only
//! Node-agent protocol message types, shared by the node agent and the
//! control plane.
//!
//! See `docs/contracts/node-agent-protocol.md`.
//!
//! The lease types below date from M0, when the node agent and the primary ran
//! in one process; they now also carry the serde attributes of their wire
//! form. [`wire`] has the envelope and every other message body, [`sign`] the
//! cluster signature and [`jcs`] the RFC 8785 canonical JSON both use.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

pub mod jcs;
pub mod metrics;
pub mod sign;
pub mod wire;

/// A message id (`msg_`). Message-local: it pairs a request with its answer
/// inside one session and is never persisted (section 3.3).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MsgId(pub String);

/// A lease id (`lse_`) (section 5.12).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(pub String);

/// What a lease protects (section 5.12, `resource_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    /// A stateful application.
    App,
    /// The primary role; used from M6 with automatic failover (section 8.1).
    Primary,
}

/// How long a lease stays valid without the authority (section 8.3, 8.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseMode {
    /// No expiry: it ends only by a revoke (section 8.3).
    Local,
    /// Expires unless renewed, with the timing of section 8.4.
    Expiring,
}

/// Why a lease was revoked (section 5.13, `reason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevokeReason {
    /// The lease moved to another node.
    Moved,
    /// The resource was deleted.
    Deleted,
    /// An operator declared this node lost.
    DeclaredLost,
    /// A higher token superseded this lease.
    Superseded,
}

/// The outcome of an intrusive message (section 5.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    /// The command was carried out.
    Ok,
    /// The command failed; `detail` and `error` say why.
    Error,
}

/// Protocol error codes (section 5.18).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Envelope major version not implemented (fatal).
    UnsupportedVersion,
    /// Credential rejected (fatal).
    Unauthenticated,
    /// The node was removed from the cluster (fatal).
    NodeRemoved,
    /// A signature did not verify; the message was discarded.
    BadSignature,
    /// Malformed frame, unknown type or otherwise unacceptable message.
    BadMessage,
    /// The node's change-log copy diverged from the primary's.
    LogDiverged,
    /// A self-fence could not confirm that every bound VM stopped (section
    /// 5.18, ADR 0079).
    FenceFailed,
    /// A `stream_open` or `command` named a build, VM or transcript this node
    /// does not have (M4, carried in a `command_result`).
    NotFound,
    /// The VM exists but cannot serve the requested stream now, such as a
    /// `terminal` on a stopped VM (M4, carried in a `command_result`).
    NotAvailable,
}

/// The `data.reason` enum of the `node.fenced` event (section 8.5,
/// `events.schema.json` `data_node_fenced`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FenceReason {
    /// The lease reached `valid_until` without a renewal.
    LeaseExpired,
    /// A `lease_revoke` ended the lease.
    LeaseRevoked,
}

/// Body of `lease_grant` (section 5.12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGrant {
    /// The lease being granted.
    pub lease_id: LeaseId,
    /// What kind of resource the lease protects.
    pub resource_type: ResourceType,
    /// The `app_` or `nod_` id the lease is for.
    pub resource_id: String,
    /// `local` or `expiring`.
    pub mode: LeaseMode,
    /// Strictly increasing per resource, never reused (section 8.6).
    pub fencing_token: u64,
    /// Present for `expiring`, absent for `local` (section 5.12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// The authority's wall-clock time of the grant.
    pub granted_at: String,
}

/// Body of `lease_renew` (section 5.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRenew {
    /// The lease being renewed.
    pub lease_id: LeaseId,
    /// The token the node believes it holds.
    pub fencing_token: u64,
}

/// Body of `lease_revoke` (section 5.13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRevoke {
    /// The lease to revoke.
    pub lease_id: LeaseId,
    /// Token of the revoke; at least the node's current token.
    pub fencing_token: u64,
    /// Why the lease ended.
    pub reason: RevokeReason,
}

/// Body of `command_result` (section 5.9).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandResult {
    /// `ok` or `error`.
    pub status: CommandStatus,
    /// Short human-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Structured error, when `status` is `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    /// Log lines answering `command` kind `logs_query` (section 5.9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lines: Option<Vec<wire::LogLine>>,
    /// Samples answering `command` kind `metrics_query` (section 5.17).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<metrics::MetricsReport>,
}

/// The `error` object of a `command_result` (section 5.9).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// One of the section 5.18 codes.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

/// Body of `error` (section 5.18).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    /// One of the codes below the section 5.18 table.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
    /// Whether the session ends.
    pub fatal: bool,
}

/// Data of the `node.fenced` event (section 8.5, `events.schema.json`
/// `data_node_fenced`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeFenced {
    /// The node that fenced itself.
    pub node_id: String,
    /// The apps whose stateful VMs were stopped.
    pub app_ids: Vec<String>,
    /// The leases that were fenced.
    pub lease_ids: Vec<LeaseId>,
    /// Why the node fenced.
    pub reason: FenceReason,
}

/// Data of the `node.fence_failed` event (section 8.5, `events.schema.json`
/// `data_node_fence_failed`).
///
/// The node lost the lease and could not confirm that every bound stateful VM
/// stopped, so the VM may still be running for a resource someone else may now
/// own. The node marks itself failed until an operator clears the mark
/// (section 12 `node_meta`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeFenceFailed {
    /// The node that failed to fence itself.
    pub node_id: String,
    /// The apps whose stateful VMs could not be stopped.
    pub app_ids: Vec<String>,
    /// The leases that were fenced.
    pub lease_ids: Vec<LeaseId>,
    /// Only the VMs whose stop could not be confirmed.
    pub vm_ids: Vec<String>,
    /// Why the node fenced.
    pub reason: FenceReason,
    /// The stop failures, joined, when there is one.
    pub error: Option<String>,
}

/// The envelope around every message (section 3.3).
///
/// `v` and `type` are implied by the enum variant that carries the envelope
/// until serialization exists; at that point they are written from the variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope<B> {
    /// Identity of this message.
    pub id: MsgId,
    /// `id` of the message this one answers, when it answers one.
    pub re: Option<MsgId>,
    /// Message-specific body.
    pub body: B,
}

/// A control-plane-to-node message.
///
/// Only the messages M0 needs; the rest of the set is added with the session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToNode {
    /// Grant a lease, or answer a `lease_renew` (section 5.12).
    LeaseGrant(Envelope<LeaseGrant>),
    /// End a lease (section 5.13).
    LeaseRevoke(Envelope<LeaseRevoke>),
}

/// A node-to-control-plane message.
///
/// Only the messages M0 needs; the rest of the set is added with the session.
#[derive(Clone, Debug, PartialEq)]
pub enum FromNode {
    /// Ask to extend an `expiring` lease (section 5.5).
    LeaseRenew(Envelope<LeaseRenew>),
    /// The answer to an intrusive message (section 5.9).
    CommandResult(Envelope<CommandResult>),
    /// A message could not be accepted (section 5.18).
    Error(Envelope<ProtocolError>),
    /// Something happened on the node (section 8.5).
    Event(Envelope<NodeEvent>),
}

/// One event as it goes into the event buffer (section 12 `event_buffer`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeEvent {
    /// Event id (`evt_`).
    pub id: String,
    /// When the event happened.
    pub time: SystemTime,
    /// Event-specific payload.
    pub data: NodeEventData,
}

/// The events M0 emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeEventData {
    /// A node self-fenced its stateful apps after losing their leases.
    NodeFenced(NodeFenced),
    /// A node lost a lease but could not confirm that the bound VMs stopped
    /// (section 8.5, `data_node_fence_failed`).
    NodeFenceFailed(NodeFenceFailed),
}
