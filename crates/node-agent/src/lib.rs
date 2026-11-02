//! The node agent: local store, reconciliation, the control-plane session and
//! the lease state machine with self-fencing.
//!
//! See `docs/contracts/node-agent-protocol.md`. Only the lease workstream
//! exists so far; the other modules arrive with their own workstreams.

pub mod lease;

pub use lease::{
    Action, FenceRecord, FenceReport, FenceTimers, Lease, LeaseAgent, LeaseAgentConfig, LeaseState,
    LeaseStore, LeaseTable, MIN_DURATION, MemoryLeaseStore, Refusal, RevokeOutcome, StoredLease,
    VmFenceOutcome, budget_for, margin, stop_vms,
};
