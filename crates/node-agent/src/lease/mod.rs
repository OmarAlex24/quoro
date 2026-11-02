//! The lease state machine and the self-fence path of
//! `docs/contracts/node-agent-protocol.md` section 8.
//!
//! - [`machine`] is pure: no I/O, no async, no clock reads.
//! - [`fence`] stops the VMs bound to a lost lease within the margin.
//! - [`store`] is what survives an agent restart.
//! - [`agent`] is the async driver that ties them together.

mod agent;
mod fence;
mod machine;
mod store;

pub use agent::{LeaseAgent, LeaseAgentConfig};
pub use fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms};
pub use machine::{
    Action, Lease, LeaseState, LeaseTable, MIN_DURATION, Refusal, RevokeOutcome, margin,
};
pub use store::{FenceRecord, LeaseStore, MemoryLeaseStore, StoredLease};
