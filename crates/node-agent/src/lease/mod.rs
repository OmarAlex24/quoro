// SPDX-License-Identifier: AGPL-3.0-only
//! The lease state machine and the self-fence path of
//! `docs/contracts/node-agent-protocol.md` section 8.
//!
//! - [`machine`] is pure: no I/O, no async, no clock reads.
//! - [`fence`] stops the VMs bound to a lost lease within the margin.
//! - [`store`] is what survives an agent restart, and [`clock`] how a held
//!   `expiring` lease's validity survives it within the same host boot.
//! - [`service`] is the async driver that ties them together for the running
//!   agent: renewals, expiry, revokes and fences.

mod clock;
mod fence;
mod machine;
mod service;
mod store;

pub use clock::{HostClock, HostNow, ManualHostClock, StoredValidity, SystemHostClock};
pub use fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms};
pub use machine::{
    Action, Lease, LeaseState, LeaseTable, MIN_DURATION, Refusal, RevokeOutcome, margin,
};
pub use service::{
    Attached, LeaseConfig, LeaseLink, LeaseView, LocalLeases, StartPermit, start_local_leases,
};
pub use store::{FenceRecord, StoredLease};
