// SPDX-License-Identifier: AGPL-3.0-only
//! The lease state machine and the self-fence path of
//! `docs/contracts/node-agent-protocol.md` section 8.
//!
//! - [`machine`] is pure: no I/O, no async, no clock reads.
//! - [`fence`] stops the VMs bound to a lost lease within the margin.
//! - [`store`] is what survives an agent restart. [`clock`] is the host
//!   clock behind the lease clock: how a host suspend is counted, and how a
//!   held `expiring` lease's validity survives a restart within the same host
//!   boot.
//! - [`service`] is the async driver that ties them together for the running
//!   agent: renewals, expiry, revokes and fences.

mod clock;
mod fence;
mod machine;
mod service;
mod store;

pub use clock::{
    HostClock, HostNow, ManualHostClock, Sleep, StoredValidity, SuspendWatch, SystemHostClock,
};
pub use fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms, stop_vms_until};
pub use machine::{
    Action, Lease, LeaseState, LeaseTable, MIN_DURATION, Refusal, RevokeOutcome, margin,
};
pub use service::{
    Attached, LeaseConfig, LeaseLink, LeaseView, LocalLeases, StartPermit, start_local_leases,
};
pub use store::{FenceRecord, StoredLease};
