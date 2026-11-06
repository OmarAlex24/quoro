// SPDX-License-Identifier: AGPL-3.0-only
//! What the node must keep across an agent restart (section 8.5 step 2,
//! section 12 `leases`): the rows the node's SQLite store
//! ([`crate::store::NodeStore`]) reads and writes.
//!
//! Monotonic instants are not persisted: after a restart they mean nothing
//! (section 8.5), so [`StoredLease`] carries only what survives.

use std::time::{Duration, SystemTime};

use quoro_engine::VmId;
use quoro_proto::{FenceReason, LeaseId, LeaseMode, ResourceType};

use crate::lease::clock::{HostNow, StoredValidity};
use crate::lease::machine::{Lease, LeaseState};

/// One lease row as it survives a restart (section 12 `leases`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredLease {
    /// Lease identity.
    pub lease_id: LeaseId,
    /// What the lease protects.
    pub resource_type: ResourceType,
    /// The `app_` or `nod_` id.
    pub resource_id: String,
    /// `local` or `expiring`.
    pub mode: LeaseMode,
    /// The fencing token the node holds.
    pub token: u64,
    /// The node-side state (`held`, `uncertain`, `expired`, `revoked`).
    pub state: LeaseState,
    /// `duration_ms` of the grant; `None` for `local`.
    pub duration: Option<Duration>,
    /// For a `held` or `uncertain` `expiring` lease with a known
    /// `valid_until`: that deadline on `CLOCK_BOOTTIME` with the boot it
    /// belongs to (section 8.4 "Agent restart", ADR 0072). `None` otherwise,
    /// and on a host that cannot tell its boot.
    pub validity: Option<StoredValidity>,
}

impl StoredLease {
    /// The row for one tracked lease, its validity converted with `now`.
    #[must_use]
    pub fn from_lease(lease: &Lease, now: &HostNow) -> Self {
        let validity = lease.valid_until.and_then(|until| now.store(until));
        Self::with_validity(lease, validity)
    }

    /// The row for one tracked lease with a validity converted earlier, when
    /// its `valid_until` was set. The validity is kept only for a `held` or
    /// `uncertain` `expiring` lease with a `valid_until`.
    #[must_use]
    pub fn with_validity(lease: &Lease, validity: Option<StoredValidity>) -> Self {
        let validity = match (lease.mode, lease.state, lease.valid_until) {
            (LeaseMode::Expiring, LeaseState::Held | LeaseState::Uncertain, Some(_)) => validity,
            _ => None,
        };
        Self {
            lease_id: lease.id.clone(),
            resource_type: lease.resource_type,
            resource_id: lease.resource_id.clone(),
            mode: lease.mode,
            token: lease.token,
            state: lease.state,
            duration: lease.duration,
            validity,
        }
    }
}

/// One self-fence as it was recorded (section 8.5 step 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceRecord {
    /// The lease that was lost.
    pub lease_id: LeaseId,
    /// The token the node held when it fenced.
    pub token: u64,
    /// The VMs it stopped.
    pub vm_ids: Vec<VmId>,
    /// When it fenced.
    pub at: SystemTime,
    /// Why it fenced.
    pub reason: FenceReason,
    /// Whether every VM was confirmed stopped.
    pub complete: bool,
}
