// SPDX-License-Identifier: AGPL-3.0-only
//! What the node must keep across an agent restart (section 8.5 step 2,
//! section 12 `leases`).
//!
//! [`LeaseStore`] is the seam for the node's SQLite store, which arrives with
//! the local node store. M0 ships [`MemoryLeaseStore`], which a test can clone
//! to model a second agent ("restart") reading the same file.
//!
//! Monotonic instants are not persisted: after a restart they mean nothing
//! (section 8.5), so [`StoredLease`] carries only what survives.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use quoro_engine::VmId;
use quoro_proto::{FenceReason, LeaseId, LeaseMode, ResourceType};

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
}

impl StoredLease {
    /// The row for one tracked lease.
    #[must_use]
    pub fn from_lease(lease: &Lease) -> Self {
        Self {
            lease_id: lease.id.clone(),
            resource_type: lease.resource_type,
            resource_id: lease.resource_id.clone(),
            mode: lease.mode,
            token: lease.token,
            state: lease.state,
            duration: lease.duration,
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

/// What survives an agent restart (section 8.5 step 2, section 12).
///
/// The implementation of this trait becomes the node's SQLite store; M0 needs
/// only the in-memory one.
pub trait LeaseStore {
    /// Every lease row, as last saved.
    fn load_leases(&self) -> Vec<StoredLease>;
    /// The highest token seen per resource (section 8.6).
    fn load_highest_tokens(&self) -> Vec<((ResourceType, String), u64)>;
    /// Persists one lease row and the highest token for its resource.
    fn save_lease(&self, lease: &StoredLease, highest: u64);
    /// Records one self-fence so the fact survives a restart.
    fn record_fence(&self, record: FenceRecord);
    /// Every recorded self-fence.
    fn fences(&self) -> Vec<FenceRecord>;
    /// Sets the node's failed mark (section 12 `node_meta`).
    ///
    /// The mark means the node may still be running a VM whose lease it lost.
    /// Clearing it is an operator action that arrives later; the agent only
    /// ever sets it.
    fn set_node_failed(&self, failed: bool);
    /// Whether the node is marked failed (section 12 `node_meta`).
    fn node_failed(&self) -> bool;
}

/// Everything the store keeps, behind one mutex.
#[derive(Clone, Debug, Default)]
struct Inner {
    leases: Vec<StoredLease>,
    highest: Vec<((ResourceType, String), u64)>,
    fences: Vec<FenceRecord>,
    failed: bool,
}

/// An in-memory [`LeaseStore`].
///
/// Cloning shares the same data, so a test can build a second agent on the
/// same store to model a restart.
#[derive(Clone, Debug, Default)]
pub struct MemoryLeaseStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryLeaseStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic before a write could poison the lock; the stored rows are
        // still readable, so recover rather than propagate.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl LeaseStore for MemoryLeaseStore {
    fn load_leases(&self) -> Vec<StoredLease> {
        self.lock().leases.clone()
    }

    fn load_highest_tokens(&self) -> Vec<((ResourceType, String), u64)> {
        self.lock().highest.clone()
    }

    fn save_lease(&self, lease: &StoredLease, highest: u64) {
        let mut inner = self.lock();
        match inner
            .leases
            .iter_mut()
            .find(|row| row.lease_id == lease.lease_id)
        {
            Some(row) => *row = lease.clone(),
            None => inner.leases.push(lease.clone()),
        }
        let key = (lease.resource_type, lease.resource_id.clone());
        match inner.highest.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, value)) => *value = (*value).max(highest),
            None => inner.highest.push((key, highest)),
        }
    }

    fn record_fence(&self, record: FenceRecord) {
        self.lock().fences.push(record);
    }

    fn fences(&self) -> Vec<FenceRecord> {
        self.lock().fences.clone()
    }

    fn set_node_failed(&self, failed: bool) {
        self.lock().failed = failed;
    }

    fn node_failed(&self) -> bool {
        self.lock().failed
    }
}
