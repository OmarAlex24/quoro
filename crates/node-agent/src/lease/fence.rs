//! The self-fence stop of `docs/contracts/node-agent-protocol.md` section 8.5
//! step 1: stop every stateful VM bound to a lost lease, within the margin.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use quoro_engine::{Engine, EngineError, StopMode, VmId};
use quoro_proto::LeaseMode;

use crate::lease::machine::{Lease, margin};

/// The two stop timeouts used when fencing (section 8.4, section 8.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceTimers {
    /// `T_grace`: how long a graceful stop may take.
    pub grace: Duration,
    /// `T_force`: upper bound for the forced stop that follows.
    pub force: Duration,
}

/// What happened to one VM during a fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VmFenceOutcome {
    /// The graceful stop succeeded.
    Stopped,
    /// The graceful stop did not finish in time and the forced stop succeeded.
    Forced,
    /// The engine did not know the VM; there is nothing running.
    Gone,
    /// Neither stop confirmed; the VM may still be running.
    Failed(String),
}

/// The result of fencing one lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceReport {
    /// One entry per VM, in the order the VMs were given.
    pub vms: Vec<(VmId, VmFenceOutcome)>,
}

impl FenceReport {
    /// True when no VM reported [`VmFenceOutcome::Failed`].
    #[must_use]
    pub fn complete(&self) -> bool {
        !self
            .vms
            .iter()
            .any(|(_, outcome)| matches!(outcome, VmFenceOutcome::Failed(_)))
    }
}

/// Clamps the configured timers so the whole stop fits in `M = margin(D)`
/// (section 8.4: `T_grace + T_force <= M`).
///
/// An `expiring` lease therefore always fences inside its margin, whatever the
/// configuration says. A `local` lease has no deadline (section 8.3): a revoke
/// waits as long as the configured timers, so they are returned unchanged.
#[must_use]
pub fn budget_for(lease: &Lease, timers: FenceTimers) -> FenceTimers {
    match (lease.mode, lease.duration) {
        (LeaseMode::Expiring, Some(d)) => {
            let budget = margin(d);
            let force = timers.force.min(budget);
            let grace = timers.grace.min(budget - force);
            FenceTimers { grace, force }
        }
        _ => timers,
    }
}

type StopFuture<'a> = Pin<Box<dyn Future<Output = (VmId, VmFenceOutcome)> + 'a>>;

/// Stops every VM in `vms`, all at the same time and inside one shared budget:
/// the bound applies to the whole lease, not to each VM (section 8.5).
///
/// The futures are polled by hand instead of spawned: the engine's `async fn`
/// futures are not `Send`, so `tokio::spawn` and `JoinSet` are not available.
/// Results come back in the same order as `vms`.
pub async fn stop_vms<E: Engine>(engine: &E, vms: &[VmId], timers: FenceTimers) -> FenceReport {
    let mut futures: Vec<StopFuture<'_>> = Vec::with_capacity(vms.len());
    for vm in vms {
        futures.push(Box::pin(stop_one(engine, vm.clone(), timers)));
    }

    let mut results: Vec<Option<(VmId, VmFenceOutcome)>> =
        (0..futures.len()).map(|_| None).collect();
    let mut pending = futures.len();
    std::future::poll_fn(|cx| {
        for (slot, future) in results.iter_mut().zip(futures.iter_mut()) {
            if slot.is_some() {
                continue;
            }
            if let Poll::Ready(value) = future.as_mut().poll(cx) {
                *slot = Some(value);
                pending -= 1;
            }
        }
        if pending == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;

    FenceReport {
        vms: results.into_iter().flatten().collect(),
    }
}

async fn stop_one<E: Engine>(engine: &E, id: VmId, timers: FenceTimers) -> (VmId, VmFenceOutcome) {
    // Section 8.5 step 1: graceful first, bounded by `T_grace`.
    if !timers.grace.is_zero() {
        let graceful = engine.stop(
            &id,
            StopMode::Graceful {
                timeout: timers.grace,
            },
        );
        match tokio::time::timeout(timers.grace, graceful).await {
            Ok(Ok(_)) => return (id, VmFenceOutcome::Stopped),
            Ok(Err(EngineError::NotFound)) => return (id, VmFenceOutcome::Gone),
            Ok(Err(_)) | Err(_) => {}
        }
    }

    // Section 8.5 step 1: then forced, bounded by `T_force`.
    let forced = engine.stop(&id, StopMode::Force);
    match tokio::time::timeout(timers.force, forced).await {
        Ok(Ok(_)) => (id, VmFenceOutcome::Forced),
        Ok(Err(EngineError::NotFound)) => (id, VmFenceOutcome::Gone),
        Ok(Err(error)) => (id, VmFenceOutcome::Failed(error.to_string())),
        Err(_) => (
            id,
            VmFenceOutcome::Failed("forced stop timed out".to_string()),
        ),
    }
}
