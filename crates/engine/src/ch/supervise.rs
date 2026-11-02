// SPDX-License-Identifier: AGPL-3.0-only
//! One task per live VMM: it watches the process and the event log, and records
//! the exit (contract §3.8, §3.11, §3.12).
//!
//! The VMM is a separate process whose lifetime is not the agent's (D-0011), so
//! this task never restarts anything (§3.11) and never takes a decision besides
//! the one the state module's [`classify`] encodes: an exit is a `Stopped` when
//! a stop was requested and a `Crashed` otherwise.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::ch::events::{EventLog, saw_reset};
use crate::ch::state::{ExitFacts, classify, marker_exists, write_json_atomic};
use crate::ch::vmm::{ProcessId, Vmm, VmmExit};

use super::engine::{Inner, VmEntry};

/// How often the process and the event log are checked.
const TICK: Duration = Duration::from_secs(1);

/// Starts the supervisor for `vmm` and returns immediately.
pub(super) fn start(inner: Arc<Inner>, entry: Arc<VmEntry>, vmm: Vmm) {
    tokio::spawn(async move { watch(inner, entry, vmm).await });
}

/// Follows one VMM until its process exits, then records the exit.
async fn watch(inner: Arc<Inner>, entry: Arc<VmEntry>, vmm: Vmm) {
    let mut log = EventLog::new(entry.paths.event_log.clone());
    let mut reset_seen = false;
    loop {
        if let Some(exit) = vmm.wait_exit(TICK).await {
            finish(&inner, &entry, Some(vmm.id()), Some(exit), reset_seen);
            return;
        }
        // A guest reset (kernel panic, init crash) is a crash, not a clean exit:
        // the VMM would keep the machine alive otherwise.
        if entry.booted.load(Ordering::SeqCst)
            && let Ok(events) = log.poll()
            && saw_reset(&events)
        {
            reset_seen = true;
            let _ = vmm.sigkill();
        }
    }
}

/// Writes the exit record, clears the runtime files and emits the event.
///
/// `vmm` is the identity of the process that ended, when it is known, so a late
/// supervisor of a replaced VMM can never clear the handle of a newer one.
pub(super) fn finish(
    inner: &Inner,
    entry: &VmEntry,
    vmm: Option<ProcessId>,
    vmm_exit: Option<VmmExit>,
    reset_seen: bool,
) {
    let booted = entry.booted.swap(false, Ordering::SeqCst);
    let facts = ExitFacts {
        booted,
        stop_requested: marker_exists(&entry.paths.stop_requested),
        reset_seen,
        status: crate::boot::read_status(&entry.paths.config_disk)
            .ok()
            .flatten(),
        vmm_exit,
    };
    let classified = classify(&entry.id, &facts);
    let _ = write_json_atomic(&entry.paths.last_exit, &classified.record);
    let _ = std::fs::remove_file(&entry.paths.vmm);
    let _ = std::fs::remove_file(&entry.paths.api_socket);

    if let Ok(mut slot) = entry.vmm.lock()
        && slot.as_ref().is_none_or(|live| Some(live.id()) == vmm)
    {
        *slot = None;
    }
    if let Some(event) = classified.event {
        inner.emit(event);
    }
    entry
        .settled
        .send_modify(|value| *value = value.wrapping_add(1));
}
