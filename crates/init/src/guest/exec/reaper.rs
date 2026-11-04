// SPDX-License-Identifier: AGPL-3.0-only
//! The exec reaper registry.
//!
//! PID 1's `supervise` loop reaps *any* child, including the helper behind an
//! exec session, so a session must not call `waitpid` for its own pid: it
//! would race the loop and sometimes never see its child. Instead a session
//! registers the pid here and waits on the channel; `supervise` and
//! `reap_remaining` call [`reaped`] with every status they collect.
//!
//! The status can arrive before the session could register (a program may
//! exit between `spawn` and the next line), so [`spawn_registered`] spawns
//! and registers under the lock [`reaped`] takes: the status of a new helper
//! cannot be handed out before its pid is in the table. A status nobody waits
//! for (the app's own orphans, reparented to PID 1) is dropped, so the table
//! never grows and a reused pid never inherits an old status.

use std::collections::HashMap;
use std::io;
use std::process::{Child, Command};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, MutexGuard, OnceLock};

use rustix::process::{Pid, WaitStatus};

/// Sessions that are waiting for their child's status, by pid.
type State = HashMap<i32, Sender<WaitStatus>>;

fn state() -> MutexGuard<'static, State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE
        .get_or_init(|| Mutex::new(State::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Spawn a helper and register its pid in one step, returning the receiver
/// its status will arrive on.
///
/// The lock is held across `spawn`, so `supervise` cannot hand the status of
/// a program that exits at once to nobody: it waits in [`reaped`] until the
/// pid is registered.
pub(crate) fn spawn_registered(command: &mut Command) -> io::Result<(Child, Receiver<WaitStatus>)> {
    let mut state = state();
    let child = command.spawn()?;
    let (sender, receiver) = channel();
    state.insert(Pid::from_child(&child).as_raw_pid(), sender);
    Ok((child, receiver))
}

/// Hand a reaped status to the session waiting for `pid`. Called for every
/// pid `supervise` reaps that is not the app; a pid nobody registered is an
/// orphan of the app and its status is dropped.
pub(crate) fn reaped(pid: Pid, status: WaitStatus) {
    let sender = state().remove(&pid.as_raw_pid());
    if let Some(sender) = sender {
        // A dropped receiver means the session ended without waiting.
        let _ = sender.send(status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::process::{WaitOptions, waitpid};

    #[test]
    fn a_registered_status_is_forwarded() {
        let (child, receiver) = spawn_registered(&mut Command::new("/bin/true"))
            .unwrap_or_else(|err| panic!("spawn /bin/true: {err}"));
        let pid = Pid::from_child(&child);
        drop(child);
        let (_, status) = waitpid(Some(pid), WaitOptions::empty())
            .unwrap_or_else(|err| panic!("waitpid: {err}"))
            .unwrap_or_else(|| panic!("no status for /bin/true"));
        reaped(pid, status);
        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_else(|err| panic!("status: {err}"));
        assert_eq!(received.exit_status(), Some(0));
        assert!(!state().contains_key(&pid.as_raw_pid()));
    }

    #[test]
    #[allow(clippy::zombie_processes)]
    fn an_unregistered_status_is_dropped() {
        let child = Command::new("/bin/true")
            .spawn()
            .unwrap_or_else(|err| panic!("spawn /bin/true: {err}"));
        let pid = Pid::from_child(&child);
        let (_, status) = waitpid(Some(pid), WaitOptions::empty())
            .unwrap_or_else(|err| panic!("waitpid: {err}"))
            .unwrap_or_else(|| panic!("no status for /bin/true"));
        reaped(pid, status);
        assert!(!state().contains_key(&pid.as_raw_pid()));
    }
}
