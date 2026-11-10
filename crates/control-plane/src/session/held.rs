// SPDX-License-Identifier: AGPL-3.0-only
//! The grants of a marked row (`node-agent-protocol.md` section 8.4, "Grants
//! of a marked row"; ADR 0280, ADR 0292, ADR 0295, ADR 0299; issue #703).
//!
//! - **Countable acknowledgements.** Each node's acknowledgement that a
//!   held grant's write is replicated ("When R is replicated",
//!   "Acknowledgements"): the newest `log_ack` a current session accepted,
//!   or a `hello.log_seq` latched once the section 7.1 check matched it,
//!   `revoke_stale_leases` ran to its end with its raises committed, and
//!   every `lease_revoke` sent on that session is answered (ADR 0299). It is
//!   latched by an event on the live session, the check or a revoke's
//!   answer, never evaluated later: a session that ended with a revoke
//!   unanswered never counts its `hello`. It drops to 0 with `log_diverged`
//!   and is never seeded from `nodes.log_seq` or `log_acks`, so after a
//!   restart a node counts again only once it acknowledges on a new session.

use super::NodeHub;
use std::collections::HashMap;

/// The grants the hub holds (filled in by the held-grant registry).
#[derive(Debug, Default)]
pub(super) struct Registry {
    entries: HashMap<String, ()>,
}

impl NodeHub {
    /// The node's countable acknowledgement ([module docs](self)), 0 when it
    /// has none in this process.
    pub(super) fn countable_ack(&self, node_id: &str) -> u64 {
        self.0
            .countable_acks
            .lock()
            .expect("countable ack lock")
            .get(node_id)
            .copied()
            .unwrap_or(0)
    }

    /// Raises the node's countable acknowledgement to `seq` while
    /// `generation` is its current session, and wakes the held grants when
    /// it rose. Called with the `sessions` lock not held.
    fn raise_countable(&self, node_id: &str, generation: u64, seq: u64) {
        let rose = {
            let sessions = self.0.sessions.lock().expect("session lock");
            if !sessions
                .get(node_id)
                .is_some_and(|handle| handle.generation == generation)
            {
                return;
            }
            let mut acks = self.0.countable_acks.lock().expect("countable ack lock");
            let newest = acks.entry(node_id.to_string()).or_insert(0);
            let rose = seq > *newest;
            *newest = (*newest).max(seq);
            rose
        };
        if rose {
            self.wake_held();
        }
    }

    /// A `log_ack` of `seq` accepted on `generation`: it counts at once,
    /// because the node holds its `log_ack` back until it has answered the
    /// revokes before it (section 4.2 rule 3; section 8.4,
    /// "Acknowledgements").
    pub(super) fn count_log_ack(&self, node_id: &str, generation: u64, seq: u64) {
        self.raise_countable(node_id, generation, seq);
    }

    /// The section 7.1 check matched `generation`'s `hello.log_seq`: it
    /// waits on the session to be latched ([`NodeHub::latch_hello`]).
    pub(super) fn hello_checked(&self, node_id: &str, generation: u64, seq: u64) {
        {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(node_id)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            handle.pending_hello = Some(seq);
        }
        self.latch_hello(node_id, generation);
    }

    /// `revoke_stale_leases` ran to its end for `generation`, its raises
    /// committed (section 8.4, "Acknowledgements").
    pub(super) fn stale_leases_revoked(&self, node_id: &str, generation: u64) {
        {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(node_id)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            handle.stale_revoked = true;
        }
        self.latch_hello(node_id, generation);
    }

    /// Makes `generation`'s checked `hello` position countable once, at the
    /// moment the last condition holds, while the session is current: the
    /// check matched it, `revoke_stale_leases` ran to its end, no step-4
    /// revoke is held back, and every `lease_revoke` sent on the session is
    /// answered (ADR 0299). Requiring every revoke sent on the session, not
    /// only those of its start, is the side that grants less. A session that
    /// is gone latches nothing, so a `hello` whose revoke was never answered
    /// never counts.
    pub(super) fn latch_hello(&self, node_id: &str, generation: u64) {
        let latched = {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(node_id)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            let Some(seq) = handle.pending_hello else {
                return;
            };
            let unanswered = !handle
                .pending
                .lock()
                .expect("pending lock")
                .revoke_leases
                .is_empty();
            if !handle.stale_revoked || handle.held_revokes > 0 || unanswered {
                return;
            }
            handle.pending_hello = None;
            let mut acks = self.0.countable_acks.lock().expect("countable ack lock");
            let newest = acks.entry(node_id.to_string()).or_insert(0);
            let rose = seq > *newest;
            *newest = (*newest).max(seq);
            rose
        };
        if latched {
            self.wake_held();
        }
    }

    /// The primary answered the node `log_diverged`: its countable
    /// acknowledgement drops to 0, and a `hello` position still waiting on
    /// `generation` is dropped (section 7.1; section 8.4,
    /// "Acknowledgements").
    pub(super) fn drop_countable(&self, node_id: &str, generation: u64) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        if let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        {
            handle.pending_hello = None;
        }
        self.0
            .countable_acks
            .lock()
            .expect("countable ack lock")
            .insert(node_id.to_string(), 0);
    }

    /// Whether `seq` is replicated over the required set (section 8.4, "When
    /// R is replicated"), with the set read on `conn` and each machine's
    /// countable acknowledgement. `Ok(Err(..))` lists the machines that have
    /// not acknowledged it, the source of `Lease.grant_waiting_on`.
    ///
    /// # Errors
    ///
    /// The database error, as text.
    pub(super) async fn replicated(
        &self,
        conn: &mut sqlx::SqliteConnection,
        seq: u64,
    ) -> Result<Result<(), Vec<String>>, String> {
        let required = self.0.voters.required_set(conn).await?;
        let acks = self
            .0
            .countable_acks
            .lock()
            .expect("countable ack lock")
            .clone();
        Ok(required.replicated(seq, |node_id| acks.get(node_id).copied().unwrap_or(0)))
    }

    /// Wakes the held-grant task.
    pub(super) fn wake_held(&self) {
        self.0.held_wake.notify_one();
    }
}
