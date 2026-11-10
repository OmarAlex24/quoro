// SPDX-License-Identifier: AGPL-3.0-only
//! The grants of a marked row (`node-agent-protocol.md` section 8.4, "Grants
//! of a marked row"; ADR 0280, ADR 0292, ADR 0295, ADR 0299; issue #703).
//!
//! - **The registry.** A grant that must wait ([`leases::NewGrant::Waiting`])
//!   is written with its row and handed to the hub ([`NodeHub::hold_grant`]),
//!   which keeps, in memory, the row's token and holder, the holder's
//!   session it may go out on, and the session it went out on. Everything it
//!   knows is derivable from the database: at its start the hub holds every
//!   step-4 row ([`leases::step4_rows`]) as pending until its holder's next
//!   session start, and each session start decides step 4 again from the
//!   rows ([`NodeHub::held_at_session_start`]). The rows decide; the
//!   registry is a cache.
//! - **The send.** A held grant goes out only on the holder's current
//!   session, after its start, and only when one single-writer transaction
//!   finds the row unchanged and still a step-4 row, the row's newest write
//!   replicated over the required set, the session's revokes answered, and
//!   the authority accepting the grant at that moment (ADR 0299). The grant's
//!   gate is recorded then, never at the write. A grant that went out is not
//!   sent again on that session.
//! - **When.** After a registration, on every rise of a countable
//!   acknowledgement, when a session finishes its start, when a revoke is
//!   answered, and on a tick of [`TICK`]. An empty registry touches neither
//!   the pool nor the writer gate.
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
//!
//! Out of scope (issue #555, ADR 0261): the writes that clear the mark (the
//! holder's `local` report at `hello`, the `pong` after a same-token `local`
//! grant, the settle at the first renewal), R and the conversion grant, and
//! the resend of the same-token `local` grant. Until they land, a row marked
//! here stays marked after its grant is taken, and moves only as a `local`
//! lease does, which is safe. `failoverApp` (#720) uses
//! [`NodeHub::hold_grant`] and [`NodeHub::grant_pending`].

use super::{ControlMessage, NodeHub, new_msg_id};
use crate::authority::{DEFAULT_DURATION_MS, Grant, GrantKind};
use crate::error::ApiError;
use crate::leases;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

/// The longest wait between two attempts, which covers support coming and
/// going (section 8.8).
pub(super) const TICK: Duration = Duration::from_secs(1);

/// The grants the hub holds, by lease id.
#[derive(Debug, Default)]
pub(super) struct Registry {
    entries: HashMap<String, HeldGrant>,
}

/// One held grant.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HeldGrant {
    /// The row's token and holder when it was held.
    token: i64,
    holder: String,
    /// The grant length the support check counts: the row's `duration_ms`
    /// for an `expiring` row, `D` for a `local` one ("Grants of a marked
    /// row", section 8.8: `D` is what every `support_request` carries).
    support_ms: u64,
    /// The holder's session generation the grant may go out on: set by a
    /// session start that finds the row a step-4 row the node does not hold,
    /// or by a registration while that session is live after its `hello`.
    /// `None` while it waits for the holder's next session start.
    eligible_on: Option<u64>,
    /// The generation it went out on.
    sent_on: Option<u64>,
    /// A revoke of the lease at this token, in the mode the node lists it,
    /// held back until support returns; the grant follows its answer ("The
    /// holder fenced first" step 4).
    revoke_first: Option<u64>,
}

/// The grant length a held grant of `lease` needs majority support for:
/// the row's `duration_ms` for an `expiring` row, `D` for a `local` one
/// (section 8.8; `D` is what every `support_request` carries).
pub(super) fn support_ms(lease: &leases::Lease) -> u64 {
    if lease.mode == "expiring" {
        lease
            .duration_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .unwrap_or(DEFAULT_DURATION_MS)
    } else {
        DEFAULT_DURATION_MS
    }
}

impl HeldGrant {
    fn of(lease: &leases::Lease, eligible_on: Option<u64>) -> Self {
        Self {
            token: lease.fencing_token,
            holder: lease.holder_node_id.clone(),
            support_ms: support_ms(lease),
            eligible_on,
            sent_on: None,
            revoke_first: None,
        }
    }

    fn names(&self, lease: &leases::Lease) -> bool {
        self.token == lease.fencing_token && self.holder == lease.holder_node_id
    }

    fn pending(&self) -> bool {
        self.sent_on.is_none()
    }
}

/// What one attempt at a held grant found.
enum Attempt {
    /// It went out.
    Sent,
    /// It waits; the entry stays.
    Wait,
    /// The row changed: the entry goes.
    Drop,
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

impl NodeHub {
    /// Holds the grant of `lease`, a row that a grant that must wait wrote
    /// ([`leases::NewGrant::Waiting`]) and the caller committed, until it may
    /// go out (section 8.4, "Grants of a marked row"). The operation that
    /// wrote it completes: under ADR 0292 `failoverApp` answers `202` once
    /// the change that moves the lease commits, and the grant follows; under
    /// ADR 0303 a failover's raise of the stored duration waits here too. A
    /// grant already held for the same token and holder is kept as it is, so
    /// a second call never sends it twice. While the holder's session is
    /// live after its `hello`, the grant may go out on it: the node cannot
    /// hold a token written after its `hello`. Otherwise it waits for the
    /// holder's next session start.
    pub(crate) fn hold_grant(&self, lease: &leases::Lease) {
        let live = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(&lease.holder_node_id)
                .filter(|handle| handle.hello_recorded)
                .map(|handle| handle.generation)
        };
        let replaced = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            match registry.entries.get_mut(&lease.id) {
                Some(held) if held.names(lease) => {
                    if held.eligible_on.is_none() {
                        held.eligible_on = live;
                    }
                    None
                }
                _ => registry
                    .entries
                    .insert(lease.id.clone(), HeldGrant::of(lease, live)),
            }
        };
        if let Some(replaced) = replaced {
            self.release_held_revoke(&replaced);
        }
        tracing::info!(
            lease_id = %lease.id,
            holder = %lease.holder_node_id,
            token = lease.fencing_token,
            "a grant of a marked row is held until its write is replicated and supported"
        );
        self.wake_held();
    }

    /// Whether the hub holds a grant of `lease_id` that it has not sent
    /// (`Lease.grant_pending`, section 8.4, "Grants of a marked row"). While
    /// it does, a second failover or a switchover of the app answers
    /// `in_progress` (ADR 0292, ADR 0303).
    pub(crate) fn grant_pending(&self, lease_id: &str) -> bool {
        self.0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .get(lease_id)
            .is_some_and(HeldGrant::pending)
    }

    /// `Lease.grant_pending` and `Lease.grant_waiting_on` of `lease`, read on
    /// `conn` with no write: whether a grant of the row as it stands waits,
    /// and the machines of the required set that have not acknowledged the
    /// row's newest write, with its holder while the grant waits for that
    /// holder's next session start (section 8.4, "Grants of a marked row").
    ///
    /// # Errors
    ///
    /// A database error.
    pub(crate) async fn grant_view(
        &self,
        conn: &mut sqlx::SqliteConnection,
        lease: &leases::Lease,
    ) -> Result<(bool, Vec<String>), ApiError> {
        let held = {
            let registry = self.0.held.lock().expect("held grant lock");
            registry
                .entries
                .get(&lease.id)
                .filter(|held| held.names(lease) && held.pending())
                .cloned()
        };
        let Some(held) = held.filter(|_| !lease.is_revoked()) else {
            return Ok((false, Vec::new()));
        };
        let mut waiting = BTreeSet::new();
        if held.eligible_on.is_none() {
            waiting.insert(held.holder.clone());
        }
        if let Some(seq) = leases::newest_write_seq(conn, &lease.id).await?
            && let Err(machines) = self
                .replicated(conn, seq)
                .await
                .map_err(|e| ApiError::internal(&e))?
        {
            waiting.extend(machines);
        }
        Ok((true, waiting.into_iter().collect()))
    }

    /// The restart rule of "Grants of a marked row": every step-4 row
    /// ([`leases::step4_rows`]) is held as pending until its holder's next
    /// session start, which decides step 4 again (ADR 0295). Runs once,
    /// before any session.
    ///
    /// # Errors
    ///
    /// The database error, as text.
    pub(crate) async fn rebuild_held_grants(&self) -> Result<(), String> {
        let rows = async {
            let mut conn = self.0.store.pool().acquire().await?;
            leases::step4_rows(&mut conn, None).await
        }
        .await
        .map_err(|e| format!("cannot read the held grants: {}", e.code.as_str()))?;
        let mut registry = self.0.held.lock().expect("held grant lock");
        for row in rows {
            registry
                .entries
                .insert(row.id.clone(), HeldGrant::of(&row, None));
        }
        Ok(())
    }

    /// A session start of the holder (`generation`) found `lease` a step-4
    /// row the node does not hold: not listed, or listed at a lower token
    /// ("The holder fenced first" step 4). The held grant may go out on this
    /// session, and nowhere else. `revoke_first` is the token of a revoke
    /// held back for support, to go out before the grant: the node lists the
    /// lease `held` or `uncertain` in the other mode.
    pub(super) fn held_eligible(
        &self,
        lease: &leases::Lease,
        generation: u64,
        revoke_first: Option<u64>,
    ) {
        if revoke_first.is_some() {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(&lease.holder_node_id)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            handle.held_revokes += 1;
        }
        let mut held = HeldGrant::of(lease, Some(generation));
        held.revoke_first = revoke_first;
        let replaced = self
            .0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .insert(lease.id.clone(), held);
        if let Some(replaced) = replaced {
            self.release_held_revoke(&replaced);
        }
        self.wake_held();
    }

    /// A session start found the step-4 row `lease_id` listed at the row's
    /// token or above: the node holds it, or this primary lacks writes.
    /// Nothing goes out for it on this session, and no grant is pending.
    pub(super) fn held_listed(&self, lease_id: &str) {
        let removed = self
            .0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .remove(lease_id);
        if let Some(removed) = removed {
            self.release_held_revoke(&removed);
        }
    }

    /// Forgets the held-back revoke of an entry that goes, so its session's
    /// `hello` position may count again.
    fn release_held_revoke(&self, held: &HeldGrant) {
        let (Some(generation), Some(_)) = (held.eligible_on, held.revoke_first) else {
            return;
        };
        {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            if let Some(handle) = sessions
                .get_mut(&held.holder)
                .filter(|handle| handle.generation == generation)
            {
                handle.held_revokes = handle.held_revokes.saturating_sub(1);
            }
        }
        self.latch_hello(&held.holder, generation);
    }

    /// `generation`'s first desired state went out after its start (section
    /// 4.2 rules 3 to 5): held grants may go out on it from now on.
    pub(super) fn mark_start_done(&self, node_id: &str, generation: u64) {
        let newly = {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get_mut(node_id)
                .filter(|handle| handle.generation == generation && !handle.start_done)
                .is_some_and(|handle| {
                    handle.start_done = true;
                    true
                })
        };
        if newly {
            self.wake_held();
        }
    }

    /// Whether `generation` is the node's live session, has finished its
    /// start, and has no `lease_revoke` unanswered and none held back. A
    /// grant goes out only then: the node holds a grant back until it has
    /// answered the revokes before it (section 4.2 rule 3), and waiting for
    /// every one of them is the side that grants less.
    fn session_ready(&self, node_id: &str, generation: u64) -> bool {
        let sessions = self.0.sessions.lock().expect("session lock");
        sessions
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .is_some_and(|handle| {
                handle.start_done
                    && handle.held_revokes == 0
                    && handle
                        .pending
                        .lock()
                        .expect("pending lock")
                        .revoke_leases
                        .is_empty()
            })
    }

    /// Runs the held grants: one attempt now, then one after each wake or
    /// [`TICK`], until the hub is gone. Never a busy loop on the writer
    /// gate: an attempt takes it only for a grant whose reads already allow
    /// it.
    pub(crate) fn spawn_held_grants(&self) {
        let hub = std::sync::Arc::downgrade(&self.0);
        let wake = std::sync::Arc::clone(&self.0.held_wake);
        tokio::spawn(async move {
            loop {
                {
                    let Some(inner) = hub.upgrade() else {
                        break;
                    };
                    NodeHub(inner).attempt_held().await;
                }
                tokio::select! {
                    () = wake.notified() => {}
                    () = tokio::time::sleep(TICK) => {}
                }
            }
        });
    }

    /// One attempt at every held grant that may go out on its holder's
    /// current session. An empty registry touches neither the pool nor the
    /// writer gate.
    pub(super) async fn attempt_held(&self) {
        let entries: Vec<(String, HeldGrant)> = {
            let registry = self.0.held.lock().expect("held grant lock");
            registry
                .entries
                .iter()
                .map(|(id, held)| (id.clone(), held.clone()))
                .collect()
        };
        for (lease_id, held) in entries {
            let Some(generation) = held.eligible_on else {
                continue;
            };
            if held.sent_on == Some(generation) {
                continue;
            }
            if let Some(token) = held.revoke_first {
                // Step 4: without majority support neither the revoke nor
                // the grant goes out.
                if self.0.authority.supports(held.support_ms) {
                    self.send_held_revoke(&lease_id, &held, generation, token);
                }
                continue;
            }
            if !self.session_ready(&held.holder, generation) {
                continue;
            }
            match self.try_send_held(&lease_id, &held, generation).await {
                Ok(Attempt::Sent | Attempt::Wait) => {}
                Ok(Attempt::Drop) => {
                    let mut registry = self.0.held.lock().expect("held grant lock");
                    if registry.entries.get(&lease_id) == Some(&held) {
                        registry.entries.remove(&lease_id);
                    }
                }
                Err(e) => tracing::warn!(
                    code = %e.code.as_str(),
                    lease_id,
                    "cannot try a held grant; trying again"
                ),
            }
        }
    }

    /// Sends the revoke held back for support on `generation`, at the token
    /// the node reported, before the grant ("The holder fenced first" step
    /// 4). Its `ok` releases nothing: the row names this node at a token it
    /// never held.
    fn send_held_revoke(&self, lease_id: &str, held: &HeldGrant, generation: u64, token: u64) {
        let id = new_msg_id();
        let text = ControlMessage::LeaseRevoke(quoro_proto::LeaseRevoke {
            lease_id: quoro_proto::LeaseId(lease_id.to_string()),
            fencing_token: token,
            reason: quoro_proto::RevokeReason::Superseded,
        })
        .to_frame(&id, None)
        .to_text();
        {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(&held.holder)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            // Counted and registered before the frame is queued, as
            // `send_control` does.
            handle.revokes += 1;
            {
                let mut pending = handle.pending.lock().expect("pending lock");
                pending.revoke_leases.insert(id, (lease_id.to_string(), 0));
                pending.revoked.insert(lease_id.to_string());
            }
            handle.held_revokes = handle.held_revokes.saturating_sub(1);
            let _ = handle.tx.send(text);
        }
        let mut registry = self.0.held.lock().expect("held grant lock");
        if let Some(entry) = registry
            .entries
            .get_mut(lease_id)
            .filter(|entry| **entry == *held)
        {
            entry.revoke_first = None;
        }
        tracing::info!(
            lease_id,
            node_id = %held.holder,
            token,
            "a held step-4 revoke went out once support returned"
        );
    }

    /// One attempt at a held grant on `generation`, whose prechecks passed.
    /// A read first, outside the writer gate, so that a grant still waiting
    /// for acknowledgements or support does not take it; then every
    /// condition again in one single-writer transaction, in which the
    /// authority records the grant and the grant is queued (ADR 0299). That
    /// transaction writes nothing: a held grant carries the duration its row
    /// already stores, so no raise comes before it (ADR 0203, ADR 0268).
    async fn try_send_held(
        &self,
        lease_id: &str,
        held: &HeldGrant,
        generation: u64,
    ) -> Result<Attempt, ApiError> {
        {
            let mut conn = self.0.store.pool().acquire().await?;
            let Some(row) = leases::by_id(&mut conn, lease_id).await? else {
                return Ok(Attempt::Drop);
            };
            if !held.names(&row) || row.is_revoked() || !row.unsettled {
                return Ok(Attempt::Drop);
            }
            if !self.0.authority.supports(held.support_ms)
                || self.0.authority.gate_remaining(lease_id).is_some()
            {
                return Ok(Attempt::Wait);
            }
            let Some(seq) = leases::newest_write_seq(&mut conn, lease_id).await? else {
                return Ok(Attempt::Drop);
            };
            if self
                .replicated(&mut conn, seq)
                .await
                .map_err(|e| ApiError::internal(&e))?
                .is_err()
            {
                return Ok(Attempt::Wait);
            }
        }
        let mut w = self.0.store.begin_write().await?;
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(Attempt::Drop);
        };
        // The row still names the holder at the token, unrevoked and marked.
        if !held.names(&row) || row.is_revoked() || !row.unsettled {
            return Ok(Attempt::Drop);
        }
        // Section 8.5: a failed fence or an acknowledgement pending delivery
        // holds the grant; the acknowledgement's increment changes the token.
        if crate::fences::listed_leases(&mut w.tx)
            .await?
            .contains(lease_id)
            || crate::fences::pending_of(&mut w.tx, &row.holder_node_id)
                .await?
                .is_some_and(|pending| pending.lease_ids.iter().any(|id| id == lease_id))
        {
            return Ok(Attempt::Wait);
        }
        // Step 4: the row's newest write gave the lease its token.
        if !leases::newest_write_gave_new_token(&mut w.tx, lease_id).await? {
            return Ok(Attempt::Drop);
        }
        let Some(seq) = leases::newest_write_seq(&mut w.tx, lease_id).await? else {
            return Ok(Attempt::Drop);
        };
        if self
            .replicated(&mut w.tx, seq)
            .await
            .map_err(|e| ApiError::internal(&e))?
            .is_err()
        {
            return Ok(Attempt::Wait);
        }
        if !self.session_ready(&row.holder_node_id, generation) {
            return Ok(Attempt::Wait);
        }
        let sent_wall = time::OffsetDateTime::now_utc();
        if row.mode == "expiring" {
            let Some(duration_ms) = row.duration_ms.and_then(|ms| u64::try_from(ms).ok()) else {
                tracing::warn!(lease_id, "a held expiring grant's row stores no duration");
                return Ok(Attempt::Wait);
            };
            // Checks the gate, the start gate after a restart, and majority
            // support for the duration, and starts the holder's gate now.
            if let Err(refused) = self.0.authority.record_grant(&Grant {
                lease_id,
                holder: &row.holder_node_id,
                duration_ms,
                kind: GrantKind::NewToken,
                sent_wall,
            }) {
                tracing::debug!(lease_id, ?refused, "a held grant waits for the authority");
                return Ok(Attempt::Wait);
            }
        } else {
            // A marked row's `local` grant needs majority support too
            // ("Grants of a marked row", section 8.8).
            if !self.0.authority.supports(DEFAULT_DURATION_MS) {
                return Ok(Attempt::Wait);
            }
            if let Err(refused) = self.0.authority.record_local_grant(lease_id) {
                tracing::debug!(lease_id, ?refused, "a held grant waits for the gate");
                return Ok(Attempt::Wait);
            }
        }
        let mut grant = row.grant();
        // Section 5.12: the authority's wall-clock time of the grant.
        grant.granted_at = crate::store::format_rfc3339(sent_wall);
        let text = ControlMessage::LeaseGrant(grant)
            .to_frame(&new_msg_id(), None)
            .to_text();
        // Queued under the writer gate: a revoke of the lease follows a
        // committed write, so it reaches the node after this grant.
        let sent = self.0.send_text(&row.holder_node_id, generation, text);
        if sent {
            let mut registry = self.0.held.lock().expect("held grant lock");
            if let Some(entry) = registry
                .entries
                .get_mut(lease_id)
                .filter(|entry| entry.names(&row))
            {
                entry.sent_on = Some(generation);
            }
        }
        drop(w);
        if !sent {
            return Ok(Attempt::Wait);
        }
        tracing::info!(
            lease_id,
            node_id = %row.holder_node_id,
            token = row.fencing_token,
            mode = %row.mode,
            "a held grant of a marked row went out"
        );
        // Section 6.4: the desired state carries the token.
        self.node_changed(&row.holder_node_id);
        Ok(Attempt::Sent)
    }
}
