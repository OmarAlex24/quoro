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
//!   rows ([`NodeHub::held_session_start`]). The rows decide; the
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
//! lease does, which is safe. `failoverApp` ([`crate::failover`]) holds its
//! waiting grant with [`NodeHub::hold_grant`] and answers `in_progress` while
//! [`NodeHub::grant_pending`] (ADR 0292).

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
    /// Test seam: the node's countable acknowledgement ([module
    /// docs](self)), 0 when it has none in this process.
    #[doc(hidden)]
    #[must_use]
    pub fn countable_ack(&self, node_id: &str) -> u64 {
        self.0
            .countable_acks
            .lock()
            .expect("countable ack lock")
            .get(node_id)
            .copied()
            .unwrap_or(0)
    }

    /// Raises the node's countable acknowledgement to `seq` while
    /// `generation` is its current session and its start pass ran to its
    /// end, and wakes the held grants when it rose. Called with the
    /// `sessions` lock not held.
    fn raise_countable(&self, node_id: &str, generation: u64, seq: u64) {
        let rose = {
            let sessions = self.0.sessions.lock().expect("session lock");
            // A session whose start pass was incomplete ends before it
            // follows the log; refusing its acknowledgements here too keeps
            // a revoke that never left from being taken as answered (ADR
            // 0299).
            if !sessions
                .get(node_id)
                .is_some_and(|handle| handle.generation == generation && handle.stale_revoked)
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
    /// "Acknowledgements"). That holds only for revokes that went out, so it
    /// counts only on a session whose start pass ran to its end.
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
                // A grant held since this row was written, for a newer
                // token, stays: this row is already behind it, and its
                // attempt would only drop the entry ([`Self::held_eligible`]
                // keeps the same guard).
                Some(held) if held.token > lease.fencing_token => None,
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

    /// Whether the hub holds a grant of `lease_id` that it has not sent,
    /// whatever row it names. The re-entry checks of a regrant and of
    /// `createVolume` read it with the row's step-4 test, and reuse the row
    /// rather than write a second token (ADR 0295). What the API shows, and
    /// what a failover's `in_progress` reads, is [`Self::row_grant_pending`].
    pub(crate) fn grant_pending(&self, lease_id: &str) -> bool {
        self.0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .get(lease_id)
            .is_some_and(HeldGrant::pending)
    }

    /// The held grant of `lease` as `Lease.grant_pending` shows it: an entry
    /// not sent yet that names the row as it stands, at its token and
    /// holder, for a row that carries the mark and is not revoked (section
    /// 8.4, "Grants of a marked row"). Whichever operation held it counts:
    /// a failover, a restore, a regrant or `createVolume`.
    fn held_of_row(&self, lease: &leases::Lease) -> Option<HeldGrant> {
        if !lease.unsettled || lease.is_revoked() {
            return None;
        }
        self.0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .get(&lease.id)
            .filter(|held| held.names(lease) && held.pending())
            .cloned()
    }

    /// `Lease.grant_pending` of `lease`, the row as just read, with no
    /// write. While it is true, a second failover or a switchover of the
    /// app answers `in_progress` (ADR 0292, ADR 0303); it reads what the
    /// API shows, so a revoked row's grant no longer counts.
    pub(crate) fn row_grant_pending(&self, lease: &leases::Lease) -> bool {
        self.held_of_row(lease).is_some()
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
        let Some(held) = self.held_of_row(lease) else {
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
            leases::step4_rows(&mut conn, &self.0.unrecorded_failures, None).await
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
        let replaced = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            // A grant held since the rows were read, for a newer token,
            // stays: the row this start read is already behind it.
            if registry
                .entries
                .get(&lease.id)
                .is_some_and(|entry| entry.token > lease.fencing_token)
            {
                Err(held)
            } else {
                Ok(registry.entries.insert(lease.id.clone(), held))
            }
        };
        match replaced {
            Ok(Some(replaced)) => self.release_held_revoke(&replaced),
            Ok(None) => {}
            // Its held-back revoke was counted above and never goes out.
            Err(held) => self.release_held_revoke(&held),
        }
        self.wake_held();
    }

    /// A session start of `node_id`, before it reads its step-4 rows: each
    /// entry held for that node is checked against its row, read now on
    /// `conn`, so the registry never keeps what the rows no longer say (ADR
    /// 0295).
    ///
    /// - A row that is a step-4 row stays: the session start registers it
    ///   ([`NodeHub::held_eligible`]).
    /// - A row that names the entry, marked and unrevoked, whose lease a
    ///   failed fence lists, one this primary could not record, or the
    ///   holder's `fence_ack_pending` holds (section 8.5), keeps its entry, waiting for the holder's next session start,
    ///   as an attempt at send keeps it ([`NodeHub::attempt_held`]): the
    ///   fence may be acknowledged without the row moving.
    /// - Any other entry goes: the row moved, was revoked, lost its mark, or
    ///   its newest write did not give it its token.
    ///
    /// Each entry is changed only while it is still the one that was
    /// checked, so a grant held while this runs is never lost.
    ///
    /// # Errors
    ///
    /// A database error.
    pub(super) async fn held_session_start(
        &self,
        conn: &mut sqlx::SqliteConnection,
        node_id: &str,
    ) -> Result<(), ApiError> {
        let entries: Vec<(String, HeldGrant)> = {
            let registry = self.0.held.lock().expect("held grant lock");
            registry
                .entries
                .iter()
                .filter(|(_, held)| held.holder == node_id)
                .map(|(id, held)| (id.clone(), held.clone()))
                .collect()
        };
        for (lease_id, held) in entries {
            let row = leases::by_id(conn, &lease_id).await?;
            let unrecorded = &self.0.unrecorded_failures;
            let fenced = match row
                .as_ref()
                .filter(|row| held.names(row) && row.unsettled && !row.is_revoked())
            {
                Some(row) if leases::is_step4(conn, unrecorded, row).await? => continue,
                Some(row) => leases::fenced(conn, unrecorded, row).await?,
                None => false,
            };
            // What the entry was when it changed, so a revoke it held back
            // goes with the session it was eligible on.
            let changed = {
                let mut registry = self.0.held.lock().expect("held grant lock");
                match registry.entries.get_mut(&lease_id) {
                    Some(entry) if *entry == held && fenced => {
                        entry.eligible_on = None;
                        entry.revoke_first = None;
                        Some(held)
                    }
                    Some(entry) if *entry == held => registry.entries.remove(&lease_id),
                    _ => None,
                }
            };
            if let Some(changed) = changed {
                self.release_held_revoke(&changed);
            }
        }
        Ok(())
    }

    /// A session start found the step-4 row `lease` listed at the row's
    /// token or above: the node holds it, or this primary lacks writes.
    /// Nothing goes out for it on this session, and no grant is pending. A
    /// grant held since the rows were read, for a newer token, stays.
    pub(super) fn held_listed(&self, lease: &leases::Lease) {
        let removed = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            if registry
                .entries
                .get(&lease.id)
                .is_some_and(|entry| entry.token <= lease.fencing_token)
            {
                registry.entries.remove(&lease.id)
            } else {
                None
            }
        };
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
        // Only the entry this attempt read sends its revoke, once: one that
        // was replaced or dropped since had its held-back revoke released
        // then ([`NodeHub::release_held_revoke`]), so neither the count of
        // held-back revokes nor the node may see it a second time. The
        // registry lock is taken alone, before the sessions lock.
        {
            let mut registry = self.0.held.lock().expect("held grant lock");
            let Some(entry) = registry
                .entries
                .get_mut(lease_id)
                .filter(|entry| **entry == *held)
            else {
                return;
            };
            entry.revoke_first = None;
        }
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
        // Section 8.5: a failed fence, recorded or not (step 1), or an
        // acknowledgement pending delivery holds the grant; the
        // acknowledgement's increment changes the token.
        if leases::fenced(&mut w.tx, &self.0.unrecorded_failures, &row).await? {
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

#[cfg(test)]
mod tests {
    use super::super::tests::hub_with_node;
    use super::{HeldGrant, NodeHub};
    use crate::leases;

    /// A marked `expiring` row naming `holder` at token 2.
    fn marked(id: &str, holder: &str) -> leases::Lease {
        leases::Lease {
            id: id.to_string(),
            resource_type: "app".to_string(),
            resource_id: format!("app_{id}"),
            holder_node_id: holder.to_string(),
            mode: "expiring".to_string(),
            fencing_token: 2,
            duration_ms: Some(15_000),
            granted_at: "2026-11-09T10:00:00Z".to_string(),
            renewed_at: None,
            expires_at: None,
            revoked_at: None,
            unsettled: true,
        }
    }

    fn held_revokes(hub: &NodeHub, node_id: &str) -> u32 {
        hub.0.sessions.lock().unwrap()[node_id].held_revokes
    }

    fn entry(hub: &NodeHub, lease_id: &str) -> HeldGrant {
        hub.0.held.lock().unwrap().entries[lease_id].clone()
    }

    /// ADR 0299 and review finding m2: two step-4 revokes are held back on
    /// a session whose `hello` position waits to count. An attempt reads the
    /// first entry, and a write replaces it before the attempt sends its
    /// revoke: the replacement released that revoke, so the attempt sends
    /// nothing and leaves the count alone. The position does not count while
    /// the second revoke is held back, nor once it is sent and unanswered;
    /// it counts once that revoke is answered. (The replaced entry's revoke
    /// never goes out, so the second is the last to be sent and answered.)
    #[tokio::test]
    async fn a_replaced_entry_sends_no_held_revoke_and_the_hello_waits_for_the_other() {
        let (state, node_id, data_dir) = hub_with_node().await;
        let hub = &state.hub;
        let (generation, mut rx, _closed, _changed) = hub.attach(&node_id);
        let one = marked("lse_one", &node_id);
        let two = marked("lse_two", &node_id);
        hub.held_eligible(&one, generation, Some(1));
        hub.held_eligible(&two, generation, Some(1));
        assert_eq!(held_revokes(hub, &node_id), 2);
        hub.stale_leases_revoked(&node_id, generation);
        hub.hello_checked(&node_id, generation, 7);
        assert_eq!(hub.countable_ack(&node_id), 0);

        let read = entry(hub, "lse_one");
        let mut moved = one.clone();
        moved.fencing_token = 3;
        hub.hold_grant(&moved);
        assert_eq!(
            held_revokes(hub, &node_id),
            1,
            "the replacement released it"
        );
        hub.send_held_revoke("lse_one", &read, generation, 1);
        assert!(rx.try_recv().is_err(), "the replaced entry sends no revoke");
        assert_eq!(held_revokes(hub, &node_id), 1);
        assert_eq!(hub.countable_ack(&node_id), 0, "the other is held back");

        let read = entry(hub, "lse_two");
        hub.send_held_revoke("lse_two", &read, generation, 1);
        let frame: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(frame["type"], "lease_revoke");
        assert_eq!(frame["body"]["lease_id"], "lse_two");
        assert!(rx.try_recv().is_err());
        assert_eq!(held_revokes(hub, &node_id), 0);
        assert_eq!(hub.countable_ack(&node_id), 0, "sent, not answered");

        let id = frame["id"].as_str().unwrap();
        assert!(hub.0.take_revoke(&node_id, generation, id).is_some());
        hub.latch_hello(&node_id, generation);
        assert_eq!(hub.countable_ack(&node_id), 7);
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    /// Section 4.2 rule 3: a held grant goes out only once the revokes
    /// before it are answered, and a held-back step-4 revoke of another
    /// lease is one of them. On a live session past its start with no
    /// revoke unanswered, one held-back revoke keeps the session from being
    /// ready; with none it is ready.
    #[tokio::test]
    async fn a_held_back_revoke_keeps_the_session_from_being_ready() {
        let (state, node_id, data_dir) = hub_with_node().await;
        let hub = &state.hub;
        let (generation, _rx, _closed, _changed) = hub.attach(&node_id);
        assert!(!hub.session_ready(&node_id, generation), "start not done");
        hub.mark_start_done(&node_id, generation);
        {
            let sessions = hub.0.sessions.lock().unwrap();
            let handle = &sessions[&node_id];
            assert!(handle.start_done);
            assert!(handle.pending.lock().unwrap().revoke_leases.is_empty());
            assert_eq!(handle.held_revokes, 0);
        }
        hub.0
            .sessions
            .lock()
            .unwrap()
            .get_mut(&node_id)
            .unwrap()
            .held_revokes = 1;
        assert!(!hub.session_ready(&node_id, generation), "one held back");
        hub.0
            .sessions
            .lock()
            .unwrap()
            .get_mut(&node_id)
            .unwrap()
            .held_revokes = 0;
        assert!(hub.session_ready(&node_id, generation));
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    /// A late registration of an older token never replaces a newer one: a
    /// regrant wrote and held token 3 before a failover holds the token 2 it
    /// wrote earlier. The entry for token 3 stays, and so does its session.
    #[tokio::test]
    async fn an_older_token_does_not_replace_a_newer_held_grant() {
        let (state, node_id, data_dir) = hub_with_node().await;
        let hub = &state.hub;
        let (generation, _rx, _closed, _changed) = hub.attach(&node_id);
        let older = marked("lse_one", &node_id);
        let mut newer = older.clone();
        newer.fencing_token = 3;
        hub.held_eligible(&newer, generation, None);

        hub.hold_grant(&older);
        let held = entry(hub, "lse_one");
        assert_eq!(held.token, 3, "the newer entry stays");
        assert_eq!(held.eligible_on, Some(generation));
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    /// ADR 0292: what a failover's `in_progress` reads is what
    /// `Lease.grant_pending` shows. A held grant of the row as it stands
    /// counts; once the row is revoked, unmarked, or at another token or
    /// holder, it does not, though the registry still holds the entry.
    #[tokio::test]
    async fn the_row_grant_pending_reads_the_row() {
        let (state, node_id, data_dir) = hub_with_node().await;
        let hub = &state.hub;
        let lease = marked("lse_one", &node_id);
        hub.hold_grant(&lease);
        assert!(hub.grant_pending("lse_one"));
        assert!(hub.row_grant_pending(&lease));

        let mut revoked = lease.clone();
        revoked.revoked_at = Some("2026-11-09T10:01:00Z".to_string());
        assert!(!hub.row_grant_pending(&revoked), "revoked");
        let mut unmarked = lease.clone();
        unmarked.unsettled = false;
        assert!(!hub.row_grant_pending(&unmarked), "unmarked");
        let mut moved = lease.clone();
        moved.fencing_token = 3;
        assert!(!hub.row_grant_pending(&moved), "another token");
        let mut other = lease.clone();
        other.holder_node_id = "nod_other".to_string();
        assert!(!hub.row_grant_pending(&other), "another holder");
        assert!(hub.grant_pending("lse_one"), "the entry stays");
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    /// ADR 0299 and review finding B1: a `log_ack` counts only on a session
    /// whose start pass ran to its end.
    #[tokio::test]
    async fn a_log_ack_counts_only_once_the_start_pass_is_complete() {
        let (state, node_id, data_dir) = hub_with_node().await;
        let hub = &state.hub;
        let (generation, _rx, _closed, _changed) = hub.attach(&node_id);
        hub.count_log_ack(&node_id, generation, 5);
        assert_eq!(hub.countable_ack(&node_id), 0);
        hub.stale_leases_revoked(&node_id, generation);
        hub.count_log_ack(&node_id, generation, 5);
        assert_eq!(hub.countable_ack(&node_id), 5);
        std::fs::remove_dir_all(&data_dir).unwrap();
    }
}
