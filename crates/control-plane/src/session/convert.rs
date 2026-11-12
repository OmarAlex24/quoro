// SPDX-License-Identifier: AGPL-3.0-only
//! The conversion grant: the same-token `expiring` grant that converts a
//! `local` lease when the app's HA policy becomes `warm`
//! (`node-agent-protocol.md` section 8.4, "Changing the mode", "From R until
//! the holder has taken the grant", "Settling a lease", "The mark", "Grants
//! of a marked row" and "What the authority must enforce"; ADR 0241, ADR
//! 0244, ADR 0254, ADR 0257, ADR 0268, ADR 0289; issue #555).
//!
//! - **Held.** The grant is held in the held-grant registry
//!   ([`super::held`], [`Kind::Convert`]), eligible on one session of the
//!   holder: the current one after its `hello` when `updateApp` changes the
//!   policy to `warm` ([`NodeHub::hold_conversion`]), or the session whose
//!   `hello` listed the lease `local` at the row's token while the policy
//!   is `warm` ([`NodeHub::hold_conversion_on`]): "When the holder's `hello`
//!   reports the lease as `local`, the primary sends the conversion grant
//!   again". A holder with no session holds nothing: its next session start
//!   decides. A waiting conversion is no wait: it shows no `grant_pending`
//!   and makes no failover answer `in_progress`, because the lease stays
//!   `local` until the first answer after it.
//! - **When it goes.** One pure function ([`convert_step`]) decides from
//!   the row and the session: no conversion over a marked row; ADR 0257's
//!   barrier first, the same-token `local` grant and its `ping` sent by the
//!   entry itself when the session has not shown the holder's mode; the
//!   raise of `leases.duration_ms` to `D` before a grant of `D` (ADR 0254),
//!   sent once that raise is replicated over the required set (ADR 0268),
//!   with the stored duration meanwhile when it is shorter, and nothing for
//!   a row that stores none (ADR 0289).
//! - **The send.** Under the writer gate, the row found again unrevoked,
//!   naming the entry and reading `local`, the policy `warm`, no failed
//!   fence, the session ready, no conversion in flight on it, and majority
//!   support for the duration sent (for `D` before the barrier or the
//!   raise, which precede a grant of `D`). The grant
//!   carries the row's token, `mode` `expiring`, the duration and no `re`
//!   (ADR 0256). It is queued and recorded on the session in one hold of the
//!   sessions lock ([`NodeHub::queue_conversion`]), which checks the barrier
//!   again: its first renewal is then answered as the first one, writing the
//!   row `expiring` ([`super::conversion`]). The send writes nothing but a
//!   raise; the conversion grant counts toward no gate (ADR 0241).
//! - **A refusal.** A refusal of the grant drops what its send recorded and
//!   the held entry, writes nothing, and sends no other on that session
//!   ([`NodeHub::conversion_refused`]): the holder's next `hello` decides.
//! - **A same-token `local` grant** sent after it ends the conversion in
//!   flight ([`NodeHub::queue_local_end`]), as R does.

use super::conversion::{self, InFlight};
use super::held::{Attempt, HeldGrant, Kind};
use super::local_end::Barrier;
use super::{ControlMessage, NodeHub, lease_token, new_msg_id};
use crate::authority::{DEFAULT_DURATION_MS, MAX_DURATION_MS, MIN_DURATION_MS};
use crate::changelog::{Actor, ActorType};
use crate::error::ApiError;
use crate::leases::{self, Lease};

/// What [`convert_step`] reads, gathered under the writer gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ConvertFacts {
    /// The row carries the unsettled mark.
    pub marked: bool,
    /// ADR 0257's barrier passed for the lease at the row's token on this
    /// session ([`NodeHub::barrier_passed`]).
    pub barrier_passed: bool,
    /// This entry already sent its barrier `local` grant on this session.
    pub barrier_sent_here: bool,
    /// The lease row's newest change-log write is replicated over the
    /// required set.
    pub newest_replicated: bool,
    /// The raise this entry wrote itself ([`HeldGrant::raised_seq`]):
    /// `Some(true)` once that write is replicated over the required set,
    /// `Some(false)` before, `None` when this entry wrote no raise.
    pub own_raise_replicated: Option<bool>,
    /// The row's stored `duration_ms`, as a valid duration (3 000 to
    /// 600 000 ms), or `None` for NULL or invalid.
    pub stored_ms: Option<u64>,
}

/// What one attempt at a conversion does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConvertStep {
    /// Nothing now; the entry waits.
    Wait,
    /// Send the same-token `local` grant and its `ping` first.
    Barrier,
    /// Raise `leases.duration_ms` to `D`, commit, and wait for that raise
    /// to be replicated.
    RaiseAndWait,
    /// Raise `leases.duration_ms` to `D`, and send the conversion now with
    /// the stored, shorter duration.
    RaiseAndSend { duration_ms: u64 },
    /// Send the conversion with `duration_ms`.
    Send { duration_ms: u64 },
}

/// Whether the conversion grant goes out now, and how, in this order:
///
/// 1. **A marked row waits.** Section 8.4 does not state whether a
///    conversion may go out over a row that carries the unsettled mark;
///    this function reads it as no, which grants less. A marked row may
///    disagree with its holder's mode ("The mark"), and "The primary sends
///    no conversion grant to a holder whose mode it has not learned on the
///    current session" ("From R until the holder has taken the grant"). The
///    mark clears first, by the `pong` after a same-token `local` grant or
///    by the next `hello`'s `local` report; then the conversion goes.
/// 2. **The barrier.** "After it sends a same-token `local` grant for a
///    lease, the primary therefore sends the node a `ping` (section 5.19),
///    and it sends no conversion grant for that lease until the node's
///    `pong` arrives" (ADR 0257); a `hello` that listed the lease `local` at
///    the token stands in for that `pong`. Section 8.4 does not state what
///    the primary does on a session where neither happened, as when the
///    lease was granted after the `hello`; this function reads it as: send
///    the same-token `local` grant and its `ping` first, which on a node that
///    holds the lease `local` changes nothing (section 5.12), and wait for
///    the `pong`. That grants less. A refusal before the `pong` fails the
///    barrier, and the entry sends no second barrier grant on that session:
///    the next session decides.
/// 3. **The raise.** "Before it sends a conversion grant, the primary raises
///    `leases.duration_ms` to at least that grant's duration … A conversion
///    grant longer than the duration held before the raise is sent only once
///    that raise is replicated (ADR 0268). Until then the conversion grant
///    carries the duration held before the raise; a row that held none, as a
///    `local` row may not, has no shorter duration to fall back to, so its
///    conversion grant waits for the raise (ADR 0289)." A row that stores no
///    valid duration raises and waits; one that stores less than `D` raises
///    and sends the stored duration now; one that stores `D` or more needs no
///    raise, and the grant carries `D`.
/// 4. **This entry's own raise.** Once this entry wrote the raise, the grant
///    of `D` goes only once that raise is replicated ("waits for the raise",
///    ADR 0289).
/// 5. **A raise this entry did not write.** Section 8.4 does not state how a
///    primary that did not write the raise, as one that restarted after an
///    earlier primary's raise, knows it is replicated; this function reads it
///    as: once the row's newest change-log write is replicated over the
///    required set, which covers whichever write raised it, and grants less.
#[must_use]
pub(super) fn convert_step(facts: ConvertFacts) -> ConvertStep {
    if facts.marked {
        return ConvertStep::Wait;
    }
    if !facts.barrier_passed {
        return if facts.barrier_sent_here {
            ConvertStep::Wait
        } else {
            ConvertStep::Barrier
        };
    }
    match facts.stored_ms {
        None => return ConvertStep::RaiseAndWait,
        Some(stored) if stored < DEFAULT_DURATION_MS => {
            return ConvertStep::RaiseAndSend {
                duration_ms: stored,
            };
        }
        Some(_) => {}
    }
    if let Some(replicated) = facts.own_raise_replicated {
        return if replicated {
            ConvertStep::Send {
                duration_ms: DEFAULT_DURATION_MS,
            }
        } else {
            ConvertStep::Wait
        };
    }
    if facts.newest_replicated {
        ConvertStep::Send {
            duration_ms: DEFAULT_DURATION_MS,
        }
    } else {
        ConvertStep::Wait
    }
}

/// The row's stored `duration_ms`, when it is a duration the authority may
/// grant (3 000 to 600 000 ms).
fn stored_ms(row: &Lease) -> Option<u64> {
    row.duration_ms
        .and_then(|ms| u64::try_from(ms).ok())
        .filter(|ms| (MIN_DURATION_MS..=MAX_DURATION_MS).contains(ms))
}

/// Whether `row` still owes the conversion `held`: it names the entry
/// (holder and token), unrevoked, and reads `local`.
fn owes(row: &Lease, held: &HeldGrant) -> bool {
    held.names(row) && !row.is_revoked() && row.mode == "local"
}

impl NodeHub {
    /// Holds the conversion grant of `lease`, a row that reads `local`, as
    /// `updateApp` commits a policy change to `warm`
    /// ([`leases::enter_warm`]): eligible on the holder's current session
    /// after its `hello`. With no such session it holds nothing: the
    /// holder's next session start decides, from what its `hello` lists.
    pub(crate) fn hold_conversion(&self, lease: &Lease) {
        let live = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(&lease.holder_node_id)
                .filter(|handle| handle.hello_recorded)
                .map(|handle| handle.generation)
        };
        let Some(generation) = live else {
            tracing::info!(
                lease_id = %lease.id,
                holder = %lease.holder_node_id,
                "the holder has no session; its next session start decides the conversion"
            );
            return;
        };
        self.register_conversion(lease, generation);
    }

    /// The resend from `hello`: the conversion grant may go out on
    /// `generation`, whose `hello` listed the lease `local` at the row's
    /// token while the policy is `warm`.
    pub(super) fn hold_conversion_on(&self, lease: &Lease, generation: u64) {
        self.register_conversion(lease, generation);
    }

    /// Holds the conversion afresh, eligible on `generation`, as
    /// [`NodeHub::register_local_end`] holds the `local` grant: a grant held
    /// for a newer token stays, and so does a grant with a new token not
    /// sent yet for this very row ("Grants of a marked row"); anything else
    /// is replaced.
    fn register_conversion(&self, lease: &Lease, generation: u64) {
        let (held, replaced) = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            match registry.entries.get(&lease.id) {
                Some(held) if held.token > lease.fencing_token => (false, None),
                Some(held)
                    if held.kind == Kind::NewToken
                        && held.names(lease)
                        && held.sent_on.is_none() =>
                {
                    (false, None)
                }
                _ => {
                    let mut held = HeldGrant::of(lease, Some(generation));
                    held.kind = Kind::Convert;
                    held.support_ms = DEFAULT_DURATION_MS;
                    (true, registry.insert(lease.id.clone(), held))
                }
            }
        };
        if let Some(replaced) = replaced {
            self.release_held_revoke(&replaced);
        }
        if !held {
            tracing::info!(
                lease_id = %lease.id,
                token = lease.fencing_token,
                "another grant is held for the row; no conversion is held over it"
            );
            return;
        }
        tracing::info!(
            lease_id = %lease.id,
            holder = %lease.holder_node_id,
            token = lease.fencing_token,
            generation,
            "the conversion grant is held"
        );
        self.wake_held();
    }

    /// Whether the held conversion `held` of `lease_id`, not sent yet, is
    /// owed no more: there is no row, the row no longer names it (holder and
    /// token), is revoked or no longer reads `local`, or the app's policy is
    /// no longer `warm`. A marked row still owes it: it waits.
    ///
    /// # Errors
    ///
    /// A database error.
    pub(super) async fn convert_obsolete(
        &self,
        lease_id: &str,
        held: &HeldGrant,
    ) -> Result<bool, ApiError> {
        let mut conn = self.0.store.pool().acquire().await?;
        let Some(row) = leases::by_id(&mut conn, lease_id).await? else {
            return Ok(true);
        };
        if !owes(&row, held) {
            return Ok(true);
        }
        Ok(!conversion::app_is_warm(&mut conn, &row.resource_id).await?)
    }

    /// What [`convert_step`] reads of `row` on `generation`, on `conn`, with
    /// the entry's `barrier_on` and `raised_seq`. A row the change log has
    /// no entry of has no newest write to be replicated, and reads as not
    /// replicated.
    async fn convert_facts(
        &self,
        conn: &mut sqlx::SqliteConnection,
        row: &Lease,
        generation: u64,
        barrier_on: Option<u64>,
        raised_seq: Option<u64>,
    ) -> Result<ConvertFacts, ApiError> {
        let newest_replicated = match leases::newest_write_seq(conn, &row.id).await? {
            Some(seq) => self
                .replicated(conn, seq)
                .await
                .map_err(|e| ApiError::internal(&e))?
                .is_ok(),
            None => false,
        };
        let own_raise_replicated = match raised_seq {
            Some(seq) => Some(
                self.replicated(conn, seq)
                    .await
                    .map_err(|e| ApiError::internal(&e))?
                    .is_ok(),
            ),
            None => None,
        };
        Ok(ConvertFacts {
            marked: row.unsettled,
            barrier_passed: self.barrier_passed(
                &row.holder_node_id,
                generation,
                &row.id,
                lease_token(row),
            ),
            barrier_sent_here: barrier_on == Some(generation),
            newest_replicated,
            own_raise_replicated,
            stored_ms: stored_ms(row),
        })
    }

    /// Changes the live entry of `lease_id` in place, keeping its
    /// registration, only while it is still the conversion registered as
    /// `registration`. The registry lock is held only for the change.
    fn change_conversion(
        &self,
        lease_id: &str,
        registration: u64,
        change: impl FnOnce(&mut HeldGrant),
    ) {
        let mut registry = self.0.held.lock().expect("held grant lock");
        if let Some(entry) = registry
            .entries
            .get_mut(lease_id)
            .filter(|entry| entry.kind == Kind::Convert && entry.registration == registration)
        {
            change(entry);
        }
    }

    /// One attempt at the conversion `held` of `lease_id` on `generation`
    /// ([module docs](self)): a read on a pool connection first, which
    /// leaves a conversion that waits without the writer gate, then every
    /// condition again under it, where the step is decided and acted on.
    pub(super) async fn try_send_conversion(
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
            if !owes(&row, held) {
                return Ok(Attempt::Drop);
            }
            if !conversion::app_is_warm(&mut conn, &row.resource_id).await? {
                return Ok(Attempt::Drop);
            }
            let facts = self
                .convert_facts(&mut conn, &row, generation, held.barrier_on, held.raised_seq)
                .await?;
            // Without majority support for what the step would send, or
            // for the grant of `D` the barrier or the raise precedes, the
            // conversion waits without taking the writer gate.
            let needs_ms = match convert_step(facts) {
                ConvertStep::Wait => return Ok(Attempt::Wait),
                ConvertStep::Barrier | ConvertStep::RaiseAndWait => DEFAULT_DURATION_MS,
                ConvertStep::RaiseAndSend { duration_ms } | ConvertStep::Send { duration_ms } => {
                    duration_ms
                }
            };
            if !self.0.authority.supports(needs_ms) {
                return Ok(Attempt::Wait);
            }
        }
        let mut w = self.0.store.begin_write().await?;
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(Attempt::Drop);
        };
        if !owes(&row, held) {
            return Ok(Attempt::Drop);
        }
        // "Changing the mode": the conversion goes while the policy is
        // `warm`; a policy off `warm` ends it, and R holds the `local` grant.
        if !conversion::app_is_warm(&mut w.tx, &row.resource_id).await? {
            return Ok(Attempt::Drop);
        }
        if leases::fenced(&mut w.tx, &self.0.unrecorded_failures, &row).await?
            || !self.session_ready(&row.holder_node_id, generation)
        {
            return Ok(Attempt::Wait);
        }
        // Two attempts may read the same entry: only one acts, the one that
        // finds it still unsent on this session under the writer gate. What
        // the entry knows is read from the live entry, not from the copy
        // this attempt took.
        let live = {
            let registry = self.0.held.lock().expect("held grant lock");
            registry
                .entries
                .get(lease_id)
                .filter(|entry| {
                    entry.kind == Kind::Convert
                        && entry.registration == held.registration
                        && entry.names(&row)
                        && entry.eligible_on == Some(generation)
                        && entry.sent_on != Some(generation)
                })
                .map(|entry| (entry.barrier_on, entry.raised_seq))
        };
        let Some((barrier_on, raised_seq)) = live else {
            return Ok(Attempt::Wait);
        };
        if self
            .conversion_in_flight(&row.holder_node_id, generation, lease_id)
            .is_some()
        {
            return Ok(Attempt::Wait);
        }
        let facts = self
            .convert_facts(&mut w.tx, &row, generation, barrier_on, raised_seq)
            .await?;
        let token = lease_token(&row);
        let actor = Actor {
            kind: ActorType::Node,
            id: row.holder_node_id.clone(),
        };
        let full = i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX);
        let step = convert_step(facts);
        // The barrier and the raise are made only while the grant they
        // precede could go: a grant of `D` needs majority support (section
        // 8.8), as the same-token `local` grant that ends the renewals does.
        if matches!(step, ConvertStep::Barrier | ConvertStep::RaiseAndWait)
            && !self.0.authority.supports(DEFAULT_DURATION_MS)
        {
            return Ok(Attempt::Wait);
        }
        let duration_ms = match step {
            ConvertStep::Wait => return Ok(Attempt::Wait),
            ConvertStep::Barrier => {
                let seq = leases::newest_write_seq(&mut w.tx, lease_id).await?;
                // Section 8.5: a `bad_message` refusal gives the lease back
                // only on a session whose `hello` listed it `held` or
                // `uncertain` at the token.
                let regrant = self.hello_listed(&row.holder_node_id, generation, lease_id, token);
                let grant_id = new_msg_id();
                let ping_id = new_msg_id();
                let grant = ControlMessage::LeaseGrant(row.grant())
                    .to_frame(&grant_id, None)
                    .to_text();
                let ping = ControlMessage::Ping.to_frame(&ping_id, None).to_text();
                let queued = self.queue_local_end(
                    &row.holder_node_id,
                    generation,
                    &Barrier {
                        lease_id: lease_id.to_string(),
                        token,
                        seq,
                    },
                    &grant_id,
                    regrant,
                    true,
                    &ping_id,
                    &[grant, ping],
                );
                if queued {
                    self.change_conversion(lease_id, held.registration, |entry| {
                        entry.barrier_on = Some(generation);
                    });
                }
                drop(w);
                if queued {
                    tracing::info!(
                        lease_id,
                        node_id = %row.holder_node_id,
                        token,
                        "the same-token local grant and its ping went out before the conversion"
                    );
                }
                // The `pong` wakes the held grants ([`Session::on_barrier_pong`]).
                return Ok(Attempt::Wait);
            }
            ConvertStep::RaiseAndWait => {
                let raised = leases::raise_duration(&mut w, lease_id, full, &actor).await?;
                if raised.is_none() {
                    tracing::warn!(
                        lease_id,
                        duration_ms = ?row.duration_ms,
                        "the stored duration cannot be raised; the conversion waits"
                    );
                    return Ok(Attempt::Wait);
                }
                let seq = leases::newest_write_seq(&mut w.tx, lease_id).await?;
                w.commit().await?;
                if let Some(seq) = seq {
                    self.change_conversion(lease_id, held.registration, |entry| {
                        entry.raised_seq = Some(seq);
                    });
                }
                tracing::info!(
                    lease_id,
                    seq = ?seq,
                    "the stored duration is raised before the conversion; it waits for the raise \
                     to be replicated"
                );
                return Ok(Attempt::Wait);
            }
            ConvertStep::RaiseAndSend { duration_ms } => {
                if !self.0.authority.supports(duration_ms) {
                    return Ok(Attempt::Wait);
                }
                leases::raise_duration(&mut w, lease_id, full, &actor).await?;
                if !self.send_conversion(&row, generation, duration_ms) {
                    // Rolled back, the raise with it.
                    drop(w);
                    return Ok(Attempt::Wait);
                }
                // The grant carries the duration stored before the raise,
                // so the stored duration covers it whether or not this
                // commit lands (ADR 0254).
                if let Err(e) = w.commit().await {
                    tracing::warn!(
                        %e,
                        lease_id,
                        "the raise after a conversion grant did not commit; the grant carried \
                         the stored duration"
                    );
                }
                duration_ms
            }
            ConvertStep::Send { duration_ms } => {
                if !self.0.authority.supports(duration_ms) {
                    return Ok(Attempt::Wait);
                }
                let sent = self.send_conversion(&row, generation, duration_ms);
                // Nothing is written.
                drop(w);
                if !sent {
                    return Ok(Attempt::Wait);
                }
                duration_ms
            }
        };
        self.change_conversion(lease_id, held.registration, |entry| {
            entry.sent_on = Some(generation);
        });
        tracing::info!(
            lease_id,
            node_id = %row.holder_node_id,
            token,
            duration_ms,
            "the conversion grant went out"
        );
        Ok(Attempt::Sent)
    }

    /// Builds the conversion grant of `row` for `duration_ms` and queues and
    /// records it on `generation` ([`NodeHub::queue_conversion`]): the row's
    /// grant with `mode` `expiring`, the row's token, the duration, the
    /// authority's wall-clock time, and no `re` (ADR 0256). Returns whether
    /// it was queued and recorded.
    fn send_conversion(&self, row: &Lease, generation: u64, duration_ms: u64) -> bool {
        let token = lease_token(row);
        let mut grant = row.grant();
        grant.mode = quoro_proto::LeaseMode::Expiring;
        grant.fencing_token = token;
        grant.duration_ms = Some(duration_ms);
        grant.granted_at = crate::store::format_rfc3339(time::OffsetDateTime::now_utc());
        let grant_id = new_msg_id();
        let frame = ControlMessage::LeaseGrant(grant)
            .to_frame(&grant_id, None)
            .to_text();
        self.queue_conversion(
            &row.holder_node_id,
            generation,
            &row.id,
            InFlight { token, duration_ms },
            &grant_id,
            frame,
        )
    }

    /// Queues `frame`, the conversion grant `grant_id` of `lease_id`, on
    /// `generation`'s channel and records it on the session, in one hold of
    /// the sessions lock that finds `generation` still the node's session
    /// and ADR 0257's barrier passed for the lease at `in_flight`'s token
    /// under that same lock. Recorded only once the frame is queued: the
    /// conversion in flight, so its first renewal is answered as the first
    /// one ([`super::conversion`]); the end of the `local` grant's refusal of
    /// renewals ("From R …", Renewals: "until a conversion grant follows");
    /// and the grant's message id, for a refusal of it. Every reader of
    /// these takes this lock, so none sees the frame queued without the
    /// record. Returns whether it was queued and recorded.
    pub(super) fn queue_conversion(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        in_flight: InFlight,
        grant_id: &str,
        frame: String,
    ) -> bool {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        if !handle.local_end.barrier_passed(lease_id, in_flight.token) {
            tracing::warn!(
                lease_id,
                node_id,
                "no conversion grant before the barrier's pong (ADR 0257)"
            );
            return false;
        }
        if handle.tx.send(frame).is_err() {
            return false;
        }
        handle.conversions.insert(lease_id.to_string(), in_flight);
        handle.local_sent.remove(lease_id);
        handle
            .conversion_grants
            .insert(grant_id.to_string(), (lease_id.to_string(), in_flight));
        true
    }

    /// The conversion grant `re` that `generation` sent, if it is one, taken
    /// off the session: the lease and what it carried.
    pub(super) fn take_conversion_grant(
        &self,
        node_id: &str,
        generation: u64,
        re: &str,
    ) -> Option<(String, InFlight)> {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
            .and_then(|handle| handle.conversion_grants.remove(re))
    }

    /// The holder refused the conversion grant of `lease_id` that
    /// `generation` sent with `in_flight`: the conversion is no longer in
    /// flight on that session (a later one recorded since stays), and the
    /// lease's held conversion eligible on that session at that token is
    /// dropped, so no other goes out there. Nothing is written: the row
    /// still reads `local`, and the holder's next `hello` decides. The
    /// sessions lock and the registry lock are taken one after the other.
    pub(super) fn conversion_refused(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        in_flight: InFlight,
    ) {
        self.conversion_answered(node_id, generation, lease_id, in_flight);
        let dropped = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            let refused = registry.entries.get(lease_id).is_some_and(|entry| {
                entry.kind == Kind::Convert
                    && entry.holder == node_id
                    && entry.eligible_on == Some(generation)
                    && u64::try_from(entry.token).ok() == Some(in_flight.token)
            });
            if refused {
                registry.entries.remove(lease_id)
            } else {
                None
            }
        };
        if let Some(dropped) = dropped {
            self.release_held_revoke(&dropped);
        }
    }
}
