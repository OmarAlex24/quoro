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
//!   fence, the session ready, its start's grants queued, no conversion in
//!   flight on it, and majority support for the duration sent (for `D`
//!   before the barrier or the raise, which precede a grant of `D`). The
//!   grant carries the row's token, `mode` `expiring`, the duration and no
//!   `re` (ADR 0256). It is queued and recorded on the session in one hold
//!   of the sessions lock ([`NodeHub::queue_conversion`]), which checks the
//!   start's grants queued and the barrier again, so the session start's
//!   same-token `local` repeat never reaches the node after it: its first
//!   renewal is then answered as the first one, writing the row `expiring`
//!   ([`super::conversion`]). The send writes nothing but a raise; the
//!   conversion grant counts toward no gate (ADR 0241).
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
                .convert_facts(
                    &mut conn,
                    &row,
                    generation,
                    held.barrier_on,
                    held.raised_seq,
                )
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
        // Neither the conversion nor the barrier `local` grant before it is
        // queued until the session start's grants are, so the start's
        // same-token `local` repeat cannot follow it on the wire
        // ([`NodeHub::mark_start_grants_queued`]).
        if leases::fenced(&mut w.tx, &self.0.unrecorded_failures, &row).await?
            || !self.session_ready(&row.holder_node_id, generation)
            || !self.start_grants_queued(&row.holder_node_id, generation)
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
    /// the sessions lock that finds `generation` still the node's session,
    /// its start's grants queued ([`NodeHub::mark_start_grants_queued`]),
    /// and ADR 0257's barrier passed for the lease at `in_flight`'s token
    /// under that same lock. The channel goes out after the outbox, so the
    /// session start's same-token `local` repeat, queued on the outbox,
    /// cannot follow the conversion and end it. Recorded only once the
    /// frame is queued: the conversion in flight, so its first renewal is
    /// answered as the first one ([`super::conversion`]); the end of the
    /// `local` grant's refusal of renewals ("From R …", Renewals: "until a
    /// conversion grant follows"); and the grant's message id, for a refusal
    /// of it. Every reader of these takes this lock, so none sees the frame
    /// queued without the record. Returns whether it was queued and
    /// recorded.
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
        if !handle.start_grants_queued {
            tracing::debug!(
                lease_id,
                node_id,
                "no conversion grant before the session start's grants are queued"
            );
            return false;
        }
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

#[cfg(test)]
mod tests {
    use super::super::conversion::InFlight;
    use super::super::conversion::tests::{
        EverySupport, Fixture, LEASE, assert_answer, end_renewals, fixture, of_type, ping_of, pong,
        renew, restart, set_policy, write_row,
    };
    use super::super::held::{HeldGrant, Kind};
    use super::super::local_end::tests::{
        Live, NoSupport, ack, created, grant_then_ping, live, queued, r, refuse, view,
    };
    use super::{ConvertFacts, ConvertStep, convert_step};
    use crate::AppState;
    use crate::authority::DEFAULT_DURATION_MS;
    use quoro_proto::{ErrorCode, LeaseMode};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    const D: u64 = DEFAULT_DURATION_MS;

    /// The conversion grants among `frames`: `lease_grant`s of the lease
    /// with `mode` `expiring`.
    fn conversions(frames: &[Value]) -> Vec<&Value> {
        frames
            .iter()
            .filter(|frame| {
                frame["type"] == "lease_grant"
                    && frame["body"]["lease_id"] == LEASE
                    && frame["body"]["mode"] == "expiring"
            })
            .collect()
    }

    /// The `local` grants of the lease among `frames`.
    fn local_grants(frames: &[Value]) -> Vec<&Value> {
        frames
            .iter()
            .filter(|frame| {
                frame["type"] == "lease_grant"
                    && frame["body"]["lease_id"] == LEASE
                    && frame["body"]["mode"] == "local"
            })
            .collect()
    }

    /// The one conversion grant among `frames`: the row's `token`,
    /// `duration_ms`, and no `re`. Returns its message id. The hub's own
    /// held-grant task may attempt alongside the test's; one entry sends
    /// one conversion per session, so the count holds.
    fn the_conversion(frames: &[Value], token: u64, duration_ms: u64) -> String {
        let grants = conversions(frames);
        assert_eq!(grants.len(), 1, "{frames:?}");
        let grant = grants[0];
        assert!(grant["re"].is_null(), "{grant}");
        assert_eq!(grant["body"]["fencing_token"], token, "{grant}");
        assert_eq!(grant["body"]["duration_ms"], duration_ms, "{grant}");
        grant["id"].as_str().unwrap().to_string()
    }

    /// The held entry of the lease, if any.
    fn entry(state: &AppState) -> Option<HeldGrant> {
        state.hub.0.held.lock().unwrap().entries.get(LEASE).cloned()
    }

    /// The message ids of the conversion grants the node's session recorded.
    fn grant_ids(state: &AppState, node_id: &str) -> Vec<String> {
        state
            .hub
            .0
            .sessions
            .lock()
            .unwrap()
            .get(node_id)
            .map(|handle| handle.conversion_grants.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The `seq` of the lease's newest change-log write.
    async fn newest(f: &Fixture) -> u64 {
        let mut conn = f.state.store.pool().acquire().await.unwrap();
        crate::leases::newest_write_seq(&mut conn, LEASE)
            .await
            .unwrap()
            .unwrap()
    }

    /// The first renewal after the conversion at `token` is answered, the
    /// row reads `expiring`, and the gate counts `D`: `T_grant + D + 2 x
    /// 1.5 s` on the primary's monotonic clock.
    async fn first_answer_counts_d(f: &Fixture, on: &mut Live, token: u64) {
        let answer = renew(&mut on.session, "msg_first", token).await;
        assert_answer(&answer, "msg_first", token, D);
        assert_eq!(f.row().await.mode, "expiring");
        let authority = f.state.hub.authority();
        f.clock.advance(Duration::from_millis(17_999));
        assert!(!authority.passed(LEASE), "the gate counts D");
        f.clock.advance(Duration::from_millis(1));
        assert!(authority.passed(LEASE));
    }

    /// A live session whose `hello` listed the lease `local` at 3, over the
    /// fixture's row (with its creation entry), the conversion sent on it.
    async fn converted(f: &Fixture) -> Live {
        created(f).await;
        let mut on = live(&f.state, f, Some(("held", LeaseMode::Local, 3))).await;
        ack(&f.state, f, &on).await;
        f.state.hub.attempt_held().await;
        the_conversion(&queued(&mut on), 3, D);
        on
    }

    fn facts() -> ConvertFacts {
        ConvertFacts {
            marked: false,
            barrier_passed: true,
            barrier_sent_here: false,
            newest_replicated: true,
            own_raise_replicated: None,
            stored_ms: Some(D),
        }
    }

    /// Test 1: [`convert_step`] over every branch, in its order: the mark,
    /// the barrier, the stored duration, the entry's own raise, then the
    /// row's newest write.
    #[test]
    fn convert_step_decides_in_its_order() {
        let send = ConvertStep::Send { duration_ms: D };
        assert_eq!(convert_step(facts()), send);

        // A marked row waits, whatever else holds.
        for marked in [
            ConvertFacts {
                marked: true,
                ..facts()
            },
            ConvertFacts {
                marked: true,
                barrier_passed: false,
                stored_ms: None,
                ..facts()
            },
        ] {
            assert_eq!(convert_step(marked), ConvertStep::Wait, "{marked:?}");
        }

        // The barrier: sent once on a session, then waited on.
        for before in [
            ConvertFacts {
                barrier_passed: false,
                ..facts()
            },
            ConvertFacts {
                barrier_passed: false,
                stored_ms: None,
                newest_replicated: false,
                ..facts()
            },
        ] {
            assert_eq!(convert_step(before), ConvertStep::Barrier, "{before:?}");
        }
        assert_eq!(
            convert_step(ConvertFacts {
                barrier_passed: false,
                barrier_sent_here: true,
                ..facts()
            }),
            ConvertStep::Wait
        );

        // The stored duration.
        assert_eq!(
            convert_step(ConvertFacts {
                stored_ms: None,
                ..facts()
            }),
            ConvertStep::RaiseAndWait
        );
        for stored in [3_000, 5_000, D - 1] {
            assert_eq!(
                convert_step(ConvertFacts {
                    stored_ms: Some(stored),
                    newest_replicated: false,
                    ..facts()
                }),
                ConvertStep::RaiseAndSend {
                    duration_ms: stored
                },
                "stored {stored}"
            );
        }
        assert_eq!(
            convert_step(ConvertFacts {
                stored_ms: Some(30_000),
                ..facts()
            }),
            send
        );

        // The entry's own raise decides on its own.
        assert_eq!(
            convert_step(ConvertFacts {
                own_raise_replicated: Some(false),
                ..facts()
            }),
            ConvertStep::Wait
        );
        assert_eq!(
            convert_step(ConvertFacts {
                own_raise_replicated: Some(true),
                newest_replicated: false,
                ..facts()
            }),
            send
        );

        // A raise this entry did not write: the row's newest write.
        assert_eq!(
            convert_step(ConvertFacts {
                newest_replicated: false,
                ..facts()
            }),
            ConvertStep::Wait
        );
    }

    /// Test 2: no conversion goes while the row's newest write is not
    /// acknowledged by a machine of the required set, nor without majority
    /// support for `D`. Then it goes, `expiring` at the row's token for `D`
    /// with no `re`, recorded on the session in flight, with its message id,
    /// and the row is not written. Before the barrier passed, the hold of the
    /// sessions lock that queues it refuses it.
    #[tokio::test]
    async fn no_conversion_goes_before_its_conditions() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let writes = f.writes().await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let generation = on.session.generation;
        let held = entry(&f.state).expect("the hello's listing holds the conversion");
        assert_eq!(
            (held.kind, held.eligible_on, held.sent_on),
            (Kind::Convert, Some(generation), None)
        );

        hub.attempt_held().await;
        assert!(conversions(&queued(&mut on)).is_empty(), "not replicated");

        hub.set_voter_support(Arc::new(NoSupport));
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(conversions(&queued(&mut on)).is_empty(), "no support");
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );

        hub.set_voter_support(Arc::new(EverySupport));
        hub.attempt_held().await;
        let id = the_conversion(&queued(&mut on), 3, D);
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            Some(InFlight {
                token: 3,
                duration_ms: D
            })
        );
        assert_eq!(grant_ids(&f.state, &f.node_id), vec![id]);
        assert_eq!(entry(&f.state).unwrap().sent_on, Some(generation));
        assert_eq!(f.row().await.mode, "local", "the send writes nothing");
        assert_eq!(f.writes().await, writes);
        f.done();

        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, None).await;
        let generation = on.session.generation;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        let in_flight = InFlight {
            token: 3,
            duration_ms: D,
        };
        assert!(!hub.queue_conversion(
            &f.node_id,
            generation,
            LEASE,
            in_flight,
            "msg_before_the_barrier",
            "{}".to_string(),
        ));
        assert!(queued(&mut on).is_empty());
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        assert!(grant_ids(&f.state, &f.node_id).is_empty());
        f.done();
    }

    /// Test 3, the barrier on a live session: the session's `hello` did not
    /// list the lease (as when it was granted after the `hello`), so no
    /// barrier has passed. The entry sends the same-token `local` grant and
    /// its `ping` first, once; no conversion goes before the `pong`. After
    /// the `pong`, the conversion goes, with no second barrier grant. A
    /// refusal of the barrier grant before the `pong` passes no barrier:
    /// no conversion, and no second barrier grant on that session.
    #[tokio::test]
    async fn the_barrier_goes_first_on_a_session_that_has_not_shown_the_mode() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, None).await;
        let generation = on.session.generation;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        hub.hold_conversion(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        let (_grant, ping) = grant_then_ping(&queued(&mut on), 3);
        assert_eq!(entry(&f.state).unwrap().barrier_on, Some(generation));
        hub.attempt_held().await;
        assert!(
            queued(&mut on).is_empty(),
            "one barrier grant, and no conversion before its pong"
        );

        pong(&mut on.session, &ping).await;
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        hub.attempt_held().await;
        let sent = queued(&mut on);
        the_conversion(&sent, 3, D);
        assert!(local_grants(&sent).is_empty(), "no second barrier grant");
        assert_eq!(f.row().await.mode, "local");
        f.done();

        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, None).await;
        let generation = on.session.generation;
        hub.hold_conversion(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        let (grant, ping) = grant_then_ping(&queued(&mut on), 3);
        refuse(&mut on.session, &grant, ErrorCode::NotFound).await;
        pong(&mut on.session, &ping).await;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        hub.attempt_held().await;
        assert!(
            queued(&mut on).is_empty(),
            "no conversion, and no second barrier grant on this session"
        );
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        f.done();
    }

    /// Test 4, R after the conversion: a conversion in flight, then R, then
    /// the same-token `local` grant on the same session. The conversion is
    /// no longer in flight and its message id is gone; no renewal is
    /// answered after the grant, and the row is never written `expiring`.
    #[tokio::test]
    async fn the_local_grant_after_r_ends_a_conversion_in_flight() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let mut on = converted(&f).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        assert!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE)
                .is_some()
        );
        assert_eq!(grant_ids(&f.state, &f.node_id).len(), 1);

        r(&f).await;
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        assert!(grant_ids(&f.state, &f.node_id).is_empty());

        let writes = f.writes().await;
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert!(of_type(&answer, "lease_grant").is_empty(), "{answer:?}");
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// Test 5: a conversion whose frame is not queued, on a moved
    /// generation or a closed channel, records nothing: no conversion in
    /// flight, the session's same-token `local` grant still refuses
    /// renewals, no message id, and the held entry is not marked sent.
    #[tokio::test]
    async fn a_conversion_not_queued_records_nothing() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let Live { session, rx, .. } =
            live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let generation = session.generation;
        let local_sent = hub.local_grant_sent(&f.node_id, generation, LEASE);
        assert!(local_sent.is_some(), "the session start's local repeat");
        let in_flight = InFlight {
            token: 3,
            duration_ms: D,
        };
        assert!(!hub.queue_conversion(
            &f.node_id,
            generation + 1,
            LEASE,
            in_flight,
            "msg_moved",
            "{}".to_string(),
        ));
        drop(rx);
        assert!(!hub.queue_conversion(
            &f.node_id,
            generation,
            LEASE,
            in_flight,
            "msg_closed",
            "{}".to_string(),
        ));
        hub.count_log_ack(&f.node_id, generation, newest(&f).await);
        hub.attempt_held().await;
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        assert_eq!(
            hub.local_grant_sent(&f.node_id, generation, LEASE),
            local_sent
        );
        assert!(grant_ids(&f.state, &f.node_id).is_empty());
        assert_eq!(entry(&f.state).unwrap().sent_on, None);
        drop(session);
        f.done();
    }

    /// Test 6, the raise, from a session start: a row that reads `local`
    /// under `warm` and stores no duration has nothing shorter to send
    /// (ADR 0289). The conversion held from the `hello`'s listing raises
    /// `duration_ms` to `D` in its own entry and waits, with no second
    /// raise, until that raise is acknowledged; then it sends `D`. The first
    /// renewal is answered and the gate counts `D`.
    #[tokio::test]
    async fn a_row_with_no_duration_is_raised_then_converted_once_the_raise_is_replicated() {
        let f = fixture("warm", "local", None, false).await;
        created(&f).await;
        let writes = f.writes().await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        hub.attempt_held().await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.duration_ms),
            ("local", Some(D as i64))
        );
        assert_eq!(f.writes().await, writes + 1, "the raise, in its own entry");
        assert_eq!(entry(&f.state).unwrap().raised_seq, Some(newest(&f).await));
        assert!(conversions(&queued(&mut on)).is_empty(), "not replicated");
        hub.attempt_held().await;
        assert!(conversions(&queued(&mut on)).is_empty(), "not replicated");
        assert_eq!(f.writes().await, writes + 1, "raised once");

        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        the_conversion(&queued(&mut on), 3, D);
        first_answer_counts_d(&f, &mut on, 3).await;
        f.done();
    }

    /// Test 6, the raise, after a `local` grant with a new token (as a
    /// restore or `grant_app_lease` with `keep_local` writes it): the row
    /// reads `local` at 4 with the mark and no duration. The holder's
    /// `hello` lists it `local` at 4: the report clears the mark, then the
    /// conversion raises the duration, waits for the raise, and sends `D`
    /// at 4. The first renewal is answered and the gate counts `D`.
    #[tokio::test]
    async fn a_new_token_local_row_is_raised_then_converted() {
        let f = fixture("warm", "local", None, true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let writes = f.writes().await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 4))).await;
        hub.attempt_held().await;
        let row = f.row().await;
        assert_eq!(
            (
                row.mode.as_str(),
                row.fencing_token,
                row.unsettled,
                row.duration_ms
            ),
            ("local", 4, false, Some(D as i64))
        );
        assert_eq!(
            f.writes().await,
            writes + 2,
            "the report clears the mark, then the raise"
        );
        assert!(conversions(&queued(&mut on)).is_empty(), "not replicated");

        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        the_conversion(&queued(&mut on), 4, D);
        first_answer_counts_d(&f, &mut on, 4).await;
        f.done();
    }

    /// Test 7: a refused conversion. What its send recorded goes, the held
    /// entry is dropped, nothing is written, and nothing is sent again on
    /// that session. The next session's `hello`, listing the lease `local`,
    /// sends it again.
    #[tokio::test]
    async fn a_refused_conversion_goes_again_only_after_the_next_hello() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let mut on = converted(&f).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        let id = grant_ids(&f.state, &f.node_id).pop().unwrap();
        let writes = f.writes().await;
        refuse(&mut on.session, &id, ErrorCode::NotFound).await;
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        assert!(grant_ids(&f.state, &f.node_id).is_empty());
        assert!(entry(&f.state).is_none(), "the held conversion is dropped");
        assert_eq!(f.writes().await, writes, "nothing is written");
        assert_eq!(f.row().await.mode, "local");
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no retry on this session");

        let mut next = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        ack(&f.state, &f, &next).await;
        hub.attempt_held().await;
        the_conversion(&queued(&mut next), 3, D);
        f.done();
    }

    /// Test 8: a conversion that waits (here on its raise) is no wait: no
    /// `grant_pending`, nothing in `grant_waiting_on`, and `failoverApp`'s
    /// `in_progress` check finds nothing.
    #[tokio::test]
    async fn a_waiting_conversion_is_no_wait() {
        let f = fixture("warm", "local", None, false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let _on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        hub.attempt_held().await;
        assert_eq!(entry(&f.state).map(|held| held.kind), Some(Kind::Convert));
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), false));
        assert!(!hub.row_grant_pending(&f.row().await));
        assert!(!hub.grant_pending(LEASE));
        f.done();
    }

    /// Test 9: a `local` row under `warm` that carries the mark (R's, the
    /// policy back on `warm`) gets no conversion: neither the barrier nor
    /// the grant goes, and the entry waits. The next `hello`'s `local`
    /// report clears the mark, and the conversion goes.
    #[tokio::test]
    async fn a_marked_local_row_waits_for_its_mark_to_clear() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, None).await;
        end_renewals(&f).await;
        set_policy(&f, "warm").await;
        let row = f.row().await;
        assert!(row.unsettled);
        hub.hold_conversion(&row);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(
            queued(&mut on).is_empty(),
            "no barrier and no conversion over a marked row"
        );
        assert_eq!(entry(&f.state).map(|held| held.kind), Some(Kind::Convert));
        // The waiting conversion is no wait, on a marked row too: the row
        // shows `local_pending`, as any marked `local` row does, but no
        // `grant_pending` and nothing in `grant_waiting_on`.
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert!(!hub.row_grant_pending(&f.row().await));
        assert!(!hub.grant_pending(LEASE));
        drop(on);

        let mut next = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        assert!(!f.row().await.unsettled, "the local report clears the mark");
        ack(&f.state, &f, &next).await;
        hub.attempt_held().await;
        the_conversion(&queued(&mut next), 3, D);
        f.done();
    }

    /// Test 10: the policy leaves `warm` before the conversion went out:
    /// R's same-token `local` grant replaces the held conversion, and only
    /// that grant and its `ping` go out.
    #[tokio::test]
    async fn leaving_warm_before_the_send_replaces_the_conversion() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        assert_eq!(entry(&f.state).map(|held| held.kind), Some(Kind::Convert));
        r(&f).await;
        assert_eq!(entry(&f.state).map(|held| held.kind), Some(Kind::LocalEnd));
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        f.done();
    }

    /// Test 11: a holder with no session holds nothing; its next session
    /// start, whose `hello` lists the lease `local`, holds the conversion on
    /// that session, and it goes.
    #[tokio::test]
    async fn a_holder_away_gets_the_conversion_from_its_next_session_start() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        hub.hold_conversion(&f.row().await);
        assert!(entry(&f.state).is_none());

        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let held = entry(&f.state).unwrap();
        assert_eq!(
            (held.kind, held.eligible_on),
            (Kind::Convert, Some(on.session.generation))
        );
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        the_conversion(&queued(&mut on), 3, D);
        f.done();
    }

    /// Test 12, a session cut between the conversion and its first answer.
    /// The next `hello` lists the lease `expiring`: the lease is settled,
    /// and the row reads `expiring` only after the `pong`, with no second
    /// conversion. It lists it `local`: the conversion goes again. It lists
    /// it `expired`: the existing regrant gives it back at a new token, and
    /// no conversion goes at the old one.
    #[tokio::test]
    async fn a_session_cut_before_the_first_answer_is_decided_by_the_next_hello() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        drop(converted(&f).await);
        let mut next = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut next.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = ping_of(&answer);
        assert_eq!(f.row().await.mode, "local", "not before the pong");
        pong(&mut next.session, &ping).await;
        assert_eq!(f.row().await.mode, "expiring");
        f.state.hub.attempt_held().await;
        assert!(conversions(&queued(&mut next)).is_empty());
        f.done();

        let f = fixture("warm", "local", Some(D as i64), false).await;
        drop(converted(&f).await);
        let mut next = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        ack(&f.state, &f, &next).await;
        f.state.hub.attempt_held().await;
        the_conversion(&queued(&mut next), 3, D);
        f.done();

        let f = fixture("warm", "local", Some(D as i64), false).await;
        drop(converted(&f).await);
        let mut next = live(&f.state, &f, Some(("expired", LeaseMode::Local, 3))).await;
        assert!(f.row().await.fencing_token > 3, "{:?}", next.start);
        f.state.hub.attempt_held().await;
        assert!(
            conversions(&queued(&mut next))
                .iter()
                .all(|grant| grant["body"]["fencing_token"] != 3),
            "no conversion at the old token"
        );
        f.done();
    }

    /// Test 12, a primary restart between the raise and the send: the
    /// restarted primary knows nothing of the raise it did not write, and
    /// waits until the row's newest write, that raise, is acknowledged by
    /// the required set; it raises nothing again. Then the conversion goes.
    #[tokio::test]
    async fn after_a_restart_the_conversion_waits_for_the_newest_write() {
        let f = fixture("warm", "local", None, false).await;
        created(&f).await;
        let on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        f.state.hub.attempt_held().await;
        assert_eq!(f.row().await.duration_ms, Some(D as i64), "raised");
        let writes = f.writes().await;
        drop(on);

        let (after, _clock) = restart(&f).await;
        after.hub.set_voter_support(Arc::new(EverySupport));
        let mut next = live(&after, &f, Some(("held", LeaseMode::Local, 3))).await;
        after.hub.attempt_held().await;
        assert!(
            conversions(&queued(&mut next)).is_empty(),
            "the raise is not acknowledged on this primary"
        );
        assert_eq!(f.writes().await, writes, "no second raise");
        ack(&after, &f, &next).await;
        after.hub.attempt_held().await;
        the_conversion(&queued(&mut next), 3, D);
        f.done();
    }

    /// Test 13: no conversion, and no barrier `local` grant, is queued on a
    /// session before its start's grants are: the start's same-token
    /// `local` repeat goes on the outbox, which goes out before the
    /// session's channel, so a conversion queued first would reach the node
    /// before that repeat and be ended by it. Here every other condition
    /// holds (the `hello` listed the lease `local` at 3, the row stores `D`,
    /// its newest write is acknowledged, support); the conversion goes only
    /// once the start's grants are marked queued. On a session whose
    /// `hello` did not list the lease, the barrier grant waits the same way.
    #[tokio::test]
    async fn no_conversion_goes_before_the_session_starts_grants_are_queued() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let generation = on.session.generation;
        assert!(hub.start_grants_queued(&f.node_id, generation));
        hub.unmark_start_grants_queued(&f.node_id, generation);
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        ack(&f.state, &f, &on).await;
        hub.hold_conversion(&f.row().await);
        hub.attempt_held().await;
        assert!(
            queued(&mut on).is_empty(),
            "no conversion before the start's grants are queued"
        );
        assert_eq!(
            hub.conversion_in_flight(&f.node_id, generation, LEASE),
            None
        );
        assert!(grant_ids(&f.state, &f.node_id).is_empty());
        let held = entry(&f.state).unwrap();
        assert_eq!((held.kind, held.sent_on), (Kind::Convert, None));

        hub.mark_start_grants_queued(&f.node_id, generation);
        hub.attempt_held().await;
        the_conversion(&queued(&mut on), 3, D);
        f.done();

        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let hub = &f.state.hub;
        let mut on = live(&f.state, &f, None).await;
        let generation = on.session.generation;
        hub.unmark_start_grants_queued(&f.node_id, generation);
        hub.hold_conversion(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(
            queued(&mut on).is_empty(),
            "no barrier grant before the start's grants are queued"
        );
        assert_eq!(entry(&f.state).unwrap().barrier_on, None);

        hub.mark_start_grants_queued(&f.node_id, generation);
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        f.done();
    }
}
