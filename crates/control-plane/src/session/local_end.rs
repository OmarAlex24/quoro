// SPDX-License-Identifier: AGPL-3.0-only
//! The same-token `local` grant that ends an `expiring` lease's renewals,
//! its barrier `ping`, the `pong` after it, and its resend from `hello`
//! (`node-agent-protocol.md` section 8.4, "Ending the renewals" steps 2 and
//! 3, "Grants of a marked row", "The mark" and "The holder's next `hello`";
//! ADR 0255, ADR 0257, ADR 0261, ADR 0280; issue #555).
//!
//! - **Held as a marked row's grant.** R ([`leases::end_renewals`]) leaves
//!   the row `local` and marked. Its `local` grant is held in the held-grant
//!   registry ([`super::held`], [`Kind::LocalEnd`]), so `Lease.grant_pending`
//!   and `grant_waiting_on` show the wait as for any held grant. It goes out
//!   once the row's newest write is replicated over the required set and a
//!   majority supports `D`. Under the writer gate the send finds again: the
//!   session current and ready, the row naming the holder at the held token,
//!   unrevoked, marked and reading `local`, the policy off `warm`, and no
//!   failed fence or pending acknowledgement. It checks no gate and drops
//!   none: the holder keeps its lease and its token.
//! - **Evidence.** It goes out only on a session with evidence that the
//!   holder holds the lease `expiring` at the row's token: its `hello`
//!   listed it so (`held` or `uncertain`), or it renewed at that token on
//!   the session. A same-token `local` grant to a node whose highest token
//!   is lower would be taken as a new-token `local` grant (section 5.12),
//!   which the contract never sends without its own conditions. A holder
//!   without that evidence holds no `expiring` lease at that token, so there
//!   is nothing to end.
//! - **The barrier.** The grant and a `ping` are queued together on the
//!   session. The session then answers no renewal of the lease at that token
//!   (ADR 0256; [`super::conversion`]), and no settle `ping` of the lease
//!   settles any more. The `pong` of that `ping` with no refusal before it
//!   writes the row `local` at the token and clears the mark, as a `local`
//!   report does, if the row's newest write is still the one recorded with
//!   the `ping` and every other check holds again. It also passes ADR 0257's
//!   barrier for the lease on that session: a conversion grant may follow
//!   ([`NodeHub::note_conversion_sent`]). A barrier that passed on the
//!   session before this grant no longer counts. The session-start `local`
//!   repeat ([`Session::send_grants`]) queues the same `ping`; there a `hello`
//!   that listed the lease `local` at the token stands in for its `pong`.
//! - **The refusal.** A refusal of the grant arrives before the `pong`
//!   (section 4.2). It drops the `ping`, so the `pong` writes nothing and
//!   passes no barrier. A `bad_message` refusal runs the regrant of "The
//!   holder fenced first" ([`Session::regrant`]).
//! - **The resend.** A `hello` that lists the lease `expiring` (`held` or
//!   `uncertain`) at the row's token or above, over a marked row that reads
//!   `local`, while the policy is off `warm`, registers the grant again on
//!   that session. A holder token above the row's is first written into the
//!   row, `local`, keeping the mark. A restarted primary does the same at
//!   the holder's next session.

use super::held::{Attempt, HeldGrant, Kind};
use super::{ControlMessage, NodeHub, Session, lease_token, new_msg_id};
use crate::authority::DEFAULT_DURATION_MS;
use crate::changelog::{Actor, ActorType};
use crate::error::ApiError;
use crate::leases::{self, Lease};
use quoro_proto::wire;
use std::collections::HashMap;

/// What one session knows of the `local` grants that end renewals.
#[derive(Debug, Default)]
pub(super) struct SessionState {
    /// Each same-token `local` grant sent on the session whose barrier
    /// `ping` is not answered yet, by message id: the lease, the token, its
    /// barrier `ping`'s id, and whether a `bad_message` refusal of it gives
    /// the lease back.
    grants: HashMap<String, (String, u64, String, bool)>,
    /// Each barrier `ping` not answered yet, by message id.
    pings: HashMap<String, Barrier>,
    /// The token at which each lease's barrier passed on the session.
    passed: HashMap<String, u64>,
    /// The token at which the session's `hello` listed each lease `local`,
    /// `held` or `uncertain`: it stands in for the barrier's `pong` (ADR
    /// 0261).
    listed_local: HashMap<String, u64>,
    /// The token at which the session has evidence that the holder holds
    /// each lease `expiring`: its `hello` listing, or a renewal.
    expiring: HashMap<String, u64>,
}

/// What a barrier `ping` stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Barrier {
    pub lease_id: String,
    pub token: u64,
    /// The row's newest change-log write when the grant was queued.
    pub seq: Option<u64>,
}

impl NodeHub {
    /// Records what `generation`'s `hello` listed: the leases it holds
    /// `local` and those it holds `expiring` (`held` or `uncertain`).
    pub(super) fn note_hello_listing(
        &self,
        node_id: &str,
        generation: u64,
        leases: &[wire::HelloLease],
    ) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return;
        };
        for lease in leases {
            if lease.state != "held" && lease.state != "uncertain" {
                continue;
            }
            let map = match lease.mode {
                quoro_proto::LeaseMode::Local => &mut handle.local_end.listed_local,
                quoro_proto::LeaseMode::Expiring => &mut handle.local_end.expiring,
            };
            map.insert(lease.lease_id.clone(), lease.fencing_token);
        }
    }

    /// A renewal of `lease_id` at `token`, the row's, arrived on
    /// `generation`: evidence that the holder holds the lease `expiring`
    /// there. A held `local` grant of the row waiting for such a session may
    /// go out on it.
    pub(super) fn saw_expiring(&self, node_id: &str, generation: u64, lease_id: &str, token: u64) {
        {
            let mut sessions = self.0.sessions.lock().expect("session lock");
            let Some(handle) = sessions
                .get_mut(node_id)
                .filter(|handle| handle.generation == generation)
            else {
                return;
            };
            handle
                .local_end
                .expiring
                .insert(lease_id.to_string(), token);
        }
        let woke = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            registry
                .entries
                .get_mut(lease_id)
                .filter(|held| {
                    held.kind == Kind::LocalEnd
                        && held.holder == node_id
                        && u64::try_from(held.token).ok() == Some(token)
                        && held.eligible_on.is_none()
                })
                .is_some_and(|held| {
                    held.eligible_on = Some(generation);
                    held.sent_on = None;
                    true
                })
        };
        if woke {
            self.wake_held();
        }
    }

    /// Holds the `local` grant that ends the renewals of `lease`, the row R
    /// left `local` and marked, as `updateApp` commits it. It may go out on
    /// the holder's current session when that session has evidence that the
    /// holder holds the lease `expiring` at the row's token; otherwise it
    /// waits for such evidence: a renewal, or the holder's next `hello`.
    pub(crate) fn hold_local_end(&self, lease: &Lease) {
        let token = u64::try_from(lease.fencing_token).unwrap_or(0);
        let evidenced = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(&lease.holder_node_id)
                .filter(|handle| {
                    handle.hello_recorded
                        && handle.local_end.expiring.get(&lease.id) == Some(&token)
                })
                .map(|handle| handle.generation)
        };
        self.register_local_end(lease, evidenced);
    }

    /// The resend from `hello`: the grant may go out on `generation`, whose
    /// `hello` listed the lease `expiring` at the row's token.
    pub(super) fn hold_local_end_on(&self, lease: &Lease, generation: u64) {
        self.register_local_end(lease, Some(generation));
    }

    fn register_local_end(&self, lease: &Lease, eligible: Option<u64>) {
        let replaced = {
            let mut registry = self.0.held.lock().expect("held grant lock");
            match registry.entries.get_mut(&lease.id) {
                Some(held) if held.kind == Kind::LocalEnd && held.names(lease) => {
                    if let Some(generation) = eligible
                        && held.eligible_on != Some(generation)
                    {
                        held.eligible_on = Some(generation);
                        held.sent_on = None;
                    }
                    None
                }
                // A grant held since, for a newer token, stays.
                Some(held) if held.token > lease.fencing_token => None,
                _ => {
                    let mut held = HeldGrant::of(lease, eligible);
                    held.kind = Kind::LocalEnd;
                    held.support_ms = DEFAULT_DURATION_MS;
                    registry.entries.insert(lease.id.clone(), held)
                }
            }
        };
        if let Some(replaced) = replaced {
            self.release_held_revoke(&replaced);
        }
        tracing::info!(
            lease_id = %lease.id,
            holder = %lease.holder_node_id,
            token = lease.fencing_token,
            eligible_on = ?eligible,
            "the local grant that ends the renewals is held until R is replicated and supported"
        );
        self.wake_held();
    }

    /// One attempt at the `local` grant of `held` on `generation` ([module
    /// docs](self)): a read first, outside the writer gate, then every
    /// condition again under it, where the grant and its `ping` are queued.
    pub(super) async fn try_send_local_end(
        &self,
        lease_id: &str,
        held: &HeldGrant,
        generation: u64,
    ) -> Result<Attempt, ApiError> {
        let still = |row: &Lease| {
            held.names(row) && !row.is_revoked() && row.unsettled && row.mode == "local"
        };
        {
            let mut conn = self.0.store.pool().acquire().await?;
            let Some(row) = leases::by_id(&mut conn, lease_id).await? else {
                return Ok(Attempt::Drop);
            };
            if !still(&row) {
                return Ok(Attempt::Drop);
            }
            if !self.0.authority.supports(DEFAULT_DURATION_MS) {
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
        if !still(&row) {
            return Ok(Attempt::Drop);
        }
        // "Ending the renewals" step 2: "while the policy is still off
        // `warm`". Back on `warm`, the lease is settled instead ("While R
        // waits"); a later change off `warm` holds the grant again.
        if super::conversion::app_is_warm(&mut w.tx, &row.resource_id).await? {
            return Ok(Attempt::Drop);
        }
        if leases::fenced(&mut w.tx, &self.0.unrecorded_failures, &row).await? {
            return Ok(Attempt::Wait);
        }
        let Some(seq) = leases::newest_write_seq(&mut w.tx, lease_id).await? else {
            return Ok(Attempt::Drop);
        };
        if self
            .replicated(&mut w.tx, seq)
            .await
            .map_err(|e| ApiError::internal(&e))?
            .is_err()
            || !self.session_ready(&row.holder_node_id, generation)
            || !self.0.authority.supports(DEFAULT_DURATION_MS)
        {
            return Ok(Attempt::Wait);
        }
        // Two attempts may read the same entry: only one sends, the one that
        // finds it still unsent on this session under the writer gate.
        let unsent = self
            .0
            .held
            .lock()
            .expect("held grant lock")
            .entries
            .get(lease_id)
            .is_some_and(|entry| {
                entry.kind == Kind::LocalEnd
                    && entry.names(&row)
                    && entry.eligible_on == Some(generation)
                    && entry.sent_on != Some(generation)
            });
        if !unsent {
            return Ok(Attempt::Wait);
        }
        let token = lease_token(&row);
        let grant_id = new_msg_id();
        let ping_id = new_msg_id();
        let grant = ControlMessage::LeaseGrant(row.grant())
            .to_frame(&grant_id, None)
            .to_text();
        let ping = ControlMessage::Ping.to_frame(&ping_id, None).to_text();
        // Queued under the writer gate, the grant then its `ping`, with the
        // session's state recorded before either can be answered.
        let sent = self.queue_local_end(
            &row.holder_node_id,
            generation,
            &Barrier {
                lease_id: lease_id.to_string(),
                token,
                seq: Some(seq),
            },
            &grant_id,
            true,
            true,
            &ping_id,
            &[grant, ping],
        );
        if sent {
            let mut registry = self.0.held.lock().expect("held grant lock");
            if let Some(entry) = registry
                .entries
                .get_mut(lease_id)
                .filter(|entry| entry.kind == Kind::LocalEnd && entry.names(&row))
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
            token,
            "the local grant that ends the renewals went out, with its ping"
        );
        Ok(Attempt::Sent)
    }

    /// Records on `generation` the same-token `local` grant `grant_id` of
    /// `barrier`'s lease and its barrier `ping` (ADR 0256: no renewal of the
    /// lease at that token is answered on the session from now on), then
    /// queues `frames` on the session's channel, unless empty (the caller
    /// queues them on its own outbox). `regrant` says whether a
    /// `bad_message` refusal of the grant gives the lease back; `fresh`,
    /// whether the grant is sent after the session start, so a barrier that
    /// passed before it no longer counts. Returns whether `generation` is
    /// the node's session.
    #[expect(clippy::too_many_arguments, reason = "one record of one send")]
    pub(super) fn queue_local_end(
        &self,
        node_id: &str,
        generation: u64,
        barrier: &Barrier,
        grant_id: &str,
        regrant: bool,
        fresh: bool,
        ping_id: &str,
        frames: &[String],
    ) -> bool {
        if !self.note_local_grant_sent(node_id, generation, &barrier.lease_id, barrier.token) {
            return false;
        }
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        if fresh {
            // A grant sent after the session start has its own barrier: one
            // that passed before it, by a `pong` or the `hello`'s listing,
            // no longer counts. The session start's repeat keeps the
            // `hello`'s: the node held the lease `local` when it sent it.
            handle.local_end.passed.remove(&barrier.lease_id);
            handle.local_end.listed_local.remove(&barrier.lease_id);
        }
        handle.local_end.grants.insert(
            grant_id.to_string(),
            (
                barrier.lease_id.clone(),
                barrier.token,
                ping_id.to_string(),
                regrant,
            ),
        );
        handle
            .local_end
            .pings
            .insert(ping_id.to_string(), barrier.clone());
        frames
            .iter()
            .all(|frame| handle.tx.send(frame.clone()).is_ok())
    }

    /// A refusal answered the `local` grant `re` on `generation`: its
    /// barrier `ping` is dropped, so its `pong` writes nothing ("The mark":
    /// "When the `pong` arrives with no refusal before it"). Returns the
    /// lease and token the grant carried, and whether a `bad_message`
    /// refusal of it gives the lease back.
    pub(super) fn take_local_grant(
        &self,
        node_id: &str,
        generation: u64,
        re: &str,
    ) -> Option<(String, u64, bool)> {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let handle = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)?;
        let (lease_id, token, ping_id, regrant) = handle.local_end.grants.remove(re)?;
        handle.local_end.pings.remove(&ping_id);
        Some((lease_id, token, regrant))
    }

    /// The barrier `ping` `re` answered on `generation`, if it is one.
    fn take_barrier(&self, node_id: &str, generation: u64, re: &str) -> Option<Barrier> {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let handle = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)?;
        let barrier = handle.local_end.pings.remove(re)?;
        handle
            .local_end
            .grants
            .retain(|_, (_, _, ping_id, _)| ping_id != re);
        handle
            .local_end
            .passed
            .insert(barrier.lease_id.clone(), barrier.token);
        Some(barrier)
    }

    /// Whether ADR 0257's barrier has passed for `lease_id` at `token` on
    /// `generation`: the `pong` of the `ping` after its same-token `local`
    /// grant arrived with no refusal before it, or the session's `hello`
    /// listed the lease `local` at that token, which stands in for it (ADR
    /// 0261). A conversion grant of the lease may go out only then.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "nothing sends a conversion grant yet (#555)")
    )]
    pub(crate) fn barrier_passed(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        token: u64,
    ) -> bool {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .is_some_and(|handle| {
                handle.local_end.passed.get(lease_id) == Some(&token)
                    || handle.local_end.listed_local.get(lease_id) == Some(&token)
            })
    }
}

impl Session {
    /// The resend from `hello` ([module docs](self)): `lease`, a marked row
    /// that reads `local`, is listed `expiring` at `claimed`, at its token
    /// or above, while the policy is off `warm`. A holder token above the
    /// row's is first written into the row, `local`, keeping the mark; then
    /// the grant is held on this session.
    pub(super) async fn resend_local_end(&self, lease: &Lease, claimed: u64) {
        let lease = if claimed > lease_token(lease) {
            match self.write_holder_token(&lease.id, claimed).await {
                Ok(Some(lease)) => {
                    // Section 6.4: the desired state carries the token.
                    self.hub.node_changed(&self.node_id);
                    lease
                }
                Ok(None) => return,
                Err(e) => {
                    tracing::warn!(code = %e.code.as_str(), lease_id = %lease.id, "cannot write the holder's token before the local grant's resend");
                    return;
                }
            }
        } else {
            lease.clone()
        };
        self.hub.hold_local_end_on(&lease, self.generation);
    }

    /// "The holder's next `hello`", third bullet: the row written `local` at
    /// the holder's `token`, keeping the mark, in one single-writer
    /// transaction that finds again this session current, the row naming
    /// the node, unrevoked, marked, `local`, below `token`, and no failed
    /// fence.
    async fn write_holder_token(
        &self,
        lease_id: &str,
        token: u64,
    ) -> Result<Option<Lease>, ApiError> {
        let mut w = self.hub.0.store.begin_write().await?;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(None);
        }
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(None);
        };
        if row.holder_node_id != self.node_id
            || row.is_revoked()
            || !row.unsettled
            || row.mode != "local"
            || lease_token(&row) >= token
            || crate::fences::has_failed_fence(&mut w.tx, &self.hub.0.unrecorded_failures, lease_id)
                .await?
        {
            return Ok(None);
        }
        let Some(token) = i64::try_from(token)
            .ok()
            .filter(|token| *token <= leases::MAX_TOKEN)
        else {
            return Ok(None);
        };
        let actor = Actor {
            kind: ActorType::Node,
            id: self.node_id.clone(),
        };
        let lease = leases::write_marked_local(&mut w.tx, row, token, &actor).await?;
        w.commit().await?;
        Ok(Some(lease))
    }

    /// Queues, after the same-token `local` grant `grant_id` of `lease_id`
    /// at `token` already on `outbox`, its barrier `ping`, and records both
    /// on the session (ADR 0257). `regrant` says whether a refusal of the
    /// grant gives the lease back.
    pub(super) async fn queue_barrier(
        &self,
        lease_id: &str,
        token: u64,
        grant_id: &str,
        regrant: bool,
        outbox: &mut Vec<String>,
    ) {
        let seq = match self.hub.0.store.pool().acquire().await {
            Ok(mut conn) => leases::newest_write_seq(&mut conn, lease_id)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!(code = %e.code.as_str(), lease_id, "cannot read a lease's newest write");
                    None
                }),
            Err(e) => {
                tracing::warn!(%e, lease_id, "cannot read a lease's newest write");
                None
            }
        };
        let ping_id = new_msg_id();
        self.hub.queue_local_end(
            &self.node_id,
            self.generation,
            &Barrier {
                lease_id: lease_id.to_string(),
                token,
                seq,
            },
            grant_id,
            regrant,
            false,
            &ping_id,
            &[],
        );
        outbox.push(ControlMessage::Ping.to_frame(&ping_id, None).to_text());
    }

    /// A `pong` that is no settle's: when it answers a barrier `ping` with no
    /// refusal of its grant before it, the barrier passes, the session's
    /// settle state of the lease is dropped, and the row is written `local`
    /// at the token with the mark cleared ("The mark", the `pong` bullet), if
    /// every check holds again ([`Session::write_pong`]).
    pub(super) async fn on_barrier_pong(&mut self, re: &str) {
        let Some(barrier) = self.hub.take_barrier(&self.node_id, self.generation, re) else {
            return;
        };
        self.settles.forget(&barrier.lease_id);
        match self.write_pong(&barrier).await {
            Ok(Some(lease)) => {
                tracing::info!(
                    lease_id = %lease.id,
                    node_id = %self.node_id,
                    token = lease.fencing_token,
                    "the holder took the local grant: its row reads local, unmarked"
                );
                self.hub.node_changed(&self.node_id);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(code = %e.code.as_str(), lease_id = %barrier.lease_id, "cannot record the pong after a local grant");
            }
        }
    }

    /// The `pong`'s write, in one single-writer transaction that finds
    /// again: this session current; the row naming the node at the barrier's
    /// token, unrevoked; its newest change-log write the one recorded with
    /// the `ping`; and no failed fence. A row that already reads `local`,
    /// unmarked, is not written.
    async fn write_pong(&self, barrier: &Barrier) -> Result<Option<Lease>, ApiError> {
        let lease_id = barrier.lease_id.as_str();
        let mut w = self.hub.0.store.begin_write().await?;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(None);
        }
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(None);
        };
        if row.holder_node_id != self.node_id
            || row.is_revoked()
            || lease_token(&row) != barrier.token
            || (row.mode == "local" && !row.unsettled)
        {
            return Ok(None);
        }
        if leases::newest_write_seq(&mut w.tx, lease_id).await? != barrier.seq {
            tracing::info!(
                lease_id,
                "the row changed since its local grant; the pong writes nothing"
            );
            return Ok(None);
        }
        if crate::fences::has_failed_fence(&mut w.tx, &self.hub.0.unrecorded_failures, lease_id)
            .await?
        {
            return Ok(None);
        }
        let token = row.fencing_token;
        let actor = Actor {
            kind: ActorType::Node,
            id: self.node_id.clone(),
        };
        let lease = leases::write_local_report(&mut w.tx, row, token, &actor).await?;
        w.commit().await?;
        Ok(Some(lease))
    }
}

#[cfg(test)]
mod tests {
    use super::super::conversion::tests::{
        EverySupport, Fixture, LEASE, assert_answer, end_renewals, fixture, frames, hello, of_type,
        ping_of, pong, renew, restart, set_policy, write_row,
    };
    use crate::AppState;
    use crate::api::leases::Lease as LeaseView;
    use crate::authority::VoterSupport;
    use crate::session::Session;
    use quoro_proto::wire::NodeMessage;
    use quoro_proto::{ErrorCode, LeaseMode, ProtocolError};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    const D: u64 = 15_000;

    #[derive(Debug)]
    struct NoSupport;
    impl VoterSupport for NoSupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            false
        }
    }

    /// A session of the fixture's node on `state` whose `hello` lists the
    /// lease as `listed`, past its start, keeping the channel the hub queues
    /// on.
    struct Live {
        session: Session,
        rx: mpsc::UnboundedReceiver<String>,
    }

    async fn live(state: &AppState, f: &Fixture, listed: Option<(&str, LeaseMode, u64)>) -> Live {
        let hub = &state.hub;
        let (generation, rx, _closed, _changed) = hub.attach(&f.node_id);
        let mut session = Session::new(hub.clone(), f.node_id.clone(), generation);
        let mut outbox = Vec::new();
        let text = NodeMessage::Hello(hello(&f.node_id, listed))
            .to_frame("msg_hello", None)
            .to_text();
        assert!(!session.handle_text(&text, &mut outbox).await);
        hub.mark_start_done(&f.node_id, generation);
        Live { session, rx }
    }

    /// The grants and `ping`s the hub queued on the session's channel.
    fn queued(live: &mut Live) -> Vec<Value> {
        let mut texts = Vec::new();
        while let Ok(text) = live.rx.try_recv() {
            texts.push(text);
        }
        frames(texts)
            .into_iter()
            .filter(|frame| frame["type"] == "lease_grant" || frame["type"] == "ping")
            .collect()
    }

    /// The row's creation entry, as every row written by the primary has
    /// one: the fixture inserts its row with none.
    async fn created(f: &Fixture) {
        write_row(
            f,
            "UPDATE leases SET fencing_token = fencing_token WHERE id = ?",
        )
        .await;
    }

    /// The node acknowledges the lease's newest write on `live`.
    async fn ack(state: &AppState, f: &Fixture, live: &Live) {
        let mut conn = state.store.pool().acquire().await.unwrap();
        let seq = crate::leases::newest_write_seq(&mut conn, LEASE)
            .await
            .unwrap()
            .unwrap();
        state
            .hub
            .count_log_ack(&f.node_id, live.session.generation, seq);
    }

    /// R as `updateApp` commits it, then the hold it registers.
    async fn r(f: &Fixture) {
        if f.writes().await == 0 {
            created(f).await;
        }
        end_renewals(f).await;
        f.state.hub.hold_local_end(&f.row().await);
    }

    async fn view(state: &AppState, f: &Fixture) -> (bool, Vec<String>, bool) {
        let row = f.row().await;
        let mut conn = state.store.pool().acquire().await.unwrap();
        let (pending, waiting) = state.hub.grant_view(&mut conn, &row).await.unwrap();
        let local_pending = LeaseView::from_row(row, false, Vec::new())
            .unwrap()
            .local_pending;
        (pending, waiting, local_pending)
    }

    /// The `local` grant at `token` and its `ping`, in that order, as the
    /// only frames: their ids.
    fn grant_then_ping(frames: &[Value], token: u64) -> (String, String) {
        assert_eq!(frames.len(), 2, "{frames:?}");
        let grant = &frames[0];
        assert_eq!(grant["type"], "lease_grant", "{frames:?}");
        assert!(grant["re"].is_null(), "{grant}");
        assert_eq!(grant["body"]["lease_id"], LEASE);
        assert_eq!(grant["body"]["mode"], "local");
        assert_eq!(grant["body"]["fencing_token"], token);
        assert!(grant["body"].get("duration_ms").is_none(), "{grant}");
        assert_eq!(frames[1]["type"], "ping", "{frames:?}");
        (
            grant["id"].as_str().unwrap().to_string(),
            frames[1]["id"].as_str().unwrap().to_string(),
        )
    }

    async fn state(f: &Fixture) -> (String, i64, Option<i64>, bool, bool) {
        let row = f.row().await;
        let revoked = row.is_revoked();
        (
            row.mode,
            row.fencing_token,
            row.duration_ms,
            row.unsettled,
            revoked,
        )
    }

    /// "Ending the renewals" steps 2 and 3, "Grants of a marked row", "The
    /// mark" (the `pong`), ADR 0256 and ADR 0257. After R, the `local` grant
    /// waits while the node has not acknowledged R (`grant_pending`, waiting
    /// on the node), and while no majority supports `D` (waiting on no
    /// machine). Then the grant goes out, `local` at the row's token with no
    /// duration, followed by its `ping`. No renewal is answered on that
    /// session after it, a crossing one included, and no conversion grant
    /// may be recorded before the `pong`. The `pong` writes the row `local`,
    /// unmarked, in one entry: `local_pending` turns false, the barrier has
    /// passed, and a conversion may be recorded.
    #[tokio::test]
    async fn the_local_grant_goes_out_once_r_is_replicated_and_supported_then_its_pong_clears_the_mark()
     {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        let hub = &f.state.hub;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "R is not acknowledged");
        assert_eq!(
            view(&f.state, &f).await,
            (true, vec![f.node_id.clone()], true)
        );

        ack(&f.state, &f, &on).await;
        hub.set_voter_support(Arc::new(NoSupport));
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no majority support");
        assert_eq!(view(&f.state, &f).await, (true, Vec::new(), true));
        // The renewals are still answered "From R", with no write; none
        // without support.
        assert!(of_type(&renew(&mut on.session, "msg_r0", 3).await, "lease_grant").is_empty());
        hub.set_voter_support(Arc::new(EverySupport));
        assert_answer(&renew(&mut on.session, "msg_r1", 3).await, "msg_r1", 3, D);

        hub.attempt_held().await;
        let (_grant, ping) = grant_then_ping(&queued(&mut on), 3);
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "sent once");
        assert!(
            of_type(&renew(&mut on.session, "msg_r2", 3).await, "lease_grant").is_empty(),
            "a renewal that crossed the grant gets no answer"
        );
        let generation = on.session.generation;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(
            !hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D),
            "no conversion before the barrier's pong"
        );
        let writes = f.writes().await;

        pong(&mut on.session, &ping).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        assert_eq!(f.writes().await, writes + 1);
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), false));
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D));
        f.done();
    }

    /// "While R waits" and "Ending the renewals" step 2 ("while the policy
    /// is still off `warm`"). R is replicated but no majority supports `D`,
    /// and the policy comes back to `warm`: once support returns, the held
    /// grant is dropped, not sent (`grant_pending` false, `local_pending`
    /// still true), and the next renewal settles the lease. A new change off
    /// `warm` writes R again and holds the grant again, which then goes out.
    #[tokio::test]
    async fn a_return_to_warm_drops_the_held_local_grant_and_a_new_r_holds_it_again() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        r(&f).await;
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no majority support");
        assert_eq!(view(&f.state, &f).await, (true, Vec::new(), true));

        set_policy(&f, "warm").await;
        hub.set_voter_support(Arc::new(EverySupport));
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "back on warm: no local grant");
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "dropped, not waiting");

        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        pong(&mut on.session, &ping_of(&answer)).await;
        assert_eq!(
            state(&f).await,
            ("expiring".to_string(), 3, Some(D as i64), false, false)
        );
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), false));

        r(&f).await;
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        f.done();
    }

    /// Up to the `local` grant and its `ping` on a session listing the
    /// lease `expiring` at token 3, after R: the fixture, the session and
    /// the grant's and the `ping`'s ids.
    async fn sent(policy: &str, marked: bool) -> (Fixture, Live, String, String) {
        let f = fixture(policy, "expiring", Some(D as i64), marked).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        let (grant, ping) = grant_then_ping(&queued(&mut on), 3);
        (f, on, grant, ping)
    }

    /// The `pong` writes nothing after a `bad_message` refusal of the grant
    /// ("The holder fenced first"), which runs the regrant: the row moves to
    /// a token above both, `local` as the policy wants, keeping the mark, and
    /// its grant is held. Nor after a write of the row since the grant, nor
    /// on a session a newer one replaced.
    #[tokio::test]
    async fn the_pong_writes_nothing_after_a_refusal_a_write_or_a_new_session() {
        let (f, mut on, grant, ping) = sent("warm", false).await;
        let refusal = NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "the lease is not held".to_string(),
            fatal: false,
            term: None,
        })
        .to_frame("msg_refusal", Some(&grant))
        .to_text();
        let mut outbox = Vec::new();
        on.session.handle_text(&refusal, &mut outbox).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 4, None, true, false),
            "the regrant"
        );
        assert!(f.state.hub.grant_pending(LEASE), "its grant is held");
        let writes = f.writes().await;
        pong(&mut on.session, &ping).await;
        assert_eq!(state(&f).await, ("local".to_string(), 4, None, true, false));
        assert_eq!(f.writes().await, writes);
        assert!(
            !f.state
                .hub
                .barrier_passed(&f.node_id, on.session.generation, LEASE, 3)
        );
        f.done();

        let (f, mut on, _grant, ping) = sent("warm", false).await;
        write_row(&f, "UPDATE leases SET duration_ms = 20000 WHERE id = ?").await;
        let writes = f.writes().await;
        pong(&mut on.session, &ping).await;
        assert!(f.row().await.unsettled, "written since the grant");
        assert_eq!(f.writes().await, writes);
        f.done();

        let (f, mut on, _grant, ping) = sent("warm", false).await;
        let _newer = live(&f.state, &f, None).await;
        let writes = f.writes().await;
        pong(&mut on.session, &ping).await;
        assert!(f.row().await.unsettled, "a replaced session");
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// A settle `ping` sent before the `local` grant
    /// never settles the lease once the session sent that grant, even after
    /// the policy returned to `warm` and with no write of the row between
    /// them. Back on `warm` after R, a renewal plans a settle and sends its
    /// `ping`; the policy leaves `warm` again (R is written already, so
    /// nothing is) and the grant goes out; back on `warm`, the settle's
    /// `pong` writes nothing, and no renewal is answered.
    #[tokio::test]
    async fn a_settle_ping_from_before_the_local_grant_never_settles_after_it() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        set_policy(&f, "warm").await;
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let settle_ping = ping_of(&answer);
        set_policy(&f, "none").await;
        f.state.hub.hold_local_end(&f.row().await);
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        set_policy(&f, "warm").await;
        let writes = f.writes().await;
        pong(&mut on.session, &settle_ping).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), true, false)
        );
        assert_eq!(f.writes().await, writes);
        assert!(
            of_type(&renew(&mut on.session, "msg_r2", 3).await, "lease_grant").is_empty(),
            "nor is a renewal answered or settled"
        );
        f.done();
    }

    /// ADR 0257 for the repeat from `hello`: a holder that lists an unmarked
    /// row reading `local` as `expiring` at its token gets the same-token
    /// `local` grant, then a `ping`. The barrier has not passed before the
    /// `pong` and has after it; the `pong` writes nothing, the row already
    /// reading `local`, unmarked. On a session whose `hello` does not list
    /// the lease, a refusal of the repeat gives nothing back, and the `pong`
    /// after it passes no barrier.
    #[tokio::test]
    async fn the_same_token_local_grant_from_hello_is_followed_by_its_barrier_ping() {
        let f = fixture("none", "local", Some(D as i64), false).await;
        created(&f).await;
        let (mut session, frames) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        let frames: Vec<Value> = frames
            .into_iter()
            .filter(|frame| frame["type"] == "lease_grant" || frame["type"] == "ping")
            .collect();
        let (_grant, ping) = grant_then_ping(&frames, 3);
        let hub = &f.state.hub;
        let generation = session.generation;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(!hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D));
        let writes = f.writes().await;
        pong(&mut session, &ping).await;
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert_eq!(f.writes().await, writes);
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );

        let (mut session, frames) = f.session(None).await;
        let frames: Vec<Value> = frames
            .into_iter()
            .filter(|frame| frame["type"] == "lease_grant" || frame["type"] == "ping")
            .collect();
        let (grant, ping) = grant_then_ping(&frames, 3);
        let refusal = NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "the lease is not held".to_string(),
            fatal: false,
            term: None,
        })
        .to_frame("msg_refusal", Some(&grant))
        .to_text();
        let mut outbox = Vec::new();
        session.handle_text(&refusal, &mut outbox).await;
        pong(&mut session, &ping).await;
        assert!(!hub.barrier_passed(&f.node_id, session.generation, LEASE, 3));
        assert_eq!(f.writes().await, writes, "no regrant");
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        f.done();
    }

    /// ADR 0257 after a conversion: each same-token `local` grant has its own
    /// barrier. A `hello` that listed the lease `local` at 3 passed it and a
    /// conversion was recorded; the holder renews at 3, the policy leaves
    /// `warm`, and the `local` grant that ends the renewals goes out: no
    /// conversion may be recorded before its `ping`'s `pong`, and one may
    /// after it. A later refused repeat on the same session passes no
    /// barrier with the `pong` after it, though one passed before.
    #[tokio::test]
    async fn every_local_grant_has_its_own_barrier_after_a_conversion() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D));
        assert_answer(&renew(&mut on.session, "msg_r1", 3).await, "msg_r1", 3, D);
        assert_eq!(f.row().await.mode, "expiring");

        r(&f).await;
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        let (grant, ping) = grant_then_ping(&queued(&mut on), 3);
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(
            !hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D),
            "no conversion before this grant's pong"
        );
        let refusal = NodeMessage::Error(ProtocolError {
            code: ErrorCode::NotFound,
            message: "refused".to_string(),
            fatal: false,
            term: None,
        })
        .to_frame("msg_refusal", Some(&grant))
        .to_text();
        let mut outbox = Vec::new();
        on.session.handle_text(&refusal, &mut outbox).await;
        pong(&mut on.session, &ping).await;
        assert!(
            !hub.barrier_passed(&f.node_id, generation, LEASE, 3),
            "a refused grant passes no barrier"
        );
        assert!(f.row().await.unsettled, "nor does its pong write");
        f.done();
    }

    /// The resend from `hello` ("The holder's next `hello`", third bullet,
    /// ADR 0255): a holder that lists the lease `expiring` over a marked row
    /// that reads `local`, while the policy is off `warm`, gets the
    /// same-token `local` grant again once the row's newest write is
    /// acknowledged, then its `ping`. A holder above the row's token has the
    /// row written `local` at its token first, keeping the mark, and the
    /// grant goes at that token once that write is acknowledged.
    #[tokio::test]
    async fn the_resend_from_hello_waits_for_replication_and_takes_the_holders_token() {
        for listed in [3, 5] {
            let f = fixture("none", "local", Some(D as i64), true).await;
            created(&f).await;
            let writes = f.writes().await;
            let mut on = live(
                &f.state,
                &f,
                Some(("uncertain", LeaseMode::Expiring, listed)),
            )
            .await;
            assert!(of_type(&queued(&mut on), "lease_grant").is_empty());
            let row = f.row().await;
            assert_eq!(
                (row.mode.as_str(), row.fencing_token, row.unsettled),
                ("local", i64::try_from(listed).unwrap(), true)
            );
            assert_eq!(f.writes().await, writes + i64::from(listed > 3));
            f.state.hub.attempt_held().await;
            assert!(queued(&mut on).is_empty(), "not acknowledged");
            assert_eq!(
                view(&f.state, &f).await,
                (true, vec![f.node_id.clone()], true)
            );
            ack(&f.state, &f, &on).await;
            f.state.hub.attempt_held().await;
            let (_, ping) = grant_then_ping(&queued(&mut on), listed);
            pong(&mut on.session, &ping).await;
            let row = f.row().await;
            assert_eq!((row.mode.as_str(), row.unsettled), ("local", false));
            f.done();
        }
    }

    /// Evidence, and the session lost while R waits. R on a live session
    /// whose `hello` did not list the lease (it was granted after the
    /// `hello`): the grant waits for the holder (`grant_waiting_on` names
    /// it) until a renewal at the row's token shows the holder holds the
    /// lease `expiring` there, then goes out. A session lost before the
    /// grant went out: the newer session, whose `hello` lists nothing, gets
    /// nothing, and neither does the old one ("Not listed": nothing is given
    /// back).
    #[tokio::test]
    async fn the_local_grant_needs_evidence_and_a_lost_session_sends_nothing() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, None).await;
        r(&f).await;
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no evidence yet");
        assert_eq!(
            view(&f.state, &f).await,
            (true, vec![f.node_id.clone()], true)
        );
        assert_answer(&renew(&mut on.session, "msg_r1", 3).await, "msg_r1", 3, D);
        f.state.hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        f.done();

        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut old = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        let mut newer = live(&f.state, &f, None).await;
        ack(&f.state, &f, &newer).await;
        f.state.hub.attempt_held().await;
        assert!(queued(&mut newer).is_empty());
        assert!(queued(&mut old).is_empty());
        assert!(!f.state.hub.grant_pending(LEASE));
        assert!(f.row().await.unsettled, "the mark stays");
        f.done();
    }

    /// A primary restart in each window. While R waits: the restarted
    /// primary seeds the marked row's start gate (`start + D + 2M`), holds
    /// nothing until the holder's `hello`, then sends the grant on that
    /// session once acknowledged. Between the grant and its `pong`: the
    /// holder took it and lists the lease `local`, whose report writes the
    /// row `local`, unmarked. After the `pong`: the row is `local`, unmarked,
    /// and no gate is seeded.
    #[tokio::test]
    async fn a_restart_in_each_window_loses_nothing() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let _on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        let (after, _clock) = restart(&f).await;
        after.hub.set_voter_support(Arc::new(EverySupport));
        assert_eq!(
            after.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(18))
        );
        assert!(
            !after.hub.grant_pending(LEASE),
            "nothing held before the hello"
        );
        let mut on = live(&after, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        ack(&after, &f, &on).await;
        after.hub.attempt_held().await;
        let (_, ping) = grant_then_ping(&queued(&mut on), 3);

        let (again, _clock) = restart(&f).await;
        assert!(f.row().await.unsettled, "between the grant and its pong");
        drop(on);
        let _taken = live(&again, &f, Some(("held", LeaseMode::Local, 3))).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        drop(ping);

        let (last, _clock) = restart(&f).await;
        assert_eq!(
            last.hub.authority().gate_remaining(LEASE),
            None,
            "after the pong"
        );
        f.done();
    }
}
