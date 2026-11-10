// SPDX-License-Identifier: AGPL-3.0-only
//! A converted lease's first renewal, and the settle (`node-agent-protocol.md`
//! section 8.4, "Changing the mode" and "Settling a lease"; ADR 0241, ADR
//! 0244, ADR 0254, ADR 0257, ADR 0261; issue #555).
//!
//! - **A conversion in flight.** The hub keeps, for each session generation,
//!   the conversion grants it sent and whose first renewal it has not
//!   answered ([`InFlight`]). It dies with the generation: after a session
//!   cut or a restart, the holder's next `hello` decides (below). Nothing
//!   sends a conversion grant yet; [`NodeHub::note_conversion_sent`] is where
//!   its send records it.
//! - **The first answer.** A `lease_renew` of a lease in flight on its
//!   session, at the conversion's token, over a row that still reads `local`,
//!   is answered as the first one (ADR 0257: the conversion goes out only
//!   after the barrier `pong`, so such a renewal is one the node sent after
//!   it took the conversion). One single-writer transaction writes the row
//!   `expiring` and records the gate at `T_grant + D_1 + 2M(D_1)`, with `D_1`
//!   the longer of the conversion's and the answer's duration (ADR 0244, ADR
//!   0254). It commits before the answer is queued. Until then the row reads
//!   `local` and opens no gate, so the lease moves only as a `local` lease
//!   does: `failoverApp` answers `lease_held` (ADR 0241).
//! - **The settle.** A renewal of the holder answered on this session, then
//!   a `ping`; once its `pong` arrives, one write gives the row the holder's
//!   token, `mode` `expiring`, the longest duration granted on the session,
//!   and clears the unsettled mark. The `ping` records the row's newest
//!   change-log write as the answer's transaction left it, and the `pong`
//!   settles only a row still at that write: any write in between refuses
//!   it, and the holder's next renewal pings again. It starts in two ways:
//!   the holder's `hello` lists the lease `expiring` (`held` or
//!   `uncertain`) while the policy is `warm`, over a marked row or over a
//!   `local` row at its token (a conversion whose first renewal this primary
//!   never answered); or a renewal of a marked `expiring` row at its token
//!   while the policy is `warm`, the first renewal after an `expiring` grant
//!   with a new token, such as a failover's or a regrant's ("The mark").
//!
//! - **From R** (section 8.4, "From R until the holder has taken the
//!   grant"). R writes the row `local` and marks it in `updateApp`'s
//!   transaction ([`leases::end_renewals`]). A renewal of such a row at its
//!   token is answered as for an `expiring` lease while the policy is off
//!   `warm`, with no write, its gate counted at the stored duration, until
//!   the session sends the same-token `local` grant; from then on none is
//!   answered on that session (ADR 0256) until a conversion grant follows.
//!   Back on `warm` before that grant, the renewal is a settle's ("While R
//!   waits").
//!
//! Not built here: sending the conversion grant, and the same-token `local`
//! grant with its `ping` (issue #555). The holder's `local` report is
//! [`super::local_report`].

use super::{ControlMessage, NodeHub, Session, lease_token, new_msg_id};
use crate::authority::{DEFAULT_DURATION_MS, GrantKind, MIN_DURATION_MS};
use crate::changelog::{Actor, ActorType};
use crate::leases::{self, Lease};
use std::collections::HashMap;

/// A conversion grant a session sent, whose first renewal it has not
/// answered: the token it carried and its `duration_ms` (`D_conv`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InFlight {
    pub token: u64,
    pub duration_ms: u64,
}

/// What one session knows of the leases it settles.
#[derive(Debug, Default)]
pub(super) struct Settles {
    /// The holder's token its `hello` listed for each lease that this
    /// session settles from `hello`.
    reported: HashMap<String, u64>,
    /// The longest `duration_ms` this session granted for each lease.
    granted: HashMap<String, u64>,
    /// Each settling `ping` not answered yet, by its message id.
    pings: HashMap<String, Pinged>,
}

/// What a settling `ping` stands for: the lease, the holder's token, and the
/// `seq` of the row's newest change-log write as the settling answer's
/// transaction left it ([`leases::newest_write_seq`]), `None` for a row the
/// log has no entry of.
#[derive(Debug)]
struct Pinged {
    lease_id: String,
    token: u64,
    seq: Option<u64>,
}

impl Settles {
    /// The settle of `lease_id` from the holder's `hello`, at `token`.
    pub(super) fn report(&mut self, lease_id: &str, token: u64) {
        self.reported.insert(lease_id.to_string(), token);
    }

    /// Drops what this session knows of settling `lease_id`: the same-token
    /// `local` grant went out, so no settle `ping` from before it may settle
    /// the lease ([`super::local_end`]).
    pub(super) fn forget(&mut self, lease_id: &str) {
        self.reported.remove(lease_id);
        self.pings.retain(|_, pinged| pinged.lease_id != lease_id);
    }
}

/// How a renewal is answered, decided from the row as read in the answer's
/// transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Plan {
    /// No answer; the reason is logged.
    Unanswered(&'static str),
    /// A conversion is in flight over a row that carries the mark, or while
    /// the app's policy is off `warm`: the policy change ended it. The
    /// renewal is answered as for an `expiring` lease, with no write, and the
    /// conversion is no longer in flight.
    Ended(InFlight),
    /// The first answer after the conversion.
    Converted(InFlight),
    /// A renewal of a marked row that reads `local` at its token while the
    /// policy is off `warm`: R wrote it, and this session has not sent the
    /// `local` grant. Answered as for an `expiring` lease, with no write
    /// (section 8.4, "From R until the holder has taken the grant").
    FromR,
    /// A settle's answer: from the holder's `hello`, at the holder's token,
    /// or of a marked row that reads `local` at its token after the policy
    /// returned to `warm` ("While R waits").
    Settle,
    /// The answer to an `expiring` row at its token. `settle` when the row
    /// carries the mark and the policy is `warm`: a `ping` follows.
    Ordinary { settle: bool },
}

/// What this session knows of a lease when one of its renewals arrives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Seen {
    /// This session's conversion of the lease in flight.
    pub in_flight: Option<InFlight>,
    /// The token this session's `hello` registered a settle at.
    pub reported: Option<u64>,
    /// The token of the same-token `local` grant this session sent, which
    /// ended the renewals on it (ADR 0256).
    pub local_sent: Option<u64>,
}

/// How a renewal at `token` of `row` is answered, once the row names the node
/// unrevoked at a token no higher than `token` (`renewal_revoke` found
/// nothing to revoke). `seen` is what this session knows of the lease, and
/// `warm` whether the app's policy is `warm`.
pub(super) fn plan(row: &Lease, token: u64, seen: Seen, warm: bool) -> Plan {
    let Seen {
        in_flight,
        reported,
        local_sent,
    } = seen;
    let row_token = lease_token(row);
    let local = row.mode != "expiring";
    // Section 8.4, "From R until the holder has taken the grant": once the
    // session sent the same-token `local` grant, it answers no renewal of the
    // lease on that session (ADR 0256), until a conversion grant follows,
    // which lifts this ([`NodeHub::note_conversion_sent`]). A renewal at a
    // higher token answers a later grant.
    if local_sent.is_some_and(|sent| token <= sent) {
        return Plan::Unanswered("the local grant ended the renewals on this session");
    }
    // A conversion keeps the row's token (ADR 0258): one in flight at a token
    // the row does not hold is answered neither as the first renewal nor as
    // ended, and the renewal falls through to the arms below.
    if let Some(in_flight) =
        in_flight.filter(|in_flight| in_flight.token == token && row_token == token)
    {
        // Section 8.4 "Changing the mode" (#555): a policy change off
        // `warm` (R) ends a conversion in flight; the primary answers the
        // holder's renewals as "From R until the holder has taken the grant"
        // says, and writes no `expiring` for them. R marks the row, so a
        // marked row is one R may have written; a policy off `warm` over an
        // unmarked row is the same change before R is built.
        if row.unsettled || !warm {
            return Plan::Ended(in_flight);
        }
        if local {
            return Plan::Converted(in_flight);
        }
    }
    if warm && reported == Some(token) && (row.unsettled || (local && row_token == token)) {
        return Plan::Settle;
    }
    // A marked row that reads `local` at the renewal's token: the renewal
    // proves the holder holds the lease `expiring` there (only an `expiring`
    // lease renews), so R wrote the row while it did. Off `warm`, its
    // renewals are answered until the `local` grant goes out ("From R …",
    // Renewals); back on `warm`, the lease is settled at this renewal ("While
    // R waits": "settles the lease").
    if local && row.unsettled && row_token == token {
        return if warm { Plan::Settle } else { Plan::FromR };
    }
    if !local && row_token == token {
        return Plan::Ordinary {
            settle: row.unsettled && warm,
        };
    }
    Plan::Unanswered(if local {
        "a renewal of a local lease gets no answer"
    } else {
        "a renewal above the lease's token gets no answer"
    })
}

/// The duration of a first answer, a settle's answer or an answer from R:
/// `D`, or the stored
/// duration when it is shorter. A grant longer than the stored duration
/// waits until its raise is replicated, and the fallback is the shorter
/// duration (ADR 0268); the conversion raised the stored duration to at
/// least its own before it went out (ADR 0254). `None` for a row that stores
/// no valid duration: there is no shorter one to fall back to (ADR 0289).
pub(super) fn answer_ms(row: &Lease) -> Option<u64> {
    let stored = row.duration_ms.and_then(|ms| u64::try_from(ms).ok())?;
    (stored >= MIN_DURATION_MS).then(|| stored.min(DEFAULT_DURATION_MS))
}

/// The kind the authority records a planned answer of `duration_ms` as, or
/// `None` when the row cannot be answered so. The first answer counts the
/// conversion's duration, and the stored duration must cover it (the raise
/// before the conversion, ADR 0254). An answer after the policy change ended
/// the conversion counts it too: the node's provisional deadline came from
/// it, and the side that grants less is a longer gate. A settle's answer from `hello` counts
/// the stored duration, which bounds every duration an earlier grant may have
/// carried, the conversion's included (section 8.4, "What the authority must
/// enforce"); so does an answer from R, which "as for an `expiring` lease"
/// gives no other count, and whose stored duration bounds the holder's
/// earlier grants (ADR 0268). An ordinary answer that settles a marked row
/// counts the stored duration as well ("Settling a lease": "The answer that
/// settles the lease counts for its gate at the longer of its own duration
/// and the stored `duration_ms`"); it still carries `D` and raises the
/// stored duration to it, as any ordinary answer does.
pub(super) fn counted(plan: Plan, row: &Lease) -> Option<GrantKind> {
    let stored = row.duration_ms.and_then(|ms| u64::try_from(ms).ok());
    match plan {
        Plan::Converted(in_flight) => stored
            .filter(|stored| *stored >= in_flight.duration_ms)
            .map(|_| GrantKind::CountedRenewal {
                counted_ms: in_flight.duration_ms,
            }),
        Plan::Ended(in_flight) => Some(GrantKind::CountedRenewal {
            counted_ms: in_flight.duration_ms,
        }),
        Plan::Settle | Plan::FromR => {
            stored.map(|counted_ms| GrantKind::CountedRenewal { counted_ms })
        }
        Plan::Ordinary { settle: true } => {
            Some(
                stored.map_or(GrantKind::Renewal, |counted_ms| GrantKind::CountedRenewal {
                    counted_ms,
                }),
            )
        }
        Plan::Ordinary { settle: false } => Some(GrantKind::Renewal),
        Plan::Unanswered(_) => None,
    }
}

impl NodeHub {
    /// Records that `generation` sent `node_id` the conversion grant of
    /// `lease_id` at `token` for `duration_ms`, so the next renewal of it on
    /// that session is answered as the first one. Called when the grant is
    /// queued. Refused (`false`) unless ADR 0257's barrier passed for the
    /// lease at `token` on `generation`: the `pong` after its same-token
    /// `local` grant, or the session's `hello` listing the lease `local`
    /// ([`NodeHub::barrier_passed`]). Returns whether it was recorded.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "nothing sends a conversion grant yet (#555)")
    )]
    pub(crate) fn note_conversion_sent(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        token: u64,
        duration_ms: u64,
    ) -> bool {
        if !self.barrier_passed(node_id, generation, lease_id, token) {
            tracing::warn!(
                lease_id,
                node_id,
                "no conversion grant before the barrier's pong (ADR 0257)"
            );
            return false;
        }
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        handle
            .conversions
            .insert(lease_id.to_string(), InFlight { token, duration_ms });
        // "From R …", Renewals: the `local` grant's refusal of renewals ends
        // when a conversion grant follows.
        handle.local_sent.remove(lease_id);
        true
    }

    /// Records that `generation` queued the same-token `local` grant of
    /// `lease_id` at `token`: from then on no renewal of the lease at that
    /// token or below is answered on that session (section 8.4, "From R until
    /// the holder has taken the grant", ADR 0256). Returns whether
    /// `generation` is still the node's session.
    pub(super) fn note_local_grant_sent(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        token: u64,
    ) -> bool {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        let sent = handle.local_sent.entry(lease_id.to_string()).or_insert(0);
        *sent = (*sent).max(token);
        true
    }

    /// The token of the same-token `local` grant `generation` sent for
    /// `lease_id`, if it sent one and no conversion grant followed.
    pub(super) fn local_grant_sent(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
    ) -> Option<u64> {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .and_then(|handle| handle.local_sent.get(lease_id).copied())
    }

    /// `generation`'s conversion of `lease_id` in flight, if any.
    pub(super) fn conversion_in_flight(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
    ) -> Option<InFlight> {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .and_then(|handle| handle.conversions.get(lease_id).copied())
    }

    /// The first answer of `answered` committed, or a policy change ended
    /// it: the conversion is no longer in flight. A conversion recorded since
    /// stays.
    pub(super) fn conversion_answered(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        answered: InFlight,
    ) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        if let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
            && handle.conversions.get(lease_id) == Some(&answered)
        {
            handle.conversions.remove(lease_id);
        }
    }
}

impl Session {
    /// Notes that this session granted `lease_id` for `duration_ms`, for the
    /// settle's row write ("the longest `duration_ms` it granted on that
    /// session").
    pub(super) fn note_granted(&mut self, lease_id: &str, duration_ms: u64) {
        let longest = self
            .settles
            .granted
            .entry(lease_id.to_string())
            .or_insert(0);
        *longest = (*longest).max(duration_ms);
    }

    /// The token this session's `hello` registered a settle of `lease_id`
    /// at.
    pub(super) fn reported_settle(&self, lease_id: &str) -> Option<u64> {
        self.settles.reported.get(lease_id).copied()
    }

    /// Queues the settling `ping` after an answer of `lease_id` at `token`,
    /// unless one is already unanswered for it on this session: its `pong`
    /// proves the node received every answer sent before it (section 4.2),
    /// and the row write takes the longest duration granted on the session,
    /// which covers any answer sent after it. `seq` is the row's newest
    /// change-log write as the settling answer's transaction left it, read
    /// after any write that transaction made and before its commit: the
    /// `pong` settles only a row still at that write ([`Session::on_pong`]).
    /// An unanswered `ping` keeps the `seq` of the answer it followed.
    pub(super) fn ping_to_settle(
        &mut self,
        lease_id: &str,
        token: u64,
        seq: Option<u64>,
        outbox: &mut Vec<String>,
    ) {
        if self
            .settles
            .pings
            .values()
            .any(|pinged| pinged.lease_id == lease_id)
        {
            return;
        }
        let id = new_msg_id();
        outbox.push(ControlMessage::Ping.to_frame(&id, None).to_text());
        self.settles.pings.insert(
            id,
            Pinged {
                lease_id: lease_id.to_string(),
                token,
                seq,
            },
        );
    }

    /// A `pong` (section 5.20): when it answers a settling `ping`, the row is
    /// settled ("Settling a lease", ADR 0261), in one single-writer
    /// transaction that finds, again:
    ///
    /// - this session still the node's;
    /// - the row as the settling answer read it: its newest change-log
    ///   write is still the one the `ping` recorded. The `pong` proves the
    ///   node received that answer, not that nothing written since changed
    ///   what the node holds, so any write of the row in between refuses the
    ///   settle;
    /// - the row naming the node, unrevoked, at a token no higher than the
    ///   holder's, and still to settle (marked, or reading `local`);
    /// - no failed fence of the lease, recorded or not (section 8.5);
    /// - the app's policy `warm`.
    ///
    /// The write takes the holder's token, `mode` `expiring`, the longest
    /// duration granted on this session but never less than the stored one,
    /// and clears the mark ([`leases::settle`]). Anything else writes nothing:
    /// the row keeps the mark and moves only as a `local` lease does. The
    /// `ping` is spent either way, so the holder's next renewal that plans a
    /// settle sends a new one, recorded against the row as that answer reads
    /// it.
    pub(super) async fn on_pong(&mut self, re: Option<&str>) {
        let Some(pinged) = re.and_then(|re| self.settles.pings.remove(re)) else {
            // Not a settle's: the `pong` after a same-token `local` grant
            // ([`super::local_end`]).
            if let Some(re) = re {
                self.on_barrier_pong(re).await;
            }
            return;
        };
        let Pinged {
            lease_id,
            token,
            seq,
        } = pinged;
        let granted = self.settles.granted.get(&lease_id).copied().unwrap_or(0);
        match self.settle_row(&lease_id, token, seq, granted).await {
            Ok(Some(lease)) => {
                self.settles.reported.remove(&lease_id);
                tracing::info!(
                    lease_id,
                    node_id = %self.node_id,
                    token = lease.fencing_token,
                    duration_ms = ?lease.duration_ms,
                    "the lease is settled: its row reads expiring, unmarked"
                );
                // Section 6.4: the desired state carries the lease's token.
                self.hub.node_changed(&self.node_id);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(code = %e.code.as_str(), lease_id, "cannot settle a lease");
            }
        }
    }

    /// The settle's write ([`Session::on_pong`]), at the holder's `token`,
    /// if the row's newest change-log write is still `seq`, the one the
    /// settling answer read, and every other check holds again.
    async fn settle_row(
        &self,
        lease_id: &str,
        token: u64,
        seq: Option<u64>,
        granted: u64,
    ) -> Result<Option<Lease>, crate::error::ApiError> {
        let mut w = self.hub.0.store.begin_write().await?;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(None);
        }
        // The session sent the same-token `local` grant since: the holder may
        // hold the lease `local` now, so no settle `ping` from before it
        // settles the lease, the `pong` arriving after the holder took
        // `local` ([`super::local_end`]).
        if self
            .hub
            .local_grant_sent(&self.node_id, self.generation, lease_id)
            .is_some_and(|sent| token <= sent)
        {
            return Ok(None);
        }
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(None);
        };
        let to_settle = row.unsettled || row.mode != "expiring";
        if leases::newest_write_seq(&mut w.tx, lease_id).await? != seq
            || row.holder_node_id != self.node_id
            || row.is_revoked()
            || lease_token(&row) > token
            || !to_settle
        {
            tracing::info!(
                lease_id,
                "the row changed since its settling answer; no settle"
            );
            return Ok(None);
        }
        if crate::fences::has_failed_fence(&mut w.tx, &self.hub.0.unrecorded_failures, lease_id)
            .await?
            || !app_is_warm(&mut w.tx, &row.resource_id).await?
        {
            return Ok(None);
        }
        let stored = row
            .duration_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .unwrap_or(0);
        let duration_ms = i64::try_from(stored.max(granted)).unwrap_or(i64::MAX);
        let token = i64::try_from(token).unwrap_or(i64::MAX);
        let actor = Actor {
            kind: ActorType::Node,
            id: self.node_id.clone(),
        };
        let lease = leases::settle(&mut w.tx, row, token, duration_ms, &actor).await?;
        w.commit().await?;
        Ok(Some(lease))
    }
}

/// Whether `app_id`'s HA policy is `warm` (or `hot`, as `grant_app_lease`
/// reads it).
///
/// # Errors
///
/// A database error.
pub(super) async fn app_is_warm(
    conn: &mut sqlx::SqliteConnection,
    app_id: &str,
) -> Result<bool, crate::error::ApiError> {
    let policy: Option<String> = sqlx::query_scalar("SELECT ha_policy FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(matches!(policy.as_deref(), Some("warm" | "hot")))
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::tests::hub_with_node;
    use super::{InFlight, Plan, Seen, plan};
    use crate::AppState;
    use crate::authority::{ManualClock, MonotonicClock, VoterSupport};
    use crate::changelog::{self, Actor, ActorType, Op};
    use crate::leases::{self, Lease};
    use crate::session::Session;
    use quoro_proto::wire::{self, NodeMessage};
    use quoro_proto::{LeaseId, LeaseMode, LeaseRenew};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    pub(in crate::session) const LEASE: &str = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D";
    pub(in crate::session) const APP: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E";
    const D: u64 = 15_000;

    #[derive(Debug)]
    pub(in crate::session) struct EverySupport;

    impl VoterSupport for EverySupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            true
        }
    }

    /// One node, one app with `policy`, and its lease row on the node: `mode`
    /// at token 3, storing `stored`, marked or not. The hub's authority runs
    /// on a manual clock and holds every support, so the tests move time
    /// only through the clock and never sleep.
    pub(in crate::session) struct Fixture {
        pub(in crate::session) state: AppState,
        pub(in crate::session) node_id: String,
        pub(in crate::session) data_dir: std::path::PathBuf,
        pub(in crate::session) clock: Arc<ManualClock>,
    }

    pub(in crate::session) async fn fixture(
        policy: &str,
        mode: &str,
        stored: Option<i64>,
        marked: bool,
    ) -> Fixture {
        let (state, node_id, data_dir) = hub_with_node().await;
        let pool = state.store.pool();
        sqlx::query("UPDATE nodes SET subnet = '10.42.1.0/24' WHERE id = ?")
            .bind(&node_id)
            .execute(pool)
            .await
            .unwrap();
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) \
             VALUES ('prj_c', 'c', 'c', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, ha_policy, image, port, health_check, \
             resources, placement, status, created_at, updated_at) \
             VALUES (?, 'prj_c', 'c', ?, 'img', 80, '{}', '{}', '{\"node_ids\":[]}', \
             'stopped', ?, ?)",
        )
        .bind(APP)
        .bind(policy)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, duration_ms, granted_at, unsettled) \
             VALUES (?, 'app', ?, ?, ?, 3, ?, ?, ?)",
        )
        .bind(LEASE)
        .bind(APP)
        .bind(&node_id)
        .bind(mode)
        .bind(stored)
        .bind(&now)
        .bind(i64::from(marked))
        .execute(pool)
        .await
        .unwrap();
        let clock = Arc::new(ManualClock::default());
        state
            .hub
            .authority()
            .set_test_clock(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        state.hub.set_voter_support(Arc::new(EverySupport));
        Fixture {
            state,
            node_id,
            data_dir,
            clock,
        }
    }

    impl Fixture {
        /// A session of the node whose `hello` lists the lease `state` in
        /// `mode` at `token`, or not at all, and the frames the session
        /// start queued on the outbox.
        pub(in crate::session) async fn session(
            &self,
            listed: Option<(&str, LeaseMode, u64)>,
        ) -> (Session, Vec<Value>) {
            let hub = &self.state.hub;
            let (generation, _rx, _closed, _changed) = hub.attach(&self.node_id);
            let mut session = Session::new(hub.clone(), self.node_id.clone(), generation);
            let mut outbox = Vec::new();
            let hello = NodeMessage::Hello(hello(&self.node_id, listed))
                .to_frame("msg_hello", None)
                .to_text();
            assert!(!session.handle_text(&hello, &mut outbox).await);
            (session, frames(outbox))
        }

        pub(in crate::session) async fn row(&self) -> Lease {
            let mut conn = self.state.store.pool().acquire().await.unwrap();
            leases::by_id(&mut conn, LEASE).await.unwrap().unwrap()
        }

        /// How many change-log entries the lease has.
        pub(in crate::session) async fn writes(&self) -> i64 {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
            )
            .bind(LEASE)
            .fetch_one(self.state.store.pool())
            .await
            .unwrap()
        }

        /// What `failoverApp` finds for a source that is not connected
        /// ([`crate::failover::held_while_away`]).
        pub(in crate::session) async fn held_while_away(&self) -> Option<&'static str> {
            let lease = self.row().await;
            let mut conn = self.state.store.pool().acquire().await.unwrap();
            crate::failover::held_while_away(&mut conn, self.state.hub.authority(), &lease)
                .await
                .unwrap()
        }

        fn passed(&self) -> bool {
            self.state.hub.authority().passed(LEASE)
        }

        pub(in crate::session) fn done(self) {
            std::fs::remove_dir_all(&self.data_dir).unwrap();
        }
    }

    pub(in crate::session) fn hello(
        node_id: &str,
        listed: Option<(&str, LeaseMode, u64)>,
    ) -> wire::Hello {
        wire::Hello {
            node_id: node_id.to_string(),
            agent_version: "agent".to_string(),
            engine_version: "engine".to_string(),
            boot_id: "boot-1".to_string(),
            stored_rev: 0,
            applied_rev: 0,
            log_seq: 0,
            log_hash: "0".repeat(64),
            leases: listed
                .into_iter()
                .map(|(state, mode, token)| wire::HelloLease {
                    lease_id: LEASE.to_string(),
                    resource_type: quoro_proto::ResourceType::App,
                    resource_id: APP.to_string(),
                    mode,
                    fencing_token: token,
                    state: state.to_string(),
                })
                .collect(),
            capacity: wire::Capacity {
                cpus: 2,
                memory_mb: 2_048,
                disk_gb: 10,
            },
            allocated: wire::Capacity::default(),
            kvm: wire::Kvm {
                available: true,
                kernel: "6.12.0".to_string(),
                nested: None,
            },
            dropped_events: 0,
            mesh_public_key: None,
            volume_snapshot: None,
            custody_public_key: None,
        }
    }

    pub(in crate::session) fn frames(outbox: Vec<String>) -> Vec<Value> {
        outbox
            .iter()
            .map(|text| serde_json::from_str(text).unwrap())
            .collect()
    }

    pub(in crate::session) fn of_type<'a>(frames: &'a [Value], kind: &str) -> Vec<&'a Value> {
        frames
            .iter()
            .filter(|frame| frame["type"] == kind)
            .collect()
    }

    /// The node renews the lease at `token` on `session`; the frames the
    /// session queued in answer.
    pub(in crate::session) async fn renew(
        session: &mut Session,
        id: &str,
        token: u64,
    ) -> Vec<Value> {
        let mut outbox = Vec::new();
        let renew = NodeMessage::LeaseRenew(LeaseRenew {
            lease_id: LeaseId(LEASE.to_string()),
            fencing_token: token,
        })
        .to_frame(id, None)
        .to_text();
        session.handle_text(&renew, &mut outbox).await;
        frames(outbox)
    }

    pub(in crate::session) async fn pong(session: &mut Session, re: &str) {
        let mut outbox = Vec::new();
        let pong = NodeMessage::Pong.to_frame("msg_pong", Some(re)).to_text();
        session.handle_text(&pong, &mut outbox).await;
        assert!(outbox.is_empty(), "a pong is not answered");
    }

    /// The one `lease_grant` among `frames`, which answers `re`, `expiring`
    /// at `token` for `duration_ms`.
    pub(in crate::session) fn assert_answer(
        frames: &[Value],
        re: &str,
        token: u64,
        duration_ms: u64,
    ) {
        let grants = of_type(frames, "lease_grant");
        assert_eq!(grants.len(), 1, "{frames:?}");
        let grant = grants[0];
        assert_eq!(grant["re"], re);
        assert_eq!(grant["body"]["lease_id"], LEASE);
        assert_eq!(grant["body"]["mode"], "expiring");
        assert_eq!(grant["body"]["fencing_token"], token);
        assert_eq!(grant["body"]["duration_ms"], duration_ms);
    }

    /// Opens a second primary on the fixture's database, as a restart does:
    /// nothing in memory survives, and the start seeds the gates from the
    /// rows. Its authority then runs on a manual clock, seeded again from
    /// its start.
    pub(in crate::session) async fn restart(fixture: &Fixture) -> (AppState, Arc<ManualClock>) {
        let store = crate::store::Store::open(&fixture.data_dir.join("quoro.db"))
            .await
            .unwrap();
        let state = AppState::open(
            store,
            crate::ServerConfig {
                data_dir: fixture.data_dir.clone(),
                secure_cookies: false,
                public_url: None,
                tls_fingerprint: String::new(),
                tls: crate::custody::test_tls(),
                node_network: crate::NodeNetwork {
                    subnet: "10.42.1.0/24".to_string(),
                    gateway: "10.42.1.1".to_string(),
                },
                heartbeat_interval_ms: 200,
                session_timeout_ms: 800,
            },
            Arc::new(crate::ReferenceAsDigest),
            Arc::new(crate::crypto::DataKey::ephemeral()),
        )
        .await
        .unwrap();
        let clock = Arc::new(ManualClock::default());
        state
            .hub
            .authority()
            .set_test_clock(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        state.hub.seed_lease_gates().await.unwrap();
        (state, clock)
    }

    /// ADR 0241, ADR 0244, ADR 0254, ADR 0257, the unsafe direction and the
    /// clocks. A renewal that reaches the primary before it sent the
    /// conversion (one that crossed it on the wire) gets no answer and
    /// writes nothing. The first renewal after the conversion is answered:
    /// the row reads `expiring` once the answer is queued, written in a
    /// transaction committed before it, and the gate counts the conversion's
    /// 30 s with its 3 s margin, on the primary's monotonic clock alone.
    /// Before that answer the lease has no gate and a failover finds it held.
    /// Later renewals are answered as any other.
    #[tokio::test]
    async fn the_first_renewal_after_a_conversion_writes_expiring_before_its_answer() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, start) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert_eq!(
            of_type(&start, "lease_grant").len(),
            1,
            "the held local lease is granted again at the start, as on main"
        );
        let writes = f.writes().await;

        // Crossed on the wire: sent before the node took the conversion.
        assert!(of_type(&renew(&mut session, "msg_r0", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed(), "the conversion opened no gate");
        assert!(f.held_while_away().await.is_some(), "lease_held");

        let in_flight = InFlight {
            token: 3,
            duration_ms: 30_000,
        };
        assert!(f.state.hub.note_conversion_sent(
            &f.node_id,
            session.generation,
            LEASE,
            in_flight.token,
            in_flight.duration_ms,
        ));
        assert!(f.passed(), "nor does the conversion's send");
        assert!(f.held_while_away().await.is_some(), "still lease_held");

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty(), "no ping: not a settle");
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", false));
        assert_eq!(
            row.duration_ms,
            Some(30_000),
            "the conversion's raise stays"
        );
        assert_eq!(f.writes().await, writes + 1, "one write, the mode");
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE),
            None
        );

        // T_grant + 30 s + 2 x 3 s, not the answer's 15 s + 2 x 1.5 s.
        f.clock.advance(Duration::from_millis(35_999));
        assert!(!f.passed());
        assert!(f.held_while_away().await.is_some(), "the gate holds it");
        f.clock.advance(Duration::from_millis(1));
        assert!(f.passed());
        assert_eq!(f.held_while_away().await, None);

        let later = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&later, "msg_r2", 3, D);
        assert_eq!(f.writes().await, writes + 1, "a renewal writes nothing");
        f.done();
    }

    /// The lost session in each window. A conversion is in flight only on
    /// the session that sent it: once a newer session replaced it (the
    /// conversion not taken, taken, or its first renewal read but not
    /// answered), the old session answers nothing and writes nothing, the
    /// lease has no gate, and a failover finds it held. The newer session
    /// has no conversion in flight.
    #[tokio::test]
    async fn a_conversion_is_answered_only_on_the_session_that_sent_it() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let (mut old, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert!(
            f.state
                .hub
                .note_conversion_sent(&f.node_id, old.generation, LEASE, 3, D)
        );
        let writes = f.writes().await;
        let (mut new, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        for generation in [old.generation, new.generation] {
            assert_eq!(
                f.state
                    .hub
                    .conversion_in_flight(&f.node_id, generation, LEASE),
                None
            );
        }
        assert!(
            !f.state
                .hub
                .note_conversion_sent(&f.node_id, old.generation, LEASE, 3, D),
            "a replaced session records nothing"
        );
        assert!(of_type(&renew(&mut old, "msg_r1", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some());
        // The newer session's renewal is a settle's, not a first answer: it
        // writes nothing before the pong.
        renew(&mut new, "msg_r2", 3).await;
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// The answer lost after its commit (the session lost between the
    /// commit and the send): the row reads `expiring` and the gate counts
    /// `D_1` = 30 s from the answer's `T_grant`. The node, which never got
    /// the answer, fences at its provisional deadline `t_send + 30 s - 3 s`,
    /// done by `t_send + 30 s`, and `t_send <= T_grant`: at least `2M` before
    /// the gate.
    #[tokio::test]
    async fn a_first_answer_lost_after_its_commit_leaves_the_conversions_gate() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);
        let _lost = renew(&mut session, "msg_r1", 3).await;
        assert_eq!(f.row().await.mode, "expiring");
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        f.done();
    }

    /// A primary restart between the conversion and its first answer: the
    /// row reads `local`, so the restarted primary seeds no gate, has no
    /// conversion in flight, and a failover finds the lease held (ADR 0244).
    /// A restart after the first answer committed (whether or not it was
    /// sent): the row reads `expiring` with the conversion's raised duration,
    /// so the start gate counts `start + 30 s + 2 x 3 s`, which covers the
    /// first answer's gate (ADR 0254).
    #[tokio::test]
    async fn a_restarted_primary_has_no_gate_before_the_first_answer_and_the_start_gate_after() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);

        let (before, _) = restart(&f).await;
        assert!(before.hub.authority().passed(LEASE), "no gate");
        assert_eq!(before.hub.authority().gate_remaining(LEASE), None);
        let lease = f.row().await;
        let mut conn = before.store.pool().acquire().await.unwrap();
        assert!(
            crate::failover::held_while_away(&mut conn, before.hub.authority(), &lease)
                .await
                .unwrap()
                .is_some()
        );
        drop(conn);

        assert_answer(&renew(&mut session, "msg_r1", 3).await, "msg_r1", 3, D);
        let (after, clock) = restart(&f).await;
        assert_eq!(
            after.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        clock.advance(Duration::from_secs(36));
        assert!(after.hub.authority().passed(LEASE));
        f.done();
    }

    /// Section 8.4 "Changing the mode" (#555): a policy change off `warm`
    /// (R, which marks the row) ends a conversion in flight. The holder's
    /// renewal is answered as for an `expiring` lease, with no write: the
    /// row stays `local` and marked, and the conversion is no longer in
    /// flight. Its gate counts the conversion's duration, the longer. The
    /// renewals after it are answered "From R" with no write either, where
    /// only the first was answered before: the app keeps running.
    #[tokio::test]
    async fn a_policy_change_off_warm_ends_a_conversion_in_flight() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert!(
            f.state
                .hub
                .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000)
        );
        end_renewals(&f).await;
        let writes = f.writes().await;

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty());
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("local", true));
        assert_eq!(f.writes().await, writes, "no write");
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE),
            None
        );
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        assert!(f.held_while_away().await.is_some(), "marked: lease_held");

        for id in ["msg_r2", "msg_r3"] {
            let later = renew(&mut session, id, 3).await;
            assert_answer(&later, id, 3, D);
            assert!(of_type(&later, "ping").is_empty());
        }
        assert_eq!(f.writes().await, writes, "still no write");
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("local", true));
        f.done();
    }

    /// "Settling a lease" from `hello` (section 8.4, ADR 0261), after a
    /// session cut that lost a conversion's first answer: the holder lists
    /// the lease `expiring` over a row that still reads `local`, and the
    /// policy is `warm`. The start sends no same-token `local` grant. Its
    /// next renewal is answered, the gate counting the stored duration, and
    /// a `ping` follows; the row still reads `local` until the `pong` that
    /// answers that `ping` writes it `expiring`. Another `pong` writes
    /// nothing.
    #[tokio::test]
    async fn a_holder_that_lists_a_converted_lease_expiring_is_settled_at_the_pong() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let (mut session, start) = f.session(Some(("uncertain", LeaseMode::Expiring, 3))).await;
        assert!(
            of_type(&start, "lease_grant").is_empty(),
            "no local grant ends its renewals: {start:?}"
        );
        let writes = f.writes().await;

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let pings = of_type(&answer, "ping");
        assert_eq!(pings.len(), 1, "{answer:?}");
        let ping = pings[0]["id"].as_str().unwrap().to_string();
        assert_eq!(f.row().await.mode, "local", "not before the pong");
        assert_eq!(f.writes().await, writes);
        assert!(f.held_while_away().await.is_some());
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(18))
        );

        // A second renewal before the pong is answered with no second ping.
        let again = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&again, "msg_r2", 3, D);
        assert!(of_type(&again, "ping").is_empty());

        pong(&mut session, "msg_other").await;
        assert_eq!(f.row().await.mode, "local", "a pong to another ping");
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (
                row.mode.as_str(),
                row.fencing_token,
                row.duration_ms,
                row.unsettled
            ),
            ("expiring", 3, Some(D as i64), false)
        );
        assert_eq!(f.writes().await, writes + 1);
        pong(&mut session, &ping).await;
        assert_eq!(f.writes().await, writes + 1, "once");
        f.done();
    }

    /// The settle of a marked row, as after a failover's held `expiring`
    /// grant (ADR 0303), and what a second failover then finds. While the
    /// row is marked, a source that is not connected holds the lease whatever
    /// its gate (`lease_held`). The holder's renewal is answered and a
    /// `ping` follows; the `pong` clears the mark. From then on the lease
    /// moves on its gate: held until it passes, free after.
    #[tokio::test]
    async fn a_marked_rows_renewal_settles_it_and_the_lease_then_moves_on_its_gate() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(None).await;
        assert!(f.held_while_away().await.is_some(), "marked");

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        f.clock.advance(Duration::from_secs(18));
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some(), "still marked");

        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", false));
        assert_eq!(f.held_while_away().await, None, "the gate has passed");

        let later = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&later, "msg_r2", 3, D);
        assert!(of_type(&later, "ping").is_empty(), "settled");
        assert!(f.held_while_away().await.is_some(), "a new gate");
        f.done();
    }

    /// A first answer the authority refuses (no majority support, section
    /// 8.8) rolls back with its write: no grant, the row still `local`, no
    /// change-log entry, no gate, and the conversion still in flight. Never
    /// a row that reads `expiring` with no gate behind it. Once support
    /// returns, the next renewal is answered as the first one.
    #[tokio::test]
    async fn a_refused_first_answer_writes_nothing_and_keeps_the_conversion() {
        #[derive(Debug)]
        struct NoSupport;
        impl VoterSupport for NoSupport {
            fn supports(&self, _duration_ms: u64) -> bool {
                false
            }
        }
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);
        let writes = f.writes().await;
        f.state.hub.set_voter_support(Arc::new(NoSupport));
        assert!(of_type(&renew(&mut session, "msg_r1", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some());
        assert!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE)
                .is_some()
        );

        f.state.hub.set_voter_support(Arc::new(EverySupport));
        assert_answer(&renew(&mut session, "msg_r2", 3).await, "msg_r2", 3, D);
        assert_eq!(f.row().await.mode, "expiring");
        assert_eq!(f.writes().await, writes + 1);
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        f.done();
    }

    /// "The holder's next `hello`" at a token above a marked row's, while the
    /// row names that holder: the settle answers the renewal at the holder's
    /// token, and the `pong` writes the row at that token (section 8.4).
    #[tokio::test]
    async fn a_holder_above_a_marked_rows_token_is_settled_at_its_own_token() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, start) = f.session(Some(("held", LeaseMode::Expiring, 5))).await;
        assert!(of_type(&start, "lease_revoke").is_empty(), "{start:?}");
        assert!(of_type(&start, "lease_grant").is_empty(), "{start:?}");
        let answer = renew(&mut session, "msg_r1", 5).await;
        assert_answer(&answer, "msg_r1", 5, D);
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(f.row().await.fencing_token, 3, "not before the pong");
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.fencing_token, row.unsettled),
            ("expiring", 5, false)
        );
        f.done();
    }

    /// No settle while the policy is off `warm`, nor once the session is
    /// replaced before the `pong`: the mark stays, and the lease moves only
    /// as a `local` lease does. A replaced session that still reads a
    /// renewal answers nothing: the newer session's start decided the lease.
    #[tokio::test]
    async fn no_settle_off_warm_or_after_the_session_is_replaced() {
        let f = fixture("none", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty(), "off warm");
        assert!(f.row().await.unsettled);

        sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
            .bind(APP)
            .execute(f.state.store.pool())
            .await
            .unwrap();
        let answer = renew(&mut session, "msg_r2", 3).await;
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let writes = f.writes().await;
        let (_new, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        pong(&mut session, &ping).await;
        assert!(f.row().await.unsettled, "the old session settles nothing");
        assert_eq!(f.writes().await, writes);
        assert!(
            of_type(&renew(&mut session, "msg_r3", 3).await, "lease_grant").is_empty(),
            "nor answers a renewal it read after the newer session started"
        );
        f.done();
    }

    /// Section 8.4, "Settling a lease": a write of the row between the
    /// settle's answer and its `pong` refuses the settle, even when every
    /// other check passes again by the `pong`. Here the row is written
    /// `local`, marked, at the same token, while the policy is still `warm`,
    /// as R would write it if the policy left `warm` and came back. The
    /// `pong` writes nothing. The next renewal is answered and pings again,
    /// and its `pong` settles the row as it then reads.
    #[tokio::test]
    async fn a_row_written_after_the_settles_answer_is_not_settled_at_its_pong() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = ping_of(&answer);

        write_row(&f, "UPDATE leases SET mode = 'local' WHERE id = ?").await;
        let writes = f.writes().await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.unsettled),
            ("local", true),
            "the row changed since the answer: no settle"
        );
        assert_eq!(f.writes().await, writes);

        let again = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&again, "msg_r2", 3, D);
        let ping = ping_of(&again);
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.fencing_token, row.unsettled),
            ("expiring", 3, false)
        );
        assert_eq!(f.writes().await, writes + 1);
        f.done();
    }

    /// The same refusal on the ordinary path, with a duration raise between
    /// the answer and the `pong`; the settle's answer reads the row after
    /// its own raise, so that raise does not refuse it. The first answer
    /// raises the stored 10 s to `D`; a raise to 30 s then refuses its
    /// `pong`. The next renewal pings again, and its `pong` settles at 30 s.
    #[tokio::test]
    async fn a_duration_raise_after_the_settles_answer_waits_for_the_next_renewal() {
        let f = fixture("warm", "expiring", Some(10_000), true).await;
        let (mut session, _) = f.session(None).await;
        let writes = f.writes().await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert_eq!(f.writes().await, writes + 1, "the answer's own raise");
        let ping = ping_of(&answer);

        let mut w = f.state.store.begin_write().await.unwrap();
        leases::raise_duration(&mut w, LEASE, 30_000, &actor())
            .await
            .unwrap()
            .unwrap();
        w.commit().await.unwrap();
        pong(&mut session, &ping).await;
        assert!(f.row().await.unsettled, "raised since the answer");
        assert_eq!(f.writes().await, writes + 2);

        let again = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&again, "msg_r2", 3, D);
        pong(&mut session, &ping_of(&again)).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.duration_ms, row.unsettled),
            ("expiring", Some(30_000), false)
        );
        assert_eq!(f.writes().await, writes + 3);
        f.done();
    }

    /// "Settling a lease": the answer that settles a marked `expiring` row
    /// counts for its gate at the longer of its own duration and the stored
    /// one. The row stores 30 s and the answer carries `D` = 15 s, so the
    /// gate is 30 s + 2 x 3 s, where an ordinary answer counts 15 s + 3 s.
    /// The stored duration stays, and an unmarked row's answer still counts
    /// its own duration.
    #[tokio::test]
    async fn a_settling_answer_counts_its_gate_at_the_stored_duration() {
        let f = fixture("warm", "expiring", Some(30_000), true).await;
        let (mut session, _) = f.session(None).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert_eq!(of_type(&answer, "ping").len(), 1, "a settling answer");
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36)),
            "counted at the stored duration"
        );
        assert_eq!(f.row().await.duration_ms, Some(30_000));
        f.done();

        let f = fixture("warm", "expiring", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        assert_eq!(f.state.hub.authority().gate_remaining(LEASE), None);
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty(), "not a settle");
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(18)),
            "an ordinary answer counts its own duration"
        );
        f.done();
    }

    /// A settling answer that raises the stored duration reads the row's
    /// newest write after its own raise: the raise is the answer's, and its
    /// `pong` settles the row at `D`.
    #[tokio::test]
    async fn a_settles_own_raise_does_not_refuse_its_pong() {
        let f = fixture("warm", "expiring", Some(10_000), true).await;
        let (mut session, _) = f.session(None).await;
        let writes = f.writes().await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert_eq!(f.writes().await, writes + 1, "the answer's own raise");
        pong(&mut session, &ping_of(&answer)).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.duration_ms, row.unsettled),
            ("expiring", Some(D as i64), false)
        );
        assert_eq!(f.writes().await, writes + 2);
        f.done();
    }

    /// A marked `expiring` row while `policy`, renewed on a session that
    /// lists nothing: the answer at `D`, and its settling `ping`'s id.
    async fn settling_answer(policy: &str, stored: i64) -> (Fixture, Session, String) {
        let f = fixture(policy, "expiring", Some(stored), true).await;
        let (mut session, _) = f.session(None).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = ping_of(&answer);
        (f, session, ping)
    }

    /// A revoke between the settle's answer and its `pong` (a failover with
    /// the source connected, a restore, a deletion): no settle. The row stays
    /// revoked and marked, and the `pong` adds no change-log entry.
    #[tokio::test]
    async fn a_revoke_before_the_pong_refuses_the_settle() {
        let (f, mut session, ping) = settling_answer("warm", D as i64).await;
        let mut w = f.state.store.begin_write().await.unwrap();
        leases::revoke_app_lease(&mut w.tx, APP, &actor())
            .await
            .unwrap()
            .unwrap();
        w.commit().await.unwrap();
        let writes = f.writes().await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert!(row.is_revoked());
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", true));
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// A token raise between the settle's answer and its `pong`, as a fence
    /// acknowledgement or a declaration writes it (`leases::bump_tokens`):
    /// no settle, and the row keeps the raised token and the mark.
    #[tokio::test]
    async fn a_token_raise_before_the_pong_refuses_the_settle() {
        let (f, mut session, ping) = settling_answer("warm", D as i64).await;
        let mut w = f.state.store.begin_write().await.unwrap();
        let bumped = leases::bump_tokens(&mut w.tx, &[LEASE.to_string()], &f.node_id, &actor())
            .await
            .unwrap();
        assert_eq!(bumped.len(), 1);
        w.commit().await.unwrap();
        let writes = f.writes().await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!((row.fencing_token, row.unsettled), (4, true));
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// A failed fence of the lease between the settle's answer and its
    /// `pong`, recorded in the node's `fence_failure` or noted unrecorded
    /// (section 8.5 step 1): no settle. Neither writes the lease's row, so
    /// only the failed-fence check refuses it.
    #[tokio::test]
    async fn a_failed_fence_before_the_pong_refuses_the_settle() {
        for recorded in [true, false] {
            let (f, mut session, ping) = settling_answer("warm", D as i64).await;
            if recorded {
                let mut w = f.state.store.begin_write().await.unwrap();
                let failed = crate::fences::Failed {
                    lease_ids: &[LEASE.to_string()],
                    app_ids: &[APP.to_string()],
                    vm_ids: &[],
                };
                assert!(
                    crate::fences::record_failure(&mut w.tx, &f.node_id, &failed, &actor())
                        .await
                        .unwrap()
                );
                w.commit().await.unwrap();
            } else {
                f.state.hub.note_failure_recorded(&f.node_id, LEASE, false);
            }
            let writes = f.writes().await;
            pong(&mut session, &ping).await;
            let row = f.row().await;
            assert!(row.unsettled, "recorded: {recorded}");
            assert_eq!(f.writes().await, writes, "recorded: {recorded}");
            f.done();
        }
    }

    /// The settle never lowers the stored duration: the row stores 30 s,
    /// the answer carries `D` = 15 s, and after the `pong` the row still
    /// stores 30 s (section 8.4, "What the authority must enforce").
    #[tokio::test]
    async fn the_settle_never_lowers_the_duration() {
        let (f, mut session, ping) = settling_answer("warm", 30_000).await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.duration_ms, row.unsettled),
            ("expiring", Some(30_000), false)
        );
        f.done();
    }

    /// R, as `updateApp` writes it when the policy leaves `warm`: the policy
    /// and the row in one single-writer transaction. The row R wrote, `None`
    /// when it wrote nothing.
    pub(in crate::session) async fn end_renewals(f: &Fixture) -> Option<Lease> {
        let mut w = f.state.store.begin_write().await.unwrap();
        sqlx::query("UPDATE apps SET ha_policy = 'none' WHERE id = ?")
            .bind(APP)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let written = leases::end_renewals(&mut w.tx, APP, &actor())
            .await
            .unwrap();
        w.commit().await.unwrap();
        written
    }

    pub(in crate::session) async fn set_policy(f: &Fixture, policy: &str) {
        sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
            .bind(policy)
            .bind(APP)
            .execute(f.state.store.pool())
            .await
            .unwrap();
    }

    /// R ("Ending the renewals" step 1): the row reads `local`, marked, at
    /// its token, `duration_ms` kept, with one change-log entry, and
    /// `Lease.local_pending` is true. R over a row that already reads `local`
    /// with the mark writes nothing. A revoked row is not written.
    #[tokio::test]
    async fn r_writes_the_row_local_and_marked_and_keeps_the_duration() {
        let f = fixture("warm", "expiring", Some(30_000), false).await;
        let writes = f.writes().await;
        end_renewals(&f).await;
        let row = f.row().await;
        assert_eq!(
            (
                row.mode.as_str(),
                row.fencing_token,
                row.duration_ms,
                row.unsettled
            ),
            ("local", 3, Some(30_000), true)
        );
        assert_eq!(f.writes().await, writes + 1);
        let view = crate::api::leases::Lease::from_row(row, false, Vec::new()).unwrap();
        assert!(view.local_pending);

        let mut w = f.state.store.begin_write().await.unwrap();
        let again = leases::end_renewals(&mut w.tx, APP, &actor())
            .await
            .unwrap();
        w.commit().await.unwrap();
        assert_eq!(again, None, "already local and marked: nothing written");
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("local", true));
        assert_eq!(f.writes().await, writes + 1);

        let mut w = f.state.store.begin_write().await.unwrap();
        leases::revoke_app_lease(&mut w.tx, APP, &actor())
            .await
            .unwrap()
            .unwrap();
        assert!(
            leases::end_renewals(&mut w.tx, APP, &actor())
                .await
                .unwrap()
                .is_none()
        );
        w.commit().await.unwrap();
        f.done();
    }

    /// "From R until the holder has taken the grant", Renewals: after R,
    /// the holder's renewals at the row's token are answered `expiring` at
    /// that token for `min(D, stored)`, with no write, the gate counted at
    /// the stored 30 s; none without majority support; and none once the
    /// session sent the same-token `local` grant (ADR 0256), which a
    /// conversion grant lifts.
    #[tokio::test]
    async fn renewals_after_r_are_answered_with_no_write_until_the_local_grant() {
        #[derive(Debug)]
        struct NoSupport;
        impl VoterSupport for NoSupport {
            fn supports(&self, _duration_ms: u64) -> bool {
                false
            }
        }
        let f = fixture("warm", "expiring", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        end_renewals(&f).await;
        let writes = f.writes().await;
        for id in ["msg_r1", "msg_r2", "msg_r3"] {
            let answer = renew(&mut session, id, 3).await;
            assert_answer(&answer, id, 3, D);
            assert!(of_type(&answer, "ping").is_empty(), "no settle off warm");
        }
        assert_eq!(f.writes().await, writes, "these answers write nothing");
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.duration_ms, row.unsettled),
            ("local", Some(30_000), true)
        );
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36)),
            "counted at the stored duration"
        );

        f.state.hub.set_voter_support(Arc::new(NoSupport));
        assert!(of_type(&renew(&mut session, "msg_r4", 3).await, "lease_grant").is_empty());
        f.state.hub.set_voter_support(Arc::new(EverySupport));

        assert!(
            f.state
                .hub
                .note_local_grant_sent(&f.node_id, session.generation, LEASE, 3)
        );
        for id in ["msg_r5", "msg_r6"] {
            assert!(of_type(&renew(&mut session, id, 3).await, "lease_grant").is_empty());
        }
        assert_eq!(f.writes().await, writes);

        // A newer session sent no `local` grant: its renewals are answered.
        let (mut newer, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        assert_answer(&renew(&mut newer, "msg_r7", 3).await, "msg_r7", 3, D);
        f.done();
    }

    /// A row R wrote over a `local` row stores no duration (a conversion
    /// never answered): there is no shorter duration to fall back to, so a
    /// renewal gets no answer (ADR 0289), and nothing is written.
    #[tokio::test]
    async fn a_renewal_after_r_over_a_row_with_no_duration_gets_no_answer() {
        let f = fixture("warm", "local", None, false).await;
        let (mut session, _) = f.session(None).await;
        end_renewals(&f).await;
        let writes = f.writes().await;
        assert!(of_type(&renew(&mut session, "msg_r1", 3).await, "lease_grant").is_empty());
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// "While R waits", last sentence: the policy returns to `warm` on a
    /// live session before the `local` grant went out. The next renewal is
    /// answered and a `ping` follows; its `pong` settles the row `expiring`,
    /// unmarked, at the holder's token, and no `local` grant goes out.
    #[tokio::test]
    async fn a_return_to_warm_before_the_local_grant_settles_at_the_next_renewal() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        end_renewals(&f).await;
        let off = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&off, "msg_r1", 3, D);
        assert!(of_type(&off, "ping").is_empty());

        set_policy(&f, "warm").await;
        let writes = f.writes().await;
        let answer = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&answer, "msg_r2", 3, D);
        let ping = ping_of(&answer);
        assert_eq!(f.row().await.mode, "local", "not before the pong");
        assert_eq!(f.writes().await, writes);
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (
                row.mode.as_str(),
                row.fencing_token,
                row.duration_ms,
                row.unsettled
            ),
            ("expiring", 3, Some(D as i64), false)
        );
        assert_eq!(f.writes().await, writes + 1);
        f.done();
    }

    /// "Settling a lease": a settling `ping` goes out while the policy is
    /// `warm`, then R writes the row, and the policy is back on `warm` by
    /// the `pong`. Every check of the `pong` passes again but one: the row's
    /// newest write is R, not the write the answer read, so the `pong`
    /// writes nothing and the row stays `local` with the mark.
    #[tokio::test]
    async fn r_between_a_settles_ping_and_its_pong_refuses_the_settle() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = ping_of(&answer);

        let before_r = f.writes().await;
        end_renewals(&f).await;
        assert_eq!(f.writes().await, before_r + 1, "R wrote the row");
        set_policy(&f, "warm").await;
        let writes = f.writes().await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.fencing_token, row.unsettled),
            ("local", 3, true),
            "the row's newest write changed since the answer: no settle"
        );
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// The double flip. R, then back on `warm` with a renewal whose answer
    /// pings to settle, then off `warm` again before the `pong`. The second
    /// policy change writes nothing, because the row already reads `local`
    /// with the mark, so the row's newest write is still the one the `ping`
    /// recorded. The `pong` is refused because the policy is off `warm`, and
    /// the row stays `local` with the mark.
    #[tokio::test]
    async fn a_pong_after_the_policy_left_warm_again_settles_nothing() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        end_renewals(&f).await;
        let off = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&off, "msg_r1", 3, D);
        assert!(of_type(&off, "ping").is_empty(), "no settle off warm");

        set_policy(&f, "warm").await;
        let answer = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&answer, "msg_r2", 3, D);
        let ping = ping_of(&answer);
        let writes = f.writes().await;
        end_renewals(&f).await;
        assert_eq!(f.writes().await, writes, "the second R writes nothing");

        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.fencing_token, row.unsettled),
            ("local", 3, true),
            "off warm at the pong: no settle"
        );
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// A renewal whose conversion R ended, without majority support: no
    /// answer and no write, and the conversion still ends ("Changing the
    /// mode"). Once support returns, the next renewal is answered from R,
    /// with no write either.
    #[tokio::test]
    async fn an_ended_conversions_renewal_without_support_gets_no_answer() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert!(
            f.state
                .hub
                .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000)
        );
        end_renewals(&f).await;
        let writes = f.writes().await;

        f.state
            .hub
            .set_voter_support(Arc::new(crate::authority::NoVoterSupport));
        let refused = renew(&mut session, "msg_r1", 3).await;
        assert!(of_type(&refused, "lease_grant").is_empty(), "{refused:?}");
        assert_eq!(f.writes().await, writes);
        assert_eq!(f.state.hub.authority().gate_remaining(LEASE), None);
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE),
            None,
            "the policy change ended the conversion"
        );

        f.state.hub.set_voter_support(Arc::new(EverySupport));
        let answer = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&answer, "msg_r2", 3, D);
        assert_eq!(f.writes().await, writes);
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("local", true));
        f.done();
    }

    /// The unsafe direction while R waits. `failoverApp` finds the lease
    /// held while its holder is neither connected nor declared lost, for a
    /// marked row whatever its mode. A primary restart seeds the start gate
    /// `start + 30 s + 2 x 3 s`, which covers the holder's last answer, and
    /// once that gate has passed the marked row still does not move on it.
    #[tokio::test]
    async fn a_row_r_marked_never_moves_on_a_gate_across_a_restart() {
        for mode in ["expiring", "local"] {
            let f = fixture("warm", mode, Some(30_000), mode == "local").await;
            let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
            end_renewals(&f).await;
            assert_answer(&renew(&mut session, "msg_r1", 3).await, "msg_r1", 3, D);
            assert!(f.held_while_away().await.is_some(), "{mode}: lease_held");
            f.clock.advance(Duration::from_secs(36));
            assert!(f.passed());
            assert!(
                f.held_while_away().await.is_some(),
                "{mode}: not on its gate"
            );

            let (after, clock) = restart(&f).await;
            assert_eq!(
                after.hub.authority().gate_remaining(LEASE),
                Some(Duration::from_secs(36)),
                "{mode}"
            );
            let lease = f.row().await;
            let mut conn = after.store.pool().acquire().await.unwrap();
            for advance in [0, 36] {
                clock.advance(Duration::from_secs(advance));
                assert!(
                    crate::failover::held_while_away(&mut conn, after.hub.authority(), &lease)
                        .await
                        .unwrap()
                        .is_some(),
                    "{mode}: lease_held after {advance} s"
                );
            }
            assert!(after.hub.authority().passed(LEASE));
            drop(conn);
            f.done();
        }
    }

    /// A primary restart between the settle's answer and its `pong`. For a
    /// marked `expiring` row storing 30 s, the restarted primary's start gate
    /// counts `start + 30 s + 2 x 3 s`, and a failover finds the lease held
    /// whether or not that gate has passed, since the row is still marked.
    #[tokio::test]
    async fn a_restart_before_the_pong_keeps_a_marked_rows_start_gate() {
        let (f, _session, _ping) = settling_answer("warm", 30_000).await;
        let (after, clock) = restart(&f).await;
        assert_eq!(
            after.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        let lease = f.row().await;
        assert!(lease.unsettled);
        let mut conn = after.store.pool().acquire().await.unwrap();
        for advance in [0, 36] {
            clock.advance(Duration::from_secs(advance));
            assert!(
                crate::failover::held_while_away(&mut conn, after.hub.authority(), &lease)
                    .await
                    .unwrap()
                    .is_some(),
                "lease_held after {advance} s"
            );
        }
        drop(conn);
        f.done();
    }

    /// A primary restart between the settle's answer and its `pong`, for an
    /// unmarked row that reads `local`, settled from the holder's `hello`:
    /// the restarted primary seeds no gate, and a failover finds the lease
    /// held as a `local` one (`lease_held`).
    #[tokio::test]
    async fn a_restart_before_the_pong_leaves_a_local_row_held() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let (mut session, _) = f.session(Some(("uncertain", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        ping_of(&answer);
        let (after, _clock) = restart(&f).await;
        assert_eq!(after.hub.authority().gate_remaining(LEASE), None);
        let lease = f.row().await;
        assert_eq!((lease.mode.as_str(), lease.unsettled), ("local", false));
        let mut conn = after.store.pool().acquire().await.unwrap();
        assert!(
            crate::failover::held_while_away(&mut conn, after.hub.authority(), &lease)
                .await
                .unwrap()
                .is_some()
        );
        drop(conn);
        f.done();
    }

    /// Policy `hot` is read as `warm` (section 8.4, as `grant_app_lease`
    /// reads it): a marked `expiring` row's renewal is answered, a `ping`
    /// follows, and its `pong` settles the row.
    #[tokio::test]
    async fn a_hot_apps_marked_row_is_settled_as_a_warm_ones() {
        let (f, mut session, ping) = settling_answer("hot", D as i64).await;
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", false));
        f.done();
    }

    /// The one settling `ping` among `frames`: its message id.
    pub(in crate::session) fn ping_of(frames: &[Value]) -> String {
        let pings = of_type(frames, "ping");
        assert_eq!(pings.len(), 1, "{frames:?}");
        pings[0]["id"].as_str().unwrap().to_string()
    }

    pub(in crate::session) fn actor() -> Actor {
        Actor {
            kind: ActorType::System,
            id: "test".to_string(),
        }
    }

    /// Runs `update` (bound to the lease's id) on the row and appends its
    /// change-log entry, in one single-writer transaction, as any write of
    /// the row does.
    pub(in crate::session) async fn write_row(f: &Fixture, update: &str) {
        let mut w = f.state.store.begin_write().await.unwrap();
        sqlx::query(update)
            .bind(LEASE)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let lease = leases::by_id(&mut w.tx, LEASE).await.unwrap().unwrap();
        changelog::append(
            &mut w.tx,
            &actor(),
            "lease",
            LEASE,
            Op::Update,
            lease.payload(),
        )
        .await
        .unwrap();
        w.commit().await.unwrap();
    }

    fn row(mode: &str, token: i64, unsettled: bool) -> Lease {
        Lease {
            id: LEASE.to_string(),
            resource_type: "app".to_string(),
            resource_id: APP.to_string(),
            holder_node_id: "nod_a".to_string(),
            mode: mode.to_string(),
            fencing_token: token,
            duration_ms: Some(15_000),
            granted_at: "2026-11-10T10:00:00Z".to_string(),
            renewed_at: None,
            expires_at: None,
            revoked_at: None,
            unsettled,
        }
    }

    /// Every branch of the plan, the crossing renewal's included.
    #[test]
    fn the_plan_follows_the_row_the_conversion_and_the_settle() {
        let conv = InFlight {
            token: 3,
            duration_ms: 15_000,
        };
        let local = row("local", 3, false);
        let marked_local = row("local", 3, true);
        let expiring = row("expiring", 3, false);
        let marked = row("expiring", 3, true);
        let unanswered = |plan: Plan| matches!(plan, Plan::Unanswered(_));
        let p = |row: &Lease, token, in_flight, reported, warm| {
            plan(
                row,
                token,
                Seen {
                    in_flight,
                    reported,
                    local_sent: None,
                },
                warm,
            )
        };
        // The first answer, and only at the conversion's token.
        assert_eq!(p(&local, 3, Some(conv), None, true), Plan::Converted(conv));
        assert!(unanswered(p(&local, 4, Some(conv), None, true)));
        assert!(unanswered(p(&local, 3, None, None, true)), "no conversion");
        // Ended by the policy change (section 8.4 "Changing the mode").
        assert_eq!(
            p(&marked_local, 3, Some(conv), None, true),
            Plan::Ended(conv)
        );
        assert_eq!(p(&local, 3, Some(conv), None, false), Plan::Ended(conv));
        // Neither ends a conversion at a token the row does not hold: the
        // plan falls through to the later arms.
        let below = row("local", 2, false);
        let marked_below = row("local", 2, true);
        assert!(unanswered(p(&marked_below, 3, Some(conv), None, true)));
        assert!(unanswered(p(&below, 3, Some(conv), None, false)));
        assert_eq!(p(&marked_below, 3, Some(conv), Some(3), true), Plan::Settle);
        // The settle from `hello`, at the holder's token.
        assert_eq!(p(&local, 3, None, Some(3), true), Plan::Settle);
        assert_eq!(p(&marked, 5, None, Some(5), true), Plan::Settle);
        assert!(unanswered(p(&local, 3, None, Some(3), false)), "off warm");
        assert!(unanswered(p(&local, 4, None, Some(4), true)), "unmarked");
        assert!(unanswered(p(&expiring, 5, None, Some(5), true)));
        // Ordinary answers, settling a marked row while `warm`.
        assert_eq!(
            p(&expiring, 3, None, None, true),
            Plan::Ordinary { settle: false }
        );
        assert_eq!(
            p(&marked, 3, None, None, true),
            Plan::Ordinary { settle: true }
        );
        assert_eq!(
            p(&marked, 3, None, None, false),
            Plan::Ordinary { settle: false }
        );
        assert!(unanswered(p(&expiring, 4, None, None, true)));
        // From R (section 8.4, "From R until the holder has taken the
        // grant"): a marked row that reads `local` at the renewal's token is
        // answered off `warm`, and settled on `warm` ("While R waits").
        assert_eq!(p(&marked_local, 3, None, None, false), Plan::FromR);
        assert_eq!(p(&marked_local, 3, None, None, true), Plan::Settle);
        assert!(unanswered(p(&marked_local, 4, None, None, false)));
        assert!(unanswered(p(&marked_below, 3, None, None, false)));
        // Once the session sent the `local` grant, nothing at its token or
        // below is answered (ADR 0256); a higher token answers a later grant.
        let sent = |row: &Lease, token, in_flight, warm| {
            plan(
                row,
                token,
                Seen {
                    in_flight,
                    reported: Some(token),
                    local_sent: Some(3),
                },
                warm,
            )
        };
        assert!(unanswered(sent(&marked_local, 3, None, false)));
        assert!(unanswered(sent(&marked_local, 3, None, true)));
        assert!(unanswered(sent(&marked_local, 3, Some(conv), false)));
        assert!(unanswered(sent(&expiring, 3, None, true)));
        assert!(unanswered(sent(&marked_below, 2, None, false)));
        let above = row("expiring", 4, false);
        assert_eq!(
            sent(&above, 4, None, true),
            Plan::Ordinary { settle: false }
        );
    }
}
