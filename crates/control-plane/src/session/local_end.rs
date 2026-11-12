// SPDX-License-Identifier: AGPL-3.0-only
//! The same-token `local` grant that ends an `expiring` lease's renewals,
//! its barrier `ping`, the `pong` after it, and its resend from `hello`
//! (`node-agent-protocol.md` section 8.4, "Ending the renewals" steps 2 and
//! 3, "Grants of a marked row", "The mark" and "The holder's next `hello`";
//! ADR 0255, ADR 0257, ADR 0261, ADR 0280; issue #555).
//!
//! - **Held as a marked row's grant.** R ([`leases::end_renewals`]) leaves
//!   the row `local` and marked. Its `local` grant is held in the held-grant
//!   registry ([`super::held`], [`Kind::LocalEnd`]). It goes out once the
//!   row's newest write is replicated over the required set and a majority
//!   supports `D`. Under the writer gate the send finds again: the session
//!   current and ready, the row naming the holder at the held token,
//!   unrevoked, marked and reading `local`, the policy off `warm`, and no
//!   failed fence or pending acknowledgement. It checks no gate and drops
//!   none: the holder keeps its lease and its token.
//! - **After every policy change off `warm`.** The grant is held whenever
//!   the row, after R, is unrevoked, marked, reads `local`, and its newest
//!   write gave it no new token ([`leases::leave_warm`]), whether or not R
//!   wrote it: a return to `warm` and back before any settle leaves the row
//!   as R left it, and still ends the renewals. A row whose newest write gave
//!   it a new token has its own grant held ("Grants of a marked row"): that
//!   grant stays, and a grant with a new token never goes out through this
//!   path, which checks no gate. When R turns a row that reads `expiring`
//!   into `local`, a grant with a new token held for it and not sent "is
//!   dropped when R commits" ([`NodeHub::end_renewals_committed`]), and R's
//!   own transaction records `app.left_stopped` for it
//!   ([`NodeHub::record_left_stopped`]).
//! - **Evidence.** "Ending the renewals" step 2: the primary "sends it only
//!   on a session on which the holder showed that it holds the lease at the
//!   row's token: its `hello` listed the lease `held` or `uncertain` at that
//!   token, in either mode, or it renewed the lease at that token on that
//!   session. Otherwise a node whose highest token for the app is lower
//!   would take the grant as a new grant (section 5.12), so the primary
//!   waits for such a session". The evidence is kept per session
//!   ([`SessionState`]), so none carries over to a later session. Over a
//!   lease the node holds `local` at that token the grant changes nothing
//!   (section 5.12), and its `pong` clears the mark.
//! - **What the API shows.** "Grants of a marked row": the grant "is not
//!   reported as a wait while it waits for a session on which its holder
//!   shows that it holds the lease at the row's token": `grant_pending`
//!   stays false for it, `grant_waiting_on` lists nothing for it, and no
//!   `failoverApp` answers `in_progress` for it. "While the holder's current
//!   session is such a session, `grant_pending` is true until the grant is
//!   sent on it, and `grant_waiting_on` lists the machines of the required
//!   set that have not acknowledged R. When that session ends first, the
//!   grant again waits for such a session and is not pending"
//!   ([`super::held`]'s `HeldGrant::pending`). "When the policy returns to
//!   `warm`, the grant is dropped", as every unsent one is.
//! - **The barrier.** The grant and a `ping` are queued together on the
//!   session, and recorded there in the same hold of the sessions lock, only
//!   once both are queued. The session then answers no renewal of the lease
//!   at that token (ADR 0256; [`super::conversion`]), and every settle of
//!   the lease pending on the session is cleared, whether planned from its
//!   `hello` or waiting for its `pong` ("Settling a lease"); the settle's
//!   row write also refuses a settling `ping` at or below the grant's
//!   token. The `pong` of that `ping` with no refusal before it
//!   writes the row `local` at the token and clears the mark, as a `local`
//!   report does, if the row's newest write is still the one recorded with
//!   the `ping` and every other check holds again. It also passes ADR 0257's
//!   barrier for the lease on that session: a conversion grant may follow
//!   ([`super::convert`]). A barrier that passed on the
//!   session before this grant no longer counts, nor does the `pong` of an
//!   older grant's `ping`: only the newest grant's `pong` passes it. A later
//!   policy change off `warm` on the same session holds the grant afresh, so
//!   a grant sent there before is no reason to send none. The session-start
//!   `local` repeat ([`Session::send_grants`]) queues the same `ping`; there a `hello`
//!   that listed the lease `local` at the token stands in for its `pong`.
//! - **The refusal.** A refusal of the grant arrives before the `pong`
//!   (section 4.2). It drops the `ping`, so the `pong` writes nothing and
//!   passes no barrier. A `bad_message` refusal runs the regrant of "The
//!   holder fenced first" ([`Session::regrant`]) only "on a session whose
//!   `hello` listed the lease `held` or `uncertain` at that token" (section
//!   8.5); with a renewal as the only evidence that condition does not
//!   hold, so no regrant runs.
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
use crate::events::{self, NewEvent, Source};
use crate::leases::{self, Lease};
use quoro_proto::wire;
use serde_json::json;
use std::collections::HashMap;

/// The `updateApp` call whose policy change off `warm` writes R: what
/// `app.left_stopped` carries of it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PolicyChange<'a> {
    /// The app's project.
    pub project_id: &'a str,
    /// The call's request id.
    pub request_id: &'a str,
    /// The caller of `updateApp`.
    pub actor: &'a Actor,
}

/// The grant with a new token that R drops as it commits, which
/// `app.left_stopped` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LeftStopped {
    /// The node the dropped grant named.
    pub node_id: String,
    /// The dropped grant's token.
    pub fencing_token: i64,
}

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
    /// 0261) until a later grant of the lease is sent.
    listed_local: HashMap<String, u64>,
    /// The token at which the session's `hello` listed each lease `held` or
    /// `uncertain`, in either mode: evidence that the holder holds it there,
    /// and section 8.5's condition for the regrant after a `bad_message`
    /// refusal.
    listed: HashMap<String, u64>,
    /// The token at which the holder renewed each lease on the session:
    /// evidence that it holds it `expiring` there.
    renewed: HashMap<String, u64>,
}

impl SessionState {
    /// Whether this is a session on which the holder showed that it holds
    /// `lease_id` at `token` ("Ending the renewals" step 2): "its `hello`
    /// listed the lease `held` or `uncertain` at that token, in either mode,
    /// or it renewed the lease at that token on that session". The state is
    /// the session's own, so nothing shown on an earlier session counts.
    fn evidence(&self, lease_id: &str, token: u64) -> bool {
        self.listed.get(lease_id) == Some(&token) || self.renewed.get(lease_id) == Some(&token)
    }

    /// Whether ADR 0257's barrier has passed for `lease_id` at `token` on
    /// the session: the `pong` of the `ping` after its newest same-token
    /// `local` grant arrived with no refusal before it, or the session's
    /// `hello` listed the lease `local` at that token, which stands in for
    /// it (ADR 0261).
    pub(super) fn barrier_passed(&self, lease_id: &str, token: u64) -> bool {
        self.passed.get(lease_id) == Some(&token) || self.listed_local.get(lease_id) == Some(&token)
    }
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
    /// Records what `generation`'s `hello` listed `held` or `uncertain`:
    /// every such lease, and apart those it holds `local`.
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
            if lease.mode == quoro_proto::LeaseMode::Local {
                handle
                    .local_end
                    .listed_local
                    .insert(lease.lease_id.clone(), lease.fencing_token);
            }
            handle
                .local_end
                .listed
                .insert(lease.lease_id.clone(), lease.fencing_token);
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
            handle.local_end.renewed.insert(lease_id.to_string(), token);
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

    /// The grant with a new token that R drops as it commits, read in R's
    /// transaction (`ended`), while it holds the writer gate: R turned a row
    /// that read `expiring` into `local`, and the registry holds a grant
    /// with a new token for that row, naming it (holder and token), not sent
    /// and not dropped already ("Ending the renewals": "a grant with a new
    /// token that the primary holds for that row and has not sent … is
    /// dropped when R commits"). A grant goes out only under the writer
    /// gate, so none can be sent between this read and R's commit. The
    /// registry lock is held only for the read.
    pub(crate) fn left_stopped(&self, ended: &leases::Ended) -> Option<LeftStopped> {
        if !(ended.wrote && ended.from_expiring) {
            return None;
        }
        let lease = &ended.lease;
        let registry = self.0.held.lock().expect("held grant lock");
        registry
            .entries
            .get(&lease.id)
            .filter(|held| {
                held.kind == Kind::NewToken
                    && held.sent_on.is_none()
                    && !held.grant_dropped
                    && held.names(lease)
            })
            .map(|held| LeftStopped {
                node_id: held.holder.clone(),
                fencing_token: held.token,
            })
    }

    /// Records `app.left_stopped` on `conn`, R's own transaction, when R
    /// drops a grant as it commits ([`NodeHub::left_stopped`]), and returns
    /// that grant. "The primary emits `app.left_stopped` as it drops the
    /// grant", and `events.schema.json` has it emitted "right after the
    /// change of the policy commits": written in the transaction that writes
    /// R and the policy, it commits with that change, so no drop after the
    /// commit can lose it, and a failed commit writes neither. The actor,
    /// request id and project are those of the `updateApp` call (`by`).
    ///
    /// # Errors
    ///
    /// A database error.
    pub(crate) async fn record_left_stopped(
        &self,
        conn: &mut sqlx::SqliteConnection,
        ended: &leases::Ended,
        by: &PolicyChange<'_>,
    ) -> Result<Option<LeftStopped>, ApiError> {
        let Some(dropped) = self.left_stopped(ended) else {
            return Ok(None);
        };
        let app_id = &ended.lease.resource_id;
        events::emit(
            conn,
            NewEvent {
                kind: "app.left_stopped",
                source: Source::ControlPlane,
                subject_type: "app",
                subject_id: app_id,
                project_id: Some(by.project_id),
                request_id: Some(by.request_id),
                actor: Some(by.actor),
                data: json!({
                    "app_id": app_id,
                    "node_id": dropped.node_id,
                    "fencing_token": dropped.fencing_token,
                    "reason": "policy_left_warm",
                }),
            },
        )
        .await?;
        Ok(Some(dropped))
    }

    /// What follows a policy change off `warm` once `updateApp` committed it
    /// ("Ending the renewals"). It runs in the poll that commits, with no
    /// await, so a caller that goes away after the commit cannot skip it.
    ///
    /// - **The drop.** When R wrote the row, its newest write gives no new
    ///   token any more: "a grant with a new token that the primary holds
    ///   for that row and has not sent … is dropped when R commits". It goes
    ///   now, whether or not its holder is connected, so it no longer reads
    ///   as pending. A grant held since for a newer token stays. R's
    ///   transaction already recorded `app.left_stopped` for it
    ///   ([`NodeHub::record_left_stopped`]); nothing is recorded here.
    /// - **A held-back revoke.** An entry whose step-4 revoke is held back
    ///   for support keeps that revoke: section 5.1 revokes the lease its
    ///   holder listed at a lower token in the other mode, whatever R
    ///   writes. Its grant is dropped all the same: the entry is marked
    ///   ([`HeldGrant::grant_dropped`]), so the grant never goes out and
    ///   never reads as pending, and once the revoke goes out the next
    ///   attempt drops the entry ([`NodeHub::attempt_held`]). No `local`
    ///   grant is held over it.
    ///
    /// Then the same-token `local` grant is held when [`leases::leave_warm`]
    /// found the row needs it.
    pub(crate) fn end_renewals_committed(&self, ended: &leases::Ended) {
        let lease = &ended.lease;
        if ended.wrote {
            let dropped = {
                let mut registry = self.0.held.lock().expect("held grant lock");
                match registry.entries.get_mut(&lease.id) {
                    Some(held)
                        if held.kind == Kind::NewToken
                            && held.sent_on.is_none()
                            && !held.grant_dropped
                            && held.token <= lease.fencing_token =>
                    {
                        if held.revoke_first.is_some() {
                            held.grant_dropped = true;
                            None
                        } else {
                            registry.entries.remove(&lease.id)
                        }
                    }
                    _ => None,
                }
            };
            if let Some(dropped) = &dropped {
                tracing::info!(
                    lease_id = %lease.id,
                    token = dropped.token,
                    "R gave the row no new token: its held grant with a new token is dropped"
                );
                self.release_held_revoke(dropped);
            }
        }
        if ended.hold {
            self.hold_local_end(lease);
        }
    }

    /// Holds the `local` grant that ends the renewals of `lease`, a row that
    /// reads `local` with the mark and whose newest write gave it no new
    /// token, as `updateApp` commits a policy change off `warm`. It may go out on
    /// the holder's current session when that session has evidence that the
    /// holder holds the lease at the row's token; otherwise it waits for
    /// such evidence: a renewal, or the holder's next `hello`.
    fn hold_local_end(&self, lease: &Lease) {
        let token = u64::try_from(lease.fencing_token).unwrap_or(0);
        let evidenced = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(&lease.holder_node_id)
                .filter(|handle| {
                    handle.hello_recorded && handle.local_end.evidence(&lease.id, token)
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

    /// Holds the grant afresh, eligible on `eligible`: a `local` grant held
    /// or sent before for the same row is replaced, since the policy left
    /// `warm` again, the row was written since (the holder's token before
    /// the resend) or the session is new, so a grant sent before is no
    /// reason to send none. A
    /// grant held for a newer token stays, and so does a grant with a new
    /// token not sent yet for this very row ("Grants of a marked row"): it is
    /// the grant the row waits for, and it goes out through its own path,
    /// gated.
    fn register_local_end(&self, lease: &Lease, eligible: Option<u64>) {
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
                    let mut held = HeldGrant::of(lease, eligible);
                    held.kind = Kind::LocalEnd;
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
                "a grant with a new token is held for the row; no local grant is held over it"
            );
            return;
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

    /// Whether the held `local` grant `held` of `lease_id`, not sent yet,
    /// has nothing left to end: the row no longer names it unrevoked,
    /// marked and `local`, or the policy is back on `warm` ("Ending the
    /// renewals" step 2: "while the policy is still off `warm`"). Read for
    /// every unsent entry, eligible or not, so one that waits for evidence
    /// is dropped too ([`NodeHub::attempt_held`]).
    ///
    /// # Errors
    ///
    /// A database error.
    pub(super) async fn local_end_obsolete(
        &self,
        lease_id: &str,
        held: &HeldGrant,
    ) -> Result<bool, ApiError> {
        let mut conn = self.0.store.pool().acquire().await?;
        let Some(row) = leases::by_id(&mut conn, lease_id).await? else {
            return Ok(true);
        };
        if !held.names(&row) || row.is_revoked() || !row.unsettled || row.mode != "local" {
            return Ok(true);
        }
        super::conversion::app_is_warm(&mut conn, &row.resource_id).await
    }

    /// Whether `generation`'s `hello` listed `lease_id` `held` or
    /// `uncertain` at `token`, in either mode.
    pub(super) fn hello_listed(
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
            .is_some_and(|handle| handle.local_end.listed.get(lease_id) == Some(&token))
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
        // Section 8.5: a `bad_message` refusal gives the lease back only on a
        // session whose `hello` listed it `held` or `uncertain` at the token.
        let regrant = self.hello_listed(&row.holder_node_id, generation, lease_id, token);
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
            regrant,
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

    /// Queues `frames` on `generation`'s channel, the same-token `local`
    /// grant `grant_id` of `barrier`'s lease and its barrier `ping`, and
    /// records both on the session, in one hold of the sessions lock that
    /// finds `generation` still the node's session. `frames` is empty when
    /// the caller pushes them to the session's own outbox right after this
    /// returns, with no await between ([`Session::queue_local_grant`]).
    ///
    /// The record is made only once every frame is queued: a refused send
    /// means the session's receiver is gone, and nothing queued on it
    /// reaches the wire. It sets the session's `local_sent` (ADR 0256: no
    /// renewal of the lease at that token is answered on the session from
    /// then on) and clears every settle of the lease pending on the session
    /// ("Settling a lease": "Sending the same-token `local` grant clears any
    /// pending settle for that lease"). Every reader of these takes this
    /// lock, so none sees the frames queued without the record. `regrant`
    /// says whether a `bad_message` refusal of the grant gives the lease
    /// back; `fresh`, whether the grant is sent after the session start, so
    /// a barrier that passed before it no longer counts. Returns whether the
    /// grant was queued and recorded.
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
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        if !frames
            .iter()
            .all(|frame| handle.tx.send(frame.clone()).is_ok())
        {
            return false;
        }
        let sent = handle
            .local_sent
            .entry(barrier.lease_id.clone())
            .or_insert(0);
        *sent = (*sent).max(barrier.token);
        handle.settles.forget(&barrier.lease_id);
        // A conversion of the lease in flight on the session ends here: the
        // node takes this grant after it, so no renewal it sends from now on
        // is a first renewal after the conversion (ADR 0256), and none is
        // answered at that token ([`super::conversion`]). A refusal of that
        // conversion grant, still on its way, finds nothing to drop.
        handle.conversions.remove(&barrier.lease_id);
        handle
            .conversion_grants
            .retain(|_, (lease_id, _)| *lease_id != barrier.lease_id);
        if fresh {
            // A grant sent after the session start has its own barrier: one
            // that passed before it, by a `pong` or the `hello`'s listing,
            // no longer counts. The session start's repeat keeps the
            // `hello`'s: the node held the lease `local` when it sent it.
            handle.local_end.passed.remove(&barrier.lease_id);
            handle.local_end.listed_local.remove(&barrier.lease_id);
        }
        // ADR 0257: only the newest grant's `pong` passes the barrier. An
        // older grant's `ping` of the lease, still unanswered, no longer
        // stands for it, and its `pong` writes nothing.
        handle
            .local_end
            .pings
            .retain(|_, older| older.lease_id != barrier.lease_id);
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
        true
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
    /// 0261). A conversion grant of the lease may go out only then
    /// ([`super::convert`]).
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
            .is_some_and(|handle| handle.local_end.barrier_passed(lease_id, token))
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

    /// The session start's same-token `local` grant of `lease` at `token`
    /// and its barrier `ping` (ADR 0257), pushed to `outbox` only once
    /// [`NodeHub::queue_local_end`] recorded them on this session, with no
    /// await between, so the record and the frames go together: a session
    /// that is no longer the node's pushes neither. `regrant` says whether a
    /// refusal of the grant gives the lease back.
    pub(super) async fn queue_local_grant(
        &self,
        lease: &Lease,
        token: u64,
        regrant: bool,
        outbox: &mut Vec<String>,
    ) {
        let lease_id = lease.id.as_str();
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
        let grant_id = new_msg_id();
        let ping_id = new_msg_id();
        if self.hub.queue_local_end(
            &self.node_id,
            self.generation,
            &Barrier {
                lease_id: lease_id.to_string(),
                token,
                seq,
            },
            &grant_id,
            regrant,
            false,
            &ping_id,
            &[],
        ) {
            outbox.push(
                ControlMessage::LeaseGrant(lease.grant())
                    .to_frame(&grant_id, None)
                    .to_text(),
            );
            outbox.push(ControlMessage::Ping.to_frame(&ping_id, None).to_text());
        }
    }

    /// A `pong` that is no settle's: when it answers a barrier `ping` with no
    /// refusal of its grant before it, the barrier passes, and the row is
    /// written `local` at the token with the mark cleared ("The mark", the
    /// `pong` bullet), if every check holds again ([`Session::write_pong`]).
    /// The session's settles of the lease went when the grant was queued
    /// ([`NodeHub::queue_local_end`]).
    pub(super) async fn on_barrier_pong(&mut self, re: &str) {
        let Some(barrier) = self.hub.take_barrier(&self.node_id, self.generation, re) else {
            return;
        };
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
        // The barrier passed: a conversion grant held for the lease may go
        // out now ([`super::convert`]).
        self.hub.wake_held();
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
pub(super) mod tests {
    use super::super::conversion::tests::{
        APP, EverySupport, Fixture, LEASE, actor, assert_answer, end_renewals, fixture, frames,
        hello, of_type, ping_of, pong, renew, restart, set_policy, write_row,
    };
    use super::Kind;
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
    pub(in crate::session) struct NoSupport;
    impl VoterSupport for NoSupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            false
        }
    }

    /// A session of the fixture's node on `state` whose `hello` lists the
    /// lease as `listed`, past its start, keeping the channel the hub queues
    /// on and the grants and `ping`s its start queued on its outbox.
    pub(in crate::session) struct Live {
        pub(in crate::session) session: Session,
        pub(in crate::session) rx: mpsc::UnboundedReceiver<String>,
        pub(in crate::session) start: Vec<Value>,
    }

    pub(in crate::session) async fn live(
        state: &AppState,
        f: &Fixture,
        listed: Option<(&str, LeaseMode, u64)>,
    ) -> Live {
        let hub = &state.hub;
        let (generation, rx, _closed, _changed) = hub.attach(&f.node_id);
        let mut session = Session::new(hub.clone(), f.node_id.clone(), generation);
        let mut outbox = Vec::new();
        let text = NodeMessage::Hello(hello(&f.node_id, listed))
            .to_frame("msg_hello", None)
            .to_text();
        assert!(!session.handle_text(&text, &mut outbox).await);
        hub.mark_start_done(&f.node_id, generation);
        let start = frames(outbox)
            .into_iter()
            .filter(|frame| frame["type"] == "lease_grant" || frame["type"] == "ping")
            .collect();
        Live { session, rx, start }
    }

    /// The grants and `ping`s the hub queued on the session's channel.
    pub(in crate::session) fn queued(live: &mut Live) -> Vec<Value> {
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
    pub(in crate::session) async fn created(f: &Fixture) {
        write_row(
            f,
            "UPDATE leases SET fencing_token = fencing_token WHERE id = ?",
        )
        .await;
    }

    /// The node acknowledges the lease's newest write on `live`.
    pub(in crate::session) async fn ack(state: &AppState, f: &Fixture, live: &Live) {
        let mut conn = state.store.pool().acquire().await.unwrap();
        let seq = crate::leases::newest_write_seq(&mut conn, LEASE)
            .await
            .unwrap()
            .unwrap();
        state
            .hub
            .count_log_ack(&f.node_id, live.session.generation, seq);
    }

    /// A policy change off `warm` as `updateApp` commits it: R, what follows
    /// it and `app.left_stopped` in the transaction, then the hub's part
    /// once it commits.
    pub(in crate::session) async fn r(f: &Fixture) {
        r_on(&f.state, f).await;
    }

    /// [`r`] on the primary `state`, opened on the fixture's database.
    async fn r_on(state: &AppState, f: &Fixture) {
        if f.writes().await == 0 {
            created(f).await;
        }
        let mut w = state.store.begin_write().await.unwrap();
        sqlx::query("UPDATE apps SET ha_policy = 'none' WHERE id = ?")
            .bind(APP)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let ended = crate::leases::leave_warm(&mut w.tx, APP, &actor())
            .await
            .unwrap();
        if let Some(ended) = &ended {
            state
                .hub
                .record_left_stopped(&mut w.tx, ended, &by(&actor()))
                .await
                .unwrap();
        }
        w.commit().await.unwrap();
        if let Some(ended) = ended {
            state.hub.end_renewals_committed(&ended);
        }
    }

    /// The `updateApp` call of [`r`]: the fixture's project, a request id
    /// and `actor`.
    fn by(actor: &crate::changelog::Actor) -> super::PolicyChange<'_> {
        super::PolicyChange {
            project_id: PROJECT,
            request_id: REQUEST,
            actor,
        }
    }

    /// The fixture's project, and the request id of [`r`].
    const PROJECT: &str = "prj_c";
    const REQUEST: &str = "req_left_warm";

    /// Every `app.left_stopped` stored, oldest first, as (subject, project,
    /// request id, actor, data).
    async fn left_stopped(f: &Fixture) -> Vec<(Value, String, String, Value, Value)> {
        let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT subject, project_id, request_id, actor, data FROM events \
             WHERE type = 'app.left_stopped' ORDER BY seq",
        )
        .fetch_all(f.state.store.pool())
        .await
        .unwrap();
        rows.into_iter()
            .map(|(subject, project, request, actor, data)| {
                (
                    serde_json::from_str(&subject).unwrap(),
                    project,
                    request,
                    serde_json::from_str(&actor).unwrap(),
                    serde_json::from_str(&data).unwrap(),
                )
            })
            .collect()
    }

    /// The one `app.left_stopped` that the drop of the grant with a new
    /// token held for the fixture's node at `token` emits, from [`r`].
    fn the_event(f: &Fixture, token: u64) -> (Value, String, String, Value, Value) {
        (
            serde_json::json!({ "type": "app", "id": APP }),
            PROJECT.to_string(),
            REQUEST.to_string(),
            serde_json::json!({ "type": "system", "id": "test" }),
            serde_json::json!({
                "app_id": APP,
                "node_id": f.node_id,
                "fencing_token": token,
                "reason": "policy_left_warm",
            }),
        )
    }

    pub(in crate::session) async fn view(
        state: &AppState,
        f: &Fixture,
    ) -> (bool, Vec<String>, bool) {
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
    pub(in crate::session) fn grant_then_ping(frames: &[Value], token: u64) -> (String, String) {
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

    pub(in crate::session) async fn state(f: &Fixture) -> (String, i64, Option<i64>, bool, bool) {
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
        f.done().await;
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
        f.done().await;
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
        f.done().await;

        let (f, mut on, _grant, ping) = sent("warm", false).await;
        write_row(&f, "UPDATE leases SET duration_ms = 20000 WHERE id = ?").await;
        let writes = f.writes().await;
        pong(&mut on.session, &ping).await;
        assert!(f.row().await.unsettled, "written since the grant");
        assert_eq!(f.writes().await, writes);
        f.done().await;

        let (f, mut on, _grant, ping) = sent("warm", false).await;
        let _newer = live(&f.state, &f, None).await;
        let writes = f.writes().await;
        pong(&mut on.session, &ping).await;
        assert!(f.row().await.unsettled, "a replaced session");
        assert_eq!(f.writes().await, writes);
        f.done().await;
    }

    /// A settle `ping` sent before the `local` grant never settles the lease
    /// once the session sent that grant, even after the policy returned to
    /// `warm`. A marked `expiring` row (a failover's grant, say) while
    /// `warm`: a renewal is answered and its settle `ping` sent. The policy
    /// leaves `warm`: R writes the row and its grant goes out; back on
    /// `warm`, the settle's `pong` writes nothing, and no renewal is
    /// answered. With no write of the row between the settle's answer and
    /// the grant (the policy back on `warm` and off it again before an
    /// attempt read the held grant), the session's record of the grant,
    /// which the send makes, refuses the settle on its own.
    #[tokio::test]
    async fn a_settle_ping_from_before_the_local_grant_never_settles_after_it() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let settle_ping = ping_of(&answer);
        r(&f).await;
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
        f.done().await;

        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        assert!(end_renewals(&f).await.is_some());
        set_policy(&f, "warm").await;
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let settle_ping = ping_of(&answer);
        let writes = f.writes().await;
        assert!(
            f.state
                .hub
                .note_local_grant_sent(&f.node_id, on.session.generation, LEASE, 3)
        );
        pong(&mut on.session, &settle_ping).await;
        assert!(
            f.row().await.unsettled,
            "no write since, refused all the same"
        );
        assert_eq!(f.writes().await, writes);
        f.done().await;
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
        f.done().await;
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
        f.done().await;
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
            f.done().await;
        }
    }

    /// Evidence, and the session lost while R waits. R on a live session
    /// whose `hello` did not list the lease (it was granted after the
    /// `hello`): the grant waits, shown as no grant pending, until a renewal
    /// at the row's token shows the holder holds the lease `expiring` there,
    /// then goes out. A session lost before the
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
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert_answer(&renew(&mut on.session, "msg_r1", 3).await, "msg_r1", 3, D);
        f.state.hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        f.done().await;

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
        f.done().await;
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
        f.done().await;
    }

    /// The holder's refusal `code` of the grant `grant` on `session`.
    pub(in crate::session) async fn refuse(session: &mut Session, grant: &str, code: ErrorCode) {
        let refusal = NodeMessage::Error(ProtocolError {
            code,
            message: "refused".to_string(),
            fatal: false,
            term: None,
        })
        .to_frame("msg_refusal", Some(grant))
        .to_text();
        let mut outbox = Vec::new();
        session.handle_text(&refusal, &mut outbox).await;
    }

    /// A holder whose `hello` listed the lease `local` at the row's token
    /// holds it so, and a same-token `local` grant changes nothing on it
    /// (section 5.12). R over that row (a `warm` app's `local` lease) marks
    /// it, and the grant goes out on that session once R is acknowledged,
    /// then its `ping`. The `pong` clears the mark: no grant pending, none
    /// waiting, and `local_pending` false.
    #[tokio::test]
    async fn a_holder_listed_local_gets_the_local_grant_and_its_pong_clears_the_mark() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        created(&f).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        r(&f).await;
        assert!(f.row().await.unsettled);
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        let (_grant, ping) = grant_then_ping(&queued(&mut on), 3);
        pong(&mut on.session, &ping).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), false));
        f.done().await;
    }

    /// A `local` grant that waits for evidence (the holder's `hello` did not
    /// list the lease, and it has not renewed it) is no grant pending: no
    /// `grant_pending`, nothing in `grant_waiting_on`, and `failoverApp`'s
    /// `in_progress` check finds nothing. Back on `warm`, the entry is
    /// dropped though it was never eligible, and the holder's next renewal
    /// settles the lease with no `local` grant.
    #[tokio::test]
    async fn a_local_grant_waiting_for_evidence_is_not_pending_and_goes_on_warm() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, None).await;
        r(&f).await;
        ack(&f.state, &f, &on).await;
        let hub = &f.state.hub;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no evidence");
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert!(!hub.row_grant_pending(&f.row().await));

        set_policy(&f, "warm").await;
        hub.attempt_held().await;
        assert!(
            !hub.0.held.lock().unwrap().entries.contains_key(LEASE),
            "dropped back on warm"
        );
        assert!(!hub.row_grant_pending(&f.row().await));
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "no local grant");
        pong(&mut on.session, &ping_of(&answer)).await;
        assert_eq!(
            state(&f).await,
            ("expiring".to_string(), 3, Some(D as i64), false, false)
        );
        f.done().await;
    }

    /// The held grant of the row at token 4 went out, through the path of
    /// a grant with a new token: `local` at 4, with no `ping` after it. The
    /// hub's own held-grant task may attempt alongside the test's, so the
    /// frames are checked, not counted.
    fn new_token_grants(frames: &[Value]) {
        assert!(!frames.is_empty(), "the grant went out");
        for frame in frames {
            assert_eq!(frame["type"], "lease_grant", "{frames:?}");
            assert_eq!(frame["body"]["mode"], "local", "{frames:?}");
            assert_eq!(frame["body"]["fencing_token"], 4, "{frames:?}");
        }
    }

    /// Each frame, if any, is the held grant of the row at token 4, `local`,
    /// with no `ping`: no same-token `local` grant went out.
    fn new_token_grants_only(frames: &[Value]) {
        if !frames.is_empty() {
            new_token_grants(frames);
        }
    }

    /// The row at token 4, `local` with the mark, written by a grant its
    /// holder never took: a step-4 row whose new-token grant is held
    /// ("Grants of a marked row"). R writes nothing over it, and its newest
    /// write gave it a new token, so `updateApp` holds no `local` grant, and
    /// a registration of one over it keeps the
    /// held grant a new-token grant: it goes out as before, `local` at 4,
    /// only once the gate has passed, and never as the same-token grant
    /// that ends the renewals.
    #[tokio::test]
    async fn r_over_a_held_new_token_grant_keeps_it_and_its_gate() {
        let f = fixture("warm", "local", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let hub = &f.state.hub;
        hub.authority().seed(LEASE, &f.node_id, Some(D as i64));
        let mut on = live(&f.state, &f, None).await;
        hub.hold_grant(&f.row().await);
        let writes = f.writes().await;
        end_renewals(&f).await;
        assert_eq!(f.writes().await, writes, "already local and marked");
        hub.hold_local_end(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "the gate is open");
        assert_eq!(view(&f.state, &f).await, (true, Vec::new(), true));
        f.clock.advance(Duration::from_secs(18));
        hub.attempt_held().await;
        new_token_grants(&queued(&mut on));
        f.done().await;
    }

    /// A re-entry after a restart: the restarted primary seeds the marked
    /// row's start gate, and the step-4 row's grant is held for its
    /// holder's session. R writes nothing over it, its newest write gave it
    /// a new token so `updateApp` holds no `local` grant, and a registration
    /// of the same-token grant over it changes nothing. A `hold_grant` of the row on
    /// that live session, whose `hello` gave no evidence (a regrant's or
    /// `createVolume`'s re-entry), keeps it a new-token grant: nothing goes
    /// out while the start gate is open, and the grant goes out after.
    #[tokio::test]
    async fn a_re_entry_after_a_restart_waits_for_the_start_gate() {
        let f = fixture("warm", "local", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let (after, clock) = restart(&f).await;
        after.hub.set_voter_support(Arc::new(EverySupport));
        let mut on = live(&after, &f, None).await;
        end_renewals(&f).await;
        after.hub.hold_local_end(&f.row().await);
        after.hub.hold_grant(&f.row().await);
        ack(&after, &f, &on).await;
        assert!(after.hub.authority().gate_remaining(LEASE).is_some());
        after.hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "the start gate is open");
        clock.advance(Duration::from_secs(18));
        after.hub.attempt_held().await;
        new_token_grants(&queued(&mut on));
        f.done().await;
    }

    /// ADR 0257: only the newest same-token `local` grant's `pong` passes
    /// the barrier. The session start repeats the `local` grant (G1) with
    /// its `ping` (P1); the policy goes `warm` and back off it, R marks the
    /// row, and R's grant (G2) goes out with its `ping` (P2). P1's `pong`
    /// passes no barrier, a conversion is refused, and the row keeps its
    /// mark; P2's `pong` passes the barrier and clears the mark.
    #[tokio::test]
    async fn an_older_barrier_ping_does_not_pass_a_later_grants_barrier() {
        let f = fixture("none", "local", Some(D as i64), false).await;
        created(&f).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let (_g1, p1) = grant_then_ping(&on.start, 3);
        set_policy(&f, "warm").await;
        r(&f).await;
        ack(&f.state, &f, &on).await;
        let hub = &f.state.hub;
        hub.attempt_held().await;
        let (_g2, p2) = grant_then_ping(&queued(&mut on), 3);
        let generation = on.session.generation;
        pong(&mut on.session, &p1).await;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(!hub.note_conversion_sent(&f.node_id, generation, LEASE, 3, D));
        assert!(f.row().await.unsettled);
        pong(&mut on.session, &p2).await;
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        f.done().await;
    }

    /// A grant and its `pong`, then the policy back on `warm` and off it
    /// again on the same session: R marks the row again, and a second
    /// `local` grant goes out with its own barrier, which its `pong` passes.
    #[tokio::test]
    async fn a_second_r_on_the_same_session_sends_a_second_local_grant() {
        let (f, mut on, _grant, ping) = sent("warm", false).await;
        pong(&mut on.session, &ping).await;
        assert!(!f.row().await.unsettled);
        set_policy(&f, "warm").await;
        r(&f).await;
        assert!(f.row().await.unsettled);
        ack(&f.state, &f, &on).await;
        let hub = &f.state.hub;
        hub.attempt_held().await;
        let (_grant, ping) = grant_then_ping(&queued(&mut on), 3);
        let generation = on.session.generation;
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        pong(&mut on.session, &ping).await;
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        assert!(!f.row().await.unsettled);
        f.done().await;
    }

    /// "Ending the renewals" step 2 ("once R is replicated (below), while
    /// the policy is still off `warm` and while it holds majority support
    /// (section 8.8), sends the holder the same-token `local` grant") and
    /// ADR 0257: a change off `warm` leaves the policy off `warm`, so the
    /// grant is owed again whether or not R wrote the row. The grant (G1)
    /// and its `ping` (P1) went out; before P1's `pong`, the policy goes
    /// back on `warm`, which sends nothing, and off it again, where R writes
    /// nothing: the row still reads `local` with the mark and its newest
    /// write gave no new token. A second grant (G2) goes out with its own
    /// `ping` (P2). P1's `pong` writes nothing and passes no barrier; P2's
    /// writes the row `local`, unmarked, and passes it.
    #[tokio::test]
    async fn a_warm_round_trip_before_the_pong_sends_a_second_local_grant() {
        let (f, mut on, _g1, p1) = sent("warm", false).await;
        let hub = &f.state.hub;
        set_policy(&f, "warm").await;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty(), "back on warm: nothing more");
        let writes = f.writes().await;
        r(&f).await;
        assert_eq!(f.writes().await, writes, "R writes nothing");
        hub.attempt_held().await;
        let (_g2, p2) = grant_then_ping(&queued(&mut on), 3);
        let generation = on.session.generation;
        pong(&mut on.session, &p1).await;
        assert!(f.row().await.unsettled, "the older ping writes nothing");
        assert!(!hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        pong(&mut on.session, &p2).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        assert!(hub.barrier_passed(&f.node_id, generation, LEASE, 3));
        f.done().await;
    }

    /// Section 8.5's regrant runs only on a session whose `hello` listed the
    /// lease `held` or `uncertain` at the token. R's grant on a session whose
    /// only evidence is a renewal, refused with `bad_message`: no regrant,
    /// the row stays `local` and marked at its token, and the `pong` after
    /// the refusal writes nothing. The renewal answer's gate has passed, so
    /// a regrant would write at once.
    #[tokio::test]
    async fn a_refused_local_grant_with_renewal_evidence_only_runs_no_regrant() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, None).await;
        r(&f).await;
        assert_answer(&renew(&mut on.session, "msg_r1", 3).await, "msg_r1", 3, D);
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        let (grant, ping) = grant_then_ping(&queued(&mut on), 3);
        f.clock.advance(Duration::from_secs(18));
        assert_eq!(f.state.hub.authority().gate_remaining(LEASE), None);
        let writes = f.writes().await;
        refuse(&mut on.session, &grant, ErrorCode::BadMessage).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), true, false),
            "no regrant"
        );
        assert_eq!(f.writes().await, writes);
        pong(&mut on.session, &ping).await;
        assert_eq!(f.writes().await, writes);
        f.done().await;
    }

    /// R over a row whose grant with a new token is still held, `expiring`
    /// at token 4 (a failover's grant to a node that never took it), while
    /// the holder's session gives no evidence. R writes the row `local` and
    /// marked; its newest write, R's, gave no new token, so the held grant
    /// is dropped as `updateApp` commits, before any attempt: nothing is
    /// pending at once, and one `app.left_stopped` names the grant's node
    /// and token. The `local` grant held in its place waits for
    /// evidence, which is no grant pending, and nothing goes out: the lease
    /// is given to no one, and the row stays `local` and marked at 4.
    #[tokio::test]
    async fn r_over_a_held_expiring_grant_drops_it_at_once() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let mut on = live(&f.state, &f, None).await;
        let hub = &f.state.hub;
        hub.hold_grant(&f.row().await);
        assert!(hub.grant_pending(LEASE));
        r(&f).await;
        assert!(!hub.grant_pending(LEASE), "dropped at once");
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)]);
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        assert!(queued(&mut on).is_empty());
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)], "once");
        {
            let registry = hub.0.held.lock().unwrap();
            let held = registry
                .entries
                .get(LEASE)
                .expect("the local grant is held");
            assert_eq!((held.kind, held.eligible_on), (Kind::LocalEnd, None));
        }
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 4, Some(D as i64), true, false)
        );
        f.done().await;
    }

    /// R over a row whose grant with a new token already went out on the
    /// holder's session (a failover's or a regrant's grant, `expiring` at
    /// token 4), and which the holder renewed there: R writes the row, and
    /// the sent grant's entry, which stands for nothing any more, gives way
    /// to the `local` grant that ends the renewals, with no
    /// `app.left_stopped`: that grant was sent. It goes out on that session,
    /// then its `ping`.
    #[tokio::test]
    async fn r_after_a_sent_new_token_grant_sends_the_local_grant() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let mut on = live(&f.state, &f, None).await;
        let hub = &f.state.hub;
        hub.hold_grant(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        let sent = queued(&mut on);
        assert!(!sent.is_empty(), "the new-token grant went out");
        for frame in &sent {
            assert_eq!(frame["body"]["mode"], "expiring", "{sent:?}");
            assert_eq!(frame["body"]["fencing_token"], 4, "{sent:?}");
        }
        let answer = renew(&mut on.session, "msg_r1", 4).await;
        assert_answer(&answer, "msg_r1", 4, D);
        r(&f).await;
        assert!(left_stopped(&f).await.is_empty(), "the grant was sent");
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 4);
        f.done().await;
    }

    /// What follows R keeps a grant with a new token held since R's
    /// transaction read the row, for a newer token: R's commit drops only an
    /// unsent one at a token no higher than the row it wrote. R writes the
    /// row at token 3; before its hub part runs, a write gives the lease
    /// token 4, marked, and its grant is held. R's hub part then leaves that
    /// grant as it was, holds no `local` grant over it, and emits no
    /// `app.left_stopped`.
    #[tokio::test]
    async fn r_keeps_a_grant_held_since_for_a_newer_token() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        created(&f).await;
        let mut w = f.state.store.begin_write().await.unwrap();
        sqlx::query("UPDATE apps SET ha_policy = 'none' WHERE id = ?")
            .bind(APP)
            .execute(&mut *w.tx)
            .await
            .unwrap();
        let ended = crate::leases::leave_warm(&mut w.tx, APP, &actor())
            .await
            .unwrap()
            .unwrap();
        let hub = &f.state.hub;
        let left = hub
            .record_left_stopped(&mut w.tx, &ended, &by(&actor()))
            .await
            .unwrap();
        assert_eq!(left, None, "nothing held at R");
        w.commit().await.unwrap();
        assert!(ended.wrote && ended.hold);
        assert_eq!(ended.lease.fencing_token, 3);
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let row = f.row().await;
        hub.hold_grant(&row);
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!((held.kind, held.token), (Kind::NewToken, 4));
        hub.end_renewals_committed(&ended);
        assert_eq!(
            hub.0.held.lock().unwrap().entries.get(LEASE),
            Some(&held),
            "the grant for the newer token stays"
        );
        assert!(hub.row_grant_pending(&row));
        assert!(left_stopped(&f).await.is_empty());
        f.done().await;
    }

    /// The drop of a `local` grant whose row moved: one held while its
    /// holder is away (no session, so none is eligible) goes once the row
    /// moves, to a new token or revoked. Before that it stays, waiting; after it, the
    /// registry holds nothing for the lease and no grant is pending.
    #[tokio::test]
    async fn a_local_grant_with_its_holder_away_goes_once_the_row_moves() {
        for moved in [
            "UPDATE leases SET fencing_token = 4 WHERE id = ?",
            "UPDATE leases SET revoked_at = '2026-11-10T10:00:00Z' WHERE id = ?",
        ] {
            let f = fixture("warm", "expiring", Some(D as i64), false).await;
            r(&f).await;
            let hub = &f.state.hub;
            hub.attempt_held().await;
            let held = hub.0.held.lock().unwrap().entries.get(LEASE).cloned();
            let held = held.expect("the local grant is held");
            assert_eq!(
                (held.kind, held.eligible_on),
                (Kind::LocalEnd, None),
                "{moved}"
            );
            write_row(&f, moved).await;
            hub.attempt_held().await;
            assert!(
                !hub.0.held.lock().unwrap().entries.contains_key(LEASE),
                "{moved}"
            );
            assert!(!hub.grant_pending(LEASE), "{moved}");
            f.done().await;
        }
    }

    /// Evidence from an `uncertain` listing: a holder whose `hello` listed
    /// the lease `uncertain`, `expiring` at the row's token, holds it there,
    /// so R's `local` grant goes out on that session once R is acknowledged,
    /// then its `ping`, whose `pong` clears the mark.
    #[tokio::test]
    async fn an_uncertain_listing_is_evidence_for_the_local_grant() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("uncertain", LeaseMode::Expiring, 3))).await;
        r(&f).await;
        ack(&f.state, &f, &on).await;
        f.state.hub.attempt_held().await;
        let (_grant, ping) = grant_then_ping(&queued(&mut on), 3);
        pong(&mut on.session, &ping).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), false, false)
        );
        f.done().await;
    }

    /// A failover's hold that lands after R. The failover wrote token 4,
    /// marked; R then wrote the row `local` before the failover held its
    /// grant, so the grant with a new token is held over a row whose newest
    /// write, R's, gave no new token. It stands for nothing: the next
    /// attempt drops it, though no session of its holder may take it, and
    /// `grant_pending` reads false, so a failover after a return to `warm`
    /// does not answer `in_progress` for it. It was not held when R
    /// committed, so no `app.left_stopped` is emitted, at R or at the drop.
    #[tokio::test]
    async fn a_new_token_grant_held_after_r_goes_with_its_holder_away() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let failover = f.row().await;
        r(&f).await;
        assert!(left_stopped(&f).await.is_empty(), "nothing held at R");
        let hub = &f.state.hub;
        hub.hold_grant(&failover);
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (held.kind, held.token, held.eligible_on),
            (Kind::NewToken, 4, None)
        );
        assert!(hub.grant_pending(LEASE));
        hub.attempt_held().await;
        assert!(!hub.0.held.lock().unwrap().entries.contains_key(LEASE));
        assert!(!hub.grant_pending(LEASE));
        assert!(!hub.row_grant_pending(&f.row().await));
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert!(left_stopped(&f).await.is_empty(), "the drop emits nothing");
        f.done().await;
    }

    /// The grants, revokes and `ping`s the hub queued on the session's
    /// channel.
    fn drained(live: &mut Live) -> Vec<Value> {
        let mut texts = Vec::new();
        while let Ok(text) = live.rx.try_recv() {
            texts.push(text);
        }
        frames(texts)
            .into_iter()
            .filter(|frame| {
                frame["type"] == "lease_grant"
                    || frame["type"] == "lease_revoke"
                    || frame["type"] == "ping"
            })
            .collect()
    }

    /// Section 5.1 and "The holder fenced first" step 4: a holder whose
    /// store went back lists the lease `held` `local` at 3 over a step-4
    /// row that reads `expiring` at 4. Without majority support, its session
    /// start holds back both the revoke at 3 and the grant. The row is then
    /// revoked at a higher token, as the raise for another lister does, so
    /// the grant is owed no more. While support is still lacking, the entry
    /// stays and its revoke stays held back. Once support returns, the
    /// revoke at 3 goes out, then the entry goes, and no grant follows.
    #[tokio::test]
    async fn a_held_back_revoke_goes_out_after_its_row_is_revoked() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let generation = on.session.generation;
        let held_revokes = || hub.0.sessions.lock().unwrap()[&f.node_id].held_revokes;
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (held.kind, held.token, held.eligible_on, held.revoke_first),
            (Kind::NewToken, 4, Some(generation), Some(3))
        );
        assert_eq!(held_revokes(), 1);

        write_row(
            &f,
            "UPDATE leases SET fencing_token = 5, revoked_at = '2026-11-10T10:00:00Z' \
             WHERE id = ?",
        )
        .await;
        hub.attempt_held().await;
        hub.attempt_held().await;
        assert_eq!(
            hub.0
                .held
                .lock()
                .unwrap()
                .entries
                .get(LEASE)
                .map(|held| held.revoke_first),
            Some(Some(3)),
            "the entry and its held-back revoke stay"
        );
        assert_eq!(held_revokes(), 1, "the revoke is not released");
        assert!(drained(&mut on).is_empty());

        hub.set_voter_support(Arc::new(EverySupport));
        hub.attempt_held().await;
        hub.attempt_held().await;
        let sent = drained(&mut on);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0]["type"], "lease_revoke", "{sent:?}");
        assert_eq!(sent[0]["body"]["lease_id"], LEASE);
        assert_eq!(sent[0]["body"]["fencing_token"], 3);
        assert_eq!(held_revokes(), 0);
        assert!(!hub.0.held.lock().unwrap().entries.contains_key(LEASE));
        f.done().await;
    }

    /// Section 5.1 and "The holder fenced first" step 4, with R: a holder
    /// whose store went back lists the lease `held` `local` at 3 over a
    /// step-4 row that reads `expiring` at 4, and without majority support
    /// its session start holds back both the revoke at 3 and the grant. A
    /// policy change off `warm` then writes R, which gives the row no new
    /// token. The entry and its held-back revoke stay, nothing goes out, and
    /// no `local` grant is held over it. Its grant is dropped all the same:
    /// one `app.left_stopped` names it as R commits, and it reads as no
    /// grant pending (`grant_pending` false, `grant_waiting_on` empty, no
    /// failover `in_progress`). A second change off `warm` writes nothing
    /// and emits nothing. Once support returns, the revoke at 3 goes out
    /// once; then the entry goes, with no second event, and no grant or
    /// `ping` follows.
    #[tokio::test]
    async fn a_held_back_revoke_goes_out_after_r_writes_its_row() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Local, 3))).await;
        let generation = on.session.generation;
        let held_revokes = || hub.0.sessions.lock().unwrap()[&f.node_id].held_revokes;
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (held.kind, held.token, held.eligible_on, held.revoke_first),
            (Kind::NewToken, 4, Some(generation), Some(3))
        );
        assert_eq!(held_revokes(), 1);

        let writes = f.writes().await;
        r(&f).await;
        assert_eq!(f.writes().await, writes + 1, "R wrote the row");
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 4, Some(D as i64), true, false)
        );
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)]);
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert!(!hub.row_grant_pending(&f.row().await));
        assert!(!hub.grant_pending(LEASE));
        r(&f).await;
        assert_eq!(f.writes().await, writes + 1, "a second R writes nothing");
        assert_eq!(
            left_stopped(&f).await,
            vec![the_event(&f, 4)],
            "a second R emits nothing"
        );
        hub.attempt_held().await;
        hub.attempt_held().await;
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        {
            let registry = hub.0.held.lock().unwrap();
            let entry = registry.entries.get(LEASE).expect("the entry stays");
            assert_eq!(
                (entry.kind, entry.token, entry.revoke_first),
                (Kind::NewToken, 4, Some(3)),
                "the held-back revoke stays, and no local grant is held over it"
            );
        }
        assert_eq!(held_revokes(), 1, "the revoke is not released");
        assert!(drained(&mut on).is_empty());

        hub.set_voter_support(Arc::new(EverySupport));
        hub.attempt_held().await;
        hub.attempt_held().await;
        let sent = drained(&mut on);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0]["type"], "lease_revoke", "{sent:?}");
        assert_eq!(sent[0]["body"]["lease_id"], LEASE);
        assert_eq!(sent[0]["body"]["fencing_token"], 3);
        assert_eq!(held_revokes(), 0);
        assert!(!hub.0.held.lock().unwrap().entries.contains_key(LEASE));
        hub.attempt_held().await;
        assert!(drained(&mut on).is_empty(), "no grant and no ping");
        assert_eq!(
            left_stopped(&f).await,
            vec![the_event(&f, 4)],
            "the entry's drop emits nothing"
        );
        f.done().await;
    }

    /// A policy change off `warm` over a row whose newest write gave it a
    /// new token, `local` at 4 with the mark, after that token's grant went
    /// out on the holder's session: R writes nothing, and the row is owed no
    /// same-token `local` grant, so the sent grant's entry stays as it was
    /// and no `local` grant is held in its place. No `ping` follows: the
    /// hub's own held-grant task may still deliver the new-token grant it
    /// attempted alongside the test's, but no same-token grant goes out.
    #[tokio::test]
    async fn leaving_warm_after_a_sent_new_token_grant_holds_no_local_grant() {
        let f = fixture("warm", "local", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let mut on = live(&f.state, &f, None).await;
        let hub = &f.state.hub;
        hub.hold_grant(&f.row().await);
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        new_token_grants(&queued(&mut on));
        let sent = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (sent.kind, sent.token, sent.sent_on),
            (Kind::NewToken, 4, Some(on.session.generation))
        );

        let writes = f.writes().await;
        r(&f).await;
        assert_eq!(f.writes().await, writes, "already local and marked");
        assert_eq!(
            hub.0.held.lock().unwrap().entries.get(LEASE),
            Some(&sent),
            "the sent grant's entry stays"
        );
        hub.attempt_held().await;
        new_token_grants_only(&queued(&mut on));
        assert!(!hub.grant_pending(LEASE));
        f.done().await;
    }

    /// "Settling a lease": "Sending the same-token `local` grant clears any
    /// pending settle for that lease, whether planned from a `hello` or
    /// waiting for its `pong`: none of them writes the row afterwards".
    ///
    /// 1. The policy leaves `warm` (R) and returns before the grant; the
    ///    holder's renewal at 3 is answered as a settle, and its `ping` is
    ///    recorded against R's write.
    /// 2. The policy leaves `warm` again: the row already reads `local` with
    ///    the mark, so nothing is written, and the settle's `ping` still
    ///    matches the row's newest write.
    /// 3. The `local` grant goes out: the session's settles of the lease are
    ///    gone at once.
    /// 4. Back on `warm`, the settle's `pong` arrives: the row stays `local`
    ///    and marked, never `expiring` while the holder holds the lease
    ///    `local`.
    ///
    /// A settle that a `hello` planned over a marked row goes the same way
    /// as the grant goes out.
    #[tokio::test]
    async fn the_local_grant_clears_the_settles_pending_on_its_session() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        r(&f).await;
        set_policy(&f, "warm").await;
        let answer = renew(&mut on.session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let settle_ping = ping_of(&answer);
        assert_eq!(
            hub.settle_state(&f.node_id, generation, LEASE),
            (None, vec![3])
        );
        let writes = f.writes().await;

        r(&f).await;
        assert_eq!(f.writes().await, writes, "R writes nothing the second time");

        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        let after_grant = hub.settle_state(&f.node_id, generation, LEASE);
        assert_eq!(f.writes().await, writes, "the settle's ping still matches");

        set_policy(&f, "warm").await;
        pong(&mut on.session, &settle_ping).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(D as i64), true, false),
            "not settled expiring"
        );
        assert_eq!(f.writes().await, writes);
        assert_eq!(
            after_grant,
            (None, Vec::new()),
            "cleared as the grant went out"
        );
        f.done().await;

        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        assert_eq!(
            hub.settle_state(&f.node_id, generation, LEASE),
            (Some(3), Vec::new()),
            "planned from the hello"
        );
        r(&f).await;
        ack(&f.state, &f, &on).await;
        hub.attempt_held().await;
        grant_then_ping(&queued(&mut on), 3);
        assert_eq!(
            hub.settle_state(&f.node_id, generation, LEASE),
            (None, Vec::new())
        );
        f.done().await;
    }

    /// The grant is recorded on a session only once it is queued there, in
    /// the hold of the sessions lock that finds the session current. A
    /// generation that is not the node's session gets nothing queued and
    /// nothing recorded. A session whose channel is closed gets no record:
    /// its renewals at the token are still answered, and the held grant is
    /// not marked sent. A session start on a session that is no longer the
    /// node's pushes neither the grant nor its `ping`.
    #[tokio::test]
    async fn a_local_grant_that_was_not_queued_is_not_recorded() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let mut on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        let generation = on.session.generation;
        r(&f).await;
        let barrier = super::Barrier {
            lease_id: LEASE.to_string(),
            token: 3,
            seq: None,
        };
        assert!(!hub.queue_local_end(
            &f.node_id,
            generation + 1,
            &barrier,
            "msg_grant",
            false,
            true,
            "msg_ping",
            &["{}".to_string()],
        ));
        assert!(queued(&mut on).is_empty());
        assert_eq!(hub.local_grant_sent(&f.node_id, generation, LEASE), None);

        ack(&f.state, &f, &on).await;
        let Live {
            mut session, rx, ..
        } = on;
        drop(rx);
        hub.attempt_held().await;
        assert_eq!(hub.local_grant_sent(&f.node_id, generation, LEASE), None);
        {
            let registry = hub.0.held.lock().unwrap();
            let held = &registry.entries[LEASE];
            assert_eq!(
                (held.kind, held.eligible_on, held.sent_on),
                (Kind::LocalEnd, Some(generation), None)
            );
        }
        assert_answer(&renew(&mut session, "msg_r1", 3).await, "msg_r1", 3, D);

        let (newer, _rx, _closed, _changed) = hub.attach(&f.node_id);
        let mut outbox = Vec::new();
        session
            .queue_local_grant(&f.row().await, 3, true, &mut outbox)
            .await;
        assert!(outbox.is_empty());
        assert_eq!(hub.local_grant_sent(&f.node_id, newer, LEASE), None);
        f.done().await;
    }

    /// "Grants of a marked row": "When that session ends first, the grant
    /// again waits for such a session and is not pending", and "When the
    /// policy returns to `warm`, the grant is dropped". `failoverApp` reads
    /// the policy before any wait: off `warm`, it answers `not_warm`, the
    /// grant pending or not. The holder's session ends; the policy returns
    /// to `warm`, and until an attempt drops the grant, it is still held for
    /// the ended session: it reads as not pending, so no failover answers
    /// `in_progress` for it.
    #[tokio::test]
    async fn a_local_grant_of_an_ended_session_makes_no_failover_answer_in_progress() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        r(&f).await;
        assert!(
            hub.row_grant_pending(&f.row().await),
            "pending on its session"
        );
        assert_eq!(failover(&f).await, Some(crate::error::Reason::NotWarm));

        hub.detach(&f.node_id, on.session.generation);
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (held.kind, held.eligible_on, held.sent_on),
            (Kind::LocalEnd, Some(on.session.generation), None)
        );
        set_policy(&f, "warm").await;
        let row = f.row().await;
        // As the held-grant task may not have attempted it yet; no await
        // until the reads below.
        hub.0
            .held
            .lock()
            .unwrap()
            .entries
            .insert(LEASE.to_string(), held);
        assert!(!hub.row_grant_pending(&row), "no in_progress");
        assert!(!hub.grant_pending(LEASE));
        assert_ne!(failover(&f).await, Some(crate::error::Reason::InProgress));
        f.done().await;
    }

    /// "A primary that restarted after R committed holds no such grant, so
    /// it drops nothing and emits nothing." R drops a failover's grant held
    /// at 4 and records one `app.left_stopped`, which opens no incident.
    /// The restarted primary holds nothing for the row, and the holder's
    /// session start, an attempt, and a return to `warm` and a new change
    /// off it record no other.
    #[tokio::test]
    async fn a_primary_restarted_after_r_emits_no_app_left_stopped() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        f.state.hub.hold_grant(&f.row().await);
        r(&f).await;
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)]);

        let (after, _clock) = restart(&f).await;
        after.hub.set_voter_support(Arc::new(EverySupport));
        assert!(!after.hub.grant_pending(LEASE), "nothing held");
        let on = live(&after, &f, None).await;
        ack(&after, &f, &on).await;
        after.hub.attempt_held().await;
        set_policy(&f, "warm").await;
        r_on(&after, &f).await;
        after.hub.attempt_held().await;
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)], "once");
        let incidents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM incidents")
            .fetch_one(f.state.store.pool())
            .await
            .unwrap();
        let linked: Option<String> =
            sqlx::query_scalar("SELECT incident_id FROM events WHERE type = 'app.left_stopped'")
                .fetch_one(f.state.store.pool())
                .await
                .unwrap();
        assert_eq!((incidents, linked), (0, None), "no incident");
        f.done().await;
    }

    /// A session start checks each entry held for its node against the row
    /// and drops the `local` grant that R held: the session start holds it
    /// again if its `hello` gives a reason. When the same grant is held
    /// again while the session start reads the row, equal to the one it
    /// read in every field but its registration, that fresh registration
    /// stays. The session start pauses after its read of the row, so the
    /// second registration always lands between that read and its change
    /// of the entry.
    #[tokio::test]
    async fn a_session_start_keeps_a_registration_made_while_it_reads_the_row() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        r(&f).await;
        let hub = &f.state.hub;
        let (generation, _rx, _closed, _changed) = hub.attach(&f.node_id);
        let row = f.row().await;
        let mut conn = f.state.store.pool().acquire().await.unwrap();

        hub.hold_local_end_on(&row, generation);
        hub.held_session_start(&mut conn, &f.node_id).await.unwrap();
        assert!(
            !hub.0.held.lock().unwrap().entries.contains_key(LEASE),
            "the session start drops it"
        );

        hub.hold_local_end_on(&row, generation);
        let pause = hub.pause_next_held_start();
        let start = hub.held_session_start(&mut conn, &f.node_id);
        let meanwhile = async {
            pause.reached().await;
            hub.hold_local_end_on(&row, generation);
            let fresh = hub.0.held.lock().unwrap().entries[LEASE].clone();
            pause.release();
            fresh
        };
        let (started, fresh) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(start, meanwhile)
        })
        .await
        .expect("the session start pauses once it has read the row");
        started.unwrap();
        assert_eq!(
            hub.0.held.lock().unwrap().entries.get(LEASE),
            Some(&fresh),
            "the fresh registration stays"
        );
        drop(conn);
        f.done().await;
    }

    /// "Grants of a marked row": "While the holder's current session is such
    /// a session, `grant_pending` is true until the grant is sent on it, and
    /// `grant_waiting_on` lists the machines of the required set that have
    /// not acknowledged R. When that session ends first, the grant again
    /// waits for such a session and is not pending." The holder's `hello`
    /// lists the lease `expiring` at the row's token, and R is not
    /// acknowledged: the `local` grant is pending, waiting on the holder.
    /// That session ends: no grant pending, nothing waited on, no failover
    /// `in_progress`, while `local_pending` stays true. A new session whose
    /// `hello` shows the lease makes it pending again, waiting on the
    /// holder, which lacks R, then on no machine once it acknowledges R.
    #[tokio::test]
    async fn a_pending_local_grant_is_not_pending_once_its_session_ends() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        r(&f).await;
        assert_eq!(
            view(&f.state, &f).await,
            (true, vec![f.node_id.clone()], true)
        );
        assert!(hub.row_grant_pending(&f.row().await));

        hub.detach(&f.node_id, on.session.generation);
        assert_eq!(view(&f.state, &f).await, (false, Vec::new(), true));
        assert!(!hub.row_grant_pending(&f.row().await), "no in_progress");
        assert!(!hub.grant_pending(LEASE));

        let mut again = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        assert_eq!(
            view(&f.state, &f).await,
            (true, vec![f.node_id.clone()], true),
            "pending again, waiting on the holder, which lacks R"
        );
        assert!(hub.row_grant_pending(&f.row().await));
        ack(&f.state, &f, &again).await;
        assert_eq!(view(&f.state, &f).await, (true, Vec::new(), true));
        hub.attempt_held().await;
        assert!(queued(&mut again).is_empty(), "no majority support");
        f.done().await;
    }

    /// "While the holder's current session is such a session, …
    /// `grant_waiting_on` lists the machines of the required set that have
    /// not acknowledged R": a second machine with the `node` role that has
    /// not acknowledged R is listed, and the holder is listed only until it
    /// acknowledges R.
    #[tokio::test]
    async fn a_pending_local_grant_waits_only_on_the_machines_that_lack_r() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let other = "nod_01JC9Z0000000000000000000B".to_string();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, \
             behind_nat, labels, capacity, allocated, kvm, created_at) \
             VALUES (?, 'node-b', '[\"node\"]', NULL, 'disconnected', '', NULL, 0, '{}', \
             '{}', '{}', '{}', ?)",
        )
        .bind(&other)
        .bind(crate::store::now_rfc3339())
        .execute(f.state.store.pool())
        .await
        .unwrap();
        let on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        f.state.hub.set_voter_support(Arc::new(NoSupport));
        r(&f).await;
        let mut both = vec![f.node_id.clone(), other.clone()];
        both.sort();
        assert_eq!(view(&f.state, &f).await, (true, both, true));
        ack(&f.state, &f, &on).await;
        assert_eq!(
            view(&f.state, &f).await,
            (true, vec![other], true),
            "the holder acknowledged R"
        );
        f.done().await;
    }

    /// What follows an R that wrote the row, over a held same-token `local`
    /// grant that is not sent: that is no grant with a new token, so it is
    /// not dropped and no `app.left_stopped` is emitted. The grant is held
    /// by a first R, on a session whose `hello` showed the lease, without
    /// majority support so it stays unsent; the hub's part of a policy
    /// change that wrote the row then runs over it.
    #[tokio::test]
    async fn r_over_a_held_local_grant_emits_nothing() {
        let f = fixture("warm", "expiring", Some(D as i64), false).await;
        let on = live(&f.state, &f, Some(("held", LeaseMode::Expiring, 3))).await;
        let hub = &f.state.hub;
        hub.set_voter_support(Arc::new(NoSupport));
        r(&f).await;
        let held = hub.0.held.lock().unwrap().entries[LEASE].clone();
        assert_eq!(
            (held.kind, held.eligible_on, held.sent_on),
            (Kind::LocalEnd, Some(on.session.generation), None)
        );
        let ended = crate::leases::Ended {
            lease: f.row().await,
            wrote: true,
            from_expiring: true,
            hold: false,
        };
        assert_eq!(hub.left_stopped(&ended), None);
        hub.end_renewals_committed(&ended);
        assert_eq!(
            hub.0.held.lock().unwrap().entries.get(LEASE),
            Some(&held),
            "the local grant stays"
        );
        assert!(left_stopped(&f).await.is_empty());
        f.done().await;
    }

    /// `updateApp` taking the fixture's app off `warm`, as its handler does
    /// up to the commit ([`crate::api::apps::update_app_tx`]), by a user
    /// whose actor is returned.
    async fn update_app_off_warm(
        f: &Fixture,
    ) -> (crate::api::apps::UpdatedApp, crate::changelog::Actor) {
        let (ctx, actor) = call(f, "PATCH /api/v1/apps/{app_id}").await;
        let params = serde_json::json!({ "ha_policy": "none" });
        let body = serde_json::from_value(params.clone()).unwrap();
        let updated = crate::api::apps::update_app_tx(&f.state, &ctx, APP, &body, &params)
            .await
            .unwrap();
        (updated, actor)
    }

    /// An API call `route` by a user, over the fixture's app with a health
    /// check and resources an API read accepts, and the user's actor.
    async fn call(
        f: &Fixture,
        route: &'static str,
    ) -> (crate::request::CallCtx, crate::changelog::Actor) {
        use crate::auth::{Credential, Identity, Principal, UserIdentity};
        let health_check = serde_json::to_string(&crate::api::apps::HealthCheck::default());
        let resources = serde_json::to_string(&crate::api::apps::AppResources::default());
        sqlx::query("UPDATE apps SET health_check = ?, resources = ? WHERE id = ?")
            .bind(health_check.unwrap())
            .bind(resources.unwrap())
            .bind(APP)
            .execute(f.state.store.pool())
            .await
            .unwrap();
        let identity = Identity {
            principal: Principal::User(UserIdentity {
                user_id: "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D".into(),
                email: "a@example.com".into(),
                name: "A".into(),
                role: quoro_types::Role::Owner,
            }),
            credential: Credential::Session {
                session_id: "ses_1".into(),
            },
        };
        let actor = identity.actor();
        let ctx = crate::request::CallCtx::new(
            identity,
            REQUEST.to_string(),
            route,
            &axum::http::HeaderMap::new(),
        );
        (ctx, actor)
    }

    /// The reason `failoverApp` gives for refusing a failover of the
    /// fixture's app to its holder.
    async fn failover(f: &Fixture) -> Option<crate::error::Reason> {
        let (ctx, _) = call(f, "POST /api/v1/apps/{app_id}/failover").await;
        let refused = crate::failover::run(&f.state, &ctx, APP, &f.node_id, &Value::Null)
            .await
            .expect_err("no failover of the fixture's app goes through");
        refused.reason
    }

    /// "The primary emits `app.left_stopped` as it drops the grant": a
    /// failover's grant to a node that is away is held, `expiring` at 4,
    /// when `updateApp` takes the app off `warm`. Once R commits, the
    /// held-grant task reads R and drops that grant before the handler's
    /// part after the commit runs, as it may on its tick or on any wake.
    /// That part then finds nothing to drop, and exactly one
    /// `app.left_stopped` stands all the same, written by R's own
    /// transaction with the caller of `updateApp` as its actor.
    #[tokio::test]
    async fn a_drop_after_r_commits_loses_no_app_left_stopped() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        created(&f).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let hub = &f.state.hub;
        hub.hold_grant(&f.row().await);
        assert!(hub.grant_pending(LEASE));
        let (updated, actor) = update_app_off_warm(&f).await;
        assert!(updated.converting.is_none(), "the policy left warm");
        let ended = updated.ended.expect("the app has a lease");
        assert!(ended.wrote && ended.from_expiring);
        let dropped = updated.left_stopped.expect("R drops the held grant");
        assert_eq!(
            (dropped.node_id.as_str(), dropped.fencing_token),
            (f.node_id.as_str(), 4)
        );
        hub.attempt_held().await;
        assert!(
            !hub.0.held.lock().unwrap().entries.contains_key(LEASE),
            "the held-grant task dropped it first"
        );
        hub.end_renewals_committed(&ended);
        let mut event = the_event(&f, 4);
        assert_eq!(actor.kind, crate::changelog::ActorType::User);
        event.3 = serde_json::json!({ "type": "user", "id": actor.id });
        assert_eq!(left_stopped(&f).await, vec![event]);
        assert!(!hub.grant_pending(LEASE));
        f.done().await;
    }

    /// What R's transaction decides and records for `held`, put in the
    /// registry for the fixture's lease right before the decision, with no
    /// await between them, so the hub's own held-grant task cannot change
    /// it first. The entry is taken out again afterwards.
    async fn decide(
        f: &Fixture,
        ended: &crate::leases::Ended,
        held: super::HeldGrant,
    ) -> Option<super::LeftStopped> {
        let hub = &f.state.hub;
        let mut w = f.state.store.begin_write().await.unwrap();
        hub.0.held.lock().unwrap().insert(LEASE.to_string(), held);
        let decided = hub
            .record_left_stopped(&mut w.tx, ended, &by(&actor()))
            .await
            .unwrap();
        w.commit().await.unwrap();
        hub.0.held.lock().unwrap().entries.remove(LEASE);
        decided
    }

    /// Each condition of the decision in R's transaction, alone: R turned a
    /// row that read `expiring` into `local`, and the registry holds a grant
    /// with a new token that names the row (holder and token), not sent and
    /// not dropped by an earlier R. The row is the one R left, `local` at 4
    /// with the mark; the grant is a failover's at 4 to the row's holder.
    /// That grant is reported once; with any one condition off, nothing is
    /// recorded.
    #[tokio::test]
    async fn r_records_app_left_stopped_only_for_an_unsent_grant_naming_the_row() {
        let f = fixture("none", "local", Some(D as i64), true).await;
        write_row(&f, "UPDATE leases SET fencing_token = 4 WHERE id = ?").await;
        let row = f.row().await;
        let ended = |from_expiring: bool| crate::leases::Ended {
            lease: row.clone(),
            wrote: true,
            from_expiring,
            hold: false,
        };
        let grant = super::HeldGrant::of(&row, None);
        assert_eq!(
            (grant.kind, grant.token, grant.holder.as_str()),
            (Kind::NewToken, 4, f.node_id.as_str())
        );

        let reported = decide(&f, &ended(true), grant.clone()).await;
        assert_eq!(
            reported,
            Some(super::LeftStopped {
                node_id: f.node_id.clone(),
                fencing_token: 4,
            })
        );
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)]);

        assert_eq!(
            decide(&f, &ended(false), grant.clone()).await,
            None,
            "R over a row that did not read expiring"
        );
        let mut lower = grant.clone();
        lower.token = 3;
        assert_eq!(
            decide(&f, &ended(true), lower).await,
            None,
            "a grant at a lower token"
        );
        let mut other = grant.clone();
        other.holder = "nod_01JC9Z0000000000000000000B".to_string();
        assert_eq!(
            decide(&f, &ended(true), other).await,
            None,
            "a grant to another node"
        );
        let mut sent = grant.clone();
        sent.sent_on = Some(1);
        assert_eq!(decide(&f, &ended(true), sent).await, None, "a sent grant");
        let mut dropped = grant.clone();
        dropped.revoke_first = Some(3);
        dropped.grant_dropped = true;
        assert_eq!(
            decide(&f, &ended(true), dropped).await,
            None,
            "a grant an earlier R dropped, kept for its held-back revoke"
        );
        assert_eq!(left_stopped(&f).await, vec![the_event(&f, 4)], "once");
        f.done().await;
    }
}
