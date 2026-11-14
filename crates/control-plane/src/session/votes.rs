// SPDX-License-Identifier: AGPL-3.0-only
//! Voter support on a session, the primary's side (`node-agent-protocol.md`
//! sections 4.2 rule 7, 5.27, 5.28 and 8.8; ADR 0169).
//!
//! - **When.** A voter's first `support_request` of a session goes out once
//!   the voter has acknowledged, with `command_result` `ok`, a
//!   `desired_state` that carries `spec.votes`, so it holds its voter-set
//!   version (section 4.2 rule 7). After that one goes out every `S / 3`
//!   while the session lives. A machine that becomes a voter, or whose
//!   version changes, gets one as soon as it acknowledges the `desired_state`
//!   that says so. No request goes to a machine that is not a voter.
//! - **What.** `term` is the cluster term, `primary_node_id` the one this
//!   session's `welcome` carried, `support_ms` is `S`, `max_lease_ms` is `D`,
//!   or `W` while a maintenance window is opening or open (section 8.11), and
//!   `voters_version` the version read when the request is built. A window
//!   that starts opening, fails or ends sends every voter a request at once.
//! - **Answers.** A `support_grant` goes to the tracker
//!   ([`crate::support::SupportTracker`]), which counts it or ignores it. An
//!   `error` that answers a request is logged and counts nothing. A
//!   `stale_term`, whatever it answers, stops all support and demotes the
//!   primary (sections 7.4 and 8.8, "Terms").
//! - **A restored primary (section 8.8; `backup-format.md` section 8.2; ADR
//!   0261, ADR 0280).** A primary restored with `quoro restore
//!   --accept-loss` that has sent no `lease_grant` yet
//!   ([`crate::store::Store::restarts_on_a_higher_term`]) starts again at
//!   one above the `term` of a `stale_term`, or of a voter's `bad_message`
//!   that answers a `support_request`, instead of demoting itself
//!   ([`NodeHub::start_again_above`]). A restart that would pass the term
//!   ceiling demotes it. Any other primary demotes on `stale_term`, and a
//!   voter's `bad_message` only leaves it without that voter's support. The
//!   first `lease_grant` a session sends ends the exception
//!   ([`NodeHub::frame_going_out`]). A session start sends the node every
//!   lease it holds again (`Session::finish_start`, `Session::send_grants`),
//!   each as a `lease_grant`, so the first session start of the host's own
//!   node ends the exception when that node holds a lease: the contract's
//!   "has sent no `lease_grant` yet" counts those grants too.

use super::{NodeHub, Session, cluster_term, new_msg_id};
use crate::error::ApiError;
use crate::store::TermRestart;
use crate::support::{Answer, REQUEST_INTERVAL, SUPPORT_MS};
use quoro_proto::wire::{self, ControlMessage};
use sqlx::SqliteConnection;
use tokio::time::Instant;

/// What a term above this primary's does to it ([`NodeHub::start_again_above`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HigherTerm {
    /// This primary is not one restored with `--accept-loss` that has sent no
    /// `lease_grant` yet: the rules of any primary apply.
    NoException,
    /// It starts again at one above the term; the write runs on its own task.
    StartsAgain,
    /// One above the term would pass the term ceiling, 2^53 - 1: it demotes
    /// itself instead (section 8.8, "The term ceiling").
    PastTheCeiling,
}

/// Whether the frame `text` is a `lease_grant`. A frame this primary built
/// always parses; one that does not is taken for a grant, the safe side of
/// [`NodeHub::frame_going_out`].
pub(super) fn is_lease_grant(text: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct Frame<'a> {
        #[serde(rename = "type", borrow)]
        kind: std::borrow::Cow<'a, str>,
    }
    match serde_json::from_str::<Frame<'_>>(text) {
        Ok(frame) => frame.kind == "lease_grant",
        Err(_) => true,
    }
}

/// The `detail` of rule 2's refusal (section 8.10).
pub const VOTERS_CHANGING: &str = "the previous voter-set change has not reached a majority of voters yet; try again once they have acknowledged it";

/// The `detail` of the refusal while the `nodes` rows and the change log
/// disagree on the voters ([`NodeHub::voter_change_gate`]).
pub const VOTERS_DISAGREE: &str =
    "the voter rows and the change log disagree on the voters; the change waits until they agree";

/// What removing one machine does to the voter set (`node-agent-protocol.md`
/// sections 8.10 and 10), as one read of the rows and the log gives it
/// ([`voter_removal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoterRemoval {
    /// The machine is a voter neither in the rows nor in the log: the set
    /// does not change.
    NotVoter,
    /// The rows and the log agree, and the machine is a voter in both: its
    /// removal entry moves the voter-set version to its own `seq`.
    Changes,
    /// The rows and the log disagree on this machine alone (ADR 0321 item
    /// 4): its removal entry ends the disagreement, the one voter change
    /// that passes it. `in_log` says whether the log counts the machine as a
    /// voter, so whether the entry moves the version.
    Repairs { in_log: bool },
    /// The rows and the log disagree on another machine, and this one is a
    /// voter in one of them: the removal waits like every other voter change.
    Blocked,
}

/// Classifies the removal of `node_id` against `set`, read on the removal's
/// own write transaction.
pub(crate) fn voter_removal(set: &crate::voters::SetOnWrite, node_id: &str) -> VoterRemoval {
    let in_rows = set.contains_row_voter(node_id);
    let Some(differs) = &set.disagreement else {
        return if in_rows {
            VoterRemoval::Changes
        } else {
            VoterRemoval::NotVoter
        };
    };
    let only_rows = differs.only_in_rows.iter().any(|id| id == node_id);
    let only_log = differs.only_in_log.iter().any(|id| id == node_id);
    let alone = differs
        .only_in_rows
        .iter()
        .chain(&differs.only_in_log)
        .all(|id| id == node_id);
    if (only_rows || only_log) && alone {
        VoterRemoval::Repairs { in_log: only_log }
    } else if in_rows || only_log {
        VoterRemoval::Blocked
    } else {
        VoterRemoval::NotVoter
    }
}

/// Rule 2 of section 8.10 for a removal classified `removal` against `set`:
/// whether more than half of the current voters has acknowledged the current
/// version. The current voters are those the change log gives at that
/// version ([`crate::voters::SetOnWrite::log_voters`]), not the rows': in a
/// repair where only the log names the machine, the machine counts in the
/// denominator although it never acknowledges, and where only the rows name
/// it, it does not count at all. When the two agree they are the same
/// machines. A machine that is no voter changes nothing and passes; a
/// blocked removal never does.
pub(crate) fn removal_allowed(
    set: &crate::voters::SetOnWrite,
    removal: VoterRemoval,
    acked: impl Fn(&str) -> Option<u64>,
) -> bool {
    match removal {
        VoterRemoval::NotVoter => true,
        VoterRemoval::Blocked => false,
        VoterRemoval::Changes | VoterRemoval::Repairs { .. } => {
            crate::voters::majority_holds(set.version, &set.log_voters(), acked)
        }
    }
}

impl NodeHub {
    /// A machine answered with `term`, the `term` of a `stale_term` or of a
    /// voter's `bad_message` (sections 5.18, 5.27 and 8.8). A primary
    /// restored with `quoro restore --accept-loss` that has sent no
    /// `lease_grant` yet starts again at one above it instead of demoting
    /// itself (`backup-format.md` section 8.2, ADR 0261, ADR 0280): its
    /// `meta.term` is raised in one write, on its own task, so the session
    /// waits for neither the writer gate nor SQLite
    /// ([`crate::store::Store::restart_term`]). That write never lowers
    /// the term; when it fails, the primary demotes itself. The decision is
    /// taken here, when the error arrives.
    ///
    /// When one above `term` would pass the term ceiling, nothing is written
    /// and the caller demotes the primary. Any other primary gets
    /// [`HigherTerm::NoException`] and nothing happens here.
    pub(crate) fn start_again_above(&self, term: u64) -> HigherTerm {
        if !self.0.store.restarts_on_a_higher_term() {
            return HigherTerm::NoException;
        }
        if term >= wire::MAX_TERM {
            return HigherTerm::PastTheCeiling;
        }
        let hub = self.clone();
        tokio::spawn(async move {
            match hub.0.store.restart_term(term).await {
                Ok(TermRestart::Raised { from, to }) => tracing::warn!(
                    node_term = term,
                    from,
                    to,
                    "a machine knows a higher term: this primary, restored with --accept-loss and with no lease_grant sent yet, starts again at one above it"
                ),
                Ok(TermRestart::Kept(current)) => tracing::info!(
                    node_term = term,
                    term = current,
                    "this primary, restored with --accept-loss, already runs above that term"
                ),
                Err(reason) => {
                    tracing::error!(
                        node_term = term,
                        %reason,
                        "this primary, restored with --accept-loss, cannot start again above that term; it demotes itself"
                    );
                    drop(hub.stale_term(Some(term)));
                }
            }
        });
        HigherTerm::StartsAgain
    }

    /// A session is about to send the frame `text`. When it is the first
    /// `lease_grant` of a primary restored with `--accept-loss`, a higher
    /// term demotes this primary from then on, as any other (section 7.4).
    /// Called before the frame is sent; reads the frame only while the
    /// exception holds.
    pub(crate) fn frame_going_out(&self, text: &str) {
        if self.0.store.restarts_on_a_higher_term()
            && is_lease_grant(text)
            && self.0.store.lease_grant_going_out()
        {
            tracing::info!(
                "the restored primary sends its first lease_grant: from now on a higher term demotes it"
            );
        }
    }

    /// Records that `node_id` acknowledged, with `command_result` `ok`, a
    /// `desired_state` carrying voter-set version `version`. The version
    /// only grows, so an older acknowledgement arriving late lowers nothing.
    pub(crate) fn record_votes_ack(&self, node_id: &str, version: u64) {
        let mut acks = self.0.votes_acks.lock().expect("votes acks lock");
        let held = acks.entry(node_id.to_string()).or_insert(version);
        *held = (*held).max(version);
    }

    /// Rule 2 of section 8.10, for the enrollment of a voter and
    /// `updateNodeRoles` adding or removing a vote; the removal of a voter
    /// passes the same rule through [`NodeHub::voter_removal_gate`]. It reads the version and
    /// the voters on `conn`, which must be the change's own write
    /// transaction, so the acknowledgements are counted against the version
    /// that transaction sees and two changes never pass on the same
    /// acknowledgements: the second reads the version the first wrote, which
    /// nobody has acknowledged yet. Returns that version and the number of
    /// current voters.
    ///
    /// **Rows and log must agree.** While the `nodes` rows and the change
    /// log disagree on the voters (the bug class of [`crate::voters`]), the
    /// gate refuses before it counts anything. Counting the rows' voters at
    /// the log's version would let a change pass that the log does not see
    /// as one: adding a vote the log already gives keeps the version, so the
    /// next change would pass on the same acknowledgements and land two
    /// voters away from the last acknowledged set (rules 1 and 2). Only this
    /// read's disagreement refuses, not the one [`crate::voters::VoterLog`]
    /// remembers for support: that one lasts until the version moves, which
    /// would then never happen.
    ///
    /// A disagreement heals at the disagreeing machine's next session:
    /// `connect_node` (its `hello`) always appends the whole row
    /// (`driver::node_payload`, with `roles` and `status`) to the change log,
    /// and `disconnect_node` does too when the row was `connected`. The
    /// replay then follows the row, the version moves to that entry's `seq`
    /// if the machine joined or left the set, and the voter watch sends every
    /// voter the new `spec.votes`. A machine that never connects again, or a
    /// row turned `removed` without its entry (a removed machine has no
    /// session), does not heal this way.
    ///
    /// # Errors
    ///
    /// `conflict` while the rows and the log disagree on the voters, or
    /// while a majority of the current voters has not acknowledged the
    /// current version; `internal` when the set cannot be read.
    pub(crate) async fn voter_change_gate(
        &self,
        conn: &mut SqliteConnection,
    ) -> Result<(u64, usize), ApiError> {
        let set = self
            .0
            .voters
            .read_on(conn)
            .await
            .map_err(|e| ApiError::internal(&e))?;
        if let Some(differs) = &set.disagreement {
            tracing::warn!(
                version = set.version,
                only_in_rows = ?differs.only_in_rows,
                only_in_log = ?differs.only_in_log,
                "a voter-set change waits: the voters in the nodes rows and those the change log replays differ"
            );
            return Err(ApiError::conflict(VOTERS_DISAGREE));
        }
        let acks = self.0.votes_acks.lock().expect("votes acks lock");
        if crate::voters::change_allowed(set.version, &set.voters, |id| acks.get(id).copied()) {
            Ok((set.version, set.voters.len()))
        } else {
            Err(ApiError::conflict(VOTERS_CHANGING))
        }
    }

    /// The check after the write of a voter-set change that passed
    /// [`NodeHub::voter_change_gate`], on the same transaction `conn`: the
    /// voter-set version must now be `seq`, the change's own change-log
    /// entry ([`crate::voters::VoterLog::landed`]). Otherwise the caller
    /// returns the error and the transaction rolls back unwritten.
    ///
    /// The answer is `internal`, not `conflict`. With the gate refusing while
    /// the rows and the log disagree, the version fails to move only when the
    /// change's own entry leaves out the `roles` or `status` it wrote: a bug
    /// in the write, which no retry and no acknowledgement fixes, so the
    /// caller is not told to try again.
    ///
    /// # Errors
    ///
    /// `internal` when the version is not `seq` or cannot be read.
    pub(crate) async fn voter_change_landed(
        &self,
        conn: &mut SqliteConnection,
        seq: i64,
    ) -> Result<(), ApiError> {
        self.0
            .voters
            .landed(conn, seq)
            .await
            .map_err(|e| ApiError::internal(&e))
    }

    /// The voter set as the removal's own write transaction `conn` sees it
    /// ([`crate::voters::VoterLog::read_on`]), for
    /// [`voter_removal`] and [`NodeHub::voter_removal_gate`].
    ///
    /// # Errors
    ///
    /// `internal` when the set cannot be read.
    pub(crate) async fn voter_set_on_write(
        &self,
        conn: &mut SqliteConnection,
    ) -> Result<crate::voters::SetOnWrite, ApiError> {
        self.0
            .voters
            .read_on(conn)
            .await
            .map_err(|e| ApiError::internal(&e))
    }

    /// Section 8.10 for `removeNode` (section 10), over `set` as the
    /// removal's own write transaction read it
    /// ([`NodeHub::voter_set_on_write`]), as [`NodeHub::voter_change_gate`]
    /// does for the other voter changes:
    ///
    /// - a machine that is no voter changes nothing and passes;
    /// - while the rows and the log disagree on another machine, the removal
    ///   of a voter is refused with `conflict`;
    /// - while they disagree on this machine alone, its removal is the one
    ///   change the gate lets through, because its removal entry ends the
    ///   disagreement (ADR 0321 item 4). It still needs rule 2's majority:
    ///   there is no other bypass;
    /// - rule 2 counts a majority of the current voters, those the change
    ///   log gives at the current version ([`removal_allowed`]).
    ///
    /// Removal is never refused for leaving fewer than three voters, and has
    /// no `force` (section 10). Returns what the removal does, never
    /// [`VoterRemoval::Blocked`].
    ///
    /// # Errors
    ///
    /// `conflict` as above.
    pub(crate) fn voter_removal_gate(
        &self,
        set: &crate::voters::SetOnWrite,
        node_id: &str,
    ) -> Result<VoterRemoval, ApiError> {
        let removal = voter_removal(set, node_id);
        if removal == VoterRemoval::Blocked {
            if let Some(differs) = &set.disagreement {
                tracing::warn!(
                    version = set.version,
                    only_in_rows = ?differs.only_in_rows,
                    only_in_log = ?differs.only_in_log,
                    node_id,
                    "the removal of a voter waits: the voters in the nodes rows and those the change log replays differ on another machine"
                );
            }
            return Err(ApiError::conflict(VOTERS_DISAGREE));
        }
        let acks = self.0.votes_acks.lock().expect("votes acks lock");
        if removal_allowed(set, removal, |id| acks.get(id).copied()) {
            Ok(removal)
        } else {
            Err(ApiError::conflict(VOTERS_CHANGING))
        }
    }

    /// The check after the removal's write, on the same transaction `conn`,
    /// once its removal entry is `seq` ([`NodeHub::voter_removal_gate`]):
    /// a voter's removal moved the voter-set version to `seq`
    /// ([`NodeHub::voter_change_landed`]); a repair ended the disagreement,
    /// and moved the version too when the log counted the machine. As for
    /// the other voter changes, a miss is a bug in the write, `internal`,
    /// and the caller's transaction rolls back unwritten.
    ///
    /// # Errors
    ///
    /// `internal` as above, or when the set cannot be read.
    pub(crate) async fn voter_removal_landed(
        &self,
        conn: &mut SqliteConnection,
        removal: VoterRemoval,
        seq: i64,
    ) -> Result<(), ApiError> {
        match removal {
            VoterRemoval::NotVoter | VoterRemoval::Blocked => Ok(()),
            VoterRemoval::Changes => self.voter_change_landed(conn, seq).await,
            VoterRemoval::Repairs { in_log } => {
                let set = self
                    .0
                    .voters
                    .read_on(conn)
                    .await
                    .map_err(|e| ApiError::internal(&e))?;
                if set.disagreement.is_some() {
                    return Err(ApiError::internal(&format!(
                        "the removal entry at change-log seq {seq} did not end the disagreement on the voters"
                    )));
                }
                if in_log && u64::try_from(seq).ok() != Some(set.version) {
                    return Err(ApiError::internal(&format!(
                        "the removal entry at change-log seq {seq} left the voter-set version at {}",
                        set.version
                    )));
                }
                Ok(())
            }
        }
    }

    /// Test seam: records that every current voter acknowledged the current
    /// voter-set version, as if each had a session that did, so a test that
    /// enrolls voters back to back without sessions passes rule 2 of section
    /// 8.10. The tests of rule 2 itself never call it.
    ///
    /// # Panics
    ///
    /// When the voter set cannot be read.
    #[doc(hidden)]
    pub async fn ack_voter_set(&self) {
        let set = self.voter_set().await.expect("the voter set");
        for voter in &set.voters {
            self.record_votes_ack(&voter.node_id, set.version);
        }
    }
}

impl Session {
    /// Takes the `command_result` that answers `re`, when `re` is a
    /// `desired_state` this session sent with `spec.votes`. Returns whether
    /// it was. An `ok` acknowledgement starts the support requests, or sends
    /// one at once for a new voter-set version (section 4.2 rule 7).
    pub(super) async fn snapshot_answered(
        &mut self,
        re: &str,
        result: &quoro_proto::CommandResult,
        outbox: &mut Vec<String>,
    ) -> bool {
        let Some(index) = self.voter_snapshots.iter().position(|(id, _)| id == re) else {
            return false;
        };
        let Some((_, version)) = self.voter_snapshots.remove(index) else {
            return false;
        };
        if result.status != quoro_proto::CommandStatus::Ok {
            return true;
        }
        // An `ok` counts as holding `version` even though the agent also
        // answers `ok` to a `desired_state` it ignores as old ("ignored rev
        // N", node agent `session.rs`): no snapshot this session sends is
        // ignored, so every `ok` here means the node stored it.
        // - The agent's `hello.stored_rev` and its "ignored" check both read
        //   its `latest_desired()`, the newest revision it stored.
        // - `refresh` sends `rev` = max(`nodes.desired_rev`, the session's
        //   last `rev`, that `stored_rev`) + 1, the first revision above
        //   that floor.
        // - Within a session revisions only grow, and only this session's
        //   snapshots raise what the agent stores, so each one is above it.
        // - A promoted primary must send above both its own
        //   `nodes.desired_rev` and the node's `stored_rev` (section 7.3),
        //   so no older primary's revision makes this one look old.
        self.hub.record_votes_ack(&self.node_id, version);
        let first = self.acked_votes_version.is_none() || self.support_due.is_none();
        let moved = self.acked_votes_version != Some(version);
        self.acked_votes_version = Some(version);
        if first || moved {
            self.send_support_request(outbox).await;
        }
        true
    }

    /// Sends one `support_request` (section 5.27) and schedules the next one
    /// `S / 3` later, while the node is a voter that acknowledged a
    /// `desired_state` with `spec.votes` on this session. Otherwise it sends
    /// nothing and schedules nothing: the next acknowledgement starts again.
    /// A request that cannot be built now is tried again `S / 3` later.
    pub(super) async fn send_support_request(&mut self, outbox: &mut Vec<String>) {
        self.support_due = None;
        let votes_sent = self
            .last_spec
            .as_ref()
            .is_some_and(|spec| spec.votes.is_some());
        if !votes_sent {
            // The node's newest desired state names it no voter any more.
            self.acked_votes_version = None;
            return;
        }
        if self.acked_votes_version.is_none() {
            return;
        }
        let Some(primary_node_id) = self.welcome_primary.clone() else {
            return;
        };
        let retry = Instant::now() + REQUEST_INTERVAL;
        let set = match self.hub.voter_set().await {
            Ok(set) => set,
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read the voter set; no support_request now");
                self.support_due = Some(retry);
                return;
            }
        };
        if !set.contains(&self.node_id) {
            return;
        }
        let term = match cluster_term(&self.hub).await {
            Ok(term) => term,
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read the cluster term; no support_request now");
                self.support_due = Some(retry);
                return;
            }
        };
        let request = wire::SupportRequest {
            term,
            primary_node_id,
            support_ms: SUPPORT_MS,
            // `D`, or `W` while a maintenance window is opening or open
            // (sections 5.27 and 8.11).
            max_lease_ms: self.hub.support_lease_ms(),
            voters_version: set.version,
        };
        let msg_id = new_msg_id();
        // `p_send` is read here, before the frame reaches the socket, so the
        // window can only end earlier than the voter's promise (ADR 0263).
        self.hub
            .0
            .support
            .sent(&msg_id, &self.node_id, self.generation, &request);
        outbox.push(
            ControlMessage::SupportRequest(request)
                .to_frame(&msg_id, None)
                .to_text(),
        );
        self.support_due = Some(Instant::now() + REQUEST_INTERVAL);
    }

    /// Takes a `support_grant` (section 5.28). The voter set is read first,
    /// so the answer is counted against the current version (section 8.10
    /// rule 3); when it cannot be read, nothing counts.
    pub(super) async fn on_support_grant(&self, re: Option<&str>, grant: &wire::SupportGrant) {
        if let Err(e) = self.hub.voter_set().await {
            tracing::warn!(%e, node_id = %self.node_id, "cannot read the voter set; a support_grant is not counted");
            return;
        }
        match self
            .hub
            .0
            .support
            .granted(&self.node_id, self.generation, re, grant)
        {
            Answer::Counted => {}
            Answer::Unpaired => tracing::debug!(
                node_id = %self.node_id,
                re,
                "a support_grant that answers no outstanding request is ignored"
            ),
            Answer::WrongEcho => tracing::warn!(
                node_id = %self.node_id,
                term = grant.term,
                support_ms = grant.support_ms,
                max_lease_ms = grant.max_lease_ms,
                "a support_grant that does not echo its request is not counted"
            ),
            Answer::OtherVersion { current, asked } => tracing::debug!(
                node_id = %self.node_id,
                voters_version = grant.voters_version,
                ?current,
                asked,
                "a support_grant at another voter-set version is not counted"
            ),
            Answer::NotVoter => tracing::debug!(
                node_id = %self.node_id,
                "a support_grant from a machine that is not a voter is not counted"
            ),
        }
    }

    /// The node answered `stale_term`: a higher term exists. The primary
    /// stops granting and renewing `expiring` leases at once, then demotes
    /// itself ([`NodeHub::stale_term`], section 7.4, ADR 0230). The flag is
    /// set when this returns; the session does not wait for the `meta`
    /// write, so it ends within its cap. Returns `true`.
    ///
    /// A primary restored with `--accept-loss` that has sent no
    /// `lease_grant` yet starts again at one above the error's `term`
    /// instead, and this returns `false` ([`NodeHub::start_again_above`]).
    /// Without a `term` there is nothing to start above, and it demotes;
    /// the contract gives no rule for that case.
    pub(super) fn on_stale_term(&self, node_term: Option<u64>) -> bool {
        if let Some(term) = node_term {
            match self.hub.start_again_above(term) {
                HigherTerm::StartsAgain => {
                    tracing::warn!(
                        node_id = %self.node_id,
                        node_term = term,
                        "a node answered stale_term: this primary, restored with --accept-loss and with no lease_grant sent yet, starts again above its term"
                    );
                    return false;
                }
                HigherTerm::PastTheCeiling => tracing::error!(
                    node_id = %self.node_id,
                    node_term = term,
                    "a node answered stale_term: one above its term would pass the term ceiling, so this primary, restored with --accept-loss, demotes itself instead"
                ),
                HigherTerm::NoException => {}
            }
        }
        tracing::error!(
            node_id = %self.node_id,
            ?node_term,
            "a node answered stale_term: a higher term exists; this primary grants and renews no expiring lease and demotes itself"
        );
        drop(self.hub.stale_term(node_term));
        true
    }

    /// A voter answered a `support_request` with `bad_message` carrying its
    /// `term`: it voted in that term for another candidate (sections 5.27
    /// and 8.8, ADR 0280). The answer already counts for nothing. A primary
    /// restored with `--accept-loss` that has sent no `lease_grant` yet
    /// treats it like `stale_term` and starts again at one above that term;
    /// when that would pass the term ceiling it demotes itself, and this
    /// returns `true`. Any other primary, a plain restore's included, only
    /// lacks that voter's support.
    pub(super) fn on_voter_term(&self, term: u64) -> bool {
        match self.hub.start_again_above(term) {
            HigherTerm::StartsAgain => {
                tracing::warn!(
                    node_id = %self.node_id,
                    node_term = term,
                    "a voter voted in this term for another candidate: this primary, restored with --accept-loss and with no lease_grant sent yet, starts again above it"
                );
                false
            }
            HigherTerm::PastTheCeiling => {
                tracing::error!(
                    node_id = %self.node_id,
                    node_term = term,
                    "a voter voted in this term for another candidate, and one above it would pass the term ceiling: this primary, restored with --accept-loss, demotes itself instead"
                );
                drop(self.hub.stale_term(Some(term)));
                true
            }
            HigherTerm::NoException => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VoterRemoval, removal_allowed, voter_removal};
    use crate::voters::{Disagreement, SetOnWrite, Voter};

    fn set(rows: &[&str], only_in_rows: &[&str], only_in_log: &[&str]) -> SetOnWrite {
        let ids = |ids: &[&str]| ids.iter().map(|id| (*id).to_string()).collect::<Vec<_>>();
        SetOnWrite {
            version: 7,
            voters: rows
                .iter()
                .map(|id| Voter {
                    node_id: (*id).to_string(),
                    roles: vec!["node".to_string()],
                    mesh_address: None,
                })
                .collect(),
            disagreement: (!only_in_rows.is_empty() || !only_in_log.is_empty()).then(|| {
                Disagreement {
                    only_in_rows: ids(only_in_rows),
                    only_in_log: ids(only_in_log),
                }
            }),
        }
    }

    /// Sections 8.10 and 10, ADR 0321 item 4: what the removal of one
    /// machine does to the voter set, for each way the rows and the log can
    /// stand.
    #[test]
    fn a_removal_changes_repairs_or_waits_for_the_voter_set() {
        // They agree: a voter's removal changes the set, a non-voter's not.
        let agree = set(&["nod_a", "nod_b"], &[], &[]);
        assert_eq!(voter_removal(&agree, "nod_a"), VoterRemoval::Changes);
        assert_eq!(voter_removal(&agree, "nod_e"), VoterRemoval::NotVoter);

        // They disagree on X alone: removing X repairs it, from either side.
        let in_log = set(&["nod_a", "nod_b"], &[], &["nod_x"]);
        assert_eq!(
            voter_removal(&in_log, "nod_x"),
            VoterRemoval::Repairs { in_log: true }
        );
        let in_rows = set(&["nod_a", "nod_b", "nod_x"], &["nod_x"], &[]);
        assert_eq!(
            voter_removal(&in_rows, "nod_x"),
            VoterRemoval::Repairs { in_log: false }
        );
        // Any other voter's removal waits; a non-voter's passes.
        assert_eq!(voter_removal(&in_log, "nod_a"), VoterRemoval::Blocked);
        assert_eq!(voter_removal(&in_rows, "nod_b"), VoterRemoval::Blocked);
        assert_eq!(voter_removal(&in_log, "nod_e"), VoterRemoval::NotVoter);

        // They disagree on X and Y: removing X does not end it.
        let two = set(&["nod_a", "nod_y"], &["nod_y"], &["nod_x"]);
        assert_eq!(voter_removal(&two, "nod_x"), VoterRemoval::Blocked);
        assert_eq!(voter_removal(&two, "nod_y"), VoterRemoval::Blocked);
        assert_eq!(voter_removal(&two, "nod_e"), VoterRemoval::NotVoter);
    }

    /// Whether rule 2 lets `node_id`'s removal through with `acked` holding
    /// the set's version and every other machine holding none.
    fn passes(set: &SetOnWrite, node_id: &str, acked: &[&str]) -> bool {
        removal_allowed(set, voter_removal(set, node_id), |id| {
            acked.contains(&id).then_some(set.version)
        })
    }

    /// Section 8.10 rule 2 for a removal: the majority is counted over the
    /// voters the change log gives at the current version, not the rows'.
    #[test]
    fn a_removal_counts_the_majority_over_the_logs_voters() {
        // They agree, an even set: two of four is not a majority.
        let agree = set(&["nod_a", "nod_b", "nod_c", "nod_d"], &[], &[]);
        assert!(!passes(&agree, "nod_d", &["nod_a", "nod_b"]));
        assert!(passes(&agree, "nod_d", &["nod_a", "nod_b", "nod_c"]));

        // Only the log has X: the current set is {A, B, C, X}, four voters,
        // and X never acknowledges. Two of the rows' three would be a
        // majority of the rows, not of the set: three are needed.
        let in_log = set(&["nod_a", "nod_b", "nod_c"], &[], &["nod_x"]);
        assert_eq!(
            voter_removal(&in_log, "nod_x"),
            VoterRemoval::Repairs { in_log: true }
        );
        assert!(!passes(&in_log, "nod_x", &["nod_a", "nod_b"]));
        assert!(passes(&in_log, "nod_x", &["nod_a", "nod_b", "nod_c"]));

        // Only the rows have X: the current set is {A, B}, and X's own
        // acknowledgement does not count. A and X are two of the rows'
        // three, but only one of the set's two.
        let in_rows = set(&["nod_a", "nod_b", "nod_x"], &["nod_x"], &[]);
        assert_eq!(
            voter_removal(&in_rows, "nod_x"),
            VoterRemoval::Repairs { in_log: false }
        );
        assert!(!passes(&in_rows, "nod_x", &["nod_a", "nod_x"]));
        assert!(passes(&in_rows, "nod_x", &["nod_a", "nod_b"]));
        // Rows {A, B, C, X}, log {A, B, C}: two of the log's three pass,
        // though they are only half of the rows.
        let in_rows_even = set(&["nod_a", "nod_b", "nod_c", "nod_x"], &["nod_x"], &[]);
        assert!(passes(&in_rows_even, "nod_x", &["nod_a", "nod_b"]));
        assert!(!passes(&in_rows_even, "nod_x", &["nod_a", "nod_x"]));

        // A non-voter changes nothing; a blocked removal never passes.
        assert!(passes(&agree, "nod_e", &[]));
        let two = set(&["nod_a", "nod_y"], &["nod_y"], &["nod_x"]);
        assert!(!passes(&two, "nod_x", &["nod_a", "nod_y", "nod_x"]));
    }
}
