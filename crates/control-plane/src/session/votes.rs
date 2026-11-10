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
//!   and `voters_version` the version read when the request is built.
//! - **Answers.** A `support_grant` goes to the tracker
//!   ([`crate::support::SupportTracker`]), which counts it or ignores it. An
//!   `error` that answers a request is logged and counts nothing. A
//!   `stale_term`, whatever it answers, stops all support and demotes the
//!   primary (sections 7.4 and 8.8, "Terms").

use super::{NodeHub, Session, cluster_term, new_msg_id};
use crate::authority::DEFAULT_DURATION_MS;
use crate::error::ApiError;
use crate::support::{Answer, REQUEST_INTERVAL, SUPPORT_MS};
use quoro_proto::wire::{self, ControlMessage};
use sqlx::SqliteConnection;
use tokio::time::Instant;

/// The `detail` of rule 2's refusal (section 8.10).
pub const VOTERS_CHANGING: &str = "the previous voter-set change has not reached a majority of voters yet; try again once they have acknowledged it";

/// The `detail` of the refusal while the `nodes` rows and the change log
/// disagree on the voters ([`NodeHub::voter_change_gate`]).
pub const VOTERS_DISAGREE: &str =
    "the voter rows and the change log disagree on the voters; the change waits until they agree";

impl NodeHub {
    /// Records that `node_id` acknowledged, with `command_result` `ok`, a
    /// `desired_state` carrying voter-set version `version`. The version
    /// only grows, so an older acknowledgement arriving late lowers nothing.
    pub(crate) fn record_votes_ack(&self, node_id: &str, version: u64) {
        let mut acks = self.0.votes_acks.lock().expect("votes acks lock");
        let held = acks.entry(node_id.to_string()).or_insert(version);
        *held = (*held).max(version);
    }

    /// Rule 2 of section 8.10, for every change of the voter set: the
    /// enrollment of a voter, `updateNodeRoles` adding or removing a vote,
    /// and (once it exists) the removal of a voter. It reads the version and
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
            max_lease_ms: DEFAULT_DURATION_MS,
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
    /// itself ([`NodeHub::stale_term`], section 7.4, ADR 0230).
    pub(super) async fn on_stale_term(&self, node_term: Option<u64>) {
        tracing::error!(
            node_id = %self.node_id,
            ?node_term,
            "a node answered stale_term: a higher term exists; this primary grants and renews no expiring lease and demotes itself"
        );
        self.hub.stale_term(node_term).await;
    }
}
