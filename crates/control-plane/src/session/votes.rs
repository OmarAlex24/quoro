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
//!   `stale_term`, whatever it answers, stops all support (section 8.8,
//!   "Terms").

use super::{Session, cluster_term, new_msg_id};
use crate::authority::DEFAULT_DURATION_MS;
use crate::support::{Answer, REQUEST_INTERVAL, SUPPORT_MS};
use quoro_proto::wire::{self, ControlMessage};
use tokio::time::Instant;

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
    /// stops granting and renewing `expiring` leases at once and holds no
    /// support until it restarts (section 8.8, "Terms"). Demotion, which ends
    /// the sessions with `not_primary` (section 7.4, ADR 0182), belongs to
    /// the promotion work and is not built here.
    pub(super) fn on_stale_term(&self, node_term: Option<u64>) {
        tracing::error!(
            node_id = %self.node_id,
            ?node_term,
            "a node answered stale_term: a higher term exists; this primary grants and renews no expiring lease until it restarts"
        );
        self.hub.0.support.stale_term();
    }
}
