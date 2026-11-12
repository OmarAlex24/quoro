// SPDX-License-Identifier: AGPL-3.0-only
//! Terms and the voter's support (`node-agent-protocol.md` section 8.8,
//! ADR 0169).
//!
//! Every machine, voter or not, keeps the highest term it has seen in its
//! store ([`check_term`]): a higher term in a `welcome`, a `desired_state` or
//! a `support_request` is stored before anything acts on it, and a lower one
//! is answered `stale_term`.
//!
//! A voter is a machine whose newest desired state carries `spec.votes`
//! (section 6.12). It answers each `support_request` ([`answer_support`]) with
//! a promise, `v_recv + S`, and a lease horizon, `v_recv + S + L + 2 M_L`,
//! both on `CLOCK_BOOTTIME` next to the kernel boot id. It stores them before
//! it answers `support_grant`, so a promise survives an agent restart within
//! the same boot; after a reboot both start again from the agent's first
//! start ([`rebase_at_start`]).
//!
//! A voter also answers promotion votes on its mesh address (section 8.9,
//! [`crate::vote_listener`]): [`decide_vote`] decides each one on the vote
//! state one transaction reads, and [`answer_vote`] writes a granted vote in
//! that transaction before the answer goes out.
//!
//! Every value this module stores errs late: a later promise only keeps the
//! voter from supporting another candidate a little longer, and a later
//! horizon only makes a candidate wait a little longer (section 8.9), while an
//! earlier one could let two primaries grant the same lease.

use std::net::IpAddr;
use std::time::Duration;

use quoro_proto::wire::{
    MIN_SUPPORT_MS, SpecVoter, SpecVotes, SupportGrant, SupportRequest, VoteAnswer, VoteOutcome,
    VoteRefusal, VoteRequest,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::AgentError;
use crate::lease::{HostClock, margin};
use crate::store::NodeStore;

/// The vote a voter granted (section 8.9), as [`crate::store::VOTE`] stores
/// it. Only the vote listener of section 8.9 sets it; the support rule reads
/// it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredVote {
    /// The term the voter voted in.
    pub term: u64,
    /// The `nod_` id of the candidate it voted for.
    pub candidate: String,
}

/// The primary a voter supports, and its promise and lease horizon (section
/// 8.8), as [`crate::store::SUPPORT`] stores them.
///
/// The instants are nanoseconds of `CLOCK_BOOTTIME` on the boot `boot_id`
/// names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSupport {
    /// The term supported.
    pub term: u64,
    /// The primary supported.
    pub primary_node_id: String,
    /// `S` of the last request granted.
    pub support_ms: u64,
    /// `L` of the last request granted.
    pub max_lease_ms: u64,
    /// The kernel boot id the instants below belong to.
    pub boot_id: String,
    /// `v_recv` of the last request granted, or the agent's start after a
    /// reboot.
    pub received_ns: u64,
    /// The promise: no other candidate is supported for a higher term before
    /// it.
    pub promise_ns: u64,
    /// The lease horizon: until then a grant made under this support may not
    /// yet have passed its gate.
    pub horizon_ns: u64,
}

impl StoredSupport {
    /// The promise on `CLOCK_BOOTTIME`.
    #[must_use]
    pub fn promise(&self) -> Duration {
        Duration::from_nanos(self.promise_ns)
    }

    /// The lease horizon on `CLOCK_BOOTTIME`.
    #[must_use]
    pub fn horizon(&self) -> Duration {
        Duration::from_nanos(self.horizon_ns)
    }

    /// The same support on boot `boot_id`, whose `CLOCK_BOOTTIME` reads `now`.
    ///
    /// On the same boot nothing changes: the stored instants keep counting
    /// (section 8.8 "Restarts"). On another boot the voter cannot tell how
    /// long it was down, so both start again from `now`: the promise until
    /// `now + S` and the horizon until `now + S + L + 2 M_L`, with the stored
    /// `S` and `L`.
    ///
    /// Reading of section 8.8, erring late: `S` and `L` are those of the last
    /// grant, but an earlier grant with a longer window may have set the
    /// stored promise or horizon, and the primary may still count it (it keeps
    /// the newest support it got an answer for, which is that earlier one when
    /// the last answer was lost). So each instant starts again from `now` with
    /// the longer of the rule's span and what was left of it at the last grant
    /// (`promise - v_recv`, `horizon - v_recv`). When the last grant set both,
    /// which is the steady case, this is the rule's value exactly.
    #[must_use]
    pub fn on_boot(&self, boot_id: &str, now: Duration) -> Self {
        if self.boot_id == boot_id {
            return self.clone();
        }
        let received = Duration::from_nanos(self.received_ns);
        let promise_span = window(self.support_ms).max(self.promise().saturating_sub(received));
        let horizon_span = horizon_span(self.support_ms, self.max_lease_ms)
            .max(self.horizon().saturating_sub(received));
        Self {
            boot_id: boot_id.to_owned(),
            received_ns: nanos(now),
            promise_ns: nanos(now.saturating_add(promise_span)),
            horizon_ns: nanos(now.saturating_add(horizon_span)),
            ..self.clone()
        }
    }
}

/// `S + L + 2 M_L`, with `M_L = max(1000 ms, L / 10)` (section 8.8): how far
/// past `v_recv` the lease horizon lies.
#[must_use]
pub fn horizon_span(support_ms: u64, max_lease_ms: u64) -> Duration {
    let lease = Duration::from_millis(max_lease_ms);
    window(support_ms)
        .saturating_add(lease)
        .saturating_add(margin(lease).saturating_mul(2))
}

fn window(support_ms: u64) -> Duration {
    Duration::from_millis(support_ms)
}

fn nanos(at: Duration) -> u64 {
    u64::try_from(at.as_nanos()).unwrap_or(u64::MAX)
}

/// The support a voter stores when it grants `request`, received at `v_recv`
/// on `CLOCK_BOOTTIME` of boot `boot_id` (section 8.8 "Exchange").
///
/// The promise is `v_recv + S` and the horizon `v_recv + S + L + 2 M_L`, but
/// neither is ever earlier than the stored one. Section 8.8 says the voter
/// never lowers the horizon within a term; a higher term starts a new horizon
/// only when that is never earlier, so the horizon keeps the maximum across
/// terms as well. The promise keeps its maximum too: the primary keeps the
/// newest support it got an answer for, so when a later grant with a shorter
/// `S` is lost on the way it still counts the earlier, longer window. A stored
/// support from another boot is first brought onto this one
/// ([`StoredSupport::on_boot`]), as at a start after a reboot.
#[must_use]
pub fn support_after(
    stored: Option<&StoredSupport>,
    request: &SupportRequest,
    boot_id: &str,
    v_recv: Duration,
) -> StoredSupport {
    let mut promise = v_recv.saturating_add(window(request.support_ms));
    let mut horizon = v_recv.saturating_add(horizon_span(request.support_ms, request.max_lease_ms));
    if let Some(stored) = stored {
        let stored = stored.on_boot(boot_id, v_recv);
        promise = promise.max(stored.promise());
        horizon = horizon.max(stored.horizon());
    }
    StoredSupport {
        term: request.term,
        primary_node_id: request.primary_node_id.clone(),
        support_ms: request.support_ms,
        max_lease_ms: request.max_lease_ms,
        boot_id: boot_id.to_owned(),
        received_ns: nanos(v_recv),
        promise_ns: nanos(promise),
        horizon_ns: nanos(horizon),
    }
}

/// How a frame's term compares with the highest term the machine has seen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TermCheck {
    /// The same term.
    Current,
    /// A higher term, now stored.
    Raised,
    /// A lower term: the frame is answered `stale_term`, which carries
    /// `highest`.
    Stale {
        /// The highest term seen.
        highest: u64,
    },
}

/// Compares `term` with the highest term seen and stores it when it is higher
/// (section 8.8 "Terms"). A machine with no stored term has seen none; terms
/// start at 1. A vote counts among the terms seen (section 8.8), so the term
/// of a stored vote counts even if the stored term was not raised with it.
///
/// # Errors
///
/// [`AgentError::Store`] when the stored term cannot be read or the higher one
/// cannot be stored. The caller then acts on nothing the frame carries.
pub async fn check_term(store: &NodeStore, term: u64) -> Result<TermCheck, AgentError> {
    let highest = current_term(store).await?;
    if term < highest {
        return Ok(TermCheck::Stale { highest });
    }
    if term > highest {
        store.raise_term(term).await?;
        return Ok(TermCheck::Raised);
    }
    Ok(TermCheck::Current)
}

/// `CLOCK_BOOTTIME` and the kernel boot id, read when a `support_request` was
/// received: `v_recv` (section 8.8). Either is `None` on a host that cannot
/// tell it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Received {
    /// The kernel boot id.
    pub boot_id: Option<String>,
    /// `CLOCK_BOOTTIME`.
    pub boottime: Option<Duration>,
}

impl Received {
    /// Reads the host clock now.
    #[must_use]
    pub fn read(clock: &dyn HostClock) -> Self {
        Self {
            boottime: clock.boottime(),
            boot_id: clock.boot_id(),
        }
    }
}

/// What the machine answers a `support_request` with (sections 5.27, 5.28 and
/// 8.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupportAnswer {
    /// A machine that is not a voter ignores the request: no answer, no error.
    Ignore,
    /// The request's term is lower than the highest seen: `stale_term`, fatal,
    /// carrying `highest`.
    Stale {
        /// The highest term seen.
        highest: u64,
    },
    /// `bad_message`, non-fatal, carrying `term` when it is set.
    Refuse {
        /// Why.
        message: String,
        /// The voter's term, for a vote in this term for another candidate.
        term: Option<u64>,
    },
    /// The promise is stored: answer `support_grant`.
    Grant(SupportGrant),
    /// Nothing could be stored, so nothing is promised and nothing is sent;
    /// the primary asks again every `S / 3`.
    Withhold(String),
}

/// A voter's term, vote and support as one transaction read them
/// ([`NodeStore::update_vote_state`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VoteState {
    /// The highest term seen, `None` before the first.
    pub highest_term: Option<u64>,
    /// The vote, when the voter voted (section 8.9).
    pub vote: Option<StoredVote>,
    /// The support, when the voter granted one.
    pub support: Option<StoredSupport>,
}

impl VoteState {
    /// The highest term seen: the stored one, or the term of the vote when
    /// that is higher. A vote is a term the voter has seen (section 8.8
    /// "Terms"), so it counts even if the stored term was not raised with it.
    #[must_use]
    pub fn highest(&self) -> u64 {
        let stored = self.highest_term.unwrap_or(0);
        self.vote
            .as_ref()
            .map_or(stored, |vote| stored.max(vote.term))
    }
}

/// What the transaction of [`NodeStore::update_vote_state`] writes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VoteWrite {
    /// A term to store as the highest seen; a lower one changes nothing.
    pub term: Option<u64>,
    /// The new support, written before the grant that promises it goes out.
    pub support: Option<StoredSupport>,
    /// The vote of a real promotion vote, written before its `granted`
    /// answer goes out (section 8.9).
    pub vote: Option<StoredVote>,
}

/// Decides the answer to one `support_request` from the vote state `state`
/// (sections 5.27 and 8.8), and what to write with it.
///
/// `voters_version` is `spec.votes.version` of the machine's newest desired
/// state, `None` when it carries no `spec.votes` and the machine is therefore
/// not a voter (section 6.12). `session_primary` is the `primary_node_id` of
/// this session's `welcome`. `received` is the clock read when the request
/// arrived.
///
/// The order:
///
/// 1. The term, on every machine. Section 8.8 "Terms" has every machine,
///    voter or not, refuse a lower term in a `support_request` and keep a
///    higher one, so the term check runs before the voter check; a
///    non-voter then ignores a request of its own term or a higher one. The
///    term of a stored vote counts ([`VoteState::highest`]), and is stored
///    as the highest term when it is above it.
/// 2. A machine that is not a voter ignores the request.
/// 3. A vote in this term for a candidate other than `primary_node_id` is
///    `bad_message` carrying the voter's term (section 5.27, ADR 0280). After
///    step 1 the request's term is the voter's highest. It comes before the
///    floor, so a request from the wrong candidate always learns the term.
/// 4. In a term the voter did not vote in, it supports only the primary of
///    its session (section 8.8 "Terms"): a `primary_node_id` other than the
///    `welcome`'s is `bad_message`, without a term.
/// 5. `support_ms` below 3 000 is `bad_message`, without a term.
/// 6. A host with no `CLOCK_BOOTTIME` or no boot id cannot keep a promise
///    across a restart (section 8.8 "Restarts"): a stored instant would mean
///    nothing to the next agent, and the voter could vote for a candidate
///    while the primary still counts its support. So it grants nothing, as
///    when the store cannot be written: the primary then lacks this voter's
///    support, which is the safe side of ADR 0169.
/// 7. Otherwise the new promise and horizon ([`support_after`]) are written,
///    and the grant follows.
#[must_use]
pub fn decide_support(
    state: &VoteState,
    request: &SupportRequest,
    voters_version: Option<u64>,
    session_primary: Option<&str>,
    received: &Received,
) -> (SupportAnswer, VoteWrite) {
    let highest = state.highest();
    if request.term < highest {
        let write = VoteWrite {
            term: (state.highest_term.unwrap_or(0) < highest).then_some(highest),
            ..VoteWrite::default()
        };
        return (SupportAnswer::Stale { highest }, write);
    }
    let raise = VoteWrite {
        term: (state.highest_term.unwrap_or(0) < request.term).then_some(request.term),
        ..VoteWrite::default()
    };
    let Some(voters_version) = voters_version else {
        return (SupportAnswer::Ignore, raise);
    };
    let voted_in_term = state.vote.as_ref().filter(|vote| vote.term == request.term);
    if let Some(vote) = voted_in_term
        && vote.candidate != request.primary_node_id
    {
        let answer = SupportAnswer::Refuse {
            message: format!(
                "this voter voted in term {} for {}, not for {}",
                vote.term, vote.candidate, request.primary_node_id
            ),
            term: Some(request.term),
        };
        return (answer, raise);
    }
    if voted_in_term.is_none() && session_primary != Some(request.primary_node_id.as_str()) {
        let answer = SupportAnswer::Refuse {
            message: format!(
                "{} is not the primary of this session ({}), and this voter did not vote in \
                 term {}",
                request.primary_node_id,
                session_primary.unwrap_or("none"),
                request.term
            ),
            term: None,
        };
        return (answer, raise);
    }
    if request.support_ms < MIN_SUPPORT_MS {
        let answer = SupportAnswer::Refuse {
            message: format!(
                "support_ms {} is below the {MIN_SUPPORT_MS} ms floor",
                request.support_ms
            ),
            term: None,
        };
        return (answer, raise);
    }
    let (Some(boot_id), Some(v_recv)) = (received.boot_id.as_deref(), received.boottime) else {
        let answer = SupportAnswer::Withhold(
            "this host has no CLOCK_BOOTTIME or no boot id, so a promise could not outlast an \
             agent restart (section 8.8)"
                .to_owned(),
        );
        return (answer, raise);
    };
    let support = support_after(state.support.as_ref(), request, boot_id, v_recv);
    let answer = SupportAnswer::Grant(SupportGrant {
        term: request.term,
        support_ms: request.support_ms,
        max_lease_ms: request.max_lease_ms,
        voters_version,
    });
    (
        answer,
        VoteWrite {
            support: Some(support),
            ..raise
        },
    )
}

/// Answers one `support_request` (sections 5.27 and 8.8): reads the vote
/// state, decides ([`decide_support`]) and writes what the answer promises,
/// in one transaction, and returns the answer only once it committed. A
/// state that cannot be read or written sends nothing.
pub async fn answer_support(
    store: &NodeStore,
    voters_version: Option<u64>,
    session_primary: Option<&str>,
    request: &SupportRequest,
    received: &Received,
) -> SupportAnswer {
    store
        .update_vote_state(|state| {
            decide_support(state, request, voters_version, session_primary, received)
        })
        .await
        .unwrap_or_else(|e| SupportAnswer::Withhold(format!("could not keep the vote state: {e}")))
}

/// What the vote listener answers a `POST /v1/vote` with (section 8.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VoteDecision {
    /// The `200` answer.
    Answer(VoteAnswer),
    /// The voter cannot tell whether it may grant: it answers `503` with this
    /// reason as the text of its `{"error": "<text>"}` body (section 8.9),
    /// and never `granted`.
    Unavailable(String),
}

/// The role a voter must have for its mesh address to ask for votes
/// (section 8.9): only such a machine holds custody seals (ADR 0180).
const NODE_ROLE: &str = "node";

/// An address that may carry a prefix length, as an IP address in its
/// canonical form. `None` for an unspecified or multicast address, which
/// names no one machine: a listener bound to it would not be the address the
/// mesh firewall's vote rules name. A loopback address is kept, and those
/// rules match it as they match any other.
#[must_use]
pub fn mesh_ip(address: &str) -> Option<IpAddr> {
    address
        .split('/')
        .next()?
        .trim()
        .parse::<IpAddr>()
        .ok()
        .map(|ip| ip.to_canonical())
        .filter(|ip| !(ip.is_unspecified() || ip.is_multicast()))
}

/// The mesh addresses that may ask for votes: those of every voter with the
/// `node` role in `votes.voters` (sections 6.9 and 8.9). A voter without a
/// mesh address, or with one that is not an address, is left out.
pub fn node_voter_addresses(votes: &SpecVotes) -> impl Iterator<Item = (&SpecVoter, IpAddr)> {
    votes
        .voters
        .iter()
        .filter(|voter| voter.roles.iter().any(|role| role == NODE_ROLE))
        .filter_map(|voter| Some((voter, mesh_ip(voter.mesh_address.as_deref()?)?)))
}

/// `duration` in whole milliseconds, rounded up: a wait the voter reports
/// is never shorter than the one it measured (section 8.9).
fn ceil_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
}

/// Decides the answer to one `POST /v1/vote` from the vote state `state`
/// read in the transaction (section 8.9), and what to write with it.
///
/// `node_id` and `cluster_id` are this machine's, `votes` is `spec.votes` of
/// its newest stored desired state, read in the same transaction, `peer` the
/// TCP peer address of the connection (never a header), and `received` the
/// clock read when the request arrived. `peer` is `None` when the listener
/// does not take the connection as a source at all: a caller on this machine
/// that is not root (see `crate::vote_listener`).
///
/// The rules are checked in section 8.9's order of the refusals,
/// `not_allowed`, `stale_term`, `voters_changed`, `support_held`, and the
/// first that applies decides. The `503` of a voter that cannot tell comes
/// before `support_held`, which section 8.9 allows ("A `503` may come before
/// a refusal that would otherwise apply"):
///
/// 1. `not_allowed`: another `cluster_id`, no `peer`, a peer that is not the
///    mesh address of a voter with the `node` role, or a `candidate_node_id`
///    other than that voter's. This machine's own mesh address passes when
///    it has the `node` role: the candidate asks its own agent too, as root.
/// 2. `stale_term`: `term` is below the highest term seen, or equals it and
///    the stored vote is not this very (`term`, candidate).
/// 3. `voters_changed`: `voters_version` is not `votes.version`; the answer
///    lists the voter set (section 8.10 rule 4).
/// 4. A host with no `CLOCK_BOOTTIME` or no boot id, or whose stored support
///    is on another boot and was not brought onto this one, cannot tell
///    whether its promise ran out or how long the lease horizon lasts:
///    [`VoteDecision::Unavailable`], never `granted`.
/// 5. `support_held`: the promise has not run out (`now < promise`) and the
///    candidate is not the primary supported; `retry_after_ms` is
///    `promise - now`, rounded up.
/// 6. Otherwise `granted`, with `lease_wait_ms` = `max(0, horizon - now)`,
///    rounded up, or 0 with no support stored.
///
/// Every answer carries the voter's highest term after the request: `T`
/// after a real grant, the unchanged one otherwise, `check` included, so a
/// candidate's `T` from a `check` answer is never pushed up by its own
/// request (section 7.3). Only a real grant that the voter had not granted
/// already writes: the vote (`T`, candidate) and `T` as the highest term,
/// in the same transaction. A refusal and a `check` write nothing, not even
/// a higher term.
#[must_use]
pub fn decide_vote(
    state: &VoteState,
    request: &VoteRequest,
    node_id: &str,
    cluster_id: &str,
    votes: &SpecVotes,
    peer: Option<IpAddr>,
    received: &Received,
) -> (VoteDecision, VoteWrite) {
    let highest = state.highest();
    let answer = |vote: VoteOutcome, term: u64| VoteAnswer {
        node_id: node_id.to_owned(),
        term,
        vote,
        reason: None,
        retry_after_ms: None,
        lease_wait_ms: None,
        voters_version: votes.version,
        voters: None,
    };
    // A refusal writes nothing and carries the unchanged term.
    let refused = |reason: VoteRefusal| VoteAnswer {
        reason: Some(reason),
        ..answer(VoteOutcome::Refused, highest)
    };
    let refuse = |answer: VoteAnswer| (VoteDecision::Answer(answer), VoteWrite::default());

    let source = peer.and_then(|peer| {
        let peer = peer.to_canonical();
        node_voter_addresses(votes).find(|(_, address)| *address == peer)
    });
    if request.cluster_id != cluster_id
        || source.is_none_or(|(voter, _)| voter.node_id != request.candidate_node_id)
    {
        return refuse(refused(VoteRefusal::NotAllowed));
    }

    let granted_already = state.vote.as_ref().is_some_and(|vote| {
        vote.term == request.term && vote.candidate == request.candidate_node_id
    });
    if request.term < highest || (request.term == highest && !granted_already) {
        return refuse(refused(VoteRefusal::StaleTerm));
    }

    if request.voters_version != votes.version {
        let voters = votes
            .voters
            .iter()
            .map(|voter| voter.node_id.clone())
            .collect();
        return refuse(VoteAnswer {
            voters: Some(voters),
            ..refused(VoteRefusal::VotersChanged)
        });
    }

    let (Some(boot_id), Some(now)) = (received.boot_id.as_deref(), received.boottime) else {
        return (
            VoteDecision::Unavailable(
                "this host has no CLOCK_BOOTTIME or no boot id, so it cannot tell whether its \
                 promise ran out"
                    .to_owned(),
            ),
            VoteWrite::default(),
        );
    };
    if let Some(support) = &state.support
        && support.boot_id != boot_id
    {
        return (
            VoteDecision::Unavailable(
                "the stored support is on another boot and was not brought onto this one"
                    .to_owned(),
            ),
            VoteWrite::default(),
        );
    }

    if let Some(support) = &state.support
        && now < support.promise()
        && support.primary_node_id != request.candidate_node_id
    {
        return refuse(VoteAnswer {
            retry_after_ms: Some(ceil_ms(support.promise().saturating_sub(now))),
            ..refused(VoteRefusal::SupportHeld)
        });
    }

    let lease_wait_ms = state
        .support
        .as_ref()
        .map_or(0, |support| ceil_ms(support.horizon().saturating_sub(now)));
    let real = !request.is_check();
    let term = if real { request.term } else { highest };
    let write = if real && !granted_already {
        VoteWrite {
            term: (state.highest_term.unwrap_or(0) < request.term).then_some(request.term),
            vote: Some(StoredVote {
                term: request.term,
                candidate: request.candidate_node_id.clone(),
            }),
            ..VoteWrite::default()
        }
    } else {
        VoteWrite::default()
    };
    (
        VoteDecision::Answer(VoteAnswer {
            lease_wait_ms: Some(lease_wait_ms),
            ..answer(VoteOutcome::Granted, term)
        }),
        write,
    )
}

/// Whether `peer` may ask for votes under `votes` at all: it is the mesh
/// address of a voter with the `node` role (section 8.9). `None`, a caller
/// the listener does not take as a source, never may. A `peer` for which
/// this is false is refused `not_allowed` by [`decide_vote`]'s first rule.
#[must_use]
pub fn may_ask(votes: &SpecVotes, peer: Option<IpAddr>) -> bool {
    peer.is_some_and(|peer| {
        let peer = peer.to_canonical();
        node_voter_addresses(votes).any(|(_, address)| address == peer)
    })
}

/// The `not_allowed` answer of the voter `node_id`, whose highest term is
/// `highest`, under `votes`: the answer [`decide_vote`] gives to a caller
/// that may not ask ([`may_ask`]).
#[must_use]
pub fn not_allowed(node_id: &str, highest: u64, votes: &SpecVotes) -> VoteAnswer {
    VoteAnswer {
        node_id: node_id.to_owned(),
        term: highest,
        vote: VoteOutcome::Refused,
        reason: Some(VoteRefusal::NotAllowed),
        retry_after_ms: None,
        lease_wait_ms: None,
        voters_version: votes.version,
        voters: None,
    }
}

/// The voter's highest term, its stored term or the term of its stored vote,
/// read without a write transaction.
///
/// # Errors
///
/// [`AgentError::Store`] when either cannot be read.
pub async fn current_term(store: &NodeStore) -> Result<u64, AgentError> {
    let stored = store.highest_term().await?.unwrap_or(0);
    let voted = store.vote().await?.map_or(0, |vote| vote.term);
    Ok(stored.max(voted))
}

/// Answers one `POST /v1/vote` (section 8.9): reads the vote state and the
/// newest `spec.votes` (`votes`), decides ([`decide_vote`]) and writes the
/// vote, in one `BEGIN IMMEDIATE` transaction, and returns the answer only
/// once it committed. `spec.votes` is read once the transaction holds the
/// write lock, so the vote is decided against the voter-set version that any
/// `support_grant` written meanwhile carries (section 8.10 rule 4). A state
/// that cannot be read or written, no `spec.votes`, or a `spec.votes` in
/// which votes are not open ([`SpecVotes::open`], sections 6.12 and 8.9) is
/// [`VoteDecision::Unavailable`], never `granted`, and writes nothing. The
/// last is checked here, under the write lock, so a state that closed votes
/// after the caller last looked decides no vote.
pub async fn answer_vote(
    store: &NodeStore,
    request: &VoteRequest,
    node_id: &str,
    cluster_id: &str,
    votes: impl FnOnce() -> Option<SpecVotes>,
    peer: Option<IpAddr>,
    received: &Received,
) -> VoteDecision {
    store
        .update_vote_state(|state| {
            let unavailable = |reason: &str| {
                (
                    VoteDecision::Unavailable(reason.to_owned()),
                    VoteWrite::default(),
                )
            };
            let Some(votes) = votes() else {
                return unavailable("this machine is not a voter");
            };
            if !votes.open() {
                return unavailable("votes closed before this vote was decided");
            }
            decide_vote(state, request, node_id, cluster_id, &votes, peer, received)
        })
        .await
        .unwrap_or_else(|e| {
            VoteDecision::Unavailable(format!("could not keep the vote state: {e}"))
        })
}

/// Brings the stored support onto the current boot at agent start, before any
/// session answers a `support_request` (section 8.8 "Restarts").
///
/// After an agent restart within the same boot nothing changes and the stored
/// promise and horizon keep counting. After a reboot both start again from
/// now ([`StoredSupport::on_boot`]). A host that cannot tell its boot changes
/// nothing; it grants nothing either ([`answer_support`]). The grant path
/// brings a support from another boot onto this one as well, so a failure here
/// is only logged.
///
/// # Errors
///
/// [`AgentError::Store`] when the support cannot be read or written.
pub async fn rebase_at_start(store: &NodeStore, clock: &dyn HostClock) -> Result<(), AgentError> {
    let Some(stored) = store.support().await? else {
        return Ok(());
    };
    let (Some(boot_id), Some(now)) = (clock.boot_id(), clock.boottime()) else {
        warn!(
            "this host has no CLOCK_BOOTTIME or no boot id; the stored support stays as it is \
             and no support is granted"
        );
        return Ok(());
    };
    if stored.boot_id == boot_id {
        return Ok(());
    }
    store.save_support(&stored.on_boot(&boot_id, now)).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quoro_proto::wire::SupportRequest;

    use std::net::IpAddr;

    use quoro_proto::wire::{
        SpecVoter, SpecVotes, VoteAnswer, VoteOutcome, VoteRefusal, VoteRequest,
    };

    use super::{
        Received, StoredSupport, StoredVote, SupportAnswer, TermCheck, VoteDecision, VoteState,
        VoteWrite, answer_support, answer_vote, check_term, decide_support, decide_vote,
        horizon_span, may_ask, mesh_ip, not_allowed, rebase_at_start, support_after,
    };
    use crate::lease::{HostClock, ManualHostClock};
    use crate::store::{NodeStore, VOTE};
    use crate::util;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    /// A mesh address names one machine. An unspecified or multicast
    /// address, in either family or IPv4-mapped, is none.
    #[test]
    fn mesh_ip_refuses_an_unspecified_or_multicast_address() {
        for (address, ip) in [
            ("10.43.0.2/16", "10.43.0.2"),
            (" fd00::2/64", "fd00::2"),
            ("::ffff:10.43.0.2", "10.43.0.2"),
            ("127.0.0.2/8", "127.0.0.2"),
        ] {
            assert_eq!(mesh_ip(address), Some(ip.parse().unwrap()), "{address}");
        }
        for address in [
            "0.0.0.0/16",
            "0.0.0.0",
            "224.0.0.1/4",
            "239.255.255.250",
            "::",
            "::/0",
            "ff02::1",
            "::ffff:0.0.0.0",
            "::ffff:224.0.0.1",
        ] {
            assert_eq!(mesh_ip(address), None, "{address}");
        }
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    /// The `primary_node_id` of the session's `welcome`.
    const PRIMARY: Option<&str> = Some("nod_primary");

    fn request(term: u64, support_ms: u64, max_lease_ms: u64) -> SupportRequest {
        SupportRequest {
            term,
            primary_node_id: "nod_primary".to_owned(),
            support_ms,
            max_lease_ms,
            voters_version: 3120,
        }
    }

    fn at(boot_id: &str, boottime: Duration) -> Received {
        Received {
            boot_id: Some(boot_id.to_owned()),
            boottime: Some(boottime),
        }
    }

    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn open() -> (TempDir, NodeStore) {
        let path = std::env::temp_dir().join(format!(
            "quoro-vote-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        std::fs::create_dir_all(&path).unwrap();
        let store = NodeStore::open(&path).await.unwrap();
        (TempDir(path), store)
    }

    async fn granted(store: &NodeStore, request: &SupportRequest, received: &Received) {
        let answer = answer_support(store, Some(3120), PRIMARY, request, received).await;
        assert!(matches!(answer, SupportAnswer::Grant(_)), "{answer:?}");
    }

    /// Section 8.8: `M_L = max(1000 ms, L / 10)`, so `S + L + 2 M_L` is
    /// 48 s with the defaults and 17 s with `L` = 5 s.
    #[test]
    fn the_horizon_span_uses_the_lease_margin() {
        assert_eq!(horizon_span(15_000, 15_000), ms(15_000 + 15_000 + 3_000));
        assert_eq!(horizon_span(15_000, 5_000), ms(15_000 + 5_000 + 2_000));
        assert_eq!(horizon_span(3_000, 60_000), ms(3_000 + 60_000 + 12_000));
    }

    /// Section 8.8 "Exchange": the promise is `v_recv + S` and the horizon
    /// `v_recv + S + L + 2 M_L`, on `CLOCK_BOOTTIME` of the boot.
    #[test]
    fn a_first_grant_promises_from_v_recv() {
        let support = support_after(None, &request(1, 15_000, 15_000), "boot-a", secs(1000));
        assert_eq!(support.boot_id, "boot-a");
        assert_eq!(support.promise(), secs(1015));
        assert_eq!(support.horizon(), secs(1000 + 15 + 15 + 3));
        assert_eq!((support.support_ms, support.max_lease_ms), (15_000, 15_000));
        assert_eq!(support.primary_node_id, "nod_primary");
    }

    /// Section 8.8: the voter never lowers its horizon within a term, and a
    /// higher term starts no earlier one either; a later one is taken.
    #[test]
    fn the_horizon_is_never_lowered() {
        let first = support_after(None, &request(1, 15_000, 60_000), "boot-a", secs(1000));
        assert_eq!(first.horizon(), secs(1000 + 15 + 60 + 12));

        // A shorter `L` a second later would end earlier: the stored one stays.
        let same_term = support_after(
            Some(&first),
            &request(1, 15_000, 5_000),
            "boot-a",
            secs(1001),
        );
        assert_eq!(same_term.horizon(), first.horizon());
        assert_eq!(same_term.promise(), secs(1016));
        assert_eq!(same_term.max_lease_ms, 5_000, "the last L is recorded");

        // Neither does a higher term lower it.
        let higher = support_after(
            Some(&same_term),
            &request(2, 3_000, 3_000),
            "boot-a",
            secs(1002),
        );
        assert_eq!(higher.horizon(), first.horizon());
        assert_eq!(
            higher.promise(),
            secs(1016),
            "the promise keeps its maximum"
        );
        assert_eq!(higher.term, 2);

        // A later one is taken.
        let later = support_after(
            Some(&higher),
            &request(2, 15_000, 60_000),
            "boot-a",
            secs(1030),
        );
        assert_eq!(later.horizon(), secs(1030 + 15 + 60 + 12));
        assert_eq!(later.promise(), secs(1045));
    }

    /// Section 8.8 "Restarts": on the same boot the stored instants keep
    /// counting; after a reboot both start again from now with the stored
    /// `S` and `L`.
    #[test]
    fn a_reboot_starts_the_promise_and_the_horizon_again() {
        let stored = support_after(None, &request(1, 15_000, 15_000), "boot-a", secs(1000));
        assert_eq!(stored.on_boot("boot-a", secs(1010)), stored);

        let rebooted = stored.on_boot("boot-b", secs(7));
        assert_eq!(rebooted.boot_id, "boot-b");
        assert_eq!(rebooted.promise(), secs(7 + 15));
        assert_eq!(rebooted.horizon(), secs(7 + 15 + 15 + 3));
        assert_eq!((rebooted.term, rebooted.support_ms), (1, 15_000));

        // A longer window granted before the last one is not cut short.
        let long = support_after(None, &request(1, 60_000, 15_000), "boot-a", secs(1000));
        let short = support_after(
            Some(&long),
            &request(1, 15_000, 15_000),
            "boot-a",
            secs(1005),
        );
        assert_eq!(short.promise(), secs(1060));
        assert_eq!(short.horizon(), secs(1000 + 60 + 15 + 3));
        let rebooted = short.on_boot("boot-b", secs(7));
        assert_eq!(rebooted.promise(), secs(7 + 55));
        assert_eq!(rebooted.horizon(), secs(7 + 73));
        assert_eq!(rebooted.support_ms, 15_000, "the last S is kept");
    }

    /// Section 8.8 "Terms": a lower term is stale, a higher one is stored, and
    /// the stored term never goes down.
    #[tokio::test]
    async fn the_highest_term_is_kept_and_never_lowered() {
        let (_dir, store) = open().await;
        assert_eq!(store.highest_term().await.unwrap(), None);
        assert_eq!(check_term(&store, 1).await.unwrap(), TermCheck::Raised);
        assert_eq!(check_term(&store, 1).await.unwrap(), TermCheck::Current);
        assert_eq!(check_term(&store, 3).await.unwrap(), TermCheck::Raised);
        assert_eq!(
            check_term(&store, 2).await.unwrap(),
            TermCheck::Stale { highest: 3 }
        );
        store.raise_term(2).await.unwrap();
        assert_eq!(store.highest_term().await.unwrap(), Some(3));
        store.raise_term(10).await.unwrap();
        assert_eq!(store.highest_term().await.unwrap(), Some(10));
    }

    /// Section 8.8: the grant is returned only once the promise and the
    /// horizon are stored, with the values the clock gave.
    #[tokio::test]
    async fn a_grant_follows_the_stored_promise() {
        let (_dir, store) = open().await;
        let answer = answer_support(
            &store,
            Some(3120),
            PRIMARY,
            &request(2, 15_000, 15_000),
            &at("boot-a", secs(500)),
        )
        .await;
        let SupportAnswer::Grant(grant) = answer else {
            panic!("not a grant: {answer:?}");
        };
        assert_eq!(
            (
                grant.term,
                grant.support_ms,
                grant.max_lease_ms,
                grant.voters_version
            ),
            (2, 15_000, 15_000, 3120)
        );
        let stored = store
            .support()
            .await
            .unwrap()
            .expect("stored before the grant");
        assert_eq!(stored.promise(), secs(515));
        assert_eq!(stored.horizon(), secs(500 + 33));
        assert_eq!(stored.boot_id, "boot-a");
        assert_eq!(store.highest_term().await.unwrap(), Some(2));
    }

    /// Sections 5.27 and 8.8: a host that cannot tell its boot grants nothing
    /// and stores nothing; the term is still kept.
    #[tokio::test]
    async fn a_host_without_boottime_or_boot_id_grants_nothing() {
        let (_dir, store) = open().await;
        for received in [
            Received {
                boot_id: None,
                boottime: Some(secs(500)),
            },
            Received {
                boot_id: Some("boot-a".to_owned()),
                boottime: None,
            },
        ] {
            let answer = answer_support(
                &store,
                Some(3120),
                PRIMARY,
                &request(1, 15_000, 15_000),
                &received,
            )
            .await;
            assert!(matches!(answer, SupportAnswer::Withhold(_)), "{answer:?}");
        }
        assert_eq!(store.support().await.unwrap(), None);
        assert_eq!(store.highest_term().await.unwrap(), Some(1));
    }

    /// Section 5.27: a lower term is stale on every machine, a non-voter
    /// ignores the rest, the floor and a vote for another candidate are
    /// refused, and a vote for this primary is granted.
    #[tokio::test]
    async fn the_refusals_come_in_the_section_order() {
        let (_dir, store) = open().await;
        let now = at("boot-a", secs(500));
        store.raise_term(4).await.unwrap();
        assert_eq!(
            answer_support(&store, None, PRIMARY, &request(3, 15_000, 15_000), &now).await,
            SupportAnswer::Stale { highest: 4 }
        );
        assert_eq!(
            answer_support(&store, None, PRIMARY, &request(4, 15_000, 15_000), &now).await,
            SupportAnswer::Ignore
        );
        assert!(matches!(
            answer_support(&store, Some(1), PRIMARY, &request(4, 2_999, 15_000), &now).await,
            SupportAnswer::Refuse { term: None, .. }
        ));
        let vote = StoredVote {
            term: 4,
            candidate: "nod_other".to_owned(),
        };
        store
            .set_meta(VOTE, &serde_json::to_string(&vote).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            answer_support(&store, Some(1), PRIMARY, &request(4, 15_000, 15_000), &now).await,
            SupportAnswer::Refuse { term: Some(4), .. }
        ));
        assert_eq!(store.support().await.unwrap(), None, "nothing promised");
        // A vote in an older term binds nothing in this one.
        granted(&store, &request(5, 15_000, 15_000), &now).await;
        assert_eq!(store.highest_term().await.unwrap(), Some(5));
    }

    /// The time the clock read for the stored support's `v_recv`, or for the
    /// agent's start that brought it onto a new boot.
    fn received(support: &StoredSupport) -> Duration {
        Duration::from_nanos(support.received_ns)
    }

    /// Section 8.8 "Restarts": an agent restart on the same boot leaves the
    /// stored promise and horizon as they were, so they keep counting.
    ///
    /// The manual clock follows the tokio clock, which these tests leave
    /// running: the store's pool times out on a paused one.
    #[tokio::test]
    async fn an_agent_restart_on_the_same_boot_keeps_counting() {
        let (_dir, store) = open().await;
        let clock = Arc::new(ManualHostClock::new(Some("boot-a"), secs(1000)));
        granted(
            &store,
            &request(1, 15_000, 15_000),
            &Received::read(&*clock),
        )
        .await;
        let before = store.support().await.unwrap().unwrap();
        assert_eq!(before.promise(), received(&before) + secs(15));

        tokio::time::sleep(ms(20)).await;
        rebase_at_start(&store, &*clock).await.unwrap();
        assert_eq!(store.support().await.unwrap().unwrap(), before);
        assert!(clock.boottime().unwrap() >= received(&before) + ms(20));
    }

    /// Section 8.8 "Restarts": after a reboot the promise and the horizon
    /// start again from the agent's first start, with the stored `S` and `L`.
    #[tokio::test]
    async fn a_reboot_starts_both_again_from_the_first_start() {
        let (_dir, store) = open().await;
        let clock = Arc::new(ManualHostClock::new(Some("boot-a"), secs(1000)));
        granted(&store, &request(1, 15_000, 5_000), &Received::read(&*clock)).await;
        let before = store.support().await.unwrap().unwrap();
        assert_eq!(before.horizon(), received(&before) + secs(15 + 5 + 2));

        // The new boot's CLOCK_BOOTTIME starts low again: the old instants,
        // read on it, would be far in the future.
        clock.reboot("boot-b", secs(12));
        rebase_at_start(&store, &*clock).await.unwrap();
        let after = store.support().await.unwrap().unwrap();
        let start = received(&after);
        assert!(start >= secs(12) && start < secs(13), "{start:?}");
        assert_eq!(after.boot_id, "boot-b");
        assert_eq!(after.promise(), start + secs(15));
        assert_eq!(after.horizon(), start + secs(15 + 5 + 2));
        assert_eq!(
            (after.term, after.support_ms, after.max_lease_ms),
            (1, 15_000, 5_000)
        );

        // A second start on the new boot keeps what the first one stored.
        tokio::time::sleep(ms(20)).await;
        rebase_at_start(&store, &*clock).await.unwrap();
        assert_eq!(store.support().await.unwrap().unwrap(), after);
    }

    /// A start on a host that cannot tell its boot changes nothing.
    #[tokio::test]
    async fn a_start_without_a_boot_id_changes_nothing() {
        let (_dir, store) = open().await;
        let support = support_after(None, &request(1, 15_000, 15_000), "boot-a", secs(1000));
        store.save_support(&support).await.unwrap();
        let clock = ManualHostClock::new(None, secs(3));
        rebase_at_start(&store, &clock).await.unwrap();
        assert_eq!(store.support().await.unwrap(), Some(support));
    }

    /// Section 8.8: a grant on a boot whose start-up could not bring the
    /// stored support onto it still never ends earlier than the reboot rule.
    #[tokio::test]
    async fn a_grant_after_an_unrebased_reboot_errs_late() {
        let (_dir, store) = open().await;
        let support = support_after(None, &request(1, 60_000, 60_000), "boot-a", secs(1000));
        store.save_support(&support).await.unwrap();
        granted(&store, &request(1, 15_000, 15_000), &at("boot-b", secs(20))).await;
        let stored = store.support().await.unwrap().unwrap();
        assert_eq!(stored.boot_id, "boot-b");
        assert_eq!(stored.promise(), secs(20 + 60));
        assert_eq!(stored.horizon(), secs(20 + 60 + 60 + 12));
    }

    fn store_vote(term: u64, candidate: &str) -> String {
        serde_json::to_string(&StoredVote {
            term,
            candidate: candidate.to_owned(),
        })
        .unwrap()
    }

    /// Section 8.8 "Terms": a stored vote above the request's term makes the
    /// request stale, carrying the higher of the vote's term and the stored
    /// one, even when the stored term was not raised with the vote; the
    /// vote's term is then stored. The welcome's and the desired state's
    /// check count it too.
    #[tokio::test]
    async fn a_vote_above_the_request_term_makes_it_stale() {
        let (_dir, store) = open().await;
        let now = at("boot-a", secs(500));
        store.raise_term(2).await.unwrap();
        store
            .set_meta(VOTE, &store_vote(4, "nod_primary"))
            .await
            .unwrap();
        assert_eq!(
            answer_support(&store, Some(1), PRIMARY, &request(3, 15_000, 15_000), &now).await,
            SupportAnswer::Stale { highest: 4 }
        );
        assert_eq!(store.highest_term().await.unwrap(), Some(4));
        assert_eq!(store.support().await.unwrap(), None);
        assert_eq!(
            check_term(&store, 3).await.unwrap(),
            TermCheck::Stale { highest: 4 }
        );

        // A stored term above the vote's is the one carried.
        store.raise_term(6).await.unwrap();
        assert_eq!(
            answer_support(&store, Some(1), PRIMARY, &request(5, 15_000, 15_000), &now).await,
            SupportAnswer::Stale { highest: 6 }
        );
    }

    /// Section 8.8 "Terms": the voter decides on the vote state as one
    /// transaction reads it, and what it promises is written in the same
    /// transaction. A vote for another candidate in the request's term
    /// refuses it; with no vote the same request is granted.
    #[test]
    fn the_decision_reads_the_vote_with_the_support() {
        let now = at("boot-a", secs(500));
        let request = request(3, 15_000, 15_000);
        let voted = VoteState {
            highest_term: Some(3),
            vote: Some(StoredVote {
                term: 3,
                candidate: "nod_other".to_owned(),
            }),
            support: None,
        };
        let (answer, write) = decide_support(&voted, &request, Some(1), PRIMARY, &now);
        assert!(matches!(
            answer,
            SupportAnswer::Refuse { term: Some(3), .. }
        ));
        assert_eq!(write, VoteWrite::default(), "nothing written");

        let not_voted = VoteState {
            highest_term: Some(2),
            ..VoteState::default()
        };
        let (answer, write) = decide_support(&not_voted, &request, Some(1), PRIMARY, &now);
        assert!(matches!(answer, SupportAnswer::Grant(_)), "{answer:?}");
        assert_eq!(write.term, Some(3), "the higher term is stored with it");
        assert_eq!(write.support.unwrap().promise(), secs(515));
    }

    /// Section 5.27: the vote is checked before the floor, so a request from
    /// the wrong candidate learns the voter's term even when its window is
    /// also below the floor.
    #[tokio::test]
    async fn the_vote_is_checked_before_the_floor() {
        let (_dir, store) = open().await;
        store
            .set_meta(VOTE, &store_vote(2, "nod_other"))
            .await
            .unwrap();
        let answer = answer_support(
            &store,
            Some(1),
            PRIMARY,
            &request(2, 2_999, 15_000),
            &at("boot-a", secs(500)),
        )
        .await;
        assert!(
            matches!(answer, SupportAnswer::Refuse { term: Some(2), .. }),
            "{answer:?}"
        );
    }

    /// Section 8.8 "Terms": in a term it did not vote in, a voter supports
    /// only the primary of its session; in a term it voted in, the candidate
    /// it voted for, whoever the session's primary is.
    #[tokio::test]
    async fn without_a_vote_only_the_session_s_primary_is_supported() {
        let (_dir, store) = open().await;
        let now = at("boot-a", secs(500));
        let mut other = request(2, 15_000, 15_000);
        other.primary_node_id = "nod_other".to_owned();
        let answer = answer_support(&store, Some(1), PRIMARY, &other, &now).await;
        assert!(
            matches!(answer, SupportAnswer::Refuse { term: None, .. }),
            "{answer:?}"
        );
        let answer = answer_support(&store, Some(1), None, &request(2, 15_000, 15_000), &now).await;
        assert!(
            matches!(answer, SupportAnswer::Refuse { term: None, .. }),
            "no session primary, no support: {answer:?}"
        );
        assert_eq!(store.support().await.unwrap(), None, "nothing promised");

        store
            .set_meta(VOTE, &store_vote(2, "nod_other"))
            .await
            .unwrap();
        let answer = answer_support(&store, Some(1), PRIMARY, &other, &now).await;
        assert!(matches!(answer, SupportAnswer::Grant(_)), "{answer:?}");
    }

    /// Section 8.9 needs that the primary held no support from a voter once
    /// it had voted. A vote written while a `support_request` is answered
    /// either commits first and refuses the request, or waits for the
    /// answer's promise: the answer's reads and its write are one
    /// transaction, so it cannot grant on the state from before the vote.
    #[tokio::test]
    async fn a_vote_written_meanwhile_is_seen_by_the_answer() {
        let (_dir, store) = open().await;
        store.raise_term(2).await.unwrap();
        // The vote listener of section 8.9 takes the write lock first.
        let mut voting = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO node_meta (key, value) VALUES (?, ?)")
            .bind(VOTE)
            .bind(store_vote(2, "nod_other"))
            .execute(&mut *voting)
            .await
            .unwrap();

        let answering = {
            let store = store.clone();
            tokio::spawn(async move {
                answer_support(
                    &store,
                    Some(1),
                    PRIMARY,
                    &request(2, 15_000, 15_000),
                    &at("boot-a", secs(500)),
                )
                .await
            })
        };
        tokio::time::sleep(ms(200)).await;
        assert!(!answering.is_finished(), "the answer waits for the vote");
        voting.commit().await.unwrap();

        let answer = answering.await.unwrap();
        assert!(
            matches!(answer, SupportAnswer::Refuse { term: Some(2), .. }),
            "{answer:?}"
        );
        assert_eq!(store.support().await.unwrap(), None, "nothing promised");
    }

    // ---- Promotion votes (section 8.9) ----

    /// This machine: node B of section 6.12's example.
    const ME: &str = "nod_b";
    const CLUSTER: &str = "cls_1";
    const A: &str = "10.43.0.2";
    const B: &str = "10.43.0.3";
    const WITNESS: &str = "10.43.0.1";

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    /// Two voters with the `node` role, A and this machine B, and a witness.
    fn spec_votes() -> SpecVotes {
        let voter = |node_id: &str, roles: &[&str], address: &str| SpecVoter {
            node_id: node_id.to_owned(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            mesh_address: Some(address.to_owned()),
        };
        SpecVotes {
            version: 3120,
            port: 51821,
            voters: vec![
                voter("nod_a", &["node"], A),
                voter(ME, &["node"], B),
                voter("nod_w", &["edge", "witness"], WITNESS),
            ],
            listen: None,
        }
    }

    fn vote_request(term: u64, candidate: &str) -> VoteRequest {
        VoteRequest {
            cluster_id: CLUSTER.to_owned(),
            term,
            candidate_node_id: candidate.to_owned(),
            voters_version: 3120,
            check: None,
        }
    }

    fn checked(request: &VoteRequest) -> VoteRequest {
        VoteRequest {
            check: Some(true),
            ..request.clone()
        }
    }

    /// The support of `primary`, received at 490 s on boot-a with `S` =
    /// 15 s and `L` = 15 s: promise at 505 s, horizon at 523 s.
    fn support_of(primary: &str) -> StoredSupport {
        let mut request = request(1, 15_000, 15_000);
        request.primary_node_id = primary.to_owned();
        support_after(None, &request, "boot-a", secs(490))
    }

    fn state(highest: Option<u64>, vote: Option<(u64, &str)>) -> VoteState {
        VoteState {
            highest_term: highest,
            vote: vote.map(|(term, candidate)| StoredVote {
                term,
                candidate: candidate.to_owned(),
            }),
            support: None,
        }
    }

    fn decide_at(
        state: &VoteState,
        request: &VoteRequest,
        peer: &str,
        now: &Received,
    ) -> (VoteDecision, VoteWrite) {
        decide_vote(
            state,
            request,
            ME,
            CLUSTER,
            &spec_votes(),
            Some(ip(peer)),
            now,
        )
    }

    fn decide(state: &VoteState, request: &VoteRequest, peer: &str) -> (VoteDecision, VoteWrite) {
        decide_at(state, request, peer, &at("boot-a", secs(500)))
    }

    fn answered(decision: VoteDecision) -> VoteAnswer {
        match decision {
            VoteDecision::Answer(answer) => answer,
            VoteDecision::Unavailable(reason) => panic!("unavailable: {reason}"),
        }
    }

    /// The reason of a refusal that writes nothing.
    fn refusal(state: &VoteState, request: &VoteRequest, peer: &str) -> VoteRefusal {
        let (decision, write) = decide(state, request, peer);
        let answer = answered(decision);
        assert_eq!(answer.vote, VoteOutcome::Refused, "{answer:?}");
        assert_eq!(write, VoteWrite::default(), "a refusal writes nothing");
        assert_eq!(answer.term, state.highest(), "the term is unchanged");
        assert_eq!((answer.node_id.as_str(), answer.voters_version), (ME, 3120));
        answer.reason.expect("a refusal has a reason")
    }

    /// Section 8.9: a real vote from a voter with the `node` role is granted,
    /// carries `T`, and writes the vote and `T` as the highest term.
    #[test]
    fn a_node_voter_s_vote_is_granted_and_written() {
        let (decision, write) = decide(&state(Some(1), None), &vote_request(2, "nod_a"), A);
        assert_eq!(
            answered(decision),
            VoteAnswer {
                node_id: ME.to_owned(),
                term: 2,
                vote: VoteOutcome::Granted,
                reason: None,
                retry_after_ms: None,
                lease_wait_ms: Some(0),
                voters_version: 3120,
                voters: None,
            }
        );
        assert_eq!(
            write,
            VoteWrite {
                term: Some(2),
                vote: Some(StoredVote {
                    term: 2,
                    candidate: "nod_a".to_owned(),
                }),
                support: None,
            }
        );
        // The candidate asks its own agent too: this machine's own address
        // passes, since it has the `node` role.
        let (decision, _) = decide(&state(Some(1), None), &vote_request(2, ME), B);
        assert_eq!(answered(decision).vote, VoteOutcome::Granted);
    }

    /// Section 8.9: another cluster, a source that is not a voter with the
    /// `node` role, and a candidate that is not the source's voter are
    /// `not_allowed`.
    #[test]
    fn requests_from_outside_the_node_voters_are_not_allowed() {
        let state = state(Some(1), None);
        let mut other_cluster = vote_request(2, "nod_a");
        other_cluster.cluster_id = "cls_other".to_owned();
        assert_eq!(refusal(&state, &other_cluster, A), VoteRefusal::NotAllowed);
        assert_eq!(
            refusal(&state, &vote_request(2, "nod_a"), "10.43.0.9"),
            VoteRefusal::NotAllowed,
            "an address not in spec.votes"
        );
        assert_eq!(
            refusal(&state, &vote_request(2, "nod_w"), WITNESS),
            VoteRefusal::NotAllowed,
            "a witness-only voter"
        );
        assert_eq!(
            refusal(&state, &vote_request(2, ME), A),
            VoteRefusal::NotAllowed,
            "a candidate that is not the source's voter"
        );
        assert_eq!(
            refusal(&state, &vote_request(2, "nod_a"), "::ffff:10.43.0.9"),
            VoteRefusal::NotAllowed
        );
        // An IPv4-mapped peer address is the voter's address.
        let (decision, _) = decide(&state, &vote_request(2, "nod_a"), "::ffff:10.43.0.2");
        assert_eq!(answered(decision).vote, VoteOutcome::Granted);

        // A connection the listener does not take as a source (a caller on
        // this machine that is not root) is `not_allowed`, even for a vote
        // this machine would grant to itself, and writes nothing.
        let request = vote_request(2, ME);
        let (decision, write) = decide_vote(
            &state,
            &request,
            ME,
            CLUSTER,
            &spec_votes(),
            None,
            &at("boot-a", secs(500)),
        );
        let answer = answered(decision);
        assert_eq!(
            (answer.vote, answer.reason, answer.term),
            (VoteOutcome::Refused, Some(VoteRefusal::NotAllowed), 1)
        );
        assert_eq!(write, VoteWrite::default());
    }

    /// The listener's early refusal matches [`decide_vote`]: a peer that may
    /// not ask is exactly one the first rule refuses, with the same answer,
    /// whatever else the request would fail; a node voter's address may ask.
    #[test]
    fn the_early_refusal_is_the_first_rule_s_answer() {
        let votes = spec_votes();
        let mut state = state(Some(4), Some((4, ME)));
        state.support = Some(support_of("nod_other"));
        let mut request = vote_request(3, "nod_a");
        request.voters_version = 1;
        for peer in [
            Some("10.43.0.9"),
            Some(WITNESS),
            Some("::ffff:10.43.0.9"),
            None,
        ] {
            let peer = peer.map(ip);
            assert!(!may_ask(&votes, peer), "{peer:?}");
            let (decision, write) = decide_vote(
                &state,
                &request,
                ME,
                CLUSTER,
                &votes,
                peer,
                &at("boot-a", secs(500)),
            );
            assert_eq!(write, VoteWrite::default());
            assert_eq!(
                answered(decision),
                not_allowed(ME, state.highest(), &votes),
                "{peer:?}"
            );
        }
        for peer in [A, B, "::ffff:10.43.0.2"] {
            assert!(may_ask(&votes, Some(ip(peer))), "{peer}");
        }
    }

    /// Section 8.9 rule 1: a term below the voter's, or equal to it without
    /// this very vote, is `stale_term`; a vote at the current term for
    /// another candidate refuses this one.
    #[test]
    fn a_term_not_above_the_voter_s_is_stale() {
        let request = vote_request(3, "nod_a");
        assert_eq!(
            refusal(&state(Some(4), None), &request, A),
            VoteRefusal::StaleTerm
        );
        assert_eq!(
            refusal(&state(Some(3), None), &request, A),
            VoteRefusal::StaleTerm
        );
        assert_eq!(
            refusal(&state(Some(3), Some((3, ME))), &request, A),
            VoteRefusal::StaleTerm,
            "the voter voted in term 3 for another candidate"
        );
        assert_eq!(
            refusal(&state(Some(2), Some((3, ME))), &request, A),
            VoteRefusal::StaleTerm,
            "the vote's term counts even if the stored term was not raised"
        );
        assert_eq!(
            refusal(&state(Some(4), Some((3, "nod_a"))), &request, A),
            VoteRefusal::StaleTerm,
            "a vote for this candidate in a lower term binds nothing"
        );
    }

    /// Section 8.9 rule 1: a repeated request for a term the voter already
    /// granted to this candidate is granted again and writes nothing new.
    #[test]
    fn a_repeated_grant_is_granted_and_writes_nothing() {
        let state = state(Some(3), Some((3, "nod_a")));
        let (decision, write) = decide(&state, &vote_request(3, "nod_a"), A);
        let answer = answered(decision);
        assert_eq!((answer.vote, answer.term), (VoteOutcome::Granted, 3));
        assert_eq!(write, VoteWrite::default());
    }

    /// Section 8.10 rule 4: another voter-set version is `voters_changed`,
    /// with the voter's own set.
    #[test]
    fn another_voters_version_is_voters_changed() {
        let mut request = vote_request(2, "nod_a");
        request.voters_version = 3119;
        let state = state(Some(1), None);
        assert_eq!(refusal(&state, &request, A), VoteRefusal::VotersChanged);
        let answer = answered(decide(&state, &request, A).0);
        assert_eq!(
            answer.voters,
            Some(vec!["nod_a".to_owned(), ME.to_owned(), "nod_w".to_owned()])
        );
        assert_eq!(answer.voters_version, 3120);
    }

    /// Section 8.9 rule 2: while the promise to another primary runs, the
    /// vote is `support_held` with the rest of the promise, rounded up; the
    /// primary it supports is granted, as is anyone once it ran out.
    #[test]
    fn a_running_promise_holds_the_vote() {
        let mut state = state(Some(1), None);
        state.support = Some(support_of("nod_other"));
        let request = vote_request(2, "nod_a");
        assert_eq!(refusal(&state, &request, A), VoteRefusal::SupportHeld);
        let held = |now: Duration| {
            answered(decide_at(&state, &request, A, &at("boot-a", now)).0).retry_after_ms
        };
        assert_eq!(held(secs(500)), Some(5_000));
        assert_eq!(held(secs(500) + Duration::from_micros(400)), Some(5_000));
        assert_eq!(held(secs(505) - Duration::from_nanos(1)), Some(1));
        // At the promise itself it has run out.
        let (decision, write) = decide_at(&state, &request, A, &at("boot-a", secs(505)));
        let answer = answered(decision);
        assert_eq!(answer.vote, VoteOutcome::Granted);
        assert_eq!(answer.lease_wait_ms, Some(18_000));
        assert_eq!(write.vote.unwrap().term, 2);

        // The primary it supports may take the term while the promise runs.
        state.support = Some(support_of("nod_a"));
        let answer = answered(decide(&state, &request, A).0);
        assert_eq!(answer.vote, VoteOutcome::Granted);
        assert_eq!(answer.lease_wait_ms, Some(23_000));
    }

    /// Section 8.9: `lease_wait_ms` is the rest of the lease horizon, rounded
    /// up to whole ms, 0 once it passed, and 0 with no support stored.
    #[test]
    fn the_lease_wait_is_the_rest_of_the_horizon() {
        let mut state = state(Some(1), None);
        state.support = Some(support_of("nod_a"));
        let request = vote_request(2, "nod_a");
        let wait = |now: Duration| {
            answered(decide_at(&state, &request, A, &at("boot-a", now)).0).lease_wait_ms
        };
        assert_eq!(wait(secs(500)), Some(23_000));
        assert_eq!(wait(secs(500) + Duration::from_nanos(1)), Some(23_000));
        assert_eq!(wait(secs(523) - Duration::from_micros(1_500)), Some(2));
        assert_eq!(wait(secs(523)), Some(0));
        assert_eq!(wait(secs(600)), Some(0));
    }

    /// Section 8.9: the first rule that fails decides. A request that fails
    /// two rules gets the first one's reason.
    #[test]
    fn the_first_failing_rule_decides() {
        let mut state = state(Some(4), None);
        state.support = Some(support_of("nod_other"));
        let mut changed = vote_request(3, "nod_a");
        changed.voters_version = 1;
        // not_allowed before stale_term.
        assert_eq!(refusal(&state, &changed, WITNESS), VoteRefusal::NotAllowed);
        // stale_term before voters_changed.
        assert_eq!(refusal(&state, &changed, A), VoteRefusal::StaleTerm);
        // voters_changed before support_held.
        changed.term = 5;
        assert_eq!(refusal(&state, &changed, A), VoteRefusal::VotersChanged);
        // support_held last.
        assert_eq!(
            refusal(&state, &vote_request(5, "nod_a"), A),
            VoteRefusal::SupportHeld
        );
        // A host that cannot read its clock still refuses on the clock-free
        // rules, and never grants.
        let blind = Received {
            boot_id: None,
            boottime: None,
        };
        let (decision, _) = decide_at(&state, &changed, A, &blind);
        assert_eq!(answered(decision).reason, Some(VoteRefusal::VotersChanged));
    }

    /// Sections 7.3 and 8.9: a `check` answers what a real vote would, with
    /// the voter's unchanged term, and writes nothing, not even a higher
    /// term.
    #[test]
    fn a_check_answers_as_a_vote_and_writes_nothing() {
        let mut held = state(Some(1), None);
        held.support = Some(support_of("nod_other"));
        let cases = [
            (state(Some(1), None), vote_request(2, "nod_a"), A),
            (state(Some(4), None), vote_request(3, "nod_a"), A),
            (state(Some(1), None), vote_request(2, "nod_w"), WITNESS),
            (held, vote_request(2, "nod_a"), A),
            (
                state(Some(3), Some((3, "nod_a"))),
                vote_request(3, "nod_a"),
                A,
            ),
        ];
        for (state, request, peer) in cases {
            let (real, _) = decide(&state, &request, peer);
            let (check, write) = decide(&state, &checked(&request), peer);
            assert_eq!(write, VoteWrite::default(), "{request:?}");
            let (real, check) = (answered(real), answered(check));
            assert_eq!(check.term, state.highest(), "the term is unchanged");
            assert_eq!(
                VoteAnswer {
                    term: real.term,
                    ..check
                },
                real
            );
        }
    }

    /// Section 8.8 "Restarts": a support from another boot that the start
    /// did not bring onto this one, or a host that cannot read its clock or
    /// boot id, never grants.
    #[test]
    fn an_unrebased_support_or_a_blind_clock_never_grants() {
        let mut state = state(Some(1), None);
        state.support = Some(support_of("nod_a"));
        let request = vote_request(2, "nod_a");
        let (decision, write) = decide_at(&state, &request, A, &at("boot-b", secs(9_000)));
        assert!(
            matches!(decision, VoteDecision::Unavailable(_)),
            "{decision:?}"
        );
        assert_eq!(write, VoteWrite::default());

        state.support = None;
        for blind in [
            Received {
                boot_id: None,
                boottime: Some(secs(500)),
            },
            Received {
                boot_id: Some("boot-a".to_owned()),
                boottime: None,
            },
        ] {
            let (decision, write) = decide_at(&state, &request, A, &blind);
            assert!(
                matches!(decision, VoteDecision::Unavailable(_)),
                "{decision:?}"
            );
            assert_eq!(write, VoteWrite::default());
        }
    }

    /// Answers `request` from `peer` under [`spec_votes`] with `listen` set
    /// to `listen`.
    async fn vote_listen(
        store: &NodeStore,
        request: &VoteRequest,
        peer: &str,
        listen: Option<bool>,
    ) -> VoteDecision {
        answer_vote(
            store,
            request,
            ME,
            CLUSTER,
            || {
                Some(SpecVotes {
                    listen,
                    ..spec_votes()
                })
            },
            Some(ip(peer)),
            &at("boot-a", secs(500)),
        )
        .await
    }

    /// Answers `request` from `peer` while votes are open.
    async fn vote(store: &NodeStore, request: &VoteRequest, peer: &str) -> VoteDecision {
        vote_listen(store, request, peer, Some(true)).await
    }

    /// Sections 6.12 and 8.9: a `spec.votes` in which votes are not open,
    /// read inside the transaction, decides no vote and writes nothing,
    /// whatever the request; the same request is granted once votes are
    /// open.
    #[tokio::test]
    async fn a_vote_while_votes_are_closed_is_unavailable_and_writes_nothing() {
        let (_dir, store) = open().await;
        store.raise_term(1).await.unwrap();
        let request = vote_request(2, "nod_a");
        for listen in [None, Some(false)] {
            let decision = vote_listen(&store, &request, A, listen).await;
            assert_eq!(
                decision,
                VoteDecision::Unavailable("votes closed before this vote was decided".to_owned()),
                "listen {listen:?}"
            );
            assert_eq!(store.vote().await.unwrap(), None, "listen {listen:?}");
            assert_eq!(
                store.highest_term().await.unwrap(),
                Some(1),
                "listen {listen:?}"
            );
        }
        let answer = answered(vote_listen(&store, &request, A, Some(true)).await);
        assert_eq!((answer.vote, answer.term), (VoteOutcome::Granted, 2));
    }

    /// Sections 8.8 and 8.9: a granted vote is stored with its term before
    /// the answer, survives an agent restart, and then refuses a lower term
    /// to the old primary's `support_request` and another candidate's vote
    /// at the same term; a `check` stored nothing before it.
    #[tokio::test]
    async fn a_granted_vote_survives_a_restart_and_refuses_lower_terms() {
        let (dir, store) = open().await;
        store.raise_term(1).await.unwrap();
        let check = vote(&store, &checked(&vote_request(2, "nod_a")), A).await;
        assert_eq!(answered(check).term, 1);
        assert_eq!(store.vote().await.unwrap(), None, "a check writes nothing");
        assert_eq!(store.highest_term().await.unwrap(), Some(1));

        let answer = answered(vote(&store, &vote_request(2, "nod_a"), A).await);
        assert_eq!((answer.vote, answer.term), (VoteOutcome::Granted, 2));
        store.pool().close().await;
        drop(store);

        let store = NodeStore::open(&dir.0).await.unwrap();
        assert_eq!(
            store.vote().await.unwrap(),
            Some(StoredVote {
                term: 2,
                candidate: "nod_a".to_owned(),
            })
        );
        assert_eq!(store.highest_term().await.unwrap(), Some(2));
        assert_eq!(
            answer_support(
                &store,
                Some(3120),
                PRIMARY,
                &request(1, 15_000, 15_000),
                &at("boot-a", secs(500)),
            )
            .await,
            SupportAnswer::Stale { highest: 2 }
        );
        let other = answered(vote(&store, &vote_request(2, ME), B).await);
        assert_eq!(other.reason, Some(VoteRefusal::StaleTerm));
        let again = answered(vote(&store, &vote_request(2, "nod_a"), A).await);
        assert_eq!(again.vote, VoteOutcome::Granted, "a repeated grant");
    }

    /// Section 8.9: a vote that cannot be written is never answered
    /// `granted`, and neither its term nor the vote is kept.
    #[tokio::test]
    async fn a_vote_that_cannot_be_written_is_never_granted() {
        let (_dir, store) = open().await;
        store.raise_term(1).await.unwrap();
        for event in ["INSERT", "UPDATE"] {
            sqlx::query(&format!(
                "CREATE TRIGGER no_vote_{event} BEFORE {event} ON node_meta \
                 WHEN NEW.key = '{VOTE}' BEGIN SELECT RAISE(ABORT, 'read-only'); END"
            ))
            .execute(store.pool())
            .await
            .unwrap();
        }
        let decision = vote(&store, &vote_request(2, "nod_a"), A).await;
        assert!(
            matches!(decision, VoteDecision::Unavailable(_)),
            "{decision:?}"
        );
        assert_eq!(store.vote().await.unwrap(), None);
        assert_eq!(store.highest_term().await.unwrap(), Some(1), "rolled back");
    }
}
