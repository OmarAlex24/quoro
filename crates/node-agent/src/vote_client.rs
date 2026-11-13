// SPDX-License-Identifier: AGPL-3.0-only
//! The candidate's side of a promotion vote (`node-agent-protocol.md`
//! section 8.9 and section 7.3 steps 2 and 3; ADR 0182, ADR 0261, ADR 0280):
//! the vote client that `quoro cluster promote` calls.
//!
//! [`VoteClient::check`] takes the term `T` with `check` requests, and
//! [`VoteClient::vote`] then asks for `T` for real. The command freezes its
//! own agent between the two (section 7.3 "The freeze"); this module does
//! not. Each round asks every voter of `spec.votes.voters` once, all at the
//! same time, the candidate's own agent included: `POST /v1/vote` to the
//! voter's mesh address on `spec.votes.port`, from the candidate's own mesh
//! address. The socket is bound to that address before it connects, so the
//! voter sees the source that section 8.9 checks.
//!
//! **Root.** A new connection leaves through the mesh interface only when one
//! of the machine's own root processes opens it (section 6.9), and a voter
//! answers a caller on its own host only when that caller's socket is root's
//! (section 8.9). So both rounds refuse with [`VoteClientError::NotRoot`]
//! before any socket is opened while the effective uid is not 0
//! ([`VoteClientOptions::effective_uid`]). The client never changes its uid.
//!
//! **The term** (section 7.3 step 2). `T` starts one above the highest term
//! the machine has seen. When a `check` answer carries a term at or above
//! `T`, `T` becomes one above the highest such term and every voter is
//! checked again at the new `T`. The checks settle once no answer carries a
//! term at or above `T` ([`CheckOutcome::Settled`]). When `T` would pass
//! [`MAX_TERM`] they stop ([`CheckOutcome::AboveCeiling`]), and no real
//! request is ever sent above it ([`VoteClientError::BadTerm`]). Every
//! answer that parses and is within the ceiling raises `T`, including one
//! that does not count toward the majority ([`NoAnswer::Uncounted`]).
//!
//! **No answer.** A voter gives no answer ([`NoAnswer`]) when it has no mesh
//! address, when the connection or the HTTP exchange fails or does not finish
//! in time, when its status is not `200` (`503` included, section 8.9 "When
//! the voter cannot tell"), when the body is not a vote answer, and when the
//! answer's raw `term` is above [`MAX_TERM`] (section 8.8,
//! [`term_above_ceiling`]). An answer that does not name the voter asked, or
//! a `granted` answer without `lease_wait_ms`, at another `voters_version`,
//! or, for a real request, at another term than `T`, does not count either.
//!
//! **The majority** (sections 8.8 and 8.9). Granted answers count when they
//! come from more than half of the voter set of the candidate's
//! `voters_version`, which is `spec.votes.voters` as given: every voter
//! counts in that set, the candidate itself, a witness, a voter with no mesh
//! address and a voter that gives no answer included, and each node id counts
//! once. With two voters both must grant, and with one voter that one.
//!
//! **The wait** (section 7.3 step 3). The latest receipt time plus
//! `lease_wait_ms` over the granting answers, on the candidate's monotonic
//! clock ([`Monotonic`]). The receipt time is read once the whole answer has
//! arrived, the latest moment that can be called its receipt, so the wait
//! is never shorter than section 8.9's. This module only computes the
//! deadline; the command waits.
//!
//! The client writes nothing to the node store: only an agent stores a term,
//! when it grants a real vote (section 7.3). It never sends a request again
//! on its own; the command decides whether to ask again.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::header::{CONNECTION, CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use quoro_proto::wire::{
    MAX_TERM, SpecVoter, SpecVotes, VOTE_PATH, VoteAnswer, VoteOutcome, VoteRequest,
    term_above_ceiling,
};
use serde_json::Value;
use tokio::net::TcpSocket;
use tracing::debug;

use crate::util;
use crate::vote::mesh_ip;

/// The longest answer body read, in bytes. A `voters_changed` answer lists
/// the voter set, a few dozen bytes per voter.
pub const ANSWER_LIMIT: usize = 64 << 10;
/// The longest error text kept from an answer that is not `200`.
const ERROR_CHARS: usize = 300;

/// Reads the effective uid of this process.
pub type EffectiveUid = fn() -> u32;

/// The effective uid of this process, from `geteuid(2)`.
#[must_use]
pub fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// The clock receipt times are read on.
pub trait Monotonic: Send + Sync + fmt::Debug {
    /// The current time.
    fn now(&self) -> Instant;
}

/// `CLOCK_MONOTONIC` on Linux, through [`Instant`]. It stops while the host
/// is suspended, which only makes the command's wait longer (section 8.8).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemMonotonic;

impl Monotonic for SystemMonotonic {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Timings and seams; tests replace the seams.
#[derive(Clone, Debug)]
pub struct VoteClientOptions {
    /// How long a voter may take to accept the connection.
    pub connect_timeout: Duration,
    /// How long the request and the whole answer may take once connected.
    pub answer_timeout: Duration,
    /// The most check rounds [`VoteClient::check`] runs before it gives up
    /// ([`CheckOutcome::Unsettled`]); at least one runs.
    pub max_checks: u32,
    /// Reads the effective uid: [`effective_uid`] outside tests.
    pub effective_uid: EffectiveUid,
    /// The clock receipt times are read on: [`SystemMonotonic`] outside
    /// tests.
    pub clock: Arc<dyn Monotonic>,
}

impl Default for VoteClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            answer_timeout: Duration::from_secs(10),
            max_checks: 8,
            effective_uid,
            clock: Arc::new(SystemMonotonic),
        }
    }
}

/// The machine that asks for the votes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    /// The cluster, `cls_`.
    pub cluster_id: String,
    /// The candidate's node id, `nod_`: `candidate_node_id` of every request.
    pub node_id: String,
    /// The candidate's own mesh address, without its prefix length: the
    /// source of every request.
    pub mesh_address: IpAddr,
}

/// Why a round was not run. Each is decided before any socket is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VoteClientError {
    /// The effective uid is not 0 (sections 6.9 and 8.9).
    NotRoot {
        /// The effective uid read.
        euid: u32,
    },
    /// A real vote asked for a term that is 0 or above [`MAX_TERM`].
    BadTerm(u64),
}

impl fmt::Display for VoteClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRoot { euid } => write!(
                f,
                "vote requests are sent only as root (effective uid {euid}): the mesh carries \
                 new connections only from root's processes"
            ),
            Self::BadTerm(term) => write!(f, "term {term} is not between 1 and {MAX_TERM}"),
        }
    }
}

impl std::error::Error for VoteClientError {}

/// One voter's reply in a round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoterReply {
    /// The voter asked, from `spec.votes.voters`.
    pub node_id: String,
    /// What it answered.
    pub reply: Reply,
}

/// What a voter answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// A vote answer that counts, `granted` or `refused`.
    Answer {
        /// The answer.
        answer: VoteAnswer,
        /// When the whole answer had arrived, on the candidate's clock.
        received: Instant,
    },
    /// No answer.
    NoAnswer(NoAnswer),
}

impl Reply {
    /// The term of an answer that parsed and is within the ceiling, whether
    /// or not it counts: the terms that raise `T` (section 7.3).
    #[must_use]
    pub fn term(&self) -> Option<u64> {
        match self {
            Self::Answer { answer, .. } => Some(answer.term),
            Self::NoAnswer(NoAnswer::Uncounted { answer, .. }) => Some(answer.term),
            Self::NoAnswer(_) => None,
        }
    }
}

/// Why a voter gave no answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoAnswer {
    /// The voter has no mesh address, or one that is not an address: no
    /// request was sent.
    NoMeshAddress,
    /// The socket could not be opened or bound, or the voter could not be
    /// reached.
    Connect(String),
    /// The connection or the answer did not arrive in time.
    Timeout(String),
    /// The HTTP exchange failed, or the answer was too large.
    Http(String),
    /// A status other than `200`, with the text of its `{"error": ...}`
    /// body. Section 8.9's `503`, `400`, `404`, `405`, `408` and `413` are
    /// all here.
    Status {
        /// The HTTP status.
        status: u16,
        /// The `error` text, when the body has one.
        error: Option<String>,
    },
    /// A `200` whose body is not a vote answer.
    Malformed(String),
    /// A `200` whose `term` is above [`MAX_TERM`] (section 8.8).
    AboveCeiling,
    /// A vote answer that does not count, and why. Its term still raises
    /// `T` ([`Reply::term`]).
    Uncounted {
        /// The answer.
        answer: Box<VoteAnswer>,
        /// Why it does not count.
        why: String,
    },
}

impl fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMeshAddress => f.write_str("no mesh address"),
            Self::Connect(e) | Self::Timeout(e) | Self::Http(e) | Self::Malformed(e) => {
                f.write_str(e)
            }
            Self::Status {
                status,
                error: Some(error),
            } => write!(f, "{status}: {error}"),
            Self::Status {
                status,
                error: None,
            } => write!(f, "{status}"),
            Self::AboveCeiling => write!(f, "an answer whose term is above {MAX_TERM}"),
            Self::Uncounted { why, .. } => write!(f, "an answer that does not count: {why}"),
        }
    }
}

/// One round: every voter asked once at `term`, and the tally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Round {
    /// The term asked for, `T`.
    pub term: u64,
    /// Whether the requests were `check` requests.
    pub check: bool,
    /// The voter-set version every request carried.
    pub voters_version: u64,
    /// One reply per entry of `spec.votes.voters`, in its order.
    pub replies: Vec<VoterReply>,
    /// The size of the voter set: its distinct node ids.
    pub voters: usize,
    /// The distinct voters whose answer counts as `granted`.
    pub granted: usize,
    /// Whether `granted` is more than half of `voters`.
    pub majority: bool,
    /// The highest term an answer carried ([`Reply::term`]).
    pub highest_term: Option<u64>,
    /// The latest receipt time plus `lease_wait_ms` over the granting
    /// answers, or `None` with none. After a real round, the command waits
    /// until then unless the operator acknowledged the fence (section 7.3
    /// step 3). After a check round, it is what the wait would be if the
    /// voters granted now.
    pub wait_until: Option<Instant>,
}

/// How [`VoteClient::check`] ended. No outcome sent a real request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    /// No answer of the last check carried a term at or above its `T`:
    /// `round.term` is the term to ask for for real, and `round.majority`
    /// says whether a majority would grant it.
    Settled(Round),
    /// `T` would pass [`MAX_TERM`]: one above `highest`, the highest term the
    /// machine has seen or an answer carried. `last` is the last check
    /// round, if one ran. The promotion stops and changes nothing (section
    /// 7.3 step 2).
    AboveCeiling {
        /// The highest term known.
        highest: u64,
        /// The last check round, if one ran.
        last: Option<Round>,
    },
    /// The terms kept rising for [`VoteClientOptions::max_checks`] rounds:
    /// `term` is the `T` the next check would have asked for.
    Unsettled {
        /// The next `T`.
        term: u64,
        /// The last check round.
        last: Round,
    },
}

/// The vote client of one candidate over one voter set.
#[derive(Clone, Debug)]
pub struct VoteClient {
    votes: SpecVotes,
    candidate: Candidate,
    options: VoteClientOptions,
}

impl VoteClient {
    /// A client for `candidate` over `votes`, the `spec.votes` of the
    /// agent's newest stored desired state: its `version`, `port` and
    /// `voters`. `votes.listen` is not read: a voter that does not listen
    /// gives no answer.
    #[must_use]
    pub fn new(votes: SpecVotes, candidate: Candidate, options: VoteClientOptions) -> Self {
        Self {
            votes,
            candidate,
            options,
        }
    }

    /// Takes the term with `check` requests, starting one above `seen`, the
    /// highest term this machine has seen (`crate::vote::current_term`), and
    /// sends no real request (section 7.3 step 2).
    ///
    /// # Errors
    ///
    /// [`VoteClientError::NotRoot`] when the effective uid is not 0, before
    /// any socket is opened.
    pub async fn check(&self, seen: u64) -> Result<CheckOutcome, VoteClientError> {
        self.require_root()?;
        Ok(settle(seen, self.options.max_checks, |term| self.round(term, true)).await)
    }

    /// Asks every voter once for `term` for real and tallies the answers.
    /// Nothing is sent again: the command decides whether to ask again.
    ///
    /// # Errors
    ///
    /// [`VoteClientError::NotRoot`] when the effective uid is not 0, and
    /// [`VoteClientError::BadTerm`] for a `term` of 0 or above
    /// [`MAX_TERM`], both before any socket is opened.
    pub async fn vote(&self, term: u64) -> Result<Round, VoteClientError> {
        self.require_root()?;
        if term == 0 || term > MAX_TERM {
            return Err(VoteClientError::BadTerm(term));
        }
        Ok(self.round(term, false).await)
    }

    fn require_root(&self) -> Result<(), VoteClientError> {
        match (self.options.effective_uid)() {
            0 => Ok(()),
            euid => Err(VoteClientError::NotRoot { euid }),
        }
    }

    /// Asks every voter once at `term`, all at the same time.
    async fn round(&self, term: u64, check: bool) -> Round {
        let request = VoteRequest {
            cluster_id: self.candidate.cluster_id.clone(),
            term,
            candidate_node_id: self.candidate.node_id.clone(),
            voters_version: self.votes.version,
            check: check.then_some(true),
        };
        let body = Bytes::from(serde_json::to_vec(&request).unwrap_or_default());
        let replies = futures_util::future::join_all(
            self.votes
                .voters
                .iter()
                .map(|voter| self.ask(voter, body.clone(), term, check)),
        )
        .await;
        tally(term, check, &self.votes, replies)
    }

    /// One request to `voter`, and what it answered.
    async fn ask(&self, voter: &SpecVoter, body: Bytes, term: u64, check: bool) -> VoterReply {
        let reply = match voter.mesh_address.as_deref().and_then(mesh_ip) {
            None => Reply::NoAnswer(NoAnswer::NoMeshAddress),
            Some(ip) => match self
                .exchange(SocketAddr::new(ip, self.votes.port), body)
                .await
            {
                Ok((status, bytes, received)) => classify(
                    &voter.node_id,
                    self.votes.version,
                    term,
                    check,
                    status,
                    &bytes,
                    received,
                ),
                Err(no_answer) => Reply::NoAnswer(no_answer),
            },
        };
        match &reply {
            Reply::Answer { answer, .. } => debug!(
                voter = %voter.node_id,
                term,
                check,
                vote = ?answer.vote,
                reason = ?answer.reason,
                "a vote answer"
            ),
            Reply::NoAnswer(no_answer) => {
                debug!(voter = %voter.node_id, term, check, %no_answer, "no vote answer");
            }
        }
        VoterReply {
            node_id: voter.node_id.clone(),
            reply,
        }
    }

    /// Sends `body` to `target` from the candidate's mesh address and reads
    /// the status, the body and the time the body had arrived.
    async fn exchange(
        &self,
        target: SocketAddr,
        body: Bytes,
    ) -> Result<(StatusCode, Bytes, Instant), NoAnswer> {
        let source = SocketAddr::new(self.candidate.mesh_address, 0);
        let socket = if target.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|e| NoAnswer::Connect(format!("cannot open a socket: {e}")))?;
        socket.bind(source).map_err(|e| {
            NoAnswer::Connect(format!(
                "cannot use the mesh address {}: {e}",
                self.candidate.mesh_address
            ))
        })?;
        let stream = tokio::time::timeout(self.options.connect_timeout, socket.connect(target))
            .await
            .map_err(|_| NoAnswer::Timeout(format!("{target} did not accept in time")))?
            .map_err(|e| NoAnswer::Connect(format!("cannot reach {target}: {e}")))?;
        let exchange = async {
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .map_err(|e| NoAnswer::Http(format!("HTTP handshake failed: {e}")))?;
            let request = Request::builder()
                .method(Method::POST)
                .uri(VOTE_PATH)
                .header(HOST, target.to_string())
                .header(CONTENT_TYPE, "application/json")
                .header(CONNECTION, "close")
                .body(Full::new(body))
                .map_err(|e| NoAnswer::Http(format!("cannot build the request: {e}")))?;
            let answer = async {
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(|e| NoAnswer::Http(format!("the request failed: {e}")))?;
                let status = response.status();
                let bytes = Limited::new(response.into_body(), ANSWER_LIMIT)
                    .collect()
                    .await
                    .map_err(|e| NoAnswer::Http(format!("cannot read the answer: {e}")))?
                    .to_bytes();
                Ok::<_, NoAnswer>((status, bytes, self.options.clock.now()))
            };
            // The connection is driven here, not on a task of its own, so
            // nothing outlives the round.
            let mut answer = std::pin::pin!(answer);
            let mut connection = std::pin::pin!(connection);
            tokio::select! {
                answer = &mut answer => answer,
                ended = &mut connection => match ended {
                    // The connection ended once the answer was in; the
                    // answer future finishes with it.
                    Ok(()) => answer.await,
                    Err(e) => Err(NoAnswer::Http(format!("the connection failed: {e}"))),
                },
            }
        };
        tokio::time::timeout(self.options.answer_timeout, exchange)
            .await
            .map_err(|_| NoAnswer::Timeout(format!("{target} did not answer in time")))?
    }
}

/// One above `highest`, while that is within [`MAX_TERM`].
fn term_above(highest: u64) -> Option<u64> {
    highest.checked_add(1).filter(|term| *term <= MAX_TERM)
}

/// The check rounds of section 7.3 step 2, from `seen`, with `ask` running
/// one check round at a given `T`.
async fn settle<F, Fut>(seen: u64, max_checks: u32, mut ask: F) -> CheckOutcome
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Round>,
{
    let Some(mut term) = term_above(seen) else {
        return CheckOutcome::AboveCeiling {
            highest: seen,
            last: None,
        };
    };
    let mut round = ask(term).await;
    let mut checks = 1;
    loop {
        let Some(top) = round.highest_term.filter(|top| *top >= term) else {
            return CheckOutcome::Settled(round);
        };
        let Some(next) = term_above(top) else {
            return CheckOutcome::AboveCeiling {
                highest: top,
                last: Some(round),
            };
        };
        if checks >= max_checks {
            return CheckOutcome::Unsettled {
                term: next,
                last: round,
            };
        }
        term = next;
        round = ask(term).await;
        checks += 1;
    }
}

/// The reply of the voter `voter` to a request at `term` (`check` or real)
/// under voter-set version `version`: `status` and `body` as they arrived,
/// complete at `received`.
fn classify(
    voter: &str,
    version: u64,
    term: u64,
    check: bool,
    status: StatusCode,
    body: &[u8],
    received: Instant,
) -> Reply {
    let value = serde_json::from_slice::<Value>(body);
    if status != StatusCode::OK {
        let error = value
            .ok()
            .as_ref()
            .and_then(|value| value.get("error"))
            .and_then(Value::as_str)
            .map(|text| util::one_line(text, ERROR_CHARS));
        return Reply::NoAnswer(NoAnswer::Status {
            status: status.as_u16(),
            error,
        });
    }
    let value = match value {
        Ok(value) => value,
        Err(e) => {
            return Reply::NoAnswer(NoAnswer::Malformed(format!("the body is not JSON: {e}")));
        }
    };
    // Before the typed parse: a term above `u64::MAX` or written as a float
    // is above the ceiling too, not merely malformed.
    if term_above_ceiling(&value) {
        return Reply::NoAnswer(NoAnswer::AboveCeiling);
    }
    let answer = match serde_json::from_value::<VoteAnswer>(value) {
        Ok(answer) => answer,
        Err(e) => {
            return Reply::NoAnswer(NoAnswer::Malformed(format!(
                "the body is not a vote answer: {e}"
            )));
        }
    };
    match uncounted(&answer, voter, version, term, check, received) {
        Some(why) => Reply::NoAnswer(NoAnswer::Uncounted {
            answer: Box::new(answer),
            why,
        }),
        None => Reply::Answer { answer, received },
    }
}

/// Why `answer` does not count, if it does not: it names another voter than
/// `voter`, or it is `granted` without `lease_wait_ms`, at another
/// voter-set version than `version`, for a real request at another term than
/// `term`, or with a wait that cannot be added to `received`.
fn uncounted(
    answer: &VoteAnswer,
    voter: &str,
    version: u64,
    term: u64,
    check: bool,
    received: Instant,
) -> Option<String> {
    if answer.node_id != voter {
        return Some(format!("it names {}, not the voter asked", answer.node_id));
    }
    if answer.vote != VoteOutcome::Granted {
        return None;
    }
    let Some(wait) = answer.lease_wait_ms else {
        return Some("granted without lease_wait_ms".to_owned());
    };
    if answer.voters_version != version {
        return Some(format!(
            "granted at voter-set version {}, not {version}",
            answer.voters_version
        ));
    }
    if !check && answer.term != term {
        return Some(format!(
            "a real vote granted at term {}, not {term}",
            answer.term
        ));
    }
    if received.checked_add(Duration::from_millis(wait)).is_none() {
        return Some(format!("lease_wait_ms {wait} is too long"));
    }
    None
}

/// The tally of one round at `term` over `votes`: the majority and the wait
/// of the module documentation.
fn tally(term: u64, check: bool, votes: &SpecVotes, replies: Vec<VoterReply>) -> Round {
    let set = votes
        .voters
        .iter()
        .map(|voter| voter.node_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut granting = BTreeSet::new();
    let mut wait_until: Option<Instant> = None;
    for reply in &replies {
        let Reply::Answer { answer, received } = &reply.reply else {
            continue;
        };
        if !set.contains(reply.node_id.as_str()) {
            continue;
        }
        if answer.vote != VoteOutcome::Granted {
            continue;
        }
        // A grant counts only with its wait: [`classify`] already left out
        // one without it.
        let Some(until) = answer
            .lease_wait_ms
            .and_then(|wait| received.checked_add(Duration::from_millis(wait)))
        else {
            continue;
        };
        granting.insert(reply.node_id.as_str());
        wait_until = Some(wait_until.map_or(until, |latest| latest.max(until)));
    }
    let (voters, granted) = (set.len(), granting.len());
    let highest_term = replies.iter().filter_map(|reply| reply.reply.term()).max();
    Round {
        term,
        check,
        voters_version: votes.version,
        voters,
        granted,
        majority: granted * 2 > voters,
        highest_term,
        wait_until,
        replies,
    }
}

#[cfg(test)]
mod tests;
