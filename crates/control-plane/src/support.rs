// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's side of voter support (`node-agent-protocol.md` sections
//! 5.27, 5.28 and 8.8; ADR 0169, ADR 0263).
//!
//! The primary asks every connected voter for support over its session
//! (`support_request`), and each voter promises it for `S` from the moment it
//! received the request (`support_grant`). The primary counts that support
//! from `p_send`, the moment it sent the request, never from the answer's
//! arrival, until `p_send + S - M_S`. [`SupportTracker`] keeps:
//!
//! - **per outstanding request**, by message id: the voter, the session
//!   generation, `p_send`, `S`, `L`, the term and the `voters_version` the
//!   request carried;
//! - **per voter**, its newest counted support: the latest `p_send` with its
//!   `S`, `L` and `voters_version`. It outlives the session it came over,
//!   until its window runs out: the promise is the voter's.
//!
//! An answer counts only when it pairs by `re` with an outstanding request of
//! the same node and session, echoes the request's `term`, `support_ms` and
//! `max_lease_ms`, and carries the primary's current voter-set version
//! (section 8.10 rule 3). The primary holds support for a grant of duration
//! `d` while more than half of the current voter set, connected or not, holds
//! a support at the current version whose `L` is at least `d` and whose
//! window is still open on `CLOCK_BOOTTIME` ([`SupportTracker::supports`]).
//! The primary's own host counts only through its own agent's session, like
//! any other voter: nothing here counts it otherwise.
//!
//! Everything is in memory: after a restart the primary holds no support and
//! asks again before it grants or renews (section 8.8, "Restarts").

use crate::authority::VoterSupport;
use crate::voters::VoterLog;
use quoro_proto::wire::{SupportGrant, SupportRequest};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// `S`, the support window the primary asks for (section 8.8).
pub const SUPPORT_MS: u64 = 15_000;

/// How often the primary asks each connected voter again: `S / 3` (section
/// 5.27, section 4.3).
pub const REQUEST_INTERVAL: Duration = Duration::from_millis(SUPPORT_MS / 3);

/// `M_S = max(1000 ms, S / 10)` (section 8.8).
#[must_use]
pub fn support_margin_ms(support_ms: u64) -> u64 {
    (support_ms / 10).max(1_000)
}

/// The clock the primary measures support on: `CLOCK_BOOTTIME` (ADR 0263).
pub trait BootClock: Send + Sync + std::fmt::Debug {
    /// `CLOCK_BOOTTIME` now, or `None` on a host without it.
    fn now(&self) -> Option<Duration>;
}

/// The host's `CLOCK_BOOTTIME` on Linux, nothing elsewhere.
///
/// Section 8.8 measures `p_send` on `CLOCK_BOOTTIME`, which keeps counting
/// while the host is suspended, as the voters measure their promises and the
/// nodes their deadlines. A host without it (a macOS dev build) gets `None`
/// and so counts no support: `CLOCK_MONOTONIC` stops during a suspend, so a
/// window measured on it would outlast the voters' promises, which is the
/// unsafe side (ADR 0263).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemBootClock;

impl BootClock for SystemBootClock {
    #[cfg(target_os = "linux")]
    fn now(&self) -> Option<Duration> {
        let now = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
        let secs = u64::try_from(now.tv_sec).ok()?;
        let nanos = u32::try_from(now.tv_nsec).ok()?;
        Some(Duration::new(secs, nanos))
    }

    #[cfg(not(target_os = "linux"))]
    fn now(&self) -> Option<Duration> {
        None
    }
}

/// Test seam: a `CLOCK_BOOTTIME` that moves only when told.
#[derive(Debug, Default)]
pub struct ManualBootClock(Mutex<Duration>);

impl ManualBootClock {
    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += by;
    }
}

impl BootClock for ManualBootClock {
    fn now(&self) -> Option<Duration> {
        Some(
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// A `support_request` that has not been answered.
#[derive(Clone, Debug)]
struct Outstanding {
    node_id: String,
    generation: u64,
    p_send: Duration,
    support_ms: u64,
    max_lease_ms: u64,
    term: u64,
    /// The version the request carried. The answer is counted against the
    /// primary's version when it arrives, not this one.
    voters_version: u64,
}

/// A voter's newest counted support.
#[derive(Clone, Copy, Debug)]
struct Held {
    p_send: Duration,
    support_ms: u64,
    max_lease_ms: u64,
    voters_version: u64,
}

impl Held {
    /// Whether it supports a grant of `duration_ms` at `now`, at `version`:
    /// `now < p_send + S - M_S` and `L >= d` (section 8.8).
    fn supports(&self, version: u64, duration_ms: u64, now: Duration) -> bool {
        let window = self
            .support_ms
            .saturating_sub(support_margin_ms(self.support_ms));
        self.voters_version == version
            && self.max_lease_ms >= duration_ms
            && now < self.p_send.saturating_add(Duration::from_millis(window))
    }
}

/// What became of a `support_grant`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// It counts.
    Counted,
    /// No outstanding request of this node and session has its `re`
    /// (section 5.28): ignored.
    Unpaired,
    /// Its `term`, `support_ms` or `max_lease_ms` differ from the request's.
    WrongEcho,
    /// Its `voters_version` is not the primary's current one (section 8.10
    /// rule 3): `current` is that one, `None` when the primary has read no
    /// voter set yet, and `asked` the one the request carried.
    OtherVersion { current: Option<u64>, asked: u64 },
    /// The node is not in the current voter set.
    NotVoter,
}

#[derive(Debug, Default)]
struct State {
    outstanding: HashMap<String, Outstanding>,
    held: HashMap<String, Held>,
}

/// The voters' support of this primary ([module docs](self)).
///
/// Section 8.10 rule 2 is not built here, on purpose: the primary does not
/// yet hold a voter-set change until a majority of the current voters has
/// acknowledged the current version. That gate comes with the voter-set
/// change operations in #687, before any vote listener (section 8.9). The
/// support counted here is safe against a candidate's majority only once
/// rule 2 gates voter-set changes; until then it is safe because no build
/// collects votes yet, so no candidate can form a majority.
#[derive(Debug)]
pub struct SupportTracker {
    clock: RwLock<Arc<dyn BootClock>>,
    voters: Arc<VoterLog>,
    /// Set by a `stale_term`: no support is held until the process restarts.
    stopped: AtomicBool,
    state: Mutex<State>,
}

impl SupportTracker {
    /// A tracker that holds no support, measuring on `clock` and counting
    /// against the newest set `voters` read.
    #[must_use]
    pub fn new(clock: Arc<dyn BootClock>, voters: Arc<VoterLog>) -> Self {
        Self {
            clock: RwLock::new(clock),
            voters,
            stopped: AtomicBool::new(false),
            state: Mutex::new(State::default()),
        }
    }

    fn now(&self) -> Option<Duration> {
        self.clock
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .now()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Test seam: measures on `clock` from now on, forgetting every request
    /// and support measured on the previous one.
    pub fn set_clock(&self, clock: Arc<dyn BootClock>) {
        *self
            .clock
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = clock;
        let mut state = self.state();
        state.outstanding.clear();
        state.held.clear();
    }

    /// Records `request`, about to go out as message `msg_id` on `node_id`'s
    /// session `generation`, with `p_send` read now: call it just before the
    /// frame is handed to the session, so `p_send` is never later than the
    /// send. On a host without `CLOCK_BOOTTIME` nothing is recorded, so no
    /// answer to it can count. Requests whose window has closed are dropped:
    /// an answer to them could not count either.
    pub(crate) fn sent(
        &self,
        msg_id: &str,
        node_id: &str,
        generation: u64,
        request: &SupportRequest,
    ) {
        let Some(p_send) = self.now() else {
            tracing::debug!(
                node_id,
                "no CLOCK_BOOTTIME on this host; the support_request goes out but its answer cannot count"
            );
            return;
        };
        let mut state = self.state();
        state.outstanding.retain(|_, sent| {
            p_send
                < sent
                    .p_send
                    .saturating_add(Duration::from_millis(sent.support_ms))
        });
        state.outstanding.insert(
            msg_id.to_string(),
            Outstanding {
                node_id: node_id.to_string(),
                generation,
                p_send,
                support_ms: request.support_ms,
                max_lease_ms: request.max_lease_ms,
                term: request.term,
                voters_version: request.voters_version,
            },
        );
    }

    /// Takes a `support_grant` that `node_id` sent on session `generation`
    /// with `re` (section 5.28). A counted answer keeps the voter's later
    /// `p_send` at the current version, and replaces a support held at an
    /// older one. The outstanding request it pairs with is used up whether it
    /// counts or not.
    pub(crate) fn granted(
        &self,
        node_id: &str,
        generation: u64,
        re: Option<&str>,
        grant: &SupportGrant,
    ) -> Answer {
        let Some(re) = re else {
            return Answer::Unpaired;
        };
        let mut state = self.state();
        let paired = state
            .outstanding
            .get(re)
            .is_some_and(|sent| sent.node_id == node_id && sent.generation == generation);
        if !paired {
            return Answer::Unpaired;
        }
        let Some(sent) = state.outstanding.remove(re) else {
            return Answer::Unpaired;
        };
        if grant.term != sent.term
            || grant.support_ms != sent.support_ms
            || grant.max_lease_ms != sent.max_lease_ms
        {
            return Answer::WrongEcho;
        }
        let Some(set) = self.voters.latest() else {
            return Answer::OtherVersion {
                current: None,
                asked: sent.voters_version,
            };
        };
        if grant.voters_version != set.version {
            return Answer::OtherVersion {
                current: Some(set.version),
                asked: sent.voters_version,
            };
        }
        if !set.contains(node_id) {
            return Answer::NotVoter;
        }
        let support = Held {
            p_send: sent.p_send,
            support_ms: sent.support_ms,
            max_lease_ms: sent.max_lease_ms,
            voters_version: grant.voters_version,
        };
        // The later `p_send` wins at one version. A support held at another
        // version can never count again, since the version only grows
        // (section 8.10), so an answer at the current one replaces it.
        state
            .held
            .entry(node_id.to_string())
            .and_modify(|held| {
                if held.voters_version != support.voters_version || held.p_send < support.p_send {
                    *held = support;
                }
            })
            .or_insert(support);
        Answer::Counted
    }

    /// Takes an `error` answer to the request `re` of `node_id`'s session
    /// `generation`, such as `bad_message` (section 5.27): the request is used
    /// up and nothing counts. Returns whether `re` was such a request.
    pub(crate) fn refused(&self, node_id: &str, generation: u64, re: &str) -> bool {
        let mut state = self.state();
        let paired = state
            .outstanding
            .get(re)
            .is_some_and(|sent| sent.node_id == node_id && sent.generation == generation);
        if paired {
            state.outstanding.remove(re);
        }
        paired
    }

    /// `node_id`'s session `generation` ended: its outstanding requests can
    /// no longer be answered. Its counted support stays until its window
    /// runs out.
    pub(crate) fn session_ended(&self, node_id: &str, generation: u64) {
        self.state()
            .outstanding
            .retain(|_, sent| !(sent.node_id == node_id && sent.generation == generation));
    }

    /// A node answered `stale_term`: a higher term exists, so this primary
    /// stops granting and renewing `expiring` leases at once (section 8.8,
    /// "Terms"), and holds no support again.
    ///
    /// This runs first; the demotion follows it ([`crate::store::Store::demote`]),
    /// which ends the sessions with `not_primary`, answers every later
    /// request with `503` `not_primary` and keeps a restart demoted
    /// (section 7.4, ADR 0182, ADR 0230).
    pub(crate) fn stale_term(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let mut state = self.state();
        state.outstanding.clear();
        state.held.clear();
    }

    /// How many voters of the current set support a grant of `duration_ms`
    /// now, and how many voters the set has; `None` when no support can be
    /// held at all (after `stale_term`, without `CLOCK_BOOTTIME`, before the
    /// first voter set is read, or while the set is not countable because
    /// the `nodes` rows and the change log disagree at its version, sections
    /// 6.12 and 8.10 rule 4).
    fn count(&self, duration_ms: u64) -> Option<(usize, usize)> {
        if self.stopped.load(Ordering::SeqCst) {
            return None;
        }
        let now = self.now()?;
        let set = self.voters.latest()?;
        if !set.countable {
            return None;
        }
        let state = self.state();
        let supporting = set
            .voters
            .iter()
            .filter(|voter| {
                state
                    .held
                    .get(&voter.node_id)
                    .is_some_and(|held| held.supports(set.version, duration_ms, now))
            })
            .count();
        Some((supporting, set.voters.len()))
    }
}

impl VoterSupport for SupportTracker {
    /// Section 8.8, "Majority" and "What support allows": more than half of
    /// the whole voter set, connected or not, supports this primary now at
    /// the current version with an `L` of at least `duration_ms`. With no
    /// voter there is no majority.
    fn supports(&self, duration_ms: u64) -> bool {
        self.count(duration_ms)
            .is_some_and(|(supporting, voters)| supporting * 2 > voters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voters::{Voter, VoterSet};

    const D: u64 = 15_000;

    fn tracker(
        ids: &[&str],
        version: u64,
    ) -> (SupportTracker, Arc<ManualBootClock>, Arc<VoterLog>) {
        let voters = Arc::new(VoterLog::default());
        voters.set_latest(VoterSet {
            head: version,
            version,
            voters: ids
                .iter()
                .map(|id| Voter {
                    node_id: (*id).to_string(),
                    roles: vec!["node".to_string()],
                    mesh_address: None,
                })
                .collect(),
            disagreement: None,
            countable: true,
            open: false,
            closed_by: Vec::new(),
        });
        let clock = Arc::new(ManualBootClock::default());
        let tracker = SupportTracker::new(
            Arc::clone(&clock) as Arc<dyn BootClock>,
            Arc::clone(&voters),
        );
        (tracker, clock, voters)
    }

    fn request(version: u64) -> SupportRequest {
        SupportRequest {
            term: 1,
            primary_node_id: "nod_p".to_string(),
            support_ms: SUPPORT_MS,
            max_lease_ms: D,
            voters_version: version,
        }
    }

    fn grant(version: u64) -> SupportGrant {
        SupportGrant {
            term: 1,
            support_ms: SUPPORT_MS,
            max_lease_ms: D,
            voters_version: version,
        }
    }

    /// Asks `node` on session 1 and has it answer at `version`.
    fn answer(tracker: &SupportTracker, id: &str, node: &str, version: u64) -> Answer {
        tracker.sent(id, node, 1, &request(version));
        tracker.granted(node, 1, Some(id), &grant(version))
    }

    #[test]
    fn the_margin_is_a_tenth_of_s_and_at_least_a_second() {
        assert_eq!(support_margin_ms(15_000), 1_500);
        assert_eq!(support_margin_ms(3_000), 1_000);
    }

    /// Section 8.8, "Majority": more than half of the whole set.
    #[test]
    fn a_majority_is_more_than_half_of_the_whole_set() {
        let (three, _, _) = tracker(&["nod_a", "nod_b", "nod_c"], 7);
        assert!(!three.supports(D));
        assert_eq!(answer(&three, "msg_1", "nod_a", 7), Answer::Counted);
        assert!(!three.supports(D), "1 of 3");
        assert_eq!(answer(&three, "msg_2", "nod_b", 7), Answer::Counted);
        assert!(three.supports(D), "2 of 3");
        assert!(!three.supports(D + 1), "L < d");

        let (two, _, _) = tracker(&["nod_a", "nod_b"], 7);
        answer(&two, "msg_1", "nod_a", 7);
        assert!(!two.supports(D), "1 of 2");
        answer(&two, "msg_2", "nod_b", 7);
        assert!(two.supports(D), "2 of 2");

        let (four, _, _) = tracker(&["nod_a", "nod_b", "nod_c", "nod_d"], 7);
        answer(&four, "msg_1", "nod_a", 7);
        answer(&four, "msg_2", "nod_b", 7);
        assert!(!four.supports(D), "2 of 4 is half, not more");
        answer(&four, "msg_3", "nod_c", 7);
        assert!(four.supports(D), "3 of 4");

        let (none, _, _) = tracker(&[], 7);
        assert!(!none.supports(D), "no voter, no majority");
    }

    /// Section 8.8: the window is `p_send + S - M_S`, from the request's send
    /// time, whenever the answer arrives.
    #[test]
    fn support_ends_at_p_send_plus_s_minus_m_s() {
        let (tracker, clock, _) = tracker(&["nod_a"], 7);
        tracker.sent("msg_1", "nod_a", 1, &request(7));
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_1"), &grant(7)),
            Answer::Counted
        );
        clock.advance(Duration::from_millis(3_499));
        assert!(tracker.supports(D));
        clock.advance(Duration::from_millis(1));
        assert!(!tracker.supports(D), "13.5 s after p_send");
    }

    /// Sections 5.28 and 8.10: an unpaired, mis-echoed or other-version
    /// answer does not count, and uses up its request.
    #[test]
    fn only_a_paired_echoing_current_answer_counts() {
        let (tracker, _, voters) = tracker(&["nod_a"], 7);
        tracker.sent("msg_1", "nod_a", 1, &request(7));
        assert_eq!(
            tracker.granted("nod_a", 1, None, &grant(7)),
            Answer::Unpaired
        );
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_x"), &grant(7)),
            Answer::Unpaired
        );
        assert_eq!(
            tracker.granted("nod_b", 1, Some("msg_1"), &grant(7)),
            Answer::Unpaired,
            "another node"
        );
        assert_eq!(
            tracker.granted("nod_a", 2, Some("msg_1"), &grant(7)),
            Answer::Unpaired,
            "another session"
        );
        let mut echo = grant(7);
        echo.max_lease_ms = D + 1;
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_1"), &echo),
            Answer::WrongEcho
        );
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_1"), &grant(7)),
            Answer::Unpaired,
            "used up"
        );
        assert_eq!(
            answer(&tracker, "msg_2", "nod_a", 6),
            Answer::OtherVersion {
                current: Some(7),
                asked: 6
            }
        );
        assert!(!tracker.supports(D));
        assert_eq!(answer(&tracker, "msg_3", "nod_a", 7), Answer::Counted);
        assert!(tracker.supports(D));
        // A support held at an older version stops counting once the set
        // moves on.
        voters.set_latest(VoterSet {
            head: 9,
            version: 9,
            voters: voters.latest().unwrap().voters.clone(),
            disagreement: None,
            countable: true,
            open: false,
            closed_by: Vec::new(),
        });
        assert!(!tracker.supports(D));
        // An answer at the new version replaces it, even one whose `p_send`
        // is no later.
        assert_eq!(answer(&tracker, "msg_4", "nod_a", 9), Answer::Counted);
        assert!(tracker.supports(D));
    }

    /// Section 8.8, "Terms": after `stale_term` nothing counts.
    #[test]
    fn a_stale_term_stops_all_support() {
        let (tracker, _, _) = tracker(&["nod_a"], 7);
        answer(&tracker, "msg_1", "nod_a", 7);
        assert!(tracker.supports(D));
        tracker.stale_term();
        assert!(!tracker.supports(D));
        answer(&tracker, "msg_2", "nod_a", 7);
        assert!(!tracker.supports(D));
    }

    /// A voter's support outlives its session, and the later `p_send` wins.
    #[test]
    fn support_outlives_the_session_and_keeps_the_later_p_send() {
        let (tracker, clock, _) = tracker(&["nod_a"], 7);
        tracker.sent("msg_1", "nod_a", 1, &request(7));
        clock.advance(Duration::from_secs(5));
        tracker.sent("msg_2", "nod_a", 1, &request(7));
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_2"), &grant(7)),
            Answer::Counted
        );
        assert_eq!(
            tracker.granted("nod_a", 1, Some("msg_1"), &grant(7)),
            Answer::Counted
        );
        tracker.session_ended("nod_a", 1);
        // Measured from the second request's p_send: 5 s + 13.5 s.
        clock.advance(Duration::from_millis(13_499));
        assert!(tracker.supports(D));
        clock.advance(Duration::from_millis(1));
        assert!(!tracker.supports(D));
    }
}
