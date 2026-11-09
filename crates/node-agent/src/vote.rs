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
//! Every value this module stores errs late: a later promise only keeps the
//! voter from supporting another candidate a little longer, and a later
//! horizon only makes a candidate wait a little longer (section 8.9), while an
//! earlier one could let two primaries grant the same lease.

use std::time::Duration;

use quoro_proto::wire::{MIN_SUPPORT_MS, SupportGrant, SupportRequest};
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
/// start at 1.
///
/// # Errors
///
/// [`AgentError::Store`] when the stored term cannot be read or the higher one
/// cannot be stored. The caller then acts on nothing the frame carries.
pub async fn check_term(store: &NodeStore, term: u64) -> Result<TermCheck, AgentError> {
    let highest = store.highest_term().await?.unwrap_or(0);
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

/// Answers one `support_request` (sections 5.27 and 8.8).
///
/// `voters_version` is `spec.votes.version` of the machine's newest desired
/// state, `None` when it carries no `spec.votes` and the machine is therefore
/// not a voter (section 6.12). `received` is the clock read when the request
/// arrived.
///
/// The order:
///
/// 1. The term, on every machine. Section 8.8 "Terms" has every machine,
///    voter or not, refuse a lower term in a `support_request` and keep a
///    higher one, so the term check runs before the voter check; a
///    non-voter then ignores a request of its own term or a higher one.
/// 2. A machine that is not a voter ignores the request.
/// 3. `support_ms` below 3 000 is `bad_message`, without a term.
/// 4. A vote in this term for a candidate other than `primary_node_id` is
///    `bad_message` carrying the voter's term (section 5.27, ADR 0280). After
///    step 1 the request's term is the voter's highest.
/// 5. The promise and the horizon are stored, and only then is the grant
///    returned. A host with no `CLOCK_BOOTTIME` or no boot id cannot keep a
///    promise across a restart (section 8.8 "Restarts"): a stored instant
///    would mean nothing to the next agent, and the voter could vote for a
///    candidate while the primary still counts its support. So it grants
///    nothing, as when the store cannot be written: the primary then lacks
///    this voter's support, which is the safe side of ADR 0169.
pub async fn answer_support(
    store: &NodeStore,
    voters_version: Option<u64>,
    request: &SupportRequest,
    received: &Received,
) -> SupportAnswer {
    match check_term(store, request.term).await {
        Ok(TermCheck::Stale { highest }) => return SupportAnswer::Stale { highest },
        Ok(TermCheck::Current | TermCheck::Raised) => {}
        Err(e) => return SupportAnswer::Withhold(format!("could not keep the term: {e}")),
    }
    let Some(voters_version) = voters_version else {
        return SupportAnswer::Ignore;
    };
    if request.support_ms < MIN_SUPPORT_MS {
        return SupportAnswer::Refuse {
            message: format!(
                "support_ms {} is below the {MIN_SUPPORT_MS} ms floor",
                request.support_ms
            ),
            term: None,
        };
    }
    match store.vote().await {
        Ok(Some(vote))
            if vote.term == request.term && vote.candidate != request.primary_node_id =>
        {
            return SupportAnswer::Refuse {
                message: format!(
                    "this voter voted in term {} for {}, not for {}",
                    vote.term, vote.candidate, request.primary_node_id
                ),
                term: Some(request.term),
            };
        }
        Ok(_) => {}
        // A vote that cannot be read may be one for another candidate.
        Err(e) => return SupportAnswer::Withhold(format!("could not read the vote: {e}")),
    }
    let (Some(boot_id), Some(v_recv)) = (received.boot_id.as_deref(), received.boottime) else {
        return SupportAnswer::Withhold(
            "this host has no CLOCK_BOOTTIME or no boot id, so a promise could not outlast an \
             agent restart (section 8.8)"
                .to_owned(),
        );
    };
    let stored = match store.support().await {
        Ok(stored) => stored,
        // Without the stored horizon a new one might be earlier.
        Err(e) => return SupportAnswer::Withhold(format!("could not read the support: {e}")),
    };
    let support = support_after(stored.as_ref(), request, boot_id, v_recv);
    if let Err(e) = store.save_support(&support).await {
        return SupportAnswer::Withhold(format!("could not store the promise: {e}"));
    }
    SupportAnswer::Grant(SupportGrant {
        term: request.term,
        support_ms: request.support_ms,
        max_lease_ms: request.max_lease_ms,
        voters_version,
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

    use super::{
        Received, StoredSupport, StoredVote, SupportAnswer, TermCheck, answer_support, check_term,
        horizon_span, rebase_at_start, support_after,
    };
    use crate::lease::{HostClock, ManualHostClock};
    use crate::store::{NodeStore, VOTE};
    use crate::util;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

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
        let answer = answer_support(store, Some(3120), request, received).await;
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
            let answer =
                answer_support(&store, Some(3120), &request(1, 15_000, 15_000), &received).await;
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
            answer_support(&store, None, &request(3, 15_000, 15_000), &now).await,
            SupportAnswer::Stale { highest: 4 }
        );
        assert_eq!(
            answer_support(&store, None, &request(4, 15_000, 15_000), &now).await,
            SupportAnswer::Ignore
        );
        assert!(matches!(
            answer_support(&store, Some(1), &request(4, 2_999, 15_000), &now).await,
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
            answer_support(&store, Some(1), &request(4, 15_000, 15_000), &now).await,
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
}
