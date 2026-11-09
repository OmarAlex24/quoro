//! What the primary keeps as the authority of `expiring` leases
//! (`node-agent-protocol.md` section 8.4, "What the authority must enforce",
//! and section 8.8; ADR 0203).
//!
//! - **The gate.** For each `expiring` lease, the latest
//!   `T_grant + duration_ms + 2M` over every grant the primary sent its
//!   current holder, on the primary's own monotonic clock. A shorter grant
//!   after a longer one never brings it forward. The primary grants the lease
//!   with a new token, to another node or to the same one after expiry or
//!   revoke, `expiring` or `local`, only once the gate has passed
//!   ([`Authority::record_grant`], [`Authority::record_local_grant`]);
//!   renewals by the current holder are not gated.
//! - **The restart rule.** No monotonic time survives a restart: at start
//!   every `expiring` lease counts as granted at that moment for its stored
//!   `leases.duration_ms`, the longest duration the primary may have granted.
//! - **Voter support.** An `expiring` grant of duration `d`, a first grant or
//!   the answer to a renewal, needs the support of a majority of voters whose
//!   `max_lease_ms` is at least `d` (section 8.8). An authority holds none
//!   until it is given a [`VoterSupport`]: the primary's session hub gives it
//!   the voters' answers ([`crate::support::SupportTracker`]), and tests may
//!   set their own.
//!
//! A grant is checked and recorded in one step, under one lock, before it
//! goes out: [`Authority::record_grant`] refuses it, or counts its gate from
//! the authority's clock at that moment. The caller does that inside the
//! single-writer transaction that raises the stored duration
//! ([`crate::leases::raise_duration`]) or writes a new token's row
//! ([`crate::leases::grant_app_lease`]), commits that transaction, and only
//! then sends the grant. A lowering checks [`Authority::may_lower`] inside its
//! own single-writer transaction ([`crate::leases::lower_duration`]), so a
//! longer grant and a lowering never interleave.
//!
//! All of it is in memory only, and nothing here writes the database:
//! renewals change no state and append no change-log entry. The API's
//! `renewed_at`, `expires_at` and `gate_at` are this memory's view, null after
//! a restart until the next grant ([`Authority::view`]).
//!
//! The clock is `CLOCK_MONOTONIC` in production ([`SystemMonotonic`]). It
//! stops while the primary's host is suspended, so a gate opens later in
//! real time, never sooner.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::store::format_rfc3339;

/// The longest `duration_ms` a grant may carry (section 5.12).
pub const MAX_DURATION_MS: u64 = 600_000;

/// The shortest `duration_ms` the authority grants (section 8.4).
pub const MIN_DURATION_MS: u64 = 3_000;

/// `D`, the duration of an `expiring` grant outside a maintenance window
/// (section 8.4, ADR 0171).
pub const DEFAULT_DURATION_MS: u64 = 15_000;

/// The margin of a grant of `duration_ms`: `M = max(1000 ms, D / 10)`
/// (section 8.4).
#[must_use]
pub fn margin_ms(duration_ms: u64) -> u64 {
    (duration_ms / 10).max(1_000)
}

/// How long after a grant of `duration_ms` its gate opens: `D + 2M`.
fn gate_after(duration_ms: u64) -> Duration {
    Duration::from_millis(duration_ms.saturating_add(2 * margin_ms(duration_ms)))
}

/// The monotonic clock the authority measures gates on.
pub trait MonotonicClock: Send + Sync + std::fmt::Debug {
    /// Time since an origin fixed when the clock was made. It never goes
    /// back.
    fn now(&self) -> Duration;
}

/// `CLOCK_MONOTONIC`, through [`Instant`].
#[derive(Debug)]
pub struct SystemMonotonic {
    origin: Instant,
}

impl Default for SystemMonotonic {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl MonotonicClock for SystemMonotonic {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// Test seam: a clock that moves only when told.
#[derive(Debug, Default)]
pub struct ManualClock(Mutex<Duration>);

impl ManualClock {
    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now += by;
    }
}

impl MonotonicClock for ManualClock {
    fn now(&self) -> Duration {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Whether a majority of voters supports this primary for a grant (section
/// 8.8).
pub trait VoterSupport: Send + Sync + std::fmt::Debug {
    /// Whether, now, a majority of the voter set supports this primary with a
    /// `max_lease_ms` of at least `duration_ms`.
    fn supports(&self, duration_ms: u64) -> bool;
}

/// No support at all, what an [`Authority`] answers until it is given a
/// [`VoterSupport`]. Without support the primary sends no `expiring` grant
/// and answers no renewal (section 8.8).
#[derive(Debug, Default)]
pub struct NoVoterSupport;

impl VoterSupport for NoVoterSupport {
    fn supports(&self, _duration_ms: u64) -> bool {
        false
    }
}

/// What an `expiring` grant is to the lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrantKind {
    /// A grant with a new token, to another node or to the same one after
    /// expiry or revoke: gated (section 8.4).
    NewToken,
    /// The answer to a renewal by the lease's current holder (`re` set): not
    /// gated.
    Renewal,
}

/// One `expiring` grant the primary is about to send. Its `T_grant` is the
/// authority's clock when [`Authority::record_grant`] records it.
#[derive(Debug)]
pub struct Grant<'a> {
    pub lease_id: &'a str,
    pub holder: &'a str,
    pub duration_ms: u64,
    pub kind: GrantKind,
    /// The wall-clock time of the grant, for the API's view only
    /// ([`LeaseTimes`]). Nothing decides on it.
    pub sent_wall: time::OffsetDateTime,
}

/// The primary's in-memory view of an `expiring` lease (`openapi.yaml`,
/// `Lease`). Wall-clock times, for display: after a restart `gate_at` shows
/// only the gates of this process's grants, not the restart rule's, so no
/// decision reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseTimes {
    /// When the primary last answered a renewal.
    pub renewed_at: Option<String>,
    /// The last grant's time plus its `duration_ms`.
    pub expires_at: String,
    /// The earliest time the primary may grant the lease with a new token.
    pub gate_at: String,
}

/// Why a grant was not recorded, so must not be sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// A new token while the lease's gate is open (section 8.4).
    GateOpen {
        /// How long until the gate passes.
        remaining: Duration,
    },
    /// No majority of voters supports a grant this long (section 8.8).
    NoSupport,
    /// A duration outside 3 000 to 600 000 ms (sections 5.12 and 8.4).
    Duration(u64),
    /// A renewal answer to a node that is not the lease's current holder.
    NotHolder,
}

/// The gate of one lease.
#[derive(Debug)]
struct Gate {
    holder: String,
    /// For each duration granted to `holder`, the latest `T_grant + d + 2M`
    /// of a grant of that duration. The gate is the latest of them; a
    /// duration's entry tells when its last grant's gate passes, so the
    /// stored `duration_ms` can be lowered below it ([`Authority::may_lower`]).
    by_duration: BTreeMap<u64, Duration>,
    /// What this process sent, for the API. `None` after a restart until the
    /// next grant (ADR 0203).
    times: Option<Times>,
}

#[derive(Debug)]
struct Times {
    renewed_at: Option<time::OffsetDateTime>,
    expires_at: time::OffsetDateTime,
    gate_at: time::OffsetDateTime,
}

impl Gate {
    fn gate(&self) -> Duration {
        self.by_duration
            .values()
            .copied()
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

/// The primary's gates and voter support ([module docs](self)).
#[derive(Debug)]
pub struct Authority {
    clock: Arc<dyn MonotonicClock>,
    gates: Mutex<HashMap<String, Gate>>,
    support: RwLock<Arc<dyn VoterSupport>>,
}

impl Authority {
    /// An authority on `clock` that holds no support.
    #[must_use]
    pub fn new(clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            clock,
            gates: Mutex::new(HashMap::new()),
            support: RwLock::new(Arc::new(NoVoterSupport)),
        }
    }

    /// The authority's clock now.
    #[must_use]
    pub fn now(&self) -> Duration {
        self.clock.now()
    }

    /// Whether a majority of voters supports a grant of `duration_ms`
    /// (section 8.8).
    #[must_use]
    pub fn supports(&self, duration_ms: u64) -> bool {
        self.support
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .supports(duration_ms)
    }

    /// Test seam: replaces how support is answered.
    pub fn set_support(&self, support: Arc<dyn VoterSupport>) {
        *self
            .support
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = support;
    }

    fn gates(&self) -> std::sync::MutexGuard<'_, HashMap<String, Gate>> {
        self.gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The restart rule: `lease_id`, `expiring` in the database with
    /// `holder` as its last holder, counts as granted now for
    /// `duration_ms`, the longest duration this primary may have granted. A
    /// revoked lease counts too: a lost holder's lease keeps its gate
    /// (section 8.3, ADR 0202). A missing duration, or one below
    /// [`MIN_DURATION_MS`], counts as [`MAX_DURATION_MS`]; one above it counts
    /// as stored. Either way the gate is no shorter than the stored value's.
    /// The view stays empty until the next grant.
    pub fn seed(&self, lease_id: &str, holder: &str, duration_ms: Option<i64>) {
        let stored = duration_ms.and_then(|d| u64::try_from(d).ok());
        let duration_ms = match stored {
            Some(d) if (MIN_DURATION_MS..=MAX_DURATION_MS).contains(&d) => d,
            Some(d) if d > MAX_DURATION_MS => {
                tracing::warn!(
                    lease_id,
                    duration_ms = d,
                    "an expiring lease stores a duration above the longest; its gate counts it"
                );
                d
            }
            _ => {
                tracing::warn!(
                    lease_id,
                    ?duration_ms,
                    "an expiring lease has no valid duration; its gate counts the longest"
                );
                MAX_DURATION_MS
            }
        };
        let gate = self.now() + gate_after(duration_ms);
        self.gates().insert(
            lease_id.to_string(),
            Gate {
                holder: holder.to_string(),
                by_duration: BTreeMap::from([(duration_ms, gate)]),
                times: None,
            },
        );
    }

    /// Checks an `expiring` grant and records it, before it goes out, with
    /// `T_grant` the authority's clock now. Refused, it must not be sent:
    ///
    /// - a duration outside 3 000 to 600 000 ms;
    /// - no majority support for the duration (section 8.8);
    /// - a [`GrantKind::NewToken`] while the lease's gate is open, whatever
    ///   the holder (section 8.4);
    /// - a [`GrantKind::Renewal`] to a node that is not the current holder.
    ///
    /// A new token starts the holder's gate afresh. A renewal answer raises
    /// the gate when it ends later, and never brings it forward. A renewal of
    /// a lease with no gate starts one: a converted lease gets its first gate
    /// from the first answer (ADR 0254; conversions are not built yet).
    ///
    /// # Errors
    ///
    /// Why the grant was refused ([`Refused`]).
    pub fn record_grant(&self, grant: &Grant<'_>) -> Result<(), Refused> {
        if !(MIN_DURATION_MS..=MAX_DURATION_MS).contains(&grant.duration_ms) {
            return Err(Refused::Duration(grant.duration_ms));
        }
        if !self.supports(grant.duration_ms) {
            return Err(Refused::NoSupport);
        }
        let after = gate_after(grant.duration_ms);
        let gate_wall = grant.sent_wall + after;
        let expires_wall = grant.sent_wall + Duration::from_millis(grant.duration_ms);
        let mut gates = self.gates();
        let now = self.now();
        let current = gates.get(grant.lease_id);
        match grant.kind {
            GrantKind::NewToken => {
                if let Some(current) = current {
                    let open = current.gate();
                    if now < open {
                        return Err(Refused::GateOpen {
                            remaining: open - now,
                        });
                    }
                }
                gates.insert(
                    grant.lease_id.to_string(),
                    Gate {
                        holder: grant.holder.to_string(),
                        by_duration: BTreeMap::new(),
                        times: None,
                    },
                );
            }
            GrantKind::Renewal => {
                if current.is_some_and(|current| current.holder != grant.holder) {
                    return Err(Refused::NotHolder);
                }
            }
        }
        let entry = gates
            .entry(grant.lease_id.to_string())
            .or_insert_with(|| Gate {
                holder: grant.holder.to_string(),
                by_duration: BTreeMap::new(),
                times: None,
            });
        let gate_mono = now + after;
        let slot = entry
            .by_duration
            .entry(grant.duration_ms)
            .or_insert(gate_mono);
        *slot = (*slot).max(gate_mono);
        let renewed_at = if grant.kind == GrantKind::Renewal {
            Some(grant.sent_wall)
        } else {
            entry.times.as_ref().and_then(|times| times.renewed_at)
        };
        let gate_at = entry
            .times
            .as_ref()
            .map_or(gate_wall, |times| times.gate_at.max(gate_wall));
        entry.times = Some(Times {
            renewed_at,
            expires_at: expires_wall,
            gate_at,
        });
        Ok(())
    }

    /// Checks a `local` grant of `lease_id` with a new token, before it goes
    /// out: refused while the lease's gate is open, whatever the holder
    /// (section 8.4, ADR 0258). Allowed, the lease's gate is dropped: a
    /// `local` lease has none.
    ///
    /// # Errors
    ///
    /// [`Refused::GateOpen`] with the time until the gate passes.
    pub fn record_local_grant(&self, lease_id: &str) -> Result<(), Refused> {
        let mut gates = self.gates();
        let now = self.now();
        if let Some(open) = gates.get(lease_id).map(Gate::gate)
            && now < open
        {
            return Err(Refused::GateOpen {
                remaining: open - now,
            });
        }
        gates.remove(lease_id);
        Ok(())
    }

    /// Whether `lease_id` may be granted with a new token: its holder's gate
    /// has passed, or the lease has none (it was never `expiring` in this
    /// process or at its start).
    #[must_use]
    pub fn passed(&self, lease_id: &str) -> bool {
        let now = self.now();
        self.gates()
            .get(lease_id)
            .is_none_or(|gate| gate.gate() <= now)
    }

    /// Whether the stored `duration_ms` of `lease_id` may be lowered to
    /// `to_ms`: the gate of every grant longer than that has passed (ADR
    /// 0203). A lease with no gate had no `expiring` grant in this process
    /// and was not `expiring` at its start, so none is running; a conversion
    /// grant (ADR 0254, not built yet) must add its duration here before it
    /// is sent.
    #[must_use]
    pub fn may_lower(&self, lease_id: &str, to_ms: u64) -> bool {
        let now = self.now();
        self.gates().get(lease_id).is_none_or(|gate| {
            gate.by_duration
                .range(to_ms.saturating_add(1)..)
                .all(|(_, passes)| *passes <= now)
        })
    }

    /// The primary's view of `lease_id`'s times, `None` until this process
    /// sent it a grant.
    #[must_use]
    pub fn view(&self, lease_id: &str) -> Option<LeaseTimes> {
        let gates = self.gates();
        let times = gates.get(lease_id)?.times.as_ref()?;
        Some(LeaseTimes {
            renewed_at: times.renewed_at.map(format_rfc3339),
            expires_at: format_rfc3339(times.expires_at),
            gate_at: format_rfc3339(times.gate_at),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: u64 = DEFAULT_DURATION_MS;
    const NEW: GrantKind = GrantKind::NewToken;
    const RENEWAL: GrantKind = GrantKind::Renewal;

    #[derive(Debug)]
    struct UpTo(u64);

    impl VoterSupport for UpTo {
        fn supports(&self, duration_ms: u64) -> bool {
            duration_ms <= self.0
        }
    }

    /// An authority on a manual clock, supported for every duration.
    fn authority() -> (Authority, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::default());
        let authority = Authority::new(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        authority.set_support(Arc::new(UpTo(MAX_DURATION_MS)));
        (authority, clock)
    }

    fn wall(seconds: i64) -> time::OffsetDateTime {
        time::OffsetDateTime::from_unix_timestamp(1_800_000_000 + seconds).unwrap()
    }

    fn grant(holder: &str, duration_ms: u64, kind: GrantKind, wall_s: i64) -> Grant<'_> {
        Grant {
            lease_id: "lse_a",
            holder,
            duration_ms,
            kind,
            sent_wall: wall(wall_s),
        }
    }

    fn gate_open(seconds: u64) -> Result<(), Refused> {
        Err(Refused::GateOpen {
            remaining: Duration::from_secs(seconds),
        })
    }

    #[test]
    fn the_margin_is_a_tenth_of_the_duration_and_at_least_a_second() {
        assert_eq!(margin_ms(15_000), 1_500);
        assert_eq!(margin_ms(3_000), 1_000);
        assert_eq!(margin_ms(600_000), 60_000);
    }

    /// Section 8.4: `T_gate = T_grant + D + 2M`, 18 s for `D` = 15 s.
    #[test]
    fn a_grant_opens_its_gate_after_its_duration_and_two_margins() {
        let (authority, clock) = authority();
        authority.record_grant(&grant("nod_a", D, NEW, 0)).unwrap();
        assert!(!authority.passed("lse_a"));
        clock.advance(Duration::from_millis(17_999));
        assert!(!authority.passed("lse_a"));
        clock.advance(Duration::from_millis(1));
        assert!(authority.passed("lse_a"));
        assert!(authority.passed("lse_other"), "a lease with no gate");
    }

    /// ADR 0203: a shorter grant after a longer one never brings the gate
    /// forward; a later grant moves it later.
    #[test]
    fn a_shorter_grant_after_a_longer_one_keeps_the_gate() {
        let (authority, clock) = authority();
        authority
            .record_grant(&grant("nod_a", 180_000, NEW, 0))
            .unwrap();
        clock.advance(Duration::from_secs(1));
        authority
            .record_grant(&grant("nod_a", D, RENEWAL, 1))
            .unwrap();
        // 180 s + 2 x 18 s from the first grant.
        clock.advance(Duration::from_secs(214));
        assert!(!authority.passed("lse_a"));
        clock.advance(Duration::from_secs(1));
        assert!(authority.passed("lse_a"));

        authority
            .record_grant(&grant("nod_a", D, RENEWAL, 216))
            .unwrap();
        clock.advance(Duration::from_millis(17_999));
        assert!(!authority.passed("lse_a"));
    }

    /// Renewals by the current holder are not gated, a new holder is: no
    /// lease goes to another node before the old holder has fenced.
    #[test]
    fn a_new_holder_waits_for_the_old_holders_gate() {
        let (authority, clock) = authority();
        authority.record_grant(&grant("nod_a", D, NEW, 0)).unwrap();
        authority
            .record_grant(&grant("nod_a", D, RENEWAL, 0))
            .unwrap();
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            authority.record_grant(&grant("nod_b", D, NEW, 10)),
            gate_open(8)
        );
        clock.advance(Duration::from_secs(8));
        authority.record_grant(&grant("nod_b", D, NEW, 18)).unwrap();
        // The new holder's gate starts at its own grant.
        assert!(!authority.passed("lse_a"));
        assert_eq!(
            authority.record_grant(&grant("nod_a", D, NEW, 18)),
            gate_open(18)
        );
        assert_eq!(
            authority.record_grant(&grant("nod_a", D, RENEWAL, 18)),
            Err(Refused::NotHolder),
            "the old holder's renewal is not answered"
        );
    }

    /// Section 8.4: a new token to the same node after expiry or revoke, and
    /// a `local` grant with a new token (ADR 0258), wait for the gate too.
    #[test]
    fn a_new_token_to_the_same_holder_or_a_local_grant_waits_for_the_gate() {
        let (authority, clock) = authority();
        authority.record_grant(&grant("nod_a", D, NEW, 0)).unwrap();
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            authority.record_grant(&grant("nod_a", D, NEW, 10)),
            gate_open(8)
        );
        assert_eq!(authority.record_local_grant("lse_a"), gate_open(8));
        clock.advance(Duration::from_secs(8));
        authority.record_local_grant("lse_a").unwrap();
        assert!(authority.passed("lse_a"));
        assert_eq!(authority.view("lse_a"), None, "a local lease has no gate");
        authority.record_local_grant("lse_other").unwrap();
    }

    /// A grant is refused, and records nothing, without majority support for
    /// its duration or outside 3 000 to 600 000 ms.
    #[test]
    fn a_grant_needs_support_and_a_duration_in_range() {
        let (authority, _) = authority();
        authority.set_support(Arc::new(UpTo(D)));
        assert_eq!(
            authority.record_grant(&grant("nod_a", D + 1, NEW, 0)),
            Err(Refused::NoSupport)
        );
        assert_eq!(
            authority.record_grant(&grant("nod_a", 2_999, NEW, 0)),
            Err(Refused::Duration(2_999))
        );
        authority.set_support(Arc::new(UpTo(u64::MAX)));
        assert_eq!(
            authority.record_grant(&grant("nod_a", 600_001, NEW, 0)),
            Err(Refused::Duration(600_001))
        );
        assert!(authority.passed("lse_a"));
        assert_eq!(authority.view("lse_a"), None);
        authority
            .record_grant(&grant("nod_a", MIN_DURATION_MS, NEW, 0))
            .unwrap();
    }

    /// The restart rule: every `expiring` lease, revoked or not, counts as
    /// granted at start for its stored duration, and the view stays empty
    /// until the next grant.
    #[test]
    fn a_restarted_primary_counts_every_gate_from_its_start() {
        let (authority, clock) = authority();
        clock.advance(Duration::from_secs(100));
        authority.seed("lse_a", "nod_a", Some(15_000));
        authority.seed("lse_b", "nod_a", None);
        authority.seed("lse_c", "nod_a", Some(2_999));
        authority.seed("lse_d", "nod_a", Some(1_200_000));
        assert!(!authority.passed("lse_a"));
        assert_eq!(authority.view("lse_a"), None);
        assert_eq!(
            authority.record_grant(&grant("nod_b", D, NEW, 0)),
            gate_open(18)
        );
        clock.advance(Duration::from_secs(18));
        assert!(authority.passed("lse_a"));
        // No valid duration counts as the longest: 600 s + 2 x 60 s.
        for lease in ["lse_b", "lse_c"] {
            assert!(!authority.passed(lease));
        }
        clock.advance(Duration::from_secs(720 - 18 - 1));
        for lease in ["lse_b", "lse_c"] {
            assert!(!authority.passed(lease));
        }
        clock.advance(Duration::from_secs(1));
        for lease in ["lse_b", "lse_c"] {
            assert!(authority.passed(lease));
        }
        // One above the longest counts as stored: 1 200 s + 2 x 120 s.
        clock.advance(Duration::from_secs(1_440 - 720 - 1));
        assert!(!authority.passed("lse_d"));
        clock.advance(Duration::from_secs(1));
        assert!(authority.passed("lse_d"));
    }

    /// ADR 0203: the stored duration is lowered only once every longer
    /// grant's gate has passed.
    #[test]
    fn a_duration_is_lowered_only_after_the_longer_grants_gate() {
        let (authority, clock) = authority();
        assert!(authority.may_lower("lse_a", D));
        authority
            .record_grant(&grant("nod_a", 180_000, NEW, 0))
            .unwrap();
        authority
            .record_grant(&grant("nod_a", D, RENEWAL, 0))
            .unwrap();
        assert!(!authority.may_lower("lse_a", D));
        assert!(authority.may_lower("lse_a", 180_000));
        clock.advance(Duration::from_secs(215));
        assert!(!authority.may_lower("lse_a", D));
        clock.advance(Duration::from_secs(1));
        assert!(authority.may_lower("lse_a", D));
    }

    /// The view: `expires_at` is the last grant's time plus its duration,
    /// `gate_at` the latest gate, `renewed_at` the last renewal answer.
    #[test]
    fn the_view_shows_the_last_grant_and_the_latest_gate() {
        let (authority, clock) = authority();
        authority
            .record_grant(&grant("nod_a", 180_000, NEW, 0))
            .unwrap();
        let first = authority.view("lse_a").unwrap();
        assert_eq!(first.renewed_at, None);
        assert_eq!(first.expires_at, format_rfc3339(wall(180)));
        assert_eq!(first.gate_at, format_rfc3339(wall(216)));
        clock.advance(Duration::from_secs(5));
        authority
            .record_grant(&grant("nod_a", D, RENEWAL, 5))
            .unwrap();
        let second = authority.view("lse_a").unwrap();
        assert_eq!(second.renewed_at, Some(format_rfc3339(wall(5))));
        assert_eq!(second.expires_at, format_rfc3339(wall(20)));
        assert_eq!(second.gate_at, format_rfc3339(wall(216)));
    }

    /// Section 8.8: a new authority holds no support, so every grant is
    /// refused until it is given a [`VoterSupport`].
    #[test]
    fn no_support_is_held_until_one_is_set() {
        let authority = Authority::new(Arc::new(ManualClock::default()));
        assert!(!authority.supports(D));
        assert_eq!(
            authority.record_grant(&grant("nod_a", D, NEW, 0)),
            Err(Refused::NoSupport)
        );
        authority.set_support(Arc::new(UpTo(D)));
        assert!(authority.supports(D));
        assert!(!authority.supports(D + 1));
    }
}
