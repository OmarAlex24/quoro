//! What the primary keeps as the authority of `expiring` leases
//! (`node-agent-protocol.md` section 8.4, "What the authority must enforce",
//! and section 8.8; ADR 0203).
//!
//! - **The gate.** For each `expiring` lease, the latest
//!   `T_grant + duration_ms + 2M` over every grant the primary sent its
//!   current holder, on the primary's own monotonic clock. A shorter grant
//!   after a longer one never brings it forward. The primary grants the lease
//!   with a new token, to another node or to the same one after expiry or
//!   revoke, only once the gate has passed; renewals by the current holder are
//!   not gated.
//! - **The restart rule.** No monotonic time survives a restart: at start
//!   every `expiring` lease counts as granted at that moment for its stored
//!   `leases.duration_ms`, the longest duration the primary may have granted.
//! - **Voter support.** An `expiring` grant of duration `d`, a first grant or
//!   the answer to a renewal, needs the support of a majority of voters whose
//!   `max_lease_ms` is at least `d` (section 8.8). The support accounting is
//!   not built yet, so every build refuses ([`NoVoterSupport`]); tests set
//!   their own.
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

/// The support of a primary that counts no voter support yet: none. Without
/// support the primary sends no `expiring` grant and answers no renewal
/// (section 8.8). Every build uses it until the support accounting lands.
#[derive(Debug, Default)]
pub struct NoVoterSupport;

impl VoterSupport for NoVoterSupport {
    fn supports(&self, _duration_ms: u64) -> bool {
        false
    }
}

/// One `expiring` grant the primary is about to send.
#[derive(Debug)]
pub struct Grant<'a> {
    pub lease_id: &'a str,
    pub holder: &'a str,
    pub duration_ms: u64,
    /// `T_grant`: the authority's clock when the grant goes out, read by the
    /// caller in the step that sends it ([`Authority::now`]).
    pub sent: Duration,
    /// The wall-clock time of the grant, its `granted_at`.
    pub sent_wall: time::OffsetDateTime,
    /// Whether the grant answers a renewal (`re` set), for `renewed_at`.
    pub renewal: bool,
}

/// The primary's in-memory view of an `expiring` lease (`openapi.yaml`,
/// `Lease`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseTimes {
    /// When the primary last answered a renewal.
    pub renewed_at: Option<String>,
    /// The last grant's time plus its `duration_ms`.
    pub expires_at: String,
    /// The earliest time the primary may grant the lease with a new token.
    pub gate_at: String,
}

/// Why a grant to a new holder was not recorded: the current holder's gate is
/// still open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GateOpen {
    /// How long until the gate passes.
    pub remaining: Duration,
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
    /// (section 8.3, ADR 0202). A missing or invalid duration counts as
    /// [`MAX_DURATION_MS`]. The view stays empty until the next grant.
    pub fn seed(&self, lease_id: &str, holder: &str, duration_ms: Option<i64>) {
        let duration_ms = match duration_ms.and_then(|d| u64::try_from(d).ok()) {
            Some(d) if (1..=MAX_DURATION_MS).contains(&d) => d,
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

    /// Records an `expiring` grant before it goes out. A grant to the
    /// lease's current holder raises its gate when it ends later, and never
    /// brings it forward. A grant to another holder starts a new gate, and is
    /// refused while the current holder's gate is open: no lease goes to a
    /// new holder before the old one has fenced (section 8.4).
    pub fn record_grant(&self, grant: &Grant<'_>) -> Result<(), GateOpen> {
        let now = self.now();
        let after = gate_after(grant.duration_ms);
        let gate_mono = grant.sent + after;
        let gate_wall = grant.sent_wall + after;
        let expires_wall = grant.sent_wall + Duration::from_millis(grant.duration_ms);
        let mut gates = self.gates();
        if let Some(current) = gates.get(grant.lease_id)
            && current.holder != grant.holder
        {
            let open = current.gate();
            if now < open {
                return Err(GateOpen {
                    remaining: open - now,
                });
            }
        }
        let entry = gates
            .entry(grant.lease_id.to_string())
            .and_modify(|gate| {
                if gate.holder != grant.holder {
                    *gate = Gate {
                        holder: grant.holder.to_string(),
                        by_duration: BTreeMap::new(),
                        times: None,
                    };
                }
            })
            .or_insert_with(|| Gate {
                holder: grant.holder.to_string(),
                by_duration: BTreeMap::new(),
                times: None,
            });
        let slot = entry
            .by_duration
            .entry(grant.duration_ms)
            .or_insert(gate_mono);
        *slot = (*slot).max(gate_mono);
        let renewed_at = if grant.renewal {
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
    /// 0203).
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

    fn authority() -> (Authority, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::default());
        (
            Authority::new(Arc::clone(&clock) as Arc<dyn MonotonicClock>),
            clock,
        )
    }

    fn wall(seconds: i64) -> time::OffsetDateTime {
        time::OffsetDateTime::from_unix_timestamp(1_800_000_000 + seconds).unwrap()
    }

    fn grant<'a>(
        authority: &Authority,
        holder: &'a str,
        duration_ms: u64,
        renewal: bool,
        wall_s: i64,
    ) -> Grant<'a> {
        Grant {
            lease_id: "lse_a",
            holder,
            duration_ms,
            sent: authority.now(),
            sent_wall: wall(wall_s),
            renewal,
        }
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
        authority
            .record_grant(&grant(&authority, "nod_a", D, false, 0))
            .unwrap();
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
            .record_grant(&grant(&authority, "nod_a", 180_000, false, 0))
            .unwrap();
        clock.advance(Duration::from_secs(1));
        authority
            .record_grant(&grant(&authority, "nod_a", D, true, 1))
            .unwrap();
        // 180 s + 2 x 18 s from the first grant.
        clock.advance(Duration::from_secs(214));
        assert!(!authority.passed("lse_a"));
        clock.advance(Duration::from_secs(1));
        assert!(authority.passed("lse_a"));

        authority
            .record_grant(&grant(&authority, "nod_a", D, true, 216))
            .unwrap();
        clock.advance(Duration::from_millis(17_999));
        assert!(!authority.passed("lse_a"));
    }

    /// Renewals by the current holder are not gated, a new holder is: no
    /// lease goes to another node before the old holder has fenced.
    #[test]
    fn a_new_holder_waits_for_the_old_holders_gate() {
        let (authority, clock) = authority();
        authority
            .record_grant(&grant(&authority, "nod_a", D, false, 0))
            .unwrap();
        authority
            .record_grant(&grant(&authority, "nod_a", D, true, 0))
            .unwrap();
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            authority.record_grant(&grant(&authority, "nod_b", D, false, 10)),
            Err(GateOpen {
                remaining: Duration::from_secs(8)
            })
        );
        clock.advance(Duration::from_secs(8));
        authority
            .record_grant(&grant(&authority, "nod_b", D, false, 18))
            .unwrap();
        // The new holder's gate starts at its own grant.
        assert!(!authority.passed("lse_a"));
        assert_eq!(
            authority.record_grant(&grant(&authority, "nod_a", D, false, 18)),
            Err(GateOpen {
                remaining: Duration::from_secs(18)
            })
        );
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
        assert!(!authority.passed("lse_a"));
        assert_eq!(authority.view("lse_a"), None);
        assert_eq!(
            authority.record_grant(&grant(&authority, "nod_b", D, false, 0)),
            Err(GateOpen {
                remaining: Duration::from_secs(18)
            })
        );
        clock.advance(Duration::from_secs(18));
        assert!(authority.passed("lse_a"));
        // No valid duration counts as the longest: 600 s + 2 x 60 s.
        assert!(!authority.passed("lse_b"));
        clock.advance(Duration::from_secs(720 - 18 - 1));
        assert!(!authority.passed("lse_b"));
        clock.advance(Duration::from_secs(1));
        assert!(authority.passed("lse_b"));
    }

    /// ADR 0203: the stored duration is lowered only once every longer
    /// grant's gate has passed.
    #[test]
    fn a_duration_is_lowered_only_after_the_longer_grants_gate() {
        let (authority, clock) = authority();
        assert!(authority.may_lower("lse_a", D));
        authority
            .record_grant(&grant(&authority, "nod_a", 180_000, false, 0))
            .unwrap();
        authority
            .record_grant(&grant(&authority, "nod_a", D, true, 0))
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
            .record_grant(&grant(&authority, "nod_a", 180_000, false, 0))
            .unwrap();
        let first = authority.view("lse_a").unwrap();
        assert_eq!(first.renewed_at, None);
        assert_eq!(first.expires_at, format_rfc3339(wall(180)));
        assert_eq!(first.gate_at, format_rfc3339(wall(216)));
        clock.advance(Duration::from_secs(5));
        authority
            .record_grant(&grant(&authority, "nod_a", D, true, 5))
            .unwrap();
        let second = authority.view("lse_a").unwrap();
        assert_eq!(second.renewed_at, Some(format_rfc3339(wall(5))));
        assert_eq!(second.expires_at, format_rfc3339(wall(20)));
        assert_eq!(second.gate_at, format_rfc3339(wall(216)));
    }

    /// Section 8.8: no build counts voter support yet, so none is held; a
    /// test can set its own.
    #[test]
    fn no_support_is_held_until_a_test_sets_it() {
        #[derive(Debug)]
        struct UpTo(u64);
        impl VoterSupport for UpTo {
            fn supports(&self, duration_ms: u64) -> bool {
                duration_ms <= self.0
            }
        }
        let (authority, _) = authority();
        assert!(!authority.supports(D));
        authority.set_support(Arc::new(UpTo(D)));
        assert!(authority.supports(D));
        assert!(!authority.supports(D + 1));
    }
}
