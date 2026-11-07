// SPDX-License-Identifier: AGPL-3.0-only
//! The pure lease state machine of `docs/contracts/node-agent-protocol.md`
//! section 8: no I/O, no async, no clock reads.
//!
//! Every method takes `now: Instant` from the caller, so the same code runs
//! against the paused test clock and against the node's real monotonic clock.
//!
//! Those instants are on the lease clock of section 8.4 only once a host
//! suspend is counted: the caller measures it ([`crate::lease::SuspendWatch`])
//! and hands it to [`LeaseTable::catch_up`], which brings every instant the
//! table keeps earlier by it (ADR 0240).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use quoro_proto::{FenceReason, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, MsgId, ResourceType};
use tokio::time::Instant;

use crate::lease::clock::{HostNow, StoredValidity};
use crate::lease::store::StoredLease;

/// The floor the authority must respect for an `expiring` grant (section 8.4).
pub const MIN_DURATION: Duration = Duration::from_millis(3000);

/// The longest `duration_ms` a grant may carry (section 5.12).
const MAX_DURATION: Duration = Duration::from_millis(600_000);

/// A measured suspend shorter than this is not applied yet: it is within the
/// noise of reading two clocks, and it is applied once more adds up.
const SUSPEND_RESOLUTION: Duration = Duration::from_millis(1);

/// `M = max(1000 ms, D / 10)`, the margin of section 8.4.
#[must_use]
pub fn margin(d: Duration) -> Duration {
    std::cmp::max(Duration::from_millis(1000), d / 10)
}

/// The node-side state of one lease (section 8.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseState {
    /// The node may run the resource's stateful VMs.
    Held,
    /// The authority is unreachable or a renewal is late. Keep running, start
    /// nothing new stateful (section 8.4).
    Uncertain,
    /// `valid_until` passed without a renewal. The node must self-fence.
    Expired,
    /// A `lease_revoke` ended the lease. The node must self-fence.
    Revoked,
}

/// One lease as the node tracks it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    /// Lease identity.
    pub id: LeaseId,
    /// What the lease protects.
    pub resource_type: ResourceType,
    /// The `app_` or `nod_` id.
    pub resource_id: String,
    /// `local` or `expiring`.
    pub mode: LeaseMode,
    /// The fencing token the node holds.
    pub token: u64,
    /// The current state.
    pub state: LeaseState,
    /// `D` from the grant; `None` for `local`.
    pub duration: Option<Duration>,
    /// `t_send + D - M`; `None` for `local`, until a first renewal is
    /// answered, and once fenced (section 8.4). A lease converted from
    /// `local` gets it when its first renewal is sent instead: its
    /// provisional deadline (section 8.4 "Changing the mode", ADR 0241). An
    /// `uncertain` lease with `None` never expires by the clock.
    pub valid_until: Option<Instant>,
    /// `valid_until` as it is stored: `CLOCK_BOOTTIME(t_send) + D - M`, read
    /// when the renewal was sent (section 8.4 "Agent restart", ADR 0240), or
    /// the stored deadline a restore found. `None` whenever `valid_until` is,
    /// and on a host that cannot tell its boot.
    validity: Option<StoredValidity>,
    /// Whether `valid_until` is still a converted lease's provisional
    /// deadline: its first renewal is out, and no answer has been taken since
    /// the conversion (ADR 0241). The conversion grant counts toward no gate,
    /// so until an answer is taken no gate backs this deadline.
    provisional: bool,
    /// When to send the next renewal; `None` for `local` (section 8.4).
    next_renew_at: Option<Instant>,
    /// Renewals sent and not yet answered.
    outstanding: Vec<Renewal>,
}

/// One renewal sent and not yet answered.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Renewal {
    msg: MsgId,
    /// `t_send`, on the lease clock.
    sent: Instant,
    /// `CLOCK_BOOTTIME` read before `sent`, in its stored form. An answer
    /// stores this plus its own `D - M`: the answer may carry another
    /// duration than the lease had when the renewal went out.
    sent_boottime: Option<StoredValidity>,
}

impl Lease {
    /// The stored form of `valid_until` (section 8.4 "Agent restart").
    #[must_use]
    pub fn validity(&self) -> Option<&StoredValidity> {
        self.validity.as_ref()
    }

    /// Whether this is a lease converted from `local` to `expiring` whose
    /// first renewal has not been sent yet (section 8.4 "Changing the mode",
    /// ADR 0241). The authority has set no gate for it, so it behaves as
    /// `local`: a lost session neither makes it `uncertain` nor stops its
    /// VMs.
    ///
    /// Only such a lease is `held` and `expiring` without a deadline: any
    /// other becomes `held` through an answer, which sets `valid_until`, and
    /// keeps it until it is fenced.
    #[must_use]
    pub fn awaits_first_renewal(&self) -> bool {
        self.mode == LeaseMode::Expiring
            && self.state == LeaseState::Held
            && self.valid_until.is_none()
    }

    /// Brings every instant the lease keeps on the lease clock earlier by
    /// `by`, the length of a host suspend (ADR 0240), and none before `floor`.
    ///
    /// Section 8.4 names the timer for `valid_until`. The send times of the
    /// renewals still outstanding move too, or an answer to a renewal sent
    /// before the suspend would set a `valid_until` later by the suspend. The
    /// next renewal moves with them, so the lease renews as soon as the host
    /// is back. The stored deadline does not move: it is on `CLOCK_BOOTTIME`,
    /// which counted the suspend. An instant added to the lease on the lease
    /// clock must move here too.
    ///
    /// A renewal whose send time cannot move that far is dropped instead of
    /// floored: its answer then matches nothing and changes nothing, so no
    /// floor that is later than the real send time can set `valid_until`.
    fn suspended(&mut self, by: Duration, floor: Instant) {
        let earlier = |at: Instant| at.checked_sub(by).unwrap_or(floor);
        self.valid_until = self.valid_until.map(earlier);
        self.next_renew_at = self.next_renew_at.map(earlier);
        self.outstanding
            .retain_mut(|renewal| match renewal.sent.checked_sub(by) {
                Some(sent) => {
                    renewal.sent = sent;
                    true
                }
                None => false,
            });
    }
}

/// Something the caller must do now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send a `lease_renew` for this lease and token (section 5.5).
    SendRenew {
        /// The lease to renew.
        lease_id: LeaseId,
        /// The token to echo.
        fencing_token: u64,
    },
    /// Self-fence the VMs bound to this lease (section 8.5).
    Fence {
        /// The lease that was lost.
        lease_id: LeaseId,
        /// Why it was lost.
        reason: FenceReason,
    },
}

/// A message the machine refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The message is malformed or not acceptable; answered with a non-fatal
    /// `error` code `bad_message` (section 5.18).
    BadMessage(String),
}

/// What a revoke did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RevokeOutcome {
    /// The lease is now revoked; fence it.
    Fence {
        /// The lease that was revoked.
        lease_id: LeaseId,
    },
    /// No lease by that id is known, so there is nothing bound to stop.
    UnknownLease,
}

/// Every lease the node tracks, plus the highest token seen per resource.
#[derive(Clone, Debug)]
pub struct LeaseTable {
    leases: HashMap<LeaseId, Lease>,
    highest_token: HashMap<(ResourceType, String), u64>,
    /// The part of the measured suspend already applied by [`Self::catch_up`].
    suspend_applied: Duration,
}

impl LeaseTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            leases: HashMap::new(),
            highest_token: HashMap::new(),
            suspend_applied: Duration::ZERO,
        }
    }

    /// Section 8.4 "Suspend" (ADR 0240): `suspended` is how long the host has
    /// been suspended since the table's lease clock started, as measured now.
    /// Whatever part of it is new moves every `held` or `uncertain`
    /// `expiring` lease's instants earlier by that much, so the next
    /// [`Self::poll`] fences a lease whose `valid_until` passed during the
    /// suspend, makes a lease whose renewal went unanswered meanwhile
    /// `uncertain`, and renews the rest. Returns the part it applied.
    ///
    /// The caller measures and catches up before it records any instant, so
    /// an instant taken after a suspend never moves for it: a suspend while no
    /// `expiring` lease is held shortens no lease granted afterwards.
    pub fn catch_up(&mut self, suspended: Duration, now: Instant) -> Duration {
        let by = suspended.saturating_sub(self.suspend_applied);
        if by < SUSPEND_RESOLUTION {
            return Duration::ZERO;
        }
        self.suspend_applied = suspended;
        // An instant too early to represent is long past: as early as the
        // longest grant would ever look back, so a fence that counts its
        // budget from it has none of its graceful stop left.
        let floor = now.checked_sub(MAX_DURATION).unwrap_or(now);
        for lease in self.leases.values_mut() {
            if lease.mode == LeaseMode::Expiring
                && matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
            {
                lease.suspended(by, floor);
            }
        }
        by
    }

    /// The part of the measured suspend [`Self::catch_up`] has applied: a
    /// suspend measured beyond it has moved no instant yet. A fence that runs
    /// on after its lease left the table's catch-up counts the rest itself
    /// (ADR 0240).
    #[must_use]
    pub fn suspend_applied(&self) -> Duration {
        self.suspend_applied
    }

    /// Whether any lease keeps instants on the lease clock: an `expiring`
    /// lease that is `held` or `uncertain`. Only then does a suspend need to
    /// be checked for (section 8.4 "Suspend").
    #[must_use]
    pub fn has_timed_leases(&self) -> bool {
        self.leases.values().any(|lease| {
            lease.mode == LeaseMode::Expiring
                && matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
        })
    }

    /// Rebuilds the table from what the local store kept across a restart
    /// (section 8.5 step 2, section 12 `leases`).
    ///
    /// Rules, all chosen so a stateful VM can never run twice:
    ///
    /// - `local` leases come back with their state and token.
    /// - `expiring` leases that were `held` or `uncertain` come back
    ///   `uncertain` with their remaining validity when it was stored on this
    ///   host boot and has not run out on `CLOCK_BOOTTIME` (section 8.4 "Agent
    ///   restart", ADR 0072): they renew at once and start nothing until the
    ///   authority answers. Otherwise they come back `expired` and are fenced:
    ///   after a reboot, or with no stored validity (a first grant never
    ///   answered), nothing proves the lease is still valid.
    /// - `expired` and `revoked` stay as they are. One whose fence has no
    ///   record for its token was interrupted by the restart (section 8.5
    ///   "Concurrency", ADR 0075): it is fenced again, because the VMMs outlive
    ///   the agent and a half-done stop may have left a VM running.
    /// - The highest token per resource comes back exactly.
    ///
    /// `fenced` holds the lease id and token of every recorded fence, complete
    /// or not: a failed fence is covered by the node's failed mark instead.
    /// `now` is one reading of the host clock and the monotonic clock.
    #[must_use]
    pub fn restore(
        leases: Vec<StoredLease>,
        highest: Vec<((ResourceType, String), u64)>,
        fenced: &HashSet<(LeaseId, u64)>,
        now: &HostNow,
    ) -> (Self, Vec<Action>) {
        let mut table = Self::new();
        for entry in highest {
            table.highest_token.insert(entry.0, entry.1);
        }
        let mut actions = Vec::new();
        for stored in leases {
            let mut valid_until = None;
            let mut validity = None;
            let state = match (stored.mode, stored.state) {
                (LeaseMode::Expiring, LeaseState::Held | LeaseState::Uncertain) => match stored
                    .validity
                    .as_ref()
                    .and_then(|validity| now.restore(validity))
                {
                    Some(until)
                        if stored
                            .duration
                            .is_some_and(|d| plausible_validity(d, until, now.instant)) =>
                    {
                        // The stored deadline itself is kept: converting it
                        // back and forth would move it.
                        valid_until = Some(until);
                        validity.clone_from(&stored.validity);
                        LeaseState::Uncertain
                    }
                    _ => {
                        actions.push(Action::Fence {
                            lease_id: stored.lease_id.clone(),
                            reason: FenceReason::LeaseExpired,
                        });
                        LeaseState::Expired
                    }
                },
                (_, other) => other,
            };
            // A lease that reached a fenced state always starts a fence, and
            // the fence's record is written once its stop finished. A fenced
            // state without that record means the agent died mid-fence.
            let interrupted = match state {
                LeaseState::Revoked => Some(FenceReason::LeaseRevoked),
                LeaseState::Expired => Some(FenceReason::LeaseExpired),
                LeaseState::Held | LeaseState::Uncertain => None,
            };
            if let Some(reason) = interrupted
                && state == stored.state
                && !fenced.contains(&(stored.lease_id.clone(), stored.token))
            {
                actions.push(Action::Fence {
                    lease_id: stored.lease_id.clone(),
                    reason,
                });
            }
            let key = (stored.resource_type, stored.resource_id.clone());
            table
                .highest_token
                .entry(key)
                .and_modify(|seen| *seen = (*seen).max(stored.token))
                .or_insert(stored.token);
            table.leases.insert(
                stored.lease_id.clone(),
                Lease {
                    id: stored.lease_id,
                    resource_type: stored.resource_type,
                    resource_id: stored.resource_id,
                    mode: stored.mode,
                    token: stored.token,
                    state,
                    duration: stored.duration,
                    valid_until,
                    validity,
                    provisional: false,
                    // A lease that came back with its validity renews at once
                    // (section 8.4 "Agent restart").
                    next_renew_at: valid_until.map(|_| now.instant),
                    outstanding: Vec::new(),
                },
            );
        }
        (table, actions)
    }

    /// Applies a `lease_grant` (section 5.12) and the grant rows of the
    /// section 8.4 state table.
    ///
    /// Fencing tokens change only on ownership change (D-0073): a token equal
    /// to the highest seen for the resource is an answer to a renewal for the
    /// lease that already holds it, never a new grant; a token below the
    /// highest is always a bug in the authority.
    ///
    /// A grant that answers no outstanding renewal never sets or extends
    /// `valid_until` (D-0072): the lease becomes `held` only through a grant
    /// that answers one of its renewals.
    ///
    /// On any refusal nothing changes: "the node keeps its current state and
    /// does not start or stop anything" (section 5.12).
    ///
    /// # Errors
    ///
    /// Returns [`Refusal::BadMessage`] for a grant the authority must not send.
    pub fn on_grant(
        &mut self,
        grant: &LeaseGrant,
        answers: Option<&MsgId>,
        now: Instant,
    ) -> Result<(), Refusal> {
        // Section 8.4 floor: `expiring` carries a duration of at least
        // 3 000 ms and at most 600 000 ms; `local` carries none (section
        // 5.12).
        let duration = match grant.mode {
            LeaseMode::Expiring => {
                let Some(ms) = grant.duration_ms else {
                    return Err(Refusal::BadMessage(
                        "expiring grant without duration_ms".to_string(),
                    ));
                };
                let duration = Duration::from_millis(ms);
                if duration < MIN_DURATION {
                    return Err(Refusal::BadMessage(format!(
                        "duration_ms {ms} is below the {MIN_DURATION:?} floor"
                    )));
                }
                if duration > MAX_DURATION {
                    return Err(Refusal::BadMessage(format!(
                        "duration_ms {ms} is above the {MAX_DURATION:?} ceiling"
                    )));
                }
                Some(duration)
            }
            LeaseMode::Local => {
                if grant.duration_ms.is_some() {
                    return Err(Refusal::BadMessage(
                        "local grant with duration_ms".to_string(),
                    ));
                }
                None
            }
        };

        // A lease id may only ever name one resource. Its mode may change
        // (section 8.4 "Changing the mode", ADR 0241).
        if let Some(existing) = self.leases.get(&grant.lease_id)
            && (existing.resource_type != grant.resource_type
                || existing.resource_id != grant.resource_id)
        {
            return Err(Refusal::BadMessage(format!(
                "lease {} already names a different resource",
                grant.lease_id.0
            )));
        }

        // D-0073: compare the token with the highest seen for the resource.
        let key = (grant.resource_type, grant.resource_id.clone());
        let highest = self.highest_token.get(&key).copied().unwrap_or(0);
        if grant.fencing_token < highest {
            return Err(Refusal::BadMessage(format!(
                "fencing_token {} is below {highest} for this resource",
                grant.fencing_token
            )));
        }
        let same_token = grant.fencing_token == highest;
        let existing = self.leases.get(&grant.lease_id).cloned();
        if same_token {
            // D-0073: an equal token is only acceptable for the lease that
            // already holds it, while that lease is still live. An expired or
            // revoked lease, or another lease id, needs a strictly higher one.
            let Some(existing) = &existing else {
                return Err(Refusal::BadMessage(format!(
                    "fencing_token {} equals {highest} for this resource but no live lease by that id holds it",
                    grant.fencing_token
                )));
            };
            if existing.token != grant.fencing_token
                || !matches!(existing.state, LeaseState::Held | LeaseState::Uncertain)
            {
                return Err(Refusal::BadMessage(format!(
                    "fencing_token {} is not higher than {highest} for this resource and the lease is {:?}",
                    grant.fencing_token, existing.state
                )));
            }
        }

        // Section 5.12, ADR 0258: a lease changes mode only with the same
        // token. A higher token with the other mode, over a lease the node
        // holds, would replace it with nothing in between: a `local` lease's
        // start, permitted with a 60 s bound, could still run under the new
        // `expiring` lease, whose fence must stop its VMs within `M`, and an
        // `expiring` lease would lose its deadline without a fence. After
        // `expired` or `revoked`, the node holds nothing, and either mode is
        // a new grant.
        if !same_token
            && let Some(existing) = &existing
            && matches!(existing.state, LeaseState::Held | LeaseState::Uncertain)
            && existing.mode != grant.mode
        {
            return Err(Refusal::BadMessage(format!(
                "fencing_token {} would change lease {} from {} to {}; a lease changes mode only with the same token",
                grant.fencing_token,
                grant.lease_id.0,
                mode_name(existing.mode),
                mode_name(grant.mode)
            )));
        }

        // Section 5.12 and section 8.4 "Changing the mode" (ADR 0241): the
        // same token with the other mode changes the mode of the lease the
        // node holds. It is no ownership change and answers no renewal, so an
        // `expiring` grant that answers one is not a conversion: a late answer
        // to a renewal sent before a `local` grant ended the renewals finds
        // no outstanding renewal below and changes nothing.
        if same_token
            && !(grant.mode == LeaseMode::Expiring && answers.is_some())
            && existing
                .as_ref()
                .is_some_and(|existing| existing.mode != grant.mode)
            && let Some(lease) = self.leases.get_mut(&grant.lease_id)
        {
            // Past its `valid_until` the lease is due to expire at the next
            // poll (section 8.4), and no change of mode revives it.
            if lease.valid_until.is_some_and(|until| until <= now) {
                return Ok(());
            }
            lease.state = LeaseState::Held;
            lease.duration = duration;
            lease.valid_until = None;
            lease.validity = None;
            lease.provisional = false;
            lease.outstanding.clear();
            if duration.is_some() {
                // To `expiring`: the lease stays `held`, its VMs run on, and
                // it renews at once. Its first renewal gives it a provisional
                // deadline ([`Self::renew_sent`]).
                lease.mode = LeaseMode::Expiring;
                lease.next_renew_at = Some(now);
            } else {
                // To `local`: the renewals end, and so does the deadline.
                lease.mode = LeaseMode::Local;
                lease.next_renew_at = None;
            }
            return Ok(());
        }

        match duration {
            // Section 8.3: a local lease has no timing at all.
            None => {
                if same_token {
                    // D-0073: the same token for the same held lease is a
                    // no-op; the lease is already held.
                    return Ok(());
                }
                self.highest_token.insert(key, grant.fencing_token);
                self.leases.insert(
                    grant.lease_id.clone(),
                    Lease {
                        id: grant.lease_id.clone(),
                        resource_type: grant.resource_type,
                        resource_id: grant.resource_id.clone(),
                        mode: grant.mode,
                        token: grant.fencing_token,
                        state: LeaseState::Held,
                        duration: None,
                        valid_until: None,
                        validity: None,
                        provisional: false,
                        next_renew_at: None,
                        outstanding: Vec::new(),
                    },
                );
            }
            Some(d) => {
                if same_token {
                    // D-0072: only a grant that answers an outstanding renewal
                    // moves the timing; anything else changes nothing.
                    let answered = existing.as_ref().and_then(|existing| {
                        answers.and_then(|id| {
                            existing
                                .outstanding
                                .iter()
                                .find(|renewal| renewal.msg == *id)
                                .cloned()
                        })
                    });
                    let Some(Renewal {
                        sent: t_send,
                        sent_boottime,
                        ..
                    }) = answered
                    else {
                        return Ok(());
                    };
                    // An answer that arrives once its own `valid_until` has
                    // passed proves nothing still valid: it changes nothing,
                    // and the renewals go on. A `valid_until` an earlier
                    // answer set stands, as that answer's gate does. A
                    // provisional deadline has no gate behind it (ADR 0241),
                    // and this answer may carry a shorter duration than the
                    // conversion did: the lease takes the answer's deadline,
                    // already past, and expires at the next poll.
                    let valid_until = t_send + (d - margin(d));
                    if valid_until <= now {
                        if let Some(lease) = self.leases.get_mut(&grant.lease_id)
                            && lease.provisional
                            && lease.valid_until.is_some_and(|until| until > valid_until)
                        {
                            lease.valid_until = Some(valid_until);
                            lease.validity = sent_boottime.map(|at| at.later_by(d - margin(d)));
                        }
                        return Ok(());
                    }
                    // Section 8.4: `valid_until = t_send + D - M`, renew every
                    // `D / 3`. Renewals sent at or before `t_send` are
                    // answered by this grant.
                    let outstanding: Vec<Renewal> = existing
                        .as_ref()
                        .map(|existing| {
                            existing
                                .outstanding
                                .iter()
                                .filter(|renewal| renewal.sent > t_send)
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default();
                    if let Some(lease) = self.leases.get_mut(&grant.lease_id) {
                        lease.state = LeaseState::Held;
                        lease.duration = Some(d);
                        lease.valid_until = Some(valid_until);
                        // ADR 0240: the stored deadline comes from the
                        // reading made when the renewal was sent, so a
                        // suspend before the answer cannot make it later,
                        // and from this answer's duration, like
                        // `valid_until`.
                        lease.validity = sent_boottime.map(|at| at.later_by(d - margin(d)));
                        lease.provisional = false;
                        lease.next_renew_at = Some(t_send + d / 3);
                        lease.outstanding = outstanding;
                    }
                    return Ok(());
                }

                // D-0072, D-0073: a higher token is a new ownership. The node
                // cannot prove the new grant until a renewal is answered, so
                // the lease is `uncertain` and renews immediately. It never
                // extends the validity the previous ownership had.
                let (valid_until, validity, provisional) = existing
                    .as_ref()
                    .filter(|existing| {
                        matches!(existing.state, LeaseState::Held | LeaseState::Uncertain)
                    })
                    .map_or((None, None, false), |existing| {
                        (
                            existing.valid_until,
                            existing.validity.clone(),
                            existing.provisional,
                        )
                    });
                self.highest_token.insert(key, grant.fencing_token);
                self.leases.insert(
                    grant.lease_id.clone(),
                    Lease {
                        id: grant.lease_id.clone(),
                        resource_type: grant.resource_type,
                        resource_id: grant.resource_id.clone(),
                        mode: grant.mode,
                        token: grant.fencing_token,
                        state: LeaseState::Uncertain,
                        duration: Some(d),
                        valid_until,
                        validity,
                        provisional,
                        next_renew_at: Some(now),
                        outstanding: Vec::new(),
                    },
                );
            }
        }
        Ok(())
    }

    /// Applies a `lease_revoke` (section 5.13).
    ///
    /// # Errors
    ///
    /// Returns [`Refusal::BadMessage`] when the revoke's token is below the
    /// node's current token; nothing changes in that case.
    pub fn on_revoke(&mut self, revoke: &LeaseRevoke) -> Result<RevokeOutcome, Refusal> {
        let Some(lease) = self.leases.get_mut(&revoke.lease_id) else {
            return Ok(RevokeOutcome::UnknownLease);
        };
        // Section 5.13: the revoke's token must be at least the node's.
        if revoke.fencing_token < lease.token {
            return Err(Refusal::BadMessage(format!(
                "revoke token {} is below the current token {}",
                revoke.fencing_token, lease.token
            )));
        }
        // Re-fencing is idempotent: stopping a stopped VM is fine.
        lease.state = LeaseState::Revoked;
        lease.token = lease.token.max(revoke.fencing_token);
        lease.valid_until = None;
        lease.validity = None;
        lease.next_renew_at = None;
        lease.outstanding.clear();
        let token = lease.token;
        let key = (lease.resource_type, lease.resource_id.clone());
        self.highest_token
            .entry(key)
            .and_modify(|seen| *seen = (*seen).max(token))
            .or_insert(token);
        Ok(RevokeOutcome::Fence {
            lease_id: revoke.lease_id.clone(),
        })
    }

    /// Records that a renewal for `lease_id` was just sent: `sent` is the
    /// reading taken just before it was handed to the session. Its instant is
    /// `t_send`, and its `CLOCK_BOOTTIME` gives the deadline an answer stores
    /// (section 8.4 "Agent restart", ADR 0240).
    ///
    /// Returns whether the renewal gave a converted lease its provisional
    /// deadline (ADR 0241): the caller stores the lease then, so an agent
    /// restart keeps that deadline.
    pub fn renew_sent(&mut self, lease_id: &LeaseId, msg: MsgId, sent: &HostNow) -> bool {
        let now = sent.instant;
        let Some(lease) = self.leases.get_mut(lease_id) else {
            return false;
        };
        if lease.mode != LeaseMode::Expiring
            || !matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
        {
            return false;
        }
        let Some(d) = lease.duration else {
            return false;
        };
        let deadline = now + (d - margin(d));
        let sent_boottime = sent.store(now);
        // ADR 0241: the first renewal of a converted lease gives it a
        // provisional deadline, the `valid_until` an answer to it would set.
        // Any later answer sets one no earlier, so the deadline moves later
        // only on proof from the authority.
        let provisional = lease.awaits_first_renewal();
        if provisional {
            lease.valid_until = Some(deadline);
            lease.validity = sent_boottime.as_ref().map(|at| at.later_by(d - margin(d)));
            lease.provisional = true;
        }
        prune_outstanding(&mut lease.outstanding, d, now);
        lease.outstanding.push(Renewal {
            msg,
            sent: now,
            sent_boottime,
        });
        lease.next_renew_at = Some(now + d / 3);
        provisional
    }

    /// Takes back a renewal [`Self::renew_sent`] recorded that never reached
    /// the session. Its answer can never come, and a converted lease whose
    /// first renewal it was gets its provisional deadline taken back with it
    /// (`provisional`, as `renew_sent` returned it): until a first renewal is
    /// sent, the lease behaves as `local` (ADR 0241).
    pub fn renewal_not_sent(&mut self, lease_id: &LeaseId, msg: &MsgId, provisional: bool) {
        let Some(lease) = self.leases.get_mut(lease_id) else {
            return;
        };
        lease.outstanding.retain(|renewal| renewal.msg != *msg);
        if provisional && lease.outstanding.is_empty() {
            lease.valid_until = None;
            lease.validity = None;
            lease.provisional = false;
        }
    }

    /// The authority is unreachable (section 8.4): every expiring lease that
    /// was held becomes uncertain, except a converted lease whose first
    /// renewal was not sent yet. Local leases never change for this reason
    /// (section 8.3).
    pub fn authority_unreachable(&mut self, _now: Instant) {
        for lease in self.leases.values_mut() {
            // A converted lease whose first renewal was not sent stays
            // `held`: no gate exists for it yet, so the app runs on (ADR
            // 0241).
            if lease.mode == LeaseMode::Expiring
                && lease.state == LeaseState::Held
                && !lease.awaits_first_renewal()
            {
                lease.state = LeaseState::Uncertain;
            }
        }
    }

    /// The authority is reachable again: every `expiring` lease that is `held`
    /// or `uncertain` renews at `now` instead of at its next renewal interval,
    /// so a lease that went `uncertain` while the session was down returns to
    /// `held` as soon as the authority answers (section 8.4). Nothing else
    /// changes: only an answered renewal moves `valid_until`.
    pub fn renew_now(&mut self, now: Instant) {
        for lease in self.leases.values_mut() {
            if lease.mode == LeaseMode::Expiring
                && matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
                && lease.duration.is_some()
            {
                lease.next_renew_at = Some(now);
            }
        }
    }

    /// Advances the state machine to `now` and returns what the caller must do
    /// (section 8.4 rows for the monotonic clock and the renewals).
    #[must_use]
    pub fn poll(&mut self, now: Instant) -> Vec<Action> {
        let mut ids: Vec<LeaseId> = self.leases.keys().cloned().collect();
        ids.sort_by(|a, b| a.0.cmp(&b.0));

        let mut actions = Vec::new();
        for id in ids {
            let Some(lease) = self.leases.get_mut(&id) else {
                continue;
            };
            if lease.mode != LeaseMode::Expiring
                || !matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
            {
                continue;
            }
            let Some(d) = lease.duration else {
                continue;
            };

            // 1. Section 8.4: the clock reached `valid_until`; self-fence.
            if lease.valid_until.is_some_and(|until| now >= until) {
                lease.state = LeaseState::Expired;
                lease.valid_until = None;
                lease.validity = None;
                lease.next_renew_at = None;
                lease.outstanding.clear();
                actions.push(Action::Fence {
                    lease_id: id,
                    reason: FenceReason::LeaseExpired,
                });
                continue;
            }

            // 2. A renewal unanswered for a whole renewal interval is late.
            if lease.state == LeaseState::Held
                && lease
                    .outstanding
                    .iter()
                    .any(|renewal| now >= renewal.sent + d / 3)
            {
                lease.state = LeaseState::Uncertain;
            }
            prune_outstanding(&mut lease.outstanding, d, now);

            // 3. It is time to renew. `next_renew_at` moves here too, so a
            // caller that forgets `renew_sent` cannot make us send twice.
            if lease.next_renew_at.is_some_and(|at| now >= at) {
                actions.push(Action::SendRenew {
                    lease_id: id.clone(),
                    fencing_token: lease.token,
                });
                lease.next_renew_at = Some(now + d / 3);
            }
        }
        actions
    }

    /// The earliest instant strictly after `now` at which [`Self::poll`] has
    /// something to do, so the driver can sleep exactly that long.
    ///
    /// Instants at or before `now` are not reported: the caller polls at `now`
    /// before it waits, which handles them, and a renewal already late stays
    /// late, so reporting it would wake the driver again and again. The caller
    /// passes the `now` it polled at, so an instant reached since is reported
    /// and its wait ends at once.
    #[must_use]
    pub fn next_deadline(&self, now: Instant) -> Option<Instant> {
        let mut earliest: Option<Instant> = None;
        for lease in self.leases.values() {
            if lease.mode != LeaseMode::Expiring
                || !matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
            {
                continue;
            }
            let Some(d) = lease.duration else {
                continue;
            };
            let mut candidates: Vec<Instant> = Vec::new();
            candidates.extend(lease.valid_until);
            candidates.extend(lease.next_renew_at);
            candidates.extend(lease.outstanding.iter().map(|renewal| renewal.sent + d / 3));
            for candidate in candidates {
                if candidate <= now {
                    continue;
                }
                earliest = Some(earliest.map_or(candidate, |at| at.min(candidate)));
            }
        }
        earliest
    }

    /// Section 6.4: a stateful VM may start only while the node holds the
    /// lease and the lease's token equals the token in the VM spec. The token
    /// must also be the highest seen for the resource, so a lease id that a
    /// newer ownership superseded can never start anything (D-0073).
    ///
    /// An `expiring` lease must also have time left for a start (section 6.4,
    /// ADR 0242): `valid_until - now >= start bound + M`. The engine gives a
    /// start up once the start bound passes, so a start that took its permit
    /// is over at least `M` before `valid_until`, and the fence never waits
    /// for it past its stop budget. That also keeps out the gap between
    /// `valid_until` and the next [`Self::poll`], while the lease is still
    /// `held` (section 8.4). A converted lease's provisional deadline stands in
    /// for `valid_until`; before its first renewal it has none and starts
    /// nothing, while its VMs that already run keep running (ADR 0241). A
    /// lease granted for 5 000 ms or less never has the time left.
    #[must_use]
    pub fn may_start_stateful(&self, lease_id: &LeaseId, spec_token: u64, now: Instant) -> bool {
        self.leases.get(lease_id).is_some_and(|lease| {
            lease.state == LeaseState::Held
                && match lease.mode {
                    LeaseMode::Local => true,
                    LeaseMode::Expiring => {
                        lease
                            .valid_until
                            .zip(lease.duration)
                            .is_some_and(|(until, d)| {
                                until.checked_duration_since(now).is_some_and(|left| {
                                    left >= crate::EXPIRING_START_WITHIN + margin(d)
                                })
                            })
                    }
                }
                && lease.token == spec_token
                && lease.token == self.highest_for(lease.resource_type, &lease.resource_id)
        })
    }

    /// Section 8.4: whether a stateful VM with this spec token that is
    /// already running may keep running. True while the node holds the lease
    /// `held` or `uncertain` with exactly that token, the highest seen for
    /// the resource: an `uncertain` lease keeps its VMs and starts nothing new.
    /// Only an `uncertain` lease with a deadline keeps its VMs, so the timer
    /// fences them at that deadline; one with none (a first grant that was
    /// never confirmed) never had VMs of its own.
    #[must_use]
    pub fn may_keep_running(&self, lease_id: &LeaseId, spec_token: u64) -> bool {
        self.leases.get(lease_id).is_some_and(|lease| {
            (lease.state == LeaseState::Held
                || (lease.state == LeaseState::Uncertain && lease.valid_until.is_some()))
                && lease.token == spec_token
                && lease.token == self.highest_for(lease.resource_type, &lease.resource_id)
        })
    }

    /// The lease with this id, if the node knows it.
    #[must_use]
    pub fn get(&self, lease_id: &LeaseId) -> Option<&Lease> {
        self.leases.get(lease_id)
    }

    /// Every lease the node tracks.
    pub fn leases(&self) -> impl Iterator<Item = &Lease> {
        self.leases.values()
    }

    /// The highest token seen per resource (section 8.6).
    pub fn highest_tokens(&self) -> impl Iterator<Item = (&(ResourceType, String), &u64)> {
        self.highest_token.iter()
    }

    /// The highest token seen for one resource, or `0` when it was never seen.
    #[must_use]
    pub fn highest_for(&self, resource_type: ResourceType, resource_id: &str) -> u64 {
        self.highest_token
            .get(&(resource_type, resource_id.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

impl Default for LeaseTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Drops the renewals whose answer could no longer set a `valid_until` in the
/// future (`t_send + D - M <= now`), so a renewal the authority never answers
/// is not kept forever.
fn prune_outstanding(outstanding: &mut Vec<Renewal>, d: Duration, now: Instant) {
    let keep = d - margin(d);
    outstanding.retain(|renewal| renewal.sent + keep > now);
}

/// Whether a restored `valid_until` can come from a grant of duration `d`:
/// `d` within the bounds of section 5.12, and no more than `D - M` left
/// (section 8.4). Anything else is a damaged row, and the lease is fenced.
fn plausible_validity(d: Duration, until: Instant, now: Instant) -> bool {
    (MIN_DURATION..=MAX_DURATION).contains(&d)
        && until.saturating_duration_since(now) <= d.saturating_sub(margin(d))
}

/// The wire name of a lease mode (section 5.12).
fn mode_name(mode: LeaseMode) -> &'static str {
    match mode {
        LeaseMode::Local => "local",
        LeaseMode::Expiring => "expiring",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use quoro_proto::{
        FenceReason, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, MsgId, ResourceType, RevokeReason,
    };
    use tokio::time::Instant;

    use super::{Action, LeaseState, LeaseTable, MIN_DURATION, Refusal, RevokeOutcome, margin};
    use crate::lease::clock::{HostNow, ManualHostClock, StoredValidity};
    use crate::lease::store::StoredLease;

    fn millis(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    fn expiring(lease: &str, token: u64, duration_ms: u64) -> LeaseGrant {
        LeaseGrant {
            lease_id: LeaseId(lease.to_string()),
            resource_type: ResourceType::App,
            resource_id: "app_test".to_string(),
            mode: LeaseMode::Expiring,
            fencing_token: token,
            duration_ms: Some(duration_ms),
            granted_at: "2026-11-02T10:15:33Z".to_string(),
        }
    }

    fn local(lease: &str, token: u64) -> LeaseGrant {
        LeaseGrant {
            lease_id: LeaseId(lease.to_string()),
            resource_type: ResourceType::App,
            resource_id: "app_test".to_string(),
            mode: LeaseMode::Local,
            fencing_token: token,
            duration_ms: None,
            granted_at: "2026-11-02T10:15:33Z".to_string(),
        }
    }

    fn local_on(lease: &str, resource: &str, token: u64) -> LeaseGrant {
        let mut grant = local(lease, token);
        grant.resource_id = resource.to_string();
        grant
    }

    fn revoke(lease: &str, token: u64) -> LeaseRevoke {
        LeaseRevoke {
            lease_id: LeaseId(lease.to_string()),
            fencing_token: token,
            reason: quoro_proto::RevokeReason::Moved,
        }
    }

    /// Answers the immediate renewal a first `expiring` grant produces, with
    /// the same token, as the authority does (D-0072). Returns the renewal id.
    ///
    /// After this the lease is `held` with `valid_until = now + D - M`.
    fn confirm(table: &mut LeaseTable, grant: &LeaseGrant, now: Instant) -> MsgId {
        confirm_with(table, grant, &HostNow::unknown(now))
    }

    /// [`confirm`], with the renewal sent at `reading`, so the answer stores
    /// a validity when the reading has `CLOCK_BOOTTIME`.
    fn confirm_with(table: &mut LeaseTable, grant: &LeaseGrant, reading: &HostNow) -> MsgId {
        let now = reading.instant;
        let sent = table
            .poll(now)
            .into_iter()
            .find_map(|action| match action {
                Action::SendRenew {
                    lease_id,
                    fencing_token,
                } if lease_id == grant.lease_id => Some(fencing_token),
                _ => None,
            })
            .expect("a first expiring grant asks for a renewal at once");
        assert_eq!(sent, grant.fencing_token, "the renewal echoes the token");
        let id = MsgId(format!("msg_renew_{}", grant.lease_id.0));
        table.renew_sent(&grant.lease_id, id.clone(), reading);
        table
            .on_grant(grant, Some(&id), now)
            .expect("the answering grant");
        id
    }

    #[test]
    fn a_superseded_lease_id_cannot_start_stateful() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_old", 1), None, now)
            .expect("grant");
        assert!(table.may_start_stateful(&LeaseId("lse_old".to_string()), 1, Instant::now()));
        table
            .on_grant(&local("lse_new", 2), None, now)
            .expect("grant");
        assert!(!table.may_start_stateful(&LeaseId("lse_old".to_string()), 1, Instant::now()));
        assert!(table.may_start_stateful(&LeaseId("lse_new".to_string()), 2, Instant::now()));
    }

    #[test]
    fn margin_matches_section_8_4() {
        assert_eq!(margin(millis(3000)), millis(1000));
        assert_eq!(margin(millis(15000)), millis(1500));
        assert_eq!(margin(millis(30000)), millis(3000));
    }

    /// Section 8.4: an `expiring` lease's fence fits in `M` with the agent's
    /// default timers (`T_grace + T_force <= M`, the worked example's
    /// 1 000 + 300 ms), from the shortest `D` a grant may carry to the
    /// longest; a `local` lease keeps its own timers.
    #[test]
    fn the_expiring_fence_budget_fits_in_the_margin() {
        use crate::lease::fence::budget_for;
        use crate::lease::service::LeaseConfig;

        let timers = LeaseConfig::EXPIRING_TIMERS;
        for d in [3000, 6000, 15_000, 30_000, 600_000] {
            let now = Instant::now();
            let mut table = LeaseTable::new();
            table
                .on_grant(&expiring("lse_a", 1, d), None, now)
                .expect("grant");
            let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
            let budget = budget_for(lease, timers);
            assert!(budget.grace + budget.force <= margin(millis(d)), "D = {d}");
            if margin(millis(d)) >= timers.grace + timers.force {
                assert_eq!(budget, timers, "D = {d}: nothing to clamp");
            }
        }
        // `D` = 3 000 leaves `M` = 1 000: the forced stop keeps its 300 ms and
        // the graceful one gets the rest.
        let mut table = LeaseTable::new();
        table
            .on_grant(&expiring("lse_a", 1, 3000), None, Instant::now())
            .expect("grant");
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        let budget = budget_for(lease, timers);
        assert_eq!((budget.grace, budget.force), (millis(700), millis(300)));

        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_b", 1), None, Instant::now())
            .expect("grant");
        let local_timers = crate::lease::fence::FenceTimers {
            grace: Duration::from_secs(10),
            force: Duration::from_secs(5),
        };
        let lease = table.get(&LeaseId("lse_b".to_string())).expect("lease");
        assert_eq!(budget_for(lease, local_timers), local_timers);
    }

    #[test]
    fn a_grant_holds_a_local_lease_without_timing() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_a", 1), None, now)
            .expect("grant");

        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.token, 1);
        assert_eq!(lease.duration, None);
        assert_eq!(lease.valid_until, None);
        assert_eq!(table.next_deadline(now), None);
        assert!(table.may_start_stateful(&LeaseId("lse_a".to_string()), 1, Instant::now()));
        assert!(!table.may_start_stateful(&LeaseId("lse_a".to_string()), 2, Instant::now()));
    }

    #[test]
    fn a_grant_holds_an_expiring_lease_with_the_section_8_4_timing() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, now).expect("grant");
        confirm(&mut table, &grant, now);

        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(now + millis(13500)));
        assert_eq!(table.next_deadline(now), Some(now + millis(5000)));
    }

    #[test]
    fn a_grant_below_the_duration_floor_is_refused() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        assert!(
            table
                .on_grant(&expiring("lse_e", 1, 2999), None, now)
                .is_err()
        );
        assert!(table.leases().next().is_none());
        assert!(matches!(
            table.on_grant(&expiring("lse_e", 1, 3000), None, now),
            Ok(())
        ));
        assert_eq!(MIN_DURATION, millis(3000));
    }

    #[test]
    fn a_local_grant_with_a_duration_is_refused() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let mut grant = local("lse_a", 1);
        grant.duration_ms = Some(3000);
        assert!(matches!(
            table.on_grant(&grant, None, now),
            Err(Refusal::BadMessage(_))
        ));
        assert!(table.leases().next().is_none());
    }

    #[test]
    fn an_expiring_grant_without_a_duration_is_refused() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let mut grant = expiring("lse_e", 1, 15000);
        grant.duration_ms = None;
        assert!(matches!(
            table.on_grant(&grant, None, now),
            Err(Refusal::BadMessage(_))
        ));
    }

    #[test]
    fn a_stale_token_grant_is_refused_and_changes_nothing() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_a", 5), None, now)
            .expect("grant");
        // D-0073: the same token for the same held lease is a no-op, not a
        // new grant, so it is accepted and changes nothing.
        assert!(matches!(
            table.on_grant(&local("lse_a", 5), None, now),
            Ok(())
        ));
        // A lower token is always a bug in the authority.
        assert!(matches!(
            table.on_grant(&local("lse_a", 4), None, now),
            Err(Refusal::BadMessage(_))
        ));
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(lease.token, 5);
        assert_eq!(lease.state, LeaseState::Held);
        // Once the lease is revoked the same token is refused: only a higher
        // token re-grants.
        table.on_revoke(&revoke("lse_a", 5)).expect("revoke");
        assert!(matches!(
            table.on_grant(&local("lse_a", 5), None, now),
            Err(Refusal::BadMessage(_))
        ));
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Revoked);
        assert_eq!(lease.token, 5);
    }

    #[test]
    fn a_lease_id_cannot_change_resource() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_a", 1), None, now)
            .expect("grant");
        for token in [1, 2] {
            let mut other_resource = local("lse_a", token);
            other_resource.resource_id = "app_other".to_string();
            assert!(matches!(
                table.on_grant(&other_resource, None, now),
                Err(Refusal::BadMessage(_))
            ));
        }
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(lease.resource_id, "app_test");
        assert_eq!(lease.token, 1);
    }

    /// Section 8.4 "Changing the mode" (ADR 0241): a same-token `expiring`
    /// grant converts a held `local` lease. It stays `held` and keeps its
    /// VMs, but starts nothing until its first renewal is sent, which it is
    /// at once. That renewal gives it a provisional deadline,
    /// `t_send + D - M`, and the clock fences it there when no answer comes.
    #[test]
    fn a_same_token_expiring_grant_converts_a_local_lease() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        let id = LeaseId("lse_a".to_string());
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("the conversion is taken");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.mode, LeaseMode::Expiring);
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.token, 1, "no ownership change");
        assert_eq!(lease.valid_until, None, "the conversion answers no renewal");
        assert!(lease.awaits_first_renewal());
        assert!(table.may_keep_running(&id, 1), "its VMs run on");
        assert!(!table.may_start_stateful(&id, 1, t0));
        assert!(table.has_timed_leases());

        let t_send = t0 + millis(10);
        assert_eq!(
            table.poll(t_send),
            vec![Action::SendRenew {
                lease_id: id.clone(),
                fencing_token: 1,
            }],
            "renews at once"
        );
        assert!(
            table.renew_sent(
                &id,
                MsgId("msg_first".to_string()),
                &HostNow::unknown(t_send)
            ),
            "the first renewal sets the provisional deadline"
        );
        let deadline = t_send + millis(13_500);
        assert_eq!(table.get(&id).expect("lease").valid_until, Some(deadline));
        assert!(!table.get(&id).expect("lease").awaits_first_renewal());
        assert!(table.may_start_stateful(&id, 1, t_send));

        // A later renewal leaves the provisional deadline where it is.
        let later = t_send + millis(5000);
        assert!(matches!(
            table.poll(later).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(!table.renew_sent(
            &id,
            MsgId("msg_second".to_string()),
            &HostNow::unknown(later)
        ));
        assert_eq!(table.get(&id).expect("lease").valid_until, Some(deadline));

        assert!(
            !table
                .poll(deadline - millis(1))
                .iter()
                .any(|action| matches!(action, Action::Fence { .. }))
        );
        assert!(table.poll(deadline).contains(&Action::Fence {
            lease_id: id.clone(),
            reason: FenceReason::LeaseExpired,
        }));
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Expired);
    }

    /// ADR 0241: an answer computes `valid_until` from the send time of the
    /// renewal it answers, so after a conversion the deadline moves later
    /// only when the authority answers a later renewal.
    #[test]
    fn an_answer_moves_a_converted_lease_s_deadline_from_its_renewal() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        let grant = expiring("lse_a", 1, 15000);
        table.on_grant(&grant, None, t0).expect("conversion");
        let id = LeaseId("lse_a".to_string());
        let first = MsgId("msg_first".to_string());
        assert!(matches!(
            table.poll(t0).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(&id, first.clone(), &HostNow::unknown(t0)));

        table
            .on_grant(&grant, Some(&first), t0 + millis(40))
            .expect("the answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(
            lease.valid_until,
            Some(t0 + millis(13_500)),
            "the same deadline"
        );

        let later = t0 + millis(5000);
        let second = MsgId("msg_second".to_string());
        assert!(matches!(
            table.poll(later).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(!table.renew_sent(&id, second.clone(), &HostNow::unknown(later)));
        table
            .on_grant(&grant, Some(&second), later + millis(40))
            .expect("the second answer");
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(later + millis(13_500))
        );
    }

    /// ADR 0241: a late answer to a converted lease's renewal, one that
    /// arrives once its own deadline has passed, may carry a shorter duration
    /// than the conversion did. No gate backs the provisional deadline, so
    /// the lease takes the answer's deadline and expires at the next poll. A
    /// deadline an answer set stands through a later late answer.
    #[test]
    fn a_late_answer_ends_a_provisional_deadline_but_not_an_answered_one() {
        let t0 = Instant::now();
        let id = LeaseId("lse_a".to_string());
        let first = MsgId("msg_first".to_string());
        let converted = |at: Instant| {
            let mut table = LeaseTable::new();
            table.on_grant(&local("lse_a", 1), None, at).expect("grant");
            table
                .on_grant(&expiring("lse_a", 1, 15000), None, at)
                .expect("conversion");
            assert!(matches!(
                table.poll(at).as_slice(),
                [Action::SendRenew { .. }]
            ));
            assert!(table.renew_sent(&id, first.clone(), &HostNow::unknown(at)));
            table
        };

        let mut table = converted(t0);
        let late = t0 + millis(2500);
        table
            .on_grant(&expiring("lse_a", 1, 3000), Some(&first), late)
            .expect("the late answer");
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(2000)),
            "the answer's deadline, already past"
        );
        assert!(table.poll(late).contains(&Action::Fence {
            lease_id: id.clone(),
            reason: FenceReason::LeaseExpired,
        }));

        let mut table = converted(t0);
        table
            .on_grant(&expiring("lse_a", 1, 15000), Some(&first), t0 + millis(40))
            .expect("the answer");
        let later = t0 + millis(5000);
        let second = MsgId("msg_second".to_string());
        assert!(matches!(
            table.poll(later).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(!table.renew_sent(&id, second.clone(), &HostNow::unknown(later)));
        table
            .on_grant(
                &expiring("lse_a", 1, 3000),
                Some(&second),
                later + millis(2500),
            )
            .expect("the late answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.valid_until, Some(t0 + millis(13_500)), "it stands");
        assert_eq!(lease.state, LeaseState::Held);
    }

    /// ADR 0241: until its first renewal is sent, a lost session leaves a
    /// converted lease `held`, because no gate exists for it yet. Once that
    /// renewal is out, a lost session makes it `uncertain` like any other
    /// `expiring` lease, and it keeps its VMs until its deadline.
    #[test]
    fn a_converted_lease_stays_held_through_a_lost_session_until_its_first_renewal() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("conversion");
        let id = LeaseId("lse_a".to_string());

        table.authority_unreachable(t0);
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Held);
        assert!(table.may_keep_running(&id, 1));
        // With no session, nothing is sent: the lease stays as it was, and no
        // clock fences it.
        let _ = table.poll(t0 + millis(60_000));
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Held);
        assert_eq!(table.get(&id).expect("lease").valid_until, None);

        let t_send = t0 + millis(60_000);
        table.renew_now(t_send);
        assert!(matches!(
            table.poll(t_send).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(
            &id,
            MsgId("msg_first".to_string()),
            &HostNow::unknown(t_send)
        ));
        table.authority_unreachable(t_send);
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert!(table.may_keep_running(&id, 1), "until its deadline");
        assert!(!table.may_start_stateful(&id, 1, t_send));
        assert!(
            table
                .poll(t_send + millis(13_500))
                .contains(&Action::Fence {
                    lease_id: id.clone(),
                    reason: FenceReason::LeaseExpired,
                })
        );
    }

    /// Section 8.4 "Changing the mode": a same-token `local` grant ends the
    /// renewals of an `expiring` lease. The node drops `valid_until` and
    /// keeps the lease `held` as `local`, from `held` or `uncertain`.
    #[test]
    fn a_same_token_local_grant_converts_an_expiring_lease() {
        for unreachable in [false, true] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            let grant = expiring("lse_e", 1, 15000);
            table.on_grant(&grant, None, t0).expect("grant");
            confirm(&mut table, &grant, t0);
            let id = LeaseId("lse_e".to_string());
            let later = t0 + millis(5000);
            assert!(matches!(
                table.poll(later).as_slice(),
                [Action::SendRenew { .. }]
            ));
            table.renew_sent(&id, MsgId("msg_out".to_string()), &HostNow::unknown(later));
            if unreachable {
                table.authority_unreachable(later);
                assert_eq!(table.get(&id).expect("lease").state, LeaseState::Uncertain);
            }

            table
                .on_grant(&local("lse_e", 1), None, later)
                .expect("the conversion is taken");
            let lease = table.get(&id).expect("lease");
            assert_eq!(lease.mode, LeaseMode::Local);
            assert_eq!(lease.state, LeaseState::Held);
            assert_eq!(lease.token, 1);
            assert_eq!(lease.duration, None);
            assert_eq!(lease.valid_until, None);
            assert_eq!(lease.validity(), None);
            assert!(!table.has_timed_leases());
            assert_eq!(
                table.next_deadline(later),
                None,
                "no renewal and no deadline"
            );
            assert!(table.poll(t0 + millis(60_000)).is_empty());
            assert!(table.may_start_stateful(&id, 1, t0 + millis(60_000)));

            // A late answer to the renewal sent before is an answer, not a
            // conversion: it answers no renewal the lease still has, so it
            // changes nothing.
            table
                .on_grant(&grant, Some(&MsgId("msg_out".to_string())), later)
                .expect("taken");
            let lease = table.get(&id).expect("lease");
            assert_eq!(lease.mode, LeaseMode::Local);
            assert_eq!(lease.valid_until, None);
            assert!(table.poll(later).is_empty(), "no renewal");
        }
    }

    /// Section 8.4: a lease past its `valid_until` expires at the next poll,
    /// and a change of mode that arrives first does not revive it.
    #[test]
    fn a_mode_change_past_valid_until_changes_nothing() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let id = LeaseId("lse_e".to_string());
        let until = t0 + millis(13_500);
        table
            .on_grant(&local("lse_e", 1), None, until)
            .expect("taken");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.mode, LeaseMode::Expiring);
        assert_eq!(lease.valid_until, Some(until));
        assert!(table.poll(until).contains(&Action::Fence {
            lease_id: id.clone(),
            reason: FenceReason::LeaseExpired,
        }));
    }

    /// ADR 0241: a revoke fences a converted lease like any other, before
    /// its first renewal and after.
    #[test]
    fn a_revoke_fences_a_converted_lease() {
        for after_first_renewal in [false, true] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
            table
                .on_grant(&expiring("lse_a", 1, 15000), None, t0)
                .expect("conversion");
            let id = LeaseId("lse_a".to_string());
            if after_first_renewal {
                assert!(matches!(
                    table.poll(t0).as_slice(),
                    [Action::SendRenew { .. }]
                ));
                assert!(table.renew_sent(
                    &id,
                    MsgId("msg_first".to_string()),
                    &HostNow::unknown(t0)
                ));
            }
            let outcome = table
                .on_revoke(&LeaseRevoke {
                    lease_id: id.clone(),
                    fencing_token: 1,
                    reason: RevokeReason::Moved,
                })
                .expect("revoke");
            assert_eq!(
                outcome,
                RevokeOutcome::Fence {
                    lease_id: id.clone()
                }
            );
            let lease = table.get(&id).expect("lease");
            assert_eq!(lease.state, LeaseState::Revoked);
            assert!(!table.may_keep_running(&id, 1));
            assert!(table.poll(t0 + millis(60_000)).is_empty());
        }
    }

    /// A higher token over a converted lease is a new ownership (section
    /// 8.4): the lease keeps the deadline it has, if any. Before its first
    /// renewal it has none, so it keeps no VMs.
    #[test]
    fn a_higher_token_over_a_converted_lease_keeps_only_its_deadline() {
        for after_first_renewal in [false, true] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
            table
                .on_grant(&expiring("lse_a", 1, 15000), None, t0)
                .expect("conversion");
            let id = LeaseId("lse_a".to_string());
            if after_first_renewal {
                assert!(matches!(
                    table.poll(t0).as_slice(),
                    [Action::SendRenew { .. }]
                ));
                assert!(table.renew_sent(
                    &id,
                    MsgId("msg_first".to_string()),
                    &HostNow::unknown(t0)
                ));
            }
            table
                .on_grant(&expiring("lse_a", 2, 15000), None, t0)
                .expect("a new ownership");
            let lease = table.get(&id).expect("lease");
            assert_eq!(lease.state, LeaseState::Uncertain);
            assert_eq!(
                lease.valid_until,
                after_first_renewal.then(|| t0 + millis(13_500))
            );
            assert_eq!(table.may_keep_running(&id, 2), after_first_renewal);
        }
    }

    /// ADR 0244: the primary may send the conversion again. A lease already
    /// `expiring` takes it as a grant that answers no renewal, which changes
    /// nothing, so the provisional deadline stays where it is.
    #[test]
    fn a_resent_conversion_changes_nothing() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        let conversion = expiring("lse_a", 1, 15000);
        table.on_grant(&conversion, None, t0).expect("conversion");
        let id = LeaseId("lse_a".to_string());
        assert!(matches!(
            table.poll(t0).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(&id, MsgId("msg_first".to_string()), &HostNow::unknown(t0)));
        let before = table.get(&id).expect("lease").clone();
        table
            .on_grant(&conversion, None, t0 + millis(1000))
            .expect("taken");
        assert_eq!(table.get(&id).expect("lease"), &before);
    }

    /// A renewal that never reached the session is taken back. For a
    /// converted lease's first renewal its provisional deadline goes too, so
    /// the lease behaves as `local` again until a renewal is sent (ADR 0241).
    #[test]
    fn a_renewal_not_sent_takes_back_the_provisional_deadline() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("conversion");
        let id = LeaseId("lse_a".to_string());
        let msg = MsgId("msg_first".to_string());
        assert!(matches!(
            table.poll(t0).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(&id, msg.clone(), &HostNow::unknown(t0)));
        table.renewal_not_sent(&id, &msg, true);
        let lease = table.get(&id).expect("lease");
        assert!(lease.awaits_first_renewal());
        assert_eq!(lease.validity(), None);
        table.authority_unreachable(t0);
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Held);
        // Its answer, should one come, answers nothing.
        table
            .on_grant(&expiring("lse_a", 1, 15000), Some(&msg), t0)
            .expect("taken");
        assert_eq!(table.get(&id).expect("lease").valid_until, None);
    }

    /// ADR 0240: the stored deadline is `CLOCK_BOOTTIME(t_send)` plus the
    /// answer's own `D - M`, like `valid_until`, also when the answer carries
    /// another duration than the lease had when the renewal went out (the end
    /// of a maintenance window).
    #[tokio::test(start_paused = true)]
    async fn an_answer_stores_the_deadline_of_its_own_duration() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let window = expiring("lse_e", 1, 60_000);
        table.on_grant(&window, None, t0).expect("grant");
        confirm_with(&mut table, &window, &HostNow::read(&clock));
        let id = LeaseId("lse_e".to_string());
        tokio::time::advance(millis(20_000)).await;
        let reading = HostNow::read(&clock);
        assert!(matches!(
            table.poll(reading.instant).as_slice(),
            [Action::SendRenew { .. }]
        ));
        let msg = MsgId("msg_after_window".to_string());
        table.renew_sent(&id, msg.clone(), &reading);
        table
            .on_grant(&expiring("lse_e", 1, 15000), Some(&msg), reading.instant)
            .expect("the answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.valid_until, Some(reading.instant + millis(13_500)));
        assert_eq!(
            lease.validity(),
            Some(&StoredValidity {
                boot_id: "boot-a".to_string(),
                until_boottime: millis(520_000 + 13_500),
            })
        );
    }

    /// Section 5.12: `duration_ms` is at most 600 000.
    #[test]
    fn a_grant_above_the_duration_ceiling_is_refused() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        assert!(matches!(
            table.on_grant(&expiring("lse_e", 1, 600_001), None, now),
            Err(Refusal::BadMessage(_))
        ));
        assert!(table.get(&LeaseId("lse_e".to_string())).is_none());
        table
            .on_grant(&expiring("lse_e", 1, 600_000), None, now)
            .expect("the ceiling itself is taken");
    }

    /// Section 5.12, ADR 0258: a higher token with the other mode is refused
    /// while the node holds the lease, `held` or `uncertain`, and changes
    /// nothing: the token, the mode, the deadline and the renewals stay, and
    /// a grant the authority may send at that token is still taken.
    #[test]
    fn a_higher_token_never_changes_the_mode_of_a_lease_the_node_holds() {
        let t0 = Instant::now();
        let id = LeaseId("lse_a".to_string());

        // `held` `local`, then a higher `expiring` grant.
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        let before = table.get(&id).cloned().expect("lease");
        let refused = table.on_grant(&expiring("lse_a", 2, 15000), None, t0);
        assert!(
            matches!(&refused, Err(Refusal::BadMessage(message)) if message.contains("only with the same token")),
            "{refused:?}"
        );
        assert_eq!(table.get(&id), Some(&before));
        assert!(table.may_keep_running(&id, 1));
        assert!(table.poll(t0 + millis(60_000)).is_empty(), "no renewal");
        table
            .on_grant(&local("lse_a", 2), None, t0)
            .expect("the same mode at that token is a new ownership");
        assert_eq!(table.get(&id).expect("lease").token, 2);

        // `held` `expiring`, then a higher `local` grant.
        let mut table = LeaseTable::new();
        let grant = expiring("lse_a", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let before = table.get(&id).cloned().expect("lease");
        assert_eq!(before.state, LeaseState::Held);
        assert!(matches!(
            table.on_grant(&local("lse_a", 2), None, t0),
            Err(Refusal::BadMessage(_))
        ));
        assert_eq!(table.get(&id), Some(&before));
        assert!(
            matches!(
                table.poll(t0 + millis(13_500)).as_slice(),
                [Action::Fence { .. }]
            ),
            "the deadline it had still fences it"
        );

        // `uncertain` `expiring`, before its first renewal is answered.
        let mut table = LeaseTable::new();
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("grant");
        let before = table.get(&id).cloned().expect("lease");
        assert_eq!(before.state, LeaseState::Uncertain);
        assert!(matches!(
            table.on_grant(&local("lse_a", 2), None, t0),
            Err(Refusal::BadMessage(_))
        ));
        assert_eq!(table.get(&id), Some(&before));
        assert!(!table.may_keep_running(&id, 2));
    }

    /// Section 5.12, ADR 0258: after `expired` or `revoked` the node holds
    /// nothing, so a higher token may carry either mode.
    #[test]
    fn a_higher_token_may_carry_the_other_mode_after_expired_or_revoked() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        table
            .on_revoke(&LeaseRevoke {
                lease_id: LeaseId("lse_a".to_string()),
                fencing_token: 1,
                reason: RevokeReason::Moved,
            })
            .expect("revoke");
        table
            .on_grant(&expiring("lse_a", 2, 15000), None, t0)
            .expect("granted again as expiring");
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(
            (lease.mode, lease.state, lease.token, lease.valid_until),
            (LeaseMode::Expiring, LeaseState::Uncertain, 2, None)
        );

        let mut table = LeaseTable::new();
        let grant = expiring("lse_x", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        assert!(matches!(
            table.poll(t0 + millis(13_500)).as_slice(),
            [Action::Fence { .. }]
        ));
        table
            .on_grant(&local("lse_x", 2), None, t0)
            .expect("granted again as local after expiry");
        let lease = table.get(&LeaseId("lse_x".to_string())).expect("lease");
        assert_eq!(
            (lease.mode, lease.state, lease.token),
            (LeaseMode::Local, LeaseState::Held, 2)
        );

        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let id = LeaseId("lse_e".to_string());
        table
            .on_revoke(&LeaseRevoke {
                lease_id: id.clone(),
                fencing_token: 2,
                reason: RevokeReason::Moved,
            })
            .expect("revoke");
        table
            .on_grant(&local("lse_e", 3), None, t0)
            .expect("granted again as local");
        let lease = table.get(&id).expect("lease");
        assert_eq!(
            (lease.mode, lease.state, lease.token),
            (LeaseMode::Local, LeaseState::Held, 3)
        );
    }

    /// ADR 0240 and 0241: the provisional deadline is on the lease clock like
    /// any `valid_until`, so a suspend moves it earlier. Before the first
    /// renewal there is none to move, and the renewal still goes out at once.
    #[test]
    fn a_suspend_moves_a_converted_lease_s_provisional_deadline() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("conversion");
        let id = LeaseId("lse_a".to_string());
        assert_eq!(table.catch_up(millis(2000), t0), millis(2000));
        assert_eq!(table.get(&id).expect("lease").valid_until, None);
        assert!(matches!(
            table.poll(t0).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(&id, MsgId("msg_first".to_string()), &HostNow::unknown(t0)));
        assert_eq!(table.catch_up(millis(5000), t0), millis(3000));
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(10_500))
        );
    }

    /// Section 8.4 "Agent restart" (ADR 0241): a converted lease stored after
    /// its first renewal keeps its provisional deadline across a restart. One
    /// stored before has no deadline: nothing proves the authority set no
    /// gate since, so it comes back `expired` and is fenced.
    #[tokio::test(start_paused = true)]
    async fn a_restore_keeps_a_converted_lease_s_provisional_deadline() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        table
            .on_grant(&expiring("lse_a", 1, 15000), None, t0)
            .expect("conversion");
        let id = LeaseId("lse_a".to_string());
        let before = StoredLease::from_lease(table.get(&id).expect("lease"));
        assert_eq!(before.validity, None);
        assert!(matches!(
            table.poll(t0).as_slice(),
            [Action::SendRenew { .. }]
        ));
        assert!(table.renew_sent(&id, MsgId("msg_first".to_string()), &HostNow::read(&clock)));
        let after = StoredLease::from_lease(table.get(&id).expect("lease"));
        assert_eq!(
            after.validity,
            Some(StoredValidity {
                boot_id: "boot-a".to_string(),
                until_boottime: millis(513_500),
            })
        );

        tokio::time::advance(millis(4000)).await;
        let now = HostNow::read(&clock);
        let highest = vec![((ResourceType::App, "app_test".to_string()), 1_u64)];
        let (restored, actions) =
            LeaseTable::restore(vec![after], highest.clone(), &HashSet::new(), &now);
        assert!(actions.is_empty());
        let lease = restored.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.valid_until, Some(now.instant + millis(9500)));

        let (restored, actions) = LeaseTable::restore(vec![before], highest, &HashSet::new(), &now);
        assert_eq!(
            actions,
            vec![Action::Fence {
                lease_id: id.clone(),
                reason: FenceReason::LeaseExpired,
            }]
        );
        assert_eq!(restored.get(&id).expect("lease").state, LeaseState::Expired);
    }

    #[test]
    fn a_renewal_sets_the_next_renewal_interval() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, now).expect("grant");
        confirm(&mut table, &grant, now);

        assert_eq!(
            table.poll(now),
            Vec::new(),
            "nothing to do right after the answering grant"
        );
        let at = table.next_deadline(now).expect("deadline");
        assert_eq!(at, now + millis(5000));
        assert_eq!(
            table.poll(at),
            vec![Action::SendRenew {
                lease_id: LeaseId("lse_e".to_string()),
                fencing_token: 1,
            }]
        );
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            MsgId("msg_1".to_string()),
            &HostNow::unknown(at),
        );
        assert_eq!(table.next_deadline(at), Some(at + millis(5000)));
    }

    #[test]
    fn a_grant_answering_a_renewal_uses_the_send_time() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let renew_at = t0 + millis(5000);
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            MsgId("msg_1".to_string()),
            &HostNow::unknown(renew_at),
        );
        let answer = MsgId("msg_1".to_string());
        // D-0073: a renewal answer carries the held token.
        table
            .on_grant(&grant, Some(&answer), renew_at + millis(20))
            .expect("grant");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.token, 1);
        assert_eq!(lease.valid_until, Some(renew_at + millis(13500)));
    }

    /// Section 8.4: an answer that arrives once `t_send + D - M` has passed
    /// would set a `valid_until` in the past. It changes nothing: a first
    /// grant stays `uncertain` and keeps renewing, and a held lease is left
    /// to the clock.
    #[test]
    fn an_answer_after_its_own_valid_until_sets_nothing() {
        let t0 = Instant::now();
        let id = LeaseId("lse_e".to_string());
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 3000);
        table.on_grant(&grant, None, t0).expect("grant");
        let _ = table.poll(t0);
        table.renew_sent(&id, MsgId("msg_first".to_string()), &HostNow::unknown(t0));
        // `valid_until` would be `t0 + 2 000`; the answer comes at `t0 + 2 000`.
        table
            .on_grant(
                &grant,
                Some(&MsgId("msg_first".to_string())),
                t0 + millis(2000),
            )
            .expect("the answer is taken");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.valid_until, None);
        assert!(!table.may_start_stateful(&id, 1, t0 + millis(2000)));
        assert!(!table.may_keep_running(&id, 1));

        // A timely answer to a later renewal makes it `held`.
        let later = t0 + millis(2500);
        table.renew_sent(
            &id,
            MsgId("msg_second".to_string()),
            &HostNow::unknown(later),
        );
        table
            .on_grant(
                &grant,
                Some(&MsgId("msg_second".to_string())),
                later + millis(10),
            )
            .expect("the answer is taken");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(later + millis(2000)));
    }

    /// Section 6.4 and section 8.4 (ADR 0242): a held `expiring` lease starts
    /// a stateful VM only with the start bound and `M` left before
    /// `valid_until`. Nothing starts in the last 4 500 ms of a 15 000 ms
    /// lease, nor past `valid_until` before the next poll marks it `expired`,
    /// while its VMs keep running. A lease of 5 000 ms or less has the time
    /// at most at the instant its renewal was sent, never once an answer
    /// arrives.
    #[test]
    fn a_start_needs_the_start_bound_and_the_margin_left() {
        let t0 = Instant::now();
        let id = LeaseId("lse_e".to_string());
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let until = t0 + millis(13_500);
        assert_eq!(table.get(&id).expect("lease").valid_until, Some(until));
        assert!(table.may_start_stateful(&id, 1, t0));
        assert!(table.may_start_stateful(&id, 1, until - millis(4500)));
        assert!(!table.may_start_stateful(&id, 1, until - millis(4499)));
        assert!(!table.may_start_stateful(&id, 1, until - millis(1)));
        assert!(!table.may_start_stateful(&id, 1, until));
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Held);
        assert!(table.may_keep_running(&id, 1));
        for (lease, resource, duration_ms) in [("lse_5", "app_5", 5000), ("lse_3", "app_3", 3000)] {
            let mut short = expiring(lease, 1, duration_ms);
            short.resource_id = resource.to_string();
            table.on_grant(&short, None, t0).expect("grant");
            confirm(&mut table, &short, t0);
            let short_id = LeaseId(lease.to_string());
            assert_eq!(table.get(&short_id).expect("lease").state, LeaseState::Held);
            assert!(
                !table.may_start_stateful(&short_id, 1, t0 + millis(1)),
                "{duration_ms}"
            );
        }
        // A `local` lease has no deadline.
        table
            .on_grant(&local_on("lse_l", "app_other", 1), None, t0)
            .expect("grant");
        assert!(table.may_start_stateful(&LeaseId("lse_l".to_string()), 1, t0 + millis(3_600_000)));
    }

    /// A renewal the authority never answers is dropped once its answer could
    /// no longer set a `valid_until` in the future, so the list stays bounded.
    #[test]
    fn unanswered_renewals_are_dropped_after_d_minus_m() {
        let t0 = Instant::now();
        let id = LeaseId("lse_e".to_string());
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 3000);
        table.on_grant(&grant, None, t0).expect("grant");
        // No answer ever comes: a renewal every `D / 3` for a minute.
        let mut now = t0;
        for step in 0..60 {
            for action in table.poll(now) {
                if let Action::SendRenew { lease_id, .. } = action {
                    table.renew_sent(
                        &lease_id,
                        MsgId(format!("msg_{step}")),
                        &HostNow::unknown(now),
                    );
                }
            }
            now += millis(1000);
        }
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert!(
            lease.outstanding.len() <= 3,
            "only the renewals of the last D - M are kept: {}",
            lease.outstanding.len()
        );
    }

    #[test]
    fn a_late_renewal_makes_the_lease_uncertain() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            MsgId("msg_1".to_string()),
            &HostNow::unknown(t0 + millis(5000)),
        );

        let late = t0 + millis(10000);
        let _ = table.poll(late);
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1, Instant::now()));
    }

    #[test]
    fn next_deadline_skips_instants_that_have_already_happened() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            MsgId("msg_1".to_string()),
            &HostNow::unknown(t0 + millis(5000)),
        );
        let late = t0 + millis(10000);
        let _ = table.poll(late);
        // The unanswered renewal's deadline is `late` itself; the next wakeup
        // the driver can sleep on is `valid_until`.
        assert_eq!(table.next_deadline(late), Some(t0 + millis(13500)));
    }

    #[test]
    fn the_authority_being_unreachable_makes_only_expiring_leases_uncertain() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, now).expect("grant");
        confirm(&mut table, &grant, now);
        table
            .on_grant(&local_on("lse_a", "app_local", 1), None, now)
            .expect("grant");

        table.authority_unreachable(now);
        assert_eq!(
            table
                .get(&LeaseId("lse_e".to_string()))
                .expect("lease")
                .state,
            LeaseState::Uncertain
        );
        assert_eq!(
            table
                .get(&LeaseId("lse_a".to_string()))
                .expect("lease")
                .state,
            LeaseState::Held
        );
    }

    #[test]
    fn renewing_now_moves_only_the_next_renewal() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15_000);
        table.on_grant(&grant, None, t0).expect("grant");
        let _ = confirm(&mut table, &grant, t0);
        table
            .on_grant(&local_on("lse_l", "app_other", 1), None, t0)
            .expect("the local grant is held");
        let held = table.get(&LeaseId("lse_e".to_owned())).unwrap().clone();
        table.authority_unreachable(t0 + millis(100));

        let at = t0 + millis(200);
        table.renew_now(at);
        let lease = table.get(&LeaseId("lse_e".to_owned())).unwrap();
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.valid_until, held.valid_until, "no validity is added");
        assert_eq!(
            table.poll(at),
            vec![Action::SendRenew {
                lease_id: LeaseId("lse_e".to_owned()),
                fencing_token: 1,
            }]
        );
        assert_eq!(
            table.get(&LeaseId("lse_l".to_owned())).unwrap().state,
            LeaseState::Held,
            "a local lease never renews"
        );
    }

    #[test]
    fn an_uncertain_lease_returns_to_held_on_a_renewal() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        table.authority_unreachable(t0);
        assert_eq!(
            table
                .get(&LeaseId("lse_e".to_string()))
                .expect("lease")
                .state,
            LeaseState::Uncertain
        );
        let renew_at = t0 + millis(5000);
        let answer = MsgId("msg_1".to_string());
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            answer.clone(),
            &HostNow::unknown(renew_at),
        );
        // D-0073: the answer carries the held token.
        table
            .on_grant(&grant, Some(&answer), renew_at)
            .expect("grant");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(renew_at + millis(13500)));
        assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 1, Instant::now()));
    }

    #[test]
    fn reaching_valid_until_expires_and_fences() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);

        assert!(
            !table
                .poll(t0 + millis(13499))
                .iter()
                .any(|action| matches!(action, Action::Fence { .. })),
            "nothing fences before valid_until"
        );
        assert_eq!(
            table.poll(t0 + millis(13500)),
            vec![Action::Fence {
                lease_id: LeaseId("lse_e".to_string()),
                reason: FenceReason::LeaseExpired,
            }]
        );
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Expired);
        assert_eq!(lease.valid_until, None);
        assert_eq!(table.next_deadline(t0 + millis(13500)), None);
    }

    #[test]
    fn a_revoke_from_any_state_fences() {
        for state in [
            LeaseState::Held,
            LeaseState::Uncertain,
            LeaseState::Expired,
            LeaseState::Revoked,
        ] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            let grant = expiring("lse_e", 1, 15000);
            table.on_grant(&grant, None, t0).expect("grant");
            confirm(&mut table, &grant, t0);
            match state {
                LeaseState::Held => {}
                LeaseState::Uncertain => table.authority_unreachable(t0),
                LeaseState::Expired => {
                    let _ = table.poll(t0 + millis(13500));
                }
                LeaseState::Revoked => {
                    table.on_revoke(&revoke("lse_e", 1)).expect("revoke");
                }
            }
            assert_eq!(
                table.on_revoke(&revoke("lse_e", 1)),
                Ok(RevokeOutcome::Fence {
                    lease_id: LeaseId("lse_e".to_string())
                }),
                "from {state:?}"
            );
            let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
            assert_eq!(lease.state, LeaseState::Revoked);
            assert_eq!(lease.valid_until, None);
        }
    }

    #[test]
    fn a_revoke_below_the_current_token_is_refused() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 3), None, t0).expect("grant");
        assert!(matches!(
            table.on_revoke(&revoke("lse_a", 2)),
            Err(Refusal::BadMessage(_))
        ));
        let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.token, 3);
    }

    #[test]
    fn a_revoke_of_an_unknown_lease_has_nothing_to_fence() {
        let mut table = LeaseTable::new();
        assert_eq!(
            table.on_revoke(&revoke("lse_missing", 1)),
            Ok(RevokeOutcome::UnknownLease)
        );
    }

    #[test]
    fn a_higher_token_regrant_holds_an_expired_or_revoked_lease() {
        for fenced in [LeaseState::Expired, LeaseState::Revoked] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            if fenced == LeaseState::Expired {
                let grant = expiring("lse_e", 1, 15000);
                table.on_grant(&grant, None, t0).expect("grant");
                confirm(&mut table, &grant, t0);
                let _ = table.poll(t0 + millis(13500));
                assert_eq!(
                    table
                        .get(&LeaseId("lse_e".to_string()))
                        .expect("lease")
                        .state,
                    LeaseState::Expired
                );
                // D-0072, D-0073: a higher token is new ownership, so the lease
                // is `uncertain` until a renewal is answered.
                let regrant = expiring("lse_e", 2, 15000);
                assert!(matches!(table.on_grant(&regrant, None, t0), Ok(())));
                let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
                assert_eq!(lease.state, LeaseState::Uncertain);
                assert_eq!(lease.token, 2);
                assert_eq!(lease.valid_until, None, "an expired lease had none");
                assert!(!table.may_start_stateful(
                    &LeaseId("lse_e".to_string()),
                    2,
                    Instant::now()
                ));
                confirm(&mut table, &regrant, t0);
                let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
                assert_eq!(lease.state, LeaseState::Held);
                assert_eq!(lease.valid_until, Some(t0 + millis(13500)));
                assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 2, Instant::now()));
                assert_eq!(table.highest_for(ResourceType::App, "app_test"), 2);
            } else {
                table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
                table.on_revoke(&revoke("lse_a", 1)).expect("revoke");
                assert!(matches!(
                    table.on_grant(&local("lse_a", 2), None, t0),
                    Ok(())
                ));
                let lease = table.get(&LeaseId("lse_a".to_string())).expect("lease");
                assert_eq!(lease.state, LeaseState::Held);
                assert_eq!(lease.token, 2);
                assert!(table.may_start_stateful(&LeaseId("lse_a".to_string()), 2, Instant::now()));
            }
        }
    }

    #[test]
    fn a_first_expiring_grant_is_uncertain_and_renews_at_once() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, now).expect("grant");

        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(
            lease.state,
            LeaseState::Uncertain,
            "D-0072: nothing proved the timing yet"
        );
        assert_eq!(lease.valid_until, None);
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1, Instant::now()));
        assert_eq!(
            table.poll(now),
            vec![Action::SendRenew {
                lease_id: LeaseId("lse_e".to_string()),
                fencing_token: 1,
            }],
            "the first renewal goes out at once"
        );

        let sent_at = now;
        let id = MsgId("msg_1".to_string());
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            id.clone(),
            &HostNow::unknown(sent_at),
        );
        table
            .on_grant(&grant, Some(&id), sent_at)
            .expect("the answer");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(sent_at + millis(13500)));
        assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 1, Instant::now()));
    }

    #[test]
    fn an_unsolicited_same_token_grant_does_not_move_valid_until() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);

        // Re None and no matching renewal: a duplicate, not an answer.
        table
            .on_grant(&grant, None, t0 + millis(1000))
            .expect("a no-op");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(t0 + millis(13500)));
        assert_eq!(
            table.next_deadline(t0 + millis(1000)),
            Some(t0 + millis(5000))
        );

        // A `re` that matches no outstanding renewal is a duplicate too.
        let stale = MsgId("msg_missing".to_string());
        table
            .on_grant(&grant, Some(&stale), t0 + millis(2000))
            .expect("a no-op");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.valid_until, Some(t0 + millis(13500)));
    }

    #[test]
    fn a_higher_token_grant_while_held_is_uncertain_and_keeps_valid_until() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let first = expiring("lse_e", 1, 15000);
        table.on_grant(&first, None, t0).expect("grant");
        confirm(&mut table, &first, t0);

        let at = t0 + millis(1000);
        let second = expiring("lse_e", 2, 15000);
        table
            .on_grant(&second, None, at)
            .expect("the new ownership");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.token, 2);
        assert_eq!(
            lease.valid_until,
            Some(t0 + millis(13500)),
            "a new ownership never extends the old validity"
        );
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 2, Instant::now()));
        assert_eq!(
            table.poll(at),
            vec![Action::SendRenew {
                lease_id: LeaseId("lse_e".to_string()),
                fencing_token: 2,
            }],
            "the new ownership proves itself with a renewal at once"
        );
    }

    #[test]
    fn an_equal_token_grant_after_revoke_or_expiry_is_refused() {
        for fenced in [LeaseState::Expired, LeaseState::Revoked] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            let grant = expiring("lse_e", 1, 15000);
            table.on_grant(&grant, None, t0).expect("grant");
            confirm(&mut table, &grant, t0);
            match fenced {
                LeaseState::Expired => {
                    let _ = table.poll(t0 + millis(13500));
                }
                _ => {
                    table.on_revoke(&revoke("lse_e", 1)).expect("revoke");
                }
            }
            assert!(matches!(
                table.on_grant(&grant, None, t0 + millis(14000)),
                Err(Refusal::BadMessage(_))
            ));
            let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
            assert_eq!(lease.state, fenced);
            assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1, Instant::now()));
        }
    }

    #[test]
    fn a_grant_answering_a_renewal_with_a_higher_token_is_new_ownership() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let first = expiring("lse_e", 1, 15000);
        table.on_grant(&first, None, t0).expect("grant");
        confirm(&mut table, &first, t0);

        let renew_at = t0 + millis(5000);
        let renew_id = MsgId("msg_1".to_string());
        table.renew_sent(
            &LeaseId("lse_e".to_string()),
            renew_id.clone(),
            &HostNow::unknown(renew_at),
        );
        // D-0073: a higher token is ownership change even when it answers a
        // renewal, so it is `uncertain`, never `held`.
        let second = expiring("lse_e", 2, 15000);
        table
            .on_grant(&second, Some(&renew_id), renew_at)
            .expect("the new ownership");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.token, 2);
        assert_eq!(lease.valid_until, Some(t0 + millis(13500)));
        assert_eq!(
            table.poll(renew_at),
            vec![Action::SendRenew {
                lease_id: LeaseId("lse_e".to_string()),
                fencing_token: 2,
            }]
        );
    }

    #[test]
    fn local_leases_never_expire_with_the_clock() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table.on_grant(&local("lse_a", 1), None, t0).expect("grant");
        assert!(table.poll(t0 + millis(600_000)).is_empty());
        assert_eq!(
            table
                .get(&LeaseId("lse_a".to_string()))
                .expect("lease")
                .state,
            LeaseState::Held
        );
        assert_eq!(table.next_deadline(t0 + millis(600_000)), None);
    }

    #[test]
    fn restore_fences_expiring_leases_that_were_held() {
        let stored = StoredLease {
            lease_id: LeaseId("lse_e".to_string()),
            resource_type: ResourceType::App,
            resource_id: "app_test".to_string(),
            mode: LeaseMode::Expiring,
            token: 7,
            state: LeaseState::Uncertain,
            duration: Some(millis(15000)),
            validity: None,
        };
        let highest = vec![((ResourceType::App, "app_test".to_string()), 7_u64)];
        let (table, actions) = LeaseTable::restore(
            vec![stored],
            highest,
            &HashSet::new(),
            &HostNow::unknown(Instant::now()),
        );
        assert_eq!(
            actions,
            vec![Action::Fence {
                lease_id: LeaseId("lse_e".to_string()),
                reason: FenceReason::LeaseExpired,
            }]
        );
        assert_eq!(
            table
                .get(&LeaseId("lse_e".to_string()))
                .expect("lease")
                .state,
            LeaseState::Expired
        );
        assert_eq!(table.highest_for(ResourceType::App, "app_test"), 7);
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 7, Instant::now()));
    }

    #[test]
    fn restore_keeps_local_and_fenced_leases_as_they_were() {
        let leases = vec![
            StoredLease {
                lease_id: LeaseId("lse_local".to_string()),
                resource_type: ResourceType::App,
                resource_id: "app_local".to_string(),
                mode: LeaseMode::Local,
                token: 2,
                state: LeaseState::Held,
                duration: None,
                validity: None,
            },
            StoredLease {
                lease_id: LeaseId("lse_revoked".to_string()),
                resource_type: ResourceType::App,
                resource_id: "app_revoked".to_string(),
                mode: LeaseMode::Local,
                token: 3,
                state: LeaseState::Revoked,
                duration: None,
                validity: None,
            },
        ];
        let fenced: HashSet<(LeaseId, u64)> = [(LeaseId("lse_revoked".to_string()), 3)]
            .into_iter()
            .collect();
        let (table, actions) = LeaseTable::restore(
            leases,
            Vec::new(),
            &fenced,
            &HostNow::unknown(Instant::now()),
        );
        assert!(actions.is_empty());
        assert_eq!(
            table
                .get(&LeaseId("lse_local".to_string()))
                .expect("lease")
                .state,
            LeaseState::Held
        );
        assert_eq!(
            table
                .get(&LeaseId("lse_revoked".to_string()))
                .expect("lease")
                .state,
            LeaseState::Revoked
        );
        assert_eq!(table.highest_for(ResourceType::App, "app_local"), 2);
        assert_eq!(table.highest_for(ResourceType::App, "app_revoked"), 3);
    }

    fn stored(lease: &str, mode: LeaseMode, token: u64, state: LeaseState) -> StoredLease {
        StoredLease {
            lease_id: LeaseId(lease.to_string()),
            resource_type: ResourceType::App,
            resource_id: format!("app_{lease}"),
            mode,
            token,
            state,
            duration: match mode {
                LeaseMode::Local => None,
                LeaseMode::Expiring => Some(millis(15000)),
            },
            validity: None,
        }
    }

    /// Section 8.5 "Concurrency" (D-0075): a lease that reached a fenced state
    /// but has no fence record for its token was interrupted mid-fence, so the
    /// restore fences it again; a recorded fence is not repeated.
    #[test]
    fn restore_fences_again_a_fence_the_restart_interrupted() {
        let leases = vec![
            stored("lse_cut", LeaseMode::Local, 4, LeaseState::Revoked),
            stored("lse_done", LeaseMode::Local, 2, LeaseState::Revoked),
            stored("lse_exp", LeaseMode::Expiring, 5, LeaseState::Expired),
            stored("lse_held", LeaseMode::Local, 1, LeaseState::Held),
        ];
        // `lse_cut` was recorded at an older token only: its newest revoke
        // never finished its fence.
        let fenced: HashSet<(LeaseId, u64)> = [
            (LeaseId("lse_cut".to_string()), 3),
            (LeaseId("lse_done".to_string()), 2),
        ]
        .into_iter()
        .collect();
        let (table, actions) = LeaseTable::restore(
            leases,
            Vec::new(),
            &fenced,
            &HostNow::unknown(Instant::now()),
        );
        assert_eq!(
            actions,
            vec![
                Action::Fence {
                    lease_id: LeaseId("lse_cut".to_string()),
                    reason: FenceReason::LeaseRevoked,
                },
                Action::Fence {
                    lease_id: LeaseId("lse_exp".to_string()),
                    reason: FenceReason::LeaseExpired,
                },
            ]
        );
        for (id, state) in [
            ("lse_cut", LeaseState::Revoked),
            ("lse_done", LeaseState::Revoked),
            ("lse_exp", LeaseState::Expired),
            ("lse_held", LeaseState::Held),
        ] {
            assert_eq!(
                table.get(&LeaseId(id.to_string())).expect("lease").state,
                state
            );
        }
        assert!(!table.may_start_stateful(&LeaseId("lse_cut".to_string()), 4, Instant::now()));
    }

    /// A held expiring lease is fenced once by the restore, not twice.
    #[test]
    fn restore_fences_a_held_expiring_lease_once() {
        let leases = vec![stored("lse_e", LeaseMode::Expiring, 7, LeaseState::Held)];
        let (_table, actions) = LeaseTable::restore(
            leases,
            Vec::new(),
            &HashSet::new(),
            &HostNow::unknown(Instant::now()),
        );
        assert_eq!(
            actions,
            vec![Action::Fence {
                lease_id: LeaseId("lse_e".to_string()),
                reason: FenceReason::LeaseExpired,
            }]
        );
    }

    /// Holds an expiring lease, stores it through `clock`, and returns the
    /// stored row with its `valid_until` (D = 15 s, so 13.5 s after `t0`).
    fn held_and_stored(clock: &ManualHostClock, t0: Instant) -> (StoredLease, Instant) {
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 7, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        let reading = HostNow::read(clock);
        assert_eq!(reading.instant, t0, "the test clock is paused");
        confirm_with(&mut table, &grant, &reading);
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        let until = lease.valid_until.expect("held with a deadline");
        (StoredLease::from_lease(lease), until)
    }

    /// Section 8.4 "Agent restart" (ADR 0072): within the same boot a held
    /// expiring lease comes back `uncertain` with what is left of its
    /// validity, renews at once, keeps its VMs and starts nothing.
    #[tokio::test(start_paused = true)]
    async fn restore_keeps_a_held_expiring_lease_within_the_same_boot() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let t0 = Instant::now();
        let (stored, _until) = held_and_stored(&clock, t0);
        assert_eq!(
            stored.validity,
            Some(StoredValidity {
                boot_id: "boot-a".to_string(),
                until_boottime: millis(513_500),
            })
        );

        // The agent restarts 4 s later.
        tokio::time::advance(millis(4000)).await;
        let now = HostNow::read(&clock);
        let highest = vec![((ResourceType::App, "app_test".to_string()), 7_u64)];
        let (mut table, actions) =
            LeaseTable::restore(vec![stored], highest, &HashSet::new(), &now);
        assert!(actions.is_empty(), "nothing is fenced");
        let id = LeaseId("lse_e".to_string());
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.valid_until, Some(now.instant + millis(9500)));
        assert!(
            !table.may_start_stateful(&id, 7, Instant::now()),
            "starts nothing new"
        );
        assert!(table.may_keep_running(&id, 7), "keeps what runs");
        assert!(!table.may_keep_running(&id, 6));
        assert_eq!(
            table.poll(now.instant),
            vec![Action::SendRenew {
                lease_id: id.clone(),
                fencing_token: 7,
            }],
            "renews at once"
        );

        // Unanswered, it expires at the restored deadline, not later.
        let expiry = now.instant + millis(9500);
        assert!(
            !table
                .poll(expiry - millis(1))
                .iter()
                .any(|action| matches!(action, Action::Fence { .. }))
        );
        assert_eq!(
            table.poll(expiry),
            vec![Action::Fence {
                lease_id: id,
                reason: FenceReason::LeaseExpired,
            }]
        );
    }

    /// The renewal a restored lease sends makes it `held` again when it is
    /// answered with the same token.
    #[tokio::test(start_paused = true)]
    async fn a_restored_lease_is_held_again_once_its_renewal_is_answered() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let (stored, _) = held_and_stored(&clock, Instant::now());
        tokio::time::advance(millis(2000)).await;
        let now = HostNow::read(&clock);
        let (mut table, _) = LeaseTable::restore(vec![stored], Vec::new(), &HashSet::new(), &now);
        let id = LeaseId("lse_e".to_string());
        let _ = table.poll(now.instant);
        let renew = MsgId("msg_after_restart".to_string());
        table.renew_sent(&id, renew.clone(), &now);
        table
            .on_grant(&expiring("lse_e", 7, 15000), Some(&renew), now.instant)
            .expect("the answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(now.instant + millis(13500)));
        assert!(table.may_start_stateful(&id, 7, Instant::now()));
    }

    /// After a reboot, after the validity ran out, with no stored validity,
    /// or with a row no grant could have produced, a held expiring lease comes
    /// back `expired` and is fenced.
    #[tokio::test(start_paused = true)]
    async fn restore_fences_a_held_expiring_lease_it_cannot_prove() {
        for case in [
            "reboot",
            "ran out",
            "suspended past it",
            "no validity",
            "no duration",
            "more left than D - M",
            "duration below the floor",
            "duration above the ceiling",
            "stored uncertain, ran out",
        ] {
            let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
            let (mut stored, _) = held_and_stored(&clock, Instant::now());
            match case {
                "reboot" => clock.reboot("boot-b", millis(1000)),
                "ran out" => tokio::time::advance(millis(13_500)).await,
                "suspended past it" => clock.suspend(millis(20_000)),
                "no validity" => stored.validity = None,
                "no duration" => stored.duration = None,
                "more left than D - M" => {
                    if let Some(validity) = stored.validity.as_mut() {
                        validity.until_boottime += millis(1);
                    }
                }
                "duration below the floor" => stored.duration = Some(millis(2999)),
                "duration above the ceiling" => stored.duration = Some(millis(600_001)),
                _ => {
                    stored.state = LeaseState::Uncertain;
                    tokio::time::advance(millis(13_500)).await;
                }
            }
            let now = HostNow::read(&clock);
            let (table, actions) =
                LeaseTable::restore(vec![stored], Vec::new(), &HashSet::new(), &now);
            let id = LeaseId("lse_e".to_string());
            assert_eq!(
                actions,
                vec![Action::Fence {
                    lease_id: id.clone(),
                    reason: FenceReason::LeaseExpired,
                }],
                "{case}"
            );
            let lease = table.get(&id).expect("lease");
            assert_eq!(lease.state, LeaseState::Expired, "{case}");
            assert!(!table.may_keep_running(&id, 7), "{case}");
        }
    }

    /// A stored `uncertain` lease within its validity comes back `uncertain`
    /// with what is left, like a held one.
    #[tokio::test(start_paused = true)]
    async fn restore_keeps_a_stored_uncertain_lease_within_its_validity() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let (mut stored, _) = held_and_stored(&clock, Instant::now());
        stored.state = LeaseState::Uncertain;
        tokio::time::advance(millis(3000)).await;
        let now = HostNow::read(&clock);
        let (table, actions) = LeaseTable::restore(vec![stored], Vec::new(), &HashSet::new(), &now);
        assert!(actions.is_empty());
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(lease.valid_until, Some(now.instant + millis(10_500)));
    }

    /// Section 8.4: an `uncertain` lease with no deadline, a first grant that
    /// was never confirmed, keeps no running VM, since nothing would ever
    /// fence it.
    #[tokio::test(start_paused = true)]
    async fn an_unconfirmed_first_grant_keeps_no_running_vm() {
        let mut table = LeaseTable::new();
        table
            .on_grant(&expiring("lse_e", 1, 15000), None, Instant::now())
            .expect("grant");
        let id = LeaseId("lse_e".to_string());
        assert_eq!(table.get(&id).expect("lease").state, LeaseState::Uncertain);
        assert!(!table.may_start_stateful(&id, 1, Instant::now()));
        assert!(!table.may_keep_running(&id, 1));
    }

    /// An uncertain lease whose first grant was never answered has no
    /// validity to store, and a fenced one stores none.
    #[tokio::test(start_paused = true)]
    async fn only_a_known_validity_is_stored() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        let t0 = Instant::now();
        table.on_grant(&grant, None, t0).expect("grant");
        let id = LeaseId("lse_e".to_string());
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert_eq!(StoredLease::from_lease(lease).validity, None);

        confirm_with(&mut table, &grant, &HostNow::read(&clock));
        let lease = table.get(&id).expect("lease");
        assert!(StoredLease::from_lease(lease).validity.is_some(), "held");
        let _ = table.poll(t0 + millis(13_500));
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Expired);
        assert_eq!(StoredLease::from_lease(lease).validity, None);

        table
            .on_grant(&local_on("lse_l", "app_l", 1), None, t0)
            .expect("grant");
        let local = table.get(&LeaseId("lse_l".to_string())).expect("lease");
        assert_eq!(StoredLease::from_lease(local).validity, None);
    }

    #[test]
    fn polls_are_sorted_by_lease_id() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        for id in ["lse_c", "lse_a", "lse_b"] {
            let mut grant = expiring(id, 1, 3000);
            grant.resource_id = format!("app_{id}");
            table.on_grant(&grant, None, t0).expect("grant");
            confirm(&mut table, &grant, t0);
        }
        let actions = table.poll(t0 + millis(2000));
        let ids: Vec<String> = actions
            .into_iter()
            .map(|action| match action {
                Action::Fence { lease_id, .. } | Action::SendRenew { lease_id, .. } => lease_id.0,
            })
            .collect();
        assert_eq!(ids, vec!["lse_a", "lse_b", "lse_c"]);
    }

    /// Section 8.4 "Suspend" (ADR 0240): a suspend moves `valid_until`, the
    /// next renewal and the send time of a renewal still outstanding earlier
    /// by its length, so an answer to that renewal sets a `valid_until` that
    /// counts the suspend. The same measurement is applied once.
    #[tokio::test(start_paused = true)]
    async fn a_suspend_moves_the_lease_clock_instants_earlier() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let id = LeaseId("lse_e".to_string());
        let now = t0 + millis(2000);
        let renew = MsgId("msg_before_the_suspend".to_string());
        table.renew_sent(&id, renew.clone(), &HostNow::unknown(now));

        assert_eq!(table.catch_up(millis(3000), now), millis(3000));
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.valid_until, Some(t0 + millis(10_500)));
        assert_eq!(
            table.catch_up(millis(3000), now),
            Duration::ZERO,
            "applied once"
        );
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(10_500))
        );
        // The renewal now counts as sent at t0 - 1 000 ms.
        table
            .on_grant(&grant, Some(&renew), now)
            .expect("the answer");
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(12_500))
        );
        // The next renewal moved too: it is due at t0 + 4 000 ms, not
        // t0 + 4 000 + 3 000.
        assert_eq!(table.next_deadline(now), Some(t0 + millis(4000)));
    }

    /// A lease whose `valid_until` passed during the suspend expires at the
    /// next poll, and a suspend too long for an instant to represent still
    /// fences.
    #[tokio::test(start_paused = true)]
    async fn a_suspend_past_valid_until_fences_at_the_next_poll() {
        for suspended in [millis(12_000), Duration::MAX] {
            let t0 = Instant::now();
            let mut table = LeaseTable::new();
            let grant = expiring("lse_e", 1, 15000);
            table.on_grant(&grant, None, t0).expect("grant");
            confirm(&mut table, &grant, t0);
            let id = LeaseId("lse_e".to_string());
            let now = t0 + millis(2000);
            assert!(table.may_start_stateful(&id, 1, now));
            assert_eq!(table.catch_up(suspended, now), suspended);
            assert!(!table.may_start_stateful(&id, 1, now), "{suspended:?}");
            assert_eq!(
                table.poll(now),
                vec![Action::Fence {
                    lease_id: id.clone(),
                    reason: FenceReason::LeaseExpired,
                }],
                "{suspended:?}"
            );
            assert_eq!(table.get(&id).expect("lease").state, LeaseState::Expired);
        }
    }

    /// A measurement within the noise of two clock reads is not applied
    /// until more adds up, and then all of it is.
    #[tokio::test(start_paused = true)]
    async fn a_suspend_below_the_resolution_waits_until_it_adds_up() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        confirm(&mut table, &grant, t0);
        let id = LeaseId("lse_e".to_string());
        assert_eq!(
            table.catch_up(Duration::from_micros(500), t0),
            Duration::ZERO
        );
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(13_500))
        );
        assert_eq!(
            table.catch_up(Duration::from_micros(1500), t0),
            Duration::from_micros(1500)
        );
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(13_500) - Duration::from_micros(1500))
        );
    }

    /// A suspend caught up with before a grant shortens nothing the grant
    /// records afterwards, and only `held` or `uncertain` `expiring` leases
    /// keep instants that move.
    #[tokio::test(start_paused = true)]
    async fn a_suspend_shortens_no_instant_recorded_after_it() {
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local_on("lse_l", "app_l", 1), None, t0)
            .expect("grant");
        assert!(!table.has_timed_leases());
        assert_eq!(table.catch_up(millis(60_000), t0), millis(60_000));
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        assert!(table.has_timed_leases());
        confirm(&mut table, &grant, t0);
        assert_eq!(table.catch_up(millis(60_000), t0), Duration::ZERO);
        let id = LeaseId("lse_e".to_string());
        assert_eq!(
            table.get(&id).expect("lease").valid_until,
            Some(t0 + millis(13_500))
        );
        assert!(table.may_start_stateful(&LeaseId("lse_l".to_string()), 1, t0));
    }

    /// Section 8.4 "Agent restart" (ADR 0240): the stored deadline is
    /// `CLOCK_BOOTTIME(t_send) + D - M`, read when the renewal was sent, so a
    /// suspend between the renewal and its answer does not make it later.
    #[tokio::test(start_paused = true)]
    async fn the_stored_deadline_comes_from_the_send_time() {
        let clock = ManualHostClock::new(Some("boot-a"), millis(500_000));
        let t0 = Instant::now();
        let mut table = LeaseTable::new();
        let grant = expiring("lse_e", 1, 15000);
        table.on_grant(&grant, None, t0).expect("grant");
        let _ = table.poll(t0);
        let id = LeaseId("lse_e".to_string());
        let renew = MsgId("msg_renew".to_string());
        table.renew_sent(&id, renew.clone(), &HostNow::read(&clock));

        clock.suspend(millis(3000));
        assert_eq!(table.catch_up(millis(3000), t0), millis(3000));
        table
            .on_grant(&grant, Some(&renew), t0)
            .expect("the answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(t0 + millis(10_500)));
        assert_eq!(
            StoredLease::from_lease(lease).validity,
            Some(StoredValidity {
                boot_id: "boot-a".to_string(),
                until_boottime: millis(513_500),
            }),
            "not 516 500: the suspend came after the send"
        );
        // Restored right away, it has the same 10 500 ms left.
        let (restored, actions) = LeaseTable::restore(
            vec![StoredLease::from_lease(lease)],
            Vec::new(),
            &HashSet::new(),
            &HostNow::read(&clock),
        );
        assert!(actions.is_empty());
        assert_eq!(
            restored.get(&id).expect("lease").valid_until,
            Some(t0 + millis(10_500))
        );
    }
}
