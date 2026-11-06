// SPDX-License-Identifier: AGPL-3.0-only
//! The pure lease state machine of `docs/contracts/node-agent-protocol.md`
//! section 8: no I/O, no async, no clock reads.
//!
//! Every method takes `now: Instant` from the caller, so the same code runs
//! against the paused test clock and against the node's real monotonic clock.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use quoro_proto::{FenceReason, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, MsgId, ResourceType};
use tokio::time::Instant;

use crate::lease::clock::HostNow;
use crate::lease::store::StoredLease;

/// The floor the authority must respect for an `expiring` grant (section 8.4).
pub const MIN_DURATION: Duration = Duration::from_millis(3000);

/// The longest `duration_ms` a grant may carry (section 5.12).
const MAX_DURATION: Duration = Duration::from_millis(600_000);

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
    /// answered, and once fenced (section 8.4). An `uncertain` lease with
    /// `None` never expires by the clock.
    pub valid_until: Option<Instant>,
    /// When to send the next renewal; `None` for `local` (section 8.4).
    next_renew_at: Option<Instant>,
    /// Renewals sent and not yet answered, with their send times.
    outstanding: Vec<(MsgId, Instant)>,
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
}

impl LeaseTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            leases: HashMap::new(),
            highest_token: HashMap::new(),
        }
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
                        valid_until = Some(until);
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
        // 3 000 ms; `local` carries none (section 5.12).
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

        // A lease id may only ever name one resource and one mode.
        if let Some(existing) = self.leases.get(&grant.lease_id)
            && (existing.resource_type != grant.resource_type
                || existing.resource_id != grant.resource_id
                || existing.mode != grant.mode)
        {
            return Err(Refusal::BadMessage(format!(
                "lease {} already names a different resource or mode",
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
                        next_renew_at: None,
                        outstanding: Vec::new(),
                    },
                );
            }
            Some(d) => {
                if same_token {
                    // D-0072: only a grant that answers an outstanding renewal
                    // moves the timing; anything else changes nothing.
                    let t_send = existing.as_ref().and_then(|existing| {
                        answers.and_then(|id| {
                            existing
                                .outstanding
                                .iter()
                                .find(|(sent, _)| sent == id)
                                .map(|(_, at)| *at)
                        })
                    });
                    let Some(t_send) = t_send else {
                        return Ok(());
                    };
                    // Section 8.4: `valid_until = t_send + D - M`, renew every
                    // `D / 3`. Renewals sent at or before `t_send` are
                    // answered by this grant.
                    let outstanding: Vec<(MsgId, Instant)> = existing
                        .as_ref()
                        .map(|existing| {
                            existing
                                .outstanding
                                .iter()
                                .filter(|(_, at)| *at > t_send)
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default();
                    if let Some(lease) = self.leases.get_mut(&grant.lease_id) {
                        lease.state = LeaseState::Held;
                        lease.duration = Some(d);
                        lease.valid_until = Some(t_send + (d - margin(d)));
                        lease.next_renew_at = Some(t_send + d / 3);
                        lease.outstanding = outstanding;
                    }
                    return Ok(());
                }

                // D-0072, D-0073: a higher token is a new ownership. The node
                // cannot prove the new grant until a renewal is answered, so
                // the lease is `uncertain` and renews immediately. It never
                // extends the validity the previous ownership had.
                let valid_until = existing.as_ref().and_then(|existing| {
                    if matches!(existing.state, LeaseState::Held | LeaseState::Uncertain) {
                        existing.valid_until
                    } else {
                        None
                    }
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

    /// Records that a renewal for `lease_id` was just sent at `now`.
    pub fn renew_sent(&mut self, lease_id: &LeaseId, msg: MsgId, now: Instant) {
        let Some(lease) = self.leases.get_mut(lease_id) else {
            return;
        };
        if lease.mode != LeaseMode::Expiring
            || !matches!(lease.state, LeaseState::Held | LeaseState::Uncertain)
        {
            return;
        }
        let Some(d) = lease.duration else {
            return;
        };
        lease.outstanding.push((msg, now));
        lease.next_renew_at = Some(now + d / 3);
    }

    /// The authority is unreachable (section 8.4): every expiring lease that
    /// was held becomes uncertain. Local leases never change for this reason
    /// (section 8.3).
    pub fn authority_unreachable(&mut self, _now: Instant) {
        for lease in self.leases.values_mut() {
            if lease.mode == LeaseMode::Expiring && lease.state == LeaseState::Held {
                lease.state = LeaseState::Uncertain;
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
                    .any(|(_, sent)| now >= *sent + d / 3)
            {
                lease.state = LeaseState::Uncertain;
            }

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
    /// Instants already reached are not reported: the caller polls for those
    /// before it waits, and sleeping on a past instant never wakes.
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
            candidates.extend(lease.outstanding.iter().map(|(_, sent)| *sent + d / 3));
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
    #[must_use]
    pub fn may_start_stateful(&self, lease_id: &LeaseId, spec_token: u64) -> bool {
        self.leases.get(lease_id).is_some_and(|lease| {
            lease.state == LeaseState::Held
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

/// Whether a restored `valid_until` can come from a grant of duration `d`:
/// `d` within the bounds of section 5.12, and no more than `D - M` left
/// (section 8.4). Anything else is a damaged row, and the lease is fenced.
fn plausible_validity(d: Duration, until: Instant, now: Instant) -> bool {
    (MIN_DURATION..=MAX_DURATION).contains(&d)
        && until.saturating_duration_since(now) <= d.saturating_sub(margin(d))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use quoro_proto::{
        FenceReason, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, MsgId, ResourceType,
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
        table.renew_sent(&grant.lease_id, id.clone(), now);
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
        assert!(table.may_start_stateful(&LeaseId("lse_old".to_string()), 1));
        table
            .on_grant(&local("lse_new", 2), None, now)
            .expect("grant");
        assert!(!table.may_start_stateful(&LeaseId("lse_old".to_string()), 1));
        assert!(table.may_start_stateful(&LeaseId("lse_new".to_string()), 2));
    }

    #[test]
    fn margin_matches_section_8_4() {
        assert_eq!(margin(millis(3000)), millis(1000));
        assert_eq!(margin(millis(15000)), millis(1500));
        assert_eq!(margin(millis(30000)), millis(3000));
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
        assert!(table.may_start_stateful(&LeaseId("lse_a".to_string()), 1));
        assert!(!table.may_start_stateful(&LeaseId("lse_a".to_string()), 2));
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
    fn a_lease_id_cannot_change_resource_or_mode() {
        let now = Instant::now();
        let mut table = LeaseTable::new();
        table
            .on_grant(&local("lse_a", 1), None, now)
            .expect("grant");
        let mut other_resource = local("lse_a", 2);
        other_resource.resource_id = "app_other".to_string();
        assert!(matches!(
            table.on_grant(&other_resource, None, now),
            Err(Refusal::BadMessage(_))
        ));
        assert!(matches!(
            table.on_grant(&expiring("lse_a", 2, 15000), None, now),
            Err(Refusal::BadMessage(_))
        ));
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
            at,
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
            renew_at,
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
            t0 + millis(5000),
        );

        let late = t0 + millis(10000);
        let _ = table.poll(late);
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Uncertain);
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1));
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
            t0 + millis(5000),
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
        table.renew_sent(&LeaseId("lse_e".to_string()), answer.clone(), renew_at);
        // D-0073: the answer carries the held token.
        table
            .on_grant(&grant, Some(&answer), renew_at)
            .expect("grant");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(renew_at + millis(13500)));
        assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 1));
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
                assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 2));
                confirm(&mut table, &regrant, t0);
                let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
                assert_eq!(lease.state, LeaseState::Held);
                assert_eq!(lease.valid_until, Some(t0 + millis(13500)));
                assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 2));
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
                assert!(table.may_start_stateful(&LeaseId("lse_a".to_string()), 2));
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
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1));
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
        table.renew_sent(&LeaseId("lse_e".to_string()), id.clone(), sent_at);
        table
            .on_grant(&grant, Some(&id), sent_at)
            .expect("the answer");
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(sent_at + millis(13500)));
        assert!(table.may_start_stateful(&LeaseId("lse_e".to_string()), 1));
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
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 2));
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
            assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 1));
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
        table.renew_sent(&LeaseId("lse_e".to_string()), renew_id.clone(), renew_at);
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
        assert!(!table.may_start_stateful(&LeaseId("lse_e".to_string()), 7));
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
        assert!(!table.may_start_stateful(&LeaseId("lse_cut".to_string()), 4));
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
        confirm(&mut table, &grant, t0);
        let lease = table.get(&LeaseId("lse_e".to_string())).expect("lease");
        let until = lease.valid_until.expect("held with a deadline");
        (StoredLease::from_lease(lease, &HostNow::read(clock)), until)
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
        assert!(!table.may_start_stateful(&id, 7), "starts nothing new");
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
        table.renew_sent(&id, renew.clone(), now.instant);
        table
            .on_grant(&expiring("lse_e", 7, 15000), Some(&renew), now.instant)
            .expect("the answer");
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Held);
        assert_eq!(lease.valid_until, Some(now.instant + millis(13500)));
        assert!(table.may_start_stateful(&id, 7));
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
        assert!(!table.may_start_stateful(&id, 1));
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
        assert_eq!(
            StoredLease::from_lease(lease, &HostNow::read(&clock)).validity,
            None
        );

        confirm(&mut table, &grant, t0);
        let _ = table.poll(t0 + millis(13_500));
        let lease = table.get(&id).expect("lease");
        assert_eq!(lease.state, LeaseState::Expired);
        assert_eq!(
            StoredLease::from_lease(lease, &HostNow::read(&clock)).validity,
            None
        );

        table
            .on_grant(&local_on("lse_l", "app_l", 1), None, t0)
            .expect("grant");
        let local = table.get(&LeaseId("lse_l".to_string())).expect("lease");
        assert_eq!(
            StoredLease::from_lease(local, &HostNow::read(&clock)).validity,
            None
        );
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
}
