// SPDX-License-Identifier: AGPL-3.0-only
//! The maintenance window (`node-agent-protocol.md` section 8.11; ADR 0171,
//! ADR 0268, ADR 0289; issue #764).
//!
//! `quoro cluster maintenance [--duration <seconds>]` reaches the running
//! primary over its local control socket, and the primary opens a window of
//! `W` (default 180 s, at least `D`, at most 600 s) in four steps, each bounded
//! by `D / 3` plus one margin ([`step_bound`]):
//!
//! 1. **Support.** Every `support_request` carries `max_lease_ms` = `W` from
//!    the moment the window starts opening, and each session sends one at
//!    once ([`Window::subscribe`]). The window goes on once a majority of
//!    voters supports `W` (section 8.8). Without one in time it fails and
//!    does not open.
//! 2. **The raise.** One single-writer transaction raises `leases.duration_ms`
//!    to `W` for every lease row that reads `expiring`, carries no unsettled
//!    mark and is not revoked ([`NodeHub::raise_for_window`]). Those rows are
//!    the window's leases.
//! 3. **Replication.** The window opens only once the newest write of every
//!    one of those rows, the raise included, is replicated over the required
//!    set (section 8.4, "When R is replicated", ADR 0268). Not replicated in
//!    time, the window does not open (ADR 0289). Until it opens, renewals are
//!    answered with `D`.
//! 4. **The renewals.** While it is open, the answer to a renewal of one of
//!    its leases by that lease's holder carries `W`
//!    ([`NodeHub::window_answer_ms`]). The command succeeds once every one of
//!    them had such an answer, and fails when a holder has not renewed in
//!    time; the window then stays open, so the leases already answered keep
//!    their longer validity.
//!
//! The primary writes one `cluster.maintenance` audit record per window it
//! tries to open: `ok` once the window opens, `error` with the reason when it
//! does not (`audit-log-format.md` section 3.7). `--end` writes another,
//! with `params` `{end: true}`.
//!
//! **Closing.** The window closes `W` after it opened, with `--end`, or at
//! the primary's next start, since it lives in memory only; whichever comes
//! first. Renewals are then answered with `D` again and `support_request`s
//! carry `D`. The gates keep every `W` answer's longer gate
//! ([`crate::authority::Authority`] keeps the latest gate per duration and
//! never brings it forward), and a restarted primary counts the stored `W`
//! for every raised lease from its start.
//!
//! **Lowering.** Once the window is closed and the gate of every grant longer
//! than `D` has passed ([`crate::authority::Authority::may_lower`]), the
//! stored duration goes back to `D` ([`NodeHub::lower_durations`]), so later
//! restarts of the primary count `D` again. Section 8.4 allows it ("lowers
//! only once the longer grant's gate has passed"), and a primary that lacks
//! the lowering only waits longer.
//!
//! **Safety.** A `W` answer goes out only when, in the answer's own
//! single-writer transaction, the row still stores at least `W` and the
//! window is open, which it is only after the raise is replicated; and when a
//! majority supports `W` at that moment. The gate of that answer, `T_grant +
//! W + 2M(W)`, is recorded in the same transaction. A lowering runs in a
//! single-writer transaction too, only while no window is opening or open,
//! and only once that gate has passed, so no `W` answer can follow it without
//! a new raise.

use super::NodeHub;
use crate::audit::{self, Channel, Outcome, Record};
use crate::authority::{DEFAULT_DURATION_MS, MAX_DURATION_MS, margin_ms};
use crate::changelog::Actor;
use crate::leases::{self, Lease};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

/// `W` when the command names none: 180 s (section 8.11).
pub const DEFAULT_WINDOW_S: u64 = 180;

/// The shortest `W`: `D` (section 8.11).
pub const MIN_WINDOW_S: u64 = DEFAULT_DURATION_MS.div_ceil(1_000);

/// The longest `W`: 600 s, the longest `duration_ms` a grant may carry
/// (sections 5.12 and 8.11).
pub const MAX_WINDOW_S: u64 = MAX_DURATION_MS / 1_000;

/// How often the opening checks its progress.
const POLL: Duration = Duration::from_millis(50);

/// How often the primary looks for stored durations to lower after a window
/// ([`NodeHub::spawn_duration_lowering`]).
const LOWER_TICK: Duration = Duration::from_secs(5);

/// The bound of each step of the opening: `D / 3` plus one margin, the bound
/// the command applies to a holder's renewal and, by ADR 0289, to the
/// replication of the raise. The contract gives the wait for a majority no
/// bound of its own; it gets the same one (one `support_request` round,
/// `S / 3`, plus `M_S` is the same 6.5 s with the defaults).
#[must_use]
pub fn step_bound() -> Duration {
    Duration::from_millis(DEFAULT_DURATION_MS / 3 + margin_ms(DEFAULT_DURATION_MS))
}

/// Why a window did not open, or the command did not succeed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintenanceError {
    /// `W` outside `D` to 600 s.
    OutOfRange { duration_s: u64 },
    /// A window is already opening or open.
    Busy,
    /// This primary demoted itself (section 7.4).
    Demoted,
    /// No majority of voters supported `W` within [`step_bound`].
    NoMajority { duration_s: u64 },
    /// The raise was not replicated within [`step_bound`]; `waiting` lists
    /// the machines of the required set that had not acknowledged it.
    NotReplicated { waiting: Vec<String> },
    /// The window opened, but these leases' holders did not renew within
    /// [`step_bound`]. The window stays open until it closes.
    NotRenewed {
        /// `(lease_id, holder)` of each lease not answered with `W`.
        missing: Vec<(String, String)>,
        /// How many leases were answered with `W`.
        answered: usize,
        /// When the window closes, RFC 3339.
        closes_at: String,
    },
    /// `--end` ended the window before the command's work was done.
    Ended,
    /// A database or audit write failed.
    Internal(String),
}

impl std::fmt::Display for MaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bound = step_bound().as_millis();
        match self {
            Self::OutOfRange { duration_s } => write!(
                f,
                "a maintenance window of {duration_s} s is out of range: it must be {MIN_WINDOW_S} \
                 to {MAX_WINDOW_S} s"
            ),
            Self::Busy => f.write_str(
                "a maintenance window is already opening or open; end it with \
                 'quoro cluster maintenance --end' first",
            ),
            Self::Demoted => f.write_str(
                "this primary has demoted itself: a higher term exists, so it opens no window",
            ),
            Self::NoMajority { duration_s } => write!(
                f,
                "no majority of voters supported a {duration_s} s lease within {bound} ms; the \
                 window did not open"
            ),
            Self::NotReplicated { waiting } => write!(
                f,
                "the raise of the lease durations was not replicated within {bound} ms (waiting \
                 on {}); the window did not open",
                if waiting.is_empty() {
                    "the required set".to_string()
                } else {
                    waiting.join(", ")
                }
            ),
            Self::NotRenewed {
                missing,
                answered,
                closes_at,
            } => write!(
                f,
                "the window is open until {closes_at}, but {} expiring lease(s) had no renewal \
                 answered with it within {bound} ms ({}); {answered} lease(s) answered keep their \
                 longer validity",
                missing.len(),
                missing
                    .iter()
                    .map(|(lease, holder)| format!("{lease} on {holder}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Ended => {
                f.write_str("the window was ended with --end before every lease was covered")
            }
            Self::Internal(message) => f.write_str(message),
        }
    }
}

/// A window that opened and covered every one of its leases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Opened {
    /// `W`, in seconds.
    pub duration_s: u64,
    /// How many `expiring` leases the window covers.
    pub leases: usize,
    /// Until about when every one of them is valid on its holder without
    /// another answer: the earliest `T_grant + W - M(W)` of their `W`
    /// answers, RFC 3339. `None` when there was no lease to cover.
    pub covered_until: Option<String>,
    /// When the window closes, RFC 3339.
    pub closes_at: String,
}

/// What `--end` found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ending {
    /// A window was opening or open, and it is closed now.
    Ended,
    /// No window was opening or open.
    NothingOpen,
}

/// One lease of an open window.
#[derive(Clone, Debug)]
struct Target {
    holder: String,
    /// The wall-clock time of the newest `W` answer.
    answered: Option<time::OffsetDateTime>,
}

#[derive(Debug, Default)]
enum Phase {
    #[default]
    Idle,
    Opening {
        w_ms: u64,
    },
    Open {
        w_ms: u64,
        /// On the authority's clock.
        closes_at: Duration,
        closes_wall: time::OffsetDateTime,
        leases: BTreeMap<String, Target>,
    },
}

#[derive(Debug, Default)]
struct State {
    /// Bumped by every opening, so an opening that `--end` interrupted
    /// never acts on a later one.
    epoch: u64,
    phase: Phase,
}

/// Where the progress of an open window stands.
struct Progress {
    missing: Vec<(String, String)>,
    answered: Vec<time::OffsetDateTime>,
    w_ms: u64,
    closes_wall: time::OffsetDateTime,
}

/// The primary's maintenance window, in memory only ([module docs](self)).
#[derive(Debug)]
pub(super) struct Window {
    state: Mutex<State>,
    /// The `max_lease_ms` that `support_request`s carry. Every session
    /// watches it and sends a request at once when it changes.
    lease: watch::Sender<u64>,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            lease: watch::channel(DEFAULT_DURATION_MS).0,
        }
    }
}

impl Window {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Closes an open window whose `W` has run out at `now`.
    fn expire(&self, state: &mut State, now: Duration) {
        if let Phase::Open { closes_at, .. } = state.phase
            && now >= closes_at
        {
            state.phase = Phase::Idle;
            self.lease.send_replace(DEFAULT_DURATION_MS);
        }
    }

    /// The `max_lease_ms` a `support_request` carries at `now`: `W` while a
    /// window is opening or open, `D` otherwise (section 5.27).
    pub(super) fn support_ms(&self, now: Duration) -> u64 {
        let mut state = self.state();
        self.expire(&mut state, now);
        match state.phase {
            Phase::Idle => DEFAULT_DURATION_MS,
            Phase::Opening { w_ms } | Phase::Open { w_ms, .. } => w_ms,
        }
    }

    /// Changes whenever the `max_lease_ms` of the support requests does at
    /// an opening, its failure or `--end`. A window that runs out changes it
    /// at the next read of [`Window::support_ms`], which each request makes.
    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.lease.subscribe()
    }

    /// Whether no window is opening or open at `now`.
    pub(super) fn is_idle(&self, now: Duration) -> bool {
        let mut state = self.state();
        self.expire(&mut state, now);
        matches!(state.phase, Phase::Idle)
    }

    /// Starts opening a window of `w_ms`: refused while one is opening or
    /// open. Returns the opening's epoch.
    fn begin(&self, w_ms: u64, now: Duration) -> Option<u64> {
        let mut state = self.state();
        self.expire(&mut state, now);
        if !matches!(state.phase, Phase::Idle) {
            return None;
        }
        state.epoch += 1;
        state.phase = Phase::Opening { w_ms };
        self.lease.send_replace(w_ms);
        Some(state.epoch)
    }

    /// Whether the opening `epoch` is still opening or open.
    fn still(&self, epoch: u64, now: Duration) -> bool {
        let mut state = self.state();
        self.expire(&mut state, now);
        state.epoch == epoch && !matches!(state.phase, Phase::Idle)
    }

    /// The opening `epoch` failed: no window opens.
    fn abort(&self, epoch: u64) {
        let mut state = self.state();
        if state.epoch == epoch && matches!(state.phase, Phase::Opening { .. }) {
            state.phase = Phase::Idle;
            self.lease.send_replace(DEFAULT_DURATION_MS);
        }
    }

    /// Opens the window of `epoch` at `now` for `leases`. Returns `false`
    /// when `--end` ended it first.
    fn open(
        &self,
        epoch: u64,
        now: Duration,
        now_wall: time::OffsetDateTime,
        leases: BTreeMap<String, Target>,
    ) -> bool {
        let mut state = self.state();
        let Phase::Opening { w_ms } = state.phase else {
            return false;
        };
        if state.epoch != epoch {
            return false;
        }
        let w = Duration::from_millis(w_ms);
        state.phase = Phase::Open {
            w_ms,
            closes_at: now + w,
            closes_wall: now_wall + w,
            leases,
        };
        true
    }

    /// `W` for a renewal of `lease_id` by `holder` at `now`, when the window
    /// is open and the lease is one of its leases held by `holder`.
    fn answer_ms(&self, lease_id: &str, holder: &str, now: Duration) -> Option<u64> {
        let mut state = self.state();
        self.expire(&mut state, now);
        match &state.phase {
            Phase::Open { w_ms, leases, .. } => leases
                .get(lease_id)
                .filter(|target| target.holder == holder)
                .map(|_| *w_ms),
            Phase::Idle | Phase::Opening { .. } => None,
        }
    }

    /// Records that a renewal of `lease_id` by `holder` was answered with
    /// `w_ms` at `wall`.
    fn answered(
        &self,
        lease_id: &str,
        holder: &str,
        w_ms: u64,
        wall: time::OffsetDateTime,
        now: Duration,
    ) {
        let mut state = self.state();
        self.expire(&mut state, now);
        if let Phase::Open {
            w_ms: open_ms,
            leases,
            ..
        } = &mut state.phase
            && *open_ms == w_ms
            && let Some(target) = leases.get_mut(lease_id)
            && target.holder == holder
        {
            target.answered = Some(wall);
        }
    }

    /// Where the window of `epoch` stands, `None` once it is closed.
    fn progress(&self, epoch: u64, now: Duration) -> Option<Progress> {
        let mut state = self.state();
        self.expire(&mut state, now);
        if state.epoch != epoch {
            return None;
        }
        let Phase::Open {
            w_ms,
            closes_wall,
            leases,
            ..
        } = &state.phase
        else {
            return None;
        };
        let mut progress = Progress {
            missing: Vec::new(),
            answered: Vec::new(),
            w_ms: *w_ms,
            closes_wall: *closes_wall,
        };
        for (lease_id, target) in leases {
            match target.answered {
                Some(wall) => progress.answered.push(wall),
                None => progress
                    .missing
                    .push((lease_id.clone(), target.holder.clone())),
            }
        }
        Some(progress)
    }

    /// `--end`: closes a window that is opening or open.
    fn end(&self, now: Duration) -> Ending {
        let mut state = self.state();
        self.expire(&mut state, now);
        if matches!(state.phase, Phase::Idle) {
            return Ending::NothingOpen;
        }
        state.phase = Phase::Idle;
        self.lease.send_replace(DEFAULT_DURATION_MS);
        Ending::Ended
    }
}

/// The actor of the window's change-log writes and audit records
/// (`audit-log-format.md` section 3.7: `actor_id` `cluster`).
fn actor() -> Actor {
    Actor::system("cluster")
}

fn rfc3339(wall: time::OffsetDateTime) -> String {
    crate::store::format_rfc3339(wall)
}

impl NodeHub {
    /// `quoro cluster maintenance --duration <duration_s>` ([module
    /// docs](self)). Returns once every `expiring` lease of the window had a
    /// renewal answered with `W`, or with the reason the command fails.
    ///
    /// # Errors
    ///
    /// [`MaintenanceError`]: out of range, busy and demoted refuse before
    /// anything starts and write no audit record; the others follow section
    /// 8.11.
    pub async fn open_maintenance(&self, duration_s: u64) -> Result<Opened, MaintenanceError> {
        if !(MIN_WINDOW_S..=MAX_WINDOW_S).contains(&duration_s) {
            return Err(MaintenanceError::OutOfRange { duration_s });
        }
        if self.0.store.is_demoted() {
            return Err(MaintenanceError::Demoted);
        }
        let w_ms = duration_s * 1_000;
        let window = &self.0.maintenance;
        let authority = &self.0.authority;
        let Some(epoch) = window.begin(w_ms, authority.now()) else {
            return Err(MaintenanceError::Busy);
        };
        tracing::info!(duration_s, "opening a maintenance window (section 8.11)");
        let params = json!({ "duration_s": duration_s });
        let bound = step_bound();

        // 1. A majority supports `W`. The sessions ask at once.
        let asked = authority.now();
        loop {
            if !window.still(epoch, authority.now()) {
                return self.not_opened(&params, MaintenanceError::Ended).await;
            }
            if authority.supports(w_ms) {
                break;
            }
            if authority.now() >= asked + bound {
                window.abort(epoch);
                return self
                    .not_opened(&params, MaintenanceError::NoMajority { duration_s })
                    .await;
            }
            tokio::time::sleep(POLL).await;
        }

        // 2. The raise.
        let (leases, seq) = match self.raise_for_window(epoch, w_ms).await {
            Ok(Some(raised)) => raised,
            Ok(None) => return self.not_opened(&params, MaintenanceError::Ended).await,
            Err(message) => {
                window.abort(epoch);
                return self
                    .not_opened(&params, MaintenanceError::Internal(message))
                    .await;
            }
        };

        // 3. The raise is replicated over the required set (ADR 0268, ADR
        //    0289).
        if let Some(seq) = seq {
            let raised = authority.now();
            loop {
                if !window.still(epoch, authority.now()) {
                    return self.not_opened(&params, MaintenanceError::Ended).await;
                }
                let waiting = match self.raise_replicated(seq).await {
                    Ok(()) => break,
                    Err(waiting) => waiting,
                };
                if authority.now() >= raised + bound {
                    window.abort(epoch);
                    return self
                        .not_opened(&params, MaintenanceError::NotReplicated { waiting })
                        .await;
                }
                tokio::time::sleep(POLL).await;
            }
        }

        // 4. Open, and record it.
        if !window.open(
            epoch,
            authority.now(),
            time::OffsetDateTime::now_utc(),
            leases,
        ) {
            return self.not_opened(&params, MaintenanceError::Ended).await;
        }
        let change_seq = seq.and_then(|seq| i64::try_from(seq).ok());
        if let Err(message) = self
            .audit_maintenance(params, Outcome::Ok, None, change_seq)
            .await
        {
            // No record, no window: the command fails and renewals go back
            // to `D`; the leases already answered keep their gates.
            window.end(authority.now());
            return Err(MaintenanceError::Internal(message));
        }
        tracing::info!(duration_s, "the maintenance window is open");

        // 5. Every lease of the window had a renewal answered with `W`.
        let opened = authority.now();
        loop {
            let Some(progress) = window.progress(epoch, authority.now()) else {
                return Err(MaintenanceError::Ended);
            };
            if progress.missing.is_empty() {
                let w = Duration::from_millis(progress.w_ms);
                let valid = w.saturating_sub(Duration::from_millis(margin_ms(progress.w_ms)));
                return Ok(Opened {
                    duration_s,
                    leases: progress.answered.len(),
                    covered_until: progress
                        .answered
                        .iter()
                        .min()
                        .map(|first| rfc3339(*first + valid)),
                    closes_at: rfc3339(progress.closes_wall),
                });
            }
            if authority.now() >= opened + bound {
                return Err(MaintenanceError::NotRenewed {
                    missing: progress.missing,
                    answered: progress.answered.len(),
                    closes_at: rfc3339(progress.closes_wall),
                });
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// `quoro cluster maintenance --end`: closes a window that is opening or
    /// open, and records it with `params` `{end: true}`.
    ///
    /// # Errors
    ///
    /// The audit record could not be written; the window is closed anyway.
    pub async fn end_maintenance(&self) -> Result<Ending, MaintenanceError> {
        let ending = self.0.maintenance.end(self.0.authority.now());
        if ending == Ending::Ended {
            tracing::info!("the maintenance window was ended with --end");
            self.audit_maintenance(json!({ "end": true }), Outcome::Ok, None, None)
                .await
                .map_err(MaintenanceError::Internal)?;
        }
        Ok(ending)
    }

    /// Writes the `error` record of a window that did not open and returns
    /// `error`. A failure to write the record is logged: the command fails
    /// either way.
    async fn not_opened<T>(
        &self,
        params: &Value,
        error: MaintenanceError,
    ) -> Result<T, MaintenanceError> {
        tracing::warn!(%error, "the maintenance window did not open");
        if let Err(message) = self
            .audit_maintenance(
                params.clone(),
                Outcome::Error,
                Some(error.to_string()),
                None,
            )
            .await
        {
            tracing::error!(%message, "cannot write the cluster.maintenance record");
        }
        Err(error)
    }

    /// Appends one `cluster.maintenance` record (`audit-log-format.md`
    /// section 3.7).
    async fn audit_maintenance(
        &self,
        params: Value,
        result: Outcome,
        result_detail: Option<String>,
        change_seq: Option<i64>,
    ) -> Result<(), String> {
        let record = Record {
            actor: actor(),
            agent_model: None,
            on_behalf_of: None,
            channel: Channel::Cli,
            operation: "cluster.maintenance".to_string(),
            scope: None,
            target_type: Some("cluster".to_string()),
            target_id: Some(self.0.cluster_id.clone()),
            project_id: None,
            params,
            justification: None,
            trigger_event_id: None,
            incident_id: None,
            grant_id: None,
            approval_id: None,
            request_id: quoro_types::new_id(quoro_types::Prefix::Request),
            result,
            result_detail,
            change_seq,
        };
        let mut w = self
            .0
            .store
            .begin_write()
            .await
            .map_err(|e| format!("cannot write the cluster.maintenance record: {e}"))?;
        audit::append(&mut w.tx, &record)
            .await
            .map_err(|e| format!("cannot write the cluster.maintenance record: {e}"))?;
        w.commit()
            .await
            .map_err(|e| format!("cannot write the cluster.maintenance record: {e}"))
    }

    /// Step 2: raises `leases.duration_ms` to `w_ms` for every row that reads
    /// `expiring`, unmarked and not revoked, in one single-writer
    /// transaction. Returns those rows' holders and the newest change-log
    /// write among them, which must be replicated before any `W` answer;
    /// `None` when `--end` ended the opening first.
    ///
    /// A row the change log has no entry of, which only a database written
    /// outside the primary can hold, is left out: no successor could count
    /// its gate, so it gets no `W`.
    async fn raise_for_window(
        &self,
        epoch: u64,
        w_ms: u64,
    ) -> Result<Option<(BTreeMap<String, Target>, Option<u64>)>, String> {
        let read = |e: crate::error::ApiError| format!("cannot raise the lease durations: {e:?}");
        let mut w = self
            .0
            .store
            .begin_write()
            .await
            .map_err(|e| format!("cannot raise the lease durations: {e}"))?;
        // Checked under the writer, so no lowering runs between this and the
        // raise ([`NodeHub::lower_durations`]).
        if !self.0.maintenance.still(epoch, self.0.authority.now()) {
            return Ok(None);
        }
        let rows = sqlx::query(&format!(
            "SELECT {} FROM leases WHERE mode = 'expiring' AND unsettled = 0 \
             AND revoked_at IS NULL ORDER BY id",
            leases::LEASE_COLUMNS
        ))
        .fetch_all(&mut *w.tx)
        .await
        .map_err(|e| format!("cannot read the expiring leases: {e}"))?;
        let rows: Vec<Lease> = rows
            .iter()
            .map(leases::lease_from_row)
            .collect::<Result<_, _>>()
            .map_err(read)?;
        let stored = i64::try_from(w_ms).unwrap_or(i64::MAX);
        let mut targets = BTreeMap::new();
        let mut newest: Option<u64> = None;
        for row in rows {
            leases::raise_duration(&mut w, &row.id, stored, &actor())
                .await
                .map_err(read)?;
            let Some(seq) = leases::newest_write_seq(&mut w.tx, &row.id)
                .await
                .map_err(read)?
            else {
                tracing::warn!(
                    lease_id = %row.id,
                    "an expiring lease has no change-log entry; the maintenance window does not cover it"
                );
                continue;
            };
            newest = Some(newest.map_or(seq, |newest| newest.max(seq)));
            targets.insert(
                row.id,
                Target {
                    holder: row.holder_node_id,
                    answered: None,
                },
            );
        }
        w.commit()
            .await
            .map_err(|e| format!("cannot raise the lease durations: {e}"))?;
        Ok(Some((targets, newest)))
    }

    /// Step 3: whether `seq` is replicated over the required set, or the
    /// machines it waits on.
    async fn raise_replicated(&self, seq: u64) -> Result<(), Vec<String>> {
        let mut conn = match self.0.store.pool().acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(%e, "cannot read whether the raise is replicated");
                return Err(Vec::new());
            }
        };
        match self.replicated(&mut conn, seq).await {
            Ok(replicated) => replicated,
            Err(e) => {
                tracing::warn!(%e, "cannot read whether the raise is replicated");
                Err(Vec::new())
            }
        }
    }

    /// The `duration_ms` an ordinary answer to `holder`'s renewal of `lease`
    /// carries while a window is open: `W` when the lease is one of the
    /// window's, held by `holder`, its row as the answer's transaction reads
    /// it still `expiring`, unmarked, unrevoked and storing at least `W`,
    /// and a majority supports `W` now. `None` means `D`.
    pub(super) fn window_answer_ms(&self, lease: &Lease, holder: &str) -> Option<u64> {
        if lease.mode != "expiring" || lease.unsettled || lease.is_revoked() {
            return None;
        }
        let w_ms = self
            .0
            .maintenance
            .answer_ms(&lease.id, holder, self.0.authority.now())?;
        let stored = lease.duration_ms.and_then(|ms| u64::try_from(ms).ok())?;
        (stored >= w_ms && self.0.authority.supports(w_ms)).then_some(w_ms)
    }

    /// Records that `holder`'s renewal of `lease_id` was answered with
    /// `duration_ms` at `sent_wall`, after the answer's transaction
    /// committed. Only a `W` answer counts toward the command's success.
    pub(super) fn window_answered(
        &self,
        lease_id: &str,
        holder: &str,
        duration_ms: u64,
        sent_wall: time::OffsetDateTime,
    ) {
        if duration_ms > DEFAULT_DURATION_MS {
            self.0.maintenance.answered(
                lease_id,
                holder,
                duration_ms,
                sent_wall,
                self.0.authority.now(),
            );
        }
    }

    /// The `max_lease_ms` a `support_request` carries now (section 5.27).
    pub(super) fn support_lease_ms(&self) -> u64 {
        self.0.maintenance.support_ms(self.0.authority.now())
    }

    /// Lowers the stored `duration_ms` back to `D` for every row that reads
    /// `expiring`, unmarked and unrevoked, stores more, has a gate in this
    /// process, and whose every grant longer than `D` has passed its gate
    /// ([`crate::authority::Authority::may_lower`]); only while no window is
    /// opening or open. Each row is checked again and lowered in its own
    /// single-writer transaction, where every `W` answer records its gate
    /// too. Returns how many rows were lowered.
    pub async fn lower_durations(&self) -> usize {
        let authority = &self.0.authority;
        if !self.0.maintenance.is_idle(authority.now()) {
            return 0;
        }
        let d = i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX);
        let ids: Vec<String> = match sqlx::query_scalar(
            "SELECT id FROM leases WHERE mode = 'expiring' AND unsettled = 0 \
             AND revoked_at IS NULL AND duration_ms > ? ORDER BY id",
        )
        .bind(d)
        .fetch_all(self.0.store.pool())
        .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(%e, "cannot read the lease durations to lower");
                return 0;
            }
        };
        let mut lowered = 0;
        for id in ids {
            if !authority.has_gate(&id) || !authority.may_lower(&id, DEFAULT_DURATION_MS) {
                continue;
            }
            let mut w = match self.0.store.begin_write().await {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!(%e, "cannot lower a lease duration");
                    return lowered;
                }
            };
            if !self.0.maintenance.is_idle(authority.now()) {
                return lowered;
            }
            let row = match leases::by_id(&mut w.tx, &id).await {
                Ok(Some(row)) => row,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(code = %e.code.as_str(), lease_id = %id, "cannot read a lease to lower");
                    continue;
                }
            };
            if row.mode != "expiring" || row.unsettled || row.is_revoked() {
                continue;
            }
            match leases::lower_duration(&mut w, authority, &id, d, &actor()).await {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(code = %e.code.as_str(), lease_id = %id, "cannot lower a lease duration");
                    continue;
                }
            }
            match w.commit().await {
                Ok(()) => {
                    lowered += 1;
                    tracing::info!(lease_id = %id, "the lease's stored duration is back to D");
                }
                Err(e) => tracing::warn!(%e, lease_id = %id, "cannot lower a lease duration"),
            }
        }
        lowered
    }

    /// Runs [`NodeHub::lower_durations`] every few seconds while the hub
    /// lives and the primary is not demoted. `quoro serve` starts it.
    pub fn spawn_duration_lowering(&self) {
        let hub = std::sync::Arc::downgrade(&self.0);
        tokio::spawn(async move {
            loop {
                {
                    let Some(inner) = hub.upgrade() else {
                        break;
                    };
                    if inner.store.stops_for_demotion("the duration lowering") {
                        break;
                    }
                    NodeHub(inner).lower_durations().await;
                }
                tokio::time::sleep(LOWER_TICK).await;
            }
        });
    }
}

#[cfg(test)]
mod tests;
