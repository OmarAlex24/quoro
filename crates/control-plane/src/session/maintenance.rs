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
//!    ([`NodeHub::window_answer_ms`]), as long as the row still reads
//!    `expiring`, unmarked, unrevoked and storing at least `W`, and a majority
//!    still supports `W`; otherwise it carries `D`.
//!
//! **Success.** The command succeeds once the newest answer to every one of
//! the window's leases carried `W` (a holder counts its validity from its
//! newest answer, so a later `D` answer takes the cover away again), and no
//! other unrevoked lease reads `expiring` or carries the unsettled mark. Such
//! a lease gets only `D` from this window: one that was marked at the raise,
//! one that became `expiring` after it, or one granted again since
//! ([`Why`]). The command fails, naming each lease it does not cover and why,
//! when a holder has not renewed within [`step_bound`] or when such a lease
//! exists. The window then stays open, so the leases already answered keep
//! their longer validity (section 8.11). It waits for the window's own
//! leases before it fails for another one, so that the installer does not
//! restart the primary before they are covered.
//!
//! The primary writes one `cluster.maintenance` audit record per window it
//! tries to open: `ok` once the window opens, `error` with the reason when it
//! does not (`audit-log-format.md` section 3.7). A window that opened but does
//! not cover every lease still records `ok`, as the contract defines it. The
//! `ok` record's `change_seq` is the newest change-log write of the raise,
//! absent when the raise wrote nothing. `--end` writes another record, with
//! `params` `{end: true}`.
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
use sqlx::SqliteConnection;
use std::collections::{BTreeMap, BTreeSet};
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
    /// The window opened, but it does not cover these leases. The window
    /// stays open until it closes.
    NotCovered {
        /// Each lease not covered, and why.
        leases: Vec<Uncovered>,
        /// How many leases had their newest renewal answered with `W`.
        answered: usize,
        /// When the window closes, RFC 3339.
        closes_at: String,
    },
    /// `--end` ended the window before the command's work was done: while it
    /// was opening (`opened` false), or once it was open, before every lease
    /// was covered.
    Ended { opened: bool },
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
            Self::NotCovered {
                leases,
                answered,
                closes_at,
            } => {
                write!(
                    f,
                    "the window is open until {closes_at}, but it does not cover {} lease(s); the \
                     {answered} lease(s) answered with it keep their longer validity:",
                    leases.len()
                )?;
                for lease in leases {
                    write!(
                        f,
                        "\n  {} on {}: {}",
                        lease.lease_id, lease.holder, lease.why
                    )?;
                }
                Ok(())
            }
            Self::Ended { opened: false } => {
                f.write_str("the window was ended with --end while it was opening; it did not open")
            }
            Self::Ended { opened: true } => f.write_str(
                "the window was ended with --end before every lease had a renewal answered with it",
            ),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

/// A lease the window does not cover ([`MaintenanceError::NotCovered`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uncovered {
    pub lease_id: String,
    /// The holder the row names, or, for one of the window's leases its
    /// holder has not renewed, the holder the window raised it for.
    pub holder: String,
    pub why: Why,
}

/// Why the window does not cover a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// One of the window's leases whose holder has not renewed within
    /// [`step_bound`].
    NotRenewed,
    /// One of the window's leases whose newest renewal was answered with
    /// `D`, which its holder counts its validity from: no majority supported
    /// `W` at that moment, or its row changed as the variants below say.
    AnsweredWithD,
    /// It carries the unsettled mark, so its renewals are answered with `D`.
    Marked,
    /// It was not an unmarked `expiring` lease when the window raised the
    /// durations, so its renewals are answered with `D`.
    NotRaised,
    /// Its row had no change-log entry at the raise, so no successor could
    /// count a longer gate, and its renewals are answered with `D`.
    NoLogEntry,
    /// Its row names another holder than the one the window raised it for.
    OtherHolder,
    /// Its stored duration went back below `W`, so its renewals are answered
    /// with `D`.
    StoredBelowW,
}

impl std::fmt::Display for Why {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotRenewed => write!(
                f,
                "its holder did not renew within {} ms",
                step_bound().as_millis()
            ),
            Self::AnsweredWithD => f.write_str(
                "its newest renewal was answered with D, which its holder counts its validity from",
            ),
            Self::Marked => {
                f.write_str("it carries the unsettled mark, so its renewals are answered with D")
            }
            Self::NotRaised => f.write_str(
                "it was not an unmarked expiring lease when the window raised the durations, so \
                 its renewals are answered with D",
            ),
            Self::NoLogEntry => {
                f.write_str("its row has no change-log entry, so its renewals are answered with D")
            }
            Self::OtherHolder => f.write_str(
                "it was granted to another node after the window raised it, so its renewals are \
                 answered with D",
            ),
            Self::StoredBelowW => f.write_str(
                "its stored duration went back below the window's, so its renewals are answered \
                 with D",
            ),
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
    /// another answer: the earliest `T_grant + W - 2M(W)` of their `W`
    /// answers, RFC 3339. The holder counts `W - M(W)` from the moment it
    /// sent the renewal, which is before `T_grant`; the second margin keeps
    /// the printed time on the early side. `None` when there was no lease to
    /// cover.
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
    /// The wall-clock time of the newest answer to its holder's renewal,
    /// when that answer carried `W`.
    answered: Option<time::OffsetDateTime>,
    /// Whether the newest answer carried less than `W`: the holder counts
    /// its validity from its newest answer (section 8.4), so the lease is not
    /// covered until another answer carries `W`.
    short: bool,
}

impl Target {
    fn new(holder: String) -> Self {
        Self {
            holder,
            answered: None,
            short: false,
        }
    }
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
    /// The window's leases whose newest answer did not carry `W`.
    missing: Vec<Uncovered>,
    /// The time of each `W` answer that is its lease's newest answer.
    answered: Vec<time::OffsetDateTime>,
    /// Each of the window's leases and the holder it was raised for.
    holders: BTreeMap<String, String>,
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

    /// Closes the window of `epoch`, opening or open: its opening failed, or
    /// its `ok` record could not be written. A later window is left as it is.
    fn close(&self, epoch: u64) {
        let mut state = self.state();
        if state.epoch == epoch && !matches!(state.phase, Phase::Idle) {
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
    /// `duration_ms` at `wall`. An answer with `W` covers the lease; any other
    /// takes the cover of an earlier one away.
    fn answered(
        &self,
        lease_id: &str,
        holder: &str,
        duration_ms: u64,
        wall: time::OffsetDateTime,
        now: Duration,
    ) {
        let mut state = self.state();
        self.expire(&mut state, now);
        if let Phase::Open { w_ms, leases, .. } = &mut state.phase
            && let Some(target) = leases.get_mut(lease_id)
            && target.holder == holder
        {
            let covers = duration_ms == *w_ms;
            target.answered = covers.then_some(wall);
            target.short = !covers;
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
            holders: BTreeMap::new(),
            w_ms: *w_ms,
            closes_wall: *closes_wall,
        };
        for (lease_id, target) in leases {
            progress
                .holders
                .insert(lease_id.clone(), target.holder.clone());
            match target.answered {
                Some(wall) => progress.answered.push(wall),
                None => progress.missing.push(Uncovered {
                    lease_id: lease_id.clone(),
                    holder: target.holder.clone(),
                    why: if target.short {
                        Why::AnsweredWithD
                    } else {
                        Why::NotRenewed
                    },
                }),
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

/// What the raise of an opening found ([`NodeHub::raise_for_window`]).
struct Raised {
    /// The window's leases.
    targets: BTreeMap<String, Target>,
    /// The newest change-log write among them, the raise included: the one
    /// that must be replicated before any `W` answer.
    newest: Option<u64>,
    /// The newest change-log write the raise itself made, the `ok` record's
    /// `change_seq`; `None` when every row already stored at least `W`.
    wrote: Option<u64>,
    /// The `expiring` rows left out for having no change-log entry.
    unlogged: BTreeSet<String>,
}

/// Why the window does not answer the renewals of `row`, one of its leases,
/// with `W`, as far as the row decides it: it carries the unsettled mark, or
/// it stores less than `W`. `None` when the row lets the answer carry `W`.
fn row_short_of_w(row: &Lease, w_ms: u64) -> Option<Why> {
    if row.unsettled {
        return Some(Why::Marked);
    }
    let stored = row.duration_ms.and_then(|ms| u64::try_from(ms).ok());
    if !stored.is_some_and(|stored| stored >= w_ms) {
        return Some(Why::StoredBelowW);
    }
    None
}

/// The leases a window of `w_ms` does not cover, as `conn` reads them: every
/// unrevoked row that reads `expiring` or carries the unsettled mark and whose
/// renewals the window does not answer with `W` (section 8.11: the command
/// succeeds once every `expiring` lease had a `W` answer). `holders` maps each
/// of the window's leases to the holder it was raised for; `unlogged` names
/// the rows the raise left out for having no change-log entry. A revoked row,
/// and an unmarked `local` one, need no cover.
async fn uncovered(
    conn: &mut SqliteConnection,
    holders: &BTreeMap<String, String>,
    unlogged: &BTreeSet<String>,
    w_ms: u64,
) -> Result<Vec<Uncovered>, String> {
    let rows = sqlx::query(&format!(
        "SELECT {} FROM leases WHERE revoked_at IS NULL \
         AND (mode = 'expiring' OR unsettled = 1) ORDER BY id",
        leases::LEASE_COLUMNS
    ))
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| format!("cannot read the leases the window must cover: {e}"))?;
    let mut found = Vec::new();
    for row in &rows {
        let row = leases::lease_from_row(row)
            .map_err(|e| format!("cannot read the leases the window must cover: {e:?}"))?;
        let why = match holders.get(&row.id) {
            Some(holder) if *holder != row.holder_node_id => Some(Why::OtherHolder),
            Some(_) => row_short_of_w(&row, w_ms),
            None if row.unsettled => Some(Why::Marked),
            None if unlogged.contains(&row.id) => Some(Why::NoLogEntry),
            None => Some(Why::NotRaised),
        };
        if let Some(why) = why {
            found.push(Uncovered {
                lease_id: row.id,
                holder: row.holder_node_id,
                why,
            });
        }
    }
    Ok(found)
}

/// Whether `row`'s stored duration may go back to `D`, as far as the row
/// decides it: it reads `expiring`, carries no unsettled mark, is not revoked
/// and stores more than `D`. A marked or revoked row keeps its longer
/// duration, the side that only waits longer.
fn lowerable(row: &Lease) -> bool {
    let d = i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX);
    row.mode == "expiring"
        && !row.unsettled
        && !row.is_revoked()
        && row.duration_ms.is_some_and(|stored| stored > d)
}

impl NodeHub {
    /// `quoro cluster maintenance --duration <duration_s>` ([module
    /// docs](self)). Returns once the newest answer to every lease of the
    /// window carried `W` and no other lease needs cover, or with the reason
    /// the command fails.
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
                return self
                    .not_opened(&params, MaintenanceError::Ended { opened: false })
                    .await;
            }
            if authority.supports(w_ms) {
                break;
            }
            if authority.now() >= asked + bound {
                window.close(epoch);
                return self
                    .not_opened(&params, MaintenanceError::NoMajority { duration_s })
                    .await;
            }
            tokio::time::sleep(POLL).await;
        }

        // 2. The raise.
        let raised = match self.raise_for_window(epoch, w_ms).await {
            Ok(Some(raised)) => raised,
            Ok(None) => {
                return self
                    .not_opened(&params, MaintenanceError::Ended { opened: false })
                    .await;
            }
            Err(message) => {
                window.close(epoch);
                return self
                    .not_opened(&params, MaintenanceError::Internal(message))
                    .await;
            }
        };
        let Raised {
            targets,
            newest,
            wrote,
            unlogged,
        } = raised;

        // 3. The raise is replicated over the required set (ADR 0268, ADR
        //    0289).
        if let Some(seq) = newest {
            let raised_at = authority.now();
            loop {
                if !window.still(epoch, authority.now()) {
                    return self
                        .not_opened(&params, MaintenanceError::Ended { opened: false })
                        .await;
                }
                let waiting = match self.raise_replicated(seq).await {
                    Ok(()) => break,
                    Err(waiting) => waiting,
                };
                if authority.now() >= raised_at + bound {
                    window.close(epoch);
                    return self
                        .not_opened(&params, MaintenanceError::NotReplicated { waiting })
                        .await;
                }
                tokio::time::sleep(POLL).await;
            }
        }

        // 4. Open, and record it. The holders' bound counts from this moment,
        //    not from after the record.
        let opened = authority.now();
        if !window.open(epoch, opened, time::OffsetDateTime::now_utc(), targets) {
            return self
                .not_opened(&params, MaintenanceError::Ended { opened: false })
                .await;
        }
        let change_seq = wrote.and_then(|seq| i64::try_from(seq).ok());
        if let Err(message) = self
            .audit_maintenance(params, Outcome::Ok, None, change_seq)
            .await
        {
            // No record, no window: the command fails and renewals go back
            // to `D`; the leases already answered keep their gates. Only
            // this window is closed, never a later one.
            window.close(epoch);
            return Err(MaintenanceError::Internal(message));
        }
        tracing::info!(duration_s, "the maintenance window is open");

        // 5. The newest answer to every lease of the window carried `W`, and
        //    no other lease needs cover. The window's own leases are waited
        //    for first, so a failure for another lease does not let the
        //    restart start before they are covered.
        loop {
            let Some(progress) = window.progress(epoch, authority.now()) else {
                return Err(MaintenanceError::Ended { opened: true });
            };
            let timed_out = authority.now() >= opened + bound;
            if progress.missing.is_empty() || timed_out {
                let mut not_covered = BTreeMap::new();
                for lease in self
                    .uncovered_now(&progress.holders, &unlogged, progress.w_ms)
                    .await
                    .map_err(MaintenanceError::Internal)?
                {
                    not_covered.insert(lease.lease_id.clone(), lease);
                }
                for lease in progress.missing {
                    not_covered.entry(lease.lease_id.clone()).or_insert(lease);
                }
                if not_covered.is_empty() {
                    let w = Duration::from_millis(progress.w_ms);
                    let margin = Duration::from_millis(margin_ms(progress.w_ms));
                    let valid = w.saturating_sub(margin * 2);
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
                let error = MaintenanceError::NotCovered {
                    leases: not_covered.into_values().collect(),
                    answered: progress.answered.len(),
                    closes_at: rfc3339(progress.closes_wall),
                };
                tracing::warn!(%error, "the maintenance window does not cover every lease");
                return Err(error);
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// The leases the open window does not cover now ([`uncovered`]).
    async fn uncovered_now(
        &self,
        holders: &BTreeMap<String, String>,
        unlogged: &BTreeSet<String>,
        w_ms: u64,
    ) -> Result<Vec<Uncovered>, String> {
        let mut conn = self
            .0
            .store
            .pool()
            .acquire()
            .await
            .map_err(|e| format!("cannot read the leases the window must cover: {e}"))?;
        uncovered(&mut conn, holders, unlogged, w_ms).await
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
    /// transaction. Returns those rows' holders, the newest change-log write
    /// among them, which must be replicated before any `W` answer, and the
    /// newest write the raise made; `None` when `--end` ended the opening
    /// first. In the same transaction it counts the leases the window will
    /// not cover ([`uncovered`]) and logs each one; the command fails for
    /// them once the window's own leases are answered.
    ///
    /// A row the change log has no entry of, which only a database written
    /// outside the primary can hold, is left out: no successor could count
    /// its gate, so it gets no `W`.
    async fn raise_for_window(&self, epoch: u64, w_ms: u64) -> Result<Option<Raised>, String> {
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
            "SELECT {} FROM leases WHERE revoked_at IS NULL ORDER BY id",
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
        let newer = |seq: Option<u64>, other: u64| Some(seq.map_or(other, |seq| seq.max(other)));
        let mut raised = Raised {
            targets: BTreeMap::new(),
            newest: None,
            wrote: None,
            unlogged: BTreeSet::new(),
        };
        // Only an unmarked `expiring` row is one of the window's leases: a
        // marked one is between modes (section 8.4, "The mark"), and its
        // renewals keep getting `D`.
        for row in rows
            .into_iter()
            .filter(|row| row.mode == "expiring" && !row.unsettled)
        {
            let wrote = leases::raise_duration(&mut w, &row.id, stored, &actor())
                .await
                .map_err(read)?
                .is_some();
            let Some(seq) = leases::newest_write_seq(&mut w.tx, &row.id)
                .await
                .map_err(read)?
            else {
                tracing::warn!(
                    lease_id = %row.id,
                    "an expiring lease has no change-log entry; the maintenance window does not cover it"
                );
                raised.unlogged.insert(row.id);
                continue;
            };
            raised.newest = newer(raised.newest, seq);
            if wrote {
                raised.wrote = newer(raised.wrote, seq);
            }
            raised
                .targets
                .insert(row.id, Target::new(row.holder_node_id));
        }
        let holders = raised
            .targets
            .iter()
            .map(|(id, target)| (id.clone(), target.holder.clone()))
            .collect();
        for lease in uncovered(&mut w.tx, &holders, &raised.unlogged, w_ms).await? {
            tracing::warn!(
                lease_id = %lease.lease_id,
                holder = %lease.holder,
                why = %lease.why,
                "the maintenance window does not cover this lease"
            );
        }
        w.commit()
            .await
            .map_err(|e| format!("cannot raise the lease durations: {e}"))?;
        Ok(Some(raised))
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
    /// it still `expiring`, unmarked, unrevoked and storing at least `W`
    /// ([`row_short_of_w`]), and a majority supports `W` now. `None` means
    /// `D`.
    pub(super) fn window_answer_ms(&self, lease: &Lease, holder: &str) -> Option<u64> {
        if lease.mode != "expiring" || lease.is_revoked() {
            return None;
        }
        let w_ms = self
            .0
            .maintenance
            .answer_ms(&lease.id, holder, self.0.authority.now())?;
        if row_short_of_w(lease, w_ms).is_some() {
            return None;
        }
        self.0.authority.supports(w_ms).then_some(w_ms)
    }

    /// Records that `holder`'s renewal of `lease_id` was answered with
    /// `duration_ms` at `sent_wall`, after the answer's transaction
    /// committed. A `W` answer to one of the window's leases covers it; any
    /// other answer to it takes that cover away, since the holder counts its
    /// validity from its newest answer.
    pub(super) fn window_answered(
        &self,
        lease_id: &str,
        holder: &str,
        duration_ms: u64,
        sent_wall: time::OffsetDateTime,
    ) {
        self.0.maintenance.answered(
            lease_id,
            holder,
            duration_ms,
            sent_wall,
            self.0.authority.now(),
        );
    }

    /// The `max_lease_ms` a `support_request` carries now (section 5.27).
    pub(super) fn support_lease_ms(&self) -> u64 {
        self.0.maintenance.support_ms(self.0.authority.now())
    }

    /// Lowers the stored `duration_ms` back to `D` for every row that reads
    /// `expiring`, unmarked and unrevoked, stores more ([`lowerable`]), has a
    /// gate in this process, and whose every grant longer than `D` has passed
    /// its gate ([`crate::authority::Authority::may_lower`]); only while no
    /// window is opening or open. Each row is checked again and lowered in
    /// its own single-writer transaction, where every `W` answer records its
    /// gate too. Returns how many rows were lowered.
    pub async fn lower_durations(&self) -> usize {
        let authority = &self.0.authority;
        if !self.0.maintenance.is_idle(authority.now()) {
            return 0;
        }
        let d = i64::try_from(DEFAULT_DURATION_MS).unwrap_or(i64::MAX);
        let rows = match sqlx::query(&format!(
            "SELECT {} FROM leases WHERE duration_ms > ? ORDER BY id",
            leases::LEASE_COLUMNS
        ))
        .bind(d)
        .fetch_all(self.0.store.pool())
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(%e, "cannot read the lease durations to lower");
                return 0;
            }
        };
        let ids: Vec<String> = rows
            .iter()
            .filter_map(|row| leases::lease_from_row(row).ok())
            .filter(lowerable)
            .map(|row| row.id)
            .collect();
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
            if !lowerable(&row) || !authority.has_gate(&id) {
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
