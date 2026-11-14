//! Steps 2 to 6 of `quoro cluster promote` (`node-agent-protocol.md`
//! section 7.3; ADR 0182, ADR 0228, ADR 0232, ADR 0236, ADR 0261, ADR 0280,
//! ADR 0285, ADR 0297, ADR 0317, ADR 0325), after the refusals of
//! [`super::refusals`] and step 1. In order:
//!
//! 1. **The rest of the refusals, before any change.** Step 6's host
//!    preparation would accept this host; the keys opened from this
//!    machine's seals pass step 6's checks (`PrimaryKeys::check`); with
//!    `--ack-fence`, no lease the old primary's node holds is at the token
//!    cap (`promotion::check_token_cap` on the fold of the `leases` and
//!    `nodes` entries, issue #931); and the followed log folds and builds,
//!    with every check of step 4's build run in memory
//!    (`rebuild::database::check`).
//! 2. **Step 2's pre-check.** The vote client is made from the agent's newest
//!    stored `spec.votes` and asks every voter with `check` requests, from
//!    one above the highest term this machine has seen. A term above the
//!    ceiling (section 8.8, issue #862), terms that keep rising, and no
//!    majority each stop here, having changed nothing. `--dry-run` stops
//!    here too; it exits 0 only when a majority would grant.
//! 3. **The lock and the freeze.** The promotion lock is taken without
//!    waiting and held to the end, refusal 2 runs again under it, and the
//!    agent is frozen before the first real vote request.
//! 4. **The frozen log, checked again** before the first real vote request:
//!    the freeze holds, every refusal on the log passes on it (ADR 0280),
//!    the keys open from it, the token cap holds, and it folds and builds.
//! 5. **Step 2's vote** at the pre-check's term `T`; no majority stops.
//! 6. **Step 3's wait** until the latest receipt time plus `lease_wait_ms`
//!    over the granting answers, printing the time left, unless
//!    `--ack-fence`.
//! 7. **Step 4.** The frozen log is read again: the freeze held, the log has
//!    not changed since the freeze, every refusal on the log passes, and the
//!    token cap holds with `--ack-fence`. Then the database is built at the
//!    control plane's path, with the identity's cluster id and term `T`.
//! 8. **Step 5**, the one change-log write that moves the primary status.
//! 9. **Step 6**: the keys, the host's preparation and the control plane's
//!    start, the end of the freeze, and the agent pointed at the new
//!    primary.
//!
//! Every vote connection, the pre-check's and the real one's, is opened by
//! this root process before step 6 hands the data directory to the control
//! plane's user, and nothing here changes the process's uid (issue #19):
//! the mesh carries a new connection only from a root process (section
//! 6.9).
//!
//! **If the promotion stops after the freeze** ([`Stopped`]), the freeze
//! ends and the agent dials again, as section 7.3 says. The message says
//! whether this machine's agent granted the real vote, and, once the new
//! database is written, that a rerun is refused until it is moved aside
//! with `quoro node rejoin --force --statement`.
//!
//! Every effect goes through [`Steps`], so the tests run the whole sequence
//! with a recording fake, without root.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use quoro_control_plane::promotion::{self, Promotion, Written};
use quoro_control_plane::rebuild::database::{Built, FollowedEntry};
use quoro_control_plane::rebuild::{self, Entry, Folded};
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::freeze::PromotionReads;
use quoro_node_agent::identity::Identity;
use quoro_node_agent::vote_client::{CheckOutcome, Reply, Round};
use quoro_proto::wire::{SpecVotes, VoteOutcome, VoteRefusal};

use super::command::{Flags, Host, Outcome, Paths};
use super::keys;
use super::refusals::{self, Refusal};
use crate::host::{DATABASE, Out};

/// The `entity_type`s the token-cap check folds: the leases and the nodes.
const CAP_ENTITY_TYPES: [&str; 2] = ["lease", "node"];
/// How often step 3 prints the time left, at most.
const WAIT_STEP: Duration = Duration::from_secs(5);
/// What every refusal's text ends with.
const UNCHANGED: &str = ". Nothing was changed";

/// The effects of steps 2 to 6. The host's are `super::host_steps`; the
/// tests record them.
pub(crate) trait Steps {
    /// The keys step 6 installs.
    type Keys;
    /// The promotion lock, held until it is dropped.
    type Lock;

    /// Step 6's host preparation would accept this host's configuration and
    /// data directory. Reads only.
    fn check_host(&self) -> Result<(), String>;
    /// The agent's followed log, every entry in `seq` order.
    async fn log(&self) -> Result<Vec<LogEntry>, String>;
    /// The agent's log position, mark, state and freeze.
    async fn reads(&self) -> Result<PromotionReads, String>;
    /// The keys in use, opened from this machine's seals in `entries`, and
    /// checked as step 6 checks them before it writes. Writes nothing.
    async fn open_keys(&self, entries: &[LogEntry]) -> Result<Self::Keys, Vec<String>>;
    /// Every check of step 4's build on `entries`, writing nothing.
    async fn check_log(&self, entries: &[FollowedEntry]) -> Result<(), String>;
    /// The `spec.votes` of the agent's newest stored desired state.
    async fn votes(&self) -> Result<SpecVotes, String>;
    /// The highest term this machine has seen.
    async fn seen(&self) -> Result<u64, String>;
    /// Step 2's `check` rounds, from one above `seen`.
    async fn check(&self, votes: &SpecVotes, seen: u64) -> Result<CheckOutcome, String>;
    /// Takes the promotion lock without waiting.
    fn lock(&self) -> Result<Self::Lock, Refusal>;
    /// Freezes the agent.
    async fn freeze(&self) -> Result<(), String>;
    /// Step 2's real round at `term`.
    async fn vote(&self, votes: &SpecVotes, term: u64) -> Result<Round, String>;
    /// The clock of the vote answers' receipt times.
    fn now(&self) -> Instant;
    /// Sleeps for `duration`.
    async fn sleep(&self, duration: Duration);
    /// Step 4: builds the database from `entries` at `term`.
    async fn build(&self, entries: &[FollowedEntry], term: u64) -> Result<Built, String>;
    /// Step 5 on the built database.
    async fn write(&self, promotion: &Promotion) -> Result<Written, String>;
    /// Step 6: writes the keys where a primary keeps them.
    fn install_keys(&self, keys: &Self::Keys, out: &mut Out) -> Result<(), String>;
    /// Step 6: prepares the host, starts the control plane and returns the
    /// URL this machine's agent dials it at.
    async fn prepare_host(&self, out: &mut Out) -> Result<String, String>;
    /// Ends the freeze.
    async fn unfreeze(&self) -> Result<(), String>;
    /// Step 6: points this machine's agent at `url`.
    async fn point_agent(&self, url: &str, out: &mut Out) -> Result<(), String>;
}

/// Where a promotion stopped after its freeze.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    /// The frozen log failed a check before the first real vote request.
    FrozenLog,
    /// The real vote requests were not sent.
    Vote,
    /// The real round got no majority.
    NoMajority,
    /// Step 4: the frozen log changed or failed a check, or the build
    /// stopped. Nothing is at the database's path then.
    Build,
    /// Step 5.
    Write,
    /// Step 6, the keys.
    Keys,
    /// Step 6, the host's preparation and the control plane's start.
    Prepare,
    /// Step 6, the end of the freeze.
    Unfreeze,
    /// Step 6, pointing this machine's agent at the new primary.
    Point,
}

impl Stop {
    /// Where, as the message says it.
    fn describe(self) -> &'static str {
        match self {
            Self::FrozenLog => "stopped after the freeze, before any real vote request",
            Self::Vote => "stopped at step 2, the vote",
            Self::NoMajority => "stopped at step 2: no majority granted the term",
            Self::Build => "stopped at step 4, the database",
            Self::Write => "stopped at step 5, the primary-status move",
            Self::Keys => "stopped at step 6, the keys",
            Self::Prepare => "stopped at step 6, the host's preparation and start",
            Self::Unfreeze => "stopped at step 6, the end of the freeze",
            Self::Point => "stopped at step 6, pointing this machine's agent",
        }
    }
}

/// A promotion that stopped after its freeze. Its text says why, what
/// became of the freeze, and how to go on.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Stopped {
    /// Where it stopped.
    pub at: Stop,
    /// Why.
    pub why: String,
    /// The end of the freeze: `Ok` once it ended, by this stop or by step
    /// 6, or why it could not end.
    pub freeze: Result<(), String>,
    /// Whether this machine's agent granted the real vote; `None` when no
    /// real vote request was sent.
    pub own_granted: Option<bool>,
    /// The new database, once step 4 wrote it: a rerun is refused until it
    /// is moved aside.
    pub database: Option<PathBuf>,
    /// The URL of the control plane step 6 started.
    pub started: Option<String>,
    /// The term the promotion took.
    pub term: u64,
}

impl fmt::Display for Stopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let term = self.term;
        write!(f, "{}: {}.", self.at.describe(), self.why)?;
        match &self.freeze {
            Ok(()) => f.write_str(
                " The freeze ended: this machine's agent dials its primary again within a second.",
            )?,
            Err(e) => write!(
                f,
                " The freeze could not be ended ({e}): quoro cluster promote --abort ends it."
            )?,
        }
        if let Some(url) = &self.started {
            return write!(
                f,
                " The control plane runs at term {term} at {url}. {}Point this machine's agent \
                 at it with quoro node set-primary {url}.",
                if self.freeze.is_err() {
                    "End the freeze first. "
                } else {
                    ""
                }
            );
        }
        match self.own_granted {
            None => f.write_str(" No real vote request was sent, so no voter moved.")?,
            Some(false) => write!(
                f,
                " This machine's agent did not grant term {term}, so it resumes following."
            )?,
            Some(true) => write!(
                f,
                " This machine's agent granted term {term}: it answers the old primary's welcome \
                 with stale_term, which demotes that primary. Finish or retry the promotion at \
                 term {term} or above."
            )?,
        }
        if let Some(database) = &self.database {
            write!(
                f,
                " The new database stays at {}, and a rerun is refused while it is there. To \
                 retry, move it aside as for a demoted primary: run quoro node rejoin --force \
                 --statement \"<why>\" on this host (section 7.4), then run quoro cluster \
                 promote again.",
                database.display()
            )?;
        }
        Ok(())
    }
}

/// What happened after the freeze, for a stop.
#[derive(Default)]
struct Progress {
    /// Whether this machine's agent granted the real vote; `None` before
    /// any real vote request.
    own_granted: Option<bool>,
    /// Whether step 4 wrote the new database.
    database_written: bool,
    /// The URL of the control plane step 6 started.
    started: Option<String>,
    /// Whether step 6 ended the freeze.
    unfrozen: bool,
}

/// Why the steps after the freeze stopped.
struct Halt {
    at: Stop,
    why: String,
}

impl Halt {
    fn new(at: Stop, why: impl Into<String>) -> Self {
        Self {
            at,
            why: why.into(),
        }
    }
}

/// The frozen log as read right after the freeze.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    log_seq: u64,
    log_hash: String,
    frozen_at: Option<String>,
}

/// Runs steps 2 to 6 for `identity`, this machine's agent, once the
/// refusals of [`super::refusals`] passed and step 1 ran.
///
/// # Errors
///
/// A [`Refusal`] when the run stopped before the freeze: nothing was
/// changed then. A stop after the freeze is [`Outcome::Stopped`].
pub(crate) async fn run<S: Steps>(
    flags: &Flags,
    paths: &Paths,
    host: &dyn Host,
    identity: &Identity,
    steps: &S,
    out: &mut Out,
) -> Result<Outcome, Refusal> {
    before_any_change(flags, identity, steps, out).await?;

    // Step 2's pre-check.
    let votes = steps.votes().await.map_err(Refusal::NoVotes)?;
    let seen = steps.seen().await.map_err(|e| {
        Refusal::Unreadable(format!(
            "cannot read the highest term this machine has seen: {e}"
        ))
    })?;
    out.say(format!(
        "step 2, the term: the highest term this machine has seen is {seen}; asking the voters \
         of voter-set version {} with check requests first",
        votes.version
    ));
    let round = match steps.check(&votes, seen).await.map_err(Refusal::NoVotes)? {
        CheckOutcome::Settled(round) => round,
        CheckOutcome::AboveCeiling { highest, last } => {
            if let Some(last) = &last {
                show_round(last, &identity.node_id, out);
            }
            return Err(Refusal::AboveCeiling { highest });
        }
        CheckOutcome::Unsettled { term, last } => {
            show_round(&last, &identity.node_id, out);
            return Err(Refusal::Unsettled { term });
        }
    };
    show_round(&round, &identity.node_id, out);
    if flags.dry_run {
        dry_run(&round, steps, out);
        return Ok(Outcome::DryRun {
            majority: round.majority,
        });
    }
    if !round.majority {
        return Err(Refusal::NoMajority {
            term: round.term,
            granted: round.granted,
            voters: round.voters,
        });
    }
    let term = round.term;

    // The lock, held to the end, and the freeze.
    let _lock = steps.lock()?;
    refusals::control_plane(paths, host)?;
    steps.freeze().await.map_err(Refusal::FreezeFailed)?;
    out.say(
        "froze this machine's agent: it ends its session with the primary, stores no more log \
         entries and does not dial until the freeze ends",
    );

    let database = paths.data_dir.join(DATABASE);
    let after = After {
        flags,
        identity,
        steps,
        votes: &votes,
        term,
        database: &database,
    };
    let mut progress = Progress::default();
    let ended = after.run(&mut progress, out).await;
    match ended {
        Ok(()) => {
            out.say(format!(
                "promoted node {} to primary of cluster {} at term {term}",
                identity.node_id, identity.cluster_id
            ));
            Ok(Outcome::Promoted)
        }
        Err(halt) => Ok(Outcome::Stopped(
            stop(halt, progress, term, &database, steps).await,
        )),
    }
}

/// The refusals of this layer, before any change: the host step 6
/// prepares, the keys it installs, the token cap of `--ack-fence`, and a
/// log that does not fold.
async fn before_any_change<S: Steps>(
    flags: &Flags,
    identity: &Identity,
    steps: &S,
    out: &mut Out,
) -> Result<(), Refusal> {
    steps.check_host().map_err(Refusal::Host)?;
    let entries = steps
        .log()
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot read the agent's followed log: {e}")))?;
    steps
        .open_keys(&entries)
        .await
        .map_err(Refusal::PrimaryKeys)?;
    out.say("the keys step 6 installs, and the files it writes them to, pass its checks");
    if flags.ack_fence {
        match token_cap(identity, &entries).await? {
            Some(old) => out.say(format!(
                "--ack-fence: step 5 declares the old primary's node {old} lost; no lease it \
                 holds is at the token cap"
            )),
            None => out.say(
                "--ack-fence: no other node holds the primary status in the followed log, so \
                 step 5 declares no node lost",
            ),
        }
    }
    let last = last_seq(&entries);
    steps
        .check_log(&followed(entries))
        .await
        .map_err(Refusal::DoesNotFold)?;
    out.say(format!(
        "the followed log, seq 1 to {last}, folds and builds (step 4's checks, in memory)"
    ));
    Ok(())
}

/// The token-cap check of `--ack-fence` (section 7.3, section 8.6, ADR
/// 0325) on `entries`: the old primary's node, `None` when no other node
/// holds the primary status.
///
/// # Errors
///
/// [`Refusal::TokenExhausted`] naming each lease at the cap, or
/// [`Refusal::DoesNotFold`] when the leases and nodes do not fold.
async fn token_cap(identity: &Identity, entries: &[LogEntry]) -> Result<Option<String>, Refusal> {
    let folded = fold_cap(entries).await.map_err(Refusal::DoesNotFold)?;
    let Some(old) = promotion::old_primary(&folded, &identity.node_id) else {
        return Ok(None);
    };
    promotion::check_token_cap(&folded, old).map_err(|refusal| Refusal::TokenExhausted {
        node_id: old.to_string(),
        detail: refusal.to_string(),
    })?;
    Ok(Some(old.to_string()))
}

/// The fold of the `leases` and `nodes` entries of `entries`, against this
/// binary's schema.
async fn fold_cap(entries: &[LogEntry]) -> Result<Folded, String> {
    let schema = keys::schema().await?;
    rebuild::fold(
        &schema,
        entries
            .iter()
            .filter(|entry| CAP_ENTITY_TYPES.contains(&entry.entity_type.as_str()))
            .map(|entry| Entry {
                seq: entry.seq,
                entity_type: &entry.entity_type,
                entity_id: &entry.entity_id,
                op: &entry.op,
                payload: &entry.payload,
            }),
    )
    .map_err(|e| format!("the followed log's leases and nodes do not fold: {e}"))
}

/// The followed entries as the build reads them.
fn followed(entries: Vec<LogEntry>) -> Vec<FollowedEntry> {
    entries
        .into_iter()
        .map(|entry| FollowedEntry {
            seq: entry.seq,
            term: entry.term,
            time: entry.time,
            actor_type: entry.actor_type,
            actor_id: entry.actor_id,
            entity_type: entry.entity_type,
            entity_id: entry.entity_id,
            op: entry.op,
            payload: entry.payload,
            prev_hash: entry.prev_hash,
            hash: entry.hash,
        })
        .collect()
}

/// The last `seq` of `entries`, 0 when there is none.
fn last_seq(entries: &[LogEntry]) -> u64 {
    entries.last().map_or(0, |entry| entry.seq)
}

/// `refusal`'s text without the closing "Nothing was changed", for a stop
/// after the freeze, which changed something.
fn after_freeze(refusal: &Refusal) -> String {
    if let Refusal::TokenExhausted { node_id, detail } = refusal {
        return format!(
            "--ack-fence declares the old primary's node {node_id} lost, which raises the token \
             of every lease it holds: {detail}"
        );
    }
    let text = refusal.to_string();
    match text.strip_suffix(UNCHANGED) {
        Some(stripped) => stripped.to_string(),
        None => text,
    }
}

/// Prints `round`: each voter's answer, then the tally.
fn show_round(round: &Round, own_node_id: &str, out: &mut Out) {
    let (kind, verb) = if round.check {
        ("check", "would grant")
    } else {
        ("vote", "granted")
    };
    out.say(format!(
        "  {kind} round at term {}, voter-set version {}:",
        round.term, round.voters_version
    ));
    for reply in &round.replies {
        let mine = if reply.node_id == own_node_id {
            " (this machine)"
        } else {
            ""
        };
        out.say(format!(
            "    {}{mine}: {}",
            reply.node_id,
            describe(&reply.reply)
        ));
    }
    out.say(format!(
        "  {} of {} voters {verb} term {}: {}",
        round.granted,
        round.voters,
        round.term,
        if round.majority {
            "a majority"
        } else {
            "not a majority"
        }
    ));
}

/// One voter's reply, as [`show_round`] prints it.
fn describe(reply: &Reply) -> String {
    match reply {
        Reply::Answer { answer, .. } => match answer.vote {
            VoteOutcome::Granted => format!(
                "granted, at term {}, lease wait {} ms",
                answer.term,
                answer.lease_wait_ms.unwrap_or_default()
            ),
            VoteOutcome::Refused => {
                let reason = answer.reason.map_or("no reason given", refusal_name);
                let retry = answer
                    .retry_after_ms
                    .map(|ms| format!(", retry after {ms} ms"))
                    .unwrap_or_default();
                format!("refused, {reason}, at term {}{retry}", answer.term)
            }
        },
        Reply::NoAnswer(why) => format!("no answer: {why}"),
    }
}

/// A refused answer's `reason`, as section 8.9 writes it.
fn refusal_name(reason: VoteRefusal) -> &'static str {
    match reason {
        VoteRefusal::StaleTerm => "stale_term",
        VoteRefusal::SupportHeld => "support_held",
        VoteRefusal::VotersChanged => "voters_changed",
        VoteRefusal::NotAllowed => "not_allowed",
    }
}

/// Whether `own_node_id`'s answer in `round` counts as `granted`.
fn granted_by(round: &Round, own_node_id: &str) -> bool {
    round.replies.iter().any(|reply| {
        reply.node_id == own_node_id
            && matches!(&reply.reply, Reply::Answer { answer, .. } if answer.vote == VoteOutcome::Granted)
    })
}

/// What a dry run says once the pre-check settled.
fn dry_run<S: Steps>(round: &Round, steps: &S, out: &mut Out) {
    if round.majority {
        if let Some(until) = round.wait_until {
            out.say(format!(
                "if the voters granted term {} now, step 3 would wait about {} s for the old \
                 primary's grants (--ack-fence skips that wait)",
                round.term,
                seconds(until.saturating_duration_since(steps.now()))
            ));
        }
    } else {
        out.say(format!(
            "a promotion now would stop at step 2: {} of {} voters would grant term {}",
            round.granted, round.voters, round.term
        ));
    }
    out.say(
        "dry run: stopped after step 2's vote pre-check. Nothing was changed: no term was taken, \
         the agent was not frozen and no voter moved",
    );
}

/// `duration` in whole seconds, rounded up.
fn seconds(duration: Duration) -> u64 {
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

/// Step 3 (section 7.3): waits until the latest receipt time plus
/// `lease_wait_ms` over the granting answers, printing the time left, unless
/// `--ack-fence` skips it.
async fn wait<S: Steps>(flags: &Flags, round: &Round, steps: &S, out: &mut Out) {
    if flags.ack_fence {
        out.say(
            "step 3, the wait for the old primary's grants: skipped, as --ack-fence states that \
             the old primary's host is down and stays down",
        );
        return;
    }
    let Some(until) = round.wait_until else {
        out.say(
            "step 3, the wait for the old primary's grants: none, no granting answer asks for one",
        );
        return;
    };
    loop {
        let left = until.saturating_duration_since(steps.now());
        if left.is_zero() {
            break;
        }
        out.say(format!(
            "step 3, the wait for the old primary's grants: {} s left",
            seconds(left)
        ));
        steps.sleep(left.min(WAIT_STEP)).await;
    }
    out.say("step 3, the wait for the old primary's grants: over");
}

/// What the steps after the freeze work with.
struct After<'a, S> {
    flags: &'a Flags,
    identity: &'a Identity,
    steps: &'a S,
    votes: &'a SpecVotes,
    term: u64,
    database: &'a Path,
}

impl<S: Steps> After<'_, S> {
    /// From the checks of the frozen log to the end of step 6. `progress`
    /// keeps what a stop's message needs.
    async fn run(&self, progress: &mut Progress, out: &mut Out) -> Result<(), Halt> {
        let Self {
            flags,
            identity,
            steps,
            votes,
            term,
            database,
        } = *self;

        // The checks again, on the frozen log, before the first real vote
        // request (section 7.3).
        let (snapshot, entries) = read_frozen(flags, steps)
            .await
            .map_err(|why| Halt::new(Stop::FrozenLog, why))?;
        let keys = steps.open_keys(&entries).await.map_err(|problems| {
            Halt::new(
                Stop::FrozenLog,
                format!(
                    "step 6 would refuse to install the keys opened from the frozen log: {}",
                    problems.join("; ")
                ),
            )
        })?;
        if flags.ack_fence {
            token_cap(identity, &entries)
                .await
                .map_err(|refusal| Halt::new(Stop::FrozenLog, after_freeze(&refusal)))?;
        }
        steps.check_log(&followed(entries)).await.map_err(|e| {
            Halt::new(
                Stop::FrozenLog,
                format!("the frozen log does not fold: {e}"),
            )
        })?;
        out.say(format!(
            "the frozen log, seq 1 to {}, passes the refusals on the log again and folds",
            snapshot.log_seq
        ));

        // Step 2, the real vote.
        out.say(format!(
            "step 2, the vote: asking every voter for term {term}"
        ));
        let round = steps
            .vote(votes, term)
            .await
            .map_err(|e| Halt::new(Stop::Vote, format!("the vote requests were not sent: {e}")))?;
        show_round(&round, &identity.node_id, out);
        progress.own_granted = Some(granted_by(&round, &identity.node_id));
        if !round.majority {
            return Err(Halt::new(
                Stop::NoMajority,
                format!(
                    "{} of {} voters granted term {term}, and a majority is needed",
                    round.granted, round.voters
                ),
            ));
        }

        // Step 3.
        wait(flags, &round, steps, out).await;

        // Step 4.
        let entries = step_4_log(flags, identity, steps, &snapshot)
            .await
            .map_err(|why| Halt::new(Stop::Build, why))?;
        out.say(format!(
            "step 4, the database: the freeze held and the log is the frozen one, seq 1 to {}",
            snapshot.log_seq
        ));
        let built = steps
            .build(&entries, term)
            .await
            .map_err(|e| Halt::new(Stop::Build, format!("the database build stopped: {e}")))?;
        drop(entries);
        progress.database_written = true;
        show_built(&built, database, term, out);

        // Step 5.
        let promotion = Promotion {
            term: i64::try_from(term)
                .map_err(|_| Halt::new(Stop::Write, format!("term {term} is not a stored term")))?,
            to_node_id: identity.node_id.clone(),
            log_seq: i64::try_from(built.head_seq).map_err(|_| {
                Halt::new(
                    Stop::Write,
                    format!("seq {} is not a stored seq", built.head_seq),
                )
            })?,
            ack_fence: flags.ack_fence,
            accept_loss: flags.accept_loss,
            statement: flags.statement.clone(),
        };
        let written = steps
            .write(&promotion)
            .await
            .map_err(|e| Halt::new(Stop::Write, format!("the primary-status write failed: {e}")))?;
        out.say(format!(
            "step 5, the primary status: moved from {} to {} at term {term}, change-log seq {} to \
             {}, audit record {}",
            written.from_node_id.as_deref().unwrap_or("no other node"),
            identity.node_id,
            written.first_seq,
            written.last_seq,
            written.audit_seq
        ));

        // Step 6.
        out.say("step 6, the keys and the start:");
        steps
            .install_keys(&keys, out)
            .map_err(|e| Halt::new(Stop::Keys, e))?;
        drop(keys);
        let url = steps
            .prepare_host(out)
            .await
            .map_err(|e| Halt::new(Stop::Prepare, e))?;
        progress.started = Some(url.clone());
        steps
            .unfreeze()
            .await
            .map_err(|e| Halt::new(Stop::Unfreeze, format!("cannot end the freeze: {e}")))?;
        progress.unfrozen = true;
        out.say("ended the freeze");
        steps
            .point_agent(&url, out)
            .await
            .map_err(|e| Halt::new(Stop::Point, e))?;
        Ok(())
    }
}

/// Reads the frozen log and runs every refusal on the log again on it
/// (section 7.3 step 4, ADR 0280): the agent is frozen, and the log is not
/// quarantined or empty and, unless `--accept-loss`, not refollowing or
/// below the mark. The entries must end where the reads say the log ends.
async fn read_frozen<S: Steps>(
    flags: &Flags,
    steps: &S,
) -> Result<(Snapshot, Vec<LogEntry>), String> {
    let reads = steps
        .reads()
        .await
        .map_err(|e| format!("cannot read the frozen log: {e}"))?;
    if reads.frozen_at.is_none() {
        return Err("the agent is not frozen: the freeze did not hold".to_string());
    }
    refusals::log_state(&reads, flags.accept_loss)
        .map_err(|refusal| format!("the frozen log is refused: {}", after_freeze(&refusal)))?;
    let entries = steps
        .log()
        .await
        .map_err(|e| format!("cannot read the frozen log: {e}"))?;
    let ends = entries.last().map(|last| (last.seq, last.hash.as_str()));
    if ends != Some((reads.log_seq, reads.log_hash.as_str())) {
        return Err(format!(
            "the frozen log does not end where the agent's store says it ends, at seq {}",
            reads.log_seq
        ));
    }
    Ok((
        Snapshot {
            log_seq: reads.log_seq,
            log_hash: reads.log_hash,
            frozen_at: reads.frozen_at,
        },
        entries,
    ))
}

/// Step 4's checks (section 7.3, ADR 0261, ADR 0280): the log read again
/// is the frozen one, under the same freeze, every refusal on the log
/// passes on it, and with `--ack-fence` the token cap holds. Returns the
/// entries the build reads.
async fn step_4_log<S: Steps>(
    flags: &Flags,
    identity: &Identity,
    steps: &S,
    snapshot: &Snapshot,
) -> Result<Vec<FollowedEntry>, String> {
    let (now, entries) = read_frozen(flags, steps).await?;
    if now.frozen_at != snapshot.frozen_at {
        return Err(format!(
            "the freeze did not hold: it was set at {} and now reads {}",
            snapshot.frozen_at.as_deref().unwrap_or("none"),
            now.frozen_at.as_deref().unwrap_or("none")
        ));
    }
    if (now.log_seq, &now.log_hash) != (snapshot.log_seq, &snapshot.log_hash) {
        return Err(format!(
            "the followed log changed since the freeze: it was at seq {} and is at seq {}",
            snapshot.log_seq, now.log_seq
        ));
    }
    if flags.ack_fence {
        token_cap(identity, &entries)
            .await
            .map_err(|refusal| after_freeze(&refusal))?;
    }
    Ok(followed(entries))
}

/// What step 4 built.
fn show_built(built: &Built, database: &Path, term: u64, out: &mut Out) {
    let rows: u64 = built.rows.values().sum();
    out.say(format!(
        "step 4, the database: built {} at term {term} from seq 1 to {}: {rows} rows, schema \
         version {}",
        database.display(),
        built.head_seq,
        built.schema_version
    ));
    for (count, what) in [
        (built.cascaded.len(), "rows removed with a deleted parent"),
        (built.dropped.len(), "rows made only of updates, left out"),
        (
            built.mismatches.len(),
            "creates whose key is not their entity id",
        ),
    ] {
        if count > 0 {
            out.say(format!("  {count} {what}"));
        }
    }
}

/// A stop after the freeze (section 7.3, "If the promotion stops"): the
/// freeze ends, unless step 6 already ended it.
async fn stop<S: Steps>(
    halt: Halt,
    progress: Progress,
    term: u64,
    database: &Path,
    steps: &S,
) -> Stopped {
    let freeze = if progress.unfrozen {
        Ok(())
    } else {
        steps.unfreeze().await
    };
    Stopped {
        at: halt.at,
        why: halt.why,
        freeze,
        own_granted: progress.own_granted,
        database: progress.database_written.then(|| database.to_path_buf()),
        started: progress.started,
        term,
    }
}
