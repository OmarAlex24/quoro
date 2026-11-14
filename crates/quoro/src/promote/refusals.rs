//! The refusals of `quoro cluster promote` before it changes anything
//! (`node-agent-protocol.md` section 7.3), in the order the contract lists
//! them. The first that holds stops the run:
//!
//! 1. the machine does not have the `node` role, or its agent has no
//!    identity;
//! 2. this host has a control-plane database or a control-plane unit;
//! 3. the agent's followed log is quarantined or empty;
//! 4. the agent is refollowing, or its log is below its mark (section 7.1,
//!    ADR 0261). `--accept-loss` lifts these two and no other;
//! 5. a `restore-pending` marker is next to the control-plane database
//!    (`backup-format.md` section 8.2, ADR 0285);
//! 6. the newest `welcome.schema_version` the agent stored is above the
//!    newest schema version this binary knows (section 5.10, ADR 0229);
//! 7. this machine cannot open a seal of every key in use ([`super::keys`]).
//!
//! The refusals that read the whole followed log or the host's preparation
//! for step 6 come after these, from `super::steps`, and still before any
//! change: the log does not fold, the keys step 6 installs, the host step 6
//! prepares, and the token cap of `--ack-fence`. So do the stops of step 2's
//! vote pre-check, which changes nothing either.
//!
//! Within one item the more specific state comes first: a quarantined log
//! is also empty once it was moved aside, and a refollowing agent's log is
//! below its mark until it reaches the head it refollows to.
//!
//! Every check reads; none writes. The agent's store is opened read-only
//! and only when `node.db` exists, so a host without one gets none, and no
//! table, migration or node-local key is created. systemd's answer about
//! the unit must be read: when it cannot be, the command cannot tell and
//! refuses.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use quoro_control_plane::store::Store;
use quoro_node_agent::freeze::{self, PromotionReads};
use quoro_node_agent::identity::{self, Identity};
use quoro_node_agent::store::{NodeStore, Refollowing};
use quoro_proto::custody::KeyKind;
use quoro_proto::wire::MAX_TERM;

use super::command::{Flags, Host, Paths};
use super::keys;
use crate::node::Role;

/// The marker `quoro restore` writes next to the database it installs
/// (`backup-format.md` section 8.2, ADR 0285). Its writer comes with #838;
/// the refusal only tests that the file exists.
pub(crate) const RESTORE_PENDING: &str = "restore-pending";

/// Why the command stopped. Each message ends by saying that nothing was
/// changed, which holds for every one of them.
#[derive(Debug)]
pub(crate) enum Refusal {
    /// The effective uid is not 0.
    NotRoot,
    /// Something the command must read could not be read.
    Unreadable(String),
    /// The machine enrolled with another role.
    NotANode(Role),
    /// The agent has no identity, or it does not load.
    NoIdentity(String),
    /// A control-plane database is on this host.
    ControlPlaneDatabase(PathBuf),
    /// systemd knows a control-plane unit on this host.
    ControlPlaneUnit,
    /// The followed log is quarantined.
    LogQuarantined,
    /// The followed log is empty, or the agent has no store.
    LogEmpty,
    /// The agent refollows its log to `head`.
    Refollowing { log_seq: u64, head: u64 },
    /// The followed log is below the mark.
    BelowMark { log_seq: u64, mark: u64 },
    /// A `restore-pending` marker is on this host.
    RestorePending(PathBuf),
    /// A primary announced a schema newer than this binary's.
    SchemaTooNew { stored: u32, newest: u64 },
    /// A key in use has no seal this machine can open to it.
    Keys(Vec<String>),
    /// `--abort` while a promotion runs on this host.
    PromotionRunning,
    /// `--abort` on a host whose agent has no store.
    NoAgentStore(PathBuf),
    /// The followed log does not fold: the build of step 4 would stop on it.
    DoesNotFold(String),
    /// Step 6 would refuse to install the keys opened from the seals.
    PrimaryKeys(Vec<String>),
    /// Step 6 would refuse to prepare this host for the control plane.
    Host(String),
    /// `--ack-fence` would declare `node_id` lost, and a lease it holds is
    /// already at the token cap (section 8.6, ADR 0325).
    TokenExhausted { node_id: String, detail: String },
    /// The vote pre-check cannot run: no voter set, or a client that would
    /// not send.
    NoVotes(String),
    /// The term would pass the ceiling, 2^53 - 1 (section 8.8).
    AboveCeiling { highest: u64 },
    /// The voters' terms kept rising during the pre-check.
    Unsettled { term: u64 },
    /// The pre-check found no majority that would grant `term`.
    NoMajority {
        term: u64,
        granted: usize,
        voters: usize,
    },
    /// Another promotion holds the promotion lock on this host.
    AnotherPromotion,
    /// The agent could not be frozen.
    FreezeFailed(String),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRoot => f.write_str(
                "run this as root on the machine to promote: the command reads the agent's custody \
                 key, and its later steps must open their connections as root. Nothing was changed",
            ),
            Self::Unreadable(message) => write!(f, "{message}. Nothing was changed"),
            Self::NotANode(role) => write!(
                f,
                "this machine enrolled as {}, not with the node role. Only a machine with the \
                 node role follows the change log and holds the cluster keys, so only it can be \
                 promoted. Nothing was changed",
                role.as_str()
            ),
            Self::NoIdentity(message) => write!(
                f,
                "the node agent has no identity ({message}). Only an enrolled machine with the \
                 node role can be promoted. Nothing was changed"
            ),
            Self::ControlPlaneDatabase(path) => write!(
                f,
                "this host already has a control-plane database, {}. A demoted primary's data \
                 must be moved aside first: run quoro node rejoin on this host. Nothing was \
                 changed",
                path.display()
            ),
            Self::ControlPlaneUnit => f.write_str(
                "this host already has a control-plane unit, quoro.service. A demoted primary's \
                 unit must be moved aside first: run quoro node rejoin on this host. Nothing was \
                 changed",
            ),
            Self::LogQuarantined => f.write_str(
                "the agent's followed log is quarantined: the primary declared it diverged, and \
                 the agent has not yet learned the head it follows again to. Wait until it \
                 follows again; --accept-loss does not lift this. Nothing was changed",
            ),
            Self::LogEmpty => f.write_str(
                "the agent's followed log is empty: this machine holds no change-log entry to \
                 build the database from. Nothing was changed",
            ),
            Self::Refollowing { log_seq, head } => write!(
                f,
                "the agent is refollowing: its log is at seq {log_seq} and follows again to seq \
                 {head}. Wait until it reaches seq {head}, or, when no other candidate is left, \
                 promote with --accept-loss --statement \"<text>\", which loses every entry past \
                 seq {log_seq}, replicated or not. Nothing was changed"
            ),
            Self::BelowMark { log_seq, mark } => write!(
                f,
                "the agent's followed log is below its mark: it is at seq {log_seq}, and this \
                 machine once acknowledged seq {mark}, so the log lost entries. Promote another \
                 candidate, or, when none is left, promote with --accept-loss --statement \
                 \"<text>\", which loses every entry past seq {log_seq}, replicated or not. \
                 Nothing was changed"
            ),
            Self::RestorePending(path) => write!(
                f,
                "a restore-pending marker exists, {}. Finish the restore, or delete the marker \
                 after checking that no quoro restore runs on this host. Nothing was changed",
                path.display()
            ),
            Self::SchemaTooNew { stored, newest } => write!(
                f,
                "a primary announced schema version {stored} to this agent, and this binary \
                 knows schema versions up to {newest}: it cannot build a database from that \
                 log. Upgrade quoro on this machine first. Nothing was changed"
            ),
            Self::Keys(problems) => write!(
                f,
                "this machine cannot open a seal of every key in use: {}. Nothing was changed",
                problems.join("; ")
            ),
            Self::PromotionRunning => f.write_str(
                "a promotion runs on this host, and --abort is refused while it runs. Nothing \
                 was changed",
            ),
            Self::NoAgentStore(path) => write!(
                f,
                "the node agent has no store at {}, so there is no freeze to end. Nothing was \
                 changed",
                path.display()
            ),
            Self::DoesNotFold(message) => write!(
                f,
                "the followed log does not fold: {message}. Step 4 would stop on it after the \
                 votes, so the command stops before them. Nothing was changed"
            ),
            Self::PrimaryKeys(problems) => write!(
                f,
                "step 6 would refuse to install the keys opened from this machine's seals: {}. \
                 Nothing was changed",
                problems.join("; ")
            ),
            Self::Host(message) => write!(
                f,
                "step 6 would refuse to prepare this host for the control plane: {message}. \
                 Nothing was changed"
            ),
            Self::TokenExhausted { node_id, detail } => write!(
                f,
                "--ack-fence declares the old primary's node {node_id} lost, which raises the \
                 token of every lease it holds: {detail}. The command stopped before any \
                 vote request: no term was taken and the agent was not frozen. Nothing was \
                 changed"
            ),
            Self::NoVotes(message) => write!(
                f,
                "the vote pre-check cannot run: {message}. Nothing was changed"
            ),
            Self::AboveCeiling { highest } => write!(
                f,
                "the term would pass the term ceiling, {MAX_TERM} (2^53 - 1, section 8.8): the \
                 highest term this machine has seen or a voter answered is {highest}. The \
                 command stopped before any real vote request and before the freeze. Nothing \
                 was changed"
            ),
            Self::Unsettled { term } => write!(
                f,
                "the voters' terms kept rising during the vote pre-check; the next check would \
                 ask for term {term}. Run the command again: it starts from the highest term it \
                 then sees. Only the pre-check ran, so no voter moved. Nothing was changed"
            ),
            Self::NoMajority {
                term,
                granted,
                voters,
            } => write!(
                f,
                "{granted} of {voters} voters would grant term {term}, and a majority is \
                 needed. Only the pre-check ran, so no voter moved. Nothing was changed"
            ),
            Self::AnotherPromotion => {
                f.write_str("another quoro cluster promote runs on this host. Nothing was changed")
            }
            Self::FreezeFailed(message) => write!(
                f,
                "cannot freeze this machine's agent: {message}. No real vote request was sent. \
                 Nothing was changed"
            ),
        }
    }
}

/// What the refusals read, for step 1.
pub(crate) struct Passed {
    /// The agent's identity.
    pub identity: Identity,
    /// The agent's log position, mark and state.
    pub reads: PromotionReads,
    /// Every key in use, each opened from its seal: signing keys first,
    /// then the data-encryption key, then the TLS key.
    pub keys: Vec<(KeyKind, String)>,
}

/// Whether `path` exists as itself: a dangling symbolic link exists.
fn exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

/// Refusal 2: a control-plane database or a control-plane unit on this
/// host. The command runs it again once it holds the promotion lock, so a
/// promotion that finished on this host meanwhile stops the next.
///
/// # Errors
///
/// [`Refusal::ControlPlaneDatabase`] or [`Refusal::ControlPlaneUnit`], or
/// [`Refusal::Unreadable`] when systemd's answer cannot be read.
pub(crate) fn control_plane(paths: &Paths, host: &dyn Host) -> Result<(), Refusal> {
    let database = paths.data_dir.join(crate::host::DATABASE);
    if exists(&database) {
        return Err(Refusal::ControlPlaneDatabase(database));
    }
    let unit = host.control_plane_unit_known().map_err(|e| {
        Refusal::Unreadable(format!(
            "cannot tell whether this host has a control-plane unit: {e}"
        ))
    })?;
    if unit {
        return Err(Refusal::ControlPlaneUnit);
    }
    Ok(())
}

/// Refusals 3 and 4 on the log `reads` describes: quarantined, empty, and,
/// unless `accept_loss` lifts them, refollowing or below the mark. Step 4
/// runs them again on the frozen log (section 7.3, ADR 0280).
///
/// # Errors
///
/// The first of those refusals that holds.
pub(crate) fn log_state(reads: &PromotionReads, accept_loss: bool) -> Result<(), Refusal> {
    if reads.quarantined {
        return Err(Refusal::LogQuarantined);
    }
    if reads.is_empty() {
        return Err(Refusal::LogEmpty);
    }
    // Refollowing, or below the mark: `--accept-loss` lifts both.
    if !accept_loss {
        if let Some(Refollowing::Head(head)) = reads.refollowing {
            return Err(Refusal::Refollowing {
                log_seq: reads.log_seq,
                head,
            });
        }
        if reads.below_mark {
            return Err(Refusal::BelowMark {
                log_seq: reads.log_seq,
                mark: reads.mark.unwrap_or_default(),
            });
        }
    }
    Ok(())
}

/// Runs every refusal in order (module documentation).
///
/// # Errors
///
/// The first [`Refusal`] that holds.
pub(crate) async fn check(
    flags: &Flags,
    paths: &Paths,
    host: &dyn Host,
) -> Result<Passed, Refusal> {
    let node_dir = &paths.node_data_dir;

    // 1. The node role and the agent's identity.
    let role = crate::node::read_role(node_dir);
    if role != Role::Node {
        return Err(Refusal::NotANode(role));
    }
    if !identity::is_enrolled(node_dir) {
        return Err(Refusal::NoIdentity(format!(
            "{} holds no complete enrollment",
            node_dir.display()
        )));
    }
    let identity =
        identity::load_identity(node_dir).map_err(|e| Refusal::NoIdentity(e.to_string()))?;

    // 2. A control-plane database or unit.
    control_plane(paths, host)?;

    // 3. An empty or quarantined log. A host whose agent never opened its
    // store has followed nothing, and opening it here would create it.
    if !exists(&node_dir.join(crate::host::AGENT_STORE)) {
        return Err(Refusal::LogEmpty);
    }
    let store = NodeStore::open_read_only(node_dir)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot open the agent's store: {e}")))?;
    let reads = freeze::promotion_reads(&store)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot read the agent's followed log: {e}")))?;
    // 3, then 4: refollowing, or below the mark, which `--accept-loss`
    // lifts.
    log_state(&reads, flags.accept_loss)?;

    // 5. A restore that has not finished.
    let marker = paths.data_dir.join(RESTORE_PENDING);
    if exists(&marker) {
        return Err(Refusal::RestorePending(marker));
    }

    // 6. A log newer than this binary.
    let newest = u64::try_from(Store::shipped_schema_version()).unwrap_or(u64::MAX);
    if let Some(stored) = reads.welcome_schema_version
        && u64::from(stored) > newest
    {
        return Err(Refusal::SchemaTooNew { stored, newest });
    }

    // 7. A seal of every key in use, opened to its key. Only the entries of
    // the two tables that say which keys are in use are read.
    let entries = store
        .log_entries_of(&keys::ENTITY_TYPES)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot read the agent's followed log: {e}")))?;
    let keys = keys::check(&identity, &entries, node_dir)
        .await
        .map_err(Refusal::Keys)?;

    Ok(Passed {
        identity,
        reads,
        keys,
    })
}
