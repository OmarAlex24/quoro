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
//! Within one item the more specific state comes first: a quarantined log
//! is also empty once it was moved aside, and a refollowing agent's log is
//! below its mark until it reaches the head it refollows to.
//!
//! Every check reads; none writes. The agent's store is opened only when
//! `node.db` exists, so a host without one gets none.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use quoro_control_plane::store::Store;
use quoro_node_agent::freeze::{self, PromotionReads};
use quoro_node_agent::identity::{self, Identity};
use quoro_node_agent::store::{NodeStore, Refollowing};
use quoro_proto::custody::KeyKind;

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
    let database = paths.data_dir.join(crate::host::DATABASE);
    if exists(&database) {
        return Err(Refusal::ControlPlaneDatabase(database));
    }
    if host.control_plane_unit_known() {
        return Err(Refusal::ControlPlaneUnit);
    }

    // 3. An empty or quarantined log. A host whose agent never opened its
    // store has followed nothing, and opening it here would create it.
    if !exists(&node_dir.join(crate::host::AGENT_STORE)) {
        return Err(Refusal::LogEmpty);
    }
    let store = NodeStore::open(node_dir)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot open the agent's store: {e}")))?;
    let reads = freeze::promotion_reads(&store)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot read the agent's followed log: {e}")))?;
    if reads.quarantined {
        return Err(Refusal::LogQuarantined);
    }
    if reads.is_empty() {
        return Err(Refusal::LogEmpty);
    }

    // 4. Refollowing, or below the mark: `--accept-loss` lifts both.
    if !flags.accept_loss {
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

    // 7. A seal of every key in use, opened to its key.
    let entries = store
        .log_entries()
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
