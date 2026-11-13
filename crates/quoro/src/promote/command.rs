//! The command line of `quoro cluster promote`, the host it reads, step 1
//! and `--abort` (`node-agent-protocol.md` section 7.3).
//!
//! Every read of the host goes through [`Host`]: the effective uid, the
//! configuration that names the two data directories, systemd and the
//! clock. The files under those directories, the agent's store among them,
//! are read where [`Paths`] points, so the tests lay a host out in a
//! temporary directory and run without root.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::SystemTime;

use quoro_node_agent::AgentError;
use quoro_node_agent::freeze::{self, PromotionLock, PromotionReads};
use quoro_node_agent::store::{NodeStore, PROMOTION_FREEZE};
use quoro_proto::custody::KeyKind;

use super::refusals::{self, Passed, Refusal};
use crate::host::{AGENT_STORE, Out, UNIT};

/// The command line, read by [`parse_args`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Request {
    /// `quoro cluster promote --abort`.
    Abort,
    /// `quoro cluster promote [--dry-run] [--ack-fence] [--accept-loss]
    /// [--statement <text>]`.
    Promote(Flags),
}

/// The flags of a promotion.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Flags {
    /// `--dry-run`: stop before anything changes.
    pub dry_run: bool,
    /// `--ack-fence`: the old primary's host is down and stays down.
    pub ack_fence: bool,
    /// `--accept-loss`: lifts the refusals for a log below the mark and for
    /// a refollowing agent.
    pub accept_loss: bool,
    /// The operator's statement, which `--ack-fence` and `--accept-loss`
    /// require.
    pub statement: Option<String>,
}

/// Reads the command line after `cluster promote`.
///
/// # Errors
///
/// A usage message: an unknown option or a positional argument,
/// `--statement` without a value, with an option as its value, given twice
/// or blank, `--ack-fence` or `--accept-loss` without `--statement`,
/// `--statement` without either, and `--abort` with any other option.
pub(crate) fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut abort = false;
    let mut others = 0usize;
    let mut flags = Flags::default();
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(format!("cluster promote: unexpected argument '{argument}'"));
        };
        match name {
            "abort" => {
                abort = true;
                continue;
            }
            "dry-run" => flags.dry_run = true,
            "ack-fence" => flags.ack_fence = true,
            "accept-loss" => flags.accept_loss = true,
            "statement" => {
                let Some(value) = args.next() else {
                    return Err(format!("cluster promote: '{argument}' needs a value"));
                };
                // `--statement --dry-run` would record the flag as the
                // operator's statement and drop the flag.
                if value.starts_with("--") {
                    return Err(format!(
                        "cluster promote: --statement needs the statement's text, not the option \
                         '{value}'"
                    ));
                }
                if flags.statement.is_some() {
                    return Err("cluster promote: --statement is given twice".to_string());
                }
                flags.statement = Some(value.clone());
            }
            _ => return Err(format!("cluster promote: unknown option '{argument}'")),
        }
        others += 1;
    }
    if abort {
        return if others == 0 {
            Ok(Request::Abort)
        } else {
            Err("cluster promote --abort takes no other option".to_string())
        };
    }
    let needs_statement = flags.ack_fence || flags.accept_loss;
    let refused = match flags.statement.as_deref() {
        None if needs_statement => {
            Some("cluster promote: --ack-fence and --accept-loss need --statement \"<text>\"")
        }
        Some(_) if !needs_statement => {
            Some("cluster promote: --statement goes with --ack-fence or --accept-loss")
        }
        Some(text) if text.trim().is_empty() => {
            Some("cluster promote: --statement needs a non-empty statement")
        }
        _ => None,
    };
    match refused {
        Some(message) => Err(message.to_string()),
        None => Ok(Request::Promote(flags)),
    }
}

/// The two data directories the command reads.
#[derive(Clone, Debug)]
pub(crate) struct Paths {
    /// The control-plane data directory (`quoro.toml` `data_dir`): where a
    /// control-plane database and a `restore-pending` marker would be.
    pub data_dir: PathBuf,
    /// The node agent's data directory (`[node] data_dir`): its role, its
    /// identity, its custody key, its mark and its store.
    pub node_data_dir: PathBuf,
}

/// What the command reads from the host besides the files under [`Paths`].
pub(crate) trait Host {
    /// The effective uid of this process, when it could be read.
    fn euid(&self) -> Option<u32>;
    /// The two data directories, from `quoro.toml`.
    ///
    /// # Errors
    ///
    /// A message when the configuration cannot be read.
    fn paths(&self) -> Result<Paths, String>;
    /// Whether systemd knows the control-plane unit, `quoro.service`.
    ///
    /// # Errors
    ///
    /// A message when systemd's answer could not be read, so the command
    /// cannot tell.
    fn control_plane_unit_known(&self) -> Result<bool, String>;
    /// The clock, for the age of the head step 1 shows.
    fn now(&self) -> SystemTime;
}

/// The host this process runs on.
struct RealHost;

impl Host for RealHost {
    fn euid(&self) -> Option<u32> {
        Some(rustix::process::geteuid().as_raw())
    }

    fn paths(&self) -> Result<Paths, String> {
        let path = Path::new(crate::config::DEFAULT_PATH);
        let config = crate::config::Config::load(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        Ok(Paths {
            data_dir: config.data_dir,
            node_data_dir: config.node.data_dir,
        })
    }

    fn control_plane_unit_known(&self) -> Result<bool, String> {
        let output = crate::host::systemctl(&["show", "--property=LoadState", "--value", UNIT])?;
        read_load_state(
            output.status.success(),
            &String::from_utf8_lossy(&output.stdout),
            &String::from_utf8_lossy(&output.stderr),
        )
    }

    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Reads `systemctl show --property=LoadState --value quoro.service`: a
/// unit systemd does not know reads `not-found`, and any other load state
/// (`loaded`, `masked`, `bad-setting` and so on) is a unit on this host.
///
/// # Errors
///
/// A message when `systemctl` failed or printed no state: the command cannot
/// tell, so it refuses rather than read that as no unit.
pub(crate) fn read_load_state(succeeded: bool, stdout: &str, stderr: &str) -> Result<bool, String> {
    let state = stdout.trim();
    if !succeeded || state.is_empty() {
        return Err(format!(
            "systemctl show --property=LoadState {UNIT} {}: {}",
            if succeeded {
                "printed no state"
            } else {
                "failed"
            },
            stderr.trim()
        ));
    }
    Ok(state != "not-found")
}

/// What a run that did not refuse did. None of them changed anything but
/// `Aborted`, which ended a freeze when there was one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// `--abort` ran.
    Aborted,
    /// `--dry-run` stopped after step 1.
    DryRun,
    /// Every refusal passed and step 1 ran; the rest of the promotion is
    /// not built yet, so the command stopped.
    NotBuilt,
}

/// `quoro cluster promote [--dry-run] [--ack-fence] [--accept-loss]
/// [--statement <text>]` and `quoro cluster promote --abort`.
pub async fn command(args: &[String]) -> ExitCode {
    let request = match parse_args(args) {
        Ok(request) => request,
        Err(message) => return crate::usage_error(&message),
    };
    match run(&request, &RealHost, &mut Out::printing()).await {
        Ok(Outcome::Aborted | Outcome::DryRun) => ExitCode::SUCCESS,
        // The promotion did not happen: a script must not read it as done.
        Ok(Outcome::NotBuilt) => ExitCode::from(1),
        Err(refusal) => {
            eprintln!("quoro cluster promote: {refusal}");
            ExitCode::from(1)
        }
    }
}

/// Runs `request` on `host`. The effective uid is read first and alone: a
/// run that is not root's stops before it reads anything else.
///
/// # Errors
///
/// The [`Refusal`] that stopped the run; nothing was changed then.
pub(crate) async fn run(
    request: &Request,
    host: &dyn Host,
    out: &mut Out,
) -> Result<Outcome, Refusal> {
    if host.euid() != Some(0) {
        return Err(Refusal::NotRoot);
    }
    let paths = host.paths().map_err(Refusal::Unreadable)?;
    match request {
        Request::Abort => abort(&paths, out).await,
        Request::Promote(flags) => promote(flags, &paths, host, out).await,
    }
}

/// The refusals, then step 1; then a dry run stops, and so does every other
/// run for now.
async fn promote(
    flags: &Flags,
    paths: &Paths,
    host: &dyn Host,
    out: &mut Out,
) -> Result<Outcome, Refusal> {
    let passed = refusals::check(flags, paths, host).await?;
    out.say(format!(
        "node {} of cluster {}: every refusal before a promotion passed",
        passed.identity.node_id, passed.identity.cluster_id
    ));
    out.say(keys_line(&passed));
    show_position(&passed.reads, flags.accept_loss, host.now(), out);
    if flags.dry_run {
        out.say(
            "dry run: stopped after step 1. Step 2's vote pre-check is not built yet, so no \
             voter was asked. Nothing was changed.",
        );
        return Ok(Outcome::DryRun);
    }
    out.say(not_built(flags));
    Ok(Outcome::NotBuilt)
}

/// The keys in use the refusals opened.
fn keys_line(passed: &Passed) -> String {
    let named: Vec<String> = passed
        .keys
        .iter()
        .map(|(kind, key_id)| format!("{} {key_id}", kind_name(*kind)))
        .collect();
    format!(
        "keys in use, each opened from its seal with this machine's custody key: {}",
        named.join(", ")
    )
}

/// The short name of a key kind, as the command prints it.
pub(crate) fn kind_name(kind: KeyKind) -> &'static str {
    match kind {
        KeyKind::Signing => "signing key",
        KeyKind::Dek => "data-encryption key",
        KeyKind::Tls => "TLS key",
    }
}

/// Step 1 (section 7.3): the agent's last verified `seq`, the `head_seq` the
/// primary last reported with its time, and the difference.
fn show_position(reads: &PromotionReads, accept_loss: bool, now: SystemTime, out: &mut Out) {
    out.say("step 1, the log position:");
    out.say(format!("  last verified seq: {}", reads.log_seq));
    match &reads.head {
        Some((head, at)) => {
            let age = age_seconds(at, now).map_or_else(String::new, |s| format!(", {s} s ago"));
            out.say(format!(
                "  head_seq the primary last reported: {head}, received {at}{age}"
            ));
            out.say(format!(
                "  difference: {}",
                head.saturating_sub(reads.log_seq)
            ));
        }
        None => {
            out.say("  head_seq the primary last reported: none stored");
            out.say("  difference: unknown");
        }
    }
    if accept_loss {
        out.say(format!(
            "  with --accept-loss, every entry past seq {} is lost to the cluster, replicated or \
             not",
            reads.log_seq
        ));
    } else {
        out.say(
            "  entries the old primary wrote and this machine never received are lost to the \
             cluster; a machine that passes the refusals holds every entry the old primary \
             counted as replicated",
        );
    }
    if let Some(at) = &reads.frozen_at {
        out.say(format!(
            "  the agent is frozen since {at} by a promotion that stopped; \
             quoro cluster promote --abort ends the freeze"
        ));
    }
}

/// Seconds from the RFC 3339 time `at` to `now`; `None` when `at` does not
/// parse or is after `now`.
fn age_seconds(at: &str, now: SystemTime) -> Option<u64> {
    let at =
        time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()?;
    now.duration_since(SystemTime::from(at))
        .ok()
        .map(|age| age.as_secs())
}

/// What a run that passed every refusal says before it stops.
fn not_built(flags: &Flags) -> String {
    let mut text = "the rest of the promotion (the term, the freeze, the database build, the \
                    primary-status move, the keys and the start) is not built yet, so the command \
                    stops here. Nothing was changed."
        .to_string();
    if flags.ack_fence {
        text.push_str(
            " With --ack-fence the promotion skips step 3's wait and declares the old \
             primary's node lost.",
        );
    }
    if let Some(statement) = &flags.statement {
        text.push_str(&format!(
            " The statement it records as the justification: {statement:?}."
        ));
    }
    text
}

/// `--abort` (section 7.3, ADR 0280): ends a freeze a stopped promotion
/// left. It takes the promotion lock without waiting, as
/// `freeze::abort` does, so it is refused while a promotion runs on this
/// host, and holds it while it reads the freeze and ends it, so no
/// promotion starts or freezes in between. The store is read through a
/// read-only open.
async fn abort(paths: &Paths, out: &mut Out) -> Result<Outcome, Refusal> {
    let dir = &paths.node_data_dir;
    let store_path = dir.join(AGENT_STORE);
    if fs::symlink_metadata(&store_path).is_err() {
        return Err(Refusal::NoAgentStore(store_path));
    }
    let _lock = match PromotionLock::acquire(dir) {
        Ok(lock) => lock,
        Err(AgentError::PromotionRunning) => return Err(Refusal::PromotionRunning),
        Err(e) => {
            return Err(Refusal::Unreadable(format!(
                "cannot take the promotion lock: {e}"
            )));
        }
    };
    let frozen_at = {
        let store = NodeStore::open_read_only(dir).await.map_err(|e| {
            Refusal::Unreadable(format!(
                "cannot open the agent's store {}: {e}",
                store_path.display()
            ))
        })?;
        store
            .meta(PROMOTION_FREEZE)
            .await
            .map_err(|e| Refusal::Unreadable(format!("cannot read the freeze: {e}")))?
    };
    freeze::unfreeze(dir)
        .await
        .map_err(|e| Refusal::Unreadable(format!("cannot end the freeze: {e}")))?;
    match &frozen_at {
        Some(at) => out.say(format!(
            "ended the promotion freeze set at {at}; the agent dials its primary again within \
             a second"
        )),
        None => out.say("the agent was not frozen; there was no freeze to end"),
    }
    Ok(Outcome::Aborted)
}
