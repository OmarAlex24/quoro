//! `quoro node rejoin`: makes a demoted primary's host a node again
//! (`node-agent-protocol.md` section 7.4, ADR 0230, issue #728).
//!
//! Run as root on the old primary, the command checks that the control plane
//! here is no longer the primary, then moves the control-plane data directory
//! and the control-plane unit into a moved-aside copy next to the data
//! directory, `<basename>.demoted-t<term>-<UTC time>`. It deletes nothing: the
//! copy may hold writes the new primary lacks, and the unit file keeps the
//! operator's edits. The node agent, its data directory and `quoro.toml` are
//! left as they are, so the agent keeps running and reaches the new primary.
//!
//! The order is the contract's. The check runs first and changes nothing.
//! A forced run then sends its statement to the system journal, before
//! anything changes. Then the copy is created with `rejoin-started` in it,
//! before anything else, then a forced run's `rejoin-statement`; the unit is
//! stopped, disabled and confirmed inactive; the data directory is renamed
//! into the copy, then the unit file and its drop-in directory; `rejoin-done`
//! is written last. A unit file that is a symbolic link is copied into the
//! copy after the stop and before the disable, which removes such a link.
//! When `[node] local` is true, the control-plane unit also runs this host's
//! node agent: the command warns, before anything changes, that stopping the
//! unit stops the agent and that `quoro-node.service` must be started
//! afterwards, and goes on. A run interrupted on the way leaves the copy
//! without `rejoin-done`, and the next run finds it and finishes the moves
//! into it.
//!
//! The systemd calls go through [`Systemd`] and the file-system calls the
//! tests replace through [`Fs`], so the tests drive every failure with
//! fakes; the unit directory, the journal socket and the clock are fields of
//! [`Options`].

use sqlx::Connection as _;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::os::unix::fs::{PermissionsExt as _, chown, fchown, lchown, symlink};
use std::os::unix::net::UnixDatagram;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The control-plane unit.
const UNIT: &str = "quoro.service";
/// The control-plane unit's drop-in directory.
const DROP_IN: &str = "quoro.service.d";
/// The directory inside the copy that holds the unit file and its drop-ins.
const SYSTEMD_DIR: &str = "systemd";
/// The control-plane database inside the data directory.
const DATABASE: &str = "quoro.db";
/// The node agent's store inside the node data directory.
const AGENT_STORE: &str = "node.db";
/// Written first in the copy, before anything else changes.
const STARTED: &str = "rejoin-started";
/// A forced run's statement, next to `rejoin-started`.
const STATEMENT: &str = "rejoin-statement";
/// Written last, once everything moved.
const DONE: &str = "rejoin-done";
/// The highest term any machine stores (section 8.8, "The term ceiling").
const TERM_CEILING: u64 = (1 << 53) - 1;
/// The states of `systemctl is-active` in which the unit still runs.
const RUNNING_STATES: [&str; 4] = ["active", "activating", "deactivating", "reloading"];

/// The command line of `quoro node rejoin`, read by [`parse_args`].
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Request {
    /// `--config PATH`.
    pub config: Option<PathBuf>,
    /// The statement of `--force --statement "<why>"`; `None` without
    /// `--force`.
    pub statement: Option<String>,
}

/// Reads `[--config PATH] [--force --statement "<why>"]`.
///
/// # Errors
///
/// A usage message: an unknown option or a positional argument, a flag
/// without its value, `--force` without a non-empty `--statement`, or
/// `--statement` without `--force`.
pub fn parse_args(args: &[String]) -> Result<Request, String> {
    let mut config = None;
    let mut force = false;
    let mut statement: Option<String> = None;
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(format!("unexpected argument '{argument}'"));
        };
        match name {
            "force" => force = true,
            "config" | "statement" => {
                let Some(value) = args.next() else {
                    return Err(format!("'{argument}' needs a value"));
                };
                if name == "config" {
                    config = Some(PathBuf::from(value));
                } else {
                    statement = Some(value.clone());
                }
            }
            _ => return Err(format!("unknown option '{argument}'")),
        }
    }
    match (force, statement) {
        (true, None) => Err("node rejoin --force needs --statement \"<why>\"".to_string()),
        (false, Some(_)) => Err("node rejoin --statement needs --force".to_string()),
        (true, Some(text)) if text.trim().is_empty() => {
            Err("node rejoin --statement needs a non-empty statement".to_string())
        }
        (_, statement) => Ok(Request { config, statement }),
    }
}

/// The systemd calls the command makes on `quoro.service`.
pub trait Systemd {
    /// Whether systemd knows the unit (its load state is not `not-found`).
    fn known(&self) -> bool;
    /// `systemctl stop quoro.service`.
    ///
    /// # Errors
    ///
    /// A message when the stop failed.
    fn stop(&self) -> Result<(), String>;
    /// `systemctl disable quoro.service`.
    ///
    /// # Errors
    ///
    /// A message when the disable failed.
    fn disable(&self) -> Result<(), String>;
    /// What `systemctl is-active quoro.service` prints: `inactive`,
    /// `failed`, `active` and so on.
    ///
    /// # Errors
    ///
    /// A message when the state could not be read.
    fn state(&self) -> Result<String, String>;
    /// `systemctl daemon-reload`.
    ///
    /// # Errors
    ///
    /// A message when the reload failed.
    fn daemon_reload(&self) -> Result<(), String>;
}

/// The host's `systemctl`.
pub struct RealSystemd;

impl RealSystemd {
    fn run(args: &[&str]) -> Result<std::process::Output, String> {
        std::process::Command::new("systemctl")
            .args(args)
            .output()
            .map_err(|e| format!("cannot run systemctl {}: {e}", args.join(" ")))
    }

    fn check(args: &[&str]) -> Result<(), String> {
        let output = Self::run(args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "systemctl {}: {}: {}",
                args.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }
}

impl Systemd for RealSystemd {
    fn known(&self) -> bool {
        Self::run(&["show", "--property=LoadState", "--value", UNIT]).is_ok_and(|output| {
            let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
            output.status.success() && !state.is_empty() && state != "not-found"
        })
    }

    fn stop(&self) -> Result<(), String> {
        Self::check(&["stop", UNIT])
    }

    fn disable(&self) -> Result<(), String> {
        Self::check(&["disable", UNIT])
    }

    fn state(&self) -> Result<String, String> {
        // `is-active` exits non-zero for every state but `active`, so the
        // state is read from what it prints, not from its status.
        let output = Self::run(&["is-active", UNIT])?;
        let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if state.is_empty() {
            Err(format!(
                "systemctl is-active {UNIT} printed nothing: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        } else {
            Ok(state)
        }
    }

    fn daemon_reload(&self) -> Result<(), String> {
        Self::check(&["daemon-reload"])
    }
}

/// The file-system calls a test replaces: the moves of the unit file and
/// its drop-in directory, so a rename can cross devices or a removal fail,
/// the device of a directory, and a point between two reads of a database.
#[derive(Clone, Copy)]
pub struct Fs {
    /// Renames a file, a symbolic link or a directory.
    pub rename: fn(&Path, &Path) -> io::Result<()>,
    /// Removes a file or a symbolic link, after its copy is in place.
    pub remove_file: fn(&Path) -> io::Result<()>,
    /// Removes a directory tree, after its copy is in place.
    pub remove_tree: fn(&Path) -> io::Result<()>,
    /// The device of the filesystem a directory is on.
    pub device: fn(&Path) -> io::Result<u64>,
    /// Runs after an `immutable` read of a database and before the test for
    /// a `-wal` that sends it again; does nothing in production.
    pub after_immutable_read: fn(&Path),
}

impl Fs {
    /// The real calls.
    pub const REAL: Self = Self {
        rename: real_rename,
        remove_file: real_remove_file,
        remove_tree: real_remove_tree,
        device: real_device,
        after_immutable_read: no_hook,
    };
}

fn real_device(path: &Path) -> io::Result<u64> {
    fs::metadata(path).map(|metadata| metadata.dev())
}

fn no_hook(_: &Path) {}

fn real_rename(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

fn real_remove_file(path: &Path) -> io::Result<()> {
    fs::remove_file(path)
}

fn real_remove_tree(path: &Path) -> io::Result<()> {
    fs::remove_dir_all(path)
}

/// Where the command looks and what it uses; production fills it with the
/// host's real paths ([`command`]).
pub struct Options {
    /// The control-plane data directory (`quoro.toml` `data_dir`).
    pub data_dir: PathBuf,
    /// The node agent's data directory (`[node] data_dir`).
    pub node_data_dir: PathBuf,
    /// `/etc/systemd/system`.
    pub unit_dir: PathBuf,
    /// journald's native socket, `/run/systemd/journal/socket`.
    pub journal_socket: PathBuf,
    /// `/proc/self/mountinfo`, to tell whether the data directory is a mount
    /// point.
    pub mountinfo: PathBuf,
    /// The effective uid of this process, when it could be read.
    pub euid: Option<u32>,
    /// The clock.
    pub now: fn() -> SystemTime,
    /// The command line `rejoin-started` records, without the statement.
    pub command_line: String,
    /// `[node] local`: the control-plane unit also runs this host's node
    /// agent, so stopping it stops the agent; the command warns and goes on.
    pub local_agent: bool,
    /// The file-system calls the tests replace.
    pub fs: Fs,
}

/// What a run that did not refuse did.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No control-plane database, no unit and no unfinished copy.
    NothingToDo,
    /// Nothing in place and a finished copy: this copy.
    AlreadyRejoined(PathBuf),
    /// The host is a node again; everything is in this copy.
    Rejoined(PathBuf),
}

/// The lines the command prints on stdout, kept for the tests.
pub struct Out {
    /// Every line, in order.
    pub lines: Vec<String>,
    echo: bool,
}

impl Out {
    /// Prints each line as it comes.
    #[must_use]
    pub fn printing() -> Self {
        Self {
            lines: Vec::new(),
            echo: true,
        }
    }

    /// Keeps the lines without printing them.
    #[cfg(test)]
    fn quiet() -> Self {
        Self {
            lines: Vec::new(),
            echo: false,
        }
    }

    fn say(&mut self, line: impl Into<String>) {
        let line = line.into();
        if self.echo {
            println!("{line}");
        }
        self.lines.push(line);
    }
}

/// `quoro node rejoin [--config PATH] [--force --statement "<why>"]`.
pub async fn command(args: &[String]) -> ExitCode {
    let request = match parse_args(args) {
        Ok(request) => request,
        Err(message) => return crate::usage_error(&message),
    };
    // Root first, before the configuration is read: the command moves root's
    // and the control plane's files. It never drops to the data directory's
    // owner (`privilege.rs`): it renames that directory.
    let euid = effective_uid();
    if euid != Some(0) {
        eprintln!("quoro node rejoin: {}", not_root());
        return ExitCode::from(1);
    }
    let path = request
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from(crate::config::DEFAULT_PATH));
    let Some(config) = crate::load_config("node rejoin", &path) else {
        return ExitCode::from(1);
    };
    let options = Options {
        data_dir: config.data_dir,
        node_data_dir: config.node.data_dir,
        unit_dir: PathBuf::from("/etc/systemd/system"),
        journal_socket: PathBuf::from("/run/systemd/journal/socket"),
        mountinfo: PathBuf::from("/proc/self/mountinfo"),
        euid,
        now: SystemTime::now,
        command_line: command_line(&request),
        local_agent: config.node.local,
        fs: Fs::REAL,
    };
    match rejoin(&request, &options, &RealSystemd, &mut Out::printing()).await {
        Ok(_) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("quoro node rejoin: {message}");
            ExitCode::from(1)
        }
    }
}

/// The command line `rejoin-started` records: the statement is in
/// `rejoin-statement`, not here.
fn command_line(request: &Request) -> String {
    let mut line = "quoro node rejoin".to_string();
    if let Some(config) = &request.config {
        line.push_str(&format!(" --config {}", config.display()));
    }
    if request.statement.is_some() {
        line.push_str(" --force --statement (in rejoin-statement)");
    }
    line
}

/// The effective uid: `/proc/self` belongs to the effective user of this
/// process (as in `privilege.rs`).
fn effective_uid() -> Option<u32> {
    fs::metadata("/proc/self")
        .ok()
        .map(|metadata| metadata.uid())
}

fn not_root() -> String {
    "run this as root on the host: it stops the control-plane unit and moves its data directory"
        .to_string()
}

/// One stored value: absent, or the text the database holds.
type Raw = Option<String>;

/// The value as a term: absent when it does not parse as one or is above the
/// term ceiling.
fn term_of(raw: &Raw) -> Option<u64> {
    raw.as_deref()?
        .parse::<u64>()
        .ok()
        .filter(|term| *term <= TERM_CEILING)
}

/// The value for a message.
fn describe(raw: &Raw) -> String {
    match (raw, term_of(raw)) {
        (None, _) => "absent".to_string(),
        (Some(_), Some(term)) => term.to_string(),
        (Some(text), None) => format!("{text:?}, not a term, so absent"),
    }
}

/// What the check read.
struct Readings {
    /// The control-plane database read, in place or in the unfinished copy.
    database: Option<PathBuf>,
    /// Its `meta.demoted_by_term` and `meta.term`, or why they could not be
    /// read.
    control_plane: Result<(Raw, Raw), String>,
    /// The agent's store.
    agent_store: PathBuf,
    /// Its `node_meta.highest_term`, or why it could not be read.
    agent: Result<Raw, String>,
}

impl Readings {
    fn demoted_by_term(&self) -> Option<u64> {
        self.control_plane
            .as_ref()
            .ok()
            .and_then(|(demoted, _)| term_of(demoted))
    }

    fn term(&self) -> Option<u64> {
        self.control_plane
            .as_ref()
            .ok()
            .and_then(|(_, term)| term_of(term))
    }

    fn highest_term(&self) -> Option<u64> {
        self.agent.as_ref().ok().and_then(term_of)
    }

    /// Case (b): the agent has seen a term above the database's. A missing
    /// `meta.term` means it cannot hold.
    fn agent_ahead(&self) -> Option<u64> {
        match (self.highest_term(), self.term()) {
            (Some(highest), Some(term)) if highest > term => Some(highest),
            _ => None,
        }
    }

    /// Both readings, for the operator.
    fn lines(&self) -> Vec<String> {
        let control_plane = match (&self.database, &self.control_plane) {
            (None, _) => "control-plane database: none".to_string(),
            (Some(path), Ok((demoted, term))) => format!(
                "control-plane database {}: demoted_by_term {}, term {}",
                path.display(),
                describe(demoted),
                describe(term)
            ),
            (Some(path), Err(e)) => format!(
                "control-plane database {}: unreadable ({e})",
                path.display()
            ),
        };
        let agent = match &self.agent {
            Ok(highest) => format!(
                "node agent store {}: highest_term {}",
                self.agent_store.display(),
                describe(highest)
            ),
            Err(e) => format!(
                "node agent store {}: unreadable ({e})",
                self.agent_store.display()
            ),
        };
        vec![control_plane, agent]
    }
}

/// Reads `keys` of the key-value table `table` in the SQLite database at
/// `path`, read-only, without a migration and without `Store::open`: the
/// control plane may be live and the agent is. It returns the values and
/// a warning for each side file it left as it is.
///
/// The control-plane database belongs to the control plane's user, and this
/// command runs as root. A read-only open of a WAL database whose `-wal` and
/// `-shm` are absent creates them. The bundled SQLite gives a side file that
/// a root process creates to the database file's owner; as a fallback, after
/// the read, this changes the owner of any side file whose owner differs
/// from the database file's to that owner, through the side file's open
/// descriptor, never through a link ([`give_side_files_to_owner`]). It does
/// not read through a child running as the owner: this command runs as root
/// and never re-executes itself as another user (`privilege.rs`).
///
/// When no `-wal` exists, the database is opened `immutable`, so the read
/// creates no side file at all and the listing of the directory stays the
/// same: without a `-wal`, every committed write is in the main file. A writer
/// can start between that test and the read, and an immutable read would then
/// miss its writes, so a `-wal` that exists after the read sends it again
/// through a plain read-only open, which reads the WAL.
async fn read_keys(
    path: &Path,
    table: &str,
    keys: &[&str],
    after_immutable_read: fn(&Path),
) -> Result<(Vec<Raw>, Vec<String>), String> {
    let wal = side_file(path, "-wal");
    let mut immutable = !exists(&wal);
    loop {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .read_only(true)
            .immutable(immutable)
            .busy_timeout(Duration::from_secs(5));
        let read = async {
            let mut connection = SqliteConnection::connect_with(&options).await?;
            let values = read_table(&mut connection, table, keys).await;
            connection.close().await?;
            values
        }
        .await
        .map_err(|e| e.to_string());
        let restored = give_side_files_to_owner(path);
        if immutable {
            after_immutable_read(path);
            if exists(&wal) {
                immutable = false;
                continue;
            }
        }
        let values = read?;
        return Ok((values, restored?));
    }
}

async fn read_table(
    connection: &mut SqliteConnection,
    table: &str,
    keys: &[&str],
) -> Result<Vec<Raw>, sqlx::Error> {
    let exists: Option<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_optional(&mut *connection)
            .await?;
    if exists.is_none() {
        return Ok(vec![None; keys.len()]);
    }
    let mut values = Vec::with_capacity(keys.len());
    for key in keys {
        let value: Option<Option<String>> = sqlx::query_scalar(&format!(
            "SELECT CAST(value AS TEXT) FROM {table} WHERE key = ?"
        ))
        .bind(key)
        .fetch_optional(&mut *connection)
        .await?;
        values.push(value.flatten());
    }
    Ok(values)
}

fn side_file(database: &Path, suffix: &str) -> PathBuf {
    let mut name = database.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// Changes the owner of each `-wal` and `-shm` next to `database` whose
/// owner differs from the database file's to that owner and group. Each one
/// is opened without following a link, and only a regular file with one
/// link is changed, through its open descriptor; anything else is left as
/// it is, with a warning in the result.
fn give_side_files_to_owner(database: &Path) -> Result<Vec<String>, String> {
    use rustix::fs::{Mode, OFlags};
    let owner = fs::metadata(database)
        .map_err(|e| format!("cannot read the owner of {}: {e}", database.display()))?;
    let mut warnings = Vec::new();
    for suffix in ["-wal", "-shm"] {
        let path = side_file(database, suffix);
        if !exists(&path) {
            continue;
        }
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
        let file = match rustix::fs::open(&path, flags, Mode::empty()) {
            Ok(descriptor) => fs::File::from(descriptor),
            Err(e) => {
                warnings.push(format!(
                    "left {} as it is: it cannot be opened as a file without following a link ({e})",
                    path.display()
                ));
                continue;
            }
        };
        let metadata = file
            .metadata()
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            warnings.push(format!(
                "left {} as it is: it is not a regular file with one link",
                path.display()
            ));
            continue;
        }
        if (metadata.uid(), metadata.gid()) == (owner.uid(), owner.gid()) {
            continue;
        }
        fchown(&file, Some(owner.uid()), Some(owner.gid())).map_err(|e| {
            format!(
                "cannot give {} to uid {} gid {}: {e}; the control plane may not open its database until it is",
                path.display(),
                owner.uid(),
                owner.gid()
            )
        })?;
    }
    Ok(warnings)
}

/// `path` rebuilt from its components: absolute, without `.`, `..`, a
/// repeated or a trailing `/`, and with a file name.
fn normalized(path: &Path, what: &str) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!(
            "the {what} {} is not an absolute path; nothing was changed",
            path.display()
        ));
    }
    let mut rebuilt = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => rebuilt.push("/"),
            Component::Normal(name) => rebuilt.push(name),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(format!(
                    "the {what} {} contains '..'; nothing was changed",
                    path.display()
                ));
            }
        }
    }
    match (rebuilt.parent(), rebuilt.file_name()) {
        (Some(parent), Some(name)) => Ok(parent.join(name)),
        _ => Err(format!(
            "the {what} {} has no file name; nothing was changed",
            path.display()
        )),
    }
}

/// Whether `path` exists, as itself (a dangling symbolic link exists).
fn exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

/// The moved-aside copies next to the data directory, by name.
struct Copies {
    unfinished: Vec<PathBuf>,
    finished: Vec<PathBuf>,
}

fn find_copies(parent: &Path, basename: &str) -> Result<Copies, String> {
    let prefix = format!("{basename}.demoted-t");
    let entries =
        fs::read_dir(parent).map_err(|e| format!("cannot read {}: {e}", parent.display()))?;
    let mut copies = Copies {
        unfinished: Vec::new(),
        finished: Vec::new(),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("cannot read {}: {e}", parent.display()))?;
        let name = entry.file_name();
        if name.to_str().is_some_and(|name| name.starts_with(&prefix))
            && fs::symlink_metadata(entry.path()).is_ok_and(|m| m.is_dir())
        {
            names.push(entry.path());
        }
    }
    names.sort();
    for copy in names {
        if exists(&copy.join(DONE)) {
            copies.finished.push(copy);
        } else if exists(&copy.join(STARTED)) {
            copies.unfinished.push(copy);
        }
        // A copy without `rejoin-started` is never adopted.
    }
    Ok(copies)
}

/// What to do with one of the unit's two paths.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    /// Neither in place nor in the copy.
    Absent,
    /// In place: move it.
    Move,
    /// In the copy only: moved before.
    Moved,
    /// In both, identical, after a copy across filesystems: remove the
    /// source.
    RemoveSource,
}

fn plan(source: &Path, target: &Path) -> Result<Plan, String> {
    match (exists(source), exists(target)) {
        (false, false) => Ok(Plan::Absent),
        (true, false) => Ok(Plan::Move),
        (false, true) => Ok(Plan::Moved),
        (true, true) => match same_tree(source, target) {
            Ok(true) => Ok(Plan::RemoveSource),
            Ok(false) => Err(format!(
                "{} and {} both exist and differ; nothing is overwritten: compare them and keep one",
                source.display(),
                target.display()
            )),
            Err(e) => Err(format!(
                "cannot compare {} with {}: {e}",
                source.display(),
                target.display()
            )),
        },
    }
}

/// Whether two paths hold the same tree: the same kinds, the same bytes, the
/// same link targets and the same names.
fn same_tree(a: &Path, b: &Path) -> io::Result<bool> {
    let (ma, mb) = (fs::symlink_metadata(a)?, fs::symlink_metadata(b)?);
    if ma.file_type().is_symlink() || mb.file_type().is_symlink() {
        return Ok(ma.file_type().is_symlink()
            && mb.file_type().is_symlink()
            && fs::read_link(a)? == fs::read_link(b)?);
    }
    if ma.is_file() && mb.is_file() {
        return Ok(
            ma.permissions().mode() == mb.permissions().mode() && fs::read(a)? == fs::read(b)?
        );
    }
    if ma.is_dir() && mb.is_dir() {
        let (na, nb) = (sorted_names(a)?, sorted_names(b)?);
        if na != nb {
            return Ok(false);
        }
        for name in na {
            if !same_tree(&a.join(&name), &b.join(&name))? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    Ok(false)
}

fn sorted_names(dir: &Path) -> io::Result<Vec<std::ffi::OsString>> {
    let mut names = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// Whether `dir` is a mount point in `mountinfo` (`proc(5)`): its fifth
/// field is the mount point, with spaces and the like as octal escapes.
fn is_mount_point(dir: &Path, mountinfo: &Path) -> Result<bool, String> {
    let canonical =
        fs::canonicalize(dir).map_err(|e| format!("cannot resolve {}: {e}", dir.display()))?;
    let text = fs::read_to_string(mountinfo)
        .map_err(|e| format!("cannot read {}: {e}", mountinfo.display()))?;
    Ok(text.lines().any(|line| {
        line.split(' ')
            .nth(4)
            .is_some_and(|point| Path::new(&unescape_mount(point)) == canonical)
    }))
}

fn unescape_mount(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1..i + 4]
                .iter()
                .all(|b| (b'0'..=b'7').contains(b))
        {
            let value =
                (bytes[i + 1] - b'0') * 64 + (bytes[i + 2] - b'0') * 8 + (bytes[i + 3] - b'0');
            out.push(value);
            i += 4;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Creates `dir` inside the copy, mode `0700`, when it is missing.
fn make_dir(dir: &Path, copy: &Path) -> Result<(), String> {
    if exists(dir) {
        return Ok(());
    }
    fs::DirBuilder::new()
        .mode(0o700)
        .create(dir)
        .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    sync_dir(copy)
}

/// Syncs a directory, so a rename or a new entry in it is durable.
fn sync_dir(dir: &Path) -> Result<(), String> {
    fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|e| format!("cannot sync {}: {e}", dir.display()))
}

/// Writes a new file `name` in `dir`, mode `0600`, syncs it and `dir`. It
/// never overwrites.
fn write_new(dir: &Path, name: &str, text: &str) -> Result<(), String> {
    let path = dir.join(name);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("cannot create {}: {e}", path.display()))?;
    file.write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    sync_dir(dir)
}

/// The UTC civil time of `time`: year, month, day, hour, minute, second.
fn civil(time: SystemTime) -> (i64, u32, u32, u32, u32, u32) {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| i64::try_from(since.as_secs()).unwrap_or(0));
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let narrow = |value: i64| u32::try_from(value).unwrap_or(0);
    (
        year,
        narrow(month),
        narrow(day),
        narrow(rest / 3600),
        narrow(rest % 3600 / 60),
        narrow(rest % 60),
    )
}

/// `YYYYMMDDTHHMMSSZ`, for the copy's name.
fn compact_utc(time: SystemTime) -> String {
    let (y, mo, d, h, mi, s) = civil(time);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// RFC 3339 in UTC, for the files and the journal.
fn rfc3339(time: SystemTime) -> String {
    let (y, mo, d, h, mi, s) = civil(time);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Sends one entry to journald's native socket (`systemd.journal-fields(7)`,
/// the native protocol). `journalctl -u quoro-node` shows it: for a unit it
/// also matches entries from uid 0 that carry `OBJECT_SYSTEMD_UNIT` with the
/// unit's name (systemd's `add_matches_for_unit_full`), and this command runs
/// as root.
fn send_to_journal(socket: &Path, message: &str) -> io::Result<()> {
    let mut entry = Vec::new();
    for (key, value) in [
        ("MESSAGE", message),
        ("PRIORITY", "5"),
        ("SYSLOG_IDENTIFIER", "quoro"),
        ("OBJECT_SYSTEMD_UNIT", "quoro-node.service"),
    ] {
        entry.extend_from_slice(key.as_bytes());
        if value.contains('\n') {
            // A value with a newline is sent with its length, little-endian.
            entry.push(b'\n');
            entry.extend_from_slice(&(value.len() as u64).to_le_bytes());
        } else {
            entry.push(b'=');
        }
        entry.extend_from_slice(value.as_bytes());
        entry.push(b'\n');
    }
    let datagram = UnixDatagram::unbound()?;
    let sent = datagram.send_to(&entry, socket)?;
    if sent == entry.len() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "sent {sent} of {} bytes",
            entry.len()
        )))
    }
}

/// Moves `source` to `target`, a file, a symbolic link (the link itself) or
/// a directory. A rename first; across filesystems, a copy that keeps modes,
/// owners and links under a temporary name in the target's directory, synced
/// and renamed into place, and only then the source's removal.
fn move_entry(fs_calls: &Fs, source: &Path, target: &Path) -> Result<(), String> {
    let (source_parent, target_parent) = (parent_of(source)?, parent_of(target)?);
    match (fs_calls.rename)(source, target) {
        Ok(()) => {
            sync_dir(target_parent)?;
            sync_dir(source_parent)
        }
        Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {
            let name = target
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            let temporary = target_parent.join(format!(".{name}.partial-{}", unique_suffix()));
            copy_tree(source, &temporary).map_err(|e| {
                format!(
                    "cannot copy {} to {}: {e}",
                    source.display(),
                    temporary.display()
                )
            })?;
            sync_dir(target_parent)?;
            if exists(target) {
                return Err(format!(
                    "{} appeared during the copy; nothing is overwritten",
                    target.display()
                ));
            }
            fs::rename(&temporary, target).map_err(|e| {
                format!(
                    "cannot rename {} to {}: {e}",
                    temporary.display(),
                    target.display()
                )
            })?;
            sync_dir(target_parent)?;
            remove_source(fs_calls, source)
        }
        Err(e) => Err(format!(
            "cannot move {} to {}: {e}",
            source.display(),
            target.display()
        )),
    }
}

/// A suffix for a temporary name that no earlier run used: the time in
/// nanoseconds.
fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos())
}

/// Removes the source of a move once its copy is in place, and syncs its
/// directory.
fn remove_source(fs_calls: &Fs, source: &Path) -> Result<(), String> {
    let is_dir = fs::symlink_metadata(source).is_ok_and(|m| m.is_dir());
    let removed = if is_dir {
        (fs_calls.remove_tree)(source)
    } else {
        (fs_calls.remove_file)(source)
    };
    removed.map_err(|e| {
        format!(
            "cannot remove {} after copying it: {e}; a rerun compares it with its copy and finishes",
            source.display()
        )
    })?;
    sync_dir(parent_of(source)?)
}

/// Copies a file, a symbolic link or a directory tree, with modes and
/// owners, links as links, each file synced.
fn copy_tree(source: &Path, target: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    let kind = metadata.file_type();
    if kind.is_symlink() {
        symlink(fs::read_link(source)?, target)?;
        lchown(target, Some(metadata.uid()), Some(metadata.gid()))?;
    } else if kind.is_file() {
        let mode = metadata.permissions().mode() & 0o7777;
        let mut input = fs::File::open(source)?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(target)?;
        let mut buffer = Vec::new();
        input.read_to_end(&mut buffer)?;
        output.write_all(&buffer)?;
        chown(target, Some(metadata.uid()), Some(metadata.gid()))?;
        fs::set_permissions(target, fs::Permissions::from_mode(mode))?;
        output.sync_all()?;
    } else if kind.is_dir() {
        let mode = metadata.permissions().mode() & 0o7777;
        fs::DirBuilder::new().mode(0o700).create(target)?;
        for name in sorted_names(source)? {
            copy_tree(&source.join(&name), &target.join(&name))?;
        }
        chown(target, Some(metadata.uid()), Some(metadata.gid()))?;
        fs::set_permissions(target, fs::Permissions::from_mode(mode))?;
        fs::File::open(target)?.sync_all()?;
    } else {
        return Err(io::Error::other(format!(
            "{} is neither a file, a link nor a directory",
            source.display()
        )));
    }
    Ok(())
}

fn parent_of(path: &Path) -> Result<&Path, String> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| format!("{} has no parent directory", path.display()))
}

/// The refusal of a host whose control plane does not count as demoted.
fn not_demoted(readings: &Readings) -> String {
    let mut message = "this host's control plane does not count as demoted, so it may still be the cluster's primary; nothing was changed".to_string();
    for line in readings.lines() {
        message.push_str("\n  ");
        message.push_str(&line);
    }
    message.push_str(
        "\nIt counts as demoted when its database records demoted_by_term, or when the node agent \
         has seen a term above the database's term (node-agent-protocol.md section 7.4). If the \
         agent has not reached the new primary yet, wait until it does and run this again.\
         \nAfter a promotion on this host that stopped once it had written its new database \
         (quoro cluster promote --abort, section 7.3), the host was never demoted: move that \
         database aside with quoro node rejoin --force --statement \"<why>\".",
    );
    message
}

/// Runs the command with `options` (section 7.4).
///
/// # Errors
///
/// The refusal or the failure, for stderr; the lines printed before it are in
/// `out`.
pub async fn rejoin(
    request: &Request,
    options: &Options,
    systemd: &dyn Systemd,
    out: &mut Out,
) -> Result<Outcome, String> {
    if options.euid != Some(0) {
        return Err(not_root());
    }
    let data_dir = &normalized(&options.data_dir, "data directory")?;
    let node_data_dir = normalized(&options.node_data_dir, "node data directory")?;
    let parent = parent_of(data_dir)?;
    let basename = data_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{} has no usable name", data_dir.display()))?
        .to_string();
    if node_data_dir.starts_with(data_dir) {
        return Err(format!(
            "the node data directory {} is inside {}; moving it would move the agent's data too. Nothing was changed",
            node_data_dir.display(),
            data_dir.display()
        ));
    }

    // Step 3: an unfinished copy first.
    let copies = find_copies(parent, &basename)?;
    if copies.unfinished.len() > 1 {
        let mut message = "more than one unfinished copy (with rejoin-started and no rejoin-done); nothing was changed. Keep the one this rejoin should finish and rename the others' rejoin-started:".to_string();
        for copy in &copies.unfinished {
            message.push_str(&format!("\n  {}", copy.display()));
        }
        return Err(message);
    }
    let resume = copies.unfinished.first().cloned();

    let unit_file = options.unit_dir.join(UNIT);
    let drop_in = options.unit_dir.join(DROP_IN);
    let has_unit = systemd.known() || exists(&unit_file);
    let in_place_database = data_dir.join(DATABASE);
    let data_in_place = exists(data_dir);

    // Step 8: nothing to do.
    if resume.is_none() && !exists(&in_place_database) && !has_unit {
        if let Some(copy) = copies.finished.last() {
            out.say(format!(
                "this host already rejoined: its control plane is in {}, and it has no control-plane database or unit",
                copy.display()
            ));
            return Ok(Outcome::AlreadyRejoined(copy.clone()));
        }
        out.say("this host has no control plane to move aside: no control-plane database, no control-plane unit and no unfinished copy");
        return Ok(Outcome::NothingToDo);
    }

    let moved_data = resume.as_ref().map(|copy| copy.join(&basename));
    if data_in_place && moved_data.as_deref().is_some_and(exists) {
        return Err(format!(
            "the data directory exists both in place, {}, and in the copy, {}; nothing is overwritten: compare them and keep one",
            data_dir.display(),
            moved_data.as_deref().unwrap_or(data_dir).display()
        ));
    }

    // Step 4: the check, on the database where it now is.
    let database = if data_in_place {
        Some(in_place_database).filter(|path| exists(path))
    } else {
        moved_data
            .as_ref()
            .map(|dir| dir.join(DATABASE))
            .filter(|path| exists(path))
    };
    let hook = options.fs.after_immutable_read;
    let mut warnings = Vec::new();
    let control_plane = match &database {
        Some(path) => read_keys(path, "meta", &["demoted_by_term", "term"], hook)
            .await
            .map(|(values, mut left)| {
                warnings.append(&mut left);
                (values[0].clone(), values[1].clone())
            }),
        None => Ok((None, None)),
    };
    let agent_store = node_data_dir.join(AGENT_STORE);
    let agent = if exists(&agent_store) {
        read_keys(&agent_store, "node_meta", &["highest_term"], hook)
            .await
            .map(|(values, mut left)| {
                warnings.append(&mut left);
                values[0].clone()
            })
    } else {
        Ok(None)
    };
    for warning in warnings {
        out.say(format!("warning: {warning}"));
    }
    let readings = Readings {
        database,
        control_plane,
        agent_store,
        agent,
    };
    for line in readings.lines() {
        out.say(line);
    }
    let earlier_statement = resume
        .as_ref()
        .map(|copy| copy.join(STATEMENT))
        .filter(|path| exists(path));
    let demoted = readings.demoted_by_term();
    let ahead = readings.agent_ahead();
    if let Some(path) = &earlier_statement {
        out.say(format!(
            "the statement of the earlier forced run stands ({}); the check is skipped",
            path.display()
        ));
        if request.statement.is_some() {
            out.say("the statement given now is not recorded: an earlier one is never overwritten");
        }
    } else if request.statement.is_some() {
        out.say("--force: the check is skipped and the statement is recorded");
    } else if let Some(term) = demoted {
        out.say(format!("the database records demoted_by_term {term}"));
    } else if let Some(highest) = ahead {
        out.say(format!(
            "the node agent has seen term {highest}, above the database's"
        ));
    } else {
        return Err(not_demoted(&readings));
    }
    let record_statement = request
        .statement
        .as_deref()
        .filter(|_| earlier_statement.is_none());
    if options.local_agent && has_unit {
        out.say(format!(
            "warning: quoro.toml has [node] local = true (the default when it is not set), so {UNIT} also runs this host's node agent: stopping {UNIT} stops that agent too. After this command, start quoro-node.service (systemctl enable --now quoro-node.service) so the agent runs again"
        ));
    }

    // Step 7, preflight: nothing changes before all of these hold.
    if data_in_place {
        let metadata = fs::symlink_metadata(data_dir)
            .map_err(|e| format!("cannot read {}: {e}", data_dir.display()))?;
        if !metadata.is_dir() {
            return Err(format!(
                "{} is not a directory (a symbolic link moves only the link); nothing was changed",
                data_dir.display()
            ));
        }
        if is_mount_point(data_dir, &options.mountinfo)? {
            return Err(format!(
                "{} is a mount point, so it cannot be renamed into the copy; nothing was changed",
                data_dir.display()
            ));
        }
        let device = |path: &Path| {
            (options.fs.device)(path).map_err(|e| format!("cannot read {}: {e}", path.display()))
        };
        if device(data_dir)? != device(parent)? {
            return Err(format!(
                "{} is on another filesystem than {}, so it cannot be renamed into the copy; nothing was changed",
                data_dir.display(),
                parent.display()
            ));
        }
    }
    let now = (options.now)();
    let copy = match &resume {
        Some(copy) => copy.clone(),
        None => {
            let (term, why) = if let Some(term) = demoted {
                (term, None)
            } else if let Some(highest) = ahead {
                (highest, None)
            } else if let Some(term) = readings.term() {
                (term, None)
            } else {
                (
                    0,
                    Some("the database's term is unreadable, so the copy is named t0"),
                )
            };
            if let Some(why) = why {
                out.say(why);
            }
            let copy = parent.join(format!("{basename}.demoted-t{term}-{}", compact_utc(now)));
            if exists(&copy) {
                return Err(format!(
                    "{} already exists; nothing was changed",
                    copy.display()
                ));
            }
            copy
        }
    };
    let systemd_dir = copy.join(SYSTEMD_DIR);
    let unit_target = systemd_dir.join(UNIT);
    let drop_in_target = systemd_dir.join(DROP_IN);
    let data_target = copy.join(&basename);
    if resume.is_none() {
        // A new copy holds nothing yet; this only guards the names.
        for target in [&data_target, &unit_target, &drop_in_target] {
            if exists(target) {
                return Err(format!(
                    "{} already exists; nothing was changed",
                    target.display()
                ));
            }
        }
    }
    let unit_plan = plan(&unit_file, &unit_target)?;
    let drop_in_plan = plan(&drop_in, &drop_in_target)?;

    // Step 6: the statement in the journal, before anything changes.
    if let Some(statement) = record_statement {
        let message = format!("quoro node rejoin --force at {}: {statement}", rfc3339(now));
        send_to_journal(&options.journal_socket, &message).map_err(|e| {
            format!(
                "cannot record the statement in the system journal through {}: {e}; nothing was changed",
                options.journal_socket.display()
            )
        })?;
        out.say("the statement is recorded in the journal of quoro-node.service");
    }

    // The copy, then `rejoin-started` before anything else.
    if resume.is_none() {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&copy)
            .map_err(|e| format!("cannot create {}: {e}", copy.display()))?;
        fs::set_permissions(&copy, fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("cannot set the mode of {}: {e}", copy.display()))?;
        sync_dir(parent)?;
        write_new(
            &copy,
            STARTED,
            &format!(
                "started: {}\ncommand: {}\n",
                rfc3339(now),
                options.command_line
            ),
        )?;
        out.say(format!("created {}", copy.display()));
    } else {
        out.say(format!(
            "finishing the rejoin started in {}",
            copy.display()
        ));
    }
    if let Some(statement) = record_statement {
        write_new(
            &copy,
            STATEMENT,
            &format!("recorded: {}\nstatement: {statement}\n", rfc3339(now)),
        )?;
    }

    // The unit: stopped, disabled and confirmed inactive before any move.
    let interrupted = |what: &str| {
        format!(
            "{what}; nothing more was moved. {} keeps rejoin-started, and running quoro node rejoin again finishes it",
            copy.display()
        )
    };
    let mut unit_plan = unit_plan;
    if has_unit {
        systemd
            .stop()
            .map_err(|e| interrupted(&format!("cannot stop {UNIT}: {e}")))?;
        if unit_plan == Plan::Move
            && fs::symlink_metadata(&unit_file).is_ok_and(|m| m.file_type().is_symlink())
        {
            // `systemctl disable` removes a unit file that is a link to a
            // file outside systemd's unit path: it undoes `systemctl link`.
            // So the link is copied into the copy before the disable, and
            // its source is removed at its step below if the disable left
            // it. A rerun then finds the link in the copy.
            make_dir(&systemd_dir, &copy)?;
            // An interrupted run may have left the temporary name: it is
            // reused when it matches the link, and otherwise kept, with a
            // fresh name used instead.
            let mut temporary = systemd_dir.join(format!(".{UNIT}.partial-link"));
            let mut reuse = false;
            if exists(&temporary) {
                if same_tree(&unit_file, &temporary).unwrap_or(false) {
                    reuse = true;
                } else {
                    temporary =
                        systemd_dir.join(format!(".{UNIT}.partial-link-{}", unique_suffix()));
                }
            }
            let copied = if reuse {
                Ok(())
            } else {
                copy_tree(&unit_file, &temporary)
            };
            copied
                .and_then(|()| fs::rename(&temporary, &unit_target))
                .map_err(|e| {
                    interrupted(&format!(
                        "cannot copy the link {} into {}: {e}",
                        unit_file.display(),
                        unit_target.display()
                    ))
                })?;
            sync_dir(&systemd_dir)?;
            unit_plan = Plan::RemoveSource;
        }
        if unit_plan == Plan::Moved {
            // `systemctl disable` refuses a unit whose file is gone; an
            // earlier run disabled it before it moved the file.
            out.say(format!(
                "{UNIT} was disabled before its unit file moved; not disabling again"
            ));
        } else {
            systemd
                .disable()
                .map_err(|e| interrupted(&format!("cannot disable {UNIT}: {e}")))?;
        }
        let state = systemd
            .state()
            .map_err(|e| interrupted(&format!("cannot tell whether {UNIT} stopped: {e}")))?;
        if RUNNING_STATES.contains(&state.as_str()) {
            return Err(interrupted(&format!("{UNIT} is still {state}")));
        }
        out.say(format!("stopped and disabled {UNIT} ({state})"));
    }

    // The data directory: one rename, never a copy.
    let mut moves = Vec::new();
    if data_in_place {
        fs::rename(data_dir, &data_target).map_err(|e| {
            interrupted(&format!(
                "cannot rename {} to {}: {e}",
                data_dir.display(),
                data_target.display()
            ))
        })?;
        sync_dir(&copy)?;
        sync_dir(parent)?;
        moves.push((data_dir.clone(), data_target.clone()));
    }

    // The unit file, then its drop-ins.
    for (source, target, step) in [
        (&unit_file, &unit_target, &unit_plan),
        (&drop_in, &drop_in_target, &drop_in_plan),
    ] {
        match step {
            Plan::Absent | Plan::Moved => {}
            Plan::Move => {
                make_dir(&systemd_dir, &copy)?;
                move_entry(&options.fs, source, target).map_err(|e| interrupted(&e))?;
                moves.push((source.clone(), target.clone()));
            }
            Plan::RemoveSource if !exists(source) => {
                out.say(format!(
                    "systemctl disable removed the link {}; it is kept in {}",
                    source.display(),
                    target.display()
                ));
                moves.push((source.clone(), target.clone()));
            }
            Plan::RemoveSource => {
                remove_source(&options.fs, source).map_err(|e| interrupted(&e))?;
                out.say(format!(
                    "{} was already copied to {}; removed the source",
                    source.display(),
                    target.display()
                ));
                moves.push((source.clone(), target.clone()));
            }
        }
    }

    if let Err(e) = systemd.daemon_reload() {
        out.say(format!("warning: systemctl daemon-reload failed: {e}"));
    }
    let mut done = format!("done: {}\n", rfc3339((options.now)()));
    for (from, to) in &moves {
        done.push_str(&format!("moved: {} -> {}\n", from.display(), to.display()));
    }
    write_new(&copy, DONE, &done)?;
    for (from, to) in &moves {
        out.say(format!("moved {} -> {}", from.display(), to.display()));
    }
    out.say(format!(
        "this host is a node again; its control plane is kept in {}, and nothing was deleted. It can be promoted later like any machine with the node role",
        copy.display()
    ));
    Ok(Outcome::Rejoined(copy))
}

#[cfg(test)]
mod tests;
