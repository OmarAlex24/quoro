//! Step 6 of `quoro cluster promote` (`node-agent-protocol.md` section 7.3,
//! ADR 0232): prepares a node's host as the installer prepares a primary,
//! starts the control plane and points this machine's agent at it.
//!
//! When this step runs, the command has written the new database at the
//! control plane's path (steps 4 and 5) and the keys where a primary keeps
//! them (the start of step 6), all as root. [`prepare_host`] then does what
//! `installer/install.sh` does for a primary:
//!
//! 1. creates the control plane's system user `quoro-api`
//!    (`install_api_user`): a system account with its own group, no home
//!    created, home `/var/lib/quoro` and the first nologin shell present. An
//!    existing `quoro-api` with a login uid (1000 or above) refuses, and an
//!    existing system one is kept;
//! 2. hands the data directory to that user (`install_primary_data_dir`):
//!    everything in `/var/lib/quoro` but `engine/` becomes
//!    `quoro-api:quoro-api`, each directory's contents before the directory,
//!    then `/var/lib/quoro` itself, `0700`. The walk opens every entry
//!    without following a link and changes the owner of what it opened, so
//!    it never reaches outside the directory: a symbolic link, or a file
//!    with another hard link, refuses. `/etc/quoro` becomes `root:root`
//!    `0755`;
//! 3. edits `quoro.toml` and never replaces it (`install_primary_config`):
//!    it sets `local = false` in the `[node]` table, appending the table when
//!    there is none, keeps every other line byte for byte, and makes the file
//!    `root:quoro-api` `0640`. A copy of the file as it was,
//!    `quoro.toml.pre-promote`, is written first. An existing copy is kept and
//!    never overwritten;
//! 4. installs the unit embedded from `packaging/systemd/quoro.service`,
//!    `root:root` `0644` (`install_unit`), runs `systemctl daemon-reload` and
//!    `enable`, checks again that the data directory holds the database and
//!    the keys, and then restarts the unit when it was active or its file
//!    changed, and starts it otherwise (`start_unit`). It waits, with a
//!    bound, until the unit has stayed active (`wait_until_active`) and its
//!    listener accepts a connection.
//!
//! Before any change, it refuses:
//!
//! - a configuration other than `/etc/quoro/quoro.toml`, or one whose
//!   `data_dir` is not `/var/lib/quoro`: the unit runs `quoro serve` with
//!   `QUORO_CONFIG=/etc/quoro/quoro.toml` and may write only
//!   `/var/lib/quoro` (`ReadWritePaths`);
//! - `/etc/quoro`, `quoro.toml` or `/var/lib/quoro` that is a symbolic link
//!   or not what it should be, and a `[node]` table it cannot edit;
//! - a data directory that does not hold `quoro.db` and every key file a
//!   primary reads ([`PRIMARY_FILES`]): a control plane started without them
//!   would start empty, or make keys of its own;
//! - a symbolic link, or a file with another hard link, in the data
//!   directory outside `engine/`;
//! - a `quoro-api` with a login uid, or without its group.
//!
//! [`check_host`] runs every one of these refusals but the one on the
//! database and the keys, and changes nothing, so the command runs it among
//! its refusals before its votes, when the promotion has not written them
//! yet.
//!
//! [`point_agent`] is the end of step 6. It is a separate call so that the
//! command can end the freeze between the two, as the contract orders: start
//! the control plane, end the freeze, point the agent. It points this
//! machine's agent at the local control plane as `quoro node set-primary`
//! does, with the pin unchanged: the certificate is the old primary's
//! (ADR 0181). It leaves the agent's unit running.
//!
//! Each step leaves the host in a state a rerun accepts, so a run stopped at
//! any point, after the start included, can be run again. A file is written
//! under a temporary name by root, synced, given its mode, then its owner,
//! and only then renamed into place.
//!
//! The systemd calls go through [`Systemd`], the account calls through
//! [`Accounts`], the owner changes and the renames into place through
//! [`HostFs`], the connections to the listener through [`Probe`], the waits
//! through [`Clock`] and the agent's identity through [`Agent`]; every path
//! is under [`Options::root`]. So the tests run with fakes, in a temporary
//! directory and without root.

use crate::config::Config;
use crate::host::{DATABASE, NODE_UNIT, Out, UNIT, systemctl, systemctl_check};
use rustix::fs::{AtFlags, CWD, Dir, FileType, Gid, Mode, OFlags, Uid};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsFd as _, BorrowedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _, lchown};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The control plane's system user and its group (ADR 0089).
pub const API_USER: &str = "quoro-api";
/// The home `install_api_user` gives that user. It is never created.
pub const API_HOME: &str = crate::config::DEFAULT_DATA_DIR;
/// The configuration the unit's `quoro serve` reads (`QUORO_CONFIG`).
pub const CONFIG_PATH: &str = crate::config::DEFAULT_PATH;
/// The one data directory the unit may write (`ReadWritePaths`).
pub const DATA_DIR: &str = crate::config::DEFAULT_DATA_DIR;
/// The unit directory `install_unit` writes to.
pub const UNIT_DIR: &str = "/etc/systemd/system";
/// The files a primary's data directory holds before its control plane
/// starts: the database (`serve.rs`), the signing key
/// (`quoro_control_plane::keys`), the data-encryption key
/// (`quoro_control_plane::crypto`) and the TLS pair (`tls.rs`). `quoro
/// serve` makes each key that is missing, so a promoted primary started
/// without one would serve with a key no node holds a seal of.
pub const PRIMARY_FILES: [&str; 5] = [
    DATABASE,
    "keys/signing.key",
    "keys/data.key",
    "tls/cert.pem",
    "tls/key.pem",
];
/// The M1 engine state in the data directory, which stays root's, as
/// `install_primary_data_dir` leaves it.
pub const ENGINE_DIR: &str = "engine";
/// The shells `install_api_user` picks from: the first executable one, or
/// the last when none is.
pub const SHELLS: [&str; 3] = ["/usr/sbin/nologin", "/sbin/nologin", "/bin/false"];
/// The suffix of the copy of `quoro.toml` as it was before the promotion.
pub const PRE_PROMOTE: &str = ".pre-promote";
/// The control-plane unit as the release ships it.
pub const UNIT_FILE: &str = include_str!("../../../../packaging/systemd/quoro.service");
/// The lowest uid of a login account. A `quoro-api` at or above it refuses.
const FIRST_LOGIN_UID: u32 = 1000;
/// root's uid and gid.
const ROOT: u32 = 0;
/// The comment `install_primary_config` writes above `local = false`.
const LOCAL_COMMENT: &str = "# The node agent runs as quoro-node.service (ADR 0089).";
/// How often the waits poll.
const POLL: Duration = Duration::from_secs(1);
/// How many polls `wait_until_active` makes at most.
const ACTIVE_POLLS: u32 = 20;
/// How many polls in a row must find the unit active.
const ACTIVE_IN_A_ROW: u32 = 5;
/// How many connections to the listener are tried at most, one per poll.
const LISTENER_POLLS: u32 = 30;
/// What every refusal ends with.
const UNCHANGED: &str = "nothing was changed";

/// The systemd calls of this step.
pub trait Systemd {
    /// `systemctl daemon-reload`.
    ///
    /// # Errors
    ///
    /// A message when the reload failed.
    fn daemon_reload(&self) -> Result<(), String>;
    /// `systemctl enable <unit>`.
    ///
    /// # Errors
    ///
    /// A message when the enable failed.
    fn enable(&self, unit: &str) -> Result<(), String>;
    /// `systemctl start <unit>`.
    ///
    /// # Errors
    ///
    /// A message when the start failed.
    fn start(&self, unit: &str) -> Result<(), String>;
    /// `systemctl restart <unit>`.
    ///
    /// # Errors
    ///
    /// A message when the restart failed.
    fn restart(&self, unit: &str) -> Result<(), String>;
    /// `systemctl stop <unit>`.
    ///
    /// # Errors
    ///
    /// A message when the stop failed.
    fn stop(&self, unit: &str) -> Result<(), String>;
    /// Whether `systemctl is-active --quiet <unit>` succeeds.
    fn is_active(&self, unit: &str) -> bool;
}

/// The host's `systemctl`.
pub struct RealSystemd;

impl Systemd for RealSystemd {
    fn daemon_reload(&self) -> Result<(), String> {
        systemctl_check(&["daemon-reload"])
    }

    fn enable(&self, unit: &str) -> Result<(), String> {
        systemctl_check(&["enable", unit])
    }

    fn start(&self, unit: &str) -> Result<(), String> {
        systemctl_check(&["start", unit])
    }

    fn restart(&self, unit: &str) -> Result<(), String> {
        systemctl_check(&["restart", unit])
    }

    fn stop(&self, unit: &str) -> Result<(), String> {
        systemctl_check(&["stop", unit])
    }

    fn is_active(&self, unit: &str) -> bool {
        systemctl(&["is-active", "--quiet", unit]).is_ok_and(|output| output.status.success())
    }
}

/// The account calls of this step.
pub trait Accounts {
    /// The uid of the user `name`, or `None` when there is no such user.
    ///
    /// # Errors
    ///
    /// A message when the account database could not be read.
    fn uid(&self, name: &str) -> Result<Option<u32>, String>;
    /// The gid of the group `name`, or `None` when there is no such group.
    ///
    /// # Errors
    ///
    /// A message when the group database could not be read.
    fn gid(&self, name: &str) -> Result<Option<u32>, String>;
    /// `useradd --system --user-group --no-create-home --home-dir <home>
    /// --shell <shell> <name>`.
    ///
    /// # Errors
    ///
    /// A message when `useradd` failed.
    fn create_system_user(&self, name: &str, home: &Path, shell: &Path) -> Result<(), String>;
}

/// The host's account database, through `getent` and `useradd`.
pub struct RealAccounts;

/// Runs `getent <database> <name>` and reads its answer ([`read_getent`]).
fn getent(database: &str, name: &str) -> Result<Option<u32>, String> {
    let output = std::process::Command::new("getent")
        .args([database, name])
        .output()
        .map_err(|e| format!("cannot run getent {database} {name}: {e}"))?;
    read_getent(
        database,
        name,
        output.status.code(),
        &String::from_utf8_lossy(&output.stdout),
        &String::from_utf8_lossy(&output.stderr),
    )
}

/// The id in the third field of the first line `getent <database> <name>`
/// printed, or `None` when `getent` found no such entry (exit status 2).
///
/// # Errors
///
/// A message for any other exit status, and for a first line without a
/// numeric third field.
fn read_getent(
    database: &str,
    name: &str,
    code: Option<i32>,
    stdout: &str,
    stderr: &str,
) -> Result<Option<u32>, String> {
    match code {
        Some(0) => stdout
            .lines()
            .next()
            .and_then(|line| line.split(':').nth(2))
            .and_then(|id| id.trim().parse().ok())
            .map(Some)
            .ok_or_else(|| format!("getent {database} {name} printed no id: {stdout}")),
        Some(2) => Ok(None),
        code => Err(format!(
            "getent {database} {name}: exit status {}: {}",
            code.map_or_else(
                || "none (killed by a signal)".to_string(),
                |c| c.to_string()
            ),
            stderr.trim()
        )),
    }
}

impl Accounts for RealAccounts {
    fn uid(&self, name: &str) -> Result<Option<u32>, String> {
        getent("passwd", name)
    }

    fn gid(&self, name: &str) -> Result<Option<u32>, String> {
        getent("group", name)
    }

    fn create_system_user(&self, name: &str, home: &Path, shell: &Path) -> Result<(), String> {
        let output = std::process::Command::new("useradd")
            .args(["--system", "--user-group", "--no-create-home", "--home-dir"])
            .arg(home)
            .arg("--shell")
            .arg(shell)
            .arg(name)
            .output()
            .map_err(|e| format!("cannot run useradd: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "useradd {name}: {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }
}

/// The file-system calls a test replaces: those that need root, and the
/// renames into place, so a test can stop a run between two writes.
pub trait HostFs {
    /// Changes the owner and group of `path` itself, never of a link's
    /// target.
    ///
    /// # Errors
    ///
    /// The error of the change.
    fn chown(&self, path: &Path, uid: u32, gid: u32) -> io::Result<()>;
    /// Changes the owner and group of the file `fd` was opened on, with
    /// `O_PATH` and without following a link; `path` names it.
    ///
    /// # Errors
    ///
    /// The error of the change.
    fn chown_opened(&self, fd: BorrowedFd<'_>, path: &Path, uid: u32, gid: u32) -> io::Result<()>;
    /// Renames `from` to `to`, replacing `to`.
    ///
    /// # Errors
    ///
    /// The error of the rename.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Gives the file `from` the name `to`, failing with `AlreadyExists` when
    /// `to` exists, then removes the name `from`.
    ///
    /// # Errors
    ///
    /// The error of the link or of the removal.
    fn link_new(&self, from: &Path, to: &Path) -> io::Result<()>;
}

/// The real calls.
pub struct RealFs;

impl HostFs for RealFs {
    fn chown(&self, path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        lchown(path, Some(uid), Some(gid))
    }

    fn chown_opened(&self, fd: BorrowedFd<'_>, _path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        rustix::fs::chownat(
            fd,
            c"",
            Some(Uid::from_raw(uid)),
            Some(Gid::from_raw(gid)),
            AtFlags::EMPTY_PATH,
        )
        .map_err(io::Error::from)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn link_new(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::hard_link(from, to)?;
        fs::remove_file(from)
    }
}

/// The connections to the control plane's listener.
pub trait Probe {
    /// Opens a TCP connection to `address`, waiting at most `timeout`, and
    /// closes it.
    ///
    /// # Errors
    ///
    /// Why the connection failed.
    fn connect(&self, address: SocketAddr, timeout: Duration) -> Result<(), String>;
}

/// The host's network.
pub struct RealProbe;

impl Probe for RealProbe {
    fn connect(&self, address: SocketAddr, timeout: Duration) -> Result<(), String> {
        TcpStream::connect_timeout(&address, timeout)
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

/// The waits.
pub trait Clock {
    /// Waits for `duration`.
    fn sleep(&self, duration: Duration);
}

/// The real clock.
pub struct RealClock;

impl Clock for RealClock {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// The seams of [`prepare_host`].
pub struct Seams<'a> {
    /// `systemctl`.
    pub systemd: &'a dyn Systemd,
    /// The account database.
    pub accounts: &'a dyn Accounts,
    /// The owner changes and the renames into place.
    pub fs: &'a dyn HostFs,
    /// The connections to the listener.
    pub probe: &'a dyn Probe,
    /// The waits.
    pub clock: &'a dyn Clock,
}

impl Seams<'static> {
    /// The host's.
    pub const REAL: Self = Self {
        systemd: &RealSystemd,
        accounts: &RealAccounts,
        fs: &RealFs,
        probe: &RealProbe,
        clock: &RealClock,
    };
}

/// What [`prepare_host`] works on.
pub struct Options {
    /// The configuration file the command read, as it named it. Anything
    /// but [`CONFIG_PATH`] refuses.
    pub config: PathBuf,
    /// Where `/` is: `/` on the host, a temporary directory in the tests.
    /// [`CONFIG_PATH`], [`DATA_DIR`] and [`UNIT_DIR`] are read and written
    /// under it.
    pub root: PathBuf,
    /// The shells the user may get, in order of preference: the first
    /// executable one, or the last when none is ([`SHELLS`]).
    pub shells: Vec<PathBuf>,
}

impl Options {
    /// The host's places, with `config` as the configuration the command
    /// read.
    #[must_use]
    pub fn host(config: PathBuf) -> Self {
        Self {
            config,
            root: PathBuf::from("/"),
            shells: SHELLS.iter().map(PathBuf::from).collect(),
        }
    }

    /// `path`, an absolute path of the host, under [`Self::root`].
    fn on_host(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_start_matches('/'))
    }
}

/// What [`set_local_false`] changed in `quoro.toml`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalEdit {
    /// The `[node]` table already set `local = false`; the text is unchanged.
    AlreadySet,
    /// The `[node]` table set `local = true`; only that value changed.
    Replaced,
    /// The `[node]` table had no `local`; `local = false` was added to it,
    /// after its last key.
    Added,
    /// There was no `[node]` table; it was appended, as
    /// `install_primary_config` appends it.
    Appended,
}

/// What happened to `quoro.toml.pre-promote`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrePromote {
    /// It held nothing, and now holds `quoro.toml` as this run found it.
    Written,
    /// It existed, from an earlier run or promotion, and was kept as it was.
    Kept,
}

/// How the unit was brought up, as `start_unit` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Brought {
    /// The unit was inactive and its file unchanged: `systemctl start`.
    Started,
    /// The unit was active, or its file changed: `systemctl restart`, so the
    /// running control plane reads the handed-over directory, the edited
    /// `quoro.toml` and the new unit.
    Restarted,
}

/// What [`prepare_host`] did.
#[derive(Debug, PartialEq, Eq)]
pub struct Prepared {
    /// Whether this run created `quoro-api`; `false` when it existed.
    pub user_created: bool,
    /// `quoro-api`'s uid.
    pub uid: u32,
    /// The gid of the group `quoro-api`.
    pub gid: u32,
    /// The control plane's data directory, under [`Options::root`].
    pub data_dir: PathBuf,
    /// How many entries of the data directory were handed to `quoro-api`,
    /// the directory itself and `engine/` aside.
    pub handed: usize,
    /// The change to `quoro.toml`.
    pub local: LocalEdit,
    /// The copy of `quoro.toml` as it was.
    pub pre_promote: PrePromote,
    /// Whether the unit file changed.
    pub unit_changed: bool,
    /// How the unit was brought up.
    pub brought: Brought,
    /// The address the control plane listens on.
    pub listen: SocketAddr,
    /// The URL this machine's agent dials: the listener on loopback, as a
    /// primary's own agent dials it. [`point_agent`] takes it.
    pub url: String,
}

/// A file's or a directory's owner and mode.
#[derive(Clone, Copy)]
struct Owner {
    uid: u32,
    gid: u32,
    mode: u32,
}

/// What [`check`] read, for the changes of [`prepare_host`].
struct Checked {
    /// `quoro.toml`, under [`Options::root`].
    config_path: PathBuf,
    /// Its directory, `/etc/quoro`.
    config_dir: PathBuf,
    /// Its text and metadata as read.
    original: String,
    original_metadata: fs::Metadata,
    config: Config,
    /// Its text with `local = false` in its `[node]` table.
    edited: String,
    local: LocalEdit,
    /// `/var/lib/quoro`, under [`Options::root`].
    data_dir: PathBuf,
    /// `quoro-api`'s uid, when the user exists.
    existing: Option<u32>,
}

/// Whether [`check`] checks the files the promotion writes into the data
/// directory before step 6 ([`PRIMARY_FILES`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Files {
    Checked,
    Unchecked,
}

/// The refusals of [`prepare_host`] that need nothing the promotion writes
/// before step 6, so that `quoro cluster promote` runs them before its votes
/// (section 7.3): the configuration is [`CONFIG_PATH`]; `/etc/quoro` is a
/// directory, not a link; `quoro.toml` is a regular file that reads, sets
/// `data_dir` to [`DATA_DIR`] or leaves it out, and has a `[node]` table
/// [`set_local_false`] can edit; `/var/lib/quoro` is a directory, not a
/// link, with no link and no file with another hard link in it but in
/// `engine/`; and `quoro-api`, when it exists, has no login uid and has its
/// group. The database and the keys are not checked: steps 4 to 6 write them
/// later.
///
/// It takes only the account database, which it reads, and changes nothing.
///
/// # Errors
///
/// The refusal [`prepare_host`] stops with on the same host, which says that
/// nothing was changed.
pub fn check_host(options: &Options, accounts: &dyn Accounts) -> Result<(), String> {
    check(options, accounts, Files::Unchecked).map(drop)
}

/// The refusals of [`prepare_host`], in its order: those of [`check_host`],
/// and with [`Files::Checked`] the files the promotion wrote, checked after
/// the data directory itself.
fn check(options: &Options, accounts: &dyn Accounts, files: Files) -> Result<Checked, String> {
    if options.config != Path::new(CONFIG_PATH) {
        return Err(format!(
            "the configuration is {}, but {UNIT} runs quoro serve with QUORO_CONFIG={CONFIG_PATH}: the control plane would read another file. Move the configuration to {CONFIG_PATH} and run this again; {UNCHANGED}",
            options.config.display()
        ));
    }
    let config_path = options.on_host(CONFIG_PATH);
    let config_dir = parent_of(&config_path)?.to_path_buf();
    check_directory(&config_dir, "the configuration directory")?;
    let (original, original_metadata) = read_config(&config_path)?;
    let config: Config = toml::from_str(&original)
        .map_err(|e| format!("cannot read {}: {e}; {UNCHANGED}", config_path.display()))?;
    if config.data_dir != Path::new(DATA_DIR) {
        return Err(format!(
            "{CONFIG_PATH} sets data_dir = {:?}, but {UNIT} may write only {DATA_DIR}, where the promotion writes the database: set data_dir to {DATA_DIR} or remove the line, and run this again; {UNCHANGED}",
            config.data_dir.display().to_string()
        ));
    }
    let (edited, local) = set_local_false(&original).map_err(|e| {
        format!(
            "cannot set local = false in the [node] table of {}: {e}; {UNCHANGED}",
            config_path.display()
        )
    })?;
    let data_dir = options.on_host(DATA_DIR);
    check_directory(&data_dir, "the data directory")?;
    if files == Files::Checked {
        check_primary_files(&data_dir).map_err(|e| format!("{e}; {UNCHANGED}"))?;
    }
    hand_over(&data_dir, None).map_err(|e| format!("{e}; {UNCHANGED}"))?;
    let existing = accounts.uid(API_USER)?;
    if let Some(uid) = existing {
        if uid >= FIRST_LOGIN_UID {
            return Err(format!(
                "user {API_USER} exists with uid {uid}, which is a login account; refusing to hand {DATA_DIR} to a human account. Remove it and run this again: userdel {API_USER}. Nothing was changed"
            ));
        }
        if accounts.gid(API_USER)?.is_none() {
            return Err(format!(
                "user {API_USER} exists without a group {API_USER}, which owns the data directory; {UNCHANGED}"
            ));
        }
    }
    Ok(Checked {
        config_path,
        config_dir,
        original,
        original_metadata,
        config,
        edited,
        local,
        data_dir,
        existing,
    })
}

/// Steps 1 to 4: prepares this host to run the control plane and starts it.
///
/// It blocks: it runs `getent`, `useradd` and `systemctl`, and sleeps
/// between its polls for up to about a minute. An async caller runs it on a
/// blocking thread (`tokio::task::spawn_blocking`), with [`Seams::REAL`].
///
/// # Errors
///
/// A refusal, which comes before any change and says that nothing was
/// changed; a data directory that lost a file a primary needs after the
/// changes began, which says that the unit was not started; or the failure
/// of a step. The lines printed before it are in `out`. A run that failed
/// can be run again.
pub fn prepare_host(
    options: &Options,
    seams: &Seams<'_>,
    out: &mut Out,
) -> Result<Prepared, String> {
    // Every check first: nothing changes before they all passed.
    let Checked {
        config_path,
        config_dir,
        original,
        original_metadata,
        config,
        edited,
        local,
        data_dir,
        existing,
    } = check(options, seams.accounts, Files::Checked)?;

    // Step 1: the user.
    let user_created = existing.is_none();
    if user_created {
        let shell = pick_shell(&options.shells)
            .ok_or_else(|| format!("no shell to give the system user; {UNCHANGED}"))?;
        seams
            .accounts
            .create_system_user(API_USER, Path::new(API_HOME), shell)
            .map_err(|e| format!("could not create the system user {API_USER}: {e}"))?;
        out.say(format!(
            "created the system user {API_USER} (no home, {})",
            shell.display()
        ));
    }
    let uid = seams
        .accounts
        .uid(API_USER)?
        .ok_or_else(|| format!("useradd ran, but there is no user {API_USER}"))?;
    let gid = seams
        .accounts
        .gid(API_USER)?
        .ok_or_else(|| format!("there is no group {API_USER}"))?;
    if !user_created {
        out.say(format!("user {API_USER} exists (uid {uid}); left as is"));
    }

    // Step 2: the data directory's contents, then the directory, then the
    // configuration directory.
    let handed = hand_over(&data_dir, Some((seams.fs, uid, gid)))?;
    let data_owner = Owner {
        uid,
        gid,
        mode: 0o700,
    };
    own_in_place(seams.fs, &data_dir, data_owner)?;
    out.say(format!(
        "handed {handed} entries in {} to {API_USER}:{API_USER}, {ENGINE_DIR}/ left as it is; {} is {API_USER}:{API_USER} 0700",
        data_dir.display(),
        data_dir.display()
    ));
    let config_owner = Owner {
        uid: ROOT,
        gid: ROOT,
        mode: 0o755,
    };
    own_in_place(seams.fs, &config_dir, config_owner)?;
    out.say(format!("kept {} (root:root 0755)", config_dir.display()));

    // Step 3: quoro.toml, after its copy.
    let copy = sibling(&config_path, PRE_PROMOTE)?;
    let pre_promote = if exists(&copy) {
        // A run stopped between the copy's link and the removal of its
        // partial name leaves that name; the copy holds the same file.
        remove_if_present(&partial_of(&copy)?)?;
        out.say(format!(
            "kept the existing {}: an earlier run or promotion wrote it, and it is never overwritten, so {} was not copied again",
            copy.display(),
            config_path.display()
        ));
        PrePromote::Kept
    } else {
        let owner = Owner {
            uid: original_metadata.uid(),
            gid: original_metadata.gid(),
            mode: original_metadata.mode() & 0o7777,
        };
        stage(seams.fs, &copy, original.as_bytes(), owner, Place::New)?;
        out.say(format!(
            "copied {} to {}",
            config_path.display(),
            copy.display()
        ));
        PrePromote::Written
    };
    let toml_owner = Owner {
        uid: ROOT,
        gid,
        mode: 0o640,
    };
    if local == LocalEdit::AlreadySet {
        own_in_place(seams.fs, &config_path, toml_owner)?;
    } else {
        stage(
            seams.fs,
            &config_path,
            edited.as_bytes(),
            toml_owner,
            Place::Replace,
        )?;
    }
    let what = match local {
        LocalEdit::AlreadySet => "already sets local = false in its [node] table",
        LocalEdit::Replaced => "now sets local = false in its [node] table",
        LocalEdit::Added => "has local = false added to its [node] table",
        LocalEdit::Appended => "has the [node] table (local = false) appended",
    };
    out.say(format!(
        "{} {what} (root:{API_USER} 0640)",
        config_path.display()
    ));

    // Step 4: the unit, its start and the waits.
    let was_active = seams.systemd.is_active(UNIT);
    let unit_changed = install_unit(seams.fs, &options.on_host(UNIT_DIR), out)?;
    seams
        .systemd
        .daemon_reload()
        .map_err(|e| format!("systemctl daemon-reload failed: {e}"))?;
    seams
        .systemd
        .enable(UNIT)
        .map_err(|e| format!("systemctl enable {UNIT} failed: {e}"))?;
    out.say(format!("enabled {UNIT}"));
    check_primary_files(&data_dir).map_err(|e| format!("{e}; {UNIT} was not started"))?;
    let brought = if was_active || unit_changed {
        seams.systemd.restart(UNIT).map_err(|e| {
            format!(
                "cannot restart {UNIT}: {e}; check the journal: journalctl -u {UNIT} -n 50 --no-pager"
            )
        })?;
        out.say(format!("restarted {UNIT}"));
        Brought::Restarted
    } else {
        seams.systemd.start(UNIT).map_err(|e| {
            format!(
                "cannot start {UNIT}: {e}; check the journal: journalctl -u {UNIT} -n 50 --no-pager"
            )
        })?;
        out.say(format!("started {UNIT}"));
        Brought::Started
    };
    wait_until_active(seams.systemd, seams.clock)?;
    out.say(format!("{UNIT} is active"));
    let address = crate::serve::loopback(config.listen);
    wait_for_listener(seams.probe, seams.clock, address)?;
    out.say(format!(
        "the control plane accepts connections on {address}"
    ));

    Ok(Prepared {
        user_created,
        uid,
        gid,
        data_dir,
        handed,
        local,
        pre_promote,
        unit_changed,
        brought,
        listen: config.listen,
        url: format!("https://{address}"),
    })
}

/// Refuses a directory this step must not take: a missing one, a symbolic
/// link, or a name that is not a directory.
fn check_directory(dir: &Path, what: &str) -> Result<(), String> {
    match fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(metadata) if metadata.is_symlink() => Err(format!(
            "{what} {} is a symbolic link; this step hands it over and never follows a link. Make it a directory and run this again; {UNCHANGED}",
            dir.display()
        )),
        Ok(_) => Err(format!(
            "{what} {} exists and is not a directory; {UNCHANGED}",
            dir.display()
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(format!(
            "{what} {} does not exist; the promotion writes its database and keys there before this step; {UNCHANGED}",
            dir.display()
        )),
        Err(e) => Err(format!("cannot read {}: {e}; {UNCHANGED}", dir.display())),
    }
}

/// `quoro.toml` as text, with its metadata. It must be a regular file: a
/// host installed with the node role has one, and it is edited, never
/// created.
fn read_config(path: &Path) -> Result<(String, fs::Metadata), String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(format!(
                "{} does not exist; a host installed with the node role has one, and this step edits it. Nothing was changed",
                path.display()
            ));
        }
        Err(e) => {
            return Err(format!("cannot read {}: {e}; {UNCHANGED}", path.display()));
        }
    };
    if !metadata.is_file() {
        return Err(format!(
            "{} is not a regular file; {UNCHANGED}",
            path.display()
        ));
    }
    let text = fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}; {UNCHANGED}", path.display()))?;
    Ok((text, metadata))
}

/// Checks that `data_dir` holds every file of [`PRIMARY_FILES`], each a
/// regular file reached through directories, none through a link.
///
/// # Errors
///
/// A message that names each file that is missing or not a regular file.
fn check_primary_files(data_dir: &Path) -> Result<(), String> {
    let mut missing = Vec::new();
    for name in PRIMARY_FILES {
        let mut path = data_dir.to_path_buf();
        let parts: Vec<&str> = name.split('/').collect();
        let mut found = true;
        for (index, part) in parts.iter().enumerate() {
            path.push(part);
            let last = index + 1 == parts.len();
            let fits = fs::symlink_metadata(&path).is_ok_and(|metadata| {
                if last {
                    metadata.is_file()
                } else {
                    metadata.is_dir()
                }
            });
            if !fits {
                found = false;
                break;
            }
        }
        if !found {
            missing.push(name);
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} does not hold {} as a regular file: the promotion writes the database (steps 4 and 5) and the keys (step 6) there before the control plane starts, and a control plane started without them would start empty or make keys of its own",
            data_dir.display(),
            missing.join(", ")
        ))
    }
}

/// Walks everything in `data_dir` but its `engine/`, and with `owner` hands
/// each entry to that uid and gid, a directory's contents before the
/// directory. Returns how many entries it passed.
///
/// Each entry is opened relative to its directory with `O_PATH` and
/// `O_NOFOLLOW`, so the walk acts on what it opened and never follows a
/// link, even one that replaced an entry after it was listed; a directory is
/// listed through the descriptor it was opened with.
///
/// # Errors
///
/// A symbolic link, which the walk would otherwise have to follow or skip;
/// a file that is not a directory with more than one hard link, whose other
/// names may be outside the directory; or a failed read or owner change.
fn hand_over(data_dir: &Path, owner: Option<(&dyn HostFs, u32, u32)>) -> Result<usize, String> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let top = rustix::fs::openat(CWD, data_dir, flags, Mode::empty())
        .map_err(|e| format!("cannot open {}: {e}", data_dir.display()))?;
    let mut passed = 0;
    hand_over_in(top.as_fd(), data_dir, true, owner, &mut passed)?;
    Ok(passed)
}

/// [`hand_over`] inside the open directory `dir`, named `path`.
fn hand_over_in(
    dir: BorrowedFd<'_>,
    path: &Path,
    top: bool,
    owner: Option<(&dyn HostFs, u32, u32)>,
    passed: &mut usize,
) -> Result<(), String> {
    let cannot = |what: &str, at: &Path, e: rustix::io::Errno| {
        format!("cannot {what} {}: {e}", at.display())
    };
    let listing = Dir::read_from(dir).map_err(|e| cannot("list", path, e))?;
    for entry in listing {
        let entry = entry.map_err(|e| cannot("list", path, e))?;
        let name = entry.file_name();
        let bytes = name.to_bytes();
        if bytes == b"." || bytes == b".." || (top && bytes == ENGINE_DIR.as_bytes()) {
            continue;
        }
        let child = path.join(OsStr::from_bytes(bytes));
        let opened = rustix::fs::openat(
            dir,
            name,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| cannot("open", &child, e))?;
        let stat = rustix::fs::fstat(&opened).map_err(|e| cannot("read", &child, e))?;
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Symlink => {
                return Err(format!(
                    "{} is a symbolic link; the data directory is handed to {API_USER} without following a link, so move it away and run this again",
                    child.display()
                ));
            }
            FileType::Directory => {
                let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
                let inner = rustix::fs::openat(&opened, c".", flags, Mode::empty())
                    .map_err(|e| cannot("open", &child, e))?;
                hand_over_in(inner.as_fd(), &child, false, owner, passed)?;
            }
            _ if stat.st_nlink > 1 => {
                return Err(format!(
                    "{} has {} hard links, and a name of it outside the data directory would be handed to {API_USER} with it; move it away and run this again",
                    child.display(),
                    stat.st_nlink
                ));
            }
            _ => {}
        }
        if let Some((fs_calls, uid, gid)) = owner {
            fs_calls
                .chown_opened(opened.as_fd(), &child, uid, gid)
                .map_err(|e| {
                    format!(
                        "cannot give {} to uid {uid} gid {gid}: {e}",
                        child.display()
                    )
                })?;
        }
        *passed += 1;
    }
    Ok(())
}

/// The first executable shell of `shells`, or the last one when none is, as
/// `install_api_user` picks it.
fn pick_shell(shells: &[PathBuf]) -> Option<&Path> {
    shells
        .iter()
        .find(|shell| {
            fs::metadata(shell)
                .is_ok_and(|metadata| metadata.is_file() && metadata.mode() & 0o111 != 0)
        })
        .or(shells.last())
        .map(PathBuf::as_path)
}

/// Gives the file or directory `path`, which is not a link, `owner`'s mode,
/// then its owner.
fn own_in_place(fs_calls: &dyn HostFs, path: &Path, owner: Owner) -> Result<(), String> {
    fs::set_permissions(path, fs::Permissions::from_mode(owner.mode)).map_err(|e| {
        format!(
            "cannot set the mode of {} to {:04o}: {e}",
            path.display(),
            owner.mode
        )
    })?;
    fs_calls.chown(path, owner.uid, owner.gid).map_err(|e| {
        format!(
            "cannot give {} to uid {} gid {}: {e}",
            path.display(),
            owner.uid,
            owner.gid
        )
    })
}

/// How [`stage`] puts a file in place.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    /// Over an existing file.
    Replace,
    /// Only where nothing exists.
    New,
}

/// The partial name [`stage`] writes `target` under:
/// `.<name>.promote-partial` next to it.
fn partial_of(target: &Path) -> Result<PathBuf, String> {
    let dir = parent_of(target)?;
    let name = target
        .file_name()
        .ok_or_else(|| format!("{} has no file name", target.display()))?;
    Ok(dir.join(format!(".{}.promote-partial", name.to_string_lossy())))
}

/// Removes `path` when it exists.
fn remove_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("cannot remove {}: {e}", path.display())),
    }
}

/// Writes `bytes` to `target` through its partial name ([`partial_of`]):
/// created by this process `0600`, written and synced, given `owner`'s mode,
/// then its owner, then put at `target` and the directory synced. A partial
/// file left by a stopped run is removed first.
fn stage(
    fs_calls: &dyn HostFs,
    target: &Path,
    bytes: &[u8],
    owner: Owner,
    place: Place,
) -> Result<(), String> {
    let dir = parent_of(target)?;
    let partial = partial_of(target)?;
    remove_if_present(&partial)?;
    write_partial(&partial, bytes, owner.mode)
        .map_err(|e| format!("cannot write {}: {e}", partial.display()))?;
    fs_calls
        .chown(&partial, owner.uid, owner.gid)
        .map_err(|e| {
            format!(
                "cannot give {} to uid {} gid {}: {e}",
                partial.display(),
                owner.uid,
                owner.gid
            )
        })?;
    let placed = match place {
        Place::Replace => fs_calls.rename(&partial, target),
        Place::New => fs_calls.link_new(&partial, target),
    };
    placed.map_err(|e| {
        format!(
            "cannot put {} at {}: {e}",
            partial.display(),
            target.display()
        )
    })?;
    sync_dir(dir)
}

/// Creates `path` `0600`, writes `bytes`, syncs it, gives it `mode` and
/// syncs it again.
fn write_partial(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    file.set_permissions(fs::Permissions::from_mode(mode))?;
    file.sync_all()
}

/// Installs the embedded unit as `<unit_dir>/quoro.service`, `root:root`
/// `0644`. A file that already holds the same text keeps it and gets the
/// mode and owner; anything else is replaced, as `install_unit` replaces it.
/// Returns whether the file changed.
fn install_unit(fs_calls: &dyn HostFs, unit_dir: &Path, out: &mut Out) -> Result<bool, String> {
    let path = unit_dir.join(UNIT);
    let owner = Owner {
        uid: ROOT,
        gid: ROOT,
        mode: 0o644,
    };
    let same = fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file())
        && fs::read(&path).is_ok_and(|bytes| bytes == UNIT_FILE.as_bytes());
    if same {
        own_in_place(fs_calls, &path, owner)?;
        out.say(format!(
            "kept {} (unchanged, root:root 0644)",
            path.display()
        ));
    } else {
        stage(fs_calls, &path, UNIT_FILE.as_bytes(), owner, Place::Replace)?;
        out.say(format!("installed {} (root:root 0644)", path.display()));
    }
    Ok(!same)
}

/// Waits until the unit has been active for `ACTIVE_IN_A_ROW` polls in a
/// row, for at most `ACTIVE_POLLS` polls, as the installer's
/// `wait_until_active` does: `Type=simple` counts as active as soon as the
/// process runs, and `quoro serve` checks the host after that, so a refusal
/// (exit 78) shows here and not a moment later.
fn wait_until_active(systemd: &dyn Systemd, clock: &dyn Clock) -> Result<(), String> {
    let mut in_a_row = 0;
    for _ in 0..ACTIVE_POLLS {
        if systemd.is_active(UNIT) {
            in_a_row += 1;
            if in_a_row >= ACTIVE_IN_A_ROW {
                return Ok(());
            }
        } else {
            in_a_row = 0;
        }
        clock.sleep(POLL);
    }
    Err(format!(
        "{UNIT} did not stay active; check the journal: journalctl -u {UNIT} -n 50 --no-pager"
    ))
}

/// Waits until a TCP connection to `address` succeeds, for at most
/// `LISTENER_POLLS` tries.
fn wait_for_listener(
    probe: &dyn Probe,
    clock: &dyn Clock,
    address: SocketAddr,
) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 1..=LISTENER_POLLS {
        match probe.connect(address, POLL) {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
        if attempt < LISTENER_POLLS {
            clock.sleep(POLL);
        }
    }
    Err(format!(
        "{UNIT} is active, but nothing accepted a connection on {address} ({last}); check the journal: journalctl -u {UNIT} -n 50 --no-pager"
    ))
}

/// The agent calls of [`point_agent`].
pub trait Agent {
    /// Proves that the host at `url` holds the pinned certificate, with a TLS
    /// handshake and the pin the agent stored at enrollment, and returns the
    /// agent's current `primary_url` and `url` normalized. Writes nothing.
    ///
    /// # Errors
    ///
    /// A message that says nothing was changed.
    fn verify(&self, url: &str) -> impl Future<Output = Result<(String, String), String>>;
    /// Rewrites only `primary_url` in the agent's identity.
    ///
    /// # Errors
    ///
    /// A message when the identity could not be read or written.
    fn write(&self, url: &str) -> Result<(), String>;
}

/// This machine's agent, through `quoro node set-primary`'s calls.
pub struct NodeAgent {
    /// The agent's data directory (`[node] data_dir`).
    pub data_dir: PathBuf,
}

impl Agent for NodeAgent {
    async fn verify(&self, url: &str) -> Result<(String, String), String> {
        let (identity, normalized) =
            quoro_node_agent::set_primary::verify_primary(&self.data_dir, url)
                .await
                .map_err(|e| e.to_string())?;
        Ok((identity.primary_url, normalized))
    }

    fn write(&self, url: &str) -> Result<(), String> {
        quoro_node_agent::set_primary::write_primary(&self.data_dir, url)
            .map(drop)
            .map_err(|e| e.to_string())
    }
}

/// What [`point_agent`] did. Either way, `quoro-node.service` runs when it
/// returns.
#[derive(Debug, PartialEq, Eq)]
pub enum Pointed {
    /// The agent already dialed that URL; its identity is unchanged.
    /// `started` says whether its unit was not active, and was started.
    Already {
        /// Whether the agent's unit was started.
        started: bool,
    },
    /// The agent's `primary_url` changed from `previous`. `was_running`
    /// says whether `quoro-node.service` was active, and so was stopped
    /// around the write and started again; otherwise it was started after
    /// the write.
    Changed {
        /// The URL the agent dialed before.
        previous: String,
        /// Whether the agent's unit was active before the write.
        was_running: bool,
    },
}

/// The end of step 6: points this machine's agent at `url`, the local
/// control plane ([`Prepared::url`]), as `quoro node set-primary` does. The
/// pin is checked first; then a running agent is stopped, only `primary_url`
/// is rewritten, and the agent is started, also after a failed write and
/// also when it was not running before. An agent that already dials `url`
/// keeps its identity and is started when it is not active, so a rerun after
/// a failed start finishes. The pin, the cluster key and the credential
/// never change.
///
/// # Errors
///
/// A refused handshake, which changed nothing, or a failed stop, write or
/// start.
pub async fn point_agent(
    url: &str,
    agent: &impl Agent,
    systemd: &dyn Systemd,
    out: &mut Out,
) -> Result<Pointed, String> {
    let (previous, url) = agent.verify(url).await?;
    let was_running = systemd.is_active(NODE_UNIT);
    if previous == url {
        out.say(format!(
            "this machine's agent already dials {url}; its identity was not changed"
        ));
        if was_running {
            out.say(format!("{NODE_UNIT} is active"));
        } else {
            systemd
                .start(NODE_UNIT)
                .map_err(|e| format!("{NODE_UNIT} was not active, and cannot start it: {e}"))?;
            out.say(format!("{NODE_UNIT} was not active; started it"));
        }
        return Ok(Pointed::Already {
            started: !was_running,
        });
    }
    if was_running {
        systemd
            .stop(NODE_UNIT)
            .map_err(|e| format!("cannot stop {NODE_UNIT}: {e}; nothing was changed"))?;
    }
    let written = agent.write(&url);
    if written.is_ok() {
        out.say(format!("primary address {previous} -> {url}"));
    }
    let started = systemd.start(NODE_UNIT);
    match (written, started) {
        (Ok(()), Ok(())) => {
            out.say(if was_running {
                format!("restarted {NODE_UNIT}")
            } else {
                format!("{NODE_UNIT} was not active; started it")
            });
            Ok(Pointed::Changed {
                previous,
                was_running,
            })
        }
        (Ok(()), Err(e)) => Err(format!(
            "pointed the agent at {url}, but cannot start {NODE_UNIT}: {e}"
        )),
        (Err(e), Ok(())) => Err(format!("cannot point the agent at {url}: {e}")),
        (Err(e), Err(start)) => Err(format!(
            "cannot point the agent at {url}: {e}; and cannot start {NODE_UNIT}: {start}"
        )),
    }
}

/// `text` with `local = false` in its `[node]` table, and what changed. Every
/// other line is kept byte for byte:
///
/// - a `local = <value>` line in the table gets `false` in place of its
///   value, keeping its spacing and comment;
/// - a table without `local` gets the installer's comment and
///   `local = false` after its last key, or after its header when it has
///   none;
/// - a text without the table gets it appended, as `install_primary_config`
///   appends it.
///
/// The result is read back: it must hold exactly what `text` holds, with
/// `node.local` false. A `[node]` table written as dotted keys or as an
/// inline table does not read back, and is refused.
///
/// # Errors
///
/// A message when `text` does not read as TOML or the edit does not read
/// back as only that change.
pub fn set_local_false(text: &str) -> Result<(String, LocalEdit), String> {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let newline = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let mut in_node = false;
    let mut header = None;
    let mut last_key = None;
    let mut local = None;
    for (index, line) in lines.iter().enumerate() {
        if let Some(name) = table_name(line) {
            in_node = name == "node";
            if in_node && header.is_none() {
                header = Some(index);
            }
            continue;
        }
        let body = line.trim();
        if !in_node || body.is_empty() || body.starts_with('#') {
            continue;
        }
        last_key = Some(index);
        if local.is_none() {
            local = local_value(line).map(|span| (index, span));
        }
    }

    let (edited, edit) = match (header, local) {
        (None, _) => {
            let mut edited = text.to_string();
            if !edited.is_empty() && !edited.ends_with('\n') {
                edited.push_str(newline);
            }
            for line in ["", "[node]", LOCAL_COMMENT, "local = false"] {
                edited.push_str(line);
                edited.push_str(newline);
            }
            (edited, LocalEdit::Appended)
        }
        (Some(_), Some((index, (start, end)))) => {
            if lines[index].get(start..end) == Some("false") {
                (text.to_string(), LocalEdit::AlreadySet)
            } else {
                let mut edited = String::with_capacity(text.len());
                for (at, line) in lines.iter().enumerate() {
                    if at == index {
                        edited.push_str(&line[..start]);
                        edited.push_str("false");
                        edited.push_str(&line[end..]);
                    } else {
                        edited.push_str(line);
                    }
                }
                (edited, LocalEdit::Replaced)
            }
        }
        (Some(header), None) => {
            let after = last_key.unwrap_or(header);
            let mut edited = String::with_capacity(text.len() + 80);
            for (at, line) in lines.iter().enumerate() {
                edited.push_str(line);
                if at == after {
                    if !line.ends_with('\n') {
                        edited.push_str(newline);
                    }
                    for added in [LOCAL_COMMENT, "local = false"] {
                        edited.push_str(added);
                        edited.push_str(newline);
                    }
                }
            }
            (edited, LocalEdit::Added)
        }
    };
    check_edit(text, &edited)?;
    Ok((edited, edit))
}

/// The name of the table a `[name]` header line opens, or `None` when `line`
/// is not a header. An array of tables, `[[name]]`, is returned with its
/// brackets, so it is never `node`.
fn table_name(line: &str) -> Option<&str> {
    let body = line.trim();
    let inner = body.strip_prefix('[')?;
    if inner.starts_with('[') {
        return Some(body);
    }
    let end = inner.find(']')?;
    let rest = inner[end + 1..].trim_start();
    if rest.is_empty() || rest.starts_with('#') {
        Some(inner[..end].trim())
    } else {
        None
    }
}

/// The byte span of the value in a `local = <value>` line.
fn local_value(line: &str) -> Option<(usize, usize)> {
    let key = line.trim_start().strip_prefix("local")?;
    let value = key.trim_start().strip_prefix('=')?.trim_start();
    let start = line.len() - value.len();
    let length = value
        .find(|c: char| c.is_whitespace() || c == '#')
        .unwrap_or(value.len());
    Some((start, start + length))
}

/// Checks that `after` holds exactly what `before` holds, with `node.local`
/// false.
fn check_edit(before: &str, after: &str) -> Result<(), String> {
    const BY_HAND: &str = "set local = false in the [node] table by hand and run this again";
    let mut expected: toml::Table =
        toml::from_str(before).map_err(|e| format!("it does not read as TOML ({e})"))?;
    let node = expected
        .entry("node")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    let Some(node) = node.as_table_mut() else {
        return Err(format!("its node is not a table; {BY_HAND}"));
    };
    node.insert("local".to_string(), toml::Value::Boolean(false));
    let edited: toml::Table = toml::from_str(after).map_err(|e| {
        format!(
            "the edited file would not read as TOML ({e}), as when [node] is written as dotted keys or an inline table; {BY_HAND}"
        )
    })?;
    if edited != expected {
        return Err(format!(
            "the edit would change more than local in the [node] table; {BY_HAND}"
        ));
    }
    Ok(())
}

/// `path` with `suffix` appended to its file name.
fn sibling(path: &Path, suffix: &str) -> Result<PathBuf, String> {
    let name = path
        .file_name()
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    let mut name = name.to_os_string();
    name.push(suffix);
    Ok(path.with_file_name(name))
}

fn parent_of(path: &Path) -> Result<&Path, String> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| format!("{} has no parent directory", path.display()))
}

/// Whether `path` exists, as itself (a dangling link exists).
fn exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

/// Syncs a directory, so a new entry or a rename in it is durable.
fn sync_dir(dir: &Path) -> Result<(), String> {
    fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|e| format!("cannot sync {}: {e}", dir.display()))
}

#[cfg(test)]
mod tests;
