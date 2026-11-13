//! Step 6 of `quoro cluster promote` (`node-agent-protocol.md` section 7.3,
//! ADR 0232): prepares a node's host as the installer prepares a primary,
//! starts the control plane and points this machine's agent at it.
//!
//! [`prepare_host`] does what `installer/install.sh` does for a primary:
//!
//! 1. creates the control plane's system user `quoro-api`
//!    (`install_api_user`): a system account with its own group, no home
//!    created, home `/var/lib/quoro` and the first nologin shell present. An
//!    existing `quoro-api` with a login uid (1000 or above) refuses, and an
//!    existing system one is kept;
//! 2. creates the data directory, `quoro-api:quoro-api` `0700`
//!    (`install_primary_data_dir`), and `/etc/quoro`, `root:root` `0755`. A
//!    data directory that already holds a `quoro.db` refuses. The command
//!    refuses such a host before it changes anything (section 7.3), so this
//!    is only a guard;
//! 3. edits `quoro.toml` and never replaces it (`install_primary_config`):
//!    it sets `local = false` in the `[node]` table, appending the table when
//!    there is none, keeps every other line byte for byte, and makes the file
//!    `root:quoro-api` `0640`. A copy of the file as it was,
//!    `quoro.toml.pre-promote`, is written first. An existing copy is kept and
//!    never overwritten;
//! 4. installs the unit embedded from `packaging/systemd/quoro.service`,
//!    `root:root` `0644` (`install_unit`), runs `systemctl daemon-reload`,
//!    `enable` and `start`, and waits, with a bound, until the unit has stayed
//!    active (`wait_until_active`) and its listener accepts a connection.
//!
//! [`point_agent`] is the end of step 6. It is a separate call so that the
//! command can end the freeze between the two, as the contract orders: start
//! the control plane, end the freeze, point the agent. It points this
//! machine's agent at the local control plane as `quoro node set-primary`
//! does, with the pin unchanged: the certificate is the old primary's
//! (ADR 0181).
//!
//! Every check that can refuse runs before anything changes. Each step
//! leaves the host in a state a rerun accepts, so a run stopped at any point
//! can be run again. A file is written under a temporary name by root,
//! synced, given its mode, then its owner, and only then renamed into place.
//! A directory is created by root and handed to its owner last.
//!
//! The systemd calls go through [`Systemd`], the account calls through
//! [`Accounts`], the owner changes and the renames into place through
//! [`HostFs`], the waits through [`Clock`] and the agent's identity through
//! [`Agent`], so the tests run with fakes and without root.

use crate::config::Config;
use crate::host::{DATABASE, NODE_UNIT, Out, UNIT, systemctl, systemctl_check};
use std::fs;
use std::io::{self, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::os::unix::fs::{PermissionsExt as _, lchown};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The control plane's system user and its group (ADR 0089).
pub const API_USER: &str = "quoro-api";
/// The home `install_api_user` gives that user. It is never created.
pub const API_HOME: &str = crate::config::DEFAULT_DATA_DIR;
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

/// The id in the third field of `getent <database> <name>`, or `None` when
/// `getent` finds no such entry (exit status 2).
fn getent(database: &str, name: &str) -> Result<Option<u32>, String> {
    let output = std::process::Command::new("getent")
        .args([database, name])
        .output()
        .map_err(|e| format!("cannot run getent {database} {name}: {e}"))?;
    match output.status.code() {
        Some(0) => {
            let text = String::from_utf8_lossy(&output.stdout);
            text.lines()
                .next()
                .and_then(|line| line.split(':').nth(2))
                .and_then(|id| id.trim().parse().ok())
                .map(Some)
                .ok_or_else(|| format!("getent {database} {name} printed no id: {text}"))
        }
        Some(2) => Ok(None),
        _ => Err(format!(
            "getent {database} {name}: {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
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

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn link_new(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::hard_link(from, to)?;
        fs::remove_file(from)
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
    /// The waits.
    pub clock: &'a dyn Clock,
}

impl Seams<'static> {
    /// The host's.
    pub const REAL: Self = Self {
        systemd: &RealSystemd,
        accounts: &RealAccounts,
        fs: &RealFs,
        clock: &RealClock,
    };
}

/// Where [`prepare_host`] works; [`Options::host`] fills in the host's
/// places.
pub struct Options {
    /// `quoro.toml`. Its directory is the configuration directory, and its
    /// `data_dir` and `listen` are the control plane's.
    pub config: PathBuf,
    /// `/etc/systemd/system`.
    pub unit_dir: PathBuf,
    /// The shells the user may get, in order of preference: the first
    /// executable one, or the last when none is ([`SHELLS`]).
    pub shells: Vec<PathBuf>,
}

impl Options {
    /// The host's places, with `config` as the configuration.
    #[must_use]
    pub fn host(config: PathBuf) -> Self {
        Self {
            config,
            unit_dir: PathBuf::from("/etc/systemd/system"),
            shells: SHELLS.iter().map(PathBuf::from).collect(),
        }
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

/// What [`prepare_host`] did.
#[derive(Debug, PartialEq, Eq)]
pub struct Prepared {
    /// Whether this run created `quoro-api`; `false` when it existed.
    pub user_created: bool,
    /// `quoro-api`'s uid.
    pub uid: u32,
    /// The gid of the group `quoro-api`.
    pub gid: u32,
    /// The control plane's data directory.
    pub data_dir: PathBuf,
    /// The change to `quoro.toml`.
    pub local: LocalEdit,
    /// The copy of `quoro.toml` as it was.
    pub pre_promote: PrePromote,
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

/// Steps 1 to 4: prepares this host to run the control plane and starts it.
///
/// # Errors
///
/// A refusal, which comes before any change and says that nothing was
/// changed, or the failure of a step; the lines printed before it are in
/// `out`. A run that failed can be run again.
pub fn prepare_host(
    options: &Options,
    seams: &Seams<'_>,
    out: &mut Out,
) -> Result<Prepared, String> {
    // Every check first: nothing changes before they all passed.
    let config_path = &options.config;
    let config_dir = parent_of(config_path)?;
    let (original, original_metadata) = read_config(config_path)?;
    let config: Config = toml::from_str(&original).map_err(|e| {
        format!(
            "cannot read {}: {e}; nothing was changed",
            config_path.display()
        )
    })?;
    let (edited, local) = set_local_false(&original).map_err(|e| {
        format!(
            "cannot set local = false in the [node] table of {}: {e}; nothing was changed",
            config_path.display()
        )
    })?;
    let data_dir = config.data_dir.clone();
    check_data_dir(&data_dir)?;
    let existing = seams.accounts.uid(API_USER)?;
    if let Some(uid) = existing {
        if uid >= FIRST_LOGIN_UID {
            return Err(format!(
                "user {API_USER} exists with uid {uid}, which is a login account; refusing to hand {} to a human account. Remove it and run this again: userdel {API_USER}. Nothing was changed",
                data_dir.display()
            ));
        }
        if seams.accounts.gid(API_USER)?.is_none() {
            return Err(format!(
                "user {API_USER} exists without a group {API_USER}, which owns the data directory; nothing was changed"
            ));
        }
    }

    // Step 1: the user.
    let user_created = existing.is_none();
    if user_created {
        let shell = pick_shell(&options.shells)
            .ok_or_else(|| "no shell to give the system user; nothing was changed".to_string())?;
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

    // Step 2: the data and configuration directories.
    let data_owner = Owner {
        uid,
        gid,
        mode: 0o700,
    };
    let made = dir_in_place(seams.fs, &data_dir, data_owner)?;
    out.say(format!(
        "{} {} ({API_USER}:{API_USER} 0700)",
        if made { "created" } else { "kept" },
        data_dir.display()
    ));
    let config_owner = Owner {
        uid: ROOT,
        gid: ROOT,
        mode: 0o755,
    };
    let made = dir_in_place(seams.fs, config_dir, config_owner)?;
    out.say(format!(
        "{} {} (root:root 0755)",
        if made { "created" } else { "kept" },
        config_dir.display()
    ));

    // Step 3: quoro.toml, after its copy.
    let copy = sibling(config_path, PRE_PROMOTE)?;
    let pre_promote = if exists(&copy) {
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
        own_in_place(seams.fs, config_path, toml_owner)?;
    } else {
        stage(
            seams.fs,
            config_path,
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
    install_unit(seams.fs, &options.unit_dir, out)?;
    seams
        .systemd
        .daemon_reload()
        .map_err(|e| format!("systemctl daemon-reload failed: {e}"))?;
    seams
        .systemd
        .enable(UNIT)
        .map_err(|e| format!("systemctl enable {UNIT} failed: {e}"))?;
    out.say(format!("enabled {UNIT}"));
    seams.systemd.start(UNIT).map_err(|e| {
        format!(
            "cannot start {UNIT}: {e}; check the journal: journalctl -u {UNIT} -n 50 --no-pager"
        )
    })?;
    wait_until_active(seams.systemd, seams.clock)?;
    out.say(format!("{UNIT} is active"));
    let address = crate::serve::loopback(config.listen);
    wait_for_listener(seams.clock, address)?;
    out.say(format!(
        "the control plane accepts connections on {address}"
    ));

    Ok(Prepared {
        user_created,
        uid,
        gid,
        data_dir,
        local,
        pre_promote,
        listen: config.listen,
        url: format!("https://{address}"),
    })
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
            return Err(format!(
                "cannot read {}: {e}; nothing was changed",
                path.display()
            ));
        }
    };
    if !metadata.is_file() {
        return Err(format!(
            "{} is not a regular file; nothing was changed",
            path.display()
        ));
    }
    let text = fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}; nothing was changed", path.display()))?;
    Ok((text, metadata))
}

/// Refuses a data directory this step must not take: a relative path, a name
/// that is not a directory (a link included), or one that already holds a
/// control-plane database.
fn check_data_dir(dir: &Path) -> Result<(), String> {
    if !dir.is_absolute() {
        return Err(format!(
            "the data directory {} is not an absolute path; nothing was changed",
            dir.display()
        ));
    }
    match fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(format!(
                "{} exists and is not a directory; nothing was changed",
                dir.display()
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(format!(
                "cannot read {}: {e}; nothing was changed",
                dir.display()
            ));
        }
    }
    let database = dir.join(DATABASE);
    if exists(&database) {
        return Err(format!(
            "{} already holds a control-plane database, {}; this host is not prepared over one (node-agent-protocol.md section 7.3). Nothing was changed",
            dir.display(),
            database.display()
        ));
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

/// Creates `dir` with `owner` when it is missing (by root, `0700`, then its
/// mode, then its owner), or gives an existing one `owner`'s mode and owner;
/// what it holds is left as it is. Returns whether it created the directory.
fn dir_in_place(fs_calls: &dyn HostFs, dir: &Path, owner: Owner) -> Result<bool, String> {
    let made = match fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.is_dir() => false,
        Ok(_) => {
            return Err(format!("{} is not a directory", dir.display()));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(dir)
                .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
            true
        }
        Err(e) => return Err(format!("cannot read {}: {e}", dir.display())),
    };
    own_in_place(fs_calls, dir, owner)?;
    if made {
        sync_dir(parent_of(dir)?)?;
    }
    Ok(made)
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

/// Writes `bytes` to `target` through `.<name>.promote-partial` next to it:
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
    let name = target
        .file_name()
        .ok_or_else(|| format!("{} has no file name", target.display()))?;
    let partial = dir.join(format!(".{}.promote-partial", name.to_string_lossy()));
    match fs::remove_file(&partial) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("cannot remove {}: {e}", partial.display())),
    }
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
fn install_unit(fs_calls: &dyn HostFs, unit_dir: &Path, out: &mut Out) -> Result<(), String> {
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
    Ok(())
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
fn wait_for_listener(clock: &dyn Clock, address: SocketAddr) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 1..=LISTENER_POLLS {
        match TcpStream::connect_timeout(&address, POLL) {
            Ok(_) => return Ok(()),
            Err(e) => last = e.to_string(),
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

/// What [`point_agent`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Pointed {
    /// The agent already dialed that URL; nothing changed.
    Already,
    /// The agent's `primary_url` changed from `previous`. `restarted` says
    /// whether `quoro-node.service` was running, and so was stopped around the
    /// write and started again.
    Changed {
        /// The URL the agent dialed before.
        previous: String,
        /// Whether the agent's unit was stopped and started again.
        restarted: bool,
    },
}

/// The end of step 6: points this machine's agent at `url`, the local
/// control plane ([`Prepared::url`]), as `quoro node set-primary` does. The
/// pin is checked first; then a running agent is stopped, only `primary_url`
/// is rewritten, and the agent is started again, also after a failed write.
/// The pin, the cluster key and the credential never change.
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
    if previous == url {
        out.say(format!(
            "this machine's agent already dials {url}; nothing was changed"
        ));
        return Ok(Pointed::Already);
    }
    let restarted = systemd.is_active(NODE_UNIT);
    if restarted {
        systemd
            .stop(NODE_UNIT)
            .map_err(|e| format!("cannot stop {NODE_UNIT}: {e}; nothing was changed"))?;
    }
    let written = agent.write(&url);
    if written.is_ok() {
        out.say(format!("primary address {previous} -> {url}"));
    }
    let started = if restarted {
        systemd.start(NODE_UNIT)
    } else {
        Ok(())
    };
    match (written, started) {
        (Ok(()), Ok(())) => {
            if restarted {
                out.say(format!("restarted {NODE_UNIT}"));
            } else {
                out.say(format!(
                    "{NODE_UNIT} was not running; start it to use the new address"
                ));
            }
            Ok(Pointed::Changed {
                previous,
                restarted,
            })
        }
        (Ok(()), Err(e)) => Err(format!(
            "pointed the agent at {url}, but cannot start {NODE_UNIT}: {e}"
        )),
        (Err(e), Ok(())) => Err(format!("cannot point the agent at {url}: {e}")),
        (Err(e), Err(start)) => Err(format!(
            "cannot point the agent at {url}: {e}; and cannot start {NODE_UNIT} again: {start}"
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
