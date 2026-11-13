//! The host preparation of `quoro cluster promote` on a host laid out in a
//! temporary directory, with a fake systemd, account database, owner change
//! and clock, so nothing needs root.

use super::*;
use quoro_node_agent::identity::{self, Identity};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::net::TcpListener;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::sync::atomic::{AtomicU32, Ordering};

/// The uid and gid the fake `useradd` gives `quoro-api`.
const SYSTEM_UID: u32 = 997;
const SYSTEM_GID: u32 = 996;

/// What `install_primary_config` writes above `local = false`.
const ADDED: &str = "# The node agent runs as quoro-node.service (ADR 0089).\nlocal = false\n";

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A directory of its own under the system's temporary directory, removed
/// on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "quoro-promote-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_mode(path: &Path, bytes: &[u8], mode: u32) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn mode_of(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

/// A node host's `quoro.toml` as `install_node_config` writes it, with the
/// test's listener and data directory.
fn node_config(listen: SocketAddr, data_dir: &Path) -> String {
    format!(
        "# Quoro configuration (ADR 0089 privilege split).\n\
         # This host is a node: it runs microVMs for its primary and runs no\n\
         # control plane. Edit this file and restart with:\n\
         #   systemctl restart quoro-node\n\
         \n\
         listen = \"{listen}\"\n\
         data_dir = \"{}\"\n\
         \n\
         engine = \"cloud-hypervisor\"\n\
         \n\
         [node]\n\
         # The identity, local store and engine state of the agent on this host.\n\
         # data_dir = \"/var/lib/quoro-node\"\n",
        data_dir.display()
    )
}

/// A node host: `etc/quoro/quoro.toml` (`0640`), `var/lib` without the
/// control plane's directory, `units/` for `/etc/systemd/system`, and
/// `shells/` with an executable `nologin` and `false`.
struct Host {
    scratch: Scratch,
    listen: SocketAddr,
    _listener: Option<TcpListener>,
}

impl Host {
    /// A host whose control plane's address accepts connections.
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap();
        Self::with(listen, Some(listener))
    }

    /// A host whose control plane's address accepts nothing: port 1 is not
    /// an ephemeral port, so no connection to it can connect to itself.
    fn without_listener() -> Self {
        Self::with("127.0.0.1:1".parse().unwrap(), None)
    }

    fn with(listen: SocketAddr, listener: Option<TcpListener>) -> Self {
        let scratch = Scratch::new();
        for dir in ["etc/quoro", "var/lib", "units", "shells"] {
            fs::create_dir_all(scratch.0.join(dir)).unwrap();
        }
        write_mode(&scratch.0.join("shells/nologin"), b"", 0o755);
        write_mode(&scratch.0.join("shells/false"), b"", 0o755);
        let host = Self {
            scratch,
            listen,
            _listener: listener,
        };
        write_mode(
            &host.config(),
            node_config(listen, &host.data_dir()).as_bytes(),
            0o640,
        );
        host
    }

    fn root(&self) -> &Path {
        &self.scratch.0
    }

    fn config(&self) -> PathBuf {
        self.root().join("etc/quoro/quoro.toml")
    }

    fn copy(&self) -> PathBuf {
        self.root().join("etc/quoro/quoro.toml.pre-promote")
    }

    fn data_dir(&self) -> PathBuf {
        self.root().join("var/lib/quoro")
    }

    fn unit(&self) -> PathBuf {
        self.root().join("units/quoro.service")
    }

    fn options(&self) -> Options {
        Options {
            config: self.config(),
            unit_dir: self.root().join("units"),
            shells: vec![
                self.root().join("shells/missing"),
                self.root().join("shells/nologin"),
                self.root().join("shells/false"),
            ],
        }
    }

    /// The names under `etc/quoro`, `var/lib` and `units`, with each file's
    /// bytes, so a test can tell that nothing changed.
    fn snapshot(&self) -> Vec<(PathBuf, Option<Vec<u8>>)> {
        let mut entries = Vec::new();
        for dir in ["etc/quoro", "var/lib", "units"] {
            let mut stack = vec![self.root().join(dir)];
            while let Some(path) = stack.pop() {
                let metadata = fs::symlink_metadata(&path).unwrap();
                if metadata.is_dir() {
                    for entry in fs::read_dir(&path).unwrap() {
                        stack.push(entry.unwrap().path());
                    }
                    entries.push((path, None));
                } else {
                    let bytes = fs::read(&path).unwrap();
                    entries.push((path, Some(bytes)));
                }
            }
        }
        entries.sort();
        entries
    }
}

/// A fake `systemctl` that records every call, the agent's identity writes
/// included, in one log.
#[derive(Default)]
struct FakeSystemd {
    log: RefCell<Vec<String>>,
    /// The answers of `is-active quoro.service`, in order. Once they are
    /// used up, the unit reads active, or inactive with `stays_down`.
    answers: RefCell<VecDeque<bool>>,
    stays_down: Cell<bool>,
    /// What `is-active quoro-node.service` answers.
    node_active: Cell<bool>,
}

impl FakeSystemd {
    fn record(&self, call: String) -> Result<(), String> {
        self.log.borrow_mut().push(call);
        Ok(())
    }

    /// The log, each run of the same entry counted once.
    fn sequence(&self) -> Vec<String> {
        let mut log = self.log.borrow().clone();
        log.dedup();
        log
    }

    fn count(&self, entry: &str) -> usize {
        self.log
            .borrow()
            .iter()
            .filter(|logged| logged.as_str() == entry)
            .count()
    }
}

impl Systemd for FakeSystemd {
    fn daemon_reload(&self) -> Result<(), String> {
        self.record("daemon-reload".to_string())
    }

    fn enable(&self, unit: &str) -> Result<(), String> {
        self.record(format!("enable {unit}"))
    }

    fn start(&self, unit: &str) -> Result<(), String> {
        self.record(format!("start {unit}"))
    }

    fn stop(&self, unit: &str) -> Result<(), String> {
        self.record(format!("stop {unit}"))
    }

    fn is_active(&self, unit: &str) -> bool {
        self.log.borrow_mut().push(format!("is-active {unit}"));
        if unit == UNIT {
            let next = self.answers.borrow_mut().pop_front();
            next.unwrap_or(!self.stays_down.get())
        } else {
            self.node_active.get()
        }
    }
}

/// A fake account database that starts without `quoro-api` unless told
/// otherwise.
#[derive(Default)]
struct FakeAccounts {
    user: Cell<Option<u32>>,
    group: Cell<Option<u32>>,
    created: RefCell<Vec<(String, PathBuf, PathBuf)>>,
}

impl Accounts for FakeAccounts {
    fn uid(&self, name: &str) -> Result<Option<u32>, String> {
        assert_eq!(name, API_USER);
        Ok(self.user.get())
    }

    fn gid(&self, name: &str) -> Result<Option<u32>, String> {
        assert_eq!(name, API_USER);
        Ok(self.group.get())
    }

    fn create_system_user(&self, name: &str, home: &Path, shell: &Path) -> Result<(), String> {
        self.created
            .borrow_mut()
            .push((name.to_string(), home.to_path_buf(), shell.to_path_buf()));
        self.user.set(Some(SYSTEM_UID));
        self.group.set(Some(SYSTEM_GID));
        Ok(())
    }
}

/// What the fake file system saw.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    /// An owner change, with the mode the path had then and, for a file, its
    /// bytes.
    Chown {
        path: PathBuf,
        uid: u32,
        gid: u32,
        mode: u32,
        bytes: Option<Vec<u8>>,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    Link {
        from: PathBuf,
        to: PathBuf,
    },
}

/// Records the owner changes instead of making them, and keeps each path's
/// owner across renames; renames and links are real.
#[derive(Default)]
struct FakeFs {
    events: RefCell<Vec<Event>>,
    owners: RefCell<BTreeMap<PathBuf, (u32, u32)>>,
    /// A rename onto this path fails, as if the run stopped there.
    stop_at: RefCell<Option<PathBuf>>,
}

impl FakeFs {
    fn owner(&self, path: &Path) -> Option<(u32, u32)> {
        self.owners.borrow().get(path).copied()
    }

    fn moved(&self, from: &Path, to: &Path) {
        let mut owners = self.owners.borrow_mut();
        if let Some(owner) = owners.remove(from) {
            owners.insert(to.to_path_buf(), owner);
        }
    }
}

impl HostFs for FakeFs {
    fn chown(&self, path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        let bytes = metadata.is_file().then(|| fs::read(path)).transpose()?;
        self.events.borrow_mut().push(Event::Chown {
            path: path.to_path_buf(),
            uid,
            gid,
            mode: metadata.mode() & 0o7777,
            bytes,
        });
        self.owners
            .borrow_mut()
            .insert(path.to_path_buf(), (uid, gid));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if self.stop_at.borrow().as_deref() == Some(to) {
            return Err(io::Error::other("the run stopped here"));
        }
        fs::rename(from, to)?;
        self.events.borrow_mut().push(Event::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        });
        self.moved(from, to);
        Ok(())
    }

    fn link_new(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.link_new(from, to)?;
        self.events.borrow_mut().push(Event::Link {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        });
        self.moved(from, to);
        Ok(())
    }
}

/// A clock that only adds up the waits.
#[derive(Default)]
struct FakeClock {
    slept: Cell<Duration>,
}

impl Clock for FakeClock {
    fn sleep(&self, duration: Duration) {
        self.slept.set(self.slept.get() + duration);
    }
}

#[derive(Default)]
struct Fakes {
    systemd: FakeSystemd,
    accounts: FakeAccounts,
    fs: FakeFs,
    clock: FakeClock,
}

impl Fakes {
    fn seams(&self) -> Seams<'_> {
        Seams {
            systemd: &self.systemd,
            accounts: &self.accounts,
            fs: &self.fs,
            clock: &self.clock,
        }
    }

    fn prepare(&self, host: &Host) -> (Result<Prepared, String>, Vec<String>) {
        let mut out = Out::quiet();
        let result = prepare_host(&host.options(), &self.seams(), &mut out);
        (result, out.lines)
    }

    /// Asserts that nothing reached a seam.
    fn assert_untouched(&self) {
        assert!(self.systemd.log.borrow().is_empty(), "no systemd call");
        assert!(self.accounts.created.borrow().is_empty(), "no user created");
        assert!(
            self.fs.events.borrow().is_empty(),
            "no owner change or rename"
        );
    }
}

#[test]
fn a_node_host_is_prepared_as_the_installer_prepares_a_primary() {
    let host = Host::new();
    let fakes = Fakes::default();
    let original = fs::read_to_string(host.config()).unwrap();
    let metadata = fs::metadata(host.config()).unwrap();

    let (result, lines) = fakes.prepare(&host);
    let prepared = result.unwrap();
    assert_eq!(
        prepared,
        Prepared {
            user_created: true,
            uid: SYSTEM_UID,
            gid: SYSTEM_GID,
            data_dir: host.data_dir(),
            local: LocalEdit::Added,
            pre_promote: PrePromote::Written,
            listen: host.listen,
            url: format!("https://{}", host.listen),
        }
    );

    // Step 1: a system user with its own group, home /var/lib/quoro, and the
    // first executable shell.
    assert_eq!(
        *fakes.accounts.created.borrow(),
        [(
            API_USER.to_string(),
            PathBuf::from("/var/lib/quoro"),
            host.root().join("shells/nologin"),
        )]
    );

    // Step 2: the data directory, empty, and /etc/quoro.
    assert_eq!(mode_of(&host.data_dir()), 0o700);
    assert_eq!(
        fakes.fs.owner(&host.data_dir()),
        Some((SYSTEM_UID, SYSTEM_GID))
    );
    assert_eq!(fs::read_dir(host.data_dir()).unwrap().count(), 0);
    let etc = host.root().join("etc/quoro");
    assert_eq!(mode_of(&etc), 0o755);
    assert_eq!(fakes.fs.owner(&etc), Some((0, 0)));

    // Step 3: the copy as the file was, with its mode and owner, then the
    // file with only `local = false` added, root:quoro-api 0640.
    assert_eq!(fs::read_to_string(host.copy()).unwrap(), original);
    assert_eq!(mode_of(&host.copy()), 0o640);
    assert_eq!(
        fakes.fs.owner(&host.copy()),
        Some((metadata.uid(), metadata.gid()))
    );
    let edited = fs::read_to_string(host.config()).unwrap();
    assert_eq!(
        edited,
        original.replacen("[node]\n", &format!("[node]\n{ADDED}"), 1)
    );
    let config: Config = toml::from_str(&edited).unwrap();
    assert!(
        !config.node.local,
        "the control plane does not run the agent"
    );
    assert_eq!(mode_of(&host.config()), 0o640);
    assert_eq!(fakes.fs.owner(&host.config()), Some((0, SYSTEM_GID)));

    // Step 4: the packaged unit, root:root 0644, reloaded, enabled, started
    // and waited for.
    assert_eq!(fs::read_to_string(host.unit()).unwrap(), UNIT_FILE);
    assert_eq!(mode_of(&host.unit()), 0o644);
    assert_eq!(fakes.fs.owner(&host.unit()), Some((0, 0)));
    assert_eq!(
        fakes.systemd.sequence(),
        [
            "daemon-reload",
            "enable quoro.service",
            "start quoro.service",
            "is-active quoro.service",
        ]
    );
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 5);
    assert_eq!(
        lines.last().map(String::as_str),
        Some(format!("the control plane accepts connections on {}", host.listen).as_str())
    );

    // No partial file is left.
    for dir in [etc, host.root().join("units")] {
        for entry in fs::read_dir(dir).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().ends_with(".promote-partial"),
                "{name:?} is left"
            );
        }
    }
}

#[test]
fn each_file_is_written_whole_then_given_its_mode_and_owner_then_put_in_place() {
    let host = Host::new();
    let fakes = Fakes::default();
    fakes.prepare(&host).0.unwrap();

    let events = fakes.fs.events.borrow().clone();
    let mut placed = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let (Event::Rename { from, to } | Event::Link { from, to }) = event else {
            continue;
        };
        let Some(Event::Chown {
            path, mode, bytes, ..
        }) = index.checked_sub(1).map(|before| &events[before])
        else {
            panic!("{to:?} was put in place right after something other than its owner change");
        };
        assert_eq!(
            path, from,
            "the file put in place is the one given its owner"
        );
        assert!(
            from.to_string_lossy().ends_with(".promote-partial"),
            "{from:?} is a partial name"
        );
        assert_eq!(
            bytes.as_deref(),
            Some(fs::read(to).unwrap().as_slice()),
            "{to:?} was whole when it was given its owner"
        );
        assert_eq!(*mode, mode_of(to), "{to:?} had its mode before its owner");
        placed.push(to.clone());
    }
    assert_eq!(placed, [host.copy(), host.config(), host.unit()]);

    // The copy keeps its name: it is linked, never renamed over a name.
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Link { to, .. } if *to == host.copy()
    )));

    // The data directory got its mode before its owner.
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Chown { path, uid: SYSTEM_UID, gid: SYSTEM_GID, mode: 0o700, bytes: None }
            if *path == host.data_dir()
    )));
}

#[test]
fn a_quoro_api_with_a_login_uid_refuses_and_changes_nothing() {
    let host = Host::new();
    let fakes = Fakes::default();
    fakes.accounts.user.set(Some(1000));
    fakes.accounts.group.set(Some(1000));
    let before = host.snapshot();

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("uid 1000, which is a login account"),
        "{error}"
    );
    assert!(error.contains("userdel quoro-api"), "{error}");
    assert!(error.ends_with("Nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();
}

#[test]
fn an_existing_system_quoro_api_is_kept() {
    let host = Host::new();
    let fakes = Fakes::default();
    fakes.accounts.user.set(Some(999));
    fakes.accounts.group.set(Some(990));

    let (result, lines) = fakes.prepare(&host);
    let prepared = result.unwrap();
    assert!(!prepared.user_created);
    assert_eq!((prepared.uid, prepared.gid), (999, 990));
    assert!(fakes.accounts.created.borrow().is_empty());
    assert!(lines.contains(&"user quoro-api exists (uid 999); left as is".to_string()));
    assert_eq!(fakes.fs.owner(&host.data_dir()), Some((999, 990)));
    assert_eq!(fakes.fs.owner(&host.config()), Some((0, 990)));
}

#[test]
fn a_data_directory_that_holds_a_database_refuses_and_changes_nothing() {
    let host = Host::new();
    fs::create_dir(host.data_dir()).unwrap();
    fs::write(host.data_dir().join("quoro.db"), b"SQLite format 3\0").unwrap();
    let fakes = Fakes::default();
    let before = host.snapshot();

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("already holds a control-plane database"),
        "{error}"
    );
    assert!(error.ends_with("Nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();
}

#[test]
fn a_rerun_finds_every_step_done_and_keeps_it() {
    let host = Host::new();
    let fakes = Fakes::default();
    let original = fs::read(host.config()).unwrap();
    fakes.prepare(&host).0.unwrap();
    let after_first = host.snapshot();

    let (result, lines) = fakes.prepare(&host);
    let prepared = result.unwrap();
    assert!(!prepared.user_created);
    assert_eq!(prepared.local, LocalEdit::AlreadySet);
    assert_eq!(prepared.pre_promote, PrePromote::Kept);
    assert_eq!(host.snapshot(), after_first, "no file changed");
    assert_eq!(fs::read(host.copy()).unwrap(), original);
    assert_eq!(fakes.accounts.created.borrow().len(), 1);
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("kept ") && line.ends_with("(unchanged, root:root 0644)")),
        "{lines:?}"
    );
    // Owners and modes hold after the second run.
    assert_eq!(fakes.fs.owner(&host.config()), Some((0, SYSTEM_GID)));
    assert_eq!(mode_of(&host.config()), 0o640);
    assert_eq!(fakes.fs.owner(&host.unit()), Some((0, 0)));
    assert_eq!(
        fakes.fs.owner(&host.data_dir()),
        Some((SYSTEM_UID, SYSTEM_GID))
    );
    // Both runs reload, enable and start; starting a running unit does
    // nothing.
    assert_eq!(fakes.systemd.count("start quoro.service"), 2);
}

#[test]
fn a_run_stopped_between_the_copy_and_the_edit_is_finished_by_the_next() {
    let host = Host::new();
    let fakes = Fakes::default();
    let original = fs::read(host.config()).unwrap();
    *fakes.fs.stop_at.borrow_mut() = Some(host.config());

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(error.contains("cannot put"), "{error}");
    assert_eq!(fs::read(host.config()).unwrap(), original, "untouched");
    assert_eq!(fs::read(host.copy()).unwrap(), original);
    assert!(!host.unit().exists());
    assert!(fakes.systemd.log.borrow().is_empty());

    *fakes.fs.stop_at.borrow_mut() = None;
    let prepared = fakes.prepare(&host).0.unwrap();
    assert_eq!(prepared.pre_promote, PrePromote::Kept);
    assert_eq!(prepared.local, LocalEdit::Added);
    assert_eq!(fs::read(host.copy()).unwrap(), original);
    let edited = fs::read_to_string(host.config()).unwrap();
    assert!(edited.contains(ADDED), "{edited}");
    assert_eq!(fakes.fs.owner(&host.config()), Some((0, SYSTEM_GID)));
    assert_eq!(fs::read_to_string(host.unit()).unwrap(), UNIT_FILE);
    assert!(
        !host
            .root()
            .join("etc/quoro/.quoro.toml.promote-partial")
            .exists(),
        "the partial file of the stopped run is gone"
    );
}

#[test]
fn an_existing_pre_promote_copy_is_never_overwritten() {
    let host = Host::new();
    write_mode(&host.copy(), b"# from an earlier promotion\n", 0o600);
    let fakes = Fakes::default();

    let (result, lines) = fakes.prepare(&host);
    assert_eq!(result.unwrap().pre_promote, PrePromote::Kept);
    assert_eq!(
        fs::read(host.copy()).unwrap(),
        b"# from an earlier promotion\n"
    );
    assert_eq!(mode_of(&host.copy()), 0o600);
    assert_eq!(fakes.fs.owner(&host.copy()), None, "not touched");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("it is never overwritten")),
        "{lines:?}"
    );
}

#[test]
fn the_unit_must_read_active_five_polls_in_a_row() {
    let host = Host::new();
    let fakes = Fakes::default();
    fakes
        .systemd
        .answers
        .borrow_mut()
        .extend([true, true, true, true, false]);

    fakes.prepare(&host).0.unwrap();
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 10);
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(9));
}

#[test]
fn a_unit_that_does_not_stay_active_fails_after_twenty_polls() {
    let host = Host::new();
    let fakes = Fakes::default();
    fakes.systemd.stays_down.set(true);

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("quoro.service did not stay active"),
        "{error}"
    );
    assert!(
        error.contains("journalctl -u quoro.service -n 50 --no-pager"),
        "{error}"
    );
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 20);
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(20));
}

#[test]
fn a_listener_that_never_answers_fails_within_the_bound() {
    let host = Host::without_listener();
    let fakes = Fakes::default();

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("nothing accepted a connection on 127.0.0.1:1"),
        "{error}"
    );
    // Four waits while the unit reads active five times, then 29 between
    // the 30 tries.
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(4 + 29));
}

#[test]
fn the_embedded_unit_is_the_packaged_one() {
    let packaged =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packaging/systemd/quoro.service");
    assert_eq!(UNIT_FILE, fs::read_to_string(packaged).unwrap());
}

#[test]
fn the_first_executable_shell_is_picked_or_else_the_last() {
    let scratch = Scratch::new();
    let shell = |name: &str| scratch.0.join(name);
    write_mode(&shell("plain"), b"", 0o644);
    write_mode(&shell("nologin"), b"", 0o755);
    write_mode(&shell("false"), b"", 0o755);

    let shells = [
        shell("missing"),
        shell("plain"),
        shell("nologin"),
        shell("false"),
    ];
    assert_eq!(pick_shell(&shells), Some(shell("nologin").as_path()));
    let shells = [shell("missing"), shell("plain")];
    assert_eq!(pick_shell(&shells), Some(shell("plain").as_path()));
    assert_eq!(pick_shell(&[]), None);
}

#[test]
fn a_file_without_a_node_table_gets_the_installer_s_table_appended() {
    let text = "listen = \"0.0.0.0:8443\"\n# the end\n";
    let (edited, edit) = set_local_false(text).unwrap();
    assert_eq!(edit, LocalEdit::Appended);
    assert_eq!(edited, format!("{text}\n[node]\n{ADDED}"));

    // A last line without its newline gets one first.
    let (edited, _) = set_local_false("engine = \"fake\"").unwrap();
    assert_eq!(edited, format!("engine = \"fake\"\n\n[node]\n{ADDED}"));
}

#[test]
fn local_true_becomes_false_and_nothing_else_moves() {
    let text = "[node]\n  local=true   # the agent runs in quoro serve\ndata_dir = \"/x\"\n";
    let (edited, edit) = set_local_false(text).unwrap();
    assert_eq!(edit, LocalEdit::Replaced);
    assert_eq!(
        edited,
        "[node]\n  local=false   # the agent runs in quoro serve\ndata_dir = \"/x\"\n"
    );
}

#[test]
fn local_false_is_left_as_it_is() {
    let text = "[node]\nlocal = false # already\n";
    let (edited, edit) = set_local_false(text).unwrap();
    assert_eq!(edit, LocalEdit::AlreadySet);
    assert_eq!(edited, text);
}

#[test]
fn comments_and_other_tables_are_kept_byte_for_byte() {
    let text = "# Quoro configuration.\n\
                listen = \"0.0.0.0:8443\"   # one listener\n\
                \n\
                [cloud_hypervisor]\n\
                # bridge = \"quoro0\"\n\
                subnet = \"10.42.0.0/24\"\n\
                \n\
                [ node ]  # the agent\n\
                data_dir = \"/var/lib/quoro-node\"\n\
                \n\
                # Ingress settings.\n\
                [ingress]\n\
                acme_contact = [\"mailto:ops@example.com\"]\n";
    let (edited, edit) = set_local_false(text).unwrap();
    assert_eq!(edit, LocalEdit::Added);
    let line = "data_dir = \"/var/lib/quoro-node\"\n";
    assert_eq!(edited, text.replacen(line, &format!("{line}{ADDED}"), 1));
    let config: Config = toml::from_str(&edited).unwrap();
    assert!(!config.node.local);
    assert_eq!(config.node.data_dir, PathBuf::from("/var/lib/quoro-node"));
}

#[test]
fn a_file_with_crlf_line_ends_gets_crlf_lines() {
    let text = "[node]\r\ndata_dir = \"/x\"\r\n";
    let (edited, _) = set_local_false(text).unwrap();
    assert_eq!(
        edited,
        "[node]\r\ndata_dir = \"/x\"\r\n\
         # The node agent runs as quoro-node.service (ADR 0089).\r\nlocal = false\r\n"
    );
}

#[test]
fn a_node_table_written_another_way_is_refused() {
    for text in ["node.local = true\n", "node = { local = true }\n"] {
        let error = set_local_false(text).unwrap_err();
        assert!(error.contains("by hand"), "{text:?}: {error}");
    }
}

#[test]
fn a_node_table_written_another_way_refuses_before_any_change() {
    let host = Host::new();
    let text = format!(
        "listen = \"{}\"\ndata_dir = \"{}\"\nnode = {{ local = true }}\n",
        host.listen,
        host.data_dir().display()
    );
    write_mode(&host.config(), text.as_bytes(), 0o640);
    let fakes = Fakes::default();
    let before = host.snapshot();

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(error.contains("by hand"), "{error}");
    assert!(error.ends_with("nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();
}

/// An enrolled agent's `identity.json` in `dir`; returns its pin.
fn enrolled(dir: &Path, primary_url: &str) -> String {
    let cluster = quoro_proto::sign::signing_key_from_seed(&[3; 32]);
    let public_key = quoro_proto::sign::public_key_to_base64(&cluster.verifying_key());
    let key_id = Identity::key_id_for(&public_key).unwrap();
    let fingerprint = format!("sha256:{}", "ab".repeat(32));
    // JSON, not a struct literal, so a member added to `Identity` later is
    // not filled in here by accident.
    let text = format!(
        r#"{{
  "node_id": "nod_test",
  "cluster_id": "cls_test",
  "primary_url": "{primary_url}",
  "primary_urls": ["https://10.0.0.1:8443"],
  "fingerprint": "{fingerprint}",
  "cluster_public_key": "{public_key}",
  "cluster_key_id": "{key_id}"
}}"#
    );
    write_mode(&dir.join(identity::IDENTITY_FILE), text.as_bytes(), 0o600);
    fingerprint
}

/// An agent whose handshake is faked and whose identity write is the real
/// one; both go to the systemd fake's log, so the order shows.
struct FakeAgent<'a> {
    data_dir: PathBuf,
    refuse: bool,
    log: &'a RefCell<Vec<String>>,
}

impl Agent for FakeAgent<'_> {
    async fn verify(&self, url: &str) -> Result<(String, String), String> {
        self.log.borrow_mut().push(format!("verify {url}"));
        if self.refuse {
            return Err(format!(
                "the certificate at {url} is not the pinned primary certificate; nothing was changed"
            ));
        }
        let identity = identity::load_identity(&self.data_dir).map_err(|e| e.to_string())?;
        Ok((identity.primary_url, url.to_string()))
    }

    fn write(&self, url: &str) -> Result<(), String> {
        self.log.borrow_mut().push("write identity".to_string());
        NodeAgent {
            data_dir: self.data_dir.clone(),
        }
        .write(url)
    }
}

const LOCAL_URL: &str = "https://127.0.0.1:8443";

#[tokio::test]
async fn the_agent_is_pointed_at_the_local_control_plane_and_keeps_its_pin() {
    let scratch = Scratch::new();
    let fingerprint = enrolled(&scratch.0, "https://old.example.com:8443");
    let systemd = FakeSystemd::default();
    systemd.node_active.set(true);
    let agent = FakeAgent {
        data_dir: scratch.0.clone(),
        refuse: false,
        log: &systemd.log,
    };

    let pointed = point_agent(LOCAL_URL, &agent, &systemd, &mut Out::quiet())
        .await
        .unwrap();
    assert_eq!(
        pointed,
        Pointed::Changed {
            previous: "https://old.example.com:8443".to_string(),
            restarted: true,
        }
    );
    // The pin first, then the stop, the write and the start, as
    // `quoro node set-primary` does.
    assert_eq!(
        systemd.sequence(),
        [
            format!("verify {LOCAL_URL}"),
            "is-active quoro-node.service".to_string(),
            "stop quoro-node.service".to_string(),
            "write identity".to_string(),
            "start quoro-node.service".to_string(),
        ]
    );
    let identity = identity::load_identity(&scratch.0).unwrap();
    assert_eq!(identity.primary_url, LOCAL_URL);
    assert_eq!(identity.fingerprint, fingerprint, "the pin never changes");
    assert_eq!(identity.primary_urls, ["https://10.0.0.1:8443"]);
    assert_eq!(identity.node_id, "nod_test");
    assert_eq!(identity.cluster_id, "cls_test");
}

#[tokio::test]
async fn an_agent_that_already_dials_it_is_left_alone() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, LOCAL_URL);
    let path = scratch.0.join(identity::IDENTITY_FILE);
    let before = fs::read(&path).unwrap();
    let systemd = FakeSystemd::default();
    systemd.node_active.set(true);
    let agent = FakeAgent {
        data_dir: scratch.0.clone(),
        refuse: false,
        log: &systemd.log,
    };

    let pointed = point_agent(LOCAL_URL, &agent, &systemd, &mut Out::quiet())
        .await
        .unwrap();
    assert_eq!(pointed, Pointed::Already);
    assert_eq!(systemd.sequence(), [format!("verify {LOCAL_URL}")]);
    assert_eq!(fs::read(path).unwrap(), before);
}

#[tokio::test]
async fn a_refused_pin_changes_nothing() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, "https://old.example.com:8443");
    let path = scratch.0.join(identity::IDENTITY_FILE);
    let before = fs::read(&path).unwrap();
    let systemd = FakeSystemd::default();
    systemd.node_active.set(true);
    let agent = FakeAgent {
        data_dir: scratch.0.clone(),
        refuse: true,
        log: &systemd.log,
    };

    let error = point_agent(LOCAL_URL, &agent, &systemd, &mut Out::quiet())
        .await
        .unwrap_err();
    assert!(
        error.contains("not the pinned primary certificate"),
        "{error}"
    );
    assert_eq!(systemd.sequence(), [format!("verify {LOCAL_URL}")]);
    assert_eq!(fs::read(path).unwrap(), before);
}

#[tokio::test]
async fn an_agent_that_is_not_running_is_pointed_and_left_stopped() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, "https://old.example.com:8443");
    let systemd = FakeSystemd::default();
    let agent = FakeAgent {
        data_dir: scratch.0.clone(),
        refuse: false,
        log: &systemd.log,
    };
    let mut out = Out::quiet();

    let pointed = point_agent(LOCAL_URL, &agent, &systemd, &mut out)
        .await
        .unwrap();
    assert_eq!(
        pointed,
        Pointed::Changed {
            previous: "https://old.example.com:8443".to_string(),
            restarted: false,
        }
    );
    assert_eq!(
        systemd.sequence(),
        [
            format!("verify {LOCAL_URL}"),
            "is-active quoro-node.service".to_string(),
            "write identity".to_string(),
        ]
    );
    assert!(
        out.lines
            .iter()
            .any(|line| line.contains("was not running")),
        "{:?}",
        out.lines
    );
    assert_eq!(
        identity::load_identity(&scratch.0).unwrap().primary_url,
        LOCAL_URL
    );
}
