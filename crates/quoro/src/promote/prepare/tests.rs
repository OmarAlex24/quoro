//! The host preparation of `quoro cluster promote` on a host laid out in a
//! temporary directory, with a fake systemd, account database, owner change,
//! listener and clock, so nothing needs root.

use super::*;
use quoro_node_agent::identity::{self, Identity};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};

/// The uid and gid the fake `useradd` gives `quoro-api`.
const SYSTEM_UID: u32 = 997;
const SYSTEM_GID: u32 = 996;

/// What `install_primary_config` writes above `local = false`.
const ADDED: &str = "# The node agent runs as quoro-node.service (ADR 0089).\nlocal = false\n";

/// A node host's `quoro.toml` as `install_node_config` writes it: no
/// `listen` and no `data_dir`, so the defaults apply.
const NODE_CONFIG: &str = "\
# Quoro configuration (ADR 0089 privilege split).
# This host is a node: it runs microVMs for its primary and runs no
# control plane. Edit this file and restart with:
#   systemctl restart quoro-node

# Engine: \"cloud-hypervisor\" runs one microVM per app; \"fake\" runs nothing
# and exists for development only. An edge starts no VM, so the agent
# ignores this line and runs without an engine.
engine = \"cloud-hypervisor\"

# Engine settings. The values below are the defaults, and the block is
# commented out; uncomment a line only to change it. Guest DNS defaults
# to this host's non-loopback resolvers; set dns to override them.
# [cloud_hypervisor]
# artifacts_dir = \"/usr/local/lib/quoro\"
# init_binary = \"/usr/local/lib/quoro/quoro-init\"
# state_dir = \"/var/lib/quoro-node/engine\"
# bridge = \"quoro0\"
# subnet = \"10.42.0.0/24\"
# dns = [\"192.0.2.53\"]

[node]
# The identity, local store and engine state of the agent on this host.
# data_dir = \"/var/lib/quoro-node\"
";

/// The address the agent dials with the default `listen`, `0.0.0.0:8443`.
const LOOPBACK: &str = "127.0.0.1:8443";

/// What the promotion wrote in the data directory before this step, as
/// root: the database with its WAL and shared-memory files, and the keys.
const WRITTEN: [&str; 7] = [
    "quoro.db",
    "quoro.db-wal",
    "quoro.db-shm",
    "keys/signing.key",
    "keys/data.key",
    "tls/cert.pem",
    "tls/key.pem",
];

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A directory of its own under the system's temporary directory, removed
/// on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "quoro-test-promote-{}-{}",
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

/// A node host that the promotion brought to step 6, under a temporary
/// root: `etc/quoro/quoro.toml` as the installer wrote it (`0640`),
/// `etc/systemd/system` without the control plane's unit, `var/lib/quoro`
/// with what the promotion wrote ([`WRITTEN`], `0600`) and the M1 engine
/// state `engine/state`, and `shells/` with an executable `nologin` and
/// `false`.
struct Host {
    scratch: Scratch,
}

impl Host {
    fn new() -> Self {
        let host = Self {
            scratch: Scratch::new(),
        };
        for dir in [
            "etc/quoro",
            "etc/systemd/system",
            "var/lib/quoro/keys",
            "var/lib/quoro/tls",
            "var/lib/quoro/engine",
            "shells",
        ] {
            fs::create_dir_all(host.root().join(dir)).unwrap();
        }
        fs::set_permissions(host.data_dir(), fs::Permissions::from_mode(0o755)).unwrap();
        for name in WRITTEN {
            write_mode(&host.data_dir().join(name), name.as_bytes(), 0o600);
        }
        write_mode(&host.data_dir().join("engine/state"), b"m1", 0o600);
        write_mode(&host.root().join("shells/nologin"), b"", 0o755);
        write_mode(&host.root().join("shells/false"), b"", 0o755);
        write_mode(&host.config(), NODE_CONFIG.as_bytes(), 0o640);
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
        self.root().join("etc/systemd/system/quoro.service")
    }

    fn options(&self) -> Options {
        Options {
            config: PathBuf::from(CONFIG_PATH),
            root: self.root().to_path_buf(),
            shells: vec![
                self.root().join("shells/missing"),
                self.root().join("shells/nologin"),
                self.root().join("shells/false"),
            ],
        }
    }

    /// Every name under `etc` and `var`, with each file's bytes and each
    /// link's target, so a test can tell that nothing changed.
    fn snapshot(&self) -> Vec<(PathBuf, Option<Vec<u8>>)> {
        let mut entries = Vec::new();
        let mut stack = vec![self.root().join("etc"), self.root().join("var")];
        while let Some(path) = stack.pop() {
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_symlink() {
                let target = fs::read_link(&path).unwrap();
                entries.push((path, Some(target.as_os_str().as_bytes().to_vec())));
            } else if metadata.is_dir() {
                for entry in fs::read_dir(&path).unwrap() {
                    stack.push(entry.unwrap().path());
                }
                entries.push((path, None));
            } else {
                let bytes = fs::read(&path).unwrap();
                entries.push((path, Some(bytes)));
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
    /// What `is-active quoro.service` answers until this fake started it.
    active_before: Cell<bool>,
    /// Whether `start` or `restart` of `quoro.service` ran.
    started: Cell<bool>,
    /// The answers of `is-active quoro.service` once it started, in order.
    /// Once they are used up, the unit reads active, or inactive with
    /// `stays_down`.
    answers: RefCell<VecDeque<bool>>,
    stays_down: Cell<bool>,
    /// What `is-active quoro-node.service` answers.
    node_active: Cell<bool>,
    /// The data directory a start of `quoro.service` writes in, as `quoro
    /// serve` does: the database when it is missing, and its WAL and
    /// shared-memory files.
    serves: RefCell<Option<PathBuf>>,
    /// A file `daemon-reload` removes, as if it went away after the checks.
    lost_on_reload: RefCell<Option<PathBuf>>,
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

    fn bring_up(&self, call: &str, unit: &str) -> Result<(), String> {
        if unit == UNIT {
            self.started.set(true);
            if let Some(dir) = self.serves.borrow().as_deref() {
                let database = dir.join(DATABASE);
                if !database.exists() {
                    write_mode(&database, b"SQLite format 3\0", 0o600);
                }
                for side in ["quoro.db-wal", "quoro.db-shm"] {
                    write_mode(&dir.join(side), b"served", 0o600);
                }
            }
        }
        self.record(format!("{call} {unit}"))
    }
}

impl Systemd for FakeSystemd {
    fn daemon_reload(&self) -> Result<(), String> {
        if let Some(path) = self.lost_on_reload.borrow().as_deref() {
            fs::remove_file(path).unwrap();
        }
        self.record("daemon-reload".to_string())
    }

    fn enable(&self, unit: &str) -> Result<(), String> {
        self.record(format!("enable {unit}"))
    }

    fn start(&self, unit: &str) -> Result<(), String> {
        self.bring_up("start", unit)
    }

    fn restart(&self, unit: &str) -> Result<(), String> {
        self.bring_up("restart", unit)
    }

    fn stop(&self, unit: &str) -> Result<(), String> {
        self.record(format!("stop {unit}"))
    }

    fn is_active(&self, unit: &str) -> bool {
        self.log.borrow_mut().push(format!("is-active {unit}"));
        if unit != UNIT {
            return self.node_active.get();
        }
        if !self.started.get() {
            return self.active_before.get();
        }
        let next = self.answers.borrow_mut().pop_front();
        next.unwrap_or(!self.stays_down.get())
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

    /// The paths given an owner, in order.
    fn chowned(&self) -> Vec<PathBuf> {
        self.events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                Event::Chown { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect()
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

    fn chown_opened(&self, fd: BorrowedFd<'_>, path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        // The descriptor is the entry at `path`, opened without following
        // a link.
        let opened = rustix::fs::fstat(fd)?;
        let named = fs::symlink_metadata(path)?;
        assert_eq!((opened.st_dev, opened.st_ino), (named.dev(), named.ino()));
        self.chown(path, uid, gid)
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

/// A listener that accepts, or refuses with `refuses`, and records every
/// address tried.
#[derive(Default)]
struct FakeProbe {
    refuses: Cell<bool>,
    tried: RefCell<Vec<SocketAddr>>,
}

impl Probe for FakeProbe {
    fn connect(&self, address: SocketAddr, timeout: Duration) -> Result<(), String> {
        assert_eq!(timeout, POLL);
        self.tried.borrow_mut().push(address);
        if self.refuses.get() {
            Err("Connection refused (os error 111)".to_string())
        } else {
            Ok(())
        }
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
    probe: FakeProbe,
    clock: FakeClock,
}

impl Fakes {
    /// Fakes whose `quoro.service` writes in `host`'s data directory when
    /// it starts.
    fn serving(host: &Host) -> Self {
        let fakes = Self::default();
        *fakes.systemd.serves.borrow_mut() = Some(host.data_dir());
        fakes
    }

    fn seams(&self) -> Seams<'_> {
        Seams {
            systemd: &self.systemd,
            accounts: &self.accounts,
            fs: &self.fs,
            probe: &self.probe,
            clock: &self.clock,
        }
    }

    fn prepare(&self, host: &Host) -> (Result<Prepared, String>, Vec<String>) {
        self.prepare_with(&host.options())
    }

    fn prepare_with(&self, options: &Options) -> (Result<Prepared, String>, Vec<String>) {
        let mut out = Out::quiet();
        let result = prepare_host(options, &self.seams(), &mut out);
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
        assert!(self.probe.tried.borrow().is_empty(), "no connection");
    }
}

/// Asserts that `host` refuses with an error that holds `says` and ends by
/// saying that nothing was changed, and that nothing changed.
fn assert_refused(host: &Host, says: &str) {
    let fakes = Fakes::serving(host);
    let before = host.snapshot();
    let error = fakes.prepare(host).0.unwrap_err();
    assert!(error.contains(says), "{error}");
    assert!(error.ends_with("nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();
}

#[test]
fn a_node_host_is_prepared_as_the_installer_prepares_a_primary() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
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
            handed: WRITTEN.len() + 2,
            local: LocalEdit::Added,
            pre_promote: PrePromote::Written,
            unit_changed: true,
            brought: Brought::Restarted,
            listen: crate::config::DEFAULT_LISTEN.parse().unwrap(),
            url: format!("https://{LOOPBACK}"),
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

    // Step 2: the data directory, quoro-api:quoro-api 0700, and /etc/quoro.
    assert_eq!(mode_of(&host.data_dir()), 0o700);
    assert_eq!(
        fakes.fs.owner(&host.data_dir()),
        Some((SYSTEM_UID, SYSTEM_GID))
    );
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

    // Step 4: the packaged unit, root:root 0644, reloaded, enabled,
    // restarted because it is new, and waited for on the default listener.
    assert_eq!(fs::read_to_string(host.unit()).unwrap(), UNIT_FILE);
    assert_eq!(mode_of(&host.unit()), 0o644);
    assert_eq!(fakes.fs.owner(&host.unit()), Some((0, 0)));
    assert_eq!(
        fakes.systemd.sequence(),
        [
            "is-active quoro.service",
            "daemon-reload",
            "enable quoro.service",
            "restart quoro.service",
            "is-active quoro.service",
        ]
    );
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 1 + 5);
    let loopback: SocketAddr = LOOPBACK.parse().unwrap();
    assert_eq!(*fakes.probe.tried.borrow(), [loopback]);
    assert_eq!(
        lines.last().map(String::as_str),
        Some(format!("the control plane accepts connections on {LOOPBACK}").as_str())
    );

    // No partial file is left.
    for dir in [etc, host.root().join("etc/systemd/system")] {
        for entry in fs::read_dir(dir).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().ends_with(".promote-partial"),
                "{name:?} is left"
            );
        }
    }
}

/// Section 7.3: the promotion wrote the database and the keys as root. The
/// hand-over gives every one of them, and every directory, to quoro-api,
/// each directory after its contents and the data directory last, and
/// leaves `engine/` root's, as `install_primary_data_dir` does.
#[test]
fn the_data_directory_is_handed_to_quoro_api_but_engine() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    let prepared = fakes.prepare(&host).0.unwrap();

    let data = host.data_dir();
    let chowned = fakes.fs.chowned();
    let mut handed: Vec<PathBuf> = WRITTEN.iter().map(|name| data.join(name)).collect();
    handed.extend([data.join("keys"), data.join("tls"), data.clone()]);
    for path in &handed {
        assert_eq!(
            fakes.fs.owner(path),
            Some((SYSTEM_UID, SYSTEM_GID)),
            "{path:?}"
        );
        assert_eq!(
            chowned.iter().filter(|chowned| *chowned == path).count(),
            1,
            "{path:?} is handed over once"
        );
    }
    assert_eq!(prepared.handed, handed.len() - 1);
    assert_eq!(fakes.fs.owner(&data.join("engine")), None);
    assert_eq!(fakes.fs.owner(&data.join("engine/state")), None);
    // The modes are kept.
    assert_eq!(mode_of(&data.join("keys/data.key")), 0o600);

    let at = |path: PathBuf| chowned.iter().position(|chowned| *chowned == path).unwrap();
    assert!(at(data.join("keys/signing.key")) < at(data.join("keys")));
    assert!(at(data.join("tls/key.pem")) < at(data.join("tls")));
    let last_inside = handed[..handed.len() - 1]
        .iter()
        .map(|path| at(path.clone()))
        .max()
        .unwrap();
    assert!(last_inside < at(data.clone()), "the directory comes last");
}

#[test]
fn each_file_is_written_whole_then_given_its_mode_and_owner_then_put_in_place() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
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

/// Section 7.3 writes the database and the keys before the start. A data
/// directory without one of them refuses before any change, so the control
/// plane never starts empty or with keys of its own.
#[test]
fn a_data_directory_without_the_database_or_a_key_refuses_and_starts_nothing() {
    for name in PRIMARY_FILES {
        let host = Host::new();
        fs::remove_file(host.data_dir().join(name)).unwrap();
        assert_refused(&host, &format!("does not hold {name} as a regular file"));
    }

    let host = Host::new();
    fs::remove_dir_all(host.data_dir()).unwrap();
    assert_refused(&host, "does not exist");
}

/// The files are checked again right before the start: one that went away
/// after the changes began stops the run with the unit not started.
#[test]
fn a_file_lost_before_the_start_leaves_the_unit_not_started() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    *fakes.systemd.lost_on_reload.borrow_mut() = Some(host.data_dir().join("keys/data.key"));

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(error.contains("does not hold keys/data.key"), "{error}");
    assert!(error.ends_with("quoro.service was not started"), "{error}");
    assert_eq!(fakes.systemd.count("start quoro.service"), 0);
    assert_eq!(fakes.systemd.count("restart quoro.service"), 0);
    assert!(!host.data_dir().join("keys/data.key").exists());
}

/// A symbolic link where this step reads or hands something over refuses
/// before any change: at `quoro.toml`, at `/etc/quoro`, at `/var/lib/quoro`
/// and inside it.
#[test]
fn a_symbolic_link_refuses_before_any_change() {
    let host = Host::new();
    let real = host.root().join("elsewhere.toml");
    fs::rename(host.config(), &real).unwrap();
    std::os::unix::fs::symlink(&real, host.config()).unwrap();
    assert_refused(&host, "quoro.toml is not a regular file");

    let host = Host::new();
    let etc = host.root().join("etc/quoro");
    fs::rename(&etc, host.root().join("etc/quoro-real")).unwrap();
    std::os::unix::fs::symlink("quoro-real", &etc).unwrap();
    assert_refused(&host, "the configuration directory");
    assert_refused(&host, "is a symbolic link");

    let host = Host::new();
    let data = host.data_dir();
    fs::rename(&data, host.root().join("var/lib/quoro-real")).unwrap();
    std::os::unix::fs::symlink("quoro-real", &data).unwrap();
    assert_refused(&host, "the data directory");
    assert_refused(&host, "is a symbolic link");

    // A link inside the data directory, to a directory or to a file.
    for target in ["/etc", "/etc/passwd"] {
        let host = Host::new();
        std::os::unix::fs::symlink(target, host.data_dir().join("tls/extra")).unwrap();
        assert_refused(&host, "tls/extra is a symbolic link");
    }

    // A key reached through a link is not in the data directory.
    let host = Host::new();
    let keys = host.data_dir().join("keys");
    fs::rename(&keys, host.root().join("keys-real")).unwrap();
    std::os::unix::fs::symlink(host.root().join("keys-real"), &keys).unwrap();
    assert_refused(&host, "does not hold keys/signing.key, keys/data.key");
}

/// A file with another hard link may have a name outside the data
/// directory, which the hand-over would give to quoro-api too.
#[test]
fn a_file_with_another_hard_link_refuses_before_any_change() {
    let host = Host::new();
    let outside = host.root().join("outside");
    write_mode(&outside, b"root's", 0o600);
    fs::hard_link(&outside, host.data_dir().join("linked")).unwrap();
    assert_refused(&host, "linked has 2 hard links");
}

/// The unit runs `quoro serve` with `QUORO_CONFIG=/etc/quoro/quoro.toml`
/// and may write only `/var/lib/quoro`.
#[test]
fn another_configuration_or_data_directory_refuses_before_any_change() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    let before = host.snapshot();
    let mut options = host.options();
    options.config = PathBuf::from("/etc/quoro/other.toml");
    let error = fakes.prepare_with(&options).0.unwrap_err();
    assert!(
        error.contains("QUORO_CONFIG=/etc/quoro/quoro.toml"),
        "{error}"
    );
    assert!(error.ends_with("nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();

    let host = Host::new();
    let text = NODE_CONFIG.replacen("engine = ", "data_dir = \"/srv/quoro\"\nengine = ", 1);
    write_mode(&host.config(), text.as_bytes(), 0o640);
    assert_refused(&host, "may write only /var/lib/quoro");

    // Naming the default is the same as leaving it out.
    let host = Host::new();
    let text = NODE_CONFIG.replacen("engine = ", "data_dir = \"/var/lib/quoro\"\nengine = ", 1);
    write_mode(&host.config(), text.as_bytes(), 0o640);
    Fakes::serving(&host).prepare(&host).0.unwrap();
}

#[test]
fn a_quoro_api_with_a_login_uid_refuses_and_changes_nothing() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
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
fn a_quoro_api_without_its_group_refuses_and_changes_nothing() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    fakes.accounts.user.set(Some(999));
    let before = host.snapshot();

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("exists without a group quoro-api"),
        "{error}"
    );
    assert!(error.ends_with("nothing was changed"), "{error}");
    assert_eq!(host.snapshot(), before);
    fakes.assert_untouched();
}

#[test]
fn an_existing_system_quoro_api_is_kept() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    fakes.accounts.user.set(Some(999));
    fakes.accounts.group.set(Some(990));

    let (result, lines) = fakes.prepare(&host);
    let prepared = result.unwrap();
    assert!(!prepared.user_created);
    assert_eq!((prepared.uid, prepared.gid), (999, 990));
    assert!(fakes.accounts.created.borrow().is_empty());
    assert!(lines.contains(&"user quoro-api exists (uid 999); left as is".to_string()));
    assert_eq!(fakes.fs.owner(&host.data_dir()), Some((999, 990)));
    assert_eq!(
        fakes.fs.owner(&host.data_dir().join("quoro.db")),
        Some((999, 990))
    );
    assert_eq!(fakes.fs.owner(&host.config()), Some((0, 990)));
}

/// `getent` as the host answers it: the id in the third field, `None` for
/// exit status 2 (no such entry), and an error for any other status.
#[test]
fn getent_s_answer_is_read_as_the_installer_reads_it() {
    let passwd = "quoro-api:x:997:996::/var/lib/quoro:/usr/sbin/nologin\n";
    assert_eq!(
        read_getent("passwd", "quoro-api", Some(0), passwd, ""),
        Ok(Some(997))
    );
    let group = "quoro-api:x:996:\n";
    assert_eq!(
        read_getent("group", "quoro-api", Some(0), group, ""),
        Ok(Some(996))
    );
    assert_eq!(
        read_getent("passwd", "quoro-api", Some(2), "", ""),
        Ok(None)
    );
    let error = read_getent("passwd", "quoro-api", Some(1), "", "bad option").unwrap_err();
    assert!(error.contains("exit status 1: bad option"), "{error}");
    let error = read_getent("passwd", "quoro-api", None, "", "").unwrap_err();
    assert!(error.contains("killed by a signal"), "{error}");
    let error = read_getent("passwd", "quoro-api", Some(0), "quoro-api:x\n", "").unwrap_err();
    assert!(error.contains("printed no id"), "{error}");

    // The host's own getent: root is uid and gid 0, and a name no host has
    // is no entry.
    assert_eq!(RealAccounts.uid("root"), Ok(Some(0)));
    assert_eq!(RealAccounts.gid("root"), Ok(Some(0)));
    assert_eq!(RealAccounts.uid("quoro-no-such-user-x9"), Ok(None));
    assert_eq!(RealAccounts.gid("quoro-no-such-group-x9"), Ok(None));
}

#[test]
fn a_rerun_finds_every_step_done_and_keeps_it() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    let original = fs::read(host.config()).unwrap();
    fakes.prepare(&host).0.unwrap();
    let after_first = host.snapshot();

    let (result, lines) = fakes.prepare(&host);
    let prepared = result.unwrap();
    assert!(!prepared.user_created);
    assert_eq!(prepared.local, LocalEdit::AlreadySet);
    assert_eq!(prepared.pre_promote, PrePromote::Kept);
    assert!(!prepared.unit_changed);
    // The unit is active, so it is restarted.
    assert_eq!(prepared.brought, Brought::Restarted);
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
    assert_eq!(fakes.systemd.count("restart quoro.service"), 2);
}

/// A run whose unit did not stay active can be run again: the database the
/// start opened refuses nothing, the files it wrote are handed over too,
/// and an inactive unit whose file did not change is started.
#[test]
fn a_rerun_after_the_unit_did_not_stay_active_finishes() {
    let host = Host::new();
    fs::remove_file(host.data_dir().join("quoro.db-wal")).unwrap();
    fs::remove_file(host.data_dir().join("quoro.db-shm")).unwrap();
    let fakes = Fakes::serving(&host);
    fakes.systemd.stays_down.set(true);
    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(error.contains("did not stay active"), "{error}");
    let wal = host.data_dir().join("quoro.db-wal");
    assert!(wal.exists(), "the start wrote it");

    fakes.systemd.stays_down.set(false);
    fakes.systemd.answers.borrow_mut().push_back(false);
    fakes.fs.events.borrow_mut().clear();
    let prepared = fakes.prepare(&host).0.unwrap();
    assert_eq!(prepared.brought, Brought::Started);
    assert!(!prepared.unit_changed);
    assert_eq!(fakes.systemd.count("start quoro.service"), 1);
    assert_eq!(fakes.fs.owner(&wal), Some((SYSTEM_UID, SYSTEM_GID)));
    assert!(
        fakes.fs.chowned().contains(&wal),
        "handed over by the rerun"
    );
}

/// A run whose listener never answered can be run again: the active unit
/// is restarted and the rerun finishes.
#[test]
fn a_rerun_after_the_listener_did_not_answer_finishes() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    fakes.probe.refuses.set(true);
    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(error.contains("nothing accepted a connection"), "{error}");

    fakes.probe.refuses.set(false);
    let prepared = fakes.prepare(&host).0.unwrap();
    assert_eq!(prepared.brought, Brought::Restarted);
    assert_eq!(fakes.systemd.count("restart quoro.service"), 2);
    assert_eq!(fakes.systemd.count("start quoro.service"), 0);
}

#[test]
fn a_run_stopped_between_the_copy_and_the_edit_is_finished_by_the_next() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
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
    let fakes = Fakes::serving(&host);

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

/// A run stopped after the copy was linked and before its partial name was
/// removed leaves that name; the next run keeps the copy and removes it.
#[test]
fn a_stray_partial_name_of_a_kept_copy_is_removed() {
    let host = Host::new();
    let original = fs::read(host.config()).unwrap();
    write_mode(&host.copy(), &original, 0o640);
    let stray = host
        .root()
        .join("etc/quoro/.quoro.toml.pre-promote.promote-partial");
    fs::hard_link(host.copy(), &stray).unwrap();
    let fakes = Fakes::serving(&host);

    let prepared = fakes.prepare(&host).0.unwrap();
    assert_eq!(prepared.pre_promote, PrePromote::Kept);
    assert!(!stray.exists(), "the stray name is removed");
    assert_eq!(fs::read(host.copy()).unwrap(), original);
}

#[test]
fn the_unit_must_read_active_five_polls_in_a_row() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    fakes
        .systemd
        .answers
        .borrow_mut()
        .extend([true, true, true, true, false]);

    fakes.prepare(&host).0.unwrap();
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 1 + 10);
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(9));
}

#[test]
fn a_unit_that_does_not_stay_active_fails_after_twenty_polls() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
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
    assert_eq!(fakes.systemd.count("is-active quoro.service"), 1 + 20);
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(20));
    assert!(fakes.probe.tried.borrow().is_empty());
}

#[test]
fn a_listener_that_never_answers_fails_within_the_bound() {
    let host = Host::new();
    let fakes = Fakes::serving(&host);
    fakes.probe.refuses.set(true);

    let error = fakes.prepare(&host).0.unwrap_err();
    assert!(
        error.contains("nothing accepted a connection on 127.0.0.1:8443 (Connection refused"),
        "{error}"
    );
    assert_eq!(fakes.probe.tried.borrow().len(), 30);
    // Four waits while the unit reads active five times, then 29 between
    // the 30 tries.
    assert_eq!(fakes.clock.slept.get(), Duration::from_secs(4 + 29));
}

#[test]
fn the_embedded_unit_is_the_packaged_one() {
    let packaged =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packaging/systemd/quoro.service");
    assert_eq!(UNIT_FILE, fs::read_to_string(packaged).unwrap());
    // The two places this step requires are the unit's.
    assert!(UNIT_FILE.contains(&format!("QUORO_CONFIG={CONFIG_PATH}\n")));
    assert!(UNIT_FILE.contains(&format!("ReadWritePaths={DATA_DIR}\n")));
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
    write_mode(
        &host.config(),
        b"engine = \"cloud-hypervisor\"\nnode = { local = true }\n",
        0o640,
    );
    assert_refused(&host, "by hand");
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

/// An agent of `systemd`'s log, enrolled with `primary_url` in `scratch`.
fn agent<'a>(scratch: &Scratch, systemd: &'a FakeSystemd, refuse: bool) -> FakeAgent<'a> {
    FakeAgent {
        data_dir: scratch.0.clone(),
        refuse,
        log: &systemd.log,
    }
}

#[tokio::test]
async fn the_agent_is_pointed_at_the_local_control_plane_and_keeps_its_pin() {
    let scratch = Scratch::new();
    let fingerprint = enrolled(&scratch.0, "https://old.example.com:8443");
    let systemd = FakeSystemd::default();
    systemd.node_active.set(true);

    let pointed = point_agent(
        LOCAL_URL,
        &agent(&scratch, &systemd, false),
        &systemd,
        &mut Out::quiet(),
    )
    .await
    .unwrap();
    assert_eq!(
        pointed,
        Pointed::Changed {
            previous: "https://old.example.com:8443".to_string(),
            was_running: true,
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
async fn a_running_agent_that_already_dials_it_is_left_alone() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, LOCAL_URL);
    let path = scratch.0.join(identity::IDENTITY_FILE);
    let before = fs::read(&path).unwrap();
    let systemd = FakeSystemd::default();
    systemd.node_active.set(true);
    let mut out = Out::quiet();

    let pointed = point_agent(
        LOCAL_URL,
        &agent(&scratch, &systemd, false),
        &systemd,
        &mut out,
    )
    .await
    .unwrap();
    assert_eq!(pointed, Pointed::Already { started: false });
    assert_eq!(
        systemd.sequence(),
        [
            format!("verify {LOCAL_URL}"),
            "is-active quoro-node.service".to_string(),
        ]
    );
    assert_eq!(fs::read(path).unwrap(), before);
    assert_eq!(out.lines.last().unwrap(), "quoro-node.service is active");
}

/// A rerun after a start of the agent that failed finds the address set:
/// it starts the agent, so the run never ends with the agent stopped.
#[tokio::test]
async fn a_stopped_agent_that_already_dials_it_is_started() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, LOCAL_URL);
    let path = scratch.0.join(identity::IDENTITY_FILE);
    let before = fs::read(&path).unwrap();
    let systemd = FakeSystemd::default();

    let pointed = point_agent(
        LOCAL_URL,
        &agent(&scratch, &systemd, false),
        &systemd,
        &mut Out::quiet(),
    )
    .await
    .unwrap();
    assert_eq!(pointed, Pointed::Already { started: true });
    assert_eq!(
        systemd.sequence(),
        [
            format!("verify {LOCAL_URL}"),
            "is-active quoro-node.service".to_string(),
            "start quoro-node.service".to_string(),
        ]
    );
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

    let error = point_agent(
        LOCAL_URL,
        &agent(&scratch, &systemd, true),
        &systemd,
        &mut Out::quiet(),
    )
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
async fn an_agent_that_is_not_running_is_pointed_and_started() {
    let scratch = Scratch::new();
    enrolled(&scratch.0, "https://old.example.com:8443");
    let systemd = FakeSystemd::default();
    let mut out = Out::quiet();

    let pointed = point_agent(
        LOCAL_URL,
        &agent(&scratch, &systemd, false),
        &systemd,
        &mut out,
    )
    .await
    .unwrap();
    assert_eq!(
        pointed,
        Pointed::Changed {
            previous: "https://old.example.com:8443".to_string(),
            was_running: false,
        }
    );
    assert_eq!(
        systemd.sequence(),
        [
            format!("verify {LOCAL_URL}"),
            "is-active quoro-node.service".to_string(),
            "write identity".to_string(),
            "start quoro-node.service".to_string(),
        ]
    );
    assert_eq!(
        out.lines.last().unwrap(),
        "quoro-node.service was not active; started it"
    );
    assert_eq!(
        identity::load_identity(&scratch.0).unwrap().primary_url,
        LOCAL_URL
    );
}
