//! `quoro node rejoin` against a host laid out in a temporary directory, with
//! a fake systemd and the journal on a test socket.

use super::*;
use sqlx::sqlite::SqliteJournalMode;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// 2026-11-11T14:05:02Z.
fn fixed_now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_794_405_902)
}

const STAMP: &str = "20261111T140502Z";

/// The uid the control-plane database gets when the tests run as root, as
/// `quoro-api` would own it.
const OTHER_UID: u32 = 65_534;

/// Whether the tests run as root, which the owner assertions need. The
/// container gate sets `QUORO_TEST_AS_ROOT`, so there a run that is not
/// root's fails instead of skipping them.
fn is_root() -> bool {
    let root = fs::metadata("/proc/self").is_ok_and(|metadata| metadata.uid() == 0);
    assert!(
        root || std::env::var_os("QUORO_TEST_AS_ROOT").is_none(),
        "QUORO_TEST_AS_ROOT is set, but the tests do not run as root"
    );
    root
}

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A host in a temporary directory: `lib/` holds the control-plane data
/// directory `lib/quoro` and the agent's `lib/quoro-node`, `units/` is
/// `/etc/systemd/system`. The path stays short for the journal socket.
struct Host {
    root: PathBuf,
}

impl Drop for Host {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl Host {
    fn new() -> Self {
        let root = PathBuf::from(format!(
            "/tmp/qrj-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("lib/quoro-node")).unwrap();
        fs::create_dir_all(root.join("units")).unwrap();
        fs::create_dir(root.join("proc")).unwrap();
        fs::write(
            root.join("mountinfo"),
            "22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n",
        )
        .unwrap();
        fs::write(
            root.join("quoro.toml"),
            format!(
                "data_dir = \"{}\"\n\n[node]\ndata_dir = \"{}\"\nlocal = false\n",
                root.join("lib/quoro").display(),
                root.join("lib/quoro-node").display()
            ),
        )
        .unwrap();
        Self { root }
    }

    /// The host's `quoro.toml`, which the unit runs with.
    fn config(&self) -> PathBuf {
        self.root.join("quoro.toml")
    }

    fn parent(&self) -> PathBuf {
        self.root.join("lib")
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("lib/quoro")
    }

    fn node_dir(&self) -> PathBuf {
        self.root.join("lib/quoro-node")
    }

    fn unit_dir(&self) -> PathBuf {
        self.root.join("units")
    }

    fn unit_file(&self) -> PathBuf {
        self.unit_dir().join(UNIT)
    }

    fn drop_in(&self) -> PathBuf {
        self.unit_dir().join(DROP_IN)
    }

    fn socket(&self) -> PathBuf {
        self.root.join("j.sock")
    }

    fn options(&self) -> Options {
        Options {
            data_dir: self.data_dir(),
            node_data_dir: self.node_dir(),
            unit_dir: self.unit_dir(),
            journal_socket: self.socket(),
            mountinfo: self.root.join("mountinfo"),
            proc_dir: self.root.join("proc"),
            default_config: self.config(),
            euid: Some(0),
            now: fixed_now,
            command_line: "quoro node rejoin".to_string(),
            fs: Fs::REAL,
        }
    }

    /// The control-plane data directory with its database, whose `meta`
    /// holds `rows`, and a key file; owned by [`OTHER_UID`] as root.
    async fn control_plane(&self, rows: &[(&str, &str)]) {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(self.data_dir())
            .unwrap();
        create_database(&self.data_dir().join(DATABASE), "meta", rows).await;
        fs::create_dir(self.data_dir().join("keys")).unwrap();
        fs::write(self.data_dir().join("keys/signing.key"), b"secret").unwrap();
        give_to_other_user(&self.data_dir());
    }

    /// The agent's store, with `highest_term` when given.
    async fn agent(&self, highest: Option<&str>) {
        let rows: Vec<(&str, &str)> = highest.map(|h| ("highest_term", h)).into_iter().collect();
        create_database(&self.node_dir().join(AGENT_STORE), "node_meta", &rows).await;
        fs::write(self.node_dir().join("identity.json"), b"{}").unwrap();
    }

    fn unit(&self) {
        fs::write(
            self.unit_file(),
            "[Service]\nExecStart=/usr/local/bin/quoro serve\n",
        )
        .unwrap();
        fs::set_permissions(self.unit_file(), fs::Permissions::from_mode(0o644)).unwrap();
    }

    fn drop_in_dir(&self) {
        fs::create_dir(self.drop_in()).unwrap();
        fs::write(
            self.drop_in().join("override.conf"),
            "[Service]\nEnvironment=A=1\n",
        )
        .unwrap();
    }

    /// The moved-aside copies, by name.
    fn copies(&self) -> Vec<PathBuf> {
        let mut copies: Vec<PathBuf> = fs::read_dir(self.parent())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("quoro.demoted-t")
            })
            .collect();
        copies.sort();
        copies
    }

    /// Every path under the host, with its owner, group, mode and link
    /// target.
    fn listing(&self) -> BTreeMap<PathBuf, (u32, u32, u32, Option<PathBuf>)> {
        let mut listing = BTreeMap::new();
        walk(&self.root, &self.root, &mut listing);
        listing
    }
}

fn walk(
    root: &Path,
    dir: &Path,
    listing: &mut BTreeMap<PathBuf, (u32, u32, u32, Option<PathBuf>)>,
) {
    let metadata = fs::symlink_metadata(dir).unwrap();
    listing.insert(
        dir.strip_prefix(root).unwrap().to_path_buf(),
        (metadata.uid(), metadata.gid(), metadata.mode(), None),
    );
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let metadata = fs::symlink_metadata(&path).unwrap();
        if metadata.is_dir() {
            walk(root, &path, listing);
        } else {
            let link = metadata
                .file_type()
                .is_symlink()
                .then(|| fs::read_link(&path).unwrap());
            listing.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                (metadata.uid(), metadata.gid(), metadata.mode(), link),
            );
        }
    }
}

/// A WAL database with a key-value `table` holding `rows`, closed, so no
/// `-wal` or `-shm` is left.
async fn create_database(path: &Path, table: &str, rows: &[(&str, &str)]) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {table} (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    for (key, value) in rows {
        sqlx::query(&format!("INSERT INTO {table} (key, value) VALUES (?, ?)"))
            .bind(key)
            .bind(value)
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
    assert!(
        !exists(&side_file(path, "-wal")),
        "the close removed the WAL"
    );
}

/// Gives a tree to [`OTHER_UID`] when the tests run as root.
fn give_to_other_user(path: &Path) {
    if !is_root() {
        return;
    }
    lchown(path, Some(OTHER_UID), Some(OTHER_UID)).unwrap();
    if fs::symlink_metadata(path).unwrap().is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            give_to_other_user(&entry.unwrap().path());
        }
    }
}

/// systemd for one unit, `quoro.service`, whose file is the host's. Like
/// the real one, it refuses to stop a unit it does not know and to disable
/// one whose file is gone, and it forgets the unit at a daemon-reload once
/// the file is gone. Its `show` reports the installer's unit: `quoro serve`
/// without `--config`, and `QUORO_CONFIG` naming the host's `quoro.toml`.
struct FakeSystemd {
    unit_file: PathBuf,
    drop_in: PathBuf,
    data_dir: PathBuf,
    parent: PathBuf,
    loaded: Cell<bool>,
    active: Cell<bool>,
    fail_stop: bool,
    fail_disable: bool,
    disable_keeps_links: bool,
    stuck: bool,
    /// What `is-active` prints instead of `active` or `inactive`.
    state_text: Option<&'static str>,
    /// A directory `is-active` creates, as another process could between
    /// the checks and the data directory's rename.
    plant: Option<PathBuf>,
    /// `FragmentPath` instead of the host's unit file.
    fragment: Option<PathBuf>,
    /// Drop-ins `DropInPaths` lists after those in the host's drop-in
    /// directory.
    extra_drop_ins: Vec<PathBuf>,
    /// The `--config` of `ExecStart`.
    exec_config: Option<PathBuf>,
    /// The `QUORO_CONFIG` of `Environment`.
    env_config: Option<PathBuf>,
    /// Whether `quoro-node.service` is enabled and active.
    node_agent: bool,
    calls: RefCell<Vec<String>>,
    /// At each call: whether an unfinished copy exists, whether the data
    /// directory and the unit file are in place.
    seen: RefCell<Vec<(String, bool, bool, bool)>>,
}

impl FakeSystemd {
    fn new(host: &Host) -> Self {
        Self {
            unit_file: host.unit_file(),
            drop_in: host.drop_in(),
            data_dir: host.data_dir(),
            parent: host.parent(),
            loaded: Cell::new(exists(&host.unit_file())),
            active: Cell::new(true),
            fail_stop: false,
            fail_disable: false,
            disable_keeps_links: false,
            stuck: false,
            state_text: None,
            plant: None,
            fragment: None,
            extra_drop_ins: Vec::new(),
            exec_config: None,
            env_config: Some(host.config()),
            node_agent: true,
            calls: RefCell::new(Vec::new()),
            seen: RefCell::new(Vec::new()),
        }
    }

    fn call(&self, name: &str) {
        self.calls.borrow_mut().push(name.to_string());
        let copies = find_copies(&self.parent, "quoro").unwrap();
        self.seen.borrow_mut().push((
            name.to_string(),
            !copies.unfinished.is_empty(),
            exists(&self.data_dir),
            exists(&self.unit_file),
        ));
    }

    fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
}

impl Systemd for FakeSystemd {
    fn known(&self) -> bool {
        self.loaded.get() || exists(&self.unit_file)
    }

    fn stop(&self) -> Result<(), String> {
        self.call("stop");
        if self.fail_stop {
            return Err("Job for quoro.service failed".to_string());
        }
        if !self.known() {
            return Err("Unit quoro.service not loaded.".to_string());
        }
        self.active.set(false);
        Ok(())
    }

    fn disable(&self) -> Result<(), String> {
        self.call("disable");
        if self.fail_disable {
            return Err("Failed to disable unit: Access denied".to_string());
        }
        if !exists(&self.unit_file) {
            return Err("Unit file quoro.service does not exist.".to_string());
        }
        // A unit file that is a link out of the unit path is removed, as
        // `systemctl disable` undoes `systemctl link`.
        if !self.disable_keeps_links
            && fs::symlink_metadata(&self.unit_file).is_ok_and(|m| m.file_type().is_symlink())
        {
            fs::remove_file(&self.unit_file).unwrap();
        }
        Ok(())
    }

    fn state(&self) -> Result<String, String> {
        self.call("is-active");
        if let Some(path) = &self.plant {
            fs::create_dir(path).unwrap();
        }
        if let Some(state) = self.state_text {
            return Ok(state.to_string());
        }
        Ok(if self.stuck || self.active.get() {
            "active"
        } else {
            "inactive"
        }
        .to_string())
    }

    fn daemon_reload(&self) -> Result<(), String> {
        self.call("daemon-reload");
        self.loaded.set(exists(&self.unit_file));
        Ok(())
    }

    fn show(&self) -> Result<String, String> {
        let fragment = match &self.fragment {
            Some(path) => path.display().to_string(),
            None if self.known() => self.unit_file.display().to_string(),
            None => String::new(),
        };
        let mut drop_ins: Vec<PathBuf> = fs::read_dir(&self.drop_in)
            .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
            .unwrap_or_default();
        drop_ins.sort();
        drop_ins.extend(self.extra_drop_ins.iter().cloned());
        let drop_ins: Vec<String> = drop_ins
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        let argv = match &self.exec_config {
            Some(config) => format!("/usr/local/bin/quoro serve --config {}", config.display()),
            None => "/usr/local/bin/quoro serve".to_string(),
        };
        let environment = self
            .env_config
            .as_ref()
            .map(|config| format!("QUORO_CONFIG={}", config.display()))
            .unwrap_or_default();
        Ok(format!(
            "ExecStart={{ path=/usr/local/bin/quoro ; argv[]={argv} ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }}\n\
             Environment={environment}\nFragmentPath={fragment}\nDropInPaths={}\n",
            drop_ins.join(" ")
        ))
    }

    fn node_agent_runs(&self) -> bool {
        self.node_agent
    }
}

fn plain() -> Request {
    Request::default()
}

fn forced(statement: &str) -> Request {
    Request {
        config: None,
        statement: Some(statement.to_string()),
    }
}

async fn run(
    host: &Host,
    request: &Request,
    options: &Options,
    systemd: &FakeSystemd,
) -> (Result<Outcome, String>, Vec<String>) {
    let _ = host;
    let mut out = Out::quiet();
    let result = rejoin(request, options, systemd, &mut out).await;
    (result, out.lines)
}

/// A demoted host with a unit and a drop-in: the usual starting point.
async fn demoted_host() -> Host {
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
        .await;
    host.agent(Some("7")).await;
    host.unit();
    host.drop_in_dir();
    host
}

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|a| (*a).to_string()).collect()
}

fn cross_device(_: &Path, _: &Path) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::CrossesDevices))
}

fn denied_rename(_: &Path, _: &Path) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::PermissionDenied))
}

fn denied_removal(_: &Path) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::PermissionDenied))
}

fn deny_drop_in(from: &Path, to: &Path) -> io::Result<()> {
    if from.ends_with(DROP_IN) {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    } else {
        fs::rename(from, to)
    }
}

/// Asserts the finished layout of a copy of [`demoted_host`].
fn assert_finished(host: &Host, copy: &Path) {
    assert!(!exists(&host.data_dir()), "the data directory moved");
    assert!(!exists(&host.unit_file()), "the unit file moved");
    assert!(!exists(&host.drop_in()), "the drop-ins moved");
    assert!(copy.join("quoro").join(DATABASE).is_file());
    assert_eq!(
        fs::read(copy.join("quoro/keys/signing.key")).unwrap(),
        b"secret"
    );
    assert!(
        fs::read_to_string(copy.join("systemd").join(UNIT))
            .unwrap()
            .contains("quoro serve")
    );
    assert!(
        copy.join("systemd")
            .join(DROP_IN)
            .join("override.conf")
            .is_file()
    );
    assert!(copy.join(STARTED).is_file());
    assert!(copy.join(DONE).is_file());
    assert!(
        host.node_dir().join(AGENT_STORE).is_file(),
        "the agent's store stays"
    );
    assert!(host.node_dir().join("identity.json").is_file());
}

#[test]
fn the_flags_follow_the_rules() {
    assert_eq!(parse_args(&[]).unwrap(), Request::default());
    assert_eq!(
        parse_args(&args(&["--config", "/etc/q.toml"])).unwrap(),
        Request {
            config: Some(PathBuf::from("/etc/q.toml")),
            statement: None
        }
    );
    assert_eq!(
        parse_args(&args(&["--statement", "the promotion stopped", "--force"])).unwrap(),
        forced("the promotion stopped")
    );
    for (bad, expected) in [
        (&["--force"][..], "needs --statement"),
        (&["--statement", "why"][..], "needs --force"),
        (&["--force", "--statement", "  "][..], "non-empty"),
        (&["--force", "--statement"][..], "needs a value"),
        (&["--config"][..], "needs a value"),
        (&["--nope"][..], "unknown option"),
        (&["now"][..], "unexpected argument"),
    ] {
        let error = parse_args(&args(bad)).expect_err(expected);
        assert!(error.contains(expected), "{bad:?}: {error}");
    }
}

#[tokio::test]
async fn only_root_rejoins() {
    let host = demoted_host().await;
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        euid: Some(1000),
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.unwrap_err().contains("root"));
    let options = Options {
        euid: None,
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.unwrap_err().contains("root"));
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_recorded_demotion_lets_the_host_rejoin() {
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
        .await;
    host.agent(Some("5")).await;
    host.unit();
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert!(lines.iter().any(|l| l.contains("demoted_by_term 7")));
    assert!(copy.join(DONE).is_file());
}

#[tokio::test]
async fn an_agent_ahead_of_the_database_lets_the_host_rejoin() {
    let host = Host::new();
    host.control_plane(&[("term", "5")]).await;
    host.agent(Some("6")).await;
    host.unit();
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t6-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert!(lines.iter().any(|l| l.contains("term 6")), "{lines:?}");
    assert!(copy.join("quoro").join(DATABASE).is_file());
}

#[tokio::test]
async fn neither_reading_refuses_and_changes_nothing() {
    let host = Host::new();
    host.control_plane(&[("term", "5")]).await;
    host.agent(Some("5")).await;
    host.unit();
    host.drop_in_dir();
    if is_root() {
        let owner = fs::metadata(host.data_dir().join(DATABASE)).unwrap();
        assert_eq!(owner.uid(), OTHER_UID, "the database is not root's");
    }
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("nothing was changed"), "{error}");
    assert!(error.contains("demoted_by_term absent, term 5"), "{error}");
    assert!(error.contains("highest_term 5"), "{error}");
    assert!(error.contains("--force --statement"), "{error}");
    assert!(error.contains("--abort"), "{error}");
    assert_eq!(
        host.listing(),
        before,
        "names, owners and modes are the same"
    );
    assert!(systemd.calls().is_empty(), "nothing was stopped");
    assert!(host.copies().is_empty());
}

#[tokio::test]
async fn a_term_above_the_ceiling_counts_as_absent() {
    for (rows, highest) in [
        (
            vec![("term", "5"), ("demoted_by_term", "9007199254740992")],
            "5",
        ),
        (vec![("term", "5")], "9007199254740992"),
        (vec![("term", "5"), ("demoted_by_term", "seven")], "5"),
        (vec![("term", "9007199254740992")], "6"),
    ] {
        let host = Host::new();
        host.control_plane(&rows).await;
        host.agent(Some(highest)).await;
        host.unit();
        let before = host.listing();
        let systemd = FakeSystemd::new(&host);
        let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains("not a term, so absent"), "{error}");
        assert_eq!(host.listing(), before);
    }

    // The ceiling itself is a term.
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "9007199254740991")])
        .await;
    host.unit();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(
        result.unwrap(),
        Outcome::Rejoined(
            host.parent()
                .join(format!("quoro.demoted-t9007199254740991-{STAMP}"))
        )
    );
}

#[tokio::test]
async fn a_missing_term_keeps_the_agent_from_counting() {
    let host = Host::new();
    host.control_plane(&[]).await;
    host.agent(Some("9")).await;
    host.unit();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("term absent"), "{error}");
    assert!(error.contains("highest_term 9"), "{error}");
    assert_eq!(host.listing(), before);

    // No control-plane database at all, but a unit: nothing can hold.
    let host = Host::new();
    host.agent(Some("9")).await;
    host.unit();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("control-plane database: none"), "{error}");
    assert_eq!(host.listing(), before);
}

#[tokio::test]
async fn a_read_with_a_live_writer_sees_its_wal_and_leaves_side_files_to_the_owner() {
    let host = Host::new();
    host.control_plane(&[("term", "5")]).await;
    let database = host.data_dir().join(DATABASE);
    // A live control plane: a write that is only in the WAL.
    let options = SqliteConnectOptions::new()
        .filename(&database)
        .journal_mode(SqliteJournalMode::Wal);
    let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("INSERT INTO meta (key, value) VALUES ('demoted_by_term', '8')")
        .execute(&mut writer)
        .await
        .unwrap();
    assert!(exists(&side_file(&database, "-wal")));
    if is_root() {
        // SQLite itself gives the side files a root process creates to the
        // database's owner; make them root's, as a root-owned side file
        // left by any other path would be, and the read must give them
        // back.
        for suffix in ["-wal", "-shm"] {
            lchown(side_file(&database, suffix), Some(0), Some(0)).unwrap();
        }
        assert_eq!(fs::metadata(side_file(&database, "-wal")).unwrap().uid(), 0);
    }

    let (values, warnings) = read_keys(&database, "meta", &["demoted_by_term", "term"], no_hook)
        .await
        .unwrap();
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(values, vec![Some("8".to_string()), Some("5".to_string())]);
    let owner = fs::metadata(&database).unwrap();
    for suffix in ["-wal", "-shm"] {
        let side = fs::metadata(side_file(&database, suffix)).unwrap();
        assert_eq!(
            (side.uid(), side.gid()),
            (owner.uid(), owner.gid()),
            "{suffix} belongs to the database's owner"
        );
    }
    writer.close().await.unwrap();
}

/// Reads one native journal entry: `KEY=value` lines, or a key, a newline,
/// a little-endian length and the value.
fn journal_fields(datagram: &[u8]) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    let mut rest = datagram;
    while !rest.is_empty() {
        let end = rest.iter().position(|b| *b == b'=' || *b == b'\n').unwrap();
        let key = String::from_utf8(rest[..end].to_vec()).unwrap();
        if rest[end] == b'=' {
            let line_end = end + 1 + rest[end + 1..].iter().position(|b| *b == b'\n').unwrap();
            fields.insert(
                key,
                String::from_utf8(rest[end + 1..line_end].to_vec()).unwrap(),
            );
            rest = &rest[line_end + 1..];
        } else {
            let length = usize::try_from(u64::from_le_bytes(
                rest[end + 1..end + 9].try_into().unwrap(),
            ))
            .unwrap();
            let value = &rest[end + 9..end + 9 + length];
            fields.insert(key, String::from_utf8(value.to_vec()).unwrap());
            assert_eq!(rest[end + 9 + length], b'\n');
            rest = &rest[end + 10 + length..];
        }
    }
    fields
}

fn receive(socket: &UnixDatagram) -> Option<BTreeMap<String, String>> {
    let mut buffer = vec![0_u8; 65_536];
    socket.set_nonblocking(true).unwrap();
    match socket.recv(&mut buffer) {
        Ok(length) => Some(journal_fields(&buffer[..length])),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => None,
        Err(e) => panic!("{e}"),
    }
}

#[tokio::test]
async fn a_forced_run_records_the_statement() {
    let host = Host::new();
    host.control_plane(&[("term", "4")]).await;
    host.agent(Some("4")).await;
    host.unit();
    let journal = UnixDatagram::bind(host.socket()).unwrap();
    let systemd = FakeSystemd::new(&host);
    let statement = "the promotion on this host stopped after its database\nand was aborted";
    let (result, _) = run(&host, &forced(statement), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t4-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));

    let fields = receive(&journal).expect("one journal entry");
    assert!(fields["MESSAGE"].contains(statement), "{fields:?}");
    assert!(fields["MESSAGE"].contains("2026-11-11T14:05:02Z"));
    assert_eq!(fields["OBJECT_SYSTEMD_UNIT"], "quoro-node.service");
    assert_eq!(fields["SYSLOG_IDENTIFIER"], "quoro");
    assert_eq!(fields["PRIORITY"], "5");
    assert!(receive(&journal).is_none(), "one entry only");

    let recorded = fs::read_to_string(copy.join(STATEMENT)).unwrap();
    assert!(recorded.contains(statement));
    assert!(recorded.contains("2026-11-11T14:05:02Z"));
    let started = fs::read_to_string(copy.join(STARTED)).unwrap();
    assert!(
        !started.contains("aborted"),
        "rejoin-started leaves out the statement"
    );
    assert!(copy.join(DONE).is_file());
}

#[tokio::test]
async fn a_forced_run_refuses_when_the_journal_cannot_be_written() {
    let host = Host::new();
    host.control_plane(&[("term", "4")]).await;
    host.unit();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    // No socket is bound.
    let (result, _) = run(&host, &forced("why"), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("system journal"), "{error}");
    assert!(error.contains("nothing was changed"), "{error}");
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_forced_run_without_a_readable_term_names_the_copy_t0() {
    let host = Host::new();
    fs::create_dir(host.data_dir()).unwrap();
    fs::write(host.data_dir().join(DATABASE), b"not a database at all").unwrap();
    host.unit();
    let _journal = UnixDatagram::bind(host.socket()).unwrap();

    // Unforced, an unreadable database refuses.
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(result.unwrap_err().contains("unreadable"));

    let (result, lines) = run(&host, &forced("why"), &host.options(), &systemd).await;
    assert_eq!(
        result.unwrap(),
        Outcome::Rejoined(host.parent().join(format!("quoro.demoted-t0-{STAMP}")))
    );
    assert!(lines.iter().any(|l| l.contains("t0")), "{lines:?}");
}

#[tokio::test]
async fn a_full_run_moves_everything_in_order_and_deletes_nothing() {
    let host = demoted_host().await;
    let node_before: BTreeMap<_, _> = host
        .listing()
        .into_iter()
        .filter(|(path, _)| path.starts_with("lib/quoro-node"))
        .collect();
    let files_before = host
        .listing()
        .values()
        .filter(|(_, _, mode, _)| mode & 0o170_000 != 0o040_000)
        .count();
    let database_owner = fs::metadata(host.data_dir().join(DATABASE)).unwrap().uid();
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert_finished(&host, &copy);

    assert_eq!(
        systemd.calls(),
        ["stop", "disable", "is-active", "daemon-reload"]
    );
    let seen = systemd.seen.borrow().clone();
    assert_eq!(
        seen[0],
        ("stop".to_string(), true, true, true),
        "rejoin-started is written before the stop, and nothing has moved"
    );
    assert_eq!(
        seen[3],
        ("daemon-reload".to_string(), true, false, false),
        "everything moved before the reload, and rejoin-done comes after it"
    );

    assert_eq!(fs::symlink_metadata(&copy).unwrap().mode() & 0o777, 0o700);
    if is_root() {
        assert_eq!(fs::symlink_metadata(&copy).unwrap().uid(), 0);
    }
    assert_eq!(
        fs::metadata(copy.join("quoro").join(DATABASE))
            .unwrap()
            .uid(),
        database_owner,
        "a rename keeps the owner"
    );
    let started = fs::read_to_string(copy.join(STARTED)).unwrap();
    assert!(started.contains("quoro node rejoin") && started.contains("2026-11-11T14:05:02Z"));
    assert!(!copy.join(STATEMENT).exists());

    let node_after: BTreeMap<_, _> = host
        .listing()
        .into_iter()
        .filter(|(path, _)| path.starts_with("lib/quoro-node"))
        .collect();
    assert_eq!(
        node_after, node_before,
        "the agent's directory is untouched"
    );
    let files_after = host
        .listing()
        .values()
        .filter(|(_, _, mode, _)| mode & 0o170_000 != 0o040_000)
        .count();
    assert_eq!(
        files_after,
        files_before + 2,
        "nothing deleted; the two markers added"
    );
    assert!(lines.iter().any(|l| l.contains("is a node again")));
    assert!(lines.iter().any(|l| l.starts_with("moved ")));
}

#[tokio::test]
async fn a_failed_stop_moves_nothing_and_a_rerun_finishes() {
    let host = demoted_host().await;
    let systemd = FakeSystemd {
        fail_stop: true,
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("cannot stop"), "{error}");
    assert!(error.contains("rejoin-started"), "{error}");
    let copies = host.copies();
    assert_eq!(copies.len(), 1);
    assert!(copies[0].join(STARTED).is_file());
    assert!(!copies[0].join(DONE).exists());
    assert_eq!(sorted_names(&copies[0]).unwrap(), [STARTED]);
    assert!(host.data_dir().is_dir() && host.unit_file().is_file());
    assert_eq!(systemd.calls(), ["stop"]);

    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copies[0].clone()));
    assert!(lines.iter().any(|l| l.contains("finishing")), "{lines:?}");
    assert_eq!(host.copies(), copies, "the same copy, no second one");
    assert_finished(&host, &copies[0]);
}

#[tokio::test]
async fn a_failed_disable_moves_nothing_and_a_rerun_finishes() {
    let host = demoted_host().await;
    let systemd = FakeSystemd {
        fail_disable: true,
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(result.unwrap_err().contains("cannot disable"));
    assert_eq!(systemd.calls(), ["stop", "disable"]);
    let copies = host.copies();
    assert_eq!(sorted_names(&copies[0]).unwrap(), [STARTED]);
    assert!(host.data_dir().is_dir() && host.unit_file().is_file());

    // The unit stays stopped; a rerun stops it again, which is harmless.
    let systemd = FakeSystemd {
        active: Cell::new(false),
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copies[0].clone()));
    assert_eq!(
        systemd.calls(),
        ["stop", "disable", "is-active", "daemon-reload"]
    );
    assert_finished(&host, &copies[0]);
}

#[tokio::test]
async fn a_unit_still_active_moves_nothing() {
    let host = demoted_host().await;
    let systemd = FakeSystemd {
        stuck: true,
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("reads active"), "{error}");
    assert_eq!(sorted_names(&host.copies()[0]).unwrap(), [STARTED]);
    assert!(host.data_dir().is_dir() && host.unit_file().is_file());
}

/// #881 point 1.
#[tokio::test]
async fn only_inactive_or_failed_counts_as_stopped() {
    for state in ["refreshing", "maintenance", "unknown"] {
        let host = demoted_host().await;
        let systemd = FakeSystemd {
            state_text: Some(state),
            ..FakeSystemd::new(&host)
        };
        let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains(&format!("reads {state}")), "{error}");
        assert_eq!(
            sorted_names(&host.copies()[0]).unwrap(),
            [STARTED],
            "{state}: nothing moved"
        );
        assert!(host.data_dir().is_dir() && host.unit_file().is_file());
    }
    for state in ["inactive", "failed"] {
        let host = demoted_host().await;
        let systemd = FakeSystemd {
            state_text: Some(state),
            ..FakeSystemd::new(&host)
        };
        let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
        assert!(result.is_ok(), "{state}: {result:?}");
        assert!(
            lines.iter().any(|l| l.contains(&format!("({state})"))),
            "{lines:?}"
        );
    }
}

#[tokio::test]
async fn an_interruption_after_the_data_move_finishes_on_a_rerun() {
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: denied_rename,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.unwrap_err().contains("cannot move"));
    let copy = host.copies()[0].clone();
    assert!(!host.data_dir().exists(), "the data moved");
    assert!(copy.join("quoro").join(DATABASE).is_file());
    assert!(host.unit_file().is_file(), "the unit file did not");

    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    let in_copy = copy.join("quoro").join(DATABASE);
    assert!(
        lines
            .iter()
            .any(|l| l.contains(&in_copy.display().to_string())),
        "the check read the database in the copy: {lines:?}"
    );
    assert_finished(&host, &copy);
}

#[tokio::test]
async fn an_interruption_after_the_unit_file_move_finishes_without_disabling_again() {
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: deny_drop_in,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.unwrap_err().contains("cannot move"));
    let copy = host.copies()[0].clone();
    assert!(!host.unit_file().exists());
    assert!(host.drop_in().is_dir());

    // systemd still has the unit loaded: no daemon-reload ran.
    let systemd = FakeSystemd {
        loaded: Cell::new(true),
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert_eq!(systemd.calls(), ["stop", "is-active", "daemon-reload"]);
    assert_finished(&host, &copy);
}

#[tokio::test]
async fn an_interruption_after_a_cross_device_copy_finishes_on_a_rerun() {
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: cross_device,
            remove_file: denied_removal,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.unwrap_err().contains("cannot remove"));
    let copy = host.copies()[0].clone();
    let target = copy.join("systemd").join(UNIT);
    assert!(target.is_file() && host.unit_file().is_file(), "both exist");

    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert!(
        lines.iter().any(|l| l.contains("removed the source")),
        "{lines:?}"
    );
    assert_finished(&host, &copy);
}

#[tokio::test]
async fn a_cross_device_copy_that_differs_refuses() {
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: cross_device,
            remove_file: denied_removal,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.is_err());
    fs::write(host.unit_file(), "[Service]\nExecStart=/edited\n").unwrap();
    let before = host.listing();

    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    let copy = host.copies()[0].clone();
    assert!(
        error.contains(&host.unit_file().display().to_string())
            && error.contains(&copy.join("systemd").join(UNIT).display().to_string()),
        "{error}"
    );
    assert!(error.contains("differ"), "{error}");
    assert_eq!(host.listing(), before, "nothing is overwritten");
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_rerun_of_a_forced_copy_needs_no_force() {
    let host = Host::new();
    host.control_plane(&[("term", "4")]).await;
    host.agent(Some("4")).await;
    host.unit();
    let journal = UnixDatagram::bind(host.socket()).unwrap();
    let systemd = FakeSystemd {
        fail_stop: true,
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &forced("first"), &host.options(), &systemd).await;
    assert!(result.is_err());
    assert!(receive(&journal).is_some());
    let copy = host.copies()[0].clone();
    let statement = fs::read_to_string(copy.join(STATEMENT)).unwrap();

    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert!(
        lines.iter().any(|l| l.contains("earlier forced run")),
        "{lines:?}"
    );
    assert_eq!(fs::read_to_string(copy.join(STATEMENT)).unwrap(), statement);
    assert!(receive(&journal).is_none(), "nothing more in the journal");
}

#[tokio::test]
async fn a_forced_rerun_never_overwrites_the_earlier_statement() {
    let host = Host::new();
    host.control_plane(&[("term", "4")]).await;
    host.unit();
    let journal = UnixDatagram::bind(host.socket()).unwrap();
    let systemd = FakeSystemd {
        fail_stop: true,
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &forced("first"), &host.options(), &systemd).await;
    assert!(result.is_err());
    let _ = receive(&journal);
    let copy = host.copies()[0].clone();

    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &forced("second"), &host.options(), &systemd).await;
    assert!(result.is_ok());
    let statement = fs::read_to_string(copy.join(STATEMENT)).unwrap();
    assert!(statement.contains("first") && !statement.contains("second"));
    assert!(receive(&journal).is_none());
}

#[tokio::test]
async fn two_unfinished_copies_refuse() {
    let host = demoted_host().await;
    for name in [
        "quoro.demoted-t3-20261101T000000Z",
        "quoro.demoted-t4-20261102T000000Z",
    ] {
        let copy = host.parent().join(name);
        fs::create_dir(&copy).unwrap();
        fs::write(copy.join(STARTED), "started").unwrap();
    }
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(
        error.contains("quoro.demoted-t3-20261101T000000Z")
            && error.contains("quoro.demoted-t4-20261102T000000Z"),
        "{error}"
    );
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_copy_without_rejoin_started_or_with_rejoin_done_is_never_adopted() {
    let host = demoted_host().await;
    let bare = host.parent().join("quoro.demoted-t3-20261101T000000Z");
    fs::create_dir(&bare).unwrap();
    let finished = host.parent().join("quoro.demoted-t4-20261102T000000Z");
    fs::create_dir(&finished).unwrap();
    fs::write(finished.join(STARTED), "started").unwrap();
    fs::write(finished.join(DONE), "done").unwrap();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert_eq!(sorted_names(&bare).unwrap().len(), 0);
    assert_eq!(sorted_names(&finished).unwrap().len(), 2);
    assert_finished(&host, &copy);
}

#[tokio::test]
async fn a_cross_device_move_keeps_modes_owners_and_links() {
    let host = demoted_host().await;
    fs::set_permissions(host.unit_file(), fs::Permissions::from_mode(0o640)).unwrap();
    symlink("../shared.conf", host.drop_in().join("link.conf")).unwrap();
    fs::set_permissions(host.drop_in(), fs::Permissions::from_mode(0o750)).unwrap();
    give_to_other_user(&host.unit_file());
    let unit_owner = fs::symlink_metadata(host.unit_file()).unwrap().uid();
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: cross_device,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert_finished(&host, &copy);
    let unit = fs::symlink_metadata(copy.join("systemd").join(UNIT)).unwrap();
    assert_eq!(unit.mode() & 0o7777, 0o640);
    assert_eq!(unit.uid(), unit_owner);
    let drop_in = copy.join("systemd").join(DROP_IN);
    assert_eq!(
        fs::symlink_metadata(&drop_in).unwrap().mode() & 0o7777,
        0o750
    );
    assert_eq!(
        fs::read_link(drop_in.join("link.conf")).unwrap(),
        PathBuf::from("../shared.conf"),
        "a link stays a link"
    );
    let leftovers: Vec<_> = sorted_names(&copy.join("systemd"))
        .unwrap()
        .into_iter()
        .filter(|name| name.to_string_lossy().contains("partial"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn a_unit_file_that_is_a_symlink_moves_as_a_link() {
    for rename in [Fs::REAL.rename, cross_device] {
        let host = Host::new();
        host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
            .await;
        let real = host.root.join("quoro.service.real");
        fs::write(&real, "[Service]\nExecStart=/usr/local/bin/quoro serve\n").unwrap();
        symlink(&real, host.unit_file()).unwrap();
        let systemd = FakeSystemd::new(&host);
        let options = Options {
            fs: Fs { rename, ..Fs::REAL },
            ..host.options()
        };
        let (result, _) = run(&host, &plain(), &options, &systemd).await;
        let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
        assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
        let moved = copy.join("systemd").join(UNIT);
        assert!(
            fs::symlink_metadata(&moved)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_link(&moved).unwrap(), real);
        assert!(real.is_file(), "the link's target stays where it was");
        assert!(!exists(&host.unit_file()));
        assert!(copy.join(DONE).is_file());
    }

    // The disable left the link (a link inside the unit path, say): the
    // copy made before it is identical, so the source is removed.
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
        .await;
    fs::write(host.unit_dir().join("quoro.service.real"), "[Service]\n").unwrap();
    symlink("quoro.service.real", host.unit_file()).unwrap();
    let systemd = FakeSystemd {
        disable_keeps_links: true,
        ..FakeSystemd::new(&host)
    };
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    let moved = copy.join("systemd").join(UNIT);
    assert_eq!(
        fs::read_link(&moved).unwrap(),
        PathBuf::from("quoro.service.real")
    );
    assert!(!exists(&host.unit_file()));
    assert!(
        lines.iter().any(|l| l.contains("removed the source")),
        "{lines:?}"
    );
}

#[tokio::test]
async fn nothing_to_do_and_already_rejoined() {
    let host = Host::new();
    host.agent(Some("9")).await;
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::NothingToDo);
    assert!(lines[0].contains("no control plane to move aside"));
    assert!(systemd.calls().is_empty());

    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let Outcome::Rejoined(copy) = result.unwrap() else {
        panic!("a demoted host rejoins");
    };
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::AlreadyRejoined(copy.clone()));
    assert!(lines[0].contains("already rejoined"));
    assert!(lines[0].contains(&copy.display().to_string()));
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_data_directory_in_both_places_refuses() {
    let host = demoted_host().await;
    let copy = host.parent().join("quoro.demoted-t7-20261101T000000Z");
    fs::create_dir_all(copy.join("quoro")).unwrap();
    fs::write(copy.join(STARTED), "started").unwrap();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(
        error.contains(&host.data_dir().display().to_string())
            && error.contains(&copy.join("quoro").display().to_string()),
        "{error}"
    );
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[tokio::test]
async fn a_mount_point_refuses() {
    let host = demoted_host().await;
    let canonical = fs::canonicalize(host.data_dir()).unwrap();
    fs::write(
        host.root.join("mountinfo"),
        format!(
            "22 1 8:1 / / rw - ext4 /dev/sda1 rw\n40 22 8:2 / {} rw - ext4 /dev/sdb1 rw\n",
            canonical.display().to_string().replace(' ', "\\040")
        ),
    )
    .unwrap();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("mount point"), "{error}");
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());
}

#[test]
fn times_are_utc() {
    assert_eq!(compact_utc(UNIX_EPOCH), "19700101T000000Z");
    assert_eq!(compact_utc(fixed_now()), STAMP);
    let leap = UNIX_EPOCH + Duration::from_secs(1_709_251_199);
    assert_eq!(rfc3339(leap), "2024-02-29T23:59:59Z");
    assert_eq!(unescape_mount("/a\\040b"), "/a b");
}

#[tokio::test]
async fn a_side_file_that_is_a_link_or_has_two_links_is_left_alone() {
    if !is_root() {
        eprintln!("skipped: the owner change needs root");
        return;
    }
    for hard in [false, true] {
        let host = Host::new();
        host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
            .await;
        let database = host.data_dir().join(DATABASE);
        let target = host.root.join("elsewhere");
        fs::write(&target, b"root's file").unwrap();
        lchown(&target, Some(0), Some(0)).unwrap();
        if hard {
            fs::hard_link(&target, side_file(&database, "-shm")).unwrap();
        } else {
            symlink(&target, side_file(&database, "-shm")).unwrap();
        }
        let (values, warnings) = read_keys(&database, "meta", &["demoted_by_term"], no_hook)
            .await
            .unwrap();
        assert_eq!(values, vec![Some("7".to_string())]);
        let after = fs::metadata(&target).unwrap();
        assert_eq!(
            (after.uid(), after.gid()),
            (0, 0),
            "the target keeps its owner (hard link: {hard})"
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("left"), "{warnings:?}");

        // The whole command says so, and goes on.
        let systemd = FakeSystemd::new(&host);
        host.unit();
        let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
        assert!(result.is_ok(), "{result:?}");
        assert!(
            lines.iter().any(|l| l.starts_with("warning: left")),
            "{lines:?}"
        );
        assert_eq!(fs::metadata(&target).unwrap().uid(), 0);
    }
}

/// Moves the data directory to `lib/real` and leaves a symbolic link to it
/// at `lib/quoro`.
fn link_data_dir(host: &Host) {
    fs::rename(host.data_dir(), host.parent().join("real")).unwrap();
    symlink("real", host.data_dir()).unwrap();
}

#[tokio::test]
async fn a_symlinked_data_directory_is_refused_with_or_without_a_trailing_slash() {
    for slash in [false, true] {
        let host = demoted_host().await;
        link_data_dir(&host);
        let before = host.listing();
        let systemd = FakeSystemd::new(&host);
        let mut data_dir = host.data_dir().into_os_string();
        if slash {
            data_dir.push("/");
        }
        let options = Options {
            data_dir: PathBuf::from(data_dir),
            ..host.options()
        };
        let (result, _) = run(&host, &plain(), &options, &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains("not a directory"), "{error}");
        assert!(systemd.calls().is_empty(), "nothing was stopped");
        assert_eq!(host.listing(), before);
    }
}

#[tokio::test]
async fn a_relative_or_parent_data_directory_is_refused() {
    let host = demoted_host().await;
    let before = host.listing();
    for (data_dir, expected) in [
        (PathBuf::from("lib/quoro"), "not an absolute path"),
        (host.parent().join("x/../quoro"), "'..'"),
        (PathBuf::from("/"), "no file name"),
    ] {
        let systemd = FakeSystemd::new(&host);
        let options = Options {
            data_dir,
            ..host.options()
        };
        let (result, _) = run(&host, &plain(), &options, &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains(expected), "{error}");
        assert!(systemd.calls().is_empty());
    }
    assert_eq!(host.listing(), before);
}

fn data_dir_elsewhere(path: &Path) -> io::Result<u64> {
    let device = fs::metadata(path)?.dev();
    Ok(if path.ends_with("quoro") {
        device + 1
    } else {
        device
    })
}

#[tokio::test]
async fn a_data_directory_on_another_filesystem_is_refused() {
    let host = demoted_host().await;
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            device: data_dir_elsewhere,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("another filesystem"), "{error}");
    assert!(systemd.calls().is_empty());
    assert_eq!(host.listing(), before);
}

#[tokio::test]
async fn a_node_data_directory_inside_the_data_directory_is_refused() {
    let host = demoted_host().await;
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        node_data_dir: host.data_dir().join("node"),
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("is inside"), "{error}");
    assert!(systemd.calls().is_empty());
    assert_eq!(host.listing(), before);
}

#[tokio::test]
async fn a_taken_copy_name_is_refused() {
    let host = demoted_host().await;
    // Not adopted: it has no rejoin-started.
    fs::create_dir(host.parent().join(format!("quoro.demoted-t7-{STAMP}"))).unwrap();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("already exists"), "{error}");
    assert!(systemd.calls().is_empty());
    assert_eq!(host.listing(), before);
}

/// Makes the `-wal` that the test prepared next to `database` appear.
fn wal_appears(database: &Path) {
    let pending = side_file(database, ".pending-wal");
    if exists(&pending) {
        fs::rename(&pending, side_file(database, "-wal")).unwrap();
    }
}

#[tokio::test]
async fn a_wal_that_appears_after_the_immutable_read_is_read() {
    let host = Host::new();
    // A database whose demotion is only in its WAL: the main file and the
    // WAL are copied while the writer holds them.
    let scratch = host.root.join("scratch");
    fs::create_dir(&scratch).unwrap();
    let source = scratch.join(DATABASE);
    create_database(&source, "meta", &[("term", "5")]).await;
    let options = SqliteConnectOptions::new()
        .filename(&source)
        .journal_mode(SqliteJournalMode::Wal);
    let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("INSERT INTO meta (key, value) VALUES ('demoted_by_term', '8')")
        .execute(&mut writer)
        .await
        .unwrap();
    fs::create_dir(host.data_dir()).unwrap();
    let database = host.data_dir().join(DATABASE);
    fs::copy(&source, &database).unwrap();
    fs::copy(
        side_file(&source, "-wal"),
        side_file(&database, ".pending-wal"),
    )
    .unwrap();
    writer.close().await.unwrap();

    let (values, _) = read_keys(&database, "meta", &["demoted_by_term", "term"], no_hook)
        .await
        .unwrap();
    assert_eq!(values[0], None, "the main file alone has no demotion");
    let (values, _) = read_keys(&database, "meta", &["demoted_by_term", "term"], wal_appears)
        .await
        .unwrap();
    assert_eq!(
        values,
        vec![Some("8".to_string()), Some("5".to_string())],
        "the -wal that appeared is read"
    );
}

#[tokio::test]
async fn a_left_temporary_link_does_not_block_a_rerun() {
    for identical in [true, false] {
        let host = Host::new();
        host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
            .await;
        let real = host.root.join("quoro.service.real");
        fs::write(&real, "[Service]\n").unwrap();
        symlink(&real, host.unit_file()).unwrap();
        // An interruption after rejoin-started...
        let systemd = FakeSystemd {
            fail_stop: true,
            ..FakeSystemd::new(&host)
        };
        let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
        assert!(result.is_err());
        let copy = host.copies()[0].clone();
        // ...in a later run, after the stop and the copy of the link to its
        // temporary name, before the rename into place.
        fs::create_dir(copy.join("systemd")).unwrap();
        let temporary = copy.join("systemd").join(format!(".{UNIT}.partial-link"));
        let left = if identical {
            real.clone()
        } else {
            host.root.join("other")
        };
        symlink(&left, &temporary).unwrap();

        let systemd = FakeSystemd::new(&host);
        let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
        assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
        let moved = copy.join("systemd").join(UNIT);
        assert_eq!(fs::read_link(&moved).unwrap(), real);
        assert!(copy.join(DONE).is_file());
        if identical {
            assert!(!exists(&temporary), "the identical one was reused");
        } else {
            assert_eq!(
                fs::read_link(&temporary).unwrap(),
                left,
                "the one that differs is kept"
            );
        }
    }
}

#[tokio::test]
async fn an_agent_without_its_own_running_unit_is_warned_about_and_the_run_finishes() {
    let host = demoted_host().await;
    let systemd = FakeSystemd {
        node_agent: false,
        ..FakeSystemd::new(&host)
    };
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    assert_finished(&host, &copy);
    let warning = lines
        .iter()
        .position(|l| l.starts_with("warning: ") && l.contains("not enabled and active"))
        .unwrap_or_else(|| panic!("a warning: {lines:?}"));
    assert!(lines[warning].contains("stops that agent"), "{lines:?}");
    assert!(lines[warning].contains("local = true"), "{lines:?}");
    assert!(
        lines[warning].contains("systemctl enable --now quoro-node.service"),
        "{lines:?}"
    );
    let created = lines
        .iter()
        .position(|l| l.starts_with("created "))
        .unwrap();
    assert!(
        warning < created,
        "the warning comes before any change: {lines:?}"
    );

    // With quoro-node.service enabled and active, the agent runs on its own.
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(
        !lines.iter().any(|l| l.starts_with("warning: ")),
        "{lines:?}"
    );

    // Without a unit there is nothing to stop, and no warning.
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
        .await;
    let systemd = FakeSystemd {
        node_agent: false,
        ..FakeSystemd::new(&host)
    };
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(
        !lines.iter().any(|l| l.contains("not enabled and active")),
        "{lines:?}"
    );
}

#[test]
fn what_systemctl_show_prints_is_read() {
    let report = parse_show(
        "ExecStart={ path=/usr/local/bin/quoro ; argv[]=/usr/local/bin/quoro serve --config \"/etc/quoro/a b.toml\" ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }\n\
         Environment=\"QUORO_CONFIG=/etc/quoro/a b.toml\" RUST_LOG=info\n\
         FragmentPath=/etc/systemd/system/quoro.service\n\
         DropInPaths=/etc/systemd/system/quoro.service.d/override.conf /run/systemd/system/service.d/zzz-lxc-service.conf\n",
    );
    assert_eq!(
        report.fragment,
        Some(PathBuf::from("/etc/systemd/system/quoro.service"))
    );
    assert_eq!(
        report.drop_ins,
        [
            PathBuf::from("/etc/systemd/system/quoro.service.d/override.conf"),
            PathBuf::from("/run/systemd/system/service.d/zzz-lxc-service.conf"),
        ]
    );
    assert_eq!(
        report.environment,
        ["QUORO_CONFIG=/etc/quoro/a b.toml", "RUST_LOG=info"]
    );
    assert_eq!(
        report.exec_start,
        [vec![
            "/usr/local/bin/quoro",
            "serve",
            "--config",
            "/etc/quoro/a b.toml"
        ]]
    );
    assert_eq!(
        parse_show("FragmentPath=\nDropInPaths=\nEnvironment=\nExecStart=\n"),
        UnitReport::default()
    );
}

/// A configuration whose data directory is not the host's.
fn other_config(host: &Host) -> PathBuf {
    let path = host.root.join("other.toml");
    fs::write(
        &path,
        format!("data_dir = \"{}\"\n", host.root.join("elsewhere").display()),
    )
    .unwrap();
    path
}

/// #881 point 2.
#[tokio::test]
async fn a_unit_file_drop_in_or_configuration_left_behind_refuses() {
    for case in ["fragment", "drop-in", "QUORO_CONFIG", "--config", "default"] {
        let host = demoted_host().await;
        let mut systemd = FakeSystemd::new(&host);
        let mut options = host.options();
        let expected = match case {
            "fragment" => {
                let vendor = host.root.join("vendor");
                fs::create_dir(&vendor).unwrap();
                fs::write(vendor.join(UNIT), "[Service]\n").unwrap();
                systemd.fragment = Some(vendor.join(UNIT));
                format!("systemd loads {UNIT} from {}", vendor.join(UNIT).display())
            }
            "drop-in" => {
                let runtime = host.root.join("run").join(DROP_IN);
                fs::create_dir_all(&runtime).unwrap();
                fs::write(runtime.join("50-x.conf"), "[Service]\n").unwrap();
                systemd.extra_drop_ins.push(runtime.join("50-x.conf"));
                format!("drop-in {}", runtime.join("50-x.conf").display())
            }
            "QUORO_CONFIG" => {
                systemd.env_config = Some(other_config(&host));
                "its QUORO_CONFIG".to_string()
            }
            "--config" => {
                systemd.exec_config = Some(other_config(&host));
                "the --config of its ExecStart".to_string()
            }
            _ => {
                options.default_config = other_config(&host);
                "reads without --config".to_string()
            }
        };
        let before = host.listing();
        let (result, _) = run(&host, &plain(), &options, &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains(&expected), "{case}: {error}");
        assert!(error.contains("nothing was changed"), "{case}: {error}");
        assert_eq!(host.listing(), before, "{case}");
        assert!(systemd.calls().is_empty(), "{case}: nothing was stopped");
    }

    // A drop-in for every service is not the unit's own, and a --config
    // that names the host's configuration is the right one: the run goes on.
    let host = demoted_host().await;
    let every_service = host.root.join("run/service.d");
    fs::create_dir_all(&every_service).unwrap();
    fs::write(every_service.join("zzz-lxc-service.conf"), "[Service]\n").unwrap();
    let systemd = FakeSystemd {
        extra_drop_ins: vec![every_service.join("zzz-lxc-service.conf")],
        exec_config: Some(host.config()),
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(result.is_ok(), "{result:?}");
}

/// #881 point 3.
#[tokio::test]
async fn a_database_held_open_without_a_unit_refuses() {
    let host = Host::new();
    host.control_plane(&[("term", "5"), ("demoted_by_term", "7")])
        .await;
    host.agent(Some("7")).await;
    // A quoro serve started by hand holds the database.
    let descriptors = host.root.join("proc/4242/fd");
    fs::create_dir_all(&descriptors).unwrap();
    symlink(host.data_dir().join(DATABASE), descriptors.join("7")).unwrap();
    fs::write(host.root.join("proc/4242/comm"), "quoro\n").unwrap();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("4242 (quoro)"), "{error}");
    assert!(error.contains("Nothing was changed"), "{error}");
    assert_eq!(host.listing(), before);
    assert!(host.copies().is_empty());

    // Once it is gone, the run goes on.
    fs::remove_file(descriptors.join("7")).unwrap();
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(matches!(result, Ok(Outcome::Rejoined(_))), "{result:?}");

    // The real /proc: a process holding a file is found through its
    // descriptor's link.
    let held = host.root.join("held");
    fs::write(&held, b"x").unwrap();
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .stdin(fs::File::open(&held).unwrap())
        .spawn()
        .unwrap();
    let found = holders(Path::new("/proc"), std::slice::from_ref(&held));
    child.kill().unwrap();
    child.wait().unwrap();
    let found = found.unwrap();
    assert!(
        found
            .iter()
            .any(|(pid, name)| *pid == child.id() && name == "sleep"),
        "{found:?}"
    );
}

/// #881 point 4: the lock.
#[tokio::test]
async fn a_second_run_is_refused_while_the_first_holds_the_lock() {
    let host = demoted_host().await;
    let first = fs::File::open(host.parent()).unwrap();
    rustix::fs::flock(&first, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    let before = host.listing();
    let systemd = FakeSystemd::new(&host);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("holds the lock"), "{error}");
    assert_eq!(host.listing(), before);
    assert!(systemd.calls().is_empty());

    drop(first);
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert!(matches!(result, Ok(Outcome::Rejoined(_))), "{result:?}");
}

/// #881 point 4: the renames.
#[tokio::test]
async fn no_rename_replaces_a_target_that_appeared() {
    // An empty directory that appears where the data directory goes: a plain
    // rename would replace it.
    let host = demoted_host().await;
    let copy = host.parent().join(format!("quoro.demoted-t7-{STAMP}"));
    let systemd = FakeSystemd {
        plant: Some(copy.join("quoro")),
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    let error = result.unwrap_err();
    assert!(error.contains("cannot rename"), "{error}");
    assert!(
        host.data_dir().join(DATABASE).is_file(),
        "the data directory stays"
    );
    assert!(sorted_names(&copy.join("quoro")).unwrap().is_empty());

    // The rename of the unit file and its drop-ins.
    let (from, to) = (host.root.join("from"), host.root.join("to"));
    fs::write(&from, "from").unwrap();
    fs::write(&to, "to").unwrap();
    let error = (Fs::REAL.rename)(&from, &to).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(fs::read_to_string(&to).unwrap(), "to");
    assert!(from.is_file());
}

/// #881 point 5.
#[tokio::test]
async fn a_forced_run_without_a_database_refuses() {
    let host = Host::new();
    fs::create_dir_all(host.data_dir().join("keys")).unwrap();
    host.unit();
    let journal = UnixDatagram::bind(host.socket()).unwrap();
    for data_dir in [true, false] {
        if !data_dir {
            fs::remove_dir_all(host.data_dir()).unwrap();
        }
        let before = host.listing();
        let systemd = FakeSystemd::new(&host);
        let (result, _) = run(&host, &forced("why"), &host.options(), &systemd).await;
        let error = result.unwrap_err();
        assert!(error.contains(&format!("holds no {DATABASE}")), "{error}");
        assert_eq!(host.listing(), before);
        assert!(systemd.calls().is_empty());
        assert!(receive(&journal).is_none(), "no statement was recorded");
    }
}

/// The `moved:` lines of a copy's `rejoin-done`.
fn moved_lines(copy: &Path) -> Vec<String> {
    fs::read_to_string(copy.join(DONE))
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("moved: "))
        .map(str::to_string)
        .collect()
}

/// #881 point 6.
#[tokio::test]
async fn rejoin_done_lists_the_moves_of_an_earlier_run() {
    // The first run moved the data directory only.
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: denied_rename,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.is_err());
    let copy = host.copies()[0].clone();
    let systemd = FakeSystemd::new(&host);
    let (result, lines) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    let line = |from: PathBuf, to: PathBuf, earlier: bool| {
        let by = if earlier { " (by an earlier run)" } else { "" };
        format!("moved: {} -> {}{by}", from.display(), to.display())
    };
    let systemd_dir = copy.join("systemd");
    assert_eq!(
        moved_lines(&copy),
        [
            line(host.data_dir(), copy.join("quoro"), true),
            line(host.unit_file(), systemd_dir.join(UNIT), false),
            line(host.drop_in(), systemd_dir.join(DROP_IN), false),
        ]
    );
    assert!(
        lines.iter().any(|l| l.ends_with("(by an earlier run)")),
        "{lines:?}"
    );

    // The first run also moved the unit file.
    let host = demoted_host().await;
    let systemd = FakeSystemd::new(&host);
    let options = Options {
        fs: Fs {
            rename: deny_drop_in,
            ..Fs::REAL
        },
        ..host.options()
    };
    let (result, _) = run(&host, &plain(), &options, &systemd).await;
    assert!(result.is_err());
    let copy = host.copies()[0].clone();
    let systemd = FakeSystemd {
        loaded: Cell::new(true),
        ..FakeSystemd::new(&host)
    };
    let (result, _) = run(&host, &plain(), &host.options(), &systemd).await;
    assert_eq!(result.unwrap(), Outcome::Rejoined(copy.clone()));
    let systemd_dir = copy.join("systemd");
    assert_eq!(
        moved_lines(&copy),
        [
            line(host.data_dir(), copy.join("quoro"), true),
            line(host.unit_file(), systemd_dir.join(UNIT), true),
            line(host.drop_in(), systemd_dir.join(DROP_IN), false),
        ]
    );
}
