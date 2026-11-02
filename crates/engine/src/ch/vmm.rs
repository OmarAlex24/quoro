// SPDX-License-Identifier: AGPL-3.0-only
//! One Cloud Hypervisor process per VM (engine-trait.md §5, D-0011).
//!
//! [`Vmm::spawn`] starts a pinned `cloud-hypervisor-static` in its own process
//! group and waits until its API socket answers. [`Vmm::adopt`] re-attaches to
//! a VMM an earlier agent process started, identified by pid plus start time so
//! a reused pid is never mistaken for the VMM. The exit is published on a watch
//! channel, so every clone of a [`Vmm`] sees the same result: the wait status
//! for our own children, `Gone` for adopted processes whose status the kernel
//! does not hand us.

use std::io::{Read as _, Seek as _, SeekFrom};
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use rustix::process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal};
use tokio::io::unix::AsyncFd;
use tokio::process::{Child, Command};
use tokio::sync::watch;

use crate::EngineError;
use crate::ch::api::ApiClient;

/// How long [`Vmm::spawn`] waits for the API socket to answer.
const START_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause between two pings while starting.
const PING_INTERVAL: Duration = Duration::from_millis(20);
/// Timeout of one ping while starting; short, so an exit is noticed quickly.
const PING_TIMEOUT: Duration = Duration::from_millis(500);
/// How long [`Vmm::spawn`] waits for a failed VMM to be reaped after SIGKILL.
const REAP_TIMEOUT: Duration = Duration::from_secs(5);
/// Bytes of the VMM log quoted in a start-up failure.
const LOG_TAIL_BYTES: u64 = 64 * 1024;
/// Log lines quoted in a start-up failure.
const LOG_TAIL_LINES: usize = 20;

/// The identity of a process: its pid and the kernel's start time for it.
///
/// A pid alone is not an identity: the kernel reuses pids, and start time
/// (field 22 of `/proc/<pid>/stat`) distinguishes two processes that had the
/// same pid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProcessId {
    /// The process id.
    pub pid: u32,
    /// Field 22 of `/proc/<pid>/stat`: start time in clock ticks since boot.
    pub start_time: u64,
}

impl ProcessId {
    /// Reads the identity of a live, non-zombie process; `None` when absent or
    /// a zombie.
    pub fn of(pid: u32) -> std::io::Result<Option<ProcessId>> {
        if pid == 0 {
            return Ok(None);
        }
        let path = format!("/proc/{pid}/stat");
        let stat = match std::fs::read_to_string(&path) {
            Ok(stat) => stat,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        match parse_start_time(pid, &stat)? {
            Some(start_time) => Ok(Some(Self { pid, start_time })),
            None => Ok(None),
        }
    }

    /// True while that exact process (same start time) exists and is not a
    /// zombie.
    pub fn is_alive(&self) -> bool {
        matches!(
            ProcessId::of(self.pid),
            Ok(Some(current)) if current.start_time == self.start_time
        )
    }
}

/// Parses the start time out of `/proc/<pid>/stat`, or `None` for a zombie.
fn parse_start_time(pid: u32, stat: &str) -> std::io::Result<Option<u64>> {
    let malformed = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("/proc/{pid}/stat: malformed"),
        )
    };
    // The command name may contain spaces and parentheses; the state follows
    // the last closing parenthesis.
    let close = stat.rfind(')').ok_or_else(malformed)?;
    let rest = stat.get(close + 1..).ok_or_else(malformed)?.trim_start();
    let mut fields = rest.split_whitespace();
    let state = fields.next().ok_or_else(malformed)?;
    if matches!(state, "Z" | "X" | "x") {
        return Ok(None);
    }
    // Remaining tokens start at field 4 (ppid); field 22 is index 18.
    let start_time = fields.nth(18).ok_or_else(malformed)?;
    let start_time = start_time.parse::<u64>().map_err(|_| malformed())?;
    Ok(Some(start_time))
}

/// How a VMM process ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VmmExit {
    /// Our own child: its wait status.
    Exited {
        /// `Some` when it exited normally.
        code: Option<i32>,
        /// `Some` when a signal killed it.
        signal: Option<i32>,
    },
    /// An adopted process (not our child): it is gone, the status is unknown.
    Gone,
}

/// Everything [`Vmm::spawn`] needs to start one VMM.
pub struct VmmLaunch<'a> {
    /// The pinned `cloud-hypervisor-static` binary.
    pub binary: &'a Path,
    /// The VMM's `--api-socket` path.
    pub api_socket: &'a Path,
    /// The `--event-monitor` file the VMM appends events to.
    pub event_log: &'a Path,
    /// Where the VMM's stdout and stderr are appended.
    pub log_file: &'a Path,
}

/// The state every clone of a [`Vmm`] shares.
struct Shared {
    id: ProcessId,
    exit: watch::Sender<Option<VmmExit>>,
}

/// A handle on one VMM process. Cheap to clone (an `Arc` inside); every clone
/// sees the same exit.
#[derive(Clone)]
pub struct Vmm {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Vmm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vmm")
            .field("id", &self.shared.id)
            .field("exit", &self.exit())
            .finish()
    }
}

impl Vmm {
    /// Starts the VMM listed in `launch` and waits until its API answers.
    ///
    /// The process runs in its own process group, with `/dev/null` as stdin
    /// and its output appended to `log_file`. A stale socket file is removed
    /// first, because a UNIX socket cannot be bound over one, and so is the
    /// previous run's event log. When the VMM never
    /// answers or exits early it is killed, reaped, and the error carries the
    /// last lines of its log.
    pub async fn spawn(launch: &VmmLaunch<'_>) -> Result<Vmm, EngineError> {
        remove_stale_socket(launch.api_socket)?;
        // The supervisor reads the event log from offset 0: a `rebooting` left
        // by the previous run would kill this one as a guest reset.
        match std::fs::remove_file(launch.event_log) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error(launch.event_log, &err)),
        }
        let stdout = open_log(launch.log_file)?;
        let stderr = stdout
            .try_clone()
            .map_err(|err| io_error(launch.log_file, &err))?;
        let mut command = Command::new(launch.binary);
        command
            .arg("--api-socket")
            .arg(format!("path={}", launch.api_socket.display()))
            .arg("--event-monitor")
            .arg(format!("path={}", launch.event_log.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(false)
            .process_group(0);
        let mut child = command.spawn().map_err(|err| {
            EngineError::Hypervisor(format!(
                "cloud-hypervisor did not start: cannot run {}: {err}",
                launch.binary.display()
            ))
        })?;
        let Some(pid) = child.id() else {
            let _ = child.wait().await;
            return Err(start_failure(
                launch,
                "the process exited before its pid could be read",
            ));
        };
        let id = match ProcessId::of(pid) {
            Ok(Some(id)) => id,
            Ok(None) => {
                let _ = child.wait().await;
                return Err(start_failure(
                    launch,
                    "the process exited while it was starting",
                ));
            }
            Err(err) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(EngineError::Hypervisor(format!(
                    "cannot read the identity of cloud-hypervisor pid {pid}: {err}"
                )));
            }
        };
        let (exit, _receiver) = watch::channel(None);
        let shared = Arc::new(Shared { id, exit });
        tokio::spawn(watch_child(child, Arc::clone(&shared)));

        let client = ApiClient::new(launch.api_socket).with_timeout(PING_TIMEOUT);
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            if shared.exit.borrow().is_some() {
                break;
            }
            if client.ping().await.is_ok() {
                return Ok(Self { shared });
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(PING_INTERVAL).await;
        }

        let vmm = Self { shared };
        let had_exited = vmm.exit().is_some();
        let _ = vmm.sigkill();
        let _ = vmm.wait_exit(REAP_TIMEOUT).await;
        let reason = if had_exited {
            "the process exited during start-up"
        } else {
            "no answer on the API socket within 5 s"
        };
        Err(start_failure(launch, reason))
    }

    /// Re-attaches to a VMM started by an earlier agent process.
    ///
    /// `Ok(None)` when that exact process (pid plus start time) is gone. The
    /// process is watched with a pidfd, so the pid cannot be recycled under us;
    /// when it becomes readable the exit is published as [`VmmExit::Gone`].
    pub fn adopt(id: ProcessId) -> Result<Option<Vmm>, EngineError> {
        if !id.is_alive() {
            return Ok(None);
        }
        let pid = Pid::from_raw(id.pid as i32).ok_or_else(|| {
            EngineError::Hypervisor(format!("cloud-hypervisor: invalid pid {}", id.pid))
        })?;
        let pidfd = match pidfd_open(pid, PidfdFlags::NONBLOCK) {
            Ok(pidfd) => pidfd,
            Err(rustix::io::Errno::SRCH) => return Ok(None),
            Err(err) => {
                return Err(EngineError::Hypervisor(format!(
                    "cannot watch cloud-hypervisor pid {}: {err}",
                    id.pid
                )));
            }
        };
        // The pid could have been recycled between the two checks; the pidfd
        // pins the process, and this re-check ties the fd to the start time.
        if !id.is_alive() {
            return Ok(None);
        }
        let (exit, _receiver) = watch::channel(None);
        let shared = Arc::new(Shared { id, exit });
        let watcher = Arc::clone(&shared);
        tokio::spawn(async move {
            // A pidfd becomes readable when the process exits. tokio 1.54
            // deprecates `AsyncFd::new` in favour of the `unsafe`
            // `AsyncFd::register`, which this crate forbids. The deprecation is
            // about fds closed behind tokio's back; here the `OwnedFd` moves
            // into the `AsyncFd`, so nothing else can close it.
            #[allow(deprecated)]
            let exit = match AsyncFd::new(pidfd) {
                Ok(pidfd) => {
                    let _ = pidfd.readable().await;
                    VmmExit::Gone
                }
                Err(_) => VmmExit::Gone,
            };
            let _ = watcher.exit.send_replace(Some(exit));
        });
        Ok(Some(Self { shared }))
    }

    /// The identity of the process this handle is about.
    pub fn id(&self) -> ProcessId {
        self.shared.id
    }

    /// The exit, once it happened.
    pub fn exit(&self) -> Option<VmmExit> {
        *self.shared.exit.borrow()
    }

    /// Waits at most `timeout` for the exit; returns it, or `None` on timeout.
    pub async fn wait_exit(&self, timeout: Duration) -> Option<VmmExit> {
        let mut exit = self.shared.exit.subscribe();
        let current = *exit.borrow_and_update();
        if current.is_some() {
            return current;
        }
        match tokio::time::timeout(timeout, exit.wait_for(|value| value.is_some())).await {
            Ok(Ok(value)) => *value,
            _ => *self.shared.exit.borrow(),
        }
    }

    /// SIGKILLs the process if it is still the same process.
    ///
    /// The signal goes through a pidfd opened after the start-time check, so a
    /// recycled pid can never receive it. A process that is already gone
    /// (`ESRCH`) is not an error.
    pub fn sigkill(&self) -> Result<(), EngineError> {
        let id = self.shared.id;
        if !id.is_alive() {
            return Ok(());
        }
        let pid = Pid::from_raw(id.pid as i32).ok_or_else(|| {
            EngineError::Hypervisor(format!("cloud-hypervisor: invalid pid {}", id.pid))
        })?;
        let pidfd = match pidfd_open(pid, PidfdFlags::NONBLOCK) {
            Ok(pidfd) => pidfd,
            Err(rustix::io::Errno::SRCH) => return Ok(()),
            Err(rustix::io::Errno::NOSYS) => {
                return match rustix::process::kill_process(pid, Signal::KILL) {
                    Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
                    Err(err) => Err(EngineError::Hypervisor(format!(
                        "cannot kill cloud-hypervisor pid {}: {err}",
                        id.pid
                    ))),
                };
            }
            Err(err) => {
                return Err(EngineError::Hypervisor(format!(
                    "cannot watch cloud-hypervisor pid {}: {err}",
                    id.pid
                )));
            }
        };
        if !id.is_alive() {
            return Ok(());
        }
        match pidfd_send_signal(&pidfd, Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(err) => Err(EngineError::Hypervisor(format!(
                "cannot kill cloud-hypervisor pid {}: {err}",
                id.pid
            ))),
        }
    }
}

/// Awaits one child to completion and publishes its wait status.
async fn watch_child(mut child: Child, shared: Arc<Shared>) {
    let exit = match child.wait().await {
        Ok(status) => VmmExit::Exited {
            code: status.code(),
            signal: status.signal(),
        },
        Err(_) => VmmExit::Exited {
            code: None,
            signal: None,
        },
    };
    let _ = shared.exit.send_replace(Some(exit));
}

/// Removes a socket file left by an earlier VMM.
fn remove_stale_socket(socket: &Path) -> Result<(), EngineError> {
    match std::fs::remove_file(socket) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(io_error(socket, &err)),
    }
}

/// Opens the VMM log for appending.
fn open_log(path: &Path) -> Result<std::fs::File, EngineError> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| io_error(path, &err))
}

/// A start-up failure that carries the tail of the VMM log.
fn start_failure(launch: &VmmLaunch<'_>, reason: &str) -> EngineError {
    EngineError::Hypervisor(format!(
        "cloud-hypervisor did not start: {reason}{}",
        log_tail(launch.log_file)
    ))
}

/// The last lines of a file, for an error message.
fn log_tail(path: &Path) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    let length = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let start = length.saturating_sub(LOG_TAIL_BYTES);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return String::new();
    }
    let mut bytes = Vec::new();
    if file.take(LOG_TAIL_BYTES).read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    let text = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let first = lines.len().saturating_sub(LOG_TAIL_LINES);
    if first == lines.len() {
        return String::new();
    }
    format!(
        "\ncloud-hypervisor log ({}):\n{}",
        path.display(),
        lines[first..].join("\n")
    )
}

/// Wraps an IO error with the path it happened on.
fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::{EngineError, ProcessId, Vmm, VmmExit, VmmLaunch, parse_start_time};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-vmm-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// Writes an executable shell script and returns its path.
    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join(name);
        std::fs::write(&path, body).expect("script");
        let mut permissions = std::fs::metadata(&path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).expect("chmod");
        path
    }

    #[test]
    fn parses_start_time_with_parentheses_and_spaces_in_comm() {
        let stat = "1234 (my proc) S 1 1234 1234 0 -1 4194560 100 0 0 0 1 2 0 0 \
                    20 0 1 0 987654 1000 100 0 0 0";
        assert_eq!(parse_start_time(1234, stat).expect("stat"), Some(987_654));
    }

    #[test]
    fn zombie_and_dead_states_have_no_identity() {
        for state in ["Z", "X", "x"] {
            let stat =
                format!("1234 (x) {state} 1 1234 1234 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 987654 0 0");
            assert_eq!(
                parse_start_time(1234, &stat).expect("stat"),
                None,
                "{state}"
            );
        }
    }

    #[test]
    fn malformed_stat_is_an_error() {
        assert!(parse_start_time(1, "no parentheses here").is_err());
        assert!(parse_start_time(1, "1 (x) S 1").is_err());
        assert!(
            parse_start_time(
                1,
                "1 (x) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 notanumber"
            )
            .is_err()
        );
    }

    #[test]
    fn reads_its_own_identity() {
        let id = ProcessId::of(std::process::id())
            .expect("stat")
            .expect("own pid is alive");
        assert_eq!(id.pid, std::process::id());
        assert!(id.start_time > 0);
        assert!(id.is_alive());
    }

    #[test]
    fn missing_pid_and_pid_zero_have_no_identity() {
        assert!(ProcessId::of(0).expect("stat").is_none());
        assert!(ProcessId::of(u32::MAX - 1).expect("stat").is_none());
    }

    #[test]
    fn a_different_start_time_is_not_the_same_process() {
        let id = ProcessId::of(std::process::id())
            .expect("stat")
            .expect("alive");
        let impostor = ProcessId {
            pid: id.pid,
            start_time: id.start_time.wrapping_add(1),
        };
        assert!(!impostor.is_alive());
    }

    #[tokio::test]
    async fn spawn_reports_an_early_exit_with_the_log() {
        let dir = temp_dir("early-exit");
        let binary = script(
            &dir,
            "vmm.sh",
            "#!/bin/sh\necho 'boom: bad option' >&2\nexit 3\n",
        );
        let socket = dir.join("api.sock");
        let events = dir.join("events.json");
        let log = dir.join("vmm.log");
        let launch = VmmLaunch {
            binary: &binary,
            api_socket: &socket,
            event_log: &events,
            log_file: &log,
        };
        let err = Vmm::spawn(&launch).await.expect_err("must fail");
        match err {
            EngineError::Hypervisor(message) => {
                assert!(message.contains("did not start"), "{message}");
                // The log may not be flushed yet when the wait status arrives;
                // poll briefly for the line the script wrote.
                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    let text = std::fs::read_to_string(&log).unwrap_or_default();
                    if text.contains("boom: bad option") {
                        break;
                    }
                    assert!(std::time::Instant::now() < deadline, "log: {text:?}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            other => panic!("unexpected error: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn spawn_of_a_missing_binary_is_a_hypervisor_error() {
        let dir = temp_dir("missing");
        let binary = dir.join("does-not-exist");
        let socket = dir.join("api.sock");
        let events = dir.join("events.json");
        let log = dir.join("vmm.log");
        let launch = VmmLaunch {
            binary: &binary,
            api_socket: &socket,
            event_log: &events,
            log_file: &log,
        };
        let err = Vmm::spawn(&launch).await.expect_err("must fail");
        assert!(matches!(err, EngineError::Hypervisor(_)), "{err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn spawn_removes_a_stale_socket_file() {
        let dir = temp_dir("stale");
        let binary = script(&dir, "vmm.sh", "#!/bin/sh\nexit 5\n");
        let socket = dir.join("api.sock");
        std::fs::write(&socket, b"stale").expect("stale socket");
        let events = dir.join("events.json");
        let log = dir.join("vmm.log");
        let launch = VmmLaunch {
            binary: &binary,
            api_socket: &socket,
            event_log: &events,
            log_file: &log,
        };
        let _ = Vmm::spawn(&launch).await;
        assert!(!socket.exists(), "stale socket file is removed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn adopt_of_a_dead_pid_is_none() {
        let id = ProcessId {
            pid: u32::MAX - 2,
            start_time: 1,
        };
        assert!(Vmm::adopt(id).expect("adopt").is_none());
    }

    #[test]
    fn exit_variants_are_comparable() {
        assert_eq!(
            VmmExit::Exited {
                code: Some(0),
                signal: None
            },
            VmmExit::Exited {
                code: Some(0),
                signal: None
            }
        );
        assert_ne!(
            VmmExit::Gone,
            VmmExit::Exited {
                code: None,
                signal: Some(9)
            }
        );
    }
}

#[cfg(test)]
mod lab {
    //! Lab-only tests of the VMM process layer, below the engine: they need
    //! KVM and root, plus
    //!
    //! - `QUORO_ENGINE_ARTIFACTS`: the pinned `cloud-hypervisor-static` and
    //!   guest kernel (`scripts/fetch-pinned.sh`);
    //! - `QUORO_ENGINE_PROBE_DIR`: two raw ext4 root filesystems booted with
    //!   `init=/quoro/init`: `probe.ext4`, whose init powers the guest off at
    //!   once, and `pb.ext4`, whose init waits for the ACPI power button and
    //!   then powers off.
    //!
    //! Scratch files go under `/var/tmp/quoro-vmm-lab`. Run with
    //! `QUORO_ENGINE_LAB=1 cargo test -p quoro-engine ch:: -- --ignored --test-threads=1`.
    //! The end-to-end engine suite is `tests/ch_lab.rs`.

    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use super::{ProcessId, Vmm, VmmExit, VmmLaunch};
    use crate::ch::api::ApiClient;
    use crate::ch::events::{EventLog, saw_reset};

    const LAB_ENV: &str = "QUORO_ENGINE_LAB";
    const CMDLINE: &str = "console=ttyS0 root=/dev/vda rw init=/quoro/init panic=-1";

    fn lab_ready() {
        assert_eq!(
            std::env::var(LAB_ENV).ok().as_deref(),
            Some("1"),
            "set {LAB_ENV}=1 and run with --ignored"
        );
    }

    const SCRATCH: &str = "/var/tmp/quoro-vmm-lab";

    fn env_dir(name: &str) -> PathBuf {
        PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set")))
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = Path::new(SCRATCH).join(name);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn ch_binary() -> PathBuf {
        env_dir("QUORO_ENGINE_ARTIFACTS").join("cloud-hypervisor-static")
    }

    fn kernel() -> PathBuf {
        env_dir("QUORO_ENGINE_ARTIFACTS").join("vmlinux-x86_64")
    }

    fn rootfs(image: &str) -> PathBuf {
        let dst = scratch("images").join(image);
        if !dst.exists() {
            std::fs::copy(env_dir("QUORO_ENGINE_PROBE_DIR").join(image), &dst)
                .expect("copy rootfs");
        }
        dst
    }

    /// The files of one lab VM under `/var/tmp/quoro-vmm-lab/<name>`.
    struct Vm {
        dir: PathBuf,
        socket: PathBuf,
        event_log: PathBuf,
        log: PathBuf,
        serial: PathBuf,
        console: PathBuf,
    }

    impl Vm {
        fn new(name: &str) -> Self {
            let dir = scratch(name);
            let vm = Self {
                socket: dir.join("api.sock"),
                event_log: dir.join("events.json"),
                log: dir.join("vmm.log"),
                serial: dir.join("serial.log"),
                console: dir.join("console.log"),
                dir,
            };
            for file in [&vm.socket, &vm.event_log, &vm.log, &vm.serial, &vm.console] {
                let _ = std::fs::remove_file(file);
            }
            vm
        }

        fn socket(&self) -> &Path {
            &self.socket
        }

        fn event_log(&self) -> &Path {
            &self.event_log
        }

        fn launch<'a>(&'a self, binary: &'a Path) -> VmmLaunch<'a> {
            VmmLaunch {
                binary,
                api_socket: &self.socket,
                event_log: &self.event_log,
                log_file: &self.log,
            }
        }

        fn config(&self, rootfs: &Path, cmdline: &str) -> serde_json::Value {
            serde_json::json!({
                "payload": {"kernel": kernel(), "cmdline": cmdline},
                "cpus": {"boot_vcpus": 1, "max_vcpus": 1},
                "memory": {"size": 268435456},
                "disks": [{"path": rootfs, "image_type": "Raw", "serial": "quoro-root"}],
                "serial": {"mode": "File", "file": &self.serial},
                "console": {"mode": "File", "file": &self.console}
            })
        }
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_spawn_kills_a_vmm_that_never_answers() {
        lab_ready();
        use std::os::unix::fs::PermissionsExt as _;
        let dir = scratch("silent");
        let binary = dir.join("silent.sh");
        std::fs::write(
            &binary,
            "#!/bin/sh\necho 'starting without an api socket'\nexec sleep 30\n",
        )
        .expect("script");
        let mut permissions = std::fs::metadata(&binary).expect("metadata").permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&binary, permissions).expect("chmod");
        let vm = Vm::new("silent-vm");
        let launch = vm.launch(&binary);
        let started = Instant::now();
        let err = Vmm::spawn(&launch).await.expect_err("no API, no VMM");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "gave up after {:?}",
            started.elapsed()
        );
        let message = err.to_string();
        assert!(message.contains("did not start"), "{message}");
        assert!(message.contains("no answer on the API socket"), "{message}");
        assert!(
            message.contains("starting without an api socket"),
            "the error quotes the log tail: {message}"
        );
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_spawn_boot_poweroff() {
        lab_ready();
        let vm = Vm::new("boot");
        let binary = ch_binary();
        let launch = vm.launch(&binary);
        let vmm = Vmm::spawn(&launch).await.expect("spawn");
        assert!(vmm.id().pid > 0);
        assert!(vmm.exit().is_none());

        let client = ApiClient::new(vm.socket());
        let ping = client.ping().await.expect("ping");
        assert!(ping.get("version").is_some(), "ping: {ping}");

        let config = vm.config(&rootfs("probe.ext4"), CMDLINE);
        client.create(&config).await.expect("create");
        let info = client.info().await.expect("info");
        assert_eq!(info["state"], "Created", "info: {info}");

        client.boot().await.expect("boot");
        let info = client.info().await.expect("info after boot");
        assert_eq!(info["state"], "Running", "info: {info}");

        let exit = vmm.wait_exit(Duration::from_secs(120)).await.expect("exit");
        assert_eq!(
            exit,
            VmmExit::Exited {
                code: Some(0),
                signal: None
            }
        );
        assert_eq!(vmm.exit(), Some(exit));
        assert!(!vm.socket().exists(), "the VMM removed its socket file");
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_power_button_then_shutdown_vmm() {
        lab_ready();
        let vm = Vm::new("power-button");
        let binary = ch_binary();
        let launch = vm.launch(&binary);
        let vmm = Vmm::spawn(&launch).await.expect("spawn");
        let client = ApiClient::new(vm.socket());
        let config = vm.config(&rootfs("pb.ext4"), CMDLINE);
        client.create(&config).await.expect("create");
        client.boot().await.expect("boot");
        let info = client.info().await.expect("info");
        assert_eq!(info["state"], "Running", "info: {info}");

        tokio::time::sleep(Duration::from_secs(1)).await;
        // The old init in `pb.ext4` ignores the ACPI power button; the API
        // accepts the request either way.
        let _ = client.power_button().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            vmm.exit().is_none(),
            "the guest still runs after an ignored power button"
        );

        client.shutdown_vmm().await.expect("shutdown_vmm");
        let exit = vmm.wait_exit(Duration::from_secs(5)).await.expect("exit");
        assert!(
            matches!(exit, VmmExit::Exited { .. }),
            "shutdown_vmm exit: {exit:?}"
        );
        assert!(!vm.socket().exists());
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_sigkill_reports_signal_nine() {
        lab_ready();
        let vm = Vm::new("sigkill");
        let binary = ch_binary();
        let launch = vm.launch(&binary);
        let vmm = Vmm::spawn(&launch).await.expect("spawn");
        let client = ApiClient::new(vm.socket());
        let config = vm.config(&rootfs("pb.ext4"), CMDLINE);
        client.create(&config).await.expect("create");
        client.boot().await.expect("boot");
        tokio::time::sleep(Duration::from_secs(1)).await;

        vmm.sigkill().expect("sigkill");
        let exit = vmm.wait_exit(Duration::from_secs(5)).await.expect("exit");
        assert_eq!(
            exit,
            VmmExit::Exited {
                code: None,
                signal: Some(9)
            }
        );
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_adopt_sees_an_external_vmm_die() {
        lab_ready();
        let vm = Vm::new("adopt");
        let socket = vm.socket().to_path_buf();
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "setsid {} --api-socket path={} --event-monitor path={} >> {} 2>&1 & echo $!",
                ch_binary().display(),
                socket.display(),
                vm.event_log().display(),
                vm.dir.join("vmm.log").display()
            ))
            .output()
            .expect("start cloud-hypervisor with setsid");
        assert!(output.status.success(), "shell: {output:?}");
        let pid: u32 = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .expect("pid from echo");
        let client = ApiClient::new(&socket).with_timeout(Duration::from_millis(500));
        let deadline = Instant::now() + Duration::from_secs(10);
        while client.ping().await.is_err() {
            assert!(Instant::now() < deadline, "external VMM never answered");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let id = ProcessId::of(pid)
            .expect("stat")
            .expect("external VMM is alive");
        let vmm = Vmm::adopt(id).expect("adopt").expect("adopted");
        assert_eq!(vmm.id(), id);
        assert!(vmm.exit().is_none());

        let status = std::process::Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .expect("kill -9");
        assert!(status.success(), "kill: {status:?}");
        let exit = vmm.wait_exit(Duration::from_secs(5)).await;
        assert_eq!(exit, Some(VmmExit::Gone));

        assert!(
            Vmm::adopt(id).expect("adopt dead").is_none(),
            "a dead pid is not adopted"
        );
        let _ = std::fs::remove_file(&socket);
    }

    #[tokio::test]
    #[ignore = "needs KVM and Cloud Hypervisor: QUORO_ENGINE_LAB=1"]
    async fn lab_event_log_reports_a_guest_reset() {
        lab_ready();
        let vm = Vm::new("reset");
        let binary = ch_binary();
        let launch = vm.launch(&binary);
        let vmm = Vmm::spawn(&launch).await.expect("spawn");
        let client = ApiClient::new(vm.socket());
        let cmdline = "console=ttyS0 root=/dev/vda rw init=/nonexistent panic=-1";
        let config = vm.config(&rootfs("probe.ext4"), cmdline);
        client.create(&config).await.expect("create");
        client.boot().await.expect("boot");

        let mut events = EventLog::new(vm.event_log());
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reset = false;
        while Instant::now() < deadline {
            let batch = events.poll().expect("poll events");
            if saw_reset(&batch) {
                reset = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(reset, "the event log did not report a reset");

        vmm.sigkill().expect("sigkill");
        let exit = vmm.wait_exit(Duration::from_secs(5)).await.expect("exit");
        assert!(
            matches!(
                exit,
                VmmExit::Exited {
                    signal: Some(9),
                    ..
                }
            ),
            "exit: {exit:?}"
        );
    }
}
