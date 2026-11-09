// SPDX-License-Identifier: AGPL-3.0-only
//! One exec session: read the open, allocate a PTY, start the program through
//! the helper, relay both directions, report the exit. Or, when the first
//! frame is a sync (ADR 0282), one sync session: `syncfs` on each named
//! volume's mount and one `SYNCED` or `ERROR` answer, with no program and no
//! PTY.
//!
//! The relay is generic over the connection ([`SessionIo`]) so the tests can
//! run a real PTY and real programs over `UnixStream::pair()` without a vsock
//! device. Production passes [`vsock::VsockStream`].
//!
//! Back-pressure is the host's job: the output thread writes `OUTPUT` frames
//! with blocking writes, so a host that stops reading stops the PTY reader,
//! the program's terminal fills up, and the program blocks. Nothing here
//! buffers output beyond one 8 KiB read.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use quoro_init::exec::{
    FrameReader, GuestFrame, HostFrame, MAX_PAYLOAD, OPEN, OpenRequest, SYNC, SyncRequest,
};
use rustix::fs::{Mode, OFlags};
#[cfg(test)]
use rustix::io::Errno;
#[cfg(test)]
use rustix::process::WaitOptions;
use rustix::process::{Pid, Signal, WaitStatus};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use rustix::termios::{Winsize, tcsetwinsize};
use vsock::VsockStream;

use crate::guest::app;
use crate::guest::diag;
use crate::guest::exec::helper::HELPER_ARG;
use crate::guest::exec::reaper;
use crate::guest::user::{self, Identity};

/// How long a session waits for the PTY reader to drain after the program
/// exits before it gives up and closes. A host that stopped reading would
/// otherwise hold the session (and one of the 16 slots) for ever.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// `TERM` every session gets, replacing whatever the app's environment had.
const TERM: &str = "xterm-256color";

/// A stream the session can duplicate for its reader and shut down when it
/// ends. Implemented for the vsock connection in production and for
/// `UnixStream` in the tests.
pub(super) trait SessionIo: Read + Write + Send + Sync + Sized + 'static {
    /// An independent handle to the same connection.
    fn try_clone_io(&self) -> io::Result<Self>;
    /// Write a frame through a shared reference. Both `VsockStream` and
    /// `UnixStream` implement `Write` for `&Self`, which lets the output and
    /// control threads share one connection.
    fn write_shared(&self, bytes: &[u8]) -> io::Result<()>;
    /// End both directions, which wakes a reader blocked on this connection.
    fn shutdown_both(&self) -> io::Result<()>;
}

impl SessionIo for VsockStream {
    fn try_clone_io(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn write_shared(&self, bytes: &[u8]) -> io::Result<()> {
        let mut this = self;
        this.write_all(bytes)
    }

    fn shutdown_both(&self) -> io::Result<()> {
        self.shutdown(Shutdown::Both)
    }
}

impl SessionIo for UnixStream {
    fn try_clone_io(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn write_shared(&self, bytes: &[u8]) -> io::Result<()> {
        let mut this = self;
        this.write_all(bytes)
    }

    fn shutdown_both(&self) -> io::Result<()> {
        self.shutdown(Shutdown::Both)
    }
}

/// End a connection that never became a session, such as the "too many
/// sessions" answer.
pub(super) fn close_stream<S: SessionIo>(stream: &S) {
    let _ = stream.shutdown_both();
}

/// What every session of this VM shares: the app's environment, working
/// directory and identity, its volumes for sync sessions, and the reason of
/// the last sync that failed.
pub(super) struct Context {
    env: Vec<(String, String)>,
    workdir: Option<String>,
    identity: Identity,
    /// `(serial, mount_path)` of every volume in the configuration.
    volumes: Vec<(String, String)>,
    /// Per volume serial, the reason of its last failed sync, cleared when a
    /// sync of that volume succeeds.
    sync_failures: Mutex<HashMap<String, String>>,
}

impl Context {
    pub(super) fn new(config: &quoro_init::GuestConfig, identity: &Identity) -> Self {
        Self {
            env: config.env.clone(),
            workdir: config.workdir.clone(),
            identity: identity.clone(),
            volumes: config
                .volumes
                .iter()
                .map(|volume| (volume.serial.clone(), volume.mount_path.clone()))
                .collect(),
            sync_failures: Mutex::new(HashMap::new()),
        }
    }

    /// Records the result of one volume's sync. A failure is logged on the
    /// serial console only when it is that volume's first one or its reason
    /// changed, so a sync every few seconds that keeps failing, even
    /// alternating with another volume's success, does not push the boot
    /// output out of the serial ring (`engine-trait.md` section 6). Returns
    /// whether it was logged.
    fn note_sync(&self, serial: &str, failure: Option<&str>) -> bool {
        let Ok(mut failures) = self.sync_failures.lock() else {
            return false;
        };
        let Some(reason) = failure else {
            failures.remove(serial);
            return false;
        };
        if failures.get(serial).map(String::as_str) == Some(reason) {
            return false;
        }
        diag::log(&format!("exec: {reason}"));
        failures.insert(serial.to_string(), reason.to_string());
        true
    }
}

/// How a session is started: `/proc/self/exe` with the helper argument in
/// production, an injected wrapper in the tests.
pub(super) struct Helper {
    pub(super) program: PathBuf,
    pub(super) leading: Vec<OsString>,
}

impl Helper {
    pub(super) fn quoro_init() -> Self {
        Self {
            program: PathBuf::from("/proc/self/exe"),
            leading: vec![OsString::from(HELPER_ARG)],
        }
    }
}

/// Where the child's status comes from. `supervise` reaps every child, so
/// production waits on the [`reaper`] channel; the tests have no PID-1 loop
/// and wait on their own specific pid.
#[derive(Clone, Copy)]
pub(super) enum Wait {
    Registry,
    #[cfg(test)]
    Direct,
}

/// One session, start to close.
pub(super) fn serve<S: SessionIo>(stream: S, context: &Context, helper: &Helper, wait: Wait) {
    let mut reader = match stream.try_clone_io() {
        Ok(reader) => reader,
        Err(err) => {
            diag::log(&format!(
                "exec: cannot duplicate the session connection: {err}"
            ));
            return;
        }
    };
    let sink = Arc::new(Sink::new(stream));

    let mut frames = FrameReader::new();
    let request = match read_open(&mut reader, &mut frames) {
        Opened::Request(request) => request,
        Opened::Sync(request) => {
            // A sync is the session's only frame: nothing after it is read.
            serve_sync(&request, context, &sink);
            return;
        }
        Opened::Closed => return,
        Opened::Bad(message) => {
            sink.error(&message);
            sink.shutdown();
            return;
        }
    };

    // The user of the open, or the app's own identity.
    let identity = match request.user.as_deref() {
        None => context.identity.clone(),
        Some(spec) => match user::resolve(Some(spec)) {
            Ok(identity) => identity,
            Err(_) => {
                sink.error(&format!("no such user {spec:?}"));
                sink.shutdown();
                return;
            }
        },
    };

    // A program the image does not have is not an error of the open: the
    // session starts, says so on its terminal, and exits 127.
    let path = app::path_env(&context.env);
    let Some(program) = app::resolve_program(&request.argv[0], &path) else {
        sink.output(format!("quoro-init: {}: not found\r\n", request.argv[0]).as_bytes());
        sink.finish(&GuestFrame::Exit(127));
        sink.shutdown();
        return;
    };

    let (master, slave_path) = match open_pty(request.rows, request.cols) {
        Ok(pty) => pty,
        Err(err) => {
            sink.error(&format!("cannot allocate a pty: {err}"));
            sink.shutdown();
            return;
        }
    };
    let [slave_in, slave_out, slave_err] = match open_slave(&slave_path) {
        Ok(fds) => fds,
        Err(err) => {
            sink.error(&format!("cannot open the pty slave: {err}"));
            sink.shutdown();
            return;
        }
    };

    let mut command = Command::new(&helper.program);
    for argument in &helper.leading {
        command.arg(argument);
    }
    command.arg(&program);
    // argv[0] is passed through the helper so `sh -c` and friends see the
    // name the host asked for, not `/proc/self/exe`.
    command.arg(&request.argv[0]);
    for argument in request.argv.iter().skip(1) {
        command.arg(argument);
    }

    command.env_clear();
    for (key, value) in session_env(&context.env, &identity) {
        command.env(key, value);
    }
    command.current_dir(context.workdir.as_deref().unwrap_or("/"));
    // Only change credentials that differ from the init's own: `Command::uid`
    // also clears the supplementary groups, and the tests do not run as root.
    if identity.uid != rustix::process::getuid().as_raw() {
        command.uid(identity.uid);
    }
    if identity.gid != rustix::process::getgid().as_raw() {
        command.gid(identity.gid);
    }
    command.stdin(Stdio::from(slave_in));
    command.stdout(Stdio::from(slave_out));
    command.stderr(Stdio::from(slave_err));

    // Spawn and register in one step: a program that exits at once still
    // reaches this session's status receiver.
    let spawned = match wait {
        Wait::Registry => reaper::spawn_registered(&mut command),
        #[cfg(test)]
        Wait::Direct => command.spawn().map(|child| {
            let receiver = direct_wait(Pid::from_child(&child));
            (child, receiver)
        }),
    };
    let (child, status) = match spawned {
        Ok(spawned) => spawned,
        Err(err) => {
            diag::log(&format!("exec: spawn {program:?}: {err}"));
            sink.error(&format!("cannot start {program:?}: {err}"));
            sink.shutdown();
            return;
        }
    };
    // The command still owns the three slave descriptors it passed to the
    // child; holding them would keep the PTY master from ever reaching EIO.
    drop(command);
    // PID 1 reaps this child from `supervise`; dropping `Child` does not wait.
    let pid = Pid::from_child(&child);
    drop(child);
    diag::log(&format!(
        "exec: session started pid={pid} program={program}"
    ));

    let master_read = match master.try_clone() {
        Ok(file) => file,
        Err(err) => {
            sink.error(&format!("cannot duplicate the pty: {err}"));
            hangup(pid);
            sink.shutdown();
            return;
        }
    };
    let sink_for_input = Arc::clone(&sink);
    if let Err(err) = thread::Builder::new()
        .name("exec-input".to_string())
        .spawn(move || relay_input(reader, frames, master, &sink_for_input, pid))
    {
        diag::log(&format!("exec: cannot start the input relay: {err}"));
        sink.error("cannot start the session relay");
        hangup(pid);
        sink.shutdown();
        return;
    }

    let (output_done, drained) = channel();
    let sink_for_output = Arc::clone(&sink);
    if let Err(err) = thread::Builder::new()
        .name("exec-output".to_string())
        .spawn(move || {
            relay_output(master_read, &sink_for_output);
            let _ = output_done.send(());
        })
    {
        diag::log(&format!("exec: cannot start the output relay: {err}"));
        sink.error("cannot start the session relay");
        hangup(pid);
        sink.shutdown();
        return;
    }

    let Some(status) = status.recv().ok() else {
        diag::log("exec: the session child was reaped without a status");
        hangup(pid);
        sink.error("the session ended without a status");
        sink.shutdown();
        return;
    };
    let code = exit_code(status);

    // Let the program's own children go, then drain what the PTY still holds.
    // `drained` only arrives after the master reaches EIO, which needs every
    // slave closed.
    let _ = rustix::process::kill_process_group(pid, Signal::HUP);
    if drained.recv_timeout(DRAIN_TIMEOUT).is_err() {
        let _ = rustix::process::kill_process_group(pid, Signal::KILL);
        // A host that stopped reading blocks the output thread in a write;
        // shutting the connection down frees it before we send the exit.
        sink.shutdown();
        let _ = drained.recv_timeout(Duration::from_secs(1));
    }

    sink.finish(&GuestFrame::Exit(code));
    sink.shutdown();
    diag::log(&format!("exec: session ended pid={pid} code={code}"));
}

/// How the first frame was read.
enum Opened {
    Request(OpenRequest),
    /// A sync session (ADR 0282).
    Sync(SyncRequest),
    /// The host closed before the open completed; there is nobody to answer.
    Closed,
    /// The host broke the format; the message goes back as `ERROR`.
    Bad(String),
}

/// Read frames until the open or the sync arrives. Any other first frame, a
/// malformed or oversized one, is refused; extra bytes already buffered stay
/// in `frames` for the relay.
fn read_open<R: Read>(reader: &mut R, frames: &mut FrameReader) -> Opened {
    let mut chunk = [0_u8; 8192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => {
                return if frames.buffered() == 0 {
                    Opened::Closed
                } else {
                    Opened::Bad("the connection closed in the middle of a frame".to_string())
                };
            }
            Ok(read) => {
                frames.push(&chunk[..read]);
                match frames.next_frame() {
                    Ok(Some((kind, payload))) => {
                        if kind != OPEN && kind != SYNC {
                            return Opened::Bad(format!(
                                "the first frame must be open or sync, got type {kind}"
                            ));
                        }
                        return match HostFrame::decode(kind, &payload) {
                            Ok(HostFrame::Open(request)) => Opened::Request(request),
                            Ok(HostFrame::Sync(request)) => Opened::Sync(request),
                            Ok(_) => {
                                Opened::Bad("the first frame is not an open or a sync".to_string())
                            }
                            Err(err) => Opened::Bad(err.to_string()),
                        };
                    }
                    Ok(None) => continue,
                    Err(err) => return Opened::Bad(err.to_string()),
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Opened::Closed,
        }
    }
}

/// A sync session (ADR 0282, `engine-trait.md` section 6): `syncfs` on the
/// mount of each named volume, in order, then `SYNCED`; or `ERROR` with the
/// first reason. It starts no program, allocates no PTY and writes nothing to
/// the app's console.
fn serve_sync<S: SessionIo>(request: &SyncRequest, context: &Context, sink: &Sink<S>) {
    for serial in &request.serials {
        if let Err(reason) = sync_volume(serial, &context.volumes) {
            context.note_sync(serial, Some(&reason));
            sink.error(&reason);
            sink.shutdown();
            return;
        }
        context.note_sync(serial, None);
    }
    sink.finish(&GuestFrame::Synced);
    sink.shutdown();
}

/// `syncfs` on the serial's mount path, opened now, so a path the app
/// remounted or the init unmounted is seen as it is at this moment.
fn sync_volume(serial: &str, volumes: &[(String, String)]) -> Result<(), String> {
    let Some((_, path)) = volumes.iter().find(|(known, _)| known == serial) else {
        return Err(format!("sync: no volume {serial} in the configuration"));
    };
    let directory = rustix::fs::open(
        path.as_str(),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|err| format!("sync: open {path} of {serial}: {err}"))?;
    rustix::fs::syncfs(&directory).map_err(|err| format!("sync: syncfs {path} of {serial}: {err}"))
}

/// Host frames to the PTY. Returns when the host closes, drops the connection,
/// the PTY goes away, or the format breaks (after answering with `ERROR`).
fn relay_input<S: SessionIo>(
    mut reader: S,
    mut frames: FrameReader,
    mut master: File,
    sink: &Sink<S>,
    pid: Pid,
) {
    let mut chunk = [0_u8; 8192];
    loop {
        // Frames may already be buffered: the open and the first keystrokes
        // often arrive in one read.
        if !handle_frames(&mut frames, &mut master, sink, pid) {
            return;
        }
        match reader.read(&mut chunk) {
            Ok(0) => {
                hangup(pid);
                return;
            }
            Ok(read) => frames.push(&chunk[..read]),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                hangup(pid);
                return;
            }
        }
    }
}

/// Handle every complete frame in the reader. `false` means the relay ends.
fn handle_frames<S: SessionIo>(
    frames: &mut FrameReader,
    master: &mut File,
    sink: &Sink<S>,
    pid: Pid,
) -> bool {
    loop {
        match frames.next_frame() {
            Ok(Some((kind, payload))) => match HostFrame::decode(kind, &payload) {
                Ok(HostFrame::Input(bytes)) => {
                    if master.write_all(&bytes).is_err() {
                        hangup(pid);
                        return false;
                    }
                }
                Ok(HostFrame::Resize { rows, cols }) => {
                    let _ = tcsetwinsize(&*master, window_size(rows, cols));
                }
                Ok(HostFrame::Close) => {
                    hangup(pid);
                    return false;
                }
                Ok(HostFrame::Open(_)) => {
                    sink.error("the open frame may appear only once");
                    hangup(pid);
                    return false;
                }
                Ok(HostFrame::Sync(_)) => {
                    sink.error("a sync frame may only be a session's first frame");
                    hangup(pid);
                    return false;
                }
                Err(err) => {
                    sink.error(&err.to_string());
                    hangup(pid);
                    return false;
                }
            },
            Ok(None) => return true,
            Err(err) => {
                sink.error(&err.to_string());
                hangup(pid);
                return false;
            }
        }
    }
}

/// PTY output to `OUTPUT` frames until EIO/EOF. The writes block, so a host
/// that stops reading applies back-pressure to the program.
fn relay_output<S: SessionIo>(mut master: File, sink: &Sink<S>) {
    let mut chunk = [0_u8; 8192];
    loop {
        match master.read(&mut chunk) {
            Ok(0) => return,
            Ok(read) => {
                if !sink.output(&chunk[..read]) {
                    return;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            // EIO is how a PTY master reports that every slave is closed.
            Err(_) => return,
        }
    }
}

/// Allocate a PTY, unlock its slave and set the initial window size.
fn open_pty(rows: u16, cols: u16) -> io::Result<(File, PathBuf)> {
    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC)
        .map_err(io::Error::from)?;
    grantpt(&master).map_err(io::Error::from)?;
    unlockpt(&master).map_err(io::Error::from)?;
    let name = ptsname(&master, Vec::new()).map_err(io::Error::from)?;
    let path = PathBuf::from(OsStr::from_bytes(name.to_bytes()));
    tcsetwinsize(&master, window_size(rows, cols)).map_err(io::Error::from)?;
    Ok((File::from(master), path))
}

/// Three handles on the slave for stdin, stdout and stderr. `NOCTTY` matters:
/// PID 1 must not acquire the terminal itself, only the helper does. Each is
/// opened separately so all three carry `CLOEXEC`: a slave that leaked into
/// another session's helper would keep this PTY alive after its program exits.
fn open_slave(path: &Path) -> io::Result<[std::os::fd::OwnedFd; 3]> {
    let flags = OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC;
    let open = || rustix::fs::open(path, flags, Mode::empty()).map_err(io::Error::from);
    Ok([open()?, open()?, open()?])
}

fn window_size(rows: u16, cols: u16) -> Winsize {
    Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

/// The app's environment for a session: the config's, plus `HOME` and `PATH`
/// defaults like the app gets, with `TERM` replaced.
fn session_env(env: &[(String, String)], identity: &Identity) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = env
        .iter()
        .filter(|(key, _)| key != "TERM")
        .cloned()
        .collect();
    if !out.iter().any(|(key, _)| key == "HOME") {
        out.push(("HOME".to_string(), identity.home.clone()));
    }
    if !out.iter().any(|(key, _)| key == "PATH") {
        out.push(("PATH".to_string(), app::DEFAULT_PATH.to_string()));
    }
    out.push(("TERM".to_string(), TERM.to_string()));
    out
}

/// `SIGHUP` the session's process group. Before the helper has called
/// `setsid` the group does not exist yet, so fall back to the child itself.
fn hangup(pid: Pid) {
    if rustix::process::kill_process_group(pid, Signal::HUP).is_err() {
        let _ = rustix::process::kill_process(pid, Signal::HUP);
    }
}

/// `exit n` → `n`, killed by signal `s` → `128 + s`.
fn exit_code(status: WaitStatus) -> i32 {
    if let Some(code) = status.exit_status() {
        return code;
    }
    if let Some(signal) = status.terminating_signal() {
        return 128 + signal;
    }
    1
}

/// Test-only status source: this process is not PID 1's supervise loop, so a
/// specific `waitpid` is safe and no registry entry is needed.
#[cfg(test)]
fn direct_wait(pid: Pid) -> std::sync::mpsc::Receiver<WaitStatus> {
    let (sender, receiver) = channel();
    thread::spawn(move || {
        loop {
            match rustix::process::waitpid(Some(pid), WaitOptions::empty()) {
                Ok(Some((_, status))) => {
                    let _ = sender.send(status);
                    return;
                }
                Ok(None) => return,
                Err(Errno::INTR) => continue,
                Err(_) => return,
            }
        }
    });
    receiver
}

/// Serialised, terminal-aware writes to the connection.
///
/// `finished` makes `ERROR` and `EXIT` terminal: once the reader thread sends
/// an `ERROR`, the output thread stops, and `EXIT` is never appended after it.
struct Sink<S> {
    stream: Arc<S>,
    writing: Mutex<()>,
    finished: AtomicBool,
}

impl<S: SessionIo> Sink<S> {
    fn new(stream: S) -> Self {
        Self {
            stream: Arc::new(stream),
            writing: Mutex::new(()),
            finished: AtomicBool::new(false),
        }
    }

    /// One `OUTPUT` frame, chunked to the 64 KiB payload limit. `false` means
    /// the connection is gone or finished; the caller stops relaying.
    fn output(&self, bytes: &[u8]) -> bool {
        for chunk in bytes.chunks(MAX_PAYLOAD) {
            if !self.transmit(&GuestFrame::Output(chunk.to_vec())) {
                return false;
            }
        }
        true
    }

    /// A terminal `ERROR` frame.
    fn error(&self, message: &str) {
        self.terminate(&GuestFrame::Error(message.to_string()));
    }

    /// A terminal frame, dropped when the session already ended.
    fn finish(&self, frame: &GuestFrame) {
        self.terminate(frame);
    }

    /// Write unless the session already ended. Returns whether it was sent.
    fn transmit(&self, frame: &GuestFrame) -> bool {
        let Ok(bytes) = frame.encode() else {
            return false;
        };
        let Ok(_guard) = self.writing.lock() else {
            return false;
        };
        if self.finished.load(Ordering::SeqCst) {
            return false;
        }
        self.stream.write_shared(&bytes).is_ok()
    }

    /// Mark the session finished and write its last frame, if none was written
    /// before.
    fn terminate(&self, frame: &GuestFrame) {
        let Ok(bytes) = frame.encode() else {
            return;
        };
        let Ok(_guard) = self.writing.lock() else {
            return;
        };
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.stream.write_shared(&bytes);
    }

    fn shutdown(&self) {
        let _ = self.stream.shutdown_both();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    /// A wrapper that behaves like the real helper's `setsid` + `exec`, minus
    /// the controlling-terminal dance (the PTY is already the child's
    /// terminal). It drops the arg0 the session passes, because only the real
    /// helper knows how to set argv[0] safely.
    fn test_helper() -> Helper {
        Helper {
            program: PathBuf::from("/bin/sh"),
            leading: vec![
                OsString::from("-c"),
                OsString::from("p=$0; shift; exec \"$p\" \"$@\""),
            ],
        }
    }

    fn test_context() -> Context {
        Context {
            env: vec![
                (
                    "PATH".to_string(),
                    "/usr/local/bin:/usr/bin:/bin".to_string(),
                ),
                ("TERM".to_string(), "dumb".to_string()),
                ("APP".to_string(), "yes".to_string()),
            ],
            workdir: None,
            identity: Identity {
                uid: rustix::process::getuid().as_raw(),
                gid: rustix::process::getgid().as_raw(),
                home: "/".to_string(),
            },
            volumes: Vec::new(),
            sync_failures: Mutex::new(HashMap::new()),
        }
    }

    /// A session over one end of a socket pair. The other end is the host.
    fn start(argv: &[&str], rows: u16, cols: u16) -> Client {
        let (host, guest) = UnixStream::pair().unwrap_or_else(|err| panic!("pair: {err}"));
        let request = OpenRequest {
            argv: argv.iter().map(|arg| (*arg).to_string()).collect(),
            user: None,
            rows,
            cols,
        };
        let context = test_context();
        let helper = test_helper();
        thread::spawn(move || {
            serve(guest, &context, &helper, Wait::Direct);
        });
        let mut client = Client::new(host);
        client.send(HostFrame::Open(request));
        client
    }

    /// The host side of a session in the tests: a frame reader with a timeout
    /// and the bytes collected so far.
    struct Client {
        stream: UnixStream,
        frames: FrameReader,
        output: Vec<u8>,
        exit: Option<i32>,
        error: Option<String>,
        synced: bool,
    }

    impl Client {
        fn new(stream: UnixStream) -> Self {
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap_or_else(|err| panic!("read timeout: {err}"));
            Self {
                stream,
                frames: FrameReader::new(),
                output: Vec::new(),
                exit: None,
                error: None,
                synced: false,
            }
        }

        fn send(&mut self, frame: HostFrame) {
            let bytes = frame.encode().unwrap_or_else(|err| panic!("encode: {err}"));
            self.stream
                .write_all(&bytes)
                .unwrap_or_else(|err| panic!("write: {err}"));
        }

        fn send_raw(&mut self, bytes: &[u8]) {
            self.stream
                .write_all(bytes)
                .unwrap_or_else(|err| panic!("write: {err}"));
        }

        /// Read one chunk and decode whatever frames it completed. Returns
        /// `false` at end of stream.
        fn pump(&mut self) -> bool {
            let mut chunk = [0_u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => false,
                Ok(read) => {
                    self.frames.push(&chunk[..read]);
                    loop {
                        match self.frames.next_frame() {
                            Ok(Some((kind, payload))) => match GuestFrame::decode(kind, &payload) {
                                Ok(GuestFrame::Output(bytes)) => self.output.extend(bytes),
                                Ok(GuestFrame::Exit(code)) => self.exit = Some(code),
                                Ok(GuestFrame::Error(message)) => self.error = Some(message),
                                Ok(GuestFrame::Synced) => self.synced = true,
                                Err(err) => panic!("decode: {err}"),
                            },
                            Ok(None) => return true,
                            Err(err) => panic!("frame: {err}"),
                        }
                    }
                }
                Err(err)
                    if err.kind() == io::ErrorKind::Interrupted
                        || err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::TimedOut =>
                {
                    panic!(
                        "timed out; output so far: {:?}",
                        String::from_utf8_lossy(&self.output)
                    );
                }
                Err(_) => false,
            }
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.output).into_owned()
        }

        fn wait_for_output(&mut self, needle: &str) {
            loop {
                if self.text().contains(needle) {
                    return;
                }
                assert!(
                    self.pump(),
                    "end of stream before {needle:?}; got {:?}",
                    self.text()
                );
            }
        }

        /// Read until `EXIT`, returning the exit code.
        fn finish(mut self) -> i32 {
            loop {
                if let Some(code) = self.exit {
                    return code;
                }
                assert!(
                    self.pump(),
                    "end of stream without EXIT; got {:?}",
                    self.text()
                );
            }
        }

        /// Read until a terminal `ERROR`.
        fn finish_error(mut self) -> String {
            loop {
                if let Some(message) = self.error.take() {
                    return message;
                }
                assert!(self.pump(), "end of stream without ERROR");
            }
        }
    }

    #[test]
    fn a_session_runs_the_program_and_reports_its_exit() {
        let mut client = start(&["/bin/sh", "-c", "echo hi; exit 3"], 24, 80);
        client.wait_for_output("hi");
        assert_eq!(client.finish(), 3);
    }

    #[test]
    fn the_window_size_is_set_and_resized() {
        let mut client = start(
            &["/bin/sh", "-c", "stty size; read line; stty size"],
            24,
            80,
        );
        client.wait_for_output("24 80");
        client.send(HostFrame::Resize {
            rows: 50,
            cols: 132,
        });
        client.send(HostFrame::Input(b"\n".to_vec()));
        client.wait_for_output("50 132");
        assert_eq!(client.finish(), 0);
    }

    #[test]
    fn close_hangs_up_the_program() {
        // A direct program: the injected test helper has no `setsid`, so a
        // shell would leave its `sleep` child outside the killed group.
        let mut client = start(&["/bin/sleep", "100"], 24, 80);
        // Give the helper time to start before the close.
        thread::sleep(Duration::from_millis(300));
        client.send(HostFrame::Close);
        // 128 + SIGHUP.
        assert_eq!(client.finish(), 129);
    }

    #[test]
    fn a_missing_program_prints_not_found_and_exits_127() {
        let mut client = start(&["/no/such/program"], 24, 80);
        client.wait_for_output("quoro-init: /no/such/program: not found");
        assert_eq!(client.finish(), 127);
    }

    /// A directory of its own for one test, removed when dropped.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("quoro-init-sync-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap_or_else(|err| panic!("mkdir: {err}"));
            Self(path)
        }

        fn path(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A sync session over a socket pair: sends the sync and reads until the
    /// guest closes. Returns the `SYNCED` flag and the `ERROR` message.
    fn sync(volumes: &[(&str, &str)], first: &[u8]) -> (bool, Option<String>, Arc<Context>) {
        let (host, guest) = UnixStream::pair().unwrap_or_else(|err| panic!("pair: {err}"));
        let mut context = test_context();
        context.volumes = volumes
            .iter()
            .map(|(serial, path)| ((*serial).to_string(), (*path).to_string()))
            .collect();
        let context = Arc::new(context);
        let shared = Arc::clone(&context);
        let helper = test_helper();
        let served = thread::spawn(move || serve(guest, &shared, &helper, Wait::Direct));
        let mut client = Client::new(host);
        client.send_raw(first);
        while client.pump() {}
        served
            .join()
            .unwrap_or_else(|_| panic!("the session panicked"));
        assert!(client.output.is_empty(), "a sync session writes no output");
        assert_eq!(client.exit, None, "a sync session runs no program");
        (client.synced, client.error, context)
    }

    fn sync_frame(serials: &[&str]) -> Vec<u8> {
        HostFrame::Sync(SyncRequest {
            serials: serials.iter().map(|serial| (*serial).to_string()).collect(),
        })
        .encode()
        .unwrap_or_else(|err| panic!("encode: {err}"))
    }

    #[test]
    fn a_sync_session_flushes_each_named_volume_and_answers_synced() {
        let data = TempDir::new("data");
        let uploads = TempDir::new("uploads");
        let (synced, error, context) = sync(
            &[
                ("quoro-vol-0", &data.path()),
                ("quoro-vol-1", &uploads.path()),
            ],
            &sync_frame(&["quoro-vol-1", "quoro-vol-0"]),
        );
        assert!(synced, "error: {error:?}");
        assert_eq!(error, None);
        assert_eq!(
            context
                .sync_failures
                .lock()
                .map(|failures| failures.len())
                .ok(),
            Some(0)
        );
    }

    #[test]
    fn a_sync_session_reads_nothing_after_the_sync() {
        let data = TempDir::new("after");
        let mut first = sync_frame(&["quoro-vol-0"]);
        first.extend(
            HostFrame::Input(b"ignored".to_vec())
                .encode()
                .unwrap_or_default(),
        );
        let (synced, error, _) = sync(&[("quoro-vol-0", &data.path())], &first);
        assert!(synced, "error: {error:?}");
    }

    #[test]
    fn a_sync_of_a_volume_the_configuration_lacks_is_an_error() {
        let data = TempDir::new("lacks");
        let (synced, error, context) = sync(
            &[("quoro-vol-0", &data.path())],
            &sync_frame(&["quoro-vol-0", "quoro-vol-7"]),
        );
        assert!(!synced);
        let message = error.unwrap_or_default();
        assert!(message.contains("quoro-vol-7"), "{message:?}");
        assert_eq!(
            context
                .sync_failures
                .lock()
                .map(|failures| failures.get("quoro-vol-7").cloned())
                .ok(),
            Some(Some(message)),
            "the failure is recorded against the volume that failed"
        );
    }

    #[test]
    fn a_sync_whose_mount_path_is_gone_is_an_error() {
        let gone = TempDir::new("gone");
        let path = gone.path();
        drop(gone);
        let (synced, error, _) = sync(&[("quoro-vol-0", &path)], &sync_frame(&["quoro-vol-0"]));
        assert!(!synced);
        let message = error.unwrap_or_default();
        assert!(message.contains(&path), "{message:?}");
    }

    #[test]
    fn a_malformed_sync_is_an_error() {
        let mut empty = vec![SYNC];
        let payload = br#"{"serials":[]}"#;
        empty.extend_from_slice(&u32::try_from(payload.len()).unwrap_or(0).to_be_bytes());
        empty.extend_from_slice(payload);
        let (synced, error, _) = sync(&[], &empty);
        assert!(!synced);
        assert!(error.is_some());
    }

    #[test]
    fn a_sync_after_the_open_is_an_error() {
        let mut client = start(&["/bin/sh", "-c", "read line; echo never"], 24, 80);
        client.send(HostFrame::Sync(SyncRequest {
            serials: vec!["quoro-vol-0".to_string()],
        }));
        let message = client.finish_error();
        assert!(message.contains("first frame"), "{message:?}");
    }

    #[test]
    fn a_failing_sync_is_logged_once_per_volume_and_reason() {
        let context = test_context();
        let eio = "sync: syncfs /data of quoro-vol-0: EIO";
        let gone = "sync: open /srv of quoro-vol-1: ENOENT";
        assert!(context.note_sync("quoro-vol-0", Some(eio)));
        assert!(
            !context.note_sync("quoro-vol-0", Some(eio)),
            "the same reason again is not logged"
        );
        // Another volume's success and failure, alternating as warm cycles of
        // two volumes would, log only each volume's first failure.
        assert!(
            !context.note_sync("quoro-vol-2", None),
            "a success is never logged"
        );
        assert!(context.note_sync("quoro-vol-1", Some(gone)));
        assert!(!context.note_sync("quoro-vol-2", None));
        assert!(!context.note_sync("quoro-vol-0", Some(eio)));
        assert!(!context.note_sync("quoro-vol-1", Some(gone)));
        assert!(
            context.note_sync(
                "quoro-vol-0",
                Some("sync: syncfs /data of quoro-vol-0: EROFS")
            ),
            "a new reason is logged"
        );
        assert!(!context.note_sync("quoro-vol-0", None));
        assert!(
            context.note_sync("quoro-vol-0", Some(eio)),
            "a failure after that volume's success is logged again"
        );
    }

    #[test]
    fn a_bad_first_frame_is_an_error() {
        let (host, guest) = UnixStream::pair().unwrap_or_else(|err| panic!("pair: {err}"));
        let context = test_context();
        let helper = test_helper();
        thread::spawn(move || serve(guest, &context, &helper, Wait::Direct));
        let mut client = Client::new(host);
        client.send(HostFrame::Input(b"too early".to_vec()));
        let message = client.finish_error();
        assert!(message.contains("first frame must be open"), "{message:?}");
    }

    #[test]
    fn an_oversized_first_frame_is_an_error() {
        let (host, guest) = UnixStream::pair().unwrap_or_else(|err| panic!("pair: {err}"));
        let context = test_context();
        let helper = test_helper();
        thread::spawn(move || serve(guest, &context, &helper, Wait::Direct));
        let mut client = Client::new(host);
        // `OPEN` announcing one byte more than the payload limit.
        let mut header = vec![OPEN];
        header.extend_from_slice(&u32::try_from(MAX_PAYLOAD + 1).unwrap().to_be_bytes());
        client.send_raw(&header);
        assert!(client.finish_error().contains("more than"));
    }

    #[test]
    fn an_unknown_user_is_an_error() {
        let (host, guest) = UnixStream::pair().unwrap_or_else(|err| panic!("pair: {err}"));
        let context = test_context();
        let helper = test_helper();
        thread::spawn(move || serve(guest, &context, &helper, Wait::Direct));
        let mut client = Client::new(host);
        client.send(HostFrame::Open(OpenRequest {
            argv: vec!["/bin/sh".to_string()],
            user: Some("quoro-no-such-user".to_string()),
            rows: 24,
            cols: 80,
        }));
        assert!(client.finish_error().contains("no such user"));
    }

    #[test]
    fn a_second_open_is_refused() {
        let mut client = start(&["/bin/sh", "-c", "sleep 100"], 24, 80);
        client.send(HostFrame::Open(OpenRequest {
            argv: vec!["/bin/sh".to_string()],
            user: None,
            rows: 24,
            cols: 80,
        }));
        assert!(client.finish_error().contains("only once"));
    }

    #[test]
    fn the_environment_names_the_terminal() {
        let mut client = start(&["/bin/sh", "-c", "echo TERM=$TERM APP=$APP"], 24, 80);
        client.wait_for_output("TERM=xterm-256color APP=yes");
        assert_eq!(client.finish(), 0);
    }
}
