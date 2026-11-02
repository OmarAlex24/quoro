// SPDX-License-Identifier: AGPL-3.0-only
//! `quoro-init`: static PID 1 for the Quoro OCI microVM spike.
//!
//! What it does, in order (see `../../config-format.md` for the config file and
//! the console protocol, which this file implements literally):
//!
//! 1. open `/dev/ttyS0` for diagnostics (fd 2 as fallback),
//! 2. mount `/proc`, `/sys`, `/dev`, then tmpfs on `/tmp` and `/run`,
//! 3. parse `/quoro/config`,
//! 4. set the hostname,
//! 5. print the `QUORO_SPIKE_BOOT_OK` marker with the guest uptime,
//! 6. run the app with its stdout/stderr on two pipes, tagging each line
//!    `out `/`err ` onto `/dev/hvc0`,
//! 7. reap, forward termination signals, and power off with
//!    `QUORO_SPIKE_APP_EXIT`.
//!
//! Deliberate constraints: one dependency (`libc`), no panicking paths in the
//! boot flow (everything funnels into `fatal()`), and every `unsafe` block
//! carries a `SAFETY:` note saying why it is sound. Single-threaded throughout,
//! which is what makes the post-`fork()` allocation and the `static mut`-free
//! fd handling sound.

mod config;

use std::ffi::CString;
use std::sync::atomic::{AtomicI32, Ordering};

use config::{parse_config, Config};

/// Where the pipeline writes the app description.
const CONFIG_PATH: &str = "/quoro/config";
/// Kernel console; diagnostics and markers go here.
const SERIAL_PATH: &str = "/dev/ttyS0";
/// virtio-console; the app's tagged output goes here.
const APP_TTY_PATH: &str = "/dev/hvc0";
/// PATH used to resolve `arg[0]` when the config env sets none.
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
/// Grace period between forwarding SIGTERM and sending SIGKILL.
const KILL_GRACE_MS: u64 = 10_000;
/// How long to keep reading pipes after the app exits (grandchildren may hold
/// them open) before powering off anyway.
const EXIT_GRACE_MS: u64 = 2_000;

/// Serial console fd used by `log`/`fatal`. Starts at 2 (the kernel hands PID 1
/// a console on fd 0/1/2) and is replaced once `/dev/ttyS0` opens.
static SERIAL_FD: AtomicI32 = AtomicI32::new(2);

fn main() {
    if let Err(msg) = run() {
        fatal(&msg);
    }
}

// ---------------------------------------------------------------------------
// Boot flow
// ---------------------------------------------------------------------------

fn run() -> Result<(), String> {
    // 1. Diagnostics channel. Before devtmpfs is mounted `/dev/ttyS0` may not
    //    exist, in which case we keep the inherited fd 2 and retry in step 2.
    let mut have_serial = match open_path(SERIAL_PATH, libc::O_WRONLY | libc::O_NOCTTY) {
        Ok(fd) => {
            SERIAL_FD.store(fd, Ordering::Relaxed);
            true
        }
        Err(_) => false,
    };

    // 2. Filesystems. proc/sys/dev first: everything below needs them.
    mount_fs("proc", "/proc", "proc", 0, true)?;
    mount_fs("sysfs", "/sys", "sysfs", 0, true)?;
    mount_fs("devtmpfs", "/dev", "devtmpfs", 0, true)?;

    for dir in ["/tmp", "/run"] {
        // Only mount where the image already has the directory; do not invent
        // mount points in the app's filesystem.
        if path_exists(dir) {
            mount_fs("tmpfs", dir, "tmpfs", 0, false)?;
        }
    }

    // devtmpfs may have just materialised /dev/ttyS0; retry the console.
    if !have_serial {
        if let Ok(fd) = open_path(SERIAL_PATH, libc::O_WRONLY | libc::O_NOCTTY) {
            SERIAL_FD.store(fd, Ordering::Relaxed);
            have_serial = true;
        }
    }
    if !have_serial {
        log(&format!(
            "cannot open {SERIAL_PATH}; falling back to inherited fd 2"
        ));
    }
    log("mounts ready");

    // 3. Config.
    let raw = read_file(CONFIG_PATH)?;
    let text = String::from_utf8(raw)
        .map_err(|_| format!("{CONFIG_PATH} is not valid UTF-8"))?;
    let cfg = parse_config(&text).map_err(|e| format!("config {CONFIG_PATH}: {e}"))?;
    log(&format!(
        "config parsed: {} arg(s), {} env entr{}, uid={}, gid={}, workdir={}",
        cfg.args.len(),
        cfg.env.len(),
        if cfg.env.len() == 1 { "y" } else { "ies" },
        cfg.uid,
        cfg.gid,
        cfg.workdir
    ));

    // 4. Hostname, if the image asked for one.
    if let Some(name) = &cfg.hostname {
        set_hostname(name)?;
        log(&format!("hostname set to {name}"));
    }

    // 5. Boot marker, after mounts and config: the spike waits for this line.
    let uptime_ms = read_uptime_ms()?;
    log(&format!("QUORO_SPIKE_BOOT_OK uptime_ms={uptime_ms}"));

    // 6. App output channel.
    let app_tty = match open_path(APP_TTY_PATH, libc::O_WRONLY | libc::O_NOCTTY) {
        Ok(fd) => fd,
        Err(e) => {
            log(&format!(
                "cannot open {APP_TTY_PATH} ({e}); app output goes to the serial console"
            ));
            serial_fd()
        }
    };

    // 7. Signals, pipes, child. Block first so the child cannot slip a SIGCHLD
    //    in between fork() and the signalfd being polled.
    let sigmask = block_signals()?;
    let sigfd = make_signalfd(&sigmask)?;

    let (out_r, out_w) = make_pipe()?;
    let (err_r, err_w) = make_pipe()?;

    // SAFETY: fork() takes no arguments and cannot corrupt memory; the process
    // is single-threaded, so the child only sees the state compiled above and
    // nothing else can hold a lock across the call.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let e = errno();
        return Err(format!("fork: {}", errno_name(e)));
    }
    if pid == 0 {
        // Child never returns.
        child_exec(&cfg, out_w, err_w, out_r, err_r, sigfd);
    }
    let app_pid = pid;

    // Parent: the child owns the write ends now; close ours so EOF arrives.
    // SAFETY: these are plain fd numbers we own and will not use again.
    unsafe {
        libc::close(out_w);
        libc::close(err_w);
    }
    // Read ends are non-blocking so `drain` can loop to EAGAIN; the child's
    // dup'ed copies are unaffected because it already duplicated them.
    set_nonblocking(out_r)?;
    set_nonblocking(err_r)?;

    log(&format!("app started: pid={app_pid}, program={}", cfg.args[0]));

    // 8-9. Supervise.
    supervise(app_pid, out_r, err_r, sigfd, app_tty)
}

/// Parent loop: pump the pipes onto hvc0, reap, forward signals, decide when
/// the app is done, then print the exit marker and power off.
fn supervise(
    app_pid: i32,
    out_r: i32,
    err_r: i32,
    sigfd: i32,
    app_tty: i32,
) -> Result<(), String> {
    let mut out_open = true;
    let mut err_open = true;
    let mut out_line: Vec<u8> = Vec::new();
    let mut err_line: Vec<u8> = Vec::new();
    let mut app_status: Option<i32> = None;
    let mut app_exited_at: Option<u64> = None;
    let mut kill_at: Option<u64> = None;

    loop {
        let now = now_ms();

        // 9. Done when the app is reaped and its pipes are drained, or once the
        //    grace window after its exit has elapsed.
        if let Some(exit_at) = app_exited_at {
            let grace_over = now >= exit_at.saturating_add(EXIT_GRACE_MS);
            if (!out_open && !err_open) || grace_over {
                if grace_over && (out_open || err_open) {
                    // Final non-blocking attempt to flush what is already there;
                    // the stream state no longer matters, we are about to exit.
                    let _ = drain(out_r, "out ", &mut out_line, app_tty);
                    let _ = drain(err_r, "err ", &mut err_line, app_tty);
                    log("app exit grace elapsed; closing output pipes");
                }
                break;
            }
        }

        // Escalate an unanswered SIGTERM.
        if let Some(deadline) = kill_at {
            if now >= deadline {
                if app_status.is_none() {
                    log("app still alive after SIGTERM; sending SIGKILL");
                    kill_group(app_pid, libc::SIGKILL);
                }
                kill_at = None;
            }
        }

        let mut fds = [
            libc::pollfd {
                fd: if out_open { out_r } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if err_open { err_r } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: sigfd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];

        let timeout = poll_timeout(now, kill_at, app_exited_at);
        // SAFETY: `fds` is a live, correctly sized array of pollfd for the
        // duration of the call; poll only writes revents into it.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if rc < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            return Err(format!("poll: {}", errno_name(e)));
        }
        if rc == 0 {
            continue;
        }

        if fds[0].revents != 0 {
            out_open = drain(out_r, "out ", &mut out_line, app_tty);
        }
        if fds[1].revents != 0 {
            err_open = drain(err_r, "err ", &mut err_line, app_tty);
        }
        if fds[2].revents != 0 {
            for sig in read_signals(sigfd)? {
                match sig {
                    libc::SIGCHLD => reap(app_pid, &mut app_status, &mut app_exited_at),
                    libc::SIGTERM | libc::SIGINT | libc::SIGHUP => {
                        log(&format!(
                            "got signal {sig}, forwarding SIGTERM to app process group"
                        ));
                        if app_status.is_none() {
                            kill_group(app_pid, libc::SIGTERM);
                            if kill_at.is_none() {
                                kill_at = Some(now_ms().saturating_add(KILL_GRACE_MS));
                            }
                        }
                    }
                    other => log(&format!("ignoring unexpected signal {other}")),
                }
            }
        }
    }

    // 9. Exit marker, then power off. PID 1 is going away, so the VM ends here.
    let status = app_status.ok_or_else(|| "app reaped without a status".to_string())?;
    log(&app_exit_marker(status));
    // SAFETY: sync() and reboot() take no pointers and are the documented way
    // for PID 1 to flush filesystems and power the machine off.
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    Err("reboot(RB_POWER_OFF) returned".to_string())
}

/// Format the protocol's exit marker from a waitpid status.
fn app_exit_marker(status: i32) -> String {
    if libc::WIFEXITED(status) {
        format!("QUORO_SPIKE_APP_EXIT code={}", libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        format!("QUORO_SPIKE_APP_EXIT signal={}", libc::WTERMSIG(status))
    } else {
        format!("QUORO_SPIKE_APP_EXIT code=unknown status={status}")
    }
}

// ---------------------------------------------------------------------------
// Child (the app)
// ---------------------------------------------------------------------------

/// Everything the child does before execve. Returns only by way of `_exit(127)`
/// or the exec: `set -euo pipefail` in spirit.
fn child_exec(cfg: &Config, out_w: i32, err_w: i32, out_r: i32, err_r: i32, sigfd: i32) -> ! {
    // Back to the default mask: the app must not inherit our blocked SIGTERM.
    // SAFETY: `empty` is a zeroed sigset_t that sigemptyset initialises; the
    // mask pointer is null, which sigprocmask allows.
    unsafe {
        let mut empty: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut empty);
        libc::sigprocmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
    }

    // SAFETY: these fds belong to the child and are never used again here.
    unsafe {
        libc::close(out_r);
        libc::close(err_r);
        libc::close(sigfd);
    }

    // New session so the app is its own process group: the parent can signal
    // the whole group, and the app cannot reach our terminal.
    // SAFETY: setsid() takes no arguments and only affects this process.
    if unsafe { libc::setsid() } < 0 {
        child_fatal("setsid failed");
    }

    // Plain dup2: the copies land on 1/2 without O_CLOEXEC, so the exec'd
    // program keeps them, while the originals close at exec.
    // SAFETY: dup2 on fds the child owns; the targets are valid destinations.
    unsafe {
        if libc::dup2(out_w, libc::STDOUT_FILENO) < 0 {
            child_fatal("dup2 stdout failed");
        }
        if libc::dup2(err_w, libc::STDERR_FILENO) < 0 {
            child_fatal("dup2 stderr failed");
        }
    }

    match open_path("/dev/null", libc::O_RDONLY) {
        Ok(null) => {
            // SAFETY: same reasoning as above: owned fds, valid destination.
            unsafe {
                if libc::dup2(null, libc::STDIN_FILENO) < 0 {
                    child_fatal("dup2 stdin failed");
                }
                libc::close(null);
            }
        }
        Err(e) => child_fatal(&format!("/dev/null: {e}")),
    }

    // Drop supplementary groups before changing identity, otherwise a
    // setuid() to a non-root user would keep root's group list.
    // SAFETY: a null list with length 0 is the documented way to clear groups.
    if unsafe { libc::setgroups(0, std::ptr::null()) } < 0 {
        child_fatal("setgroups failed");
    }
    // SAFETY: gid/uid are validated u32 values from the config.
    if unsafe { libc::setgid(cfg.gid) } < 0 {
        child_fatal("setgid failed");
    }
    // SAFETY: same as setgid; order matters, gid first while still privileged.
    if unsafe { libc::setuid(cfg.uid) } < 0 {
        child_fatal("setuid failed");
    }

    match chdir(&cfg.workdir) {
        Ok(()) => {}
        Err(e) => child_fatal(&format!("chdir {}: {e}", cfg.workdir)),
    }

    // Resolve after setuid/chdir so PATH lookups see the app's privileges and
    // relative PATH entries resolve against the app's directory.
    let path_env = path_from_env(&cfg.env);
    let program = match resolve_program(&cfg.args[0], &path_env) {
        Some(p) => p,
        None => child_fatal(&format!(
            "cannot find {:?} in PATH {:?}",
            cfg.args[0], path_env
        )),
    };

    let err = exec_app(&program, &cfg.args, &cfg.env);
    child_fatal(&err);
}

/// execve never returns on success; on failure it returns a message.
fn exec_app(program: &str, args: &[String], env: &[String]) -> String {
    let program_c = match CString::new(program) {
        Ok(c) => c,
        Err(_) => return "program path contains a NUL byte".to_string(),
    };
    let args_c = match to_cstrings(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let env_c = match to_cstrings(env) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let mut arg_ptrs: Vec<*const libc::c_char> = args_c.iter().map(|c| c.as_ptr()).collect();
    arg_ptrs.push(std::ptr::null());
    let mut env_ptrs: Vec<*const libc::c_char> = env_c.iter().map(|c| c.as_ptr()).collect();
    env_ptrs.push(std::ptr::null());

    // SAFETY: all pointers reference CStrings (and their backing Vecs) that are
    // alive for the duration of the call, both vectors are null-terminated, and
    // the env list is exactly the config's (no additions, no filtering).
    unsafe {
        libc::execve(program_c.as_ptr(), arg_ptrs.as_ptr(), env_ptrs.as_ptr());
    }
    format!("execve {program}: {}", errno_name(errno()))
}

/// Resolve `arg[0]`: a path with `/` is used as-is, a bare name is searched in
/// PATH and must be executable.
fn resolve_program(program: &str, path_env: &str) -> Option<String> {
    if program.contains('/') {
        // Absolute or explicitly relative: hand it to execve untouched.
        return Some(program.to_string());
    }
    for dir in path_env.split(':') {
        let dir = if dir.is_empty() { "." } else { dir };
        let candidate = format!("{dir}/{program}");
        let Ok(c) = CString::new(candidate.clone()) else {
            continue;
        };
        // SAFETY: `c` is a valid NUL-terminated path for the duration of the call.
        if unsafe { libc::access(c.as_ptr(), libc::X_OK) } == 0 {
            return Some(candidate);
        }
    }
    None
}

/// `PATH` from the config env, or the conventional default.
fn path_from_env(env: &[String]) -> String {
    for entry in env {
        if let Some(value) = entry.strip_prefix("PATH=") {
            return value.to_string();
        }
    }
    DEFAULT_PATH.to_string()
}

// ---------------------------------------------------------------------------
// Signals and pipes
// ---------------------------------------------------------------------------

/// Block the four signals we supervise. Doing this before fork() means the
/// child inherits the mask and the parent cannot lose a SIGCHLD to the default
/// action while it is setting up.
fn block_signals() -> Result<libc::sigset_t, String> {
    // SAFETY: `set` is initialised by sigemptyset/sigaddset before use; the
    // null `oldset` is allowed and we do not want the previous mask.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for sig in [libc::SIGCHLD, libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
            if libc::sigaddset(&mut set, sig) != 0 {
                return Err("sigaddset failed".to_string());
            }
        }
        if libc::sigprocmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) != 0 {
            return Err(format!("sigprocmask: {}", errno_name(errno())));
        }
        Ok(set)
    }
}

/// signalfd for the blocked set: signals arrive as readable events, so one
/// poll() covers pipes and signals without a handler.
fn make_signalfd(set: &libc::sigset_t) -> Result<i32, String> {
    // SAFETY: `set` is a live sigset_t; the returned fd is owned by this
    // process and non-blocking so read_signals can drain to EAGAIN.
    let fd = unsafe {
        libc::signalfd(
            -1,
            set,
            libc::SFD_CLOEXEC | libc::SFD_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(format!("signalfd: {}", errno_name(errno())));
    }
    Ok(fd)
}

/// Drain the signalfd into a list of signal numbers.
fn read_signals(sigfd: i32) -> Result<Vec<i32>, String> {
    let mut sigs = Vec::new();
    loop {
        // SAFETY: zeroed signalfd_siginfo is a valid buffer; the kernel writes
        // at most size_of it into it and we pass exactly that length.
        let mut info: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
        let n = unsafe {
            libc::read(
                sigfd,
                (&mut info as *mut libc::signalfd_siginfo).cast(),
                std::mem::size_of::<libc::signalfd_siginfo>(),
            )
        };
        if n < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            if e == libc::EAGAIN {
                return Ok(sigs);
            }
            return Err(format!("read signalfd: {}", errno_name(e)));
        }
        if n == 0 {
            return Ok(sigs);
        }
        sigs.push(info.ssi_signo as i32);
    }
}

/// Create a CLOEXEC pipe. The read end becomes non-blocking in the parent only.
fn make_pipe() -> Result<(i32, i32), String> {
    let mut fds = [0i32; 2];
    // SAFETY: `fds` is a two-element array, which is exactly what pipe2 writes.
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(format!("pipe2: {}", errno_name(errno())));
    }
    Ok((fds[0], fds[1]))
}

fn set_nonblocking(fd: i32) -> Result<(), String> {
    // SAFETY: F_GETFL/F_SETFL on an fd we own; no pointers involved.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(format!("fcntl F_GETFL: {}", errno_name(errno())));
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(format!("fcntl F_SETFL: {}", errno_name(errno())));
        }
    }
    Ok(())
}

/// Reap every child that is ready. Returns nothing: the caller only needs to
/// know whether the app itself was reaped, which lands in `app_status`.
fn reap(app_pid: i32, app_status: &mut Option<i32>, app_exited_at: &mut Option<u64>) {
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: `status` is a live c_int for the kernel to write into;
        // waitpid(-1, ..., WNOHANG) also collects orphaned grandchildren.
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if pid > 0 {
            if pid == app_pid && app_status.is_none() {
                *app_status = Some(status);
                *app_exited_at = Some(now_ms());
            }
            continue;
        }
        if pid < 0 && errno() == libc::EINTR {
            continue;
        }
        return;
    }
}

/// Signal a process group (negative pid). `app_pid` is a session leader
/// (`setsid()` in the child), so its pgid equals its pid.
fn kill_group(pgid: i32, sig: i32) {
    // SAFETY: kill() takes plain integers; a negative pid means "the group".
    unsafe {
        libc::kill(-pgid, sig);
    }
}

// ---------------------------------------------------------------------------
// Pipe pumping
// ---------------------------------------------------------------------------

/// Read everything available from one pipe, emitting complete lines to the app
/// tty tagged with `tag`. Returns false once the pipe is at EOF (its write end
/// is gone), after flushing any trailing partial line.
fn drain(fd: i32, tag: &str, buf: &mut Vec<u8>, app_tty: i32) -> bool {
    let mut chunk = [0u8; 4096];
    loop {
        // SAFETY: `chunk` is a live buffer of exactly the length passed; read
        // writes at most that many bytes into it.
        let n = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
        if n < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            if e == libc::EAGAIN {
                return true;
            }
            // A real read error ends this stream; report and treat as EOF.
            log(&format!("read {tag}pipe: {}", errno_name(e)));
            return false;
        }
        if n == 0 {
            if !buf.is_empty() {
                emit_line(app_tty, tag, buf);
                buf.clear();
            }
            return false;
        }

        let n = n as usize;
        let mut start = 0;
        for i in 0..n {
            if chunk[i] == b'\n' {
                buf.extend_from_slice(&chunk[start..i]);
                emit_line(app_tty, tag, buf);
                buf.clear();
                start = i + 1;
            }
        }
        buf.extend_from_slice(&chunk[start..n]);
    }
}

/// Write one `tag + line + "\n"` record. The line is written byte-for-byte as
/// the app produced it (minus the newline that delimits it).
fn emit_line(app_tty: i32, tag: &str, line: &[u8]) {
    let mut record = Vec::with_capacity(tag.len() + line.len() + 1);
    record.extend_from_slice(tag.as_bytes());
    record.extend_from_slice(line);
    record.push(b'\n');
    write_all(app_tty, &record);
}

// ---------------------------------------------------------------------------
// Small syscall helpers
// ---------------------------------------------------------------------------

fn serial_fd() -> i32 {
    SERIAL_FD.load(Ordering::Relaxed)
}

/// One `quoro-init: ` diagnostic line on the serial console.
fn log(msg: &str) {
    let line = format!("quoro-init: {msg}\n");
    write_all(serial_fd(), line.as_bytes());
}

/// `quoro-init: FATAL <msg>`, flush, power off. Never returns.
fn fatal(msg: &str) -> ! {
    let line = format!("quoro-init: FATAL {msg}\n");
    write_all(serial_fd(), line.as_bytes());
    power_off("FATAL");
}

/// Child-side fatal: the app failed before exec. The serial fd is inherited.
fn child_fatal(msg: &str) -> ! {
    let line = format!("quoro-init: FATAL child: {msg}\n");
    write_all(serial_fd(), line.as_bytes());
    // SAFETY: _exit is async-signal-safe and terminates this process without
    // running atexit handlers or flushing the app's (inherited) state.
    unsafe { libc::_exit(127) }
}

/// Flush filesystems and power the VM off. If reboot fails there is nothing
/// useful left to do, so park the process rather than return into a half-dead
/// PID 1.
fn power_off(reason: &str) -> ! {
    // SAFETY: sync() and reboot() take no pointers; PID 1 is allowed to call
    // reboot(RB_POWER_OFF) and this is the intended shutdown path.
    unsafe {
        libc::sync();
        if libc::reboot(libc::RB_POWER_OFF) != 0 {
            let e = errno();
            let line = format!("quoro-init: reboot(RB_POWER_OFF) failed ({reason}): {}\n", errno_name(e));
            write_all(serial_fd(), line.as_bytes());
        }
    }
    loop {
        // SAFETY: pause() blocks until a signal; taking no pointers.
        unsafe { libc::pause() };
    }
}

fn errno() -> i32 {
    // SAFETY: __errno_location returns this thread's valid errno slot.
    unsafe { *libc::__errno_location() }
}

fn write_all(fd: i32, mut buf: &[u8]) -> bool {
    while !buf.is_empty() {
        // SAFETY: buf.as_ptr()/len describe a live slice; write only reads it.
        let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
        if n < 0 {
            if errno() == libc::EINTR {
                continue;
            }
            return false;
        }
        if n == 0 {
            return false;
        }
        buf = &buf[n as usize..];
    }
    true
}

fn open_path(path: &str, flags: i32) -> Result<i32, String> {
    let c = cstr(path)?;
    // O_CLOEXEC so the app does not inherit our console fds.
    // SAFETY: `c` is a valid NUL-terminated path; mode is unused without O_CREAT.
    let fd = unsafe { libc::open(c.as_ptr(), flags | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(format!("open {path}: {}", errno_name(errno())));
    }
    Ok(fd)
}

fn read_file(path: &str) -> Result<Vec<u8>, String> {
    let fd = open_path(path, libc::O_RDONLY)?;
    let mut out = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        // SAFETY: `chunk` is a live buffer of the passed length.
        let n = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
        if n < 0 {
            if errno() == libc::EINTR {
                continue;
            }
            let e = errno();
            // SAFETY: closing an fd we own exactly once.
            unsafe { libc::close(fd) };
            return Err(format!("read {path}: {}", errno_name(e)));
        }
        if n == 0 {
            break;
        }
        out.extend_from_slice(&chunk[..n as usize]);
    }
    // SAFETY: closing an fd we own exactly once.
    unsafe { libc::close(fd) };
    Ok(out)
}

fn read_uptime_ms() -> Result<u64, String> {
    let raw = read_file("/proc/uptime")?;
    let text = String::from_utf8_lossy(&raw);
    let first = text
        .split_whitespace()
        .next()
        .ok_or_else(|| "/proc/uptime: empty".to_string())?;
    let secs: f64 = first
        .parse()
        .map_err(|_| format!("/proc/uptime: cannot parse {first:?}"))?;
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("/proc/uptime: implausible value {first:?}"));
    }
    Ok((secs * 1000.0) as u64)
}

fn set_hostname(name: &str) -> Result<(), String> {
    let c = cstr(name)?;
    // SAFETY: `c` is NUL-terminated and the length excludes the NUL, as
    // sethostname requires.
    let rc = unsafe { libc::sethostname(c.as_ptr(), name.len()) };
    if rc != 0 {
        return Err(format!("sethostname {name:?}: {}", errno_name(errno())));
    }
    Ok(())
}

fn chdir(path: &str) -> Result<(), String> {
    let c = cstr(path)?;
    // SAFETY: `c` is a valid NUL-terminated path.
    if unsafe { libc::chdir(c.as_ptr()) } != 0 {
        return Err(format!("chdir: {}", errno_name(errno())));
    }
    Ok(())
}

fn path_exists(path: &str) -> bool {
    match CString::new(path) {
        // SAFETY: valid NUL-terminated path; F_OK only checks existence.
        Ok(c) => (unsafe { libc::access(c.as_ptr(), libc::F_OK) }) == 0,
        Err(_) => false,
    }
}

fn mkdir_if_missing(path: &str) -> Result<(), String> {
    let c = cstr(path)?;
    // SAFETY: valid NUL-terminated path and a literal mode.
    let rc = unsafe { libc::mkdir(c.as_ptr(), 0o755) };
    if rc == 0 {
        return Ok(());
    }
    let e = errno();
    if e == libc::EEXIST {
        return Ok(());
    }
    Err(format!("mkdir {path}: {}", errno_name(e)))
}

/// Mount `fstype` on `target`. `create` makes the mount point first (for the
/// kernel filesystems); EBUSY means it is already mounted, which is fine.
fn mount_fs(
    source: &str,
    target: &str,
    fstype: &str,
    flags: libc::c_ulong,
    create: bool,
) -> Result<(), String> {
    if create {
        mkdir_if_missing(target)?;
    }
    let src = cstr(source)?;
    let tgt = cstr(target)?;
    let fs = cstr(fstype)?;
    // SAFETY: all three pointers are valid NUL-terminated strings that outlive
    // the call; mount takes no ownership and `data` is legitimately null for
    // these filesystems.
    let rc = unsafe {
        libc::mount(
            src.as_ptr(),
            tgt.as_ptr(),
            fs.as_ptr(),
            flags,
            std::ptr::null(),
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let e = errno();
    if e == libc::EBUSY {
        return Ok(());
    }
    Err(format!("mount {fstype} on {target}: {}", errno_name(e)))
}

fn cstr(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("string contains a NUL byte: {s:?}"))
}

fn to_cstrings(items: &[String]) -> Result<Vec<CString>, String> {
    items.iter().map(|s| cstr(s)).collect()
}

fn now_ms() -> u64 {
    // SAFETY: `ts` is a live timespec for the kernel to fill in.
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        // Not worth a fatal: a zero clock only shortens timeouts.
        return 0;
    }
    ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000
}

/// poll() timeout in milliseconds: the nearest deadline, 0 if overdue, -1 for
/// "no deadline, block".
fn poll_timeout(now: u64, kill_at: Option<u64>, app_exited_at: Option<u64>) -> i32 {
    let mut best: Option<u64> = None;
    for deadline in [
        kill_at,
        app_exited_at.map(|t| t.saturating_add(EXIT_GRACE_MS)),
    ]
    .into_iter()
    .flatten()
    {
        best = Some(match best {
            Some(current) => current.min(deadline),
            None => deadline,
        });
    }
    match best {
        None => -1,
        Some(deadline) if deadline <= now => 0,
        Some(deadline) => (deadline - now).min(i32::MAX as u64) as i32,
    }
}

fn errno_name(e: i32) -> String {
    let name = match e {
        libc::EPERM => "EPERM (operation not permitted)",
        libc::ENOENT => "ENOENT (no such file or directory)",
        libc::EACCES => "EACCES (permission denied)",
        libc::EBUSY => "EBUSY (device or resource busy)",
        libc::EEXIST => "EEXIST (file exists)",
        libc::EINVAL => "EINVAL (invalid argument)",
        libc::ENODEV => "ENODEV (no such device)",
        libc::ENOMEM => "ENOMEM (out of memory)",
        libc::ENOTDIR => "ENOTDIR (not a directory)",
        libc::EIO => "EIO (I/O error)",
        _ => return format!("errno {e}"),
    };
    name.to_string()
}
