// SPDX-License-Identifier: AGPL-3.0-only
//! The Linux PID 1. See `crates/init/README.md` for the boot sequence, the
//! config disk layout and the console protocol.
//!
//! Everything here funnels into [`shutdown::exited`] or [`shutdown::fatal`]:
//! the process never returns from `main` and never panics on a boot we can
//! report instead. There is no `unsafe` in this crate; the syscalls `std` does
//! not wrap (mount, chroot, reboot, netlink, waiting on any child) go through
//! `rustix`.

mod app;
mod console;
mod diag;
mod disks;
mod exec;
mod mounts;
mod netlink;
mod power;
mod shutdown;
mod teardown;
mod user;

use std::path::Path;
use std::time::Duration;

use quoro_init::{GuestConfig, SERIAL_CONFIG, SERIAL_DATA, SERIAL_ROOT};
use rustix::io::Errno;
use rustix::process::{Pid, WaitOptions, WaitStatus};

/// Entry point of the guest init. Never returns: it powers the guest off.
pub fn main() {
    // The helper re-exec of an exec session must not touch any PID-1 state.
    exec::run_helper_if_requested();
    match run() {
        Ok(code) => shutdown::exited(code),
        Err(fault) => shutdown::fatal(fault.code, &fault.message),
    }
}

/// A boot failure: the status code the engine will see, plus the message that
/// goes into the status record and the serial log.
pub(crate) struct Fault {
    /// `127` when the entrypoint was missing, `1` otherwise.
    pub(crate) code: i32,
    pub(crate) message: String,
}

impl Fault {
    /// A generic boot failure (`exit=1` in the status record).
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }

    /// The entrypoint was not found or not executable (`exit=127`).
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: 127,
            message: message.into(),
        }
    }
}

/// The whole boot, up to (and including) the app's exit. The returned code is
/// what the status record reports: the app's exit status, or `128 + signal`.
fn run() -> Result<i32, Fault> {
    diag::open_console();
    diag::log("booting");
    mounts::mount_early()?;
    if diag::open_console() {
        diag::log("diagnostics console ready");
    }

    // Everything below depends on the three disks: the config to know what to
    // run, the root to run it from, the data disk for the overlay's upper.
    let found = disks::wait_for_serials();
    let config_device = found.config.clone().ok_or_else(|| {
        Fault::new(format!(
            "no {SERIAL_CONFIG} disk found within {} ms",
            disks::SERIAL_WAIT.as_millis()
        ))
    })?;
    // Remember it before anything else can fail, so every later FATAL can be
    // written back to the engine.
    shutdown::remember_config_device(disks::open_config_device(&config_device)?);
    let config = disks::read_config(&config_device)?;
    diag::log(&format!(
        "config: {} argument(s), {} env entries",
        config.args.len(),
        config.env.len()
    ));

    let root_device = found
        .root
        .ok_or_else(|| Fault::new(format!("no {SERIAL_ROOT} disk found within 5 s")))?;
    let data_device = found
        .data
        .ok_or_else(|| Fault::new(format!("no {SERIAL_DATA} disk found within 5 s")))?;
    // Resolve the volume disks before the switch: the device nodes under
    // `/dev` are carried into the app's root, and a missing volume must fail
    // the boot rather than leave the app without its data.
    let volume_devices = disks::wait_for_volumes(&config.volumes)?;

    mounts::mount_root(&root_device, &data_device)?;
    diag::log("root: overlay ready");
    mounts::switch_root()?;
    diag::log("root: switched to the overlay");
    mounts::mount_late()?;
    mounts::mount_volumes(&volume_devices)?;

    configure_system(&config)?;

    if let Some(net) = config.net.as_ref() {
        netlink::configure(net)?;
        diag::log(&format!(
            "network: {}/{} via {}",
            net.address, net.prefix_len, net.gateway
        ));
    }

    let identity = user::resolve(config.user.as_deref())?;
    diag::log(&format!(
        "user: uid={} gid={} home={}",
        identity.uid, identity.gid, identity.home
    ));

    let entrypoint = config.args.first().cloned().unwrap_or_default();
    let path = app::path_env(&config.env);
    let program = app::resolve_program(&entrypoint, &path).ok_or_else(|| {
        Fault::not_found(format!(
            "entrypoint {entrypoint:?} not found or not executable in PATH {path:?}"
        ))
    })?;
    app::prepare_workdir(config.workdir.as_deref())?;

    // The web terminal's listener: after the switch into the app's root, the
    // network and the user, and before the app. A failure to listen only
    // costs the terminal, never the boot.
    if let Some(port) = config.exec_port {
        exec::start(port, &config, &identity);
    }

    let log = console::open_log(config.log_console);
    let mut child = app::spawn(&config, &identity, &program)?;
    diag::log(&format!("app started pid={}", child.id()));
    let app_pid = Pid::from_raw(match i32::try_from(child.id()) {
        Ok(raw) => raw,
        Err(_) => return Err(Fault::new("app pid out of range")),
    })
    .ok_or_else(|| Fault::new("app pid is 0"))?;

    let pumps = console::spawn_pumps(&mut child, &log);
    power::spawn(app_pid, config.stop_timeout_ms);

    Ok(supervise(app_pid, pumps))
}

/// Hostname, `/etc/hosts` and `/etc/resolv.conf`, inside the new root.
fn configure_system(config: &GuestConfig) -> Result<(), Fault> {
    if let Some(name) = config.hostname.as_ref() {
        rustix::system::sethostname(name.as_bytes())
            .map_err(|err| Fault::new(format!("sethostname {name:?}: {err}")))?;
        diag::log(&format!("hostname set to {name}"));
    }

    let mut hosts = String::from("127.0.0.1 localhost\n::1 localhost\n");
    if let (Some(name), Some(net)) = (config.hostname.as_ref(), config.net.as_ref()) {
        hosts.push_str(&format!("{} {name}\n", net.address));
    }
    write_fresh("/etc/hosts", &hosts)?;

    if !config.dns.is_empty() {
        let mut resolv = String::new();
        for server in &config.dns {
            resolv.push_str(&format!("nameserver {server}\n"));
        }
        write_fresh("/etc/resolv.conf", &resolv)?;
    }
    Ok(())
}

/// Write a file, unlinking whatever was there first so a symlink in the image
/// is never written through. `/etc` is created when the image lacks it.
fn write_fresh(path: &str, contents: &str) -> Result<(), Fault> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| Fault::new(format!("mkdir {}: {err}", parent.display())))?;
    }
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(Fault::new(format!("unlink {path}: {err}"))),
    }
    // `create_new` so a racing symlink cannot redirect the write.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| Fault::new(format!("create {path}: {err}")))?;
    {
        use std::io::Write as _;
        let mut file = file;
        file.write_all(contents.as_bytes())
            .map_err(|err| Fault::new(format!("write {path}: {err}")))?;
        file.sync_all()
            .map_err(|err| Fault::new(format!("sync {path}: {err}")))?;
    }
    Ok(())
}

/// Wait for the app to exit, reaping every other child on the way, then tear
/// the guest down and return the app's exit code.
fn supervise(app_pid: Pid, pumps: Option<console::Pumps>) -> i32 {
    let mut code: Option<i32> = None;
    loop {
        match rustix::process::wait(WaitOptions::empty()) {
            Ok(Some((pid, status))) => {
                if pid == app_pid {
                    if code.is_none() {
                        code = Some(exit_code(status));
                        break;
                    }
                } else {
                    // An exec session's helper; its thread waits on the status.
                    exec::reaped(pid, status);
                }
            }
            Ok(None) => break,
            Err(Errno::INTR) => continue,
            Err(err) => {
                diag::log(&format!("wait: {err}"));
                break;
            }
        }
    }

    let code = match code {
        Some(code) => code,
        None => {
            diag::log("app exited without a status");
            1
        }
    };

    // Nobody may outlive the app: kill its group, give the output pumps their
    // two seconds, then kill whatever is left.
    let _ = rustix::process::kill_process_group(app_pid, rustix::process::Signal::KILL);
    if let Some(pumps) = pumps {
        pumps.wait(Duration::from_secs(2));
    }
    teardown::signal_everyone(rustix::process::Signal::KILL);
    teardown::reap_remaining(Duration::from_secs(1));
    code
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
