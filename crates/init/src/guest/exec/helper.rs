// SPDX-License-Identifier: AGPL-3.0-only
//! The session helper: the re-executed copy of `quoro-init` that becomes the
//! program of one exec session.
//!
//! `std::os::unix::process::CommandExt::pre_exec` is the usual place to call
//! `setsid` and take a controlling terminal between `fork` and `exec`, but it
//! is `unsafe` and the workspace forbids `unsafe`. The safe path is to spawn
//! `/proc/self/exe` with [`HELPER_ARG`] as its first argument: this module
//! sees that argument before any PID-1 work, calls `setsid`, makes fd 0 (the
//! PTY slave) the controlling terminal, and replaces itself with the program
//! through the safe `CommandExt::exec`.
//!
//! `/proc/self/exe` keeps working after `switch_root`: the initramfs is moved
//! aside but never unmounted (`mounts.rs`, "No `pivot_root`" in the README),
//! and the magic link resolves to the binary's inode, not to a path in the
//! new root.
//!
//! Arguments are `quoro-init --quoro-exec-helper <program> <arg0> <arg>...`.
//! The `<arg0>` slot lets the session keep the argv[0] the host asked for.

use std::ffi::OsString;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::Command;

/// First argument of the helper re-exec.
pub(super) const HELPER_ARG: &str = "--quoro-exec-helper";

/// Run the helper when this process was re-executed as one. Returns when the
/// arguments do not ask for it, which is the normal PID-1 boot.
///
/// A malformed helper invocation exits 127 without doing any PID-1 work.
pub(crate) fn run_helper_if_requested() {
    let mut args = std::env::args_os();
    let _ = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new(HELPER_ARG)) {
        return;
    }
    let Some(program) = args.next() else {
        fail("helper: missing program");
    };
    let Some(arg0) = args.next() else {
        fail("helper: missing argv[0]");
    };
    let rest: Vec<OsString> = args.collect();
    exec_program(program, arg0, rest);
}

/// The helper body. Never returns: it either `exec`s the program or exits 127.
fn exec_program(program: OsString, arg0: OsString, args: Vec<OsString>) -> ! {
    if let Err(err) = rustix::process::setsid() {
        fail(&format!("helper: setsid: {err}"));
    }
    if let Err(err) = rustix::process::ioctl_tiocsctty(std::io::stdin()) {
        fail(&format!(
            "helper: cannot take the controlling terminal: {err}"
        ));
    }

    let err = Command::new(&program).arg0(&arg0).args(&args).exec();
    fail(&format!("exec {}: {err}", Path::new(&program).display()))
}

/// The one error report of the helper: a line on stderr (the PTY, so the host
/// receives it as `OUTPUT`) and exit 127.
fn fail(message: &str) -> ! {
    eprintln!("quoro-init: {message}");
    std::process::exit(127);
}
