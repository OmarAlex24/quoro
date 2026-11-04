// SPDX-License-Identifier: AGPL-3.0-only
//! The exec helper path, through the real `quoro-init` binary.
//!
//! `cargo test` builds `quoro-init` for its unit tests, but the unit-test
//! process is libtest, not the init, so the helper's `run_helper_if_requested`
//! is unreachable from `#[cfg(test)]` code in the binary. This integration
//! test runs the built binary with the helper argument on a real PTY, which
//! exercises `setsid`, `TIOCSCTTY`, `argv[0]` and the `exec`-failure path
//! without a microVM. The session relay itself is tested in `guest/exec`.

#![cfg(target_os = "linux")]

use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use rustix::fs::{Mode, OFlags};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};
use rustix::termios::{Winsize, tcsetwinsize};

/// A PTY master and the path of its slave. The slave is opened with `NOCTTY`
/// so the test process never takes it as its own controlling terminal.
fn pty() -> (File, PathBuf) {
    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY | OpenptFlags::CLOEXEC)
        .unwrap_or_else(|err| panic!("openpt: {err}"));
    grantpt(&master).unwrap_or_else(|err| panic!("grantpt: {err}"));
    unlockpt(&master).unwrap_or_else(|err| panic!("unlockpt: {err}"));
    let name = ptsname(&master, Vec::new()).unwrap_or_else(|err| panic!("ptsname: {err}"));
    let path = PathBuf::from(std::ffi::OsStr::from_bytes(name.to_bytes()));
    tcsetwinsize(
        &master,
        Winsize {
            ws_row: 24,
            ws_col: 80,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )
    .unwrap_or_else(|err| panic!("winsize: {err}"));
    (File::from(master), path)
}

fn slave(path: &std::path::Path) -> OwnedFd {
    rustix::fs::open(
        path,
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap_or_else(|err| panic!("open slave: {err}"))
}

/// Run the helper with `program` and `args`, all three stdio fds on the PTY,
/// and return what the PTY produced.
fn run_helper(program: &str, arg0: &str, args: &[&str]) -> (std::process::ExitStatus, String) {
    let (mut master, path) = pty();
    let stdin = slave(&path);
    let stdout = slave(&path);
    let stderr = slave(&path);

    let mut command = Command::new(env!("CARGO_BIN_EXE_quoro-init"));
    command
        .arg("--quoro-exec-helper")
        .arg(program)
        .arg(arg0)
        .args(args);
    command.stdin(Stdio::from(stdin));
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));

    let mut child = command
        .spawn()
        .unwrap_or_else(|err| panic!("spawn quoro-init as helper: {err}"));
    // The command still owns the slave descriptors it passed to the child.
    drop(command);
    let status = child
        .wait()
        .unwrap_or_else(|err| panic!("wait for quoro-init: {err}"));

    let mut output = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match master.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => output.extend_from_slice(&chunk[..read]),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            // EIO is the normal end of a PTY whose slave is closed.
            Err(_) => break,
        }
    }
    (status, String::from_utf8_lossy(&output).into_owned())
}

#[test]
fn the_helper_becomes_the_program_on_its_controlling_terminal() {
    let (status, output) = run_helper("/bin/sh", "sh", &["-c", "tty; echo \"argv0=$0\"; exit 7"]);
    assert_eq!(status.code(), Some(7), "output: {output:?}");
    assert!(output.contains("/dev/pts/"), "output: {output:?}");
    assert!(output.contains("argv0=sh"), "output: {output:?}");
}

#[test]
fn a_program_that_cannot_be_executed_reports_127() {
    let (status, output) = run_helper("/no/such/program", "missing", &[]);
    assert_eq!(status.code(), Some(127), "output: {output:?}");
    assert!(
        output.contains("quoro-init: exec /no/such/program:"),
        "output: {output:?}"
    );
}
