// SPDX-License-Identifier: AGPL-3.0-only
//! `quoro-init`: PID 1 in a Quoro microVM. See `crates/init/README.md`.

#[cfg(target_os = "linux")]
mod guest;

#[cfg(target_os = "linux")]
fn main() {
    guest::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("quoro-init only runs as PID 1 inside a Linux microVM");
    std::process::exit(1);
}
