// SPDX-License-Identifier: AGPL-3.0-only
//! Reporting back to the engine and powering the guest off.
//!
//! Both endings — the app exited, or the boot failed — write the status record
//! into the config disk, flush the filesystems and ask the kernel to power the
//! machine off. Nothing after `main` is allowed to return, so the last stop is
//! a park loop.

use std::fs::File;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use quoro_init::AppStatus;
use rustix::system::RebootCommand;

use crate::guest::{diag, disks};

/// The config disk, once it has been found. Every FATAL after that point is
/// reported through it.
static CONFIG_DEVICE: OnceLock<Mutex<Option<File>>> = OnceLock::new();

fn state() -> &'static Mutex<Option<File>> {
    CONFIG_DEVICE.get_or_init(|| Mutex::new(None))
}

/// Hand the config disk to the shutdown path. Called as soon as the disk is
/// open, before anything that can fail.
pub(crate) fn remember_config_device(file: File) {
    if let Ok(mut slot) = state().lock() {
        *slot = Some(file);
    }
}

/// Write the status record, if there is a config disk to write it to.
fn write_status(status: &AppStatus) {
    let Ok(slot) = state().lock() else {
        diag::log("status: config disk lock poisoned");
        return;
    };
    let Some(file) = slot.as_ref() else {
        diag::log("status: no config disk; nothing to report");
        return;
    };
    if let Err(err) = disks::write_status(file, status) {
        diag::log(&format!("status: write failed: {err}"));
    }
}

/// The app exited with `code`: report it and power off.
pub(crate) fn exited(code: i32) -> ! {
    diag::log(&format!("app exited code={code}"));
    write_status(&AppStatus::Exited { code });
    power_off()
}

/// The boot failed: report it and power off.
pub(crate) fn fatal(code: i32, message: &str) -> ! {
    diag::log(&format!("FATAL {message}"));
    write_status(&AppStatus::Fatal {
        code,
        message: message.to_string(),
    });
    power_off()
}

/// Flush the filesystems, power the machine off, and park if the kernel
/// refuses. Never returns.
fn power_off() -> ! {
    rustix::fs::sync();
    if let Err(err) = rustix::system::reboot(RebootCommand::PowerOff) {
        diag::log(&format!("reboot(PowerOff) failed: {err}"));
    }
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
