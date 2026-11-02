// SPDX-License-Identifier: AGPL-3.0-only
//! The power button: the first `KEY_POWER` press stops the app.
//!
//! Cloud Hypervisor's ACPI power button shows up as an evdev device named
//! `Power Button`. A dedicated thread reads its events and turns the first
//! press into `SIGTERM`, then `SIGKILL` after `stop_timeout_ms`. The main
//! thread keeps waiting for the app, so the guest still powers off through the
//! normal exit path.

use std::io::Read as _;
use std::thread;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process_group};

use crate::guest::diag;

/// `EV_KEY`.
const EV_KEY: u16 = 1;
/// `KEY_POWER`.
const KEY_POWER: u16 = 116;
/// `struct input_event` on x86_64: 16-byte `timeval`, then type, code, value.
const EVENT_LEN: usize = 24;

/// Whether one `input_event` record is a power key press (not the release or
/// the autorepeat).
fn is_power_press(record: &[u8; EVENT_LEN]) -> bool {
    let kind = u16::from_ne_bytes([record[16], record[17]]);
    let code = u16::from_ne_bytes([record[18], record[19]]);
    let value = i32::from_ne_bytes([record[20], record[21], record[22], record[23]]);
    kind == EV_KEY && code == KEY_POWER && value == 1
}

/// Find the event device named `Power Button` under `/sys/class/input`.
fn find_power_button() -> Option<String> {
    let entries = std::fs::read_dir("/sys/class/input").ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("event") {
            continue;
        }
        let Ok(device_name) = std::fs::read_to_string(entry.path().join("device/name")) else {
            continue;
        };
        if device_name.trim() == "Power Button" {
            return Some(format!("/dev/input/{name}"));
        }
    }
    None
}

/// Start the power-button thread. Without a device the guest only stops when
/// the engine forces it; that is a warning, not a failure.
pub(crate) fn spawn(app_pid: Pid, stop_timeout_ms: u64) {
    let Some(device) = find_power_button() else {
        diag::log("power button: no such input device; only a forced stop will end this guest");
        return;
    };
    let spawned = thread::Builder::new()
        .name("power".to_string())
        .spawn(move || watch(&device, app_pid, stop_timeout_ms));
    if spawned.is_err() {
        diag::log("power button: cannot start the watcher thread");
    }
}

/// Read events until the button is pressed, then stop the app's group.
fn watch(device: &str, app_pid: Pid, stop_timeout_ms: u64) {
    let Ok(file) = std::fs::File::open(device) else {
        diag::log(&format!("power button: cannot open {device}"));
        return;
    };
    let mut file = file;
    let mut record = [0u8; EVENT_LEN];
    loop {
        if file.read_exact(&mut record).is_err() {
            diag::log("power button: read failed; no longer watching");
            return;
        }
        if is_power_press(&record) {
            break;
        }
    }

    diag::log("power button: stopping app");
    let _ = kill_process_group(app_pid, Signal::TERM);
    thread::sleep(Duration::from_millis(stop_timeout_ms));
    let _ = kill_process_group(app_pid, Signal::KILL);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(kind: u16, code: u16, value: i32) -> [u8; EVENT_LEN] {
        let mut record = [0u8; EVENT_LEN];
        record[16..18].copy_from_slice(&kind.to_ne_bytes());
        record[18..20].copy_from_slice(&code.to_ne_bytes());
        record[20..24].copy_from_slice(&value.to_ne_bytes());
        record
    }

    #[test]
    fn a_press_is_recognised() {
        assert!(is_power_press(&record(EV_KEY, KEY_POWER, 1)));
    }

    #[test]
    fn a_release_or_repeat_is_not() {
        assert!(!is_power_press(&record(EV_KEY, KEY_POWER, 0)));
        assert!(!is_power_press(&record(EV_KEY, KEY_POWER, 2)));
    }

    #[test]
    fn other_keys_and_events_are_not() {
        assert!(!is_power_press(&record(EV_KEY, 30, 1)));
        assert!(!is_power_press(&record(0, KEY_POWER, 1)));
    }

    #[test]
    fn an_all_zero_record_is_not_a_press() {
        assert!(!is_power_press(&[0u8; EVENT_LEN]));
    }
}
