// SPDX-License-Identifier: AGPL-3.0-only
//! The diagnostics console.
//!
//! Every line is `quoro-init: <text>\n` on `/dev/ttyS0`, or on `/dev/console`
//! before devtmpfs has been mounted over `/dev`. The kernel hands PID 1 an open
//! console on fd 0/1/2, but the path is what the engine's `--serial` file
//! captures, so it is opened explicitly.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::sync::{Mutex, OnceLock};

/// The console currently in use, if any. Kept open for the life of the guest:
/// its file descriptor survives `chroot` and the mounts below it.
static CONSOLE: OnceLock<Mutex<Option<File>>> = OnceLock::new();

fn state() -> &'static Mutex<Option<File>> {
    CONSOLE.get_or_init(|| Mutex::new(None))
}

/// Open the diagnostics console: `/dev/ttyS0` if it exists yet, `/dev/console`
/// otherwise. Called twice, before and after devtmpfs is mounted; an already
/// open console keeps working if the second attempt finds neither.
///
/// Returns whether a console is open.
pub(crate) fn open_console() -> bool {
    for path in ["/dev/ttyS0", "/dev/console"] {
        if let Ok(file) = OpenOptions::new().write(true).open(path)
            && let Ok(mut slot) = state().lock()
        {
            *slot = Some(file);
            return true;
        }
    }
    state().lock().map(|slot| slot.is_some()).unwrap_or(false)
}

/// One `quoro-init: <text>` line on the diagnostics console.
pub(crate) fn log(text: &str) {
    if let Ok(mut slot) = state().lock()
        && let Some(file) = slot.as_mut()
    {
        let mut line = String::with_capacity(text.len() + 12);
        line.push_str("quoro-init: ");
        line.push_str(text);
        line.push('\n');
        let _ = file.write_all(line.as_bytes());
    }
}
