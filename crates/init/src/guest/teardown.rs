// SPDX-License-Identifier: AGPL-3.0-only
//! The last thing the guest does before it powers off.
//!
//! Two jobs, done only after the status record is written: make sure no process
//! outlives PID 1, then flush and clean every mount backed by a block device so
//! the node can read a work disk the app mounted without journal recovery.
//!
//! Nothing here may fail the shutdown. A busy mount is the normal case (the
//! overlay's upper disk backs the running root), and a disk that will not go
//! quietly is logged and left as it is; the VM still powers off.

use std::time::{Duration, Instant};

use rustix::io::Errno;
use rustix::mount::{MountFlags, UnmountFlags, mount_remount, unmount};
use rustix::process::{Pid, Signal, WaitOptions, kill_process};

use crate::guest::diag;

/// The kernel's mount table, one line per mount, in mount order.
const MOUNT_TABLE: &str = "/proc/self/mounts";

/// How long the remaining processes get to answer `SIGTERM` before `SIGKILL`,
/// and then how long the killed processes get to be reaped.
const STOP_GRACE: Duration = Duration::from_secs(1);

/// Stop every process that is left: `SIGTERM` first, one second to exit, then
/// `SIGKILL` and one second to reap.
pub(crate) fn stop_processes() {
    signal_everyone(Signal::TERM);
    reap_remaining(STOP_GRACE);
    signal_everyone(Signal::KILL);
    reap_remaining(STOP_GRACE);
}

/// Signal every process in `/proc` except ourselves (`/proc/1`), which is what
/// `kill(-1, signal)` reaches: PID 1 is the init and never signals itself.
pub(crate) fn signal_everyone(signal: Signal) {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Ok(raw) = name.parse::<i32>() else {
            continue;
        };
        if raw <= 1 {
            continue;
        }
        if let Some(pid) = Pid::from_raw(raw) {
            let _ = kill_process(pid, signal);
        }
    }
}

/// Reap the processes [`signal_everyone`] just signalled, for at most `budget`,
/// handing each status to the exec session waiting on it.
pub(crate) fn reap_remaining(budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        match rustix::process::wait(WaitOptions::NOHANG) {
            // An exec session waiting on one of these hears its status
            // (section 3.18).
            Ok(Some((pid, status))) => {
                crate::guest::exec::reaped(pid, status);
                continue;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(Errno::INTR) => continue,
            Err(_) => return,
        }
    }
}

/// Flush the page cache, then unmount or remount read-only every mount whose
/// source is a disk under `/dev/`, the last mounted first. Failures are logged
/// and never stop the power-off.
pub(crate) fn unmount_disks() {
    rustix::fs::sync();
    let Ok(table) = std::fs::read(MOUNT_TABLE) else {
        diag::log("disks: cannot read /proc/self/mounts; leaving them mounted");
        return;
    };
    let targets = unmount_targets(&String::from_utf8_lossy(&table));
    if targets.is_empty() {
        diag::log("disks: no block-device mounts to clean");
    }
    for target in targets {
        clean_mount(&target);
    }
}

/// Unmount one mount, falling back to a read-only remount when the plain
/// unmount finds it busy. Neither call blocks: a busy mount answers `EBUSY`,
/// so a stuck disk cannot keep the VM from powering off.
fn clean_mount(target: &str) {
    let unmount_error = match unmount(target, UnmountFlags::empty()) {
        Ok(()) => {
            diag::log(&format!("disks: unmounted {target}"));
            return;
        }
        Err(err) => err,
    };

    // A bind mount is remounted read-only only when `MS_BIND` is part of the
    // remount, so that is the second attempt.
    let mut remount_error = None;
    for flags in [MountFlags::RDONLY, MountFlags::RDONLY | MountFlags::BIND] {
        match mount_remount(target, flags, c"") {
            Ok(()) => {
                diag::log(&format!("disks: remounted {target} read-only"));
                return;
            }
            Err(err) => remount_error = Some(err),
        }
    }
    let remount_error = remount_error.map(|err| err.to_string()).unwrap_or_default();
    diag::log(&format!(
        "disks: {target}: unmount failed ({unmount_error}); read-only remount failed ({remount_error})"
    ));
}

/// The paths [`unmount_disks`] should clean, from the text of
/// `/proc/self/mounts`: every mount whose source is a block device under
/// `/dev/`, in reverse mount order so the most recently mounted is tried
/// first. Pseudo filesystems are never included, because their source is not a
/// `/dev` path.
pub(crate) fn unmount_targets(mounts: &str) -> Vec<String> {
    let mut targets = Vec::new();
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let (Some(source), Some(target)) = (fields.next(), fields.next()) else {
            continue;
        };
        if !is_block_device(source) {
            continue;
        }
        targets.push(unescape(target));
    }
    targets.reverse();
    targets
}

/// Whether a mount source names a device node under `/dev/`.
fn is_block_device(source: &str) -> bool {
    source
        .strip_prefix("/dev/")
        .is_some_and(|name| !name.is_empty())
}

/// Undo the kernel's octal escapes in a mount-table field: `\040` is a space,
/// `\011` a tab, `\012` a newline and `\134` a backslash. Any other backslash
/// sequence is kept as written.
fn unescape(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && let Some(value) = octal(&bytes[index + 1..index + 4])
        {
            out.push(value);
            index += 4;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Three octal digits as one byte, or `None` when any of them is not an octal
/// digit or the value does not fit a byte.
fn octal(digits: &[u8]) -> Option<u8> {
    let mut value: u32 = 0;
    for digit in digits {
        if !(b'0'..=b'7').contains(digit) {
            return None;
        }
        value = value * 8 + u32::from(digit - b'0');
    }
    u8::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = "\
devtmpfs /dev devtmpfs rw,nosuid 0 0
proc /proc proc rw 0 0
/sys /sys sysfs rw 0 0
/dev/vda /quoro/lower ext4 ro,relatime 0 0
/dev/vdb /quoro/data ext4 rw,noatime 0 0
overlay / overlay rw 0 0
/dev/vdd /mnt/work ext4 rw,relatime 0 0
cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec 0 0
";

    #[test]
    fn block_device_mounts_come_back_last_mounted_first() {
        assert_eq!(
            unmount_targets(TABLE),
            vec![
                "/mnt/work".to_string(),
                "/quoro/data".to_string(),
                "/quoro/lower".to_string(),
            ]
        );
    }

    #[test]
    fn volume_mounts_at_any_path_inside_the_app_root_are_cleaned() {
        let table = "\
/dev/vdd /var/lib/app/data ext4 rw,relatime 0 0
/dev/vde /srv/uploads ext4 rw,relatime 0 0
/dev/vdf /a/b/c/d ext4 rw,relatime 0 0
";
        assert_eq!(
            unmount_targets(table),
            vec![
                "/a/b/c/d".to_string(),
                "/srv/uploads".to_string(),
                "/var/lib/app/data".to_string(),
            ]
        );
    }

    #[test]
    fn pseudo_filesystems_are_ignored() {
        let table = "\
tmpfs /run tmpfs rw 0 0
devpts /dev/pts devpts rw 0 0
cgroup2 /sys/fs/cgroup cgroup2 rw 0 0
";
        assert!(unmount_targets(table).is_empty());
    }

    #[test]
    fn a_source_that_is_not_under_dev_is_ignored() {
        let table = "\
/dev /mnt/dev ext4 rw 0 0
/devices/vdd /mnt/work ext4 rw 0 0
";
        assert!(unmount_targets(table).is_empty());
    }

    #[test]
    fn octal_escapes_in_a_mount_path_are_decoded() {
        let table = "/dev/vdd /mnt/my\\040work\\011one ext4 rw 0 0\n";
        assert_eq!(unmount_targets(table), vec!["/mnt/my work\tone"]);
    }

    #[test]
    fn an_escaped_backslash_is_decoded() {
        let table = "/dev/vdd /mnt/back\\134slash ext4 rw 0 0\n";
        assert_eq!(unmount_targets(table), vec!["/mnt/back\\slash"]);
    }

    #[test]
    fn an_unknown_backslash_sequence_is_kept_as_written() {
        let table = "/dev/vdd /mnt/odd\\999path ext4 rw 0 0\n";
        assert_eq!(unmount_targets(table), vec!["/mnt/odd\\999path"]);
    }

    #[test]
    fn short_lines_and_blank_lines_are_skipped() {
        let table = "\n/dev/vdd\nonly-one-field\n/dev/vde /mnt/work ext4 rw 0 0\n";
        assert_eq!(unmount_targets(table), vec!["/mnt/work"]);
    }

    #[test]
    fn an_empty_table_yields_no_targets() {
        assert!(unmount_targets("").is_empty());
    }
}
