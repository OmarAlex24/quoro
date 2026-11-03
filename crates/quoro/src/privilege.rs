//! Dropping root for commands that write into the control plane's data
//! directory.
//!
//! The control plane runs as its own unprivileged user (ADR 0089), and that
//! user owns `/var/lib/quoro`. An operator still types `sudo quoro
//! setup-token`; run as root, the command would leave root-owned files (the
//! token, SQLite's `-wal` and `-shm`) that the service can then no longer
//! write. So the command runs itself again as the directory's owner.

use std::path::Path;

/// Set in the re-executed child so it never tries again.
const DROPPED: &str = "QUORO_PRIVILEGES_DROPPED";

/// When this process is root and `data_dir` belongs to another user, runs the
/// same command line again as that user and group, and returns its exit code.
/// `None` means the caller goes on in this process.
///
/// # Errors
///
/// The child could not be started or waited for.
#[cfg(target_os = "linux")]
pub fn run_as_data_dir_owner(data_dir: &Path) -> Option<std::io::Result<i32>> {
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::process::CommandExt as _;

    if std::env::var_os(DROPPED).is_some() {
        return None;
    }
    // `/proc/self` belongs to the effective user of this process.
    let euid = std::fs::metadata("/proc/self").ok()?.uid();
    let owner = std::fs::metadata(data_dir).ok()?;
    if euid != 0 || owner.uid() == 0 {
        return None;
    }

    let run = || -> std::io::Result<i32> {
        let exe = std::env::current_exe()?;
        // Changing the uid from root also clears the supplementary groups.
        let status = std::process::Command::new(exe)
            .args(std::env::args_os().skip(1))
            .env(DROPPED, "1")
            .uid(owner.uid())
            .gid(owner.gid())
            .status()?;
        Ok(status.code().unwrap_or(1))
    };
    Some(run())
}

/// Elsewhere there is no service user to drop to.
#[cfg(not(target_os = "linux"))]
pub fn run_as_data_dir_owner(_data_dir: &Path) -> Option<std::io::Result<i32>> {
    let _ = DROPPED;
    None
}
