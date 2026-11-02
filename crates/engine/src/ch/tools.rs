// SPDX-License-Identifier: AGPL-3.0-only
//! The host programs the Cloud Hypervisor backend runs.
//!
//! `ip`, `nft` and `mkfs.ext4` are executed directly, each with an absolute
//! path and an argument vector. Nothing here goes through a shell, so a bridge
//! name, a TAP name or a subnet coming from the control plane can never become
//! shell syntax.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use crate::EngineError;

/// Where a host tool is looked for, in order.
const SEARCH_DIRS: [&str; 4] = ["/usr/sbin", "/sbin", "/usr/bin", "/bin"];

/// How long one host tool call may take before it is killed.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The host programs the backend runs, already resolved to absolute paths.
///
/// Resolving once at startup means a broken `PATH` in the service manager
/// surfaces as `Unsupported` when the engine is built, not in the middle of
/// creating a VM.
#[derive(Clone, Debug)]
pub struct HostTools {
    /// `ip`, from iproute2.
    pub ip: PathBuf,
    /// `nft`, from nftables.
    pub nft: PathBuf,
    /// `mkfs.ext4`, from e2fsprogs.
    pub mkfs_ext4: PathBuf,
}

impl HostTools {
    /// Finds `ip`, `nft` and `mkfs.ext4` in `/usr/sbin`, `/sbin`, `/usr/bin`
    /// and `/bin`, taking the first executable regular file of each name.
    ///
    /// A symlink to an executable counts: Debian's `mkfs.ext4` is a symlink to
    /// `mke2fs`.
    ///
    /// # Errors
    ///
    /// `Unsupported` naming the missing tool and the package that provides it.
    pub fn discover() -> Result<HostTools, EngineError> {
        Ok(HostTools {
            ip: find("ip", "iproute2")?,
            nft: find("nft", "nftables")?,
            mkfs_ext4: find("mkfs.ext4", "e2fsprogs")?,
        })
    }
}

/// The first executable regular file called `name`, or `Unsupported`.
fn find(name: &str, package: &str) -> Result<PathBuf, EngineError> {
    SEARCH_DIRS
        .iter()
        .map(|dir| Path::new(dir).join(name))
        .find(|candidate| is_executable(candidate))
        .ok_or_else(|| EngineError::Unsupported(format!("{name} not found; install {package}")))
}

/// True when `path` is a file (following symlinks) with an executable bit.
fn is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

/// Runs `program args…` without a shell, with stdin on `/dev/null`, and waits
/// at most thirty seconds for it.
///
/// # Errors
///
/// `Io` when the program cannot be started or exits non-zero (the message
/// carries the command, its exit status and its trimmed stderr); `Timeout`
/// after thirty seconds, in which case the program is killed.
pub async fn run(program: &Path, args: &[&str]) -> Result<String, EngineError> {
    run_inner(program, args, None).await
}

/// [`run`], with `stdin` written to the child's standard input.
///
/// This is how an nftables ruleset is applied: `nft -f -`.
///
/// # Errors
///
/// The same as [`run`].
pub async fn run_with_stdin(
    program: &Path,
    args: &[&str],
    stdin: &str,
) -> Result<String, EngineError> {
    run_inner(program, args, Some(stdin)).await
}

/// The one implementation of both entry points.
async fn run_inner(
    program: &Path,
    args: &[&str],
    stdin: Option<&str>,
) -> Result<String, EngineError> {
    let label = label(program, args);
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Dropping the `wait_with_output` future on the timeout below drops the
        // child, which then kills the tool instead of leaving it running.
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .map_err(|err| EngineError::Io(format!("{label}: {err}")))?;

    if let Some(text) = stdin
        && let Some(mut pipe) = child.stdin.take()
    {
        // A write error means the tool closed its stdin early (a parse error,
        // say); the exit status and stderr below say why, and those are the
        // message worth returning.
        let _ = pipe.write_all(text.as_bytes()).await;
        let _ = pipe.shutdown().await;
    }

    let output = match tokio::time::timeout(TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => return Err(EngineError::Io(format!("{label}: {err}"))),
        Err(_) => return Err(EngineError::Timeout),
    };

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let code = output
        .status
        .code()
        .map_or_else(|| "killed by signal".to_owned(), |code| code.to_string());
    Err(EngineError::Io(format!(
        "{label}: exit {code}: {}",
        detail(&output.stdout, &output.stderr)
    )))
}

/// The command as it appears in an error message.
fn label(program: &Path, args: &[&str]) -> String {
    format!("{} {}", program.display(), args.join(" "))
        .trim_end()
        .to_owned()
}

/// The trimmed stderr, or the trimmed stdout when the tool said nothing there.
fn detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        String::from_utf8_lossy(stdout).trim().to_owned()
    } else {
        stderr.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh() -> PathBuf {
        PathBuf::from("/bin/sh")
    }

    #[test]
    fn discover_finds_the_three_tools_as_absolute_paths() {
        let tools =
            HostTools::discover().expect("the lab host has iproute2, nftables and e2fsprogs");
        for path in [&tools.ip, &tools.nft, &tools.mkfs_ext4] {
            assert!(path.is_absolute(), "{} is not absolute", path.display());
            assert!(is_executable(path), "{} is not executable", path.display());
        }
        assert!(tools.ip.ends_with("ip"));
        assert!(tools.nft.ends_with("nft"));
        assert!(tools.mkfs_ext4.ends_with("mkfs.ext4"));
    }

    #[test]
    fn find_reports_the_package_of_a_missing_tool() {
        let err = find("definitely-not-a-host-tool", "somepackage").expect_err("must be missing");
        assert_eq!(
            err,
            EngineError::Unsupported(
                "definitely-not-a-host-tool not found; install somepackage".to_owned()
            )
        );
    }

    #[test]
    fn find_skips_directories_and_non_executables() {
        // `/bin` holds `true`, which is a regular executable; a directory with
        // the same name would not qualify.
        assert!(!is_executable(&PathBuf::from("/bin")));
        assert!(!is_executable(&PathBuf::from("/etc/hostname")));
    }

    #[tokio::test]
    async fn run_returns_stdout() {
        let out = run(&sh(), &["-c", "printf 'hello world'"])
            .await
            .expect("sh runs");
        assert_eq!(out, "hello world");
    }

    #[tokio::test]
    async fn run_reports_the_exit_code_and_stderr() {
        let err = run(&sh(), &["-c", "printf '  bad thing  ' >&2; exit 3"])
            .await
            .expect_err("non-zero exit is an error");
        assert_eq!(
            err,
            EngineError::Io(
                "/bin/sh -c printf '  bad thing  ' >&2; exit 3: exit 3: bad thing".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn run_falls_back_to_stdout_when_stderr_is_empty() {
        let err = run(&sh(), &["-c", "printf 'the reason'; exit 1"])
            .await
            .expect_err("non-zero exit is an error");
        let EngineError::Io(message) = err else {
            panic!("expected Io");
        };
        assert!(message.ends_with("exit 1: the reason"), "{message}");
    }

    #[tokio::test]
    async fn run_reports_a_program_that_cannot_be_started() {
        let err = run(&PathBuf::from("/nonexistent/tool"), &[])
            .await
            .expect_err("cannot start");
        let EngineError::Io(message) = err else {
            panic!("expected Io");
        };
        assert!(message.starts_with("/nonexistent/tool:"), "{message}");
    }

    #[tokio::test]
    async fn run_with_stdin_feeds_the_child() {
        let out = run_with_stdin(&sh(), &["-c", "cat"], "quoro\n")
            .await
            .expect("cat runs");
        assert_eq!(out, "quoro\n");
    }
}
