// SPDX-License-Identifier: AGPL-3.0-only
//! The build source: one commit fetched with the host's Git client into a
//! bare repository, written as a tar stream onto the raw `quoro-src` disk
//! (`node-agent-protocol.md` section 6.6).
//!
//! No working tree of the repository ever exists on the host, and Git runs
//! hardened: HTTPS only, no prompts, no submodules, no hooks or templates, a
//! wall-clock timeout.
//!
//! The repository is untrusted, so the tar re-emitted onto the disk is filtered
//! entry by entry: only regular files, directories and symlinks survive, their
//! paths must stay inside `src/`, and the total size is capped. A symlink is
//! data inside the tar; the host never creates one.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;

use super::truncate_chars;

/// The longest repository URL accepted.
const MAX_URL_CHARS: usize = 2048;
/// The longest ref accepted.
const MAX_REF_CHARS: usize = 255;
/// The most bytes kept from one Git command's stderr.
const MAX_STDERR_BYTES: usize = 64 * 1024;
/// The most bytes kept from one Git command's stdout (a commit SHA is 41).
const MAX_STDOUT_BYTES: usize = 4096;
/// The longest message taken from Git's output for an error.
const MAX_MESSAGE_CHARS: usize = 500;
/// The largest source tar written to the source disk.
const MAX_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;
/// The most the bare repository may grow to during the fetch. Git has no size
/// limit of its own, so a watcher stops a fetch that passes it.
const MAX_FETCH_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// How often the fetch watcher measures the bare repository.
const FETCH_WATCH: Duration = Duration::from_millis(500);
/// The most secrets one build may carry.
const MAX_SECRETS: usize = 128;
/// The largest single secret value.
const MAX_SECRET_BYTES: usize = 1024 * 1024;
/// The longest secret id.
const MAX_SECRET_ID_CHARS: usize = 128;
/// One chunk while streaming `git archive` output.
const ARCHIVE_CHUNK: usize = 64 * 1024;
/// How many chunks may sit between the archive pipe and the tar writer.
const ARCHIVE_CHANNELS: usize = 8;
/// The padding of the source disk: a whole MiB, and at least one.
const DISK_PAD_BYTES: u64 = 1024 * 1024;

/// True only for test builds: unit tests fetch from a local `file://`
/// repository so they need no network. A production build never accepts a
/// `file://` URL nor lets Git use the `file` protocol.
#[cfg(test)]
const ALLOW_FILE_URLS: bool = true;
/// False for production builds; see [`ALLOW_FILE_URLS`].
#[cfg(not(test))]
const ALLOW_FILE_URLS: bool = false;

/// The `-c` options every Git call carries, before its subcommand.
const CONFIG_ARGS: &[&str] = &[
    "-c",
    "protocol.allow=never",
    "-c",
    "protocol.https.allow=always",
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "submodule.recurse=false",
    "-c",
    "http.followRedirects=initial",
    "-c",
    "credential.helper=",
    "-c",
    "init.templateDir=",
];

/// Fetches `git_ref` of `url` (a branch, a tag or a full commit SHA) into a
/// new bare repository at `repo` with depth 1, and returns the full commit
/// SHA it resolved to (40 lowercase hex).
///
/// # Errors
///
/// A one-line reason when the URL is not `https://`, Git fails, or the
/// timeout passes.
pub async fn fetch_commit(
    git: &Path,
    url: &str,
    git_ref: &str,
    repo: &Path,
    timeout: Duration,
) -> Result<String, String> {
    validate_url(url)?;
    validate_git_ref(git_ref)?;
    if !repo.is_absolute() {
        return Err("the bare repository path must be absolute".to_owned());
    }
    if !git.is_absolute() {
        return Err("the git path must be absolute".to_owned());
    }
    let Some(home) = repo.parent() else {
        return Err("the bare repository path has no parent directory".to_owned());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(home)
        .map_err(|e| format!("could not create {home:?}: {e}"))?;
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return Err("the fetch timeout is too large".to_owned());
    };

    let mut init = hardened(git, home);
    init.args(config_args())
        .args(["init", "--bare", "--quiet"])
        .arg(repo);
    let init = run(init, deadline, "git init").await?;
    if !init.status.success() {
        return Err(failure("git init", &init));
    }

    let mut fetch = hardened(git, home);
    fetch
        .args(config_args())
        .arg("-C")
        .arg(repo)
        .args([
            "-c",
            "fetch.unpackLimit=1",
            "fetch",
            "--depth=1",
            "--no-tags",
            "--no-recurse-submodules",
            "--quiet",
        ])
        .arg(url)
        .arg(git_ref);
    let fetch = run_bounded(fetch, deadline, "git fetch", repo, MAX_FETCH_BYTES).await?;
    if !fetch.status.success() {
        return Err(failure("git fetch", &fetch));
    }

    let mut rev_parse = hardened(git, home);
    rev_parse.args(config_args()).arg("-C").arg(repo).args([
        "rev-parse",
        "--verify",
        "FETCH_HEAD^{commit}",
    ]);
    let rev_parse = run(rev_parse, deadline, "git rev-parse").await?;
    if !rev_parse.status.success() {
        return Err(failure("git rev-parse", &rev_parse));
    }
    let commit = String::from_utf8_lossy(&rev_parse.stdout).trim().to_owned();
    validate_commit(&commit)?;
    Ok(commit)
}

/// Writes the source tar onto `disk` (created, or truncated, then padded with
/// zeros to a whole MiB): the tree of `commit` under `src/`, and one file per
/// secret under `secrets/<id>` (mode `0400`). Returns the size of `disk`.
///
/// # Errors
///
/// A one-line reason when Git or a write fails.
pub async fn write_source_disk(
    git: &Path,
    repo: &Path,
    commit: &str,
    secrets: &BTreeMap<String, String>,
    disk: &Path,
) -> Result<u64, String> {
    validate_commit(commit)?;
    validate_secrets(secrets)?;
    if !repo.is_absolute() || !disk.is_absolute() || !git.is_absolute() {
        return Err("the git, repository and disk paths must be absolute".to_owned());
    }
    let Some(home) = repo.parent() else {
        return Err("the bare repository path has no parent directory".to_owned());
    };

    let mut command = hardened(git, home);
    command
        .args(config_args())
        .arg("-C")
        .arg(repo)
        .args(["archive", "--format=tar", "--prefix=src/", commit])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| format!("git archive could not start: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "git archive has no output pipe".to_owned())?;
    let stderr = child.stderr.take();

    let (sender, receiver) = mpsc::channel::<Vec<u8>>(ARCHIVE_CHANNELS);
    let pump = tokio::spawn(pump_stdout(stdout, sender));
    let stderr_task = tokio::spawn(drain(stderr, MAX_STDERR_BYTES));

    let secrets = secrets.clone();
    let path = disk.to_path_buf();
    let writer = tokio::task::spawn_blocking(move || write_disk(&path, &secrets, receiver));

    let written = match writer.await {
        Ok(written) => written?,
        Err(e) => return Err(format!("the source disk writer failed: {e}")),
    };
    let status = child
        .wait()
        .await
        .map_err(|e| format!("git archive could not be waited for: {e}"))?;
    let stderr = stderr_task.await.unwrap_or_default();
    pump.abort();

    if !status.success() {
        return Err(failure(
            "git archive",
            &Captured {
                status,
                stdout: Vec::new(),
                stderr,
            },
        ));
    }
    Ok(written)
}

/// Validates a repository URL: `https://`, no user information, no whitespace
/// or control characters. Test builds also accept `file://` (see
/// [`ALLOW_FILE_URLS`]).
fn validate_url(url: &str) -> Result<(), String> {
    if url.chars().count() > MAX_URL_CHARS {
        return Err(format!(
            "the repository URL is longer than {MAX_URL_CHARS} characters"
        ));
    }
    let prefix = if url.starts_with("https://") {
        "https://"
    } else if ALLOW_FILE_URLS && url.starts_with("file://") {
        "file://"
    } else {
        return Err("the repository URL must start with https://".to_owned());
    };
    if url.len() == prefix.len() {
        return Err("the repository URL has no host".to_owned());
    }
    if url.contains('@') {
        return Err("the repository URL must not carry a user name".to_owned());
    }
    for ch in url.chars() {
        if ch.is_whitespace() || ch.is_control() {
            return Err("the repository URL contains whitespace or a control character".to_owned());
        }
    }
    Ok(())
}

/// Validates a ref: non-empty, at most [`MAX_REF_CHARS`] characters, no
/// leading `-` (so it is never read as an option), no whitespace or control
/// characters, and none of the characters Git treats specially.
fn validate_git_ref(git_ref: &str) -> Result<(), String> {
    if git_ref.is_empty() {
        return Err("the ref is empty".to_owned());
    }
    if git_ref.chars().count() > MAX_REF_CHARS {
        return Err(format!("the ref is longer than {MAX_REF_CHARS} characters"));
    }
    if git_ref.starts_with('-') {
        return Err("the ref must not start with -".to_owned());
    }
    if git_ref.contains("..") {
        return Err("the ref contains ..".to_owned());
    }
    for ch in git_ref.chars() {
        if ch.is_whitespace() || ch.is_control() {
            return Err("the ref contains whitespace or a control character".to_owned());
        }
        if matches!(ch, ':' | '~' | '^' | '\\' | '*' | '?' | '[') {
            return Err("the ref contains a character Git refuses".to_owned());
        }
    }
    Ok(())
}

/// True for exactly 40 lowercase hex characters.
fn validate_commit(commit: &str) -> Result<(), String> {
    let valid = commit.len() == 40
        && commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid {
        Ok(())
    } else {
        Err("the resolved commit is not a full lowercase SHA".to_owned())
    }
}

/// Validates secret ids and values before anything is written.
pub(crate) fn validate_secrets(secrets: &BTreeMap<String, String>) -> Result<(), String> {
    if secrets.len() > MAX_SECRETS {
        return Err(format!("a build may carry at most {MAX_SECRETS} secrets"));
    }
    for (id, value) in secrets {
        if !valid_secret_id(id) {
            return Err(format!("the secret id {id:?} is not a valid name"));
        }
        if value.len() > MAX_SECRET_BYTES {
            return Err(format!(
                "the secret {id:?} is larger than {MAX_SECRET_BYTES} bytes"
            ));
        }
    }
    Ok(())
}

/// `^[A-Za-z_][A-Za-z0-9_]*$`, at most [`MAX_SECRET_ID_CHARS`] characters.
fn valid_secret_id(id: &str) -> bool {
    if id.is_empty() || id.chars().count() > MAX_SECRET_ID_CHARS {
        return false;
    }
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// A hardened Git command: a fixed environment, no stdin, no prompt, killed
/// when dropped, and nothing inherited from the node agent.
fn hardened(git: &Path, home: &Path) -> Command {
    let mut command = Command::new(git);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", home)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_ASKPASS", "/bin/false")
        .env("SSH_ASKPASS", "/bin/false")
        .env("GIT_PROTOCOL_FROM_USER", "0")
        .env(
            "GIT_ALLOW_PROTOCOL",
            if ALLOW_FILE_URLS {
                "https:file"
            } else {
                "https"
            },
        )
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    command
}

/// The `-c` options of every Git call, including the test-only `file`
/// protocol.
fn config_args() -> Vec<&'static str> {
    let mut args = CONFIG_ARGS.to_vec();
    if ALLOW_FILE_URLS {
        args.extend(["-c", "protocol.file.allow=always"]);
    }
    args
}

/// What one Git command printed, bounded.
struct Captured {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Runs one Git command under the wall-clock `deadline`, capturing bounded
/// stdout and stderr.
async fn run(mut command: Command, deadline: Instant, label: &str) -> Result<Captured, String> {
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(format!("{label} timed out"));
    }
    let mut child = command
        .spawn()
        .map_err(|e| format!("{label} could not start: {e}"))?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(drain(stdout, MAX_STDOUT_BYTES));
    let stderr_task = tokio::spawn(drain(stderr, MAX_STDERR_BYTES));
    let status = match tokio::time::timeout(remaining, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            let _ = child.start_kill();
            return Err(format!("{label} failed: {e}"));
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(format!("{label} timed out"));
        }
    };
    Ok(Captured {
        status,
        stdout: stdout_task.await.unwrap_or_default(),
        stderr: stderr_task.await.unwrap_or_default(),
    })
}

/// [`run`] for a command that writes into `directory`: the command is killed
/// once `directory` holds more than `max` bytes.
async fn run_bounded(
    command: Command,
    deadline: Instant,
    label: &str,
    directory: &Path,
    max: u64,
) -> Result<Captured, String> {
    let run = run(command, deadline, label);
    tokio::pin!(run);
    let mut ticker = tokio::time::interval(FETCH_WATCH);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            captured = &mut run => return captured,
            _ = ticker.tick() => {
                let walked = directory.to_owned();
                let size = tokio::task::spawn_blocking(move || directory_size(&walked))
                    .await
                    .unwrap_or(0);
                if size > max {
                    // Dropping `run` kills the child (`kill_on_drop`).
                    return Err(format!(
                        "{label} stopped: the repository is larger than {} GiB",
                        max / (1024 * 1024 * 1024)
                    ));
                }
            }
        }
    }
}

/// The bytes of every regular file under `directory`, without following
/// symlinks. Unreadable entries count as empty.
fn directory_size(directory: &Path) -> u64 {
    let mut total = 0u64;
    let mut pending = vec![directory.to_owned()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.path().symlink_metadata() else {
                continue;
            };
            if meta.is_dir() {
                pending.push(entry.path());
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

/// Reads a child's pipe to its end, keeping at most the last `max` bytes.
async fn drain<R>(reader: Option<R>, max: usize) -> Vec<u8>
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Vec::new();
    };
    let mut kept: Vec<u8> = Vec::new();
    let mut buffer = vec![0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => return kept,
            Ok(read) => {
                kept.extend_from_slice(&buffer[..read]);
                if kept.len() > max {
                    let drop = kept.len() - max;
                    kept.drain(..drop);
                }
            }
        }
    }
}

/// Copies `git archive` output into the tar writer, one bounded chunk at a
/// time.
async fn pump_stdout<R>(stdout: R, sender: mpsc::Sender<Vec<u8>>)
where
    R: AsyncRead + Unpin,
{
    let mut stdout = stdout;
    let mut buffer = vec![0u8; ARCHIVE_CHUNK];
    loop {
        match stdout.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => {
                if sender.send(buffer[..read].to_vec()).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// The one-line reason for a failed Git command: its last non-empty stderr
/// line, or the exit status when it printed nothing.
fn failure(label: &str, captured: &Captured) -> String {
    let text = String::from_utf8_lossy(&captured.stderr);
    let line = text
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(|line| truncate_chars(line.trim(), MAX_MESSAGE_CHARS));
    match line {
        Some(line) => line,
        None => format!("{label} exited with {}", captured.status),
    }
}

/// Writes the whole source disk: the secrets, then the commit's tree, then the
/// zero padding. Runs on a blocking thread because it is file and tar work.
fn write_disk(
    disk: &Path,
    secrets: &BTreeMap<String, String>,
    archive: mpsc::Receiver<Vec<u8>>,
) -> Result<u64, String> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(disk)
        .map_err(|e| format!("could not create the source disk {disk:?}: {e}"))?;
    let mut builder = tar::Builder::new(file);
    let mut total = secrets
        .values()
        .map(|value| value.len() as u64)
        .fold(0u64, |sum, len| sum.saturating_add(len));
    check_source_size(total)?;

    write_secrets(&mut builder, secrets)?;

    let mut reader = ChannelReader::new(archive);
    let mut source = tar::Archive::new(&mut reader);
    let entries = source
        .entries()
        .map_err(|e| format!("could not read the repository tar: {e}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| format!("could not read the repository tar: {e}"))?;
        if !matches!(
            entry.header().entry_type(),
            tar::EntryType::Regular | tar::EntryType::Directory | tar::EntryType::Symlink
        ) {
            // The pax global header (the commit comment), hard links, devices,
            // FIFOs and anything unknown never reach the builder VM.
            continue;
        }
        total = total.saturating_add(entry.size());
        check_source_size(total)?;
        let path_bytes = entry.path_bytes().into_owned();
        validate_entry_path(&path_bytes)?;
        let path = PathBuf::from(std::ffi::OsStr::from_bytes(&path_bytes));
        let mut header = entry.header().clone();
        match entry.header().entry_type() {
            tar::EntryType::Regular => {
                let size = entry.size();
                header.set_size(size);
                builder
                    .append_data(&mut header, &path, &mut entry)
                    .map_err(|e| format!("could not write {path:?} to the source disk: {e}"))?;
            }
            tar::EntryType::Directory => {
                header.set_size(0);
                builder
                    .append_data(&mut header, &path, io::empty())
                    .map_err(|e| format!("could not write {path:?} to the source disk: {e}"))?;
            }
            tar::EntryType::Symlink => {
                let target = entry
                    .link_name()
                    .map_err(|e| format!("could not read a link in the repository tar: {e}"))?
                    .ok_or_else(|| "a symlink in the repository tar has no target".to_owned())?;
                header.set_size(0);
                let target = target.into_owned();
                builder
                    .append_link(&mut header, &path, &target)
                    .map_err(|e| format!("could not write {path:?} to the source disk: {e}"))?;
            }
            _ => unreachable!("only regular files, directories and symlinks are kept"),
        }
    }

    let file = builder
        .into_inner()
        .map_err(|e| format!("could not finish the source tar: {e}"))?;
    let size = file
        .metadata()
        .map_err(|e| format!("could not measure the source disk: {e}"))?
        .len();
    let padded = size.max(DISK_PAD_BYTES).div_ceil(DISK_PAD_BYTES) * DISK_PAD_BYTES;
    file.set_len(padded)
        .map_err(|e| format!("could not pad the source disk: {e}"))?;
    Ok(padded)
}

/// Refuses a source tar larger than [`MAX_SOURCE_BYTES`].
fn check_source_size(total: u64) -> Result<(), String> {
    if total > MAX_SOURCE_BYTES {
        return Err(format!(
            "the source tree is larger than {MAX_SOURCE_BYTES} bytes"
        ));
    }
    Ok(())
}

/// Writes `secrets/` and one `0400` file per secret.
fn write_secrets(
    builder: &mut tar::Builder<File>,
    secrets: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut dir = header(tar::EntryType::Directory, 0o500, 0);
    builder
        .append_data(&mut dir, Path::new("secrets/"), io::empty())
        .map_err(|e| format!("could not write the secret directory: {e}"))?;
    for (id, value) in secrets {
        let mut file_header = header(tar::EntryType::Regular, 0o400, value.len() as u64);
        let path = PathBuf::from(format!("secrets/{id}"));
        builder
            .append_data(&mut file_header, &path, value.as_bytes())
            .map_err(|e| format!("could not write the secret {id:?}: {e}"))?;
    }
    Ok(())
}

/// A tar header with a fixed owner, time and mode.
fn header(entry_type: tar::EntryType, mode: u32, size: u64) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(entry_type);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(size);
    header
}

/// Refuses an entry path that is absolute, carries `..`, or leaves `src/`.
fn validate_entry_path(path: &[u8]) -> Result<(), String> {
    if path.is_empty() {
        return Err("the repository tar holds an entry with no name".to_owned());
    }
    if path.contains(&0) {
        return Err("the repository tar holds a path with a NUL byte".to_owned());
    }
    if path[0] == b'/' {
        return Err("the repository tar holds an absolute path".to_owned());
    }
    for component in path.split(|byte| *byte == b'/') {
        if component == b".." {
            return Err("the repository tar holds a path with ..".to_owned());
        }
    }
    if !path.starts_with(b"src/") {
        return Err("the repository tar holds a path outside src/".to_owned());
    }
    Ok(())
}

/// A synchronous `Read` over the asynchronous `git archive` pipe.
///
/// It is driven from a blocking thread by [`write_disk`]: `blocking_recv`
/// parks that thread while the async side fills the channel, so memory stays
/// bounded to [`ARCHIVE_CHANNELS`] chunks.
struct ChannelReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    position: usize,
}

impl ChannelReader {
    fn new(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            chunk: Vec::new(),
            position: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.position >= self.chunk.len() {
            match self.receiver.blocking_recv() {
                Some(chunk) => {
                    self.chunk = chunk;
                    self.position = 0;
                }
                None => return Ok(0),
            }
        }
        let available = self.chunk.len() - self.position;
        let count = available.min(buffer.len());
        buffer[..count].copy_from_slice(&self.chunk[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    /// A fresh temporary directory for one test.
    fn scratch(name: &str) -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("quoro-build-source-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("a temporary directory");
        base
    }

    #[test]
    fn the_repository_size_counts_files_but_not_symlinks() {
        let base = scratch("size");
        std::fs::create_dir_all(base.join("objects/pack")).unwrap();
        std::fs::write(base.join("HEAD"), [0u8; 10]).unwrap();
        std::fs::write(base.join("objects/pack/p.pack"), [0u8; 1000]).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", base.join("link")).unwrap();
        assert_eq!(directory_size(&base), 1010);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn a_command_that_outgrows_its_directory_is_stopped() {
        let base = scratch("bounded");
        std::fs::write(base.join("big"), vec![0u8; 4096]).unwrap();
        let mut command = Command::new("/bin/sleep");
        command.arg("30").kill_on_drop(true);
        let deadline = Instant::now() + Duration::from_secs(20);
        let started = Instant::now();
        let result = run_bounded(command, deadline, "sleep", &base, 1024).await;
        let Err(error) = result else {
            panic!("the command was not stopped");
        };
        assert!(error.contains("larger than"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(10));
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The absolute path of the host's `git`, or `None` when it is missing.
    fn host_git() -> Option<PathBuf> {
        let path = std::env::var_os("PATH")?;
        for directory in std::env::split_paths(&path) {
            let candidate = directory.join("git");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        None
    }

    /// Runs `git` in `directory` and returns its stdout.
    fn run_git(git: &Path, directory: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new(git)
            .args([
                "-c",
                "user.name=Quoro Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// A working repository with a regular file, a directory and a symlink
    /// out of the tree, committed.
    fn repository(git: &Path, root: &Path) -> PathBuf {
        let work = root.join("work");
        std::fs::create_dir_all(work.join("app")).expect("a work tree");
        std::fs::write(work.join("app/main.txt"), b"hello\n").expect("a file");
        std::fs::write(work.join("README.md"), b"read me\n").expect("a file");
        std::os::unix::fs::symlink("/etc/passwd", work.join("passwd-link")).expect("a symlink");
        run_git(git, &work, &["init", "--quiet"]);
        run_git(git, &work, &["add", "--all"]);
        run_git(git, &work, &["commit", "--quiet", "-m", "initial"]);
        work
    }

    /// The entries of a tar file: path, type and mode, in order.
    fn tar_entries(path: &Path) -> Vec<(PathBuf, tar::EntryType, u32)> {
        let file = File::open(path).expect("the disk file");
        let mut archive = tar::Archive::new(std::io::BufReader::new(file));
        let mut entries = Vec::new();
        for entry in archive.entries().expect("a valid tar") {
            let entry = entry.expect("a valid entry");
            let path = entry.path().expect("a path").into_owned();
            let kind = entry.header().entry_type();
            let mode = entry.header().mode().expect("a mode");
            entries.push((path, kind, mode));
        }
        entries
    }

    #[test]
    fn url_validation_table() {
        for accepted in [
            "https://github.com/example/blog.git",
            "https://example.invalid/repo",
            // Test builds need a local repository; production does not.
            "file:///srv/repo.git",
        ] {
            assert!(validate_url(accepted).is_ok(), "{accepted} is accepted");
        }
        for refused in [
            "",
            "github.com/example/blog",
            "http://example.invalid/repo",
            "ssh://git@example.invalid/repo",
            "git@github.com:example/blog.git",
            "https://user@example.invalid/repo",
            "https://example.invalid/a b",
            "https://example.invalid/a\nb",
            "https://example.invalid/\u{7f}",
            "https://",
        ] {
            assert!(validate_url(refused).is_err(), "{refused:?} is refused");
        }
    }

    #[test]
    fn ref_validation_table() {
        for accepted in [
            "main",
            "v1.0.0",
            "refs/heads/feature-x",
            "0e1e2f3a4b5c6d7e8f9012345678901234567890",
        ] {
            assert!(validate_git_ref(accepted).is_ok(), "{accepted} is accepted");
        }
        for refused in [
            "",
            "-main",
            "main..dev",
            "a:b",
            "a~1",
            "a^1",
            "a\\b",
            "a*b",
            "a?b",
            "a[b",
            "a b",
            "a\tb",
            "a\u{7f}b",
        ] {
            assert!(validate_git_ref(refused).is_err(), "{refused:?} is refused");
        }
        assert!(validate_git_ref(&"a".repeat(256)).is_err());
        assert!(validate_git_ref(&"a".repeat(255)).is_ok());
    }

    #[test]
    fn commit_and_secret_validation() {
        assert!(validate_commit("0e1e2f3a4b5c6d7e8f9012345678901234567890").is_ok());
        for refused in [
            "",
            "0E1E2F3A4B5C6D7E8F9012345678901234567890",
            "0e1e2f3a4b5c6d7e8f901234567890123456789",
            "0e1e2f3a4b5c6d7e8f90123456789012345678900",
            "0e1e2f3a4b5c6d7e8f901234567890123456789g",
        ] {
            assert!(validate_commit(refused).is_err(), "{refused:?} is refused");
        }
        for accepted in ["DB_URL", "_token", "a1", &"a".repeat(MAX_SECRET_ID_CHARS)] {
            assert!(valid_secret_id(accepted), "{accepted} is accepted");
        }
        for refused in [
            "",
            "1TOKEN",
            "a-b",
            "a.b",
            "a b",
            "a/b",
            &"a".repeat(MAX_SECRET_ID_CHARS + 1),
        ] {
            assert!(!valid_secret_id(refused), "{refused:?} is refused");
        }
        assert!(check_source_size(MAX_SOURCE_BYTES).is_ok());
        assert!(check_source_size(MAX_SOURCE_BYTES + 1).is_err());
    }

    #[test]
    fn entry_paths_must_stay_under_src() {
        for accepted in ["src/a", "src/a/b.txt", "src/dir/"] {
            assert!(
                validate_entry_path(accepted.as_bytes()).is_ok(),
                "{accepted}"
            );
        }
        for refused in ["", "/src/a", "src/../a", "src/a/../../b", "other/a", "src"] {
            assert!(
                validate_entry_path(refused.as_bytes()).is_err(),
                "{refused:?} is refused"
            );
        }
        assert!(validate_entry_path(b"src/a\0b").is_err());
    }

    #[tokio::test]
    async fn fetch_and_write_the_source_disk() {
        let Some(git) = host_git() else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let root = scratch("fetch");
        let work = repository(&git, &root);
        let bare = root.join("run/repo.git");
        let url = format!("file://{}", work.display());
        let commit = fetch_commit(&git, &url, "master", &bare, Duration::from_secs(60))
            .await
            .expect("the commit is fetched");
        assert_eq!(commit.len(), 40);
        assert!(commit.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(bare.join("HEAD").is_file());

        let mut secrets = BTreeMap::new();
        secrets.insert("TOKEN".to_owned(), "s3cr3t\n".to_owned());
        let disk = root.join("src.disk");
        let passwd_before = std::fs::metadata("/etc/passwd")
            .map(|m| (m.len(), m.modified().ok()))
            .ok();

        let size = write_source_disk(&git, &bare, &commit, &secrets, &disk)
            .await
            .expect("the source disk is written");

        let metadata = std::fs::metadata(&disk).expect("the disk file");
        assert_eq!(size, metadata.len());
        assert_eq!(size % DISK_PAD_BYTES, 0);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let entries = tar_entries(&disk);
        let (paths, kinds): (Vec<&PathBuf>, Vec<tar::EntryType>) =
            entries.iter().map(|(p, k, _)| (p, *k)).unzip();
        assert_eq!(paths[0], &PathBuf::from("secrets/"));
        assert_eq!(kinds[0], tar::EntryType::Directory);
        assert_eq!(paths[1], &PathBuf::from("secrets/TOKEN"));
        assert_eq!(kinds[1], tar::EntryType::Regular);
        assert_eq!(entries[1].2, 0o400);
        assert!(paths[2..].iter().all(|path| path.starts_with("src")));
        assert!(paths.contains(&&PathBuf::from("src/app/main.txt")));
        assert!(paths.contains(&&PathBuf::from("src/README.md")));
        let link = entries
            .iter()
            .find(|(path, _, _)| path == &PathBuf::from("src/passwd-link"))
            .expect("the symlink is kept");
        assert_eq!(link.1, tar::EntryType::Symlink);

        // Reading the tar never follows the link: the host's file is intact.
        let passwd_after = std::fs::metadata("/etc/passwd")
            .map(|m| (m.len(), m.modified().ok()))
            .ok();
        assert_eq!(passwd_before, passwd_after);

        // The archive parses to its end despite the zero padding.
        assert!(paths.len() >= 5);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_missing_ref_is_one_error_line() {
        let Some(git) = host_git() else {
            eprintln!("skipping: git is not installed");
            return;
        };
        let root = scratch("missing-ref");
        let work = repository(&git, &root);
        let bare = root.join("run/repo.git");
        let url = format!("file://{}", work.display());
        let error = fetch_commit(&git, &url, "no-such-branch", &bare, Duration::from_secs(60))
            .await
            .expect_err("the ref does not exist");
        assert!(!error.contains('\n'), "{error:?}");
        assert!(error.chars().count() <= MAX_MESSAGE_CHARS);
        assert!(error.contains("no-such-branch"), "{error}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn non_https_and_relative_paths_are_refused() {
        let mut result = fetch_commit(
            Path::new("/usr/bin/git"),
            "http://example.invalid/repo.git",
            "main",
            Path::new("/srv/repo.git"),
            Duration::from_secs(1),
        )
        .await;
        assert!(result.unwrap_err().contains("https://"));

        result = fetch_commit(
            Path::new("/usr/bin/git"),
            "https://example.invalid/repo.git",
            "main",
            Path::new("relative/repo.git"),
            Duration::from_secs(1),
        )
        .await;
        assert!(result.unwrap_err().contains("absolute"));
    }

    /// A tar with one kept file, one symlink and entries that must be dropped.
    fn hostile_tar() -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut file = header(tar::EntryType::Regular, 0o644, 2);
        builder
            .append_data(&mut file, Path::new("src/ok.txt"), b"hi".as_slice())
            .expect("a file");
        let mut fifo = header(tar::EntryType::Fifo, 0o644, 0);
        builder
            .append_data(&mut fifo, Path::new("src/pipe"), io::empty())
            .expect("a fifo");
        let mut device = header(tar::EntryType::Char, 0o644, 0);
        builder
            .append_data(&mut device, Path::new("src/null"), io::empty())
            .expect("a device");
        let mut link = header(tar::EntryType::Symlink, 0o777, 0);
        builder
            .append_link(&mut link, Path::new("src/link"), Path::new("/etc/passwd"))
            .expect("a symlink");
        let mut hard = header(tar::EntryType::Link, 0o644, 0);
        builder
            .append_link(&mut hard, Path::new("src/hard"), Path::new("src/ok.txt"))
            .expect("a hard link");
        builder.into_inner().expect("a finished tar")
    }

    /// Writes `bytes` through [`write_disk`] as if they came from `git
    /// archive`, and returns its result.
    async fn write_through(
        disk: &Path,
        bytes: Vec<u8>,
        secrets: BTreeMap<String, String>,
    ) -> Result<u64, String> {
        let (sender, receiver) = mpsc::channel(1);
        let pump = tokio::spawn(async move {
            for chunk in bytes.chunks(7) {
                if sender.send(chunk.to_vec()).await.is_err() {
                    return;
                }
            }
        });
        let path = disk.to_path_buf();
        let written = tokio::task::spawn_blocking(move || write_disk(&path, &secrets, receiver))
            .await
            .expect("the writer thread");
        let _ = pump.await;
        written
    }

    #[tokio::test]
    async fn only_files_directories_and_symlinks_are_kept() {
        let root = scratch("filter");
        let disk = root.join("src.disk");
        let written = write_through(&disk, hostile_tar(), BTreeMap::new())
            .await
            .expect("the tar is filtered");

        assert_eq!(written % DISK_PAD_BYTES, 0);
        let entries = tar_entries(&disk);
        let paths: Vec<&PathBuf> = entries.iter().map(|(path, _, _)| path).collect();
        // `secrets/` is always first, even without secrets.
        assert_eq!(
            paths,
            vec![
                &PathBuf::from("secrets/"),
                &PathBuf::from("src/ok.txt"),
                &PathBuf::from("src/link"),
            ]
        );
        assert_eq!(entries[2].1, tar::EntryType::Symlink);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_path_with_a_parent_component_fails_the_write() {
        let root = scratch("escape");
        let disk = root.join("src.disk");
        // The `tar` crate refuses to write such a path, so the name of a
        // valid one is patched in place.
        let mut builder = tar::Builder::new(Vec::new());
        let mut file = header(tar::EntryType::Regular, 0o644, 2);
        file.set_path("src/axx/bb").expect("a path");
        file.set_cksum();
        file.as_mut_bytes()[..10].copy_from_slice(b"src/a/../b");
        file.set_cksum();
        builder.append(&file, b"hi".as_slice()).expect("a file");
        let bytes = builder.into_inner().expect("a finished tar");

        let error = write_through(&disk, bytes, BTreeMap::new())
            .await
            .expect_err("the path is refused");
        assert!(error.contains(".."));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_bad_secret_id_fails_before_the_disk_is_written() {
        let root = scratch("secret");
        let disk = root.join("src.disk");
        let mut secrets = BTreeMap::new();
        secrets.insert("BAD-ID".to_owned(), "value".to_owned());
        let error = write_source_disk(
            Path::new("/usr/bin/git"),
            Path::new("/srv/repo.git"),
            "0e1e2f3a4b5c6d7e8f9012345678901234567890",
            &secrets,
            &disk,
        )
        .await
        .expect_err("the secret id is refused");
        assert!(error.contains("BAD-ID"));
        assert!(!disk.exists());
        let _ = std::fs::remove_dir_all(&root);
    }
}
