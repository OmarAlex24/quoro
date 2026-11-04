// SPDX-License-Identifier: AGPL-3.0-only
//! Build logs kept on the node (ADR 0014, ADR 0119).
//!
//! One file per build, `<root>/<app_id>/<build_id>.jsonl`, one JSON object per
//! line: `{seq, time, stream, vertex?, line}`. `seq` is gap-free from 1 and is
//! the cursor the control plane pages with (`build_logs_query`) and the SSE
//! event id it resumes from (`build_logs` with `after_seq`). Appending and
//! finishing a build take one lock, so sequence numbers never repeat within an
//! agent run, and a file left by an earlier run is read back to continue its
//! numbering.
//!
//! The file is capped at [`MAX_LOG_BYTES`]: past it lines are counted and
//! dropped, and the build's end appends exactly one marker line saying how many
//! were dropped. A single line is cut to [`MAX_LINE_BYTES`]. Both bounds keep a
//! hostile Dockerfile from filling the node's disk.
//!
//! Live readers subscribe to a bounded broadcast before they replay the file,
//! so no line is lost or repeated between the replay and the live part: a line
//! the subscriber missed is read from the file, and sequence numbers are the
//! only filter needed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use quoro_proto::wire::LogStream;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tracing::debug;

use crate::AgentError;
use crate::util;

/// The cap on one build's log file, in bytes (section 6.6).
pub const MAX_LOG_BYTES: u64 = 8 * 1024 * 1024;
/// The cap on one log line, in bytes.
pub const MAX_LINE_BYTES: usize = 16 * 1024;
/// Capacity of the live fan-out channel. A reader that falls this far behind
/// catches up from the file, which is the source of truth.
const BROADCAST_CAPACITY: usize = 1_024;

/// One line of a build's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBuildLine {
    /// The build the line belongs to.
    pub build_id: String,
    /// Gap-free position in that build's log, from 1.
    pub seq: u64,
    /// Nanoseconds since the Unix epoch.
    pub time_ns: i64,
    /// `stdout` or `stderr`.
    pub stream: LogStream,
    /// BuildKit's step number, when the line belongs to one.
    pub vertex: Option<u32>,
    /// The line without its trailing newline.
    pub line: String,
}

/// The on-disk form of one line.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogRecord {
    seq: u64,
    /// RFC 3339 with nine fractional digits.
    time: String,
    stream: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vertex: Option<u32>,
    line: String,
}

/// What one build's writer knows about its file.
struct LogState {
    app_id: String,
    path: PathBuf,
    next_seq: u64,
    bytes: u64,
    dropped: u64,
    marker_written: bool,
}

/// Every build's human log on this node.
pub struct BuildLogs {
    root: PathBuf,
    state: Mutex<HashMap<String, LogState>>,
    live: broadcast::Sender<StoredBuildLine>,
}

impl BuildLogs {
    /// Builds the log store under `root` (for example
    /// `<data_dir>/builds/logs`).
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        let (live, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            root,
            state: Mutex::new(HashMap::new()),
            live,
        }
    }

    /// The directory holding one app's build logs.
    #[must_use]
    pub fn app_dir(&self, app_id: &str) -> PathBuf {
        self.root.join(app_id)
    }

    /// The file holding one build's log.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when an id is not a safe path component; both come
    /// from a signed snapshot, but the node never trusts one blindly.
    pub fn path(&self, app_id: &str, build_id: &str) -> Result<PathBuf, AgentError> {
        check_component(app_id)?;
        check_component(build_id)?;
        Ok(self.app_dir(app_id).join(format!("{build_id}.jsonl")))
    }

    /// Subscribes to lines as they are appended.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<StoredBuildLine> {
        self.live.subscribe()
    }

    /// Appends one line, assigning it the next sequence number.
    ///
    /// Returns the stored line, or `None` when the line had to be dropped
    /// because the log reached [`MAX_LOG_BYTES`].
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when an id is not a safe path component, and
    /// [`AgentError::Io`] when the file cannot be created or written.
    pub async fn append(
        &self,
        app_id: &str,
        build_id: &str,
        stream: LogStream,
        vertex: Option<u32>,
        line: &str,
    ) -> Result<Option<StoredBuildLine>, AgentError> {
        let path = self.path(app_id, build_id)?;
        let line = cut_bytes(line, MAX_LINE_BYTES);
        let mut state = self.state.lock().await;
        if !state.contains_key(build_id) {
            let loaded = load_state(app_id, path.clone()).await?;
            state.insert(build_id.to_owned(), loaded);
        }
        let entry = state
            .get_mut(build_id)
            .expect("the state was inserted just above");
        if entry.bytes >= MAX_LOG_BYTES {
            entry.dropped = entry.dropped.saturating_add(1);
            return Ok(None);
        }
        let record = LogRecord {
            seq: entry.next_seq,
            time: util::rfc3339_nanos(util::now_ns()),
            stream: stream_name(stream).to_owned(),
            vertex,
            line,
        };
        let text = encode(&record)?;
        if entry.bytes.saturating_add(text.len() as u64) > MAX_LOG_BYTES {
            entry.dropped = entry.dropped.saturating_add(1);
            return Ok(None);
        }
        write_line(&entry.path, &text).await?;
        entry.bytes = entry.bytes.saturating_add(text.len() as u64);
        entry.next_seq = entry.next_seq.saturating_add(1);
        let stored = StoredBuildLine {
            build_id: build_id.to_owned(),
            seq: record.seq,
            time_ns: util::parse_rfc3339_nanos(&record.time).unwrap_or_else(util::now_ns),
            stream,
            vertex,
            line: record.line,
        };
        drop(state);
        let _ = self.live.send(stored.clone());
        Ok(Some(stored))
    }

    /// Closes one build's log: appends the single truncation marker when lines
    /// were dropped. Idempotent.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] or [`AgentError::Io`], as [`Self::append`].
    pub async fn finish(&self, app_id: &str, build_id: &str) -> Result<(), AgentError> {
        let path = self.path(app_id, build_id)?;
        let mut state = self.state.lock().await;
        if !state.contains_key(build_id) {
            let loaded = load_state(app_id, path.clone()).await?;
            state.insert(build_id.to_owned(), loaded);
        }
        let entry = state
            .get_mut(build_id)
            .expect("the state was inserted just above");
        if entry.dropped == 0 || entry.marker_written {
            return Ok(());
        }
        let record = LogRecord {
            seq: entry.next_seq,
            time: util::rfc3339_nanos(util::now_ns()),
            stream: stream_name(LogStream::Stdout).to_owned(),
            vertex: None,
            line: format!("log truncated: {} more lines dropped", entry.dropped),
        };
        let text = encode(&record)?;
        // The marker is written whatever the cap says: it is the one line that
        // explains the gap, so it must be in the file.
        write_line(&entry.path, &text).await?;
        entry.bytes = entry.bytes.saturating_add(text.len() as u64);
        entry.next_seq = entry.next_seq.saturating_add(1);
        entry.marker_written = true;
        entry.dropped = 0;
        let stored = StoredBuildLine {
            build_id: build_id.to_owned(),
            seq: record.seq,
            time_ns: util::parse_rfc3339_nanos(&record.time).unwrap_or_else(util::now_ns),
            stream: LogStream::Stdout,
            vertex: None,
            line: record.line,
        };
        drop(state);
        let _ = self.live.send(stored);
        Ok(())
    }

    /// The lines after `after_seq`, oldest first, at most `limit`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] or [`AgentError::Io`] when the file cannot be
    /// read.
    pub async fn lines_after(
        &self,
        app_id: &str,
        build_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredBuildLine>, AgentError> {
        let path = self.path(app_id, build_id)?;
        let records = read_records(&path).await?;
        Ok(records
            .into_iter()
            .filter(|record| record.seq > after_seq)
            .take(limit)
            .map(|record| stored_line(build_id, record))
            .collect())
    }

    /// The newest `n` lines, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] or [`AgentError::Io`] when the file cannot be
    /// read.
    pub async fn tail(
        &self,
        app_id: &str,
        build_id: &str,
        n: usize,
    ) -> Result<Vec<StoredBuildLine>, AgentError> {
        let path = self.path(app_id, build_id)?;
        let mut records = read_records(&path).await?;
        if records.len() > n {
            records.drain(..records.len() - n);
        }
        Ok(records
            .into_iter()
            .map(|record| stored_line(build_id, record))
            .collect())
    }

    /// Whether a log file exists for the build.
    #[must_use]
    pub fn exists(&self, app_id: &str, build_id: &str) -> bool {
        self.path(app_id, build_id).is_ok_and(|path| path.exists())
    }

    /// Removes one app's log directory and forgets its builds.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] when the id is not a safe path component, and
    /// [`AgentError::Io`] for a deletion that is not "not found".
    pub async fn remove_app(&self, app_id: &str) -> Result<(), AgentError> {
        check_component(app_id)?;
        let dir = self.app_dir(app_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(AgentError::io("delete an app's build logs", e)),
        }
        self.state
            .lock()
            .await
            .retain(|_, state| state.app_id != app_id);
        Ok(())
    }

    /// Removes one build's log file (its row is the caller's to delete).
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] or [`AgentError::Io`], as [`Self::remove_app`].
    pub async fn remove_build(&self, app_id: &str, build_id: &str) -> Result<(), AgentError> {
        let path = self.path(app_id, build_id)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(AgentError::io("delete a build log", e)),
        }
        self.state.lock().await.remove(build_id);
        Ok(())
    }
}

fn stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn parse_stream(text: &str) -> LogStream {
    if text == "stderr" {
        LogStream::Stderr
    } else {
        LogStream::Stdout
    }
}

/// Rejects an id that is not a safe single path component.
fn check_component(id: &str) -> Result<(), AgentError> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if ok {
        Ok(())
    } else {
        Err(AgentError::Store(format!(
            "a build or app id is not a safe path component: {id:?}"
        )))
    }
}

/// Cuts `text` to at most `max` bytes, on a character boundary.
fn cut_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let end = (0..=max)
        .rev()
        .find(|end| text.is_char_boundary(*end))
        .unwrap_or(0);
    text[..end].to_owned()
}

fn encode(record: &LogRecord) -> Result<String, AgentError> {
    let mut text = serde_json::to_string(record)
        .map_err(|e| AgentError::Store(format!("could not encode a build log line: {e}")))?;
    text.push('\n');
    Ok(text)
}

/// Reads the file's writer state, or starts a new one.
async fn load_state(app_id: &str, path: PathBuf) -> Result<LogState, AgentError> {
    let mut next_seq = 1u64;
    let mut bytes = 0u64;
    match tokio::fs::read(&path).await {
        Ok(raw) => {
            bytes = raw.len() as u64;
            for record in parse_records(&raw) {
                next_seq = next_seq.max(record.seq.saturating_add(1));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(AgentError::io("read a build log", e)),
    }
    Ok(LogState {
        app_id: app_id.to_owned(),
        path,
        next_seq,
        bytes,
        dropped: 0,
        marker_written: false,
    })
}

/// Appends one already-encoded line, creating the app directory `0700`.
async fn write_line(path: &Path, text: &str) -> Result<(), AgentError> {
    use tokio::io::AsyncWriteExt as _;

    let Some(dir) = path.parent() else {
        return Err(AgentError::Store(format!(
            "the build log path {} has no directory",
            path.display()
        )));
    };
    if !dir.exists() {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| AgentError::io("create a build log directory", e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(|e| AgentError::io("secure a build log directory", e))?;
        }
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|e| AgentError::io("open a build log", e))?;
    file.write_all(text.as_bytes())
        .await
        .map_err(|e| AgentError::io("append to a build log", e))?;
    file.flush()
        .await
        .map_err(|e| AgentError::io("flush a build log", e))
}

/// Every record in a file, in order, skipping lines that are not ours.
async fn read_records(path: &Path) -> Result<Vec<LogRecord>, AgentError> {
    match tokio::fs::read(path).await {
        Ok(raw) => Ok(parse_records(&raw)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(AgentError::io("read a build log", e)),
    }
}

fn parse_records(raw: &[u8]) -> Vec<LogRecord> {
    let text = String::from_utf8_lossy(raw);
    let mut records = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<LogRecord>(line) {
            Ok(record) => records.push(record),
            Err(e) => debug!(error = %e, "skipping a malformed build log line"),
        }
    }
    records
}

fn stored_line(build_id: &str, record: LogRecord) -> StoredBuildLine {
    let time_ns = util::parse_rfc3339_nanos(&record.time).unwrap_or(0);
    StoredBuildLine {
        build_id: build_id.to_owned(),
        seq: record.seq,
        time_ns,
        stream: parse_stream(&record.stream),
        vertex: record.vertex,
        line: record.line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-build-logs-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn app() -> &'static str {
        "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E"
    }

    fn build() -> &'static str {
        "bld_01JC9ZA2B3C4D5E6F7G8H9J0KM"
    }

    #[tokio::test]
    async fn lines_are_numbered_from_one_and_read_back_in_order() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        for text in ["one", "two", "three"] {
            logs.append(app(), build(), LogStream::Stdout, None, text)
                .await
                .unwrap();
        }
        let lines = logs.lines_after(app(), build(), 0, 10).await.unwrap();
        assert_eq!(
            lines.iter().map(|l| l.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            lines.iter().map(|l| l.line.as_str()).collect::<Vec<_>>(),
            vec!["one", "two", "three"]
        );
        let after = logs.lines_after(app(), build(), 2, 10).await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].line, "three");

        let tail = logs.tail(app(), build(), 2).await.unwrap();
        assert_eq!(
            tail.iter().map(|l| l.line.as_str()).collect::<Vec<_>>(),
            vec!["two", "three"]
        );
    }

    #[tokio::test]
    async fn a_reopened_store_continues_the_sequence() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        logs.append(app(), build(), LogStream::Stdout, None, "one")
            .await
            .unwrap();
        drop(logs);

        let logs = BuildLogs::new(dir.0.clone());
        logs.append(app(), build(), LogStream::Stdout, None, "two")
            .await
            .unwrap();
        let lines = logs.lines_after(app(), build(), 0, 10).await.unwrap();
        assert_eq!(lines.iter().map(|l| l.seq).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_long_line_is_cut_and_the_cap_leaves_one_marker() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        let long = "x".repeat(MAX_LINE_BYTES * 2);
        let stored = logs
            .append(app(), build(), LogStream::Stdout, None, &long)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.line.len(), MAX_LINE_BYTES);

        // Fill the log past its cap without writing 8 MiB in the test: the
        // state records the size, so a second line that crosses the cap is
        // dropped and counted.
        let mut state = logs.state.lock().await;
        state.get_mut(build()).unwrap().bytes = MAX_LOG_BYTES - 1;
        drop(state);
        assert!(
            logs.append(app(), build(), LogStream::Stdout, None, "dropped")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            logs.append(app(), build(), LogStream::Stdout, None, "dropped again")
                .await
                .unwrap()
                .is_none()
        );
        logs.finish(app(), build()).await.unwrap();
        logs.finish(app(), build()).await.unwrap();

        let lines = logs.lines_after(app(), build(), 0, 10).await.unwrap();
        let marker = lines.last().unwrap();
        assert_eq!(marker.line, "log truncated: 2 more lines dropped");
        assert_eq!(marker.seq, stored.seq + 1);
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.line.starts_with("log truncated:"))
                .count(),
            1,
            "exactly one marker"
        );
    }

    #[tokio::test]
    async fn appended_lines_reach_a_subscriber() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        let mut rx = logs.subscribe();
        logs.append(app(), build(), LogStream::Stderr, Some(6), "live")
            .await
            .unwrap();
        let line = rx.recv().await.unwrap();
        assert_eq!(line.seq, 1);
        assert_eq!(line.vertex, Some(6));
        assert_eq!(line.stream, LogStream::Stderr);
    }

    #[tokio::test]
    async fn removing_an_app_deletes_its_directory() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        logs.append(app(), build(), LogStream::Stdout, None, "one")
            .await
            .unwrap();
        assert!(logs.exists(app(), build()));
        logs.remove_app(app()).await.unwrap();
        assert!(!logs.exists(app(), build()));
        assert!(
            logs.lines_after(app(), build(), 0, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unsafe_ids_are_refused() {
        let dir = TempDir::new();
        let logs = BuildLogs::new(dir.0.clone());
        for id in ["../../etc/passwd", "app/other", "", "app id"] {
            assert!(
                logs.append(id, build(), LogStream::Stdout, None, "x")
                    .await
                    .is_err(),
                "{id:?} was accepted"
            );
        }
    }
}
