// SPDX-License-Identifier: AGPL-3.0-only
//! Terminal transcripts (`audit-log-format.md` section 3.3).
//!
//! One asciicast v2 file per terminal session under
//! `<data_dir>/transcripts/<trm_id>.cast`, its SHA-256 computed while the file
//! is written, a row in the local `transcripts` table, and the retention sweep
//! that keeps at most 1 GiB of them for at most 30 days.
//!
//! The file is the record: the hash the node reports in `stream_end` covers it
//! exactly as written, so the primary can write `session.end` and an edited
//! transcript is evident afterwards.

use std::fmt;
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::store::{NodeStore, StoredTranscript};
use crate::util;

/// The per-session transcript cap (16 MiB, `audit-log-format.md` 3.3).
pub const MAX_TRANSCRIPT_BYTES: u64 = 16 * 1024 * 1024;
/// How long a node keeps a transcript (30 days).
pub const MAX_TRANSCRIPT_AGE_SECS: i64 = 30 * 24 * 60 * 60;
/// How much transcripts may take on one node (1 GiB).
pub const MAX_TRANSCRIPT_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

/// The transcript policy: the per-session cap and the two retention limits.
///
/// Tests build their own so a cap of 16 MiB never has to be written to reach
/// the `limit` end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranscriptLimits {
    /// Largest file one session may write.
    pub max_bytes: u64,
    /// Oldest transcript age kept on the node.
    pub max_age_secs: i64,
    /// Total size every transcript on the node may take.
    pub total_bytes: u64,
}

impl Default for TranscriptLimits {
    fn default() -> Self {
        Self {
            max_bytes: MAX_TRANSCRIPT_BYTES,
            max_age_secs: MAX_TRANSCRIPT_AGE_SECS,
            total_bytes: MAX_TRANSCRIPT_TOTAL_BYTES,
        }
    }
}

/// Why a transcript write stopped.
#[derive(Debug)]
pub enum TranscriptError {
    /// The next event would pass [`TranscriptLimits::max_bytes`]; it was not
    /// written and the session ends with `limit`.
    Limit,
    /// The file or the store could not be written.
    Io(String),
}

impl fmt::Display for TranscriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limit => f.write_str("the transcript reached its size limit"),
            Self::Io(message) => f.write_str(message),
        }
    }
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> TranscriptError {
    TranscriptError::Io(format!("could not {action} {}: {error}", path.display()))
}

/// `<data_dir>/transcripts`, created `0700` when missing.
///
/// # Errors
///
/// [`TranscriptError::Io`] when the directory cannot be created or secured.
pub fn transcripts_dir(store: &NodeStore) -> Result<PathBuf, TranscriptError> {
    let dir = store.data_dir().join("transcripts");
    if !dir.exists() {
        std::fs::create_dir_all(&dir)
            .map_err(|e| io_error("create the transcript directory", &dir, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| io_error("secure the transcript directory", &dir, e))?;
        }
    }
    Ok(dir)
}

/// The transcript file path for one terminal session.
///
/// The caller validates `session_id` first, so it can never escape the
/// directory.
///
/// # Errors
///
/// [`TranscriptError::Io`] when the directory cannot be created.
pub fn transcript_path(store: &NodeStore, session_id: &str) -> Result<PathBuf, TranscriptError> {
    Ok(transcripts_dir(store)?.join(format!("{session_id}.cast")))
}

/// One asciicast v2 file being written.
///
/// Output and input bytes become `o` and `i` events, resizes `r`; the hash is
/// updated with exactly the bytes written, so [`TranscriptWriter::finish`]
/// returns the hex digest of the stored file without reading it back.
pub struct TranscriptWriter {
    file: tokio::fs::File,
    path: PathBuf,
    hasher: Sha256,
    size: u64,
    max_bytes: u64,
    started: tokio::time::Instant,
    session_id: String,
    store: NodeStore,
    /// Bytes of an incomplete UTF-8 sequence carried to the next output
    /// chunk; input keeps its own, so the two directions never mix.
    partial_output: Vec<u8>,
    partial_input: Vec<u8>,
}

impl TranscriptWriter {
    /// Creates the file, writes the header and records the row.
    ///
    /// # Errors
    ///
    /// [`TranscriptError::Limit`] when a header alone would pass the cap, and
    /// [`TranscriptError::Io`] when the file or the row cannot be written.
    pub async fn create(
        store: &NodeStore,
        session_id: &str,
        vm_id: &str,
        cols: u16,
        rows: u16,
        limits: TranscriptLimits,
        started_at: i64,
    ) -> Result<Self, TranscriptError> {
        let path = transcript_path(store, session_id)?;
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .await
            .map_err(|e| io_error("create the transcript", &path, e))?;

        let header = serde_json::json!({
            "version": 2,
            "width": cols,
            "height": rows,
            "timestamp": started_at,
            "env": { "TERM": "xterm-256color" },
        })
        .to_string();
        let header = format!("{header}\n");
        if header.len() as u64 > limits.max_bytes {
            cleanup(&path).await;
            return Err(TranscriptError::Limit);
        }
        if let Err(e) = file.write_all(header.as_bytes()).await {
            cleanup(&path).await;
            return Err(io_error("write the transcript header", &path, e));
        }
        let mut hasher = Sha256::new();
        hasher.update(header.as_bytes());

        let row = StoredTranscript {
            session_id: session_id.to_owned(),
            vm_id: vm_id.to_owned(),
            path: path.clone(),
            size_bytes: 0,
            sha256: String::new(),
            started_at,
            ended_at: None,
        };
        if let Err(e) = store.insert_transcript(&row).await {
            cleanup(&path).await;
            return Err(TranscriptError::Io(format!(
                "could not record the transcript row: {e}"
            )));
        }

        Ok(Self {
            file,
            path,
            hasher,
            size: header.len() as u64,
            max_bytes: limits.max_bytes,
            started: tokio::time::Instant::now(),
            session_id: session_id.to_owned(),
            store: store.clone(),
            partial_output: Vec::new(),
            partial_input: Vec::new(),
        })
    }

    /// Appends the program's output as one `o` event.
    ///
    /// # Errors
    ///
    /// [`TranscriptError::Limit`] when the event would pass the cap; nothing is
    /// written then.
    pub async fn output(&mut self, bytes: &[u8]) -> Result<(), TranscriptError> {
        let text = self.text('o', bytes);
        if text.is_empty() {
            // Bytes held back as an incomplete UTF-8 sequence write nothing
            // yet; the next chunk completes them.
            return Ok(());
        }
        self.event('o', &text).await
    }

    /// Appends client input as one `i` event.
    ///
    /// # Errors
    ///
    /// [`TranscriptError::Limit`] when the event would pass the cap.
    pub async fn input(&mut self, bytes: &[u8]) -> Result<(), TranscriptError> {
        let text = self.text('i', bytes);
        if text.is_empty() {
            return Ok(());
        }
        self.event('i', &text).await
    }

    /// Appends a resize as one `r` event, `"<cols>x<rows>"`.
    ///
    /// # Errors
    ///
    /// [`TranscriptError::Limit`] when the event would pass the cap.
    pub async fn resize(&mut self, rows: u16, cols: u16) -> Result<(), TranscriptError> {
        self.event('r', &format!("{cols}x{rows}")).await
    }

    /// Flushes the file, writes the final hash and size to the row and returns
    /// them.
    ///
    /// The digest covers every byte written, including a trailing replacement
    /// character if a chunk ended inside a UTF-8 sequence.
    ///
    /// # Errors
    ///
    /// [`TranscriptError::Io`] when the file cannot be flushed.
    pub async fn finish(mut self) -> Result<(String, u64), TranscriptError> {
        for kind in ['o', 'i'] {
            let partial = match kind {
                'o' => &mut self.partial_output,
                _ => &mut self.partial_input,
            };
            if partial.is_empty() {
                continue;
            }
            partial.clear();
            // The cap already ended the session; a trailing replacement that
            // no longer fits is dropped with the rest of the file intact.
            let _ = self.event(kind, "\u{fffd}").await;
        }
        self.file
            .flush()
            .await
            .map_err(|e| io_error("flush the transcript", &self.path, e))?;

        let sha = util::hex(&std::mem::take(&mut self.hasher).finalize());
        let size = self.size;
        if let Err(e) = self
            .store
            .finish_transcript(&self.session_id, size, &sha, util::now_secs())
            .await
        {
            tracing::warn!(session_id = %self.session_id, error = %e, "could not record the finished transcript");
        }
        Ok((sha, size))
    }

    /// Applies the incremental UTF-8 rule: invalid bytes become U+FFFD and an
    /// incomplete sequence at the end of a chunk waits for the next one.
    fn text(&mut self, kind: char, bytes: &[u8]) -> String {
        let partial = match kind {
            'o' => &mut self.partial_output,
            _ => &mut self.partial_input,
        };
        let mut combined = std::mem::take(partial);
        combined.extend_from_slice(bytes);
        let mut out = String::with_capacity(combined.len());
        let mut rest: &[u8] = &combined;
        loop {
            match std::str::from_utf8(rest) {
                Ok(text) => {
                    out.push_str(text);
                    break;
                }
                Err(e) => {
                    let valid = e.valid_up_to();
                    if let Ok(text) = std::str::from_utf8(&rest[..valid]) {
                        out.push_str(text);
                    }
                    match e.error_len() {
                        Some(len) => {
                            out.push('\u{fffd}');
                            rest = &rest[valid + len..];
                        }
                        None => {
                            let held = rest[valid..].to_vec();
                            match kind {
                                'o' => self.partial_output = held,
                                _ => self.partial_input = held,
                            }
                            break;
                        }
                    }
                }
            }
        }
        out
    }

    async fn event(&mut self, kind: char, text: &str) -> Result<(), TranscriptError> {
        let seconds = self.started.elapsed().as_secs_f64();
        let encoded = serde_json::to_string(text)
            .map_err(|e| TranscriptError::Io(format!("could not encode an event: {e}")))?;
        let line = format!("[{seconds:.6}, \"{kind}\", {encoded}]\n");
        if self.size + line.len() as u64 > self.max_bytes {
            return Err(TranscriptError::Limit);
        }
        self.file
            .write_all(line.as_bytes())
            .await
            .map_err(|e| io_error("write the transcript", &self.path, e))?;
        self.hasher.update(line.as_bytes());
        self.size += line.len() as u64;
        Ok(())
    }
}

/// Removes a transcript row and its file.
///
/// # Errors
///
/// [`TranscriptError::Io`] when the row cannot be deleted.
pub async fn remove(store: &NodeStore, row: &StoredTranscript) -> Result<(), TranscriptError> {
    let _ = tokio::fs::remove_file(&row.path).await;
    store
        .delete_transcript(&row.session_id)
        .await
        .map_err(|e| TranscriptError::Io(format!("could not delete a transcript row: {e}")))
}

/// Deletes transcripts older than `limits.max_age_secs`, then the oldest until
/// the total fits `limits.total_bytes`.
///
/// Runs at agent start, after each terminal session and hourly. `now` is a Unix
/// second count so tests drive it.
///
/// # Errors
///
/// [`crate::AgentError`] when the rows cannot be read or deleted.
pub async fn sweep(
    store: &NodeStore,
    limits: TranscriptLimits,
    now: i64,
) -> Result<usize, crate::AgentError> {
    let mut rows = store.transcripts().await?;
    rows.sort_by_key(|row| (row.started_at, row.session_id.clone()));
    let mut removed = 0usize;
    let mut kept: Vec<StoredTranscript> = Vec::with_capacity(rows.len());
    for row in rows {
        let age = now.saturating_sub(row.started_at);
        if age > limits.max_age_secs {
            remove(store, &row).await.map_err(|e| {
                crate::AgentError::Store(format!("could not prune a transcript: {e}"))
            })?;
            removed += 1;
        } else {
            kept.push(row);
        }
    }

    let mut total: u64 = kept.iter().map(|row| row.size_bytes).sum();
    for row in &kept {
        if total <= limits.total_bytes {
            break;
        }
        if row.ended_at.is_none() {
            // A session still writing this file; its end prunes again.
            continue;
        }
        remove(store, row)
            .await
            .map_err(|e| crate::AgentError::Store(format!("could not prune a transcript: {e}")))?;
        total = total.saturating_sub(row.size_bytes);
        removed += 1;
    }
    Ok(removed)
}

async fn cleanup(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-transcript-{}",
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

    fn limits(max_bytes: u64) -> TranscriptLimits {
        TranscriptLimits {
            max_bytes,
            ..TranscriptLimits::default()
        }
    }

    fn lines(path: &Path) -> Vec<serde_json::Value> {
        let text = std::fs::read_to_string(path).unwrap();
        text.lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn the_header_and_events_are_asciicast_v2() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut writer = TranscriptWriter::create(
            &store,
            "trm_one",
            "vm_one",
            80,
            24,
            limits(1 << 20),
            1_793_614_590,
        )
        .await
        .unwrap();
        writer.output(b"hello\n").await.unwrap();
        writer.input(b"ls\n").await.unwrap();
        writer.resize(50, 132).await.unwrap();
        let (sha, size) = writer.finish().await.unwrap();

        let path = transcript_path(&store, "trm_one").unwrap();
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());
        let lines = lines(&path);
        assert_eq!(lines[0]["version"], 2);
        assert_eq!(lines[0]["width"], 80);
        assert_eq!(lines[0]["height"], 24);
        assert_eq!(lines[0]["timestamp"], 1_793_614_590);
        assert_eq!(lines[0]["env"]["TERM"], "xterm-256color");
        assert_eq!(lines[1][1], "o");
        assert_eq!(lines[1][2], "hello\n");
        assert_eq!(lines[2][1], "i");
        assert_eq!(lines[2][2], "ls\n");
        assert_eq!(lines[3][1], "r");
        assert_eq!(lines[3][2], "132x50");
        assert!(lines[1][0].as_f64().is_some(), "the time is a float");

        let mut hasher = Sha256::new();
        hasher.update(std::fs::read(&path).unwrap());
        assert_eq!(sha, util::hex(&hasher.finalize()));

        let row = store.transcript("trm_one").await.unwrap().unwrap();
        assert_eq!(row.size_bytes, size);
        assert_eq!(row.sha256, sha);
        assert_eq!(row.vm_id, "vm_one");
        assert!(row.ended_at.is_some());
    }

    #[tokio::test]
    async fn invalid_utf8_is_replaced_and_partial_bytes_are_kept() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut writer =
            TranscriptWriter::create(&store, "trm_utf8", "vm", 80, 24, limits(1 << 20), 0)
                .await
                .unwrap();
        // "é" is two bytes; split it across two chunks.
        writer.output(&[0xc3]).await.unwrap();
        writer.output(&[0xa9]).await.unwrap();
        writer.output(&[0xff, b'a']).await.unwrap();
        writer.finish().await.unwrap();
        let path = transcript_path(&store, "trm_utf8").unwrap();
        let lines = lines(&path);
        assert_eq!(lines[1][2], "é");
        assert_eq!(lines[2][2], "\u{fffd}a");
    }

    #[tokio::test]
    async fn an_event_past_the_cap_is_not_written() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut writer = TranscriptWriter::create(&store, "trm_cap", "vm", 80, 24, limits(300), 0)
            .await
            .unwrap();
        writer.output(&[b'a'; 100]).await.unwrap();
        assert!(matches!(
            writer.output(&[b'b'; 100]).await,
            Err(TranscriptError::Limit)
        ));
        let (_, size) = writer.finish().await.unwrap();
        let path = transcript_path(&store, "trm_cap").unwrap();
        assert!(size <= 300);
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());
        assert!(!std::fs::read_to_string(&path).unwrap().contains("bbb"));
    }

    #[tokio::test]
    async fn retention_drops_old_and_oversized_transcripts() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let now = 1_800_000_000i64;

        let mut rows = Vec::new();
        for (session, started, size) in [
            ("trm_old", now - 40 * 24 * 60 * 60, 10u64),
            ("trm_big", now - 60, 600),
            ("trm_small", now - 30, 100),
        ] {
            let mut writer =
                TranscriptWriter::create(&store, session, "vm", 80, 24, limits(1 << 20), started)
                    .await
                    .unwrap();
            writer
                .output(&vec![b'x'; usize::try_from(size).unwrap()])
                .await
                .unwrap();
            let _ = writer.finish().await.unwrap();
            rows.push(transcript_path(&store, session).unwrap());
        }

        let policy = TranscriptLimits {
            max_bytes: 1 << 20,
            max_age_secs: MAX_TRANSCRIPT_AGE_SECS,
            total_bytes: 500,
        };
        let removed = sweep(&store, policy, now).await.unwrap();
        // The 40-day-old one and the 600-byte one go; the small one stays.
        assert_eq!(removed, 2);
        assert!(!rows[0].exists());
        assert!(!rows[1].exists());
        assert!(rows[2].exists());
        let left = store.transcripts().await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].session_id, "trm_small");
    }
}
