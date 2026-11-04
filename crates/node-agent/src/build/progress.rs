// SPDX-License-Identifier: AGPL-3.0-only
//! BuildKit progress parsing (ADR 0119).
//!
//! The builder VM's tagged output lines arrive here one at a time. A line is
//! one of:
//!
//! - a builder marker the guest script prints, `quoro-build phase=<phase>`
//!   ([`MARKER`]), which becomes a [`BuildEvent::Phase`];
//! - one BuildKit `--progress=rawjson` object (a `SolveStatus`: `vertexes`,
//!   `statuses`, `logs`, `warnings`), which becomes human log lines in the
//!   style of `--progress=plain` (`#6 [3/4] RUN ...`, `#6 0.187 <log text>`,
//!   `#6 DONE 0.2s`, `#6 ERROR: ...`), and updates what the parser knows
//!   about each vertex;
//! - anything else (BuildKit's final plain error with the Dockerfile excerpt,
//!   shell output), which is kept as a plain line.
//!
//! The guest is untrusted: every input is bounded and parse errors never
//! panic. A line over [`MAX_LINE_BYTES`] is cut and kept as a plain line, at
//! most [`MAX_VERTICES`] vertices are tracked (later ones lose their number),
//! a partial log line is buffered up to [`MAX_PARTIAL_BYTES`], and only the
//! last [`MAX_STEP_LOG_LINES`] log lines of a vertex are remembered for the
//! failure excerpt.

use std::collections::{HashMap, VecDeque};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use quoro_proto::wire::{BuildPhase, FailedStep, LogStream};
use serde::Deserialize;

use super::{BuildEvent, MAX_ERROR_CHARS, MAX_EXCERPT_CHARS, MAX_EXCERPT_LINES, truncate_chars};
use crate::util::parse_rfc3339_nanos;

/// The prefix of a marker line the builder's guest script prints.
pub const MARKER: &str = "quoro-build ";

/// The longest input line that is looked at; a longer line is cut and kept as
/// a plain line.
const MAX_LINE_BYTES: usize = 64 * 1024;

/// The most vertices that get a number. Later ones are still printed, just
/// without a `#N`.
const MAX_VERTICES: usize = 10_000;

/// The most partial log text buffered for one vertex.
const MAX_PARTIAL_BYTES: usize = 64 * 1024;

/// The most step log lines remembered for the failure excerpt.
const MAX_STEP_LOG_LINES: usize = 10;

/// The most statuses tracked, keyed by vertex and status id.
const MAX_STATUS_IDS: usize = 10_000;

/// The most plain lines kept to find the Dockerfile excerpt.
const MAX_PLAIN_LINES: usize = 64;

/// The longest vertex name, status id or step log line kept in a line.
const MAX_NAME_CHARS: usize = MAX_EXCERPT_CHARS;

/// A vertex name that marks the export phase.
const EXPORTING_PREFIX: &str = "exporting to";

/// Turns builder output into build events and remembers the failing step.
#[derive(Debug, Default)]
pub struct ProgressParser {
    /// Tracked vertices, in order of first appearance; `vertices[i].number`
    /// is `i + 1`.
    vertices: Vec<Vertex>,
    /// Vertex digest to its index in `vertices`.
    numbers: HashMap<String, usize>,
    /// Status keys seen so far, and whether their completion was reported.
    statuses: HashMap<(usize, String), bool>,
    /// Whether `BuildEvent::Phase(Exporting)` was emitted.
    exporting_seen: bool,
    /// The last plain lines, for the Dockerfile excerpt of a failure.
    plain: VecDeque<String>,
    /// The line of the Dockerfile the build failed at, when BuildKit printed
    /// an excerpt.
    dockerfile_line: Option<u32>,
    /// The Dockerfile excerpt block, from its `<path>:<line>` line through the
    /// closing dashed line.
    excerpt: Vec<String>,
}

/// What the parser knows about one BuildKit vertex.
#[derive(Debug, Default)]
struct Vertex {
    /// The `#N` of this vertex.
    number: u32,
    name: String,
    /// The start of the current run, in nanoseconds since the Unix epoch.
    started_ns: Option<i64>,
    /// True between a start that was announced and the completion that ended
    /// it.
    running: bool,
    /// The last non-empty error, kept for [`ProgressParser::failed_step`].
    error: Option<String>,
    /// Log text received without a closing newline yet.
    pending: String,
    /// The stream of `pending`.
    pending_stream: Option<LogStream>,
    /// The last `MAX_STEP_LOG_LINES` log lines, oldest first.
    log: VecDeque<String>,
}

impl ProgressParser {
    /// An empty parser for one build.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one output line (without its newline) and returns the events it
    /// produced, in order.
    pub fn push(&mut self, stream: LogStream, line: &str) -> Vec<BuildEvent> {
        if line.len() > MAX_LINE_BYTES {
            let end = floor_boundary(line, MAX_LINE_BYTES);
            return self.plain_line(stream, &line[..end]);
        }
        if line.starts_with(MARKER) {
            return marker_phase(line)
                .map(|phase| vec![BuildEvent::Phase(phase)])
                .unwrap_or_default();
        }
        match parse_status(line) {
            Some(status) => self.status_events(status),
            None => self.plain_line(stream, line),
        }
    }

    /// Flushes the log text still buffered without a closing newline, at the
    /// end of the builder's output.
    pub fn finish(&mut self) -> Vec<BuildEvent> {
        let mut events = Vec::new();
        for index in 0..self.vertices.len() {
            self.flush_pending(index, &mut events);
        }
        events
    }

    /// The step that failed, once BuildKit reported a vertex error: the
    /// vertex's number, its name, its error, the Dockerfile line from the
    /// plain error excerpt when there was one, and an excerpt (the step's last
    /// log lines and the Dockerfile excerpt), bounded by the limits in
    /// [`super`].
    #[must_use]
    pub fn failed_step(&self) -> Option<FailedStep> {
        let vertex = self.failing_vertex()?;
        let error = vertex.error.as_deref().unwrap_or_default();

        let mut excerpt: Vec<String> = self
            .excerpt
            .iter()
            .take(MAX_EXCERPT_LINES)
            .map(|line| truncate_chars(line, MAX_EXCERPT_CHARS))
            .collect();
        let room = MAX_EXCERPT_LINES.saturating_sub(excerpt.len());
        let skip = vertex.log.len().saturating_sub(room);
        let mut step_log: Vec<String> = vertex
            .log
            .iter()
            .skip(skip)
            .map(|line| truncate_chars(line, MAX_EXCERPT_CHARS))
            .collect();
        step_log.append(&mut excerpt);

        Some(FailedStep {
            vertex: vertex.number,
            name: truncate_chars(&vertex.name, MAX_NAME_CHARS),
            error: truncate_chars(error, MAX_ERROR_CHARS),
            dockerfile_line: self.dockerfile_line,
            excerpt: step_log,
        })
    }

    /// The vertex to report: the last one that failed without being a
    /// cancellation, preferring a step over an internal vertex.
    fn failing_vertex(&self) -> Option<&Vertex> {
        let mut chosen: Option<&Vertex> = None;
        for vertex in &self.vertices {
            let Some(error) = vertex.error.as_deref().filter(|error| !error.is_empty()) else {
                continue;
            };
            if is_cancellation(error) {
                continue;
            }
            let replace = match chosen {
                None => true,
                Some(previous) => {
                    previous.name.starts_with("[internal]")
                        || !vertex.name.starts_with("[internal]")
                }
            };
            if replace {
                chosen = Some(vertex);
            }
        }
        chosen
    }

    /// Keeps a plain line as one event and remembers it for the Dockerfile
    /// excerpt of a failure.
    fn plain_line(&mut self, stream: LogStream, line: &str) -> Vec<BuildEvent> {
        self.remember_plain(line);
        vec![BuildEvent::Line {
            stream,
            vertex: None,
            text: line.to_owned(),
        }]
    }

    /// Appends one plain line to the tail kept for the Dockerfile excerpt, and
    /// records the excerpt when this line closes one.
    fn remember_plain(&mut self, line: &str) {
        if self.plain.len() == MAX_PLAIN_LINES {
            self.plain.pop_front();
        }
        self.plain
            .push_back(truncate_chars(line, MAX_EXCERPT_CHARS));
        if !is_dashed(line) {
            return;
        }
        let count = self.plain.len();
        for index in (0..count.saturating_sub(1)).rev() {
            if !is_dashed(&self.plain[index]) {
                continue;
            }
            let Some(location) = index.checked_sub(1).map(|at| &self.plain[at]) else {
                return;
            };
            if let Some((path, line_number)) = split_location(location)
                && !path.is_empty()
            {
                self.excerpt = self.plain.iter().skip(index - 1).cloned().collect();
                self.dockerfile_line = Some(line_number);
            }
            return;
        }
    }

    /// Turns one `SolveStatus` into events, in BuildKit's order.
    fn status_events(&mut self, status: RawStatus) -> Vec<BuildEvent> {
        let mut events = Vec::new();
        for vertex in &status.vertexes {
            self.vertex_update(vertex, &mut events);
        }
        for entry in &status.statuses {
            self.status_update(entry, &mut events);
        }
        for log in &status.logs {
            self.log_chunk(log, &mut events);
        }
        for warning in &status.warnings {
            self.warning_update(warning, &mut events);
        }
        events
    }

    /// Applies one vertex update: its name on the first start of a run, then
    /// its completion or its error.
    fn vertex_update(&mut self, raw: &RawVertex, events: &mut Vec<BuildEvent>) {
        let Some(index) = self.vertex_index(&raw.digest) else {
            if !raw.name.is_empty() {
                events.push(plain_text(truncate_chars(&raw.name, MAX_NAME_CHARS)));
            }
            return;
        };
        if !raw.name.is_empty() && self.vertices[index].name.is_empty() {
            self.vertices[index].name = truncate_chars(&raw.name, MAX_NAME_CHARS);
        }
        let number = self.vertices[index].number;

        if let Some(started) = raw.started.as_deref().and_then(parse_rfc3339_nanos) {
            self.vertices[index].started_ns = Some(started);
            if !self.vertices[index].running {
                let name = self.vertices[index].name.clone();
                self.vertices[index].running = true;
                if name.starts_with(EXPORTING_PREFIX) && !self.exporting_seen {
                    self.exporting_seen = true;
                    events.push(BuildEvent::Phase(BuildPhase::Exporting));
                }
                events.push(vertex_text(number, name));
            }
        }

        if let Some(error) = raw.error.as_deref().filter(|error| !error.is_empty()) {
            self.vertices[index].error = Some(truncate_chars(error, MAX_ERROR_CHARS));
            self.vertices[index].running = false;
            self.flush_pending(index, events);
            events.push(vertex_text(
                number,
                format!("ERROR: {}", truncate_chars(error, MAX_ERROR_CHARS)),
            ));
            return;
        }

        let Some(completed) = raw.completed.as_deref().and_then(parse_rfc3339_nanos) else {
            return;
        };
        self.vertices[index].running = false;
        self.flush_pending(index, events);
        if raw.cached {
            events.push(vertex_text(number, "CACHED".to_owned()));
            return;
        }
        let started = self.vertices[index].started_ns.unwrap_or(completed);
        let seconds = (completed - started).max(0) as f64 / 1_000_000_000.0;
        events.push(vertex_text(number, format!("DONE {seconds:.1}s")));
    }

    /// Reports a status the first time it starts and the first time it
    /// completes.
    fn status_update(&mut self, raw: &RawStatusEntry, events: &mut Vec<BuildEvent>) {
        let Some(index) = self.numbers.get(&truncate_chars(&raw.vertex, 128)).copied() else {
            return;
        };
        let identifier = if raw.id.is_empty() {
            truncate_chars(&raw.name, MAX_NAME_CHARS)
        } else {
            truncate_chars(&raw.id, MAX_NAME_CHARS)
        };
        if identifier.is_empty() {
            return;
        }
        let key = (index, identifier.clone());
        let started = raw.started.is_some();
        let completed = raw.completed.is_some();
        match self.statuses.get(&key).copied() {
            Some(true) => return,
            Some(false) => {
                if !completed {
                    return;
                }
                self.statuses.insert(key, true);
            }
            None => {
                if self.statuses.len() >= MAX_STATUS_IDS {
                    return;
                }
                if started && !completed {
                    self.statuses.insert(key, false);
                } else if completed {
                    self.statuses.insert(key, true);
                } else {
                    return;
                }
            }
        }
        let number = self.vertices[index].number;
        if completed {
            events.push(vertex_text(number, format!("{identifier} done")));
        } else {
            events.push(vertex_text(number, identifier));
        }
    }

    /// Decodes one log chunk, joining it with the text still buffered for the
    /// vertex, and emits every line it completes.
    fn log_chunk(&mut self, raw: &RawLog, events: &mut Vec<BuildEvent>) {
        let stream = if raw.stream == 2 {
            LogStream::Stderr
        } else {
            LogStream::Stdout
        };
        let Ok(bytes) = STANDARD.decode(raw.data.as_bytes()) else {
            events.push(BuildEvent::Line {
                stream,
                vertex: None,
                text: truncate_chars(&raw.data, MAX_EXCERPT_CHARS),
            });
            return;
        };
        let text = String::from_utf8_lossy(&bytes);
        let index = self.numbers.get(&truncate_chars(&raw.vertex, 128)).copied();
        match index {
            Some(index) => self.append_log(index, stream, &text, events),
            None => {
                for line in text.split('\n') {
                    if !line.is_empty() {
                        events.push(BuildEvent::Line {
                            stream,
                            vertex: None,
                            text: truncate_chars(line, MAX_NAME_CHARS),
                        });
                    }
                }
            }
        }
    }

    /// Appends decoded log text to a vertex: the joined text up to the last
    /// newline becomes lines, the rest stays buffered.
    fn append_log(
        &mut self,
        index: usize,
        stream: LogStream,
        text: &str,
        events: &mut Vec<BuildEvent>,
    ) {
        let vertex = &mut self.vertices[index];
        if vertex
            .pending_stream
            .is_some_and(|pending| pending != stream)
        {
            let pending = std::mem::take(&mut vertex.pending);
            let old = vertex.pending_stream.unwrap_or(LogStream::Stdout);
            vertex.pending_stream = None;
            if !pending.is_empty() {
                push_step_line(vertex, old, pending, events);
            }
        }
        vertex.pending_stream = Some(stream);

        let mut parts: Vec<&str> = text.split('\n').collect();
        let last = parts.pop().unwrap_or_default();
        if parts.is_empty() {
            // No newline yet: the whole chunk joins the buffered line.
            let room = MAX_PARTIAL_BYTES.saturating_sub(vertex.pending.len());
            if vertex.pending.len() < MAX_PARTIAL_BYTES {
                vertex.pending.push_str(&last[..floor_boundary(last, room)]);
            }
            return;
        }

        let first = parts.remove(0);
        let room = MAX_PARTIAL_BYTES.saturating_sub(vertex.pending.len());
        if vertex.pending.len() < MAX_PARTIAL_BYTES {
            vertex
                .pending
                .push_str(&first[..floor_boundary(first, room)]);
        }
        let pending = std::mem::take(&mut vertex.pending);
        push_step_line(vertex, stream, pending, events);
        for line in parts {
            push_step_line(vertex, stream, line.to_owned(), events);
        }
        let end = floor_boundary(last, MAX_PARTIAL_BYTES);
        vertex.pending = last[..end].to_owned();
    }

    /// Emits the partial line buffered for one vertex, when there is one.
    fn flush_pending(&mut self, index: usize, events: &mut Vec<BuildEvent>) {
        let vertex = &mut self.vertices[index];
        let stream = match vertex.pending_stream.take() {
            Some(stream) => stream,
            None => return,
        };
        let pending = std::mem::take(&mut vertex.pending);
        if pending.is_empty() {
            return;
        }
        push_step_line(vertex, stream, pending, events);
    }

    /// Emits `#N WARN: <short>` for one BuildKit warning.
    fn warning_update(&mut self, raw: &RawWarning, events: &mut Vec<BuildEvent>) {
        let Some(index) = self.numbers.get(&truncate_chars(&raw.vertex, 128)).copied() else {
            return;
        };
        let text = if raw.short.is_empty() {
            &raw.detail
        } else {
            &raw.short
        };
        if text.is_empty() {
            return;
        }
        let number = self.vertices[index].number;
        events.push(vertex_text(
            number,
            format!("WARN: {}", truncate_chars(text, MAX_NAME_CHARS)),
        ));
    }

    /// The index of the vertex for `digest`, allocating its number on first
    /// appearance. `None` once [`MAX_VERTICES`] vertices are tracked.
    fn vertex_index(&mut self, digest: &str) -> Option<usize> {
        let key = truncate_chars(digest, 128);
        if let Some(&index) = self.numbers.get(&key) {
            return Some(index);
        }
        if key.is_empty() || self.vertices.len() >= MAX_VERTICES {
            return None;
        }
        let index = self.vertices.len();
        let number = u32::try_from(index + 1).ok()?;
        self.vertices.push(Vertex {
            number,
            ..Vertex::default()
        });
        self.numbers.insert(key, index);
        Some(index)
    }
}

/// One line belonging to a vertex.
fn vertex_text(number: u32, text: String) -> BuildEvent {
    BuildEvent::Line {
        stream: LogStream::Stdout,
        vertex: Some(number),
        text: format!("#{number} {text}"),
    }
}

/// One line that belongs to no vertex.
fn plain_text(text: String) -> BuildEvent {
    BuildEvent::Line {
        stream: LogStream::Stdout,
        vertex: None,
        text,
    }
}

/// Remembers one step log line and emits it with the vertex's number.
fn push_step_line(
    vertex: &mut Vertex,
    stream: LogStream,
    text: String,
    events: &mut Vec<BuildEvent>,
) {
    if vertex.log.len() == MAX_STEP_LOG_LINES {
        vertex.log.pop_front();
    }
    vertex
        .log
        .push_back(truncate_chars(&text, MAX_EXCERPT_CHARS));
    events.push(BuildEvent::Line {
        stream,
        vertex: Some(vertex.number),
        text: format!("#{} {text}", vertex.number),
    });
}

/// The phase of a marker line, when it names a known one.
fn marker_phase(line: &str) -> Option<BuildPhase> {
    let value = line
        .strip_prefix(MARKER)?
        .trim()
        .strip_prefix("phase=")?
        .trim();
    match value {
        "preparing" => Some(BuildPhase::Preparing),
        "building" => Some(BuildPhase::Building),
        "exporting" => Some(BuildPhase::Exporting),
        _ => None,
    }
}

/// Parses one `rawjson` line. `None` when the line is not an object with any
/// of the four `SolveStatus` lists.
fn parse_status(line: &str) -> Option<RawStatus> {
    let status: RawStatus = serde_json::from_str(line).ok()?;
    if status.vertexes.is_empty()
        && status.statuses.is_empty()
        && status.logs.is_empty()
        && status.warnings.is_empty()
    {
        return None;
    }
    Some(status)
}

/// True for the dashed line that closes a Dockerfile excerpt.
fn is_dashed(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 3 && trimmed.bytes().all(|byte| byte == b'-')
}

/// Splits a `<path>:<line>` excerpt location into its path and line number.
fn split_location(line: &str) -> Option<(&str, u32)> {
    let (path, number) = line.trim_end().rsplit_once(':')?;
    let line_number: u32 = number.parse().ok()?;
    Some((path, line_number))
}

/// True for an error that means the build was cancelled, not that a step
/// failed.
fn is_cancellation(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("context canceled") || error.contains("canceled") || error.contains("cancelled")
}

/// The largest index no greater than `max` that is a character boundary in
/// `text`.
fn floor_boundary(text: &str, max: usize) -> usize {
    if text.len() <= max {
        return text.len();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// A `SolveStatus` as BuildKit sends it with `--progress=rawjson`.
#[derive(Debug, Default, Deserialize)]
struct RawStatus {
    #[serde(default)]
    vertexes: Vec<RawVertex>,
    #[serde(default)]
    statuses: Vec<RawStatusEntry>,
    #[serde(default)]
    logs: Vec<RawLog>,
    #[serde(default)]
    warnings: Vec<RawWarning>,
}

/// One vertex update.
#[derive(Debug, Default, Deserialize)]
struct RawVertex {
    #[serde(default)]
    digest: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    started: Option<String>,
    #[serde(default)]
    completed: Option<String>,
    #[serde(default)]
    cached: bool,
    #[serde(default)]
    error: Option<String>,
}

/// One progress status of a vertex.
#[derive(Debug, Default, Deserialize)]
struct RawStatusEntry {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    vertex: String,
    #[serde(default)]
    started: Option<String>,
    #[serde(default)]
    completed: Option<String>,
}

/// One log chunk of a vertex.
#[derive(Debug, Default, Deserialize)]
struct RawLog {
    #[serde(default)]
    vertex: String,
    #[serde(default)]
    stream: i64,
    #[serde(default)]
    data: String,
}

/// One warning of a vertex.
#[derive(Debug, Default, Deserialize)]
struct RawWarning {
    #[serde(default)]
    vertex: String,
    #[serde(default)]
    short: String,
    #[serde(default)]
    detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAIL_JSON: &str = include_str!("testdata/fail-rawjson.jsonl");
    const FAIL_TRAILER: &str = include_str!("testdata/fail-trailer.txt");

    /// Feeds every line of `text` with the stdout stream.
    fn push_lines(parser: &mut ProgressParser, text: &str) -> Vec<BuildEvent> {
        let mut events = Vec::new();
        for line in text.lines() {
            events.extend(parser.push(LogStream::Stdout, line));
        }
        events
    }

    /// The text of every line event, in order.
    fn texts(events: &[BuildEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| match event {
                BuildEvent::Line { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn failing_build_becomes_plain_lines_and_a_failed_step() {
        let mut parser = ProgressParser::new();
        let mut events = push_lines(&mut parser, FAIL_JSON);
        events.extend(push_lines(&mut parser, FAIL_TRAILER));

        // Vertices are numbered by first appearance.
        assert_eq!(
            texts(&events)[0],
            "#1 [internal] load build definition from Dockerfile"
        );

        let step: Vec<&BuildEvent> = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    BuildEvent::Line {
                        vertex: Some(5),
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(
            texts(&step.iter().copied().cloned().collect::<Vec<_>>()),
            vec![
                "#5 [3/4] RUN echo \"about to fail\" && ls /does-not-exist".to_owned(),
                "#5 about to fail".to_owned(),
                "#5 ls: /does-not-exist: No such file or directory".to_owned(),
                "#5 ERROR: process \"/bin/sh -c echo \\\"about to fail\\\" && ls /does-not-exist\" \
                 did not complete successfully: exit code: 1"
                    .to_owned(),
            ]
        );
        assert!(matches!(
            step[2],
            BuildEvent::Line {
                stream: LogStream::Stderr,
                ..
            }
        ));
        // The name is printed once, not on every update of the vertex.
        let names = events
            .iter()
            .filter(|event| {
                matches!(event, BuildEvent::Line { text, .. }
                    if text.contains("[3/4] RUN"))
            })
            .count();
        assert_eq!(names, 1);

        let failed = parser.failed_step().expect("a failed step");
        assert_eq!(failed.vertex, 5);
        assert_eq!(
            failed.name,
            "[3/4] RUN echo \"about to fail\" && ls /does-not-exist"
        );
        assert!(failed.error.contains("exit code: 1"));
        assert_eq!(failed.dockerfile_line, Some(3));
        assert!(
            failed
                .excerpt
                .iter()
                .any(|line| line == "ls: /does-not-exist: No such file or directory")
        );
        assert!(failed.excerpt.iter().any(|line| line == "Dockerfile:3"));
        assert_eq!(
            failed.excerpt.last().map(String::as_str),
            Some("--------------------")
        );
        assert!(failed.excerpt.len() <= MAX_EXCERPT_LINES);
    }

    #[test]
    fn cached_vertex_is_one_cached_line() {
        let mut parser = ProgressParser::new();
        let events = parser.push(
            LogStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:aa","name":"[2/2] RUN true","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:00.050000000Z","cached":true}]}"#,
        );
        assert_eq!(texts(&events), vec!["#1 [2/2] RUN true", "#1 CACHED"]);
        assert!(parser.failed_step().is_none());
    }

    #[test]
    fn completed_vertex_reports_its_duration() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:ad","name":"[1/1] RUN true","started":"2026-11-04T00:00:00.000000000Z"}]}
{"vertexes":[{"digest":"sha256:ad","name":"[1/1] RUN true","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:00.250000000Z"}]}"#,
        );
        let events = parser.push(
            LogStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:ae","name":"[2/2] RUN true","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:01.000000000Z"}]}"#,
        );
        // A vertex seen for the first time with both times prints its name
        // and its duration.
        assert_eq!(texts(&events), vec!["#2 [2/2] RUN true", "#2 DONE 1.0s"]);
    }

    #[test]
    fn log_chunks_join_across_objects() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:bb","name":"[1/1] RUN true","started":"2026-11-04T00:00:00.000000000Z"}]}"#,
        );
        let first = parser.push(
            LogStream::Stdout,
            r#"{"logs":[{"vertex":"sha256:bb","stream":1,"data":"YWJvdXQgdG8g"}]}"#,
        );
        assert!(first.is_empty());
        let second = parser.push(
            LogStream::Stdout,
            r#"{"logs":[{"vertex":"sha256:bb","stream":1,"data":"ZmFpbAo="}]}"#,
        );
        assert_eq!(texts(&second), vec!["#1 about to fail"]);
    }

    #[test]
    fn a_partial_log_line_is_flushed_at_the_end() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:bc","name":"[1/1] RUN true","started":"2026-11-04T00:00:00.000000000Z"}]}"#,
        );
        let events = parser.push(
            LogStream::Stdout,
            r#"{"logs":[{"vertex":"sha256:bc","stream":1,"data":"cGFydGlhbA=="}]}"#,
        );
        assert!(events.is_empty());
        assert_eq!(texts(&parser.finish()), vec!["#1 partial"]);
    }

    #[test]
    fn exporting_vertex_starts_the_exporting_phase() {
        let mut parser = ProgressParser::new();
        let events = parser.push(
            LogStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:cc","name":"exporting to oci image format","started":"2026-11-04T00:00:00.000000000Z"}]}"#,
        );
        assert_eq!(
            events,
            vec![
                BuildEvent::Phase(BuildPhase::Exporting),
                BuildEvent::Line {
                    stream: LogStream::Stdout,
                    vertex: Some(1),
                    text: "#1 exporting to oci image format".to_owned(),
                },
            ]
        );
        // The phase is reported once.
        let again = parser.push(
            LogStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:cd","name":"exporting to oci image format","started":"2026-11-04T00:00:01.000000000Z"}]}"#,
        );
        assert_eq!(texts(&again), vec!["#2 exporting to oci image format"]);
    }

    #[test]
    fn marker_lines_become_phases() {
        let mut parser = ProgressParser::new();
        assert_eq!(
            parser.push(LogStream::Stdout, "quoro-build phase=preparing"),
            vec![BuildEvent::Phase(BuildPhase::Preparing)]
        );
        assert_eq!(
            parser.push(LogStream::Stderr, "quoro-build phase=exporting"),
            vec![BuildEvent::Phase(BuildPhase::Exporting)]
        );
        assert!(
            parser
                .push(LogStream::Stdout, "quoro-build phase=unknown")
                .is_empty()
        );
    }

    #[test]
    fn other_lines_stay_plain() {
        let mut parser = ProgressParser::new();
        let events = parser.push(LogStream::Stderr, "some shell output");
        assert_eq!(
            events,
            vec![BuildEvent::Line {
                stream: LogStream::Stderr,
                vertex: None,
                text: "some shell output".to_owned(),
            }]
        );
    }

    #[test]
    fn a_huge_line_is_cut_not_parsed() {
        let mut parser = ProgressParser::new();
        let line = format!("{{\"vertexes\":[{{\"name\":\"{}\"}}]", "x".repeat(70_000));
        let events = parser.push(LogStream::Stdout, &line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            BuildEvent::Line { vertex, text, .. } => {
                assert!(vertex.is_none());
                assert_eq!(text.len(), MAX_LINE_BYTES);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(parser.failed_step().is_none());
    }

    #[test]
    fn invalid_base64_degrades_to_a_plain_line() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:ee","name":"[1/1] RUN true","started":"2026-11-04T00:00:00.000000000Z"}]}"#,
        );
        let events = parser.push(
            LogStream::Stderr,
            r#"{"logs":[{"vertex":"sha256:ee","stream":2,"data":"not base64!!"}]}"#,
        );
        assert_eq!(
            events,
            vec![BuildEvent::Line {
                stream: LogStream::Stderr,
                vertex: None,
                text: "not base64!!".to_owned(),
            }]
        );
    }

    #[test]
    fn invalid_json_degrades_to_a_plain_line() {
        let mut parser = ProgressParser::new();
        let events = parser.push(LogStream::Stdout, "{not json");
        assert_eq!(texts(&events), vec!["{not json"]);
    }

    #[test]
    fn a_cancellation_is_not_a_failed_step() {
        let mut parser = ProgressParser::new();
        let events = parser.push(
            LogStream::Stdout,
            r#"{"vertexes":[{"digest":"sha256:ff","name":"[1/1] RUN sleep 60","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:05.000000000Z","error":"context canceled"}]}"#,
        );
        assert_eq!(
            texts(&events),
            vec![
                "#1 [1/1] RUN sleep 60".to_owned(),
                "#1 ERROR: context canceled".to_owned(),
            ]
        );
        assert!(parser.failed_step().is_none());
    }

    #[test]
    fn statuses_start_and_complete_once() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:11","name":"[1/1] FROM alpine:3.22","started":"2026-11-04T00:00:00.000000000Z"}]}"#,
        );
        let started = parser.push(
            LogStream::Stdout,
            r#"{"statuses":[{"id":"resolve alpine:3.22","vertex":"sha256:11","name":"resolve","started":"2026-11-04T00:00:00.100000000Z"}]}"#,
        );
        assert_eq!(texts(&started), vec!["#1 resolve alpine:3.22"]);
        let progress = parser.push(
            LogStream::Stdout,
            r#"{"statuses":[{"id":"resolve alpine:3.22","vertex":"sha256:11","name":"resolve","started":"2026-11-04T00:00:00.100000000Z"}]}"#,
        );
        assert!(progress.is_empty());
        let done = parser.push(
            LogStream::Stdout,
            r#"{"statuses":[{"id":"resolve alpine:3.22","vertex":"sha256:11","name":"resolve","started":"2026-11-04T00:00:00.100000000Z","completed":"2026-11-04T00:00:00.400000000Z"}]}"#,
        );
        assert_eq!(texts(&done), vec!["#1 resolve alpine:3.22 done"]);
    }

    #[test]
    fn an_internal_vertex_is_not_preferred_over_a_step() {
        let mut parser = ProgressParser::new();
        push_lines(
            &mut parser,
            r#"{"vertexes":[{"digest":"sha256:21","name":"[1/1] RUN false","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:01.000000000Z","error":"exit code: 1"}]}
{"vertexes":[{"digest":"sha256:22","name":"[internal] load metadata","started":"2026-11-04T00:00:00.000000000Z","completed":"2026-11-04T00:00:01.000000000Z","error":"context canceled"}]}"#,
        );
        let failed = parser.failed_step().expect("a failed step");
        assert_eq!(failed.vertex, 1);
        assert_eq!(failed.name, "[1/1] RUN false");
    }
}
