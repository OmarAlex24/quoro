// SPDX-License-Identifier: AGPL-3.0-only
//! Reader for Cloud Hypervisor's `--event-monitor` file (engine-trait.md §5).
//!
//! Cloud Hypervisor appends pretty-printed JSON objects back to back, with no
//! separator between them, to the file given with `--event-monitor path=…`. A
//! complete object is one event; a trailing object that is still being written
//! is kept for the next poll. The offset makes each call read only what was
//! appended since the previous one.

use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::PathBuf;

use serde_json::Value;

/// One event from the event monitor: its source and its name.
///
/// Cloud Hypervisor also sends a timestamp and a property bag; neither is part
/// of the engine's decisions, so they are not carried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChEvent {
    /// Where the event came from, for example `vm` or `vmm`.
    pub source: String,
    /// The event name, for example `booted` or `rebooting`.
    pub event: String,
}

/// An incremental reader of one event-monitor file.
pub struct EventLog {
    path: PathBuf,
    offset: u64,
    /// Bytes of a record that was not complete in the previous poll.
    pending: Vec<u8>,
}

/// Largest number of bytes held for one incomplete record before the file is
/// treated as garbage, so a corrupt file cannot grow memory without bound.
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const READ_CHUNK: usize = 64 * 1024;

impl EventLog {
    /// Opens a reader for `path`, starting at offset 0.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            offset: 0,
            pending: Vec::new(),
        }
    }

    /// Reads what was appended since the last call.
    ///
    /// Returns the complete events and keeps an incomplete trailing object for
    /// the next call. A missing file is `Ok` with no events.
    pub fn poll(&mut self) -> std::io::Result<Vec<ChEvent>> {
        let mut file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        let length = file.metadata()?.len();
        if length < self.offset {
            // The file was truncated or replaced; start again from the top.
            self.offset = 0;
            self.pending.clear();
        }
        if length == self.offset {
            return Ok(Vec::new());
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut appended = Vec::new();
        let mut chunk = [0u8; READ_CHUNK];
        loop {
            let read = file.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            appended.extend_from_slice(&chunk[..read]);
        }
        self.offset += appended.len() as u64;

        let mut buffer = std::mem::take(&mut self.pending);
        buffer.extend_from_slice(&appended);
        let (events, consumed) = parse_events(&buffer)?;
        let rest = &buffer[consumed..];
        if rest.len() > MAX_RECORD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{}: event record exceeds {MAX_RECORD_BYTES} bytes",
                    self.path.display()
                ),
            ));
        }
        self.pending = rest.to_vec();
        Ok(events)
    }
}

/// Parses every complete event in `buffer` and returns them with the offset
/// where the first incomplete record starts.
fn parse_events(buffer: &[u8]) -> std::io::Result<(Vec<ChEvent>, usize)> {
    let mut events = Vec::new();
    let mut stream = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();
    let mut consumed;
    loop {
        let start = stream.byte_offset();
        match stream.next() {
            None => {
                // The stream ended: everything left is whitespace between
                // records, so nothing needs to be carried into the next poll.
                consumed = start;
                while consumed < buffer.len() && buffer[consumed].is_ascii_whitespace() {
                    consumed += 1;
                }
                break;
            }
            Some(Ok(value)) => {
                events.push(to_event(&value)?);
            }
            Some(Err(err)) if err.is_eof() => {
                // The object is still being written; keep it for the next poll,
                // without the whitespace that separated it from the last one.
                consumed = start;
                while consumed < buffer.len() && buffer[consumed].is_ascii_whitespace() {
                    consumed += 1;
                }
                break;
            }
            Some(Err(err)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("event monitor: {err}"),
                ));
            }
        }
    }
    Ok((events, consumed))
}

/// Maps one event-record JSON object to a [`ChEvent`].
fn to_event(value: &Value) -> std::io::Result<ChEvent> {
    let source = value.get("source").and_then(Value::as_str).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "event monitor record has no string \"source\"",
        )
    })?;
    let event = value.get("event").and_then(Value::as_str).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "event monitor record has no string \"event\"",
        )
    })?;
    Ok(ChEvent {
        source: source.to_owned(),
        event: event.to_owned(),
    })
}

/// True when the events contain a guest reset (source `vm`, event `rebooting`
/// or `rebooted`).
pub fn saw_reset(events: &[ChEvent]) -> bool {
    events.iter().any(|event| {
        event.source == "vm" && matches!(event.event.as_str(), "rebooting" | "rebooted")
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::path::PathBuf;

    use super::{ChEvent, EventLog, parse_events, saw_reset};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-events-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn record(source: &str, event: &str) -> String {
        format!(
            "{{\n  \"timestamp\": {{\n    \"secs\": 1,\n    \"nanos\": 2\n  }},\n  \"source\": \"{source}\",\n  \"event\": \"{event}\",\n  \"properties\": null\n}}\n"
        )
    }

    #[test]
    fn parses_several_back_to_back_objects() {
        let text = format!(
            "{}{}{}",
            record("vmm", "started"),
            record("vm", "booted"),
            record("vm", "poweroff")
        );
        let (events, consumed) = parse_events(text.as_bytes()).expect("parse");
        assert_eq!(consumed, text.len());
        assert_eq!(
            events,
            vec![
                ChEvent {
                    source: "vmm".to_owned(),
                    event: "started".to_owned()
                },
                ChEvent {
                    source: "vm".to_owned(),
                    event: "booted".to_owned()
                },
                ChEvent {
                    source: "vm".to_owned(),
                    event: "poweroff".to_owned()
                },
            ]
        );
    }

    #[test]
    fn keeps_an_incomplete_trailing_object() {
        let complete = record("vm", "booted");
        let partial = &record("vm", "rebooting")[..20];
        let text = format!("{complete}{partial}");
        let (events, consumed) = parse_events(text.as_bytes()).expect("parse");
        assert_eq!(events.len(), 1);
        assert_eq!(consumed, complete.len());
        assert_eq!(&text.as_bytes()[consumed..], partial.as_bytes());
    }

    #[test]
    fn garbage_is_an_error() {
        let err = parse_events(b"this is not json").expect_err("garbage");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let err = parse_events(b"{\"source\": \"vm\", }").expect_err("syntax");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_record_without_source_or_event_is_an_error() {
        assert!(parse_events(b"{\"event\": \"booted\"}").is_err());
        assert!(parse_events(b"{\"source\": \"vm\"}").is_err());
        assert!(parse_events(b"[1, 2]").is_err());
    }

    #[test]
    fn reset_detection() {
        let booted = ChEvent {
            source: "vm".to_owned(),
            event: "booted".to_owned(),
        };
        let rebooting = ChEvent {
            source: "vm".to_owned(),
            event: "rebooting".to_owned(),
        };
        let rebooted = ChEvent {
            source: "vm".to_owned(),
            event: "rebooted".to_owned(),
        };
        let other = ChEvent {
            source: "vmm".to_owned(),
            event: "rebooting".to_owned(),
        };
        assert!(!saw_reset(&[]));
        assert!(!saw_reset(std::slice::from_ref(&booted)));
        assert!(saw_reset(&[booted.clone(), rebooting]));
        assert!(saw_reset(&[rebooted]));
        assert!(!saw_reset(&[other]));
    }

    #[test]
    fn poll_reads_only_what_was_appended() {
        let dir = temp_dir("poll");
        let path = dir.join("events.json");
        let mut log = EventLog::new(&path);
        assert!(log.poll().expect("missing file").is_empty());

        let mut file = std::fs::File::create(&path).expect("create");
        file.write_all(record("vmm", "started").as_bytes())
            .expect("write");
        file.flush().expect("flush");
        let events = log.poll().expect("first poll");
        assert_eq!(events.len(), 1);
        assert!(log.poll().expect("second poll").is_empty());

        file.write_all(record("vm", "booted").as_bytes())
            .expect("write");
        file.flush().expect("flush");
        let events = log.poll().expect("third poll");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "booted");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_split_object_is_completed_by_the_next_poll() {
        let dir = temp_dir("split");
        let path = dir.join("events.json");
        let mut log = EventLog::new(&path);
        let text = record("vm", "rebooting");
        let (first, second) = text.as_bytes().split_at(25);

        let mut file = std::fs::File::create(&path).expect("create");
        file.write_all(first).expect("write");
        file.flush().expect("flush");
        assert!(log.poll().expect("partial").is_empty());

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open");
        file.write_all(second).expect("write");
        file.flush().expect("flush");
        let events = log.poll().expect("complete");
        assert_eq!(events.len(), 1);
        assert!(saw_reset(&events));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_error_record_is_reported_once() {
        let dir = temp_dir("garbage");
        let path = dir.join("events.json");
        let mut log = EventLog::new(&path);
        std::fs::write(&path, b"not json at all").expect("write");
        assert!(log.poll().is_err());
        // The offset advanced past the garbage, so the next poll is quiet.
        log.poll().expect("quiet");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
