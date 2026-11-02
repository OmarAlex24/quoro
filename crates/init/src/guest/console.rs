// SPDX-License-Identifier: AGPL-3.0-only
//! The app's output: two pipes in, tagged lines out.
//!
//! The app's stdout and stderr are pipes read by one thread each. Every line
//! becomes `out <line>\n` or `err <line>\n` on the log console, written with a
//! single `write_all` under one mutex so the two streams never interleave in
//! the middle of a line. Bytes are passed through unchanged; the init never
//! writes anything of its own onto this console.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write as _};
use std::process::Child;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use quoro_init::LogConsole;

use crate::guest::diag;

/// Longest line the console protocol allows; longer lines are split.
const MAX_LINE: usize = 16384;
/// Where the app's tagged output goes, shared by the two pump threads. `None`
/// means the console could not be opened and the output is discarded.
pub(crate) type LogSink = Arc<Mutex<Option<File>>>;

/// The output threads, and how many of them there are, so the shutdown can wait
/// for their last lines.
pub(crate) struct Pumps {
    done: Receiver<()>,
    count: usize,
}

impl Pumps {
    /// Wait for every pump to reach EOF, for at most `budget`.
    pub(crate) fn wait(self, budget: Duration) {
        let deadline = Instant::now() + budget;
        let mut finished = 0;
        while finished < self.count {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            match self.done.recv_timeout(deadline - now) {
                Ok(()) => finished += 1,
                Err(_) => return,
            }
        }
    }
}

/// Open the console the config asks for. A console that cannot be opened is
/// warned about on the diagnostics console and the output is discarded.
pub(crate) fn open_log(console: LogConsole) -> LogSink {
    let path = match console {
        LogConsole::Virtio => "/dev/hvc0",
        LogConsole::Serial => "/dev/ttyS0",
        LogConsole::None => {
            return Arc::new(Mutex::new(None));
        }
    };
    match OpenOptions::new().write(true).open(path) {
        Ok(file) => {
            // A tty's output processing turns `\n` into `\r\n` (ONLCR); the
            // engine reads the records byte for byte, so switch it off.
            if let Err(err) = raw_output(&file) {
                diag::log(&format!(
                    "cannot switch {path} to raw output: {err}; lines may end in \\r\\n"
                ));
            }
            Arc::new(Mutex::new(Some(file)))
        }
        Err(err) => {
            diag::log(&format!(
                "cannot open app console {path}: {err}; discarding output"
            ));
            Arc::new(Mutex::new(None))
        }
    }
}

/// Clear `OPOST` on a tty so bytes reach the host unchanged.
fn raw_output(file: &File) -> rustix::io::Result<()> {
    let mut termios = rustix::termios::tcgetattr(file)?;
    termios
        .output_modes
        .remove(rustix::termios::OutputModes::OPOST);
    rustix::termios::tcsetattr(file, rustix::termios::OptionalActions::Now, &termios)
}

/// Start one thread per pipe the child has.
pub(crate) fn spawn_pumps(child: &mut Child, sink: &LogSink) -> Option<Pumps> {
    let (sender, done) = channel();
    let mut count = 0;

    if let Some(stdout) = child.stdout.take() {
        let sink = Arc::clone(sink);
        let sender = sender.clone();
        let spawned = thread::Builder::new()
            .name("stdout".to_string())
            .spawn(move || {
                pump(stdout, "out ", &sink);
                let _ = sender.send(());
            });
        if spawned.is_ok() {
            count += 1;
        } else {
            diag::log("cannot start the stdout pump; its output is discarded");
        }
    }

    if let Some(stderr) = child.stderr.take() {
        let sink = Arc::clone(sink);
        let sender = sender.clone();
        let spawned = thread::Builder::new()
            .name("stderr".to_string())
            .spawn(move || {
                pump(stderr, "err ", &sink);
                let _ = sender.send(());
            });
        if spawned.is_ok() {
            count += 1;
        } else {
            diag::log("cannot start the stderr pump; its output is discarded");
        }
    }

    // The threads hold the senders; without this the channel would stay open.
    drop(sender);

    if count == 0 {
        None
    } else {
        Some(Pumps { done, count })
    }
}

/// Read one pipe to EOF, emitting complete lines and the trailing partial one.
fn pump(mut source: impl Read, tag: &'static str, sink: &LogSink) {
    let mut line = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match source.read(&mut chunk) {
            Ok(0) => {
                if !line.is_empty() {
                    emit(sink, tag, &line);
                }
                return;
            }
            Ok(read) => {
                for byte in &chunk[..read] {
                    if *byte == b'\n' {
                        emit(sink, tag, &line);
                        line.clear();
                    } else {
                        line.push(*byte);
                        if line.len() == MAX_LINE {
                            emit(sink, tag, &line);
                            line.clear();
                        }
                    }
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                diag::log(&format!("read {tag}pipe: {err}"));
                if !line.is_empty() {
                    emit(sink, tag, &line);
                }
                return;
            }
        }
    }
}

/// One `tag + line + "\n"` record, as a single write under the shared lock.
fn emit(sink: &LogSink, tag: &str, line: &[u8]) {
    let mut record = Vec::with_capacity(tag.len() + line.len() + 1);
    record.extend_from_slice(tag.as_bytes());
    record.extend_from_slice(line);
    record.push(b'\n');

    let Ok(mut slot) = sink.lock() else {
        return;
    };
    if let Some(file) = slot.as_mut() {
        let _ = file.write_all(&record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run the pump over an in-memory source and collect what it emitted.
    fn pump_to_vec(input: &[u8]) -> Vec<u8> {
        let path = std::env::temp_dir().join(format!(
            "quoro-init-pump-{}-{}",
            std::process::id(),
            input.len()
        ));
        let _ = std::fs::remove_file(&path);
        let sink: LogSink = Arc::new(Mutex::new(Some(
            File::create(&path).unwrap_or_else(|err| panic!("{err}")),
        )));
        pump(input, "out ", &sink);
        drop(sink);
        let out = std::fs::read(&path).unwrap_or_default();
        let _ = std::fs::remove_file(&path);
        out
    }

    #[test]
    fn lines_are_tagged_and_the_trailing_partial_line_is_flushed() {
        assert_eq!(
            pump_to_vec(b"one\ntwo\nthree"),
            b"out one\nout two\nout three\n"
        );
    }

    #[test]
    fn empty_lines_survive() {
        assert_eq!(pump_to_vec(b"a\n\nb\n"), b"out a\nout \nout b\n");
    }

    #[test]
    fn long_lines_become_chunks_each_tagged() {
        let mut input = vec![b'x'; MAX_LINE * 2 + 5];
        input.push(b'\n');
        let out = pump_to_vec(&input);
        let text = String::from_utf8_lossy(&out).into_owned();
        let records: Vec<&str> = text.split_inclusive('\n').collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].len(), "out ".len() + MAX_LINE + 1);
        assert_eq!(records[1].len(), "out ".len() + MAX_LINE + 1);
        assert_eq!(records[2], "out xxxxx\n");
    }

    #[test]
    fn bytes_are_passed_through_unchanged() {
        assert_eq!(
            pump_to_vec(&[0xff, 0xfe, b'\n']),
            [&b"out "[..], &[0xff, 0xfe], b"\n"].concat()
        );
    }

    #[test]
    fn nothing_is_emitted_for_empty_input() {
        assert_eq!(pump_to_vec(b""), b"");
    }
}
