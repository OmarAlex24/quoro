// SPDX-License-Identifier: AGPL-3.0-only
//! The console connections of a VM: tagged output lines and the interactive
//! console (contract §3.13, §3.14).
//!
//! Cloud Hypervisor listens on the UNIX socket a spec names and a client
//! connects to it; one client at a time. Both connections hold a flag of the
//! VM's entry so a second call is a `Conflict` rather than a surprise on the
//! guest side, and both clear it when the caller drops the stream.

use std::io;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::SystemTime;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, BufReader, ReadBuf};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use crate::{EngineError, LineStream, OutputLine, OutputStream};

/// How many untagged lines a slow reader may fall behind before its lines are
/// dropped (§3.12's rule, applied to output).
const OUTPUT_CAPACITY: usize = 256;
/// Longest line read from the console; the init emits records of at most
/// 16 KiB plus the tag.
const MAX_LINE_BYTES: u64 = 64 * 1024;

/// One VM's tagged output stream: the receiving end of the reader task.
///
/// Dropping it closes the channel and clears the VM's `output_open` flag, so the
/// output can be opened again.
pub(super) struct OutputLines {
    rx: mpsc::Receiver<OutputLine>,
    open: Arc<AtomicBool>,
}

impl futures_core::Stream for OutputLines {
    type Item = OutputLine;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<OutputLine>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for OutputLines {
    fn drop(&mut self) {
        self.open.store(false, Ordering::SeqCst);
        // Explicitly close so the reader task sees the end and stops.
        self.rx.close();
    }
}

/// One VM's interactive console connection.
///
/// Dropping it clears the VM's `console_open` flag, so a later caller can take
/// the console over.
pub(super) struct ConsoleConnection {
    stream: UnixStream,
    open: Arc<AtomicBool>,
}

impl AsyncRead for ConsoleConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for ConsoleConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Drop for ConsoleConnection {
    fn drop(&mut self) {
        self.open.store(false, Ordering::SeqCst);
    }
}

/// Opens the tagged output of one VM (§3.13).
///
/// # Errors
///
/// `Conflict` when the output is already open or the VM is not listening on
/// `socket`, and `Io` when the connection cannot be prepared.
pub(super) fn open_output(
    vm: &str,
    socket: &Path,
    open: Arc<AtomicBool>,
) -> Result<LineStream, EngineError> {
    let handle = claim(&open, vm, "output")?;
    let stream = match connect(socket, vm) {
        Ok(stream) => stream,
        Err(err) => {
            open.store(false, Ordering::SeqCst);
            return Err(err);
        }
    };
    let (tx, rx) = mpsc::channel(OUTPUT_CAPACITY);
    handle.spawn(async move { read_lines(stream, tx).await });
    Ok(Box::pin(OutputLines { rx, open }))
}

/// Opens the interactive console of one VM (§3.14).
///
/// # Errors
///
/// `Conflict` when the console is already open or the VM is not listening on
/// `socket`, and `Io` when the connection cannot be prepared.
pub(super) fn open_console(
    vm: &str,
    socket: &Path,
    open: Arc<AtomicBool>,
) -> Result<crate::ConsoleStream, EngineError> {
    let _handle = claim(&open, vm, "console")?;
    let stream = match connect(socket, vm) {
        Ok(stream) => stream,
        Err(err) => {
            open.store(false, Ordering::SeqCst);
            return Err(err);
        }
    };
    Ok(Box::pin(ConsoleConnection { stream, open }))
}

/// Marks the connection of one kind as open, or reports that it already is.
fn claim(
    open: &Arc<AtomicBool>,
    vm: &str,
    what: &str,
) -> Result<tokio::runtime::Handle, EngineError> {
    let handle = tokio::runtime::Handle::try_current().map_err(|err| {
        EngineError::Io(format!(
            "the {what} of VM {vm} needs a tokio runtime: {err}"
        ))
    })?;
    if open.swap(true, Ordering::SeqCst) {
        return Err(EngineError::Conflict(format!(
            "the {what} of VM {vm} is already open"
        )));
    }
    Ok(handle)
}

/// Connects to the console socket Cloud Hypervisor listens on.
///
/// A socket that refuses the connection belongs to a VM that is not running:
/// Cloud Hypervisor creates it when it boots and removes it when it stops.
fn connect(socket: &Path, vm: &str) -> Result<UnixStream, EngineError> {
    let std_stream = StdUnixStream::connect(socket).map_err(|err| {
        EngineError::Conflict(format!(
            "VM {vm} is not running: cannot connect to {}: {err}",
            socket.display()
        ))
    })?;
    std_stream
        .set_nonblocking(true)
        .map_err(|err| EngineError::Io(format!("{}: {err}", socket.display())))?;
    UnixStream::from_std(std_stream)
        .map_err(|err| EngineError::Io(format!("{}: {err}", socket.display())))
}

/// Reads tagged lines until the guest side closes, dropping what does not fit.
async fn read_lines(stream: UnixStream, tx: mpsc::Sender<OutputLine>) {
    let mut reader = BufReader::new(stream);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        // The guest is not trusted: cap a line so a writer that never sends a
        // newline cannot grow host memory. The init's records are at most
        // 16 KiB; a longer read is cut and its continuation, untagged, skipped.
        let mut limited = (&mut reader).take(MAX_LINE_BYTES);
        // Stop as soon as the caller drops the stream, even if the guest is
        // silent, so the socket's single client slot is freed.
        let read = tokio::select! {
            () = tx.closed() => return,
            read = limited.read_until(b'\n', &mut buffer) => read,
        };
        match read {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        if buffer.last() == Some(&b'\n') {
            buffer.pop();
        }
        let Some((stream, line)) = parse_line(&buffer) else {
            continue;
        };
        let line = OutputLine {
            stream,
            time: SystemTime::now(),
            line,
        };
        match tx.try_send(line) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}

/// Splits one raw console line into its tag and text.
///
/// The guest init writes `out ` or `err ` in front of every line; anything else
/// (the init's own diagnostics, an unterminated fragment) is skipped. One
/// trailing carriage return is removed defensively, and invalid UTF-8 is made
/// lossy rather than losing the line.
fn parse_line(raw: &[u8]) -> Option<(OutputStream, String)> {
    let text = String::from_utf8_lossy(raw);
    let text = text.strip_suffix('\r').unwrap_or(&text);
    if let Some(rest) = text.strip_prefix("out ") {
        return Some((OutputStream::Stdout, rest.to_string()));
    }
    if let Some(rest) = text.strip_prefix("err ") {
        return Some((OutputStream::Stderr, rest.to_string()));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::parse_line;
    use crate::OutputStream;

    #[test]
    fn stdout_lines_keep_their_text() {
        assert_eq!(
            parse_line(b"out ready"),
            Some((OutputStream::Stdout, "ready".to_string()))
        );
    }

    #[test]
    fn stderr_lines_keep_their_text() {
        assert_eq!(
            parse_line(b"err boom"),
            Some((OutputStream::Stderr, "boom".to_string()))
        );
    }

    #[test]
    fn untagged_lines_are_skipped() {
        assert_eq!(parse_line(b"random noise"), None);
        assert_eq!(parse_line(b""), None);
        assert_eq!(
            parse_line(b"out"),
            None,
            "the tag is only a tag with its space"
        );
        assert_eq!(
            parse_line(b"output ignored"),
            None,
            "a longer word is not the tag"
        );
    }

    #[test]
    fn one_trailing_carriage_return_is_stripped() {
        assert_eq!(
            parse_line(b"out ready\r"),
            Some((OutputStream::Stdout, "ready".to_string()))
        );
        assert_eq!(
            parse_line(b"out a\r\r"),
            Some((OutputStream::Stdout, "a\r".to_string())),
            "only one carriage return belongs to the line ending"
        );
    }

    #[test]
    fn empty_text_is_a_line_of_its_own() {
        assert_eq!(
            parse_line(b"out "),
            Some((OutputStream::Stdout, String::new()))
        );
    }

    #[test]
    fn invalid_utf8_is_lossy_not_lost() {
        let parsed = parse_line(b"out caf\xff").expect("a tagged line survives");
        assert_eq!(parsed.0, OutputStream::Stdout);
        assert_eq!(parsed.1, "caf\u{fffd}");
    }
}
