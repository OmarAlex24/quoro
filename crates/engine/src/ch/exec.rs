// SPDX-License-Identifier: AGPL-3.0-only
//! Exec sessions over Cloud Hypervisor's hybrid vsock socket (contract §3.18,
//! ADR 0113).
//!
//! One connection to the VM's vsock socket is one PTY session. The handshake
//! is Cloud Hypervisor's: `CONNECT <port>\n` on the host side, answered
//! `OK <port>\n`; after that the connection carries the frames of
//! [`quoro_init::exec`], the same codec the guest init uses.
//!
//! The relay bridges the vsock connection and an in-memory duplex pair handed
//! out as [`ExecSession::io`]. The duplex is fixed-size, so when the client
//! stops reading, the relay stops reading vsock, virtio-vsock's credit flow
//! stops the guest program, and no unbounded buffer appears on either side.
//! The node never creates a `<socket>_<port>` listener, so the guest cannot
//! open a connection to the host over vsock.

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream, ReadHalf, WriteHalf, duplex};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};

use quoro_init::exec::{FrameReader, GuestFrame, HostFrame, OpenRequest};

use crate::{EngineError, ExecRequest, ExecSession};

/// How long connecting and the hybrid handshake may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
/// Longest reply line the handshake accepts; `OK <port>\n` is nine bytes.
const REPLY_BYTES: usize = 64;
/// Bytes the client's `io` buffers before its writer waits, and the relay
/// stops reading the socket. This is the session's whole back-pressure.
const DUPLEX_BYTES: usize = 64 * 1024;
/// Resize requests a caller may queue before `resize.send` waits.
const RESIZE_CAPACITY: usize = 64;
/// The longest read from the socket.
const READ_BYTES: usize = 64 * 1024;

/// Opens one exec session on `socket`, the VM's hybrid vsock socket.
///
/// `request` is validated here as well as by `exec`, so a direct caller cannot
/// send a request the init would refuse.
///
/// # Errors
///
/// `Conflict` when the request breaks the format, when the socket refuses the
/// connection, or when the handshake is not an `OK` for the exec port — the
/// last is what a VM whose init predates M4 answers.
pub(super) async fn open(
    vm: &str,
    socket: &Path,
    request: &ExecRequest,
) -> Result<ExecSession, EngineError> {
    let open = OpenRequest {
        argv: request.argv.clone(),
        user: request.user.clone(),
        rows: request.rows,
        cols: request.cols,
    };
    open.validate()
        .map_err(|err| EngineError::Conflict(err.to_string()))?;

    let mut stream = connect(vm, socket).await?;
    let reply = match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&mut stream)).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) | Err(_) => return Err(refused(vm)),
    };
    if !accepts_exec_port(&reply) {
        return Err(refused(vm));
    }
    let frame = HostFrame::Open(open)
        .encode()
        .map_err(|err| EngineError::Conflict(err.to_string()))?;
    stream.write_all(&frame).await.map_err(|_| refused(vm))?;

    let (client, engine_side) = duplex(DUPLEX_BYTES);
    let (resize, resize_rx) = mpsc::channel(RESIZE_CAPACITY);
    let (exit, exit_rx) = oneshot::channel();
    tokio::spawn(relay(stream, engine_side, resize_rx, exit));
    Ok(ExecSession {
        io: Box::pin(client),
        resize,
        exit: exit_rx,
    })
}

/// The conflict every "there is no exec listener on the other side" path
/// reports (a VM started before its node got the M4 init).
fn refused(vm: &str) -> EngineError {
    EngineError::Conflict(format!(
        "the VM's init does not accept exec sessions; restart the VM ({vm})"
    ))
}

/// Connects to the VM's hybrid vsock socket.
async fn connect(vm: &str, socket: &Path) -> Result<UnixStream, EngineError> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, UnixStream::connect(socket)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(_)) | Err(_) => Err(refused(vm)),
    }
}

/// Sends `CONNECT <port>\n` and reads the reply line, bounded to
/// [`REPLY_BYTES`].
async fn handshake(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    stream
        .write_all(format!("CONNECT {}\n", quoro_init::EXEC_PORT).as_bytes())
        .await?;
    stream.flush().await?;
    let mut reply = Vec::new();
    let mut byte = [0_u8; 1];
    while reply.len() < REPLY_BYTES {
        if stream.read(&mut byte).await? == 0 {
            break;
        }
        reply.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    Ok(reply)
}

/// True when `reply` is `OK <port>\n` for the exec port.
fn accepts_exec_port(reply: &[u8]) -> bool {
    let Some(line) = reply.strip_suffix(b"\n") else {
        return false;
    };
    let Some(port) = line.strip_prefix(b"OK ") else {
        return false;
    };
    std::str::from_utf8(port)
        .ok()
        .and_then(|text| text.trim().parse::<u32>().ok())
        == Some(quoro_init::EXEC_PORT)
}

/// How one session ended.
enum SessionEnd {
    /// The client dropped or shut its `io`; the guest got a `CLOSE`.
    ClientClosed,
    /// The guest reported the program's exit code.
    GuestExit(i32),
    /// The guest reported an error and ended the session.
    GuestError,
    /// The connection ended without an exit code.
    GuestGone,
    /// A frame the codec refuses: oversized, unknown, or malformed.
    Malformed,
}

/// Bridges the vsock connection and the client's duplex pair until either side
/// ends the session.
///
/// The guest's `EXIT` resolves `exit`; every other way of ending drops the
/// sender, so the caller's `exit.await` reports an error. Output already
/// written to the duplex stays readable after the write half is dropped.
async fn relay(
    stream: UnixStream,
    engine_side: DuplexStream,
    mut resize: mpsc::Receiver<(u16, u16)>,
    exit: oneshot::Sender<i32>,
) {
    let (mut from_guest_stream, mut to_guest) = tokio::io::split(stream);
    let (mut from_client, mut to_client) = tokio::io::split(engine_side);
    let mut input = InputPump::new();
    let mut output = OutputPump::new();

    let end = tokio::select! {
        // The guest side first: an `EXIT` that arrives with the client's
        // last input must win.
        biased;
        end = output.pump(&mut from_guest_stream, &mut to_client) => end,
        end = input.pump(&mut from_client, &mut to_guest, &mut resize) => end,
    };

    match end {
        SessionEnd::GuestExit(code) => {
            let _ = exit.send(code);
        }
        // An error is a message for the operator, but the engine has no log
        // channel of its own; the close record the node agent writes carries
        // the outcome. The dropped sender is the error it sees.
        SessionEnd::GuestError
        | SessionEnd::ClientClosed
        | SessionEnd::GuestGone
        | SessionEnd::Malformed => {}
    }
}

/// The client-to-guest direction: `io` bytes become `INPUT` frames and resize
/// requests become `RESIZE` frames.
struct InputPump {
    /// Bytes read from `io` that are not yet on the wire, and how many of them
    /// are. The buffer is owned by the pump so a cancelled `select!` branch
    /// never loses a byte.
    pending: Vec<u8>,
    written: usize,
    /// The buffer one read fills.
    read_buf: Vec<u8>,
    /// The resize channel returned `None`; stop selecting on it.
    resize_open: bool,
}

impl InputPump {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            written: 0,
            read_buf: vec![0_u8; READ_BYTES],
            resize_open: true,
        }
    }

    async fn pump(
        &mut self,
        from_client: &mut ReadHalf<DuplexStream>,
        to_guest: &mut WriteHalf<UnixStream>,
        resize: &mut mpsc::Receiver<(u16, u16)>,
    ) -> SessionEnd {
        loop {
            // Flush the input still on this side of the wire.
            while self.written < self.pending.len() {
                match to_guest.write(&self.pending[self.written..]).await {
                    Ok(0) | Err(_) => return SessionEnd::GuestGone,
                    Ok(n) => self.written += n,
                }
            }
            self.pending.clear();
            self.written = 0;

            let read = tokio::select! {
                size = resize.recv(), if self.resize_open => {
                    let Some((rows, cols)) = size else {
                        self.resize_open = false;
                        continue;
                    };
                    // A bad size is the caller's mistake, not the guest's:
                    // skip it rather than ending an interactive session.
                    if let Ok(frame) = (HostFrame::Resize { rows, cols }).encode()
                        && to_guest.write_all(&frame).await.is_err()
                    {
                        return SessionEnd::GuestGone;
                    }
                    continue;
                }
                read = from_client.read(&mut self.read_buf) => read,
            };
            match read {
                Ok(0) => {
                    // The client is gone: ask the guest for `SIGHUP` and end.
                    if let Ok(frame) = HostFrame::Close.encode() {
                        let _ = to_guest.write_all(&frame).await;
                    }
                    let _ = to_guest.shutdown().await;
                    return SessionEnd::ClientClosed;
                }
                Ok(n) => match HostFrame::Input(self.read_buf[..n].to_vec()).encode() {
                    Ok(frame) => self.pending.extend_from_slice(&frame),
                    // A read is at most `READ_BYTES` == `MAX_PAYLOAD`.
                    Err(_) => return SessionEnd::Malformed,
                },
                Err(_) => return SessionEnd::ClientClosed,
            }
        }
    }
}

/// The guest-to-client direction: `OUTPUT` frames become bytes readable from
/// `io`; `EXIT` and `ERROR` end the session.
struct OutputPump {
    reader: FrameReader,
    read_buf: Vec<u8>,
}

impl OutputPump {
    fn new() -> Self {
        Self {
            reader: FrameReader::new(),
            read_buf: vec![0_u8; READ_BYTES],
        }
    }

    async fn pump(
        &mut self,
        from_guest: &mut ReadHalf<UnixStream>,
        to_client: &mut WriteHalf<DuplexStream>,
    ) -> SessionEnd {
        loop {
            match self.reader.next_frame() {
                Ok(Some((kind, payload))) => {
                    match GuestFrame::decode(kind, &payload) {
                        Ok(GuestFrame::Output(bytes)) => {
                            if !bytes.is_empty() && to_client.write_all(&bytes).await.is_err() {
                                return SessionEnd::ClientClosed;
                            }
                        }
                        Ok(GuestFrame::Exit(code)) => return SessionEnd::GuestExit(code),
                        Ok(GuestFrame::Error(_)) => return SessionEnd::GuestError,
                        Err(_) => return SessionEnd::Malformed,
                    }
                    // A read can carry several frames: parse them all before
                    // reading again, or a frame already buffered waits for a
                    // read that may only return end of file.
                    continue;
                }
                Ok(None) => {}
                // The header announced a payload over the limit: end now.
                Err(_) => return SessionEnd::Malformed,
            }
            match from_guest.read(&mut self.read_buf).await {
                Ok(0) | Err(_) => return SessionEnd::GuestGone,
                Ok(n) => self.reader.push(&self.read_buf[..n]),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use quoro_init::exec::{FrameReader, GuestFrame, HostFrame, MAX_PAYLOAD};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{UnixListener, UnixStream};

    use super::{accepts_exec_port, open};
    use crate::{EngineError, ExecRequest};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(tag: &str) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "quoro-exec-{tag}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create the test directory");
        dir
    }

    fn request() -> ExecRequest {
        ExecRequest {
            argv: vec!["sh".to_string(), "-lc".to_string(), "uname -a".to_string()],
            user: None,
            rows: 24,
            cols: 80,
        }
    }

    /// Reads one newline-terminated line from the fake VMM connection.
    async fn read_line(stream: &mut UnixStream) -> Vec<u8> {
        let mut line = Vec::new();
        let mut byte = [0_u8; 1];
        while stream.read(&mut byte).await.expect("read") == 1 {
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        line
    }

    /// Reads one host frame from the fake guest connection.
    async fn read_host_frame(stream: &mut UnixStream, reader: &mut FrameReader) -> HostFrame {
        loop {
            if let Some((kind, payload)) = reader.next_frame().expect("frame") {
                return HostFrame::decode(kind, &payload).expect("host frame");
            }
            let mut buf = [0_u8; 4096];
            let n = stream.read(&mut buf).await.expect("read");
            assert_ne!(n, 0, "the engine closed the connection");
            reader.push(&buf[..n]);
        }
    }

    /// A fake hybrid-vsock endpoint: answers the handshake, then runs `guest`
    /// on the connection.
    async fn fake_vmm<F, Fut>(socket: PathBuf, guest: F)
    where
        F: FnOnce(UnixStream, FrameReader) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = UnixListener::bind(&socket).expect("bind");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let line = read_line(&mut stream).await;
            assert_eq!(line, b"CONNECT 1024\n");
            stream.write_all(b"OK 1024\n").await.expect("reply");
            guest(stream, FrameReader::new()).await;
        });
    }

    async fn next_chunk(session: &mut crate::ExecSession) -> Option<Vec<u8>> {
        let mut buf = [0_u8; 64];
        let n = session.io.read(&mut buf).await.ok()?;
        if n == 0 {
            None
        } else {
            Some(buf[..n].to_vec())
        }
    }

    /// Waits for the session's exit code, failing instead of hanging.
    async fn exit_code(session: crate::ExecSession) -> Result<i32, crate::EngineError> {
        tokio::time::timeout(std::time::Duration::from_secs(5), session.exit)
            .await
            .expect("the session ends")
            .map_err(|_| crate::EngineError::Timeout)
    }

    #[tokio::test]
    async fn a_session_relays_input_output_and_the_exit_code() {
        let dir = temp_dir("relay");
        let socket = dir.join("vsock.sock");
        fake_vmm(socket.clone(), |mut stream, mut reader| async move {
            let open = read_host_frame(&mut stream, &mut reader).await;
            assert!(matches!(open, HostFrame::Open(ref request) if request.argv[0] == "sh"));
            stream
                .write_all(&GuestFrame::Output(b"$ ".to_vec()).encode().expect("output"))
                .await
                .expect("write");
            let input = read_host_frame(&mut stream, &mut reader).await;
            assert_eq!(input, HostFrame::Input(b"uname -a\n".to_vec()));
            stream
                .write_all(
                    &GuestFrame::Output(b"Linux guest\n".to_vec())
                        .encode()
                        .expect("output"),
                )
                .await
                .expect("write");
            stream
                .write_all(&GuestFrame::Exit(7).encode().expect("exit"))
                .await
                .expect("write");
        })
        .await;

        let mut session = open("vm_test", &socket, &request()).await.expect("open");
        assert_eq!(next_chunk(&mut session).await.as_deref(), Some(&b"$ "[..]));
        session
            .io
            .write_all(b"uname -a\n")
            .await
            .expect("write the input");
        session.io.flush().await.expect("flush");
        assert_eq!(
            next_chunk(&mut session).await.as_deref(),
            Some(&b"Linux guest\n"[..])
        );
        // The write half is dropped after `EXIT`, so the reader sees EOF.
        assert_eq!(next_chunk(&mut session).await, None);
        assert_eq!(exit_code(session).await.expect("exit code"), 7);
    }

    #[tokio::test]
    async fn a_resize_request_reaches_the_guest() {
        let dir = temp_dir("resize");
        let socket = dir.join("vsock.sock");
        let (seen, seen_rx) = tokio::sync::oneshot::channel();
        fake_vmm(socket.clone(), |mut stream, mut reader| async move {
            assert!(matches!(
                read_host_frame(&mut stream, &mut reader).await,
                HostFrame::Open(_)
            ));
            let resize = read_host_frame(&mut stream, &mut reader).await;
            stream
                .write_all(&GuestFrame::Exit(0).encode().expect("exit"))
                .await
                .expect("write");
            let _ = seen.send(resize);
        })
        .await;

        let session = open("vm_test", &socket, &request()).await.expect("open");
        session
            .resize
            .send((50, 132))
            .await
            .expect("queue the resize");
        assert_eq!(
            seen_rx.await.expect("the guest saw the frame"),
            HostFrame::Resize {
                rows: 50,
                cols: 132
            }
        );
        assert_eq!(session.exit.await.expect("exit code"), 0);
    }

    #[tokio::test]
    async fn a_guest_error_ends_the_session_without_an_exit_code() {
        let dir = temp_dir("error");
        let socket = dir.join("vsock.sock");
        fake_vmm(socket.clone(), |mut stream, mut reader| async move {
            assert!(matches!(
                read_host_frame(&mut stream, &mut reader).await,
                HostFrame::Open(_)
            ));
            stream
                .write_all(
                    &GuestFrame::Error("no such user".to_string())
                        .encode()
                        .expect("error"),
                )
                .await
                .expect("write");
        })
        .await;

        let mut session = open("vm_test", &socket, &request()).await.expect("open");
        assert_eq!(next_chunk(&mut session).await, None, "io ends");
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), session.exit)
                .await
                .expect("the session ends")
                .is_err(),
            "no exit code after an error frame"
        );
    }

    #[tokio::test]
    async fn an_oversized_frame_ends_the_session() {
        let dir = temp_dir("oversized");
        let socket = dir.join("vsock.sock");
        fake_vmm(socket.clone(), |mut stream, mut reader| async move {
            assert!(matches!(
                read_host_frame(&mut stream, &mut reader).await,
                HostFrame::Open(_)
            ));
            // A header that announces more than the codec allows.
            let mut header = vec![129_u8];
            let len = u32::try_from(MAX_PAYLOAD + 1).expect("fits");
            header.extend_from_slice(&len.to_be_bytes());
            stream.write_all(&header).await.expect("write");
        })
        .await;

        let mut session = open("vm_test", &socket, &request()).await.expect("open");
        assert_eq!(next_chunk(&mut session).await, None);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), session.exit)
                .await
                .expect("the session ends")
                .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_io_sends_close_and_ends_the_session() {
        let dir = temp_dir("close");
        let socket = dir.join("vsock.sock");
        let (seen, seen_rx) = tokio::sync::oneshot::channel();
        fake_vmm(socket.clone(), |mut stream, mut reader| async move {
            assert!(matches!(
                read_host_frame(&mut stream, &mut reader).await,
                HostFrame::Open(_)
            ));
            let frame = read_host_frame(&mut stream, &mut reader).await;
            let _ = seen.send(frame);
        })
        .await;

        let mut session = open("vm_test", &socket, &request()).await.expect("open");
        session.io.shutdown().await.expect("close");
        drop(session.io);
        assert_eq!(
            seen_rx.await.expect("the guest saw the close"),
            HostFrame::Close
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), session.exit)
                .await
                .expect("the session ends")
                .is_err(),
            "no exit code after a close"
        );
    }

    /// The conflict message a refused `open` returns; any other outcome panics
    /// with what happened instead.
    fn conflict_of(result: Result<crate::ExecSession, EngineError>) -> String {
        match result {
            Err(EngineError::Conflict(reason)) => reason,
            Err(err) => panic!("expected Conflict, got {err:?}"),
            Ok(_) => panic!("expected Conflict, got a session"),
        }
    }

    #[tokio::test]
    async fn a_refused_handshake_is_the_old_init_conflict() {
        let dir = temp_dir("refused");
        let socket = dir.join("vsock.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let _ = read_line(&mut stream).await;
            stream.write_all(b"ERR\n").await.expect("reply");
        });

        let reason = conflict_of(open("vm_test", &socket, &request()).await);
        assert!(reason.contains("does not accept exec sessions"), "{reason}");
    }

    #[tokio::test]
    async fn a_missing_listener_is_the_old_init_conflict() {
        let dir = temp_dir("missing");
        let reason = conflict_of(open("vm_test", &dir.join("absent.sock"), &request()).await);
        assert!(reason.contains("does not accept exec sessions"), "{reason}");
    }

    #[tokio::test]
    async fn an_invalid_request_is_refused_before_connecting() {
        let dir = temp_dir("invalid");
        let mut request = request();
        request.argv.clear();
        let reason = conflict_of(open("vm_test", &dir.join("absent.sock"), &request).await);
        assert_eq!(reason, "argv is empty");
    }

    #[test]
    fn the_reply_must_name_the_exec_port() {
        assert!(accepts_exec_port(b"OK 1024\n"));
        assert!(!accepts_exec_port(b"OK 1024"));
        assert!(!accepts_exec_port(b"OK 2048\n"));
        assert!(!accepts_exec_port(b"ERR 1024\n"));
        assert!(!accepts_exec_port(b"OK 1024\nOK 1024\n"));
        assert!(!accepts_exec_port(b""));
    }
}
