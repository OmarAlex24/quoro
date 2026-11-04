// SPDX-License-Identifier: AGPL-3.0-only
//! Web terminal and read-only console streams (`node-agent-protocol.md`
//! sections 5.15, 5.16, 5.24, 5.25; ADR 0113, ADR 0116).
//!
//! A `terminal` stream is one exec session of its own: the engine's
//! `ExecSession::io` is relayed to the primary as base64 `stream_data`, input
//! arrives through `stream_input`, and the session is recorded in an asciicast
//! transcript. A `console` stream follows the engine's serial-output ring and
//! takes no input. A `transcript` stream replays a finished transcript file.
//!
//! Credit flow control (section 5.25) lives between the source and the frames:
//! at most [`STREAM_WINDOW_BYTES`] of decoded data may be sent and not yet
//! acknowledged. At the window a terminal stops reading its PTY, which
//! back-pressures the guest, and a console stops polling its serial stream,
//! which lets the engine's ring drop chunks for a slow viewer rather than slow
//! the guest.
//!
//! [`Session`](crate::session::Session) holds no engine and [`quoro_engine::Engine`]
//! is not object safe, so [`Interactive`] carries a private, type-erased handle
//! built once in [`crate::run`].

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt as _;
use quoro_engine::{ByteStream, Engine, EngineError, ExecRequest, ExecSession, VmId};
use quoro_proto::wire::{NodeMessage, StreamData, StreamEnd, StreamEndReason, StreamInput};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, ReadHalf, WriteHalf};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::store::NodeStore;
use crate::transcript::{self, TranscriptError, TranscriptLimits, TranscriptWriter};
use crate::util;

/// Decoded bytes one stream may have in flight before it is acknowledged
/// (section 5.25).
pub const STREAM_WINDOW_BYTES: usize = 256 * 1024;
/// Largest `data` payload per `stream_data` (section 5.7).
pub const MAX_DATA_BYTES: usize = 64 * 1024;
/// Largest decoded `stream_input` payload (section 5.24).
pub const MAX_INPUT_BYTES: usize = 64 * 1024;
/// Largest `tail_bytes` a console open may ask to replay.
pub const MAX_TAIL_BYTES: u64 = 1024 * 1024;
/// How long output is coalesced so one echoed keystroke is not one frame.
const COALESCE: Duration = Duration::from_millis(5);
/// Capacity of the per-stream input queue. The control plane sends at most one
/// client message per `stream_input`, so this only ever holds a short burst.
const INPUT_CAPACITY: usize = 64;
/// The largest window or row/column value the contract allows.
const MAX_SIZE: u16 = 1000;
/// Input recorded but not yet written to the guest. A program that stops
/// reading its input for this long ends the session instead of growing the
/// queue without bound.
const MAX_QUEUED_INPUT: usize = 1024 * 1024;
/// After the program exited, how long one read may wait for the rest of its
/// output before the stream ends anyway.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The two engine calls interactive streams need, erased so the session does
/// not have to know the backend.
trait InteractiveEngine: Send + Sync {
    fn exec<'a>(
        &'a self,
        id: &'a VmId,
        request: &'a ExecRequest,
    ) -> BoxFuture<'a, Result<ExecSession, EngineError>>;

    fn serial_output(&self, id: &VmId, tail: Option<u64>) -> Result<ByteStream, EngineError>;
}

impl<E: Engine + 'static> InteractiveEngine for E {
    fn exec<'a>(
        &'a self,
        id: &'a VmId,
        request: &'a ExecRequest,
    ) -> BoxFuture<'a, Result<ExecSession, EngineError>> {
        Box::pin(Engine::exec(self, id, request))
    }

    fn serial_output(&self, id: &VmId, tail: Option<u64>) -> Result<ByteStream, EngineError> {
        Engine::serial_output(self, id, tail)
    }
}

/// The node's interactive handles: the engine behind the streams and the store
/// that holds transcripts.
#[derive(Clone)]
pub struct Interactive {
    engine: Arc<dyn InteractiveEngine>,
    store: NodeStore,
    limits: TranscriptLimits,
}

impl Interactive {
    /// Wraps an engine and the node store.
    pub fn new<E: Engine + 'static>(engine: Arc<E>, store: NodeStore) -> Self {
        Self {
            engine,
            store,
            limits: TranscriptLimits::default(),
        }
    }

    /// Replaces the transcript policy, so tests reach the cap without writing
    /// 16 MiB.
    #[must_use]
    pub fn with_transcript_limits(mut self, limits: TranscriptLimits) -> Self {
        self.limits = limits;
        self
    }

    /// The transcript policy in use.
    #[must_use]
    pub fn limits(&self) -> TranscriptLimits {
        self.limits
    }
}

/// What kind of stream a [`Door`] belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Terminal,
    Console,
    Transcript,
}

/// What a stream open produced: the task that runs it and the control surface
/// the session routes input and acks to.
pub struct Opened {
    pub handle: JoinHandle<()>,
    pub door: Door,
}

/// The session's side of one running stream.
pub struct Door {
    pub kind: StreamKind,
    pub credit: Arc<Credit>,
    pub input: Option<mpsc::Sender<Input>>,
    /// Dropping the sender (or sending `true`) stops the task, which then
    /// finalises a transcript and answers `cancelled`.
    cancel: watch::Sender<bool>,
    /// Set before a cancel when the input queue overflowed, so the task
    /// answers `error` rather than `cancelled`.
    overflow: Arc<AtomicBool>,
}

impl Door {
    /// Asks the task to stop and answers `stream_end` `cancelled`.
    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    /// Hands one input to the stream without waiting.
    ///
    /// The session loop must never block on one stream, so a full queue ends
    /// the stream with `error` instead of dropping a keystroke (section 5.24
    /// keeps input gap-free).
    pub fn push_input(&self, input: Input) {
        let Some(sender) = &self.input else {
            return;
        };
        match sender.try_send(input) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("terminal input queue is full; ending the stream");
                self.overflow.store(true, Ordering::SeqCst);
                self.cancel();
            }
        }
    }
}

fn new_door(
    kind: StreamKind,
    credit: &Arc<Credit>,
    input: Option<mpsc::Sender<Input>>,
) -> (Door, watch::Receiver<bool>, Arc<AtomicBool>) {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let overflow = Arc::new(AtomicBool::new(false));
    let door = Door {
        kind,
        credit: Arc::clone(credit),
        input,
        cancel: cancel_tx,
        overflow: Arc::clone(&overflow),
    };
    (door, cancel_rx, overflow)
}

/// One `stream_input`, already validated: the three parts are applied in this
/// order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Input {
    pub data: Option<Vec<u8>>,
    pub resize: Option<(u16, u16)>,
    pub eof: bool,
}

/// Why a stream could not open; each maps to a `command_result` error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenError {
    NotFound(String),
    NotAvailable(String),
    BadMessage(String),
}

impl OpenError {
    /// The `error.code` the answer carries.
    #[must_use]
    pub fn code(&self) -> quoro_proto::ErrorCode {
        match self {
            Self::NotFound(_) => quoro_proto::ErrorCode::NotFound,
            Self::NotAvailable(_) => quoro_proto::ErrorCode::NotAvailable,
            Self::BadMessage(_) => quoro_proto::ErrorCode::BadMessage,
        }
    }

    /// The human-readable detail.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::NotFound(message) | Self::NotAvailable(message) | Self::BadMessage(message) => {
                message
            }
        }
    }
}

/// The credit window of one stream (section 5.25).
pub struct Credit {
    state: Mutex<CreditState>,
    notify: Notify,
}

#[derive(Default)]
struct CreditState {
    sent: u64,
    acked: u64,
}

/// What one `stream_ack` did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    /// At or below the previous ack; ignored.
    Stale,
    /// Credit was extended.
    Accepted,
    /// Beyond the bytes already sent: a primary bug.
    Beyond,
}

impl Credit {
    fn new() -> Self {
        Self {
            state: Mutex::new(CreditState::default()),
            notify: Notify::new(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, CreditState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Decoded bytes that may be sent without passing the window.
    #[must_use]
    pub fn available(&self) -> usize {
        let state = self.state();
        let outstanding = state.sent.saturating_sub(state.acked);
        usize::try_from(STREAM_WINDOW_BYTES as u64 - outstanding.min(STREAM_WINDOW_BYTES as u64))
            .unwrap_or(0)
    }

    /// Records bytes just queued for delivery.
    pub fn add_sent(&self, bytes: usize) {
        let mut state = self.state();
        state.sent = state.sent.saturating_add(bytes as u64);
    }

    /// Applies one `stream_ack`.
    pub fn ack(&self, acked: u64) -> Ack {
        let mut state = self.state();
        if acked <= state.acked {
            return Ack::Stale;
        }
        if acked > state.sent {
            return Ack::Beyond;
        }
        state.acked = acked;
        drop(state);
        self.notify.notify_one();
        Ack::Accepted
    }

    /// Waits until an ack may have opened the window.
    async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Parses one `stream_input` body.
///
/// # Errors
///
/// Returns the `bad_message` detail when the body is malformed or the payload
/// decodes to more than [`MAX_INPUT_BYTES`].
pub fn parse_input(input: &StreamInput) -> Result<Input, String> {
    let has_any = input.data.is_some() || input.resize.is_some() || input.eof.is_some_and(|e| e);
    if !has_any {
        return Err("stream_input has no data, resize or eof".to_owned());
    }
    let data = match &input.data {
        Some(encoded) => {
            let decoded = BASE64
                .decode(encoded)
                .map_err(|e| format!("stream_input data is not base64: {e}"))?;
            if decoded.len() > MAX_INPUT_BYTES {
                return Err(format!(
                    "stream_input data is larger than {MAX_INPUT_BYTES} bytes"
                ));
            }
            Some(decoded)
        }
        None => None,
    };
    let resize = match input.resize {
        Some(size) => Some(validate_size(size.rows, size.cols)?),
        None => None,
    };
    Ok(Input {
        data,
        resize,
        eof: input.eof.unwrap_or(false),
    })
}

fn validate_size(rows: u16, cols: u16) -> Result<(u16, u16), String> {
    if rows == 0 || rows > MAX_SIZE || cols == 0 || cols > MAX_SIZE {
        return Err(format!("rows and cols must be between 1 and {MAX_SIZE}"));
    }
    Ok((rows, cols))
}

fn required_string(params: &serde_json::Value, name: &str) -> Result<String, String> {
    match params.get(name) {
        Some(serde_json::Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(format!("{name} must be a non-empty string")),
    }
}

fn optional_string(params: &serde_json::Value, name: &str) -> Result<Option<String>, String> {
    match params.get(name) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(format!("{name} must be a non-empty string")),
    }
}

fn size_param(params: &serde_json::Value, name: &str) -> Result<u16, String> {
    let value = params
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("{name} must be an integer"))?;
    let value = u16::try_from(value).map_err(|_| format!("{name} is out of range"))?;
    validate_size(value, 1).map(|(rows, _)| rows)
}

/// The `trm_` session id, validated so it can only name a file in the
/// transcripts directory.
fn session_id_param(params: &serde_json::Value) -> Result<String, String> {
    let id = required_string(params, "session_id")?;
    let well_formed = id.starts_with("trm_")
        && id.len() > 4
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !well_formed {
        return Err("session_id must be a trm_ id".to_owned());
    }
    Ok(id)
}

fn tail_param(params: &serde_json::Value) -> Result<Option<u64>, String> {
    match params.get("tail_bytes") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => {
            let tail = value
                .as_u64()
                .ok_or_else(|| "tail_bytes must be a non-negative integer".to_owned())?;
            if tail > MAX_TAIL_BYTES {
                return Err(format!("tail_bytes may be at most {MAX_TAIL_BYTES}"));
            }
            Ok(Some(tail))
        }
    }
}

struct TerminalParams {
    session_id: String,
    vm_id: String,
    rows: u16,
    cols: u16,
    argv: Vec<String>,
    user: Option<String>,
}

fn parse_terminal(params: &serde_json::Value) -> Result<TerminalParams, String> {
    let session_id = session_id_param(params)?;
    let vm_id = required_string(params, "vm_id")?;
    let rows = size_param(params, "rows")?;
    let cols = size_param(params, "cols")?;
    let argv = match params.get("argv") {
        Some(serde_json::Value::Array(items)) if !items.is_empty() => items
            .iter()
            .map(|item| {
                item.as_str()
                    .filter(|arg| !arg.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| "argv must hold non-empty strings".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("argv must be a non-empty array of strings".to_owned()),
    };
    Ok(TerminalParams {
        session_id,
        vm_id,
        rows,
        cols,
        argv,
        user: optional_string(params, "user")?,
    })
}

fn open_error(error: EngineError, vm_id: &str) -> OpenError {
    match error {
        EngineError::NotFound => OpenError::NotFound(format!("no VM {vm_id} on this node")),
        other => OpenError::NotAvailable(other.to_string()),
    }
}

impl Interactive {
    /// Opens a `terminal` stream: one exec session and its transcript.
    ///
    /// # Errors
    ///
    /// [`OpenError::BadMessage`] for malformed params, [`OpenError::NotFound`]
    /// when the VM is not on this node, and [`OpenError::NotAvailable`] when
    /// the engine refuses the session (a stopped VM, an old guest init).
    pub async fn open_terminal(
        &self,
        stream_id: &str,
        params: &serde_json::Value,
        tx: mpsc::Sender<NodeMessage>,
    ) -> Result<Opened, OpenError> {
        let params = parse_terminal(params).map_err(OpenError::BadMessage)?;
        let request = ExecRequest {
            argv: params.argv,
            user: params.user,
            rows: params.rows,
            cols: params.cols,
        };
        let exec = self
            .engine
            .exec(&VmId(params.vm_id.clone()), &request)
            .await
            .map_err(|e| open_error(e, &params.vm_id))?;
        // The resize channel and the exit receiver travel with the session.
        let ExecSession { io, resize, exit } = exec;
        let writer = TranscriptWriter::create(
            &self.store,
            &params.session_id,
            &params.vm_id,
            params.cols,
            params.rows,
            self.limits,
            util::now_secs(),
        )
        .await
        .map_err(|e| OpenError::NotAvailable(format!("could not start a transcript: {e}")))?;

        let (input_tx, input_rx) = mpsc::channel(INPUT_CAPACITY);
        let credit = Arc::new(Credit::new());
        let (door, cancel_rx, overflow) = new_door(StreamKind::Terminal, &credit, Some(input_tx));
        let handle = tokio::spawn(run_terminal(TerminalRun {
            stream_id: stream_id.to_owned(),
            io,
            resize,
            exit,
            writer,
            credit,
            input: input_rx,
            cancel: cancel_rx,
            overflow,
            tx,
            store: self.store.clone(),
            limits: self.limits,
        }));
        Ok(Opened { handle, door })
    }

    /// Opens a `console` stream: the VM's serial-output ring, then live output.
    ///
    /// # Errors
    ///
    /// [`OpenError::BadMessage`] for malformed params, [`OpenError::NotFound`]
    /// when the VM is not on this node, and [`OpenError::NotAvailable`] when
    /// its serial port is not a socket or the engine does not support it.
    pub fn open_console(
        &self,
        stream_id: &str,
        params: &serde_json::Value,
        tx: mpsc::Sender<NodeMessage>,
    ) -> Result<Opened, OpenError> {
        let vm_id = required_string(params, "vm_id").map_err(OpenError::BadMessage)?;
        let tail = tail_param(params).map_err(OpenError::BadMessage)?;
        let source = self
            .engine
            .serial_output(&VmId(vm_id.clone()), tail)
            .map_err(|e| open_error(e, &vm_id))?;
        let credit = Arc::new(Credit::new());
        let (door, cancel_rx, _) = new_door(StreamKind::Console, &credit, None);
        let handle = tokio::spawn(run_console(ConsoleRun {
            stream_id: stream_id.to_owned(),
            source,
            credit,
            cancel: cancel_rx,
            tx,
        }));
        Ok(Opened { handle, door })
    }

    /// Opens a `transcript` stream: the stored file from its first byte.
    ///
    /// # Errors
    ///
    /// [`OpenError::BadMessage`] for a malformed id and [`OpenError::NotFound`]
    /// when this node has neither the row nor the file.
    pub async fn open_transcript(
        &self,
        stream_id: &str,
        params: &serde_json::Value,
        tx: mpsc::Sender<NodeMessage>,
    ) -> Result<Opened, OpenError> {
        let session_id = session_id_param(params).map_err(OpenError::BadMessage)?;
        let row = self
            .store
            .transcript(&session_id)
            .await
            .map_err(|e| OpenError::NotAvailable(format!("could not read the transcript: {e}")))?
            .ok_or_else(|| {
                OpenError::NotFound(format!("no transcript {session_id} on this node"))
            })?;
        if !row.path.exists() {
            return Err(OpenError::NotFound(format!(
                "the transcript file for {session_id} is gone"
            )));
        }
        let credit = Arc::new(Credit::new());
        let (door, cancel_rx, _) = new_door(StreamKind::Transcript, &credit, None);
        let handle = tokio::spawn(run_transcript(TranscriptRun {
            stream_id: stream_id.to_owned(),
            path: row.path,
            credit,
            cancel: cancel_rx,
            tx,
        }));
        Ok(Opened { handle, door })
    }
}

/// Why a running stream stopped.
enum Finish {
    /// Answer `stream_end` with this reason, then drop the exec session.
    End {
        reason: StreamEndReason,
        message: Option<String>,
        exit_code: Option<i32>,
    },
    /// The session is gone; finalise and say nothing.
    Silent,
}

impl Finish {
    fn cancelled() -> Self {
        Self::End {
            reason: StreamEndReason::Cancelled,
            message: None,
            exit_code: None,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self::End {
            reason: StreamEndReason::Error,
            message: Some(message.into()),
            exit_code: None,
        }
    }

    fn transcript_error(error: TranscriptError) -> Self {
        match error {
            TranscriptError::Limit => Self::End {
                reason: StreamEndReason::Limit,
                message: Some("the transcript reached its size limit".to_owned()),
                exit_code: None,
            },
            TranscriptError::Io(message) => Self::error(message),
        }
    }
}

/// One event the terminal's run loop woke on.
enum TerminalEvent {
    Cancel,
    Input(Option<Input>),
    Exit(Result<i32, ()>),
    Read(std::io::Result<usize>),
    Credit,
}

struct TerminalRun {
    stream_id: String,
    io: quoro_engine::ConsoleStream,
    resize: mpsc::Sender<(u16, u16)>,
    exit: tokio::sync::oneshot::Receiver<i32>,
    writer: TranscriptWriter,
    credit: Arc<Credit>,
    input: mpsc::Receiver<Input>,
    cancel: watch::Receiver<bool>,
    overflow: Arc<AtomicBool>,
    tx: mpsc::Sender<NodeMessage>,
    store: NodeStore,
    limits: TranscriptLimits,
}

/// One step for the task that writes to the guest, in `stream_input` order.
enum WriteOp {
    Data(Vec<u8>),
    Resize(u16, u16),
    Eof,
}

/// The guest side of terminal input: a task of its own, so a program that
/// stops reading its input never stalls the output relay or the session.
struct GuestInput {
    ops: mpsc::UnboundedSender<WriteOp>,
    queued: Arc<AtomicUsize>,
}

async fn write_guest(
    mut half: WriteHalf<quoro_engine::ConsoleStream>,
    mut ops: mpsc::UnboundedReceiver<WriteOp>,
    resize: mpsc::Sender<(u16, u16)>,
    queued: Arc<AtomicUsize>,
) {
    while let Some(op) = ops.recv().await {
        match op {
            WriteOp::Data(data) => {
                if let Err(e) = half.write_all(&data).await {
                    // The program may have exited just before the write; the
                    // exit receiver reports it and the stream ends correctly.
                    debug!(error = %e, "could not write terminal input");
                }
                let _ = half.flush().await;
                queued.fetch_sub(data.len(), Ordering::SeqCst);
            }
            WriteOp::Resize(rows, cols) => {
                let _ = resize.send((rows, cols)).await;
            }
            WriteOp::Eof => {
                if let Err(e) = half.shutdown().await {
                    debug!(error = %e, "could not close terminal input");
                }
            }
        }
    }
}

async fn run_terminal(run: TerminalRun) {
    let TerminalRun {
        stream_id,
        io,
        resize,
        exit,
        mut writer,
        credit,
        mut input,
        mut cancel,
        overflow,
        tx,
        store,
        limits,
    } = run;
    let (mut io, write_half) = tokio::io::split(io);
    let (ops_tx, ops_rx) = mpsc::unbounded_channel();
    let queued = Arc::new(AtomicUsize::new(0));
    let write_task = tokio::spawn(write_guest(write_half, ops_rx, resize, Arc::clone(&queued)));
    let guest = GuestInput {
        ops: ops_tx,
        queued,
    };
    let cancelled = || {
        if overflow.load(Ordering::SeqCst) {
            Finish::error("the terminal input queue overflowed")
        } else {
            Finish::cancelled()
        }
    };
    let mut exit = Some(exit);
    let mut seq = 1u64;
    let mut pending: Vec<u8> = Vec::new();
    let mut eof = false;
    // `Some(code)` once the program exited (`None` when the end had no code).
    let mut exited: Option<Option<i32>> = None;
    let finish;

    loop {
        // 1. Send what is buffered, up to the window.
        if !pending.is_empty() {
            match flush_terminal(
                &mut pending,
                &credit,
                &mut writer,
                &tx,
                &stream_id,
                &mut seq,
            )
            .await
            {
                Ok(()) => {}
                Err(stop) => {
                    finish = stop;
                    break;
                }
            }
            if !pending.is_empty() {
                // The window is full: wait for credit, input, an exit or a close.
                tokio::select! {
                    biased;
                    _ = cancel.changed() => { finish = cancelled(); break; }
                    received = input.recv() => match received {
                        Some(item) => {
                            if let Err(stop) = apply_input(item, &guest, &mut writer).await {
                                finish = stop;
                                break;
                            }
                        }
                        None => { finish = cancelled(); break; }
                    },
                    code = wait_exit(&mut exit) => { exited = Some(code.ok()); }
                    _ = credit.wait() => {}
                }
                continue;
            }
        }

        // 2. The program exited: drain the rest of its output, then end.
        if let Some(code) = exited {
            if eof {
                finish = Finish::End {
                    reason: if code.is_some() {
                        StreamEndReason::Completed
                    } else {
                        StreamEndReason::VmStopped
                    },
                    message: None,
                    exit_code: code,
                };
                break;
            }
            if credit.available() == 0 {
                tokio::select! {
                    biased;
                    _ = cancel.changed() => { finish = cancelled(); break; }
                    _ = credit.wait() => {}
                }
                continue;
            }
            // The guest closes the channel after its last output; a channel
            // that stays open past the deadline ends the stream anyway.
            let mut buffer = vec![0u8; MAX_DATA_BYTES];
            match tokio::time::timeout(DRAIN_TIMEOUT, io.read(&mut buffer)).await {
                Err(_) | Ok(Ok(0)) => eof = true,
                Ok(Ok(read)) => {
                    buffer.truncate(read);
                    pending.extend_from_slice(&buffer);
                }
                Ok(Err(e)) => {
                    finish = Finish::End {
                        reason: StreamEndReason::Error,
                        message: Some(format!("the exec session failed: {e}")),
                        exit_code: code,
                    };
                    break;
                }
            }
            continue;
        }

        // 3. Run: relay output, apply input, notice the exit or a close.
        //
        // The select only picks an event; the IO happens afterwards, so the
        // read future's borrow of `io` ends before input is applied.
        let available = credit.available();
        let mut buffer = vec![0u8; available.min(MAX_DATA_BYTES)];
        let event = tokio::select! {
            biased;
            _ = cancel.changed() => TerminalEvent::Cancel,
            received = input.recv() => TerminalEvent::Input(received),
            code = wait_exit(&mut exit) => TerminalEvent::Exit(code),
            read = io.read(&mut buffer), if available > 0 && !eof => TerminalEvent::Read(read),
            _ = credit.wait(), if available == 0 => TerminalEvent::Credit,
        };
        match event {
            TerminalEvent::Cancel | TerminalEvent::Input(None) => {
                finish = cancelled();
                break;
            }
            TerminalEvent::Input(Some(item)) => {
                if let Err(stop) = apply_input(item, &guest, &mut writer).await {
                    finish = stop;
                    break;
                }
            }
            TerminalEvent::Exit(code) => exited = Some(code.ok()),
            TerminalEvent::Credit => {}
            TerminalEvent::Read(Ok(0)) => eof = true,
            TerminalEvent::Read(Ok(read)) => {
                buffer.truncate(read);
                coalesce_io(&mut io, &mut buffer).await;
                pending.extend_from_slice(&buffer);
            }
            TerminalEvent::Read(Err(e)) => {
                finish = Finish::End {
                    reason: StreamEndReason::Error,
                    message: Some(format!("the exec session failed: {e}")),
                    exit_code: None,
                };
                break;
            }
        }
    }

    // Stop writing to the guest; with both halves gone the exec session is
    // dropped, which ends the guest program.
    drop(guest);
    write_task.abort();
    let _ = write_task.await;
    drop(io);

    // Finalise the transcript and answer.
    let fields = match writer.finish().await {
        Ok(fields) => Some(fields),
        Err(e) => {
            warn!(stream_id = %stream_id, error = %e, "could not finalise a transcript");
            None
        }
    };
    if let Finish::End {
        reason,
        message,
        exit_code,
    } = finish
    {
        let _ = tx
            .send(NodeMessage::StreamEnd(StreamEnd {
                stream_id,
                reason,
                message,
                exit_code,
                transcript_sha256: fields.as_ref().map(|(sha, _)| sha.clone()),
                transcript_bytes: fields.as_ref().map(|(_, bytes)| *bytes),
            }))
            .await;
    }
    if let Err(e) = transcript::sweep(&store, limits, util::now_secs()).await {
        warn!(error = %e, "could not prune transcripts after a session");
    }
}

/// Applies one `stream_input` in the contract's order: data, resize, eof.
///
/// The transcript records the input and the resize before they are queued for
/// the guest, so it never misses a byte the guest got. The guest writes happen
/// on the writer task, in the same order.
async fn apply_input(
    input: Input,
    guest: &GuestInput,
    writer: &mut TranscriptWriter,
) -> Result<(), Finish> {
    if let Some(data) = input.data {
        if guest.queued.load(Ordering::SeqCst) + data.len() > MAX_QUEUED_INPUT {
            return Err(Finish::error("the program stopped reading its input"));
        }
        writer
            .input(&data)
            .await
            .map_err(Finish::transcript_error)?;
        guest.queued.fetch_add(data.len(), Ordering::SeqCst);
        let _ = guest.ops.send(WriteOp::Data(data));
    }
    if let Some((rows, cols)) = input.resize {
        writer
            .resize(rows, cols)
            .await
            .map_err(Finish::transcript_error)?;
        let _ = guest.ops.send(WriteOp::Resize(rows, cols));
    }
    if input.eof {
        let _ = guest.ops.send(WriteOp::Eof);
    }
    Ok(())
}

async fn wait_exit(exit: &mut Option<tokio::sync::oneshot::Receiver<i32>>) -> Result<i32, ()> {
    let Some(receiver) = exit.as_mut() else {
        return std::future::pending().await;
    };
    let result = receiver.await.map_err(|_| ());
    *exit = None;
    result
}

/// Reads more output for a few milliseconds so one keystroke is one frame.
async fn coalesce_io(io: &mut ReadHalf<quoro_engine::ConsoleStream>, buffer: &mut Vec<u8>) {
    let deadline = Instant::now() + COALESCE;
    while buffer.len() < MAX_DATA_BYTES {
        let mut extra = vec![0u8; MAX_DATA_BYTES - buffer.len()];
        match tokio::time::timeout_at(deadline, io.read(&mut extra)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(read)) => {
                extra.truncate(read);
                buffer.extend_from_slice(&extra);
            }
        }
    }
}

/// Sends buffered terminal output as `data` frames while the window allows.
///
/// Each chunk is written to the transcript before it leaves the node, so a
/// session the cap ends has every byte it delivered on disk.
async fn flush_terminal(
    pending: &mut Vec<u8>,
    credit: &Credit,
    writer: &mut TranscriptWriter,
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    seq: &mut u64,
) -> Result<(), Finish> {
    let mut drained = 0usize;
    while drained < pending.len() {
        let available = credit.available();
        if available == 0 {
            break;
        }
        let take = (pending.len() - drained).min(MAX_DATA_BYTES).min(available);
        let chunk = &pending[drained..drained + take];
        writer
            .output(chunk)
            .await
            .map_err(Finish::transcript_error)?;
        credit.add_sent(take);
        if !send_data(tx, stream_id, seq, chunk).await {
            return Err(Finish::Silent);
        }
        drained += take;
    }
    pending.drain(..drained);
    Ok(())
}

/// Sends buffered bytes without a transcript (console and transcript streams).
async fn flush_plain(
    pending: &mut Vec<u8>,
    credit: &Credit,
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    seq: &mut u64,
) -> Result<(), Finish> {
    let mut drained = 0usize;
    while drained < pending.len() {
        let available = credit.available();
        if available == 0 {
            break;
        }
        let take = (pending.len() - drained).min(MAX_DATA_BYTES).min(available);
        let chunk = &pending[drained..drained + take];
        credit.add_sent(take);
        if !send_data(tx, stream_id, seq, chunk).await {
            return Err(Finish::Silent);
        }
        drained += take;
    }
    pending.drain(..drained);
    Ok(())
}

async fn send_data(
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    seq: &mut u64,
    bytes: &[u8],
) -> bool {
    let frame = StreamData {
        stream_id: stream_id.to_owned(),
        seq: *seq,
        lines: None,
        data: Some(BASE64.encode(bytes)),
        metrics: None,
    };
    *seq = seq.saturating_add(1);
    tx.send(NodeMessage::StreamData(frame)).await.is_ok()
}

async fn send_end(tx: &mpsc::Sender<NodeMessage>, stream_id: &str, finish: Finish) {
    if let Finish::End {
        reason,
        message,
        exit_code,
    } = finish
    {
        let _ = tx
            .send(NodeMessage::StreamEnd(StreamEnd {
                stream_id: stream_id.to_owned(),
                reason,
                message,
                exit_code,
                transcript_sha256: None,
                transcript_bytes: None,
            }))
            .await;
    }
}

struct ConsoleRun {
    stream_id: String,
    source: ByteStream,
    credit: Arc<Credit>,
    cancel: watch::Receiver<bool>,
    tx: mpsc::Sender<NodeMessage>,
}

async fn run_console(run: ConsoleRun) {
    let ConsoleRun {
        stream_id,
        mut source,
        credit,
        mut cancel,
        tx,
    } = run;
    let mut seq = 1u64;
    let mut pending: Vec<u8> = Vec::new();
    let mut ended = false;
    let finish;

    loop {
        if !pending.is_empty() {
            match flush_plain(&mut pending, &credit, &tx, &stream_id, &mut seq).await {
                Ok(()) => {}
                Err(stop) => {
                    finish = stop;
                    break;
                }
            }
            if !pending.is_empty() {
                tokio::select! {
                    biased;
                    _ = cancel.changed() => { finish = Finish::cancelled(); break; }
                    _ = credit.wait() => {}
                }
                continue;
            }
        }
        if ended {
            finish = Finish::End {
                reason: StreamEndReason::VmStopped,
                message: None,
                exit_code: None,
            };
            break;
        }

        let available = credit.available();
        let event = tokio::select! {
            biased;
            _ = cancel.changed() => ConsoleEvent::Cancel,
            chunk = source.next(), if available > 0 => ConsoleEvent::Chunk(chunk),
            _ = credit.wait(), if available == 0 => ConsoleEvent::Credit,
        };
        match event {
            ConsoleEvent::Cancel => {
                finish = Finish::cancelled();
                break;
            }
            ConsoleEvent::Credit => {}
            ConsoleEvent::Chunk(None) => ended = true,
            ConsoleEvent::Chunk(Some(bytes)) => {
                pending.extend_from_slice(&bytes);
                coalesce_stream(&mut source, &mut pending).await;
            }
        }
    }
    send_end(&tx, &stream_id, finish).await;
}

/// One event a console stream woke on.
enum ConsoleEvent {
    Cancel,
    Chunk(Option<Vec<u8>>),
    Credit,
}

async fn coalesce_stream(source: &mut ByteStream, buffer: &mut Vec<u8>) {
    let deadline = Instant::now() + COALESCE;
    while buffer.len() < MAX_DATA_BYTES {
        match tokio::time::timeout_at(deadline, source.next()).await {
            Ok(Some(more)) => buffer.extend_from_slice(&more),
            _ => break,
        }
    }
}

struct TranscriptRun {
    stream_id: String,
    path: std::path::PathBuf,
    credit: Arc<Credit>,
    cancel: watch::Receiver<bool>,
    tx: mpsc::Sender<NodeMessage>,
}

async fn run_transcript(run: TranscriptRun) {
    let TranscriptRun {
        stream_id,
        path,
        credit,
        mut cancel,
        tx,
    } = run;
    let mut file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(e) => {
            send_end(
                &tx,
                &stream_id,
                Finish::error(format!("could not read the transcript: {e}")),
            )
            .await;
            return;
        }
    };
    let mut seq = 1u64;
    let mut pending: Vec<u8> = Vec::new();
    let mut eof = false;
    let finish;

    loop {
        if !pending.is_empty() {
            match flush_plain(&mut pending, &credit, &tx, &stream_id, &mut seq).await {
                Ok(()) => {}
                Err(stop) => {
                    finish = stop;
                    break;
                }
            }
            if !pending.is_empty() {
                tokio::select! {
                    biased;
                    _ = cancel.changed() => { finish = Finish::cancelled(); break; }
                    _ = credit.wait() => {}
                }
                continue;
            }
        }
        if eof {
            finish = Finish::End {
                reason: StreamEndReason::Completed,
                message: None,
                exit_code: None,
            };
            break;
        }
        if credit.available() == 0 {
            tokio::select! {
                biased;
                _ = cancel.changed() => { finish = Finish::cancelled(); break; }
                _ = credit.wait() => {}
            }
            continue;
        }
        let mut buffer = vec![0u8; MAX_DATA_BYTES];
        let event = tokio::select! {
            biased;
            _ = cancel.changed() => FileEvent::Cancel,
            read = file.read(&mut buffer) => FileEvent::Read(read),
        };
        match event {
            FileEvent::Cancel => {
                finish = Finish::cancelled();
                break;
            }
            FileEvent::Read(Ok(0)) => eof = true,
            FileEvent::Read(Ok(read)) => {
                buffer.truncate(read);
                pending.extend_from_slice(&buffer);
            }
            FileEvent::Read(Err(e)) => {
                finish = Finish::error(format!("could not read the transcript: {e}"));
                break;
            }
        }
    }
    send_end(&tx, &stream_id, finish).await;
}

/// One event a transcript stream woke on.
enum FileEvent {
    Cancel,
    Read(std::io::Result<usize>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_proto::wire::WindowSize;
    use serde_json::json;

    #[test]
    fn a_window_extends_only_on_a_new_ack() {
        let credit = Credit::new();
        assert_eq!(credit.available(), STREAM_WINDOW_BYTES);
        credit.add_sent(1000);
        assert_eq!(credit.available(), STREAM_WINDOW_BYTES - 1000);
        assert_eq!(credit.ack(1000), Ack::Accepted);
        assert_eq!(credit.available(), STREAM_WINDOW_BYTES);
        assert_eq!(credit.ack(500), Ack::Stale);
        assert_eq!(credit.ack(1000), Ack::Stale);
        assert_eq!(credit.ack(1001), Ack::Beyond);
    }

    #[test]
    fn input_is_validated_and_decoded() {
        let input = parse_input(&StreamInput {
            stream_id: "msg_1".to_owned(),
            seq: 1,
            data: Some(BASE64.encode(b"ls\n")),
            resize: Some(WindowSize {
                rows: 50,
                cols: 132,
            }),
            eof: Some(true),
        })
        .unwrap();
        assert_eq!(input.data.as_deref(), Some(&b"ls\n"[..]));
        assert_eq!(input.resize, Some((50, 132)));
        assert!(input.eof);

        for body in [
            StreamInput {
                stream_id: "msg_1".to_owned(),
                seq: 1,
                data: None,
                resize: None,
                eof: None,
            },
            StreamInput {
                stream_id: "msg_1".to_owned(),
                seq: 1,
                data: Some("not base64!".to_owned()),
                resize: None,
                eof: None,
            },
            StreamInput {
                stream_id: "msg_1".to_owned(),
                seq: 1,
                data: None,
                resize: Some(WindowSize { rows: 0, cols: 80 }),
                eof: None,
            },
        ] {
            assert!(parse_input(&body).is_err(), "{body:?}");
        }

        let huge = BASE64.encode(vec![b'x'; MAX_INPUT_BYTES + 1]);
        assert!(
            parse_input(&StreamInput {
                stream_id: "msg_1".to_owned(),
                seq: 1,
                data: Some(huge),
                resize: None,
                eof: None,
            })
            .is_err()
        );
    }

    #[test]
    fn terminal_params_are_checked() {
        assert!(
            parse_terminal(&json!({
                "session_id": "trm_one",
                "vm_id": "vm_1",
                "rows": 24,
                "cols": 80,
                "argv": ["/bin/sh"],
            }))
            .is_ok()
        );
        for params in [
            json!({"vm_id": "vm_1", "rows": 24, "cols": 80, "argv": ["/bin/sh"]}),
            json!({"session_id": "trm_1", "rows": 24, "cols": 80, "argv": ["/bin/sh"]}),
            json!({"session_id": "trm_1", "vm_id": "vm_1", "rows": 0, "cols": 80, "argv": ["/bin/sh"]}),
            json!({"session_id": "trm_1", "vm_id": "vm_1", "rows": 24, "cols": 1001, "argv": ["/bin/sh"]}),
            json!({"session_id": "trm_1", "vm_id": "vm_1", "rows": 24, "cols": 80, "argv": []}),
            json!({"session_id": "../escape", "vm_id": "vm_1", "rows": 24, "cols": 80, "argv": ["/bin/sh"]}),
        ] {
            assert!(parse_terminal(&params).is_err(), "{params}");
        }
    }
}
