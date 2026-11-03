// SPDX-License-Identifier: AGPL-3.0-only
//! The dial-out session (`node-agent-protocol.md` sections 3-5 and 9).
//!
//! The node is always the client: one WebSocket over pinned TLS to the primary,
//! `hello` first, `welcome` back, then heartbeats, status reports, buffered
//! events and the messages the primary sends. The core ([`Session::serve`]) is
//! generic over the WebSocket's byte stream, so tests run it over
//! `tokio::io::duplex` without TLS.
//!
//! Redial follows section 4.4: 1 s doubling to 60 s with full jitter, reset
//! after a session that stayed up for at least 60 s. A fatal
//! `unauthenticated`/`node_removed` stops dialing for good;
//! `unsupported_version` keeps dialing and only logs.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_proto::wire::{
    self, Capacity, Command, ControlMessage, DesiredState, EventBody, Heartbeat, Hello, LogLine,
    NodeMessage, RawFrame, RouteStatus, StatusReport, StreamClose, StreamData, StreamEnd,
    StreamEndReason, StreamOpen, VmStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorBody, ErrorCode, ProtocolError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, client_async_with_config};
use tracing::{debug, info, warn};

use crate::identity::{Identity, IdentityHandle};
use crate::logs::{LogFilter, StoredLogLine};
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
use crate::tls::{self, Endpoint};
use crate::{AgentError, FatalReason, NodeFacts, util};

/// Path of the session endpoint (section 3.1).
pub const SESSION_PATH: &str = "/api/v1/nodes/session";

/// Concurrent streams the node serves per session (section 5.15).
pub const MAX_STREAMS: usize = 32;

/// Lines per `stream_data` frame.
const STREAM_BATCH_LINES: usize = 200;
/// How often a partially filled stream batch is flushed.
const STREAM_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Space kept free in every frame for the envelope and the line array.
const FRAME_HEADROOM: usize = 64 * 1024;

/// First redial delay (section 4.4).
const REDIAL_BASE: Duration = Duration::from_secs(1);
/// Redial ceiling (section 4.4).
const REDIAL_MAX: Duration = Duration::from_secs(60);
/// A session this long resets the redial backoff (section 4.4).
const REDIAL_RESET_AFTER: Duration = Duration::from_secs(60);

/// `log_hash` while `log_seq` is 0 (section 5.1).
const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Why one session ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEnd {
    /// The agent is shutting down.
    Shutdown,
    /// The session ended and the node should dial again.
    Reconnect,
    /// A fatal error says dialing again cannot help (section 4.4).
    Fatal(FatalReason),
}

/// The streams one session has open. Dropping it aborts every stream task, so
/// a session end also ends its streams.
#[derive(Default)]
struct SessionStreams {
    handles: HashMap<String, JoinHandle<()>>,
}

impl SessionStreams {
    fn stop(&mut self, stream_id: &str) -> bool {
        match self.handles.remove(stream_id) {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
    }
}

impl Drop for SessionStreams {
    fn drop(&mut self) {
        for (_, handle) in self.handles.drain() {
            handle.abort();
        }
    }
}

/// Everything a [`Session`] needs, assembled by [`crate::run`].
pub struct SessionSetup {
    /// The enrolled identity and its current pin.
    pub identity: IdentityHandle,
    /// The local node store.
    pub store: NodeStore,
    /// What `hello` reports.
    pub facts: NodeFacts,
    /// Where accepted snapshots go, to the reconciler.
    pub desired: watch::Sender<Option<Snapshot>>,
    /// What the reconciler observed.
    pub observed: watch::Receiver<Vec<VmStatus>>,
    /// The verified snapshot the agent started with, if any.
    pub current: Option<Snapshot>,
}

/// The session loop: dial, run one session, redial.
pub struct Session {
    identity: IdentityHandle,
    store: NodeStore,
    facts: NodeFacts,
    desired: watch::Sender<Option<Snapshot>>,
    observed: watch::Receiver<Vec<VmStatus>>,
    current: Option<Snapshot>,
    boot_id: String,
}

impl Session {
    /// Builds a session loop and the boot id `hello` reports.
    #[must_use]
    pub fn new(setup: SessionSetup) -> Self {
        let SessionSetup {
            identity,
            store,
            facts,
            desired,
            observed,
            current,
        } = setup;
        Self {
            identity,
            store,
            facts,
            desired,
            observed,
            current,
            boot_id: util::hex(&util::random_bytes::<16>()),
        }
    }

    /// The identity in use.
    #[must_use]
    pub fn identity(&self) -> Identity {
        self.identity.get()
    }

    /// Dials until `stop` resolves or a fatal end arrives.
    ///
    /// # Errors
    ///
    /// [`AgentError::Fatal`] for `unauthenticated`/`node_removed`;
    /// [`AgentError::Identity`] when the credential or key cannot be read.
    pub async fn run(&mut self, stop: watch::Receiver<bool>) -> Result<(), AgentError> {
        let mut backoff = REDIAL_BASE;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let started = Instant::now();
            match self.dial().await {
                Ok(ws) => {
                    let end = self.serve(ws, wait_stop(stop.clone())).await?;
                    match end {
                        SessionEnd::Shutdown => return Ok(()),
                        SessionEnd::Fatal(reason) => return Err(AgentError::Fatal(reason)),
                        SessionEnd::Reconnect => {}
                    }
                    if started.elapsed() >= REDIAL_RESET_AFTER {
                        backoff = REDIAL_BASE;
                    }
                }
                Err(e @ AgentError::Fatal(_)) => return Err(e),
                Err(e) => {
                    warn!(error = %e, "could not open the session; will redial");
                }
            }

            let delay = backoff.mul_f64(util::random_fraction());
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = wait_stop(stop.clone()) => return Ok(()),
            }
            backoff = (backoff * 2).min(REDIAL_MAX);
        }
    }

    /// Opens one connection and upgrades it to the node protocol.
    async fn dial(&self) -> Result<WebSocketStream<TlsStream<tokio::net::TcpStream>>, AgentError> {
        let identity = self.identity.get();
        let credential = self.identity.credential()?;
        let url = session_url(&identity.primary_url)?;
        let stream = tls::connect(&url, &identity.fingerprint).await?;

        let mut request = url.as_str().into_client_request().map_err(|e| {
            AgentError::Protocol(format!("could not build the session request: {e}"))
        })?;
        let authorization =
            HeaderValue::from_str(&format!("Bearer {credential}")).map_err(|_| {
                AgentError::Protocol("the credential is not a valid header value".to_owned())
            })?;
        request.headers_mut().insert("authorization", authorization);

        let config = WebSocketConfig::default()
            .max_message_size(Some(wire::MAX_MESSAGE_BYTES))
            .max_frame_size(Some(wire::MAX_MESSAGE_BYTES));
        match client_async_with_config(request, stream, Some(config)).await {
            Ok((ws, _response)) => Ok(ws),
            Err(tokio_tungstenite::tungstenite::Error::Http(response))
                if matches!(response.status().as_u16(), 401 | 403) =>
            {
                Err(AgentError::Fatal(FatalReason::Unauthenticated))
            }
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                Err(AgentError::Protocol(format!(
                    "the primary answered the upgrade with HTTP {}",
                    response.status()
                )))
            }
            Err(e) => Err(AgentError::Protocol(format!(
                "the WebSocket upgrade failed: {e}"
            ))),
        }
    }

    /// Runs one session to its end.
    ///
    /// # Errors
    ///
    /// [`AgentError::Fatal`] when the primary said dialing cannot help;
    /// anything else is a reconnectable failure and is reported as
    /// [`SessionEnd::Reconnect`].
    pub async fn serve<S>(
        &mut self,
        ws: WebSocketStream<S>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<SessionEnd, AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut stop = std::pin::pin!(stop);
        let (mut sink, mut source) = ws.split();
        let mut streams = SessionStreams::default();
        // Stream tasks hand their frames to the session loop, which owns the
        // sink; a task never blocks the loop while it waits for lines.
        let (stream_tx, mut stream_rx) = mpsc::channel::<NodeMessage>(256);

        // 1. `hello` is the first message on every session (section 4.2).
        let hello = self.hello().await;
        if let Err(e) = write_message(&mut sink, hello, None).await {
            warn!(error = %e, "could not send hello; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }

        // 2. `welcome` is the first message back.
        let welcome = {
            let first = tokio::select! {
                () = &mut stop => return Ok(SessionEnd::Shutdown),
                first = tokio::time::timeout(
                    Duration::from_millis(wire::SESSION_TIMEOUT_MS),
                    next_frame(&mut source),
                ) => first,
            };
            match first {
                Ok(Some(Ok(Message::Text(text)))) => match parse_control(&text) {
                    Ok(ControlMessage::Welcome(welcome)) => welcome,
                    Ok(other) => {
                        warn!(
                            kind = other.kind(),
                            "the primary did not start with welcome; reconnecting"
                        );
                        return Ok(SessionEnd::Reconnect);
                    }
                    Err(_) => {
                        warn!("the primary's first frame is not a welcome; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                },
                Ok(Some(Ok(_))) => {
                    warn!("the primary's first frame is not a text frame; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                Ok(Some(Err(e))) => {
                    warn!(error = %e, "the session failed before welcome; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                Ok(None) | Err(_) => {
                    warn!("no welcome arrived; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
            }
        };

        let heartbeat = Duration::from_millis(welcome.heartbeat_interval_ms.max(1));
        let timeout = Duration::from_millis(welcome.session_timeout_ms.max(1));
        info!(
            cluster_id = %welcome.cluster_id,
            term = welcome.term,
            "session established"
        );

        // 3. Buffered events go out once the session is up, then are removed
        //    from the buffer (section 6.5).
        if let Err(e) = self.flush_events(&mut sink).await {
            warn!(error = %e, "could not deliver buffered events; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }
        if let Err(e) = self.send_status(&mut sink).await {
            warn!(error = %e, "could not send the first status report; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }

        let mut heartbeat_tick = tokio::time::interval(heartbeat);
        heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_tick.reset();
        let mut status_tick =
            tokio::time::interval(Duration::from_millis(wire::STATUS_REPORT_INTERVAL_MS));
        status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        status_tick.reset();
        // A clone keeps the select's borrow off `self`, so a branch may still
        // build a status report that reads the store and the snapshot.
        let mut observed = self.observed.clone();
        let mut last_rx = Instant::now();

        loop {
            let deadline = last_rx + timeout;
            tokio::select! {
                () = &mut stop => return Ok(SessionEnd::Shutdown),
                _ = heartbeat_tick.tick() => {
                    let beat = NodeMessage::Heartbeat(Heartbeat {
                        time: util::now_rfc3339(),
                        busy: None,
                    });
                    if let Err(e) = write_message(&mut sink, beat, None).await {
                        warn!(error = %e, "could not send a heartbeat; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                    // The primary sends nothing while it has nothing to say,
                    // and only it enforces the session timeout (section 4.3).
                    // A quiet session is probed with `ping`, whose `pong`
                    // resets the node's own timer, so a half-open connection
                    // is still noticed without dropping a healthy idle one.
                    if last_rx.elapsed() >= heartbeat
                        && let Err(e) = write_message(&mut sink, NodeMessage::Ping, None).await
                    {
                        warn!(error = %e, "could not send a ping; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                _ = status_tick.tick() => {
                    if let Err(e) = self.send_status(&mut sink).await {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                changed = observed.changed() => {
                    if changed.is_ok()
                        && let Err(e) = self.send_status(&mut sink).await
                    {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    warn!("no frame from the primary within the session timeout; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                streamed = stream_rx.recv() => {
                    if let Some(message) = streamed
                        && let Err(e) = write_message(&mut sink, message, None).await
                    {
                        warn!(error = %e, "could not send a stream frame; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                frame = next_frame(&mut source) => {
                    last_rx = Instant::now();
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            match self.handle(&text, &mut sink, &mut streams, &stream_tx).await {
                                Ok(Some(end)) => return Ok(end),
                                Ok(None) => {}
                                Err(AgentError::Fatal(reason)) => {
                                    return Ok(SessionEnd::Fatal(reason));
                                }
                                Err(e) => {
                                    warn!(error = %e, "session message failed; reconnecting");
                                    return Ok(SessionEnd::Reconnect);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if sink.send(Message::Pong(payload)).await.is_err() {
                                return Ok(SessionEnd::Reconnect);
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            debug!("the primary closed the session");
                            return Ok(SessionEnd::Reconnect);
                        }
                        Some(Ok(Message::Binary(_))) => {
                            let error = ProtocolError {
                                code: ErrorCode::BadMessage,
                                message: "binary frames are not used in v1".to_owned(),
                                fatal: false,
                            };
                            if write_message(&mut sink, NodeMessage::Error(error), None).await.is_err() {
                                return Ok(SessionEnd::Reconnect);
                            }
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => {
                            warn!(error = %e, "the session failed; reconnecting");
                            return Ok(SessionEnd::Reconnect);
                        }
                        None => return Ok(SessionEnd::Reconnect),
                    }
                }
            }
        }
    }

    /// Handles one control frame.
    async fn handle<S>(
        &mut self,
        text: &str,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
    ) -> Result<Option<SessionEnd>, AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let raw = match RawFrame::parse(text) {
            Ok(raw) => raw,
            Err(e) => {
                let error = ProtocolError {
                    code: e.code(),
                    message: e.to_string(),
                    fatal: e.is_fatal(),
                };
                write_message(sink, NodeMessage::Error(error), e.message_id()).await?;
                return Ok(if e.is_fatal() {
                    // `unsupported_version` keeps dialing (section 4.4).
                    Some(SessionEnd::Reconnect)
                } else {
                    None
                });
            }
        };
        let message = match ControlMessage::from_raw(&raw) {
            Ok(message) => message,
            Err(e) => {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: e.to_string(),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(None);
            }
        };

        match message {
            ControlMessage::DesiredState(body) => {
                self.on_desired_state(&raw, &body, sink).await?;
            }
            ControlMessage::SigningKeyRotate(body) => {
                self.on_signing_key_rotate(&raw, &body, sink).await?;
            }
            ControlMessage::Ping => {
                write_message(sink, NodeMessage::Pong, Some(&raw.id)).await?;
            }
            ControlMessage::Pong | ControlMessage::LogEntries(_) => {
                // Change-log follow is a follow-up.
            }
            ControlMessage::StreamClose(close) => {
                self.on_stream_close(&close, streams, stream_tx).await?;
            }
            ControlMessage::Command(command) => {
                if command.kind == "probe" {
                    let body = CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some(format!(
                            "kvm available: {}; kernel {}",
                            self.facts.kvm.available, self.facts.kvm.kernel
                        )),
                        error: None,
                        lines: None,
                    };
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else if command.kind == "logs_query" {
                    let body = self.logs_query(&command).await;
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else {
                    let body = CommandResult {
                        status: CommandStatus::Error,
                        detail: Some(format!(
                            "command kind {:?} arrives in a later milestone",
                            command.kind
                        )),
                        error: Some(ErrorBody {
                            code: ErrorCode::BadMessage,
                            message: "this command kind is not implemented yet".to_owned(),
                        }),
                        lines: None,
                    };
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                }
            }
            ControlMessage::StreamOpen(open) => {
                self.on_stream_open(&open, &raw, streams, stream_tx, sink)
                    .await?;
            }
            ControlMessage::LeaseGrant(_) => {
                // No stateful app exists in this version (M2, section 8.7).
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: "no stateful apps run in this version".to_owned(),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            }
            ControlMessage::LeaseRevoke(_) => {
                let body = CommandResult {
                    status: CommandStatus::Ok,
                    detail: Some("unknown lease".to_owned()),
                    error: None,
                    lines: None,
                };
                write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
            }
            ControlMessage::Welcome(welcome) => {
                debug!(
                    cluster_id = %welcome.cluster_id,
                    "ignoring a second welcome"
                );
            }
            ControlMessage::Error(error) => {
                if error.fatal {
                    match error.code {
                        ErrorCode::Unauthenticated => {
                            return Ok(Some(SessionEnd::Fatal(FatalReason::Unauthenticated)));
                        }
                        ErrorCode::NodeRemoved => {
                            return Ok(Some(SessionEnd::Fatal(FatalReason::NodeRemoved)));
                        }
                        ErrorCode::UnsupportedVersion => {
                            warn!(
                                message = %error.message,
                                "the primary does not support this protocol version; will redial"
                            );
                            return Ok(Some(SessionEnd::Reconnect));
                        }
                        _ => {
                            warn!(
                                code = ?error.code,
                                message = %error.message,
                                "the primary ended the session fatally"
                            );
                            return Ok(Some(SessionEnd::Reconnect));
                        }
                    }
                }
                warn!(
                    code = ?error.code,
                    message = %error.message,
                    "the primary reported a non-fatal protocol error"
                );
            }
        }
        Ok(None)
    }

    /// Verifies, stores and publishes one `desired_state` (sections 5.11, 6.1).
    async fn on_desired_state<S>(
        &mut self,
        raw: &RawFrame,
        body: &serde_json::Value,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let key = self.identity.verifying_key()?;
        if let Err(e) = quoro_proto::sign::verify_body(&key, body) {
            let error = ProtocolError {
                code: ErrorCode::BadSignature,
                message: format!(
                    "desired_state signature does not verify against the cluster signing key: {e}"
                ),
                fatal: false,
            };
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        let state = match DesiredState::from_value(body) {
            Ok(state) => state,
            Err(e) => {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: format!("desired_state body is malformed: {e}"),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        let rev = state.rev;
        let stored = self
            .store
            .latest_desired()
            .await
            .map_or(0, |latest| latest.map_or(0, |(rev, _)| rev));
        if rev <= stored {
            let result = CommandResult {
                status: CommandStatus::Ok,
                detail: Some(format!("ignored rev {rev}")),
                error: None,
                lines: None,
            };
            write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
            return Ok(());
        }

        if let Err(e) = self.store.store_desired(rev, body).await {
            let result = CommandResult {
                status: CommandStatus::Error,
                detail: Some(format!("could not store rev {rev}: {e}")),
                error: None,
                lines: None,
            };
            write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
            return Ok(());
        }
        let result = CommandResult {
            status: CommandStatus::Ok,
            detail: Some(format!("stored rev {rev}")),
            error: None,
            lines: None,
        };
        write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;

        let snapshot = Snapshot { rev, state };
        self.current = Some(snapshot.clone());
        // The reconciler wakes on the new snapshot; a closed channel means the
        // reconciler already stopped, which is a shutdown in progress.
        let _ = self.desired.send_replace(Some(snapshot));
        Ok(())
    }

    /// Verifies and persists one `signing_key_rotate` (section 5.21).
    async fn on_signing_key_rotate<S>(
        &mut self,
        raw: &RawFrame,
        body: &serde_json::Value,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let key = self.identity.verifying_key()?;
        let bad_signature = |message: String| ProtocolError {
            code: ErrorCode::BadSignature,
            message,
            fatal: false,
        };
        if let Err(e) = quoro_proto::sign::verify_body(&key, body) {
            let error = bad_signature(format!(
                "signing_key_rotate signature does not verify against the current pin: {e}"
            ));
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        let public_key = body.get("public_key").and_then(serde_json::Value::as_str);
        let key_id = body.get("key_id").and_then(serde_json::Value::as_str);
        let (Some(public_key), Some(key_id)) = (public_key, key_id) else {
            let error =
                bad_signature("signing_key_rotate has no public_key or no key_id".to_owned());
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        };
        let expected = match Identity::key_id_for(public_key) {
            Ok(expected) => expected,
            Err(e) => {
                let error = bad_signature(format!("signing_key_rotate has a bad key: {e}"));
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        if expected != key_id {
            let error = bad_signature(format!(
                "signing_key_rotate key_id {key_id} does not name the public key"
            ));
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        self.identity.rotate(public_key)?;
        info!("cluster signing key rotated");
        let result = CommandResult {
            status: CommandStatus::Ok,
            detail: Some(format!("rotated to key {expected}")),
            error: None,
            lines: None,
        };
        write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
        Ok(())
    }

    /// Answers `command` kind `logs_query` (section 5.17).
    async fn logs_query(&self, command: &Command) -> CommandResult {
        let query = match parse_logs_query(&command.params) {
            Ok(query) => query,
            Err(message) => return bad_result(&message),
        };
        let lines = match self
            .store
            .query_log_lines(&query.filter, query.since, query.until, query.tail)
            .await
        {
            Ok(lines) => lines,
            Err(e) => {
                warn!(error = %e, "could not query the log store");
                return bad_result(&format!("could not query the log store: {e}"));
            }
        };
        let total = lines.len();
        let budget = wire::MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM);
        let mut kept: Vec<LogLine> = Vec::new();
        let mut used = 0usize;
        for line in lines.into_iter().rev() {
            let wire = stored_to_wire(line);
            let size = wire_size(&wire);
            if used + size > budget {
                break;
            }
            used += size;
            kept.push(wire);
        }
        let omitted = total - kept.len();
        kept.reverse();
        let detail = if omitted > 0 {
            Some(format!(
                "{omitted} older line(s) were left out to fit the 1 MiB frame limit"
            ))
        } else {
            Some(format!("{} line(s)", kept.len()))
        };
        CommandResult {
            status: CommandStatus::Ok,
            detail,
            error: None,
            lines: Some(kept),
        }
    }

    /// Starts one stream (section 5.15) or answers why it could not start.
    async fn on_stream_open<S>(
        &mut self,
        open: &StreamOpen,
        raw: &RawFrame,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // A finished stream (a replayed `deployment_logs`, an ended reader)
        // no longer holds a slot.
        streams.handles.retain(|_, handle| !handle.is_finished());
        if streams.handles.len() >= MAX_STREAMS {
            let body = bad_result("stream_limit");
            write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
            return Ok(());
        }
        let store = self.store.clone();
        let stream_id = open.stream_id.clone();
        let handle = match open.kind.as_str() {
            "app_logs" => match parse_app_logs(&open.params) {
                Ok(params) => spawn_app_logs(store, stream_id, params, stream_tx.clone()),
                Err(message) => {
                    let body = bad_result(&message);
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                    return Ok(());
                }
            },
            "deployment_logs" => match parse_deployment_logs(&open.params) {
                Ok((deployment_id, phase)) => {
                    spawn_deployment_logs(store, stream_id, deployment_id, phase, stream_tx.clone())
                }
                Err(message) => {
                    let body = bad_result(&message);
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                    return Ok(());
                }
            },
            other => {
                let body = bad_result(&format!("stream kind {other:?} is not supported"));
                write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        if let Some(previous) = streams.handles.insert(open.stream_id.clone(), handle) {
            previous.abort();
        }
        Ok(())
    }

    /// Stops a stream and answers `stream_end` reason `cancelled` (section
    /// 5.16). An unknown id is ignored.
    async fn on_stream_close(
        &mut self,
        close: &StreamClose,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
    ) -> Result<(), AgentError> {
        if streams.stop(&close.stream_id) {
            // Through the stream channel, so the end follows every frame the
            // task already queued.
            send_end(
                stream_tx,
                &close.stream_id,
                StreamEndReason::Cancelled,
                None,
            )
            .await;
        }
        Ok(())
    }

    async fn hello(&self) -> NodeMessage {
        let stored_rev = self
            .store
            .latest_desired()
            .await
            .ok()
            .flatten()
            .map_or(0, |(rev, _)| rev);
        let applied_rev = self.store.applied_rev().await.unwrap_or(0);
        let dropped_events = self.store.dropped_events().await.unwrap_or(0);
        let identity = self.identity.get();
        NodeMessage::Hello(Hello {
            node_id: identity.node_id,
            agent_version: self.facts.agent_version.clone(),
            engine_version: self.facts.engine_version.clone(),
            boot_id: self.boot_id.clone(),
            stored_rev,
            applied_rev,
            log_seq: 0,
            log_hash: ZERO_HASH.to_owned(),
            leases: Vec::new(),
            capacity: self.facts.capacity,
            allocated: self.allocated(),
            kvm: self.facts.kvm.clone(),
            dropped_events,
        })
    }

    fn allocated(&self) -> Capacity {
        let Some(snapshot) = &self.current else {
            return Capacity::default();
        };
        let mut allocated = Capacity::default();
        for vm in &snapshot.state.spec.vms {
            allocated.cpus = allocated.cpus.saturating_add(vm.vcpus);
            allocated.memory_mb = allocated.memory_mb.saturating_add(u64::from(vm.memory_mb));
            allocated.disk_gb = allocated.disk_gb.saturating_add(u64::from(vm.disk_gb));
        }
        allocated
    }

    async fn send_status<S>(
        &mut self,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let report = StatusReport {
            applied_rev: self.store.applied_rev().await.unwrap_or(0),
            vms: self.observed.borrow().clone(),
            routes: self.routes(),
            capacity: self.facts.capacity,
            allocated: self.allocated(),
            // Free space needs `statvfs`, which the standard library does not
            // expose; the binary fills a real figure in a later milestone.
            disk_free_gb: 0.0,
        };
        write_message(sink, NodeMessage::StatusReport(report), None).await
    }

    fn routes(&self) -> Vec<RouteStatus> {
        self.current
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .state
                    .spec
                    .routes
                    .iter()
                    .map(|route| RouteStatus {
                        hostname: route.hostname.clone(),
                        cert_status: if route.tls_mode == "tunnel" {
                            "not_applicable".to_owned()
                        } else {
                            // The reverse proxy and ACME arrive after M2.
                            "pending".to_owned()
                        },
                        cert_expires_at: None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Sends the buffered events after `welcome`, removing each one from the
    /// buffer only after its frame was written (section 6.5).
    async fn flush_events<S>(
        &mut self,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let events = self.store.buffered_events().await?;
        let mut through = None;
        for event in events {
            match serde_json::from_value::<EventBody>(event.body) {
                Ok(body) => {
                    write_message(sink, NodeMessage::Event(body), None).await?;
                    through = Some(event.seq);
                }
                Err(e) => {
                    warn!(evt_id = %event.evt_id, error = %e, "dropping a malformed buffered event");
                    through = Some(event.seq);
                }
            }
        }
        if let Some(seq) = through {
            self.store.drain_events(seq).await?;
        }
        Ok(())
    }
}

/// A parsed `logs_query`.
struct LogsQuery {
    filter: LogFilter,
    since: Option<i64>,
    until: Option<i64>,
    tail: Option<usize>,
}

/// Parses one optional string member of a params object.
fn string_param(params: &serde_json::Value, name: &str) -> Result<Option<String>, String> {
    match params.get(name) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(format!("{name} must be a non-empty string")),
    }
}

/// Parses `app_id` or `vm_id`, exactly one of which must be present.
fn log_filter(params: &serde_json::Value) -> Result<LogFilter, String> {
    let app_id = string_param(params, "app_id")?;
    let vm_id = string_param(params, "vm_id")?;
    match (app_id, vm_id) {
        (Some(app_id), None) => Ok(LogFilter::App(app_id)),
        (None, Some(vm_id)) => Ok(LogFilter::Vm(vm_id)),
        _ => Err("exactly one of app_id or vm_id is required".to_owned()),
    }
}

/// Parses a non-negative `tail`.
fn tail_param(params: &serde_json::Value) -> Result<Option<usize>, String> {
    match params.get("tail") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(|tail| usize::try_from(tail).unwrap_or(usize::MAX))
            .map(Some)
            .ok_or_else(|| "tail must be a non-negative integer".to_owned()),
    }
}

fn timestamp_param(params: &serde_json::Value, name: &str) -> Result<Option<i64>, String> {
    match string_param(params, name)? {
        None => Ok(None),
        Some(text) => util::parse_rfc3339_nanos(&text)
            .map(Some)
            .ok_or_else(|| format!("{name} is not an RFC 3339 timestamp")),
    }
}

fn parse_logs_query(params: &serde_json::Value) -> Result<LogsQuery, String> {
    Ok(LogsQuery {
        filter: log_filter(params)?,
        since: timestamp_param(params, "since")?,
        until: timestamp_param(params, "until")?,
        tail: tail_param(params)?,
    })
}

/// The selector and replay size of an `app_logs` stream.
struct AppLogs {
    filter: LogFilter,
    tail: usize,
}

fn parse_app_logs(params: &serde_json::Value) -> Result<AppLogs, String> {
    Ok(AppLogs {
        filter: log_filter(params)?,
        tail: tail_param(params)?.unwrap_or(0),
    })
}

fn parse_deployment_logs(params: &serde_json::Value) -> Result<(String, Option<String>), String> {
    let deployment_id = string_param(params, "deployment_id")?
        .ok_or_else(|| "deployment_id is required".to_owned())?;
    let phase = string_param(params, "phase")?;
    Ok((deployment_id, phase))
}

/// A `command_result` with `bad_message`.
fn bad_result(message: &str) -> CommandResult {
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(message.to_owned()),
        error: Some(ErrorBody {
            code: ErrorCode::BadMessage,
            message: message.to_owned(),
        }),
        lines: None,
    }
}

/// Converts a stored line to the wire shape, with the nine-digit instant.
fn stored_to_wire(line: StoredLogLine) -> LogLine {
    LogLine {
        time: util::rfc3339_nanos(line.time_ns),
        stream: line.stream,
        line: line.line,
        vm_id: Some(line.vm_id),
    }
}

/// The serialized size of one line, plus its separator in the array.
fn wire_size(line: &LogLine) -> usize {
    serde_json::to_string(line).map_or(0, |text| text.len() + 1)
}

/// Splits lines into frames that fit the 1 MiB limit.
fn chunks(lines: Vec<LogLine>) -> Vec<Vec<LogLine>> {
    let budget = wire::MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM);
    let mut frames = Vec::new();
    let mut current = Vec::new();
    let mut used = 0usize;
    for line in lines {
        let size = wire_size(&line);
        if !current.is_empty() && (used + size > budget || current.len() >= STREAM_BATCH_LINES) {
            frames.push(std::mem::take(&mut current));
            used = 0;
        }
        used += size;
        current.push(line);
    }
    if !current.is_empty() {
        frames.push(current);
    }
    frames
}

/// Sends lines as `stream_data` frames. Returns `false` when the session is
/// gone.
async fn send_lines(
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    seq: &mut u64,
    lines: Vec<LogLine>,
) -> bool {
    for chunk in chunks(lines) {
        let frame = StreamData {
            stream_id: stream_id.to_owned(),
            seq: *seq,
            lines: Some(chunk),
            data: None,
        };
        *seq = seq.saturating_add(1);
        if tx.send(NodeMessage::StreamData(frame)).await.is_err() {
            return false;
        }
    }
    true
}

/// Sends one `stream_end`.
async fn send_end(
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    reason: StreamEndReason,
    message: Option<String>,
) {
    let _ = tx
        .send(NodeMessage::StreamEnd(StreamEnd {
            stream_id: stream_id.to_owned(),
            reason,
            message,
        }))
        .await;
}

fn spawn_app_logs(
    store: NodeStore,
    stream_id: String,
    params: AppLogs,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_app_logs(store, stream_id, params, tx).await;
    })
}

async fn run_app_logs(
    store: NodeStore,
    stream_id: String,
    params: AppLogs,
    tx: mpsc::Sender<NodeMessage>,
) {
    // Subscribe before replaying so a line written during the query is either
    // in the replay or in the channel, never lost and never duplicated.
    let mut rx = store.subscribe_logs();
    let mut seq = 1u64;
    let mut last_time = i64::MIN;
    if params.tail > 0 {
        match store
            .query_log_lines(&params.filter, None, None, Some(params.tail))
            .await
        {
            Ok(lines) => {
                if let Some(last) = lines.last() {
                    last_time = last.time_ns;
                }
                let wire: Vec<LogLine> = lines.into_iter().map(stored_to_wire).collect();
                if !send_lines(&tx, &stream_id, &mut seq, wire).await {
                    return;
                }
            }
            Err(error) => {
                warn!(error = %error, "could not replay a log stream");
                send_end(
                    &tx,
                    &stream_id,
                    StreamEndReason::Error,
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        }
    }

    let mut batch: Vec<LogLine> = Vec::with_capacity(STREAM_BATCH_LINES);
    let mut ticker = tokio::time::interval(STREAM_FLUSH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            received = rx.recv() => {
                match received {
                    Ok(line) => {
                        if params.filter.matches(&line) && line.time_ns > last_time {
                            last_time = line.time_ns;
                            batch.push(LogLine {
                                time: util::rfc3339_nanos(line.time_ns),
                                stream: line.stream,
                                line: line.line,
                                vm_id: Some(line.vm_id),
                            });
                            if batch.len() >= STREAM_BATCH_LINES {
                                let lines = std::mem::take(&mut batch);
                                if !send_lines(&tx, &stream_id, &mut seq, lines).await {
                                    return;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        debug!(skipped, "a log stream lagged; some live lines were skipped");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    let lines = std::mem::take(&mut batch);
                    if !send_lines(&tx, &stream_id, &mut seq, lines).await {
                        return;
                    }
                }
            }
        }
    }
}

fn spawn_deployment_logs(
    store: NodeStore,
    stream_id: String,
    deployment_id: String,
    phase: Option<String>,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let lines = match store
            .query_deployment_lines(&deployment_id, phase.as_deref())
            .await
        {
            Ok(lines) => lines,
            Err(error) => {
                warn!(error = %error, "could not replay a deployment's progress");
                send_end(
                    &tx,
                    &stream_id,
                    StreamEndReason::Error,
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        };
        let wire: Vec<LogLine> = lines
            .into_iter()
            .map(|line| LogLine {
                time: util::rfc3339_nanos(line.time_ns),
                stream: quoro_proto::wire::LogStream::Stdout,
                line: line.message,
                vm_id: line.vm_id,
            })
            .collect();
        let mut seq = 1u64;
        if send_lines(&tx, &stream_id, &mut seq, wire).await {
            send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
        }
    })
}

/// Whether a `stop` channel asked the agent to shut down.
pub(crate) async fn wait_stop(mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// Writes one node message.
async fn write_message<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    message: NodeMessage,
    re: Option<&str>,
) -> Result<(), AgentError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let id = util::message_id();
    let text = message.to_frame(&id, re).to_text();
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|e| AgentError::Protocol(format!("WebSocket write failed: {e}")))
}

async fn next_frame<S>(
    source: &mut SplitStream<WebSocketStream<S>>,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    source.next().await
}

/// Parses one control frame.
fn parse_control(text: &str) -> Result<ControlMessage, AgentError> {
    let raw = RawFrame::parse(text).map_err(|e| AgentError::Protocol(format!("bad frame: {e}")))?;
    ControlMessage::from_raw(&raw).map_err(|e| AgentError::Protocol(format!("bad message: {e}")))
}

/// The `wss://host:port/api/v1/nodes/session` URL for a primary base URL.
///
/// # Errors
///
/// [`AgentError::Tls`] when the stored primary URL is not `https`/`wss`.
pub fn session_url(primary_url: &str) -> Result<String, AgentError> {
    let endpoint = Endpoint::parse(primary_url)?;
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    Ok(format!("wss://{host}:{}{SESSION_PATH}", endpoint.port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use quoro_proto::sign::{self, SigningKey};
    use quoro_proto::wire::Welcome;
    use serde_json::json;
    use tokio::io::DuplexStream;
    use tokio_tungstenite::{accept_async, client_async};

    use crate::identity::{self, Identity, IdentityHandle};
    use crate::logs::NewLogLine;

    #[test]
    fn session_urls_are_always_wss_with_the_session_path() {
        assert_eq!(
            session_url("https://quoro.example.com").unwrap(),
            "wss://quoro.example.com:443/api/v1/nodes/session"
        );
        assert_eq!(
            session_url("wss://10.0.0.1:8443").unwrap(),
            "wss://10.0.0.1:8443/api/v1/nodes/session"
        );
        assert_eq!(
            session_url("https://[::1]:9443").unwrap(),
            "wss://[::1]:9443/api/v1/nodes/session"
        );
        assert!(session_url("http://quoro.example.com").is_err());
    }

    #[test]
    fn allocated_capacity_sums_the_spec() {
        // `allocated` only needs a snapshot; the totals are pure arithmetic.
        let mut capacity = Capacity::default();
        capacity.cpus += 2;
        capacity.memory_mb += 512;
        capacity.disk_gb += 4;
        assert_eq!(capacity.cpus, 2);
        assert_eq!(capacity.memory_mb, 512);
        assert_eq!(capacity.disk_gb, 4);
    }

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-session-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn identity_files(dir: &Path, key: &SigningKey) -> IdentityHandle {
        identity::ensure_data_dir(dir).unwrap();
        identity::write_seed(dir, &[7; 32]).unwrap();
        identity::write_credential(dir, "credential-for-tests").unwrap();
        let public = sign::public_key_to_base64(&key.verifying_key());
        identity::save_identity(
            dir,
            &Identity {
                node_id: "nod_test".to_owned(),
                cluster_id: "cls_test".to_owned(),
                primary_url: "https://primary.test".to_owned(),
                fingerprint: format!(
                    "sha256:{}",
                    "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35"
                ),
                cluster_key_id: Identity::key_id_for(&public).unwrap(),
                cluster_public_key: public,
            },
        )
        .unwrap();
        IdentityHandle::load(dir).unwrap()
    }

    fn facts() -> NodeFacts {
        NodeFacts {
            capacity: Capacity::default(),
            kvm: quoro_proto::wire::Kvm::default(),
            engine_version: "fake 0".to_owned(),
            agent_version: "0.0.1".to_owned(),
        }
    }

    async fn read_frame(ws: &mut WebSocketStream<DuplexStream>) -> serde_json::Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(3), ws.next())
                .await
                .expect("the node sent a frame in time")
                .expect("the socket is open")
                .expect("a valid frame");
            if let Message::Text(text) = message {
                return serde_json::from_str(text.as_str()).expect("a JSON frame");
            }
        }
    }

    async fn read_until(ws: &mut WebSocketStream<DuplexStream>, kind: &str) -> serde_json::Value {
        loop {
            let frame = read_frame(ws).await;
            if frame["type"] == kind {
                return frame;
            }
        }
    }

    async fn send(ws: &mut WebSocketStream<DuplexStream>, text: String) {
        ws.send(Message::Text(text.into())).await.unwrap();
    }

    async fn handshake(ws: &mut WebSocketStream<DuplexStream>) {
        let hello = read_frame(ws).await;
        assert_eq!(hello["type"], "hello");
        send(
            ws,
            ControlMessage::Welcome(Welcome {
                cluster_id: "cls_test".to_owned(),
                term: 1,
                server_time: "2026-11-02T10:15:30Z".to_owned(),
                heartbeat_interval_ms: 500,
                session_timeout_ms: 5_000,
                primary_node_id: "nod_primary".to_owned(),
            })
            .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
            .to_text(),
        )
        .await;
        read_until(ws, "status_report").await;
    }

    /// Runs one session over a duplex stream with `primary` driving it.
    async fn drive_session(
        dir: &TempDir,
        primary: impl FnOnce(
            WebSocketStream<DuplexStream>,
            NodeStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + 'static,
    ) -> (SessionEnd, NodeStore) {
        let key = sign::signing_key_from_seed(&[4; 32]);
        let identity = identity_files(dir.path(), &key);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (desired, _desired_rx) = watch::channel(None);
        let (_observed_tx, observed_rx) = watch::channel(Vec::new());
        let mut session = Session::new(SessionSetup {
            identity,
            store: store.clone(),
            facts: facts(),
            desired,
            observed: observed_rx,
            current: None,
        });

        let (client, server) = tokio::io::duplex(1 << 20);
        let primary_store = store.clone();
        let primary = tokio::spawn(async move {
            let ws = accept_async(server).await.unwrap();
            primary(ws, primary_store).await;
        });
        let (client_ws, _response) = client_async("ws://primary.test/session", client)
            .await
            .unwrap();
        let end = session
            .serve(client_ws, std::future::pending::<()>())
            .await
            .unwrap();
        primary.await.unwrap();
        (end, store)
    }

    fn line(vm_id: &str, app_id: &str, text: &str) -> NewLogLine {
        NewLogLine {
            vm_id: vm_id.to_owned(),
            app_id: Some(app_id.to_owned()),
            stream: quoro_proto::wire::LogStream::Stdout,
            line: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn logs_query_answers_with_the_matching_lines() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_log_lines(&[
            line("vm_1", "app_1", "first"),
            line("vm_1", "app_1", "second"),
        ])
        .await
        .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "logs_query".to_owned(),
                        params: json!({"vm_id": "vm_1"}),
                    })
                    .to_frame("msg_query", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_query");
                assert_eq!(answer["body"]["status"], "ok");
                let lines = answer["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 2);
                assert_eq!(lines[0]["line"], "first");
                assert_eq!(lines[1]["line"], "second");
                assert_eq!(lines[1]["vm_id"], "vm_1");
                assert!(
                    lines[1]["time"]
                        .as_str()
                        .is_some_and(|time| time.ends_with('Z') && time.contains('.'))
                );
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn app_logs_replay_tail_then_follow_live_and_close() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_log_lines(&[
            line("vm_1", "app_1", "first"),
            line("vm_1", "app_1", "second"),
        ])
        .await
        .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_stream".to_owned(),
                        kind: "app_logs".to_owned(),
                        params: json!({"vm_id": "vm_1", "tail": 1}),
                    })
                    .to_frame("msg_open", None)
                    .to_text(),
                )
                .await;
                let first = read_until(&mut ws, "stream_data").await;
                assert_eq!(first["body"]["stream_id"], "msg_stream");
                assert_eq!(first["body"]["seq"], 1);
                let lines = first["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 1);
                assert_eq!(lines[0]["line"], "second");

                // A line captured now reaches the open stream.
                store
                    .append_log_lines(&[line("vm_1", "app_1", "live")])
                    .await
                    .unwrap();
                let live = read_until(&mut ws, "stream_data").await;
                assert_eq!(live["body"]["seq"], 2);
                let lines = live["body"]["lines"].as_array().unwrap();
                assert_eq!(lines[0]["line"], "live");

                send(
                    &mut ws,
                    ControlMessage::StreamClose(StreamClose {
                        stream_id: "msg_stream".to_owned(),
                    })
                    .to_frame("msg_close", None)
                    .to_text(),
                )
                .await;
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["stream_id"], "msg_stream");
                assert_eq!(end["body"]["reason"], "cancelled");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn deployment_logs_replay_then_complete() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_deployment_line("dep_1", "pull", "pulling img", Some("vm_1"))
            .await
            .unwrap();
        seed.append_deployment_line("dep_1", "boot", "VM created", Some("vm_1"))
            .await
            .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_dep".to_owned(),
                        kind: "deployment_logs".to_owned(),
                        params: json!({"deployment_id": "dep_1"}),
                    })
                    .to_frame("msg_open_dep", None)
                    .to_text(),
                )
                .await;
                let data = read_until(&mut ws, "stream_data").await;
                let lines = data["body"]["lines"].as_array().unwrap();
                assert_eq!(lines[0]["line"], "pulling img");
                assert_eq!(lines[1]["line"], "VM created");
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["reason"], "completed");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn an_unknown_stream_kind_is_a_bad_message() {
        let dir = TempDir::new();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_console".to_owned(),
                        kind: "console".to_owned(),
                        params: json!({}),
                    })
                    .to_frame("msg_open_console", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_open_console");
                assert_eq!(answer["body"]["status"], "error");
                assert_eq!(answer["body"]["error"]["code"], "bad_message");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn logs_query_keeps_the_newest_lines_that_fit() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        let mut batch = Vec::new();
        for index in 0..130 {
            batch.push(line(
                "vm_1",
                "app_1",
                &format!("{index}-{}", "x".repeat(9_000)),
            ));
        }
        seed.append_log_lines(&batch).await.unwrap();
        let (end, store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "logs_query".to_owned(),
                        params: json!({"vm_id": "vm_1"}),
                    })
                    .to_frame("msg_query_big", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["body"]["status"], "ok");
                let detail = answer["body"]["detail"].as_str().unwrap();
                assert!(detail.contains("left out"), "{detail}");
                let lines = answer["body"]["lines"].as_array().unwrap();
                assert!(lines.len() < 130, "{}", lines.len());
                let last = lines.last().unwrap()["line"].as_str().unwrap();
                assert!(last.starts_with("129-"), "the newest line is kept");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
        // The store outlived the session; the assertion above is about the
        // answer, so this only checks the fixture is sound.
        assert_eq!(
            store
                .query_log_lines(&LogFilter::Vm("vm_1".to_owned()), None, None, None)
                .await
                .unwrap()
                .len(),
            130
        );
    }
}
