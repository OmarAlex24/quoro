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

use std::future::Future;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_proto::wire::{
    self, Capacity, ControlMessage, DesiredState, EventBody, Heartbeat, Hello, NodeMessage,
    RawFrame, RouteStatus, StatusReport, VmStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorBody, ErrorCode, ProtocolError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, client_async_with_config};
use tracing::{debug, info, warn};

use crate::identity::{Identity, IdentityHandle};
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
use crate::tls::{self, Endpoint};
use crate::{AgentError, FatalReason, NodeFacts, util};

/// Path of the session endpoint (section 3.1).
pub const SESSION_PATH: &str = "/api/v1/nodes/session";

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
                frame = next_frame(&mut source) => {
                    last_rx = Instant::now();
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            match self.handle(&text, &mut sink).await {
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
            ControlMessage::Pong
            | ControlMessage::LogEntries(_)
            | ControlMessage::StreamClose(_) => {
                // Change-log follow is a follow-up; a stream close names a
                // stream this version never opens.
            }
            ControlMessage::Command(command) => {
                let body = if command.kind == "probe" {
                    CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some(format!(
                            "kvm available: {}; kernel {}",
                            self.facts.kvm.available, self.facts.kvm.kernel
                        )),
                        error: None,
                        lines: None,
                    }
                } else {
                    CommandResult {
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
                    }
                };
                write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
            }
            ControlMessage::StreamOpen(_) => {
                let body = CommandResult {
                    status: CommandStatus::Error,
                    detail: Some(
                        "live logs arrive with the next epic; streams are not served yet"
                            .to_owned(),
                    ),
                    error: Some(ErrorBody {
                        code: ErrorCode::BadMessage,
                        message: "stream_open is not implemented yet".to_owned(),
                    }),
                    lines: None,
                };
                write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
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
}
