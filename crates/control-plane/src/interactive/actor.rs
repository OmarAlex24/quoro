// SPDX-License-Identifier: AGPL-3.0-only
//! The one task that owns an open interactive session: it relays the node
//! stream to the WebSocket and back, acknowledges credit, re-checks access and
//! writes the `session.end` record exactly once (`permission-model.md` section
//! 7.5, `node-agent-protocol.md` sections 5.7, 5.8, 5.24 and 5.25).

use super::{
    ATTACH_WAIT, CloseReason, Command, EndInfo, Interactive, MAX_CLIENT_BYTES, SessionRow,
    SocketSlot, Timings, finalize,
};
use crate::AppState;
use crate::auth::{Credential, Identity};
use crate::error::Target;
use crate::session::{NodeStream, SessionEndReason, StreamEvent};
use crate::store::now_rfc3339;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use quoro_proto::wire::WindowSize;
use quoro_types::{Role, Scope};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// What ends a session: the reason, the details a close needs, and an
/// optional DELETE reply.
struct Ending {
    reason: CloseReason,
    exit_code: Option<i32>,
    transcript: Option<(String, u64)>,
    ended_by: Option<String>,
    justification: Option<String>,
    request_id: Option<String>,
    message: String,
    reply: Option<oneshot::Sender<bool>>,
}

impl Ending {
    fn new(reason: CloseReason) -> Self {
        Self {
            reason,
            exit_code: None,
            transcript: None,
            ended_by: None,
            justification: None,
            request_id: None,
            message: String::new(),
            reply: None,
        }
    }

    fn client(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::new(CloseReason::EndedByClient)
        }
    }
}

/// Starts the session actor. The caller has already registered its attach
/// point and minted the ticket.
#[allow(clippy::too_many_arguments)]
pub async fn spawn(
    state: AppState,
    row: SessionRow,
    identity: Identity,
    interactive: Interactive,
    stream: NodeStream,
    buffered: Vec<Vec<u8>>,
    commands: mpsc::UnboundedReceiver<Command>,
    socket: Arc<SocketSlot>,
) {
    let timings = interactive.timings();
    let remaining = credential_remaining(&state, &identity).await;
    let credential_deadline = remaining.map(|left| Instant::now() + left);
    let actor = Actor {
        state,
        row,
        identity,
        interactive,
        stream,
        commands,
        socket,
        buffered: buffered.into(),
        timings,
        now: Instant::now(),
        ticket_deadline: Instant::now() + timings.ticket_ttl,
        max_deadline: Instant::now() + timings.max_duration,
        credential_deadline,
        last_recheck: Instant::now(),
        connected: false,
        bytes_in: 0,
        bytes_out: 0,
        sent: 0,
        acked: 0,
        input_seq: 0,
        ended: false,
    };
    tokio::spawn(async move { actor.run().await });
}

struct Actor {
    state: AppState,
    row: SessionRow,
    identity: Identity,
    interactive: Interactive,
    stream: NodeStream,
    commands: mpsc::UnboundedReceiver<Command>,
    /// The registry's claim on the session's one WebSocket.
    socket: Arc<SocketSlot>,
    buffered: VecDeque<Vec<u8>>,
    timings: Timings,
    now: Instant,
    ticket_deadline: Instant,
    max_deadline: Instant,
    credential_deadline: Option<Instant>,
    last_recheck: Instant,
    connected: bool,
    bytes_in: u64,
    bytes_out: u64,
    /// Bytes handed to the WebSocket, cumulative.
    sent: u64,
    /// Last `acked_bytes` sent to the node.
    acked: u64,
    /// `stream_input` sequence, gap-free from 1.
    input_seq: u64,
    ended: bool,
}

impl Actor {
    async fn run(mut self) {
        // Phase 1: the session exists but no WebSocket attached yet. Data that
        // arrives is buffered (never acked) until it is handed to the socket.
        let mut ticket_grace_used = false;
        let socket = loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(Command::Attach(socket)) => break *socket,
                    Some(Command::End { request_id, ended_by, justification, reply }) => {
                        let ending = Ending {
                            reason: CloseReason::EndedByRequest,
                            ended_by: Some(ended_by),
                            justification,
                            request_id: Some(request_id),
                            reply: Some(reply),
                            ..Ending::new(CloseReason::EndedByRequest)
                        };
                        self.end_before_attach(ending).await;
                        return;
                    }
                    None => return,
                },
                event = self.stream.next() => {
                    match event {
                        None => {
                            self.end_before_attach(Ending::new(CloseReason::NodeDisconnected))
                                .await;
                            return;
                        }
                        Some(StreamEvent::Data(bytes)) => self.buffered.push_back(bytes),
                        Some(StreamEvent::SessionEnd {
                            reason,
                            message,
                            exit_code,
                            transcript_sha256,
                            transcript_bytes,
                            ..
                        }) => {
                            let mut ending = node_ending(reason, message, exit_code);
                            ending.transcript = transcript_sha256.map(|sha| {
                                (sha, transcript_bytes.unwrap_or(0))
                            });
                            self.end_before_attach(ending).await;
                            return;
                        }
                        Some(_) => {}
                    }
                },
                () = tokio::time::sleep_until(self.ticket_deadline) => {
                    // An upgrade that claimed the session presented the ticket
                    // in time: its socket is on the way, so the session goes on.
                    if self.socket.seal() || ticket_grace_used {
                        self.close(Ending::new(CloseReason::TicketExpired), None).await;
                        return;
                    }
                    ticket_grace_used = true;
                    self.ticket_deadline = Instant::now() + ATTACH_WAIT;
                }
            }
        };
        let mut socket = socket;
        self.attach().await;
        let ending = self.connected_loop(&mut socket).await;
        self.close(ending, Some(socket)).await;
    }

    /// Ends a session whose WebSocket has not reached the actor. An upgrade
    /// claims the session before its `101` and only then hands the socket
    /// over, so the session can end (the node drops, the shell exits, a
    /// DELETE lands) between the two. Dropping that socket unanswered would
    /// reset the client's connection without telling it why: wait, bounded,
    /// for the claimed socket and close through it instead. A session no
    /// upgrade claimed is sealed, so a later upgrade is refused.
    async fn end_before_attach(&mut self, ending: Ending) {
        if self.socket.seal() {
            self.close(ending, None).await;
            return;
        }
        let deadline = Instant::now() + ATTACH_WAIT;
        loop {
            match tokio::time::timeout_at(deadline, self.commands.recv()).await {
                Ok(Some(Command::Attach(socket))) => {
                    let mut socket = *socket;
                    self.attach().await;
                    // What the node produced before it ended still reaches
                    // the client, ahead of the close.
                    while let Some(bytes) = self.buffered.pop_front() {
                        let len = bytes.len();
                        if socket.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                        self.delivered(len);
                    }
                    self.close(ending, Some(socket)).await;
                    return;
                }
                // This ending already writes the session's one record.
                Ok(Some(Command::End { reply, .. })) => {
                    let _ = reply.send(false);
                }
                Ok(None) | Err(_) => {
                    self.close(ending, None).await;
                    return;
                }
            }
        }
    }

    /// Marks the session connected and records it on the row.
    async fn attach(&mut self) {
        self.connected = true;
        self.now = Instant::now();
        // Through the writer gate like every write (one writer): a write beside
        // it breaks another transaction's snapshot (SQLITE_BUSY_SNAPSHOT).
        let recorded = async {
            let mut w = self.state.store.begin_write().await?;
            sqlx::query(
                "UPDATE interactive_sessions SET connected_at = ? WHERE id = ? AND ended_at IS NULL",
            )
            .bind(now_rfc3339())
            .bind(&self.row.id)
            .execute(&mut *w.tx)
            .await?;
            w.commit().await
        }
        .await;
        if let Err(error) = recorded {
            tracing::warn!(%error, session = %self.row.id, "cannot record a session's connect time");
        }
        // The first data may already be buffered; the loop below sends it.
    }

    async fn connected_loop(&mut self, socket: &mut WebSocket) -> Ending {
        // An absolute deadline: a busy stream never starves the timers.
        let mut next_check = Instant::now() + self.timings.tick;
        loop {
            // Credit: every byte handed to the socket is acknowledged, at the
            // latest every 64 KiB, and once the buffer drains.
            if self.sent > self.acked {
                let _ = self.stream.send_ack(self.sent);
                self.acked = self.sent;
            }
            if let Some(bytes) = self.buffered.pop_front() {
                if socket
                    .send(Message::Binary(bytes.clone().into()))
                    .await
                    .is_err()
                {
                    return Ending::client("the client went away");
                }
                self.delivered(bytes.len());
                continue;
            }

            tokio::select! {
                message = socket.recv() => match message {
                    None | Some(Err(_)) => return Ending::client("the client went away"),
                    Some(Ok(Message::Close(_))) => return Ending::client("the client closed"),
                    Some(Ok(Message::Binary(bytes))) => {
                        self.touch();
                        if let Some(ending) = self.on_binary(socket, bytes.to_vec()).await {
                            return ending;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        self.touch();
                        if let Some(ending) = self.on_control(socket, text.as_str()).await {
                            return ending;
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                },
                event = self.stream.next() => match event {
                    None => return Ending::new(CloseReason::NodeDisconnected),
                    Some(StreamEvent::Data(bytes)) => {
                        if socket.send(Message::Binary(bytes.clone().into())).await.is_err() {
                            return Ending::client("the client went away");
                        }
                        self.delivered(bytes.len());
                    }
                    Some(StreamEvent::SessionEnd {
                        reason,
                        message,
                        exit_code,
                        transcript_sha256,
                        transcript_bytes,
                        ..
                    }) => {
                        let mut ending = node_ending(reason, message, exit_code);
                        ending.transcript =
                            transcript_sha256.map(|sha| (sha, transcript_bytes.unwrap_or(0)));
                        return ending;
                    }
                    Some(_) => {}
                },
                command = self.commands.recv() => match command {
                    Some(Command::End { request_id, ended_by, justification, reply }) => {
                        return Ending {
                            reason: CloseReason::EndedByRequest,
                            ended_by: Some(ended_by),
                            justification,
                            request_id: Some(request_id),
                            reply: Some(reply),
                            ..Ending::new(CloseReason::EndedByRequest)
                        };
                    }
                    Some(Command::Attach(_)) => {}
                    None => return Ending::new(CloseReason::NodeDisconnected),
                },
                () = tokio::time::sleep_until(next_check) => {
                    next_check = Instant::now() + self.timings.tick;
                    if let Some(ending) = self.check_deadlines().await {
                        return ending;
                    }
                }
            }
        }
    }

    fn delivered(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.bytes_out += bytes;
        self.sent += bytes;
    }

    fn touch(&mut self) {
        self.now = Instant::now();
    }

    /// One client binary frame becomes one `stream_input`.
    async fn on_binary(&mut self, socket: &mut WebSocket, bytes: Vec<u8>) -> Option<Ending> {
        if self.row.kind != "terminal" {
            send_error(socket, "the console is read-only").await;
            return None;
        }
        if bytes.len() > MAX_CLIENT_BYTES {
            send_error(socket, "a message may carry at most 64 KiB").await;
            return None;
        }
        self.input_seq += 1;
        if !self
            .stream
            .send_input(self.input_seq, Some(&bytes), None, false)
        {
            return Some(Ending::new(CloseReason::NodeDisconnected));
        }
        self.bytes_in += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        None
    }

    /// One client text frame is one JSON control object.
    async fn on_control(&mut self, socket: &mut WebSocket, text: &str) -> Option<Ending> {
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            send_error(socket, "a control message must be JSON").await;
            return None;
        };
        match value["type"].as_str() {
            Some("resize") => {
                let rows = value["rows"].as_i64();
                let cols = value["cols"].as_i64();
                let valid = |value: Option<i64>| value.is_some_and(|v| (1..=1000).contains(&v));
                if !valid(rows) || !valid(cols) {
                    send_error(socket, "rows and cols must be 1-1000").await;
                    return None;
                }
                if self.row.kind != "terminal" {
                    send_error(socket, "the console is read-only").await;
                    return None;
                }
                let resize = WindowSize {
                    rows: u16::try_from(rows.unwrap_or(1)).unwrap_or(1),
                    cols: u16::try_from(cols.unwrap_or(1)).unwrap_or(1),
                };
                self.input_seq += 1;
                if !self
                    .stream
                    .send_input(self.input_seq, None, Some(resize), false)
                {
                    return Some(Ending::new(CloseReason::NodeDisconnected));
                }
                None
            }
            _ => {
                send_error(socket, "unknown control type").await;
                None
            }
        }
    }

    async fn check_deadlines(&mut self) -> Option<Ending> {
        let now = Instant::now();
        if now >= self.max_deadline {
            return Some(Ending::new(CloseReason::MaxDuration));
        }
        if now.duration_since(self.now) >= self.timings.idle {
            return Some(Ending::new(CloseReason::IdleTimeout));
        }
        if self
            .credential_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            return Some(Ending::new(CloseReason::CredentialExpired));
        }
        if now.duration_since(self.last_recheck) >= self.timings.recheck {
            self.last_recheck = now;
            if let Some(reason) = self.recheck().await {
                return Some(Ending::new(reason));
            }
        }
        None
    }

    /// Section 7.5: the opener's access and credential, re-read without
    /// consuming a grant use.
    async fn recheck(&mut self) -> Option<CloseReason> {
        let scope = Scope::parse(&self.row.scope)?;
        let target = Target {
            kind: self.row.target_type.clone(),
            id: self.row.target_id.clone(),
        };
        let mut conn = match self.state.store.pool().acquire().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(%error, session = %self.row.id, "cannot re-check a session");
                return None;
            }
        };
        let now = now_rfc3339();
        match &self.identity.principal {
            crate::auth::Principal::User(user) => {
                let row: Result<Option<(i64, String)>, sqlx::Error> =
                    sqlx::query_as("SELECT disabled, role FROM users WHERE id = ?")
                        .bind(&self.row.actor_id)
                        .fetch_optional(&mut *conn)
                        .await;
                match row {
                    Ok(None) => return Some(CloseReason::AccessRevoked),
                    Ok(Some((disabled, role))) => {
                        if disabled != 0 || Role::parse(&role).is_none_or(|role| !role.holds(scope))
                        {
                            return Some(CloseReason::AccessRevoked);
                        }
                    }
                    Err(_) => return None,
                }
                let _ = user;
            }
            crate::auth::Principal::Agent(agent) => {
                let disabled: Result<Option<i64>, sqlx::Error> =
                    sqlx::query_scalar("SELECT disabled FROM agents WHERE id = ?")
                        .bind(&self.row.actor_id)
                        .fetch_optional(&mut *conn)
                        .await;
                match disabled {
                    Ok(Some(0)) => {}
                    _ => return Some(CloseReason::AccessRevoked),
                }
                let mut agent = agent.clone();
                agent.disabled = false;
                if crate::authz::decide(&mut conn, &agent, scope, Some(&target), &json!({}))
                    .await
                    .is_err()
                {
                    return Some(CloseReason::AccessRevoked);
                }
            }
        }
        let alive: Result<Option<i64>, sqlx::Error> = match &self.identity.credential {
            Credential::Session { session_id } => {
                sqlx::query_scalar("SELECT 1 FROM sessions WHERE id = ? AND expires_at > ?")
                    .bind(session_id)
                    .bind(&now)
                    .fetch_optional(&mut *conn)
                    .await
            }
            Credential::ApiToken { token_id } => {
                sqlx::query_scalar("SELECT 1 FROM api_tokens WHERE id = ? AND revoked_at IS NULL")
                    .bind(token_id)
                    .fetch_optional(&mut *conn)
                    .await
            }
            Credential::AccessToken { token_id } => {
                sqlx::query_scalar("SELECT 1 FROM access_tokens WHERE id = ? AND expires_at > ?")
                    .bind(token_id)
                    .bind(&now)
                    .fetch_optional(&mut *conn)
                    .await
            }
        };
        match alive {
            Ok(Some(_)) => None,
            Ok(None) => Some(CloseReason::CredentialExpired),
            Err(_) => None,
        }
    }

    /// Section 5.8: a primary-side close of a live terminal asks the node to
    /// stop the stream and waits, bounded, for the answering `stream_end` that
    /// carries the transcript fields, so `session.end` can record them
    /// whatever ended the session. Data that arrives meanwhile is discarded.
    async fn request_transcript(&mut self) -> Option<(String, u64)> {
        if self.stream.is_ended() || !self.stream.request_close() {
            return None;
        }
        let deadline = Instant::now() + self.timings.stream_close_wait;
        loop {
            match tokio::time::timeout_at(deadline, self.stream.next()).await {
                Ok(Some(StreamEvent::SessionEnd {
                    transcript_sha256,
                    transcript_bytes,
                    ..
                })) => {
                    return transcript_sha256.map(|sha| (sha, transcript_bytes.unwrap_or(0)));
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return None,
            }
        }
    }

    /// Writes the one `session.end`, tells the client why, and forgets the
    /// session. Returns whether the record was written (false when the session
    /// had already ended).
    async fn close(&mut self, ending: Ending, mut socket: Option<WebSocket>) -> bool {
        if self.ended {
            if let Some(reply) = ending.reply {
                let _ = reply.send(false);
            }
            return false;
        }
        self.ended = true;
        let transcript = if self.row.kind == "terminal" && self.connected {
            match ending.transcript {
                Some(transcript) => Some(transcript),
                None => self.request_transcript().await,
            }
        } else {
            None
        };
        let info = EndInfo {
            reason: ending.reason,
            exit_code: ending.exit_code,
            transcript_sha256: transcript.as_ref().map(|(sha, _)| sha.clone()),
            transcript_bytes: transcript.as_ref().map(|(_, bytes)| *bytes),
            connected: self.connected,
            bytes_in: self.bytes_in,
            bytes_out: self.bytes_out,
            ended_by: ending.ended_by,
            justification: ending.justification,
            request_id: ending
                .request_id
                .unwrap_or_else(|| quoro_types::new_id(quoro_types::Prefix::Request)),
        };
        let wrote = match finalize(&self.state, &self.row, info).await {
            Ok(wrote) => wrote,
            Err(error) => {
                tracing::error!(code = %error.code.as_str(), session = %self.row.id, "cannot write session.end");
                false
            }
        };
        if let Some(reply) = ending.reply {
            let _ = reply.send(wrote);
        }
        if let Some(socket) = socket.as_mut() {
            if let Some(code) = ending.exit_code {
                let exit = json!({ "type": "exit", "code": code });
                let _ = socket.send(Message::Text(exit.to_string().into())).await;
            }
            if ending.reason == CloseReason::Error {
                let message = if ending.message.is_empty() {
                    "the session failed".to_string()
                } else {
                    ending.message.clone()
                };
                send_error_code(socket, "internal", &message).await;
            }
            let closed = json!({ "type": "closed", "reason": ending.reason.as_str() });
            let _ = socket.send(Message::Text(closed.to_string().into())).await;
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "".into(),
                })))
                .await;
        }
        self.interactive.unregister(&self.row.id);
        wrote
    }
}

fn node_ending(reason: SessionEndReason, message: String, exit_code: Option<i32>) -> Ending {
    let close = match reason {
        SessionEndReason::Completed => CloseReason::Exited,
        SessionEndReason::VmStopped => CloseReason::VmStopped,
        SessionEndReason::Limit => CloseReason::TranscriptLimit,
        SessionEndReason::Lost => CloseReason::NodeDisconnected,
        SessionEndReason::Error | SessionEndReason::Cancelled => CloseReason::Error,
    };
    Ending {
        exit_code,
        message,
        ..Ending::new(close)
    }
}

async fn send_error(socket: &mut WebSocket, message: &str) {
    send_error_code(socket, "validation_failed", message).await;
}

async fn send_error_code(socket: &mut WebSocket, code: &str, message: &str) {
    let error = json!({ "type": "error", "code": code, "message": message });
    let _ = socket.send(Message::Text(error.to_string().into())).await;
}

/// How long the credential that opened the session may still be used.
async fn credential_remaining(state: &AppState, identity: &Identity) -> Option<Duration> {
    let expires_at: Option<String> = match &identity.credential {
        Credential::Session { session_id } => {
            sqlx::query_scalar("SELECT expires_at FROM sessions WHERE id = ?")
                .bind(session_id)
                .fetch_optional(state.store.pool())
                .await
                .ok()?
        }
        Credential::ApiToken { token_id } => {
            let row: Option<Option<String>> =
                sqlx::query_scalar("SELECT expires_at FROM api_tokens WHERE id = ?")
                    .bind(token_id)
                    .fetch_optional(state.store.pool())
                    .await
                    .ok()?;
            match row {
                Some(Some(value)) => Some(value),
                // A token that never expires has no deadline.
                _ => return None,
            }
        }
        Credential::AccessToken { token_id } => {
            sqlx::query_scalar("SELECT expires_at FROM access_tokens WHERE id = ?")
                .bind(token_id)
                .fetch_optional(state.store.pool())
                .await
                .ok()?
        }
    };
    let expires_at = expires_at?;
    let expires_at =
        time::OffsetDateTime::parse(&expires_at, &time::format_description::well_known::Rfc3339)
            .ok()?;
    let delta = expires_at - time::OffsetDateTime::now_utc();
    Some(Duration::try_from(delta).unwrap_or_default())
}
