// SPDX-License-Identifier: AGPL-3.0-only
//! Interactive sessions: the web terminal and the read-only VM console
//! (ADR 0113-0116, `permission-model.md` sections 7.5 and 7.6,
//! `audit-log-format.md` section 3.3).
//!
//! The open call authorizes once and opens the stream on the node; the answer
//! is a single-use ticket valid for 30 s. Only the ticket's SHA-256 lives in
//! this process (never on disk, never in the audit log), next to the session
//! id and the expiry. The WebSocket upgrade presents the ticket and the actor
//! spawned here then owns the node stream for the life of the session: it
//! relays bytes, pushes input, acknowledges credit, re-checks the opener's
//! access and writes the one `session.end` record.
//!
//! `interactive_sessions` (migration 0005) is local, not replicated. It makes
//! every opened session end with exactly one `session.end`, even one the
//! primary lost in a restart: [`close_orphans`] closes every row still open at
//! start-up with `primary_restarted`.

pub mod actor;

use crate::AppState;
use crate::audit::{self, Channel, Outcome, Record};
use crate::changelog::{Actor, ActorType};
use crate::error::ApiError;
use crate::store::now_rfc3339;
use axum::extract::ws::WebSocket;
use base64::Engine as _;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Lifetime of a ticket: 30 seconds (`InteractiveSessionTicket`).
pub const TICKET_TTL: Duration = Duration::from_secs(30);

/// How long the open waits for the first data before handing out the ticket.
pub const TERMINAL_OPEN_WAIT: Duration = Duration::from_millis(1_500);
/// The console replays its ring immediately, so it needs less time.
pub const CONSOLE_OPEN_WAIT: Duration = Duration::from_millis(300);

/// How often an open session re-checks the opener's access (the contract
/// allows up to 30 s; 5 s closes a revoked session sooner).
pub const RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// No client input for this long closes a session with `idle_timeout`.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// A session open for this long closes with `max_duration`.
pub const MAX_DURATION: Duration = Duration::from_secs(8 * 60 * 60);

/// How often idle and max-duration deadlines are checked.
pub const TICK: Duration = Duration::from_millis(100);

/// How long a primary-side close waits for the node's answering `stream_end`
/// (with the transcript fields) after it asks the node to stop a stream.
pub const STREAM_CLOSE_WAIT: Duration = Duration::from_secs(5);

/// The largest client WebSocket message that becomes one `stream_input`.
pub const MAX_CLIENT_BYTES: usize = 64 * 1024;

/// Cumulative bytes between two `stream_ack` frames, at the latest.
pub const ACK_EVERY_BYTES: u64 = 64 * 1024;

/// The session durations, injectable so tests need not wait 15 minutes.
#[derive(Clone, Copy, Debug)]
pub struct Timings {
    pub ticket_ttl: Duration,
    pub terminal_open_wait: Duration,
    pub console_open_wait: Duration,
    pub recheck: Duration,
    pub idle: Duration,
    pub max_duration: Duration,
    pub tick: Duration,
    /// How long a primary-side close waits for the node's `stream_end`.
    pub stream_close_wait: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            ticket_ttl: TICKET_TTL,
            terminal_open_wait: TERMINAL_OPEN_WAIT,
            console_open_wait: CONSOLE_OPEN_WAIT,
            recheck: RECHECK_INTERVAL,
            idle: IDLE_TIMEOUT,
            max_duration: MAX_DURATION,
            tick: TICK,
            stream_close_wait: STREAM_CLOSE_WAIT,
        }
    }
}

/// Why a session closed (`InteractiveSessionCloseReason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    Exited,
    EndedByClient,
    EndedByRequest,
    TicketExpired,
    IdleTimeout,
    MaxDuration,
    AccessRevoked,
    CredentialExpired,
    VmStopped,
    NodeDisconnected,
    TranscriptLimit,
    PrimaryRestarted,
    Error,
}

impl CloseReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exited => "exited",
            Self::EndedByClient => "ended_by_client",
            Self::EndedByRequest => "ended_by_request",
            Self::TicketExpired => "ticket_expired",
            Self::IdleTimeout => "idle_timeout",
            Self::MaxDuration => "max_duration",
            Self::AccessRevoked => "access_revoked",
            Self::CredentialExpired => "credential_expired",
            Self::VmStopped => "vm_stopped",
            Self::NodeDisconnected => "node_disconnected",
            Self::TranscriptLimit => "transcript_limit",
            Self::PrimaryRestarted => "primary_restarted",
            Self::Error => "error",
        }
    }
}

/// What the actor sends the registry: the socket once the upgrade completed,
/// or a request to end the session.
pub enum Command {
    /// The upgraded WebSocket of this session (one per session).
    Attach(Box<WebSocket>),
    /// `DELETE /api/v1/interactive-sessions/{session_id}` asked to end it.
    End {
        request_id: String,
        ended_by: String,
        /// The caller's `Quoro-Justification`; required when an agent ends
        /// its own session.
        justification: Option<String>,
        reply: oneshot::Sender<bool>,
    },
}

/// One session's attach point in the registry.
pub struct SessionHandle {
    pub tx: mpsc::UnboundedSender<Command>,
    /// Whether a WebSocket already attached; a second upgrade is a conflict.
    pub attached: Arc<AtomicBool>,
}

/// One minted, not yet expired ticket. Only its hash is kept.
struct Ticket {
    hash: [u8; 32],
    expires_at: time::OffsetDateTime,
    /// A ticket minted with the panel's session cookie needs an `Origin`.
    needs_origin: bool,
    /// Consumed on the first successful upgrade; kept until its expiry so a
    /// replay can be told from an unknown ticket.
    used: bool,
}

/// The outcome of presenting a ticket at the WebSocket upgrade.
pub enum TicketCheck {
    /// A fresh ticket; it has just been consumed.
    Valid { needs_origin: bool },
    /// The ticket was already used.
    Spent,
    /// The ticket is past its 30 s.
    Expired,
    /// No such ticket for this session (never minted, or another session's).
    Unknown,
}

struct Inner {
    timings: RwLock<Timings>,
    tickets: Mutex<HashMap<String, Ticket>>,
    sessions: Mutex<HashMap<String, SessionHandle>>,
}

/// The in-memory half of interactive sessions: tickets and attach points.
#[derive(Clone)]
pub struct Interactive(Arc<Inner>);

impl Default for Interactive {
    fn default() -> Self {
        Self(Arc::new(Inner {
            timings: RwLock::new(Timings::default()),
            tickets: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
        }))
    }
}

/// Constant-time equality of two SHA-256 digests.
fn hashes_equal(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl Interactive {
    #[must_use]
    pub fn timings(&self) -> Timings {
        *self.0.timings.read().expect("timings lock")
    }

    /// Replaces the durations. Tests use it to make idle and max-duration
    /// timeouts observable in seconds.
    pub fn set_timings(&self, timings: Timings) {
        *self.0.timings.write().expect("timings lock") = timings;
    }

    /// Mints a 32-byte single-use ticket for `session_id` (base64url without
    /// padding, 43 characters) and keeps only its SHA-256.
    pub fn mint_ticket(&self, session_id: &str, needs_origin: bool) -> (String, String) {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("OS random generator");
        let ticket = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let ttl = self.timings().ticket_ttl;
        let expires_at =
            time::OffsetDateTime::now_utc() + time::Duration::try_from(ttl).unwrap_or_default();
        self.0.tickets.lock().expect("ticket lock").insert(
            session_id.to_string(),
            Ticket {
                hash: Sha256::digest(ticket.as_bytes()).into(),
                expires_at,
                needs_origin,
                used: false,
            },
        );
        let expires_text = expires_at
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        (ticket, expires_text)
    }

    /// Checks `presented` against the ticket minted for `session_id` and
    /// consumes it when it is fresh.
    pub fn consume_ticket(&self, session_id: &str, presented: &str) -> TicketCheck {
        let computed: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        let mut tickets = self.0.tickets.lock().expect("ticket lock");
        let Some(ticket) = tickets.get_mut(session_id) else {
            return TicketCheck::Unknown;
        };
        if !hashes_equal(&ticket.hash, &computed) {
            return TicketCheck::Unknown;
        }
        if ticket.expires_at <= time::OffsetDateTime::now_utc() {
            tickets.remove(session_id);
            return TicketCheck::Expired;
        }
        if ticket.used {
            return TicketCheck::Spent;
        }
        ticket.used = true;
        TicketCheck::Valid {
            needs_origin: ticket.needs_origin,
        }
    }

    /// Forgets a session's ticket once the session ended.
    pub fn forget_ticket(&self, session_id: &str) {
        self.0
            .tickets
            .lock()
            .expect("ticket lock")
            .remove(session_id);
    }

    pub fn register(&self, session_id: &str, handle: SessionHandle) {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .insert(session_id.to_string(), handle);
    }

    #[must_use]
    pub fn handle(
        &self,
        session_id: &str,
    ) -> Option<(mpsc::UnboundedSender<Command>, Arc<AtomicBool>)> {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(session_id)
            .map(|handle| (handle.tx.clone(), Arc::clone(&handle.attached)))
    }

    pub fn unregister(&self, session_id: &str) {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .remove(session_id);
        self.forget_ticket(session_id);
    }
}

/// Marks that the session has its one WebSocket; `false` if it already did.
#[must_use]
pub fn claim_socket(attached: &AtomicBool) -> bool {
    !attached.swap(true, Ordering::SeqCst)
}

// ---------------------------------------------------------------------------
// The local `interactive_sessions` row
// ---------------------------------------------------------------------------

/// Columns of [`SessionRow`], in the order [`session_from_row`] reads them.
pub const SESSION_COLUMNS: &str = "id, kind, vm_id, app_id, project_id, node_id, scope, actor_type, \
     actor_id, agent_model, on_behalf_of, channel, grant_id, target_type, target_id, \
     open_audit_seq, opened_at, connected_at, ended_at, end_audit_seq";

/// One `interactive_sessions` row (migration 0005). The open's audit fields
/// are copied into it so the end record can repeat them exactly.
#[derive(Clone, Debug)]
pub struct SessionRow {
    pub id: String,
    pub kind: String,
    pub vm_id: String,
    pub app_id: Option<String>,
    pub project_id: String,
    pub node_id: String,
    pub scope: String,
    pub actor_type: String,
    pub actor_id: String,
    pub agent_model: Option<String>,
    pub on_behalf_of: Option<String>,
    pub channel: String,
    pub grant_id: Option<String>,
    pub target_type: String,
    pub target_id: String,
    pub open_audit_seq: i64,
    pub opened_at: String,
    pub connected_at: Option<String>,
    pub ended_at: Option<String>,
    pub end_audit_seq: Option<i64>,
}

/// Reads one row selected with [`SESSION_COLUMNS`].
///
/// # Errors
///
/// A missing or mistyped column.
pub fn session_from_row(row: &SqliteRow) -> Result<SessionRow, sqlx::Error> {
    Ok(SessionRow {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        vm_id: row.try_get("vm_id")?,
        app_id: row.try_get("app_id")?,
        project_id: row.try_get("project_id")?,
        node_id: row.try_get("node_id")?,
        scope: row.try_get("scope")?,
        actor_type: row.try_get("actor_type")?,
        actor_id: row.try_get("actor_id")?,
        agent_model: row.try_get("agent_model")?,
        on_behalf_of: row.try_get("on_behalf_of")?,
        channel: row.try_get("channel")?,
        grant_id: row.try_get("grant_id")?,
        target_type: row.try_get("target_type")?,
        target_id: row.try_get("target_id")?,
        open_audit_seq: row.try_get("open_audit_seq")?,
        opened_at: row.try_get("opened_at")?,
        connected_at: row.try_get("connected_at")?,
        ended_at: row.try_get("ended_at")?,
        end_audit_seq: row.try_get("end_audit_seq")?,
    })
}

/// Loads one session row.
///
/// # Errors
///
/// A database error.
pub async fn load_session(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<Option<SessionRow>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {SESSION_COLUMNS} FROM interactive_sessions WHERE id = ?"
    ))
    .bind(session_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.as_ref().map(session_from_row).transpose()?)
}

fn actor_type(value: &str) -> ActorType {
    match value {
        "agent" => ActorType::Agent,
        _ => ActorType::User,
    }
}

fn channel(value: &str) -> Channel {
    match value {
        "panel" => Channel::Panel,
        "mcp" => Channel::Mcp,
        "cli" => Channel::Cli,
        "system" => Channel::System,
        _ => Channel::Api,
    }
}

/// `duration_ms` between an RFC 3339 `opened_at` and `now`.
fn duration_ms(opened_at: &str) -> i64 {
    let Ok(opened) =
        time::OffsetDateTime::parse(opened_at, &time::format_description::well_known::Rfc3339)
    else {
        return 0;
    };
    let delta = time::OffsetDateTime::now_utc() - opened;
    i64::try_from(delta.whole_milliseconds().max(0)).unwrap_or(i64::MAX)
}

/// Everything the close needs beyond the row.
pub struct EndInfo {
    pub reason: CloseReason,
    pub exit_code: Option<i32>,
    pub transcript_sha256: Option<String>,
    pub transcript_bytes: Option<u64>,
    pub connected: bool,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub ended_by: Option<String>,
    /// The `DELETE` caller's justification, recorded on `session.end`.
    pub justification: Option<String>,
    pub request_id: String,
}

/// Writes the one `session.end` record and closes the row, in one write
/// transaction. Idempotent: a session that already ended changes nothing and
/// returns `false`, so a race between DELETE, the node and a timer produces
/// exactly one record.
///
/// # Errors
///
/// A database error, or params JCS cannot canonicalize.
pub async fn finalize(state: &AppState, row: &SessionRow, info: EndInfo) -> Result<bool, ApiError> {
    let mut w = state.store.begin_write().await?;
    let still_open: Option<Option<String>> =
        sqlx::query_scalar("SELECT ended_at FROM interactive_sessions WHERE id = ?")
            .bind(&row.id)
            .fetch_optional(&mut *w.tx)
            .await?;
    match still_open {
        Some(None) => {}
        _ => return Ok(false),
    }

    let mut params = Map::new();
    params.insert("session_id".to_string(), json!(row.id));
    params.insert("kind".to_string(), json!(row.kind));
    params.insert("node_id".to_string(), json!(row.node_id));
    params.insert("vm_id".to_string(), json!(row.vm_id));
    params.insert("opened_at".to_string(), json!(row.opened_at));
    params.insert("connected".to_string(), json!(info.connected));
    params.insert(
        "duration_ms".to_string(),
        json!(duration_ms(&row.opened_at)),
    );
    params.insert("bytes_in".to_string(), json!(info.bytes_in));
    params.insert("bytes_out".to_string(), json!(info.bytes_out));
    if let Some(exit_code) = info.exit_code {
        params.insert("exit_code".to_string(), json!(exit_code));
    }
    params.insert("close_reason".to_string(), json!(info.reason.as_str()));
    if let Some(ended_by) = &info.ended_by {
        params.insert("ended_by".to_string(), json!(ended_by));
    }
    if let Some(sha) = &info.transcript_sha256 {
        params.insert("transcript_sha256".to_string(), json!(sha));
    }
    if let Some(bytes) = info.transcript_bytes {
        params.insert("transcript_bytes".to_string(), json!(bytes));
    }

    let (result, detail) = match info.reason {
        CloseReason::Error => (Outcome::Error, Some("internal".to_string())),
        CloseReason::NodeDisconnected => (Outcome::Error, Some("node_unreachable".to_string())),
        _ => (Outcome::Ok, None),
    };

    let record = Record {
        actor: Actor {
            kind: actor_type(&row.actor_type),
            id: row.actor_id.clone(),
        },
        agent_model: row.agent_model.clone(),
        on_behalf_of: row.on_behalf_of.clone(),
        channel: channel(&row.channel),
        operation: "session.end".to_string(),
        scope: Some(row.scope.clone()),
        target_type: Some(row.target_type.clone()),
        target_id: Some(row.target_id.clone()),
        project_id: Some(row.project_id.clone()),
        params: Value::Object(params),
        justification: info.justification,
        trigger_event_id: None,
        incident_id: None,
        grant_id: row.grant_id.clone(),
        approval_id: None,
        request_id: info.request_id,
        result,
        result_detail: detail,
        change_seq: None,
    };
    let seq = audit::append(&mut w.tx, &record).await?;
    let connected_at = info.connected.then(now_rfc3339);
    sqlx::query(
        "UPDATE interactive_sessions SET ended_at = ?, end_audit_seq = ?, \
         connected_at = COALESCE(connected_at, ?) WHERE id = ? AND ended_at IS NULL",
    )
    .bind(now_rfc3339())
    .bind(seq)
    .bind(connected_at)
    .bind(&row.id)
    .execute(&mut *w.tx)
    .await?;
    w.commit().await?;
    Ok(true)
}

/// Closes every session a previous primary process left open, with
/// `primary_restarted`, before the listener opens (audit-log-format.md
/// section 3.3).
///
/// # Errors
///
/// A message for the operator when the rows cannot be read or a record cannot
/// be written.
pub async fn close_orphans(state: &AppState) -> Result<(), String> {
    let rows = sqlx::query(&format!(
        "SELECT {SESSION_COLUMNS} FROM interactive_sessions WHERE ended_at IS NULL ORDER BY opened_at"
    ))
    .fetch_all(state.store.pool())
    .await
    .map_err(|e| format!("cannot read the open interactive sessions: {e}"))?;
    for row in rows {
        let row = session_from_row(&row)
            .map_err(|e| format!("cannot read an interactive session: {e}"))?;
        let info = EndInfo {
            reason: CloseReason::PrimaryRestarted,
            exit_code: None,
            transcript_sha256: None,
            transcript_bytes: None,
            connected: row.connected_at.is_some(),
            bytes_in: 0,
            bytes_out: 0,
            ended_by: None,
            justification: None,
            request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        };
        finalize(state, &row, info)
            .await
            .map_err(|e| format!("cannot close interactive session {}: {e:?}", row.id))?;
    }
    Ok(())
}

/// Whether `id` is a `<trm_>` session id. The session prefix is not in
/// `quoro_types::Prefix` (the contract freezes the id as `trm_`), so the
/// Crockford body is checked through a `vm_` id.
#[must_use]
pub fn is_session_id(id: &str) -> bool {
    id.strip_prefix("trm_")
        .is_some_and(|rest| quoro_types::is_id(quoro_types::Prefix::Vm, &format!("vm_{rest}")))
}

/// The target of a session's open audit record, as read back from its row.
#[must_use]
pub fn row_target(row: &SessionRow) -> crate::error::Target {
    crate::error::Target {
        kind: row.target_type.clone(),
        id: row.target_id.clone(),
    }
}
