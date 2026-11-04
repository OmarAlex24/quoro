// SPDX-License-Identifier: AGPL-3.0-only
//! Opening, ending and reading interactive sessions (ADR 0113-0116,
//! `permission-model.md` sections 7.5 and 7.6).

use crate::AppState;
use crate::api::apps::app_target;
use crate::api::interactive::{
    CreateConsoleSessionRequest, CreateTerminalSessionRequest, InteractiveSessionKind,
    InteractiveSessionTicket,
};
use crate::api::logs::require_app;
use crate::audit::{self, Outcome};
use crate::auth::Credential;
use crate::error::{ApiError, Code, Reason, Target};
use crate::interactive::{
    CloseReason, Command, EndInfo, SESSION_COLUMNS, SessionHandle, SessionRow, actor, is_session_id,
};
use crate::request::CallCtx;
use crate::session::{HubError, NodeStream, SessionEndReason, StreamEvent};
use crate::store::now_rfc3339;
use axum::body::{Body, Bytes};
use axum::response::Response;
use quoro_types::{Prefix, Role, Scope, is_id};
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// The pinned HTTP answers of a node stream that failed to open.
fn open_error(error: HubError) -> ApiError {
    match error {
        HubError::StreamLimit => ApiError::conflict("the node session holds too many open streams"),
        other => ApiError::new(Code::NodeUnreachable, other.to_string()),
    }
}

/// A `trm_` id with the same 26-character Crockford body as every other id.
fn new_session_id() -> String {
    let id = quoro_types::new_id(Prefix::Vm);
    format!("trm_{}", id.strip_prefix("vm_").unwrap_or(&id))
}

/// Reads the app's project, or answers `not_found`.
async fn app_project(state: &AppState, app_id: &str) -> Result<String, ApiError> {
    require_app(state, app_id).await?;
    let row: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?;
    row.map(|(project,)| project)
        .ok_or_else(|| ApiError::not_found("app"))
}

/// Validates the terminal request body against `CreateTerminalSessionRequest`.
fn validate_terminal(request: &CreateTerminalSessionRequest) -> Result<(), ApiError> {
    if !(1..=1000).contains(&request.rows) {
        return Err(ApiError::validation("rows must be 1-1000"));
    }
    if !(1..=1000).contains(&request.cols) {
        return Err(ApiError::validation("cols must be 1-1000"));
    }
    if let Some(command) = &request.command {
        if command.is_empty() || command.len() > 64 {
            return Err(ApiError::validation("command must have 1-64 items"));
        }
        for arg in command {
            if arg.is_empty() || arg.chars().count() > 4096 {
                return Err(ApiError::validation(
                    "each command item must have 1-4096 characters",
                ));
            }
        }
    }
    if let Some(user) = &request.user
        && (user.is_empty() || user.chars().count() > 64)
    {
        return Err(ApiError::validation("user must have 1-64 characters"));
    }
    if let Some(vm_id) = &request.vm_id
        && !is_id(Prefix::Vm, vm_id)
    {
        return Err(ApiError::validation("vm_id is not a VM id"));
    }
    Ok(())
}

fn validate_console(request: &CreateConsoleSessionRequest) -> Result<(), ApiError> {
    if let Some(tail_bytes) = request.tail_bytes
        && !(0..=1_048_576).contains(&tail_bytes)
    {
        return Err(ApiError::validation("tail_bytes must be 0-1048576"));
    }
    Ok(())
}

/// What waiting for the first node event told us.
enum FirstEvent {
    /// Bytes already produced, to be handed to the WebSocket later.
    Data(Vec<Vec<u8>>),
    /// No data yet; the session still exists.
    Quiet,
}

/// Waits for the first data event, a failed open, or the deadline.
async fn wait_open(stream: &mut NodeStream, wait: Duration) -> Result<FirstEvent, ApiError> {
    let deadline = Instant::now() + wait;
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => return Ok(FirstEvent::Quiet),
            Ok(None) => {
                return Err(ApiError::new(
                    Code::NodeUnreachable,
                    "the node session ended",
                ));
            }
            Ok(Some(StreamEvent::Data(bytes))) => return Ok(FirstEvent::Data(vec![bytes])),
            Ok(Some(StreamEvent::SessionEnd {
                reason: SessionEndReason::Error,
                message,
                node_code,
                ..
            })) => {
                return Err(match node_code {
                    // An old VM without the M4 init, a stopped VM, or a VM
                    // that is not on this node.
                    Some(
                        quoro_proto::ErrorCode::NotFound | quoro_proto::ErrorCode::NotAvailable,
                    ) => ApiError::conflict(if message.is_empty() {
                        "the VM cannot serve this session".to_string()
                    } else {
                        message
                    }),
                    _ => ApiError::new(
                        Code::NodeUnreachable,
                        if message.is_empty() {
                            "the node could not open the session".to_string()
                        } else {
                            message
                        },
                    ),
                });
            }
            Ok(Some(StreamEvent::SessionEnd {
                reason, message, ..
            })) => {
                return Err(ApiError::conflict(if message.is_empty() {
                    format!("the node ended the session ({})", reason.as_str())
                } else {
                    message
                }));
            }
            Ok(Some(_)) => {}
        }
    }
}

/// Registers the ticket and the actor, returning the ticket response.
#[allow(clippy::too_many_arguments)]
fn hand_over(
    state: &AppState,
    ctx: &CallCtx,
    row: SessionRow,
    stream: NodeStream,
    buffered: Vec<Vec<u8>>,
) -> InteractiveSessionTicket {
    let interactive = state.interactive.clone();
    let needs_origin = matches!(ctx.identity.credential, Credential::Session { .. });
    let (ticket, expires_at) = interactive.mint_ticket(&row.id, needs_origin);
    let (tx, rx) = mpsc::unbounded_channel();
    interactive.register(
        &row.id,
        SessionHandle {
            tx,
            attached: Arc::new(AtomicBool::new(false)),
        },
    );
    let identity = ctx.identity.clone();
    let state = state.clone();
    let id = row.id.clone();
    let kind = if row.kind == "console" {
        InteractiveSessionKind::Console
    } else {
        InteractiveSessionKind::Terminal
    };
    let vm_id = row.vm_id.clone();
    tokio::spawn(async move {
        actor::spawn(state, row, identity, interactive, stream, buffered, rx).await;
    });
    InteractiveSessionTicket {
        websocket_path: format!("/api/v1/interactive-sessions/{id}/ws"),
        id,
        kind,
        vm_id,
        ticket,
        expires_at,
    }
}

/// `POST /api/v1/apps/{app_id}/terminal-sessions` (`app:terminal`,
/// human-only in M4).
///
/// # Errors
///
/// `forbidden` for a viewer or an agent, `not_found` for an unknown app or a
/// VM that is not the app's, `conflict` when no instance runs or the node
/// cannot serve the session, `validation_failed` for a bad body, or
/// `node_unreachable` when the node has no session.
pub async fn open_terminal(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    request: &CreateTerminalSessionRequest,
) -> Result<InteractiveSessionTicket, ApiError> {
    validate_terminal(request)?;
    let session_id = new_session_id();
    let target = app_target(app_id);
    let argv = request
        .command
        .clone()
        .unwrap_or_else(|| vec!["/bin/sh".to_string()]);
    let rows = request.rows;
    let cols = request.cols;
    let user = request.user.clone();
    let requested_vm = request.vm_id.clone();
    let mut request_params = Map::new();
    request_params.insert("rows".to_string(), json!(rows));
    request_params.insert("cols".to_string(), json!(cols));
    request_params.insert("command".to_string(), json!(argv));
    if let Some(user) = &user {
        request_params.insert("user".to_string(), json!(user));
    }
    if let Some(vm_id) = &requested_vm {
        request_params.insert("vm_id".to_string(), json!(vm_id));
    }
    let params = Value::Object(request_params);
    // Authorization comes first so a refusal is audited and an unknown app is
    // an authorized-but-failed open (404, audited as an error).
    ctx.authorize_human(state, Scope::AppTerminal, Some(&target), None, &params)
        .await?;
    let mut project_id: Option<String> = None;

    let result = async {
        let project = app_project(state, app_id).await?;
        project_id = Some(project.clone());
        let project_id = project;
        let (vm_id, node_id) = match requested_vm {
            Some(vm_id) => {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT node_id FROM vms WHERE id = ? AND app_id = ?")
                        .bind(&vm_id)
                        .bind(app_id)
                        .fetch_optional(state.store.pool())
                        .await?;
                let Some((node_id,)) = row else {
                    return Err(ApiError::not_found("vm"));
                };
                (vm_id, node_id)
            }
            None => {
                let row: Option<(String, String)> = sqlx::query_as(
                    "SELECT id, node_id FROM vms WHERE app_id = ? AND observed_status = 'running' \
                     ORDER BY id LIMIT 1",
                )
                .bind(app_id)
                .fetch_optional(state.store.pool())
                .await?;
                let Some(row) = row else {
                    return Err(ApiError::conflict("the app has no running instance"));
                };
                row
            }
        };
        if !state.hub.is_connected(&node_id) {
            return Err(open_error(HubError::NotConnected));
        }
        // The params the audit record carries name the resolved VM, not the
        // body's placeholder.
        let mut stream_params = json!({
            "session_id": session_id,
            "vm_id": vm_id,
            "rows": rows,
            "cols": cols,
            "argv": argv,
        });
        if let Some(user) = &user {
            stream_params["user"] = json!(user);
        }
        let mut stream = state
            .hub
            .open_stream(&node_id, "terminal", stream_params)
            .map_err(open_error)?;
        let buffered =
            match wait_open(&mut stream, state.interactive.timings().terminal_open_wait).await? {
                FirstEvent::Data(bytes) => bytes,
                FirstEvent::Quiet => Vec::new(),
            };

        let mut record_params = Map::new();
        record_params.insert("session_id".to_string(), json!(session_id));
        record_params.insert("vm_id".to_string(), json!(vm_id));
        record_params.insert("rows".to_string(), json!(rows));
        record_params.insert("cols".to_string(), json!(cols));
        record_params.insert("command".to_string(), json!(argv));
        if let Some(user) = &user {
            record_params.insert("user".to_string(), json!(user));
        }
        let record_params = Value::Object(record_params);
        let authority = ctx.authority();
        let actor = ctx.identity.actor();
        let opened_at = now_rfc3339();
        let row = SessionRow {
            id: session_id.clone(),
            kind: "terminal".to_string(),
            vm_id: vm_id.clone(),
            app_id: Some(app_id.to_string()),
            project_id: project_id.clone(),
            node_id: node_id.clone(),
            scope: Scope::AppTerminal.as_str().to_string(),
            actor_type: actor.kind.as_str().to_string(),
            actor_id: actor.id.clone(),
            agent_model: ctx.identity.agent().map(|agent| agent.model.clone()),
            on_behalf_of: authority.and_then(|a| a.on_behalf_of.clone()),
            channel: ctx.channel().as_str().to_string(),
            grant_id: authority.and_then(|a| a.grant_id.clone()),
            target_type: "app".to_string(),
            target_id: app_id.to_string(),
            open_audit_seq: 0,
            opened_at,
            connected_at: None,
            ended_at: None,
            end_audit_seq: None,
        };
        let mut w = state.store.begin_write().await?;
        let seq = ctx
            .append_ok(
                &mut w.tx,
                Some(Scope::AppTerminal),
                Some(&target),
                Some(&project_id),
                &record_params,
                None,
            )
            .await?;
        insert_session(&mut w.tx, &row, seq).await?;
        w.commit().await?;
        Ok::<_, ApiError>((row, stream, buffered))
    }
    .await;

    match result {
        Ok((row, stream, buffered)) => Ok(hand_over(state, ctx, row, stream, buffered)),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AppTerminal),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// `POST /api/v1/vms/{vm_id}/console-sessions` (`vm:console`).
///
/// # Errors
///
/// `forbidden` without the scope or selector, `not_found` for an unknown VM,
/// `validation_failed` for a bad body or an agent without a justification,
/// `conflict` when the node cannot serve the session, or `node_unreachable`
/// when the node has no session.
pub async fn open_console(
    state: &AppState,
    ctx: &CallCtx,
    vm_id: &str,
    request: &CreateConsoleSessionRequest,
) -> Result<InteractiveSessionTicket, ApiError> {
    validate_console(request)?;
    let session_id = new_session_id();
    let target = Target {
        kind: "vm".to_string(),
        id: vm_id.to_string(),
    };
    let tail_bytes = request.tail_bytes;
    let mut request_params = Map::new();
    if let Some(tail_bytes) = tail_bytes {
        request_params.insert("tail_bytes".to_string(), json!(tail_bytes));
    }
    let params = Value::Object(request_params);
    ctx.authorize(state, Scope::VmConsole, Some(&target), None, &params)
        .await?;
    let mut project_id: Option<String> = None;

    let result = async {
        if !is_id(Prefix::Vm, vm_id) {
            return Err(ApiError::not_found("vm"));
        }
        let row: Option<(String, Option<String>, String)> =
            sqlx::query_as("SELECT project_id, app_id, node_id FROM vms WHERE id = ?")
                .bind(vm_id)
                .fetch_optional(state.store.pool())
                .await?;
        let Some((project, app_id, node_id)) = row else {
            return Err(ApiError::not_found("vm"));
        };
        project_id = Some(project.clone());
        let project_id = project;
        if !state.hub.is_connected(&node_id) {
            return Err(open_error(HubError::NotConnected));
        }
        let mut stream_params = json!({ "vm_id": vm_id });
        if let Some(tail_bytes) = tail_bytes {
            stream_params["tail_bytes"] = json!(tail_bytes);
        }
        let mut stream = state
            .hub
            .open_stream(&node_id, "console", stream_params)
            .map_err(open_error)?;
        let buffered =
            match wait_open(&mut stream, state.interactive.timings().console_open_wait).await? {
                FirstEvent::Data(bytes) => bytes,
                FirstEvent::Quiet => Vec::new(),
            };

        let mut record_params = Map::new();
        record_params.insert("session_id".to_string(), json!(session_id));
        record_params.insert("vm_id".to_string(), json!(vm_id));
        if let Some(tail_bytes) = tail_bytes {
            record_params.insert("tail_bytes".to_string(), json!(tail_bytes));
        }
        let record_params = Value::Object(record_params);
        let authority = ctx.authority();
        let actor = ctx.identity.actor();
        let opened_at = now_rfc3339();
        let row = SessionRow {
            id: session_id.clone(),
            kind: "console".to_string(),
            vm_id: vm_id.to_string(),
            app_id,
            project_id: project_id.clone(),
            node_id: node_id.clone(),
            scope: Scope::VmConsole.as_str().to_string(),
            actor_type: actor.kind.as_str().to_string(),
            actor_id: actor.id.clone(),
            agent_model: ctx.identity.agent().map(|agent| agent.model.clone()),
            on_behalf_of: authority.and_then(|a| a.on_behalf_of.clone()),
            channel: ctx.channel().as_str().to_string(),
            grant_id: authority.and_then(|a| a.grant_id.clone()),
            target_type: "vm".to_string(),
            target_id: vm_id.to_string(),
            open_audit_seq: 0,
            opened_at,
            connected_at: None,
            ended_at: None,
            end_audit_seq: None,
        };
        let mut w = state.store.begin_write().await?;
        let seq = ctx
            .append_ok(
                &mut w.tx,
                Some(Scope::VmConsole),
                Some(&target),
                Some(&project_id),
                &record_params,
                None,
            )
            .await?;
        insert_session(&mut w.tx, &row, seq).await?;
        w.commit().await?;
        Ok::<_, ApiError>((row, stream, buffered))
    }
    .await;

    match result {
        Ok((row, stream, buffered)) => Ok(hand_over(state, ctx, row, stream, buffered)),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::VmConsole),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Inserts the local session row with every column.
async fn insert_session(
    conn: &mut sqlx::SqliteConnection,
    row: &SessionRow,
    open_audit_seq: i64,
) -> Result<(), ApiError> {
    sqlx::query(&format!(
        "INSERT INTO interactive_sessions ({SESSION_COLUMNS}) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ))
    .bind(&row.id)
    .bind(&row.kind)
    .bind(&row.vm_id)
    .bind(&row.app_id)
    .bind(&row.project_id)
    .bind(&row.node_id)
    .bind(&row.scope)
    .bind(&row.actor_type)
    .bind(&row.actor_id)
    .bind(&row.agent_model)
    .bind(&row.on_behalf_of)
    .bind(&row.channel)
    .bind(&row.grant_id)
    .bind(&row.target_type)
    .bind(&row.target_id)
    .bind(open_audit_seq)
    .bind(&row.opened_at)
    .bind(&row.connected_at)
    .bind(&row.ended_at)
    .bind(row.end_audit_seq)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `DELETE /api/v1/interactive-sessions/{session_id}`.
///
/// # Errors
///
/// `not_found` for an unknown or already ended session, `forbidden` for a
/// caller that neither opened it nor holds the `owner` role (the refusal is
/// audited), or a database error.
pub async fn end_session(
    state: &AppState,
    ctx: &CallCtx,
    session_id: &str,
) -> Result<(), ApiError> {
    if !is_session_id(session_id) {
        return Err(ApiError::not_found("session"));
    }
    let mut conn = state.store.pool().acquire().await?;
    let row = crate::interactive::load_session(&mut conn, session_id).await?;
    drop(conn);
    let Some(row) = row else {
        return Err(ApiError::not_found("session"));
    };
    if row.ended_at.is_some() {
        return Err(ApiError::not_found("session"));
    }
    let target = crate::interactive::row_target(&row);
    let params = json!({ "session_id": session_id });
    let is_opener =
        ctx.identity.id() == row.actor_id && ctx.identity.actor().kind.as_str() == row.actor_type;
    let is_owner = ctx
        .identity
        .user()
        .is_some_and(|user| user.role == Role::Owner);
    if !is_opener && !is_owner {
        let error = ApiError::forbidden(Reason::ScopeMissing, None);
        ctx.audit_error(
            state,
            None,
            Some(&target),
            Some(&row.project_id),
            &params,
            &error,
        )
        .await?;
        return Err(error);
    }

    // Like every mutating agent call, an agent's DELETE states why (the
    // `Quoro-Justification` header); the reason lands on `session.end`.
    if !ctx.identity.is_human() && ctx.justification.is_none() {
        let error =
            ApiError::validation("a justification is required for an agent's mutating call");
        ctx.audit_error(
            state,
            Some(Scope::parse(&row.scope).unwrap_or(Scope::VmConsole)),
            Some(&target),
            Some(&row.project_id),
            &params,
            &error,
        )
        .await?;
        return Err(error);
    }

    let request_id = ctx.request_id.clone();
    let ended_by = ctx.identity.id().to_string();
    let justification = ctx.justification.clone();
    if let Some((tx, _)) = state.interactive.handle(session_id) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = Command::End {
            request_id: request_id.clone(),
            ended_by: ended_by.clone(),
            justification: justification.clone(),
            reply: reply_tx,
        };
        if tx.send(command).is_ok() {
            match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
                Ok(Ok(true)) => return Ok(()),
                Ok(Ok(false)) => return Err(ApiError::not_found("session")),
                _ => {}
            }
        }
    }
    // The actor is gone (or never existed in this process): close the row
    // here. `finalize` is idempotent, so a race still writes one record.
    let info = EndInfo {
        reason: CloseReason::EndedByRequest,
        exit_code: None,
        transcript_sha256: None,
        transcript_bytes: None,
        connected: row.connected_at.is_some(),
        bytes_in: 0,
        bytes_out: 0,
        ended_by: Some(ended_by),
        justification,
        request_id,
    };
    if crate::interactive::finalize(state, &row, info).await? {
        Ok(())
    } else {
        Err(ApiError::not_found("session"))
    }
}

/// Records one transcript fetch (audit-log-format.md section 3.3: every fetch
/// is audited, also a human's).
async fn audit_transcript(
    state: &AppState,
    ctx: &CallCtx,
    target: &Target,
    project_id: Option<&str>,
    params: &Value,
    outcome: Outcome,
    detail: Option<String>,
) -> Result<(), ApiError> {
    let record = ctx.record(
        Some(Scope::AuditRead),
        Some(target),
        project_id,
        params,
        outcome,
        detail,
        None,
    );
    let mut w = state.store.begin_write().await?;
    audit::append(&mut w.tx, &record).await?;
    w.commit().await?;
    Ok(())
}

/// Audits a failed transcript fetch and returns the error.
async fn transcript_error(
    state: &AppState,
    ctx: &CallCtx,
    target: &Target,
    project_id: &str,
    params: &Value,
    error: ApiError,
) -> ApiError {
    let _ = audit_transcript(
        state,
        ctx,
        target,
        Some(project_id),
        params,
        Outcome::Error,
        Some(error.audit_detail()),
    )
    .await;
    error
}

/// Streams a terminal's transcript as asciicast v2 from the node.
///
/// # Errors
///
/// `forbidden` (`human_only`) for an agent, `not_found` for an unknown or
/// console session or one whose transcript the node no longer has, or
/// `node_unreachable` when the node has no session.
pub async fn transcript(
    state: &AppState,
    ctx: &CallCtx,
    session_id: &str,
) -> Result<Response, ApiError> {
    let params = json!({ "session_id": session_id });
    let row = if is_session_id(session_id) {
        let mut conn = state.store.pool().acquire().await?;
        let row = crate::interactive::load_session(&mut conn, session_id).await?;
        drop(conn);
        row
    } else {
        None
    };
    // Human-only: an agent is refused with `human_only` before anything else,
    // whether the session exists or not. The refusal names the session's VM
    // when there is one, like every transcript fetch record.
    if !ctx.identity.is_human() {
        let target = row.as_ref().map(|row| Target {
            kind: "vm".to_string(),
            id: row.vm_id.clone(),
        });
        ctx.authorize_human(
            state,
            Scope::AuditRead,
            target.as_ref(),
            row.as_ref().map(|row| row.project_id.as_str()),
            &params,
        )
        .await?;
    }
    let Some(row) = row else {
        return Err(ApiError::not_found("session"));
    };
    if row.kind != "terminal" {
        return Err(ApiError::not_found("transcript"));
    }
    let target = Target {
        kind: "vm".to_string(),
        id: row.vm_id.clone(),
    };
    // A human without `audit:read` is refused, and the refusal is audited
    // like every transcript fetch.
    if !ctx
        .identity
        .role()
        .is_some_and(|role| role.holds(Scope::AuditRead))
    {
        let error = ApiError::forbidden(Reason::ScopeMissing, Some(Scope::AuditRead.as_str()));
        audit_transcript(
            state,
            ctx,
            &target,
            Some(&row.project_id),
            &params,
            Outcome::Refused,
            Some("scope_missing".to_string()),
        )
        .await?;
        return Err(error);
    }
    if !state.hub.is_connected(&row.node_id) {
        return Err(transcript_error(
            state,
            ctx,
            &target,
            &row.project_id,
            &params,
            open_error(HubError::NotConnected),
        )
        .await);
    }
    let mut stream = match state.hub.open_stream(
        &row.node_id,
        "transcript",
        json!({ "session_id": session_id }),
    ) {
        Ok(stream) => stream,
        Err(error) => {
            return Err(transcript_error(
                state,
                ctx,
                &target,
                &row.project_id,
                &params,
                open_error(error),
            )
            .await);
        }
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => {
                return Err(transcript_error(
                    state,
                    ctx,
                    &target,
                    &row.project_id,
                    &params,
                    ApiError::new(Code::NodeUnreachable, "the node did not answer in time"),
                )
                .await);
            }
            Ok(None) => {
                return Err(transcript_error(
                    state,
                    ctx,
                    &target,
                    &row.project_id,
                    &params,
                    ApiError::new(Code::NodeUnreachable, "the node session ended"),
                )
                .await);
            }
            Ok(Some(StreamEvent::Data(bytes))) => {
                pending.push_back(bytes);
                break;
            }
            Ok(Some(StreamEvent::SessionEnd {
                reason,
                message,
                node_code,
                ..
            })) => {
                let error = match node_code {
                    Some(quoro_proto::ErrorCode::NotFound) | None => {
                        ApiError::not_found("transcript")
                    }
                    _ => ApiError::new(
                        Code::NodeUnreachable,
                        if message.is_empty() {
                            format!("the node ended the stream ({})", reason.as_str())
                        } else {
                            message
                        },
                    ),
                };
                return Err(
                    transcript_error(state, ctx, &target, &row.project_id, &params, error).await,
                );
            }
            Ok(Some(_)) => {}
        }
    }
    audit_transcript(
        state,
        ctx,
        &target,
        Some(&row.project_id),
        &params,
        Outcome::Ok,
        None,
    )
    .await?;

    let feed = TranscriptFeed {
        stream,
        pending,
        acked: 0,
    };
    let body = Body::from_stream(futures_util::stream::unfold(feed, |mut feed| async move {
        feed.next_chunk()
            .await
            .map(|bytes| (Ok::<Bytes, std::convert::Infallible>(bytes), feed))
    }));
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-asciicast")
        .body(body)
        .map_err(|e| ApiError::internal(&e))
}

/// One transcript fetch's body: the node stream, acked as it is handed to the
/// HTTP body (section 5.25).
struct TranscriptFeed {
    stream: NodeStream,
    pending: VecDeque<Vec<u8>>,
    acked: u64,
}

impl TranscriptFeed {
    async fn next_chunk(&mut self) -> Option<Bytes> {
        loop {
            if let Some(bytes) = self.pending.pop_front() {
                self.acked += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let _ = self.stream.send_ack(self.acked);
                return Some(Bytes::from(bytes));
            }
            match self.stream.next().await {
                Some(StreamEvent::Data(bytes)) => self.pending.push_back(bytes),
                Some(StreamEvent::Lines(_) | StreamEvent::Metrics(_)) => {}
                Some(StreamEvent::End { .. } | StreamEvent::SessionEnd { .. }) | None => {
                    return None;
                }
            }
        }
    }
}
