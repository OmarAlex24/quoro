// SPDX-License-Identifier: AGPL-3.0-only
//! Interactive sessions: the web terminal, the read-only VM console and the
//! transcript fetch (ADR 0113-0116, `openapi.yaml` operations
//! `createTerminalSession`, `createConsoleSession`, `endInteractiveSession`,
//! `connectInteractiveSession` and `getSessionTranscript`).

use crate::AppState;
use crate::auth::Identity;
use crate::error::{ApiError, Code};
use crate::interactive::{Claim, Command, TicketCheck};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Router;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

/// Routes of the interactive sessions.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/apps/{app_id}/terminal-sessions",
            post(create_terminal_session),
        )
        .route(
            "/api/v1/vms/{vm_id}/console-sessions",
            post(create_console_session),
        )
        .route(
            "/api/v1/interactive-sessions/{session_id}",
            axum::routing::delete(end_interactive_session),
        )
        .route(
            "/api/v1/interactive-sessions/{session_id}/ws",
            get(connect_interactive_session),
        )
        .route(
            "/api/v1/interactive-sessions/{session_id}/transcript",
            get(get_session_transcript),
        )
}

// ---------------------------------------------------------------------------
// Schemas (`openapi.yaml`)
// ---------------------------------------------------------------------------

/// Body of `createTerminalSession`.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateTerminalSessionRequest")]
pub struct CreateTerminalSessionRequest {
    /// Initial terminal height in rows.
    #[schema(minimum = 1, maximum = 1000)]
    pub rows: i64,
    /// Initial terminal width in columns.
    #[schema(minimum = 1, maximum = 1000)]
    pub cols: i64,
    /// The argv to run; default `["/bin/sh"]`.
    #[serde(default)]
    #[schema(min_items = 1, max_items = 64, nullable = false)]
    pub command: Option<Vec<String>>,
    /// User to run as, a name or uid optionally followed by `:` and a group.
    #[serde(default)]
    #[schema(min_length = 1, max_length = 64, nullable = false)]
    pub user: Option<String>,
    /// The instance to open it in; default the app's running instance.
    #[serde(default)]
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false)]
    pub vm_id: Option<String>,
}

/// Body of `createConsoleSession`; every field is optional.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateConsoleSessionRequest")]
pub struct CreateConsoleSessionRequest {
    /// Replay at most this many bytes of recent output before live output.
    #[serde(default)]
    #[schema(minimum = 0, maximum = 1_048_576, nullable = false)]
    pub tail_bytes: Option<i64>,
}

/// Whether the session is a web terminal or a console.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[schema(
    title = "InteractiveSessionKind",
    description = "Whether this is a web terminal or a read-only console."
)]
pub enum InteractiveSessionKind {
    Terminal,
    Console,
}

/// An opened session and its single-use ticket.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "InteractiveSessionTicket")]
pub struct InteractiveSessionTicket {
    /// Session id.
    #[schema(pattern = "^trm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Whether this is a web terminal or a read-only console.
    #[schema(inline)]
    pub kind: InteractiveSessionKind,
    /// The VM the session is attached to.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub vm_id: String,
    /// Single-use ticket, base64url without padding.
    #[schema(min_length = 43, max_length = 64)]
    pub ticket: String,
    /// When the ticket stops being accepted.
    #[schema(format = "date-time")]
    pub expires_at: String,
    /// Path to upgrade; the ticket goes in the `ticket` query parameter.
    pub websocket_path: String,
}

/// Query of `connectInteractiveSession`.
#[derive(Debug, Deserialize)]
pub struct TicketQuery {
    pub ticket: Option<String>,
}

/// OpenAPI paths and schemas of the interactive operations.
#[derive(OpenApi)]
#[openapi(
    paths(
        create_terminal_session,
        create_console_session,
        end_interactive_session,
        connect_interactive_session,
        get_session_transcript,
    ),
    components(schemas(
        CreateTerminalSessionRequest,
        CreateConsoleSessionRequest,
        InteractiveSessionTicket,
    ))
)]
pub struct InteractiveDoc;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Open a web terminal in an app.
///
/// # Errors
///
/// See [`crate::service::interactive::open_terminal`].
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/terminal-sessions",
    tag = "interactive",
    operation_id = "createTerminalSession",
    extensions(("x-quoro-scope" = json!("app:terminal"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).", pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body = CreateTerminalSessionRequest,
    responses(
        (status = 201, description = "The session is open and waits for its WebSocket.", body = InteractiveSessionTicket,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn create_terminal_session(
    State(state): State<AppState>,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    identity: Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(request): ApiJson<CreateTerminalSessionRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/terminal-sessions",
        &headers,
    );
    let ticket =
        crate::service::interactive::open_terminal(&state, &ctx, &app_id, &request).await?;
    Ok((StatusCode::CREATED, axum::Json(ticket)).into_response())
}

/// Open a read-only view of a VM's console.
///
/// # Errors
///
/// See [`crate::service::interactive::open_console`].
#[utoipa::path(
    post,
    path = "/api/v1/vms/{vm_id}/console-sessions",
    tag = "interactive",
    operation_id = "createConsoleSession",
    extensions(("x-quoro-scope" = json!("vm:console"))),
    params(
        ("vm_id" = String, Path, description = "VM id (`vm_...`).", pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    request_body = CreateConsoleSessionRequest,
    responses(
        (status = 201, description = "The session is open and waits for its WebSocket.", body = InteractiveSessionTicket,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn create_console_session(
    State(state): State<AppState>,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    identity: Identity,
    headers: HeaderMap,
    Path(vm_id): Path<String>,
    body: Option<ApiJson<CreateConsoleSessionRequest>>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/vms/{vm_id}/console-sessions",
        &headers,
    );
    let request = body.map_or_else(CreateConsoleSessionRequest::default, |body| body.0);
    let ticket = crate::service::interactive::open_console(&state, &ctx, &vm_id, &request).await?;
    Ok((StatusCode::CREATED, axum::Json(ticket)).into_response())
}

/// End a terminal or console session.
///
/// # Errors
///
/// See [`crate::service::interactive::end_session`].
#[utoipa::path(
    delete,
    path = "/api/v1/interactive-sessions/{session_id}",
    tag = "interactive",
    operation_id = "endInteractiveSession",
    extensions(("x-quoro-scope" = json!("authenticated"))),
    params(
        ("session_id" = String, Path, description = "Terminal or console session id (`trm_...`).", pattern = "^trm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    responses(
        (status = 204, description = "The session was closed.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn end_interactive_session(
    State(state): State<AppState>,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    identity: Identity,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "DELETE /api/v1/interactive-sessions/{session_id}",
        &headers,
    );
    crate::service::interactive::end_session(&state, &ctx, &session_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Attach a WebSocket to an open session.
///
/// # Errors
///
/// `unauthenticated` for a spent, expired or unknown ticket, `forbidden` for
/// a missing or mismatched `Origin`, or `conflict` for a session that already
/// has its WebSocket.
#[utoipa::path(
    get,
    path = "/api/v1/interactive-sessions/{session_id}/ws",
    tag = "interactive",
    operation_id = "connectInteractiveSession",
    security(()),
    extensions(("x-quoro-scope" = json!("public"))),
    params(
        ("session_id" = String, Path, description = "Terminal or console session id (`trm_...`).", pattern = "^trm_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("ticket" = String, Query, description = "The single-use ticket from the open.", min_length = 43, max_length = 64),
    ),
    responses(
        (status = 101, description = "Switching protocols; the WebSocket carries the session."),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn connect_interactive_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<TicketQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let Some(ticket) = query
        .ticket
        .as_deref()
        .filter(|t| (43..=64).contains(&t.len()))
    else {
        return Err(ApiError::unauthenticated());
    };
    let needs_origin = match state.interactive.consume_ticket(&session_id, ticket) {
        TicketCheck::Valid { needs_origin } => needs_origin,
        TicketCheck::Spent => {
            // The one WebSocket of the session is already attached: the
            // replay is a second upgrade, not an unknown ticket.
            if state
                .interactive
                .handle(&session_id)
                .is_some_and(|(_, socket)| socket.is_claimed())
            {
                return Err(ApiError::conflict("the session already has its WebSocket"));
            }
            return Err(ApiError::unauthenticated());
        }
        TicketCheck::Expired | TicketCheck::Unknown => {
            return Err(ApiError::unauthenticated());
        }
    };
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let panel = state.config.primary_url(&headers);
    match origin {
        Some(origin) => {
            if normalise_origin(origin) != normalise_origin(&panel) {
                return Err(ApiError::new(
                    Code::Forbidden,
                    "the Origin does not match the panel's origin",
                ));
            }
        }
        None if needs_origin => {
            return Err(ApiError::new(
                Code::Forbidden,
                "a ticket minted with a panel session needs an Origin header",
            ));
        }
        None => {}
    }
    let Some((tx, socket)) = state.interactive.handle(&session_id) else {
        return Err(ApiError::unauthenticated());
    };
    match socket.claim() {
        Claim::Claimed => {}
        Claim::Taken => {
            return Err(ApiError::conflict("the session already has its WebSocket"));
        }
        // The session ended before this upgrade: like a session already gone.
        Claim::Sealed => return Err(ApiError::unauthenticated()),
    }
    Ok(ws
        .on_upgrade(move |socket| async move {
            let _ = tx.send(Command::Attach(Box::new(socket)));
        })
        .into_response())
}

/// Fetch a terminal session's asciicast v2 transcript.
///
/// # Errors
///
/// See [`crate::service::interactive::transcript`].
#[utoipa::path(
    get,
    path = "/api/v1/interactive-sessions/{session_id}/transcript",
    tag = "interactive",
    operation_id = "getSessionTranscript",
    extensions(("x-quoro-scope" = json!("audit:read"))),
    params(
        ("session_id" = String, Path, description = "Terminal or console session id (`trm_...`).", pattern = "^trm_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The transcript file, streamed from the node.",
            content_type = "application/x-asciicast", body = String,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn get_session_transcript(
    State(state): State<AppState>,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    identity: Identity,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/interactive-sessions/{session_id}/transcript",
        &headers,
    );
    crate::service::interactive::transcript(&state, &ctx, &session_id).await
}

fn normalise_origin(origin: &str) -> String {
    origin.trim_end_matches('/').to_ascii_lowercase()
}
