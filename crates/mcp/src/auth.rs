//! Inbound authentication of the MCP endpoint (permission-model.md sections 2
//! and 10): `Authorization: Bearer atk_...` and nothing else.
//!
//! The token is resolved by the control plane's
//! [`authenticate_bearer`](quoro_control_plane::auth::authenticate_bearer),
//! which never reads cookies. A personal API token (`tok_`) also resolves
//! there, so this middleware checks the `atk_` prefix before the lookup and
//! the agent principal after it. Authentication failures carry no identity
//! and are not audited (audit-log-format.md section 3).

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use quoro_control_plane::AppState;
use quoro_control_plane::audit::Channel;
use quoro_control_plane::auth::{Identity, authenticate_bearer};
use quoro_control_plane::error::{ApiError, Code};
use quoro_control_plane::request::{
    CallCtx, Limited, read_limited, refuse_too_large, request_id_of,
};

/// The only credential prefix the MCP endpoint accepts.
pub const ACCESS_TOKEN_PREFIX: &str = "atk_";

/// Middleware, in front of [`require_agent`]: the request body limit of
/// `/mcp` (README "Request body limit", ADR 0306). A body within the limit
/// goes on, in memory, to authentication and rmcp. A larger one is answered
/// `413 payload_too_large` without being parsed. The request is authenticated
/// first, without its body: a valid agent access token makes the refusal an
/// audited agent call, and since the tool is named inside the unread body,
/// the record's operation is the request line, `POST /mcp`. A call without
/// one is answered the same and not recorded, as an authentication failure
/// is not.
pub async fn limit_body(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let parts = match read_limited(request).await {
        Ok(Limited::Within(request)) => return next.run(request).await,
        Ok(Limited::Above(parts)) => parts,
        Err(error) => return error.into_response(),
    };
    let caller = match agent_identity(&state, &parts.headers).await {
        Ok(identity) => Some(CallCtx::with_channel(
            identity,
            Channel::Mcp,
            request_id_of(&parts.extensions),
            format!("{} {}", parts.method, crate::MCP_PATH),
            None,
            None,
        )),
        Err(error) if error.code == Code::Unauthenticated => None,
        Err(error) => return error.into_response(),
    };
    refuse_too_large(&state, caller.as_ref())
        .await
        .into_response()
}

/// Middleware: resolves the agent access token and stores the [`Identity`] in
/// the request extensions, where the tool handler reads it. Anything else is
/// `401 unauthenticated` with `WWW-Authenticate: Bearer`.
pub async fn require_agent(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    match agent_identity(&state, request.headers()).await {
        Ok(identity) => {
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        Err(error) => {
            let mut response = error.into_response();
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            response
        }
    }
}

async fn agent_identity(state: &AppState, headers: &HeaderMap) -> Result<Identity, ApiError> {
    let is_access_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|token| token.trim().starts_with(ACCESS_TOKEN_PREFIX));
    if !is_access_token {
        return Err(ApiError::unauthenticated());
    }
    let identity = authenticate_bearer(state, headers).await?;
    if identity.agent().is_none() {
        return Err(ApiError::unauthenticated());
    }
    Ok(identity)
}
