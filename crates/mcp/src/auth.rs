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
use quoro_control_plane::auth::{Identity, authenticate_bearer};
use quoro_control_plane::error::ApiError;

/// The only credential prefix the MCP endpoint accepts.
pub const ACCESS_TOKEN_PREFIX: &str = "atk_";

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
