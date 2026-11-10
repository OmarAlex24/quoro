//! Quoro's MCP server: rmcp's Streamable HTTP transport mounted at `/mcp` on
//! the control plane's listener (ADR 0054), exposing exactly the tools of the
//! "MCP tools" table in `docs/contracts/README.md`.
//!
//! The server is a thin adapter (ADR 0031, permission-model.md section 10).
//! For every tool call it checks the shape of the arguments, builds a
//! [`CallCtx`](quoro_control_plane::request::CallCtx) with `Channel::Mcp`, the
//! tool name as the operation and the tool's `justification` and
//! `incident_id`, and calls the same service function as the matching HTTP
//! endpoint. Authorization, the justification rule, grants, approvals and the
//! audit record all happen in that service function. This crate depends on no
//! database crate, so it cannot query SQL or build an audit record.
//!
//! Clients authenticate with `Authorization: Bearer atk_...` only: a session
//! cookie, a personal API token or an agent key is `unauthenticated` here
//! ([`auth`]).

pub mod args;
pub mod auth;
pub mod problem;
pub mod server;
pub mod tools;

use axum::Router;
use quoro_control_plane::AppState;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use std::sync::Arc;

/// Where the MCP endpoint is mounted (architecture.md, "MCP").
pub const MCP_PATH: &str = "/mcp";

/// The `/mcp` route: the bearer-token middleware in front of rmcp's
/// Streamable HTTP service. Merge it into the control plane's router.
pub fn router(state: AppState) -> Router {
    let config = StreamableHttpServerConfig::default()
        // No protocol sessions: every request stands alone (permission-model.md
        // section 10; the 2026-07-28 revision removes sessions).
        .with_legacy_session_mode(false)
        // Simple calls answer `application/json` instead of an SSE stream.
        .with_json_response(true)
        // The listener is reached by IP, by the primary's name and through
        // tunnels, so the Host allowlist cannot be known here. It guards
        // against DNS rebinding of a server that trusts ambient credentials;
        // this endpoint honours none (no cookie, only an explicit bearer
        // token), so a rebound page gains nothing.
        .disable_allowed_hosts()
        // With an empty list this refuses every request that carries an
        // `Origin` header: browsers have no business calling the endpoint.
        .enforce_origin_validation();
    let handler_state = state.clone();
    let service = StreamableHttpService::new(
        move || Ok(server::QuoroMcp::new(handler_state.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    // The body limit runs first, so a body above it is answered 413 whether
    // or not its credential is valid, as on the API.
    Router::new()
        .route_service(MCP_PATH, service)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_agent,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state,
            auth::limit_body,
        ))
}
