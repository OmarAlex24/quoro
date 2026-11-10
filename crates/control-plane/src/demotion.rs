// SPDX-License-Identifier: AGPL-3.0-only
//! The demoted responder (`node-agent-protocol.md` section 7.4, ADR 0230).
//!
//! A primary that learned of a higher term answers every request on its
//! listener with `503` and the `NotPrimary` problem body (`code`
//! `not_primary`, `openapi.yaml`): the API, the panel and its static assets,
//! MCP, setup, health and the node session upgrade. It answers before any
//! authentication. A node that enrolled with the new primary after the
//! promotion holds a credential this host never saw, and a `401` at the
//! upgrade would stop that node for good, while a `503` sends it on to its
//! next primary address (section 4.4).
//!
//! A primary demoted while it runs keeps its routes behind [`guard`], the
//! outermost layer of the whole service. A primary that starts demoted serves
//! [`responder`] alone.

use crate::error::{ApiError, Code};
use crate::store::Store;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use quoro_types::{Prefix, new_id};

/// The `detail` of the `503`.
pub const NOT_PRIMARY_DETAIL: &str =
    "this primary learned of a higher term and is demoted; use the primary of the newest term";

/// The `503` `not_primary` problem, with its own `Quoro-Request-Id`: the
/// guard answers before the request-id layer of the API runs.
#[must_use]
pub fn not_primary() -> Response {
    let mut response = ApiError::new(Code::NotPrimary, NOT_PRIMARY_DETAIL).into_response();
    if let Ok(value) = HeaderValue::from_str(&new_id(Prefix::Request)) {
        response.headers_mut().insert("quoro-request-id", value);
    }
    response
}

/// Wraps the whole service, API, MCP and the panel fallback, so that once
/// `store` reads demoted every request is answered with [`not_primary`]
/// before any route, extractor or authentication runs. Apply it last, after
/// every route and the fallback are in place.
pub fn guard(service: Router, store: Store) -> Router {
    service.layer(axum::middleware::from_fn_with_state(store, demoted_guard))
}

async fn demoted_guard(State(store): State<Store>, request: Request, next: Next) -> Response {
    if store.is_demoted() {
        return not_primary();
    }
    next.run(request).await
}

/// The whole service of a primary that starts demoted: every request, on any
/// path and with any method, gets [`not_primary`].
pub fn responder() -> Router {
    Router::new().fallback(|| async { not_primary() })
}
