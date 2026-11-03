//! Maps a service-layer [`ApiError`] to an MCP tool error result.
//!
//! A refusal or a failed call is a tool result with `isError: true`, not a
//! JSON-RPC error, so the agent reads why it was refused. The structured
//! content is the same problem body the HTTP API answers (README "Errors",
//! `openapi.yaml` `Problem`): `code`, and when present `reason`, `scope`,
//! `target` and `detail`.

use quoro_control_plane::error::ApiError;
use rmcp::model::CallToolResult;
use serde_json::{Map, Value, json};

/// The problem body of `error`, as the HTTP API would answer it.
#[must_use]
pub fn problem_body(error: &ApiError) -> Value {
    let status = error.code.status();
    let mut body = Map::new();
    body.insert("type".to_string(), json!("about:blank"));
    body.insert(
        "title".to_string(),
        json!(status.canonical_reason().unwrap_or("Error")),
    );
    body.insert("status".to_string(), json!(status.as_u16()));
    if let Some(detail) = &error.detail {
        body.insert("detail".to_string(), json!(detail));
    }
    body.insert("code".to_string(), json!(error.code.as_str()));
    if let Some(reason) = error.reason {
        body.insert("reason".to_string(), json!(reason.as_str()));
    }
    if let Some(scope) = &error.scope {
        body.insert("scope".to_string(), json!(scope));
    }
    if let Some(target) = &error.target {
        body.insert(
            "target".to_string(),
            json!({ "type": target.kind, "id": target.id }),
        );
    }
    Value::Object(body)
}

/// The tool error result for `error`.
#[must_use]
pub fn tool_error(error: &ApiError) -> CallToolResult {
    CallToolResult::structured_error(problem_body(error))
}
