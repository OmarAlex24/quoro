//! MCP/HTTP parity: one call, one audit record, whichever adapter it arrives
//! through (D-0031; audit-log-format.md sections 2 and 3).
//!
//! Each pair below makes the same call twice with the same agent token, once
//! through `/mcp` and once through the HTTP API, and compares the two audit
//! records the service layer wrote. The adapters differ only in `channel` and
//! `operation`; everything the record says about the actor, the scope, the
//! target, the parameters, the justification and the outcome must match.
//!
//! The agent used here holds no grants yet (grants land in a later PR), so
//! every agent call is refused and audited. The tests are written against an
//! agent without grants, so they keep passing once grants exist.

mod common;

use common::{Seeded, Server, call_tool, rpc, seeded};
use serde_json::{Value, json};

/// A syntactically valid, nonexistent app id (`app_` plus 26 Crockford
/// characters).
const UNKNOWN_APP: &str = "app_0123456789ABCDEFGHJKMNPQRS";

/// A syntactically valid incident id (`inc_` plus 26 Crockford characters).
const INCIDENT: &str = "inc_0123456789ABCDEFGHJKMNPQRS";

/// Columns that differ by construction between an MCP call and the same HTTP
/// call: the log position, the wall clock, the correlation id, the channel and
/// the operation name.
const ADAPTER_FIELDS: [&str; 5] = ["seq", "time", "request_id", "channel", "operation"];

/// The audit record of one call appended after `before` rows existed.
async fn rows_after(server: &Server, before: usize, call: &str) -> Value {
    let rows = server.audit_rows().await;
    assert_eq!(
        rows.len(),
        before + 1,
        "{call}: expected exactly one new audit record, got {}",
        rows.len() - before
    );
    rows[before].clone()
}

/// A record with the adapter-specific fields removed, ready to compare.
fn comparable(row: &Value) -> Value {
    let mut map = row
        .as_object()
        .unwrap_or_else(|| panic!("audit record is not an object: {row}"))
        .clone();
    for field in ADAPTER_FIELDS {
        map.remove(field);
    }
    Value::Object(map)
}

/// One call over HTTP with the seeded agent's bearer token.
async fn http(
    seeded: &Seeded,
    method: reqwest::Method,
    path: &str,
    headers: &[(&str, &str)],
) -> (u16, Value) {
    let mut request = reqwest::Client::new()
        .request(method, seeded.server.url(path))
        .header("Authorization", format!("Bearer {}", seeded.access_token));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// Runs `tool` over MCP and `http_call` over the API, then asserts that the
/// two audit records agree on every column but the adapter ones.
///
/// Returns `(tool result, MCP record, HTTP record)`.
async fn compare_pair<F>(
    seeded: &Seeded,
    tool: &str,
    arguments: Value,
    http_method: &str,
    http_call: F,
) -> (Value, Value, Value)
where
    F: std::future::Future<Output = (u16, Value)>,
{
    let before = seeded.server.audit_rows().await.len();
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        tool,
        arguments.clone(),
    )
    .await;
    let mcp_record = rows_after(&seeded.server, before, tool).await;

    // The audit record and the tool result name the same call.
    assert_eq!(
        tool_result["_meta"]["quoro/request_id"], mcp_record["request_id"],
        "{tool}: the tool's _meta request id must match the audit record"
    );
    assert_eq!(mcp_record["channel"], "mcp", "{tool}");
    assert_eq!(mcp_record["operation"], tool, "{tool}");
    if let Some(justification) = arguments.get("justification") {
        assert_eq!(
            mcp_record["justification"], *justification,
            "{tool}: justification must be recorded"
        );
    }
    if let Some(incident_id) = arguments.get("incident_id") {
        assert_eq!(
            mcp_record["incident_id"], *incident_id,
            "{tool}: incident_id must be recorded"
        );
    }

    let before = before + 1;
    let (_status, _body) = http_call.await;
    let http_record = rows_after(&seeded.server, before, tool).await;
    assert_eq!(http_record["channel"], "api", "{tool}");
    let operation = http_record["operation"]
        .as_str()
        .unwrap_or_else(|| panic!("{tool}: HTTP operation is not a string"));
    assert!(
        operation.starts_with(http_method),
        "{tool}: expected an operation starting with {http_method}, got {operation}"
    );

    assert_eq!(
        comparable(&mcp_record),
        comparable(&http_record),
        "{tool}: the MCP and HTTP audit records differ"
    );

    (tool_result, mcp_record, http_record)
}

/// The problem body of a tool error result, for either channel.
fn problem<'a>(value: &'a Value, what: &str) -> &'a Value {
    value
        .get("structuredContent")
        .or_else(|| value.get("code").map(|_| value))
        .unwrap_or_else(|| panic!("{what}: no structured problem body in {value}"))
}

/// Asserts two problem bodies carry the same value for each field.
fn assert_same_fields(tool: &Value, http_body: &Value, fields: &[&str]) {
    for field in fields {
        assert_eq!(
            tool[field], http_body[field],
            "field {field} differs: tool {tool} vs http {http_body}"
        );
    }
}

#[tokio::test]
async fn mcp_and_http_write_the_same_audit_record() {
    let seeded = seeded().await;
    let app_id = seeded.app_id.clone();
    let project_id = seeded.project_id.clone();

    // get_cluster ↔ GET /api/v1/cluster
    compare_pair(&seeded, "get_cluster", json!({}), "GET", async {
        http(&seeded, reqwest::Method::GET, "/api/v1/cluster", &[]).await
    })
    .await;

    // get_app ↔ GET /api/v1/apps/{app_id}
    compare_pair(
        &seeded,
        "get_app",
        json!({ "app_id": app_id }),
        "GET",
        async {
            http(
                &seeded,
                reqwest::Method::GET,
                &format!("/api/v1/apps/{app_id}"),
                &[],
            )
            .await
        },
    )
    .await;

    // list_apps ↔ GET /api/v1/apps?project_id=..&limit=10
    compare_pair(
        &seeded,
        "list_apps",
        json!({ "project_id": project_id, "limit": 10 }),
        "GET",
        async {
            http(
                &seeded,
                reqwest::Method::GET,
                &format!("/api/v1/apps?project_id={project_id}&limit=10"),
                &[],
            )
            .await
        },
    )
    .await;

    // restart_app ↔ POST /api/v1/apps/{app_id}/restart, justification and
    // incident in the request headers.
    let (_result, mcp_record, http_record) = compare_pair(
        &seeded,
        "restart_app",
        json!({ "app_id": app_id, "justification": "J", "incident_id": INCIDENT }),
        "POST",
        async {
            http(
                &seeded,
                reqwest::Method::POST,
                &format!("/api/v1/apps/{app_id}/restart"),
                &[("Quoro-Justification", "J"), ("Quoro-Incident", INCIDENT)],
            )
            .await
        },
    )
    .await;
    for record in [&mcp_record, &http_record] {
        assert_eq!(record["justification"], "J", "restart justification");
        assert_eq!(record["incident_id"], INCIDENT, "restart incident");
        assert_eq!(record["scope"], "app:restart");
        assert_eq!(record["result"], "refused");
    }

    // delete_app ↔ DELETE /api/v1/apps/{app_id}
    compare_pair(
        &seeded,
        "delete_app",
        json!({ "app_id": app_id, "justification": "J" }),
        "DELETE",
        async {
            http(
                &seeded,
                reqwest::Method::DELETE,
                &format!("/api/v1/apps/{app_id}"),
                &[("Quoro-Justification", "J")],
            )
            .await
        },
    )
    .await;
}

#[tokio::test]
async fn refusals_map_to_contract_error_codes() {
    let seeded = seeded().await;
    let app_id = seeded.app_id.clone();

    // restart_app: forbidden / scope_missing on both channels.
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": app_id, "justification": "J" }),
    )
    .await;
    assert_eq!(tool_result["isError"], true);
    let tool_problem = problem(&tool_result, "restart_app").clone();
    assert_eq!(tool_problem["code"], "forbidden");
    assert_eq!(tool_problem["reason"], "scope_missing");
    assert_eq!(tool_problem["scope"], "app:restart");
    assert_eq!(tool_problem["status"], 403);

    let (status, http_body) = http(
        &seeded,
        reqwest::Method::POST,
        &format!("/api/v1/apps/{app_id}/restart"),
        &[("Quoro-Justification", "J")],
    )
    .await;
    assert_eq!(status, 403);
    assert_same_fields(
        &tool_problem,
        &http_body,
        &["code", "reason", "scope", "status"],
    );

    // delete_app: approval_required carrying scope and target on both channels.
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "delete_app",
        json!({ "app_id": app_id, "justification": "J" }),
    )
    .await;
    assert_eq!(tool_result["isError"], true);
    let tool_problem = problem(&tool_result, "delete_app").clone();
    assert_eq!(tool_problem["code"], "approval_required");
    assert_eq!(tool_problem["scope"], "app:delete");
    assert_eq!(
        tool_problem["target"],
        json!({ "type": "app", "id": app_id })
    );

    let (status, http_body) = http(
        &seeded,
        reqwest::Method::DELETE,
        &format!("/api/v1/apps/{app_id}"),
        &[("Quoro-Justification", "J")],
    )
    .await;
    assert_eq!(status, 403);
    assert_same_fields(
        &tool_problem,
        &http_body,
        &["code", "scope", "target", "status"],
    );

    // A malformed app id is rejected by the adapter's shape check, before the
    // service layer and therefore without an audit record.
    let before = seeded.server.audit_rows().await.len();
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_app",
        json!({ "app_id": "app_123" }),
    )
    .await;
    assert_eq!(tool_result["isError"], true);
    assert_eq!(
        problem(&tool_result, "get_app")["code"],
        "validation_failed"
    );
    assert_eq!(
        seeded.server.audit_rows().await.len(),
        before,
        "a shape rejection must not write an audit record"
    );

    // A well-formed but unknown id is authorized first (the service layer
    // refuses an agent without a grant before looking the app up) and the
    // refusal is audited.
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_app",
        json!({ "app_id": UNKNOWN_APP }),
    )
    .await;
    assert_eq!(tool_result["isError"], true);
    let tool_problem = problem(&tool_result, "get_app").clone();
    assert_eq!(tool_problem["code"], "forbidden");
    assert_eq!(tool_problem["reason"], "scope_missing");
    assert_eq!(tool_problem["scope"], "app:read");
    let record = rows_after(&seeded.server, before, "get_app").await;
    assert_eq!(record["result"], "refused");
    assert_eq!(record["target_id"], UNKNOWN_APP);

    // get_app_logs shape rejections.
    let before = seeded.server.audit_rows().await.len();
    for arguments in [
        json!({ "app_id": app_id, "tail": 0 }),
        json!({ "app_id": app_id, "tail": 1.5 }),
    ] {
        let tool_result = call_tool(
            &seeded.server,
            &seeded.access_token,
            "get_app_logs",
            arguments.clone(),
        )
        .await;
        assert_eq!(tool_result["isError"], true, "{arguments}");
        assert_eq!(
            problem(&tool_result, "get_app_logs")["code"],
            "validation_failed",
            "{arguments}"
        );
    }
    assert_eq!(
        seeded.server.audit_rows().await.len(),
        before,
        "shape rejections must not write audit records"
    );

    // An unknown argument is a shape rejection that names it.
    let before = seeded.server.audit_rows().await.len();
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_app_logs",
        json!({ "app_id": app_id, "foo": "bar" }),
    )
    .await;
    assert_eq!(tool_result["isError"], true);
    let tool_problem = problem(&tool_result, "get_app_logs").clone();
    assert_eq!(tool_problem["code"], "validation_failed");
    assert!(
        tool_problem["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("foo"),
        "expected the detail to mention foo: {tool_problem}"
    );
    assert_eq!(seeded.server.audit_rows().await.len(), before);
}

#[tokio::test]
async fn a_mutating_call_without_justification_reaches_the_service_layer_and_is_audited() {
    let seeded = seeded().await;
    let app_id = seeded.app_id.clone();

    let before = seeded.server.audit_rows().await.len();
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": app_id }),
    )
    .await;

    // The adapter lets the call through: the justification rule belongs to
    // the service layer. Today an agent without `app:restart` is refused with
    // `forbidden`; once grants land, an agent holding `app:restart` gets
    // `validation_failed` here from the service layer, still audited.
    let tool_problem = problem(&tool_result, "restart_app");
    assert_eq!(tool_result["isError"], true);
    assert_eq!(tool_problem["code"], "forbidden");
    assert_eq!(tool_problem["reason"], "scope_missing");

    let record = rows_after(&seeded.server, before, "restart_app").await;
    assert_eq!(record["channel"], "mcp");
    assert_eq!(record["operation"], "restart_app");
    assert_eq!(record["result"], "refused");
    assert!(record["justification"].is_null(), "{record}");
}

#[tokio::test]
async fn an_unknown_tool_is_a_jsonrpc_error() {
    let seeded = seeded().await;

    let name = "nope";
    let (status, body) = rpc(
        &seeded.server,
        Some(&seeded.access_token),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": name, "arguments": {} },
        }),
    )
    .await;
    assert_eq!(status, 200, "{name}: {body}");
    assert!(
        body.get("result").is_none(),
        "{name}: expected a JSON-RPC error, got a result: {body}"
    );
    assert_eq!(
        body["error"]["code"], -32602,
        "{name}: expected -32602, got {body}"
    );
}
