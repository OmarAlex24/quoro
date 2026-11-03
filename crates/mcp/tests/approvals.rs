//! The two MCP approval tools (permission-model.md sections 9 and 10, D-0030,
//! D-0033): `request_approval` files a just-in-time request, `get_approval`
//! reads it back. Deciding is human-only and has no MCP tool.
//!
//! Each test issues grants with the owner's session (`POST /api/v1/grants`),
//! calls `/mcp` with the agent's access token and reads the audit log with SQL.

mod common;

use common::{Seeded, call_tool, rpc, seeded};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The structured problem body of a tool result.
fn problem_body<'a>(result: &'a Value, what: &str) -> &'a Value {
    result
        .get("structuredContent")
        .unwrap_or_else(|| panic!("{what}: no structuredContent in {result}"))
}

fn is_error(result: &Value) -> bool {
    result["isError"] == Value::Bool(true)
}

/// Issues a grant with the owner's session.
async fn create_grant(seeded: &Seeded, scopes: &[&str], selector: Value) -> Value {
    let res = seeded
        .owner
        .post(seeded.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": seeded.agent_id,
            "scopes": scopes,
            "selector": selector,
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create grant: {body}");
    body
}

/// The agent's `delete_app` with a justification; refused until an approval
/// grant exists.
async fn agent_delete(seeded: &Seeded, justification: &str) -> Value {
    call_tool(
        &seeded.server,
        &seeded.access_token,
        "delete_app",
        json!({ "app_id": seeded.app_id, "justification": justification }),
    )
    .await
}

/// Files the approval for the seeded app's deletion through the MCP tool.
async fn file_approval(seeded: &Seeded, target: &Value, justification: &str) -> Value {
    call_tool(
        &seeded.server,
        &seeded.access_token,
        "request_approval",
        json!({
            "scope": "app:delete",
            "target": target,
            "operation": "delete_app",
            "params": { "app_id": seeded.app_id },
            "justification": justification,
        }),
    )
    .await
}

fn approval_id_of(result: &Value) -> String {
    result["structuredContent"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("no approval id in {result}"))
        .to_string()
}

/// The one audit record with `operation` and `channel: mcp`.
fn mcp_record(rows: &[Value], operation: &str) -> Value {
    rows.iter()
        .find(|row| row["operation"] == operation && row["channel"] == "mcp")
        .unwrap_or_else(|| panic!("no mcp {operation} record in {rows:?}"))
        .clone()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn request_approval_files_a_pending_request_and_get_approval_reads_it_back() {
    let seeded = seeded().await;
    create_grant(
        &seeded,
        &["approval:create", "approval:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    // 1. The destructive call is refused with `approval_required` and the
    // scope and target the request must name.
    let refusal = agent_delete(&seeded, "the app is obsolete").await;
    assert!(is_error(&refusal), "delete_app: {refusal}");
    let problem = problem_body(&refusal, "delete_app");
    assert_eq!(problem["code"], "approval_required", "{problem}");
    assert_eq!(problem["scope"], "app:delete", "{problem}");
    let target = problem["target"].clone();
    assert_eq!(
        target,
        json!({ "type": "app", "id": seeded.app_id }),
        "{problem}"
    );

    // 2. The agent files the request through MCP. One `justification` feeds
    // both the header equivalent and the body field.
    let justification = "the app is obsolete and its data must go";
    let filed = file_approval(&seeded, &target, justification).await;
    assert!(!is_error(&filed), "request_approval: {filed}");
    let approval = &filed["structuredContent"];
    assert_eq!(approval["status"], "pending", "{approval}");
    assert_eq!(approval["agent_id"], seeded.agent_id, "{approval}");
    assert_eq!(approval["scope"], "app:delete", "{approval}");
    assert_eq!(approval["target"], target, "{approval}");
    assert_eq!(approval["operation"], "delete_app", "{approval}");
    assert_eq!(
        approval["params"],
        json!({ "app_id": seeded.app_id }),
        "{approval}"
    );
    assert_eq!(approval["justification"], justification, "{approval}");
    assert!(approval["grant_id"].is_null(), "{approval}");
    let approval_id = approval_id_of(&filed);

    // 3. `get_approval` returns the same request.
    let read = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_approval",
        json!({ "approval_id": approval_id }),
    )
    .await;
    assert!(!is_error(&read), "get_approval: {read}");
    assert_eq!(
        read["structuredContent"], *approval,
        "the MCP read must return the filed request"
    );

    // 4. Both calls are audited as MCP agent calls that succeeded.
    let rows = seeded.server.audit_rows().await;
    let request_record = mcp_record(&rows, "request_approval");
    assert_eq!(request_record["result"], "ok", "{request_record}");
    assert_eq!(
        request_record["actor_id"], seeded.agent_id,
        "{request_record}"
    );
    let read_record = mcp_record(&rows, "get_approval");
    assert_eq!(read_record["result"], "ok", "{read_record}");
}

#[tokio::test]
async fn an_approved_request_lets_the_delete_through_and_the_audit_names_it() {
    let seeded = seeded().await;
    create_grant(
        &seeded,
        &["approval:create", "approval:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    let refusal = agent_delete(&seeded, "cleanup").await;
    assert!(is_error(&refusal), "delete_app: {refusal}");
    let target = problem_body(&refusal, "delete_app")["target"].clone();

    let filed = file_approval(&seeded, &target, "cleanup").await;
    assert!(!is_error(&filed), "request_approval: {filed}");
    let approval_id = approval_id_of(&filed);

    // A human decides; there is no MCP tool for this.
    let res = seeded
        .owner
        .post(
            seeded
                .server
                .url(&format!("/api/v1/approvals/{approval_id}/approve")),
        )
        .send()
        .await
        .unwrap();
    let status = res.status();
    let approved: Value = res.json().await.unwrap();
    assert_eq!(status, 200, "approve: {approved}");
    assert_eq!(approved["status"], "approved", "{approved}");
    assert_eq!(approved["id"], approval_id, "{approved}");

    // The same call with the same params now runs.
    let deleted = agent_delete(&seeded, "approved cleanup").await;
    assert!(!is_error(&deleted), "delete_app after approval: {deleted}");
    assert_eq!(
        deleted["structuredContent"]["id"], seeded.app_id,
        "the deleted app is the one the approval named"
    );

    let rows = seeded.server.audit_rows().await;
    let record = rows
        .iter()
        .find(|row| {
            row["operation"] == "delete_app" && row["channel"] == "mcp" && row["result"] == "ok"
        })
        .unwrap_or_else(|| panic!("no ok delete_app record in {rows:?}"));
    assert_eq!(record["approval_id"], approval_id, "{record}");
    assert!(!record["grant_id"].is_null(), "{record}");
    assert_eq!(record["on_behalf_of"], seeded.owner_id, "{record}");
}

#[tokio::test]
async fn a_request_without_justification_or_with_an_unknown_argument_is_refused_unaudited() {
    let seeded = seeded().await;
    create_grant(
        &seeded,
        &["approval:create", "approval:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let target = json!({ "type": "app", "id": seeded.app_id });

    // A missing justification is refused before the service, as a body without
    // one would be over HTTP.
    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "request_approval",
        json!({
            "scope": "app:delete",
            "target": target,
            "operation": "delete_app",
            "params": { "app_id": seeded.app_id },
        }),
    )
    .await;
    assert!(is_error(&result), "request_approval: {result}");
    let problem = problem_body(&result, "request_approval without justification");
    assert_eq!(problem["code"], "validation_failed", "{problem}");
    assert_eq!(problem["detail"], "justification is required", "{problem}");
    assert_eq!(
        seeded.server.audit_rows().await.len(),
        before,
        "the adapter refuses before the service, so nothing is audited"
    );

    // An unknown argument is a shape error, never a body field.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "request_approval",
        json!({
            "scope": "app:delete",
            "target": target,
            "operation": "delete_app",
            "params": { "app_id": seeded.app_id },
            "justification": "cleanup",
            "decide": true,
        }),
    )
    .await;
    assert!(is_error(&result), "request_approval: {result}");
    let problem = problem_body(&result, "request_approval with an unknown argument");
    assert_eq!(problem["code"], "validation_failed", "{problem}");
}

#[tokio::test]
async fn no_mcp_tool_decides_an_approval() {
    let seeded = seeded().await;
    let (status, body) = rpc(
        &seeded.server,
        Some(&seeded.access_token),
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    assert_eq!(status, 200, "tools/list: {body}");
    let tools = body["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools in {body}"));
    assert!(!tools.is_empty(), "{body}");
    for tool in tools {
        let name = tool["name"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        for forbidden in ["approve", "deny", "decide"] {
            assert!(
                !name.contains(forbidden),
                "{name} exposes an approval decision (permission-model.md section 10)"
            );
        }
    }
}
