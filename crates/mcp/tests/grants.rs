//! The MCP server with an agent that holds grants (permission-model.md
//! sections 5-7, 10 and 11).
//!
//! Each test issues grants with the owner's session (`POST /api/v1/grants`),
//! then calls `/mcp` with the agent's access token and reads the audit log
//! with SQL. The MCP adapter is a thin layer over the same service functions
//! the HTTP API uses, so these tests pin the behaviour the runtime contract
//! promises: an allowed read returns the HTTP body, a refusal names a
//! contract reason and is audited, a mutating call needs a justification, and
//! the selector decides which resources and which list rows a grant reaches.

mod common;

use common::{Seeded, Server, call_tool, mint, seeded};
use serde_json::{Map, Value, json};

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// A syntactically valid incident id (`inc_` plus 26 Crockford characters).
const INCIDENT: &str = "inc_0123456789ABCDEFGHJKMNPQRS";

/// The reasons permission-model.md section 7.4 allows on a `forbidden`.
const CONTRACT_REASONS: [&str; 7] = [
    "scope_missing",
    "selector_mismatch",
    "grant_expired",
    "grant_revoked",
    "grant_exhausted",
    "human_only",
    "agent_disabled",
];

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

/// The one audit record appended after `before` rows existed.
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

/// Issues a grant with the owner's session.
async fn create_grant(seeded: &Seeded, agent_id: &str, scopes: &[&str], selector: Value) -> Value {
    let res = seeded
        .owner
        .post(seeded.server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
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

/// A second app in the seeded project, created by the owner.
async fn create_app(seeded: &Seeded, name: &str) -> String {
    let res = seeded
        .owner
        .post(seeded.server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": seeded.project_id,
            "name": name,
            "image": IMAGE,
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create app {name}: {body}");
    body["id"].as_str().unwrap().to_string()
}

/// A fresh agent owned by the seeded owner and an access token minted for it.
async fn create_agent_token(seeded: &Seeded, name: &str) -> (String, String) {
    let res = seeded
        .owner
        .post(seeded.server.url("/api/v1/agents"))
        .json(&json!({
            "name": name,
            "model": "test-model",
            "owner_user_id": seeded.owner_id,
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create agent {name}: {body}");
    let agent_id = body["id"].as_str().unwrap().to_string();

    let res = seeded
        .owner
        .post(
            seeded
                .server
                .url(&format!("/api/v1/agents/{agent_id}/keys")),
        )
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create key for {name}: {body}");
    let key = body["key"].as_str().unwrap().to_string();
    let token = mint(&seeded.server, &key).await;
    (agent_id, token)
}

/// `GET /api/v1/apps/{app_id}` with an agent's bearer token.
async fn agent_get_app(seeded: &Seeded, token: &str, app_id: &str) -> (u16, Value) {
    let res = reqwest::Client::new()
        .get(seeded.server.url(&format!("/api/v1/apps/{app_id}")))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body = res.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// The app's `restart_generation` as the owner sees it (a human's read is not
/// audited).
async fn restart_generation(seeded: &Seeded, app_id: &str) -> i64 {
    let app: Value = seeded
        .owner
        .get(seeded.server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    app["restart_generation"].as_i64().unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_allowed_read_returns_the_http_body_and_is_audited_with_the_grant() {
    let seeded = seeded().await;
    let grant = create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_app",
        json!({ "app_id": seeded.app_id }),
    )
    .await;
    assert!(!is_error(&result), "get_app: {result}");

    // One agent read, one audit record: the grant that lent the authority,
    // the human it was lent on behalf of, and the model behind the agent.
    let record = rows_after(&seeded.server, before, "get_app").await;
    assert_eq!(record["result"], "ok", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["operation"], "get_app", "{record}");
    assert_eq!(record["grant_id"], grant_id, "{record}");
    assert_eq!(record["on_behalf_of"], seeded.owner_id, "{record}");
    assert_eq!(record["agent_model"], "test-model", "{record}");
    assert_eq!(record["actor_id"], seeded.agent_id, "{record}");
    assert_eq!(record["actor_type"], "agent", "{record}");

    // The tool's structured content is exactly the HTTP body of the same read
    // with the same token (one service function, two adapters).
    let (status, body) = agent_get_app(&seeded, &seeded.access_token, &seeded.app_id).await;
    assert_eq!(status, 200, "GET app: {body}");
    assert_eq!(
        result["structuredContent"], body,
        "the MCP read must return the HTTP body"
    );

    // The HTTP read that followed is audited too, under `channel: api`.
    let rows = seeded.server.audit_rows().await;
    assert_eq!(rows.len(), before + 2);
    assert_eq!(rows[before + 1]["channel"], "api");
    assert_eq!(rows[before + 1]["operation"], "GET /api/v1/apps/{app_id}");
}

#[tokio::test]
async fn a_read_only_grant_cannot_restart() {
    let seeded = seeded().await;
    create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": seeded.app_id, "justification": "it looks down" }),
    )
    .await;
    assert!(is_error(&result), "restart_app: {result}");

    let problem = problem_body(&result, "restart_app");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    let reason = problem["reason"]
        .as_str()
        .unwrap_or_else(|| panic!("no reason in {problem}"));
    println!("read-only grant restart refusal reason: {reason}");
    assert!(
        CONTRACT_REASONS.contains(&reason),
        "reason {reason} is not one of the section 7.4 reasons"
    );
    // Section 7.4 row 2: the grant exists but does not carry `app:restart`.
    assert_eq!(reason, "scope_missing", "{problem}");

    let record = rows_after(&seeded.server, before, "restart_app").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["result_detail"], reason, "{record}");
    assert_eq!(record["scope"], "app:restart", "{record}");
    assert!(record["change_seq"].is_null(), "{record}");
}

#[tokio::test]
async fn restart_under_a_grant_needs_a_justification_and_records_it() {
    let seeded = seeded().await;
    let grant = create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read", "app:restart"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();

    // (a) A mutating agent call without a justification reaches the service
    // layer, which refuses it with `validation_failed` and audits it.
    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": seeded.app_id }),
    )
    .await;
    assert!(
        is_error(&result),
        "restart_app without justification: {result}"
    );
    let problem = problem_body(&result, "restart_app without justification");
    assert_eq!(problem["code"], "validation_failed", "{problem}");

    let record = rows_after(&seeded.server, before, "restart_app").await;
    assert_eq!(record["operation"], "restart_app", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_ne!(record["result"], "ok", "{record}");
    assert!(record["justification"].is_null(), "{record}");

    // (b) With a justification the restart runs and every agent column of
    // section 11 is filled. An incident id is only accepted when it names a
    // real incident; the contract has no way to create one by hand yet, so
    // the test adapts if the service rejects the synthetic id.
    let generation_before = restart_generation(&seeded, &seeded.app_id).await;
    let justification = "health check failed";
    let mut result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({
            "app_id": seeded.app_id,
            "justification": justification,
            "incident_id": INCIDENT,
        }),
    )
    .await;
    let mut expected_incident = Some(INCIDENT);
    if is_error(&result) {
        let problem = problem_body(&result, "restart_app with incident");
        println!("incident_id {INCIDENT} was rejected: {problem}");
        assert_eq!(
            problem["code"], "validation_failed",
            "an unknown incident must be validation_failed: {problem}"
        );
        let detail = problem["detail"].as_str().unwrap_or_default();
        assert!(
            detail.contains("incident") && detail.contains("does not exist"),
            "unexpected rejection: {problem}"
        );
        expected_incident = None;
        result = call_tool(
            &seeded.server,
            &seeded.access_token,
            "restart_app",
            json!({ "app_id": seeded.app_id, "justification": justification }),
        )
        .await;
    }
    assert!(!is_error(&result), "restart_app: {result}");
    assert_eq!(
        result["structuredContent"]["restart_generation"],
        generation_before + 1,
        "the restart bumps the generation: {result}"
    );

    let rows = seeded.server.audit_rows().await;
    let record = rows
        .iter()
        .rev()
        .find(|row| {
            row["channel"] == "mcp" && row["operation"] == "restart_app" && row["result"] == "ok"
        })
        .unwrap_or_else(|| panic!("no ok restart record in {rows:?}"));
    assert_eq!(record["justification"], justification, "{record}");
    assert_eq!(record["grant_id"], grant_id, "{record}");
    assert_eq!(record["on_behalf_of"], seeded.owner_id, "{record}");
    assert_eq!(record["agent_model"], "test-model", "{record}");
    assert_eq!(record["scope"], "app:restart", "{record}");
    assert!(!record["change_seq"].is_null(), "{record}");
    assert_eq!(
        record["incident_id"].as_str().map(str::to_string),
        expected_incident.map(str::to_string),
        "{record}"
    );
}

#[tokio::test]
async fn mcp_and_http_allowed_restarts_write_equivalent_audit_records() {
    let seeded = seeded().await;
    let grant = create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read", "app:restart"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let justification = "the health check failed";

    let before = seeded.server.audit_rows().await.len();
    let tool_result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": seeded.app_id, "justification": justification }),
    )
    .await;
    assert!(!is_error(&tool_result), "MCP restart: {tool_result}");
    let mcp_record = rows_after(&seeded.server, before, "restart_app").await;

    let res = reqwest::Client::new()
        .post(
            seeded
                .server
                .url(&format!("/api/v1/apps/{}/restart", seeded.app_id)),
        )
        .header("Authorization", format!("Bearer {}", seeded.access_token))
        .header("Quoro-Justification", justification)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 202, "HTTP restart: {body}");
    let http_record = rows_after(&seeded.server, before + 1, "HTTP restart").await;

    for record in [&mcp_record, &http_record] {
        assert_eq!(record["result"], "ok", "{record}");
        assert_eq!(record["grant_id"], grant_id, "{record}");
        assert_eq!(record["scope"], "app:restart", "{record}");
        assert!(!record["change_seq"].is_null(), "{record}");
    }
    assert_ne!(
        mcp_record["change_seq"], http_record["change_seq"],
        "each restart is its own change"
    );

    // The adapters differ only in the columns that name the adapter.
    let adapter_fields = [
        "seq",
        "time",
        "request_id",
        "channel",
        "operation",
        "change_seq",
    ];
    let comparable = |row: &Value| {
        let mut map: Map<String, Value> = row.as_object().unwrap().clone();
        for field in adapter_fields {
            map.remove(field);
        }
        Value::Object(map)
    };
    assert_eq!(
        comparable(&mcp_record),
        comparable(&http_record),
        "the MCP and HTTP restart records must agree everywhere else"
    );
    assert_eq!(mcp_record["channel"], "mcp");
    assert_eq!(http_record["channel"], "api");
}

#[tokio::test]
async fn a_grant_on_another_app_is_a_selector_mismatch() {
    let seeded = seeded().await;
    let other_app = create_app(&seeded, "other").await;
    create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:restart"],
        json!({ "apps": [&other_app] }),
    )
    .await;

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "restart_app",
        json!({ "app_id": seeded.app_id, "justification": "restart A" }),
    )
    .await;
    assert!(is_error(&result), "restart_app: {result}");
    let problem = problem_body(&result, "restart_app");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    let reason = problem["reason"]
        .as_str()
        .unwrap_or_else(|| panic!("no reason in {problem}"));
    println!("cross-app grant refusal reason: {reason}");
    assert!(
        CONTRACT_REASONS.contains(&reason),
        "reason {reason} is not one of the section 7.4 reasons"
    );
    assert_eq!(reason, "selector_mismatch", "{problem}");

    let record = rows_after(&seeded.server, before, "restart_app").await;
    assert_ne!(record["result"], "ok", "{record}");
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], reason, "{record}");
    assert_eq!(record["target_id"], seeded.app_id, "{record}");
    assert!(record["change_seq"].is_null(), "{record}");
}

#[tokio::test]
async fn expired_and_revoked_grants_are_refused_with_their_reason() {
    let seeded = seeded().await;

    // Row 3: the only grant with the scope has passed `expires_at`. The test
    // moves the clock by moving the grant's expiry, which is the column
    // `authz::GrantRow::unusable` reads.
    let (expired_agent, expired_token) = create_agent_token(&seeded, "expired").await;
    let expired_grant = create_grant(
        &seeded,
        &expired_agent,
        &["app:restart"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let expired_grant_id = expired_grant["id"].as_str().unwrap().to_string();
    sqlx::query("UPDATE grants SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?")
        .bind(&expired_grant_id)
        .execute(seeded.server.state.store.pool())
        .await
        .unwrap();

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &expired_token,
        "restart_app",
        json!({ "app_id": seeded.app_id, "justification": "expired grant" }),
    )
    .await;
    assert!(is_error(&result), "expired restart: {result}");
    let problem = problem_body(&result, "expired restart");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    assert_eq!(problem["reason"], "grant_expired", "{problem}");
    let record = rows_after(&seeded.server, before, "expired restart").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "grant_expired", "{record}");

    // Row 4: the only grant with the scope was revoked by the owner. A
    // separate agent keeps the two reasons from collapsing into the first
    // one of section 7.4's order.
    let (revoked_agent, revoked_token) = create_agent_token(&seeded, "revoked").await;
    let revoked_grant = create_grant(
        &seeded,
        &revoked_agent,
        &["app:restart"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let revoked_grant_id = revoked_grant["id"].as_str().unwrap().to_string();
    let res = seeded
        .owner
        .post(
            seeded
                .server
                .url(&format!("/api/v1/grants/{revoked_grant_id}/revoke")),
        )
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 200, "revoke: {body}");
    assert_eq!(body["status"], "revoked", "{body}");

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &revoked_token,
        "restart_app",
        json!({ "app_id": seeded.app_id, "justification": "revoked grant" }),
    )
    .await;
    assert!(is_error(&result), "revoked restart: {result}");
    let problem = problem_body(&result, "revoked restart");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    assert_eq!(problem["reason"], "grant_revoked", "{problem}");
    let record = rows_after(&seeded.server, before, "revoked restart").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "grant_revoked", "{record}");
}

#[tokio::test]
async fn delete_needs_an_approval_even_with_other_grants() {
    let seeded = seeded().await;
    create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read", "app:restart"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "delete_app",
        json!({ "app_id": seeded.app_id, "justification": "the app is obsolete" }),
    )
    .await;
    assert!(is_error(&result), "delete_app: {result}");
    let problem = problem_body(&result, "delete_app");
    assert_eq!(problem["code"], "approval_required", "{problem}");
    assert_eq!(problem["scope"], "app:delete", "{problem}");
    assert_eq!(
        problem["target"],
        json!({ "type": "app", "id": seeded.app_id }),
        "{problem}"
    );

    let record = rows_after(&seeded.server, before, "delete_app").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["result_detail"], "approval_required", "{record}");
    assert_eq!(record["scope"], "app:delete", "{record}");
    assert!(record["change_seq"].is_null(), "{record}");

    // Nothing was deleted.
    let (status, _) = agent_get_app(&seeded, &seeded.access_token, &seeded.app_id).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn list_results_are_filtered_by_the_grant_selector() {
    let seeded = seeded().await;
    let other_app = create_app(&seeded, "other").await;
    create_grant(
        &seeded,
        &seeded.agent_id,
        &["app:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    let result = call_tool(&seeded.server, &seeded.access_token, "list_apps", json!({})).await;
    assert!(!is_error(&result), "list_apps: {result}");
    let items = result["structuredContent"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items in {result}"));
    let ids: Vec<&str> = items.iter().filter_map(|app| app["id"].as_str()).collect();
    assert_eq!(
        ids,
        vec![seeded.app_id.as_str()],
        "the selector admits only the granted app (other app: {other_app})"
    );
    assert!(!ids.contains(&other_app.as_str()));
}
