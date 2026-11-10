//! Inbound authentication of `/mcp` (permission-model.md sections 2 and 10;
//! audit-log-format.md section 3): `Authorization: Bearer atk_...` and nothing
//! else, and no audit record for an authentication failure.

mod common;

use common::{PROTOCOL_VERSION, Seeded, Server, call_tool, seeded};
use serde_json::{Value, json};

/// A response as the tests read it: status, the headers the contract names and
/// the parsed JSON body (or `Value::Null` when the body is not JSON).
struct Response {
    status: u16,
    content_type: Option<String>,
    www_authenticate: Option<String>,
    session_id: Option<String>,
    text: String,
    body: Value,
}

fn header(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn post(
    client: &reqwest::Client,
    server: &Server,
    headers: &[(String, String)],
    body: Value,
) -> Response {
    let mut request = client
        .post(server.url("/mcp"))
        .header("Accept", "application/json, text/event-stream")
        .header("Content-Type", "application/json")
        .header("MCP-Protocol-Version", PROTOCOL_VERSION)
        .json(&body);
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let content_type = header(response.headers(), "content-type");
    let www_authenticate = header(response.headers(), "www-authenticate");
    let session_id = header(response.headers(), "mcp-session-id");
    let text = response.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Response {
        status,
        content_type,
        www_authenticate,
        session_id,
        text,
        body,
    }
}

/// One request with an optional bearer token, the way a client sends it.
async fn send(
    server: &Server,
    token: Option<&str>,
    extra: &[(&str, &str)],
    body: Value,
) -> Response {
    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(token) = token {
        headers.push(("Authorization".to_string(), format!("Bearer {token}")));
    }
    for (name, value) in extra {
        headers.push(((*name).to_string(), (*value).to_string()));
    }
    post(&reqwest::Client::new(), server, &headers, body).await
}

fn tools_list(id: u64) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": "tools/list" })
}

fn initialize(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "t", "version": "0" }
        }
    })
}

fn tool_names(body: &Value) -> Vec<String> {
    body["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools in response: {body}"))
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect()
}

/// The 401 of every rejected credential: `application/problem+json`, code
/// `unauthenticated`, and `WWW-Authenticate: Bearer`.
fn assert_unauthenticated(response: &Response) {
    assert_eq!(
        response.status, 401,
        "expected 401, got {}: {}",
        response.status, response.text
    );
    assert_eq!(
        response.content_type.as_deref(),
        Some("application/problem+json"),
        "body: {}",
        response.text
    );
    assert_eq!(
        response.body["code"], "unauthenticated",
        "body: {}",
        response.text
    );
    assert_eq!(
        response.www_authenticate.as_deref(),
        Some("Bearer"),
        "body: {}",
        response.text
    );
}

/// Creates a personal API token (`tok_...`) directly: the control plane's
/// `POST /api/v1/tokens` is not part of this worktree yet, and the test only
/// needs a row the authenticator can resolve. Only tests touch SQL.
async fn insert_personal_token(s: &Seeded) -> String {
    let secret = format!("tok_{}", "C".repeat(40));
    let hash = quoro_control_plane::store::sha256_hex(secret.as_bytes());
    sqlx::query(
        "INSERT INTO api_tokens \
         (id, user_id, name, token_hash, expires_at, last_used_at, created_at, revoked_at) \
         VALUES (?, ?, ?, ?, NULL, NULL, '2026-01-01T00:00:00.000Z', NULL)",
    )
    .bind(format!("tok_{}", &hash[..26]))
    .bind(&s.owner_id)
    .bind("t")
    .bind(&hash)
    .execute(s.server.state.store.pool())
    .await
    .unwrap();
    secret
}

// 1. A valid access token reaches `tools/list` and sees exactly the wired
// tools.
#[tokio::test]
async fn valid_access_token_lists_the_tools() {
    let s = seeded().await;
    let response = send(&s.server, Some(&s.access_token), &[], tools_list(1)).await;
    assert_eq!(response.status, 200, "{}", response.text);
    assert_eq!(response.body["jsonrpc"], "2.0");
    let expected: Vec<String> = quoro_mcp::tools::specs()
        .iter()
        .map(|spec| spec.name.to_string())
        .collect();
    assert_eq!(tool_names(&response.body), expected);
}

// 2. No credential at all is a 401 problem response with the bearer challenge.
#[tokio::test]
async fn no_credential_is_unauthenticated() {
    let s = seeded().await;
    let response = send(&s.server, None, &[], tools_list(1)).await;
    assert_unauthenticated(&response);
}

// 3. The owner's session cookie is not an MCP credential.
#[tokio::test]
async fn session_cookie_is_unauthenticated() {
    let s = seeded().await;
    let response = post(&s.owner, &s.server, &[], tools_list(1)).await;
    assert_unauthenticated(&response);
}

// 4. The bearer decides when both a cookie and an access token are present,
// and an MCP read is audited as the agent, never as the user.
#[tokio::test]
async fn access_token_wins_over_session_cookie() {
    let s = seeded().await;

    // The cookie-carrying client alone is rejected (the same as test 3).
    let cookie_only = post(&s.owner, &s.server, &[], tools_list(1)).await;
    assert_unauthenticated(&cookie_only);

    // The same cookie-carrying client with an Authorization header: reqwest
    // sends both, the middleware reads only the bearer, and the agent is the
    // caller.
    let response = post(
        &s.owner,
        &s.server,
        &[(
            "Authorization".to_string(),
            format!("Bearer {}", s.access_token),
        )],
        tools_list(2),
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text);
    let expected: Vec<String> = quoro_mcp::tools::specs()
        .iter()
        .map(|spec| spec.name.to_string())
        .collect();
    assert_eq!(tool_names(&response.body), expected);

    // The call itself may be allowed or refused by authorization; either way
    // an agent read is recorded and the record names the agent behind the
    // bearer, not the human whose cookie travelled with it.
    let _ = call_tool(&s.server, &s.access_token, "get_cluster", json!({})).await;

    let rows = s.server.audit_rows().await;
    let recorded: Vec<&Value> = rows
        .iter()
        .filter(|row| row["operation"] == "get_cluster" && row["channel"] == "mcp")
        .collect();
    assert_eq!(recorded.len(), 1, "rows: {rows:?}");
    assert_eq!(recorded[0]["actor_type"], "agent", "row: {}", recorded[0]);
    assert_eq!(
        recorded[0]["actor_id"],
        s.agent_id.as_str(),
        "row: {}",
        recorded[0]
    );
    assert_ne!(
        recorded[0]["actor_id"],
        s.owner_id.as_str(),
        "row: {}",
        recorded[0]
    );
}

// 5. A personal API token (`tok_`) authenticates the HTTP API but never MCP.
#[tokio::test]
async fn personal_api_token_is_unauthenticated_on_mcp() {
    let s = seeded().await;
    let token = insert_personal_token(&s).await;

    // The same secret works on the HTTP API through a client with no cookie.
    let api = reqwest::Client::new();
    let me = api
        .get(s.server.url("/api/v1/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(me.status(), 200, "me: {}", me.text().await.unwrap());

    let response = post(
        &api,
        &s.server,
        &[("Authorization".to_string(), format!("Bearer {token}"))],
        tools_list(1),
    )
    .await;
    assert_unauthenticated(&response);
}

// 6. An agent key (`akey_`) only mints access tokens; it is not an MCP
// credential.
#[tokio::test]
async fn agent_key_is_unauthenticated() {
    let s = seeded().await;
    let response = send(&s.server, Some(&s.agent_key), &[], tools_list(1)).await;
    assert_unauthenticated(&response);
}

// 7. An unknown `atk_` is unauthenticated.
#[tokio::test]
async fn unknown_access_token_is_unauthenticated() {
    let s = seeded().await;
    let unknown = format!("atk_{}", "A".repeat(40));
    let response = send(&s.server, Some(&unknown), &[], tools_list(1)).await;
    assert_unauthenticated(&response);
}

// 8. An expired access token is unauthenticated.
#[tokio::test]
async fn expired_access_token_is_unauthenticated() {
    let s = seeded().await;
    sqlx::query("UPDATE access_tokens SET expires_at = '2000-01-01T00:00:00.000Z'")
        .execute(s.server.state.store.pool())
        .await
        .unwrap();
    let response = send(&s.server, Some(&s.access_token), &[], tools_list(1)).await;
    assert_unauthenticated(&response);
}

// 9. How the scheme is matched: the control plane strips `Bearer ` exactly and
// then trims the token, so extra whitespace after the scheme is accepted while
// a lowercase `bearer` scheme is not.
#[tokio::test]
async fn scheme_matching_is_exact() {
    let s = seeded().await;
    let token = s.access_token.clone();

    let headers = vec![("Authorization".to_string(), format!("Bearer  {token}"))];
    let extra_space = post(&reqwest::Client::new(), &s.server, &headers, tools_list(1)).await;
    // `strip_prefix("Bearer ")` leaves the second space; `trim()` removes it, so
    // the lookup runs and the request is authenticated.
    assert_eq!(extra_space.status, 200, "extra space: {}", extra_space.text);
    assert_eq!(
        tool_names(&extra_space.body).len(),
        quoro_mcp::tools::specs().len()
    );

    let headers = vec![("Authorization".to_string(), format!("bearer {token}"))];
    let lowercase = post(&reqwest::Client::new(), &s.server, &headers, tools_list(2)).await;
    // The scheme is matched case-sensitively; lowercase does not authenticate.
    assert_unauthenticated(&lowercase);
}

// 10. rmcp refuses a request that carries an `Origin` header.
#[tokio::test]
async fn browser_origin_is_refused() {
    let s = seeded().await;
    let response = send(
        &s.server,
        Some(&s.access_token),
        &[("Origin", "https://evil.example")],
        tools_list(1),
    )
    .await;
    println!("origin refusal: {} {}", response.status, response.text);
    assert!(
        (400..500).contains(&response.status),
        "expected 4xx for a browser Origin, got {}: {}",
        response.status,
        response.text
    );
}

// 11. A disabled agent authenticates but is refused by authorization, and the
// refusal is audited with `channel: mcp` and `result: refused`.
#[tokio::test]
async fn disabled_agent_is_authenticated_then_refused_and_audited() {
    let s = seeded().await;
    let response = s
        .owner
        .patch(s.server.url(&format!("/api/v1/agents/{}", s.agent_id)))
        .json(&json!({ "disabled": true }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "update agent: {body}");
    assert_eq!(body["disabled"], true, "agent: {body}");

    // Authentication still passes: the token is valid and disabled is part of
    // the identity, not a reason to answer 401.
    let listed = send(&s.server, Some(&s.access_token), &[], tools_list(1)).await;
    assert_eq!(listed.status, 200, "{}", listed.text);

    let result = call_tool(&s.server, &s.access_token, "get_cluster", json!({})).await;
    assert_eq!(result["isError"], true, "result: {result}");
    assert_eq!(
        result["structuredContent"]["code"], "forbidden",
        "result: {result}"
    );
    assert_eq!(
        result["structuredContent"]["reason"], "agent_disabled",
        "result: {result}"
    );

    let rows = s.server.audit_rows().await;
    let recorded: Vec<&Value> = rows
        .iter()
        .filter(|row| {
            row["operation"] == "get_cluster"
                && row["channel"] == "mcp"
                && row["result"] == "refused"
        })
        .collect();
    assert_eq!(recorded.len(), 1, "rows: {rows:?}");
    assert_eq!(recorded[0]["actor_type"], "agent", "row: {}", recorded[0]);
    assert_eq!(
        recorded[0]["actor_id"],
        s.agent_id.as_str(),
        "row: {}",
        recorded[0]
    );
    assert_eq!(
        recorded[0]["result_detail"], "agent_disabled",
        "row: {}",
        recorded[0]
    );
}

// 12. Authentication failures are not audited (audit-log-format.md section 3):
// every rejected request above leaves the log exactly as long as it was.
#[tokio::test]
async fn authentication_failures_write_no_audit_record() {
    let s = seeded().await;

    // Prepare the token before taking the baseline, so its creation cannot be
    // mistaken for a rejected request's record.
    let tok = insert_personal_token(&s).await;

    let before = s.server.audit_rows().await.len();

    let unknown = format!("atk_{}", "B".repeat(40));
    assert_unauthenticated(&send(&s.server, None, &[], tools_list(1)).await);
    assert_unauthenticated(&post(&s.owner, &s.server, &[], tools_list(2)).await);
    assert_unauthenticated(&send(&s.server, Some(&s.agent_key), &[], tools_list(3)).await);
    assert_unauthenticated(&send(&s.server, Some(&unknown), &[], tools_list(4)).await);
    assert_unauthenticated(&send(&s.server, Some(&tok), &[], tools_list(5)).await);
    let headers = vec![(
        "Authorization".to_string(),
        format!("bearer {}", s.access_token),
    )];
    assert_unauthenticated(
        &post(&reqwest::Client::new(), &s.server, &headers, tools_list(6)).await,
    );

    let after = s.server.audit_rows().await.len();
    assert_eq!(
        before, after,
        "an authentication failure wrote an audit record"
    );
}

// 13. The legacy `initialize` handshake works, and no `Mcp-Session-Id` is
// needed for the next request.
#[tokio::test]
async fn initialize_needs_no_session() {
    let s = seeded().await;
    let response = send(&s.server, Some(&s.access_token), &[], initialize(1)).await;
    assert_eq!(response.status, 200, "{}", response.text);
    assert_eq!(
        response.body["result"]["serverInfo"]["name"], "quoro",
        "body: {}",
        response.text
    );
    println!("initialize session header: {:?}", response.session_id);

    // No session id is sent on the follow-up; it still works.
    let listed = send(&s.server, Some(&s.access_token), &[], tools_list(2)).await;
    assert_eq!(listed.status, 200, "{}", listed.text);
    assert_eq!(
        tool_names(&listed.body).len(),
        quoro_mcp::tools::specs().len()
    );
}

/// The limit of a request body: 256 KiB (README "Request body limit").
const LIMIT: usize = 262_144;

/// A `tools/call` body of exactly `len` bytes.
fn tool_call_body(len: usize) -> String {
    let empty = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_cluster","arguments":{"pad":""}}}"#;
    let body = empty.replace(
        r#""pad":"""#,
        &format!(r#""pad":"{}""#, "p".repeat(len - empty.len())),
    );
    assert_eq!(body.len(), len);
    body
}

/// `POST /mcp` with `body` as it is, and an optional bearer token.
async fn send_raw(server: &Server, token: Option<&str>, body: String) -> Response {
    let mut headers = Vec::new();
    if let Some(token) = token {
        headers.push(("Authorization".to_string(), format!("Bearer {token}")));
    }
    let mut request = reqwest::Client::new()
        .post(server.url("/mcp"))
        .header("Accept", "application/json, text/event-stream")
        .header("Content-Type", "application/json")
        .header("MCP-Protocol-Version", PROTOCOL_VERSION)
        .body(body);
    for (name, value) in &headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let content_type = header(response.headers(), "content-type");
    let www_authenticate = header(response.headers(), "www-authenticate");
    let session_id = header(response.headers(), "mcp-session-id");
    let text = response.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Response {
        status,
        content_type,
        www_authenticate,
        session_id,
        text,
        body,
    }
}

// 14. A body above the limit is answered 413 without being parsed, and an
// agent's call is audited with the request line as its operation, since the
// tool is named inside the unread body (README "Request body limit").
#[tokio::test]
async fn a_body_above_the_limit_is_refused_and_audited_as_post_mcp() {
    let s = seeded().await;
    let before = s.server.audit_rows().await.len();
    let response = send_raw(&s.server, Some(&s.access_token), tool_call_body(LIMIT + 1)).await;
    assert_eq!(response.status, 413, "{}", response.text);
    assert_eq!(
        response.content_type.as_deref(),
        Some("application/problem+json")
    );
    assert_eq!(
        response.body["code"], "payload_too_large",
        "{}",
        response.text
    );

    let rows = s.server.audit_rows().await;
    assert_eq!(rows.len(), before + 1);
    let record = rows.last().unwrap();
    assert_eq!(record["operation"], "POST /mcp", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["actor_type"], "agent", "{record}");
    assert_eq!(record["actor_id"], s.agent_id.as_str(), "{record}");
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "payload_too_large", "{record}");
    assert_eq!(record["params"], json!({}), "{record}");
    let (digest,): (Option<String>,) =
        sqlx::query_as("SELECT params_sha256 FROM audit_log ORDER BY seq DESC LIMIT 1")
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(digest, None);

    // Without a credential: the same answer, and no record.
    let response = send_raw(&s.server, None, tool_call_body(LIMIT + 1)).await;
    assert_eq!(response.status, 413, "{}", response.text);
    assert_eq!(
        response.body["code"], "payload_too_large",
        "{}",
        response.text
    );
    assert_eq!(s.server.audit_rows().await.len(), before + 1);

    // A body of exactly the limit is not refused for its size.
    let response = send_raw(&s.server, Some(&s.access_token), tool_call_body(LIMIT)).await;
    assert_ne!(response.status, 413, "{}", response.text);
    let refused: (i64,) =
        sqlx::query_as("SELECT count(*) FROM audit_log WHERE result_detail = 'payload_too_large'")
            .fetch_one(s.server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(refused.0, 1);
}

/// A primary that learned of a higher term answers `/mcp` with `503`
/// `not_primary` before it authenticates anything, as every other request
/// (`node-agent-protocol.md` section 7.4, ADR 0230).
#[tokio::test]
async fn a_demoted_primary_answers_mcp_with_not_primary_before_authentication() {
    let Seeded {
        server,
        access_token,
        ..
    } = seeded().await;
    server.state.hub.stale_term(Some(5)).await;
    let list = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
    for token in [Some(access_token.as_str()), None] {
        let res = reqwest::Client::new()
            .post(server.url("/mcp"))
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION)
            .header(
                "Authorization",
                token.map_or_else(String::new, |token| format!("Bearer {token}")),
            )
            .json(&list)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 503);
        let content_type = header(res.headers(), "content-type");
        assert!(header(res.headers(), "quoro-request-id").is_some());
        assert_eq!(content_type.as_deref(), Some("application/problem+json"));
        let body: Value = res.json().await.unwrap();
        assert_eq!(body["code"], "not_primary", "{body}");
    }
}
