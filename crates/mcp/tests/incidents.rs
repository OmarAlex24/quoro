//! The MCP incident, event and audit tools (permission-model.md sections 5.1,
//! 8.2, 8.3 and 11, D-0031, D-0032).
//!
//! Each test opens a real incident the way the control plane does: the owner
//! enrolls a node, the node opens its websocket session and sends an
//! `app.down` event. The owner then issues grants (`POST /api/v1/grants`) and
//! the agent calls `/mcp` with its access token. Every read is compared with
//! the owner's HTTP answer, and the audit log is read with SQL to pin the
//! record of a resolve and of a refusal.

mod common;

use common::{Seeded, Server, call_tool, seeded};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, EventBody, Kvm as WireKvm, NodeMessage};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

// ---------------------------------------------------------------------------
// Node session helpers, copied from `crates/control-plane/tests/incidents.rs`
// and adapted to the MCP harness's `Server`.
// ---------------------------------------------------------------------------

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[9u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
}

/// `127.0.0.1:port` of the harness's ephemeral listener.
fn address(server: &Server) -> &str {
    server
        .base
        .strip_prefix("http://")
        .expect("the harness listens on plain HTTP")
}

fn ws_url(server: &Server) -> String {
    format!("ws://{}/api/v1/nodes/session", address(server))
}

struct Enrolled {
    node_id: String,
    credential: String,
}

async fn enrolled(server: &Server, owner: &reqwest::Client) -> Enrolled {
    // The registered local node is a voter without a session: record its
    // acknowledgement, so this second voter passes rule 2 of
    // node-agent-protocol.md section 8.10.
    server.state.hub.ack_voter_set().await;
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "join token: {body}");
    let token = body["token"].clone();

    let res = reqwest::Client::new()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token,
            "name": "node-alpha",
            "public_key": public_key(),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "enroll: {body}");
    Enrolled {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

fn hello(node_id: &str) -> NodeMessage {
    NodeMessage::Hello(wire::Hello {
        node_id: node_id.to_string(),
        agent_version: "test-agent".to_string(),
        engine_version: "test-engine".to_string(),
        boot_id: "boot-1".to_string(),
        stored_rev: 0,
        applied_rev: 0,
        log_seq: 0,
        log_hash: "0".repeat(64),
        leases: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        mesh_public_key: None,
        volume_snapshot: None,
        custody_public_key: None,
    })
}

async fn open(server: &Server, node: &Enrolled) -> Socket {
    let mut request = ws_url(server).into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", node.credential).parse().unwrap(),
    );
    let stream = TcpStream::connect(address(server)).await.unwrap();
    let (mut socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status(), 101);
    send(&mut socket, hello(&node.node_id)).await;
    // `welcome` then the first `desired_state`.
    for kind in ["welcome", "desired_state"] {
        loop {
            let frame = recv_frame(&mut socket).await;
            if frame["type"] == kind {
                break;
            }
        }
    }
    socket
}

async fn send(socket: &mut Socket, message: NodeMessage) {
    let id = new_id(Prefix::Request);
    let text = message.to_frame(&id, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("a frame within 3 s")
            .expect("the socket is open")
            .expect("a valid websocket frame");
        match message {
            WsMessage::Text(text) => {
                return serde_json::from_str(text.as_str()).expect("JSON frame");
            }
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

/// Sends one node event (`source` and `actor` name this node).
async fn send_event(
    socket: &mut Socket,
    node_id: &str,
    kind: &str,
    app_id: &str,
    project_id: &str,
) -> String {
    let id = new_id(Prefix::Event);
    let mut data = json!({ "app_id": app_id });
    if kind == "app.down" {
        data = json!({
            "app_id": app_id,
            "node_id": node_id,
            "check": "health_check",
            "failures": 3,
            "last_error": "connect: connection refused",
        });
    }
    let event = EventBody {
        schema_version: 1,
        id: id.clone(),
        kind: kind.to_string(),
        time: "2026-11-02T09:15:00Z".to_string(),
        source: json!({ "kind": "node", "node_id": node_id }),
        subject: json!({ "type": "app", "id": app_id }),
        project_id: Some(project_id.to_string()),
        incident_id: None,
        request_id: None,
        actor: Some(json!({ "type": "node", "id": node_id })),
        data,
    };
    send(socket, NodeMessage::Event(event)).await;
    id
}

/// Waits until an open incident exists for `app_id` and returns it.
async fn wait_open_incident(server: &Server, app_id: &str) -> String {
    for _ in 0..100 {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM incidents WHERE status = 'open' \
             AND json_extract(subject, '$.id') = ? LIMIT 1",
        )
        .bind(app_id)
        .fetch_optional(server.state.store.pool())
        .await
        .unwrap();
        if let Some((id,)) = row {
            return id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no open incident for {app_id}");
}

// ---------------------------------------------------------------------------
// Test helpers
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

/// Opens an incident with a real node session and returns its id and the id
/// of the `app.down` event that opened it.
async fn open_incident(seeded: &Seeded) -> (String, String) {
    let node = enrolled(&seeded.server, &seeded.owner).await;
    let mut socket = open(&seeded.server, &node).await;
    let event_id = send_event(
        &mut socket,
        &node.node_id,
        "app.down",
        &seeded.app_id,
        &seeded.project_id,
    )
    .await;
    let incident_id = wait_open_incident(&seeded.server, &seeded.app_id).await;
    (incident_id, event_id)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn incident_reads_over_mcp_match_http() {
    let seeded = seeded().await;
    let (incident_id, event_id) = open_incident(&seeded).await;
    create_grant(
        &seeded,
        &["incident:read", "event:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    // `list_incidents` is one service function behind two adapters: the
    // agent's MCP page equals the owner's HTTP page (the grant's selector
    // admits the one app that has an incident).
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_incidents",
        json!({ "status": "open" }),
    )
    .await;
    assert!(!is_error(&result), "list_incidents: {result}");
    let http: Value = seeded
        .owner
        .get(seeded.server.url("/api/v1/incidents?status=open"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"], http,
        "the MCP incident list must match the HTTP list"
    );
    assert_eq!(http["items"][0]["id"], incident_id);

    // `get_incident` returns the HTTP detail body field for field.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_incident",
        json!({ "incident_id": incident_id }),
    )
    .await;
    assert!(!is_error(&result), "get_incident: {result}");
    let http: Value = seeded
        .owner
        .get(
            seeded
                .server
                .url(&format!("/api/v1/incidents/{incident_id}")),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"], http,
        "the MCP incident detail must match the HTTP detail"
    );
    assert_eq!(http["trigger_event_id"], event_id);

    // `list_events` reaches the app.down event that opened the incident.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_events",
        json!({ "type": ["app.down"], "subject_id": seeded.app_id }),
    )
    .await;
    assert!(!is_error(&result), "list_events: {result}");
    let items = result["structuredContent"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items in {result}"));
    let down = items
        .iter()
        .find(|event| event["id"] == event_id.as_str())
        .unwrap_or_else(|| panic!("no app.down event {event_id} in {result}"));
    assert_eq!(down["type"], "app.down", "{down}");
    assert_eq!(down["subject"]["id"], seeded.app_id, "{down}");
}

#[tokio::test]
async fn list_events_rejects_an_unknown_type() {
    let seeded = seeded().await;
    let (_, _) = open_incident(&seeded).await;
    create_grant(
        &seeded,
        &["event:read"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_events",
        json!({ "type": ["nope.nope"] }),
    )
    .await;
    assert!(is_error(&result), "list_events unknown type: {result}");
    let problem = problem_body(&result, "list_events unknown type");
    assert_eq!(problem["code"], "validation_failed", "{problem}");

    // The type is checked before the call is authorized, so no audit record
    // was written for it.
    let rows = seeded.server.audit_rows().await;
    assert_eq!(
        rows.len(),
        before,
        "an unknown event type must not be audited: {rows:?}"
    );
}

#[tokio::test]
async fn resolve_incident_needs_a_justification_and_is_audited() {
    let seeded = seeded().await;
    let (incident_id, _) = open_incident(&seeded).await;
    let grant = create_grant(
        &seeded,
        &["incident:read", "incident:resolve"],
        json!({ "apps": [&seeded.app_id] }),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();

    // (a) No justification: the service layer refuses the mutating call and
    // records the refusal.
    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "resolve_incident",
        json!({ "incident_id": incident_id }),
    )
    .await;
    assert!(
        is_error(&result),
        "resolve_incident without justification: {result}"
    );
    let problem = problem_body(&result, "resolve_incident without justification");
    assert_eq!(problem["code"], "validation_failed", "{problem}");

    let record = rows_after(&seeded.server, before, "resolve_incident").await;
    assert_eq!(record["operation"], "resolve_incident", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["result"], "error", "{record}");
    assert!(record["justification"].is_null(), "{record}");

    // (b) With a justification and a note the incident resolves. The single
    // `incident_id` argument is both the target and the call's context
    // incident.
    let justification = "the node's health check settled";
    let note = "restarted the app on the node";
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "resolve_incident",
        json!({
            "incident_id": incident_id,
            "justification": justification,
            "resolution_note": note,
        }),
    )
    .await;
    assert!(!is_error(&result), "resolve_incident: {result}");
    assert_eq!(
        result["structuredContent"]["status"], "resolved",
        "{result}"
    );
    assert_eq!(
        result["structuredContent"]["resolution_note"], note,
        "{result}"
    );
    assert_eq!(
        result["structuredContent"]["resolved_by_type"], "agent",
        "{result}"
    );

    let record = rows_after(
        &seeded.server,
        before + 1,
        "resolve_incident with justification",
    )
    .await;
    assert_eq!(record["result"], "ok", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert_eq!(record["operation"], "resolve_incident", "{record}");
    assert_eq!(record["incident_id"], incident_id, "{record}");
    assert_eq!(record["justification"], justification, "{record}");
    assert_eq!(record["grant_id"], grant_id, "{record}");
    assert_eq!(record["on_behalf_of"], seeded.owner_id, "{record}");
    assert!(!record["change_seq"].is_null(), "{record}");

    // A second resolve is a conflict; the incident is already resolved.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "resolve_incident",
        json!({
            "incident_id": incident_id,
            "justification": "resolve it again",
        }),
    )
    .await;
    assert!(is_error(&result), "second resolve: {result}");
    let problem = problem_body(&result, "second resolve");
    assert_eq!(problem["code"], "conflict", "{problem}");
}

#[tokio::test]
async fn list_audit_filters_by_incident() {
    let seeded = seeded().await;
    let (incident_id, _) = open_incident(&seeded).await;
    // `audit:read` needs a selector that reaches an audit record's target.
    // Records about the incident carry `target_type: incident` and the app's
    // project, so the test grants the whole set explicitly.
    create_grant(
        &seeded,
        &["incident:read", "incident:resolve", "audit:read"],
        json!({ "all": true }),
    )
    .await;

    let justify = "close it out";
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "resolve_incident",
        json!({ "incident_id": incident_id, "justification": justify }),
    )
    .await;
    assert!(!is_error(&result), "resolve_incident: {result}");

    // One incident read with no incident context, so the unfiltered page is
    // guaranteed to hold a record whose `incident_id` is null.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_incident",
        json!({ "incident_id": incident_id }),
    )
    .await;
    assert!(!is_error(&result), "get_incident: {result}");

    // Filtered: every row belongs to the incident, and the ok resolve record
    // is among them.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_audit",
        json!({ "incident_id": incident_id, "limit": 500 }),
    )
    .await;
    assert!(!is_error(&result), "list_audit by incident: {result}");
    let items = result["structuredContent"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items in {result}"));
    assert!(!items.is_empty(), "{result}");
    for item in items {
        assert_eq!(item["incident_id"], incident_id, "{item}");
    }
    let resolved = items
        .iter()
        .find(|row| {
            row["operation"] == "resolve_incident"
                && row["result"] == "ok"
                && row["channel"] == "mcp"
        })
        .unwrap_or_else(|| panic!("no ok resolve record in {result}"));
    assert_eq!(resolved["justification"], justify, "{resolved}");
    assert_eq!(resolved["incident_id"], incident_id, "{resolved}");

    // Newest first, the filter selects the same records (ADR 0283).
    // `before_seq` closes the window after the page above, so the records of
    // the reads since then stay outside it.
    let newest = items
        .last()
        .and_then(|row| row["seq"].as_i64())
        .unwrap_or_else(|| panic!("no seq in {result}"));
    let result_desc = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_audit",
        json!({
            "incident_id": incident_id,
            "order": "desc",
            "before_seq": newest + 1,
            "limit": 500,
        }),
    )
    .await;
    assert!(
        !is_error(&result_desc),
        "list_audit newest first: {result_desc}"
    );
    let newest_first = result_desc["structuredContent"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items in {result_desc}"));
    let expected: Vec<Value> = items.iter().rev().cloned().collect();
    assert_eq!(*newest_first, expected, "{result_desc}");

    // Unfiltered: the same log also holds records outside the incident, so
    // the filter above really selected.
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "list_audit",
        json!({ "limit": 500 }),
    )
    .await;
    assert!(!is_error(&result), "list_audit: {result}");
    let items = result["structuredContent"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("no items in {result}"));
    assert!(
        items
            .iter()
            .any(|row| row["incident_id"] != incident_id.as_str()),
        "the unfiltered page must hold a record outside the incident: {result}"
    );
}

#[tokio::test]
async fn incident_tools_without_a_grant_are_refused() {
    let seeded = seeded().await;
    let (incident_id, _) = open_incident(&seeded).await;

    let before = seeded.server.audit_rows().await.len();
    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "get_incident",
        json!({ "incident_id": incident_id }),
    )
    .await;
    assert!(is_error(&result), "get_incident: {result}");
    let problem = problem_body(&result, "get_incident");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    assert_eq!(problem["reason"], "scope_missing", "{problem}");
    let record = rows_after(&seeded.server, before, "get_incident").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "scope_missing", "{record}");
    assert_eq!(record["scope"], "incident:read", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");

    let result = call_tool(
        &seeded.server,
        &seeded.access_token,
        "resolve_incident",
        json!({
            "incident_id": incident_id,
            "justification": "no grant, so this must not run",
        }),
    )
    .await;
    assert!(is_error(&result), "resolve_incident: {result}");
    let problem = problem_body(&result, "resolve_incident");
    assert_eq!(problem["code"], "forbidden", "{problem}");
    assert_eq!(problem["reason"], "scope_missing", "{problem}");
    let record = rows_after(&seeded.server, before + 1, "resolve_incident").await;
    assert_eq!(record["result"], "refused", "{record}");
    assert_eq!(record["result_detail"], "scope_missing", "{record}");
    assert_eq!(record["scope"], "incident:resolve", "{record}");
    assert_eq!(record["channel"], "mcp", "{record}");
    assert!(record["change_seq"].is_null(), "{record}");
}
