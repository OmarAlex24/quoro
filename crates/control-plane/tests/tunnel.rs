//! Cloudflare Tunnel and DNS-01 wiring in the primary (ADR 0094, 0095;
//! `node-agent-protocol.md` sections 5.11, 5.22, 5.23).
//!
//! A local mock of the Cloudflare API stands in for the provider. The account
//! credential reaches it in the `Authorization` header; the node session only
//! ever sees the tunnel's own run token.

mod common;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, put};
use axum::{Json, Router};
use common::{Server, problem, start_with_cloudflare};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const ACCOUNT_ID: &str = "acct_example";
const ZONE_ID: &str = "zone_example";
const PROVIDER_SECRET: &str = "cf-account-token-never-on-a-node";

// ---------------------------------------------------------------------------
// A local Cloudflare API mock
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Option<Value>,
    /// When the mock received it.
    at: std::time::Instant,
}

#[derive(Clone, Debug)]
struct MockRecord {
    id: String,
    name: String,
    kind: String,
    content: String,
    proxied: bool,
    comment: Option<String>,
}

#[derive(Clone, Debug)]
struct MockTunnel {
    id: String,
    name: String,
}

#[derive(Default)]
struct MockState {
    requests: Vec<Recorded>,
    records: Vec<MockRecord>,
    tunnels: Vec<MockTunnel>,
    next_id: usize,
}

#[derive(Clone, Default)]
struct Mock {
    state: Arc<Mutex<MockState>>,
    base: String,
}

async fn start_mock() -> Mock {
    let state: Arc<Mutex<MockState>> = Arc::new(Mutex::new(MockState::default()));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let app = mock_router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Mock {
        state,
        base: format!("http://{addr}"),
    }
}

impl Mock {
    fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    fn records(&self) -> Vec<MockRecord> {
        self.state.lock().unwrap().records.clone()
    }

    fn tunnels(&self) -> Vec<MockTunnel> {
        self.state.lock().unwrap().tunnels.clone()
    }

    /// Every request of `method` whose path contains `fragment`.
    fn matching(&self, method: &str, fragment: &str) -> Vec<Recorded> {
        self.requests()
            .into_iter()
            .filter(|request| request.method == method && request.path.contains(fragment))
            .collect()
    }

    fn assert_all_authorized(&self) {
        let expected = format!("Bearer {PROVIDER_SECRET}");
        for request in self.requests() {
            assert_eq!(
                request.authorization.as_deref(),
                Some(expected.as_str()),
                "{} {} lost its bearer token",
                request.method,
                request.path
            );
        }
    }
}

fn mock_router(state: Arc<Mutex<MockState>>) -> Router {
    Router::new()
        .route(
            "/accounts/{account}/cfd_tunnel",
            get(list_tunnels).post(create_tunnel),
        )
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/connections",
            delete(delete_connections),
        )
        .route("/accounts/{account}/cfd_tunnel/{id}", delete(delete_tunnel))
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/token",
            get(tunnel_token),
        )
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/configurations",
            put(put_configuration),
        )
        .route("/zones/{zone}/dns_records", get(list_dns).post(create_dns))
        .route(
            "/zones/{zone}/dns_records/{id}",
            patch(patch_dns).delete(delete_dns),
        )
        .with_state(state)
}

fn ok(result: Value) -> Response {
    (
        StatusCode::OK,
        Json(json!({"success": true, "errors": [], "result": result})),
    )
        .into_response()
}

fn fail(status: u16, code: i64, message: &str) -> Response {
    (
        StatusCode::from_u16(status).unwrap(),
        Json(json!({
            "success": false,
            "errors": [{"code": code, "message": message}],
            "result": null,
        })),
    )
        .into_response()
}

fn parse_body(bytes: &Bytes) -> Option<Value> {
    if bytes.is_empty() {
        None
    } else {
        serde_json::from_slice(bytes).ok()
    }
}

fn record_into(state: &mut MockState, body: &Value) -> MockRecord {
    state.next_id += 1;
    MockRecord {
        id: format!("rec{}", state.next_id),
        name: body["name"].as_str().unwrap_or_default().to_string(),
        kind: body["type"].as_str().unwrap_or_default().to_string(),
        content: body["content"].as_str().unwrap_or_default().to_string(),
        proxied: body["proxied"].as_bool().unwrap_or(false),
        comment: body["comment"].as_str().map(str::to_string),
    }
}

fn record_json(record: &MockRecord) -> Value {
    json!({
        "id": record.id,
        "type": record.kind,
        "name": record.name,
        "content": record.content,
        "proxied": record.proxied,
        "comment": record.comment,
    })
}

/// Records the request and its body.
fn record(
    state: &Arc<Mutex<MockState>>,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Option<Value>,
) {
    state.lock().unwrap().requests.push(Recorded {
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        authorization: headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        body,
        at: std::time::Instant::now(),
    });
}

async fn list_tunnels(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    let name = params.get("name").cloned().unwrap_or_default();
    let state = state.lock().unwrap();
    let result: Vec<Value> = state
        .tunnels
        .iter()
        .filter(|tunnel| name.is_empty() || tunnel.name == name)
        .map(|tunnel| json!({"id": tunnel.id, "name": tunnel.name}))
        .collect();
    ok(Value::Array(result))
}

async fn create_tunnel(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes).unwrap_or(Value::Null);
    record(&state, &method, &uri, &headers, Some(body.clone()));
    let mut state = state.lock().unwrap();
    state.next_id += 1;
    let tunnel = MockTunnel {
        id: format!("tun{}", state.next_id),
        name: body["name"].as_str().unwrap_or_default().to_string(),
    };
    state.tunnels.push(tunnel.clone());
    ok(json!({"id": tunnel.id, "name": tunnel.name}))
}

async fn tunnel_token(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    let id = params.get("id").cloned().unwrap_or_default();
    let exists = state
        .lock()
        .unwrap()
        .tunnels
        .iter()
        .any(|tunnel| tunnel.id == id);
    if !exists {
        return fail(404, 1003, "Tunnel not found");
    }
    ok(json!(format!("tunnel-token-{id}")))
}

async fn put_configuration(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    ok(json!({"tunnel_id": "tun"}))
}

async fn delete_connections(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    ok(json!({}))
}

async fn delete_tunnel(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    state.tunnels.retain(|tunnel| tunnel.id != id);
    ok(json!({"id": id}))
}

async fn list_dns(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    let name = params
        .get("name.exact")
        .or_else(|| params.get("name"))
        .cloned()
        .unwrap_or_default();
    let state = state.lock().unwrap();
    let result: Vec<Value> = state
        .records
        .iter()
        .filter(|record| name.is_empty() || record.name == name)
        .map(record_json)
        .collect();
    ok(Value::Array(result))
}

async fn create_dns(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes).unwrap_or(Value::Null);
    record(&state, &method, &uri, &headers, Some(body.clone()));
    let mut state = state.lock().unwrap();
    let record = record_into(&mut state, &body);
    state.records.push(record.clone());
    drop(state);
    ok(record_json(&record))
}

async fn patch_dns(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes).unwrap_or(Value::Null);
    record(&state, &method, &uri, &headers, Some(body.clone()));
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    let Some(record) = state.records.iter_mut().find(|record| record.id == id) else {
        return fail(404, 81044, "Record does not exist");
    };
    if let Some(content) = body["content"].as_str() {
        record.content = content.to_string();
    }
    let record = record.clone();
    ok(record_json(&record))
}

async fn delete_dns(
    State(state): State<Arc<Mutex<MockState>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    record(&state, &method, &uri, &headers, parse_body(&bytes));
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    let before = state.records.len();
    state.records.retain(|record| record.id != id);
    if state.records.len() == before {
        return fail(404, 81044, "Record does not exist");
    }
    ok(json!({"id": id}))
}

// ---------------------------------------------------------------------------
// The node-session harness (a copy of the shape `tests/node_session.rs` uses)
// ---------------------------------------------------------------------------

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[11u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
}

struct Enrolled {
    node_id: String,
    credential: String,
}

/// The change log's newest `seq`, 0 for an empty log.
async fn log_head(server: &Server) -> i64 {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    head.unwrap_or(0)
}

/// Asserts that `node`'s `log_lag` is the newest `seq` when the response was
/// built minus its `log_seq`: above `before`, read before the call that wrote,
/// and at most `after` (node-agent-protocol.md section 7.1, ADR 0301).
fn assert_log_lag(node: &Value, before: i64, after: i64) {
    let reported =
        node["log_seq"].as_i64().expect("log_seq") + node["log_lag"].as_i64().expect("log_lag");
    assert!(
        before < reported && reported <= after,
        "{before} < {reported} <= {after}: {node}"
    );
}

async fn create_token(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    let token = create_token(server, owner).await;
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token,
            "name": name,
            "public_key": public_key(),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
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

async fn connect(server: &Server, credential: &str) -> Socket {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status(), 101);
    socket
}

async fn send(socket: &mut Socket, id: &str, message: NodeMessage) {
    socket
        .send(WsMessage::Text(message.to_frame(id, None).to_text().into()))
        .await
        .unwrap();
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("a frame within 5 s")
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

async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

async fn open(server: &Server, node: &Enrolled) -> (Socket, Value) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, "msg_hello", hello(&node.node_id)).await;
    let _welcome = recv_type(&mut socket, "welcome").await;
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async fn node_json(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

async fn create_provider(
    server: &Server,
    owner: &reqwest::Client,
    kind: &str,
    name: &str,
    config: Value,
) -> Value {
    let res = owner
        .post(server.url("/api/v1/providers"))
        .json(&json!({ "kind": kind, "name": name, "config": config, "secret": PROVIDER_SECRET }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "provider: {body}");
    body
}

async fn setup_project_and_app(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "Demo" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let project: Value = res.json().await.unwrap();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project["id"],
            "name": "web",
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    app["id"].as_str().unwrap().to_string()
}

async fn create_domain(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
    hostname: &str,
    tls_mode: &str,
    provider_id: &str,
) -> Value {
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
        .json(&json!({
            "hostname": hostname,
            "tls_mode": tls_mode,
            "provider_id": provider_id,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "domain: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// Places a running VM of `app_id` on `node_id` (the deployment the route
/// rule looks for).
async fn place_vm(server: &Server, app_id: &str, node_id: &str, name: &str) -> String {
    let now = quoro_control_plane::store::now_rfc3339();
    let app_project: String = sqlx::query_scalar("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let vm_id = quoro_types::new_id(quoro_types::Prefix::Vm);
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, 1, 256, 1, 'ghcr.io/example/web@sha256:1111', 'running', 'running', \
         NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(&vm_id)
    .bind(&app_project)
    .bind(app_id)
    .bind(node_id)
    .bind(name)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    vm_id
}

/// Polls `check` until it returns a value or five seconds pass.
async fn wait_for<T>(mut check: impl FnMut() -> Option<T>) -> T {
    for _ in 0..100 {
        if let Some(value) = check() {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not met within 5 s");
}

/// Reads desired states until one carries `spec.ingress`.
async fn wait_for_ingress(socket: &mut Socket) -> Value {
    loop {
        let frame = recv_type(socket, "desired_state").await;
        if frame["body"]["spec"]["ingress"].is_object() {
            return frame;
        }
    }
}

/// Reads desired states until one has no `spec.ingress`.
async fn wait_for_no_ingress(socket: &mut Socket) -> Value {
    loop {
        let frame = recv_type(socket, "desired_state").await;
        if frame["body"]["spec"]["ingress"].is_null() {
            return frame;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tunnel_mapping_reaches_the_node_and_never_leaks_the_token() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-alpha").await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let app_id = setup_project_and_app(&server, &owner).await;
    let domain = create_domain(
        &server,
        &owner,
        &app_id,
        "tunnel.example.com",
        "tunnel",
        &provider_id,
    )
    .await;
    place_vm(&server, &app_id, &node.node_id, "web-a").await;

    // Before the mapping the node gets no ingress.
    let (mut socket, snapshot) = open(&server, &node).await;
    assert!(snapshot["body"]["spec"]["ingress"].is_null(), "{snapshot}");
    assert_eq!(
        snapshot["body"]["spec"]["routes"][0]["hostname"],
        "tunnel.example.com"
    );
    // Every route names its domain, for the node's `certificate.*` events.
    assert_eq!(
        snapshot["body"]["spec"]["routes"][0]["domain_id"],
        domain["id"]
    );

    // PUT maps the node; the manager creates the tunnel and resends the
    // snapshot with the run token.
    let before = log_head(&server).await;
    let res = owner
        .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .json(&json!({ "provider_id": provider_id, "origin_port": 8080 }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.bytes().await.unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["tunnel"]["provider_id"], provider_id);
    assert_eq!(body["tunnel"]["origin_port"], 8080);
    // The returned node carries its change-log lag, as every node read does
    // (node-agent-protocol.md section 7.1, ADR 0301).
    assert_log_lag(&body, before, log_head(&server).await);

    let tunnel_id = wait_for(|| mock.tunnels().first().map(|tunnel| tunnel.id.clone())).await;
    let token = format!("tunnel-token-{tunnel_id}");
    let frame = wait_for_ingress(&mut socket).await;
    let ingress = &frame["body"]["spec"]["ingress"]["tunnel"];
    assert_eq!(ingress["tunnel_id"], tunnel_id);
    assert_eq!(ingress["token"], token);
    assert_eq!(ingress["origin_port"], 8080);
    // Only this node's tunnel travels to the node, never the account token.
    assert_ne!(ingress["token"], PROVIDER_SECRET);

    // The API shows the mapping without the token.
    let node_body = node_json(&server, &owner, &node.node_id).await;
    assert_eq!(node_body["tunnel"]["provider_id"], provider_id);
    assert_eq!(node_body["tunnel"]["tunnel_id"], tunnel_id);
    assert_eq!(node_body["tunnel"]["origin_port"], 8080);
    let node_text = node_body.to_string();
    assert!(!node_text.contains(&token), "GET node leaked the run token");
    assert!(node_body["tunnel"].get("token").is_none(), "{node_body}");

    // The tunnel ingress rule and the proxied CNAME are the primary's work.
    wait_for(|| {
        (mock
            .matching("PUT", "/configurations")
            .iter()
            .any(|request| {
                request.body.as_ref().is_some_and(|body| {
                    body["config"]["ingress"].as_array().is_some_and(|rules| {
                        rules.iter().any(|rule| {
                            rule["hostname"] == "tunnel.example.com"
                                && rule["service"] == "http://127.0.0.1:8080"
                        })
                    })
                })
            }))
        .then_some(())
    })
    .await;
    wait_for(|| {
        mock.records()
            .iter()
            .find(|record| {
                record.name == "tunnel.example.com"
                    && record.kind == "CNAME"
                    && record.proxied
                    && record.content == format!("{tunnel_id}.cfargotunnel.com")
            })
            .map(|_| ())
    })
    .await;

    // The token and the account credential stay out of the audit and change
    // logs.
    let audit: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT operation, params, result_detail FROM audit_log")
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    let payloads: Vec<(String,)> = sqlx::query_as("SELECT payload FROM change_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    for row in &audit {
        let text = format!("{row:?}");
        assert!(!text.contains(&token), "audit log leaked the run token");
        assert!(
            !text.contains(PROVIDER_SECRET),
            "audit log leaked the secret"
        );
    }
    for (payload,) in &payloads {
        assert!(!payload.contains(&token), "change log leaked the run token");
        assert!(
            !payload.contains(PROVIDER_SECRET),
            "change log leaked the secret"
        );
    }
    let puts: Vec<(String, String, Option<String>)> = audit
        .iter()
        .filter(|(operation, _, _)| operation == "PUT /api/v1/nodes/{node_id}/tunnel")
        .cloned()
        .collect();
    assert_eq!(puts.len(), 1, "{puts:?}");
    mock.assert_all_authorized();

    // DELETE removes the mapping, the node's next snapshot has no ingress and
    // the manager retires the tunnel at the provider. A node that still serves
    // a tunnel domain is refused first.
    let (status, code, _) = problem(
        owner
            .delete(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    let domain_id: String = sqlx::query_scalar("SELECT id FROM domains WHERE hostname = ?")
        .bind("tunnel.example.com")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let res = owner
        .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    let res = owner
        .delete(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let frame = wait_for_no_ingress(&mut socket).await;
    assert!(frame["body"]["spec"]["ingress"].is_null());
    let deleted = node_json(&server, &owner, &node.node_id).await;
    assert!(deleted["tunnel"].is_null(), "{deleted}");
    wait_for(|| (mock.tunnels().is_empty()).then_some(())).await;
}

#[tokio::test]
async fn tunnel_operations_are_forbidden_refused_and_validated() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-beta").await;
    let first = create_provider(
        &server,
        &owner,
        "tunnel",
        "First",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let second = create_provider(
        &server,
        &owner,
        "tunnel",
        "Second",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let dns = create_provider(&server, &owner, "dns", "Dns", json!({ "zone_id": ZONE_ID })).await;
    let incomplete = create_provider(
        &server,
        &owner,
        "tunnel",
        "NoZone",
        json!({ "account_id": ACCOUNT_ID }),
    )
    .await;
    let app_id = setup_project_and_app(&server, &owner).await;
    create_domain(
        &server,
        &owner,
        &app_id,
        "tunnel.example.com",
        "tunnel",
        first["id"].as_str().unwrap(),
    )
    .await;
    place_vm(&server, &app_id, &node.node_id, "web-b").await;

    // A viewer lacks `provider:manage`.
    server
        .insert_user(
            "viewer@example.com",
            "viewer password, long enough",
            "viewer",
        )
        .await;
    let viewer = server.client();
    server
        .login(
            &viewer,
            "viewer@example.com",
            "viewer password, long enough",
        )
        .await;
    let (status, code, reason) = problem(
        viewer
            .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .json(&json!({ "provider_id": first["id"] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    // An unknown node, a provider that does not exist, the wrong kind and a
    // tunnel provider without account_id/zone_id are all refused.
    let (status, code, _) = problem(
        owner
            .put(server.url("/api/v1/nodes/nod_00000000000000000000000000/tunnel"))
            .json(&json!({ "provider_id": first["id"] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
    let (status, code, _) = problem(
        owner
            .delete(server.url("/api/v1/nodes/nod_00000000000000000000000000/tunnel"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
    let (status, code, _) = problem(
        owner
            .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .json(&json!({ "provider_id": dns["id"] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let (status, code, _) = problem(
        owner
            .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .json(&json!({ "provider_id": incomplete["id"] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let (status, code, _) = problem(
        owner
            .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .json(&json!({
                "provider_id": "prv_00000000000000000000000000",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // The first mapping succeeds; replacing the provider of a node that still
    // serves a tunnel domain conflicts, and so does removing it.
    let res = owner
        .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .json(&json!({ "provider_id": first["id"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let (status, code, _) = problem(
        owner
            .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .json(&json!({ "provider_id": second["id"] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    let (status, code, _) = problem(
        owner
            .delete(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // The same provider may change only the origin port.
    let res = owner
        .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .json(&json!({ "provider_id": first["id"], "origin_port": 9000 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["tunnel"]["provider_id"], first["id"]);
    assert_eq!(body["tunnel"]["origin_port"], 9000);

    // Removing the route then lets the mapping go.
    let domain_id: String = sqlx::query_scalar("SELECT id FROM domains WHERE hostname = ?")
        .bind("tunnel.example.com")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let res = owner
        .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success(), "{}", res.status());
    let res = owner
        .delete(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    // A second delete finds no mapping.
    let (status, code, _) = problem(
        owner
            .delete(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

#[tokio::test]
async fn dns_challenges_are_written_by_the_primary_for_the_owning_node() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    let alpha = enrolled(&server, &owner, "node-alpha").await;
    let beta = enrolled(&server, &owner, "node-beta").await;
    let dns = create_provider(&server, &owner, "dns", "Dns", json!({ "zone_id": ZONE_ID })).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    let domain = create_domain(
        &server,
        &owner,
        &app_id,
        "acme.example.com",
        "acme",
        dns["id"].as_str().unwrap(),
    )
    .await;
    place_vm(&server, &app_id, &alpha.node_id, "web-alpha").await;
    place_vm(&server, &app_id, &beta.node_id, "web-beta").await;

    let (mut a, snapshot) = open(&server, &alpha).await;
    assert_eq!(snapshot["body"]["spec"]["routes"][0]["tls_mode"], "acme");
    assert_eq!(
        snapshot["body"]["spec"]["routes"][0]["domain_id"],
        domain["id"]
    );
    let (mut b, _) = open(&server, &beta).await;

    // `set` for an acme route writes the TXT record and answers with its id.
    let value = "LoqXcYV8q5ONbJQxbmR7SCTNo3tiAXDfowyjxAjEuX0";
    send(
        &mut a,
        "msg_dns_set",
        NodeMessage::DnsChallenge(wire::DnsChallenge {
            action: wire::DnsChallengeAction::Set,
            hostname: "acme.example.com".to_string(),
            value: Some(value.to_string()),
            record_id: None,
        }),
    )
    .await;
    let result = recv_type(&mut a, "dns_challenge_result").await;
    assert_eq!(result["re"], "msg_dns_set");
    assert_eq!(result["body"]["ok"], true, "{result}");
    let record_id = result["body"]["record_id"].as_str().unwrap().to_string();
    let record = wait_for(|| {
        mock.records()
            .into_iter()
            .find(|record| record.kind == "TXT")
    })
    .await;
    assert_eq!(record.name, "_acme-challenge.acme.example.com");
    assert_eq!(record.content, value);
    assert_eq!(record.comment.as_deref(), Some("managed by quoro"));

    // A hostname that is not an acme route of this node is refused.
    send(
        &mut a,
        "msg_dns_wrong",
        NodeMessage::DnsChallenge(wire::DnsChallenge {
            action: wire::DnsChallengeAction::Set,
            hostname: "other.example.com".to_string(),
            value: Some(value.to_string()),
            record_id: None,
        }),
    )
    .await;
    let result = recv_type(&mut a, "dns_challenge_result").await;
    assert_eq!(result["re"], "msg_dns_wrong");
    assert_eq!(result["body"]["ok"], false, "{result}");
    assert!(
        !result["body"]["message"]
            .as_str()
            .unwrap()
            .contains(PROVIDER_SECRET)
    );

    // A record id created for another node cannot be cleared.
    send(
        &mut b,
        "msg_dns_foreign",
        NodeMessage::DnsChallenge(wire::DnsChallenge {
            action: wire::DnsChallengeAction::Clear,
            hostname: "acme.example.com".to_string(),
            value: None,
            record_id: Some(record_id.clone()),
        }),
    )
    .await;
    let result = recv_type(&mut b, "dns_challenge_result").await;
    assert_eq!(result["body"]["ok"], false, "{result}");
    assert!(
        !mock.records().is_empty(),
        "another node's record was deleted"
    );

    // The owning node clears it.
    send(
        &mut a,
        "msg_dns_clear",
        NodeMessage::DnsChallenge(wire::DnsChallenge {
            action: wire::DnsChallengeAction::Clear,
            hostname: "acme.example.com".to_string(),
            value: None,
            record_id: Some(record_id),
        }),
    )
    .await;
    let result = recv_type(&mut a, "dns_challenge_result").await;
    assert_eq!(result["body"]["ok"], true, "{result}");
    wait_for(|| mock.records().is_empty().then_some(())).await;

    // The node's certificate status lands on the domain row.
    send(
        &mut a,
        "msg_status",
        NodeMessage::StatusReport(wire::StatusReport {
            applied_rev: 0,
            vms: Vec::new(),
            routes: vec![wire::RouteStatus {
                hostname: "acme.example.com".to_string(),
                cert_status: "issued".to_string(),
                cert_expires_at: Some("2027-01-01T00:00:00Z".to_string()),
            }],
            capacity: WireCapacity {
                cpus: 8,
                memory_mb: 16_384,
                disk_gb: 100,
            },
            allocated: WireCapacity::default(),
            disk_free_gb: 80.0,
            builds: Vec::new(),
            edge_backends: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: Vec::new(),
            volumes: Vec::new(),
            quarantines: Vec::new(),
        }),
    )
    .await;
    wait_for_domain_cert(&server, "acme.example.com").await;
    mock.assert_all_authorized();
}

/// Polls the `domains` row until the node's certificate status landed.
async fn wait_for_domain_cert(server: &Server, hostname: &str) {
    for _ in 0..100 {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT cert_status, cert_expires_at FROM domains WHERE hostname = ?")
                .bind(hostname)
                .fetch_optional(server.state.store.pool())
                .await
                .unwrap();
        if let Some((status, expires)) = row
            && status == "issued"
            && expires.as_deref() == Some("2027-01-01T00:00:00Z")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the certificate status did not land within 5 s");
}

// ---------------------------------------------------------------------------
// Tunnel hostnames that move between machines (issue #283)
// ---------------------------------------------------------------------------

/// A node id that sorts before [`NODE_B`] and [`EDGE`].
const NODE_A: &str = "nod_00000000000000000000000001";
/// The second placement node, so an app can have two replicas.
const NODE_B: &str = "nod_00000000000000000000000002";
/// A machine with the `edge` role and no `node` role.
const EDGE: &str = "nod_0000000000000000000000000E";

/// Inserts a connected machine row with `roles` and no session: the tunnel
/// manager must not need one to publish its hostnames.
async fn insert_machine(server: &Server, id: &str, name: &str, roles: &[&str]) {
    let now = quoro_control_plane::store::now_rfc3339();
    let roles = serde_json::to_string(roles).unwrap();
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
         labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
         log_seq, last_seen_at, created_at) \
         VALUES (?, ?, ?, NULL, 'connected', '', NULL, 1, '{}', \
         '{\"cpus\":8,\"memory_mb\":16384,\"disk_gb\":100}', '{}', \
         '{\"available\":true,\"kernel\":\"6.9.0\"}', NULL, NULL, 0, 0, 0, ?, ?)",
    )
    .bind(id)
    .bind(name)
    .bind(roles)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Maps `node_id` to the tunnel provider and returns its tunnel id once the
/// manager created it at the mock.
async fn map_tunnel(
    server: &Server,
    owner: &reqwest::Client,
    mock: &Mock,
    node_id: &str,
    name: &str,
    provider_id: &str,
) -> String {
    let res = owner
        .put(server.url(&format!("/api/v1/nodes/{node_id}/tunnel")))
        .json(&json!({ "provider_id": provider_id, "origin_port": 8081 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let tunnel_name = format!("quoro-{name}");
    wait_for(|| {
        mock.tunnels()
            .into_iter()
            .find(|tunnel| tunnel.name == tunnel_name)
            .map(|tunnel| tunnel.id)
    })
    .await
}

/// The target of the quoro CNAME of `hostname`, if there is one.
fn cname_target(mock: &Mock, hostname: &str) -> Option<String> {
    mock.records()
        .into_iter()
        .find(|record| record.name == hostname && record.kind == "CNAME")
        .map(|record| record.content)
}

/// Whether the newest ingress configuration of `tunnel_id` routes `hostname`.
fn tunnel_routes(mock: &Mock, tunnel_id: &str, hostname: &str) -> bool {
    mock.matching("PUT", &format!("/cfd_tunnel/{tunnel_id}/configurations"))
        .last()
        .and_then(|request| request.body.clone())
        .is_some_and(|body| {
            body["config"]["ingress"]
                .as_array()
                .is_some_and(|rules| rules.iter().any(|rule| rule["hostname"] == hostname))
        })
}

/// Waits until `hostname`'s CNAME points at `tunnel_id`, then checks it still
/// does after every pending reconcile had time to run.
async fn settles_on(mock: &Mock, hostname: &str, tunnel_id: &str) {
    let target = format!("{tunnel_id}.cfargotunnel.com");
    wait_for(|| (cname_target(mock, hostname).as_deref() == Some(target.as_str())).then_some(()))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        cname_target(mock, hostname).as_deref(),
        Some(target.as_str()),
        "the CNAME of {hostname} moved away from {tunnel_id}: {:?}",
        mock.records()
    );
}

async fn set_replicas(server: &Server, owner: &reqwest::Client, app_id: &str, replicas: i64) {
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "replicas": replicas }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
}

#[tokio::test]
async fn a_new_tunnel_domain_of_a_replicated_app_reaches_the_edge_tunnel_at_once() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    set_replicas(&server, &owner, &app_id, 2).await;

    // The edge has no session: its tunnel is still the primary's to publish,
    // well before the 60 s periodic pass.
    create_domain(
        &server,
        &owner,
        &app_id,
        "replicated.example.com",
        "tunnel",
        &provider_id,
    )
    .await;
    wait_for(|| tunnel_routes(&mock, &edge_tunnel, "replicated.example.com").then_some(())).await;
    settles_on(&mock, "replicated.example.com", &edge_tunnel).await;

    // Deleting the domain takes it off the edge's tunnel just as fast.
    let domain_id: String = sqlx::query_scalar("SELECT id FROM domains WHERE hostname = ?")
        .bind("replicated.example.com")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let res = owner
        .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    wait_for(|| {
        cname_target(&mock, "replicated.example.com")
            .is_none()
            .then_some(())
    })
    .await;
    assert!(!tunnel_routes(
        &mock,
        &edge_tunnel,
        "replicated.example.com"
    ));
}

#[tokio::test]
async fn a_replica_count_across_one_moves_the_hostname_between_node_and_edge() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    // The tunnel a hostname leaves keeps it for a drain window (ADR 0194); a
    // short one keeps this test about where the CNAME points.
    server
        .state
        .hub
        .set_tunnel_drain_window(Duration::from_millis(500));
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let node_tunnel = map_tunnel(&server, &owner, &mock, NODE_A, "alpha", &provider_id).await;
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    place_vm(&server, &app_id, NODE_A, "web-a").await;
    create_domain(
        &server,
        &owner,
        &app_id,
        "moving.example.com",
        "tunnel",
        &provider_id,
    )
    .await;

    // One replica: the node's tunnel serves the hostname.
    settles_on(&mock, "moving.example.com", &node_tunnel).await;

    // Two replicas: the edge answers it, and the node's tunnel must not
    // delete the CNAME the edge now owns.
    set_replicas(&server, &owner, &app_id, 2).await;
    settles_on(&mock, "moving.example.com", &edge_tunnel).await;
    wait_for(|| tunnel_routes(&mock, &edge_tunnel, "moving.example.com").then_some(())).await;
    wait_for(|| (!tunnel_routes(&mock, &node_tunnel, "moving.example.com")).then_some(())).await;

    // Back to one replica: the hostname returns to the node, and the edge's
    // tunnel must not delete the node's CNAME.
    set_replicas(&server, &owner, &app_id, 1).await;
    settles_on(&mock, "moving.example.com", &node_tunnel).await;
    wait_for(|| tunnel_routes(&mock, &node_tunnel, "moving.example.com").then_some(())).await;
    wait_for(|| (!tunnel_routes(&mock, &edge_tunnel, "moving.example.com")).then_some(())).await;

    // The tunnels sync in node id order, so on the way back the node's
    // tunnel repointed the edge's CNAME at itself before the edge's tunnel
    // dropped the hostname. The edge's tunnel then left that CNAME alone: the
    // last DNS write is the repoint, not a delete.
    let writes: Vec<Recorded> = mock
        .requests()
        .into_iter()
        .filter(|request| request.path.contains("/dns_records") && request.method != "GET")
        .collect();
    let last = writes.last().expect("DNS writes");
    assert_eq!(last.method, "PATCH", "{writes:?}");
    assert_eq!(
        last.body.as_ref().unwrap()["content"],
        format!("{node_tunnel}.cfargotunnel.com"),
        "{writes:?}"
    );
}

// ---------------------------------------------------------------------------
// A scale-down that would leave tunnel domains without a tunnel (issue #345)
// ---------------------------------------------------------------------------

/// Writes a succeeded deployment as the app's current one, with one running
/// instance on each of `nodes` linked to it, and sets the app's replica count
/// to match.
async fn deploy_on(server: &Server, app_id: &str, nodes: &[&str]) {
    let pool = server.state.store.pool();
    let now = quoro_control_plane::store::now_rfc3339();
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_one(pool)
        .await
        .unwrap();
    let deployment_id = "dep_01HZZZZZZZZZZZZZZZZZZZZZZZ".to_string();
    sqlx::query(
        "INSERT INTO deployments (id, app_id, image, status, \"trigger\", created_by_type, \
         created_by_id, created_at, finished_at) \
         VALUES (?, ?, 'ghcr.io/example/web@sha256:1111', 'succeeded', 'api', 'user', 'usr_test', ?, ?)",
    )
    .bind(&deployment_id)
    .bind(app_id)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    for node_id in nodes {
        sqlx::query(
            "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
             image, desired_status, observed_status, ip, restart_generation, restart_count, \
             last_exit_code, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 'web', 1, 256, 1, 'ghcr.io/example/web@sha256:1111', 'running', \
             'running', NULL, 0, 0, NULL, ?, ?)",
        )
        .bind(quoro_types::new_id(quoro_types::Prefix::Vm))
        .bind(&project_id)
        .bind(app_id)
        .bind(node_id)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE apps SET current_deployment_id = ?, replicas = ? WHERE id = ?")
        .bind(&deployment_id)
        .bind(i64::try_from(nodes.len()).unwrap())
        .bind(app_id)
        .execute(pool)
        .await
        .unwrap();
}

/// The nodes that run an instance of `app_id`, in id order.
async fn instance_nodes(server: &Server, app_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT node_id FROM vms WHERE app_id = ? ORDER BY node_id")
        .bind(app_id)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap()
}

/// The `(time, data)` of every event of type `kind`, oldest first.
async fn events_of(server: &Server, kind: &str) -> Vec<(String, Value)> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT time, data FROM events WHERE type = ? ORDER BY seq")
            .bind(kind)
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    rows.into_iter()
        .map(|(time, data)| (time, serde_json::from_str(&data).unwrap()))
        .collect()
}

/// Stops or starts `app_id` (`action` is `stop` or `start`).
async fn app_action(server: &Server, owner: &reqwest::Client, app_id: &str, action: &str) {
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/{action}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{action}: {}", res.text().await.unwrap());
}

/// Waits until the domain `hostname` of `app_id` has `serving_status`
/// `status` (ADR 0167), and returns it as the API shows it.
async fn wait_for_serving(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
    hostname: &str,
    status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/apps/{app_id}/domains")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let page: Value = res.json().await.unwrap();
        let domain = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|domain| domain["hostname"] == hostname)
            .cloned()
            .unwrap_or_else(|| panic!("no domain {hostname}: {page}"));
        if domain["serving_status"] == status {
            return domain;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{hostname} did not become {status}: {domain}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_scale_down_keeps_the_instance_on_the_node_with_a_tunnel() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    // Only the node with the higher id has a tunnel, so the old order (the
    // highest node ids go first) would keep the instance on the other one.
    let node_tunnel = map_tunnel(&server, &owner, &mock, NODE_B, "beta", &provider_id).await;
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    deploy_on(&server, &app_id, &[NODE_A, NODE_B]).await;
    create_domain(
        &server,
        &owner,
        &app_id,
        "kept.example.com",
        "tunnel",
        &provider_id,
    )
    .await;
    settles_on(&mock, "kept.example.com", &edge_tunnel).await;
    wait_for_serving(&server, &owner, &app_id, "kept.example.com", "serving").await;

    set_replicas(&server, &owner, &app_id, 1).await;
    assert_eq!(instance_nodes(&server, &app_id).await, vec![NODE_B]);
    settles_on(&mock, "kept.example.com", &node_tunnel).await;
    wait_for(|| tunnel_routes(&mock, &node_tunnel, "kept.example.com").then_some(())).await;
    let domain = wait_for_serving(&server, &owner, &app_id, "kept.example.com", "serving").await;
    assert!(domain["last_error"].is_null(), "{domain}");
    assert!(events_of(&server, "domain.unserved").await.is_empty());
    assert!(events_of(&server, "certificate.failed").await.is_empty());
}

/// Issue #349 (ADR 0167): the domain's `serving_status` follows where its
/// hostname can be served, and a change between `serving` and `unserved`
/// emits `domain.unserved` and `domain.served` instead of the old
/// `certificate.failed` stopgap.
#[tokio::test]
async fn a_scale_down_without_a_node_tunnel_makes_the_tunnel_domain_unserved() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    deploy_on(&server, &app_id, &[NODE_A, NODE_B]).await;
    let domain = create_domain(
        &server,
        &owner,
        &app_id,
        "dark.example.com",
        "tunnel",
        &provider_id,
    )
    .await;
    settles_on(&mock, "dark.example.com", &edge_tunnel).await;
    // Two replicas: the edge serves the hostname from its tunnel.
    wait_for_serving(&server, &owner, &app_id, "dark.example.com", "serving").await;
    assert!(events_of(&server, "domain.unserved").await.is_empty());

    // No node has a tunnel: the scale-down keeps the old order, and the
    // hostname that no tunnel serves any more is reported, not dropped
    // silently.
    set_replicas(&server, &owner, &app_id, 1).await;
    assert_eq!(instance_nodes(&server, &app_id).await, vec![NODE_A]);
    let unserved = wait_for_serving(&server, &owner, &app_id, "dark.example.com", "unserved").await;
    assert_eq!(
        unserved["last_error"],
        "no instance of the app runs on a node with a tunnel"
    );
    // A tunnel domain has no certificate: its status does not change.
    assert_eq!(unserved["cert_status"], "not_applicable");
    let reports = events_of(&server, "domain.unserved").await;
    assert_eq!(reports.len(), 1, "{reports:?}");
    let (unserved_at, data) = &reports[0];
    assert_eq!(
        data,
        &json!({
            "domain_id": domain["id"],
            "app_id": app_id,
            "hostname": "dark.example.com",
            "tls_mode": "tunnel",
            "error": "no instance of the app runs on a node with a tunnel",
        })
    );
    let res = owner
        .get(server.url("/api/v1/events?type=domain.unserved"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let page: Value = res.json().await.unwrap();
    assert_eq!(
        page["items"][0]["subject"],
        json!({ "type": "domain", "id": domain["id"] })
    );
    // The stopgap is gone: the hostname is not reported as a certificate.
    assert!(events_of(&server, "certificate.failed").await.is_empty());

    // Scaling up again hands the hostname back to the edge.
    set_replicas(&server, &owner, &app_id, 2).await;
    settles_on(&mock, "dark.example.com", &edge_tunnel).await;
    let served = wait_for_serving(&server, &owner, &app_id, "dark.example.com", "serving").await;
    assert!(served["last_error"].is_null(), "{served}");
    let recoveries = events_of(&server, "domain.served").await;
    assert_eq!(recoveries.len(), 1, "{recoveries:?}");
    assert_eq!(recoveries[0].1["domain_id"], domain["id"]);
    assert_eq!(recoveries[0].1["tls_mode"], "tunnel");
    assert_eq!(recoveries[0].1["unserved_since"], unserved_at.as_str());
}

/// Issue #349: every `domain.unserved` gets its `domain.served`, even when the
/// app was stopped in between (`unserved`, `pending`, `serving`), and a
/// domain that turns `unserved` again before it was served opens no second
/// period.
#[tokio::test]
async fn an_unserved_domain_is_announced_as_served_after_a_stop_and_a_start() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let app_id = setup_project_and_app(&server, &owner).await;
    deploy_on(&server, &app_id, &[NODE_A]).await;
    let domain = create_domain(
        &server,
        &owner,
        &app_id,
        "gap.example.com",
        "tunnel",
        &provider_id,
    )
    .await;

    // The app's only node has no tunnel: `pending` becomes `unserved`.
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "unserved").await;
    let reports = events_of(&server, "domain.unserved").await;
    assert_eq!(reports.len(), 1, "{reports:?}");
    let unserved_at = reports[0].0.clone();

    // Stopped, then started on the same node: `unserved` again, within the
    // period already open.
    app_action(&server, &owner, &app_id, "stop").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "pending").await;
    app_action(&server, &owner, &app_id, "start").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "unserved").await;
    assert_eq!(events_of(&server, "domain.unserved").await.len(), 1);

    // Stopped, given a tunnel, started: `pending` becomes `serving`, and the
    // period closes with `domain.served`.
    app_action(&server, &owner, &app_id, "stop").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "pending").await;
    map_tunnel(&server, &owner, &mock, NODE_A, "alpha", &provider_id).await;
    app_action(&server, &owner, &app_id, "start").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "serving").await;
    let recoveries = events_of(&server, "domain.served").await;
    assert_eq!(recoveries.len(), 1, "{recoveries:?}");
    assert_eq!(
        recoveries[0].1,
        json!({
            "domain_id": domain["id"],
            "app_id": app_id,
            "hostname": "gap.example.com",
            "tls_mode": "tunnel",
            "unserved_since": unserved_at,
        })
    );

    // The period is closed: a later stop and start announces nothing.
    app_action(&server, &owner, &app_id, "stop").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "pending").await;
    app_action(&server, &owner, &app_id, "start").await;
    wait_for_serving(&server, &owner, &app_id, "gap.example.com", "serving").await;
    assert_eq!(events_of(&server, "domain.served").await.len(), 1);
    assert_eq!(events_of(&server, "domain.unserved").await.len(), 1);
}

/// Issue #349: an `acme` domain of an app with one replica is `serving` while
/// an instance is meant to run, and `pending` again once the app stops. On
/// `main` it stayed `pending` for good.
#[tokio::test]
async fn an_acme_domain_is_serving_while_its_app_runs_and_pending_once_it_stops() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "dns",
        "Dns",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let app_id = setup_project_and_app(&server, &owner).await;
    deploy_on(&server, &app_id, &[NODE_A]).await;
    let domain = create_domain(
        &server,
        &owner,
        &app_id,
        "lit.example.com",
        "acme",
        &provider_id,
    )
    .await;
    assert_eq!(domain["serving_status"], "pending");

    let serving = wait_for_serving(&server, &owner, &app_id, "lit.example.com", "serving").await;
    assert!(serving["last_error"].is_null(), "{serving}");

    app_action(&server, &owner, &app_id, "stop").await;
    wait_for_serving(&server, &owner, &app_id, "lit.example.com", "pending").await;
    // Neither change is between `serving` and `unserved`.
    assert!(events_of(&server, "domain.unserved").await.is_empty());
    assert!(events_of(&server, "domain.served").await.is_empty());
}

// ---------------------------------------------------------------------------
// A moved hostname keeps its DNS record (issue #344)
// ---------------------------------------------------------------------------

/// The id of the quoro CNAME of `hostname`, if there is one.
fn cname_id(mock: &Mock, hostname: &str) -> Option<String> {
    mock.records()
        .into_iter()
        .find(|record| record.name == hostname && record.kind == "CNAME")
        .map(|record| record.id)
}

/// Every DNS record write (`POST`, `PATCH`, `DELETE`) the mock received.
fn dns_writes(mock: &Mock) -> Vec<Recorded> {
    mock.requests()
        .into_iter()
        .filter(|request| request.path.contains("/dns_records") && request.method != "GET")
        .collect()
}

#[tokio::test]
async fn a_hostname_that_moves_between_tunnels_keeps_its_cname_record() {
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    // A short drain window (ADR 0194): the tunnel the hostname left keeps
    // its ingress rule for that long, and never touches the record.
    server
        .state
        .hub
        .set_tunnel_drain_window(Duration::from_millis(500));
    let owner = server.owner().await;
    insert_machine(&server, NODE_A, "alpha", &["node"]).await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let node_tunnel = map_tunnel(&server, &owner, &mock, NODE_A, "alpha", &provider_id).await;
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    place_vm(&server, &app_id, NODE_A, "web-a").await;
    create_domain(
        &server,
        &owner,
        &app_id,
        "handoff.example.com",
        "tunnel",
        &provider_id,
    )
    .await;
    settles_on(&mock, "handoff.example.com", &node_tunnel).await;
    let record = cname_id(&mock, "handoff.example.com").expect("the CNAME");
    let before = dns_writes(&mock).len();

    // The node syncs before the edge (node id order), so the node's tunnel
    // drops the hostname first. It must leave the record to the edge, which
    // repoints it: no delete, no second record.
    set_replicas(&server, &owner, &app_id, 2).await;
    settles_on(&mock, "handoff.example.com", &edge_tunnel).await;
    wait_for(|| (!tunnel_routes(&mock, &node_tunnel, "handoff.example.com")).then_some(())).await;
    assert_eq!(cname_id(&mock, "handoff.example.com"), Some(record.clone()));
    let writes: Vec<Recorded> = dns_writes(&mock).split_off(before);
    assert_eq!(writes.len(), 1, "{writes:?}");
    assert_eq!(writes[0].method, "PATCH", "{writes:?}");
    assert!(
        writes[0].path.ends_with(&format!("/dns_records/{record}")),
        "{writes:?}"
    );
    assert_eq!(
        writes[0].body.as_ref().unwrap()["content"],
        format!("{edge_tunnel}.cfargotunnel.com")
    );

    // Back to one replica: the same record moves back to the node.
    set_replicas(&server, &owner, &app_id, 1).await;
    settles_on(&mock, "handoff.example.com", &node_tunnel).await;
    wait_for(|| (!tunnel_routes(&mock, &edge_tunnel, "handoff.example.com")).then_some(())).await;
    assert_eq!(cname_id(&mock, "handoff.example.com"), Some(record.clone()));
    let writes: Vec<Recorded> = dns_writes(&mock).split_off(before);
    assert!(
        writes.iter().all(|write| write.method == "PATCH"),
        "{writes:?}"
    );

    // Deleting the domain still removes the record: the tunnel that serves
    // the hostname deletes it, and the one it moved away from leaves it be.
    let domain_id: String = sqlx::query_scalar("SELECT id FROM domains WHERE hostname = ?")
        .bind("handoff.example.com")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let res = owner
        .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    wait_for(|| {
        cname_id(&mock, "handoff.example.com")
            .is_none()
            .then_some(())
    })
    .await;
}

// ---------------------------------------------------------------------------
// A moved hostname drains on the machine it left (issue #344, ADR 0194)
// ---------------------------------------------------------------------------

/// The route of `hostname` in a desired-state frame, from `spec.routes` or
/// `spec.edge.routes`.
fn route_in<'a>(frame: &'a Value, hostname: &str) -> Option<&'a Value> {
    let spec = &frame["body"]["spec"];
    spec["routes"]
        .as_array()
        .into_iter()
        .chain(spec["edge"]["routes"].as_array())
        .flatten()
        .find(|route| route["hostname"] == hostname)
}

/// The first ingress configuration of `tunnel_id` in `requests` that no
/// longer routes `hostname`.
fn first_config_without<'a>(
    requests: &'a [Recorded],
    tunnel_id: &str,
    hostname: &str,
) -> Option<&'a Recorded> {
    let path = format!("/cfd_tunnel/{tunnel_id}/configurations");
    requests.iter().find(|request| {
        request.method == "PUT"
            && request.path.ends_with(&path)
            && request.body.as_ref().is_some_and(|body| {
                !body["config"]["ingress"]
                    .as_array()
                    .is_some_and(|rules| rules.iter().any(|rule| rule["hostname"] == hostname))
            })
    })
}

/// The DNS write in `requests` that pointed `hostname`'s record at `tunnel_id`.
fn repoint_to<'a>(requests: &'a [Recorded], tunnel_id: &str) -> Option<&'a Recorded> {
    let target = format!("{tunnel_id}.cfargotunnel.com");
    requests.iter().find(|request| {
        request.path.contains("/dns_records")
            && matches!(request.method.as_str(), "PATCH" | "POST")
            && request
                .body
                .as_ref()
                .is_some_and(|body| body["content"] == target)
    })
}

/// Polls `check` for up to `limit`.
async fn wait_longer<T>(limit: Duration, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_moved_hostname_drains_on_the_machine_it_left_for_one_window() {
    let window = Duration::from_secs(2);
    let mock = start_mock().await;
    let server = start_with_cloudflare(&mock.base, &format!("{}/dns-query", mock.base)).await;
    server.state.hub.set_tunnel_drain_window(window);
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "alpha").await;
    insert_machine(&server, NODE_B, "beta", &["node"]).await;
    insert_machine(&server, EDGE, "edge", &["edge"]).await;
    let provider = create_provider(
        &server,
        &owner,
        "tunnel",
        "Tunnel",
        json!({ "account_id": ACCOUNT_ID, "zone_id": ZONE_ID }),
    )
    .await;
    let provider_id = provider["id"].as_str().unwrap().to_string();
    let node_tunnel =
        map_tunnel(&server, &owner, &mock, &node.node_id, "alpha", &provider_id).await;
    let edge_tunnel = map_tunnel(&server, &owner, &mock, EDGE, "edge", &provider_id).await;
    let app_id = setup_project_and_app(&server, &owner).await;
    let vm_id = place_vm(&server, &app_id, &node.node_id, "web-a").await;
    // The instance has a guest address, so the edge can keep it as a
    // backend on the way back.
    sqlx::query("UPDATE vms SET ip = '10.42.1.11' WHERE id = ?")
        .bind(&vm_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let hostname = "drain.example.com";
    create_domain(&server, &owner, &app_id, hostname, "tunnel", &provider_id).await;
    let (mut socket, snapshot) = open(&server, &node).await;
    let route = route_in(&snapshot, hostname).expect("the node serves the hostname");
    assert!(route["draining_until"].is_null(), "{snapshot}");
    settles_on(&mock, hostname, &node_tunnel).await;

    // One to two replicas: the edge takes the hostname. The node keeps its
    // route, marked draining and with its own VM, in every snapshot until
    // the window ends; no snapshot in between drops it.
    let before = mock.requests().len();
    set_replicas(&server, &owner, &app_id, 2).await;
    let route = loop {
        let frame = recv_type(&mut socket, "desired_state").await;
        let route = route_in(&frame, hostname)
            .unwrap_or_else(|| panic!("a snapshot dropped the route before its window: {frame}"))
            .clone();
        if route["draining_until"].is_string() {
            break route;
        }
    };
    assert_eq!(route["vm_ids"], json!([vm_id]));
    assert_eq!(route["tls_mode"], "tunnel");
    settles_on(&mock, hostname, &edge_tunnel).await;
    assert!(
        tunnel_routes(&mock, &node_tunnel, hostname),
        "the node's tunnel dropped the hostname inside the window"
    );
    loop {
        let frame = recv_type(&mut socket, "desired_state").await;
        match route_in(&frame, hostname) {
            None => break,
            Some(route) => assert!(route["draining_until"].is_string(), "{frame}"),
        }
    }
    wait_longer(window * 3, || {
        (!tunnel_routes(&mock, &node_tunnel, hostname)).then_some(())
    })
    .await;
    let requests = mock.requests().split_off(before);
    let repoint = repoint_to(&requests, &edge_tunnel).expect("the edge repointed the CNAME");
    let dropped = first_config_without(&requests, &node_tunnel, hostname)
        .expect("the node's tunnel dropped the hostname");
    let kept = dropped.at.duration_since(repoint.at);
    assert!(
        kept >= window && kept < window + Duration::from_secs(3),
        "the node's tunnel kept the hostname {kept:?} after the repoint"
    );
    // The node's tunnel never wrote the record back to itself.
    let node_target = format!("{node_tunnel}.cfargotunnel.com");
    assert!(
        requests
            .iter()
            .filter(|request| request.path.contains("/dns_records") && request.method != "GET")
            .all(|request| request
                .body
                .as_ref()
                .is_none_or(|body| body["content"] != node_target.as_str())),
        "{requests:?}"
    );

    // Back to one replica: the hostname returns to the node, and the edge's
    // tunnel keeps it for one window after the node repointed the CNAME.
    let before = mock.requests().len();
    set_replicas(&server, &owner, &app_id, 1).await;
    settles_on(&mock, hostname, &node_tunnel).await;
    assert!(
        tunnel_routes(&mock, &edge_tunnel, hostname),
        "the edge's tunnel dropped the hostname inside the window"
    );
    wait_longer(window * 3, || {
        (!tunnel_routes(&mock, &edge_tunnel, hostname)).then_some(())
    })
    .await;
    let requests = mock.requests().split_off(before);
    let repoint = repoint_to(&requests, &node_tunnel).expect("the node repointed the CNAME");
    let dropped = first_config_without(&requests, &edge_tunnel, hostname)
        .expect("the edge's tunnel dropped the hostname");
    let kept = dropped.at.duration_since(repoint.at);
    assert!(
        kept >= window && kept < window + Duration::from_secs(3),
        "the edge's tunnel kept the hostname {kept:?} after the repoint"
    );
    let edge_target = format!("{edge_tunnel}.cfargotunnel.com");
    assert!(
        requests
            .iter()
            .filter(|request| request.path.contains("/dns_records") && request.method != "GET")
            .all(|request| request
                .body
                .as_ref()
                .is_none_or(|body| body["content"] != edge_target.as_str())),
        "{requests:?}"
    );
    // The record only ever moved: no tunnel deleted it on the way.
    assert!(
        dns_writes(&mock)
            .iter()
            .all(|write| write.method != "DELETE"),
        "{:?}",
        dns_writes(&mock)
    );
    assert!(tunnel_routes(&mock, &node_tunnel, hostname));
}
