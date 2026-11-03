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
    create_domain(
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

    // PUT maps the node; the manager creates the tunnel and resends the
    // snapshot with the run token.
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
    create_domain(
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
