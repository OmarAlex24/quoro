//! Integration tests for [`quoro_providers::cloudflare`] against a local mock
//! of the Cloudflare API and its DNS-over-HTTPS endpoint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, put};
use axum::{Json, Router};
use quoro_providers::cloudflare::{Cloudflare, IngressRule, RECORD_TAG, Tunnel};
use quoro_providers::{DnsProvider, ProviderError, RecordId, Token};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const TOKEN: &str = "cf-test-token-9f3a2b7c";

type Shared = Arc<Mutex<MockState>>;

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    query: String,
    authorization: Option<String>,
    accept: Option<String>,
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
    /// The edge still lists connections: deletion fails with 1022 until
    /// they are cleaned up.
    connected: bool,
}

struct Script {
    method: &'static str,
    path_contains: &'static str,
    status: u16,
    body: Value,
    remaining: usize,
    retry_after: Option<&'static str>,
}

#[derive(Default)]
struct MockState {
    requests: Vec<Recorded>,
    records: Vec<MockRecord>,
    tunnels: Vec<MockTunnel>,
    next_id: usize,
    scripts: Vec<Script>,
}

#[derive(Default)]
struct Mock {
    state: Shared,
    base: String,
}

async fn start_mock() -> Mock {
    let state: Shared = Arc::new(Mutex::new(MockState::default()));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Mock {
        state,
        base: format!("http://{addr}"),
    }
}

impl Mock {
    fn client(&self) -> Cloudflare {
        Cloudflare::with_endpoints(
            Token::new(TOKEN.to_string()),
            &self.base,
            &format!("{}/dns-query", self.base),
        )
        .unwrap()
    }

    fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    fn count(&self, method: &str, path_contains: &str) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.method == method && request.path.contains(path_contains))
            .count()
    }

    fn last_body(&self, method: &str, path_contains: &str) -> Value {
        self.requests()
            .into_iter()
            .rev()
            .find(|request| request.method == method && request.path.contains(path_contains))
            .and_then(|request| request.body)
            .expect("a matching request with a JSON body")
    }

    fn seed(&self, record: MockRecord) {
        self.state.lock().unwrap().records.push(record);
    }

    fn seed_tunnel(&self, id: &str, name: &str) {
        self.state.lock().unwrap().tunnels.push(MockTunnel {
            id: id.to_string(),
            name: name.to_string(),
            connected: false,
        });
    }

    fn script(&self, method: &'static str, path_contains: &'static str, status: u16, body: Value) {
        self.script_with_retry_after(method, path_contains, status, body, None);
    }

    fn script_with_retry_after(
        &self,
        method: &'static str,
        path_contains: &'static str,
        status: u16,
        body: Value,
        retry_after: Option<&'static str>,
    ) {
        self.state.lock().unwrap().scripts.push(Script {
            method,
            path_contains,
            status,
            body,
            remaining: 1,
            retry_after,
        });
    }

    fn record(&self, id: &str) -> Option<MockRecord> {
        self.state
            .lock()
            .unwrap()
            .records
            .iter()
            .find(|record| record.id == id)
            .cloned()
    }

    fn record_count(&self) -> usize {
        self.state.lock().unwrap().records.len()
    }

    fn assert_all_authorized(&self) {
        let expected = format!("Bearer {TOKEN}");
        for request in self.requests() {
            if request.path.ends_with("/dns-query") {
                // The DoH resolver must never see the API token.
                assert_eq!(request.authorization, None, "DoH query carried the token");
                continue;
            }
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

fn router(state: Shared) -> Router {
    Router::new()
        .route("/user/tokens/verify", get(verify_user))
        .route("/accounts/{account}/tokens/verify", get(verify_account))
        .route(
            "/accounts/{account}/cfd_tunnel",
            get(list_tunnels).post(create_tunnel),
        )
        .route("/accounts/{account}/cfd_tunnel/{id}", delete(delete_tunnel))
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/connections",
            delete(delete_connections),
        )
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
        .route("/dns-query", get(doh))
        .with_state(state)
}

fn ok(result: Value) -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK,
        Json(json!({"success": true, "errors": [], "result": result})),
    )
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

/// Records the request and answers with a scripted response when one matches.
fn prepare(
    state: &Shared,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Option<Value>,
) -> Option<Response> {
    let mut state = state.lock().unwrap();
    state.requests.push(Recorded {
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        query: uri.query().unwrap_or_default().to_string(),
        authorization: headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        accept: headers
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        body,
    });
    let index = state.scripts.iter().position(|script| {
        script.remaining > 0
            && script.method == method.as_str()
            && uri.path().contains(script.path_contains)
    })?;
    let script = &mut state.scripts[index];
    script.remaining -= 1;
    let mut response = (
        StatusCode::from_u16(script.status).unwrap(),
        Json(script.body.clone()),
    )
        .into_response();
    if let Some(retry_after) = script.retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, retry_after.parse().unwrap());
    }
    Some(response)
}

async fn verify_user(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    ok(json!({"id": "tok-1", "status": "active"})).into_response()
}

async fn verify_account(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    ok(json!({"id": "tok-1", "status": "active"})).into_response()
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

async fn list_dns(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
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
    ok(Value::Array(result)).into_response()
}

async fn create_dns(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    _path: Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes);
    if let Some(response) = prepare(&state, &method, &uri, &headers, body.clone()) {
        return response;
    }
    let body = body.unwrap_or(Value::Null);
    let mut state = state.lock().unwrap();
    state.next_id += 1;
    let record = MockRecord {
        id: format!("rec{}", state.next_id),
        name: body["name"].as_str().unwrap_or_default().to_string(),
        kind: body["type"].as_str().unwrap_or_default().to_string(),
        content: body["content"].as_str().unwrap_or_default().to_string(),
        proxied: body["proxied"].as_bool().unwrap_or(false),
        comment: body["comment"].as_str().map(str::to_string),
    };
    state.records.push(record.clone());
    drop(state);
    ok(record_json(&record)).into_response()
}

async fn patch_dns(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes);
    if let Some(response) = prepare(&state, &method, &uri, &headers, body.clone()) {
        return response;
    }
    let body = body.unwrap_or(Value::Null);
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    let Some(record) = state.records.iter_mut().find(|record| record.id == id) else {
        return fail(404, 81044, "Record does not exist").into_response();
    };
    if let Some(content) = body["content"].as_str() {
        record.content = content.to_string();
    }
    if let Some(proxied) = body["proxied"].as_bool() {
        record.proxied = proxied;
    }
    let record = record.clone();
    drop(state);
    ok(record_json(&record)).into_response()
}

async fn delete_dns(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    let before = state.records.len();
    state.records.retain(|record| record.id != id);
    if state.records.len() == before {
        return fail(404, 81044, "Record does not exist").into_response();
    }
    ok(json!({"id": id})).into_response()
}

async fn list_tunnels(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let name = params.get("name").cloned().unwrap_or_default();
    let state = state.lock().unwrap();
    let result: Vec<Value> = state
        .tunnels
        .iter()
        .filter(|tunnel| tunnel.name == name)
        .map(|tunnel| json!({"id": tunnel.id, "name": tunnel.name}))
        .collect();
    ok(Value::Array(result)).into_response()
}

async fn create_tunnel(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    _path: Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes);
    if let Some(response) = prepare(&state, &method, &uri, &headers, body.clone()) {
        return response;
    }
    let body = body.unwrap_or(Value::Null);
    let mut state = state.lock().unwrap();
    state.next_id += 1;
    let tunnel = MockTunnel {
        id: format!("tun{}", state.next_id),
        name: body["name"].as_str().unwrap_or_default().to_string(),
        connected: true,
    };
    state.tunnels.push(tunnel.clone());
    drop(state);
    ok(json!({"id": tunnel.id, "name": tunnel.name})).into_response()
}

async fn tunnel_token(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let id = params.get("id").cloned().unwrap_or_default();
    let state = state.lock().unwrap();
    if !state.tunnels.iter().any(|tunnel| tunnel.id == id) {
        return fail(404, 1003, "Tunnel not found").into_response();
    }
    ok(json!(format!("tunnel-token-{id}"))).into_response()
}

async fn put_configuration(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    _path: Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    let body = parse_body(&bytes);
    if let Some(response) = prepare(&state, &method, &uri, &headers, body.clone()) {
        return response;
    }
    ok(json!({"tunnel_id": "tun1"})).into_response()
}

async fn delete_tunnel(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    if state.tunnels.iter().any(|t| t.id == id && t.connected) {
        return fail(
            400,
            1022,
            "This tunnel has active connections. Please stop all cloudflared replicas",
        );
    }
    state.tunnels.retain(|tunnel| tunnel.id != id);
    ok(json!({"id": id})).into_response()
}

async fn delete_connections(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let id = params.get("id").cloned().unwrap_or_default();
    let mut state = state.lock().unwrap();
    let Some(tunnel) = state.tunnels.iter_mut().find(|t| t.id == id) else {
        return fail(404, 1003, "tunnel not found");
    };
    tunnel.connected = false;
    ok(json!({})).into_response()
}

async fn doh(
    State(state): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> Response {
    if let Some(response) = prepare(&state, &method, &uri, &headers, parse_body(&bytes)) {
        return response;
    }
    let name = params.get("name").cloned().unwrap_or_default();
    let state = state.lock().unwrap();
    let answers: Vec<Value> = state
        .records
        .iter()
        .filter(|record| record.kind == "TXT" && record.name == name)
        .map(|record| json!({"name": record.name, "type": 16, "data": record.content}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"Status": 0, "Answer": answers})),
    )
        .into_response()
}

fn tagged_cname(id: &str, name: &str, content: &str, proxied: bool) -> MockRecord {
    MockRecord {
        id: id.to_string(),
        name: name.to_string(),
        kind: "CNAME".to_string(),
        content: content.to_string(),
        proxied,
        comment: Some(RECORD_TAG.to_string()),
    }
}

#[tokio::test]
async fn create_txt_is_tagged_and_delete_tolerates_404() {
    let mock = start_mock().await;
    let dns = mock.client().dns("zone1");

    let id = dns
        .create_txt("_acme-challenge.app.example.com", "challenge-value")
        .await
        .unwrap();
    assert_eq!(id, RecordId("rec1".to_string()));

    let body = mock.last_body("POST", "/zones/zone1/dns_records");
    assert_eq!(body["type"], "TXT");
    assert_eq!(body["name"], "_acme-challenge.app.example.com");
    assert_eq!(body["content"], "challenge-value");
    assert_eq!(body["ttl"], 60);
    assert_eq!(body["comment"], RECORD_TAG);

    // Deleting an unknown record is not an error (the mock answers 404).
    dns.delete_record(&RecordId("gone".to_string()))
        .await
        .unwrap();
    assert!(mock.record("gone").is_none());

    mock.assert_all_authorized();
}

#[tokio::test]
async fn upsert_cname_creates_when_absent() {
    let mock = start_mock().await;
    let dns = mock.client().dns("zone1");

    let id = dns
        .upsert_cname("app.example.com", "target.example.net", true)
        .await
        .unwrap();
    assert_eq!(id, RecordId("rec1".to_string()));

    let body = mock.last_body("POST", "/zones/zone1/dns_records");
    assert_eq!(body["type"], "CNAME");
    assert_eq!(body["name"], "app.example.com");
    assert_eq!(body["content"], "target.example.net");
    assert_eq!(body["proxied"], true);
    assert_eq!(body["ttl"], 1);
    assert_eq!(body["comment"], RECORD_TAG);

    assert_eq!(mock.count("PATCH", "/dns_records"), 0);
    mock.assert_all_authorized();
}

#[tokio::test]
async fn upsert_cname_patches_only_when_changed() {
    let mock = start_mock().await;
    mock.seed(tagged_cname(
        "rec1",
        "app.example.com",
        "old.example.net",
        true,
    ));
    let dns = mock.client().dns("zone1");

    let id = dns
        .upsert_cname("app.example.com", "new.example.net", false)
        .await
        .unwrap();
    assert_eq!(id, RecordId("rec1".to_string()));
    assert_eq!(mock.count("POST", "/dns_records"), 0);
    assert_eq!(mock.count("PATCH", "/dns_records/rec1"), 1);

    let body = mock.last_body("PATCH", "/dns_records/rec1");
    assert_eq!(body["content"], "new.example.net");
    assert_eq!(body["proxied"], false);
    assert!(body.get("ttl").is_none());
    assert!(body.get("comment").is_none());

    let stored = mock.record("rec1").unwrap();
    assert_eq!(stored.content, "new.example.net");
    assert!(!stored.proxied);

    // An identical upsert returns the id without a PATCH.
    let id = dns
        .upsert_cname("app.example.com", "new.example.net", false)
        .await
        .unwrap();
    assert_eq!(id, RecordId("rec1".to_string()));
    assert_eq!(mock.count("PATCH", "/dns_records/rec1"), 1);

    mock.assert_all_authorized();
}

#[tokio::test]
async fn upsert_cname_refuses_foreign_records() {
    let mock = start_mock().await;
    mock.seed(MockRecord {
        id: "hand-a".to_string(),
        name: "app.example.com".to_string(),
        kind: "A".to_string(),
        content: "203.0.113.7".to_string(),
        proxied: false,
        comment: None,
    });
    let dns = mock.client().dns("zone1");

    let err = dns
        .upsert_cname("app.example.com", "target.example.net", true)
        .await
        .unwrap_err();
    match &err {
        ProviderError::Conflict(message) => assert!(message.contains('A'), "{message}"),
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert!(!err.to_string().contains(TOKEN));
    assert_eq!(mock.count("POST", "/dns_records"), 0);
    assert_eq!(mock.count("PATCH", "/dns_records"), 0);
    assert_eq!(mock.count("DELETE", "/dns_records"), 0);
    assert_eq!(mock.record("hand-a").unwrap().content, "203.0.113.7");

    // An untagged CNAME is equally off limits.
    mock.seed(MockRecord {
        id: "hand-cname".to_string(),
        name: "www.example.com".to_string(),
        kind: "CNAME".to_string(),
        content: "hand.example.net".to_string(),
        proxied: false,
        comment: None,
    });
    let err = dns
        .upsert_cname("www.example.com", "target.example.net", true)
        .await
        .unwrap_err();
    match &err {
        ProviderError::Conflict(message) => assert!(message.contains("CNAME"), "{message}"),
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert_eq!(mock.count("PATCH", "/dns_records"), 0);
    assert_eq!(
        mock.record("hand-cname").unwrap().content,
        "hand.example.net"
    );

    mock.assert_all_authorized();
}

#[tokio::test]
async fn delete_cname_removes_only_tagged_cnames() {
    let mock = start_mock().await;
    mock.seed(tagged_cname(
        "rec1",
        "same.example.com",
        "q.example.net",
        true,
    ));
    mock.seed(MockRecord {
        id: "rec2".to_string(),
        name: "same.example.com".to_string(),
        kind: "CNAME".to_string(),
        content: "hand.example.net".to_string(),
        proxied: false,
        comment: None,
    });
    mock.seed(MockRecord {
        id: "rec3".to_string(),
        name: "same.example.com".to_string(),
        kind: "A".to_string(),
        content: "203.0.113.9".to_string(),
        proxied: false,
        comment: Some(RECORD_TAG.to_string()),
    });
    let dns = mock.client().dns("zone1");

    dns.delete_cname("same.example.com", "q.example.net")
        .await
        .unwrap();
    assert!(mock.record("rec1").is_none());
    assert!(mock.record("rec2").is_some());
    assert!(mock.record("rec3").is_some());
    assert_eq!(mock.count("DELETE", "/dns_records/rec1"), 1);
    assert_eq!(mock.count("DELETE", "/dns_records/rec2"), 0);
    assert_eq!(mock.count("DELETE", "/dns_records/rec3"), 0);

    // Nothing tagged: the call succeeds and touches nothing.
    dns.delete_cname("missing.example.com", "q.example.net")
        .await
        .unwrap();
    assert_eq!(mock.record_count(), 2);

    mock.assert_all_authorized();
}

#[tokio::test]
async fn delete_cname_keeps_a_cname_that_points_elsewhere() {
    // A hostname that moved to another tunnel keeps the CNAME that tunnel
    // wrote: the old tunnel's delete must not remove it.
    let mock = start_mock().await;
    mock.seed(tagged_cname(
        "rec1",
        "moved.example.com",
        "new-tunnel.cfargotunnel.com",
        true,
    ));
    let dns = mock.client().dns("zone1");

    dns.delete_cname("moved.example.com", "old-tunnel.cfargotunnel.com")
        .await
        .unwrap();
    assert!(mock.record("rec1").is_some());
    assert_eq!(mock.count("DELETE", "/dns_records/rec1"), 0);

    dns.delete_cname("moved.example.com", "new-tunnel.cfargotunnel.com")
        .await
        .unwrap();
    assert!(mock.record("rec1").is_none());
    mock.assert_all_authorized();
}

#[tokio::test]
async fn txt_visible_matches_quoted_and_plain_answers() {
    let mock = start_mock().await;
    mock.seed(MockRecord {
        id: "q".to_string(),
        name: "_acme.example.com".to_string(),
        kind: "TXT".to_string(),
        content: "\"quoted-value\"".to_string(),
        proxied: false,
        comment: None,
    });
    mock.seed(MockRecord {
        id: "p".to_string(),
        name: "plain.example.com".to_string(),
        kind: "TXT".to_string(),
        content: "plain-value".to_string(),
        proxied: false,
        comment: None,
    });
    let dns = mock.client().dns("zone1");

    assert!(
        dns.txt_visible("_acme.example.com", "quoted-value")
            .await
            .unwrap()
    );
    assert!(!dns.txt_visible("_acme.example.com", "other").await.unwrap());
    assert!(
        dns.txt_visible("plain.example.com", "plain-value")
            .await
            .unwrap()
    );
    assert!(
        !dns.txt_visible("missing.example.com", "plain-value")
            .await
            .unwrap()
    );

    let queries: Vec<String> = mock.requests().iter().map(|r| r.query.clone()).collect();
    assert!(queries.iter().all(|query| query.contains("type=TXT")));
    // The API token must never be sent to the DoH resolver.
    assert!(
        mock.requests().iter().all(|r| r.authorization.is_none()),
        "a DoH query carried an Authorization header"
    );
    assert!(
        mock.requests()
            .iter()
            .all(|request| request.accept.as_deref() == Some("application/dns-json"))
    );

    mock.assert_all_authorized();
}

#[tokio::test]
async fn tunnels_are_found_created_and_read() {
    let mock = start_mock().await;
    mock.seed_tunnel("tun-a", "alpha");
    mock.seed_tunnel("tun-b", "beta");
    let client = mock.client();

    let found = client.find_tunnel("acct1", "alpha").await.unwrap();
    assert_eq!(
        found,
        Some(Tunnel {
            id: "tun-a".to_string(),
            name: "alpha".to_string(),
        })
    );
    assert!(client.find_tunnel("acct1", "alp").await.unwrap().is_none());
    assert!(
        client
            .find_tunnel("acct1", "gamma")
            .await
            .unwrap()
            .is_none()
    );

    let list = mock.requests();
    let query = list
        .iter()
        .map(|request| request.query.clone())
        .find(|query| query.contains("name=alpha"))
        .expect("a list request for alpha");
    assert!(query.contains("is_deleted=false"), "{query}");

    let created = client.create_tunnel("acct1", "gamma").await.unwrap();
    assert_eq!(created.name, "gamma");
    let body = mock.last_body("POST", "/accounts/acct1/cfd_tunnel");
    assert_eq!(body["name"], "gamma");
    assert_eq!(body["config_src"], "cloudflare");

    let token = client.tunnel_token("acct1", &created.id).await.unwrap();
    assert_eq!(token.expose(), format!("tunnel-token-{}", created.id));
    assert!(!format!("{token:?}").contains(token.expose()));
    assert!(!format!("{:?}", mock.client()).contains(TOKEN));

    // A freshly run tunnel still has edge connections: the client cleans
    // them up before deleting, so the delete does not fail with 1022.
    client.delete_tunnel("acct1", &created.id).await.unwrap();
    let tunnels = mock.state.lock().unwrap().tunnels.len();
    assert_eq!(tunnels, 2);
    assert_eq!(
        mock.count("DELETE", &format!("/cfd_tunnel/{}/connections", created.id)),
        1
    );

    mock.assert_all_authorized();
}

#[tokio::test]
async fn put_tunnel_ingress_sorts_and_appends_catch_all() {
    let mock = start_mock().await;
    let client = mock.client();
    let rules = vec![
        IngressRule {
            hostname: "b.example.com".to_string(),
            service: "http://10.0.0.2:8080".to_string(),
        },
        IngressRule {
            hostname: "a.example.com".to_string(),
            service: "http://10.0.0.1:8080".to_string(),
        },
    ];

    client
        .put_tunnel_ingress("acct1", "tun1", &rules)
        .await
        .unwrap();

    let body = mock.last_body("PUT", "/accounts/acct1/cfd_tunnel/tun1/configurations");
    let ingress = body["config"]["ingress"].as_array().unwrap();
    assert_eq!(ingress.len(), 3);
    assert_eq!(ingress[0]["hostname"], "a.example.com");
    assert_eq!(ingress[0]["service"], "http://10.0.0.1:8080");
    assert_eq!(ingress[1]["hostname"], "b.example.com");
    assert_eq!(ingress[1]["service"], "http://10.0.0.2:8080");
    assert_eq!(
        ingress[2],
        json!({"service": "http_status:404"}),
        "the catch-all is last and has no hostname"
    );

    mock.assert_all_authorized();
}

#[tokio::test]
async fn verify_token_falls_back_to_the_account_endpoint() {
    let mock = start_mock().await;
    let client = mock.client();

    client.verify_token(None).await.unwrap();
    mock.script(
        "GET",
        "/user/tokens/verify",
        403,
        json!({
            "success": false,
            "errors": [{"code": 9109, "message": "Unauthorized to access requested resource"}],
            "result": null,
        }),
    );
    client.verify_token(Some("acct1")).await.unwrap();
    assert_eq!(mock.count("GET", "/accounts/acct1/tokens/verify"), 1);

    mock.assert_all_authorized();
}

#[tokio::test]
async fn errors_map_to_the_provider_error_kinds() {
    let mock = start_mock().await;
    let client = mock.client();

    // 403 with an envelope is an auth failure.
    mock.script(
        "GET",
        "/cfd_tunnel",
        403,
        json!({
            "success": false,
            "errors": [{"code": 10000, "message": "Authentication error"}],
            "result": null,
        }),
    );
    let err = client.find_tunnel("acct1", "alpha").await.unwrap_err();
    assert!(
        matches!(err, ProviderError::Unauthorized(_)),
        "expected Unauthorized, got {err:?}"
    );
    assert!(err.to_string().contains("10000: Authentication error"));
    assert!(!err.to_string().contains(TOKEN));

    // 500 then 200: a GET is retried and succeeds.
    mock.script_with_retry_after(
        "GET",
        "/cfd_tunnel",
        500,
        json!({
            "success": false,
            "errors": [{"code": 500, "message": "Internal error"}],
            "result": null,
        }),
        Some("0"),
    );
    mock.seed_tunnel("tun-a", "alpha");
    let before = mock.count("GET", "/accounts/acct1/cfd_tunnel");
    let found = client.find_tunnel("acct1", "alpha").await.unwrap();
    assert!(found.is_some());
    assert_eq!(mock.count("GET", "/accounts/acct1/cfd_tunnel"), before + 2);

    // success=false on a 200 is an Api error even though the HTTP status is fine.
    mock.script(
        "GET",
        "/cfd_tunnel",
        200,
        json!({
            "success": false,
            "errors": [{"code": 1001, "message": "nope"}],
            "result": [],
        }),
    );
    let err = client.find_tunnel("acct1", "alpha").await.unwrap_err();
    match &err {
        ProviderError::Api { status, message } => {
            assert_eq!(*status, 200);
            assert!(message.contains("1001: nope"), "{message}");
        }
        other => panic!("expected an Api error, got {other:?}"),
    }
    assert!(!err.to_string().contains(TOKEN));

    // A POST is never retried.
    mock.script(
        "POST",
        "/zones/zone1/dns_records",
        500,
        json!({
            "success": false,
            "errors": [{"code": 500, "message": "Internal error"}],
            "result": null,
        }),
    );
    let dns = client.dns("zone1");
    let err = dns
        .create_txt("_acme.example.com", "value")
        .await
        .unwrap_err();
    match &err {
        ProviderError::Api { status, message } => {
            assert_eq!(*status, 500);
            assert!(message.contains("500: Internal error"), "{message}");
        }
        other => panic!("expected an Api error, got {other:?}"),
    }
    assert_eq!(mock.count("POST", "/zones/zone1/dns_records"), 1);
    assert!(!err.to_string().contains(TOKEN));

    mock.assert_all_authorized();
}

#[tokio::test]
async fn debug_output_never_contains_the_token() {
    let mock = start_mock().await;
    let client = mock.client();

    let debug = format!("{client:?}");
    assert!(!debug.contains(TOKEN), "{debug}");
    assert!(debug.contains("redacted"), "{debug}");

    // Every error produced by a failing call stays free of the credential.
    mock.script(
        "GET",
        "/cfd_tunnel",
        404,
        json!({
            "success": false,
            "errors": [{"code": 1003, "message": "Tunnel not found"}],
            "result": null,
        }),
    );
    let err = client.find_tunnel("acct1", "alpha").await.unwrap_err();
    assert!(matches!(err, ProviderError::NotFound(_)));
    assert!(!err.to_string().contains(TOKEN));
}
