//! End-to-end tests for the two [`Ingress`] backends.
//!
//! The tunnel backend is split along the trust boundary of ADR 0094. The
//! primary's `TunnelController` runs against a hand-written Cloudflare API
//! (hyper on `127.0.0.1:0`) that keeps tunnels, ingress configurations and DNS
//! records in memory and records every request; the node's `TunnelIngress`
//! holds no Cloudflare client, so the tests also assert that the mock records
//! no request during node-side work. `cloudflared` is a shell script that
//! records its argv and environment and then sleeps, and the connector's
//! metrics endpoint is a listener that answers 200.
//!
//! The public backend uses a recording [`DnsProvider`] and an ACME directory
//! nothing listens on, plus a certificate stored in the [`CertStore`] before
//! the ingress starts, so the TLS listener has something to serve without a CA.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use quoro_ingress::acme::{AcmeConfig, renewal_due};
use quoro_ingress::certs::CertStore;
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_ingress::public::{PublicConfig, PublicIngress};
use quoro_ingress::tunnel::{TunnelConfig, TunnelController, TunnelIngress, TunnelRun};
use quoro_ingress::{Ingress, IngressError, IngressEvent, Route, RouteTable, TlsMode};
use quoro_providers::cloudflare::Cloudflare;
use quoro_providers::{DnsProvider, ProviderError, RecordId, Token};
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_rustls::TlsConnector;

const API_TOKEN: &str = "api-token-ABC";
const RUN_TOKEN: &str = "tunnel-run-token-XYZ";
const APP_BODY: &str = "hello from app";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn tmp_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn route(hostname: &str, backend: SocketAddr, tls: TlsMode) -> Route {
    Route {
        hostname: hostname.to_string(),
        backends: vec![backend],
        tls,
        retry: quoro_ingress::RetryPolicy::Unsent,
        response_timeout: None,
    }
}

fn status_line(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in response: {response:?}"))
}

/// GET over plain HTTP with an explicit `Host` header.
async fn http_get(
    addr: SocketAddr,
    host: &str,
    path: &str,
) -> (StatusCode, hyper::HeaderMap, String) {
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let request = Request::builder()
        .uri(format!("http://{addr}{path}"))
        .header(hyper::header::HOST, host)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

/// The application every route points at: 200 `hello from app`, and the value
/// of `X-Forwarded-Proto` it received echoed back in `X-Seen-Proto`.
async fn spawn_app() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    let proto = request
                        .headers()
                        .get("x-forwarded-proto")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let seen =
                        HeaderValue::from_str(&proto).unwrap_or(HeaderValue::from_static(""));
                    let mut response =
                        Response::new(Full::new(Bytes::from_static(APP_BODY.as_bytes())));
                    response.headers_mut().insert("x-seen-proto", seen);
                    Ok::<_, Infallible>(response)
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

/// Waits for an event matching `predicate`, using the wall clock only to bound
/// the wait.
async fn wait_for_event(
    events: &mut broadcast::Receiver<IngressEvent>,
    predicate: impl Fn(&IngressEvent) -> bool,
    timeout: Duration,
    label: &str,
) -> IngressEvent {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {label}");
        match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Ok(event)) => {
                if predicate(&event) {
                    return event;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                panic!("event channel lagged by {skipped} while waiting for {label}");
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                panic!("event channel closed while waiting for {label}");
            }
            Err(_) => panic!("timed out waiting for {label}"),
        }
    }
}

async fn shutdown_within_15s<I: Ingress>(ingress: &I) {
    tokio::time::timeout(Duration::from_secs(15), ingress.shutdown())
        .await
        .expect("shutdown returned within 15 s");
}

// ---------------------------------------------------------------------------
// The mock Cloudflare API
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    query: String,
    body: Option<Value>,
}

#[derive(Clone, Debug)]
struct MockRecord {
    id: String,
    kind: String,
    name: String,
    content: String,
    proxied: bool,
    comment: Option<String>,
}

#[derive(Default)]
struct MockState {
    tunnels: Vec<Value>,
    configurations: Vec<Value>,
    records: Vec<MockRecord>,
    requests: Vec<RecordedRequest>,
    next_tunnel: u64,
    next_record: u64,
}

struct MockCloudflare {
    addr: SocketAddr,
    state: Arc<Mutex<MockState>>,
}

impl MockCloudflare {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockState::default()));
        let serving = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&serving);
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        mock_cloudflare(Arc::clone(&state), request)
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { addr, state }
    }

    fn api_base(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().unwrap()
    }

    fn requests_matching(&self, method: &str, path: &str) -> Vec<RecordedRequest> {
        self.lock()
            .requests
            .iter()
            .filter(|request| request.method == method && request.path == path)
            .cloned()
            .collect()
    }

    fn records(&self) -> Vec<MockRecord> {
        self.lock().records.clone()
    }

    fn tunnels(&self) -> Vec<Value> {
        self.lock().tunnels.clone()
    }

    fn last_configuration(&self) -> Option<Value> {
        self.lock().configurations.last().cloned()
    }

    fn add_record(&self, record: MockRecord) {
        self.lock().records.push(record);
    }

    fn request_count(&self) -> usize {
        self.lock().requests.len()
    }
}

fn json_response(value: Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&value).unwrap();
    let mut response = Response::new(Full::new(Bytes::from(body)));
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// The Cloudflare envelope every successful call answers with.
fn envelope(result: Value) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(json_response(
        json!({"success": true, "errors": [], "result": result}),
    ))
}

fn failure(status: u16, message: &str) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut response = json_response(json!({
        "success": false,
        "errors": [{"code": status, "message": message}],
        "result": null,
    }));
    *response.status_mut() = StatusCode::from_u16(status).unwrap();
    Ok(response)
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

async fn mock_cloudflare(
    state: Arc<Mutex<MockState>>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let body = request
        .into_body()
        .collect()
        .await
        .ok()
        .and_then(|collected| {
            let bytes = collected.to_bytes();
            serde_json::from_slice::<Value>(&bytes).ok()
        });
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    let mut state = state.lock().unwrap();
    state.requests.push(RecordedRequest {
        method: method.clone(),
        path: path.clone(),
        query: query.clone(),
        body: body.clone(),
    });

    match (method.as_str(), segments.as_slice()) {
        ("GET", ["accounts", _account, "cfd_tunnel"]) => {
            let name = query_value(&query, "name");
            let tunnels: Vec<Value> = state
                .tunnels
                .iter()
                .filter(|tunnel| {
                    name.as_deref()
                        .is_none_or(|name| tunnel["name"].as_str() == Some(name))
                })
                .cloned()
                .collect();
            envelope(json!(tunnels))
        }
        ("POST", ["accounts", _account, "cfd_tunnel"]) => {
            state.next_tunnel += 1;
            let id = format!("0f0e0000-0000-4000-8000-{:012}", state.next_tunnel);
            let name = body
                .as_ref()
                .and_then(|body| body["name"].as_str())
                .unwrap_or_default()
                .to_string();
            let tunnel = json!({"id": id, "name": name});
            state.tunnels.push(tunnel.clone());
            envelope(tunnel)
        }
        ("GET", ["accounts", _account, "cfd_tunnel", _id, "token"]) => envelope(json!(RUN_TOKEN)),
        ("PUT", ["accounts", _account, "cfd_tunnel", _id, "configurations"]) => {
            state.configurations.push(body.unwrap_or(Value::Null));
            envelope(json!({}))
        }
        ("GET", ["zones", _zone, "dns_records"]) => {
            let name = query_value(&query, "name.exact");
            let records: Vec<Value> = state
                .records
                .iter()
                .filter(|record| name.as_deref().is_none_or(|name| record.name == name))
                .map(record_json)
                .collect();
            envelope(json!(records))
        }
        ("POST", ["zones", _zone, "dns_records"]) => {
            state.next_record += 1;
            let body = body.unwrap_or_else(|| json!({}));
            let record = MockRecord {
                id: format!("rec-{}", state.next_record),
                kind: body["type"].as_str().unwrap_or("TXT").to_string(),
                name: body["name"].as_str().unwrap_or_default().to_string(),
                content: body["content"].as_str().unwrap_or_default().to_string(),
                proxied: body["proxied"].as_bool().unwrap_or(false),
                comment: body["comment"].as_str().map(str::to_string),
            };
            let response = record_json(&record);
            state.records.push(record);
            envelope(response)
        }
        ("PATCH", ["zones", _zone, "dns_records", id]) => {
            let Some(record) = state.records.iter_mut().find(|record| record.id == *id) else {
                return failure(404, "record not found");
            };
            if let Some(body) = &body {
                if let Some(content) = body["content"].as_str() {
                    record.content = content.to_string();
                }
                if let Some(proxied) = body["proxied"].as_bool() {
                    record.proxied = proxied;
                }
            }
            envelope(record_json(record))
        }
        ("DELETE", ["zones", _zone, "dns_records", id]) => {
            state.records.retain(|record| record.id != *id);
            envelope(json!({"id": id}))
        }
        (method, segments) => failure(
            404,
            &format!("no mock route for {method} /{}", segments.join("/")),
        ),
    }
}

/// The value of one `application/x-www-form-urlencoded` query parameter.
fn query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then(|| percent_decode(value))
    })
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).ok();
                match hex.and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
                    Some(byte) => {
                        decoded.push(byte as char);
                        index += 3;
                    }
                    None => {
                        decoded.push('%');
                        index += 1;
                    }
                }
            }
            b'+' => {
                decoded.push(' ');
                index += 1;
            }
            byte => {
                decoded.push(byte as char);
                index += 1;
            }
        }
    }
    decoded
}

// ---------------------------------------------------------------------------
// The fake connector and its metrics endpoint
// ---------------------------------------------------------------------------

struct FakeCloudflared {
    config: CloudflaredConfig,
    log: PathBuf,
}

/// Writes a fake `cloudflared` that records argv, pid and environment to `log`
/// (one block per run) and then sleeps.
fn fake_cloudflared(dir: &Path, metrics: SocketAddr) -> FakeCloudflared {
    let binary = dir.join("cloudflared");
    let log = dir.join("runs.log");
    let script = format!(
        "#!/bin/sh\n{{ printf '%s\\n' '--- run'; printf 'argv:%s\\n' \"$*\"; printf 'pid:%s\\n' \"$$\"; \
         env; printf '%s\\n' '--- end'; }} >> '{log}'\nexec sleep 300\n",
        log = log.display()
    );
    std::fs::write(&binary, script).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    FakeCloudflared {
        config: CloudflaredConfig {
            binary,
            metrics,
            extra_args: Vec::new(),
        },
        log,
    }
}

/// A metrics endpoint that answers `200` to every connection, so
/// `Cloudflared::ready` is true immediately.
async fn spawn_metrics() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buffer = [0_u8; 512];
                let _ = stream.read(&mut buffer).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    addr
}

#[derive(Debug, Default)]
struct Run {
    argv: String,
    pid: u32,
    env: BTreeMap<String, String>,
}

fn read_runs(log: &Path) -> Vec<Run> {
    let Ok(text) = std::fs::read_to_string(log) else {
        return Vec::new();
    };
    let mut runs = Vec::new();
    let mut current: Option<Run> = None;
    for line in text.lines() {
        if line == "--- run" {
            current = Some(Run::default());
            continue;
        }
        if line == "--- end" {
            if let Some(run) = current.take() {
                runs.push(run);
            }
            continue;
        }
        let Some(run) = current.as_mut() else {
            continue;
        };
        if let Some(argv) = line.strip_prefix("argv:") {
            run.argv = argv.to_string();
        } else if let Some(pid) = line.strip_prefix("pid:") {
            run.pid = pid.parse().unwrap_or_default();
        } else if let Some((key, value)) = line.split_once('=') {
            run.env.insert(key.to_string(), value.to_string());
        }
    }
    runs
}

async fn wait_for_runs(log: &Path, count: usize) -> Vec<Run> {
    let started = Instant::now();
    loop {
        let runs = read_runs(log);
        if runs.len() >= count {
            return runs;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "expected {count} runs, saw {}",
            runs.len()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn process_is_gone(pid: u32) -> bool {
    !std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -0 {pid}"))
        .stderr(std::process::Stdio::null())
        .status()
        .expect("kill -0 runs")
        .success()
}

// ---------------------------------------------------------------------------
// The tunnel backend: the primary's TunnelController and the node's
// TunnelIngress (ADR 0094)
// ---------------------------------------------------------------------------

struct TunnelSetup {
    ingress: TunnelIngress,
    controller: TunnelController,
    mock: MockCloudflare,
    app: SocketAddr,
    log: PathBuf,
    dir: PathBuf,
    tunnel_id: String,
    /// Requests the mock recorded once the node side was started. The node
    /// holds no Cloudflare client, so this never grows on its own.
    api_requests_after_start: usize,
    published: BTreeSet<String>,
}

fn cloudflare_for(mock: &MockCloudflare) -> Cloudflare {
    Cloudflare::with_endpoints(
        Token::new(API_TOKEN.to_string()),
        &mock.api_base(),
        "http://127.0.0.1:1/dns-query",
    )
    .unwrap()
}

/// Starts the primary side (the controller ensures the tunnel) and then the
/// node side with what `desired_state` would carry: the tunnel id and its run
/// token.
async fn tunnel_setup(name: &str) -> TunnelSetup {
    let dir = tmp_dir(name);
    let mock = MockCloudflare::spawn().await;
    let fake = fake_cloudflared(&dir, spawn_metrics().await);
    let app = spawn_app().await;
    let controller =
        TunnelController::new(cloudflare_for(&mock), "acc".to_string(), "zone".to_string());
    let (tunnel, token) = controller
        .ensure_tunnel("lab")
        .await
        .expect("the controller ensures the tunnel");
    let ingress = TunnelIngress::start(
        TunnelConfig {
            origin: "127.0.0.1:0".parse().unwrap(),
            cloudflared: fake.config.clone(),
        },
        TunnelRun {
            tunnel_id: tunnel.id.clone(),
            token,
        },
    )
    .await
    .expect("the tunnel ingress starts");
    let api_requests_after_start = mock.request_count();
    TunnelSetup {
        ingress,
        controller,
        mock,
        app,
        log: fake.log,
        dir,
        tunnel_id: tunnel.id,
        api_requests_after_start,
        published: BTreeSet::new(),
    }
}

fn hostnames(table: &RouteTable) -> BTreeSet<String> {
    table.iter().map(|route| route.hostname.clone()).collect()
}

#[tokio::test]
async fn the_controller_creates_the_tunnel_and_fetches_its_run_token() {
    let setup = tunnel_setup("backends-tunnel-create").await;

    // The tunnel was looked up by name, created remotely managed, and its
    // connector token fetched.
    let lookups = setup
        .mock
        .requests_matching("GET", "/accounts/acc/cfd_tunnel");
    assert_eq!(lookups.len(), 1, "{lookups:?}");
    assert!(lookups[0].query.contains("name=quoro-lab"), "{lookups:?}");
    assert!(lookups[0].query.contains("is_deleted=false"), "{lookups:?}");

    let posts = setup
        .mock
        .requests_matching("POST", "/accounts/acc/cfd_tunnel");
    assert_eq!(posts.len(), 1, "{posts:?}");
    let body = posts[0].body.as_ref().expect("the POST carried a body");
    assert_eq!(body["name"], "quoro-lab");
    assert_eq!(body["config_src"], "cloudflare");

    assert_eq!(
        setup.mock.tunnels(),
        vec![json!({"id": &setup.tunnel_id, "name": "quoro-lab"})]
    );
    assert_eq!(
        setup
            .mock
            .requests_matching(
                "GET",
                &format!("/accounts/acc/cfd_tunnel/{}/token", setup.tunnel_id)
            )
            .len(),
        1
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

/// A controller stopped in the middle of a sync makes no Cloudflare call
/// after the check that stopped it: here it stops after the ingress PUT, so
/// no CNAME is written and `published` holds nothing.
#[tokio::test]
async fn a_stopped_controller_makes_no_further_cloudflare_write() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mock = MockCloudflare::spawn().await;
    let armed = Arc::new(AtomicBool::new(false));
    let checks = Arc::new(AtomicUsize::new(0));
    let controller =
        TunnelController::new(cloudflare_for(&mock), "acc".to_string(), "zone".to_string())
            .stop_when({
                let armed = Arc::clone(&armed);
                let checks = Arc::clone(&checks);
                // Once armed, the first check passes and every later one stops.
                move || armed.load(Ordering::SeqCst) && checks.fetch_add(1, Ordering::SeqCst) >= 1
            });
    let (tunnel, _token) = controller
        .ensure_tunnel("lab")
        .await
        .expect("an unstopped controller creates the tunnel");

    armed.store(true, Ordering::SeqCst);
    let before = mock.request_count();
    let wanted: BTreeSet<String> = ["a.example.com", "b.example.com"]
        .into_iter()
        .map(String::from)
        .collect();
    let mut published = BTreeSet::new();
    let stopped = controller
        .sync_routes(&tunnel.id, 8080, &wanted, &mut published)
        .await
        .expect_err("the sync stops before its first CNAME");
    assert!(stopped.to_string().contains("stopped"), "{stopped}");
    assert_eq!(mock.request_count(), before + 1, "only the ingress PUT");
    assert!(mock.last_configuration().is_some());
    assert!(published.is_empty(), "{published:?}");
    assert!(mock.records().is_empty(), "{:?}", mock.records());

    // Stopped for good: neither a deletion nor a creation reaches Cloudflare.
    controller
        .delete_tunnel(&tunnel.id)
        .await
        .expect_err("the deletion is stopped");
    assert_eq!(mock.request_count(), before + 1);
}

#[tokio::test]
async fn the_controller_finds_an_existing_tunnel_instead_of_creating_it() {
    let setup = tunnel_setup("backends-tunnel-reuse").await;

    let (tunnel, _token) = setup
        .controller
        .ensure_tunnel("lab")
        .await
        .expect("the second ensure succeeds");

    assert_eq!(tunnel.id, setup.tunnel_id);
    assert_eq!(setup.mock.tunnels().len(), 1);
    assert_eq!(
        setup
            .mock
            .requests_matching("POST", "/accounts/acc/cfd_tunnel")
            .len(),
        1,
        "a second tunnel must not be created"
    );
    assert_eq!(
        setup
            .mock
            .requests_matching("GET", "/accounts/acc/cfd_tunnel")
            .len(),
        2,
        "the second ensure looks the tunnel up again"
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn the_node_starts_cloudflared_with_the_run_token_and_never_calls_the_cloudflare_api() {
    let setup = tunnel_setup("backends-tunnel-start").await;
    assert_eq!(setup.ingress.tunnel_id(), setup.tunnel_id);
    assert!(setup.ingress.origin().ip().is_loopback());

    // cloudflared got the run token only through TUNNEL_TOKEN, and the API
    // token reached neither argv nor the child's environment.
    let runs = wait_for_runs(&setup.log, 1).await;
    let run = &runs[0];
    assert_eq!(
        run.env.get("TUNNEL_TOKEN").map(String::as_str),
        Some(RUN_TOKEN)
    );
    assert!(!run.argv.contains(RUN_TOKEN), "argv was {:?}", run.argv);
    assert!(!run.argv.contains(API_TOKEN), "argv was {:?}", run.argv);
    assert!(
        !run.env.values().any(|value| value.contains(API_TOKEN)),
        "the API token reached the child's environment"
    );
    let log = std::fs::read_to_string(&setup.log).unwrap();
    assert!(!log.contains(API_TOKEN), "the API token reached the log");

    // Applying routes and waiting for the connector uses only node-local
    // state: the node side has no Cloudflare client to call the API with.
    let mut events = setup.ingress.subscribe();
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Tunnel)]).unwrap())
        .await
        .unwrap();
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::TunnelRouteReady { hostname } if hostname == "app.example.com")
        },
        Duration::from_secs(20),
        "TunnelRouteReady for app.example.com",
    )
    .await;
    assert_eq!(
        setup.mock.request_count(),
        setup.api_requests_after_start,
        "the node must never call the Cloudflare API"
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn the_controller_publishes_routes_and_the_node_proxies_them() {
    let mut setup = tunnel_setup("backends-tunnel-routes").await;
    let mut events = setup.ingress.subscribe();

    let table = RouteTable::new([route("app.example.com", setup.app, TlsMode::Tunnel)]).unwrap();
    let wanted = hostnames(&table);
    setup.ingress.apply(table).await.unwrap();
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::TunnelRouteReady { hostname } if hostname == "app.example.com")
        },
        Duration::from_secs(20),
        "TunnelRouteReady for app.example.com",
    )
    .await;

    let origin = setup.ingress.origin();
    let failures = setup
        .controller
        .sync_routes(
            &setup.tunnel_id,
            origin.port(),
            &wanted,
            &mut setup.published,
        )
        .await
        .expect("the ingress push succeeds");
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(setup.published, wanted);

    // The pushed ingress rules: the hostname points at this node's loopback
    // origin and the catch-all last.
    let configuration = setup
        .mock
        .last_configuration()
        .expect("configuration pushed");
    assert_eq!(
        configuration["config"]["ingress"],
        json!([
            {"hostname": "app.example.com", "service": format!("http://127.0.0.1:{}", origin.port())},
            {"service": "http_status:404"},
        ])
    );

    // The hostname points at the tunnel with a proxied CNAME Quoro owns.
    let records = setup.mock.records();
    let cname = records
        .iter()
        .find(|record| record.kind == "CNAME" && record.name == "app.example.com")
        .unwrap_or_else(|| panic!("no CNAME created: {records:?}"));
    assert_eq!(
        cname.content,
        format!("{}.cfargotunnel.com", setup.tunnel_id)
    );
    assert!(cname.proxied, "the CNAME must be proxied");
    assert_eq!(cname.comment.as_deref(), Some("managed by quoro"));

    // cloudflared dials the loopback listener: Host routing and the https
    // forwarded proto both reach the app.
    let (status, headers, body) = http_get(origin, "app.example.com", "/hello").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, APP_BODY);
    assert_eq!(
        headers
            .get("x-seen-proto")
            .and_then(|value| value.to_str().ok()),
        Some("https")
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn a_hand_made_record_is_reported_as_a_failure_and_left_untouched() {
    let mut setup = tunnel_setup("backends-tunnel-conflict").await;
    setup.mock.add_record(MockRecord {
        id: "manual-a-record".to_string(),
        kind: "A".to_string(),
        name: "other.example.com".to_string(),
        content: "203.0.113.10".to_string(),
        proxied: false,
        comment: None,
    });
    let wanted: BTreeSet<String> = ["other.example.com".to_string()].into_iter().collect();

    let failures = setup
        .controller
        .sync_routes(
            &setup.tunnel_id,
            setup.ingress.origin().port(),
            &wanted,
            &mut setup.published,
        )
        .await
        .expect("the ingress push succeeds");

    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].0, "other.example.com");
    assert!(
        failures[0].1.contains("conflict") || failures[0].1.contains("already exists"),
        "the failure must name the conflicting record: {}",
        failures[0].1
    );
    assert!(
        setup.published.is_empty(),
        "a failed CNAME is not recorded as published"
    );

    let records = setup.mock.records();
    let manual = records
        .iter()
        .filter(|record| record.name == "other.example.com")
        .collect::<Vec<_>>();
    assert_eq!(manual.len(), 1, "{records:?}");
    assert_eq!(manual[0].kind, "A");
    assert_eq!(manual[0].content, "203.0.113.10");
    assert_eq!(manual[0].comment, None);
    assert!(
        !records
            .iter()
            .any(|record| record.kind == "CNAME" && record.name == "other.example.com"),
        "no CNAME was created next to the hand-made record"
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn an_empty_set_removes_the_published_cnames_and_leaves_the_catch_all() {
    let mut setup = tunnel_setup("backends-tunnel-empty").await;
    let wanted: BTreeSet<String> = ["app.example.com".to_string()].into_iter().collect();
    let origin_port = setup.ingress.origin().port();

    let failures = setup
        .controller
        .sync_routes(&setup.tunnel_id, origin_port, &wanted, &mut setup.published)
        .await
        .expect("the first push succeeds");
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(setup.published, wanted);

    let failures = setup
        .controller
        .sync_routes(
            &setup.tunnel_id,
            origin_port,
            &BTreeSet::new(),
            &mut setup.published,
        )
        .await
        .expect("the empty push succeeds");
    assert!(failures.is_empty(), "{failures:?}");
    assert!(setup.published.is_empty());
    assert!(
        setup
            .mock
            .records()
            .iter()
            .all(|record| record.name != "app.example.com"),
        "the CNAME of a withdrawn hostname is deleted"
    );
    let configuration = setup
        .mock
        .last_configuration()
        .expect("configuration pushed");
    assert_eq!(
        configuration["config"]["ingress"],
        json!([{"service": "http_status:404"}])
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn an_acme_route_is_refused_and_the_previous_table_keeps_routing() {
    let setup = tunnel_setup("backends-tunnel-acme-refused").await;
    let mut events = setup.ingress.subscribe();
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Tunnel)]).unwrap())
        .await
        .unwrap();
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::TunnelRouteReady { hostname } if hostname == "app.example.com")
        },
        Duration::from_secs(20),
        "TunnelRouteReady for app.example.com",
    )
    .await;

    let bad = RouteTable::new([route("other.example.com", setup.app, TlsMode::Acme)]).unwrap();
    let error = setup.ingress.apply(bad).await.expect_err("Acme is refused");
    assert!(
        matches!(error, IngressError::InvalidRoutes(_)),
        "unexpected error: {error}"
    );

    // The rejected table never replaced the previous one.
    let origin = setup.ingress.origin();
    let (status, _, body) = http_get(origin, "app.example.com", "/still").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, APP_BODY);
    let (status, _, _) = http_get(origin, "other.example.com", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn set_run_restarts_the_connector_with_the_new_token() {
    let setup = tunnel_setup("backends-tunnel-set-run").await;
    wait_for_runs(&setup.log, 1).await;
    let mut events = setup.ingress.subscribe();
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Tunnel)]).unwrap())
        .await
        .unwrap();
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::TunnelRouteReady { hostname } if hostname == "app.example.com")
        },
        Duration::from_secs(20),
        "TunnelRouteReady for app.example.com",
    )
    .await;

    setup
        .ingress
        .set_run(TunnelRun {
            tunnel_id: "rotated-tunnel".to_string(),
            token: Token::new("rotated-run-token".to_string()),
        })
        .await;

    let runs = wait_for_runs(&setup.log, 2).await;
    let rerun = &runs[1];
    assert_eq!(
        rerun.env.get("TUNNEL_TOKEN").map(String::as_str),
        Some("rotated-run-token")
    );
    assert!(
        !rerun.argv.contains("rotated-run-token"),
        "the run token must not reach argv: {:?}",
        rerun.argv
    );
    assert_eq!(setup.ingress.tunnel_id(), "rotated-tunnel");

    // The connector is up again, so the current table is reported ready again.
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::TunnelRouteReady { hostname } if hostname == "app.example.com")
        },
        Duration::from_secs(20),
        "TunnelRouteReady after the token rotation",
    )
    .await;

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn shutdown_is_prompt_and_kills_the_connector() {
    let setup = tunnel_setup("backends-tunnel-shutdown").await;
    let runs = wait_for_runs(&setup.log, 1).await;
    let pid = runs[0].pid;
    assert!(!process_is_gone(pid), "the fake connector already exited");

    let started = Instant::now();
    shutdown_within_15s(&setup.ingress).await;
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "shutdown took {:?}",
        started.elapsed()
    );

    // The supervisor terminates and reaps the child on the same shutdown
    // signal, asynchronously with the listeners.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !process_is_gone(pid) {
        assert!(
            Instant::now() < deadline,
            "the connector outlived shutdown (pid {pid})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn a_non_loopback_origin_is_refused() {
    let dir = tmp_dir("backends-tunnel-nonloopback");
    let mock = MockCloudflare::spawn().await;
    let fake = fake_cloudflared(&dir, spawn_metrics().await);
    let error = TunnelIngress::start(
        TunnelConfig {
            origin: "0.0.0.0:0".parse().unwrap(),
            cloudflared: fake.config,
        },
        TunnelRun {
            tunnel_id: "tunnel-1".to_string(),
            token: Token::new(RUN_TOKEN.to_string()),
        },
    )
    .await
    .err()
    .expect("a non-loopback listener is refused");
    assert!(
        matches!(error, IngressError::Other(_)),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("not a loopback"),
        "unexpected error: {error}"
    );
    assert_eq!(
        mock.request_count(),
        0,
        "the node must never call the Cloudflare API"
    );

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// PublicIngress
// ---------------------------------------------------------------------------

/// A DNS provider that only records what it was asked to do; DNS-01 TXT
/// records are always visible.
#[derive(Default)]
struct MockDns {
    calls: Mutex<Vec<String>>,
}

impl MockDns {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl DnsProvider for MockDns {
    async fn create_txt(&self, name: &str, value: &str) -> Result<RecordId, ProviderError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("create_txt {name} {value}"));
        Ok(RecordId("txt-1".to_string()))
    }

    async fn delete_record(&self, id: &RecordId) -> Result<(), ProviderError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("delete_record {}", id.0));
        Ok(())
    }

    async fn upsert_cname(
        &self,
        name: &str,
        target: &str,
        proxied: bool,
    ) -> Result<RecordId, ProviderError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("upsert_cname {name} {target} {proxied}"));
        Ok(RecordId("cname-1".to_string()))
    }

    async fn delete_cname(&self, name: &str, target: &str) -> Result<(), ProviderError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("delete_cname {name} {target}"));
        Ok(())
    }

    async fn txt_visible(&self, _name: &str, _value: &str) -> Result<bool, ProviderError> {
        Ok(true)
    }
}

struct PublicSetup {
    ingress: PublicIngress<MockDns>,
    http: SocketAddr,
    https: SocketAddr,
    app: SocketAddr,
    dns: Arc<MockDns>,
    dir: PathBuf,
    cert_der: CertificateDer<'static>,
}

fn self_signed(host: &str) -> (String, String, CertificateDer<'static>) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec![host.to_string()]).unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert.pem(), key.serialize_pem(), cert.der().clone())
}

fn tls_client(certificate: CertificateDer<'static>) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

async fn tls_get(
    addr: SocketAddr,
    sni: &str,
    config: Arc<rustls::ClientConfig>,
    path: &str,
) -> String {
    let connector = TlsConnector::from(config);
    let stream = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from(sni.to_string()).unwrap();
    let mut stream = connector.connect(name, stream).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {sni}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer).into_owned()
}

async fn public_setup(name: &str) -> PublicSetup {
    let dir = tmp_dir(name);
    let cert_dir = dir.join("certs");
    let (certificate, key, cert_der) = self_signed("app.example.com");
    // Store the certificate before `start`, exactly like a restart after an
    // earlier issuance: `CertStore::open` inside `start` loads it from disk.
    CertStore::open(&cert_dir)
        .unwrap()
        .store("app.example.com", &certificate, &key)
        .unwrap();

    let mut acme = AcmeConfig::new("http://127.0.0.1:1/dir", dir.join("state"));
    acme.check_interval = Duration::from_secs(60 * 60);
    acme.propagation_poll = Duration::from_millis(100);

    let dns = Arc::new(MockDns::default());
    let ingress = PublicIngress::start(
        PublicConfig {
            http: "127.0.0.1:0".parse().unwrap(),
            https: "127.0.0.1:0".parse().unwrap(),
            cert_dir,
            acme,
        },
        Arc::clone(&dns),
    )
    .await
    .expect("the public ingress starts");
    let (http, https) = ingress.local_addrs();
    let app = spawn_app().await;
    PublicSetup {
        ingress,
        http,
        https,
        app,
        dns,
        dir,
        cert_der,
    }
}

#[tokio::test]
async fn the_tls_listener_serves_the_stored_certificate() {
    let setup = public_setup("backends-public-tls").await;
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Acme)]).unwrap())
        .await
        .unwrap();

    let response = tls_get(
        setup.https,
        "app.example.com",
        tls_client(setup.cert_der.clone()),
        "/tls?q=1",
    )
    .await;
    assert_eq!(status_line(&response), 200, "{response}");
    assert!(response.contains(APP_BODY), "{response}");

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn the_http_listener_redirects_to_https() {
    let setup = public_setup("backends-public-redirect").await;
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Acme)]).unwrap())
        .await
        .unwrap();

    let (status, headers, _) = http_get(setup.http, "app.example.com", "/path?q=1").await;
    assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        headers
            .get("location")
            .and_then(|value| value.to_str().ok()),
        Some("https://app.example.com/path?q=1")
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn a_new_hostname_reports_pending_then_failed_and_a_fresh_certificate_is_not_renewed() {
    let setup = public_setup("backends-public-events").await;
    let mut events = setup.ingress.subscribe();

    // `rcgen::CertificateParams::new` uses its default validity: 1975-01-01 to
    // 4096-01-01. Almost that whole lifetime remains, so `renewal_due` (which
    // wants less than a third of the lifetime left) is false. The ARI lookup
    // cannot produce a window either, because the directory is unreachable.
    // `app.example.com` therefore has its fresh certificate and is not due:
    // the worker must not emit any event for it.
    let stored = CertStore::open(&setup.dir.join("certs"))
        .unwrap()
        .get("app.example.com")
        .unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        !renewal_due(now, stored.not_before, stored.not_after, None),
        "a fresh rcgen certificate must not be due for renewal"
    );
    assert!(stored.not_after - now > 24 * 60 * 60);

    // `new.example.com` has no certificate at all, so issuance is attempted
    // and fails against the unreachable directory.
    setup
        .ingress
        .apply(
            RouteTable::new([
                route("app.example.com", setup.app, TlsMode::Acme),
                route("new.example.com", setup.app, TlsMode::Acme),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::CertificatePending { hostname, renewal: false } if hostname == "new.example.com")
        },
        Duration::from_secs(20),
        "CertificatePending for new.example.com",
    )
    .await;
    wait_for_event(
        &mut events,
        |event| {
            matches!(event, IngressEvent::CertificateFailed { hostname, .. } if hostname == "new.example.com")
        },
        Duration::from_secs(20),
        "CertificateFailed for new.example.com",
    )
    .await;

    // Nothing was reported for the fresh, not-due name.
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(&event, IngressEvent::CertificatePending { hostname, .. }
                | IngressEvent::CertificateIssued { hostname, .. }
                | IngressEvent::CertificateFailed { hostname, .. }
                | IngressEvent::CertificateExpiring { hostname, .. }
                if hostname == "app.example.com"),
            "unexpected event for the not-due certificate: {event:?}"
        );
    }
    // The failure happened before any DNS-01 record could be created.
    assert!(
        !setup
            .dns
            .calls()
            .iter()
            .any(|call| call.starts_with("create_txt")),
        "no DNS-01 record is created when the account cannot be reached: {:?}",
        setup.dns.calls()
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn an_acme_backend_refuses_tunnel_routes() {
    let setup = public_setup("backends-public-refuses-tunnel").await;
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Acme)]).unwrap())
        .await
        .unwrap();

    let error = setup
        .ingress
        .apply(RouteTable::new([route("tunnel.example.com", setup.app, TlsMode::Tunnel)]).unwrap())
        .await
        .expect_err("the public backend does not serve Tunnel routes");
    assert!(
        matches!(error, IngressError::InvalidRoutes(_)),
        "unexpected error: {error}"
    );

    // The rejected table did not replace the previous one.
    let (status, headers, _) = http_get(setup.http, "app.example.com", "/x").await;
    assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        headers
            .get("location")
            .and_then(|value| value.to_str().ok()),
        Some("https://app.example.com/x")
    );

    shutdown_within_15s(&setup.ingress).await;
    std::fs::remove_dir_all(&setup.dir).ok();
}

#[tokio::test]
async fn shutdown_is_prompt_and_both_ports_stop_accepting() {
    let setup = public_setup("backends-public-shutdown").await;
    setup
        .ingress
        .apply(RouteTable::new([route("app.example.com", setup.app, TlsMode::Acme)]).unwrap())
        .await
        .unwrap();

    let started = Instant::now();
    shutdown_within_15s(&setup.ingress).await;
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "shutdown took {:?}",
        started.elapsed()
    );

    for addr in [setup.http, setup.https] {
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(addr).await.is_ok() {
            assert!(
                Instant::now() < deadline,
                "{addr} still accepts connections after shutdown"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    std::fs::remove_dir_all(&setup.dir).ok();
}
