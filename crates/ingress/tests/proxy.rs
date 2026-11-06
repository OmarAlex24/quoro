//! End-to-end tests for the embedded reverse proxy core.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, HOST, HeaderValue, UPGRADE};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use quoro_ingress::certs::CertStore;
use quoro_ingress::proxy::{
    BackendObserver, ListenerMode, ProxyCore, serve_http, serve_https, serve_redirect,
    tls_server_config,
};
use quoro_ingress::{RetryPolicy, Route, RouteTable, TlsMode};
use rustls::pki_types::{CertificateDer, ServerName};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

type TestClient = Client<HttpConnector, Full<Bytes>>;

fn client() -> TestClient {
    Client::builder(TokioExecutor::new()).build(HttpConnector::new())
}

fn mode(serves: TlsMode, forwarded_proto: &'static str, trust_forwarded: bool) -> ListenerMode {
    ListenerMode {
        serves,
        forwarded_proto,
        trust_forwarded,
    }
}

fn route(host: &str, backends: Vec<SocketAddr>, tls: TlsMode) -> Route {
    Route {
        hostname: host.to_string(),
        backends,
        tls,
        retry: quoro_ingress::RetryPolicy::Unsent,
        response_timeout: None,
    }
}

fn edge_route(host: &str, backends: Vec<SocketAddr>, tls: TlsMode) -> Route {
    Route {
        hostname: host.to_string(),
        backends,
        tls,
        retry: RetryPolicy::IdempotentOnce,
        response_timeout: Some(Duration::from_secs(60)),
    }
}

fn table(routes: Vec<Route>) -> RouteTable {
    RouteTable::new(routes).unwrap()
}

fn hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// An echo backend: it reports its tag, method, path and every header it saw,
/// plus the length and hash of the body. One line per item.
async fn spawn_backend(tag: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    let mut lines = vec![
                        format!("backend: {tag}"),
                        format!("method: {}", request.method()),
                        format!(
                            "path: {}",
                            request
                                .uri()
                                .path_and_query()
                                .map_or("/", hyper::http::uri::PathAndQuery::as_str)
                        ),
                    ];
                    for (name, value) in request.headers() {
                        lines.push(format!(
                            "header: {name}: {}",
                            value.to_str().unwrap_or("<binary>")
                        ));
                    }
                    let body = request.into_body().collect().await.unwrap().to_bytes();
                    lines.push(format!("body-len: {}", body.len()));
                    lines.push(format!("body-hash: {:016x}", hash(&body)));
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(
                        lines.join("\n") + "\n",
                    ))))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

/// A backend whose upgrade answers 101 and then echoes every byte it reads.
async fn spawn_upgrade_backend() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let service = service_fn(|mut request: Request<Incoming>| async move {
                    let upgrade = hyper::upgrade::on(&mut request);
                    tokio::spawn(async move {
                        if let Ok(upgraded) = upgrade.await {
                            let mut io = TokioIo::new(upgraded);
                            let mut buffer = [0_u8; 4096];
                            loop {
                                match io.read(&mut buffer).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(read) => {
                                        if io.write_all(&buffer[..read]).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    });
                    let mut response = Response::new(Full::new(Bytes::new()));
                    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                    response
                        .headers_mut()
                        .insert(CONNECTION, HeaderValue::from_static("upgrade"));
                    response
                        .headers_mut()
                        .insert(UPGRADE, HeaderValue::from_static("websocket"));
                    Ok::<_, Infallible>(response)
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}

async fn spawn_http(
    core: Arc<ProxyCore>,
    listener_mode: ListenerMode,
) -> (
    SocketAddr,
    watch::Sender<bool>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (sender, receiver) = watch::channel(false);
    let handle = tokio::spawn(serve_http(listener, core, listener_mode, receiver));
    (addr, sender, handle)
}

async fn spawn_redirect(
    core: Arc<ProxyCore>,
) -> (
    SocketAddr,
    watch::Sender<bool>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (sender, receiver) = watch::channel(false);
    let handle = tokio::spawn(serve_redirect(listener, core, receiver));
    (addr, sender, handle)
}

async fn spawn_https(
    core: Arc<ProxyCore>,
    listener_mode: ListenerMode,
    tls: Arc<rustls::ServerConfig>,
) -> (
    SocketAddr,
    watch::Sender<bool>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (sender, receiver) = watch::channel(false);
    let handle = tokio::spawn(serve_https(listener, core, listener_mode, tls, receiver));
    (addr, sender, handle)
}

async fn request(
    client: &TestClient,
    addr: SocketAddr,
    host: Option<&str>,
    method: &str,
    path: &str,
    body: Bytes,
) -> (StatusCode, String) {
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{addr}{path}"));
    if let Some(host) = host {
        builder = builder.header(HOST, host);
    }
    let request = builder.body(Full::new(body)).unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn get(
    client: &TestClient,
    addr: SocketAddr,
    host: &str,
    path: &str,
) -> (StatusCode, String) {
    request(client, addr, Some(host), "GET", path, Bytes::new()).await
}

async fn raw_request(addr: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer).into_owned()
}

fn status_line(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in response: {response:?}"))
}

/// Keeps a dead backend's port reserved while it lives (issue #414).
struct HeldPort {
    _client: std::net::TcpStream,
    _accepted: std::net::TcpStream,
}

/// A backend address that refuses every connection for as long as the
/// returned guard lives. The port is the local end of a loopback connection
/// held open: nothing listens on it, so a connect is refused at once, and
/// while the connection is open no other bind gets the port. A port released
/// right away could be bound by the next backend of a test running in
/// parallel, and the "dead" backend would then answer. A socket bound
/// without `listen` would reserve the port too, but on macOS a connect to it
/// times out instead of being refused.
fn dead_port() -> (SocketAddr, HeldPort) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    let addr = client.local_addr().unwrap();
    (
        addr,
        HeldPort {
            _client: client,
            _accepted: accepted,
        },
    )
}

#[tokio::test]
async fn routes_by_host_and_answers_missing_or_unknown_hosts() {
    let backend = spawn_backend("A").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![
        route("app.example.com", vec![backend], TlsMode::Acme),
        route("tunnel.example.com", vec![backend], TlsMode::Tunnel),
    ]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    let (status, body) = get(&client, addr, "app.example.com", "/hello?x=1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("backend: A"), "{body}");
    assert!(body.contains("path: /hello?x=1"), "{body}");

    let (status, body) = get(&client, addr, "APP.Example.COM:8443", "/case").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("path: /case"), "{body}");

    let (status, body) = get(&client, addr, "unknown.example.com", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, "no route for this host\n");

    // A route with the other TLS mode is not served on this listener.
    let (status, body) = get(&client, addr, "tunnel.example.com", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, "no route for this host\n");

    let response = raw_request(addr, "GET / HTTP/1.1\r\nConnection: close\r\n\r\n").await;
    assert_eq!(status_line(&response), 400, "{response}");

    let response = raw_request(
        addr,
        "GET / HTTP/1.1\r\nHost: bad_host.example.com\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status_line(&response), 400, "{response}");
}

#[tokio::test]
async fn sets_forwarded_headers_and_drops_hop_by_hop_headers() {
    let backend = spawn_backend("A").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![backend],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", true)).await;
    let client = client();

    let request = Request::builder()
        .uri(format!("http://{addr}/forwarded"))
        .header(HOST, "app.example.com")
        .header("x-forwarded-for", "1.2.3.4")
        .header("x-forwarded-proto", "http")
        .header("x-forwarded-host", "evil.example.com")
        .header("connection", "x-secret")
        .header("x-secret", "1")
        .header("keep-alive", "timeout=5")
        .header("te", "trailers")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = String::from_utf8_lossy(&response.into_body().collect().await.unwrap().to_bytes())
        .into_owned();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("header: x-forwarded-for: 1.2.3.4, 127.0.0.1"),
        "{body}"
    );
    // A trusted connector's own scheme survives (the visitor used http).
    assert!(body.contains("header: x-forwarded-proto: http\n"), "{body}");
    assert!(
        body.contains("header: x-forwarded-host: app.example.com"),
        "{body}"
    );
    assert!(!body.contains("evil.example.com"), "{body}");
    assert!(!body.contains("x-secret"), "{body}");
    assert!(!body.contains("header: keep-alive"), "{body}");
    assert!(!body.contains("header: te:"), "{body}");
    assert!(!body.contains("header: connection:"), "{body}");

    // CF-Connecting-IP is used when no X-Forwarded-For arrives.
    let request = Request::builder()
        .uri(format!("http://{addr}/cf"))
        .header(HOST, "app.example.com")
        .header("cf-connecting-ip", "9.9.9.9")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let body = String::from_utf8_lossy(&response.into_body().collect().await.unwrap().to_bytes())
        .into_owned();
    assert!(body.contains("header: x-forwarded-for: 9.9.9.9"), "{body}");
    // Without one, the listener's own scheme is sent.
    assert!(body.contains("header: x-forwarded-proto: https"), "{body}");
    assert!(!body.contains("cf-connecting-ip"), "{body}");

    // Untrusted: incoming X-Forwarded-* are dropped and replaced with the peer.
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![backend],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "http", false)).await;
    let request = Request::builder()
        .uri(format!("http://{addr}/spoofed"))
        .header(HOST, "app.example.com")
        .header("x-forwarded-for", "6.6.6.6")
        .header("x-forwarded-proto", "ftp")
        .header("x-forwarded-host", "evil.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let body = String::from_utf8_lossy(&response.into_body().collect().await.unwrap().to_bytes())
        .into_owned();
    assert!(
        body.contains("header: x-forwarded-for: 127.0.0.1"),
        "{body}"
    );
    assert!(body.contains("header: x-forwarded-proto: http"), "{body}");
    assert!(!body.contains("6.6.6.6"), "{body}");
    assert!(!body.contains("ftp"), "{body}");
}

#[tokio::test]
async fn round_robin_skips_dead_backends_and_reports_502_when_all_are_dead() {
    let first = spawn_backend("A").await;
    let second = spawn_backend("B").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![first, second],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    let mut tags = Vec::new();
    for _ in 0..4 {
        let (status, body) = get(&client, addr, "app.example.com", "/").await;
        assert_eq!(status, StatusCode::OK);
        let tag = body
            .lines()
            .find_map(|line| line.strip_prefix("backend: "))
            .unwrap()
            .to_string();
        tags.push(tag);
    }
    assert_eq!(tags, vec!["A", "B", "A", "B"]);

    // A dead backend is skipped without failing the request.
    let (dead, _dead_port) = dead_port();
    let live = spawn_backend("L").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![dead, live],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    for _ in 0..3 {
        let (status, body) = get(&client, addr, "app.example.com", "/").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains("backend: L"), "{body}");
    }

    // Every backend dead -> 502.
    let (first_dead, _first_dead_port) = dead_port();
    let (second_dead, _second_dead_port) = dead_port();
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![first_dead, second_dead],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let (status, body) = get(&client, addr, "app.example.com", "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body, "backend unavailable\n");
}

#[tokio::test]
async fn set_table_switches_routing_for_new_requests() {
    let first = spawn_backend("A").await;
    let second = spawn_backend("B").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![first],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    let (_, body) = get(&client, addr, "app.example.com", "/").await;
    assert!(body.contains("backend: A"), "{body}");

    core.set_table(table(vec![route(
        "app.example.com",
        vec![second],
        TlsMode::Acme,
    )]));
    let (_, body) = get(&client, addr, "app.example.com", "/").await;
    assert!(body.contains("backend: B"), "{body}");
}

#[tokio::test]
async fn streams_a_large_request_body_intact() {
    let backend = spawn_backend("A").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![backend],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    let body: Vec<u8> = (0..5 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let (status, echoed) = request(
        &client,
        addr,
        Some("app.example.com"),
        "POST",
        "/upload",
        Bytes::from(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{echoed}");
    assert!(echoed.contains("method: POST"), "{echoed}");
    assert!(echoed.contains("path: /upload"), "{echoed}");
    assert!(
        echoed.contains(&format!("body-len: {}", body.len())),
        "{echoed}"
    );
    assert!(
        echoed.contains(&format!("body-hash: {:016x}", hash(&body))),
        "{echoed}"
    );
}

#[tokio::test]
async fn redirect_listener_sends_308_for_acme_routes_only() {
    let backend = spawn_backend("A").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![
        route("app.example.com", vec![backend], TlsMode::Acme),
        route("tunnel.example.com", vec![backend], TlsMode::Tunnel),
    ]));
    let (addr, _shutdown, _handle) = spawn_redirect(core).await;
    let client = client();

    let request = Request::builder()
        .uri(format!("http://{addr}/path?x=1"))
        .header(HOST, "app.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap(),
        "https://app.example.com/path?x=1"
    );

    let (status, _) = get(&client, addr, "unknown.example.com", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = get(&client, addr, "tunnel.example.com", "/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

fn self_signed(host: &str) -> (String, String, CertificateDer<'static>) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec![host.to_string()]).unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert.pem(), key.serialize_pem(), cert.der().clone())
}

fn tls_client_config(roots: Vec<CertificateDer<'static>>) -> rustls::ClientConfig {
    let mut store = rustls::RootCertStore::empty();
    for cert in roots {
        store.add(cert).unwrap();
    }
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(store)
        .with_no_client_auth()
}

async fn tls_request(
    addr: SocketAddr,
    sni: &str,
    config: Arc<rustls::ClientConfig>,
    request: &str,
) -> std::io::Result<String> {
    let connector = TlsConnector::from(config);
    let stream = TcpStream::connect(addr).await?;
    let name = ServerName::try_from(sni.to_string())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let mut stream = connector.connect(name, stream).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

#[tokio::test]
async fn tls_listener_serves_routes_and_enforces_sni_match() {
    let backend = spawn_backend("A").await;
    let (certificate, key, der) = self_signed("app.example.com");
    let dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("proxy-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = CertStore::open(&dir).unwrap();
    store.store("app.example.com", &certificate, &key).unwrap();
    let tls = tls_server_config(store.resolver()).unwrap();

    let core = ProxyCore::new();
    core.set_table(table(vec![
        route("app.example.com", vec![backend], TlsMode::Acme),
        route("other.example.com", vec![backend], TlsMode::Acme),
    ]));
    let (addr, _shutdown, _handle) =
        spawn_https(core, mode(TlsMode::Acme, "https", false), tls).await;
    let config = Arc::new(tls_client_config(vec![der]));

    let response = tls_request(
        addr,
        "app.example.com",
        Arc::clone(&config),
        "GET /tls HTTP/1.1\r\nHost: app.example.com\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    assert_eq!(status_line(&response), 200, "{response}");
    assert!(response.contains("path: /tls"), "{response}");

    // SNI for one host with the Host header of another is misdirected.
    let response = tls_request(
        addr,
        "app.example.com",
        Arc::clone(&config),
        "GET / HTTP/1.1\r\nHost: other.example.com\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    assert_eq!(status_line(&response), 421, "{response}");

    // No certificate for this name: the handshake fails.
    let unknown = tls_request(
        addr,
        "unknown.example.com",
        config,
        "GET / HTTP/1.1\r\nHost: unknown.example.com\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(unknown.is_err(), "unknown SNI must fail the handshake");

    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn proxies_an_upgrade_and_echoes_bytes_both_ways() {
    let backend = spawn_upgrade_backend().await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![backend],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    let request = Request::builder()
        .uri(format!("http://{addr}/ws"))
        .method("GET")
        .header(HOST, "app.example.com")
        .header(CONNECTION, "upgrade")
        .header(UPGRADE, "websocket")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let mut response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let upgraded = hyper::upgrade::on(&mut response).await.unwrap();
    let mut io = TokioIo::new(upgraded);
    io.write_all(b"ping").await.unwrap();
    let mut buffer = [0_u8; 4];
    io.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"ping");
    io.write_all(b"pong").await.unwrap();
    let mut buffer = [0_u8; 4];
    io.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"pong");
}

#[tokio::test]
async fn shutdown_stops_accepting() {
    let backend = spawn_backend("A").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "app.example.com",
        vec![backend],
        TlsMode::Acme,
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (sender, receiver) = watch::channel(false);
    let handle = tokio::spawn(serve_http(
        listener,
        core,
        mode(TlsMode::Acme, "https", false),
        receiver,
    ));

    let (status, _) = get(&client(), addr, "app.example.com", "/").await;
    assert_eq!(status, StatusCode::OK);

    sender.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("listener did not stop")
        .unwrap()
        .unwrap();
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "listener still accepting"
    );
}

#[derive(Default)]
struct RecordingObserver {
    failures: Mutex<Vec<(String, SocketAddr, String)>>,
}

impl BackendObserver for RecordingObserver {
    fn connect_failed(&self, hostname: &str, backend: SocketAddr, error: &str) {
        self.failures
            .lock()
            .unwrap()
            .push((hostname.to_string(), backend, error.to_string()));
    }
}

#[tokio::test]
async fn an_edge_route_retries_a_connect_failure_once_for_get_and_head_only() {
    let (dead, _dead_port) = dead_port();
    let live = spawn_backend("L").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![edge_route(
        "edge.example.com",
        vec![dead, live],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    // GET starts on the dead backend and is retried on the live one.
    let (status, body) = get(&client, addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("backend: L"), "{body}");

    // HEAD is idempotent too.
    let head = Request::builder()
        .method("HEAD")
        .uri(format!("http://{addr}/"))
        .header(HOST, "edge.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(head).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // POST is never sent twice: with the dead backend first it answers 502.
    let core = ProxyCore::new();
    core.set_table(table(vec![edge_route(
        "edge.example.com",
        vec![dead, live],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let (status, body) = request(
        &client,
        addr,
        Some("edge.example.com"),
        "POST",
        "/",
        Bytes::from_static(b"payload"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
}

#[tokio::test]
async fn the_observer_sees_a_backend_that_cannot_connect() {
    let (dead, _dead_port) = dead_port();
    let observer = Arc::new(RecordingObserver::default());
    let core = ProxyCore::new();
    core.set_observer(Arc::clone(&observer) as Arc<dyn BackendObserver>);
    core.set_table(table(vec![edge_route(
        "edge.example.com",
        vec![dead],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;

    let (status, _) = get(&client(), addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let failures = observer.failures.lock().unwrap();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].0, "edge.example.com");
    assert_eq!(failures[0].1, dead);
    // The root cause, worded as the edge's health probes word theirs, not
    // the client's `client error (Connect)`.
    assert!(
        failures[0].2.starts_with(&format!("connect to {dead}: ")),
        "{}",
        failures[0].2
    );
    assert!(
        failures[0].2.to_ascii_lowercase().contains("refused"),
        "{}",
        failures[0].2
    );
}

#[tokio::test]
async fn a_route_with_no_backend_answers_502() {
    let core = ProxyCore::new();
    core.set_table(table(vec![edge_route(
        "edge.example.com",
        Vec::new(),
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;
    let (status, body) = get(&client(), addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body, "backend unavailable\n");
}

#[tokio::test]
async fn a_backend_that_never_sends_headers_hits_the_response_timeout() {
    // The listener accepts and then never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend = listener.local_addr().unwrap();
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let held = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            held.lock().unwrap().push(stream);
        }
    });

    let core = ProxyCore::new();
    core.set_table(table(vec![Route {
        hostname: "edge.example.com".to_string(),
        backends: vec![backend],
        tls: TlsMode::Acme,
        retry: RetryPolicy::IdempotentOnce,
        response_timeout: Some(Duration::from_millis(200)),
    }]));
    let (addr, _shutdown, _handle) = spawn_http(core, mode(TlsMode::Acme, "https", false)).await;

    let started = std::time::Instant::now();
    let (status, body) = get(&client(), addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the response timeout did not fire"
    );
}

/// Connections and requests a counting backend has seen.
#[derive(Default)]
struct Counts {
    connections: std::sync::atomic::AtomicUsize,
    requests: std::sync::atomic::AtomicUsize,
}

impl Counts {
    fn connections(&self) -> usize {
        self.connections.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn requests(&self) -> usize {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// A keep-alive backend that answers `backend: {tag}` and counts the
/// connections it accepted and the requests it answered.
async fn spawn_counting_backend(tag: &'static str) -> (SocketAddr, Arc<Counts>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    let seen = Arc::clone(&counts);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            seen.connections
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let seen = Arc::clone(&seen);
                    async move {
                        let _ = request.into_body().collect().await;
                        seen.requests
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(format!(
                            "backend: {tag}\n"
                        )))))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (addr, counts)
}

/// A backend that reads a request, writes `reply` (possibly nothing) and then
/// goes silent with the connection open, like a VM that lost power. Counts
/// the requests it read.
async fn spawn_stalling_backend(reply: &'static [u8]) -> (SocketAddr, Arc<Counts>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    let seen = Arc::clone(&counts);
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((mut stream, _)) = listener.accept().await {
            seen.connections
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut buffer = [0_u8; 4096];
            if matches!(stream.read(&mut buffer).await, Ok(read) if read > 0) {
                seen.requests
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = stream.write_all(reply).await;
            }
            held.push(stream);
        }
    });
    (addr, counts)
}

/// Waits until `check` holds, for at most 5 s.
async fn wait_for(mut check: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !check() {
        assert!(
            std::time::Instant::now() < deadline,
            "condition not reached in 5 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An edge route whose response timeout is far longer than any test waits,
/// so a fast answer proves the request was cancelled, not timed out.
fn slow_edge_route(host: &str, backends: Vec<SocketAddr>) -> Route {
    Route {
        hostname: host.to_string(),
        backends,
        tls: TlsMode::Acme,
        retry: RetryPolicy::IdempotentOnce,
        response_timeout: Some(Duration::from_secs(60)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_waiting_on_a_backend_that_leaves_the_rotation_is_retried_on_another() {
    let (stalled, stalled_counts) = spawn_stalling_backend(b"").await;
    let (live, live_counts) = spawn_counting_backend("L").await;
    let core = ProxyCore::new();
    // The first request goes to the first backend: the one that stalls.
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![stalled, live],
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;

    let started = std::time::Instant::now();
    let pending = tokio::spawn(async move { get(&client(), addr, "edge.example.com", "/").await });
    wait_for(|| stalled_counts.requests() == 1).await;
    // The edge takes it out; its table still names it, as it does until the
    // supervisor applies the next one.
    core.backend_down(stalled);

    let (status, body) = pending.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, "backend: L\n");
    assert_eq!(live_counts.requests(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_post_waiting_on_a_backend_that_leaves_the_rotation_fails_fast_without_a_retry() {
    let (stalled, stalled_counts) = spawn_stalling_backend(b"").await;
    let (live, live_counts) = spawn_counting_backend("L").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![stalled, live],
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;

    let started = std::time::Instant::now();
    let pending = tokio::spawn(async move {
        request(
            &client(),
            addr,
            Some("edge.example.com"),
            "POST",
            "/",
            Bytes::from_static(b"payload"),
        )
        .await
    });
    wait_for(|| stalled_counts.requests() == 1).await;
    core.backend_down(stalled);

    let (status, body) = pending.await.unwrap();
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert_eq!(body, "backend unavailable\n");
    assert_eq!(live_counts.requests(), 0, "the POST was sent twice");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_already_streaming_is_cut_and_not_retried() {
    let (stalled, stalled_counts) =
        spawn_stalling_backend(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nfirst bytes").await;
    let (live, live_counts) = spawn_counting_backend("L").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![stalled, live],
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;

    let started = std::time::Instant::now();
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .header(HOST, "edge.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client().request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(stalled_counts.requests(), 1);

    let mut body = response.into_body();
    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(&first[..], b"first bytes");

    core.backend_down(stalled);
    let rest = tokio::time::timeout(Duration::from_secs(5), body.collect())
        .await
        .expect("the body was not cut");
    assert!(rest.is_err(), "the cut body ended cleanly");
    assert_eq!(live_counts.requests(), 0, "a started response was retried");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pooled_connections_to_a_backend_taken_down_are_not_reused() {
    let (backend, counts) = spawn_counting_backend("B").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![backend],
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;
    let client = client();

    for _ in 0..2 {
        let (status, body) = get(&client, addr, "edge.example.com", "/").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(counts.requests(), 2);
    assert_eq!(
        counts.connections(),
        1,
        "the edge did not pool its connection"
    );

    // The backend leaves the rotation and comes back: the next request must
    // open a new connection instead of reusing the pooled one.
    core.backend_down(backend);
    let (status, body) = get(&client, addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(counts.connections(), 2, "a pooled connection was reused");

    // Dropping it from the table drops its pool too.
    let (other, _) = spawn_counting_backend("O").await;
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![other],
    )]));
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![backend],
    )]));
    let (status, body) = get(&client, addr, "edge.example.com", "/").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        counts.connections(),
        3,
        "a pooled connection survived the table"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_route_is_not_cancelled_by_backend_down() {
    // Node routes keep their behaviour: only edge requests watch the rotation.
    let (stalled, stalled_counts) =
        spawn_stalling_backend(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n").await;
    let core = ProxyCore::new();
    core.set_table(table(vec![route(
        "node.example.com",
        vec![stalled],
        TlsMode::Acme,
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .header(HOST, "node.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client().request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(stalled_counts.requests(), 1);
    core.backend_down(stalled);
    let body =
        tokio::time::timeout(Duration::from_millis(300), response.into_body().collect()).await;
    assert!(body.is_err(), "a node response was cut by backend_down");
}

/// A keep-alive backend that answers `backend: {tag}` until `dead` is set.
/// After that it reads the next request on each open connection and closes
/// that connection without answering, as a process does when it dies
/// gracefully. `rst` makes the close a reset instead of a clean close.
async fn spawn_dying_backend(
    tag: &'static str,
    rst: bool,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicBool>, Arc<Counts>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dead = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counts = Arc::new(Counts::default());
    let (flag, seen) = (Arc::clone(&dead), Arc::clone(&counts));
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            seen.connections
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (flag, seen) = (Arc::clone(&flag), Arc::clone(&seen));
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 4096];
                loop {
                    while !buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                        }
                    }
                    buffer.clear();
                    seen.requests
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if flag.load(std::sync::atomic::Ordering::SeqCst) {
                        if rst {
                            let _ = stream.set_zero_linger();
                        }
                        return;
                    }
                    let body = format!("backend: {tag}\n");
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if stream.write_all(reply.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, dead, counts)
}

/// Sends one request to warm a pooled connection to each of `backends`,
/// in round-robin order.
async fn warm(client: &TestClient, addr: SocketAddr, backends: usize) {
    for _ in 0..backends {
        let (status, body) = get(client, addr, "edge.example.com", "/").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_on_a_pooled_connection_lost_before_the_response_is_retried() {
    for rst in [false, true] {
        let (dying, dead, dying_counts) = spawn_dying_backend("D", rst).await;
        let (live, live_counts) = spawn_counting_backend("L").await;
        let observer = Arc::new(RecordingObserver::default());
        let core = ProxyCore::new();
        core.set_observer(Arc::clone(&observer) as Arc<dyn BackendObserver>);
        core.set_table(table(vec![slow_edge_route(
            "edge.example.com",
            vec![dying, live],
        )]));
        let (addr, _shutdown, _handle) =
            spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;
        let client = client();
        warm(&client, addr, 2).await;
        assert_eq!(dying_counts.connections(), 1);

        // The next request goes to the dying backend, over its pooled
        // connection, which is lost before any response.
        dead.store(true, std::sync::atomic::Ordering::SeqCst);
        let (status, body) = get(&client, addr, "edge.example.com", "/").await;
        assert_eq!(status, StatusCode::OK, "rst={rst}: {body}");
        assert_eq!(body, "backend: L\n", "rst={rst}");
        assert_eq!(dying_counts.requests(), 2, "rst={rst}");
        assert_eq!(live_counts.requests(), 2, "rst={rst}");

        // A reset takes the backend out at once; a clean close does not,
        // since a healthy backend closes idle keep-alive connections too.
        let failures = observer.failures.lock().unwrap().clone();
        if rst {
            let lost = format!("connection to {dying} lost: ");
            assert!(
                failures
                    .iter()
                    .any(|(_, backend, error)| *backend == dying && error.starts_with(&lost)),
                "{failures:?}"
            );
        } else {
            assert!(failures.is_empty(), "{failures:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_resets_its_own_upload_does_not_take_the_backend_out() {
    let (backend, _counts) = spawn_counting_backend("B").await;
    let observer = Arc::new(RecordingObserver::default());
    let core = ProxyCore::new();
    core.set_observer(Arc::clone(&observer) as Arc<dyn BackendObserver>);
    core.set_table(table(vec![slow_edge_route(
        "edge.example.com",
        vec![backend],
    )]));
    let (addr, _shutdown, _handle) =
        spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;

    // Promise a 1000-byte body, send 10 bytes, then reset the connection.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST / HTTP/1.1\r\nHost: edge.example.com\r\nContent-Length: 1000\r\n\r\n0123456789",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    stream.set_zero_linger().unwrap();
    drop(stream);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let failures = observer.failures.lock().unwrap().clone();
    assert!(
        failures.is_empty(),
        "a client's reset blamed the backend: {failures:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_post_on_a_pooled_connection_lost_before_the_response_is_not_retried() {
    for rst in [false, true] {
        let (dying, dead, dying_counts) = spawn_dying_backend("D", rst).await;
        let (live, live_counts) = spawn_counting_backend("L").await;
        let core = ProxyCore::new();
        core.set_table(table(vec![slow_edge_route(
            "edge.example.com",
            vec![dying, live],
        )]));
        let (addr, _shutdown, _handle) =
            spawn_http(Arc::clone(&core), mode(TlsMode::Acme, "https", false)).await;
        let client = client();
        warm(&client, addr, 2).await;

        dead.store(true, std::sync::atomic::Ordering::SeqCst);
        let (status, body) = request(
            &client,
            addr,
            Some("edge.example.com"),
            "POST",
            "/",
            Bytes::from_static(b"payload"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "rst={rst}: {body}");
        assert_eq!(dying_counts.requests(), 2, "rst={rst}");
        assert_eq!(
            live_counts.requests(),
            1,
            "rst={rst}: the POST was sent twice"
        );
    }
}
