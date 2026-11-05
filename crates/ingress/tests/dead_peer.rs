//! A backend that stops acknowledging without a reset, as a VM that lost
//! power does, reproduced with iptables on loopback. Linux only, needs root
//! (`CAP_NET_ADMIN`) and the `iptables` binary, and takes about 20 s per case
//! (`TCP_USER_TIMEOUT`), so it is ignored by default:
//!
//! ```text
//! cargo test -p quoro-ingress --test dead_peer -- --ignored --test-threads=1
//! ```
#![cfg(target_os = "linux")]

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::HOST;
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use quoro_ingress::proxy::{BackendObserver, ListenerMode, ProxyCore, serve_http};
use quoro_ingress::{RetryPolicy, Route, RouteTable, TlsMode};
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Default)]
struct RecordingObserver {
    failures: Mutex<Vec<(SocketAddr, String)>>,
}

impl BackendObserver for RecordingObserver {
    fn connect_failed(&self, _hostname: &str, backend: SocketAddr, error: &str) {
        self.failures
            .lock()
            .unwrap()
            .push((backend, error.to_string()));
    }
}

/// A keep-alive backend answering `backend: {tag}` to every request.
async fn spawn_backend(tag: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
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
    addr
}

/// Runs `iptables` with `args`, panicking with its output on failure.
fn iptables(args: &[&str]) {
    let output = Command::new("iptables").args(args).output().unwrap();
    assert!(
        output.status.success(),
        "iptables {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Removes an iptables rule when dropped, so a failed test leaves no rule.
struct Rule(Vec<String>);

impl Rule {
    fn insert(spec: &[&str]) -> Self {
        let mut args = vec!["-I"];
        args.extend_from_slice(spec);
        iptables(&args);
        Self(spec.iter().map(|s| (*s).to_string()).collect())
    }
}

impl Drop for Rule {
    fn drop(&mut self) {
        let mut args = vec!["-D"];
        args.extend(self.0.iter().map(String::as_str));
        let _ = Command::new("iptables").args(&args).output();
    }
}

async fn get(
    client: &Client<HttpConnector, Full<Bytes>>,
    addr: SocketAddr,
) -> (StatusCode, String) {
    let request = Request::builder()
        .uri(format!("http://{addr}/"))
        .header(HOST, "edge.example.com")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Warms a pooled connection to a dead-to-be backend and a live one, cuts the
/// first with `rule`, and sends a GET that lands on the cut connection.
async fn a_get_on_a_silent_pooled_connection_is_retried(rule: fn(u16) -> Vec<String>) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let dying = spawn_backend("D").await;
    let live = spawn_backend("L").await;
    let observer = Arc::new(RecordingObserver::default());
    let core = ProxyCore::new();
    core.set_observer(Arc::clone(&observer) as Arc<dyn BackendObserver>);
    core.set_table(
        RouteTable::new([Route {
            hostname: "edge.example.com".to_string(),
            backends: vec![dying, live],
            tls: TlsMode::Acme,
            retry: RetryPolicy::IdempotentOnce,
            response_timeout: Some(Duration::from_secs(60)),
        }])
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_shutdown, shutdown) = watch::channel(false);
    let mode = ListenerMode {
        serves: TlsMode::Acme,
        forwarded_proto: "https",
        trust_forwarded: false,
    };
    tokio::spawn(serve_http(listener, core, mode, shutdown));
    let client = Client::builder(TokioExecutor::new()).build(HttpConnector::new());

    // Round robin: one request to each backend leaves one pooled connection
    // to each.
    assert_eq!(get(&client, addr).await.1, "backend: D\n");
    assert_eq!(get(&client, addr).await.1, "backend: L\n");

    let spec = rule(dying.port());
    let spec: Vec<&str> = spec.iter().map(String::as_str).collect();
    let _rule = Rule::insert(&spec);

    let started = Instant::now();
    let (status, body) = get(&client, addr).await;
    let elapsed = started.elapsed();
    let failures = observer.failures.lock().unwrap().clone();
    eprintln!("status {status} after {elapsed:?}; observer saw {failures:?}");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, "backend: L\n");
    assert!(elapsed < Duration::from_secs(30), "took {elapsed:?}");
    assert!(
        failures.iter().any(|(backend, _)| *backend == dying),
        "the dead backend was not reported: {failures:?}"
    );
}

/// The peer's packets are dropped: the request is never acknowledged and the
/// socket fails with `ETIMEDOUT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs root and iptables; takes about 20 s"]
async fn a_get_on_a_pooled_connection_to_a_peer_that_vanished_is_retried() {
    a_get_on_a_silent_pooled_connection_is_retried(|port| {
        [
            "INPUT",
            "-i",
            "lo",
            "-p",
            "tcp",
            "--sport",
            &port.to_string(),
            "-j",
            "DROP",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
    })
    .await;
}

/// The peer's host answers with ICMP host unreachable, as a node does for a
/// VM that died behind its bridge: the socket fails with `EHOSTUNREACH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs root and iptables; takes about 20 s"]
async fn a_get_on_a_pooled_connection_to_an_unreachable_peer_is_retried() {
    a_get_on_a_silent_pooled_connection_is_retried(|port| {
        [
            "INPUT",
            "-i",
            "lo",
            "-p",
            "tcp",
            "--dport",
            &port.to_string(),
            "-j",
            "REJECT",
            "--reject-with",
            "icmp-host-unreachable",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
    })
    .await;
}
