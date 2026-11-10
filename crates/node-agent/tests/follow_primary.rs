// SPDX-License-Identifier: AGPL-3.0-only
//! Following the primary to an advertised address (section 5.10): when
//! `primary_url` refuses a dial, the node tries `primary_urls` with the same
//! pin and, once a session reaches `welcome`, persists that address. A
//! restarting primary that refuses for a moment is dialed again before the
//! advertised addresses, and an advertised address that never answers costs a
//! short timeout (section 4.4, issue #359).

mod agent_common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use agent_common::{TempDir, facts, self_signed, server_config};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_node_agent::identity::{self, Identity, IdentityHandle};
use quoro_node_agent::session::{Session, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::tls;
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{ControlMessage, Welcome};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

/// Serves the node session far enough to answer `hello` with `welcome`, and
/// counts the welcomes it sent in `welcomed`.
async fn serve_welcome(listener: TcpListener, acceptor: TlsAcceptor, welcomed: Arc<AtomicUsize>) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
        let welcomed = Arc::clone(&welcomed);
        tokio::spawn(async move {
            let Ok(stream) = acceptor.accept(tcp).await else {
                return;
            };
            let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            while let Some(Ok(message)) = ws.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let Ok(frame) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                if frame["type"] == "hello" {
                    let welcome = ControlMessage::Welcome(Welcome {
                        cluster_id: "cls_test".to_owned(),
                        term: 1,
                        server_time: "2026-11-02T10:15:30Z".to_owned(),
                        heartbeat_interval_ms: 500,
                        session_timeout_ms: 5_000,
                        primary_node_id: "nod_primary".to_owned(),
                        primary_urls: Vec::new(),
                        schema_version: None,
                    })
                    .to_frame("msg_welcome", frame["id"].as_str())
                    .to_text();
                    if ws.send(Message::Text(welcome.into())).await.is_ok() {
                        welcomed.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
    }
}

/// A port nothing listens on: bound once, then released.
fn closed_url() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe port");
    let port = probe.local_addr().expect("probe address").port();
    drop(probe);
    format!("https://127.0.0.1:{port}")
}

/// An address that accepts the TCP connection and never answers the TLS
/// handshake, like an address of the primary that this node cannot really
/// reach. Returns its URL and the count of connections it accepted.
async fn silent_url() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((tcp, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(tcp);
        }
    });
    (url, accepted, task)
}

/// Waits until `done` holds, for at most `limit`, and returns how long it took.
async fn within(limit: Duration, done: impl Fn() -> bool) -> Duration {
    let started = Instant::now();
    tokio::time::timeout(limit, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("not done within {limit:?}"));
    started.elapsed()
}

fn write_identity(
    dir: &Path,
    cluster: &SigningKey,
    primary_url: &str,
    fingerprint: &str,
    primary_urls: &[String],
) -> IdentityHandle {
    identity::ensure_data_dir(dir).expect("create the data directory");
    identity::write_seed(dir, &[7; 32]).expect("write the seed");
    identity::write_credential(dir, "credential-for-tests").expect("write the credential");
    let public = sign::public_key_to_base64(&cluster.verifying_key());
    identity::save_identity(
        dir,
        &Identity {
            node_id: "nod_test".to_owned(),
            cluster_id: "cls_test".to_owned(),
            primary_url: primary_url.to_owned(),
            primary_urls: primary_urls.to_vec(),
            fingerprint: format!("sha256:{}", fingerprint.trim_start_matches("sha256:")),
            cluster_key_id: Identity::key_id_for(&public).expect("key id"),
            cluster_public_key: public,
        },
    )
    .expect("write the identity");
    IdentityHandle::load(dir).expect("load the identity")
}

async fn session(
    dir: &TempDir,
    identity: IdentityHandle,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), quoro_node_agent::AgentError>>,
) {
    let store = NodeStore::open(dir.path()).await.expect("open the store");
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store,
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });
    let (stop_tx, stop_rx) = watch::channel(false);
    let running = tokio::spawn(async move { session.run(stop_rx).await });
    (stop_tx, running)
}

#[tokio::test]
async fn a_reachable_advertised_address_becomes_the_primary() {
    let dir = TempDir::new();
    let cluster = sign::signing_key_from_seed(&[4; 32]);
    let (cert, key) = self_signed();
    let pin = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let fallback = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let primary = tokio::spawn(serve_welcome(
        listener,
        acceptor,
        Arc::new(AtomicUsize::new(0)),
    ));

    let closed = closed_url();
    let identity = write_identity(
        dir.path(),
        &cluster,
        &closed,
        &pin,
        std::slice::from_ref(&fallback),
    );
    let (stop, running) = session(&dir, identity.clone()).await;

    let updated = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = identity.get();
            if current.primary_url == fallback {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the fallback address was promoted");

    assert_eq!(updated.primary_url, fallback);
    assert_eq!(updated.primary_urls, vec![closed.clone()]);
    assert_eq!(updated.fingerprint, pin, "the pin never changes");

    let on_disk = identity::load_identity(dir.path()).expect("read identity.json");
    assert_eq!(on_disk.primary_url, fallback);
    assert_eq!(on_disk.primary_urls, vec![closed]);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    primary.abort();
}

#[tokio::test]
async fn an_advertised_address_with_another_certificate_is_never_promoted() {
    let dir = TempDir::new();
    let cluster = sign::signing_key_from_seed(&[5; 32]);
    let (pinned_cert, _pinned_key) = self_signed();
    let pin = format!("sha256:{}", tls::cert_fingerprint(pinned_cert.as_ref()));
    let (other_cert, other_key) = self_signed();
    let acceptor = TlsAcceptor::from(server_config(other_cert, other_key));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let other = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let primary = tokio::spawn(serve_welcome(
        listener,
        acceptor,
        Arc::new(AtomicUsize::new(0)),
    ));

    let closed = closed_url();
    let identity = write_identity(
        dir.path(),
        &cluster,
        &closed,
        &pin,
        std::slice::from_ref(&other),
    );
    let (stop, running) = session(&dir, identity.clone()).await;

    // Long enough for the first attempt: the closed port, dialed again while
    // it refuses for two seconds, then the wrong pin.
    tokio::time::sleep(Duration::from_millis(2_700)).await;
    let current = identity.get();
    assert_eq!(current.primary_url, closed, "the pin refused the host");
    assert_eq!(current.primary_urls, vec![other]);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    primary.abort();
}

#[tokio::test]
async fn a_restarting_primary_is_dialed_again_before_the_other_addresses() {
    let dir = TempDir::new();
    let cluster = sign::signing_key_from_seed(&[6; 32]);
    let (cert, key) = self_signed();
    let pin = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    // Nothing listens on the primary's port yet, so it refuses the
    // connection, as a primary does while it restarts.
    let primary_url = closed_url();
    let port: u16 = primary_url.rsplit(':').next().unwrap().parse().unwrap();
    let (silent, silent_accepted, silent_task) = silent_url().await;
    let identity = write_identity(
        dir.path(),
        &cluster,
        &primary_url,
        &pin,
        std::slice::from_ref(&silent),
    );
    let started = Instant::now();
    let (stop, running) = session(&dir, identity.clone()).await;

    // The listener comes back 300 ms later.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let welcomed = Arc::new(AtomicUsize::new(0));
    let primary = tokio::spawn(serve_welcome(listener, acceptor, Arc::clone(&welcomed)));

    within(Duration::from_secs(20), || {
        welcomed.load(Ordering::SeqCst) > 0
    })
    .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "the node reconnected {elapsed:?} after the restart began"
    );
    assert_eq!(
        silent_accepted.load(Ordering::SeqCst),
        0,
        "the address that does not answer was never dialed"
    );
    assert_eq!(identity.get().primary_url, primary_url);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    primary.abort();
    silent_task.abort();
}

#[tokio::test]
async fn an_advertised_address_that_never_answers_costs_a_short_timeout() {
    let dir = TempDir::new();
    let cluster = sign::signing_key_from_seed(&[7; 32]);
    let (cert, key) = self_signed();
    let pin = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let reachable = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let primary = tokio::spawn(serve_welcome(
        listener,
        acceptor,
        Arc::new(AtomicUsize::new(0)),
    ));
    let (silent, silent_accepted, silent_task) = silent_url().await;

    // `primary_url` refuses for good; the first advertised address never
    // answers, the second is the primary.
    let closed = closed_url();
    let identity = write_identity(
        dir.path(),
        &cluster,
        &closed,
        &pin,
        &[silent.clone(), reachable.clone()],
    );
    let (stop, running) = session(&dir, identity.clone()).await;

    // Two seconds of redialing the refusing address, then two seconds on
    // the silent one, instead of the full ten-second connect timeout.
    let elapsed = within(Duration::from_secs(20), || {
        identity.get().primary_url == reachable
    })
    .await;
    assert!(
        elapsed < Duration::from_secs(6),
        "the reachable address was promoted after {elapsed:?}"
    );
    assert_eq!(silent_accepted.load(Ordering::SeqCst), 1);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    primary.abort();
    silent_task.abort();
}

/// Serves TLS and answers every session upgrade with `503` and the
/// `not_primary` problem, as a demoted primary does (section 7.4), counting
/// the upgrades it refused. The handshake's callback returns tungstenite's
/// own error response, whatever its size.
#[allow(clippy::result_large_err)]
async fn serve_not_primary(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    refused: Arc<AtomicUsize>,
) {
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
        let refused = Arc::clone(&refused);
        tokio::spawn(async move {
            let Ok(stream) = acceptor.accept(tcp).await else {
                return;
            };
            let _ = tokio_tungstenite::accept_hdr_async(
                stream,
                |_request: &Request, _response: Response| -> Result<Response, ErrorResponse> {
                    refused.fetch_add(1, Ordering::SeqCst);
                    let mut answer = ErrorResponse::new(Some(
                        r#"{"type":"about:blank","title":"Not primary","status":503,"code":"not_primary"}"#
                            .to_owned(),
                    ));
                    *answer.status_mut() =
                        tokio_tungstenite::tungstenite::http::StatusCode::SERVICE_UNAVAILABLE;
                    answer.headers_mut().insert(
                        "content-type",
                        tokio_tungstenite::tungstenite::http::HeaderValue::from_static(
                            "application/problem+json",
                        ),
                    );
                    Err(answer)
                },
            )
            .await;
        });
    }
}

/// A demoted primary answers the session upgrade with `503` `not_primary`
/// (section 7.4). That is not fatal: the node tries its next address in the
/// same attempt, reaches the primary of the newer term there, and keeps it
/// (sections 4.4 and 5.10).
#[tokio::test]
async fn an_upgrade_answered_with_503_moves_on_to_the_next_address() {
    let dir = TempDir::new();
    let cluster = sign::signing_key_from_seed(&[8; 32]);
    let (cert, key) = self_signed();
    let pin = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    let acceptor = TlsAcceptor::from(server_config(cert, key));

    let demoted_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let demoted = format!(
        "https://127.0.0.1:{}",
        demoted_listener.local_addr().unwrap().port()
    );
    let refused = Arc::new(AtomicUsize::new(0));
    let old_primary = tokio::spawn(serve_not_primary(
        demoted_listener,
        acceptor.clone(),
        Arc::clone(&refused),
    ));

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let promoted = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let welcomed = Arc::new(AtomicUsize::new(0));
    let new_primary = tokio::spawn(serve_welcome(listener, acceptor, Arc::clone(&welcomed)));

    let identity = write_identity(
        dir.path(),
        &cluster,
        &demoted,
        &pin,
        std::slice::from_ref(&promoted),
    );
    let (stop, running) = session(&dir, identity.clone()).await;

    within(Duration::from_secs(10), || {
        welcomed.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(
        refused.load(Ordering::SeqCst) >= 1,
        "the demoted primary was dialed first"
    );
    within(Duration::from_secs(5), || {
        identity.get().primary_url == promoted
    })
    .await;
    assert_eq!(identity.get().primary_urls, vec![demoted]);
    assert!(!running.is_finished(), "a 503 is not fatal");

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    old_primary.abort();
    new_primary.abort();
}
