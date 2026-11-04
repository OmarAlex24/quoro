// SPDX-License-Identifier: AGPL-3.0-only
//! Following the primary to an advertised address (section 5.10): when
//! `primary_url` refuses a dial, the node tries `primary_urls` with the same
//! pin and, once a session reaches `welcome`, persists that address.

mod agent_common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agent_common::{TempDir, facts};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_node_agent::identity::{self, Identity, IdentityHandle};
use quoro_node_agent::session::{Session, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::tls;
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{ControlMessage, Welcome};
use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

/// A self-signed certificate for `127.0.0.1` and its private key, DER encoded.
fn self_signed() -> (CertificateDer<'static>, Vec<u8>) {
    let key = KeyPair::generate().expect("generate a key");
    let params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("params");
    let cert = params.self_signed(&key).expect("self-sign");
    (cert.der().clone(), key.serialize_der())
}

fn server_config(cert: CertificateDer<'static>, key: Vec<u8>) -> Arc<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![cert],
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key)),
        )
        .expect("server config");
    Arc::new(config)
}

/// Serves the node session far enough to answer `hello` with `welcome`.
async fn serve_welcome(listener: TcpListener, acceptor: TlsAcceptor) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
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
                    })
                    .to_frame("msg_welcome", frame["id"].as_str())
                    .to_text();
                    let _ = ws.send(Message::Text(welcome.into())).await;
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
    let primary = tokio::spawn(serve_welcome(listener, acceptor));

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
    let primary = tokio::spawn(serve_welcome(listener, acceptor));

    let closed = closed_url();
    let identity = write_identity(
        dir.path(),
        &cluster,
        &closed,
        &pin,
        std::slice::from_ref(&other),
    );
    let (stop, running) = session(&dir, identity.clone()).await;

    // Long enough for the first attempt (the closed port, then the wrong pin)
    // and well inside the one-second redial backoff.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let current = identity.get();
    assert_eq!(current.primary_url, closed, "the pin refused the host");
    assert_eq!(current.primary_urls, vec![other]);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), running).await;
    primary.abort();
}
