// SPDX-License-Identifier: AGPL-3.0-only
//! Pinned TLS tests (`node-agent-protocol.md` section 2.2): the verifier accepts
//! the pinned self-signed certificate and refuses any other.

use std::sync::Arc;

use quoro_node_agent::tls;
use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

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

#[tokio::test]
async fn the_pin_decides_which_certificate_is_accepted() {
    let (cert, key) = self_signed();
    let (other_cert, _other_key) = self_signed();
    let acceptor = TlsAcceptor::from(server_config(cert.clone(), key));

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _ = acceptor.accept(tcp).await;
            });
        }
    });

    let url = format!("https://127.0.0.1:{}", address.port());
    let pin = tls::cert_fingerprint(cert.as_ref());
    let stream = tls::connect(&url, &format!("sha256:{pin}"))
        .await
        .expect("the pinned certificate is accepted");
    drop(stream);

    let wrong = tls::cert_fingerprint(other_cert.as_ref());
    let refused = tls::connect(&url, &format!("sha256:{wrong}")).await;
    assert!(
        refused.is_err(),
        "a certificate with another digest must be refused"
    );

    server.abort();
}

#[tokio::test]
async fn an_unusable_pin_is_refused_before_connecting() {
    assert!(tls::client_config("not-a-fingerprint").is_err());
    assert!(tls::client_config(&"a".repeat(64)).is_ok());
}

#[tokio::test]
async fn a_server_that_never_answers_the_handshake_times_out() {
    // Accept the TCP connection and then say nothing: the timeout must fire
    // instead of waiting for the handshake forever.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            held.push(tcp);
        }
    });

    let url = format!("https://127.0.0.1:{}", address.port());
    let start = std::time::Instant::now();
    let error = tls::connect_with_timeout(
        &url,
        &format!("sha256:{}", "ab".repeat(32)),
        std::time::Duration::from_millis(100),
    )
    .await
    .expect_err("a silent server cannot complete the handshake");
    let elapsed = start.elapsed();
    assert!(
        error.to_string().contains("timed out"),
        "the error names the timeout: {error}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the overridable timeout bounds the wait, took {elapsed:?}"
    );

    server.abort();
}
