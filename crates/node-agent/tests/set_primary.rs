// SPDX-License-Identifier: AGPL-3.0-only
//! `quoro node set-primary` (ADR 0139, layer 1): a node moves to its primary's
//! new address only when the host there holds the pinned certificate, and the
//! identity file keeps every other member.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;

use quoro_node_agent::Identity;
use quoro_node_agent::{identity, set_primary, tls};
use quoro_proto::sign;
use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn temp_dir() -> std::path::PathBuf {
    // Tests run in parallel and can read the clock in the same nanosecond, so
    // the counter makes the directory unique within the process too.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "quoro-set-primary-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

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

/// Serves TLS with `cert` on loopback and returns its base URL.
async fn serve(
    cert: CertificateDer<'static>,
    key: Vec<u8>,
) -> (String, tokio::task::JoinHandle<()>) {
    let acceptor = TlsAcceptor::from(server_config(cert, key));
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
    (format!("https://127.0.0.1:{}", address.port()), server)
}

/// Writes a complete enrolled identity the way `quoro join` would, and returns
/// the cluster public key it stores.
fn write_identity(dir: &Path, primary_url: &str, fingerprint: &str) -> String {
    identity::ensure_data_dir(dir).unwrap();
    identity::write_seed(dir, &[7; 32]).unwrap();
    identity::write_credential(dir, "secret-credential").unwrap();

    let cluster = sign::signing_key_from_seed(&[3; 32]);
    let public_key = sign::public_key_to_base64(&cluster.verifying_key());
    let key_id = Identity::key_id_for(&public_key).unwrap();
    // Written as JSON, not through a struct literal, so a member another
    // change adds to `Identity` does not silently leave a required field out.
    let text = format!(
        r#"{{
  "node_id": "nod_test",
  "cluster_id": "cls_test",
  "primary_url": "{primary_url}",
  "primary_urls": ["https://other.example.com:8443"],
  "fingerprint": "{fingerprint}",
  "cluster_public_key": "{public_key}",
  "cluster_key_id": "{key_id}"
}}"#
    );
    let path = dir.join(identity::IDENTITY_FILE);
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    public_key
}

fn identity_bytes(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join(identity::IDENTITY_FILE)).unwrap()
}

#[tokio::test]
async fn a_pinned_certificate_is_accepted_and_only_primary_url_changes() {
    let dir = temp_dir();
    let (cert, key) = self_signed();
    let fingerprint = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    let old = "https://old.example.com:8443".to_owned();
    let public_key = write_identity(&dir, &old, &fingerprint);
    let (url, server) = serve(cert, key).await;

    let (identity, normalized) = set_primary::verify_primary(&dir, &url).await.unwrap();
    assert_eq!(normalized, url);
    assert_eq!(identity.primary_url, old, "verification writes nothing");

    let updated = set_primary::write_primary(&dir, &normalized).unwrap();
    assert_eq!(updated.primary_url, normalized);

    let loaded = identity::load_identity(&dir).unwrap();
    assert_eq!(loaded.primary_url, normalized);
    assert_eq!(loaded.node_id, "nod_test");
    assert_eq!(loaded.cluster_id, "cls_test");
    assert_eq!(
        loaded.primary_urls,
        vec!["https://other.example.com:8443".to_owned()],
        "the advertised addresses are kept"
    );
    assert_eq!(loaded.fingerprint, fingerprint, "the pin never changes");
    assert_eq!(loaded.cluster_public_key, public_key);
    assert_eq!(
        loaded.cluster_key_id,
        Identity::key_id_for(&public_key).unwrap()
    );

    let mode = std::fs::metadata(dir.join(identity::IDENTITY_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "identity.json stays root-only");

    // The same address again is reported as nothing to do and writes nothing.
    let before = identity_bytes(&dir);
    let again = set_primary::write_primary(&dir, &normalized).unwrap();
    assert_eq!(again.primary_url, normalized);
    assert_eq!(
        identity_bytes(&dir),
        before,
        "an unchanged URL is not rewritten"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn another_certificate_is_refused_and_the_identity_is_untouched() {
    let dir = temp_dir();
    let (pinned_cert, _pinned_key) = self_signed();
    let (other_cert, other_key) = self_signed();
    let fingerprint = format!("sha256:{}", tls::cert_fingerprint(pinned_cert.as_ref()));
    write_identity(&dir, "https://old.example.com", &fingerprint);
    let (url, server) = serve(other_cert, other_key).await;

    let before = identity_bytes(&dir);
    let error = set_primary::verify_primary(&dir, &url)
        .await
        .expect_err("a different certificate must be refused");
    let text = error.to_string();
    assert!(
        text.contains("not the pinned primary certificate"),
        "the refusal names the pin: {text}"
    );
    assert!(text.contains("nothing was changed"), "{text}");
    assert_eq!(identity_bytes(&dir), before, "a refusal writes nothing");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn only_https_is_accepted() {
    let dir = temp_dir();
    write_identity(
        &dir,
        "https://old.example.com",
        &format!("sha256:{}", "ab".repeat(32)),
    );
    let before = identity_bytes(&dir);
    for url in [
        "http://127.0.0.1:8443",
        "wss://127.0.0.1:8443",
        "not a url",
        "https://",
    ] {
        let error = set_primary::verify_primary(&dir, url)
            .await
            .expect_err("a non-https URL must be refused");
        assert!(error.to_string().contains("https://"), "{url}: {error}");
        assert_eq!(identity_bytes(&dir), before, "{url} writes nothing");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_closed_port_is_refused_quickly() {
    let dir = temp_dir();
    write_identity(
        &dir,
        "https://old.example.com",
        &format!("sha256:{}", "ab".repeat(32)),
    );

    // A closed port, held for the whole test (issue #414): the local end of
    // a loopback connection kept open. Nothing listens on it, so a connect is
    // refused, and no test running in parallel can bind it, as it could a
    // port released right away.
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_accepted, _) = listener.accept().unwrap();
    let address = client.local_addr().unwrap();
    let url = format!("https://127.0.0.1:{}", address.port());

    let start = std::time::Instant::now();
    let error = set_primary::verify_primary(&dir, &url)
        .await
        .expect_err("nothing listens there");
    let elapsed = start.elapsed();
    assert!(error.to_string().contains("nothing was changed"), "{error}");
    assert!(
        elapsed < tls::CONNECT_TIMEOUT,
        "a refused connection does not wait for the timeout, took {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_url_with_a_path_is_normalized() {
    let dir = temp_dir();
    let (cert, key) = self_signed();
    let fingerprint = format!("sha256:{}", tls::cert_fingerprint(cert.as_ref()));
    write_identity(&dir, "https://old.example.com", &fingerprint);
    let (base, server) = serve(cert, key).await;

    let url = format!("{base}/api/v1/nodes/session?x=1#frag");
    let (_, normalized) = set_primary::verify_primary(&dir, &url).await.unwrap();
    assert_eq!(normalized, base);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
