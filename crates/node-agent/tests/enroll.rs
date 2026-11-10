// SPDX-License-Identifier: AGPL-3.0-only
//! Enrollment (`node-agent-protocol.md` sections 2.2-2.5): the join command's
//! three values, the pinned POST, and the identity files it writes.
//!
//! The primary is a hand-written HTTP/1.1 server over tokio-rustls, so the test
//! exercises the real hyper client and TLS verifier without adding a server
//! framework.

mod agent_common;

use std::sync::Arc;
use std::time::Duration;

use agent_common::{TempDir, facts};
use futures_util::StreamExt as _;
use quoro_node_agent::identity::{CUSTODY_KEY_FILE, IdentityHandle};
use quoro_node_agent::session::{Session, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::tls::cert_fingerprint;
use quoro_node_agent::{AgentConfig, AgentError, Identity, JoinTarget, join};
use quoro_proto::custody::CustodyKey;
use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::tungstenite::Message;

fn self_signed() -> (rustls::pki_types::CertificateDer<'static>, Vec<u8>) {
    let key = KeyPair::generate().expect("generate a key");
    let params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("params");
    let cert = params.self_signed(&key).expect("self-sign");
    (cert.der().clone(), key.serialize_der())
}

fn server_config(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: Vec<u8>,
) -> Arc<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Arc::new(
        rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(
                vec![cert],
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key)),
            )
            .expect("server config"),
    )
}

/// Reads one request: the header block and the body selected by
/// `Content-Length`.
async fn read_request(stream: &mut TlsStream<TcpStream>) -> (String, Vec<u8>) {
    let mut buffer = Vec::new();
    let header_end = loop {
        if let Some(index) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break index + 4;
        }
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await.expect("read the request");
        assert!(read > 0, "the connection closed before the headers");
        buffer.extend_from_slice(&chunk[..read]);
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buffer.len() < header_end + length {
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await.expect("read the body");
        assert!(read > 0, "the connection closed before the body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    (headers, buffer[header_end..header_end + length].to_vec())
}

async fn respond(stream: &mut TlsStream<TcpStream>, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("write the response");
    stream.shutdown().await.ok();
}

fn config(dir: &TempDir) -> AgentConfig {
    AgentConfig {
        data_dir: dir.path().to_path_buf(),
        node_name: "node-1".to_owned(),
        bridge: "quoro0".to_owned(),
        dns: vec!["9.9.9.9".parse().unwrap()],
        roles: vec!["node".to_owned()],
        cloudflared_binary: dir.path().join("cloudflared"),
        cloudflared_metrics: "127.0.0.1:20241".parse().unwrap(),
        acme_directory_url: quoro_node_agent::DEFAULT_ACME_DIRECTORY.to_owned(),
        acme_contact: Vec::new(),
        snapshots_at_start: None,
    }
}

#[tokio::test]
async fn join_sends_the_public_key_and_writes_the_identity() {
    let dir = TempDir::new();
    let (cert, key) = self_signed();
    let pin = cert_fingerprint(cert.as_ref());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let secrets = std::sync::Arc::new(std::sync::Mutex::new(None));
    let captured = secrets.clone();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(tcp).await.unwrap();
        let (headers, body) = read_request(&mut stream).await;
        assert!(headers.starts_with("POST /api/v1/nodes/enroll "));
        assert!(headers.to_ascii_lowercase().contains("host: 127.0.0.1:"));
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["join_token"], "join-secret");
        assert_eq!(json["name"], "node-1");
        assert_eq!(json["roles"], serde_json::json!(["node"]));
        assert_eq!(json["capacity"]["cpus"], 8);
        assert_eq!(json["kvm"]["available"], true);
        let public_key = json["public_key"].as_str().unwrap().to_owned();
        *captured.lock().unwrap() = Some(public_key.clone());
        let cluster = quoro_proto::sign::signing_key_from_seed(&[21; 32]);
        let response = serde_json::json!({
            "node_id": "nod_1",
            "cluster_id": "cls_1",
            "primary_url": "https://primary.test",
            "credential": "the-credential",
            "cluster_public_key": quoro_proto::sign::public_key_to_base64(&cluster.verifying_key()),
        })
        .to_string();
        respond(&mut stream, "201 Created", &response).await;
    });

    let target = JoinTarget {
        url: format!("https://127.0.0.1:{}", address.port()),
        token: "join-secret".to_owned(),
        fingerprint: format!("sha256:{pin}"),
    };
    let identity = join(&config(&dir), &target, &facts())
        .await
        .expect("enrollment succeeds");
    server.await.unwrap();

    assert!(quoro_node_agent::is_enrolled(dir.path()));
    assert_eq!(identity.node_id, "nod_1");
    assert_eq!(identity.cluster_id, "cls_1");
    assert_eq!(identity.primary_url, "https://primary.test");
    assert_eq!(identity.fingerprint, format!("sha256:{pin}"));
    assert_eq!(
        identity.cluster_key_id,
        Identity::key_id_for(&identity.cluster_public_key).unwrap()
    );
    assert_eq!(
        quoro_node_agent::identity::read_credential(dir.path()).unwrap(),
        "the-credential"
    );
    let public_key = secrets.lock().unwrap().clone().unwrap();
    let seed = quoro_node_agent::identity::read_seed(dir.path()).unwrap();
    assert_eq!(
        quoro_proto::sign::public_key_to_base64(
            &quoro_proto::sign::signing_key_from_seed(&seed).verifying_key()
        ),
        public_key,
        "the public key sent is the one whose seed stays on the node"
    );

    // A second join on the same directory is refused.
    let error = join(&config(&dir), &target, &facts()).await.unwrap_err();
    assert!(matches!(error, AgentError::AlreadyEnrolled(_)));
}

#[tokio::test]
async fn a_refused_join_carries_the_problem_detail() {
    let dir = TempDir::new();
    let (cert, key) = self_signed();
    let pin = cert_fingerprint(cert.as_ref());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(tcp).await.unwrap();
        let _ = read_request(&mut stream).await;
        respond(
            &mut stream,
            "409 Conflict",
            r#"{"title":"Conflict","detail":"the join token was already used"}"#,
        )
        .await;
    });

    let target = JoinTarget {
        url: format!("https://127.0.0.1:{}", address.port()),
        token: "used".to_owned(),
        fingerprint: format!("sha256:{pin}"),
    };
    let error = join(&config(&dir), &target, &facts()).await.unwrap_err();
    server.await.unwrap();
    match error {
        AgentError::Enroll { status, detail } => {
            assert_eq!(status, 409);
            assert_eq!(detail, "the join token was already used");
        }
        other => panic!("expected an enroll refusal, got {other:?}"),
    }
    assert!(!quoro_node_agent::is_enrolled(dir.path()));
}

/// Enrolls a machine with `roles` in `dir` against a fake primary that
/// answers 201, and returns the enroll request body the primary received.
async fn enroll_with_roles(dir: &TempDir, roles: &[&str]) -> Value {
    let (cert, key) = self_signed();
    let pin = cert_fingerprint(cert.as_ref());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(tcp).await.unwrap();
        let (_headers, body) = read_request(&mut stream).await;
        let cluster = quoro_proto::sign::signing_key_from_seed(&[21; 32]);
        let response = serde_json::json!({
            "node_id": "nod_1",
            "cluster_id": "cls_1",
            "primary_url": "https://primary.test",
            "credential": "the-credential",
            "cluster_public_key": quoro_proto::sign::public_key_to_base64(&cluster.verifying_key()),
        })
        .to_string();
        respond(&mut stream, "201 Created", &response).await;
        serde_json::from_slice::<Value>(&body).unwrap()
    });
    let target = JoinTarget {
        url: format!("https://127.0.0.1:{}", address.port()),
        token: "join-secret".to_owned(),
        fingerprint: format!("sha256:{pin}"),
    };
    let mut config = config(dir);
    config.roles = roles.iter().map(|role| (*role).to_owned()).collect();
    join(&config, &target, &facts())
        .await
        .expect("enrollment succeeds");
    server.await.unwrap()
}

/// The first `hello` a session over the enrolled `dir` sends, as the agent's
/// next start would.
async fn first_hello(dir: &TempDir, roles: &[&str]) -> Value {
    let roles: Vec<String> = roles.iter().map(|role| (*role).to_owned()).collect();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity: IdentityHandle::load(dir.path()).unwrap(),
        store: NodeStore::open(dir.path()).await.unwrap(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    })
    .with_roles(&roles);
    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) =
        tokio_tungstenite::client_async("ws://primary.test/api/v1/nodes/session", client)
            .await
            .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("the node sent a frame in time")
        .expect("the socket is open")
        .expect("a valid WebSocket frame");
    serving.abort();
    let Message::Text(text) = frame else {
        panic!("the first frame is not text: {frame:?}");
    };
    let hello: Value = serde_json::from_str(text.as_str()).unwrap();
    assert_eq!(hello["type"], "hello");
    hello
}

/// Sections 2.3 and 7.2: a machine that enrolls with the `node` role keeps
/// its custody key `0600` before it asks, sends the key's public half as
/// standard base64 of 32 bytes, and its next `hello` reports the same key.
#[tokio::test]
async fn a_node_join_sends_its_custody_key_and_the_next_hello_the_same() {
    use base64::Engine as _;

    let dir = TempDir::new();
    let request = enroll_with_roles(&dir, &["node"]).await;
    let sent = request["custody_public_key"]
        .as_str()
        .unwrap_or_else(|| panic!("a node's enroll request carries a custody key: {request}"))
        .to_owned();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&sent)
        .expect("standard base64");
    assert_eq!(raw.len(), 32);
    let path = dir.path().join(CUSTODY_KEY_FILE);
    let kept = CustodyKey::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(kept.public_key().to_base64(), sent, "the key sent is kept");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "custody.key is root-only");
    }

    let hello = first_hello(&dir, &["node"]).await;
    assert_eq!(hello["body"]["custody_public_key"], sent.as_str());
}

/// Section 7.2: a machine that enrolls without the `node` role sends no
/// custody key, keeps none, and its `hello` reports none.
#[tokio::test]
async fn an_edge_or_witness_join_sends_no_custody_key_and_keeps_none() {
    for roles in [&["edge"][..], &["witness"][..], &["edge", "witness"][..]] {
        let dir = TempDir::new();
        let request = enroll_with_roles(&dir, roles).await;
        assert!(
            request.get("custody_public_key").is_none(),
            "{roles:?}: {request}"
        );
        assert!(!dir.path().join(CUSTODY_KEY_FILE).exists(), "{roles:?}");
        let hello = first_hello(&dir, roles).await;
        assert!(
            hello["body"].get("custody_public_key").is_none(),
            "{roles:?}: {hello}"
        );
        assert!(!dir.path().join(CUSTODY_KEY_FILE).exists(), "{roles:?}");
    }
}
