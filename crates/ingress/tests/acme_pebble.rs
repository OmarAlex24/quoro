//! ACME issuance and renewal against Pebble (issue #65).
//!
//! The test runs only when the maintainer's Pebble is up: it reads
//! `PEBBLE_DIRECTORY` (for example `https://127.0.0.1:14000/dir`),
//! `PEBBLE_ROOT` (the PEM of the CA that signed the directory's certificate)
//! and `CHALLTESTSRV` (for example `http://127.0.0.1:8055`). Without them it
//! prints a skip line and passes, so normal `cargo test` stays offline.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use quoro_ingress::IngressEvent;
use quoro_ingress::acme::{AcmeConfig, AcmeManager};
use quoro_ingress::certs::CertStore;
use quoro_providers::{DnsProvider, ProviderError, RecordId};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const HOSTNAME: &str = "app.example.test";

/// A `DnsProvider` backed by pebble-challtestsrv's management API.
struct ChallTestSrv {
    address: Address,
    created: Mutex<Vec<(String, String)>>,
    deleted: Mutex<Vec<RecordId>>,
}

#[derive(Clone, Debug)]
struct Address {
    host: String,
    port: u16,
}

impl Address {
    fn parse(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| format!("CHALLTESTSRV must be an http:// URL, got {url:?}"))?;
        let rest = rest.trim_end_matches('/');
        match rest.split_once(':') {
            Some((host, port)) => Ok(Self {
                host: host.to_string(),
                port: port
                    .parse()
                    .map_err(|_| format!("bad port in CHALLTESTSRV {url:?}"))?,
            }),
            None => Ok(Self {
                host: rest.to_string(),
                port: 80,
            }),
        }
    }
}

impl ChallTestSrv {
    async fn post(&self, path: &str, body: &str) -> Result<(), ProviderError> {
        let socket = format!("{}:{}", self.address.host, self.address.port);
        let mut stream = TcpStream::connect(&socket)
            .await
            .map_err(|e| ProviderError::Transport(format!("challtestsrv {socket}: {e}")))?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {socket}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| ProviderError::Transport(format!("challtestsrv {path}: {e}")))?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .map_err(|e| ProviderError::Transport(format!("challtestsrv {path}: {e}")))?;
        let status_line = response.lines().next().unwrap_or_default();
        let code = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        if code != 200 {
            return Err(ProviderError::Api {
                status: code,
                message: format!("challtestsrv {path}: {status_line}"),
            });
        }
        Ok(())
    }
}

impl DnsProvider for ChallTestSrv {
    async fn create_txt(&self, name: &str, value: &str) -> Result<RecordId, ProviderError> {
        let body = serde_json::json!({ "host": format!("{name}."), "value": value }).to_string();
        self.post("/set-txt", &body).await?;
        self.created
            .lock()
            .await
            .push((name.to_string(), value.to_string()));
        Ok(RecordId(name.to_string()))
    }

    async fn delete_record(&self, id: &RecordId) -> Result<(), ProviderError> {
        let body = serde_json::json!({ "host": format!("{}.", id.0) }).to_string();
        self.post("/clear-txt", &body).await?;
        self.deleted.lock().await.push(id.clone());
        Ok(())
    }

    async fn upsert_cname(
        &self,
        name: &str,
        _target: &str,
        _proxied: bool,
    ) -> Result<RecordId, ProviderError> {
        // Unused by the ACME manager; the test server has no CNAME API.
        Ok(RecordId(name.to_string()))
    }

    async fn delete_cname(&self, _name: &str) -> Result<(), ProviderError> {
        Ok(())
    }

    async fn txt_visible(&self, _name: &str, _value: &str) -> Result<bool, ProviderError> {
        Ok(true)
    }
}

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

#[tokio::test]
async fn issues_then_renews_against_pebble() {
    let (Some(directory), Some(root), Some(challtestsrv)) = (
        env("PEBBLE_DIRECTORY"),
        env("PEBBLE_ROOT"),
        env("CHALLTESTSRV"),
    ) else {
        println!(
            "skip: PEBBLE_DIRECTORY, PEBBLE_ROOT and CHALLTESTSRV are not set (Pebble is not running)"
        );
        return;
    };

    let dir = tmp_dir("acme-pebble");
    let store_dir = dir.join("certs");
    let state_dir = dir.join("state");
    let store = CertStore::open(&store_dir).unwrap();
    let dns = Arc::new(ChallTestSrv {
        address: Address::parse(&challtestsrv).unwrap(),
        created: Mutex::new(Vec::new()),
        deleted: Mutex::new(Vec::new()),
    });
    let (events, mut receiver) = tokio::sync::broadcast::channel(64);
    let mut config = AcmeConfig::new(directory, state_dir.clone());
    config.root_pem = Some(PathBuf::from(root));
    config.contact = Vec::new();
    config.propagation_timeout = Duration::from_secs(30);
    config.propagation_poll = Duration::from_millis(200);
    let manager = AcmeManager::new(config, Arc::clone(&dns), store.clone(), events);

    let first = manager
        .ensure(HOSTNAME)
        .await
        .expect("first issuance succeeds");
    assert!(store.get(HOSTNAME).is_some());
    assert_eq!(store.get(HOSTNAME).unwrap().leaf_der, first.leaf_der);

    let issued = collect_events(&mut receiver);
    assert!(
        issued.iter().any(|e| matches!(
            e,
            IngressEvent::CertificateIssued { hostname, not_after }
                if hostname == HOSTNAME && not_after.ends_with('Z')
        )),
        "no CertificateIssued event: {issued:?}"
    );
    assert!(
        issued
            .iter()
            .any(|e| matches!(e, IngressEvent::CertificatePending { renewal: false, .. })),
        "no initial CertificatePending event: {issued:?}"
    );

    let created = dns.created.lock().await.clone();
    assert!(
        created
            .iter()
            .any(|(name, _)| name == "_acme-challenge.app.example.test"),
        "the challenge record was not created: {created:?}"
    );
    let deleted = dns.deleted.lock().await.clone();
    assert!(
        deleted
            .iter()
            .any(|id| id.0 == "_acme-challenge.app.example.test"),
        "the challenge record was not cleared: {deleted:?}"
    );

    // The account key is private, in a private directory.
    assert_eq!(mode(&state_dir), 0o700);
    let account = std::fs::read_dir(&state_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("account-")
        })
        .expect("an account file");
    assert_eq!(mode(&account), 0o600);

    // A second issuance renews, names the old certificate in `replaces` and
    // uses a new key, so the leaf differs.
    let second = manager.ensure(HOSTNAME).await.expect("renewal succeeds");
    assert_ne!(first.leaf_der, second.leaf_der);
    let events = collect_events(&mut receiver);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, IngressEvent::CertificatePending { renewal: true, .. })),
        "no renewal CertificatePending event: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, IngressEvent::CertificateIssued { .. })),
        "no renewal CertificateIssued event: {events:?}"
    );
    assert_eq!(store.get(HOSTNAME).unwrap().leaf_der, second.leaf_der);

    // Every run left nothing behind.
    assert!(dns.deleted.lock().await.len() >= dns.created.lock().await.len());

    std::fs::remove_dir_all(&dir).unwrap();
}

/// Drains the events already queued by an `ensure` call.
fn collect_events(
    receiver: &mut tokio::sync::broadcast::Receiver<IngressEvent>,
) -> Vec<IngressEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    events
}

/// The provider's HTTP path, against a local stand-in for challtestsrv, so the
/// request shape is checked even when Pebble is not running.
#[tokio::test]
async fn posts_txt_records_to_challtestsrv() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = Address {
        host: "127.0.0.1".to_string(),
        port: listener.local_addr().unwrap().port(),
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&seen);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 512];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
                if n == 0 || request.ends_with(b"}") {
                    break;
                }
            }
            recorder
                .lock()
                .await
                .push(String::from_utf8_lossy(&request).into_owned());
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        }
    });
    let dns = ChallTestSrv {
        address,
        created: Mutex::new(Vec::new()),
        deleted: Mutex::new(Vec::new()),
    };
    let id = dns
        .create_txt("_acme-challenge.app.example.test", "dns01-value")
        .await
        .unwrap();
    dns.delete_record(&id).await.unwrap();
    server.await.unwrap();

    let requests = seen.lock().await;
    let set = &requests[0];
    assert!(set.starts_with("POST /set-txt HTTP/1.1"), "{set}");
    assert!(
        set.contains(r#"{"host":"_acme-challenge.app.example.test.","value":"dns01-value"}"#),
        "{set}"
    );
    let clear = &requests[1];
    assert!(clear.starts_with("POST /clear-txt HTTP/1.1"), "{clear}");
    assert!(
        clear.contains(r#"{"host":"_acme-challenge.app.example.test."}"#),
        "{clear}"
    );
}
