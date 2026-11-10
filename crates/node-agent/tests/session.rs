// SPDX-License-Identifier: AGPL-3.0-only
//! The session over `tokio::io::duplex` with a fake primary
//! (`node-agent-protocol.md` sections 3-5). No TLS, no network: the protocol
//! core is exercised directly, as the contract asks.

mod agent_common;
mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_common::{
    GATEWAY, SUBNET, TempDir, facts, identity_files, identity_files_pinned, self_signed,
    server_config, snapshot, snapshot_full, spec_vm, stateful_vm,
};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_engine::{Engine as _, FakeEngine, VmState};
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_ingress::proxy::BackendObserver as _;
use quoro_node_agent::edge::EdgeManager;
use quoro_node_agent::freeze;
use quoro_node_agent::identity::{CUSTODY_KEY_FILE, LOG_MARK_FILE};
use quoro_node_agent::ingress::{CertificateEvents, DnsBridge, IngressConfig, IngressSupervisor};
use quoro_node_agent::reconcile::Snapshot;
use quoro_node_agent::session::{Session, SessionEnd, SessionSetup};
use quoro_node_agent::store::{FreezeChecked, NodeStore, PROMOTION_FREEZE, Refollowing, VOTE};
use quoro_node_agent::tls;
use quoro_node_agent::{FenceTimers, LeaseConfig, LeaseState, ManualHostClock, start_local_leases};
use quoro_proto::custody::CustodyKey;
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{
    Command, ControlMessage, DesiredState, DesiredStatus, EdgeBackend, EdgeRoute, HealthCheck,
    LogEntries, QuarantineStatus, Spec, SpecEdge, SpecNetwork, SpecVoter, SpecVotes, SpecWitness,
    SupportRequest, Welcome,
};
use quoro_proto::{
    ErrorCode, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, ProtocolError, ResourceType,
    RevokeReason,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::io::DuplexStream;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, client_async};

const REV: u64 = 5;

fn desired_frame(rev: u64, key: &SigningKey, id: &str) -> String {
    let state = DesiredState {
        rev,
        issued_at: "2026-11-02T10:15:31Z".to_owned(),
        term: 1,
        spec: Spec {
            vms: vec![spec_vm("vm_1", "prj_1", DesiredStatus::Running)],
            routes: Vec::new(),
            network: Some(SpecNetwork {
                subnet: SUBNET.to_owned(),
                gateway: GATEWAY.to_owned(),
            }),
            ingress: None,
            builds: Vec::new(),
            mesh: None,
            edge: None,
            volumes: Vec::new(),
            backups: Vec::new(),
            backup_repository: None,
            votes: None,
            witness: None,
            quarantine_deletes: Vec::new(),
        },
        signature: String::new(),
    };
    let mut body = state.to_unsigned_value();
    sign::sign_body(key, &mut body).unwrap();
    ControlMessage::DesiredState(body)
        .to_frame(id, None)
        .to_text()
}

async fn send(ws: &mut WebSocketStream<DuplexStream>, text: String) {
    ws.send(Message::Text(text.into())).await.unwrap();
}

async fn read_frame(ws: &mut WebSocketStream<DuplexStream>) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("the node sent a frame in time")
            .expect("the socket is open")
            .expect("a valid WebSocket frame");
        if let Message::Text(text) = message {
            return serde_json::from_str(text.as_str()).expect("a JSON frame");
        }
    }
}

async fn read_until(ws: &mut WebSocketStream<DuplexStream>, kind: &str) -> Value {
    loop {
        let frame = read_frame(ws).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

/// The fake primary: drives the handshake and every message the brief lists.
async fn fake_primary(mut ws: WebSocketStream<DuplexStream>, key: SigningKey) {
    let hello = read_frame(&mut ws).await;
    assert_eq!(hello["type"], "hello", "hello is the first message");
    assert_eq!(hello["body"]["node_id"], "nod_test");
    assert!(hello["body"]["log_hash"].as_str().unwrap().len() == 64);

    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 20,
            session_timeout_ms: 120,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    let status = read_until(&mut ws, "status_report").await;
    assert_eq!(status["body"]["applied_rev"], 0);

    // A signed snapshot is stored and reaches the reconciler.
    send(&mut ws, desired_frame(REV, &key, "msg_desired_1")).await;
    let stored = read_until(&mut ws, "command_result").await;
    assert_eq!(stored["body"]["status"], "ok");
    assert_eq!(stored["body"]["detail"], format!("stored rev {REV}"));

    // The same revision is acknowledged and ignored.
    send(&mut ws, desired_frame(REV, &key, "msg_desired_2")).await;
    let ignored = read_until(&mut ws, "command_result").await;
    assert_eq!(ignored["body"]["detail"], format!("ignored rev {REV}"));

    // A snapshot signed by another key is discarded, old snapshot kept.
    let other = sign::signing_key_from_seed(&[99; 32]);
    send(&mut ws, desired_frame(REV + 1, &other, "msg_desired_bad")).await;
    let bad = read_until(&mut ws, "error").await;
    assert_eq!(bad["body"]["code"], "bad_signature");
    assert_eq!(bad["body"]["fatal"], false);
    assert_eq!(bad["re"], "msg_desired_bad");

    // An unknown type is a non-fatal bad_message answered with `re`.
    send(
        &mut ws,
        r#"{"v":1,"type":"teleport","id":"msg_teleport","body":{}}"#.to_owned(),
    )
    .await;
    let unknown = read_until(&mut ws, "error").await;
    assert_eq!(unknown["body"]["code"], "bad_message");
    assert_eq!(unknown["body"]["fatal"], false);
    assert_eq!(unknown["re"], "msg_teleport");

    // A ping is answered with a pong carrying `re`.
    send(
        &mut ws,
        ControlMessage::Ping.to_frame("msg_ping", None).to_text(),
    )
    .await;
    let pong = read_until(&mut ws, "pong").await;
    assert_eq!(pong["re"], "msg_ping");

    // Heartbeats arrive at the interval the welcome configured (20 ms).
    let mut heartbeats = 0;
    while heartbeats < 2 {
        let frame = read_frame(&mut ws).await;
        if frame["type"] == "heartbeat" {
            heartbeats += 1;
        }
    }

    // Go silent: the session must notice the missing frames and redial.
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn the_session_speaks_the_protocol_over_a_duplex_stream() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, mut desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity: identity.clone(),
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });

    let (client, server) = tokio::io::duplex(1 << 16);
    let primary = tokio::spawn(fake_primary_owned(server, key.clone()));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });

    // The primary waits out its silence window and the session redials.
    let end = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the session ended on the timeout")
        .expect("the session task did not panic");
    assert_eq!(end.unwrap(), SessionEnd::Reconnect);
    primary.abort();

    // The stored snapshot reached the reconciler, and the bad one did not.
    let snapshot = desired_rx.borrow_and_update().clone();
    let snapshot = snapshot.expect("a snapshot arrived");
    assert_eq!(snapshot.rev, REV);
    assert_eq!(store.latest_desired().await.unwrap().unwrap().0, REV);
    assert_eq!(
        identity.get().cluster_public_key,
        key_verifying(&key),
        "the pin did not change"
    );
}

async fn fake_primary_owned(server: DuplexStream, key: SigningKey) {
    let ws = tokio_tungstenite::accept_async(server).await.unwrap();
    fake_primary(ws, key).await;
}

fn key_verifying(key: &SigningKey) -> String {
    sign::public_key_to_base64(&key.verifying_key())
}

/// A primary that says nothing after `welcome` but answers every `ping`, as the
/// real one does: only the primary enforces the session timeout (section 4.3).
async fn quiet_primary(server: DuplexStream) {
    let mut ws = tokio_tungstenite::accept_async(server).await.unwrap();
    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 20,
            session_timeout_ms: 120,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    while let Some(Ok(message)) = ws.next().await {
        if let Message::Text(text) = message {
            let frame: Value = serde_json::from_str(text.as_str()).unwrap();
            if frame["type"] == "ping" {
                let pong = ControlMessage::Pong
                    .to_frame("msg_pong", frame["id"].as_str())
                    .to_text();
                send(&mut ws, pong).await;
            }
        }
    }
}

#[tokio::test]
async fn an_idle_session_with_a_live_primary_stays_up() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[5; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
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

    let (client, server) = tokio::io::duplex(1 << 16);
    let primary = tokio::spawn(quiet_primary(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });

    // Ten session timeouts of silence from the primary, apart from pongs.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(!serving.is_finished(), "the idle session was dropped");
    serving.abort();
    primary.abort();
}

/// A primary like [`quiet_primary`] that hands every `event` frame to the test.
async fn listening_primary(server: DuplexStream, events: tokio::sync::mpsc::Sender<Value>) {
    let mut ws = tokio_tungstenite::accept_async(server).await.unwrap();
    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 20,
            session_timeout_ms: 120,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    while let Some(Ok(message)) = ws.next().await {
        if let Message::Text(text) = message {
            let frame: Value = serde_json::from_str(text.as_str()).unwrap();
            if frame["type"] == "ping" {
                let pong = ControlMessage::Pong
                    .to_frame("msg_pong", frame["id"].as_str())
                    .to_text();
                send(&mut ws, pong).await;
            } else if frame["type"] == "event" {
                let _ = events.send(frame).await;
            }
        }
    }
}

#[tokio::test]
async fn an_event_recorded_during_a_session_is_sent_without_a_reconnect() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[6; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });

    let (events_tx, mut events_rx) = tokio::sync::mpsc::channel(4);
    let (client, server) = tokio::io::duplex(1 << 16);
    let primary = tokio::spawn(listening_primary(server, events_tx));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });

    // The session is up; the node then records an event (an app went down).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let body = serde_json::json!({
        "schema_version": 1,
        "id": "evt_01JC9Z6Q5W8N3K4T7R2M1XHB0E",
        "type": "app.down",
        "time": "2026-11-02T10:15:40.000000000Z",
        "source": {"kind": "node", "node_id": "nod_test"},
        "subject": {"type": "app", "id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0D"},
        "actor": {"type": "node", "id": "nod_test"},
        "data": {"app_id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "node_id": "nod_test",
                 "check": "health_check", "failures": 2},
    });
    store
        .push_event("evt_01JC9Z6Q5W8N3K4T7R2M1XHB0E", &body)
        .await
        .unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(3), events_rx.recv())
        .await
        .expect("the event left within the flush interval")
        .expect("the primary saw an event");
    assert_eq!(frame["body"]["type"], "app.down");
    assert!(!serving.is_finished(), "no reconnect was needed");
    // The node removes the event from its buffer only after the frame was
    // written, so the primary may read it first; a loaded runner can take a
    // while to finish the removal (#598).
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !store.buffered_events().await.unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the sent event was still buffered after 3 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    serving.abort();
    primary.abort();
}

/// A primary that sends one `welcome` with `primary_urls` and then stays up,
/// signalling after the node's first status report (which follows the point
/// where the session stores the list).
async fn primary_with_primary_urls(
    server: DuplexStream,
    primary_urls: Vec<String>,
    ready: tokio::sync::oneshot::Sender<()>,
) {
    let mut ws = tokio_tungstenite::accept_async(server).await.unwrap();
    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 20,
            session_timeout_ms: 120,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls,
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    let _ = read_until(&mut ws, "status_report").await;
    let _ = ready.send(());
    tokio::time::sleep(Duration::from_secs(30)).await;
}

async fn serve_one_welcome(
    dir: &TempDir,
    identity: &quoro_node_agent::identity::IdentityHandle,
    urls: Vec<String>,
) {
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity: identity.clone(),
        store,
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (client, server) = tokio::io::duplex(1 << 16);
    let primary = tokio::spawn(primary_with_primary_urls(server, urls, ready_tx));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });
    tokio::time::timeout(Duration::from_secs(5), ready_rx)
        .await
        .expect("the welcome and the first status report")
        .expect("the primary task did not drop the signal");
    serving.abort();
    primary.abort();
}

#[tokio::test]
async fn a_welcome_with_primary_urls_persists_the_sanitized_list() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[5; 32]);
    let identity = identity_files(dir.path(), &key);

    let mut advertised = vec!["http://plain.test:9000".to_owned()];
    advertised.push("https://reachable.test:9000".to_owned());
    advertised.push("https://reachable.test:9000".to_owned());
    for index in 0..10 {
        advertised.push(format!("https://host{index}.test:9000"));
    }
    serve_one_welcome(&dir, &identity, advertised).await;

    let expected: Vec<String> = std::iter::once("https://reachable.test:9000".to_owned())
        .chain((0..7).map(|index| format!("https://host{index}.test:9000")))
        .collect();
    assert_eq!(identity.get().primary_urls, expected);
    assert_eq!(
        quoro_node_agent::identity::load_identity(dir.path())
            .unwrap()
            .primary_urls,
        expected,
        "the list reached identity.json"
    );
}

#[tokio::test]
async fn a_welcome_without_primary_urls_leaves_the_stored_list_alone() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[6; 32]);
    let identity = identity_files(dir.path(), &key);
    let stored = vec!["https://kept.test:9000".to_owned()];
    assert!(identity.set_primary_urls(&stored).unwrap());

    serve_one_welcome(&dir, &identity, Vec::new()).await;

    assert_eq!(identity.get().primary_urls, stored);
    assert_eq!(
        quoro_node_agent::identity::load_identity(dir.path())
            .unwrap()
            .primary_urls,
        stored
    );
}

#[tokio::test]
async fn the_current_primary_url_stays_in_the_stored_list() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[7; 32]);
    let identity = identity_files(dir.path(), &key);
    let current = identity.get().primary_url;
    // A welcome lists the address the node already dials; it is kept, so the
    // node can still fall back to it after `primary_url` moves to an address
    // that does not answer.
    identity
        .set_primary_urls(&[current.clone(), "https://other.test:9000".to_owned()])
        .unwrap();
    let stored = identity.get().primary_urls;
    assert_eq!(stored.len(), 2, "{stored:?}");
    assert_eq!(stored[1], "https://other.test:9000");
    assert!(
        stored[0].starts_with(current.trim_end_matches('/')),
        "{stored:?} keeps {current}"
    );
}

const EDGE_HOST: &str = "edge-demo.example.com";

/// An edge manager over a real ingress supervisor that starts nothing: the
/// routes below are `tunnel` routes and the snapshot carries no tunnel, so no
/// listener and no connector run.
fn edge_manager(dir: &TempDir, store: &NodeStore) -> Arc<EdgeManager> {
    let config = IngressConfig::new(
        dir.path(),
        CloudflaredConfig {
            binary: dir.path().join("no-cloudflared"),
            metrics: "127.0.0.1:0".parse().unwrap(),
            extra_args: Vec::new(),
        },
    );
    let supervisor = Arc::new(IngressSupervisor::new(
        config,
        Arc::new(DnsBridge::new()),
        CertificateEvents::new(store.clone(), None),
    ));
    Arc::new(EdgeManager::new(supervisor))
}

fn edge_backend(vm_id: &str, port: u16) -> EdgeBackend {
    EdgeBackend {
        vm_id: vm_id.to_owned(),
        node_id: "nod_backend".to_owned(),
        address: "127.0.0.1".to_owned(),
        port,
    }
}

/// An edge-only snapshot with one route over `backends`, probed every
/// `interval_s` and taken out after two failed probes.
fn edge_snapshot(rev: u64, backends: Vec<EdgeBackend>, interval_s: u32) -> Snapshot {
    let mut snap = snapshot(rev, Vec::new());
    snap.state.spec.network = None;
    snap.state.spec.edge = Some(SpecEdge {
        routes: vec![EdgeRoute {
            hostname: EDGE_HOST.to_owned(),
            domain_id: "dom_edge".to_owned(),
            app_id: "app_edge".to_owned(),
            tls_mode: "tunnel".to_owned(),
            health_check: HealthCheck {
                path: Some("/health".to_owned()),
                interval_s: Some(interval_s),
                timeout_s: Some(1),
                failures: Some(2),
                start_period_s: None,
                restart: None,
            },
            backends,
            draining_until: None,
        }],
        tunnel: None,
    });
    snap
}

/// The `edge_backends` of a status report, as (vm_id, in_rotation, reason).
fn edge_entries(report: &Value) -> Vec<(String, bool, Option<String>)> {
    report["body"]["edge_backends"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    (
                        entry["vm_id"].as_str().unwrap().to_owned(),
                        entry["in_rotation"].as_bool().unwrap(),
                        entry["reason"].as_str().map(str::to_owned),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The next status report, or `None` when none comes within `limit`. The
/// periodic report is 30 s away, so a report this soon was sent for a change.
async fn status_within(ws: &mut WebSocketStream<DuplexStream>, limit: Duration) -> Option<Value> {
    tokio::time::timeout(limit, read_until(ws, "status_report"))
        .await
        .ok()
}

/// An edge running `first`, and a session over it whose handshake is done.
struct EdgeSession {
    edge: Arc<EdgeManager>,
    store: NodeStore,
    snapshots: watch::Sender<Option<Snapshot>>,
    ws: WebSocketStream<DuplexStream>,
    /// The status report the session sent after `welcome`.
    first: Value,
    serving: tokio::task::JoinHandle<Result<SessionEnd, quoro_node_agent::AgentError>>,
    stop: watch::Sender<bool>,
    manager: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

impl EdgeSession {
    async fn start(first: Snapshot) -> Self {
        let dir = TempDir::new();
        let key = sign::signing_key_from_seed(&[8; 32]);
        let identity = identity_files(dir.path(), &key);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let edge = edge_manager(&dir, &store);
        let rev = first.rev;
        let (snapshots, snapshots_rx) = watch::channel(Some(first));
        let (stop, mut stop_rx) = watch::channel(false);
        let manager = tokio::spawn(Arc::clone(&edge).run(snapshots_rx, async move {
            let _ = stop_rx.wait_for(|stop| *stop).await;
        }));
        tokio::time::timeout(Duration::from_secs(5), async {
            while edge.applied_rev() < rev {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the edge manager applied the snapshot");

        let (desired, _desired_rx) = watch::channel(None);
        let (_observed_tx, observed_rx) = watch::channel(Vec::new());
        let mut session = Session::new(SessionSetup {
            identity,
            store: store.clone(),
            facts: facts(),
            desired,
            observed: observed_rx,
            current: None,
            builds: None,
        })
        .with_edge(Arc::clone(&edge));
        let (client, server) = tokio::io::duplex(1 << 16);
        let accepting = tokio::spawn(tokio_tungstenite::accept_async(server));
        let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
            .await
            .unwrap();
        let serving =
            tokio::spawn(
                async move { session.serve(client_ws, std::future::pending::<()>()).await },
            );
        let mut ws = accepting.await.unwrap().unwrap();

        let hello = read_frame(&mut ws).await;
        send(
            &mut ws,
            ControlMessage::Welcome(Welcome {
                cluster_id: "cls_test".to_owned(),
                term: 1,
                server_time: "2026-11-02T10:15:30Z".to_owned(),
                heartbeat_interval_ms: 500,
                session_timeout_ms: 5_000,
                primary_node_id: "nod_primary".to_owned(),
                primary_urls: Vec::new(),
                schema_version: None,
            })
            .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
            .to_text(),
        )
        .await;
        let first = read_until(&mut ws, "status_report").await;
        Self {
            edge,
            store,
            snapshots,
            ws,
            first,
            serving,
            stop,
            manager,
            _dir: dir,
        }
    }

    async fn finish(self) {
        assert!(!self.serving.is_finished(), "the session stayed up");
        self.serving.abort();
        let _ = self.stop.send(true);
        self.manager.await.unwrap();
    }
}

#[tokio::test]
async fn an_edge_reports_a_rotation_change_at_once() {
    // The first probe is a minute away, so only the test changes the rotation.
    let a: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    let mut run =
        EdgeSession::start(edge_snapshot(1, vec![edge_backend("vm_a", a.port())], 60)).await;
    assert_eq!(
        edge_entries(&run.first),
        vec![("vm_a".to_owned(), true, None)]
    );

    // The proxy could not connect to the backend: the edge takes it out at
    // once, and the primary hears of it at once, not at the next periodic
    // report.
    let error = "connect to 127.0.0.1:18080: Connection refused (os error 111)";
    run.edge.connect_failed(EDGE_HOST, a, error);
    let out = status_within(&mut run.ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the rotation change");
    assert_eq!(
        edge_entries(&out),
        vec![("vm_a".to_owned(), false, Some("connect_failed".to_owned()))]
    );
    assert_eq!(out["body"]["edge_backends"][0]["last_error"], error);

    // A snapshot that adds an instance changes the report too.
    run.snapshots.send_replace(Some(edge_snapshot(
        2,
        vec![edge_backend("vm_a", a.port()), edge_backend("vm_b", 18081)],
        60,
    )));
    let added = status_within(&mut run.ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the new backend");
    assert_eq!(
        edge_entries(&added),
        vec![
            ("vm_a".to_owned(), false, Some("connect_failed".to_owned())),
            ("vm_b".to_owned(), true, None),
        ]
    );

    // The same failure again changes nothing, so it sends nothing.
    run.edge.connect_failed(EDGE_HOST, a, error);
    assert!(
        status_within(&mut run.ws, Duration::from_secs(1))
            .await
            .is_none(),
        "a report without a change"
    );
    run.finish().await;
}

#[tokio::test]
async fn an_edge_reports_a_failed_probe_and_the_backend_it_takes_out() {
    // A port nothing listens on: every probe is refused.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut run = EdgeSession::start(edge_snapshot(1, vec![edge_backend("vm_a", closed)], 1)).await;
    assert_eq!(
        edge_entries(&run.first),
        vec![("vm_a".to_owned(), true, None)]
    );

    // The first failed probe leaves the backend in rotation with an error;
    // the second takes it out. Each is reported within a probe interval.
    let failed = status_within(&mut run.ws, Duration::from_millis(2_500))
        .await
        .expect("a status report after the first failed probe");
    assert_eq!(edge_entries(&failed), vec![("vm_a".to_owned(), true, None)]);
    let error = failed["body"]["edge_backends"][0]["last_error"]
        .as_str()
        .expect("the failed probe's error")
        .to_owned();
    assert!(
        error.starts_with(&format!("connect to 127.0.0.1:{closed}: ")),
        "{error}"
    );
    let out = status_within(&mut run.ws, Duration::from_millis(2_500))
        .await
        .expect("a status report when the backend left the rotation");
    assert_eq!(
        edge_entries(&out),
        vec![("vm_a".to_owned(), false, Some("health_check".to_owned()))]
    );
    run.finish().await;
}

#[tokio::test]
async fn a_newly_applied_revision_is_reported_without_waiting_for_the_periodic_report() {
    // An edge: no VM changes, so only the applied revision moves.
    let mut run = EdgeSession::start(edge_snapshot(1, vec![edge_backend("vm_a", 18080)], 60)).await;
    assert_eq!(run.first["body"]["applied_rev"], 0);

    run.store.set_applied_rev(2).await.unwrap();
    let report = status_within(&mut run.ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the applied revision");
    assert_eq!(report["body"]["applied_rev"], 2);

    // Revisions applied back to back go out in one report.
    run.store.set_applied_rev(3).await.unwrap();
    run.store.set_applied_rev(4).await.unwrap();
    let report = status_within(&mut run.ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the applied revisions");
    assert_eq!(report["body"]["applied_rev"], 4);
    assert!(
        status_within(&mut run.ws, Duration::from_secs(1))
            .await
            .is_none(),
        "a second report for revisions already reported"
    );
    run.finish().await;
}

/// Sections 5.3 and 9: every status report lists the quarantined copies,
/// with `sha256` only once it is known, and a change of the list is reported
/// at once: a copy added, hashed or deleted.
#[tokio::test]
async fn the_quarantined_copies_are_reported_and_a_change_goes_out_at_once() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[11; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let copy = QuarantineStatus {
        quarantine_id: "qrn_01JD0QRN000000000000000001".to_owned(),
        volume_id: "vol_01JD0QRN000000000000000001".to_owned(),
        app_id: "app_01JD0QRN000000000000000001".to_owned(),
        created_at: "2026-11-10T10:00:00Z".to_owned(),
        size_bytes: 4096,
        sha256: None,
    };
    let (quarantines, quarantines_rx) = watch::channel(vec![copy.clone()]);
    let session = Session::new(SessionSetup {
        identity,
        store,
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    })
    .with_quarantines(quarantines_rx);
    let (mut ws, hello, serving) = open_session(session).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 500,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    let first = read_until(&mut ws, "status_report").await;
    assert_eq!(
        first["body"]["quarantines"],
        json!([{
            "quarantine_id": copy.quarantine_id,
            "volume_id": copy.volume_id,
            "app_id": copy.app_id,
            "created_at": copy.created_at,
            "size_bytes": 4096,
        }])
    );

    // The hash is known.
    let sha256 = "ab".repeat(32);
    quarantines.send_replace(vec![QuarantineStatus {
        sha256: Some(sha256.clone()),
        ..copy.clone()
    }]);
    let report = status_within(&mut ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the hash");
    assert_eq!(report["body"]["quarantines"][0]["sha256"], sha256);

    // The copy is deleted.
    quarantines.send_replace(Vec::new());
    let report = status_within(&mut ws, Duration::from_secs(2))
        .await
        .expect("a status report within 2 s of the delete");
    assert!(
        report["body"].get("quarantines").is_none(),
        "{}",
        report["body"]
    );
    assert!(!serving.is_finished(), "the session stayed up");
    serving.abort();
}

/// Waits (real time, bounded) until `ready` holds.
async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn local_grant(lease_id: &str, app_id: &str, token: u64) -> LeaseGrant {
    LeaseGrant {
        lease_id: LeaseId(lease_id.to_owned()),
        resource_type: ResourceType::App,
        resource_id: app_id.to_owned(),
        mode: LeaseMode::Local,
        fencing_token: token,
        duration_ms: None,
        granted_at: "2026-11-02T10:15:33Z".to_owned(),
    }
}

fn revoke_frame(lease_id: &str, token: u64, id: &str) -> String {
    ControlMessage::LeaseRevoke(LeaseRevoke {
        lease_id: LeaseId(lease_id.to_owned()),
        fencing_token: token,
        reason: RevokeReason::Moved,
    })
    .to_frame(id, None)
    .to_text()
}

/// Section 5.13 "Order while the fence runs" (ADR 0222): while a revoke's
/// fence stops a slow VM, the session stays alive. It keeps sending
/// heartbeats, answers `ping`, applies a `lease_grant` for another lease and,
/// on a voter, answers `support_request` (ADR 0237); a `desired_state` and a
/// second revoke of the same lease wait, and are processed in the order they
/// arrived once the revoke is answered.
#[tokio::test]
async fn a_revoke_s_fence_keeps_the_session_alive_and_holds_back_the_rest() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(FakeEngine::new());
    let (lease_desired, _) = watch::channel(Some(snapshot_full(
        1,
        vec![
            stateful_vm("vm_slow", "lse_slow", 1, Vec::new()),
            stateful_vm("vm_next", "lse_next", 1, Vec::new()),
        ],
        Vec::new(),
        Vec::new(),
    )));
    let leases = start_local_leases(
        Arc::clone(&engine),
        store.clone(),
        LeaseConfig::new(
            "nod_test",
            FenceTimers {
                grace: Duration::from_secs(60),
                force: Duration::from_secs(60),
            },
        ),
        lease_desired.subscribe(),
    )
    .await
    .expect("the lease service starts");
    let view = leases.view.clone();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: Some(voter_snapshot(1, 3120)),
        builds: None,
    })
    .with_leases(leases.link.clone())
    .with_host_clock(Arc::new(ManualHostClock::new(
        Some("boot-a"),
        Duration::from_secs(1000),
    )));

    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        session
            .serve(client_ws, async {
                let _ = stop_rx.await;
            })
            .await
    });

    let hello = read_frame(&mut ws).await;
    assert_eq!(hello["type"], "hello");
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 100,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(&mut ws, "status_report").await;

    send(
        &mut ws,
        ControlMessage::LeaseGrant(local_grant("lse_slow", "app_slow", 1))
            .to_frame("msg_grant_slow", None)
            .to_text(),
    )
    .await;
    send(
        &mut ws,
        ControlMessage::LeaseGrant(local_grant("lse_next", "app_next", 1))
            .to_frame("msg_grant_next", None)
            .to_text(),
    )
    .await;
    wait_for("both slow leases to be held", || {
        view.state("lse_slow") == Some(LeaseState::Held)
            && view.state("lse_next") == Some(LeaseState::Held)
    })
    .await;
    let slow = common::running_vm(&engine, "vm_slow", 2).await;
    engine.set_stop_delay(&slow, Duration::from_millis(1500));
    let next = common::running_vm(&engine, "vm_next", 3).await;
    engine.set_stop_delay(&next, Duration::from_millis(800));

    send(&mut ws, revoke_frame("lse_slow", 1, "msg_revoke")).await;
    wait_for("the slow stop to begin", || {
        engine.stop_calls().iter().any(|(id, _)| *id == slow)
    })
    .await;
    // A revoke of another lease waits too, and the frames behind it wait
    // for its own fence.
    send(&mut ws, revoke_frame("lse_next", 1, "msg_revoke_next")).await;
    send(&mut ws, desired_frame(REV, &key, "msg_desired")).await;
    send(
        &mut ws,
        r#"{"v":1,"type":"teleport","id":"msg_teleport","body":{}}"#.to_owned(),
    )
    .await;
    send(&mut ws, revoke_frame("lse_slow", 1, "msg_revoke_again")).await;
    // A grant for the revoked lease waits behind its fence.
    send(
        &mut ws,
        ControlMessage::LeaseGrant(local_grant("lse_slow", "app_slow", 2))
            .to_frame("msg_grant_slow_again", None)
            .to_text(),
    )
    .await;
    send(
        &mut ws,
        ControlMessage::LeaseGrant(local_grant("lse_other", "app_other", 1))
            .to_frame("msg_grant_other", None)
            .to_text(),
    )
    .await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support"),
    )
    .await;
    send(
        &mut ws,
        ControlMessage::Ping.to_frame("msg_ping", None).to_text(),
    )
    .await;

    // Everything let through is answered while the fence runs.
    let mut heartbeats = 0;
    let mut pong_before_answer = false;
    let mut grant_before_answer = false;
    let mut answered: Vec<String> = Vec::new();
    let mut first_answered_at = None;
    let mut desired_answered_at = None;
    while answered.len() < 5 {
        let frame = read_frame(&mut ws).await;
        match frame["type"].as_str().unwrap() {
            // Only the heartbeats between the pong, sent during the fence,
            // and the revoke's answer count.
            "heartbeat" if pong_before_answer && answered.is_empty() => heartbeats += 1,
            "support_grant" => {
                assert_eq!(frame["re"], "msg_support");
                grant_before_answer = answered.is_empty();
            }
            "pong" => {
                assert_eq!(frame["re"], "msg_ping");
                pong_before_answer = answered.is_empty();
                assert_eq!(view.state("lse_other"), Some(LeaseState::Held));
                assert_eq!(view.state("lse_slow"), Some(LeaseState::Revoked));
            }
            "command_result" | "error" => {
                let re = frame["re"].as_str().unwrap().to_owned();
                match re.as_str() {
                    "msg_revoke" => first_answered_at = Some(std::time::Instant::now()),
                    "msg_desired" => desired_answered_at = Some(std::time::Instant::now()),
                    _ => {}
                }
                answered.push(re);
            }
            _ => {}
        }
    }
    assert!(pong_before_answer, "the ping was answered during the fence");
    assert!(
        grant_before_answer,
        "the support_request was answered during the fence"
    );
    assert!(store.support().await.unwrap().is_some());
    assert!(
        heartbeats >= 5,
        "heartbeats kept flowing during the 1.5 s fence, before its answer: {heartbeats}"
    );
    // The revoke first, then the held frames in the order they arrived.
    assert_eq!(
        answered,
        [
            "msg_revoke",
            "msg_revoke_next",
            "msg_desired",
            "msg_teleport",
            "msg_revoke_again"
        ]
    );
    // The held revoke's fence ran its 0.8 s stop after the first answer, and
    // the desired state behind it waited for that fence.
    let waited = desired_answered_at.unwrap() - first_answered_at.unwrap();
    assert!(
        waited >= Duration::from_millis(700),
        "the desired state waited for the second fence: {waited:?}"
    );
    assert!(
        engine.stop_calls().iter().any(|(id, _)| *id == next),
        "the held revoke fenced its lease"
    );
    assert_eq!(view.state("lse_next"), Some(LeaseState::Revoked));
    assert_eq!(engine.inspect(&next).await.unwrap().state, VmState::Stopped);
    wait_for("the held grant of the revoked lease", || {
        view.may_start_stateful("lse_slow", 2)
    })
    .await;
    assert_eq!(engine.inspect(&slow).await.unwrap().state, VmState::Stopped);
    assert_eq!(store.latest_desired().await.unwrap().unwrap().0, REV);

    let _ = stop_tx.send(());
    assert_eq!(serving.await.unwrap().unwrap(), SessionEnd::Shutdown);
    leases.abort();
}

/// Section 5.13: on receipt of a revoke the node starts nothing more for the
/// lease. The revoke reaches the lease service when the session reads it, so
/// a session that ends right after still leaves the lease `revoked` and its
/// VM stopped.
#[tokio::test]
async fn a_revoke_takes_effect_even_when_the_session_ends_at_once() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(FakeEngine::new());
    let (lease_desired, _) = watch::channel(Some(snapshot_full(
        1,
        vec![stateful_vm("vm_gone", "lse_gone", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    )));
    let leases = start_local_leases(
        Arc::clone(&engine),
        store.clone(),
        LeaseConfig::new(
            "nod_test",
            FenceTimers {
                grace: Duration::from_millis(200),
                force: Duration::from_millis(200),
            },
        ),
        lease_desired.subscribe(),
    )
    .await
    .expect("the lease service starts");
    leases
        .link
        .grant(
            quoro_proto::MsgId("msg_grant".to_owned()),
            None,
            local_grant("lse_gone", "app_gone", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_gone", 2).await;
    let view = leases.view.clone();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    })
    .with_leases(leases.link.clone());

    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });

    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 100,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(&mut ws, "status_report").await;
    send(&mut ws, revoke_frame("lse_gone", 1, "msg_revoke")).await;
    ws.close(None).await.unwrap();
    drop(ws);

    assert_eq!(serving.await.unwrap().unwrap(), SessionEnd::Reconnect);
    wait_for("the lease to be revoked", || {
        view.state("lse_gone") == Some(LeaseState::Revoked)
    })
    .await;
    wait_for("the fence to stop the VM", || {
        engine.stop_calls().iter().any(|(id, _)| *id == vm)
    })
    .await;
    leases.abort();
}

/// Section 8.4: the session carries the lease service's renewals. A first
/// `expiring` grant is `uncertain` and renews at once; the renewal goes out
/// as a `lease_renew` frame, the primary's answer (a grant whose `re` names
/// it) makes the lease `held`, and the end of the session makes it
/// `uncertain` again.
#[tokio::test]
async fn the_session_carries_renewals_and_its_end_makes_the_lease_uncertain() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(FakeEngine::new());
    let (lease_desired, _) = watch::channel(Some(snapshot_full(
        1,
        vec![stateful_vm("vm_warm", "lse_warm", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    )));
    let leases = start_local_leases(
        Arc::clone(&engine),
        store.clone(),
        LeaseConfig::new(
            "nod_test",
            FenceTimers {
                grace: Duration::from_secs(10),
                force: Duration::from_secs(5),
            },
        )
        .with_host_clock(Arc::new(ManualHostClock::new(
            Some("boot-a"),
            Duration::from_secs(1000),
        ))),
        lease_desired.subscribe(),
    )
    .await
    .expect("the lease service starts");
    let view = leases.view.clone();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    })
    .with_leases(leases.link.clone());

    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        session
            .serve(client_ws, async {
                let _ = stop_rx.await;
            })
            .await
    });

    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(&mut ws, "status_report").await;

    let grant = |re: Option<&str>| {
        ControlMessage::LeaseGrant(LeaseGrant {
            mode: LeaseMode::Expiring,
            duration_ms: Some(15_000),
            ..local_grant("lse_warm", "app_warm", 1)
        })
        .to_frame(&format!("msg_grant_{}", re.unwrap_or("first")), re)
        .to_text()
    };
    send(&mut ws, grant(None)).await;
    let renew = read_until(&mut ws, "lease_renew").await;
    assert_eq!(renew["body"]["lease_id"], "lse_warm");
    assert_eq!(renew["body"]["fencing_token"], 1);
    assert_eq!(view.state("lse_warm"), Some(LeaseState::Uncertain));
    assert!(!view.may_start_stateful("lse_warm", 1));

    send(&mut ws, grant(Some(renew["id"].as_str().unwrap()))).await;
    wait_for("the answered renewal to make the lease held", || {
        view.state("lse_warm") == Some(LeaseState::Held)
    })
    .await;
    assert!(view.may_start_stateful("lse_warm", 1));

    let _ = stop_tx.send(());
    assert_eq!(serving.await.unwrap().unwrap(), SessionEnd::Shutdown);
    wait_for("the lost session to make the lease uncertain", || {
        view.state("lse_warm") == Some(LeaseState::Uncertain)
    })
    .await;
    assert!(view.may_keep_running("lse_warm", 1));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    leases.abort();
}

/// Reads frames until the answer to the message `id`.
async fn answer_to(ws: &mut WebSocketStream<DuplexStream>, id: &str) -> Value {
    loop {
        let frame = read_frame(ws).await;
        if frame["type"] == "command_result" && frame["re"] == id {
            return frame;
        }
    }
}

fn fence_ack_frame(lease_id: &str, token: u64, id: &str) -> String {
    ControlMessage::Command(Command {
        kind: "fence_ack".to_owned(),
        params: serde_json::json!({
            "lease_ids": [lease_id],
            "fencing_tokens": { lease_id: token },
        }),
    })
    .to_frame(id, None)
    .to_text()
}

/// Section 5.17 (ADR 0245): a `fence_ack` that reaches the node over its
/// session clears only the failed fences below its `fencing_tokens`. The
/// fence of a revoke at token 1 fails; an acknowledgement at token 1 leaves
/// it standing, and one at token 2 clears it.
#[tokio::test]
async fn a_fence_ack_over_the_session_clears_only_below_its_tokens() {
    let dir = TempDir::new();
    let key = sign::signing_key_from_seed(&[4; 32]);
    let identity = identity_files(dir.path(), &key);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(FakeEngine::new());
    let (lease_desired, _) = watch::channel(Some(snapshot_full(
        1,
        vec![stateful_vm("vm_hung", "lse_hung", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    )));
    let leases = start_local_leases(
        Arc::clone(&engine),
        store.clone(),
        LeaseConfig::new(
            "nod_test",
            FenceTimers {
                grace: Duration::from_millis(50),
                force: Duration::from_millis(50),
            },
        ),
        lease_desired.subscribe(),
    )
    .await
    .expect("the lease service starts");
    leases
        .link
        .grant(
            quoro_proto::MsgId("msg_grant".to_owned()),
            None,
            local_grant("lse_hung", "app_hung", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_hung", 2).await;
    engine.set_unkillable(&vm, true);
    let view = leases.view.clone();
    let (desired, _desired_rx) = watch::channel(None);
    let (_observed_tx, observed_rx) = watch::channel(Vec::new());
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    })
    .with_leases(leases.link.clone());

    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving =
        tokio::spawn(async move { session.serve(client_ws, std::future::pending::<()>()).await });

    let hello = read_frame(&mut ws).await;
    send(
        &mut ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 100,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(&mut ws, "status_report").await;

    send(&mut ws, revoke_frame("lse_hung", 1, "msg_revoke")).await;
    let revoked = answer_to(&mut ws, "msg_revoke").await;
    assert_eq!(
        revoked["body"]["error"]["code"], "fence_failed",
        "{revoked}"
    );
    assert!(view.is_failed());

    send(&mut ws, fence_ack_frame("lse_hung", 1, "msg_ack_1")).await;
    let kept = answer_to(&mut ws, "msg_ack_1").await;
    assert_eq!(kept["body"]["status"], "ok", "{kept}");
    assert!(
        kept["body"]["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("lse_hung")),
        "{kept}"
    );
    assert!(view.is_failed(), "a failed fence at the token stays");

    send(&mut ws, fence_ack_frame("lse_hung", 2, "msg_ack_2")).await;
    let cleared = answer_to(&mut ws, "msg_ack_2").await;
    assert_eq!(cleared["body"]["status"], "ok", "{cleared}");
    assert!(!view.is_failed(), "one above the token clears it");

    ws.close(None).await.unwrap();
    drop(ws);
    assert_eq!(serving.await.unwrap().unwrap(), SessionEnd::Reconnect);
    leases.abort();
}

/// `prev_hash` of seq 1 and `log_hash` at `log_seq` 0 (sections 5.1, 5.14).
const ZEROS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// `count` change-log entries from `from_seq`, the first linking to
/// `prev_hash`, hashed the way the primary's `changelog::append` hashes them:
/// hex SHA-256 over the RFC 8785 text of the entry without `hash`.
fn log_chain(from_seq: u64, prev_hash: &str, count: u64, salt: &str) -> Vec<Value> {
    let mut prev = prev_hash.to_owned();
    (from_seq..from_seq + count)
        .map(|seq| {
            let mut entry = json!({
                "seq": seq,
                "term": 1,
                "time": "2026-11-02T10:15:31Z",
                "actor_type": "agent",
                "actor_id": "agt_1",
                "entity_type": "app",
                "entity_id": format!("app_{seq}"),
                "op": "update",
                "payload": { "name": format!("blog-{seq}"), "salt": salt },
                "prev_hash": prev,
            });
            let canonical = quoro_proto::jcs::canonicalize(&entry).unwrap();
            let hash: String = Sha256::digest(canonical.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            entry["hash"] = json!(hash);
            prev = hash;
            entry
        })
        .collect()
}

fn log_entries_frame(entries: Vec<Value>, head_seq: Option<u64>, id: &str) -> String {
    let from_seq = entries[0]["seq"].as_u64().unwrap();
    ControlMessage::LogEntries(LogEntries {
        from_seq,
        entries,
        head_seq,
    })
    .to_frame(id, None)
    .to_text()
}

/// The stored copy of the change log, as section 5.14 entries.
async fn stored_log(store: &NodeStore) -> Vec<Value> {
    store
        .log_entries()
        .await
        .unwrap()
        .iter()
        .map(|entry| entry.to_value().unwrap())
        .collect()
}

/// A node's session setup over `dir`, and the sender of its observed VMs,
/// which the test keeps alive for as long as the session runs.
async fn follow_node(
    dir: &TempDir,
) -> (
    SessionSetup,
    watch::Sender<Vec<quoro_proto::wire::VmStatus>>,
) {
    let key = sign::signing_key_from_seed(&[8; 32]);
    let (desired, _desired_rx) = watch::channel(None);
    let (observed_tx, observed_rx) = watch::channel(Vec::new());
    let setup = SessionSetup {
        identity: identity_files(dir.path(), &key),
        store: NodeStore::open(dir.path()).await.unwrap(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    };
    (setup, observed_tx)
}

type Serving = tokio::task::JoinHandle<(Session, Result<SessionEnd, quoro_node_agent::AgentError>)>;

/// Serves `session` over a new duplex stream: a fake primary reads `hello`,
/// answers `welcome` and waits for the first status report. The task hands
/// the session back when it ends, so a test can reconnect it.
async fn follow_session(session: Session) -> (WebSocketStream<DuplexStream>, Value, Serving) {
    let (mut ws, hello, serving) = open_session(session).await;
    welcome(&mut ws, &hello).await;
    (ws, hello, serving)
}

/// Serves `session` over a new duplex stream and reads its `hello`, and
/// nothing more: the node waits for `welcome`, so the test sees its state
/// as it was when the primary received `hello`.
async fn open_session(mut session: Session) -> (WebSocketStream<DuplexStream>, Value, Serving) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving = tokio::spawn(async move {
        let end = session.serve(client_ws, std::future::pending::<()>()).await;
        (session, end)
    });
    let hello = read_frame(&mut ws).await;
    assert_eq!(hello["type"], "hello", "hello is the first message");
    (ws, hello, serving)
}

/// Answers `hello` with `welcome` and waits for the first status report.
async fn welcome(ws: &mut WebSocketStream<DuplexStream>, hello: &Value) {
    send(
        ws,
        ControlMessage::Welcome(Welcome {
            cluster_id: "cls_test".to_owned(),
            term: 1,
            server_time: "2026-11-02T10:15:30Z".to_owned(),
            heartbeat_interval_ms: 100,
            session_timeout_ms: 15_000,
            primary_node_id: "nod_primary".to_owned(),
            primary_urls: Vec::new(),
            schema_version: None,
        })
        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
        .to_text(),
    )
    .await;
    read_until(ws, "status_report").await;
}

/// Closes the primary's side and returns the session once it ended.
async fn end_session(mut ws: WebSocketStream<DuplexStream>, serving: Serving) -> Session {
    ws.close(None).await.unwrap();
    drop(ws);
    let (session, end) = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the session ended")
        .unwrap();
    assert_eq!(end.unwrap(), SessionEnd::Reconnect);
    session
}

/// The first frame that answers the message `id`.
async fn reply_to(ws: &mut WebSocketStream<DuplexStream>, id: &str) -> Value {
    loop {
        let frame = read_frame(ws).await;
        if frame["re"] == id {
            return frame;
        }
    }
}

/// Sends a `ping` and returns every frame the node sent before its `pong`.
/// Messages are handled in order, so these include every answer to what was
/// sent before the ping.
async fn frames_before_pong(ws: &mut WebSocketStream<DuplexStream>, id: &str) -> Vec<Value> {
    send(ws, ControlMessage::Ping.to_frame(id, None).to_text()).await;
    let mut frames = Vec::new();
    loop {
        let frame = read_frame(ws).await;
        if frame["type"] == "pong" && frame["re"] == id {
            return frames;
        }
        frames.push(frame);
    }
}

fn log_acks(frames: &[Value]) -> Vec<&Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == "log_ack")
        .collect()
}

/// Sections 5.6 and 7.1: a valid `log_entries` is stored, then acknowledged
/// once with its highest entry; a second batch continues the first.
#[tokio::test]
async fn valid_log_entries_are_stored_then_acknowledged_once() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, hello, serving) = follow_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(hello["body"]["log_hash"], ZEROS);

    let first = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(first.clone(), Some(5), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    // The batch was committed before the ack left.
    assert_eq!(stored_log(&store).await, first);
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 3);
    assert_eq!(ack["body"]["hash"], first[2]["hash"]);
    assert_eq!(store.log_head().await.unwrap().unwrap().0, 5);
    let after = frames_before_pong(&mut ws, "msg_ping_1").await;
    assert!(
        log_acks(&after).is_empty(),
        "one ack per message: {after:?}"
    );

    let second = log_chain(4, first[2]["hash"].as_str().unwrap(), 2, "a");
    send(
        &mut ws,
        log_entries_frame(second.clone(), Some(5), "msg_log_2"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_2").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 5);
    assert_eq!(ack["body"]["hash"], second[1]["hash"]);
    assert_eq!(stored_log(&store).await.len(), 5);

    end_session(ws, serving).await;
}

/// Section 7.1: a message whose entries do not verify stores nothing, not
/// even its valid first entry, and is answered `bad_message`, never acked.
#[tokio::test]
async fn invalid_log_entries_are_a_bad_message_and_store_nothing() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;

    let mut broken = log_chain(1, ZEROS, 3, "a");
    broken[1]["payload"]["name"] = json!("tampered");
    send(&mut ws, log_entries_frame(broken, Some(3), "msg_log_bad")).await;
    let error = reply_to(&mut ws, "msg_log_bad").await;
    assert_eq!(error["type"], "error", "{error}");
    assert_eq!(error["body"]["code"], "bad_message");
    assert_eq!(error["body"]["fatal"], false);
    let after = frames_before_pong(&mut ws, "msg_ping_1").await;
    assert!(log_acks(&after).is_empty(), "no ack: {after:?}");
    assert!(store.log_entries().await.unwrap().is_empty());
    assert_eq!(store.log_head().await.unwrap(), None);

    // A resend from seq 1 of the right entries is then accepted.
    let entries = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(entries.clone(), None, "msg_log_good"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_good").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 3);
    assert_eq!(stored_log(&store).await, entries);

    end_session(ws, serving).await;
}

/// Section 5.1: after storing entries, the `hello` of the next session
/// reports the newest stored `seq` and its hash.
#[tokio::test]
async fn a_reconnect_s_hello_carries_the_stored_log_position() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let session = Session::new(setup);
    let (mut ws, hello, serving) = follow_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(hello["body"]["log_hash"], ZEROS);

    let entries = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(entries.clone(), Some(3), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    let session = end_session(ws, serving).await;

    let (ws, hello, serving) = follow_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 3);
    assert_eq!(hello["body"]["log_hash"], entries[2]["hash"]);
    end_session(ws, serving).await;
}

/// Section 7.1: on `log_diverged` the node moves its copy aside, never
/// deleting it, before it handles the next message: the primary's entries
/// from seq 1 that follow at once are accepted and acked.
#[tokio::test]
async fn log_diverged_quarantines_the_copy_and_the_node_refollows_from_seq_1() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;

    let old = log_chain(1, ZEROS, 3, "old");
    send(
        &mut ws,
        log_entries_frame(old.clone(), Some(3), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");

    let diverged = ControlMessage::Error(ProtocolError {
        code: ErrorCode::LogDiverged,
        message: "log_seq 3 does not match".to_owned(),
        fatal: false,
        term: None,
    });
    send(&mut ws, diverged.to_frame("msg_diverged", None).to_text()).await;
    let new = log_chain(1, ZEROS, 2, "new");
    send(
        &mut ws,
        log_entries_frame(new.clone(), Some(2), "msg_log_2"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_2").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 2);
    assert_eq!(ack["body"]["hash"], new[1]["hash"]);
    assert_eq!(stored_log(&store).await, new);

    let files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path().join("quarantine"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1, "{files:?}");
    let copy: Vec<Value> = std::fs::read_to_string(&files[0])
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(copy, old);
    let rows: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT kind, path, last_seq, last_hash FROM quarantine")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![(
            "change_log".to_owned(),
            files[0].to_string_lossy().into_owned(),
            3,
            old[2]["hash"].as_str().unwrap().to_owned(),
        )]
    );

    end_session(ws, serving).await;
}

/// Every text frame the node sends until its session ends and the socket
/// closes.
async fn frames_until_closed(ws: &mut WebSocketStream<DuplexStream>) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the session ended in time");
        match message {
            Some(Ok(Message::Text(text))) => {
                frames.push(serde_json::from_str(text.as_str()).expect("a JSON frame"));
            }
            Some(Ok(Message::Close(_)) | Err(_)) | None => return frames,
            Some(Ok(_)) => {}
        }
    }
}

/// The session's end, once the socket closed.
async fn ended(serving: Serving) -> Session {
    let (session, end) = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the session ended")
        .unwrap();
    assert_eq!(end.unwrap(), SessionEnd::Reconnect);
    session
}

/// Section 7.1: when the quarantined copy cannot be written, nothing is
/// deleted and nothing recorded in `quarantine`, and the session ends. The
/// mark and the refollowing state were written before the copy was to move
/// (ADR 0261), so the next session moves it before its `hello`, and sends
/// no `hello` while it cannot. A regular file where the quarantine
/// directory goes makes creating the copy fail.
#[tokio::test]
async fn a_failed_quarantine_export_deletes_nothing_and_ends_the_session() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;

    let old = log_chain(1, ZEROS, 3, "old");
    send(
        &mut ws,
        log_entries_frame(old.clone(), Some(3), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");

    std::fs::write(dir.path().join("quarantine"), b"not a directory").unwrap();
    let diverged = ControlMessage::Error(ProtocolError {
        code: ErrorCode::LogDiverged,
        message: "log_seq 3 does not match".to_owned(),
        fatal: false,
        term: None,
    });
    send(&mut ws, diverged.to_frame("msg_diverged", None).to_text()).await;
    frames_until_closed(&mut ws).await;
    let session = ended(serving).await;

    assert_eq!(stored_log(&store).await, old);
    assert_eq!(quarantined(&store).await, 0);
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );

    // The copy still cannot move: no `hello` reports it.
    let (session, frames) = no_hello(session).await;
    assert!(frames.is_empty(), "no hello: {frames:?}");
    assert_eq!(stored_log(&store).await, old);

    // Once it can, the next session moves it before its `hello`.
    std::fs::remove_file(dir.path().join("quarantine")).unwrap();
    let (ws, hello, serving) = open_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(quarantined(&store).await, 1);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    drop(ws);
    ended(serving).await;
}

/// Section 7.1: a batch the store cannot write is neither acknowledged nor
/// answered `bad_message`; the session ends, and the next `hello` reports
/// the position actually stored.
#[tokio::test]
async fn a_store_failure_sends_no_ack_and_ends_the_session() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;

    sqlx::query("CREATE TRIGGER t BEFORE INSERT ON change_log BEGIN SELECT RAISE(ABORT,'x'); END;")
        .execute(store.pool())
        .await
        .unwrap();
    let entries = log_chain(1, ZEROS, 3, "a");
    send(&mut ws, log_entries_frame(entries, Some(3), "msg_log_1")).await;
    let frames = frames_until_closed(&mut ws).await;
    assert!(log_acks(&frames).is_empty(), "no ack: {frames:?}");
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_log_1"),
        "no answer at all: {frames:?}"
    );
    let session = ended(serving).await;

    sqlx::query("DROP TRIGGER t")
        .execute(store.pool())
        .await
        .unwrap();
    let (ws, hello, serving) = follow_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(hello["body"]["log_hash"], ZEROS);
    end_session(ws, serving).await;
}

/// Section 5.1: a node that cannot read its change-log position sends no
/// `hello` rather than one reporting an empty log, which would skip the
/// primary's divergence check (section 7.1). The session ends at once.
#[tokio::test]
async fn no_hello_is_sent_without_the_change_log_position() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let mut session = Session::new(setup);
    sqlx::query("ALTER TABLE change_log RENAME TO change_log_away")
        .execute(store.pool())
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving = tokio::spawn(async move {
        let end = session.serve(client_ws, std::future::pending::<()>()).await;
        (session, end)
    });
    let frames = frames_until_closed(&mut ws).await;
    assert!(frames.is_empty(), "no hello: {frames:?}");
    let session = ended(serving).await;

    sqlx::query("ALTER TABLE change_log_away RENAME TO change_log")
        .execute(store.pool())
        .await
        .unwrap();
    let (ws, hello, serving) = follow_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    end_session(ws, serving).await;
}

/// The change-log mark at a quarantine, 2^53 - 1, as the file holds it
/// (section 7.1 "Refollowing").
const MAX_SEQ_TEXT: &str = "9007199254740991";

/// The mark file's text, read straight from the disk, or `None` when there
/// is no mark file (section 7.1 "The node's mark", section 12).
fn mark_text(dir: &TempDir) -> Option<String> {
    match std::fs::read_to_string(dir.path().join(LOG_MARK_FILE)) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => panic!("the mark file cannot be read: {e}"),
    }
}

/// Writes the mark file straight to the disk, as an earlier agent left it.
fn seed_mark(dir: &TempDir, text: &str) {
    std::fs::write(dir.path().join(LOG_MARK_FILE), text).unwrap();
}

/// Stores `entries` as an earlier session would have.
async fn seed_log(store: &NodeStore, entries: &[Value]) {
    let from_seq = entries[0]["seq"].as_u64().unwrap();
    let prev_hash = entries[0]["prev_hash"].as_str().unwrap();
    let body = LogEntries {
        from_seq,
        entries: entries.to_vec(),
        head_seq: None,
    };
    let verified = quoro_node_agent::follow::verify_batch(from_seq - 1, prev_hash, &body).unwrap();
    store.store_log_batch(&verified, None).await.unwrap();
}

/// How many copies were quarantined.
async fn quarantined(store: &NodeStore) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM quarantine")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// The primary's non-fatal `log_diverged` (section 7.1).
fn diverged_frame(id: &str) -> String {
    ControlMessage::Error(ProtocolError {
        code: ErrorCode::LogDiverged,
        message: "log_seq 3 does not match".to_owned(),
        fatal: false,
        term: None,
    })
    .to_frame(id, None)
    .to_text()
}

/// Serves `session` once, expecting it to end before `welcome`; returns the
/// session and every frame it sent.
async fn no_hello(mut session: Session) -> (Session, Vec<Value>) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let serving = tokio::spawn(async move {
        let end = session.serve(client_ws, std::future::pending::<()>()).await;
        (session, end)
    });
    let frames = frames_until_closed(&mut ws).await;
    (ended(serving).await, frames)
}

/// (a) ADR 0285: an agent that stopped after writing the mark as 2^53 - 1
/// and before recording the state takes the state as "head unknown" and
/// moves its copy aside before it follows: when the primary receives
/// `hello`, both are done and `hello` reports an empty log.
#[tokio::test]
async fn a_quarantine_mark_without_state_finishes_the_quarantine_before_hello() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let old = log_chain(1, ZEROS, 3, "old");
    seed_log(&store, &old).await;
    seed_mark(&dir, MAX_SEQ_TEXT);

    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(hello["body"]["log_hash"], ZEROS);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(quarantined(&store).await, 1);
    let copy: Vec<Value> = std::fs::read_dir(dir.path().join("quarantine"))
        .unwrap()
        .flat_map(|entry| {
            std::fs::read_to_string(entry.unwrap().path())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(copy, old);
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));

    welcome(&mut ws, &hello).await;
    end_session(ws, serving).await;
}

/// (b) Section 7.1 "Refollowing": with the state "head unknown" recorded,
/// any stored copy predates the quarantine, which stopped before it moved
/// the copy. It is moved aside before `hello`, whatever the mark.
#[tokio::test]
async fn a_copy_under_head_unknown_is_quarantined_before_hello() {
    for mark in [Some(MAX_SEQ_TEXT), None] {
        let dir = TempDir::new();
        let (setup, _observed) = follow_node(&dir).await;
        let store = setup.store.clone();
        seed_log(&store, &log_chain(1, ZEROS, 3, "old")).await;
        assert_eq!(
            store
                .set_refollowing(Refollowing::HeadUnknown)
                .await
                .unwrap(),
            FreezeChecked::Written
        );
        if let Some(mark) = mark {
            seed_mark(&dir, mark);
        }

        let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
        assert_eq!(hello["body"]["log_seq"], 0, "mark {mark:?}");
        assert!(stored_log(&store).await.is_empty(), "mark {mark:?}");
        assert_eq!(quarantined(&store).await, 1, "mark {mark:?}");
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );
        assert_eq!(mark_text(&dir).as_deref(), mark);
        welcome(&mut ws, &hello).await;
        end_session(ws, serving).await;
    }
}

/// (c) Section 7.1 "Refollowing", ADR 0285: a mark at 2^53 - 1 is a
/// quarantine's, whatever state is recorded. With a head recorded and a copy
/// stored, a second `log_diverged` came while the node refollowed, and the
/// agent stopped before it recorded "head unknown": the copy is diverged.
/// When the primary receives `hello`, the state is "head unknown" and the
/// copy is moved aside. The node then learns its head again from the next
/// batch, as an agent that stopped between recording the head and lowering
/// the mark, which reads the same, does.
#[tokio::test]
async fn a_quarantine_mark_over_a_recorded_head_finishes_the_quarantine_before_hello() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let old = log_chain(1, ZEROS, 3, "old");
    seed_log(&store, &old).await;
    assert_eq!(
        store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
        FreezeChecked::Written
    );
    seed_mark(&dir, MAX_SEQ_TEXT);

    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(hello["body"]["log_hash"], ZEROS);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(quarantined(&store).await, 1);
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    welcome(&mut ws, &hello).await;

    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 2, "new"), Some(7), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 2);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::Head(7))
    );
    assert_eq!(mark_text(&dir).as_deref(), Some("7"));
    end_session(ws, serving).await;
}

/// (c2) ADR 0280: with a head recorded, a mark that is missing or above it
/// is lowered to that head, and not to the `head_seq` of the batch at hand,
/// before anything is stored: before `hello`, and in a session before the
/// next batch, which here the store refuses. Lowering a missing mark keeps a
/// later ack below the head from writing a mark below it.
#[tokio::test]
async fn a_mark_above_the_recorded_head_is_lowered_to_it_before_the_next_batch() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    assert_eq!(
        store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
        FreezeChecked::Written
    );

    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    welcome(&mut ws, &hello).await;

    // A mark above the head in a running session, as recovery left it
    // before it read a mark at 2^53 - 1 as a quarantine's. The batch
    // carries a later `head_seq`, which does not move the mark.
    seed_mark(&dir, MAX_SEQ_TEXT);
    sqlx::query("CREATE TRIGGER t BEFORE INSERT ON change_log BEGIN SELECT RAISE(ABORT,'x'); END;")
        .execute(store.pool())
        .await
        .unwrap();
    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 2, "new"), Some(9), "msg_log_1"),
    )
    .await;
    let frames = frames_until_closed(&mut ws).await;
    assert!(log_acks(&frames).is_empty(), "no ack: {frames:?}");
    ended(serving).await;
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::Head(5))
    );
}

/// (d) Section 7.1 "The node's mark": a node with a stored copy at L and no
/// mark, as an agent before the mark left it, or a mark below L, as a stop
/// between a batch's commit and its mark write leaves it, reports L in
/// `hello`, and the mark reads L by the time the primary receives it.
#[tokio::test]
async fn hello_raises_a_missing_or_lower_mark_to_the_stored_position() {
    for mark in [None, Some("1")] {
        let dir = TempDir::new();
        let (setup, _observed) = follow_node(&dir).await;
        let store = setup.store.clone();
        let entries = log_chain(1, ZEROS, 3, "a");
        seed_log(&store, &entries).await;
        if let Some(mark) = mark {
            seed_mark(&dir, mark);
        }

        let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
        assert_eq!(hello["body"]["log_seq"], 3, "mark {mark:?}");
        assert_eq!(hello["body"]["log_hash"], entries[2]["hash"]);
        assert_eq!(mark_text(&dir).as_deref(), Some("3"), "mark {mark:?}");
        welcome(&mut ws, &hello).await;
        end_session(ws, serving).await;
    }
}

/// (e) Section 7.1 "The node's mark": a node whose `node.db` was lost keeps
/// its mark. `hello` reports the empty log, the mark stays where it was,
/// also over a batch below it, and the stored copy reads as below the mark,
/// which `quoro cluster promote` refuses (section 7.3).
#[tokio::test]
async fn a_lost_store_leaves_the_log_below_the_mark() {
    let dir = TempDir::new();
    seed_mark(&dir, "7");
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();

    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(mark_text(&dir).as_deref(), Some("7"));
    assert!(store.log_below_mark().await.unwrap());
    welcome(&mut ws, &hello).await;

    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 3, "a"), Some(3), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 3);
    assert_eq!(mark_text(&dir).as_deref(), Some("7"));
    assert!(store.log_below_mark().await.unwrap());
    end_session(ws, serving).await;
}

/// (f) Section 7.1 "The node's mark": the mark covers every `log_ack`
/// before it leaves. When the mark cannot be kept, the committed batch goes
/// unacknowledged and the session ends, and no `hello` goes out while the
/// mark is unreadable; the next `hello` reports the batch and raises the
/// mark to it.
#[tokio::test]
async fn every_log_ack_follows_its_mark() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    assert_eq!(mark_text(&dir), None);

    let first = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(first.clone(), Some(6), "msg_log_1"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["body"]["seq"], 3, "{ack}");
    assert_eq!(mark_text(&dir).as_deref(), Some("3"));

    let second = log_chain(4, first[2]["hash"].as_str().unwrap(), 2, "a");
    send(
        &mut ws,
        log_entries_frame(second.clone(), Some(6), "msg_log_2"),
    )
    .await;
    let ack = reply_to(&mut ws, "msg_log_2").await;
    assert_eq!(ack["body"]["seq"], 5, "{ack}");
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));

    // A mark the node cannot read: the batch commits, but no ack goes out.
    seed_mark(&dir, "not a seq");
    let third = log_chain(6, second[1]["hash"].as_str().unwrap(), 1, "a");
    send(&mut ws, log_entries_frame(third, Some(6), "msg_log_3")).await;
    let frames = frames_until_closed(&mut ws).await;
    assert!(log_acks(&frames).is_empty(), "no ack: {frames:?}");
    let session = ended(serving).await;
    assert_eq!(stored_log(&store).await.len(), 6);

    // While the mark cannot be read, no `hello` goes out.
    let (session, frames) = no_hello(session).await;
    assert!(frames.is_empty(), "no hello: {frames:?}");

    std::fs::remove_file(dir.path().join(LOG_MARK_FILE)).unwrap();
    let (mut ws, hello, serving) = open_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 6);
    assert_eq!(mark_text(&dir).as_deref(), Some("6"));
    welcome(&mut ws, &hello).await;
    end_session(ws, serving).await;
}

/// (g) Section 7.1 "Refollowing", ADR 0261: `log_diverged` writes the mark
/// as 2^53 - 1, records "head unknown" and moves the copy aside, on a full
/// copy and on an empty one, which has nothing to move. The mark comes
/// first: when the state cannot be recorded, the mark is already written
/// and the copy has not moved, and the next session finishes the quarantine
/// before its `hello` (ADR 0285).
#[tokio::test]
async fn log_diverged_marks_records_and_quarantines_in_that_order() {
    // A full copy.
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 3, "old"), Some(3), "msg_log_1"),
    )
    .await;
    assert_eq!(reply_to(&mut ws, "msg_log_1").await["type"], "log_ack");
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(quarantined(&store).await, 1);
    end_session(ws, serving).await;

    // An empty copy, as a lost `node.db` leaves it.
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert_eq!(quarantined(&store).await, 0);
    assert!(!dir.path().join("quarantine").exists());
    end_session(ws, serving).await;

    // The state cannot be recorded: only the mark is written.
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    let old = log_chain(1, ZEROS, 3, "old");
    send(
        &mut ws,
        log_entries_frame(old.clone(), Some(3), "msg_log_1"),
    )
    .await;
    assert_eq!(reply_to(&mut ws, "msg_log_1").await["type"], "log_ack");
    sqlx::query(
        "CREATE TRIGGER r BEFORE INSERT ON node_meta WHEN NEW.key = 'log_refollowing' \
         BEGIN SELECT RAISE(ABORT,'x'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_until_closed(&mut ws).await;
    let session = ended(serving).await;
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert_eq!(store.refollowing().await.unwrap(), None);
    assert_eq!(stored_log(&store).await, old);
    assert_eq!(quarantined(&store).await, 0);

    sqlx::query("DROP TRIGGER r")
        .execute(store.pool())
        .await
        .unwrap();
    let (mut ws, hello, serving) = open_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(quarantined(&store).await, 1);
    welcome(&mut ws, &hello).await;
    end_session(ws, serving).await;
}

/// (h) ADR 0280: the first batch after a quarantine records its `head_seq`
/// H and lowers the mark to H before it is stored: a batch the store
/// refuses leaves both. A batch that ends below H keeps them: its ack does
/// not raise the mark, and a later `head_seq` does not replace H.
#[tokio::test]
async fn the_first_batch_after_a_quarantine_records_its_head_and_lowers_the_mark() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 3, "old"), Some(3), "msg_log_1"),
    )
    .await;
    assert_eq!(reply_to(&mut ws, "msg_log_1").await["type"], "log_ack");
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;

    sqlx::query("CREATE TRIGGER t BEFORE INSERT ON change_log BEGIN SELECT RAISE(ABORT,'x'); END;")
        .execute(store.pool())
        .await
        .unwrap();
    let new = log_chain(1, ZEROS, 2, "new");
    send(
        &mut ws,
        log_entries_frame(new.clone(), Some(5), "msg_log_2"),
    )
    .await;
    let frames = frames_until_closed(&mut ws).await;
    assert!(log_acks(&frames).is_empty(), "no ack: {frames:?}");
    let session = ended(serving).await;
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::Head(5))
    );
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    assert!(stored_log(&store).await.is_empty());

    sqlx::query("DROP TRIGGER t")
        .execute(store.pool())
        .await
        .unwrap();
    let (mut ws, hello, serving) = open_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 0);
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    welcome(&mut ws, &hello).await;
    send(&mut ws, log_entries_frame(new, Some(9), "msg_log_3")).await;
    let ack = reply_to(&mut ws, "msg_log_3").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 2);
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::Head(5))
    );
    assert_eq!(stored_log(&store).await.len(), 2);
    end_session(ws, serving).await;
}

/// (i) Section 7.1 "Refollowing": the batch that brings the copy to the
/// head it refollows to ends the state, and its ack raises the mark to its
/// last `seq`.
#[tokio::test]
async fn the_batch_that_reaches_the_head_ends_refollowing_and_raises_the_mark() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let new = log_chain(1, ZEROS, 2, "new");
    seed_log(&store, &new).await;
    assert_eq!(
        store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
        FreezeChecked::Written
    );
    seed_mark(&dir, "5");

    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 2);
    assert_eq!(mark_text(&dir).as_deref(), Some("5"));
    welcome(&mut ws, &hello).await;

    let rest = log_chain(3, new[1]["hash"].as_str().unwrap(), 4, "new");
    send(&mut ws, log_entries_frame(rest, Some(9), "msg_log_1")).await;
    let ack = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 6);
    assert_eq!(mark_text(&dir).as_deref(), Some("6"));
    assert_eq!(store.refollowing().await.unwrap(), None);
    assert_eq!(store.log_head().await.unwrap().unwrap().0, 9);
    end_session(ws, serving).await;
}

/// (j) Section 7.1 "Refollowing": with the head unknown, a batch without
/// `head_seq` cannot say how far the node must refollow. It stores nothing
/// and is answered `bad_message`, non-fatal, with `re`; the state and the
/// mark do not move, and the session goes on.
#[tokio::test]
async fn a_batch_without_head_seq_while_the_head_is_unknown_is_a_bad_message() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;

    let entries = log_chain(1, ZEROS, 2, "new");
    send(
        &mut ws,
        log_entries_frame(entries.clone(), None, "msg_log_1"),
    )
    .await;
    let error = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(error["type"], "error", "{error}");
    assert_eq!(error["body"]["code"], "bad_message");
    assert_eq!(error["body"]["fatal"], false);
    let after = frames_before_pong(&mut ws, "msg_ping_2").await;
    assert!(log_acks(&after).is_empty(), "no ack: {after:?}");
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));

    send(&mut ws, log_entries_frame(entries, Some(2), "msg_log_2")).await;
    let ack = reply_to(&mut ws, "msg_log_2").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 2);
    end_session(ws, serving).await;
}

/// (k) Section 7.1: an edge-only machine never follows, so it acknowledges
/// nothing and its `hello` reports 0: no mark file is ever written.
#[tokio::test]
async fn an_edge_session_leaves_no_mark_file() {
    let mut run = EdgeSession::start(edge_snapshot(1, vec![edge_backend("vm_a", 18080)], 60)).await;
    let frames = frames_before_pong(&mut run.ws, "msg_ping_1").await;
    assert!(log_acks(&frames).is_empty(), "{frames:?}");
    assert!(!run._dir.path().join(LOG_MARK_FILE).exists());
    assert_eq!(run.store.log_position().await.unwrap().0, 0);
    run.finish().await;
}

/// (l) ADR 0280: with the head unknown, the first batch records its
/// `head_seq` before it lowers the mark. When the head cannot be recorded,
/// the mark still reads 2^53 - 1, nothing is stored and no ack goes out.
#[tokio::test]
async fn the_head_is_recorded_before_the_mark_is_lowered() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );

    // Recording a numeric head fails. "head unknown" is already recorded, so
    // the upsert takes its update path; both paths abort.
    for trigger in [
        "CREATE TRIGGER h_insert BEFORE INSERT ON node_meta \
         WHEN NEW.key = 'log_refollowing' AND NEW.value GLOB '[0-9]*' \
         BEGIN SELECT RAISE(ABORT,'x'); END;",
        "CREATE TRIGGER h_update BEFORE UPDATE ON node_meta \
         WHEN NEW.key = 'log_refollowing' AND NEW.value GLOB '[0-9]*' \
         BEGIN SELECT RAISE(ABORT,'x'); END;",
    ] {
        sqlx::query(trigger).execute(store.pool()).await.unwrap();
    }
    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 2, "new"), Some(5), "msg_log_1"),
    )
    .await;
    let frames = frames_until_closed(&mut ws).await;
    assert!(log_acks(&frames).is_empty(), "no ack: {frames:?}");
    ended(serving).await;
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
}

/// With the head unknown, sends a three-entry batch carrying `head_seq`,
/// which the node must refuse: `bad_message`, non-fatal, with `re`, nothing
/// stored, and neither the state nor the mark moved; the session goes on.
/// The same batch with `head_seq` at its last `seq` is then recorded,
/// stored and acknowledged, and ends the refollowing it reaches.
async fn a_refused_head_seq(head_seq: u64) {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    send(&mut ws, diverged_frame("msg_diverged")).await;
    frames_before_pong(&mut ws, "msg_ping_1").await;

    let entries = log_chain(1, ZEROS, 3, "new");
    send(
        &mut ws,
        log_entries_frame(entries.clone(), Some(head_seq), "msg_log_1"),
    )
    .await;
    let error = reply_to(&mut ws, "msg_log_1").await;
    assert_eq!(error["type"], "error", "head_seq {head_seq}: {error}");
    assert_eq!(error["body"]["code"], "bad_message");
    assert_eq!(error["body"]["fatal"], false);
    let after = frames_before_pong(&mut ws, "msg_ping_2").await;
    assert!(log_acks(&after).is_empty(), "no ack: {after:?}");
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));

    send(&mut ws, log_entries_frame(entries, Some(3), "msg_log_2")).await;
    let ack = reply_to(&mut ws, "msg_log_2").await;
    assert_eq!(ack["type"], "log_ack", "{ack}");
    assert_eq!(ack["body"]["seq"], 3);
    assert_eq!(store.refollowing().await.unwrap(), None);
    assert_eq!(mark_text(&dir).as_deref(), Some("3"));
    end_session(ws, serving).await;
}

/// (m) Section 7.1 "Refollowing": with the head unknown, a `head_seq` below
/// the batch's own last `seq` cannot be the primary's newest, so the node
/// does not record it.
#[tokio::test]
async fn a_head_seq_below_the_batch_while_the_head_is_unknown_is_a_bad_message() {
    a_refused_head_seq(2).await;
}

/// (n) Section 7.1 "Refollowing": with the head unknown, a `head_seq` above
/// 2^53 - 1, which no change log reaches, is not recorded either.
#[tokio::test]
async fn a_head_seq_above_the_highest_seq_while_the_head_is_unknown_is_a_bad_message() {
    a_refused_head_seq(quoro_node_agent::follow::MAX_SEQ + 1).await;
}

// --- terms and voter support (section 8.8) ----------------------------------

/// `spec.votes` at voter-set version `version` (section 6.12).
fn votes(version: u64) -> SpecVotes {
    SpecVotes {
        version,
        port: 51821,
        voters: vec![SpecVoter {
            node_id: "nod_test".to_owned(),
            roles: vec!["node".to_owned()],
            mesh_address: Some("10.43.0.2".to_owned()),
        }],
    }
}

/// A snapshot that makes the machine a voter, with no signature check.
fn voter_snapshot(rev: u64, version: u64) -> Snapshot {
    let mut snapshot = snapshot(rev, Vec::new());
    snapshot.state.spec.votes = Some(votes(version));
    snapshot
}

/// A signed `desired_state` at `term`, a voter's when `votes` is set.
fn desired_frame_at(
    rev: u64,
    term: u64,
    votes: Option<SpecVotes>,
    key: &SigningKey,
    id: &str,
) -> String {
    let mut state = snapshot(rev, Vec::new()).state;
    state.term = term;
    state.spec.votes = votes;
    let mut body = state.to_unsigned_value();
    sign::sign_body(key, &mut body).unwrap();
    ControlMessage::DesiredState(body)
        .to_frame(id, None)
        .to_text()
}

fn support_frame(term: u64, primary: &str, support_ms: u64, max_lease_ms: u64, id: &str) -> String {
    ControlMessage::SupportRequest(SupportRequest {
        term,
        primary_node_id: primary.to_owned(),
        support_ms,
        max_lease_ms,
        voters_version: 3120,
    })
    .to_frame(id, None)
    .to_text()
}

fn welcome_frame(term: u64, re: &str) -> String {
    ControlMessage::Welcome(Welcome {
        cluster_id: "cls_test".to_owned(),
        term,
        server_time: "2026-11-02T10:15:30Z".to_owned(),
        heartbeat_interval_ms: 100,
        session_timeout_ms: 15_000,
        primary_node_id: "nod_primary".to_owned(),
        primary_urls: Vec::new(),
        schema_version: None,
    })
    .to_frame("msg_welcome", Some(re))
    .to_text()
}

/// The node of a term test: its session, store and cluster key, and the
/// sender of its observed VMs, which the test keeps alive for as long as the
/// session runs.
struct TermNode {
    session: Session,
    store: NodeStore,
    key: SigningKey,
    _observed: watch::Sender<Vec<quoro_proto::wire::VmStatus>>,
}

/// A session over `dir` whose newest snapshot is `current` (a voter's when it
/// carries `spec.votes`), measuring a promise on a manual clock on boot
/// `boot-a`.
async fn term_node(dir: &TempDir, current: Option<Snapshot>) -> TermNode {
    let key = sign::signing_key_from_seed(&[8; 32]);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, _desired_rx) = watch::channel(None);
    let (observed_tx, observed_rx) = watch::channel(Vec::new());
    let session = Session::new(SessionSetup {
        identity: identity_files(dir.path(), &key),
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current,
        builds: None,
    })
    .with_host_clock(Arc::new(ManualHostClock::new(
        Some("boot-a"),
        Duration::from_secs(1000),
    )));
    TermNode {
        session,
        store,
        key,
        _observed: observed_tx,
    }
}

/// A session whose `welcome` at `term` was accepted.
async fn welcomed(session: Session, term: u64) -> (WebSocketStream<DuplexStream>, Serving) {
    let (mut ws, hello, serving) = open_session(session).await;
    send(&mut ws, welcome_frame(term, hello["id"].as_str().unwrap())).await;
    read_until(&mut ws, "status_report").await;
    (ws, serving)
}

/// Asserts that `frame` is the fatal `stale_term` answering `re`, carrying
/// `term`.
fn assert_stale_term(frame: &Value, re: &str, term: u64) {
    assert_eq!(frame["type"], "error", "{frame}");
    assert_eq!(frame["re"], re, "{frame}");
    assert_eq!(frame["body"]["code"], "stale_term", "{frame}");
    assert_eq!(frame["body"]["fatal"], true, "{frame}");
    assert_eq!(frame["body"]["term"], term, "{frame}");
}

/// Sections 4.2 rule 2 and 8.8: a `welcome` below the highest term seen is
/// answered `stale_term`, fatal, carrying that term; the session ends and the
/// node redials, keeping its stored desired state. A `welcome` of that term
/// is then accepted.
#[tokio::test]
async fn a_welcome_with_a_lower_term_is_answered_stale_term_and_the_node_redials() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        key,
        _observed,
    } = term_node(&dir, None).await;
    let mut body = snapshot(2, Vec::new()).state.to_unsigned_value();
    sign::sign_body(&key, &mut body).unwrap();
    store.store_desired(2, &body).await.unwrap();
    store.raise_term(3).await.unwrap();

    let (mut ws, hello, serving) = open_session(session).await;
    send(&mut ws, welcome_frame(2, hello["id"].as_str().unwrap())).await;
    let frames = frames_until_closed(&mut ws).await;
    assert_eq!(frames.len(), 1, "only the error: {frames:?}");
    assert_stale_term(&frames[0], "msg_welcome", 3);
    let session = ended(serving).await;
    assert_eq!(store.highest_term().await.unwrap(), Some(3));
    assert_eq!(store.latest_desired().await.unwrap().unwrap().0, 2);

    let (ws, serving) = welcomed(session, 3).await;
    end_session(ws, serving).await;
}

/// Section 8.8: a higher term in a `welcome` is stored before the session
/// goes on, and the stored term never goes down.
#[tokio::test]
async fn a_welcome_with_a_higher_term_is_stored() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, None).await;
    assert_eq!(store.highest_term().await.unwrap(), None);
    let (ws, serving) = welcomed(session, 4).await;
    assert_eq!(store.highest_term().await.unwrap(), Some(4));
    end_session(ws, serving).await;
}

/// Sections 5.11 and 8.8: a `desired_state`'s term is checked after its
/// signature and before it is stored. A badly signed one is `bad_signature`
/// whatever its term; a lower term is `stale_term` and nothing is stored; a
/// higher one is stored with the snapshot.
#[tokio::test]
async fn a_desired_state_with_a_lower_term_is_refused_and_a_higher_one_adopted() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        key,
        _observed,
    } = term_node(&dir, None).await;
    let (mut ws, serving) = welcomed(session, 2).await;

    let other = sign::signing_key_from_seed(&[99; 32]);
    send(&mut ws, desired_frame_at(4, 1, None, &other, "msg_forged")).await;
    let forged = reply_to(&mut ws, "msg_forged").await;
    assert_eq!(forged["body"]["code"], "bad_signature", "{forged}");

    send(&mut ws, desired_frame_at(4, 1, None, &key, "msg_old")).await;
    let frames = frames_until_closed(&mut ws).await;
    let stale = frames
        .iter()
        .find(|frame| frame["re"] == "msg_old")
        .expect("an answer to the old snapshot");
    assert_stale_term(stale, "msg_old", 2);
    let session = ended(serving).await;
    assert_eq!(
        store.latest_desired().await.unwrap(),
        None,
        "nothing stored"
    );

    let (mut ws, serving) = welcomed(session, 2).await;
    send(&mut ws, desired_frame_at(5, 3, None, &key, "msg_new")).await;
    let stored = reply_to(&mut ws, "msg_new").await;
    assert_eq!(stored["body"]["status"], "ok", "{stored}");
    assert_eq!(store.highest_term().await.unwrap(), Some(3));
    assert_eq!(store.latest_desired().await.unwrap().unwrap().0, 5);
    end_session(ws, serving).await;
}

/// Sections 5.27 and 8.8: a `support_request` below the highest term seen,
/// learned here after the session began, is answered `stale_term` and ends
/// the session.
#[tokio::test]
async fn a_support_request_with_a_lower_term_ends_the_session() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    let (mut ws, serving) = welcomed(session, 2).await;
    store.raise_term(3).await.unwrap();

    send(
        &mut ws,
        support_frame(2, "nod_primary", 15_000, 15_000, "msg_support"),
    )
    .await;
    let frames = frames_until_closed(&mut ws).await;
    let stale = frames
        .iter()
        .find(|frame| frame["re"] == "msg_support")
        .expect("an answer to the request");
    assert_stale_term(stale, "msg_support", 3);
    assert!(frames.iter().all(|frame| frame["type"] != "support_grant"));
    ended(serving).await;
    assert_eq!(store.support().await.unwrap(), None);
}

/// Sections 5.28 and 8.8: a voter answers `support_grant` echoing the
/// request, with the voter-set version of its desired state, and its store
/// then holds the promise and lease horizon the grant stands for. That the
/// store is written before the grant is sent is what
/// [`a_promise_the_store_cannot_keep_sends_no_grant`] shows: a write that
/// fails sends no grant. A later request with a shorter `L` never lowers the
/// horizon.
#[tokio::test]
async fn a_voter_grants_support_once_its_promise_is_stored() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    let (mut ws, serving) = welcomed(session, 1).await;

    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_1"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_1").await;
    // The store holds the promise once the grant has arrived.
    let first = store
        .support()
        .await
        .unwrap()
        .expect("the promise is stored");
    assert_eq!(grant["type"], "support_grant", "{grant}");
    assert_eq!(
        grant["body"],
        json!({"term": 1, "support_ms": 15000, "max_lease_ms": 15000, "voters_version": 3120})
    );
    assert_eq!(first.boot_id, "boot-a");
    assert_eq!(first.primary_node_id, "nod_primary");
    let v_recv = Duration::from_nanos(first.received_ns);
    assert!(v_recv >= Duration::from_secs(1000), "{v_recv:?}");
    assert_eq!(first.promise(), v_recv + Duration::from_secs(15));
    assert_eq!(first.horizon(), v_recv + Duration::from_secs(15 + 15 + 3));

    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 5_000, "msg_support_2"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_2").await;
    assert_eq!(grant["body"]["max_lease_ms"], 5000, "{grant}");
    let second = store.support().await.unwrap().unwrap();
    assert_eq!(
        second.horizon(),
        first.horizon(),
        "the horizon is not lowered"
    );
    assert!(second.promise() >= first.promise());
    assert_eq!(second.max_lease_ms, 5_000);
    end_session(ws, serving).await;
}

/// Sections 5.28 and 8.8: the promise is stored before the grant is sent, so
/// a promise that cannot be stored is not granted. The session goes on, and
/// the next request is granted once the store takes writes.
#[tokio::test]
async fn a_promise_the_store_cannot_keep_sends_no_grant() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    sqlx::query(
        "CREATE TRIGGER no_support BEFORE INSERT ON node_meta WHEN NEW.key = 'support' \
         BEGIN SELECT RAISE(ABORT, 'x'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();

    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_1"),
    )
    .await;
    let frames = frames_before_pong(&mut ws, "msg_ping_1").await;
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_support_1"),
        "no answer: {frames:?}"
    );
    assert_eq!(store.support().await.unwrap(), None);

    sqlx::query("DROP TRIGGER no_support")
        .execute(store.pool())
        .await
        .unwrap();
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_2"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_2").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    end_session(ws, serving).await;
}

/// Sections 5.27 and 6.12: a machine that is not a voter ignores
/// `support_request`, and one whose desired state loses `spec.votes` stops
/// answering and keeps its vote state.
#[tokio::test]
async fn a_machine_that_is_not_a_voter_ignores_support_request() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        key,
        _observed,
    } = term_node(&dir, Some(snapshot(1, Vec::new()))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_1"),
    )
    .await;
    let frames = frames_before_pong(&mut ws, "msg_ping_1").await;
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_support_1"),
        "no answer, no error: {frames:?}"
    );
    assert_eq!(store.support().await.unwrap(), None);

    // A voter's snapshot makes it answer.
    send(
        &mut ws,
        desired_frame_at(2, 1, Some(votes(7)), &key, "msg_voter"),
    )
    .await;
    reply_to(&mut ws, "msg_voter").await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_2"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_2").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    assert_eq!(grant["body"]["voters_version"], 7);
    let kept = store.support().await.unwrap().expect("stored");

    // Losing `spec.votes` stops the answers and keeps the vote state.
    send(&mut ws, desired_frame_at(3, 1, None, &key, "msg_not_voter")).await;
    reply_to(&mut ws, "msg_not_voter").await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_3"),
    )
    .await;
    let frames = frames_before_pong(&mut ws, "msg_ping_2").await;
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_support_3"),
        "no answer once not a voter: {frames:?}"
    );
    assert_eq!(store.support().await.unwrap(), Some(kept));
    end_session(ws, serving).await;
}

/// Section 5.27: `support_ms` below the 3 000 ms floor is `bad_message`,
/// non-fatal, and promises nothing.
#[tokio::test]
async fn a_support_window_below_the_floor_is_a_bad_message() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 2_999, 15_000, "msg_support"),
    )
    .await;
    let refused = reply_to(&mut ws, "msg_support").await;
    assert_eq!(refused["type"], "error", "{refused}");
    assert_eq!(refused["body"]["code"], "bad_message");
    assert_eq!(refused["body"]["fatal"], false);
    assert!(refused["body"].get("term").is_none(), "{refused}");
    assert_eq!(store.support().await.unwrap(), None);
    // The session goes on.
    frames_before_pong(&mut ws, "msg_ping").await;
    end_session(ws, serving).await;
}

/// Sections 5.27 and 8.8 (ADR 0280): a voter that voted in this term for
/// another candidate answers `bad_message`, non-fatal, carrying its term, and
/// promises nothing.
#[tokio::test]
async fn a_vote_in_this_term_for_another_candidate_refuses_support() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    store
        .set_meta(VOTE, r#"{"term":2,"candidate":"nod_other"}"#)
        .await
        .unwrap();
    let (mut ws, serving) = welcomed(session, 2).await;
    send(
        &mut ws,
        support_frame(2, "nod_primary", 15_000, 15_000, "msg_support"),
    )
    .await;
    let refused = reply_to(&mut ws, "msg_support").await;
    assert_eq!(refused["type"], "error", "{refused}");
    assert_eq!(refused["body"]["code"], "bad_message");
    assert_eq!(refused["body"]["fatal"], false);
    assert_eq!(refused["body"]["term"], 2);
    assert_eq!(store.support().await.unwrap(), None);

    // The candidate it voted for is supported.
    send(
        &mut ws,
        support_frame(2, "nod_other", 15_000, 15_000, "msg_support_other"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_other").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    end_session(ws, serving).await;
}

/// Section 8.8 "Terms": in a term it did not vote in, a voter supports only
/// the primary of its session. A `support_request` naming another primary
/// than the session's `welcome` is `bad_message`, non-fatal, without a term,
/// and promises nothing; the session goes on.
#[tokio::test]
async fn a_support_request_for_another_primary_than_the_session_s_is_refused() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(voter_snapshot(1, 3120))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    send(
        &mut ws,
        support_frame(1, "nod_impostor", 15_000, 15_000, "msg_support"),
    )
    .await;
    let refused = reply_to(&mut ws, "msg_support").await;
    assert_eq!(refused["type"], "error", "{refused}");
    assert_eq!(refused["body"]["code"], "bad_message");
    assert_eq!(refused["body"]["fatal"], false);
    assert!(refused["body"].get("term").is_none(), "{refused}");
    assert_eq!(store.support().await.unwrap(), None);

    // The session's own primary is still supported.
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_own"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_own").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    end_session(ws, serving).await;
}

/// The `node` role, as a machine that enrolled with it runs.
fn node_roles() -> Vec<String> {
    vec!["node".to_owned()]
}

/// The public half of the custody key `custody.key` holds, read straight
/// from the disk.
fn custody_key_on_disk(dir: &TempDir) -> String {
    let bytes = std::fs::read(dir.path().join(CUSTODY_KEY_FILE)).expect("custody.key exists");
    CustodyKey::from_bytes(&bytes)
        .expect("custody.key is 32 bytes")
        .public_key()
        .to_base64()
}

/// Section 7.2: a machine with the `node` role generates its custody key
/// before its first `hello`, keeps it `0600` next to its credential, and
/// reports its public half as standard base64 of 32 bytes. After an agent
/// restart, a new session over the same data directory reports the same
/// key and leaves the file as it was.
#[tokio::test]
async fn a_node_s_hello_carries_its_custody_key_and_a_restart_keeps_it() {
    use base64::Engine as _;

    let dir = TempDir::new();
    let path = dir.path().join(CUSTODY_KEY_FILE);
    assert!(!path.exists());
    let (setup, _observed) = follow_node(&dir).await;
    let (ws, hello, serving) = open_session(Session::new(setup).with_roles(&node_roles())).await;
    let key = hello["body"]["custody_public_key"]
        .as_str()
        .unwrap_or_else(|| panic!("a node's hello carries a custody key: {hello}"))
        .to_owned();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&key)
        .expect("standard base64");
    assert_eq!(raw.len(), 32);
    assert_eq!(
        custody_key_on_disk(&dir),
        key,
        "the key sent is the one kept"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "custody.key is root-only");
    }
    let bytes = std::fs::read(&path).unwrap();
    drop(end_session(ws, serving).await);

    // The agent restarts: a new store handle and a new session, with a new
    // boot id, over the same data directory.
    let (setup, _observed_again) = follow_node(&dir).await;
    let (ws, hello_again, serving) =
        open_session(Session::new(setup).with_roles(&node_roles())).await;
    assert_ne!(hello_again["body"]["boot_id"], hello["body"]["boot_id"]);
    assert_eq!(hello_again["body"]["custody_public_key"], key.as_str());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "custody.key is not rewritten"
    );
    end_session(ws, serving).await;
}

/// Section 7.2: an edge-only or witness-only machine keeps no custody key.
/// Its `hello` has no `custody_public_key` and no `custody.key` is written,
/// while an edge that also has the `node` role keeps one.
#[tokio::test]
async fn only_a_machine_with_the_node_role_keeps_and_reports_a_custody_key() {
    for (roles, keeps) in [
        (vec!["edge"], false),
        (vec!["witness"], false),
        (vec!["edge", "witness"], false),
        (vec!["edge", "node"], true),
    ] {
        let roles: Vec<String> = roles.into_iter().map(str::to_owned).collect();
        let dir = TempDir::new();
        let (setup, _observed) = follow_node(&dir).await;
        let (ws, hello, serving) = open_session(Session::new(setup).with_roles(&roles)).await;
        assert_eq!(
            hello["body"].get("custody_public_key").is_some(),
            keeps,
            "{roles:?}: {hello}"
        );
        assert_eq!(
            dir.path().join(CUSTODY_KEY_FILE).exists(),
            keeps,
            "{roles:?}"
        );
        end_session(ws, serving).await;
    }
}

/// Section 7.2, ADR 0231: the primary binds the first custody key it sees,
/// so a `custody.key` that cannot be read or is not 32 bytes is never
/// replaced or overwritten: it is kept byte for byte. The custody key guards
/// no lease, so the node still connects: `hello` goes out without
/// `custody_public_key` and the session runs, on this session and the next.
#[tokio::test]
async fn a_malformed_custody_key_is_kept_and_hello_goes_out_without_it() {
    for contents in [
        Some(vec![0x42; 31]),
        Some(vec![0x42; 33]),
        Some(Vec::new()),
        None,
    ] {
        let dir = TempDir::new();
        let (setup, _observed) = follow_node(&dir).await;
        let path = dir.path().join(CUSTODY_KEY_FILE);
        match &contents {
            Some(bytes) => std::fs::write(&path, bytes).unwrap(),
            // A directory where the file goes cannot be read as one.
            None => std::fs::create_dir(&path).unwrap(),
        }
        let mut session = Session::new(setup).with_roles(&node_roles());
        for _ in 0..2 {
            let (mut ws, hello, serving) = open_session(session).await;
            assert_eq!(hello["body"]["node_id"], "nod_test", "{hello}");
            assert!(
                hello["body"].get("custody_public_key").is_none(),
                "no custody key ({contents:?}): {hello}"
            );
            match &contents {
                Some(bytes) => assert_eq!(&std::fs::read(&path).unwrap(), bytes),
                None => assert!(path.is_dir()),
            }
            // The session goes on past `hello`.
            welcome(&mut ws, &hello).await;
            session = end_session(ws, serving).await;
        }
        drop(session);
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("custody"))
            .collect();
        assert_eq!(names, vec![CUSTODY_KEY_FILE.to_owned()], "no new key file");
    }
}

// --- the witness role (sections 6.12 and 8.8, ADR 0170) ---------------------

/// A machine without the `node` role: no `spec.network`, no VM; an edge
/// when `edge` is set, and a voter with the `witness` role when `version`
/// is given.
fn roleless_state(rev: u64, edge: bool, version: Option<u64>) -> DesiredState {
    let mut state = snapshot(rev, Vec::new()).state;
    state.spec.network = None;
    state.spec.edge = edge.then(SpecEdge::default);
    state.spec.witness = version.map(|_| SpecWitness {});
    state.spec.votes = version.map(votes);
    state
}

/// The same, as a snapshot the session starts with.
fn roleless_snapshot(rev: u64, edge: bool, version: Option<u64>) -> Snapshot {
    let mut snapshot = snapshot(rev, Vec::new());
    snapshot.state = roleless_state(rev, edge, version);
    snapshot
}

/// The same, as a signed `desired_state` frame.
fn roleless_frame(
    rev: u64,
    edge: bool,
    version: Option<u64>,
    key: &SigningKey,
    id: &str,
) -> String {
    let mut body = roleless_state(rev, edge, version).to_unsigned_value();
    sign::sign_body(key, &mut body).unwrap();
    ControlMessage::DesiredState(body)
        .to_frame(id, None)
        .to_text()
}

/// Section 6.12: a witness-only machine (enrolled with `roles:
/// ["witness"]`) has no `spec.network` and no VM, and answers
/// `support_request` at the version of its `spec.votes`.
#[tokio::test]
async fn a_witness_only_machine_answers_support_request() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        _observed,
        ..
    } = term_node(&dir, Some(roleless_snapshot(1, false, Some(41)))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    assert_eq!(grant["body"]["voters_version"], 41);
    assert!(
        store.support().await.unwrap().is_some(),
        "the promise is stored"
    );
    end_session(ws, serving).await;
}

/// Sections 6.12 and 8.8: an edge-only machine ignores `support_request`.
/// Once it gains the witness role, its next `desired_state` carries
/// `spec.witness` and `spec.votes`, and it answers on the same session,
/// without a restart. Losing the role stops the answers again.
#[tokio::test]
async fn an_edge_that_gains_the_witness_role_votes_on_its_next_desired_state() {
    let dir = TempDir::new();
    let TermNode {
        session,
        store,
        key,
        _observed,
    } = term_node(&dir, Some(roleless_snapshot(1, true, None))).await;
    let (mut ws, serving) = welcomed(session, 1).await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_1"),
    )
    .await;
    let frames = frames_before_pong(&mut ws, "msg_ping_1").await;
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_support_1"),
        "an edge does not vote: {frames:?}"
    );
    assert_eq!(store.support().await.unwrap(), None);

    send(
        &mut ws,
        roleless_frame(2, true, Some(52), &key, "msg_witness"),
    )
    .await;
    let stored = reply_to(&mut ws, "msg_witness").await;
    assert_eq!(stored["body"]["status"], "ok", "{stored}");
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_2"),
    )
    .await;
    let grant = reply_to(&mut ws, "msg_support_2").await;
    assert_eq!(grant["type"], "support_grant", "{grant}");
    assert_eq!(grant["body"]["voters_version"], 52);

    send(&mut ws, roleless_frame(3, true, None, &key, "msg_edge")).await;
    reply_to(&mut ws, "msg_edge").await;
    send(
        &mut ws,
        support_frame(1, "nod_primary", 15_000, 15_000, "msg_support_3"),
    )
    .await;
    let frames = frames_before_pong(&mut ws, "msg_ping_2").await;
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_support_3"),
        "no answer once the role is gone: {frames:?}"
    );
    end_session(ws, serving).await;
}

// --- `welcome.schema_version` and the promotion freeze (sections 5.10, 7.3) --

/// A `welcome` answering `re` that carries `schema_version`, or none.
fn welcome_with_schema(re: &str, schema_version: Option<u32>) -> String {
    ControlMessage::Welcome(Welcome {
        cluster_id: "cls_test".to_owned(),
        term: 1,
        server_time: "2026-11-02T10:15:30Z".to_owned(),
        heartbeat_interval_ms: 100,
        session_timeout_ms: 15_000,
        primary_node_id: "nod_primary".to_owned(),
        primary_urls: Vec::new(),
        schema_version,
    })
    .to_frame("msg_welcome", Some(re))
    .to_text()
}

/// Section 5.10 (ADR 0229): the node keeps the newest
/// `welcome.schema_version` it saw. A lower one does not lower it, and a
/// `welcome` without the field keeps what is stored.
#[tokio::test]
async fn the_newest_welcome_schema_version_is_kept_and_never_lowered() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let mut session = Session::new(setup);
    for (sent, kept) in [
        (None, None),
        (Some(14), Some(14)),
        (Some(12), Some(14)),
        (None, Some(14)),
        (Some(15), Some(15)),
    ] {
        let (mut ws, hello, serving) = open_session(session).await;
        send(
            &mut ws,
            welcome_with_schema(hello["id"].as_str().unwrap(), sent),
        )
        .await;
        read_until(&mut ws, "status_report").await;
        assert_eq!(
            store.welcome_schema_version().await.unwrap(),
            kept,
            "after a welcome with {sent:?}"
        );
        session = end_session(ws, serving).await;
    }
}

/// Every frame the node sends until its session ends, which must happen
/// within `limit`.
async fn frames_until_closed_within(
    ws: &mut WebSocketStream<DuplexStream>,
    limit: Duration,
) -> Vec<Value> {
    tokio::time::timeout(limit, frames_until_closed(ws))
        .await
        .unwrap_or_else(|_| panic!("the session did not close within {limit:?}"))
}

/// Asserts that no frame in `frames` is a `hello` or a `log_ack`.
fn assert_no_hello_or_ack(frames: &[Value]) {
    assert!(
        frames
            .iter()
            .all(|frame| frame["type"] != "hello" && frame["type"] != "log_ack"),
        "no hello or log_ack after the freeze: {frames:?}"
    );
}

/// The session's end and the session, once the socket closed.
async fn ended_with(serving: Serving) -> (Session, SessionEnd) {
    let (session, end) = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("the session ended")
        .unwrap();
    (session, end.unwrap())
}

/// Section 7.3 "The freeze" (ADR 0261): a freeze set from another process,
/// here its own connection to `node.db`, while a session is up ends the
/// session within 2 s even though nothing arrives; nothing after the freeze
/// is a `hello` or a `log_ack`. While the flag is set a new session sends no
/// `hello` at all; once it ends, `hello` reports the log as it stood.
#[tokio::test]
async fn a_freeze_from_another_process_ends_the_session_within_2_s() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    let entries = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(entries.clone(), Some(3), "msg_log_1"),
    )
    .await;
    assert_eq!(reply_to(&mut ws, "msg_log_1").await["type"], "log_ack");

    freeze::freeze(dir.path()).await.unwrap();
    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert_no_hello_or_ack(&frames);
    let (session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);
    assert_eq!(stored_log(&store).await, entries);

    // A session that starts while the flag is set sends nothing.
    let (client, server) = tokio::io::duplex(1 << 16);
    let accept = tokio::spawn(tokio_tungstenite::accept_async(server));
    let (client_ws, _response) = client_async("ws://primary.test/api/v1/nodes/session", client)
        .await
        .unwrap();
    let mut ws = accept.await.unwrap().unwrap();
    let mut session = session;
    let serving = tokio::spawn(async move {
        let end = session.serve(client_ws, std::future::pending::<()>()).await;
        (session, end)
    });
    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert!(frames.is_empty(), "no hello while frozen: {frames:?}");
    let (session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);

    freeze::unfreeze(dir.path()).await.unwrap();
    let (ws, hello, serving) = follow_session(session).await;
    assert_eq!(hello["body"]["log_seq"], 3);
    assert_eq!(hello["body"]["log_hash"], entries[2]["hash"]);
    end_session(ws, serving).await;
}

/// Section 7.3 "The freeze" (ADR 0261): a `log_entries` delivered after the
/// freeze committed is not stored and not acknowledged, and the log's newest
/// `seq` is unchanged; the session ends without answering it.
#[tokio::test]
async fn log_entries_after_the_freeze_are_neither_stored_nor_acknowledged() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let (mut ws, _hello, serving) = follow_session(Session::new(setup)).await;
    let first = log_chain(1, ZEROS, 3, "a");
    send(
        &mut ws,
        log_entries_frame(first.clone(), Some(5), "msg_log_1"),
    )
    .await;
    assert_eq!(reply_to(&mut ws, "msg_log_1").await["type"], "log_ack");

    freeze::freeze(dir.path()).await.unwrap();
    let second = log_chain(4, first[2]["hash"].as_str().unwrap(), 2, "a");
    send(&mut ws, log_entries_frame(second, Some(5), "msg_log_2")).await;
    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert_no_hello_or_ack(&frames);
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_log_2"),
        "no answer: {frames:?}"
    );
    let (_session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);
    assert_eq!(
        store.log_position().await.unwrap(),
        (3, first[2]["hash"].as_str().unwrap().to_owned())
    );
    assert_eq!(stored_log(&store).await, first);
}

/// What the pinned TLS primary of the dial tests saw, in order.
#[derive(Debug)]
enum Seen {
    /// A TCP connection was accepted: the node dialed.
    Dial,
    /// A text frame from the node.
    Frame(Value),
    /// A connection ended.
    Closed,
}

/// A pinned TLS primary on `127.0.0.1` that answers `hello` with `welcome`
/// and `ping` with `pong`, and reports every dial, frame and closed
/// connection. Returns its URL, its certificate's pin and the reports.
async fn counting_primary() -> (
    String,
    String,
    mpsc::UnboundedReceiver<Seen>,
    tokio::task::JoinHandle<()>,
) {
    let (cert, key) = self_signed();
    let pin = tls::cert_fingerprint(cert.as_ref());
    let acceptor = TlsAcceptor::from(server_config(cert, key));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let _ = seen_tx.send(Seen::Dial);
            let acceptor = acceptor.clone();
            let seen = seen_tx.clone();
            tokio::spawn(async move {
                if let Ok(stream) = acceptor.accept(tcp).await
                    && let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await
                {
                    while let Some(Ok(message)) = ws.next().await {
                        let Message::Text(text) = message else {
                            continue;
                        };
                        let Ok(frame) = serde_json::from_str::<Value>(text.as_str()) else {
                            continue;
                        };
                        let id = frame["id"].as_str().unwrap_or_default().to_owned();
                        let reply = match frame["type"].as_str() {
                            Some("hello") => Some(welcome_frame(1, &id)),
                            Some("ping") => Some(
                                ControlMessage::Pong
                                    .to_frame("msg_pong", Some(&id))
                                    .to_text(),
                            ),
                            _ => None,
                        };
                        let _ = seen.send(Seen::Frame(frame));
                        if let Some(reply) = reply
                            && ws.send(Message::Text(reply.into())).await.is_err()
                        {
                            break;
                        }
                    }
                }
                let _ = seen.send(Seen::Closed);
            });
        }
    });
    (url, pin, seen_rx, task)
}

/// The next report of the primary, which must come within `limit`.
async fn next_seen(seen: &mut mpsc::UnboundedReceiver<Seen>, limit: Duration) -> Seen {
    tokio::time::timeout(limit, seen.recv())
        .await
        .unwrap_or_else(|_| panic!("the primary saw nothing within {limit:?}"))
        .expect("the primary runs")
}

/// Waits until the primary sees a frame of type `kind`, within `limit`.
async fn seen_frame(seen: &mut mpsc::UnboundedReceiver<Seen>, kind: &str, limit: Duration) {
    let deadline = Instant::now() + limit;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if let Seen::Frame(frame) = next_seen(seen, left).await
            && frame["type"] == kind
        {
            return;
        }
    }
}

/// Asserts that the primary sees nothing at all for `quiet`: no dial, no
/// frame.
async fn nothing_seen_for(seen: &mut mpsc::UnboundedReceiver<Seen>, quiet: Duration) {
    if let Ok(report) = tokio::time::timeout(quiet, seen.recv()).await {
        panic!("the primary saw {report:?} while the agent was frozen");
    }
}

/// The dialing agent of a freeze test: a session over `dir` pinned to the
/// primary at `url`, with `advertised` as the primary's other addresses, run
/// until `stop`, and the store it holds.
async fn dialing_agent(
    dir: &TempDir,
    url: &str,
    pin: &str,
    advertised: &[String],
) -> (
    NodeStore,
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), quoro_node_agent::AgentError>>,
) {
    let key = sign::signing_key_from_seed(&[8; 32]);
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (desired, _desired_rx) = watch::channel(None);
    let (observed_tx, observed_rx) = watch::channel(Vec::new());
    let identity = identity_files_pinned(dir.path(), &key, url, pin);
    identity.set_primary_urls(advertised).unwrap();
    let mut session = Session::new(SessionSetup {
        identity,
        store: store.clone(),
        facts: facts(),
        desired,
        observed: observed_rx,
        current: None,
        builds: None,
    });
    let (stop_tx, stop_rx) = watch::channel(false);
    let running = tokio::spawn(async move {
        let _observed = observed_tx;
        session.run(stop_rx).await
    });
    (store, stop_tx, running)
}

/// Section 7.3 "The freeze" and section 4.4 (ADR 0261, ADR 0280): a freeze
/// from another process ends the running session within 2 s with no `hello`
/// or `log_ack` after it, and the primary sees no new dial for 3 s. Once the
/// freeze ends, the agent dials again and sends `hello`.
#[tokio::test]
async fn a_frozen_agent_does_not_redial_until_the_freeze_ends() {
    let dir = TempDir::new();
    let (url, pin, mut seen, primary) = counting_primary().await;
    let (store, stop, running) = dialing_agent(&dir, &url, &pin, &[]).await;
    assert!(matches!(
        next_seen(&mut seen, Duration::from_secs(5)).await,
        Seen::Dial
    ));
    seen_frame(&mut seen, "status_report", Duration::from_secs(5)).await;

    freeze::freeze(dir.path()).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, seen.recv()).await {
            Ok(Some(Seen::Closed)) => break,
            Ok(Some(Seen::Frame(frame))) => assert_no_hello_or_ack(&[frame]),
            Ok(Some(Seen::Dial)) => panic!("a dial while frozen"),
            Ok(None) | Err(_) => panic!("the session did not close within 2 s of the freeze"),
        }
    }
    nothing_seen_for(&mut seen, Duration::from_secs(3)).await;
    assert!(freeze::is_frozen(&store).await.unwrap());

    freeze::unfreeze(dir.path()).await.unwrap();
    assert!(matches!(
        next_seen(&mut seen, Duration::from_secs(3)).await,
        Seen::Dial
    ));
    seen_frame(&mut seen, "hello", Duration::from_secs(5)).await;

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    primary.abort();
}

/// Section 7.3 "The freeze", section 12 `node_meta`: the freeze is in the
/// agent's store, so an agent that starts with it set (a restart) never
/// dials until it ends, and then dials at once.
#[tokio::test]
async fn an_agent_started_frozen_never_dials_until_the_freeze_ends() {
    let dir = TempDir::new();
    let (url, pin, mut seen, primary) = counting_primary().await;
    // The agent that ran before the restart left its store and a freeze.
    drop(NodeStore::open(dir.path()).await.unwrap());
    freeze::freeze(dir.path()).await.unwrap();

    let (_store, stop, running) = dialing_agent(&dir, &url, &pin, &[]).await;
    nothing_seen_for(&mut seen, Duration::from_secs(3)).await;

    freeze::unfreeze(dir.path()).await.unwrap();
    assert!(matches!(
        next_seen(&mut seen, Duration::from_secs(3)).await,
        Seen::Dial
    ));
    seen_frame(&mut seen, "hello", Duration::from_secs(5)).await;

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    primary.abort();
}

/// Waits until `done` holds, checking every 10 ms, and panics with `what`
/// after `limit`.
async fn wait_until(done: impl Fn() -> bool, limit: Duration, what: &str) {
    let deadline = Instant::now() + limit;
    while !done() {
        assert!(Instant::now() < deadline, "{what} within {limit:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Section 7.3 "The freeze" and section 4.4: a dial attempt under way when
/// the freeze commits ends. Here `primary_url` accepts the TCP connection of
/// the first dial and never answers its TLS handshake, as a hung primary
/// does, so that attempt would wait the full connect timeout, 10 s, and then
/// try the advertised address, which accepts and never answers. The freeze
/// commits only once that first connection was accepted, so a dial is under
/// way: the agent closes it within the dial loop's check of the flag, about
/// a second, and dials nowhere while the freeze lasts. Once it ends, the
/// agent dials again. `primary_url` now drops each connection, so the
/// handshake fails at once; unlike a refused connection, `dial_known` does
/// not dial it again, and the attempt reaches the advertised address.
#[tokio::test]
async fn a_freeze_ends_a_dial_attempt_under_way() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt as _;

    let dir = TempDir::new();
    let hung = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let hung_url = format!("https://127.0.0.1:{}", hung.local_addr().unwrap().port());
    let hung_accepts = Arc::new(AtomicUsize::new(0));
    let first_closed = Arc::new(AtomicBool::new(false));
    let hung_listener = {
        let accepts = Arc::clone(&hung_accepts);
        let closed = Arc::clone(&first_closed);
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = hung.accept().await {
                if accepts.fetch_add(1, Ordering::SeqCst) > 0 {
                    // Every later connection is dropped at once.
                    continue;
                }
                // The first is held, unanswered, until the agent closes it.
                let closed = Arc::clone(&closed);
                tokio::spawn(async move {
                    let mut buf = [0_u8; 1024];
                    while matches!(tcp.read(&mut buf).await, Ok(read) if read > 0) {}
                    closed.store(true, Ordering::SeqCst);
                });
            }
        })
    };
    let silent = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let silent_url = format!("https://127.0.0.1:{}", silent.local_addr().unwrap().port());
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    let listener = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((tcp, _)) = silent.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(tcp);
        }
    });
    let (_store, stop, running) =
        dialing_agent(&dir, &hung_url, &"0".repeat(64), &[silent_url]).await;

    wait_until(
        || hung_accepts.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
        "the agent dials primary_url",
    )
    .await;
    freeze::freeze(dir.path()).await.unwrap();
    wait_until(
        || first_closed.load(Ordering::SeqCst),
        Duration::from_secs(3),
        "the dial under way ends after the freeze",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "the attempt under way went on to the advertised address while frozen"
    );
    assert_eq!(
        hung_accepts.load(Ordering::SeqCst),
        1,
        "no new dial while frozen"
    );

    freeze::unfreeze(dir.path()).await.unwrap();
    wait_until(
        || accepted.load(Ordering::SeqCst) > 0,
        Duration::from_secs(6),
        "a dial reaches the advertised address after the freeze ended",
    )
    .await;

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    hung_listener.abort();
    listener.abort();
}

/// Section 7.3 "The freeze" (ADR 0261): a `log_diverged` handled after a
/// freeze committed changes nothing. The freeze is set over another
/// connection between `hello` and `welcome`, and the error follows
/// `welcome` at once, so the session handles it before its own timer would
/// see the flag. The session ends with no `hello` or `log_ack`, and the
/// mark, the refollowing state and the log's newest `seq` and hash stay as
/// they were, with no quarantined copy.
#[tokio::test]
async fn log_diverged_after_the_freeze_changes_nothing_and_ends_the_session() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let entries = log_chain(1, ZEROS, 3, "a");
    seed_log(&store, &entries).await;
    seed_mark(&dir, "3");
    let (mut ws, hello, serving) = open_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 3);

    freeze::freeze(dir.path()).await.unwrap();
    send(&mut ws, welcome_frame(1, hello["id"].as_str().unwrap())).await;
    send(&mut ws, diverged_frame("msg_diverged")).await;
    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert_no_hello_or_ack(&frames);
    let (_session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);
    assert_eq!(mark_text(&dir).as_deref(), Some("3"));
    assert_eq!(store.refollowing().await.unwrap(), None);
    assert_eq!(
        store.log_position().await.unwrap(),
        (3, entries[2]["hash"].as_str().unwrap().to_owned())
    );
    assert_eq!(stored_log(&store).await, entries);
    assert_eq!(quarantined(&store).await, 0);
    assert!(!dir.path().join("quarantine").exists());
}

/// Waits until the session holds a second connection of `store`'s pool,
/// besides the one the test holds with SQLite's write lock, for 200 ms in a
/// row: a transaction waiting on that lock. In WAL mode a read never waits
/// on it, and while the session's loop handles one message it does nothing
/// else, so on the `log_entries` path that is the transaction that would
/// record the head. It panics after 3 s, well inside the store's 5 s busy
/// timeout.
async fn waiting_on_the_write_lock(store: &NodeStore) {
    let pool = store.pool();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut since: Option<Instant> = None;
    loop {
        let in_use = usize::try_from(pool.size())
            .unwrap()
            .saturating_sub(pool.num_idle());
        if in_use >= 2 {
            if since.get_or_insert_with(Instant::now).elapsed() >= Duration::from_millis(200) {
                return;
            }
        } else {
            since = None;
        }
        assert!(
            Instant::now() < deadline,
            "the session never waited on the write lock"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Section 7.3 "The freeze", section 7.1 "Refollowing" (ADR 0261, ADR
/// 0280): a frozen agent refollowing with the head unknown that receives
/// `log_entries` records no head, leaves the mark at 2^53 - 1, stores
/// nothing and answers nothing. The test holds SQLite's write lock with the
/// freeze written but not committed, so the session passes its first read
/// of the flag and waits in the transaction that would record the head. The
/// freeze commits only once the session is seen waiting there
/// ([`waiting_on_the_write_lock`]), and that transaction reads it.
#[tokio::test]
async fn a_frozen_agent_refollowing_with_the_head_unknown_records_no_head() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    assert_eq!(
        store
            .set_refollowing(Refollowing::HeadUnknown)
            .await
            .unwrap(),
        FreezeChecked::Written
    );
    seed_mark(&dir, MAX_SEQ_TEXT);
    let (mut ws, hello, serving) = follow_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 0);

    let mut freezing = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("INSERT INTO node_meta (key, value) VALUES (?, ?)")
        .bind(PROMOTION_FREEZE)
        .bind("2026-11-10T00:00:00Z")
        .execute(&mut *freezing)
        .await
        .unwrap();
    send(
        &mut ws,
        log_entries_frame(log_chain(1, ZEROS, 2, "new"), Some(5), "msg_log_1"),
    )
    .await;
    waiting_on_the_write_lock(&store).await;
    freezing.commit().await.unwrap();

    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert_no_hello_or_ack(&frames);
    assert!(
        frames.iter().all(|frame| frame["re"] != "msg_log_1"),
        "no answer: {frames:?}"
    );
    let (_session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert!(stored_log(&store).await.is_empty());
    assert_eq!(store.log_head().await.unwrap(), None);
}

/// Section 7.3 "The freeze", section 7.1 "Refollowing" (ADR 0261): a freeze
/// that commits after a quarantine's mark and state and before its copy
/// moves ends the session as `Frozen`, not `Reconnect`, so the agent dials
/// again within about a second of the freeze's end, with no backoff. A
/// trigger on the test's store sets the freeze in the transaction that
/// records "head unknown", so it commits with that state, before the move
/// reads it. The copy stays where it was, read as quarantined, with no
/// quarantined copy recorded and no file left.
#[tokio::test]
async fn a_freeze_that_stops_the_move_of_a_diverged_log_ends_the_session_as_frozen() {
    let dir = TempDir::new();
    let (setup, _observed) = follow_node(&dir).await;
    let store = setup.store.clone();
    let entries = log_chain(1, ZEROS, 3, "a");
    seed_log(&store, &entries).await;
    seed_mark(&dir, "3");
    let (mut ws, hello, serving) = follow_session(Session::new(setup)).await;
    assert_eq!(hello["body"]["log_seq"], 3);
    assert_eq!(store.refollowing().await.unwrap(), None);

    sqlx::query(&format!(
        "CREATE TRIGGER freeze_after_the_state AFTER INSERT ON node_meta \
         WHEN NEW.key = 'log_refollowing' BEGIN \
         INSERT INTO node_meta (key, value) VALUES ('{PROMOTION_FREEZE}', '2026-11-10T00:00:00Z'); \
         END;"
    ))
    .execute(store.pool())
    .await
    .unwrap();
    send(&mut ws, diverged_frame("msg_diverged")).await;
    let frames = frames_until_closed_within(&mut ws, Duration::from_secs(2)).await;
    assert_no_hello_or_ack(&frames);
    let (_session, end) = ended_with(serving).await;
    assert_eq!(end, SessionEnd::Frozen);
    assert!(freeze::is_frozen(&store).await.unwrap());
    assert_eq!(mark_text(&dir).as_deref(), Some(MAX_SEQ_TEXT));
    assert_eq!(
        store.refollowing().await.unwrap(),
        Some(Refollowing::HeadUnknown)
    );
    assert_eq!(stored_log(&store).await, entries);
    assert_eq!(quarantined(&store).await, 0);
    let files = std::fs::read_dir(dir.path().join("quarantine")).map_or(0, Iterator::count);
    assert_eq!(files, 0, "no quarantine file is left");
    assert!(freeze::promotion_reads(&store).await.unwrap().quarantined);
}
