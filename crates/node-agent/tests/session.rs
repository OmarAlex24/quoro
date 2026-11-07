// SPDX-License-Identifier: AGPL-3.0-only
//! The session over `tokio::io::duplex` with a fake primary
//! (`node-agent-protocol.md` sections 3-5). No TLS, no network: the protocol
//! core is exercised directly, as the contract asks.

mod agent_common;
mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use agent_common::{
    GATEWAY, SUBNET, TempDir, facts, identity_files, snapshot, snapshot_full, spec_vm, stateful_vm,
};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_engine::{Engine as _, FakeEngine, VmState};
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_ingress::proxy::BackendObserver as _;
use quoro_node_agent::edge::EdgeManager;
use quoro_node_agent::ingress::{CertificateEvents, DnsBridge, IngressConfig, IngressSupervisor};
use quoro_node_agent::reconcile::Snapshot;
use quoro_node_agent::session::{Session, SessionEnd, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::{FenceTimers, LeaseConfig, LeaseState, ManualHostClock, start_local_leases};
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{
    Command, ControlMessage, DesiredState, DesiredStatus, EdgeBackend, EdgeRoute, HealthCheck,
    Spec, SpecEdge, SpecNetwork, Welcome,
};
use quoro_proto::{LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, ResourceType, RevokeReason};
use serde_json::Value;
use tokio::io::DuplexStream;
use tokio::sync::watch;
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
/// heartbeats, answers `ping`, and applies a `lease_grant` for another lease;
/// a `desired_state` and a second revoke of the same lease wait, and are
/// processed in the order they arrived once the revoke is answered.
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
        ControlMessage::Ping.to_frame("msg_ping", None).to_text(),
    )
    .await;

    // Everything let through is answered while the fence runs.
    let mut heartbeats = 0;
    let mut pong_before_answer = false;
    let mut answered: Vec<String> = Vec::new();
    let mut first_answered_at = None;
    let mut desired_answered_at = None;
    while answered.len() < 5 {
        let frame = read_frame(&mut ws).await;
        match frame["type"].as_str().unwrap() {
            // Only the heartbeats between the pong, sent during the fence,
            // and the revoke's answer count.
            "heartbeat" if pong_before_answer && answered.is_empty() => heartbeats += 1,
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
