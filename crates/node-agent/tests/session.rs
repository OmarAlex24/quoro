// SPDX-License-Identifier: AGPL-3.0-only
//! The session over `tokio::io::duplex` with a fake primary
//! (`node-agent-protocol.md` sections 3-5). No TLS, no network: the protocol
//! core is exercised directly, as the contract asks.

mod agent_common;

use std::time::Duration;

use agent_common::{GATEWAY, SUBNET, TempDir, facts, identity_files, spec_vm};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_node_agent::session::{Session, SessionEnd, SessionSetup};
use quoro_node_agent::store::NodeStore;
use quoro_proto::sign::{self, SigningKey};
use quoro_proto::wire::{ControlMessage, DesiredState, DesiredStatus, Spec, SpecNetwork, Welcome};
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
            network: SpecNetwork {
                subnet: SUBNET.to_owned(),
                gateway: GATEWAY.to_owned(),
            },
            ingress: None,
            builds: Vec::new(),
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
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(store.buffered_events().await.unwrap().is_empty());
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
