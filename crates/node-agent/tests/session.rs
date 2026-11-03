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
