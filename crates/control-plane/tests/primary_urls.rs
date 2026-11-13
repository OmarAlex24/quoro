//! `welcome.primary_urls` lists the machines a promotion could choose
//! (`node-agent-protocol.md` sections 5.1 and 5.10, ADR 0235, issue #951):
//! the primary's own URLs first, then the `hello.control_plane_urls` of
//! every other connected machine whose `Node.custody` is `sealed`, the first
//! URL of each, oldest enrolled first, then the second of each, `https://`
//! only, at most 16.

mod common;

use common::{Server, TestDir, start_patient_in};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::custody::{self, CustodyKey};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

/// The envelope id of every `hello` these tests send.
const HELLO_ID: &str = "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D";

/// The URLs this primary advertises for itself, in `welcome.primary_urls`
/// before any other machine's.
const OWN: [&str; 2] = ["https://primary.example:8443", "https://192.0.2.10:8443"];

/// A primary whose keys are all files in its data directory, so it seals
/// them to every custody key it binds (section 7.2), and that advertises
/// [`OWN`] for itself.
async fn start() -> Server {
    let server = start_patient_in(TestDir::new("quoro-primary-urls")).await;
    let _state = server
        .state
        .clone()
        .with_advertised_urls(|| OWN.map(str::to_string).to_vec());
    server
}

struct Enrolled {
    node_id: String,
    credential: String,
}

/// Enrolls a machine with `roles`. A machine without the `node` role sends
/// no KVM probe, as an edge does.
async fn enrolled(
    server: &Server,
    owner: &reqwest::Client,
    name: &str,
    roles: &[&str],
) -> Enrolled {
    // Section 8.10 rule 2: these tests enroll voters back to back.
    common::settle_voters(server).await;
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let token: Value = res.json().await.unwrap();
    let mut body = json!({
        "join_token": token["token"],
        "name": name,
        "public_key": sign::public_key_to_base64(
            &sign::signing_key_from_seed(&[7u8; 32]).verifying_key()
        ),
        "roles": roles,
        "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
        "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
    });
    if !roles.contains(&"node") {
        body.as_object_mut().unwrap().remove("kvm");
    }
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "enroll: {body}");
    Enrolled {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

/// Sets when `node_id` enrolled, so the tests choose the order in which
/// `welcome.primary_urls` lists the machines.
async fn enrolled_at(server: &Server, node_id: &str, created_at: &str) {
    sqlx::query("UPDATE nodes SET created_at = ? WHERE id = ?")
        .bind(created_at)
        .bind(node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

fn hello(node_id: &str, custody: Option<&CustodyKey>, urls: &[&str]) -> NodeMessage {
    NodeMessage::Hello(wire::Hello {
        node_id: node_id.to_string(),
        agent_version: "test-agent".to_string(),
        engine_version: "test-engine".to_string(),
        boot_id: "boot-1".to_string(),
        stored_rev: 0,
        applied_rev: 0,
        log_seq: 0,
        log_hash: "0".repeat(64),
        leases: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        mesh_public_key: None,
        volume_snapshot: None,
        custody_public_key: custody.map(|key| key.public_key().to_base64()),
        mesh_rules: None,
        control_plane_urls: urls.iter().map(|url| (*url).to_string()).collect(),
    })
}

async fn connect(server: &Server, credential: &str) -> Socket {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status(), 101);
    socket
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(30), socket.next())
            .await
            .expect("a frame within 30 s")
            .expect("the socket is open")
            .expect("a valid websocket frame");
        match message {
            WsMessage::Text(text) => {
                return serde_json::from_str(text.as_str()).expect("JSON frame");
            }
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

/// Opens a session for `node` with `hello`, reads every frame up to the
/// session's first `desired_state` (the custody binding and its seals are
/// written before it, section 4.2), and returns the open socket and the
/// `welcome.primary_urls` it received.
async fn session(server: &Server, node: &Enrolled, hello: NodeMessage) -> (Socket, Vec<String>) {
    let mut socket = connect(server, &node.credential).await;
    let text = hello.to_frame(HELLO_ID, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
    let mut primary_urls = None;
    loop {
        let frame = recv_frame(&mut socket).await;
        match frame["type"].as_str() {
            Some("welcome") => {
                primary_urls = Some(
                    frame["body"]
                        .get("primary_urls")
                        .map(|urls| serde_json::from_value::<Vec<String>>(urls.clone()).unwrap())
                        .unwrap_or_default(),
                );
            }
            Some("error") => panic!("the session start failed: {frame}"),
            Some("desired_state") => {
                return (socket, primary_urls.expect("welcome before desired_state"));
            }
            _ => {}
        }
    }
}

/// A machine with the `node` role whose custody key is bound and sealed,
/// connected with `urls` as its `control_plane_urls`. Returns its open
/// socket.
async fn sealed(server: &Server, node: &Enrolled, urls: &[&str]) -> Socket {
    let key = CustodyKey::generate(&mut custody::os_rng());
    let (socket, _) = session(server, node, hello(&node.node_id, Some(&key), urls)).await;
    let seals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM node_key_custody WHERE node_id = ?")
        .bind(&node.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(seals, 3, "every key in use is sealed to {}", node.node_id);
    socket
}

/// The `welcome.primary_urls` a machine that reports nothing receives.
async fn welcome_urls(server: &Server, reader: &Enrolled) -> Vec<String> {
    let (_socket, urls) = session(server, reader, hello(&reader.node_id, None, &[])).await;
    urls
}

fn owned(urls: &[&str]) -> Vec<String> {
    urls.iter().map(|url| (*url).to_string()).collect()
}

// ---------------------------------------------------------------------------

/// Section 5.10 (ADR 0235): the primary's own URLs, then the first URL of
/// each connected `sealed` machine, oldest enrolled first, then the second
/// of each. The primary's own machine, a machine whose custody is not
/// `sealed`, one without the `node` role and one that is no longer connected
/// are left out, and so is every entry that is not `https://`.
#[tokio::test]
async fn welcome_lists_the_connected_sealed_machines_oldest_enrolled_first() {
    let server = start().await;
    let owner = server.owner().await;
    let own_node = enrolled(&server, &owner, "primary", &["node"]).await;
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;
    let b = enrolled(&server, &owner, "node-b", &["node"]).await;
    let c = enrolled(&server, &owner, "node-c", &["node"]).await;
    let gone = enrolled(&server, &owner, "node-gone", &["node"]).await;
    let unsealed = enrolled(&server, &owner, "node-unsealed", &["node"]).await;
    let edge = enrolled(&server, &owner, "edge-1", &["edge"]).await;
    let reader = enrolled(&server, &owner, "edge-2", &["edge"]).await;

    // The primary's own machine holds the primary status.
    sqlx::query("UPDATE nodes SET cp_status = 'primary' WHERE id = ?")
        .bind(&own_node.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    // B enrolled first, then C, then A: not the order they connect in.
    enrolled_at(&server, &own_node.node_id, "2026-11-01T09:00:00Z").await;
    enrolled_at(&server, &gone.node_id, "2026-11-01T09:30:00Z").await;
    enrolled_at(&server, &b.node_id, "2026-11-01T10:00:00Z").await;
    enrolled_at(&server, &c.node_id, "2026-11-01T11:00:00Z").await;
    enrolled_at(&server, &a.node_id, "2026-11-01T12:00:00Z").await;

    let _own = sealed(&server, &own_node, &["https://192.0.2.9:8443"]).await;
    let _a = sealed(
        &server,
        &a,
        &[
            "http://192.0.2.31:8443",
            "https://192.0.2.31:8443",
            "https://[2001:db8::31]:8443",
        ],
    )
    .await;
    let _b = sealed(
        &server,
        &b,
        &[
            "https://192.0.2.21:8443",
            "https://[2001:db8::21]:8443",
            "https://node-b.local:8443",
        ],
    )
    .await;
    let _c = sealed(&server, &c, &["https://192.0.2.22:8443"]).await;
    let gone_socket = sealed(&server, &gone, &["https://192.0.2.40:8443"]).await;
    // Bound but never sealed: custody `pending`.
    let key = CustodyKey::generate(&mut custody::os_rng());
    let (_unsealed, _) = session(
        &server,
        &unsealed,
        hello(&unsealed.node_id, Some(&key), &["https://192.0.2.50:8443"]),
    )
    .await;
    sqlx::query("DELETE FROM node_key_custody WHERE node_id = ? AND key_kind = 'tls'")
        .bind(&unsealed.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    // An edge has no custody at all, whatever it reports.
    let (_edge, _) = session(
        &server,
        &edge,
        hello(&edge.node_id, None, &["https://192.0.2.60:8443"]),
    )
    .await;

    // A machine that disconnected is left out.
    drop(gone_socket);
    timeout(Duration::from_secs(10), async {
        while server.state.hub.is_connected(&gone.node_id) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the closed session ends");

    assert_eq!(
        welcome_urls(&server, &reader).await,
        owned(&[
            OWN[0],
            OWN[1],
            "https://192.0.2.21:8443",
            "https://192.0.2.22:8443",
            "https://192.0.2.31:8443",
            "https://[2001:db8::21]:8443",
            "https://[2001:db8::31]:8443",
            "https://node-b.local:8443",
        ])
    );
}

/// Section 5.10 (ADR 0235): the list stops at 16 entries, so the URLs a
/// machine lists last, and the machines enrolled last, are the ones left
/// out.
#[tokio::test]
async fn welcome_lists_at_most_16_urls() {
    let server = start().await;
    let owner = server.owner().await;
    let reader = enrolled(&server, &owner, "edge-1", &["edge"]).await;
    let mut sockets = Vec::new();
    let mut machines: Vec<Vec<String>> = Vec::new();
    for machine in 1..=5 {
        let node = enrolled(&server, &owner, &format!("node-{machine}"), &["node"]).await;
        enrolled_at(
            &server,
            &node.node_id,
            &format!("2026-11-01T1{machine}:00:00Z"),
        )
        .await;
        let urls: Vec<String> = (1..=4)
            .map(|url| format!("https://192.0.{url}.{machine}:8443"))
            .collect();
        let listed: Vec<&str> = urls.iter().map(String::as_str).collect();
        sockets.push(sealed(&server, &node, &listed).await);
        machines.push(urls);
    }

    let urls = welcome_urls(&server, &reader).await;
    // Two of the primary's own, then five firsts, five seconds and five
    // thirds would make 17: the third URL of the newest machine is cut.
    let mut expected = owned(&OWN);
    for round in 0..3 {
        for machine in &machines {
            expected.push(machine[round].clone());
        }
    }
    expected.truncate(16);
    assert_eq!(wire::MAX_PRIMARY_URLS, 16);
    assert_eq!(urls, expected);
    assert!(!urls.contains(&machines[4][2]), "{urls:?}");
}

/// Section 5.10 (ADR 0235): "every other connected machine" is read as
/// other than the primary, so a `sealed` machine finds its own
/// `control_plane_urls` in its own `welcome`, merged in the order of its
/// enrollment like any other machine's. That lets the agent of a promoted
/// machine reach its own new control plane. Its first session binds its
/// custody key and seals the keys to it; the next one, with the same key,
/// starts `sealed`.
#[tokio::test]
async fn a_sealed_machine_finds_its_own_urls_in_its_own_welcome() {
    let server = start().await;
    let owner = server.owner().await;
    let reader = enrolled(&server, &owner, "node-reader", &["node"]).await;
    let other = enrolled(&server, &owner, "node-other", &["node"]).await;
    enrolled_at(&server, &reader.node_id, "2026-11-01T10:00:00Z").await;
    enrolled_at(&server, &other.node_id, "2026-11-01T11:00:00Z").await;
    let _other = sealed(&server, &other, &["https://192.0.2.22:8443"]).await;

    let key = CustodyKey::generate(&mut custody::os_rng());
    let own_urls = ["https://192.0.2.21:8443", "https://node-reader.local:8443"];
    let (first, _) = session(
        &server,
        &reader,
        hello(&reader.node_id, Some(&key), &own_urls),
    )
    .await;
    let seals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM node_key_custody WHERE node_id = ?")
        .bind(&reader.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(seals, 3, "every key in use is sealed to the reader");
    drop(first);
    timeout(Duration::from_secs(10), async {
        while server.state.hub.is_connected(&reader.node_id) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the closed session ends");

    let (_socket, urls) = session(
        &server,
        &reader,
        hello(&reader.node_id, Some(&key), &own_urls),
    )
    .await;
    assert_eq!(
        urls,
        owned(&[
            OWN[0],
            OWN[1],
            "https://192.0.2.21:8443",
            "https://192.0.2.22:8443",
            "https://node-reader.local:8443",
        ])
    );
}
