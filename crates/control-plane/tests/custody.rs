//! Key custody on the primary (`node-agent-protocol.md` section 7.2, ADR
//! 0180, ADR 0181, ADR 0231, issue #727): binding a machine's custody key
//! from `hello`, the seals of every key in use in `node_key_custody`,
//! `Node.custody`, the reconcile at every start, the seals a follower
//! receives, and `Cluster.signing_key_id`.

mod common;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use common::replay;
use common::{Server, restart_with_resolver, start_patient_in};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::ReferenceAsDigest;
use quoro_proto::custody::{self, CustodyKey, KeyKind, SealContext};
use quoro_proto::wire::{self, Capacity as WireCapacity, Kvm as WireKvm, LogAck, NodeMessage};
use quoro_proto::{jcs, sign};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

/// The envelope id of every `hello` these tests send.
const HELLO_ID: &str = "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D";
/// A `nod_` id no machine has.
const OTHER_NODE: &str = "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP";

/// A primary whose keys are all files in its data directory.
async fn start() -> Server {
    let dir = std::env::temp_dir().join(format!(
        "quoro-custody-{}",
        &quoro_control_plane::store::random_secret()[..16]
    ));
    start_patient_in(dir).await
}

fn new_key() -> CustodyKey {
    CustodyKey::generate(&mut custody::os_rng())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Enrolled {
    node_id: String,
    credential: String,
}

/// Enrolls a machine with `roles`. A machine without the `node` role sends no
/// KVM probe, as an edge does.
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

fn hello(node_id: &str, custody_public_key: Option<String>) -> NodeMessage {
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
        custody_public_key,
        mesh_rules: None,
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

async fn send_with_id(socket: &mut Socket, message: NodeMessage, id: &str) {
    let text = message.to_frame(id, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
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

/// Opens a session with a `hello` carrying `key`, and returns every frame up
/// to the session's first `desired_state`. The custody binding is written
/// before it (section 4.2).
async fn session(server: &Server, node: &Enrolled, key: Option<String>) -> (Socket, Vec<Value>) {
    let mut socket = connect(server, &node.credential).await;
    send_with_id(&mut socket, hello(&node.node_id, key), HELLO_ID).await;
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(&mut socket).await;
        let done = frame["type"] == "desired_state";
        frames.push(frame);
        if done {
            return (socket, frames);
        }
    }
}

fn of_type<'a>(frames: &'a [Value], kind: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == kind)
        .collect()
}

/// Pings and returns every frame that came before the answer: the session
/// is still up, and the frames are what the primary queued before it.
async fn ping(socket: &mut Socket, round: u32) -> Vec<Value> {
    let id = format!("msg_ping{round:022}");
    send_with_id(socket, NodeMessage::Ping, &id).await;
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == "pong" && frame["re"] == id.as_str() {
            return frames;
        }
        frames.push(frame);
    }
}

async fn node_json(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

async fn listed_json(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let res = owner
        .get(server.url("/api/v1/nodes?limit=100"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == node_id)
        .unwrap_or_else(|| panic!("{node_id} is not listed: {list}"))
        .clone()
}

/// `Node.custody` from `getNode` and from `listNodes`, which must agree.
async fn custody_of(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let node = node_json(server, owner, node_id).await;
    let listed = listed_json(server, owner, node_id).await;
    let custody = node
        .get("custody")
        .unwrap_or_else(|| panic!("custody is absent: {node}"))
        .clone();
    assert_eq!(listed.get("custody"), Some(&custody), "{listed}");
    custody
}

async fn bound_key(server: &Server, node_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT custody_public_key FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

async fn changed_at(server: &Server, node_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT custody_key_changed_at FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// One `node_key_custody` change-log entry.
struct SealEntry {
    op: String,
    entity_id: String,
    payload: Value,
}

/// The `node_key_custody` change-log entries of `node_id`, oldest first.
async fn seal_entries(server: &Server, node_id: &str) -> Vec<SealEntry> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT op, entity_id, payload FROM change_log \
         WHERE entity_type = 'node_key_custody' AND entity_id LIKE ? ORDER BY seq",
    )
    .bind(format!("{node_id}/%"))
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(op, entity_id, payload)| SealEntry {
            op,
            entity_id,
            payload: serde_json::from_str(&payload).unwrap(),
        })
        .collect()
}

/// The `(key_kind, key_id)` of the seal rows of `node_id`, sorted.
async fn seal_rows(server: &Server, node_id: &str) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT key_kind, key_id FROM node_key_custody WHERE node_id = ? ORDER BY key_kind, key_id",
    )
    .bind(node_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

/// Every custody write in the change log: seals, bound keys and refused
/// changes. A `hello` also writes the node's whole row, which carries both
/// custody columns with every other column; it is told apart by `status`,
/// which no custody write carries.
async fn custody_writes(server: &Server) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM change_log WHERE entity_type = 'node_key_custody' \
         OR (entity_type = 'node' AND json_type(payload, '$.status') IS NULL \
             AND (json_type(payload, '$.custody_public_key') IS NOT NULL \
                  OR json_type(payload, '$.custody_key_changed_at') IS NOT NULL))",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
}

async fn key_changed_records(server: &Server) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE operation = 'node.custody_key_changed'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
}

fn kind_of(text: &str) -> KeyKind {
    match text {
        "signing" => KeyKind::Signing,
        "dek" => KeyKind::Dek,
        "tls" => KeyKind::Tls,
        other => panic!("unknown key_kind {other}"),
    }
}

/// Opens the seal a payload carries with `key`, under `node_id`.
fn open_seal(
    key: &CustodyKey,
    cluster_id: &str,
    node_id: &str,
    payload: &Value,
) -> Result<Vec<u8>, custody::CustodyError> {
    let sealed = STANDARD
        .decode(payload["sealed"].as_str().expect("sealed is base64 text"))
        .expect("sealed is standard base64");
    let context = SealContext {
        cluster_id,
        node_id,
        key_kind: kind_of(payload["key_kind"].as_str().unwrap()),
        key_id: payload["key_id"].as_str().unwrap(),
    };
    custody::open(key, &context, &sealed).map(|plaintext| plaintext.to_vec())
}

/// The DER of the first `CERTIFICATE` block of `pem`.
fn leaf_der(pem: &str) -> Vec<u8> {
    let begin = "-----BEGIN CERTIFICATE-----";
    let start = pem.find(begin).unwrap() + begin.len();
    let stop = start + pem[start..].find("-----END CERTIFICATE-----").unwrap();
    let body: String = pem[start..stop]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    STANDARD.decode(body).unwrap()
}

/// Every key the primary holds, read from its files, as `(key_kind, key_id,
/// plaintext)` defined by section 7.2.
fn expected_keys(data_dir: &Path) -> Vec<(&'static str, String, Vec<u8>)> {
    let seed: [u8; 32] = std::fs::read(data_dir.join("keys/signing.key"))
        .unwrap()
        .try_into()
        .unwrap();
    let public = sign::signing_key_from_seed(&seed).verifying_key();
    let data = std::fs::read(data_dir.join("keys/data.key")).unwrap();
    assert_eq!(data.len(), 48);
    let cert_pem = std::fs::read_to_string(data_dir.join("tls/cert.pem")).unwrap();
    let key_pem = std::fs::read_to_string(data_dir.join("tls/key.pem")).unwrap();
    let tls = jcs::canonicalize(&json!({ "cert_pem": cert_pem, "key_pem": key_pem })).unwrap();
    vec![
        ("signing", sha256_hex(public.as_bytes()), seed.to_vec()),
        (
            "dek",
            data[..16]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            data[16..].to_vec(),
        ),
        ("tls", sha256_hex(&leaf_der(&cert_pem)), tls.into_bytes()),
    ]
}

/// Binds `key` to `node` through a first session, and checks the three seals
/// it wrote. Returns the open socket.
async fn bind(server: &Server, node: &Enrolled, key: &CustodyKey) -> Socket {
    let (socket, frames) = session(server, node, Some(key.public_key().to_base64())).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert_eq!(seal_rows(server, &node.node_id).await.len(), 3);
    socket
}

// ---------------------------------------------------------------------------

/// Section 7.2, "Binding" and "Seals": the first key is stored, and every key
/// in use is sealed to it in the same write, one change-log entry per kind,
/// each opening with the machine's key to the primary's key and to nothing
/// with another key or under another machine.
#[tokio::test]
async fn a_first_hello_binds_the_key_and_seals_every_key_in_use() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let key = new_key();
    let other = new_key();
    assert_eq!(custody_writes(&server).await, 0);

    let (_socket, frames) = session(&server, &node, Some(key.public_key().to_base64())).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert_eq!(
        bound_key(&server, &node.node_id).await,
        Some(key.public_key().to_base64())
    );

    let entries = seal_entries(&server, &node.node_id).await;
    assert_eq!(entries.len(), 3, "one seal entry per kind");
    let cluster_id = server.state.cluster_id.as_str();
    for (kind, key_id, plaintext) in expected_keys(&server.data_dir) {
        let entry = entries
            .iter()
            .find(|entry| entry.payload["key_kind"] == kind)
            .unwrap_or_else(|| panic!("no {kind} seal"));
        assert_eq!(entry.op, "create");
        assert_eq!(entry.entity_id, format!("{}/{kind}/{key_id}", node.node_id));
        let payload = entry.payload.as_object().unwrap();
        assert_eq!(payload.len(), 5, "every column: {payload:?}");
        assert_eq!(payload["node_id"], node.node_id.as_str());
        assert_eq!(payload["key_id"], key_id.as_str());
        assert!(payload["created_at"].is_string());

        let opened = open_seal(&key, cluster_id, &node.node_id, &entry.payload).unwrap();
        assert_eq!(
            opened, plaintext,
            "the {kind} seal opens to the primary's key"
        );
        assert!(
            open_seal(&other, cluster_id, &node.node_id, &entry.payload).is_err(),
            "another machine's key opens no {kind} seal"
        );
        assert!(
            open_seal(&key, cluster_id, OTHER_NODE, &entry.payload).is_err(),
            "a {kind} seal opens under no other node_id"
        );

        // The row holds the bytes the payload carries.
        let sealed: Vec<u8> = sqlx::query_scalar(
            "SELECT sealed FROM node_key_custody WHERE node_id = ? AND key_kind = ? AND key_id = ?",
        )
        .bind(&node.node_id)
        .bind(kind)
        .bind(&key_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        assert_eq!(
            sealed,
            STANDARD
                .decode(payload["sealed"].as_str().unwrap())
                .unwrap()
        );
    }
    // The signing key id is the one the primary signs with.
    assert_eq!(
        expected_keys(&server.data_dir)[0].1,
        server.state.keys.key_id()
    );
}

/// Section 7.2, "Binding": the same key again changes nothing.
#[tokio::test]
async fn a_second_hello_with_the_same_key_writes_nothing() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let key = new_key();
    let socket = bind(&server, &node, &key).await;
    let writes = custody_writes(&server).await;
    assert_eq!(writes, 4, "the bound key and three seals");
    drop(socket);

    let (_socket, frames) = session(&server, &node, Some(key.public_key().to_base64())).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert_eq!(custody_writes(&server).await, writes);
    assert_eq!(key_changed_records(&server).await, 0);
    assert_eq!(
        bound_key(&server, &node.node_id).await,
        Some(key.public_key().to_base64())
    );
}

/// Section 7.2, "Binding", and section 5.18: a different key is answered
/// with `custody_key_changed`, non-fatal; the session goes on; the change is
/// recorded and audited once; nothing is sealed to the new key.
#[tokio::test]
async fn a_changed_key_is_refused_audited_once_and_gets_no_seal() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let key = new_key();
    let changed = new_key();
    drop(bind(&server, &node, &key).await);
    let writes = custody_writes(&server).await;

    let (mut socket, frames) =
        session(&server, &node, Some(changed.public_key().to_base64())).await;
    assert_eq!(frames[0]["type"], "welcome", "{frames:?}");
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["code"], "custody_key_changed");
    assert_eq!(errors[0]["body"]["fatal"], false);
    assert_eq!(errors[0]["re"], HELLO_ID);
    assert!(errors[0]["body"].get("term").is_none(), "{}", errors[0]);
    // The session goes on.
    assert!(of_type(&ping(&mut socket, 1).await, "error").is_empty());
    assert_eq!(
        node_json(&server, &owner, &node.node_id).await["status"],
        "connected"
    );

    assert_eq!(
        bound_key(&server, &node.node_id).await,
        Some(key.public_key().to_base64()),
        "the bound key is never replaced"
    );
    let first_change = changed_at(&server, &node.node_id).await;
    assert!(first_change.is_some());
    assert_eq!(
        custody_writes(&server).await,
        writes + 1,
        "only the timestamp"
    );

    #[allow(clippy::type_complexity)]
    let records: Vec<(
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        String,
        Option<i64>,
    )> = sqlx::query_as(
        "SELECT actor_type, actor_id, channel, scope, target_type, target_id, params, change_seq \
         FROM audit_log WHERE operation = 'node.custody_key_changed'",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(records.len(), 1);
    let (actor_type, actor_id, channel, scope, target_type, target_id, params, change_seq) =
        &records[0];
    assert_eq!(
        (actor_type.as_str(), actor_id.as_str(), channel.as_str()),
        ("system", "nodes", "system")
    );
    assert_eq!(scope, &None);
    assert_eq!(
        (target_type.as_str(), target_id.as_str()),
        ("node", node.node_id.as_str())
    );
    assert_eq!(
        serde_json::from_str::<Value>(params).unwrap(),
        json!({ "node_id": node.node_id })
    );
    let logged: String = sqlx::query_scalar("SELECT payload FROM change_log WHERE seq = ?")
        .bind(change_seq.expect("the record names its change-log entry"))
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let logged: Value = serde_json::from_str(&logged).unwrap();
    assert_eq!(
        logged["custody_key_changed_at"],
        first_change.as_deref().unwrap()
    );

    // No seal to the new key: every seal is the first key's.
    let entries = seal_entries(&server, &node.node_id).await;
    assert_eq!(entries.len(), 3);
    for entry in &entries {
        let cluster_id = server.state.cluster_id.as_str();
        assert!(open_seal(&key, cluster_id, &node.node_id, &entry.payload).is_ok());
        assert!(open_seal(&changed, cluster_id, &node.node_id, &entry.payload).is_err());
    }
    drop(socket);

    // A third hello with the changed key is refused again, and recorded no
    // second time.
    let (_socket, frames) = session(&server, &node, Some(changed.public_key().to_base64())).await;
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["code"], "custody_key_changed");
    assert_eq!(key_changed_records(&server).await, 1);
    assert_eq!(changed_at(&server, &node.node_id).await, first_change);
    assert_eq!(custody_writes(&server).await, writes + 1);
    assert_eq!(seal_entries(&server, &node.node_id).await.len(), 3);
    assert_eq!(
        custody_of(&server, &owner, &node.node_id).await,
        "key_changed"
    );
}

/// Section 7.2, "Status": `none`, `sealed`, `pending`, and null without the
/// `node` role, in `getNode` and `listNodes`, never absent.
#[tokio::test]
async fn node_custody_reads_none_sealed_pending_and_null() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let edge = enrolled(&server, &owner, "edge-1", &["edge"]).await;
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "none");
    assert_eq!(
        custody_of(&server, &owner, &edge.node_id).await,
        Value::Null
    );

    let key = new_key();
    let _socket = bind(&server, &node, &key).await;
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "sealed");

    sqlx::query("DELETE FROM node_key_custody WHERE node_id = ? AND key_kind = 'tls'")
        .bind(&node.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "pending");
    assert_eq!(
        custody_of(&server, &owner, &edge.node_id).await,
        Value::Null
    );
}

/// Section 7.2: edge-only machines get no seal, even when their `hello`
/// carries a key.
#[tokio::test]
async fn an_edge_only_machine_binds_no_key() {
    let server = start().await;
    let owner = server.owner().await;
    let edge = enrolled(&server, &owner, "edge-1", &["edge"]).await;
    let key = new_key();
    let (_socket, frames) = session(&server, &edge, Some(key.public_key().to_base64())).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert_eq!(bound_key(&server, &edge.node_id).await, None);
    assert!(seal_rows(&server, &edge.node_id).await.is_empty());
    assert_eq!(custody_writes(&server).await, 0);
}

/// A key that is not base64 of 32 bytes is treated as absent: nothing is
/// bound, nothing is answered, and a later valid key is the first key.
#[tokio::test]
async fn an_unusable_key_binds_nothing() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    for unusable in ["not base64!", "AAAA"] {
        let (socket, frames) = session(&server, &node, Some(unusable.to_string())).await;
        assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
        drop(socket);
    }
    assert_eq!(bound_key(&server, &node.node_id).await, None);
    assert_eq!(custody_writes(&server).await, 0);
    let key = new_key();
    let _socket = bind(&server, &node, &key).await;
    assert_eq!(
        bound_key(&server, &node.node_id).await,
        Some(key.public_key().to_base64())
    );
}

/// A key of 32 zero bytes parses, but HPKE seals nothing to it (a low-order
/// point), so binding it fails as a whole: the session goes on, and no key,
/// no seal and no change-log entry are written; `Node.custody` reads `none`.
/// A later good key then binds as the first key.
#[tokio::test]
async fn a_zero_key_binds_nothing_and_the_session_goes_on() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;

    let (mut socket, frames) = session(&server, &node, Some(STANDARD.encode([0u8; 32]))).await;
    assert_eq!(frames[0]["type"], "welcome", "{frames:?}");
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert!(of_type(&ping(&mut socket, 1).await, "error").is_empty());
    assert_eq!(
        node_json(&server, &owner, &node.node_id).await["status"],
        "connected"
    );
    assert_eq!(bound_key(&server, &node.node_id).await, None);
    assert!(seal_rows(&server, &node.node_id).await.is_empty());
    assert_eq!(custody_writes(&server).await, 0);
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "none");
    drop(socket);

    let key = new_key();
    let _socket = bind(&server, &node, &key).await;
    assert_eq!(
        bound_key(&server, &node.node_id).await,
        Some(key.public_key().to_base64())
    );
    assert_eq!(custody_writes(&server).await, 4, "the key and three seals");
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "sealed");
}

/// A client signed in as the owner on `server`, whose owner already exists.
async fn owner_of(server: &Server) -> reqwest::Client {
    let client = server.client();
    server
        .login(&client, common::OWNER_EMAIL, common::OWNER_PASSWORD)
        .await;
    client
}

/// Section 7.2, "When": a restarted primary seals what is missing, and
/// deletes through the change log the seals of key ids no longer in use and
/// of removed machines.
#[tokio::test]
async fn a_restart_reconciles_the_seals() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let gone = enrolled(&server, &owner, "node-gone", &["node"]).await;
    let key = new_key();
    drop(bind(&server, &node, &key).await);
    drop(bind(&server, &gone, &new_key()).await);
    let in_use = seal_rows(&server, &node.node_id).await;

    let pool = server.state.store.pool();
    sqlx::query("DELETE FROM node_key_custody WHERE node_id = ? AND key_kind = 'tls'")
        .bind(&node.node_id)
        .execute(pool)
        .await
        .unwrap();
    let retired = "ab".repeat(16);
    sqlx::query(
        "INSERT INTO node_key_custody (node_id, key_kind, key_id, sealed, created_at) \
         VALUES (?, 'dek', ?, x'00', '2026-11-09T00:00:00.000Z')",
    )
    .bind(&node.node_id)
    .bind(&retired)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&gone.node_id)
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(custody_of(&server, &owner, &node.node_id).await, "pending");

    let restarted = restart_with_resolver(&server, Arc::new(ReferenceAsDigest)).await;

    assert_eq!(seal_rows(&restarted, &node.node_id).await, in_use);
    let entries = seal_entries(&restarted, &node.node_id).await;
    let tls: Vec<&SealEntry> = entries
        .iter()
        .filter(|entry| entry.payload["key_kind"] == "tls")
        .collect();
    assert_eq!(tls.len(), 2, "sealed at binding and again at the start");
    assert_eq!(tls[1].op, "create");
    assert!(
        open_seal(
            &key,
            &restarted.state.cluster_id,
            &node.node_id,
            &tls[1].payload
        )
        .is_ok()
    );
    let retired_entry = format!("{}/dek/{retired}", node.node_id);
    assert!(
        entries
            .iter()
            .any(|entry| entry.entity_id == retired_entry && entry.op == "delete"),
        "the retired key's seal is deleted through the change log"
    );

    assert!(seal_rows(&restarted, &gone.node_id).await.is_empty());
    let gone_deletes = seal_entries(&restarted, &gone.node_id)
        .await
        .into_iter()
        .filter(|entry| entry.op == "delete")
        .count();
    assert_eq!(gone_deletes, 3, "a removed machine's seals are deleted");
    let owner = owner_of(&restarted).await;
    assert_eq!(
        custody_of(&restarted, &owner, &node.node_id).await,
        "sealed"
    );
}

/// Section 7.2: a machine whose changed key was refused gets no seal at a
/// restart, not even one it lacks, and keeps the seals made to its bound key,
/// since only a removal or a key's retirement deletes seals ("When"). A
/// healthy machine lacking the same seal is sealed again, so the reconcile
/// did run.
#[tokio::test]
async fn a_restart_seals_nothing_to_a_key_changed_machine_and_keeps_its_seals() {
    let server = start().await;
    let owner = server.owner().await;
    let refused = enrolled(&server, &owner, "node-refused", &["node"]).await;
    let healthy = enrolled(&server, &owner, "node-healthy", &["node"]).await;
    drop(bind(&server, &refused, &new_key()).await);
    drop(bind(&server, &healthy, &new_key()).await);
    let (socket, frames) =
        session(&server, &refused, Some(new_key().public_key().to_base64())).await;
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["code"], "custody_key_changed");
    drop(socket);
    assert!(changed_at(&server, &refused.node_id).await.is_some());

    let pool = server.state.store.pool();
    for node in [&refused, &healthy] {
        sqlx::query("DELETE FROM node_key_custody WHERE node_id = ? AND key_kind = 'tls'")
            .bind(&node.node_id)
            .execute(pool)
            .await
            .unwrap();
    }
    let kept = seal_rows(&server, &refused.node_id).await;
    assert_eq!(kept.len(), 2, "{kept:?}");
    let logged = seal_entries(&server, &refused.node_id).await.len();

    let restarted = restart_with_resolver(&server, Arc::new(ReferenceAsDigest)).await;

    assert_eq!(
        seal_rows(&restarted, &healthy.node_id).await.len(),
        3,
        "the reconcile ran and sealed the healthy machine again"
    );
    assert_eq!(
        seal_rows(&restarted, &refused.node_id).await,
        kept,
        "no seal is made to the refused machine, and its seals are kept"
    );
    let entries = seal_entries(&restarted, &refused.node_id).await;
    assert_eq!(entries.len(), logged, "nothing written for it");
    assert!(entries.iter().all(|entry| entry.op == "create"));
    let owner = owner_of(&restarted).await;
    assert_eq!(
        custody_of(&restarted, &owner, &refused.node_id).await,
        "key_changed"
    );
}

/// Section 7.2, "When": a primary restarted on a new TLS certificate and key,
/// read from `<data_dir>/tls/` as at every start, seals the new pair to every
/// bound machine under the new `key_id`, and deletes the old `tls` seals
/// through the change log, as a retired key's.
#[tokio::test]
async fn a_replaced_tls_certificate_is_sealed_and_the_old_seal_deleted() {
    let server = start().await;
    let owner = server.owner().await;
    let first = enrolled(&server, &owner, "node-a", &["node"]).await;
    let second = enrolled(&server, &owner, "node-b", &["node"]).await;
    let first_key = new_key();
    let second_key = new_key();
    drop(bind(&server, &first, &first_key).await);
    drop(bind(&server, &second, &second_key).await);
    let (_, old_id, _) = expected_keys(&server.data_dir).remove(2);

    let replaced = rcgen::generate_simple_self_signed(vec!["primary.example".to_string()]).unwrap();
    let tls_dir = server.data_dir.join("tls");
    std::fs::write(tls_dir.join("key.pem"), replaced.key_pair.serialize_pem()).unwrap();
    std::fs::write(tls_dir.join("cert.pem"), replaced.cert.pem()).unwrap();
    let (kind, new_id, new_plaintext) = expected_keys(&server.data_dir).remove(2);
    assert_eq!(kind, "tls");
    assert_ne!(new_id, old_id);

    let restarted = common::restart_reloading_tls(&server).await;
    assert_eq!(restarted.state.config.tls.key_id(), new_id);

    for (node, key) in [(&first, &first_key), (&second, &second_key)] {
        let rows = seal_rows(&restarted, &node.node_id).await;
        assert_eq!(rows.len(), 3, "{rows:?}");
        assert!(
            rows.contains(&("tls".to_string(), new_id.clone())),
            "{rows:?}"
        );
        let entries = seal_entries(&restarted, &node.node_id).await;
        let sealed = entries
            .iter()
            .find(|entry| {
                entry.op == "create" && entry.entity_id == format!("{}/tls/{new_id}", node.node_id)
            })
            .expect("a tls seal under the new key id");
        let opened = open_seal(
            key,
            &restarted.state.cluster_id,
            &node.node_id,
            &sealed.payload,
        )
        .unwrap();
        assert_eq!(opened, new_plaintext, "the seal opens to the new PEM");
        let pair: Value = serde_json::from_slice(&opened).unwrap();
        assert_eq!(pair["cert_pem"], replaced.cert.pem());
        assert!(
            entries.iter().any(|entry| entry.op == "delete"
                && entry.entity_id == format!("{}/tls/{old_id}", node.node_id)),
            "the old tls seal is deleted through the change log"
        );
    }
    let owner = owner_of(&restarted).await;
    assert_eq!(
        custody_of(&restarted, &owner, &first.node_id).await,
        "sealed"
    );
}

/// Checks a `log_entries` body as a node does (section 7.1): no gap from
/// `from`, each entry's `hash` over the RFC 8785 form of the entry without
/// it, each linked to the one before. Returns the last `seq` and `hash`.
fn verify_entries(body: &Value, from: u64, prev_hash: &str) -> (u64, String) {
    assert_eq!(body["from_seq"], from, "{body}");
    let mut prev = prev_hash.to_string();
    let mut seq = from;
    for entry in body["entries"].as_array().unwrap() {
        let mut fields = entry.as_object().unwrap().clone();
        assert_eq!(entry["seq"], seq, "{entry}");
        assert_eq!(entry["prev_hash"], prev.as_str(), "{entry}");
        let hash = fields.remove("hash").unwrap();
        prev = sha256_hex(
            jcs::canonicalize(&Value::Object(fields))
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(hash, prev.as_str(), "{entry}");
        seq += 1;
    }
    (seq - 1, prev)
}

/// Section 7.2: `node_key_custody` is replicated. A follower receives its
/// seals in `log_entries`, they verify in the chain, and it opens them.
#[tokio::test]
async fn a_follower_receives_its_seals_and_they_verify() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let key = new_key();
    let (mut socket, mut frames) =
        session(&server, &node, Some(key.public_key().to_base64())).await;

    let mut entries: Vec<Value> = Vec::new();
    let mut last = 0;
    let mut hash = "0".repeat(64);
    for round in 0..50 {
        frames.extend(ping(&mut socket, round).await);
        let messages: Vec<Value> = of_type(&frames, "log_entries")
            .into_iter()
            .cloned()
            .collect();
        frames.clear();
        let Some(message) = messages.first() else {
            if last > 0 {
                break;
            }
            continue;
        };
        assert_eq!(messages.len(), 1, "one message outstanding");
        (last, hash) = verify_entries(&message["body"], last + 1, &hash);
        entries.extend(
            message["body"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
        send_with_id(
            &mut socket,
            NodeMessage::LogAck(LogAck {
                seq: last,
                hash: hash.clone(),
            }),
            &format!("msg_ack{round:023}"),
        )
        .await;
    }
    assert!(last > 0, "the node followed the change log");

    let prefix = format!("{}/", node.node_id);
    let seals: Vec<&Value> = entries
        .iter()
        .filter(|entry| {
            entry["entity_type"] == "node_key_custody"
                && entry["entity_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with(&prefix))
        })
        .collect();
    assert_eq!(seals.len(), 3, "{seals:?}");
    let expected = expected_keys(&server.data_dir);
    for seal in seals {
        assert_eq!(seal["op"], "create");
        let payload = &seal["payload"];
        let (_, _, plaintext) = expected
            .iter()
            .find(|(kind, _, _)| payload["key_kind"] == *kind)
            .unwrap();
        assert_eq!(
            &open_seal(&key, &server.state.cluster_id, &node.node_id, payload).unwrap(),
            plaintext
        );
    }
}

/// `Cluster.signing_key_id` is the current signing key's id; no rotation
/// runs, so `signing_key_rotation` stays absent.
#[tokio::test]
async fn the_cluster_reports_its_signing_key_id() {
    let server = start().await;
    let owner = server.owner().await;
    let res = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let cluster: Value = res.json().await.unwrap();
    assert_eq!(cluster["signing_key_id"], server.state.keys.key_id());
    assert_eq!(
        cluster["signing_key_id"],
        expected_keys(&server.data_dir)[0].1.as_str()
    );
    assert!(cluster.get("signing_key_rotation").is_none(), "{cluster}");
}

/// Section 7.3 step 4 (ADR 0297): every column of a node's row survives a
/// rebuild from the change log. The machine enrolls (its subnet), says
/// `hello` with a mesh key (its mesh address), a snapshot method and a
/// custody key, and says `hello` again with another custody key (its
/// `custody_key_changed_at`). Its `signing_key_id`, which no code path
/// writes yet, is set in the row before the first `hello`, whose entry
/// carries the whole row. Replaying the `node` entries in `seq` order into
/// an empty database gives the same rows, column for column, and the
/// newest whole-row entry names every column of `nodes`.
#[tokio::test]
async fn node_entries_rebuild_every_column_of_the_row() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-a", &["node"]).await;
    let mesh_key = STANDARD.encode([5u8; 32]);
    let signing_key_id = server.state.keys.key_id().to_string();
    sqlx::query("UPDATE nodes SET signing_key_id = ? WHERE id = ?")
        .bind(&signing_key_id)
        .bind(&node.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let full_hello = |key: &CustodyKey| {
        let mut message = hello(&node.node_id, Some(key.public_key().to_base64()));
        if let NodeMessage::Hello(hello) = &mut message {
            hello.mesh_public_key = Some(mesh_key.clone());
            hello.volume_snapshot = Some(wire::SnapshotMethod::Reflink);
        }
        message
    };
    let open = |message: NodeMessage| async {
        let mut socket = connect(&server, &node.credential).await;
        send_with_id(&mut socket, message, HELLO_ID).await;
        let mut frames = Vec::new();
        loop {
            let frame = recv_frame(&mut socket).await;
            let done = frame["type"] == "desired_state";
            frames.push(frame);
            if done {
                return (socket, frames);
            }
        }
    };

    // The first key binds; the session ends and the primary records it.
    let (socket, frames) = open(full_hello(&new_key())).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    drop(socket);
    let mut ended = false;
    for _ in 0..400 {
        let status: String = sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
            .bind(&node.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        if status == "disconnected" {
            ended = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ended, "the primary never recorded the first session's end");
    // Another key is refused and recorded; this session stays open, so no
    // session end writes the row while the test reads it.
    let (mut socket, frames) = open(full_hello(&new_key())).await;
    let errors = of_type(&frames, "error");
    assert_eq!(errors.len(), 1, "{frames:?}");
    assert_eq!(errors[0]["body"]["code"], "custody_key_changed");
    assert!(of_type(&ping(&mut socket, 1).await, "error").is_empty());

    #[allow(clippy::type_complexity)]
    let row: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT volume_snapshot, subnet, mesh_public_key, mesh_address, custody_public_key, \
         custody_key_changed_at, signing_key_id FROM nodes WHERE id = ?",
    )
    .bind(&node.node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some("reflink"));
    assert!(row.1.is_some(), "a subnet: {row:?}");
    assert_eq!(row.2.as_deref(), Some(mesh_key.as_str()));
    assert!(row.3.is_some(), "a mesh address: {row:?}");
    assert!(row.4.is_some(), "a bound custody key: {row:?}");
    assert!(row.5.is_some(), "a changed custody key: {row:?}");
    assert_eq!(row.6.as_deref(), Some(signing_key_id.as_str()));

    let rebuilt = replay::rebuild(&server, &["node"]).await;
    let columns = replay::column_names(&server.state.store, "nodes").await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM nodes")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    replay::assert_rebuilt(
        &server,
        &rebuilt,
        "nodes",
        &names,
        usize::try_from(count).unwrap(),
    )
    .await;

    // A whole-row entry names every column, so a column no write path sets
    // yet still travels once one does.
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM change_log WHERE entity_type = 'node' AND entity_id = ? \
         AND json_type(payload, '$.status') IS NOT NULL ORDER BY seq DESC LIMIT 1",
    )
    .bind(&node.node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    let mut logged: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    logged.sort_unstable();
    let mut expected = names.clone();
    expected.sort_unstable();
    assert_eq!(logged, expected);
}
