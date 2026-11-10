//! Node enrollment and the node-agent session over real HTTP and WebSocket
//! (`node-agent-protocol.md` sections 2-5, issue #62).

mod common;

use common::{
    Server, TLS_FINGERPRINT, problem, restart_with_resolver, start, start_with_patient_node_session,
};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::api::enroll::{ensure_local_join, local_join_token};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Health, Heartbeat, Kvm as WireKvm, LogAck, NodeMessage,
    ObservedStatus, StatusReport, VmStatus,
};
use quoro_proto::{ErrorCode, ProtocolError, jcs, sign};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const NODE_NAME: &str = "node-alpha";

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[7u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
}

fn enroll_body(token: &str, name: &str) -> Value {
    json!({
        "join_token": token,
        "name": name,
        "public_key": public_key(),
        "roles": ["node"],
        "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
        "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
    })
}

async fn create_token(server: &Server, owner: &reqwest::Client) -> Value {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create join token: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

async fn enroll(server: &Server, token: &str, name: &str) -> (u16, Value) {
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&enroll_body(token, name))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap())
}

struct Enrolled {
    node_id: String,
    credential: String,
    cluster_public_key: String,
}

async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    let token = create_token(server, owner).await;
    let (status, body) = enroll(server, token["token"].as_str().unwrap(), name).await;
    assert_eq!(status, 201, "enroll: {body}");
    Enrolled {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
        cluster_public_key: body["cluster_public_key"].as_str().unwrap().to_string(),
    }
}

fn hello(node_id: &str) -> NodeMessage {
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
        custody_public_key: None,
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

async fn send(socket: &mut Socket, message: NodeMessage) -> String {
    let id = "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D".to_string();
    let text = message.to_frame(&id, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
    id
}

async fn send_text(socket: &mut Socket, text: String) {
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

/// Sends the `heartbeat` a live node sends every `heartbeat_interval_ms`
/// (section 5.2), so the primary does not drop an idle session while the test
/// works over HTTP.
async fn heartbeat(socket: &mut Socket) {
    send(
        socket,
        NodeMessage::Heartbeat(Heartbeat {
            time: "2026-11-02T10:15:32Z".to_string(),
            busy: None,
        }),
    )
    .await;
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

async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

/// Sends `hello` and consumes `welcome` and the first `desired_state`.
async fn open(server: &Server, node: &Enrolled) -> (Socket, Value) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let welcome = recv_type(&mut socket, "welcome").await;
    assert_eq!(welcome["body"]["cluster_id"], server.state.cluster_id);
    assert_eq!(welcome["body"]["primary_node_id"], node.node_id);
    assert_eq!(welcome["body"]["heartbeat_interval_ms"], 200);
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
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

async fn vm_json(server: &Server, owner: &reqwest::Client, vm_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/vms/{vm_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn join_tokens_enroll_a_node_and_refuse_reuse() {
    let server = start().await;
    let owner = server.owner().await;

    let created = create_token(&server, &owner).await;
    let token = created["token"].as_str().unwrap();
    assert!(token.starts_with("njt_"), "{created}");
    assert!(quoro_types::is_id(
        quoro_types::Prefix::NodeJoinToken,
        created["id"].as_str().unwrap()
    ));
    let command = created["join_command"].as_str().unwrap();
    assert!(command.starts_with("quoro join https://primary.example:8443 --token "));
    assert!(command.contains(token));
    assert!(command.contains(TLS_FINGERPRINT));

    let (status, body) = enroll(&server, token, NODE_NAME).await;
    assert_eq!(status, 201, "{body}");
    let node_id = body["node_id"].as_str().unwrap();
    assert!(quoro_types::is_id(quoro_types::Prefix::Node, node_id));
    assert!(quoro_types::is_id(
        quoro_types::Prefix::Cluster,
        body["cluster_id"].as_str().unwrap()
    ));
    assert_eq!(body["primary_url"], "https://primary.example:8443");
    assert!(!body["credential"].as_str().unwrap().is_empty());
    assert_eq!(
        body["cluster_public_key"],
        server.state.keys.public_key_base64()
    );

    // The token is single use.
    let (status, body) = enroll(&server, token, "node-beta").await;
    assert_eq!(status, 409, "{body}");
    // An unknown token is a validation failure.
    let (status, body) = enroll(&server, "njt_unknown", "node-beta").await;
    assert_eq!(status, 422, "{body}");
    // A duplicate name is a conflict even with a fresh token.
    let fresh = create_token(&server, &owner).await;
    let (status, body) = enroll(&server, fresh["token"].as_str().unwrap(), NODE_NAME).await;
    assert_eq!(status, 409, "{body}");

    // An agent access token cannot create join tokens: M1 refuses `atk_` at
    // authentication, so it never reaches the human-only check.
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/join-tokens"))
        .bearer_auth("atk_01JC9Z6Q5W8N3K4T7R2M1XHB0D")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));
}

#[tokio::test]
async fn a_session_receives_welcome_and_a_verifiable_snapshot() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    // A bad credential never upgrades.
    let (status, code, _) = problem(
        server
            .client()
            .get(server.url("/api/v1/nodes/session"))
            .bearer_auth("not-a-credential")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));

    let (_socket, snapshot) = open(&server, &node).await;
    let body = &snapshot["body"];
    assert_eq!(body["rev"], 1);
    assert_eq!(body["term"], 1);
    assert_eq!(body["spec"]["network"]["subnet"], "10.42.1.0/24");
    assert_eq!(body["spec"]["network"]["gateway"], "10.42.1.1");
    assert!(body["spec"]["vms"].as_array().unwrap().is_empty());
    let cluster_key: sign::VerifyingKey =
        sign::public_key_from_base64(&node.cluster_public_key).unwrap();
    sign::verify_body(&cluster_key, body).expect("the desired state is signed by the cluster key");

    let node_json = node_json(&server, &owner, &node.node_id).await;
    assert_eq!(node_json["status"], "connected");
    assert_eq!(node_json["agent_version"], "test-agent");
    assert_eq!(node_json["engine_version"], "test-engine");
    assert_eq!(node_json["capacity"]["cpus"], 8);
    let (events,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM events WHERE type = 'node.connected' AND json_extract(subject, '$.id') = ?",
    )
    .bind(&node.node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(events, 1);
}

#[tokio::test]
async fn vm_changes_push_snapshots_and_observations_land() {
    // Not a timeout test: the fake node must not lose its session while the
    // test creates a project and a VM and polls the API.
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let (mut socket, first) = open(&server, &node).await;

    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "Demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap();
    // The node keeps the session alive while the test works (5.2).
    heartbeat(&mut socket).await;

    let res = owner
        .post(server.url("/api/v1/vms"))
        .json(&json!({
            "project_id": project_id,
            "node_id": node.node_id,
            "name": "web",
            "vcpus": 2,
            "memory_mb": 512,
            "disk_gb": 4,
            "image": "ghcr.io/example/rootfs:1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create vm: {}",
        res.text().await.unwrap()
    );
    let vm: Value = res.json().await.unwrap();
    let vm_id = vm["id"].as_str().unwrap();
    heartbeat(&mut socket).await;

    let second = recv_type(&mut socket, "desired_state").await;
    let second_rev = second["body"]["rev"].as_u64().unwrap();
    assert!(second_rev > first["body"]["rev"].as_u64().unwrap());
    let vms = second["body"]["spec"]["vms"].as_array().unwrap();
    assert_eq!(vms.len(), 1);
    assert_eq!(vms[0]["vm_id"], vm_id);
    assert_eq!(vms[0]["image_digest"], "ghcr.io/example/rootfs:1");
    assert_eq!(vms[0]["desired_status"], "running");

    send(
        &mut socket,
        NodeMessage::StatusReport(StatusReport {
            applied_rev: second_rev,
            vms: vec![VmStatus {
                vm_id: vm_id.to_string(),
                observed_status: ObservedStatus::Running,
                health: Health::Passing,
                ip: Some("10.42.1.7".to_string()),
                restart_count: 0,
                last_exit_code: None,
            }],
            routes: Vec::new(),
            capacity: WireCapacity {
                cpus: 8,
                memory_mb: 16_384,
                disk_gb: 100,
            },
            allocated: WireCapacity {
                cpus: 2,
                memory_mb: 512,
                disk_gb: 4,
            },
            disk_free_gb: 90.0,
            builds: Vec::new(),
            edge_backends: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: Vec::new(),
            volumes: Vec::new(),
            quarantines: Vec::new(),
        }),
    )
    .await;

    let bean = wait_for_vm(&server, &owner, &mut socket, vm_id, "running").await;
    assert_eq!(bean["ip"], "10.42.1.7");

    let res = owner
        .delete(server.url(&format!("/api/v1/vms/{vm_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    heartbeat(&mut socket).await;
    let third = recv_type(&mut socket, "desired_state").await;
    assert!(third["body"]["rev"].as_u64().unwrap() > second_rev);
    assert!(third["body"]["spec"]["vms"].as_array().unwrap().is_empty());
}

/// Polls the VM until it reports `status`, sending a heartbeat on every round so
/// the fake node's session never goes idle. Panics after thirty seconds.
async fn wait_for_vm(
    server: &Server,
    owner: &reqwest::Client,
    socket: &mut Socket,
    vm_id: &str,
    status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let vm = vm_json(server, owner, vm_id).await;
        if vm["observed_status"] == status {
            return vm;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "VM {vm_id} did not become {status}"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn silence_disconnects_and_a_new_session_supersedes() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    // Silence: no frame within the 800 ms session timeout. This test starts the
    // server with the real short timeout on purpose.
    let (mut silent, _) = open(&server, &node).await;
    let _ = timeout(Duration::from_secs(10), silent.next()).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let current = node_json(&server, &owner, &node.node_id).await;
        if current["status"] == "disconnected" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the idle node is not disconnected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let current = node_json(&server, &owner, &node.node_id).await;
    assert_eq!(current["status"], "disconnected");
    let (events,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM events WHERE type = 'node.disconnected' AND json_extract(subject, '$.id') = ?",
    )
    .bind(&node.node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(events, 1);
    drop(silent);

    // Two live sessions: the second supersedes the first, and the first must
    // not mark the node disconnected when it closes.
    let (mut first, _) = open(&server, &node).await;
    let (mut second, _) = open(&server, &node).await;
    let first_closed = timeout(Duration::from_secs(10), async {
        loop {
            match first.next().await {
                None | Some(Err(_)) | Some(Ok(WsMessage::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(first_closed.is_ok(), "the superseded socket closes");

    let current = node_json(&server, &owner, &node.node_id).await;
    assert_eq!(current["status"], "connected");
    // The surviving session still receives its own snapshots.
    send(&mut second, NodeMessage::Ping).await;
    recv_type(&mut second, "pong").await;
}

#[tokio::test]
async fn the_local_bootstrap_token_enrolls_the_primary() {
    // A fresh cluster: the file is written mode 0600 and consumed once.
    let server = start().await;
    ensure_local_join(
        &server.data_dir,
        "https://primary.local:8443",
        TLS_FINGERPRINT,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(server.data_dir.join("local-node-join.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let token = local_join_token(&server.data_dir).unwrap();
    let (status, body) = enroll(&server, &token, "primary").await;
    assert_eq!(status, 201, "{body}");
    let node_id = body["node_id"].as_str().unwrap();
    let (cp_status,): (Option<String>,) =
        sqlx::query_as("SELECT cp_status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(cp_status.as_deref(), Some("primary"));
    assert!(!server.data_dir.join("local-node-join.json").exists());

    // An existing v0.0.1 primary row is adopted, never duplicated.
    let server = start().await;
    let legacy = driver::register_local_node(
        &server.state.store,
        &LocalNode {
            name: "legacy-primary".to_string(),
            capacity: Capacity {
                cpus: 4,
                memory_mb: 8_192,
                disk_gb: 50,
            },
            kvm: Kvm {
                available: true,
                kernel: "6.8.0".to_string(),
                nested: None,
            },
            engine_version: "old-engine".to_string(),
            agent_version: "old-agent".to_string(),
        },
    )
    .await
    .unwrap();
    ensure_local_join(
        &server.data_dir,
        "https://primary.local:8443",
        TLS_FINGERPRINT,
    )
    .unwrap();
    let token = local_join_token(&server.data_dir).unwrap();
    let (status, body) = enroll(&server, &token, "renamed-primary").await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["node_id"], legacy);
    let (name, cp_status): (String, Option<String>) =
        sqlx::query_as("SELECT name, cp_status FROM nodes WHERE id = ?")
            .bind(&legacy)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(name, "legacy-primary", "the adopted name is kept");
    assert_eq!(cp_status.as_deref(), Some("primary"));
    let (primaries, credentials): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM nodes WHERE cp_status = 'primary'), \
         (SELECT count(*) FROM node_credentials)",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!((primaries, credentials), (1, 1));
    assert!(!server.data_dir.join("local-node-join.json").exists());
}

#[tokio::test]
async fn protocol_errors_are_non_fatal_except_an_unsupported_version() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let (mut socket, _) = open(&server, &node).await;

    // An unknown type is answered with `bad_message`, not a closed session.
    send_text(
        &mut socket,
        json!({
            "v": 1,
            "type": "teleport",
            "id": "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
            "body": {}
        })
        .to_string(),
    )
    .await;
    let error = recv_type(&mut socket, "error").await;
    assert_eq!(error["body"]["code"], "bad_message");
    assert_eq!(error["body"]["fatal"], false);
    assert_eq!(error["re"], "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D");

    // The session is still usable.
    let ping = "msg_01JC9Z7A4M8Q2V6B3N5T9R1KXW";
    send_text(
        &mut socket,
        json!({ "v": 1, "type": "ping", "id": ping, "body": {} }).to_string(),
    )
    .await;
    let pong = recv_type(&mut socket, "pong").await;
    assert_eq!(pong["re"], ping);

    // An unsupported major version is fatal.
    send_text(
        &mut socket,
        json!({ "v": 2, "type": "heartbeat", "id": "msg_x", "body": {} }).to_string(),
    )
    .await;
    let error = recv_type(&mut socket, "error").await;
    assert_eq!(error["body"]["code"], "unsupported_version");
    assert_eq!(error["body"]["fatal"], true);
    let closed = timeout(Duration::from_secs(10), async {
        loop {
            match socket.next().await {
                None | Some(Err(_)) | Some(Ok(WsMessage::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "the session ends");
}

#[tokio::test]
async fn a_restarted_primary_does_not_keep_nodes_connected() {
    // The previous process left a node `connected` (an M1 primary row is
    // written that way); a new process has no session for it yet.
    let server = start().await;
    let node_id = driver::register_local_node(
        &server.state.store,
        &LocalNode {
            name: "left-connected".to_string(),
            capacity: Capacity {
                cpus: 1,
                memory_mb: 1_024,
                disk_gb: 10,
            },
            kvm: Kvm {
                available: true,
                kernel: "6.8.0".to_string(),
                nested: None,
            },
            engine_version: "engine".to_string(),
            agent_version: "agent".to_string(),
        },
    )
    .await
    .unwrap();
    let status = || async {
        sqlx::query_scalar::<_, String>("SELECT status FROM nodes WHERE id = ?")
            .bind(&node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap()
    };
    assert_eq!(status().await, "connected");

    quoro_control_plane::AppState::open(
        server.state.store.clone(),
        (*server.state.config).clone(),
        std::sync::Arc::new(quoro_control_plane::ReferenceAsDigest),
        std::sync::Arc::clone(&server.state.data_key),
    )
    .await
    .unwrap();
    assert_eq!(status().await, "disconnected");
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE type = 'node.disconnected' AND json_extract(subject, '$.id') = ?",
    )
    .bind(&node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(events, 1);
}

#[tokio::test]
async fn the_welcome_carries_the_advertised_urls() {
    let server = start().await;
    let mut advertised = vec!["http://plain.example:8443".to_string()];
    advertised.push("https://primary.example:8443".to_string());
    advertised.push("https://a.example:8443".to_string());
    advertised.push("https://a.example:8443".to_string());
    for index in 0..10 {
        advertised.push(format!("https://h{index}.example:8443"));
    }
    // The provider lives on the shared hub, so setting it on a clone reaches
    // the router that is already serving.
    let _ = server
        .state
        .clone()
        .with_advertised_urls(move || advertised.clone());

    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-advertised").await;
    let mut socket = connect(&server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let welcome = recv_type(&mut socket, "welcome").await;

    let urls = welcome["body"]["primary_urls"]
        .as_array()
        .unwrap_or_else(|| panic!("no primary_urls: {welcome}"));
    let expected: Vec<Value> = std::iter::once("https://primary.example:8443".to_string())
        .chain(std::iter::once("https://a.example:8443".to_string()))
        .chain((0..6).map(|index| format!("https://h{index}.example:8443")))
        .map(Value::String)
        .collect();
    assert_eq!(urls, &expected, "{welcome}");
}

#[tokio::test]
async fn a_hello_mesh_key_is_stored_with_a_mesh_address() {
    let server = start().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, "node-mesh").await;
    let key = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";
    let mut socket = connect(&server, &node.credential).await;
    let NodeMessage::Hello(mut body) = hello(&node.node_id) else {
        unreachable!("hello() builds a hello");
    };
    body.mesh_public_key = Some(key.to_string());
    send(&mut socket, NodeMessage::Hello(body)).await;
    let _welcome = recv_type(&mut socket, "welcome").await;

    let (stored, address): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT mesh_public_key, mesh_address FROM nodes WHERE id = ?")
            .bind(&node.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(stored.as_deref(), Some(key));
    assert_eq!(address.as_deref(), Some("10.43.0.1"));
}

/// Sections 2.3, 5.1 and 7.2: from M5b the enroll request and every `hello`
/// of a machine with the `node` role carry `custody_public_key`. A primary
/// that does not bind the key yet accepts both messages, and the session
/// starts as it did without the member (section 3).
#[tokio::test]
async fn an_enroll_and_a_hello_with_a_custody_key_are_accepted() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let key = "hSDwCYkwp1R0i33ctD73Wg2/Og0mOBr066SpjqqbTmo=";
    let token = create_token(&server, &owner).await;
    let mut body = enroll_body(token["token"].as_str().unwrap(), "node-custody");
    body["custody_public_key"] = json!(key);
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let enrolled: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "enroll: {enrolled}");
    let node_id = enrolled["node_id"].as_str().unwrap();

    let mut socket = connect(&server, enrolled["credential"].as_str().unwrap()).await;
    let NodeMessage::Hello(mut body) = hello(node_id) else {
        unreachable!("hello() builds a hello");
    };
    body.custody_public_key = Some(key.to_string());
    send(&mut socket, NodeMessage::Hello(body)).await;
    let first = recv_frame(&mut socket).await;
    assert_eq!(first["type"], "welcome", "{first}");
    let _snapshot = recv_type(&mut socket, "desired_state").await;
    let node = node_json(&server, &owner, node_id).await;
    assert_eq!(node["status"], "connected", "{node}");
}

/// Enrolls a machine with an explicit role set. A machine without the `node`
/// role sends no KVM probe, as an edge does (D-0143).
async fn enrolled_with_roles(
    server: &Server,
    owner: &reqwest::Client,
    name: &str,
    roles: &[&str],
) -> Enrolled {
    let token = create_token(server, owner).await;
    let mut body = enroll_body(token["token"].as_str().unwrap(), name);
    body["roles"] = json!(roles);
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
        cluster_public_key: body["cluster_public_key"].as_str().unwrap().to_string(),
    }
}

#[tokio::test]
async fn an_edge_reports_backend_health_and_a_replicated_app_gets_degraded() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let edge = enrolled_with_roles(&server, &owner, "edge-1", &["edge"]).await;
    // An edge-only machine needs no guest subnet and no KVM (D-0143).
    let (subnet,): (Option<String>,) = sqlx::query_as("SELECT subnet FROM nodes WHERE id = ?")
        .bind(&edge.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert!(subnet.is_none(), "an edge machine has no subnet");

    // Two placement nodes, never connected: they only count for eligibility.
    let first = enrolled_with_roles(&server, &owner, "node-one", &["node"]).await;
    let second = enrolled_with_roles(&server, &owner, "node-two", &["node"]).await;

    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "edge", "name": "Edge" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "image": "img@sha256:1",
            "port": 8080,
            "replicas": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    let app: Value = res.json().await.unwrap();
    let app_id = app["id"].as_str().unwrap().to_string();

    let (mut socket, snapshot) = open(&server, &edge).await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let first_vm = "vm_00000000000000000000000001";
    let second_vm = "vm_00000000000000000000000002";
    send(
        &mut socket,
        NodeMessage::StatusReport(StatusReport {
            applied_rev: rev,
            vms: Vec::new(),
            routes: Vec::new(),
            capacity: WireCapacity {
                cpus: 2,
                memory_mb: 2_048,
                disk_gb: 20,
            },
            allocated: WireCapacity::default(),
            disk_free_gb: 18.0,
            builds: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: Vec::new(),
            volumes: Vec::new(),
            edge_backends: vec![
                wire::EdgeBackendStatus {
                    app_id: app_id.clone(),
                    vm_id: first_vm.to_string(),
                    node_id: first.node_id.clone(),
                    in_rotation: true,
                    since: "2026-11-04T10:00:00Z".to_string(),
                    reason: None,
                    last_error: None,
                },
                wire::EdgeBackendStatus {
                    app_id: app_id.clone(),
                    vm_id: second_vm.to_string(),
                    node_id: second.node_id.clone(),
                    in_rotation: false,
                    since: "2026-11-04T10:00:05Z".to_string(),
                    reason: Some(wire::EdgeOutReason::HealthCheck),
                    last_error: Some("connection refused".to_string()),
                },
            ],
            quarantines: Vec::new(),
        }),
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let detail = loop {
        let res = owner
            .get(server.url(&format!("/api/v1/apps/{app_id}")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let detail: Value = res.json().await.unwrap();
        if detail["edge_backends"]
            .as_array()
            .is_some_and(|b| b.len() == 2)
        {
            break detail;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the edge's report never reached AppDetail: {detail}"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let backends = detail["edge_backends"].as_array().unwrap();
    let down = backends
        .iter()
        .find(|backend| backend["vm_id"] == second_vm)
        .expect("the out-of-rotation backend is listed");
    assert_eq!(down["edge_node_id"], edge.node_id);
    assert_eq!(down["node_id"], second.node_id);
    assert_eq!(down["in_rotation"], false);
    assert_eq!(down["reason"], "health_check");
    assert_eq!(down["last_error"], "connection refused");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let degraded = loop {
        let (degraded,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM events WHERE type = 'app.degraded' \
             AND json_extract(subject, '$.id') = ?",
        )
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        if degraded > 0 {
            break degraded;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no app.degraded event was emitted"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(degraded, 1);
    // `app.degraded` opens no incident (ADR 0143).
    let (no_incident,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM incidents WHERE json_extract(subject, '$.id') = ?")
            .bind(&app_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(no_incident, 0);

    // Every backend out is `app.down` with `check: edge_health`, and that one
    // opens the app's incident as any other `app.down` does (ADR 0143).
    send(
        &mut socket,
        NodeMessage::StatusReport(StatusReport {
            applied_rev: rev,
            vms: Vec::new(),
            routes: Vec::new(),
            capacity: WireCapacity {
                cpus: 2,
                memory_mb: 2_048,
                disk_gb: 20,
            },
            allocated: WireCapacity::default(),
            disk_free_gb: 18.0,
            builds: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: Vec::new(),
            volumes: Vec::new(),
            edge_backends: vec![
                wire::EdgeBackendStatus {
                    app_id: app_id.clone(),
                    vm_id: first_vm.to_string(),
                    node_id: first.node_id.clone(),
                    in_rotation: false,
                    since: "2026-11-04T10:05:00Z".to_string(),
                    reason: Some(wire::EdgeOutReason::ConnectFailed),
                    last_error: Some("connection reset".to_string()),
                },
                wire::EdgeBackendStatus {
                    app_id: app_id.clone(),
                    vm_id: second_vm.to_string(),
                    node_id: second.node_id.clone(),
                    in_rotation: false,
                    since: "2026-11-04T10:00:05Z".to_string(),
                    reason: Some(wire::EdgeOutReason::HealthCheck),
                    last_error: Some("connection refused".to_string()),
                },
            ],
            quarantines: Vec::new(),
        }),
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let incidents = loop {
        let (down,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM events WHERE type = 'app.down' \
             AND json_extract(subject, '$.id') = ? AND json_extract(data, '$.check') = 'edge_health'",
        )
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        let (incidents,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM incidents WHERE status = 'open' \
             AND json_extract(subject, '$.id') = ?",
        )
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        if down > 0 && incidents > 0 {
            break incidents;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no app.down event or incident for the all-out report"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(incidents, 1);
}

/// A `status_report` with one VM, as a node sends it after `desired_state`.
fn report_one(applied_rev: u64, vm_id: &str, status: ObservedStatus) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev,
        vms: vec![VmStatus {
            vm_id: vm_id.to_string(),
            observed_status: status,
            health: Health::Passing,
            ip: Some("10.42.1.7".to_string()),
            restart_count: 0,
            last_exit_code: None,
        }],
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        allocated: WireCapacity {
            cpus: 1,
            memory_mb: 256,
            disk_gb: 1,
        },
        disk_free_gb: 90.0,
        builds: Vec::new(),
        edge_backends: Vec::new(),
        backups: Vec::new(),
        images: Vec::new(),
        warm: Vec::new(),
        volumes: Vec::new(),
        quarantines: Vec::new(),
    })
}

/// How the API shows `vm_id` through each read path: `GET /vms/{id}`, the
/// node's `GET /vms` page and the app's `instances`.
async fn observed_everywhere(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
    node_id: &str,
    vm_id: &str,
) -> [String; 3] {
    let one = vm_json(server, owner, vm_id).await;
    let page: Value = owner
        .get(server.url(&format!("/api/v1/vms?node_id={node_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let listed = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vm| vm["id"] == vm_id)
        .expect("the VM is listed");
    let instance = app["instances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vm| vm["id"] == vm_id)
        .expect("the VM is an instance");
    [
        one["observed_status"].as_str().unwrap().to_string(),
        listed["observed_status"].as_str().unwrap().to_string(),
        instance["observed_status"].as_str().unwrap().to_string(),
    ]
}

/// The number of `vm.*` events, and the status the database row holds.
async fn vm_facts(server: &Server, vm_id: &str) -> (i64, String) {
    let (events,): (i64,) = sqlx::query_as("SELECT count(*) FROM events WHERE type LIKE 'vm.%'")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let (stored,): (String,) = sqlx::query_as("SELECT observed_status FROM vms WHERE id = ?")
        .bind(vm_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    (events, stored)
}

#[tokio::test]
async fn an_instance_on_a_disconnected_node_reads_unknown_until_its_node_reports() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let (mut socket, _) = open(&server, &node).await;

    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "Demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap();
    heartbeat(&mut socket).await;
    let image = "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "image": image,
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    let app: Value = res.json().await.unwrap();
    let app_id = app["id"].as_str().unwrap().to_string();
    heartbeat(&mut socket).await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": image }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "create deployment: {}",
        res.text().await.unwrap()
    );
    heartbeat(&mut socket).await;

    // The node gets the instance and reports it running.
    let snapshot = loop {
        let snapshot = recv_type(&mut socket, "desired_state").await;
        if !snapshot["body"]["spec"]["vms"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            break snapshot;
        }
    };
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    let vm_id = snapshot["body"]["spec"]["vms"][0]["vm_id"]
        .as_str()
        .unwrap()
        .to_string();
    send(
        &mut socket,
        report_one(rev, &vm_id, ObservedStatus::Running),
    )
    .await;
    wait_for_vm(&server, &owner, &mut socket, &vm_id, "running").await;
    assert_eq!(
        observed_everywhere(&server, &owner, &app_id, &node.node_id, &vm_id).await,
        ["running", "running", "running"]
    );
    let (events, stored) = vm_facts(&server, &vm_id).await;
    assert_eq!(stored, "running");

    // The node's session ends. Its instance is no longer known to be running.
    socket.close(None).await.unwrap();
    drop(socket);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node_json(&server, &owner, &node.node_id).await["status"] != "disconnected" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node is not disconnected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        observed_everywhere(&server, &owner, &app_id, &node.node_id, &vm_id).await,
        ["unknown", "unknown", "unknown"],
        "an instance on a disconnected node reads unknown in every read path"
    );
    // Only what the API shows changed: the row keeps the node's last report,
    // and no `vm.*` event was emitted.
    assert_eq!(
        vm_facts(&server, &vm_id).await,
        (events, "running".to_string())
    );

    // The node comes back and reports again: the instance reads running, and
    // still no `vm.*` event, since nothing about the VM changed.
    let (mut socket, snapshot) = open(&server, &node).await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        report_one(rev, &vm_id, ObservedStatus::Running),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = observed_everywhere(&server, &owner, &app_id, &node.node_id, &vm_id).await;
        if seen == ["running", "running", "running"] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reconnected node's instance reads {seen:?}"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        vm_facts(&server, &vm_id).await,
        (events, "running".to_string())
    );
}

/// The digest a registry tag resolves to once it has moved upstream.
const MOVED_DIGEST: &str =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// A registry whose tags all moved to [`MOVED_DIGEST`].
struct MovedTag;

impl quoro_control_plane::DigestResolver for MovedTag {
    fn resolve<'a>(
        &'a self,
        _reference: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
    {
        Box::pin(async { Ok(MOVED_DIGEST.to_string()) })
    }
}

/// The `spec.vms` entry of `vm_id` in a `desired_state` frame.
fn spec_vm<'a>(snapshot: &'a Value, vm_id: &str) -> Option<&'a Value> {
    snapshot["body"]["spec"]["vms"]
        .as_array()?
        .iter()
        .find(|vm| vm["vm_id"] == vm_id)
}

/// Issue #354: a VM keeps the digest it was created with. A restarted primary
/// starts with an empty digest cache; when the app's tag moved meanwhile, the
/// running instance must not be sent the tag's new digest. Only a new
/// deployment, with a new VM id, resolves the tag again.
#[tokio::test]
async fn a_restarted_primary_keeps_an_instance_on_its_create_time_digest() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let (mut socket, _) = open(&server, &node).await;

    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "Demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap();
    heartbeat(&mut socket).await;
    let image = "ghcr.io/example/web:latest";
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "image": image,
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    let app: Value = res.json().await.unwrap();
    let app_id = app["id"].as_str().unwrap().to_string();
    heartbeat(&mut socket).await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": image }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "deploy: {}", res.text().await.unwrap());

    // The instance is created with the digest the tag had then.
    let snapshot = loop {
        heartbeat(&mut socket).await;
        let snapshot = recv_type(&mut socket, "desired_state").await;
        if !snapshot["body"]["spec"]["vms"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            break snapshot;
        }
    };
    let vm_id = snapshot["body"]["spec"]["vms"][0]["vm_id"]
        .as_str()
        .unwrap()
        .to_string();
    let created = snapshot["body"]["spec"]["vms"][0]["image_digest"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(created, MOVED_DIGEST);
    // The deployment records it with that first snapshot.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let recorded: Option<String> =
            sqlx::query_scalar("SELECT image_digest FROM deployments WHERE app_id = ?")
                .bind(&app_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        if recorded.as_deref() == Some(created.as_str()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the deployment did not record {created}: {recorded:?}"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The instance runs, so the deployment succeeds.
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        report_one(rev, &vm_id, ObservedStatus::Running),
    )
    .await;
    wait_for_vm(&server, &owner, &mut socket, &vm_id, "running").await;

    // The primary restarts while upstream has moved the tag.
    socket.close(None).await.unwrap();
    drop(socket);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while node_json(&server, &owner, &node.node_id).await["status"] != "disconnected" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node is not disconnected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let restarted = restart_with_resolver(&server, std::sync::Arc::new(MovedTag)).await;
    let (mut socket, snapshot) = open(&restarted, &node).await;
    let vm = spec_vm(&snapshot, &vm_id).expect("the instance is still desired");
    assert_eq!(
        vm["image_digest"], created,
        "the restarted primary moved a running instance to the tag's new digest"
    );
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        report_one(rev, &vm_id, ObservedStatus::Running),
    )
    .await;

    // A new deployment creates a new VM id, and only that resolves the tag
    // again; the running instance keeps its digest meanwhile.
    let owner = restarted.client();
    restarted
        .login(&owner, common::OWNER_EMAIL, common::OWNER_PASSWORD)
        .await;
    let res = owner
        .post(restarted.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": image }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "redeploy: {}", res.text().await.unwrap());
    let snapshot = loop {
        heartbeat(&mut socket).await;
        let snapshot = recv_type(&mut socket, "desired_state").await;
        if snapshot["body"]["spec"]["vms"].as_array().unwrap().len() == 2 {
            break snapshot;
        }
    };
    assert_eq!(spec_vm(&snapshot, &vm_id).unwrap()["image_digest"], created);
    let new_vm = snapshot["body"]["spec"]["vms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vm| vm["vm_id"] != vm_id.as_str())
        .unwrap();
    assert_eq!(new_vm["image_digest"], MOVED_DIGEST);
}

// ---------------------------------------------------------------------------
// Change-log follow (sections 5.6, 5.14 and 7.1, ADR 0227)
// ---------------------------------------------------------------------------

/// 64 zeros: the `log_hash` of an empty copy and the `prev_hash` of `seq` 1.
fn genesis() -> String {
    "0".repeat(64)
}

fn hello_at(node_id: &str, log_seq: u64, log_hash: &str) -> NodeMessage {
    let NodeMessage::Hello(mut hello) = hello(node_id) else {
        panic!("hello() builds a hello");
    };
    hello.log_seq = log_seq;
    hello.log_hash = log_hash.to_string();
    NodeMessage::Hello(hello)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The primary's newest change-log `seq`.
async fn log_head(server: &Server) -> u64 {
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    u64::try_from(head.unwrap_or(0)).unwrap()
}

/// The primary's `hash` at `seq`.
async fn log_hash_at(server: &Server, seq: u64) -> String {
    sqlx::query_scalar("SELECT hash FROM change_log WHERE seq = ?")
        .bind(i64::try_from(seq).unwrap())
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

async fn stored_log_seq(server: &Server, node_id: &str) -> u64 {
    let seq: i64 = sqlx::query_scalar("SELECT log_seq FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    u64::try_from(seq).unwrap()
}

/// Writes `nodes.log_seq` directly, as a session's end write would have.
async fn set_stored_log_seq(server: &Server, node_id: &str, seq: u64) {
    sqlx::query("UPDATE nodes SET log_seq = ? WHERE id = ?")
        .bind(i64::try_from(seq).unwrap())
        .bind(node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

/// Waits until the primary has recorded the end of the node's session, and
/// with it the end write of `nodes.log_seq`.
async fn wait_disconnected(server: &Server, owner: &reqwest::Client, node_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node_json(server, owner, node_id).await["status"] == "disconnected" {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node is not disconnected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Appends `count` change-log entries in one write, as any state change does;
/// the entity ids are `<tag>-0`, `<tag>-1` and so on.
async fn append_entries(server: &Server, tag: &str, count: usize) {
    let mut w = server.state.store.begin_write().await.unwrap();
    for i in 0..count {
        changelog::append(
            &mut w.tx,
            &Actor::system("test"),
            "probe",
            &format!("{tag}-{i}"),
            Op::Create,
            json!({ "n": i }),
        )
        .await
        .unwrap();
    }
    w.commit().await.unwrap();
}

fn of_type<'a>(frames: &'a [Value], kind: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == kind)
        .collect()
}

/// Sends `hello` and reads every frame up to the first `log_entries`, which
/// must come after the session's first `desired_state` (section 4.2 rule 6).
async fn follow_from(server: &Server, node: &Enrolled, hello: NodeMessage) -> (Socket, Vec<Value>) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello).await;
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(&mut socket).await;
        let done = frame["type"] == "log_entries";
        frames.push(frame);
        if done {
            break;
        }
    }
    let snapshot = frames
        .iter()
        .position(|frame| frame["type"] == "desired_state");
    assert!(
        snapshot.is_some_and(|index| index < frames.len() - 1),
        "log_entries before the desired_state: {frames:?}"
    );
    (socket, frames)
}

/// Checks one `log_entries` body as a node does (section 7.1,
/// "Verification"): its entries run without a gap from `from_seq`, each has
/// exactly the 11 fields of section 5.14, hashes to its `hash` (SHA-256 over
/// the RFC 8785 form of the entry without `hash`) and links to the hash
/// before it, starting from `prev_hash`; `head_seq` is at least the last
/// `seq`. Returns the last entry's `seq` and `hash`.
fn check_entries(body: &Value, prev_hash: &str) -> (u64, String) {
    let from_seq = body["from_seq"].as_u64().unwrap();
    let entries = body["entries"].as_array().unwrap();
    assert!(!entries.is_empty() && entries.len() <= 500, "{body}");
    let mut prev = prev_hash.to_string();
    let mut seq = from_seq;
    for entry in entries {
        let mut fields = entry.as_object().unwrap().clone();
        assert_eq!(fields.len(), 11, "{entry}");
        assert_eq!(entry["seq"], seq, "{entry}");
        assert_eq!(entry["prev_hash"], prev.as_str(), "{entry}");
        let hash = fields.remove("hash").unwrap();
        let canonical = jcs::canonicalize(&Value::Object(fields)).unwrap();
        prev = sha256_hex(canonical.as_bytes());
        assert_eq!(hash, prev.as_str(), "{entry}");
        seq += 1;
    }
    let last = seq - 1;
    assert!(body["head_seq"].as_u64().unwrap() >= last, "{body}");
    (last, prev)
}

async fn ack(socket: &mut Socket, seq: u64, hash: &str) {
    send(
        socket,
        NodeMessage::LogAck(LogAck {
            seq,
            hash: hash.to_string(),
        }),
    )
    .await;
}

/// Pings twice and returns every frame that came before the answers: what
/// the primary queued before it handled them.
async fn settle(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    for round in 0..2 {
        let id = format!("msg_settle{round:020}");
        send_text(socket, NodeMessage::Ping.to_frame(&id, None).to_text()).await;
        loop {
            let frame = recv_frame(socket).await;
            if frame["type"] == "pong" && frame["re"] == id.as_str() {
                break;
            }
            frames.push(frame);
        }
    }
    frames
}

/// How many `log_entries` messages [`catch_up`] acknowledges before it
/// fails, so an endless stream fails the test instead of hanging it.
const CATCH_UP_MESSAGES: usize = 50;

/// Acknowledges `log_entries` until the primary sends none: the session has
/// caught up. Returns the newest acknowledged `seq` and `hash`. Panics after
/// [`CATCH_UP_MESSAGES`] messages.
async fn catch_up(socket: &mut Socket, mut last: u64, mut hash: String) -> (u64, String) {
    for _ in 0..CATCH_UP_MESSAGES {
        let settled = settle(socket).await;
        let messages = of_type(&settled, "log_entries");
        assert!(
            messages.len() <= 1,
            "more than one outstanding: {settled:?}"
        );
        let Some(message) = messages.first() else {
            return (last, hash);
        };
        assert_eq!(message["body"]["from_seq"], last + 1, "{message}");
        (last, hash) = check_entries(&message["body"], &hash);
        ack(socket, last, &hash).await;
    }
    panic!("still not caught up after {CATCH_UP_MESSAGES} log_entries messages, at seq {last}");
}

#[tokio::test]
async fn a_node_follows_the_change_log_from_seq_1_after_its_desired_state() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    let (_socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    let body = &frames.last().unwrap()["body"];
    assert_eq!(body["from_seq"], 1);
    let (last, _) = check_entries(body, &genesis());
    // Fewer than 500 entries exist, so one message carries them all, up to
    // the head it names.
    assert_eq!(body["head_seq"], last);
    assert!(last <= log_head(&server).await);
}

#[tokio::test]
async fn one_log_entries_message_is_outstanding_until_its_ack() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 600).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let first = &frames.last().unwrap()["body"];
    assert_eq!(first["from_seq"], 1);
    assert_eq!(first["entries"].as_array().unwrap().len(), 500);
    assert!(
        first["head_seq"].as_u64().unwrap() > 600,
        "{}",
        first["head_seq"]
    );
    let (last, hash) = check_entries(first, &genesis());
    assert_eq!(last, 500);

    // Nothing more goes out while it is unacknowledged, not even for a new
    // write.
    append_entries(&server, "more", 1).await;
    let settled = settle(&mut socket).await;
    assert!(of_type(&settled, "log_entries").is_empty(), "{settled:?}");

    ack(&mut socket, last, &hash).await;
    let next = recv_type(&mut socket, "log_entries").await;
    assert_eq!(next["body"]["from_seq"], 501);
    check_entries(&next["body"], &hash);
}

#[tokio::test]
async fn an_ack_that_skips_or_does_not_match_is_ignored() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 600).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let first = &frames.last().unwrap()["body"];
    let (last, hash) = check_entries(first, &genesis());
    assert_eq!(last, 500);
    let before_last = first["entries"][498]["hash"].as_str().unwrap().to_string();

    // Not the last entry, the wrong hash, and a seq past the message.
    ack(&mut socket, 499, &before_last).await;
    ack(&mut socket, 500, &"f".repeat(64)).await;
    ack(&mut socket, 501, &log_hash_at(&server, 501).await).await;
    let settled = settle(&mut socket).await;
    assert!(of_type(&settled, "log_entries").is_empty(), "{settled:?}");

    // The right ack still moves on from 501.
    ack(&mut socket, last, &hash).await;
    let next = recv_type(&mut socket, "log_entries").await;
    assert_eq!(next["body"]["from_seq"], 501);
}

#[tokio::test]
async fn a_write_after_catch_up_is_sent_through_the_head_watch() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let (last, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    ack(&mut socket, last, &hash).await;
    let (mut last, mut hash) = catch_up(&mut socket, last, hash).await;

    // Nothing but the write wakes the session: no ack is outstanding.
    append_entries(&server, "after-catch-up", 1).await;
    let delivered = timeout(Duration::from_secs(30), async {
        loop {
            let message = recv_type(&mut socket, "log_entries").await;
            assert_eq!(message["body"]["from_seq"], last + 1, "{message}");
            let (seq, seq_hash) = check_entries(&message["body"], &hash);
            if message["body"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["entity_id"] == "after-catch-up-0")
            {
                break;
            }
            // Another write came first: acknowledged, the next goes out.
            ack(&mut socket, seq, &seq_hash).await;
            (last, hash) = (seq, seq_hash);
        }
    })
    .await;
    assert!(delivered.is_ok(), "the new entry was not sent within 30 s");
}

/// The frames of a session whose `hello` position differs: exactly one
/// `log_diverged`, non-fatal, after the `desired_state` and before the
/// `log_entries`, which start at `seq` 1.
fn check_diverged(frames: &[Value]) {
    let snapshot = frames
        .iter()
        .position(|frame| frame["type"] == "desired_state")
        .unwrap();
    let diverged: Vec<usize> = frames
        .iter()
        .enumerate()
        .filter(|(_, frame)| frame["type"] == "error")
        .map(|(index, frame)| {
            assert_eq!(frame["body"]["code"], "log_diverged", "{frame}");
            assert_eq!(frame["body"]["fatal"], false, "{frame}");
            index
        })
        .collect();
    assert_eq!(diverged.len(), 1, "{frames:?}");
    assert!(snapshot < diverged[0], "{frames:?}");
    assert!(diverged[0] < frames.len() - 1, "{frames:?}");
    let body = &frames.last().unwrap()["body"];
    assert_eq!(body["from_seq"], 1);
    check_entries(body, &genesis());
}

#[tokio::test]
async fn a_diverged_position_gets_log_diverged_before_any_entries() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    // Another hash at a seq the primary holds.
    let head = log_head(&server).await;
    let (socket, frames) = follow_from(
        &server,
        &node,
        hello_at(&node.node_id, head, &"f".repeat(64)),
    )
    .await;
    check_diverged(&frames);
    drop(socket);

    // A seq above the primary's head, as after a promotion (section 7.3).
    let head = log_head(&server).await;
    let hash = log_hash_at(&server, head).await;
    let (socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, head + 1000, &hash)).await;
    check_diverged(&frames);
    drop(socket);

    // An empty copy names 64 zeros.
    let (_socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, 0, &"f".repeat(64))).await;
    check_diverged(&frames);
}

/// Sends `hello` on `socket` and returns every frame up to the session's
/// first `desired_state` and what the primary queued after it before it
/// answered two pings (section 4.2 rule 6).
async fn hello_then_settle(socket: &mut Socket, hello: NodeMessage) -> Vec<Value> {
    send(socket, hello).await;
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(socket).await;
        let done = frame["type"] == "desired_state";
        frames.push(frame);
        if done {
            break;
        }
    }
    frames.extend(settle(socket).await);
    frames
}

/// A machine without the `node` role gets neither `log_diverged` nor
/// `log_entries`, even for a position that differs from the primary's log
/// and is below its stored `nodes.log_seq`, and not after a new write either.
async fn does_not_follow(server: &Server, machine: &Enrolled, tag: &str) {
    append_entries(server, tag, 3).await;
    let stored = log_head(server).await;
    assert!(stored > 3, "{stored}");
    set_stored_log_seq(server, &machine.node_id, stored).await;

    let mut socket = connect(server, &machine.credential).await;
    let mut frames =
        hello_then_settle(&mut socket, hello_at(&machine.node_id, 3, &"f".repeat(64))).await;
    append_entries(server, tag, 1).await;
    frames.extend(settle(&mut socket).await);
    assert!(of_type(&frames, "log_entries").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
}

#[tokio::test]
async fn an_edge_only_machine_does_not_follow() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let edge = enrolled_with_roles(&server, &owner, "edge-1", &["edge"]).await;
    does_not_follow(&server, &edge, "edge").await;
}

#[tokio::test]
async fn a_witness_only_machine_does_not_follow() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let witness = enrolled_with_roles(&server, &owner, "witness-1", &["witness"]).await;
    does_not_follow(&server, &witness, "witness").await;
}

#[tokio::test]
async fn a_bad_message_answer_is_sent_again_from_the_last_ack_after_a_delay() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 600).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let (last, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    ack(&mut socket, last, &hash).await;
    let refused = recv_type(&mut socket, "log_entries").await;
    assert_eq!(refused["body"]["from_seq"], last + 1);

    // The node could not verify it (section 7.1, "Verification").
    let error = NodeMessage::Error(ProtocolError {
        code: ErrorCode::BadMessage,
        message: "log_entries does not verify".to_string(),
        fatal: false,
        term: None,
    })
    .to_frame("msg_refusal0000000000000000", refused["id"].as_str())
    .to_text();
    let refused_at = Instant::now();
    send_text(&mut socket, error).await;

    let again = recv_type(&mut socket, "log_entries").await;
    let waited = refused_at.elapsed();
    assert!(
        waited >= Duration::from_millis(2_900),
        "sent again after {waited:?}"
    );
    assert_ne!(again["id"], refused["id"]);
    assert_eq!(again["body"]["from_seq"], last + 1);
    check_entries(&again["body"], &hash);
}

#[tokio::test]
async fn a_bad_message_that_answers_another_message_resends_nothing() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 600).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let outstanding = frames.last().unwrap();
    let (last, hash) = check_entries(&outstanding["body"], &genesis());
    assert_eq!(last, 500);

    // A bad_message that answers another message, and one that answers none,
    // while the log_entries is outstanding.
    let other = "msg_notthelogentries0000000000";
    assert_ne!(outstanding["id"], other);
    for (id, re) in [
        ("msg_refusalother0000000000000", Some(other)),
        ("msg_refusalnore00000000000000", None),
    ] {
        let error = NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "not about log_entries".to_string(),
            fatal: false,
            term: None,
        })
        .to_frame(id, re)
        .to_text();
        send_text(&mut socket, error).await;
    }
    let settled = settle(&mut socket).await;
    assert!(of_type(&settled, "log_entries").is_empty(), "{settled:?}");

    // The log_entries is still outstanding: its ack moves on from 501, with
    // no resend from seq 1 in between.
    ack(&mut socket, last, &hash).await;
    let next = recv_type(&mut socket, "log_entries").await;
    assert_ne!(next["id"], outstanding["id"]);
    assert_eq!(next["body"]["from_seq"], last + 1, "{next}");
    check_entries(&next["body"], &hash);
}

#[tokio::test]
async fn nodes_log_seq_is_written_when_a_session_starts_and_ends() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    // A node that already holds the log up to the current head.
    let start = log_head(&server).await;
    assert!(start > 0);
    let start_hash = log_hash_at(&server, start).await;
    let (mut socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, start, &start_hash)).await;
    assert_eq!(stored_log_seq(&server, &node.node_id).await, start);
    let body = &frames.last().unwrap()["body"];
    assert_eq!(body["from_seq"], start + 1);
    let (last, hash) = check_entries(body, &start_hash);
    ack(&mut socket, last, &hash).await;
    let (last, _) = catch_up(&mut socket, last, hash).await;
    assert!(last > start);
    assert_eq!(
        stored_log_seq(&server, &node.node_id).await,
        start,
        "an ack is kept in memory, not written"
    );

    // The session's end writes the newest ack.
    drop(socket);
    wait_disconnected(&server, &owner, &node.node_id).await;
    assert_eq!(stored_log_seq(&server, &node.node_id).await, last);
}

// ---------------------------------------------------------------------------
// A log that went back (section 7.1, "On `hello`", ADR 0261)
// ---------------------------------------------------------------------------

/// The node's change-log `update` entries in `seq` order, each `seq` with its
/// payload.
async fn node_updates(server: &Server, node_id: &str) -> Vec<(u64, Value)> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT seq, payload FROM change_log \
         WHERE entity_type = 'node' AND entity_id = ? AND op = 'update' ORDER BY seq",
    )
    .bind(node_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.into_iter()
        .map(|(seq, payload)| {
            (
                u64::try_from(seq).unwrap(),
                serde_json::from_str(&payload).unwrap(),
            )
        })
        .collect()
}

/// Makes the 0 write before `log_diverged` fail: a write of
/// `nodes.log_seq = 0` over a nonzero value aborts. A start write, which
/// stores the `hello`'s nonzero `seq`, and an end write of a nonzero `seq`
/// pass.
async fn refuse_log_seq_reset(server: &Server) {
    sqlx::query(
        "CREATE TRIGGER test_refuse_log_seq_reset BEFORE UPDATE OF log_seq ON nodes \
         WHEN NEW.log_seq = 0 AND OLD.log_seq > 0 \
         BEGIN SELECT RAISE(ABORT, 'test: the 0 write fails'); END",
    )
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Ends [`refuse_log_seq_reset`].
async fn allow_log_seq_reset(server: &Server) {
    sqlx::query("DROP TRIGGER test_refuse_log_seq_reset")
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

/// Reads every frame up to and including the next `log_entries`.
async fn recv_through_log_entries(socket: &mut Socket) -> Vec<Value> {
    let mut frames = Vec::new();
    loop {
        let frame = recv_frame(socket).await;
        let done = frame["type"] == "log_entries";
        frames.push(frame);
        if done {
            return frames;
        }
    }
}

/// A session from an empty copy that acknowledges up to the primary's head
/// and ends. Returns that `seq`, which `nodes.log_seq` then holds, as does
/// the primary's memory.
async fn acked_then_ended(server: &Server, owner: &reqwest::Client, node: &Enrolled) -> u64 {
    let (mut socket, frames) = follow_from(server, node, hello(&node.node_id)).await;
    let (last, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    ack(&mut socket, last, &hash).await;
    let (acked, _) = catch_up(&mut socket, last, hash).await;
    assert!(acked > 1, "{acked}");
    drop(socket);
    wait_disconnected(server, owner, &node.node_id).await;
    assert_eq!(stored_log_seq(server, &node.node_id).await, acked);
    acked
}

/// (j) A node acknowledged up to N, then comes back with a matching `hello`
/// below N: `log_diverged` before any `log_entries`, entries from `seq` 1,
/// and `nodes.log_seq` is 0 while that session is still open.
#[tokio::test]
async fn a_hello_below_an_earlier_ack_gets_log_diverged_and_resets_nodes_log_seq() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let acked = acked_then_ended(&server, &owner, &node).await;

    // The primary's own entry at that seq: only the ack makes it diverge.
    let below = acked - 1;
    let below_hash = log_hash_at(&server, below).await;
    let (socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, below, &below_hash)).await;
    check_diverged(&frames);
    assert_eq!(
        stored_log_seq(&server, &node.node_id).await,
        0,
        "written with log_diverged, not with the session's end"
    );
    // The 0 write is a change-log entry of its own, after the session's
    // start entry (section 7.1, "Lag").
    let updates = node_updates(&server, &node.node_id).await;
    let start = updates
        .iter()
        .rposition(|(_, payload)| payload["log_seq"] == below && payload["status"] == "connected")
        .expect("the session's start entry");
    assert!(
        updates[start + 1..]
            .iter()
            .any(|(_, payload)| payload["log_seq"] == 0),
        "no update entry with log_seq 0 after the start entry: {updates:?}"
    );
    drop(socket);
}

/// (k) `nodes.log_seq` alone, with nothing in the primary's memory: a
/// matching `hello` below it gets `log_diverged`, one at exactly it does not.
#[tokio::test]
async fn a_hello_below_the_stored_log_seq_alone_gets_log_diverged() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 3).await;

    // As a session of an earlier primary process would have left it.
    let stored = log_head(&server).await;
    set_stored_log_seq(&server, &node.node_id, stored).await;
    let below = stored - 1;
    let below_hash = log_hash_at(&server, below).await;
    let (socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, below, &below_hash)).await;
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    drop(socket);
    wait_disconnected(&server, &owner, &node.node_id).await;

    // At exactly the stored seq, the node follows on from it.
    set_stored_log_seq(&server, &node.node_id, stored).await;
    let stored_hash = log_hash_at(&server, stored).await;
    let (_socket, frames) = follow_from(
        &server,
        &node,
        hello_at(&node.node_id, stored, &stored_hash),
    )
    .await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    let body = &frames.last().unwrap()["body"];
    assert_eq!(body["from_seq"], stored + 1);
    check_entries(body, &stored_hash);
}

/// (l) Session A acknowledges up to N; session B of the same node replaces
/// it while A is still attached, with a matching `hello` below N. Only the
/// primary's memory holds N: A's start write left 0 in `nodes.log_seq`.
#[tokio::test]
async fn an_ack_of_a_superseded_session_still_counts() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    let (mut a, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let (last, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    ack(&mut a, last, &hash).await;
    let (acked, _) = catch_up(&mut a, last, hash).await;
    assert!(acked > 1, "{acked}");
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);

    let below = acked - 1;
    let below_hash = log_hash_at(&server, below).await;
    let (b, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, below, &below_hash)).await;
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    drop(b);
    drop(a);
}

/// (m) After `log_diverged`, the node's next `hello` from an empty copy
/// follows from `seq` 1 without another `log_diverged`: the primary set its
/// memory and `nodes.log_seq` to 0, so it does not read as a log that went
/// back.
#[tokio::test]
async fn after_log_diverged_a_hello_from_an_empty_copy_follows_normally() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    let acked = acked_then_ended(&server, &owner, &node).await;

    let below = acked - 1;
    let below_hash = log_hash_at(&server, below).await;
    let (socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, below, &below_hash)).await;
    check_diverged(&frames);
    // The node moves its copy aside; it acknowledges nothing on this session.
    drop(socket);
    wait_disconnected(&server, &owner, &node.node_id).await;
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);

    let (_socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    let body = &frames.last().unwrap()["body"];
    assert_eq!(body["from_seq"], 1);
    check_entries(body, &genesis());
}

/// `log_diverged` never goes out before `nodes.log_seq = 0` is written: while
/// that write fails, the session gets neither the error nor entries, and the
/// follow start is tried again after the retry delay.
#[tokio::test]
async fn log_diverged_waits_for_its_0_write() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    refuse_log_seq_reset(&server).await;

    // Another hash at a seq the primary holds.
    let head = log_head(&server).await;
    let mut socket = connect(&server, &node.credential).await;
    let mut frames =
        hello_then_settle(&mut socket, hello_at(&node.node_id, head, &"f".repeat(64))).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "log_entries").is_empty(), "{frames:?}");
    assert_eq!(stored_log_seq(&server, &node.node_id).await, head);

    // Once the write can succeed, the retry answers log_diverged after it.
    allow_log_seq_reset(&server).await;
    frames.extend(recv_through_log_entries(&mut socket).await);
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
}

/// The same with the (l) setup: the acknowledgement is held only in the
/// primary's memory, from session A, still attached when session B replaces
/// it with a matching `hello` below it. While the 0 write fails, B gets
/// neither the error nor entries; the retry after it still answers
/// `log_diverged`. Had the memory been set to 0 before the write succeeded,
/// the retry would find nothing above B's position and send entries.
#[tokio::test]
async fn log_diverged_waits_for_its_0_write_with_the_ack_in_memory_only() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    let (mut a, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let (last, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    ack(&mut a, last, &hash).await;
    let (acked, _) = catch_up(&mut a, last, hash).await;
    assert!(acked > 1, "{acked}");
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    refuse_log_seq_reset(&server).await;

    let below = acked - 1;
    let below_hash = log_hash_at(&server, below).await;
    let mut b = connect(&server, &node.credential).await;
    let mut frames = hello_then_settle(&mut b, hello_at(&node.node_id, below, &below_hash)).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "log_entries").is_empty(), "{frames:?}");
    assert_eq!(stored_log_seq(&server, &node.node_id).await, below);

    allow_log_seq_reset(&server).await;
    frames.extend(recv_through_log_entries(&mut b).await);
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    drop(b);
    drop(a);
}

/// (n) `nodes.log_seq` holds N, with nothing in the primary's memory. A
/// session's matching `hello` below N is stored by its start write, but the
/// session ends before its check completes: the 0 write keeps failing. Its
/// end puts N back, so the next `hello` at the same position still draws
/// `log_diverged` (section 7.1, "On `hello`", ADR 0261).
#[tokio::test]
async fn a_session_ended_before_its_check_puts_the_stored_log_seq_back() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 3).await;

    // As a session of an earlier primary process would have left it.
    let stored = log_head(&server).await;
    set_stored_log_seq(&server, &node.node_id, stored).await;
    refuse_log_seq_reset(&server).await;

    let below = stored - 1;
    let below_hash = log_hash_at(&server, below).await;
    let mut socket = connect(&server, &node.credential).await;
    let frames = hello_then_settle(&mut socket, hello_at(&node.node_id, below, &below_hash)).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "log_entries").is_empty(), "{frames:?}");
    assert_eq!(
        stored_log_seq(&server, &node.node_id).await,
        below,
        "the start write"
    );

    drop(socket);
    wait_disconnected(&server, &owner, &node.node_id).await;
    assert_eq!(
        stored_log_seq(&server, &node.node_id).await,
        stored,
        "the end write puts back what the start write replaced"
    );

    allow_log_seq_reset(&server).await;
    let (_socket, frames) =
        follow_from(&server, &node, hello_at(&node.node_id, below, &below_hash)).await;
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
}

/// (o) `nodes.log_seq` holds N, with nothing in the primary's memory.
/// Session A's matching `hello` below N is stored by its start write, and
/// its check keeps failing on the 0 write. Session B replaces A, still
/// attached, with the same `hello`: B's start write reads A's as the stored
/// value, but the primary raised its memory to N at A's start, so B's check
/// still finds the log went back and B gets `log_diverged` once the write
/// can succeed.
#[tokio::test]
async fn a_session_superseded_before_its_check_leaves_the_stored_log_seq_in_memory() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 3).await;

    let stored = log_head(&server).await;
    set_stored_log_seq(&server, &node.node_id, stored).await;
    refuse_log_seq_reset(&server).await;

    let below = stored - 1;
    let below_hash = log_hash_at(&server, below).await;
    let mut a = connect(&server, &node.credential).await;
    let frames = hello_then_settle(&mut a, hello_at(&node.node_id, below, &below_hash)).await;
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");
    assert!(of_type(&frames, "log_entries").is_empty(), "{frames:?}");
    assert_eq!(stored_log_seq(&server, &node.node_id).await, below);

    let mut b = connect(&server, &node.credential).await;
    let mut frames = hello_then_settle(&mut b, hello_at(&node.node_id, below, &below_hash)).await;
    assert!(
        of_type(&frames, "log_entries").is_empty(),
        "B's hello below N passed the check: {frames:?}"
    );
    assert!(of_type(&frames, "error").is_empty(), "{frames:?}");

    allow_log_seq_reset(&server).await;
    frames.extend(recv_through_log_entries(&mut b).await);
    check_diverged(&frames);
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    drop(b);
    drop(a);
}

// ---------------------------------------------------------------------------
// The lag in the API (section 7.1, "Lag", ADR 0227, ADR 0301)
// ---------------------------------------------------------------------------

/// `log_seq` and `log_lag` of `node_id` as `GET /api/v1/nodes/{node_id}`
/// reports them, `None` for a null `log_lag`. Checks that `log_lag` is
/// present, never absent, and that the node's item in `GET /api/v1/nodes`
/// carries the same values.
async fn log_position(
    server: &Server,
    owner: &reqwest::Client,
    node_id: &str,
) -> (u64, Option<u64>) {
    let node = node_json(server, owner, node_id).await;
    let res = owner
        .get(server.url("/api/v1/nodes?limit=500"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    let listed = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == node_id)
        .expect("the node is listed");
    for body in [&node, listed] {
        assert!(
            body.as_object().unwrap().contains_key("log_lag"),
            "log_lag is absent: {body}"
        );
    }
    assert_eq!(listed["log_seq"], node["log_seq"], "{listed}");
    assert_eq!(listed["log_lag"], node["log_lag"], "{listed}");
    let log_seq = node["log_seq"].as_u64().expect("log_seq is a whole number");
    let log_lag = if node["log_lag"].is_null() {
        None
    } else {
        Some(node["log_lag"].as_u64().expect("log_lag is a whole number"))
    };
    (log_seq, log_lag)
}

#[tokio::test]
async fn the_api_reports_a_following_nodes_newest_ack_and_its_lag() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;
    append_entries(&server, "bulk", 600).await;

    let (mut socket, frames) = follow_from(&server, &node, hello(&node.node_id)).await;
    let (acked, hash) = check_entries(&frames.last().unwrap()["body"], &genesis());
    assert_eq!(acked, 500);
    ack(&mut socket, acked, &hash).await;
    // The ack is handled once the pings after it are answered; the next
    // message went out before their answers.
    let settled = settle(&mut socket).await;
    let next = of_type(&settled, "log_entries");
    assert_eq!(next.len(), 1, "{settled:?}");

    // Acknowledged up to 500: the API reads the acknowledgement in memory,
    // not the row, which still holds the hello's 0.
    assert_eq!(stored_log_seq(&server, &node.node_id).await, 0);
    let head = log_head(&server).await;
    assert!(head > 600, "{head}");
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (500, Some(head - 500))
    );

    // Caught up: no lag.
    let (last, hash) = check_entries(&next[0]["body"], &hash);
    ack(&mut socket, last, &hash).await;
    let (last, hash) = catch_up(&mut socket, last, hash).await;
    assert_eq!(last, log_head(&server).await);
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (last, Some(0))
    );

    // A new write puts the node one entry behind until it acknowledges it.
    append_entries(&server, "lag", 1).await;
    assert_eq!(log_head(&server).await, last + 1);
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (last, Some(1))
    );
    let message = recv_type(&mut socket, "log_entries").await;
    assert_eq!(message["body"]["from_seq"], last + 1, "{message}");
    let (seq, seq_hash) = check_entries(&message["body"], &hash);
    assert_eq!(seq, last + 1);
    ack(&mut socket, seq, &seq_hash).await;
    settle(&mut socket).await;
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (seq, Some(0))
    );
}

#[tokio::test]
async fn a_machine_without_the_node_role_reads_a_null_log_lag() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let edge = enrolled_with_roles(&server, &owner, "edge-1", &["edge"]).await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    // `log_position` checks that the null is present, not absent.
    assert_eq!(
        log_position(&server, &owner, &edge.node_id).await,
        (0, None)
    );
    // A machine with the `node` role that never followed is behind by the
    // whole log.
    let head = log_head(&server).await;
    assert!(head > 0);
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (0, Some(head))
    );
}

#[tokio::test]
async fn a_disconnected_node_reads_the_log_seq_its_session_end_wrote() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled(&server, &owner, NODE_NAME).await;

    let acked = acked_then_ended(&server, &owner, &node).await;
    // The end write is itself a change-log entry, so the node is behind.
    let head = log_head(&server).await;
    assert!(head > acked, "{head} {acked}");
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (acked, Some(head - acked))
    );

    // A row behind the acknowledgement the primary still holds in memory,
    // as between a session's detach and its end write, or after a failed end
    // write, reads that acknowledgement, not the row.
    set_stored_log_seq(&server, &node.node_id, acked - 1).await;
    assert_eq!(log_head(&server).await, head);
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (acked, Some(head - acked))
    );

    // A row above both, as a session's start writes from a `hello` before
    // its check, reads the row; above the primary's newest seq it reads as
    // no lag, never below 0.
    set_stored_log_seq(&server, &node.node_id, head + 5).await;
    assert_eq!(
        log_position(&server, &owner, &node.node_id).await,
        (head + 5, Some(0))
    );
}
