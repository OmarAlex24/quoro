//! Node enrollment and the node-agent session over real HTTP and WebSocket
//! (`node-agent-protocol.md` sections 2-5, issue #62).

mod common;

use common::{Server, TLS_FINGERPRINT, problem, start, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::api::enroll::{ensure_local_join, local_join_token};
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_proto::sign;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Health, Heartbeat, Kvm as WireKvm, NodeMessage, ObservedStatus,
    StatusReport, VmStatus,
};
use serde_json::{Value, json};
use std::time::Duration;
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
