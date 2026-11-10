//! Local volumes of stateful apps: the API, the desired state and the node
//! session (ADR 0145, issue #230), over real HTTP and WebSocket.

mod common;

use common::{Server, problem, start, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::deploy;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Health, Heartbeat, Kvm as WireKvm, NodeMessage, ObservedStatus,
    StatusReport, VmStatus, VolumeState as WireVolumeState, VolumeStatus,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn local_node() -> LocalNode {
    LocalNode {
        name: "test-node".to_string(),
        capacity: Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        kvm: Kvm {
            available: true,
            kernel: "6.8.0".to_string(),
            nested: Some(true),
        },
        engine_version: "test-engine".to_string(),
        agent_version: "test-agent".to_string(),
    }
}

/// A control plane with the owner logged in, one registered (connected) node
/// and one project.
async fn http_setup() -> (Server, reqwest::Client, String, String) {
    let server = start().await;
    let owner = server.owner().await;
    let node_id = driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
    let project_id = create_project(&owner, &server, "demo").await;
    (server, owner, node_id, project_id)
}

async fn create_project(owner: &reqwest::Client, server: &Server, slug: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": slug, "name": slug }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create project: {}",
        res.text().await.unwrap()
    );
    let project: Value = res.json().await.unwrap();
    project["id"].as_str().unwrap().to_string()
}

async fn create_app(
    owner: &reqwest::Client,
    server: &Server,
    project_id: &str,
    name: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": name,
            "image": IMAGE,
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
    app["id"].as_str().unwrap().to_string()
}

async fn get_app(owner: &reqwest::Client, server: &Server, app_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

/// Creates a deployment and returns its VM id.
async fn deploy_app(owner: &reqwest::Client, server: &Server, app_id: &str, image: &str) -> String {
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
    sqlx::query_as::<_, (String,)>("SELECT id FROM vms WHERE app_id = ? ORDER BY created_at DESC")
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
        .0
}

fn volume_body(size_gb: i64, mount_path: &str) -> Value {
    json!({ "size_gb": size_gb, "mount_path": mount_path })
}

/// The payload of the app's newest change-log entry that carries the whole
/// app, and its warm fields (ADR 0286).
async fn whole_app_payload(server: &Server, app_id: &str) -> (Value, Value) {
    let (payload,): (String,) = sqlx::query_as(
        "SELECT payload FROM change_log WHERE entity_type = 'app' AND entity_id = ? \
         AND json_extract(payload, '$.placement') IS NOT NULL ORDER BY seq DESC LIMIT 1",
    )
    .bind(app_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    let warm = json!({
        "warm": payload["warm"],
        "warm_node_id": payload["warm_node_id"],
        "warm_node_pinned": payload["warm_node_pinned"],
        "warm_interval_s": payload["warm_interval_s"],
    });
    (payload, warm)
}

/// The warm fields of an app that never was `warm`.
fn no_warm() -> Value {
    json!({
        "warm": null,
        "warm_node_id": null,
        "warm_node_pinned": false,
        "warm_interval_s": 30,
    })
}

async fn create_volume(
    owner: &reqwest::Client,
    server: &Server,
    app_id: &str,
    size_gb: i64,
    mount_path: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&volume_body(size_gb, mount_path))
        .send()
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// The fake node
// ---------------------------------------------------------------------------

struct Enrolled {
    node_id: String,
    credential: String,
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
        mesh_public_key: None,
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        kvm: WireKvm {
            available: true,
            kernel: "6.9.0".to_string(),
            nested: Some(true),
        },
        dropped_events: 0,
        volume_snapshot: Some(wire::SnapshotMethod::Reflink),
        custody_public_key: None,
    })
}

async fn enrolled_node(server: &Server, owner: &reqwest::Client, name: &str) -> Enrolled {
    let token: Value = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"].as_str().unwrap(),
            "name": name,
            "public_key": quoro_proto::sign::public_key_to_base64(
                &quoro_proto::sign::signing_key_from_seed(&[3u8; 32]).verifying_key()
            ),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "enroll: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    Enrolled {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
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

async fn send(socket: &mut Socket, message: NodeMessage) {
    let text = message
        .to_frame("msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D", None)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

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
            WsMessage::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

/// Reads frames until one of type `kind` arrives, and returns it; frames of
/// other types before it, such as `log_entries`, are skipped.
async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    let frames = recv_until(socket, &[kind]).await;
    frames
        .into_iter()
        .find(|frame| frame["type"] == kind)
        .expect("recv_until stops at a frame of the wanted type")
}

/// Reads frames until one of each `wanted` type was seen, and returns them in
/// arrival order. Earlier frames of other types are kept in the result.
async fn recv_until(socket: &mut Socket, wanted: &[&str]) -> Vec<Value> {
    let mut remaining: Vec<&str> = wanted.to_vec();
    let mut seen = Vec::new();
    while !remaining.is_empty() {
        let frame = recv_frame(socket).await;
        let kind = frame["type"].as_str().unwrap_or_default().to_string();
        if let Some(index) = remaining.iter().position(|wanted| *wanted == kind) {
            remaining.remove(index);
        }
        seen.push(frame);
    }
    seen
}

/// Sends `hello` and consumes `welcome` and the first `desired_state`.
async fn open(server: &Server, node: &Enrolled) -> (Socket, Value) {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id)).await;
    let welcome = recv_type(&mut socket, "welcome").await;
    assert_eq!(welcome["body"]["cluster_id"], server.state.cluster_id);
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
}

fn status_report(applied_rev: u64, volumes: Vec<VolumeStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev,
        vms: Vec::new(),
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        disk_free_gb: 90.0,
        builds: Vec::new(),
        edge_backends: Vec::new(),
        backups: Vec::new(),
        images: Vec::new(),
        warm: Vec::new(),
        volumes,
        quarantines: Vec::new(),
    })
}

async fn wait_for_volume_state(
    owner: &reqwest::Client,
    server: &Server,
    socket: &mut Socket,
    volume_id: &str,
    state: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let volume: Value = res.json().await.unwrap();
        if volume["state"] == state {
            return volume;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "volume {volume_id} did not become {state}: {volume}"
        );
        heartbeat(socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// The API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_makes_the_app_stateful_and_validates_the_body() {
    let (server, owner, node_id, project_id) = http_setup().await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(
        res.status(),
        201,
        "create volume: {}",
        res.text().await.unwrap()
    );
    let volume: Value = res.json().await.unwrap();
    let volume_id = volume["id"].as_str().unwrap().to_string();
    assert!(volume_id.starts_with("vol_"), "{volume}");
    assert_eq!(volume["app_id"], app_id);
    assert_eq!(volume["node_id"], node_id);
    assert_eq!(volume["state"], "creating");
    assert_eq!(volume["size_gb"], 5);
    assert_eq!(volume["mount_path"], "/data");
    assert!(volume["allocated_bytes"].is_null());
    assert!(volume["restored_from_backup_id"].is_null());
    assert!(volume["error"].is_null());
    assert!(!volume["created_at"].as_str().unwrap().is_empty());

    // The app is stateful and pinned to the volume's node.
    let app = get_app(&owner, &server, &app_id).await;
    assert_eq!(app["stateful"], true);
    assert_eq!(app["placement"]["node_ids"], json!([node_id]));
    // Its change-log entry carries the warm columns too (ADR 0286).
    let (payload, warm) = whole_app_payload(&server, &app_id).await;
    assert_eq!(payload["stateful"], true);
    assert_eq!(warm, no_warm());

    // One attached volume per app (ADR 0145).
    let (status, code, _) =
        problem(create_volume(&owner, &server, &app_id, 5, "/other").await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // Unknown app.
    let (status, code, _) = problem(
        create_volume(
            &owner,
            &server,
            "app_00000000000000000000000000",
            5,
            "/data",
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // Bad mount paths and sizes.
    for (size, path) in [
        (5, "data"),
        (5, "/"),
        (5, "/data/"),
        (5, "/data/./x"),
        (5, "/data/../x"),
        (5, "/a//b"),
        (5, "/proc/1"),
        (5, "/sys"),
        (5, "/dev/shm"),
        (5, "/data x"),
        (5, "/é"),
        (0, "/data"),
        (1025, "/data"),
    ] {
        let (status, code, _) =
            problem(create_volume(&owner, &server, &app_id, size, path).await).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "size {size}, path {path:?}"
        );
    }

    // list and get.
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["id"], volume_id);
    let res = owner
        .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.json::<Value>().await.unwrap()["id"], volume_id);
    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/volumes/vol_00000000000000000000000000"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

#[tokio::test]
async fn an_agent_needs_a_justification_and_an_approval_to_delete_a_volume() {
    let (server, owner, _node_id, project_id) = http_setup().await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201);
    let volume: Value = res.json().await.unwrap();
    let volume_id = volume["id"].as_str().unwrap().to_string();

    let owner_user_id: String = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["user"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "volume-operator",
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "a test agent",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent_id = agent["id"].as_str().unwrap();
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": ["app:read", "app:update", "approval:create"],
            "selector": { "projects": ["demo"] },
            "ttl_s": 3600,
            "reason": "tests",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "grant: {}", res.text().await.unwrap());
    let key: Value = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token: Value = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = token["access_token"].as_str().unwrap().to_string();
    let agent_client = || {
        reqwest::Client::new()
            .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
            .header("Authorization", format!("Bearer {token}"))
    };

    // A mutating agent call states why (permission-model.md section 10).
    let res = agent_client()
        .json(&volume_body(9, "/agent"))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let res = agent_client()
        .header("Quoro-Justification", "the second app needs a volume")
        .json(&volume_body(9, "/agent"))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!(
        (status, code.as_str()),
        (409, "conflict"),
        "the justification passes the check, the volume rule still applies"
    );

    // A destructive delete is refused without an approval on the volume.
    let delete = reqwest::Client::new()
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .header("Authorization", format!("Bearer {token}"));
    let (status, code, reason) = problem(delete.try_clone().unwrap().send().await.unwrap()).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));
    assert_eq!(reason, None);

    // The agent files a request naming the volume and the exact params.
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({
            "scope": "app:delete",
            "target": { "type": "volume", "id": volume_id },
            "operation": "DELETE /api/v1/volumes/{volume_id}",
            "params": { "volume_id": volume_id },
            "justification": "the volume is finished and its data must go",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "file approval: {}",
        res.text().await.unwrap()
    );
    let approval: Value = res.json().await.unwrap();
    let approval_id = approval["id"].as_str().unwrap();
    assert_eq!(approval["target"]["type"], "volume");
    assert_eq!(approval["target"]["id"], volume_id);

    let res = owner
        .post(server.url(&format!("/api/v1/approvals/{approval_id}/approve")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());

    let res = delete
        .header("Quoro-Justification", "approved cleanup")
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "delete after approval: {}",
        res.text().await.unwrap()
    );
    assert_eq!(res.json::<Value>().await.unwrap()["state"], "deleting");
}

#[tokio::test]
async fn delete_conflicts_while_the_app_runs_and_succeeds_once_it_stops() {
    let (server, owner, node_id, project_id) = http_setup().await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201);
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A deployment the node reports running makes the app `running`.
    let vm_id = deploy_app(&owner, &server, &app_id, IMAGE).await;
    deploy::on_vm_observed(
        &server.state.store,
        &node_id,
        &VmStatus {
            vm_id: vm_id.clone(),
            observed_status: ObservedStatus::Running,
            health: Health::Passing,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(get_app(&owner, &server, &app_id).await["status"], "running");

    let (status, code, _) = problem(
        owner
            .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/stop")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());
    assert_eq!(res.json::<Value>().await.unwrap()["state"], "deleting");

    // The app keeps its volume row until the node confirms the file is gone,
    // so it is still stateful here.
    assert_eq!(get_app(&owner, &server, &app_id).await["stateful"], true);
}

// ---------------------------------------------------------------------------
// The node session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_node_gets_the_volume_the_lease_and_the_ready_report() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT volume_snapshot FROM nodes WHERE id = ?")
            .bind(&node.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap()
            .as_deref(),
        Some("reflink"),
        "hello stores the node's snapshot method"
    );
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let vm_id = deploy_app(&owner, &server, &app_id, IMAGE).await;
    let deployment_snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        deployment_snapshot["body"]["spec"]["vms"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    heartbeat(&mut socket).await;

    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume: Value = res.json().await.unwrap();
    let volume_id = volume["id"].as_str().unwrap().to_string();

    let frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let grant = frames
        .iter()
        .find(|frame| frame["type"] == "lease_grant")
        .unwrap();
    let body = &grant["body"];
    let lease_id = body["lease_id"].as_str().unwrap().to_string();
    assert!(lease_id.starts_with("lse_"), "{grant}");
    assert_eq!(body["resource_type"], "app");
    assert_eq!(body["resource_id"], app_id);
    assert_eq!(body["mode"], "local");
    assert_eq!(body["fencing_token"], 1);
    assert!(body["duration_ms"].is_null(), "{grant}");
    assert!(!body["granted_at"].as_str().unwrap().is_empty());

    // From M5b every read of the app carries its lease (`App.lease`).
    let app = get_app(&owner, &server, &app_id).await;
    let lease = &app["lease"];
    assert_eq!(lease["id"], lease_id, "{app}");
    assert_eq!(lease["resource_type"], "app");
    assert_eq!(lease["resource_id"], app_id);
    assert_eq!(lease["holder_node_id"], node.node_id);
    assert_eq!(lease["mode"], "local");
    assert_eq!(lease["fencing_token"], 1);
    assert!(lease["duration_ms"].is_null());
    assert!(lease["gate_at"].is_null());
    assert!(lease["revoked_at"].is_null());

    let snapshot = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap();
    let spec = &snapshot["body"]["spec"];
    let volumes = spec["volumes"].as_array().unwrap();
    assert_eq!(volumes.len(), 1, "{snapshot}");
    assert_eq!(volumes[0]["volume_id"], volume_id);
    assert_eq!(volumes[0]["app_id"], app_id);
    assert_eq!(volumes[0]["size_gb"], 5);
    assert_eq!(volumes[0]["state"], "present");
    assert!(volumes[0].get("restore").is_none());

    let vms = spec["vms"].as_array().unwrap();
    assert_eq!(vms.len(), 1);
    assert_eq!(vms[0]["vm_id"], vm_id);
    assert_eq!(vms[0]["stateful"], true);
    assert_eq!(vms[0]["lease_id"], lease_id);
    assert_eq!(vms[0]["fencing_token"], 1);
    assert_eq!(
        vms[0]["volumes"],
        json!([{ "volume_id": volume_id, "serial": "quoro-vol-0", "mount_path": "/data" }])
    );

    // The node reports the file; the row turns ready with its allocated bytes.
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
    )
    .await;
    let ready = wait_for_volume_state(&owner, &server, &mut socket, &volume_id, "ready").await;
    assert_eq!(ready["allocated_bytes"], 4096);
    assert!(ready["error"].is_null());

    // The last reported free disk bounds the next volume's size.
    let second_app = create_app(&owner, &server, &project_id, "api").await;
    let (status, code, _) =
        problem(create_volume(&owner, &server, &second_app, 100, "/data").await).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn delete_releases_the_lease_and_the_deleted_report_ends_the_stateful_app() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let vm_id = deploy_app(&owner, &server, &app_id, IMAGE).await;
    recv_type(&mut socket, "desired_state").await;
    // The deployment succeeds, so the app runs and can then be stopped.
    deploy::on_vm_observed(
        &server.state.store,
        &node.node_id,
        &VmStatus {
            vm_id,
            observed_status: ObservedStatus::Running,
            health: Health::Passing,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        },
    )
    .await
    .unwrap();
    heartbeat(&mut socket).await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/stop")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "stop: {}", res.text().await.unwrap());
    recv_type(&mut socket, "desired_state").await;
    heartbeat(&mut socket).await;

    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201);
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let snapshots = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let ready_rev = snapshots
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap()["body"]["rev"]
        .as_u64()
        .unwrap();
    send(
        &mut socket,
        status_report(
            ready_rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
    )
    .await;
    wait_for_volume_state(&owner, &server, &mut socket, &volume_id, "ready").await;
    heartbeat(&mut socket).await;

    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());
    assert_eq!(res.json::<Value>().await.unwrap()["state"], "deleting");

    // The holder is revoked with reason `deleted`, then the snapshot carries
    // the file as `deleted`.
    let frames = recv_until(&mut socket, &["lease_revoke", "desired_state"]).await;
    let revoke = frames
        .iter()
        .find(|frame| frame["type"] == "lease_revoke")
        .unwrap();
    assert_eq!(revoke["body"]["reason"], "deleted");
    assert_eq!(revoke["body"]["fencing_token"], 1);
    let snapshot = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap();
    assert_eq!(
        snapshot["body"]["spec"]["volumes"][0]["state"], "deleted",
        "{snapshot}"
    );

    // The node removed the file: the row goes, the app is stateless and its
    // placement is unpinned.
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Deleted,
                allocated_bytes: None,
                error: None,
            }],
        ),
    )
    .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap();
        if res.status() == 404 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the volume row was not removed"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let app = get_app(&owner, &server, &app_id).await;
    assert_eq!(app["stateful"], false);
    assert_eq!(app["placement"]["node_ids"], json!([]));
    let (payload, warm) = whole_app_payload(&server, &app_id).await;
    assert_eq!(payload["stateful"], false);
    assert_eq!(warm, no_warm());
    let (lease_rows,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM leases WHERE resource_id = ? AND revoked_at IS NOT NULL",
    )
    .bind(&app_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(lease_rows, 1, "the lease row is revoked");
}

/// Issue #333 (ADR 0165): deleting an app keeps each of its volumes as
/// `deleting`, so the node gets the file as `deleted` and removes it; the row
/// goes only when the node reports it deleted. The holder of the app's
/// `local` lease is told with reason `deleted` before that snapshot.
#[tokio::test]
async fn deleting_an_app_keeps_its_volume_deleting_until_the_node_removes_the_file() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    deploy_app(&owner, &server, &app_id, IMAGE).await;
    recv_type(&mut socket, "desired_state").await;
    heartbeat(&mut socket).await;

    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201);
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let ready_rev = frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap()["body"]["rev"]
        .as_u64()
        .unwrap();
    send(
        &mut socket,
        status_report(
            ready_rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
    )
    .await;
    wait_for_volume_state(&owner, &server, &mut socket, &volume_id, "ready").await;
    heartbeat(&mut socket).await;

    let res = owner
        .delete(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());

    // The app is gone, but its volume stays readable as `deleting`.
    let res = owner
        .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "the volume outlives its app");
    assert_eq!(res.json::<Value>().await.unwrap()["state"], "deleting");

    // The holder is revoked with reason `deleted`, and a snapshot without the
    // app's instance carries the file as `deleted`.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut revoked = false;
    let snapshot = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no snapshot carried the volume as deleted"
        );
        let frame = recv_frame(&mut socket).await;
        if frame["type"] == "lease_revoke" {
            assert_eq!(frame["body"]["reason"], "deleted", "{frame}");
            revoked = true;
            continue;
        }
        if frame["type"] == "desired_state"
            && frame["body"]["spec"]["volumes"][0]["state"] == "deleted"
        {
            break frame;
        }
    };
    assert!(revoked, "the lease_revoke comes before the snapshot");
    assert_eq!(
        snapshot["body"]["spec"]["volumes"][0]["volume_id"],
        volume_id
    );
    assert_eq!(snapshot["body"]["spec"]["vms"], json!([]), "{snapshot}");

    // The node removed the file: the row goes.
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    send(
        &mut socket,
        status_report(
            rev,
            vec![VolumeStatus {
                volume_id: volume_id.clone(),
                state: WireVolumeState::Deleted,
                allocated_bytes: None,
                error: None,
            }],
        ),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let res = owner
            .get(server.url(&format!("/api/v1/volumes/{volume_id}")))
            .send()
            .await
            .unwrap();
        if res.status() == 404 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the volume row was not removed"
        );
        heartbeat(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_stateful_deployment_drops_the_old_vm_in_the_same_revision() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let node = enrolled_node(&server, &owner, "node-alpha").await;
    let (mut socket, _) = open(&server, &node).await;
    heartbeat(&mut socket).await;

    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let first_vm = deploy_app(&owner, &server, &app_id, IMAGE).await;
    let first_snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(first_snapshot["body"]["spec"]["vms"][0]["vm_id"], first_vm);
    heartbeat(&mut socket).await;

    let res = create_volume(&owner, &server, &app_id, 5, "/data").await;
    assert_eq!(res.status(), 201);
    let volume_frames = recv_until(&mut socket, &["lease_grant", "desired_state"]).await;
    let volume_rev = volume_frames
        .iter()
        .find(|frame| frame["type"] == "desired_state")
        .unwrap()["body"]["rev"]
        .as_u64()
        .unwrap();
    heartbeat(&mut socket).await;

    // The next deployment writes its VM and removes the old one in the same
    // transaction, so one snapshot both adds and drops an instance.
    let second_vm = deploy_app(
        &owner,
        &server,
        &app_id,
        "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222",
    )
    .await;
    assert_ne!(second_vm, first_vm);
    let snapshot = recv_type(&mut socket, "desired_state").await;
    let rev = snapshot["body"]["rev"].as_u64().unwrap();
    assert!(rev > volume_rev, "the snapshot moved to a new revision");
    let vms = snapshot["body"]["spec"]["vms"].as_array().unwrap();
    assert_eq!(vms.len(), 1, "the old VM is gone: {snapshot}");
    assert_eq!(vms[0]["vm_id"], second_vm);
    assert_eq!(vms[0]["stateful"], true);
    assert_eq!(
        vms[0]["volumes"][0]["serial"], "quoro-vol-0",
        "the new VM attaches the volume"
    );
    let (old_rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM vms WHERE id = ?")
        .bind(&first_vm)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(old_rows, 0);
}
