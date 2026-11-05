//! Git apps and builds (`createApp` with a `source`, `startBuild`,
//! `listAppBuilds`, `getBuild`, `cancelBuild`, `getBuildLogs`,
//! `streamBuildLogs`) over real HTTP, SSE and the node-agent WebSocket
//! (`node-agent-protocol.md` sections 5.3, 5.11, 5.15 and 5.17; ADR 0117,
//! ADR 0118, ADR 0119).

mod common;

use common::{Server, problem, start, start_with_cloudflare};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::store::now_rfc3339;
use quoro_proto::sign;
use quoro_proto::wire::{
    self, BuildState, BuildStatus, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage,
    StatusReport,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const NODE_NAME: &str = "node-builds";
const SECRET: &str = "s3cret-value";
const DIGEST: &str = "sha256:5c1f0d2a9b8e7f6a5d4c3b2a1908f7e6d5c4b3a29180f7e6d5c4b3a291807f6e";

// ---------------------------------------------------------------------------
// Node session harness
// ---------------------------------------------------------------------------

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

struct Enrolled {
    node_id: String,
    credential: String,
}

async fn enrolled(server: &Server, owner: &reqwest::Client) -> Enrolled {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&enroll_body(token["token"].as_str().unwrap(), NODE_NAME))
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

async fn send_text(socket: &mut Socket, text: String) {
    socket.send(WsMessage::Text(text.into())).await.unwrap();
}

async fn send(socket: &mut Socket, message: NodeMessage) {
    send_text(socket, message.to_frame("msg_test_send", None).to_text()).await;
}

async fn recv_frame(socket: &mut Socket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("a frame within 3 s")
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
    recv_type(&mut socket, "welcome").await;
    let snapshot = recv_type(&mut socket, "desired_state").await;
    (socket, snapshot)
}

/// The next `desired_state` whose `spec.builds` names `build_id`.
async fn recv_build_snapshot(socket: &mut Socket, build_id: &str) -> Value {
    loop {
        let snapshot = recv_type(socket, "desired_state").await;
        let carries = snapshot["body"]["spec"]["builds"]
            .as_array()
            .is_some_and(|builds| builds.iter().any(|build| build["build_id"] == build_id));
        if carries {
            return snapshot;
        }
    }
}

/// A `status_report` that only carries build statuses.
/// The ids of a snapshot's `spec.builds`, in order.
fn spec_build_ids(snapshot: &Value) -> Vec<String> {
    snapshot["body"]["spec"]["builds"]
        .as_array()
        .map(|builds| {
            builds
                .iter()
                .map(|build| build["build_id"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// The `spec.builds` entry of `build_id` in a snapshot.
fn spec_build<'a>(snapshot: &'a Value, build_id: &str) -> &'a Value {
    snapshot["body"]["spec"]["builds"]
        .as_array()
        .and_then(|builds| builds.iter().find(|build| build["build_id"] == build_id))
        .unwrap_or_else(|| panic!("{build_id} is not in {snapshot}"))
}

fn report(builds: Vec<BuildStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev: 0,
        vms: Vec::new(),
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
        },
        allocated: WireCapacity::default(),
        disk_free_gb: 64.0,
        builds,
        edge_backends: Vec::new(),
        backups: Vec::new(),
        volumes: Vec::new(),
    })
}

fn build_status(build_id: &str, status: BuildState) -> BuildStatus {
    BuildStatus {
        build_id: build_id.to_string(),
        status,
        commit: None,
        image_digest: None,
        failed_phase: None,
        error: None,
        failed_step: None,
        started_at: None,
        finished_at: None,
    }
}

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

async fn create_project(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "builds-demo", "name": "Builds Demo" }))
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

fn git_source() -> Value {
    json!({
        "kind": "git",
        "repo_url": "https://github.com/example/blog.git",
        "ref": "main",
        "build_args": [{ "name": "NODE_ENV", "value": "production" }],
        "secrets": ["TOKEN"],
    })
}

async fn create_git_app(server: &Server, owner: &reqwest::Client, project_id: &str) -> Value {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "source": git_source(),
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
    res.json().await.unwrap()
}

async fn set_token_env(server: &Server, owner: &reqwest::Client, app_id: &str) {
    let res = owner
        .put(server.url(&format!("/api/v1/apps/{app_id}/env")))
        .json(&json!({ "items": [{ "name": "TOKEN", "value": SECRET }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "env: {}", res.text().await.unwrap());
}

async fn start_build(server: &Server, owner: &reqwest::Client, app_id: &str) -> Value {
    // The body is optional; the panel's generated client still sends a JSON
    // content type with nothing after it.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "start build: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

async fn get_build(server: &Server, owner: &reqwest::Client, build_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/builds/{build_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "get build: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

/// Waits until the build's row shows `expected`, so a status report has been
/// applied before the test reads it.
async fn wait_for_status(
    server: &Server,
    owner: &reqwest::Client,
    build_id: &str,
    expected: &str,
) -> Value {
    for _ in 0..60 {
        let build = get_build(server, owner, build_id).await;
        if build["status"] == expected {
            return build;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "{} stayed at {}",
        build_id,
        get_build(server, owner, build_id).await
    );
}

/// The event types of the cluster, oldest first.
async fn event_types(server: &Server) -> Vec<String> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT type FROM events ORDER BY seq")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    rows.into_iter().map(|(kind,)| kind).collect()
}

/// The `data` of the newest event of `kind`.
async fn event_data(server: &Server, kind: &str) -> Value {
    let (data,): (String,) =
        sqlx::query_as("SELECT data FROM events WHERE type = ? ORDER BY seq DESC LIMIT 1")
            .bind(kind)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    serde_json::from_str(&data).unwrap()
}

async fn count_events(server: &Server, kind: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM events WHERE type = ?")
        .bind(kind)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_git_app_builds_its_image_and_deploys_it() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&server, &owner).await;
    let node = enrolled(&server, &owner).await;
    let (mut socket, _) = open(&server, &node).await;

    // A Git app has no image before its first successful build, and returns
    // its source; both or neither of `image` and `source` is a 422.
    let app = create_git_app(&server, &owner, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    assert_eq!(app["image"], "");
    assert_eq!(app["source"]["ref"], "main");
    assert_eq!(app["source"]["kind"], "git");
    assert_eq!(app["source"]["dockerfile_path"], "Dockerfile");
    assert_eq!(app["source"]["context_dir"], ".");
    assert_eq!(app["source"]["secrets"], json!(["TOKEN"]));

    for (label, body) in [
        (
            "both",
            json!({
                "project_id": project_id,
                "name": "both",
                "image": "ghcr.io/example/web:1",
                "source": git_source(),
                "port": 8080,
            }),
        ),
        (
            "neither",
            json!({ "project_id": project_id, "name": "neither", "port": 8080 }),
        ),
        (
            "unsafe dockerfile",
            json!({
                "project_id": project_id,
                "name": "unsafe",
                "source": {
                    "kind": "git",
                    "repo_url": "https://github.com/example/blog.git",
                    "ref": "main",
                    "dockerfile_path": "/etc/passwd",
                },
                "port": 8080,
            }),
        ),
    ] {
        let (status, code, _) = problem(
            owner
                .post(server.url("/api/v1/apps"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{label}"
        );
    }

    set_token_env(&server, &owner, &app_id).await;

    // A build of an app without a source is a 409; a malformed app id a 404.
    let image_app = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "image-only",
            "image": "ghcr.io/example/web:1",
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(image_app.status(), 201);
    let image_app: Value = image_app.json().await.unwrap();
    let image_app_id = image_app["id"].as_str().unwrap();
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{image_app_id}/builds")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // An image app returns `source: null`.
    assert!(image_app["source"].is_null());
    // A deployment of an image onto a Git app is refused.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
            .json(&json!({ "image": "ghcr.io/example/web:1" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // And a Git app has no built image to redeploy yet.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/redeploy")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // The build is queued on the node and reaches it with the source, the
    // default limits and the decrypted secret.
    let build = start_build(&server, &owner, &app_id).await;
    let build_id = build["id"].as_str().unwrap().to_string();
    assert!(build_id.starts_with("bld_"), "{build_id}");
    assert_eq!(build["status"], "queued");
    assert_eq!(build["app_id"], app_id);
    assert_eq!(build["node_id"], node.node_id);
    assert_eq!(build["trigger"], "api");
    assert_eq!(build["refresh_base"], false);
    assert_eq!(build["created_by_type"], "user");
    assert!(build["commit"].is_null());
    assert!(build["error"].is_null());

    let started = event_data(&server, "build.started").await;
    assert_eq!(started["build_id"], build_id);
    assert_eq!(started["app_id"], app_id);
    assert_eq!(started["node_id"], node.node_id);
    assert_eq!(started["ref"], "main");
    assert_eq!(started["repo_url"], "https://github.com/example/blog.git");

    let snapshot = recv_type(&mut socket, "desired_state").await;
    let builds = snapshot["body"]["spec"]["builds"].as_array().unwrap();
    assert_eq!(builds.len(), 1, "{snapshot}");
    let spec = &builds[0];
    assert_eq!(spec["build_id"], build_id);
    assert_eq!(spec["app_id"], app_id);
    assert_eq!(spec["project_id"], project_id);
    assert_eq!(spec["repo_url"], "https://github.com/example/blog.git");
    assert_eq!(spec["ref"], "main");
    assert_eq!(spec["dockerfile_path"], "Dockerfile");
    assert_eq!(spec["context_dir"], ".");
    assert_eq!(spec["build_args"], json!({ "NODE_ENV": "production" }));
    assert_eq!(spec["secrets"], json!({ "TOKEN": SECRET }));
    // A build that reuses the cached base images says nothing about it.
    assert!(spec.get("refresh_base").is_none(), "{spec}");
    assert_eq!(
        spec["limits"],
        json!({
            "vcpus": 2,
            "memory_mb": 2048,
            "timeout_s": 1800,
            "cache_gb": 10,
            "output_gb": 10,
        })
    );
    assert_eq!(spec["cancelled"], false);

    // Only one build of an app runs at a time.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // Status reports move it through `preparing` and `building`.
    let mut preparing = build_status(&build_id, BuildState::Preparing);
    preparing.commit = Some("a".repeat(40));
    send(&mut socket, report(vec![preparing])).await;
    let preparing = wait_for_status(&server, &owner, &build_id, "preparing").await;
    assert_eq!(preparing["commit"], "a".repeat(40));
    assert!(preparing["started_at"].is_string());

    send(
        &mut socket,
        report(vec![build_status(&build_id, BuildState::Building)]),
    )
    .await;
    wait_for_status(&server, &owner, &build_id, "building").await;

    let mut succeeded = build_status(&build_id, BuildState::Succeeded);
    succeeded.commit = Some("a".repeat(40));
    succeeded.image_digest = Some(DIGEST.to_string());
    succeeded.finished_at = Some(now_rfc3339());
    send(&mut socket, report(vec![succeeded])).await;

    let build = wait_for_status(&server, &owner, &build_id, "succeeded").await;
    let image = format!("build.quoro.invalid/{app_id}:{build_id}");
    assert_eq!(build["image"], image);
    assert_eq!(build["image_digest"], DIGEST);
    let deployment_id = build["deployment_id"].as_str().unwrap().to_string();
    assert!(deployment_id.starts_with("dep_"));
    assert!(build["finished_at"].is_string());

    // The app's image is the built reference.
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["image"], image);

    // The deployment runs the built image on the build's node, with the
    // digest the build produced and `trigger: build`.
    let deployment: Value = owner
        .get(server.url(&format!("/api/v1/deployments/{deployment_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(deployment["trigger"], "build");
    assert_eq!(deployment["image"], image);
    assert_eq!(deployment["image_digest"], DIGEST);
    let (vm_node, vm_image): (String, String) =
        sqlx::query_as("SELECT node_id, image FROM vms WHERE app_id = ?")
            .bind(&app_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(vm_node, node.node_id);
    assert_eq!(vm_image, image);

    // `build.succeeded` comes before the deployment's own event.
    let kinds = event_types(&server).await;
    let succeeded_at = kinds
        .iter()
        .position(|kind| kind == "build.succeeded")
        .unwrap();
    let deployed_at = kinds
        .iter()
        .position(|kind| kind == "deployment.started")
        .unwrap();
    assert!(succeeded_at < deployed_at, "{kinds:?}");
    let data = event_data(&server, "build.succeeded").await;
    assert_eq!(data["deployment_id"], deployment_id);
    assert_eq!(data["commit"], "a".repeat(40));
    assert_eq!(data["image"], image);
    assert_eq!(data["image_digest"], DIGEST);

    // The next snapshot carries the VM in the same revision, and no build.
    let snapshot = recv_type(&mut socket, "desired_state").await;
    let spec = &snapshot["body"]["spec"];
    assert!(
        spec["builds"].as_array().is_none_or(Vec::is_empty),
        "{snapshot}"
    );
    let vms = spec["vms"].as_array().unwrap();
    assert_eq!(vms.len(), 1, "{snapshot}");
    assert_eq!(vms[0]["image"], image);
    assert_eq!(vms[0]["image_digest"], DIGEST);
    assert_eq!(vms[0]["build_id"], build_id);
    assert_eq!(vms[0]["deployment_id"], deployment_id);

    // The built image can be rolled out again, and a start may override the
    // source's ref for one build only.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/redeploy")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "redeploy: {}", res.text().await.unwrap());
    let redeployed: Value = res.json().await.unwrap();
    assert_eq!(redeployed["trigger"], "redeploy");
    assert_eq!(redeployed["image"], image);

    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .header("Idempotency-Key", "one-build")
        .json(&json!({ "ref": "release" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "start: {}", res.text().await.unwrap());
    let overridden: Value = res.json().await.unwrap();
    assert_eq!(overridden["source"]["ref"], "release");
    let overridden_id = overridden["id"].as_str().unwrap().to_string();
    // The same key and body replays the same build.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .header("Idempotency-Key", "one-build")
        .json(&json!({ "ref": "release" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let replayed: Value = res.json().await.unwrap();
    assert_eq!(replayed["id"], overridden_id);
    // The app source is unchanged.
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["source"]["ref"], "main");

    // Newest first, and the override reaches the node.
    let list: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/builds?limit=1")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"][0]["id"], overridden_id);
    let cursor = list["next_cursor"].as_str().unwrap().to_string();
    let page: Value = owner
        .get(server.url(&format!(
            "/api/v1/apps/{app_id}/builds?limit=1&cursor={cursor}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"][0]["id"], build_id);
    let snapshot = recv_build_snapshot(&mut socket, &overridden_id).await;
    assert_eq!(snapshot["body"]["spec"]["builds"][0]["ref"], "release");
}

#[tokio::test]
async fn a_build_with_refresh_base_resolves_its_base_images_again() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&server, &owner).await;
    let node = enrolled(&server, &owner).await;
    let (mut socket, _) = open(&server, &node).await;
    let app = create_git_app(&server, &owner, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    set_token_env(&server, &owner, &app_id).await;

    // "Rebuild with refreshed base" (ADR 0150): the flag is recorded on the
    // build and reaches the node, which resolves every base image again.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .json(&json!({ "refresh_base": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "start: {}", res.text().await.unwrap());
    let build: Value = res.json().await.unwrap();
    let build_id = build["id"].as_str().unwrap().to_string();
    assert_eq!(build["refresh_base"], true);
    assert_eq!(
        get_build(&server, &owner, &build_id).await["refresh_base"],
        true
    );

    let snapshot = recv_build_snapshot(&mut socket, &build_id).await;
    assert_eq!(spec_build(&snapshot, &build_id)["refresh_base"], true);
}

#[tokio::test]
async fn a_failed_build_keeps_its_step_and_a_cancel_is_durable() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&server, &owner).await;
    let node = enrolled(&server, &owner).await;
    let (mut socket, _) = open(&server, &node).await;
    let app = create_git_app(&server, &owner, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    set_token_env(&server, &owner, &app_id).await;

    // A build whose source names a secret that is not an environment variable
    // is recorded as failed in `preparing`, and the answer shows it.
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({
            "source": {
                "kind": "git",
                "repo_url": "https://github.com/example/blog.git",
                "ref": "main",
                "secrets": ["TOKEN", "MISSING"],
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "update: {}", res.text().await.unwrap());
    let missing = start_build(&server, &owner, &app_id).await;
    assert_eq!(missing["status"], "failed");
    assert_eq!(missing["failed_phase"], "preparing");
    assert_eq!(
        missing["error"],
        "secret MISSING is not an environment variable of the app"
    );
    assert_eq!(count_events(&server, "build.failed").await, 1);
    assert!(count_events(&server, "build.started").await >= 1);
    // A failed build never reaches the node: it is already terminal.
    let (unsent,): (String,) = sqlx::query_as("SELECT status FROM builds WHERE id = ?")
        .bind(missing["id"].as_str().unwrap())
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(unsent, "failed");

    // Back to the plain source: a build that fails in a step keeps the step.
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "source": git_source() }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "update: {}", res.text().await.unwrap());
    let build = start_build(&server, &owner, &app_id).await;
    let build_id = build["id"].as_str().unwrap().to_string();
    send(
        &mut socket,
        report(vec![build_status(&build_id, BuildState::Preparing)]),
    )
    .await;
    let mut failed = build_status(&build_id, BuildState::Failed);
    failed.failed_phase = Some(wire::BuildPhase::Building);
    failed.error = Some("process \"/bin/sh -c npm ci\" did not complete successfully".to_string());
    failed.failed_step = Some(wire::FailedStep {
        vertex: 6,
        name: "[3/4] RUN npm ci".to_string(),
        error: "exit code: 1".to_string(),
        dockerfile_line: Some(4),
        excerpt: vec!["npm ERR! missing script".to_string()],
    });
    send(&mut socket, report(vec![failed])).await;

    let build = wait_for_status(&server, &owner, &build_id, "failed").await;
    assert_eq!(build["failed_phase"], "building");
    assert_eq!(
        build["error"],
        "process \"/bin/sh -c npm ci\" did not complete successfully"
    );
    assert_eq!(build["failed_step"]["vertex"], 6);
    assert_eq!(build["failed_step"]["name"], "[3/4] RUN npm ci");
    assert_eq!(build["failed_step"]["error"], "exit code: 1");
    assert_eq!(build["failed_step"]["dockerfile_line"], 4);
    assert_eq!(
        build["failed_step"]["excerpt"],
        json!(["npm ERR! missing script"])
    );
    let data = event_data(&server, "build.failed").await;
    assert_eq!(data["phase"], "building");
    assert_eq!(data["failed_step"]["vertex"], 6);
    // The failed build stays in the node's list, so the node keeps its log
    // while the app has no VM: marked cancelled and without secrets, so it
    // never runs again.
    let snapshot = loop {
        let snapshot = recv_build_snapshot(&mut socket, &build_id).await;
        if spec_build(&snapshot, &build_id)["cancelled"] == true {
            break snapshot;
        }
    };
    assert_eq!(spec_build_ids(&snapshot), vec![build_id.clone()]);
    let kept = spec_build(&snapshot, &build_id);
    assert_eq!(kept["cancelled"], true, "{snapshot}");
    assert!(
        kept["secrets"]
            .as_object()
            .is_none_or(serde_json::Map::is_empty),
        "{snapshot}"
    );

    // A report that arrives after the terminal status changes nothing.
    send(
        &mut socket,
        report(vec![build_status(&build_id, BuildState::Building)]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        get_build(&server, &owner, &build_id).await["status"],
        "failed"
    );

    // The record is replicated: it is readable while the node is gone.
    drop(socket);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let build = get_build(&server, &owner, &build_id).await;
    assert_eq!(build["status"], "failed");
    let list: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/builds?limit=1")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["id"], build_id);
    // A finished build cannot be cancelled.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/builds/{build_id}/cancel")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // The build log is on the node, which is gone.
    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/builds/{build_id}/logs")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (504, "node_unreachable"));

    // A queued build is cancelled at once.
    let (mut socket, _) = open(&server, &node).await;
    let queued = start_build(&server, &owner, &app_id).await;
    let queued_id = queued["id"].as_str().unwrap().to_string();
    // The snapshot that carries the queued build to the node.
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        spec_build_ids(&snapshot),
        vec![build_id.clone(), queued_id.clone()]
    );
    let res = owner
        .post(server.url(&format!("/api/v1/builds/{queued_id}/cancel")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "cancel: {}", res.text().await.unwrap());
    let cancelled: Value = res.json().await.unwrap();
    assert_eq!(cancelled["status"], "cancelled");
    assert!(cancelled["finished_at"].is_string());
    assert!(event_data(&server, "build.cancelled").await["build_id"] == queued_id);
    // A build cancelled before the node started it has no log to keep: the
    // node's list is back to the kept failed build.
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        spec_build_ids(&snapshot),
        vec![build_id.clone()],
        "{snapshot}"
    );
    // A second cancel of a cancelled build is idempotent.
    let res = owner
        .post(server.url(&format!("/api/v1/builds/{queued_id}/cancel")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);

    // A running build is cancelled through the durable marker, which the
    // desired state carries until the node confirms.
    let running = start_build(&server, &owner, &app_id).await;
    let running_id = running["id"].as_str().unwrap().to_string();
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        spec_build_ids(&snapshot),
        vec![build_id.clone(), running_id.clone()]
    );
    assert_eq!(spec_build(&snapshot, &running_id)["cancelled"], false);
    send(
        &mut socket,
        report(vec![build_status(&running_id, BuildState::Building)]),
    )
    .await;
    // Once the newer build started, the failed one is no longer kept.
    let snapshot = loop {
        let snapshot = recv_type(&mut socket, "desired_state").await;
        if spec_build_ids(&snapshot) == vec![running_id.clone()] {
            break snapshot;
        }
    };
    assert_eq!(spec_build(&snapshot, &running_id)["cancelled"], false);
    let res = owner
        .post(server.url(&format!("/api/v1/builds/{running_id}/cancel")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "cancel: {}", res.text().await.unwrap());
    let recorded: Value = res.json().await.unwrap();
    assert_eq!(recorded["status"], "building");
    assert!(
        recorded["error"].is_null(),
        "the marker is not an API error"
    );
    let snapshot = recv_type(&mut socket, "desired_state").await;
    let builds = snapshot["body"]["spec"]["builds"].as_array().unwrap();
    assert_eq!(builds.len(), 1, "{snapshot}");
    assert_eq!(builds[0]["build_id"], running_id);
    assert_eq!(builds[0]["cancelled"], true);
    // A second cancel while the marker is set changes nothing.
    let res = owner
        .post(server.url(&format!("/api/v1/builds/{running_id}/cancel")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    // The marker survives a later status change of the running build.
    send(
        &mut socket,
        report(vec![build_status(&running_id, BuildState::Exporting)]),
    )
    .await;
    let exporting = wait_for_status(&server, &owner, &running_id, "exporting").await;
    assert!(exporting["error"].is_null());
    let (marker,): (Option<String>,) = sqlx::query_as("SELECT error FROM builds WHERE id = ?")
        .bind(&running_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(marker.as_deref(), Some("cancel requested"));

    send(
        &mut socket,
        report(vec![build_status(&running_id, BuildState::Cancelled)]),
    )
    .await;
    let build = wait_for_status(&server, &owner, &running_id, "cancelled").await;
    assert!(build["error"].is_null());
    // The cancelled build ran on the node: it is kept, as the failed one was.
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        spec_build_ids(&snapshot),
        vec![running_id.clone()],
        "{snapshot}"
    );
    assert_eq!(spec_build(&snapshot, &running_id)["cancelled"], true);

    // Deleting an app with a running build drops the build and the node's
    // next snapshot no longer carries it.
    let doomed = start_build(&server, &owner, &app_id).await;
    let doomed_id = doomed["id"].as_str().unwrap().to_string();
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert_eq!(
        spec_build_ids(&snapshot),
        vec![running_id.clone(), doomed_id.clone()]
    );
    let res = owner
        .delete(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());
    let (builds,): (i64,) = sqlx::query_as("SELECT count(*) FROM builds WHERE app_id = ?")
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(builds, 0);
    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/builds/{doomed_id}")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
    let snapshot = recv_type(&mut socket, "desired_state").await;
    assert!(
        snapshot["body"]["spec"]["builds"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "{snapshot}"
    );
}

#[tokio::test]
async fn a_build_log_pages_from_the_node_and_streams_over_sse() {
    // A session timeout long enough for the one-second status poll of the
    // live stream; no provider is involved.
    let server = start_with_cloudflare("http://127.0.0.1:1", "http://127.0.0.1:1").await;
    let owner = server.owner().await;
    let project_id = create_project(&server, &owner).await;
    let node = enrolled(&server, &owner).await;
    let (mut socket, _) = open(&server, &node).await;
    let app = create_git_app(&server, &owner, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    set_token_env(&server, &owner, &app_id).await;
    let build = start_build(&server, &owner, &app_id).await;
    let build_id = build["id"].as_str().unwrap().to_string();
    // A terminal build ends the page with `next_cursor: null`.
    send(
        &mut socket,
        report(vec![build_status(&build_id, BuildState::Failed)]),
    )
    .await;
    wait_for_status(&server, &owner, &build_id, "failed").await;

    let lines = json!([
        { "time": "2026-11-02T10:00:00Z", "stream": "stdout", "line": "one", "seq": 1 },
        { "time": "2026-11-02T10:00:01Z", "stream": "stderr", "line": "two", "seq": 2, "vertex": 3 },
        { "time": "2026-11-02T10:00:02Z", "stream": "stdout", "line": "three", "seq": 3 },
    ]);

    let client = owner.clone();
    let url = server.url(&format!("/api/v1/builds/{build_id}/logs?limit=2"));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });
    let command = recv_type(&mut socket, "command").await;
    assert_eq!(command["body"]["kind"], "build_logs_query");
    assert_eq!(command["body"]["params"]["build_id"], build_id);
    assert_eq!(command["body"]["params"]["after_seq"], 0);
    assert_eq!(command["body"]["params"]["limit"], 2);
    // The node returns at most `limit` lines.
    let limited = json!([lines[0].clone(), lines[1].clone()]);
    send_text(
        &mut socket,
        json!({
            "v": 1,
            "type": "command_result",
            "id": "msg_test_result",
            "re": command["id"],
            "body": { "status": "ok", "lines": limited },
        })
        .to_string(),
    )
    .await;
    let page: Value = http.await.unwrap().json().await.unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "{page}");
    assert_eq!(items[0]["seq"], 1);
    assert_eq!(items[0]["stream"], "stdout");
    assert_eq!(items[1]["seq"], 2);
    assert_eq!(items[1]["vertex"], 3);
    assert_eq!(page["next_cursor"], "2");

    let client = owner.clone();
    let url = server.url(&format!("/api/v1/builds/{build_id}/logs?limit=2&cursor=2"));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });
    let command = recv_type(&mut socket, "command").await;
    assert_eq!(command["body"]["params"]["after_seq"], 2);
    send_text(
        &mut socket,
        json!({
            "v": 1,
            "type": "command_result",
            "id": "msg_test_result",
            "re": command["id"],
            "body": { "status": "ok", "lines": [lines[2].clone()] },
        })
        .to_string(),
    )
    .await;
    let page: Value = http.await.unwrap().json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert!(page["next_cursor"].is_null(), "{page}");

    // A node that does not have the build answers `not_found`.
    let client = owner.clone();
    let url = server.url(&format!("/api/v1/builds/{build_id}/logs"));
    let http = tokio::spawn(async move { client.get(url).send().await.unwrap() });
    let command = recv_type(&mut socket, "command").await;
    send_text(
        &mut socket,
        json!({
            "v": 1,
            "type": "command_result",
            "id": "msg_test_result",
            "re": command["id"],
            "body": { "status": "error", "detail": "this node has no such build", "error": { "code": "not_found", "message": "this node has no such build" } },
        })
        .to_string(),
    )
    .await;
    let (status, code, _) = problem(http.await.unwrap()).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // The live stream resumes after `Last-Event-ID` and ends when the node
    // ends it; a status change in between is a `status` frame.
    let live = start_build(&server, &owner, &app_id).await;
    let live_id = live["id"].as_str().unwrap().to_string();
    recv_type(&mut socket, "desired_state").await;
    send(
        &mut socket,
        report(vec![build_status(&live_id, BuildState::Preparing)]),
    )
    .await;

    let token = owner_token(&server).await;
    let path = format!("/api/v1/builds/{live_id}/logs/stream");
    let mut sse = Sse::open(&server, &path, &token, Some("2")).await;
    let opened = recv_type(&mut socket, "stream_open").await;
    assert_eq!(opened["body"]["kind"], "build_logs");
    assert_eq!(opened["body"]["params"]["build_id"], live_id);
    assert_eq!(opened["body"]["params"]["after_seq"], 2);
    let stream_id = opened["body"]["stream_id"].as_str().unwrap().to_string();

    send_text(
        &mut socket,
        json!({
            "v": 1,
            "type": "stream_data",
            "id": "msg_test_data",
            "body": {
                "stream_id": stream_id,
                "seq": 1,
                "lines": [
                    { "time": "2026-11-02T10:00:03Z", "stream": "stdout", "line": "four", "seq": 3, "vertex": 4 },
                    { "time": "2026-11-02T10:00:04Z", "stream": "stdout", "line": "five", "seq": 4 },
                ],
            },
        })
        .to_string(),
    )
    .await;
    // A status change reaches the stream through the row poll.
    send(
        &mut socket,
        report(vec![build_status(&live_id, BuildState::Building)]),
    )
    .await;
    let mut seen_status = false;
    let mut seen_logs = Vec::new();
    loop {
        let frame = sse.next_event().await;
        match field(&frame, "event").as_deref() {
            Some("log") => {
                if let Some(data) = field(&frame, "data") {
                    let line: Value = serde_json::from_str(&data).unwrap();
                    seen_logs.push((
                        field(&frame, "id").unwrap(),
                        line["line"].as_str().unwrap_or_default().to_string(),
                    ));
                }
            }
            Some("status") => {
                let data: Value = serde_json::from_str(&field(&frame, "data").unwrap()).unwrap();
                if data["status"] == "building" {
                    seen_status = true;
                }
            }
            Some("end") => break,
            Some(other) => panic!("unexpected SSE event {other}: {frame}"),
            None => {}
        }
        if seen_status && seen_logs.len() == 2 {
            // The node ends the stream once the build is terminal.
            send(
                &mut socket,
                report(vec![build_status(&live_id, BuildState::Cancelled)]),
            )
            .await;
            send_text(
                &mut socket,
                json!({
                    "v": 1,
                    "type": "stream_end",
                    "id": "msg_test_end",
                    "body": { "stream_id": stream_id, "reason": "completed" },
                })
                .to_string(),
            )
            .await;
        }
    }
    assert!(seen_status, "a status frame for `building`");
    assert_eq!(
        seen_logs,
        vec![
            ("3".to_string(), "four".to_string()),
            ("4".to_string(), "five".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_viewer_reads_builds_but_cannot_start_one() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&server, &owner).await;
    let app = create_git_app(&server, &owner, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    // No node is needed: the app has no build yet.
    let list: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list["items"].as_array().unwrap().is_empty());
    for (status, shown) in [
        (
            owner
                .get(server.url("/api/v1/builds/bld_00000000000000000000000001"))
                .send()
                .await
                .unwrap(),
            "an unknown build",
        ),
        (
            owner
                .get(server.url("/api/v1/apps/app_00000000000000000000000001/builds"))
                .send()
                .await
                .unwrap(),
            "an unknown app",
        ),
    ] {
        let (status, code, _) = problem(status).await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{shown}");
    }

    server
        .insert_user("viewer@example.com", "a viewer password", "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", "a viewer password")
        .await;
    // A viewer holds every `:read` scope, so it reads the app and its builds.
    let res = viewer
        .get(server.url(&format!("/api/v1/apps/{app_id}/builds")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    // It does not hold `app:deploy`, so it cannot start a build.
    let (status, code, _) = problem(
        viewer
            .post(server.url(&format!("/api/v1/apps/{app_id}/builds")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    let audit = server.audit().await;
    assert!(audit.iter().any(|(operation, result, _)| {
        operation == "POST /api/v1/apps/{app_id}/builds" && result == "refused"
    }));
}

// ---------------------------------------------------------------------------
// The raw SSE client
// ---------------------------------------------------------------------------

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Reads the SSE body of one request over a raw HTTP/1.0 connection, which
/// carries no chunked framing (it ends when the connection closes).
struct Sse {
    stream: TcpStream,
    buffer: Vec<u8>,
    headers_checked: bool,
}

impl Sse {
    async fn open(server: &Server, path: &str, token: &str, last_event: Option<&str>) -> Self {
        let mut stream = TcpStream::connect(server.address()).await.unwrap();
        let mut request = format!(
            "GET {path} HTTP/1.0\r\nHost: {}\r\nAuthorization: Bearer {token}\r\n\
             Accept: text/event-stream\r\n",
            server.address()
        );
        if let Some(last_event) = last_event {
            request.push_str(&format!("Last-Event-ID: {last_event}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        Self {
            stream,
            buffer: Vec::new(),
            headers_checked: false,
        }
    }

    async fn read_headers(&mut self) {
        loop {
            if let Some(position) = find(&self.buffer, b"\r\n\r\n") {
                let headers =
                    String::from_utf8_lossy(&self.buffer[..position]).to_ascii_lowercase();
                assert!(
                    headers.starts_with("http/1.1 200") || headers.starts_with("http/1.0 200"),
                    "{headers}"
                );
                assert!(headers.contains("text/event-stream"), "{headers}");
                assert!(headers.contains("cache-control: no-cache"), "{headers}");
                self.buffer.drain(..position + 4);
                self.headers_checked = true;
                return;
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "the response headers ended early");
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    async fn next_frame(&mut self) -> Option<String> {
        if !self.headers_checked {
            self.read_headers().await;
        }
        loop {
            if let Some(position) = find(&self.buffer, b"\n\n") {
                let frame = String::from_utf8_lossy(&self.buffer[..position]).to_string();
                self.buffer.drain(..position + 2);
                return Some(frame);
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    async fn next_event(&mut self) -> String {
        loop {
            let frame = timeout(Duration::from_secs(5), self.next_frame())
                .await
                .expect("an SSE frame within 5 s")
                .expect("the stream is open");
            if frame.lines().any(|line| line.starts_with("event:")) {
                return frame;
            }
        }
    }
}

fn field(frame: &str, name: &str) -> Option<String> {
    frame.lines().find_map(|line| {
        line.strip_prefix(&format!("{name}:"))
            .map(|value| value.trim_start().to_string())
    })
}

/// A bearer token for the owner, for requests that hold a streaming body open.
async fn owner_token(server: &Server) -> String {
    let (user_id,): (String,) = sqlx::query_as("SELECT id FROM users ORDER BY created_at LIMIT 1")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let secret = format!("tok_{}", "B".repeat(26));
    let id = quoro_types::new_id(quoro_types::Prefix::User);
    sqlx::query(
        "INSERT INTO api_tokens (id, user_id, name, token_hash, created_at) VALUES (?, ?, 'sse', ?, ?)",
    )
    .bind(id)
    .bind(user_id)
    .bind(quoro_control_plane::store::sha256_hex(secret.as_bytes()))
    .bind(now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    secret
}
