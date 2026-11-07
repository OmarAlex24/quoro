//! Warm copies on the primary: what both nodes of a `warm` app are sent, and
//! `App.warm` from their reports (node-agent-protocol.md sections 5.3, 5.11
//! and 6.13, ADR 0173, ADR 0174), over real HTTP and WebSocket.
//!
//! `updateApp` does not accept `ha_policy: warm` yet, so the tests write the
//! warm app's rows directly.

mod common;

use base64::Engine as _;
use common::{Server, start};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Kvm as WireKvm, NodeMessage, StatusReport, WarmRole, WarmStatus,
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
    "ghcr.io/example/db@sha256:0000000000000000000000000000000000000000000000000000000000000000";

struct Enrolled {
    node_id: String,
    credential: String,
}

/// Enrolls a node whose identity key comes from `seed`.
async fn enroll(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> Enrolled {
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
                &quoro_proto::sign::signing_key_from_seed(&[seed; 32]).verifying_key()
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

/// `hello` with a mesh key, so the primary gives the node a mesh address.
fn hello(node_id: &str, mesh_seed: u8) -> NodeMessage {
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
        mesh_public_key: Some(base64::engine::general_purpose::STANDARD.encode([mesh_seed; 32])),
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

async fn send(socket: &mut Socket, message: NodeMessage) {
    let text = message
        .to_frame("msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D", None)
        .to_text();
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
            WsMessage::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            WsMessage::Close(_) => panic!("the socket closed"),
            _ => {}
        }
    }
}

/// The `spec` of the first `desired_state` that `wanted` accepts; earlier
/// frames are skipped.
async fn spec_where(socket: &mut Socket, wanted: impl Fn(&Value) -> bool) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == "desired_state" && wanted(&frame["body"]["spec"]) {
            return frame["body"]["spec"].clone();
        }
    }
}

/// Sends `hello` and reads up to the first `desired_state`.
async fn open(server: &Server, node: &Enrolled, mesh_seed: u8) -> Socket {
    let mut socket = connect(server, &node.credential).await;
    send(&mut socket, hello(&node.node_id, mesh_seed)).await;
    spec_where(&mut socket, |_| true).await;
    socket
}

fn report(warm: Vec<WarmStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev: 0,
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
        warm,
        volumes: Vec::new(),
    })
}

fn warm_entry(volume_id: &str, app_id: &str, role: WarmRole) -> WarmStatus {
    WarmStatus {
        volume_id: volume_id.to_string(),
        app_id: app_id.to_string(),
        role,
        snapshot_seq: None,
        snapshot_taken_at: None,
        completed_at: None,
        allocated_bytes: None,
        extent_count: None,
        frozen: None,
        rejected_token: None,
        rejected_at: None,
        error: None,
    }
}

/// A `warm` app on `source` with `target` as its warm node: an app made
/// through the API, then its VM, a `ready` volume and a lease with token 4
/// written as a deployment, the volume API and the policy change would.
/// Returns `(app_id, vm_id, volume_id)`.
async fn warm_app(
    server: &Server,
    owner: &reqwest::Client,
    source: &str,
    target: &str,
) -> (String, String, String) {
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "warm", "name": "warm" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let project_id = project["id"].as_str().unwrap().to_string();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": "db", "image": IMAGE, "port": 8080 }))
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

    let pool = server.state.store.pool();
    let now = quoro_control_plane::store::now_rfc3339();
    let vm_id = "vm_0000000000000000000000000W".to_string();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
         image, desired_status, observed_status, ip, restart_generation, restart_count, \
         last_exit_code, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'db-1', 1, 512, 4, ?, 'running', 'running', NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(&vm_id)
    .bind(&project_id)
    .bind(&app_id)
    .bind(source)
    .bind(IMAGE)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    let volume_id = "vol_0000000000000000000000000W".to_string();
    sqlx::query(
        "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, allocated_bytes, \
         restored_from_backup_id, error, created_at, updated_at) \
         VALUES (?, ?, ?, 2, '/data', 'ready', NULL, NULL, NULL, ?, ?)",
    )
    .bind(&volume_id)
    .bind(&app_id)
    .bind(source)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, fencing_token, \
         granted_at) VALUES ('lse_0000000000000000000000000W', 'app', ?, ?, 'local', 4, ?)",
    )
    .bind(&app_id)
    .bind(source)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE apps SET stateful = 1, ha_policy = 'warm', warm_node_id = ? WHERE id = ?")
        .bind(target)
        .bind(&app_id)
        .execute(pool)
        .await
        .unwrap();
    (app_id, vm_id, volume_id)
}

/// The app's `warm` once `ready` accepts it, read every 50 ms for 30 s.
async fn warm_where(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
    ready: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let app: Value = owner
            .get(server.url(&format!("/api/v1/apps/{app_id}")))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let warm = app["warm"].clone();
        if ready(&warm) {
            return warm;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "App.warm never matched: {warm}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn both_nodes_get_their_part_and_app_warm_follows_their_reports() {
    let server = start().await;
    let owner = server.owner().await;
    let source = enroll(&server, &owner, "source", 3).await;
    let target = enroll(&server, &owner, "target", 4).await;
    let mut on_source = open(&server, &source, 1).await;
    let mut on_target = open(&server, &target, 2).await;
    let (app_id, vm_id, volume_id) =
        warm_app(&server, &owner, &source.node_id, &target.node_id).await;
    let mesh_address = |node_id: &str| {
        let pool = server.state.store.pool().clone();
        let node_id = node_id.to_string();
        async move {
            sqlx::query_scalar::<_, String>("SELECT mesh_address FROM nodes WHERE id = ?")
                .bind(node_id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    let source_address = mesh_address(&source.node_id).await;
    let target_address = mesh_address(&target.node_id).await;

    // Only the source is asked to rebuild: the warm node follows because its
    // peer's warm entry appeared.
    server.state.hub.node_changed(&source.node_id);
    let spec = spec_where(&mut on_source, |spec| {
        spec["volumes"][0]["warm"].is_object()
    })
    .await;
    let warm = &spec["volumes"][0]["warm"];
    assert_eq!(warm["role"], "source");
    assert_eq!(warm["peer_node_id"], target.node_id.as_str());
    assert_eq!(warm["peer_address"], target_address.as_str());
    assert_eq!(warm["port"], 51822);
    assert_eq!(warm["interval_s"], 30);
    assert_eq!(warm["fencing_token"], 4);

    let spec = spec_where(&mut on_target, |spec| {
        spec["volumes"]
            .as_array()
            .is_some_and(|volumes| !volumes.is_empty())
    })
    .await;
    assert_eq!(spec["volumes"][0]["volume_id"], volume_id.as_str());
    assert_eq!(spec["volumes"][0]["state"], "present");
    assert_eq!(spec["volumes"][0]["size_gb"], 2);
    let warm = &spec["volumes"][0]["warm"];
    assert_eq!(warm["role"], "target");
    assert_eq!(warm["peer_node_id"], source.node_id.as_str());
    assert_eq!(warm["peer_address"], source_address.as_str());
    let standby = &spec["vms"][0];
    assert_eq!(standby["vm_id"], vm_id.as_str());
    assert_eq!(standby["standby"], true);
    assert_eq!(standby["desired_status"], "stopped");
    assert_eq!(standby["image_digest"], IMAGE);
    assert_eq!(spec["vms"].as_array().unwrap().len(), 1);
    assert_eq!(spec["routes"], json!([]));

    // Before any report the copy is syncing.
    let warm = warm_where(&server, &owner, &app_id, |warm| warm.is_object()).await;
    assert_eq!(warm["state"], "syncing");
    assert_eq!(warm["node_id"], target.node_id.as_str());
    assert_eq!(warm["last_snapshot_seq"], Value::Null);

    // The two sides report: App.warm shows the last shipped snapshot.
    let taken_at = (time::OffsetDateTime::now_utc() - time::Duration::seconds(5))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    send(
        &mut on_source,
        report(vec![WarmStatus {
            snapshot_seq: Some(3),
            extent_count: Some(7),
            ..warm_entry(&volume_id, &app_id, WarmRole::Source)
        }]),
    )
    .await;
    send(
        &mut on_target,
        report(vec![WarmStatus {
            snapshot_seq: Some(3),
            snapshot_taken_at: Some(taken_at.clone()),
            completed_at: Some(taken_at),
            ..warm_entry(&volume_id, &app_id, WarmRole::Target)
        }]),
    )
    .await;
    let warm = warm_where(&server, &owner, &app_id, |warm| {
        warm["last_snapshot_seq"] == 3 && warm["extent_count"] == 7
    })
    .await;
    assert_eq!(warm["state"], "in_sync", "{warm}");
    assert!(warm["lag_s"].as_i64().unwrap() >= 5, "{warm}");
    assert!(warm["last_shipped_at"].is_string());
    assert_eq!(warm["error"], Value::Null);

    // An error on the source fails the copy.
    send(
        &mut on_source,
        report(vec![WarmStatus {
            error: Some("connect timed out".to_string()),
            ..warm_entry(&volume_id, &app_id, WarmRole::Source)
        }]),
    )
    .await;
    let warm = warm_where(&server, &owner, &app_id, |warm| warm["state"] == "failed").await;
    assert_eq!(warm["error"], "source: connect timed out");

    // A copy the warm node reports but no longer targets gets a `deleted`
    // entry; its own copy stays.
    send(
        &mut on_target,
        report(vec![
            warm_entry(&volume_id, &app_id, WarmRole::Target),
            warm_entry(
                "vol_0000000000000000000000000X",
                "app_0000000000000000000000000X",
                WarmRole::Target,
            ),
        ]),
    )
    .await;
    let spec = spec_where(&mut on_target, |spec| {
        spec["volumes"]
            .as_array()
            .is_some_and(|volumes| volumes.len() == 2)
    })
    .await;
    assert_eq!(spec["volumes"][0]["volume_id"], volume_id.as_str());
    assert_eq!(spec["volumes"][0]["state"], "present");
    assert_eq!(
        spec["volumes"][1]["volume_id"],
        "vol_0000000000000000000000000X"
    );
    assert_eq!(spec["volumes"][1]["state"], "deleted");
    assert!(spec["volumes"][1].get("warm").is_none());

    // A new token on the source's lease reaches the warm node as well.
    sqlx::query("UPDATE leases SET fencing_token = 5")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    server.state.hub.node_changed(&source.node_id);
    spec_where(&mut on_source, |spec| {
        spec["volumes"][0]["warm"]["fencing_token"] == 5
    })
    .await;
    let spec = spec_where(&mut on_target, |spec| spec["vms"][0]["fencing_token"] == 5).await;
    assert_eq!(spec["volumes"][0]["warm"]["fencing_token"], 5);

    // A warm node that cannot be one fails the copy, with the reason.
    sqlx::query("UPDATE nodes SET volume_snapshot = 'pause_copy' WHERE id = ?")
        .bind(&target.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let warm = warm_where(&server, &owner, &app_id, |warm| {
        warm["error"] == "the warm node does not snapshot with reflink"
    })
    .await;
    assert_eq!(warm["state"], "failed");
}

#[tokio::test]
async fn a_pair_that_goes_away_while_the_source_is_away_reaches_the_warm_node() {
    let server = start().await;
    let owner = server.owner().await;
    let source = enroll(&server, &owner, "source", 3).await;
    let target = enroll(&server, &owner, "target", 4).await;
    let mut on_source = open(&server, &source, 1).await;
    let mut on_target = open(&server, &target, 2).await;
    let (app_id, _, _) = warm_app(&server, &owner, &source.node_id, &target.node_id).await;
    server.state.hub.node_changed(&source.node_id);
    spec_where(&mut on_source, |spec| {
        spec["volumes"][0]["warm"].is_object()
    })
    .await;
    spec_where(&mut on_target, |spec| {
        spec["vms"].as_array().is_some_and(|vms| vms.len() == 1)
    })
    .await;

    // The source goes away, and the app leaves `warm` meanwhile. Nothing
    // reaches the warm node while the source is away.
    on_source.close(None).await.unwrap();
    drop(on_source);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while server.state.hub.is_connected(&source.node_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the source never left"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sqlx::query("UPDATE apps SET ha_policy = 'none', warm_node_id = NULL WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // The source's first snapshot after it returns no longer carries the
    // pair, which its last one did: the warm node drops the copy's entry and
    // the standby VM.
    let _on_source = open(&server, &source, 1).await;
    let spec = spec_where(&mut on_target, |spec| {
        spec["vms"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    assert!(
        spec["volumes"].as_array().is_none_or(Vec::is_empty),
        "{}",
        spec["volumes"]
    );

    // An app that is not `warm` carries `warm: null` (`openapi.yaml` `App`).
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app.get("warm"), Some(&Value::Null), "{app}");
}

#[tokio::test]
async fn a_pair_gone_before_the_source_first_connects_reaches_the_warm_node() {
    // As after a primary restart: the warm node was sent its part, and the
    // source has had no snapshot from this primary yet. Its first session
    // rebuilds the warm node too.
    let server = start().await;
    let owner = server.owner().await;
    let source = enroll(&server, &owner, "source", 3).await;
    let target = enroll(&server, &owner, "target", 4).await;
    let mut on_target = open(&server, &target, 2).await;
    let (app_id, _, _) = warm_app(&server, &owner, &source.node_id, &target.node_id).await;
    sqlx::query(
        "UPDATE nodes SET mesh_address = '10.43.0.9', volume_snapshot = 'reflink' WHERE id = ?",
    )
    .bind(&source.node_id)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    server.state.hub.node_changed(&target.node_id);
    spec_where(&mut on_target, |spec| {
        spec["vms"].as_array().is_some_and(|vms| vms.len() == 1)
    })
    .await;

    // The app leaves `warm`, then the source connects for the first time.
    sqlx::query("UPDATE apps SET ha_policy = 'none', warm_node_id = NULL WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let _on_source = open(&server, &source, 1).await;
    spec_where(&mut on_target, |spec| {
        spec["vms"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
}

/// `(volume_id, state, has warm)` of each `spec.volumes` entry.
fn volume_states(spec: &Value) -> Vec<(String, String, bool)> {
    spec["volumes"]
        .as_array()
        .map(|volumes| {
            volumes
                .iter()
                .map(|volume| {
                    (
                        volume["volume_id"].as_str().unwrap().to_string(),
                        volume["state"].as_str().unwrap().to_string(),
                        volume.get("warm").is_some_and(|warm| !warm.is_null()),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A copy of a volume and an app the database does not have, which no
/// failover can need: reported next to another copy, it shows which entries
/// that report got.
const GONE_VOLUME: &str = "vol_0000000000000000000000000X";
const GONE_APP: &str = "app_0000000000000000000000000X";

#[tokio::test]
async fn a_copy_the_old_warm_node_still_holds_is_deleted_while_it_reports_it() {
    // ADR 0267: a node keeps reporting a copy it is no longer the target of
    // until it is removed. The primary keeps no report across a restart, so
    // the node's first report is all it knows, as here.
    let server = start().await;
    let owner = server.owner().await;
    let source = enroll(&server, &owner, "source", 3).await;
    let target = enroll(&server, &owner, "target", 4).await;
    let _on_source = open(&server, &source, 1).await;
    let (app_id, _, volume_id) = warm_app(&server, &owner, &source.node_id, &target.node_id).await;
    sqlx::query("UPDATE apps SET ha_policy = 'none', warm_node_id = NULL WHERE id = ?")
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // Before it reports anything, the old warm node is sent nothing for it.
    let mut on_target = connect(&server, &target.credential).await;
    send(&mut on_target, hello(&target.node_id, 2)).await;
    let spec = spec_where(&mut on_target, |_| true).await;
    assert_eq!(volume_states(&spec), Vec::new());

    // Its first report names the copy: the source is healthy, so it gets a
    // `deleted` entry without `warm`, with the volume's size.
    let held = WarmStatus {
        snapshot_seq: Some(3),
        ..warm_entry(&volume_id, &app_id, WarmRole::Target)
    };
    send(&mut on_target, report(vec![held.clone()])).await;
    let spec = spec_where(&mut on_target, |spec| !volume_states(spec).is_empty()).await;
    assert_eq!(
        volume_states(&spec),
        vec![(volume_id.clone(), "deleted".to_string(), false)]
    );
    assert_eq!(spec["volumes"][0]["app_id"], app_id.as_str());
    assert_eq!(spec["volumes"][0]["size_gb"], 2);
    assert!(spec["vms"].as_array().is_none_or(Vec::is_empty));

    // The entry stays while the node reports the copy, so a removal that
    // failed is asked again.
    send(
        &mut on_target,
        report(vec![
            held,
            warm_entry(GONE_VOLUME, GONE_APP, WarmRole::Target),
        ]),
    )
    .await;
    let spec = spec_where(&mut on_target, |spec| volume_states(spec).len() == 2).await;
    let mut expected = vec![
        (volume_id.clone(), "deleted".to_string(), false),
        (GONE_VOLUME.to_string(), "deleted".to_string(), false),
    ];
    expected.sort();
    assert_eq!(volume_states(&spec), expected);

    // A copy the node no longer reports loses its entry.
    send(
        &mut on_target,
        report(vec![warm_entry(GONE_VOLUME, GONE_APP, WarmRole::Target)]),
    )
    .await;
    let spec = spec_where(&mut on_target, |spec| volume_states(spec).len() == 1).await;
    assert_eq!(
        volume_states(&spec),
        vec![(GONE_VOLUME.to_string(), "deleted".to_string(), false)]
    );
    send(&mut on_target, report(Vec::new())).await;
    spec_where(&mut on_target, |spec| volume_states(spec).is_empty()).await;
}

#[tokio::test]
async fn a_copy_a_failover_may_still_need_gets_no_entry_until_the_source_is_healthy() {
    let server = start().await;
    let owner = server.owner().await;
    let source = enroll(&server, &owner, "source", 3).await;
    let target = enroll(&server, &owner, "target", 4).await;
    let _on_source = open(&server, &source, 1).await;
    let mut on_target = open(&server, &target, 2).await;
    let (app_id, _, volume_id) = warm_app(&server, &owner, &source.node_id, &target.node_id).await;

    // The app leaves `warm` while its source is fenced: the copy may be the
    // only good data. The node reports it with a copy no failover can need,
    // and only that one gets an entry.
    let pool = server.state.store.pool();
    sqlx::query("UPDATE leases SET revoked_at = '2026-11-07T10:00:00Z'")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE apps SET ha_policy = 'none', warm_node_id = NULL WHERE id = ?")
        .bind(&app_id)
        .execute(pool)
        .await
        .unwrap();
    send(
        &mut on_target,
        report(vec![
            warm_entry(&volume_id, &app_id, WarmRole::Target),
            warm_entry(GONE_VOLUME, GONE_APP, WarmRole::Target),
        ]),
    )
    .await;
    let spec = spec_where(&mut on_target, |spec| {
        volume_states(spec).iter().any(|(id, ..)| id == GONE_VOLUME)
    })
    .await;
    assert_eq!(
        volume_states(&spec),
        vec![(GONE_VOLUME.to_string(), "deleted".to_string(), false)]
    );

    // The lease is granted again. Nothing wakes the warm node for that, but
    // its next report, unchanged, has the copy it keeps checked again, and
    // the copy gets its entry.
    sqlx::query("UPDATE leases SET revoked_at = NULL")
        .execute(pool)
        .await
        .unwrap();
    send(
        &mut on_target,
        report(vec![
            warm_entry(&volume_id, &app_id, WarmRole::Target),
            warm_entry(GONE_VOLUME, GONE_APP, WarmRole::Target),
        ]),
    )
    .await;
    let spec = spec_where(&mut on_target, |spec| volume_states(spec).len() == 2).await;
    let mut expected = vec![
        (volume_id.clone(), "deleted".to_string(), false),
        (GONE_VOLUME.to_string(), "deleted".to_string(), false),
    ];
    expected.sort();
    assert_eq!(volume_states(&spec), expected);

    // Fenced again, and the other copy is gone: the copy is kept again, with
    // no entry.
    sqlx::query("UPDATE leases SET revoked_at = '2026-11-07T10:05:00Z'")
        .execute(pool)
        .await
        .unwrap();
    let held = || report(vec![warm_entry(&volume_id, &app_id, WarmRole::Target)]);
    send(&mut on_target, held()).await;
    spec_where(&mut on_target, |spec| volume_states(spec).is_empty()).await;

    // Then its volume is deleted: the volume's own node removed the file, and
    // the row went with it, while the warm node still holds the copy. No
    // failover can start from it, so the node's next report, unchanged, gets
    // it its entry, whatever the lease.
    sqlx::query("DELETE FROM volumes WHERE id = ?")
        .bind(&volume_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE apps SET stateful = 0 WHERE id = ?")
        .bind(&app_id)
        .execute(pool)
        .await
        .unwrap();
    send(&mut on_target, held()).await;
    let spec = spec_where(&mut on_target, |spec| volume_states(spec).len() == 1).await;
    assert_eq!(
        volume_states(&spec),
        vec![(volume_id.clone(), "deleted".to_string(), false)]
    );
    assert_eq!(spec["volumes"][0]["size_gb"], 1, "the row is gone");
}
