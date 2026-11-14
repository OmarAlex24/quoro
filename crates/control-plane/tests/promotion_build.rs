//! The database a promotion rebuilds from the followed change log
//! (`node-agent-protocol.md` section 7.3 step 4, issue #19): a real primary
//! runs a broad set of operations, its change log is built into a new file
//! with `rebuild::database::build`, and every replicated table of the new
//! file is compared row by row with the primary's.

mod common;

use axum::Router;
use axum::extract::{Path as UrlPath, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use common::{Server, TestDir, replay};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::api::audit_exports::CreateAuditExportRequest;
use quoro_control_plane::audit_export;
use quoro_control_plane::auth::{Credential, Identity, Principal, UserIdentity};
use quoro_control_plane::backups::{Bucket, BucketFactory, BucketStore, RepositoryTarget};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::deploy;
use quoro_control_plane::rebuild::database::{self, Built};
use quoro_control_plane::request::CallCtx;
use quoro_control_plane::store::Store;
use quoro_control_plane::vulns::osv::{Affected, Event, Range, Record, Severity};
use quoro_control_plane::vulns::subjects;
use quoro_proto::custody::{self, CustodyKey};
use quoro_proto::wire::{
    self, Capacity as WireCapacity, EventBody, Health, Heartbeat, Kvm as WireKvm, NodeMessage,
    ObservedStatus, QuarantineStatus, StatusReport, VmStatus, VolumeState as WireVolumeState,
    VolumeStatus,
};
use quoro_providers::object_store::{MemoryStore, ObjectInfo, ObjectStore, ObjectStoreError};
use quoro_types::{Prefix, Role, new_id};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// The buckets: one in-memory store for the backup repository
// ---------------------------------------------------------------------------

struct MemoryBuckets(Arc<MemoryStore>);

impl BucketStore for MemoryBuckets {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.0.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.list(prefix))
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.delete(key))
    }
}

impl BucketFactory for MemoryBuckets {
    fn open(
        &self,
        _target: &RepositoryTarget,
    ) -> Result<Bucket, quoro_control_plane::error::ApiError> {
        Ok(Arc::new(MemoryBuckets(Arc::clone(&self.0))))
    }
}

// ---------------------------------------------------------------------------
// A Cloudflare tunnel API mock: just enough for the tunnel manager to create
// a node's tunnel and store its run token
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Tunnels {
    named: Vec<(String, String)>,
}

type Shared = Arc<Mutex<Tunnels>>;

fn ok(result: Value) -> Response {
    axum::Json(json!({"success": true, "errors": [], "result": result})).into_response()
}

async fn list_tunnels(
    State(state): State<Shared>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let name = params.get("name").cloned().unwrap_or_default();
    let tunnels = state.lock().unwrap();
    ok(Value::Array(
        tunnels
            .named
            .iter()
            .filter(|(_, n)| name.is_empty() || *n == name)
            .map(|(id, n)| json!({"id": id, "name": n}))
            .collect(),
    ))
}

async fn create_tunnel(State(state): State<Shared>, body: axum::body::Bytes) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let name = body["name"].as_str().unwrap_or_default().to_string();
    let mut tunnels = state.lock().unwrap();
    let id = format!("tun{}", tunnels.named.len() + 1);
    tunnels.named.push((id.clone(), name.clone()));
    ok(json!({"id": id, "name": name}))
}

async fn tunnel_token(UrlPath(params): UrlPath<HashMap<String, String>>) -> Response {
    ok(json!(format!("tunnel-token-{}", params["id"])))
}

async fn anything() -> Response {
    ok(json!([]))
}

async fn start_mock() -> String {
    let state: Shared = Arc::default();
    let app = Router::new()
        .route(
            "/accounts/{account}/cfd_tunnel",
            get(list_tunnels).post(create_tunnel),
        )
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/token",
            get(tunnel_token),
        )
        .route(
            "/accounts/{account}/cfd_tunnel/{id}/configurations",
            put(anything),
        )
        .route("/zones/{zone}/dns_records", get(anything).post(anything))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    base
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

struct Enrolled {
    node_id: String,
    credential: String,
}

async fn join_token(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "join token: {}",
        res.text().await.unwrap()
    );
    let body: Value = res.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> Enrolled {
    common::settle_voters(server).await;
    let token = join_token(server, owner).await;
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token,
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

fn hello(node_id: &str, custody_public_key: String) -> NodeMessage {
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
        custody_public_key: Some(custody_public_key),
        mesh_rules: None,
        control_plane_urls: Vec::new(),
    })
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
            time: "2026-11-13T10:15:32Z".to_string(),
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

async fn recv_type(socket: &mut Socket, kind: &str) -> Value {
    loop {
        let frame = recv_frame(socket).await;
        if frame["type"] == kind {
            return frame;
        }
    }
}

/// Opens the node's session with its custody key, so the primary binds the
/// key and seals every key in use to it (section 7.2).
async fn open(server: &Server, node: &Enrolled, key: &CustodyKey) -> Socket {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", node.credential).parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    let (mut socket, _) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    send(
        &mut socket,
        hello(&node.node_id, key.public_key().to_base64()),
    )
    .await;
    recv_type(&mut socket, "welcome").await;
    recv_type(&mut socket, "desired_state").await;
    socket
}

fn status_report(
    applied_rev: u64,
    volumes: Vec<VolumeStatus>,
    quarantines: Vec<QuarantineStatus>,
) -> NodeMessage {
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
        volumes,
        images: Vec::new(),
        warm: Vec::new(),
        quarantines,
    })
}

/// The volume `volume_id` as its node reports it once it is ready.
fn ready(volume_id: &str) -> VolumeStatus {
    VolumeStatus {
        volume_id: volume_id.to_string(),
        state: WireVolumeState::Ready,
        allocated_bytes: Some(4096),
        error: None,
    }
}

/// A Linux kernel record that affects the node's kernel, 6.9.0.
fn kernel_record() -> Record {
    Record {
        id: "CVE-2026-98431".to_owned(),
        modified: "2026-01-01T00:00:00Z".to_owned(),
        summary: Some("CVE-2026-98431 summary".to_owned()),
        severity: vec![Severity {
            kind: "CVSS_V3".to_owned(),
            score: "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H".to_owned(),
        }],
        affected: vec![Affected {
            ecosystem: "Linux".to_owned(),
            package: "Kernel".to_owned(),
            ranges: vec![Range {
                kind: "ECOSYSTEM".to_owned(),
                events: vec![
                    Event {
                        introduced: Some("6.9.0".to_owned()),
                        ..Default::default()
                    },
                    Event {
                        fixed: Some("6.9.10".to_owned()),
                        ..Default::default()
                    },
                ],
            }],
            versions: Vec::new(),
        }],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/// POSTs `body` to `path` and returns the body of the expected status.
async fn post(
    client: &reqwest::Client,
    server: &Server,
    path: &str,
    body: Value,
    status: u16,
) -> Value {
    let res = client
        .post(server.url(path))
        .json(&body)
        .send()
        .await
        .unwrap();
    let got = res.status().as_u16();
    let text = res.text().await.unwrap();
    assert_eq!(got, status, "POST {path}: {text}");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

/// POSTs to `path` with no body and returns the body of the expected
/// status.
async fn post_empty(client: &reqwest::Client, server: &Server, path: &str, status: u16) -> Value {
    let res = client.post(server.url(path)).send().await.unwrap();
    let got = res.status().as_u16();
    let text = res.text().await.unwrap();
    assert_eq!(got, status, "POST {path}: {text}");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

async fn put_env(owner: &reqwest::Client, server: &Server, app_id: &str, names: &[&str]) {
    let items: Vec<Value> = names
        .iter()
        .map(|name| json!({ "name": name, "value": format!("value of {name}") }))
        .collect();
    let res = owner
        .put(server.url(&format!("/api/v1/apps/{app_id}/env")))
        .json(&json!({ "items": items }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "env: {}", res.text().await.unwrap());
}

/// Polls `sql`, one row or none, until it returns a row.
async fn wait_for(server: &Server, sql: &str, what: &str) {
    for _ in 0..400 {
        let found: Option<i64> = sqlx::query_scalar(sql)
            .fetch_optional(server.state.store.pool())
            .await
            .unwrap();
        if found.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{what} never happened");
}

fn owner_ctx(owner_id: &str) -> CallCtx {
    CallCtx::new(
        Identity {
            principal: Principal::User(UserIdentity {
                user_id: owner_id.to_string(),
                email: common::OWNER_EMAIL.to_string(),
                name: "Owner".to_string(),
                role: Role::Owner,
            }),
            credential: Credential::Session {
                session_id: "ses_test".to_string(),
            },
        },
        new_id(Prefix::Request),
        audit_export::CREATE_OPERATION,
        &axum::http::HeaderMap::new(),
    )
}

// ---------------------------------------------------------------------------
// Reading a database
// ---------------------------------------------------------------------------

/// The row `filter` selects in `table`, as a payload carrying every column
/// under its name: a JSON object or array in a TEXT column parsed, an
/// integer as an integer, NULL as null. What a writer that logs the whole
/// row writes.
async fn whole_row(conn: &mut sqlx::SqliteConnection, table: &str, id: &str) -> Value {
    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
            .bind(table)
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    let mut payload = Map::new();
    for name in names {
        let (kind, text): (String, Option<String>) = sqlx::query_as(&format!(
            "SELECT typeof(\"{name}\"), CAST(\"{name}\" AS TEXT) FROM \"{table}\" WHERE id = ?"
        ))
        .bind(id)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        let value = match (kind.as_str(), text) {
            (_, None) => Value::Null,
            ("integer", Some(text)) => Value::from(text.parse::<i64>().unwrap()),
            (_, Some(text)) => match serde_json::from_str::<Value>(&text) {
                Ok(parsed @ (Value::Object(_) | Value::Array(_))) => parsed,
                _ => Value::String(text),
            },
        };
        payload.insert(name, value);
    }
    Value::Object(payload)
}

/// Every table of `store` but `meta` and `change_log`.
async fn tables(store: &Store) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT IN ('meta', 'change_log') \
         ORDER BY name",
    )
    .fetch_all(store.pool())
    .await
    .unwrap()
}

async fn count(store: &Store, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{table}\""))
        .fetch_one(store.pool())
        .await
        .unwrap()
}

/// A copy of `server`'s database as it stands, taken at one instant:
/// `VACUUM INTO` reads one snapshot, so the copy's tables are exactly what
/// its change log says.
async fn snapshot(server: &Server, dir: &Path) -> Store {
    let path = dir.join("primary.db");
    sqlx::query("VACUUM INTO ?")
        .bind(path.to_str().unwrap())
        .execute(server.state.store.pool())
        .await
        .unwrap();
    Store::open(&path).await.unwrap()
}

// ---------------------------------------------------------------------------

/// Section 7.3 step 4 (ADR 0228, ADR 0297, ADR 0317, ADR 0322): a primary
/// runs apps, deployments, a volume and its lease, environment variables,
/// tokens, agents with keys and grants, a decided approval and the grant it
/// issued, providers, the backup repository and a backup, the off-site audit
/// export, a node with its custody seals and its tunnel, a domain, a
/// webhook, an incident and the event that triggered it, a finding of the
/// node's host kernel, a quarantined copy of a volume, a Git app's queued
/// build, a user deleted with its tokens, and an app deleted by an older
/// writer that relied on the schema's cascade and then created again under
/// the same id, so that no replicated table is empty. The database built
/// from its change log holds every replicated table row for row as the
/// primary does, a JSON text column as the same JSON value, the change log
/// entry for entry; `repo_key_enc` is an empty BLOB, the LOCAL tables are
/// empty, and `meta` holds the four keys section 7.3 names.
#[tokio::test]
async fn the_built_database_holds_every_replicated_row_of_the_primary() {
    let mock = start_mock().await;
    let server = common::start_with_cloudflare(&mock, &format!("{mock}/dns-query")).await;
    let memory = Arc::new(MemoryStore::new());
    server
        .state
        .backups
        .set_factory(Arc::new(MemoryBuckets(Arc::clone(&memory))));
    let owner = server.owner().await;
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let owner_id = me["user"]["id"].as_str().unwrap().to_string();

    // A node with the `node` role, its custody key bound and sealed to, and
    // an unused join token.
    let node = enrolled(&server, &owner, "node-alpha", 3).await;
    let custody_key = CustodyKey::generate(&mut custody::os_rng());
    let mut socket = open(&server, &node, &custody_key).await;
    heartbeat(&mut socket).await;
    join_token(&server, &owner).await;

    // An app, deployed and running, with variables and a volume, whose
    // lease the node is granted.
    let project = post(
        &owner,
        &server,
        "/api/v1/projects",
        json!({"slug": "demo", "name": "Demo"}),
        201,
    )
    .await;
    let project_id = project["id"].as_str().unwrap().to_string();
    let app = post(
        &owner,
        &server,
        "/api/v1/apps",
        json!({"project_id": project_id, "name": "web", "image": IMAGE, "port": 8080}),
        201,
    )
    .await;
    let app_id = app["id"].as_str().unwrap().to_string();
    put_env(&owner, &server, &app_id, &["TOKEN", "MODE"]).await;
    put_env(&owner, &server, &app_id, &["TOKEN"]).await;
    post(
        &owner,
        &server,
        &format!("/api/v1/apps/{app_id}/deployments"),
        json!({"image": IMAGE}),
        202,
    )
    .await;
    let (vm_id,): (String,) = sqlx::query_as("SELECT id FROM vms WHERE app_id = ?")
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    recv_type(&mut socket, "desired_state").await;
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
    let volume = post(
        &owner,
        &server,
        &format!("/api/v1/apps/{app_id}/volumes"),
        json!({"size_gb": 5, "mount_path": "/data"}),
        201,
    )
    .await;
    let volume_id = volume["id"].as_str().unwrap().to_string();
    let snapshot_frame = recv_type(&mut socket, "desired_state").await;
    let rev = snapshot_frame["body"]["rev"].as_u64().unwrap_or(1);
    send(
        &mut socket,
        status_report(rev, vec![ready(&volume_id)], Vec::new()),
    )
    .await;
    wait_for(
        &server,
        &format!("SELECT 1 FROM volumes WHERE id = '{volume_id}' AND state = 'ready'"),
        "the volume turning ready",
    )
    .await;

    // The backup repository on its provider, and a queued backup.
    let backups_provider = post(
        &owner,
        &server,
        "/api/v1/providers",
        json!({
            "kind": "object_store", "name": "backups",
            "config": {"endpoint": "https://s3.example.test", "bucket": "quoro", "access_key_id": "AKIATEST", "region": "auto"},
            "secret": "secret-access-key",
        }),
        201,
    )
    .await;
    post(
        &owner,
        &server,
        "/api/v1/backup-repositories",
        json!({"name": "primary", "provider_id": backups_provider["id"], "prefix": "backups-test"}),
        201,
    )
    .await;
    post_empty(
        &owner,
        &server,
        &format!("/api/v1/apps/{app_id}/backups"),
        202,
    )
    .await;

    // The off-site audit export on another provider.
    let audit_provider = post(
        &owner,
        &server,
        "/api/v1/providers",
        json!({
            "kind": "object_store", "name": "audit copy",
            "config": {"endpoint": "https://s3.example.test", "bucket": "audit", "access_key_id": "AKIAAUDIT", "region": "auto"},
            "secret": "audit-secret",
        }),
        201,
    )
    .await;
    audit_export::create_with(
        &server.state,
        &owner_ctx(&owner_id),
        &CreateAuditExportRequest {
            provider_id: audit_provider["id"].as_str().unwrap().to_string(),
            prefix: "quoro-audit".to_string(),
        },
        &MemoryStore::new(),
        time::OffsetDateTime::now_utc(),
    )
    .await
    .unwrap();

    // The node's tunnel, whose run token the tunnel manager stores.
    let tunnel_provider = post(
        &owner,
        &server,
        "/api/v1/providers",
        json!({
            "kind": "tunnel", "name": "tunnel",
            "config": {"account_id": "acct_example", "zone_id": "zone_example"},
            "secret": "cf-account-token",
        }),
        201,
    )
    .await;
    let res = owner
        .put(server.url(&format!("/api/v1/nodes/{}/tunnel", node.node_id)))
        .json(&json!({"provider_id": tunnel_provider["id"], "origin_port": 8081}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "tunnel: {}", res.text().await.unwrap());
    wait_for(
        &server,
        &format!(
            "SELECT 1 FROM node_tunnels WHERE node_id = '{}' AND tunnel_id IS NOT NULL",
            node.node_id
        ),
        "the tunnel's run token being stored",
    )
    .await;

    // A domain of the app, served through the tunnel.
    post(
        &owner,
        &server,
        &format!("/api/v1/apps/{app_id}/domains"),
        json!({"hostname": "web.example.com", "tls_mode": "tunnel", "provider_id": tunnel_provider["id"]}),
        201,
    )
    .await;

    // Tokens, and a user deleted with theirs, a revoked one included.
    post(
        &owner,
        &server,
        "/api/v1/tokens",
        json!({"name": "owner token"}),
        201,
    )
    .await;
    let operator = post(
        &owner,
        &server,
        "/api/v1/users",
        json!({"email": "operator@example.com", "name": "Operator", "role": "operator", "password": "operator-password"}),
        201,
    )
    .await;
    let operator_client = server.client();
    server
        .login(
            &operator_client,
            "operator@example.com",
            "operator-password",
        )
        .await;
    let kept = post(
        &operator_client,
        &server,
        "/api/v1/tokens",
        json!({"name": "kept"}),
        201,
    )
    .await;
    let revoked = post(
        &operator_client,
        &server,
        "/api/v1/tokens",
        json!({"name": "revoked"}),
        201,
    )
    .await;
    let res = operator_client
        .delete(server.url(&format!(
            "/api/v1/tokens/{}",
            revoked["id"].as_str().unwrap()
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let res = owner
        .delete(server.url(&format!(
            "/api/v1/users/{}",
            operator["id"].as_str().unwrap()
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        204,
        "delete user: {}",
        res.text().await.unwrap()
    );
    assert!(kept["id"].is_string());

    // An agent with a key it minted a token with, and a grant.
    let agent = post(
        &owner,
        &server,
        "/api/v1/agents",
        json!({"name": "ops-agent", "model": "test-model", "owner_user_id": owner_id, "description": "tests"}),
        201,
    )
    .await;
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let key = post_empty(
        &owner,
        &server,
        &format!("/api/v1/agents/{agent_id}/keys"),
        201,
    )
    .await;
    post(
        &owner,
        &server,
        "/api/v1/grants",
        json!({"agent_id": agent_id, "scopes": ["app:read", "approval:create"], "selector": {"projects": ["demo"]}, "ttl_s": 3600, "reason": "tests"}),
        201,
    )
    .await;
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "the build test needs a token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "mint: {}", res.text().await.unwrap());
    let minted: Value = res.json().await.unwrap();
    let agent_token = minted["access_token"].as_str().unwrap().to_string();

    // A decided approval and the one-use grant it issued, which point at
    // each other.
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {agent_token}"))
        .json(&json!({
            "scope": "app:delete",
            "target": {"type": "app", "id": app_id},
            "operation": "DELETE /api/v1/apps/{app_id}",
            "params": {"app_id": app_id},
            "justification": "the build test needs a decided approval",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "approval: {}", res.text().await.unwrap());
    let approval: Value = res.json().await.unwrap();
    let approved = post_empty(
        &owner,
        &server,
        &format!(
            "/api/v1/approvals/{}/approve",
            approval["id"].as_str().unwrap()
        ),
        200,
    )
    .await;
    assert!(approved["grant_id"].is_string(), "{approved}");
    post(
        &owner,
        &server,
        "/api/v1/grants",
        json!({"agent_id": agent_id, "scopes": ["app:read"], "selector": {"apps": [app_id]}, "ttl_s": 3600, "reason": "tests"}),
        201,
    )
    .await;

    // A webhook, whose signing secret travels as ciphertext.
    post(
        &owner,
        &server,
        "/api/v1/webhooks",
        json!({"name": "hook", "url": "https://hooks.example.com/quoro", "event_types": ["app.deleted"]}),
        201,
    )
    .await;

    // The node reports the app down, which opens an incident that names the
    // event as its trigger.
    let down_id = new_id(Prefix::Event);
    send(
        &mut socket,
        NodeMessage::Event(EventBody {
            schema_version: 1,
            id: down_id.clone(),
            kind: "app.down".to_string(),
            time: "2026-11-13T10:15:32Z".to_string(),
            source: json!({"kind": "node", "node_id": node.node_id}),
            subject: json!({"type": "app", "id": app_id}),
            project_id: Some(project_id.clone()),
            incident_id: None,
            request_id: None,
            actor: Some(json!({"type": "node", "id": node.node_id})),
            data: json!({
                "app_id": app_id,
                "node_id": node.node_id,
                "check": "health_check",
                "failures": 3,
                "last_error": "connect: connection refused",
            }),
        }),
    )
    .await;
    wait_for(
        &server,
        &format!("SELECT 1 FROM incidents WHERE trigger_event_id = '{down_id}'"),
        "the incident the app.down opens",
    )
    .await;

    // A finding of the node's host kernel, written by the reconcile of the
    // host kernels against the cached Linux data.
    server
        .state
        .vulns
        .cache()
        .replace_bucket("Linux", vec![kernel_record()])
        .await
        .unwrap();
    let kernels = subjects::node_kernels(&server.state).await.unwrap();
    subjects::reconcile_host_kernels(&server.state, &kernels)
        .await
        .unwrap();
    wait_for(
        &server,
        &format!(
            "SELECT 1 FROM vulnerability_findings WHERE node_id = '{}'",
            node.node_id
        ),
        "the host kernel's finding",
    )
    .await;

    // A quarantined copy of the volume, reported with its hash, which the
    // primary records.
    let quarantine_id = "qrn_01JC9Z6Q5W8N3K4T7R2M000001";
    send(
        &mut socket,
        status_report(
            rev,
            vec![ready(&volume_id)],
            vec![QuarantineStatus {
                quarantine_id: quarantine_id.to_string(),
                volume_id: volume_id.clone(),
                app_id: app_id.clone(),
                created_at: "2026-11-09T10:00:00Z".to_string(),
                size_bytes: 1 << 20,
                sha256: Some("a".repeat(64)),
            }],
        ),
    )
    .await;
    wait_for(
        &server,
        &format!("SELECT 1 FROM volume_quarantines WHERE id = '{quarantine_id}'"),
        "the quarantined copy being recorded",
    )
    .await;

    // A Git app with a build queued on the node.
    let site = post(
        &owner,
        &server,
        "/api/v1/apps",
        json!({
            "project_id": project_id, "name": "site", "port": 8080,
            "source": {"kind": "git", "repo_url": "https://github.com/example/blog.git", "ref": "main"},
        }),
        201,
    )
    .await;
    let res = owner
        .post(server.url(&format!(
            "/api/v1/apps/{}/builds",
            site["id"].as_str().unwrap()
        )))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "build: {}", res.text().await.unwrap());

    // An app deleted the way an older writer did it: only the app's own
    // delete is logged, and the schema's cascade removes its variable with
    // no entry. The app is then created again under the same id, with a
    // variable of its own.
    let old = post(
        &owner,
        &server,
        "/api/v1/apps",
        json!({"project_id": project_id, "name": "old", "image": IMAGE, "port": 8080}),
        201,
    )
    .await;
    let old_id = old["id"].as_str().unwrap().to_string();
    put_env(&owner, &server, &old_id, &["BEFORE"]).await;
    let mut w = server.state.store.begin_write().await.unwrap();
    let recreated = whole_row(&mut w.tx, "apps", &old_id).await;
    sqlx::query("CREATE TEMP TABLE old_app AS SELECT * FROM apps WHERE id = ?")
        .bind(&old_id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM apps WHERE id = ?")
        .bind(&old_id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    let older = Actor::system("older-writer");
    changelog::append(&mut w.tx, &older, "app", &old_id, Op::Delete, json!({}))
        .await
        .unwrap();
    sqlx::query("INSERT INTO apps SELECT * FROM temp.old_app")
        .execute(&mut *w.tx)
        .await
        .unwrap();
    sqlx::query("DROP TABLE temp.old_app")
        .execute(&mut *w.tx)
        .await
        .unwrap();
    changelog::append(&mut w.tx, &older, "app", &old_id, Op::Create, recreated)
        .await
        .unwrap();
    w.commit().await.unwrap();
    put_env(&owner, &server, &old_id, &["AFTER"]).await;
    let env_names: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM app_env WHERE app_id = ? ORDER BY name")
            .bind(&old_id)
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(
        env_names,
        [("AFTER".to_string(),)],
        "the cascade ran on the primary"
    );

    // The primary as it stands, and the database built from its log.
    let dir = TestDir::new("quoro-test-build");
    let primary = snapshot(&server, &dir).await;
    let entries = replay::followed(&primary).await;
    let term: String =
        sqlx::query_scalar("SELECT IFNULL((SELECT value FROM meta WHERE key = 'term'), '1')")
            .fetch_one(primary.pool())
            .await
            .unwrap();
    let promoted_term = term.parse::<u64>().unwrap() + 1;
    let target = dir.join("rebuilt.db");
    let built: Built = database::build(&entries, &server.state.cluster_id, promoted_term, &target)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(built.dropped.is_empty(), "{:?}", built.dropped);
    assert!(built.mismatches.is_empty(), "{:?}", built.mismatches);
    assert_eq!(built.head_seq, entries.last().unwrap().seq);
    assert_eq!(
        built
            .cascaded
            .iter()
            .map(|c| (c.table, c.entity_id.clone()))
            .collect::<Vec<_>>(),
        [("app_env", format!("{old_id}/BEFORE"))]
    );
    let rebuilt = Store::open(&target).await.unwrap();

    // Every replicated table, row for row; the repository key aside.
    for (table, rows) in &built.rows {
        let columns = replay::column_names(&primary, table).await;
        let compared: Vec<&str> = columns
            .iter()
            .map(String::as_str)
            .filter(|column| (*table, *column) != ("backup_repositories", "repo_key_enc"))
            .collect();
        let original = replay::rows(&primary, table, &compared).await;
        assert_eq!(
            replay::rows(&rebuilt, table, &compared).await,
            original,
            "{table} rebuilt from the log"
        );
        assert_eq!(*rows, u64::try_from(original.len()).unwrap(), "{table}");
    }
    // No replicated table is empty on either side.
    let empty: Vec<&str> = built
        .rows
        .iter()
        .filter(|(_, rows)| **rows == 0)
        .map(|(table, _)| *table)
        .collect();
    assert!(empty.is_empty(), "the scenario leaves {empty:?} empty");
    assert_eq!(
        built.rows.values().filter(|rows| **rows > 0).count(),
        31,
        "{:?}",
        built.rows
    );
    assert_eq!(
        built.rows["api_tokens"], 1,
        "the operator's tokens left with them"
    );
    assert_eq!(built.rows["users"], 1);

    // The repository key is an empty BLOB.
    let keys = replay::rows(&rebuilt, "backup_repositories", &["repo_key_enc"]).await;
    assert_eq!(keys, [vec![json!("x''")]]);
    let original_keys = replay::rows(&primary, "backup_repositories", &["repo_key_enc"]).await;
    assert_ne!(original_keys, keys, "the primary holds its key");

    // The change log, entry for entry.
    let log_columns = [
        "seq",
        "term",
        "time",
        "actor_type",
        "actor_id",
        "entity_type",
        "entity_id",
        "op",
        "payload",
        "prev_hash",
        "hash",
    ];
    assert_eq!(
        replay::rows(&rebuilt, "change_log", &log_columns).await,
        replay::rows(&primary, "change_log", &log_columns).await
    );

    // The LOCAL tables start empty; the primary's are not.
    let replicated: Vec<&str> = built.rows.keys().copied().collect();
    let mut local = Vec::new();
    for table in tables(&rebuilt).await {
        if replicated.contains(&table.as_str()) {
            continue;
        }
        assert_eq!(count(&rebuilt, &table).await, 0, "{table}");
        local.push(table);
    }
    assert_eq!(
        local,
        [
            "access_tokens",
            "interactive_sessions",
            "osv_mirror",
            "sessions",
            "webhook_deliveries",
        ]
    );
    assert!(count(&primary, "sessions").await > 0);
    assert!(count(&primary, "access_tokens").await > 0);

    // `meta`: the agent's cluster id, the promotion's term, this binary's
    // schema version and the newest event's seq, and nothing else.
    let meta: Vec<(String, String)> = sqlx::query_as("SELECT key, value FROM meta ORDER BY key")
        .fetch_all(rebuilt.pool())
        .await
        .unwrap();
    let newest_event: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM events")
        .fetch_one(primary.pool())
        .await
        .unwrap();
    assert!(newest_event > 0);
    assert_eq!(
        meta,
        [
            ("cluster_id".to_string(), server.state.cluster_id.clone()),
            (
                "schema_version".to_string(),
                Store::shipped_schema_version().to_string()
            ),
            ("term".to_string(), promoted_term.to_string()),
            ("webhook_cursor".to_string(), newest_event.to_string()),
        ]
    );
    assert_eq!(built.webhook_cursor, newest_event.unsigned_abs());

    rebuilt.pool().close().await;
    primary.pool().close().await;
    drop(socket);
}
