//! Incidents: the detection hook on node events, the read endpoints and
//! resolution with grant revocation (D-0032, permission-model.md sections 8.2
//! and 8.3), over real HTTP and a real node session.

mod common;

use common::{Server, problem, start};
use futures_util::{SinkExt, StreamExt};
use quoro_proto::sign;
use quoro_proto::wire::{self, Capacity as WireCapacity, EventBody, Kvm as WireKvm, NodeMessage};
use quoro_types::{Prefix, new_id};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

type Socket = WebSocketStream<TcpStream>;

fn signing_key() -> sign::SigningKey {
    sign::signing_key_from_seed(&[9u8; 32])
}

fn public_key() -> String {
    sign::public_key_to_base64(&signing_key().verifying_key())
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
    assert_eq!(
        res.status(),
        201,
        "join token: {}",
        res.text().await.unwrap()
    );
    let token: Value = res.json().await.unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"],
            "name": "node-alpha",
            "public_key": public_key(),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
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

async fn open(server: &Server, node: &Enrolled) -> Socket {
    let url = format!("ws://{}/api/v1/nodes/session", server.address());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", node.credential).parse().unwrap(),
    );
    let stream = TcpStream::connect(server.address()).await.unwrap();
    let (mut socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status(), 101);
    send(&mut socket, hello(&node.node_id)).await;
    // `welcome` then the first `desired_state`.
    for kind in ["welcome", "desired_state"] {
        loop {
            let frame = recv_frame(&mut socket).await;
            if frame["type"] == kind {
                break;
            }
        }
    }
    socket
}

async fn send(socket: &mut Socket, message: NodeMessage) {
    let id = new_id(Prefix::Request);
    let text = message.to_frame(&id, None).to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
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

/// Sends one node event (`source` and `actor` name this node).
async fn send_event(
    socket: &mut Socket,
    node_id: &str,
    kind: &str,
    app_id: &str,
    project_id: &str,
) -> String {
    let id = new_id(Prefix::Event);
    let mut data = json!({ "app_id": app_id });
    if kind == "app.down" {
        data = json!({
            "app_id": app_id,
            "node_id": node_id,
            "check": "health_check",
            "failures": 3,
            "last_error": "connect: connection refused",
        });
    }
    let event = EventBody {
        schema_version: 1,
        id: id.clone(),
        kind: kind.to_string(),
        time: "2026-11-02T09:15:00Z".to_string(),
        source: json!({ "kind": "node", "node_id": node_id }),
        subject: json!({ "type": "app", "id": app_id }),
        project_id: Some(project_id.to_string()),
        incident_id: None,
        request_id: None,
        actor: Some(json!({ "type": "node", "id": node_id })),
        data,
    };
    send(socket, NodeMessage::Event(event)).await;
    id
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
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
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

async fn owner_id(owner: &reqwest::Client, server: &Server) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    me["user"]["id"].as_str().unwrap().to_string()
}

/// Waits until an open incident exists for `app_id` and returns it.
async fn wait_open_incident(server: &Server, app_id: &str) -> String {
    for _ in 0..100 {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM incidents WHERE status = 'open' \
             AND json_extract(subject, '$.id') = ? LIMIT 1",
        )
        .bind(app_id)
        .fetch_optional(server.state.store.pool())
        .await
        .unwrap();
        if let Some((id,)) = row {
            return id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no open incident for {app_id}");
}

#[tokio::test]
async fn app_down_opens_one_incident_and_app_up_resolves_it() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let node = enrolled(&server, &owner).await;
    let mut socket = open(&server, &node).await;

    let down_id = send_event(&mut socket, &node.node_id, "app.down", &app_id, &project_id).await;
    let incident_id = wait_open_incident(&server, &app_id).await;

    let incident: Value = owner
        .get(server.url(&format!("/api/v1/incidents/{incident_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(incident["status"], "open");
    assert_eq!(incident["subject"]["type"], "app");
    assert_eq!(incident["subject"]["id"], app_id);
    assert_eq!(incident["trigger_event_id"], down_id);
    assert_eq!(incident["project_id"], project_id);
    assert_eq!(incident["title"], "app web is down");
    assert_eq!(incident["trigger_event"]["id"], down_id);
    assert_eq!(incident["grants"].as_array().unwrap().len(), 0);

    // The incident.opened event names the incident and carries its id.
    let (opened,): (String,) = sqlx::query_as(
        "SELECT data FROM events WHERE type = 'incident.opened' AND incident_id = ?",
    )
    .bind(&incident_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let opened: Value = serde_json::from_str(&opened).unwrap();
    assert_eq!(opened["incident_id"], incident_id);
    assert_eq!(opened["trigger_event_id"], down_id);

    // A second app.down for the same app opens no second incident.
    send_event(&mut socket, &node.node_id, "app.down", &app_id, &project_id).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM incidents WHERE json_extract(subject, '$.id') = ?")
            .bind(&app_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);

    // app.up resolves it as the system.
    send_event(&mut socket, &node.node_id, "app.up", &app_id, &project_id).await;
    let mut status = String::new();
    for _ in 0..100 {
        let (value,): (String,) = sqlx::query_as("SELECT status FROM incidents WHERE id = ?")
            .bind(&incident_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        status = value;
        if status == "resolved" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(status, "resolved");
    let (resolved_by_type, resolved_by_id): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT resolved_by_type, resolved_by_id FROM incidents WHERE id = ?")
            .bind(&incident_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(resolved_by_type.as_deref(), Some("system"));
    assert_eq!(resolved_by_id.as_deref(), Some("incidents"));
}

#[tokio::test]
async fn list_and_get_incidents_filter_by_status() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let node = enrolled(&server, &owner).await;
    let mut socket = open(&server, &node).await;
    send_event(&mut socket, &node.node_id, "app.down", &app_id, &project_id).await;
    let incident_id = wait_open_incident(&server, &app_id).await;

    let open: Value = owner
        .get(server.url("/api/v1/incidents?status=open"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(open["items"].as_array().unwrap().len(), 1);
    assert_eq!(open["items"][0]["id"], incident_id);

    let resolved: Value = owner
        .get(server.url("/api/v1/incidents?status=resolved"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(resolved["items"].as_array().unwrap().is_empty());

    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/incidents/inc_unknown"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

#[tokio::test]
async fn resolving_revokes_the_incident_grant_and_a_second_resolve_conflicts() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;
    let node = enrolled(&server, &owner).await;
    let mut socket = open(&server, &node).await;
    send_event(&mut socket, &node.node_id, "app.down", &app_id, &project_id).await;
    let incident_id = wait_open_incident(&server, &app_id).await;

    // A real agent, then an incident grant inserted directly (the incident
    // grant issuer is a later, webhook-side task).
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "woken",
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "a woken agent",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "agent: {}", res.text().await.unwrap());
    let agent: Value = res.json().await.unwrap();
    let agent_id = agent["id"].as_str().unwrap();
    let grant_id = new_id(Prefix::Grant);
    sqlx::query(
        "INSERT INTO grants (id, agent_id, kind, scopes, selector, expires_at, max_uses, uses, \
         status, incident_id, approval_id, on_behalf_of, created_by_type, created_by_id, reason, \
         created_at, revoked_at, revoked_by) \
         VALUES (?, ?, 'incident', ?, ?, ?, NULL, 0, 'active', ?, NULL, ?, 'system', 'incidents', \
         'the incident woke it', ?, NULL, NULL)",
    )
    .bind(&grant_id)
    .bind(agent_id)
    .bind(json!(["app:read", "incident:resolve"]).to_string())
    .bind(json!({ "apps": [&app_id] }).to_string())
    .bind("2999-01-01T00:00:00.000Z")
    .bind(&incident_id)
    .bind(&owner_user_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();

    let detail: Value = owner
        .get(server.url(&format!("/api/v1/incidents/{incident_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["grants"].as_array().unwrap().len(), 1);
    assert_eq!(detail["grants"][0]["id"], grant_id);

    let res = owner
        .post(server.url(&format!("/api/v1/incidents/{incident_id}/resolve")))
        .json(&json!({ "resolution_note": "restarted the app" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "resolve: {}", res.text().await.unwrap());
    let resolved: Value = res.json().await.unwrap();
    assert_eq!(resolved["status"], "resolved");
    assert_eq!(resolved["resolved_by_type"], "user");
    assert_eq!(resolved["resolved_by_id"], owner_user_id);
    assert_eq!(resolved["resolution_note"], "restarted the app");
    assert!(resolved["resolved_at"].is_string());

    let (status, revoked_at): (String, Option<String>) =
        sqlx::query_as("SELECT status, revoked_at FROM grants WHERE id = ?")
            .bind(&grant_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(status, "revoked");
    assert!(revoked_at.is_some());
    let (revoked,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events WHERE type = 'grant.revoked' \
         AND json_extract(subject, '$.id') = ?",
    )
    .bind(&grant_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(revoked, 1);

    // Resolving again is a conflict.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/incidents/{incident_id}/resolve")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}
