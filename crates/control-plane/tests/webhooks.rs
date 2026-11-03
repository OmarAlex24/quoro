//! `/api/v1/webhooks*` and the dispatcher (D-0032, permission-model.md
//! section 8), over real HTTP and a local receiver.

mod common;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::any;
use common::{Server, problem, start};
use hmac::{Hmac, Mac};
use quoro_control_plane::changelog::Actor;
use quoro_control_plane::events::{self, NewEvent, Source, StoredEvent};
use quoro_control_plane::store::now_rfc3339;
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::Row;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

fn image() -> &'static str {
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000"
}

#[derive(Debug)]
struct Captured {
    headers: HeaderMap,
    body: Vec<u8>,
}

struct Recorder {
    requests: Mutex<Vec<Captured>>,
    status: AtomicU16,
}

impl Recorder {
    async fn requests(&self) -> Vec<Captured> {
        self.requests
            .lock()
            .await
            .iter()
            .map(|r| Captured {
                headers: r.headers.clone(),
                body: r.body.clone(),
            })
            .collect()
    }
}

async fn receiver(
    State(recorder): State<Arc<Recorder>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    recorder.requests.lock().await.push(Captured {
        headers,
        body: body.to_vec(),
    });
    let status = recorder.status.load(Ordering::SeqCst);
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        "ok",
    )
}

/// Starts a local receiver and returns it with `http://localhost:<port>`.
async fn start_receiver(status: u16) -> (Arc<Recorder>, String) {
    let recorder = Arc::new(Recorder {
        requests: Mutex::new(Vec::new()),
        status: AtomicU16::new(status),
    });
    let app = Router::new()
        .fallback(any(receiver))
        .with_state(Arc::clone(&recorder));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (recorder, format!("http://localhost:{port}"))
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

async fn create_app(owner: &reqwest::Client, server: &Server, project_id: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "image": image(),
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

async fn create_agent(owner: &reqwest::Client, server: &Server, owner_user_id: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "woken",
            "model": "test-model",
            "owner_user_id": owner_user_id,
            "description": "a test agent",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create agent: {}",
        res.text().await.unwrap()
    );
    let agent: Value = res.json().await.unwrap();
    agent["id"].as_str().unwrap().to_string()
}

async fn token_for(owner: &reqwest::Client, server: &Server, agent_id: &str) -> String {
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create key: {}",
        res.text().await.unwrap()
    );
    let key: Value = res.json().await.unwrap();
    let key = key["key"].as_str().unwrap().to_string();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"))
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "token: {}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

async fn create_webhook(owner: &reqwest::Client, server: &Server, body: Value) -> (Value, String) {
    let res = owner
        .post(server.url("/api/v1/webhooks"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create webhook: {}",
        res.text().await.unwrap()
    );
    let created: Value = res.json().await.unwrap();
    let secret = created["secret"].as_str().unwrap().to_string();
    (created, secret)
}

async fn emit_app_event(server: &Server, app_id: &str, kind: &'static str) -> String {
    let mut w = server.state.store.begin_write().await.unwrap();
    let id = events::emit(
        &mut w.tx,
        NewEvent {
            kind,
            source: Source::ControlPlane,
            subject_type: "app",
            subject_id: app_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({ "app_id": app_id }),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
    id
}

/// Waits until `check` returns `Some`, or panics after five seconds.
async fn wait_for<T, F, Fut>(mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    for _ in 0..100 {
        if let Some(value) = check().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("condition not met within five seconds");
}

fn signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.").as_bytes());
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let mut out = String::from("v1=");
    for byte in digest {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

async fn delivery_count(server: &Server, webhook_id: &str) -> i64 {
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ?")
            .bind(webhook_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    count
}

#[tokio::test]
async fn delivers_signed_event_and_records_the_attempt() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (recorder, url) = start_receiver(200).await;
    let (created, secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "receiver",
            "url": url,
            "event_types": ["app.up"],
            "selector": { "apps": [app_id] },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    // The secret is shown once and never again.
    assert_eq!(
        owner
            .get(server.url(&format!("/api/v1/webhooks/{webhook_id}")))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
            .get("secret"),
        None
    );

    let event_id = emit_app_event(&server, &app_id, "app.up").await;
    let captured = wait_for(|| async {
        let captured = recorder.requests().await;
        (!captured.is_empty()).then_some(captured)
    })
    .await;
    let first = &captured[0];
    assert_eq!(
        first
            .headers
            .get("quoro-event-id")
            .unwrap()
            .to_str()
            .unwrap(),
        event_id
    );
    assert_eq!(
        first
            .headers
            .get("quoro-event-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "app.up"
    );
    let delivery_id = first
        .headers
        .get("quoro-delivery-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let signed = first
        .headers
        .get("quoro-signature")
        .unwrap()
        .to_str()
        .unwrap();
    let (timestamp, value) = signed.split_once(',').unwrap();
    let timestamp = timestamp.strip_prefix("t=").unwrap();
    assert_eq!(signature(&secret, timestamp, &first.body), value);
    let body: Value = serde_json::from_slice(&first.body).unwrap();
    assert_eq!(body["type"], "app.up");
    assert_eq!(body["id"], event_id);
    assert_eq!(body["subject"]["id"], app_id);

    let delivery_id = delivery_id.clone();
    let row = wait_for(|| async {
        let row =
            sqlx::query("SELECT status, response_code, error FROM webhook_deliveries WHERE id = ?")
                .bind(&delivery_id)
                .fetch_optional(server.state.store.pool())
                .await
                .unwrap()?;
        Some((
            row.get::<String, _>("status"),
            row.get::<Option<i64>, _>("response_code"),
            row.get::<Option<String>, _>("error"),
        ))
    })
    .await;
    assert_eq!(row, ("delivered".to_string(), Some(200), None));
    assert_eq!(delivery_count(&server, &webhook_id).await, 1);
}

#[tokio::test]
async fn failed_attempt_schedules_a_retry() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (_recorder, url) = start_receiver(500).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "broken",
            "url": url,
            "event_types": ["app.up"],
            "selector": { "all": true },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    emit_app_event(&server, &app_id, "app.up").await;

    let rows = wait_for(|| async {
        let rows = sqlx::query(
            "SELECT attempt, status, response_code, next_attempt_at FROM webhook_deliveries \
             WHERE webhook_id = ? ORDER BY attempt",
        )
        .bind(&webhook_id)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
        (rows.len() >= 2).then_some(rows)
    })
    .await;
    assert_eq!(rows[0].get::<i64, _>("attempt"), 1);
    assert_eq!(rows[0].get::<String, _>("status"), "failed");
    assert_eq!(rows[0].get::<Option<i64>, _>("response_code"), Some(500));
    assert_eq!(rows[1].get::<i64, _>("attempt"), 2);
    assert_eq!(rows[1].get::<String, _>("status"), "pending");
    assert!(
        rows[1]
            .get::<Option<String>, _>("next_attempt_at")
            .is_some()
    );
}

#[tokio::test]
async fn incident_opened_wakes_the_agent_once() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let (recorder, url) = start_receiver(500).await;
    let (created, secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "waker",
            "url": url,
            "event_types": ["incident.opened"],
            "selector": { "apps": [app_id] },
            "wake": {
                "agent_id": agent_id,
                "grant_scopes": ["app:read", "app:restart"],
                "grant_ttl_s": 600,
                "grant_max_uses": 3,
            },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();

    // A detection event, the incident it opened, then `incident.opened`.
    let trigger = emit_app_event(&server, &app_id, "app.down").await;
    let incident_id = quoro_types::new_id(quoro_types::Prefix::Incident);
    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query(
        "INSERT INTO incidents (id, status, title, trigger_event_id, subject, project_id, opened_at) \
         VALUES (?, 'open', ?, ?, ?, ?, ?)",
    )
    .bind(&incident_id)
    .bind("web is down")
    .bind(&trigger)
    .bind(json!({ "type": "app", "id": app_id }).to_string())
    .bind(&project_id)
    .bind(now_rfc3339())
    .execute(&mut *w.tx)
    .await
    .unwrap();
    let event_id = quoro_types::new_id(quoro_types::Prefix::Event);
    events::insert(
        &mut w.tx,
        StoredEvent {
            id: &event_id,
            time: &now_rfc3339(),
            kind: "incident.opened",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "incident", "id": incident_id }),
            project_id: Some(&project_id),
            incident_id: Some(&incident_id),
            request_id: None,
            actor: None,
            data: json!({
                "incident_id": incident_id,
                "title": "web is down",
                "trigger_event_id": trigger,
                "subject": { "type": "app", "id": app_id },
            }),
            change_actor: Actor::system("tests"),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();

    let grants = wait_for(|| async {
        let rows = sqlx::query(
            "SELECT id, agent_id, kind, scopes, selector, max_uses, incident_id, on_behalf_of, \
             created_by_type, created_by_id, reason FROM grants WHERE incident_id = ?",
        )
        .bind(&incident_id)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
        (!rows.is_empty()).then_some(rows)
    })
    .await;
    assert_eq!(grants.len(), 1);
    let grant = &grants[0];
    assert_eq!(grant.get::<String, _>("kind"), "incident");
    assert_eq!(grant.get::<String, _>("agent_id"), agent_id);
    assert_eq!(
        serde_json::from_str::<Value>(&grant.get::<String, _>("selector")).unwrap(),
        json!({ "apps": [app_id] })
    );
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&grant.get::<String, _>("scopes")).unwrap(),
        vec!["app:read", "app:restart"]
    );
    assert_eq!(grant.get::<Option<i64>, _>("max_uses"), Some(3));
    assert_eq!(grant.get::<String, _>("on_behalf_of"), owner_user_id);
    assert_eq!(grant.get::<String, _>("created_by_type"), "system");
    assert_eq!(grant.get::<String, _>("created_by_id"), "webhooks");
    assert!(grant.get::<String, _>("reason").contains(&webhook_id));
    assert!(grant.get::<String, _>("reason").contains(&incident_id));

    let evidence = sqlx::query(
        "SELECT actor_type, actor_id, operation, on_behalf_of, incident_id, trigger_event_id, \
         result, change_seq FROM audit_log WHERE operation = 'grant.issue'",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(evidence.len(), 1);
    let record = &evidence[0];
    assert_eq!(record.get::<String, _>("actor_type"), "system");
    assert_eq!(record.get::<String, _>("actor_id"), "webhooks");
    assert_eq!(record.get::<String, _>("on_behalf_of"), owner_user_id);
    assert_eq!(record.get::<String, _>("incident_id"), incident_id);
    assert_eq!(record.get::<String, _>("trigger_event_id"), trigger);
    assert_eq!(record.get::<String, _>("result"), "ok");
    assert!(record.get::<Option<i64>, _>("change_seq").is_some());

    // A retry must not issue a second grant.
    let pending = wait_for(|| async {
        let row =
            sqlx::query("SELECT id FROM webhook_deliveries WHERE webhook_id = ? AND attempt = 2")
                .bind(&webhook_id)
                .fetch_optional(server.state.store.pool())
                .await
                .unwrap()?;
        Some(row.get::<String, _>("id"))
    })
    .await;
    sqlx::query(
        "UPDATE webhook_deliveries SET next_attempt_at = '2000-01-01T00:00:00.000Z' WHERE id = ?",
    )
    .bind(&pending)
    .execute(server.state.store.pool())
    .await
    .unwrap();
    wait_for(|| async {
        let row = sqlx::query("SELECT status FROM webhook_deliveries WHERE id = ?")
            .bind(&pending)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        (row.get::<String, _>("status") != "pending").then_some(())
    })
    .await;
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM grants WHERE incident_id = ?")
        .bind(&incident_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Every attempt carries the same `data.wake`, inside the signed bytes.
    let captured = wait_for(|| async {
        let captured = recorder.requests().await;
        (captured.len() >= 2).then_some(captured)
    })
    .await;
    let grant_id = grant.get::<String, _>("id");
    for request in &captured {
        let signed = request
            .headers
            .get("quoro-signature")
            .unwrap()
            .to_str()
            .unwrap();
        let (timestamp, value) = signed.split_once(',').unwrap();
        let timestamp = timestamp.strip_prefix("t=").unwrap();
        assert_eq!(
            signature(&secret, timestamp, &request.body),
            value,
            "the signature covers data.wake"
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["data"]["wake"]["agent_id"], agent_id);
        assert_eq!(body["data"]["wake"]["grant_id"], grant_id);
    }

    // The event log never carries `wake`.
    let events: Value = owner
        .get(server.url("/api/v1/events"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for event in events["items"].as_array().unwrap() {
        assert!(
            event["data"].get("wake").is_none(),
            "the event log carries wake: {event}"
        );
    }
}

#[tokio::test]
async fn two_wake_webhooks_get_their_own_grant() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let (recorder, url) = start_receiver(200).await;
    let webhook_ids = {
        let mut ids = Vec::new();
        for name in ["first", "second"] {
            let (created, _secret) = create_webhook(
                &owner,
                &server,
                json!({
                    "name": name,
                    "url": url,
                    "event_types": ["incident.opened"],
                    "selector": { "apps": [app_id] },
                    "wake": {
                        "agent_id": agent_id,
                        "grant_scopes": ["app:read"],
                        "grant_ttl_s": 600,
                    },
                }),
            )
            .await;
            ids.push(created["id"].as_str().unwrap().to_string());
        }
        ids
    };

    let trigger = emit_app_event(&server, &app_id, "app.down").await;
    let incident_id = quoro_types::new_id(quoro_types::Prefix::Incident);
    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query(
        "INSERT INTO incidents (id, status, title, trigger_event_id, subject, project_id, opened_at) \
         VALUES (?, 'open', ?, ?, ?, ?, ?)",
    )
    .bind(&incident_id)
    .bind("web is down")
    .bind(&trigger)
    .bind(json!({ "type": "app", "id": app_id }).to_string())
    .bind(&project_id)
    .bind(now_rfc3339())
    .execute(&mut *w.tx)
    .await
    .unwrap();
    let event_id = quoro_types::new_id(quoro_types::Prefix::Event);
    events::insert(
        &mut w.tx,
        StoredEvent {
            id: &event_id,
            time: &now_rfc3339(),
            kind: "incident.opened",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "incident", "id": incident_id }),
            project_id: Some(&project_id),
            incident_id: Some(&incident_id),
            request_id: None,
            actor: None,
            data: json!({ "incident_id": incident_id, "app_id": app_id }),
            change_actor: Actor::system("tests"),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();

    let captured = wait_for(|| async {
        let captured = recorder.requests().await;
        (captured.len() >= 2).then_some(captured)
    })
    .await;
    let mut wake_grants = Vec::new();
    for request in &captured {
        assert_eq!(
            request
                .headers
                .get("quoro-event-id")
                .unwrap()
                .to_str()
                .unwrap(),
            event_id
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["data"]["wake"]["agent_id"], agent_id);
        wake_grants.push(
            body["data"]["wake"]["grant_id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    assert_ne!(wake_grants[0], wake_grants[1], "one grant per webhook");

    let mut grants: Vec<String> =
        sqlx::query_scalar("SELECT id FROM grants WHERE incident_id = ? ORDER BY id")
            .bind(&incident_id)
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    grants.sort();
    wake_grants.sort();
    assert_eq!(grants, wake_grants);
    assert_eq!(grants.len(), 2);
    for webhook_id in &webhook_ids {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ?")
                .bind(webhook_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }
}

#[tokio::test]
async fn test_delivery_never_wakes_the_agent() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let (recorder, url) = start_receiver(200).await;
    // The type filter keeps the real dispatcher out of this incident; the test
    // delivery replays the newest event, which is the `incident.opened`.
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "tester",
            "url": url,
            "event_types": ["incident.resolved"],
            "selector": { "all": true },
            "wake": {
                "agent_id": agent_id,
                "grant_scopes": ["app:read"],
                "grant_ttl_s": 600,
            },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();

    let trigger = emit_app_event(&server, &app_id, "app.down").await;
    let incident_id = quoro_types::new_id(quoro_types::Prefix::Incident);
    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query(
        "INSERT INTO incidents (id, status, title, trigger_event_id, subject, project_id, opened_at) \
         VALUES (?, 'open', ?, ?, ?, ?, ?)",
    )
    .bind(&incident_id)
    .bind("web is down")
    .bind(&trigger)
    .bind(json!({ "type": "app", "id": app_id }).to_string())
    .bind(&project_id)
    .bind(now_rfc3339())
    .execute(&mut *w.tx)
    .await
    .unwrap();
    events::insert(
        &mut w.tx,
        StoredEvent {
            id: &quoro_types::new_id(quoro_types::Prefix::Event),
            time: &now_rfc3339(),
            kind: "incident.opened",
            source: json!({ "kind": "control_plane" }),
            subject: json!({ "type": "incident", "id": incident_id }),
            project_id: Some(&project_id),
            incident_id: Some(&incident_id),
            request_id: None,
            actor: None,
            data: json!({ "incident_id": incident_id, "app_id": app_id }),
            change_actor: Actor::system("tests"),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();

    let res = owner
        .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
    let delivery: Value = res.json().await.unwrap();
    assert_eq!(delivery["status"], "delivered");
    let delivery_id = delivery["id"].as_str().unwrap().to_string();

    let captured = wait_for(|| async {
        let captured = recorder.requests().await;
        (!captured.is_empty()).then_some(captured)
    })
    .await;
    let request = captured
        .iter()
        .find(|request| {
            request
                .headers
                .get("quoro-delivery-id")
                .is_some_and(|value| value.to_str().unwrap() == delivery_id)
        })
        .expect("the test delivery reached the receiver");
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert!(body["data"].get("wake").is_none(), "{body}");
    let (grants,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM grants WHERE incident_id = ?")
        .bind(&incident_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(grants, 0);
}

#[tokio::test]
async fn validation_refuses_unsafe_wake_and_plain_http() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;

    for body in [
        json!({
            "name": "bad", "url": "https://example.com/hook",
            "wake": { "agent_id": agent_id, "grant_scopes": ["app:delete"] },
        }),
        json!({
            "name": "bad", "url": "https://example.com/hook",
            "wake": { "agent_id": agent_id, "grant_scopes": ["webhook:write"] },
        }),
        json!({
            "name": "bad", "url": "https://example.com/hook",
            "wake": { "agent_id": agent_id, "grant_scopes": ["app:*"] },
        }),
        json!({
            "name": "bad", "url": "https://example.com/hook",
            "wake": { "agent_id": agent_id, "grant_ttl_s": 0 },
        }),
        json!({ "name": "bad", "url": "ftp://example.com/hook" }),
        json!({ "name": "bad", "url": "http://example.com/hook" }),
    ] {
        let (status, code, _) = problem(
            owner
                .post(server.url("/api/v1/webhooks"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, 422, "body {body}");
        assert_eq!(code, "validation_failed");
    }
}

#[tokio::test]
async fn webhook_write_is_human_only_for_agents() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let token = token_for(&owner, &server, &agent_id).await;

    let (status, code, reason) = problem(
        reqwest::Client::new()
            .post(server.url("/api/v1/webhooks"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "should not work")
            .json(&json!({ "name": "agent", "url": "https://example.com/hook" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(code, "forbidden");
    assert_eq!(reason.as_deref(), Some("human_only"));
}

#[tokio::test]
async fn test_delivery_is_signed_and_recorded() {
    let server = start().await;
    let owner = server.owner().await;
    let (recorder, url) = start_receiver(200).await;
    // A filter that no event of this test matches, so only the test delivery
    // reaches the receiver.
    let (created, secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "tester",
            "url": url,
            "event_types": ["incident.resolved"],
            "selector": { "all": true },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let event_id = emit_app_event(&server, &app_id, "app.up").await;

    let res = owner
        .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
    let delivery: Value = res.json().await.unwrap();
    assert_eq!(delivery["status"], "delivered");
    assert_eq!(delivery["response_code"], 200);
    assert_eq!(delivery["event_id"], event_id);
    let delivery_id = delivery["id"].as_str().unwrap().to_string();

    let captured = wait_for(|| async {
        let captured = recorder.requests().await;
        (!captured.is_empty()).then_some(captured)
    })
    .await;
    let request = captured
        .iter()
        .find(|request| {
            request
                .headers
                .get("quoro-delivery-id")
                .is_some_and(|value| value.to_str().unwrap() == delivery_id)
        })
        .expect("the test delivery reached the receiver");
    let signed = request
        .headers
        .get("quoro-signature")
        .unwrap()
        .to_str()
        .unwrap();
    let (timestamp, value) = signed.split_once(',').unwrap();
    let timestamp = timestamp.strip_prefix("t=").unwrap();
    assert_eq!(signature(&secret, timestamp, &request.body), value);
    assert_eq!(
        request
            .headers
            .get("quoro-event-id")
            .unwrap()
            .to_str()
            .unwrap(),
        event_id
    );

    let list: Value = owner
        .get(server.url(&format!("/api/v1/webhooks/{webhook_id}/deliveries")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"][0]["id"], delivery_id);
}

/// One delivery row of a webhook, by test flag.
async fn delivery_row(
    server: &Server,
    webhook_id: &str,
    test: i64,
) -> Option<(String, i64, String, Option<String>)> {
    let row = sqlx::query(
        "SELECT id, attempt, status, next_attempt_at FROM webhook_deliveries \
         WHERE webhook_id = ? AND test = ? ORDER BY created_at LIMIT 1",
    )
    .bind(webhook_id)
    .bind(test)
    .fetch_optional(server.state.store.pool())
    .await
    .unwrap()?;
    Some((
        row.get("id"),
        row.get("attempt"),
        row.get("status"),
        row.get("next_attempt_at"),
    ))
}

/// A test delivery after a real delivery of the same event: the partial unique
/// index lets both attempt 1 rows coexist (#127).
#[tokio::test]
async fn test_delivery_after_a_real_delivery_does_not_collide() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (_recorder, url) = start_receiver(200).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "receiver",
            "url": url,
            "event_types": ["app.up"],
            "selector": { "apps": [app_id] },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    let event_id = emit_app_event(&server, &app_id, "app.up").await;

    let real = wait_for(|| async {
        let row = delivery_row(&server, &webhook_id, 0).await?;
        (row.2 == "delivered").then_some(row)
    })
    .await;
    assert_eq!(real.1, 1);

    let res = owner
        .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
    let delivery: Value = res.json().await.unwrap();
    assert_eq!(delivery["status"], "delivered");
    assert_eq!(delivery["event_id"], event_id);
    let replay = delivery_row(&server, &webhook_id, 1)
        .await
        .expect("the test row exists next to the real one");
    assert_eq!(replay.1, 1);
    assert_eq!(replay.2, "delivered");
    assert!(replay.3.is_none());
}

/// A test delivery of an event before the dispatcher reaches it does not block
/// the real delivery (#127).
#[tokio::test]
async fn test_delivery_before_the_real_one_does_not_block_it() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (_recorder, url) = start_receiver(200).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "receiver",
            "url": url,
            "event_types": ["app.up"],
            "selector": { "apps": [app_id] },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();

    emit_app_event(&server, &app_id, "app.up").await;
    let res = owner
        .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
    let test_delivery: Value = res.json().await.unwrap();
    assert_eq!(test_delivery["status"], "delivered");

    let real = wait_for(|| async {
        let row = delivery_row(&server, &webhook_id, 0).await?;
        (row.2 == "delivered").then_some(row)
    })
    .await;
    assert_eq!(real.1, 1);
    let test_row = delivery_row(&server, &webhook_id, 1)
        .await
        .expect("the test row exists next to the real one");
    assert_eq!(test_row.1, 1);
}

/// Two test deliveries of the same event in a row both answer 202.
#[tokio::test]
async fn two_test_deliveries_of_one_event_do_not_collide() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (_recorder, url) = start_receiver(200).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "tester",
            "url": url,
            "event_types": ["incident.resolved"],
            "selector": { "all": true },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    emit_app_event(&server, &app_id, "app.up").await;

    let mut ids = Vec::new();
    for _ in 0..2 {
        let res = owner
            .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
        let delivery: Value = res.json().await.unwrap();
        assert_eq!(delivery["status"], "delivered");
        ids.push(delivery["id"].as_str().unwrap().to_string());
    }
    assert_ne!(ids[0], ids[1]);
    let (rows,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ? AND test = 1")
            .bind(&webhook_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(rows, 2);
}

/// A failed test delivery is attempted once and never retried.
#[tokio::test]
async fn failed_test_delivery_is_not_retried() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "site").await;
    let app_id = create_app(&owner, &server, &project_id).await;
    let (_recorder, url) = start_receiver(500).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "tester",
            "url": url,
            "event_types": ["incident.resolved"],
            "selector": { "all": true },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    emit_app_event(&server, &app_id, "app.up").await;

    let res = owner
        .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "test: {}", res.text().await.unwrap());
    let delivery: Value = res.json().await.unwrap();
    assert_eq!(delivery["status"], "failed");

    // Give the dispatcher several ticks: a test row is never retried.
    wait_for(|| async {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ?")
                .bind(&webhook_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        (count > 0).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = ?")
            .bind(&webhook_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let row = delivery_row(&server, &webhook_id, 1).await.unwrap();
    assert_eq!(row.1, 1);
    assert_eq!(row.2, "failed");
    assert!(row.3.is_none());
}

/// A test delivery with an empty event log is a validation error.
#[tokio::test]
async fn test_delivery_without_an_event_is_refused() {
    let server = start().await;
    let owner = server.owner().await;
    let (_recorder, url) = start_receiver(200).await;
    let (created, _secret) = create_webhook(
        &owner,
        &server,
        json!({
            "name": "tester",
            "url": url,
            "event_types": ["incident.resolved"],
            "selector": { "all": true },
        }),
    )
    .await;
    let webhook_id = created["id"].as_str().unwrap().to_string();
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/webhooks/{webhook_id}/test")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}
