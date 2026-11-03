//! `GET /api/v1/events` and the SSE stream (D-0032, D-0057), over real HTTP.

mod common;

use common::{Server, problem, start};
use quoro_control_plane::events::{self, NewEvent, Source};
use serde_json::{Value, json};
use std::time::Duration;

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

/// Emits one event into the log, as a detection would.
async fn emit(
    server: &Server,
    kind: &str,
    subject_type: &str,
    subject_id: &str,
    project_id: Option<&str>,
) -> (String, i64) {
    let mut w = server.state.store.begin_write().await.unwrap();
    let id = events::emit(
        &mut w.tx,
        NewEvent {
            kind,
            source: Source::ControlPlane,
            subject_type,
            subject_id,
            project_id,
            request_id: None,
            actor: None,
            data: json!({ "app_id": subject_id }),
        },
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
    let (seq,): (i64,) = sqlx::query_as("SELECT seq FROM events WHERE id = ?")
        .bind(&id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    (id, seq)
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

async fn create_agent(
    owner: &reqwest::Client,
    server: &Server,
    name: &str,
    owner_user_id: &str,
) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": name,
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
    assert_eq!(
        res.status(),
        200,
        "mint token: {}",
        res.text().await.unwrap()
    );
    let token: Value = res.json().await.unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

async fn grant(owner: &reqwest::Client, server: &Server, body: Value) -> Value {
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create grant: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

/// The current head of the event log.
async fn head(server: &Server) -> i64 {
    let (seq,): (i64,) = sqlx::query_as("SELECT IFNULL(MAX(seq), 0) FROM events")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    seq
}

/// Reads one complete SSE frame (up to the blank line) from `response`.
async fn next_frame(response: &mut reqwest::Response, buffer: &mut String) -> String {
    loop {
        if let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_string();
            buffer.drain(..index + 2);
            return frame;
        }
        let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
            .await
            .expect("an SSE chunk within 5 s")
            .expect("the stream is open")
            .expect("a chunk");
        buffer.push_str(&String::from_utf8_lossy(&chunk));
    }
}

fn frame_field<'a>(frame: &'a str, name: &str) -> Option<&'a str> {
    frame.lines().find_map(|line| {
        line.strip_prefix(name)
            .and_then(|rest| rest.strip_prefix(": "))
    })
}

#[tokio::test]
async fn list_filters_by_type_subject_after_seq_and_cursor() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_a = create_app(&owner, &server, &project_id, "a").await;
    let app_b = create_app(&owner, &server, &project_id, "b").await;

    let baseline = head(&server).await;
    let (_, first) = emit(&server, "app.down", "app", &app_a, Some(&project_id)).await;
    let (_, second) = emit(&server, "app.up", "app", &app_a, Some(&project_id)).await;
    let (_, third) = emit(&server, "app.down", "app", &app_b, Some(&project_id)).await;

    let all: Value = owner
        .get(server.url(&format!("/api/v1/events?after_seq={baseline}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let items = all["items"].as_array().unwrap();
    assert_eq!(items.len(), 3, "{all}");
    assert_eq!(items[0]["seq"], first);
    assert_eq!(items[2]["seq"], third);
    assert_eq!(items[0]["type"], "app.down");
    assert_eq!(items[0]["subject"]["id"], app_a);
    assert_eq!(items[0]["source"]["kind"], "control_plane");
    assert!(items[0]["id"].as_str().unwrap().starts_with("evt_"));

    let downs: Value = owner
        .get(server.url(&format!(
            "/api/v1/events?type=app.down&after_seq={baseline}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(downs["items"].as_array().unwrap().len(), 2);

    let both_types: Value = owner
        .get(server.url(&format!(
            "/api/v1/events?type=app.down&type=app.up&after_seq={baseline}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(both_types["items"].as_array().unwrap().len(), 3);

    let subject: Value = owner
        .get(server.url(&format!(
            "/api/v1/events?subject_id={app_a}&after_seq={baseline}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let subject_items = subject["items"].as_array().unwrap();
    assert_eq!(subject_items.len(), 2);
    assert_eq!(subject_items[1]["seq"], second);

    let resumed: Value = owner
        .get(server.url(&format!("/api/v1/events?after_seq={first}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let resumed_items = resumed["items"].as_array().unwrap();
    assert_eq!(resumed_items.len(), 2);
    assert_eq!(resumed_items[0]["seq"], second);

    let page: Value = owner
        .get(server.url(&format!("/api/v1/events?limit=2&after_seq={baseline}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    let cursor = page["next_cursor"].as_str().unwrap().to_string();
    let next: Value = owner
        .get(server.url(&format!(
            "/api/v1/events?cursor={cursor}&after_seq={baseline}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let next_items = next["items"].as_array().unwrap();
    assert_eq!(next_items.len(), 1);
    assert_eq!(next_items[0]["seq"], third);
    assert!(next["next_cursor"].is_null());

    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/events?type=app.exploded"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn an_agent_sees_only_the_events_its_selector_covers() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_a = create_project(&owner, &server, "alpha").await;
    let project_b = create_project(&owner, &server, "beta").await;
    let app_a = create_app(&owner, &server, &project_a, "a").await;
    let app_b = create_app(&owner, &server, &project_b, "b").await;
    emit(&server, "app.down", "app", &app_a, Some(&project_a)).await;
    emit(&server, "app.down", "app", &app_b, Some(&project_b)).await;

    let agent_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    grant(
        &owner,
        &server,
        json!({
            "agent_id": agent_id,
            "scopes": ["event:read"],
            "selector": { "projects": ["alpha"] },
            "ttl_s": 3600,
            "reason": "read one project's events",
        }),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;
    let res = reqwest::Client::new()
        .get(server.url("/api/v1/events?type=app.down"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    assert_eq!(items[0]["subject"]["id"], app_a);

    // The read is audited as an agent's read.
    let row: (String, String, String) = sqlx::query_as(
        "SELECT actor_type, actor_id, result FROM audit_log \
         WHERE operation = 'GET /api/v1/events' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0, "agent");
    assert_eq!(row.1, agent_id);
    assert_eq!(row.2, "ok");
}

#[tokio::test]
async fn the_stream_delivers_a_new_event_and_resumes_after_last_event_id() {
    let server = start().await;
    let owner = server.owner().await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let baseline = head(&server).await;
    let mut response = owner
        .get(server.url("/api/v1/events/stream"))
        .header("Last-Event-ID", baseline.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-cache")
    );
    let (first_id, first_seq) = emit(&server, "app.down", "app", &app_id, Some(&project_id)).await;
    let mut buffer = String::new();
    let frame = next_frame(&mut response, &mut buffer).await;
    assert_eq!(
        frame_field(&frame, "id"),
        Some(first_seq.to_string().as_str())
    );
    assert_eq!(frame_field(&frame, "event"), Some("app.down"));
    let data: Value = serde_json::from_str(frame_field(&frame, "data").unwrap()).unwrap();
    assert_eq!(data["id"], first_id);
    assert_eq!(data["seq"], first_seq);

    // A new event reaches the open stream.
    let (_, second_seq) = emit(&server, "app.up", "app", &app_id, Some(&project_id)).await;
    let frame = next_frame(&mut response, &mut buffer).await;
    assert_eq!(
        frame_field(&frame, "id"),
        Some(second_seq.to_string().as_str())
    );
    assert_eq!(frame_field(&frame, "event"), Some("app.up"));

    // A reconnect resumes after the last seq it saw: the third event only.
    drop(response);
    let (_, third_seq) = emit(&server, "app.down", "app", &app_id, Some(&project_id)).await;
    let mut response = owner
        .get(server.url("/api/v1/events/stream"))
        .header("Last-Event-ID", second_seq.to_string())
        .send()
        .await
        .unwrap();
    let mut buffer = String::new();
    let frame = next_frame(&mut response, &mut buffer).await;
    assert_eq!(
        frame_field(&frame, "id"),
        Some(third_seq.to_string().as_str())
    );
    assert_eq!(frame_field(&frame, "event"), Some("app.down"));
}

#[tokio::test]
async fn the_stream_applies_the_agent_selector_to_every_event() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_a = create_project(&owner, &server, "alpha").await;
    let project_b = create_project(&owner, &server, "beta").await;
    let app_a = create_app(&owner, &server, &project_a, "a").await;
    let app_b = create_app(&owner, &server, &project_b, "b").await;

    let agent_id = create_agent(&owner, &server, "streamer", &owner_user_id).await;
    grant(
        &owner,
        &server,
        json!({
            "agent_id": agent_id,
            "scopes": ["event:read"],
            "selector": { "projects": ["alpha"] },
            "ttl_s": 3600,
            "reason": "stream one project",
        }),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    // The out-of-scope event already exists when the stream starts.
    emit(&server, "app.down", "app", &app_b, Some(&project_b)).await;
    let baseline = head(&server).await;
    let mut response = reqwest::Client::new()
        .get(server.url("/api/v1/events/stream"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Last-Event-ID", baseline.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let (_, visible_seq) = emit(&server, "app.down", "app", &app_a, Some(&project_a)).await;
    let mut buffer = String::new();
    let frame = next_frame(&mut response, &mut buffer).await;
    assert_eq!(
        frame_field(&frame, "id"),
        Some(visible_seq.to_string().as_str()),
        "the out-of-scope event is not streamed"
    );
    assert!(buffer.is_empty() || !buffer.contains("app.down"));
}
