//! `GET|POST /api/v1/projects` and `GET /api/v1/projects/{project_id}`.

mod common;

use quoro_types::{Prefix, is_id};
use serde_json::{Value, json};

const VIEWER_PASSWORD: &str = "viewer password, long enough";

#[tokio::test]
async fn create_list_and_get_a_project() {
    let server = common::start().await;
    let owner = server.owner().await;

    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "web", "name": "Web" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "web-two", "name": "Web Two" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = project["id"].as_str().unwrap().to_string();
    assert!(is_id(Prefix::Project, &id), "{id}");

    // A duplicate slug is a conflict.
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "web", "name": "Other" }))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // An invalid slug is a validation failure.
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "Web Two!", "name": "Bad" }))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // An empty name is too.
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "empty-name", "name": "" }))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // Listing returns both, in id order, with no further page.
    let res = owner
        .get(server.url("/api/v1/projects"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "{list}");
    assert!(list["next_cursor"].is_null(), "{list}");

    // Reading one returns it; an unknown or malformed id is a 404.
    let res = owner
        .get(server.url(&format!("/api/v1/projects/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let fetched: Value = res.json().await.unwrap();
    assert_eq!(fetched["id"].as_str(), Some(id.as_str()));
    assert_eq!(fetched["slug"].as_str(), Some("web-two"));

    for unknown in ["prj_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "prj_nope"] {
        let res = owner
            .get(server.url(&format!("/api/v1/projects/{unknown}")))
            .send()
            .await
            .unwrap();
        let (status, code, _) = common::problem(res).await;
        assert_eq!((status, code.as_str()), (404, "not_found"));
    }
}

#[tokio::test]
async fn a_viewer_may_not_create_and_the_refusal_is_audited() {
    let server = common::start().await;
    let _owner = server.owner().await;
    server
        .insert_user("viewer@example.com", VIEWER_PASSWORD, "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", VIEWER_PASSWORD)
        .await;

    let res = viewer
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "nope", "name": "Nope" }))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    let audit = server.audit().await;
    let (operation, result, detail) = audit.last().unwrap();
    assert_eq!(operation, "POST /api/v1/projects");
    assert_eq!(result, "refused");
    assert_eq!(detail.as_deref(), Some("scope_missing"));

    // A refusal claims no idempotency slot: the same key is refused again,
    // not reported as a conflicting reuse.
    for _ in 0..2 {
        let res = viewer
            .post(server.url("/api/v1/projects"))
            .header("Idempotency-Key", "viewer-try")
            .json(&json!({ "slug": "nope", "name": "Nope" }))
            .send()
            .await
            .unwrap();
        assert_eq!(common::problem(res).await.0, 403);
    }

    // The viewer may still read.
    let res = viewer
        .get(server.url("/api/v1/projects"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn an_idempotency_key_replays_the_first_create() {
    let server = common::start().await;
    let owner = server.owner().await;
    let body = json!({ "slug": "replayed", "name": "Replayed" });

    let first: Value = owner
        .post(server.url("/api/v1/projects"))
        .header("Idempotency-Key", "create-replayed-1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = owner
        .post(server.url("/api/v1/projects"))
        .header("Idempotency-Key", "create-replayed-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let replay: Value = res.json().await.unwrap();
    assert_eq!(replay["id"], first["id"]);

    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM projects")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // The replay is audited too (IdempotencyKey: D-0031 records the call
    // either way), `ok` and without a change-log link.
    let audit: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT result, change_seq FROM audit_log WHERE operation = 'POST /api/v1/projects' AND target_id = ? ORDER BY seq",
    )
    .bind(first["id"].as_str().unwrap())
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(audit.len(), 2, "{audit:?}");
    assert!(audit.iter().all(|(result, _)| result == "ok"));
    assert!(audit[0].1.is_some() && audit[1].1.is_none(), "{audit:?}");
}
