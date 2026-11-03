//! `GET|POST /api/v1/providers`, `GET|DELETE /api/v1/providers/{provider_id}`
//! and `POST /api/v1/providers/{provider_id}/rotate`.
//!
//! The secret is the security-critical part: it must never appear in a
//! response body, the change log or the audit log, and the database must hold
//! only its ciphertext.

mod common;

use quoro_control_plane::api::providers::{find_providers, provider_secret};
use quoro_types::{Prefix, is_id};
use serde_json::{Value, json};

const SECRET: &str = "sk-live-very-secret-value-9f3a";
const NEW_SECRET: &str = "sk-live-rotated-secret-value-42c7";
const VIEWER_PASSWORD: &str = "viewer password, long enough";

/// Recursively asserts that no object in `value` carries a secret field.
fn assert_no_secret_keys(value: &Value) {
    match value {
        Value::Object(map) => {
            assert!(!map.contains_key("secret"), "leaked `secret`: {value}");
            assert!(
                !map.contains_key("secret_enc"),
                "leaked `secret_enc`: {value}"
            );
            for child in map.values() {
                assert_no_secret_keys(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                assert_no_secret_keys(item);
            }
        }
        _ => {}
    }
}

/// Asserts the raw response bytes never contain the plaintext.
fn assert_no_plaintext(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    assert!(!text.contains(SECRET), "response leaked the secret: {text}");
    assert!(
        !text.contains(NEW_SECRET),
        "response leaked the rotated secret: {text}"
    );
}

async fn insert_provider(server: &common::Server, client: &reqwest::Client, body: &Value) -> Value {
    let res = client
        .post(server.url("/api/v1/providers"))
        .json(body)
        .send()
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.bytes().await.unwrap();
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&bytes));
    assert_no_plaintext(&bytes);
    let provider: Value = serde_json::from_slice(&bytes).unwrap();
    assert_no_secret_keys(&provider);
    provider
}

#[tokio::test]
async fn create_get_and_list_never_return_the_secret() {
    let server = common::start().await;
    let owner = server.owner().await;
    let body = json!({
        "kind": "dns",
        "name": "Cloudflare",
        "config": { "zone_id": "z1", "account_id": "a1" },
        "secret": SECRET,
    });

    let res = owner
        .post(server.url("/api/v1/providers"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let bytes = res.bytes().await.unwrap();
    assert_no_plaintext(&bytes);
    let provider: Value = serde_json::from_slice(&bytes).unwrap();
    assert_no_secret_keys(&provider);
    assert_eq!(provider["has_secret"], json!(true));
    assert_eq!(provider["kind"], json!("dns"));
    assert_eq!(provider["name"], json!("Cloudflare"));
    assert_eq!(provider["config"]["zone_id"], json!("z1"));
    assert!(provider["rotated_at"].is_null());
    let id = provider["id"].as_str().unwrap().to_string();
    assert!(is_id(Prefix::Provider, &id), "{id}");

    let res = owner
        .get(server.url(&format!("/api/v1/providers/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let bytes = res.bytes().await.unwrap();
    assert_no_plaintext(&bytes);
    let fetched: Value = serde_json::from_slice(&bytes).unwrap();
    assert_no_secret_keys(&fetched);
    assert_eq!(fetched["id"].as_str(), Some(id.as_str()));

    let res = owner
        .get(server.url("/api/v1/providers"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let bytes = res.bytes().await.unwrap();
    assert_no_plaintext(&bytes);
    let list: Value = serde_json::from_slice(&bytes).unwrap();
    assert_no_secret_keys(&list);
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    assert!(list["next_cursor"].is_null(), "{list}");

    // Unknown and malformed ids are both 404.
    for unknown in ["prv_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "prv_nope"] {
        let res = owner
            .get(server.url(&format!("/api/v1/providers/{unknown}")))
            .send()
            .await
            .unwrap();
        let (status, code, _) = common::problem(res).await;
        assert_eq!((status, code.as_str()), (404, "not_found"));
    }
}

#[tokio::test]
async fn the_database_holds_only_ciphertext_bound_to_the_row() {
    let server = common::start().await;
    let owner = server.owner().await;
    let provider = insert_provider(
        &server,
        &owner,
        &json!({
            "kind": "tunnel",
            "name": "Tunnel",
            "config": { "tunnel_id": "t1" },
            "secret": SECRET,
        }),
    )
    .await;
    let id = provider["id"].as_str().unwrap();

    let (blob,): (Vec<u8>,) = sqlx::query_as("SELECT secret_enc FROM providers WHERE id = ?")
        .bind(id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let key_id = server.state.data_key.key_id();
    assert!(blob.starts_with(&key_id), "blob is not keyed");
    assert!(
        !blob
            .windows(SECRET.len())
            .any(|window| window == SECRET.as_bytes()),
        "the plaintext is stored"
    );

    let (record, secret) = provider_secret(&server.state, id).await.unwrap();
    assert_eq!(record.id, id);
    assert_eq!(record.kind, "tunnel");
    assert_eq!(record.config["tunnel_id"], json!("t1"));
    assert_eq!(secret.expose(), SECRET);

    let found = find_providers(&server.state, "tunnel").await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, id);
    assert!(
        find_providers(&server.state, "lb")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn the_change_log_and_audit_log_carry_no_plaintext() {
    let server = common::start().await;
    let owner = server.owner().await;
    insert_provider(
        &server,
        &owner,
        &json!({
            "kind": "object_store",
            "name": "Backups",
            "config": { "bucket": "b" },
            "secret": SECRET,
        }),
    )
    .await;

    let payloads: Vec<(String,)> =
        sqlx::query_as("SELECT payload FROM change_log WHERE entity_type = 'provider'")
            .fetch_all(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(payloads.len(), 1);
    for (payload,) in &payloads {
        assert!(!payload.contains(SECRET), "change log leaked: {payload}");
        assert!(payload.contains("secret_enc"), "{payload}");
    }

    let params: Vec<(String,)> = sqlx::query_as("SELECT params FROM audit_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    assert!(!params.is_empty());
    for (params,) in &params {
        assert!(!params.contains(SECRET), "audit log leaked: {params}");
    }
    assert!(
        params.iter().any(|(params,)| params.contains("[redacted]")),
        "{params:?}"
    );
}

#[tokio::test]
async fn validation_conflicts_unauthenticated_and_forbidden() {
    let server = common::start().await;
    let owner = server.owner().await;
    let body = json!({
        "kind": "lb",
        "name": "Balancer",
        "config": { "pool": "p" },
        "secret": SECRET,
    });
    insert_provider(&server, &owner, &body).await;

    // A duplicate name is a conflict.
    let res = owner
        .post(server.url("/api/v1/providers"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // Bad kind, empty secret, non-object config and an over-long name.
    let cases = [
        json!({ "kind": "dns2", "name": "X", "config": {}, "secret": SECRET }),
        json!({ "kind": "dns", "name": "X", "config": {}, "secret": "" }),
        json!({ "kind": "dns", "name": "X", "config": [], "secret": SECRET }),
        json!({ "kind": "dns", "name": "x".repeat(201), "config": {}, "secret": SECRET }),
    ];
    for body in cases {
        let res = owner
            .post(server.url("/api/v1/providers"))
            .json(&body)
            .send()
            .await
            .unwrap();
        let (status, code, _) = common::problem(res).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{body}"
        );
    }

    // No credential at all.
    let anonymous = server.client();
    let res = anonymous
        .post(server.url("/api/v1/providers"))
        .json(&json!({ "kind": "dns", "name": "Anon", "config": {}, "secret": SECRET }))
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));

    // A viewer lacks `provider:manage`, and the refusal is audited.
    server
        .insert_user("viewer@example.com", VIEWER_PASSWORD, "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", VIEWER_PASSWORD)
        .await;
    let res = viewer
        .post(server.url("/api/v1/providers"))
        .json(&json!({ "kind": "dns", "name": "Nope", "config": {}, "secret": SECRET }))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));
    let audit = server.audit().await;
    let (operation, result, detail) = audit.last().unwrap();
    assert_eq!(operation, "POST /api/v1/providers");
    assert_eq!(result, "refused");
    assert_eq!(detail.as_deref(), Some("scope_missing"));

    // Reads need the same scope.
    let res = viewer
        .get(server.url("/api/v1/providers"))
        .send()
        .await
        .unwrap();
    assert_eq!(common::problem(res).await.0, 403);
}

#[tokio::test]
async fn an_idempotency_key_replays_and_refuses_a_changed_body() {
    let server = common::start().await;
    let owner = server.owner().await;
    let body = json!({
        "kind": "dns",
        "name": "Replayed",
        "config": { "zone_id": "z" },
        "secret": SECRET,
    });

    let first = owner
        .post(server.url("/api/v1/providers"))
        .header("Idempotency-Key", "provider-create-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);
    let first: Value = first.json().await.unwrap();

    let replay = owner
        .post(server.url("/api/v1/providers"))
        .header("Idempotency-Key", "provider-create-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 201);
    let replay: Value = replay.json().await.unwrap();
    assert_eq!(replay["id"], first["id"]);
    assert_no_secret_keys(&replay);

    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM providers")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // The same key with a different secret is a conflict.
    let mut changed = body.clone();
    changed["secret"] = json!(NEW_SECRET);
    changed["name"] = json!("Replayed Two");
    let res = owner
        .post(server.url("/api/v1/providers"))
        .header("Idempotency-Key", "provider-create-1")
        .json(&changed)
        .send()
        .await
        .unwrap();
    let (status, code, _) = common::problem(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn rotate_replaces_the_secret_and_delete_removes_the_row() {
    let server = common::start().await;
    let owner = server.owner().await;
    let provider = insert_provider(
        &server,
        &owner,
        &json!({
            "kind": "dns",
            "name": "Rotated",
            "config": { "zone_id": "z" },
            "secret": SECRET,
        }),
    )
    .await;
    let id = provider["id"].as_str().unwrap().to_string();
    let (old_blob,): (Vec<u8>,) = sqlx::query_as("SELECT secret_enc FROM providers WHERE id = ?")
        .bind(&id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();

    let res = owner
        .post(server.url(&format!("/api/v1/providers/{id}/rotate")))
        .json(&json!({ "secret": NEW_SECRET }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let bytes = res.bytes().await.unwrap();
    assert_no_plaintext(&bytes);
    let rotated: Value = serde_json::from_slice(&bytes).unwrap();
    assert_no_secret_keys(&rotated);
    assert!(rotated["rotated_at"].is_string(), "{rotated}");

    let (new_blob,): (Vec<u8>,) = sqlx::query_as("SELECT secret_enc FROM providers WHERE id = ?")
        .bind(&id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_ne!(old_blob, new_blob);
    let (_, secret) = provider_secret(&server.state, &id).await.unwrap();
    assert_eq!(secret.expose(), NEW_SECRET);

    // An empty secret is a validation failure, an unknown id a 404.
    let res = owner
        .post(server.url(&format!("/api/v1/providers/{id}/rotate")))
        .json(&json!({ "secret": "" }))
        .send()
        .await
        .unwrap();
    assert_eq!(common::problem(res).await.0, 422);
    let res = owner
        .post(server.url("/api/v1/providers/prv_01JC9Z6Q5W8N3K4T7R2M1XHB0D/rotate"))
        .json(&json!({ "secret": NEW_SECRET }))
        .send()
        .await
        .unwrap();
    assert_eq!(common::problem(res).await.0, 404);

    let res = owner
        .delete(server.url(&format!("/api/v1/providers/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let res = owner
        .get(server.url(&format!("/api/v1/providers/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(common::problem(res).await.0, 404);
    let res = owner
        .delete(server.url("/api/v1/providers/prv_01JC9Z6Q5W8N3K4T7R2M1XHB0D"))
        .send()
        .await
        .unwrap();
    assert_eq!(common::problem(res).await.0, 404);
}

#[tokio::test]
async fn delete_is_refused_while_the_provider_is_referenced() {
    let server = common::start().await;
    let owner = server.owner().await;
    let provider = insert_provider(
        &server,
        &owner,
        &json!({
            "kind": "object_store",
            "name": "Audit copy",
            "config": { "bucket": "b" },
            "secret": SECRET,
        }),
    )
    .await;
    let id = provider["id"].as_str().unwrap().to_string();
    let pool = server.state.store.pool();
    sqlx::query(
        "INSERT INTO audit_exports (id, target, updated_at, provider_id)
         VALUES ('aex_1', 's3://b/audit', '2026-11-03T00:00:00Z', ?)",
    )
    .bind(&id)
    .execute(pool)
    .await
    .unwrap();

    let delete = || async {
        owner
            .delete(server.url(&format!("/api/v1/providers/{id}")))
            .send()
            .await
            .unwrap()
    };
    let (status, code, _) = common::problem(delete().await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM providers WHERE id = ?")
        .bind(&id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    sqlx::query("UPDATE audit_exports SET provider_id = NULL")
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(delete().await.status(), 204);
}
