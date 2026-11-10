//! `/api/v1/agents*` and `POST /api/v1/auth/agent-token` over real HTTP.
//!
//! The security-critical parts: the agent key secret is shown once and never
//! stored or logged, `agent:write` is human-only, and an agent access token
//! carries no scope of its own.

mod common;

use common::{problem, start};
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_control_plane::store::{rfc3339_in, sha256_hex};
use quoro_types::{Prefix, is_id};
use serde_json::{Value, json};

fn local_node() -> LocalNode {
    LocalNode {
        name: "test-node".to_string(),
        capacity: Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 100,
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

async fn create_agent(
    owner: &reqwest::Client,
    server: &common::Server,
    name: &str,
    owner_user_id: &str,
) -> Value {
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
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create agent: {body}");
    body
}

async fn create_key(owner: &reqwest::Client, server: &common::Server, agent_id: &str) -> Value {
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "create key: {body}");
    body
}

async fn mint(
    server: &common::Server,
    key: &str,
    justification: Option<&str>,
) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut request = client
        .post(server.url("/api/v1/auth/agent-token"))
        .header("Authorization", format!("Bearer {key}"));
    if let Some(justification) = justification {
        request = request.header("Quoro-Justification", justification);
    }
    request.send().await.unwrap()
}

async fn owner_id(owner: &reqwest::Client, server: &common::Server) -> String {
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

#[tokio::test]
async fn agent_lifecycle_keys_and_tokens() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;

    let agent = create_agent(&owner, &server, "watcher", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap();
    assert!(is_id(Prefix::Agent, agent_id), "{agent_id}");
    assert_eq!(agent["name"], "watcher");
    assert_eq!(agent["owner_user_id"], owner_id);
    assert_eq!(agent["disabled"], false);

    // List and get.
    let list: Value = owner
        .get(server.url("/api/v1/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["id"], agent_id);

    let got: Value = owner
        .get(server.url(&format!("/api/v1/agents/{agent_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["id"], agent_id);

    // Patch name and model.
    let patched: Value = owner
        .patch(server.url(&format!("/api/v1/agents/{agent_id}")))
        .json(&json!({ "name": "watcher-2", "model": "test-model-2" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(patched["name"], "watcher-2");
    assert_eq!(patched["model"], "test-model-2");
    assert_eq!(patched["created_at"], agent["created_at"]);

    // Validation and conflicts.
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/agents"))
            .json(&json!({
                "name": "orphan",
                "model": "m",
                "owner_user_id": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/agents"))
            .json(&json!({
                "name": "watcher-2",
                "model": "m",
                "owner_user_id": owner_id,
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/agents/agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // A key: the secret starts with `akey_`, differs from the id, and only
    // its hash is stored.
    let created = create_key(&owner, &server, agent_id).await;
    let key_id = created["id"].as_str().unwrap();
    let secret = created["key"].as_str().unwrap();
    assert!(is_id(Prefix::AgentKey, key_id), "{key_id}");
    assert!(secret.starts_with("akey_"), "{secret}");
    assert_ne!(secret, key_id);
    assert_eq!(created["agent_id"], agent_id);
    let stored: (String,) = sqlx::query_as("SELECT key_hash FROM agent_keys WHERE id = ?")
        .bind(key_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(stored.0, sha256_hex(secret.as_bytes()));
    // Neither the change log nor the audit record carries the secret. The
    // hash travels in the key's `create` entry only (`node-agent-protocol.md`
    // section 7.1, ADR 0317).
    let payloads: Vec<(String, String)> = sqlx::query_as(
        "SELECT op, payload FROM change_log WHERE entity_type = 'agent_key' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(payloads.len(), 1, "{payloads:?}");
    for (op, payload) in payloads {
        assert!(
            !payload.contains(secret),
            "change log leaked the key: {payload}"
        );
        assert_eq!(op, "create");
        let payload: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["key_hash"], stored.0.as_str(), "{payload}");
    }
    let params: Vec<(String,)> = sqlx::query_as(
        "SELECT params FROM audit_log WHERE operation = 'POST /api/v1/agents/{agent_id}/keys'",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    for (params,) in params {
        assert!(
            !params.contains(secret),
            "audit log leaked the key: {params}"
        );
        assert!(
            !params.contains(&stored.0),
            "audit log carries the hash: {params}"
        );
    }

    // Mint an access token with the key and a justification.
    let res = mint(&server, secret, Some("checking the app")).await;
    let status = res.status();
    let body = res.text().await.unwrap();
    assert_eq!(status, 200, "mint: {body}");
    let token: Value = serde_json::from_str(&body).unwrap();
    let atk = token["access_token"].as_str().unwrap();
    assert!(is_id(Prefix::AccessToken, atk), "{atk}");
    let expires_at = token["expires_at"].as_str().unwrap();
    assert!(
        expires_at >= rfc3339_in(280).as_str() && expires_at <= rfc3339_in(320).as_str(),
        "expires_at {expires_at} is not about five minutes away"
    );

    // The access token authenticates `/auth/me` as the agent.
    let me: Value = reqwest::Client::new()
        .get(server.url("/api/v1/auth/me"))
        .header("Authorization", format!("Bearer {atk}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["type"], "agent");
    assert_eq!(me["agent"]["id"], agent_id);
    assert_eq!(me["agent"]["model"], "test-model-2");
    assert_eq!(me["grants"], json!([]));

    // An agent key is accepted only by the mint endpoint.
    let res = reqwest::Client::new()
        .get(server.url("/api/v1/agents"))
        .header("Authorization", format!("Bearer {secret}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);

    // A cookie never yields an agent.
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["type"], "user");
    assert!(!me["user"].is_null());
    let res = owner
        .post(server.url("/api/v1/auth/agent-token"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);

    // An access token past its expiry is unauthenticated.
    sqlx::query("UPDATE access_tokens SET expires_at = ? WHERE token_hash = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(sha256_hex(atk.as_bytes()))
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let res = reqwest::Client::new()
        .get(server.url("/api/v1/auth/me"))
        .header("Authorization", format!("Bearer {atk}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);

    // A key without a justification is a validation error, audited as error.
    let res = mint(&server, secret, None).await;
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // Revocation is immediate for minting and idempotent for the call itself.
    let res = owner
        .delete(server.url(&format!("/api/v1/agents/{agent_id}/keys/{key_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let res = owner
        .delete(server.url(&format!("/api/v1/agents/{agent_id}/keys/{key_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let res = mint(&server, secret, Some("after revocation")).await;
    assert_eq!(res.status(), 401);

    // A disabled agent cannot mint, and the refusal is audited.
    let second = create_key(&owner, &server, agent_id).await;
    let second_secret = second["key"].as_str().unwrap();
    let res = owner
        .patch(server.url(&format!("/api/v1/agents/{agent_id}")))
        .json(&json!({ "disabled": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let res = mint(&server, second_secret, Some("still watching")).await;
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("agent_disabled"));
    let refused: (String, String, Option<String>) = sqlx::query_as(
        "SELECT actor_type, result, result_detail FROM audit_log \
         WHERE operation = 'POST /api/v1/auth/agent-token' AND result = 'refused' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(refused.0, "agent");
    assert_eq!(refused.1, "refused");
    assert_eq!(refused.2.as_deref(), Some("agent_disabled"));
}

async fn list_keys(
    client: &reqwest::Client,
    server: &common::Server,
    agent_id: &str,
    query: &[(&str, &str)],
) -> reqwest::Response {
    client
        .get(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .query(query)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn every_role_lists_an_agents_keys_including_revoked_ones() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    let agent = create_agent(&owner, &server, "watcher", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let first = create_key(&owner, &server, &agent_id).await;
    let second = create_key(&owner, &server, &agent_id).await;
    let first_id = first["id"].as_str().unwrap().to_string();
    let second_id = second["id"].as_str().unwrap().to_string();
    let res = owner
        .delete(server.url(&format!("/api/v1/agents/{agent_id}/keys/{first_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);

    server
        .insert_user("operator@example.com", "operator-password", "operator")
        .await;
    server
        .insert_user("viewer@example.com", "viewer-password", "viewer")
        .await;
    for (email, password) in [
        (common::OWNER_EMAIL, common::OWNER_PASSWORD),
        ("operator@example.com", "operator-password"),
        ("viewer@example.com", "viewer-password"),
    ] {
        let client = server.client();
        server.login(&client, email, password).await;
        let res = list_keys(&client, &server, &agent_id, &[]).await;
        assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
        let body: Value = res.json().await.unwrap();
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 2, "{body}");
        let ids: Vec<&str> = items
            .iter()
            .map(|key| key["id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&first_id.as_str()), "{ids:?}");
        assert!(ids.contains(&second_id.as_str()), "{ids:?}");
        let revoked = items
            .iter()
            .find(|key| key["id"] == first_id)
            .expect("the revoked key is listed");
        assert!(revoked["revoked_at"].as_str().is_some(), "{revoked}");
        let live = items
            .iter()
            .find(|key| key["id"] == second_id)
            .expect("the live key is listed");
        assert!(live["revoked_at"].is_null(), "{live}");
        assert!(live["last_used_at"].is_null(), "{live}");
        for key in items {
            assert!(key.get("key").is_none(), "{key}");
            assert!(key.get("key_hash").is_none(), "{key}");
        }
        assert!(
            !res_text_has_secret(&body, first["key"].as_str().unwrap()),
            "a key secret leaked"
        );
    }
}

fn res_text_has_secret(body: &Value, secret: &str) -> bool {
    body.to_string().contains(secret)
}

#[tokio::test]
async fn agent_keys_page_with_a_cursor() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    let agent = create_agent(&owner, &server, "watcher", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap().to_string();
    let first = create_key(&owner, &server, &agent_id).await;
    let second = create_key(&owner, &server, &agent_id).await;

    let res = list_keys(&owner, &server, &agent_id, &[("limit", "1")]).await;
    let page: Value = res.json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["id"], second["id"]);
    let cursor = page["next_cursor"]
        .as_str()
        .expect("another page follows")
        .to_string();

    let res = list_keys(
        &owner,
        &server,
        &agent_id,
        &[("limit", "1"), ("cursor", &cursor)],
    )
    .await;
    let last: Value = res.json().await.unwrap();
    assert_eq!(last["items"].as_array().unwrap().len(), 1);
    assert_eq!(last["items"][0]["id"], first["id"]);
    assert!(last["next_cursor"].is_null());
}

#[tokio::test]
async fn listing_keys_of_an_unknown_agent_is_not_found() {
    let server = start().await;
    let owner = server.owner().await;
    let (status, code, _) =
        problem(list_keys(&owner, &server, "agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D", &[]).await).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

#[tokio::test]
async fn listing_keys_with_a_bad_limit_is_refused() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    let agent = create_agent(&owner, &server, "watcher", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap();
    let (status, code, _) =
        problem(list_keys(&owner, &server, agent_id, &[("limit", "0")]).await).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn agent_calls_are_human_only_and_scope_checked() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
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
    let app: Value = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
            "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "port": 8080,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let app_id = app["id"].as_str().unwrap();

    let agent = create_agent(&owner, &server, "operator-agent", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap();
    let key = create_key(&owner, &server, agent_id).await;
    let secret = key["key"].as_str().unwrap();
    let token: Value = mint(&server, secret, Some("acting on the incident"))
        .await
        .json()
        .await
        .unwrap();
    let atk = token["access_token"].as_str().unwrap();
    let agent_client = reqwest::Client::new();
    let bearer =
        |request: reqwest::RequestBuilder| request.header("Authorization", format!("Bearer {atk}"));

    // `agent:write` is human-only even for an agent with no grants.
    let res = bearer(agent_client.post(server.url("/api/v1/agents")))
        .header("Quoro-Justification", "trying to make a friend")
        .json(&json!({
            "name": "friend",
            "model": "m",
            "owner_user_id": owner_id,
        }))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
    let refusal: (String, String, Option<String>, String) = sqlx::query_as(
        "SELECT actor_type, result, result_detail, agent_model FROM audit_log \
         WHERE operation = 'POST /api/v1/agents' AND actor_type = 'agent' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(refusal.0, "agent");
    assert_eq!(refusal.1, "refused");
    assert_eq!(refusal.2.as_deref(), Some("human_only"));
    assert_eq!(refusal.3, "test-model");

    // With no grants, a non-destructive scope is `scope_missing`.
    let res = bearer(agent_client.post(server.url(&format!("/api/v1/apps/{app_id}/restart"))))
        .header("Quoro-Justification", "restarting the app")
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    // A destructive scope asks for approval and names the scope and target.
    let res = bearer(agent_client.delete(server.url(&format!("/api/v1/apps/{app_id}"))))
        .header("Quoro-Justification", "deleting the app")
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 403);
    assert_eq!(body["code"], "approval_required");
    assert_eq!(body["scope"], "app:delete");
    assert_eq!(body["target"]["type"], "app");
    assert_eq!(body["target"]["id"], app_id);

    // The refusal is in the audit log as a refused agent call.
    let refused: (String, String, Option<String>) = sqlx::query_as(
        "SELECT actor_type, result, result_detail FROM audit_log \
         WHERE operation = 'DELETE /api/v1/apps/{app_id}' AND actor_type = 'agent' \
         ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(
        refused,
        (
            "agent".to_string(),
            "refused".to_string(),
            Some("approval_required".to_string())
        )
    );

    // A human read is not audited, and a human is still a user.
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["type"], "user");
    assert!(me["agent"].is_null());
}

/// Section 7.3 step 4 and ADR 0317: replaying the `agent_key` entries into an
/// empty database rebuilds every column of every key, `key_hash` included,
/// with no placeholder. A mint and a revoke write updates that merge over a
/// create that carries the hash. Each rebuilt hash is the SHA-256 of the
/// secret the API handed out, so a promoted primary still accepts the key.
#[tokio::test]
async fn the_change_log_rebuilds_every_agent_key_column_and_the_hash_of_its_secret() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    let agent = create_agent(&owner, &server, "watcher", &owner_id).await;
    let agent_id = agent["id"].as_str().unwrap();
    let used = create_key(&owner, &server, agent_id).await;
    let revoked = create_key(&owner, &server, agent_id).await;
    let untouched = create_key(&owner, &server, agent_id).await;
    let res = mint(&server, used["key"].as_str().unwrap(), Some("replay test")).await;
    assert_eq!(res.status(), 200, "mint: {}", res.text().await.unwrap());
    let res = owner
        .delete(server.url(&format!(
            "/api/v1/agents/{agent_id}/keys/{}",
            revoked["id"].as_str().unwrap()
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204, "revoke");

    let rebuilt = common::replay::rebuild(&server, &["agent_key"]).await;
    let columns = common::replay::column_names(&server.state.store, "agent_keys").await;
    assert!(columns.iter().any(|c| c == "key_hash"), "{columns:?}");
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    common::replay::assert_rebuilt(&server, &rebuilt, "agent_keys", &columns, 3).await;

    for created in [&used, &revoked, &untouched] {
        let (hash, last_used_at, revoked_at): (String, Option<String>, Option<String>) =
            sqlx::query_as(
                "SELECT key_hash, last_used_at, revoked_at FROM agent_keys WHERE id = ?",
            )
            .bind(created["id"].as_str().unwrap())
            .fetch_one(rebuilt.pool())
            .await
            .unwrap();
        assert_eq!(
            hash,
            sha256_hex(created["key"].as_str().unwrap().as_bytes()),
            "the rebuilt hash of {} matches its secret",
            created["id"]
        );
        assert_eq!(last_used_at.is_some(), created["id"] == used["id"]);
        assert_eq!(revoked_at.is_some(), created["id"] == revoked["id"]);
    }
}

/// Section 7.1: session and access-token hashes stay on the primary
/// (`sessions` and `access_tokens` are local). After a login and an agent
/// token exchange, no change-log entry's payload carries any of them.
#[tokio::test]
async fn session_and_access_token_hashes_stay_out_of_the_change_log() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = owner_id(&owner, &server).await;
    let client = server.client();
    server
        .login(&client, common::OWNER_EMAIL, common::OWNER_PASSWORD)
        .await;
    let agent = create_agent(&client, &server, "watcher", &owner_id).await;
    let key = create_key(&client, &server, agent["id"].as_str().unwrap()).await;
    let res = mint(&server, key["key"].as_str().unwrap(), Some("scan test")).await;
    assert_eq!(res.status(), 200, "mint: {}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();

    let pool = server.state.store.pool();
    let sessions: Vec<(String,)> = sqlx::query_as("SELECT token_hash FROM sessions")
        .fetch_all(pool)
        .await
        .unwrap();
    assert!(sessions.len() >= 2, "a session for each login");
    let access: Vec<(String,)> = sqlx::query_as("SELECT token_hash FROM access_tokens")
        .fetch_all(pool)
        .await
        .unwrap();
    assert_eq!(
        access,
        vec![(sha256_hex(
            token["access_token"].as_str().unwrap().as_bytes()
        ),)]
    );
    let payloads: Vec<(i64, String)> = sqlx::query_as("SELECT seq, payload FROM change_log")
        .fetch_all(pool)
        .await
        .unwrap();
    assert!(!payloads.is_empty());
    for (hash,) in sessions.iter().chain(&access) {
        for (seq, payload) in &payloads {
            assert!(
                !payload.contains(hash.as_str()),
                "change-log entry {seq} carries a local hash"
            );
        }
    }
}
