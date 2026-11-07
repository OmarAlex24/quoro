//! `/api/v1/tokens*` over real HTTP (`listTokens`, `createToken`,
//! `revokeToken`).
//!
//! The security-critical parts: the secret is shown once and only its hash is
//! stored or logged, a token authenticates as its human until it expires or
//! is revoked, a human only ever sees and revokes their own tokens, and an
//! agent can never manage tokens (`human_only`).

mod common;

use common::{problem, start};
use quoro_control_plane::store::{rfc3339_in, sha256_hex};
use serde_json::{Value, json};

/// `channel, result, scope, target_type, target_id, params, change_seq`.
type AuditRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<i64>,
);

const AUDIT_PARAMS: &str = "SELECT params FROM audit_log";

fn is_token_id(id: &str) -> bool {
    id.strip_prefix("tok_").is_some_and(|rest| {
        rest.len() == 26
            && rest
                .bytes()
                .all(|b| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&b))
    })
}

async fn create(
    client: &reqwest::Client,
    server: &common::Server,
    body: Value,
) -> reqwest::Response {
    client
        .post(server.url("/api/v1/tokens"))
        .json(&body)
        .send()
        .await
        .unwrap()
}

/// Creates a token and returns the response body. The body holds the secret,
/// so it never goes into an assertion message.
async fn create_ok(client: &reqwest::Client, server: &common::Server, name: &str) -> Value {
    let res = create(client, server, json!({ "name": name })).await;
    assert_eq!(res.status(), 201, "create token");
    res.json().await.unwrap()
}

fn bearer(token: &str) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

async fn me_status(server: &common::Server, token: &str) -> (u16, Value) {
    let res = bearer(token)
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let body = res.json().await.unwrap_or(Value::Null);
    (status, body)
}

async fn list(client: &reqwest::Client, server: &common::Server) -> Value {
    let res = client
        .get(server.url("/api/v1/tokens"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

async fn user_id(client: &reqwest::Client, server: &common::Server) -> String {
    let me: Value = client
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    me["user"]["id"].as_str().unwrap().to_string()
}

/// Every audit `params` and change-log `payload`, to search for a secret.
async fn logged_text(server: &common::Server) -> Vec<String> {
    let mut text: Vec<(String,)> = sqlx::query_as(AUDIT_PARAMS)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    let payloads: Vec<(String,)> = sqlx::query_as("SELECT payload FROM change_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    text.extend(payloads);
    text.into_iter().map(|(t,)| t).collect()
}

#[tokio::test]
async fn create_shows_the_secret_once_and_it_authenticates_as_the_user() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = user_id(&owner, &server).await;

    let created = create_ok(&owner, &server, "  laptop cli  ").await;
    let id = created["id"].as_str().unwrap().to_string();
    let secret = created["token"].as_str().unwrap().to_string();
    assert!(is_token_id(&id), "{id}");
    assert!(secret.starts_with("tok_"), "secret has the tok_ prefix");
    assert_ne!(secret, id);
    assert_eq!(created["user_id"], owner_id.as_str());
    assert_eq!(created["name"], "laptop cli", "the label is trimmed");
    assert_eq!(created["expires_at"], Value::Null);
    assert_eq!(created["last_used_at"], Value::Null);
    assert_eq!(created["revoked_at"], Value::Null);

    // Only the hash is stored.
    let (hash,): (String,) = sqlx::query_as("SELECT token_hash FROM api_tokens WHERE id = ?")
        .bind(&id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(hash, sha256_hex(secret.as_bytes()));
    for text in logged_text(&server).await {
        assert!(!text.contains(&secret), "a log carries the secret");
        assert!(!text.contains(&hash), "a log carries the hash");
    }

    // The secret authenticates as the owner.
    let (status, me) = me_status(&server, &secret).await;
    assert_eq!(status, 200);
    assert_eq!(me["user"]["id"], owner_id.as_str());

    // The list carries the token but never its secret or hash.
    let page = list(&owner, &server).await;
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], id.as_str());
    assert!(items[0].get("token").is_none());
    assert!(items[0].get("token_hash").is_none());
    assert_eq!(page["next_cursor"], Value::Null);
    let raw = owner
        .get(server.url("/api/v1/tokens"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!raw.contains(&secret) && !raw.contains(&hash));

    // The token itself can manage tokens too (it is the human's credential).
    let by_token = bearer(&secret);
    let page = list(&by_token, &server).await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn each_create_and_revoke_writes_one_change_and_one_audit_record() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = user_id(&owner, &server).await;
    let created = create_ok(&owner, &server, "ci").await;
    let id = created["id"].as_str().unwrap().to_string();

    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT seq, op, actor_type, actor_id, payload FROM change_log \
         WHERE entity_type = 'api_token' AND entity_id = ? ORDER BY seq",
    )
    .bind(&id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    let (create_seq, op, actor_type, actor_id, payload) = &rows[0];
    assert_eq!(op, "create");
    assert_eq!(
        (actor_type.as_str(), actor_id.as_str()),
        ("user", owner_id.as_str())
    );
    let payload: Value = serde_json::from_str(payload).unwrap();
    assert_eq!(payload["id"], id.as_str());
    assert_eq!(payload["user_id"], owner_id.as_str());
    assert_eq!(payload["name"], "ci");
    assert!(payload.get("token_hash").is_none());

    let record: AuditRow = sqlx::query_as(
        "SELECT channel, result, scope, target_type, target_id, params, change_seq FROM audit_log \
             WHERE operation = 'POST /api/v1/tokens' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(record.0, "panel", "a session cookie is the panel channel");
    assert_eq!(record.1, "ok");
    assert_eq!(record.2, None, "the operation has no scope");
    assert_eq!(record.3.as_deref(), Some("user"));
    assert_eq!(record.4.as_deref(), Some(owner_id.as_str()));
    let params: Value = serde_json::from_str(&record.5).unwrap();
    assert_eq!(params["name"], "ci");
    assert_eq!(params["token_id"], id.as_str());
    assert!(params.get("token").is_none());
    assert_eq!(record.6, Some(*create_seq));

    let res = owner
        .delete(server.url(&format!("/api/v1/tokens/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT seq, op, payload FROM change_log \
         WHERE entity_type = 'api_token' AND entity_id = ? ORDER BY seq",
    )
    .bind(&id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].1, "update");
    let payload: Value = serde_json::from_str(&rows[1].2).unwrap();
    assert!(payload["revoked_at"].is_string());
    let (revoked_at,): (Option<String>,) =
        sqlx::query_as("SELECT revoked_at FROM api_tokens WHERE id = ?")
            .bind(&id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(revoked_at.as_deref(), payload["revoked_at"].as_str());
    let record: (String, String, Option<i64>) = sqlx::query_as(
        "SELECT result, params, change_seq FROM audit_log \
         WHERE operation = 'DELETE /api/v1/tokens/{token_id}' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(record.0, "ok");
    assert_eq!(
        serde_json::from_str::<Value>(&record.1).unwrap()["token_id"],
        id.as_str()
    );
    assert_eq!(record.2, Some(rows[1].0));

    // Revoking again answers 204, records the call and changes nothing.
    let res = owner
        .delete(server.url(&format!("/api/v1/tokens/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let (changes,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM change_log WHERE entity_type = 'api_token'")
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(changes, 2);
    let (result, change_seq): (String, Option<i64>) = sqlx::query_as(
        "SELECT result, change_seq FROM audit_log \
         WHERE operation = 'DELETE /api/v1/tokens/{token_id}' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!((result.as_str(), change_seq), ("ok", None));

    // A human's list is a read: no record.
    let before = server.audit().await.len();
    list(&owner, &server).await;
    assert_eq!(server.audit().await.len(), before);
}

#[tokio::test]
async fn revoking_stops_the_token_on_the_next_call() {
    let server = start().await;
    let owner = server.owner().await;
    let created = create_ok(&owner, &server, "short-lived").await;
    let id = created["id"].as_str().unwrap();
    let secret = created["token"].as_str().unwrap();
    assert_eq!(me_status(&server, secret).await.0, 200);

    // The token revokes itself; the very next call is unauthenticated.
    let res = bearer(secret)
        .delete(server.url(&format!("/api/v1/tokens/{id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let (status, code, _) = problem(
        bearer(secret)
            .get(server.url("/api/v1/auth/me"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));

    // It stays listed, marked revoked.
    let page = list(&owner, &server).await;
    assert!(page["items"][0]["revoked_at"].is_string());

    // The bearer revoke was recorded on the api channel.
    let (channel,): (String,) = sqlx::query_as(
        "SELECT channel FROM audit_log WHERE operation = 'DELETE /api/v1/tokens/{token_id}' \
         ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(channel, "api");
}

#[tokio::test]
async fn expiry_is_validated_and_ends_the_token() {
    let server = start().await;
    let owner = server.owner().await;

    for (expires_at, what) in [
        (json!("2020-01-01T00:00:00Z"), "past"),
        (json!("tomorrow"), "malformed"),
    ] {
        let (status, code, _) = problem(
            create(
                &owner,
                &server,
                json!({ "name": "x", "expires_at": expires_at }),
            )
            .await,
        )
        .await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{what}"
        );
    }
    for name in [json!(""), json!("   "), json!("n".repeat(201))] {
        let (status, code, _) =
            problem(create(&owner, &server, json!({ "name": name })).await).await;
        assert_eq!((status, code.as_str()), (422, "validation_failed"));
    }
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM api_tokens")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "a refused create stores nothing");
    let errors: Vec<_> = server
        .audit()
        .await
        .into_iter()
        .filter(|(op, result, _)| op == "POST /api/v1/tokens" && result == "error")
        .collect();
    assert_eq!(errors.len(), 5, "each failed create is recorded");

    // A future expiry in another offset is stored in the fixed UTC form.
    let res = create(
        &owner,
        &server,
        json!({ "name": "dated", "expires_at": "2999-06-30T20:00:00+02:00" }),
    )
    .await;
    assert_eq!(res.status(), 201);
    let created: Value = res.json().await.unwrap();
    assert_eq!(created["expires_at"], "2999-06-30T18:00:00.000Z");

    // A token whose expiry has passed stops authenticating.
    let res = create(
        &owner,
        &server,
        json!({ "name": "soon", "expires_at": rfc3339_in(3600) }),
    )
    .await;
    assert_eq!(res.status(), 201);
    let soon: Value = res.json().await.unwrap();
    let secret = soon["token"].as_str().unwrap();
    assert_eq!(me_status(&server, secret).await.0, 200);
    sqlx::query("UPDATE api_tokens SET expires_at = ? WHERE id = ?")
        .bind(rfc3339_in(-1))
        .bind(soon["id"].as_str().unwrap())
        .execute(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(me_status(&server, secret).await.0, 401);
}

#[tokio::test]
async fn a_human_sees_and_revokes_only_their_own_tokens() {
    let server = start().await;
    let owner = server.owner().await;
    let mine = create_ok(&owner, &server, "owner's").await;

    // Any role may hold tokens, a viewer included.
    server
        .insert_user("viewer@example.com", "viewer password long", "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", "viewer password long")
        .await;
    let viewer_id = user_id(&viewer, &server).await;
    let theirs = create_ok(&viewer, &server, "viewer's").await;
    assert_eq!(theirs["user_id"], viewer_id.as_str());

    let page = list(&viewer, &server).await;
    let ids: Vec<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![theirs["id"].as_str().unwrap()]);
    let page = list(&owner, &server).await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["id"], mine["id"]);

    // Another user's token is not found, and stays working.
    let (status, code, _) = problem(
        viewer
            .delete(server.url(&format!("/api/v1/tokens/{}", mine["id"].as_str().unwrap())))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
    assert_eq!(
        me_status(&server, mine["token"].as_str().unwrap()).await.0,
        200
    );
    let (result, detail): (String, Option<String>) = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log \
         WHERE operation = 'DELETE /api/v1/tokens/{token_id}' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(
        (result.as_str(), detail.as_deref()),
        ("error", Some("not_found"))
    );

    // Unknown and malformed ids are not found too.
    for id in ["tok_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "not-a-token"] {
        let (status, code, _) = problem(
            owner
                .delete(server.url(&format!("/api/v1/tokens/{id}")))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{id}");
    }
}

#[tokio::test]
async fn the_list_pages_newest_first() {
    let server = start().await;
    let owner = server.owner().await;
    let mut ids = Vec::new();
    for n in 0..3 {
        ids.push(
            create_ok(&owner, &server, &format!("t{n}")).await["id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    let first: Value = owner
        .get(server.url("/api/v1/tokens?limit=2"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let got: Vec<&str> = first["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(got, vec![ids[2].as_str(), ids[1].as_str()]);
    let cursor = first["next_cursor"].as_str().unwrap();
    let second: Value = owner
        .get(server.url(&format!("/api/v1/tokens?limit=2&cursor={cursor}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert_eq!(second["items"][0]["id"], ids[0].as_str());
    assert_eq!(second["next_cursor"], Value::Null);

    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/tokens?limit=0"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn idempotency_key_replays_the_same_token() {
    let server = start().await;
    let owner = server.owner().await;
    let send = |name: &'static str| {
        owner
            .post(server.url("/api/v1/tokens"))
            .header("Idempotency-Key", "laptop-1")
            .json(&json!({ "name": name }))
            .send()
    };
    let first = send("laptop").await.unwrap();
    assert_eq!(first.status(), 201);
    let first: Value = first.json().await.unwrap();
    let again = send("laptop").await.unwrap();
    assert_eq!(again.status(), 201);
    let again: Value = again.json().await.unwrap();
    assert_eq!(again["id"], first["id"]);
    assert!(
        again["token"] == first["token"],
        "the replay returns the same secret"
    );
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM api_tokens")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);

    // The replay is recorded, with no change-log link.
    let records: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT result, change_seq FROM audit_log WHERE operation = 'POST /api/v1/tokens' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(records.len(), 2);
    assert!(records[0].1.is_some());
    assert_eq!(records[1], ("ok".to_string(), None));

    // The same key with another body is a conflict.
    let (status, code, _) = problem(send("desktop").await.unwrap()).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn an_agent_is_refused_human_only_on_every_operation() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_id = user_id(&owner, &server).await;
    let mine = create_ok(&owner, &server, "owner's").await;
    let mine_id = mine["id"].as_str().unwrap();

    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({ "name": "helper", "model": "test-model", "owner_user_id": owner_id }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent_id = agent["id"].as_str().unwrap();
    let key: Value = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "tests")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let minted: Value = res.json().await.unwrap();
    let agent_client = bearer(minted["access_token"].as_str().unwrap());

    let calls = [
        (
            "GET /api/v1/tokens",
            agent_client.get(server.url("/api/v1/tokens")),
        ),
        (
            "POST /api/v1/tokens",
            agent_client
                .post(server.url("/api/v1/tokens"))
                .json(&json!({ "name": "escape hatch" })),
        ),
        (
            "DELETE /api/v1/tokens/{token_id}",
            agent_client.delete(server.url(&format!("/api/v1/tokens/{mine_id}"))),
        ),
    ];
    for (operation, request) in calls {
        let res = request
            .header("Quoro-Justification", "trying to get a token")
            .send()
            .await
            .unwrap();
        let (status, code, reason) = problem(res).await;
        assert_eq!((status, code.as_str()), (403, "forbidden"), "{operation}");
        assert_eq!(reason.as_deref(), Some("human_only"), "{operation}");
        let record: (String, String, String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT actor_type, actor_id, result, result_detail, agent_model FROM audit_log \
             WHERE operation = ? ORDER BY seq DESC LIMIT 1",
        )
        .bind(operation)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        assert_eq!(record.0, "agent", "{operation}");
        assert_eq!(record.1, agent_id, "{operation}");
        assert_eq!(record.2, "refused", "{operation}");
        assert_eq!(record.3.as_deref(), Some("human_only"), "{operation}");
        assert_eq!(record.4.as_deref(), Some("test-model"), "{operation}");
    }

    // Nothing was created or revoked.
    let (count, revoked): (i64, i64) =
        sqlx::query_as("SELECT COUNT(*), COUNT(revoked_at) FROM api_tokens")
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!((count, revoked), (1, 0));
}
