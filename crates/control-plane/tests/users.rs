//! `GET /api/v1/users` over real HTTP: every role may read humans, an agent is
//! refused with `human_only`, paging works and no credential is ever returned.

mod common;

use common::replay;
use common::{Server, problem, start};
use serde_json::{Value, json};

async fn create_agent(owner: &reqwest::Client, server: &Server, owner_user_id: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "reader",
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

async fn agent_token(owner: &reqwest::Client, server: &Server, agent_id: &str) -> String {
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

async fn list(client: &reqwest::Client, server: &Server) -> Value {
    let res = client
        .get(server.url("/api/v1/users"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        200,
        "list users: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

#[tokio::test]
async fn every_role_lists_users_and_no_password_is_returned() {
    let server = start().await;
    server.owner().await;
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
        let body = list(&client, &server).await;
        let items = body["items"].as_array().unwrap();
        let emails: Vec<&str> = items
            .iter()
            .map(|user| user["email"].as_str().unwrap())
            .collect();
        assert!(emails.contains(&"operator@example.com"), "{emails:?}");
        assert!(emails.contains(&"viewer@example.com"), "{emails:?}");
        for user in items {
            assert!(user["role"].as_str().is_some());
            assert!(user.get("password").is_none(), "{user}");
            assert!(user.get("password_hash").is_none(), "{user}");
        }
    }
}

#[tokio::test]
async fn an_agent_cannot_list_users() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let token = agent_token(&owner, &server, &agent_id).await;

    let (status, code, reason) = problem(
        reqwest::Client::new()
            .get(server.url("/api/v1/users"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
}

#[tokio::test]
async fn users_page_with_a_cursor() {
    let server = start().await;
    let owner = server.owner().await;
    let mut expected = vec![owner_id(&owner, &server).await];
    for name in ["a", "b", "c"] {
        expected.push(
            server
                .insert_user(&format!("{name}@example.com"), "some-password", "viewer")
                .await,
        );
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut request = owner
            .get(server.url("/api/v1/users"))
            .query(&[("limit", "1")]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let res = request.send().await.unwrap();
        assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
        let body: Value = res.json().await.unwrap();
        for user in body["items"].as_array().unwrap() {
            seen.push(user["id"].as_str().unwrap().to_string());
        }
        match body["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    expected.sort();
    seen.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn a_bad_limit_is_a_validation_error() {
    let server = start().await;
    let owner = server.owner().await;
    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/users"))
            .query(&[("limit", "0")])
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

// ---------------------------------------------------------------------------
// User management: POST/PATCH/DELETE /api/v1/users
// ---------------------------------------------------------------------------

async fn create_user(
    client: &reqwest::Client,
    server: &Server,
    email: &str,
    name: &str,
    role: &str,
    password: &str,
) -> Value {
    let res = client
        .post(server.url("/api/v1/users"))
        .json(&json!({
            "email": email,
            "name": name,
            "role": role,
            "password": password,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create user: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

async fn patch_user(
    client: &reqwest::Client,
    server: &Server,
    user_id: &str,
    body: Value,
) -> reqwest::Response {
    client
        .patch(server.url(&format!("/api/v1/users/{user_id}")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn delete_user(
    client: &reqwest::Client,
    server: &Server,
    user_id: &str,
) -> reqwest::Response {
    client
        .delete(server.url(&format!("/api/v1/users/{user_id}")))
        .send()
        .await
        .unwrap()
}

const OPERATOR_PASSWORD: &str = "operator-password";
const VIEWER_PASSWORD: &str = "viewer-password";

async fn create_viewer(client: &reqwest::Client, server: &Server) -> Value {
    create_user(
        client,
        server,
        "viewer@example.com",
        "Viewer",
        "viewer",
        VIEWER_PASSWORD,
    )
    .await
}

#[tokio::test]
async fn owner_creates_users_and_no_credential_is_returned() {
    let server = start().await;
    let owner = server.owner().await;

    for (email, name, role, password) in [
        (
            "operator@example.com",
            "Operator",
            "operator",
            OPERATOR_PASSWORD,
        ),
        ("viewer@example.com", "Viewer", "viewer", VIEWER_PASSWORD),
    ] {
        let user = create_user(&owner, &server, email, name, role, password).await;
        assert_eq!(user["email"].as_str(), Some(email));
        assert_eq!(user["name"].as_str(), Some(name));
        assert_eq!(user["role"].as_str(), Some(role));
        assert_eq!(user["disabled"].as_bool(), Some(false));
        assert!(user.get("password").is_none(), "{user}");
        assert!(user.get("password_hash").is_none(), "{user}");
        assert!(!user.to_string().contains(password), "{user}");
    }

    // The new viewer can log in and read users, but cannot manage them.
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", VIEWER_PASSWORD)
        .await;
    let body = list(&viewer, &server).await;
    assert!(
        body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|u| u["email"] == "viewer@example.com")
    );
    let (status, code, reason) = problem(
        viewer
            .post(server.url("/api/v1/users"))
            .json(&json!({
                "email": "new@example.com",
                "name": "New",
                "role": "viewer",
                "password": "a-long-enough-password",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    // An operator cannot create, update or delete users either.
    let operator = server.client();
    server
        .login(&operator, "operator@example.com", OPERATOR_PASSWORD)
        .await;
    let viewer_id = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["email"] == "viewer@example.com")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, code, reason) = problem(
        operator
            .post(server.url("/api/v1/users"))
            .json(&json!({
                "email": "other@example.com",
                "name": "Other",
                "role": "viewer",
                "password": "a-long-enough-password",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    let (status, code, _) =
        problem(patch_user(&operator, &server, &viewer_id, json!({ "name": "Renamed" })).await)
            .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    let (status, code, _) = problem(delete_user(&operator, &server, &viewer_id).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));

    // An agent is refused as a non-human before its grants are consulted.
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, &owner_user_id).await;
    let token = agent_token(&owner, &server, &agent_id).await;
    let (status, code, reason) = problem(
        reqwest::Client::new()
            .post(server.url("/api/v1/users"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "tests need a user")
            .json(&json!({
                "email": "agent-made@example.com",
                "name": "Agent Made",
                "role": "viewer",
                "password": "a-long-enough-password",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
    let (status, code, reason) = problem(
        reqwest::Client::new()
            .patch(server.url(&format!("/api/v1/users/{viewer_id}")))
            .header("Authorization", format!("Bearer {token}"))
            .header("Quoro-Justification", "tests need a rename")
            .json(&json!({ "name": "Renamed" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
}

#[tokio::test]
async fn create_validates_and_refuses_a_duplicate_email() {
    let server = start().await;
    let owner = server.owner().await;

    let duplicate = create_viewer(&owner, &server).await;
    assert!(!duplicate["id"].as_str().unwrap().is_empty());
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/users"))
            .json(&json!({
                "email": "viewer@example.com",
                "name": "Another",
                "role": "viewer",
                "password": "a-long-enough-password",
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    for body in [
        json!({ "email": "not-an-email", "name": "Bad", "role": "viewer", "password": "a-long-enough-password" }),
        json!({ "email": "bad-name@example.com", "name": "   ", "role": "viewer", "password": "a-long-enough-password" }),
        json!({ "email": "short@example.com", "name": "Short", "role": "viewer", "password": "too-short" }),
    ] {
        let (status, code, _) = problem(
            owner
                .post(server.url("/api/v1/users"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{body}"
        );
    }
}

#[tokio::test]
async fn a_promoted_viewer_gets_operator_powers_on_the_next_request() {
    let server = start().await;
    let owner = server.owner().await;
    let viewer = create_viewer(&owner, &server).await;
    let viewer_id = viewer["id"].as_str().unwrap();
    let viewer_client = server.client();
    server
        .login(&viewer_client, "viewer@example.com", VIEWER_PASSWORD)
        .await;

    let (status, code, _) = problem(
        viewer_client
            .post(server.url("/api/v1/projects"))
            .json(&json!({ "slug": "before", "name": "Before" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));

    let res = patch_user(&owner, &server, viewer_id, json!({ "role": "operator" })).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let res = viewer_client
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "after", "name": "After" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
}

#[tokio::test]
async fn the_last_enabled_owner_cannot_be_demoted_disabled_or_deleted() {
    let server = start().await;
    let owner = server.owner().await;
    let id = owner_id(&owner, &server).await;

    for body in [json!({ "role": "viewer" }), json!({ "disabled": true })] {
        let (status, code, _) = problem(patch_user(&owner, &server, &id, body).await).await;
        assert_eq!((status, code.as_str()), (409, "conflict"));
    }
    let (status, code, _) = problem(delete_user(&owner, &server, &id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // With a second enabled owner the demotion is allowed.
    create_user(
        &owner,
        &server,
        "second-owner@example.com",
        "Second Owner",
        "owner",
        "second-owner-password",
    )
    .await;
    let res = patch_user(&owner, &server, &id, json!({ "role": "operator" })).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
}

#[tokio::test]
async fn disabling_a_user_ends_their_session() {
    let server = start().await;
    let owner = server.owner().await;
    let viewer = create_viewer(&owner, &server).await;
    let viewer_id = viewer["id"].as_str().unwrap();
    let viewer_client = server.client();
    server
        .login(&viewer_client, "viewer@example.com", VIEWER_PASSWORD)
        .await;
    assert_eq!(
        viewer_client
            .get(server.url("/api/v1/users"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let res = patch_user(&owner, &server, viewer_id, json!({ "disabled": true })).await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let user: Value = res.json().await.unwrap();
    assert_eq!(user["disabled"].as_bool(), Some(true));

    let res = viewer_client
        .get(server.url("/api/v1/users"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "a disabled user's session is refused");
    let res = server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": "viewer@example.com", "password": VIEWER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "a disabled user cannot log in");
}

#[tokio::test]
async fn changing_a_password_ends_the_target_sessions() {
    let server = start().await;
    let owner = server.owner().await;
    let viewer = create_viewer(&owner, &server).await;
    let viewer_id = viewer["id"].as_str().unwrap();
    let viewer_client = server.client();
    server
        .login(&viewer_client, "viewer@example.com", VIEWER_PASSWORD)
        .await;

    let new_password = "a-brand-new-password";
    let res = patch_user(
        &owner,
        &server,
        viewer_id,
        json!({ "password": new_password }),
    )
    .await;
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());

    assert_eq!(
        viewer_client
            .get(server.url("/api/v1/users"))
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "the old session is gone"
    );
    let old = server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": "viewer@example.com", "password": VIEWER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(old.status(), 401, "the old password is refused");
    let fresh = server.client();
    server
        .login(&fresh, "viewer@example.com", new_password)
        .await;
}

/// Section 7.3 step 4 (ADR 0297): deleting a user removes its API tokens
/// by cascade, and each one gets its own `delete` entry. Replayed in `seq`
/// order into an empty database with foreign keys off, so no cascade runs,
/// the log drops the user's tokens, a revoked one included, and keeps the
/// owner's.
#[tokio::test]
async fn a_deleted_users_tokens_leave_the_log_with_it() {
    let server = start().await;
    let owner = server.owner().await;
    let res = owner
        .post(server.url("/api/v1/tokens"))
        .json(&json!({ "name": "owner token" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "create the owner's token");

    let operator = create_user(
        &owner,
        &server,
        "operator@example.com",
        "Operator",
        "operator",
        OPERATOR_PASSWORD,
    )
    .await;
    let operator_id = operator["id"].as_str().unwrap().to_string();
    let client = server.client();
    server
        .login(&client, "operator@example.com", OPERATOR_PASSWORD)
        .await;
    let mut token_ids = Vec::new();
    for name in ["kept", "revoked"] {
        let res = client
            .post(server.url("/api/v1/tokens"))
            .json(&json!({ "name": name }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "create the operator's token");
        let body: Value = res.json().await.unwrap();
        token_ids.push(body["id"].as_str().unwrap().to_string());
    }
    let res = client
        .delete(server.url(&format!("/api/v1/tokens/{}", token_ids[1])))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204, "revoke: {}", res.text().await.unwrap());

    let res = delete_user(&owner, &server, &operator_id).await;
    assert_eq!(res.status(), 204, "delete: {}", res.text().await.unwrap());

    let rebuilt = replay::rebuild(&server, &["user", "api_token"]).await;
    replay::assert_rebuilt(
        &server,
        &rebuilt,
        "users",
        &["id", "email", "name", "role", "disabled", "created_at"],
        1,
    )
    .await;
    replay::assert_rebuilt(
        &server,
        &rebuilt,
        "api_tokens",
        &[
            "id",
            "user_id",
            "name",
            "token_hash",
            "expires_at",
            "last_used_at",
            "created_at",
            "revoked_at",
        ],
        1,
    )
    .await;
}

#[tokio::test]
async fn deleting_a_user_requires_no_references_and_keeps_the_last_owner() {
    let server = start().await;
    let owner = server.owner().await;

    let viewer = create_viewer(&owner, &server).await;
    let viewer_id = viewer["id"].as_str().unwrap();
    let res = delete_user(&owner, &server, viewer_id).await;
    assert_eq!(res.status(), 204, "{}", res.text().await.unwrap());
    let res = server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": "viewer@example.com", "password": VIEWER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401, "a deleted user cannot log in");

    // An unknown or malformed id is not found.
    for id in ["usr_01JC9Z6Q5W8N3K4T7R2M1XHB0D", "nope"] {
        let (status, code, _) = problem(delete_user(&owner, &server, id).await).await;
        assert_eq!((status, code.as_str()), (404, "not_found"), "{id}");
    }

    // A user still referenced by an agent cannot be deleted.
    let operator = create_user(
        &owner,
        &server,
        "operator@example.com",
        "Operator",
        "operator",
        OPERATOR_PASSWORD,
    )
    .await;
    let operator_id = operator["id"].as_str().unwrap();
    create_agent(&owner, &server, operator_id).await;
    let (status, code, _) = problem(delete_user(&owner, &server, operator_id).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn idempotent_create_replays_or_conflicts() {
    let server = start().await;
    let owner = server.owner().await;
    let body = json!({
        "email": "replayed@example.com",
        "name": "Replayed",
        "role": "viewer",
        "password": "a-long-enough-password",
    });

    let first = owner
        .post(server.url("/api/v1/users"))
        .header("Idempotency-Key", "user-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201, "{}", first.text().await.unwrap());
    let first: Value = first.json().await.unwrap();

    let replay = owner
        .post(server.url("/api/v1/users"))
        .header("Idempotency-Key", "user-1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 201);
    let replay: Value = replay.json().await.unwrap();
    assert_eq!(replay["id"], first["id"]);

    let mut different = body.clone();
    different["password"] = json!("a-different-password");
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/users"))
            .header("Idempotency-Key", "user-1")
            .json(&different)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn a_create_audit_record_never_carries_the_password() {
    let server = start().await;
    let owner = server.owner().await;
    let password = "audit-plaintext-password-42";
    let user = create_user(
        &owner,
        &server,
        "audited@example.com",
        "Audited",
        "viewer",
        password,
    )
    .await;
    let user_id = user["id"].as_str().unwrap();

    let res = owner
        .get(server.url("/api/v1/audit"))
        .query(&[("target_id", user_id)])
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let text = res.text().await.unwrap();
    assert!(
        !text.contains(password),
        "the plaintext password appeared in the audit record"
    );
    let audit: Value = serde_json::from_str(&text).unwrap();
    let record = audit["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["operation"] == "POST /api/v1/users")
        .expect("a create record for the new user");
    assert_eq!(record["params"]["password"], "[redacted]");
    assert_eq!(record["params"]["email"], "audited@example.com");
}
