//! Setup token, setup, login, session, logout and `GET /auth/me` over HTTP
//! (D-0054, permission-model.md section 2, audit-log-format.md section 3).

mod common;

use common::{OWNER_EMAIL, OWNER_PASSWORD, problem};
use serde_json::{Value, json};

fn owner_body() -> Value {
    json!({ "email": OWNER_EMAIL, "name": "Owner", "password": OWNER_PASSWORD })
}

#[tokio::test]
async fn setup_requires_a_valid_unexpired_single_use_token() {
    let server = common::start().await;
    let client = server.client();
    let url = server.url("/api/v1/setup");

    // No token.
    let res = client.post(&url).json(&owner_body()).send().await.unwrap();
    assert_eq!(problem(res).await, (401, "unauthenticated".into(), None));

    // A token exists, but the caller presents another one.
    let token = server.setup_token();
    let res = client
        .post(&url)
        .header("Quoro-Setup-Token", "qst_wrong")
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401);

    // An expired token: rewrite its expiry into the past.
    let path = server.data_dir.join("setup-token.json");
    let mut stored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let fresh_expiry = stored["expires_at"].clone();
    stored["expires_at"] = json!("2000-01-01T00:00:00.000Z");
    std::fs::write(&path, serde_json::to_vec(&stored).unwrap()).unwrap();
    let res = client
        .post(&url)
        .header("Quoro-Setup-Token", &token)
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401, "expired token");
    stored["expires_at"] = fresh_expiry;
    std::fs::write(&path, serde_json::to_vec(&stored).unwrap()).unwrap();

    // Valid token: 201, the user without any password material.
    let res = client
        .post(&url)
        .header("Quoro-Setup-Token", &token)
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let user: Value = res.json().await.unwrap();
    assert_eq!(user["role"], "owner");
    assert_eq!(user["email"], OWNER_EMAIL);
    assert!(user.get("password").is_none() && user.get("password_hash").is_none());
    assert!(!path.exists(), "the token file is removed on success");

    // The consumed token, and a fresh one, both meet 409 now.
    for presented in [token, server.setup_token()] {
        let res = client
            .post(&url)
            .header("Quoro-Setup-Token", presented)
            .json(&owner_body())
            .send()
            .await
            .unwrap();
        assert_eq!(problem(res).await, (409, "conflict".into(), None));
    }
}

#[tokio::test]
async fn setup_validates_the_body() {
    let server = common::start().await;
    let token = server.setup_token();
    for body in [
        json!({ "email": OWNER_EMAIL, "name": "Owner", "password": "short" }),
        json!({ "email": "not-an-email", "name": "Owner", "password": OWNER_PASSWORD }),
        json!({ "email": OWNER_EMAIL, "name": "  ", "password": OWNER_PASSWORD }),
        json!({ "email": OWNER_EMAIL }),
    ] {
        let res = server
            .client()
            .post(server.url("/api/v1/setup"))
            .header("Quoro-Setup-Token", &token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(problem(res).await.1, "validation_failed", "{body}");
    }
    // Nothing was created and the token still works.
    server.owner().await;
}

#[tokio::test]
async fn setup_is_idempotent_under_idempotency_key() {
    let server = common::start().await;
    let token = server.setup_token();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let res = server
            .client()
            .post(server.url("/api/v1/setup"))
            .header("Quoro-Setup-Token", &token)
            .header("Idempotency-Key", "first-run")
            .json(&owner_body())
            .send()
            .await
            .unwrap();
        let status = res.status().as_u16();
        let body: Value = res.json().await.unwrap();
        ids.push((status, body["id"].clone()));
    }
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM users")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    // The replay returns the original result, not the 409 a new setup gets.
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[0].0, 201);

    // Both calls are audited `ok` as the owner (IdempotencyKey: D-0031 records
    // the call either way); only the first points at a change, and no record
    // holds the password.
    let audit: Vec<(String, String, Option<i64>, String)> = sqlx::query_as(
        "SELECT result, actor_id, change_seq, params FROM audit_log WHERE operation = 'POST /api/v1/setup' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(audit.len(), 2, "{audit:?}");
    for (result, actor, _, params) in &audit {
        assert_eq!(result, "ok");
        assert_eq!(Some(actor.as_str()), ids[0].1.as_str());
        assert!(!params.contains(OWNER_PASSWORD));
    }
    assert!(audit[0].2.is_some() && audit[1].2.is_none());

    // A replay with a different token is not the same call.
    let res = server
        .client()
        .post(server.url("/api/v1/setup"))
        .header("Quoro-Setup-Token", "qst_other")
        .header("Idempotency-Key", "first-run")
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 409);
}

/// `GET /api/v1/setup` from a plain client: no cookie jar, no bearer token, no
/// setup token.
async fn read_setup(server: &common::Server) -> Value {
    let res = reqwest::get(server.url("/api/v1/setup")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["content-type"], "application/json");
    let id = res.headers()["quoro-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        quoro_types::is_id(quoro_types::Prefix::Request, &id),
        "{id}"
    );
    res.json().await.unwrap()
}

/// ADR 0293: `GET /api/v1/setup` answers one public bit, whether setup is
/// still open, to a caller with no credential, and nothing else.
#[tokio::test]
async fn get_setup_tells_anyone_whether_setup_is_still_open_and_nothing_else() {
    let server = common::start().await;
    let read = || read_setup(&server);

    // No user yet: setup is open. The body is exactly the one bit.
    assert_eq!(read().await, json!({ "required": true }));

    // An issued setup token changes nothing the read says.
    let token = server.setup_token();
    assert_eq!(read().await, json!({ "required": true }));

    // Once the owner exists, setup is closed, for anyone who asks.
    let res = server
        .client()
        .post(server.url("/api/v1/setup"))
        .header("Quoro-Setup-Token", &token)
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    assert_eq!(read().await, json!({ "required": false }));

    // A signed-in owner reads the same bit, no more.
    let owner = server.client();
    server.login(&owner, OWNER_EMAIL, OWNER_PASSWORD).await;
    let body: Value = owner
        .get(server.url("/api/v1/setup"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body, json!({ "required": false }));

    // POST keeps its own answer: a second setup is still `conflict`.
    let res = server
        .client()
        .post(server.url("/api/v1/setup"))
        .header("Quoro-Setup-Token", server.setup_token())
        .json(&owner_body())
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await, (409, "conflict".into(), None));

    // A read is not audited: no record names the GET.
    assert!(
        server
            .audit()
            .await
            .iter()
            .all(|(operation, _, _)| !operation.starts_with("GET ")),
        "{:?}",
        server.audit().await
    );
}

#[tokio::test]
async fn login_session_me_and_logout() {
    let server = common::start().await;
    let client = server.owner().await;

    let me: Value = client
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["type"], "user");
    assert_eq!(me["user"]["role"], "owner");
    assert_eq!(me["grants"], json!([]));

    // No cookie: 401 problem.
    let res = server
        .client()
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await, (401, "unauthenticated".into(), None));

    // Logout clears the session server side.
    let res = client
        .post(server.url("/api/v1/auth/logout"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 204);
    let cookie = res.headers()["set-cookie"].to_str().unwrap().to_string();
    assert!(cookie.contains("Max-Age=0"), "{cookie}");
    let (sessions,): (i64,) = sqlx::query_as("SELECT count(*) FROM sessions")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(sessions, 0);
    let res = client
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn the_session_cookie_is_http_only_strict_and_stored_hashed() {
    let server = common::start().await;
    server.owner().await;
    let res = server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": OWNER_EMAIL, "password": OWNER_PASSWORD }))
        .send()
        .await
        .unwrap();
    let cookie = res.headers()["set-cookie"].to_str().unwrap().to_string();
    assert!(cookie.starts_with("quoro_session="));
    assert!(
        cookie.contains("HttpOnly")
            && cookie.contains("SameSite=Strict")
            && cookie.contains("Max-Age=86400")
    );
    let secret = cookie
        .trim_start_matches("quoro_session=")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let hashes: Vec<(String,)> = sqlx::query_as("SELECT token_hash FROM sessions")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    assert!(
        hashes.iter().all(|(h,)| h != &secret),
        "the secret itself is never stored"
    );
    assert!(
        hashes
            .iter()
            .any(|(h,)| h == &quoro_control_plane::store::sha256_hex(secret.as_bytes()))
    );
}

#[tokio::test]
async fn wrong_password_unknown_email_and_disabled_users_are_refused() {
    let server = common::start().await;
    let client = server.owner().await;
    for (email, password) in [
        (OWNER_EMAIL, "wrong password!"),
        ("nobody@example.com", OWNER_PASSWORD),
    ] {
        let res = server
            .client()
            .post(server.url("/api/v1/auth/login"))
            .json(&json!({ "email": email, "password": password }))
            .send()
            .await
            .unwrap();
        assert_eq!(problem(res).await, (401, "unauthenticated".into(), None));
    }

    // Disabling the user ends their existing session and blocks new logins.
    sqlx::query("UPDATE users SET disabled = 1")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        client
            .get(server.url("/api/v1/auth/me"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let res = server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": OWNER_EMAIL, "password": OWNER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn an_expired_session_is_unauthenticated() {
    let server = common::start().await;
    let client = server.owner().await;
    sqlx::query("UPDATE sessions SET expires_at = '2000-01-01T00:00:00.000Z'")
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let res = client
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.1, "unauthenticated");
}

#[tokio::test]
async fn setup_login_and_logout_are_audited_without_secrets() {
    let server = common::start().await;
    let client = server.owner().await;
    client
        .post(server.url("/api/v1/auth/logout"))
        .send()
        .await
        .unwrap();
    // A failed login has no identity and leaves no record.
    server
        .client()
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": OWNER_EMAIL, "password": "wrong password!" }))
        .send()
        .await
        .unwrap();

    let audit = server.audit().await;
    let ops: Vec<(&str, &str)> = audit
        .iter()
        .map(|(o, r, _)| (o.as_str(), r.as_str()))
        .collect();
    assert_eq!(
        ops,
        vec![
            ("POST /api/v1/setup", "ok"),
            ("POST /api/v1/auth/login", "ok"),
            ("POST /api/v1/auth/logout", "ok"),
        ]
    );
    let params: Vec<(String,)> = sqlx::query_as("SELECT params FROM audit_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    assert!(params.iter().all(|(p,)| !p.contains(OWNER_PASSWORD)));

    // The setup record points at the change that created the user, and both
    // chains verify.
    let (change_seq,): (Option<i64>,) =
        sqlx::query_as("SELECT change_seq FROM audit_log WHERE operation = 'POST /api/v1/setup'")
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    let (entity,): (String,) = sqlx::query_as("SELECT entity_type FROM change_log WHERE seq = ?")
        .bind(change_seq.unwrap())
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(entity, "user");
    let mut conn = server.state.store.pool().acquire().await.unwrap();
    assert_eq!(
        quoro_control_plane::changelog::verify(&mut conn)
            .await
            .unwrap(),
        None
    );
}
