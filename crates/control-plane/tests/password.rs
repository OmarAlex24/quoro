//! `POST /api/v1/auth/password`: the self-service password change over real
//! HTTP (ADR 0135, `audit-log-format.md` section 3.4).

mod common;

use common::{OWNER_EMAIL, OWNER_PASSWORD, Server, problem, start};
use quoro_control_plane::store::{now_rfc3339, sha256_hex};
use serde_json::{Value, json};

const OPERATION: &str = "POST /api/v1/auth/password";
const NEW_PASSWORD: &str = "a brand new horse battery";

async fn change(
    client: &reqwest::Client,
    server: &Server,
    current: &str,
    new: &str,
) -> reqwest::Response {
    client
        .post(server.url("/api/v1/auth/password"))
        .json(&json!({ "current_password": current, "new_password": new }))
        .send()
        .await
        .unwrap()
}

async fn me_status(client: &reqwest::Client, server: &Server) -> u16 {
    client
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn owner_id(client: &reqwest::Client, server: &Server) -> String {
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

/// A personal API token for `user_id`, inserted the way the token API does.
async fn personal_token(server: &Server, user_id: &str) -> String {
    let secret = format!("tok_{}", "P".repeat(26));
    let id = quoro_types::new_id(quoro_types::Prefix::User);
    sqlx::query(
        "INSERT INTO api_tokens (id, user_id, name, token_hash, created_at) \
         VALUES (?, ?, 'password-test', ?, ?)",
    )
    .bind(id)
    .bind(user_id)
    .bind(sha256_hex(secret.as_bytes()))
    .bind(now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    secret
}

/// The password-change audit records, oldest first: (result, result_detail,
/// params).
async fn password_audit(server: &Server) -> Vec<(String, Option<String>, String)> {
    sqlx::query_as(
        "SELECT result, result_detail, params FROM audit_log WHERE operation = ? ORDER BY seq",
    )
    .bind(OPERATION)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn changes_the_password_and_ends_the_other_sessions_only() {
    let server = start().await;
    let session = server.owner().await;
    let user_id = owner_id(&session, &server).await;
    // A second panel session and a personal API token of the same user.
    let other_session = server.client();
    server
        .login(&other_session, OWNER_EMAIL, OWNER_PASSWORD)
        .await;
    let token = personal_token(&server, &user_id).await;

    let res = change(&session, &server, OWNER_PASSWORD, NEW_PASSWORD).await;
    assert_eq!(res.status(), 204, "{}", res.text().await.unwrap());

    // The calling session survives; the other one is gone.
    assert_eq!(me_status(&session, &server).await, 200);
    assert_eq!(me_status(&other_session, &server).await, 401);

    // The old password no longer signs in, the new one does.
    let old = server.client();
    let res = old
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": OWNER_EMAIL, "password": OWNER_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401);
    let fresh = server.client();
    server.login(&fresh, OWNER_EMAIL, NEW_PASSWORD).await;

    // Personal API tokens are not touched.
    let res = reqwest::Client::new()
        .get(server.url("/api/v1/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());

    // Exactly one `ok` record, redacted, linked to the change-log update.
    let audit = password_audit(&server).await;
    assert_eq!(audit.len(), 1, "{audit:?}");
    assert_eq!(audit[0].0, "ok");
    assert_eq!(audit[0].1, None);
    let params: Value = serde_json::from_str(&audit[0].2).unwrap();
    assert_eq!(
        params,
        json!({ "current_password": "[redacted]", "new_password": "[redacted]" })
    );

    let (change_seq,): (Option<i64>,) = sqlx::query_as(
        "SELECT change_seq FROM audit_log WHERE operation = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(OPERATION)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let change_seq = change_seq.expect("the ok record names its change-log entry");
    let (entity, op, payload): (String, String, String) =
        sqlx::query_as("SELECT entity_type, op, payload FROM change_log WHERE seq = ?")
            .bind(change_seq)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(entity, "user");
    assert_eq!(op, "update");
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["id"].as_str(), Some(user_id.as_str()));
    assert!(
        payload["password_hash"].as_str().is_some(),
        "the replicated row carries the new hash"
    );
    assert!(!payload.to_string().contains(NEW_PASSWORD));
}

#[tokio::test]
async fn a_wrong_current_password_is_refused_and_audited() {
    let server = start().await;
    let session = server.owner().await;

    let res = change(&session, &server, "not the current password", NEW_PASSWORD).await;
    assert_eq!(problem(res).await, (422, "validation_failed".into(), None));

    let audit = password_audit(&server).await;
    assert_eq!(audit.len(), 1, "{audit:?}");
    assert_eq!(audit[0].0, "refused");
    assert_eq!(audit[0].1.as_deref(), Some("wrong_password"));
    let params: Value = serde_json::from_str(&audit[0].2).unwrap();
    assert_eq!(
        params,
        json!({ "current_password": "[redacted]", "new_password": "[redacted]" })
    );
    // Nothing changed.
    let fresh = server.client();
    server.login(&fresh, OWNER_EMAIL, OWNER_PASSWORD).await;
}

#[tokio::test]
async fn a_short_or_unchanged_new_password_is_an_error() {
    let server = start().await;
    let session = server.owner().await;

    let res = change(&session, &server, OWNER_PASSWORD, "short").await;
    assert_eq!(problem(res).await, (422, "validation_failed".into(), None));
    let res = change(&session, &server, OWNER_PASSWORD, OWNER_PASSWORD).await;
    assert_eq!(problem(res).await, (422, "validation_failed".into(), None));

    let audit = password_audit(&server).await;
    assert_eq!(audit.len(), 2, "{audit:?}");
    for (result, detail, _) in &audit {
        assert_eq!(result, "error");
        assert_eq!(detail.as_deref(), Some("validation_failed"));
    }
    // The old password is still the one that works.
    let fresh = server.client();
    server.login(&fresh, OWNER_EMAIL, OWNER_PASSWORD).await;
}

#[tokio::test]
async fn only_the_session_cookie_changes_a_password() {
    let server = start().await;
    let session = server.owner().await;
    let user_id = owner_id(&session, &server).await;
    let token = personal_token(&server, &user_id).await;

    // A bearer header disqualifies the call even beside a valid cookie, and
    // the call never reaches the rate limiter.
    let res = session
        .post(server.url("/api/v1/auth/password"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({ "current_password": OWNER_PASSWORD, "new_password": NEW_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401);

    // A bearer token alone, and no credential at all, are both 401.
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/password"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({ "current_password": OWNER_PASSWORD, "new_password": NEW_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401);
    let res = change(&server.client(), &server, OWNER_PASSWORD, NEW_PASSWORD).await;
    assert_eq!(problem(res).await.0, 401);

    // None of those was attributed to an identity.
    assert!(password_audit(&server).await.is_empty());
}

#[tokio::test]
async fn attempts_are_rate_limited_per_user() {
    let server = start().await;
    let session = server.owner().await;

    for _ in 0..5 {
        let res = change(&session, &server, "wrong password", NEW_PASSWORD).await;
        assert_eq!(problem(res).await.0, 422);
    }
    let res = change(&session, &server, "wrong password", NEW_PASSWORD).await;
    assert_eq!(problem(res).await, (429, "rate_limited".into(), None));

    let audit = password_audit(&server).await;
    assert_eq!(audit.len(), 6, "{audit:?}");
    assert!(
        audit[..5]
            .iter()
            .all(|(result, detail, _)| result == "refused"
                && detail.as_deref() == Some("wrong_password"))
    );
    assert_eq!(audit[5].0, "refused");
    assert_eq!(audit[5].1.as_deref(), Some("rate_limited"));
}

#[tokio::test]
async fn a_viewer_changes_their_own_password() {
    let server = start().await;
    let owner = server.owner().await;
    let res = owner
        .post(server.url("/api/v1/users"))
        .json(&json!({
            "email": "viewer@example.com",
            "name": "Viewer",
            "role": "viewer",
            "password": "viewer-password",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());

    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", "viewer-password")
        .await;
    let res = change(&viewer, &server, "viewer-password", NEW_PASSWORD).await;
    assert_eq!(res.status(), 204, "{}", res.text().await.unwrap());

    let old = server.client();
    let res = old
        .post(server.url("/api/v1/auth/login"))
        .json(&json!({ "email": "viewer@example.com", "password": "viewer-password" }))
        .send()
        .await
        .unwrap();
    assert_eq!(problem(res).await.0, 401);
    let fresh = server.client();
    server
        .login(&fresh, "viewer@example.com", NEW_PASSWORD)
        .await;
}
