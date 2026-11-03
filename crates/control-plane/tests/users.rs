//! `GET /api/v1/users` over real HTTP: every role may read humans, an agent is
//! refused with `human_only`, paging works and no credential is ever returned.

mod common;

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
