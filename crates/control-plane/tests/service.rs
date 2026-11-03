//! The service layer called directly, the way an MCP adapter calls it: the
//! same operations as HTTP, with the same audit records and results.

mod common;

use common::start;
use quoro_control_plane::api::apps::ListAppsQuery;
use quoro_control_plane::audit::Channel;
use quoro_control_plane::auth::{Credential, Identity, Principal, UserIdentity};
use quoro_control_plane::request::CallCtx;
use quoro_types::Role;
use serde_json::{Value, json};

/// The owner's identity, the way an adapter builds it for an authenticated
/// human.
async fn owner_identity(server: &common::Server) -> Identity {
    let (user_id, email, name, role): (String, String, String, String) =
        sqlx::query_as("SELECT id, email, name, role FROM users LIMIT 1")
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    Identity {
        principal: Principal::User(UserIdentity {
            user_id,
            email,
            name,
            role: Role::parse(&role).unwrap(),
        }),
        credential: Credential::ApiToken {
            token_id: "test".to_string(),
        },
    }
}

/// A running control plane with the owner logged in, one project and one app.
async fn setup() -> (common::Server, reqwest::Client, String) {
    let server = start().await;
    let owner = server.owner().await;
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "Demo" }))
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
    let project_id = project["id"].as_str().unwrap().to_string();
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "web",
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
    let app_id = app["id"].as_str().unwrap().to_string();
    (server, owner, app_id)
}

/// The `(channel, params)` of the newest audit record with `operation`.
async fn newest_audit(server: &common::Server, operation: &str) -> (String, Value) {
    let (channel, params): (String, String) = sqlx::query_as(
        "SELECT channel, params FROM audit_log WHERE operation = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(operation)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    let params: Value = serde_json::from_str(&params).unwrap();
    (channel, params)
}

#[tokio::test]
async fn an_mcp_restart_runs_through_the_service_and_is_audited_as_mcp() {
    let (server, _owner, app_id) = setup().await;
    let identity = owner_identity(&server).await;
    let ctx = CallCtx::with_channel(
        identity,
        Channel::Mcp,
        "req_mcp_restart".to_string(),
        "restart_app",
        Some("justification text".to_string()),
        None,
    );
    let app = quoro_control_plane::service::apps::restart_app(&server.state, &ctx, &app_id)
        .await
        .unwrap();
    assert_eq!(app.id, app_id);
    assert_eq!(app.restart_generation, 1);

    let (channel, params) = newest_audit(&server, "restart_app").await;
    assert_eq!(channel, "mcp");
    assert_eq!(params, json!({ "app_id": app_id }));
}

#[tokio::test]
async fn the_http_restart_writes_the_same_params_as_the_service() {
    let (server, owner, app_id) = setup().await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/restart")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "restart: {}", res.text().await.unwrap());

    let (channel, params) = newest_audit(&server, "POST /api/v1/apps/{app_id}/restart").await;
    assert_eq!(channel, "panel");
    assert_eq!(params, json!({ "app_id": app_id }));
}

#[tokio::test]
async fn a_service_list_returns_the_http_items() {
    let (server, owner, _app_id) = setup().await;
    let http: Value = owner
        .get(server.url("/api/v1/apps"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let http_ids: Vec<String> = http["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().to_string())
        .collect();

    let identity = owner_identity(&server).await;
    let ctx = CallCtx::with_channel(
        identity,
        Channel::Mcp,
        "req_mcp_list".to_string(),
        "list_apps",
        Some("justification text".to_string()),
        None,
    );
    let query = ListAppsQuery {
        project_id: None,
        limit: None,
        cursor: None,
    };
    let list = quoro_control_plane::service::apps::list_apps(&server.state, &ctx, &query)
        .await
        .unwrap();
    let service_ids: Vec<String> = list.items.iter().map(|app| app.id.clone()).collect();
    assert_eq!(service_ids, http_ids);
}
