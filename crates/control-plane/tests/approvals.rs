//! `/api/v1/approvals*` and the just-in-time approval flow
//! (permission-model.md section 9, D-0030), over real HTTP.

mod common;

use common::{Server, problem, start};
use serde_json::{Value, json};

fn image() -> &'static str {
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000"
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
            "image": image(),
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

async fn create_grant(owner: &reqwest::Client, server: &Server, body: Value) -> Value {
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

fn grant_body(agent_id: &str, scopes: &[&str], selector: Value) -> Value {
    json!({
        "agent_id": agent_id,
        "scopes": scopes,
        "selector": selector,
        "ttl_s": 3600,
        "reason": "tests",
    })
}

async fn agent_delete(
    server: &Server,
    token: &str,
    app_id: &str,
    justification: Option<&str>,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .delete(server.url(&format!("/api/v1/apps/{app_id}")))
        .header("Authorization", format!("Bearer {token}"));
    if let Some(justification) = justification {
        request = request.header("Quoro-Justification", justification);
    }
    request.send().await.unwrap()
}

fn approval_body(scope: &str, app_id: &str, params: Value) -> Value {
    json!({
        "scope": scope,
        "target": { "type": "app", "id": app_id },
        "operation": "DELETE /api/v1/apps/{app_id}",
        "params": params,
        "justification": "the app is finished and its data must go",
    })
}

async fn agent_file(server: &Server, token: &str, body: &Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {token}"))
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn decide(
    owner: &reqwest::Client,
    server: &Server,
    approval_id: &str,
    approve: bool,
    note: Option<&str>,
) -> reqwest::Response {
    let path = if approve { "approve" } else { "deny" };
    let mut request = owner.post(server.url(&format!("/api/v1/approvals/{approval_id}/{path}")));
    if let Some(note) = note {
        request = request.json(&json!({ "note": note }));
    }
    request.send().await.unwrap()
}

async fn file_pending(server: &Server, token: &str, app_id: &str, params: Value) -> Value {
    let res = agent_file(server, token, &approval_body("app:delete", app_id, params)).await;
    assert_eq!(
        res.status(),
        201,
        "file approval: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

#[tokio::test]
async fn approve_issues_one_use_grant_and_the_delete_succeeds_once() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "deleter", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["app:read", "approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    // 1. The destructive call is refused with `approval_required`.
    let (status, code, _) =
        problem(agent_delete(&server, &token, &app_id, Some("cleanup")).await).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));
    let (result, detail): (String, Option<String>) = sqlx::query_as(
        "SELECT result, result_detail FROM audit_log \
         WHERE operation = 'DELETE /api/v1/apps/{app_id}' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(result, "refused");
    assert_eq!(detail.as_deref(), Some("approval_required"));

    // 2. The agent files a request. The header is absent, so the body's
    // justification is the one the call records.
    let approval = file_pending(&server, &token, &app_id, json!({ "app_id": app_id })).await;
    let approval_id = approval["id"].as_str().unwrap().to_string();
    assert_eq!(approval["status"], "pending");
    assert_eq!(approval["agent_id"], agent_id);
    assert_eq!(approval["scope"], "app:delete");
    assert_eq!(approval["target"]["type"], "app");
    assert_eq!(approval["target"]["id"], app_id);
    assert_eq!(
        approval["params_hash"].as_str().unwrap().len(),
        64,
        "sha-256 hex"
    );
    assert!(approval["grant_id"].is_null());

    // 3. An agent cannot decide its own request.
    let res = reqwest::Client::new()
        .post(server.url(&format!("/api/v1/approvals/{approval_id}/approve")))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));

    // 4. The owner approves; the request carries its grant.
    let res = decide(&owner, &server, &approval_id, true, Some("ok, remove it")).await;
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());
    let approved: Value = res.json().await.unwrap();
    assert_eq!(approved["status"], "approved");
    assert_eq!(approved["decided_by"], owner_user_id);
    assert_eq!(approved["decision_note"], "ok, remove it");
    let grant_id = approved["grant_id"].as_str().unwrap().to_string();

    let grant: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(grant["kind"], "approval");
    assert_eq!(grant["max_uses"], 1);
    assert_eq!(grant["uses"], 0);
    assert_eq!(grant["on_behalf_of"], owner_user_id);
    assert_eq!(grant["approval_id"], approval_id);
    assert_eq!(grant["agent_id"], agent_id);

    // 5. The same call now runs, and the audit record names the approval and
    // the human behind it.
    let res = agent_delete(&server, &token, &app_id, Some("approved cleanup")).await;
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());

    #[allow(clippy::type_complexity)]
    let row: (Option<String>, Option<String>, Option<String>, String) = sqlx::query_as(
        "SELECT approval_id, grant_id, on_behalf_of, result FROM audit_log \
         WHERE operation = 'DELETE /api/v1/apps/{app_id}' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some(approval_id.as_str()));
    assert_eq!(row.1.as_deref(), Some(grant_id.as_str()));
    assert_eq!(row.2.as_deref(), Some(owner_user_id.as_str()));
    assert_eq!(row.3, "ok");

    // 6. One use only: the grant is exhausted.
    let grant: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(grant["uses"], 1);
    assert_eq!(grant["status"], "exhausted");

    let second_app = create_app(&owner, &server, &project_id, "api").await;
    let (status, code, _) =
        problem(agent_delete(&server, &token, &second_app, Some("cleanup")).await).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));
}

#[tokio::test]
async fn a_different_params_hash_is_a_different_call() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "deleter", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    // Filed and approved for different params than the call sends.
    let approval = file_pending(
        &server,
        &token,
        &app_id,
        json!({ "app_id": app_id, "force": true }),
    )
    .await;
    let approval_id = approval["id"].as_str().unwrap();
    let res = decide(&owner, &server, approval_id, true, None).await;
    assert_eq!(res.status(), 200, "approve: {}", res.text().await.unwrap());

    let (status, code, _) =
        problem(agent_delete(&server, &token, &app_id, Some("cleanup")).await).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));
}

#[tokio::test]
async fn deny_creates_no_grant_and_a_second_decision_conflicts() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "deleter", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    let approval = file_pending(&server, &token, &app_id, json!({ "app_id": app_id })).await;
    let approval_id = approval["id"].as_str().unwrap().to_string();
    let res = decide(&owner, &server, &approval_id, false, Some("not now")).await;
    assert_eq!(res.status(), 200, "deny: {}", res.text().await.unwrap());
    let denied: Value = res.json().await.unwrap();
    assert_eq!(denied["status"], "denied");
    assert_eq!(denied["decided_by"], owner_user_id);
    assert!(denied["grant_id"].is_null());

    let (status, code, _) =
        problem(agent_delete(&server, &token, &app_id, Some("cleanup")).await).await;
    assert_eq!((status, code.as_str()), (403, "approval_required"));

    let (status, code) = {
        let res = decide(&owner, &server, &approval_id, true, None).await;
        let (status, code, _) = problem(res).await;
        (status, code)
    };
    assert_eq!((status, code.as_str()), (409, "conflict"));
}

#[tokio::test]
async fn an_expired_request_cannot_be_approved_and_the_sweeper_marks_it() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "deleter", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    let approval = file_pending(&server, &token, &app_id, json!({ "app_id": app_id })).await;
    let approval_id = approval["id"].as_str().unwrap().to_string();
    sqlx::query("UPDATE approvals SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?")
        .bind(&approval_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    let (status, code, _) = problem(decide(&owner, &server, &approval_id, true, None).await).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    let expired = quoro_control_plane::service::approvals::expire_due(&server.state)
        .await
        .unwrap();
    assert_eq!(expired, 1);
    let status: (String,) = sqlx::query_as("SELECT status FROM approvals WHERE id = ?")
        .bind(&approval_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(status.0, "expired");
    let events: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM events WHERE type = 'approval.expired' \
         AND json_extract(subject, '$.id') = ?",
    )
    .bind(&approval_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(events.0, 1);
}

#[tokio::test]
async fn validation_refuses_what_the_contract_refuses() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "deleter", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    // A non-destructive scope needs no approval.
    let (status, code, _) = problem(
        agent_file(
            &server,
            &token,
            &approval_body("app:read", &app_id, json!({ "app_id": app_id })),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // The justification is mandatory and non-empty.
    let mut body = approval_body("app:delete", &app_id, json!({ "app_id": app_id }));
    body["justification"] = json!("");
    let (status, code, _) = problem(agent_file(&server, &token, &body).await).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // An unknown target matches no selector: authorization runs before
    // validation, so the agent learns nothing about what exists.
    let missing = quoro_types::new_id(quoro_types::Prefix::Approval).replacen("apr_", "app_", 1);
    let (status, code, _) = problem(
        agent_file(
            &server,
            &token,
            &approval_body("app:delete", &missing, json!({ "app_id": missing })),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));

    // A human holding `approval:create` is told a human needs no approval.
    let res = owner
        .post(server.url("/api/v1/approvals"))
        .json(&approval_body(
            "app:delete",
            &app_id,
            json!({ "app_id": app_id }),
        ))
        .send()
        .await
        .unwrap();
    let (status, code, _) = problem(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

#[tokio::test]
async fn list_filters_approvals_by_the_agents_selector() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let demo = create_project(&owner, &server, "demo").await;
    let other = create_project(&owner, &server, "other").await;
    let demo_app = create_app(&owner, &server, &demo, "web").await;
    let other_app = create_app(&owner, &server, &other, "worker").await;

    let reader_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &reader_id,
            &["approval:read", "approval:create"],
            json!({ "projects": ["demo"] }),
        ),
    )
    .await;
    let reader_token = token_for(&owner, &server, &reader_id).await;

    let wider_id = create_agent(&owner, &server, "wider", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &wider_id,
            &["approval:create"],
            json!({ "projects": ["other"] }),
        ),
    )
    .await;
    let wider_token = token_for(&owner, &server, &wider_id).await;

    let demo_approval = file_pending(
        &server,
        &reader_token,
        &demo_app,
        json!({ "app_id": demo_app }),
    )
    .await;
    let other_approval = file_pending(
        &server,
        &wider_token,
        &other_app,
        json!({ "app_id": other_app }),
    )
    .await;

    let res = reqwest::Client::new()
        .get(server.url("/api/v1/approvals"))
        .header("Authorization", format!("Bearer {reader_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "list: {}", res.text().await.unwrap());
    let list: Value = res.json().await.unwrap();
    let ids: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&demo_approval["id"].as_str().unwrap()));
    assert!(!ids.contains(&other_approval["id"].as_str().unwrap()));

    // The owner sees both.
    let list: Value = owner
        .get(server.url("/api/v1/approvals"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 2);
}
