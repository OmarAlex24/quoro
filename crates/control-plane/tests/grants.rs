//! `/api/v1/grants*` and the agent authorization paths they enable
//! (permission-model.md sections 6 and 13), over real HTTP.

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

/// Creates a key and mints a 5-minute access token from it.
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

async fn agent_get(server: &Server, token: &str, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(server.url(path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
}

async fn agent_restart(
    server: &Server,
    token: &str,
    app_id: &str,
    justification: Option<&str>,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(server.url(&format!("/api/v1/apps/{app_id}/restart")))
        .header("Authorization", format!("Bearer {token}"));
    if let Some(justification) = justification {
        request = request.header("Quoro-Justification", justification);
    }
    request.send().await.unwrap()
}

async fn audit_count(server: &Server, sql: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(sql)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    count
}

#[tokio::test]
async fn read_only_grant_authorizes_a_read_and_refuses_a_restart() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    let grant = create_grant(
        &owner,
        &server,
        grant_body(&agent_id, &["app:read"], json!({ "apps": [&app_id] })),
    )
    .await;
    assert_eq!(grant["status"], "active");
    assert_eq!(grant["uses"], 0);
    assert_eq!(grant["on_behalf_of"], owner_user_id);
    assert_eq!(grant["created_by_type"], "user");
    let token = token_for(&owner, &server, &agent_id).await;

    let res = agent_get(&server, &token, &format!("/api/v1/apps/{app_id}")).await;
    assert_eq!(res.status(), 200, "get app: {}", res.text().await.unwrap());

    let (status, code, reason) =
        problem(agent_restart(&server, &token, &app_id, Some("check it")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("scope_missing"));

    // The refusal is evidence: an agent row with the reason and the model.
    let row: (String, String, String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT actor_type, actor_id, result, result_detail, agent_model FROM audit_log \
         WHERE operation = 'POST /api/v1/apps/{app_id}/restart' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0, "agent");
    assert_eq!(row.1, agent_id);
    assert_eq!(row.2, "refused");
    assert_eq!(row.3.as_deref(), Some("scope_missing"));
    assert_eq!(row.4.as_deref(), Some("test-model"));
}

#[tokio::test]
async fn restart_grant_consumes_one_use_and_requires_a_justification() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "operator", &owner_user_id).await;
    let grant = create_grant(
        &owner,
        &server,
        grant_body(
            &agent_id,
            &["app:read", "app:restart"],
            json!({ "apps": [&app_id] }),
        ),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let token = token_for(&owner, &server, &agent_id).await;

    let res = agent_restart(&server, &token, &app_id, Some("the app is down")).await;
    assert_eq!(res.status(), 202, "restart: {}", res.text().await.unwrap());

    #[allow(clippy::type_complexity)]
    let row: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<i64>,
    ) = sqlx::query_as(
        "SELECT grant_id, on_behalf_of, agent_model, justification, result, change_seq \
         FROM audit_log WHERE operation = 'POST /api/v1/apps/{app_id}/restart' \
         ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some(grant_id.as_str()));
    assert_eq!(row.1.as_deref(), Some(owner_user_id.as_str()));
    assert_eq!(row.2.as_deref(), Some("test-model"));
    assert_eq!(row.3.as_deref(), Some("the app is down"));
    assert_eq!(row.4, "ok");
    assert!(row.5.is_some(), "an ok call records its change_seq");

    let read: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(read["uses"], 1);

    // A mutating agent call without a justification is validation_failed and
    // consumes nothing.
    let (status, code, _) = problem(agent_restart(&server, &token, &app_id, None).await).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let read: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(read["uses"], 1);
}

#[tokio::test]
async fn expired_revoked_mismatched_and_exhausted_grants_are_refused() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    // Row 3: expired.
    let expired_agent = create_agent(&owner, &server, "expired", &owner_user_id).await;
    let mut body = grant_body(
        &expired_agent,
        &["app:restart"],
        json!({ "apps": [&app_id] }),
    );
    body["ttl_s"] = json!(1);
    create_grant(&owner, &server, body).await;
    let expired_token = token_for(&owner, &server, &expired_agent).await;
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    let (status, code, reason) =
        problem(agent_restart(&server, &expired_token, &app_id, Some("expired")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("grant_expired"));

    // Row 4: revoked.
    let revoked_agent = create_agent(&owner, &server, "revoked", &owner_user_id).await;
    let revoked_grant = create_grant(
        &owner,
        &server,
        grant_body(
            &revoked_agent,
            &["app:restart"],
            json!({ "apps": [&app_id] }),
        ),
    )
    .await;
    let revoked_grant_id = revoked_grant["id"].as_str().unwrap();
    let res = owner
        .post(server.url(&format!("/api/v1/grants/{revoked_grant_id}/revoke")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "revoke: {}", res.text().await.unwrap());
    let revoked: Value = res.json().await.unwrap();
    assert_eq!(revoked["status"], "revoked");
    assert_eq!(revoked["revoked_by"], owner_user_id);
    let revoked_token = token_for(&owner, &server, &revoked_agent).await;
    let (status, code, reason) =
        problem(agent_restart(&server, &revoked_token, &app_id, Some("revoked")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("grant_revoked"));

    // Row 5: the selector names another project.
    let mismatch_agent = create_agent(&owner, &server, "mismatch", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(
            &mismatch_agent,
            &["app:restart"],
            json!({ "projects": ["other"] }),
        ),
    )
    .await;
    let mismatch_token = token_for(&owner, &server, &mismatch_agent).await;
    let (status, code, reason) =
        problem(agent_restart(&server, &mismatch_token, &app_id, Some("mismatch")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("selector_mismatch"));

    // Row 6: max_uses 1, one use spent.
    let exhausted_agent = create_agent(&owner, &server, "exhausted", &owner_user_id).await;
    let mut body = grant_body(
        &exhausted_agent,
        &["app:restart"],
        json!({ "apps": [&app_id] }),
    );
    body["max_uses"] = json!(1);
    let grant = create_grant(&owner, &server, body).await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let exhausted_token = token_for(&owner, &server, &exhausted_agent).await;
    let res = agent_restart(&server, &exhausted_token, &app_id, Some("first use")).await;
    assert_eq!(
        res.status(),
        202,
        "first use: {}",
        res.text().await.unwrap()
    );
    let (status, code, reason) =
        problem(agent_restart(&server, &exhausted_token, &app_id, Some("second use")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("grant_exhausted"));

    let read: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(read["status"], "exhausted");
    assert_eq!(read["uses"], 1);

    let events = audit_count(
        &server,
        "SELECT count(*) FROM events WHERE type = 'grant.expired' \
         AND json_extract(data, '$.reason') = 'max_uses'",
    )
    .await;
    assert!(events >= 1, "grant.expired(max_uses) was emitted");
}

#[tokio::test]
async fn grant_creation_refuses_what_the_contract_forbids() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let agent_id = create_agent(&owner, &server, "target", &owner_user_id).await;
    let selector = json!({ "all": true });

    for body in [
        grant_body(&agent_id, &["app:delete"], selector.clone()),
        grant_body(&agent_id, &["grant:write"], selector.clone()),
        grant_body(&agent_id, &["app:*"], selector.clone()),
        json!({
            "agent_id": agent_id,
            "scopes": ["app:read"],
            "selector": selector,
            "reason": "no expiry",
        }),
        json!({
            "agent_id": agent_id,
            "scopes": ["app:read"],
            "selector": selector,
            "expires_at": "2020-01-01T00:00:00Z",
            "reason": "in the past",
        }),
    ] {
        let (status, code, _) = problem(
            owner
                .post(server.url("/api/v1/grants"))
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

    // Row 10: an agent can never reach grant:write.
    let token = token_for(&owner, &server, &agent_id).await;
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/grants"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&grant_body(
            &agent_id,
            &["app:read"],
            json!({ "apps": ["app_01JC9Z6Q5W8N3K4T7R2M1XHB0D"] }),
        ))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
}

#[tokio::test]
async fn disabled_agent_is_refused_and_me_lists_active_grants() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "sleeper", &owner_user_id).await;
    let grant = create_grant(
        &owner,
        &server,
        grant_body(&agent_id, &["app:read"], json!({ "apps": [&app_id] })),
    )
    .await;
    let grant_id = grant["id"].as_str().unwrap().to_string();
    let token = token_for(&owner, &server, &agent_id).await;

    // Row 13-adjacent: `GET /auth/me` lists the grant that is usable now.
    let me: Value = agent_get(&server, &token, "/api/v1/auth/me")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(me["type"], "agent");
    let grants = me["grants"].as_array().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0]["id"], grant_id);

    let res = owner
        .patch(server.url(&format!("/api/v1/agents/{agent_id}")))
        .json(&json!({ "disabled": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "disable: {}", res.text().await.unwrap());

    // Row 11: a valid grant does not outrank the disabled flag.
    let (status, code, reason) =
        problem(agent_get(&server, &token, &format!("/api/v1/apps/{app_id}")).await).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("agent_disabled"));
}

#[tokio::test]
async fn selector_filters_the_app_list_and_all_returns_everything() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let alpha = create_project(&owner, &server, "alpha").await;
    let beta = create_project(&owner, &server, "beta").await;
    let alpha_app = create_app(&owner, &server, &alpha, "alpha-web").await;
    let beta_app = create_app(&owner, &server, &beta, "beta-web").await;

    // Row 14 shape, with `list_apps` instead of `list_events`.
    let scoped = create_agent(&owner, &server, "scoped", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(&scoped, &["app:read"], json!({ "projects": ["alpha"] })),
    )
    .await;
    let scoped_token = token_for(&owner, &server, &scoped).await;
    let list: Value = agent_get(&server, &scoped_token, "/api/v1/apps")
        .await
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|app| app["id"].as_str())
        .collect();
    assert_eq!(ids, vec![alpha_app.as_str()]);
    assert!(!ids.contains(&beta_app.as_str()));

    let everything = create_agent(&owner, &server, "everything", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(&everything, &["app:read"], json!({ "all": true })),
    )
    .await;
    let everything_token = token_for(&owner, &server, &everything).await;
    let list: Value = agent_get(&server, &everything_token, "/api/v1/apps")
        .await
        .json()
        .await
        .unwrap();
    let count = list["items"].as_array().unwrap().len();
    assert_eq!(count, 2);

    // `{}` matches nothing: a list answers an empty page, never a refusal
    // (section 5.1), and a cluster-level read is refused.
    let nothing = create_agent(&owner, &server, "nothing", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(&nothing, &["app:read", "cluster:read"], json!({})),
    )
    .await;
    let nothing_token = token_for(&owner, &server, &nothing).await;
    let response = agent_get(&server, &nothing_token, "/api/v1/apps").await;
    assert_eq!(response.status(), 200);
    let list: Value = response.json().await.unwrap();
    assert!(list["items"].as_array().unwrap().is_empty());
    let response = agent_get(&server, &nothing_token, "/api/v1/cluster").await;
    assert_eq!(response.status(), 403);
    let problem: Value = response.json().await.unwrap();
    assert_eq!(problem["reason"], "selector_mismatch");
}

#[tokio::test]
async fn agent_reads_are_audited_and_human_reads_are_not() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "reader", &owner_user_id).await;
    create_grant(
        &owner,
        &server,
        grant_body(&agent_id, &["app:read"], json!({ "apps": [&app_id] })),
    )
    .await;
    let token = token_for(&owner, &server, &agent_id).await;

    for _ in 0..3 {
        let res = agent_get(&server, &token, &format!("/api/v1/apps/{app_id}")).await;
        assert_eq!(res.status(), 200);
    }
    let agent_ok = audit_count(
        &server,
        &format!(
            "SELECT count(*) FROM audit_log WHERE actor_type = 'agent' AND actor_id = '{agent_id}' \
             AND result = 'ok' AND operation = 'GET /api/v1/apps/{{app_id}}'"
        ),
    )
    .await;
    assert_eq!(agent_ok, 3);

    let before = audit_count(&server, "SELECT count(*) FROM audit_log").await;
    for _ in 0..3 {
        let res = owner
            .get(server.url(&format!("/api/v1/apps/{app_id}")))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }
    let after = audit_count(&server, "SELECT count(*) FROM audit_log").await;
    assert_eq!(before, after, "a human's read is not recorded");
}

#[tokio::test]
async fn expire_due_flips_a_grant_to_expired_and_emits_an_event() {
    let server = start().await;
    let owner = server.owner().await;
    let owner_user_id = owner_id(&owner, &server).await;
    let project_id = create_project(&owner, &server, "demo").await;
    let app_id = create_app(&owner, &server, &project_id, "web").await;

    let agent_id = create_agent(&owner, &server, "short", &owner_user_id).await;
    let mut body = grant_body(&agent_id, &["app:read"], json!({ "apps": [&app_id] }));
    body["ttl_s"] = json!(1);
    let grant = create_grant(&owner, &server, body).await;
    let grant_id = grant["id"].as_str().unwrap().to_string();

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let expired = quoro_control_plane::service::grants::expire_due(&server.state)
        .await
        .unwrap();
    assert!(expired >= 1, "the expired grant was swept");

    let read: Value = owner
        .get(server.url(&format!("/api/v1/grants/{grant_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(read["status"], "expired");
    let ttl_events = audit_count(
        &server,
        "SELECT count(*) FROM events WHERE type = 'grant.expired' \
         AND json_extract(data, '$.reason') = 'ttl'",
    )
    .await;
    assert!(ttl_events >= 1, "grant.expired(ttl) was emitted");

    // Running it again finds nothing left to do.
    let again = quoro_control_plane::service::grants::expire_due(&server.state)
        .await
        .unwrap();
    assert_eq!(again, 0);
}
