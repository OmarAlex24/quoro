//! `/api/v1/apps*`, deployments and domains, over real HTTP (M2).

mod common;

use common::{problem, start};
use quoro_control_plane::deploy;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_proto::wire;
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

/// A running control plane with the owner logged in, the local node registered
/// and one project created through the API.
async fn setup() -> (common::Server, reqwest::Client, String, String) {
    let server = start().await;
    let owner = server.owner().await;
    let node_id = driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
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
    // One provider of each domain kind, so a domain needs no `provider_id`.
    for (kind, name) in [("dns", "Dns"), ("tunnel", "Tunnel")] {
        let res = owner
            .post(server.url("/api/v1/providers"))
            .json(&json!({
                "kind": kind,
                "name": name,
                "config": { "account_id": "a1" },
                "secret": "provider-secret",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "provider: {}", res.text().await.unwrap());
    }
    (server, owner, node_id, project_id)
}

fn create_body(project_id: &str) -> Value {
    json!({
        "project_id": project_id,
        "name": "web",
        "image": "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "port": 8080,
    })
}

async fn create_app(owner: &reqwest::Client, server: &common::Server, project_id: &str) -> Value {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&create_body(project_id))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}

#[tokio::test]
async fn app_lifecycle_over_http() {
    let (server, owner, node_id, project_id) = setup().await;

    // create → stopped, with the documented defaults.
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    assert!(app_id.starts_with("app_"), "{app_id}");
    assert_eq!(app["status"], "stopped");
    assert_eq!(app["stateful"], false);
    assert_eq!(app["ha_policy"], "none");
    assert_eq!(app["port"], 8080);
    assert_eq!(app["health_check"]["path"], "/");
    assert_eq!(app["health_check"]["interval_s"], 10);
    assert_eq!(app["resources"]["vcpus"], 1);
    assert_eq!(app["resources"]["memory_mb"], 256);
    assert_eq!(app["resources"]["disk_gb"], 1);
    assert!(app["placement"]["node_ids"].as_array().unwrap().is_empty());
    assert!(app["current_deployment_id"].is_null());
    assert_eq!(
        app["backup_schedule"],
        json!({ "interval_h": 24, "keep": 7 })
    );
    assert!(app.get("ha_cost").is_none());
    assert_eq!(app["replicas"], 1);

    // list and filters.
    let list: Value = owner
        .get(server.url("/api/v1/apps"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    let list: Value = owner
        .get(server.url(&format!("/api/v1/apps?project_id={project_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);

    // env: values are write-only and never appear in a response.
    let secret = "s3cret-value-that-must-never-leak";
    let res = owner
        .put(server.url(&format!("/api/v1/apps/{app_id}/env")))
        .json(&json!({ "items": [
            { "name": "TOKEN", "value": secret },
            { "name": "MODE", "value": "production" },
        ] }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "env: {}", res.text().await.unwrap());
    let body = res.text().await.unwrap();
    assert!(!body.contains(secret), "{body}");
    assert!(!body.contains("production"), "{body}");
    let env: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/env")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = env["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["MODE", "TOKEN"]);
    let env_text = env.to_string();
    assert!(!env_text.contains(secret), "{env_text}");
    assert!(!env_text.contains("production"), "{env_text}");

    // The change log carries ciphertext, never the plaintext value.
    let payloads: Vec<(String,)> = sqlx::query_as("SELECT payload FROM change_log")
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
    for (payload,) in payloads {
        assert!(!payload.contains(secret), "change log leaked a value");
        assert!(!payload.contains("production"), "change log leaked a value");
    }

    // domains: acme starts pending; a duplicate hostname conflicts.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
        .json(&json!({ "hostname": "web.example.com", "tls_mode": "acme" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "domain: {}", res.text().await.unwrap());
    let domain: Value = res.json().await.unwrap();
    assert_eq!(domain["cert_status"], "pending");
    assert!(
        domain["provider_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("prv_")),
        "{domain}"
    );
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
            .json(&json!({ "hostname": "web.example.com", "tls_mode": "acme" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // A tunnel domain needs no certificate.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
        .json(&json!({ "hostname": "tunnel.example.com", "tls_mode": "tunnel" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let tunnel: Value = res.json().await.unwrap();
    assert_eq!(tunnel["cert_status"], "not_applicable");
    // A provider of the wrong kind for the TLS mode is refused.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
            .json(&json!({
                "hostname": "wrong.example.com",
                "tls_mode": "acme",
                "provider_id": tunnel["provider_id"],
            }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let domains: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/domains")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(domains["items"].as_array().unwrap().len(), 2);

    // A first deployment creates a VM row carrying `app_id`; the link is the
    // shared `created_at`.
    let image = "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": image }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        202,
        "create deployment: {}",
        res.text().await.unwrap()
    );
    let first: Value = res.json().await.unwrap();
    let first_id = first["id"].as_str().unwrap().to_string();
    assert_eq!(first["status"], "pending");
    assert_eq!(first["trigger"], "api");
    let (vm_id, vm_created_at): (String, String) =
        sqlx::query_as("SELECT id, created_at FROM vms WHERE app_id = ?")
            .bind(&app_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(first["created_at"], vm_created_at);
    assert_eq!(
        deployment_ids(&server, &node_id).await.get(&vm_id),
        Some(&first_id)
    );

    // A second deployment supersedes the first and deletes its VM row.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let second: Value = res.json().await.unwrap();
    let second_id = second["id"].as_str().unwrap().to_string();
    let first_now: Value = owner
        .get(server.url(&format!("/api/v1/deployments/{first_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first_now["status"], "superseded");
    assert!(!first_now["finished_at"].is_null());
    let (vms,): (i64,) = sqlx::query_as("SELECT count(*) FROM vms WHERE app_id = ?")
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(vms, 1, "only the new deployment's VM remains");

    // The node reports the VM running and healthy: the deployment succeeds and
    // becomes current.
    let (vm_id,): (String,) = sqlx::query_as("SELECT id FROM vms WHERE app_id = ?")
        .bind(&app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let observed = wire::VmStatus {
        vm_id: vm_id.clone(),
        observed_status: wire::ObservedStatus::Running,
        health: wire::Health::Passing,
        ip: None,
        restart_count: 0,
        last_exit_code: None,
    };
    deploy::on_vm_observed(&server.state.store, &node_id, &observed)
        .await
        .unwrap();
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["status"], "running");
    assert_eq!(app["current_deployment_id"], second_id);
    assert_eq!(app["instances"].as_array().unwrap().len(), 1);
    assert_eq!(app["instances"][0]["id"], vm_id);
    assert_eq!(app["instances"][0]["app_id"], app_id);
    assert_eq!(app["domains"].as_array().unwrap().len(), 2);

    // Newest-first deployment list paging: `cursor` means "ids below it".
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}/deployments?limit=1")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let page: Value = res.json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["id"], second_id);
    let cursor = page["next_cursor"].as_str().unwrap().to_string();
    let page: Value = owner
        .get(server.url(&format!(
            "/api/v1/apps/{app_id}/deployments?limit=1&cursor={cursor}"
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"][0]["id"], first_id);

    // stop → stopped, start → deploying, restart bumps the generation.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/stop")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "stop: {}", res.text().await.unwrap());
    let stopped: Value = res.json().await.unwrap();
    assert_eq!(stopped["status"], "stopped");
    let (desired,): (String,) = sqlx::query_as("SELECT desired_status FROM vms WHERE id = ?")
        .bind(&vm_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(desired, "stopped");

    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/start")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "start: {}", res.text().await.unwrap());
    let started: Value = res.json().await.unwrap();
    assert_eq!(started["status"], "deploying");
    // Starting again while already desired running conflicts.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/start")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/restart")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "restart: {}", res.text().await.unwrap());
    let restarted: Value = res.json().await.unwrap();
    assert_eq!(restarted["restart_generation"], 1);
    let (vm_generation,): (i64,) =
        sqlx::query_as("SELECT restart_generation FROM vms WHERE id = ?")
            .bind(&vm_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(vm_generation, 1);

    // redeploy returns the new deployment.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/redeploy")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "redeploy: {}", res.text().await.unwrap());
    let redeployed: Value = res.json().await.unwrap();
    assert_eq!(redeployed["trigger"], "redeploy");
    assert_eq!(redeployed["image"], app["image"]);

    // delete → 202; the app's VMs, domains, env and deployments are gone.
    let res = owner
        .delete(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());
    let deleted: Value = res.json().await.unwrap();
    assert_eq!(deleted["id"], app_id);
    for table in ["vms", "domains", "app_env", "deployments"] {
        let (rows,): (i64,) =
            sqlx::query_as(&format!("SELECT count(*) FROM {table} WHERE app_id = ?"))
                .bind(&app_id)
                .fetch_one(server.state.store.pool())
                .await
                .unwrap();
        assert_eq!(rows, 0, "{table} rows remain");
    }
    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/apps/{app_id}")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}

async fn deployment_ids(
    server: &common::Server,
    node_id: &str,
) -> std::collections::HashMap<String, String> {
    deploy::deployment_ids_for_node(&server.state.store, node_id)
        .await
        .unwrap()
}

#[tokio::test]
async fn app_validation_and_authorization() {
    let (server, owner, node_id, project_id) = setup().await;

    // No credential: 401.
    let (status, code, _) = problem(
        server
            .client()
            .get(server.url("/api/v1/apps"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));

    // A viewer reads but cannot create; the refusal is audited.
    server
        .insert_user("viewer@example.com", "a viewer password", "viewer")
        .await;
    let viewer = server.client();
    server
        .login(&viewer, "viewer@example.com", "a viewer password")
        .await;
    let res = viewer.get(server.url("/api/v1/apps")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let (status, code, reason) = problem(
        viewer
            .post(server.url("/api/v1/apps"))
            .json(&create_body(&project_id))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("scope_missing"))
    );
    assert!(
        server.audit().await.iter().any(|(operation, result, _)| {
            operation == "POST /api/v1/apps" && result == "refused"
        })
    );

    // Every refused setting is a `validation_failed`, and nothing is inserted.
    let mut cases = Vec::new();
    let mut body = create_body(&project_id);
    body["name"] = json!("Not A Slug");
    cases.push(("name", body));
    let mut body = create_body(&project_id);
    body["port"] = json!(0);
    cases.push(("port", body));
    let mut body = create_body(&project_id);
    body["ha_policy"] = json!("warm");
    cases.push(("ha_policy", body));
    let mut body = create_body(&project_id);
    body["stateful"] = json!(true);
    cases.push(("stateful", body));
    let mut body = create_body(&project_id);
    body["resources"] = json!({ "vcpus": 1, "memory_mb": 256, "disk_gb": 0 });
    cases.push(("disk_gb", body));
    let mut body = create_body(&project_id);
    body["health_check"] =
        json!({ "path": "health", "interval_s": 10, "timeout_s": 2, "failures": 3 });
    cases.push(("health path", body));
    let mut body = create_body(&project_id);
    body["placement"] = json!({ "node_ids": ["nod_not-a-real-node"] });
    cases.push(("placement", body));
    let mut body = create_body(&project_id);
    body["project_id"] = json!(quoro_types::new_id(quoro_types::Prefix::Project));
    cases.push(("unknown project", body));
    // Exactly one of `image` and `source` (openapi.yaml `CreateAppRequest`).
    let source = json!({ "kind": "git", "repo_url": "https://example.com/app.git", "ref": "main" });
    let mut body = create_body(&project_id);
    body.as_object_mut().unwrap().remove("image");
    cases.push(("neither image nor source", body));
    let mut body = create_body(&project_id);
    body["source"] = source.clone();
    cases.push(("both image and source", body));
    let mut body = create_body(&project_id);
    body.as_object_mut().unwrap().remove("image");
    body["source"] = json!({ "kind": "git", "repo_url": "git@example.com:app.git", "ref": "main" });
    cases.push(("source url is not https", body));
    for (label, body) in cases {
        let (status, code, _) = problem(
            owner
                .post(server.url("/api/v1/apps"))
                .json(&body)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{label}"
        );
    }
    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM apps")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);

    // A duplicate name in the same project is a conflict.
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/apps"))
            .json(&create_body(&project_id))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // An invalid hostname is refused.
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
            .json(&json!({ "hostname": "Web.Example.com", "tls_mode": "acme" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // Duplicate environment names are refused.
    let (status, code, _) = problem(
        owner
            .put(server.url(&format!("/api/v1/apps/{app_id}/env")))
            .json(&json!({ "items": [
                { "name": "TOKEN", "value": "a" },
                { "name": "TOKEN", "value": "b" },
            ] }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // The names the node sets on every instance are reserved (ADR 0142).
    for name in ["QUORO_INSTANCE_ID", "QUORO_NODE_ID"] {
        let (status, code, _) = problem(
            owner
                .put(server.url(&format!("/api/v1/apps/{app_id}/env")))
                .json(&json!({ "items": [{ "name": name, "value": "x" }] }))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{name}"
        );
    }

    // updateApp changes given fields only and conflicts on a taken name.
    let _other = create_app_named(&owner, &server, &project_id, "api").await;
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "port": 9090 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "update: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(updated["port"], 9090);
    assert_eq!(updated["name"], "web");
    let (status, code, _) = problem(
        owner
            .patch(server.url(&format!("/api/v1/apps/{app_id}")))
            .json(&json!({ "name": "api" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));
    // The backup schedule is stored; `cold` needs a stateful app and a
    // backup repository (409), `warm` and `hot` wait for M5b and M6 (422),
    // and a schedule out of range is 422 (ADR 0147).
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "backup_schedule": { "interval_h": 6, "keep": 3 } }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "schedule: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(
        updated["backup_schedule"],
        json!({ "interval_h": 6, "keep": 3 })
    );
    for (body, expected) in [
        (json!({ "ha_policy": "cold" }), (409, "conflict")),
        (json!({ "ha_policy": "warm" }), (422, "validation_failed")),
        (
            json!({ "backup_schedule": { "interval_h": 0, "keep": 7 } }),
            (422, "validation_failed"),
        ),
        (
            json!({ "backup_schedule": { "interval_h": 24, "keep": 101 } }),
            (422, "validation_failed"),
        ),
        // Replicas above 1 need an enrolled edge (ADR 0142, ADR 0143).
        (json!({ "replicas": 2 }), (409, "conflict")),
        (json!({ "replicas": 0 }), (422, "validation_failed")),
        (json!({ "replicas": 65 }), (422, "validation_failed")),
    ] {
        let (status, code, _) = problem(
            owner
                .patch(server.url(&format!("/api/v1/apps/{app_id}")))
                .json(&body)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!((status, code.as_str()), expected, "{body}");
    }
    // A Git source cannot be added to an app created from an image.
    let (status, code, _) = problem(
        owner
            .patch(server.url(&format!("/api/v1/apps/{app_id}")))
            .json(&json!({ "source": source }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    // A malformed app id is a 404, not a 422.
    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/apps/nope"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // Domain delete is 204 and notifies; a second delete is 404.
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/domains")))
        .json(&json!({ "hostname": "one.example.com", "tls_mode": "acme" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let domain: Value = res.json().await.unwrap();
    let domain_id = domain["id"].as_str().unwrap();
    let res = owner
        .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body = res.text().await.unwrap();
    assert_eq!(status, 204, "{body}");
    let (status, code, _) = problem(
        owner
            .delete(server.url(&format!("/api/v1/domains/{domain_id}")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // Idempotent create: the same key and body return the same app, one row.
    let body = json!({
        "project_id": project_id,
        "name": "idem",
        "image": "ghcr.io/example/idem@sha256:0",
        "port": 8080,
    });
    let first = owner
        .post(server.url("/api/v1/apps"))
        .header("Idempotency-Key", "create-idem-once")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201, "{}", first.text().await.unwrap());
    let first: Value = first.json().await.unwrap();
    let second = owner
        .post(server.url("/api/v1/apps"))
        .header("Idempotency-Key", "create-idem-once")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 201);
    let second: Value = second.json().await.unwrap();
    assert_eq!(first["id"], second["id"]);
    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM apps WHERE name = 'idem'")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // The deployment create needs a usable node; with none available it is a
    // `validation_failed` (the test node is removed here).
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, _) = problem(
        owner
            .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
            .json(&json!({ "image": "ghcr.io/example/web@sha256:3" }))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

async fn create_app_named(
    owner: &reqwest::Client,
    server: &common::Server,
    project_id: &str,
    name: &str,
) -> Value {
    let mut body = create_body(project_id);
    body["name"] = json!(name);
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create app: {}",
        res.text().await.unwrap()
    );
    res.json().await.unwrap()
}
