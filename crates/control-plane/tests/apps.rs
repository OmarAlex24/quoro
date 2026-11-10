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
    setup_on(start().await).await
}

/// [`setup`] on `server`.
async fn setup_on(server: common::Server) -> (common::Server, reqwest::Client, String, String) {
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
    // ADR 0184: absent from the request, the restart fields take their defaults.
    assert_eq!(app["health_check"]["start_period_s"], 30);
    assert_eq!(app["health_check"]["restart"], true);
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
    // ADR 0167: a new domain is `pending` until the primary computes its status.
    assert_eq!(domain["serving_status"], "pending");
    assert!(domain["last_error"].is_null(), "{domain}");
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
    deploy::deployments_for_node(&server.state.store, node_id)
        .await
        .unwrap()
        .into_iter()
        .map(|(vm_id, deployment)| (vm_id, deployment.id))
        .collect()
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
    body["health_check"] = json!({
        "path": "/", "interval_s": 10, "timeout_s": 2, "failures": 3, "start_period_s": -1
    });
    cases.push(("health start period", body));
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
    // backup repository (409), and a schedule out of range is 422 (ADR 0147).
    // `warm` has its own tests below, since a build with the dev-only
    // `dev-warm-policy` feature accepts it (ADR 0223).
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
        (json!({ "ha_policy": "hot" }), (422, "validation_failed")),
        (
            json!({ "backup_schedule": { "interval_h": 0, "keep": 7 } }),
            (422, "validation_failed"),
        ),
        (
            json!({ "backup_schedule": { "interval_h": 24, "keep": 101 } }),
            (422, "validation_failed"),
        ),
        // Replicas above 1 need an enrolled edge, and this cluster has none
        // (ADR 0142, ADR 0143); the whole rules live in `replicas.rs`.
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

// ---------------------------------------------------------------------------
// `ha_policy: warm` and the dev-only `dev-warm-policy` feature (ADR 0223,
// ADR 0277)
// ---------------------------------------------------------------------------

async fn patch_app(
    owner: &reqwest::Client,
    server: &common::Server,
    app_id: &str,
    body: Value,
) -> reqwest::Response {
    owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn ha_policy_of(owner: &reqwest::Client, server: &common::Server, app_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "get app: {}", res.text().await.unwrap());
    let app: Value = res.json().await.unwrap();
    app["ha_policy"].clone()
}

/// A default build refuses `ha_policy` `warm` and its settings until the last
/// warm PR (ADR 0277), prices `warm` as not in this release, and the app
/// keeps its policy.
#[cfg(not(feature = "dev-warm-policy"))]
#[tokio::test]
async fn a_default_build_refuses_ha_policy_warm() {
    let (server, owner, node_id, project_id) = setup().await;
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap();
    // Even an app that could be `warm` in a test-lab build.
    warm::stateful(&server, app_id, &node_id).await;
    warm::eligible_node(&server, warm::NODE_A, 2).await;
    for body in [
        json!({ "ha_policy": "warm" }),
        json!({ "warm": { "interval_s": 60 } }),
        json!({ "ha_policy": "warm", "warm": { "node_id": warm::NODE_A } }),
    ] {
        let (status, code, _) =
            problem(patch_app(&owner, &server, app_id, body.clone()).await).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{body}"
        );
    }
    assert_eq!(ha_policy_of(&owner, &server, app_id).await, "none");
    // The refused settings are audited as before: an absent `node_id` reads
    // null.
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT params FROM audit_log WHERE operation = 'PATCH /api/v1/apps/{app_id}' ORDER BY seq",
    )
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    let audited: Vec<Value> = rows
        .iter()
        .map(|(params,)| serde_json::from_str::<Value>(params).unwrap())
        .filter(|params| params["ha_policy"].is_null() && !params["warm"].is_null())
        .map(|params| params["warm"].clone())
        .collect();
    assert_eq!(audited, [json!({ "node_id": null, "interval_s": 60 })]);
    let cost = warm::cost(&owner, &server, app_id).await;
    assert_eq!(cost["available"], false);
    assert_eq!(cost["unavailable_reason"], "not_in_this_release");
    assert_eq!(
        cost["reserved"],
        json!({ "vcpus": 1, "memory_mb": 256, "disk_gb": 1 })
    );
    assert!(cost["storage_bytes"].is_null());

    // The sweep picks no warm node in this build, even for a `warm` app
    // written directly with none.
    sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    quoro_control_plane::sweeper::repick_warm_nodes(&server.state)
        .await
        .unwrap();
    assert_eq!(warm::stored(&server, app_id).await, (None, 0, 30));
}

/// Rows for the warm policy tests (ADR 0277): nodes as a session would leave
/// them, and an app's attached volume, written directly.
#[cfg_attr(not(feature = "dev-warm-policy"), allow(dead_code))]
mod warm {
    use super::common;
    use serde_json::{Value, json};

    pub const NODE_A: &str = "nod_0000000000000000000000000A";
    pub const NODE_B: &str = "nod_0000000000000000000000000B";
    pub const NODE_C: &str = "nod_0000000000000000000000000C";

    /// A connected node with the `node` role that snapshots with `reflink`
    /// and has the mesh address `10.43.0.<host>`: eligible as a warm node.
    pub async fn eligible_node(server: &common::Server, id: &str, host: u8) {
        let now = quoro_control_plane::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, \
             behind_nat, labels, capacity, allocated, kvm, created_at, volume_snapshot, \
             mesh_address) \
             VALUES (?, ?, '[\"node\"]', NULL, 'connected', '', NULL, 1, '{}', \
             '{\"cpus\":8,\"memory_mb\":16384,\"disk_gb\":200}', \
             '{\"cpus\":0,\"memory_mb\":0,\"disk_gb\":0}', \
             '{\"available\":true,\"kernel\":\"6.9.0\"}', ?, 'reflink', ?)",
        )
        .bind(id)
        .bind(format!("warm-{host}"))
        .bind(&now)
        .bind(format!("10.43.0.{host}"))
        .execute(server.state.store.pool())
        .await
        .unwrap();
    }

    /// The id of the volume [`stateful`] writes for `app_id`.
    pub fn volume_id(app_id: &str) -> String {
        format!("vol_{}", &app_id[4..])
    }

    /// Makes `app_id` stateful: a `ready` volume of 2 GiB with 4096 allocated
    /// bytes on `node_id`, which snapshots with `reflink` and has a mesh
    /// address.
    pub async fn stateful(server: &common::Server, app_id: &str, node_id: &str) {
        let pool = server.state.store.pool();
        let now = quoro_control_plane::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, \
             allocated_bytes, restored_from_backup_id, error, created_at, updated_at) \
             VALUES (?, ?, ?, 2, '/data', 'ready', 4096, NULL, NULL, ?, ?)",
        )
        .bind(volume_id(app_id))
        .bind(app_id)
        .bind(node_id)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("UPDATE apps SET stateful = 1 WHERE id = ?")
            .bind(app_id)
            .execute(pool)
            .await
            .unwrap();
        set_node(
            server,
            node_id,
            "volume_snapshot = 'reflink', mesh_address = '10.43.0.1'",
        )
        .await;
    }

    /// Runs `UPDATE nodes SET <set> WHERE id = <node_id>`.
    pub async fn set_node(server: &common::Server, node_id: &str, set: &str) {
        sqlx::query(&format!("UPDATE nodes SET {set} WHERE id = ?"))
            .bind(node_id)
            .execute(server.state.store.pool())
            .await
            .unwrap();
    }

    /// The stored `(warm_node_id, warm_node_pinned, warm_interval_s)`.
    pub async fn stored(server: &common::Server, app_id: &str) -> (Option<String>, i64, i64) {
        sqlx::query_as(
            "SELECT warm_node_id, warm_node_pinned, warm_interval_s FROM apps WHERE id = ?",
        )
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
    }

    /// The `warm` entry of the app's `ha_cost`.
    pub async fn cost(owner: &reqwest::Client, server: &common::Server, app_id: &str) -> Value {
        let app = super::get_app(owner, server, app_id).await;
        let policies = app["ha_cost"]["policies"].as_array().unwrap().clone();
        assert_eq!(policies[2]["policy"], "warm", "{policies:?}");
        policies[2].clone()
    }

    /// The payload of the app's newest change-log entry.
    pub async fn payload(server: &common::Server, app_id: &str) -> Value {
        let (payload,): (String,) = sqlx::query_as(
            "SELECT payload FROM change_log WHERE entity_type = 'app' AND entity_id = ? \
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(app_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
        serde_json::from_str(&payload).unwrap()
    }

    /// The warm fields of an app's change-log payload (ADR 0286).
    pub fn warm_fields(payload: &Value) -> Value {
        json!({
            "warm": payload["warm"],
            "warm_node_id": payload["warm_node_id"],
            "warm_node_pinned": payload["warm_node_pinned"],
            "warm_interval_s": payload["warm_interval_s"],
        })
    }

    /// The `changed` lists of the app's `app.updated` events, oldest first.
    pub async fn changes(server: &common::Server, app_id: &str) -> Vec<Value> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT data FROM events WHERE type = 'app.updated' \
             AND json_extract(data, '$.app_id') = ? ORDER BY seq",
        )
        .bind(app_id)
        .fetch_all(server.state.store.pool())
        .await
        .unwrap();
        rows.into_iter()
            .map(|(data,)| serde_json::from_str::<Value>(&data).unwrap()["changed"].clone())
            .collect()
    }
}

/// Every app's change-log payload carries the three warm columns beside the
/// App fields, with `warm` null, in every build (ADR 0286).
#[tokio::test]
async fn an_app_change_log_payload_carries_the_warm_columns() {
    let (server, owner, _node_id, project_id) = setup().await;
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap();
    let defaults = json!({
        "warm": null,
        "warm_node_id": null,
        "warm_node_pinned": false,
        "warm_interval_s": 30,
    });
    let payload = warm::payload(&server, app_id).await;
    assert_eq!(payload["id"], app_id);
    assert_eq!(payload["name"], app["name"]);
    assert_eq!(warm::warm_fields(&payload), defaults);

    // An update writes the stored values, not the defaults.
    sqlx::query("UPDATE apps SET warm_interval_s = 45 WHERE id = ?")
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let res = patch_app(&owner, &server, app_id, json!({ "name": "renamed" })).await;
    assert_eq!(res.status(), 200, "rename: {}", res.text().await.unwrap());
    let payload = warm::payload(&server, app_id).await;
    assert_eq!(payload["name"], "renamed");
    assert_eq!(
        warm::warm_fields(&payload),
        json!({
            "warm": null,
            "warm_node_id": null,
            "warm_node_pinned": false,
            "warm_interval_s": 45,
        })
    );
}

async fn get_app(owner: &reqwest::Client, server: &common::Server, app_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "get app: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// A test-lab build with the dev-only `dev-warm-policy` feature runs the warm
/// policy (ADR 0277): the scheduler picks the warm node of an app that becomes
/// `warm`, an operator pins and unpins it and sets the interval, and an app
/// that leaves `warm` loses its node and pin and keeps its interval
/// (`openapi.yaml` `UpdateAppRequest`, `WarmSettings`, `WarmCopy.node_id`,
/// migration 0011).
#[cfg(feature = "dev-warm-policy")]
#[tokio::test]
async fn a_dev_warm_policy_build_picks_and_pins_the_warm_node() {
    let (server, owner, node_id, project_id) = setup().await;
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap();
    warm::stateful(&server, app_id, &node_id).await;
    warm::eligible_node(&server, warm::NODE_A, 2).await;
    warm::eligible_node(&server, warm::NODE_B, 3).await;
    // Not eligible: it snapshots with `pause_copy`.
    warm::eligible_node(&server, warm::NODE_C, 4).await;
    warm::set_node(&server, warm::NODE_C, "volume_snapshot = 'pause_copy'").await;

    let available = warm::cost(&owner, &server, app_id).await;
    assert_eq!(available["available"], true, "{available}");
    assert!(available["unavailable_reason"].is_null());
    // Disk only: the 2 GiB volume plus the 1 GiB root disk; the volume's
    // allocated bytes before the warm node reports its copy.
    assert_eq!(
        available["reserved"],
        json!({ "vcpus": 0, "memory_mb": 0, "disk_gb": 3 })
    );
    assert_eq!(available["storage_bytes"], 4096);
    assert_eq!(available["restore_estimate_s"], 10);
    assert_eq!(available["pause_estimate_ms"], 0);

    // The scheduler picks the first eligible node by id when none keeps a copy.
    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "warm" })).await;
    assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(updated["ha_policy"], "warm");
    assert_eq!(updated["warm"]["node_id"], warm::NODE_A);
    assert_eq!(updated["warm"]["node_pinned"], false);
    assert_eq!(updated["warm"]["interval_s"], 30);
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_A.to_owned()), 0, 30)
    );
    assert_eq!(
        warm::changes(&server, app_id).await.last().unwrap(),
        &json!(["ha_policy", "warm"])
    );
    // The change log carries the stored settings beside the App fields, with
    // `warm` null (ADR 0286).
    let payload = warm::payload(&server, app_id).await;
    assert_eq!(payload["ha_policy"], "warm");
    assert_eq!(
        warm::warm_fields(&payload),
        json!({
            "warm": null,
            "warm_node_id": warm::NODE_A,
            "warm_node_pinned": false,
            "warm_interval_s": 30,
        })
    );

    // Pins and settings the API refuses, with nothing written.
    for body in [
        json!({ "ha_policy": "hot" }),
        json!({ "warm": { "interval_s": 4 } }),
        json!({ "warm": { "interval_s": 3601 } }),
        json!({ "warm": { "node_id": "node-a" } }),
        // The volume's node, a `pause_copy` node and an unknown node.
        json!({ "warm": { "node_id": node_id } }),
        json!({ "warm": { "node_id": warm::NODE_C } }),
        json!({ "warm": { "node_id": "nod_0000000000000000000000000Z" } }),
        // Warm settings need the app to stay `warm`.
        json!({ "ha_policy": "none", "warm": { "interval_s": 60 } }),
    ] {
        let (status, code, _) =
            problem(patch_app(&owner, &server, app_id, body.clone()).await).await;
        assert_eq!(
            (status, code.as_str()),
            (422, "validation_failed"),
            "{body}"
        );
    }
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_A.to_owned()), 0, 30)
    );

    // A pin, then an interval that leaves the pin alone.
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": warm::NODE_B } }),
    )
    .await;
    assert_eq!(res.status(), 200, "pin: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(updated["warm"]["node_id"], warm::NODE_B);
    assert_eq!(updated["warm"]["node_pinned"], true);
    assert_eq!(
        warm::changes(&server, app_id).await.last().unwrap(),
        &json!(["warm"])
    );
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "interval_s": 60 } }),
    )
    .await;
    assert_eq!(res.status(), 200, "interval: {}", res.text().await.unwrap());
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_B.to_owned()), 1, 60)
    );
    assert_eq!(
        warm::warm_fields(&warm::payload(&server, app_id).await),
        json!({
            "warm": null,
            "warm_node_id": warm::NODE_B,
            "warm_node_pinned": true,
            "warm_interval_s": 60,
        })
    );
    // The same settings again change nothing, and emit no event.
    let events = warm::changes(&server, app_id).await.len();
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "ha_policy": "warm", "warm": { "node_id": warm::NODE_B, "interval_s": 60 } }),
    )
    .await;
    assert_eq!(res.status(), 200, "same: {}", res.text().await.unwrap());
    assert_eq!(warm::changes(&server, app_id).await.len(), events);

    // `null` lets the scheduler pick: it keeps an eligible node, which
    // already holds the copy.
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": null } }),
    )
    .await;
    assert_eq!(res.status(), 200, "unpin: {}", res.text().await.unwrap());
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_B.to_owned()), 0, 60)
    );
    // Pinned again, then unpinned while disconnected: the scheduler picks among
    // the connected nodes.
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": warm::NODE_B } }),
    )
    .await;
    assert_eq!(res.status(), 200, "pin: {}", res.text().await.unwrap());
    warm::set_node(&server, warm::NODE_B, "status = 'disconnected'").await;
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": null } }),
    )
    .await;
    assert_eq!(res.status(), 200, "unpin: {}", res.text().await.unwrap());
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_A.to_owned()), 0, 60)
    );

    // Leaving `warm` drops the node and the pin and keeps the interval.
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": warm::NODE_A } }),
    )
    .await;
    assert_eq!(res.status(), 200, "pin: {}", res.text().await.unwrap());
    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "none" })).await;
    assert_eq!(res.status(), 200, "none: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert!(updated["warm"].is_null(), "{updated}");
    assert_eq!(warm::stored(&server, app_id).await, (None, 0, 60));
    assert_eq!(
        warm::changes(&server, app_id).await.last().unwrap(),
        &json!(["ha_policy", "warm"])
    );

    // Back to `warm` with a pin in the same request; a disconnected node is
    // not eligible.
    let (status, code, _) = problem(
        patch_app(
            &owner,
            &server,
            app_id,
            json!({ "ha_policy": "warm", "warm": { "node_id": warm::NODE_B } }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert_eq!(ha_policy_of(&owner, &server, app_id).await, "none");
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "ha_policy": "warm", "warm": { "node_id": warm::NODE_A } }),
    )
    .await;
    assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_A.to_owned()), 1, 60)
    );

    // With its volume gone, a `warm` app cannot pin a node, and can still be
    // unpinned.
    sqlx::query("UPDATE volumes SET state = 'deleting' WHERE app_id = ?")
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    let (status, code, _) = problem(
        patch_app(
            &owner,
            &server,
            app_id,
            json!({ "warm": { "node_id": warm::NODE_B } }),
        )
        .await,
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": null } }),
    )
    .await;
    assert_eq!(res.status(), 200, "unpin: {}", res.text().await.unwrap());
    assert_eq!(
        warm::stored(&server, app_id).await,
        (Some(warm::NODE_A.to_owned()), 0, 60)
    );

    // `createApp` still accepts only `none`.
    let mut body = create_body(&project_id);
    body["name"] = json!("born-warm");
    body["ha_policy"] = json!("warm");
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/apps"))
            .json(&body)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
}

/// A test-lab build refuses `warm` where it cannot run (`openapi.yaml`
/// `UpdateAppRequest.ha_policy`, `HaPolicyCost.unavailable_reason`): a
/// stateless app, a volume node that does not snapshot with `reflink`, no
/// other eligible node, and an app with a Git source (ADR 0265). `ha_cost`
/// gives the same reason.
#[cfg(feature = "dev-warm-policy")]
#[tokio::test]
async fn a_dev_warm_policy_build_refuses_warm_where_it_cannot_run() {
    let (server, owner, node_id, project_id) = setup().await;
    let refused = |app_id: String, reason: &'static str| {
        let (server, owner) = (&server, &owner);
        async move {
            let (status, code, _) =
                problem(patch_app(owner, server, &app_id, json!({ "ha_policy": "warm" })).await)
                    .await;
            assert_eq!(
                (status, code.as_str()),
                (422, "validation_failed"),
                "{reason}"
            );
            assert_eq!(ha_policy_of(owner, server, &app_id).await, "none");
            let cost = warm::cost(owner, server, &app_id).await;
            assert_eq!(cost["available"], false, "{reason}");
            assert_eq!(cost["unavailable_reason"], reason);
            cost
        }
    };

    // Stateless: nothing to copy, and nothing reserved.
    let stateless = create_app(&owner, &server, &project_id).await;
    let stateless = stateless["id"].as_str().unwrap().to_owned();
    let cost = refused(stateless, "stateless").await;
    assert_eq!(
        cost["reserved"],
        json!({ "vcpus": 0, "memory_mb": 0, "disk_gb": 0 })
    );
    assert!(cost["restore_estimate_s"].is_null());

    // Stateful, with no other node yet.
    let app = create_app_named(&owner, &server, &project_id, "db").await;
    let app_id = app["id"].as_str().unwrap().to_owned();
    warm::stateful(&server, &app_id, &node_id).await;
    refused(app_id.clone(), "no_warm_node").await;
    // A node that is not eligible: disconnected, without a mesh address,
    // without the `node` role, or with an unknown snapshot method.
    warm::eligible_node(&server, warm::NODE_A, 2).await;
    for set in [
        "status = 'disconnected'",
        "mesh_address = NULL",
        "roles = '[\"edge\"]'",
        "volume_snapshot = NULL",
    ] {
        warm::set_node(&server, warm::NODE_A, set).await;
        refused(app_id.clone(), "no_warm_node").await;
        warm::set_node(
            &server,
            warm::NODE_A,
            "status = 'connected', mesh_address = '10.43.0.2', roles = '[\"node\"]', \
             volume_snapshot = 'reflink'",
        )
        .await;
    }
    // The volume's node does not snapshot with `reflink`.
    warm::set_node(&server, &node_id, "volume_snapshot = 'pause_copy'").await;
    refused(app_id.clone(), "no_warm_node").await;
    warm::set_node(&server, &node_id, "volume_snapshot = 'reflink'").await;
    let res = patch_app(&owner, &server, &app_id, json!({ "ha_policy": "warm" })).await;
    assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());

    // An app built from a Git source, even stateful with an eligible node.
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "git-app",
            "source": { "kind": "git", "repo_url": "https://example.com/app.git", "ref": "main" },
            "port": 8080,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "git app: {}", res.text().await.unwrap());
    let git: Value = res.json().await.unwrap();
    let git_id = git["id"].as_str().unwrap().to_owned();
    warm::stateful(&server, &git_id, &node_id).await;
    refused(git_id, "not_in_this_release").await;
}

/// The sweeper keeps a `warm` app's warm node while it is disconnected, has no
/// mesh address or has not reported its snapshot method, and picks another
/// once it is lost for good (`openapi.yaml` `WarmCopy.node_id`, ADR 0278). An
/// operator's pin lasts until the node is removed, and with no node eligible
/// the app has none until one is.
#[cfg(feature = "dev-warm-policy")]
#[tokio::test]
async fn a_dev_warm_policy_build_picks_another_warm_node_only_for_a_lasting_loss() {
    let (server, owner, node_id, project_id) = setup().await;
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap();
    warm::stateful(&server, app_id, &node_id).await;
    warm::eligible_node(&server, warm::NODE_A, 2).await;
    warm::eligible_node(&server, warm::NODE_B, 3).await;
    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "warm" })).await;
    assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());
    let sweep = || async {
        quoro_control_plane::sweeper::repick_warm_nodes(&server.state)
            .await
            .unwrap();
        warm::stored(&server, app_id).await
    };
    let on = |node: &str, pinned: i64| (Some(node.to_owned()), pinned, 30);
    assert_eq!(sweep().await, on(warm::NODE_A, 0));

    // Transient: the node stays the warm node.
    for set in [
        "status = 'disconnected'",
        "mesh_address = NULL",
        "volume_snapshot = NULL",
    ] {
        warm::set_node(&server, warm::NODE_A, set).await;
        assert_eq!(sweep().await, on(warm::NODE_A, 0), "{set}");
    }
    warm::set_node(
        &server,
        warm::NODE_A,
        "status = 'connected', mesh_address = '10.43.0.2', volume_snapshot = 'reflink'",
    )
    .await;
    let events = warm::changes(&server, app_id).await.len();

    // Lasting, for a node the scheduler picked: no `node` role, `pause_copy`,
    // or the volume's node.
    warm::set_node(&server, warm::NODE_A, "roles = '[\"edge\"]'").await;
    assert_eq!(sweep().await, on(warm::NODE_B, 0));
    assert_eq!(
        warm::changes(&server, app_id).await[events..],
        [json!(["warm"])]
    );
    // The sweep's change-log entry carries the new node (ADR 0286).
    assert_eq!(
        warm::warm_fields(&warm::payload(&server, app_id).await),
        json!({
            "warm": null,
            "warm_node_id": warm::NODE_B,
            "warm_node_pinned": false,
            "warm_interval_s": 30,
        })
    );
    warm::set_node(&server, warm::NODE_A, "roles = '[\"node\"]'").await;
    warm::set_node(&server, warm::NODE_B, "volume_snapshot = 'pause_copy'").await;
    assert_eq!(sweep().await, on(warm::NODE_A, 0));
    warm::set_node(&server, warm::NODE_B, "volume_snapshot = 'reflink'").await;
    sqlx::query("UPDATE volumes SET node_id = ? WHERE app_id = ?")
        .bind(warm::NODE_A)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(sweep().await, on(warm::NODE_B, 0));
    sqlx::query("UPDATE volumes SET node_id = ? WHERE app_id = ?")
        .bind(&node_id)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    // A pin outlasts all of that, and ends when its node is removed.
    let res = patch_app(
        &owner,
        &server,
        app_id,
        json!({ "warm": { "node_id": warm::NODE_A } }),
    )
    .await;
    assert_eq!(res.status(), 200, "pin: {}", res.text().await.unwrap());
    warm::set_node(
        &server,
        warm::NODE_A,
        "roles = '[\"edge\"]', volume_snapshot = 'pause_copy'",
    )
    .await;
    assert_eq!(sweep().await, on(warm::NODE_A, 1));
    warm::set_node(&server, warm::NODE_A, "status = 'removed'").await;
    assert_eq!(sweep().await, on(warm::NODE_B, 0));

    // No node eligible after a lasting loss: none, until one is.
    warm::set_node(&server, warm::NODE_B, "status = 'removed'").await;
    assert_eq!(sweep().await, (None, 0, 30));
    let warm = get_app(&owner, &server, app_id).await["warm"].clone();
    assert!(warm["node_id"].is_null(), "{warm}");
    assert_eq!(warm["state"], "failed", "{warm}");
    warm::eligible_node(&server, warm::NODE_C, 4).await;
    assert_eq!(sweep().await, on(warm::NODE_C, 0));

    // A `warm` app with no warm node that leaves `warm` still changes
    // `App.warm`, to null.
    warm::set_node(&server, warm::NODE_C, "status = 'removed'").await;
    assert_eq!(sweep().await, (None, 0, 30));
    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "none" })).await;
    assert_eq!(res.status(), 200, "none: {}", res.text().await.unwrap());
    assert_eq!(
        warm::changes(&server, app_id).await.last().unwrap(),
        &json!(["ha_policy", "warm"])
    );
}

/// A node's session with the primary, for the warm policy tests that need
/// one: an enrolled node whose `hello` gives it a mesh address and `reflink`,
/// and its `status_report`.
#[cfg(feature = "dev-warm-policy")]
mod session {
    use super::common::Server;
    use base64::Engine as _;
    use futures_util::{SinkExt, StreamExt};
    use quoro_proto::wire::{self, NodeMessage};
    use serde_json::{Value, json};
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    pub type Socket = WebSocketStream<TcpStream>;

    fn capacity() -> wire::Capacity {
        wire::Capacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        }
    }

    async fn send(socket: &mut Socket, message: NodeMessage) {
        let text = message
            .to_frame("msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D", None)
            .to_text();
        socket.send(WsMessage::Text(text.into())).await.unwrap();
    }

    /// Enrolls a node with the `node` role, opens its session and reads up to
    /// its first `desired_state`. Returns its id and socket.
    pub async fn connected_node(
        server: &Server,
        owner: &reqwest::Client,
        name: &str,
        seed: u8,
    ) -> (String, Socket) {
        // Section 8.10 rule 2: these tests enroll voters back to back.
        super::common::settle_voters(server).await;
        let token: Value = owner
            .post(server.url("/api/v1/nodes/join-tokens"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let res = server
            .client()
            .post(server.url("/api/v1/nodes/enroll"))
            .json(&json!({
                "join_token": token["token"].as_str().unwrap(),
                "name": name,
                "public_key": quoro_proto::sign::public_key_to_base64(
                    &quoro_proto::sign::signing_key_from_seed(&[seed; 32]).verifying_key()
                ),
                "roles": ["node"],
                "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
                "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 201, "enroll: {}", res.text().await.unwrap());
        let body: Value = res.json().await.unwrap();
        let node_id = body["node_id"].as_str().unwrap().to_string();
        let credential = body["credential"].as_str().unwrap();

        let url = format!("ws://{}/api/v1/nodes/session", server.address());
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {credential}").parse().unwrap(),
        );
        let stream = TcpStream::connect(server.address()).await.unwrap();
        let (mut socket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .expect("websocket upgrade");
        let hello = NodeMessage::Hello(wire::Hello {
            node_id: node_id.clone(),
            agent_version: "test-agent".to_string(),
            engine_version: "test-engine".to_string(),
            boot_id: "boot-1".to_string(),
            stored_rev: 0,
            applied_rev: 0,
            log_seq: 0,
            log_hash: "0".repeat(64),
            leases: Vec::new(),
            mesh_public_key: Some(base64::engine::general_purpose::STANDARD.encode([seed; 32])),
            capacity: capacity(),
            allocated: wire::Capacity::default(),
            kvm: wire::Kvm {
                available: true,
                kernel: "6.9.0".to_string(),
                nested: Some(true),
            },
            dropped_events: 0,
            volume_snapshot: Some(wire::SnapshotMethod::Reflink),
            custody_public_key: None,
        });
        send(&mut socket, hello).await;
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
                .await
                .expect("a frame within 30 s")
                .expect("the socket is open")
                .expect("a valid websocket frame");
            if let WsMessage::Text(text) = message {
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                if frame["type"] == "desired_state" {
                    break;
                }
            }
        }
        (node_id, socket)
    }

    /// The `spec` of the first `desired_state` that `wanted` accepts, within
    /// 30 s; earlier frames are skipped.
    pub async fn spec_where(socket: &mut Socket, wanted: impl Fn(&Value) -> bool) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
                .await
                .expect("a desired_state within 30 s")
                .expect("the socket is open")
                .expect("a valid websocket frame");
            if let WsMessage::Text(text) = message {
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                if frame["type"] == "desired_state" && wanted(&frame["body"]["spec"]) {
                    return frame["body"]["spec"].clone();
                }
            }
        }
    }

    /// Sends a `status_report` whose `warm` holds one `role: target` entry
    /// for `volume_id` of `app_id`, with `allocated_bytes`.
    pub async fn report_target(
        socket: &mut Socket,
        volume_id: &str,
        app_id: &str,
        allocated_bytes: u64,
    ) {
        let entry = wire::WarmStatus {
            volume_id: volume_id.to_string(),
            app_id: app_id.to_string(),
            role: wire::WarmRole::Target,
            snapshot_seq: Some(1),
            snapshot_taken_at: None,
            completed_at: None,
            allocated_bytes: Some(allocated_bytes),
            extent_count: None,
            frozen: None,
            rejected_token: None,
            rejected_at: None,
            error: None,
        };
        let report = NodeMessage::StatusReport(wire::StatusReport {
            applied_rev: 0,
            vms: Vec::new(),
            routes: Vec::new(),
            capacity: capacity(),
            allocated: wire::Capacity::default(),
            disk_free_gb: 90.0,
            builds: Vec::new(),
            edge_backends: Vec::new(),
            backups: Vec::new(),
            images: Vec::new(),
            warm: vec![entry],
            volumes: Vec::new(),
            quarantines: Vec::new(),
        });
        send(socket, report).await;
    }
}

/// The node the scheduler picks is sent its target entry at once (section
/// 6.13), and `ha_cost.warm.storage_bytes` is then the allocated bytes of the
/// copy it last reported, and the volume's allocated bytes before that
/// (`openapi.yaml` `HaPolicyCost.storage_bytes`). A report of the same volume
/// from a node that is not the warm node does not count.
#[cfg(feature = "dev-warm-policy")]
#[tokio::test]
async fn a_dev_warm_policy_build_wakes_the_warm_node_and_prices_its_report() {
    // The node sends nothing while the test talks to the API.
    let (server, owner, node_id, project_id) =
        setup_on(common::start_with_patient_node_session().await).await;
    let app = create_app(&owner, &server, &project_id).await;
    let app_id = app["id"].as_str().unwrap();
    warm::stateful(&server, app_id, &node_id).await;
    let volume_id = warm::volume_id(app_id);
    // The volume's node holds the app's lease, so the pair forms.
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, granted_at) VALUES ('lse_0000000000000000000000000W', 'app', ?, ?, \
         'local', 4, ?)",
    )
    .bind(app_id)
    .bind(&node_id)
    .bind(quoro_control_plane::store::now_rfc3339())
    .execute(server.state.store.pool())
    .await
    .unwrap();
    let (warm_node, mut socket) = session::connected_node(&server, &owner, "warm-a", 7).await;

    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "warm" })).await;
    assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(updated["warm"]["node_id"], warm_node.as_str());
    session::spec_where(&mut socket, |spec| {
        spec["volumes"].as_array().is_some_and(|files| {
            files.iter().any(|file| {
                file["volume_id"] == volume_id.as_str() && file["warm"]["role"] == "target"
            })
        })
    })
    .await;
    assert_eq!(
        warm::cost(&owner, &server, app_id).await["storage_bytes"],
        4096
    );

    // Another node's report of the same volume is not the warm copy.
    let (other, mut other_socket) = session::connected_node(&server, &owner, "warm-b", 8).await;
    session::report_target(&mut other_socket, &volume_id, app_id, 1_000).await;
    session::report_target(&mut socket, &volume_id, app_id, 8_192).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let cost = warm::cost(&owner, &server, app_id).await;
        if cost["storage_bytes"] == 8_192 {
            break;
        }
        assert_ne!(cost["storage_bytes"], 1_000, "{cost}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "storage_bytes never followed the report: {cost}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // The warm node stops snapshotting with `reflink` while no other node is
    // eligible: the app has no warm node, and the old one keeps its copy, which
    // a failover may still need. Once another node is eligible the sweep picks
    // it. The new node gets its target entry, and the old one, which still
    // reports the copy, now gets a `deleted` entry (section 5.11).
    let has = |state: &'static str, target: bool| {
        let volume_id = volume_id.clone();
        move |spec: &Value| {
            spec["volumes"].as_array().is_some_and(|files| {
                files.iter().any(|file| {
                    file["volume_id"] == volume_id.as_str()
                        && file["state"] == state
                        && (file["warm"]["role"] == "target") == target
                })
            })
        }
    };
    let sweep = || async {
        quoro_control_plane::sweeper::repick_warm_nodes(&server.state)
            .await
            .unwrap();
        warm::stored(&server, app_id).await.0
    };
    warm::set_node(&server, &other, "volume_snapshot = 'pause_copy'").await;
    warm::set_node(&server, &warm_node, "volume_snapshot = 'pause_copy'").await;
    assert_eq!(sweep().await, None);
    // The old warm node's snapshot drops the copy and sends it no entry:
    // nothing is stale while the app has no warm node.
    session::spec_where(&mut socket, |spec| {
        !spec["volumes"].as_array().is_some_and(|files| {
            files
                .iter()
                .any(|file| file["volume_id"] == volume_id.as_str())
        })
    })
    .await;
    warm::set_node(&server, &other, "volume_snapshot = 'reflink'").await;
    assert_eq!(sweep().await, Some(other.clone()));
    session::spec_where(&mut other_socket, has("present", true)).await;
    session::spec_where(&mut socket, has("deleted", false)).await;

    // Leaving `warm` wakes the warm node, whose reported copy gets its
    // `deleted` entry.
    let res = patch_app(&owner, &server, app_id, json!({ "ha_policy": "none" })).await;
    assert_eq!(res.status(), 200, "none: {}", res.text().await.unwrap());
    session::spec_where(&mut other_socket, has("deleted", false)).await;
}

/// The scheduler spreads warm copies: each app that becomes `warm` gets the
/// eligible node that keeps the fewest copies, then the lowest id.
#[cfg(feature = "dev-warm-policy")]
#[tokio::test]
async fn a_dev_warm_policy_build_spreads_warm_copies_over_the_nodes() {
    let (server, owner, node_id, project_id) = setup().await;
    warm::eligible_node(&server, warm::NODE_A, 2).await;
    warm::eligible_node(&server, warm::NODE_B, 3).await;
    let mut picked = Vec::new();
    for name in ["db-1", "db-2", "db-3"] {
        let app = create_app_named(&owner, &server, &project_id, name).await;
        let app_id = app["id"].as_str().unwrap().to_owned();
        warm::stateful(&server, &app_id, &node_id).await;
        let res = patch_app(&owner, &server, &app_id, json!({ "ha_policy": "warm" })).await;
        assert_eq!(res.status(), 200, "warm: {}", res.text().await.unwrap());
        let updated: Value = res.json().await.unwrap();
        picked.push(updated["warm"]["node_id"].as_str().unwrap().to_owned());
    }
    assert_eq!(picked, [warm::NODE_A, warm::NODE_B, warm::NODE_A]);
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
