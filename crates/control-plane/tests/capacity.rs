//! `GET /api/v1/cluster/capacity` (`getClusterCapacity`, the N+1 indicator,
//! ADR 0178) over real HTTP: refusals, placement against what the remaining
//! nodes have left, and an agent's narrow grant.
//!
//! The apps' VMs, volumes and resources are written directly, as the warm
//! tests do, so each case is exact without a running node.

mod common;

use common::{Server, problem, start};
use quoro_proto::sign;
use serde_json::{Value, json};

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// Enrolls a node with 8 vCPUs, 16 GiB and 200 GiB.
async fn node(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> String {
    common::settle_voters(server).await;
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
            "join_token": token["token"],
            "name": name,
            "public_key": sign::public_key_to_base64(
                &sign::signing_key_from_seed(&[seed; 32]).verifying_key()
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
    body["node_id"].as_str().unwrap().to_string()
}

/// Sets a node's status, as a session that opens or ends would.
async fn set_status(server: &Server, node_id: &str, status: &str) {
    sqlx::query("UPDATE nodes SET status = ? WHERE id = ?")
        .bind(status)
        .bind(node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

async fn project(server: &Server, owner: &reqwest::Client, slug: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": slug, "name": slug }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "project: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    body["id"].as_str().unwrap().to_string()
}

/// Creates an app, then gives it 1 vCPU, `memory_mb` and 4 GiB.
async fn app(
    server: &Server,
    owner: &reqwest::Client,
    project_id: &str,
    name: &str,
    memory_mb: i64,
) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "app: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    let app_id = body["id"].as_str().unwrap().to_string();
    sqlx::query("UPDATE apps SET resources = ? WHERE id = ?")
        .bind(json!({ "vcpus": 1, "memory_mb": memory_mb, "disk_gb": 4 }).to_string())
        .bind(&app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    app_id
}

/// Places a running VM of `app_id` (or of no app) on `node_id`.
async fn vm(
    server: &Server,
    project_id: &str,
    app_id: Option<&str>,
    node_id: &str,
    id: &str,
    memory_mb: i64,
) {
    let now = quoro_control_plane::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
         image, desired_status, observed_status, ip, restart_generation, restart_count, \
         last_exit_code, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, 1, ?, 4, ?, 'running', 'running', NULL, 0, 0, NULL, ?, ?)",
    )
    .bind(id)
    .bind(project_id)
    .bind(app_id)
    .bind(node_id)
    .bind(id)
    .bind(memory_mb)
    .bind(IMAGE)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Makes `app_id` a stateful `cold` app with a ready volume on `node_id`.
async fn stateful_on(server: &Server, app_id: &str, node_id: &str, volume_id: &str) {
    let pool = server.state.store.pool();
    let now = quoro_control_plane::store::now_rfc3339();
    sqlx::query("UPDATE apps SET stateful = 1, ha_policy = 'cold' WHERE id = ?")
        .bind(app_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO volumes (id, app_id, node_id, size_gb, mount_path, state, allocated_bytes, \
         restored_from_backup_id, error, created_at, updated_at) \
         VALUES (?, ?, ?, 2, '/data', 'ready', NULL, NULL, NULL, ?, ?)",
    )
    .bind(volume_id)
    .bind(app_id)
    .bind(node_id)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
}

async fn capacity(client: &reqwest::Client, server: &Server, lost: &str) -> Value {
    let res = client
        .get(server.url(&format!("/api/v1/cluster/capacity?lost_node_id={lost}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "capacity: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// The entry of `app_id` in an answer.
fn entry<'a>(answer: &'a Value, app_id: &str) -> &'a Value {
    answer["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["app_id"] == app_id)
        .unwrap_or_else(|| panic!("{app_id} is not listed: {answer}"))
}

#[tokio::test]
async fn a_missing_malformed_unknown_or_removed_node_is_refused() {
    let server = start().await;
    let owner = server.owner().await;
    let removed = node(&server, &owner, "gone", 41).await;
    set_status(&server, &removed, "removed").await;

    let cases = [
        (String::new(), 422, "validation_failed"),
        (
            "?lost_node_id=nod_short".to_string(),
            422,
            "validation_failed",
        ),
        (
            "?lost_node_id=nod_00000000000000000000000000".to_string(),
            404,
            "not_found",
        ),
        (format!("?lost_node_id={removed}"), 404, "not_found"),
    ];
    for (query, status, code) in cases {
        let res = owner
            .get(server.url(&format!("/api/v1/cluster/capacity{query}")))
            .send()
            .await
            .unwrap();
        let (got, got_code, _) = problem(res).await;
        assert_eq!((got, got_code.as_str()), (status, code), "{query:?}");
    }
}

#[tokio::test]
async fn the_lost_nodes_apps_are_placed_on_what_the_others_have_left() {
    let server = start().await;
    let owner = server.owner().await;
    let lost = node(&server, &owner, "lost", 51).await;
    let full = node(&server, &owner, "full", 52).await;
    let roomy = node(&server, &owner, "roomy", 53).await;
    let project_id = project(&server, &owner, "cap").await;

    let web = app(&server, &owner, &project_id, "web", 1024).await;
    let api = app(&server, &owner, &project_id, "api", 512).await;
    let db = app(&server, &owner, &project_id, "db", 2048).await;
    vm(
        &server,
        &project_id,
        Some(&web),
        &lost,
        "vm_00000000000000000000000001",
        1024,
    )
    .await;
    // `api` runs on `lost` and on `roomy`: it only loses a replica.
    vm(
        &server,
        &project_id,
        Some(&api),
        &lost,
        "vm_00000000000000000000000002",
        512,
    )
    .await;
    vm(
        &server,
        &project_id,
        Some(&api),
        &roomy,
        "vm_00000000000000000000000003",
        512,
    )
    .await;
    // `db` keeps its data on `lost` and has no backup.
    vm(
        &server,
        &project_id,
        Some(&db),
        &lost,
        "vm_00000000000000000000000004",
        2048,
    )
    .await;
    stateful_on(&server, &db, &lost, "vol_00000000000000000000000001").await;
    // `full` has 384 MiB left, so `web` goes to `roomy`.
    vm(
        &server,
        &project_id,
        None,
        &full,
        "vm_00000000000000000000000005",
        16_000,
    )
    .await;
    for node_id in [&lost, &full, &roomy] {
        set_status(&server, node_id, "connected").await;
    }

    let answer = capacity(&owner, &server, &lost).await;
    assert_eq!(answer["lost_node_id"], lost.as_str());
    assert_eq!(answer["fits"], true, "{answer}");
    assert_eq!(
        answer["missing"],
        json!({ "cpus": 0, "memory_mb": 0, "disk_gb": 0 })
    );
    assert!(answer["computed_at"].is_string());
    assert_eq!(answer["apps"].as_array().unwrap().len(), 3, "{answer}");
    assert_eq!(
        entry(&answer, &api),
        &json!({ "app_id": api, "policy": "none", "action": "keep",
                 "node_id": roomy, "reason": null })
    );
    assert_eq!(
        entry(&answer, &web),
        &json!({ "app_id": web, "policy": "none", "action": "start",
                 "node_id": roomy, "reason": null })
    );
    assert_eq!(
        entry(&answer, &db),
        &json!({ "app_id": db, "policy": "cold", "action": "restore",
                 "node_id": null, "reason": "no_backup" })
    );

    // `roomy` is not connected any more: nothing has room for `web`, and the
    // answer says what is missing. `api` still keeps its instance there.
    set_status(&server, &roomy, "disconnected").await;
    let answer = capacity(&owner, &server, &lost).await;
    assert_eq!(answer["fits"], false, "{answer}");
    assert_eq!(
        answer["missing"],
        json!({ "cpus": 1, "memory_mb": 1024, "disk_gb": 4 })
    );
    assert_eq!(entry(&answer, &web)["reason"], "no_capacity");
    assert_eq!(entry(&answer, &web)["node_id"], Value::Null);

    // A node that runs no app: nothing to move, it fits.
    let answer = capacity(&owner, &server, &full).await;
    assert_eq!(answer["fits"], true);
    assert_eq!(answer["apps"], json!([]));
}

#[tokio::test]
async fn an_agent_sees_only_the_apps_its_grant_matches() {
    let server = start().await;
    let owner = server.owner().await;
    let lost = node(&server, &owner, "lost", 61).await;
    let other = node(&server, &owner, "other", 62).await;
    let alpha = project(&server, &owner, "alpha").await;
    let beta = project(&server, &owner, "beta").await;
    let seen = app(&server, &owner, &alpha, "seen", 1024).await;
    let hidden = app(&server, &owner, &beta, "hidden", 4096).await;
    vm(
        &server,
        &alpha,
        Some(&seen),
        &lost,
        "vm_00000000000000000000000011",
        1024,
    )
    .await;
    vm(
        &server,
        &beta,
        Some(&hidden),
        &lost,
        "vm_00000000000000000000000012",
        4096,
    )
    .await;
    // `other` has 512 MiB left: neither app fits.
    vm(
        &server,
        &alpha,
        None,
        &other,
        "vm_00000000000000000000000013",
        15_872,
    )
    .await;
    for node_id in [&lost, &other] {
        set_status(&server, node_id, "connected").await;
    }

    let full = capacity(&owner, &server, &lost).await;
    assert_eq!(full["apps"].as_array().unwrap().len(), 2, "{full}");
    assert_eq!(full["missing"]["memory_mb"], 4096 + 1024, "{full}");

    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let res = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "capacity-watcher",
            "model": "test-model",
            "owner_user_id": me["user"]["id"],
            "description": "reads the N+1 indicator",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "agent: {}", res.text().await.unwrap());
    let agent: Value = res.json().await.unwrap();
    let agent_id = agent["id"].as_str().unwrap();
    let res = owner
        .post(server.url("/api/v1/grants"))
        .json(&json!({
            "agent_id": agent_id,
            "scopes": ["cluster:read"],
            "selector": { "projects": ["alpha"] },
            "ttl_s": 3600,
            "reason": "watch one project's capacity",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "grant: {}", res.text().await.unwrap());
    let res = owner
        .post(server.url(&format!("/api/v1/agents/{agent_id}/keys")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "key: {}", res.text().await.unwrap());
    let key: Value = res.json().await.unwrap();
    let res = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "token: {}", res.text().await.unwrap());
    let token: Value = res.json().await.unwrap();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "Authorization",
        format!("Bearer {}", token["access_token"].as_str().unwrap())
            .parse()
            .unwrap(),
    );
    let agent = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();

    // Only `seen` is listed, and `fits` and `missing` count only it: the
    // hidden app's resources never show through.
    let narrow = capacity(&agent, &server, &lost).await;
    assert_eq!(
        narrow["apps"],
        json!([{ "app_id": seen, "policy": "none", "action": "start",
                 "node_id": null, "reason": "no_capacity" }]),
        "{narrow}"
    );
    assert_eq!(narrow["fits"], false);
    assert_eq!(
        narrow["missing"],
        json!({ "cpus": 1, "memory_mb": 1024, "disk_gb": 4 })
    );

    // The read is audited for the agent, as every agent read is.
    let audited = server
        .audit()
        .await
        .into_iter()
        .any(|(operation, _, _)| operation.contains("cluster/capacity"));
    assert!(audited, "no audit record for the agent's read");
}
