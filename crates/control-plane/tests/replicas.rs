//! Replicas of stateless apps (ADR 0142): validation, the scheduler,
//! the rolling deployment and a scale without a deployment.

mod common;

use common::{Server, start};
use quoro_control_plane::deploy;
use quoro_control_plane::driver::{self, Observation, ObservedStatus};
use quoro_proto::wire;
use serde_json::{Value, json};
use sqlx::Row;

/// A node id that sorts before [`NODE_B`], so placement order is predictable.
const NODE_A: &str = "nod_00000000000000000000000001";
/// The second placement node.
const NODE_B: &str = "nod_00000000000000000000000002";
/// A machine with the `edge` role and no `node` role.
const EDGE: &str = "nod_0000000000000000000000000E";

/// Starts a control plane with the owner logged in, one project and the given
/// node rows.
async fn setup(nodes: &[(&str, &[&str])]) -> (Server, reqwest::Client, String) {
    let server = start().await;
    let owner = server.owner().await;
    for (id, roles) in nodes {
        insert_node(&server, id, roles).await;
    }
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "rep", "name": "Replicas" }))
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
    (server, owner, project_id)
}

/// Inserts a connected node row with the given roles (the API would enroll
/// one; these tests only need the scheduler's view).
async fn insert_node(server: &Server, id: &str, roles: &[&str]) {
    let now = quoro_control_plane::store::now_rfc3339();
    let roles = serde_json::to_string(roles).unwrap();
    sqlx::query(
        "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
         labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
         log_seq, last_seen_at, created_at) \
         VALUES (?, ?, ?, NULL, 'connected', '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, \
         0, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(roles)
    .bind(&now)
    .bind(&now)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// Creates an app through the API with `replicas` and an optional placement.
async fn create_app(
    owner: &reqwest::Client,
    server: &Server,
    project_id: &str,
    name: &str,
    replicas: i64,
    placement: Option<Value>,
) -> Value {
    let mut body = json!({
        "project_id": project_id,
        "name": name,
        "image": "img@sha256:1",
        "port": 8080,
        "replicas": replicas,
    });
    if let Some(placement) = placement {
        body["placement"] = placement;
    }
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

/// A problem response as `(status, code, detail)`.
async fn failure(res: reqwest::Response) -> (u16, String, String) {
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap();
    (
        status,
        body["code"].as_str().unwrap_or_default().to_string(),
        body["detail"].as_str().unwrap_or_default().to_string(),
    )
}

/// Starts a deployment through the API and returns its response.
async fn deploy_image(
    owner: &reqwest::Client,
    server: &Server,
    app_id: &str,
    image: &str,
) -> Value {
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": image }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "deploy: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// One VM row of an app.
#[derive(Debug)]
struct VmRow {
    id: String,
    node_id: String,
    created_at: String,
}

/// The VM rows of `app_id`, ordered by node id.
async fn vms(server: &Server, app_id: &str) -> Vec<VmRow> {
    let rows = sqlx::query(
        "SELECT id, node_id, created_at FROM vms WHERE app_id = ? ORDER BY node_id, id",
    )
    .bind(app_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|row| VmRow {
            id: row.get("id"),
            node_id: row.get("node_id"),
            created_at: row.get("created_at"),
        })
        .collect()
}

/// The VM rows linked to the deployment written at `created_at`.
async fn deployment_vms(server: &Server, app_id: &str, created_at: &str) -> Vec<VmRow> {
    vms(server, app_id)
        .await
        .into_iter()
        .filter(|vm| vm.created_at == created_at)
        .collect()
}

/// The status of a deployment row.
async fn deployment_status(server: &Server, deployment_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM deployments WHERE id = ?")
        .bind(deployment_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Reports a VM's observed state the way a node's status report does, then
/// applies it to the deployment. Returns the VM ids whose desired state
/// changed.
async fn observe(
    server: &Server,
    vm_id: &str,
    observed: wire::ObservedStatus,
    health: wire::Health,
) -> Vec<String> {
    let node_id: String = sqlx::query_scalar("SELECT node_id FROM vms WHERE id = ?")
        .bind(vm_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let status = match observed {
        wire::ObservedStatus::Creating => ObservedStatus::Creating,
        wire::ObservedStatus::Starting => ObservedStatus::Starting,
        wire::ObservedStatus::Running => ObservedStatus::Running,
        wire::ObservedStatus::Crashed => ObservedStatus::Crashed,
        wire::ObservedStatus::Deleted => ObservedStatus::Deleted,
        _ => ObservedStatus::Unknown,
    };
    driver::report_observed(
        &server.state.store,
        &node_id,
        vm_id,
        &Observation {
            status,
            ip: None,
            exit_code: None,
            restart_count: None,
        },
    )
    .await
    .unwrap();
    deploy::on_vm_observed(
        &server.state.store,
        &node_id,
        &wire::VmStatus {
            vm_id: vm_id.to_string(),
            observed_status: observed,
            health,
            ip: None,
            restart_count: 0,
            last_exit_code: None,
        },
    )
    .await
    .unwrap()
}

/// Rolls a two-replica app through to success and returns `(app_id,
/// deployment, its VMs)`.
async fn successful_two_replica_deployment(
    owner: &reqwest::Client,
    server: &Server,
    project_id: &str,
    image: &str,
) -> (String, Value, Vec<VmRow>) {
    let app = create_app(owner, server, project_id, "web", 2, None).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    let deployment = deploy_image(owner, server, &app_id, image).await;
    let created_at = deployment["created_at"].as_str().unwrap().to_string();

    let first = deployment_vms(server, &app_id, &created_at).await;
    assert_eq!(first.len(), 1, "a rollout starts one instance");
    assert_eq!(first[0].node_id, NODE_A);
    let next = observe(
        server,
        &first[0].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;
    assert_eq!(next.len(), 1, "the next instance is created");
    let second = deployment_vms(server, &app_id, &created_at).await;
    assert_eq!(second.len(), 2);
    assert_eq!(second[1].node_id, NODE_B);
    let retired = observe(
        server,
        &second[1].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;
    assert!(retired.is_empty(), "{retired:?}");
    assert_eq!(
        deployment_status(server, deployment["id"].as_str().unwrap()).await,
        "succeeded"
    );
    let vms = deployment_vms(server, &app_id, &created_at).await;
    assert_eq!(vms.len(), 2);
    (app_id, deployment, vms)
}

#[tokio::test]
async fn replicas_follow_the_edge_source_and_eligible_node_rules() {
    let (server, owner, project_id) = setup(&[(NODE_A, &["node"]), (NODE_B, &["node"])]).await;

    // Without an edge, more than one replica is a conflict (ADR 0142).
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "no-edge",
            "image": "img@sha256:1",
            "port": 8080,
            "replicas": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(failure(res).await.0, 409);

    // updateApp applies the same edge rule.
    let solo = create_app(&owner, &server, &project_id, "solo", 1, None).await;
    let solo_id = solo["id"].as_str().unwrap();
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{solo_id}")))
        .json(&json!({ "replicas": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(failure(res).await.0, 409);

    insert_node(&server, EDGE, &["edge"]).await;

    // A Git-source app cannot run more than one instance (ADR 0142), whether the
    // count arrives at creation or in an update.
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "git-app",
            "source": { "kind": "git", "repo_url": "https://example.com/app.git", "ref": "main" },
            "port": 8080,
            "replicas": 2,
        }))
        .send()
        .await
        .unwrap();
    let (status, code, _) = failure(res).await;
    assert_eq!((status, code.as_str()), (409, "conflict"));

    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "git-one",
            "source": { "kind": "git", "repo_url": "https://example.com/app.git", "ref": "main" },
            "port": 8080,
            "replicas": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "create: {}", res.text().await.unwrap());
    let git_one: Value = res.json().await.unwrap();
    let git_one_id = git_one["id"].as_str().unwrap();
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{git_one_id}")))
        .json(&json!({ "replicas": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(failure(res).await.0, 409);

    // Above the eligible nodes is a validation failure that names both
    // numbers (ADR 0142).
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "too-many",
            "image": "img@sha256:1",
            "port": 8080,
            "replicas": 3,
        }))
        .send()
        .await
        .unwrap();
    let (status, code, detail) = failure(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert!(
        detail.contains("replicas 3 is above the 2 eligible nodes"),
        "{detail}"
    );

    // Exactly the eligible nodes succeeds.
    let app = create_app(&owner, &server, &project_id, "two", 2, None).await;
    assert_eq!(app["replicas"], 2);

    // Placement narrows eligibility.
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({
            "project_id": project_id,
            "name": "narrow",
            "image": "img@sha256:1",
            "port": 8080,
            "replicas": 2,
            "placement": { "node_ids": [NODE_A] },
        }))
        .send()
        .await
        .unwrap();
    let (status, code, detail) = failure(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert!(
        detail.contains("replicas 2 is above the 1 eligible nodes"),
        "{detail}"
    );

    // updateApp applies the same rules; a short count writes nothing.
    let one = create_app(&owner, &server, &project_id, "one", 1, None).await;
    let one_id = one["id"].as_str().unwrap();
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{one_id}")))
        .json(&json!({ "replicas": 3 }))
        .send()
        .await
        .unwrap();
    assert_eq!(failure(res).await.0, 422);
    let current: Value = owner
        .get(server.url(&format!("/api/v1/apps/{one_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(current["replicas"], 1);
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{one_id}")))
        .json(&json!({ "replicas": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let updated: Value = res.json().await.unwrap();
    assert_eq!(updated["replicas"], 2);
}

#[tokio::test]
async fn a_rollout_starts_one_node_at_a_time_and_retires_the_old_instances() {
    let (server, owner, project_id) =
        setup(&[(NODE_A, &["node"]), (NODE_B, &["node"]), (EDGE, &["edge"])]).await;
    let (app_id, _first, first_vms) =
        successful_two_replica_deployment(&owner, &server, &project_id, "img@sha256:1").await;

    // A second deployment starts on the first node only, next to the old
    // instances.
    let second = deploy_image(&owner, &server, &app_id, "img@sha256:2").await;
    let second_at = second["created_at"].as_str().unwrap().to_string();
    let new_vms = deployment_vms(&server, &app_id, &second_at).await;
    assert_eq!(new_vms.len(), 1);
    assert_eq!(new_vms[0].node_id, NODE_A);
    assert_eq!(
        vms(&server, &app_id).await.len(),
        3,
        "old instances keep running"
    );

    // The first new instance passing starts the second one.
    let created = observe(
        &server,
        &new_vms[0].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;
    let second_vms = deployment_vms(&server, &app_id, &second_at).await;
    assert_eq!(second_vms.len(), 2);
    assert_eq!(second_vms[1].node_id, NODE_B);
    assert_eq!(created, vec![second_vms[1].id.clone()]);
    assert_eq!(
        deployment_status(&server, second["id"].as_str().unwrap()).await,
        "starting"
    );

    // The second new instance passing succeeds the deployment and retires the
    // previous deployment's instances.
    let retired = observe(
        &server,
        &second_vms[1].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;
    let first_ids: Vec<String> = first_vms.iter().map(|vm| vm.id.clone()).collect();
    let mut sorted = retired.clone();
    sorted.sort();
    let mut expected = first_ids.clone();
    expected.sort();
    assert_eq!(sorted, expected);
    assert_eq!(
        deployment_status(&server, second["id"].as_str().unwrap()).await,
        "succeeded"
    );
    let final_vms = vms(&server, &app_id).await;
    assert_eq!(final_vms.len(), 2);
    for vm in &final_vms {
        assert_eq!(vm.created_at, second_at);
    }

    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["current_deployment_id"], second["id"]);
    assert_eq!(app["status"], "running");
}

#[tokio::test]
async fn a_failure_mid_roll_deletes_the_new_instances_and_keeps_the_old_ones() {
    let (server, owner, project_id) =
        setup(&[(NODE_A, &["node"]), (NODE_B, &["node"]), (EDGE, &["edge"])]).await;
    let (app_id, _first, first_vms) =
        successful_two_replica_deployment(&owner, &server, &project_id, "img@sha256:1").await;

    let second = deploy_image(&owner, &server, &app_id, "img@sha256:2").await;
    let second_at = second["created_at"].as_str().unwrap().to_string();
    let new_vms = deployment_vms(&server, &app_id, &second_at).await;
    assert_eq!(new_vms.len(), 1);
    observe(
        &server,
        &new_vms[0].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;
    let new_vms = deployment_vms(&server, &app_id, &second_at).await;
    assert_eq!(new_vms.len(), 2);

    // The second new instance crashes before the rollout completes.
    let deleted = observe(
        &server,
        &new_vms[1].id,
        wire::ObservedStatus::Crashed,
        wire::Health::Unknown,
    )
    .await;
    let mut sorted = deleted.clone();
    sorted.sort();
    let mut expected: Vec<String> = new_vms.iter().map(|vm| vm.id.clone()).collect();
    expected.sort();
    assert_eq!(sorted, expected);
    assert_eq!(
        deployment_status(&server, second["id"].as_str().unwrap()).await,
        "failed"
    );

    let remaining = vms(&server, &app_id).await;
    let mut remaining_ids: Vec<String> = remaining.iter().map(|vm| vm.id.clone()).collect();
    remaining_ids.sort();
    let mut old_ids: Vec<String> = first_vms.iter().map(|vm| vm.id.clone()).collect();
    old_ids.sort();
    assert_eq!(remaining_ids, old_ids);
}

#[tokio::test]
async fn scaling_without_a_deployment_adds_and_removes_instances() {
    let (server, owner, project_id) =
        setup(&[(NODE_A, &["node"]), (NODE_B, &["node"]), (EDGE, &["edge"])]).await;
    let app = create_app(&owner, &server, &project_id, "web", 1, None).await;
    let app_id = app["id"].as_str().unwrap().to_string();
    let deployment = deploy_image(&owner, &server, &app_id, "img@sha256:1").await;
    let created_at = deployment["created_at"].as_str().unwrap().to_string();
    let first = deployment_vms(&server, &app_id, &created_at).await;
    assert_eq!(first.len(), 1);
    observe(
        &server,
        &first[0].id,
        wire::ObservedStatus::Running,
        wire::Health::Passing,
    )
    .await;

    // Scale up: a second instance linked to the current deployment on the
    // other node.
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "replicas": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let scaled = deployment_vms(&server, &app_id, &created_at).await;
    assert_eq!(scaled.len(), 2);
    assert_eq!(scaled[0].node_id, NODE_A);
    assert_eq!(scaled[1].node_id, NODE_B);

    // A count the nodes cannot hold is refused and writes nothing.
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "replicas": 3 }))
        .send()
        .await
        .unwrap();
    let (status, code, detail) = failure(res).await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));
    assert!(
        detail.contains("replicas 3 is above the 2 eligible nodes"),
        "{detail}"
    );
    assert_eq!(vms(&server, &app_id).await.len(), 2);

    // Scale down removes the instance on the highest node id.
    let res = owner
        .patch(server.url(&format!("/api/v1/apps/{app_id}")))
        .json(&json!({ "replicas": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{}", res.text().await.unwrap());
    let remaining = vms(&server, &app_id).await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].node_id, NODE_A);
}

#[tokio::test]
async fn an_app_with_one_of_two_instances_crashed_is_degraded() {
    let (server, owner, project_id) =
        setup(&[(NODE_A, &["node"]), (NODE_B, &["node"]), (EDGE, &["edge"])]).await;
    let (app_id, _deployment, _vms) =
        successful_two_replica_deployment(&owner, &server, &project_id, "img@sha256:1").await;

    let vms = vms(&server, &app_id).await;
    assert_eq!(vms.len(), 2);
    observe(
        &server,
        &vms[0].id,
        wire::ObservedStatus::Crashed,
        wire::Health::Unknown,
    )
    .await;

    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["status"], "degraded");
}
