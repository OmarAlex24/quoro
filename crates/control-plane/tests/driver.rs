//! The driver API: node registration, desired state reads and observation
//! reports.

mod common;

use quoro_control_plane::changelog;
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode, Observation, ObservedStatus};
use quoro_control_plane::store::now_rfc3339;
use quoro_types::{Prefix, is_id, new_id};
use serde_json::Value;

fn local_node(name: &str, cpus: u32) -> LocalNode {
    LocalNode {
        name: name.to_string(),
        capacity: Capacity {
            cpus,
            memory_mb: 8192,
            disk_gb: 200,
        },
        kvm: Kvm {
            available: true,
            kernel: "6.9.0-test".to_string(),
            nested: Some(false),
        },
        engine_version: "0.1.0".to_string(),
        agent_version: "0.1.0".to_string(),
    }
}

#[tokio::test]
async fn registers_once_observes_and_serves_the_node() {
    let server = common::start().await;
    let store = server.state.store.clone();

    // First start inserts the primary.
    let node_id = driver::register_local_node(&store, &local_node("primary", 4))
        .await
        .unwrap();
    assert!(is_id(Prefix::Node, &node_id), "{node_id}");

    // A later start updates the same row instead of inserting a second node.
    let again = driver::register_local_node(&store, &local_node("primary-renamed", 8))
        .await
        .unwrap();
    assert_eq!(again, node_id);
    let (nodes,): (i64,) = sqlx::query_as("SELECT count(*) FROM nodes")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(nodes, 1);
    let (name,): (String,) = sqlx::query_as("SELECT name FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(name, "primary-renamed");

    // A project and a VM placed on the node, as the VM services will leave them.
    let project_id = new_id(Prefix::Project);
    let vm_id = new_id(Prefix::Vm);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&project_id)
    .bind("driver")
    .bind("Driver")
    .bind(&now)
    .bind(&now)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, image, \
         desired_status, observed_status, ip, restart_generation, restart_count, last_exit_code, \
         created_at, updated_at) \
         VALUES (?, ?, NULL, ?, 'one', 1, 512, 10, 'img@sha256:abc', 'running', 'starting', NULL, \
         0, 0, NULL, ?, ?)",
    )
    .bind(&vm_id)
    .bind(&project_id)
    .bind(&node_id)
    .bind(&now)
    .bind(&now)
    .execute(store.pool())
    .await
    .unwrap();

    // desired_vm reads it, vms_on_node lists it.
    let desired = driver::desired_vm(&store, &vm_id).await.unwrap().unwrap();
    assert_eq!(desired.observed_status, ObservedStatus::Starting);
    assert_eq!(desired.memory_mb, 512);
    assert_eq!(
        driver::vms_on_node(&store, &node_id).await.unwrap(),
        vec![vm_id.clone()]
    );
    assert!(
        driver::desired_vm(&store, "vm_01JC9Z6Q5W8N3K4T7R2M1XHB0D")
            .await
            .unwrap()
            .is_none()
    );

    // Entering running emits vm.started.
    driver::report_observed(
        &store,
        &node_id,
        &vm_id,
        &Observation {
            status: ObservedStatus::Running,
            ip: Some("10.0.0.2".to_string()),
            exit_code: None,
            restart_count: None,
        },
    )
    .await
    .unwrap();
    let started: Vec<(String, String)> =
        sqlx::query_as("SELECT subject, data FROM events WHERE type = 'vm.started'")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(started.len(), 1);
    let subject: Value = serde_json::from_str(&started[0].0).unwrap();
    assert_eq!(subject["id"].as_str(), Some(vm_id.as_str()));
    let data: Value = serde_json::from_str(&started[0].1).unwrap();
    assert_eq!(data["ip"], "10.0.0.2");
    assert_eq!(data["node_id"], node_id);

    // The same report again changes nothing and appends nothing.
    let (before,): (i64,) = sqlx::query_as("SELECT count(*) FROM change_log")
        .fetch_one(store.pool())
        .await
        .unwrap();
    driver::report_observed(
        &store,
        &node_id,
        &vm_id,
        &Observation {
            status: ObservedStatus::Running,
            ip: Some("10.0.0.2".to_string()),
            exit_code: None,
            restart_count: None,
        },
    )
    .await
    .unwrap();
    let (after,): (i64,) = sqlx::query_as("SELECT count(*) FROM change_log")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(before, after);

    // Renaming the node after the VM exists refreshes the allocation.
    driver::register_local_node(&store, &local_node("primary-renamed", 8))
        .await
        .unwrap();
    let (allocated,): (String,) = sqlx::query_as("SELECT allocated FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let allocated: Value = serde_json::from_str(&allocated).unwrap();
    assert_eq!(allocated["cpus"], 1);
    assert_eq!(allocated["memory_mb"], 512);
    assert_eq!(allocated["disk_gb"], 10);

    // The HTTP read shows the primary node, with no fence failure.
    let owner = server.owner().await;
    let res = owner.get(server.url("/api/v1/nodes")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    let node = &items[0];
    assert_eq!(node["id"].as_str(), Some(node_id.as_str()));
    assert_eq!(node["cp_status"].as_str(), Some("primary"));
    assert!(node["fence_failure"].is_null(), "{node}");
    assert_eq!(node["roles"], serde_json::json!(["node"]));
    assert_eq!(node["capacity"]["cpus"], 8);
    assert_eq!(node["kvm"]["available"], true);

    let res = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let node: Value = res.json().await.unwrap();
    assert_eq!(node["id"].as_str(), Some(node_id.as_str()));

    // A deleted VM is acknowledged without touching state tables.
    driver::report_deleted(&store, &node_id, &vm_id)
        .await
        .unwrap();
    let (deleted,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM events WHERE type = 'vm.deleted'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(deleted, 1);

    // The whole flow left a valid change log.
    let mut conn = store.pool().acquire().await.unwrap();
    assert_eq!(changelog::verify(&mut conn).await.unwrap(), None);
}
