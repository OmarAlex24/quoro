//! `GET/POST /api/v1/vms` and the lifecycle verbs, over real HTTP (issue #26).

mod common;

use common::{problem, start};
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode, Observation, ObservedStatus};
use quoro_types::{Prefix, is_id};
use serde_json::{Value, json};

const VIEWER_EMAIL: &str = "viewer@example.com";
const VIEWER_PASSWORD: &str = "a viewer password";

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
    (server, owner, node_id, project_id)
}

fn create_body(project_id: &str, node_id: &str) -> Value {
    json!({
        "project_id": project_id,
        "node_id": node_id,
        "name": "web",
        "vcpus": 2,
        "memory_mb": 512,
        "disk_gb": 8,
        "image": "ghcr.io/example/rootfs@sha256:0000000000000000000000000000000000000000000000000000000000000000"
    })
}

async fn audit_records(
    server: &common::Server,
    operation: &str,
    target_id: &str,
) -> Vec<(String, Option<String>)> {
    sqlx::query_as(
        "SELECT result, result_detail FROM audit_log WHERE operation = ? AND target_id = ? ORDER BY seq",
    )
    .bind(operation)
    .bind(target_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn vm_lifecycle_over_http() {
    let (server, owner, node_id, project_id) = setup().await;

    // create → 201 with the desired and observed status of a fresh VM.
    let res = owner
        .post(server.url("/api/v1/vms"))
        .json(&create_body(&project_id, &node_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "create: {}", res.text().await.unwrap());
    let vm: Value = res.json().await.unwrap();
    let vm_id = vm["id"].as_str().unwrap().to_string();
    assert!(is_id(Prefix::Vm, &vm_id), "{vm_id}");
    assert_eq!(vm["desired_status"], "running");
    assert_eq!(vm["observed_status"], "creating");
    assert_eq!(vm["project_id"], project_id);
    assert_eq!(vm["node_id"], node_id);
    assert!(vm["app_id"].is_null());
    assert_eq!(vm["restart_generation"], 0);
    assert_eq!(vm["restart_count"], 0);

    // list shows it.
    let res = owner.get(server.url("/api/v1/vms")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["id"], vm_id);
    assert!(list["next_cursor"].is_null());

    // filters.
    let res = owner
        .get(server.url(&format!("/api/v1/vms?project_id={project_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    let other = quoro_types::new_id(Prefix::Project);
    let res = owner
        .get(server.url(&format!("/api/v1/vms?project_id={other}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    assert!(list["items"].as_array().unwrap().is_empty());

    // stop → 202 desired stopped, then the node reports stopped.
    let res = owner
        .post(server.url(&format!("/api/v1/vms/{vm_id}/stop")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "stop: {}", res.text().await.unwrap());
    let stopped: Value = res.json().await.unwrap();
    assert_eq!(stopped["desired_status"], "stopped");

    driver::report_observed(
        &server.state.store,
        &node_id,
        &vm_id,
        &Observation {
            status: ObservedStatus::Stopped,
            ip: None,
            exit_code: None,
            restart_count: None,
        },
    )
    .await
    .unwrap();
    let res = owner
        .get(server.url(&format!("/api/v1/vms/{vm_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let vm: Value = res.json().await.unwrap();
    assert_eq!(vm["observed_status"], "stopped");

    // stop again: already desired, still 202 and still exactly one audit record
    // per call, but no second change-log entry.
    let before: (i64,) = sqlx::query_as("SELECT count(*) FROM change_log WHERE entity_type = 'vm'")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    let res = owner
        .post(server.url(&format!("/api/v1/vms/{vm_id}/stop")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let _: Value = res.json().await.unwrap();
    let after: (i64,) = sqlx::query_as("SELECT count(*) FROM change_log WHERE entity_type = 'vm'")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(
        before.0, after.0,
        "a no-op stop appends no change-log entry"
    );

    // start → 202.
    let res = owner
        .post(server.url(&format!("/api/v1/vms/{vm_id}/start")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "start: {}", res.text().await.unwrap());
    let started: Value = res.json().await.unwrap();
    assert_eq!(started["desired_status"], "running");

    // delete → 202 with the record as it was, desired stopped.
    let res = owner
        .delete(server.url(&format!("/api/v1/vms/{vm_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "delete: {}", res.text().await.unwrap());
    let deleted: Value = res.json().await.unwrap();
    assert_eq!(deleted["id"], vm_id);
    assert_eq!(deleted["desired_status"], "stopped");

    // list is empty, get is 404.
    let list: Value = owner
        .get(server.url("/api/v1/vms"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list["items"].as_array().unwrap().is_empty());
    let (status, code, _) = problem(
        owner
            .get(server.url(&format!("/api/v1/vms/{vm_id}")))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // one audit record per mutating call: create, two stops, start, delete.
    for (operation, expected) in [
        ("POST /api/v1/vms", 1),
        ("POST /api/v1/vms/{vm_id}/stop", 2),
        ("POST /api/v1/vms/{vm_id}/start", 1),
        ("DELETE /api/v1/vms/{vm_id}", 1),
    ] {
        let records = audit_records(&server, operation, &vm_id).await;
        assert_eq!(records.len(), expected, "{operation}: {records:?}");
        assert!(records.iter().all(|(result, _)| result == "ok"));
    }
}

#[tokio::test]
async fn vm_authorization_and_validation() {
    let (server, owner, node_id, project_id) = setup().await;

    // No credential: 401.
    let anonymous = server.client();
    let (status, code, _) = problem(
        anonymous
            .get(server.url("/api/v1/vms"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (401, "unauthenticated"));

    // A viewer reads but cannot create; the refusal is audited.
    server
        .insert_user(VIEWER_EMAIL, VIEWER_PASSWORD, "viewer")
        .await;
    let viewer = server.client();
    server.login(&viewer, VIEWER_EMAIL, VIEWER_PASSWORD).await;
    let res = viewer.get(server.url("/api/v1/vms")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let (status, code, reason) = problem(
        viewer
            .post(server.url("/api/v1/vms"))
            .json(&create_body(&project_id, &node_id))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        (status, code.as_str(), reason.as_deref()),
        (403, "forbidden", Some("scope_missing"))
    );
    let audit = server.audit().await;
    assert!(
        audit.iter().any(|(operation, result, detail)| {
            operation == "POST /api/v1/vms"
                && result == "refused"
                && detail.as_deref() == Some("scope_missing")
        }),
        "{audit:?}"
    );

    // disk_gb 0 is refused (Q-0088), and nothing is inserted.
    let mut body = create_body(&project_id, &node_id);
    body["disk_gb"] = json!(0);
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/vms"))
            .json(&body)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // An unknown project is refused.
    let ghost = quoro_types::new_id(Prefix::Project);
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/vms"))
            .json(&create_body(&ghost, &node_id))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // An unknown node is refused.
    let ghost = quoro_types::new_id(Prefix::Node);
    let (status, code, _) = problem(
        owner
            .post(server.url("/api/v1/vms"))
            .json(&create_body(&project_id, &ghost))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (422, "validation_failed"));

    // Idempotent create: the same key and body return the same VM, one row.
    let body = create_body(&project_id, &node_id);
    let first = owner
        .post(server.url("/api/v1/vms"))
        .header("Idempotency-Key", "create-web-once")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201, "{}", first.text().await.unwrap());
    let first: Value = first.json().await.unwrap();
    let second = owner
        .post(server.url("/api/v1/vms"))
        .header("Idempotency-Key", "create-web-once")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 201);
    let second: Value = second.json().await.unwrap();
    assert_eq!(first["id"], second["id"]);
    let rows: (i64,) = sqlx::query_as("SELECT count(*) FROM vms")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(rows.0, 1);

    // A malformed path id is a 404, not a 422.
    let (status, code, _) = problem(
        owner
            .get(server.url("/api/v1/vms/nope"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((status, code.as_str()), (404, "not_found"));

    // The create and its replay are both audited `ok` (IdempotencyKey: D-0031
    // records the call either way); only the create points at a change.
    let vm_id = first["id"].as_str().unwrap().to_string();
    let records = audit_records(&server, "POST /api/v1/vms", &vm_id).await;
    assert_eq!(records, vec![("ok".into(), None), ("ok".into(), None)]);
    let seqs: Vec<(Option<i64>,)> = sqlx::query_as(
        "SELECT change_seq FROM audit_log WHERE operation = 'POST /api/v1/vms' AND target_id = ? ORDER BY seq",
    )
    .bind(&vm_id)
    .fetch_all(server.state.store.pool())
    .await
    .unwrap();
    assert!(seqs[0].0.is_some() && seqs[1].0.is_none(), "{seqs:?}");
}
