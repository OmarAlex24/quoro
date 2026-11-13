//! `GET /api/v1/cluster` over real HTTP.

mod common;

use common::{problem, start};
use quoro_control_plane::driver::{self, Capacity, Kvm, LocalNode};
use quoro_types::{Prefix, is_id};
use serde_json::Value;

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

#[tokio::test]
async fn get_cluster_returns_identity_and_log_position() {
    let server = start().await;
    let owner = server.owner().await;
    let node_id = driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();

    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let id = cluster["id"].as_str().unwrap();
    assert!(is_id(Prefix::Cluster, id), "{id}");
    assert_eq!(id, server.state.cluster_id);
    assert_eq!(cluster["name"], "quoro");
    assert_eq!(cluster["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(cluster["primary_node_id"], node_id);
    assert_eq!(cluster["term"], 1);
    assert!(cluster["log_seq"].as_i64().unwrap() >= 1);
    let hash = cluster["log_head_hash"].as_str().unwrap();
    assert_eq!(hash.len(), 64);
    assert!(hash.bytes().all(|b| b.is_ascii_hexdigit()));
    assert!(!cluster["created_at"].as_str().unwrap().is_empty());

    // ADR 0170: the one `node` machine is the only voter, and HA is not ready.
    let votes = &cluster["votes"];
    assert_eq!(votes["count"], 1, "{cluster}");
    assert_eq!(votes["required"], 3);
    assert_eq!(votes["ha_ready"], false);
    assert_eq!(votes["voters"][0]["node_id"], node_id);
    assert_eq!(votes["voters"][0]["roles"], serde_json::json!(["node"]));
    assert!(votes["voters"][0]["connected"].is_boolean());

    let node: Value = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(node["lost_at"].is_null(), "{node}");
}

/// Sets `meta.term` to `value`.
async fn set_term(server: &common::Server, value: &str) {
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES ('term', ?) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(value)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// `term` is the term `meta` stores, which a promotion raises
/// (`node-agent-protocol.md` section 7.3 step 5). A stored value that holds
/// no term answers `500`, so no term above 2^53 - 1 is reported (section
/// 8.8).
#[tokio::test]
async fn get_cluster_reports_the_term_in_meta() {
    let server = start().await;
    let owner = server.owner().await;
    driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();

    set_term(&server, "7").await;
    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cluster["term"], 7, "{cluster}");

    for value in ["9007199254740992", "+7", "007", "seven"] {
        set_term(&server, value).await;
        let res = owner
            .get(server.url("/api/v1/cluster"))
            .send()
            .await
            .unwrap();
        let (status, code, _) = problem(res).await;
        assert_eq!((status, code.as_str()), (500, "internal"), "{value:?}");
    }
}

#[tokio::test]
async fn votes_count_node_and_witness_machines_but_not_an_edge_alone() {
    let server = start().await;
    let owner = server.owner().await;
    let node_id = driver::register_local_node(&server.state.store, &local_node())
        .await
        .unwrap();
    for (id, roles, status) in [
        (
            "nod_01JCA0A1B2C3D4E5F6G7H8J9KM",
            r#"["edge","witness"]"#,
            "connected",
        ),
        ("nod_01JCA0A1B2C3D4E5F6G7H8J9KN", r#"["edge"]"#, "connected"),
        (
            "nod_01JCA0A1B2C3D4E5F6G7H8J9KP",
            r#"["witness"]"#,
            "removed",
        ),
    ] {
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, status, public_key, behind_nat, labels, \
             capacity, allocated, kvm, created_at) \
             VALUES (?, ?, ?, ?, 'key', 0, '{}', '{}', '{}', '{}', '2099-01-01T00:00:00Z')",
        )
        .bind(id)
        .bind(id)
        .bind(roles)
        .bind(status)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    }

    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let votes = &cluster["votes"];
    assert_eq!(votes["count"], 2, "{cluster}");
    assert_eq!(votes["voters"][0]["node_id"], node_id);
    assert_eq!(
        votes["voters"][1]["node_id"],
        "nod_01JCA0A1B2C3D4E5F6G7H8J9KM"
    );
    assert_eq!(
        votes["voters"][1]["roles"],
        serde_json::json!(["edge", "witness"])
    );
    assert_eq!(votes["voters"][1]["connected"], true);
    assert_eq!(votes["ha_ready"], false);
}
