//! `GET /api/v1/cluster` over real HTTP.

mod common;

use common::start;
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
}
