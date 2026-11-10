//! `allocated` of a node is summed from the VMs placed on it at read time
//! (issue #187): a joined node reports its VMs, and a delete frees them again.

mod common;

use common::{Server, start};
use quoro_proto::sign;
use serde_json::{Value, json};

/// Issues a join token and enrolls a non-local node with it.
async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str, seed: u8) -> String {
    // Section 8.10 rule 2: these tests enroll voters back to back.
    common::settle_voters(server).await;
    let key = sign::signing_key_from_seed(&[seed; 32]);
    let res = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create join token: {}",
        res.text().await.unwrap()
    );
    let token: Value = res.json().await.unwrap();

    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token["token"],
            "name": name,
            "public_key": sign::public_key_to_base64(&key.verifying_key()),
            "roles": ["node"],
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 100 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body: Value = res.json().await.unwrap();
    assert_eq!(status, 201, "enroll: {body}");
    body["node_id"].as_str().unwrap().to_string()
}

async fn project(server: &Server, owner: &reqwest::Client) -> String {
    let res = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "alloc", "name": "Alloc" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create project: {}",
        res.text().await.unwrap()
    );
    let body: Value = res.json().await.unwrap();
    body["id"].as_str().unwrap().to_string()
}

async fn node_json(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let res = owner
        .get(server.url(&format!("/api/v1/nodes/{node_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

/// The node's entry in `GET /api/v1/nodes`.
async fn listed_node(server: &Server, owner: &reqwest::Client, node_id: &str) -> Value {
    let res = owner.get(server.url("/api/v1/nodes")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let list: Value = res.json().await.unwrap();
    list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == node_id)
        .unwrap_or_else(|| panic!("node {node_id} is not listed: {list}"))
        .clone()
}

fn zero() -> Value {
    json!({ "cpus": 0, "memory_mb": 0, "disk_gb": 0 })
}

#[tokio::test]
async fn a_joined_node_reports_the_vms_placed_on_it() {
    let server = start().await;
    let owner = server.owner().await;
    let node_id = enrolled(&server, &owner, "node-alloc-a", 9).await;
    let project_id = project(&server, &owner).await;

    // A fresh joined node owns nothing.
    let node = node_json(&server, &owner, &node_id).await;
    assert_eq!(node["cp_status"], Value::Null, "{node}");
    assert_eq!(node["allocated"], zero(), "{node}");

    // A VM placed on it shows up in both reads, with its own numbers.
    let res = owner
        .post(server.url("/api/v1/vms"))
        .json(&json!({
            "project_id": project_id,
            "node_id": node_id,
            "name": "web",
            "vcpus": 2,
            "memory_mb": 512,
            "disk_gb": 4,
            "image": "ghcr.io/example/rootfs:1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        201,
        "create vm: {}",
        res.text().await.unwrap()
    );
    let vm: Value = res.json().await.unwrap();
    let vm_id = vm["id"].as_str().unwrap();

    let node = node_json(&server, &owner, &node_id).await;
    assert_eq!(node["allocated"]["cpus"], 2, "{node}");
    assert_eq!(node["allocated"]["memory_mb"], 512, "{node}");
    assert_eq!(node["allocated"]["disk_gb"], 4, "{node}");
    assert_eq!(
        listed_node(&server, &owner, &node_id).await["allocated"],
        node["allocated"]
    );

    // A second joined node is listed beside it and still owns nothing.
    let other_id = enrolled(&server, &owner, "node-alloc-b", 11).await;
    let other = listed_node(&server, &owner, &other_id).await;
    assert_eq!(other["allocated"], zero(), "{other}");
    assert_eq!(
        listed_node(&server, &owner, &node_id).await["allocated"],
        node["allocated"]
    );

    // Deleting the VM frees the allocation without waiting for the node.
    let res = owner
        .delete(server.url(&format!("/api/v1/vms/{vm_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202);
    let node = node_json(&server, &owner, &node_id).await;
    assert_eq!(node["allocated"], zero(), "{node}");
    assert_eq!(
        listed_node(&server, &owner, &node_id).await["allocated"],
        zero()
    );
}
