// SPDX-License-Identifier: AGPL-3.0-only
//! Voter-set changes (`node-agent-protocol.md` sections 2.3, 6.12 and 8.10,
//! ADR 0170): rule 2 for the enrollment of a voter, which waits for a
//! majority of the current voters to hold the current version without
//! consuming its token. Every acknowledgement here comes from a real node
//! session; these tests never call `common::settle_voters`.

mod common;

use common::machine::{Machine, Node};
use common::{Server, problem, start_with_patient_node_session};
use serde_json::{Value, json};

/// A join token's secret.
async fn join_token(server: &Server, owner: &reqwest::Client) -> String {
    let body: Value = owner
        .post(server.url("/api/v1/nodes/join-tokens"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["token"].as_str().unwrap().to_string()
}

/// `POST /api/v1/nodes/enroll` with `token`, as `name` with `roles`.
async fn enroll_with(
    server: &Server,
    token: &str,
    name: &str,
    roles: &[&str],
) -> reqwest::Response {
    server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token,
            "name": name,
            "public_key": quoro_proto::sign::public_key_to_base64(
                &quoro_proto::sign::signing_key_from_seed(&[3u8; 32]).verifying_key()
            ),
            "roles": roles,
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap()
}

/// Enrolls `name` with a new token, which must succeed.
async fn enrolled(server: &Server, owner: &reqwest::Client, name: &str, roles: &[&str]) -> Node {
    let token = join_token(server, owner).await;
    let res = enroll_with(server, &token, name, roles).await;
    assert_eq!(
        res.status(),
        201,
        "enroll {name}: {}",
        res.text().await.unwrap()
    );
    let body: Value = res.json().await.unwrap();
    Node {
        node_id: body["node_id"].as_str().unwrap().to_string(),
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

/// A session of `node` that has acknowledged its desired states so far, and
/// whose acknowledgements the primary has handled.
async fn connected(server: &Server, node: &Node) -> Machine {
    let mut machine = Machine::connect(server, node).await;
    machine.settle().await;
    machine
}

/// Reads `machine`'s frames until its newest `spec.votes` names `count`
/// voters, and until the primary handled its acknowledgement.
async fn holds_voters(machine: &mut Machine, count: usize) {
    while machine.spec["votes"]["voters"]
        .as_array()
        .map_or(0, Vec::len)
        != count
    {
        machine.until("desired_state").await;
    }
    machine.settle().await;
}

async fn log_head(server: &Server) -> i64 {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(seq) FROM change_log")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
        .unwrap_or(0)
}

/// Whether the join token `secret` is still unused.
async fn token_unused(server: &Server, secret: &str) -> bool {
    let used: Option<String> =
        sqlx::query_scalar("SELECT used_at FROM node_join_tokens WHERE token_hash = ?")
            .bind(quoro_control_plane::store::sha256_hex(secret.as_bytes()))
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    used.is_none()
}

/// Sections 2.3 and 8.10 rule 2: the first voter enrolls at once. The next
/// voter's enrollment answers `409 conflict` until a majority of the current
/// voters has acknowledged the current version, and that refusal writes
/// nothing and leaves the token unused, so the same token enrolls once the
/// voters caught up. An edge-only enrollment changes no vote and is never
/// held back.
#[tokio::test]
async fn a_voter_enrolls_only_once_a_majority_holds_the_current_version() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;

    // The first voter: an empty set may change.
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;

    // Nobody holds the version of A's enrollment yet.
    let token_b = join_token(&server, &owner).await;
    let head = log_head(&server).await;
    let res = enroll_with(&server, &token_b, "node-b", &["node"]).await;
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str(), reason), (409, "conflict", None));
    assert!(token_unused(&server, &token_b).await, "not consumed");
    assert_eq!(log_head(&server).await, head, "nothing written");
    let named: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM nodes WHERE name = 'node-b'")
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(named, 0);

    // An edge-only machine does not vote, so it enrolls meanwhile.
    enrolled(&server, &owner, "edge", &["edge"]).await;

    // A's session acknowledges a desired state carrying the version: 1 of 1.
    let mut machine_a = connected(&server, &a).await;
    let res = enroll_with(&server, &token_b, "node-b", &["node"]).await;
    assert_eq!(
        res.status(),
        201,
        "the same token: {}",
        res.text().await.unwrap()
    );
    let b: Value = res.json().await.unwrap();
    let b = Node {
        node_id: b["node_id"].as_str().unwrap().to_string(),
        credential: b["credential"].as_str().unwrap().to_string(),
    };

    // Two changes never pass on the same acknowledgements: B's enrollment
    // moved the version, which A held only before.
    let token_w = join_token(&server, &owner).await;
    let res = enroll_with(&server, &token_w, "witness", &["witness"]).await;
    assert_eq!(problem(res).await.0, 409);
    // A holds the new version: 1 of 2 is no majority.
    holds_voters(&mut machine_a, 2).await;
    let set_version = machine_a.version();
    let res = enroll_with(&server, &token_w, "witness", &["witness"]).await;
    assert_eq!(problem(res).await.0, 409, "1 of 2 at version {set_version}");
    assert!(token_unused(&server, &token_w).await);

    // B's session makes it 2 of 2: a witness-only machine enrolls.
    let machine_b = connected(&server, &b).await;
    assert_eq!(machine_b.version(), set_version);
    let res = enroll_with(&server, &token_w, "witness", &["witness"]).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
}

/// Rule 2 counts the whole voter set, connected or not: a voter that
/// acknowledged once and then left still counts at that version, and one
/// that never connected never counts.
#[tokio::test]
async fn rule_2_counts_every_voter_connected_or_not() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;
    let machine_a = connected(&server, &a).await;
    // A holds the version and leaves: its acknowledgement still counts.
    drop(machine_a);
    let b = enrolled(&server, &owner, "node-b", &["node"]).await;
    let _machine_b = connected(&server, &b).await;

    // At B's version only B holds it; A never saw it: 1 of 2.
    let token = join_token(&server, &owner).await;
    let res = enroll_with(&server, &token, "node-c", &["node"]).await;
    assert_eq!(problem(res).await.0, 409);
    // A reconnects and acknowledges: 2 of 2.
    let _machine_a = connected(&server, &a).await;
    let res = enroll_with(&server, &token, "node-c", &["node"]).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
}
