// SPDX-License-Identifier: AGPL-3.0-only
//! Opening votes (`node-agent-protocol.md` sections 5.1, 6.12 and 8.9,
//! migration 0015): the primary sets `listen: true` in every voter's
//! `spec.votes` once every machine with the `node` role that is not removed
//! reports mesh rules of version 2 in its last `hello`, leaves it out
//! otherwise, and sends every voter a new desired state when that changes.
//! The machines are fake ones that answer as the node agent does.

mod common;

use common::machine::{Machine, Node, enroll, hello_body};
use common::{Server, restart_with_resolver, start_with_patient_node_session};
use quoro_control_plane::changelog::{self, Actor, Op};
use serde_json::{Value, json};
use std::time::Duration;

/// Dials as `node` with a `hello` that reports `mesh_rules`, and reads up
/// to the first `desired_state`.
async fn connect(server: &Server, node: &Node, mesh_rules: Option<u64>) -> Machine {
    let mut hello = hello_body(&node.node_id);
    hello.mesh_rules = mesh_rules;
    let mut machine = Machine::connect_with(server, node, hello, true, false).await;
    machine.settle().await;
    machine
}

/// `spec.votes.listen` of `machine`'s newest desired state.
fn listen(machine: &Machine) -> Value {
    machine.spec["votes"]
        .get("listen")
        .cloned()
        .unwrap_or(Value::Null)
}

/// Reads `machine`'s frames until a new `desired_state` carries
/// `spec.votes.listen` as `open` says: `true`, or absent. A `false` is never
/// sent.
async fn holds(machine: &mut Machine, open: bool) {
    let wanted = if open { json!(true) } else { Value::Null };
    while listen(machine) != wanted {
        let frame = machine.until("desired_state").await;
        assert_ne!(
            frame["body"]["spec"]["votes"].get("listen"),
            Some(&json!(false)),
            "{frame}"
        );
    }
    machine.settle().await;
}

async fn node_status(server: &Server, node_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap()
}

/// Drops `machine` and waits until the primary recorded the end of its
/// session.
async fn leave(server: &Server, machine: Machine, node_id: &str) {
    drop(machine);
    tokio::time::timeout(Duration::from_secs(10), async {
        while node_status(server, node_id).await != "disconnected" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the session ended");
}

/// Removes `node_id` as section 10 does: `status` `removed`, written with its
/// change-log entry.
async fn remove(server: &Server, node_id: &str) {
    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(node_id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    changelog::append(
        &mut w.tx,
        &Actor::system("test"),
        "node",
        node_id,
        Op::Update,
        json!({ "id": node_id, "status": "removed" }),
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// Section 6.12: votes stay closed while one voter with the `node` role has
/// never connected or reports version 1, and open once every one reports 2;
/// a witness-only voter and an edge-only machine do not count, whatever
/// they report. Every change of the result sends every voter a new desired
/// state: with `listen: true` while votes are open, without `listen`
/// otherwise. A `hello` that reports 1 closes them again, removing the
/// lagging node opens them, and enrolling a new node-role voter, which has
/// never connected, closes them.
#[tokio::test]
async fn votes_open_only_while_every_node_voter_reports_version_2() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let w = enroll(&server, &owner, "witness-w", &["witness"]).await;
    let e = enroll(&server, &owner, "edge-e", &["edge"]).await;

    // B has never connected: NULL keeps votes closed.
    let mut on_a = connect(&server, &a, Some(2)).await;
    assert_eq!(listen(&on_a), Value::Null, "{}", on_a.spec);
    let mut on_w = connect(&server, &w, None).await;
    assert_eq!(listen(&on_w), Value::Null, "{}", on_w.spec);
    let on_e = connect(&server, &e, Some(1)).await;
    assert!(on_e.spec.get("votes").is_none(), "{}", on_e.spec);

    // B reports 1: still closed.
    let on_b = connect(&server, &b, Some(1)).await;
    assert_eq!(listen(&on_b), Value::Null, "{}", on_b.spec);
    leave(&server, on_b, &b.node_id).await;
    on_a.settle().await;
    assert_eq!(listen(&on_a), Value::Null, "{}", on_a.spec);

    // B reports 2: every node-role voter does, so votes open, and every
    // voter gets a new desired state with `listen: true`.
    let on_b = connect(&server, &b, Some(2)).await;
    assert_eq!(listen(&on_b), json!(true), "{}", on_b.spec);
    holds(&mut on_a, true).await;
    holds(&mut on_w, true).await;

    // B's next `hello` reports 1: closed again, and every voter hears it.
    leave(&server, on_b, &b.node_id).await;
    let on_b = connect(&server, &b, Some(1)).await;
    assert_eq!(listen(&on_b), Value::Null, "{}", on_b.spec);
    holds(&mut on_a, false).await;
    holds(&mut on_w, false).await;

    // Removing the lagging node opens them.
    leave(&server, on_b, &b.node_id).await;
    remove(&server, &b.node_id).await;
    holds(&mut on_a, true).await;
    holds(&mut on_w, true).await;
    assert_eq!(
        on_a.spec["votes"]["voters"].as_array().map(Vec::len),
        Some(2),
        "{}",
        on_a.spec
    );

    // A new node-role voter has never connected: closed.
    enroll(&server, &owner, "node-c", &["node"]).await;
    holds(&mut on_a, false).await;
    holds(&mut on_w, false).await;
    assert_eq!(
        on_a.spec["votes"]["voters"].as_array().map(Vec::len),
        Some(3),
        "{}",
        on_a.spec
    );
}

/// Section 6.12: the primary checks at its start. Over a database where
/// every node-role voter reported version 2, a restarted primary sends
/// `listen: true` in the first desired state of a witness, whose own `hello`
/// does not count, and of a node, while the other node has not connected
/// since the restart: the stored `nodes.mesh_rules` decide.
#[tokio::test]
async fn a_restarted_primary_opens_votes_from_the_stored_mesh_rules() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let w = enroll(&server, &owner, "witness-w", &["witness"]).await;
    let on_a = connect(&server, &a, Some(2)).await;
    let on_b = connect(&server, &b, Some(2)).await;
    assert_eq!(listen(&on_b), json!(true), "{}", on_b.spec);
    leave(&server, on_a, &a.node_id).await;
    leave(&server, on_b, &b.node_id).await;

    let restarted = restart_with_resolver(
        &server,
        std::sync::Arc::new(quoro_control_plane::ReferenceAsDigest),
    )
    .await;
    let on_w = connect(&restarted, &w, None).await;
    assert_eq!(listen(&on_w), json!(true), "{}", on_w.spec);
    let on_a = connect(&restarted, &a, Some(2)).await;
    assert_eq!(listen(&on_a), json!(true), "{}", on_a.spec);
    assert_eq!(node_status(&restarted, &b.node_id).await, "disconnected");
}
