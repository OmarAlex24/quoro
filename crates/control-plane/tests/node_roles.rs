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

/// Reads `machine`'s frames until its newest `spec.votes` carries
/// `version`, and until the primary handled its acknowledgement.
async fn holds_version(machine: &mut Machine, version: u64) {
    while machine.spec["votes"]["version"].as_u64() != Some(version) {
        machine.until("desired_state").await;
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

/// The newest change-log `seq` other than an audit record's: audit records
/// travel in the change log too (audit-log-format.md), and a refused or
/// no-op call is still recorded.
async fn state_head(server: &Server) -> i64 {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(seq) FROM change_log WHERE entity_type <> 'audit'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
    .unwrap_or(0)
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

// --- updateNodeRoles (ADR 0170) ---------------------------------------------

/// `POST /api/v1/nodes/{node_id}/roles` as `client`.
async fn roles(
    client: &reqwest::Client,
    server: &Server,
    node_id: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(server.url(&format!("/api/v1/nodes/{node_id}/roles")))
        .json(&body)
        .send()
        .await
        .unwrap()
}

/// `roles` that must answer `200`; returns the node.
async fn roles_ok(client: &reqwest::Client, server: &Server, node_id: &str, body: Value) -> Value {
    let res = roles(client, server, node_id, body.clone()).await;
    assert_eq!(res.status(), 200, "{body}: {}", res.text().await.unwrap());
    res.json().await.unwrap()
}

/// `(status, code)` of a refused `roles` call.
async fn roles_refused(
    client: &reqwest::Client,
    server: &Server,
    node_id: &str,
    body: Value,
) -> (u16, String) {
    let (status, code, _) = problem(roles(client, server, node_id, body).await).await;
    (status, code)
}

/// The newest audit record of `updateNodeRoles`: `(result, params,
/// change_seq)`.
async fn last_roles_audit(server: &Server) -> (String, Value, Option<i64>) {
    let (result, params, change_seq): (String, String, Option<i64>) = sqlx::query_as(
        "SELECT result, params, change_seq FROM audit_log \
         WHERE operation = 'POST /api/v1/nodes/{node_id}/roles' ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    (result, serde_json::from_str(&params).unwrap(), change_seq)
}

async fn audit_count(server: &Server) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE operation = 'POST /api/v1/nodes/{node_id}/roles'",
    )
    .fetch_one(server.state.store.pool())
    .await
    .unwrap()
}

async fn roles_of(server: &Server, node_id: &str) -> Value {
    let roles: String = sqlx::query_scalar("SELECT roles FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    serde_json::from_str(&roles).unwrap()
}

/// An agent's access token, under an agent the owner creates.
async fn agent_token(server: &Server, owner: &reqwest::Client) -> String {
    let me: Value = owner
        .get(server.url("/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let agent: Value = owner
        .post(server.url("/api/v1/agents"))
        .json(&json!({
            "name": "roles-operator",
            "model": "test-model",
            "owner_user_id": me["user"]["id"],
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key: Value = owner
        .post(server.url(&format!(
            "/api/v1/agents/{}/keys",
            agent["id"].as_str().unwrap()
        )))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token: Value = reqwest::Client::new()
        .post(server.url("/api/v1/auth/agent-token"))
        .header(
            "Authorization",
            format!("Bearer {}", key["key"].as_str().unwrap()),
        )
        .header("Quoro-Justification", "tests need a token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    token["access_token"].as_str().unwrap().to_string()
}

/// Inserts an `expiring` lease held by `holder`, as a warm app's would be.
async fn expiring_lease(server: &Server, holder: &str) {
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, fencing_token, \
         duration_ms, granted_at) VALUES ('lse_01JC9Z6Q5W8N3K4T7R2M1XHB0D', 'app', \
         'app_01JC9Z6Q5W8N3K4T7R2M1XHB0D', ?, 'expiring', 1, 15000, '2026-11-10T00:00:00.000000Z')",
    )
    .bind(holder)
    .execute(server.state.store.pool())
    .await
    .unwrap();
}

/// The refusals of `updateNodeRoles` in the order the brief and the
/// openapi text give, and the no-op `200`, each audited: human-only first,
/// then a malformed body, then an unknown node; a no-op answers `200` before
/// any later check (even on a removed node or while rule 2 waits); then a
/// change that would leave no role, a removed node, and rule 2.
#[tokio::test]
async fn the_roles_refusals_come_in_order_and_a_no_op_changes_nothing() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;
    let edge = enrolled(&server, &owner, "edge", &["edge"]).await;
    let witness = {
        let _machine_a = connected(&server, &a).await;
        enrolled(&server, &owner, "witness", &["witness"]).await
    };
    // Rule 2 now waits: nobody holds the witness's enrollment version.
    // A's session has ended; wait until the primary has recorded that, so
    // the change log moves only for the calls below.
    while node_status(&server, &a.node_id).await != "disconnected" {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // An agent is refused `human_only`, whatever it sends, and audited.
    let token = agent_token(&server, &owner).await;
    let res = reqwest::Client::new()
        .post(server.url(&format!("/api/v1/nodes/{}/roles", edge.node_id)))
        .header("Authorization", format!("Bearer {token}"))
        .header("Quoro-Justification", "try to take a vote")
        .json(&json!({ "add": ["witness"], "remove": ["witness"] }))
        .send()
        .await
        .unwrap();
    let (status, code, reason) = problem(res).await;
    assert_eq!((status, code.as_str()), (403, "forbidden"));
    assert_eq!(reason.as_deref(), Some("human_only"));
    assert_eq!(last_roles_audit(&server).await.0, "refused");

    // A malformed body: both, neither, a role that cannot change, an empty
    // or repeated list.
    for body in [
        json!({ "add": ["witness"], "remove": ["witness"] }),
        json!({}),
        json!({ "force": true }),
        json!({ "add": ["edge"] }),
        json!({ "remove": ["node"] }),
        json!({ "add": [] }),
        json!({ "add": ["witness", "witness"] }),
        json!({ "add": "witness" }),
    ] {
        let before = audit_count(&server).await;
        assert_eq!(
            roles_refused(&owner, &server, &edge.node_id, body.clone()).await,
            (422, "validation_failed".to_string()),
            "{body}"
        );
        assert_eq!(audit_count(&server).await, before + 1, "audited: {body}");
    }
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            "nod_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
            json!({ "add": ["witness"] })
        )
        .await,
        (404, "not_found".to_string())
    );

    // A no-op answers 200 before rule 2, recorded with no change_seq.
    let head = state_head(&server).await;
    let node = roles_ok(
        &owner,
        &server,
        &witness.node_id,
        json!({ "add": ["witness"] }),
    )
    .await;
    assert_eq!(node["roles"], json!(["witness"]));
    let node = roles_ok(
        &owner,
        &server,
        &a.node_id,
        json!({ "remove": ["witness"] }),
    )
    .await;
    assert_eq!(node["roles"], json!(["node"]));
    assert_eq!(state_head(&server).await, head, "nothing written");
    let (result, params, change_seq) = last_roles_audit(&server).await;
    assert_eq!((result.as_str(), change_seq), ("ok", None));
    assert_eq!(
        params,
        json!({ "remove": ["witness"] }),
        "params is the body"
    );

    // Removing the only role is a validation failure, before rule 2.
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &witness.node_id,
            json!({ "remove": ["witness"] })
        )
        .await,
        (422, "validation_failed".to_string())
    );

    // Rule 2: adding the edge's vote waits for a majority at the version.
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &edge.node_id,
            json!({ "add": ["witness"] })
        )
        .await,
        (409, "conflict".to_string())
    );
    assert_eq!(roles_of(&server, &edge.node_id).await, json!(["edge"]));
    assert_eq!(state_head(&server).await, head, "nothing written");

    // A removed node: a no-op still answers 200, a change is a conflict.
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&edge.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    roles_ok(
        &owner,
        &server,
        &edge.node_id,
        json!({ "remove": ["witness"] }),
    )
    .await;
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &edge.node_id,
            json!({ "add": ["witness"] })
        )
        .await,
        (409, "conflict".to_string())
    );
}

/// D-0170's guard: removing a vote that would leave fewer than three voters
/// is refused with `conflict` while any `expiring` lease exists, unless the
/// request sets `force`; `force` never bypasses rule 2. The machine's next
/// `desired_state` drops `spec.witness` and `spec.votes`.
#[tokio::test]
async fn removing_a_vote_below_three_with_expiring_leases_needs_force() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;
    let mut machine_a = connected(&server, &a).await;
    let b = enrolled(&server, &owner, "node-b", &["node"]).await;
    let edge = enrolled(&server, &owner, "edge", &["edge"]).await;
    let mut machine_b = connected(&server, &b).await;
    holds_voters(&mut machine_a, 2).await;
    let mut machine_edge = connected(&server, &edge).await;
    roles_ok(
        &owner,
        &server,
        &edge.node_id,
        json!({ "add": ["witness"] }),
    )
    .await;
    while machine_edge.spec["witness"].is_null() {
        machine_edge.until("desired_state").await;
    }
    machine_edge.settle().await;
    holds_voters(&mut machine_a, 3).await;
    let three = machine_a.version();

    // Without an `expiring` lease, removing the third vote needs no force;
    // it is not done here. With one, it does.
    expiring_lease(&server, &b.node_id).await;
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &edge.node_id,
            json!({ "remove": ["witness"] })
        )
        .await,
        (409, "conflict".to_string())
    );
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &edge.node_id,
            json!({ "remove": ["witness"], "force": false })
        )
        .await,
        (409, "conflict".to_string())
    );
    assert_eq!(
        roles_of(&server, &edge.node_id).await,
        json!(["edge", "witness"])
    );

    // `force` removes it.
    let node = roles_ok(
        &owner,
        &server,
        &edge.node_id,
        json!({ "remove": ["witness"], "force": true }),
    )
    .await;
    assert_eq!(node["roles"], json!(["edge"]));
    let (_, params, change_seq) = last_roles_audit(&server).await;
    assert_eq!(params, json!({ "remove": ["witness"], "force": true }));
    let removed_at = u64::try_from(change_seq.unwrap()).unwrap();
    assert!(removed_at > three);

    // The edge's next desired state drops both fields.
    while !machine_edge.spec["votes"].is_null() {
        machine_edge.until("desired_state").await;
    }
    assert!(machine_edge.spec["witness"].is_null());

    // Rule 2 waits again, and `force` does not bypass it: nobody holds the
    // version of the removal yet... until A and B acknowledge it.
    assert_eq!(
        roles_refused(
            &owner,
            &server,
            &edge.node_id,
            json!({ "add": ["witness"], "force": true })
        )
        .await,
        (409, "conflict".to_string())
    );
    holds_version(&mut machine_a, removed_at).await;
    holds_version(&mut machine_b, removed_at).await;
    roles_ok(
        &owner,
        &server,
        &edge.node_id,
        json!({ "add": ["witness"] }),
    )
    .await;
}

/// Rule 2 for the roles operation counts against the version read in its own
/// write transaction: two role changes in a row never pass on the same
/// acknowledgements.
#[tokio::test]
async fn two_vote_changes_never_pass_on_the_same_acknowledgements() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enrolled(&server, &owner, "node-a", &["node"]).await;
    let _machine_a = connected(&server, &a).await;
    let e1 = enrolled(&server, &owner, "edge-1", &["edge"]).await;
    let e2 = enrolled(&server, &owner, "edge-2", &["edge"]).await;
    roles_ok(&owner, &server, &e1.node_id, json!({ "add": ["witness"] })).await;
    // A held only the version before e1's vote.
    assert_eq!(
        roles_refused(&owner, &server, &e2.node_id, json!({ "add": ["witness"] })).await,
        (409, "conflict".to_string())
    );
}
