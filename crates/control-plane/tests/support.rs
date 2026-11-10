//! Voter support, the primary's side (`node-agent-protocol.md` sections 4.2
//! rule 7, 5.27, 5.28, 6.12, 8.8 and 8.10; ADR 0169, ADR 0170, ADR 0263;
//! issue #686), over real HTTP and WebSocket with fake machines that answer
//! as the node agent does: each acknowledges every `desired_state` and keeps
//! its `spec.votes.version`, and the test decides which `support_request` it
//! answers and how. Support is measured on a manual `CLOCK_BOOTTIME`, and no
//! test here sets the authority's support by hand.

mod common;

use common::machine::{Machine, echo, enroll, manual_clock, ok_result};
use common::{Server, start_with_patient_node_session};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_proto::wire::{
    Capacity as WireCapacity, Heartbeat, NodeMessage, StatusReport, VolumeState as WireVolumeState,
    VolumeStatus,
};
use quoro_proto::{ErrorCode, LeaseId, LeaseRenew, ProtocolError};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// `D`, and the `S` and `L` every request carries.
const D: u64 = 15_000;

/// `S - M_S`: how long after `p_send` a support counts.
const WINDOW: Duration = Duration::from_millis(13_500);

fn supports(server: &Server, duration_ms: u64) -> bool {
    server.state.hub.authority().supports(duration_ms)
}

/// The `seq` of `node_id`'s create entry: its enrollment.
async fn create_seq(server: &Server, node_id: &str) -> u64 {
    let seq: i64 = sqlx::query_scalar(
        "SELECT seq FROM change_log WHERE entity_type = 'node' AND entity_id = ? AND op = 'create'",
    )
    .bind(node_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    u64::try_from(seq).unwrap()
}

async fn cluster_votes(server: &Server, owner: &reqwest::Client) -> Value {
    let cluster: Value = owner
        .get(server.url("/api/v1/cluster"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cluster["votes"].clone()
}

/// Sections 6.12 and 8.10: `spec.votes` goes to every voter and names every
/// voter, oldest enrolled first, with port 51821. `spec.witness` goes to a
/// machine with the `witness` role, and neither to an edge-only machine. The
/// version is the `seq` of the entry that last changed the set, so it moves
/// with an enrollment of a voter and a removal, and not with an enrollment
/// of an edge-only machine or a node's own updates. A change of the set
/// rebuilds every voter's desired state.
#[tokio::test]
async fn spec_votes_name_every_voter_and_move_only_with_the_voter_set() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    assert_eq!(
        on_a.spec["votes"],
        json!({
            "version": create_seq(&server, &a.node_id).await,
            "port": 51821,
            "voters": [{ "node_id": a.node_id, "roles": ["node"], "mesh_address": null }],
        })
    );
    assert!(on_a.spec.get("witness").is_none(), "{}", on_a.spec);

    // A witness-only machine joins: every voter's desired state names it.
    let w = enroll(&server, &owner, "witness-w", &["witness"]).await;
    let joined = create_seq(&server, &w.node_id).await;
    // A witness gets its mesh address when it enrolls (section 6.12).
    let w_mesh: Option<String> = sqlx::query_scalar("SELECT mesh_address FROM nodes WHERE id = ?")
        .bind(&w.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert!(w_mesh.is_some());
    while on_a.spec["votes"]["voters"].as_array().unwrap().len() < 2 {
        on_a.until("desired_state").await;
    }
    let votes = json!({
        "version": joined,
        "port": 51821,
        "voters": [
            { "node_id": a.node_id, "roles": ["node"], "mesh_address": null },
            { "node_id": w.node_id, "roles": ["witness"], "mesh_address": w_mesh },
        ],
    });
    assert_eq!(on_a.spec["votes"], votes);
    let mut on_w = Machine::connect(&server, &w).await;
    assert_eq!(on_w.spec["votes"], votes);
    assert_eq!(on_w.spec["witness"], json!({}));
    // The same voters, in the same order, as `Cluster.votes`.
    let ids = |voters: &Value| -> Vec<Value> {
        voters
            .as_array()
            .unwrap()
            .iter()
            .map(|voter| voter["node_id"].clone())
            .collect()
    };
    assert_eq!(
        ids(&votes["voters"]),
        ids(&cluster_votes(&server, &owner).await["voters"])
    );

    // An edge-only machine is no voter, and neither its enrollment nor any
    // node's own updates (its session's start and end) move the version.
    let e = enroll(&server, &owner, "edge-e", &["edge"]).await;
    let on_e = Machine::connect(&server, &e).await;
    assert!(on_e.spec.get("votes").is_none(), "{}", on_e.spec);
    assert!(on_e.spec.get("witness").is_none(), "{}", on_e.spec);
    drop(on_e);
    on_w.settle().await;
    drop(on_w);
    let on_w = Machine::connect(&server, &w).await;
    assert_eq!(on_w.spec["votes"], votes, "a reconnect changes nothing");
    for frame in on_a.settle().await {
        if frame["type"] == "desired_state" {
            assert_eq!(frame["body"]["spec"]["votes"], votes, "{frame}");
        }
    }

    // A removal moves the version to its own entry, and the voter left gets
    // a desired state without it.
    drop(on_w);
    let mut w_tx = server.state.store.begin_write().await.unwrap();
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&w.node_id)
        .execute(&mut *w_tx.tx)
        .await
        .unwrap();
    let removed = changelog::append(
        &mut w_tx.tx,
        &Actor::system("test"),
        "node",
        &w.node_id,
        Op::Update,
        json!({ "id": w.node_id, "status": "removed" }),
    )
    .await
    .unwrap();
    w_tx.commit().await.unwrap();
    while on_a.spec["votes"]["voters"].as_array().unwrap().len() != 1 {
        on_a.until("desired_state").await;
    }
    assert_eq!(on_a.version(), u64::try_from(removed).unwrap());
}

/// Sections 4.2 rule 7 and 5.27: a voter's first `support_request` follows
/// its acknowledgement of a `desired_state` with `spec.votes`, then one goes
/// out every `S / 3`, with the cluster term, the `welcome`'s
/// `primary_node_id`, `S` = `L` = 15 000 ms and the current version. A
/// change of the version reaches it with the next acknowledged desired
/// state. A machine that is not a voter gets none.
#[tokio::test]
async fn support_requests_follow_the_acknowledged_desired_state_every_five_seconds() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let e = enroll(&server, &owner, "edge-e", &["edge"]).await;
    let mut on_e = Machine::connect(&server, &e).await;
    let mut on_a = Machine::connect_acking(&server, &a, false).await;
    on_a.settle().await;
    assert!(
        on_a.requests.is_empty(),
        "no request before the acknowledgement"
    );

    on_a.start_acking().await;
    let (first_at, first) = on_a.request().await;
    assert_eq!(
        first["body"],
        json!({
            "term": 1,
            "primary_node_id": on_a.primary,
            "support_ms": 15_000,
            "max_lease_ms": 15_000,
            "voters_version": on_a.version(),
        })
    );
    let (second_at, second) = on_a.request().await;
    // `S / 3` is 5 s. The gap is read on the test's side of a loaded host,
    // so it is checked loosely: well after the first request, well before `S`.
    let gap = second_at - first_at;
    assert!(
        gap >= Duration::from_secs(4) && gap < Duration::from_secs(12),
        "every S / 3: {gap:?}"
    );
    assert_eq!(second["body"], first["body"]);
    assert_ne!(second["id"], first["id"]);

    let frames = on_e.settle().await;
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "support_request"),
        "no request to an edge-only machine: {frames:?}"
    );
    assert!(on_e.requests.is_empty());

    // A second voter enrolls: the next request after A's acknowledgement of
    // the desired state that says so carries the new version.
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let moved = create_seq(&server, &b.node_id).await;
    while on_a.version() != moved {
        on_a.until("desired_state").await;
    }
    on_a.requests.clear();
    let (_, request) = on_a.request().await;
    assert_eq!(request["body"]["voters_version"], moved);
}

/// Section 8.8, "Majority": more than half of the whole voter set, connected
/// or not. With three voters, of which the third never connects, one
/// answer is not a majority and two are. A majority with `L` = `D` does not
/// support a longer grant. `Cluster.votes.ha_ready` follows it, and the
/// support ends at `p_send + S - M_S`.
#[tokio::test]
async fn two_of_three_voters_are_a_majority_with_the_third_disconnected() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    assert!(!supports(&server, D));

    on_a.support().await;
    assert!(!supports(&server, D), "1 of 3");
    let votes = cluster_votes(&server, &owner).await;
    assert_eq!(
        (votes["count"].as_i64(), votes["ha_ready"].as_bool()),
        (Some(3), Some(false))
    );

    on_b.support().await;
    assert!(supports(&server, D), "2 of 3");
    assert!(!supports(&server, D + 1), "L < d");
    let votes = cluster_votes(&server, &owner).await;
    assert_eq!(votes["ha_ready"], true, "{votes}");
    let disconnected = votes["voters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|voter| voter["node_id"] == c.node_id.as_str())
        .unwrap();
    assert_eq!(disconnected["connected"], false);

    clock.advance(WINDOW);
    assert!(!supports(&server, D), "the window ran out");
    assert_eq!(cluster_votes(&server, &owner).await["ha_ready"], false);
}

/// Section 8.8, "Majority": with two voters both must support, and
/// `ha_ready` stays false below three voters.
#[tokio::test]
async fn two_voters_need_both() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    on_a.support().await;
    assert!(!supports(&server, D), "1 of 2");
    on_b.support().await;
    assert!(supports(&server, D), "2 of 2");
    assert_eq!(cluster_votes(&server, &owner).await["ha_ready"], false);
}

/// Sections 5.28 and 8.10 rule 3: an answer counts only when it pairs by
/// `re` with an outstanding request, echoes its term, `S` and `L`, and
/// carries the primary's voter-set version. A mis-echoed answer uses up its
/// request, and so does a refusal.
#[tokio::test]
async fn an_answer_counts_only_when_it_pairs_echoes_and_carries_the_current_version() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let version = on_a.version();

    let (_, request) = on_a.request().await;
    let re = request["id"].as_str().unwrap().to_string();
    on_a.answer(echo(&request, version), None).await;
    assert!(!supports(&server, D), "no re");
    on_a.answer(
        echo(&request, version),
        Some("msg_01JC9Z6Q5W8N3K4T7R2M9999XX"),
    )
    .await;
    assert!(!supports(&server, D), "an re that pairs with nothing");
    let mut wrong = echo(&request, version);
    wrong.term += 1;
    on_a.answer(wrong, Some(&re)).await;
    assert!(!supports(&server, D), "another term");
    on_a.answer(echo(&request, version), Some(&re)).await;
    assert!(!supports(&server, D), "the request was used up");

    let (_, request) = on_a.request().await;
    on_a.answer(echo(&request, version - 1), request["id"].as_str())
        .await;
    assert!(!supports(&server, D), "another voter-set version");

    // Section 5.27: a voter that refuses a request with `bad_message` grants
    // nothing, and the refusal answers the request.
    let (_, request) = on_a.request().await;
    let re = request["id"].as_str().unwrap().to_string();
    on_a.send(
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::BadMessage,
            message: "primary_node_id is not the welcome's".to_string(),
            fatal: false,
            term: None,
        }),
        Some(&re),
    )
    .await;
    on_a.settle().await;
    assert!(!supports(&server, D), "a refusal");
    on_a.answer(echo(&request, version), Some(&re)).await;
    assert!(!supports(&server, D), "the refusal answered the request");

    let (_, request) = on_a.request().await;
    on_a.grant(&request).await;
    assert!(supports(&server, D), "1 of 1");
}

/// Section 8.8 and ADR 0263: support is measured from `p_send` on
/// `CLOCK_BOOTTIME`, never from the answer: an answer that arrives 10 s
/// after its request counts until 13.5 s after the request.
#[tokio::test]
async fn support_runs_out_at_p_send_plus_s_minus_m_s() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let (_, request) = on_a.request().await;
    clock.advance(Duration::from_secs(10));
    on_a.grant(&request).await;
    assert!(supports(&server, D));
    clock.advance(Duration::from_millis(3_499));
    assert!(supports(&server, D), "1 ms before the window ends");
    clock.advance(Duration::from_millis(1));
    assert!(!supports(&server, D), "at p_send + S - M_S");
}

/// Section 8.8, "Terms": after a `stale_term` from any node the primary
/// holds no support at once. It also demotes itself, so the session ends
/// with `not_primary` and no later answer can come (section 7.4; the
/// demotion itself is tested in `tests/demotion.rs`).
#[tokio::test]
async fn a_stale_term_stops_all_support_at_once() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    on_a.support().await;
    assert!(supports(&server, D));

    on_a.send(
        NodeMessage::Error(ProtocolError {
            code: ErrorCode::StaleTerm,
            message: "the term is lower than the highest seen".to_string(),
            fatal: true,
            term: Some(2),
        }),
        None,
    )
    .await;
    let ended = on_a.until("error").await;
    assert_eq!(ended["body"]["code"], "not_primary", "{ended}");
    assert!(!supports(&server, D), "stale_term stops all support");
}

/// Section 8.8, "Restarts": the primary keeps support in memory only, so a
/// restarted primary holds none until its voters answer its own requests.
#[tokio::test]
async fn no_support_survives_a_restart() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    on_a.support().await;
    assert!(supports(&server, D));

    drop(on_a);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status: String = sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
            .bind(&a.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
        if status == "disconnected" {
            break;
        }
        assert!(Instant::now() < deadline, "the session did not end");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let restarted =
        common::restart_with_resolver(&server, Arc::new(quoro_control_plane::ReferenceAsDigest))
            .await;
    manual_clock(&restarted);
    assert!(!supports(&restarted, D), "no support survives a restart");
    let mut on_a = Machine::connect(&restarted, &a).await;
    on_a.settle().await;
    assert!(!supports(&restarted, D), "not before the voter answers");
    on_a.support().await;
    assert!(supports(&restarted, D));
}

fn renew(lease_id: &str, token: u64) -> NodeMessage {
    NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: token,
    })
}

fn status_report(applied_rev: u64, volumes: Vec<VolumeStatus>) -> NodeMessage {
    NodeMessage::StatusReport(StatusReport {
        applied_rev,
        vms: Vec::new(),
        routes: Vec::new(),
        capacity: WireCapacity {
            cpus: 8,
            memory_mb: 16_384,
            disk_gb: 200,
        },
        allocated: WireCapacity::default(),
        disk_free_gb: 90.0,
        builds: Vec::new(),
        edge_backends: Vec::new(),
        backups: Vec::new(),
        images: Vec::new(),
        volumes,
        warm: Vec::new(),
        quarantines: Vec::new(),
    })
}

async fn heartbeat(machine: &mut Machine) {
    machine
        .send(
            NodeMessage::Heartbeat(Heartbeat {
                time: "2026-11-09T10:15:32Z".to_string(),
                busy: None,
            }),
            None,
        )
        .await;
}

async fn project(server: &Server, owner: &reqwest::Client) -> String {
    let project: Value = owner
        .post(server.url("/api/v1/projects"))
        .json(&json!({ "slug": "demo", "name": "demo" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    project["id"].as_str().unwrap().to_string()
}

async fn app(server: &Server, owner: &reqwest::Client, project_id: &str, name: &str) -> String {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn set_policy(server: &Server, app_id: &str, policy: &str) {
    sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
        .bind(policy)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

async fn create_volume(
    server: &Server,
    owner: &reqwest::Client,
    app_id: &str,
) -> reqwest::Response {
    owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap()
}

/// #603 under real support: with three voters, the holder and a second one
/// answering and the third never connected, the holder's renewal of its
/// `expiring` lease is answered. Once the voters stop answering and their
/// window runs out, the next renewal is not.
#[tokio::test]
async fn a_renewal_is_answered_while_two_of_three_voters_support_and_not_after() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    heartbeat(&mut on_a).await;

    // A stateful app on A, its lease made `expiring` for `D` in the row, as
    // a first `expiring` grant would leave it.
    let project_id = project(&server, &owner).await;
    let app_id = app(&server, &owner, &project_id, "web").await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let grant = on_a.until("lease_grant").await;
    let lease_id = grant["body"]["lease_id"].as_str().unwrap().to_string();
    let token = grant["body"]["fencing_token"].as_u64().unwrap();
    on_a.settle().await;
    let rev: i64 = sqlx::query_scalar("SELECT desired_rev FROM nodes WHERE id = ?")
        .bind(&a.node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    on_a.send(
        status_report(
            u64::try_from(rev).unwrap(),
            vec![VolumeStatus {
                volume_id,
                state: WireVolumeState::Ready,
                allocated_bytes: Some(4096),
                error: None,
            }],
        ),
        None,
    )
    .await;
    on_a.settle().await;
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();

    on_a.support().await;
    on_b.support().await;
    assert!(supports(&server, D));
    let id = on_a.send(renew(&lease_id, token), None).await;
    let answer = on_a.until("lease_grant").await;
    assert_eq!(answer["re"], id.as_str());
    assert_eq!(answer["body"]["fencing_token"], token);
    assert_eq!(answer["body"]["mode"], "expiring");
    assert_eq!(answer["body"]["duration_ms"], 15_000);

    // The voters answer nothing more, and the window runs out.
    clock.advance(WINDOW);
    assert!(!supports(&server, D));
    on_a.send(renew(&lease_id, token), None).await;
    let frames = on_a.settle().await;
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "lease_grant" || frame["type"] == "lease_revoke"),
        "{frames:?}"
    );
}

/// Gives `app_id` a released lease whose row reads `expiring` for `D`, as
/// one whose gate has passed: a first volume on A while the policy is
/// `none`, deleted and its revoke answered; then the row reads as an
/// `expiring` lease's, and the policy becomes `warm`. Returns the lease id.
async fn released_expiring_lease(
    server: &Server,
    owner: &reqwest::Client,
    on_a: &mut Machine,
    app_id: &str,
) -> String {
    set_policy(server, app_id, "none").await;
    let res = create_volume(server, owner, app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let first = on_a.until("lease_grant").await;
    let lease_id = first["body"]["lease_id"].as_str().unwrap().to_string();
    let res = owner
        .delete(server.url(&format!("/api/v1/volumes/{volume_id}")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    let revoke = on_a.until("lease_revoke").await;
    on_a.send(ok_result(None), revoke["id"].as_str()).await;
    on_a.settle().await;
    sqlx::query("UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?")
        .bind(&lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    set_policy(server, app_id, "warm").await;
    lease_id
}

/// #604 under real support: a `warm` app's new token over a released lease
/// row that already reads `expiring` for `D` and carries no unsettled mark
/// goes out `expiring` for `D` while two of three voters support the
/// primary, and the row stays unmarked. Once their support has run out, a
/// second app's new token over such a row is written `expiring` for `D`
/// with the mark, and its grant is held (section 8.4, "Grants of a marked
/// row"; issue #703): it goes out only once a majority supports `D` and the
/// write is replicated over the machines with the `node` role, and C never
/// connects. Before #703 that write was not made and `createVolume`
/// answered `conflict`.
#[tokio::test]
async fn a_warm_apps_new_token_goes_out_expiring_under_real_support() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    let clock = manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let mut on_a = Machine::connect(&server, &a).await;
    let mut on_b = Machine::connect(&server, &b).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let row = |lease_id: &str| {
        sqlx::query_as::<_, (String, Option<i64>, i64, i64)>(
            "SELECT mode, duration_ms, unsettled, fencing_token FROM leases WHERE id = ?",
        )
        .bind(lease_id.to_string())
        .fetch_one(server.state.store.pool())
    };

    let app_id = app(&server, &owner, &project_id, "web").await;
    let lease_id = released_expiring_lease(&server, &owner, &mut on_a, &app_id).await;
    assert_eq!(
        row(&lease_id).await.unwrap(),
        ("expiring".to_string(), Some(15_000), 0, 1)
    );
    on_a.support().await;
    on_b.support().await;
    assert!(supports(&server, D));
    let res = create_volume(&server, &owner, &app_id).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let grant = on_a.until("lease_grant").await;
    assert_eq!(grant["body"]["lease_id"], lease_id.as_str());
    assert_eq!(grant["body"]["fencing_token"], 2);
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    assert_eq!(
        row(&lease_id).await.unwrap(),
        ("expiring".to_string(), Some(15_000), 0, 2)
    );

    // The voters answer nothing more, and their support runs out.
    let web2 = app(&server, &owner, &project_id, "web-2").await;
    let lease_2 = released_expiring_lease(&server, &owner, &mut on_a, &web2).await;
    clock.advance(WINDOW);
    assert!(!supports(&server, D));
    let res = create_volume(&server, &owner, &web2).await;
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    assert_eq!(
        row(&lease_2).await.unwrap(),
        ("expiring".to_string(), Some(15_000), 1, 2),
        "written with the mark"
    );
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let frames = on_a.settle().await;
    assert!(
        !frames.iter().any(|frame| frame["type"] == "lease_grant"),
        "{frames:?}"
    );
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{web2}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(app["lease"]["grant_pending"], true, "{app}");
    let waiting = app["lease"]["grant_waiting_on"].as_array().unwrap();
    assert!(waiting.contains(&json!(c.node_id)), "{app}");
}
