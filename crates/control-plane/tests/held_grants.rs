//! The grants of a marked row (`node-agent-protocol.md` section 8.4,
//! "Grants of a marked row" and "The holder fenced first" step 4; ADR 0280,
//! ADR 0285, ADR 0292, ADR 0295, ADR 0299; issue #703), over real HTTP and
//! WebSocket with fake machines that follow the change log, acknowledge
//! each `log_entries`, and answer `support_request`s as the node agent
//! does. Support is measured on a manual `CLOCK_BOOTTIME`, and no test here
//! sets the authority's support by hand: every machine has the `node` role,
//! so each is a voter and in the required set.
//!
//! Case 11 of #703 (a `warm` restore completes with its grant held, and
//! `createVolume` of a `warm` app succeeds with its grant held) lives with
//! their other tests, in `restore.rs`, `new_token_grants.rs` and
//! `support.rs`; `failoverApp`'s held grant (ADR 0292) lives in
//! `failover.rs`.
//!
//! The holder's `local` report, which clears the mark at its next session
//! start (#555), is checked here over the case 2 row; its other cases are
//! the `session::local_report` unit tests. Out of scope here, as in the
//! code: the `pong` after a same-token `local` grant, R, the conversion
//! grant and the same-token resend.

mod common;

use common::log::{check_entries, log_hash_at, log_head};
use common::machine::{Machine, Node, dial, echo, enroll, hello_body, manual_clock, ok_result};
use common::{Server, TLS_FINGERPRINT, restart_with_resolver, start_with_patient_node_session};
use futures_util::{SinkExt, StreamExt};
use quoro_control_plane::api::enroll::{ensure_local_join, local_join_token};
use quoro_control_plane::changelog::{self, Actor, Op};
use quoro_control_plane::session::Fault;
use quoro_proto::wire::{
    self, Capacity as WireCapacity, Heartbeat, LogAck, NodeMessage, StatusReport,
    VolumeState as WireVolumeState, VolumeStatus,
};
use quoro_proto::{CommandResult, CommandStatus, ErrorBody, ErrorCode, LeaseMode, ResourceType};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const IMAGE: &str =
    "ghcr.io/example/web@sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// `D`, the duration every `support_request` carries.
const D: u64 = 15_000;

/// `D + 2M`: an `expiring` grant's gate, measured from its send.
const GATE: Duration = Duration::from_millis(18_000);

/// Longer than the held-grant task's tick, so an attempt that could send
/// has run.
const PAST_TICK: Duration = Duration::from_millis(1_500);

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

async fn set_policy(server: &Server, app_id: &str, policy: &str) {
    sqlx::query("UPDATE apps SET ha_policy = ? WHERE id = ?")
        .bind(policy)
        .bind(app_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

async fn run(server: &Server, sql: &str, lease_id: &str) {
    sqlx::query(sql)
        .bind(lease_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
}

/// A deployed stateful app on `machine`, the only node connected, whose
/// volume is ready and whose lease the node holds `local` at token 1, as a
/// `none` app's first grant leaves it. Returns the app and the lease.
async fn app_with_lease(
    server: &Server,
    owner: &reqwest::Client,
    machine: &mut Machine,
    project_id: &str,
    name: &str,
) -> (String, String) {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let app_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    set_policy(server, &app_id, "none").await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/deployments")))
        .json(&json!({ "image": IMAGE }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 202, "{}", res.text().await.unwrap());
    machine.settle().await;
    heartbeat(machine).await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let volume_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let grant = machine.until("lease_grant").await;
    assert_eq!(grant["body"]["fencing_token"], 1, "{grant}");
    assert_eq!(grant["body"]["mode"], "local", "{grant}");
    let lease_id = grant["body"]["lease_id"].as_str().unwrap().to_string();
    machine.settle().await;
    let rev: i64 = sqlx::query_scalar(
        "SELECT desired_rev FROM nodes WHERE id = (SELECT holder_node_id FROM leases WHERE id = ?)",
    )
    .bind(&lease_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    machine
        .send(
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
    machine.settle().await;
    (app_id, lease_id)
}

/// `lease_id` of `app_id` as a `hello` lists it.
fn listed(
    lease_id: &str,
    app_id: &str,
    mode: LeaseMode,
    token: u64,
    state: &str,
) -> wire::HelloLease {
    wire::HelloLease {
        lease_id: lease_id.to_string(),
        resource_type: ResourceType::App,
        resource_id: app_id.to_string(),
        mode,
        fencing_token: token,
        state: state.to_string(),
    }
}

/// A new session of `node`, its `hello` at the copy of the log `from` gives
/// as (`seq`, `hash`) (the start of the log when `None`) and listing
/// `leases`. It follows the log when `follows`. Returns once the session
/// start is over, with every frame of the session.
async fn session(
    server: &Server,
    node: &Node,
    from: Option<(u64, String)>,
    leases: Vec<wire::HelloLease>,
    follows: bool,
) -> (Machine, Vec<Value>) {
    let mut hello = hello_body(&node.node_id);
    if let Some((seq, hash)) = from {
        hello.log_seq = seq;
        hello.log_hash = hash;
    }
    hello.leases = leases;
    let mut machine = Machine::connect_with(server, node, hello, true, follows).await;
    let mut frames = machine.start.clone();
    frames.extend(machine.settle().await);
    (machine, frames)
}

/// The app's `lease` as the API shows it.
async fn lease_view(server: &Server, owner: &reqwest::Client, app_id: &str) -> Value {
    let app: Value = owner
        .get(server.url(&format!("/api/v1/apps/{app_id}")))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    app["lease"].clone()
}

/// `grant_pending` and `grant_waiting_on` of the app's lease.
async fn pending(server: &Server, owner: &reqwest::Client, app_id: &str) -> (bool, Vec<String>) {
    let lease = lease_view(server, owner, app_id).await;
    let waiting = lease["grant_waiting_on"]
        .as_array()
        .unwrap_or_else(|| panic!("{lease}"))
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    (lease["grant_pending"].as_bool().unwrap(), waiting)
}

/// The row as (token, mode, `duration_ms`, unsettled, revoked).
async fn row(server: &Server, lease_id: &str) -> (i64, String, Option<i64>, i64, bool) {
    let (token, mode, duration, unsettled, revoked): (
        i64,
        String,
        Option<i64>,
        i64,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT fencing_token, mode, duration_ms, unsettled, revoked_at FROM leases WHERE id = ?",
    )
    .bind(lease_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    (token, mode, duration, unsettled, revoked.is_some())
}

/// The `seq` of the newest change-log entry of `lease_id`.
async fn newest_write(server: &Server, lease_id: &str) -> u64 {
    let seq: i64 = sqlx::query_scalar(
        "SELECT MAX(seq) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
    )
    .bind(lease_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    u64::try_from(seq).unwrap()
}

/// Reads `machine`'s frames until its copy of the log holds `seq`, and
/// returns them. Once it does, one more [`Machine::settle`] puts a `ping`
/// behind the machine's last `log_ack` on the ordered session, so its
/// `pong` proves the primary processed that acknowledgement (#793).
async fn follow_to(machine: &mut Machine, seq: u64) -> Vec<Value> {
    let mut frames = Vec::new();
    for _ in 0..50 {
        if machine.log.0 >= seq {
            frames.extend(machine.settle().await);
            return frames;
        }
        frames.extend(machine.settle().await);
    }
    panic!("the machine did not reach seq {seq}, at {}", machine.log.0);
}

/// The grants of `lease_id` among `frames`.
fn grants_of<'a>(frames: &'a [Value], lease_id: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == "lease_grant" && frame["body"]["lease_id"] == lease_id)
        .collect()
}

/// Waits past the held-grant task's tick, then returns every frame up to
/// two pongs: an attempt that could have sent a grant has run.
async fn after_a_tick(machine: &mut Machine) -> Vec<Value> {
    tokio::time::sleep(PAST_TICK).await;
    machine.settle().await
}

/// Whether a `desired_state` among `frames` carries `token` for the VM of
/// `lease_id` (section 6.4).
fn spec_carries(frames: &[Value], lease_id: &str, token: u64) -> bool {
    frames.iter().any(|frame| {
        frame["type"] == "desired_state"
            && frame["body"]["spec"]["vms"].as_array().is_some_and(|vms| {
                vms.iter()
                    .any(|vm| vm["lease_id"] == lease_id && vm["fencing_token"] == token)
            })
    })
}

/// Case 1. A `warm` app's regrant over a `local` row (an `expiring` new
/// token over a row that stores no duration, a raise; ADR 0268) is written
/// with the mark at token + 1, and the authority records nothing at the
/// write. No grant goes out while a machine with the `node` role has not
/// acknowledged the write, nor while a majority does not support `D`; then
/// exactly one goes out, a desired state carrying the token follows, and
/// the new holder's gate is measured from the send, not from the write.
/// Case 2 is checked on the way: `grant_waiting_on` names the machine that
/// has not acknowledged, is empty once the write is replicated but not
/// supported, and after the send `grant_pending` is false while the row
/// stays marked.
#[tokio::test]
async fn a_warm_regrant_over_a_local_row_goes_out_once_replicated_and_supported() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_b, _) = session(&server, &b, None, Vec::new(), false).await;
    set_policy(&server, &app_id, "warm").await;

    // The holder's next `hello` lists the lease `expired`: the regrant.
    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (mut on_a, frames) = session(&server, &a, Some(on_a.log.clone()), vec![lease], true).await;
    let written = Instant::now();
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "expiring".to_string(), Some(15_000), 1, false),
        "written with the mark at token + 1"
    );
    let authority = server.state.hub.authority();
    assert_eq!(
        authority.gate_remaining(&lease_id),
        None,
        "nothing recorded"
    );
    assert_eq!(authority.view(&lease_id), None, "nothing recorded");
    let seq = newest_write(&server, &lease_id).await;

    // A acknowledges the write; B does not follow the log yet.
    follow_to(&mut on_a, seq).await;
    assert!(server.state.hub.countable_ack(&a.node_id) >= seq);
    assert!(server.state.hub.countable_ack(&b.node_id) < seq);
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![b.node_id.clone()])
    );
    assert!(grants_of(&after_a_tick(&mut on_a).await, &lease_id).is_empty());

    // B acknowledges it: replicated, and no majority supports `D` yet.
    on_b.start_following().await;
    follow_to(&mut on_b, seq).await;
    assert!(server.state.hub.countable_ack(&b.node_id) >= seq);
    assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
    assert!(!authority.supports(D));
    assert!(grants_of(&after_a_tick(&mut on_a).await, &lease_id).is_empty());

    // One of two voters is no majority.
    on_a.support().await;
    assert!(!authority.supports(D));
    assert!(grants_of(&after_a_tick(&mut on_a).await, &lease_id).is_empty());
    assert_eq!(authority.gate_remaining(&lease_id), None);
    assert!(written.elapsed() >= Duration::from_secs(2));

    on_b.support().await;
    assert!(authority.supports(D));
    let grant = on_a.until("lease_grant").await;
    let received = Instant::now();
    assert_eq!(grant["body"]["lease_id"], lease_id.as_str());
    assert_eq!(grant["body"]["fencing_token"], 2);
    assert_eq!(grant["body"]["mode"], "expiring");
    assert_eq!(grant["body"]["duration_ms"], 15_000);
    assert!(grant["re"].is_null(), "{grant}");
    // The gate runs `D + 2M` from the send: the write was more than 2 s
    // before it.
    let remaining = authority
        .gate_remaining(&lease_id)
        .expect("the gate is open");
    assert!(remaining <= GATE, "{remaining:?}");
    assert!(
        remaining + received.elapsed() + Duration::from_millis(500) >= GATE,
        "{remaining:?}"
    );
    assert!(authority.view(&lease_id).is_some());
    // `granted_at` is the send's time, after the write's.
    let granted_at: String = sqlx::query_scalar("SELECT granted_at FROM leases WHERE id = ?")
        .bind(&lease_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert!(
        grant["body"]["granted_at"].as_str().unwrap() > granted_at.as_str(),
        "{grant} {granted_at}"
    );

    // Exactly one grant, and a desired state carrying its token.
    let mut seen = Vec::new();
    while !spec_carries(&seen, &lease_id, 2) {
        seen.push(on_a.next().await);
    }
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["grant_pending"], false, "{view}");
    assert_eq!(view["grant_waiting_on"], json!([]), "{view}");
    assert_eq!(view["unsettled"], true, "{view}");
    assert_eq!(view["local_pending"], false, "{view}");
}

/// A `warm` app with no deployment whose first volume is created on the one
/// node with a heartbeat, `holder`: an `expiring` new token for `D` over no
/// row, a raise (ADR 0268), so it is written with the mark at token 1 and
/// its grant held. Returns the app and the lease.
async fn warm_volume(
    server: &Server,
    owner: &reqwest::Client,
    project_id: &str,
    name: &str,
    holder: &Node,
) -> (String, String) {
    let res = owner
        .post(server.url("/api/v1/apps"))
        .json(&json!({ "project_id": project_id, "name": name, "image": IMAGE, "port": 8080 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201);
    let app_id = res.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    set_policy(server, &app_id, "warm").await;
    let res = owner
        .post(server.url(&format!("/api/v1/apps/{app_id}/volumes")))
        .json(&json!({ "size_gb": 5, "mount_path": "/data" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "{}", res.text().await.unwrap());
    let (lease_id, holder_id): (String, String) = sqlx::query_as(
        "SELECT id, holder_node_id FROM leases WHERE resource_type = 'app' AND resource_id = ?",
    )
    .bind(&app_id)
    .fetch_one(server.state.store.pool())
    .await
    .unwrap();
    assert_eq!(holder_id, holder.node_id);
    assert_eq!(
        row(server, &lease_id).await,
        (1, "expiring".to_string(), Some(15_000), 1, false),
        "{name}: written with the mark"
    );
    (app_id, lease_id)
}

/// Answers `machine`'s next `support_request` and returns every frame up to
/// two pongs after it, so none that came meanwhile is lost.
async fn support_keeping_frames(machine: &mut Machine) -> Vec<Value> {
    let (_, request) = machine.request().await;
    let grant = echo(&request, machine.version());
    machine
        .send(NodeMessage::SupportGrant(grant), request["id"].as_str())
        .await;
    machine.settle().await
}

/// Reads `machine`'s frames into `seen` until `done` holds for them, or
/// panics after `within`.
async fn read_until(
    machine: &mut Machine,
    seen: &mut Vec<Value>,
    within: Duration,
    done: impl Fn(&[Value]) -> bool,
) {
    let deadline = Instant::now() + within;
    while !done(seen) {
        assert!(Instant::now() < deadline, "not within {within:?}: {seen:?}");
        seen.extend(machine.settle().await);
        if !done(seen) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// The revokes of `lease_id` among `frames`.
fn revokes_of<'a>(frames: &'a [Value], lease_id: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|frame| frame["type"] == "lease_revoke" && frame["body"]["lease_id"] == lease_id)
        .collect()
}

/// A lease no row knows, as a `hello` lists it: the session start revokes
/// it (section 5.1), and the revoke waits for the node's answer.
fn unknown_lease() -> wire::HelloLease {
    listed(
        "lse_01JC9Z6Q5W8N3K4T7R2M000000",
        "app_01JC9Z6Q5W8N3K4T7R2M000000",
        LeaseMode::Local,
        1,
        "held",
    )
}

/// Waits until this primary has recorded the end of `node`'s session.
async fn wait_disconnected(server: &Server, node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while server.state.hub.is_connected(&node.node_id) {
        assert!(Instant::now() < deadline, "the session did not end");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Case 2. A `none` app's regrant over a row that reads `expiring` is
/// `local`, a write that sets the mark ("The mark"): `local_pending` and
/// `grant_pending` are true. Its grant needs majority support for `D` too
/// (reading: "Grants of a marked row" asks support of every grant of a
/// marked row, and `D` is what every `support_request` carries); once the
/// write is replicated and the one voter supports `D`, it goes out `local`,
/// and the row stays `local` and marked until the holder reports it
/// (`a_holders_local_report_clears_the_mark_and_drops_the_held_grant`).
#[tokio::test]
async fn a_local_marked_row_shows_local_pending_and_its_grant_waits_for_support() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (mut on_a, frames) = session(&server, &a, Some(on_a.log.clone()), vec![lease], true).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 1, false)
    );
    follow_to(&mut on_a, newest_write(&server, &lease_id).await).await;
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["local_pending"], true, "{view}");
    assert_eq!(view["grant_pending"], true, "{view}");
    assert_eq!(view["grant_waiting_on"], json!([]), "{view}");
    assert!(grants_of(&after_a_tick(&mut on_a).await, &lease_id).is_empty());

    let mut seen = support_keeping_frames(&mut on_a).await;
    read_until(&mut on_a, &mut seen, Duration::from_secs(10), |seen| {
        !grants_of(seen, &lease_id).is_empty()
    })
    .await;
    let grants = grants_of(&seen, &lease_id);
    assert_eq!(grants.len(), 1, "{seen:?}");
    assert_eq!(grants[0]["body"]["fencing_token"], 2);
    assert_eq!(grants[0]["body"]["mode"], "local");
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["grant_pending"], false, "{view}");
    assert_eq!(view["local_pending"], true, "{view}");
}

/// #555, the holder's `local` report (section 8.4, "The holder's next
/// `hello`" and "The mark"): the case 2 row, `local` at token 2 with the
/// mark after its grant went out, is listed `local` `held` at token 2 by
/// its holder's next session. One change-log entry writes the row `local`,
/// unmarked, at token 2, so `local_pending` and `unsettled` turn false. The
/// session start sends the same-token `local` repeat with no
/// `duration_ms`, which the node takes as a no-op (section 8.4, node state
/// machine), and no other grant: the row was a step-4 row, and its held
/// grant was dropped at the session start (`grant_pending` false).
#[tokio::test]
async fn a_holders_local_report_clears_the_mark_and_drops_the_held_grant() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (mut on_a, _) = session(&server, &a, Some(on_a.log.clone()), vec![lease], true).await;
    follow_to(&mut on_a, newest_write(&server, &lease_id).await).await;
    let (_, request) = on_a.request().await;
    let grant = echo(&request, on_a.version());
    on_a.send(NodeMessage::SupportGrant(grant), request["id"].as_str())
        .await;
    let grant = on_a.until("lease_grant").await;
    assert_eq!(grant["body"]["lease_id"], lease_id.as_str());
    assert_eq!(grant["body"]["fencing_token"], 2);
    assert_eq!(grant["body"]["mode"], "local");
    on_a.settle().await;
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 1, false)
    );
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["local_pending"], true, "{view}");
    let entries = |server: &Server| {
        let pool = server.state.store.pool().clone();
        let lease_id = lease_id.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
            )
            .bind(lease_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let before = entries(&server).await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 2, "held");
    let (_on_a, frames) = session(&server, &a, Some(on_a.log.clone()), vec![lease], true).await;
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 0, false)
    );
    assert_eq!(entries(&server).await, before + 1, "one change-log entry");
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["local_pending"], false, "{view}");
    assert_eq!(view["unsettled"], false, "{view}");
    assert_eq!(view["grant_pending"], false, "{view}");
    let grants = grants_of(&frames, &lease_id);
    assert_eq!(grants.len(), 1, "{frames:?}");
    assert_eq!(grants[0]["body"]["mode"], "local");
    assert_eq!(grants[0]["body"]["fencing_token"], 2);
    assert!(grants[0]["body"].get("duration_ms").is_none(), "{frames:?}");
    assert!(grants[0]["re"].is_null(), "{frames:?}");
    assert!(revokes_of(&frames, &lease_id).is_empty(), "{frames:?}");
}

/// #555, a `local` report whose write fails. The row reads `expiring`,
/// unmarked, at token 1 with a stored `duration_ms`, and its holder lists
/// the lease `local` `held` at that token. The write fails on a database
/// error, so the session ends with a non-fatal error, "dial again", before
/// its first desired state and its grants: the holder must not go on
/// `local` over a row that reads `expiring`, unmarked. The next session
/// writes the row `local`, unmarked, at token 1, and keeps the stored
/// `duration_ms`, which `getApp`'s lease view does not show for a `local`
/// lease (`openapi.yaml` `Lease.duration_ms`, "expiring leases only"). Its
/// session start sends the same-token `local` repeat, with no
/// `duration_ms`.
#[tokio::test]
async fn a_failed_local_report_write_ends_the_session_and_the_next_one_writes_it() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    drop(on_a);
    wait_disconnected(&server, &a).await;
    let unwritten = (1, "expiring".to_string(), Some(15_000), 0, false);
    assert_eq!(row(&server, &lease_id).await, unwritten);
    let before = newest_write(&server, &lease_id).await;
    let listing = vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "held")];

    server.state.hub.inject_fault(Fault::LocalReport {
        node_id: a.node_id.clone(),
    });
    let hello = hello_at_head(&server, &a, listing.clone()).await;
    let (frames, closed) = serve_until_closed(&server, &a, hello, Duration::from_secs(3)).await;
    assert!(closed, "the session ends: {frames:?}");
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "desired_state" || frame["type"] == "lease_grant"),
        "{frames:?}"
    );
    let error = frames
        .iter()
        .find(|frame| frame["type"] == "error")
        .unwrap_or_else(|| panic!("{frames:?}"));
    assert_eq!(error["body"]["code"], "bad_message", "{error}");
    assert_eq!(error["body"]["fatal"], false, "{error}");
    assert!(
        error["body"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("dial again")),
        "{error}"
    );
    wait_disconnected(&server, &a).await;
    assert_eq!(row(&server, &lease_id).await, unwritten);
    assert_eq!(newest_write(&server, &lease_id).await, before, "no entry");

    let hello = hello_at_head(&server, &a, listing).await;
    let mut on_a = Machine::connect_with(&server, &a, hello, true, false).await;
    let mut frames = on_a.start.clone();
    frames.extend(on_a.settle().await);
    assert_eq!(
        row(&server, &lease_id).await,
        (1, "local".to_string(), Some(15_000), 0, false),
        "written local, the stored duration kept"
    );
    assert!(newest_write(&server, &lease_id).await > before);
    let grants = grants_of(&frames, &lease_id);
    assert_eq!(grants.len(), 1, "{frames:?}");
    assert_eq!(grants[0]["body"]["mode"], "local");
    assert_eq!(grants[0]["body"]["fencing_token"], 1);
    assert!(grants[0]["body"].get("duration_ms").is_none(), "{frames:?}");
    assert!(grants[0]["re"].is_null(), "{frames:?}");
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["mode"], "local", "{view}");
    assert!(view["duration_ms"].is_null(), "{view}");
    assert_eq!(view["local_pending"], false, "{view}");
}

/// Case 3, ADR 0299. A `hello.log_seq` that covers the write counts only
/// once every `lease_revoke` sent on that session is answered, and is
/// latched on the live session: C's counts once it answers its revoke; D's
/// session ends with its revoke unanswered and never counts, also after the
/// disconnect, while a later session's clean `hello` at the same position
/// does. B's `log_ack` counts at once, its revoke unanswered.
#[tokio::test]
async fn a_hello_counts_only_once_its_sessions_revokes_are_answered() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let d = enroll(&server, &owner, "node-d", &["node"]).await;
    let seq = newest_write(&server, &lease_id).await;
    follow_to(&mut on_a, seq).await;
    let hub = &server.state.hub;
    let waiting = |node: &Node| {
        let node_id = node.node_id.clone();
        let server = &server;
        let owner = &owner;
        let app_id = app_id.clone();
        async move { pending(server, owner, &app_id).await.1.contains(&node_id) }
    };

    // C: its `hello` covers the write, and its session's revoke is not
    // answered yet.
    let head = log_head(&server).await;
    let at_head = Some((head, log_hash_at(&server, head).await));
    let (mut on_c, frames) =
        session(&server, &c, at_head.clone(), vec![unknown_lease()], false).await;
    let revoke = frames
        .iter()
        .find(|frame| frame["type"] == "lease_revoke")
        .unwrap_or_else(|| panic!("{frames:?}"))
        .clone();
    assert_eq!(hub.countable_ack(&c.node_id), 0);
    assert!(waiting(&c).await);
    on_c.send(ok_result(None), revoke["id"].as_str()).await;
    on_c.settle().await;
    assert_eq!(hub.countable_ack(&c.node_id), head);
    assert!(!waiting(&c).await);

    // D: the same, but its session ends with the revoke unanswered.
    let (on_d, frames) = session(&server, &d, at_head.clone(), vec![unknown_lease()], false).await;
    assert!(frames.iter().any(|frame| frame["type"] == "lease_revoke"));
    assert_eq!(hub.countable_ack(&d.node_id), 0);
    drop(on_d);
    wait_disconnected(&server, &d).await;
    assert_eq!(hub.countable_ack(&d.node_id), 0, "never counts");
    assert!(waiting(&d).await);
    let (_on_d, frames) = session(&server, &d, at_head, Vec::new(), false).await;
    assert!(!frames.iter().any(|frame| frame["type"] == "lease_revoke"));
    assert_eq!(hub.countable_ack(&d.node_id), head);
    assert!(!waiting(&d).await);

    // B: at the start of the log, its revoke unanswered; its `log_ack`
    // counts at once.
    let (mut on_b, frames) = session(&server, &b, None, vec![unknown_lease()], true).await;
    assert!(frames.iter().any(|frame| frame["type"] == "lease_revoke"));
    follow_to(&mut on_b, seq).await;
    assert!(hub.countable_ack(&b.node_id) >= seq);
    assert!(!waiting(&b).await);
    assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
}

/// Case 4, ADR 0285 ("The mark"). B, which the marked rows do not name,
/// lists one lease `held` at the row's token and another `uncertain` above
/// it: each is revoked at the token B reports, and each row is raised above
/// it in one write that sets `revoked_at` and keeps the mark, with its
/// change-log entry. The named holder's next session gets no grant, with
/// voters supporting and every write replicated.
#[tokio::test]
async fn a_marked_row_listed_by_a_node_it_does_not_name_is_raised_and_revoked() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_1, lease_1) = warm_volume(&server, &owner, &project_id, "web-1", &a).await;
    let (app_2, lease_2) = warm_volume(&server, &owner, &project_id, "web-2", &a).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let before = newest_write(&server, &lease_1).await;

    let (mut on_b, frames) = session(
        &server,
        &b,
        None,
        vec![
            listed(&lease_1, &app_1, LeaseMode::Local, 1, "held"),
            listed(&lease_2, &app_2, LeaseMode::Expiring, 5, "uncertain"),
        ],
        true,
    )
    .await;
    for (lease_id, reported, raised) in [(&lease_1, 1, 2), (&lease_2, 5, 6)] {
        let revokes = revokes_of(&frames, lease_id);
        assert_eq!(revokes.len(), 1, "{frames:?}");
        assert_eq!(revokes[0]["body"]["fencing_token"], reported);
        let (token, mode, duration, unsettled, revoked) = row(&server, lease_id).await;
        assert_eq!(
            (token, mode.as_str(), duration, unsettled, revoked),
            (raised, "expiring", Some(15_000), 1, true),
            "raised above {reported}, revoked, the mark kept"
        );
    }
    assert!(newest_write(&server, &lease_1).await > before);
    for frame in &frames {
        if frame["type"] == "lease_revoke" {
            on_b.send(ok_result(None), frame["id"].as_str()).await;
        }
    }
    on_b.settle().await;

    // A's next session: replicated and supported, and nothing goes out.
    let (mut on_a, frames) = session(&server, &a, Some(on_a.log.clone()), Vec::new(), true).await;
    let head = log_head(&server).await;
    follow_to(&mut on_a, head).await;
    follow_to(&mut on_b, head).await;
    let mut seen = frames;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(server.state.hub.authority().supports(D));
    seen.extend(after_a_tick(&mut on_a).await);
    for (app_id, lease_id) in [(&app_1, &lease_1), (&app_2, &lease_2)] {
        assert!(grants_of(&seen, lease_id).is_empty(), "{seen:?}");
        assert_eq!(pending(&server, &owner, app_id).await, (false, Vec::new()));
    }
}

/// Cases 5 and 6 (the start gate), with one restart. The new hub rebuilds
/// every step-4 row as pending, `grant_waiting_on` naming its holder until
/// the holder's session starts; countable acknowledgements start again from
/// nothing. At A's session start:
///
/// - `web-1`, not listed: its grant goes out once replicated and supported,
///   and only once the start gate of `D + 2M` from the restart has passed;
/// - `web-2`, listed `held` at the row's token: no grant, and
///   `grant_pending` turns false;
/// - `web-3`, whose newest write kept the token (a same-token raise of
///   `duration_ms`): never pending, no grant;
/// - `web-4`, a marked `local` row with no duration: its start gate counts
///   the longest duration (`600 s + 2M`), so its grant waits, replicated
///   and supported (reading: the cost of the restart rule for a marked
///   `local` row).
#[tokio::test]
async fn a_restarted_primary_rebuilds_the_held_grants_from_the_rows() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    let (on_b, _) = session(&server, &b, None, Vec::new(), false).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_1, lease_1) = warm_volume(&server, &owner, &project_id, "web-1", &a).await;
    let (app_2, lease_2) = warm_volume(&server, &owner, &project_id, "web-2", &a).await;
    let (app_3, lease_3) = warm_volume(&server, &owner, &project_id, "web-3", &a).await;
    let (app_4, lease_4) = warm_volume(&server, &owner, &project_id, "web-4", &a).await;

    // `web-3`: a same-token raise of `duration_ms`, with its entry.
    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query("UPDATE leases SET duration_ms = 30000 WHERE id = ?")
        .bind(&lease_3)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM change_log WHERE entity_type = 'lease' AND entity_id = ? \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(&lease_3)
    .fetch_one(&mut *w.tx)
    .await
    .unwrap();
    let mut payload: Value = serde_json::from_str(&payload).unwrap();
    payload["duration_ms"] = json!(30_000);
    changelog::append(
        &mut w.tx,
        &Actor::system("test"),
        "lease",
        &lease_3,
        Op::Update,
        payload,
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
    // `web-4`: a marked `local` row with no duration, as a `local` new token
    // over a row that read `expiring` leaves it.
    run(
        &server,
        "UPDATE leases SET mode = 'local', duration_ms = NULL WHERE id = ?",
        &lease_4,
    )
    .await;
    follow_to(&mut on_a, log_head(&server).await).await;
    let a_log = on_a.log.clone();
    drop(on_a);
    drop(on_b);
    wait_disconnected(&server, &a).await;
    wait_disconnected(&server, &b).await;

    let restarted =
        restart_with_resolver(&server, Arc::new(quoro_control_plane::ReferenceAsDigest)).await;
    let started = Instant::now();
    manual_clock(&restarted);
    let authority = restarted.state.hub.authority();
    for app_id in [&app_1, &app_2, &app_4] {
        let (pending_now, waiting) = pending(&restarted, &owner, app_id).await;
        assert!(pending_now, "{app_id}");
        assert!(waiting.contains(&a.node_id), "the holder: {waiting:?}");
        assert!(
            waiting.contains(&b.node_id),
            "no acknowledgement yet: {waiting:?}"
        );
    }
    assert_eq!(
        pending(&restarted, &owner, &app_3).await,
        (false, Vec::new())
    );
    assert_eq!(restarted.state.hub.countable_ack(&a.node_id), 0);
    let start_gate = authority.gate_remaining(&lease_1).expect("the start gate");
    assert!(start_gate <= GATE && start_gate > GATE - Duration::from_secs(5));
    let local_gate = authority.gate_remaining(&lease_4).expect("the start gate");
    assert!(local_gate > Duration::from_secs(700), "{local_gate:?}");

    let (mut on_a, frames) = session(
        &restarted,
        &a,
        Some(a_log),
        vec![listed(&lease_2, &app_2, LeaseMode::Expiring, 1, "held")],
        true,
    )
    .await;
    let mut seen = frames;
    assert_eq!(
        pending(&restarted, &owner, &app_2).await,
        (false, Vec::new())
    );
    let head = log_head(&restarted).await;
    seen.extend(follow_to(&mut on_a, head).await);
    assert_eq!(
        pending(&restarted, &owner, &app_1).await,
        (true, vec![b.node_id.clone()])
    );
    let (mut on_b, _) = session(&restarted, &b, None, Vec::new(), true).await;
    follow_to(&mut on_b, head).await;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(authority.supports(D));
    assert_eq!(
        pending(&restarted, &owner, &app_1).await,
        (true, Vec::new())
    );
    // The start gate is still open: nothing goes out.
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(
        authority.gate_remaining(&lease_1).is_some(),
        "the test ran too slowly"
    );
    assert!(grants_of(&seen, &lease_1).is_empty(), "{seen:?}");

    read_until(&mut on_a, &mut seen, Duration::from_secs(30), |seen| {
        !grants_of(seen, &lease_1).is_empty()
    })
    .await;
    assert!(started.elapsed() >= GATE - Duration::from_millis(1_000));
    let grants = grants_of(&seen, &lease_1);
    assert_eq!(grants.len(), 1, "{seen:?}");
    assert_eq!(grants[0]["body"]["fencing_token"], 1);
    assert_eq!(grants[0]["body"]["mode"], "expiring");
    seen.extend(after_a_tick(&mut on_a).await);
    for lease_id in [&lease_2, &lease_3, &lease_4] {
        assert!(
            grants_of(&seen, lease_id).is_empty(),
            "{lease_id}: {seen:?}"
        );
    }
    assert_eq!(
        pending(&restarted, &owner, &app_4).await,
        (true, Vec::new())
    );
    assert!(authority.gate_remaining(&lease_4).is_some());
    assert_eq!(
        pending(&restarted, &owner, &app_3).await,
        (false, Vec::new())
    );
}

/// Case 6, "The holder fenced first" step 4. Three `warm` regrants over
/// `local` rows are written with the mark at token 2 on one session, which
/// ends before any goes out. A's next session lists one `expired` at
/// token 1, one `held` `local` at token 1 while its row reads `expiring`,
/// and one `revoked` at the row's token. Without support nothing goes out,
/// not even the revoke. Once the one voter supports `D`, the revoke goes
/// out at the token A reports, and both grants follow its answer, at token
/// 2, with no new token minted; the `revoked` one gets nothing. A later
/// session that lists nothing (a lost `node.db`) gets every grant again:
/// the one never sent at once, the others once the gates of their first
/// sends have passed.
#[tokio::test]
async fn step_4_sends_the_held_grant_by_what_the_node_lists() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let mut leases = Vec::new();
    for name in ["web-expired", "web-held", "web-revoked"] {
        let (app_id, lease_id) =
            app_with_lease(&server, &owner, &mut on_a, &project_id, name).await;
        set_policy(&server, &app_id, "warm").await;
        leases.push((app_id, lease_id));
    }
    let listed_expired = |token: u64| -> Vec<wire::HelloLease> {
        leases
            .iter()
            .map(|(app_id, lease_id)| listed(lease_id, app_id, LeaseMode::Local, token, "expired"))
            .collect()
    };
    let (on_a, frames) =
        session(&server, &a, Some(on_a.log.clone()), listed_expired(1), true).await;
    let mut written = Vec::new();
    for (_, lease_id) in &leases {
        assert!(grants_of(&frames, lease_id).is_empty(), "{frames:?}");
        assert_eq!(
            row(&server, lease_id).await,
            (2, "expiring".to_string(), Some(15_000), 1, false)
        );
        written.push(newest_write(&server, lease_id).await);
    }
    let a_log = on_a.log.clone();
    drop(on_a);
    wait_disconnected(&server, &a).await;

    let [expired, held, revoked] = [&leases[0], &leases[1], &leases[2]];
    let (mut on_a, frames) = session(
        &server,
        &a,
        Some(a_log),
        vec![
            listed(&expired.1, &expired.0, LeaseMode::Local, 1, "expired"),
            listed(&held.1, &held.0, LeaseMode::Local, 1, "held"),
            listed(&revoked.1, &revoked.0, LeaseMode::Expiring, 2, "revoked"),
        ],
        true,
    )
    .await;
    let mut seen = frames;
    seen.extend(follow_to(&mut on_a, log_head(&server).await).await);
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(
        !seen
            .iter()
            .any(|frame| frame["type"] == "lease_grant" || frame["type"] == "lease_revoke"),
        "without support neither goes out: {seen:?}"
    );
    assert_eq!(
        pending(&server, &owner, &expired.0).await,
        (true, Vec::new())
    );
    assert_eq!(pending(&server, &owner, &held.0).await, (true, Vec::new()));
    assert_eq!(
        pending(&server, &owner, &revoked.0).await,
        (false, Vec::new())
    );

    let mut seen = support_keeping_frames(&mut on_a).await;
    read_until(&mut on_a, &mut seen, Duration::from_secs(10), |seen| {
        !revokes_of(seen, &held.1).is_empty()
    })
    .await;
    let revoke = revokes_of(&seen, &held.1)[0].clone();
    assert_eq!(revoke["body"]["fencing_token"], 1);
    assert_eq!(revoke["body"]["reason"], "superseded");
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(
        !seen.iter().any(|frame| frame["type"] == "lease_grant"),
        "no grant before the revoke's answer: {seen:?}"
    );
    on_a.send(ok_result(None), revoke["id"].as_str()).await;
    read_until(&mut on_a, &mut seen, Duration::from_secs(10), |seen| {
        !grants_of(seen, &expired.1).is_empty() && !grants_of(seen, &held.1).is_empty()
    })
    .await;
    let sent = Instant::now();
    seen.extend(after_a_tick(&mut on_a).await);
    for lease_id in [&expired.1, &held.1] {
        let grants = grants_of(&seen, lease_id);
        assert_eq!(grants.len(), 1, "{seen:?}");
        assert_eq!(grants[0]["body"]["fencing_token"], 2);
        assert_eq!(grants[0]["body"]["mode"], "expiring");
    }
    assert!(grants_of(&seen, &revoked.1).is_empty(), "{seen:?}");
    for ((_, lease_id), seq) in leases.iter().zip(&written) {
        assert_eq!(row(&server, lease_id).await.0, 2, "no new token");
        assert_eq!(
            newest_write(&server, lease_id).await,
            *seq,
            "nothing written"
        );
    }

    // A lost `node.db`: a session at the start of the log that lists nothing.
    let (mut on_a, frames) = session(&server, &a, None, Vec::new(), true).await;
    let mut seen = frames;
    read_until(&mut on_a, &mut seen, Duration::from_secs(10), |seen| {
        !grants_of(seen, &revoked.1).is_empty()
    })
    .await;
    assert!(grants_of(&seen, &expired.1).is_empty(), "{seen:?}");
    read_until(
        &mut on_a,
        &mut seen,
        GATE + Duration::from_secs(10),
        |seen| !grants_of(seen, &expired.1).is_empty() && !grants_of(seen, &held.1).is_empty(),
    )
    .await;
    assert!(sent.elapsed() >= GATE - Duration::from_millis(500));
    for (_, lease_id) in &leases {
        let grants = grants_of(&seen, lease_id);
        assert_eq!(grants.len(), 1, "{seen:?}");
        assert_eq!(grants[0]["body"]["fencing_token"], 2);
        assert_eq!(row(&server, lease_id).await.0, 2, "no new token");
    }
}

/// Case 7. Every condition is read again at the send. With the one voter
/// supporting and before A acknowledges the writes, one row is recorded
/// revoked (as declaring A lost does), another's lease is listed in a failed
/// fence, and a third's token moves. Once A acknowledges, nothing goes out.
/// The revoked and the moved entries are dropped: set back by hand, their
/// rows get no grant on this session. The fenced entry stays, and its grant
/// goes out once the failure is cleared.
#[tokio::test]
async fn every_condition_is_read_again_at_the_send() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let edge = enroll(&server, &owner, "edge-e", &["edge"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), false).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_r, lease_r) = warm_volume(&server, &owner, &project_id, "web-r", &a).await;
    let (app_f, lease_f) = warm_volume(&server, &owner, &project_id, "web-f", &a).await;
    let (app_t, lease_t) = warm_volume(&server, &owner, &project_id, "web-t", &a).await;
    let mut seen = support_keeping_frames(&mut on_a).await;
    assert!(server.state.hub.authority().supports(D));
    for app_id in [&app_r, &app_f, &app_t] {
        assert_eq!(
            pending(&server, &owner, app_id).await,
            (true, vec![a.node_id.clone()])
        );
    }

    run(
        &server,
        "UPDATE leases SET revoked_at = '2026-11-09T10:00:00.000Z' WHERE id = ?",
        &lease_r,
    )
    .await;
    let failure = json!({
        "detected_at": "2026-11-09T10:00:00Z",
        "lease_ids": [lease_f],
        "app_ids": [app_f],
        "vm_ids": [],
    })
    .to_string();
    sqlx::query("UPDATE nodes SET fence_failure = ? WHERE id = ?")
        .bind(&failure)
        .bind(&edge.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token + 1 WHERE id = ?",
        &lease_t,
    )
    .await;

    on_a.start_following().await;
    seen.extend(follow_to(&mut on_a, log_head(&server).await).await);
    seen.extend(after_a_tick(&mut on_a).await);
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(
        !seen.iter().any(|frame| frame["type"] == "lease_grant"),
        "{seen:?}"
    );
    assert_eq!(pending(&server, &owner, &app_r).await, (false, Vec::new()));
    assert_eq!(pending(&server, &owner, &app_t).await, (false, Vec::new()));
    assert_eq!(pending(&server, &owner, &app_f).await, (true, Vec::new()));

    // Set back by hand: the dropped entries send nothing on this session.
    run(
        &server,
        "UPDATE leases SET revoked_at = NULL WHERE id = ?",
        &lease_r,
    )
    .await;
    run(
        &server,
        "UPDATE leases SET fencing_token = fencing_token - 1 WHERE id = ?",
        &lease_t,
    )
    .await;
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_r).is_empty(), "{seen:?}");
    assert!(grants_of(&seen, &lease_t).is_empty(), "{seen:?}");
    assert_eq!(pending(&server, &owner, &app_r).await, (false, Vec::new()));
    assert_eq!(pending(&server, &owner, &app_t).await, (false, Vec::new()));
    assert!(grants_of(&seen, &lease_f).is_empty(), "{seen:?}");

    sqlx::query("UPDATE nodes SET fence_failure = NULL WHERE id = ?")
        .bind(&edge.node_id)
        .execute(server.state.store.pool())
        .await
        .unwrap();
    read_until(&mut on_a, &mut seen, Duration::from_secs(10), |seen| {
        !grants_of(seen, &lease_f).is_empty()
    })
    .await;
    assert_eq!(grants_of(&seen, &lease_f)[0]["body"]["fencing_token"], 1);
    assert!(grants_of(&seen, &lease_r).is_empty(), "{seen:?}");
    assert!(grants_of(&seen, &lease_t).is_empty(), "{seen:?}");
}

/// Case 8, "When R is replicated". C is removed after the write and never
/// acknowledged it: it stays in the required set until both A and B have
/// acknowledged its removal entry. B's `hello` at the write counts, so only
/// C is waited on, and once B follows past the removal nobody is.
#[tokio::test]
async fn a_removed_machine_leaves_the_set_once_the_others_acknowledge_its_removal() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let seq = newest_write(&server, &lease_id).await;
    let hash = log_hash_at(&server, seq).await;

    let mut w = server.state.store.begin_write().await.unwrap();
    sqlx::query("UPDATE nodes SET status = 'removed' WHERE id = ?")
        .bind(&c.node_id)
        .execute(&mut *w.tx)
        .await
        .unwrap();
    let removal = changelog::append(
        &mut w.tx,
        &Actor::system("test"),
        "node",
        &c.node_id,
        Op::Update,
        json!({ "id": c.node_id, "status": "removed" }),
    )
    .await
    .unwrap();
    let removal = u64::try_from(removal).unwrap();
    w.commit().await.unwrap();
    follow_to(&mut on_a, removal).await;
    let (_, waiting) = pending(&server, &owner, &app_id).await;
    assert_eq!(waiting, {
        let mut both = vec![b.node_id.clone(), c.node_id.clone()];
        both.sort();
        both
    });

    let (mut on_b, _) = session(&server, &b, Some((seq, hash)), Vec::new(), false).await;
    assert_eq!(server.state.hub.countable_ack(&b.node_id), seq);
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![c.node_id.clone()]),
        "C stays while B has not acknowledged its removal"
    );
    on_b.start_following().await;
    follow_to(&mut on_b, removal).await;
    assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
}

/// Case 9, section 7.1. B acknowledged the write; a later `hello` whose
/// hash does not match the primary's log gets `log_diverged`, and B's
/// countable acknowledgement drops to 0, so the grant waits on B again.
#[tokio::test]
async fn log_diverged_drops_the_countable_acknowledgement() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let seq = newest_write(&server, &lease_id).await;
    follow_to(&mut on_a, seq).await;
    let (mut on_b, _) = session(&server, &b, None, Vec::new(), true).await;
    follow_to(&mut on_b, seq).await;
    assert!(server.state.hub.countable_ack(&b.node_id) >= seq);
    assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
    let diverged = (on_b.log.0, "f".repeat(64));
    drop(on_b);
    wait_disconnected(&server, &b).await;

    let (mut on_b, mut seen) = session(&server, &b, Some(diverged), Vec::new(), false).await;
    read_until(&mut on_b, &mut seen, Duration::from_secs(10), |seen| {
        seen.iter()
            .any(|frame| frame["type"] == "error" && frame["body"]["code"] == "log_diverged")
    })
    .await;
    assert_eq!(server.state.hub.countable_ack(&b.node_id), 0);
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![b.node_id.clone()])
    );
}

/// Enrolls `name` with the local bootstrap token, as the primary's own
/// agent does (ADR 0110): its row is the primary's host.
async fn enroll_primary_host(server: &Server, roles: &[&str]) -> Node {
    ensure_local_join(
        &server.data_dir,
        "https://primary.local:8443",
        TLS_FINGERPRINT,
    )
    .unwrap();
    let token = local_join_token(&server.data_dir).unwrap();
    let res = server
        .client()
        .post(server.url("/api/v1/nodes/enroll"))
        .json(&json!({
            "join_token": token,
            "name": "primary",
            "public_key": quoro_proto::sign::public_key_to_base64(
                &quoro_proto::sign::signing_key_from_seed(&[3u8; 32]).verifying_key()
            ),
            "roles": roles,
            "capacity": { "cpus": 8, "memory_mb": 16_384, "disk_gb": 200 },
            "kvm": { "available": true, "kernel": "6.9.0", "nested": true }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 201, "enroll: {}", res.text().await.unwrap());
    let body: Value = res.json().await.unwrap();
    let node_id = body["node_id"].as_str().unwrap().to_string();
    let cp_status: Option<String> = sqlx::query_scalar("SELECT cp_status FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_one(server.state.store.pool())
        .await
        .unwrap();
    assert_eq!(cp_status.as_deref(), Some("primary"));
    Node {
        node_id,
        credential: body["credential"].as_str().unwrap().to_string(),
    }
}

/// Case 10, "When R is replicated". The primary's host with the `node` role
/// is in the required set like any node and counts only through its own
/// agent's session; without the role it is not in the set.
#[tokio::test]
async fn the_primarys_host_counts_only_through_its_own_session() {
    for with_node_role in [true, false] {
        let server = start_with_patient_node_session().await;
        let owner = server.owner().await;
        manual_clock(&server);
        let roles: &[&str] = if with_node_role {
            &["node"]
        } else {
            &["witness"]
        };
        let host = enroll_primary_host(&server, roles).await;
        let a = enroll(&server, &owner, "node-a", &["node"]).await;
        let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
        heartbeat(&mut on_a).await;
        let project_id = project(&server, &owner).await;
        let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
        let seq = newest_write(&server, &lease_id).await;
        follow_to(&mut on_a, seq).await;
        if !with_node_role {
            assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
            continue;
        }
        assert_eq!(
            pending(&server, &owner, &app_id).await,
            (true, vec![host.node_id.clone()])
        );
        let (mut on_host, _) = session(&server, &host, None, Vec::new(), true).await;
        follow_to(&mut on_host, seq).await;
        assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
    }
}

/// Dials as `node`, sends `hello` and serves the session as a following
/// node would, checking and acknowledging each `log_entries` from the
/// `hello`'s position, until the primary closes it or it stays quiet past
/// `within`. Returns every frame, and whether the primary closed it.
async fn serve_until_closed(
    server: &Server,
    node: &Node,
    hello: wire::Hello,
    within: Duration,
) -> (Vec<Value>, bool) {
    let mut socket = dial(server, node).await;
    let mut log = (hello.log_seq, hello.log_hash.clone());
    let text = NodeMessage::Hello(hello)
        .to_frame("msg_01JC9Z6Q5W8N3K4T7R2MHELL00", None)
        .to_text();
    socket.send(WsMessage::Text(text.into())).await.unwrap();
    let mut frames = Vec::new();
    let mut acks = 0u32;
    loop {
        let message = match tokio::time::timeout(within, socket.next()).await {
            Err(_) => return (frames, false),
            Ok(None | Some(Err(_))) => return (frames, true),
            Ok(Some(Ok(message))) => message,
        };
        match message {
            WsMessage::Text(text) => {
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                if frame["type"] == "log_entries" {
                    let (seq, hash) = check_entries(&frame["body"], &log.1);
                    log = (seq, hash.clone());
                    acks += 1;
                    let ack = NodeMessage::LogAck(LogAck { seq, hash })
                        .to_frame(&format!("msg_01JC9Z6Q5W8N3K4T7R2MACK{acks:03}"), None)
                        .to_text();
                    let _ = socket.send(WsMessage::Text(ack.into())).await;
                }
                frames.push(frame);
            }
            WsMessage::Close(_) => return (frames, true),
            _ => {}
        }
    }
}

/// The `hello` of `node` at the primary's newest change-log entry, listing
/// `leases`.
async fn hello_at_head(server: &Server, node: &Node, leases: Vec<wire::HelloLease>) -> wire::Hello {
    let head = log_head(server).await;
    let mut hello = hello_body(&node.node_id);
    hello.log_seq = head;
    hello.log_hash = log_hash_at(server, head).await;
    hello.leases = leases;
    hello
}

/// B1 (ADR 0299, section 8.4 "Acknowledgements"). A, the holder, and C have
/// acknowledged the marked row's write and support the primary, a majority
/// of three voters. B, which the row does not name, lists the lease `held`
/// `local` above the row's token, from writes this primary lacks, and an
/// unknown lease; its session start cannot read the marked row. The pass is
/// incomplete, so the session ends before the welcome: no revoke reaches B,
/// B's position and acknowledgements never count, and A gets no grant. B's
/// next session runs the pass to its end: the revoke at B's token reaches
/// it, the row is raised above it and recorded revoked (ADR 0285), and A
/// still gets no grant once every condition else holds.
#[tokio::test]
async fn a_failed_start_pass_ends_the_session_without_its_revokes() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let c = enroll(&server, &owner, "node-c", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let (mut on_c, _) = session(&server, &c, None, Vec::new(), true).await;
    let seq = newest_write(&server, &lease_id).await;
    follow_to(&mut on_a, seq).await;
    follow_to(&mut on_c, seq).await;
    let mut seen = support_keeping_frames(&mut on_a).await;
    on_c.support().await;
    assert!(server.state.hub.authority().supports(D));
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![b.node_id.clone()])
    );

    let listing = vec![
        listed(&lease_id, &app_id, LeaseMode::Local, 2, "held"),
        unknown_lease(),
    ];
    server.state.hub.inject_fault(Fault::StartRead {
        node_id: b.node_id.clone(),
        lease_id: lease_id.clone(),
    });
    let hello = hello_at_head(&server, &b, listing.clone()).await;
    let (frames, closed) = serve_until_closed(&server, &b, hello, Duration::from_secs(3)).await;
    assert!(closed, "the session ends: {frames:?}");
    assert!(
        !frames
            .iter()
            .any(|frame| frame["type"] == "lease_revoke" || frame["type"] == "welcome"),
        "{frames:?}"
    );
    assert_eq!(frames[0]["type"], "error", "{frames:?}");
    assert_eq!(server.state.hub.countable_ack(&b.node_id), 0);
    wait_disconnected(&server, &b).await;
    assert_eq!(
        row(&server, &lease_id).await,
        (1, "expiring".to_string(), Some(15_000), 1, false),
        "neither raised nor revoked"
    );
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![b.node_id.clone()])
    );

    // The next session runs the pass to its end.
    let hello = hello_at_head(&server, &b, listing).await;
    let mut on_b = Machine::connect_with(&server, &b, hello, true, true).await;
    let mut frames = on_b.start.clone();
    frames.extend(on_b.settle().await);
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], 2);
    assert_eq!(
        row(&server, &lease_id).await,
        (3, "expiring".to_string(), Some(15_000), 1, true),
        "raised above B's token, revoked, the mark kept"
    );
    for frame in &frames {
        if frame["type"] == "lease_revoke" {
            on_b.send(ok_result(None), frame["id"].as_str()).await;
        }
    }
    on_b.settle().await;
    let head = log_head(&server).await;
    follow_to(&mut on_a, head).await;
    follow_to(&mut on_b, head).await;
    follow_to(&mut on_c, head).await;
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(pending(&server, &owner, &app_id).await, (false, Vec::new()));
}

/// B2 ("The mark", ADR 0285). B, which the marked row does not name, lists
/// the lease `held` at the row's token, and its session start cannot
/// commit the raise. The pass is incomplete, so the session ends and the
/// revoke never reaches B, which therefore lists the lease `held` again on
/// its next session: the raise commits there, the revoke follows, and step
/// 4 never sends A a grant, on its live session or its next one.
#[tokio::test]
async fn a_failed_raise_is_tried_again_before_its_revoke_goes_out() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let listing = vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "held")];

    server.state.hub.inject_fault(Fault::Raise {
        node_id: b.node_id.clone(),
    });
    let mut hello = hello_body(&b.node_id);
    hello.leases = listing.clone();
    let (frames, closed) = serve_until_closed(&server, &b, hello, Duration::from_secs(3)).await;
    assert!(closed, "the session ends: {frames:?}");
    assert!(revokes_of(&frames, &lease_id).is_empty(), "{frames:?}");
    wait_disconnected(&server, &b).await;
    assert_eq!(
        row(&server, &lease_id).await,
        (1, "expiring".to_string(), Some(15_000), 1, false)
    );

    let (mut on_b, frames) = session(&server, &b, None, listing, true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], 1);
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "expiring".to_string(), Some(15_000), 1, true),
        "raised, revoked, the mark kept"
    );
    on_b.send(ok_result(None), revokes[0]["id"].as_str()).await;
    on_b.settle().await;

    let head = log_head(&server).await;
    let mut seen = follow_to(&mut on_a, head).await;
    follow_to(&mut on_b, head).await;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(server.state.hub.authority().supports(D));
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    let (mut on_a, frames) = session(&server, &a, Some(on_a.log.clone()), Vec::new(), true).await;
    let mut seen = frames;
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(pending(&server, &owner, &app_id).await, (false, Vec::new()));
}

/// B2, section 8.5 step 1. B, an old holder, lists the lease `expired`; it
/// answers its revoke `fence_failed`, and the primary cannot record the
/// failure, which it then keeps in memory only. With every other condition
/// met, A gets no grant: not on its live session, not as a regrant when it
/// lists the lease `expired` at the row's token (no token is minted), and
/// not as step 4 when a later session lists nothing.
#[tokio::test]
async fn an_unrecorded_failed_fence_holds_the_grant() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;

    server.state.hub.inject_fault(Fault::FailureRecord {
        node_id: b.node_id.clone(),
    });
    let listing = vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired")];
    let (mut on_b, frames) = session(&server, &b, None, listing, true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    on_b.send(
        NodeMessage::CommandResult(CommandResult {
            status: CommandStatus::Error,
            detail: Some("the VM may still run".to_string()),
            error: Some(ErrorBody {
                code: ErrorCode::FenceFailed,
                message: "the forced stop could not be confirmed".to_string(),
            }),
            lines: None,
            metrics: None,
        }),
        revokes[0]["id"].as_str(),
    )
    .await;
    on_b.settle().await;
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&b.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(failure, None, "not recorded");

    let head = log_head(&server).await;
    let mut seen = follow_to(&mut on_a, head).await;
    follow_to(&mut on_b, head).await;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(server.state.hub.authority().supports(D));
    assert!(server.state.hub.countable_ack(&b.node_id) >= head);
    seen.extend(after_a_tick(&mut on_a).await);
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(pending(&server, &owner, &app_id).await, (true, Vec::new()));
    let written = newest_write(&server, &lease_id).await;

    for leases in [
        vec![listed(
            &lease_id,
            &app_id,
            LeaseMode::Expiring,
            1,
            "expired",
        )],
        Vec::new(),
    ] {
        let (mut on_a_again, frames) =
            session(&server, &a, Some(on_a.log.clone()), leases, true).await;
        let mut seen = frames;
        seen.extend(after_a_tick(&mut on_a_again).await);
        assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
        assert_eq!(row(&server, &lease_id).await.0, 1, "no token minted");
        assert_eq!(newest_write(&server, &lease_id).await, written);
        on_a = on_a_again;
    }
}

/// Section 9 step 2, ADR 0285 ("The mark"): only a marked row is raised for
/// a node it does not name. B lists `held`, at the row's token, a lease
/// whose unmarked row names A: B gets the revoke at the token it reports,
/// and the row is left as it was, with no new change-log entry.
#[tokio::test]
async fn an_unmarked_row_listed_by_a_node_it_does_not_name_is_revoked_not_raised() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let unchanged = (1, "local".to_string(), None, 0, false);
    assert_eq!(row(&server, &lease_id).await, unchanged);
    let before = newest_write(&server, &lease_id).await;

    let (_on_b, frames) = session(
        &server,
        &b,
        None,
        vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "held")],
        true,
    )
    .await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], 1);
    assert_eq!(row(&server, &lease_id).await, unchanged, "not raised");
    assert_eq!(
        newest_write(&server, &lease_id).await,
        before,
        "no new entry"
    );
}

/// "Grants of a marked row": the session start never sends a marked row's
/// grant itself. A's regrant over a row that reads `expiring` is written
/// marked, `local` at token 2, on a session that does not follow the log:
/// A, the one voter, never acknowledges the write and supports nothing, so
/// a grant would be wrong. A's next session lists the lease `held` at
/// token 2, `expiring`, so that the row stays marked (listed `local`, it is
/// the holder's `local` report, which clears the mark:
/// `a_holders_local_report_clears_the_mark_and_drops_the_held_grant`), and
/// no `lease_grant` of it goes out.
#[tokio::test]
async fn a_marked_row_listed_held_at_its_token_gets_no_grant_at_the_session_start() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let a_log = on_a.log.clone();
    drop(on_a);
    wait_disconnected(&server, &a).await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (on_a, frames) = session(&server, &a, Some(a_log.clone()), vec![lease], false).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let marked = (2, "local".to_string(), None, 1, false);
    assert_eq!(row(&server, &lease_id).await, marked);
    assert!(a_log.0 < newest_write(&server, &lease_id).await);
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![a.node_id.clone()]),
        "not replicated"
    );
    assert!(!server.state.hub.authority().supports(D));
    drop(on_a);
    wait_disconnected(&server, &a).await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Expiring, 2, "held");
    let (mut on_a, frames) = session(&server, &a, Some(a_log), vec![lease], false).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    let frames = after_a_tick(&mut on_a).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(row(&server, &lease_id).await, marked);
}

/// #555, the holder's `local` report over the row of
/// `a_marked_row_listed_held_at_its_token_gets_no_grant_at_the_session_start`:
/// A's regrant over a row that reads `expiring` is written marked, `local`
/// at token 2, on a session that does not follow the log, so A, the one
/// voter, never acknowledges the write and supports nothing. A's next
/// session lists the lease `local` `held` at token 2: that is the holder's
/// `local` report, so one change-log entry writes the row `local`,
/// unmarked, at token 2 ("The mark", the `local` report bullet), with no
/// replication and no support. The session start sends exactly one grant,
/// the same-token `local` repeat with no `duration_ms` and no `re`, the
/// held grant is gone (`grant_pending` false), and nothing else goes out
/// after a tick.
#[tokio::test]
async fn a_marked_row_listed_local_held_at_its_token_is_written_unmarked_with_no_support() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET mode = 'expiring', duration_ms = 15000 WHERE id = ?",
        &lease_id,
    )
    .await;
    let a_log = on_a.log.clone();
    drop(on_a);
    wait_disconnected(&server, &a).await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (on_a, frames) = session(&server, &a, Some(a_log.clone()), vec![lease], false).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 1, false)
    );
    assert!(a_log.0 < newest_write(&server, &lease_id).await);
    assert_eq!(
        pending(&server, &owner, &app_id).await,
        (true, vec![a.node_id.clone()]),
        "not replicated"
    );
    assert!(!server.state.hub.authority().supports(D));
    drop(on_a);
    wait_disconnected(&server, &a).await;
    let entries = |server: &Server| {
        let pool = server.state.store.pool().clone();
        let lease_id = lease_id.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
            )
            .bind(lease_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let before = entries(&server).await;

    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 2, "held");
    let (mut on_a, frames) = session(&server, &a, Some(a_log), vec![lease], false).await;
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 0, false)
    );
    assert_eq!(entries(&server).await, before + 1, "one change-log entry");
    assert!(!server.state.hub.authority().supports(D));
    let view = lease_view(&server, &owner, &app_id).await;
    assert_eq!(view["grant_pending"], false, "{view}");
    assert_eq!(view["local_pending"], false, "{view}");
    let grants = grants_of(&frames, &lease_id);
    assert_eq!(grants.len(), 1, "{frames:?}");
    assert_eq!(grants[0]["body"]["mode"], "local");
    assert_eq!(grants[0]["body"]["fencing_token"], 2);
    assert!(grants[0]["body"].get("duration_ms").is_none(), "{frames:?}");
    assert!(grants[0]["re"].is_null(), "{frames:?}");
    let frames = after_a_tick(&mut on_a).await;
    assert!(grants_of(&frames, &lease_id).is_empty(), "{frames:?}");
    assert!(revokes_of(&frames, &lease_id).is_empty(), "{frames:?}");
}

/// The highest token any primary issues (section 8.5): 2^53 − 1.
const MAX_TOKEN: u64 = (1 << 53) - 1;

/// "The mark" (ADR 0285) at the highest token. B, which the marked row does
/// not name, lists the lease `held` at 2^53 − 1. No write can raise the row
/// above it (section 8.5), so the raise stops there and records the row
/// revoked: B gets its `welcome` and the revoke at the token it reports, the
/// row keeps the mark, and A's next session gets no grant with every write
/// replicated and supported. B's next session lists the same, and the row
/// is not written again.
#[tokio::test]
async fn a_marked_row_listed_at_the_highest_token_is_revoked_at_that_token() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = warm_volume(&server, &owner, &project_id, "web", &a).await;
    let listing = vec![listed(
        &lease_id,
        &app_id,
        LeaseMode::Local,
        MAX_TOKEN,
        "held",
    )];
    let capped = (
        i64::try_from(MAX_TOKEN).unwrap(),
        "expiring".to_string(),
        Some(15_000),
        1,
        true,
    );

    let mut hello = hello_body(&b.node_id);
    hello.leases = listing.clone();
    let (frames, closed) = serve_until_closed(&server, &b, hello, Duration::from_secs(3)).await;
    assert!(!closed, "the session goes on: {frames:?}");
    assert!(
        frames.iter().any(|frame| frame["type"] == "welcome"),
        "{frames:?}"
    );
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], MAX_TOKEN);
    assert_eq!(
        row(&server, &lease_id).await,
        capped,
        "capped, revoked, the mark kept"
    );
    wait_disconnected(&server, &b).await;

    // B's next session: the revoke again, and no second write.
    let written = newest_write(&server, &lease_id).await;
    let (mut on_b, frames) = session(&server, &b, None, listing, true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], MAX_TOKEN);
    assert_eq!(row(&server, &lease_id).await, capped);
    assert_eq!(
        newest_write(&server, &lease_id).await,
        written,
        "no new entry"
    );
    on_b.send(ok_result(None), revokes[0]["id"].as_str()).await;
    on_b.settle().await;

    // A's next session: replicated and supported, and nothing goes out.
    let (mut on_a, frames) = session(&server, &a, Some(on_a.log.clone()), Vec::new(), true).await;
    let head = log_head(&server).await;
    let mut seen = frames;
    seen.extend(follow_to(&mut on_a, head).await);
    follow_to(&mut on_b, head).await;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(server.state.hub.authority().supports(D));
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(pending(&server, &owner, &app_id).await, (false, Vec::new()));
}

/// Section 8.5 step 1 and section 5.1. A's regrant writes the marked row at
/// token 2, a step-4 row for A. B, an old holder, answers its revoke
/// `fence_failed`, and the primary cannot record the failure. A's next
/// session lists the lease `held` `local` at token 1: the row takes the
/// path a recorded failure gives it, not step 4, so A gets the `superseded`
/// revoke at the row's token, and no grant once every other condition
/// holds.
#[tokio::test]
async fn a_step_4_row_with_an_unrecorded_failed_fence_gets_the_superseded_revoke() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    set_policy(&server, &app_id, "warm").await;
    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired");
    let (on_a, _) = session(&server, &a, Some(on_a.log.clone()), vec![lease], true).await;
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "expiring".to_string(), Some(15_000), 1, false)
    );

    server.state.hub.inject_fault(Fault::FailureRecord {
        node_id: b.node_id.clone(),
    });
    let listing = vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "expired")];
    let (mut on_b, frames) = session(&server, &b, None, listing, true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    on_b.send(
        NodeMessage::CommandResult(CommandResult {
            status: CommandStatus::Error,
            detail: Some("the VM may still run".to_string()),
            error: Some(ErrorBody {
                code: ErrorCode::FenceFailed,
                message: "the forced stop could not be confirmed".to_string(),
            }),
            lines: None,
            metrics: None,
        }),
        revokes[0]["id"].as_str(),
    )
    .await;
    on_b.settle().await;
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&b.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(failure, None, "not recorded");

    let a_log = on_a.log.clone();
    drop(on_a);
    wait_disconnected(&server, &a).await;
    let lease = listed(&lease_id, &app_id, LeaseMode::Local, 1, "held");
    let (mut on_a, frames) = session(&server, &a, Some(a_log), vec![lease], true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    assert_eq!(revokes[0]["body"]["fencing_token"], 2);
    assert_eq!(revokes[0]["body"]["reason"], "superseded");
    let mut seen = frames.clone();
    on_a.send(ok_result(None), revokes[0]["id"].as_str()).await;
    seen.extend(on_a.settle().await);

    let head = log_head(&server).await;
    seen.extend(follow_to(&mut on_a, head).await);
    follow_to(&mut on_b, head).await;
    seen.extend(support_keeping_frames(&mut on_a).await);
    on_b.support().await;
    assert!(server.state.hub.authority().supports(D));
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(row(&server, &lease_id).await.0, 2, "no token minted");
}

/// Section 8.5 step 1 and section 5.12. A's unmarked `local` row of a `none`
/// app stands at token 2. B, an old holder, lists the lease `held` at token
/// 1; it answers its revoke `fence_failed`, and the primary cannot record
/// the failure, which it then keeps in memory only. A's next session lists
/// nothing, and no `lease_grant` of the lease goes out to it: the session
/// start's grants read the unrecorded failures as well as the recorded ones.
#[tokio::test]
async fn an_unrecorded_failed_fence_holds_an_unmarked_rows_grant_at_the_session_start() {
    let server = start_with_patient_node_session().await;
    let owner = server.owner().await;
    manual_clock(&server);
    let a = enroll(&server, &owner, "node-a", &["node"]).await;
    let (mut on_a, _) = session(&server, &a, None, Vec::new(), true).await;
    heartbeat(&mut on_a).await;
    let project_id = project(&server, &owner).await;
    let (app_id, lease_id) = app_with_lease(&server, &owner, &mut on_a, &project_id, "web").await;
    run(
        &server,
        "UPDATE leases SET fencing_token = 2 WHERE id = ?",
        &lease_id,
    )
    .await;
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 0, false)
    );
    let a_log = on_a.log.clone();
    drop(on_a);
    wait_disconnected(&server, &a).await;

    let b = enroll(&server, &owner, "node-b", &["node"]).await;
    server.state.hub.inject_fault(Fault::FailureRecord {
        node_id: b.node_id.clone(),
    });
    let listing = vec![listed(&lease_id, &app_id, LeaseMode::Local, 1, "held")];
    let (mut on_b, frames) = session(&server, &b, None, listing, true).await;
    let revokes = revokes_of(&frames, &lease_id);
    assert_eq!(revokes.len(), 1, "{frames:?}");
    on_b.send(
        NodeMessage::CommandResult(CommandResult {
            status: CommandStatus::Error,
            detail: Some("the VM may still run".to_string()),
            error: Some(ErrorBody {
                code: ErrorCode::FenceFailed,
                message: "the forced stop could not be confirmed".to_string(),
            }),
            lines: None,
            metrics: None,
        }),
        revokes[0]["id"].as_str(),
    )
    .await;
    on_b.settle().await;
    let failure: Option<String> =
        sqlx::query_scalar("SELECT fence_failure FROM nodes WHERE id = ?")
            .bind(&b.node_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
    assert_eq!(failure, None, "not recorded");

    let (mut on_a, frames) = session(&server, &a, Some(a_log), Vec::new(), true).await;
    let mut seen = frames;
    seen.extend(after_a_tick(&mut on_a).await);
    assert!(grants_of(&seen, &lease_id).is_empty(), "{seen:?}");
    assert_eq!(
        row(&server, &lease_id).await,
        (2, "local".to_string(), None, 0, false)
    );
}
