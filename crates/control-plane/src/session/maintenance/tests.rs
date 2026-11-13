// SPDX-License-Identifier: AGPL-3.0-only
//! Section 8.11 on the primary: the window's steps, its failures, its
//! closing and the lowering after it, on the conversion tests' fixture (one
//! node in the required set, one `warm` app whose lease row reads `expiring`
//! at token 3, a manual clock and every support).

use super::super::conversion::tests::{
    APP, EverySupport, Fixture, LEASE, assert_answer, fixture, frames, hello, restart,
};
use super::{Ending, MaintenanceError, Opened, Phase, step_bound};
use crate::AppState;
use crate::authority::{DEFAULT_DURATION_MS, VoterSupport};
use crate::session::{NodeHub, Session};
use quoro_proto::wire::{self, NodeMessage};
use quoro_proto::{LeaseId, LeaseMode, LeaseRenew};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

const D: u64 = DEFAULT_DURATION_MS;
/// The second lease of [`two_leases`], on the same node.
const LEASE_2: &str = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B9E";
const APP_2: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9F";

/// Supports a grant of at most `.0` ms (section 8.8).
#[derive(Debug)]
struct UpTo(u64);

impl VoterSupport for UpTo {
    fn supports(&self, duration_ms: u64) -> bool {
        duration_ms <= self.0
    }
}

/// The fixture with its row's creation entry, as every row the primary
/// writes has one: a `warm` app whose lease reads `expiring` at token 3 for
/// `D`.
async fn window_fixture() -> Fixture {
    let f = fixture("warm", "expiring", Some(i64::try_from(D).unwrap()), false).await;
    append_row(&f.state, LEASE).await;
    f
}

/// Appends the change-log entry of `lease_id`'s row as it stands.
async fn append_row(state: &AppState, lease_id: &str) {
    let mut w = state.store.begin_write().await.unwrap();
    let lease = crate::leases::by_id(&mut w.tx, lease_id)
        .await
        .unwrap()
        .unwrap();
    crate::changelog::append(
        &mut w.tx,
        &crate::changelog::Actor::system("test"),
        "lease",
        lease_id,
        crate::changelog::Op::Update,
        lease.payload(),
    )
    .await
    .unwrap();
    w.commit().await.unwrap();
}

/// A second `warm` app on the fixture's node, its lease `expiring` at token
/// 3 for `D`.
async fn two_leases(f: &Fixture) {
    let pool = f.state.store.pool();
    let now = crate::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO apps (id, project_id, name, ha_policy, image, port, health_check, \
         resources, placement, status, created_at, updated_at) \
         VALUES (?, 'prj_c', 'c2', 'warm', 'img', 80, '{}', '{}', '{\"node_ids\":[]}', \
         'stopped', ?, ?)",
    )
    .bind(APP_2)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, duration_ms, granted_at, unsettled) \
         VALUES (?, 'app', ?, ?, 'expiring', 3, ?, ?, 0)",
    )
    .bind(LEASE_2)
    .bind(APP_2)
    .bind(&f.node_id)
    .bind(i64::try_from(D).unwrap())
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    append_row(&f.state, LEASE_2).await;
}

/// A session of `node_id` on `state`'s hub whose `hello` lists `leases`
/// `held` and `expiring` at token 3, its start done.
async fn session_on(state: &AppState, node_id: &str, leases: &[&str]) -> Session {
    let hub = &state.hub;
    let (generation, _rx, _closed, _changed) = hub.attach(node_id);
    let mut session = Session::new(hub.clone(), node_id.to_string(), generation);
    let mut message = hello(node_id, None);
    message.leases = leases
        .iter()
        .map(|lease_id| wire::HelloLease {
            lease_id: (*lease_id).to_string(),
            resource_type: quoro_proto::ResourceType::App,
            resource_id: if *lease_id == LEASE { APP } else { APP_2 }.to_string(),
            mode: LeaseMode::Expiring,
            fencing_token: 3,
            state: "held".to_string(),
        })
        .collect();
    let text = NodeMessage::Hello(message)
        .to_frame("msg_hello", None)
        .to_text();
    let mut outbox = Vec::new();
    assert!(!session.handle_text(&text, &mut outbox).await);
    session
}

/// The node renews `lease_id` at token 3; the frames queued in answer.
async fn renew_lease(session: &mut Session, id: &str, lease_id: &str) -> Vec<Value> {
    let mut outbox = Vec::new();
    let text = NodeMessage::LeaseRenew(LeaseRenew {
        lease_id: LeaseId(lease_id.to_string()),
        fencing_token: 3,
    })
    .to_frame(id, None)
    .to_text();
    session.handle_text(&text, &mut outbox).await;
    frames(outbox)
}

/// The one `lease_grant` among `frames`: its `duration_ms`.
fn answered_ms(frames: &[Value]) -> u64 {
    let grants: Vec<&Value> = frames
        .iter()
        .filter(|frame| frame["type"] == "lease_grant")
        .collect();
    assert_eq!(grants.len(), 1, "{frames:?}");
    grants[0]["body"]["duration_ms"].as_u64().unwrap()
}

/// Starts `quoro cluster maintenance --duration <duration_s>` on `hub`.
fn open(hub: &NodeHub, duration_s: u64) -> JoinHandle<Result<Opened, MaintenanceError>> {
    let hub = hub.clone();
    tokio::spawn(async move { hub.open_maintenance(duration_s).await })
}

/// Waits, on the real clock, until `ready` holds.
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    for _ in 0..200 {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn is_open(hub: &NodeHub) -> bool {
    matches!(hub.0.maintenance.state().phase, Phase::Open { .. })
}

fn is_idle(hub: &NodeHub) -> bool {
    matches!(hub.0.maintenance.state().phase, Phase::Idle)
}

/// The stored `duration_ms` of `lease_id`.
async fn stored(state: &AppState, lease_id: &str) -> Option<i64> {
    let mut conn = state.store.pool().acquire().await.unwrap();
    crate::leases::by_id(&mut conn, lease_id)
        .await
        .unwrap()
        .unwrap()
        .duration_ms
}

/// Waits until the raise of `lease_id` to `w_ms` committed.
async fn until_raised(state: &AppState, lease_id: &str, w_ms: u64) {
    let want = i64::try_from(w_ms).unwrap();
    for _ in 0..200 {
        if stored(state, lease_id).await == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the raise of {lease_id} did not commit");
}

/// The node acknowledges the change log's head on `session`.
async fn ack_head(f: &Fixture, session: &Session) {
    let head: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(f.state.store.pool())
        .await
        .unwrap();
    f.state
        .hub
        .count_log_ack(&f.node_id, session.generation, u64::try_from(head).unwrap());
}

/// Moves the clock by one step's bound until `task` ends, and returns its
/// result.
async fn run_out<T>(f: &Fixture, mut task: JoinHandle<T>) -> T {
    for _ in 0..20 {
        f.clock.advance(step_bound());
        if let Ok(done) = tokio::time::timeout(Duration::from_millis(300), &mut task).await {
            return done.unwrap();
        }
    }
    panic!("the opening did not end");
}

/// One `cluster.maintenance` record: its `params`, `result`,
/// `result_detail` and `change_seq`.
#[derive(Debug)]
struct Recorded {
    params: Value,
    result: String,
    detail: Option<String>,
    change_seq: Option<i64>,
}

/// Every `cluster.maintenance` record, oldest first, after checking the
/// fields `audit-log-format.md` section 3.7 fixes for it.
async fn records(state: &AppState) -> Vec<Recorded> {
    use sqlx::Row as _;
    let rows = sqlx::query(
        "SELECT actor_type, actor_id, channel, target_type, target_id, scope, params, result, \
         result_detail, change_seq FROM audit_log WHERE operation = 'cluster.maintenance' \
         ORDER BY seq",
    )
    .fetch_all(state.store.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|row| {
            let field = |name: &str| row.get::<Option<String>, _>(name);
            assert_eq!(field("actor_type").as_deref(), Some("system"));
            assert_eq!(field("actor_id").as_deref(), Some("cluster"));
            assert_eq!(field("channel").as_deref(), Some("cli"));
            assert_eq!(field("target_type").as_deref(), Some("cluster"));
            assert_eq!(
                field("target_id").as_deref(),
                Some(state.cluster_id.as_str())
            );
            assert_eq!(field("scope"), None);
            Recorded {
                params: serde_json::from_str(&field("params").unwrap()).unwrap(),
                result: field("result").unwrap(),
                detail: field("result_detail"),
                change_seq: row.get("change_seq"),
            }
        })
        .collect()
}

/// Opens a window of `duration_s` that succeeds: the raise is acknowledged
/// and the holder renews. Returns the command's answer, after the frames of
/// that renewal, which carry `W`.
async fn open_window(f: &Fixture, session: &mut Session, duration_s: u64) -> Opened {
    let hub = &f.state.hub;
    let task = open(hub, duration_s);
    until_raised(&f.state, LEASE, duration_s * 1_000).await;
    ack_head(f, session).await;
    until("the window to open", || is_open(hub)).await;
    let answer = renew_lease(session, "msg_open", LEASE).await;
    assert_answer(&answer, "msg_open", 3, duration_s * 1_000);
    task.await.unwrap().unwrap()
}

/// Section 8.11: `W` is at least `D` and at most 600 s. Out of range, the
/// command is refused before anything starts: no support asked for, no
/// raise, no record. The demo's 30 s is in range ([`open_window`] below).
#[tokio::test]
async fn a_window_out_of_range_is_refused_before_anything_starts() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    assert_eq!((super::MIN_WINDOW_S, super::MAX_WINDOW_S), (15, 600));
    for duration_s in [0, 14, 601] {
        assert_eq!(
            hub.open_maintenance(duration_s).await,
            Err(MaintenanceError::OutOfRange { duration_s })
        );
    }
    assert!(is_idle(hub));
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000));
    assert!(records(&f.state).await.is_empty());
    f.done();
}

/// Section 8.11 step 1: the support requests ask for `W` from the moment
/// the window starts opening. Without a majority for `W` in time the window
/// does not open: nothing is raised, renewals are answered with `D`
/// throughout, the requests go back to `D`, and one `error` record says why.
#[tokio::test]
async fn without_a_majority_for_w_nothing_is_raised_and_renewals_get_d() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    hub.set_voter_support(Arc::new(UpTo(D)));
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    let writes = f.writes().await;
    let task = open(hub, 30);
    until("the opening", || !is_idle(hub)).await;
    assert_eq!(hub.support_lease_ms(), 30_000, "the requests ask for W");
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    assert_eq!(
        run_out(&f, task).await,
        Err(MaintenanceError::NoMajority { duration_s: 30 })
    );
    assert!(is_idle(hub));
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(
        stored(&f.state, LEASE).await,
        Some(15_000),
        "nothing raised"
    );
    assert_eq!(f.writes().await, writes, "nothing written to the row");
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        D
    );
    let records = records(&f.state).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].params, serde_json::json!({ "duration_s": 30 }));
    assert_eq!(records[0].result, "error");
    assert!(
        records[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("no majority"),
        "{records:?}"
    );
    f.done();
}

/// Section 8.11 and ADR 0289: a raise not replicated within `D / 3` plus
/// one margin opens no window. Renewals are answered with `D` before and
/// after, the requests go back to `D`, and the `error` record names the
/// machine that did not acknowledge. The raised duration stays stored, the
/// side that only waits longer.
#[tokio::test]
async fn a_raise_not_replicated_in_time_opens_no_window() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE, 30_000).await;
    assert!(!is_open(hub));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D,
        "D until the raise is replicated"
    );
    let result = run_out(&f, task).await;
    let Err(MaintenanceError::NotReplicated { waiting }) = result else {
        panic!("{result:?}");
    };
    assert_eq!(waiting, vec![f.node_id.clone()]);
    assert!(is_idle(hub));
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        D
    );
    assert_eq!(stored(&f.state, LEASE).await, Some(30_000));
    let records = records(&f.state).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].result, "error");
    let detail = records[0].detail.as_deref().unwrap();
    assert!(detail.contains("not replicated"), "{detail}");
    assert!(detail.contains(&f.node_id), "{detail}");
    f.done();
}

/// Section 8.11 steps 2 and 3: once a majority supports `W` and the raise
/// is replicated, the window opens and the holder's renewal is answered with
/// `W`; the command succeeds once every lease had such an answer. The answer's
/// gate is `T_grant + W + 2M(W)`, and the `ok` record carries `{duration_s}`
/// and the raise's change-log position.
#[tokio::test]
async fn once_replicated_every_renewal_gets_w_and_the_command_succeeds() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE, 30_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!task.is_finished(), "not before the holder renewed");
    let answer = renew_lease(&mut session, "msg_r1", LEASE).await;
    assert_answer(&answer, "msg_r1", 3, 30_000);
    let opened = task.await.unwrap().unwrap();
    assert_eq!((opened.duration_s, opened.leases), (30, 1));
    assert!(opened.covered_until.is_some());
    assert_eq!(
        hub.authority().gate_remaining(LEASE),
        Some(Duration::from_secs(36)),
        "30 s + 2 x 3 s"
    );
    let raise_seq: i64 = sqlx::query_scalar(
        "SELECT MAX(seq) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
    )
    .bind(LEASE)
    .fetch_one(f.state.store.pool())
    .await
    .unwrap();
    let records = records(&f.state).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].params, serde_json::json!({ "duration_s": 30 }));
    assert_eq!(
        (records[0].result.as_str(), records[0].change_seq),
        ("ok", Some(raise_seq))
    );
    f.clock.advance(Duration::from_secs(5));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        30_000,
        "every renewal while the window is open"
    );
    f.done();
}

/// Section 8.11: a holder that has not renewed within `D / 3` plus one
/// margin fails the command, and the window stays open: the lease already
/// answered keeps `W`, and the late holder still gets it when it renews.
#[tokio::test]
async fn a_holder_that_does_not_renew_fails_the_command_and_the_others_keep_w() {
    let f = window_fixture().await;
    two_leases(&f).await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE, LEASE_2]).await;
    let task = open(hub, 60);
    until_raised(&f.state, LEASE_2, 60_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        60_000
    );
    let result = run_out(&f, task).await;
    let Err(MaintenanceError::NotRenewed {
        missing, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(missing, vec![(LEASE_2.to_string(), f.node_id.clone())]);
    assert_eq!(answered, 1);
    assert!(is_open(hub), "the window stays open");
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        60_000
    );
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r3", LEASE_2).await),
        60_000
    );
    let records = records(&f.state).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].result, "ok", "the window opened");
    f.done();
}

/// Section 8.11: `--end` closes the window. Renewals are answered with `D`
/// again and the requests ask for `D`, the `W` answer keeps its gate, and
/// `--end` writes `{end: true}`. With no window open it changes and writes
/// nothing.
#[tokio::test]
async fn end_closes_the_window_and_renewals_get_d_again() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    assert!(is_idle(hub));
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    assert_eq!(
        hub.authority().gate_remaining(LEASE),
        Some(Duration::from_secs(36)),
        "the W answer's gate stands"
    );
    let records = records(&f.state).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[1].params, serde_json::json!({ "end": true }));
    assert_eq!(records[1].result, "ok");
    assert_eq!(hub.end_maintenance().await, Ok(Ending::NothingOpen));
    assert_eq!(self::records(&f.state).await.len(), 2);
    f.done();
}

/// Section 8.11: the window closes `W` after it opened. From then on
/// renewals are answered with `D`, and the last `W` answer keeps its gate.
#[tokio::test]
async fn the_window_closes_w_after_it_opened() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    f.clock.advance(Duration::from_secs(29));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        30_000
    );
    f.clock.advance(Duration::from_secs(1));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        D
    );
    assert!(is_idle(hub));
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(
        hub.authority().gate_remaining(LEASE),
        Some(Duration::from_secs(35)),
        "the answer at 29 s gates until 29 + 36 s"
    );
    f.done();
}

/// Section 8.11 and the restart rule of section 8.4: the primary's next
/// start closes the window. The restarted primary answers with `D` and asks
/// for `D`, and counts the stored `W` from its start: `start + W + 2M(W)`.
#[tokio::test]
async fn a_restart_closes_the_window_and_counts_w_from_its_start() {
    let f = window_fixture().await;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    let (after, clock) = restart(&f).await;
    after.hub.set_voter_support(Arc::new(EverySupport));
    assert!(is_idle(&after.hub));
    assert_eq!(after.hub.support_lease_ms(), D);
    assert_eq!(
        after.hub.authority().gate_remaining(LEASE),
        Some(Duration::from_secs(36))
    );
    let mut again = session_on(&after, &f.node_id, &[LEASE]).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut again, "msg_r1", LEASE).await),
        D
    );
    clock.advance(Duration::from_secs(35));
    assert!(!after.hub.authority().passed(LEASE));
    clock.advance(Duration::from_secs(1));
    assert!(after.hub.authority().passed(LEASE));
    f.done();
}

/// Section 8.4: after a window, the stored duration is lowered back to `D`
/// only once the `W` answer's gate has passed, `T_grant + W + 2M(W)`, even
/// with renewals answered with `D` since; a restart after that counts `D`
/// again.
#[tokio::test]
async fn the_stored_duration_goes_back_to_d_once_the_longer_gate_passed() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 60).await;
    assert_eq!(
        hub.lower_durations().await,
        0,
        "not while the window is open"
    );
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    assert_eq!(
        hub.lower_durations().await,
        0,
        "the W answer's gate is open"
    );
    f.clock.advance(Duration::from_secs(71));
    assert_eq!(hub.lower_durations().await, 0, "60 s + 2 x 6 s");
    assert_eq!(stored(&f.state, LEASE).await, Some(60_000));
    f.clock.advance(Duration::from_secs(1));
    assert_eq!(hub.lower_durations().await, 1);
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000));
    assert_eq!(hub.lower_durations().await, 0, "once");
    let (after, _) = restart(&f).await;
    assert_eq!(
        after.hub.authority().gate_remaining(LEASE),
        Some(Duration::from_secs(18)),
        "a restart counts D again"
    );
    f.done();
}

/// No lowering runs while a window is open, even for a raised lease whose
/// gates have all passed because its holder has not renewed yet: lowering it
/// would take away the `W` the window is about to answer. Once the window
/// ends, the raise of a lease never answered with `W` is lowered at once.
#[tokio::test]
async fn no_lowering_while_a_window_is_open() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    let task = open(hub, 60);
    until_raised(&f.state, LEASE, 60_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    f.clock.advance(Duration::from_secs(20));
    assert!(hub.authority().passed(LEASE), "the D answer's gate passed");
    assert_eq!(hub.lower_durations().await, 0);
    assert_eq!(stored(&f.state, LEASE).await, Some(60_000));
    assert!(matches!(
        task.await.unwrap(),
        Err(MaintenanceError::NotRenewed { .. })
    ));
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    assert_eq!(hub.lower_durations().await, 1);
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000));
    f.done();
}

/// A second window is refused while one is opening or open, with no record.
#[tokio::test]
async fn a_second_window_is_refused_while_one_is_open() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    assert_eq!(hub.open_maintenance(60).await, Err(MaintenanceError::Busy));
    assert_eq!(records(&f.state).await.len(), 1);
    f.done();
}

/// With no `expiring` lease the window opens at once and covers nothing.
#[tokio::test]
async fn with_no_expiring_lease_the_window_opens_and_covers_nothing() {
    let f = fixture("warm", "local", Some(i64::try_from(D).unwrap()), false).await;
    let opened = f.state.hub.open_maintenance(15).await.unwrap();
    assert_eq!((opened.leases, opened.covered_until), (0, None));
    assert!(is_open(&f.state.hub));
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000), "a local row");
    f.done();
}

/// The `max_lease_ms` of the support requests: `W` from the start of an
/// opening, `D` again after a failed one, `--end` or the end of `W`, with a
/// change on the watch the sessions follow at each of the first three.
#[test]
fn the_support_requests_ask_for_w_only_while_a_window_opens_or_is_open() {
    let window = super::Window::default();
    let mut watched = window.subscribe();
    let at = Duration::from_secs;
    assert_eq!(window.support_ms(at(0)), D);
    let first = window.begin(30_000, at(0)).unwrap();
    assert_eq!(window.support_ms(at(0)), 30_000);
    assert!(watched.has_changed().unwrap());
    assert_eq!(*watched.borrow_and_update(), 30_000);
    assert_eq!(window.begin(60_000, at(0)), None, "busy");
    window.abort(first);
    assert_eq!(window.support_ms(at(0)), D);
    assert_eq!(*watched.borrow_and_update(), D);

    let second = window.begin(30_000, at(1)).unwrap();
    assert!(second > first);
    window.abort(first);
    assert_eq!(
        window.support_ms(at(1)),
        30_000,
        "an older opening aborts nothing"
    );
    let now_wall = time::OffsetDateTime::UNIX_EPOCH;
    assert!(window.open(second, at(1), now_wall, std::collections::BTreeMap::new()));
    assert_eq!(window.support_ms(at(30)), 30_000);
    assert_eq!(window.support_ms(at(31)), D, "W after it opened");
    assert!(window.is_idle(at(31)));

    let third = window.begin(30_000, at(40)).unwrap();
    assert_eq!(window.end(at(40)), Ending::Ended);
    assert!(!window.still(third, at(40)));
    assert!(!window.open(third, at(40), now_wall, std::collections::BTreeMap::new()));
    assert_eq!(window.support_ms(at(40)), D);
    assert_eq!(window.end(at(40)), Ending::NothingOpen);
}
