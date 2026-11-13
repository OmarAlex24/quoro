// SPDX-License-Identifier: AGPL-3.0-only
//! Section 8.11 on the primary: the window's steps, its failures, its
//! closing and the lowering after it, on the conversion tests' fixture (one
//! node in the required set, one `warm` app whose lease row reads `expiring`
//! at token 3, a manual clock and every support).

use super::super::conversion::tests::{
    APP, EverySupport, Fixture, LEASE, assert_answer, fixture, frames, hello, restart,
};
use super::{Ending, MaintenanceError, Opened, Phase, Uncovered, Why, step_bound};
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
/// A third lease, on the same node.
const LEASE_3: &str = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B9F";
const APP_3: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9G";
/// A `revoked_at` for [`set_row`].
const REVOKED: &str = "2026-11-12T00:00:00.000Z";

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

/// Another `warm` app on the fixture's node, `app_id`, its lease `lease_id`
/// reading `mode` at token 3 and storing `stored_ms`, with its change-log
/// entry.
async fn add_lease(f: &Fixture, lease_id: &str, app_id: &str, mode: &str, stored_ms: u64) {
    let pool = f.state.store.pool();
    let now = crate::store::now_rfc3339();
    sqlx::query(
        "INSERT INTO apps (id, project_id, name, ha_policy, image, port, health_check, \
         resources, placement, status, created_at, updated_at) \
         VALUES (?, 'prj_c', ?, 'warm', 'img', 80, '{}', '{}', '{\"node_ids\":[]}', \
         'stopped', ?, ?)",
    )
    .bind(app_id)
    // A name of `[a-z0-9-]`: the id without its prefix.
    .bind(app_id.trim_start_matches("app_").to_lowercase())
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
         fencing_token, duration_ms, granted_at, unsettled) \
         VALUES (?, 'app', ?, ?, ?, 3, ?, ?, 0)",
    )
    .bind(lease_id)
    .bind(app_id)
    .bind(&f.node_id)
    .bind(mode)
    .bind(i64::try_from(stored_ms).unwrap())
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    append_row(&f.state, lease_id).await;
}

/// A second `warm` app on the fixture's node, its lease `expiring` at token
/// 3 for `D`.
async fn two_leases(f: &Fixture) {
    add_lease(f, LEASE_2, APP_2, "expiring", D).await;
}

/// Runs `update`, whose one parameter is the lease id, on `lease_id`'s row
/// and appends the row's change-log entry, as every write of the primary
/// does.
async fn set_row(state: &AppState, lease_id: &str, update: &str) {
    sqlx::query(update)
        .bind(lease_id)
        .execute(state.store.pool())
        .await
        .unwrap();
    append_row(state, lease_id).await;
}

/// The row of `lease_id` as it stands.
async fn row_of(state: &AppState, lease_id: &str) -> crate::leases::Lease {
    let mut conn = state.store.pool().acquire().await.unwrap();
    crate::leases::by_id(&mut conn, lease_id)
        .await
        .unwrap()
        .unwrap()
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
            resource_id: match *lease_id {
                LEASE => APP,
                LEASE_2 => APP_2,
                _ => APP_3,
            }
            .to_string(),
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
    row_of(state, lease_id).await.duration_ms
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

/// The result of `task`, which ends without the clock moving.
async fn ended<T>(task: JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the opening ends without the clock moving")
        .unwrap()
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
    let before = time::OffsetDateTime::now_utc();
    let answer = renew_lease(&mut session, "msg_r1", LEASE).await;
    let after = time::OffsetDateTime::now_utc();
    assert_answer(&answer, "msg_r1", 3, 30_000);
    let opened = task.await.unwrap().unwrap();
    assert_eq!((opened.duration_s, opened.leases), (30, 1));
    // `T_grant + W - 2M(W)`: 30 s - 2 x 3 s after the answer went out, one
    // margin earlier than the holder's own `W - M(W)` from its send time.
    let covered = time::OffsetDateTime::parse(
        opened.covered_until.as_deref().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    let valid = time::Duration::seconds(24);
    assert!(
        covered >= before + valid - time::Duration::milliseconds(1) && covered <= after + valid,
        "{covered} is not 24 s after the answer ({before} to {after})"
    );
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
    let Err(MaintenanceError::NotCovered {
        leases, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(
        leases,
        vec![Uncovered {
            lease_id: LEASE_2.to_string(),
            holder: f.node_id.clone(),
            why: Why::NotRenewed,
        }]
    );
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
    assert_eq!(after.hub.lower_durations().await, 0, "the seeded W gate");
    clock.advance(Duration::from_secs(35));
    assert!(!after.hub.authority().passed(LEASE));
    assert_eq!(
        after.hub.lower_durations().await,
        0,
        "not before start + 36 s"
    );
    assert_eq!(stored(&after, LEASE).await, Some(30_000));
    clock.advance(Duration::from_secs(1));
    assert!(after.hub.authority().passed(LEASE));
    assert_eq!(after.hub.lower_durations().await, 1);
    assert_eq!(stored(&after, LEASE).await, Some(15_000));
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
        Err(MaintenanceError::NotCovered { .. })
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
    window.close(first);
    assert_eq!(window.support_ms(at(0)), D);
    assert_eq!(*watched.borrow_and_update(), D);

    let second = window.begin(30_000, at(1)).unwrap();
    assert!(second > first);
    window.close(first);
    assert_eq!(
        window.support_ms(at(1)),
        30_000,
        "an older opening closes nothing"
    );
    let now_wall = time::OffsetDateTime::UNIX_EPOCH;
    assert!(window.open(second, at(1), now_wall, std::collections::BTreeMap::new()));
    window.close(first);
    assert!(
        window.still(second, at(1)),
        "nor does it close a later window that is open"
    );
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

/// Section 8.11: a `W` answer needs a majority for `W` at the moment it goes
/// out. When support for `W` is lost after the window opened, the holder's
/// renewal is answered with `D`, which a majority still supports, not left
/// unanswered; once `W` is supported again, the answer carries `W` again.
#[tokio::test]
async fn support_for_w_lost_after_the_window_opened_answers_with_d() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    hub.set_voter_support(Arc::new(UpTo(D)));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    hub.set_voter_support(Arc::new(EverySupport));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        30_000
    );
    f.done();
}

/// A lease of the window whose row is marked during the window is answered
/// with `D`: a marked lease is between modes (section 8.4, "The mark"), and
/// the window covers only unmarked `expiring` rows.
#[tokio::test]
async fn a_lease_marked_during_the_window_is_answered_with_d() {
    let f = window_fixture().await;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    set_row(
        &f.state,
        LEASE,
        "UPDATE leases SET unsettled = 1 WHERE id = ?",
    )
    .await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    f.done();
}

/// A lease of the window granted again during the window, its stored
/// duration back at `D`, is answered with `D`: a `W` answer goes out only
/// over a row whose replicated stored duration already covers `W` (ADR
/// 0268).
#[tokio::test]
async fn a_lease_stored_back_at_d_during_the_window_is_answered_with_d() {
    let f = window_fixture().await;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    set_row(
        &f.state,
        LEASE,
        "UPDATE leases SET duration_ms = 15000 WHERE id = ?",
    )
    .await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    assert_eq!(
        stored(&f.state, LEASE).await,
        Some(15_000),
        "nothing raised"
    );
    f.done();
}

/// A lease of the window revoked during the window gets no `W`: its renewal
/// is answered with a revoke, and the window's answer for its row is `D`
/// whatever path reads it.
#[tokio::test]
async fn a_lease_revoked_during_the_window_gets_no_w() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 30).await;
    let unrevoked = row_of(&f.state, LEASE).await;
    assert_eq!(hub.window_answer_ms(&unrevoked, &f.node_id), Some(30_000));
    set_row(
        &f.state,
        LEASE,
        &format!("UPDATE leases SET revoked_at = '{REVOKED}' WHERE id = ?"),
    )
    .await;
    let answer = renew_lease(&mut session, "msg_r1", LEASE).await;
    assert!(
        !answer.iter().any(|frame| frame["type"] == "lease_grant"),
        "{answer:?}"
    );
    assert!(
        answer.iter().any(|frame| frame["type"] == "lease_revoke"),
        "{answer:?}"
    );
    let revoked = row_of(&f.state, LEASE).await;
    assert_eq!(hub.window_answer_ms(&revoked, &f.node_id), None);
    f.done();
}

/// Another node, `node-b`, enrolled as a copy of the fixture's node with its
/// own subnet and no session.
async fn other_node(f: &Fixture) -> String {
    let other = "nod_01JC9Z8K2M4P6R8T0V2X4Z6B9Q";
    let mut conn = f.state.store.pool().acquire().await.unwrap();
    sqlx::query("CREATE TEMP TABLE copied AS SELECT * FROM nodes WHERE id = ?")
        .bind(&f.node_id)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE copied SET id = ?, name = 'node-b', cp_status = NULL, \
         subnet = '10.42.2.0/24', status = 'disconnected'",
    )
    .bind(other)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query("INSERT INTO nodes SELECT * FROM copied")
        .execute(&mut *conn)
        .await
        .unwrap();
    other.to_string()
}

/// A lease of the window whose row names another node than the one the
/// window raised it for, here moved to node B before its first holder A
/// renewed it: A's renewal gets a revoke, B's is answered with `D` although
/// the row stores `W`, and the command names the lease, held by B, with the
/// other-holder reason. The authority refuses a renewal's answer to any node
/// but the holder of the lease's gate, so B's renewal reaches the window's
/// holder check only while the lease has no gate in this process, as here.
#[tokio::test]
async fn a_lease_granted_to_another_node_during_the_window_is_answered_with_d() {
    let f = window_fixture().await;
    two_leases(&f).await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE, LEASE_2]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE_2, 30_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    let other = other_node(&f).await;
    sqlx::query("UPDATE leases SET holder_node_id = ? WHERE id = ?")
        .bind(&other)
        .bind(LEASE)
        .execute(f.state.store.pool())
        .await
        .unwrap();
    append_row(&f.state, LEASE).await;
    assert_eq!(stored(&f.state, LEASE).await, Some(30_000));

    let answer = renew_lease(&mut session, "msg_a", LEASE).await;
    assert!(
        !answer.iter().any(|frame| frame["type"] == "lease_grant"),
        "{answer:?}"
    );
    assert!(
        answer.iter().any(|frame| frame["type"] == "lease_revoke"),
        "{answer:?}"
    );
    let mut on_b = session_on(&f.state, &other, &[LEASE]).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut on_b, "msg_b", LEASE).await),
        D,
        "the window raised the lease for A, not for B"
    );
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_a2", LEASE_2).await),
        30_000
    );

    let result = run_out(&f, task).await;
    let Err(MaintenanceError::NotCovered {
        leases, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(
        leases,
        vec![Uncovered {
            lease_id: LEASE.to_string(),
            holder: other,
            why: Why::OtherHolder,
        }]
    );
    assert_eq!(answered, 1);
    f.done();
}

/// Section 8.11 step 2: the raise skips a marked row and a revoked one. The
/// window covers the other lease, and the command fails naming the marked
/// lease and why (a revoked one needs no cover); the window stays open for
/// the lease it covers, and its `ok` record stands.
#[tokio::test]
async fn the_raise_skips_marked_and_revoked_rows_and_the_command_names_the_marked_one() {
    let f = fixture("warm", "expiring", Some(i64::try_from(D).unwrap()), true).await;
    add_lease(&f, LEASE_2, APP_2, "expiring", D).await;
    add_lease(&f, LEASE_3, APP_3, "expiring", D).await;
    set_row(
        &f.state,
        LEASE_3,
        &format!("UPDATE leases SET revoked_at = '{REVOKED}' WHERE id = ?"),
    )
    .await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE_2]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE_2, 30_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE_2).await),
        30_000
    );
    let result = ended(task).await;
    let Err(error @ MaintenanceError::NotCovered { .. }) = result else {
        panic!("{result:?}");
    };
    let MaintenanceError::NotCovered {
        leases, answered, ..
    } = &error
    else {
        unreachable!();
    };
    assert_eq!(
        leases,
        &vec![Uncovered {
            lease_id: LEASE.to_string(),
            holder: f.node_id.clone(),
            why: Why::Marked,
        }]
    );
    assert_eq!(*answered, 1);
    let printed = error.to_string();
    assert!(
        printed.contains(&format!(
            "\n  {LEASE} on {}: it carries the unsettled mark",
            f.node_id
        )),
        "{printed}"
    );
    assert!(
        is_open(hub),
        "the window stays open for the lease it covers"
    );
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000), "marked");
    assert_eq!(stored(&f.state, LEASE_3).await, Some(15_000), "revoked");
    let records = records(&f.state).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].result, "ok", "the window opened");
    f.done();
}

/// Section 8.11: a lease that turns `expiring` after the raise gets only `D`
/// from this window, so the command does not succeed while it exists, even
/// once every lease of the window was answered with `W`.
#[tokio::test]
async fn a_lease_that_turns_expiring_during_the_window_fails_the_command() {
    let f = window_fixture().await;
    add_lease(&f, LEASE_2, APP_2, "local", D).await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE, 30_000).await;
    assert_eq!(stored(&f.state, LEASE_2).await, Some(15_000), "a local row");
    set_row(
        &f.state,
        LEASE_2,
        "UPDATE leases SET mode = 'expiring' WHERE id = ?",
    )
    .await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        30_000
    );
    let result = ended(task).await;
    let Err(MaintenanceError::NotCovered {
        leases, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(
        leases,
        vec![Uncovered {
            lease_id: LEASE_2.to_string(),
            holder: f.node_id.clone(),
            why: Why::NotRaised,
        }]
    );
    assert_eq!(answered, 1);
    assert!(is_open(hub));
    // Its renewals get no `W` from this window. (The row turned `expiring`
    // here by hand, without the conversion grant that records its gate, so
    // the authority itself answers none of them.)
    let row = row_of(&f.state, LEASE_2).await;
    assert_eq!(hub.window_answer_ms(&row, &f.node_id), None);
    f.done();
}

/// A holder counts its validity from its newest answer, so a lease whose
/// `W` answer was followed by a `D` answer is not covered: the command fails
/// naming it, even though it was answered with `W` once.
#[tokio::test]
async fn a_lease_whose_newest_answer_carried_d_is_not_covered() {
    let f = window_fixture().await;
    two_leases(&f).await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE, LEASE_2]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE_2, 30_000).await;
    ack_head(&f, &session).await;
    until("the window to open", || is_open(hub)).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        30_000
    );
    hub.set_voter_support(Arc::new(UpTo(D)));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r2", LEASE).await),
        D
    );
    hub.set_voter_support(Arc::new(EverySupport));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r3", LEASE_2).await),
        30_000
    );
    let result = run_out(&f, task).await;
    let Err(MaintenanceError::NotCovered {
        leases, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(
        leases,
        vec![Uncovered {
            lease_id: LEASE.to_string(),
            holder: f.node_id.clone(),
            why: Why::AnsweredWithD,
        }]
    );
    assert_eq!(answered, 1);
    f.done();
}

/// An `expiring` row the change log has no entry of, and that already stores
/// at least `W`, is left out of the raise: the command names it with that
/// reason.
#[tokio::test]
async fn a_row_with_no_change_log_entry_is_named_as_not_covered() {
    let f = fixture("warm", "expiring", Some(60_000), false).await;
    let result = f.state.hub.open_maintenance(30).await;
    let Err(MaintenanceError::NotCovered {
        leases, answered, ..
    }) = result
    else {
        panic!("{result:?}");
    };
    assert_eq!(
        leases,
        vec![Uncovered {
            lease_id: LEASE.to_string(),
            holder: f.node_id.clone(),
            why: Why::NoLogEntry,
        }]
    );
    assert_eq!(answered, 0);
    f.done();
}

/// The lowering skips a marked row, a revoked one and one this process has
/// no gate of, whatever they store: only an unmarked, unrevoked `expiring`
/// row whose every longer gate this process knows has passed goes back to
/// `D`.
#[tokio::test]
async fn the_lowering_skips_marked_revoked_and_gate_less_rows() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 60).await;
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    f.clock.advance(Duration::from_secs(72));
    // Written after this process started and granted nothing since: no gate.
    add_lease(&f, LEASE_2, APP_2, "expiring", 60_000).await;
    assert!(!hub.authority().has_gate(LEASE_2));

    set_row(
        &f.state,
        LEASE,
        "UPDATE leases SET unsettled = 1 WHERE id = ?",
    )
    .await;
    assert_eq!(hub.lower_durations().await, 0, "marked, and no gate");
    set_row(
        &f.state,
        LEASE,
        &format!("UPDATE leases SET unsettled = 0, revoked_at = '{REVOKED}' WHERE id = ?"),
    )
    .await;
    assert_eq!(hub.lower_durations().await, 0, "revoked, and no gate");
    assert_eq!(stored(&f.state, LEASE).await, Some(60_000));
    assert_eq!(stored(&f.state, LEASE_2).await, Some(60_000));

    set_row(
        &f.state,
        LEASE,
        "UPDATE leases SET revoked_at = NULL WHERE id = ?",
    )
    .await;
    assert_eq!(hub.lower_durations().await, 1);
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000));
    assert_eq!(stored(&f.state, LEASE_2).await, Some(60_000), "no gate");
    f.done();
}

/// `--end` while the window waits for a majority: the opening stops at once,
/// with an `error` record that says so, and `--end` writes its own record.
#[tokio::test]
async fn end_while_the_window_waits_for_a_majority_records_an_error_and_the_end() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    hub.set_voter_support(Arc::new(UpTo(D)));
    let task = open(hub, 30);
    until("the opening", || !is_idle(hub)).await;
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    assert_eq!(
        ended(task).await,
        Err(MaintenanceError::Ended { opened: false })
    );
    assert_eq!(hub.support_lease_ms(), D);
    assert_eq!(stored(&f.state, LEASE).await, Some(15_000));
    assert_opening_ended(&f.state).await;
    f.done();
}

/// `--end` while the window waits for its raise to be replicated: the
/// opening stops at once, nothing is answered with `W`, the raised duration
/// stays stored, and the records are the same as above.
#[tokio::test]
async fn end_while_the_raise_is_replicated_records_an_error_and_the_end() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    let task = open(hub, 30);
    until_raised(&f.state, LEASE, 30_000).await;
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    assert_eq!(
        ended(task).await,
        Err(MaintenanceError::Ended { opened: false })
    );
    assert!(is_idle(hub));
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        D
    );
    assert_eq!(stored(&f.state, LEASE).await, Some(30_000));
    assert_opening_ended(&f.state).await;
    f.done();
}

/// The records of an opening that `--end` ended: one `error` with
/// `{duration_s}` that says the window did not open, and one `ok` with
/// `{end: true}`, in either order.
async fn assert_opening_ended(state: &AppState) {
    let records = records(state).await;
    assert_eq!(records.len(), 2, "{records:?}");
    let error = records
        .iter()
        .find(|record| record.result == "error")
        .expect("an error record");
    assert_eq!(error.params, serde_json::json!({ "duration_s": 30 }));
    let detail = error.detail.as_deref().unwrap();
    assert!(
        detail.contains("ended with --end while it was opening"),
        "{detail}"
    );
    let end = records
        .iter()
        .find(|record| record.result == "ok")
        .expect("the end record");
    assert_eq!(end.params, serde_json::json!({ "end": true }));
}

/// The `ok` record's `change_seq` is a write of this raise: a window whose
/// lease already stores at least `W` raises nothing, and its record carries
/// no `change_seq`.
#[tokio::test]
async fn a_window_that_raises_nothing_records_no_change_seq() {
    let f = window_fixture().await;
    let hub = &f.state.hub;
    let mut session = session_on(&f.state, &f.node_id, &[LEASE]).await;
    open_window(&f, &mut session, 60).await;
    assert_eq!(hub.end_maintenance().await, Ok(Ending::Ended));
    let writes = f.writes().await;
    let task = open(hub, 30);
    until("the window to open", || is_open(hub)).await;
    assert_eq!(
        answered_ms(&renew_lease(&mut session, "msg_r1", LEASE).await),
        30_000
    );
    task.await.unwrap().unwrap();
    assert_eq!(f.writes().await, writes, "nothing raised");
    let records = records(&f.state).await;
    assert_eq!(records.len(), 3, "{records:?}");
    assert!(records[0].change_seq.is_some(), "the first window raised");
    assert_eq!(records[2].params, serde_json::json!({ "duration_s": 30 }));
    assert_eq!(
        (records[2].result.as_str(), records[2].change_seq),
        ("ok", None)
    );
    f.done();
}
