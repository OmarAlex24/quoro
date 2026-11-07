// SPDX-License-Identifier: AGPL-3.0-only
//! `expiring` leases on the node's lease service: renewals, `uncertain`,
//! expiry and its self-fence, a host suspend, and a held lease across an agent
//! restart (`node-agent-protocol.md` sections 5.5, 5.12, 8.4 and 8.5; ADR 0072,
//! ADR 0240).
//!
//! These run in real time with `D` of a few seconds, because the service
//! stores every change in SQLite and a paused clock would jump while it waits.
//! Each bound is checked against an instant the test knows is at or before
//! the one the service used.

mod agent_common;
mod common;

use std::sync::Arc;
use std::time::Duration;

use agent_common::{TargetDir, snapshot_full, stateful_vm};
use common::{millis, running_vm, stop_counts, wait_for};
use quoro_engine::{Engine, FakeEngine, VmState};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::{
    Attached, FenceTimers, HostClock, LeaseConfig, LeaseState, LocalLeases, ManualHostClock,
    StoredLease, StoredValidity, VmFenceOutcome, start_local_leases, stop_vms,
};
use quoro_proto::{
    CommandStatus, Envelope, LeaseGrant, LeaseId, LeaseMode, LeaseRenew, LeaseRevoke, MsgId,
    ResourceType, RevokeReason,
};
use tokio::sync::watch;
use tokio::time::Instant;

const LEASE: &str = "lse_warm";
const APP: &str = "app_warm";

fn clock() -> ManualHostClock {
    ManualHostClock::new(Some("boot-a"), Duration::from_secs(1000))
}

fn config(clock: &ManualHostClock) -> LeaseConfig {
    LeaseConfig::new(
        "nod_test",
        FenceTimers {
            grace: Duration::from_secs(10),
            force: Duration::from_secs(5),
        },
    )
    .with_host_clock(Arc::new(clock.clone()))
}

fn expiring(token: u64, duration_ms: u64) -> LeaseGrant {
    LeaseGrant {
        lease_id: LeaseId(LEASE.to_owned()),
        resource_type: ResourceType::App,
        resource_id: APP.to_owned(),
        mode: LeaseMode::Expiring,
        fencing_token: token,
        duration_ms: Some(duration_ms),
        granted_at: "2026-11-02T10:15:33Z".to_owned(),
    }
}

/// A snapshot that binds `vms` to the lease.
fn bound(vms: &[&str]) -> watch::Sender<Option<quoro_node_agent::reconcile::Snapshot>> {
    let snapshot = snapshot_full(
        1,
        vms.iter()
            .map(|vm| stateful_vm(vm, LEASE, 1, Vec::new()))
            .collect(),
        Vec::new(),
        Vec::new(),
    );
    watch::channel(Some(snapshot)).0
}

async fn start(
    dir: &TargetDir,
    engine: &Arc<FakeEngine>,
    desired: &watch::Sender<Option<quoro_node_agent::reconcile::Snapshot>>,
    config: LeaseConfig,
) -> (LocalLeases, NodeStore) {
    let store = NodeStore::open(dir.path()).await.unwrap();
    let leases = start_local_leases(
        Arc::clone(engine),
        store.clone(),
        config,
        desired.subscribe(),
    )
    .await
    .expect("the lease service starts");
    (leases, store)
}

/// The next `lease_renew` the service hands the session.
async fn next_renewal(attached: &mut Attached) -> Envelope<LeaseRenew> {
    tokio::time::timeout(Duration::from_secs(10), attached.renewals.recv())
        .await
        .expect("a renewal in time")
        .expect("the service runs")
}

/// Answers `renew` the way the primary does: the same token, `re` set.
async fn answer(leases: &LocalLeases, renew: &Envelope<LeaseRenew>, duration_ms: u64) {
    leases
        .link
        .grant(
            MsgId(format!("msg_answer_{}", renew.id.0)),
            Some(renew.id.clone()),
            expiring(renew.body.fencing_token, duration_ms),
        )
        .await
        .expect("the answer is taken");
}

/// Grants the lease, answers the renewal the node sends at once, and returns
/// the instant just before the grant: the renewal's send time is no earlier.
async fn grant_and_confirm(
    leases: &LocalLeases,
    attached: &mut Attached,
    duration_ms: u64,
) -> Instant {
    let before = Instant::now();
    leases
        .link
        .grant(
            MsgId("msg_grant".to_owned()),
            None,
            expiring(1, duration_ms),
        )
        .await
        .expect("the grant is taken");
    // Section 8.4 "First grant": `uncertain` until a renewal is answered.
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(!leases.view.may_start_stateful(LEASE, 1));
    let renew = next_renewal(attached).await;
    assert_eq!(renew.body.lease_id.0, LEASE);
    assert_eq!(renew.body.fencing_token, 1, "the renewal echoes the token");
    assert!(
        before.elapsed() < millis(500),
        "the first renewal goes out at once"
    );
    answer(leases, &renew, duration_ms).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    before
}

async fn events_of(store: &NodeStore, kind: &str) -> Vec<serde_json::Value> {
    store
        .buffered_events()
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.body)
        .filter(|body| body["type"] == kind)
        .collect()
}

async fn wait_for_fence_record(store: &NodeStore) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while store.fences().await.unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the fence is recorded"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Sections 5.5 and 8.4: renewals answered in time keep the lease `held`,
/// past the `valid_until` of the first answer, and every renewal echoes the
/// held token.
#[tokio::test]
async fn renewals_answered_in_time_keep_the_lease_held() {
    let dir = TargetDir::new("expiring-renewed");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_kept"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_kept", 2).await;
    let mut attached = leases.link.attach();

    let before = grant_and_confirm(&leases, &mut attached, 3000).await;
    // `D` = 3 000: a renewal every 1 000 ms, and `valid_until` 2 000 ms after
    // each answered one. Three answered renewals carry it past 3 000 ms.
    for _ in 0..3 {
        let renew = next_renewal(&mut attached).await;
        assert_eq!(renew.body.fencing_token, 1);
        answer(&leases, &renew, 3000).await;
        assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    }
    assert!(before.elapsed() >= millis(2900), "{:?}", before.elapsed());
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4 with the margin of the worked example scaled down
/// (`D` = 6 000, `M` = 1 000): a renewal left unanswered for `D / 3` makes the
/// lease `uncertain`, which keeps its VM and starts nothing; at `valid_until`
/// the lease expires and the fence, within its own timers clamped to `M`,
/// finishes before `t_send + D`.
#[tokio::test]
async fn an_unanswered_lease_goes_uncertain_then_expires_and_fences_in_time() {
    let dir = TargetDir::new("expiring-expires");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    // The guest ignores the power button and the VMM is slow, so the graceful
    // stop runs out its time and the forced stop follows.
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(100));
    let mut attached = leases.link.attach();

    let before = grant_and_confirm(&leases, &mut attached, 6000).await;
    // The renewal at +2 000 goes unanswered; it is late at +4 000.
    let _unanswered = next_renewal(&mut attached).await;
    tokio::time::sleep_until(before + millis(4500)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert!(!leases.view.may_start_stateful(LEASE, 1));
    assert!(engine.stop_calls().is_empty(), "uncertain stops nothing");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);

    // `valid_until` = `t_send` + 5 000; the fence deadline is `t_send` + 6 000.
    wait_for_fence_record(&store).await;
    let done = before.elapsed();
    assert!(done >= millis(5000), "no early fence: {done:?}");
    assert!(done <= millis(6000), "fenced by t_send + D: {done:?}");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(
        stop_counts(&engine.stop_calls()),
        (1, 1),
        "a graceful stop, then a forced one"
    );
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0]["data"]["reason"], "lease_expired");
    assert_eq!(fenced[0]["data"]["lease_ids"][0], LEASE);
    assert_eq!(fenced[0]["data"]["app_ids"][0], APP);
    assert_eq!(
        fenced[0]["data"]["fencing_tokens"],
        serde_json::json!({ LEASE: 1 })
    );
    assert_eq!(fenced[0]["data"]["vm_ids"], serde_json::json!(["vm_warm"]));
    let rows = store.load_leases().await.unwrap();
    assert_eq!(rows[0].state, LeaseState::Expired);
    assert_eq!(rows[0].validity, None);
    leases.abort();
}

/// Section 8.5 "Concurrency": every VM of the lease stops at the same time,
/// so the whole fence fits in `M`, not once per VM.
#[tokio::test]
async fn an_expiry_stops_every_bound_vm_together_within_the_margin() {
    let dir = TargetDir::new("expiring-concurrent");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_first", "vm_second"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let first = running_vm(&engine, "vm_first", 2).await;
    let second = running_vm(&engine, "vm_second", 3).await;
    for vm in [&first, &second] {
        engine.set_ignore_power_button(vm, true);
        engine.set_stop_delay(vm, millis(100));
    }
    let mut attached = leases.link.attach();

    // `D` = 3 000, `M` = 1 000: the fence starts at `t_send` + 2 000.
    let before = grant_and_confirm(&leases, &mut attached, 3000).await;
    wait_for_fence_record(&store).await;
    let done = before.elapsed();
    assert!(done >= millis(2000), "no early fence: {done:?}");
    assert!(
        done <= millis(3000),
        "both stops fit in M together: {done:?}"
    );
    for vm in [&first, &second] {
        assert_eq!(engine.inspect(vm).await.unwrap().state, VmState::Stopped);
    }
    assert_eq!(stop_counts(&engine.stop_calls()), (2, 2));
    let fences = store.fences().await.unwrap();
    assert!(fences[0].complete);
    leases.abort();
}

/// D-0074, the expiring variant: an expiry on a VM that cannot be stopped
/// emits `node.fence_failed` with reason `lease_expired` and marks the node
/// failed, and no `node.fenced` goes out.
#[tokio::test]
async fn an_expiring_lease_that_cannot_be_stopped_reports_fence_failed() {
    let dir = TargetDir::new("expiring-stuck");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_stuck"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_stuck", 2).await;
    engine.set_unkillable(&vm, true);
    let mut attached = leases.link.attach();

    let _before = grant_and_confirm(&leases, &mut attached, 3000).await;
    wait_for_fence_record(&store).await;
    let failed = events_of(&store, "node.fence_failed").await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["data"]["reason"], "lease_expired");
    assert_eq!(failed[0]["data"]["lease_ids"][0], LEASE);
    assert_eq!(failed[0]["data"]["vm_ids"][0], "vm_stuck");
    assert_eq!(
        failed[0]["data"]["fencing_tokens"],
        serde_json::json!({ LEASE: 1 })
    );
    assert!(events_of(&store, "node.fenced").await.is_empty());
    assert!(leases.view.is_failed());
    assert!(store.node_failed().await.unwrap());
    assert!(!store.fences().await.unwrap()[0].complete);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4: losing the session makes a `held` lease `uncertain` (keep
/// running, start nothing); the next session renews it at once, and the
/// answer makes it `held` again.
#[tokio::test]
async fn a_lost_session_makes_the_lease_uncertain_until_the_next_one_renews() {
    let dir = TargetDir::new("expiring-detached");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;

    drop(attached);
    wait_for("the lease to go uncertain", || {
        leases.view.state(LEASE) == Some(LeaseState::Uncertain)
    })
    .await;
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert!(!leases.view.may_start_stateful(LEASE, 1));
    assert_eq!(leases.link.hello()[0].state, "uncertain");

    let reattached_at = Instant::now();
    let mut attached = leases.link.attach();
    let renew = next_renewal(&mut attached).await;
    assert!(
        reattached_at.elapsed() < millis(500),
        "the next session renews at once"
    );
    answer(&leases, &renew, 15_000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(engine.stop_calls().is_empty());
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4 "Agent restart" (ADR 0072): a held lease survives an agent
/// restart on the same host boot. It comes back `uncertain` with what is
/// left of its validity, keeps its VM, renews once a session attaches, and
/// is `held` again on the answer.
#[tokio::test]
async fn a_held_lease_survives_an_agent_restart_on_the_same_boot() {
    let dir = TargetDir::new("expiring-restart");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
    let stored = store.load_leases().await.unwrap()[0].clone();
    assert!(
        stored.validity.is_some(),
        "a held lease stores its validity"
    );
    drop(attached);
    leases.abort();
    drop(store);

    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert!(!leases.view.may_start_stateful(LEASE, 1));
    assert_eq!(
        store.load_leases().await.unwrap()[0].validity,
        stored.validity,
        "the restore keeps the stored deadline as it was"
    );
    let mut attached = leases.link.attach();
    let renew = next_renewal(&mut attached).await;
    assert_eq!(renew.body.fencing_token, 1);
    answer(&leases, &renew, 15_000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4 "Agent restart": after a reboot, or a suspend past the
/// deadline, nothing proves the lease is still valid. It comes back
/// `expired`, is written back without a validity, and is fenced.
#[tokio::test]
async fn a_held_lease_is_fenced_after_a_reboot_or_a_long_suspend() {
    for (name, interrupt) in [
        (
            "expiring-reboot",
            Box::new(|clock: &ManualHostClock| clock.reboot("boot-b", Duration::from_secs(5)))
                as Box<dyn Fn(&ManualHostClock)>,
        ),
        (
            "expiring-suspend",
            Box::new(|clock: &ManualHostClock| clock.suspend(Duration::from_secs(60))),
        ),
    ] {
        let dir = TargetDir::new(name);
        let engine = Arc::new(FakeEngine::new());
        let desired = bound(&["vm_warm"]);
        let clock = clock();
        let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
        let vm = running_vm(&engine, "vm_warm", 2).await;
        let mut attached = leases.link.attach();
        let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
        drop(attached);
        leases.abort();
        drop(store);

        interrupt(&clock);
        let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
        assert_eq!(
            leases.view.state(LEASE),
            Some(LeaseState::Expired),
            "{name}"
        );
        let rows = store.load_leases().await.unwrap();
        assert_eq!(rows[0].state, LeaseState::Expired, "{name}");
        assert_eq!(rows[0].validity, None, "{name}");
        wait_for_fence_record(&store).await;
        assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
        let fenced = events_of(&store, "node.fenced").await;
        assert_eq!(fenced.len(), 1, "{name}");
        assert_eq!(
            fenced[0]["data"]["fencing_tokens"],
            serde_json::json!({ LEASE: 1 }),
            "{name}"
        );
        assert_eq!(
            fenced[0]["data"]["vm_ids"],
            serde_json::json!(["vm_warm"]),
            "{name}"
        );
        leases.abort();
    }
}

/// Section 8.4 "Agent restart": a later save of the same `valid_until`, here
/// the higher-token grant of a new ownership, which keeps the old validity,
/// stores the same deadline even after a suspend, which `CLOCK_BOOTTIME`
/// counts and the monotonic clock does not. The suspend is shorter than what
/// is left, so the lease stays valid.
#[tokio::test]
async fn a_suspend_after_the_conversion_never_moves_the_stored_deadline_later() {
    let dir = TargetDir::new("expiring-suspend-save");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
    let first: StoredLease = store.load_leases().await.unwrap()[0].clone();
    let first_validity: StoredValidity = first.validity.clone().expect("a stored validity");

    clock.suspend(Duration::from_secs(5));
    leases
        .link
        .grant(MsgId("msg_regrant".to_owned()), None, expiring(2, 15_000))
        .await
        .expect("a higher token is a new ownership");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    let after = store.load_leases().await.unwrap()[0].clone();
    assert_eq!(after.token, 2);
    assert_eq!(
        after.validity,
        Some(first_validity),
        "the same valid_until is stored as it was first stored"
    );
    leases.abort();
}

/// Section 8.4 "Suspend" (ADR 0240): a suspend longer than what was left of
/// the validity expires the lease as soon as the host wakes. The fence counts
/// its stop from the deadline the lease really had, long past, so it skips the
/// graceful stop and forces at once. Nothing stateful may start from the
/// moment of the wake-up, and the node is not marked failed.
#[tokio::test]
async fn a_suspend_past_valid_until_fences_at_once_on_wake_up() {
    let dir = TargetDir::new("expiring-suspend-long");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
    tokio::time::sleep(millis(300)).await;
    assert!(leases.view.may_start_stateful(LEASE, 1));

    let woke = Instant::now();
    clock.suspend(Duration::from_secs(60));
    assert!(
        !leases.view.may_start_stateful(LEASE, 1),
        "the start check counts the suspend before the service does"
    );
    wait_for_fence_record(&store).await;
    let fenced_after = woke.elapsed();
    assert!(
        fenced_after < millis(1000),
        "fenced at the wake-up, not at the old valid_until 13.5 s on: {fenced_after:?}"
    );
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(
        stop_counts(&engine.stop_calls()),
        (0, 1),
        "no graceful stop: the deadline is long past"
    );
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0]["data"]["reason"], "lease_expired");
    assert!(!leases.view.is_failed(), "a suspend is not a failure");
    assert!(!store.node_failed().await.unwrap());
    leases.abort();
}

/// ADR 0240: a revoke the service takes before the wake-up's timer fires
/// still counts the suspend. The lease's `valid_until` passed during it, so
/// its fence has no graceful stop left, as at an expiry. The timers give an
/// `expiring` fence 1 000 ms of graceful stop when it starts in time.
#[tokio::test]
async fn a_revoke_right_after_a_wake_up_counts_the_suspend() {
    let dir = TargetDir::new("expiring-suspend-revoke");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let config = LeaseConfig::new(
        "nod_test",
        FenceTimers {
            grace: Duration::from_secs(1),
            force: millis(300),
        },
    )
    .with_host_clock(Arc::new(clock.clone()));
    let (leases, store) = start(&dir, &engine, &desired, config).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;

    clock.suspend_unnoticed(Duration::from_secs(60));
    let result = leases
        .link
        .revoke(
            MsgId("msg_revoke".to_owned()),
            None,
            LeaseRevoke {
                lease_id: LeaseId(LEASE.to_owned()),
                fencing_token: 1,
                reason: RevokeReason::Moved,
            },
        )
        .await
        .expect("the revoke is taken");
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(
        stop_counts(&engine.stop_calls()),
        (0, 1),
        "no graceful stop: the deadline passed during the suspend"
    );
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    // The 250 ms check may still fire first; the revoke then joins that
    // expiry's fence, which is just as late.
    assert!(
        ["lease_revoked", "lease_expired"].contains(&fenced[0]["data"]["reason"].as_str().unwrap()),
        "{}",
        fenced[0]["data"]["reason"]
    );
    assert!(!leases.view.is_failed());
    leases.abort();
}

/// ADR 0240: a lease the restore brought back `uncertain` keeps instants on
/// the lease clock, so a suspend after the restart counts against it like
/// one before. A suspend past what is left fences it at the wake-up.
#[tokio::test]
async fn a_suspend_after_a_restore_fences_the_restored_lease() {
    let dir = TargetDir::new("expiring-restore-suspend");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
    drop(attached);
    leases.abort();
    drop(store);

    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(leases.view.may_keep_running(LEASE, 1));
    let woke = Instant::now();
    clock.suspend(Duration::from_secs(60));
    wait_for_fence_record(&store).await;
    let fenced_after = woke.elapsed();
    assert!(
        fenced_after < millis(1000),
        "fenced at the wake-up, not 13.5 s on: {fenced_after:?}"
    );
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0]["data"]["reason"], "lease_expired");
    assert!(!leases.view.is_failed());
    leases.abort();
}

/// Section 8.4 "Suspend": a suspend shorter than what is left brings the
/// expiry earlier by its length, and the lease renews as soon as the host
/// wakes. With `D` = 6 000 and the renewals unanswered, `valid_until` is
/// `t_send + 5 000`; after a 2 000 ms suspend the lease expires at
/// `t_send + 3 000` on the tokio clock.
#[tokio::test]
async fn a_short_suspend_brings_the_expiry_earlier_by_its_length() {
    let dir = TargetDir::new("expiring-suspend-short");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let before = grant_and_confirm(&leases, &mut attached, 6000).await;

    let woke = Instant::now();
    clock.suspend(millis(2000));
    // The renewal due at `t_send + 2 000` moved to `t_send`: it goes out now.
    let _unanswered = next_renewal(&mut attached).await;
    assert!(
        woke.elapsed() < millis(500),
        "renews at the wake-up: {:?}",
        woke.elapsed()
    );
    wait_for_fence_record(&store).await;
    let done = before.elapsed();
    assert!(
        done >= millis(3000),
        "no earlier than the moved deadline: {done:?}"
    );
    assert!(done < millis(4500), "earlier than t_send + 5 000: {done:?}");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    leases.abort();
}

/// A suspend while no `expiring` lease is held shortens no lease granted
/// after it: the service catches up with the suspend before it records the
/// grant's instants.
#[tokio::test]
async fn a_suspend_before_a_grant_shortens_nothing() {
    let dir = TargetDir::new("expiring-suspend-before");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    clock.suspend(Duration::from_secs(60));
    tokio::time::sleep(millis(100)).await;

    let _before = grant_and_confirm(&leases, &mut attached, 6000).await;
    tokio::time::sleep(millis(1000)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    assert!(store.fences().await.unwrap().is_empty());
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4 "Agent restart" (ADR 0240): the stored deadline is
/// `CLOCK_BOOTTIME(t_send) + D - M`, read when the renewal was sent. A
/// suspend between the renewal and its answer makes it no later, and the
/// answer still makes the lease `held` with what is left.
#[tokio::test]
async fn a_suspend_between_a_renewal_and_its_answer_stores_the_send_time_deadline() {
    let dir = TargetDir::new("expiring-suspend-between");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let mut attached = leases.link.attach();
    leases
        .link
        .grant(MsgId("msg_grant".to_owned()), None, expiring(1, 15_000))
        .await
        .expect("the grant is taken");
    let renew = next_renewal(&mut attached).await;
    // The renewal was read and sent no later than this.
    let sent_by = clock.boottime().expect("a boottime");

    clock.suspend(Duration::from_secs(3));
    answer(&leases, &renew, 15_000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    let stored = store.load_leases().await.unwrap()[0]
        .validity
        .clone()
        .expect("a stored validity");
    assert_eq!(stored.boot_id, "boot-a");
    assert!(
        stored.until_boottime <= sent_by + millis(13_500),
        "from the send time, not later by the suspend: {stored:?}, sent by {sent_by:?}"
    );
    assert!(
        stored.until_boottime + millis(500) >= sent_by + millis(13_500),
        "{stored:?}, sent by {sent_by:?}"
    );
    leases.abort();
}

/// Section 8.4: a renewal left unanswered for `D / 3` makes the lease
/// `uncertain` while the session stays up, and the answer to the next
/// renewal makes it `held` again, with nothing stopped.
#[tokio::test]
async fn a_late_renewal_makes_the_lease_uncertain_until_the_next_answer() {
    let dir = TargetDir::new("expiring-late");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();

    // `D` = 6 000: a renewal every 2 000 ms.
    let _before = grant_and_confirm(&leases, &mut attached, 6000).await;
    let _unanswered = next_renewal(&mut attached).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    // The next renewal goes out once the first one is late.
    let renew = next_renewal(&mut attached).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert!(!leases.view.may_start_stateful(LEASE, 1));
    answer(&leases, &renew, 6000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.3: losing the session never touches a `local` lease.
#[tokio::test]
async fn a_lost_session_leaves_a_local_lease_held() {
    let dir = TargetDir::new("local-detached");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let attached = leases.link.attach();
    leases
        .link
        .grant(
            MsgId("msg_grant".to_owned()),
            None,
            LeaseGrant {
                mode: LeaseMode::Local,
                duration_ms: None,
                ..expiring(1, 3000)
            },
        )
        .await
        .expect("the grant is taken");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));

    drop(attached);
    tokio::time::sleep(millis(200)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    assert_eq!(leases.link.hello()[0].state, "held");
    leases.abort();
}

/// A session that ends after a newer one attached changes nothing: the newer
/// session keeps carrying the renewals and the lease stays `held`.
#[tokio::test]
async fn an_old_session_ending_after_a_new_one_attached_changes_nothing() {
    let dir = TargetDir::new("expiring-reattached");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let mut old = leases.link.attach();
    // `D` = 3 000: a renewal every 1 000 ms.
    let _before = grant_and_confirm(&leases, &mut old, 3000).await;

    let mut new = leases.link.attach();
    let renew = next_renewal(&mut new).await;
    answer(&leases, &renew, 3000).await;
    drop(old);
    tokio::time::sleep(millis(100)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));

    let renew = next_renewal(&mut new).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    answer(&leases, &renew, 3000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    leases.abort();
}

/// Section 8.4: `T_grace + T_force <= M` holds for the whole stop, the check
/// after a forced stop included. A VM whose stops both hang fails its fence,
/// and the fence still ends by `t_send + D`.
#[tokio::test]
async fn an_expiry_whose_stops_hang_fails_within_the_margin() {
    let dir = TargetDir::new("expiring-hang");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_hung"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(5000));
    let mut attached = leases.link.attach();

    // `D` = 3 000, `M` = 1 000: the fence starts at `t_send` + 2 000, the
    // forced stop by `t_send` + 2 700, and the stop gives up at `t_send` +
    // 3 000. Recording a failed fence takes a few writes after that.
    let before = grant_and_confirm(&leases, &mut attached, 3000).await;
    wait_for("the forced stop", || {
        stop_counts(&engine.stop_calls()).1 == 1
    })
    .await;
    let forced = before.elapsed();
    assert!(forced >= millis(2000), "no early fence: {forced:?}");
    assert!(
        forced <= millis(2750),
        "the graceful stop is clamped: {forced:?}"
    );
    assert!(
        leases.view.fence_owns_stop(LEASE),
        "the reconciler leaves the VM to the fence"
    );
    wait_for_fence_record(&store).await;
    let done = before.elapsed();
    assert!(!leases.view.fence_owns_stop(LEASE));
    assert!(
        done <= millis(3250),
        "the stop gave up by t_send + D, then was recorded: {done:?}"
    );
    let failed = events_of(&store, "node.fence_failed").await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["data"]["reason"], "lease_expired");
    assert_eq!(
        failed[0]["data"]["fencing_tokens"],
        serde_json::json!({ LEASE: 1 })
    );
    assert!(leases.view.is_failed());
    leases.abort();
}

/// #434, section 8.5: a forced stop that outlives `T_force` is a failed fence,
/// since nothing confirmed the VM stopped in time, but the engine still
/// finishes it: the VMM does not keep running because the fence gave up.
#[tokio::test(start_paused = true)]
async fn a_forced_stop_that_outlives_t_force_still_stops_the_vm() {
    let engine = FakeEngine::new();
    let vm = running_vm(&engine, "vm_slow_kill", 2).await;
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(500));

    let report = stop_vms(
        &engine,
        std::slice::from_ref(&vm),
        FenceTimers {
            grace: millis(100),
            force: millis(300),
        },
    )
    .await;
    assert!(!report.complete());
    // The engine's own bound ends with the fence's: its `Timeout` reads as
    // the fence's own (#459).
    assert!(
        matches!(
            &report.vms[..],
            [(id, VmFenceOutcome::Failed(reason))]
                if id == &vm && reason == "forced stop timed out"
        ),
        "{report:?}"
    );
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopping);
    assert_eq!(stop_counts(&engine.stop_calls()), (1, 1));

    tokio::time::sleep(millis(500)).await;
    let info = engine.inspect(&vm).await.unwrap();
    assert_eq!(info.state, VmState::Stopped);
    assert_eq!(info.pid, None);
    assert_eq!(info.exit_code, Some(137));
}

/// The app's lease as a `local` grant with `token`.
fn local_grant(token: u64) -> LeaseGrant {
    LeaseGrant {
        lease_id: LeaseId(LEASE.to_owned()),
        resource_type: ResourceType::App,
        resource_id: APP.to_owned(),
        mode: LeaseMode::Local,
        fencing_token: token,
        duration_ms: None,
        granted_at: "2026-11-02T10:15:33Z".to_owned(),
    }
}

/// Grants the lease `local` with token 1, as the primary does for a stateful
/// app, then converts it to `expiring` with the same token (section 8.4
/// "Changing the mode"). Returns the instant just before the conversion.
async fn grant_local_then_convert(leases: &LocalLeases, duration_ms: u64) -> Instant {
    leases
        .link
        .grant(MsgId("msg_local".to_owned()), None, local_grant(1))
        .await
        .expect("the local grant is taken");
    assert!(leases.view.may_start_stateful(LEASE, 1));
    let before = Instant::now();
    leases
        .link
        .grant(
            MsgId("msg_convert".to_owned()),
            None,
            expiring(1, duration_ms),
        )
        .await
        .expect("the conversion is taken");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_keep_running(LEASE, 1), "its VMs run on");
    before
}

async fn wait_for_stored_validity(store: &NodeStore) -> StoredValidity {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(validity) = store.load_leases().await.unwrap()[0].validity.clone() {
            return validity;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the provisional deadline is stored"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Section 8.4 "Changing the mode" (ADR 0241): a converted lease keeps its
/// VMs and renews at once. Its first renewal gives it a provisional deadline,
/// `t_send + D - M`, and with no answer the node fences there. With `D` =
/// 6 000 that is 5 000 ms after the renewal.
#[tokio::test]
async fn a_converted_lease_keeps_its_vms_and_fences_at_its_first_renewal_s_deadline() {
    let dir = TargetDir::new("expiring-convert-unanswered");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let before = grant_local_then_convert(&leases, 6000).await;
    assert_eq!(leases.link.hello()[0].mode, LeaseMode::Expiring);

    let renew = next_renewal(&mut attached).await;
    assert_eq!(renew.body.fencing_token, 1, "the same token");
    assert!(
        before.elapsed() < millis(500),
        "the first renewal goes out at once"
    );
    let stored = wait_for_stored_validity(&store).await;
    assert_eq!(stored.boot_id, "boot-a");

    tokio::time::sleep(millis(4000)).await;
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    assert!(leases.view.may_keep_running(LEASE, 1));
    wait_for_fence_record(&store).await;
    let done = before.elapsed();
    assert!(
        done >= millis(5000),
        "no earlier than t_send + 5 000: {done:?}"
    );
    assert!(done < millis(6500), "at the provisional deadline: {done:?}");
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0]["data"]["reason"], "lease_expired");
    assert_eq!(
        fenced[0]["data"]["fencing_tokens"],
        serde_json::json!({ LEASE: 1 })
    );
    assert!(!leases.view.is_failed());
    leases.abort();
}

/// Section 8.4 "Changing the mode": an answered first renewal holds the
/// converted lease like any `expiring` one. A `local` grant with the same
/// token then ends the renewals and the deadline, and the app runs on.
#[tokio::test]
async fn a_local_grant_ends_a_converted_lease_s_renewals() {
    let dir = TargetDir::new("expiring-convert-back");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_local_then_convert(&leases, 6000).await;
    let first = next_renewal(&mut attached).await;
    answer(&leases, &first, 6000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful(LEASE, 1));
    let second = next_renewal(&mut attached).await;
    answer(&leases, &second, 6000).await;

    leases
        .link
        .grant(MsgId("msg_local_again".to_owned()), None, local_grant(1))
        .await
        .expect("the conversion back is taken");
    let after = Instant::now();
    assert_eq!(leases.link.hello()[0].mode, LeaseMode::Local);
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    let row = store.load_leases().await.unwrap()[0].clone();
    assert_eq!(row.mode, LeaseMode::Local);
    assert_eq!(row.validity, None);
    assert!(
        tokio::time::timeout(millis(3000), attached.renewals.recv())
            .await
            .is_err(),
        "no renewal after the local grant"
    );
    // Past the deadline the last answer gave, `t_send + 5 000`.
    tokio::time::sleep(millis(5500).saturating_sub(after.elapsed())).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    assert!(store.fences().await.unwrap().is_empty());
    assert!(leases.view.may_start_stateful(LEASE, 1));
    leases.abort();
}

/// ADR 0241: until its first renewal is sent, a converted lease behaves as
/// `local`. With no session nothing is sent, so the lease stays `held` and
/// its VMs run past any deadline the duration would give. Once a session
/// attaches, the renewal goes out and the provisional deadline counts from
/// it: `D` = 3 000 gives 2 000 ms.
#[tokio::test]
async fn a_converted_lease_runs_on_without_a_session_until_its_first_renewal() {
    let dir = TargetDir::new("expiring-convert-no-session");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let _before = grant_local_then_convert(&leases, 3000).await;

    tokio::time::sleep(millis(3500)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    assert!(store.fences().await.unwrap().is_empty());
    assert_eq!(store.load_leases().await.unwrap()[0].validity, None);

    let attached_at = Instant::now();
    let mut attached = leases.link.attach();
    let _renew = next_renewal(&mut attached).await;
    assert!(
        attached_at.elapsed() < millis(500),
        "renews as soon as a session attaches"
    );
    wait_for_fence_record(&store).await;
    let done = attached_at.elapsed();
    assert!(
        done >= millis(2000),
        "no earlier than t_send + 2 000: {done:?}"
    );
    assert!(done < millis(3500), "at the provisional deadline: {done:?}");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert!(!leases.view.is_failed());
    leases.abort();
}

/// Section 8.4 "Agent restart" (ADR 0241): the provisional deadline is stored
/// once the first renewal is out, so a restart on the same boot brings the
/// converted lease back `uncertain` with it, keeping its VMs. An answer to
/// the renewal the restored lease sends holds it again.
#[tokio::test]
async fn a_restart_keeps_a_converted_lease_s_provisional_deadline() {
    let dir = TargetDir::new("expiring-convert-restart");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_local_then_convert(&leases, 15_000).await;
    let _unanswered = next_renewal(&mut attached).await;
    let stored = wait_for_stored_validity(&store).await;
    drop(attached);
    leases.abort();
    drop(store);

    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
    assert!(leases.view.may_keep_running(LEASE, 1));
    assert_eq!(
        store.load_leases().await.unwrap()[0].validity,
        Some(stored),
        "the restore keeps the provisional deadline"
    );
    let mut attached = leases.link.attach();
    let renew = next_renewal(&mut attached).await;
    answer(&leases, &renew, 15_000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    leases.abort();
}

/// Section 8.4 "Agent restart": a converted lease stored before its first
/// renewal has no deadline. Nothing proves the authority set no gate for it
/// since, so a restart brings it back `expired` and fences it.
#[tokio::test]
async fn a_restart_before_a_converted_lease_s_first_renewal_fences_it() {
    let dir = TargetDir::new("expiring-convert-restart-early");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let _before = grant_local_then_convert(&leases, 15_000).await;
    let row = store.load_leases().await.unwrap()[0].clone();
    assert_eq!(
        (row.mode, row.state, row.validity),
        (LeaseMode::Expiring, LeaseState::Held, None)
    );
    leases.abort();
    drop(store);

    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    wait_for_fence_record(&store).await;
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    let fenced = events_of(&store, "node.fenced").await;
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0]["data"]["reason"], "lease_expired");
    leases.abort();
}
