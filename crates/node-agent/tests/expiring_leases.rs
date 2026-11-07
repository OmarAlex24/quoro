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
use quoro_engine::{Engine, FakeEngine, StopMode, VmState};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::{
    Attached, FenceTimers, HostClock, LeaseConfig, LeaseState, LocalLeases, ManualHostClock,
    StoredLease, StoredValidity, VmFenceOutcome, start_local_leases, stop_vms, stop_vms_until,
};
use quoro_proto::{
    CommandStatus, Envelope, ErrorCode, LeaseGrant, LeaseId, LeaseMode, LeaseRenew, LeaseRevoke,
    MsgId, ResourceType, RevokeReason,
};
use tokio::sync::watch;
use tokio::time::Instant;

const LEASE: &str = "lse_warm";
const APP: &str = "app_warm";
/// Another `expiring` lease, which sorts before [`LEASE`].
const OTHER: &str = "lse_other";

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
    // Section 6.4 (ADR 0242): a start also needs the start bound, 3 000 ms,
    // and `M` left before `valid_until`, which a lease of 5 000 ms or less
    // never has once its answer arrives.
    if duration_ms >= 15_000 {
        assert!(leases.view.may_start_stateful(LEASE, 1));
    } else if duration_ms <= 5000 {
        assert!(!leases.view.may_start_stateful(LEASE, 1));
    }
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
    // The fence counts the suspend once (#453): the table had already moved
    // the deadline by it, so the fence's clock leaves it out. Counted again,
    // the 2 000 ms suspend would use up the 700 ms graceful stop and leave
    // only the forced one.
    let calls = engine.stop_calls();
    assert!(
        matches!(
            &calls[..],
            [(_, StopMode::Graceful { timeout, .. })]
                if !timeout.is_zero() && *timeout <= millis(700)
        ),
        "one graceful stop, with the time it had left: {calls:?}"
    );
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

    let _before = grant_and_confirm(&leases, &mut attached, 15_000).await;
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
/// and the fence hands the engine stops that fit in `M`, so that it ends by
/// `t_send + D`. That the fence keeps those bounds is shown on a paused clock
/// by `a_hung_stop_keeps_the_bounds_the_fence_hands_the_engine`.
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

    // `D` = 3 000, `M` = 1 000: the fence starts at `t_send` + 2 000, with a
    // graceful stop of at most `M - T_force` = 700 ms and a forced stop of
    // 300 ms. The guest ignores the power button and the VMM takes 5 s to
    // stop it, so neither stop ends before its bound. The test checks the
    // bounds, not how long the stops took, so a loaded host cannot fail it.
    let before = grant_and_confirm(&leases, &mut attached, 3000).await;
    wait_for("the graceful stop", || {
        stop_counts(&engine.stop_calls()).0 == 1
    })
    .await;
    let graceful_at = before.elapsed();
    assert!(
        graceful_at >= millis(2000),
        "no early fence: {graceful_at:?}"
    );
    wait_for("the forced stop", || {
        stop_counts(&engine.stop_calls()).1 == 1
    })
    .await;
    let calls = engine.stop_calls();
    let StopMode::Graceful {
        timeout,
        force_within,
    } = calls[0].1
    else {
        panic!("a graceful stop first: {calls:?}");
    };
    assert!(timeout <= millis(700), "T_grace is clamped: {timeout:?}");
    assert_eq!(force_within, millis(300));
    assert_eq!(
        calls[1].1,
        StopMode::Force {
            within: millis(300)
        }
    );
    assert!(
        leases.view.fence_owns_stop(LEASE),
        "the reconciler leaves the VM to the fence"
    );
    wait_for_fence_record(&store).await;
    assert!(!leases.view.fence_owns_stop(LEASE));
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

/// ADR 0240, #453: the graceful stops of a fence end as soon as the lease
/// clock says their time is over, however much the tokio clock gave them.
/// The forced stops then begin at once.
#[tokio::test(start_paused = true)]
async fn a_graceful_stop_ends_when_the_lease_clock_says_its_time_is_over() {
    let engine = FakeEngine::new();
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);

    let began = Instant::now();
    let report = stop_vms_until(
        &engine,
        std::slice::from_ref(&vm),
        FenceTimers {
            grace: millis(1000),
            force: millis(300),
        },
        Box::pin(tokio::time::sleep(millis(100))),
    )
    .await;
    let took = began.elapsed();
    assert!(
        took >= millis(100) && took < millis(400),
        "the forced stop began at 100 ms: {took:?}"
    );
    assert!(
        matches!(&report.vms[..], [(id, VmFenceOutcome::Forced)] if id == &vm),
        "{report:?}"
    );
    let calls = engine.stop_calls();
    assert_eq!(stop_counts(&calls), (1, 1));
    assert_eq!(
        calls[1].1,
        StopMode::Force {
            within: millis(300)
        },
        "the forced stop gets its full T_force"
    );
}

/// Section 8.4: the fence keeps the bounds it hands the engine. Against a
/// guest that ignores the power button and a VMM that takes 5 s to stop, the
/// forced stop begins when `T_grace` has passed, and the fence reports the VM
/// failed when `T_force` has passed after that, so the whole stop takes
/// `T_grace + T_force`.
#[tokio::test(start_paused = true)]
async fn a_hung_stop_keeps_the_bounds_the_fence_hands_the_engine() {
    let engine = FakeEngine::new();
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(5000));

    let began = Instant::now();
    let fence = async {
        let report = stop_vms(
            &engine,
            std::slice::from_ref(&vm),
            FenceTimers {
                grace: millis(700),
                force: millis(300),
            },
        )
        .await;
        (report, began.elapsed())
    };
    let watch = async {
        tokio::time::sleep(millis(699)).await;
        let before_grace = stop_counts(&engine.stop_calls());
        tokio::time::sleep(millis(2)).await;
        (before_grace, stop_counts(&engine.stop_calls()))
    };
    let ((report, took), (before_grace, after_grace)) = tokio::join!(fence, watch);
    assert_eq!(before_grace, (1, 0), "graceful until T_grace");
    assert_eq!(after_grace, (1, 1), "forced once T_grace has passed");
    assert!(took <= millis(1000), "T_grace + T_force: {took:?}");
    assert!(
        matches!(&report.vms[..], [(id, VmFenceOutcome::Failed(_))] if id == &vm),
        "{report:?}"
    );
    assert_eq!(
        engine.stop_calls()[0].1,
        StopMode::Graceful {
            timeout: millis(700),
            force_within: millis(300)
        }
    );
}

/// Section 8.4, ADR 0240, #536: the same hung stop, on the path of an
/// `expiring` fence with graceful time left. A tokio sleep stands in for the
/// lease clock and ends the graceful time before `T_grace`, as the lease
/// clock does after a host suspend. The forced stop then begins at once with
/// its full `T_force`, and the fence reports the VM failed when `T_force` has
/// passed after that.
#[tokio::test(start_paused = true)]
async fn a_hung_stop_the_lease_clock_cuts_short_still_gets_its_full_t_force() {
    let engine = FakeEngine::new();
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(5000));

    let began = Instant::now();
    let fence = async {
        let report = stop_vms_until(
            &engine,
            std::slice::from_ref(&vm),
            FenceTimers {
                grace: millis(700),
                force: millis(300),
            },
            Box::pin(tokio::time::sleep(millis(400))),
        )
        .await;
        (report, began.elapsed())
    };
    let watch = async {
        tokio::time::sleep(millis(399)).await;
        let before_over = stop_counts(&engine.stop_calls());
        tokio::time::sleep(millis(2)).await;
        (before_over, stop_counts(&engine.stop_calls()))
    };
    let ((report, took), (before_over, after_over)) = tokio::join!(fence, watch);
    assert_eq!(before_over, (1, 0), "graceful until the lease clock says");
    assert_eq!(after_over, (1, 1), "forced at once, not at T_grace");
    assert!(
        took >= millis(700) && took < millis(750),
        "the forced stop got its full T_force from 400 ms: {took:?}"
    );
    assert!(
        matches!(&report.vms[..], [(id, VmFenceOutcome::Failed(_))] if id == &vm),
        "{report:?}"
    );
    let calls = engine.stop_calls();
    assert_eq!(
        calls[0].1,
        StopMode::Graceful {
            timeout: millis(700),
            force_within: millis(300)
        }
    );
    assert_eq!(
        calls[1].1,
        StopMode::Force {
            within: millis(300)
        },
        "the forced stop gets its full T_force"
    );
}

/// Section 8.4, ADR 0240, #453: a host suspend during an `expiring` lease's
/// graceful stop counts against it. A suspend longer than what was left ends
/// it at the wake-up, and the forced stop begins then, instead of when the
/// tokio clock, which stopped during the suspend, reaches `T_grace`.
#[tokio::test]
async fn a_suspend_during_a_fence_s_graceful_stop_ends_it_at_the_wake_up() {
    let dir = TargetDir::new("expiring-fence-suspend");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_hung"]);
    let clock = clock();
    // `D` = 60 000, `M` = 6 000, which holds 5 000 ms of graceful stop and
    // 300 ms forced.
    let config = config(&clock).with_expiring_timers(FenceTimers {
        grace: millis(5000),
        force: millis(300),
    });
    let (leases, _store) = start(&dir, &engine, &desired, config).await;
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 60_000).await;

    let revoke = leases.link.revoke(
        MsgId("msg_revoke".to_owned()),
        None,
        LeaseRevoke {
            lease_id: LeaseId(LEASE.to_owned()),
            fencing_token: 1,
            reason: RevokeReason::Moved,
        },
    );
    let suspend = async {
        wait_for("the graceful stop", || {
            stop_counts(&engine.stop_calls()).0 == 1
        })
        .await;
        let woke = Instant::now();
        clock.suspend(Duration::from_secs(60));
        wait_for("the forced stop", || {
            stop_counts(&engine.stop_calls()).1 == 1
        })
        .await;
        woke.elapsed()
    };
    let (result, forced_after) = tokio::join!(revoke, suspend);
    assert_eq!(
        result.expect("the revoke is taken").status,
        CommandStatus::Ok
    );
    let calls = engine.stop_calls();
    let StopMode::Graceful { timeout, .. } = calls[0].1 else {
        panic!("a graceful stop first: {calls:?}");
    };
    assert!(
        timeout > millis(4500),
        "the graceful stop began in time: {timeout:?}"
    );
    assert!(
        forced_after < timeout / 2,
        "the forced stop began at the wake-up, not {timeout:?} into the graceful stop: {forced_after:?}"
    );
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(stop_counts(&calls), (1, 1));
    leases.abort();
}

/// ADR 0240, #453: a suspend shorter than what is left of a fence's graceful
/// stop shortens it by its length. At the wake-up the fence's clock waits
/// again for what is left, and the forced stop begins then.
#[tokio::test]
async fn a_short_suspend_during_a_fence_s_graceful_stop_shortens_it_by_its_length() {
    let dir = TargetDir::new("expiring-fence-suspend-short");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_hung"]);
    let clock = clock();
    // `D` = 60 000, `M` = 6 000, which holds 5 000 ms of graceful stop and
    // 300 ms forced.
    let config = config(&clock).with_expiring_timers(FenceTimers {
        grace: millis(5000),
        force: millis(300),
    });
    let (leases, _store) = start(&dir, &engine, &desired, config).await;
    let vm = running_vm(&engine, "vm_hung", 2).await;
    engine.set_ignore_power_button(&vm, true);
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 60_000).await;

    let revoke = leases.link.revoke(
        MsgId("msg_revoke".to_owned()),
        None,
        LeaseRevoke {
            lease_id: LeaseId(LEASE.to_owned()),
            fencing_token: 1,
            reason: RevokeReason::Moved,
        },
    );
    let suspend = async {
        wait_for("the graceful stop", || {
            stop_counts(&engine.stop_calls()).0 == 1
        })
        .await;
        let woke = Instant::now();
        clock.suspend(millis(1000));
        wait_for("the forced stop", || {
            stop_counts(&engine.stop_calls()).1 == 1
        })
        .await;
        woke.elapsed()
    };
    let (result, forced_after) = tokio::join!(revoke, suspend);
    assert_eq!(
        result.expect("the revoke is taken").status,
        CommandStatus::Ok
    );
    let calls = engine.stop_calls();
    let StopMode::Graceful { timeout, .. } = calls[0].1 else {
        panic!("a graceful stop first: {calls:?}");
    };
    assert!(
        timeout > millis(4500),
        "the graceful stop began in time: {timeout:?}"
    );
    // The wake-up comes after the graceful stop began, so the forced stop
    // begins at most `timeout - 1 000` after it, and the test sees it a few
    // milliseconds later. Without the suspend counted, it would begin about
    // `timeout` after the wake-up.
    assert!(
        forced_after > millis(2500),
        "the fence waited again at the wake-up: {forced_after:?}"
    );
    assert!(
        forced_after + millis(500) < timeout,
        "the suspend shortened the graceful stop of {timeout:?}: {forced_after:?}"
    );
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(stop_counts(&calls), (1, 1));
    leases.abort();
}

/// Section 8.4, ADR 0240, #453: a fence of an `expiring` lease waits for a
/// start that holds the lease's gate, and a host suspend while it waits comes
/// out of its graceful stop like the wait itself. A suspend longer than the
/// whole graceful stop leaves only the forced one.
#[tokio::test]
async fn a_suspend_while_a_fence_waits_for_a_start_uses_up_its_graceful_stop() {
    let dir = TargetDir::new("expiring-fence-suspend-start");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    let _before = grant_and_confirm(&leases, &mut attached, 60_000).await;
    let permit = leases
        .view
        .start_permit(LEASE, 1)
        .await
        .expect("the lease starts its VMs");

    let revoke = leases.link.revoke(
        MsgId("msg_revoke".to_owned()),
        None,
        LeaseRevoke {
            lease_id: LeaseId(LEASE.to_owned()),
            fencing_token: 1,
            reason: RevokeReason::Moved,
        },
    );
    let suspend = async {
        wait_for("the fence", || leases.view.fence_owns_stop(LEASE)).await;
        clock.suspend(Duration::from_secs(60));
        tokio::time::sleep(millis(50)).await;
        assert!(
            engine.stop_calls().is_empty(),
            "the fence waits for the start"
        );
        drop(permit);
    };
    let (result, ()) = tokio::join!(revoke, suspend);
    assert_eq!(
        result.expect("the revoke is taken").status,
        CommandStatus::Ok
    );
    assert_eq!(
        stop_counts(&engine.stop_calls()),
        (0, 1),
        "the suspend used up the graceful stop"
    );
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    leases.abort();
}

/// Section 8.4, ADR 0240, #536: a fence takes its trigger instant and the
/// suspend the table had applied under one lock. Here a host suspend lands
/// after that lock and before the fence begins: the renewal of another lease,
/// due in the same look as the expiry, applies it to the table in between.
/// The fence counts that suspend once. Missed, it would leave the graceful
/// stop about 5 600 ms, above the test's bound; counted twice, it would
/// leave none.
#[tokio::test]
async fn a_suspend_applied_between_an_expiry_and_its_fence_counts_once() {
    let dir = TargetDir::new("expiring-trigger-suspend");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    // `D` = 60 000, `M` = 6 000, which holds 5 700 ms of graceful stop and
    // 300 ms forced.
    let config = config(&clock).with_expiring_timers(FenceTimers {
        grace: millis(10_000),
        force: millis(300),
    });
    let (leases, store) = start(&dir, &engine, &desired, config).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();

    // The other lease, of 120 000 ms, renews 40 000 ms after its send time
    // and is valid until 108 000 ms after it. A look returns its actions in
    // lease id order (`polls_are_sorted_by_lease_id`) and the service runs
    // them in that order, so a look that finds the other lease's renewal due
    // together with the expiry renews it first.
    let other = LeaseGrant {
        lease_id: LeaseId(OTHER.to_owned()),
        resource_id: "app_other".to_owned(),
        ..expiring(1, 120_000)
    };
    leases
        .link
        .grant(MsgId("msg_grant_other".to_owned()), None, other.clone())
        .await
        .expect("the grant is taken");
    let renew = next_renewal(&mut attached).await;
    assert_eq!(renew.body.lease_id.0, OTHER);
    leases
        .link
        .grant(
            MsgId("msg_answer_other".to_owned()),
            Some(renew.id.clone()),
            other,
        )
        .await
        .expect("the answer is taken");
    assert_eq!(leases.view.state(OTHER), Some(LeaseState::Held));
    let _before = grant_and_confirm(&leases, &mut attached, 60_000).await;
    let confirmed = Instant::now();

    // The app's lease is valid until at most 54 000 ms after `confirmed`, so
    // a suspend that ends 100 ms past that leaves the next look with the
    // expiry and the other lease's renewal due. A second suspend, of 3 000 ms,
    // begins at the first read of the clock after that look triggered the
    // fence: the renewal's, before the fence begins.
    let view = leases.view.clone();
    clock.suspend_when(millis(3000), move || view.fence_owns_stop(LEASE));
    clock.suspend((confirmed + millis(54_100)).saturating_duration_since(Instant::now()));
    wait_for_fence_record(&store).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Expired));
    let renewed = next_renewal(&mut attached).await;
    assert_eq!(renewed.body.lease_id.0, OTHER, "the other lease renewed");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    // The fence began at least 100 ms past `valid_until`, and 3 000 ms of
    // suspend came after the trigger: at most 2 600 ms of graceful stop
    // remain.
    let calls = engine.stop_calls();
    assert!(
        matches!(
            &calls[..],
            [(_, StopMode::Graceful { timeout, .. })]
                if !timeout.is_zero() && *timeout < millis(2700)
        ),
        "one graceful stop, short by the suspend once: {calls:?}"
    );
    assert!(!leases.view.is_failed(), "a suspend is not a failure");
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
    assert_eq!(leases.link.hello()[0].state, "held");

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

/// Section 5.12, ADR 0258: while the node holds a lease, a grant with a
/// higher token and the other mode is refused with `bad_message`, non-fatal,
/// and changes nothing: the lease keeps its token and mode, its row is not
/// rewritten, no renewal goes out and its VM runs on. Either way round.
#[tokio::test]
async fn a_higher_token_with_the_other_mode_is_refused_while_the_lease_is_held() {
    let dir = TargetDir::new("expiring-mode-refusal");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
    let vm = running_vm(&engine, "vm_warm", 2).await;
    let mut attached = leases.link.attach();
    leases
        .link
        .grant(MsgId("msg_local".to_owned()), None, local_grant(1))
        .await
        .expect("the local grant is taken");
    let row = store.load_leases().await.unwrap();

    let refused = leases
        .link
        .grant(MsgId("msg_other".to_owned()), None, expiring(2, 15_000))
        .await
        .expect_err("a higher token with the other mode is refused");
    assert_eq!(refused.code, ErrorCode::BadMessage);
    assert!(!refused.fatal);
    let hello = leases.link.hello();
    assert_eq!(
        (
            hello[0].mode,
            hello[0].fencing_token,
            hello[0].state.as_str()
        ),
        (LeaseMode::Local, 1, "held")
    );
    assert_eq!(store.load_leases().await.unwrap(), row);
    assert!(
        tokio::time::timeout(millis(1000), attached.renewals.recv())
            .await
            .is_err(),
        "no renewal for a refused grant"
    );
    assert!(leases.view.may_start_stateful(LEASE, 1));
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);

    // An `expiring` lease the node holds keeps its deadline the same way.
    leases
        .link
        .grant(MsgId("msg_convert".to_owned()), None, expiring(1, 15_000))
        .await
        .expect("the conversion is taken");
    let renew = next_renewal(&mut attached).await;
    answer(&leases, &renew, 15_000).await;
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    let row = store.load_leases().await.unwrap();
    let refused = leases
        .link
        .grant(MsgId("msg_local_higher".to_owned()), None, local_grant(2))
        .await
        .expect_err("a higher local token over a held expiring lease is refused");
    assert_eq!(refused.code, ErrorCode::BadMessage);
    assert_eq!(leases.link.hello()[0].mode, LeaseMode::Expiring);
    assert_eq!(leases.link.hello()[0].fencing_token, 1);
    assert_eq!(store.load_leases().await.unwrap(), row);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
    assert!(engine.stop_calls().is_empty(), "nothing is fenced");
    leases.abort();
}

/// ADR 0241 and ADR 0242: a start that took its permit before the
/// conversion has a `local` lease's bound, and the fence of the converted
/// lease would wait for it. So the first renewal, which gives the lease its
/// deadline, waits until that start is done, and the lease stays `local` in
/// all but name until then. The renewal interval after the start is done
/// sends it: with `D` = 6 000, 2 000 ms after the conversion.
#[tokio::test]
async fn a_converted_lease_s_first_renewal_waits_for_a_start_permitted_before_it() {
    let dir = TargetDir::new("expiring-convert-start");
    let engine = Arc::new(FakeEngine::new());
    let desired = bound(&["vm_warm"]);
    let clock = clock();
    let (leases, _store) = start(&dir, &engine, &desired, config(&clock)).await;
    let mut attached = leases.link.attach();
    leases
        .link
        .grant(MsgId("msg_local".to_owned()), None, local_grant(1))
        .await
        .expect("the local grant is taken");
    let permit = leases
        .view
        .start_permit(LEASE, 1)
        .await
        .expect("a local lease starts");
    leases
        .link
        .grant(MsgId("msg_convert".to_owned()), None, expiring(1, 6000))
        .await
        .expect("the conversion is taken");
    let converted = Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_millis(1500), attached.renewals.recv())
            .await
            .is_err(),
        "no renewal while the start runs"
    );
    assert!(leases.view.start_permit(LEASE, 1).await.is_none());
    assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
    assert!(leases.view.may_keep_running(LEASE, 1));

    drop(permit);
    let _renew = next_renewal(&mut attached).await;
    let sent = converted.elapsed();
    assert!(
        sent < Duration::from_millis(3000),
        "the next renewal interval sends it: {sent:?}"
    );
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
/// converted lease back `uncertain` with it, keeping its VMs until that
/// deadline: with `D` = 6 000, 5 000 ms after the first renewal. A restored
/// lease whose renewal is answered is held again.
#[tokio::test]
async fn a_restart_keeps_a_converted_lease_s_provisional_deadline() {
    for answered in [false, true] {
        let dir = TargetDir::new(if answered {
            "expiring-convert-restart-answered"
        } else {
            "expiring-convert-restart"
        });
        let engine = Arc::new(FakeEngine::new());
        let desired = bound(&["vm_warm"]);
        let clock = clock();
        let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
        let vm = running_vm(&engine, "vm_warm", 2).await;
        let mut attached = leases.link.attach();
        let before = grant_local_then_convert(&leases, 6000).await;
        let _unanswered = next_renewal(&mut attached).await;
        let stored = wait_for_stored_validity(&store).await;
        drop(attached);
        leases.abort();
        drop(store);

        let (leases, store) = start(&dir, &engine, &desired, config(&clock)).await;
        assert_eq!(leases.view.state(LEASE), Some(LeaseState::Uncertain));
        assert_eq!(leases.link.hello()[0].state, "uncertain");
        assert_eq!(leases.link.hello()[0].mode, LeaseMode::Expiring);
        assert!(leases.view.may_keep_running(LEASE, 1));
        assert_eq!(
            store.load_leases().await.unwrap()[0].validity,
            Some(stored),
            "the restore keeps the provisional deadline"
        );
        let mut attached = leases.link.attach();
        let renew = next_renewal(&mut attached).await;
        if answered {
            answer(&leases, &renew, 6000).await;
            assert_eq!(leases.view.state(LEASE), Some(LeaseState::Held));
            assert!(engine.stop_calls().is_empty(), "nothing is fenced");
            assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);
        } else {
            wait_for_fence_record(&store).await;
            let done = before.elapsed();
            assert!(
                done >= millis(5000),
                "no earlier than t_send + 5 000: {done:?}"
            );
            assert!(done < millis(6500), "at the stored deadline: {done:?}");
            assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
        }
        leases.abort();
    }
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
