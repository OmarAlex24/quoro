//! The M0 lease tests of `docs/contracts/node-agent-protocol.md` section 8.7:
//! a revoked `local` lease and an expired short `expiring` lease, both against
//! the in-process primary and the `FakeEngine`.
//!
//! The agent and the primary script run in the same task: the engine's
//! `async fn` futures are not `Send`, so the agent cannot be spawned.

mod common;

use common::{
    APP, NODE, command_result_of, config, error_of, event_of, lease_state, millis, pair, renew_of,
    run_phase, running_vm, settle, stop_counts,
};
use quoro_engine::{Engine, FakeEngine, StopMode, VmId, VmState};
use quoro_node_agent::{
    FenceTimers, LeaseAgent, LeaseState, LeaseStore, MemoryLeaseStore, budget_for, margin,
};
use quoro_proto::{
    CommandStatus, ErrorCode, FenceReason, FromNode, LeaseGrant, LeaseId, LeaseMode, NodeEventData,
    ResourceType, RevokeReason,
};
use tokio::time::Instant;

const LOCAL_LEASE: &str = "lse_local";
const EXPIRING_LEASE: &str = "lse_expiring";

fn lease(id: &str) -> LeaseId {
    LeaseId(id.to_string())
}

/// Section 8.7 test 1.
#[tokio::test(start_paused = true)]
async fn revoking_a_local_lease_fences_its_vm() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_local", 2).await;
    let lease_id = lease(LOCAL_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let mut observed = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 1);
        let revoke = primary.revoke(LOCAL_LEASE, 1, RevokeReason::Moved);
        let event = primary.recv().await;
        let result = primary.recv().await;
        observed = Some((revoke, event, result));
    })
    .await;
    let (revoke, event, result) = observed.expect("the phase observed the fence");

    let event = event_of(event);
    assert!(event.id.starts_with("evt_"));
    let NodeEventData::NodeFenced(fenced) = event.data;
    assert_eq!(fenced.node_id, NODE);
    assert_eq!(fenced.app_ids, vec![APP.to_string()]);
    assert_eq!(fenced.lease_ids, vec![lease_id.clone()]);
    assert_eq!(fenced.reason, FenceReason::LeaseRevoked);

    let (answered, result) = command_result_of(result);
    assert_eq!(answered, revoke, "the result answers the revoke");
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(result.detail, None);
    assert_eq!(result.error, None);

    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Stopped
    );
    let stop_calls = engine.stop_calls();
    assert_eq!(stop_calls.len(), 1);
    assert!(matches!(stop_calls[0].1, StopMode::Graceful { .. }));

    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Revoked);
    assert!(!agent.may_start_stateful(&lease_id, 1));

    let fences = store.fences();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].lease_id, lease_id);
    assert_eq!(fences[0].token, 1);
    assert_eq!(fences[0].reason, FenceReason::LeaseRevoked);
    assert!(fences[0].complete);
    assert_eq!(fences[0].vm_ids, vec![vm.clone()]);

    // A bare re-grant with the old token is a bug in the authority.
    let mut refused = None;
    let mut accepted = None;
    run_phase(&mut agent, &mut inbound, async {
        let stale = primary.grant_local(LOCAL_LEASE, APP, 1);
        let error = primary.recv().await;
        refused = Some((stale, error));
        accepted = Some(primary.grant_local(LOCAL_LEASE, APP, 2));
        settle().await;
    })
    .await;

    let (stale, error) = refused.expect("the phase observed the refusal");
    let (answered, error) = error_of(error);
    assert_eq!(answered, stale);
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(!error.fatal);
    assert!(accepted.is_some());

    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Held);
    assert!(agent.may_start_stateful(&lease_id, 2));
    assert!(!agent.may_start_stateful(&lease_id, 1));
}

/// What one run of the short-expiry scenario observed.
struct ExpiryRun {
    t0: Instant,
    renew_at: Instant,
    fenced_at: Instant,
    stop_calls: Vec<(VmId, StopMode)>,
    vm_state: VmState,
    lease_state: LeaseState,
    budget: FenceTimers,
    fenced: NodeEventData,
}

/// Grants an expiring lease, never answers its renewals, and waits for the
/// fence.
async fn run_expiring_fence(duration_ms: u64) -> ExpiryRun {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_short", 2).await;
    // The guest ignores the power button and the VMM is slow, so the graceful
    // stop cannot finish inside its bound and the forced stop runs.
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(100));
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let t0 = Instant::now();
    let mut renew_at = None;
    let mut fenced_at = None;
    let mut fenced = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, duration_ms, None);
        let (_id, at) = primary.recv_renew().await;
        renew_at = Some(at);
        let message = primary.recv().await;
        fenced_at = Some(Instant::now());
        fenced = Some(event_of(message));
    })
    .await;

    let fenced = fenced.expect("a node.fenced event");
    let NodeEventData::NodeFenced(fenced) = fenced.data;
    let budget = budget_for(
        agent.table().get(&lease_id).expect("the lease"),
        config().timers,
    );
    ExpiryRun {
        t0,
        renew_at: renew_at.expect("a lease_renew"),
        fenced_at: fenced_at.expect("a fence time"),
        stop_calls: engine.stop_calls(),
        vm_state: engine.inspect(&vm).await.expect("inspect").state,
        lease_state: lease_state(&agent, &lease_id),
        budget,
        fenced: NodeEventData::NodeFenced(fenced),
    }
}

/// Section 8.7 test 2: the short lease the milestone table calls out, plus the
/// worked example of section 8.4.
#[tokio::test(start_paused = true)]
async fn a_short_expiring_lease_expires_and_fences_within_the_bound() {
    // Variant 1: D = 3000, so M = 1000 and valid_until = t0 + 2000.
    let short = run_expiring_fence(3000).await;
    assert_eq!(short.renew_at - short.t0, millis(1000));
    let NodeEventData::NodeFenced(fenced) = &short.fenced;
    assert_eq!(fenced.reason, FenceReason::LeaseExpired);
    assert_eq!(fenced.app_ids, vec![APP.to_string()]);
    assert_eq!(fenced.lease_ids, vec![lease(EXPIRING_LEASE)]);
    // valid_until (2000) + T_grace (700) + the slow VMM's forced stop (100).
    assert_eq!(
        short.fenced_at - short.t0,
        millis(2800),
        "the fence starts at valid_until and fits in M"
    );
    assert!(short.fenced_at <= short.t0 + millis(3000));
    assert_eq!(short.vm_state, VmState::Stopped);
    assert_eq!(short.lease_state, LeaseState::Expired);
    assert_eq!(stop_counts(&short.stop_calls), (1, 1));
    assert!(short.budget.grace + short.budget.force <= margin(millis(3000)));

    // Variant 2: D = 15000, the worked example of section 8.4. Here the
    // uncertain window is real: a renewal is late at +10000 and expiry is at
    // +13500. (With D = 3000 the two coincide at +2000, which is why the
    // "still running while uncertain" assertions live in this variant.)
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_long", 3).await;
    engine.set_ignore_power_button(&vm, true);
    engine.set_stop_delay(&vm, millis(100));
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let t0 = Instant::now();
    let mut first_renew = None;
    let mut second_renew = None;
    let mut running_at = None;
    let mut stops_at = None;
    let mut fenced_at = None;
    let mut fenced = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, 15000, None);
        let (_id, at) = primary.recv_renew().await;
        first_renew = Some(at);

        tokio::time::sleep_until(t0 + millis(10000)).await;
        running_at = Some(engine.inspect(&vm).await.expect("inspect").state);
        stops_at = Some(engine.stop_calls().len());

        let (_id, at) = primary.recv_renew().await;
        second_renew = Some(at);

        let message = primary.recv().await;
        fenced_at = Some(Instant::now());
        fenced = Some(event_of(message));
    })
    .await;

    assert_eq!(first_renew.expect("a renewal") - t0, millis(5000));
    assert_eq!(second_renew.expect("a late renewal") - t0, millis(10000));
    assert_eq!(
        running_at,
        Some(VmState::Running),
        "uncertain keeps running"
    );
    assert_eq!(stops_at, Some(0), "uncertain stops nothing");

    let fenced_at = fenced_at.expect("a fence time");
    let NodeEventData::NodeFenced(fenced) = fenced.expect("a node.fenced event").data;
    assert_eq!(fenced.reason, FenceReason::LeaseExpired);
    assert_eq!(fenced.lease_ids, vec![lease_id.clone()]);
    // valid_until (13 500) + T_grace (700) + the slow VMM's forced stop (100).
    assert_eq!(
        fenced_at - t0,
        millis(14300),
        "the fence starts at valid_until and finishes by t_send + D"
    );
    assert!(fenced_at <= t0 + millis(15000));

    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Stopped
    );
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Expired);
    assert!(!agent.may_start_stateful(&lease_id, 1));
    let budget = budget_for(
        agent.table().get(&lease_id).expect("the lease"),
        config().timers,
    );
    assert!(budget.grace + budget.force <= margin(millis(15000)));
    assert_eq!(stop_counts(&engine.stop_calls()), (1, 1));
}

/// Section 8.4: `uncertain` keeps running and starts nothing new; a renewal
/// answered before `valid_until` returns the lease to `held`.
#[tokio::test(start_paused = true)]
async fn uncertain_never_stops_and_returns_to_held() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_uncertain", 2).await;
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let t0 = Instant::now();
    let mut first_renew = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, 15000, None);
        let (_id, at) = primary.recv_renew().await;
        first_renew = Some(at);
        // The renewal is late at +10000: a whole renewal interval passed.
        tokio::time::sleep_until(t0 + millis(10000)).await;
    })
    .await;

    assert_eq!(first_renew.expect("a renewal") - t0, millis(5000));
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Uncertain);
    assert!(!agent.may_start_stateful(&lease_id, 1));
    assert_eq!(engine.stop_calls().len(), 0);
    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Running
    );
    assert!(store.fences().is_empty());

    // The renewal sent at +10000 is still queued; answering it returns the
    // lease to held before valid_until.
    let mut answered = None;
    run_phase(&mut agent, &mut inbound, async {
        answered = Some(primary.answer_renewal(EXPIRING_LEASE, APP, 15000, 1).await);
        settle().await;
    })
    .await;

    let (_renew_id, answered_at) = answered.expect("an answered renewal");
    assert_eq!(answered_at - t0, millis(10000));
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Held);
    assert!(agent.may_start_stateful(&lease_id, 2));
    assert_eq!(engine.stop_calls().len(), 0);
    assert!(store.fences().is_empty());
    assert!(
        primary.try_recv().is_none(),
        "no node.fenced event while the lease is held"
    );
}

/// Section 8.4: renewals answered in time keep the lease held, and
/// `valid_until` is recomputed from each renewal's send time.
#[tokio::test(start_paused = true)]
async fn renewals_answered_in_time_keep_the_lease_held() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_steady", 2).await;
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let t0 = Instant::now();
    let mut last = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, 15000, None);
        last = primary
            .serve_renewals(EXPIRING_LEASE, APP, 15000, 1, t0 + millis(60000))
            .await;
    })
    .await;

    let (_id, last_send) = last.expect("at least one answered renewal");
    let lease = agent.table().get(&lease_id).expect("the lease");
    assert_eq!(lease.state, LeaseState::Held);
    assert_eq!(
        lease.token, 12,
        "one higher token per answered renewal (11 renewals in 60 s)"
    );
    assert_eq!(
        lease.valid_until,
        Some(last_send + millis(13500)),
        "valid_until is t_send + D - M"
    );
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Held);
    assert_eq!(engine.stop_calls().len(), 0);
    assert!(store.fences().is_empty());
    if let Some(trailing) = primary.try_recv() {
        assert!(
            matches!(trailing, FromNode::LeaseRenew(_)),
            "no event while renewals are answered, got {trailing:?}"
        );
    }
    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Running
    );
    assert!(agent.may_start_stateful(&lease_id, 12));
}

/// Section 5.12 and 8.4: a `duration_ms` below 3 000 is refused, non-fatally.
#[tokio::test(start_paused = true)]
async fn a_grant_below_the_duration_floor_is_refused() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);

    let mut refused = None;
    run_phase(&mut agent, &mut inbound, async {
        let id = primary.grant_expiring(EXPIRING_LEASE, APP, 1, 2999, None);
        refused = Some((id, primary.recv().await));
        settle().await;
    })
    .await;

    let (grant, error) = refused.expect("the phase observed the refusal");
    let (answered, error) = error_of(error);
    assert_eq!(answered, grant);
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(!error.fatal);
    assert_eq!(agent.table().leases().count(), 0, "nothing changed");
}

/// Section 5.12: `local` carries no duration.
#[tokio::test(start_paused = true)]
async fn a_local_grant_with_a_duration_is_refused() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);

    let mut refused = None;
    run_phase(&mut agent, &mut inbound, async {
        let id = primary.grant_body(LeaseGrant {
            lease_id: lease(LOCAL_LEASE),
            resource_type: ResourceType::App,
            resource_id: APP.to_string(),
            mode: LeaseMode::Local,
            fencing_token: 1,
            duration_ms: Some(3000),
            granted_at: "2026-11-02T10:15:33Z".to_string(),
        });
        refused = Some((id, primary.recv().await));
        settle().await;
    })
    .await;

    let (grant, error) = refused.expect("the phase observed the refusal");
    let (answered, error) = error_of(error);
    assert_eq!(answered, grant);
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(!error.fatal);
    assert_eq!(agent.table().leases().count(), 0, "nothing changed");
}

/// Section 5.12: a token not higher than the one already seen is refused.
#[tokio::test(start_paused = true)]
async fn a_stale_token_grant_is_refused_and_changes_nothing() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let lease_id = lease(LOCAL_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);

    let mut refused = Vec::new();
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 5);
        settle().await;
        for token in [5, 4] {
            let id = primary.grant_local(LOCAL_LEASE, APP, token);
            refused.push((id, primary.recv().await));
        }
        settle().await;
    })
    .await;

    for (grant, error) in refused {
        let (answered, error) = error_of(error);
        assert_eq!(answered, grant);
        assert_eq!(error.code, ErrorCode::BadMessage);
        assert!(!error.fatal);
    }
    let tracked = agent.table().get(&lease_id).expect("the lease");
    assert_eq!(tracked.state, LeaseState::Held);
    assert_eq!(tracked.token, 5);
    assert_eq!(agent.table().highest_for(ResourceType::App, APP), 5);
    assert!(agent.may_start_stateful(&lease_id, 5));
}

/// Section 5.9 and 12 `cmd_results`: a redelivered revoke gets the same answer
/// and fences nothing again.
#[tokio::test(start_paused = true)]
async fn a_redelivered_revoke_is_answered_without_fencing_again() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_revoke", 2).await;
    let lease_id = lease(LOCAL_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let mut answers = Vec::new();
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 1);
        let id = primary.revoke(LOCAL_LEASE, 1, RevokeReason::Moved);
        let _event = event_of(primary.recv().await);
        answers.push((id.clone(), command_result_of(primary.recv().await)));
        primary.resend_revoke(&id, LOCAL_LEASE, 1, RevokeReason::Moved);
        answers.push((id.clone(), command_result_of(primary.recv().await)));
        settle().await;
    })
    .await;

    assert_eq!(answers.len(), 2);
    assert_eq!(
        answers[0].0, answers[1].0,
        "both answers answer the same id"
    );
    assert_eq!(answers[0].1, answers[1].1, "the same answer twice");
    assert_eq!(answers[0].1.1.status, CommandStatus::Ok);
    assert_eq!(engine.stop_calls().len(), 1, "one fence, not two");
    assert_eq!(store.fences().len(), 1);
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Revoked);
    assert!(
        primary.try_recv().is_none(),
        "a redelivery emits no second node.fenced"
    );
}

/// Section 5.13: a revoke below the current token is refused and changes
/// nothing.
#[tokio::test(start_paused = true)]
async fn a_revoke_below_the_current_token_is_refused() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_below", 2).await;
    let lease_id = lease(LOCAL_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    let mut refused = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 3);
        settle().await;
        let id = primary.revoke(LOCAL_LEASE, 2, RevokeReason::Moved);
        refused = Some((id, primary.recv().await));
        settle().await;
    })
    .await;

    let (revoke, error) = refused.expect("the phase observed the refusal");
    let (answered, error) = error_of(error);
    assert_eq!(answered, revoke);
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(!error.fatal);
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Held);
    assert_eq!(agent.table().get(&lease_id).expect("the lease").token, 3);
    assert_eq!(engine.stop_calls().len(), 0);
    assert!(store.fences().is_empty());
    assert!(primary.try_recv().is_none());
    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Running
    );
}

/// Section 8.3: a `local` lease survives a lost session.
#[tokio::test(start_paused = true)]
async fn losing_the_session_does_not_touch_a_local_lease() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm = running_vm(&engine, "vm_local", 2).await;
    let lease_id = lease(LOCAL_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), vm.clone());

    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 1);
        settle().await;
        primary.lose_session();
        tokio::time::sleep(millis(600_000)).await;
    })
    .await;

    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Held);
    assert!(agent.may_start_stateful(&lease_id, 1));
    assert_eq!(
        engine.inspect(&vm).await.expect("inspect").state,
        VmState::Running
    );
    assert_eq!(engine.stop_calls().len(), 0);
    assert!(store.fences().is_empty());
}

/// Section 8.5 step 2 and section 12 `leases`: a restart keeps a fenced lease
/// fenced and the tokens monotonic; an expiring lease that was held at
/// restart is fenced.
#[tokio::test(start_paused = true)]
async fn a_restart_keeps_fenced_leases_fenced_and_tokens_monotonic() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let vm_local = running_vm(&engine, "vm_local", 2).await;
    let vm_expiring = running_vm(&engine, "vm_expiring", 3).await;
    let local_id = lease(LOCAL_LEASE);
    let expiring_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(local_id.clone(), vm_local.clone());
    agent.bind_vm(expiring_id.clone(), vm_expiring.clone());

    let t0 = Instant::now();
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_local(LOCAL_LEASE, APP, 1);
        primary.grant_expiring(EXPIRING_LEASE, "app_expiring", 1, 15000, None);
        primary.revoke(LOCAL_LEASE, 1, RevokeReason::Moved);
        let revoke_event = event_of(primary.recv().await);
        let NodeEventData::NodeFenced(fenced) = revoke_event.data;
        assert_eq!(fenced.reason, FenceReason::LeaseRevoked);
        let _result = command_result_of(primary.recv().await);

        // Keep the expiring lease held until the restart: answer its renewal.
        let (renew_id, at) = primary.recv_renew().await;
        assert_eq!(at - t0, millis(5000));
        primary.grant_expiring(EXPIRING_LEASE, "app_expiring", 2, 15000, Some(renew_id));
        settle().await;
    })
    .await;

    assert_eq!(lease_state(&agent, &local_id), LeaseState::Revoked);
    assert_eq!(lease_state(&agent, &expiring_id), LeaseState::Held);
    assert_eq!(store.fences().len(), 1);

    // A second agent on the same store models the restart.
    let (mut primary, mut inbound, outbound) = pair();
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    assert_eq!(lease_state(&agent, &local_id), LeaseState::Revoked);
    assert_eq!(
        lease_state(&agent, &expiring_id),
        LeaseState::Expired,
        "an expiring lease cannot prove itself after a restart"
    );
    assert!(!agent.may_start_stateful(&local_id, 1));
    assert!(!agent.may_start_stateful(&expiring_id, 2));

    let mut refused = None;
    let mut accepted = None;
    run_phase(&mut agent, &mut inbound, async {
        // The restore fence runs before anything else.
        let restore_event = primary.recv().await;
        let NodeEventData::NodeFenced(fenced) = event_of(restore_event).data;
        assert_eq!(fenced.reason, FenceReason::LeaseExpired);
        assert_eq!(fenced.lease_ids, vec![expiring_id.clone()]);

        let stale = primary.grant_local(LOCAL_LEASE, APP, 1);
        refused = Some((stale, primary.recv().await));
        accepted = Some(primary.grant_local(LOCAL_LEASE, APP, 2));
        settle().await;
    })
    .await;

    let (stale, error) = refused.expect("the phase observed the refusal");
    let (answered, error) = error_of(error);
    assert_eq!(answered, stale);
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(accepted.is_some());
    assert_eq!(lease_state(&agent, &local_id), LeaseState::Held);
    assert_eq!(agent.table().get(&local_id).expect("the lease").token, 2);
    assert!(agent.may_start_stateful(&local_id, 2));

    let fences = store.fences();
    assert_eq!(fences.len(), 2, "the restart fence is recorded too");
    assert_eq!(fences[1].reason, FenceReason::LeaseExpired);
    assert_eq!(fences[1].token, 2);
}

/// Section 8.5: the fence stops every bound VM concurrently, inside one
/// budget for the whole lease.
#[tokio::test(start_paused = true)]
async fn fencing_stops_all_bound_vms_concurrently() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let first = running_vm(&engine, "vm_first", 2).await;
    let second = running_vm(&engine, "vm_second", 3).await;
    for vm in [&first, &second] {
        engine.set_ignore_power_button(vm, true);
        engine.set_stop_delay(vm, millis(100));
    }
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), first.clone());
    agent.bind_vm(lease_id.clone(), second.clone());

    let t0 = Instant::now();
    let mut renew_at = None;
    let mut fenced_at = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, 3000, None);
        let (_id, at) = primary.recv_renew().await;
        renew_at = Some(at);
        let _event = event_of(primary.recv().await);
        fenced_at = Some(Instant::now());
    })
    .await;

    assert_eq!(renew_at.expect("a renewal") - t0, millis(1000));
    let fence_start = t0 + millis(2000);
    let elapsed = fenced_at.expect("a fence time") - fence_start;
    let budget = margin(millis(3000));
    assert!(
        elapsed <= budget,
        "the whole fence ({elapsed:?}) must fit in M ({budget:?}), not 2x"
    );

    for vm in [&first, &second] {
        assert_eq!(
            engine.inspect(vm).await.expect("inspect").state,
            VmState::Stopped
        );
    }
    assert_eq!(stop_counts(&engine.stop_calls()), (2, 2));
    assert_eq!(lease_state(&agent, &lease_id), LeaseState::Expired);
    assert!(store.fences()[0].complete);
    assert_eq!(store.fences()[0].vm_ids, vec![first, second]);
}

/// Keeps the renewal-body helper used, so the test file compiles with every
/// helper it declares.
#[tokio::test(start_paused = true)]
async fn a_renewal_carries_the_held_token() {
    let engine = FakeEngine::new();
    let store = MemoryLeaseStore::new();
    let (mut primary, mut inbound, outbound) = pair();
    let lease_id = lease(EXPIRING_LEASE);
    let mut agent = LeaseAgent::new(&engine, store.clone(), config(), outbound);
    agent.bind_vm(lease_id.clone(), running_vm(&engine, "vm_token", 2).await);

    let mut observed = None;
    run_phase(&mut agent, &mut inbound, async {
        primary.grant_expiring(EXPIRING_LEASE, APP, 1, 15000, None);
        observed = Some(renew_of(primary.recv().await));
        settle().await;
    })
    .await;

    let (_id, body) = observed.expect("a lease_renew");
    assert_eq!(body.lease_id, lease_id);
    assert_eq!(body.fencing_token, 1);
}
