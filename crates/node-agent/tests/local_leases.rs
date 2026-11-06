// SPDX-License-Identifier: AGPL-3.0-only
//! The `local` lease service on the node (M5a): a grant is held and reported,
//! a revoke fences the bound VMs before it answers, stale grants are refused,
//! and a restart keeps a held lease (`node-agent-protocol.md` sections 5.12,
//! 5.13, 8.3, 8.5).

mod agent_common;
mod common;

use std::sync::Arc;
use std::time::Duration;

use agent_common::{TargetDir, snapshot_full, stateful_vm};
use quoro_engine::{Engine, FakeEngine, VmState};
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::{FenceTimers, LeaseConfig, LeaseState, LocalLeases, start_local_leases};
use quoro_proto::{
    CommandStatus, ErrorCode, LeaseGrant, LeaseId, LeaseMode, LeaseRevoke, MsgId, ResourceType,
    RevokeReason,
};
use tokio::sync::watch;

fn config() -> LeaseConfig {
    LeaseConfig::new(
        "nod_test",
        FenceTimers {
            grace: Duration::from_millis(20),
            force: Duration::from_millis(20),
        },
    )
}

fn local_grant(lease_id: &str, app_id: &str, token: u64) -> LeaseGrant {
    LeaseGrant {
        lease_id: LeaseId(lease_id.to_owned()),
        resource_type: ResourceType::App,
        resource_id: app_id.to_owned(),
        mode: LeaseMode::Local,
        fencing_token: token,
        duration_ms: None,
        granted_at: "2026-11-02T10:15:33Z".to_owned(),
    }
}

fn revoke(lease_id: &str, token: u64) -> LeaseRevoke {
    LeaseRevoke {
        lease_id: LeaseId(lease_id.to_owned()),
        fencing_token: token,
        reason: RevokeReason::Moved,
    }
}

fn msg(id: &str) -> MsgId {
    MsgId(id.to_owned())
}

async fn start(
    dir: &TargetDir,
    engine: Arc<FakeEngine>,
    desired: &watch::Sender<Option<quoro_node_agent::reconcile::Snapshot>>,
) -> (LocalLeases, NodeStore) {
    let store = NodeStore::open(dir.path()).await.unwrap();
    let leases = start_local_leases(engine, store.clone(), config(), desired.subscribe())
        .await
        .expect("the lease service starts");
    (leases, store)
}

#[tokio::test]
async fn a_local_grant_is_held_and_reported_in_hello() {
    let dir = TargetDir::new("lease-grant");
    let engine = Arc::new(FakeEngine::new());
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine, &desired).await;

    leases
        .link
        .grant(msg("msg_grant"), None, local_grant("lse_1", "app_1", 3))
        .await
        .expect("the grant is accepted");

    assert_eq!(leases.view.state("lse_1"), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful("lse_1", 3));
    assert!(!leases.view.may_start_stateful("lse_1", 2));

    let hello = leases.link.hello();
    assert_eq!(hello.len(), 1);
    assert_eq!(hello[0].lease_id, "lse_1");
    assert_eq!(hello[0].resource_id, "app_1");
    assert_eq!(hello[0].mode, LeaseMode::Local);
    assert_eq!(hello[0].fencing_token, 3);
    assert_eq!(hello[0].state, "held");

    let rows = store.load_leases().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, LeaseState::Held);
    assert_eq!(rows[0].token, 3);
    assert_eq!(
        store.load_highest_tokens().await.unwrap(),
        vec![((ResourceType::App, "app_1".to_owned()), 3)]
    );
    leases.abort();
}

#[tokio::test]
async fn a_revoke_stops_the_stateful_vm_and_is_idempotent() {
    let dir = TargetDir::new("lease-revoke");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_stateful", "lse_1", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(msg("msg_grant"), None, local_grant("lse_1", "app_1", 1))
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_stateful", 2).await;

    let result = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_1", 1))
        .await
        .expect("the revoke is accepted");
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(engine.stop_calls().len(), 1, "one fence");
    assert_eq!(leases.view.state("lse_1"), Some(LeaseState::Revoked));
    assert!(!leases.view.may_start_stateful("lse_1", 1));
    assert_eq!(leases.link.hello()[0].state, "revoked");
    let fences = store.fences().await.unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].lease_id, LeaseId("lse_1".to_owned()));
    assert!(fences[0].complete);
    assert_eq!(fences[0].vm_ids, vec![vm.clone()]);

    // A redelivered revoke gets the same answer and fences nothing again.
    let again = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_1", 1))
        .await
        .expect("the redelivery is accepted");
    assert_eq!(again, result);
    assert_eq!(engine.stop_calls().len(), 1, "no second fence");
    assert_eq!(store.fences().await.unwrap().len(), 1);
    leases.abort();
}

#[tokio::test]
async fn a_grant_with_a_lower_token_is_refused() {
    let dir = TargetDir::new("lease-stale");
    let engine = Arc::new(FakeEngine::new());
    let (desired, _) = watch::channel(None);
    let (leases, _store) = start(&dir, engine, &desired).await;

    leases
        .link
        .grant(msg("msg_1"), None, local_grant("lse_1", "app_1", 5))
        .await
        .unwrap();
    let error = leases
        .link
        .grant(msg("msg_2"), None, local_grant("lse_1", "app_1", 4))
        .await
        .expect_err("a lower token is refused");
    assert_eq!(error.code, ErrorCode::BadMessage);
    assert!(!error.fatal);
    assert_eq!(leases.view.state("lse_1"), Some(LeaseState::Held));
    assert_eq!(leases.link.hello()[0].fencing_token, 5);
    leases.abort();
}

#[tokio::test]
async fn a_restart_keeps_a_held_local_lease_held() {
    let dir = TargetDir::new("lease-restart");
    let engine = Arc::new(FakeEngine::new());
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(msg("msg_1"), None, local_grant("lse_1", "app_1", 7))
        .await
        .unwrap();
    let view = leases.view.clone();
    assert!(view.may_start_stateful("lse_1", 7));
    leases.abort();
    drop(store);
    tokio::task::yield_now().await;

    // A new agent process over the same store.
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine, &desired).await;
    assert_eq!(leases.view.state("lse_1"), Some(LeaseState::Held));
    assert!(leases.view.may_start_stateful("lse_1", 7));
    assert!(!leases.view.may_start_stateful("lse_1", 6));
    let hello = leases.link.hello();
    assert_eq!(hello.len(), 1);
    assert_eq!(hello[0].state, "held");
    assert_eq!(hello[0].fencing_token, 7);
    assert_eq!(store.load_leases().await.unwrap().len(), 1);
    leases.abort();
}

#[tokio::test]
async fn a_revoke_of_an_unknown_lease_is_answered_ok() {
    let dir = TargetDir::new("lease-unknown");
    let engine = Arc::new(FakeEngine::new());
    let (desired, _) = watch::channel(None);
    let (leases, _store) = start(&dir, engine.clone(), &desired).await;

    let result = leases
        .link
        .revoke(msg("msg_unknown"), None, revoke("lse_missing", 1))
        .await
        .expect("an unknown lease is not a protocol error");
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(result.detail.as_deref(), Some("unknown lease"));
    assert!(engine.stop_calls().is_empty());
    leases.abort();
}

fn slow_config() -> LeaseConfig {
    LeaseConfig::new(
        "nod_test",
        FenceTimers {
            grace: Duration::from_secs(60),
            force: Duration::from_secs(60),
        },
    )
}

async fn start_with(
    dir: &TargetDir,
    engine: Arc<FakeEngine>,
    desired: &watch::Sender<Option<quoro_node_agent::reconcile::Snapshot>>,
    config: LeaseConfig,
) -> (LocalLeases, NodeStore) {
    let store = NodeStore::open(dir.path()).await.unwrap();
    let leases = start_local_leases(engine, store.clone(), config, desired.subscribe())
        .await
        .expect("the lease service starts");
    (leases, store)
}

/// Waits (real time, bounded) until `ready` holds.
async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn stops_of(engine: &FakeEngine, vm: &quoro_engine::VmId) -> usize {
    engine
        .stop_calls()
        .iter()
        .filter(|(id, _)| id == vm)
        .count()
}

async fn fenced_events(store: &NodeStore) -> Vec<serde_json::Value> {
    store
        .buffered_events()
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.body)
        .filter(|body| body["type"] == "node.fenced" || body["type"] == "node.fence_failed")
        .collect()
}

/// Section 8.5 "Concurrency" (D-0075): the service keeps taking messages while
/// one lease is being fenced, and the fence of another lease does not wait for
/// it.
#[tokio::test]
async fn a_fence_in_progress_does_not_block_other_leases() {
    let dir = TargetDir::new("lease-nonblocking");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![
            stateful_vm("vm_slow", "lse_slow", 1, Vec::new()),
            stateful_vm("vm_fast", "lse_fast", 1, Vec::new()),
        ],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, _store) = start_with(&dir, engine.clone(), &desired, slow_config()).await;
    for (lease, app) in [("lse_slow", "app_slow"), ("lse_fast", "app_fast")] {
        leases
            .link
            .grant(
                msg(&format!("msg_grant_{lease}")),
                None,
                local_grant(lease, app, 1),
            )
            .await
            .unwrap();
    }
    let slow = common::running_vm(&engine, "vm_slow", 2).await;
    let fast = common::running_vm(&engine, "vm_fast", 3).await;
    engine.set_stop_delay(&slow, Duration::from_secs(3));

    let link = leases.link.clone();
    let slow_revoke = tokio::spawn(async move {
        link.revoke(msg("msg_revoke_slow"), None, revoke("lse_slow", 1))
            .await
    });
    wait_for("the slow stop to begin", || stops_of(&engine, &slow) == 1).await;

    // The other lease is revoked, fenced and answered while the slow fence
    // still runs, and a grant is applied meanwhile.
    let fast_result = leases
        .link
        .revoke(msg("msg_revoke_fast"), None, revoke("lse_fast", 1))
        .await
        .expect("the fast revoke is answered");
    assert_eq!(fast_result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&fast).await.unwrap().state, VmState::Stopped);
    leases
        .link
        .grant(
            msg("msg_grant_new"),
            None,
            local_grant("lse_new", "app_new", 1),
        )
        .await
        .expect("a grant is applied during another lease's fence");
    assert!(
        !slow_revoke.is_finished(),
        "the slow fence is still running"
    );
    assert_eq!(engine.inspect(&slow).await.unwrap().state, VmState::Running);

    // A newer grant of the lease being fenced does not let its VM start
    // before the fence is recorded.
    leases
        .link
        .grant(
            msg("msg_regrant"),
            None,
            local_grant("lse_slow", "app_slow", 2),
        )
        .await
        .expect("a higher token is accepted");
    assert_eq!(leases.view.state("lse_slow"), Some(LeaseState::Held));
    assert!(!leases.view.may_start_stateful("lse_slow", 2));

    let slow_result = slow_revoke.await.unwrap().expect("the slow revoke");
    assert_eq!(slow_result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&slow).await.unwrap().state, VmState::Stopped);
    assert!(leases.view.may_start_stateful("lse_slow", 2));
    leases.abort();
}

/// Section 8.5 "Concurrency" (D-0075): the fences of two leases stop their VMs
/// side by side, inside one stop time rather than two.
#[tokio::test]
async fn fences_of_different_leases_run_concurrently() {
    let dir = TargetDir::new("lease-concurrent");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![
            stateful_vm("vm_a", "lse_a", 1, Vec::new()),
            stateful_vm("vm_b", "lse_b", 1, Vec::new()),
        ],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start_with(&dir, engine.clone(), &desired, slow_config()).await;
    let mut vms = Vec::new();
    for (index, (lease, app, vm)) in [("lse_a", "app_a", "vm_a"), ("lse_b", "app_b", "vm_b")]
        .into_iter()
        .enumerate()
    {
        leases
            .link
            .grant(
                msg(&format!("msg_grant_{lease}")),
                None,
                local_grant(lease, app, 1),
            )
            .await
            .unwrap();
        let id = common::running_vm(&engine, vm, 2 + u8::try_from(index).unwrap()).await;
        engine.set_stop_delay(&id, Duration::from_millis(1500));
        vms.push(id);
    }

    let began = std::time::Instant::now();
    let (a, b) = tokio::join!(
        leases
            .link
            .revoke(msg("msg_revoke_a"), None, revoke("lse_a", 1)),
        leases
            .link
            .revoke(msg("msg_revoke_b"), None, revoke("lse_b", 1)),
    );
    let elapsed = began.elapsed();
    assert_eq!(a.unwrap().status, CommandStatus::Ok);
    assert_eq!(b.unwrap().status, CommandStatus::Ok);
    assert!(
        elapsed < Duration::from_millis(2900),
        "two 1.5 s stops took {elapsed:?}: the fences ran one after the other"
    );
    for vm in &vms {
        assert_eq!(engine.inspect(vm).await.unwrap().state, VmState::Stopped);
    }
    assert_eq!(store.fences().await.unwrap().len(), 2);
    assert_eq!(fenced_events(&store).await.len(), 2);
    leases.abort();
}

/// Section 6.4 against section 8.5: a start that holds the lease's start
/// permit finishes before the fence stops the VM, and once the lease is lost
/// no permit is given.
#[tokio::test]
async fn a_fence_waits_for_a_start_in_progress_and_then_stops_the_vm() {
    let dir = TargetDir::new("lease-gate");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_gate", "lse_gate", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, _store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_gate", "app_gate", 1),
        )
        .await
        .unwrap();

    // The reconciler is starting the VM: it holds the permit.
    let permit = leases
        .view
        .start_permit("lse_gate", 1)
        .await
        .expect("a held lease gives a permit");
    assert!(leases.view.start_permit("lse_gate", 2).await.is_none());
    let vm = common::running_vm(&engine, "vm_gate", 2).await;

    let link = leases.link.clone();
    let revoke_task = tokio::spawn(async move {
        link.revoke(msg("msg_revoke"), None, revoke("lse_gate", 1))
            .await
    });
    wait_for("the lease to be revoked", || {
        leases.view.state("lse_gate") == Some(LeaseState::Revoked)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(stops_of(&engine, &vm), 0, "the fence waits for the start");
    assert!(!revoke_task.is_finished());

    drop(permit);
    let result = revoke_task.await.unwrap().expect("the revoke");
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert!(leases.view.start_permit("lse_gate", 1).await.is_none());
    leases.abort();
}

/// Section 8.5 "Concurrency" (D-0075): an agent that dies in the middle of
/// two fences restores both leases fenced, fences them again at once, and
/// records each fence and its event exactly once.
#[tokio::test]
async fn a_crash_mid_fence_fences_again_on_restart() {
    let dir = TargetDir::new("lease-crash");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![
            stateful_vm("vm_one", "lse_one", 1, Vec::new()),
            stateful_vm("vm_two", "lse_two", 1, Vec::new()),
        ],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot.clone()));
    let (leases, store) = start_with(&dir, engine.clone(), &desired, slow_config()).await;
    let mut vms = Vec::new();
    for (index, (lease, app, vm)) in [
        ("lse_one", "app_one", "vm_one"),
        ("lse_two", "app_two", "vm_two"),
    ]
    .into_iter()
    .enumerate()
    {
        leases
            .link
            .grant(
                msg(&format!("msg_grant_{lease}")),
                None,
                local_grant(lease, app, 1),
            )
            .await
            .unwrap();
        let id = common::running_vm(&engine, vm, 2 + u8::try_from(index).unwrap()).await;
        // A VMM that takes far longer to stop than the agent lives.
        engine.set_stop_delay(&id, Duration::from_secs(600));
        vms.push(id);
    }
    let mut pending = Vec::new();
    for lease in ["lse_one", "lse_two"] {
        let link = leases.link.clone();
        pending.push(tokio::spawn(async move {
            link.revoke(msg(&format!("msg_revoke_{lease}")), None, revoke(lease, 1))
                .await
        }));
    }
    wait_for("both stops to begin", || {
        vms.iter().all(|vm| stops_of(&engine, vm) == 1)
    })
    .await;

    // The agent dies mid-fence.
    leases.abort();
    for task in pending {
        let answer = task
            .await
            .unwrap()
            .expect("a dead service is not a refusal");
        assert_eq!(
            answer.status,
            CommandStatus::Error,
            "a dead service never answers ok"
        );
        assert!(answer.error.is_none(), "nor fence_failed");
    }
    drop(store);
    for vm in &vms {
        assert_eq!(
            engine.inspect(vm).await.unwrap().state,
            VmState::Running,
            "the VMM outlives the agent"
        );
        engine.set_stop_delay(vm, Duration::ZERO);
    }

    // A new agent process over the same store.
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    for lease in ["lse_one", "lse_two"] {
        assert_eq!(leases.view.state(lease), Some(LeaseState::Revoked));
        assert!(!leases.view.may_start_stateful(lease, 1));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while store.fences().await.unwrap().len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "the restart fences again"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for vm in &vms {
        assert_eq!(engine.inspect(vm).await.unwrap().state, VmState::Stopped);
    }
    let fences = store.fences().await.unwrap();
    assert_eq!(fences.len(), 2);
    assert!(
        fences
            .iter()
            .all(|fence| fence.complete && fence.token == 1)
    );
    assert_eq!(fenced_events(&store).await.len(), 2, "one event per fence");
    assert!(!leases.view.is_failed());
    leases.abort();
    drop(store);

    // A second restart finds both fences recorded and fences nothing.
    let stops_before = engine.stop_calls().len();
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(engine.stop_calls().len(), stops_before);
    assert_eq!(store.fences().await.unwrap().len(), 2);
    leases.abort();
}

/// D-0074 with concurrent fences: a VM that cannot be stopped fails its fence,
/// marks the node failed in memory and in the store, and the revoke is
/// answered `fence_failed`.
#[tokio::test]
async fn an_unstoppable_vm_fails_the_fence_and_marks_the_node_failed() {
    let dir = TargetDir::new("lease-failed");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_stuck", "lse_stuck", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_stuck", "app_stuck", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_stuck", 2).await;
    engine.set_unkillable(&vm, true);

    let result = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_stuck", 1))
        .await
        .expect("the revoke is answered");
    assert_eq!(result.status, CommandStatus::Error);
    assert_eq!(result.error.expect("an error").code, ErrorCode::FenceFailed);
    assert!(leases.view.is_failed());
    assert!(store.node_failed().await.unwrap());
    let fences = store.fences().await.unwrap();
    assert_eq!(fences.len(), 1);
    assert!(!fences[0].complete);
    let events = fenced_events(&store).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "node.fence_failed");
    assert_eq!(events[0]["data"]["vm_ids"][0], "vm_stuck");
    leases.abort();
}

/// Section 5.13 during a running fence: a redelivered revoke and a second
/// revoke with a higher token join the fence instead of stopping the VM again,
/// all of them are answered once it is recorded, and the record carries the
/// higher token.
#[tokio::test]
async fn revokes_that_arrive_during_a_fence_join_it() {
    let dir = TargetDir::new("lease-join");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_join", "lse_join", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start_with(&dir, engine.clone(), &desired, slow_config()).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_join", "app_join", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_join", 2).await;
    engine.set_stop_delay(&vm, Duration::from_millis(800));

    let mut answers = Vec::new();
    for (id, token) in [("msg_first", 1), ("msg_first", 1), ("msg_higher", 2)] {
        let link = leases.link.clone();
        answers.push(tokio::spawn(async move {
            link.revoke(msg(id), None, revoke("lse_join", token)).await
        }));
        if answers.len() == 1 {
            wait_for("the stop to begin", || stops_of(&engine, &vm) == 1).await;
        }
    }
    for answer in answers {
        let result = answer.await.unwrap().expect("every revoke is answered");
        assert_eq!(result.status, CommandStatus::Ok);
    }
    assert_eq!(stops_of(&engine, &vm), 1, "the VM was stopped once");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    let fences = store.fences().await.unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].token, 2);
    let rows = store.load_leases().await.unwrap();
    assert_eq!(rows[0].state, LeaseState::Revoked);
    assert_eq!(rows[0].token, 2);

    // Both answers are final: a redelivery fences nothing again.
    let again = leases
        .link
        .revoke(msg("msg_higher"), None, revoke("lse_join", 2))
        .await
        .unwrap();
    assert_eq!(again.status, CommandStatus::Ok);
    assert_eq!(stops_of(&engine, &vm), 1);
    assert_eq!(store.fences().await.unwrap().len(), 1);
    leases.abort();
}

/// Section 8.5 step 1: a snapshot that dropped a VM before the revoke arrived
/// does not let the VM escape the fence; it is stopped and recorded.
#[tokio::test]
async fn a_vm_dropped_from_the_snapshot_before_the_revoke_is_still_fenced() {
    let dir = TargetDir::new("lease-dropped");
    let engine = Arc::new(FakeEngine::new());
    let bound = snapshot_full(
        1,
        vec![stateful_vm("vm_dropped", "lse_dropped", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(bound));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_dropped", "app_dropped", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_dropped", 2).await;

    // The primary moves the app: the new snapshot no longer names the VM,
    // and the reconciler has not removed it yet.
    desired.send_replace(Some(snapshot_full(2, Vec::new(), Vec::new(), Vec::new())));
    let result = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_dropped", 1))
        .await
        .unwrap();
    assert_eq!(result.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    let fences = store.fences().await.unwrap();
    assert_eq!(fences[0].vm_ids, vec![vm]);
    leases.abort();
}

/// Section 8.5 with an agent restart that finds no verified snapshot: the
/// restore cannot tell which VMs the interrupted fence was stopping, so it
/// stops every app VM on the node rather than record a fence that stopped
/// nothing. A builder VM holds no lease and keeps running.
#[tokio::test]
async fn a_restart_without_a_snapshot_fences_every_app_vm() {
    let dir = TargetDir::new("lease-nosnapshot");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_bound", "lse_lost", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start_with(&dir, engine.clone(), &desired, slow_config()).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_lost", "app_lost", 1),
        )
        .await
        .unwrap();
    let bound = common::running_vm(&engine, "vm_bound", 2).await;
    let other = common::running_vm(&engine, "vm_other", 3).await;
    let builder = common::running_vm(&engine, "bld_builder", 4).await;
    engine.set_stop_delay(&bound, Duration::from_secs(600));
    let link = leases.link.clone();
    let pending = tokio::spawn(async move {
        link.revoke(msg("msg_revoke"), None, revoke("lse_lost", 1))
            .await
    });
    wait_for("the stop to begin", || stops_of(&engine, &bound) == 1).await;
    leases.abort();
    let _ = pending.await;
    drop(store);
    engine.set_stop_delay(&bound, Duration::ZERO);

    // The new process cannot verify any stored snapshot.
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while store.fences().await.unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the restart fences again"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for vm in [&bound, &other] {
        assert_eq!(engine.inspect(vm).await.unwrap().state, VmState::Stopped);
    }
    assert_eq!(
        engine.inspect(&builder).await.unwrap().state,
        VmState::Running
    );
    let fences = store.fences().await.unwrap();
    assert!(fences[0].complete);
    assert!(fences[0].vm_ids.contains(&bound));
    assert!(!fences[0].vm_ids.contains(&builder));
    leases.abort();
}

/// Section 5.9: a revoke is answered `ok` only once its fence is recorded. A
/// fence whose record cannot be written is answered with an error that is not
/// `fence_failed`, and the lease stays unstartable in this process.
#[tokio::test]
async fn a_fence_that_cannot_be_recorded_is_not_answered_ok() {
    let dir = TargetDir::new("lease-unrecorded");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm(
            "vm_unrecorded",
            "lse_unrecorded",
            1,
            Vec::new(),
        )],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_unrecorded", "app_unrecorded", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_unrecorded", 2).await;

    // The local store stops taking writes.
    store.pool().close().await;
    let result = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_unrecorded", 1))
        .await
        .unwrap();
    assert_eq!(result.status, CommandStatus::Error);
    assert!(result.error.is_none(), "not a failed fence: {result:?}");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert!(!leases.view.is_failed());
    // A newer grant does not let the VM start before a fence is recorded.
    leases
        .link
        .grant(
            msg("msg_regrant"),
            None,
            local_grant("lse_unrecorded", "app_unrecorded", 2),
        )
        .await
        .unwrap();
    assert!(!leases.view.may_start_stateful("lse_unrecorded", 2));

    // The answer is not final: a redelivery fences again.
    let stops = stops_of(&engine, &vm);
    let again = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_unrecorded", 2))
        .await
        .unwrap();
    assert_eq!(again.status, CommandStatus::Error);
    assert!(stops_of(&engine, &vm) > stops);
    leases.abort();
}

/// Section 4.2 rule 3 at every session start: the primary revokes again a
/// lease the node still reports as `revoked`. With its fence recorded, the
/// revoke is answered `ok` at once and stops nothing, even after a restart
/// that found no snapshot, so other apps' VMs keep running.
#[tokio::test]
async fn a_revoke_of_a_lease_already_fenced_stops_nothing() {
    let dir = TargetDir::new("lease-refence");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_done", "lse_done", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_done", "app_done", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_done", 2).await;
    let first = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_done", 1))
        .await
        .unwrap();
    assert_eq!(first.status, CommandStatus::Ok);
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    leases.abort();
    drop(store);

    // A restart that cannot verify a snapshot, with another app running.
    let other = common::running_vm(&engine, "vm_other", 3).await;
    let stops = engine.stop_calls().len();
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    assert_eq!(leases.view.state("lse_done"), Some(LeaseState::Revoked));
    let again = leases
        .link
        .revoke(msg("msg_revoke_again"), None, revoke("lse_done", 1))
        .await
        .unwrap();
    assert_eq!(again.status, CommandStatus::Ok);
    assert_eq!(engine.stop_calls().len(), stops, "nothing was stopped");
    assert_eq!(
        engine.inspect(&other).await.unwrap().state,
        VmState::Running
    );
    assert_eq!(store.fences().await.unwrap().len(), 1);
    assert_eq!(fenced_events(&store).await.len(), 1);

    // A revoke with a higher token is a new ownership change: it fences.
    let higher = leases
        .link
        .revoke(msg("msg_revoke_higher"), None, revoke("lse_done", 2))
        .await
        .unwrap();
    assert_eq!(higher.status, CommandStatus::Ok);
    assert_eq!(store.fences().await.unwrap().len(), 2);
    assert_eq!(store.load_highest_tokens().await.unwrap()[0].1, 2);
    leases.abort();
}

/// Section 12 `leases`: a database an older agent created gains the validity
/// columns, its rows load with none, and a stored validity reads back exactly.
#[tokio::test]
async fn the_lease_table_of_an_older_agent_gains_the_validity_columns() {
    let dir = TargetDir::new("lease-migrate");
    {
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(dir.path().join("node.db"))
            .create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE leases (lease_id TEXT PRIMARY KEY, resource_type TEXT NOT NULL, \
             resource_id TEXT NOT NULL, mode TEXT NOT NULL, token INTEGER NOT NULL, \
             state TEXT NOT NULL, duration_ms INTEGER); \
             INSERT INTO leases VALUES ('lse_old', 'app', 'app_old', 'local', 3, 'held', NULL);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }
    let store = NodeStore::open(dir.path()).await.expect("the store opens");
    let rows = store.load_leases().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].lease_id, LeaseId("lse_old".to_owned()));
    assert_eq!(rows[0].validity, None);

    let validity = quoro_node_agent::StoredValidity {
        boot_id: "boot-a".to_owned(),
        until_boottime: Duration::from_nanos(1_234_567_890_123),
    };
    let mut row = rows[0].clone();
    row.mode = LeaseMode::Expiring;
    row.duration = Some(Duration::from_millis(15_000));
    row.validity = Some(validity.clone());
    store.save_lease(&row, 3).await.unwrap();
    assert_eq!(
        store.load_leases().await.unwrap()[0].validity,
        Some(validity)
    );
}

/// Sections 5.13 and 8.5: a fence that cannot list the engine's VMs cannot
/// confirm that every bound VM stopped, so it fails like an unconfirmed forced
/// stop: the revoke is answered `fence_failed`, the node is marked failed in
/// memory and in the store, and `node.fence_failed` names the listing error.
#[tokio::test]
async fn a_fence_that_cannot_list_the_vms_fails() {
    let dir = TargetDir::new("lease-unlisted");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_listed", "lse_listed", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_listed", "app_listed", 1),
        )
        .await
        .unwrap();
    let vm = common::running_vm(&engine, "vm_listed", 2).await;
    // An app VM the snapshot does not name: only the listing would find it.
    let unnamed = common::running_vm(&engine, "vm_unnamed", 3).await;

    engine.fail_next(
        quoro_engine::FakeOp::List,
        quoro_engine::EngineError::Io("the VM state file is unreadable".to_owned()),
    );
    let result = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_listed", 1))
        .await
        .unwrap();
    assert_eq!(result.status, CommandStatus::Error);
    assert_eq!(
        result.error.as_ref().map(|error| error.code),
        Some(ErrorCode::FenceFailed)
    );
    assert!(
        result
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("could not list the engine's VMs")),
        "{:?}",
        result.detail
    );
    // The VM the fence knew of is stopped all the same; the one only the
    // listing would have found still runs, which is why the fence failed.
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Stopped);
    assert_eq!(
        engine.inspect(&unnamed).await.unwrap().state,
        VmState::Running
    );
    assert!(leases.view.is_failed());
    assert!(store.node_failed().await.unwrap());
    let fences = store.fences().await.unwrap();
    assert_eq!(fences.len(), 1);
    assert!(!fences[0].complete);
    assert_eq!(fences[0].vm_ids, vec![vm.clone()]);
    let events = fenced_events(&store).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "node.fence_failed");
    // No stop failed, so no VM is named; the error says the set is unknown.
    assert_eq!(events[0]["data"]["vm_ids"], serde_json::json!([]));
    assert!(
        events[0]["data"]["error"]
            .as_str()
            .is_some_and(|error| error.contains("could not list the engine's VMs")
                && error.contains("any app VM on this node may still run"))
    );

    // The answer is final: a redelivery gets it again and fences nothing.
    let stops = stops_of(&engine, &vm);
    let again = leases
        .link
        .revoke(msg("msg_revoke"), None, revoke("lse_listed", 1))
        .await
        .unwrap();
    assert_eq!(again, result);
    assert_eq!(stops_of(&engine, &vm), stops);
    leases.abort();
    drop(store);

    // A restart keeps the node failed and runs no fence: the failed fence is
    // recorded, and nothing stateful starts until an operator acknowledges it.
    let stops_before = engine.stop_calls().len();
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(engine.stop_calls().len(), stops_before);
    assert!(leases.view.is_failed());
    assert!(!leases.view.may_start_stateful("lse_listed", 1));
    assert_eq!(leases.view.state("lse_listed"), Some(LeaseState::Revoked));

    // A new revoke fences again; the listing works now, so the unnamed VM is
    // stopped and the fence is complete. The node stays failed.
    let result = leases
        .link
        .revoke(msg("msg_revoke_again"), None, revoke("lse_listed", 1))
        .await
        .unwrap();
    assert_eq!(result.status, CommandStatus::Ok, "{result:?}");
    assert_eq!(
        engine.inspect(&unnamed).await.unwrap().state,
        VmState::Stopped
    );
    assert!(leases.view.is_failed());
    assert!(store.node_failed().await.unwrap());
    leases.abort();
}

/// Section 5.17 `fence_ack` and section 8.5 "Acknowledging a failed fence":
/// an acknowledgement of some failed fences leaves the node failed and names
/// the rest; one of all of them clears the mark, in memory and in the store,
/// across a restart. The acknowledged leases stay unstartable until a grant
/// with a higher token, and a redelivered acknowledgement is harmless.
#[tokio::test]
async fn a_fence_ack_clears_the_failed_mark_once_no_failed_fence_remains() {
    let dir = TargetDir::new("lease-fence-ack");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![
            stateful_vm("vm_one", "lse_one", 1, Vec::new()),
            stateful_vm("vm_two", "lse_two", 1, Vec::new()),
        ],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot.clone()));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    for (index, (lease, app, vm)) in [
        ("lse_one", "app_one", "vm_one"),
        ("lse_two", "app_two", "vm_two"),
    ]
    .into_iter()
    .enumerate()
    {
        leases
            .link
            .grant(
                msg(&format!("msg_grant_{lease}")),
                None,
                local_grant(lease, app, 1),
            )
            .await
            .unwrap();
        let id = common::running_vm(&engine, vm, 2 + u8::try_from(index).unwrap()).await;
        engine.set_unkillable(&id, true);
        let result = leases
            .link
            .revoke(msg(&format!("msg_revoke_{lease}")), None, revoke(lease, 1))
            .await
            .unwrap();
        assert_eq!(
            result.error.map(|error| error.code),
            Some(ErrorCode::FenceFailed)
        );
    }
    assert!(leases.view.is_failed());
    assert_eq!(
        store.failed_fences().await.unwrap(),
        vec!["lse_one".to_owned(), "lse_two".to_owned()]
    );

    // The operator verified one of them.
    let partial = leases.link.fence_ack(vec!["lse_one".to_owned()]).await;
    assert_eq!(partial.status, CommandStatus::Ok);
    assert!(
        partial
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("lse_two")),
        "{partial:?}"
    );
    assert!(leases.view.is_failed());
    assert!(store.node_failed().await.unwrap());

    // And then both; a lease with no failed fence is not an error.
    let all = leases
        .link
        .fence_ack(vec!["lse_two".to_owned(), "lse_one".to_owned()])
        .await;
    assert_eq!(all.status, CommandStatus::Ok);
    assert!(!leases.view.is_failed());
    assert!(!store.node_failed().await.unwrap());
    assert!(store.failed_fences().await.unwrap().is_empty());
    let again = leases.link.fence_ack(vec!["lse_one".to_owned()]).await;
    assert_eq!(again.status, CommandStatus::Ok);

    // The leases stay revoked: nothing starts at the old token, and a grant
    // with the higher token the acknowledgement gave them starts again.
    assert!(!leases.view.may_start_stateful("lse_one", 1));
    leases
        .link
        .grant(
            msg("msg_regrant"),
            None,
            local_grant("lse_one", "app_one", 2),
        )
        .await
        .unwrap();
    assert!(leases.view.may_start_stateful("lse_one", 2));
    leases.abort();
    drop(store);

    // A restart keeps the mark cleared.
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    assert!(!leases.view.is_failed());
    assert!(!store.node_failed().await.unwrap());
    leases.abort();
}

/// Section 12 `node_meta`: a node an older agent marked failed has no list of
/// its failed fences; the store takes them from the incomplete fence records,
/// so a `fence_ack` can clear the mark.
#[tokio::test]
async fn an_older_failed_mark_lists_its_incomplete_fences() {
    let dir = TargetDir::new("lease-older-failed");
    let store = NodeStore::open(dir.path()).await.unwrap();
    store
        .record_fence(&quoro_node_agent::FenceRecord {
            lease_id: LeaseId("lse_old".to_owned()),
            token: 3,
            vm_ids: vec![quoro_engine::VmId("vm_old".to_owned())],
            at: std::time::SystemTime::now(),
            reason: quoro_proto::FenceReason::LeaseRevoked,
            complete: false,
        })
        .await
        .unwrap();
    store.set_node_failed(true).await.unwrap();
    store.pool().close().await;
    drop(store);

    let store = NodeStore::open(dir.path()).await.unwrap();
    assert_eq!(
        store.failed_fences().await.unwrap(),
        vec!["lse_old".to_owned()]
    );
    store.pool().close().await;
    drop(store);

    // The older primary never listed that failure, so the upgraded agent
    // reports it once, naming the VM its record names.
    let engine = Arc::new(FakeEngine::new());
    let (desired, _) = watch::channel(None);
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    assert!(leases.view.is_failed());
    let events = fenced_events(&store).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["type"], "node.fence_failed");
    assert_eq!(events[0]["subject"]["id"], "lse_old");
    assert_eq!(
        events[0]["data"]["lease_ids"],
        serde_json::json!(["lse_old"])
    );
    assert_eq!(events[0]["data"]["vm_ids"], serde_json::json!(["vm_old"]));
    assert_eq!(events[0]["data"]["reason"], "lease_revoked");
    leases.abort();
    store.pool().close().await;
    drop(store);

    // Only once: a second start reports nothing more.
    let (leases, store) = start(&dir, engine, &desired).await;
    assert_eq!(fenced_events(&store).await.len(), 1);
    let result = leases.link.fence_ack(vec!["lse_old".to_owned()]).await;
    assert_eq!(result.status, CommandStatus::Ok, "{result:?}");
    assert!(!leases.view.is_failed());
    assert!(!store.node_failed().await.unwrap());
    leases.abort();
}

/// A `fence_ack` naming a lease whose fence still runs is refused: that
/// fence may fail after the operator checked, and its outcome must not be
/// cleared unseen. Once the fence is answered, a `fence_ack` applies.
#[tokio::test]
async fn a_fence_ack_waits_for_a_running_fence_of_its_lease() {
    let dir = TargetDir::new("lease-fence-ack-running");
    let engine = Arc::new(FakeEngine::new());
    let snapshot = snapshot_full(
        1,
        vec![stateful_vm("vm_slow", "lse_slow", 1, Vec::new())],
        Vec::new(),
        Vec::new(),
    );
    let (desired, _) = watch::channel(Some(snapshot));
    let (leases, store) = start(&dir, engine.clone(), &desired).await;
    leases
        .link
        .grant(
            msg("msg_grant"),
            None,
            local_grant("lse_slow", "app_slow", 1),
        )
        .await
        .unwrap();
    let slow = common::running_vm(&engine, "vm_slow", 2).await;
    engine.set_stop_delay(&slow, Duration::from_secs(2));

    let link = leases.link.clone();
    let revoking = tokio::spawn(async move {
        link.revoke(msg("msg_revoke"), None, revoke("lse_slow", 1))
            .await
    });
    wait_for("the slow stop to begin", || stops_of(&engine, &slow) == 1).await;
    let refused = leases.link.fence_ack(vec!["lse_slow".to_owned()]).await;
    assert_eq!(refused.status, CommandStatus::Error, "{refused:?}");
    assert!(
        refused
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("is running")),
        "{refused:?}"
    );

    // The stop outlasts the fence's budget, so this fence fails after the
    // refused acknowledgement: the failure stands.
    let answer = revoking.await.unwrap().unwrap();
    assert_eq!(
        answer.error.as_ref().map(|error| error.code),
        Some(ErrorCode::FenceFailed),
        "{answer:?}"
    );
    assert!(leases.view.is_failed());
    assert_eq!(
        store.failed_fences().await.unwrap(),
        vec!["lse_slow".to_owned()]
    );

    // An acknowledgement sent once the fence is answered applies.
    let applied = leases.link.fence_ack(vec!["lse_slow".to_owned()]).await;
    assert_eq!(applied.status, CommandStatus::Ok, "{applied:?}");
    assert!(!leases.view.is_failed());
    assert!(!store.node_failed().await.unwrap());
    leases.abort();
}
