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
    LeaseConfig {
        node_id: "nod_test".to_owned(),
        timers: FenceTimers {
            grace: Duration::from_millis(20),
            force: Duration::from_millis(20),
        },
    }
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
