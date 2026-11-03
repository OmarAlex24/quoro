// SPDX-License-Identifier: AGPL-3.0-only
//! Reconciliation tests (`node-agent-protocol.md` sections 6.1-6.3): create and
//! start, crash restart, stop and delete, and restart persistence.
//!
//! They run the real reconciler over `FakeEngine` through a recording engine
//! wrapper, because the fake exposes no network or spec getters.

mod agent_common;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use agent_common::{
    BRIDGE, GATEWAY, RecordingEngine, SUBNET, TempDir, dns, reconciler, snapshot, spec_vm,
};
use quoro_engine::{Engine, StopMode, VmId, VmState};
use quoro_node_agent::store::NodeStore;
use quoro_proto::wire::{DesiredStatus, ObservedStatus};

fn ip_of(text: Option<&String>) -> Ipv4Addr {
    text.expect("the status report carries an IP")
        .parse()
        .expect("a valid IP")
}

#[tokio::test]
async fn a_snapshot_creates_and_starts_both_projects() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    let snap = snapshot(
        7,
        vec![
            spec_vm("vm_a", "prj_1", DesiredStatus::Running),
            spec_vm("vm_b", "prj_2", DesiredStatus::Running),
        ],
    );
    let statuses = reconciler.reconcile_once(Some(&snap)).await;

    assert_eq!(statuses.len(), 2, "one entry per VM: {statuses:?}");
    for status in &statuses {
        assert_eq!(status.observed_status, ObservedStatus::Running);
        assert!(status.ip.is_some());
    }
    assert_ne!(
        ip_of(statuses[0].ip.as_ref()),
        ip_of(statuses[1].ip.as_ref()),
        "two VMs never share an address"
    );

    let vms = engine.list().await.unwrap();
    assert_eq!(vms.len(), 2);
    assert!(vms.iter().all(|info| info.state == VmState::Running));

    let created = engine.created();
    assert_eq!(created.len(), 2);
    let groups: Vec<Option<String>> = created
        .iter()
        .map(|spec| spec.net.isolation_group.clone())
        .collect();
    assert!(groups.contains(&Some("prj_1".to_owned())));
    assert!(groups.contains(&Some("prj_2".to_owned())));
    for spec in &created {
        assert_eq!(
            spec.net.ip.octets()[..3],
            [10, 42, 1],
            "the address is inside the subnet"
        );
        assert_ne!(spec.net.ip, GATEWAY.parse::<Ipv4Addr>().unwrap());
        assert_eq!(
            spec.init
                .env
                .iter()
                .find(|(name, _)| name == "APP_ENV")
                .map(|(_, value)| value.as_str()),
            Some("test"),
            "the spec environment reaches the init config"
        );
    }

    let networks = engine.networks();
    assert!(!networks.is_empty());
    let last = networks.last().unwrap();
    assert_eq!(last.bridge, BRIDGE);
    assert_eq!(last.subnet, SUBNET);
    assert_eq!(last.gateway, GATEWAY.parse::<Ipv4Addr>().unwrap());
    assert_eq!(last.dns, dns(), "guests get the node's configured DNS");

    assert_eq!(store.applied_rev().await.unwrap(), 7);
}

#[tokio::test]
async fn a_crashed_vm_is_restarted_with_a_backoff_and_counted() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    let snap = snapshot(
        1,
        vec![spec_vm("vm_crash", "prj_1", DesiredStatus::Running)],
    );
    let statuses = reconciler.reconcile_once(Some(&snap)).await;
    assert_eq!(statuses[0].observed_status, ObservedStatus::Running);

    let id = VmId("vm_crash".to_owned());
    engine.inner().crash(&id, "boom");
    let after_crash = reconciler.reconcile_once(Some(&snap)).await;
    assert_eq!(after_crash[0].observed_status, ObservedStatus::Crashed);

    // The 1 ms test backoff has run out by now.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let restarted = reconciler.reconcile_once(Some(&snap)).await;
    assert_eq!(restarted[0].observed_status, ObservedStatus::Running);
    assert_eq!(restarted[0].restart_count, 1);

    let record = store.vm_record("vm_crash").await.unwrap().unwrap();
    assert_eq!(record.restart_count, 1);
    assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Running);
    assert_eq!(store.applied_rev().await.unwrap(), 1);
}

#[tokio::test]
async fn a_stop_and_a_removal_reach_the_engine_and_the_removal_is_reported_once() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    let running = snapshot(1, vec![spec_vm("vm_a", "prj_1", DesiredStatus::Running)]);
    reconciler.reconcile_once(Some(&running)).await;
    assert_eq!(
        engine
            .inspect(&VmId("vm_a".to_owned()))
            .await
            .unwrap()
            .state,
        VmState::Running
    );

    let stopped = snapshot(2, vec![spec_vm("vm_a", "prj_1", DesiredStatus::Stopped)]);
    let statuses = reconciler.reconcile_once(Some(&stopped)).await;
    assert_eq!(statuses[0].observed_status, ObservedStatus::Stopped);
    assert!(
        engine
            .stops()
            .iter()
            .any(|(id, mode)| id.0 == "vm_a" && matches!(mode, StopMode::Graceful { .. })),
        "a desired stop is graceful: {:?}",
        engine.stops()
    );

    // A VM an earlier agent run left behind, and one engine VM that is not in
    // the spec at all.
    engine.raw_running_vm("vm_leftover").await;

    // Before the first snapshot nothing is deleted.
    reconciler.reconcile_once(None).await;
    assert!(
        engine
            .inspect(&VmId("vm_leftover".to_owned()))
            .await
            .is_ok()
    );
    assert!(engine.inspect(&VmId("vm_a".to_owned())).await.is_ok());

    let empty = snapshot(3, Vec::new());
    let statuses = reconciler.reconcile_once(Some(&empty)).await;
    let deleted: Vec<&str> = statuses
        .iter()
        .filter(|status| status.observed_status == ObservedStatus::Deleted)
        .map(|status| status.vm_id.as_str())
        .collect();
    assert_eq!(deleted, vec!["vm_a", "vm_leftover"]);
    assert!(engine.inspect(&VmId("vm_a".to_owned())).await.is_err());
    assert!(
        engine
            .inspect(&VmId("vm_leftover".to_owned()))
            .await
            .is_err()
    );
    assert!(
        engine
            .stops()
            .iter()
            .any(|(id, mode)| id.0 == "vm_leftover" && matches!(mode, StopMode::Force)),
        "an extra running VM is force-stopped: {:?}",
        engine.stops()
    );

    // `deleted` is reported once, not on every later pass.
    let again = reconciler.reconcile_once(Some(&empty)).await;
    assert!(
        again
            .iter()
            .all(|status| status.observed_status != ObservedStatus::Deleted),
        "{again:?}"
    );
}

#[tokio::test]
async fn a_new_reconciler_reuses_the_persisted_vm_and_its_address() {
    let dir = TempDir::new();
    let engine = Arc::new(RecordingEngine::new());
    let snap = snapshot(4, vec![spec_vm("vm_keep", "prj_1", DesiredStatus::Running)]);

    let first_ip;
    {
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (mut reconciler, _observed) = reconciler(engine.clone(), store);
        let statuses = reconciler.reconcile_once(Some(&snap)).await;
        first_ip = statuses[0].ip.clone();
        assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    }

    // A new agent process: the store is reopened and a fresh reconciler runs
    // over the same engine, whose VMMs outlived the first agent.
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (mut reconciler, _observed) = reconciler(engine.clone(), store);
    let statuses = reconciler.reconcile_once(Some(&snap)).await;

    assert_eq!(engine.create_count(), 1, "the VM is not created twice");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    assert_eq!(statuses[0].ip, first_ip, "the address survives a restart");
    assert_eq!(statuses[0].restart_count, 0, "a restart is not a crash");
}
