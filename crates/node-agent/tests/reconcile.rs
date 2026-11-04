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
use quoro_node_agent::reconcile::Snapshot;
use quoro_node_agent::store::NodeStore;
use quoro_proto::wire::{DesiredStatus, ObservedStatus, SpecNetwork, SpecVm};

fn ip_of(text: Option<&String>) -> Ipv4Addr {
    text.expect("the status report carries an IP")
        .parse()
        .expect("a valid IP")
}

/// A snapshot whose node network is `subnet` with `gateway`.
fn snapshot_on(rev: u64, subnet: &str, gateway: &str, vms: Vec<SpecVm>) -> Snapshot {
    let mut snap = snapshot(rev, vms);
    snap.state.spec.network = Some(SpecNetwork {
        subnet: subnet.to_owned(),
        gateway: gateway.to_owned(),
    });
    snap
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

#[tokio::test]
async fn a_changed_subnet_lands_on_a_second_bridge_and_keeps_running_vms() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    let old = snapshot_on(
        1,
        "10.42.0.0/24",
        "10.42.0.1",
        vec![spec_vm("vm_a", "prj_1", DesiredStatus::Running)],
    );
    let first = reconciler.reconcile_once(Some(&old)).await;
    assert_eq!(first[0].observed_status, ObservedStatus::Running);
    let old_ip = ip_of(first[0].ip.as_ref());
    assert_eq!(old_ip.octets()[..3], [10, 42, 0]);
    assert_eq!(engine.create_count(), 1);

    // The node's subnet changed: the configured bridge is recorded with the old
    // network, so the engine refuses it and the agent takes the next name.
    let new = snapshot_on(
        2,
        "10.42.1.0/24",
        "10.42.1.1",
        vec![
            spec_vm("vm_a", "prj_1", DesiredStatus::Running),
            spec_vm("vm_b", "prj_1", DesiredStatus::Running),
        ],
    );
    let second = reconciler.reconcile_once(Some(&new)).await;

    let status_a = second
        .iter()
        .find(|status| status.vm_id == "vm_a")
        .expect("vm_a is reported");
    assert_eq!(
        ip_of(status_a.ip.as_ref()),
        old_ip,
        "an existing VM keeps its old address and is not recreated"
    );
    assert_eq!(
        store.vm_record("vm_a").await.unwrap().unwrap().ip,
        Some(old_ip)
    );
    assert_eq!(
        engine
            .inspect(&VmId("vm_a".to_owned()))
            .await
            .unwrap()
            .state,
        VmState::Running
    );

    let status_b = second
        .iter()
        .find(|status| status.vm_id == "vm_b")
        .expect("vm_b is reported");
    let new_ip = ip_of(status_b.ip.as_ref());
    assert_eq!(
        new_ip.octets()[..3],
        [10, 42, 1],
        "a new VM takes an address in the new subnet"
    );

    assert_eq!(
        engine.create_count(),
        2,
        "only the new VM is created; vm_a keeps its engine-side spec"
    );
    let created = engine.created();
    assert_eq!(created[0].id.0, "vm_a");
    assert_eq!(created[0].net.ip, old_ip);
    assert_eq!(created[1].id.0, "vm_b");
    assert_eq!(created[1].net.ip, new_ip);

    let networks = engine.networks();
    assert!(
        networks
            .iter()
            .any(|net| net.bridge == BRIDGE && net.subnet == "10.42.1.0/24"),
        "the configured bridge is tried first: {networks:?}"
    );
    let taken = networks
        .iter()
        .find(|net| net.subnet == "10.42.1.0/24" && net.bridge != BRIDGE)
        .expect("a second bridge carries the new subnet");
    assert_eq!(taken.bridge, "quoro0s1");
    assert_eq!(taken.gateway, Ipv4Addr::new(10, 42, 1, 1));
    assert_eq!(taken.dns, dns());

    assert_eq!(store.applied_rev().await.unwrap(), 2);
}

#[tokio::test]
async fn a_snapshot_without_a_network_creates_nothing_and_does_not_fail() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    // An edge-only machine has no `spec.network` (section 5.11).
    let mut edge_only = snapshot(1, vec![spec_vm("vm_edge", "prj_1", DesiredStatus::Running)]);
    edge_only.state.spec.network = None;

    let statuses = reconciler.reconcile_once(Some(&edge_only)).await;
    assert!(statuses.is_empty(), "{statuses:?}");
    assert_eq!(engine.create_count(), 0, "no VM is created");
    assert!(engine.networks().is_empty(), "no bridge is ensured");

    // The same reconciler still applies a node snapshot: the pass did not
    // leave it in a failed state.
    let node = snapshot(2, vec![spec_vm("vm_edge", "prj_1", DesiredStatus::Running)]);
    let statuses = reconciler.reconcile_once(Some(&node)).await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    assert_eq!(engine.create_count(), 1);
    assert_eq!(engine.networks().last().unwrap().subnet, SUBNET);
}
