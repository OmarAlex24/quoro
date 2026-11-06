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

use base64::Engine as _;

use agent_common::{
    BRIDGE, GATEWAY, RecordingEngine, SUBNET, TargetDir, TempDir, dns, local_lease_table,
    reconciler, snapshot, snapshot_full, spec_vm, spec_volume, stateful_vm, volume_attach,
};
use quoro_engine::{Engine, EngineError, FakeOp, StopMode, VmId, VmState, VolumeId};
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_node_agent::LeaseView;
use quoro_node_agent::edge::EdgeManager;
use quoro_node_agent::ingress::{CertificateEvents, DnsBridge, IngressConfig, IngressSupervisor};
use quoro_node_agent::reconcile::Snapshot;
use quoro_node_agent::store::{NodeStore, VolumeRecord};
use quoro_proto::wire::VolumeStatus;
use quoro_proto::wire::{
    DesiredStatus, Health, ObservedStatus, SpecEdge, SpecNetwork, SpecVm, SpecVolumeState,
    VolumeState,
};

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
    // With no `spec.edge` there is nothing else to apply: the revision is
    // recorded.
    assert_eq!(store.applied_rev().await.unwrap(), 1);

    // The same reconciler still applies a node snapshot: the pass did not
    // leave it in a failed state.
    let node = snapshot(2, vec![spec_vm("vm_edge", "prj_1", DesiredStatus::Running)]);
    let statuses = reconciler.reconcile_once(Some(&node)).await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    assert_eq!(engine.create_count(), 1);
    assert_eq!(engine.networks().last().unwrap().subnet, SUBNET);
}

/// An edge manager over a real ingress supervisor whose cloudflared binary
/// does not exist: an empty `spec.edge` starts no listener and no connector.
fn edge_manager(dir: &TempDir, store: &NodeStore) -> Arc<EdgeManager> {
    let config = IngressConfig::new(
        dir.path(),
        CloudflaredConfig {
            binary: dir.path().join("no-cloudflared"),
            metrics: "127.0.0.1:0".parse().unwrap(),
            extra_args: Vec::new(),
        },
    );
    let supervisor = Arc::new(IngressSupervisor::new(
        config,
        Arc::new(DnsBridge::new()),
        CertificateEvents::new(store.clone(), None),
    ));
    Arc::new(EdgeManager::new(supervisor))
}

/// An edge-only snapshot: no `spec.network`, no VM, and an empty `spec.edge`.
fn edge_snapshot(rev: u64) -> Snapshot {
    let mut snap = snapshot(rev, Vec::new());
    snap.state.spec.network = None;
    snap.state.spec.edge = Some(SpecEdge::default());
    snap
}

/// Waits until `edge` applied `rev`.
async fn edge_reaches(edge: &EdgeManager, rev: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while edge.applied_rev() < rev {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the edge manager applied the revision");
}

#[tokio::test]
async fn an_edge_only_machine_records_the_revision_its_edge_applied() {
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let edge = edge_manager(&dir, &store);
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let mut reconciler = reconciler.with_edge(Arc::clone(&edge));

    // Before the edge manager applied the revision, it is not recorded.
    let first = edge_snapshot(3);
    reconciler.reconcile_once(Some(&first)).await;
    assert_eq!(store.applied_rev().await.unwrap(), 0);

    // Once the edge manager applied it, the next pass records it
    // (section 6.8: an edge-only machine runs apply steps 1 and 6 only).
    let (desired, desired_rx) = tokio::sync::watch::channel(Some(first.clone()));
    let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
    let edge_task = tokio::spawn(Arc::clone(&edge).run(desired_rx, async move {
        let _ = stop_rx.wait_for(|stopped| *stopped).await;
    }));
    edge_reaches(&edge, 3).await;
    reconciler.reconcile_once(Some(&first)).await;
    assert_eq!(store.applied_rev().await.unwrap(), 3);

    // A newer snapshot is recorded after the edge applied it too, and the
    // recorded revision never moves backwards.
    let second = edge_snapshot(5);
    desired.send_replace(Some(second.clone()));
    edge_reaches(&edge, 5).await;
    reconciler.reconcile_once(Some(&second)).await;
    assert_eq!(store.applied_rev().await.unwrap(), 5);
    reconciler.reconcile_once(Some(&first)).await;
    assert_eq!(store.applied_rev().await.unwrap(), 5);
    assert_eq!(engine.create_count(), 0, "an edge runs no VM");
    assert!(engine.networks().is_empty(), "an edge has no bridge");

    let _ = stop.send(true);
    edge_task.await.unwrap();
}

#[tokio::test]
async fn an_edge_only_agent_reports_each_new_revision_as_applied() {
    // The reconciler and the edge manager run as in the agent, on one
    // snapshot channel: every new revision becomes `applied_rev`.
    let dir = TempDir::new();
    let store = NodeStore::open(dir.path()).await.unwrap();
    let engine = Arc::new(RecordingEngine::new());
    let edge = edge_manager(&dir, &store);
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let reconciler = reconciler.with_edge(Arc::clone(&edge));

    let (desired, _) = tokio::sync::watch::channel(Some(edge_snapshot(8)));
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let wait_stop = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|stopped| *stopped).await;
    };
    let edge_task =
        tokio::spawn(Arc::clone(&edge).run(desired.subscribe(), wait_stop(stop_rx.clone())));
    let reconcile_task = tokio::spawn(reconciler.run(desired.subscribe(), wait_stop(stop_rx)));

    for rev in [8, 9, 12] {
        if rev != 8 {
            desired.send_replace(Some(edge_snapshot(rev)));
        }
        let recorded = tokio::time::timeout(Duration::from_secs(5), async {
            while store.applied_rev().await.unwrap() < rev {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            recorded.is_ok(),
            "applied_rev stayed {} for rev {rev}",
            store.applied_rev().await.unwrap()
        );
    }

    let _ = stop.send(true);
    reconcile_task.await.unwrap();
    edge_task.await.unwrap();
}

// --- volumes and stateful starts (M5a, sections 6.2, 6.4, 6.7) -------------

fn volume_in(statuses: &[VolumeStatus], volume_id: &str) -> Option<VolumeStatus> {
    statuses
        .iter()
        .find(|status| status.volume_id == volume_id)
        .cloned()
}

#[tokio::test]
async fn a_present_volume_is_created_and_reported_creating_then_ready() {
    let dir = TargetDir::new("volume-create");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let mut volumes = reconciler.volume_statuses();

    let snap = snapshot_full(
        1,
        Vec::new(),
        Vec::new(),
        vec![spec_volume("vol_1", "app_1", 1, SpecVolumeState::Present)],
    );
    let passing = tokio::spawn(async move { reconciler.reconcile_once(Some(&snap)).await });

    let mut seen = Vec::new();
    while !passing.is_finished() {
        if volumes.changed().await.is_ok() {
            seen.push(volumes.borrow_and_update().clone());
        }
    }
    assert!(passing.await.unwrap().is_empty());

    let states: Vec<VolumeState> = seen
        .iter()
        .filter_map(|statuses| volume_in(statuses, "vol_1").map(|status| status.state))
        .collect();
    assert!(
        states.contains(&VolumeState::Creating),
        "the volume is reported `creating` first, saw {states:?}"
    );
    assert_eq!(
        states.last(),
        Some(&VolumeState::Ready),
        "the last report is `ready`"
    );

    let volume = VolumeId("vol_1".to_owned());
    assert_eq!(
        engine.inner().volumes(),
        vec![(volume, 1)],
        "the engine holds the file"
    );
    assert!(
        dir.path().join("vol_1").exists(),
        "the file exists in the volume directory"
    );
    let row = store.volume("vol_1").await.unwrap().expect("a volume row");
    assert_eq!(row.state, VolumeState::Ready);
    assert_eq!(row.size_gb, 1);
    assert!(row.allocated_bytes.is_some());
}

#[tokio::test]
async fn a_stateful_vm_starts_only_with_a_held_lease_of_its_token() {
    let dir = TargetDir::new("volume-stateful");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    // The node holds `lse_1` with token 2.
    let mut reconciler = reconciler.with_lease_view(LeaseView::new(
        local_lease_table("lse_1", "app_1", 2),
        false,
    ));

    let snap = snapshot_full(
        1,
        vec![
            stateful_vm("vm_ok", "lse_1", 2, vec![volume_attach("vol_ok", "/data")]),
            stateful_vm(
                "vm_bad",
                "lse_1",
                1,
                vec![volume_attach("vol_bad", "/data")],
            ),
            spec_vm("vm_plain", "prj_1", DesiredStatus::Running),
        ],
        Vec::new(),
        vec![
            spec_volume("vol_ok", "app_1", 1, SpecVolumeState::Present),
            spec_volume("vol_bad", "app_1", 1, SpecVolumeState::Present),
        ],
    );
    let statuses = reconciler.reconcile_once(Some(&snap)).await;

    let status = |vm_id: &str| {
        statuses
            .iter()
            .find(|status| status.vm_id == vm_id)
            .cloned()
            .unwrap_or_else(|| panic!("a status for {vm_id}: {statuses:?}"))
    };
    assert_eq!(status("vm_ok").observed_status, ObservedStatus::Running);
    assert_eq!(status("vm_bad").observed_status, ObservedStatus::Stopped);
    assert_eq!(status("vm_bad").health, Health::Unknown);
    assert_eq!(status("vm_plain").observed_status, ObservedStatus::Running);

    // The VM with the wrong token was created but never started.
    let bad = VmId("vm_bad".to_owned());
    assert_eq!(engine.inspect(&bad).await.unwrap().state, VmState::Stopped);

    // The attached volumes reached the engine with their mount paths.
    let ok_spec = engine
        .created()
        .into_iter()
        .find(|spec| spec.id.0 == "vm_ok")
        .expect("vm_ok was created");
    assert_eq!(ok_spec.volumes.len(), 1);
    assert_eq!(ok_spec.volumes[0].volume_id.0, "vol_ok");
    assert_eq!(ok_spec.volumes[0].mount_path, "/data");
    assert_eq!(ok_spec.volumes[0].serial, "quoro-vol-0");
}

#[tokio::test]
async fn a_deleted_entry_removes_the_file_only_after_the_vm_is_gone() {
    let dir = TargetDir::new("volume-delete");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let mut reconciler = reconciler.with_lease_view(LeaseView::new(
        local_lease_table("lse_1", "app_1", 1),
        false,
    ));

    let running = snapshot_full(
        1,
        vec![stateful_vm(
            "vm_hold",
            "lse_1",
            1,
            vec![volume_attach("vol_gone", "/data")],
        )],
        Vec::new(),
        vec![spec_volume(
            "vol_gone",
            "app_1",
            1,
            SpecVolumeState::Present,
        )],
    );
    reconciler.reconcile_once(Some(&running)).await;
    let file = dir.path().join("vol_gone");
    std::fs::write(&file, b"keep").unwrap();

    // The primary names the volume `deleted` while the VM still attaches it:
    // the file stays.
    let deleted_while_attached = snapshot_full(
        2,
        running.state.spec.vms.clone(),
        Vec::new(),
        vec![spec_volume(
            "vol_gone",
            "app_1",
            1,
            SpecVolumeState::Deleted,
        )],
    );
    reconciler
        .reconcile_once(Some(&deleted_while_attached))
        .await;
    assert!(file.exists(), "an attached volume is never removed");

    // The VM leaves the spec in the same snapshot as the `deleted` entry.
    let deleted = snapshot_full(
        3,
        Vec::new(),
        Vec::new(),
        vec![spec_volume(
            "vol_gone",
            "app_1",
            1,
            SpecVolumeState::Deleted,
        )],
    );
    let statuses = reconciler.reconcile_once(Some(&deleted)).await;
    assert!(statuses.iter().any(
        |status| status.vm_id == "vm_hold" && status.observed_status == ObservedStatus::Deleted
    ));
    assert!(!file.exists(), "the file is removed once the VM is gone");
    let volumes = reconciler.volume_statuses().borrow().clone();
    assert_eq!(
        volume_in(&volumes, "vol_gone").map(|status| status.state),
        Some(VolumeState::Deleted)
    );

    // Once the primary stops naming it, the local row goes away.
    let forgotten = snapshot_full(4, Vec::new(), Vec::new(), Vec::new());
    reconciler.reconcile_once(Some(&forgotten)).await;
    assert_eq!(store.volume("vol_gone").await.unwrap(), None);
}

#[tokio::test]
async fn an_unknown_local_volume_is_orphaned_and_kept() {
    let dir = TargetDir::new("volume-orphan");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    engine
        .inner()
        .ensure_volume(&VolumeId("vol_orphan".to_owned()), 1)
        .await
        .expect("create the orphan file");
    store
        .save_volume(&VolumeRecord {
            volume_id: "vol_orphan".to_owned(),
            app_id: "app_1".to_owned(),
            size_gb: 1,
            state: VolumeState::Ready,
            error: None,
            allocated_bytes: Some(0),
        })
        .await
        .unwrap();

    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let snap = snapshot_full(1, Vec::new(), Vec::new(), Vec::new());
    reconciler.reconcile_once(Some(&snap)).await;

    let volumes = reconciler.volume_statuses().borrow().clone();
    assert_eq!(
        volume_in(&volumes, "vol_orphan").map(|status| status.state),
        Some(VolumeState::Orphaned)
    );
    assert!(
        engine
            .inner()
            .volumes()
            .iter()
            .any(|(volume, _)| volume.0 == "vol_orphan"),
        "an orphaned volume is never deleted"
    );
    assert!(dir.path().join("vol_orphan").exists());
    assert!(store.volume("vol_orphan").await.unwrap().is_some());
}

#[tokio::test]
async fn a_failed_ensure_volume_reports_failed_with_the_reason() {
    let dir = TargetDir::new("volume-failed");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    engine.inner().fail_next(
        FakeOp::EnsureVolume,
        EngineError::ResourceExhausted("the disk is full".to_owned()),
    );
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (mut reconciler, _observed) = reconciler(engine.clone(), store.clone());

    let snap = snapshot_full(
        1,
        Vec::new(),
        Vec::new(),
        vec![spec_volume("vol_big", "app_1", 1, SpecVolumeState::Present)],
    );
    reconciler.reconcile_once(Some(&snap)).await;

    let volumes = reconciler.volume_statuses().borrow().clone();
    let status = volume_in(&volumes, "vol_big").expect("a volume status");
    assert_eq!(status.state, VolumeState::Failed);
    let error = status.error.unwrap_or_default();
    assert!(
        error.contains("disk is full"),
        "the reason is reported: {error}"
    );
    assert!(!error.contains('\n'), "the error is one line");
    let row = store
        .volume("vol_big")
        .await
        .unwrap()
        .expect("a volume row");
    assert_eq!(row.state, VolumeState::Failed);
    assert!(row.error.is_some());
}

#[tokio::test]
async fn a_redeploy_swaps_the_vm_and_keeps_the_volume() {
    let dir = TargetDir::new("volume-redeploy");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let mut reconciler = reconciler.with_lease_view(LeaseView::new(
        local_lease_table("lse_1", "app_1", 1),
        false,
    ));

    let first = snapshot_full(
        1,
        vec![stateful_vm(
            "vm_a",
            "lse_1",
            1,
            vec![volume_attach("vol_data", "/data")],
        )],
        Vec::new(),
        vec![spec_volume(
            "vol_data",
            "app_1",
            1,
            SpecVolumeState::Present,
        )],
    );
    reconciler.reconcile_once(Some(&first)).await;
    let file = dir.path().join("vol_data");
    std::fs::write(&file, b"the app's data").unwrap();

    let second = snapshot_full(
        2,
        vec![stateful_vm(
            "vm_b",
            "lse_1",
            1,
            vec![volume_attach("vol_data", "/data")],
        )],
        Vec::new(),
        vec![spec_volume(
            "vol_data",
            "app_1",
            1,
            SpecVolumeState::Present,
        )],
    );
    let statuses = reconciler.reconcile_once(Some(&second)).await;

    let ids: Vec<&str> = statuses
        .iter()
        .filter(|status| status.observed_status != ObservedStatus::Deleted)
        .map(|status| status.vm_id.as_str())
        .collect();
    assert_eq!(ids, vec!["vm_b"], "only the new VM is reported");
    assert_eq!(
        engine
            .inspect(&VmId("vm_b".to_owned()))
            .await
            .unwrap()
            .state,
        VmState::Running,
        "the replacement started"
    );
    assert!(
        engine.inspect(&VmId("vm_a".to_owned())).await.is_err(),
        "the old VM was deleted before the new one was created"
    );
    assert_eq!(
        engine.inner().volumes(),
        vec![(VolumeId("vol_data".to_owned()), 1)],
        "the volume outlived the redeploy"
    );
    assert_eq!(
        std::fs::read(&file).unwrap(),
        b"the app's data",
        "the file content is unchanged"
    );
}

#[tokio::test]
async fn a_volume_being_restored_is_not_ready_and_no_vm_attaches_it() {
    let dir = TargetDir::new("volume-restoring");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let (reconciler, _observed) = reconciler(engine.clone(), store.clone());
    let mut reconciler = reconciler.with_lease_view(LeaseView::new(
        local_lease_table("lse_1", "app_1", 1),
        false,
    ));

    let mut volume = spec_volume("vol_data", "app_1", 1, SpecVolumeState::Present);
    volume.restore = Some(quoro_proto::wire::SpecRestore {
        backup_id: "bkp_1".to_owned(),
        manifest_key: "manifests/app_1/bkp_1".to_owned(),
        manifest_digest: "0".repeat(64),
        logical_bytes: 1 << 30,
        id_key: base64::engine::general_purpose::STANDARD.encode([0u8; 32]),
        data_key: base64::engine::general_purpose::STANDARD.encode([0u8; 32]),
    });
    let snap = snapshot_full(
        1,
        vec![stateful_vm(
            "vm_a",
            "lse_1",
            1,
            vec![volume_attach("vol_data", "/data")],
        )],
        Vec::new(),
        vec![volume],
    );

    // The backup manager owns the file; the reconciler only reports the row.
    reconciler.reconcile_once(Some(&snap)).await;
    let volumes = reconciler.volume_statuses().borrow().clone();
    assert_eq!(
        volume_in(&volumes, "vol_data").map(|status| status.state),
        Some(VolumeState::Restoring),
        "a restore in progress is reported `restoring`"
    );
    assert!(
        engine.list().await.unwrap().is_empty(),
        "no VM attaches a volume that is not ready"
    );
    assert!(
        !dir.path().join("vol_data").exists(),
        "the reconciler did not create the file"
    );

    // Once the manager records the file ready, the VM starts.
    engine
        .inner()
        .ensure_volume(&VolumeId("vol_data".to_owned()), 1)
        .await
        .unwrap();
    store
        .save_volume(&VolumeRecord {
            volume_id: "vol_data".to_owned(),
            app_id: "app_1".to_owned(),
            size_gb: 1,
            state: VolumeState::Ready,
            error: None,
            allocated_bytes: Some(0),
        })
        .await
        .unwrap();
    reconciler.reconcile_once(Some(&snap)).await;
    assert_eq!(
        engine
            .inspect(&VmId("vm_a".to_owned()))
            .await
            .unwrap()
            .state,
        VmState::Running,
        "the VM starts once the restore is ready"
    );
}

/// Section 8.4: while a lease is `uncertain` with the spec's token, a stateful
/// VM that already runs keeps running and one that stopped is not started
/// again (ADR 0072: a held lease restored after an agent restart comes back
/// `uncertain`).
#[tokio::test]
async fn an_uncertain_lease_keeps_a_running_vm_and_starts_nothing() {
    let dir = TargetDir::new("volume-uncertain");
    let engine = Arc::new(RecordingEngine::new());
    engine.inner().set_volume_dir(dir.path());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let snap = snapshot_full(
        1,
        vec![stateful_vm(
            "vm_warm",
            "lse_1",
            2,
            vec![volume_attach("vol_warm", "/data")],
        )],
        Vec::new(),
        vec![spec_volume(
            "vol_warm",
            "app_1",
            1,
            SpecVolumeState::Present,
        )],
    );

    // The lease is held: the VM starts.
    let (held, _observed) = reconciler(engine.clone(), store.clone());
    let mut held = held.with_lease_view(LeaseView::new(
        local_lease_table("lse_1", "app_1", 2),
        false,
    ));
    let statuses = held.reconcile_once(Some(&snap)).await;
    assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    drop(held);

    // The same lease, now `expiring`, restored `uncertain` with the same
    // token after an agent restart on the same boot.
    let clock = quoro_node_agent::ManualHostClock::new(
        Some("boot-a"),
        std::time::Duration::from_secs(1000),
    );
    let now = quoro_node_agent::HostNow::read(&clock);
    let stored = quoro_node_agent::StoredLease {
        lease_id: quoro_proto::LeaseId("lse_1".to_owned()),
        resource_type: quoro_proto::ResourceType::App,
        resource_id: "app_1".to_owned(),
        mode: quoro_proto::LeaseMode::Expiring,
        token: 2,
        state: quoro_node_agent::LeaseState::Held,
        duration: Some(std::time::Duration::from_millis(15_000)),
        validity: Some(quoro_node_agent::StoredValidity {
            boot_id: "boot-a".to_owned(),
            until_boottime: std::time::Duration::from_secs(1010),
        }),
    };
    let (table, fences) = quoro_node_agent::LeaseTable::restore(
        vec![stored],
        vec![((quoro_proto::ResourceType::App, "app_1".to_owned()), 2)],
        &std::collections::HashSet::new(),
        &now,
    );
    assert!(fences.is_empty(), "restored, not fenced");
    let view = LeaseView::new(table, false);
    assert!(!view.may_start_stateful("lse_1", 2));
    assert!(view.may_keep_running("lse_1", 2));
    let (uncertain, _observed) = reconciler(engine.clone(), store.clone());
    let mut uncertain = uncertain.with_lease_view(view);
    let vm = VmId("vm_warm".to_owned());
    let stops = engine.stops().len();
    for _ in 0..3 {
        let statuses = uncertain.reconcile_once(Some(&snap)).await;
        assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
    }
    assert_eq!(engine.stops().len(), stops, "the running VM is not stopped");
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Running);

    // It exits on its own: nothing starts it again while the lease is
    // uncertain.
    engine.inner().crash(&vm, "boom");
    for _ in 0..3 {
        let _ = uncertain.reconcile_once(Some(&snap)).await;
    }
    assert_eq!(engine.inspect(&vm).await.unwrap().state, VmState::Crashed);
}

/// D-0224: a restart that finds a fence to run again and no verified
/// snapshot stops every app VM. Once the first desired state verifies, the
/// VMs of other held leases and of stateless apps start again; the fenced
/// lease's VM stays stopped.
#[tokio::test]
async fn vms_swept_by_a_fence_without_a_snapshot_start_again_with_the_first_snapshot() {
    let dir = TargetDir::new("sweep-restart");
    let engine = Arc::new(RecordingEngine::new());
    let store = NodeStore::open(dir.path()).await.unwrap();
    let snap = snapshot(
        1,
        vec![
            spec_vm("vm_web", "prj_1", DesiredStatus::Running),
            stateful_vm("vm_held", "lse_held", 1, Vec::new()),
            stateful_vm("vm_lost", "lse_lost", 1, Vec::new()),
        ],
    );

    // Everything runs while both leases are held.
    let mut table = local_lease_table("lse_held", "app_held", 1);
    table
        .on_grant(
            &quoro_proto::LeaseGrant {
                lease_id: quoro_proto::LeaseId("lse_lost".to_owned()),
                resource_type: quoro_proto::ResourceType::App,
                resource_id: "app_lost".to_owned(),
                mode: quoro_proto::LeaseMode::Local,
                fencing_token: 1,
                duration_ms: None,
                granted_at: "2026-11-02T10:15:33Z".to_owned(),
            },
            None,
            tokio::time::Instant::now(),
        )
        .expect("a local grant");
    let (first, _observed) = reconciler(engine.clone(), store.clone());
    let mut first = first.with_lease_view(LeaseView::new(table, false));
    first.reconcile_once(Some(&snap)).await;
    let ids = ["vm_web", "vm_held", "vm_lost"].map(|id| VmId(id.to_owned()));
    for id in &ids {
        assert_eq!(engine.inspect(id).await.unwrap().state, VmState::Running);
    }
    drop(first);

    // The agent died mid-fence of `lse_lost`: it is stored `revoked` without
    // a fence record, and `lse_held` is stored `held`.
    for (lease, app, state) in [
        ("lse_held", "app_held", quoro_node_agent::LeaseState::Held),
        (
            "lse_lost",
            "app_lost",
            quoro_node_agent::LeaseState::Revoked,
        ),
    ] {
        store
            .save_lease(
                &quoro_node_agent::StoredLease {
                    lease_id: quoro_proto::LeaseId(lease.to_owned()),
                    resource_type: quoro_proto::ResourceType::App,
                    resource_id: app.to_owned(),
                    mode: quoro_proto::LeaseMode::Local,
                    token: 1,
                    state,
                    duration: None,
                    validity: None,
                },
                1,
            )
            .await
            .unwrap();
    }

    // The restart verifies no snapshot: the fence stops every app VM.
    let (desired, desired_rx) = tokio::sync::watch::channel(None);
    let leases = quoro_node_agent::start_local_leases(
        engine.clone(),
        store.clone(),
        quoro_node_agent::LeaseConfig {
            node_id: "nod_test".to_owned(),
            timers: quoro_node_agent::FenceTimers {
                grace: Duration::from_millis(20),
                force: Duration::from_millis(20),
            },
        },
        desired_rx,
    )
    .await
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while store.fences().await.unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the fence is recorded"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for id in &ids {
        assert_eq!(engine.inspect(id).await.unwrap().state, VmState::Stopped);
    }

    // The first desired state verifies.
    desired.send_replace(Some(snap.clone()));
    let (second, _observed) = reconciler(engine.clone(), store.clone());
    let mut second = second.with_lease_view(leases.view.clone());
    second.reconcile_once(Some(&snap)).await;
    assert_eq!(
        engine.inspect(&ids[0]).await.unwrap().state,
        VmState::Running,
        "the stateless VM starts again"
    );
    assert_eq!(
        engine.inspect(&ids[1]).await.unwrap().state,
        VmState::Running,
        "the VM of the held lease starts again"
    );
    assert_eq!(
        engine.inspect(&ids[2]).await.unwrap().state,
        VmState::Stopped,
        "the fenced lease's VM stays stopped"
    );
    leases.abort();
}
