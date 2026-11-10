// SPDX-License-Identifier: AGPL-3.0-only
//! The reconciler (`node-agent-protocol.md` sections 6.1-6.3 and 6.5).
//!
//! It turns one verified snapshot into engine calls: ensure the network,
//! create missing VMs, start what should run, stop what should not, remove VMs
//! that left the spec, and publish what it observes. It runs whether or not a
//! session exists, because the node must keep its workloads running without the
//! control plane (D-0011).
//!
//! Two rules shape the loop:
//!
//! - With no snapshot it does nothing at all. A fresh agent must never delete
//!   VMs before it knows what the cluster wants.
//! - An engine error on one VM is logged and never stops the loop or the other
//!   VMs; the pass simply does not record `applied_rev`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures_util::StreamExt as _;
use quoro_engine::{
    ConsoleMode, Engine, EngineError, EngineEvent, EventStream, ImageConfig, ImageDigest, ImageRef,
    InitConfig, MacAddr, NetAttach, NodeNetwork, SerialMode, StopMode, VmId, VmInfo, VmSpec,
    VmState, VolumeAttach, VolumeId, VsockAttach,
};
use quoro_proto::wire::{
    DesiredState, DesiredStatus, Health, ObservedStatus, Spec, SpecRoute, SpecVm, SpecVolumeFile,
    SpecVolumeState, VmStatus, VolumeState, VolumeStatus, WarmRole,
};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::health::{
    DEFAULT_RESTART, FIRST_PROBE_DELAY, HealthTarget, PausedVms, is_passing, probe,
};
use crate::lease::{LeaseView, StartPermit};
use crate::logs::LogCollector;
use crate::store::{NodeStore, VolumeRecord};
use crate::util;
use crate::warm::CopyAsBase;
use crate::{FORCE_WITHIN, START_WITHIN};

/// The guest context id of every VM's vsock device: each VM has its own vsock
/// namespace, so all of them use 3 (`engine-trait.md` section 3.18).
const VSOCK_GUEST_CID: u32 = 3;

/// The verified snapshot the reconciler acts on.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Monotonic revision of the whole snapshot.
    pub rev: u64,
    /// The parsed body, whose signature was already verified.
    pub state: DesiredState,
}

/// The node-side network properties the reconciler fills in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileConfig {
    /// The node bridge (for example `quoro0`).
    pub bridge: String,
    /// Guest DNS servers, from the node's own configuration.
    pub dns: Vec<Ipv4Addr>,
}

/// Timings and backoff bounds; [`Default`] is the contract's set and tests
/// shorten the backoff to keep the loop deterministic.
#[derive(Clone, Debug)]
pub struct ReconcileOptions {
    /// How often a pass runs when nothing else wakes it (2 s).
    pub tick: Duration,
    /// Graceful stop timeout for a desired stop or a restart (10 s).
    pub stop_timeout: Duration,
    /// First backoff of a crash restart, and of the next try after a start
    /// that failed (1 s).
    pub backoff_base: Duration,
    /// Ceiling of that backoff (5 min).
    pub backoff_max: Duration,
    /// How long a VM must run before its restart count resets (10 min).
    pub healthy_after: Duration,
}

impl Default for ReconcileOptions {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(2),
            stop_timeout: Duration::from_secs(10),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(300),
            healthy_after: Duration::from_secs(600),
        }
    }
}

/// The parts of a network's identity, used to decide whether to call
/// `ensure_network` again. `dns` is not identity for the engine, but a change
/// must reach the guests created afterwards, so it is compared here too.
///
/// `bridge` is the candidate the engine accepted, not part of the desired
/// network: a changed subnet moves new VMs to a second bridge held here
/// (`node-agent-protocol.md` section 6.9).
#[derive(Clone, Debug, PartialEq, Eq)]
struct NetworkState {
    bridge: String,
    subnet: String,
    gateway: Ipv4Addr,
    dns: Vec<Ipv4Addr>,
}

impl NetworkState {
    /// Whether `other` asks for the same network. The bridge is left out: it is
    /// whatever candidate the engine accepted for this subnet.
    fn same_network(&self, other: &Self) -> bool {
        self.subnet == other.subnet && self.gateway == other.gateway && self.dns == other.dns
    }
}

/// The longest Linux interface name, in bytes.
const MAX_BRIDGE_NAME: usize = 15;

/// The bridges to try, in order: the configured one and its numbered
/// successors (`quoro0`, `quoro0s1`, `quoro0s2`, `quoro0s3`).
///
/// A name Linux cannot take is skipped. On an upgraded node the configured
/// bridge is already recorded with the old subnet, so the first candidate that
/// the engine accepts is a second bridge (`node-agent-protocol.md` section 6.9).
fn bridge_candidates(configured: &str) -> Vec<String> {
    let mut candidates = Vec::with_capacity(4);
    candidates.push(configured.to_owned());
    for n in 1..=3 {
        candidates.push(format!("{configured}s{n}"));
    }
    candidates
        .into_iter()
        .filter(|name| name.len() <= MAX_BRIDGE_NAME)
        .collect()
}

/// Per-VM timing state. It does not survive an agent restart by design: a fresh
/// agent starts a stopped desired VM without calling it a crash.
#[derive(Clone, Debug, Default)]
struct VmTiming {
    retry_at: Option<Instant>,
    backoff: Duration,
    running_since: Option<Instant>,
}

/// The app and deployment a desired VM belongs to, from the current snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SpecMeta {
    app_id: Option<String>,
    deployment_id: Option<String>,
}

/// The ids of the volumes `vm` attaches. A backup of one of them pauses the
/// VM on a `pause_copy` node, whatever VM the backup job names (#524).
fn volume_ids(vm: &SpecVm) -> impl Iterator<Item = &str> {
    vm.volumes.iter().map(|volume| volume.volume_id.as_str())
}

/// What a health check needs from the VM's spec beyond the probe target: where
/// a transition to failing or passing is reported.
#[derive(Clone, Debug)]
struct HealthTargetInfo {
    target: HealthTarget,
    deployment_id: Option<String>,
    app_id: Option<String>,
    project_id: String,
    /// The volumes the VM attaches, whose backups may pause it (D-0259).
    volumes: Vec<String>,
}

/// One VM's health-check state.
///
/// It lives while the VM is desired running with a port, across the stop and
/// the start of a health restart, so a check that went `failing` reports
/// `app.up` once it passes after the restart (section 6.3).
#[derive(Clone, Debug)]
struct HealthState {
    target: HealthTarget,
    status: Health,
    failures: u32,
    next_at: Instant,
    /// The engine's start time of the run the state follows. Another one is
    /// a new start, which begins the start period again.
    started_at: Option<SystemTime>,
    /// When this process first saw the run. The start period counts from
    /// here when the engine has no start time or the clock went back.
    seen_at: Instant,
    /// The VM stopped since the run was seen, so its next run is a new start
    /// even when the engine reports no start time.
    run_ended: bool,
    /// A health restart was already asked for this run.
    restart_asked: bool,
    /// The VM's [`crate::health::PauseMark::latest`] after its last probe.
    /// Another value means it was paused since, so its failures count from
    /// zero and a restart asked before the pause is dropped (D-0259).
    latest_pause_seen: u64,
}

impl HealthState {
    fn new(target: HealthTarget, started_at: Option<SystemTime>, now: Instant) -> Self {
        Self {
            target,
            status: Health::Unknown,
            failures: 0,
            next_at: now + FIRST_PROBE_DELAY,
            started_at,
            seen_at: now,
            run_ended: false,
            restart_asked: false,
            latest_pause_seen: 0,
        }
    }

    /// A new start of the VM: the failures and the start period begin again
    /// (section 6.3). The status stays, so a check that was `failing` before
    /// a health restart is still the same incident, closed by `app.up` when
    /// it passes.
    fn begin_run(&mut self, started_at: Option<SystemTime>, now: Instant) {
        self.failures = 0;
        self.next_at = now + FIRST_PROBE_DELAY;
        self.started_at = started_at;
        self.seen_at = now;
        self.run_ended = false;
        self.restart_asked = false;
    }

    /// Whether the run is still in its start period, where failed probes do
    /// not count (section 6.3). It counts from the engine's start time, so a
    /// VM that was already running when this process started is not given a
    /// new start period.
    fn in_start_period(&self, now: Instant) -> bool {
        let since_seen = now.duration_since(self.seen_at);
        let since_start = self
            .started_at
            .and_then(|started| SystemTime::now().duration_since(started).ok())
            .unwrap_or(since_seen);
        since_start < self.target.start_period
    }
}

/// Whether a VM whose health check keeps failing is restarted, or only
/// reported (section 6.3). A snapshot without `restart` means true.
fn restarts_when_unhealthy(vm: &SpecVm) -> bool {
    vm.health_check
        .as_ref()
        .and_then(|check| check.restart)
        .unwrap_or(DEFAULT_RESTART)
}

/// A parsed IPv4 subnet.
///
/// The build manager allocates its builder VM's address from the same subnet,
/// from the top down ([`Subnet::allocate_high`]). The reconciler only records
/// an app VM's address once the VM is created, so a builder starting while an
/// app VM is being created cannot see that address yet; taking the highest
/// free address keeps the two apart unless the subnet is almost full, where
/// the engine refuses the duplicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Subnet {
    network: u32,
    prefix: u8,
    pub(crate) gateway: Ipv4Addr,
}

impl Subnet {
    pub(crate) fn parse(cidr: &str, gateway: &str) -> Option<Self> {
        let (address, prefix) = cidr.split_once('/')?;
        let address: Ipv4Addr = address.parse().ok()?;
        let prefix: u8 = prefix.parse().ok()?;
        if prefix > 32 {
            return None;
        }
        let host_bits = 32 - u32::from(prefix);
        let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
        Some(Self {
            network: u32::from(address) & mask,
            prefix,
            gateway: gateway.parse().ok()?,
        })
    }

    fn broadcast(&self) -> u32 {
        let host_bits = 32 - u32::from(self.prefix.min(32));
        let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
        self.network | !mask
    }

    /// Whether `ip` is a usable guest address of the subnet: not the network,
    /// not the gateway and not the broadcast address.
    fn contains(&self, ip: Ipv4Addr) -> bool {
        let raw = u32::from(ip);
        raw > self.network && raw < self.broadcast() && ip != self.gateway
    }

    /// The lowest usable address that is not in `used`.
    ///
    /// When the subnet is exhausted the highest address is returned again; the
    /// engine then refuses the duplicate address and the create error is logged
    /// like any other, rather than taking the node down.
    pub(crate) fn allocate(&self, used: &HashSet<Ipv4Addr>) -> Ipv4Addr {
        let broadcast = self.broadcast();
        for raw in self.network.saturating_add(1)..broadcast {
            let ip = Ipv4Addr::from(raw);
            if ip != self.gateway && !used.contains(&ip) {
                return ip;
            }
        }
        Ipv4Addr::from(broadcast.saturating_sub(1))
    }

    /// The highest usable address that is not in `used`; like
    /// [`Subnet::allocate`], the highest address again when the subnet is
    /// exhausted.
    pub(crate) fn allocate_high(&self, used: &HashSet<Ipv4Addr>) -> Ipv4Addr {
        let broadcast = self.broadcast();
        for raw in (self.network.saturating_add(1)..broadcast).rev() {
            let ip = Ipv4Addr::from(raw);
            if ip != self.gateway && !used.contains(&ip) {
                return ip;
            }
        }
        Ipv4Addr::from(broadcast.saturating_sub(1))
    }
}

/// The reconciler. One instance owns the engine handle, the store and the
/// observation channel.
pub struct Reconciler<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    config: ReconcileConfig,
    options: ReconcileOptions,
    observed: watch::Sender<Vec<VmStatus>>,
    network: Option<NetworkState>,
    network_failed: bool,
    timings: HashMap<String, VmTiming>,
    last_states: HashMap<String, VmState>,
    once_only: Vec<VmStatus>,
    published: Option<Vec<VmStatus>>,
    collector: LogCollector<E>,
    node_id: Option<String>,
    spec_meta: HashMap<String, SpecMeta>,
    health: HashMap<String, HealthState>,
    /// The VMs whose health check failed past its threshold outside the
    /// start period: the next pass stops them so the crash path starts them
    /// again (section 6.3).
    health_restarts: HashSet<String>,
    /// The VMs that exited on their own or were stopped by a health restart
    /// and wait for the crash path to start them again. It keeps a VM on that
    /// path, its backoff doubling and its restarts counted, through the
    /// passes that wait out the backoff, also when the engine reads the VM
    /// `stopped` rather than `crashed`.
    restarts_due: HashSet<String>,
    /// The VMs a backup paused, shared with the backup manager: their failed
    /// probes do not count and they are not restarted for their health
    /// (D-0259).
    paused: PausedVms,
    /// Per VM, the requested digest last refused because the VM's digest is
    /// create-time only: the refusal is logged once per VM and digest, not on
    /// every pass (issue #354).
    refused_digests: HashMap<String, String>,
    /// The app-route supervisor, when the agent runs one (worker hook for
    /// `M2` apps). `None` in tests that only exercise the engine.
    ingress: Option<Arc<crate::ingress::IngressSupervisor>>,
    /// The leases the node holds, shared with the lease service (section 6.4).
    lease: LeaseView,
    /// The volume statuses the session reports (section 5.3).
    volumes_out: watch::Sender<Vec<VolumeStatus>>,
    /// The volumes that are `ready` on this node in the current pass.
    ready_volumes: HashSet<String>,
    /// The image-inventory transport, when the agent runs one (section 5.26).
    inventory: Option<crate::inventory::InventoryHub>,
    /// The edge manager, which applies `spec.edge` (section 6.8). A snapshot
    /// with `spec.edge` counts as applied only once the manager has applied
    /// that revision. `None` in tests that only exercise the engine.
    edge: Option<Arc<crate::edge::EdgeManager>>,
    /// The warm volumes' locks, shared with the warm receiver: the file of a
    /// volume this node holds a warm copy of is created, adopted or removed
    /// here only between two streams (section 6.13).
    warm_locks: Option<crate::warm::VolumeLocks>,
    /// The own volumes whose file the last pass kept on a `deleted` entry
    /// because the node is returning as their warm target (section 9), so
    /// that is logged once.
    kept_returning: HashSet<String>,
    /// The `standby` VMs of the current snapshot (sections 5.11 and 6.4,
    /// ADR 0174): never created or started here, and reported `stopped`.
    standby: BTreeSet<String>,
    /// Per `standby` VM, what the node last did with its image.
    standby_images: HashMap<String, StandbyImage>,
    /// The `standby` ids the engine holds a VM under, warned about once.
    standby_held: BTreeSet<String>,
}

/// How long a failed preparation of a `standby` VM's image waits before the
/// node tries again: a warm copy's image is not urgent, and a registry that
/// refuses it should not be asked on every pass.
const STANDBY_RETRY: Duration = Duration::from_secs(60);

/// What the node did with the image of one `standby` VM (section 6.2 step 2).
#[derive(Clone, Debug)]
struct StandbyImage {
    /// The digest this outcome is for. A new digest starts over.
    digest: String,
    outcome: StandbyOutcome,
}

#[derive(Clone, Copy, Debug)]
enum StandbyOutcome {
    /// Prepared, so a failover does not wait for a pull.
    Prepared,
    /// A build's output, which a node never pulls (section 6.6).
    Built,
    /// The preparation failed at this time; tried again after
    /// [`STANDBY_RETRY`].
    Failed(Instant),
}

impl StandbyOutcome {
    /// What this outcome, for the digest the snapshot names, leaves to a pass
    /// at `now`: `Some(reached)` when there is nothing to do, `None` when the
    /// image is to be prepared again.
    fn settled(self, now: Instant) -> Option<bool> {
        match self {
            Self::Prepared | Self::Built => Some(true),
            Self::Failed(at) if now.saturating_duration_since(at) < STANDBY_RETRY => Some(false),
            Self::Failed(_) => None,
        }
    }
}

impl<E: Engine + 'static> Reconciler<E> {
    /// Builds a reconciler and the channel it publishes observations on.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        config: ReconcileConfig,
        options: ReconcileOptions,
    ) -> (Self, watch::Receiver<Vec<VmStatus>>) {
        let (observed, receiver) = watch::channel(Vec::new());
        let (volumes_out, _) = watch::channel(Vec::new());
        let collector = LogCollector::new(Arc::clone(&engine), store.clone());
        (
            Self {
                engine,
                store,
                config,
                options,
                observed,
                network: None,
                network_failed: false,
                timings: HashMap::new(),
                last_states: HashMap::new(),
                refused_digests: HashMap::new(),
                once_only: Vec::new(),
                published: None,
                collector,
                node_id: None,
                spec_meta: HashMap::new(),
                health: HashMap::new(),
                health_restarts: HashSet::new(),
                restarts_due: HashSet::new(),
                paused: PausedVms::default(),
                ingress: None,
                lease: LeaseView::default(),
                volumes_out,
                ready_volumes: HashSet::new(),
                inventory: None,
                edge: None,
                warm_locks: None,
                kept_returning: HashSet::new(),
                standby: BTreeSet::new(),
                standby_images: HashMap::new(),
                standby_held: BTreeSet::new(),
            },
            receiver,
        )
    }

    /// Attaches the lease table the lease service fills: a stateful VM may
    /// start only through it (section 6.4).
    #[must_use]
    pub fn with_lease_view(mut self, lease: LeaseView) -> Self {
        self.lease = lease;
        self
    }

    /// The volume statuses this reconciler publishes (section 5.3), which the
    /// session reports.
    #[must_use]
    pub fn volume_statuses(&self) -> watch::Receiver<Vec<VolumeStatus>> {
        self.volumes_out.subscribe()
    }

    /// Attaches the app-route supervisor: every applied snapshot and every
    /// change of a VM's address feeds it the complete route set.
    #[must_use]
    pub fn with_ingress(mut self, ingress: Arc<crate::ingress::IngressSupervisor>) -> Self {
        self.ingress = Some(ingress);
        self
    }

    /// Attaches the edge manager: a snapshot with `spec.edge` is recorded as
    /// applied only after the manager applied that revision (section 6.8).
    #[must_use]
    pub fn with_edge(mut self, edge: Arc<crate::edge::EdgeManager>) -> Self {
        self.edge = Some(edge);
        self
    }

    /// Shares the pause marks the backup manager sets on the VMs a
    /// `pause_copy` snapshot pauses (D-0259).
    #[must_use]
    pub fn with_paused_vms(mut self, paused: PausedVms) -> Self {
        self.paused = paused;
        self
    }

    /// Sets the `nod_` id node-originated events carry. Without it those
    /// events are not emitted.
    #[must_use]
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Shares the warm volumes' locks with the warm receiver (section
    /// 6.13).
    #[must_use]
    pub fn with_warm_locks(mut self, locks: crate::warm::VolumeLocks) -> Self {
        self.warm_locks = Some(locks);
        self
    }

    /// Attaches the inventory transport: every image this reconciler prepares
    /// queues its inventory for the session (section 5.26).
    #[must_use]
    pub fn with_inventory(mut self, inventory: crate::inventory::InventoryHub) -> Self {
        self.inventory = Some(inventory);
        self
    }

    /// Runs passes until `shutdown` resolves: immediately, when the snapshot
    /// changes, on an engine crash or exit, and every `tick`.
    pub async fn run(
        mut self,
        mut snapshots: watch::Receiver<Option<Snapshot>>,
        shutdown: impl Future<Output = ()> + Send,
    ) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut events = self.engine.events();
        let mut events_open = true;
        let mut ticker = tokio::time::interval(self.options.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut health_tick = tokio::time::interval(Duration::from_millis(500));
        health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut current = snapshots.borrow_and_update().clone();

        loop {
            tokio::select! {
                () = &mut shutdown => break,
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    current = snapshots.borrow_and_update().clone();
                    self.reconcile_once(current.as_ref()).await;
                }
                event = async {
                    if events_open {
                        next_event(&mut events).await
                    } else {
                        std::future::pending::<Option<EngineEvent>>().await
                    }
                } => {
                    match event {
                        Some(EngineEvent::Crashed { id, reason }) => {
                            warn!(vm_id = %id.0, %reason, "the VM crashed; reconciling now");
                            self.reconcile_once(current.as_ref()).await;
                        }
                        Some(EngineEvent::Exited { id, code }) => {
                            debug!(vm_id = %id.0, code, "the VM exited; reconciling now");
                            self.reconcile_once(current.as_ref()).await;
                        }
                        Some(EngineEvent::Started { .. }) => {}
                        None => events_open = false,
                    }
                }
                _ = ticker.tick() => {
                    self.reconcile_once(current.as_ref()).await;
                }
                _ = health_tick.tick() => {
                    // A health restart was just asked for: a pass stops the
                    // VM now rather than at the next tick. One that a pass
                    // could not carry out yet waits for the tick.
                    if self.health_pass(current.as_ref()).await {
                        self.reconcile_once(current.as_ref()).await;
                    } else {
                        self.publish().await;
                    }
                }
            }
        }
        info!("reconciler stopped");
    }

    /// One full pass. Returns the observed set it published.
    ///
    /// `None` means the agent has no verified snapshot yet: it does nothing at
    /// all, in particular it deletes nothing.
    pub async fn reconcile_once(&mut self, snapshot: Option<&Snapshot>) -> Vec<VmStatus> {
        let Some(snapshot) = snapshot else {
            return self.published.clone().unwrap_or_default();
        };
        let spec = &snapshot.state.spec;
        self.spec_meta = spec
            .vms
            .iter()
            .map(|vm| {
                (
                    vm.vm_id.clone(),
                    SpecMeta {
                        app_id: vm.app_id.clone(),
                        deployment_id: vm.deployment_id.clone(),
                    },
                )
            })
            .collect();
        self.standby = spec
            .vms
            .iter()
            .filter(|vm| vm.standby)
            .map(|vm| vm.vm_id.clone())
            .collect();
        let standby = &self.standby;
        self.standby_images
            .retain(|vm_id, _| standby.contains(vm_id));
        self.standby_held.retain(|vm_id| standby.contains(vm_id));

        // A machine without the `node` role (an edge-only machine) gets no
        // `spec.network` and runs no VM (section 5.11). Its apply loop runs
        // only the network and route steps (section 6.8): the edge manager
        // and the mesh manager apply them, so the revision counts as applied
        // once the edge manager has it.
        let Some(spec_network) = spec.network.as_ref() else {
            if self.edge_applied(snapshot) {
                self.record_applied(snapshot.rev).await;
            }
            return self.publish().await;
        };
        let Some(subnet) = Subnet::parse(&spec_network.subnet, &spec_network.gateway) else {
            warn!(
                subnet = %spec_network.subnet,
                gateway = %spec_network.gateway,
                "the desired network is not a usable subnet; nothing is applied this pass"
            );
            return self.publish().await;
        };

        let network = NetworkState {
            bridge: self.config.bridge.clone(),
            subnet: spec_network.subnet.clone(),
            gateway: subnet.gateway,
            dns: self.config.dns.clone(),
        };
        if self.network_failed
            || self
                .network
                .as_ref()
                .is_none_or(|current| !current.same_network(&network))
        {
            self.ensure_network(&network).await;
        }
        let network_ok = !self.network_failed && self.network.is_some();

        let mut reached = true;

        // Step 3 (section 6.2): stop and delete the VMs that left the spec
        // before anything is created. On a redeploy the old VM and its
        // replacement attach the same volume in one snapshot, and the engine
        // refuses to create a VM while another existing VM attaches that
        // volume, so the removal must come first. A `standby` entry is in the
        // spec, so a VM the engine has under its id is not removed here.
        let desired: HashSet<&str> = spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        if !self.remove_extra_vms(&desired).await {
            reached = false;
        }

        // Step 4, first half (section 6.2): volumes, before any VM starts.
        if !self.reconcile_volumes(snapshot.rev, spec).await {
            reached = false;
        }

        // A `standby` VM is never created or started, whatever lease the node
        // holds (sections 5.11 and 6.4, ADR 0174).
        let (mut standby, mut vms): (Vec<&SpecVm>, Vec<&SpecVm>) =
            spec.vms.iter().partition(|vm| vm.standby);
        vms.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));
        standby.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));

        for vm in vms {
            if !self.reconcile_vm(vm, &subnet, network_ok).await {
                reached = false;
            }
        }

        // --- ingress hook (M2 apps): step 5 "Routes" of section 6.2 ---
        // The complete route set goes to the supervisor on every pass, so a
        // VM that got or changed its address (for example after a VMM restart)
        // is a backend from the next pass on. The call never blocks: the
        // supervisor keeps the newest set.
        if let Some(ingress) = self.ingress.clone() {
            let addresses = self.ingress_addresses(&spec.routes).await;
            ingress.update(&spec.routes, spec.ingress.as_ref(), addresses);
        }
        // --- end ingress hook ---

        // Step 2 for the `standby` VMs (section 6.2): their image is prepared
        // like any other, so a failover does not wait for a pull. It comes
        // after the VMs that run and their routes, so a pull for a warm copy
        // delays neither.
        for vm in standby {
            if !self.prepare_standby(vm).await {
                reached = false;
            }
            if !self.standby_absent(&vm.vm_id).await {
                reached = false;
            }
        }

        if reached && self.edge_applied(snapshot) {
            self.record_applied(snapshot.rev).await;
        }

        self.health_pass(Some(snapshot)).await;
        self.publish().await
    }

    /// Whether the edge part of `snapshot` is applied: always when the
    /// snapshot has no `spec.edge` or no edge manager is attached, otherwise
    /// once the manager has applied this revision or a newer one.
    fn edge_applied(&self, snapshot: &Snapshot) -> bool {
        match (&snapshot.state.spec.edge, &self.edge) {
            (Some(_), Some(edge)) => edge.applied_rev() >= snapshot.rev,
            _ => true,
        }
    }

    /// Records `rev` as the newest fully reconciled revision, never moving
    /// it backwards.
    async fn record_applied(&self, rev: u64) {
        if rev > self.store.applied_rev().await.unwrap_or(0)
            && let Err(e) = self.store.set_applied_rev(rev).await
        {
            warn!(error = %e, "could not record the applied revision");
        }
    }

    /// Ensures the desired node network, walking the bridge candidates until
    /// one is accepted.
    ///
    /// A bridge already recorded with another subnet is a `Conflict` and is
    /// never reconfigured (`engine-trait.md` section 3.3), so a node whose
    /// subnet changed takes the next candidate name instead
    /// (`node-agent-protocol.md` section 6.9). The old bridge stays without
    /// VMs, and VMs still on it keep their address until they are redeployed.
    /// Any other engine error stops the walk: retrying another name would not
    /// help.
    async fn ensure_network(&mut self, desired: &NetworkState) -> bool {
        for (index, bridge) in bridge_candidates(&self.config.bridge)
            .into_iter()
            .enumerate()
        {
            let request = NodeNetwork {
                bridge: bridge.clone(),
                subnet: desired.subnet.clone(),
                gateway: desired.gateway,
                dns: desired.dns.clone(),
            };
            match self.engine.ensure_network(&request).await {
                Ok(()) => {
                    info!(
                        bridge = %request.bridge,
                        subnet = %request.subnet,
                        "node network ensured"
                    );
                    if index > 0 {
                        info!(
                            bridge = %request.bridge,
                            previous_bridge = %self.config.bridge,
                            "running VMs stay on their previous bridge until they are redeployed"
                        );
                    }
                    self.network = Some(NetworkState {
                        bridge,
                        subnet: desired.subnet.clone(),
                        gateway: desired.gateway,
                        dns: desired.dns.clone(),
                    });
                    self.network_failed = false;
                    return true;
                }
                Err(EngineError::Conflict(_)) => {
                    debug!(
                        bridge = %request.bridge,
                        "the bridge is recorded with another network; trying the next candidate"
                    );
                }
                Err(e) => {
                    warn!(error = ?e, "could not ensure the node network; no VM is created this pass");
                    self.network_failed = true;
                    return false;
                }
            }
        }
        warn!(
            subnet = %desired.subnet,
            "every bridge candidate is recorded with another network; no VM is created this pass"
        );
        self.network_failed = true;
        false
    }

    /// The observed address of every VM a route lists, for the ingress
    /// supervisor. A VM with no recorded address is absent; its route is
    /// retried on the next pass.
    async fn ingress_addresses(&self, routes: &[SpecRoute]) -> HashMap<String, Ipv4Addr> {
        let mut addresses = HashMap::new();
        let mut checked: HashSet<&str> = HashSet::new();
        for route in routes {
            for vm_id in &route.vm_ids {
                if !checked.insert(vm_id.as_str()) {
                    continue;
                }
                if let Ok(Some(record)) = self.store.vm_record(vm_id).await
                    && let Some(ip) = record.ip
                {
                    addresses.insert(vm_id.clone(), ip);
                }
            }
        }
        addresses
    }

    /// Creates, removes and reports the volume files of `spec.volumes`
    /// (section 6.2 step 4, section 6.7).
    ///
    /// A file `spec.volumes` does not name is `orphaned` and never removed
    /// here; only a `deleted` entry removes one. Returns `false` while some
    /// volume work is still pending, so `applied_rev` is not recorded.
    async fn reconcile_volumes(&mut self, rev: u64, spec: &Spec) -> bool {
        let stored = match self.store.volumes().await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "could not read the volume rows");
                return false;
            }
        };
        let rows: HashMap<String, VolumeRecord> = stored
            .into_iter()
            .map(|row| (row.volume_id.clone(), row))
            .collect();
        let warm_copies: HashSet<String> = match self.store.warm_target_summaries().await {
            Ok(targets) => targets.into_iter().map(|row| row.volume_id).collect(),
            Err(e) => {
                warn!(error = %e, "could not read the warm targets");
                return false;
            }
        };
        // The warm copies this node holds as a target are the warm
        // receiver's (section 6.13): it creates the file only from a complete
        // stream, reports it in `status_report.warm` and removes it on a
        // `deleted` entry under the volume's lock, so no stream is cut.
        // A `deleted` entry is the receiver's when it names the target or
        // this node holds a warm copy of it, unless the node has a volume row
        // of its own for it, in any state: that file is this pass's to remove,
        // and its `deleted` is reported until the entry leaves the spec.
        let is_warm_copy = |entry: &SpecVolumeFile| {
            let target = entry
                .warm
                .as_ref()
                .is_some_and(|warm| warm.role == WarmRole::Target);
            match entry.state {
                SpecVolumeState::Present => target,
                SpecVolumeState::Deleted => {
                    let own = rows.contains_key(&entry.volume_id);
                    !own && (target || warm_copies.contains(&entry.volume_id))
                }
            }
        };

        // Start from what each named volume last reported, plus one entry per
        // orphan, which is what this pass begins with.
        let mut statuses: BTreeMap<String, VolumeStatus> = BTreeMap::new();
        for entry in spec.volumes.iter().filter(|entry| !is_warm_copy(entry)) {
            statuses.insert(
                entry.volume_id.clone(),
                match rows.get(&entry.volume_id) {
                    Some(row) => VolumeStatus {
                        volume_id: row.volume_id.clone(),
                        state: row.state,
                        allocated_bytes: row.allocated_bytes,
                        error: row.error.clone(),
                    },
                    None => VolumeStatus {
                        volume_id: entry.volume_id.clone(),
                        // A fresh restore is `restoring` until the backup
                        // manager records otherwise (section 6.7).
                        state: if entry.restore.is_some() {
                            VolumeState::Restoring
                        } else {
                            VolumeState::Creating
                        },
                        allocated_bytes: None,
                        error: None,
                    },
                },
            );
        }
        for row in rows.values() {
            if row.state == VolumeState::Deleted
                || spec
                    .volumes
                    .iter()
                    .any(|entry| entry.volume_id == row.volume_id)
            {
                continue;
            }
            statuses.insert(
                row.volume_id.clone(),
                VolumeStatus {
                    volume_id: row.volume_id.clone(),
                    state: VolumeState::Orphaned,
                    allocated_bytes: row.allocated_bytes,
                    error: None,
                },
            );
        }
        self.publish_volumes(statuses.values().cloned().collect());
        self.ready_volumes.clear();

        let attached: HashSet<&str> = spec
            .vms
            .iter()
            .flat_map(|vm| vm.volumes.iter().map(|volume| volume.volume_id.as_str()))
            .collect();
        let mut reached = true;
        // The returning sources' files this pass kept on a `deleted` entry.
        let mut kept_returning: HashSet<String> = HashSet::new();

        for entry in spec.volumes.iter().filter(|entry| !is_warm_copy(entry)) {
            let row = rows.get(&entry.volume_id);
            // The node runs its own volume again: a hand-over to the warm
            // receiver that marked the row and did not finish (section 9) is
            // cleared under the volume's lock, which the hand-over holds from
            // its comparison to its end, and so is the returning flag, which
            // keeps the file on a `deleted` entry while the node is named its
            // warm target as a returning source. Almost always neither is set.
            let own_present = entry.state == SpecVolumeState::Present && row.is_some();
            let (marked, returning) = if own_present {
                let read = async {
                    Ok::<_, crate::AgentError>((
                        self.store.volume_handing_over(&entry.volume_id).await?,
                        self.store.volume_return_pending(&entry.volume_id).await?,
                    ))
                };
                match read.await {
                    Ok(flags) => flags,
                    Err(e) => {
                        warn!(volume_id = %entry.volume_id, error = %e, "could not read the volume row");
                        reached = false;
                        continue;
                    }
                }
            } else {
                (false, false)
            };
            // A volume this node holds a warm copy of (it turns from target
            // into source), or a marked or flagged one: its file is adopted,
            // removed or cleared only while no stream or hand-over holds it; a
            // busy one is retried on the next pass.
            let warm_lock = match &self.warm_locks {
                Some(locks) if marked || returning || warm_copies.contains(&entry.volume_id) => {
                    match locks.try_lock(&entry.volume_id) {
                        Some(guard) => Some(guard),
                        None => {
                            debug!(
                                volume_id = %entry.volume_id,
                                "a warm stream holds the volume; retrying on the next pass"
                            );
                            reached = false;
                            continue;
                        }
                    }
                }
                _ => None,
            };
            match entry.state {
                SpecVolumeState::Present => {
                    // The node runs the volume again before its hand-over
                    // finished: the mark goes before the volume is ready, so
                    // before any VM may write to the file, and a later
                    // hand-over compares the file again (section 9). Read
                    // again under the lock: a hand-over may have marked the
                    // row since the read above.
                    if own_present && (marked || returning || warm_lock.is_some()) {
                        // A returning source (section 9): only a revision
                        // newer than the one that named the node the target
                        // decides that it runs the volume again. This pass
                        // may run on an older snapshot than the warm
                        // receiver's; then it touches neither the flag nor
                        // the mark, and the volume waits for a newer one.
                        let named = match self
                            .store
                            .volume_return_pending_rev(&entry.volume_id)
                            .await
                        {
                            Ok(named) => named,
                            Err(e) => {
                                warn!(volume_id = %entry.volume_id, error = %e, "could not read the volume row");
                                reached = false;
                                continue;
                            }
                        };
                        if named.is_some_and(|named| rev <= named) {
                            debug!(
                                volume_id = %entry.volume_id,
                                rev,
                                "a newer revision named the node the volume's warm target; waiting for a newer one"
                            );
                            reached = false;
                            continue;
                        }
                        let cleared = match self.store.volume_handing_over(&entry.volume_id).await {
                            Ok(false) => Ok(false),
                            Ok(true) => self
                                .store
                                .clear_volume_handing_over(&entry.volume_id)
                                .await
                                .map(|()| true),
                            Err(e) => Err(e),
                        };
                        match cleared {
                            Ok(false) => {}
                            Ok(true) => info!(
                                volume_id = %entry.volume_id,
                                "the node runs its volume again; its unfinished hand-over is cleared"
                            ),
                            Err(e) => {
                                warn!(volume_id = %entry.volume_id, error = %e, "could not clear an unfinished hand-over");
                                reached = false;
                                continue;
                            }
                        }
                        // The node is no longer returning: a later `deleted`
                        // entry removes the file as for any own volume, and
                        // the warm row the hand-over never ended goes, so the
                        // sender ships the volume.
                        if named.is_some() {
                            match self.store.end_return_pending(&entry.volume_id, rev).await {
                                Ok(true) => info!(
                                    volume_id = %entry.volume_id,
                                    "the node runs its volume again; it is no longer returning as the warm target"
                                ),
                                Ok(false) => {
                                    reached = false;
                                    continue;
                                }
                                Err(e) => {
                                    warn!(volume_id = %entry.volume_id, error = %e, "could not clear the returning flag");
                                    reached = false;
                                    continue;
                                }
                            }
                        }
                        // A warm row beside the own row that holds no copy
                        // (a stale stream's record, or a target row written
                        // while the warm node was made the source) would keep
                        // the sender from shipping the volume for good: the
                        // store row goes, never a file. One that holds a copy
                        // stays, as before. Not beside a `deleted` own row,
                        // which holds no file: there the row keeps the guard
                        // against starting the app on an empty volume. A
                        // failed forget only delays the sender, never the VM.
                        let forget = if row.is_some_and(|row| row.state != VolumeState::Deleted) {
                            self.store
                                .forget_copyless_warm_target(&entry.volume_id)
                                .await
                        } else {
                            Ok(false)
                        };
                        match forget {
                            Ok(false) => {}
                            Ok(true) => info!(
                                volume_id = %entry.volume_id,
                                "forgot a warm target row without a copy beside the node's own volume"
                            ),
                            Err(e) => {
                                warn!(volume_id = %entry.volume_id, error = %e, "could not forget a warm target row without a copy");
                                reached = false;
                            }
                        }
                    }
                    if entry.restore.is_some() {
                        // The backup manager writes the file and records its
                        // state (`backup-format.md` section 8.1); this pass only
                        // reports that row and gates the VM on it, so it never
                        // writes over a restore in progress.
                        match row {
                            Some(row)
                                if row.state == VolumeState::Ready
                                    && row.size_gb == entry.size_gb =>
                            {
                                self.ready_volumes.insert(entry.volume_id.clone());
                            }
                            Some(row) if row.state == VolumeState::Failed => {}
                            _ => reached = false,
                        }
                        continue;
                    }
                    if row.is_some_and(|row| {
                        row.state == VolumeState::Ready && row.size_gb == entry.size_gb
                    }) {
                        self.ready_volumes.insert(entry.volume_id.clone());
                        continue;
                    }
                    // A warm node that a failover or a switchover made the
                    // source: its copy becomes its volume, and is kept as
                    // the base first, before the volume is ready and so
                    // before any VM writes to it (section 6.13, "Failover").
                    if warm_copies.contains(&entry.volume_id) {
                        match crate::warm::keep_copy_as_base(
                            &*self.engine,
                            &self.store,
                            &entry.volume_id,
                        )
                        .await
                        {
                            CopyAsBase::Kept | CopyAsBase::NoCopyRow => {}
                            CopyAsBase::Resolving => {
                                debug!(
                                    volume_id = %entry.volume_id,
                                    "the warm copy is still being resolved; retrying on the next pass"
                                );
                                reached = false;
                                continue;
                            }
                            CopyAsBase::Failed(reason) => {
                                warn!(
                                    volume_id = %entry.volume_id,
                                    error = %reason,
                                    "could not keep the warm copy as the base"
                                );
                                self.report_unadopted_copy(
                                    &mut statuses,
                                    entry,
                                    &format!("could not keep the warm copy as the base: {reason}"),
                                );
                                reached = false;
                                continue;
                            }
                            CopyAsBase::NoCompleteCopy => {
                                // No file is created: an empty volume would
                                // start the app with none of its data.
                                self.report_unadopted_copy(
                                    &mut statuses,
                                    entry,
                                    "this node holds no complete warm copy of the volume to start the app on",
                                );
                                reached = false;
                                continue;
                            }
                        }
                    }
                    self.record_volume(
                        &mut statuses,
                        entry,
                        VolumeState::Creating,
                        None,
                        row.and_then(|row| row.allocated_bytes),
                    )
                    .await;
                    let id = VolumeId(entry.volume_id.clone());
                    match self.engine.ensure_volume(&id, entry.size_gb).await {
                        Ok(info) => {
                            self.record_volume(
                                &mut statuses,
                                entry,
                                VolumeState::Ready,
                                None,
                                Some(info.allocated_bytes),
                            )
                            .await;
                            self.ready_volumes.insert(entry.volume_id.clone());
                        }
                        Err(e) => {
                            warn!(
                                volume_id = %entry.volume_id,
                                error = ?e,
                                "could not create the volume"
                            );
                            reached = false;
                            self.record_volume(
                                &mut statuses,
                                entry,
                                VolumeState::Failed,
                                Some(e.to_string()),
                                None,
                            )
                            .await;
                        }
                    }
                }
                SpecVolumeState::Deleted => {
                    if row.is_some_and(|row| row.state == VolumeState::Deleted) {
                        continue;
                    }
                    // Section 6.7: remove the file only once no VM of the spec
                    // attaches it; the engine also refuses while a VM it knows
                    // attaches it.
                    if attached.contains(entry.volume_id.as_str()) {
                        continue;
                    }
                    // A returning source's own file (section 9): a `deleted`
                    // entry reaches it only through the stale-copy rule,
                    // which never removes a volume of the node's own
                    // (section 5.11, ADR 0267). Read fresh, not from the rows
                    // read at the start: the warm receiver may have forgotten
                    // its warm row since. The row keeps its state, and is
                    // reported `orphaned` once the entry leaves the spec.
                    match self.store.volume_return_pending(&entry.volume_id).await {
                        Ok(false) => {}
                        Ok(true) => {
                            if self.kept_returning.insert(entry.volume_id.clone()) {
                                info!(
                                    volume_id = %entry.volume_id,
                                    "kept the file of a `deleted` entry: it is a volume of the node's own, named its warm target as a returning source"
                                );
                            }
                            kept_returning.insert(entry.volume_id.clone());
                            continue;
                        }
                        Err(e) => {
                            warn!(volume_id = %entry.volume_id, error = %e, "could not read the volume row");
                            reached = false;
                            continue;
                        }
                    }
                    match self
                        .engine
                        .delete_volume(&VolumeId(entry.volume_id.clone()))
                        .await
                    {
                        Ok(()) => {
                            self.record_volume(
                                &mut statuses,
                                entry,
                                VolumeState::Deleted,
                                None,
                                None,
                            )
                            .await;
                        }
                        Err(EngineError::Conflict(_)) => {
                            debug!(
                                volume_id = %entry.volume_id,
                                "a VM still attaches the volume; retrying on the next pass"
                            );
                            reached = false;
                        }
                        Err(e) => {
                            warn!(
                                volume_id = %entry.volume_id,
                                error = ?e,
                                "could not delete the volume"
                            );
                            reached = false;
                        }
                    }
                }
            }
        }

        // Logged once while the entry keeps the file; again for a later one.
        self.kept_returning = kept_returning;

        // A `deleted` row the primary stopped naming is forgotten; every other
        // row it does not name stays as an orphan (section 6.7).
        for row in rows.values() {
            if row.state != VolumeState::Deleted
                || spec
                    .volumes
                    .iter()
                    .any(|entry| entry.volume_id == row.volume_id)
            {
                continue;
            }
            if let Err(e) = self.store.delete_volume(&row.volume_id).await {
                warn!(volume_id = %row.volume_id, error = %e, "could not forget the volume");
            }
        }

        self.publish_volumes(statuses.values().cloned().collect());
        reached
    }

    /// Records one volume state in the local table and in the published set.
    async fn record_volume(
        &self,
        statuses: &mut BTreeMap<String, VolumeStatus>,
        entry: &SpecVolumeFile,
        state: VolumeState,
        error: Option<String>,
        allocated_bytes: Option<u64>,
    ) {
        let error = error.map(|error| truncate_error(&error));
        statuses.insert(
            entry.volume_id.clone(),
            VolumeStatus {
                volume_id: entry.volume_id.clone(),
                state,
                allocated_bytes,
                error: error.clone(),
            },
        );
        if let Err(e) = self
            .store
            .save_volume(&VolumeRecord {
                volume_id: entry.volume_id.clone(),
                app_id: entry.app_id.clone(),
                size_gb: entry.size_gb,
                state,
                error,
                allocated_bytes,
            })
            .await
        {
            warn!(volume_id = %entry.volume_id, error = %e, "could not record the volume");
        }
        self.publish_volumes(statuses.values().cloned().collect());
    }

    /// Reports `entry` `failed` with `error` and writes no row, so the warm
    /// copy stays the warm receiver's until the node adopts it (section
    /// 6.13, "Failover").
    fn report_unadopted_copy(
        &self,
        statuses: &mut BTreeMap<String, VolumeStatus>,
        entry: &SpecVolumeFile,
        error: &str,
    ) {
        statuses.insert(
            entry.volume_id.clone(),
            VolumeStatus {
                volume_id: entry.volume_id.clone(),
                state: VolumeState::Failed,
                allocated_bytes: None,
                error: Some(truncate_error(error)),
            },
        );
        self.publish_volumes(statuses.values().cloned().collect());
    }

    fn publish_volumes(&self, mut statuses: Vec<VolumeStatus>) {
        statuses.sort_by(|a, b| a.volume_id.cmp(&b.volume_id));
        self.volumes_out.send_replace(statuses);
    }

    /// Makes one desired VM match its spec. Returns `false` when it has not
    /// reached the target yet, so `applied_rev` is not recorded.
    async fn reconcile_vm(&mut self, vm: &SpecVm, subnet: &Subnet, network_ok: bool) -> bool {
        let vm_id = vm.vm_id.clone();
        let id = VmId(vm_id.clone());
        let mut info = match self.engine.inspect(&id).await {
            Ok(info) => Some(info),
            Err(EngineError::NotFound) => None,
            Err(e) => {
                warn!(vm_id, error = ?e, "could not inspect the VM");
                return false;
            }
        };

        // Section 6.2 step 3: a changed attached-volume set needs a recreate.
        let signature = volumes_signature(&vm.volumes);
        let recorded = self.store.meta(&volumes_key(&vm_id)).await.ok().flatten();
        if info.is_some()
            && (recorded.is_some() || !vm.volumes.is_empty())
            && recorded.as_deref() != Some(signature.as_str())
        {
            info!(vm_id, "the VM's attached volumes changed; recreating it");
            if self.fence_owns_stop(vm) {
                debug!(
                    vm_id,
                    "a fence of the VM's lease is stopping it; recreating it later"
                );
                return false;
            }
            if let Some(current) = &info
                && matches!(
                    current.state,
                    VmState::Running | VmState::Starting | VmState::Stopping
                )
            {
                match self
                    .unless_fenced(
                        vm,
                        self.engine.stop(
                            &id,
                            StopMode::Force {
                                within: FORCE_WITHIN,
                            },
                        ),
                    )
                    .await
                {
                    None => {
                        debug!(vm_id, "a fence of the VM's lease took over its stop");
                        return false;
                    }
                    Some(Err(e)) => {
                        warn!(vm_id, error = ?e, "could not stop the VM for a recreate");
                        return false;
                    }
                    Some(Ok(_)) => {}
                }
            }
            match self.engine.delete(&id).await {
                Ok(()) => {
                    if let Err(e) = self.store.forget_vm(&vm_id).await {
                        warn!(vm_id, error = %e, "could not forget the recreated VM");
                    }
                    self.last_states.remove(&vm_id);
                    self.timings.remove(&vm_id);
                    self.health.remove(&vm_id);
                    self.health_restarts.remove(&vm_id);
                    self.restarts_due.remove(&vm_id);
                    self.refused_digests.remove(&vm_id);
                    info = None;
                }
                Err(e) => {
                    warn!(vm_id, error = ?e, "could not delete the VM for a recreate");
                    return false;
                }
            }
        }

        let volumes_ready = vm
            .volumes
            .iter()
            .all(|volume| self.ready_volumes.contains(&volume.volume_id));
        let lease_allowed = !vm.stateful
            || matches!(
                (&vm.lease_id, vm.fencing_token),
                (Some(lease_id), Some(token)) if self.lease.may_start_stateful(lease_id, token)
            );
        let may_start = volumes_ready && lease_allowed;
        // Section 8.4: an `uncertain` lease keeps the VMs that already run and
        // starts nothing new, so a running stateful VM is stopped only when its
        // lease no longer stands behind it.
        let may_keep_running = volumes_ready
            && (!vm.stateful
                || matches!(
                    (&vm.lease_id, vm.fencing_token),
                    (Some(lease_id), Some(token)) if self.lease.may_keep_running(lease_id, token)
                ));

        if info.is_none() {
            if !volumes_ready {
                debug!(vm_id, "not creating the VM before its volume is ready");
                return false;
            }
            if !network_ok {
                debug!(vm_id, "not creating the VM before the network is ensured");
                return false;
            }
            match self.create_vm(vm, subnet).await {
                Some(created) => info = Some(created),
                None => return false,
            }
        }
        let Some(mut info) = info else {
            return false;
        };
        let mut reached = true;

        // Section 6.4: a stateful VM starts only while its lease is `held`
        // with exactly the spec's token, and a VM whose volume is not `ready`
        // stays stopped. Neither is an error: the primary moved the lease or
        // the volume work is still running.
        if vm.desired_status == DesiredStatus::Running && !may_start {
            // A fence of the lease stops the VM itself (section 8.5 step 1):
            // checked after `may_keep_running`, so a lease that just left
            // `held` is seen fencing.
            if matches!(
                info.state,
                VmState::Running | VmState::Starting | VmState::Stopping
            ) && !may_keep_running
            {
                match self.unless_fenced(vm, self.stop_vm(&id)).await {
                    None => debug!(vm_id, "a fence of the VM's lease is stopping it"),
                    Some(Ok(stopped)) => info = stopped,
                    Some(Err(e)) => {
                        warn!(vm_id, error = ?e, "could not stop a VM that may not start");
                        return false;
                    }
                }
            }
            debug!(
                vm_id,
                stateful = vm.stateful,
                volumes_ready,
                lease_allowed,
                may_keep_running,
                "the VM waits to start, or keeps running without restarts"
            );
            // A health restart waits until the VM may start again; the next
            // counted failure asks for it again.
            self.drop_health_restart(&vm_id);
            self.remember(&vm_id, info.state);
            return true;
        }

        // Section 6.3 (ADR 0184): a VM whose health check failed past its
        // threshold outside the start period is stopped here and started
        // again below on the crash path, with its backoff. It is stopped only
        // when it may start again: a stateful VM under a lease that is not
        // `held` keeps running (section 8.4) and waits, since a stop could
        // not be followed by a start. A lease that leaves `held` between the
        // stop and the start leaves it stopped (section 6.4). A fence of the
        // lease stops the VM itself (section 8.5), so a VM it is stopping is
        // left to it. A VM a backup paused is not restarted for its health
        // (D-0259): not while it is paused, nor for failures from before a
        // pause that began after the probe that asked for the restart.
        if self.health_restarts.contains(&vm_id) {
            let mark = self.paused.mark_of(volume_ids(vm));
            let (still_failing, paused_since) =
                self.health.get(&vm_id).map_or((false, false), |state| {
                    (
                        state.status == Health::Failing
                            && HealthTarget::for_vm(vm, state.target.ip)
                                .is_some_and(|target| target.probes_like(&state.target)),
                        mark.held > 0 || mark.latest != state.latest_pause_seen,
                    )
                });
            if vm.desired_status != DesiredStatus::Running
                || info.state != VmState::Running
                || !restarts_when_unhealthy(vm)
                || !still_failing
                || paused_since
            {
                self.drop_health_restart(&vm_id);
            } else if self.fence_owns_stop(vm) {
                self.drop_health_restart(&vm_id);
                debug!(
                    vm_id,
                    "a fence of the VM's lease is stopping it; no health restart"
                );
            } else {
                match self.unless_fenced(vm, self.stop_vm(&id)).await {
                    None => {
                        self.drop_health_restart(&vm_id);
                        debug!(
                            vm_id,
                            "a fence of the VM's lease took over its stop; no health restart"
                        );
                        return false;
                    }
                    Some(Ok(stopped)) => {
                        self.health_restarts.remove(&vm_id);
                        self.restarts_due.insert(vm_id.clone());
                        if let Some(state) = self.health.get_mut(&vm_id) {
                            state.run_ended = true;
                        }
                        warn!(
                            vm_id,
                            "stopped a VM whose health check kept failing; it starts again after the backoff"
                        );
                        self.deployment_line(
                            vm,
                            "health",
                            "VM stopped after its health check kept failing; it starts again after the restart backoff",
                        )
                        .await;
                        info = stopped;
                    }
                    Some(Err(e)) => {
                        // Given up until the VM starts again: the request
                        // goes and the run's `restart_asked` stays. A stop
                        // that failed after its graceful and forced attempts
                        // would hold every pass for its whole bound if it
                        // were tried again on each.
                        self.health_restarts.remove(&vm_id);
                        warn!(
                            vm_id,
                            error = ?e,
                            "could not stop a VM whose health check kept failing; no health restart until it starts again"
                        );
                        self.deployment_line(
                            vm,
                            "health",
                            &format!(
                                "could not stop the VM for a health restart: {e}; it is not restarted for its health until it starts again"
                            ),
                        )
                        .await;
                        return false;
                    }
                }
            }
        }

        if let Some(known) = self.known_digest(&vm_id).await
            && !known.is_empty()
            && known != vm.image_digest
        {
            if first_refusal(&mut self.refused_digests, &vm_id, &vm.image_digest) {
                warn!(
                    vm_id,
                    known, requested = %vm.image_digest,
                    "a VM's image digest is create-time only; deploy a new VM id to change it"
                );
            }
        } else {
            self.refused_digests.remove(&vm_id);
        }

        let applied_generation = self
            .store
            .vm_record(&vm_id)
            .await
            .ok()
            .flatten()
            .map_or(0, |record| record.restart_generation);

        if vm.restart_generation > applied_generation && may_start {
            if matches!(info.state, VmState::Running | VmState::Starting) {
                match self.unless_fenced(vm, self.stop_vm(&id)).await {
                    None => {
                        debug!(vm_id, "a fence of the VM's lease took over its stop");
                        return false;
                    }
                    Some(Ok(stopped)) => info = stopped,
                    Some(Err(e)) => {
                        warn!(vm_id, error = ?e, "could not stop the VM for a restart");
                        return false;
                    }
                }
            }
            if let Err(e) = self
                .store
                .set_restart_generation(&vm_id, vm.restart_generation)
                .await
            {
                warn!(vm_id, error = %e, "could not record the restart generation");
            }
            // An operator's restart is tried at once, whatever a failed start,
            // a crash or a health restart left to wait out (#462).
            self.clear_backoff(&vm_id);
            self.health_restarts.remove(&vm_id);
            self.restarts_due.remove(&vm_id);
            if vm.desired_status == DesiredStatus::Running {
                let Some(permit) = self.start_permit(vm).await else {
                    debug!(vm_id, "the lease was lost before the restart; the VM waits");
                    self.remember(&vm_id, info.state);
                    return true;
                };
                let started = self.engine.start(&id, start_within(&permit)).await;
                // A fence of the lease may stop the VM from here on.
                drop(permit);
                match started {
                    Ok(started) => {
                        info!(vm_id, "VM restarted for a new restart_generation");
                        self.deployment_line(vm, "boot", "VM started").await;
                        info = started;
                    }
                    Err(e) => {
                        warn!(vm_id, error = ?e, "could not start the VM after its restart");
                        self.back_off(&vm_id, Instant::now());
                        let error = e.to_string();
                        self.deployment_line(vm, "boot", &format!("VM start failed: {error}"))
                            .await;
                        self.deployment_failed(vm, "boot", &error).await;
                        reached = false;
                    }
                }
            }
            self.remember(&vm_id, info.state);
            return reached;
        }

        let previous = self.last_states.get(&vm_id).copied();
        let was_running = matches!(previous, Some(VmState::Running | VmState::Starting));

        match vm.desired_status {
            DesiredStatus::Running => {
                if !matches!(info.state, VmState::Running | VmState::Starting) {
                    // A `Crashed` state is a crash even when this process did
                    // not see the VM running yet; a `Stopped` VM is only a
                    // crash if it had been running and the agent asked for no
                    // stop.
                    let unexpected_exit = matches!(info.state, VmState::Crashed)
                        || ((was_running || self.restarts_due.contains(&vm_id))
                            && info.state == VmState::Stopped);
                    if unexpected_exit {
                        self.restarts_due.insert(vm_id.clone());
                    }
                    // Section 6.4 at the moment of the start: the lease may
                    // have been lost since `may_start` was read, and a fence
                    // must not overlap the start (section 8.5).
                    let Some(permit) = self.start_permit(vm).await else {
                        debug!(vm_id, "the lease was lost before the start; the VM waits");
                        self.remember(&vm_id, info.state);
                        return true;
                    };
                    if unexpected_exit {
                        // The engine's start bound counts from the call, and
                        // the permit must cover no more than that bound
                        // (section 6.4, ADR 0242): a store write that waits
                        // must not sit between the lease check and the start.
                        // So the permit is taken again after it.
                        let permit = if let Some(code) = info.exit_code {
                            drop(permit);
                            if let Err(e) = self
                                .store
                                .set_exit_code(&vm_id, Some(i64::from(code)))
                                .await
                            {
                                warn!(vm_id, error = %e, "could not record the exit code");
                            }
                            let Some(permit) = self.start_permit(vm).await else {
                                debug!(vm_id, "the lease was lost before the start; the VM waits");
                                self.remember(&vm_id, info.state);
                                return true;
                            };
                            permit
                        } else {
                            permit
                        };
                        match self
                            .crash_restart(&vm_id, &id, info.exit_code, permit)
                            .await
                        {
                            Ok(Some(started)) => {
                                self.restarts_due.remove(&vm_id);
                                info = started;
                            }
                            Ok(None) => reached = false,
                            Err(e) => {
                                warn!(vm_id, error = ?e, "could not restart the crashed VM");
                                reached = false;
                            }
                        }
                    } else if self.start_backing_off(&vm_id) {
                        // #462: a start that failed, a `Timeout` included,
                        // waits out the crash-restart backoff before the next
                        // try, so a boot slower than its bound does not loop.
                        drop(permit);
                        debug!(vm_id, "waiting out the backoff after a failed start");
                        reached = false;
                    } else {
                        let started = self.engine.start(&id, start_within(&permit)).await;
                        // A fence of the lease may stop the VM from here on.
                        drop(permit);
                        match started {
                            Ok(started) => {
                                debug!(vm_id, "VM started");
                                self.clear_backoff(&vm_id);
                                self.deployment_line(vm, "boot", "VM started").await;
                                info = started;
                            }
                            Err(e) => {
                                warn!(vm_id, error = ?e, "could not start the VM");
                                self.back_off(&vm_id, Instant::now());
                                let error = e.to_string();
                                self.deployment_line(
                                    vm,
                                    "boot",
                                    &format!("VM start failed: {error}"),
                                )
                                .await;
                                self.deployment_failed(vm, "boot", &error).await;
                                reached = false;
                            }
                        }
                    }
                }
            }
            DesiredStatus::Stopped => {
                if matches!(
                    info.state,
                    VmState::Running | VmState::Starting | VmState::Stopping
                ) {
                    match self.unless_fenced(vm, self.stop_vm(&id)).await {
                        None => {
                            debug!(vm_id, "a fence of the VM's lease is stopping it");
                            reached = false;
                        }
                        Some(Ok(stopped)) => {
                            if let Some(code) = stopped.exit_code
                                && let Err(e) = self
                                    .store
                                    .set_exit_code(&vm_id, Some(i64::from(code)))
                                    .await
                            {
                                warn!(vm_id, error = %e, "could not record the exit code");
                            }
                            info = stopped;
                        }
                        Some(Err(e)) => {
                            warn!(vm_id, error = ?e, "could not stop the VM");
                            reached = false;
                        }
                    }
                }
                if let Some(timing) = self.timings.get_mut(&vm_id) {
                    timing.retry_at = None;
                }
                self.health_restarts.remove(&vm_id);
                self.restarts_due.remove(&vm_id);
            }
        }

        if info.state == VmState::Running {
            // Running again, however it got there: no crash restart is due.
            self.restarts_due.remove(&vm_id);
            let now = Instant::now();
            let healthy_after = self.options.healthy_after;
            let base = self.options.backoff_base;
            // Section 6.3: the backoff resets after the VM has been healthy
            // for `healthy_after`; while its health check fails it is not.
            let failing = self
                .health
                .get(&vm_id)
                .is_some_and(|state| state.status == Health::Failing);
            let timing = self.timings.entry(vm_id.clone()).or_insert(VmTiming {
                retry_at: None,
                backoff: base,
                running_since: None,
            });
            match timing.running_since {
                None => timing.running_since = Some(now),
                Some(_) if failing => timing.running_since = Some(now),
                Some(since) if now.duration_since(since) >= healthy_after => {
                    timing.running_since = Some(now);
                    timing.backoff = base;
                    timing.retry_at = None;
                    if let Err(e) = self.store.reset_restarts(&vm_id).await {
                        warn!(vm_id, error = %e, "could not reset the restart count");
                    }
                }
                Some(_) => {}
            }
        } else if let Some(timing) = self.timings.get_mut(&vm_id) {
            timing.running_since = None;
        }

        self.remember(&vm_id, info.state);
        reached
    }

    /// Prepares the image of a `standby` VM and creates nothing else
    /// (section 5.11, ADR 0174). Returns whether the image is as the
    /// snapshot wants it: prepared, or a build's output, which the node never
    /// pulls (section 6.6). A failed preparation is tried again after
    /// [`STANDBY_RETRY`].
    async fn prepare_standby(&mut self, vm: &SpecVm) -> bool {
        let vm_id = vm.vm_id.as_str();
        let earlier = self
            .standby_images
            .get(vm_id)
            .filter(|done| done.digest == vm.image_digest)
            .map(|done| done.outcome);
        if let Some(reached) = earlier.and_then(|earlier| earlier.settled(Instant::now())) {
            return reached;
        }
        let outcome = if vm.build_id.is_some() {
            info!(
                vm_id,
                image = %vm.image,
                "a standby VM's image is a build's output, which is never pulled; it is not prepared here"
            );
            StandbyOutcome::Built
        } else {
            let image = ImageRef {
                reference: vm.image.clone(),
                digest: ImageDigest(vm.image_digest.clone()),
            };
            match self.engine.prepare_image(&image).await {
                Ok(prepared) => {
                    if let Some(hub) = &self.inventory {
                        hub.notify_prepared(prepared.digest.clone());
                    }
                    info!(vm_id, digest = %prepared.digest.0, "prepared a standby VM's image");
                    StandbyOutcome::Prepared
                }
                Err(e) => {
                    if earlier.is_some() {
                        debug!(vm_id, image = %vm.image, error = ?e, "still cannot prepare a standby VM's image");
                    } else {
                        warn!(vm_id, image = %vm.image, error = ?e, "could not prepare a standby VM's image");
                    }
                    StandbyOutcome::Failed(Instant::now())
                }
            }
        };
        self.standby_images.insert(
            vm_id.to_owned(),
            StandbyImage {
                digest: vm.image_digest.clone(),
                outcome,
            },
        );
        !matches!(outcome, StandbyOutcome::Failed(_))
    }

    /// Whether the engine holds no VM under a `standby` id. A VM it does hold
    /// is this node's old VM of the app, from when the node was its source:
    /// section 5.11 deletes it while keeping its volume, which the warm
    /// receiver then hands over (section 9). It is deleted once the engine
    /// reports it `Stopped` or `Crashed`. In any other state it holds the
    /// revision back: one that runs, starts or stops (a paused VM reads
    /// `Running`), one being created, and one whose state the engine cannot
    /// read, whose VMM may still run a guest. The revoke or the fence stops
    /// it, and nothing here stops a VM.
    async fn standby_absent(&mut self, vm_id: &str) -> bool {
        let id = VmId(vm_id.to_owned());
        let info = match self.engine.inspect(&id).await {
            Err(EngineError::NotFound) => {
                self.standby_held.remove(vm_id);
                return true;
            }
            Ok(info) => info,
            Err(e) => {
                debug!(vm_id, error = ?e, "could not inspect a standby VM");
                return false;
            }
        };
        if !matches!(info.state, VmState::Stopped | VmState::Crashed) {
            if self.standby_held.insert(vm_id.to_owned()) {
                warn!(
                    vm_id,
                    state = ?info.state,
                    "the engine holds the node's old VM under a standby id and it is not stopped; it is deleted once stopped, and the revision is not applied until then"
                );
            }
            return false;
        }
        match self.engine.delete(&id).await {
            // Gone only once the engine no longer knows it: until then it
            // may still hold the volume.
            Ok(()) => match self.engine.inspect(&id).await {
                Err(EngineError::NotFound) => {
                    self.forget_deleted_vm(vm_id).await;
                    self.standby_held.remove(vm_id);
                    info!(
                        vm_id,
                        state = ?info.state,
                        "deleted the node's old VM under a standby id; its volume stays"
                    );
                    true
                }
                still => {
                    let state = still.as_ref().ok().map(|info| info.state);
                    if self.standby_held.insert(vm_id.to_owned()) {
                        warn!(vm_id, ?state, error = ?still.err(), "the engine still holds the node's old VM under a standby id after its delete; the revision is not applied");
                    } else {
                        debug!(
                            vm_id,
                            ?state,
                            "the engine still holds the node's old VM under a standby id"
                        );
                    }
                    false
                }
            },
            Err(e) => {
                if self.standby_held.insert(vm_id.to_owned()) {
                    warn!(vm_id, state = ?info.state, error = ?e, "could not delete the node's old VM under a standby id; the revision is not applied");
                } else {
                    debug!(vm_id, error = ?e, "still cannot delete the node's old VM under a standby id");
                }
                false
            }
        }
    }

    /// Creates one VM: prepare the image, take an address, attach it to the
    /// project's isolation group.
    async fn create_vm(&mut self, vm: &SpecVm, subnet: &Subnet) -> Option<VmInfo> {
        let vm_id = vm.vm_id.clone();
        let id = VmId(vm_id.clone());
        let image = ImageRef {
            reference: vm.image.clone(),
            digest: ImageDigest(vm.image_digest.clone()),
        };
        self.deployment_line(vm, "pull", &format!("pulling {}", vm.image))
            .await;
        let prepared = match self.engine.prepare_image(&image).await {
            Ok(prepared) => prepared,
            Err(e) => {
                warn!(vm_id, image = %vm.image, error = ?e, "could not prepare the VM image");
                // A built image is prepared by its build's `importing` step and
                // never pulled (section 6.2 step 2). If it is gone, the engine's
                // own pull error says nothing useful to the operator.
                let error = if vm.build_id.is_some() {
                    "the built image is no longer on this node".to_owned()
                } else {
                    e.to_string()
                };
                self.deployment_line(vm, "pull", &error).await;
                self.deployment_failed(vm, "pull", &error).await;
                return None;
            }
        };
        if let Some(hub) = &self.inventory {
            hub.notify_prepared(prepared.digest.clone());
        }
        self.deployment_line(
            vm,
            "rootfs",
            &format!("rootfs ready ({})", prepared.digest.0),
        )
        .await;

        let ip = self.address_for(&vm_id, subnet).await?;
        let sockets = self.store.console_socket(&vm_id).and_then(|console| {
            Ok((
                console,
                self.store.serial_socket(&vm_id)?,
                self.store.vsock_socket(&vm_id)?,
            ))
        });
        let (console, serial, vsock) = match sockets {
            Ok((console, serial, vsock)) => (
                ConsoleMode::VirtioConsole { socket: console },
                // The engine is the serial socket's only client and keeps the
                // console ring (ADR 0116); the vsock device carries exec
                // sessions for the web terminal (ADR 0113).
                SerialMode::Socket { path: serial },
                Some(VsockAttach {
                    cid: VSOCK_GUEST_CID,
                    socket: vsock,
                }),
            ),
            Err(e) => {
                warn!(vm_id, error = %e, "could not prepare the VM's console sockets");
                return None;
            }
        };
        let spec = VmSpec {
            id: id.clone(),
            vcpus: u8::try_from(vm.vcpus.max(1)).unwrap_or(u8::MAX),
            memory_mb: vm.memory_mb,
            rootfs: prepared.digest.clone(),
            disk_gb: vm.disk_gb,
            volumes: volume_attaches(vm),
            net: NetAttach {
                ip,
                gateway: subnet.gateway,
                mac: mac_for(ip),
                // VMs of one project reach each other; VMs of two projects do
                // not (engine-trait.md section 3.3, D-0084).
                isolation_group: Some(vm.project_id.clone()),
                egress_only: false,
            },
            init: init_config(&prepared.config, &vm.env, &vm_id, self.node_id.as_deref()),
            console,
            serial,
            vsock,
            work_disks: Vec::new(),
        };

        match self.engine.create(&spec).await {
            Ok(info) => {
                if let Err(e) = self
                    .store
                    .record_created(&vm_id, ip, vm.restart_generation)
                    .await
                {
                    warn!(vm_id, error = %e, "could not record the created VM");
                }
                if let Err(e) = self
                    .store
                    .set_meta(&digest_key(&vm_id), &vm.image_digest)
                    .await
                {
                    warn!(vm_id, error = %e, "could not record the created image digest");
                }
                if let Err(e) = self
                    .store
                    .set_meta(&volumes_key(&vm_id), &volumes_signature(&vm.volumes))
                    .await
                {
                    warn!(vm_id, error = %e, "could not record the created volumes");
                }
                self.deployment_line(vm, "boot", "VM created").await;
                info!(vm_id, %ip, project = %vm.project_id, "VM created");
                Some(info)
            }
            Err(e) => {
                warn!(vm_id, error = ?e, "could not create the VM");
                let error = e.to_string();
                self.deployment_line(vm, "boot", &format!("VM create failed: {error}"))
                    .await;
                self.deployment_failed(vm, "boot", &error).await;
                None
            }
        }
    }

    /// Records one deployment-progress line, when the VM belongs to a
    /// deployment.
    async fn deployment_line(&self, vm: &SpecVm, phase: &str, message: &str) {
        let Some(deployment_id) = vm.deployment_id.as_deref() else {
            return;
        };
        if let Err(e) = self
            .store
            .append_deployment_line(deployment_id, phase, message, Some(&vm.vm_id))
            .await
        {
            warn!(vm_id = %vm.vm_id, error = %e, "could not record a deployment line");
        }
    }

    /// Buffers one `deployment.failed` event, at most once per deployment.
    async fn deployment_failed(&self, vm: &SpecVm, phase: &str, error: &str) {
        let Some(deployment_id) = vm.deployment_id.as_deref() else {
            return;
        };
        let Some(node_id) = self.node_id.as_deref() else {
            debug!("no node id; not emitting deployment.failed");
            return;
        };
        let key = deployment_failed_key(deployment_id);
        match self.store.meta(&key).await {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(e) => {
                warn!(deployment_id, error = %e, "could not read the deployment.failed mark");
                return;
            }
        }
        let evt_id = util::event_id();
        let body = serde_json::json!({
            "schema_version": 1,
            "id": evt_id,
            "type": "deployment.failed",
            "time": util::rfc3339_nanos(self.store.next_log_time()),
            "source": {"kind": "node", "node_id": node_id},
            "subject": {"type": "deployment", "id": deployment_id},
            "project_id": vm.project_id,
            "actor": {"type": "node", "id": node_id},
            "data": {
                "deployment_id": deployment_id,
                "app_id": vm.app_id,
                "phase": phase,
                "error": error,
            },
        });
        if let Err(e) = self.store.push_event(&evt_id, &body).await {
            warn!(deployment_id, error = %e, "could not buffer deployment.failed");
            return;
        }
        if let Err(e) = self.store.set_meta(&key, "1").await {
            warn!(deployment_id, error = %e, "could not record the deployment.failed mark");
        }
        info!(deployment_id, phase, "deployment.failed buffered");
    }

    /// Buffers one `app.down` event for a health check that failed past its
    /// threshold (`data_app_down`): the app, this node, the check and how many
    /// consecutive failures the transition took.
    async fn emit_app_down(
        &self,
        app_id: &str,
        project_id: &str,
        vm_id: &str,
        failures: u32,
        last_error: Option<&str>,
    ) {
        let Some(node_id) = self.node_id.as_deref() else {
            debug!("no node id; not emitting app.down");
            return;
        };
        let evt_id = util::event_id();
        let mut data = serde_json::json!({
            "app_id": app_id,
            "node_id": node_id,
            "vm_id": vm_id,
            "check": "health_check",
            "failures": failures.max(1),
        });
        if let Some(last_error) = last_error {
            data["last_error"] = serde_json::json!(last_error);
        }
        let body = serde_json::json!({
            "schema_version": 1,
            "id": evt_id,
            "type": "app.down",
            "time": util::rfc3339_nanos(self.store.next_log_time()),
            "source": {"kind": "node", "node_id": node_id},
            "subject": {"type": "app", "id": app_id},
            "project_id": project_id,
            "actor": {"type": "node", "id": node_id},
            "data": data,
        });
        if let Err(e) = self.store.push_event(&evt_id, &body).await {
            warn!(app_id, error = %e, "could not buffer app.down");
            return;
        }
        info!(app_id, vm_id, failures, "app.down buffered");
    }

    /// Buffers one `app.up` event when a failing health check recovers
    /// (`data_app_up`).
    async fn emit_app_up(&self, app_id: &str, project_id: &str) {
        let Some(node_id) = self.node_id.as_deref() else {
            debug!("no node id; not emitting app.up");
            return;
        };
        let evt_id = util::event_id();
        let body = serde_json::json!({
            "schema_version": 1,
            "id": evt_id,
            "type": "app.up",
            "time": util::rfc3339_nanos(self.store.next_log_time()),
            "source": {"kind": "node", "node_id": node_id},
            "subject": {"type": "app", "id": app_id},
            "project_id": project_id,
            "actor": {"type": "node", "id": node_id},
            "data": {"app_id": app_id},
        });
        if let Err(e) = self.store.push_event(&evt_id, &body).await {
            warn!(app_id, error = %e, "could not buffer app.up");
            return;
        }
        info!(app_id, "app.up buffered");
    }

    /// Probes every running VM whose health check is due and records changes.
    ///
    /// Runs from the agent loop every 500 ms and at the end of a reconcile
    /// pass, so a deployment sees its first pass about a second after the VM
    /// reaches running. A check that failed past its threshold outside the
    /// start period, with `restart` on, queues a health restart for the next
    /// reconcile pass (section 6.3). Returns whether it queued one.
    pub async fn health_pass(&mut self, snapshot: Option<&Snapshot>) -> bool {
        let Some(snapshot) = snapshot else {
            return false;
        };
        let infos = match self.engine.list().await {
            Ok(infos) => infos,
            Err(e) => {
                warn!(error = ?e, "could not list VMs for health checks");
                return false;
            }
        };
        let mut queued = false;
        let started: HashMap<String, Option<SystemTime>> = infos
            .iter()
            .filter(|info| info.state == VmState::Running)
            .map(|info| (info.id.0.clone(), info.started_at))
            .collect();
        let running: HashSet<String> = started.keys().cloned().collect();

        let mut targets: HashMap<String, HealthTargetInfo> = HashMap::new();
        for vm in &snapshot.state.spec.vms {
            if vm.desired_status != DesiredStatus::Running || !running.contains(&vm.vm_id) {
                continue;
            }
            let ip = self
                .store
                .vm_record(&vm.vm_id)
                .await
                .ok()
                .flatten()
                .and_then(|record| record.ip);
            let Some(ip) = ip else {
                continue;
            };
            if let Some(target) = HealthTarget::for_vm(vm, ip) {
                // The VM that runs attaches the volumes it was created with,
                // which differ from the spec's until a recreate is done, and
                // a backup of either pauses it (D-0259).
                let mut volumes: Vec<String> = volume_ids(vm).map(str::to_owned).collect();
                if let Some(recorded) = self
                    .store
                    .meta(&volumes_key(&vm.vm_id))
                    .await
                    .ok()
                    .flatten()
                {
                    for volume_id in signature_volume_ids(&recorded) {
                        if !volumes.iter().any(|known| known == volume_id) {
                            volumes.push(volume_id.to_owned());
                        }
                    }
                }
                targets.insert(
                    vm.vm_id.clone(),
                    HealthTargetInfo {
                        target,
                        deployment_id: vm.deployment_id.clone(),
                        app_id: vm.app_id.clone(),
                        project_id: vm.project_id.clone(),
                        volumes,
                    },
                );
            }
        }

        // Forget VMs that were removed, are no longer desired running or lost
        // their port. A VM still desired running keeps its state while it is
        // stopped, as between the stop and the start of a health restart, so
        // a `failing` check is closed by `app.up` once it passes again.
        let watched: HashSet<&str> = snapshot
            .state
            .spec
            .vms
            .iter()
            .filter(|vm| vm.desired_status == DesiredStatus::Running && vm.port.is_some())
            .map(|vm| vm.vm_id.as_str())
            .collect();
        self.health.retain(|vm_id, state| {
            if !watched.contains(vm_id.as_str()) {
                return false;
            }
            if running.contains(vm_id) {
                return targets.contains_key(vm_id);
            }
            state.run_ended = true;
            true
        });

        let now = Instant::now();
        let ids: Vec<String> = targets.keys().cloned().collect();
        for vm_id in ids {
            let Some(info) = targets.remove(&vm_id) else {
                continue;
            };
            let HealthTargetInfo {
                target,
                deployment_id,
                app_id,
                project_id,
                volumes,
            } = info;
            let started_at = started.get(&vm_id).copied().flatten();
            let mut state = self
                .health
                .remove(&vm_id)
                .unwrap_or_else(|| HealthState::new(target.clone(), started_at, now));
            if !state.target.probes_like(&target) {
                // Another check: it starts over. A change of `start_period_s`
                // or `restart` only changes what the next failure does.
                state.status = Health::Unknown;
                state.failures = 0;
                state.next_at = now + FIRST_PROBE_DELAY;
                state.restart_asked = false;
            }
            state.target = target;
            if state.run_ended || state.started_at != started_at {
                state.begin_run(started_at, now);
            }
            if now < state.next_at {
                self.health.insert(vm_id, state);
                continue;
            }
            // D-0259: a VM paused for a backup is not probed; the probe could
            // only time out.
            let before = self.paused.mark_of(volumes.iter().map(String::as_str));
            if before.held > 0 {
                state.next_at = now + state.target.interval;
                self.health.insert(vm_id, state);
                continue;
            }

            let previous = state.status;
            let in_start_period = state.in_start_period(now);
            let reason = match probe(&state.target).await {
                Ok(code) if is_passing(code) => None,
                Ok(code) => Some(format!("status {code}")),
                Err(reason) => Some(reason),
            };
            // D-0259: once a paused VM is resumed its failures count from
            // zero, and a probe a pause overlapped does not count.
            let after = self.paused.mark_of(volumes.iter().map(String::as_str));
            if after.latest != state.latest_pause_seen {
                state.failures = 0;
                state.latest_pause_seen = after.latest;
            }
            let paused = after.held > 0 || after.latest != before.latest;
            let status = if reason.is_none() {
                state.failures = 0;
                Health::Passing
            } else if in_start_period || paused {
                // Section 6.3: a failed probe in the start period does not
                // count, nor one of a VM a backup paused (D-0259).
                debug!(
                    vm_id,
                    in_start_period, paused, "a failed probe that does not count"
                );
                previous
            } else {
                state.failures = state.failures.saturating_add(1);
                if state.failures >= state.target.failures {
                    Health::Failing
                } else {
                    previous
                }
            };
            state.status = status;
            state.next_at = now + state.target.interval;
            let changed = status != previous;
            let failures = state.failures;
            // Section 6.3 (ADR 0184): after `failures` consecutive failures
            // outside the start period, and once per start, the VM is
            // stopped and started again. Only failures outside the start
            // period are counted, and a start resets the count.
            if state.target.restart && failures >= state.target.failures && !state.restart_asked {
                state.restart_asked = true;
                self.health_restarts.insert(vm_id.clone());
                queued = true;
                warn!(
                    vm_id,
                    failures, "the VM's health check kept failing; restarting it"
                );
            }
            self.health.insert(vm_id.clone(), state);

            // One event per health transition, never per probe: `app.down`
            // when it fails past its threshold and `app.up` when it recovers.
            // The primary opens and resolves the incident from these.
            if changed && let Some(app_id) = &app_id {
                if status == Health::Failing && previous != Health::Failing {
                    self.emit_app_down(app_id, &project_id, &vm_id, failures, reason.as_deref())
                        .await;
                } else if status == Health::Passing && previous == Health::Failing {
                    self.emit_app_up(app_id, &project_id).await;
                }
            }

            if let Some(deployment_id) = deployment_id
                && changed
            {
                let line = if status == Health::Passing {
                    Some("health check passing".to_owned())
                } else if status == Health::Failing {
                    reason.map(|reason| format!("health check failing: {reason}"))
                } else {
                    None
                };
                if let Some(line) = line
                    && let Err(e) = self
                        .store
                        .append_deployment_line(&deployment_id, "health", &line, Some(&vm_id))
                        .await
                {
                    warn!(vm_id, error = %e, "could not record a health progress line");
                }
            }
        }
        queued
    }

    /// The image digest the VM was created with, if it is on record.
    async fn known_digest(&self, vm_id: &str) -> Option<String> {
        match self.store.meta(&digest_key(vm_id)).await {
            Ok(digest) => digest,
            Err(e) => {
                warn!(vm_id, error = %e, "could not read the recorded image digest");
                None
            }
        }
    }

    /// The VM's persisted address when it still fits the subnet, otherwise the
    /// lowest free one.
    async fn address_for(&self, vm_id: &str, subnet: &Subnet) -> Option<Ipv4Addr> {
        let persisted = self
            .store
            .vm_record(vm_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.ip);
        if let Some(ip) = persisted
            && subnet.contains(ip)
        {
            return Some(ip);
        }
        let used: HashSet<Ipv4Addr> = match self.store.used_ips().await {
            Ok(ips) => ips.into_iter().collect(),
            Err(e) => {
                warn!(vm_id, error = %e, "could not read the used addresses");
                HashSet::new()
            }
        };
        Some(subnet.allocate(&used))
    }

    /// Stops a VM gracefully with the configured timeout.
    /// Section 6.4 at the moment of a start: `None` when a stateful VM may not
    /// start now; otherwise a guard the caller holds until the engine's start
    /// returned, so a fence of the VM's lease waits for the start and then
    /// stops the VM (section 8.5). A stateless VM needs no lease.
    async fn start_permit(&self, vm: &SpecVm) -> Option<Option<StartPermit>> {
        if !vm.stateful {
            return Some(None);
        }
        let (Some(lease_id), Some(token)) = (&vm.lease_id, vm.fencing_token) else {
            return None;
        };
        self.lease.start_permit(lease_id, token).await.map(Some)
    }

    /// Drops a queued health restart that is not carried out, so the next
    /// failure counted for the run asks for it again (section 6.3).
    fn drop_health_restart(&mut self, vm_id: &str) {
        if self.health_restarts.remove(vm_id)
            && let Some(state) = self.health.get_mut(vm_id)
        {
            state.restart_asked = false;
        }
    }

    /// Whether a fence of the stateful VM's lease is stopping it: then the
    /// fence alone stops the VM (section 8.5 step 1), since a second stop
    /// would hold the engine's lock on the VM and make the fence's own stop
    /// time out.
    fn fence_owns_stop(&self, vm: &SpecVm) -> bool {
        Self::fenced_lease(vm).is_some_and(|lease_id| self.lease.fence_owns_stop(lease_id))
    }

    fn fenced_lease(vm: &SpecVm) -> Option<&str> {
        vm.lease_id.as_deref().filter(|_| vm.stateful)
    }

    /// Runs `stop`, an engine stop of `vm`, unless a fence of its lease is
    /// stopping the VM: then, or as soon as such a fence is triggered while
    /// `stop` runs, `stop` is given up and `None` returned. Dropping it
    /// releases the engine's lock on the VM, so the fence's own stop runs
    /// within its timers (section 8.5 step 1).
    async fn unless_fenced<T>(&self, vm: &SpecVm, stop: impl Future<Output = T>) -> Option<T> {
        let Some(lease_id) = Self::fenced_lease(vm) else {
            return Some(stop.await);
        };
        tokio::select! {
            biased;
            () = self.lease.fence_triggered(lease_id) => None,
            stopped = stop => Some(stopped),
        }
    }

    async fn stop_vm(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.engine
            .stop(
                id,
                StopMode::Graceful {
                    timeout: self.options.stop_timeout,
                    force_within: FORCE_WITHIN,
                },
            )
            .await
    }

    /// Restarts a VM that exited on its own, with exponential backoff.
    ///
    /// Returns `Ok(None)` while the backoff is still running. `permit` is the
    /// stateful VM's start permit, released as soon as the engine's start
    /// returned.
    async fn crash_restart(
        &mut self,
        vm_id: &str,
        id: &VmId,
        exit_code: Option<i32>,
        permit: Option<StartPermit>,
    ) -> Result<Option<VmInfo>, EngineError> {
        let base = self.options.backoff_base;
        let max = self.options.backoff_max;
        let now = Instant::now();
        let timing = self.timings.entry(vm_id.to_owned()).or_insert(VmTiming {
            retry_at: None,
            backoff: base,
            running_since: None,
        });
        if timing.backoff.is_zero() {
            timing.backoff = base;
        }
        let retry_at = *timing.retry_at.get_or_insert(now + timing.backoff);
        if now < retry_at {
            debug!(vm_id, "waiting out the crash-restart backoff");
            return Ok(None);
        }

        let started = self.engine.start(id, start_within(&permit)).await;
        // A fence of the lease may stop the VM from here on.
        drop(permit);
        match started {
            Ok(started) => {
                if let Err(e) = self
                    .store
                    .record_restart(vm_id, exit_code.map(i64::from))
                    .await
                {
                    warn!(vm_id, error = %e, "could not record the crash restart");
                }
                let timing = self.timings.entry(vm_id.to_owned()).or_default();
                timing.retry_at = None;
                timing.backoff = (timing.backoff * 2).min(max);
                timing.running_since = Some(now);
                warn!(vm_id, "restarted a crashed VM");
                Ok(Some(started))
            }
            Err(e) => {
                self.back_off(vm_id, Instant::now());
                Err(e)
            }
        }
    }

    /// Whether a failed start of the VM still waits out its backoff (#462).
    /// Only a failure sets the wait, so a first start never waits.
    fn start_backing_off(&self, vm_id: &str) -> bool {
        self.timings
            .get(vm_id)
            .and_then(|timing| timing.retry_at)
            .is_some_and(|retry_at| Instant::now() < retry_at)
    }

    /// After a failed start or crash restart, a `Timeout` included: the next
    /// start waits the VM's backoff from `now`, the moment the start returned,
    /// and the backoff doubles up to `backoff_max`. It resets to
    /// `backoff_base` only once the VM has run for `healthy_after`.
    fn back_off(&mut self, vm_id: &str, now: Instant) {
        let base = self.options.backoff_base;
        let max = self.options.backoff_max;
        let timing = self.timings.entry(vm_id.to_owned()).or_default();
        if timing.backoff.is_zero() {
            timing.backoff = base;
        }
        timing.retry_at = Some(now + timing.backoff);
        timing.backoff = (timing.backoff * 2).min(max);
    }

    /// Lets the next start of the VM run at once; its backoff keeps its length.
    fn clear_backoff(&mut self, vm_id: &str) {
        if let Some(timing) = self.timings.get_mut(vm_id) {
            timing.retry_at = None;
        }
    }

    /// Stops and deletes every engine VM that left the spec. Returns `false`
    /// when one of them could not be removed.
    async fn remove_extra_vms(&mut self, desired: &HashSet<&str>) -> bool {
        let vms = match self.engine.list().await {
            Ok(vms) => vms,
            Err(e) => {
                warn!(error = ?e, "could not list the engine's VMs");
                return false;
            }
        };
        let mut reached = true;
        for info in vms {
            let vm_id = info.id.0.clone();
            if !vm_id.starts_with("vm_") || desired.contains(vm_id.as_str()) {
                continue;
            }
            if matches!(
                info.state,
                VmState::Running | VmState::Starting | VmState::Stopping
            ) && let Err(e) = self
                .engine
                .stop(
                    &info.id,
                    StopMode::Force {
                        within: FORCE_WITHIN,
                    },
                )
                .await
            {
                warn!(vm_id, error = ?e, "could not force-stop the removed VM");
                reached = false;
                continue;
            }
            match self.engine.delete(&info.id).await {
                Ok(()) => {
                    self.forget_deleted_vm(&vm_id).await;
                    self.once_only.push(VmStatus {
                        vm_id: vm_id.clone(),
                        observed_status: ObservedStatus::Deleted,
                        health: Health::Unknown,
                        ip: None,
                        restart_count: 0,
                        last_exit_code: None,
                    });
                    info!(vm_id, "VM deleted from the engine");
                }
                Err(e) => {
                    warn!(vm_id, error = ?e, "could not delete the removed VM");
                    reached = false;
                }
            }
        }
        reached
    }

    /// Forgets what the node kept about a VM the engine deleted: its row,
    /// its digest, and its timers, health and log reader.
    async fn forget_deleted_vm(&mut self, vm_id: &str) {
        if let Err(e) = self.store.forget_vm(vm_id).await {
            warn!(vm_id, error = %e, "could not forget the removed VM");
        }
        self.last_states.remove(vm_id);
        if let Err(e) = self.store.set_meta(&digest_key(vm_id), "").await {
            warn!(vm_id, error = %e, "could not forget the removed VM's digest");
        }
        self.timings.remove(vm_id);
        self.health.remove(vm_id);
        self.health_restarts.remove(vm_id);
        self.restarts_due.remove(vm_id);
        self.refused_digests.remove(vm_id);
        self.collector.detach(vm_id);
    }

    /// Reads the engine's world and publishes it when it changed.
    async fn publish(&mut self) -> Vec<VmStatus> {
        let mut statuses = Vec::new();
        match self.engine.list().await {
            Ok(vms) => {
                for info in vms {
                    if !info.id.0.starts_with("vm_") {
                        continue;
                    }
                    // A VM the agent did not create in this process (an
                    // adopted one, or a restart of an existing VMM) still gets
                    // exactly one reader.
                    if matches!(info.state, VmState::Running | VmState::Starting) {
                        let app_id = self
                            .spec_meta
                            .get(&info.id.0)
                            .and_then(|meta| meta.app_id.clone());
                        self.collector.attach(&info.id.0, app_id);
                    }
                    let record = self
                        .store
                        .vm_record(&info.id.0)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let health = if info.state == VmState::Running {
                        self.health
                            .get(&info.id.0)
                            .map_or(Health::Unknown, |health| health.status)
                    } else {
                        Health::Unknown
                    };
                    statuses.push(VmStatus {
                        vm_id: info.id.0.clone(),
                        observed_status: observed_status(info.state),
                        health,
                        ip: record.ip.map(|ip| ip.to_string()),
                        restart_count: record.restart_count,
                        last_exit_code: record.last_exit_code,
                    });
                }
            }
            Err(e) => warn!(error = ?e, "could not list the engine's VMs to report them"),
        }
        // A `standby` VM is never created, so the node reports it `stopped`
        // (section 5.11). A VM the engine does hold under that id is reported
        // as the engine sees it.
        for vm_id in &self.standby {
            if !statuses.iter().any(|status| &status.vm_id == vm_id) {
                statuses.push(VmStatus {
                    vm_id: vm_id.clone(),
                    observed_status: ObservedStatus::Stopped,
                    health: Health::Unknown,
                    ip: None,
                    restart_count: 0,
                    last_exit_code: None,
                });
            }
        }
        statuses.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));
        statuses.append(&mut self.once_only);
        statuses.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));
        if self.published.as_ref() != Some(&statuses) {
            self.published = Some(statuses.clone());
            let _ = self.observed.send_replace(statuses.clone());
        }
        statuses
    }

    fn remember(&mut self, vm_id: &str, state: VmState) {
        self.last_states.insert(vm_id.to_owned(), state);
    }
}

/// The engine attachments for a VM's `spec.vms[].volumes` (section 5.11,
/// `engine-trait.md` section 3.6). A snapshot that names no serial gets the
/// contract's `quoro-vol-<n>`, numbered in order.
fn volume_attaches(vm: &SpecVm) -> Vec<VolumeAttach> {
    vm.volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| VolumeAttach {
            volume_id: VolumeId(volume.volume_id.clone()),
            mount_path: volume.mount_path.clone(),
            serial: if volume.serial.is_empty() {
                format!("quoro-vol-{index}")
            } else {
                volume.serial.clone()
            },
        })
        .collect()
}

/// The canonical form of a VM's attached volumes, compared across passes so a
/// change forces a recreate (section 6.2 step 3).
fn volumes_signature(volumes: &[quoro_proto::wire::SpecVolume]) -> String {
    let mut parts: Vec<String> = volumes
        .iter()
        .map(|volume| {
            format!(
                "{}|{}|{}",
                volume.volume_id, volume.mount_path, volume.serial
            )
        })
        .collect();
    parts.sort();
    parts.join("\n")
}

/// The volume ids of a signature [`volumes_signature`] made.
fn signature_volume_ids(signature: &str) -> impl Iterator<Item = &str> {
    signature
        .lines()
        .filter_map(|part| part.split('|').next())
        .filter(|volume_id| !volume_id.is_empty())
}

/// The `node_meta` key that remembers one VM's attached-volume signature.
fn volumes_key(vm_id: &str) -> String {
    format!("vm_volumes:{vm_id}")
}

/// A one-line error of at most 2 000 characters (section 5.3).
fn truncate_error(error: &str) -> String {
    util::one_line(error, 2_000)
}

/// Records that `vm_id` was refused `requested`, and returns whether that is
/// new: the first refusal of a digest, or one after the VM was asked for
/// another.
fn first_refusal(refused: &mut HashMap<String, String>, vm_id: &str, requested: &str) -> bool {
    if refused.get(vm_id).is_some_and(|last| last == requested) {
        return false;
    }
    refused.insert(vm_id.to_string(), requested.to_string());
    true
}

/// The `node_meta` key that remembers one VM's create-time image digest.
fn digest_key(vm_id: &str) -> String {
    format!("image_digest:{vm_id}")
}

/// The `node_meta` key that remembers a `deployment.failed` was emitted.
fn deployment_failed_key(deployment_id: &str) -> String {
    format!("deployment_failed:{deployment_id}")
}

/// The init configuration: the image's own command line, overlaid with the
/// spec's environment by name in a deterministic (name) order, then with the
/// instance identity (`QUORO_INSTANCE_ID` and `QUORO_NODE_ID`) so the node's
/// values win over a stored one (node-agent-protocol.md section 6.10).
fn init_config(
    image: &ImageConfig,
    spec_env: &BTreeMap<String, String>,
    vm_id: &str,
    node_id: Option<&str>,
) -> InitConfig {
    let mut env: BTreeMap<String, String> = image.env.iter().cloned().collect();
    for (name, value) in spec_env {
        env.insert(name.clone(), value.clone());
    }
    env.insert("QUORO_INSTANCE_ID".to_owned(), vm_id.to_owned());
    if let Some(node_id) = node_id {
        env.insert("QUORO_NODE_ID".to_owned(), node_id.to_owned());
    }
    InitConfig {
        entrypoint: image.entrypoint.clone(),
        cmd: image.cmd.clone(),
        env: env.into_iter().collect(),
        user: image.user.clone(),
        workdir: image.workdir.clone(),
    }
}

/// The MAC derived from the VM's address: locally administered (`0x52`) and
/// unique per address, so a persisted IP keeps its MAC across restarts.
#[must_use]
pub fn mac_for(ip: Ipv4Addr) -> MacAddr {
    let octets = ip.octets();
    MacAddr([0x52, 0x55, octets[0], octets[1], octets[2], octets[3]])
}

fn observed_status(state: VmState) -> ObservedStatus {
    match state {
        VmState::Creating => ObservedStatus::Creating,
        VmState::Stopped => ObservedStatus::Stopped,
        VmState::Starting => ObservedStatus::Starting,
        VmState::Running => ObservedStatus::Running,
        VmState::Stopping => ObservedStatus::Stopping,
        VmState::Crashed => ObservedStatus::Crashed,
        VmState::Deleted => ObservedStatus::Deleted,
        VmState::Unknown => ObservedStatus::Unknown,
    }
}

/// Awaits the next engine event without pulling in a `futures` dependency.
async fn next_event(stream: &mut EventStream) -> Option<EngineEvent> {
    stream.next().await
}

/// The bound the engine gets for a start: a stateful VM's permit names it
/// (node-agent-protocol.md section 6.4), and a stateless VM gets
/// [`START_WITHIN`].
fn start_within(permit: &Option<StartPermit>) -> Duration {
    permit
        .as_ref()
        .map_or(START_WITHIN, StartPermit::start_within)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #354: a refused digest is logged once per VM and digest, not on
    /// every pass; a VM asked for yet another digest is logged again.
    #[test]
    fn a_refused_digest_is_reported_once_per_vm_and_digest() {
        let mut refused = HashMap::new();
        assert!(first_refusal(&mut refused, "vm_a", "sha256:new"));
        assert!(!first_refusal(&mut refused, "vm_a", "sha256:new"));
        assert!(!first_refusal(&mut refused, "vm_a", "sha256:new"));
        assert!(first_refusal(&mut refused, "vm_b", "sha256:new"));
        assert!(first_refusal(&mut refused, "vm_a", "sha256:newer"));
        assert!(!first_refusal(&mut refused, "vm_a", "sha256:newer"));
    }

    #[test]
    fn a_failed_standby_image_is_prepared_again_only_after_the_retry_delay() {
        let now = Instant::now();
        assert_eq!(StandbyOutcome::Prepared.settled(now), Some(true));
        assert_eq!(StandbyOutcome::Built.settled(now), Some(true));
        assert_eq!(StandbyOutcome::Failed(now).settled(now), Some(false));
        let almost = now + STANDBY_RETRY - Duration::from_millis(1);
        assert_eq!(StandbyOutcome::Failed(now).settled(almost), Some(false));
        assert_eq!(
            StandbyOutcome::Failed(now).settled(now + STANDBY_RETRY),
            None
        );
    }

    #[test]
    fn subnets_skip_the_reserved_addresses() {
        let subnet = Subnet::parse("10.9.8.0/29", "10.9.8.1").unwrap();
        let used = HashSet::new();
        assert_eq!(subnet.allocate(&used), Ipv4Addr::new(10, 9, 8, 2));
        let used: HashSet<Ipv4Addr> = [Ipv4Addr::new(10, 9, 8, 2)].into_iter().collect();
        assert_eq!(subnet.allocate(&used), Ipv4Addr::new(10, 9, 8, 3));
        assert!(subnet.contains(Ipv4Addr::new(10, 9, 8, 6)));
        assert!(!subnet.contains(Ipv4Addr::new(10, 9, 8, 1)));
        assert!(!subnet.contains(Ipv4Addr::new(10, 9, 8, 7)));
        assert!(!subnet.contains(Ipv4Addr::new(10, 9, 8, 0)));
    }

    #[test]
    fn builder_addresses_come_from_the_top_of_the_subnet() {
        let subnet = Subnet::parse("10.9.8.0/29", "10.9.8.1").unwrap();
        assert_eq!(
            subnet.allocate_high(&HashSet::new()),
            Ipv4Addr::new(10, 9, 8, 6)
        );
        let used: HashSet<Ipv4Addr> = [Ipv4Addr::new(10, 9, 8, 6)].into_iter().collect();
        assert_eq!(subnet.allocate_high(&used), Ipv4Addr::new(10, 9, 8, 5));
        let full: HashSet<Ipv4Addr> = (2..=6).map(|n| Ipv4Addr::new(10, 9, 8, n)).collect();
        assert_eq!(subnet.allocate_high(&full), Ipv4Addr::new(10, 9, 8, 6));
    }

    #[test]
    fn macs_are_local_and_unique() {
        let mac = mac_for(Ipv4Addr::new(10, 42, 1, 7));
        assert_eq!(mac.0[0] & 0b11, 0b10);
        assert_ne!(mac, mac_for(Ipv4Addr::new(10, 42, 1, 8)));
    }

    #[test]
    fn spec_env_overrides_the_image_by_name() {
        let image = ImageConfig {
            entrypoint: vec!["/app".to_owned()],
            cmd: Vec::new(),
            env: vec![
                ("B".to_owned(), "image".to_owned()),
                ("A".to_owned(), "image".to_owned()),
            ],
            user: None,
            workdir: None,
            exposed_ports: Vec::new(),
        };
        let overrides = BTreeMap::from([("A".to_owned(), "spec".to_owned())]);
        let init = init_config(&image, &overrides, "vm_test", None);
        assert_eq!(
            init.env,
            vec![
                ("A".to_owned(), "spec".to_owned()),
                ("B".to_owned(), "image".to_owned()),
                ("QUORO_INSTANCE_ID".to_owned(), "vm_test".to_owned())
            ]
        );
        assert_eq!(init.entrypoint, vec!["/app".to_owned()]);
    }

    #[test]
    fn instance_identity_overrides_the_image_and_spec_env() {
        let image = ImageConfig {
            entrypoint: vec!["/app".to_owned()],
            cmd: Vec::new(),
            env: vec![("QUORO_INSTANCE_ID".to_owned(), "image".to_owned())],
            user: None,
            workdir: None,
            exposed_ports: Vec::new(),
        };
        let overrides = BTreeMap::from([
            ("QUORO_INSTANCE_ID".to_owned(), "spec".to_owned()),
            ("QUORO_NODE_ID".to_owned(), "spec".to_owned()),
        ]);
        let init = init_config(&image, &overrides, "vm_abc", Some("nod_abc"));
        assert_eq!(
            init.env,
            vec![
                ("QUORO_INSTANCE_ID".to_owned(), "vm_abc".to_owned()),
                ("QUORO_NODE_ID".to_owned(), "nod_abc".to_owned())
            ]
        );
    }

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-health-{}",
                crate::util::hex(&crate::util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A health check probed every second, `failing` at the first counted
    /// failure, with every probe counted and no restart: the transition
    /// tests.
    fn health_snapshot(port: u16) -> Snapshot {
        health_snapshot_with(
            port,
            quoro_proto::wire::HealthCheck {
                path: Some("/".to_owned()),
                interval_s: Some(1),
                timeout_s: Some(1),
                failures: Some(1),
                start_period_s: Some(0),
                restart: Some(false),
            },
        )
    }

    /// One VM on 127.0.0.1 (the lowest address of 127.0.0.0/8 that is not
    /// the gateway) with app port `port` and `check`.
    fn health_snapshot_with(port: u16, check: quoro_proto::wire::HealthCheck) -> Snapshot {
        use quoro_proto::wire::{Spec, SpecNetwork};

        Snapshot {
            rev: 1,
            state: DesiredState {
                rev: 1,
                issued_at: "2026-11-02T10:15:31Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms: vec![SpecVm {
                        vm_id: "vm_health".to_owned(),
                        project_id: "prj_1".to_owned(),
                        app_id: Some("app_1".to_owned()),
                        deployment_id: Some("dep_1".to_owned()),
                        name: "vm_health".to_owned(),
                        image: "img".to_owned(),
                        image_digest: "sha256:aa".to_owned(),
                        vcpus: 1,
                        memory_mb: 128,
                        disk_gb: 1,
                        port: Some(port),
                        env: BTreeMap::new(),
                        health_check: Some(check),
                        desired_status: DesiredStatus::Running,
                        standby: false,
                        restart_generation: 0,
                        stateful: false,
                        lease_id: None,
                        fencing_token: None,
                        volumes: Vec::new(),
                        build_id: None,
                    }],
                    routes: Vec::new(),
                    network: Some(SpecNetwork {
                        subnet: "127.0.0.0/8".to_owned(),
                        gateway: "127.0.0.2".to_owned(),
                    }),
                    ingress: None,
                    builds: Vec::new(),
                    mesh: None,
                    edge: None,
                    volumes: Vec::new(),
                    backups: Vec::new(),
                    backup_repository: None,
                    votes: None,
                    witness: None,
                    quarantine_deletes: Vec::new(),
                },
                signature: String::new(),
            },
        }
    }

    #[tokio::test]
    async fn a_failed_prepare_buffers_one_deployment_failed_event() {
        use quoro_engine::{FakeEngine, FakeOp};

        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.fail_next(
            FakeOp::PrepareImage,
            quoro_engine::EngineError::Image("pull refused".to_owned()),
        );
        let (reconciler, _observed) = Reconciler::new(
            engine,
            store.clone(),
            ReconcileConfig {
                bridge: "quoro0".to_owned(),
                dns: Vec::new(),
            },
            ReconcileOptions {
                tick: Duration::from_millis(10),
                stop_timeout: Duration::from_millis(20),
                backoff_base: Duration::from_millis(1),
                backoff_max: Duration::from_millis(8),
                healthy_after: Duration::from_secs(60),
            },
        );
        let mut reconciler = reconciler.with_node_id("nod_test".to_owned());
        let snapshot = health_snapshot(8080);

        reconciler.reconcile_once(Some(&snapshot)).await;
        let events = store.buffered_events().await.unwrap();
        assert_eq!(events.len(), 1, "{events:?}");
        let body = &events[0].body;
        assert_eq!(body["type"], "deployment.failed");
        assert_eq!(body["subject"]["id"], "dep_1");
        assert_eq!(body["source"]["node_id"], "nod_test");
        assert_eq!(body["data"]["phase"], "pull");
        assert!(
            body["data"]["error"]
                .as_str()
                .is_some_and(|error| error.contains("pull refused"))
        );
        let lines = store
            .query_deployment_lines("dep_1", Some("pull"))
            .await
            .unwrap();
        assert!(
            lines
                .last()
                .is_some_and(|line| line.message.contains("pull refused"))
        );

        // The next pass pulls successfully; the failure is not emitted twice.
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(store.buffered_events().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_health_transition_buffers_one_app_down_then_one_app_up() {
        use quoro_engine::FakeEngine;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let healthy = Arc::new(AtomicBool::new(true));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let flag = Arc::clone(&healthy);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 512];
                let _ = socket.read(&mut buffer).await;
                let status = if flag.load(Ordering::Relaxed) {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let _ = socket
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
                    .await;
            }
        });

        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let (reconciler, _observed) = Reconciler::new(
            engine,
            store.clone(),
            ReconcileConfig {
                bridge: "quoro0".to_owned(),
                dns: Vec::new(),
            },
            ReconcileOptions {
                tick: Duration::from_millis(10),
                stop_timeout: Duration::from_millis(20),
                backoff_base: Duration::from_millis(1),
                backoff_max: Duration::from_millis(8),
                healthy_after: Duration::from_secs(60),
            },
        );
        let mut reconciler = reconciler.with_node_id("nod_test".to_owned());
        let snapshot = health_snapshot(port);
        let wait = FIRST_PROBE_DELAY + Duration::from_millis(100);
        let app_events = |events: &[crate::store::BufferedEvent]| {
            events
                .iter()
                .filter(|event| matches!(event.body["type"].as_str(), Some("app.down" | "app.up")))
                .map(|event| event.body.clone())
                .collect::<Vec<_>>()
        };

        // Healthy from the start: passing, and no app event.
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Unknown);
        tokio::time::sleep(wait).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert!(app_events(&store.buffered_events().await.unwrap()).is_empty());

        // It fails past its threshold: exactly one app.down.
        healthy.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(1) + Duration::from_millis(100)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        let events = app_events(&store.buffered_events().await.unwrap());
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["type"], "app.down");
        assert_eq!(events[0]["subject"]["id"], "app_1");
        assert_eq!(events[0]["source"]["node_id"], "nod_test");
        assert_eq!(events[0]["data"]["app_id"], "app_1");
        assert_eq!(events[0]["data"]["node_id"], "nod_test");
        assert_eq!(events[0]["data"]["check"], "health_check");
        assert_eq!(events[0]["data"]["failures"], 1);

        // Still failing: no second event.
        tokio::time::sleep(Duration::from_secs(1) + Duration::from_millis(100)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(app_events(&store.buffered_events().await.unwrap()).len(), 1);

        // Recovered: one app.up, and never another app.down.
        healthy.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(1) + Duration::from_millis(100)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        let events = app_events(&store.buffered_events().await.unwrap());
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1]["type"], "app.up");
        assert_eq!(events[1]["subject"]["id"], "app_1");
        assert_eq!(events[1]["source"]["node_id"], "nod_test");
        assert_eq!(events[1]["data"]["app_id"], "app_1");
        server.abort();
    }

    #[tokio::test]
    async fn health_goes_passing_then_failing() {
        use quoro_engine::FakeEngine;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 512];
                let _ = socket.read(&mut buffer).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });

        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let (mut reconciler, _observed) = Reconciler::new(
            engine,
            store.clone(),
            ReconcileConfig {
                bridge: "quoro0".to_owned(),
                dns: Vec::new(),
            },
            ReconcileOptions {
                tick: Duration::from_millis(10),
                stop_timeout: Duration::from_millis(20),
                backoff_base: Duration::from_millis(1),
                backoff_max: Duration::from_millis(8),
                healthy_after: Duration::from_secs(60),
            },
        );
        let snapshot = health_snapshot(port);

        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Unknown, "probed a second later");

        tokio::time::sleep(FIRST_PROBE_DELAY + Duration::from_millis(100)).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Passing);
        let lines = store
            .query_deployment_lines("dep_1", Some("health"))
            .await
            .unwrap();
        assert_eq!(
            lines.last().map(|line| line.message.as_str()),
            Some("health check passing")
        );

        server.abort();
        tokio::time::sleep(Duration::from_secs(1) + Duration::from_millis(100)).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        let lines = store
            .query_deployment_lines("dep_1", Some("health"))
            .await
            .unwrap();
        assert!(
            lines
                .last()
                .is_some_and(|line| line.message.starts_with("health check failing")),
            "{lines:?}"
        );
    }

    /// An HTTP server on 127.0.0.1 that answers `200` while `healthy` holds
    /// and `503` otherwise, and its port.
    async fn switchable_server(
        healthy: Arc<std::sync::atomic::AtomicBool>,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 512];
                let _ = socket.read(&mut buffer).await;
                let status = if healthy.load(Ordering::Relaxed) {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let _ = socket
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
                    .await;
            }
        });
        (port, server)
    }

    /// A reconciler over `engine` with the test timings and a crash backoff
    /// starting at `backoff_base`.
    fn health_reconciler(
        engine: Arc<quoro_engine::FakeEngine>,
        store: NodeStore,
        backoff_base: Duration,
    ) -> Reconciler<quoro_engine::FakeEngine> {
        let (reconciler, _observed) = Reconciler::new(
            engine,
            store,
            ReconcileConfig {
                bridge: "quoro0".to_owned(),
                dns: Vec::new(),
            },
            ReconcileOptions {
                tick: Duration::from_millis(10),
                stop_timeout: Duration::from_millis(20),
                backoff_base,
                backoff_max: backoff_base * 64,
                healthy_after: Duration::from_secs(60),
            },
        );
        reconciler.with_node_id("nod_test".to_owned())
    }

    /// A check probed every second that fails at the first counted failure.
    fn check(start_period_s: u32, restart: Option<bool>) -> quoro_proto::wire::HealthCheck {
        quoro_proto::wire::HealthCheck {
            path: Some("/".to_owned()),
            interval_s: Some(1),
            timeout_s: Some(1),
            failures: Some(1),
            start_period_s: Some(start_period_s),
            restart,
        }
    }

    /// The types of the buffered `app.down` and `app.up` events, in order.
    async fn app_events(store: &NodeStore) -> Vec<String> {
        store
            .buffered_events()
            .await
            .unwrap()
            .iter()
            .filter_map(|event| event.body["type"].as_str().map(str::to_owned))
            .filter(|kind| kind == "app.down" || kind == "app.up")
            .collect()
    }

    /// Slightly more than one probe interval of [`check`].
    const NEXT_PROBE: Duration = Duration::from_millis(1100);

    /// Section 6.3: failed probes within `start_period_s` after the VM
    /// started do not count; the first one after it does.
    #[tokio::test]
    async fn failed_probes_in_the_start_period_do_not_count() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let snapshot = health_snapshot_with(port, check(2, Some(false)));

        reconciler.reconcile_once(Some(&snapshot)).await;
        // About 1.1 s after the start: failed, inside the 2 s start period.
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(
            statuses[0].health,
            Health::Unknown,
            "the failure does not count"
        );
        assert!(app_events(&store).await.is_empty());

        // About 2.2 s after the start: the failure counts.
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        assert_eq!(app_events(&store).await, ["app.down"]);
        server.abort();
    }

    /// Section 6.3 (ADR 0184): with `restart` unset, so true, a VM that
    /// fails past its threshold outside the start period is stopped and
    /// started again in place on the crash path: counted in
    /// `restart_count`, `restart_generation` untouched. The incident it
    /// opened closes with `app.up` once the restarted VM passes.
    #[tokio::test]
    async fn an_unhealthy_vm_is_stopped_and_started_again_then_reports_app_up() {
        use quoro_engine::FakeEngine;
        use std::sync::atomic::Ordering;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let snapshot = health_snapshot_with(port, check(0, None));
        let id = VmId("vm_health".to_owned());

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        assert_eq!(app_events(&store).await, ["app.down"]);
        assert!(
            engine.stop_calls().is_empty(),
            "the stop is the next pass's"
        );

        // The next pass stops the VM; the crash backoff holds the start.
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].observed_status, ObservedStatus::Stopped);
        assert_eq!(
            engine.stop_calls(),
            [(
                id.clone(),
                StopMode::Graceful {
                    timeout: Duration::from_millis(20),
                    force_within: FORCE_WITHIN,
                }
            )]
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
        assert_eq!(statuses[0].restart_count, 1, "a health restart is counted");
        let record = store.vm_record("vm_health").await.unwrap().unwrap();
        assert_eq!(record.restart_generation, 0, "not a human restart");
        // Still the same incident while the restarted VM has not passed.
        assert_eq!(statuses[0].health, Health::Failing);

        healthy.store(true, Ordering::Relaxed);
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Passing);
        assert_eq!(app_events(&store).await, ["app.down", "app.up"]);
        assert_eq!(engine.stop_calls().len(), 1, "a passing VM is left alone");
        let lines = store
            .query_deployment_lines("dep_1", Some("health"))
            .await
            .unwrap();
        assert!(
            lines.iter().any(|line| line
                .message
                .starts_with("VM stopped after its health check")),
            "{lines:?}"
        );
        server.abort();
    }

    /// No restart storm: each health restart of a VM that never gets healthy
    /// waits out the crash backoff, which doubles, and the whole stretch is
    /// one incident (one `app.down`).
    #[tokio::test]
    async fn repeated_health_restarts_double_the_crash_backoff() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let base = Duration::from_millis(100);
        let mut reconciler = health_reconciler(Arc::clone(&engine), store.clone(), base);
        let snapshot = health_snapshot_with(port, check(0, None));
        let id = VmId("vm_health".to_owned());

        reconciler.reconcile_once(Some(&snapshot)).await;
        for restart in 1..=2u32 {
            tokio::time::sleep(NEXT_PROBE).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
            assert_eq!(engine.stop_calls().len(), restart as usize);
            assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Stopped);
            // A pass right after the stop does not start the VM yet.
            reconciler.reconcile_once(Some(&snapshot)).await;
            assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Stopped);

            let wait = base * 2u32.pow(restart - 1);
            tokio::time::sleep(wait + Duration::from_millis(50)).await;
            let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
            assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
            assert_eq!(statuses[0].restart_count, u64::from(restart));
            assert_eq!(
                reconciler.timings["vm_health"].backoff,
                base * 2u32.pow(restart),
                "the backoff doubles with each restart"
            );
        }
        assert_eq!(app_events(&store).await, ["app.down"]);
        server.abort();
    }

    /// Section 6.3: the start period begins again at each start, so a
    /// restarted VM is not stopped again by a failure inside its new start
    /// period.
    #[tokio::test]
    async fn the_start_period_begins_again_at_each_start() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let snapshot = health_snapshot_with(port, check(2, None));
        let id = VmId("vm_health".to_owned());

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        // Past the first start period: failing, and stopped by the next pass.
        reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Running);

        // About 1.1 s into the new start: the failure does not count.
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1, "inside the new start period");
        assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Running);

        // About 2.2 s into it: it counts, and the VM is restarted again.
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 2);
        server.abort();
    }

    /// Section 6.3: with `restart` false the node only reports.
    #[tokio::test]
    async fn restart_false_only_reports() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let snapshot = health_snapshot_with(port, check(0, Some(false)));
        let id = VmId("vm_health".to_owned());

        reconciler.reconcile_once(Some(&snapshot)).await;
        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
        }
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        assert_eq!(statuses[0].restart_count, 0);
        assert_eq!(app_events(&store).await, ["app.down"]);
        assert!(engine.stop_calls().is_empty());
        assert_eq!(engine.inspect(&id).await.unwrap().state, VmState::Running);
        server.abort();
    }

    /// A health stop that fails is given up until the VM starts again: it is
    /// not tried on every pass, where each try of a VMM that hangs could hold
    /// the reconciler for the stop's whole bound.
    #[tokio::test]
    async fn a_failed_health_stop_is_not_tried_again_on_every_pass() {
        use quoro_engine::{FakeEngine, FakeOp};

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let snapshot = health_snapshot_with(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        // The one try fails. The fake logs only the stops it carries out, so
        // a second try, which would succeed, would show in `stop_calls`.
        engine.fail_next(FakeOp::Stop, EngineError::Timeout);
        reconciler.reconcile_once(Some(&snapshot)).await;
        let lines = store
            .query_deployment_lines("dep_1", Some("health"))
            .await
            .unwrap();
        assert!(
            lines.iter().any(|line| line
                .message
                .starts_with("could not stop the VM for a health restart")),
            "{lines:?}"
        );

        // More failed probes and passes: no second try in this run.
        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
        }
        assert!(engine.stop_calls().is_empty(), "no second try");
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].observed_status, ObservedStatus::Running);
        assert_eq!(statuses[0].health, Health::Failing, "still reported");
        server.abort();
    }

    /// A queued health restart that a pass drops without trying a stop (here
    /// because `restart` went false for a while) is asked for again by the
    /// next failure counted for the run.
    #[tokio::test]
    async fn a_dropped_health_restart_is_asked_again_at_the_next_counted_failure() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1));
        let restarting = health_snapshot_with(port, check(0, None));
        let reporting = health_snapshot_with(port, check(0, Some(false)));

        reconciler.reconcile_once(Some(&restarting)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&restarting)).await;
        // The request is queued; this snapshot turns restarts off.
        reconciler.reconcile_once(Some(&reporting)).await;
        assert!(engine.stop_calls().is_empty(), "restart is false");

        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&restarting)).await;
        reconciler.reconcile_once(Some(&restarting)).await;
        assert_eq!(engine.stop_calls().len(), 1, "asked for again");
        server.abort();
    }

    /// [`health_snapshot_with`], with the VM attaching the volume
    /// `vol_health`, which a backup's pause marks (D-0259).
    fn health_snapshot_with_volume(port: u16, check: quoro_proto::wire::HealthCheck) -> Snapshot {
        health_snapshot_attaching(port, check, "vol_health", &["vol_health"])
    }

    /// [`health_snapshot_with`], with the VM attaching `attached` and
    /// `present` in `spec.volumes`.
    fn health_snapshot_attaching(
        port: u16,
        check: quoro_proto::wire::HealthCheck,
        attached: &str,
        present: &[&str],
    ) -> Snapshot {
        let mut snapshot = health_snapshot_with(port, check);
        let spec = &mut snapshot.state.spec;
        spec.vms[0].volumes.push(quoro_proto::wire::SpecVolume {
            volume_id: attached.to_owned(),
            serial: "quoro-vol-0".to_owned(),
            mount_path: "/data".to_owned(),
        });
        for volume_id in present {
            spec.volumes.push(SpecVolumeFile {
                volume_id: (*volume_id).to_owned(),
                app_id: "app_1".to_owned(),
                size_gb: 1,
                state: quoro_proto::wire::SpecVolumeState::Present,
                restore: None,
                warm: None,
            });
        }
        snapshot
    }

    /// #524: until a recreate for a new set of volumes is done, the VM that
    /// runs still attaches its old ones, and a backup of one of them pauses
    /// it: it is not probed, so its check does not turn `failing`.
    #[tokio::test]
    async fn a_vm_paused_through_a_volume_it_attaches_until_its_recreate_is_not_probed() {
        use quoro_engine::FakeEngine;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let healthy = Arc::new(AtomicBool::new(true));
        let requests = Arc::new(AtomicUsize::new(0));
        let paused = crate::health::PausedVms::default();
        let (port, server) = pausing_server(
            Arc::clone(&healthy),
            paused.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&requests),
        )
        .await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let before = health_snapshot_attaching(port, check(0, None), "vol_old", &["vol_old"]);
        reconciler.reconcile_once(Some(&before)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&before)).await;
        assert_eq!(statuses[0].health, Health::Passing);

        // The spec moves the VM to another volume, and the stop of its
        // recreate keeps failing, so the VM keeps running on `vol_old`.
        engine.set_unkillable(&VmId("vm_health".to_owned()), true);
        let after = health_snapshot_attaching(
            port,
            check(0, None),
            "vol_health",
            &["vol_old", "vol_health"],
        );
        let mark = paused.mark("vol_old");
        healthy.store(false, Ordering::SeqCst);
        let probed = requests.load(Ordering::SeqCst);
        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            let statuses = reconciler.reconcile_once(Some(&after)).await;
            assert_eq!(statuses[0].health, Health::Passing);
        }
        assert_eq!(requests.load(Ordering::SeqCst), probed, "not probed");
        assert!(app_events(&store).await.is_empty(), "no app.down");
        drop(mark);
        server.abort();
    }

    /// #524: a VM is held back only by a backup of a volume it attaches. A
    /// pause of another volume does not hold its health restart.
    #[tokio::test]
    async fn a_backup_of_a_volume_the_vm_does_not_attach_does_not_hold_its_restart() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let paused = crate::health::PausedVms::default();
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let snapshot = health_snapshot_with_volume(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        // This pass's probe fails and asks for a restart; then a backup
        // pauses another app's VM.
        reconciler.reconcile_once(Some(&snapshot)).await;
        let other = paused.mark("vol_other");
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1, "restarted for its health");
        drop(other);
        server.abort();
    }

    /// D-0259: a VM a backup paused is not restarted for its health. A
    /// restart asked before the pause is dropped, none is carried out while
    /// the VM stays paused, and after the pause a counted failure restarts it.
    #[tokio::test]
    async fn a_vm_paused_for_a_backup_is_not_restarted_for_its_health() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let paused = crate::health::PausedVms::default();
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let snapshot = health_snapshot_with_volume(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        // This pass's probe fails and asks for a restart; then a backup
        // pauses the VM before the next pass.
        reconciler.reconcile_once(Some(&snapshot)).await;
        let mark = paused.mark("vol_health");
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert!(
            engine.stop_calls().is_empty(),
            "the paused VM is not stopped"
        );

        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
            reconciler.reconcile_once(Some(&snapshot)).await;
        }
        assert!(engine.stop_calls().is_empty(), "nor while it stays paused");

        // Resumed: the next counted failure, one with this check, restarts it.
        drop(mark);
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        server.abort();
    }

    /// D-0259: when a paused VM is resumed its failures count from zero, so
    /// the failures before the pause do not add up with those after it.
    #[tokio::test]
    async fn the_failure_count_starts_from_zero_when_a_paused_vm_resumes() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let paused = crate::health::PausedVms::default();
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let two_failures = quoro_proto::wire::HealthCheck {
            failures: Some(2),
            ..check(0, None)
        };
        let snapshot = health_snapshot_with_volume(port, two_failures);

        reconciler.reconcile_once(Some(&snapshot)).await;
        // One failure before the pause.
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        // The pause, over before the next probe.
        let mark = paused.mark("vol_health");
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        drop(mark);

        // One failure after it: one in a row, not two.
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_ne!(statuses[0].health, Health::Failing);
        assert!(engine.stop_calls().is_empty());

        // A second one after it: failing, and restarted.
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        server.abort();
    }

    /// A health server for `vm_health` that counts the requests it gets.
    /// While `pause_in_probe` is set, a pause of the VM's volume begins and
    /// ends before each answer, as a backup's would while the probe waits.
    async fn pausing_server(
        healthy: Arc<std::sync::atomic::AtomicBool>,
        paused: crate::health::PausedVms,
        pause_in_probe: Arc<std::sync::atomic::AtomicBool>,
        requests: Arc<std::sync::atomic::AtomicUsize>,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = [0u8; 512];
                let _ = socket.read(&mut buffer).await;
                requests.fetch_add(1, Ordering::SeqCst);
                if pause_in_probe.load(Ordering::SeqCst) {
                    drop(paused.mark("vol_health"));
                }
                let status = if healthy.load(Ordering::SeqCst) {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let _ = socket
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
                    .await;
            }
        });
        (port, server)
    }

    /// D-0259: a restart asked just before a pause that is over by the next
    /// pass is dropped too. Its failures are from before the pause, and once
    /// a paused VM resumes they count from zero, whatever the pause's length.
    #[tokio::test]
    async fn a_restart_asked_before_a_short_pause_is_dropped_when_the_vm_resumes() {
        use quoro_engine::FakeEngine;

        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (port, server) = switchable_server(Arc::clone(&healthy)).await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let paused = crate::health::PausedVms::default();
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let snapshot = health_snapshot_with_volume(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        // This pass's probe fails and asks for a restart; a backup pauses
        // the VM and resumes it before the next pass.
        reconciler.reconcile_once(Some(&snapshot)).await;
        drop(paused.mark("vol_health"));
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert!(
            engine.stop_calls().is_empty(),
            "not stopped for failures from before the pause"
        );

        // A failure after the pause counts, and asks again.
        tokio::time::sleep(NEXT_PROBE).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        server.abort();
    }

    /// D-0259: a paused VM is not probed, so its check does not turn
    /// `failing` and emits no `app.down` while the pause lasts. The first
    /// failure after the pause counts.
    #[tokio::test]
    async fn a_paused_vm_is_not_probed_and_does_not_turn_failing() {
        use quoro_engine::FakeEngine;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let healthy = Arc::new(AtomicBool::new(true));
        let requests = Arc::new(AtomicUsize::new(0));
        let paused = crate::health::PausedVms::default();
        let (port, server) = pausing_server(
            Arc::clone(&healthy),
            paused.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&requests),
        )
        .await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let snapshot = health_snapshot_with_volume(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Passing);

        // A backup pauses the VM, which would fail every probe now.
        let mark = paused.mark("vol_health");
        healthy.store(false, Ordering::SeqCst);
        let probed = requests.load(Ordering::SeqCst);
        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
            assert_eq!(statuses[0].health, Health::Passing);
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            probed,
            "a paused VM is not probed"
        );
        assert!(app_events(&store).await.is_empty(), "no app.down");
        assert!(engine.stop_calls().is_empty());

        drop(mark);
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        assert_eq!(app_events(&store).await, ["app.down"]);
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        server.abort();
    }

    /// D-0259: a failed probe that a pause overlapped does not count, even
    /// when the pause began and ended while the probe waited for its answer.
    #[tokio::test]
    async fn a_failed_probe_a_pause_overlapped_does_not_count() {
        use quoro_engine::FakeEngine;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let requests = Arc::new(AtomicUsize::new(0));
        let pause_in_probe = Arc::new(AtomicBool::new(true));
        let paused = crate::health::PausedVms::default();
        let (port, server) = pausing_server(
            Arc::new(AtomicBool::new(false)),
            paused.clone(),
            Arc::clone(&pause_in_probe),
            Arc::clone(&requests),
        )
        .await;
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let engine = Arc::new(FakeEngine::new());
        let mut reconciler =
            health_reconciler(Arc::clone(&engine), store.clone(), Duration::from_millis(1))
                .with_paused_vms(paused.clone());
        let snapshot = health_snapshot_with_volume(port, check(0, None));

        reconciler.reconcile_once(Some(&snapshot)).await;
        for _ in 0..2 {
            tokio::time::sleep(NEXT_PROBE).await;
            let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
            assert_ne!(statuses[0].health, Health::Failing);
        }
        assert_eq!(requests.load(Ordering::SeqCst), 2, "both were probed");
        assert!(engine.stop_calls().is_empty());

        // The same failure with no pause during the probe counts.
        pause_in_probe.store(false, Ordering::SeqCst);
        tokio::time::sleep(NEXT_PROBE).await;
        let statuses = reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(statuses[0].health, Health::Failing);
        reconciler.reconcile_once(Some(&snapshot)).await;
        assert_eq!(engine.stop_calls().len(), 1);
        server.abort();
    }
}
