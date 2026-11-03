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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use quoro_engine::{
    ConsoleMode, Engine, EngineError, EngineEvent, EventStream, ImageConfig, ImageDigest, ImageRef,
    InitConfig, MacAddr, NetAttach, NodeNetwork, StopMode, VmId, VmInfo, VmSpec, VmState,
};
use quoro_proto::wire::{
    DesiredState, DesiredStatus, Health, ObservedStatus, SpecRoute, SpecVm, VmStatus,
};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::health::{FIRST_PROBE_DELAY, HealthTarget, is_passing, probe};
use crate::logs::LogCollector;
use crate::store::NodeStore;
use crate::util;

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
    /// First crash-restart backoff (1 s).
    pub backoff_base: Duration,
    /// Crash-restart backoff ceiling (5 min).
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct NetworkState {
    subnet: String,
    gateway: Ipv4Addr,
    dns: Vec<Ipv4Addr>,
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

/// One VM's health-check state.
#[derive(Clone, Debug)]
struct HealthState {
    target: HealthTarget,
    status: Health,
    failures: u32,
    next_at: Instant,
}

/// A parsed IPv4 subnet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Subnet {
    network: u32,
    prefix: u8,
    gateway: Ipv4Addr,
}

impl Subnet {
    fn parse(cidr: &str, gateway: &str) -> Option<Self> {
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
    fn allocate(&self, used: &HashSet<Ipv4Addr>) -> Ipv4Addr {
        let broadcast = self.broadcast();
        for raw in self.network.saturating_add(1)..broadcast {
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
    /// The app-route supervisor, when the agent runs one (worker hook for
    /// `M2` apps). `None` in tests that only exercise the engine.
    ingress: Option<Arc<crate::ingress::IngressSupervisor>>,
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
                once_only: Vec::new(),
                published: None,
                collector,
                node_id: None,
                spec_meta: HashMap::new(),
                health: HashMap::new(),
                ingress: None,
            },
            receiver,
        )
    }

    /// Attaches the app-route supervisor: every applied snapshot and every
    /// change of a VM's address feeds it the complete route set.
    #[must_use]
    pub fn with_ingress(mut self, ingress: Arc<crate::ingress::IngressSupervisor>) -> Self {
        self.ingress = Some(ingress);
        self
    }

    /// Sets the `nod_` id node-originated events carry. Without it those
    /// events are not emitted.
    #[must_use]
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = Some(node_id);
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
                    self.health_pass(current.as_ref()).await;
                    self.publish().await;
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

        let Some(subnet) = Subnet::parse(&spec.network.subnet, &spec.network.gateway) else {
            warn!(
                subnet = %spec.network.subnet,
                gateway = %spec.network.gateway,
                "the desired network is not a usable subnet; nothing is applied this pass"
            );
            return self.publish().await;
        };

        let network = NetworkState {
            subnet: spec.network.subnet.clone(),
            gateway: subnet.gateway,
            dns: self.config.dns.clone(),
        };
        if self.network_failed || self.network.as_ref() != Some(&network) {
            let request = NodeNetwork {
                bridge: self.config.bridge.clone(),
                subnet: network.subnet.clone(),
                gateway: network.gateway,
                dns: network.dns.clone(),
            };
            match self.engine.ensure_network(&request).await {
                Ok(()) => {
                    info!(
                        bridge = %request.bridge,
                        subnet = %request.subnet,
                        "node network ensured"
                    );
                    self.network = Some(network);
                    self.network_failed = false;
                }
                Err(e) => {
                    warn!(error = ?e, "could not ensure the node network; no VM is created this pass");
                    self.network_failed = true;
                }
            }
        }
        let network_ok = !self.network_failed && self.network.is_some();

        let mut reached = true;
        let mut vms: Vec<&SpecVm> = spec.vms.iter().collect();
        vms.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));

        for vm in vms {
            if !self.reconcile_vm(vm, &subnet, network_ok).await {
                reached = false;
            }
        }

        let desired: HashSet<&str> = spec.vms.iter().map(|vm| vm.vm_id.as_str()).collect();
        if !self.remove_extra_vms(&desired).await {
            reached = false;
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

        if reached
            && snapshot.rev > self.store.applied_rev().await.unwrap_or(0)
            && let Err(e) = self.store.set_applied_rev(snapshot.rev).await
        {
            warn!(error = %e, "could not record the applied revision");
        }

        self.health_pass(Some(snapshot)).await;
        self.publish().await
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

        if info.is_none() {
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

        if let Some(known) = self.known_digest(&vm_id).await
            && !known.is_empty()
            && known != vm.image_digest
        {
            warn!(
                vm_id,
                known, requested = %vm.image_digest,
                "a VM's image digest is create-time only; deploy a new VM id to change it"
            );
        }

        let applied_generation = self
            .store
            .vm_record(&vm_id)
            .await
            .ok()
            .flatten()
            .map_or(0, |record| record.restart_generation);

        if vm.restart_generation > applied_generation {
            if matches!(info.state, VmState::Running | VmState::Starting) {
                match self.stop_vm(&id).await {
                    Ok(stopped) => info = stopped,
                    Err(e) => {
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
            if vm.desired_status == DesiredStatus::Running {
                match self.engine.start(&id).await {
                    Ok(started) => {
                        info!(vm_id, "VM restarted for a new restart_generation");
                        self.deployment_line(vm, "boot", "VM started").await;
                        info = started;
                    }
                    Err(e) => {
                        warn!(vm_id, error = ?e, "could not start the VM after its restart");
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
                        || (was_running && info.state == VmState::Stopped);
                    if unexpected_exit {
                        if let Some(code) = info.exit_code
                            && let Err(e) = self
                                .store
                                .set_exit_code(&vm_id, Some(i64::from(code)))
                                .await
                        {
                            warn!(vm_id, error = %e, "could not record the exit code");
                        }
                        match self.crash_restart(&vm_id, &id, info.exit_code).await {
                            Ok(Some(started)) => info = started,
                            Ok(None) => reached = false,
                            Err(e) => {
                                warn!(vm_id, error = ?e, "could not restart the crashed VM");
                                reached = false;
                            }
                        }
                    } else {
                        match self.engine.start(&id).await {
                            Ok(started) => {
                                debug!(vm_id, "VM started");
                                self.deployment_line(vm, "boot", "VM started").await;
                                info = started;
                            }
                            Err(e) => {
                                warn!(vm_id, error = ?e, "could not start the VM");
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
                    match self.stop_vm(&id).await {
                        Ok(stopped) => {
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
                        Err(e) => {
                            warn!(vm_id, error = ?e, "could not stop the VM");
                            reached = false;
                        }
                    }
                }
                if let Some(timing) = self.timings.get_mut(&vm_id) {
                    timing.retry_at = None;
                }
            }
        }

        if info.state == VmState::Running {
            let now = Instant::now();
            let healthy_after = self.options.healthy_after;
            let base = self.options.backoff_base;
            let timing = self.timings.entry(vm_id.clone()).or_insert(VmTiming {
                retry_at: None,
                backoff: base,
                running_since: None,
            });
            match timing.running_since {
                None => timing.running_since = Some(now),
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
                let error = e.to_string();
                self.deployment_line(vm, "pull", &error).await;
                self.deployment_failed(vm, "pull", &error).await;
                return None;
            }
        };
        self.deployment_line(
            vm,
            "rootfs",
            &format!("rootfs ready ({})", prepared.digest.0),
        )
        .await;

        let ip = self.address_for(&vm_id, subnet).await?;
        let console = match self.store.console_socket(&vm_id) {
            Ok(socket) => ConsoleMode::VirtioConsole { socket },
            Err(e) => {
                warn!(vm_id, error = %e, "could not prepare the VM's console socket");
                return None;
            }
        };
        let spec = VmSpec {
            id: id.clone(),
            vcpus: u8::try_from(vm.vcpus.max(1)).unwrap_or(u8::MAX),
            memory_mb: vm.memory_mb,
            rootfs: prepared.digest.clone(),
            disk_gb: vm.disk_gb,
            volumes: Vec::new(),
            net: NetAttach {
                ip,
                gateway: subnet.gateway,
                mac: mac_for(ip),
                // VMs of one project reach each other; VMs of two projects do
                // not (engine-trait.md section 3.3, D-0084).
                isolation_group: Some(vm.project_id.clone()),
            },
            init: init_config(&prepared.config, &vm.env),
            console,
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

    /// Probes every running VM whose health check is due and records changes.
    ///
    /// Runs from the agent loop every 500 ms and at the end of a reconcile
    /// pass, so a deployment sees its first pass about a second after the VM
    /// reaches running.
    pub async fn health_pass(&mut self, snapshot: Option<&Snapshot>) {
        let Some(snapshot) = snapshot else {
            return;
        };
        let infos = match self.engine.list().await {
            Ok(infos) => infos,
            Err(e) => {
                warn!(error = ?e, "could not list VMs for health checks");
                return;
            }
        };
        let running: HashSet<String> = infos
            .iter()
            .filter(|info| info.state == VmState::Running)
            .map(|info| info.id.0.clone())
            .collect();

        let mut targets: HashMap<String, (HealthTarget, Option<String>)> = HashMap::new();
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
                targets.insert(vm.vm_id.clone(), (target, vm.deployment_id.clone()));
            }
        }

        // Forget VMs that stopped, were removed or lost their port.
        self.health
            .retain(|vm_id, _| running.contains(vm_id) && targets.contains_key(vm_id));

        let now = Instant::now();
        let ids: Vec<String> = targets.keys().cloned().collect();
        for vm_id in ids {
            let Some((target, deployment_id)) = targets.remove(&vm_id) else {
                continue;
            };
            let mut state = self.health.remove(&vm_id).unwrap_or_else(|| HealthState {
                target: target.clone(),
                status: Health::Unknown,
                failures: 0,
                next_at: now + FIRST_PROBE_DELAY,
            });
            if state.target != target {
                state.target = target.clone();
                state.status = Health::Unknown;
                state.failures = 0;
                state.next_at = now + FIRST_PROBE_DELAY;
            }
            if now < state.next_at {
                self.health.insert(vm_id, state);
                continue;
            }

            let previous = state.status;
            let outcome = probe(&state.target).await;
            let (status, passing, reason) = match &outcome {
                Ok(code) if is_passing(*code) => (Health::Passing, true, None),
                Ok(code) => {
                    state.failures = state.failures.saturating_add(1);
                    let status = if state.failures >= state.target.failures {
                        Health::Failing
                    } else {
                        previous
                    };
                    (status, false, Some(format!("status {code}")))
                }
                Err(reason) => {
                    state.failures = state.failures.saturating_add(1);
                    let status = if state.failures >= state.target.failures {
                        Health::Failing
                    } else {
                        previous
                    };
                    (status, false, Some(reason.clone()))
                }
            };
            if passing {
                state.failures = 0;
            }
            state.status = status;
            state.next_at = now + state.target.interval;
            let changed = status != previous;
            self.health.insert(vm_id.clone(), state);

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
    async fn stop_vm(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.engine
            .stop(
                id,
                StopMode::Graceful {
                    timeout: self.options.stop_timeout,
                },
            )
            .await
    }

    /// Restarts a VM that exited on its own, with exponential backoff.
    ///
    /// Returns `Ok(None)` while the backoff is still running.
    async fn crash_restart(
        &mut self,
        vm_id: &str,
        id: &VmId,
        exit_code: Option<i32>,
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

        match self.engine.start(id).await {
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
                let timing = self.timings.entry(vm_id.to_owned()).or_default();
                timing.retry_at = Some(now + timing.backoff);
                timing.backoff = (timing.backoff * 2).min(max);
                Err(e)
            }
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
            ) && let Err(e) = self.engine.stop(&info.id, StopMode::Force).await
            {
                warn!(vm_id, error = ?e, "could not force-stop the removed VM");
                reached = false;
                continue;
            }
            match self.engine.delete(&info.id).await {
                Ok(()) => {
                    if let Err(e) = self.store.forget_vm(&vm_id).await {
                        warn!(vm_id, error = %e, "could not forget the removed VM");
                    }
                    self.last_states.remove(&vm_id);
                    if let Err(e) = self.store.set_meta(&digest_key(&vm_id), "").await {
                        warn!(vm_id, error = %e, "could not forget the removed VM's digest");
                    }
                    self.timings.remove(&vm_id);
                    self.health.remove(&vm_id);
                    self.collector.detach(&vm_id);
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

/// The `node_meta` key that remembers one VM's create-time image digest.
fn digest_key(vm_id: &str) -> String {
    format!("image_digest:{vm_id}")
}

/// The `node_meta` key that remembers a `deployment.failed` was emitted.
fn deployment_failed_key(deployment_id: &str) -> String {
    format!("deployment_failed:{deployment_id}")
}

/// The init configuration: the image's own command line, overlaid with the
/// spec's environment by name, in a deterministic (name) order.
fn init_config(image: &ImageConfig, spec_env: &BTreeMap<String, String>) -> InitConfig {
    let mut env: BTreeMap<String, String> = image.env.iter().cloned().collect();
    for (name, value) in spec_env {
        env.insert(name.clone(), value.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let init = init_config(&image, &overrides);
        assert_eq!(
            init.env,
            vec![
                ("A".to_owned(), "spec".to_owned()),
                ("B".to_owned(), "image".to_owned())
            ]
        );
        assert_eq!(init.entrypoint, vec!["/app".to_owned()]);
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

    fn health_snapshot(port: u16) -> Snapshot {
        use quoro_proto::wire::{HealthCheck, Spec, SpecNetwork};

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
                        health_check: Some(HealthCheck {
                            path: Some("/".to_owned()),
                            interval_s: Some(1),
                            timeout_s: Some(1),
                            failures: Some(1),
                        }),
                        desired_status: DesiredStatus::Running,
                        restart_generation: 0,
                        stateful: false,
                        lease_id: None,
                        fencing_token: None,
                        volumes: Vec::new(),
                    }],
                    routes: Vec::new(),
                    network: SpecNetwork {
                        subnet: "127.0.0.0/8".to_owned(),
                        gateway: "127.0.0.2".to_owned(),
                    },
                    ingress: None,
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
}
