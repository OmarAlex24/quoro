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
use quoro_proto::wire::{DesiredState, DesiredStatus, Health, ObservedStatus, SpecVm, VmStatus};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::store::NodeStore;

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
}

impl<E: Engine> Reconciler<E> {
    /// Builds a reconciler and the channel it publishes observations on.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        config: ReconcileConfig,
        options: ReconcileOptions,
    ) -> (Self, watch::Receiver<Vec<VmStatus>>) {
        let (observed, receiver) = watch::channel(Vec::new());
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
            },
            receiver,
        )
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

        if reached
            && snapshot.rev > self.store.applied_rev().await.unwrap_or(0)
            && let Err(e) = self.store.set_applied_rev(snapshot.rev).await
        {
            warn!(error = %e, "could not record the applied revision");
        }

        self.publish().await
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
                        info = started;
                    }
                    Err(e) => {
                        warn!(vm_id, error = ?e, "could not start the VM after its restart");
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
                                info = started;
                            }
                            Err(e) => {
                                warn!(vm_id, error = ?e, "could not start the VM");
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
        let prepared = match self.engine.prepare_image(&image).await {
            Ok(prepared) => prepared,
            Err(e) => {
                warn!(vm_id, image = %vm.image, error = ?e, "could not prepare the VM image");
                return None;
            }
        };

        let ip = self.address_for(&vm_id, subnet).await?;
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
            console: ConsoleMode::Off,
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
                info!(vm_id, %ip, project = %vm.project_id, "VM created");
                Some(info)
            }
            Err(e) => {
                warn!(vm_id, error = ?e, "could not create the VM");
                None
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
                    let record = self
                        .store
                        .vm_record(&info.id.0)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    statuses.push(VmStatus {
                        vm_id: info.id.0.clone(),
                        observed_status: observed_status(info.state),
                        // Health checks arrive after M2.
                        health: Health::Unknown,
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
}
