//! The in-process node driver (M1): the primary is the only node and there is
//! no node-agent session yet (that is M2), so the binary itself owns the
//! engine and reconciles every VM this node was told about.
//!
//! The driver only talks to the control plane through [`driver`]:
//! it reads desired state with `desired_vm`, makes the engine match it, and
//! writes observations back with `report_observed` / `report_deleted`. From M2
//! the node-agent session calls the same functions, so no service changes.
//!
//! The loop never dies on one VM: every engine or database error is logged and
//! (where it is an engine error the control plane can see) reported as
//! `unknown`, so the next notification retries the VM and the rest of the node
//! keeps working.
//!
//! The driver is generic over two seams so the same loop serves the fake
//! engine in tests and the Cloud Hypervisor backend on a Linux node: the
//! [`Engine`] itself and an [`ImageResolver`] that turns the image reference
//! the control plane stores into the content digest the engine caches by.

use quoro_control_plane::driver::{
    self, Capacity, DesiredStatus, Kvm, LocalNode, Observation, ObservedStatus,
};
use quoro_control_plane::error::ApiError;
use quoro_control_plane::store::Store;
use quoro_engine::{
    ConsoleMode, Engine, EngineEvent, EventStream, ImageDigest, ImageRef, InitConfig, MacAddr,
    NetAttach, NodeNetwork, StopMode, VmId, VmSpec, VmState,
};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// How long a graceful stop may wait for the guest.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// The node network the driver hands guest addresses out of and attaches VMs
/// to.
///
/// It is passed in rather than compiled in so `quoro.toml` can move the node
/// onto an address range that does not collide with the operator's own
/// networks. The subnet is carried both as the CIDR string the engine records
/// and as its parsed parts, because address allocation works on numbers.
#[derive(Clone, Debug)]
pub struct NodeNetworkConfig {
    /// Bridge every VM is attached to.
    pub bridge: String,
    /// The subnet as a CIDR string, for example `10.42.0.0/24`.
    pub subnet: String,
    /// The network address of that subnet (`10.42.0.0` in the example).
    pub network: Ipv4Addr,
    /// The prefix length of that subnet.
    pub prefix: u8,
    /// The node's own address on the bridge, never handed to a VM.
    pub gateway: Ipv4Addr,
    /// DNS servers handed to guests, from the node's configuration.
    pub dns: Vec<Ipv4Addr>,
}

impl From<&crate::config::CloudHypervisorConfig> for NodeNetworkConfig {
    fn from(config: &crate::config::CloudHypervisorConfig) -> Self {
        Self {
            bridge: config.bridge.clone(),
            subnet: config.subnet.to_string(),
            network: config.subnet.network(),
            prefix: config.subnet.prefix(),
            gateway: config.subnet.gateway(),
            dns: config.dns(),
        }
    }
}

impl NodeNetworkConfig {
    /// The last address of the subnet (all host bits set), the broadcast
    /// address no VM may take.
    fn broadcast(&self) -> Ipv4Addr {
        let host_bits = 32 - u32::from(self.prefix.min(32));
        let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
        Ipv4Addr::from(u32::from(self.network) | !mask)
    }
}

/// Turns an image reference into the content digest the engine caches by.
///
/// The control plane stores whatever reference the user typed; the engine
/// needs a digest, so the driver asks this seam for one before preparing the
/// image. Keeping it a trait lets the fake engine keep treating the reference
/// as the digest, while a real node resolves it against a registry.
pub trait ImageResolver: Send + Sync + 'static {
    /// Resolves `reference` to an image digest.
    fn resolve(&self, reference: &str) -> impl Future<Output = Result<ImageDigest, String>> + Send;
}

/// Treats the reference as the digest, which is what the fake engine caches
/// by. It keeps the end-to-end test offline and unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReferenceAsDigest;

impl ImageResolver for ReferenceAsDigest {
    async fn resolve(&self, reference: &str) -> Result<ImageDigest, String> {
        Ok(ImageDigest(reference.to_owned()))
    }
}

/// Resolves a reference against its OCI registry: a digest that is already
/// pinned is used as is, a tag is turned into the manifest digest it points at.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Default)]
pub struct RegistryResolver;

#[cfg(target_os = "linux")]
impl ImageResolver for RegistryResolver {
    async fn resolve(&self, reference: &str) -> Result<ImageDigest, String> {
        quoro_engine::image::resolve_digest(reference)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Registers this machine as the primary node, resyncs every VM it knows
/// about and starts the reconciliation loop.
///
/// `resolver` turns the control plane's image references into the digests the
/// engine caches by; `network` is the node network addresses are handed out
/// of.
///
/// Returns the `nod_` id and the loop's task handle.
///
/// # Errors
///
/// A database error while registering the node or reading the initial VM list.
pub async fn start<E: Engine + 'static, R: ImageResolver>(
    engine: E,
    resolver: R,
    network: NodeNetworkConfig,
    store: Store,
    rx: mpsc::UnboundedReceiver<String>,
    node_name: String,
) -> Result<(String, JoinHandle<()>), ApiError> {
    let capabilities = engine.capabilities().await;
    let probe = match engine.probe_host().await {
        Ok(probe) => Some(probe),
        // A probe failure says nothing about whether the control plane should
        // serve, so the node still registers and reports KVM as unavailable.
        Err(e) => {
            warn!(error = ?e, "host probe failed; reporting KVM as unavailable");
            None
        }
    };

    let node = LocalNode {
        name: node_name,
        capacity: Capacity {
            cpus: std::thread::available_parallelism()
                .map(|n| u32::try_from(n.get()).unwrap_or(u32::MAX))
                .unwrap_or(0),
            memory_mb: memory_mb(),
            // The free-space probe needs `statvfs`, which the standard library
            // does not expose and the binary has no libc dependency; a later
            // milestone reports the real disk figure.
            disk_gb: 0,
        },
        kvm: Kvm {
            available: probe.as_ref().is_some_and(|probe| probe.kvm_available),
            kernel: probe
                .as_ref()
                .map(|probe| probe.kernel_release.clone())
                .unwrap_or_default(),
            nested: probe.map(|probe| probe.nested),
        },
        engine_version: capabilities.version,
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let node_id = driver::register_local_node(&store, &node).await?;
    info!(engine = %capabilities.engine, "local node registered");

    let engine_network = NodeNetwork {
        bridge: network.bridge.clone(),
        subnet: network.subnet.clone(),
        gateway: network.gateway,
        dns: network.dns.clone(),
    };
    if let Err(e) = engine.ensure_network(&engine_network).await {
        // A bridge usually needs root; the driver keeps running so the node
        // stays visible and the failure is reported on the next start.
        error!(error = ?e, "could not ensure the node network");
    }

    // Resync: every VM row placed on this node, plus every VM the engine still
    // has from a previous run, so a VM whose row was deleted is removed and a
    // VM that is already created is not created twice.
    let mut ids: Vec<String> = driver::vms_on_node(&store, &node_id).await?;
    match engine.list().await {
        Ok(vms) => ids.extend(vms.into_iter().map(|info| info.id.0)),
        Err(e) => error!(error = ?e, "could not list the engine's VMs at start"),
    }
    let mut seen = HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));

    let driver = NodeDriver {
        engine,
        resolver,
        store,
        node_id: node_id.clone(),
        ips: HashMap::new(),
        network,
    };
    let handle = tokio::spawn(driver.run(ids, rx));
    Ok((node_id, handle))
}

/// MemTotal from `/proc/meminfo` in mebibytes, or 0 when it is not readable.
fn memory_mb() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .unwrap_or(0)
                / 1024;
        }
    }
    0
}

/// What woke the loop.
enum Input {
    /// The control plane changed a VM's desired state.
    Vm(String),
    /// The engine reported a lifecycle event.
    Event(EngineEvent),
}

/// The driver's state: the engine, the image resolver, the database, its node
/// id, the node network and the addresses it has handed out.
struct NodeDriver<E: Engine, R: ImageResolver> {
    engine: E,
    /// Turns the control plane's image reference into a digest per create.
    resolver: R,
    store: Store,
    node_id: String,
    /// Address per VM id, so a VM keeps its address across reconciles and the
    /// lowest free one is reused after a delete.
    ips: HashMap<String, Ipv4Addr>,
    /// The subnet addresses are handed out of and the bridge they live on.
    network: NodeNetworkConfig,
}

impl<E: Engine, R: ImageResolver> NodeDriver<E, R> {
    /// Reconciles the initial set, then every notification and engine event
    /// until the control plane closes the channel.
    async fn run(mut self, initial: Vec<String>, mut rx: mpsc::UnboundedReceiver<String>) {
        let mut events = self.engine.events();
        for vm_id in initial {
            self.reconcile(&vm_id).await;
        }
        loop {
            // The futures borrow only the channel and the stream, so the
            // driver itself is free once one of them completes. Reconciles are
            // therefore sequential, which serializes them per VM as the brief
            // requires and keeps one VM's failure away from the others.
            let input = tokio::select! {
                Some(vm_id) = rx.recv() => Input::Vm(vm_id),
                Some(event) = next_event(&mut events) => Input::Event(event),
                else => break,
            };
            match input {
                Input::Vm(vm_id) => self.reconcile(&vm_id).await,
                Input::Event(event) => self.on_event(event).await,
            }
        }
        info!("driver channel closed; the local node driver stops");
    }

    /// Makes the engine match one VM's desired state.
    async fn reconcile(&mut self, vm_id: &str) {
        let desired = match driver::desired_vm(&self.store, vm_id).await {
            Ok(desired) => desired,
            Err(e) => {
                error!(vm_id, error = ?e, "could not read the desired state");
                return;
            }
        };
        let known = match self.engine.inspect(&VmId(vm_id.to_string())).await {
            Ok(info) => Some(info),
            Err(quoro_engine::EngineError::NotFound) => None,
            Err(e) => {
                error!(vm_id, error = ?e, "could not inspect the VM on the engine");
                return;
            }
        };

        let Some(desired) = desired else {
            self.remove(vm_id, known.is_some()).await;
            return;
        };

        // After a restart the address map is empty: take back the address the
        // VM already has so a new VM is never given the same one.
        if known.is_some()
            && !self.ips.contains_key(vm_id)
            && let Some(ip) = desired.ip.as_deref().and_then(|ip| ip.parse().ok())
        {
            self.ips.insert(vm_id.to_string(), ip);
        }

        let mut state = known.as_ref().map(|info| info.state);
        if known.is_none() {
            let Some((created, ip)) = self.create(&desired).await else {
                return;
            };
            state = Some(created.state);
            if let Err(e) = self
                .report(
                    vm_id,
                    Observation {
                        status: ObservedStatus::Stopped,
                        ip: None,
                        exit_code: None,
                        restart_count: None,
                    },
                )
                .await
            {
                error!(vm_id, error = ?e, "could not report the created VM");
            }
            if created.state == VmState::Running {
                // A create that already booted has its address in the map; the
                // start below is then a no-op on the engine.
                self.ips.insert(vm_id.to_string(), ip);
            }
        }

        match desired.desired_status {
            DesiredStatus::Running => {
                if state != Some(VmState::Running) {
                    self.start(vm_id).await;
                }
            }
            DesiredStatus::Stopped => {
                if matches!(
                    state,
                    Some(VmState::Running | VmState::Starting | VmState::Stopping)
                ) {
                    self.stop(vm_id).await;
                }
            }
        }
    }

    /// Prepares the image, creates the VM and returns its engine record and the
    /// address it was given.
    async fn create(
        &mut self,
        desired: &driver::DesiredVm,
    ) -> Option<(quoro_engine::VmInfo, Ipv4Addr)> {
        let vm_id = desired.id.as_str();
        // The control plane stores the reference; the engine caches by digest.
        // A tag still resolves (that is the resolver's job), but a pinned
        // digest avoids a registry round-trip at every create.
        let digest = match self.resolver.resolve(&desired.image).await {
            Ok(digest) => digest,
            Err(e) => {
                error!(vm_id, image = %desired.image, error = %e, "could not resolve the VM image digest");
                self.report_unknown(vm_id).await;
                return None;
            }
        };
        let image = ImageRef {
            reference: desired.image.clone(),
            digest,
        };
        let prepared = match self.engine.prepare_image(&image).await {
            Ok(prepared) => prepared,
            Err(e) => {
                error!(vm_id, error = ?e, "could not prepare the VM image");
                self.report_unknown(vm_id).await;
                return None;
            }
        };

        let ip = self.allocate_ip();
        let spec = VmSpec {
            id: VmId(desired.id.clone()),
            vcpus: u8::try_from(desired.vcpus.max(1)).unwrap_or(u8::MAX),
            memory_mb: desired.memory_mb,
            rootfs: prepared.digest.clone(),
            disk_gb: desired.disk_gb,
            // Volumes arrive in M5.
            volumes: Vec::new(),
            net: NetAttach {
                ip,
                gateway: self.network.gateway,
                mac: mac_for(ip),
                // Every VM stays isolated until the node agent sets the
                // project as the group (ADR 0084).
                isolation_group: None,
            },
            init: InitConfig {
                entrypoint: prepared.config.entrypoint,
                cmd: prepared.config.cmd,
                env: prepared.config.env,
                user: prepared.config.user,
                workdir: prepared.config.workdir,
            },
            // `ConsoleMode`'s first variant: no console in M1; the console
            // endpoints arrive with the node agent in M2.
            console: ConsoleMode::Off,
        };

        match self.engine.create(&spec).await {
            Ok(info) => {
                self.ips.insert(desired.id.clone(), ip);
                info!(vm_id, %ip, "VM created on the engine");
                Some((info, ip))
            }
            Err(e) => {
                error!(vm_id, error = ?e, "could not create the VM");
                self.report_unknown(vm_id).await;
                None
            }
        }
    }

    /// Starts a created VM and reports `starting` then `running`.
    async fn start(&mut self, vm_id: &str) {
        if let Err(e) = self
            .report(
                vm_id,
                Observation {
                    status: ObservedStatus::Starting,
                    ip: None,
                    exit_code: None,
                    restart_count: None,
                },
            )
            .await
        {
            error!(vm_id, error = ?e, "could not report the starting VM");
        }

        match self.engine.start(&VmId(vm_id.to_string())).await {
            Ok(info) => {
                let ip = self.ips.get(vm_id).map(ToString::to_string);
                if let Err(e) = self
                    .report(
                        vm_id,
                        Observation {
                            status: ObservedStatus::Running,
                            ip,
                            exit_code: info.exit_code.map(i64::from),
                            restart_count: None,
                        },
                    )
                    .await
                {
                    error!(vm_id, error = ?e, "could not report the running VM");
                }
            }
            Err(e) => {
                error!(vm_id, error = ?e, "could not start the VM");
                self.report_unknown(vm_id).await;
            }
        }
    }

    /// Stops a running VM and reports `stopping` then `stopped`.
    async fn stop(&mut self, vm_id: &str) {
        if let Err(e) = self
            .report(
                vm_id,
                Observation {
                    status: ObservedStatus::Stopping,
                    ip: None,
                    exit_code: None,
                    restart_count: None,
                },
            )
            .await
        {
            error!(vm_id, error = ?e, "could not report the stopping VM");
        }

        match self
            .engine
            .stop(
                &VmId(vm_id.to_string()),
                StopMode::Graceful {
                    timeout: STOP_TIMEOUT,
                },
            )
            .await
        {
            Ok(info) => {
                if let Err(e) = self
                    .report(
                        vm_id,
                        Observation {
                            status: ObservedStatus::Stopped,
                            ip: None,
                            exit_code: info.exit_code.map(i64::from),
                            restart_count: None,
                        },
                    )
                    .await
                {
                    error!(vm_id, error = ?e, "could not report the stopped VM");
                }
            }
            Err(e) => {
                error!(vm_id, error = ?e, "could not stop the VM");
                self.report_unknown(vm_id).await;
            }
        }
    }

    /// Stops and deletes a VM whose row is gone, then tells the control plane.
    async fn remove(&mut self, vm_id: &str, engine_knows_it: bool) {
        let id = VmId(vm_id.to_string());
        if engine_knows_it {
            let running = self
                .engine
                .inspect(&id)
                .await
                .is_ok_and(|info| info.state == VmState::Running);
            if running && let Err(e) = self.engine.stop(&id, StopMode::Force).await {
                error!(vm_id, error = ?e, "could not force-stop the deleted VM");
                return;
            }
            if let Err(e) = self.engine.delete(&id).await {
                error!(vm_id, error = ?e, "could not delete the VM from the engine");
                return;
            }
            info!(vm_id, "VM deleted from the engine");
        }
        self.ips.remove(vm_id);
        if let Err(e) = driver::report_deleted(&self.store, &self.node_id, vm_id).await {
            error!(vm_id, error = ?e, "could not report the deleted VM");
        }
    }

    /// Reports the engine's lifecycle events: a crash or an exit the guest made
    /// on its own.
    async fn on_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::Started { id } => info!(vm_id = %id.0, "engine reported the VM started"),
            EngineEvent::Exited { id, code } => {
                let observation = Observation {
                    status: ObservedStatus::Stopped,
                    ip: None,
                    exit_code: Some(i64::from(code)),
                    restart_count: None,
                };
                if let Err(e) = self.report(&id.0, observation).await {
                    error!(vm_id = %id.0, error = ?e, "could not report the exited VM");
                }
            }
            EngineEvent::Crashed { id, reason } => {
                warn!(vm_id = %id.0, %reason, "the VM crashed");
                let observation = Observation {
                    status: ObservedStatus::Crashed,
                    ip: None,
                    exit_code: None,
                    restart_count: None,
                };
                if let Err(e) = self.report(&id.0, observation).await {
                    error!(vm_id = %id.0, error = ?e, "could not report the crashed VM");
                }
            }
        }
    }

    async fn report(&self, vm_id: &str, observation: Observation) -> Result<(), ApiError> {
        driver::report_observed(&self.store, &self.node_id, vm_id, &observation).await
    }

    /// Reports `unknown` after an engine error. A failure to report is logged
    /// only: there is nothing else the loop can do about it.
    async fn report_unknown(&self, vm_id: &str) {
        let observation = Observation {
            status: ObservedStatus::Unknown,
            ip: None,
            exit_code: None,
            restart_count: None,
        };
        if let Err(e) = self.report(vm_id, observation).await {
            error!(vm_id, error = ?e, "could not report the failed VM");
        }
    }

    /// The lowest host address of the subnet that is neither the gateway nor
    /// the broadcast address and is not already handed out.
    ///
    /// The network address, the gateway (the first host) and the broadcast
    /// address are never usable by a guest; everything between them is, so a
    /// `/29` gives the guests `.2` through `.6`.
    fn allocate_ip(&self) -> Ipv4Addr {
        let used: HashSet<Ipv4Addr> = self.ips.values().copied().collect();
        let broadcast = u32::from(self.network.broadcast());
        let first = u32::from(self.network.network).saturating_add(1);
        for raw in first..broadcast {
            let ip = Ipv4Addr::from(raw);
            if ip != self.network.gateway && !used.contains(&ip) {
                return ip;
            }
        }
        // Every address is taken. Handing out the highest one again lets the
        // engine refuse the duplicate address, and that error is reported like
        // any other create failure; panicking would take the whole node down.
        Ipv4Addr::from(broadcast.saturating_sub(1))
    }
}

/// A MAC address derived from the VM's IP: locally administered (`0x52` has
/// the unicast bit clear and the local bit set) and unique per address.
///
/// All four IPv4 octets are used, so two addresses in different subnets of the
/// same node never collide either.
fn mac_for(ip: Ipv4Addr) -> MacAddr {
    let octets = ip.octets();
    MacAddr([0x52, 0x55, octets[0], octets[1], octets[2], octets[3]])
}

/// Awaits the next engine event. `poll_fn` is all that is needed; a `futures`
/// dependency would only wrap this.
async fn next_event(stream: &mut EventStream) -> Option<EngineEvent> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default node network, which is what the binary's defaults produce.
    fn default_network() -> NodeNetworkConfig {
        NodeNetworkConfig::from(&crate::config::Config::default().cloud_hypervisor)
    }

    fn driver(
        network: NodeNetworkConfig,
        store: Store,
    ) -> NodeDriver<quoro_engine::FakeEngine, ReferenceAsDigest> {
        NodeDriver {
            engine: quoro_engine::FakeEngine::new(),
            resolver: ReferenceAsDigest,
            store,
            node_id: "nod_test".to_string(),
            ips: HashMap::new(),
            network,
        }
    }

    #[tokio::test]
    async fn addresses_skip_the_gateway_and_reuse_freed_slots() {
        let store = Store::open_in_memory().await.unwrap();
        let mut driver = driver(default_network(), store);
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 2));
        driver.ips.insert("a".into(), Ipv4Addr::new(10, 42, 0, 2));
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 3));
        driver.ips.insert("b".into(), Ipv4Addr::new(10, 42, 0, 4));
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 3));
        driver.ips.remove("a");
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 2));
    }

    #[tokio::test]
    async fn a_small_subnet_only_hands_out_its_host_addresses() {
        let store = Store::open_in_memory().await.unwrap();
        // 10.9.8.0/29: `.0` is the network, `.1` the gateway and `.7` the
        // broadcast, so exactly `.2` through `.6` are usable.
        let network = NodeNetworkConfig {
            bridge: "quoro0".to_string(),
            subnet: "10.9.8.0/29".to_string(),
            network: Ipv4Addr::new(10, 9, 8, 0),
            prefix: 29,
            gateway: Ipv4Addr::new(10, 9, 8, 1),
            dns: Vec::new(),
        };
        let mut driver = driver(network, store);
        let mut handed_out = Vec::new();
        for _ in 0..5 {
            let ip = driver.allocate_ip();
            driver.ips.insert(ip.to_string(), ip);
            handed_out.push(ip);
        }
        assert_eq!(
            handed_out,
            vec![
                Ipv4Addr::new(10, 9, 8, 2),
                Ipv4Addr::new(10, 9, 8, 3),
                Ipv4Addr::new(10, 9, 8, 4),
                Ipv4Addr::new(10, 9, 8, 5),
                Ipv4Addr::new(10, 9, 8, 6),
            ]
        );
        // Exhausted: no panic and no silent wrap into the next subnet.
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 9, 8, 6));
    }

    #[test]
    fn mac_is_local_and_unique_per_address() {
        let mac = mac_for(Ipv4Addr::new(10, 42, 0, 7));
        assert_eq!(mac.0[0] & 0b11, 0b10, "locally administered unicast");
        assert_eq!(mac.0[5], 7);
        assert_eq!(&mac.0[2..4], &[10, 42], "all four IP octets are used");
        assert_ne!(
            mac,
            mac_for(Ipv4Addr::new(10, 42, 0, 8)),
            "each address gets its own MAC"
        );
        assert_ne!(
            mac,
            mac_for(Ipv4Addr::new(10, 43, 0, 7)),
            "the same host address in another subnet differs too"
        );
    }
}
