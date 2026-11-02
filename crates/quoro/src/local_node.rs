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
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// The node bridge every VM is attached to.
const BRIDGE: &str = "quoro0";
/// The node subnet; VMs take `10.42.0.N` with `N >= 2`.
const SUBNET: &str = "10.42.0.0/24";
/// The address of the node on that bridge.
const GATEWAY: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 1);
/// How long a graceful stop may wait for the guest.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Registers this machine as the primary node, resyncs every VM it knows
/// about and starts the reconciliation loop.
///
/// Returns the `nod_` id and the loop's task handle.
///
/// # Errors
///
/// A database error while registering the node or reading the initial VM list.
pub async fn start<E: Engine + 'static>(
    engine: E,
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

    let network = NodeNetwork {
        bridge: BRIDGE.to_string(),
        subnet: SUBNET.to_string(),
        gateway: GATEWAY,
    };
    if let Err(e) = engine.ensure_network(&network).await {
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
        store,
        node_id: node_id.clone(),
        ips: HashMap::new(),
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

/// The driver's state: the engine, the database, its node id and the addresses
/// it has handed out.
struct NodeDriver<E: Engine> {
    engine: E,
    store: Store,
    node_id: String,
    /// Address per VM id, so a VM keeps its address across reconciles and the
    /// lowest free one is reused after a delete.
    ips: HashMap<String, Ipv4Addr>,
}

impl<E: Engine> NodeDriver<E> {
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
        // Until the OCI pipeline lands the reference stands in for the digest:
        // the engine caches images by digest and the reference is stable.
        let image = ImageRef {
            reference: desired.image.clone(),
            digest: ImageDigest(desired.image.clone()),
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
                gateway: GATEWAY,
                mac: mac_for(ip),
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

    /// The lowest address `10.42.0.N`, `N >= 2`, not already handed out. The
    /// bridge address is `.1` and `.255` is the broadcast address, so the last
    /// usable address is `.254`.
    fn allocate_ip(&self) -> Ipv4Addr {
        let used: HashSet<u8> = self.ips.values().map(|ip| ip.octets()[3]).collect();
        for last in 2..=254u8 {
            if !used.contains(&last) {
                return Ipv4Addr::new(10, 42, 0, last);
            }
        }
        // Every address is taken; the engine will refuse the duplicate address
        // and the error is reported like any other create failure.
        Ipv4Addr::new(10, 42, 0, 254)
    }
}

/// A MAC address derived from the VM's IP: locally administered (`0x52` has
/// the unicast bit clear and the local bit set) and unique per address.
fn mac_for(ip: Ipv4Addr) -> MacAddr {
    let octets = ip.octets();
    MacAddr([0x52, 0x55, 0x0a, 0x42, octets[2], octets[3]])
}

/// Awaits the next engine event. `poll_fn` is all that is needed; a `futures`
/// dependency would only wrap this.
async fn next_event(stream: &mut EventStream) -> Option<EngineEvent> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn addresses_skip_the_gateway_and_reuse_freed_slots() {
        let engine = quoro_engine::FakeEngine::new();
        let store = Store::open_in_memory().await.unwrap();
        let mut driver = NodeDriver {
            engine,
            store,
            node_id: "nod_test".to_string(),
            ips: HashMap::new(),
        };
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 2));
        driver.ips.insert("a".into(), Ipv4Addr::new(10, 42, 0, 2));
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 3));
        driver.ips.insert("b".into(), Ipv4Addr::new(10, 42, 0, 4));
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 3));
        driver.ips.remove("a");
        assert_eq!(driver.allocate_ip(), Ipv4Addr::new(10, 42, 0, 2));
    }

    #[test]
    fn mac_is_local_and_unique_per_address() {
        let mac = mac_for(Ipv4Addr::new(10, 42, 0, 7));
        assert_eq!(mac.0[0] & 0b11, 0b10, "locally administered unicast");
        assert_eq!(mac.0[5], 7);
        assert_ne!(
            mac,
            mac_for(Ipv4Addr::new(10, 42, 0, 8)),
            "each address gets its own MAC"
        );
    }
}
