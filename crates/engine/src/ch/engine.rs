// SPDX-License-Identifier: AGPL-3.0-only
//! The engine's state and the bodies of the [`Engine`](crate::Engine) methods.
//!
//! Everything the node knows about its VMs is in memory here, mirrored by the
//! per-VM runtime directory on disk. The two locks matter: `VmEntry::op`
//! serializes the calls on one VM (contract §4) and is a `tokio` mutex because
//! it is held across the API calls a lifecycle method makes; every other lock
//! is a `std` mutex that is never held across an `.await`.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, OwnedMutexGuard, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::ch::api::ApiClient;
use crate::ch::net::{self, Subnet};
use crate::ch::state::{
    self, ExitRecord, ExitState, SpecRecord, VmPaths, VmmRecord, ms_to_system_time, now_ms,
};
use crate::ch::tools::HostTools;
use crate::ch::vmconfig::{self, BootFiles};
use crate::ch::vmm::{Liveness, ProcessId, Vmm, VmmLaunch};
use crate::ch::{FORCE_KILL_BOUND, FORCE_STOP_BOUND, exec, host, serial, stream, supervise};
use crate::image::{ImageStore, ImageStoreConfig};
use crate::pins;
use crate::{
    ByteStream, Capabilities, ConsoleMode, ConsoleStream, EngineError, EngineEvent, EventStream,
    ExecRequest, ExecSession, HostProbe, ImageDigest, ImageRef, LineStream, MacAddr, NodeNetwork,
    OciLayout, PreparedImage, SerialMode, SnapshotMethod, StopMode, VmCounters, VmId, VmInfo,
    VmSpec, VmState,
};

use super::{ChConfig, OpenError};

/// Capacity of each event subscriber's buffer (§3.12).
const EVENT_CAPACITY: usize = 1024;
/// Timeout of the `vm.info` call `inspect` makes.
const INFO_TIMEOUT: Duration = Duration::from_secs(1);
/// Timeout of the `vm.counters` call `counters` makes.
const COUNTERS_TIMEOUT: Duration = Duration::from_secs(1);
/// Timeout of the `vmm.shutdown` call `delete` makes.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Timeout of the `vmm.shutdown` call a forced stop makes. A VMM that does not
/// answer within it is killed at once (§3.8): a fence's `T_force` can be a few
/// hundred milliseconds (node-agent-protocol §8.5).
const FORCE_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);
/// How long a forced stop waits for the exit after `vmm.shutdown` answered,
/// before it sends `SIGKILL`.
const FORCE_EXIT_AFTER_SHUTDOWN: Duration = Duration::from_millis(150);
/// How long any stop waits for the exit after SIGKILL.
const SETTLE_AFTER_SIGKILL: Duration = Duration::from_secs(5);
/// How long `delete` waits for the exit before it gives up.
const SETTLE_AFTER_DELETE_SHUTDOWN: Duration = Duration::from_secs(5);
/// How long one `vm.info`, `vm.pause` or `vm.resume` of a switchover freeze
/// may take (§3.22); a VMM that does not answer is a failed freeze.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a graceful stop waits for the VMM to say whether the VM is
/// paused before it asks the guest to stop anyway. A fence's graceful step
/// is bounded by `T_grace`, so the probe stays a small part of it.
const PAUSED_PROBE_TIMEOUT: Duration = Duration::from_millis(200);
// The pieces of a forced stop add up to its documented bounds.
const _: () = assert!(
    FORCE_SHUTDOWN_TIMEOUT.as_millis() + FORCE_EXIT_AFTER_SHUTDOWN.as_millis()
        == FORCE_KILL_BOUND.as_millis()
);
const _: () = assert!(
    FORCE_KILL_BOUND.as_millis() + SETTLE_AFTER_SIGKILL.as_millis() == FORCE_STOP_BOUND.as_millis()
);
/// The smallest memory a VM may be created with.
const MINIMUM_MEMORY_MB: u32 = 128;
/// The smallest data disk a VM may be created with.
const MINIMUM_DISK_GB: u32 = 1;
/// Name of the recorded networks file.
const NETWORKS_FILE: &str = "networks.json";

/// The state every clone of [`ChEngine`](super::ChEngine) shares.
pub(super) struct Inner {
    pub(super) config: ChConfig,
    pub(super) tools: HostTools,
    /// The verified pinned Cloud Hypervisor binary.
    pub(super) ch_binary: PathBuf,
    /// The pin version `capabilities` reports, for example `v54.0`.
    pub(super) ch_version: String,
    /// The verified pinned guest kernel.
    pub(super) kernel: PathBuf,
    /// The initramfs built from `config.init_binary` at start-up.
    pub(super) initramfs: PathBuf,
    /// What `cloud-hypervisor --version` printed, recorded for diagnostics; the
    /// pin's version is the authoritative one.
    #[allow(dead_code)]
    pub(super) ch_build: String,
    pub(super) images: ImageStore,
    /// Ensured networks by bridge; the mutex also serializes `ensure_network`.
    pub(super) networks: Mutex<HashMap<String, (NodeNetwork, Subnet)>>,
    pub(super) subscribers: StdMutex<Subscriptions>,
    pub(super) vms: StdMutex<HashMap<VmId, Arc<VmEntry>>>,
    /// TAPs of VMs whose `create` is in flight, with their bridge and group,
    /// so a concurrent peer sync does not drop them before their record is
    /// visible in `vms`.
    pub(super) inflight_peers: StdMutex<HashMap<String, InflightPeer>>,
    /// Work-disk paths whose `create` is in flight, with the VM that reserved
    /// them. Two VMs may never name the same work-disk path at once (§3.6).
    /// Volume files are reserved in the same map, so a volume and a work disk,
    /// or two creates of one volume, can never share a path.
    pub(super) inflight_disks: Arc<StdMutex<HashMap<PathBuf, VmId>>>,
    /// How `snapshot_volume` takes a snapshot on this node, probed once at
    /// start-up in the volume directory or inside the pool (§3.16, ADR 0156).
    pub(super) volume_method: SnapshotMethod,
    /// The loop-mounted XFS pool that holds volume files, when this node has
    /// one (§3.16, ADR 0156).
    pub(super) pool: super::pool::VolumePool,
    /// Serializes growing the pool with the volume create that needs it, so
    /// two creates never race on the pool's size (§3.16).
    pub(super) pool_grow: Mutex<()>,
    /// Serializes peer-set syncs, so the last one applied saw the latest state.
    pub(super) peers_sync: Mutex<()>,
    /// The snapshot files the start-up sweep left, or `None` when it did not
    /// run (§3.16).
    pub(super) snapshots_at_start: Option<std::collections::BTreeSet<String>>,
}

/// A TAP whose `create` has not finished yet, and what its bridge sets need.
pub(super) struct InflightPeer {
    /// The bridge the TAP will join.
    pub(super) bridge: String,
    /// The isolation group its spec carries.
    pub(super) group: Option<String>,
    /// Whether the VM is egress only.
    pub(super) egress_only: bool,
    /// The address its spec assigned, which the egress address checks allow.
    pub(super) ip: Ipv4Addr,
    /// The MAC address its spec assigned, which the egress MAC checks allow.
    pub(super) mac: MacAddr,
}

/// Where a lifecycle call on one VM stands, for `inspect` while it is in
/// flight (§3.6, §3.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Starting,
    Stopping,
}

/// One VM's shared state.
pub(super) struct VmEntry {
    pub(super) id: VmId,
    pub(super) paths: VmPaths,
    /// Serializes create, start, stop, delete, pause and resume on this id,
    /// a `PauseCopy` snapshot's pause and resume included. It is shared so a
    /// forced stop's own task can hold it (§3.8, §4).
    pub(super) op: Arc<Mutex<()>>,
    /// The live VMM handle, while the process exists.
    pub(super) vmm: StdMutex<Option<Vmm>>,
    /// `/vm.boot` succeeded for the current VMM process.
    pub(super) booted: AtomicBool,
    /// A stop was requested for the current VMM. The supervisor reads it with
    /// the `stop_requested` marker file, so an exit after a stop is a stop
    /// even when the marker could not be written (§3.8, #434).
    pub(super) stop_requested: AtomicBool,
    /// Forced stops of this VM that wait for its lock. While one waits, a
    /// graceful stop that holds the lock hands it to its own forced stop at
    /// once instead of holding it until its timeout (#434).
    force_waiting: watch::Sender<usize>,
    phase: StdMutex<Phase>,
    /// Bumped by the supervisor after it finished the exit path; a waiter
    /// subscribes before it looks at `vmm` so no completion is missed.
    pub(super) settled: watch::Sender<u64>,
    /// Cached `spec.json`, once create or rediscovery read it.
    pub(super) spec: StdMutex<Option<SpecRecord>>,
    /// A reader holds the VM's tagged output (§3.13).
    pub(super) output_open: Arc<AtomicBool>,
    /// A client holds the VM's interactive console (§3.14).
    pub(super) console_open: Arc<AtomicBool>,
    /// The serial client and the node-local ring behind `serial_output`
    /// (§3.19). It outlives VMM restarts; `delete` ends it.
    pub(super) serial: Arc<serial::SerialHub>,
}

impl VmEntry {
    fn new(id: VmId, paths: VmPaths, spec: Option<SpecRecord>) -> Self {
        let (settled, _) = watch::channel(0_u64);
        Self {
            serial: Arc::new(serial::SerialHub::new(&paths.dir)),
            id,
            paths,
            op: Arc::new(Mutex::new(())),
            vmm: StdMutex::new(None),
            booted: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            force_waiting: watch::channel(0).0,
            phase: StdMutex::new(Phase::Idle),
            settled,
            spec: StdMutex::new(spec),
            output_open: Arc::new(AtomicBool::new(false)),
            console_open: Arc::new(AtomicBool::new(false)),
        }
    }

    fn phase(&self) -> Phase {
        *lock(&self.phase)
    }

    /// Resolves once a forced stop of this VM waits for its lock. A short
    /// call that holds the lock gives it up then, so a fence's forced stop
    /// keeps its bound (§3.8, ADR 0243, #434).
    pub(super) async fn forced_stop_waiting(&self) {
        let mut forced = self.force_waiting.subscribe();
        let _ = forced.wait_for(|waiting| *waiting > 0).await;
    }

    fn set_phase(&self, phase: Phase) {
        *lock(&self.phase) = phase;
    }

    /// The spec this VM was created with, if it has been read.
    pub(super) fn spec(&self) -> Option<SpecRecord> {
        lock(&self.spec).clone()
    }

    /// The live VMM, unless its process is gone.
    ///
    /// A `/proc` that cannot be read proves nothing, so the VMM stays live: a
    /// stop goes on to stop it, and `inspect` keeps reporting it with its pid
    /// instead of stopped, which a fence would take for proof (D-0074).
    pub(super) fn live_vmm(&self) -> Option<Vmm> {
        let slot = lock(&self.vmm);
        let vmm = slot.as_ref()?;
        match vmm.id().liveness() {
            Liveness::Gone => None,
            Liveness::Alive | Liveness::Unknown(_) => Some(vmm.clone()),
        }
    }
}

/// The event subscribers and the drop counter (§3.12).
#[derive(Default)]
pub(super) struct Subscriptions {
    senders: Vec<mpsc::Sender<EngineEvent>>,
    dropped: u64,
}

/// A `Stream` over a bounded mpsc receiver, as in `fake.rs`.
struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}

/// The recorded networks, as `networks.json` stores them.
#[derive(Serialize, Deserialize)]
struct NetworksFile {
    networks: Vec<NetworkRecord>,
}

/// One recorded network.
#[derive(Serialize, Deserialize)]
struct NetworkRecord {
    bridge: String,
    subnet: String,
    gateway: Ipv4Addr,
    /// Absent in files written before guest DNS moved onto the network.
    #[serde(default)]
    dns: Vec<Ipv4Addr>,
}

impl Inner {
    /// Checks the pinned artifacts, prepares the state directory and
    /// rediscovers the VMs left by an earlier process (contract §5, §3.11).
    pub(super) async fn new(config: ChConfig) -> Result<Arc<Inner>, OpenError> {
        let tools = HostTools::discover()?;
        let ch_pin = pins::get(pins::CLOUD_HYPERVISOR)?;
        let kernel_pin = pins::get(pins::GUEST_KERNEL)?;
        let ch_binary = config.artifacts_dir.join(&ch_pin.file);
        let kernel = config.artifacts_dir.join(&kernel_pin.file);
        pins::verify(&ch_binary, &ch_pin)?;
        pins::verify(&kernel, &kernel_pin)?;
        let ch_build = observed_build(&ch_binary).await?;

        let vms_root = config.state_dir.join("vms");
        for dir in [
            vms_root.clone(),
            config.state_dir.join("images"),
            config.state_dir.join("boot"),
        ] {
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|err| io_error(&dir, &err))?;
        }

        // Volumes are sparse files owned by root, so the directory is 0700 and
        // the snapshot method is decided as soon as it exists: the pool is
        // attached before any other volume call, and a pool image that exists
        // but does not mount must never fall back to the empty directory
        // underneath it (§3.20, §3.16, ADR 0156).
        let volumes_dir = config.state_dir.join("volumes");
        create_parent_dirs_0700(&volumes_dir)?;
        let attachment = super::pool::attach(&config.state_dir, &volumes_dir).await;
        let volume_method = attachment.method;
        let pool = attachment.pool;
        // Leftovers live inside the pool, so they are only reachable once it
        // is mounted; a broken pool's directory is never touched.
        let swept = if matches!(pool, super::pool::VolumePool::Broken { .. }) {
            None
        } else {
            super::volume::remove_leftovers(&volumes_dir, &config.snapshot_sweep)
        };
        // The node agent marks its store as tracking every snapshot from what
        // the sweep left, so it is told only when the sweep saw the directory
        // the snapshots live in: the mounted pool, or a directory that
        // reflinks itself with no pool image beside it. Another filesystem
        // mounted on the directory, or a node without reflink, reports
        // nothing, and the node agent keeps every snapshot there (§3.16).
        let sees_snapshots = match &pool {
            super::pool::VolumePool::Mounted { .. } => true,
            super::pool::VolumePool::Absent => {
                volume_method == SnapshotMethod::Reflink
                    && !config.state_dir.join(super::pool::POOL_IMAGE).exists()
            }
            super::pool::VolumePool::Broken { .. } => false,
        };
        let snapshots_at_start = swept.filter(|_| sees_snapshots);

        let initramfs = config.state_dir.join("boot").join("initramfs.cpio");
        let init_binary = config.init_binary.clone();
        let out = initramfs.clone();
        tokio::task::spawn_blocking(move || crate::boot::write_initramfs(&init_binary, &out))
            .await
            .map_err(|err| EngineError::Io(format!("initramfs task failed: {err}")))??;

        let images = ImageStore::new(ImageStoreConfig {
            root: config.state_dir.join("images"),
            mkfs_ext4: tools.mkfs_ext4.clone(),
            preserve_ownership: rustix::process::geteuid().is_root(),
        })?;

        let networks = Mutex::new(load_networks(&config.state_dir.join(NETWORKS_FILE))?);

        let inner = Arc::new(Inner {
            config,
            tools,
            ch_binary,
            ch_version: ch_pin.version,
            kernel,
            initramfs,
            ch_build,
            images,
            networks,
            subscribers: StdMutex::new(Subscriptions::default()),
            vms: StdMutex::new(HashMap::new()),
            inflight_peers: StdMutex::new(HashMap::new()),
            inflight_disks: Arc::new(StdMutex::new(HashMap::new())),
            volume_method,
            pool,
            pool_grow: Mutex::new(()),
            peers_sync: Mutex::new(()),
            snapshots_at_start,
        });
        inner.rediscover(&vms_root).await?;
        Ok(inner)
    }
}

/// Adopts the VMM that `id`'s record names, at start-up.
///
/// A failure is [`OpenError::Retry`] and names the VM: the VMM may still run
/// a guest, so the engine must not start without it, and a later start may
/// get past the failure (#459).
fn adopt_recorded(id: &VmId, process: ProcessId) -> Result<Option<Vmm>, OpenError> {
    Vmm::adopt(process).map_err(|err| {
        let reason = match err {
            EngineError::Hypervisor(reason) => reason,
            other => other.to_string(),
        };
        OpenError::Retry(EngineError::Hypervisor(format!("VM {}: {reason}", id.0)))
    })
}

/// The version line `binary --version` prints.
async fn observed_build(binary: &Path) -> Result<String, EngineError> {
    let output = tokio::process::Command::new(binary)
        .arg("--version")
        .output()
        .await
        .map_err(|err| {
            EngineError::Hypervisor(format!("cannot run {} --version: {err}", binary.display()))
        })?;
    if !output.status.success() {
        return Err(EngineError::Hypervisor(format!(
            "{} --version exited with {}",
            binary.display(),
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

impl Inner {
    /// Rebuilds the in-memory view from the runtime directories (§3.11).
    ///
    /// A VMM that is still alive is adopted and supervised; one that is gone
    /// has its exit recorded now, so a restart of the agent never loses an exit
    /// that happened while it was not running.
    async fn rediscover(self: &Arc<Self>, vms_root: &Path) -> Result<(), OpenError> {
        let mut directory = tokio::fs::read_dir(vms_root)
            .await
            .map_err(|err| io_error(vms_root, &err))?;
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|err| io_error(vms_root, &err))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(".tmp-") {
                let _ = tokio::fs::remove_dir_all(entry.path()).await;
                continue;
            }
            if name.starts_with('.') {
                continue;
            }
            let id = VmId(name);
            let paths = VmPaths::new(vms_root, &id);
            let Some(record) = state::read_json::<SpecRecord>(&paths.spec)? else {
                continue;
            };
            let spec = record.to_spec().ok();
            let entry = Arc::new(VmEntry::new(id.clone(), paths.clone(), Some(record)));
            match state::read_json::<VmmRecord>(&paths.vmm)? {
                Some(vmm_record) => match adopt_recorded(&id, vmm_record.process)? {
                    Some(vmm) => {
                        entry.booted.store(vmm_record.booted, Ordering::SeqCst);
                        *lock(&entry.vmm) = Some(vmm.clone());
                        // A running VM keeps its serial socket: connect again,
                        // so the ring covers the gap while the agent was gone
                        // (§3.19).
                        if let Some(spec) = &spec {
                            start_serial_client(&entry, spec);
                        }
                        lock(&self.vms).insert(id, Arc::clone(&entry));
                        supervise::start(Arc::clone(self), entry, vmm);
                    }
                    None => {
                        entry.booted.store(vmm_record.booted, Ordering::SeqCst);
                        supervise::finish(self, &entry, None, None, false);
                        lock(&self.vms).insert(id, entry);
                    }
                },
                None => {
                    lock(&self.vms).insert(id, entry);
                }
            }
        }
        Ok(())
    }

    /// Sends one event to every subscriber, dropping it when a buffer is full
    /// (§3.12): a slow consumer never blocks a VMM.
    pub(super) fn emit(&self, event: EngineEvent) {
        let mut subscriptions = lock(&self.subscribers);
        let mut dropped = 0_u64;
        subscriptions
            .senders
            .retain(|sender| match sender.try_send(event.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    dropped = dropped.saturating_add(1);
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            });
        subscriptions.dropped = subscriptions.dropped.saturating_add(dropped);
    }

    /// A new event stream with its own buffer (§3.12).
    pub(super) fn subscribe(&self) -> EventStream {
        let (sender, receiver) = mpsc::channel(EVENT_CAPACITY);
        lock(&self.subscribers).senders.push(sender);
        Box::pin(ReceiverStream { rx: receiver })
    }

    /// How many events were dropped for slow subscribers.
    pub(super) fn dropped_events(&self) -> u64 {
        lock(&self.subscribers).dropped
    }

    /// What this backend and build support (§3.1).
    pub(super) fn capabilities(&self) -> Capabilities {
        // A pool image that does not mount leaves the node without volume
        // snapshots: the node agent reports its volumes failed (§3.16).
        let pool_broken = matches!(self.pool, super::pool::VolumePool::Broken { .. });
        Capabilities {
            engine: "cloud-hypervisor".to_string(),
            version: self.ch_version.clone(),
            kvm: host::kvm_available(),
            text_console: true,
            graphical_console: false,
            vm_snapshot: false,
            volume_snapshot: !pool_broken,
            exec: true,
            volume_snapshot_method: if pool_broken {
                None
            } else {
                Some(self.volume_method)
            },
        }
    }

    /// Reads the host facts Cloud Hypervisor v54 needs (§3.2).
    pub(super) fn probe_host(&self) -> Result<HostProbe, EngineError> {
        let kernel_release = host::kernel_release()?;
        let flags = host::cpu_flags()?;
        let kvm_available = host::kvm_available();
        Ok(HostProbe {
            kvm_available,
            kernel_supported: host::kernel_supported(&kernel_release),
            cpu_vmx: host::cpu_has_flag(&flags, "vmx"),
            cpu_svm: host::cpu_has_flag(&flags, "svm"),
            nested: kvm_available && host::cpu_has_flag(&flags, "hypervisor"),
            kernel_release,
        })
    }

    /// Brings one node network to the requested state (§3.3).
    pub(super) async fn ensure_network(&self, net: &NodeNetwork) -> Result<(), EngineError> {
        let subnet = Subnet::parse(&net.subnet)?;
        let mut networks = self.networks.lock().await;
        for (bridge, (_, recorded)) in networks.iter() {
            if bridge != &net.bridge && overlaps(&subnet, recorded) {
                return Err(EngineError::Conflict(format!(
                    "subnet {} overlaps the network already recorded on bridge {bridge}",
                    net.subnet
                )));
            }
        }
        if let Some((recorded, _)) = networks.get(&net.bridge)
            && (recorded.subnet != net.subnet || recorded.gateway != net.gateway)
        {
            return Err(EngineError::Conflict(format!(
                "bridge {} is already recorded with {} via {}; refusing to reconfigure",
                net.bridge, recorded.subnet, recorded.gateway
            )));
        }
        let subnet = net::ensure_network(&self.tools, net).await?;
        networks.insert(net.bridge.clone(), (net.clone(), subnet));
        persist_networks(&self.config.state_dir.join(NETWORKS_FILE), &networks)?;
        drop(networks);
        // Applying the ruleset recreated the bridge table with an empty
        // `peers` set; put the isolation groups back (§3.3).
        self.sync_peers(&net.bridge).await
    }

    /// Puts back a bridge this engine recorded but the host no longer has.
    ///
    /// A host reboot removes every bridge, and the node agent ensures only the
    /// node's current network. After a subnet change (ADR 0144) the VMs still
    /// recorded on the previous bridge would find nothing to attach to. Their
    /// addresses belong to that bridge's subnet, so they come back on it,
    /// recreated from `networks.json`, until a redeploy moves them to the
    /// current bridge (`node-agent-protocol.md` section 6.9).
    async fn restore_recorded_bridge(&self, bridge: &str) -> Result<(), EngineError> {
        let networks = self.networks.lock().await;
        let Some(net) =
            recorded_bridge_to_restore(bridge, net::interface_exists(bridge), &networks)
        else {
            return Ok(());
        };
        net::ensure_network(&self.tools, &net).await?;
        drop(networks);
        // The ruleset came back with empty sets; refill them (§3.3).
        self.sync_peers(bridge).await
    }

    /// Rewrites the bridge's `peers`, `egress` and `egress_addrs` sets from
    /// every VM recorded on it plus the creates in flight (§3.3).
    async fn sync_peers(&self, bridge: &str) -> Result<(), EngineError> {
        let _sync = self.peers_sync.lock().await;
        let mut members: Vec<(String, Option<String>)> = Vec::new();
        let mut egress: Vec<(String, MacAddr, Ipv4Addr)> = Vec::new();
        for (_, entry) in self.entries() {
            if let Some(record) = entry.spec()
                && record.bridge == bridge
            {
                members.push((record.tap.clone(), record.spec.net.isolation_group.clone()));
                if record.spec.net.egress_only {
                    // The egress checks key on the MAC as well as the address,
                    // so the record must carry both; `to_spec` rejects a
                    // corrupt one the same way the rest of the state does.
                    let spec = record.to_spec()?;
                    egress.push((record.tap, spec.net.mac, spec.net.ip));
                }
            }
        }
        for (tap, peer) in lock(&self.inflight_peers).iter() {
            if peer.bridge == bridge && !members.iter().any(|(known, _)| known == tap) {
                members.push((tap.clone(), peer.group.clone()));
                if peer.egress_only {
                    egress.push((tap.clone(), peer.mac, peer.ip));
                }
            }
        }
        net::sync_peers(&self.tools, bridge, &net::peer_pairs(&members)).await?;
        net::sync_egress(&self.tools, bridge, &egress).await
    }

    /// Pulls and converts one image (§3.4).
    pub(super) async fn prepare_image(
        &self,
        image: &ImageRef,
    ) -> Result<PreparedImage, EngineError> {
        self.images.prepare(image).await
    }

    /// Converts a local OCI image layout a builder wrote (§3.17).
    pub(super) async fn prepare_layout(
        &self,
        layout: &OciLayout,
    ) -> Result<PreparedImage, EngineError> {
        self.images.prepare_layout(layout).await
    }

    /// Removes one cached image, unless a VM uses it (§3.5).
    pub(super) async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        for (id, entry) in self.entries() {
            if let Some(record) = entry.spec()
                && record.spec.rootfs == digest.0
            {
                return Err(EngineError::Conflict(format!(
                    "image {} is used by VM {}",
                    digest.0, id.0
                )));
            }
        }
        self.images.remove(digest).await
    }

    /// Creates a VM and its runtime state without starting it (§3.6).
    pub(super) async fn create(self: &Arc<Self>, spec: &VmSpec) -> Result<VmInfo, EngineError> {
        state::validate_vm_id(&spec.id)?;
        let paths = VmPaths::new(&self.vms_root(), &spec.id);
        let entry = self.entry_or_insert(&spec.id, &paths, None);
        let _op = entry.op.lock().await;

        if paths.dir.exists() {
            let record = state::read_json::<SpecRecord>(&paths.spec)?.ok_or_else(|| {
                EngineError::Hypervisor(format!("{}: missing spec.json", paths.dir.display()))
            })?;
            if record.to_spec()?.canonical() != spec.canonical() {
                return Err(EngineError::Conflict(format!(
                    "VM {} already exists with a different spec",
                    spec.id.0
                )));
            }
            *lock(&entry.spec) = Some(record);
            return self.inspect(&spec.id).await;
        }

        vmconfig::validate_spec(spec)?;
        let _disks = self.reserve_work_disks(spec)?;
        let _volumes = self.reserve_volumes(spec)?;
        let (network, subnet) = self.reserve(spec).await?;
        let prepared = self.images.cached(&spec.rootfs)?.ok_or_else(|| {
            EngineError::Image(format!("rootfs {} is not a prepared image", spec.rootfs.0))
        })?;
        let config =
            vmconfig::guest_config(spec, &subnet, &network.dns, self.config.guest_stop_timeout)?;
        let tap = net::tap_name(&spec.id);
        let record = SpecRecord::new(spec, &tap, &network.bridge);

        // The peer set learns the new TAP before the TAP joins the bridge, so
        // a grouped VM is never briefly cut off from its group, and an
        // ungrouped one is never in any pair (§3.3).
        lock(&self.inflight_peers).insert(
            tap.clone(),
            InflightPeer {
                bridge: network.bridge.clone(),
                group: spec.net.isolation_group.clone(),
                egress_only: spec.net.egress_only,
                ip: spec.net.ip,
                mac: spec.net.mac,
            },
        );
        let created = self
            .create_reserved(
                &entry, spec, &paths, &network, &prepared, config, record, &tap,
            )
            .await;
        lock(&self.inflight_peers).remove(&tap);
        if created.is_err() {
            // Best effort: drop the pairs of the TAP that never came up.
            let _ = self.sync_peers(&network.bridge).await;
        }
        created
    }

    /// The part of `create` after the reservation; split out so the peer
    /// bookkeeping above wraps every exit path.
    #[allow(clippy::too_many_arguments)]
    async fn create_reserved(
        self: &Arc<Self>,
        entry: &Arc<VmEntry>,
        spec: &VmSpec,
        paths: &VmPaths,
        network: &NodeNetwork,
        prepared: &PreparedImage,
        config: quoro_init::GuestConfig,
        record: SpecRecord,
        tap: &str,
    ) -> Result<VmInfo, EngineError> {
        let tap = tap.to_owned();
        self.sync_peers(&network.bridge).await?;

        entry.set_phase(Phase::Starting);
        let tmp_dir = self
            .vms_root()
            .join(format!(".tmp-{}-{}", spec.id.0, std::process::id()));
        let tmp = rebased(paths, &tmp_dir);
        let built = self.build_files(&tmp, &record, &config, spec.disk_gb).await;
        if let Err(err) = built {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        if let Err(err) = self.prepare_work_disks(spec).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        if let Err(err) = tokio::fs::rename(&tmp_dir, &paths.dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            entry.set_phase(Phase::Idle);
            return Err(io_error(&paths.dir, &err));
        }

        if let Err(err) = net::create_tap(
            &self.tools,
            &tap,
            &network.bridge,
            spec.net.isolation_group.is_none(),
        )
        .await
        {
            let _ = tokio::fs::remove_dir_all(&paths.dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        let launch = VmmLaunch {
            binary: &self.ch_binary,
            api_socket: &paths.api_socket,
            event_log: &paths.event_log,
            log_file: &paths.vmm_log,
        };
        remove_stale_sockets(spec);
        let vmm = match Vmm::spawn(&launch).await {
            Ok(vmm) => vmm,
            Err(err) => {
                let _ = net::delete_tap(&self.tools, &tap).await;
                let _ = tokio::fs::remove_dir_all(&paths.dir).await;
                entry.set_phase(Phase::Idle);
                return Err(err);
            }
        };
        let boot_files = BootFiles {
            kernel: &self.kernel,
            initramfs: &self.initramfs,
            rootfs: &prepared.rootfs_path,
        };
        let body = vmconfig::vm_create_body(spec, paths, &boot_files, &tap, &self.volumes_dir());
        if let Err(err) = ApiClient::new(&paths.api_socket).create(&body).await {
            let _ = vmm.sigkill();
            let _ = vmm.wait_exit(SETTLE_AFTER_SIGKILL).await;
            let _ = net::delete_tap(&self.tools, &tap).await;
            let _ = tokio::fs::remove_dir_all(&paths.dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err.into());
        }

        let vmm_record = VmmRecord {
            process: vmm.id(),
            booted: false,
            started_at_ms: None,
        };
        if let Err(err) = state::write_json_atomic(&paths.vmm, &vmm_record) {
            let _ = vmm.sigkill();
            let _ = vmm.wait_exit(SETTLE_AFTER_SIGKILL).await;
            let _ = net::delete_tap(&self.tools, &tap).await;
            let _ = tokio::fs::remove_dir_all(&paths.dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err);
        }

        *lock(&entry.spec) = Some(record);
        *lock(&entry.vmm) = Some(vmm.clone());
        entry.booted.store(false, Ordering::SeqCst);
        entry.set_phase(Phase::Idle);
        // Cloud Hypervisor created the serial socket in `vm.create`: connect
        // now, before the boot, so the ring holds the whole boot (§3.19).
        start_serial_client(entry, spec);
        supervise::start(Arc::clone(self), Arc::clone(entry), vmm.clone());
        Ok(VmInfo {
            id: spec.id.clone(),
            state: VmState::Stopped,
            pid: Some(vmm.id().pid),
            started_at: None,
            exit_code: None,
        })
    }

    /// Boots a created or stopped VM within `within` (§3.7, ADR 0242).
    ///
    /// The boot runs on a task of its own that holds the VM's operation lock,
    /// so a caller that stops waiting never leaves behind a VMM that nothing
    /// stops. The task keeps to the same deadline: it waits for the TAP, for
    /// the VMM's API and for each API call only until then, and sends no
    /// `vm.boot` once it has passed; a boot that gets no answer in time is
    /// killed. Restoring a bridge after a host reboot is not cut short, as a
    /// half-restored ruleset would not be restored again. When the VM is not
    /// running by then, the caller gets
    /// `Timeout` and the task stops by force what it started. A [`StartGate`]
    /// decides between the two, so a start that returned `Timeout` never
    /// leaves a VM it booted running.
    pub(super) async fn start(
        self: &Arc<Self>,
        id: &VmId,
        within: Duration,
    ) -> Result<VmInfo, EngineError> {
        let deadline = Instant::now() + within;
        let Some(entry) = self.entry(id) else {
            return Err(EngineError::NotFound);
        };
        let gate = Arc::new(StartGate::default());
        // A caller that stops waiting gives the start up, as its deadline does.
        let _waiting = GiveUpOnDrop(Arc::clone(&gate));
        // Nothing below yields before the task is spawned, so the start keeps
        // its place in the lock's queue.
        let op = queue_for_lock(&entry).await;
        let inner = Arc::clone(self);
        let task_gate = Arc::clone(&gate);
        let mut task = tokio::spawn(async move {
            let op = match op {
                Ok(op) => op,
                Err(lock) => lock.await,
            };
            inner.bounded_start(&entry, op, deadline, &task_gate).await
        });
        let joined = match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(joined) => joined,
            Err(_) if gate.give_up() => return Err(EngineError::Timeout),
            // The VM was running in time; its task is returning it.
            Err(_) => task.await,
        };
        joined.map_err(|err| EngineError::Hypervisor(format!("the start did not finish: {err}")))?
    }

    /// The body of [`Self::start`], on its task, holding `op`, and bounded by
    /// `deadline` through `gate`.
    async fn bounded_start(
        self: &Arc<Self>,
        entry: &Arc<VmEntry>,
        op: OwnedMutexGuard<()>,
        deadline: Instant,
        gate: &StartGate,
    ) -> Result<VmInfo, EngineError> {
        let id = &entry.id;
        // Given up while it waited for the lock: the start starts nothing.
        if gate.given_up(deadline) {
            return Err(EngineError::Timeout);
        }
        if !entry.paths.dir.exists() {
            self.remove_entry(id);
            return Err(EngineError::NotFound);
        }
        let live = entry.live_vmm();
        if live.is_some() && entry.booted.load(Ordering::SeqCst) {
            return self.inspect(id).await;
        }
        let record = match entry.spec() {
            Some(record) => record,
            None => state::read_json::<SpecRecord>(&entry.paths.spec)?.ok_or_else(|| {
                EngineError::Hypervisor(format!("{}: missing spec.json", entry.paths.dir.display()))
            })?,
        };
        let spec = record.to_spec()?;
        let prepared = self.images.cached(&spec.rootfs)?.ok_or_else(|| {
            EngineError::Image(format!("rootfs {} is not a prepared image", spec.rootfs.0))
        })?;
        if !self.kernel.exists() {
            return Err(EngineError::Io(format!(
                "guest kernel {} is missing",
                self.kernel.display()
            )));
        }
        if !self.initramfs.exists() {
            return Err(EngineError::Io(format!(
                "initramfs {} is missing",
                self.initramfs.display()
            )));
        }

        self.restore_recorded_bridge(&record.bridge).await?;
        let starting = PhaseGuard::begin(entry, Phase::Starting);
        if gate.given_up(deadline) {
            return Err(EngineError::Timeout);
        }
        // `create_tap` reuses a TAP that exists, so cutting it short at the
        // deadline leaves nothing a later start cannot finish.
        tokio::time::timeout_at(
            deadline,
            net::create_tap(
                &self.tools,
                &record.tap,
                &record.bridge,
                spec.net.isolation_group.is_none(),
            ),
        )
        .await
        .map_err(|_| EngineError::Timeout)??;
        entry.stop_requested.store(false, Ordering::SeqCst);
        state::remove_marker(&entry.paths.stop_requested)?;
        let _ = std::fs::remove_file(&entry.paths.last_exit);
        crate::boot::reset_status(&entry.paths.config_disk)?;
        if gate.given_up(deadline) {
            return Err(EngineError::Timeout);
        }

        let (vmm, spawned) = match live {
            Some(vmm) => (vmm, false),
            None => (
                self.boot_vmm(entry, &spec, &prepared, &record, deadline)
                    .await?,
                true,
            ),
        };
        if gate.given_up(deadline) {
            // A VMM this start spawned is stopped again; one that `create`
            // spawned is left as it was, unbooted.
            if spawned {
                drop(starting);
                return self.give_up_start(entry, op).await;
            }
            return Err(EngineError::Timeout);
        }
        if let Err(err) = ApiClient::new(&entry.paths.api_socket)
            .by(deadline)
            .boot()
            .await
        {
            let _ = vmm.sigkill();
            let _ = wait_settled(entry, SETTLE_AFTER_SIGKILL).await;
            return Err(EngineError::Hypervisor(format!(
                "cloud-hypervisor did not boot: {err}"
            )));
        }

        let started_at_ms = now_ms();
        let vmm_record = VmmRecord {
            process: vmm.id(),
            booted: true,
            started_at_ms: Some(started_at_ms),
        };
        if let Err(err) = state::write_json_atomic(&entry.paths.vmm, &vmm_record) {
            // The guest runs but could not be recorded as booted: kill it, so
            // no start leaves a VM running that reads as stopped.
            let _ = vmm.sigkill();
            let _ = wait_settled(entry, SETTLE_AFTER_SIGKILL).await;
            return Err(err);
        }
        entry.booted.store(true, Ordering::SeqCst);
        drop(starting);
        self.emit(EngineEvent::Started { id: id.clone() });
        // Only a caller that still waits learns that the VM runs. A start
        // given up meanwhile stops the VM it booted, which then emits its
        // `Exited` after the `Started` above.
        if !gate.commit(deadline) {
            return self.give_up_start(entry, op).await;
        }
        Ok(VmInfo {
            id: id.clone(),
            state: VmState::Running,
            pid: Some(vmm.id().pid),
            started_at: Some(ms_to_system_time(started_at_ms)),
            exit_code: None,
        })
    }

    /// Stops by force what a start that was given up started, still holding
    /// its lock (§3.7, ADR 0242). Its caller already has `Timeout`.
    async fn give_up_start(
        &self,
        entry: &VmEntry,
        op: OwnedMutexGuard<()>,
    ) -> Result<VmInfo, EngineError> {
        let _ = self
            .forced_stop(entry, OpLock::Held(op), ForceTimings::DEFAULT)
            .await;
        Err(EngineError::Timeout)
    }

    /// Stops a VM, gracefully or by force, within the bound `mode` carries
    /// (§3.8, D-0074, ADR 0243).
    ///
    /// The forced part runs on a task of its own that holds the VM's operation
    /// lock, so once it has begun it sends `SIGKILL` and waits for the exit
    /// even when the caller got `Timeout` or stopped waiting: a fence that
    /// gives up after its `T_force` never leaves the VMM running and the VM
    /// `Stopping` (#434). The forced stop takes its place in the lock's queue
    /// before the call can be dropped, and a graceful stop that holds the lock
    /// hands it over at once, so no later call on the VM runs first and the
    /// kill does not wait out another caller's graceful timeout. A later stop
    /// of the same VM waits for that lock, within its own bound, so it joins
    /// the forced stop and then finds the VM stopped. A runtime that shuts
    /// down cancels the task like any other; the VMM then outlives the agent,
    /// as after an agent crash, and the restarted agent fences it again
    /// (node-agent-protocol §8.5).
    pub(super) async fn stop(
        self: &Arc<Self>,
        id: &VmId,
        mode: StopMode,
    ) -> Result<VmInfo, EngineError> {
        let began = Instant::now();
        let Some(entry) = self.entry(id) else {
            return Err(EngineError::NotFound);
        };
        match mode {
            StopMode::Force { within } => {
                // Nothing below yields before the task is spawned, so a
                // dropped call cannot lose its place in the queue.
                let waiting = ForceWaiting::begin(&entry);
                let op = match queue_for_lock(&entry).await {
                    Ok(op) => {
                        drop(waiting);
                        OpLock::Held(op)
                    }
                    Err(lock) => OpLock::Queued(lock, waiting),
                };
                let task = self.spawn_forced_stop(entry, op, ForceTimings::within(within));
                finish_by(began + within, task).await
            }
            StopMode::Graceful {
                timeout,
                force_within,
            } => {
                self.graceful_stop(entry, began + timeout, force_within)
                    .await
            }
        }
    }

    /// A graceful stop that asks the guest until `graceful_by`, waiting for
    /// the VM's lock included, then stops the VM as `Force { within:
    /// force_within }` (§3.8). It returns by `graceful_by + force_within`.
    async fn graceful_stop(
        self: &Arc<Self>,
        entry: Arc<VmEntry>,
        graceful_by: Instant,
        force_within: Duration,
    ) -> Result<VmInfo, EngineError> {
        let deadline = graceful_by + force_within;
        let mut lock: QueuedLock = Box::pin(Arc::clone(&entry.op).lock_owned());
        let op = match tokio::time::timeout_at(graceful_by, lock.as_mut()).await {
            Ok(op) => op,
            Err(_) => {
                // The graceful wait ran out in the lock's queue: the stop goes
                // on as a forced one, in the same place.
                let waiting = ForceWaiting::begin(&entry);
                let task = self.spawn_forced_stop(
                    entry,
                    OpLock::Queued(lock, waiting),
                    ForceTimings::within(force_within),
                );
                return finish_by(deadline, task).await;
            }
        };
        let id = &entry.id;
        if !entry.paths.dir.exists() {
            self.remove_entry(id);
            return Err(EngineError::NotFound);
        }
        if entry.live_vmm().is_none() {
            self.settle_crashed_exit(&entry);
            return self.inspect(id).await;
        }

        let stopping = PhaseGuard::begin(&entry, Phase::Stopping);
        request_stop(&entry);
        // A paused guest cannot answer the power button, so a graceful stop
        // of a paused VM goes straight to the shutdown instead of waiting out
        // its timeout (§3.22).
        let guest_stopped = async {
            if !entry.booted.load(Ordering::SeqCst) || is_paused(&entry.paths.api_socket).await {
                return false;
            }
            let asked = ApiClient::new(&entry.paths.api_socket)
                .by(graceful_by)
                .power_button()
                .await
                .is_ok();
            asked
                && wait_settled(
                    &entry,
                    graceful_by.saturating_duration_since(Instant::now()),
                )
                .await
        };
        // A forced stop that waits behind this one ends the graceful wait.
        // Forced stops ahead of it already hold or held the lock, so they
        // no longer count.
        let mut forced = entry.force_waiting.subscribe();
        let stopped = tokio::select! {
            stopped = tokio::time::timeout_at(graceful_by, guest_stopped) => stopped.unwrap_or(false),
            _ = forced.wait_for(|waiting| *waiting > 0) => false,
        };
        if stopped {
            drop(stopping);
            return self.inspect(id).await;
        }
        // The forced stop takes over the lock and the phase, so nothing runs
        // between the two (§4).
        stopping.hand_over();
        let task =
            self.spawn_forced_stop(entry, OpLock::Held(op), ForceTimings::within(force_within));
        finish_by(deadline, task).await
    }

    /// Runs [`Self::forced_stop`] on its own task. Dropping the returned
    /// handle does not cancel the task.
    fn spawn_forced_stop(
        self: &Arc<Self>,
        entry: Arc<VmEntry>,
        op: OpLock,
        timings: ForceTimings,
    ) -> JoinHandle<Result<VmInfo, EngineError>> {
        let inner = Arc::clone(self);
        tokio::spawn(async move { inner.forced_stop(&entry, op, timings).await })
    }

    /// The forced stop of `entry` (§3.8). `op` is the VM's lock, or the
    /// caller's place in its queue. The VMM is read only once the lock is
    /// held, so the stop never acts on a VMM a later `start` replaced.
    async fn forced_stop(
        &self,
        entry: &VmEntry,
        op: OpLock,
        timings: ForceTimings,
    ) -> Result<VmInfo, EngineError> {
        let _op = op.acquire().await;
        let id = &entry.id;
        if !entry.paths.dir.exists() {
            self.remove_entry(id);
            return Err(EngineError::NotFound);
        }
        let stopping = PhaseGuard::begin(entry, Phase::Stopping);
        let Some(vmm) = entry.live_vmm() else {
            drop(stopping);
            self.settle_crashed_exit(entry);
            return self.inspect(id).await;
        };
        request_stop(entry);
        shut_down_or_kill(entry, &vmm, timings).await?;
        drop(stopping);
        self.inspect(id).await
    }

    /// Freezes a running VM's vCPUs (§3.22). Pausing a paused VM is `Ok`.
    pub(super) async fn pause(&self, id: &VmId) -> Result<(), EngineError> {
        self.set_paused(id, true).await
    }

    /// Lets a paused VM run again (§3.22). Resuming a running VM is `Ok`.
    pub(super) async fn resume(&self, id: &VmId) -> Result<(), EngineError> {
        self.set_paused(id, false).await
    }

    /// `vm.pause` or `vm.resume` as the VMM's state needs (§3.22).
    ///
    /// The VM's operation lock is held, so a pause never races a start, a
    /// stop or a delete of the same VM (§4). A VM that is not running or
    /// paused is `Conflict`; the VMM reports `Paused` and the engine keeps
    /// reporting `Running` for it (§3.10).
    async fn set_paused(&self, id: &VmId, pause: bool) -> Result<(), EngineError> {
        let Some(entry) = self.entry(id) else {
            return Err(EngineError::NotFound);
        };
        let _op = entry.op.lock().await;
        if !entry.paths.dir.exists() {
            return Err(EngineError::NotFound);
        }
        let not_running = || EngineError::Conflict(format!("VM {} is not running", id.0));
        if entry.live_vmm().is_none() || !entry.booted.load(Ordering::SeqCst) {
            return Err(not_running());
        }
        let api = ApiClient::new(&entry.paths.api_socket).with_timeout(PAUSE_TIMEOUT);
        let document = api.info().await.map_err(|err| {
            EngineError::Hypervisor(format!(
                "cloud-hypervisor did not report VM {}: {err}",
                id.0
            ))
        })?;
        match (document.get("state").and_then(Value::as_str), pause) {
            (Some("Running"), true) => api.pause().await.map_err(|err| {
                EngineError::Hypervisor(format!(
                    "cloud-hypervisor did not pause VM {}: {err}",
                    id.0
                ))
            }),
            (Some("Paused"), false) => api.resume().await.map_err(|err| {
                EngineError::Hypervisor(format!(
                    "cloud-hypervisor did not resume VM {}: {err}",
                    id.0
                ))
            }),
            (Some("Paused"), true) | (Some("Running"), false) => Ok(()),
            _ => Err(not_running()),
        }
    }

    /// Removes a stopped VM, its disks and its runtime directory (§3.9).
    pub(super) async fn delete(&self, id: &VmId) -> Result<(), EngineError> {
        let Some(entry) = self.entry(id) else {
            return Ok(());
        };
        let _op = entry.op.lock().await;
        if !entry.paths.dir.exists() {
            self.remove_entry(id);
            return Ok(());
        }
        let info = self.inspect(id).await?;
        if matches!(
            info.state,
            VmState::Running | VmState::Starting | VmState::Stopping | VmState::Creating
        ) {
            return Err(EngineError::Conflict(format!(
                "VM {} is running; stop it before deleting",
                id.0
            )));
        }

        if let Some(vmm) = entry.live_vmm() {
            request_stop(&entry);
            let _ = ApiClient::new(&entry.paths.api_socket)
                .with_timeout(SHUTDOWN_TIMEOUT)
                .shutdown_vmm()
                .await;
            if !wait_settled(&entry, SETTLE_AFTER_DELETE_SHUTDOWN).await {
                let killed = vmm.sigkill();
                if !wait_settled(&entry, SETTLE_AFTER_SIGKILL).await {
                    return Err(killed.err().unwrap_or_else(not_exited_after_sigkill));
                }
            }
        }

        let mut bridge = None;
        if let Some(record) = state::read_json::<SpecRecord>(&entry.paths.spec)? {
            net::delete_tap(&self.tools, &record.tap).await?;
            bridge = Some(record.bridge.clone());
            if let Ok(spec) = record.to_spec() {
                for socket in guest_sockets(&spec) {
                    let _ = std::fs::remove_file(socket);
                }
            }
        }
        // Ends every open `serial_output` stream here, before the ring's
        // directory goes (§3.19).
        entry.serial.end();
        tokio::fs::remove_dir_all(&entry.paths.dir)
            .await
            .map_err(|err| io_error(&entry.paths.dir, &err))?;
        self.remove_entry(id);
        if let Some(bridge) = bridge {
            // Best effort: a stale pair names a TAP that no longer exists.
            let _ = self.sync_peers(&bridge).await;
        }
        Ok(())
    }

    /// One VM's observed state; never changes it (§3.10).
    pub(super) async fn inspect(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        let Some(entry) = self.entry(id) else {
            let paths = VmPaths::new(&self.vms_root(), id);
            if !paths.dir.exists() {
                return Err(EngineError::NotFound);
            }
            return self.inspect_record(id, &paths);
        };
        if !entry.paths.dir.exists() {
            return Err(EngineError::NotFound);
        }
        let phase = entry.phase();
        let vmm = entry.live_vmm();
        let pid = vmm.as_ref().map(|vmm| vmm.id().pid);
        if phase == Phase::Starting {
            return Ok(VmInfo {
                id: id.clone(),
                state: VmState::Starting,
                pid,
                started_at: None,
                exit_code: None,
            });
        }
        if phase == Phase::Stopping {
            return Ok(VmInfo {
                id: id.clone(),
                state: VmState::Stopping,
                pid,
                started_at: None,
                exit_code: None,
            });
        }

        let vmm_record = state::read_json::<VmmRecord>(&entry.paths.vmm)?;
        let started_at = vmm_record
            .as_ref()
            .and_then(|record| record.started_at_ms)
            .map(ms_to_system_time);
        if let Some(vmm) = vmm {
            if !entry.booted.load(Ordering::SeqCst) {
                return Ok(VmInfo {
                    id: id.clone(),
                    state: VmState::Stopped,
                    pid: Some(vmm.id().pid),
                    started_at: None,
                    exit_code: None,
                });
            }
            let api = ApiClient::new(&entry.paths.api_socket).with_timeout(INFO_TIMEOUT);
            let state = match api.info().await {
                Ok(document) => match document.get("state").and_then(Value::as_str) {
                    Some("Running") => VmState::Running,
                    Some("Created") => VmState::Stopped,
                    Some("Shutdown") => VmState::Stopping,
                    Some("Paused") => VmState::Running,
                    _ => VmState::Unknown,
                },
                Err(_) => VmState::Stopping,
            };
            return Ok(VmInfo {
                id: id.clone(),
                state,
                pid: Some(vmm.id().pid),
                started_at,
                exit_code: None,
            });
        }
        self.inspect_record(id, &entry.paths)
    }

    /// Every VM the runtime directory knows, sorted by id (§3.11).
    pub(super) async fn list(&self) -> Result<Vec<VmInfo>, EngineError> {
        let root = self.vms_root();
        let mut directory = tokio::fs::read_dir(&root)
            .await
            .map_err(|err| io_error(&root, &err))?;
        let mut ids = Vec::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(|err| io_error(&root, &err))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            ids.push(VmId(name));
        }
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        let mut infos = Vec::with_capacity(ids.len());
        for id in ids {
            match self.inspect(&id).await {
                Ok(info) => infos.push(info),
                Err(EngineError::NotFound) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(infos)
    }

    /// Opens the tagged output of one VM (§3.13).
    ///
    /// # Errors
    ///
    /// `NotFound` for a VM that does not exist or has no console, `Unsupported`
    /// for a serial console, and `Conflict` when the VM is not running or
    /// another reader already holds the output.
    pub(super) fn output(&self, id: &VmId) -> Result<LineStream, EngineError> {
        let (entry, record) = self.entry_with_spec(id)?;
        let spec = record.to_spec()?;
        match &spec.console {
            ConsoleMode::Off => Err(EngineError::NotFound),
            ConsoleMode::Serial { .. } => Err(EngineError::Unsupported(
                "output needs ConsoleMode::VirtioConsole".to_string(),
            )),
            ConsoleMode::VirtioConsole { socket } => {
                stream::open_output(&id.0, socket, Arc::clone(&entry.output_open))
            }
        }
    }

    /// Opens the interactive console of one VM (§3.14).
    ///
    /// # Errors
    ///
    /// `NotFound` for a VM that does not exist or has no console, `Unsupported`
    /// for a virtio-console, `Conflict` when a VM's serial port is a socket
    /// (the console is read through [`Self::serial_output`] in M4), and
    /// `Conflict` when the VM is not running or another client already holds
    /// the console.
    pub(super) fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError> {
        let (entry, record) = self.entry_with_spec(id)?;
        let spec = record.to_spec()?;
        // A serial socket has one client — the engine — and nothing in the
        // guest reads it, so a viewer must not take it over (§3.14, ADR 0116).
        if matches!(spec.serial, SerialMode::Socket { .. }) {
            return Err(EngineError::Conflict(format!(
                "the serial port of VM {} is read through serial_output",
                id.0
            )));
        }
        match &spec.console {
            ConsoleMode::Off => Err(EngineError::NotFound),
            ConsoleMode::VirtioConsole { .. } => Err(EngineError::Unsupported(
                "console needs ConsoleMode::Serial".to_string(),
            )),
            ConsoleMode::Serial { socket } => {
                stream::open_console(&id.0, socket, Arc::clone(&entry.console_open))
            }
        }
    }

    /// A read-only stream of one VM's serial output (§3.19).
    ///
    /// # Errors
    ///
    /// `NotFound` for an unknown id, `Conflict` when the VM's serial port is
    /// not a socket.
    pub(super) fn serial_output(
        &self,
        id: &VmId,
        tail_bytes: Option<u64>,
    ) -> Result<ByteStream, EngineError> {
        let (entry, record) = self.entry_with_spec(id)?;
        let spec = record.to_spec()?;
        match spec.serial {
            SerialMode::Socket { .. } => Ok(entry.serial.subscribe(tail_bytes)),
            SerialMode::Off | SerialMode::File { .. } => Err(EngineError::Conflict(format!(
                "VM {}'s serial port is not a socket",
                id.0
            ))),
        }
    }

    /// Starts a program on a PTY inside a running VM (§3.18).
    ///
    /// # Errors
    ///
    /// `Conflict` for a request that breaks the format, for a VM that is not
    /// running or has no vsock device, and for one whose init does not accept
    /// exec sessions; `NotFound` for an unknown id.
    pub(super) async fn exec(
        &self,
        id: &VmId,
        request: &ExecRequest,
    ) -> Result<ExecSession, EngineError> {
        let (entry, record) = self.entry_with_spec(id)?;
        let spec = record.to_spec()?;
        let Some(vsock) = &spec.vsock else {
            return Err(EngineError::Conflict(format!(
                "VM {} has no vsock device",
                id.0
            )));
        };
        if entry.live_vmm().is_none() || !entry.booted.load(Ordering::SeqCst) {
            return Err(EngineError::Conflict(format!("VM {} is not running", id.0)));
        }
        exec::open(&id.0, &vsock.socket, request).await
    }

    /// One VM's counters (§3.15).
    ///
    /// # Errors
    ///
    /// `NotFound` for a VM that does not exist. Counters are best effort: a VM
    /// without a live VMM, or one whose counters document cannot be read,
    /// reports zeros rather than an error.
    pub(super) async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError> {
        let Some(entry) = self.entry(id) else {
            // Rediscovery always keeps an entry for a runtime directory; a
            // directory without one still has no live VMM, so it reports zeros.
            return if VmPaths::new(&self.vms_root(), id).dir.exists() {
                Ok(zero_counters())
            } else {
                Err(EngineError::NotFound)
            };
        };
        if !entry.paths.dir.exists() {
            return Err(EngineError::NotFound);
        }
        let Some(vmm) = entry.live_vmm() else {
            return Ok(zero_counters());
        };
        let pid = vmm.id().pid;
        let mut counters = match ApiClient::new(&entry.paths.api_socket)
            .with_timeout(COUNTERS_TIMEOUT)
            .counters()
            .await
        {
            Ok(document) => host::counters_from_ch(&document),
            Err(_) => zero_counters(),
        };
        counters.cpu_time_ms = host::process_cpu_time_ms(pid).unwrap_or(0);
        counters.memory_rss_bytes = host::process_rss_bytes(pid).unwrap_or(0);
        Ok(counters)
    }
}

impl Inner {
    /// The runtime root, `<state_dir>/vms`.
    fn vms_root(&self) -> PathBuf {
        self.config.state_dir.join("vms")
    }

    /// The entry of one id, when create or rediscovery made one.
    fn entry(&self, id: &VmId) -> Option<Arc<VmEntry>> {
        lock(&self.vms).get(id).cloned()
    }

    /// The entry and the spec of one existing VM, for the observation methods.
    pub(super) fn entry_with_spec(
        &self,
        id: &VmId,
    ) -> Result<(Arc<VmEntry>, SpecRecord), EngineError> {
        let entry = self.entry(id).ok_or(EngineError::NotFound)?;
        if !entry.paths.dir.exists() {
            return Err(EngineError::NotFound);
        }
        let record = match entry.spec() {
            Some(record) => record,
            None => state::read_json::<SpecRecord>(&entry.paths.spec)?.ok_or_else(|| {
                EngineError::Hypervisor(format!("{}: missing spec.json", entry.paths.dir.display()))
            })?,
        };
        Ok((entry, record))
    }

    /// The entry of one id, created when it is not there yet.
    fn entry_or_insert(
        &self,
        id: &VmId,
        paths: &VmPaths,
        spec: Option<SpecRecord>,
    ) -> Arc<VmEntry> {
        let mut vms = lock(&self.vms);
        Arc::clone(
            vms.entry(id.clone())
                .or_insert_with(|| Arc::new(VmEntry::new(id.clone(), paths.clone(), spec))),
        )
    }

    /// Forgets one VM.
    fn remove_entry(&self, id: &VmId) {
        lock(&self.vms).remove(id);
    }

    /// A snapshot of every known VM, taken so no guard is held across a call.
    pub(super) fn entries(&self) -> Vec<(VmId, Arc<VmEntry>)> {
        lock(&self.vms)
            .iter()
            .map(|(id, entry)| (id.clone(), Arc::clone(entry)))
            .collect()
    }

    /// Reserves every work-disk path of `spec`, refusing a path another VM
    /// already names or a concurrent create holds (§3.6).
    ///
    /// The guard releases the paths when `create` is done with them. The
    /// caller validated the spec first.
    fn reserve_work_disks(&self, spec: &VmSpec) -> Result<WorkDiskReservation<'_>, EngineError> {
        if spec.work_disks.is_empty() {
            return Ok(WorkDiskReservation {
                inner: self,
                id: spec.id.clone(),
                paths: Vec::new(),
            });
        }
        let others: Vec<(VmId, Vec<std::path::PathBuf>)> = self
            .entries()
            .into_iter()
            .filter(|(id, _)| id != &spec.id)
            .filter_map(|(id, entry)| {
                entry.spec().map(|record| {
                    (
                        id,
                        record.spec.work_disks.into_iter().map(|d| d.path).collect(),
                    )
                })
            })
            .collect();

        {
            let mut inflight = lock(&self.inflight_disks);
            for disk in &spec.work_disks {
                for (other_id, paths) in &others {
                    if paths.contains(&disk.path) {
                        return Err(EngineError::Conflict(format!(
                            "work disk {} is already attached to VM {}",
                            disk.path.display(),
                            other_id.0
                        )));
                    }
                }
                if let Some(owner) = inflight.get(&disk.path)
                    && owner != &spec.id
                {
                    return Err(EngineError::Conflict(format!(
                        "work disk {} is being attached to VM {}",
                        disk.path.display(),
                        owner.0
                    )));
                }
            }
            for disk in &spec.work_disks {
                inflight.insert(disk.path.clone(), spec.id.clone());
            }
        }
        Ok(WorkDiskReservation {
            inner: self,
            id: spec.id.clone(),
            paths: spec
                .work_disks
                .iter()
                .map(|disk| disk.path.clone())
                .collect(),
        })
    }

    /// The checks and the reservation `create` makes before it writes anything
    /// (§3.6).
    ///
    /// Returns the ensured network the address lives in; resources are
    /// reserved here, not at `start`.
    async fn reserve(&self, spec: &VmSpec) -> Result<(NodeNetwork, Subnet), EngineError> {
        let host_cpus = std::thread::available_parallelism()
            .map(|cpus| cpus.get())
            .unwrap_or(1);
        if spec.vcpus == 0 {
            return Err(EngineError::Conflict(
                "vcpus must be at least 1".to_string(),
            ));
        }
        // A limit of the host, so `ResourceExhausted` (§3.6), not `Conflict`.
        if usize::from(spec.vcpus) > host_cpus {
            return Err(EngineError::ResourceExhausted(format!(
                "vcpus {} is more than the host's {host_cpus} CPUs",
                spec.vcpus
            )));
        }
        if spec.memory_mb < MINIMUM_MEMORY_MB {
            return Err(EngineError::Conflict(format!(
                "memory_mb {} is below the {MINIMUM_MEMORY_MB} MiB minimum",
                spec.memory_mb
            )));
        }
        if spec.disk_gb < MINIMUM_DISK_GB {
            return Err(EngineError::Conflict(format!(
                "disk_gb {} is below the {MINIMUM_DISK_GB} GiB minimum",
                spec.disk_gb
            )));
        }
        if self.images.cached(&spec.rootfs)?.is_none() {
            return Err(EngineError::Image(format!(
                "rootfs {} is not a prepared image",
                spec.rootfs.0
            )));
        }

        let network = {
            let networks = self.networks.lock().await;
            networks
                .values()
                .find(|(net, subnet)| {
                    net.gateway == spec.net.gateway
                        && spec.net.ip != net.gateway
                        && subnet.is_host(spec.net.ip)
                })
                .cloned()
        }
        .ok_or_else(|| {
            EngineError::Conflict(format!(
                "address {} is not a host address in an ensured network with gateway {}",
                spec.net.ip, spec.net.gateway
            ))
        })?;

        for (other_id, other) in self.entries() {
            if other_id == spec.id {
                continue;
            }
            let Some(record) = other.spec() else {
                continue;
            };
            let other_spec = record.to_spec()?;
            if other_spec.net.ip == spec.net.ip {
                return Err(EngineError::Conflict(format!(
                    "address {} is already used by VM {}",
                    spec.net.ip, other_id.0
                )));
            }
            if other_spec.net.mac == spec.net.mac {
                return Err(EngineError::Conflict(format!(
                    "MAC {} is already used by VM {}",
                    format_mac(spec.net.mac),
                    other_id.0
                )));
            }
        }

        // The superseded serial console and the M4 `serial` field may not both
        // claim the serial port (§3.14).
        vmconfig::check_consoles(spec)?;

        if let Some(socket) = console_socket(&spec.console) {
            check_socket_parent(socket, "console")?;
        }
        if let SerialMode::Socket { path } = &spec.serial {
            check_socket_parent(path, "serial")?;
        }
        // The vsock socket lives in the VM's runtime directory, which `create`
        // creates after this check, so only its absolute form is checked here.
        if let Some(vsock) = &spec.vsock
            && !vsock.socket.is_absolute()
        {
            return Err(EngineError::Io(format!(
                "vsock socket {} is not an absolute path",
                vsock.socket.display()
            )));
        }

        let mut requested = u64::from(spec.memory_mb);
        for (_, other) in self.entries() {
            if let Some(record) = other.spec() {
                requested = requested.saturating_add(u64::from(record.spec.memory_mb));
            }
        }
        let available = host::mem_total_mb()?.saturating_sub(self.config.reserved_memory_mb);
        if requested > available {
            return Err(EngineError::ResourceExhausted(format!(
                "requesting {requested} MiB of memory but only {available} MiB are available for VMs"
            )));
        }
        Ok(network)
    }

    /// Creates a missing work disk and formats it (§3.6). An existing file is
    /// attached as it is, which may be a raw file with no filesystem.
    async fn prepare_work_disks(&self, spec: &VmSpec) -> Result<(), EngineError> {
        for disk in &spec.work_disks {
            let path = disk.path.clone();
            match tokio::fs::symlink_metadata(&path).await {
                Ok(metadata) => {
                    if !metadata.file_type().is_file() {
                        return Err(EngineError::Conflict(format!(
                            "work disk {} exists and is not a regular file",
                            path.display()
                        )));
                    }
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_error(&path, &err)),
            }

            let create_path = path.clone();
            let size_gb = disk.size_gb;
            let created =
                tokio::task::spawn_blocking(move || create_work_disk_file(&create_path, size_gb))
                    .await
                    .map_err(|err| EngineError::Io(format!("work disk task failed: {err}")))??;
            if !created {
                // Another process created it between the check and here.
                continue;
            }

            let path_text = path.to_str().ok_or_else(|| {
                EngineError::Conflict(format!(
                    "work disk path {} is not valid UTF-8",
                    path.display()
                ))
            })?;
            let formatted = crate::ch::tools::run(
                &self.tools.mkfs_ext4,
                &[
                    "-q",
                    "-F",
                    "-t",
                    "ext4",
                    "-m",
                    "0",
                    "-E",
                    "lazy_itable_init=1,lazy_journal_init=1,nodiscard",
                    path_text,
                ],
            )
            .await;
            if let Err(err) = formatted {
                let _ = std::fs::remove_file(&path);
                return Err(err);
            }
        }
        Ok(())
    }

    /// Writes the spec, the config disk and the data disk into a fresh
    /// directory, all of it off the reactor thread.
    async fn build_files(
        &self,
        paths: &VmPaths,
        record: &SpecRecord,
        config: &quoro_init::GuestConfig,
        disk_gb: u32,
    ) -> Result<(), EngineError> {
        let paths = paths.clone();
        let record = record.clone();
        let config = config.clone();
        let mkfs_ext4 = self.tools.mkfs_ext4.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&paths.dir).map_err(|err| io_error(&paths.dir, &err))?;
            state::write_json_atomic(&paths.spec, &record)?;
            crate::boot::write_config_disk(&paths.config_disk, &config)?;
            crate::boot::create_data_disk(&mkfs_ext4, &paths.data_disk, disk_gb)
        })
        .await
        .map_err(|err| EngineError::Io(format!("create task failed: {err}")))?
    }

    /// Starts one VMM, configures the VM and hands the process to a
    /// supervisor. The caller has already created the TAP.
    async fn boot_vmm(
        self: &Arc<Self>,
        entry: &Arc<VmEntry>,
        spec: &VmSpec,
        prepared: &PreparedImage,
        record: &SpecRecord,
        deadline: Instant,
    ) -> Result<Vmm, EngineError> {
        let launch = VmmLaunch {
            binary: &self.ch_binary,
            api_socket: &entry.paths.api_socket,
            event_log: &entry.paths.event_log,
            log_file: &entry.paths.vmm_log,
        };
        remove_stale_sockets(spec);
        let vmm = Vmm::spawn_by(&launch, deadline).await?;
        let boot_files = BootFiles {
            kernel: &self.kernel,
            initramfs: &self.initramfs,
            rootfs: &prepared.rootfs_path,
        };
        let body = vmconfig::vm_create_body(
            spec,
            &entry.paths,
            &boot_files,
            &record.tap,
            &self.volumes_dir(),
        );
        if let Err(err) = ApiClient::new(&entry.paths.api_socket)
            .by(deadline)
            .create(&body)
            .await
        {
            let _ = vmm.sigkill();
            let _ = vmm.wait_exit(SETTLE_AFTER_SIGKILL).await;
            return Err(err.into());
        }
        let vmm_record = VmmRecord {
            process: vmm.id(),
            booted: false,
            started_at_ms: None,
        };
        if let Err(err) = state::write_json_atomic(&entry.paths.vmm, &vmm_record) {
            let _ = vmm.sigkill();
            let _ = vmm.wait_exit(SETTLE_AFTER_SIGKILL).await;
            return Err(err);
        }
        *lock(&entry.vmm) = Some(vmm.clone());
        entry.booted.store(false, Ordering::SeqCst);
        start_serial_client(entry, spec);
        supervise::start(Arc::clone(self), Arc::clone(entry), vmm.clone());
        Ok(vmm)
    }

    /// The state a VM with no live VMM has, from `last-exit.json` (§3.10).
    fn inspect_record(&self, id: &VmId, paths: &VmPaths) -> Result<VmInfo, EngineError> {
        let exit = state::read_json::<ExitRecord>(&paths.last_exit)?;
        let (state, exit_code) = match exit {
            Some(record) => (
                match record.state {
                    ExitState::Stopped => VmState::Stopped,
                    ExitState::Crashed => VmState::Crashed,
                },
                record.exit_code,
            ),
            None => (VmState::Stopped, None),
        };
        Ok(VmInfo {
            id: id.clone(),
            state,
            pid: None,
            started_at: None,
            exit_code,
        })
    }

    /// Rewrites a crash that a later stop explains as a requested stop (§3.8):
    /// the VM is stopped now, and its exit code is kept.
    fn settle_crashed_exit(&self, entry: &VmEntry) {
        let Ok(Some(mut record)) = state::read_json::<ExitRecord>(&entry.paths.last_exit) else {
            return;
        };
        if record.state != ExitState::Crashed {
            return;
        }
        record.state = ExitState::Stopped;
        record.reason = "stop requested after the VM had already exited".to_string();
        record.at_ms = now_ms();
        let _ = state::write_json_atomic(&entry.paths.last_exit, &record);
    }
}

/// Releases the work-disk paths one `create` reserved (§3.6).
struct WorkDiskReservation<'a> {
    inner: &'a Inner,
    id: VmId,
    paths: Vec<PathBuf>,
}

impl Drop for WorkDiskReservation<'_> {
    fn drop(&mut self) {
        let mut inflight = lock(&self.inner.inflight_disks);
        for path in &self.paths {
            if inflight.get(path) == Some(&self.id) {
                inflight.remove(path);
            }
        }
    }
}

/// Creates a sparse work-disk file and its parent directories with mode 0700.
///
/// Returns `false` when the file already exists, which makes a concurrent
/// creator a no-op rather than an error.
pub(super) fn create_work_disk_file(path: &Path, size_gb: u32) -> Result<bool, EngineError> {
    let Some(parent) = path.parent() else {
        return Err(EngineError::Conflict(format!(
            "work disk {} has no parent directory",
            path.display()
        )));
    };
    create_parent_dirs_0700(parent)?;

    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => {
            let len = u64::from(size_gb) << 30;
            if let Err(err) = file.set_len(len) {
                drop(file);
                let _ = std::fs::remove_file(path);
                return Err(if err.kind() == std::io::ErrorKind::StorageFull {
                    EngineError::ResourceExhausted(format!(
                        "{}: no space left for a {size_gb} GiB work disk",
                        path.display()
                    ))
                } else {
                    io_error(path, &err)
                });
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(io_error(path, &err)),
    }
}

/// Creates every missing directory of `dir` with mode 0700, leaving existing
/// ones alone.
fn create_parent_dirs_0700(dir: &Path) -> Result<(), EngineError> {
    use std::os::unix::fs::DirBuilderExt as _;

    if dir.as_os_str().is_empty() || dir.is_dir() {
        return Ok(());
    }
    let mut missing = Vec::new();
    let mut current = dir.to_path_buf();
    while !current.as_os_str().is_empty() && !current.exists() {
        missing.push(current.clone());
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }
    for path in missing.iter().rev() {
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(io_error(path, &err)),
        }
    }
    Ok(())
}

/// The same paths, rooted at another directory (the `.tmp-` build directory of
/// `create`).
fn rebased(paths: &VmPaths, dir: &Path) -> VmPaths {
    fn moved(path: &Path, dir: &Path) -> PathBuf {
        match path.file_name() {
            Some(name) => dir.join(name),
            None => dir.to_path_buf(),
        }
    }
    VmPaths {
        dir: dir.to_path_buf(),
        spec: moved(&paths.spec, dir),
        vmm: moved(&paths.vmm, dir),
        stop_requested: moved(&paths.stop_requested, dir),
        last_exit: moved(&paths.last_exit, dir),
        config_disk: moved(&paths.config_disk, dir),
        data_disk: moved(&paths.data_disk, dir),
        api_socket: moved(&paths.api_socket, dir),
        event_log: moved(&paths.event_log, dir),
        serial_log: moved(&paths.serial_log, dir),
        vmm_log: moved(&paths.vmm_log, dir),
    }
}

/// Checks that a socket path a spec hands to Cloud Hypervisor is absolute and
/// that its directory exists, so the VMM can bind it.
fn check_socket_parent(socket: &Path, what: &str) -> Result<(), EngineError> {
    if !socket.is_absolute() {
        return Err(EngineError::Io(format!(
            "{what} socket {} is not an absolute path",
            socket.display()
        )));
    }
    let parent = socket.parent();
    if !parent.is_some_and(Path::exists) {
        return Err(EngineError::Io(format!(
            "{what} socket directory {} does not exist",
            parent
                .map(|parent| parent.display().to_string())
                .unwrap_or_default()
        )));
    }
    Ok(())
}

/// The console socket a spec names, if it names one.
fn console_socket(mode: &ConsoleMode) -> Option<&Path> {
    match mode {
        ConsoleMode::Off => None,
        ConsoleMode::VirtioConsole { socket } | ConsoleMode::Serial { socket } => Some(socket),
    }
}

/// Every host-side socket path a spec names, for `delete` and for clearing a
/// stale socket before a VMM starts: the console socket, the serial socket and
/// the hybrid vsock socket (§3.6, §3.19).
fn guest_sockets(spec: &VmSpec) -> Vec<PathBuf> {
    let mut sockets: Vec<PathBuf> = console_socket(&spec.console)
        .map(Path::to_path_buf)
        .into_iter()
        .collect();
    if let SerialMode::Socket { path } = &spec.serial {
        sockets.push(path.clone());
    }
    if let Some(vsock) = &spec.vsock {
        sockets.push(vsock.socket.clone());
    }
    sockets
}

/// Removes the socket files a previous VMM left behind.
///
/// A stale UNIX socket makes Cloud Hypervisor's bind fail with `EADDRINUSE`.
/// This is only called before a VMM of this spec is spawned, when nothing can
/// be listening on them.
fn remove_stale_sockets(spec: &VmSpec) {
    for socket in guest_sockets(spec) {
        let _ = std::fs::remove_file(socket);
    }
}

/// Starts the serial client of a spec whose serial port is a socket.
///
/// Called after every VMM start and for every running VM rediscovery adopts,
/// so the ring covers the whole boot and the gap while the agent restarts
/// (§3.19). A spec with a file or no serial port has nothing to read.
fn start_serial_client(entry: &VmEntry, spec: &VmSpec) {
    if let SerialMode::Socket { path } = &spec.serial {
        entry.serial.start_client(path.clone());
    }
}

/// Formats a MAC address the way `spec.json` stores it.
fn format_mac(mac: crate::MacAddr) -> String {
    let [a, b, c, d, e, f] = mac.0;
    format!("{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}")
}

/// True when two subnets share any address.
fn overlaps(a: &Subnet, b: &Subnet) -> bool {
    let prefix = a.prefix_len.min(b.prefix_len);
    let shift = 32 - u32::from(prefix);
    // `checked_shr` because a /0 prefix would shift by 32.
    u32::from(a.network).checked_shr(shift).unwrap_or(0)
        == u32::from(b.network).checked_shr(shift).unwrap_or(0)
}

/// A forced stop's place in the queue of a VM's operation lock.
type QueuedLock = Pin<Box<dyn Future<Output = OwnedMutexGuard<()>> + Send>>;

/// The VM's operation lock as a forced stop's task receives it.
enum OpLock {
    /// Held: taken at once, or handed over by a graceful stop.
    Held(OwnedMutexGuard<()>),
    /// Queued for by the caller, so the task keeps the caller's place, and
    /// counted as waiting until it holds the lock.
    Queued(QueuedLock, ForceWaiting),
}

/// Takes a place in the queue of `entry`'s operation lock without yielding:
/// the lock itself when it is free, otherwise the acquisition, polled once.
///
/// The first poll of tokio's `Mutex::lock_owned` links the waiter into the
/// mutex's FIFO queue, or takes the lock, before anything else can yield; the
/// pinned future keeps that place when it moves into a task. This is how
/// tokio's semaphore behaves, not documented API, so
/// `a_queued_lock_keeps_its_place` pins it.
async fn queue_for_lock(entry: &VmEntry) -> Result<OwnedMutexGuard<()>, QueuedLock> {
    let mut lock: QueuedLock = Box::pin(Arc::clone(&entry.op).lock_owned());
    match std::future::poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await {
        Poll::Ready(op) => Ok(op),
        Poll::Pending => Err(lock),
    }
}

impl OpLock {
    /// The lock, once held. A queued forced stop stops counting as waiting
    /// as soon as it holds it.
    async fn acquire(self) -> OwnedMutexGuard<()> {
        match self {
            Self::Held(op) => op,
            Self::Queued(lock, waiting) => {
                let op = lock.await;
                drop(waiting);
                op
            }
        }
    }
}

/// Counts a forced stop in [`VmEntry::force_waiting`] until it is dropped.
struct ForceWaiting(Arc<VmEntry>);

impl ForceWaiting {
    fn begin(entry: &Arc<VmEntry>) -> Self {
        entry.force_waiting.send_modify(|waiting| *waiting += 1);
        Self(Arc::clone(entry))
    }
}

impl Drop for ForceWaiting {
    fn drop(&mut self) {
        self.0.force_waiting.send_modify(|waiting| *waiting -= 1);
    }
}

/// Records that a stop of `entry`'s VMM was requested: in memory first, so the
/// exit is a stop even when the marker cannot be written, then in the marker
/// file, which outlives the agent (§3.11). A marker that cannot be written
/// never holds back the stop.
fn request_stop(entry: &VmEntry) {
    entry.stop_requested.store(true, Ordering::SeqCst);
    let _ = state::touch_marker(&entry.paths.stop_requested);
}

/// Holds a VM in [`Phase::Starting`] or [`Phase::Stopping`] and puts it back
/// to [`Phase::Idle`] when dropped, so a start or a stop whose caller stops
/// waiting, or that fails, never leaves the VM reported as `Starting` or
/// `Stopping` (#434).
struct PhaseGuard<'a> {
    entry: &'a VmEntry,
    armed: bool,
}

impl<'a> PhaseGuard<'a> {
    fn begin(entry: &'a VmEntry, phase: Phase) -> Self {
        entry.set_phase(phase);
        Self { entry, armed: true }
    }

    /// Leaves the VM `Stopping` for the forced stop that takes over.
    fn hand_over(mut self) {
        self.armed = false;
    }
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.entry.set_phase(Phase::Idle);
        }
    }
}

/// The timings of one forced stop's escalation (§3.8), derived from the
/// caller's `within` (ADR 0243).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ForceTimings {
    /// Timeout of the `vmm.shutdown` call. A VMM that does not answer is
    /// killed at once.
    shutdown: Duration,
    /// How long a VMM that answered gets to exit before it is killed.
    exit_after_shutdown: Duration,
    /// How long the stop's task waits for the killed VMM's exit, whatever
    /// the caller's bound: the task goes on after the caller got `Timeout`.
    reap: Duration,
}

impl ForceTimings {
    /// `SIGKILL` at most [`FORCE_KILL_BOUND`] after the forced stop holds the
    /// lock, and failure after [`FORCE_STOP_BOUND`].
    const DEFAULT: Self = Self {
        shutdown: FORCE_SHUTDOWN_TIMEOUT,
        exit_after_shutdown: FORCE_EXIT_AFTER_SHUTDOWN,
        reap: SETTLE_AFTER_SIGKILL,
    };

    /// The escalation of a forced stop bounded by `within`: `SIGKILL` goes
    /// out by half of `within`, and no later than [`FORCE_KILL_BOUND`],
    /// leaving the other half for the reap, which takes milliseconds for a
    /// killed process. The task still waits up to `reap` for it; the caller
    /// gets `Timeout` at `within`. Any bound of twice [`FORCE_KILL_BOUND`] or
    /// more gives [`Self::DEFAULT`].
    fn within(within: Duration) -> Self {
        let kill_by = (within / 2).min(FORCE_KILL_BOUND);
        // The default's split: 100 ms of the 250 for the `vmm.shutdown` call.
        let shutdown = kill_by * 2 / 5;
        Self {
            shutdown,
            exit_after_shutdown: kill_by - shutdown,
            reap: SETTLE_AFTER_SIGKILL,
        }
    }
}

/// Waits for a forced stop's task until `deadline` (§3.8): `Timeout` once it
/// has passed, while the task goes on to the kill and the reap.
async fn finish_by(
    deadline: Instant,
    task: JoinHandle<Result<VmInfo, EngineError>>,
) -> Result<VmInfo, EngineError> {
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => Err(EngineError::Hypervisor(format!(
            "the forced stop did not finish: {err}"
        ))),
        Err(_) => Err(EngineError::Timeout),
    }
}

/// Decides between a bounded start's caller and its task (§3.7, ADR 0242):
/// either the task reports a running VM before the deadline, or the caller
/// gives the start up, never both.
#[derive(Default)]
struct StartGate(AtomicU8);

impl StartGate {
    const WAITING: u8 = 0;
    const RUNNING: u8 = 1;
    const GIVEN_UP: u8 = 2;

    /// The task's side, once the VM runs: true when the caller still waits
    /// for it before `deadline`.
    fn commit(&self, deadline: Instant) -> bool {
        Instant::now() < deadline
            && self
                .0
                .compare_exchange(
                    Self::WAITING,
                    Self::RUNNING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
    }

    /// The caller's side: gives the start up, unless the VM already runs.
    fn give_up(&self) -> bool {
        self.0
            .compare_exchange(
                Self::WAITING,
                Self::GIVEN_UP,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    /// Whether the task should start nothing more: the caller gave up, or
    /// `deadline` passed.
    fn given_up(&self, deadline: Instant) -> bool {
        self.0.load(Ordering::SeqCst) == Self::GIVEN_UP || Instant::now() >= deadline
    }
}

/// Gives a start up when its caller's future is dropped (§4).
struct GiveUpOnDrop(Arc<StartGate>);

impl Drop for GiveUpOnDrop {
    fn drop(&mut self) {
        self.0.give_up();
    }
}

/// The escalation of a forced stop (§3.8): `vmm.shutdown`, then `SIGKILL`
/// unless the VMM answered and exited within `exit_after_shutdown`, then a
/// bounded wait for the exit. `SIGKILL` therefore goes out at most
/// `shutdown + exit_after_shutdown` after the call, and at once when the API
/// does not answer. `Ok` once the exit is recorded.
///
/// A `SIGKILL` that could not be sent, for one because `/proc` cannot tell
/// whether the pid is still the VMM, ends the stop with that error at once:
/// nothing would make the VMM exit in the reap wait.
async fn shut_down_or_kill(
    entry: &VmEntry,
    vmm: &Vmm,
    timings: ForceTimings,
) -> Result<(), EngineError> {
    let answered = ApiClient::new(&entry.paths.api_socket)
        .with_timeout(timings.shutdown)
        .shutdown_vmm()
        .await
        .is_ok();
    if answered && wait_settled(entry, timings.exit_after_shutdown).await {
        return Ok(());
    }
    if let Err(err) = vmm.sigkill() {
        return if wait_settled(entry, Duration::ZERO).await {
            Ok(())
        } else {
            Err(err)
        };
    }
    if wait_settled(entry, timings.reap).await {
        Ok(())
    } else {
        Err(not_exited_after_sigkill())
    }
}

/// A VMM that did not exit within the reap wait after `SIGKILL`.
fn not_exited_after_sigkill() -> EngineError {
    EngineError::Hypervisor("VMM process did not exit after SIGKILL".to_string())
}

/// Waits until the supervisor of `entry` finished the exit path.
///
/// The subscription happens before the `vmm` check, so an exit that lands
/// between the two is still seen.
async fn wait_settled(entry: &VmEntry, timeout: Duration) -> bool {
    let mut settled = entry.settled.subscribe();
    let current = *settled.borrow_and_update();
    if lock(&entry.vmm).is_none() {
        return true;
    }
    matches!(
        tokio::time::timeout(timeout, settled.wait_for(|value| *value > current)).await,
        Ok(Ok(_))
    )
}

/// True when the VMM reports the VM `Paused` (§3.22). An unanswered probe
/// reads as not paused, so the stop asks the guest as before.
async fn is_paused(socket: &Path) -> bool {
    ApiClient::new(socket)
        .with_timeout(PAUSED_PROBE_TIMEOUT)
        .info()
        .await
        .is_ok_and(|document| document.get("state").and_then(Value::as_str) == Some("Paused"))
}

/// Locks a `std` mutex, taking the value even when an earlier holder panicked:
/// a poisoned map of VMs is still a map of VMs.
pub(super) fn lock<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// The recorded network to recreate before a VM on `bridge` starts: only when
/// the host lacks the bridge and this engine recorded it. A bridge the engine
/// never recorded stays missing, and attaching the TAP reports the error.
fn recorded_bridge_to_restore(
    bridge: &str,
    exists: bool,
    networks: &HashMap<String, (NodeNetwork, Subnet)>,
) -> Option<NodeNetwork> {
    if exists {
        return None;
    }
    networks.get(bridge).map(|(net, _)| net.clone())
}

/// Reads `networks.json`; a missing file is an empty set.
fn load_networks(path: &Path) -> Result<HashMap<String, (NodeNetwork, Subnet)>, EngineError> {
    let Some(file) = state::read_json::<NetworksFile>(path)? else {
        return Ok(HashMap::new());
    };
    let mut networks = HashMap::new();
    for record in file.networks {
        let subnet = Subnet::parse(&record.subnet).map_err(|_| {
            EngineError::Hypervisor(format!(
                "corrupt {NETWORKS_FILE}: invalid subnet {}",
                record.subnet
            ))
        })?;
        networks.insert(
            record.bridge.clone(),
            (
                NodeNetwork {
                    bridge: record.bridge,
                    subnet: record.subnet,
                    gateway: record.gateway,
                    dns: record.dns,
                },
                subnet,
            ),
        );
    }
    Ok(networks)
}

/// Writes `networks.json`, sorted by bridge so the file is stable.
fn persist_networks(
    path: &Path,
    networks: &HashMap<String, (NodeNetwork, Subnet)>,
) -> Result<(), EngineError> {
    let mut records: Vec<NetworkRecord> = networks
        .values()
        .map(|(net, _)| NetworkRecord {
            bridge: net.bridge.clone(),
            subnet: net.subnet.clone(),
            gateway: net.gateway,
            dns: net.dns.clone(),
        })
        .collect();
    records.sort_by(|a, b| a.bridge.cmp(&b.bridge));
    state::write_json_atomic(path, &NetworksFile { networks: records })
}

/// An `Io` error naming the path it happened on.
pub(super) fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

/// The counters of a VM that has no live VMM, or whose counters are unreadable.
fn zero_counters() -> VmCounters {
    VmCounters {
        cpu_time_ms: 0,
        memory_rss_bytes: 0,
        net_rx_bytes: 0,
        net_tx_bytes: 0,
        block_read_bytes: 0,
        block_write_bytes: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(bridge: &str, subnet: &str, gateway: [u8; 4]) -> (String, (NodeNetwork, Subnet)) {
        let net = NodeNetwork {
            bridge: bridge.to_owned(),
            subnet: subnet.to_owned(),
            gateway: Ipv4Addr::from(gateway),
            dns: vec![Ipv4Addr::new(192, 0, 2, 53)],
        };
        let parsed = Subnet::parse(subnet).expect("valid subnet");
        (bridge.to_owned(), (net, parsed))
    }

    /// After a reboot the host has no bridge at all; a VM still recorded on the
    /// node's previous bridge gets that bridge back with its own subnet, not the
    /// node's current one.
    #[test]
    fn a_missing_recorded_bridge_is_restored_with_its_own_network() {
        let networks: HashMap<_, _> = [
            recorded("quoro0", "10.42.0.0/24", [10, 42, 0, 1]),
            recorded("quoro0s1", "10.42.1.0/24", [10, 42, 1, 1]),
        ]
        .into_iter()
        .collect();
        let restored = recorded_bridge_to_restore("quoro0", false, &networks)
            .expect("the previous bridge is restored");
        assert_eq!(restored, networks["quoro0"].0);
        assert_eq!(restored.subnet, "10.42.0.0/24");
    }

    #[test]
    fn a_bridge_the_host_still_has_is_left_alone() {
        let networks: HashMap<_, _> = [recorded("quoro0", "10.42.0.0/24", [10, 42, 0, 1])]
            .into_iter()
            .collect();
        assert_eq!(recorded_bridge_to_restore("quoro0", true, &networks), None);
    }

    /// A stand-in for one VM: a `sleep` process adopted as its VMM, a
    /// supervisor that records the exit the way `supervise::finish` does, and
    /// the API socket path a test serves (or leaves dead).
    struct StandIn {
        root: PathBuf,
        entry: Arc<VmEntry>,
        vmm: Vmm,
        child: Option<std::process::Child>,
    }

    impl StandIn {
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("quoro-force-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            let id = VmId(format!("vm_{name}"));
            let paths = VmPaths::new(&root, &id);
            std::fs::create_dir_all(&paths.dir).expect("runtime dir");
            let entry = Arc::new(VmEntry::new(id, paths, None));
            let child = std::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .expect("sleep");
            let process = crate::ch::vmm::ProcessId::of(child.id())
                .expect("stat")
                .expect("alive");
            let vmm = Vmm::adopt(process).expect("adopt").expect("adopted");
            *lock(&entry.vmm) = Some(vmm.clone());
            let supervised = Arc::clone(&entry);
            let watched = vmm.clone();
            tokio::spawn(async move {
                while watched.wait_exit(Duration::from_secs(60)).await.is_none() {}
                *lock(&supervised.vmm) = None;
                supervised
                    .settled
                    .send_modify(|value| *value = value.wrapping_add(1));
            });
            Self {
                root,
                entry,
                vmm,
                child: Some(child),
            }
        }

        fn pid(&self) -> u32 {
            self.child.as_ref().map_or(0, std::process::Child::id)
        }

        /// Kills and reaps the stand-in VMM, and returns the signal that
        /// ended it.
        fn signal_now(mut self) -> Option<i32> {
            use std::os::unix::process::ExitStatusExt as _;
            let mut child = self.child.take().expect("child");
            child.kill().expect("kill");
            child.wait().expect("wait").signal()
        }

        /// Reaps the stand-in VMM and returns the signal that ended it.
        fn signal(mut self) -> Option<i32> {
            use std::os::unix::process::ExitStatusExt as _;
            let status = self.child.take().expect("child").wait().expect("wait");
            status.signal()
        }
    }

    impl Drop for StandIn {
        /// A failed assertion leaves no `sleep` and no directory behind.
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    /// Accepts API connections on `socket` and answers none of them: a hung
    /// VMM.
    fn silent_api(socket: &Path) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::UnixListener::bind(socket).expect("bind");
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        })
    }

    /// Answers every API request with `204`, then runs `after` once: a VMM
    /// that takes the shutdown request.
    fn answering_api(
        socket: &Path,
        after: impl FnOnce() + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::UnixListener::bind(socket).expect("bind");
        tokio::spawn(async move {
            let mut after = Some(after);
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![0_u8; 4096];
                let _ = stream.read(&mut request).await;
                let _ = stream.write_all(b"HTTP/1.1 204 \r\n\r\n").await;
                if let Some(after) = after.take() {
                    after();
                }
            }
        })
    }

    const SIGKILL: i32 = 9;
    const SIGTERM: i32 = 15;
    /// Slack over [`FORCE_KILL_BOUND`] for the kill and the reap on a busy
    /// machine; the old escalation took 2 s and 3 s here.
    const SLACK: Duration = Duration::from_millis(250);

    /// ADR 0243: a forced stop bounded by `within` sends `SIGKILL` by half of
    /// it, and any bound of 500 ms or more keeps the default escalation.
    #[test]
    fn force_timings_follow_the_bound() {
        assert_eq!(
            ForceTimings::within(FORCE_STOP_BOUND),
            ForceTimings::DEFAULT
        );
        assert_eq!(
            ForceTimings::within(Duration::from_millis(500)),
            ForceTimings::DEFAULT
        );
        let fence = ForceTimings::within(Duration::from_millis(300));
        assert_eq!(fence.shutdown, Duration::from_millis(60));
        assert_eq!(fence.exit_after_shutdown, Duration::from_millis(90));
        assert_eq!(fence.reap, SETTLE_AFTER_SIGKILL);
        let none = ForceTimings::within(Duration::ZERO);
        assert_eq!(none.shutdown + none.exit_after_shutdown, Duration::ZERO);
    }

    /// ADR 0242: a start's task reports a running VM only while its caller
    /// still waits, and a caller gives up only a start that is not running.
    #[test]
    fn a_start_gate_lets_one_side_win() {
        let later = Instant::now() + Duration::from_secs(60);
        let committed = StartGate::default();
        assert!(committed.commit(later));
        assert!(!committed.give_up(), "a running VM is not given up");
        assert!(!committed.given_up(later));

        let abandoned = StartGate::default();
        assert!(abandoned.give_up());
        assert!(abandoned.given_up(later));
        assert!(!abandoned.commit(later), "a given-up start reports no VM");

        let late = StartGate::default();
        assert!(late.given_up(Instant::now()), "the deadline has passed");
        assert!(!late.commit(Instant::now()));

        let dropped = Arc::new(StartGate::default());
        drop(GiveUpOnDrop(Arc::clone(&dropped)));
        assert!(dropped.given_up(later), "a dropped caller gives up");
    }

    /// A fence's `T_force` of 300 ms: a VMM whose API does not answer is
    /// killed by 150 ms, so its reap fits in the rest of the bound.
    #[tokio::test]
    async fn a_forced_stop_within_300_ms_kills_by_half_of_it() {
        let vm = StandIn::new("fence");
        silent_api(&vm.entry.paths.api_socket);
        let began = std::time::Instant::now();
        shut_down_or_kill(
            &vm.entry,
            &vm.vmm,
            ForceTimings::within(Duration::from_millis(300)),
        )
        .await
        .expect("the VMM was reaped");
        let took = began.elapsed();
        assert!(
            took < Duration::from_millis(150) + SLACK,
            "the kill took {took:?}"
        );
        assert_eq!(vm.signal(), Some(SIGKILL));
    }

    /// #459: a VMM whose `/proc` entry cannot be read stays live, so
    /// `inspect` keeps its pid, and a forced stop neither signals it nor
    /// reports it gone: it fails at once with the reason, and the fence
    /// reports `fence_failed`. Proof that the process is gone still counts.
    #[tokio::test]
    async fn a_vmm_proc_cannot_tell_about_is_not_taken_for_stopped() {
        use crate::ch::vmm::proc_faults::{Fault, inject};

        let mut vm = StandIn::new("unreadable");
        let api = silent_api(&vm.entry.paths.api_socket);
        {
            let _fault = inject(
                vm.pid(),
                Fault::Errno(rustix::io::Errno::MFILE.raw_os_error()),
            );
            assert!(vm.entry.live_vmm().is_some(), "the VMM stays live");
            let began = std::time::Instant::now();
            let result = shut_down_or_kill(
                &vm.entry,
                &vm.vmm,
                ForceTimings::within(Duration::from_millis(300)),
            )
            .await;
            match result {
                Err(EngineError::Hypervisor(message)) => assert!(
                    message.contains("cannot kill") && message.contains("cannot tell"),
                    "{message}"
                ),
                other => panic!("{other:?}"),
            }
            let took = began.elapsed();
            assert!(
                took < Duration::from_millis(150) + SLACK,
                "no reap wait: {took:?}"
            );
            let child = vm.child.as_mut().expect("child");
            assert!(child.try_wait().expect("try_wait").is_none(), "still runs");
            assert!(lock(&vm.entry.vmm).is_some(), "the VMM is not forgotten");
        }
        {
            let _fault = inject(
                vm.pid(),
                Fault::Errno(rustix::io::Errno::SRCH.raw_os_error()),
            );
            assert!(vm.entry.live_vmm().is_none(), "ESRCH is proof");
        }
        shut_down_or_kill(&vm.entry, &vm.vmm, ForceTimings::DEFAULT)
            .await
            .expect("the VMM exited");
        api.abort();
        assert_eq!(vm.signal(), Some(SIGKILL));
    }

    /// #459: at start-up, a VMM `/proc` cannot tell about is not forgotten.
    /// The engine refuses to open with a retryable error that names the VM,
    /// and a VMM that is gone is recorded as exited, as before.
    #[tokio::test]
    async fn a_vmm_left_behind_that_proc_cannot_tell_about_blocks_the_start_up() {
        use crate::ch::vmm::proc_faults::{Fault, inject};

        let vm = StandIn::new("adopt");
        let process = vm.vmm.id();
        let id = VmId("vm_adopt".to_string());
        {
            let _fault = inject(
                process.pid,
                Fault::Errno(rustix::io::Errno::MFILE.raw_os_error()),
            );
            match adopt_recorded(&id, process) {
                Err(OpenError::Retry(EngineError::Hypervisor(message))) => assert!(
                    message.starts_with("VM vm_adopt: cannot adopt cloud-hypervisor pid")
                        && message.contains("cannot tell whether it still runs"),
                    "{message}"
                ),
                Err(other) => panic!("{other:?}"),
                Ok(adopted) => panic!("adopted {:?}", adopted.map(|vmm| vmm.id())),
            }
        }
        assert!(adopt_recorded(&id, process).expect("adopt").is_some());
        assert_eq!(vm.signal_now(), Some(SIGKILL));
        assert!(adopt_recorded(&id, process).expect("adopt").is_none());
    }

    /// A forced stop queued for a held lock keeps its place when its
    /// acquisition moves into a task, so a call queued after it never runs
    /// first; and it counts as waiting only until it holds the lock.
    #[tokio::test]
    async fn a_queued_lock_keeps_its_place() {
        let vm = StandIn::new("queue");
        let held = Arc::clone(&vm.entry.op).lock_owned().await;
        let waiting = ForceWaiting::begin(&vm.entry);
        let queued = queue_for_lock(&vm.entry)
            .await
            .expect_err("the lock is held");
        assert_eq!(*vm.entry.force_waiting.borrow(), 1);

        let order = Arc::new(StdMutex::new(Vec::new()));
        let later = {
            let op = Arc::clone(&vm.entry.op);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                let _op = op.lock_owned().await;
                order.lock().expect("order").push("later");
            })
        };
        // The later call is queued behind the forced stop.
        tokio::task::yield_now().await;
        let first = {
            let order = Arc::clone(&order);
            let entry = Arc::clone(&vm.entry);
            tokio::spawn(async move {
                let _op = OpLock::Queued(queued, waiting).acquire().await;
                assert_eq!(*entry.force_waiting.borrow(), 0);
                order.lock().expect("order").push("queued");
            })
        };
        drop(held);
        first.await.expect("queued");
        later.await.expect("later");
        assert_eq!(*order.lock().expect("order"), ["queued", "later"]);
        assert_eq!(*vm.entry.force_waiting.borrow(), 0);
        assert!(queue_for_lock(&vm.entry).await.is_ok(), "the lock is free");
    }

    /// #434: a VMM whose API does not answer is killed after the short
    /// shutdown timeout, not after 2 s + 3 s.
    #[tokio::test]
    async fn a_forced_stop_kills_a_vmm_whose_api_does_not_answer() {
        let vm = StandIn::new("silent");
        let api = silent_api(&vm.entry.paths.api_socket);
        let began = std::time::Instant::now();
        shut_down_or_kill(&vm.entry, &vm.vmm, ForceTimings::DEFAULT)
            .await
            .expect("the VMM exited");
        let took = began.elapsed();
        assert!(took < FORCE_KILL_BOUND + SLACK, "SIGKILL after {took:?}");
        api.abort();
        assert_eq!(vm.signal(), Some(SIGKILL));
    }

    /// #434: a VMM with no API socket at all is killed at once.
    #[tokio::test]
    async fn a_forced_stop_kills_a_vmm_without_an_api_at_once() {
        let vm = StandIn::new("noapi");
        let began = std::time::Instant::now();
        shut_down_or_kill(&vm.entry, &vm.vmm, ForceTimings::DEFAULT)
            .await
            .expect("the VMM exited");
        let took = began.elapsed();
        assert!(
            took < FORCE_SHUTDOWN_TIMEOUT + SLACK,
            "SIGKILL after {took:?}"
        );
        assert_eq!(vm.signal(), Some(SIGKILL));
    }

    /// #434: a VMM that takes the shutdown request but does not exit is
    /// killed after the short exit wait, not after 3 s.
    #[tokio::test]
    async fn a_forced_stop_kills_a_vmm_that_answers_but_does_not_exit() {
        let vm = StandIn::new("stuck");
        let api = answering_api(&vm.entry.paths.api_socket, || {});
        let began = std::time::Instant::now();
        shut_down_or_kill(&vm.entry, &vm.vmm, ForceTimings::DEFAULT)
            .await
            .expect("the VMM exited");
        let took = began.elapsed();
        assert!(
            took >= FORCE_EXIT_AFTER_SHUTDOWN,
            "killed before the exit wait: {took:?}"
        );
        assert!(took < FORCE_KILL_BOUND + SLACK, "SIGKILL after {took:?}");
        api.abort();
        assert_eq!(vm.signal(), Some(SIGKILL));
    }

    /// A VMM that exits after `vmm.shutdown` is never killed.
    #[tokio::test]
    async fn a_forced_stop_does_not_kill_a_vmm_that_shuts_down() {
        let vm = StandIn::new("clean");
        let pid =
            rustix::process::Pid::from_raw(i32::try_from(vm.pid()).expect("pid")).expect("pid");
        let api = answering_api(&vm.entry.paths.api_socket, move || {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        });
        shut_down_or_kill(&vm.entry, &vm.vmm, ForceTimings::DEFAULT)
            .await
            .expect("the VMM exited");
        api.abort();
        assert_eq!(vm.signal(), Some(SIGTERM));
    }

    #[test]
    fn a_bridge_the_engine_never_recorded_is_not_invented() {
        let networks: HashMap<_, _> = [recorded("quoro0s1", "10.42.1.0/24", [10, 42, 1, 1])]
            .into_iter()
            .collect();
        assert_eq!(recorded_bridge_to_restore("quoro0", false, &networks), None);
    }

    /// A VMM's API that keeps its VM's state: `vm.info` reports it, and
    /// `vm.pause` and `vm.resume` change it. Every request's endpoint is
    /// recorded. With `pause_fails`, `vm.pause` pauses the VM but answers
    /// `500`, as when the answer of a pause that took effect is lost.
    struct StatefulApi {
        calls: Arc<StdMutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl StatefulApi {
        fn serve(socket: &Path, state: &'static str, pause_fails: bool) -> Self {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let listener = tokio::net::UnixListener::bind(socket).expect("bind");
            let calls = Arc::new(StdMutex::new(Vec::new()));
            let recorded = Arc::clone(&calls);
            let task = tokio::spawn(async move {
                let mut state = state;
                while let Ok((mut stream, _)) = listener.accept().await {
                    let mut request = vec![0_u8; 4096];
                    let read = stream.read(&mut request).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&request[..read]).into_owned();
                    let endpoint = request
                        .split_whitespace()
                        .nth(1)
                        .and_then(|path| path.rsplit('/').next())
                        .unwrap_or_default()
                        .to_owned();
                    let answer = match endpoint.as_str() {
                        "vm.info" => {
                            let body = format!("{{\"state\":\"{state}\"}}");
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                                body.len()
                            )
                        }
                        "vm.pause" => {
                            state = "Paused";
                            if pause_fails {
                                "HTTP/1.1 500 \r\nContent-Length: 0\r\n\r\n".to_owned()
                            } else {
                                "HTTP/1.1 204 \r\n\r\n".to_owned()
                            }
                        }
                        "vm.resume" => {
                            state = "Running";
                            "HTTP/1.1 204 \r\n\r\n".to_owned()
                        }
                        _ => "HTTP/1.1 404 \r\nContent-Length: 0\r\n\r\n".to_owned(),
                    };
                    lock(&recorded).push(endpoint);
                    let _ = stream.write_all(answer.as_bytes()).await;
                }
            });
            Self { calls, task }
        }

        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }
    }

    impl Drop for StatefulApi {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// A volume file with some data in `vm`'s directory, and where its
    /// snapshot goes.
    fn volume_files(vm: &StandIn) -> (PathBuf, PathBuf) {
        let source = vm.root.join("vol_1");
        std::fs::write(&source, vec![7_u8; 64 * 1024]).expect("volume");
        (source, vm.root.join("vol_1.snap"))
    }

    fn plain_copy(source: &Path, clone: &Path) -> Result<u64, EngineError> {
        std::fs::copy(source, clone).map_err(|err| EngineError::Io(err.to_string()))
    }

    fn vol_1() -> crate::VolumeId {
        crate::VolumeId("vol_1".to_owned())
    }

    /// Polls `check` every 10 ms until it holds or `within` passes.
    async fn eventually(within: Duration, check: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        check()
    }

    /// Sections 3.16 and 4: a `PauseCopy` snapshot pauses the VM under its
    /// operation lock, so a stop that holds the lock is waited out before
    /// the VM's state is read. The lock is free during the copy, so a stop
    /// or a fence of the paused VM is not held up by it.
    #[tokio::test]
    async fn a_snapshot_pause_waits_for_the_vm_lock_and_frees_it_for_the_copy() {
        let vm = StandIn::new("snaplock");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", false);
        let (source, clone) = volume_files(&vm);

        let stop = Arc::clone(&vm.entry.op).lock_owned().await;
        let free_during_copy = Arc::new(AtomicBool::new(false));
        let snapshot = {
            let free = Arc::clone(&free_during_copy);
            let entry = Arc::clone(&vm.entry);
            let vms = vec![Arc::clone(&vm.entry)];
            let (source, clone) = (source.clone(), clone.clone());
            tokio::spawn(async move {
                crate::ch::volume::pause_and_copy(&vms, &vol_1(), &source, &clone, move |s, c| {
                    free.store(entry.op.try_lock().is_ok(), Ordering::SeqCst);
                    plain_copy(s, c)
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            api.calls().is_empty(),
            "nothing while a stop holds the lock"
        );

        drop(stop);
        let pause = snapshot.await.expect("task").expect("snapshot");
        assert_eq!(api.calls(), ["vm.info", "vm.pause", "vm.resume"]);
        assert!(pause > Duration::ZERO);
        assert!(
            free_during_copy.load(Ordering::SeqCst),
            "the copy holds no lock"
        );
        assert_eq!(
            std::fs::read(&clone).expect("snapshot"),
            std::fs::read(&source).expect("volume")
        );
    }

    /// Section 3.16: the VM is resumed whatever the copy's outcome, also when
    /// the snapshot's caller drops its future during the copy. The snapshot
    /// file nobody received is removed.
    #[tokio::test]
    async fn a_dropped_snapshot_still_resumes_the_vm_and_removes_its_file() {
        let vm = StandIn::new("snapdrop");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", false);
        let (source, clone) = volume_files(&vm);

        let (copying, copy_started) = std::sync::mpsc::channel();
        let snapshot = tokio::spawn(crate::ch::volume::spawn_pause_copy(
            vec![Arc::clone(&vm.entry)],
            vol_1(),
            source,
            clone.clone(),
            move |s, c| {
                let copied = plain_copy(s, c);
                let _ = copying.send(());
                std::thread::sleep(Duration::from_millis(300));
                copied
            },
        ));
        tokio::task::spawn_blocking(move || copy_started.recv())
            .await
            .expect("task")
            .expect("the copy started");
        assert_eq!(api.calls(), ["vm.info", "vm.pause"]);
        snapshot.abort();

        assert!(
            eventually(Duration::from_secs(5), || api.calls().len() == 3).await,
            "{:?}",
            api.calls()
        );
        assert_eq!(api.calls(), ["vm.info", "vm.pause", "vm.resume"]);
        assert!(
            eventually(Duration::from_secs(5), || !clone.exists()).await,
            "the snapshot file nobody received is removed"
        );
    }

    /// Section 3.16: a copy that fails still resumes the VM, and leaves no
    /// snapshot file.
    #[tokio::test]
    async fn a_failed_copy_still_resumes_the_vm() {
        let vm = StandIn::new("snapfail");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", false);
        let (source, clone) = volume_files(&vm);

        let failed = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            |s, c| {
                plain_copy(s, c)?;
                Err(EngineError::Io("the disk failed".to_owned()))
            },
        )
        .await;
        assert!(matches!(failed, Err(EngineError::Io(_))), "{failed:?}");
        assert_eq!(api.calls(), ["vm.info", "vm.pause", "vm.resume"]);
        assert!(!clone.exists());
    }

    /// A pause whose answer is lost may still have paused the VM: the
    /// snapshot resumes it before it reports the failure.
    #[tokio::test]
    async fn a_failed_pause_that_took_effect_is_undone() {
        let vm = StandIn::new("snappause");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", true);
        let (source, clone) = volume_files(&vm);

        let failed = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            plain_copy,
        )
        .await;
        assert!(
            matches!(failed, Err(EngineError::Hypervisor(_))),
            "{failed:?}"
        );
        assert_eq!(api.calls(), ["vm.info", "vm.pause", "vm.info", "vm.resume"]);
        assert!(!clone.exists(), "nothing was copied");
    }

    /// A VM whose VMM stopped during the copy is not resumed, and its copy is
    /// not kept: the VM may have started again on the volume meanwhile.
    #[tokio::test]
    async fn a_vm_stopped_during_the_copy_is_not_resumed_and_its_copy_is_dropped() {
        let vm = StandIn::new("snapstop");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", false);
        let (source, clone) = volume_files(&vm);

        let entry = Arc::clone(&vm.entry);
        let failed = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            move |s, c| {
                // A stop of the paused VM ends its VMM during the copy.
                *lock(&entry.vmm) = None;
                plain_copy(s, c)
            },
        )
        .await;
        assert!(
            matches!(failed, Err(EngineError::Hypervisor(_))),
            "{failed:?}"
        );
        assert_eq!(api.calls(), ["vm.info", "vm.pause"]);
        assert!(!clone.exists());
    }

    /// A VM stopped and started again during the copy runs on a new VMM
    /// process, which the snapshot did not pause: it is not resumed, and the
    /// copy, which the new guest may have written to, is not kept.
    #[tokio::test]
    async fn a_vm_started_again_during_the_copy_is_not_resumed() {
        let vm = StandIn::new("snaprestart");
        let next = StandIn::new("snapnext");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Running", false);
        let (source, clone) = volume_files(&vm);

        let (entry, restarted) = (Arc::clone(&vm.entry), next.vmm.clone());
        let failed = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            move |s, c| {
                *lock(&entry.vmm) = Some(restarted);
                plain_copy(s, c)
            },
        )
        .await;
        assert!(
            matches!(failed, Err(EngineError::Hypervisor(_))),
            "{failed:?}"
        );
        assert_eq!(api.calls(), ["vm.info", "vm.pause"]);
        assert!(!clone.exists());
    }

    /// A VM another caller paused is copied as it stands: its guest makes no
    /// writes, and its freeze is not the snapshot's to end. A VM that never
    /// booted is copied without an API call.
    #[tokio::test]
    async fn a_paused_or_stopped_vm_is_copied_without_a_pause() {
        let vm = StandIn::new("snapidle");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = StatefulApi::serve(&vm.entry.paths.api_socket, "Paused", false);
        let (source, clone) = volume_files(&vm);
        let pause = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            plain_copy,
        )
        .await
        .expect("snapshot");
        assert_eq!(pause, Duration::ZERO);
        assert_eq!(api.calls(), ["vm.info"]);
        std::fs::remove_file(&clone).expect("snapshot");

        vm.entry.booted.store(false, Ordering::SeqCst);
        let pause = crate::ch::volume::pause_and_copy(
            &[Arc::clone(&vm.entry)],
            &vol_1(),
            &source,
            &clone,
            plain_copy,
        )
        .await
        .expect("snapshot");
        assert_eq!(pause, Duration::ZERO);
        assert_eq!(
            api.calls(),
            ["vm.info"],
            "no call for a VM that is not booted"
        );
        assert!(clone.exists());
    }

    /// ADR 0243: a forced stop that waits for the VM's lock while a snapshot
    /// holds it for its pause gets it at once. The snapshot gives up instead
    /// of waiting out a VMM that does not answer.
    #[tokio::test]
    async fn a_snapshot_pause_gives_way_to_a_forced_stop() {
        let vm = StandIn::new("snapforce");
        vm.entry.booted.store(true, Ordering::SeqCst);
        let api = silent_api(&vm.entry.paths.api_socket);
        let (source, clone) = volume_files(&vm);

        let snapshot = {
            let vms = vec![Arc::clone(&vm.entry)];
            let (source, clone) = (source.clone(), clone.clone());
            tokio::spawn(async move {
                crate::ch::volume::pause_and_copy(&vms, &vol_1(), &source, &clone, plain_copy).await
            })
        };
        assert!(
            eventually(Duration::from_secs(2), || vm.entry.op.try_lock().is_err()).await,
            "the snapshot holds the lock"
        );
        let began = Instant::now();
        let waiting = ForceWaiting::begin(&vm.entry);
        let op = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&vm.entry.op).lock_owned(),
        )
        .await
        .expect("the forced stop gets the lock at once");
        assert!(began.elapsed() < Duration::from_secs(1));
        drop((op, waiting));
        let failed = snapshot.await.expect("task");
        assert!(
            matches!(failed, Err(EngineError::Conflict(_))),
            "{failed:?}"
        );
        assert!(!clone.exists());
        api.abort();
    }
}
