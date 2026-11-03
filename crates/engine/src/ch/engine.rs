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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};

use crate::ch::api::ApiClient;
use crate::ch::net::{self, Subnet};
use crate::ch::state::{
    self, ExitRecord, ExitState, SpecRecord, VmPaths, VmmRecord, ms_to_system_time, now_ms,
};
use crate::ch::tools::HostTools;
use crate::ch::vmconfig::{self, BootFiles};
use crate::ch::vmm::{Vmm, VmmLaunch};
use crate::ch::{FORCE_STOP_BOUND, host, stream, supervise};
use crate::image::{ImageStore, ImageStoreConfig};
use crate::pins;
use crate::{
    Capabilities, ConsoleMode, ConsoleStream, EngineError, EngineEvent, EventStream, HostProbe,
    ImageDigest, ImageRef, LineStream, NodeNetwork, PreparedImage, StopMode, VmCounters, VmId,
    VmInfo, VmSpec, VmState,
};

use super::ChConfig;

/// Capacity of each event subscriber's buffer (§3.12).
const EVENT_CAPACITY: usize = 1024;
/// Timeout of the `vm.info` call `inspect` makes.
const INFO_TIMEOUT: Duration = Duration::from_secs(1);
/// Timeout of the `vm.counters` call `counters` makes.
const COUNTERS_TIMEOUT: Duration = Duration::from_secs(1);
/// Timeout of the `vmm.shutdown` call a forced stop makes.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a forced stop waits for the exit after `vmm.shutdown`.
const SETTLE_AFTER_SHUTDOWN: Duration = Duration::from_secs(3);
/// How long any stop waits for the exit after SIGKILL.
const SETTLE_AFTER_SIGKILL: Duration = Duration::from_secs(5);
/// How long `delete` waits for the exit before it gives up.
const SETTLE_AFTER_DELETE_SHUTDOWN: Duration = Duration::from_secs(5);
// The pieces of a forced stop add up to its documented bound.
const _: () = assert!(
    SHUTDOWN_TIMEOUT.as_secs() + SETTLE_AFTER_SHUTDOWN.as_secs() + SETTLE_AFTER_SIGKILL.as_secs()
        == FORCE_STOP_BOUND.as_secs()
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
    /// Serializes create, start, stop and delete on this id.
    pub(super) op: Mutex<()>,
    /// The live VMM handle, while the process exists.
    pub(super) vmm: StdMutex<Option<Vmm>>,
    /// `/vm.boot` succeeded for the current VMM process.
    pub(super) booted: AtomicBool,
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
}

impl VmEntry {
    fn new(id: VmId, paths: VmPaths, spec: Option<SpecRecord>) -> Self {
        let (settled, _) = watch::channel(0_u64);
        Self {
            id,
            paths,
            op: Mutex::new(()),
            vmm: StdMutex::new(None),
            booted: AtomicBool::new(false),
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

    fn set_phase(&self, phase: Phase) {
        *lock(&self.phase) = phase;
    }

    /// The spec this VM was created with, if it has been read.
    fn spec(&self) -> Option<SpecRecord> {
        lock(&self.spec).clone()
    }

    /// The live VMM, when the process still is the one it was started as.
    fn live_vmm(&self) -> Option<Vmm> {
        let slot = lock(&self.vmm);
        let vmm = slot.as_ref()?;
        if vmm.id().is_alive() {
            Some(vmm.clone())
        } else {
            None
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
    pub(super) async fn new(config: ChConfig) -> Result<Arc<Inner>, EngineError> {
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
        });
        inner.rediscover(&vms_root).await?;
        Ok(inner)
    }
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
    async fn rediscover(self: &Arc<Self>, vms_root: &Path) -> Result<(), EngineError> {
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
            let entry = Arc::new(VmEntry::new(id.clone(), paths.clone(), Some(record)));
            match state::read_json::<VmmRecord>(&paths.vmm)? {
                Some(vmm_record) => match Vmm::adopt(vmm_record.process)? {
                    Some(vmm) => {
                        entry.booted.store(vmm_record.booted, Ordering::SeqCst);
                        *lock(&entry.vmm) = Some(vmm.clone());
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
        Capabilities {
            engine: "cloud-hypervisor".to_string(),
            version: self.ch_version.clone(),
            kvm: host::kvm_available(),
            text_console: true,
            graphical_console: false,
            vm_snapshot: false,
            volume_snapshot: false,
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
        persist_networks(&self.config.state_dir.join(NETWORKS_FILE), &networks)
    }

    /// Pulls and converts one image (§3.4).
    pub(super) async fn prepare_image(
        &self,
        image: &ImageRef,
    ) -> Result<PreparedImage, EngineError> {
        self.images.prepare(image).await
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

        let (network, subnet) = self.reserve(spec).await?;
        let prepared = self.images.cached(&spec.rootfs)?.ok_or_else(|| {
            EngineError::Image(format!("rootfs {} is not a prepared image", spec.rootfs.0))
        })?;
        let config = vmconfig::guest_config(
            spec,
            &subnet,
            &self.config.dns,
            self.config.guest_stop_timeout,
        )?;
        let tap = net::tap_name(&spec.id);
        let record = SpecRecord::new(spec, &tap, &network.bridge);

        entry.set_phase(Phase::Starting);
        let tmp_dir = self
            .vms_root()
            .join(format!(".tmp-{}-{}", spec.id.0, std::process::id()));
        let tmp = rebased(&paths, &tmp_dir);
        let built = self.build_files(&tmp, &record, &config, spec.disk_gb).await;
        if let Err(err) = built {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        if let Err(err) = tokio::fs::rename(&tmp_dir, &paths.dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            entry.set_phase(Phase::Idle);
            return Err(io_error(&paths.dir, &err));
        }

        if let Err(err) = net::create_tap(&self.tools, &tap, &network.bridge).await {
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
        let body = vmconfig::vm_create_body(spec, &paths, &boot_files, &tap);
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
        supervise::start(Arc::clone(self), Arc::clone(&entry), vmm.clone());
        Ok(VmInfo {
            id: spec.id.clone(),
            state: VmState::Stopped,
            pid: Some(vmm.id().pid),
            started_at: None,
            exit_code: None,
        })
    }

    /// Boots a created or stopped VM (§3.7).
    pub(super) async fn start(self: &Arc<Self>, id: &VmId) -> Result<VmInfo, EngineError> {
        let Some(entry) = self.entry(id) else {
            return Err(EngineError::NotFound);
        };
        let _op = entry.op.lock().await;
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

        entry.set_phase(Phase::Starting);
        if let Err(err) = net::create_tap(&self.tools, &record.tap, &record.bridge).await {
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        state::remove_marker(&entry.paths.stop_requested)?;
        let _ = std::fs::remove_file(&entry.paths.last_exit);
        if let Err(err) = crate::boot::reset_status(&entry.paths.config_disk) {
            entry.set_phase(Phase::Idle);
            return Err(err);
        }

        let vmm = match live {
            Some(vmm) => vmm,
            None => match self.boot_vmm(&entry, &spec, &prepared, &record).await {
                Ok(vmm) => vmm,
                Err(err) => {
                    entry.set_phase(Phase::Idle);
                    return Err(err);
                }
            },
        };
        if let Err(err) = ApiClient::new(&entry.paths.api_socket).boot().await {
            let _ = vmm.sigkill();
            let _ = wait_settled(&entry, SETTLE_AFTER_SIGKILL).await;
            entry.set_phase(Phase::Idle);
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
            entry.set_phase(Phase::Idle);
            return Err(err);
        }
        entry.booted.store(true, Ordering::SeqCst);
        entry.set_phase(Phase::Idle);
        self.emit(EngineEvent::Started { id: id.clone() });
        Ok(VmInfo {
            id: id.clone(),
            state: VmState::Running,
            pid: Some(vmm.id().pid),
            started_at: Some(ms_to_system_time(started_at_ms)),
            exit_code: None,
        })
    }

    /// Stops a VM, gracefully or by force (§3.8, D-0074).
    pub(super) async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError> {
        let Some(entry) = self.entry(id) else {
            return Err(EngineError::NotFound);
        };
        let _op = entry.op.lock().await;
        if !entry.paths.dir.exists() {
            self.remove_entry(id);
            return Err(EngineError::NotFound);
        }
        let Some(vmm) = entry.live_vmm() else {
            self.settle_crashed_exit(&entry);
            return self.inspect(id).await;
        };

        entry.set_phase(Phase::Stopping);
        state::touch_marker(&entry.paths.stop_requested)?;
        let api = ApiClient::new(&entry.paths.api_socket);
        if entry.booted.load(Ordering::SeqCst)
            && let StopMode::Graceful { timeout } = mode
        {
            let asked = api.power_button().await.is_ok();
            if asked && wait_settled(&entry, timeout).await {
                entry.set_phase(Phase::Idle);
                return self.inspect(id).await;
            }
        }

        let _ = api.with_timeout(SHUTDOWN_TIMEOUT).shutdown_vmm().await;
        if !wait_settled(&entry, SETTLE_AFTER_SHUTDOWN).await {
            let _ = vmm.sigkill();
            if !wait_settled(&entry, SETTLE_AFTER_SIGKILL).await {
                entry.set_phase(Phase::Idle);
                return Err(EngineError::Hypervisor(
                    "VMM process did not exit after SIGKILL".to_string(),
                ));
            }
        }
        entry.set_phase(Phase::Idle);
        self.inspect(id).await
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
            state::touch_marker(&entry.paths.stop_requested)?;
            let _ = ApiClient::new(&entry.paths.api_socket)
                .with_timeout(SHUTDOWN_TIMEOUT)
                .shutdown_vmm()
                .await;
            if !wait_settled(&entry, SETTLE_AFTER_DELETE_SHUTDOWN).await {
                let _ = vmm.sigkill();
                if !wait_settled(&entry, SETTLE_AFTER_SIGKILL).await {
                    return Err(EngineError::Hypervisor(
                        "VMM process did not exit after SIGKILL".to_string(),
                    ));
                }
            }
        }

        if let Some(record) = state::read_json::<SpecRecord>(&entry.paths.spec)? {
            net::delete_tap(&self.tools, &record.tap).await?;
            if let Ok(spec) = record.to_spec() {
                for socket in console_sockets(&spec) {
                    let _ = std::fs::remove_file(socket);
                }
            }
        }
        tokio::fs::remove_dir_all(&entry.paths.dir)
            .await
            .map_err(|err| io_error(&entry.paths.dir, &err))?;
        self.remove_entry(id);
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
    /// for a virtio-console, and `Conflict` when the VM is not running or
    /// another client already holds the console.
    pub(super) fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError> {
        let (entry, record) = self.entry_with_spec(id)?;
        let spec = record.to_spec()?;
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
    fn entry_with_spec(&self, id: &VmId) -> Result<(Arc<VmEntry>, SpecRecord), EngineError> {
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
    fn entries(&self) -> Vec<(VmId, Arc<VmEntry>)> {
        lock(&self.vms)
            .iter()
            .map(|(id, entry)| (id.clone(), Arc::clone(entry)))
            .collect()
    }

    /// The checks and the reservation `create` makes before it writes anything
    /// (§3.6).
    ///
    /// Returns the ensured network the address lives in; resources are
    /// reserved here, not at `start`.
    async fn reserve(&self, spec: &VmSpec) -> Result<(NodeNetwork, Subnet), EngineError> {
        if !spec.volumes.is_empty() {
            return Err(EngineError::Unsupported(
                "volumes ship in M5a (D-0052)".to_string(),
            ));
        }
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

        if let Some(socket) = console_socket(&spec.console) {
            if !socket.is_absolute() {
                return Err(EngineError::Io(format!(
                    "console socket {} is not an absolute path",
                    socket.display()
                )));
            }
            let parent_exists = socket.parent().is_some_and(Path::exists);
            if !parent_exists {
                return Err(EngineError::Io(format!(
                    "console socket directory {} does not exist",
                    socket
                        .parent()
                        .map(|parent| parent.display().to_string())
                        .unwrap_or_default()
                )));
            }
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
    ) -> Result<Vmm, EngineError> {
        let launch = VmmLaunch {
            binary: &self.ch_binary,
            api_socket: &entry.paths.api_socket,
            event_log: &entry.paths.event_log,
            log_file: &entry.paths.vmm_log,
        };
        let vmm = Vmm::spawn(&launch).await?;
        let boot_files = BootFiles {
            kernel: &self.kernel,
            initramfs: &self.initramfs,
            rootfs: &prepared.rootfs_path,
        };
        let body = vmconfig::vm_create_body(spec, &entry.paths, &boot_files, &record.tap);
        if let Err(err) = ApiClient::new(&entry.paths.api_socket).create(&body).await {
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

/// The console socket a spec names, if it names one.
fn console_socket(mode: &ConsoleMode) -> Option<&Path> {
    match mode {
        ConsoleMode::Off => None,
        ConsoleMode::VirtioConsole { socket } | ConsoleMode::Serial { socket } => Some(socket),
    }
}

/// Every console socket path a spec names, for `delete`.
fn console_sockets(spec: &VmSpec) -> Vec<PathBuf> {
    console_socket(&spec.console)
        .map(|socket| vec![socket.to_path_buf()])
        .unwrap_or_default()
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

/// Locks a `std` mutex, taking the value even when an earlier holder panicked:
/// a poisoned map of VMs is still a map of VMs.
fn lock<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
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
fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
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
