// SPDX-License-Identifier: AGPL-3.0-only
//! The `Engine` boundary behind which a virtual machine monitor sits.
//!
//! `docs/contracts/engine-trait.md` is the binding contract for every type and
//! method in this crate. Section 2 is reproduced here unchanged. The additive
//! items added during M0 are part of the contract too (D-0071): derives that
//! make the types cloneable and comparable, the [`MacAddr`] newtype, `Display`
//! and `Error` for [`EngineError`], a blanket [`ConsoleIo`] implementation for
//! tokio's duplex and socket streams, and [`VmSpec::canonical`] for the
//! idempotency rule in section 3.6.
//!
//! [`FakeEngine`] is an in-memory implementation for tests; it lives in
//! [`fake`].

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncWrite};

/// vm_<ULID>, the same identity the control plane uses (README.md).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VmId(pub String);

/// vol_<ULID>; volumes exist as a shape from M5 (README.md).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VolumeId(pub String);

/// "sha256:<64 hex>".
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ImageDigest(pub String);

/// A MAC address, six bytes in the order the guest sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MacAddr(pub [u8; 6]);

/// An image as requested, and the digest it resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRef {
    pub reference: String,
    pub digest: ImageDigest,
}

/// A rootfs that is ready to boot. Cached by digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedImage {
    pub digest: ImageDigest,
    pub rootfs_path: PathBuf,
    pub size_bytes: u64,
    pub config: ImageConfig,
}

/// What the image says about how to run it, read from its OCI config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub user: Option<String>,
    pub workdir: Option<String>,
    pub exposed_ports: Vec<u16>,
}

/// Everything needed to create one microVM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmSpec {
    pub id: VmId,
    pub vcpus: u8,
    pub memory_mb: u32,
    /// Digest of a PreparedImage; the rootfs is mounted read-only.
    pub rootfs: ImageDigest,
    /// Size of the writable data disk.
    pub disk_gb: u32,
    pub volumes: Vec<VolumeAttach>,
    pub net: NetAttach,
    pub init: InitConfig,
    /// Where the app's tagged output goes (section 3.13). From M4 the node
    /// agent sets `VirtioConsole` for apps.
    pub console: ConsoleMode,
    /// The serial port (`ttyS0`): kernel and init diagnostics. From M4 the node
    /// agent sets `Socket`, and the engine is its only client (section 3.19,
    /// ADR 0116). Added in M4.
    pub serial: SerialMode,
    /// The virtio-vsock device that carries exec sessions (section 3.18,
    /// ADR 0113). `None` means no device. Added in M4.
    pub vsock: Option<VsockAttach>,
    /// Extra writable disks that outlive the VM, identified by serial. Only
    /// builder VMs use them (ADR 0118); app VMs carry none. Added in M4.
    pub work_disks: Vec<WorkDisk>,
}

impl VmSpec {
    /// The canonical form used to decide whether two specs are the same.
    ///
    /// Section 3.6 compares the same `id` by the canonical form of `VmSpec`,
    /// so `env` is sorted by `(name, value)` and `volumes` by
    /// `(volume_id, mount_path)`. Everything else keeps its order: it is
    /// either a scalar or an ordered list where order is meaningful.
    #[must_use]
    pub fn canonical(&self) -> VmSpec {
        let mut canonical = self.clone();
        canonical
            .init
            .env
            .sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        canonical.volumes.sort_by(|a, b| {
            a.volume_id
                .0
                .cmp(&b.volume_id.0)
                .then_with(|| a.mount_path.cmp(&b.mount_path))
        });
        canonical
    }
}

/// A volume attached to a VM at a mount path inside the guest. The volume
/// must exist on this node (`ensure_volume` or `import_volume`). Section 3.20.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeAttach {
    pub volume_id: VolumeId,
    pub mount_path: String,
    /// virtio-blk serial the guest init finds the disk by: `quoro-vol-<n>`,
    /// `n` from 0 in the order of `VmSpec::volumes`. Added in M5a.
    pub serial: String,
}

/// The package inventory of a prepared image (ADR 0150, ADR 0151). Added in
/// M5a. Section 3.21; the wire form is `node-agent-protocol.md` section 5.26.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageInventory {
    pub digest: ImageDigest,
    /// `ID` and `VERSION_ID` from the image's os-release, when it has one.
    pub os: Option<(String, String)>,
    /// OSV ecosystems covered, for example "Debian:13" or "npm".
    pub ecosystems: Vec<String>,
    /// Kinds of content seen and not read, for example "static_binary".
    pub not_inventoried: Vec<String>,
    pub packages: Vec<InventoryPackage>,
    /// RFC 3339 time the inventory was built.
    pub inventoried_at: String,
}

/// One package of an [`ImageInventory`]. Added in M5a.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InventoryPackage {
    pub ecosystem: String,
    pub name: String,
    /// The source package, when the package database records one.
    pub source_name: Option<String>,
    pub version: String,
    pub purl: String,
    /// For a language package, the file it was read from, relative to the root.
    pub path: Option<String>,
}

/// A volume file on this node. Added in M5a. Section 3.20.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeInfo {
    pub volume_id: VolumeId,
    pub size_gb: u32,
    /// Bytes the sparse file uses on the host's disk.
    pub allocated_bytes: u64,
    /// Node-local location of the file; not part of any API.
    pub path: PathBuf,
}

/// How `snapshot_volume` takes a snapshot on this node (ADR 0146). Added in
/// M5a. Section 3.16.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotMethod {
    /// `ioctl(FICLONE)` of the volume file; the VM keeps running.
    Reflink,
    /// Pause the VM, copy the sparse file, resume the VM.
    PauseCopy,
}

/// The VM's network attachment on the node bridge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetAttach {
    pub ip: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub mac: MacAddr,
    /// Isolation group of the VM. VMs on the same bridge that carry the same
    /// group reach each other; VMs in different groups, and a VM with `None`,
    /// reach no other VM. The node agent sets it to the VM's project id
    /// (`prj_`). Section 3.3.
    pub isolation_group: Option<String>,
    /// Egress only: the VM reaches the outside through the gateway and nothing
    /// on the node, neither another VM nor the host (section 3.3). Builder VMs
    /// set it, with `isolation_group: None` (ADR 0118). Added in M4.
    pub egress_only: bool,
}

/// A writable disk that the engine attaches but does not own: `delete` keeps
/// it, and the node agent removes it. Added in M4. Section 3.6.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkDisk {
    pub path: PathBuf,
    /// Size of the disk when the engine has to create it.
    pub size_gb: u32,
    /// virtio-blk serial the guest finds it by, for example `quoro-work`.
    pub serial: String,
}

/// An OCI image layout on the node, as a builder wrote it. Added in M4.
/// Section 3.17.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OciLayout {
    /// Directory or tarball of the layout (OCI image-layout 1.0).
    pub path: PathBuf,
    /// Digest of the image manifest the layout's index names.
    pub manifest_digest: ImageDigest,
}

/// The app's command line and identity, as the minimal init must apply it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub user: Option<String>,
    pub workdir: Option<String>,
}

/// How (and whether) a console device is attached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsoleMode {
    /// No console device.
    Off,
    /// virtio-console to this UNIX socket; carries tagged stdout/stderr lines.
    VirtioConsole { socket: PathBuf },
    /// Serial console to this UNIX socket; interactive text, one client at a time.
    /// Superseded in M4 by `VmSpec::serial` (section 3.14); a spec that sets it
    /// together with a `serial` other than `Off` is `Conflict`.
    Serial { socket: PathBuf },
}

/// How the serial port (`ttyS0`) is attached. Added in M4. Section 3.19.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SerialMode {
    /// No serial device.
    Off,
    /// Serial output appended to this file (the M1-M3 behaviour).
    File { path: PathBuf },
    /// Serial port on this UNIX socket; the engine connects to it as the only
    /// client from `create` on and keeps a node-local ring of its output.
    Socket { path: PathBuf },
}

/// A virtio-vsock device in Cloud Hypervisor's hybrid mode: the host end is a
/// UNIX socket in the VM's runtime directory. Added in M4. Section 3.18.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VsockAttach {
    /// Guest context id. Each VM has its own vsock namespace, so every VM uses 3.
    pub cid: u32,
    pub socket: PathBuf,
}

/// How a stop is requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopMode {
    /// Ask the guest to stop and wait at most this long.
    Graceful { timeout: Duration },
    /// Stop now, without cooperation from the guest.
    Force,
}

/// Mirrors the VM observed_status enum in README.md one to one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VmState {
    Creating,
    Stopped,
    Starting,
    Running,
    Stopping,
    Crashed,
    Deleted,
    Unknown,
}

/// One VM's observed state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmInfo {
    pub id: VmId,
    pub state: VmState,
    /// Present while a VMM process exists for this VM.
    pub pid: Option<u32>,
    pub started_at: Option<SystemTime>,
    pub exit_code: Option<i32>,
}

/// A lifecycle event for any VM on the node. Section 3.12.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineEvent {
    Started { id: VmId },
    Exited { id: VmId, code: i32 },
    Crashed { id: VmId, reason: String },
}

/// One tagged line of app output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputLine {
    pub stream: OutputStream,
    pub time: SystemTime,
    pub line: String,
}

/// Which of the app's two output streams a line came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Cumulative counters since VM start. Section 3.15.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmCounters {
    pub cpu_time_ms: u64,
    pub memory_rss_bytes: u64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
}

/// What this backend and build support. Section 3.1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capabilities {
    pub engine: String,  // "cloud-hypervisor"
    pub version: String, // the pinned release
    pub kvm: bool,
    pub text_console: bool,      // true
    pub graphical_console: bool, // false
    pub vm_snapshot: bool,
    pub volume_snapshot: bool,
    /// Whether `exec` works on this backend and build (section 3.18). Added in M4.
    pub exec: bool,
    /// The method `snapshot_volume` uses on this node, probed once at start-up
    /// (section 3.16); `None` when the engine has no volumes. Added in M5a.
    pub volume_snapshot_method: Option<SnapshotMethod>,
}

/// The node network a VM is attached to. Section 3.3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeNetwork {
    pub bridge: String,
    pub subnet: String, // CIDR, for example "10.42.1.0/24"
    pub gateway: Ipv4Addr,
    /// DNS servers handed to the guests created on this network, in order.
    /// Taken from the node's configuration; empty means guests get no
    /// resolver. Not part of the network's identity. Section 3.3.
    pub dns: Vec<Ipv4Addr>,
}

/// Which snapshot files an engine's start-up sweep keeps (section 3.16).
///
/// Snapshots left by a crash are removed at start. From M5b the node agent
/// keeps the snapshots its store records (a warm base, the snapshot being
/// shipped, a returning source's candidates, a quarantined copy) and hands
/// their file names to the engine before the engine starts, so the sweep
/// removes only the rest. It is safe because the node agent records a
/// snapshot before anything relies on it: a snapshot with no record never
/// holds data the live volume does not hold too.
///
/// Every doubt keeps more, so the default keeps every snapshot: an engine
/// built without the node agent's rule never removes one it cannot tell from
/// a recorded copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SnapshotSweep {
    /// Keep the snapshots whose file names are in the set and remove the
    /// others. A file name, not a `snapshot_id`: it is what the sweep sees
    /// in the volume directory.
    Keep(BTreeSet<String>),
    /// Keep every snapshot: the node agent does not know which ones it
    /// records, and removing a recorded one could lose data.
    #[default]
    KeepAll,
}

impl SnapshotSweep {
    /// Whether the sweep keeps the snapshot file with this name.
    #[must_use]
    pub fn keeps(&self, file_name: &str) -> bool {
        match self {
            Self::Keep(names) => names.contains(file_name),
            Self::KeepAll => true,
        }
    }
}

/// A point-in-time volume snapshot descriptor. Section 3.16.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeSnapshot {
    pub volume_id: VolumeId,
    pub snapshot_id: String,
    pub created_at: SystemTime,
    pub size_bytes: u64,
    /// Node-local location of the snapshot; not part of any API.
    pub location: PathBuf,
    /// How it was taken. Added in M5a.
    pub method: SnapshotMethod,
    /// How long the VM using the volume was paused; zero for `Reflink` or
    /// when no VM was running. Added in M5a.
    pub pause: Duration,
}

/// A byte range of a volume file. Added in M5b. Section 3.22.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

/// The newer side of a `volume_changed_ranges` comparison. Added in M5b.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangesOf {
    /// A snapshot of the volume.
    Snapshot(VolumeSnapshot),
    /// The volume file itself, which no VM that exists may attach.
    Volume(VolumeId),
}

/// The ranges that may differ from a base. Added in M5b. Section 3.22.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedRanges {
    /// Sorted by offset, not overlapping, aligned to 4 KiB. A range may be a
    /// hole in the newer file; it then reads as zeros.
    pub ranges: Vec<ByteRange>,
    /// True when `ranges` is every allocated range of the newer file: there
    /// was no base, or the extent maps could not be compared.
    pub full: bool,
    /// Extents in the newer file's extent map.
    pub extent_count: u64,
}

/// What a staging file starts from. Added in M5b. Section 3.22.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageFrom {
    /// An empty sparse file of the volume's size.
    Empty,
    /// A reflink clone of the volume file.
    Volume,
    /// A reflink clone of a snapshot of the volume.
    Snapshot(VolumeSnapshot),
}

/// A staging file a warm copy is written into. Added in M5b. Section 3.22.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedVolume {
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    /// Node-local location the node agent writes; not part of any API.
    pub location: PathBuf,
}

/// Every method is total: it either succeeds or returns one of these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    NotFound,
    Conflict(String),
    Unsupported(String),
    ResourceExhausted(String),
    Image(String),
    Hypervisor(String),
    Io(String),
    Timeout,
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::Conflict(reason) => write!(f, "conflict: {reason}"),
            Self::Unsupported(reason) => write!(f, "unsupported: {reason}"),
            Self::ResourceExhausted(reason) => write!(f, "resource exhausted: {reason}"),
            Self::Image(reason) => write!(f, "image error: {reason}"),
            Self::Hypervisor(reason) => write!(f, "hypervisor error: {reason}"),
            Self::Io(reason) => write!(f, "io error: {reason}"),
            Self::Timeout => f.write_str("timed out"),
        }
    }
}

impl std::error::Error for EngineError {}

/// A stream of every lifecycle event on the node.
pub type EventStream = Pin<Box<dyn Stream<Item = EngineEvent> + Send>>;

/// A stream of one VM's tagged output lines.
pub type LineStream = Pin<Box<dyn Stream<Item = OutputLine> + Send>>;

/// Bidirectional byte stream on a text console (serial or virtio-console).
pub trait ConsoleIo: AsyncRead + AsyncWrite + Send + Unpin {}

/// Any async byte stream is a console stream; tokio's duplex and UNIX socket
/// streams satisfy this without an adapter.
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> ConsoleIo for T {}

/// One end of a text console connection.
pub type ConsoleStream = Pin<Box<dyn ConsoleIo>>;

/// Raw bytes in arrival order, for example a VM's serial output. Added in M4.
pub type ByteStream = Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;

/// A program to run in a running VM, on a PTY. Added in M4. Section 3.18.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecRequest {
    /// The argv; `argv[0]` without `/` is searched in the app's `PATH`.
    pub argv: Vec<String>,
    /// A user name or uid, optionally `:` and a group name or gid; `None` is the app's own user.
    pub user: Option<String>,
    pub rows: u16,
    pub cols: u16,
}

/// One exec session. Added in M4. Section 3.18.
pub struct ExecSession {
    /// The PTY: reads are the program's output, writes are its input. Dropping
    /// or shutting it down ends the session (`SIGHUP` to its process group).
    pub io: ConsoleStream,
    /// Resize requests as `(rows, cols)`.
    pub resize: tokio::sync::mpsc::Sender<(u16, u16)>,
    /// Resolves once with the program's exit code, `128 + signal` when a
    /// signal ended it; an error when the session ended without one.
    pub exit: tokio::sync::oneshot::Receiver<i32>,
}

/// The boundary behind which a virtual machine monitor sits.
///
/// The async methods return `impl Future<Output = _> + Send`, so a caller
/// generic over `E: Engine` can spawn an engine call on the tokio runtime
/// (D-0071). A backend still implements them with `async fn`. The trait is not
/// object-safe, which is why the node agent selects a backend through an enum;
/// a `dyn` need would be met by boxed futures with otherwise identical
/// signatures.
pub trait Engine: Send + Sync {
    // Host and node preparation
    fn capabilities(&self) -> impl Future<Output = Capabilities> + Send;
    fn probe_host(&self) -> impl Future<Output = Result<HostProbe, EngineError>> + Send;
    fn ensure_network(
        &self,
        net: &NodeNetwork,
    ) -> impl Future<Output = Result<(), EngineError>> + Send;

    // Images
    fn prepare_image(
        &self,
        image: &ImageRef,
    ) -> impl Future<Output = Result<PreparedImage, EngineError>> + Send;
    fn remove_image(
        &self,
        digest: &ImageDigest,
    ) -> impl Future<Output = Result<(), EngineError>> + Send;
    /// Added in M4; section 3.17.
    fn prepare_layout(
        &self,
        _layout: &OciLayout,
    ) -> impl Future<Output = Result<PreparedImage, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "local OCI layouts are not implemented by this engine".into(),
            ))
        }
    }

    // Lifecycle
    fn create(&self, spec: &VmSpec) -> impl Future<Output = Result<VmInfo, EngineError>> + Send;
    fn start(&self, id: &VmId) -> impl Future<Output = Result<VmInfo, EngineError>> + Send;
    fn stop(
        &self,
        id: &VmId,
        mode: StopMode,
    ) -> impl Future<Output = Result<VmInfo, EngineError>> + Send;
    fn delete(&self, id: &VmId) -> impl Future<Output = Result<(), EngineError>> + Send;
    fn inspect(&self, id: &VmId) -> impl Future<Output = Result<VmInfo, EngineError>> + Send;
    fn list(&self) -> impl Future<Output = Result<Vec<VmInfo>, EngineError>> + Send;

    // Observation
    fn events(&self) -> EventStream;
    fn output(&self, id: &VmId) -> Result<LineStream, EngineError>;
    fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError>;
    fn counters(&self, id: &VmId) -> impl Future<Output = Result<VmCounters, EngineError>> + Send;

    // Interactive access (M4; default implementations return Unsupported)
    /// Section 3.18.
    fn exec(
        &self,
        _id: &VmId,
        _request: &ExecRequest,
    ) -> impl Future<Output = Result<ExecSession, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "exec is not implemented by this engine".into(),
            ))
        }
    }
    /// Section 3.19.
    fn serial_output(
        &self,
        _id: &VmId,
        _tail_bytes: Option<u64>,
    ) -> Result<ByteStream, EngineError> {
        Err(EngineError::Unsupported(
            "serial output is not implemented by this engine".into(),
        ))
    }

    // Volumes (M5; default implementations return Unsupported)
    /// Added in M5a; section 3.20.
    fn ensure_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
    ) -> impl Future<Output = Result<VolumeInfo, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5a; section 3.20.
    fn import_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
        _from: &Path,
    ) -> impl Future<Output = Result<VolumeInfo, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5a; section 3.20.
    fn delete_volume(
        &self,
        _id: &VolumeId,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "volumes are not implemented by this engine".into(),
            ))
        }
    }
    // Image inventories (M5a; the default implementation returns Unsupported)
    /// Added in M5a; section 3.21.
    fn image_inventory(
        &self,
        _digest: &ImageDigest,
    ) -> impl Future<Output = Result<ImageInventory, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "image inventories are not implemented by this engine".into(),
            ))
        }
    }
    /// Cached images that have no inventory yet: images prepared before M5a.
    /// Added in M5a; section 3.21. The default lists none, so a backend
    /// without inventories never starts a backfill.
    fn uninventoried_images(
        &self,
    ) -> impl Future<Output = Result<Vec<ImageDigest>, EngineError>> + Send {
        async { Ok(Vec::new()) }
    }
    /// Inventories a cached image from its root disk, read-only, and caches
    /// the result with the image. An image that already has an inventory
    /// returns it as it is. Added in M5a; section 3.21.
    fn inventory_cached_image(
        &self,
        _digest: &ImageDigest,
    ) -> impl Future<Output = Result<ImageInventory, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "image inventories are not implemented by this engine".into(),
            ))
        }
    }
    /// Section 3.16.
    fn snapshot_volume(
        &self,
        _id: &VolumeId,
    ) -> impl Future<Output = Result<VolumeSnapshot, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "volume snapshots are not implemented by this engine".into(),
            ))
        }
    }
    fn delete_volume_snapshot(
        &self,
        _snap: &VolumeSnapshot,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "volume snapshots are not implemented by this engine".into(),
            ))
        }
    }

    // Warm volumes (M5b; the default implementation returns Unsupported)
    /// Added in M5b; section 3.22.
    fn volume_changed_ranges(
        &self,
        _newer: &RangesOf,
        _base: Option<&VolumeSnapshot>,
    ) -> impl Future<Output = Result<ChangedRanges, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "warm volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5b; section 3.22.
    fn stage_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
        _from: &StageFrom,
    ) -> impl Future<Output = Result<StagedVolume, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "warm volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5b; section 3.22.
    fn commit_staged_volume(
        &self,
        _staged: &StagedVolume,
    ) -> impl Future<Output = Result<VolumeInfo, EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "warm volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5b; section 3.22.
    fn discard_staged_volume(
        &self,
        _staged: &StagedVolume,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "warm volumes are not implemented by this engine".into(),
            ))
        }
    }
    /// Freezes a running VM's vCPUs. Added in M5b; section 3.22.
    fn pause(&self, _id: &VmId) -> impl Future<Output = Result<(), EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "pausing a VM is not implemented by this engine".into(),
            ))
        }
    }
    /// Added in M5b; section 3.22.
    fn resume(&self, _id: &VmId) -> impl Future<Output = Result<(), EngineError>> + Send {
        async {
            Err(EngineError::Unsupported(
                "pausing a VM is not implemented by this engine".into(),
            ))
        }
    }
}

/// The result of the host check in section 3.2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostProbe {
    pub kvm_available: bool,
    pub kernel_release: String,
    /// True when the kernel is recent enough for the backend (6.8 or later for
    /// Cloud Hypervisor v54, section 3.2).
    pub kernel_supported: bool,
    pub cpu_vmx: bool,
    pub cpu_svm: bool,
    pub nested: bool,
}

pub mod fake;

/// The extent-map comparison behind `volume_changed_ranges` (section 3.22,
/// ADR 0172). Platform-independent, so its property tests run anywhere; the
/// Cloud Hypervisor backend feeds it the maps it reads with FIEMAP.
pub mod extents;

/// The package inventory of an unpacked image tree (section 3.21, ADR 0151).
/// The parsers are platform-independent so their tests run anywhere; the
/// Linux image path supplies a tree resolved inside the staging root.
pub mod inventory;

/// The packet mark the node agent sets on mesh traffic it admits: TCP from
/// the edge's mesh address to one backend's guest address and port
/// (`node-agent-protocol.md` section 6.9, ADR 0144). The engine's `forward`
/// chain lets a marked packet onto the bridge; everything else arriving from
/// outside the bridge stays dropped unless it answers a guest's own
/// connection. The node agent owns the mesh interface and the rules that set
/// the mark; the engine only honours it (`engine-trait.md` section 3.3).
pub const MESH_MARK: u32 = 0x5155_008c;

/// Pinned Cloud Hypervisor and guest kernel artifacts, verified by sha256.
#[cfg(target_os = "linux")]
pub mod pins;

/// Boot artifacts the engine builds for every VM: the initramfs that carries
/// `quoro-init`, the config disk and the writable data disk.
#[cfg(target_os = "linux")]
pub mod boot;

/// The OCI-to-ext4 pipeline behind `prepare_image` (D-0061).
#[cfg(target_os = "linux")]
pub mod image;

/// The Cloud Hypervisor backend: [`ch::ChEngine`] implements [`Engine`].
#[cfg(target_os = "linux")]
pub mod ch;

pub use fake::{FakeEngine, FakeOp, FakeRanges};
