# Engine trait

Status: M0 draft for review. Contracts freeze at the end of M0 (D-0036); after the freeze a change to this file is a standalone contract pull request, never part of feature work.

This file fixes the boundary behind which a virtual machine monitor sits. Cloud Hypervisor with KVM is the first implementation (D-0007), and the boundary exists so that other backends — Firecracker, a container runtime — can be added later without touching the rest of the system (D-0007, D-0009). The MVP requires a host with KVM; a container fallback is a later implementation of the same trait, not a mode of the first one (D-0009).

The node agent is the only caller. Nothing else in Quoro — not the API, the panel, MCP or the CLI — talks to the engine directly; they change desired state, and the node agent turns that into engine calls (`node-agent-protocol.md`). The engine therefore has no authentication, no tenancy and no policy of its own: it does what the node agent asks on the node it runs on, and the node agent is the component that checks leases, tokens and desired state first.

This file is a contract about behaviour, not about file layout. Names of modules, crates and private helpers may change; the types, method names, semantics and error variants below may not, except by a standalone contract pull request.

## 1. Shape of the boundary

| Property | Choice | Why |
| --- | --- | --- |
| Language | Rust, async, `tokio` runtime | The node agent is Rust and async; the engine runs in its process |
| Dispatch | One trait, implemented per backend, selected by an enum at start-up | The trait is not object-safe, so there is no `dyn`; boxed futures would meet one |
| Ownership | One engine instance per node, shared by the node agent as `Arc` | Serialization of per-VM operations happens above the engine |
| Process model | One VMM process per VM, outside the agent process | A VMM survives an agent restart, so an upgrade does not restart apps (D-0011) |
| State | The engine owns node-local files; the cluster state is the node agent's | Desired state is signed and stored by the agent, not by the engine (D-0011) |
| Telemetry | The engine exposes streams and counters; it stores no logs | Logs and metrics stay on the node (D-0014) |

The async methods are declared as `fn ... -> impl Future<Output = ...> + Send`, so a caller generic over `E: Engine` can `tokio::spawn` an engine call (D-0071); backends still implement them with `async fn`. The trait is not object-safe, which is why the node agent selects a backend through an enum (`enum Backend { CloudHypervisor(ChEngine), Fake(FakeEngine) }`) rather than `Box<dyn Engine>`; a need for `dyn` would be met by boxed futures with otherwise identical signatures.

## 2. The trait

```rust
// Rust 2024 edition (D-0071).
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
```

```rust
/// An image as requested, and the digest it resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRef {
    pub reference: String,
    pub digest: ImageDigest,
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
```

```rust
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
    /// agent sets `VirtioConsole` for apps and builder VMs alike.
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

/// How `snapshot_volume` takes a snapshot on this node (ADR 0146, ADR 0156).
/// Added in M5a. Section 3.16.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotMethod {
    /// `ioctl(FICLONE)` of the volume file, on the data directory's own
    /// filesystem or inside the node's XFS volume pool; the VM keeps running.
    Reflink,
    /// Pause the VM, copy the sparse file, resume the VM.
    PauseCopy,
}

/// What came of asking the guest to flush a volume's filesystem before a
/// snapshot (ADR 0282). Added in M5b. Sections 3.16 and 3.22.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuestFlush {
    /// The init answered that it ran `syncfs` on the volume's mount.
    Flushed,
    /// No VM that attaches the volume was running when the snapshot began, so
    /// no guest held unwritten data of it.
    NoRunningVm,
    /// The VM that attaches the volume is paused and cannot answer, so it was
    /// not asked.
    Paused,
    /// The VM has no vsock device, its init predates the sync session, or the
    /// backend does not flush.
    Unsupported,
    /// The flush had no result within the bound.
    TimedOut,
    /// Any other result: an error or an unexpected answer, a close without an
    /// answer, a failed connection, or a VMM that cannot report whether the VM
    /// is paused.
    Failed,
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
```

```rust
/// How a stop is requested. Every stop is bounded by its caller (section 3.8,
/// ADR 0243).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopMode {
    /// Ask the guest to stop and wait at most `timeout`, then stop it as
    /// `Force { within: force_within }`.
    Graceful { timeout: Duration, force_within: Duration },
    /// Stop now, without cooperation from the guest, within `within`.
    Force { within: Duration },
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
    /// The Cloud Hypervisor release of the VM's VMM, in `pins.txt`'s
    /// `version` form (`v54.0`), as the engine recorded it when it started
    /// that VMM; `None` when no VMM runs or its record names no release.
    /// A VMM outlives the agent, so it can differ from the pin.
    /// Added in M5b; sections 3.22 and 5.
    pub vmm_version: Option<String>,
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
    /// Every Cloud Hypervisor release whose VMM order test passed, earlier
    /// pins included, since a VMM outlives an agent upgrade; in `pins.txt`'s
    /// `version` form. Compiled into the build apart from `pins.txt`. Added
    /// in M5b; sections 3.22 and 5.
    pub vmm_order_passed: Vec<String>,
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
    /// Whether the guest flushed the volume's filesystem before the snapshot
    /// was taken (section 3.16, ADR 0282). Added in M5b.
    pub guest_flush: GuestFlush,
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
        // one short lowercase phrase per variant
        ...
    }
}

impl std::error::Error for EngineError {}
```

```rust
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
    fn start(
        &self,
        id: &VmId,
        within: Duration,
    ) -> impl Future<Output = Result<VmInfo, EngineError>> + Send;
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
        async { Err(EngineError::Unsupported("exec is not implemented by this engine".into())) }
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
        async { Err(EngineError::Unsupported("volumes are not implemented by this engine".into())) }
    }
    /// Added in M5a; section 3.20.
    fn import_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
        _from: &Path,
    ) -> impl Future<Output = Result<VolumeInfo, EngineError>> + Send {
        async { Err(EngineError::Unsupported("volumes are not implemented by this engine".into())) }
    }
    /// Added in M5a; section 3.20.
    fn delete_volume(
        &self,
        _id: &VolumeId,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async { Err(EngineError::Unsupported("volumes are not implemented by this engine".into())) }
    }

    // Image inventories (M5a; the default implementation returns Unsupported)
    /// Added in M5a; section 3.21.
    fn image_inventory(
        &self,
        _digest: &ImageDigest,
    ) -> impl Future<Output = Result<ImageInventory, EngineError>> + Send {
        async { Err(EngineError::Unsupported("image inventories are not implemented by this engine".into())) }
    }
    /// Cached images that have no inventory yet. Added in M5a; section 3.21.
    fn uninventoried_images(
        &self,
    ) -> impl Future<Output = Result<Vec<ImageDigest>, EngineError>> + Send {
        async { Ok(Vec::new()) }
    }
    /// Inventories a cached image from its root disk. Added in M5a; section 3.21.
    fn inventory_cached_image(
        &self,
        _digest: &ImageDigest,
    ) -> impl Future<Output = Result<ImageInventory, EngineError>> + Send {
        async { Err(EngineError::Unsupported("image inventories are not implemented by this engine".into())) }
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
        async { Err(EngineError::Unsupported("warm volumes are not implemented by this engine".into())) }
    }
    /// Added in M5b; section 3.22.
    fn stage_volume(
        &self,
        _id: &VolumeId,
        _size_gb: u32,
        _from: &StageFrom,
    ) -> impl Future<Output = Result<StagedVolume, EngineError>> + Send {
        async { Err(EngineError::Unsupported("warm volumes are not implemented by this engine".into())) }
    }
    /// Added in M5b; section 3.22.
    fn commit_staged_volume(
        &self,
        _staged: &StagedVolume,
    ) -> impl Future<Output = Result<VolumeInfo, EngineError>> + Send {
        async { Err(EngineError::Unsupported("warm volumes are not implemented by this engine".into())) }
    }
    /// Added in M5b; section 3.22.
    fn discard_staged_volume(
        &self,
        _staged: &StagedVolume,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async { Err(EngineError::Unsupported("warm volumes are not implemented by this engine".into())) }
    }
    /// Freezes a running VM's vCPUs, waiting at most `within`. Added in
    /// M5b; section 3.22.
    fn pause(
        &self,
        _id: &VmId,
        _within: Duration,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async { Err(EngineError::Unsupported("pausing a VM is not implemented by this engine".into())) }
    }
    /// Lets a paused VM run again, waiting at most `within`. Added in M5b;
    /// section 3.22.
    fn resume(
        &self,
        _id: &VmId,
        _within: Duration,
    ) -> impl Future<Output = Result<(), EngineError>> + Send {
        async { Err(EngineError::Unsupported("pausing a VM is not implemented by this engine".into())) }
    }
    /// Asks the guest of the running VM that attaches the volume to flush the
    /// volume's filesystem, waiting at most `within`. Added in M5b (ADR 0282);
    /// section 3.22.
    fn flush_volume(
        &self,
        _id: &VolumeId,
        _within: Duration,
    ) -> impl Future<Output = Result<GuestFlush, EngineError>> + Send {
        async { Ok(GuestFlush::Unsupported) }
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
```

The derives, `MacAddr`, the blanket `ConsoleIo` implementation, `Display` and `Error` for `EngineError` and `VmSpec::canonical` (section 3.6) are contract items (D-0071), added additively during M0. `events`, `output`, `console` and `serial_output` are not `async` because they only build a stream; the first item is produced by the stream itself.

M4 adds, additively: `VmSpec::serial`, `VmSpec::vsock` and `VmSpec::work_disks` with `SerialMode`, `VsockAttach` and `WorkDisk`; `NetAttach::egress_only`; `Capabilities::exec`; `OciLayout`, `ByteStream`, `ExecRequest` and `ExecSession`; and the methods `prepare_layout`, `exec` and `serial_output`, whose default implementations return `Unsupported`, so a backend that does not offer them still compiles. Code that builds a `VmSpec`, `NetAttach` or `Capabilities` literal sets the new fields to `SerialMode::File` (the M1-M3 serial log), `None`, an empty list, `false` and `false`, which keeps the M1-M3 behaviour. `VmSpec::canonical` keeps `work_disks` in order: their serials are unique, so the order carries no meaning, but the node agent always builds them in the same order.

M5a adds, additively: `VolumeAttach::serial`, `VolumeInfo`, `SnapshotMethod`, `Capabilities::volume_snapshot_method`, `VolumeSnapshot::method` and `VolumeSnapshot::pause`, and the methods `ensure_volume`, `import_volume` and `delete_volume`, whose default implementations return `Unsupported` like `snapshot_volume` and `delete_volume_snapshot`. It also adds `ImageInventory`, `InventoryPackage` and the methods `image_inventory` and `inventory_cached_image`, whose default implementations return `Unsupported`, and `uninventoried_images`, whose default implementation returns an empty list, so a backend without inventories never starts a backfill. Code that builds a `Capabilities` literal sets the new field to `None`; `VmSpec::canonical` keeps sorting `volumes` by `(volume_id, mount_path)`, and the serial follows the volume.

M5b adds, additively: `ByteRange`, `RangesOf`, `ChangedRanges`, `StageFrom` and `StagedVolume`, and the methods `volume_changed_ranges`, `stage_volume`, `commit_staged_volume`, `discard_staged_volume`, `pause` and `resume`, whose default implementations return `Unsupported`, so a backend without warm volumes still compiles and its node is never a warm node (ADR 0172). `VmState` gains no variant: a paused VM is reported `Running` (section 3.22). One rule changes: a volume may have several snapshots at once (section 3.16). ADR 0282 then adds `GuestFlush`, the method `flush_volume` and one field, `VolumeSnapshot::guest_flush`. The default implementation of `flush_volume` returns `Ok(GuestFlush::Unsupported)`, so a backend that does not flush still lets a switchover go on. Code that builds a `VolumeSnapshot` literal sets the new field, to `GuestFlush::Unsupported` in a backend that does not flush.

## 3. Method semantics

Every method is total in the sense that it either succeeds or returns one `EngineError`; no method panics on bad input, and no method returns a partially created VM on success. The rules below are this contract's definitions; each one that follows from an accepted decision, a research finding or an open question cites it inline.

### 3.1 `capabilities`

Reports what this backend and build support. It never fails: a missing binary or an unusable host is reported as `kvm: false`, not as an error, because the node agent must be able to report a node's real state without a working engine. `engine` is the backend name, `version` the pinned build, and `graphical_console` is `false` for Cloud Hypervisor: it has no VNC, SPICE, GPU or framebuffer device, and its consoles are text only (F-0034). `text_console` is `true`. `exec` reports whether `exec` (section 3.18) works: the backend attaches vsock devices and the guest init it ships listens for exec sessions. `vm_snapshot` reports whether the backend can snapshot VM memory and device state; that capability is not used by Quoro before M5 and never as a volume backup (F-0035).

### 3.2 `probe_host`

Checks the machine rather than a VM:

- `/dev/kvm` exists and is writable;
- the kernel release is at least **6.8**, which Cloud Hypervisor v54 requires (F-0036);
- the CPU exposes `vmx` or `svm`;
- nesting is visible when the machine is itself a guest.

The procedure follows `docs/research/vps-nested-virt.md`. A successful probe is necessary but not sufficient: `/dev/kvm` can exist while a guest still fails to start, so the node agent treats the first real boot as the confirmation and reports a failed boot as a deployment failure in the `boot` phase.

### 3.3 `ensure_network`

Brings the node network to the requested `NodeNetwork`: the bridge exists, carries the gateway address, and forwards app traffic. Quoro owns the networking layer — bridges, TAP devices, addresses, firewall rules — rather than delegating it (D-0007). Precondition: `subnet` and `gateway` are consistent. Postcondition: a VM with an address inside `subnet` can reach the gateway. Idempotent: calling it twice with the same values changes nothing. An existing bridge with a different subnet is `Conflict`, never a silent reconfiguration. A node whose subnet changes (M5a, ADR 0144) ensures a second bridge under another name instead (`node-agent-protocol.md` section 6.9). The WireGuard mesh and the firewall rules on its interface belong to the node agent, not to the engine: the engine provides the bridge that the mesh's traffic is routed to.

The identity of a network is `bridge`, `subnet` and `gateway`. `dns` is not part of it: a call that repeats the identity with a different `dns` is `Ok` and changes the servers given to VMs created afterwards; a VM that already exists keeps the servers it was created with until it is recreated. The node agent fills `dns` from the node's own configuration, so guest resolvers are a property of the node, not of the control plane.

**Isolation groups.** Every VM on the bridge is isolated from every other VM on it, except from the VMs that carry the same `NetAttach::isolation_group` (D-0084). Two VMs with the same group exchange traffic over the bridge in both directions; two VMs with different groups, or a VM whose group is `None`, exchange nothing — neither frames on the bridge nor packets routed through the gateway. Every VM still reaches the gateway and, through it, the outside, and the host still opens connections to every VM, because the ingress runs on the host. The node agent sets the group to the VM's `project_id`, so the VMs of one project reach each other and VMs of different projects do not. The rule is enforced by the engine and holds across VM creates, deletes and an agent restart; the group is part of the VM's spec, so it is compared by `create` (section 3.6) like every other field.

**Egress only.** A VM whose `NetAttach::egress_only` is `true` reaches the outside through the gateway and nothing else on the node: no other VM, whatever its group; no address of the host (the node API, the ingress listeners, the node's own services), except the DNS servers in `NodeNetwork::dns` on port 53; and no link-local address (`169.254.0.0/16`). The host may still open connections to it. Builder VMs run untrusted Dockerfiles with outbound network to fetch bases and dependencies, and this is their boundary (ADR 0118). `egress_only` with an `isolation_group` other than `None` is `Conflict`.

### 3.4 `prepare_image`

Pulls the OCI image, verifies that the pulled content matches `ImageRef::digest`, converts it into a rootfs image, records the extracted `ImageConfig`, and caches the result by digest.

- Precondition: `reference` and `digest` are well formed.
- Postcondition: `PreparedImage::rootfs_path` exists and boots; a second call with the same digest returns the cached value without network access.
- Idempotent by digest. A digest mismatch between the reference and the pulled content is `Image`.
- A local OCI layout written by a builder takes `prepare_layout` instead (section 3.17); both fill the same cache.
- The conversion approach is decided (D-0061): Quoro's own OCI-to-ext4 pipeline, with a Rust registry client and `mkfs.ext4 -d`, validated by the M0 spike in `spikes/oci-microvm`. Three gaps must be closed before the pipeline ships: resolve layer paths inside the rootfs with `openat2` `RESOLVE_IN_ROOT` instead of refusing absolute-symlink parents, support zstd layers, and deliver the configuration through a config disk or the kernel command line instead of baking it into the rootfs.

### 3.5 `remove_image`

Removes a cached prepared rootfs by digest. Deleting a VM must not remove an image another VM uses, so removal is a reference-counted operation or is driven by the node agent after it has computed the desired set. Missing digest is `Ok`. Idempotent. Rootfs images are per-digest artifacts of the deployment pipeline (D-0008, D-0033), so disposing of them is the engine's cleanup, not part of any API delete.

### 3.6 `create`

Creates the VM and its runtime state without starting it. One microVM per app instance, booted from a rootfs built from the OCI image (D-0008), with the lifecycle split that M1 requires (D-0033). Postcondition on success: a VMM process exists (or the backend's equivalent), the VM is inspectable, and `state` is `Stopped` once creation finishes — `Creating` is only observable while the call is in flight.

- The same `id` with an equal spec returns `Ok` and the existing `VmInfo`.
- The same `id` with a different spec is `Conflict`. Equality is compared on the canonical form of `VmSpec`, `VmSpec::canonical()`, which sorts `env` by `(name, value)` and `volumes` by `(volume_id, mount_path)`; everything else keeps its order, so field order and the ordering of those two lists must be normalized before comparison.
- Resources are reserved at `create`, so `ResourceExhausted` is returned here rather than at `start`.
- Precondition: `rootfs` is the digest of a prepared image, `net` is inside a network already ensured by `ensure_network`, and every volume in `volumes` exists on this node (section 3.20). A missing volume is `NotFound`, never created here. Each volume is attached as a writable virtio-blk disk with its `serial`; serials are unique within the spec, or the spec is `Conflict`. A volume attached to another VM that exists is `Conflict`: in M5a a volume belongs to one VM at a time.
- `serial: Socket` makes the engine connect to the serial socket as soon as it exists, before boot, so the ring of section 3.19 holds the whole boot. `vsock` attaches the device; its socket lives in the VM's runtime directory, which only root can enter.
- Each `WorkDisk` is attached as a writable virtio-blk disk with its serial. A missing file is created as an empty ext4 filesystem of `size_gb`; an existing file is attached as it is. Two VMs that name the same work-disk path at once is `Conflict`. The engine never removes a work disk (section 3.9).

### 3.7 `start`

Boots a created or stopped VM. Postcondition on success: `state` is `Running` and `EngineEvent::Started` has been or will be emitted. A boot is fast enough that a stopped warm copy is a practical failover target (D-0008, D-0020); the boot-time figure is still a marketing claim, not a measured number (`docs/research/cloud-hypervisor.md`).

- Starting a running VM returns `Ok` and does not reboot it. Restarts are expressed as a spec change by the node agent (`restart_generation` in `node-agent-protocol.md`), so the engine never has to guess intent.
- Starting an unknown id is `NotFound`. Starting a VM whose kernel or rootfs is missing is `Image` or `Io`.
- `within` bounds the call (ADR 0242). When the VM is not `Running` once `within` has passed, the engine gives up: it begins a forced stop of what it started and returns `Timeout` without waiting for that stop to finish. After `Timeout` the VM may still be running until that stop completes, so the node agent treats it as a VM whose state it does not know. The node agent passes 3 s for a stateful start under an `expiring` lease (`node-agent-protocol.md` section 6.4) and a bound of its choosing for every other start.

### 3.8 `stop`

Stops a VM.

- `Graceful { timeout, force_within }` asks the guest to exit — the power-button path, which the minimal init turns into `SIGTERM` to the app (section 7) — and waits at most `timeout`. On expiry it stops the VM as `Force { within: force_within }`. The call returns `Ok` only when the VM is stopped, and returns within `timeout + force_within`.
  - A VM that a `PauseCopy` snapshot paused (section 3.16) is first resumed, inside `timeout`, and then asked to exit as above. A VM that `pause` froze (section 3.22) is never resumed (ADR 0270).
  - This holds for every graceful stop, a fence's included: during its graceful phase, bounded by `T_grace` (`node-agent-protocol.md` section 8.5 step 1), a fence resumes such a VM and presses the power button, so the guest may run for up to `T_grace`, as a VM that was not paused does (ADR 0279). The forced stop that follows the expiry of `timeout`, like any `Force`, never resumes a VM.
- `Force { within }` stops without cooperation from the guest. If the VMM does not confirm the stop (its API does not answer, or the process does not exit), the backend escalates to `SIGKILL` on the VMM process and reaps it, all inside `within`. `Force` returns `Ok` only when the VMM process is gone; when it is not gone once `within` has passed, the call returns `Timeout`. Any error from a forced stop, `Timeout` included, means the VM may still be running, and the node agent must treat it that way (`node-agent-protocol.md` section 8.5, D-0074).
- A forced stop, once begun, goes on to the kill and the reap after the call has returned `Timeout` or its future was dropped (section 4). Both modes carry the caller's bound (ADR 0243).
- Stopping a stopped VM returns `Ok`. Stopping an unknown id is `NotFound`.
- Postcondition: `state` is `Stopped`, `exit_code` reports the app's code when the guest reported one, and `EngineEvent::Exited` has been or will be emitted.

### 3.9 `delete`

Removes the VM and its own disks. Missing id returns `Ok`, so a retried delete converges. Volumes are never removed by `delete`: they belong to the app, not to the instance, and are deleted through the API with `app:delete` (D-0033, `README.md`). Postcondition: the VMM process is gone, the root and data disks are removed, and `inspect` returns `NotFound`. Work disks are kept too: the node agent owns them and removes them with the app (ADR 0118). The serial ring of section 3.19 is removed with the runtime directory. Precondition: the VM is stopped; a call on a running VM is `Conflict`.

### 3.10 `inspect`

Returns one VM's `VmInfo`. Unknown id is `NotFound`. This is the only method that observes lifecycle state; it never changes it. It is the engine-level source of the `observed_status` a node reports (D-0032 detect step), and it never reports a state outside the enum in `README.md`.

### 3.11 `list`

Returns every VM the engine knows about on this node, including VMs created by a previous agent process. This is how the node agent rediscovers its world after a restart: VMMs are separate processes and keep running, so `list` must find them from the runtime directories on disk and the backend's API, not from in-memory state (D-0011). A VM whose runtime directory exists but whose process is gone is reported as `Crashed` with the last known exit code, or as `Stopped` when the exit followed a requested stop. `list` never starts, restarts or deletes anything. Neither does the engine's start-up rediscovery, which only resumes a VM a `PauseCopy` snapshot left paused (section 3.16, ADR 0271).

### 3.12 `events`

A stream of `EngineEvent` for every VM on the node. The node agent maps them to the event types in `README.md` — a `Crashed` becomes `vm.crashed`, a requested stop becomes `vm.stopped`, a successful boot becomes `vm.started` (D-0032). The stream exists for the lifetime of the engine instance; a slow consumer must not block the engine, so the implementation buffers and drops with a recorded counter rather than applying backpressure to the VMM.

### 3.13 `output`

A stream of `OutputLine` for one VM: the app's stdout and stderr, tagged by stream, produced by the minimal init and forwarded over the virtio-console. This feeds local logs, which stay on the node and are queried on demand (D-0014). A second concurrent call for the same VM is `Conflict` when the backend can serve only one reader; `NotFound` when the VM has no console configured (`ConsoleMode::Off`).

### 3.14 `console`

A bidirectional byte stream on the text console: a human types into a terminal in the panel, and the bytes reach the guest's serial console (F-0034; consoles are text-only, xterm.js over serial or virtio-console, D-0056). Used from M4 (D-0033). Text only; there is no graphical console to attach to (F-0034, D-0056). The Cloud Hypervisor console socket accepts one client at a time, so a second concurrent call is `Conflict` (`docs/research/cloud-hypervisor.md`).

**Superseded in M4.** A second connection to a Cloud Hypervisor serial socket silently takes the port away from the current client, and nothing in the guest reads the serial port, so a direct, bidirectional connection per viewer is unsafe and useless (ADR 0116). The M4 console is read-only and comes from `serial_output` (section 3.19); the web terminal comes from `exec` (section 3.18). `console` stays for `ConsoleMode::Serial` specs and for `FakeEngine`; on a VM whose `serial` is `Socket` it is `Conflict`. Nothing in the node agent calls it from M4.

### 3.15 `counters`

Returns the VM's counters for metrics (M4, D-0033). Unknown id is `NotFound`. Values are cumulative since VM start; the node agent computes rates. Counters are best-effort: a backend that cannot report one of the groups returns zeros for it rather than failing the call.

The M4 metrics use two of them (ADR 0014, `node-agent-protocol.md` sections 5.15 and 5.17): `cpu_time_ms` and `memory_rss_bytes`, which the Cloud Hypervisor backend reads from the VMM process on the host (`/proc/<pid>/stat` and `/proc/<pid>/status`), not from the backend's counter document, so they need no guest agent. The node agent samples them every 5 seconds.

### 3.16 `snapshot_volume` and `delete_volume_snapshot`

The point-in-time disk primitive the chunk engine needs (D-0023). Both are `Unsupported` in the default implementation, and only the M5 code path calls them. ADR 0156 decides the method for M5a; it supersedes ADR 0146, which superseded ADR 0062 on hosts whose filesystem has no reflink.

- **Probe.** At start-up the engine creates a scratch file in the directory that holds volumes and tries `ioctl(FICLONE)` into a second one, then removes both. If the clone succeeds, the data directory's own filesystem gives `Reflink` (XFS with reflink, btrfs). Otherwise the engine uses the volume pool below and probes again inside it. If the pool cannot be used, the method is `PauseCopy`. The method is reported in `Capabilities::volume_snapshot_method`, which the node agent sends in `hello` (`node-agent-protocol.md` section 5.1); `Reflink` covers both the native and the pool path.
- **Volume pool (ADR 0156).** On a data directory without reflink (ext4), the volume directory is the mount point of a sparse XFS image, `volume-pool.img` in the engine's state directory, created with `mkfs.xfs -m reflink=1` and mounted through a loop device with `nodev,nosuid,noexec`. Volume files and their snapshots live inside it. The host mounts the pool's XFS and nothing else: it never mounts the filesystem inside a volume file.
  - **Lifecycle.** The pool has no fstab entry and no systemd unit: the engine attaches it at every start-up, before any other volume call, and reuses a pool that is already mounted on the volume directory (a node agent restart while VMs run). The loop device is attached with auto-clear, so `umount` of the volume directory releases it; the engine never detaches the pool while it holds a volume.
  - **A pool that exists but does not mount** (missing `xfsprogs`, no loop device, a damaged image) is never replaced by the empty directory underneath: every volume call returns `Unsupported` with the reason, `volume_snapshot_method` is `None`, and the node agent reports the node's volumes `failed`.
  - **Size.** The image is sparse. Before `ensure_volume` or `import_volume` the engine grows it to at least twice the sum of its volumes' sizes plus 1 GiB, which leaves room for one snapshot of every volume, by extending the image, refreshing the loop device's capacity and running `xfs_growfs`. It never shrinks it. The free-space checks of `ensure_volume` and of a snapshot use the filesystem that holds the image, not the pool's own free space.
  - **Volumes created before the pool.** If the volume directory already holds volume files when the pool would be created, the engine leaves them in place, keeps `PauseCopy` for that node and logs why; it creates the pool at the first start-up with no such file. It never moves a volume file that a VM may hold open.
  - **Fallback.** Without `mkfs.xfs` or a loop device the pool cannot be created and the method is `PauseCopy`.
- **Guest flush first (ADR 0282).** Data an app wrote without `fsync` can still be in the guest's page cache, not in the volume file: with ext4's delayed allocation and the guest kernel's writeback defaults, it reaches the file up to about 35 s later. Before either method's own steps, the clone of `Reflink` or the pause of `PauseCopy`, the engine therefore asks the init of the running VM that attaches the volume to flush it. It opens a sync session on the VM's exec port that names the volume's serial (section 6). The whole flush, from the `/vm.info` that tells whether the VM is paused through the connect to the answer, takes at most 2 s.
  - An unknown volume is `NotFound` before the flush.
  - The guest keeps running during the flush, so `Reflink`'s `pause` stays zero and `PauseCopy`'s `pause` does not grow.
  - The flush takes no VM operation lock (section 4).
  - The outcome is the descriptor's `guest_flush`:
    - `Flushed` when the init answers `132` with an empty payload;
    - `NoRunningVm` when no running VM attaches the volume;
    - `Paused` when that VM is paused: a paused guest cannot answer, so it is not asked;
    - `Unsupported` when the VM has no `vsock`, or when its init predates ADR 0282. Such an init answers the sync frame with `131` and the message `the first frame must be open, got type 5`, the wording of every init from M4 until ADR 0282, which the engine matches exactly;
    - `TimedOut` when the flush has no result within 2 s, a `/vm.info` that does not return in time included;
    - `Failed` for every other result: any other `131`, any other answer (a `132` with a payload, a `129` or a `130` included), a close without an answer, a connection that fails, or a VMM that cannot report whether the VM is paused.
  - **The snapshot is taken whatever the outcome.** A flush that fails, times out or is unsupported never fails the snapshot (ADR 0282). Such a snapshot is crash-consistent, as below.
  - The outcome describes the VM the engine found when the snapshot began. A VM that starts after that point is not flushed, and what it wrote since its boot is crash-consistent only. A VM that is paused, stopped or fenced during the flush gives `TimedOut` or `Failed`.
- **`Reflink`.** After the flush, the engine clones the volume file into a snapshot file next to it with `FICLONE`, without touching the VM. The snapshot is taken at the instant of the clone; `pause` is zero.
- **`PauseCopy`.** After the flush, if a VM that attaches the volume is `Running`, the engine pauses it (`/vm.pause`), copies the volume file into a snapshot file keeping holes (`SEEK_DATA` and `SEEK_HOLE`), and resumes it (`/vm.resume`), whatever the copy's outcome. `pause` is the time between the pause and the resume. A volume no running VM attaches is copied without a pause. If a VM that attaches it starts during that copy, the snapshot is `Conflict` and leaves no file, since the guest may have written to the volume while it was copied (ADR 0269). A resume that fails is `Hypervisor`, and the VM is left for the node agent's crash handling.
  - **Locks and errors (ADR 0271).** The pause and the resume each take the VM's operation lock (section 4), which is free during the copy. A VM that attaches the volume and is already paused, by `pause` (section 3.22) or by a snapshot of another volume, is `Conflict`, since that freeze could end during the copy. A forced stop that waits for the lock gets it at once: before the pause the snapshot is then `Conflict`, and at the resume it is `Hypervisor`. A VM whose VMM stopped or was replaced during the copy is not resumed, its copy is not kept, and the snapshot is `Hypervisor`.
  - **A restart during the copy (ADR 0271).** Before the pause, the engine records the paused VMM process in the VM's runtime directory, and removes the record after the resume. When the engine starts again after a restart during the copy, it resumes that process once it adopts it, unless a stop of the VM was requested, a fence's included, which then ends it. A record of a process that is gone or runs is removed; while the VMM does not report whether the VM is paused, the record stays.
  - **A graceful stop during the copy (ADR 0270).** The record is the snapshot's claim on its pause. A graceful `stop` of the VM while the record names the VM's live VMM process ends that pause itself: under the VM's lock it removes the record, resumes the VM and asks the guest to power off (section 3.8). At its resume, a snapshot whose record is gone or names another process does not resume the VM and drops its copy, and the snapshot is `Hypervisor`, since the guest ran during the copy. A forced stop does not resume the VM; a fence's graceful phase is a graceful stop and does (section 3.8, ADR 0279). A `pause` freeze (section 3.22) has no such record, so a stop never resumes it.
- **Consistency (ADR 0282).**
  - **With `Flushed`,** the snapshot holds every write the guest completed before the flush, with or without `fsync`. Writes between the flush's answer and the clone or the pause, a few milliseconds, are crash-consistent only.
  - **With `NoRunningVm`,** no VM that attaches the volume was running when the snapshot began, so no guest held unwritten data of it.
  - **With any other outcome,** data written without `fsync` in about the last 35 s may be missing from the snapshot. This covers a VM whose init predates ADR 0282 until its next start, and a flush that failed or timed out.
  - **Every snapshot** still gives the guest at least the guarantee of a power cut: an ext4 volume replays its journal after a restore, and data that was `fsync`ed is always in the snapshot.
  - An app that needs more than "everything completed before the snapshot" uses its own dump (ADR 0023).
- The snapshot is a node-local file that only the chunk engine reads. It is never attached to a VM and never mounted. `delete_volume_snapshot` removes it; a missing snapshot is `Ok`, and one whose location is outside the engine's current volume directory is `Conflict` (section 3.22). Snapshots left by a crash are removed by the node agent at start. From M5b the node agent keeps those its store records: a warm volume's base and the snapshot being shipped, the candidate copies of a returning source and the quarantined copies (section 3.22, `node-agent-protocol.md` sections 6.13 and 9); it removes the rest.
- In M5a at most one snapshot per volume exists at a time, and a second `snapshot_volume` while one exists is `Conflict`. From M5b a volume may have several at once, each with its own `snapshot_id`: a warm source keeps its base and the snapshot it ships, a backup may take another, and a returning source keeps its candidates and a quarantined copy. The node agent still runs at most one backup snapshot of a volume at a time. An unknown volume is `NotFound`. Too little free space for a `PauseCopy` is `ResourceExhausted`, checked after the flush and before the pause.
- They must **not** be implemented with Cloud Hypervisor VM snapshots. A CH snapshot is a paused VM's `config.json`, `memory-ranges` and `state.json`; it contains guest RAM and device state but no disk contents, and CH exposes no dirty bitmap (F-0035). `/vm.info`, which tells the flush whether the VM is paused, `/vm.pause` and `/vm.resume` are the only VMM calls this method makes.
- Warm volumes (M5b) keep these reflink snapshots, native or in the pool: the changed ranges between two consecutive snapshots of a volume are found by comparing their extent maps (FIEMAP), with a full hashed scan of the snapshot as the fallback, and neither lvm-thin nor btrfs is used (ADR 0172). A `PauseCopy` node cannot hold a warm volume. The call that returns the changed ranges is `volume_changed_ranges` (section 3.22).

### 3.17 `prepare_layout`

Turns a local OCI image layout, as a builder VM wrote it to its output disk, into a prepared rootfs, the way `prepare_image` does for a pulled image (ADR 0117, ADR 0118). Added in M4.

- Precondition: `path` is a readable OCI image layout whose index names `manifest_digest`: a directory, or a tarball of one, which may be followed by zero padding up to the end of the file (a raw output disk, `node-agent-protocol.md` section 6.6).
- A tarball is read in user space as untrusted input: only regular files named `oci-layout`, `index.json` and `blobs/sha256/<64 hex>` are taken, every other entry (a link, a device, a path with `..`, a duplicate name) makes the layout `Image`, and the bytes read are bounded by the node's image limit.
- The engine verifies every blob against its digest, checks that the manifest's digest is `manifest_digest`, applies the layers with the same pipeline as `prepare_image` (D-0061), extracts `ImageConfig`, and caches the result by `manifest_digest` in the same cache. `create` then boots it with `rootfs` set to that digest, and `remove_image` removes it.
- Idempotent by digest: a second call with a digest already cached returns the cached value without reading the layout.
- A missing or malformed layout, a blob that does not match its digest, a media type the pipeline does not support, or a layout larger than the node's image limit is `Image`. Nothing is pulled: a layout that references a blob it does not contain is `Image`, not a registry fetch.
- The engine never deletes the layout; the node agent does, after the call returns.

### 3.18 `exec`

Starts a program on a PTY inside a running VM, in the app's root filesystem, with the app's environment and working directory, and returns its byte stream (ADR 0113). Added in M4; the web terminal is its only caller.

- The engine connects to the VM's vsock socket with Cloud Hypervisor's hybrid handshake (`CONNECT <port>\n`, answered `OK <port>\n`) on the init's exec port (section 6), and sends the open request. The guest init runs the program and relays bytes; the engine relays them to `ExecSession::io`.
- `argv` is non-empty, holds no NUL, and is at most 64 items of at most 4096 bytes. `rows` and `cols` are 1 to 1000. Otherwise `Conflict`.
- An unknown id is `NotFound`. A VM that is not `Running`, has no `vsock`, or whose init does not listen (a VM started before its node got the M4 init) is `Conflict`. A missing program is not an error of the call: the session starts and `exit` resolves with `127`.
- Many sessions per VM may be open at once. Each is independent: closing one does not touch another, and the app keeps running.
- Back-pressure: when the reader of `io` stops reading, the engine stops reading from the vsock connection, which stops the program at its next write. A session never buffers without bound.
- The guest is untrusted. The engine bounds every frame it reads from the guest (section 6) and treats output as bytes, never parsing it.
- The vsock socket accepts connections from the host only; the node never creates a `<socket>_<port>` listener, so the guest cannot open a connection to the host over vsock.
- **The same port carries the engine's sync sessions (ADR 0282).** The engine opens them itself before a volume snapshot (section 3.16) and in `flush_volume` (section 3.22). `exec` never sends a sync frame.
  - The web terminal is the only caller of `open`, and the engine's own volume flush is the only caller of `sync` (section 6).
  - Neither the API nor MCP exposes a sync session.
  - A terminal user cannot send one, because the engine frames the user's input as `2`.

### 3.19 `serial_output`

A read-only stream of a VM's serial output: first up to `tail_bytes` of the newest bytes the node kept (all of it when `None`), then live output until the VM is deleted or the caller drops the stream (ADR 0116). Added in M4.

- With `serial: Socket`, the engine is the socket's only client, from `create` on. It appends what it reads to a node-local ring in the VM's runtime directory: two segments of 512 KiB, the older one dropped when the newer one fills, so the ring holds at most 1 MiB and survives VMM restarts, which keeps the previous boot's output after a crash loop. While the node agent restarts, the VMM keeps up to 1 MiB of output and replays it when the engine reconnects.
- Any number of streams may be open on one VM; each one reads the ring and then follows, so viewers never connect to the socket.
- An unknown id is `NotFound`. A VM whose `serial` is not `Socket` is `Conflict`.
- Input is never written to the serial port: nothing in the guest reads it.

### 3.20 `ensure_volume`, `import_volume` and `delete_volume`

The local volumes of ADR 0145: one sparse raw file per volume in the node's data directory, named by the volume id, owned by root and never mounted on the host. Added in M5a; the default implementations return `Unsupported`.

- `ensure_volume` creates the file as an empty ext4 filesystem of `size_gb` gibibytes, sparse, with `mkfs.ext4` run on the file itself, never on a mount. A file that exists with the same size returns its `VolumeInfo`; one with another size is `Conflict`, because M5a has no resize. Too little free disk for the filesystem's metadata is `ResourceExhausted`. Idempotent.
- `import_volume` moves a raw file that the node agent wrote from a backup (`backup-format.md` section 8.1) into place as the volume's file, in one rename on the same filesystem. When the file is on another filesystem than the volume directory (the volume directory is the XFS volume pool of section 3.16), the engine first copies it into a temporary file in the volume directory keeping holes (`SEEK_DATA` and `SEEK_HOLE`), syncs it, renames that, and then removes the file it was given. The file must be exactly `size_gb` gibibytes. The engine does not parse its content: a filesystem a guest wrote is checked by the guest when it mounts it. A volume that already exists is `Conflict`.
- `delete_volume` removes the file. A missing file is `Ok`. A volume attached to a VM that exists is `Conflict`; the node agent deletes the VM first.
- `delete` of a VM never removes its volumes (section 3.9); only `delete_volume` does.
- The node agent, not the engine, decides when a volume is created, imported or deleted (`node-agent-protocol.md` section 6.7).

### 3.21 `image_inventory`

Returns the package inventory of a prepared image (ADR 0150, ADR 0151). Added in M5a; the default implementation returns `Unsupported`.

- `prepare_image` and `prepare_layout` build the inventory in the same pass that applies the layers, from the unpacked tree, before `mkfs.ext4 -d`, and cache it with the image by digest. What is read, the bounds (20 000 packages, 4 MiB) and the `not_inventoried` kinds are in `node-agent-protocol.md` section 6.11. A failure to inventory never fails the preparation: the image is cached, and the inventory lists what it could not read.
- The parsers only read files of the unpacked tree, resolved inside it like the layer paths (section 3.4); they never execute anything from the image.
- `image_inventory` reads the cached inventory and never touches the network. A digest that is not prepared is `NotFound`, and so is a prepared image that has no inventory yet.
- **Images prepared before M5a.** An image cached by an earlier release has its root disk and its configuration but no inventory, and a second `prepare_image` or `prepare_layout` of its digest returns the cached value (sections 3.4, 3.17), so a redeploy does not inventory it either. The engine inventories such an image from its root disk instead, once, without the network:
  - `uninventoried_images` lists the committed cache entries that have a root disk and no inventory. It reads only the cache directory.
  - `inventory_cached_image` builds the inventory of one of them from its `rootfs.ext4`, read-only, with the same parsers, bounds and `not_inventoried` kinds as a fresh preparation, caches it with the image and returns it. The root disk was built by the engine's own `mkfs.ext4 -d` and is attached read-only to every VM (section 5), so its metadata was never written by a guest; the parsers still reach its files only through paths resolved inside the root, as in section 3.4. The root disk is never written, and nothing on it is executed.
  - A digest that is not cached is `NotFound`. A digest that already has an inventory returns it as it is, without reading the root disk again.
  - A root disk that cannot be opened at all (the host lacks a tool, the kernel refuses the filesystem) is an error, and no inventory is written: the image stays without one, and the next start of the node agent tries again. A root disk that opens but whose tree cannot be read gives an inventory whose `not_inventoried` is `inventory_failed`, as for a fresh preparation, and that inventory is kept.
  - The inventory is written so that a crash never leaves a partial one: a later `image_inventory` returns either nothing or the whole inventory.
  - The node agent calls `uninventoried_images` once, in the background, after it starts, then `inventory_cached_image` for one image at a time, and sends each inventory it gets as it does for a freshly prepared image (`node-agent-protocol.md` section 6.11). The pass never delays the node's start, a VM start or a preparation; an image that fails is logged and the pass goes on with the next one.
- `remove_image` removes the inventory with the image. `inventory_cached_image` and `remove_image` of one digest are serialized, so a removal never races the inventory being written.

### 3.22 Warm volumes: `volume_changed_ranges`, staging, `pause`, `resume` and `flush_volume`

What a warm source and a warm target need from the engine (ADR 0172 to ADR 0177, `node-agent-protocol.md` section 6.13). Added in M5b; the default implementations return `Unsupported`, and a node whose `volume_snapshot_method` is not `Reflink` is never offered a warm volume. The engine finds what changed and moves files into place; the node agent reads and writes the bytes, hashes them and talks to the other node, as for backups (section 3.16).

- **`volume_changed_ranges`** returns the ranges of `newer` whose data may differ from `base`. Both are files of the same volume: `newer` is a snapshot or the volume file itself, `base` a snapshot taken earlier, so they share every block that was not written in between. The engine reads both extent maps with `FS_IOC_FIEMAP` and `FIEMAP_FLAG_SYNC` and reports every logical range of `newer` whose physical block is not the same in `base`, rounded out to 4 KiB, including the ranges that are holes in one file and not in the other. An extent flagged `UNKNOWN`, `DELALLOC`, `ENCODED`, `NOT_ALIGNED`, `DATA_INLINE` or `DATA_TAIL` in either map counts as changed. The result may contain ranges whose bytes are in fact equal, never fewer than were written: on the lab the comparison found exactly the written 4 KiB blocks over 50 cycles, in about 20 ms, at about 4 µs per extent (`docs/research/m5b-warm-and-leases.md`).
  - With no `base`, or when the extent maps cannot be compared (the files are not on the same filesystem, the filesystem does not report physical blocks), it returns every allocated range of `newer`, from `SEEK_DATA` and `SEEK_HOLE`, with `full: true`. The node agent then ships a full copy.
  - `extent_count` is the number of extents in `newer`'s map. The source reports it (`status_report.warm`), because the cost of a cycle grows with it.
  - `newer` as `Volume` compares the live volume file, which a returning source does before it keeps a quarantined copy (`node-agent-protocol.md` section 9). A VM that exists and attaches the volume makes that `Conflict`, because the file could change during the comparison. An unknown volume or snapshot is `NotFound`. A `base` of another volume is `Conflict`.
- **`stage_volume`** creates a staging file next to the volume file, `size_gb` gibibytes, which the node agent writes a received copy into: an empty sparse file (`Empty`), or a reflink clone of the volume file (`Volume`) or of a snapshot of the volume (`Snapshot`), so an incremental copy writes only the changed blocks. A clone of a file of another size is `Conflict`; a missing source is `NotFound`; too little free space for the staging file's metadata is `ResourceExhausted`. Each call creates a new staging file; a volume has at most one at a time, so a second call while one exists is `Conflict`.
- **`commit_staged_volume`** syncs the staging file, renames it over the volume file in one step on the same filesystem, syncs the directory and returns the volume's `VolumeInfo`. A volume that has no file yet (the first copy on a target) gets it. A VM that exists and attaches the volume is `Conflict`: the target's standby VM is not created until it is the source (`node-agent-protocol.md` section 5.11). After a crash the volume file is either the old copy or the new one, never a mix.
- **`discard_staged_volume`** removes the staging file; a missing one is `Ok`. Staging files left by a crash are removed by the node agent at start, so a copy that did not complete leaves nothing.
- **`pause`** freezes a running VM's vCPUs; its memory, devices and volumes stay as they are, and the guest sees no event. **`resume`** lets it run again. They are what a planned switchover freezes the app's writes with (ADR 0177, `node-agent-protocol.md` section 5.17). Pausing a paused VM and resuming a running one are `Ok`. A VM that is not running or paused is `Conflict`; an unknown one is `NotFound`. A paused VM keeps `VmState::Running` in `inspect` and `list`: the node agent records the freeze itself, so it knows after a restart which VM it paused. `stop` and `delete` work on a paused VM, and no stop ever resumes a VM `pause` froze (section 3.8, ADR 0270). A graceful stop resumes a VM that a `PauseCopy` snapshot paused before it presses the power button, also during a fence's graceful phase (`node-agent-protocol.md` section 8.5 step 1), and the snapshot then drops its copy; a forced stop, a fence's forced phase included, never resumes a VM (ADR 0279).
  - **Bounds.** Both take a bound `within`, as `start` does (section 3.7, ADR 0242) and `stop` does with `force_within` (section 3.8, ADR 0243). `within` bounds the whole call, the wait for the VM's operation lock (section 4) included. When it passes before the VMM has confirmed the pause or the resume, the engine gives up with `Timeout` and releases the lock. It also gives up at once, with `Timeout`, when a forced stop waits for the lock, as a `PauseCopy` snapshot's pause and resume give up, with their own errors (section 3.16, ADR 0271). So a fence keeps its bound: its forced phase never waits for a `pause` or a `resume`, and its graceful phase waits for the lock only within `T_grace`.
  - After a `Timeout` the VM may be paused or running, and since a paused VM reads `Running` in `inspect`, the node agent cannot tell which. It keeps the freeze recorded and treats the VM as possibly running; under an `expiring` lease the fence at `valid_until`, and under any lease a revoke, still stops it, or the node marks itself failed when that stop cannot be confirmed (`node-agent-protocol.md` section 8.5).
  - A request the engine gave up on may still take effect, but within one engine instance never after a later `pause` or `resume` of the same VM has returned `Ok`: that later call waits, within its own bound, until the VMM has answered the earlier request or is gone, and returns `Timeout` when its bound passes first, in which case the earlier request may still land after it. A call that gives up before it began writing its request sends none; a request it began writing is finished in the task. At most one given-up request per VM waits in a task. A forced stop does not wait for it, since it ends the VMM process (section 3.8). So a `resume` that returns `Ok` after a `pause` that timed out leaves the VM running.
  - An engine restarts with the node agent and forgets such a request. Across that restart, the order rests on one property of the VMM: a request the agent wrote before it exited is acted on before any request sent after it, or never. It must hold for a request the VMM had not read yet when the agent's connection closed. This is assumed of Cloud Hypervisor's API, not enforced by the engine. A lab test in `crates/engine/tests/ch_lab.rs` must run before switchovers ship, and again on every change of the pinned Cloud Hypervisor release (section 5): on a fresh VM per run, it keeps the VMM's API busy with a request whose end the test controls and that a VM the engine creates accepts, such as `vm.add-disk` of an image on a filesystem the test serves and holds back; it sends a pause through one engine instance and drops that instance, so the pause's connection closes unanswered, as at an agent's exit; it then calls `resume` on a new engine instance, whose first request is the `/vm.info` that would come after a stale pause after a restart. A run counts only when the API socket's accept queue shows the pause's connection still queued after it closed, and both that connection and the new instance's first one queued before the test releases the busy request: a connection the VMM has not accepted is one it has not read, whatever the release does inside. A run that misses that window is repeated, not counted, and after 100 runs that miss it the test fails as inconclusive. It passes after 20 counted runs when, once the accept queue is empty and a later request has been answered, the VM is running every time and the pause never landed after the resume. Each node-agent build carries, compiled in apart from `pins.txt` (section 5), the list of Cloud Hypervisor releases whose test passed, which the engine reports in `Capabilities::vmm_order_passed`. The fallback goes by the release that runs the frozen VM, not by the agent's own pin, because a VMM outlives an agent upgrade: when the frozen VM's `VmInfo.vmm_version` is not on that list, `None` included, a node agent that restarts with a freeze still recorded stops that VM without a resume where `node-agent-protocol.md` section 5.17 would resume it (a thaw, or a `desired_state` that ends the freeze, that passes the check). It drops the record once the engine reports the VM stopped or gone, and only then starts what its desired state runs, under section 6.4 there. So no pause given up on before the restart can land after a later resume. The node agent also starts no new freeze of an app while an earlier freeze of it is still recorded (`node-agent-protocol.md` section 5.17). That narrows the window, so a new freeze's `pause` follows a resume that returned `Ok`, or runs on a new VMM, but only the VMM's order puts that resume after the old one.
  - The node agent passes the start bound, 3 s, to a resume under an `expiring` lease, so the start margin that `node-agent-protocol.md` section 5.17 asks of that resume covers it, as the same margin covers a start (section 6.4 there). It passes a bound of its choosing to the freeze's `pause` and to every other resume. A `Timeout` of the freeze's `pause` is a failed step of `switchover_freeze` (section 5.17 there).
- **Snapshot files.** Every call that takes a `VolumeSnapshot`, `volume_changed_ranges` (`newer` or `base`), `stage_volume` from a snapshot and `delete_volume_snapshot` (section 3.16), answers `Conflict` for a snapshot whose `location` is not a file directly in the engine's current volume directory with a name that starts with `<volume id>.` and ends with `.snapshot`, so no call reads, clones or deletes a file elsewhere. A file that `volume_changed_ranges` or `stage_volume` opens that is not a regular file, such as a directory or a symlink, is `Conflict`, and a missing one is `NotFound`. `delete_volume_snapshot` keeps the answers of section 3.16 for a location inside the directory: a missing file is `Ok`. The node agent's hand-over relies on these answers when it takes a file of that directory as evidence that the directory is the real one (`node-agent-protocol.md` section 9).
- **`flush_volume`** (ADR 0282) asks the guest to flush the volume's filesystem, as a snapshot does first (section 3.16), and returns the outcome after at most `within`.
  - The VM keeps running during the flush, which takes no VM operation lock (section 4). `within` bounds the whole flush, from the `/vm.info` check through the connect to the answer.
  - The outcomes are those of section 3.16, with `TimedOut` after `within`.
  - The only error of the call is `NotFound`, for an unknown volume. Any other failure to reach the guest, a failed `/vm.info` included, is the outcome `Failed`. On a node whose volume pool does not mount (section 3.16), the call returns `Ok(GuestFlush::Unsupported)`.
  - The default implementation returns `Ok(GuestFlush::Unsupported)`.
  - A planned switchover calls it before `pause`, with a 2 s bound (`node-agent-protocol.md` section 5.17), since a paused guest can no longer answer. Writes the guest completed before the flush are then in the final snapshot, and that snapshot itself reports `Paused`.
- The node agent, not the engine, decides when to snapshot, compare, stage, commit, flush or pause, and checks the lease and the token first (`node-agent-protocol.md` sections 6.13 and 8.6).

## 4. Concurrency

- Operations on one `VmId` are serialized by the caller. The node agent holds one lock per VM, so it never issues `start` and `delete` for the same id concurrently. The engine does not promise ordering for concurrent calls on one id and may return `Conflict`.
- The engine must be safe to call concurrently for **different** VMs, on many VMs at once. This is what lets one node run several apps and replace them independently.
- `capabilities`, `probe_host`, `ensure_network`, `prepare_image`, `remove_image`, `image_inventory`, `uninventoried_images`, `inventory_cached_image` and `list` are safe concurrently with any VM operation. So are `volume_changed_ranges` on snapshots and the staging calls, which the node agent serializes per volume; `pause` and `resume` are VM operations, serialized per `VmId` like `start` and `stop`. `flush_volume`, and the flush that begins a snapshot (section 3.16), take no VM operation lock and are safe concurrently with any VM operation, so a forced stop never waits for a flush (ADR 0282). `prepare_image`, `prepare_layout`, `inventory_cached_image` and `remove_image` are serialized per digest (section 3.21). `ensure_network` and `prepare_image` are internally serialized by the implementation where the underlying operation is not reentrant.
- `start`, `stop`, `pause` and `resume` enforce their bounds themselves, so a caller never has to drop the future to bound them. A `pause` or a `resume` that returned `Timeout` has released the VM's operation lock (section 3.22). Its request to the VMM, and that of a `pause` or `resume` whose future was dropped, stays in a task of its own until the VMM answers or is gone, so within one engine instance a later `pause` or `resume` that returns `Ok` acts after it (section 3.22). The task ends with the engine, and so with the node agent. Dropping the future of a `start` or a `stop` does not abandon a forced stop the engine has begun: the engine carries it to the kill and the reap in a task of its own. A `stop` called while such a stop of the same VM still runs joins it, within its own bound (ADR 0243).
- No call blocks the runtime for longer than an I/O wait: image preparation and boot are awaited, never run on the reactor thread.

## 5. Cloud Hypervisor implementation

One `cloud-hypervisor` process per VM with its own `--api-socket`, because the VM's lifetime must not be the agent's lifetime (D-0011) and because a per-VM socket is the unit the engine can rebuild after a restart (`docs/research/cloud-hypervisor.md`). The API is HTTP/JSON over a UNIX socket, with reads as `GET` and mutations as `PUT`; the engine does not need a Cloud Hypervisor SDK, only an HTTP client over a `UnixStream` (`docs/research/cloud-hypervisor.md`).

| Method | What the implementation does | Backend action |
| --- | --- | --- |
| `capabilities` | Reports the pinned release compiled into the node-agent build and the releases whose VMM order test passed (below), plus values observed at start-up | Runs the checked binary once for its version (below); no VM call |
| `probe_host` | Reads `/dev/kvm`, the kernel release and the CPU flags | No backend call |
| `ensure_network` | Creates the bridge, the gateway address and the forwarding rules; each VM gets a TAP on that bridge | No backend call; the TAP is created per VM with the network device |
| `prepare_image` | Pulls, verifies the digest, builds the rootfs, extracts `ImageConfig`, caches by digest (D-0061) | No backend call |
| `remove_image` | Deletes the cached rootfs directory | No backend call |
| `create` | Creates the runtime directory, starts the VMM with `--api-socket <dir>/api.sock`, then configures the VM: direct kernel boot of the Quoro-provided guest kernel, rootfs as a virtio-blk disk, the writable disk and any volumes and work disks as further virtio-blk disks, the network device on the node bridge, the console socket, the serial socket and the vsock device; records in `vmm.json` the VMM process and the release it launched (below); then connects to the serial socket | Process start, then `/vm.create` |
| `start` | Boots the configured VM | `/vm.boot` |
| `stop` graceful | Asks the guest to stop and waits for the process to exit; first resumes a VM a `PauseCopy` snapshot paused (section 3.8) | `/vm.resume` for such a VM, then `/vm.power-button`, then `/vm.info` until stopped |
| `stop` force | Shuts the VMM down; if the socket does not answer, kills the process and reaps it | `/vmm.shutdown`, else `SIGKILL` on the VMM pid |
| `delete` | Deletes the VM, shuts the VMM down, removes the runtime directory and the VM's own disks, keeps volumes | `/vm.delete`, then `/vmm.shutdown` |
| `inspect` | Reads the VM's configuration and state, and the release its VMM record names (below) | `/vm.info` |
| `list` | Scans the per-VM runtime directories, confirms each VMM is alive and reads its state | `/vm.info` per runtime directory |
| `events` | Watches the VMM process for exit and confirms the resulting state; a requested stop is distinguished from an unexpected exit | Process reaping, `/vm.info` |
| `output` | Connects to the VM's virtio-console socket and parses the init's tagged lines | Reads the `--console socket=` endpoint |
| `console` | Connects to the VM's serial socket and relays bytes in both directions, one client at a time | Reads and writes the `--serial socket=` endpoint (F-0034) |
| `prepare_layout` | Verifies the layout's blobs, builds the rootfs with the `prepare_image` pipeline, caches by manifest digest (section 3.17) | No backend call |
| `exec` | Connects to `<vm dir>/vsock.sock`, sends `CONNECT` for the exec port, then the open frame; relays frames to the PTY stream (section 3.18) | Hybrid vsock socket; no API call |
| `serial_output` | Reads the node-local ring, then follows the engine's own serial client (section 3.19) | No backend call; the client reads the `--serial socket=` endpoint |
| `counters` | Reads the VM's I/O and device counters and maps them to `VmCounters`; the exact field names of the backend's counter document are not fixed by `docs/research/cloud-hypervisor.md`, so the mapping is **Hypothesis** | `/vm.counters` |
| `snapshot_volume` | First the guest flush of `flush_volume`, bounded at 2 s, then `FICLONE` of the volume file, natively or in the XFS volume pool, or pause, sparse copy and resume (section 3.16, ADR 0156, ADR 0282) | The hybrid vsock socket and `/vm.info` for the flush; `/vm.pause` and `/vm.resume` for `PauseCopy` only; never `/vm.snapshot` (F-0035) |
| `delete_volume_snapshot` | Removes the snapshot file | No backend call |
| `volume_changed_ranges` | Reads both extent maps with `FS_IOC_FIEMAP` and compares their physical blocks (section 3.22, ADR 0172) | No backend call |
| `stage_volume` | `FICLONE` of the volume file or of a snapshot into a staging file, or a sparse empty file (section 3.22) | No backend call |
| `commit_staged_volume` | `fsync` of the staging file, `rename` over the volume file, `fsync` of the directory (section 3.22) | No backend call |
| `discard_staged_volume` | Removes the staging file | No backend call |
| `pause` | Freezes the VM's vCPUs (section 3.22) | `/vm.pause` |
| `resume` | Lets a paused VM run (section 3.22) | `/vm.resume` |
| `flush_volume` | Connects to `<vm dir>/vsock.sock` of the running VM that attaches the volume, sends `CONNECT` for the exec port, then one sync frame naming the volume's serial, and waits for `132` or `131` (sections 3.22 and 6, ADR 0282) | Hybrid vsock socket; `/vm.info` to skip a paused VM |
| `ensure_volume` | Creates the sparse file and runs `mkfs.ext4` on it (section 3.20) | No backend call |
| `import_volume` | Renames the restored file into place (section 3.20) | No backend call |
| `delete_volume` | Removes the volume file (section 3.20) | No backend call |
| `image_inventory` | Reads the inventory cached next to the image (section 3.21) | No backend call |
| `uninventoried_images` | Lists the cache entries with a `rootfs.ext4` and no inventory (section 3.21) | No backend call |
| `inventory_cached_image` | Mounts the image's `rootfs.ext4` read-only through a loop device (`ro,noload,nodev,nosuid,noexec`, type `ext4`) on a mount point of its own in the node agent's mount namespace, runs the inventory parsers on it, unmounts it and writes the inventory next to the image (section 3.21). A mount left behind by an interrupted agent is unmounted before the next one | No backend call |

Details that follow from the research file:

- **Direct kernel boot**, not firmware: `--kernel` with the Quoro-provided guest kernel and `--cmdline`. The firmware path adds a bootloader stage and is not needed for a Linux-only guest (`docs/research/cloud-hypervisor.md`).
- **One pinned release per node-agent build.** Cloud Hypervisor has no versioned v2 API to design against, and neither snapshots nor live migration are portable across versions (`docs/research/cloud-hypervisor.md`). The pin is reported in `capabilities` and in `engine_version` in `hello`. From M5b each build also carries the list of releases whose VMM order test passed (section 3.22), compiled in apart from `pins.txt`, whose lines its parsers read as six fields, and reported in `Capabilities::vmm_order_passed`. The engine launches only the binary it checked against the pin: it opens the binary once at its start, checks that open file against the pin, runs it for its version, and launches every VMM from that open file, not from whatever its path holds later, since an upgrade renames a new binary over the same path (`installer/install.sh`) while the old agent still runs. It checks the open file itself, not the path, and launches it with the installed path as its first argument (`argv[0]`), so process lists and the documented `pkill -f` and `pgrep -af` commands still find each VMM. A change of the pin is its own pull request, which carries the lab test's result and the list's change. Every VMM it starts records in its VM's `vmm.json` the release it launched, in `pins.txt`'s `version` form, and every `VmInfo` the engine returns reports it as `vmm_version`, with no further API call. A later write of the record for the same VMM process, such as `start` marking it booted, keeps the release the record names, `None` included. A VM started by an earlier build keeps its VMM, and that build's record, across an agent upgrade. A record with no release, written before M5b, reports `None`, which is on no list. So a release the build does not list as passed, an earlier one never tested, a newer one after a downgrade, or an unknown one, is not on the list, so the stop of section 3.22 applies.
- **Console is a socket, not a pty.** `--serial socket=<path>` and `--console socket=<path>` are the v54 forms; a socket serves one client at a time and accepts a new one after the current one disconnects (F-0034, `docs/research/cloud-hypervisor.md`). That is why `console` reports `Conflict` on a second reader.
- **M4 devices.** Every VM gets `--vsock cid=3,socket=<vm dir>/vsock.sock` and `--serial socket=<vm dir>/serial.sock`, with `--console file=` or `socket=` carrying the app's tagged output as before. The kernel command line keeps `loglevel=4`: a boot then writes about 600 bytes to the serial port, which is the guest's boot output the console shows, and a higher level costs boot time on the emulated UART (ADR 0116).
- **State files under the per-VM runtime directory** — spec, sockets, the VMM pid — so `list` can rediscover a running VM after an agent restart without restarting it (D-0011).

## 6. Guest contract with the minimal init

The minimal init is part of the engine workstream and is the only process Quoro puts inside the microVM. It is PID 1; the app runs under it (D-0008). It is Quoro's own small Rust init (D-0061), because the init is on the audit path and a large guest agent would be a black box.

**What the init receives.** A small machine-readable configuration, read before the app starts: a config disk or the kernel command line. The delivery choice is settled during M1 implementation, not baked into the rootfs (D-0061). The configuration carries:

| Item | Content |
| --- | --- |
| Entrypoint and arguments | The resolved command line of the app |
| Environment | Name and value pairs, already decrypted by the node agent |
| User | The user the app must run as |
| Working directory | The app's working directory |
| Network | Address, prefix length, gateway, interface name and the DNS servers of the node network (`NodeNetwork::dns`) |
| Console | Which console carries log lines |
| Exec port | The vsock port the init listens on for exec sessions (M4) and sync sessions (M5b) |
| Volumes | For each attached volume, its disk serial and mount path (M5a) |

**What the init must do.**

1. Mount `/proc`, `/sys` and `/dev`, and mount the writable disk at the app's data location.
2. Configure the network interface with the address, gateway and DNS given in the configuration.
3. Start the app as **PID 2** under the configured user, in the configured working directory, with the configured environment.
4. Forward the app's stdout and stderr to the virtio-console **with a stream tag per line**, so the node agent can tell the two apart without parsing app output. This is the source of `OutputLine` and therefore of local app logs (D-0014).
5. Reap orphaned children so no zombie accumulates, and forward signals to the app where they matter.
6. On the power-button signal, send `SIGTERM` to the app, wait for it to exit or for the timeout to pass, then power the guest off.
7. Report the app's exit code so the engine can surface it as `exit_code` and as `EngineEvent::Exited`.
8. From M4, mount `cgroup2` on `/sys/fs/cgroup`, which BuildKit needs and app images may expect (ADR 0118).
9. From M4, listen for exec sessions (below), and unmount or remount read-only every writable disk before powering off, so a work disk is clean for the next VM that attaches it, without journal recovery (ADR 0118).
10. From M5a, after the switch into the app's root and before the app starts, find each volume's disk by its serial and mount it at its mount path inside the app's root, creating the directory if it is missing. A volume that cannot be found or mounted is a failed boot. Volumes are unmounted with the other writable disks before power-off.
11. From M5b, answer a sync session (below) by running `syncfs` on the mount of each volume it names, so a host-side snapshot holds what the app wrote (ADR 0282).

**Configuration version 2 (M4).** The init's configuration grammar refuses unknown keys, so M4 adds one: the header becomes `quoro-init-config v2`, and one line `exec_port=<u32>` follows `stop_timeout_ms` (`crates/init/README.md`). The engine writes `exec_port=1024`. The init still accepts a `v1` configuration and then starts no exec listener. The engine and the init ship together: the initramfs is rebuilt from the installed init, so a VM gets the new init at its next start, and a VM that kept running across the upgrade answers `exec` with `Conflict` until it restarts.

**Configuration version 3 (M5a).** The header becomes `quoro-init-config v3`, and zero or more lines `volume=<serial>:<mount_path>` follow `exec_port`, one per attached volume in the order of `VmSpec::volumes` (`crates/init/README.md`). `serial` matches `quoro-vol-[0-9]+`; `mount_path` is absolute, at most 255 bytes, holds no `:`, no empty, `.` or `..` segment and no trailing `/`, and is not `/` nor under `/proc`, `/sys` or `/dev`; any other line is a configuration error. The engine writes `v3` for every VM from M5a, with no `volume=` line for a VM without volumes. The init still accepts `v1` and `v2`. A VM started before its node got the M5a init boots as before; a VM with a volume needs the M5a init, which it gets at its next start.

**Exec sessions (M4).** After the switch into the app's root and before the app starts, the init listens on vsock port `exec_port` for the VM's lifetime; vsock delivers only connections from the host to a guest listener (ADR 0113). One connection is one session. Every frame is `[u8 type][u32 length, big-endian][payload]`, with a payload of at most 64 KiB; a larger or unknown frame ends the session. Host to guest: `1` open (JSON `{"argv":[...],"user":null|"...","rows":R,"cols":C}`, exactly once, first), `2` input bytes, `3` resize (`u16 rows`, `u16 cols`, big-endian), `4` close. Guest to host: `129` output bytes, `130` exit (`i32` big-endian, `128 + signal` when a signal ended the program), `131` error (UTF-8 message, then the guest closes). Per session the init allocates a PTY and starts the program as a session leader with the PTY as its controlling terminal, the app's environment with `TERM=xterm-256color`, the app's working directory, and the app's user unless the open names another. Because the workspace forbids `unsafe`, the init re-executes itself (`/proc/self/exe` with a helper argument) as a small helper that calls `setsid`, takes the PTY as its controlling terminal on stdin, stdout and stderr, and replaces itself with the program. A close, or the host dropping the connection, sends `SIGHUP` to the session's process group. The exec listener never writes to the app's stdout or to the log console.

**Sync sessions (M5b, ADR 0282).** ADR 0282 extends the framing of ADR 0126 with one frame each way, for the engine's flush of a volume before a snapshot (section 3.16).

- **Host to guest: `5` sync,** valid only as a session's first frame, in place of `1` open. Its payload is the JSON object `{"serials":["quoro-vol-0", ...]}` with no other key: one to 64 serials, each that of a volume in the configuration and each named once. The engine names one volume per session. The host sends nothing after it, and the init reads no further frame of that session. A `5` anywhere but first is an unknown frame and ends the session.
- **The init's work.** For each serial, in order, it opens that volume's mount path at that moment and calls `syncfs` on it.
- **Guest to host: `132` synced,** with an empty payload, once every named volume was synced. The init then closes the session.
- **Errors.** A malformed payload, an empty list, an unknown serial, a mount it cannot open, or a `syncfs` that fails is answered with `131` error and the reason, then a close.
- **What a sync session does not do.** It starts no program, allocates no PTY, and writes nothing to the app's stdout or to the log console. It writes a line to the serial console only when it fails, and only for a volume's first failure or a change of its reason, so a sync every few seconds does not push the boot output out of the serial ring (section 3.19).
- **Concurrency.** Each session is served on its own, as an exec session is, and counts toward the init's 16 concurrent sessions (`crates/init/README.md`). A sync refused because every slot is taken gets a `131`, which the engine reports as `Failed`. While an earlier `syncfs` of a named volume is still running, the init waits up to 2 s for it, then answers `131` `sync: an earlier syncfs of quoro-vol-N has not returned`, with that volume's serial, and closes (ADR 0291). The engine reports that `131` as `Failed`, like any `131` other than an older init's, but a snapshot's own 2 s bound starts first and normally ends the flush with `TimedOut` before it arrives. A host that stops waiting drops the connection, and the guest's `syncfs` may still finish later. When a snapshot's flush gives up after its 2 s bound, the snapshot goes ahead crash-consistent, with `guest_flush` `TimedOut`, which is `false` in the backup entry of `status_report` (`node-agent-protocol.md` section 5.3, ADR 0291).
- **Older inits and upgrades.** An init older than ADR 0282 accepts only `open` as a first frame: it answers `131` with the message `the first frame must be open, got type 5` and closes. The engine matches that message exactly and reports `Unsupported` (section 3.16). Only that wording, of the inits from M4 until ADR 0282, is part of the contract; a newer init's error messages are not, except the busy-volume refusal `sync: an earlier syncfs of quoro-vol-N has not returned` above, which the engine still does not match. The configuration does not change: the M5b init reads the same `v3` configuration and serves sync sessions on `exec_port`. A VM gets the new init at its next start, as with every init change.

While the app is running, the init must not write to the app's stdout, so a log line's stream tag is never ambiguous. On a failed boot — no configuration, a missing entrypoint, an unmountable disk — the init writes a diagnostic to the serial console and powers off with a non-zero code, so the failure surfaces as a `deployment.failed` event in the `boot` phase rather than as a silent hang.

## 7. `FakeEngine`

Tests must run on machines without KVM, including CI and the laptops of people who only work on the panel. `FakeEngine` implements the same trait with no hypervisor:

- In memory: VMs are records, `create`/`start`/`stop`/`delete` change their state, `list` returns exactly what was created since the instance started.
- Deterministic: no timers, no threads, no wall-clock dependence beyond explicit calls. Boots complete when `start` returns.
- Bounds (M5b): a test can make `start` or a forced stop overrun its bound, so the call returns `Timeout` and the VM stays running until the test lets the stop finish. A test can likewise make `pause` or `resume` return `Timeout`, and set the paused flag itself, so the node agent's handling of a state it does not know is testable.
- Injectable: a test can make a VM crash after N seconds, make `start` return `ResourceExhausted`, make `prepare_image` return `Image`, and make the console echo, so every error path and every event the node agent must map is reachable.
- Streams: `events`, `output`, `console` and `serial_output` are real streams over in-memory channels, so the node agent's stream handling is exercised, not stubbed.
- M4 methods: `prepare_layout` caches a fake prepared image by manifest digest; `exec` returns an in-memory session that echoes input and can be told to exit with a code; `serial_output` replays and follows an injectable byte buffer. `Capabilities::exec` is `true`.
- Inventory methods: `image_inventory` returns an injectable inventory for a prepared image; a test can mark an image as prepared before M5a, which `uninventoried_images` then lists and `image_inventory` reports `NotFound` until `inventory_cached_image` builds an injectable inventory for it, or fails with an injectable error.
- Volume methods: `ensure_volume`, `import_volume` and `delete_volume` keep volume records in memory with their size; `snapshot_volume` and `delete_volume_snapshot` succeed and record a snapshot descriptor, so the M5 chunk-engine code is testable before a real primitive exists. `Capabilities::volume_snapshot_method` is injectable and defaults to `Reflink`; with `PauseCopy` the fake records a pause and resume of the VM that attaches the volume.
- Warm methods (M5b): `volume_changed_ranges` returns injectable ranges, by default every range of the volume with `full: true`; `stage_volume`, `commit_staged_volume` and `discard_staged_volume` keep staging records and apply the volume rules of section 3.22, including the `Conflict` of a VM that attaches the volume; `pause` and `resume` flip a paused flag a test can read, and a test can make either fail. `VmInfo.vmm_version` reports a release that `Capabilities::vmm_order_passed` holds by default while a VM has a VMM, as the real engine's does: from `create` until a stop, delete or crash, and again from each `start`. A test can set it per VM, `None` included, and set `Capabilities::vmm_order_passed`, so the stop of section 3.22 is testable; tests compare `vmm_version` with that list, not with `Capabilities::version`. With a volume directory set, the fake applies the snapshot-file rule of section 3.22 to the `location` it is given, before it looks the snapshot up, and names its own snapshots so that they pass it.
- Guest flush (M5b, ADR 0282): `flush_volume` returns an injectable `GuestFlush` or error, and `snapshot_volume` records the injected outcome in `guest_flush`; an injected error other than `NotFound` makes it record `Failed`. The default is `Flushed` when a running VM that is not paused attaches the volume, `Paused` when that VM is paused, and `NoRunningVm` otherwise. So the node agent's report of every outcome, the false ones included, is testable.

The node agent runs its protocol tests against `FakeEngine`: the M0 lease tests — revoking a `local` lease and expiring a test-only short `expiring` lease — need no VM at all beyond a fake (`node-agent-protocol.md`).

## 8. Not in this trait

- **Live migration.** Out of scope (D-0034). Cloud Hypervisor supports it over UNIX sockets and TCP with mutual TLS (`docs/research/cloud-hypervisor.md`), and Quoro does not expose it.
- **Graphical consoles.** Cloud Hypervisor has no VNC, SPICE, GPU or framebuffer device (F-0034). Consoles are text-only (D-0056): the baseline stack ships xterm.js over the serial or virtio-console and no noVNC, and the trait exposes text consoles only. A graphical console would need an Engine with a display device — a guest-side display server or GPU passthrough — and would be an engine of its own.
- **GPU and PCI passthrough.** No app contract needs it; adding it would widen the device model and the security surface.
- **VM memory snapshots.** The backend can do them, but they contain RAM and device state and no disk contents (F-0035), and Quoro's recovery model is a lease move plus a volume primitive, not a frozen VM.
- **Multi-host scheduling, placement, leases and identity.** Those live in the node agent and the control plane (D-0019, `node-agent-protocol.md`); the engine only knows the VMs of the node it runs on.

## 9. Milestones

| Milestone | Engine work |
| --- | --- |
| M0 | The trait, `FakeEngine`, and the spike that boots a microVM from an OCI image (D-0033) |
| M1 | `create`, `start`, `stop`, `delete`, `list`, `inspect`, `prepare_image`, `ensure_network` (D-0033) |
| M2 | `output` feeds live app logs (D-0014, D-0033); isolation groups on `NetAttach` and guest DNS on `NodeNetwork` (D-0084) |
| M4 | `counters` for metrics (D-0033, ADR 0014); `serial_output` and `VmSpec::serial` for the read-only console (ADR 0116); `exec` and `VmSpec::vsock` for the web terminal, with init configuration v2 (ADR 0113); `prepare_layout`, `VmSpec::work_disks` and `NetAttach::egress_only` for builds (ADR 0117, ADR 0118). `console` is superseded |
| M5a | `image_inventory`, built by `prepare_image` and `prepare_layout` (ADR 0150, ADR 0151), and `uninventoried_images` and `inventory_cached_image` for images cached before M5a; `ensure_volume`, `import_volume`, `delete_volume` and `VolumeAttach::serial` for local volumes, with init configuration v3 (ADR 0145); `snapshot_volume` and `delete_volume_snapshot` with the `Reflink` or `PauseCopy` method probed per node (ADR 0146) |
| M5b | `volume_changed_ranges` between two reflink snapshots of a warm volume, found by comparing their extent maps (ADR 0172, D-0023); `stage_volume`, `commit_staged_volume` and `discard_staged_volume` for the target's copy (ADR 0173); `pause` and `resume` for a planned switchover (ADR 0177); several snapshots per volume (section 3.16); a caller-supplied bound on `start` (ADR 0242) and on both stop modes, with forced stops that finish after a dropped call (ADR 0243, #434); a guest flush through the init before every volume snapshot, `flush_volume` and `VolumeSnapshot::guest_flush`, with the sync session of section 6 (ADR 0282, #400); section 6 also names the init's refusal while an earlier `syncfs` of the volume is still running, and the host's rule after its 2 s bound (ADR 0291) |
