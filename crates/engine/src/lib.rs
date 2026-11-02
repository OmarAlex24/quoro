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

use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::path::PathBuf;
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
    pub console: ConsoleMode,
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

/// A volume attached to a VM at a mount path inside the guest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeAttach {
    pub volume_id: VolumeId,
    pub mount_path: String,
}

/// The VM's network attachment on the node bridge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetAttach {
    pub ip: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub mac: MacAddr,
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
    Serial { socket: PathBuf },
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
}

/// The node network a VM is attached to. Section 3.3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeNetwork {
    pub bridge: String,
    pub subnet: String, // CIDR, for example "10.42.1.0/24"
    pub gateway: Ipv4Addr,
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

    // Volumes (M5; default implementations return Unsupported)
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

pub use fake::{FakeEngine, FakeOp};
