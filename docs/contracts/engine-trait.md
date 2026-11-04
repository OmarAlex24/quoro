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
```

The derives, `MacAddr`, the blanket `ConsoleIo` implementation, `Display` and `Error` for `EngineError` and `VmSpec::canonical` (section 3.6) are contract items (D-0071), added additively during M0. `events`, `output`, `console` and `serial_output` are not `async` because they only build a stream; the first item is produced by the stream itself.

M4 adds, additively: `VmSpec::serial`, `VmSpec::vsock` and `VmSpec::work_disks` with `SerialMode`, `VsockAttach` and `WorkDisk`; `NetAttach::egress_only`; `Capabilities::exec`; `OciLayout`, `ByteStream`, `ExecRequest` and `ExecSession`; and the methods `prepare_layout`, `exec` and `serial_output`, whose default implementations return `Unsupported`, so a backend that does not offer them still compiles. Code that builds a `VmSpec`, `NetAttach` or `Capabilities` literal sets the new fields to `SerialMode::File` (the M1-M3 serial log), `None`, an empty list, `false` and `false`, which keeps the M1-M3 behaviour. `VmSpec::canonical` keeps `work_disks` in order: their serials are unique, so the order carries no meaning, but the node agent always builds them in the same order.

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

Brings the node network to the requested `NodeNetwork`: the bridge exists, carries the gateway address, and forwards app traffic. Quoro owns the networking layer — bridges, TAP devices, addresses, firewall rules — rather than delegating it (D-0007). Precondition: `subnet` and `gateway` are consistent. Postcondition: a VM with an address inside `subnet` can reach the gateway. Idempotent: calling it twice with the same values changes nothing. An existing bridge with a different subnet is `Conflict`, never a silent reconfiguration.

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
- Precondition: `rootfs` is the digest of a prepared image, `net` is inside a network already ensured by `ensure_network`, and `volumes` are attached to this node.
- `serial: Socket` makes the engine connect to the serial socket as soon as it exists, before boot, so the ring of section 3.19 holds the whole boot. `vsock` attaches the device; its socket lives in the VM's runtime directory, which only root can enter.
- Each `WorkDisk` is attached as a writable virtio-blk disk with its serial. A missing file is created as an empty ext4 filesystem of `size_gb`; an existing file is attached as it is. Two VMs that name the same work-disk path at once is `Conflict`. The engine never removes a work disk (section 3.9).

### 3.7 `start`

Boots a created or stopped VM. Postcondition on success: `state` is `Running` and `EngineEvent::Started` has been or will be emitted. A boot is fast enough that a stopped warm copy is a practical failover target (D-0008, D-0020); the boot-time figure is still a marketing claim, not a measured number (`docs/research/cloud-hypervisor.md`).

- Starting a running VM returns `Ok` and does not reboot it. Restarts are expressed as a spec change by the node agent (`restart_generation` in `node-agent-protocol.md`), so the engine never has to guess intent.
- Starting an unknown id is `NotFound`. Starting a VM whose kernel or rootfs is missing is `Image` or `Io`.

### 3.8 `stop`

Stops a VM.

- `Graceful { timeout }` asks the guest to exit — the power-button path, which the minimal init turns into `SIGTERM` to the app (section 7) — and waits at most `timeout`. On expiry it falls back to forced stop. The call returns `Ok` only when the VM is stopped.
- `Force` stops without cooperation from the guest. If the VMM does not confirm the stop (its API does not answer, or the process does not exit within the call), the backend escalates to `SIGKILL` on the VMM process and reaps it. `Force` returns `Ok` only when the VMM process is gone; an error from `Force` means the VM may still be running, and the node agent must treat it that way (`node-agent-protocol.md` section 8.5, D-0074).
- Stopping a stopped VM returns `Ok`. Stopping an unknown id is `NotFound`.
- Postcondition: `state` is `Stopped`, `exit_code` reports the app's code when the guest reported one, and `EngineEvent::Exited` has been or will be emitted.

### 3.9 `delete`

Removes the VM and its own disks. Missing id returns `Ok`, so a retried delete converges. Volumes are never removed by `delete`: they belong to the app, not to the instance, and are deleted through the API with `app:delete` (D-0033, `README.md`). Postcondition: the VMM process is gone, the root and data disks are removed, and `inspect` returns `NotFound`. Work disks are kept too: the node agent owns them and removes them with the app (ADR 0118). The serial ring of section 3.19 is removed with the runtime directory. Precondition: the VM is stopped; a call on a running VM is `Conflict`.

### 3.10 `inspect`

Returns one VM's `VmInfo`. Unknown id is `NotFound`. This is the only method that observes lifecycle state; it never changes it. It is the engine-level source of the `observed_status` a node reports (D-0032 detect step), and it never reports a state outside the enum in `README.md`.

### 3.11 `list`

Returns every VM the engine knows about on this node, including VMs created by a previous agent process. This is how the node agent rediscovers its world after a restart: VMMs are separate processes and keep running, so `list` must find them from the runtime directories on disk and the backend's API, not from in-memory state (D-0011). A VM whose runtime directory exists but whose process is gone is reported as `Crashed` with the last known exit code, or as `Stopped` when the exit followed a requested stop. `list` never starts, restarts or deletes anything.

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

The point-in-time disk primitive the chunk engine needs (D-0023). Both are `Unsupported` in the default implementation, and only the M5 code path calls them.

- The primitive for M5a is decided (D-0062): reflink snapshots with a full content-defined chunk scan for cold backups. Before M5b, a spike comparing lvm-thin and btrfs chooses the primitive for warm volumes.
- They must **not** be implemented with Cloud Hypervisor VM snapshots. A CH snapshot is a paused VM's `config.json`, `memory-ranges` and `state.json`; it contains guest RAM and device state but no disk contents, and CH exposes no dirty bitmap (F-0035). A volume snapshot is about the disk.
- If implemented with a filesystem primitive, the call takes no VMM lock: it snapshots the volume of a stopped or running VM as the chosen primitive allows, and the chunk engine handles crash consistency (D-0023).

### 3.17 `prepare_layout`

Turns a local OCI image layout, as a builder VM wrote it on its work disk, into a prepared rootfs, the way `prepare_image` does for a pulled image (ADR 0117, ADR 0118). Added in M4.

- Precondition: `path` is a readable OCI image layout whose index names `manifest_digest`.
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

### 3.19 `serial_output`

A read-only stream of a VM's serial output: first up to `tail_bytes` of the newest bytes the node kept (all of it when `None`), then live output until the VM is deleted or the caller drops the stream (ADR 0116). Added in M4.

- With `serial: Socket`, the engine is the socket's only client, from `create` on. It appends what it reads to a node-local ring in the VM's runtime directory: two segments of 512 KiB, the older one dropped when the newer one fills, so the ring holds at most 1 MiB and survives VMM restarts, which keeps the previous boot's output after a crash loop. While the node agent restarts, the VMM keeps up to 1 MiB of output and replays it when the engine reconnects.
- Any number of streams may be open on one VM; each one reads the ring and then follows, so viewers never connect to the socket.
- An unknown id is `NotFound`. A VM whose `serial` is not `Socket` is `Conflict`.
- Input is never written to the serial port: nothing in the guest reads it.

## 4. Concurrency

- Operations on one `VmId` are serialized by the caller. The node agent holds one lock per VM, so it never issues `start` and `delete` for the same id concurrently. The engine does not promise ordering for concurrent calls on one id and may return `Conflict`.
- The engine must be safe to call concurrently for **different** VMs, on many VMs at once. This is what lets one node run several apps and replace them independently.
- `capabilities`, `probe_host`, `ensure_network`, `prepare_image`, `remove_image` and `list` are safe concurrently with any VM operation. `ensure_network` and `prepare_image` are internally serialized by the implementation where the underlying operation is not reentrant.
- No call blocks the runtime for longer than an I/O wait: image preparation and boot are awaited, never run on the reactor thread.

## 5. Cloud Hypervisor implementation

One `cloud-hypervisor` process per VM with its own `--api-socket`, because the VM's lifetime must not be the agent's lifetime (D-0011) and because a per-VM socket is the unit the engine can rebuild after a restart (`docs/research/cloud-hypervisor.md`). The API is HTTP/JSON over a UNIX socket, with reads as `GET` and mutations as `PUT`; the engine does not need a Cloud Hypervisor SDK, only an HTTP client over a `UnixStream` (`docs/research/cloud-hypervisor.md`).

| Method | What the implementation does | Backend action |
| --- | --- | --- |
| `capabilities` | Reports the pinned release compiled into the node-agent build, plus values observed at start-up | Runs the binary once for its version; no VM call |
| `probe_host` | Reads `/dev/kvm`, the kernel release and the CPU flags | No backend call |
| `ensure_network` | Creates the bridge, the gateway address and the forwarding rules; each VM gets a TAP on that bridge | No backend call; the TAP is created per VM with the network device |
| `prepare_image` | Pulls, verifies the digest, builds the rootfs, extracts `ImageConfig`, caches by digest (D-0061) | No backend call |
| `remove_image` | Deletes the cached rootfs directory | No backend call |
| `create` | Creates the runtime directory, starts the VMM with `--api-socket <dir>/api.sock`, then configures the VM: direct kernel boot of the Quoro-provided guest kernel, rootfs as a virtio-blk disk, the writable disk and any volumes and work disks as further virtio-blk disks, the network device on the node bridge, the console socket, the serial socket and the vsock device; then connects to the serial socket | Process start, then `/vm.create` |
| `start` | Boots the configured VM | `/vm.boot` |
| `stop` graceful | Asks the guest to stop and waits for the process to exit | `/vm.power-button`, then `/vm.info` until stopped |
| `stop` force | Shuts the VMM down; if the socket does not answer, kills the process and reaps it | `/vmm.shutdown`, else `SIGKILL` on the VMM pid |
| `delete` | Deletes the VM, shuts the VMM down, removes the runtime directory and the VM's own disks, keeps volumes | `/vm.delete`, then `/vmm.shutdown` |
| `inspect` | Reads the VM's configuration and state | `/vm.info` |
| `list` | Scans the per-VM runtime directories, confirms each VMM is alive and reads its state | `/vmm.ping` and `/vm.info` per runtime directory |
| `events` | Watches the VMM process for exit and confirms the resulting state; a requested stop is distinguished from an unexpected exit | Process reaping, `/vm.info` |
| `output` | Connects to the VM's virtio-console socket and parses the init's tagged lines | Reads the `--console socket=` endpoint |
| `console` | Connects to the VM's serial socket and relays bytes in both directions, one client at a time | Reads and writes the `--serial socket=` endpoint (F-0034) |
| `prepare_layout` | Verifies the layout's blobs, builds the rootfs with the `prepare_image` pipeline, caches by manifest digest (section 3.17) | No backend call |
| `exec` | Connects to `<vm dir>/vsock.sock`, sends `CONNECT` for the exec port, then the open frame; relays frames to the PTY stream (section 3.18) | Hybrid vsock socket; no API call |
| `serial_output` | Reads the node-local ring, then follows the engine's own serial client (section 3.19) | No backend call; the client reads the `--serial socket=` endpoint |
| `counters` | Reads the VM's I/O and device counters and maps them to `VmCounters`; the exact field names of the backend's counter document are not fixed by `docs/research/cloud-hypervisor.md`, so the mapping is **Hypothesis** | `/vm.counters` |
| `snapshot_volume` | Default: `Unsupported`. When implemented: a filesystem-level snapshot of the volume (reflink snapshot for M5a, D-0062) | No `/vm.snapshot` call (F-0035) |
| `delete_volume_snapshot` | Default: `Unsupported`. When implemented: removes the filesystem-level snapshot | No backend call |

Details that follow from the research file:

- **Direct kernel boot**, not firmware: `--kernel` with the Quoro-provided guest kernel and `--cmdline`. The firmware path adds a bootloader stage and is not needed for a Linux-only guest (`docs/research/cloud-hypervisor.md`).
- **One pinned release per node-agent build.** Cloud Hypervisor has no versioned v2 API to design against, and neither snapshots nor live migration are portable across versions (`docs/research/cloud-hypervisor.md`). The pin is reported in `capabilities` and in `engine_version` in `hello`.
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
| Exec port | The vsock port the init listens on for exec sessions (M4) |

**What the init must do.**

1. Mount `/proc`, `/sys` and `/dev`, and mount the writable disk at the app's data location.
2. Configure the network interface with the address, gateway and DNS given in the configuration.
3. Start the app as **PID 2** under the configured user, in the configured working directory, with the configured environment.
4. Forward the app's stdout and stderr to the virtio-console **with a stream tag per line**, so the node agent can tell the two apart without parsing app output. This is the source of `OutputLine` and therefore of local app logs (D-0014).
5. Reap orphaned children so no zombie accumulates, and forward signals to the app where they matter.
6. On the power-button signal, send `SIGTERM` to the app, wait for it to exit or for the timeout to pass, then power the guest off.
7. Report the app's exit code so the engine can surface it as `exit_code` and as `EngineEvent::Exited`.
8. From M4, mount `cgroup2` on `/sys/fs/cgroup`, which BuildKit needs and app images may expect (ADR 0118).
9. From M4, listen for exec sessions (below), and unmount or remount read-only every writable disk before powering off, so the node reads a work disk without journal recovery (ADR 0118).

**Configuration version 2 (M4).** The init's configuration grammar refuses unknown keys, so M4 adds one: the header becomes `quoro-init-config v2`, and one line `exec_port=<u32>` follows `stop_timeout_ms` (`crates/init/README.md`). The engine writes `exec_port=1024`. The init still accepts a `v1` configuration and then starts no exec listener. The engine and the init ship together: the initramfs is rebuilt from the installed init, so a VM gets the new init at its next start, and a VM that kept running across the upgrade answers `exec` with `Conflict` until it restarts.

**Exec sessions (M4).** After the switch into the app's root and before the app starts, the init listens on vsock port `exec_port` for the VM's lifetime; vsock delivers only connections from the host to a guest listener (ADR 0113). One connection is one session. Every frame is `[u8 type][u32 length, big-endian][payload]`, with a payload of at most 64 KiB; a larger or unknown frame ends the session. Host to guest: `1` open (JSON `{"argv":[...],"user":null|"...","rows":R,"cols":C}`, exactly once, first), `2` input bytes, `3` resize (`u16 rows`, `u16 cols`, big-endian), `4` close. Guest to host: `129` output bytes, `130` exit (`i32` big-endian, `128 + signal` when a signal ended the program), `131` error (UTF-8 message, then the guest closes). Per session the init allocates a PTY and starts the program as a session leader with the PTY as its controlling terminal, the app's environment with `TERM=xterm-256color`, the app's working directory, and the app's user unless the open names another. Because the workspace forbids `unsafe`, the init re-executes itself (`/proc/self/exe` with a helper argument) as a small helper that calls `setsid`, takes the PTY as its controlling terminal on stdin, stdout and stderr, and replaces itself with the program. A close, or the host dropping the connection, sends `SIGHUP` to the session's process group. The exec listener never writes to the app's stdout or to the log console.

While the app is running, the init must not write to the app's stdout, so a log line's stream tag is never ambiguous. On a failed boot — no configuration, a missing entrypoint, an unmountable disk — the init writes a diagnostic to the serial console and powers off with a non-zero code, so the failure surfaces as a `deployment.failed` event in the `boot` phase rather than as a silent hang.

## 7. `FakeEngine`

Tests must run on machines without KVM, including CI and the laptops of people who only work on the panel. `FakeEngine` implements the same trait with no hypervisor:

- In memory: VMs are records, `create`/`start`/`stop`/`delete` change their state, `list` returns exactly what was created since the instance started.
- Deterministic: no timers, no threads, no wall-clock dependence beyond explicit calls. Boots complete when `start` returns.
- Injectable: a test can make a VM crash after N seconds, make `start` return `ResourceExhausted`, make `prepare_image` return `Image`, and make the console echo, so every error path and every event the node agent must map is reachable.
- Streams: `events`, `output`, `console` and `serial_output` are real streams over in-memory channels, so the node agent's stream handling is exercised, not stubbed.
- M4 methods: `prepare_layout` caches a fake prepared image by manifest digest; `exec` returns an in-memory session that echoes input and can be told to exit with a code; `serial_output` replays and follows an injectable byte buffer. `Capabilities::exec` is `true`.
- Volume methods: `snapshot_volume` and `delete_volume_snapshot` succeed and record a snapshot descriptor, so the M5 chunk-engine code is testable before a real primitive exists.

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
| M5a/M5b | `snapshot_volume` and `delete_volume_snapshot`: reflink snapshots for M5a, the primitive chosen by the lvm-thin vs btrfs spike for M5b (D-0023, D-0062) |
