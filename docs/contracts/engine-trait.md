# Engine trait

Status: M0 draft for review. Contracts freeze at the end of M0 (D-0036); after the freeze a change to this file is a standalone contract pull request, never part of feature work.

This file fixes the boundary behind which a virtual machine monitor sits. Cloud Hypervisor with KVM is the first implementation (D-0007), and the boundary exists so that other backends — Firecracker, a container runtime — can be added later without touching the rest of the system (D-0007, D-0009). The MVP requires a host with KVM; a container fallback is a later implementation of the same trait, not a mode of the first one (D-0009).

The node agent is the only caller. Nothing else in Quoro — not the API, the panel, MCP or the CLI — talks to the engine directly; they change desired state, and the node agent turns that into engine calls (`node-agent-protocol.md`). The engine therefore has no authentication, no tenancy and no policy of its own: it does what the node agent asks on the node it runs on, and the node agent is the component that checks leases, tokens and desired state first.

This file is a contract about behaviour, not about file layout. Names of modules, crates and private helpers may change; the types, method names, semantics and error variants below may not, except by a standalone contract pull request.

## 1. Shape of the boundary

| Property | Choice | Why |
| --- | --- | --- |
| Language | Rust, async, `tokio` runtime | The node agent is Rust and async; the engine runs in its process |
| Dispatch | One trait, implemented per backend, selected by an enum at start-up | No `dyn` requirement, so `async fn` in traits is usable |
| Ownership | One engine instance per node, shared by the node agent as `Arc` | Serialization of per-VM operations happens above the engine |
| Process model | One VMM process per VM, outside the agent process | A VMM survives an agent restart, so an upgrade does not restart apps (D-0011) |
| State | The engine owns node-local files; the cluster state is the node agent's | Desired state is signed and stored by the agent, not by the engine (D-0011) |
| Telemetry | The engine exposes streams and counters; it stores no logs | Logs and metrics stay on the node (D-0014) |

**Recommendation, not a frozen choice:** the trait uses `async fn` in traits instead of `#[async_trait]`, on the Rust 2024 edition. **Hypothesis:** `async fn` in traits is stable on that edition and is not object-safe, which is why the node agent selects a backend through an enum (`enum Backend { CloudHypervisor(ChEngine), Fake(FakeEngine) }`) rather than `Box<dyn Engine>`. If the M0 spike finds a reason to need `dyn`, the change is mechanical: return boxed futures, keep every signature otherwise identical.

## 2. The trait

```rust
// Rust 2024 edition. Recommendation pending the M0 spike.
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
```

```rust
/// An image as requested, and the digest it resolved to.
pub struct ImageRef {
    pub reference: String,
    pub digest: ImageDigest,
}

/// A rootfs that is ready to boot. Cached by digest.
pub struct PreparedImage {
    pub digest: ImageDigest,
    pub rootfs_path: PathBuf,
    pub size_bytes: u64,
    pub config: ImageConfig,
}

/// What the image says about how to run it, read from its OCI config.
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

pub struct VolumeAttach {
    pub volume_id: VolumeId,
    pub mount_path: String,
}

pub struct NetAttach {
    pub ip: std::net::Ipv4Addr,
    pub gateway: std::net::Ipv4Addr,
    pub mac: MacAddr,
}

/// The app's command line and identity, as the minimal init must apply it.
pub struct InitConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub user: Option<String>,
    pub workdir: Option<String>,
}

pub enum ConsoleMode {
    /// No console device.
    Off,
    /// virtio-console to this UNIX socket; carries tagged stdout/stderr lines.
    VirtioConsole { socket: PathBuf },
    /// Serial console to this UNIX socket; interactive text, one client at a time.
    Serial { socket: PathBuf },
}
```

```rust
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

pub struct VmInfo {
    pub id: VmId,
    pub state: VmState,
    /// Present while a VMM process exists for this VM.
    pub pid: Option<u32>,
    pub started_at: Option<SystemTime>,
    pub exit_code: Option<i32>,
}

pub enum EngineEvent {
    Started { id: VmId },
    Exited { id: VmId, code: i32 },
    Crashed { id: VmId, reason: String },
}

pub struct OutputLine {
    pub stream: OutputStream,
    pub time: SystemTime,
    pub line: String,
}

pub enum OutputStream {
    Stdout,
    Stderr,
}

pub struct VmCounters {
    pub cpu_time_ms: u64,
    pub memory_rss_bytes: u64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
}

pub struct Capabilities {
    pub engine: String,          // "cloud-hypervisor"
    pub version: String,         // the pinned release
    pub kvm: bool,
    pub text_console: bool,      // true
    pub graphical_console: bool, // false
    pub vm_snapshot: bool,
    pub volume_snapshot: bool,
}

pub struct NodeNetwork {
    pub bridge: String,
    pub subnet: String,  // CIDR, for example "10.42.1.0/24"
    pub gateway: std::net::Ipv4Addr,
}

pub struct VolumeSnapshot {
    pub volume_id: VolumeId,
    pub snapshot_id: String,
    pub created_at: SystemTime,
    pub size_bytes: u64,
    /// Node-local location of the snapshot; not part of any API.
    pub location: PathBuf,
}

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
```

```rust
pub type EventStream = Pin<Box<dyn Stream<Item = EngineEvent> + Send>>;
pub type LineStream = Pin<Box<dyn Stream<Item = OutputLine> + Send>>;

/// Bidirectional byte stream on a text console (serial or virtio-console).
pub trait ConsoleIo: AsyncRead + AsyncWrite + Send + Unpin {}
pub type ConsoleStream = Pin<Box<dyn ConsoleIo>>;

pub trait Engine: Send + Sync {
    // Host and node preparation
    async fn capabilities(&self) -> Capabilities;
    async fn probe_host(&self) -> Result<HostProbe, EngineError>;
    async fn ensure_network(&self, net: &NodeNetwork) -> Result<(), EngineError>;

    // Images
    async fn prepare_image(&self, image: &ImageRef) -> Result<PreparedImage, EngineError>;
    async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError>;

    // Lifecycle
    async fn create(&self, spec: &VmSpec) -> Result<VmInfo, EngineError>;
    async fn start(&self, id: &VmId) -> Result<VmInfo, EngineError>;
    async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError>;
    async fn delete(&self, id: &VmId) -> Result<(), EngineError>;
    async fn inspect(&self, id: &VmId) -> Result<VmInfo, EngineError>;
    async fn list(&self) -> Result<Vec<VmInfo>, EngineError>;

    // Observation
    fn events(&self) -> EventStream;
    fn output(&self, id: &VmId) -> Result<LineStream, EngineError>;
    fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError>;
    async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError>;

    // Volumes (M5; default implementations return Unsupported)
    async fn snapshot_volume(&self, id: &VolumeId) -> Result<VolumeSnapshot, EngineError>;
    async fn delete_volume_snapshot(&self, snap: &VolumeSnapshot) -> Result<(), EngineError>;
}

pub struct HostProbe {
    pub kvm_available: bool,
    pub kernel_release: String,
    /// True when the kernel is old enough for the backend; see the version rule below.
    pub kernel_supported: bool,
    pub cpu_vmx: bool,
    pub cpu_svm: bool,
    pub nested: bool,
}
```

`MacAddr` is a small newtype over six bytes, defined next to the trait. `events`, `output` and `console` are not `async` because they only build a stream; the first item is produced by the stream itself.

## 3. Method semantics

Every method is total in the sense that it either succeeds or returns one `EngineError`; no method panics on bad input, and no method returns a partially created VM on success. The rules below are this contract's definitions; each one that follows from an accepted decision, a research finding or an open question cites it inline.

### 3.1 `capabilities`

Reports what this backend and build support. It never fails: a missing binary or an unusable host is reported as `kvm: false`, not as an error, because the node agent must be able to report a node's real state without a working engine. `engine` is the backend name, `version` the pinned build, and `graphical_console` is `false` for Cloud Hypervisor: it has no VNC, SPICE, GPU or framebuffer device, and its consoles are text only (F-0034). `text_console` is `true`. `vm_snapshot` reports whether the backend can snapshot VM memory and device state; that capability is not used by Quoro before M5 and never as a volume backup (F-0035).

### 3.2 `probe_host`

Checks the machine rather than a VM:

- `/dev/kvm` exists and is writable;
- the kernel release is at least **6.8**, which Cloud Hypervisor v54 requires (F-0036);
- the CPU exposes `vmx` or `svm`;
- nesting is visible when the machine is itself a guest.

The procedure follows `docs/research/vps-nested-virt.md`. A successful probe is necessary but not sufficient: `/dev/kvm` can exist while a guest still fails to start, so the node agent treats the first real boot as the confirmation and reports a failed boot as a deployment failure in the `boot` phase.

### 3.3 `ensure_network`

Brings the node network to the requested `NodeNetwork`: the bridge exists, carries the gateway address, and forwards app traffic. Quoro owns the networking layer — bridges, TAP devices, addresses, firewall rules — rather than delegating it (D-0007). Precondition: `subnet` and `gateway` are consistent. Postcondition: a VM with an address inside `subnet` can reach the gateway. Idempotent: calling it twice with the same values changes nothing. An existing bridge with a different subnet is `Conflict`, never a silent reconfiguration.

### 3.4 `prepare_image`

Pulls the OCI image, verifies that the pulled content matches `ImageRef::digest`, converts it into a rootfs image, records the extracted `ImageConfig`, and caches the result by digest.

- Precondition: `reference` and `digest` are well formed.
- Postcondition: `PreparedImage::rootfs_path` exists and boots; a second call with the same digest returns the cached value without network access.
- Idempotent by digest. A digest mismatch between the reference and the pulled content is `Image`.
- The conversion approach is **open** (Q-0003). Writing our own OCI-to-ext4 pipeline with a Rust registry client and `mkfs.ext4 -d` is **recommended, pending maintainer decision**; the alternative is reusing an existing toolchain, or sharing a rootfs over virtio-fs, which needs an external daemon and shared memory (`docs/research/cloud-hypervisor.md`).

### 3.5 `remove_image`

Removes a cached prepared rootfs by digest. Deleting a VM must not remove an image another VM uses, so removal is a reference-counted operation or is driven by the node agent after it has computed the desired set. Missing digest is `Ok`. Idempotent. Rootfs images are per-digest artifacts of the deployment pipeline (D-0008, D-0033), so disposing of them is the engine's cleanup, not part of any API delete.

### 3.6 `create`

Creates the VM and its runtime state without starting it. One microVM per app instance, booted from a rootfs built from the OCI image (D-0008), with the lifecycle split that M1 requires (D-0033). Postcondition on success: a VMM process exists (or the backend's equivalent), the VM is inspectable, and `state` is `Stopped` once creation finishes — `Creating` is only observable while the call is in flight.

- The same `id` with an equal spec returns `Ok` and the existing `VmInfo`.
- The same `id` with a different spec is `Conflict`. Equality is compared on the canonical form of `VmSpec`, so field order and `Vec` ordering of `env` must be normalized before comparison.
- Resources are reserved at `create`, so `ResourceExhausted` is returned here rather than at `start`.
- Precondition: `rootfs` is the digest of a prepared image, `net` is inside a network already ensured by `ensure_network`, and `volumes` are attached to this node.

### 3.7 `start`

Boots a created or stopped VM. Postcondition on success: `state` is `Running` and `EngineEvent::Started` has been or will be emitted. A boot is fast enough that a stopped warm copy is a practical failover target (D-0008, D-0020); the boot-time figure is still a marketing claim, not a measured number (`docs/research/cloud-hypervisor.md`).

- Starting a running VM returns `Ok` and does not reboot it. Restarts are expressed as a spec change by the node agent (`restart_generation` in `node-agent-protocol.md`), so the engine never has to guess intent.
- Starting an unknown id is `NotFound`. Starting a VM whose kernel or rootfs is missing is `Image` or `Io`.

### 3.8 `stop`

Stops a VM.

- `Graceful { timeout }` asks the guest to exit — the power-button path, which the minimal init turns into `SIGTERM` to the app (section 7) — and waits at most `timeout`. On expiry it falls back to forced stop. The call returns `Ok` only when the VM is stopped.
- `Force` stops without cooperation.
- Stopping a stopped VM returns `Ok`. Stopping an unknown id is `NotFound`.
- Postcondition: `state` is `Stopped`, `exit_code` reports the app's code when the guest reported one, and `EngineEvent::Exited` has been or will be emitted.

### 3.9 `delete`

Removes the VM and its own disks. Missing id returns `Ok`, so a retried delete converges. Volumes are never removed by `delete`: they belong to the app, not to the instance, and are deleted through the API with `app:delete` (D-0033, `README.md`). Postcondition: the VMM process is gone, the root and data disks are removed, and `inspect` returns `NotFound`. Precondition: the VM is stopped; a call on a running VM is `Conflict`.

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

### 3.15 `counters`

Returns the VM's counters for metrics (M4, D-0033). Unknown id is `NotFound`. Values are cumulative since VM start; the node agent computes rates. Counters are best-effort: a backend that cannot report one of the groups returns zeros for it rather than failing the call.

### 3.16 `snapshot_volume` and `delete_volume_snapshot`

The point-in-time disk primitive the chunk engine needs (D-0023). Both are `Unsupported` in the default implementation, and only the M5 code path calls them.

- The primitive itself is **open** (Q-0004). Reflink snapshots with a full chunk scan for M5a, and a spike comparing lvm-thin and btrfs before M5b, are **recommended, pending maintainer decision**.
- They must **not** be implemented with Cloud Hypervisor VM snapshots. A CH snapshot is a paused VM's `config.json`, `memory-ranges` and `state.json`; it contains guest RAM and device state but no disk contents, and CH exposes no dirty bitmap (F-0035). A volume snapshot is about the disk.
- If implemented with a filesystem primitive, the call takes no VMM lock: it snapshots the volume of a stopped or running VM as the chosen primitive allows, and the chunk engine handles crash consistency (D-0023).

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
| `prepare_image` | Pulls, verifies the digest, builds the rootfs, extracts `ImageConfig`, caches by digest (Q-0003, pending maintainer decision) | No backend call |
| `remove_image` | Deletes the cached rootfs directory | No backend call |
| `create` | Creates the runtime directory, starts the VMM with `--api-socket <dir>/api.sock`, then configures the VM: direct kernel boot of the Quoro-provided guest kernel, rootfs as a virtio-blk disk, the writable disk and any volumes as further virtio-blk disks, the network device on the node bridge, the console socket | Process start, then `/vm.create` |
| `start` | Boots the configured VM | `/vm.boot` |
| `stop` graceful | Asks the guest to stop and waits for the process to exit | `/vm.power-button`, then `/vm.info` until stopped |
| `stop` force | Shuts the VMM down; if the socket does not answer, kills the process and reaps it | `/vmm.shutdown`, else `SIGKILL` on the VMM pid |
| `delete` | Deletes the VM, shuts the VMM down, removes the runtime directory and the VM's own disks, keeps volumes | `/vm.delete`, then `/vmm.shutdown` |
| `inspect` | Reads the VM's configuration and state | `/vm.info` |
| `list` | Scans the per-VM runtime directories, confirms each VMM is alive and reads its state | `/vmm.ping` and `/vm.info` per runtime directory |
| `events` | Watches the VMM process for exit and confirms the resulting state; a requested stop is distinguished from an unexpected exit | Process reaping, `/vm.info` |
| `output` | Connects to the VM's virtio-console socket and parses the init's tagged lines | Reads the `--console socket=` endpoint |
| `console` | Connects to the VM's serial socket and relays bytes in both directions, one client at a time | Reads and writes the `--serial socket=` endpoint (F-0034) |
| `counters` | Reads the VM's I/O and device counters and maps them to `VmCounters`; the exact field names of the backend's counter document are not fixed by `docs/research/cloud-hypervisor.md`, so the mapping is **Hypothesis** | `/vm.counters` |
| `snapshot_volume` | Default: `Unsupported`. When implemented: a filesystem-level snapshot of the volume (pending maintainer decision, Q-0004 open) | No `/vm.snapshot` call (F-0035) |
| `delete_volume_snapshot` | Default: `Unsupported`. When implemented: removes the filesystem-level snapshot | No backend call |

Details that follow from the research file:

- **Direct kernel boot**, not firmware: `--kernel` with the Quoro-provided guest kernel and `--cmdline`. The firmware path adds a bootloader stage and is not needed for a Linux-only guest (`docs/research/cloud-hypervisor.md`).
- **One pinned release per node-agent build.** Cloud Hypervisor has no versioned v2 API to design against, and neither snapshots nor live migration are portable across versions (`docs/research/cloud-hypervisor.md`). The pin is reported in `capabilities` and in `engine_version` in `hello`.
- **Console is a socket, not a pty.** `--serial socket=<path>` and `--console socket=<path>` are the v54 forms; a socket serves one client at a time and accepts a new one after the current one disconnects (F-0034, `docs/research/cloud-hypervisor.md`). That is why `console` reports `Conflict` on a second reader.
- **State files under the per-VM runtime directory** — spec, sockets, the VMM pid — so `list` can rediscover a running VM after an agent restart without restarting it (D-0011).

## 6. Guest contract with the minimal init

The minimal init is part of the engine workstream and is the only process Quoro puts inside the microVM. It is PID 1; the app runs under it (D-0008). Whether it is written for Quoro or reused is **open** (Q-0003); writing a small Rust init is **recommended, pending maintainer decision**, because the init is on the audit path and a large guest agent would be a black box.

**What the init receives.** A small machine-readable configuration, read before the app starts: a config disk or the kernel command line. The choice is part of the open Q-0003. The configuration carries:

| Item | Content |
| --- | --- |
| Entrypoint and arguments | The resolved command line of the app |
| Environment | Name and value pairs, already decrypted by the node agent |
| User | The user the app must run as |
| Working directory | The app's working directory |
| Network | Address, prefix length, gateway and interface name |
| Console | Which console carries log lines |

**What the init must do.**

1. Mount `/proc`, `/sys` and `/dev`, and mount the writable disk at the app's data location.
2. Configure the network interface with the address, gateway and DNS given in the configuration.
3. Start the app as **PID 2** under the configured user, in the configured working directory, with the configured environment.
4. Forward the app's stdout and stderr to the virtio-console **with a stream tag per line**, so the node agent can tell the two apart without parsing app output. This is the source of `OutputLine` and therefore of local app logs (D-0014).
5. Reap orphaned children so no zombie accumulates, and forward signals to the app where they matter.
6. On the power-button signal, send `SIGTERM` to the app, wait for it to exit or for the timeout to pass, then power the guest off.
7. Report the app's exit code so the engine can surface it as `exit_code` and as `EngineEvent::Exited`.

While the app is running, the init must not write to the app's stdout, so a log line's stream tag is never ambiguous. On a failed boot — no configuration, a missing entrypoint, an unmountable disk — the init writes a diagnostic to the serial console and powers off with a non-zero code, so the failure surfaces as a `deployment.failed` event in the `boot` phase rather than as a silent hang.

## 7. `FakeEngine`

Tests must run on machines without KVM, including CI and the laptops of people who only work on the panel. `FakeEngine` implements the same trait with no hypervisor:

- In memory: VMs are records, `create`/`start`/`stop`/`delete` change their state, `list` returns exactly what was created since the instance started.
- Deterministic: no timers, no threads, no wall-clock dependence beyond explicit calls. Boots complete when `start` returns.
- Injectable: a test can make a VM crash after N seconds, make `start` return `ResourceExhausted`, make `prepare_image` return `Image`, and make the console echo, so every error path and every event the node agent must map is reachable.
- Streams: `events`, `output` and `console` are real streams over in-memory channels, so the node agent's stream handling is exercised, not stubbed.
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
| M2 | `output` feeds live app logs (D-0014, D-0033) |
| M4 | `console` and `counters` (D-0033) |
| M5a/M5b | `snapshot_volume` and `delete_volume_snapshot`, once Q-0004 is decided (D-0023, Q-0004) |
