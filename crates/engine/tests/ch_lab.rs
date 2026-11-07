// SPDX-License-Identifier: AGPL-3.0-only
//! The KVM integration suite of the Cloud Hypervisor engine (contract §5).
//!
//! Every test is `#[ignore]`d: it needs `/dev/kvm`, passwordless root, the
//! pinned artifacts and a real network namespace, which only a lab host has.
//! Run with `crates/engine/scripts/lab-suite.sh` (it fetches the pinned
//! artifacts, builds `quoro-init` and the probe images, resolves the alpine
//! digest and runs every lab suite as root); the variables below are what it
//! sets.
//!
//! - `QUORO_ENGINE_LAB=1`: the lab switch every test checks.
//! - `QUORO_ENGINE_ARTIFACTS`: directory with the pinned Cloud Hypervisor and
//!   guest kernel under their pinned file names.
//! - `QUORO_ENGINE_INIT`: the static musl `quoro-init` binary.
//! - `QUORO_ENGINE_STATE`: the engine state directory (wiped by each test that
//!   calls [`reset_state`]).
//! - `QUORO_ENGINE_DNS`: optional, comma separated DNS servers for the guests.
//! - `QUORO_ENGINE_ALPINE`: the digest (`sha256:…`) of `alpine:3.22`.
//!
//! Every test holds a [`Guard`] whose `Drop` force-kills and removes whatever
//! the test created, so a failure half way never leaves a VMM, a TAP or the
//! bridge behind for the next run.
#![cfg(target_os = "linux")]
#![allow(dead_code)]

use std::future::poll_fn;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quoro_engine::ch::net::{self, tap_name};
use quoro_engine::ch::state::{self, VmmRecord};
use quoro_engine::ch::tools::{self, HostTools};
use quoro_engine::ch::{ChConfig, ChEngine};
use quoro_engine::{
    ByteStream, ConsoleMode, Engine, EngineError, EngineEvent, ExecRequest, ImageDigest, ImageRef,
    InitConfig, LineStream, MacAddr, NetAttach, NodeNetwork, OutputLine, OutputStream, RangesOf,
    SerialMode, SnapshotMethod, SnapshotSweep, StageFrom, StopMode, VmId, VmSpec, VmState,
    VolumeAttach, VolumeId, VolumeSnapshot, VsockAttach,
};
use tokio::io::AsyncReadExt as _;

/// The bound of every start here: long enough for a real boot (contract §3.7).
const START: Duration = Duration::from_secs(60);

/// The force bound of the graceful stops here (contract §3.8).
const FORCE_WITHIN: Duration = Duration::from_secs(10);

/// A forced stop that no working stop overruns (contract §3.8).
const FORCE: StopMode = StopMode::Force {
    within: FORCE_WITHIN,
};

/// The lab switch every test checks.
const LAB_ENV: &str = "QUORO_ENGINE_LAB";
/// The bridge every test uses.
const BRIDGE: &str = "qlab0";
/// The subnet every test uses.
const SUBNET: &str = "10.77.0.0/24";
/// The gateway of [`SUBNET`].
const GATEWAY: &str = "10.77.0.1";

/// True when the caller asked for the lab suite.
fn lab_enabled() -> bool {
    matches!(std::env::var(LAB_ENV).as_deref(), Ok("1"))
}

/// True when the test process is root.
fn is_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .map(|output| output.stdout.starts_with(b"0"))
        .unwrap_or(false)
}

/// The shared precondition of every test: the lab switch, root and a wiped
/// state directory. Returns `false` when the suite was not requested.
fn enter_lab() -> bool {
    if !lab_enabled() {
        eprintln!("skipping: set {LAB_ENV}=1 to run the lab suite");
        return false;
    }
    assert!(is_root(), "the lab suite runs as root");
    reset_state();
    true
}

/// A required environment variable as a path.
fn env_path(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set for the lab suite")),
    )
}

/// The engine state directory.
fn state_dir() -> PathBuf {
    env_path("QUORO_ENGINE_STATE")
}

/// Wipes and recreates the state directory.
fn reset_state() {
    let dir = state_dir();
    // The volume pool is a mount point: wiping the directory underneath a
    // mounted pool would delete the image's contents (§3.16).
    unmount_volume_pool(&dir.join("volumes"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("wipe the state directory");
    }
    std::fs::create_dir_all(&dir).expect("create the state directory");
}

/// Unmounts `<state>/volumes` when it is a mount point.
///
/// The test-only force path: it does not refuse a pool that still holds a
/// volume, which a panicked test may leave behind, and it detaches lazily when
/// a file of the pool is still open.
fn unmount_volume_pool(volumes: &Path) {
    if !is_mount_point(volumes) {
        return;
    }
    for args in [&["-l"][..], &[][..]] {
        let unmounted = std::process::Command::new("umount")
            .args(args)
            .arg(volumes)
            .status()
            .is_ok_and(|status| status.success());
        if unmounted {
            return;
        }
    }
    panic!("cannot unmount {}", volumes.display());
}

/// True when `dir` is a mount point in this namespace.
fn is_mount_point(dir: &Path) -> bool {
    !mounts_on(dir).is_empty()
}

/// The `(fstype, source)` of every mount table line mounted on `dir`.
///
/// The lab suite owns no copy of the engine's mount table parser, so it reads
/// the same file the pool does, with the kernel's `\040` escapes decoded.
fn mounts_on(dir: &Path) -> Vec<(String, String)> {
    let Ok(text) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return Vec::new();
    };
    let dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    text.lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let fields: Vec<&str> = left.split_whitespace().collect();
            let rest: Vec<&str> = right.split_whitespace().collect();
            if fields.len() < 5 || rest.len() < 2 {
                return None;
            }
            let point = fields[4].replace("\\040", " ");
            (Path::new(&point) == dir.as_path()).then(|| (rest[0].to_owned(), rest[1].to_owned()))
        })
        .collect()
}

/// True when `dir`'s filesystem clones files with `FICLONE`.
fn fs_reflinks(dir: &Path) -> bool {
    let source = dir.join(format!(".reflink-probe-{}", std::process::id()));
    let clone = dir.join(format!(".reflink-probe-{}-clone", std::process::id()));
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&clone);
    let cloned = (|| {
        std::fs::write(&source, b"quoro").ok()?;
        let src = std::fs::File::open(&source).ok()?;
        let dst = std::fs::File::create(&clone).ok()?;
        rustix::fs::ioctl_ficlone(&dst, &src).ok()
    })();
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&clone);
    cloned.is_some()
}

/// The engine configuration from the environment.
fn config() -> ChConfig {
    ChConfig::new(
        state_dir(),
        env_path("QUORO_ENGINE_ARTIFACTS"),
        env_path("QUORO_ENGINE_INIT"),
    )
}

/// A fresh engine on the current state directory.
async fn new_engine() -> ChEngine {
    ChEngine::new(config()).await.expect("build the engine")
}

/// A fresh engine whose start-up sweep keeps only the snapshots named in
/// `sweep`, as the node agent's records do (§3.16).
async fn new_engine_with(sweep: SnapshotSweep) -> ChEngine {
    let mut config = config();
    config.snapshot_sweep = sweep;
    ChEngine::new(config).await.expect("build the engine")
}

/// The file name of a snapshot: what the start-up sweep keeps it by.
fn snapshot_file_name(snapshot: &VolumeSnapshot) -> String {
    snapshot
        .location
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a snapshot file name")
        .to_owned()
}

/// The node network every test expects.
fn network() -> NodeNetwork {
    NodeNetwork {
        bridge: BRIDGE.to_string(),
        subnet: SUBNET.to_string(),
        gateway: GATEWAY.parse().expect("gateway"),
        dns: std::env::var("QUORO_ENGINE_DNS")
            .map(|dns| {
                dns.split(',')
                    .filter(|server| !server.trim().is_empty())
                    .map(|server| server.trim().parse::<Ipv4Addr>().expect("valid DNS server"))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// The image every test prepares.
fn alpine() -> ImageRef {
    ImageRef {
        reference: "docker.io/library/alpine:3.22".to_string(),
        digest: ImageDigest(
            std::env::var("QUORO_ENGINE_ALPINE")
                .expect("QUORO_ENGINE_ALPINE is not set for the lab suite"),
        ),
    }
}

/// Ensures the lab network and prepares alpine, returning its digest.
///
/// Both are idempotent, so a test may call this after a retry.
async fn prepare(engine: &ChEngine) -> ImageDigest {
    engine
        .ensure_network(&network())
        .await
        .expect("ensure the network");
    engine
        .ensure_network(&network())
        .await
        .expect("ensure_network is idempotent");
    engine
        .prepare_image(&alpine())
        .await
        .expect("prepare alpine")
        .digest
}

/// The runtime directory of one VM.
fn vm_dir(id: &VmId) -> PathBuf {
    state_dir().join("vms").join(&id.0)
}

/// What `nft list set` prints for one set of the lab bridge's table, with
/// whitespace runs collapsed to one space so a wrapped element list still
/// matches.
///
/// The table name follows the ruleset's rule: `quoro_` plus the bridge with
/// dashes flattened.
async fn listed_set(tools: &HostTools, set: &str) -> String {
    let table = format!("quoro_{}", BRIDGE.replace('-', "_"));
    let text = tools::run(&tools.nft, &["list", "set", "bridge", &table, set])
        .await
        .unwrap_or_else(|err| panic!("nft list set {set}: {err}"));
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The console socket of one VM, under the state directory.
fn console_socket(id: &VmId) -> PathBuf {
    state_dir().join(format!("{}.console.sock", id.0))
}

/// The serial socket of one VM, under the state directory (M4).
fn serial_socket(id: &VmId) -> PathBuf {
    state_dir().join(format!("{}.serial.sock", id.0))
}

/// A VM spec for the lab suite.
fn lab_spec(
    id: &VmId,
    rootfs: &ImageDigest,
    console: ConsoleMode,
    ip: &str,
    app: &str,
    seed: u8,
) -> VmSpec {
    VmSpec {
        id: id.clone(),
        vcpus: 1,
        memory_mb: 256,
        rootfs: rootfs.clone(),
        disk_gb: 1,
        volumes: Vec::new(),
        net: NetAttach {
            ip: ip.parse().expect("address"),
            gateway: GATEWAY.parse().expect("gateway"),
            mac: MacAddr([0x02, 0x00, 0x00, 0x00, 0x77, seed]),
            isolation_group: None,
            egress_only: false,
        },
        init: InitConfig {
            entrypoint: Vec::new(),
            cmd: vec!["sh".to_string(), "-c".to_string(), app.to_string()],
            env: vec![("QUORO_TEST".to_string(), "lab".to_string())],
            user: None,
            workdir: None,
        },
        console,
        serial: quoro_engine::SerialMode::Off,
        vsock: None,
        work_disks: Vec::new(),
    }
}

/// A virtio-console spec, the shape every output-reading test needs.
fn console_spec(id: &VmId, rootfs: &ImageDigest, ip: &str, app: &str, seed: u8) -> VmSpec {
    lab_spec(
        id,
        rootfs,
        ConsoleMode::VirtioConsole {
            socket: console_socket(id),
        },
        ip,
        app,
        seed,
    )
}

/// A classic lifecycle app: prints `ready`, then waits for the power button.
const LIFECYCLE_APP: &str = "echo ready; trap \"echo bye; exit 0\" TERM; while :; do sleep 1; done";

/// Removes whatever a test created when it panics half way.
///
/// The cleanup runs on its own thread with its own runtime: `Drop` cannot
/// await, and the test's runtime may already be winding down. Every step is
/// best effort, because a failing cleanup must not hide the test's own panic.
struct Guard {
    state: PathBuf,
    bridge: String,
}

impl Guard {
    fn new() -> Self {
        Self {
            state: state_dir(),
            bridge: BRIDGE.to_string(),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let state = self.state.clone();
        let bridge = self.bridge.clone();
        let _ = std::thread::spawn(move || cleanup(&state, &bridge)).join();
    }
}

/// Force-kills every recorded VMM under `state`, removes its TAP, then removes
/// the bridge and its nftables table.
fn cleanup(state: &Path, bridge: &str) {
    let Ok(tools) = HostTools::discover() else {
        return;
    };
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(state.join("vms")) else {
        unmount_volume_pool(&state.join("volumes"));
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let id = VmId(name);
        let dir = entry.path();
        if let Ok(Some(record)) = state::read_json::<VmmRecord>(&dir.join("vmm.json"))
            && let Some(pid) = rustix::process::Pid::from_raw(record.process.pid as i32)
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = runtime.block_on(net::delete_tap(&tools, &tap_name(&id)));
        let _ = std::fs::remove_file(state.join(format!("{}.console.sock", id.0)));
        let _ = std::fs::remove_file(state.join(format!("{}.serial.sock", id.0)));
        let _ = std::fs::remove_dir_all(&dir);
    }
    // The pool is a mount of the state directory: never leave it behind.
    unmount_volume_pool(&state.join("volumes"));
    let _ = runtime.block_on(net::remove_network(&tools, bridge));
}

/// Every lifecycle event the engine emitted so far.
type SharedEvents = Arc<Mutex<Vec<EngineEvent>>>;

/// Subscribes once and drains the stream in the background, so no test misses
/// an event it did not expect first.
fn collect_events(engine: &ChEngine) -> SharedEvents {
    let events: SharedEvents = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let mut stream = engine.events();
    tokio::spawn(async move {
        while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            sink.lock().expect("event log").push(event);
        }
    });
    events
}

/// Waits until an event matching `predicate` was emitted.
async fn wait_event(
    events: &SharedEvents,
    predicate: impl Fn(&EngineEvent) -> bool,
    timeout: Duration,
) -> EngineEvent {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let found = events
            .lock()
            .expect("event log")
            .iter()
            .find(|event| predicate(event))
            .cloned();
        if let Some(event) = found {
            return event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no matching engine event within {timeout:?}; saw {:?}",
            events.lock().expect("event log")
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Reads the VM's output until one line contains `needle`.
async fn read_output(stream: &mut LineStream, needle: &str, timeout: Duration) -> OutputLine {
    let deadline = std::time::Instant::now() + timeout;
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "no line containing {needle:?} within {timeout:?}; saw {seen:?}"
        );
        match tokio::time::timeout(remaining, poll_fn(|cx| stream.as_mut().poll_next(cx))).await {
            Ok(Some(line)) => {
                seen.push(line.line.clone());
                if line.line.contains(needle) {
                    return line;
                }
            }
            Ok(None) => panic!("the output ended before {needle:?}; saw {seen:?}"),
            Err(_) => panic!("no line containing {needle:?} within {timeout:?}; saw {seen:?}"),
        }
    }
}

/// Asserts that nothing of one VM survived its delete.
fn assert_no_leftovers(id: &VmId) {
    let dir = vm_dir(id);
    assert!(!dir.exists(), "{} still exists", dir.display());
    assert!(
        !net::interface_exists(&tap_name(id)),
        "TAP {} still exists",
        tap_name(id)
    );
    assert_no_vmm(id);
}

/// No process runs for `id`: nothing names its runtime directory.
fn assert_no_vmm(id: &VmId) {
    let needle = vm_dir(id).to_string_lossy().into_owned();
    for entry in std::fs::read_dir("/proc").expect("read /proc") {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let cmdline = String::from_utf8_lossy(&cmdline);
        assert!(
            !cmdline.contains(&needle),
            "a process still runs for {id:?}: {cmdline}"
        );
    }
}

/// Starts a created VM and returns its info.
async fn start_vm(engine: &ChEngine, id: &VmId) {
    let started = engine.start(id, START).await.expect("start");
    assert_eq!(started.state, VmState::Running, "{id:?} did not start");
    assert!(started.pid.is_some(), "{id:?} started without a pid");
}

/// One full lifecycle: create twice, start, tag output, graceful stop, start,
/// force stop, delete, and no leftovers.
async fn run_cycle(engine: &ChEngine, events: &SharedEvents, cycle: u8, rootfs: &ImageDigest) {
    let id = VmId(format!("vm_w7b_{cycle}"));
    let socket = console_socket(&id);
    let spec = console_spec(&id, rootfs, "10.77.0.10", LIFECYCLE_APP, cycle);

    let created = engine.create(&spec).await.expect("create");
    assert_eq!(created.state, VmState::Stopped);
    assert!(created.pid.is_some(), "a created VM has a VMM process");

    let again = engine.create(&spec).await.expect("create is idempotent");
    assert_eq!(again.state, VmState::Stopped);
    assert_eq!(again.pid, created.pid, "the same VMM serves the same spec");

    start_vm(engine, &id).await;
    wait_event(
        events,
        |event| matches!(event, EngineEvent::Started { id: started } if *started == id),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Running
    );
    let listed = engine.list().await.expect("list");
    assert!(
        listed
            .iter()
            .any(|info| info.id == id && info.state == VmState::Running),
        "list does not show {id:?} running: {listed:?}"
    );

    // The console socket is open for the whole run; the guest init tags stdout
    // with `out `, which `output` turns into a Stdout line (§3.13).
    let mut output = engine.output(&id).expect("open the output");
    let ready = read_output(&mut output, "ready", Duration::from_secs(30)).await;
    assert_eq!(ready.stream, OutputStream::Stdout, "ready is stdout");
    assert_eq!(ready.line, "ready");
    assert!(
        matches!(engine.output(&id), Err(EngineError::Conflict(_))),
        "a second reader must be a Conflict"
    );

    // The app is running, so the guest has read its root disk and holds
    // memory (§3.15).
    let counters = engine.counters(&id).await.expect("counters");
    assert!(counters.memory_rss_bytes > 0, "{counters:?}");
    assert!(counters.block_read_bytes > 0, "{counters:?}");

    let stopped = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(20),
                force_within: FORCE_WITHIN,
            },
        )
        .await
        .expect("graceful stop");
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.exit_code, Some(0), "the app exits 0 on SIGTERM");
    wait_event(
        events,
        |event| matches!(event, EngineEvent::Exited { id: exited, code: 0 } if *exited == id),
        Duration::from_secs(10),
    )
    .await;
    let bye = read_output(&mut output, "bye", Duration::from_secs(10)).await;
    assert_eq!(bye.stream, OutputStream::Stdout, "bye is stdout");
    assert_eq!(bye.line, "bye");
    assert!(
        !vm_dir(&id).join("vmm.json").exists(),
        "the exit path removes vmm.json"
    );
    drop(output);

    start_vm(engine, &id).await;
    let forced = engine.stop(&id, FORCE).await.expect("force stop");
    assert_eq!(forced.state, VmState::Stopped);
    assert_eq!(forced.exit_code, Some(137), "a forced stop reports 137");

    engine.delete(&id).await.expect("delete");
    assert!(matches!(
        engine.inspect(&id).await,
        Err(EngineError::NotFound)
    ));
    assert_no_leftovers(&id);
    assert!(!socket.exists(), "the console socket survived the delete");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn lifecycle_cycle_three_times() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;

    let probe = engine.probe_host().await.expect("probe the host");
    assert!(probe.kvm_available, "no usable /dev/kvm: {probe:?}");
    assert!(probe.kernel_supported, "kernel too old: {probe:?}");
    assert!(probe.cpu_vmx || probe.cpu_svm, "no VT-x or SVM: {probe:?}");

    let events = collect_events(&engine);
    for cycle in 1..=3 {
        let rootfs = prepare(&engine).await;
        run_cycle(&engine, &events, cycle, &rootfs).await;
    }

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn a_vm_restarts_on_its_recorded_bridge_after_a_host_reboot() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;

    let id = VmId("vm_w7b_reboot".to_string());
    let spec = console_spec(&id, &rootfs, "10.77.0.14", "echo up; sleep 120", 14);
    engine.create(&spec).await.expect("create");
    start_vm(&engine, &id).await;

    // The reboot: the VMM, its TAP, the bridge and its nftables table are gone,
    // and the next agent does not ensure this bridge again (a node whose subnet
    // changed ensures only its new one).
    engine.stop(&id, FORCE).await.expect("stop");
    drop(engine);
    let tools = HostTools::discover().expect("host tools");
    net::delete_tap(&tools, &tap_name(&id))
        .await
        .expect("remove the TAP");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the bridge");
    assert!(!net::interface_exists(BRIDGE), "the bridge is gone");

    let engine = new_engine().await;
    start_vm(&engine, &id).await;
    assert!(net::interface_exists(BRIDGE), "the recorded bridge is back");
    let addresses = tools::run(&tools.ip, &["-4", "-o", "addr", "show", "dev", BRIDGE])
        .await
        .expect("read the bridge address");
    assert!(
        addresses.contains(&format!("{GATEWAY}/24")),
        "the bridge has its recorded gateway: {addresses}"
    );
    let mut output = engine.output(&id).expect("open the output");
    let up = read_output(&mut output, "up", Duration::from_secs(45)).await;
    assert_eq!(up.line, "up", "the VM did not boot on the restored bridge");

    engine.stop(&id, FORCE).await.expect("stop again");
    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn crash_is_reported() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;
    let events = collect_events(&engine);

    let id = VmId("vm_w7b_crash".to_string());
    let spec = console_spec(&id, &rootfs, "10.77.0.12", "echo up; sleep 2; exit 7", 12);
    engine.create(&spec).await.expect("create");
    start_vm(&engine, &id).await;

    let crashed = wait_event(
        &events,
        |event| matches!(event, EngineEvent::Crashed { id: crashed, .. } if *crashed == id),
        Duration::from_secs(60),
    )
    .await;
    let EngineEvent::Crashed { reason, .. } = crashed else {
        unreachable!("the predicate only matches a Crashed event");
    };
    assert_eq!(reason, "app exited with code 7", "{reason}");

    let info = engine.inspect(&id).await.expect("inspect");
    assert_eq!(info.state, VmState::Crashed);
    assert_eq!(info.exit_code, Some(7));
    assert_eq!(info.pid, None, "the VMM is gone after the crash");
    assert!(
        !vm_dir(&id).join("vmm.json").exists(),
        "the exit path removes vmm.json"
    );

    // Nothing restarts it (§3.11).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let info = engine.inspect(&id).await.expect("inspect again");
    assert_eq!(info.state, VmState::Crashed, "the crash was not restarted");

    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn isolation_between_vms() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;

    const PEER_APP: &str = "ping -c 3 -W 2 10.77.0.1 && echo gw-ok; \
                            ping -c 3 -W 2 10.77.0.11 && echo peer-reachable \
                            || echo peer-blocked; sleep 60";
    let a = VmId("vm_w7b_iso_a".to_string());
    let b = VmId("vm_w7b_iso_b".to_string());
    let spec_a = console_spec(&a, &rootfs, "10.77.0.10", PEER_APP, 10);
    let spec_b = lab_spec(&b, &rootfs, ConsoleMode::Off, "10.77.0.11", "sleep 120", 11);
    engine.create(&spec_a).await.expect("create A");
    engine.create(&spec_b).await.expect("create B");
    start_vm(&engine, &b).await;
    start_vm(&engine, &a).await;

    let mut output = engine.output(&a).expect("open A's output");
    let gateway = read_output(&mut output, "gw-ok", Duration::from_secs(45)).await;
    assert_eq!(gateway.line, "gw-ok", "A cannot reach the gateway");
    let peer = read_output(&mut output, "peer-", Duration::from_secs(30)).await;
    assert_eq!(
        peer.line, "peer-blocked",
        "A reached B through the bridge; port isolation is broken"
    );

    engine.stop(&a, FORCE).await.expect("stop A");
    engine.stop(&b, FORCE).await.expect("stop B");
    engine.delete(&a).await.expect("delete A");
    engine.delete(&b).await.expect("delete B");
    assert_no_leftovers(&a);
    assert_no_leftovers(&b);

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn rediscovery_after_engine_restart() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;

    let id = VmId("vm_w7b_adopt".to_string());
    let spec = console_spec(&id, &rootfs, "10.77.0.10", "sleep 120", 13);
    engine.create(&spec).await.expect("create");
    start_vm(&engine, &id).await;
    let pid = engine.inspect(&id).await.expect("inspect").pid;
    assert!(pid.is_some(), "the running VM has a pid");

    // A new agent process on the same state directory adopts the live VMM.
    drop(engine);
    let engine = new_engine().await;
    let listed = engine.list().await.expect("list after rediscovery");
    let adopted = listed.iter().find(|info| info.id == id);
    let adopted = adopted.unwrap_or_else(|| panic!("{id:?} is not listed after rediscovery"));
    assert_eq!(adopted.state, VmState::Running, "the VM was not adopted");
    assert_eq!(adopted.pid, pid, "the adopted VM is the same process");

    let stopped = engine.stop(&id, FORCE).await.expect("stop");
    assert_eq!(stopped.state, VmState::Stopped);
    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn egress_only_vm_reaches_only_the_outside() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;

    let peer_ip = "10.77.0.11";
    let link_local = "169.254.169.254";
    let outside = "1.1.1.1";
    let dns = network().dns;
    let dns_app = match dns.first() {
        Some(server) => format!(
            "if nslookup example.com {server} >/dev/null 2>&1; then \
                echo egress-dns-ok; else echo egress-dns-blocked; fi\n"
        ),
        None => String::new(),
    };

    // A listener on the gateway address: an egress-only VM may not reach it,
    // whatever port it uses.
    let listener =
        tokio::net::TcpListener::bind((GATEWAY.parse::<Ipv4Addr>().expect("gateway"), 0))
            .await
            .expect("bind a listener on the gateway");
    let port = listener.local_addr().expect("listener address").port();

    let app = format!(
        "if ping -c 1 -W 2 {peer_ip} >/dev/null 2>&1; then echo egress-peer-reachable; \
            else echo egress-peer-blocked; fi\n\
         if nc -w 2 {GATEWAY} {port} </dev/null >/dev/null 2>&1; then echo egress-gateway-reachable; \
            else echo egress-gateway-blocked; fi\n\
         if ping -c 1 -W 2 {link_local} >/dev/null 2>&1; then echo egress-link-local-reachable; \
            else echo egress-link-local-blocked; fi\n\
         {dns_app}\
         if nc -w 5 {outside} 443 </dev/null >/dev/null 2>&1; then echo egress-outside-ok; \
            else echo egress-outside-blocked; fi\n\
         echo egress-done; sleep 60"
    );

    let peer = VmId("vm_w7b_egress_peer".to_string());
    let builder = VmId("vm_w7b_egress".to_string());
    let peer_spec = lab_spec(&peer, &rootfs, ConsoleMode::Off, peer_ip, "sleep 120", 21);
    let mut builder_spec = console_spec(&builder, &rootfs, "10.77.0.10", &app, 20);
    builder_spec.net.egress_only = true;
    assert!(builder_spec.net.isolation_group.is_none());

    engine.create(&peer_spec).await.expect("create the peer");
    engine
        .create(&builder_spec)
        .await
        .expect("create the builder");

    // The anti-spoofing sets name the builder's TAP with the one address and
    // MAC its spec assigned, so a frame from it claiming another address or
    // MAC is dropped before the bridge learns it.
    let tools = HostTools::discover().expect("host tools");
    let tap = tap_name(&builder);
    let addrs = listed_set(&tools, "egress_addrs").await;
    // The lab spec above gave the builder the MAC `02:00:00:00:77:14` (seed 20).
    assert!(
        addrs.contains(&format!("\"{tap}\" . 02:00:00:00:77:14 . 10.77.0.10")),
        "the egress address set does not hold the builder's tuple: {addrs}"
    );
    let taps = listed_set(&tools, "egress").await;
    assert!(
        taps.contains(&format!("\"{tap}\"")),
        "the egress set does not hold the builder's TAP: {taps}"
    );

    start_vm(&engine, &peer).await;
    start_vm(&engine, &builder).await;

    let mut output = engine.output(&builder).expect("open the builder's output");
    let peer_line = read_output(&mut output, "egress-peer-", Duration::from_secs(60)).await;
    assert_eq!(
        peer_line.line, "egress-peer-blocked",
        "an egress-only VM reached another VM"
    );
    let gateway_line = read_output(&mut output, "egress-gateway-", Duration::from_secs(30)).await;
    assert_eq!(
        gateway_line.line, "egress-gateway-blocked",
        "an egress-only VM reached a host port"
    );
    let link_local_line =
        read_output(&mut output, "egress-link-local-", Duration::from_secs(30)).await;
    assert_eq!(
        link_local_line.line, "egress-link-local-blocked",
        "an egress-only VM reached 169.254.0.0/16"
    );
    if !dns.is_empty() {
        let dns_line = read_output(&mut output, "egress-dns-", Duration::from_secs(60)).await;
        assert_eq!(
            dns_line.line, "egress-dns-ok",
            "an egress-only VM could not query its configured DNS server"
        );
    }
    let outside_line = read_output(&mut output, "egress-outside-", Duration::from_secs(60)).await;
    assert_eq!(
        outside_line.line, "egress-outside-ok",
        "an egress-only VM could not reach the outside"
    );
    read_output(&mut output, "egress-done", Duration::from_secs(30)).await;
    drop(output);
    drop(listener);

    engine
        .stop(&builder, FORCE)
        .await
        .expect("stop the builder");
    engine.stop(&peer, FORCE).await.expect("stop the peer");
    engine.delete(&builder).await.expect("delete the builder");
    engine.delete(&peer).await.expect("delete the peer");
    assert_no_leftovers(&builder);
    assert_no_leftovers(&peer);

    // Deleting the builder took its address back out of both sets.
    let addrs = listed_set(&tools, "egress_addrs").await;
    assert!(
        !addrs.contains("elements") && !addrs.contains(&tap),
        "the egress address set still holds a deleted VM: {addrs}"
    );
    let taps = listed_set(&tools, "egress").await;
    assert!(
        !taps.contains("elements") && !taps.contains(&tap),
        "the egress set still holds a deleted VM: {taps}"
    );

    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn guest_reset_is_a_crash() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;
    let events = collect_events(&engine);

    let id = VmId("vm_w7b_reset".to_string());
    let spec = console_spec(
        &id,
        &rootfs,
        "10.77.0.13",
        "echo c > /proc/sysrq-trigger",
        14,
    );
    engine.create(&spec).await.expect("create");
    start_vm(&engine, &id).await;

    let crashed = wait_event(
        &events,
        |event| matches!(event, EngineEvent::Crashed { id: crashed, .. } if *crashed == id),
        Duration::from_secs(60),
    )
    .await;
    let EngineEvent::Crashed { reason, .. } = crashed else {
        unreachable!("the predicate only matches a Crashed event");
    };
    assert!(
        reason.contains("guest reset"),
        "a guest reset is a crash, not {reason:?}"
    );

    let info = engine.inspect(&id).await.expect("inspect");
    assert_eq!(info.state, VmState::Crashed, "{info:?}");
    assert_eq!(info.pid, None, "the VMM is gone after the reset");
    assert!(
        !vm_dir(&id).join("vmm.json").exists(),
        "the exit path removes vmm.json"
    );

    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

/// M5a: a volume is created, attached to a running VM and snapshotted with the
/// probed method, and both survive the VM's delete (§3.16, §3.20).
#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn local_volume_snapshot_cycle() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;
    let method = engine
        .capabilities()
        .await
        .volume_snapshot_method
        .expect("the engine has volumes");

    let volume = VolumeId("vol_01HX8Z4Q2M9W7T3YV6B1C0D5EF".to_string());
    let created = engine.ensure_volume(&volume, 1).await.expect("ensure");
    assert_eq!(created.size_gb, 1);
    assert_eq!(
        std::fs::metadata(&created.path).expect("volume file").len(),
        1 << 30
    );
    let again = engine
        .ensure_volume(&volume, 1)
        .await
        .expect("ensure is idempotent");
    assert_eq!(again.path, created.path);
    assert!(
        matches!(
            engine.ensure_volume(&volume, 2).await,
            Err(EngineError::Conflict(_))
        ),
        "a different size is a Conflict"
    );
    assert!(
        created.allocated_bytes < 1 << 29,
        "the volume is sparse: {} bytes",
        created.allocated_bytes
    );

    let id = VmId("vm_w5a_volume".to_string());
    let mut spec = console_spec(&id, &rootfs, "10.77.0.16", "sleep 120", 16);
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    // A volume mount travels in the v3 guest configuration, which carries the
    // exec port (§6, M5a).
    spec.vsock = Some(VsockAttach {
        cid: 3,
        socket: vm_dir(&id).join("vsock.sock"),
    });
    engine.create(&spec).await.expect("create with a volume");
    start_vm(&engine, &id).await;

    let snapshot = engine
        .snapshot_volume(&volume)
        .await
        .expect("snapshot a running volume");
    assert_eq!(snapshot.volume_id, volume);
    assert_eq!(
        snapshot.location.file_name().and_then(|name| name.to_str()),
        Some(snapshot.snapshot_id.as_str()),
        "the snapshot id is its file name"
    );
    assert!(
        snapshot.snapshot_id.starts_with(&format!("{}.", volume.0))
            && snapshot.snapshot_id.ends_with(".snapshot"),
        "{}",
        snapshot.snapshot_id
    );
    assert_eq!(snapshot.method, method);
    assert_eq!(snapshot.size_bytes, 1 << 30);
    if method == SnapshotMethod::PauseCopy {
        assert!(
            snapshot.pause > Duration::ZERO,
            "a PauseCopy snapshot of a running VM pauses it"
        );
    }
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Running,
        "the VM is running after the snapshot"
    );
    // From M5b a volume keeps several snapshots, each in its own file (§3.16).
    let second = engine
        .snapshot_volume(&volume)
        .await
        .expect("a second snapshot");
    assert_ne!(second.snapshot_id, snapshot.snapshot_id);
    assert!(snapshot.location.exists() && second.location.exists());
    engine
        .delete_volume_snapshot(&second)
        .await
        .expect("delete the second snapshot");
    assert!(snapshot.location.exists(), "the first snapshot is kept");
    use std::os::unix::fs::MetadataExt as _;
    let allocated = std::fs::metadata(&snapshot.location)
        .expect("snapshot file")
        .blocks()
        * 512;
    assert!(
        allocated < 1 << 29,
        "the snapshot is sparse: {allocated} bytes"
    );

    engine.stop(&id, FORCE).await.expect("stop");
    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);
    assert!(
        created.path.exists(),
        "delete keeps the VM's volumes (§3.9)"
    );

    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete the snapshot");
    assert!(!snapshot.location.exists(), "the snapshot file is gone");
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("deleting a missing snapshot is Ok");

    engine
        .delete_volume(&volume)
        .await
        .expect("delete the volume");
    assert!(!created.path.exists(), "the volume file is gone");
    engine
        .delete_volume(&volume)
        .await
        .expect("deleting a missing volume is Ok");

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

/// M5a: on a data directory without reflink the volume pool gives `Reflink`
/// snapshots with no pause, survives an engine restart and detaches cleanly
/// (§3.16, ADR 0156).
#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn volume_pool_on_ext4_snapshots_with_reflink() {
    if !enter_lab() {
        return;
    }
    let state = state_dir();
    if fs_reflinks(&state) {
        eprintln!("skipping: the state directory reflinks natively, so there is no pool");
        return;
    }
    let tools = HostTools::discover().expect("host tools");
    if tools::find_optional("mkfs.xfs").is_none() {
        eprintln!("skipping: mkfs.xfs is missing; install xfsprogs");
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let volumes = state.join("volumes");
    assert_eq!(
        engine.capabilities().await.volume_snapshot_method,
        Some(SnapshotMethod::Reflink),
        "the pool makes the engine reflink"
    );
    assert_eq!(
        mounts_on(&volumes)
            .first()
            .map(|(fstype, _)| fstype.as_str()),
        Some("xfs"),
        "the volume directory is the pool's xfs mount"
    );
    let rootfs = prepare(&engine).await;

    let volume = VolumeId("vol_01JC9ZC2D3E4F5G6H7J8K9M0NP".to_string());
    let created = engine.ensure_volume(&volume, 1).await.expect("ensure");
    assert_eq!(created.size_gb, 1);
    assert!(
        created.allocated_bytes < 1 << 29,
        "the volume is sparse: {} bytes",
        created.allocated_bytes
    );

    let id = VmId("vm_w5a_pool".to_string());
    let mut spec = console_spec(&id, &rootfs, "10.77.0.17", "sleep 120", 17);
    spec.volumes = vec![VolumeAttach {
        volume_id: volume.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    // A volume mount travels in the v3 guest configuration, which carries the
    // exec port (§6, M5a).
    spec.vsock = Some(VsockAttach {
        cid: 3,
        socket: vm_dir(&id).join("vsock.sock"),
    });
    engine.create(&spec).await.expect("create with a volume");
    start_vm(&engine, &id).await;

    // Write through the mounted volume, so the snapshot has data to clone.
    let written = exec_in_guest(
        &engine,
        &id,
        &["sh", "-c", "printf quoro-pool > /data/marker && sync"],
    )
    .await;
    assert!(written.is_empty(), "writing the marker printed {written:?}");

    let snapshot = engine
        .snapshot_volume(&volume)
        .await
        .expect("snapshot a running volume");
    assert_eq!(snapshot.method, SnapshotMethod::Reflink);
    assert_eq!(
        snapshot.pause,
        Duration::ZERO,
        "a reflink snapshot never pauses the VM"
    );
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Running,
        "the VM is running after the snapshot"
    );

    // A second snapshot that no record names: the next start-up sweep must
    // remove it and keep the recorded one (§3.16).
    let unrecorded = engine
        .snapshot_volume(&volume)
        .await
        .expect("a second snapshot");
    assert!(unrecorded.location.exists());

    // A new engine on the same state directory reuses the mounted pool, and
    // its sweep keeps what the node agent's records name.
    drop(engine);
    let kept = snapshot_file_name(&snapshot);
    let engine = new_engine_with(SnapshotSweep::Keep([kept.clone()].into_iter().collect())).await;
    assert_eq!(
        engine.capabilities().await.volume_snapshot_method,
        Some(SnapshotMethod::Reflink)
    );
    assert_eq!(
        mounts_on(&volumes).len(),
        1,
        "the pool is mounted once, not twice"
    );
    assert!(created.path.exists(), "the volume is still in the pool");
    assert!(
        snapshot.location.exists(),
        "the recorded snapshot is still in the pool"
    );
    assert!(
        !unrecorded.location.exists(),
        "the start-up sweep removed the snapshot no record names"
    );
    assert_eq!(
        engine.snapshots_at_start(),
        Some(&[kept].into_iter().collect()),
        "the engine reports the snapshots its sweep left"
    );

    engine.stop(&id, FORCE).await.expect("stop");
    engine.delete(&id).await.expect("delete");
    engine
        .delete_volume_snapshot(&snapshot)
        .await
        .expect("delete the snapshot");
    engine
        .delete_volume(&volume)
        .await
        .expect("delete the volume");
    drop(engine);

    ChEngine::detach_volume_pool(&state).expect("detach the pool");
    assert!(
        mounts_on(&volumes).is_empty(),
        "the volume directory is no longer a mount point"
    );
    let image = state.join("volume-pool.img");
    let losetup = tools::find_optional("losetup").expect("losetup is installed with mount");
    let mut remaining = String::new();
    for _ in 0..20 {
        remaining = tools::run(
            &losetup,
            &["-j", image.to_str().expect("the image path is UTF-8")],
        )
        .await
        .expect("losetup -j");
        if remaining.trim().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        remaining.trim().is_empty(),
        "the loop device is released: {remaining:?}"
    );

    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

/// M5b: the warm volume calls a replication round uses (§3.22). Two snapshots
/// of a running VM's volume are shipped into a second volume the way a target
/// does it: a full copy into an empty staging file, then the changed ranges
/// into a clone of the last good copy. The second volume ends up byte for byte
/// equal to the second snapshot. A running VM also pauses and resumes, and a
/// paused VM stops.
#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn warm_volume_calls() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;
    let method = engine
        .capabilities()
        .await
        .volume_snapshot_method
        .expect("the engine has volumes");

    let source = VolumeId("vol_01JD0WARM5SRC0000000000000".to_string());
    let target = VolumeId("vol_01JD0WARM5TGT0000000000000".to_string());
    engine.ensure_volume(&source, 1).await.expect("ensure");

    let id = VmId("vm_w5b_warm".to_string());
    let mut spec = console_spec(&id, &rootfs, "10.77.0.18", "sleep 300", 18);
    spec.volumes = vec![VolumeAttach {
        volume_id: source.clone(),
        mount_path: "/data".to_string(),
        serial: "quoro-vol-0".to_string(),
    }];
    spec.vsock = Some(VsockAttach {
        cid: 3,
        socket: vm_dir(&id).join("vsock.sock"),
    });
    engine.create(&spec).await.expect("create with a volume");
    start_vm(&engine, &id).await;

    // A paused VM still reads Running; pausing twice and resuming twice are
    // no-ops, and the guest answers again after the resume.
    engine.pause(&id).await.expect("pause");
    engine.pause(&id).await.expect("pausing a paused VM is Ok");
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Running,
        "a paused VM reads Running"
    );
    engine.resume(&id).await.expect("resume");
    engine
        .resume(&id)
        .await
        .expect("resuming a running VM is Ok");

    let written = exec_in_guest(
        &engine,
        &id,
        &[
            "sh",
            "-c",
            "dd if=/dev/urandom of=/data/one bs=1M count=8 2>/dev/null && sync",
        ],
    )
    .await;
    assert!(written.is_empty(), "the first write printed {written:?}");
    let first = engine.snapshot_volume(&source).await.expect("snapshot 1");

    let written = exec_in_guest(
        &engine,
        &id,
        &[
            "sh",
            "-c",
            "dd if=/dev/urandom of=/data/one bs=4k count=3 seek=100 conv=notrunc 2>/dev/null \
             && dd if=/dev/urandom of=/data/two bs=1M count=2 2>/dev/null && sync",
        ],
    )
    .await;
    assert!(written.is_empty(), "the second write printed {written:?}");
    let second = engine.snapshot_volume(&source).await.expect("snapshot 2");

    // A volume a VM writes to has no stable map.
    assert!(
        matches!(
            engine
                .volume_changed_ranges(&RangesOf::Volume(source.clone()), None)
                .await,
            Err(EngineError::Conflict(_))
        ),
        "the ranges of an attached volume are a Conflict"
    );

    let full = engine
        .volume_changed_ranges(&RangesOf::Snapshot(first.clone()), None)
        .await
        .expect("ranges without a base");
    assert!(full.full, "no base means a full copy");
    let delta = engine
        .volume_changed_ranges(&RangesOf::Snapshot(second.clone()), Some(&first))
        .await
        .expect("ranges against the first snapshot");
    assert!(!delta.full, "two snapshots of one volume compare");
    assert!(!delta.ranges.is_empty(), "the second write changed blocks");
    let shipped: u64 = delta.ranges.iter().map(|range| range.length).sum();
    eprintln!(
        "method {method:?}: full copy {} bytes in {} ranges, delta {shipped} bytes in {} ranges",
        full.ranges.iter().map(|range| range.length).sum::<u64>(),
        full.ranges.len(),
        delta.ranges.len()
    );
    if method == SnapshotMethod::Reflink {
        assert!(
            shipped < 64 << 20,
            "a reflink delta holds the written blocks, not the volume: {shipped} bytes"
        );
    }

    // Round 1: a full copy into an empty staging file.
    let staged = engine
        .stage_volume(&target, 1, &StageFrom::Empty)
        .await
        .expect("stage from empty");
    assert!(
        matches!(
            engine.stage_volume(&target, 1, &StageFrom::Empty).await,
            Err(EngineError::Conflict(_))
        ),
        "one staging file per volume"
    );
    copy_ranges(&first.location, &staged.location, &full.ranges);
    let committed = engine
        .commit_staged_volume(&staged)
        .await
        .expect("commit round 1");
    assert!(
        !staged.location.exists(),
        "the commit renames the staging file"
    );
    assert_same_bytes(&committed.path, &first.location);

    // Round 2: the delta into a clone of the last good copy.
    let staged = engine
        .stage_volume(&target, 1, &StageFrom::Volume)
        .await
        .expect("stage from the volume");
    copy_ranges(&second.location, &staged.location, &delta.ranges);
    // A discarded staging file leaves the last good copy alone.
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");
    assert_same_bytes(&committed.path, &first.location);
    let staged = engine
        .stage_volume(&target, 1, &StageFrom::Volume)
        .await
        .expect("stage again");
    copy_ranges(&second.location, &staged.location, &delta.ranges);
    let committed = engine
        .commit_staged_volume(&staged)
        .await
        .expect("commit round 2");
    assert_same_bytes(&committed.path, &second.location);

    // A volume a VM uses is never replaced under it.
    let staged = engine
        .stage_volume(&source, 1, &StageFrom::Snapshot(first.clone()))
        .await
        .expect("stage the attached volume from a snapshot");
    assert!(
        matches!(
            engine.commit_staged_volume(&staged).await,
            Err(EngineError::Conflict(_))
        ),
        "committing over an attached volume is a Conflict"
    );
    engine
        .discard_staged_volume(&staged)
        .await
        .expect("discard");

    // A paused VM writes nothing more: writes in flight when it paused have
    // landed, so two snapshots taken while it stays paused under a write
    // load are equal. A switchover's final snapshot relies on it.
    if method == SnapshotMethod::Reflink {
        let started = exec_in_guest(
            &engine,
            &id,
            &[
                "sh",
                "-c",
                "setsid sh -c 'while true; do dd if=/dev/urandom of=/data/load bs=64k count=64 \
                 conv=notrunc 2>/dev/null; sync; done' >/dev/null 2>&1 & echo started",
            ],
        )
        .await;
        assert!(
            started.contains("started"),
            "the writer printed {started:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        engine.pause(&id).await.expect("pause under a write load");
        let frozen = engine.snapshot_volume(&source).await.expect("snapshot");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let later = engine.snapshot_volume(&source).await.expect("snapshot");
        let drift = engine
            .volume_changed_ranges(&RangesOf::Snapshot(later.clone()), Some(&frozen))
            .await
            .expect("ranges between the two");
        assert!(
            drift.ranges.is_empty(),
            "the paused VM wrote after the pause: {:?}",
            drift.ranges
        );
        for snapshot in [&frozen, &later] {
            engine
                .delete_volume_snapshot(snapshot)
                .await
                .expect("delete a snapshot");
        }
    }

    // A paused VM cannot answer the power button: its graceful stop goes
    // straight to the shutdown instead of waiting out the timeout.
    engine.pause(&id).await.expect("pause before the stop");
    let started = std::time::Instant::now();
    engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout: Duration::from_secs(10),
                force_within: FORCE_WITHIN,
            },
        )
        .await
        .expect("stop a paused VM");
    let took = started.elapsed();
    eprintln!("a paused VM stopped in {took:?}");
    assert!(
        took < Duration::from_secs(10),
        "the stop of a paused VM waited out its graceful timeout: {took:?}"
    );
    assert_eq!(
        engine.inspect(&id).await.expect("inspect").state,
        VmState::Stopped
    );
    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);

    for snapshot in [&first, &second] {
        engine
            .delete_volume_snapshot(snapshot)
            .await
            .expect("delete a snapshot");
    }
    for volume in [&source, &target] {
        engine.delete_volume(volume).await.expect("delete a volume");
    }
    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

/// Copies `ranges` of `from` into the same offsets of `to`, as a target applies
/// block frames, then syncs `to`.
fn copy_ranges(from: &Path, to: &Path, ranges: &[quoro_engine::ByteRange]) {
    use std::os::unix::fs::FileExt as _;
    let source = std::fs::File::open(from).expect("open the snapshot");
    let target = std::fs::OpenOptions::new()
        .write(true)
        .open(to)
        .expect("open the staging file");
    let mut buf = vec![0_u8; 1 << 20];
    for range in ranges {
        let mut offset = range.offset;
        let end = range.offset + range.length;
        while offset < end {
            let len = usize::try_from((end - offset).min(1 << 20)).expect("a chunk fits");
            source
                .read_exact_at(&mut buf[..len], offset)
                .expect("read the snapshot");
            target
                .write_all_at(&buf[..len], offset)
                .expect("write the staging file");
            offset += len as u64;
        }
    }
    target.sync_all().expect("sync the staging file");
}

/// Asserts that two files hold the same bytes.
fn assert_same_bytes(a: &Path, b: &Path) {
    use std::io::Read as _;
    let mut left = std::fs::File::open(a).expect("open");
    let mut right = std::fs::File::open(b).expect("open");
    assert_eq!(
        left.metadata().expect("metadata").len(),
        right.metadata().expect("metadata").len(),
        "{} and {} differ in length",
        a.display(),
        b.display()
    );
    let mut l = vec![0_u8; 1 << 20];
    let mut r = vec![0_u8; 1 << 20];
    let mut offset = 0_u64;
    loop {
        let n = left.read(&mut l).expect("read");
        if n == 0 {
            break;
        }
        right.read_exact(&mut r[..n]).expect("read");
        assert!(
            l[..n] == r[..n],
            "{} and {} differ near offset {offset}",
            a.display(),
            b.display()
        );
        offset += n as u64;
    }
}

/// How long [`exec_in_guest`] waits for the guest's exec listener.
const EXEC_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Opens an exec session, waiting for the guest to accept one.
///
/// `start` returns once the VMM booted the guest, but the init binds its exec
/// listener only after it mounted the volumes and set up the network, just
/// before it starts the app, so an exec right after `start` is refused with
/// `Conflict` (§3.18). This retries that refusal with a short backoff until
/// [`EXEC_READY_TIMEOUT`]; any other error fails at once.
async fn open_exec(engine: &ChEngine, id: &VmId, argv: &[&str]) -> quoro_engine::ExecSession {
    let request = ExecRequest {
        argv: argv.iter().map(|arg| (*arg).to_string()).collect(),
        user: None,
        rows: 24,
        cols: 80,
    };
    let deadline = tokio::time::Instant::now() + EXEC_READY_TIMEOUT;
    let mut backoff = Duration::from_millis(100);
    loop {
        match engine.exec(id, &request).await {
            Ok(session) => return session,
            Err(EngineError::Conflict(reason))
                if reason.contains("does not accept exec sessions")
                    && tokio::time::Instant::now() + backoff < deadline =>
            {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
            Err(err) => panic!("exec in the guest: {err:?}"),
        }
    }
}

/// Runs `argv` in the guest over the exec channel and returns its output,
/// waiting first for the guest to accept exec sessions.
async fn exec_in_guest(engine: &ChEngine, id: &VmId, argv: &[&str]) -> String {
    let mut session = open_exec(engine, id, argv).await;
    let mut out = Vec::new();
    let mut buf = [0_u8; 1024];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "exec output timed out");
        match tokio::time::timeout(remaining, session.io.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Ok(Err(err)) => panic!("exec read: {err}"),
            Err(_) => panic!("exec output timed out"),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Reads serial bytes until `needle` appeared, returning everything read.
async fn read_serial(stream: &mut ByteStream, needle: &[u8], timeout: Duration) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "no serial bytes containing {:?} within {timeout:?}; saw {:?}",
            String::from_utf8_lossy(needle),
            String::from_utf8_lossy(&seen)
        );
        match tokio::time::timeout(remaining, poll_fn(|cx| stream.as_mut().poll_next(cx))).await {
            Ok(Some(chunk)) => {
                seen.extend_from_slice(&chunk);
                if seen.windows(needle.len()).any(|window| window == needle) {
                    return seen;
                }
            }
            Ok(None) => panic!(
                "the serial stream ended before {:?}; saw {:?}",
                String::from_utf8_lossy(needle),
                String::from_utf8_lossy(&seen)
            ),
            Err(_) => panic!(
                "no serial bytes containing {:?} within {timeout:?}; saw {:?}",
                String::from_utf8_lossy(needle),
                String::from_utf8_lossy(&seen)
            ),
        }
    }
}

/// M4: a VM with a serial socket and a vsock device streams its boot from the
/// engine's ring and runs `uname -a` over the hybrid vsock exec channel
/// (§3.18, §3.19).
#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn exec_and_serial_console() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    assert!(engine.capabilities().await.exec, "exec is supported");
    let rootfs = prepare(&engine).await;

    let id = VmId("vm_w4_term".to_string());
    let serial = serial_socket(&id);
    let mut spec = lab_spec(
        &id,
        &rootfs,
        ConsoleMode::Off,
        "10.77.0.15",
        "sleep 120",
        15,
    );
    spec.serial = SerialMode::Socket {
        path: serial.clone(),
    };
    spec.vsock = Some(VsockAttach {
        cid: 3,
        socket: vm_dir(&id).join("vsock.sock"),
    });
    engine.create(&spec).await.expect("create");
    start_vm(&engine, &id).await;

    // The engine's client is the serial socket's only reader; the ring holds
    // the whole boot.
    let mut console = engine.serial_output(&id, None).expect("serial output");
    let boot = read_serial(&mut console, b"app started", Duration::from_secs(60)).await;
    assert!(
        String::from_utf8_lossy(&boot).contains("app started"),
        "the boot output is in the ring: {:?}",
        String::from_utf8_lossy(&boot)
    );
    // A tail of the ring is a valid second, independent stream: the replay
    // arrives as one item of at most the requested 64 bytes.
    let mut tail = engine.serial_output(&id, Some(64)).expect("a tail stream");
    match tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| tail.as_mut().poll_next(cx)),
    )
    .await
    {
        Ok(Some(chunk)) => assert!(chunk.len() <= 64, "the tail is bounded: {}", chunk.len()),
        other => panic!("no tail replay: {other:?}"),
    }

    // Exec runs in the guest: `uname -a` names the guest kernel.
    let mut session = engine
        .exec(
            &id,
            &ExecRequest {
                argv: vec!["uname".to_string(), "-a".to_string()],
                user: None,
                rows: 24,
                cols: 80,
            },
        )
        .await
        .expect("exec");
    let mut out = Vec::new();
    let mut buf = [0_u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "exec output timed out");
        match tokio::time::timeout(remaining, session.io.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Ok(Err(err)) => panic!("exec read failed: {err}"),
            Err(_) => panic!("exec output timed out"),
        }
    }
    assert!(
        String::from_utf8_lossy(&out).contains("Linux"),
        "uname -a ran in the guest: {:?}",
        String::from_utf8_lossy(&out)
    );
    let code = tokio::time::timeout(Duration::from_secs(10), session.exit)
        .await
        .expect("the session ends")
        .expect("an exit code");
    assert_eq!(code, 0);

    // Read-only: the console accepts no input, and the stream ends on delete.
    engine.stop(&id, FORCE).await.expect("stop");
    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);
    assert!(!serial.exists(), "the serial socket survived the delete");
    // Deleting the VM ends every open stream (§3.19): a subscriber first gets
    // what was queued before the delete (here the exec session's own serial
    // lines, after "app started"), then the end of the stream.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut drained = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, poll_fn(|cx| console.as_mut().poll_next(cx))).await {
            Ok(None) => break,
            Ok(Some(chunk)) => drained.extend_from_slice(&chunk),
            Err(_) => panic!(
                "the serial stream did not end on delete; drained {:?}",
                String::from_utf8_lossy(&drained)
            ),
        }
    }

    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}

/// Freezes the VM's VMM with `SIGSTOP` and returns its pid. A frozen VMM
/// accepts API connections and answers none: a hung VMM.
async fn freeze(engine: &ChEngine, id: &VmId) -> u32 {
    let pid = engine
        .inspect(id)
        .await
        .expect("inspect")
        .pid
        .expect("a running VM has a pid");
    let status = std::process::Command::new("kill")
        .args(["-STOP", &pid.to_string()])
        .status()
        .expect("kill -STOP");
    assert!(status.success(), "kill -STOP {pid}");
    pid
}

/// #434 (contract §3.7 and §3.8): a forced stop kills a hung VMM within its
/// bound, finishes the kill even when its caller stops waiting, and does not
/// wait out a graceful stop that holds the VM; a bounded graceful stop and a
/// bounded start keep to their bounds.
#[tokio::test]
#[ignore = "needs /dev/kvm, root and the pinned artifacts on the lab host"]
async fn a_forced_stop_kills_a_hung_vmm_quickly_even_when_abandoned() {
    if !enter_lab() {
        return;
    }
    let _guard = Guard::new();
    let engine = new_engine().await;
    let rootfs = prepare(&engine).await;
    let id = VmId("vm_w434_hung".to_string());
    let spec = console_spec(&id, &rootfs, "10.77.0.19", "sleep 120", 19);
    engine.create(&spec).await.expect("create");

    // A frozen VMM is killed inside a fence's `T_force` of 300 ms.
    start_vm(&engine, &id).await;
    let pid = freeze(&engine, &id).await;
    let began = std::time::Instant::now();
    let within = Duration::from_millis(300);
    let stopped = engine
        .stop(&id, StopMode::Force { within })
        .await
        .expect("force stop");
    let took = began.elapsed();
    assert_eq!(stopped.state, VmState::Stopped);
    assert_eq!(stopped.pid, None);
    assert!(took < within, "the forced stop took {took:?}");
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "the VMM {pid} survived"
    );

    // The caller stops waiting after 50 ms; the kill still happens.
    start_vm(&engine, &id).await;
    let pid = freeze(&engine, &id).await;
    let abandoned = tokio::time::timeout(Duration::from_millis(50), engine.stop(&id, FORCE)).await;
    assert!(abandoned.is_err(), "the caller stopped waiting");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let info = engine.inspect(&id).await.expect("inspect");
        if info.state == VmState::Stopped {
            assert_eq!(info.pid, None);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still {info:?} 2 s after the caller gave up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "the VMM {pid} survived"
    );

    // A forced stop does not wait out a graceful stop that holds the VM: the
    // graceful stop hands it over at once.
    start_vm(&engine, &id).await;
    let pid = freeze(&engine, &id).await;
    let graceful = engine.stop(
        &id,
        StopMode::Graceful {
            timeout: Duration::from_secs(30),
            force_within: FORCE_WITHIN,
        },
    );
    let forced = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let began = std::time::Instant::now();
        let stopped = engine.stop(&id, FORCE).await;
        (stopped, began.elapsed())
    };
    let (graceful, (forced, took)) = tokio::join!(graceful, forced);
    assert_eq!(forced.expect("force stop").state, VmState::Stopped);
    assert_eq!(graceful.expect("graceful stop").state, VmState::Stopped);
    assert!(
        took < Duration::from_secs(1),
        "the forced stop behind a graceful one took {took:?}"
    );
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "the VMM {pid} survived"
    );

    // A graceful stop of a frozen VMM returns within its two bounds
    // (ADR 0243): the power button gets no answer, and the forced part kills.
    start_vm(&engine, &id).await;
    let pid = freeze(&engine, &id).await;
    let (timeout, force_within) = (Duration::from_millis(200), Duration::from_millis(300));
    let began = std::time::Instant::now();
    let stopped = engine
        .stop(
            &id,
            StopMode::Graceful {
                timeout,
                force_within,
            },
        )
        .await
        .expect("graceful stop");
    let took = began.elapsed();
    assert_eq!(stopped.state, VmState::Stopped);
    assert!(
        took < timeout + force_within,
        "the bounded graceful stop took {took:?}"
    );
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "the VMM {pid} survived"
    );

    // A start that overruns its bound returns `Timeout`, and what it started
    // is stopped (ADR 0242). The VM boots again afterwards.
    let began = std::time::Instant::now();
    let bound = Duration::from_millis(30);
    assert_eq!(engine.start(&id, bound).await, Err(EngineError::Timeout));
    assert!(began.elapsed() < bound + Duration::from_millis(50));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let info = engine.inspect(&id).await.expect("inspect");
        if info.state == VmState::Stopped && info.pid.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "still {info:?} 10 s after the start gave up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_no_vmm(&id);
    start_vm(&engine, &id).await;
    engine.stop(&id, FORCE).await.expect("force stop");

    engine.delete(&id).await.expect("delete");
    assert_no_leftovers(&id);
    let tools = HostTools::discover().expect("host tools");
    net::remove_network(&tools, BRIDGE)
        .await
        .expect("remove the network");
}
