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
    InitConfig, LineStream, MacAddr, NetAttach, NodeNetwork, OutputLine, OutputStream, SerialMode,
    StopMode, VmId, VmSpec, VmState, VsockAttach,
};
use tokio::io::AsyncReadExt as _;

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
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("wipe the state directory");
    }
    std::fs::create_dir_all(&dir).expect("create the state directory");
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
    let needle = dir.to_string_lossy().into_owned();
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
    let started = engine.start(id).await.expect("start");
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
    let forced = engine.stop(&id, StopMode::Force).await.expect("force stop");
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

    engine.stop(&a, StopMode::Force).await.expect("stop A");
    engine.stop(&b, StopMode::Force).await.expect("stop B");
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

    let stopped = engine.stop(&id, StopMode::Force).await.expect("stop");
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
        .stop(&builder, StopMode::Force)
        .await
        .expect("stop the builder");
    engine
        .stop(&peer, StopMode::Force)
        .await
        .expect("stop the peer");
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
    engine.stop(&id, StopMode::Force).await.expect("stop");
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
