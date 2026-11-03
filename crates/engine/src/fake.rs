// SPDX-License-Identifier: AGPL-3.0-only
//! The in-memory [`FakeEngine`] from `docs/contracts/engine-trait.md` section 7.
//!
//! It implements the same trait with no hypervisor: VMs are records, lifecycle
//! calls change their state, and `list` returns exactly what was created since
//! the instance started. Booting completes when `start` returns, so no test has
//! to wait for a machine that does not exist.
//!
//! Determinism: there are no threads and no background tasks. The only awaits
//! are the two explicit injections — a guest that ignores the power button
//! (`set_ignore_power_button`) and a slow VMM (`set_stop_delay`) — and they are
//! plain `tokio::time::sleep` calls, so they are deterministic under
//! `tokio::time::pause` / `#[tokio::test(start_paused = true)]`. Everything
//! else, including scheduled crashes, runs on the fake's own manual clock
//! (`advance`), not on wall-clock time.
//!
//! Injection hooks are documented on the methods that provide them. They exist
//! so every error path and every event the node agent must map is reachable:
//! `fail_next` returns an error once, `crash` and `schedule_crash` produce a
//! `Crashed` event, `set_console_echo` makes a console round-trip, and
//! `set_unkillable` makes every stop fail as a VMM that survives `SIGKILL`.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::mpsc;

use crate::{
    Capabilities, ConsoleMode, ConsoleStream, Engine, EngineError, EngineEvent, EventStream,
    HostProbe, ImageConfig, ImageDigest, ImageRef, LineStream, NodeNetwork, OutputLine,
    OutputStream, PreparedImage, StopMode, VmCounters, VmId, VmInfo, VmSpec, VmState, VolumeId,
    VolumeSnapshot,
};

/// Capacity of one event subscriber's channel. A subscriber that falls further
/// behind than this loses events and the loss is counted, never backpressured
/// into the engine (section 3.12).
const EVENT_CAPACITY: usize = 1024;

/// Capacity of one output subscriber's channel.
const LINE_CAPACITY: usize = 256;

/// Size of the in-memory console pipe.
const CONSOLE_BUFFER: usize = 4096;

/// An operation a test can make fail once. See [`FakeEngine::fail_next`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FakeOp {
    PrepareImage,
    Create,
    Start,
    Stop,
    Delete,
}

/// The fake engine's per-VM record.
struct VmRecord {
    spec: VmSpec,
    info: VmInfo,
    /// Injection: the guest does not react to the power button, so a graceful
    /// stop runs out its timeout and falls back to a forced stop.
    ignores_power_button: bool,
    /// Injection: every stop call takes this long before acting.
    stop_delay: Duration,
    /// Injection: the VMM process survives `SIGKILL`, so every stop fails and
    /// the VM keeps the state it had.
    unkillable: bool,
    /// Manual-clock deadline at which this VM crashes, if scheduled.
    scheduled_crash: Option<Duration>,
    /// The live output subscriber, if one exists.
    output: Option<mpsc::Sender<OutputLine>>,
    /// The far end of the console pipe; the client holds the other end.
    console_far: Option<DuplexStream>,
    /// Set while a console client is connected, cleared when it is dropped.
    console_open: Option<Arc<AtomicBool>>,
    console_echo: bool,
}

/// Everything the fake owns, behind one mutex. The lock is never held across
/// an await point.
struct State {
    images: HashMap<ImageDigest, PreparedImage>,
    /// Bridge name to the network the bridge was created with.
    networks: HashMap<String, NodeNetwork>,
    vms: HashMap<VmId, VmRecord>,
    snapshots: HashMap<String, VolumeSnapshot>,
    event_subscribers: Vec<mpsc::Sender<EngineEvent>>,
    dropped_events: u64,
    failures: HashMap<FakeOp, VecDeque<EngineError>>,
    stop_log: Vec<(VmId, StopMode)>,
    next_pid: u32,
    next_snapshot: u64,
    /// The fake's manual clock, in time since the instance was created.
    clock: Duration,
}

impl State {
    fn new() -> Self {
        Self {
            images: HashMap::new(),
            networks: HashMap::new(),
            vms: HashMap::new(),
            snapshots: HashMap::new(),
            event_subscribers: Vec::new(),
            dropped_events: 0,
            failures: HashMap::new(),
            stop_log: Vec::new(),
            next_pid: 1000,
            next_snapshot: 1,
            clock: Duration::ZERO,
        }
    }
}

/// A `Stream` over a bounded mpsc receiver.
///
/// Hand-written because the crate has no `tokio-stream` dependency: the
/// contract's stream types only need `futures_core::Stream`, which
/// `poll_recv` satisfies directly.
struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> ReceiverStream<T> {
    fn new(rx: mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}

/// The client end of a console pipe.
///
/// It exists to notice disconnection: dropping the stream clears the `open`
/// flag, so the next `console` call can accept a new client. `tokio::io::duplex`
/// gives no way to observe the far end's closure without reading, which is why
/// the flag is kept separately.
struct ConsoleClient {
    inner: DuplexStream,
    open: Arc<AtomicBool>,
}

impl Drop for ConsoleClient {
    fn drop(&mut self) {
        self.open.store(false, Ordering::SeqCst);
    }
}

impl AsyncRead for ConsoleClient {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ConsoleClient {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// An in-memory [`Engine`] for tests. See the module documentation.
pub struct FakeEngine {
    state: Mutex<State>,
}

impl Default for FakeEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeEngine {
    /// Creates an empty fake engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic before a write could poison the lock; the state is still
        // consistent enough to inspect, so recover rather than propagate.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Makes the next call of `op` return `err` once. Further calls behave
    /// normally. This reaches the error paths section 3 defines.
    pub fn fail_next(&self, op: FakeOp, err: EngineError) {
        self.lock().failures.entry(op).or_default().push_back(err);
    }

    fn take_failure(&self, op: FakeOp) -> Option<EngineError> {
        self.lock().failures.get_mut(&op)?.pop_front()
    }

    /// The stop calls this engine has received, in order.
    #[must_use]
    pub fn stop_calls(&self) -> Vec<(VmId, StopMode)> {
        self.lock().stop_log.clone()
    }

    /// How many events were dropped because a subscriber's buffer was full.
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.lock().dropped_events
    }

    /// Injection: makes the guest ignore the power button, so a graceful stop
    /// waits for `timeout` and then falls back to a forced stop (exit 137).
    /// With `false` a graceful stop exits the guest immediately with code 0.
    pub fn set_ignore_power_button(&self, id: &VmId, ignores: bool) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.ignores_power_button = ignores;
        }
    }

    /// Injection: models a slow VMM. Every stop call on this VM awaits `delay`
    /// before acting; the default is zero. The sleep is deterministic under a
    /// paused tokio clock.
    pub fn set_stop_delay(&self, id: &VmId, delay: Duration) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.stop_delay = delay;
        }
    }

    /// Injection: every `stop` call for this VM, graceful or forced, fails with
    /// `EngineError::Hypervisor("VMM process did not exit after SIGKILL")` and
    /// the VM keeps its state (a `Running` VM stays `Running`, keeps its pid),
    /// as when the VMM process cannot be killed. `inspect` keeps reporting the
    /// real state.
    pub fn set_unkillable(&self, id: &VmId, unkillable: bool) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.unkillable = unkillable;
        }
    }

    /// Injection: pushes one tagged output line to the VM's output subscriber.
    /// Lines pushed when nobody reads are discarded.
    pub fn push_output(&self, id: &VmId, stream: OutputStream, line: &str) {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return;
        };
        let Some(sender) = record.output.as_mut() else {
            return;
        };
        let _ = sender.try_send(OutputLine {
            stream,
            time: SystemTime::now(),
            line: line.to_string(),
        });
    }

    /// Injection: echoes console input back to the client. Echo is performed by
    /// [`FakeEngine::pump_console`], not by a task.
    pub fn set_console_echo(&self, id: &VmId, echo: bool) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.console_echo = echo;
        }
    }

    /// Moves bytes the client wrote to the far end of the console pipe back to
    /// it when echo is on, and returns how many bytes were read.
    ///
    /// The fake runs no tasks, so the caller drives the loop: it awaits the
    /// far end once, which is immediate when the client has written something.
    /// Call it after writing; it is not a background pump.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from reading or writing the console pipe.
    pub async fn pump_console(&self, id: &VmId) -> io::Result<usize> {
        let (mut far, echo) = {
            let mut state = self.lock();
            let Some(record) = state.vms.get_mut(id) else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no such VM in the fake engine",
                ));
            };
            match record.console_far.take() {
                Some(far) => (far, record.console_echo),
                None => return Ok(0),
            }
        };

        let mut buf = [0u8; CONSOLE_BUFFER];
        let outcome = match far.read(&mut buf).await {
            Ok(n) if echo && n > 0 => far.write_all(&buf[..n]).await.map(|()| n),
            Ok(n) => Ok(n),
            Err(err) => Err(err),
        };

        if let Some(record) = self.lock().vms.get_mut(id) {
            record.console_far = Some(far);
        }
        outcome
    }

    /// Injection: crashes a running VM now with `reason`, emitting
    /// [`EngineEvent::Crashed`]. Does nothing to a VM that is not running.
    pub fn crash(&self, id: &VmId, reason: &str) {
        let mut state = self.lock();
        crash_locked(&mut state, id, reason);
    }

    /// Injection: crashes the VM when the fake's manual clock reaches
    /// `after` from now. The clock only moves with [`FakeEngine::advance`];
    /// no wall-clock timer is involved.
    pub fn schedule_crash(&self, id: &VmId, after: Duration) {
        let mut state = self.lock();
        let deadline = state.clock + after;
        if let Some(record) = state.vms.get_mut(id) {
            record.scheduled_crash = Some(deadline);
        }
    }

    /// Advances the fake's manual clock by `by` and crashes every running VM
    /// whose scheduled crash deadline has passed.
    pub fn advance(&self, by: Duration) {
        let mut state = self.lock();
        state.clock += by;
        let clock = state.clock;
        let due: Vec<VmId> = state
            .vms
            .iter()
            .filter(|(_, record)| {
                record.info.state == VmState::Running
                    && record.scheduled_crash.is_some_and(|at| at <= clock)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            crash_locked(&mut state, &id, "scheduled crash");
        }
    }
}

/// Emits an event to every live subscriber, counting the ones that could not
/// be delivered to a full buffer and removing the closed ones.
fn emit(state: &mut State, event: EngineEvent) {
    let mut dropped = 0u64;
    state
        .event_subscribers
        .retain(|sender| match sender.try_send(event.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                dropped += 1;
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
    state.dropped_events += dropped;
}

/// Crashes one VM if it is running. Callers hold the lock.
fn crash_locked(state: &mut State, id: &VmId, reason: &str) {
    let Some(record) = state.vms.get_mut(id) else {
        return;
    };
    if record.info.state != VmState::Running {
        return;
    }
    record.info.state = VmState::Crashed;
    record.info.pid = None;
    record.scheduled_crash = None;
    emit(
        state,
        EngineEvent::Crashed {
            id: id.clone(),
            reason: reason.to_string(),
        },
    );
}

impl Engine for FakeEngine {
    async fn capabilities(&self) -> Capabilities {
        Capabilities {
            engine: "fake".to_string(),
            version: "0".to_string(),
            kvm: false,
            text_console: true,
            graphical_console: false,
            vm_snapshot: false,
            volume_snapshot: true,
        }
    }

    async fn probe_host(&self) -> Result<HostProbe, EngineError> {
        Ok(HostProbe {
            kvm_available: false,
            kernel_release: "fake".to_string(),
            kernel_supported: true,
            cpu_vmx: false,
            cpu_svm: false,
            nested: false,
        })
    }

    async fn ensure_network(&self, net: &NodeNetwork) -> Result<(), EngineError> {
        let mut state = self.lock();
        match state.networks.get(&net.bridge) {
            // `dns` is not part of the network's identity (contract §3.3): the
            // same bridge, subnet and gateway with other servers is an update.
            Some(existing) if existing.subnet == net.subnet && existing.gateway == net.gateway => {
                state.networks.insert(net.bridge.clone(), net.clone());
                Ok(())
            }
            Some(existing) => Err(EngineError::Conflict(format!(
                "bridge {} already exists with subnet {} and gateway {}",
                existing.bridge, existing.subnet, existing.gateway
            ))),
            None => {
                state.networks.insert(net.bridge.clone(), net.clone());
                Ok(())
            }
        }
    }

    async fn prepare_image(&self, image: &ImageRef) -> Result<PreparedImage, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::PrepareImage) {
            return Err(err);
        }
        let mut state = self.lock();
        if let Some(cached) = state.images.get(&image.digest) {
            return Ok(cached.clone());
        }
        let prepared = PreparedImage {
            digest: image.digest.clone(),
            rootfs_path: PathBuf::from(format!("/fake/images/{}", image.digest.0)),
            size_bytes: 0,
            config: ImageConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
                exposed_ports: Vec::new(),
            },
        };
        state.images.insert(image.digest.clone(), prepared.clone());
        Ok(prepared)
    }

    async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        self.lock().images.remove(digest);
        Ok(())
    }

    async fn create(&self, spec: &VmSpec) -> Result<VmInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Create) {
            return Err(err);
        }
        let mut state = self.lock();
        if !state.images.contains_key(&spec.rootfs) {
            return Err(EngineError::Image(format!(
                "rootfs {} is not a prepared image",
                spec.rootfs.0
            )));
        }
        if let Some(existing) = state.vms.get(&spec.id) {
            if existing.spec.canonical() == spec.canonical() {
                return Ok(existing.info.clone());
            }
            return Err(EngineError::Conflict(format!(
                "VM {} already exists with a different spec",
                spec.id.0
            )));
        }
        let info = VmInfo {
            id: spec.id.clone(),
            state: VmState::Stopped,
            pid: None,
            started_at: None,
            exit_code: None,
        };
        state.vms.insert(
            spec.id.clone(),
            VmRecord {
                spec: spec.clone(),
                info: info.clone(),
                ignores_power_button: false,
                stop_delay: Duration::ZERO,
                unkillable: false,
                scheduled_crash: None,
                output: None,
                console_far: None,
                console_open: None,
                console_echo: false,
            },
        );
        Ok(info)
    }

    async fn start(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Start) {
            return Err(err);
        }
        let mut state = self.lock();
        let Some(record) = state.vms.get(id) else {
            return Err(EngineError::NotFound);
        };
        if record.info.state == VmState::Running {
            return Ok(record.info.clone());
        }
        state.next_pid += 1;
        let pid = state.next_pid;
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        record.info.state = VmState::Running;
        record.info.pid = Some(pid);
        record.info.started_at = Some(SystemTime::now());
        record.info.exit_code = None;
        record.scheduled_crash = None;
        let info = record.info.clone();
        emit(&mut state, EngineEvent::Started { id: id.clone() });
        Ok(info)
    }

    async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Stop) {
            return Err(err);
        }

        let (delay, ignores, unkillable, already_stopped) = {
            let mut state = self.lock();
            let Some(record) = state.vms.get(id) else {
                return Err(EngineError::NotFound);
            };
            let snapshot = (
                record.stop_delay,
                record.ignores_power_button,
                record.unkillable,
                record.info.state == VmState::Stopped,
            );
            state.stop_log.push((id.clone(), mode.clone()));
            snapshot
        };
        if unkillable {
            return Err(EngineError::Hypervisor(
                "VMM process did not exit after SIGKILL".into(),
            ));
        }
        if already_stopped {
            return self.inspect(id).await;
        }

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let graceful = matches!(mode, StopMode::Graceful { .. });
        if let StopMode::Graceful { timeout } = mode
            && ignores
        {
            tokio::time::sleep(timeout).await;
        }

        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if record.info.state == VmState::Stopped {
            return Ok(record.info.clone());
        }
        // A guest that did not react to the power button is stopped by force.
        let code = if graceful && !ignores { 0 } else { 137 };
        record.info.state = VmState::Stopped;
        record.info.pid = None;
        record.info.exit_code = Some(code);
        let info = record.info.clone();
        emit(
            &mut state,
            EngineEvent::Exited {
                id: id.clone(),
                code,
            },
        );
        Ok(info)
    }

    async fn delete(&self, id: &VmId) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Delete) {
            return Err(err);
        }
        let mut state = self.lock();
        let Some(record) = state.vms.get(id) else {
            return Ok(());
        };
        if record.info.state == VmState::Running {
            return Err(EngineError::Conflict(format!("VM {} is running", id.0)));
        }
        state.vms.remove(id);
        Ok(())
    }

    async fn inspect(&self, id: &VmId) -> Result<VmInfo, EngineError> {
        self.lock()
            .vms
            .get(id)
            .map(|record| record.info.clone())
            .ok_or(EngineError::NotFound)
    }

    async fn list(&self) -> Result<Vec<VmInfo>, EngineError> {
        let mut infos: Vec<VmInfo> = self
            .lock()
            .vms
            .values()
            .map(|record| record.info.clone())
            .collect();
        infos.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        Ok(infos)
    }

    fn events(&self) -> EventStream {
        let (sender, receiver) = mpsc::channel(EVENT_CAPACITY);
        self.lock().event_subscribers.push(sender);
        Box::pin(ReceiverStream::new(receiver))
    }

    fn output(&self, id: &VmId) -> Result<LineStream, EngineError> {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if matches!(record.spec.console, ConsoleMode::Off) {
            return Err(EngineError::NotFound);
        }
        if record.output.as_ref().is_some_and(|tx| !tx.is_closed()) {
            return Err(EngineError::Conflict(format!(
                "VM {} already has an output reader",
                id.0
            )));
        }
        let (sender, receiver) = mpsc::channel(LINE_CAPACITY);
        record.output = Some(sender);
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    fn console(&self, id: &VmId) -> Result<ConsoleStream, EngineError> {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if matches!(record.spec.console, ConsoleMode::Off) {
            return Err(EngineError::NotFound);
        }
        let connected = record
            .console_open
            .as_ref()
            .is_some_and(|open| open.load(Ordering::SeqCst));
        if connected {
            return Err(EngineError::Conflict(format!(
                "VM {} already has a console client",
                id.0
            )));
        }
        let (client, far) = tokio::io::duplex(CONSOLE_BUFFER);
        let open = Arc::new(AtomicBool::new(true));
        record.console_far = Some(far);
        record.console_open = Some(Arc::clone(&open));
        Ok(Box::pin(ConsoleClient {
            inner: client,
            open,
        }))
    }

    async fn counters(&self, id: &VmId) -> Result<VmCounters, EngineError> {
        if !self.lock().vms.contains_key(id) {
            return Err(EngineError::NotFound);
        }
        Ok(VmCounters {
            cpu_time_ms: 0,
            memory_rss_bytes: 0,
            net_rx_bytes: 0,
            net_tx_bytes: 0,
            block_read_bytes: 0,
            block_write_bytes: 0,
        })
    }

    async fn snapshot_volume(&self, id: &VolumeId) -> Result<VolumeSnapshot, EngineError> {
        let mut state = self.lock();
        let n = state.next_snapshot;
        state.next_snapshot += 1;
        let snapshot = VolumeSnapshot {
            volume_id: id.clone(),
            snapshot_id: format!("snap-{n}"),
            created_at: SystemTime::now(),
            size_bytes: 0,
            location: PathBuf::from(format!("/fake/snapshots/{n}")),
        };
        state
            .snapshots
            .insert(snapshot.snapshot_id.clone(), snapshot.clone());
        Ok(snapshot)
    }

    async fn delete_volume_snapshot(&self, snap: &VolumeSnapshot) -> Result<(), EngineError> {
        self.lock().snapshots.remove(&snap.snapshot_id);
        Ok(())
    }
}
