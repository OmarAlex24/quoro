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
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, oneshot};

use crate::{ByteRange, ChangedRanges};
use crate::{
    ByteStream, Capabilities, ConsoleMode, ConsoleStream, Engine, EngineError, EngineEvent,
    EventStream, ExecRequest, ExecSession, HostProbe, ImageConfig, ImageDigest, ImageInventory,
    ImageRef, LineStream, NodeNetwork, OciLayout, OutputLine, OutputStream, PreparedImage,
    RangesOf, SerialMode, SnapshotMethod, SnapshotSweep, StageFrom, StagedVolume, StopMode,
    VmCounters, VmId, VmInfo, VmSpec, VmState, VolumeId, VolumeInfo, VolumeSnapshot,
};

/// Bytes in one gibibyte, the unit of `size_gb`.
const GIB: u64 = 1 << 30;

/// Capacity of one event subscriber's channel. A subscriber that falls further
/// behind than this loses events and the loss is counted, never backpressured
/// into the engine (section 3.12).
const EVENT_CAPACITY: usize = 1024;

/// Capacity of one output subscriber's channel.
const LINE_CAPACITY: usize = 256;

/// Size of the in-memory console pipe.
const CONSOLE_BUFFER: usize = 4096;

/// Bytes an exec session buffers before its writer waits for a reader, so the
/// fake shows the same back-pressure a real PTY does.
const EXEC_BUFFER: usize = 64 * 1024;

/// The most serial output the fake keeps per VM, like the real ring (1 MiB).
const SERIAL_RING: usize = 1 << 20;

/// Capacity, in chunks, of one serial-output subscriber's channel.
const SERIAL_CAPACITY: usize = 256;

/// An operation a test can make fail once. See [`FakeEngine::fail_next`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FakeOp {
    PrepareImage,
    Create,
    Start,
    Stop,
    Delete,
    /// `ensure_volume` (M5a).
    EnsureVolume,
    /// `import_volume` (M5a).
    ImportVolume,
    /// `delete_volume` (M5a).
    DeleteVolume,
    /// `snapshot_volume` (M5a).
    SnapshotVolume,
    /// `inventory_cached_image` (M5a).
    InventoryCachedImage,
    /// `list` (M5b): the whole listing fails, as when the real engine cannot
    /// read one VM's state.
    List,
    /// `volume_changed_ranges` (M5b).
    VolumeChangedRanges,
    /// `stage_volume` (M5b).
    StageVolume,
    /// `commit_staged_volume` (M5b).
    CommitStagedVolume,
    /// `pause` (M5b).
    Pause,
    /// `resume` (M5b).
    Resume,
}

/// A volume the fake holds (M5a). With a volume directory set
/// ([`FakeEngine::set_volume_dir`]) it is a real sparse file there, so a test
/// can write into it, snapshot it and read the snapshot back; without one it
/// is a record only.
struct FakeVolume {
    size_gb: u32,
    path: PathBuf,
}

/// A staging file the fake holds for a volume (M5b, section 3.22): a real
/// file next to the volume files when a volume directory is set, a record
/// otherwise.
struct FakeStaged {
    size_gb: u32,
    location: PathBuf,
}

/// How `volume_changed_ranges` answers when no result was pushed with
/// [`FakeEngine::push_changed_ranges`] (M5b).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FakeRanges {
    /// The whole volume as one range, with `full: true` (section 7).
    #[default]
    Full,
    /// With a base and a volume directory: the 4 KiB blocks whose bytes
    /// differ between the two files, with `full: false`. Without a base, or
    /// without a volume directory, the whole volume as in `Full`.
    Content,
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
    /// The exec sessions started on this VM, oldest first (section 3.18).
    execs: Vec<FakeExec>,
    /// The serial output kept for `serial_output` replays (section 3.19).
    serial_ring: VecDeque<u8>,
    /// Live `serial_output` subscribers; dropped when the VM is deleted.
    serial_subscribers: Vec<mpsc::Sender<Vec<u8>>>,
    /// Set by `pause`, cleared by `resume` and whenever the VM stops running
    /// (M5b, section 3.22). A paused VM still reads `Running`.
    paused: bool,
}

/// The fake's side of one exec session.
struct FakeExec {
    request: ExecRequest,
    shared: Arc<Mutex<ExecPipe>>,
    resize: mpsc::Receiver<(u16, u16)>,
    exit: Option<oneshot::Sender<i32>>,
}

/// The loopback behind an exec session's `io`: what the client writes is read
/// back (echo), output a test pushes is read too, and once the session ended
/// a read returns end of file after the buffer drains.
#[derive(Default)]
struct ExecPipe {
    buf: VecDeque<u8>,
    ended: bool,
    /// Set when the client dropped or shut down its end.
    closed: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

impl ExecPipe {
    fn wake_all(&mut self) {
        if let Some(waker) = self.reader.take() {
            waker.wake();
        }
        if let Some(waker) = self.writer.take() {
            waker.wake();
        }
    }
}

/// The client end of a fake exec session.
struct ExecIo {
    shared: Arc<Mutex<ExecPipe>>,
}

impl ExecIo {
    fn pipe(&self) -> MutexGuard<'_, ExecPipe> {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for ExecIo {
    fn drop(&mut self) {
        let mut pipe = self.pipe();
        pipe.closed = true;
        pipe.wake_all();
    }
}

impl AsyncRead for ExecIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut pipe = self.pipe();
        if pipe.buf.is_empty() {
            if pipe.ended {
                return Poll::Ready(Ok(()));
            }
            pipe.reader = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = buf.remaining().min(pipe.buf.len());
        let chunk: Vec<u8> = pipe.buf.drain(..n).collect();
        buf.put_slice(&chunk);
        if let Some(waker) = pipe.writer.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ExecIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut pipe = self.pipe();
        if pipe.ended || pipe.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the exec session ended",
            )));
        }
        let room = EXEC_BUFFER.saturating_sub(pipe.buf.len());
        if room == 0 {
            pipe.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = room.min(data.len());
        pipe.buf.extend(&data[..n]);
        if let Some(waker) = pipe.reader.take() {
            waker.wake();
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut pipe = self.pipe();
        pipe.closed = true;
        pipe.wake_all();
        Poll::Ready(Ok(()))
    }
}

/// Ends every exec session of a VM: readers see end of file once drained, and
/// `exit` resolves with an error because no exit code was reported.
fn end_execs(record: &mut VmRecord) {
    for exec in &mut record.execs {
        exec.exit = None;
        let mut pipe = exec
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pipe.ended = true;
        pipe.wake_all();
    }
}

/// Everything the fake owns, behind one mutex. The lock is never held across
/// an await point.
struct State {
    images: HashMap<ImageDigest, PreparedImage>,
    /// Inventories set by tests (M5a). `Some(None)` makes the image behave
    /// as prepared before M5a (no inventory); a prepared image without an
    /// entry has an empty inventory.
    inventories: HashMap<ImageDigest, Option<ImageInventory>>,
    /// What `inventory_cached_image` builds for an image prepared before
    /// M5a; an empty inventory when unset (M5a).
    backfills: HashMap<ImageDigest, ImageInventory>,
    /// Digests `inventory_cached_image` read a root disk for, in order.
    backfilled: Vec<ImageDigest>,
    /// Bridge name to the network the bridge was created with.
    networks: HashMap<String, NodeNetwork>,
    vms: HashMap<VmId, VmRecord>,
    snapshots: HashMap<String, VolumeSnapshot>,
    volumes: HashMap<VolumeId, FakeVolume>,
    /// The staging file of each volume that has one (M5b, section 3.22).
    staged: HashMap<VolumeId, FakeStaged>,
    /// Results `volume_changed_ranges` returns next, oldest first (M5b).
    changed_ranges: VecDeque<ChangedRanges>,
    /// How `volume_changed_ranges` answers otherwise (M5b).
    ranges_mode: FakeRanges,
    /// Where volume files live; `None` keeps volumes as records only.
    volume_dir: Option<PathBuf>,
    snapshot_method: SnapshotMethod,
    /// The pause a `PauseCopy` snapshot reports while a running VM attaches
    /// the volume.
    snapshot_pause: Duration,
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
            inventories: HashMap::new(),
            backfills: HashMap::new(),
            backfilled: Vec::new(),
            networks: HashMap::new(),
            vms: HashMap::new(),
            snapshots: HashMap::new(),
            volumes: HashMap::new(),
            staged: HashMap::new(),
            changed_ranges: VecDeque::new(),
            ranges_mode: FakeRanges::Full,
            volume_dir: None,
            snapshot_method: SnapshotMethod::Reflink,
            snapshot_pause: Duration::ZERO,
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

    /// Sets the inventory `image_inventory` returns for `inventory.digest`
    /// once that image is prepared (M5a).
    pub fn set_inventory(&self, inventory: ImageInventory) {
        self.lock()
            .inventories
            .insert(inventory.digest.clone(), Some(inventory));
    }

    /// Makes `digest` behave like an image prepared before M5a: once
    /// prepared, `image_inventory` is `NotFound`.
    pub fn clear_inventory(&self, digest: &ImageDigest) {
        self.lock().inventories.insert(digest.clone(), None);
    }

    /// Sets the inventory `inventory_cached_image` builds for
    /// `inventory.digest` when that image has none (M5a, §3.21).
    pub fn set_backfill_inventory(&self, inventory: ImageInventory) {
        self.lock()
            .backfills
            .insert(inventory.digest.clone(), inventory);
    }

    /// The digests `inventory_cached_image` read a root disk for, in order:
    /// an image that already had an inventory is not in it.
    #[must_use]
    pub fn backfilled(&self) -> Vec<ImageDigest> {
        self.lock().backfilled.clone()
    }

    /// Keeps volume files in `dir` as real sparse files (M5a). Snapshots are
    /// copies next to them. Without this, volumes are records only and a
    /// snapshot's `location` names no file.
    pub fn set_volume_dir(&self, dir: impl Into<PathBuf>) {
        self.lock().volume_dir = Some(dir.into());
    }

    /// The method `capabilities` reports and `snapshot_volume` uses, and the
    /// pause a `PauseCopy` snapshot of a volume a running VM attaches reports.
    /// The default is `Reflink` with no pause.
    pub fn set_snapshot_method(&self, method: SnapshotMethod, pause: Duration) {
        let mut state = self.lock();
        state.snapshot_method = method;
        state.snapshot_pause = pause;
    }

    /// The volumes this engine holds, with their sizes, sorted by id.
    #[must_use]
    pub fn volumes(&self) -> Vec<(VolumeId, u32)> {
        let mut volumes: Vec<_> = self
            .lock()
            .volumes
            .iter()
            .map(|(id, volume)| (id.clone(), volume.size_gb))
            .collect();
        volumes.sort_by(|a, b| a.0.0.cmp(&b.0.0));
        volumes
    }

    /// The snapshots that exist now (taken and not deleted).
    #[must_use]
    pub fn volume_snapshots(&self) -> Vec<VolumeSnapshot> {
        self.lock().snapshots.values().cloned().collect()
    }

    /// Makes the next `volume_changed_ranges` return `ranges` once, after
    /// its checks of the files passed (M5b). Pushed results are returned
    /// oldest first.
    pub fn push_changed_ranges(&self, ranges: ChangedRanges) {
        self.lock().changed_ranges.push_back(ranges);
    }

    /// How `volume_changed_ranges` answers when no result was pushed (M5b).
    /// The default is [`FakeRanges::Full`].
    pub fn set_changed_ranges_mode(&self, mode: FakeRanges) {
        self.lock().ranges_mode = mode;
    }

    /// The staging files that exist now, by volume, sorted by volume id
    /// (M5b).
    #[must_use]
    pub fn staged_volumes(&self) -> Vec<(VolumeId, PathBuf)> {
        let mut staged: Vec<_> = self
            .lock()
            .staged
            .iter()
            .map(|(id, staged)| (id.clone(), staged.location.clone()))
            .collect();
        staged.sort_by(|a, b| a.0.0.cmp(&b.0.0));
        staged
    }

    /// What the real engine does to the volume directory when it starts
    /// (section 3.16): removes every staging file and every snapshot that
    /// `sweep` does not keep, by `snapshot_id`. A test calls it to model an
    /// agent restart, as the fake itself outlives one.
    ///
    /// # Errors
    ///
    /// `Io` when a file of a removed snapshot or staging copy cannot be
    /// removed.
    pub fn start_up_sweep(&self, sweep: &SnapshotSweep) -> Result<(), EngineError> {
        let mut state = self.lock();
        let has_files = state.volume_dir.is_some();
        let mut removed: Vec<PathBuf> = Vec::new();
        state.snapshots.retain(|id, snapshot| {
            let keep = sweep.keeps(id);
            if !keep {
                removed.push(snapshot.location.clone());
            }
            keep
        });
        removed.extend(state.staged.drain().map(|(_, staged)| staged.location));
        if has_files {
            for path in removed {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_error(err)),
                }
            }
        }
        Ok(())
    }

    /// Whether `pause` froze the VM and nothing resumed or stopped it since
    /// (M5b). An unknown VM is not paused.
    #[must_use]
    pub fn is_paused(&self, id: &VmId) -> bool {
        self.lock()
            .vms
            .get(id)
            .is_some_and(|record| record.paused && record.info.state == VmState::Running)
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

    /// The requests of the exec sessions started on a VM, oldest first.
    #[must_use]
    pub fn exec_requests(&self, id: &VmId) -> Vec<ExecRequest> {
        self.lock()
            .vms
            .get(id)
            .map(|record| record.execs.iter().map(|e| e.request.clone()).collect())
            .unwrap_or_default()
    }

    /// The resizes the `index`-th exec session of a VM received so far, in
    /// order. Each call returns only the resizes since the previous call.
    #[must_use]
    pub fn exec_resizes(&self, id: &VmId, index: usize) -> Vec<(u16, u16)> {
        let mut state = self.lock();
        let Some(exec) = state
            .vms
            .get_mut(id)
            .and_then(|record| record.execs.get_mut(index))
        else {
            return Vec::new();
        };
        let mut out = Vec::new();
        while let Ok(size) = exec.resize.try_recv() {
            out.push(size);
        }
        out
    }

    /// Whether the client of the `index`-th exec session of a VM dropped or
    /// shut down its end.
    #[must_use]
    pub fn exec_closed(&self, id: &VmId, index: usize) -> bool {
        self.lock()
            .vms
            .get(id)
            .and_then(|record| record.execs.get(index))
            .is_some_and(|exec| {
                exec.shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .closed
            })
    }

    /// Injection: the program of the `index`-th exec session of a VM writes
    /// `bytes` to its terminal.
    pub fn push_exec_output(&self, id: &VmId, index: usize, bytes: &[u8]) {
        let state = self.lock();
        let Some(exec) = state.vms.get(id).and_then(|record| record.execs.get(index)) else {
            return;
        };
        let mut pipe = exec
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pipe.buf.extend(bytes);
        if let Some(waker) = pipe.reader.take() {
            waker.wake();
        }
    }

    /// Injection: the program of the `index`-th exec session of a VM exits
    /// with `code`. Its `exit` resolves and its `io` reaches end of file once
    /// the buffered output is read.
    pub fn exit_exec(&self, id: &VmId, index: usize, code: i32) {
        let mut state = self.lock();
        let Some(exec) = state
            .vms
            .get_mut(id)
            .and_then(|record| record.execs.get_mut(index))
        else {
            return;
        };
        if let Some(exit) = exec.exit.take() {
            let _ = exit.send(code);
        }
        let mut pipe = exec
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pipe.ended = true;
        pipe.wake_all();
    }

    /// Injection: the guest writes `bytes` to its serial port. They are kept
    /// for replays (the newest 1 MiB) and sent to every live `serial_output`
    /// stream; a subscriber whose channel is full misses the chunk.
    pub fn push_serial(&self, id: &VmId, bytes: &[u8]) {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return;
        };
        record.serial_ring.extend(bytes);
        let excess = record.serial_ring.len().saturating_sub(SERIAL_RING);
        record.serial_ring.drain(..excess);
        record.serial_subscribers.retain(|tx| !tx.is_closed());
        for tx in &record.serial_subscribers {
            let _ = tx.try_send(bytes.to_vec());
        }
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

    /// Injection: the app inside a running VM exits on its own with `code`,
    /// as the Cloud Hypervisor engine reports it: the VM is `Crashed` with
    /// that exit code, and the event is [`EngineEvent::Crashed`] with the
    /// reason `app exited with code <code>` (no stop was requested).
    pub fn exit_app(&self, id: &VmId, code: i32) {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return;
        };
        if record.info.state != VmState::Running {
            return;
        }
        record.info.exit_code = Some(code);
        crash_locked(&mut state, id, &format!("app exited with code {code}"));
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

/// The prepared image the fake records for `digest`.
fn fake_image(digest: &ImageDigest) -> PreparedImage {
    PreparedImage {
        digest: digest.clone(),
        rootfs_path: PathBuf::from(format!("/fake/images/{}", digest.0)),
        size_bytes: 0,
        config: ImageConfig {
            entrypoint: Vec::new(),
            cmd: Vec::new(),
            env: Vec::new(),
            user: None,
            workdir: None,
            exposed_ports: Vec::new(),
        },
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
    record.paused = false;
    end_execs(record);
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
            exec: true,
            volume_snapshot_method: Some(self.lock().snapshot_method),
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
        let prepared = fake_image(&image.digest);
        state.images.insert(image.digest.clone(), prepared.clone());
        Ok(prepared)
    }

    async fn remove_image(&self, digest: &ImageDigest) -> Result<(), EngineError> {
        self.lock().images.remove(digest);
        Ok(())
    }

    async fn image_inventory(&self, digest: &ImageDigest) -> Result<ImageInventory, EngineError> {
        let state = self.lock();
        if !state.images.contains_key(digest) {
            return Err(EngineError::NotFound);
        }
        match state.inventories.get(digest) {
            Some(Some(inventory)) => Ok(inventory.clone()),
            Some(None) => Err(EngineError::NotFound),
            None => Ok(ImageInventory {
                digest: digest.clone(),
                os: None,
                ecosystems: Vec::new(),
                not_inventoried: Vec::new(),
                packages: Vec::new(),
                inventoried_at: "2026-01-01T00:00:00Z".to_owned(),
            }),
        }
    }

    async fn uninventoried_images(&self) -> Result<Vec<ImageDigest>, EngineError> {
        let state = self.lock();
        let mut digests: Vec<ImageDigest> = state
            .images
            .keys()
            .filter(|digest| matches!(state.inventories.get(*digest), Some(None)))
            .cloned()
            .collect();
        digests.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(digests)
    }

    async fn inventory_cached_image(
        &self,
        digest: &ImageDigest,
    ) -> Result<ImageInventory, EngineError> {
        if !self.lock().images.contains_key(digest) {
            return Err(EngineError::NotFound);
        }
        if !matches!(self.lock().inventories.get(digest), Some(None)) {
            return self.image_inventory(digest).await;
        }
        if let Some(err) = self.take_failure(FakeOp::InventoryCachedImage) {
            return Err(err);
        }
        let mut state = self.lock();
        let inventory = state
            .backfills
            .remove(digest)
            .unwrap_or_else(|| ImageInventory {
                digest: digest.clone(),
                os: None,
                ecosystems: Vec::new(),
                not_inventoried: Vec::new(),
                packages: Vec::new(),
                inventoried_at: "2026-01-01T00:00:00Z".to_owned(),
            });
        state.backfilled.push(digest.clone());
        state
            .inventories
            .insert(digest.clone(), Some(inventory.clone()));
        Ok(inventory)
    }

    async fn prepare_layout(&self, layout: &OciLayout) -> Result<PreparedImage, EngineError> {
        let mut state = self.lock();
        if let Some(cached) = state.images.get(&layout.manifest_digest) {
            return Ok(cached.clone());
        }
        if !layout.path.exists() {
            return Err(EngineError::Image(format!(
                "layout {} does not exist",
                layout.path.display()
            )));
        }
        let prepared = fake_image(&layout.manifest_digest);
        state
            .images
            .insert(layout.manifest_digest.clone(), prepared.clone());
        Ok(prepared)
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
        if spec.net.egress_only && spec.net.isolation_group.is_some() {
            return Err(EngineError::Conflict(format!(
                "VM {} is egress only and cannot carry an isolation group",
                spec.id.0
            )));
        }
        for (index, volume) in spec.volumes.iter().enumerate() {
            if !state.volumes.contains_key(&volume.volume_id) {
                return Err(EngineError::NotFound);
            }
            if spec.volumes[..index]
                .iter()
                .any(|other| other.serial == volume.serial)
            {
                return Err(EngineError::Conflict(format!(
                    "volume serial {} is used twice",
                    volume.serial
                )));
            }
            if let Some((other_id, _)) = state.vms.iter().find(|(_, record)| {
                record
                    .spec
                    .volumes
                    .iter()
                    .any(|other| other.volume_id == volume.volume_id)
            }) {
                return Err(EngineError::Conflict(format!(
                    "volume {} is already attached to VM {}",
                    volume.volume_id.0, other_id.0
                )));
            }
        }
        for disk in &spec.work_disks {
            for (other_id, record) in &state.vms {
                if record
                    .spec
                    .work_disks
                    .iter()
                    .any(|other| other.path == disk.path)
                {
                    return Err(EngineError::Conflict(format!(
                        "work disk {} is already attached to VM {}",
                        disk.path.display(),
                        other_id.0
                    )));
                }
            }
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
                execs: Vec::new(),
                serial_ring: VecDeque::new(),
                serial_subscribers: Vec::new(),
                paused: false,
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
        record.paused = false;
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
        end_execs(record);
        record.info.state = VmState::Stopped;
        record.info.pid = None;
        record.info.exit_code = Some(code);
        record.paused = false;
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
        if let Some(err) = self.take_failure(FakeOp::List) {
            return Err(err);
        }
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
        if matches!(record.spec.serial, SerialMode::Socket { .. }) {
            return Err(EngineError::Conflict(format!(
                "VM {}'s serial port is read through serial_output",
                id.0
            )));
        }
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

    async fn exec(&self, id: &VmId, request: &ExecRequest) -> Result<ExecSession, EngineError> {
        let open = quoro_init::exec::OpenRequest {
            argv: request.argv.clone(),
            user: request.user.clone(),
            rows: request.rows,
            cols: request.cols,
        };
        open.validate()
            .map_err(|err| EngineError::Conflict(err.to_string()))?;
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if record.info.state != VmState::Running {
            return Err(EngineError::Conflict(format!("VM {} is not running", id.0)));
        }
        if record.spec.vsock.is_none() {
            return Err(EngineError::Conflict(format!(
                "VM {} has no vsock device",
                id.0
            )));
        }
        let shared = Arc::new(Mutex::new(ExecPipe::default()));
        let (resize_tx, resize_rx) = mpsc::channel(64);
        let (exit_tx, exit_rx) = oneshot::channel();
        record.execs.push(FakeExec {
            request: request.clone(),
            shared: Arc::clone(&shared),
            resize: resize_rx,
            exit: Some(exit_tx),
        });
        Ok(ExecSession {
            io: Box::pin(ExecIo { shared }),
            resize: resize_tx,
            exit: exit_rx,
        })
    }

    fn serial_output(&self, id: &VmId, tail_bytes: Option<u64>) -> Result<ByteStream, EngineError> {
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if !matches!(record.spec.serial, SerialMode::Socket { .. }) {
            return Err(EngineError::Conflict(format!(
                "VM {}'s serial port is not a socket",
                id.0
            )));
        }
        let (tx, rx) = mpsc::channel(SERIAL_CAPACITY);
        let kept = record.serial_ring.len();
        let replay = tail_bytes.map_or(kept, |tail| {
            usize::try_from(tail).unwrap_or(usize::MAX).min(kept)
        });
        if replay > 0 {
            let start = kept - replay;
            let chunk: Vec<u8> = record.serial_ring.range(start..).copied().collect();
            // A fresh channel always has room for the replay.
            let _ = tx.try_send(chunk);
        }
        record.serial_subscribers.push(tx);
        Ok(Box::pin(ReceiverStream::new(rx)))
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

    async fn ensure_volume(&self, id: &VolumeId, size_gb: u32) -> Result<VolumeInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::EnsureVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        if let Some(volume) = state.volumes.get(id) {
            if volume.size_gb != size_gb {
                return Err(EngineError::Conflict(format!(
                    "volume {} exists with {} GiB",
                    id.0, volume.size_gb
                )));
            }
            return Ok(volume_info(id, volume));
        }
        let path = match &state.volume_dir {
            Some(dir) => {
                let path = dir.join(&id.0);
                let file = std::fs::File::create(&path).map_err(io_error)?;
                file.set_len(u64::from(size_gb) * GIB).map_err(io_error)?;
                path
            }
            None => PathBuf::from(format!("/fake/volumes/{}", id.0)),
        };
        let volume = FakeVolume { size_gb, path };
        let info = volume_info(id, &volume);
        state.volumes.insert(id.clone(), volume);
        Ok(info)
    }

    async fn import_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &Path,
    ) -> Result<VolumeInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::ImportVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        if state.volumes.contains_key(id) {
            return Err(EngineError::Conflict(format!("volume {} exists", id.0)));
        }
        let path = match &state.volume_dir {
            Some(dir) => {
                let len = std::fs::metadata(from).map_err(io_error)?.len();
                if len != u64::from(size_gb) * GIB {
                    return Err(EngineError::Conflict(format!(
                        "restored file is {len} bytes, not {size_gb} GiB"
                    )));
                }
                let path = dir.join(&id.0);
                std::fs::rename(from, &path).map_err(io_error)?;
                path
            }
            None => PathBuf::from(format!("/fake/volumes/{}", id.0)),
        };
        let volume = FakeVolume { size_gb, path };
        let info = volume_info(id, &volume);
        state.volumes.insert(id.clone(), volume);
        Ok(info)
    }

    async fn delete_volume(&self, id: &VolumeId) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::DeleteVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        if let Some((vm_id, _)) = state
            .vms
            .iter()
            .find(|(_, record)| record.spec.volumes.iter().any(|v| &v.volume_id == id))
        {
            return Err(EngineError::Conflict(format!(
                "volume {} is attached to VM {}",
                id.0, vm_id.0
            )));
        }
        if let Some(volume) = state.volumes.remove(id)
            && state.volume_dir.is_some()
        {
            match std::fs::remove_file(&volume.path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_error(err)),
            }
        }
        Ok(())
    }

    async fn snapshot_volume(&self, id: &VolumeId) -> Result<VolumeSnapshot, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::SnapshotVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        // From M5b a volume may have several snapshots at once (section 3.16).
        let Some(volume) = state.volumes.get(id) else {
            return Err(EngineError::NotFound);
        };
        let size_bytes = u64::from(volume.size_gb) * GIB;
        let n = state.next_snapshot;
        let location = match &state.volume_dir {
            Some(dir) => {
                let location = dir.join(format!("{}.snap-{n}", id.0));
                std::fs::copy(&volume.path, &location).map_err(io_error)?;
                location
            }
            None => PathBuf::from(format!("/fake/snapshots/{n}")),
        };
        let attached_running = state.vms.values().any(|record| {
            record.info.state == VmState::Running
                && record.spec.volumes.iter().any(|v| &v.volume_id == id)
        });
        let method = state.snapshot_method;
        let pause = if method == SnapshotMethod::PauseCopy && attached_running {
            state.snapshot_pause
        } else {
            Duration::ZERO
        };
        state.next_snapshot += 1;
        let snapshot = VolumeSnapshot {
            volume_id: id.clone(),
            snapshot_id: format!("snap-{n}"),
            created_at: SystemTime::now(),
            size_bytes,
            location,
            method,
            pause,
        };
        state
            .snapshots
            .insert(snapshot.snapshot_id.clone(), snapshot.clone());
        Ok(snapshot)
    }

    async fn delete_volume_snapshot(&self, snap: &VolumeSnapshot) -> Result<(), EngineError> {
        let mut state = self.lock();
        if state.snapshots.remove(&snap.snapshot_id).is_some() && state.volume_dir.is_some() {
            match std::fs::remove_file(&snap.location) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_error(err)),
            }
        }
        Ok(())
    }

    async fn volume_changed_ranges(
        &self,
        newer: &RangesOf,
        base: Option<&VolumeSnapshot>,
    ) -> Result<ChangedRanges, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::VolumeChangedRanges) {
            return Err(err);
        }
        let mut state = self.lock();
        // The newer side: a snapshot the fake took, or a volume no VM that
        // exists attaches (section 3.22).
        let (volume_id, newer_path, size_bytes) = match newer {
            RangesOf::Snapshot(snapshot) => {
                let Some(known) = state.snapshots.get(&snapshot.snapshot_id) else {
                    return Err(EngineError::NotFound);
                };
                (
                    known.volume_id.clone(),
                    known.location.clone(),
                    known.size_bytes,
                )
            }
            RangesOf::Volume(id) => {
                let Some(volume) = state.volumes.get(id) else {
                    return Err(EngineError::NotFound);
                };
                if let Some(vm_id) = attaching_vm(&state, id) {
                    return Err(EngineError::Conflict(format!(
                        "volume {} is attached to VM {}",
                        id.0, vm_id.0
                    )));
                }
                (
                    id.clone(),
                    volume.path.clone(),
                    u64::from(volume.size_gb) * GIB,
                )
            }
        };
        let base_path = match base {
            Some(base) if base.volume_id != volume_id => {
                return Err(EngineError::Conflict(format!(
                    "base snapshot {} is of volume {}, not {}",
                    base.snapshot_id, base.volume_id.0, volume_id.0
                )));
            }
            Some(base) => match state.snapshots.get(&base.snapshot_id) {
                Some(known) => Some(known.location.clone()),
                None => return Err(EngineError::NotFound),
            },
            None => None,
        };
        if let Some(pushed) = state.changed_ranges.pop_front() {
            return Ok(pushed);
        }
        let whole = ChangedRanges {
            ranges: vec![ByteRange {
                offset: 0,
                length: size_bytes,
            }],
            full: true,
            extent_count: 1,
        };
        match (state.ranges_mode, base_path, state.volume_dir.is_some()) {
            (FakeRanges::Content, Some(base_path), true) => {
                drop(state);
                let ranges = differing_blocks(&newer_path, &base_path, size_bytes)?;
                let extent_count = ranges.len() as u64;
                Ok(ChangedRanges {
                    ranges,
                    full: false,
                    extent_count,
                })
            }
            _ => Ok(whole),
        }
    }

    async fn stage_volume(
        &self,
        id: &VolumeId,
        size_gb: u32,
        from: &StageFrom,
    ) -> Result<StagedVolume, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::StageVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        if state.staged.contains_key(id) {
            return Err(EngineError::Conflict(format!(
                "volume {} already has a staging file",
                id.0
            )));
        }
        let size_bytes = u64::from(size_gb) * GIB;
        // The file the staging copy is cloned from, if any.
        let source = match from {
            StageFrom::Empty => None,
            StageFrom::Volume => {
                let Some(volume) = state.volumes.get(id) else {
                    return Err(EngineError::NotFound);
                };
                if volume.size_gb != size_gb {
                    return Err(EngineError::Conflict(format!(
                        "volume {} is {} GiB, not {size_gb} GiB",
                        id.0, volume.size_gb
                    )));
                }
                Some(volume.path.clone())
            }
            StageFrom::Snapshot(snapshot) => {
                if &snapshot.volume_id != id {
                    return Err(EngineError::Conflict(format!(
                        "snapshot {} is of volume {}, not {}",
                        snapshot.snapshot_id, snapshot.volume_id.0, id.0
                    )));
                }
                let Some(known) = state.snapshots.get(&snapshot.snapshot_id) else {
                    return Err(EngineError::NotFound);
                };
                if known.size_bytes != size_bytes {
                    return Err(EngineError::Conflict(format!(
                        "snapshot {} is {} bytes, not {size_gb} GiB",
                        known.snapshot_id, known.size_bytes
                    )));
                }
                Some(known.location.clone())
            }
        };
        // As in the real engine, only a reflink node clones (section 3.22).
        if source.is_some() && state.snapshot_method != SnapshotMethod::Reflink {
            return Err(EngineError::Unsupported(
                "a staging clone needs reflink; this node has no warm volumes".into(),
            ));
        }
        let location = staging_location(&state, id);
        let location = match &state.volume_dir {
            Some(_) => {
                match source {
                    Some(source) => {
                        std::fs::copy(&source, &location).map_err(io_error)?;
                    }
                    None => {
                        let file = std::fs::File::create(&location).map_err(io_error)?;
                        file.set_len(size_bytes).map_err(io_error)?;
                    }
                }
                location
            }
            None => location,
        };
        state.staged.insert(
            id.clone(),
            FakeStaged {
                size_gb,
                location: location.clone(),
            },
        );
        Ok(StagedVolume {
            volume_id: id.clone(),
            size_bytes,
            location,
        })
    }

    async fn commit_staged_volume(&self, staged: &StagedVolume) -> Result<VolumeInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::CommitStagedVolume) {
            return Err(err);
        }
        let mut state = self.lock();
        let id = &staged.volume_id;
        // The same answers as the real engine: a foreign location or size is
        // `Conflict`, a staging file that is not there is `NotFound`.
        check_staged_location(&state, staged)?;
        match state.staged.get(id) {
            None => return Err(EngineError::NotFound),
            Some(known) if u64::from(known.size_gb) * GIB != staged.size_bytes => {
                return Err(EngineError::Conflict(format!(
                    "the staging file of volume {} is {} GiB, not {} bytes",
                    id.0, known.size_gb, staged.size_bytes
                )));
            }
            Some(_) => {}
        }
        if let Some(vm_id) = attaching_vm(&state, id) {
            return Err(EngineError::Conflict(format!(
                "volume {} is attached to VM {}",
                id.0, vm_id.0
            )));
        }
        let Some(known) = state.staged.remove(id) else {
            return Err(EngineError::NotFound);
        };
        let path = match &state.volume_dir {
            Some(dir) => {
                let path = dir.join(&id.0);
                if let Err(err) = std::fs::rename(&known.location, &path) {
                    state.staged.insert(id.clone(), known);
                    return Err(io_error(err));
                }
                path
            }
            None => PathBuf::from(format!("/fake/volumes/{}", id.0)),
        };
        let volume = FakeVolume {
            size_gb: known.size_gb,
            path,
        };
        let info = volume_info(id, &volume);
        state.volumes.insert(id.clone(), volume);
        Ok(info)
    }

    async fn discard_staged_volume(&self, staged: &StagedVolume) -> Result<(), EngineError> {
        let mut state = self.lock();
        check_staged_location(&state, staged)?;
        if state.staged.remove(&staged.volume_id).is_none() {
            return Ok(());
        }
        if state.volume_dir.is_some() {
            match std::fs::remove_file(&staged.location) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_error(err)),
            }
        }
        Ok(())
    }

    async fn pause(&self, id: &VmId) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Pause) {
            return Err(err);
        }
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if record.info.state != VmState::Running {
            return Err(EngineError::Conflict(format!("VM {} is not running", id.0)));
        }
        record.paused = true;
        Ok(())
    }

    async fn resume(&self, id: &VmId) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Resume) {
            return Err(err);
        }
        let mut state = self.lock();
        let Some(record) = state.vms.get_mut(id) else {
            return Err(EngineError::NotFound);
        };
        if record.info.state != VmState::Running {
            return Err(EngineError::Conflict(format!("VM {} is not running", id.0)));
        }
        record.paused = false;
        Ok(())
    }
}

/// Where the staging file of `id` lives: next to the volume files when a
/// volume directory is set, a placeholder path otherwise.
fn staging_location(state: &State, id: &VolumeId) -> PathBuf {
    match &state.volume_dir {
        Some(dir) => dir.join(format!("{}.staging", id.0)),
        None => PathBuf::from(format!("/fake/staging/{}", id.0)),
    }
}

/// `Conflict` unless `staged` names the staging file of its own volume, as
/// in the real engine (section 3.22).
fn check_staged_location(state: &State, staged: &StagedVolume) -> Result<(), EngineError> {
    if staged.location == staging_location(state, &staged.volume_id) {
        Ok(())
    } else {
        Err(EngineError::Conflict(format!(
            "{} is not the staging file of volume {}",
            staged.location.display(),
            staged.volume_id.0
        )))
    }
}

/// The VM that attaches `id`, if one exists (section 3.22).
fn attaching_vm(state: &State, id: &VolumeId) -> Option<VmId> {
    state
        .vms
        .iter()
        .find(|(_, record)| record.spec.volumes.iter().any(|v| &v.volume_id == id))
        .map(|(vm_id, _)| vm_id.clone())
}

/// The 4 KiB blocks whose bytes differ between two files of `size` bytes,
/// merged into ranges ([`FakeRanges::Content`]).
fn differing_blocks(newer: &Path, base: &Path, size: u64) -> Result<Vec<ByteRange>, EngineError> {
    use std::io::Read as _;

    const READ: usize = 1 << 20;
    let mut newer = std::fs::File::open(newer).map_err(io_error)?;
    let mut base = std::fs::File::open(base).map_err(io_error)?;
    let mut left = vec![0_u8; READ];
    let mut right = vec![0_u8; READ];
    let mut ranges = Vec::new();
    let mut offset = 0_u64;
    while offset < size {
        let want = usize::try_from((size - offset).min(READ as u64)).unwrap_or(READ);
        read_full(&mut newer, &mut left[..want])?;
        read_full(&mut base, &mut right[..want])?;
        for (index, (a, b)) in left[..want]
            .chunks(crate::extents::BLOCK as usize)
            .zip(right[..want].chunks(crate::extents::BLOCK as usize))
            .enumerate()
        {
            if a != b {
                ranges.push(ByteRange {
                    offset: offset + index as u64 * crate::extents::BLOCK,
                    length: a.len() as u64,
                });
            }
        }
        offset += want as u64;
    }

    /// Reads `buffer.len()` bytes, or zeros past the end of the file.
    fn read_full(file: &mut std::fs::File, buffer: &mut [u8]) -> Result<(), EngineError> {
        let mut filled = 0;
        while filled < buffer.len() {
            let read = file.read(&mut buffer[filled..]).map_err(io_error)?;
            if read == 0 {
                buffer[filled..].fill(0);
                break;
            }
            filled += read;
        }
        Ok(())
    }

    Ok(crate::extents::round_and_merge(ranges, size))
}

/// The `VolumeInfo` of a fake volume; a record-only volume uses no disk.
fn volume_info(id: &VolumeId, volume: &FakeVolume) -> VolumeInfo {
    use std::os::unix::fs::MetadataExt as _;
    let allocated_bytes = std::fs::metadata(&volume.path)
        .map(|meta| meta.blocks() * 512)
        .unwrap_or(0);
    VolumeInfo {
        volume_id: id.clone(),
        size_gb: volume.size_gb,
        allocated_bytes,
        path: volume.path.clone(),
    }
}

/// A file-system error on a fake volume file.
fn io_error(err: io::Error) -> EngineError {
    EngineError::Io(err.to_string())
}
