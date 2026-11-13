// SPDX-License-Identifier: AGPL-3.0-only
//! The in-memory [`FakeEngine`] from `docs/contracts/engine-trait.md` section 7.
//!
//! It implements the same trait with no hypervisor: VMs are records, lifecycle
//! calls change their state, and `list` returns exactly what was created since
//! the instance started. Booting completes when `start` returns, unless a test
//! makes it slow, so no test has to wait for a machine that does not exist.
//!
//! Determinism: there are no threads. The only awaits are the explicit
//! injections — a guest that ignores the power button
//! (`set_ignore_power_button`), a slow VMM (`set_stop_delay`) and a slow boot
//! (`set_start_delay`) — and the bounds `start` and `stop` carry. They are
//! plain `tokio::time` sleeps and timeouts, so they are deterministic under
//! `tokio::time::pause` / `#[tokio::test(start_paused = true)]`. A slow boot
//! and a slow forced stop run on tasks of their own, as the Cloud Hypervisor
//! backend's do, so a forced stop the fake began finishes even when the call
//! returned `Timeout` or its caller stopped waiting (contract §3.7, §3.8 and
//! §7). Everything else, including scheduled crashes, runs on the fake's own
//! manual clock (`advance`), not on wall-clock time.
//!
//! Injection hooks are documented on the methods that provide them. They exist
//! so every error path and every event the node agent must map is reachable:
//! `fail_next` returns an error once, `crash` and `schedule_crash` produce a
//! `Crashed` event, `set_console_echo` makes a console round-trip, and
//! `set_unkillable` makes every stop fail as a VMM that survives `SIGKILL`,
//! and `set_guest_flush` sets what a guest flush comes to.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{ByteRange, ChangedRanges};
use crate::{
    ByteStream, Capabilities, ConsoleMode, ConsoleStream, Engine, EngineError, EngineEvent,
    EventStream, ExecRequest, ExecSession, GuestFlush, HostProbe, ImageConfig, ImageDigest,
    ImageInventory, ImageRef, LineStream, NodeNetwork, OciLayout, OutputLine, OutputStream,
    PreparedImage, RangesOf, SerialMode, SnapshotMethod, SnapshotSweep, StageFrom, StagedVolume,
    StopMode, VmCounters, VmId, VmInfo, VmSpec, VmState, VolumeId, VolumeInfo, VolumeSnapshot,
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
    /// `delete_volume_snapshot` (M5b): the file and the fake's record stay.
    DeleteVolumeSnapshot,
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
    /// `flush_volume` (M5b, ADR 0282). It fails that call only; a snapshot's
    /// own flush follows [`FakeEngine::set_guest_flush`].
    FlushVolume,
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
    /// Injection: every stop call takes this long before acting. A forced stop
    /// keeps the VM `Stopping` meanwhile and finishes even when its caller
    /// stops waiting.
    stop_delay: Duration,
    /// Injection: a start takes this long before the VM runs, and keeps it
    /// `Starting` meanwhile.
    start_delay: Duration,
    /// Set while a slow forced stop runs on its own task; it turns `true` once
    /// that stop has finished, so a later stop can join it.
    forced_stop: Option<watch::Receiver<bool>>,
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
    /// What `flush_volume` returns and `snapshot_volume` records, when a test
    /// set it (M5b, ADR 0282); `None` derives it from the VMs.
    guest_flush: Option<Result<GuestFlush, EngineError>>,
    /// The `flush_volume` calls and their results, in order.
    flush_log: Vec<(VolumeId, Result<GuestFlush, EngineError>)>,
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
            guest_flush: None,
            flush_log: Vec::new(),
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
    /// Shared with a slow forced stop's task.
    state: Arc<Mutex<State>>,
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
            state: Arc::new(Mutex::new(State::new())),
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

    /// What `flush_volume` returns and `snapshot_volume` records in
    /// `guest_flush`, until it is set again (M5b, ADR 0282). An injected error
    /// is what `flush_volume` returns; `snapshot_volume` then records
    /// `Failed`, or returns `NotFound` for `NotFound`. `None` restores the
    /// default: `Flushed` when a running VM that is not paused attaches the
    /// volume, `Paused` when that VM is paused, and `NoRunningVm` otherwise.
    pub fn set_guest_flush(&self, outcome: Option<Result<GuestFlush, EngineError>>) {
        self.lock().guest_flush = outcome;
    }

    /// The `flush_volume` calls this engine has received and what each
    /// returned, in order (M5b). A snapshot's own flush is not one of them;
    /// it is in the snapshot's `guest_flush`.
    #[must_use]
    pub fn flush_calls(&self) -> Vec<(VolumeId, Result<GuestFlush, EngineError>)> {
        self.lock().flush_log.clone()
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
    /// (section 3.16): removes every staging file and every snapshot whose
    /// file name `sweep` does not keep, and returns the file names of the
    /// snapshots it left, as `ChEngine::snapshots_at_start` does. A test
    /// calls it to model an agent restart, as the fake itself outlives one.
    ///
    /// # Errors
    ///
    /// `Io` when a file of a removed snapshot or staging copy cannot be
    /// removed.
    pub fn start_up_sweep(&self, sweep: &SnapshotSweep) -> Result<BTreeSet<String>, EngineError> {
        let mut state = self.lock();
        let has_files = state.volume_dir.is_some();
        let mut removed: Vec<PathBuf> = Vec::new();
        let mut kept = BTreeSet::new();
        state.snapshots.retain(|_, snapshot| {
            let name = snapshot
                .location
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            if sweep.keeps(&name) {
                kept.insert(name);
                true
            } else {
                removed.push(snapshot.location.clone());
                false
            }
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
        Ok(kept)
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

    /// Injection: models a slow VMM; the default is zero. A forced stop marks
    /// a running VM `Stopping` and runs on its own task, as the Cloud
    /// Hypervisor backend's does, and the VM is `Stopped` once `delay` has
    /// passed. A `delay` longer than the stop's `within` makes the call return
    /// `Timeout` at `within`, and the stop still finishes: a caller that got
    /// `Timeout` or stopped waiting does not cancel it (contract §3.8, §7).
    /// Meanwhile a later stop of the VM joins it within its own bound, and
    /// `start` and `delete` are `Conflict`. A graceful stop of a guest that
    /// reacts to the power button takes `delay` too, when that fits in its
    /// `timeout`; otherwise it goes on as a slow forced stop once `timeout`
    /// has passed. The sleeps are deterministic under a paused tokio clock.
    pub fn set_stop_delay(&self, id: &VmId, delay: Duration) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.stop_delay = delay;
        }
    }

    /// Injection: models a slow boot; the default is zero. A start of this VM
    /// keeps it `Starting`, with its pid, until `delay` has passed, then
    /// `Running`. A start whose `within` passes first returns `Timeout` at
    /// `within`, as does a start whose caller stops waiting: the fake gives
    /// the boot up and begins a forced stop of what it started, which takes
    /// the VM's stop delay ([`Self::set_stop_delay`]). Until the test lets that
    /// stop finish, the VM reads `Stopping` with its pid, a later stop joins
    /// it, and `start` and `delete` are `Conflict`; then it is `Stopped` with
    /// no pid and no event, as it never booted (contract §3.7, §7). While a
    /// slow boot runs, `stop` and `delete` are `Conflict`.
    pub fn set_start_delay(&self, id: &VmId, delay: Duration) {
        if let Some(record) = self.lock().vms.get_mut(id) {
            record.start_delay = delay;
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

/// What a [`FakeEngine::stop`] call does, decided under the state lock.
enum StopPlan {
    /// The VM is already stopped.
    Stopped,
    /// A forced stop of the VM runs; the call joins it.
    Join(watch::Receiver<bool>),
    /// A slow forced stop, already registered on the VM.
    SlowForce {
        pid: Option<u32>,
        delay: Duration,
        done: watch::Sender<bool>,
    },
    /// A graceful stop asks the guest first.
    Guest { delay: Duration, ignores: bool },
}

/// Decides what a stop of `id` does, in one critical section with the
/// registration of a slow forced stop, so two stops never both find nothing
/// to join. A forced stop of a fast VMM happens here at once.
fn plan_stop(state: &mut State, id: &VmId, forced: bool) -> Result<StopPlan, EngineError> {
    let Some(record) = state.vms.get_mut(id) else {
        return Err(EngineError::NotFound);
    };
    if record.unkillable {
        return Err(EngineError::Hypervisor(
            "VMM process did not exit after SIGKILL".into(),
        ));
    }
    if record.info.state == VmState::Stopped {
        return Ok(StopPlan::Stopped);
    }
    if let Some(pending) = record.forced_stop.clone() {
        return Ok(StopPlan::Join(pending));
    }
    if record.info.state == VmState::Starting {
        return Err(EngineError::Conflict(format!("VM {} is starting", id.0)));
    }
    if !forced {
        return Ok(StopPlan::Guest {
            delay: record.stop_delay,
            ignores: record.ignores_power_button,
        });
    }
    if record.stop_delay.is_zero() {
        stop_now(state, id, 137)?;
        return Ok(StopPlan::Stopped);
    }
    let (done, joined) = watch::channel(false);
    record.info.state = VmState::Stopping;
    record.forced_stop = Some(joined);
    Ok(StopPlan::SlowForce {
        pid: record.info.pid,
        delay: record.stop_delay,
        done,
    })
}

/// A slow forced stop on its own task (contract §3.8): the VM is stopped once
/// `delay` has passed, with `code`, or with no code and no event when it never
/// booted. It acts only on the VMM it began with, `pid`: a VM that a later
/// `start` booted again keeps running. `done` tells the stops that join it.
fn spawn_slow_stop(
    shared: &Arc<Mutex<State>>,
    id: &VmId,
    pid: Option<u32>,
    delay: Duration,
    done: watch::Sender<bool>,
    code: Option<i32>,
) -> tokio::task::JoinHandle<Result<VmInfo, EngineError>> {
    let shared = Arc::clone(shared);
    let id = id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let mut state = lock_state(&shared);
        let result = finish_slow_stop(&mut state, &id, pid, code);
        let _ = done.send(true);
        result
    })
}

/// The end of a slow forced stop, under the state lock.
fn finish_slow_stop(
    state: &mut State,
    id: &VmId,
    pid: Option<u32>,
    code: Option<i32>,
) -> Result<VmInfo, EngineError> {
    let Some(record) = state.vms.get_mut(id) else {
        return Err(EngineError::NotFound);
    };
    record.forced_stop = None;
    if record.info.pid != pid {
        return Ok(record.info.clone());
    }
    match code {
        Some(code) => stop_now(state, id, code),
        None => {
            record.info.state = VmState::Stopped;
            record.info.pid = None;
            record.info.exit_code = None;
            record.paused = false;
            Ok(record.info.clone())
        }
    }
}

/// Boots `id` now, if it is still `Starting` with `pid`: it reads `Running`
/// and `Started` is emitted. `None` when the boot was given up or the VM is
/// gone.
fn boot_now(state: &mut State, id: &VmId, pid: u32) -> Option<VmInfo> {
    let record = state.vms.get_mut(id)?;
    if record.info.state != VmState::Starting || record.info.pid != Some(pid) {
        return None;
    }
    record.info.state = VmState::Running;
    record.info.started_at = Some(SystemTime::now());
    record.scheduled_crash = None;
    record.paused = false;
    let info = record.info.clone();
    emit(state, EngineEvent::Started { id: id.clone() });
    Some(info)
}

/// Gives up the slow boot of `id` with `pid` (contract §3.7): a VM still
/// `Starting` gets a forced stop of what the start began, with the VM's stop
/// delay. `Some` when the boot landed first and the VM runs.
fn give_up_start(shared: &Arc<Mutex<State>>, id: &VmId, pid: u32) -> Option<VmInfo> {
    let mut state = lock_state(shared);
    let record = state.vms.get_mut(id)?;
    if record.info.pid != Some(pid) {
        return None;
    }
    match record.info.state {
        VmState::Running => return Some(record.info.clone()),
        VmState::Starting => {}
        _ => return None,
    }
    let delay = record.stop_delay;
    let (done, joined) = watch::channel(false);
    let runtime = tokio::runtime::Handle::try_current();
    if delay.is_zero() || runtime.is_err() {
        let _ = finish_slow_stop(&mut state, id, Some(pid), None);
        return None;
    }
    record.info.state = VmState::Stopping;
    record.forced_stop = Some(joined);
    drop(state);
    // Detached: the stop goes on after the caller gave the start up.
    drop(spawn_slow_stop(shared, id, Some(pid), delay, done, None));
    None
}

/// Gives a slow boot up when its caller's future is dropped (contract §4).
struct GiveUpStart {
    state: Arc<Mutex<State>>,
    id: VmId,
    pid: u32,
    armed: bool,
}

impl Drop for GiveUpStart {
    fn drop(&mut self) {
        if self.armed {
            let _ = give_up_start(&self.state, &self.id, self.pid);
        }
    }
}

/// Locks the fake's state, ignoring poisoning as [`FakeEngine::lock`] does.
fn lock_state(shared: &Mutex<State>) -> MutexGuard<'_, State> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Stops `id` now with `code`, unless it already stopped: its execs end, it
/// reads `Stopped` and `Exited` is emitted.
fn stop_now(state: &mut State, id: &VmId, code: i32) -> Result<VmInfo, EngineError> {
    let Some(record) = state.vms.get_mut(id) else {
        return Err(EngineError::NotFound);
    };
    if record.info.state == VmState::Stopped {
        return Ok(record.info.clone());
    }
    end_execs(record);
    record.info.state = VmState::Stopped;
    record.info.pid = None;
    record.info.exit_code = Some(code);
    record.paused = false;
    let info = record.info.clone();
    emit(
        state,
        EngineEvent::Exited {
            id: id.clone(),
            code,
        },
    );
    Ok(info)
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
                start_delay: Duration::ZERO,
                forced_stop: None,
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

    async fn start(&self, id: &VmId, within: Duration) -> Result<VmInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Start) {
            return Err(err);
        }
        let deadline = tokio::time::Instant::now() + within;
        let (pid, delay) = {
            let mut state = self.lock();
            let Some(record) = state.vms.get(id) else {
                return Err(EngineError::NotFound);
            };
            if record.info.state == VmState::Running {
                return Ok(record.info.clone());
            }
            if record.info.state == VmState::Stopping || record.forced_stop.is_some() {
                return Err(EngineError::Conflict(format!("VM {} is stopping", id.0)));
            }
            if record.info.state == VmState::Starting {
                return Err(EngineError::Conflict(format!("VM {} is starting", id.0)));
            }
            state.next_pid += 1;
            let pid = state.next_pid;
            let Some(record) = state.vms.get_mut(id) else {
                return Err(EngineError::NotFound);
            };
            record.info.pid = Some(pid);
            record.info.exit_code = None;
            if record.start_delay.is_zero() {
                record.info.state = VmState::Starting;
                return boot_now(&mut state, id, pid).ok_or(EngineError::NotFound);
            }
            record.info.state = VmState::Starting;
            (pid, record.start_delay)
        };

        // A slow boot (contract §3.7): it runs on its own task, and the call
        // waits for it until `within` has passed.
        let shared = Arc::clone(&self.state);
        let booting = id.clone();
        let mut boot = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            boot_now(&mut lock_state(&shared), &booting, pid)
        });
        // A caller that stops waiting gives the boot up, as `within` does.
        let mut give_up = GiveUpStart {
            state: Arc::clone(&self.state),
            id: id.clone(),
            pid,
            armed: true,
        };
        let booted = tokio::time::timeout_at(deadline, &mut boot).await;
        give_up.armed = false;
        match booted {
            Ok(Ok(Some(info))) => Ok(info),
            Ok(Ok(None)) => self.inspect(id).await,
            Ok(Err(err)) => Err(EngineError::Hypervisor(format!(
                "the start did not finish: {err}"
            ))),
            // A boot that lands at the deadline still counts.
            Err(_) => give_up_start(&self.state, id, pid).ok_or(EngineError::Timeout),
        }
    }

    async fn stop(&self, id: &VmId, mode: StopMode) -> Result<VmInfo, EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Stop) {
            return Err(err);
        }
        let began = tokio::time::Instant::now();
        let (graceful, force_within) = match mode {
            StopMode::Graceful {
                timeout,
                force_within,
            } => (Some(timeout), force_within),
            StopMode::Force { within } => (None, within),
        };
        let deadline = began + graceful.unwrap_or_default() + force_within;

        let plan = {
            let mut state = self.lock();
            if !state.vms.contains_key(id) {
                return Err(EngineError::NotFound);
            }
            state.stop_log.push((id.clone(), mode.clone()));
            plan_stop(&mut state, id, graceful.is_none())?
        };
        let plan = match plan {
            StopPlan::Guest { delay, ignores } => {
                let timeout = graceful.unwrap_or_default();
                // A guest that reacts to the power button exits once the slow
                // VMM let it, if that fits in the graceful timeout.
                if !ignores && delay <= timeout {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    return stop_now(&mut self.lock(), id, 0);
                }
                tokio::time::sleep(timeout).await;
                // Then the VM is stopped as `Force { within: force_within }`.
                plan_stop(&mut self.lock(), id, true)?
            }
            plan => plan,
        };
        match plan {
            StopPlan::Stopped | StopPlan::Guest { .. } => self.inspect(id).await,
            // A stop while a forced stop of the same VM runs joins it, within
            // its own bound (contract §4, ADR 0243).
            StopPlan::Join(mut done) => {
                let joined = tokio::time::timeout_at(deadline, done.wait_for(|done| *done))
                    .await
                    .is_ok();
                if joined {
                    self.inspect(id).await
                } else {
                    Err(EngineError::Timeout)
                }
            }
            StopPlan::SlowForce { pid, delay, done } => {
                let task = spawn_slow_stop(&self.state, id, pid, delay, done, Some(137));
                match tokio::time::timeout_at(deadline, task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(err)) => Err(EngineError::Hypervisor(format!(
                        "the forced stop did not finish: {err}"
                    ))),
                    Err(_) => Err(EngineError::Timeout),
                }
            }
        }
    }

    async fn delete(&self, id: &VmId) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::Delete) {
            return Err(err);
        }
        let mut state = self.lock();
        let Some(record) = state.vms.get(id) else {
            return Ok(());
        };
        if matches!(
            record.info.state,
            VmState::Running | VmState::Starting | VmState::Stopping
        ) || record.forced_stop.is_some()
        {
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
        // The flush comes first, and only `NotFound` fails the snapshot
        // (section 3.16, ADR 0282).
        let guest_flush = match &state.guest_flush {
            None => default_guest_flush(&state, id),
            Some(Ok(outcome)) => *outcome,
            Some(Err(EngineError::NotFound)) => return Err(EngineError::NotFound),
            Some(Err(_)) => GuestFlush::Failed,
        };
        let size_bytes = u64::from(volume.size_gb) * GIB;
        let n = state.next_snapshot;
        let location = match &state.volume_dir {
            Some(dir) => {
                let location = dir.join(format!("{}.snap-{n}", id.0));
                copy_sparse(&volume.path, &location).map_err(io_error)?;
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
            guest_flush,
        };
        state
            .snapshots
            .insert(snapshot.snapshot_id.clone(), snapshot.clone());
        Ok(snapshot)
    }

    async fn delete_volume_snapshot(&self, snap: &VolumeSnapshot) -> Result<(), EngineError> {
        if let Some(err) = self.take_failure(FakeOp::DeleteVolumeSnapshot) {
            return Err(err);
        }
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
        // The files themselves, as the real engine opens them (section 3.22):
        // a volume's live file first, then the base. A file missing from the
        // volume directory is `NotFound`, and a path that is not a regular
        // file is `Conflict`. Without a volume directory the fake keeps no
        // files, so the records are all there is to check.
        if state.volume_dir.is_some() {
            if matches!(newer, RangesOf::Volume(_)) {
                check_regular_file(&newer_path)?;
            }
            if let Some(base_path) = &base_path {
                check_regular_file(base_path)?;
            }
        }
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
                        copy_sparse(&source, &location).map_err(io_error)?;
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

    async fn flush_volume(
        &self,
        id: &VolumeId,
        _within: Duration,
    ) -> Result<GuestFlush, EngineError> {
        let failure = self.take_failure(FakeOp::FlushVolume);
        let mut state = self.lock();
        let result = match (failure, &state.guest_flush) {
            (Some(err), _) => Err(err),
            (None, _) if !state.volumes.contains_key(id) => Err(EngineError::NotFound),
            (None, None) => Ok(default_guest_flush(&state, id)),
            (None, Some(injected)) => injected.clone(),
        };
        state.flush_log.push((id.clone(), result.clone()));
        result
    }
}

/// What the guest flush of `id` comes to when no test set it (section 7): the
/// running VM that attaches the volume answers unless it is paused.
fn default_guest_flush(state: &State, id: &VolumeId) -> GuestFlush {
    let attached = state.vms.values().find(|record| {
        record.info.state == VmState::Running
            && record.spec.volumes.iter().any(|v| &v.volume_id == id)
    });
    match attached {
        Some(record) if record.paused => GuestFlush::Paused,
        Some(_) => GuestFlush::Flushed,
        None => GuestFlush::NoRunningVm,
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

/// The ranges of `file`, of `length` bytes, that hold data, from
/// `SEEK_DATA` and `SEEK_HOLE`; what lies between them reads as zeros.
#[cfg(target_os = "linux")]
fn data_ranges(file: &std::fs::File, length: u64) -> io::Result<Vec<(u64, u64)>> {
    use rustix::fs::SeekFrom;
    use rustix::io::Errno;

    let mut ranges = Vec::new();
    let mut offset = 0_u64;
    while offset < length {
        let data = match rustix::fs::seek(file, SeekFrom::Data(offset)) {
            Ok(data) if data < length => data,
            Ok(_) | Err(Errno::NXIO) => break,
            Err(err) => return Err(err.into()),
        };
        let end = match rustix::fs::seek(file, SeekFrom::Hole(data)) {
            Ok(hole) if hole > data => hole.min(length),
            // A hole at the data offset is not the truth; take the rest.
            Ok(_) | Err(Errno::NXIO) => length,
            Err(err) => return Err(err.into()),
        };
        ranges.push((data, end));
        offset = end;
    }
    Ok(ranges)
}

/// Elsewhere the whole file counts as data.
#[cfg(not(target_os = "linux"))]
fn data_ranges(_file: &std::fs::File, length: u64) -> io::Result<Vec<(u64, u64)>> {
    Ok(vec![(0, length)])
}

/// Copies a volume file the way a reflink clone looks: the same length, with
/// only the ranges that hold data written, so its holes stay holes, as the
/// real engine's snapshots and staging clones do. On Linux `std::fs::copy`
/// writes every byte where the filesystem cannot clone (ext4), which made
/// each copy of a 1 GiB test volume a 1 GiB write; so the data ranges are
/// found with `SEEK_DATA` and `SEEK_HOLE` and copied alone, and a chunk of
/// zeros inside them is not written. A filesystem that reports holes as
/// data (a macOS directory mounted into a Linux container does, for a file
/// renamed and opened again) then costs a read, and the copy stays sparse.
#[cfg(target_os = "linux")]
fn copy_sparse(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;

    const CHUNK: u64 = 1 << 20;
    let source = std::fs::File::open(from)?;
    let length = source.metadata()?.len();
    let target = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(to)?;
    target.set_len(length)?;
    let mut buffer = vec![0_u8; CHUNK as usize];
    let zeros = vec![0_u8; CHUNK as usize];
    for (data, end) in data_ranges(&source, length)? {
        let mut at = data;
        while at < end {
            let n = usize::try_from((end - at).min(CHUNK)).unwrap_or(buffer.len());
            source.read_exact_at(&mut buffer[..n], at)?;
            // The target is new and of the full length: a hole there reads
            // as zeros already.
            if buffer[..n] != zeros[..n] {
                target.write_all_at(&buffer[..n], at)?;
            }
            at += n as u64;
        }
    }
    Ok(())
}

/// Elsewhere `std::fs::copy` clones the file where the filesystem can (APFS
/// on macOS), which keeps its holes.
#[cfg(not(target_os = "linux"))]
fn copy_sparse(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::copy(from, to).map(|_| ())
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
/// merged into ranges ([`FakeRanges::Content`]). Only the blocks where
/// either file holds data ([`data_ranges`]) are read: elsewhere both read as
/// zeros, so a test volume costs the data it holds, not its size.
fn differing_blocks(newer: &Path, base: &Path, size: u64) -> Result<Vec<ByteRange>, EngineError> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    const READ: usize = 1 << 20;
    const BLOCK: u64 = crate::extents::BLOCK;
    let mut newer = std::fs::File::open(newer).map_err(io_error)?;
    let mut base = std::fs::File::open(base).map_err(io_error)?;
    let mut spans: Vec<(u64, u64)> = Vec::new();
    for file in [&newer, &base] {
        let length = file.metadata().map_err(io_error)?.len().min(size);
        for (start, end) in data_ranges(file, length).map_err(io_error)? {
            spans.push((
                start / BLOCK * BLOCK,
                end.div_ceil(BLOCK).saturating_mul(BLOCK).min(size),
            ));
        }
    }
    spans.sort_unstable();
    let mut read_spans: Vec<(u64, u64)> = Vec::new();
    for (start, end) in spans {
        match read_spans.last_mut() {
            Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
            _ => read_spans.push((start, end)),
        }
    }
    let mut left = vec![0_u8; READ];
    let mut right = vec![0_u8; READ];
    let mut ranges = Vec::new();
    for (start, end) in read_spans {
        newer.seek(SeekFrom::Start(start)).map_err(io_error)?;
        base.seek(SeekFrom::Start(start)).map_err(io_error)?;
        let mut offset = start;
        while offset < end {
            let want = usize::try_from((end - offset).min(READ as u64)).unwrap_or(READ);
            read_full(&mut newer, &mut left[..want])?;
            read_full(&mut base, &mut right[..want])?;
            for (index, (a, b)) in left[..want]
                .chunks(BLOCK as usize)
                .zip(right[..want].chunks(BLOCK as usize))
                .enumerate()
            {
                if a != b {
                    ranges.push(ByteRange {
                        offset: offset + index as u64 * BLOCK,
                        length: a.len() as u64,
                    });
                }
            }
            offset += want as u64;
        }
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

/// Checks that `path` is a regular file, as the real engine does before it
/// opens a file it compares (section 3.22).
///
/// # Errors
///
/// `NotFound` for a missing file, `Conflict` for anything but a regular
/// file, and `Io` when the file cannot be looked up.
fn check_regular_file(path: &Path) -> Result<(), EngineError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(EngineError::Conflict(format!(
            "{} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Err(EngineError::NotFound),
        Err(err) => Err(io_error(err)),
    }
}

/// A file-system error on a fake volume file.
fn io_error(err: io::Error) -> EngineError {
    EngineError::Io(err.to_string())
}
