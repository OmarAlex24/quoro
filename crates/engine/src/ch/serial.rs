// SPDX-License-Identifier: AGPL-3.0-only
//! The engine's only client of a VM's serial socket, and the node-local ring
//! behind `serial_output` (contract §3.19, ADR 0116).
//!
//! Cloud Hypervisor's `serial` in `socket` mode accepts one client and lets a
//! new one silently take the port away from the current one, so the engine
//! connects at `create`, before boot, and no viewer ever talks to the socket
//! (`docs/research/m4-terminal-and-console.md` §2). What the client reads is
//! appended to a ring in the VM's runtime directory:
//!
//! - two segment files, `serial.0` and `serial.1`, of [`SEGMENT_BYTES`] each,
//!   plus `serial.ring` naming the active one. When the active segment fills,
//!   the other one is truncated and becomes the active one, so at most
//!   [`RING_BYTES`] (1 MiB) is kept and the ring survives a VMM restart (which
//!   keeps the previous boot after a crash loop) and an agent restart;
//! - an in-memory copy of the newest [`RING_BYTES`] bytes, so a subscriber can
//!   replay the ring and follow live output through one lock and never miss or
//!   repeat a byte at the seam;
//! - a bounded channel per subscriber. A viewer that falls behind loses whole
//!   chunks instead of slowing the serial reader or the guest, and the ring
//!   write is a bounded local append, so a slow disk stops reading the socket
//!   and Cloud Hypervisor's own 1 MiB ring absorbs the gap.
//!
//! The socket is never written to: nothing in the guest reads `ttyS0`.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{self, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};

use crate::ByteStream;

/// Bytes of one ring segment: 512 KiB.
const SEGMENT_BYTES: u64 = 512 * 1024;
/// Bytes the ring keeps across both segments: 1 MiB.
const RING_BYTES: usize = 512 * 1024 * 2;
/// The longest read from the serial socket, and the longest replay item.
const CHUNK_BYTES: usize = 64 * 1024;
/// Chunks a live subscriber may fall behind before whole chunks are dropped.
const SUBSCRIBER_CAPACITY: usize = 256;
/// How long the client keeps trying to connect to a socket that never appears
/// (a VMM that failed before `vm.create`), before it gives up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// First pause between two connection attempts.
const CONNECT_BACKOFF_MIN: Duration = Duration::from_millis(20);
/// Longest pause between two connection attempts.
const CONNECT_BACKOFF_MAX: Duration = Duration::from_millis(200);

/// Serial of one temporary directory, so parallel tests never collide.
#[cfg(test)]
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The `serial.ring` file: which segment is being appended to.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct RingFile {
    /// The active segment, `0` or `1`.
    active: u8,
}

/// The ring and the subscribers, behind one lock that is never held across an
/// await point.
struct Inner {
    /// The newest bytes, oldest first; never more than [`RING_BYTES`]. A
    /// deque, so trimming the oldest bytes on every append is not a 1 MiB
    /// move.
    data: VecDeque<u8>,
    /// Live viewers; a sender whose receiver went away is dropped.
    subscribers: Vec<mpsc::Sender<Vec<u8>>>,
    /// Set by `delete`; a subscriber sees end of stream once it drained.
    ended: bool,
    /// The active segment, `0` or `1`.
    active: u8,
    /// Bytes already written to the active segment.
    active_len: u64,
}

/// One VM's serial client, ring and subscribers.
///
/// It lives as long as the VM's entry, so it outlives VMM restarts and the
/// streams opened on it; `delete` ends it.
pub(super) struct SerialHub {
    /// The VM's runtime directory, which holds the segments.
    dir: PathBuf,
    inner: StdMutex<Inner>,
    /// Bumped by every [`SerialHub::start_client`] and by [`SerialHub::end`];
    /// a client task whose generation is stale stops, so two tasks never read
    /// the same socket.
    generation: watch::Sender<u64>,
    next_generation: AtomicU64,
}

impl SerialHub {
    /// Loads the ring a previous agent process left in `dir`.
    ///
    /// A missing directory or unreadable segment starts from an empty ring:
    /// the ring is diagnostics, never the VM's state, so a torn ring must not
    /// stop a VM from being created or rediscovered.
    pub(super) fn new(dir: &Path) -> SerialHub {
        let (active, active_len) = load_active(dir);
        let mut data = Vec::new();
        for index in [1 - active, active] {
            if let Ok(bytes) = fs::read(segment_path(dir, index)) {
                data.extend_from_slice(&bytes);
            }
        }
        if data.len() > RING_BYTES {
            let excess = data.len() - RING_BYTES;
            data.drain(..excess);
        }
        let (generation, _) = watch::channel(0_u64);
        SerialHub {
            dir: dir.to_path_buf(),
            inner: StdMutex::new(Inner {
                data: VecDeque::from(data),
                subscribers: Vec::new(),
                ended: false,
                active,
                active_len,
            }),
            generation,
            next_generation: AtomicU64::new(0),
        }
    }

    /// Starts (or restarts) the client that reads `socket` into the ring.
    ///
    /// Called after every VMM start and for every running VM rediscovery
    /// adopts. A client task of an earlier generation stops at its next await,
    /// so a restarted VMM never has two readers.
    pub(super) fn start_client(self: &std::sync::Arc<Self>, socket: PathBuf) {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        // `send_replace`, not `send`: the task below subscribes after this,
        // and a `send` with no receiver yet would leave the value at 0, making
        // the new task look stale before it ever connected.
        self.generation.send_replace(generation);
        let hub = std::sync::Arc::clone(self);
        tokio::spawn(async move { hub.read_until_stale(socket, generation).await });
    }

    /// Ends the ring: every stream ends after its buffered chunks, and the
    /// client task stops. Called by `delete`, before the directory goes.
    pub(super) fn end(&self) {
        {
            let mut inner = lock(&self.inner);
            if inner.ended {
                return;
            }
            inner.ended = true;
            inner.subscribers.clear();
        }
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.generation.send_replace(generation);
    }

    /// Appends what the socket client read.
    ///
    /// The chunk goes to the in-memory ring and to every live subscriber under
    /// the lock, then to the active segment. A segment write failure leaves the
    /// in-memory ring intact, so viewers keep working for this agent's life.
    fn append(&self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let mut inner = lock(&self.inner);
        if inner.ended {
            return;
        }
        inner.data.extend(bytes);
        if inner.data.len() > RING_BYTES {
            let excess = inner.data.len() - RING_BYTES;
            inner.data.drain(..excess);
        }
        inner.fan_out(bytes);
        let _ = inner.write_segment(&self.dir, bytes);
    }

    /// A stream of the newest `tail_bytes` of the ring, then live output.
    ///
    /// The replay snapshot and the subscription happen under the same lock the
    /// reader appends under, so the seam has neither a gap nor a duplicate.
    /// Any number of streams may be open at once.
    pub(super) fn subscribe(&self, tail_bytes: Option<u64>) -> ByteStream {
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_CAPACITY);
        {
            let mut inner = lock(&self.inner);
            if !inner.ended {
                let kept = inner.data.len();
                let take = match tail_bytes {
                    None => kept,
                    Some(tail) => usize::try_from(tail).unwrap_or(usize::MAX).min(kept),
                };
                let start = kept - take;
                // At most 16 chunks of 64 KiB fit the channel's 256 slots, so
                // the replay is always queued whole.
                let data = inner.data.make_contiguous();
                for chunk in data[start..].chunks(CHUNK_BYTES) {
                    let _ = sender.try_send(chunk.to_vec());
                }
                inner.subscribers.push(sender);
            }
            // `ended`: the sender is dropped, so the stream is immediately
            // empty.
        }
        Box::pin(SerialStream { receiver })
    }

    /// Connects to the serial socket and appends what it reads, until the VMM
    /// goes away or a newer generation replaced this task.
    async fn read_until_stale(self: std::sync::Arc<Self>, socket: PathBuf, generation: u64) {
        let mut stop = self.generation.subscribe();
        let Some(mut stream) = self.connect(&socket, generation, &mut stop).await else {
            return;
        };
        let mut buf = vec![0_u8; CHUNK_BYTES];
        loop {
            if *stop.borrow_and_update() != generation {
                return;
            }
            let read = tokio::select! {
                changed = stop.changed() => {
                    let _ = changed;
                    return;
                }
                read = stream.read(&mut buf) => read,
            };
            match read {
                Ok(0) | Err(_) => return,
                Ok(n) => self.append(&buf[..n]),
            }
        }
    }

    /// Retries until the VMM has created the socket, the deadline passes, or a
    /// newer generation replaced this task.
    async fn connect(
        &self,
        socket: &Path,
        generation: u64,
        stop: &mut watch::Receiver<u64>,
    ) -> Option<UnixStream> {
        let deadline = Instant::now() + CONNECT_TIMEOUT;
        let mut backoff = CONNECT_BACKOFF_MIN;
        loop {
            if *stop.borrow_and_update() != generation {
                return None;
            }
            match UnixStream::connect(socket).await {
                Ok(stream) => return Some(stream),
                Err(err) if retryable(&err) && Instant::now() < deadline => {
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        _ = stop.changed() => return None,
                    }
                    backoff = (backoff * 2).min(CONNECT_BACKOFF_MAX);
                }
                Err(_) => return None,
            }
        }
    }
}

impl Inner {
    /// Sends one chunk to every live subscriber, dropping it for a viewer
    /// whose channel is full. A slow viewer never slows the guest.
    fn fan_out(&mut self, bytes: &[u8]) {
        let chunk = bytes.to_vec();
        self.subscribers
            .retain(|sender| match sender.try_send(chunk.clone()) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            });
    }

    /// Appends `bytes` to the active segment, rotating when it fills.
    fn write_segment(&mut self, dir: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            if self.active_len >= SEGMENT_BYTES {
                self.rotate(dir)?;
            }
            let room = usize::try_from(SEGMENT_BYTES - self.active_len).unwrap_or(0);
            let take = room.min(bytes.len() - written);
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(segment_path(dir, self.active))?;
            file.seek(SeekFrom::Start(self.active_len))?;
            file.write_all(&bytes[written..written + take])?;
            self.active_len += u64::try_from(take).unwrap_or(0);
            written += take;
        }
        Ok(())
    }

    /// Truncates the other segment, makes it the active one, and records it.
    fn rotate(&mut self, dir: &Path) -> io::Result<()> {
        let next = 1 - self.active;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(segment_path(dir, next))?;
        self.active = next;
        self.active_len = 0;
        // Best effort: a crash before this write is recovered by
        // `load_active`'s heuristic, and the in-memory ring is already right.
        let _ = crate::ch::state::write_json_atomic(&ring_path(dir), &RingFile { active: next });
        Ok(())
    }
}

/// The `Stream` handed out by [`SerialHub::subscribe`].
struct SerialStream {
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl Stream for SerialStream {
    type Item = Vec<u8>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<u8>>> {
        self.receiver.poll_recv(cx)
    }
}

/// `serial.0` or `serial.1` under the VM's runtime directory.
fn segment_path(dir: &Path, index: u8) -> PathBuf {
    dir.join(format!("serial.{index}"))
}

/// The `serial.ring` file under the VM's runtime directory.
fn ring_path(dir: &Path) -> PathBuf {
    dir.join("serial.ring")
}

/// Which segment is active and how full it is, from the recorded state and the
/// files themselves.
///
/// A missing state file means no rotation happened yet, so segment 0 is
/// active; a full segment 0 with data in segment 1 is the one case a crash
/// between the rotation and its state write can leave behind, and it is read
/// back as a rotated ring.
fn load_active(dir: &Path) -> (u8, u64) {
    let len = |index: u8| {
        fs::metadata(segment_path(dir, index))
            .map(|meta| meta.len().min(SEGMENT_BYTES))
            .unwrap_or(0)
    };
    let recorded = fs::read(ring_path(dir))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<RingFile>(&bytes).ok())
        .map(|file| file.active)
        .filter(|active| *active < 2);
    let (len_0, len_1) = (len(0), len(1));
    let active = recorded.unwrap_or(if len_0 >= SEGMENT_BYTES && len_1 > 0 {
        1
    } else {
        0
    });
    let active_len = if active == 0 { len_0 } else { len_1 };
    (active, active_len)
}

/// True for the connection errors a starting VMM produces before the socket
/// exists.
fn retryable(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::AddrNotAvailable
    )
}

/// Locks the ring, taking the value even when an earlier holder panicked.
fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    use tokio::io::AsyncWriteExt as _;
    use tokio::net::UnixListener;

    /// A fresh directory under the platform temp directory, named with the
    /// sequence so parallel tests in one process never share one.
    fn temp_dir(tag: &str) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "quoro-serial-{tag}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create the test directory");
        dir
    }

    /// The next item of a byte stream, or `None` at its end.
    async fn next_chunk(stream: &mut ByteStream) -> Option<Vec<u8>> {
        poll_fn(|cx| stream.as_mut().poll_next(cx)).await
    }

    /// The stream's items until `total` bytes arrived, or until it ends.
    async fn read_at_least(stream: &mut ByteStream, total: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        while bytes.len() < total {
            let Some(chunk) = next_chunk(stream).await else {
                break;
            };
            bytes.extend_from_slice(&chunk);
        }
        bytes
    }

    /// Subscribes, drops the hub (so the stream ends once drained) and reads
    /// everything the stream yields.
    fn replay(hub: SerialHub, tail: Option<u64>) -> Vec<u8> {
        let mut stream = hub.subscribe(tail);
        drop(hub);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut bytes = Vec::new();
            while let Some(chunk) = next_chunk(&mut stream).await {
                bytes.extend_from_slice(&chunk);
            }
            bytes
        })
    }

    #[tokio::test]
    async fn live_chunks_reach_a_subscriber() {
        let dir = temp_dir("live");
        let hub = SerialHub::new(&dir);
        let mut stream = hub.subscribe(None);
        hub.append(b"boot");
        hub.append(b" more");
        assert_eq!(read_at_least(&mut stream, 9).await, b"boot more");
    }

    #[test]
    fn tail_replay_keeps_the_newest_bytes() {
        let dir = temp_dir("tail");
        let hub = SerialHub::new(&dir);
        hub.append(b"0123456789");

        let mut all = hub.subscribe(None);
        let mut kept = hub.subscribe(Some(3));
        let mut none = hub.subscribe(Some(0));
        drop(hub);
        // `subscribe` is synchronous, so the replay is already queued.
        assert_eq!(drain(&mut all), b"0123456789");
        assert_eq!(drain(&mut kept), b"789");
        assert_eq!(drain(&mut none), b"");
    }

    /// Drains an already-queued stream on its own runtime.
    fn drain(stream: &mut ByteStream) -> Vec<u8> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut bytes = Vec::new();
            while let Some(chunk) = next_chunk(stream).await {
                bytes.extend_from_slice(&chunk);
            }
            bytes
        })
    }

    #[test]
    fn ring_rotates_at_one_segment() {
        let dir = temp_dir("rotate");
        let hub = SerialHub::new(&dir);

        let first = vec![b'a'; SEGMENT_BYTES as usize];
        hub.append(&first);
        assert_eq!(
            fs::metadata(segment_path(&dir, 0)).expect("0").len(),
            SEGMENT_BYTES
        );
        assert!(
            !segment_path(&dir, 1).exists(),
            "no rotation before the segment fills"
        );

        let second = vec![b'b'; 1000];
        hub.append(&second);
        assert_eq!(fs::metadata(segment_path(&dir, 1)).expect("1").len(), 1000);
        assert_eq!(
            fs::metadata(segment_path(&dir, 0)).expect("0").len(),
            SEGMENT_BYTES,
            "the older segment is kept until the newer one fills"
        );
        assert_eq!(
            serde_json::from_slice::<RingFile>(&fs::read(ring_path(&dir)).expect("state"))
                .expect("ring state")
                .active,
            1
        );

        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(replay(hub, None), expected);
    }

    #[test]
    fn the_ring_keeps_only_the_newest_mebibyte() {
        let dir = temp_dir("trim");
        let hub = SerialHub::new(&dir);
        // Three segments of distinct bytes: the oldest 512 KiB must go.
        hub.append(&vec![b'a'; SEGMENT_BYTES as usize]);
        hub.append(&vec![b'b'; SEGMENT_BYTES as usize]);
        hub.append(&vec![b'c'; SEGMENT_BYTES as usize]);
        let bytes = replay(hub, None);
        assert_eq!(bytes.len(), RING_BYTES);
        assert_eq!(&bytes[..10], b"bbbbbbbbbb");
        assert_eq!(&bytes[RING_BYTES - 10..], b"cccccccccc");
    }

    #[test]
    fn the_ring_survives_a_restart() {
        let dir = temp_dir("restart");
        {
            let hub = SerialHub::new(&dir);
            hub.append(&vec![b'a'; SEGMENT_BYTES as usize]);
            hub.append(b"second boot");
        }

        let hub = SerialHub::new(&dir);
        let bytes = replay(hub, None);
        assert_eq!(bytes.len(), SEGMENT_BYTES as usize + 11);
        assert_eq!(&bytes[..8], b"aaaaaaaa");
        assert_eq!(&bytes[SEGMENT_BYTES as usize..], b"second boot");
    }

    #[tokio::test]
    async fn every_subscriber_gets_every_chunk() {
        let dir = temp_dir("fan-out");
        let hub = SerialHub::new(&dir);
        let mut first = hub.subscribe(None);
        let mut second = hub.subscribe(None);
        hub.append(b"one");
        hub.append(b"two");
        assert_eq!(read_at_least(&mut first, 6).await, b"onetwo");
        assert_eq!(read_at_least(&mut second, 6).await, b"onetwo");
    }

    #[tokio::test]
    async fn a_slow_subscriber_never_blocks_the_reader() {
        let dir = temp_dir("slow");
        let hub = SerialHub::new(&dir);
        // Never read from this one: its channel fills up.
        let _slow = hub.subscribe(None);
        for _ in 0..(SUBSCRIBER_CAPACITY + 8) {
            hub.append(b"x");
        }
        // A subscriber that joins afterwards still gets live output, and the
        // slow one never made an append wait.
        let mut live = hub.subscribe(Some(0));
        hub.append(b"after");
        assert_eq!(read_at_least(&mut live, 5).await, b"after");
    }

    #[tokio::test]
    async fn ending_the_ring_ends_every_stream() {
        let dir = temp_dir("end");
        let hub = SerialHub::new(&dir);
        let mut stream = hub.subscribe(None);
        hub.append(b"last");
        hub.end();
        assert_eq!(read_at_least(&mut stream, 4).await, b"last");
        assert_eq!(next_chunk(&mut stream).await, None);

        let mut after = hub.subscribe(None);
        assert_eq!(next_chunk(&mut after).await, None);
        // Appends after the end change nothing.
        hub.append(b"ignored");
        assert_eq!(next_chunk(&mut after).await, None);
    }

    #[tokio::test]
    async fn the_client_reads_a_fake_ch_serial_socket() {
        let dir = temp_dir("client");
        let socket = dir.join("serial.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let hub = std::sync::Arc::new(SerialHub::new(&dir));
        let mut stream = hub.subscribe(None);
        hub.start_client(socket.clone());

        let (mut connection, _) = listener.accept().await.expect("accept");
        connection
            .write_all(b"kernel: boot\n")
            .await
            .expect("write");
        connection.flush().await.expect("flush");
        assert_eq!(read_at_least(&mut stream, 13).await, b"kernel: boot\n");

        // The bytes are on disk as well, under the active segment.
        assert_eq!(
            fs::read(segment_path(&dir, 0)).expect("segment"),
            b"kernel: boot\n"
        );
        hub.end();
    }

    #[tokio::test]
    async fn a_socket_that_never_appears_is_stopped_by_end() {
        let dir = temp_dir("missing");
        let hub = std::sync::Arc::new(SerialHub::new(&dir));
        let mut stream = hub.subscribe(None);
        // No listener at this path: the client retries, and `end` stops it.
        hub.start_client(dir.join("absent.sock"));
        hub.end();
        assert_eq!(next_chunk(&mut stream).await, None);
    }
}
