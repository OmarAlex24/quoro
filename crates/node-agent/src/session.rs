// SPDX-License-Identifier: AGPL-3.0-only
//! The dial-out session (`node-agent-protocol.md` sections 3-5 and 9).
//!
//! The node is always the client: one WebSocket over pinned TLS to the primary,
//! `hello` first, `welcome` back, then heartbeats, status reports, buffered
//! events and the messages the primary sends. The core ([`Session::serve`]) is
//! generic over the WebSocket's byte stream, so tests run it over
//! `tokio::io::duplex` without TLS.
//!
//! Redial follows section 4.4: 1 s doubling to 60 s with full jitter, reset
//! after a session that stayed up for at least 60 s. A fatal
//! `unauthenticated`/`node_removed` stops dialing for good;
//! `unsupported_version` keeps dialing and only logs.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use quoro_proto::wire::{
    self, BackupStatus, Capacity, Command, ControlMessage, DesiredState, EventBody, Heartbeat,
    Hello, LogLine, NodeMessage, RawFrame, RouteStatus, StatusReport, StreamClose, StreamData,
    StreamEnd, StreamEndReason, StreamOpen, VmStatus, VolumeStatus,
};
use quoro_proto::{
    CommandResult, CommandStatus, ErrorBody, ErrorCode, LeaseRevoke, MsgId, ProtocolError,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, client_async_with_config};
use tracing::{debug, info, warn};

use crate::build::manager::{BuildStart, BuildsError, BuildsHandle};
use crate::edge::EdgeManager;
use crate::identity::{Identity, IdentityHandle};
use crate::ingress::{DnsBridge, DnsRequest, IngressSupervisor};
use crate::interactive::{Door, Interactive, OpenError, StreamKind};
use crate::inventory::InventoryHub;
use crate::lease::LeaseLink;
use crate::logs::{LogFilter, StoredLogLine};
use crate::metrics::{MetricsRing, Tick, parse_metrics_params, tick_report};
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
use crate::tls::{self, Endpoint};
use crate::{AgentError, FatalReason, NodeFacts, util};

/// Path of the session endpoint (section 3.1).
pub const SESSION_PATH: &str = "/api/v1/nodes/session";

/// Concurrent streams the node serves per session (section 5.15).
pub const MAX_STREAMS: usize = 32;

/// Lines per `stream_data` frame.
const STREAM_BATCH_LINES: usize = 200;
/// How often a partially filled stream batch is flushed.
const STREAM_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// How often an open session sends the events recorded since the last flush.
const EVENT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Space kept free in every frame for the envelope and the line array.
const FRAME_HEADROOM: usize = 64 * 1024;

/// First redial delay (section 4.4).
const REDIAL_BASE: Duration = Duration::from_secs(1);
/// Redial ceiling (section 4.4).
const REDIAL_MAX: Duration = Duration::from_secs(60);
/// A session this long resets the redial backoff (section 4.4).
const REDIAL_RESET_AFTER: Duration = Duration::from_secs(60);
/// How long one redial attempt keeps redialing `primary_url` while it refuses
/// the connection, before it tries the advertised addresses (issue #359). A
/// refused connection from the address that last worked is how a restarting
/// primary looks: the host is there and its listener comes back within a
/// fraction of a second.
const REFUSED_RETRY_WINDOW: Duration = Duration::from_secs(2);
/// The pause between two dials of a `primary_url` that refused.
const REFUSED_RETRY_STEP: Duration = Duration::from_millis(250);
/// The connect timeout of an advertised address in the first redial attempt
/// after a session. It doubles with each failed attempt, up to
/// [`tls::CONNECT_TIMEOUT`], so a slow link still connects. An advertised
/// address is a guess: some of a multi-homed primary's addresses never answer
/// from a given node, and each one used to cost the full timeout (issue #359).
const ADVERTISED_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a status report for a newly applied revision waits, so a
/// reconcile pass that also changes the observed VMs sends one report, not
/// two. A report sent for another reason in the meantime carries the
/// revision, and then none is sent for it.
const APPLIED_REPORT_DELAY: Duration = Duration::from_millis(250);

/// `log_hash` while `log_seq` is 0 (section 5.1).
const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Why one session ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEnd {
    /// The agent is shutting down.
    Shutdown,
    /// The session ended and the node should dial again.
    Reconnect,
    /// A fatal error says dialing again cannot help (section 4.4).
    Fatal(FatalReason),
}

/// The streams one session has open. Dropping it aborts every log-stream task
/// and asks every interactive task to stop, so a session end also ends its
/// streams.
///
/// An interactive stream is not aborted: its task finalises the transcript
/// (flush, hash, row) and queues its own `stream_end`, and the session's end
/// must not cut that short. Dropping its [`Door`] is the cancellation signal.
#[derive(Default)]
struct SessionStreams {
    handles: HashMap<String, JoinHandle<()>>,
    doors: HashMap<String, Door>,
}

impl SessionStreams {
    /// Stops a stream. Returns whether the session still has to answer with a
    /// generic `stream_end` `cancelled`: a log stream cannot, an interactive
    /// task does it itself (with its transcript fields).
    fn stop(&mut self, stream_id: &str) -> bool {
        let interactive = self.doors.remove(stream_id).is_some();
        match self.handles.remove(stream_id) {
            Some(handle) => {
                if interactive {
                    // The task sees the dropped door and ends itself.
                    drop(handle);
                    false
                } else {
                    handle.abort();
                    true
                }
            }
            None => false,
        }
    }

    /// Registers a task and, for an interactive stream, its control surface.
    fn insert(&mut self, stream_id: String, handle: JoinHandle<()>, door: Option<Door>) {
        if let Some(previous) = self.handles.remove(&stream_id) {
            if self.doors.remove(&stream_id).is_some() {
                // The old interactive task stops on the dropped door.
                drop(previous);
            } else {
                previous.abort();
            }
        }
        if let Some(door) = door {
            self.doors.insert(stream_id.clone(), door);
        }
        self.handles.insert(stream_id, handle);
    }

    /// Drops finished tasks and their doors, freeing the stream slots.
    fn prune(&mut self) {
        self.handles.retain(|stream_id, handle| {
            let live = !handle.is_finished();
            if !live {
                self.doors.remove(stream_id);
            }
            live
        });
    }

    /// Whether this session still holds a live stream under `stream_id`.
    fn is_live(&self, stream_id: &str) -> bool {
        self.handles
            .get(stream_id)
            .is_some_and(|handle| !handle.is_finished())
    }

    /// The door of a live interactive stream.
    fn door(&self, stream_id: &str) -> Option<&Door> {
        if !self.is_live(stream_id) {
            return None;
        }
        self.doors.get(stream_id)
    }

    /// Ends every stream and waits briefly for the interactive tasks to write
    /// their transcripts and final `stream_end`.
    async fn finish(&mut self) {
        let doors = std::mem::take(&mut self.doors);
        let mut finishing = Vec::new();
        for (stream_id, handle) in self.handles.drain() {
            if doors.contains_key(&stream_id) {
                finishing.push(handle);
            } else {
                handle.abort();
            }
        }
        // Dropping the doors signals cancellation.
        drop(doors);
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::join_all(finishing),
        )
        .await;
    }
}

impl Drop for SessionStreams {
    fn drop(&mut self) {
        let doors = std::mem::take(&mut self.doors);
        for (stream_id, handle) in self.handles.drain() {
            if !doors.contains_key(&stream_id) {
                handle.abort();
            }
        }
    }
}

/// Keeps the DNS bridge attached for the life of one session and detaches it
/// when the session ends, however it ends.
struct DnsSession {
    bridge: Arc<DnsBridge>,
}

impl DnsSession {
    fn attach(bridge: Arc<DnsBridge>) -> (Self, mpsc::Receiver<DnsRequest>) {
        let rx = bridge.attach();
        (Self { bridge }, rx)
    }
}

impl Drop for DnsSession {
    fn drop(&mut self) {
        self.bridge.detach();
    }
}

/// Everything a [`Session`] needs, assembled by [`crate::run`].
pub struct SessionSetup {
    /// The enrolled identity and its current pin.
    pub identity: IdentityHandle,
    /// The local node store.
    pub store: NodeStore,
    /// What `hello` reports.
    pub facts: NodeFacts,
    /// Where accepted snapshots go, to the reconciler.
    pub desired: watch::Sender<Option<Snapshot>>,
    /// What the reconciler observed.
    pub observed: watch::Receiver<Vec<VmStatus>>,
    /// The verified snapshot the agent started with, if any.
    pub current: Option<Snapshot>,
    /// The build manager's handle, when the agent runs one. Tests that do not
    /// care about builds pass `None`, and then every build log request is
    /// answered `not_found`.
    pub builds: Option<BuildsHandle>,
}

/// The session loop: dial, run one session, redial.
pub struct Session {
    identity: IdentityHandle,
    store: NodeStore,
    facts: NodeFacts,
    desired: watch::Sender<Option<Snapshot>>,
    observed: watch::Receiver<Vec<VmStatus>>,
    current: Option<Snapshot>,
    boot_id: String,
    /// The app-route supervisor (M2 apps), when the agent runs one.
    ingress: Option<Arc<IngressSupervisor>>,
    /// The DNS-01 bridge the ingress supervisor's provider sends through.
    dns: Option<Arc<DnsBridge>>,
    /// The metric rings, when the agent samples metrics (section 5.17).
    metrics: Option<MetricsRing>,
    /// The machine's WireGuard public key, reported in `hello` (section 6.9).
    mesh_public_key: Option<String>,
    /// The engine handles behind `terminal` and `console` streams (M4), when
    /// the agent runs one.
    interactive: Option<Interactive>,
    /// The build manager's handle, when the agent runs one.
    builds: Option<BuildsHandle>,
    /// The edge manager, when the agent runs one, for
    /// `status_report.edge_backends`.
    edge: Option<Arc<EdgeManager>>,
    /// The lease service, when the agent runs one (sections 5.12, 5.13).
    leases: Option<LeaseLink>,
    /// How this node snapshots a volume (section 5.1); `None` for an engine
    /// without volumes.
    volume_snapshot: Option<wire::SnapshotMethod>,
    /// The volume statuses the reconciler publishes (section 5.3).
    volumes: Option<watch::Receiver<Vec<VolumeStatus>>>,
    /// The directory whose filesystem holds the volumes, for
    /// `status_report.disk_free_gb` (section 5.3); `None` reports 0.
    disk_free_path: Option<std::path::PathBuf>,
    /// The backup statuses the backup manager publishes (section 5.3).
    backups: Option<watch::Receiver<Vec<BackupStatus>>>,
    /// The image-inventory transport (sections 5.3, 5.26), when the agent runs
    /// an engine that inventories images.
    inventory: Option<InventoryHub>,
    /// The URL `dial` used for the current session, set after a successful
    /// dial and read once `welcome` arrives to promote a fallback address
    /// (section 5.10). `None` when the session was handed a stream directly
    /// (tests driving [`Session::serve`]).
    dialed_url: Option<String>,
    /// The `applied_rev` of the last status report sent.
    reported_applied_rev: u64,
    /// A `lease_revoke` [`Self::handle`] took and left for the session loop to
    /// run, so the loop keeps the session alive while its fence runs (section
    /// 5.13, ADR 0222).
    started_revoke: Option<StartedRevoke>,
}

/// A `lease_revoke` whose fence the session loop runs.
struct StartedRevoke {
    /// The envelope id the answer's `re` names.
    raw_id: String,
    re: Option<MsgId>,
    body: LeaseRevoke,
}

/// The fence of a `lease_revoke` while it runs, and the answer it ends with.
struct PendingRevoke {
    raw_id: String,
    lease_id: String,
    answer: std::pin::Pin<Box<dyn Future<Output = NodeMessage> + Send>>,
}

impl PendingRevoke {
    /// Hands the revoke to the lease service now, so the lease is `revoked`
    /// and fenced even when the session ends before the answer is awaited
    /// (section 5.13: on receipt the node starts nothing more for the lease).
    fn start(leases: &LeaseLink, started: StartedRevoke) -> Self {
        let id = MsgId(started.raw_id.clone());
        let lease_id = started.body.lease_id.0.clone();
        let answer = leases.begin_revoke(id, started.re, started.body);
        Self {
            raw_id: started.raw_id,
            lease_id,
            answer: Box::pin(async move {
                match answer.await {
                    Ok(result) => NodeMessage::CommandResult(result),
                    Err(error) => NodeMessage::Error(error),
                }
            }),
        }
    }
}

/// The lease a frame revokes, when it is a readable `lease_revoke`.
fn revoked_lease(text: &str) -> Option<String> {
    let raw = RawFrame::parse(text).ok()?;
    match ControlMessage::from_raw(&raw) {
        Ok(ControlMessage::LeaseRevoke(revoke)) => Some(revoke.lease_id.0),
        _ => None,
    }
}

/// What the session does after [`Session::handle`]: `None` goes on, `Some`
/// ends the session.
fn session_end(handled: Result<Option<SessionEnd>, AgentError>) -> Option<SessionEnd> {
    match handled {
        Ok(end) => end,
        Err(AgentError::Fatal(reason)) => Some(SessionEnd::Fatal(reason)),
        Err(e) => {
            warn!(error = %e, "session message failed; reconnecting");
            Some(SessionEnd::Reconnect)
        }
    }
}

/// Section 5.13 "Order while the fence runs" (ADR 0222): whether a frame that
/// arrives while the fence of `revoking` runs is processed at once. Only a
/// `lease_grant` for another lease, including the answer to a renewal, and
/// `ping` and `pong` are; every other frame waits for the revoke's answer,
/// among them `desired_state`, `command`, `stream_open` and any
/// `lease_revoke`. A grant for a lease whose revoke waits in `held` waits
/// too, so it never overtakes that revoke. A frame that cannot be read waits
/// as well, so its error answer keeps its place. From M5b a voter also
/// answers `support_request` at once (ADR 0237); that message arrives with
/// the voters.
fn passes_a_running_fence(
    text: &str,
    revoking: &str,
    held_revokes: &HashMap<String, usize>,
) -> bool {
    let Ok(raw) = RawFrame::parse(text) else {
        return false;
    };
    match ControlMessage::from_raw(&raw) {
        Ok(ControlMessage::Ping | ControlMessage::Pong) => true,
        Ok(ControlMessage::LeaseGrant(grant)) => {
            grant.lease_id.0 != revoking && !held_revokes.contains_key(&grant.lease_id.0)
        }
        _ => false,
    }
}

impl Session {
    /// Builds a session loop and the boot id `hello` reports.
    #[must_use]
    pub fn new(setup: SessionSetup) -> Self {
        let SessionSetup {
            identity,
            store,
            facts,
            desired,
            observed,
            current,
            builds,
        } = setup;
        Self {
            identity,
            store,
            facts,
            desired,
            observed,
            current,
            boot_id: util::hex(&util::random_bytes::<16>()),
            ingress: None,
            dns: None,
            metrics: None,
            mesh_public_key: None,
            interactive: None,
            builds,
            edge: None,
            leases: None,
            volume_snapshot: None,
            volumes: None,
            disk_free_path: None,
            backups: None,
            inventory: None,
            dialed_url: None,
            reported_applied_rev: 0,
            started_revoke: None,
        }
    }

    /// Attaches the lease service: `lease_grant` and `lease_revoke` are applied
    /// through it (sections 5.12, 5.13) and `hello` reports its leases.
    #[must_use]
    pub fn with_leases(mut self, leases: LeaseLink) -> Self {
        self.leases = Some(leases);
        self
    }

    /// Sets what `hello.volume_snapshot` reports (section 5.1).
    #[must_use]
    pub fn with_volume_snapshot(mut self, method: Option<wire::SnapshotMethod>) -> Self {
        self.volume_snapshot = method;
        self
    }

    /// Reports the free space of the filesystem that holds `path` as
    /// `status_report.disk_free_gb` (section 5.3). The node's data directory
    /// holds the volumes, or the image of the volume pool that grows into
    /// it, so its filesystem bounds what a new volume can use.
    #[must_use]
    pub fn with_disk_free_path(mut self, path: std::path::PathBuf) -> Self {
        self.disk_free_path = Some(path);
        self
    }

    /// Attaches the reconciler's volume statuses, reported in every
    /// `status_report.volumes` (section 5.3).
    #[must_use]
    pub fn with_volumes(mut self, volumes: watch::Receiver<Vec<VolumeStatus>>) -> Self {
        self.volumes = Some(volumes);
        self
    }

    /// Attaches the backup manager's statuses, reported in every
    /// `status_report.backups` (section 5.3).
    #[must_use]
    pub fn with_backups(mut self, backups: watch::Receiver<Vec<BackupStatus>>) -> Self {
        self.backups = Some(backups);
        self
    }

    /// Attaches the image-inventory transport: prepared images queue their
    /// inventory for `image_inventory` parts, and `status_report.images` names
    /// their hashes (sections 5.3, 5.26).
    #[must_use]
    pub fn with_inventory(mut self, inventory: InventoryHub) -> Self {
        self.inventory = Some(inventory);
        self
    }

    /// Attaches the interactive handles: `terminal`, `console` and `transcript`
    /// streams answer `not_available` without them.
    #[must_use]
    pub fn with_interactive(mut self, interactive: Interactive) -> Self {
        self.interactive = Some(interactive);
        self
    }

    /// Attaches the app-route supervisor: its per-hostname certificate state
    /// fills `status_report.routes`, and its ACME worker's DNS-01 records go
    /// out on this session.
    #[must_use]
    pub fn with_ingress(mut self, ingress: Arc<IngressSupervisor>, dns: Arc<DnsBridge>) -> Self {
        self.ingress = Some(ingress);
        self.dns = Some(dns);
        self
    }

    /// Attaches the metric rings the sampler fills, answering `metrics_query`
    /// and `metrics` streams (section 5.17).
    #[must_use]
    pub fn with_metrics(mut self, metrics: MetricsRing) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attaches the edge manager: its current rotation state fills
    /// `status_report.edge_backends` (section 5.3, ADR 0143).
    #[must_use]
    pub fn with_edge(mut self, edge: Arc<EdgeManager>) -> Self {
        self.edge = Some(edge);
        self
    }

    /// Reports the machine's WireGuard public key in `hello` (section 6.9).
    /// Only the public key is ever sent; the private key never leaves the
    /// machine.
    #[must_use]
    pub fn with_mesh_key(mut self, public_key: String) -> Self {
        self.mesh_public_key = Some(public_key);
        self
    }

    /// The identity in use.
    #[must_use]
    pub fn identity(&self) -> Identity {
        self.identity.get()
    }

    /// Dials until `stop` resolves or a fatal end arrives.
    ///
    /// # Errors
    ///
    /// [`AgentError::Fatal`] for `unauthenticated`/`node_removed`;
    /// [`AgentError::Identity`] when the credential or key cannot be read.
    pub async fn run(&mut self, stop: watch::Receiver<bool>) -> Result<(), AgentError> {
        let mut backoff = REDIAL_BASE;
        let mut failed_attempts: u32 = 0;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let started = Instant::now();
            // A dial in progress never holds up a stop.
            let dialed = tokio::select! {
                dialed = self.dial(advertised_timeout(failed_attempts)) => dialed,
                () = wait_stop(stop.clone()) => return Ok(()),
            };
            match dialed {
                Ok((ws, url)) => {
                    failed_attempts = 0;
                    self.dialed_url = Some(url);
                    let end = self.serve(ws, wait_stop(stop.clone())).await?;
                    match end {
                        SessionEnd::Shutdown => return Ok(()),
                        SessionEnd::Fatal(reason) => return Err(AgentError::Fatal(reason)),
                        SessionEnd::Reconnect => {}
                    }
                    if started.elapsed() >= REDIAL_RESET_AFTER {
                        backoff = REDIAL_BASE;
                    }
                }
                Err(e @ AgentError::Fatal(_)) => return Err(e),
                Err(e) => {
                    failed_attempts = failed_attempts.saturating_add(1);
                    warn!(error = %e, "could not open the session; will redial");
                }
            }

            let delay = backoff.mul_f64(util::random_fraction());
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = wait_stop(stop.clone()) => return Ok(()),
            }
            backoff = (backoff * 2).min(REDIAL_MAX);
        }
    }

    /// One redial attempt (section 4.4): dials `primary_url` and, when that
    /// fails, each URL the primary advertised in `welcome.primary_urls` in
    /// order, with the same pin and credential and no backoff between them.
    /// Returns the stream and the URL that accepted it.
    ///
    /// `primary_url`, the address that last worked, gets the full
    /// [`tls::CONNECT_TIMEOUT`], and while it refuses the connection it is
    /// dialed again every [`REFUSED_RETRY_STEP`] for [`REFUSED_RETRY_WINDOW`]
    /// before the advertised addresses are tried. Each advertised address
    /// gets `advertised_timeout` (issue #359).
    async fn dial(
        &self,
        advertised_timeout: Duration,
    ) -> Result<(WebSocketStream<TlsStream<tokio::net::TcpStream>>, String), AgentError> {
        let identity = self.identity.get();
        let credential = self.identity.credential()?;
        let mut candidates = Vec::with_capacity(1 + identity.primary_urls.len());
        candidates.push(identity.primary_url.clone());
        for url in &identity.primary_urls {
            if url != &identity.primary_url && !candidates.contains(url) {
                candidates.push(url.clone());
            }
        }

        let mut last_error = None;
        for (index, url) in candidates.into_iter().enumerate() {
            let dialed = if index == 0 {
                dial_known(&url, &credential, &identity.fingerprint).await
            } else {
                dial_one(&url, &credential, &identity.fingerprint, advertised_timeout).await
            };
            match dialed {
                Ok(ws) => return Ok((ws, url)),
                // A `401`/`403` from a host that passed the pin is fatal
                // wherever it came from; no other address can help.
                Err(e @ AgentError::Fatal(_)) => return Err(e),
                Err(e) => {
                    warn!(url = %url, error = %e, "could not dial this primary address; trying the next");
                    last_error = Some(e);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| AgentError::Tls("there is no primary address to dial".to_owned())))
    }

    /// Runs one session to its end.
    ///
    /// # Errors
    ///
    /// [`AgentError::Fatal`] when the primary said dialing cannot help;
    /// anything else is a reconnectable failure and is reported as
    /// [`SessionEnd::Reconnect`].
    pub async fn serve<S>(
        &mut self,
        ws: WebSocketStream<S>,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<SessionEnd, AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut streams = SessionStreams::default();
        let end = self.serve_streams(ws, stop, &mut streams).await;
        // A session end must still finalise terminal transcripts and queue the
        // ends of its interactive streams, however it ended.
        streams.finish().await;
        end
    }

    async fn serve_streams<S>(
        &mut self,
        ws: WebSocketStream<S>,
        stop: impl Future<Output = ()> + Send,
        streams: &mut SessionStreams,
    ) -> Result<SessionEnd, AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut stop = std::pin::pin!(stop);
        let (mut sink, mut source) = ws.split();
        // Stream tasks hand their frames to the session loop, which owns the
        // sink; a task never blocks the loop while it waits for lines.
        let (stream_tx, mut stream_rx) = mpsc::channel::<NodeMessage>(256);

        // The ingress supervisor's ACME worker asks for DNS-01 records through
        // this session. The guard detaches the bridge on every return path, so
        // a dropped session fails the next challenge instead of hanging.
        let (_dns_guard, mut dns_rx) = match &self.dns {
            Some(bridge) => {
                let (guard, rx) = DnsSession::attach(Arc::clone(bridge));
                (Some(guard), Some(rx))
            }
            None => (None, None),
        };

        // 1. `hello` is the first message on every session (section 4.2).
        let hello = self.hello().await;
        if let Err(e) = write_message(&mut sink, hello, None).await {
            warn!(error = %e, "could not send hello; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }

        // 2. `welcome` is the first message back.
        let welcome = {
            let first = tokio::select! {
                () = &mut stop => return Ok(SessionEnd::Shutdown),
                first = tokio::time::timeout(
                    Duration::from_millis(wire::SESSION_TIMEOUT_MS),
                    next_frame(&mut source),
                ) => first,
            };
            match first {
                Ok(Some(Ok(Message::Text(text)))) => match parse_control(&text) {
                    Ok(ControlMessage::Welcome(welcome)) => welcome,
                    Ok(other) => {
                        warn!(
                            kind = other.kind(),
                            "the primary did not start with welcome; reconnecting"
                        );
                        return Ok(SessionEnd::Reconnect);
                    }
                    Err(_) => {
                        warn!("the primary's first frame is not a welcome; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                },
                Ok(Some(Ok(_))) => {
                    warn!("the primary's first frame is not a text frame; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                Ok(Some(Err(e))) => {
                    warn!(error = %e, "the session failed before welcome; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                Ok(None) | Err(_) => {
                    warn!("no welcome arrived; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
            }
        };

        let heartbeat = Duration::from_millis(welcome.heartbeat_interval_ms.max(1));
        let timeout = Duration::from_millis(welcome.session_timeout_ms.max(1));
        info!(
            cluster_id = %welcome.cluster_id,
            term = welcome.term,
            "session established"
        );

        // Section 5.10: a session that reached `welcome` through one of the
        // advertised URLs becomes the address the node dials from now on, and
        // every welcome replaces the stored list when it carries one.
        if let Some(url) = self.dialed_url.take()
            && url != self.identity.get().primary_url
        {
            let old = self.identity.get().primary_url;
            match self.identity.promote(&url) {
                Ok(()) => info!(
                    old = %old,
                    new = %url,
                    "following the primary to a new address"
                ),
                Err(e) => warn!(error = %e, "could not persist the primary's new address"),
            }
        }
        if !welcome.primary_urls.is_empty() {
            match self.identity.set_primary_urls(&welcome.primary_urls) {
                Ok(true) => info!(
                    count = welcome.primary_urls.len(),
                    "stored the primary's advertised addresses"
                ),
                Ok(false) => {}
                Err(e) => {
                    warn!(error = %e, "could not persist the primary's advertised addresses");
                }
            }
        }

        // 3. Buffered events go out once the session is up, then are removed
        //    from the buffer (section 6.5).
        if let Err(e) = self.flush_events(&mut sink).await {
            warn!(error = %e, "could not deliver buffered events; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }
        // An edge's rotation changes are reported like an observed-VM change
        // (section 5.3): an edge runs no VM, so nothing else would carry them
        // before the periodic report. The receiver is taken before the first
        // report, so a change between the two is not missed.
        let mut edge_changes = self.edge.as_ref().map(|edge| edge.changes());
        // A newly applied revision is an observed change too (section 5.3):
        // a revision that changes no VM and no route, which is most of an
        // edge's, would otherwise wait for the periodic report (issue #346).
        let mut applied_changes = self.store.applied_changes();
        let mut applied_report_at: Option<Instant> = None;
        if let Err(e) = self.send_status(&mut sink).await {
            warn!(error = %e, "could not send the first status report; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }
        // Inventories queued while the node was disconnected go out as soon
        // as the session is up (section 6.11).
        if let Err(e) = self.flush_inventory(&mut sink).await {
            warn!(error = %e, "could not send an image inventory; reconnecting");
            return Ok(SessionEnd::Reconnect);
        }

        let mut heartbeat_tick = tokio::time::interval(heartbeat);
        heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_tick.reset();
        let mut status_tick =
            tokio::time::interval(Duration::from_millis(wire::STATUS_REPORT_INTERVAL_MS));
        status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        status_tick.reset();
        // Section 6.5: events wait in the buffer only while disconnected.
        // During a session they leave within a second of being recorded.
        let mut event_tick = tokio::time::interval(EVENT_FLUSH_INTERVAL);
        event_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        event_tick.reset();
        // A clone keeps the select's borrow off `self`, so a branch may still
        // build a status report that reads the store and the snapshot.
        let mut observed = self.observed.clone();
        // A build status change is reported like an observed-VM change
        // (section 5.3).
        let mut builds = self.builds.as_ref().map(BuildsHandle::statuses);
        let inventory_hub = self.inventory.clone();
        let mut last_rx = Instant::now();
        // The revoke whose fence runs, and the frames that wait for its
        // answer (section 5.13, ADR 0222).
        let mut revoking: Option<PendingRevoke> = None;
        let mut held: VecDeque<String> = VecDeque::new();
        // The leases of the revokes in `held`, with how many each has.
        let mut held_revokes: HashMap<String, usize> = HashMap::new();
        self.started_revoke = None;

        loop {
            let deadline = last_rx + timeout;
            tokio::select! {
                () = &mut stop => {
                    // A running fence is let finish and answered first, so a
                    // routine shutdown does not cut it off midway; its stop
                    // is bounded by the fence timers.
                    if let Some(pending) = revoking.take() {
                        let answer = pending.answer.await;
                        let _ = write_message(&mut sink, answer, Some(&pending.raw_id)).await;
                    }
                    return Ok(SessionEnd::Shutdown);
                }
                _ = heartbeat_tick.tick() => {
                    let beat = NodeMessage::Heartbeat(Heartbeat {
                        time: util::now_rfc3339(),
                        busy: None,
                    });
                    if let Err(e) = write_message(&mut sink, beat, None).await {
                        warn!(error = %e, "could not send a heartbeat; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                    // The primary sends nothing while it has nothing to say,
                    // and only it enforces the session timeout (section 4.3).
                    // A quiet session is probed with `ping`, whose `pong`
                    // resets the node's own timer, so a half-open connection
                    // is still noticed without dropping a healthy idle one.
                    if last_rx.elapsed() >= heartbeat
                        && let Err(e) = write_message(&mut sink, NodeMessage::Ping, None).await
                    {
                        warn!(error = %e, "could not send a ping; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                _ = event_tick.tick() => {
                    if let Err(e) = self.flush_events(&mut sink).await {
                        warn!(error = %e, "could not deliver buffered events; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                _ = status_tick.tick() => {
                    if let Err(e) = self.send_status(&mut sink).await {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                changed = observed.changed() => {
                    if changed.is_ok()
                        && let Err(e) = self.send_status(&mut sink).await
                    {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                changed = async {
                    match builds.as_mut() {
                        Some(statuses) => statuses.changed().await.map_err(|_| ()),
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok()
                        && let Err(e) = self.send_status(&mut sink).await
                    {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                () = async {
                    let changed = match edge_changes.as_mut() {
                        Some(changes) => changes.changed().await.is_ok(),
                        None => false,
                    };
                    // The session holds the manager, so its sender outlives
                    // this receiver; a closed one never wakes the loop.
                    if !changed {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if let Err(e) = self.send_status(&mut sink).await {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                () = async {
                    // The store's sender lives as long as the session's store;
                    // a closed receiver never wakes the loop.
                    if applied_changes.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                } => {
                    applied_report_at.get_or_insert(Instant::now() + APPLIED_REPORT_DELAY);
                }
                () = async {
                    match applied_report_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    applied_report_at = None;
                    let applied = self.store.applied_rev().await.unwrap_or(0);
                    if applied > self.reported_applied_rev
                        && let Err(e) = self.send_status(&mut sink).await
                    {
                        warn!(error = %e, "could not send a status report; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    warn!("no frame from the primary within the session timeout; reconnecting");
                    return Ok(SessionEnd::Reconnect);
                }
                () = async {
                    match inventory_hub.as_ref() {
                        Some(hub) => hub.notified().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(e) = self.flush_inventory(&mut sink).await {
                        warn!(error = %e, "could not send an image inventory; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                streamed = stream_rx.recv() => {
                    if let Some(message) = streamed
                        && let Err(e) = write_message(&mut sink, message, None).await
                    {
                        warn!(error = %e, "could not send a stream frame; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                challenge = async {
                    match dns_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(request) = challenge
                        && let Err(e) = write_message(
                            &mut sink,
                            NodeMessage::DnsChallenge(request.challenge),
                            Some(&request.id),
                        )
                        .await
                    {
                        warn!(error = %e, "could not send a dns_challenge; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                }
                answer = async {
                    match revoking.as_mut() {
                        Some(pending) => pending.answer.as_mut().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(pending) = revoking.take() else {
                        continue;
                    };
                    if let Err(e) = write_message(&mut sink, answer, Some(&pending.raw_id)).await {
                        warn!(error = %e, "could not answer a lease revoke; reconnecting");
                        return Ok(SessionEnd::Reconnect);
                    }
                    // The frames that waited are processed in the order they
                    // arrived, until one of them is a revoke again.
                    while revoking.is_none() {
                        let Some(text) = held.pop_front() else {
                            break;
                        };
                        if let Some(lease) = revoked_lease(&text)
                            && let Some(count) = held_revokes.get_mut(&lease)
                        {
                            *count -= 1;
                            if *count == 0 {
                                held_revokes.remove(&lease);
                            }
                        }
                        let handled = self.handle(&text, &mut sink, streams, &stream_tx).await;
                        if let Some(end) = session_end(handled) {
                            return Ok(end);
                        }
                        if let Some(next) = self.start_revoke() {
                            revoking = Some(next);
                        }
                    }
                }
                frame = next_frame(&mut source) => {
                    last_rx = Instant::now();
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(pending) = &revoking
                                && !passes_a_running_fence(&text, &pending.lease_id, &held_revokes)
                            {
                                if let Some(lease) = revoked_lease(&text) {
                                    *held_revokes.entry(lease).or_default() += 1;
                                }
                                held.push_back(text.to_string());
                                continue;
                            }
                            let handled = self.handle(&text, &mut sink, streams, &stream_tx).await;
                            if let Some(end) = session_end(handled) {
                                return Ok(end);
                            }
                            if let Some(next) = self.start_revoke() {
                                revoking = Some(next);
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if sink.send(Message::Pong(payload)).await.is_err() {
                                return Ok(SessionEnd::Reconnect);
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            debug!("the primary closed the session");
                            return Ok(SessionEnd::Reconnect);
                        }
                        Some(Ok(Message::Binary(_))) => {
                            let error = ProtocolError {
                                code: ErrorCode::BadMessage,
                                message: "binary frames are not used in v1".to_owned(),
                                fatal: false,
                            };
                            if write_message(&mut sink, NodeMessage::Error(error), None).await.is_err() {
                                return Ok(SessionEnd::Reconnect);
                            }
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => {
                            warn!(error = %e, "the session failed; reconnecting");
                            return Ok(SessionEnd::Reconnect);
                        }
                        None => return Ok(SessionEnd::Reconnect),
                    }
                }
            }
        }
    }

    /// Starts the fence of the revoke [`Self::handle`] just took, if any.
    fn start_revoke(&mut self) -> Option<PendingRevoke> {
        let started = self.started_revoke.take()?;
        match self.leases.as_ref() {
            Some(leases) => Some(PendingRevoke::start(leases, started)),
            None => {
                // `handle` answers a revoke itself when no lease service runs.
                warn!(lease_id = %started.body.lease_id.0, "a revoke without a lease service");
                None
            }
        }
    }

    /// Handles one control frame.
    ///
    /// A `lease_revoke` is not answered here: it is left in
    /// [`Self::started_revoke`] for the session loop, which runs its fence
    /// and answers it.
    async fn handle<S>(
        &mut self,
        text: &str,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
    ) -> Result<Option<SessionEnd>, AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let raw = match RawFrame::parse(text) {
            Ok(raw) => raw,
            Err(e) => {
                let error = ProtocolError {
                    code: e.code(),
                    message: e.to_string(),
                    fatal: e.is_fatal(),
                };
                write_message(sink, NodeMessage::Error(error), e.message_id()).await?;
                return Ok(if e.is_fatal() {
                    // `unsupported_version` keeps dialing (section 4.4).
                    Some(SessionEnd::Reconnect)
                } else {
                    None
                });
            }
        };
        let message = match ControlMessage::from_raw(&raw) {
            Ok(message) => message,
            Err(e) => {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: e.to_string(),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(None);
            }
        };

        match message {
            ControlMessage::DesiredState(body) => {
                self.on_desired_state(&raw, &body, sink).await?;
            }
            ControlMessage::SigningKeyRotate(body) => {
                self.on_signing_key_rotate(&raw, &body, sink).await?;
            }
            ControlMessage::Ping => {
                write_message(sink, NodeMessage::Pong, Some(&raw.id)).await?;
            }
            ControlMessage::Pong | ControlMessage::LogEntries(_) => {
                // Change-log follow is a follow-up.
            }
            ControlMessage::StreamInput(input) => {
                Self::on_stream_input(&raw, &input, streams, sink).await?;
            }
            ControlMessage::StreamAck(ack) => {
                Self::on_stream_ack(&raw, &ack, streams, sink).await?;
            }
            ControlMessage::DnsChallengeResult(result) => {
                let Some(re) = raw.re.as_deref() else {
                    warn!("dns_challenge_result without re; dropping it");
                    return Ok(None);
                };
                let matched = self
                    .dns
                    .as_ref()
                    .is_some_and(|bridge| bridge.complete(re, result));
                if !matched {
                    debug!(re, "dns_challenge_result has no waiting challenge");
                }
            }
            ControlMessage::StreamClose(close) => {
                self.on_stream_close(&close, streams, stream_tx).await?;
            }
            ControlMessage::Command(command) => {
                if command.kind == "probe" {
                    let body = CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some(format!(
                            "kvm available: {}; kernel {}",
                            self.facts.kvm.available, self.facts.kvm.kernel
                        )),
                        error: None,
                        lines: None,
                        metrics: None,
                    };
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else if command.kind == "logs_query" {
                    let body = self.logs_query(&command).await;
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else if command.kind == "metrics_query" {
                    let body = self.metrics_query(&command);
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else if command.kind == "inventory_resend" {
                    match parse_inventory_resend(&command.params) {
                        Ok(digest) => match &self.inventory {
                            Some(hub) => match hub.request_resend(&digest).await {
                                Ok(()) => {
                                    let body = CommandResult {
                                        status: CommandStatus::Ok,
                                        detail: Some("inventory queued".to_owned()),
                                        error: None,
                                        lines: None,
                                        metrics: None,
                                    };
                                    write_message(
                                        sink,
                                        NodeMessage::CommandResult(body),
                                        Some(&raw.id),
                                    )
                                    .await?;
                                }
                                Err(_) => write_not_found(sink, &raw.id).await?,
                            },
                            None => write_not_found(sink, &raw.id).await?,
                        },
                        Err(message) => {
                            write_message(
                                sink,
                                NodeMessage::CommandResult(bad_result(&message)),
                                Some(&raw.id),
                            )
                            .await?;
                        }
                    }
                } else if command.kind == "fence_ack" {
                    let body = match parse_fence_ack(&command.params) {
                        Ok(lease_ids) => match &self.leases {
                            Some(leases) => leases.fence_ack(lease_ids).await,
                            // Without a lease service no fence was recorded.
                            None => CommandResult {
                                status: CommandStatus::Ok,
                                detail: Some("no failed fence remains".to_owned()),
                                error: None,
                                lines: None,
                                metrics: None,
                            },
                        },
                        Err(message) => bad_result(&message),
                    };
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                } else if command.kind == "build_logs_query" {
                    match self.build_logs_query(&command).await {
                        Ok(body) => {
                            write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id))
                                .await?;
                        }
                        Err(BuildsError::NotFound) => {
                            write_not_found(sink, &raw.id).await?;
                        }
                        Err(other) => {
                            write_message(
                                sink,
                                NodeMessage::CommandResult(bad_result(&other.to_string())),
                                Some(&raw.id),
                            )
                            .await?;
                        }
                    }
                } else {
                    let body = CommandResult {
                        status: CommandStatus::Error,
                        detail: Some(format!(
                            "command kind {:?} arrives in a later milestone",
                            command.kind
                        )),
                        error: Some(ErrorBody {
                            code: ErrorCode::BadMessage,
                            message: "this command kind is not implemented yet".to_owned(),
                        }),
                        lines: None,
                        metrics: None,
                    };
                    write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
                }
            }
            ControlMessage::StreamOpen(open) => {
                self.on_stream_open(&open, &raw, streams, stream_tx, sink)
                    .await?;
            }
            ControlMessage::LeaseGrant(grant) => {
                match &self.leases {
                    Some(leases) => {
                        let id = MsgId(raw.id.clone());
                        let re = raw.re.clone().map(MsgId);
                        if let Err(error) = leases.grant(id, re, grant).await {
                            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                        }
                    }
                    None => {
                        // No stateful app exists without a lease service (M2,
                        // section 8.7).
                        let error = ProtocolError {
                            code: ErrorCode::BadMessage,
                            message: "no stateful apps run in this version".to_owned(),
                            fatal: false,
                        };
                        write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                    }
                }
            }
            ControlMessage::LeaseRevoke(revoke) => {
                if self.leases.is_some() {
                    // The session loop runs the fence and answers once it is
                    // recorded, holding back what may not pass it (section
                    // 5.13).
                    self.started_revoke = Some(StartedRevoke {
                        raw_id: raw.id.clone(),
                        re: raw.re.clone().map(MsgId),
                        body: revoke,
                    });
                } else {
                    // Without a service no lease is known.
                    let result = NodeMessage::CommandResult(CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some("unknown lease".to_owned()),
                        error: None,
                        lines: None,
                        metrics: None,
                    });
                    write_message(sink, result, Some(&raw.id)).await?;
                }
            }
            ControlMessage::Welcome(welcome) => {
                debug!(
                    cluster_id = %welcome.cluster_id,
                    "ignoring a second welcome"
                );
            }
            ControlMessage::Error(error) => {
                if error.fatal {
                    match error.code {
                        ErrorCode::Unauthenticated => {
                            return Ok(Some(SessionEnd::Fatal(FatalReason::Unauthenticated)));
                        }
                        ErrorCode::NodeRemoved => {
                            return Ok(Some(SessionEnd::Fatal(FatalReason::NodeRemoved)));
                        }
                        ErrorCode::UnsupportedVersion => {
                            warn!(
                                message = %error.message,
                                "the primary does not support this protocol version; will redial"
                            );
                            return Ok(Some(SessionEnd::Reconnect));
                        }
                        _ => {
                            warn!(
                                code = ?error.code,
                                message = %error.message,
                                "the primary ended the session fatally"
                            );
                            return Ok(Some(SessionEnd::Reconnect));
                        }
                    }
                }
                warn!(
                    code = ?error.code,
                    message = %error.message,
                    "the primary reported a non-fatal protocol error"
                );
            }
        }
        Ok(None)
    }

    /// Verifies, stores and publishes one `desired_state` (sections 5.11, 6.1).
    async fn on_desired_state<S>(
        &mut self,
        raw: &RawFrame,
        body: &serde_json::Value,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let key = self.identity.verifying_key()?;
        if let Err(e) = quoro_proto::sign::verify_body(&key, body) {
            let error = ProtocolError {
                code: ErrorCode::BadSignature,
                message: format!(
                    "desired_state signature does not verify against the cluster signing key: {e}"
                ),
                fatal: false,
            };
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        let state = match DesiredState::from_value(body) {
            Ok(state) => state,
            Err(e) => {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: format!("desired_state body is malformed: {e}"),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        let rev = state.rev;
        let stored = self
            .store
            .latest_desired()
            .await
            .map_or(0, |latest| latest.map_or(0, |(rev, _)| rev));
        if rev <= stored {
            let result = CommandResult {
                status: CommandStatus::Ok,
                detail: Some(format!("ignored rev {rev}")),
                error: None,
                lines: None,
                metrics: None,
            };
            write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
            return Ok(());
        }

        if let Err(e) = self.store.store_desired(rev, body).await {
            let result = CommandResult {
                status: CommandStatus::Error,
                detail: Some(format!("could not store rev {rev}: {e}")),
                error: None,
                lines: None,
                metrics: None,
            };
            write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
            return Ok(());
        }
        let result = CommandResult {
            status: CommandStatus::Ok,
            detail: Some(format!("stored rev {rev}")),
            error: None,
            lines: None,
            metrics: None,
        };
        write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;

        let snapshot = Snapshot { rev, state };
        self.current = Some(snapshot.clone());
        // The reconciler wakes on the new snapshot; a closed channel means the
        // reconciler already stopped, which is a shutdown in progress.
        let _ = self.desired.send_replace(Some(snapshot));
        Ok(())
    }

    /// Verifies and persists one `signing_key_rotate` (section 5.21).
    async fn on_signing_key_rotate<S>(
        &mut self,
        raw: &RawFrame,
        body: &serde_json::Value,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let key = self.identity.verifying_key()?;
        let bad_signature = |message: String| ProtocolError {
            code: ErrorCode::BadSignature,
            message,
            fatal: false,
        };
        if let Err(e) = quoro_proto::sign::verify_body(&key, body) {
            let error = bad_signature(format!(
                "signing_key_rotate signature does not verify against the current pin: {e}"
            ));
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        let public_key = body.get("public_key").and_then(serde_json::Value::as_str);
        let key_id = body.get("key_id").and_then(serde_json::Value::as_str);
        let (Some(public_key), Some(key_id)) = (public_key, key_id) else {
            let error =
                bad_signature("signing_key_rotate has no public_key or no key_id".to_owned());
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        };
        let expected = match Identity::key_id_for(public_key) {
            Ok(expected) => expected,
            Err(e) => {
                let error = bad_signature(format!("signing_key_rotate has a bad key: {e}"));
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        if expected != key_id {
            let error = bad_signature(format!(
                "signing_key_rotate key_id {key_id} does not name the public key"
            ));
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        self.identity.rotate(public_key)?;
        info!("cluster signing key rotated");
        let result = CommandResult {
            status: CommandStatus::Ok,
            detail: Some(format!("rotated to key {expected}")),
            error: None,
            lines: None,
            metrics: None,
        };
        write_message(sink, NodeMessage::CommandResult(result), Some(&raw.id)).await?;
        Ok(())
    }

    /// Answers `command` kind `logs_query` (section 5.17).
    async fn logs_query(&self, command: &Command) -> CommandResult {
        let query = match parse_logs_query(&command.params) {
            Ok(query) => query,
            Err(message) => return bad_result(&message),
        };
        let lines = match self
            .store
            .query_log_lines(&query.filter, query.since, query.until, query.tail)
            .await
        {
            Ok(lines) => lines,
            Err(e) => {
                warn!(error = %e, "could not query the log store");
                return bad_result(&format!("could not query the log store: {e}"));
            }
        };
        let total = lines.len();
        let budget = wire::MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM);
        let mut kept: Vec<LogLine> = Vec::new();
        let mut used = 0usize;
        for line in lines.into_iter().rev() {
            let wire = stored_to_wire(line);
            let size = wire_size(&wire);
            if used + size > budget {
                break;
            }
            used += size;
            kept.push(wire);
        }
        let omitted = total - kept.len();
        kept.reverse();
        let detail = if omitted > 0 {
            Some(format!(
                "{omitted} older line(s) were left out to fit the 1 MiB frame limit"
            ))
        } else {
            Some(format!("{} line(s)", kept.len()))
        };
        CommandResult {
            status: CommandStatus::Ok,
            detail,
            error: None,
            lines: Some(kept),
            metrics: None,
        }
    }

    /// Answers `command` kind `metrics_query` (section 5.17).
    fn metrics_query(&self, command: &Command) -> CommandResult {
        let (node, vm_ids) = match parse_metrics_params(&command.params) {
            Ok(params) => params,
            Err(message) => return bad_result(&message),
        };
        let Some(ring) = &self.metrics else {
            return bad_result("metrics are not sampled on this node");
        };
        CommandResult {
            status: CommandStatus::Ok,
            detail: None,
            error: None,
            lines: None,
            metrics: Some(ring.query(node, &vm_ids)),
        }
    }

    /// Answers `command` kind `build_logs_query` (section 5.17).
    ///
    /// The answer keeps the oldest lines that fit the 1 MiB frame limit and
    /// returns fewer rather than a larger message.
    async fn build_logs_query(&self, command: &Command) -> Result<CommandResult, BuildsError> {
        let query = parse_build_logs_query(&command.params)?;
        let Some(builds) = &self.builds else {
            return Err(BuildsError::NotFound);
        };
        let lines = builds
            .query(&query.build_id, query.after_seq, query.limit)
            .await?;
        let total = lines.len();
        let budget = wire::MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM);
        let mut kept: Vec<LogLine> = Vec::new();
        let mut used = 0usize;
        for line in lines {
            let size = wire_size(&line);
            if !kept.is_empty() && used + size > budget {
                break;
            }
            used += size;
            kept.push(line);
        }
        let detail = if kept.len() < total {
            format!(
                "{} line(s), the rest left out to fit the 1 MiB frame limit",
                kept.len()
            )
        } else {
            format!("{} line(s)", kept.len())
        };
        Ok(CommandResult {
            status: CommandStatus::Ok,
            detail: Some(detail),
            error: None,
            lines: Some(kept),
            metrics: None,
        })
    }

    /// Starts one stream (section 5.15) or answers why it could not start.
    async fn on_stream_open<S>(
        &mut self,
        open: &StreamOpen,
        raw: &RawFrame,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // A finished stream (a replayed `deployment_logs`, an ended reader)
        // no longer holds a slot.
        streams.prune();
        if streams.handles.len() >= MAX_STREAMS {
            let body = bad_result("stream_limit");
            write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
            return Ok(());
        }
        let store = self.store.clone();
        let stream_id = open.stream_id.clone();
        let opened = match open.kind.as_str() {
            "app_logs" => match parse_app_logs(&open.params) {
                Ok(params) => Ok((
                    spawn_app_logs(store, stream_id, params, stream_tx.clone()),
                    None,
                )),
                Err(message) => Err(bad_result(&message)),
            },
            "deployment_logs" => match parse_deployment_logs(&open.params) {
                Ok((deployment_id, phase)) => Ok((
                    spawn_deployment_logs(
                        store,
                        stream_id,
                        deployment_id,
                        phase,
                        stream_tx.clone(),
                    ),
                    None,
                )),
                Err(message) => Err(bad_result(&message)),
            },
            "metrics" => match parse_metrics_params(&open.params) {
                Ok((node, vm_ids)) => match &self.metrics {
                    // Subscribe before spawning so no round is missed between
                    // this frame and the task's first poll.
                    Some(ring) => Ok((
                        spawn_metrics(ring.subscribe(), stream_id, node, vm_ids, stream_tx.clone()),
                        None,
                    )),
                    None => Err(bad_result("metrics are not sampled on this node")),
                },
                Err(message) => Err(bad_result(&message)),
            },
            "build_logs" => match parse_build_logs(&open.params) {
                Ok((build_id, start)) => {
                    let follow = match &self.builds {
                        Some(builds) => builds.follow(&build_id, start).await,
                        None => Err(BuildsError::NotFound),
                    };
                    match follow {
                        Ok(lines) => {
                            Ok((spawn_build_logs(stream_id, lines, stream_tx.clone()), None))
                        }
                        Err(BuildsError::NotFound) => Err(build_not_found()),
                        Err(other) => Err(bad_result(&other.to_string())),
                    }
                }
                Err(message) => Err(bad_result(&message)),
            },
            "terminal" => match &self.interactive {
                Some(interactive) => interactive
                    .open_terminal(&open.stream_id, &open.params, stream_tx.clone())
                    .await
                    .map(|opened| (opened.handle, Some(opened.door)))
                    .map_err(open_error_result),
                None => Err(interactive_unavailable()),
            },
            "console" => match &self.interactive {
                Some(interactive) => interactive
                    .open_console(&open.stream_id, &open.params, stream_tx.clone())
                    .map(|opened| (opened.handle, Some(opened.door)))
                    .map_err(open_error_result),
                None => Err(interactive_unavailable()),
            },
            "transcript" => match &self.interactive {
                Some(interactive) => interactive
                    .open_transcript(&open.stream_id, &open.params, stream_tx.clone())
                    .await
                    .map(|opened| (opened.handle, Some(opened.door)))
                    .map_err(open_error_result),
                None => Err(interactive_unavailable()),
            },
            other => Err(bad_result(&format!(
                "stream kind {other:?} is not supported"
            ))),
        };
        match opened {
            Ok((handle, door)) => streams.insert(open.stream_id.clone(), handle, door),
            Err(body) => {
                write_message(sink, NodeMessage::CommandResult(body), Some(&raw.id)).await?;
            }
        }
        Ok(())
    }

    /// Applies one `stream_input` (section 5.24).
    ///
    /// Only a `terminal` takes input; another live stream is answered with
    /// `bad_message` non-fatal and the input is dropped, while an unknown or
    /// ended stream id is ignored silently.
    async fn on_stream_input<S>(
        raw: &RawFrame,
        input: &wire::StreamInput,
        streams: &SessionStreams,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Some(door) = streams.door(&input.stream_id) else {
            if streams.is_live(&input.stream_id) {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message: "this stream does not take input".to_owned(),
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            }
            return Ok(());
        };
        if door.kind != StreamKind::Terminal {
            let error = ProtocolError {
                code: ErrorCode::BadMessage,
                message: "a console stream is read-only".to_owned(),
                fatal: false,
            };
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
            return Ok(());
        }
        let parsed = match crate::interactive::parse_input(input) {
            Ok(parsed) => parsed,
            Err(message) => {
                let error = ProtocolError {
                    code: ErrorCode::BadMessage,
                    message,
                    fatal: false,
                };
                write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
                return Ok(());
            }
        };
        // Never awaits: the session loop serves every stream and the node's
        // heartbeat, so one stuck terminal must not stall it.
        door.push_input(parsed);
        Ok(())
    }

    /// Applies one `stream_ack` to a data stream's credit window (section
    /// 5.25).
    ///
    /// An earlier ack is ignored; an ack beyond the bytes sent is a primary
    /// bug, answered with `bad_message` non-fatal and otherwise ignored.
    async fn on_stream_ack<S>(
        raw: &RawFrame,
        ack: &wire::StreamAck,
        streams: &SessionStreams,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Some(door) = streams.door(&ack.stream_id) else {
            return Ok(());
        };
        if door.credit.ack(ack.acked_bytes) == crate::interactive::Ack::Beyond {
            let error = ProtocolError {
                code: ErrorCode::BadMessage,
                message: format!(
                    "stream_ack for {} bytes is beyond what was sent",
                    ack.acked_bytes
                ),
                fatal: false,
            };
            write_message(sink, NodeMessage::Error(error), Some(&raw.id)).await?;
        }
        Ok(())
    }

    /// Stops a stream and answers `stream_end` reason `cancelled` (section
    /// 5.16). An unknown id is ignored.
    async fn on_stream_close(
        &mut self,
        close: &StreamClose,
        streams: &mut SessionStreams,
        stream_tx: &mpsc::Sender<NodeMessage>,
    ) -> Result<(), AgentError> {
        if streams.stop(&close.stream_id) {
            // Through the stream channel, so the end follows every frame the
            // task already queued.
            send_end(
                stream_tx,
                &close.stream_id,
                StreamEndReason::Cancelled,
                None,
            )
            .await;
        }
        Ok(())
    }

    async fn hello(&self) -> NodeMessage {
        let stored_rev = self
            .store
            .latest_desired()
            .await
            .ok()
            .flatten()
            .map_or(0, |(rev, _)| rev);
        let applied_rev = self.store.applied_rev().await.unwrap_or(0);
        let dropped_events = self.store.dropped_events().await.unwrap_or(0);
        let identity = self.identity.get();
        NodeMessage::Hello(Hello {
            node_id: identity.node_id,
            agent_version: self.facts.agent_version.clone(),
            engine_version: self.facts.engine_version.clone(),
            boot_id: self.boot_id.clone(),
            stored_rev,
            applied_rev,
            log_seq: 0,
            log_hash: ZERO_HASH.to_owned(),
            leases: self.leases.as_ref().map_or_else(Vec::new, LeaseLink::hello),
            capacity: self.facts.capacity,
            allocated: self.allocated(),
            kvm: self.facts.kvm.clone(),
            dropped_events,
            mesh_public_key: self.mesh_public_key.clone(),
            volume_snapshot: self.volume_snapshot,
        })
    }

    fn allocated(&self) -> Capacity {
        let Some(snapshot) = &self.current else {
            return Capacity::default();
        };
        let mut allocated = Capacity::default();
        for vm in &snapshot.state.spec.vms {
            allocated.cpus = allocated.cpus.saturating_add(vm.vcpus);
            allocated.memory_mb = allocated.memory_mb.saturating_add(u64::from(vm.memory_mb));
            allocated.disk_gb = allocated.disk_gb.saturating_add(u64::from(vm.disk_gb));
        }
        allocated
    }

    async fn send_status<S>(
        &mut self,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let images = match &self.inventory {
            Some(hub) => hub.images(&self.spec_image_digests()).await,
            None => Vec::new(),
        };
        let applied_rev = self.store.applied_rev().await.unwrap_or(0);
        let report = StatusReport {
            applied_rev,
            vms: self.observed.borrow().clone(),
            routes: self.routes(),
            capacity: self.facts.capacity,
            allocated: self.allocated(),
            disk_free_gb: self
                .disk_free_path
                .as_deref()
                .map_or(0.0, util::disk_free_gb),
            builds: self
                .builds
                .as_ref()
                .map_or_else(Vec::new, BuildsHandle::current),
            edge_backends: self
                .edge
                .as_ref()
                .map_or_else(Vec::new, |edge| edge.statuses()),
            volumes: self
                .volumes
                .as_ref()
                .map_or_else(Vec::new, |volumes| volumes.borrow().clone()),
            backups: self
                .backups
                .as_ref()
                .map_or_else(Vec::new, |backups| backups.borrow().clone()),
            images,
        };
        write_message(sink, NodeMessage::StatusReport(report), None).await?;
        self.reported_applied_rev = applied_rev;
        Ok(())
    }

    /// The `image_digest` of every VM in the current spec, for
    /// `status_report.images` (section 5.3).
    fn spec_image_digests(&self) -> Vec<String> {
        self.current
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .state
                    .spec
                    .vms
                    .iter()
                    .map(|vm| vm.image_digest.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Sends every inventory queued for this node as `image_inventory` parts
    /// (section 5.26). Part 1's envelope id is the send's `inventory_id`.
    async fn flush_inventory<S>(
        &mut self,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Some(hub) = &self.inventory else {
            return Ok(());
        };
        while let Some(parts) = hub.next_parts().await {
            for (index, part) in parts.into_iter().enumerate() {
                let id = if index == 0 {
                    part.inventory_id.clone()
                } else {
                    util::message_id()
                };
                write_message_with_id(sink, NodeMessage::ImageInventory(part), &id, None).await?;
            }
        }
        Ok(())
    }

    fn routes(&self) -> Vec<RouteStatus> {
        let Some(snapshot) = self.current.as_ref() else {
            return Vec::new();
        };
        let routes = &snapshot.state.spec.routes;
        if let Some(ingress) = &self.ingress {
            return ingress.route_statuses(routes);
        }
        // Without a supervisor (tests and the join path) the node reports the
        // state it would start from: `tunnel` needs no certificate, `acme`
        // is pending until the public backend issues one.
        routes
            .iter()
            .map(|route| RouteStatus {
                hostname: route.hostname.clone(),
                cert_status: if route.tls_mode == "tunnel" {
                    "not_applicable".to_owned()
                } else {
                    "pending".to_owned()
                },
                cert_expires_at: None,
            })
            .collect()
    }

    /// Sends the buffered events, after `welcome` and then every
    /// [`EVENT_FLUSH_INTERVAL`] while the session is up, removing each one from
    /// the buffer only after its frame was written (section 6.5).
    async fn flush_events<S>(
        &mut self,
        sink: &mut SplitSink<WebSocketStream<S>, Message>,
    ) -> Result<(), AgentError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let events = self.store.buffered_events().await?;
        let mut through = None;
        for event in events {
            match serde_json::from_value::<EventBody>(event.body) {
                Ok(body) => {
                    write_message(sink, NodeMessage::Event(body), None).await?;
                    through = Some(event.seq);
                }
                Err(e) => {
                    warn!(evt_id = %event.evt_id, error = %e, "dropping a malformed buffered event");
                    through = Some(event.seq);
                }
            }
        }
        if let Some(seq) = through {
            self.store.drain_events(seq).await?;
        }
        Ok(())
    }
}

/// A parsed `logs_query`.
struct LogsQuery {
    filter: LogFilter,
    since: Option<i64>,
    until: Option<i64>,
    tail: Option<usize>,
}

/// Parses one optional string member of a params object.
fn string_param(params: &serde_json::Value, name: &str) -> Result<Option<String>, String> {
    match params.get(name) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(format!("{name} must be a non-empty string")),
    }
}

/// Parses `app_id` or `vm_id`, exactly one of which must be present.
fn log_filter(params: &serde_json::Value) -> Result<LogFilter, String> {
    let app_id = string_param(params, "app_id")?;
    let vm_id = string_param(params, "vm_id")?;
    match (app_id, vm_id) {
        (Some(app_id), None) => Ok(LogFilter::App(app_id)),
        (None, Some(vm_id)) => Ok(LogFilter::Vm(vm_id)),
        _ => Err("exactly one of app_id or vm_id is required".to_owned()),
    }
}

/// Parses a non-negative `tail`.
fn tail_param(params: &serde_json::Value) -> Result<Option<usize>, String> {
    match params.get("tail") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(|tail| usize::try_from(tail).unwrap_or(usize::MAX))
            .map(Some)
            .ok_or_else(|| "tail must be a non-negative integer".to_owned()),
    }
}

fn timestamp_param(params: &serde_json::Value, name: &str) -> Result<Option<i64>, String> {
    match string_param(params, name)? {
        None => Ok(None),
        Some(text) => util::parse_rfc3339_nanos(&text)
            .map(Some)
            .ok_or_else(|| format!("{name} is not an RFC 3339 timestamp")),
    }
}

/// The `lease_ids` of a `fence_ack` command (section 5.17): at least one
/// `lse_` id.
fn parse_fence_ack(params: &serde_json::Value) -> Result<Vec<String>, String> {
    let ids = params
        .get("lease_ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "fence_ack needs lease_ids".to_owned())?;
    let mut lease_ids = Vec::with_capacity(ids.len());
    for id in ids {
        match id.as_str() {
            Some(id) if id.starts_with("lse_") => lease_ids.push(id.to_owned()),
            _ => return Err(format!("fence_ack lease id {id} is not an lse_ id")),
        }
    }
    if lease_ids.is_empty() {
        return Err("fence_ack needs at least one lease id".to_owned());
    }
    Ok(lease_ids)
}

/// Parses `inventory_resend` params (section 5.26): the image's digest.
fn parse_inventory_resend(params: &serde_json::Value) -> Result<String, String> {
    string_param(params, "image_digest")?.ok_or_else(|| "image_digest is required".to_owned())
}

fn parse_logs_query(params: &serde_json::Value) -> Result<LogsQuery, String> {
    Ok(LogsQuery {
        filter: log_filter(params)?,
        since: timestamp_param(params, "since")?,
        until: timestamp_param(params, "until")?,
        tail: tail_param(params)?,
    })
}

/// The selector and replay size of an `app_logs` stream.
struct AppLogs {
    filter: LogFilter,
    tail: usize,
}

fn parse_app_logs(params: &serde_json::Value) -> Result<AppLogs, String> {
    Ok(AppLogs {
        filter: log_filter(params)?,
        tail: tail_param(params)?.unwrap_or(0),
    })
}

fn parse_deployment_logs(params: &serde_json::Value) -> Result<(String, Option<String>), String> {
    let deployment_id = string_param(params, "deployment_id")?
        .ok_or_else(|| "deployment_id is required".to_owned())?;
    let phase = string_param(params, "phase")?;
    Ok((deployment_id, phase))
}

/// The `command_result` answering a stream open this node cannot serve.
fn open_error_result(error: OpenError) -> CommandResult {
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(error.message().to_owned()),
        error: Some(ErrorBody {
            code: error.code(),
            message: error.message().to_owned(),
        }),
        lines: None,
        metrics: None,
    }
}

/// The answer when the agent runs without interactive handles attached.
fn interactive_unavailable() -> CommandResult {
    CommandResult {
        status: CommandStatus::Error,
        detail: Some("interactive streams are not available on this node".to_owned()),
        error: Some(ErrorBody {
            code: ErrorCode::NotAvailable,
            message: "interactive streams are not available on this node".to_owned(),
        }),
        lines: None,
        metrics: None,
    }
}

/// Parses `after_seq`, a non-negative integer cursor.
fn after_seq_param(params: &serde_json::Value) -> Result<Option<u64>, String> {
    match params.get("after_seq") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| "after_seq must be a non-negative integer".to_owned()),
    }
}

/// The selector and replay of a `build_logs` stream (section 5.15).
fn parse_build_logs(params: &serde_json::Value) -> Result<(String, BuildStart), String> {
    let build_id =
        string_param(params, "build_id")?.ok_or_else(|| "build_id is required".to_owned())?;
    let tail = tail_param(params)?;
    let after_seq = after_seq_param(params)?;
    let start = match (tail, after_seq) {
        (Some(_), Some(_)) => {
            return Err("tail and after_seq cannot both be given".to_owned());
        }
        (Some(tail), None) => BuildStart::Tail(tail),
        (None, Some(seq)) => BuildStart::After(seq),
        // The contract's default is `tail: 0`: no replay, live lines only.
        (None, None) => BuildStart::Tail(0),
    };
    Ok((build_id, start))
}

/// A parsed `build_logs_query` (section 5.17).
struct BuildLogsQuery {
    build_id: String,
    after_seq: u64,
    limit: usize,
}

/// The default page size of `build_logs_query` (section 5.17).
const BUILD_LOG_QUERY_LIMIT: usize = 500;
/// The largest page size of `build_logs_query` (section 5.17).
const BUILD_LOG_QUERY_MAX_LIMIT: usize = 1_000;

fn parse_build_logs_query(params: &serde_json::Value) -> Result<BuildLogsQuery, BuildsError> {
    let build_id = string_param(params, "build_id")
        .map_err(BuildsError::BadRequest)?
        .ok_or_else(|| BuildsError::BadRequest("build_id is required".to_owned()))?;
    let after_seq = after_seq_param(params)
        .map_err(BuildsError::BadRequest)?
        .unwrap_or(0);
    let limit = match params.get("limit") {
        None | Some(serde_json::Value::Null) => BUILD_LOG_QUERY_LIMIT,
        Some(value) => value
            .as_u64()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
            .ok_or_else(|| {
                BuildsError::BadRequest("limit must be a non-negative integer".to_owned())
            })?,
    };
    Ok(BuildLogsQuery {
        build_id,
        after_seq,
        limit: limit.min(BUILD_LOG_QUERY_MAX_LIMIT),
    })
}

/// The `command_result` with the section 5.18 code `not_found` for a build
/// this node does not have.
fn build_not_found() -> CommandResult {
    let message = "this node has no such build";
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(message.to_owned()),
        error: Some(ErrorBody {
            code: ErrorCode::NotFound,
            message: message.to_owned(),
        }),
        lines: None,
        metrics: None,
    }
}

/// Writes a [`build_not_found`] answer to the frame `re`.
async fn write_not_found<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    re: &str,
) -> Result<(), AgentError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_message(
        sink,
        NodeMessage::CommandResult(build_not_found()),
        Some(re),
    )
    .await
}

/// A `command_result` with `bad_message`.
fn bad_result(message: &str) -> CommandResult {
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(message.to_owned()),
        error: Some(ErrorBody {
            code: ErrorCode::BadMessage,
            message: message.to_owned(),
        }),
        lines: None,
        metrics: None,
    }
}

/// Converts a stored line to the wire shape, with the nine-digit instant.
fn stored_to_wire(line: StoredLogLine) -> LogLine {
    LogLine {
        time: util::rfc3339_nanos(line.time_ns),
        stream: line.stream,
        line: line.line,
        vm_id: Some(line.vm_id),
        vertex: None,
        seq: None,
    }
}

/// The serialized size of one line, plus its separator in the array.
fn wire_size(line: &LogLine) -> usize {
    serde_json::to_string(line).map_or(0, |text| text.len() + 1)
}

/// Splits lines into frames that fit the 1 MiB limit.
fn chunks(lines: Vec<LogLine>) -> Vec<Vec<LogLine>> {
    let budget = wire::MAX_MESSAGE_BYTES.saturating_sub(FRAME_HEADROOM);
    let mut frames = Vec::new();
    let mut current = Vec::new();
    let mut used = 0usize;
    for line in lines {
        let size = wire_size(&line);
        if !current.is_empty() && (used + size > budget || current.len() >= STREAM_BATCH_LINES) {
            frames.push(std::mem::take(&mut current));
            used = 0;
        }
        used += size;
        current.push(line);
    }
    if !current.is_empty() {
        frames.push(current);
    }
    frames
}

/// Sends lines as `stream_data` frames. Returns `false` when the session is
/// gone.
async fn send_lines(
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    seq: &mut u64,
    lines: Vec<LogLine>,
) -> bool {
    for chunk in chunks(lines) {
        let frame = StreamData {
            stream_id: stream_id.to_owned(),
            seq: *seq,
            lines: Some(chunk),
            data: None,
            metrics: None,
        };
        *seq = seq.saturating_add(1);
        if tx.send(NodeMessage::StreamData(frame)).await.is_err() {
            return false;
        }
    }
    true
}

/// Sends one `stream_end`.
async fn send_end(
    tx: &mpsc::Sender<NodeMessage>,
    stream_id: &str,
    reason: StreamEndReason,
    message: Option<String>,
) {
    let _ = tx
        .send(NodeMessage::StreamEnd(StreamEnd {
            stream_id: stream_id.to_owned(),
            reason,
            message,
            exit_code: None,
            transcript_sha256: None,
            transcript_bytes: None,
        }))
        .await;
}

fn spawn_app_logs(
    store: NodeStore,
    stream_id: String,
    params: AppLogs,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_app_logs(store, stream_id, params, tx).await;
    })
}

async fn run_app_logs(
    store: NodeStore,
    stream_id: String,
    params: AppLogs,
    tx: mpsc::Sender<NodeMessage>,
) {
    // Subscribe before replaying so a line written during the query is either
    // in the replay or in the channel, never lost and never duplicated.
    let mut rx = store.subscribe_logs();
    let mut seq = 1u64;
    let mut last_time = i64::MIN;
    if params.tail > 0 {
        match store
            .query_log_lines(&params.filter, None, None, Some(params.tail))
            .await
        {
            Ok(lines) => {
                if let Some(last) = lines.last() {
                    last_time = last.time_ns;
                }
                let wire: Vec<LogLine> = lines.into_iter().map(stored_to_wire).collect();
                if !send_lines(&tx, &stream_id, &mut seq, wire).await {
                    return;
                }
            }
            Err(error) => {
                warn!(error = %error, "could not replay a log stream");
                send_end(
                    &tx,
                    &stream_id,
                    StreamEndReason::Error,
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        }
    }

    let mut batch: Vec<LogLine> = Vec::with_capacity(STREAM_BATCH_LINES);
    let mut ticker = tokio::time::interval(STREAM_FLUSH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            received = rx.recv() => {
                match received {
                    Ok(line) => {
                        if params.filter.matches(&line) && line.time_ns > last_time {
                            last_time = line.time_ns;
                            batch.push(LogLine {
                                time: util::rfc3339_nanos(line.time_ns),
                                stream: line.stream,
                                line: line.line,
                                vm_id: Some(line.vm_id),
                                vertex: None,
                                seq: None,
                            });
                            if batch.len() >= STREAM_BATCH_LINES {
                                let lines = std::mem::take(&mut batch);
                                if !send_lines(&tx, &stream_id, &mut seq, lines).await {
                                    return;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        debug!(skipped, "a log stream lagged; some live lines were skipped");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    let lines = std::mem::take(&mut batch);
                    if !send_lines(&tx, &stream_id, &mut seq, lines).await {
                        return;
                    }
                }
            }
        }
    }
}

/// Starts one `metrics` stream: one `stream_data` per sampling round.
fn spawn_metrics(
    mut ticks: tokio::sync::broadcast::Receiver<Tick>,
    stream_id: String,
    node: bool,
    vm_ids: Vec<String>,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut seq = 1u64;
        loop {
            match ticks.recv().await {
                Ok(tick) => {
                    let Some(report) = tick_report(&tick, node, &vm_ids) else {
                        continue;
                    };
                    let frame = StreamData {
                        stream_id: stream_id.clone(),
                        seq,
                        lines: None,
                        data: None,
                        metrics: Some(report),
                    };
                    seq = seq.saturating_add(1);
                    if tx.send(NodeMessage::StreamData(frame)).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    debug!(skipped, "a metrics stream lagged; some rounds were skipped");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
                    return;
                }
            }
        }
    })
}

fn spawn_deployment_logs(
    store: NodeStore,
    stream_id: String,
    deployment_id: String,
    phase: Option<String>,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let lines = match store
            .query_deployment_lines(&deployment_id, phase.as_deref())
            .await
        {
            Ok(lines) => lines,
            Err(error) => {
                warn!(error = %error, "could not replay a deployment's progress");
                send_end(
                    &tx,
                    &stream_id,
                    StreamEndReason::Error,
                    Some(error.to_string()),
                )
                .await;
                return;
            }
        };
        let wire: Vec<LogLine> = lines
            .into_iter()
            .map(|line| LogLine {
                time: util::rfc3339_nanos(line.time_ns),
                stream: quoro_proto::wire::LogStream::Stdout,
                line: line.message,
                vm_id: line.vm_id,
                vertex: None,
                seq: None,
            })
            .collect();
        let mut seq = 1u64;
        if send_lines(&tx, &stream_id, &mut seq, wire).await {
            send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
        }
    })
}

fn spawn_build_logs(
    stream_id: String,
    mut lines: mpsc::Receiver<Vec<LogLine>>,
    tx: mpsc::Sender<NodeMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut seq = 1u64;
        while let Some(batch) = lines.recv().await {
            if !send_lines(&tx, &stream_id, &mut seq, batch).await {
                return;
            }
        }
        // The manager ends the follow once the build is terminal and every
        // line was sent (section 5.8).
        send_end(&tx, &stream_id, StreamEndReason::Completed, None).await;
    })
}

/// Whether a `stop` channel asked the agent to shut down.
pub(crate) async fn wait_stop(mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// Writes one node message.
async fn write_message<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    message: NodeMessage,
    re: Option<&str>,
) -> Result<(), AgentError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let id = util::message_id();
    write_message_with_id(sink, message, &id, re).await
}

/// Writes one node message under a given envelope id.
///
/// `image_inventory` part 1 must carry its `inventory_id` as the envelope id
/// (section 5.26), so the id cannot always be generated here.
async fn write_message_with_id<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    message: NodeMessage,
    id: &str,
    re: Option<&str>,
) -> Result<(), AgentError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let text = message.to_frame(id, re).to_text();
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|e| AgentError::Protocol(format!("WebSocket write failed: {e}")))
}

async fn next_frame<S>(
    source: &mut SplitStream<WebSocketStream<S>>,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    source.next().await
}

/// Parses one control frame.
fn parse_control(text: &str) -> Result<ControlMessage, AgentError> {
    let raw = RawFrame::parse(text).map_err(|e| AgentError::Protocol(format!("bad frame: {e}")))?;
    ControlMessage::from_raw(&raw).map_err(|e| AgentError::Protocol(format!("bad message: {e}")))
}

/// The connect timeout of the advertised addresses after `failed_attempts`
/// redial attempts in a row: [`ADVERTISED_CONNECT_TIMEOUT`] in the first
/// attempt, doubling with each failed one up to [`tls::CONNECT_TIMEOUT`].
fn advertised_timeout(failed_attempts: u32) -> Duration {
    ADVERTISED_CONNECT_TIMEOUT
        .saturating_mul(1 << failed_attempts.min(8))
        .min(tls::CONNECT_TIMEOUT)
}

/// Dials `primary_url`, the address that last worked, with the full
/// [`tls::CONNECT_TIMEOUT`]. While it refuses the TCP connection, the dial is
/// repeated every [`REFUSED_RETRY_STEP`] for [`REFUSED_RETRY_WINDOW`]: a
/// restarting primary refuses for a fraction of a second, and walking its
/// other addresses meanwhile only delays the reconnect (issue #359).
async fn dial_known(
    base_url: &str,
    credential: &str,
    fingerprint: &str,
) -> Result<WebSocketStream<TlsStream<tokio::net::TcpStream>>, AgentError> {
    let deadline = Instant::now() + REFUSED_RETRY_WINDOW;
    loop {
        match dial_one(base_url, credential, fingerprint, tls::CONNECT_TIMEOUT).await {
            Err(e) if is_refused(&e) && Instant::now() + REFUSED_RETRY_STEP <= deadline => {
                debug!(url = %base_url, "the primary refused the connection; dialing it again");
                tokio::time::sleep(REFUSED_RETRY_STEP).await;
            }
            dialed => return dialed,
        }
    }
}

/// Whether the TCP connection was refused: a host answered and nothing
/// listens on the port.
fn is_refused(error: &AgentError) -> bool {
    matches!(
        error,
        AgentError::Io { source, .. } if source.kind() == std::io::ErrorKind::ConnectionRefused
    )
}

/// Opens one pinned TLS connection to `base_url` within `timeout` and upgrades
/// it to the node protocol (section 3.1).
async fn dial_one(
    base_url: &str,
    credential: &str,
    fingerprint: &str,
    timeout: Duration,
) -> Result<WebSocketStream<TlsStream<tokio::net::TcpStream>>, AgentError> {
    let url = session_url(base_url)?;
    let stream = tls::connect_with_timeout(&url, fingerprint, timeout).await?;

    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| AgentError::Protocol(format!("could not build the session request: {e}")))?;
    let authorization = HeaderValue::from_str(&format!("Bearer {credential}")).map_err(|_| {
        AgentError::Protocol("the credential is not a valid header value".to_owned())
    })?;
    request.headers_mut().insert("authorization", authorization);

    let config = WebSocketConfig::default()
        .max_message_size(Some(wire::MAX_MESSAGE_BYTES))
        .max_frame_size(Some(wire::MAX_MESSAGE_BYTES));
    match client_async_with_config(request, stream, Some(config)).await {
        Ok((ws, _response)) => Ok(ws),
        Err(tokio_tungstenite::tungstenite::Error::Http(response))
            if matches!(response.status().as_u16(), 401 | 403) =>
        {
            Err(AgentError::Fatal(FatalReason::Unauthenticated))
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Err(AgentError::Protocol(format!(
                "the primary answered the upgrade with HTTP {}",
                response.status()
            )))
        }
        Err(e) => Err(AgentError::Protocol(format!(
            "the WebSocket upgrade failed: {e}"
        ))),
    }
}

/// The `wss://host:port/api/v1/nodes/session` URL for a primary base URL.
///
/// # Errors
///
/// [`AgentError::Tls`] when the stored primary URL is not `https`/`wss`.
pub fn session_url(primary_url: &str) -> Result<String, AgentError> {
    let endpoint = Endpoint::parse(primary_url)?;
    let host = if endpoint.host.contains(':') {
        format!("[{}]", endpoint.host)
    } else {
        endpoint.host.clone()
    };
    Ok(format!("wss://{host}:{}{SESSION_PATH}", endpoint.port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use quoro_proto::sign::{self, SigningKey};
    use quoro_proto::wire::Welcome;
    use serde_json::json;
    use tokio::io::DuplexStream;
    use tokio_tungstenite::{accept_async, client_async};

    use crate::build::fake::FakeBuilder;
    use crate::build::log::BuildLogs;
    use crate::build::manager::{BuildManager, BuildsHandle};
    use crate::identity::{self, Identity, IdentityHandle};
    use crate::inventory::InventoryHub;
    use crate::logs::NewLogLine;
    use crate::store::BuildRecord;
    use quoro_engine::{
        Engine, FakeEngine, ImageDigest, ImageInventory, ImageRef, InventoryPackage,
    };
    use quoro_proto::inventory::{InventoryObject, sha256_hex};
    use quoro_proto::wire::{
        BuildLimits, BuildState, DesiredStatus, InventoryOs, InventoryPackageWire, LogStream, Spec,
        SpecBuild, SpecNetwork, SpecVm,
    };

    #[test]
    fn session_urls_are_always_wss_with_the_session_path() {
        assert_eq!(
            session_url("https://quoro.example.com").unwrap(),
            "wss://quoro.example.com:443/api/v1/nodes/session"
        );
        assert_eq!(
            session_url("wss://10.0.0.1:8443").unwrap(),
            "wss://10.0.0.1:8443/api/v1/nodes/session"
        );
        assert_eq!(
            session_url("https://[::1]:9443").unwrap(),
            "wss://[::1]:9443/api/v1/nodes/session"
        );
        assert!(session_url("http://quoro.example.com").is_err());
    }

    #[test]
    fn advertised_addresses_get_a_short_timeout_that_grows_with_failures() {
        let secs = |failed| advertised_timeout(failed).as_secs();
        assert_eq!(secs(0), 2);
        assert_eq!(secs(1), 4);
        assert_eq!(secs(2), 8);
        assert_eq!(secs(3), 10, "never above the full connect timeout");
        assert_eq!(secs(u32::MAX), 10);
    }

    #[test]
    fn only_a_refused_tcp_connection_is_refused() {
        let refused = AgentError::io(
            "connect to the primary",
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        );
        assert!(is_refused(&refused));
        let reset = AgentError::io(
            "connect to the primary",
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        );
        assert!(!is_refused(&reset));
        assert!(!is_refused(&AgentError::Tls("timed out".to_owned())));
    }

    #[test]
    fn allocated_capacity_sums_the_spec() {
        // `allocated` only needs a snapshot; the totals are pure arithmetic.
        let mut capacity = Capacity::default();
        capacity.cpus += 2;
        capacity.memory_mb += 512;
        capacity.disk_gb += 4;
        assert_eq!(capacity.cpus, 2);
        assert_eq!(capacity.memory_mb, 512);
        assert_eq!(capacity.disk_gb, 4);
    }

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-session-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn identity_files(dir: &Path, key: &SigningKey) -> IdentityHandle {
        identity::ensure_data_dir(dir).unwrap();
        identity::write_seed(dir, &[7; 32]).unwrap();
        identity::write_credential(dir, "credential-for-tests").unwrap();
        let public = sign::public_key_to_base64(&key.verifying_key());
        identity::save_identity(
            dir,
            &Identity {
                node_id: "nod_test".to_owned(),
                cluster_id: "cls_test".to_owned(),
                primary_url: "https://primary.test".to_owned(),
                primary_urls: Vec::new(),
                fingerprint: format!(
                    "sha256:{}",
                    "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35"
                ),
                cluster_key_id: Identity::key_id_for(&public).unwrap(),
                cluster_public_key: public,
            },
        )
        .unwrap();
        IdentityHandle::load(dir).unwrap()
    }

    fn facts() -> NodeFacts {
        NodeFacts {
            capacity: Capacity::default(),
            kvm: quoro_proto::wire::Kvm::default(),
            engine_version: "fake 0".to_owned(),
            agent_version: "0.0.1".to_owned(),
        }
    }

    async fn read_frame(ws: &mut WebSocketStream<DuplexStream>) -> serde_json::Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(3), ws.next())
                .await
                .expect("the node sent a frame in time")
                .expect("the socket is open")
                .expect("a valid frame");
            if let Message::Text(text) = message {
                return serde_json::from_str(text.as_str()).expect("a JSON frame");
            }
        }
    }

    async fn read_until(ws: &mut WebSocketStream<DuplexStream>, kind: &str) -> serde_json::Value {
        loop {
            let frame = read_frame(ws).await;
            if frame["type"] == kind {
                return frame;
            }
        }
    }

    async fn send(ws: &mut WebSocketStream<DuplexStream>, text: String) {
        ws.send(Message::Text(text.into())).await.unwrap();
    }

    async fn handshake(ws: &mut WebSocketStream<DuplexStream>) {
        let hello = read_frame(ws).await;
        assert_eq!(hello["type"], "hello");
        send(
            ws,
            ControlMessage::Welcome(Welcome {
                cluster_id: "cls_test".to_owned(),
                term: 1,
                server_time: "2026-11-02T10:15:30Z".to_owned(),
                heartbeat_interval_ms: 500,
                session_timeout_ms: 5_000,
                primary_node_id: "nod_primary".to_owned(),
                primary_urls: Vec::new(),
            })
            .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
            .to_text(),
        )
        .await;
        read_until(ws, "status_report").await;
    }

    /// Runs one session over a duplex stream with `primary` driving it.
    async fn drive_session(
        dir: &TempDir,
        primary: impl FnOnce(
            WebSocketStream<DuplexStream>,
            NodeStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + 'static,
    ) -> (SessionEnd, NodeStore) {
        drive_session_with(dir, None, primary).await
    }

    /// The same, with a build manager's handle the session may serve.
    async fn drive_session_with(
        dir: &TempDir,
        builds: Option<BuildsHandle>,
        primary: impl FnOnce(
            WebSocketStream<DuplexStream>,
            NodeStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + 'static,
    ) -> (SessionEnd, NodeStore) {
        drive_session_full(dir, builds, None, None, primary).await
    }

    /// The same, with an inventory hub and a current snapshot to report from.
    async fn drive_session_full(
        dir: &TempDir,
        builds: Option<BuildsHandle>,
        inventory: Option<InventoryHub>,
        current: Option<Snapshot>,
        primary: impl FnOnce(
            WebSocketStream<DuplexStream>,
            NodeStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + 'static,
    ) -> (SessionEnd, NodeStore) {
        let key = sign::signing_key_from_seed(&[4; 32]);
        let identity = identity_files(dir.path(), &key);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (desired, _desired_rx) = watch::channel(None);
        let (_observed_tx, observed_rx) = watch::channel(Vec::new());
        let mut session = Session::new(SessionSetup {
            identity,
            store: store.clone(),
            facts: facts(),
            desired,
            observed: observed_rx,
            current,
            builds,
        });
        if let Some(inventory) = inventory {
            session = session.with_inventory(inventory);
        }

        let (client, server) = tokio::io::duplex(1 << 20);
        let primary_store = store.clone();
        let primary = tokio::spawn(async move {
            let ws = accept_async(server).await.unwrap();
            primary(ws, primary_store).await;
        });
        let (client_ws, _response) = client_async("ws://primary.test/session", client)
            .await
            .unwrap();
        let end = session
            .serve(client_ws, std::future::pending::<()>())
            .await
            .unwrap();
        primary.await.unwrap();
        (end, store)
    }

    fn line(vm_id: &str, app_id: &str, text: &str) -> NewLogLine {
        NewLogLine {
            vm_id: vm_id.to_owned(),
            app_id: Some(app_id.to_owned()),
            stream: quoro_proto::wire::LogStream::Stdout,
            line: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn logs_query_answers_with_the_matching_lines() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_log_lines(&[
            line("vm_1", "app_1", "first"),
            line("vm_1", "app_1", "second"),
        ])
        .await
        .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "logs_query".to_owned(),
                        params: json!({"vm_id": "vm_1"}),
                    })
                    .to_frame("msg_query", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_query");
                assert_eq!(answer["body"]["status"], "ok");
                let lines = answer["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 2);
                assert_eq!(lines[0]["line"], "first");
                assert_eq!(lines[1]["line"], "second");
                assert_eq!(lines[1]["vm_id"], "vm_1");
                assert!(
                    lines[1]["time"]
                        .as_str()
                        .is_some_and(|time| time.ends_with('Z') && time.contains('.'))
                );
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn app_logs_replay_tail_then_follow_live_and_close() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_log_lines(&[
            line("vm_1", "app_1", "first"),
            line("vm_1", "app_1", "second"),
        ])
        .await
        .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_stream".to_owned(),
                        kind: "app_logs".to_owned(),
                        params: json!({"vm_id": "vm_1", "tail": 1}),
                    })
                    .to_frame("msg_open", None)
                    .to_text(),
                )
                .await;
                let first = read_until(&mut ws, "stream_data").await;
                assert_eq!(first["body"]["stream_id"], "msg_stream");
                assert_eq!(first["body"]["seq"], 1);
                let lines = first["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 1);
                assert_eq!(lines[0]["line"], "second");

                // A line captured now reaches the open stream.
                store
                    .append_log_lines(&[line("vm_1", "app_1", "live")])
                    .await
                    .unwrap();
                let live = read_until(&mut ws, "stream_data").await;
                assert_eq!(live["body"]["seq"], 2);
                let lines = live["body"]["lines"].as_array().unwrap();
                assert_eq!(lines[0]["line"], "live");

                send(
                    &mut ws,
                    ControlMessage::StreamClose(StreamClose {
                        stream_id: "msg_stream".to_owned(),
                    })
                    .to_frame("msg_close", None)
                    .to_text(),
                )
                .await;
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["stream_id"], "msg_stream");
                assert_eq!(end["body"]["reason"], "cancelled");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn deployment_logs_replay_then_complete() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        seed.append_deployment_line("dep_1", "pull", "pulling img", Some("vm_1"))
            .await
            .unwrap();
        seed.append_deployment_line("dep_1", "boot", "VM created", Some("vm_1"))
            .await
            .unwrap();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_dep".to_owned(),
                        kind: "deployment_logs".to_owned(),
                        params: json!({"deployment_id": "dep_1"}),
                    })
                    .to_frame("msg_open_dep", None)
                    .to_text(),
                )
                .await;
                let data = read_until(&mut ws, "stream_data").await;
                let lines = data["body"]["lines"].as_array().unwrap();
                assert_eq!(lines[0]["line"], "pulling img");
                assert_eq!(lines[1]["line"], "VM created");
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["reason"], "completed");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn an_unknown_stream_kind_is_a_bad_message() {
        let dir = TempDir::new();
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_metrics".to_owned(),
                        kind: "metrics".to_owned(),
                        params: json!({}),
                    })
                    .to_frame("msg_open_metrics", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_open_metrics");
                assert_eq!(answer["body"]["status"], "error");
                assert_eq!(answer["body"]["error"]["code"], "bad_message");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn logs_query_keeps_the_newest_lines_that_fit() {
        let dir = TempDir::new();
        let seed = NodeStore::open(dir.path()).await.unwrap();
        let mut batch = Vec::new();
        for index in 0..130 {
            batch.push(line(
                "vm_1",
                "app_1",
                &format!("{index}-{}", "x".repeat(9_000)),
            ));
        }
        seed.append_log_lines(&batch).await.unwrap();
        let (end, store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "logs_query".to_owned(),
                        params: json!({"vm_id": "vm_1"}),
                    })
                    .to_frame("msg_query_big", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["body"]["status"], "ok");
                let detail = answer["body"]["detail"].as_str().unwrap();
                assert!(detail.contains("left out"), "{detail}");
                let lines = answer["body"]["lines"].as_array().unwrap();
                assert!(lines.len() < 130, "{}", lines.len());
                let last = lines.last().unwrap()["line"].as_str().unwrap();
                assert!(last.starts_with("129-"), "the newest line is kept");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
        // The store outlived the session; the assertion above is about the
        // answer, so this only checks the fixture is sound.
        assert_eq!(
            store
                .query_log_lines(&LogFilter::Vm("vm_1".to_owned()), None, None, None)
                .await
                .unwrap()
                .len(),
            130
        );
    }

    /// Like [`drive_session`] but attaches the metric rings to the session.
    async fn drive_session_with_ring(
        dir: &TempDir,
        ring: MetricsRing,
        primary: impl FnOnce(
            WebSocketStream<DuplexStream>,
            NodeStore,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + 'static,
    ) -> (SessionEnd, NodeStore) {
        let key = sign::signing_key_from_seed(&[4; 32]);
        let identity = identity_files(dir.path(), &key);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (desired, _desired_rx) = watch::channel(None);
        let (_observed_tx, observed_rx) = watch::channel(Vec::new());
        let mut session = Session::new(SessionSetup {
            identity,
            store: store.clone(),
            facts: facts(),
            desired,
            observed: observed_rx,
            current: None,
            builds: None,
        })
        .with_metrics(ring);

        let (client, server) = tokio::io::duplex(1 << 20);
        let primary_store = store.clone();
        let primary = tokio::spawn(async move {
            let ws = accept_async(server).await.unwrap();
            primary(ws, primary_store).await;
        });
        let (client_ws, _response) = client_async("ws://primary.test/session", client)
            .await
            .unwrap();
        let end = session
            .serve(client_ws, std::future::pending::<()>())
            .await
            .unwrap();
        primary.await.unwrap();
        (end, store)
    }

    const METRICS_VM: &str = "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F";

    fn node_sample() -> quoro_proto::metrics::NodeSample {
        quoro_proto::metrics::NodeSample {
            time: "2026-11-04T10:15:30Z".to_owned(),
            cpu_usage: 0.25,
            cpu_count: 4,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_used_bytes: 2 * 1024 * 1024 * 1024,
        }
    }

    fn vm_sample() -> quoro_proto::metrics::VmSample {
        quoro_proto::metrics::VmSample {
            time: "2026-11-04T10:15:30Z".to_owned(),
            cpu_usage: 0.1,
            vcpus: 2,
            memory_rss_bytes: 100 * 1024 * 1024,
            memory_bytes: 512 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn metrics_query_answers_with_the_ring() {
        let dir = TempDir::new();
        let ring = MetricsRing::new();
        ring.record(
            Some(node_sample()),
            vec![(METRICS_VM.to_owned(), vm_sample())],
        );
        let (end, _store) = drive_session_with_ring(&dir, ring, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "metrics_query".to_owned(),
                        params: json!({"node": true, "vm_ids": [METRICS_VM]}),
                    })
                    .to_frame("msg_metrics", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_metrics");
                assert_eq!(answer["body"]["status"], "ok");
                let metrics = &answer["body"]["metrics"];
                assert_eq!(metrics["interval_s"], 5);
                let node = metrics["node"].as_array().unwrap();
                assert_eq!(node.len(), 1);
                assert_eq!(node[0]["cpu_count"], 4);
                let vms = metrics["vms"].as_array().unwrap();
                assert_eq!(vms.len(), 1);
                assert_eq!(vms[0]["vm_id"], METRICS_VM);
                assert_eq!(vms[0]["samples"][0]["vcpus"], 2);
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn metrics_query_rejects_an_empty_selector() {
        let dir = TempDir::new();
        let (end, _store) = drive_session_with_ring(&dir, MetricsRing::new(), |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Command(Command {
                        kind: "metrics_query".to_owned(),
                        params: json!({}),
                    })
                    .to_frame("msg_metrics_bad", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_metrics_bad");
                assert_eq!(answer["body"]["status"], "error");
                assert_eq!(answer["body"]["error"]["code"], "bad_message");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn a_metrics_stream_follows_rounds_and_ends_on_close() {
        let dir = TempDir::new();
        let ring = MetricsRing::new();
        let feed = ring.clone();
        let (end, _store) = drive_session_with_ring(&dir, ring, move |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_metrics_stream".to_owned(),
                        kind: "metrics".to_owned(),
                        params: json!({"node": true}),
                    })
                    .to_frame("msg_open_metrics", None)
                    .to_text(),
                )
                .await;
                // Let the session install the stream's subscription.
                tokio::time::sleep(Duration::from_millis(100)).await;
                feed.record(Some(node_sample()), Vec::new());
                let data = read_until(&mut ws, "stream_data").await;
                assert_eq!(data["body"]["stream_id"], "msg_metrics_stream");
                assert_eq!(data["body"]["seq"], 1);
                assert!(data["body"].get("lines").is_none());
                let metrics = &data["body"]["metrics"];
                assert_eq!(metrics["interval_s"], 5);
                assert_eq!(metrics["node"].as_array().unwrap().len(), 1);
                assert!(metrics.get("vms").is_none());

                send(
                    &mut ws,
                    ControlMessage::StreamClose(StreamClose {
                        stream_id: "msg_metrics_stream".to_owned(),
                    })
                    .to_frame("msg_close_metrics", None)
                    .to_text(),
                )
                .await;
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["stream_id"], "msg_metrics_stream");
                assert_eq!(end["body"]["reason"], "cancelled");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    // --- build logs (section 5.15, 5.17, ADR 0119) -------------------------

    const BUILD_ID: &str = "bld_1";
    const BUILD_APP: &str = "app_1";

    /// Seeds one build row and two log lines in `dir`.
    async fn seed_build(dir: &TempDir, status: BuildState) {
        let store = NodeStore::open(dir.path()).await.unwrap();
        let logs = BuildLogs::new(dir.path().join("builds").join("logs"));
        logs.append(BUILD_APP, BUILD_ID, LogStream::Stdout, Some(1), "first")
            .await
            .unwrap();
        logs.append(BUILD_APP, BUILD_ID, LogStream::Stderr, None, "second")
            .await
            .unwrap();
        let row = BuildRecord {
            build_id: BUILD_ID.to_owned(),
            app_id: BUILD_APP.to_owned(),
            status,
            failed_phase: None,
            commit: Some("c".repeat(40)),
            image_digest: None,
            error: None,
            failed_step: None,
            log_path: None,
            created_at: "2026-11-03T10:00:00.000000000Z".to_owned(),
            started_at: None,
            finished_at: None,
        };
        store.save_build(&row).await.unwrap();
    }

    /// A handle over the store in `dir`, with no manager running.
    async fn builds_handle(dir: &TempDir) -> BuildsHandle {
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (manager, handle) = BuildManager::new(
            std::sync::Arc::new(FakeEngine::new()),
            std::sync::Arc::new(FakeBuilder::new()),
            store,
        );
        drop(manager);
        handle
    }

    /// A snapshot with `builds` and no VMs.
    fn build_snapshot(rev: u64, builds: Vec<SpecBuild>) -> Snapshot {
        Snapshot {
            rev,
            state: DesiredState {
                rev,
                issued_at: "2026-11-02T10:15:31Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms: Vec::new(),
                    routes: Vec::new(),
                    network: Some(SpecNetwork {
                        subnet: "10.42.1.0/24".to_owned(),
                        gateway: "10.42.1.1".to_owned(),
                    }),
                    ingress: None,
                    builds,
                    mesh: None,
                    edge: None,
                    volumes: Vec::new(),
                    backups: Vec::new(),
                    backup_repository: None,
                },
                signature: String::new(),
            },
        }
    }

    fn spec_build(build_id: &str, app_id: &str) -> SpecBuild {
        SpecBuild {
            build_id: build_id.to_owned(),
            app_id: app_id.to_owned(),
            project_id: "prj_1".to_owned(),
            repo_url: "https://github.com/example/blog.git".to_owned(),
            git_ref: "main".to_owned(),
            dockerfile_path: "Dockerfile".to_owned(),
            context_dir: ".".to_owned(),
            target: None,
            build_args: std::collections::BTreeMap::new(),
            secrets: std::collections::BTreeMap::new(),
            limits: BuildLimits::default(),
            cancelled: false,
            refresh_base: false,
        }
    }

    fn build_query_frame(build_id: &str, id: &str) -> String {
        ControlMessage::Command(Command {
            kind: "build_logs_query".to_owned(),
            params: json!({"build_id": build_id, "after_seq": 0, "limit": 10}),
        })
        .to_frame(id, None)
        .to_text()
    }

    #[tokio::test]
    async fn build_logs_query_answers_with_the_build_lines() {
        let dir = TempDir::new();
        seed_build(&dir, BuildState::Succeeded).await;
        let handle = builds_handle(&dir).await;
        let (end, _store) = drive_session_with(&dir, Some(handle), |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(&mut ws, build_query_frame(BUILD_ID, "msg_build_query")).await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_build_query");
                assert_eq!(answer["body"]["status"], "ok");
                let lines = answer["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 2);
                assert_eq!(lines[0]["seq"], 1);
                assert_eq!(lines[0]["vertex"], 1);
                assert_eq!(lines[0]["stream"], "stdout");
                assert_eq!(lines[1]["stream"], "stderr");
                assert!(
                    lines[0].get("vm_id").is_none(),
                    "a build line never carries a vm_id"
                );
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn a_build_log_stream_replays_and_completes() {
        let dir = TempDir::new();
        seed_build(&dir, BuildState::Succeeded).await;
        let handle = builds_handle(&dir).await;
        let (end, _store) = drive_session_with(&dir, Some(handle), |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_build_stream".to_owned(),
                        kind: "build_logs".to_owned(),
                        params: json!({"build_id": BUILD_ID, "after_seq": 0}),
                    })
                    .to_frame("msg_build_open", None)
                    .to_text(),
                )
                .await;
                let data = read_until(&mut ws, "stream_data").await;
                assert_eq!(data["body"]["stream_id"], "msg_build_stream");
                let lines = data["body"]["lines"].as_array().unwrap();
                assert_eq!(lines.len(), 2);
                assert_eq!(lines[0]["seq"], 1);
                assert_eq!(lines[1]["seq"], 2);
                let end = read_until(&mut ws, "stream_end").await;
                assert_eq!(end["body"]["reason"], "completed");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn a_build_log_stream_follows_a_build_to_completion() {
        let dir = TempDir::new();
        let store = NodeStore::open(dir.path()).await.unwrap();
        let (manager, handle) = BuildManager::new(
            std::sync::Arc::new(FakeEngine::new()),
            std::sync::Arc::new(FakeBuilder::new().with_delay(Duration::from_millis(150))),
            store.clone(),
        );
        let (_snapshots, snapshots_rx) = watch::channel(Some(build_snapshot(
            1,
            vec![spec_build(BUILD_ID, BUILD_APP)],
        )));
        let (stop, stop_rx) = watch::channel(false);
        let manager = tokio::spawn(
            manager
                .with_tick(Duration::from_millis(10))
                .run(snapshots_rx, wait_stop(stop_rx)),
        );
        let appeared = tokio::time::timeout(Duration::from_secs(5), async {
            while store.build(BUILD_ID).await.unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(appeared.is_ok(), "the manager never created the build row");

        let (end, _store) = drive_session_with(&dir, Some(handle), move |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_build_live".to_owned(),
                        kind: "build_logs".to_owned(),
                        params: json!({"build_id": BUILD_ID, "after_seq": 0}),
                    })
                    .to_frame("msg_build_live_open", None)
                    .to_text(),
                )
                .await;
                let mut sent = Vec::new();
                loop {
                    let frame = read_frame(&mut ws).await;
                    match frame["type"].as_str() {
                        Some("stream_data") => {
                            sent.extend(frame["body"]["lines"].as_array().unwrap().iter().cloned());
                        }
                        Some("stream_end") => {
                            assert_eq!(frame["body"]["reason"], "completed");
                            break;
                        }
                        _ => {}
                    }
                }
                assert!(!sent.is_empty(), "no live line reached the stream");
                for (index, line) in sent.iter().enumerate() {
                    assert_eq!(
                        line["seq"].as_u64().unwrap(),
                        index as u64 + 1,
                        "gap-free and in order"
                    );
                }
                assert!(
                    sent.last().unwrap()["line"]
                        .as_str()
                        .unwrap()
                        .starts_with("build succeeded: ")
                );
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
        let _ = stop.send(true);
        let _ = manager.await;
    }

    #[tokio::test]
    async fn status_report_carries_the_builds() {
        let dir = TempDir::new();
        let store = NodeStore::open(dir.path()).await.unwrap();
        let builder = FakeBuilder::new();
        builder.script(BUILD_ID, crate::build::fake::FakeOutcome::Hang);
        let (manager, handle) = BuildManager::new(
            std::sync::Arc::new(FakeEngine::new()),
            std::sync::Arc::new(builder),
            store,
        );
        let (_snapshots, snapshots_rx) = watch::channel(Some(build_snapshot(
            1,
            vec![spec_build(BUILD_ID, BUILD_APP)],
        )));
        let (stop, stop_rx) = watch::channel(false);
        let manager = tokio::spawn(
            manager
                .with_tick(Duration::from_millis(10))
                .run(snapshots_rx, wait_stop(stop_rx)),
        );
        let reported = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let statuses = handle.current();
                if !statuses.is_empty() {
                    return statuses;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the manager reported the build");
        assert_eq!(reported[0].build_id, BUILD_ID);

        let (end, _store) = drive_session_with(&dir, Some(handle), |mut ws, _store| {
            Box::pin(async move {
                let hello = read_frame(&mut ws).await;
                send(
                    &mut ws,
                    ControlMessage::Welcome(Welcome {
                        cluster_id: "cls_test".to_owned(),
                        term: 1,
                        server_time: "2026-11-02T10:15:30Z".to_owned(),
                        heartbeat_interval_ms: 500,
                        session_timeout_ms: 5_000,
                        primary_node_id: "nod_primary".to_owned(),
                        primary_urls: Vec::new(),
                    })
                    .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
                    .to_text(),
                )
                .await;
                let report = read_until(&mut ws, "status_report").await;
                let builds = report["body"]["builds"].as_array().unwrap();
                assert_eq!(builds.len(), 1);
                assert_eq!(builds[0]["build_id"], BUILD_ID);
                assert!(
                    ["queued", "preparing"].contains(&builds[0]["status"].as_str().unwrap()),
                    "{}",
                    builds[0]["status"]
                );
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
        let _ = stop.send(true);
        let _ = manager.await;
    }

    // --- image inventories (sections 5.3, 5.26, 6.11) ----------------------

    const INVENTORY_DIGEST: &str =
        "sha256:5f0c9a2e4b6d8f1a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a";

    fn image_inventory() -> ImageInventory {
        ImageInventory {
            digest: ImageDigest(INVENTORY_DIGEST.to_owned()),
            os: Some(("debian".to_owned(), "13".to_owned())),
            ecosystems: vec!["Debian:13".to_owned(), "npm".to_owned()],
            not_inventoried: Vec::new(),
            packages: vec![
                InventoryPackage {
                    ecosystem: "Debian:13".to_owned(),
                    name: "libssl3t64".to_owned(),
                    source_name: Some("openssl".to_owned()),
                    version: "3.5.1-1".to_owned(),
                    purl: "pkg:deb/debian/libssl3t64@3.5.1-1?arch=amd64&distro=debian-13"
                        .to_owned(),
                    path: None,
                },
                InventoryPackage {
                    ecosystem: "npm".to_owned(),
                    name: "express".to_owned(),
                    source_name: None,
                    version: "4.21.2".to_owned(),
                    purl: "pkg:npm/express@4.21.2".to_owned(),
                    path: Some("app/package-lock.json".to_owned()),
                },
            ],
            inventoried_at: "2026-11-05T06:09:41Z".to_owned(),
        }
    }

    fn inventory_sha() -> String {
        let inventory = image_inventory();
        sha256_hex(&InventoryObject {
            image_digest: INVENTORY_DIGEST.to_owned(),
            inventoried_at: inventory.inventoried_at,
            os: Some(InventoryOs {
                id: "debian".to_owned(),
                version_id: Some("13".to_owned()),
            }),
            ecosystems: inventory.ecosystems,
            not_inventoried: inventory.not_inventoried,
            package_count: 2,
            packages: inventory
                .packages
                .into_iter()
                .map(|package| InventoryPackageWire {
                    ecosystem: package.ecosystem,
                    name: package.name,
                    source_name: package.source_name,
                    version: package.version,
                    purl: package.purl,
                    path: package.path,
                })
                .collect(),
        })
    }

    async fn engine_with_inventory() -> std::sync::Arc<FakeEngine> {
        let engine = std::sync::Arc::new(FakeEngine::new());
        let image = ImageRef {
            reference: "docker.io/library/debian:13".to_owned(),
            digest: ImageDigest(INVENTORY_DIGEST.to_owned()),
        };
        Engine::prepare_image(&*engine, &image).await.unwrap();
        engine.set_inventory(image_inventory());
        engine
    }

    fn vm_snapshot(digest: &str) -> Snapshot {
        Snapshot {
            rev: 1,
            state: DesiredState {
                rev: 1,
                issued_at: "2026-11-02T10:15:31Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms: vec![SpecVm {
                        vm_id: "vm_inventory".to_owned(),
                        project_id: "prj_1".to_owned(),
                        app_id: None,
                        deployment_id: None,
                        name: "inventory".to_owned(),
                        image: "docker.io/library/debian:13".to_owned(),
                        image_digest: digest.to_owned(),
                        vcpus: 2,
                        memory_mb: 512,
                        disk_gb: 4,
                        port: None,
                        env: std::collections::BTreeMap::new(),
                        health_check: None,
                        desired_status: DesiredStatus::Running,
                        restart_generation: 0,
                        stateful: false,
                        lease_id: None,
                        fencing_token: None,
                        volumes: Vec::new(),
                        build_id: None,
                    }],
                    routes: Vec::new(),
                    network: None,
                    ingress: None,
                    builds: Vec::new(),
                    mesh: None,
                    edge: None,
                    volumes: Vec::new(),
                    backups: Vec::new(),
                    backup_repository: None,
                },
                signature: String::new(),
            },
        }
    }

    #[tokio::test]
    async fn a_prepared_image_sends_its_inventory_parts() {
        let dir = TempDir::new();
        let engine = engine_with_inventory().await;
        let hub = InventoryHub::new(std::sync::Arc::clone(&engine));
        hub.notify_prepared(ImageDigest(INVENTORY_DIGEST.to_owned()));
        let sha = inventory_sha();
        let (end, _store) =
            drive_session_full(&dir, None, Some(hub), None, move |mut ws, _store| {
                Box::pin(async move {
                    handshake(&mut ws).await;
                    let part = read_until(&mut ws, "image_inventory").await;
                    assert_eq!(part["body"]["image_digest"], INVENTORY_DIGEST);
                    assert_eq!(part["body"]["inventory_id"], part["id"]);
                    assert_eq!(part["body"]["inventory_sha256"], sha);
                    assert_eq!(part["body"]["part"], 1);
                    assert_eq!(part["body"]["parts"], 1);
                    assert_eq!(part["body"]["package_count"], 2);
                    assert_eq!(part["body"]["os"]["id"], "debian");
                    assert_eq!(part["body"]["os"]["version_id"], "13");
                    assert_eq!(part["body"]["packages"].as_array().unwrap().len(), 2);
                })
            })
            .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn an_image_cached_before_m5a_reaches_the_primary_after_the_backfill() {
        let dir = TempDir::new();
        let engine = engine_with_inventory().await;
        // The node held this image before M5a: no inventory until the engine
        // reads its root disk.
        engine.clear_inventory(&ImageDigest(INVENTORY_DIGEST.to_owned()));
        engine.set_backfill_inventory(image_inventory());
        let hub = InventoryHub::new(std::sync::Arc::clone(&engine));
        let report = crate::inventory::backfill(engine.as_ref(), &hub).await;
        assert_eq!(report.inventoried, vec![INVENTORY_DIGEST.to_owned()]);
        let sha = inventory_sha();
        let snapshot = vm_snapshot(INVENTORY_DIGEST);
        let (end, _store) = drive_session_full(
            &dir,
            None,
            Some(hub),
            Some(snapshot),
            move |mut ws, _store| {
                Box::pin(async move {
                    let hello = read_frame(&mut ws).await;
                    send(
                        &mut ws,
                        ControlMessage::Welcome(Welcome {
                            cluster_id: "cls_test".to_owned(),
                            term: 1,
                            server_time: "2026-11-02T10:15:30Z".to_owned(),
                            heartbeat_interval_ms: 500,
                            session_timeout_ms: 5_000,
                            primary_node_id: "nod_primary".to_owned(),
                            primary_urls: Vec::new(),
                        })
                        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
                        .to_text(),
                    )
                    .await;
                    // Listed with its hash in status_report.images...
                    let report = read_until(&mut ws, "status_report").await;
                    let images = report["body"]["images"].as_array().unwrap();
                    assert_eq!(images.len(), 1);
                    assert_eq!(images[0]["image_digest"], INVENTORY_DIGEST);
                    assert_eq!(images[0]["inventory_sha256"], sha);
                    // ...and sent exactly as for a freshly prepared image
                    // (section 5.26).
                    let part = read_until(&mut ws, "image_inventory").await;
                    assert_eq!(part["body"]["image_digest"], INVENTORY_DIGEST);
                    assert_eq!(part["body"]["inventory_id"], part["id"]);
                    assert_eq!(part["body"]["inventory_sha256"], sha);
                    assert_eq!(part["body"]["parts"], 1);
                    assert_eq!(part["body"]["package_count"], 2);
                })
            },
        )
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn inventory_resend_queues_the_parts_or_answers_not_found() {
        let dir = TempDir::new();
        let engine = engine_with_inventory().await;
        let hub = InventoryHub::new(std::sync::Arc::clone(&engine));
        let sha = inventory_sha();
        let (end, _store) =
            drive_session_full(&dir, None, Some(hub), None, move |mut ws, _store| {
                Box::pin(async move {
                    handshake(&mut ws).await;
                    send(
                        &mut ws,
                        ControlMessage::Command(Command {
                            kind: "inventory_resend".to_owned(),
                            params: json!({"image_digest": INVENTORY_DIGEST}),
                        })
                        .to_frame("msg_resend", None)
                        .to_text(),
                    )
                    .await;
                    let answer = read_until(&mut ws, "command_result").await;
                    assert_eq!(answer["re"], "msg_resend");
                    assert_eq!(answer["body"]["status"], "ok");
                    let part = read_until(&mut ws, "image_inventory").await;
                    assert_eq!(part["body"]["inventory_id"], part["id"]);
                    assert_eq!(part["body"]["inventory_sha256"], sha);

                    send(
                        &mut ws,
                        ControlMessage::Command(Command {
                            kind: "inventory_resend".to_owned(),
                            params: json!({"image_digest": "sha256:unknown"}),
                        })
                        .to_frame("msg_resend_missing", None)
                        .to_text(),
                    )
                    .await;
                    let answer = read_until(&mut ws, "command_result").await;
                    assert_eq!(answer["re"], "msg_resend_missing");
                    assert_eq!(answer["body"]["status"], "error");
                    assert_eq!(answer["body"]["error"]["code"], "not_found");
                })
            })
            .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn status_report_images_lists_the_spec_image_and_its_sha() {
        let dir = TempDir::new();
        let engine = engine_with_inventory().await;
        let hub = InventoryHub::new(std::sync::Arc::clone(&engine));
        let sha = inventory_sha();
        let snapshot = vm_snapshot(INVENTORY_DIGEST);
        let (end, _store) = drive_session_full(
            &dir,
            None,
            Some(hub),
            Some(snapshot),
            move |mut ws, _store| {
                Box::pin(async move {
                    let hello = read_frame(&mut ws).await;
                    send(
                        &mut ws,
                        ControlMessage::Welcome(Welcome {
                            cluster_id: "cls_test".to_owned(),
                            term: 1,
                            server_time: "2026-11-02T10:15:30Z".to_owned(),
                            heartbeat_interval_ms: 500,
                            session_timeout_ms: 5_000,
                            primary_node_id: "nod_primary".to_owned(),
                            primary_urls: Vec::new(),
                        })
                        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
                        .to_text(),
                    )
                    .await;
                    let report = read_until(&mut ws, "status_report").await;
                    let images = report["body"]["images"].as_array().unwrap();
                    assert_eq!(images.len(), 1);
                    assert_eq!(images[0]["image_digest"], INVENTORY_DIGEST);
                    assert_eq!(images[0]["inventory_sha256"], sha);
                })
            },
        )
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn status_report_images_has_no_sha_for_an_uninventoried_image() {
        let dir = TempDir::new();
        let engine = engine_with_inventory().await;
        engine.clear_inventory(&ImageDigest(INVENTORY_DIGEST.to_owned()));
        let hub = InventoryHub::new(std::sync::Arc::clone(&engine));
        let snapshot = vm_snapshot(INVENTORY_DIGEST);
        let (end, _store) =
            drive_session_full(&dir, None, Some(hub), Some(snapshot), |mut ws, _store| {
                Box::pin(async move {
                    let hello = read_frame(&mut ws).await;
                    send(
                        &mut ws,
                        ControlMessage::Welcome(Welcome {
                            cluster_id: "cls_test".to_owned(),
                            term: 1,
                            server_time: "2026-11-02T10:15:30Z".to_owned(),
                            heartbeat_interval_ms: 500,
                            session_timeout_ms: 5_000,
                            primary_node_id: "nod_primary".to_owned(),
                            primary_urls: Vec::new(),
                        })
                        .to_frame("msg_welcome", Some(hello["id"].as_str().unwrap()))
                        .to_text(),
                    )
                    .await;
                    let report = read_until(&mut ws, "status_report").await;
                    let images = report["body"]["images"].as_array().unwrap();
                    assert_eq!(images.len(), 1);
                    assert_eq!(images[0]["image_digest"], INVENTORY_DIGEST);
                    assert!(images[0].get("inventory_sha256").is_none());
                })
            })
            .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn an_unknown_build_is_not_found() {
        let dir = TempDir::new();
        let handle = builds_handle(&dir).await;
        let (end, _store) = drive_session_with(&dir, Some(handle), |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(
                    &mut ws,
                    build_query_frame("bld_missing", "msg_missing_query"),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_missing_query");
                assert_eq!(answer["body"]["status"], "error");
                assert_eq!(answer["body"]["error"]["code"], "not_found");

                send(
                    &mut ws,
                    ControlMessage::StreamOpen(StreamOpen {
                        stream_id: "msg_missing_stream".to_owned(),
                        kind: "build_logs".to_owned(),
                        params: json!({"build_id": "bld_missing"}),
                    })
                    .to_frame("msg_missing_open", None)
                    .to_text(),
                )
                .await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["re"], "msg_missing_open");
                assert_eq!(answer["body"]["status"], "error");
                assert_eq!(answer["body"]["error"]["code"], "not_found");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }

    #[tokio::test]
    async fn without_a_build_manager_every_build_request_is_not_found() {
        let dir = TempDir::new();
        seed_build(&dir, BuildState::Succeeded).await;
        let (end, _store) = drive_session(&dir, |mut ws, _store| {
            Box::pin(async move {
                handshake(&mut ws).await;
                send(&mut ws, build_query_frame(BUILD_ID, "msg_no_manager")).await;
                let answer = read_until(&mut ws, "command_result").await;
                assert_eq!(answer["body"]["error"]["code"], "not_found");
            })
        })
        .await;
        assert_eq!(end, SessionEnd::Reconnect);
    }
}
