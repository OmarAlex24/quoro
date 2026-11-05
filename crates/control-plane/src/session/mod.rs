// SPDX-License-Identifier: AGPL-3.0-only
//! The node-agent session (`node-agent-protocol.md` sections 3 and 4).
//!
//! A node dials `GET /api/v1/nodes/session` with its credential, then sends
//! `hello`. The primary answers `welcome`, signs a full `desired_state` with
//! the cluster signing key ([`crate::keys`]) and pushes it. The hub keeps one
//! session per node: a new session for the same node closes the older one, and
//! only the live session may report state back.
//!
//! M5a revokes stale or superseded leases from `hello` and grants the
//! node's `local` app leases after the snapshot (sections 4.2 and 5.12).
//! Change-log follow is a separate follow-up; its slot is marked below.

pub mod desired;
mod mesh;

use crate::AppState;
use crate::changelog::{self, Actor, ActorType, Op};
use crate::crypto::DataKey;
use crate::deploy;
use crate::driver::{self, ObservedStatus as DriverObservedStatus};
use crate::error::{ApiError, Code};
use crate::events::{self, StoredEvent};
use crate::keys::ClusterKeys;
use crate::leases;
use crate::store::{Store, now_rfc3339, sha256_hex};
use crate::tunnel::{CloudflareEndpoints, TunnelKick};
use crate::{DigestResolver, ServerConfig};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use quoro_proto::wire::{self, ControlMessage, NodeMessage};
use quoro_providers::DnsProvider;
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};

/// How often a failed desired-state build is retried on its own.
const SNAPSHOT_RETRY: Duration = Duration::from_secs(30);

/// Consecutive builds in which an app VM's image may fail to resolve before
/// its deployment is failed (section 5.11; the retry budget of D-0014).
const MAX_DIGEST_FAILURES: u32 = 3;

/// Concurrent log streams one node session may hold (section 5.15).
const MAX_STREAMS: usize = 32;

/// Decoded bytes one `stream_data` may carry (section 5.7).
const MAX_STREAM_DATA_BYTES: usize = 64 * 1024;

/// The outbox sender of one session.
type SessionSender = mpsc::UnboundedSender<String>;
/// The pending commands and streams of one session.
type SessionPending = Arc<Mutex<Pending>>;

/// Why a command or a stream through a node session could not be used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HubError {
    /// The node has no live session.
    NotConnected,
    /// The node did not answer in time.
    Timeout,
    /// The session ended while the command or stream was pending.
    Closed,
    /// The session already holds [`MAX_STREAMS`] open streams.
    StreamLimit,
}

impl std::fmt::Display for HubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConnected => write!(f, "the node has no live session"),
            Self::Timeout => write!(f, "the node did not answer in time"),
            Self::Closed => write!(f, "the node session ended"),
            Self::StreamLimit => write!(f, "the node session holds too many open streams"),
        }
    }
}

impl std::error::Error for HubError {}

/// One event of a [`NodeStream`] (sections 5.7 and 5.8).
#[derive(Clone, Debug)]
pub enum StreamEvent {
    /// A batch of log lines, oldest first.
    Lines(Vec<wire::LogLine>),
    /// Metric samples, one `metrics` report per `stream_data` (section 5.17).
    Metrics(quoro_proto::metrics::MetricsReport),
    /// A batch of raw bytes of a `terminal`, `console` or `transcript` stream
    /// (section 5.7, `data`).
    Data(Vec<u8>),
    /// The stream ended. `reason` is `completed`, `error` or `cancelled`.
    End { reason: String, message: String },
    /// A `terminal`, `console` or `transcript` stream ended, with the details
    /// their consumers need (section 5.8).
    SessionEnd {
        reason: SessionEndReason,
        message: String,
        /// `terminal` only: the program's exit code, or `128 + signal`.
        exit_code: Option<i32>,
        /// `terminal` only: SHA-256 and size of the stored transcript.
        transcript_sha256: Option<String>,
        transcript_bytes: Option<u64>,
        /// Set by a failed `stream_open`: the node's `command_result` code,
        /// for example `not_found` or `not_available`.
        node_code: Option<quoro_proto::ErrorCode>,
    },
}

/// Why a `terminal`, `console` or `transcript` stream ended (section 5.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEndReason {
    /// The program exited (`terminal`) or the file was sent (`transcript`).
    Completed,
    /// The node reported an error, or the open failed.
    Error,
    /// The stream was closed by `stream_close`.
    Cancelled,
    /// The VM stopped (`terminal`) or was deleted (`console`).
    VmStopped,
    /// A terminal transcript reached its cap.
    Limit,
    /// The node session dropped; sessions do not resume.
    Lost,
}

impl SessionEndReason {
    /// The wire `reason` of section 5.8.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::VmStopped => "vm_stopped",
            Self::Limit => "limit",
            Self::Lost => "node_disconnected",
        }
    }
}

/// Whether `kind` is one of the byte streams of M4 (`terminal`, `console`,
/// `transcript`) rather than a log stream.
fn is_byte_stream(kind: &str) -> bool {
    matches!(kind, "terminal" | "console" | "transcript")
}

/// One open stream of a session: where its events go and the last `seq` seen.
struct StreamSlot {
    tx: mpsc::UnboundedSender<StreamEvent>,
    last_seq: u64,
    /// The `stream_open` kind, so its events are shaped for its consumer.
    kind: String,
}

/// The commands and streams waiting on one live session (sections 5.7-5.9).
#[derive(Default)]
struct Pending {
    /// `command` msg id to the answer's channel.
    commands: HashMap<String, oneshot::Sender<Result<quoro_proto::CommandResult, HubError>>>,
    /// `stream_id` to its event channel and sequence cursor.
    streams: HashMap<String, StreamSlot>,
    /// `stream_open` msg id to the stream it opened, so a failed open ends it.
    opens: HashMap<String, String>,
}

impl Pending {
    /// Whether a new stream would exceed [`MAX_STREAMS`] (section 5.15).
    fn at_stream_limit(&self) -> bool {
        self.streams.len() >= MAX_STREAMS
    }

    /// Fails every waiter because the session ended.
    fn fail_all(&mut self) {
        for (_, tx) in self.commands.drain() {
            let _ = tx.send(Err(HubError::Closed));
        }
        for (_, slot) in self.streams.drain() {
            let message = "the node session ended".to_string();
            if is_byte_stream(&slot.kind) {
                let _ = slot.tx.send(StreamEvent::SessionEnd {
                    reason: SessionEndReason::Lost,
                    message,
                    exit_code: None,
                    transcript_sha256: None,
                    transcript_bytes: None,
                    node_code: None,
                });
            } else {
                let _ = slot.tx.send(StreamEvent::End {
                    reason: "error".to_string(),
                    message,
                });
            }
        }
        self.opens.clear();
    }
}

/// A fresh `msg_` id (`wire::msg_id` takes 16 random bytes).
fn new_msg_id() -> String {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).expect("OS random generator");
    wire::msg_id(random)
}

/// The frame that answers a failed decode (section 5.18).
fn protocol_error(
    code: quoro_proto::ErrorCode,
    message: &str,
    fatal: bool,
    re: Option<&str>,
) -> String {
    ControlMessage::Error(quoro_proto::ProtocolError {
        code,
        message: message.to_string(),
        fatal,
    })
    .to_frame(&new_msg_id(), re)
    .to_text()
}

fn decode_failure(error: &wire::DecodeError, outbox: &mut Vec<String>) -> bool {
    outbox.push(protocol_error(
        error.code(),
        &error.to_string(),
        error.is_fatal(),
        error.message_id(),
    ));
    error.is_fatal()
}

// ---------------------------------------------------------------------------
// The hub
// ---------------------------------------------------------------------------

struct SessionHandle {
    generation: u64,
    tx: mpsc::UnboundedSender<String>,
    /// Set when a newer session for the same node replaces this one.
    closed: watch::Sender<bool>,
    /// Wakes the task when the node's desired state may have changed.
    changed: Arc<Notify>,
    /// Commands and streams waiting on this session.
    pending: Arc<Mutex<Pending>>,
}

/// A source of `welcome.primary_urls` (section 5.10). The binary installs one
/// that enumerates the host's addresses; tests install a fixed list.
pub type AdvertisedUrls = dyn Fn() -> Vec<String> + Send + Sync;

struct HubInner {
    store: Store,
    config: Arc<ServerConfig>,
    keys: Arc<ClusterKeys>,
    cluster_id: String,
    resolver: Arc<dyn DigestResolver>,
    data_key: Arc<DataKey>,
    /// The bucket factory, sweep flag and last sweep of the backups code.
    backups: crate::backups::Backups,
    sessions: Mutex<HashMap<String, SessionHandle>>,
    digests: desired::DigestCache,
    /// `vm_` id to node, kept for VMs whose row is already deleted.
    vm_nodes: Mutex<HashMap<String, String>>,
    /// The tunnel manager, so a session can ask for a route reconcile.
    tunnels: Mutex<Option<TunnelKick>>,
    /// Where the Cloudflare clients point (section 5.22, ADR 0094).
    endpoints: Mutex<CloudflareEndpoints>,
    /// The base URLs other than `public_url` where this primary's listener
    /// can be reached, handed out in `welcome.primary_urls` (section 5.10).
    advertised_urls: Mutex<Option<Arc<AdvertisedUrls>>>,
    /// DNS-01 records this primary created, per node: `record_id` to it.
    dns_records: Mutex<HashMap<String, HashMap<String, DnsRecord>>>,
    /// The source address each node's session came from, in memory only: the
    /// edge's WireGuard endpoint (ADR 0144).
    session_ips: Mutex<HashMap<String, IpAddr>>,
    /// The newest `edge_backends` report of each edge machine, in memory only
    /// (section 5.3, ADR 0143).
    edge_reports: Mutex<HashMap<String, Vec<wire::EdgeBackendStatus>>>,
    /// The edge's health view of each app between reports (ADR 0143).
    edge_health: Mutex<BTreeMap<String, crate::edge_health::AppHealth>>,
    /// Free space on the volume filesystem each node last reported
    /// (`status_report.disk_free_gb`, section 5.3).
    disk_free_gb: Mutex<HashMap<String, f64>>,
    generation: AtomicU64,
}

/// A DNS-01 TXT record the primary created for one node (section 5.22).
#[derive(Clone, Debug)]
struct DnsRecord {
    hostname: String,
    provider_id: String,
}

/// The live node sessions, shared by every handler.
#[derive(Clone)]
pub struct NodeHub(Arc<HubInner>);

impl NodeHub {
    pub(crate) fn new(
        store: Store,
        config: Arc<ServerConfig>,
        keys: Arc<ClusterKeys>,
        cluster_id: String,
        resolver: Arc<dyn DigestResolver>,
        data_key: Arc<DataKey>,
        backups: crate::backups::Backups,
    ) -> Self {
        Self(Arc::new(HubInner {
            store,
            config,
            keys,
            cluster_id,
            resolver,
            data_key,
            backups,
            sessions: Mutex::new(HashMap::new()),
            digests: desired::DigestCache::default(),
            vm_nodes: Mutex::new(HashMap::new()),
            tunnels: Mutex::new(None),
            endpoints: Mutex::new(CloudflareEndpoints::production()),
            advertised_urls: Mutex::new(None),
            dns_records: Mutex::new(HashMap::new()),
            session_ips: Mutex::new(HashMap::new()),
            edge_reports: Mutex::new(HashMap::new()),
            edge_health: Mutex::new(BTreeMap::new()),
            disk_free_gb: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }))
    }

    /// Whether `node_id` has a live session.
    #[must_use]
    pub fn is_connected(&self, node_id: &str) -> bool {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .contains_key(node_id)
    }

    /// Tells the session of `vm_id`'s node to rebuild its snapshot. Runs in
    /// the background, so a request never waits for the node.
    pub fn vm_changed(&self, vm_id: &str) {
        let inner = Arc::clone(&self.0);
        let vm_id = vm_id.to_string();
        tokio::spawn(async move { inner.vm_changed(&vm_id).await });
    }

    /// Tells the live session of `node_id` to rebuild and resend its desired
    /// state (section 5.11). A node without a session is left alone: it gets
    /// the new snapshot when it dials.
    pub fn node_changed(&self, node_id: &str) {
        let changed = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(node_id)
                .map(|handle| Arc::clone(&handle.changed))
        };
        if let Some(changed) = changed {
            changed.notify_one();
        }
    }

    /// Tells every edge machine's live session to rebuild its snapshot and to
    /// reconcile its tunnel, because an input of `spec.edge` changed (ADR
    /// 0143): a VM, a domain, an app or the edge's own tunnel mapping.
    pub fn edge_changed(&self) {
        let inner = Arc::clone(&self.0);
        tokio::spawn(async move { inner.edge_changed().await });
    }

    /// Tells every live session to rebuild and resend its desired state
    /// (section 5.11). Mesh peers depend on other nodes' rows, so a node
    /// whose key or address changed has to reach all of them. A node without
    /// a session is left alone: it gets the new snapshot when it dials.
    pub fn nodes_changed_all(&self) {
        let changed: Vec<Arc<Notify>> = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .values()
                .map(|handle| Arc::clone(&handle.changed))
                .collect()
        };
        for notify in changed {
            notify.notify_one();
        }
    }

    /// Records the source address `node_id`'s session came from (ADR 0144) and
    /// returns whether it differs from the one already recorded. Nothing is
    /// stored when the peer address is unknown.
    fn remember_session_ip(&self, node_id: &str, ip: Option<IpAddr>) -> bool {
        let Some(ip) = ip else {
            return false;
        };
        let mut ips = self.0.session_ips.lock().expect("session ip lock");
        if ips.get(node_id) == Some(&ip) {
            return false;
        }
        ips.insert(node_id.to_string(), ip);
        true
    }

    /// The source address `node_id`'s session came from, when the primary
    /// recorded one.
    fn session_ip(&self, node_id: &str) -> Option<IpAddr> {
        self.0
            .session_ips
            .lock()
            .expect("session ip lock")
            .get(node_id)
            .copied()
    }

    /// Queues one control frame to `node_id`'s live session, whatever its
    /// generation. Returns whether a session took the frame; a node without
    /// one receives the change with its next snapshot.
    pub(crate) fn send_control(
        &self,
        node_id: &str,
        message: &quoro_proto::wire::ControlMessage,
    ) -> bool {
        let text = message.to_frame(&new_msg_id(), None).to_text();
        let tx = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions.get(node_id).map(|handle| handle.tx.clone())
        };
        match tx {
            Some(tx) => tx.send(text).is_ok(),
            None => false,
        }
    }

    /// Free space on `node_id`'s volume filesystem as its last `status_report`
    /// said (section 5.3), or `None` before the first report.
    #[must_use]
    pub fn disk_free_gb(&self, node_id: &str) -> Option<f64> {
        self.0
            .disk_free_gb
            .lock()
            .expect("disk free lock")
            .get(node_id)
            .copied()
    }

    /// Asks the tunnel manager to reconcile `node_id` after a snapshot was
    /// sent (section 5.11, ADR 0094).
    pub(crate) fn tunnel_changed(&self, node_id: &str) {
        let kick = {
            let tunnels = self.0.tunnels.lock().expect("tunnel handle lock");
            tunnels.clone()
        };
        if let Some(kick) = kick {
            kick.kick(node_id);
        }
    }

    /// The newest `edge_backends` report of each edge machine, keyed by edge
    /// node id and ordered by it. Empty while no edge reported since the
    /// primary started: the report is kept in memory only (ADR 0143).
    #[must_use]
    pub fn edge_reports(&self) -> Vec<(String, Vec<wire::EdgeBackendStatus>)> {
        let reports = self.0.edge_reports.lock().expect("edge report lock");
        let mut reports: Vec<(String, Vec<wire::EdgeBackendStatus>)> = reports
            .iter()
            .map(|(node_id, entries)| (node_id.clone(), entries.clone()))
            .collect();
        reports.sort_by(|a, b| a.0.cmp(&b.0));
        reports
    }

    /// Registers the tunnel manager.
    pub(crate) fn set_tunnel_manager(&self, manager: &crate::tunnel::TunnelManager) {
        *self.0.tunnels.lock().expect("tunnel handle lock") = Some(manager.kick_handle());
    }

    /// Points the Cloudflare clients at other endpoints (tests).
    pub(crate) fn set_cloudflare_endpoints(&self, endpoints: CloudflareEndpoints) {
        *self.0.endpoints.lock().expect("endpoints lock") = endpoints;
    }

    /// Where the Cloudflare clients point.
    #[must_use]
    pub(crate) fn cloudflare_endpoints(&self) -> CloudflareEndpoints {
        self.0.endpoints.lock().expect("endpoints lock").clone()
    }

    /// Installs the source of `welcome.primary_urls` (section 5.10).
    pub(crate) fn set_advertised_urls(&self, provider: Arc<AdvertisedUrls>) {
        *self.0.advertised_urls.lock().expect("advertised urls lock") = Some(provider);
    }

    /// The `welcome.primary_urls` this primary hands out: `https://` entries
    /// only, deduplicated, at most [`wire::MAX_PRIMARY_URLS`] (section 5.10).
    #[must_use]
    pub(crate) fn advertised_urls(&self) -> Vec<String> {
        let provider = self
            .0
            .advertised_urls
            .lock()
            .expect("advertised urls lock")
            .clone();
        let Some(provider) = provider else {
            return Vec::new();
        };
        let mut urls: Vec<String> = Vec::new();
        for url in provider() {
            if !url.starts_with("https://") || urls.contains(&url) {
                continue;
            }
            urls.push(url);
            if urls.len() == wire::MAX_PRIMARY_URLS {
                break;
            }
        }
        urls
    }

    /// Remembers a DNS-01 record this primary created for `node_id`.
    fn remember_dns_record(&self, node_id: &str, record_id: &str, record: DnsRecord) {
        self.0
            .dns_records
            .lock()
            .expect("dns records lock")
            .entry(node_id.to_string())
            .or_default()
            .insert(record_id.to_string(), record);
    }

    /// A DNS-01 record of this node, if the primary created it.
    fn dns_record(&self, node_id: &str, record_id: &str) -> Option<DnsRecord> {
        self.0
            .dns_records
            .lock()
            .expect("dns records lock")
            .get(node_id)
            .and_then(|records| records.get(record_id))
            .cloned()
    }

    /// Forgets a DNS-01 record once its TXT record is gone.
    fn take_dns_record(&self, node_id: &str, record_id: &str) {
        if let Some(records) = self
            .0
            .dns_records
            .lock()
            .expect("dns records lock")
            .get_mut(node_id)
        {
            records.remove(record_id);
        }
    }

    /// Sends one `command` to `node_id` and waits for its `command_result`
    /// (section 5.17).
    ///
    /// # Errors
    ///
    /// [`HubError::NotConnected`] without a live session, [`HubError::Timeout`]
    /// when no answer arrives within `timeout`, or [`HubError::Closed`] when
    /// the session ends first.
    pub async fn command(
        &self,
        node_id: &str,
        kind: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<quoro_proto::CommandResult, HubError> {
        let (_generation, tx, pending) = self.session_for(node_id)?;
        let id = new_msg_id();
        let (sender, receiver) = oneshot::channel();
        pending
            .lock()
            .expect("pending lock")
            .commands
            .insert(id.clone(), sender);
        let text = ControlMessage::Command(wire::Command {
            kind: kind.to_string(),
            params,
        })
        .to_frame(&id, None)
        .to_text();
        if tx.send(text).is_err() {
            pending.lock().expect("pending lock").commands.remove(&id);
            return Err(HubError::Closed);
        }
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(HubError::Closed),
            Err(_) => {
                pending.lock().expect("pending lock").commands.remove(&id);
                Err(HubError::Timeout)
            }
        }
    }

    /// Opens a log stream on `node_id` (section 5.15).
    ///
    /// # Errors
    ///
    /// [`HubError::NotConnected`] without a live session, [`HubError::Closed`]
    /// when it ends at once, or [`HubError::StreamLimit`] when the session
    /// already holds [`MAX_STREAMS`] streams.
    pub fn open_stream(
        &self,
        node_id: &str,
        kind: &str,
        params: Value,
    ) -> Result<NodeStream, HubError> {
        let (generation, tx, pending) = self.session_for(node_id)?;
        let stream_id = new_msg_id();
        let open_id = new_msg_id();
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        {
            let mut pending = pending.lock().expect("pending lock");
            if pending.at_stream_limit() {
                return Err(HubError::StreamLimit);
            }
            pending.streams.insert(
                stream_id.clone(),
                StreamSlot {
                    tx: events_tx,
                    last_seq: 0,
                    kind: kind.to_string(),
                },
            );
            pending.opens.insert(open_id.clone(), stream_id.clone());
        }
        let text = ControlMessage::StreamOpen(wire::StreamOpen {
            stream_id: stream_id.clone(),
            kind: kind.to_string(),
            params,
        })
        .to_frame(&open_id, None)
        .to_text();
        if tx.send(text).is_err() {
            let mut pending = pending.lock().expect("pending lock");
            pending.streams.remove(&stream_id);
            pending.opens.remove(&open_id);
            return Err(HubError::Closed);
        }
        Ok(NodeStream {
            inner: Arc::clone(&self.0),
            node_id: node_id.to_string(),
            generation,
            stream_id,
            rx: events_rx,
            ended: false,
        })
    }

    /// The generation, outbox and pending maps of `node_id`'s live session.
    fn session_for(&self, node_id: &str) -> Result<(u64, SessionSender, SessionPending), HubError> {
        let sessions = self.0.sessions.lock().expect("session lock");
        sessions
            .get(node_id)
            .map_or(Err(HubError::NotConnected), |handle| {
                Ok((
                    handle.generation,
                    handle.tx.clone(),
                    Arc::clone(&handle.pending),
                ))
            })
    }

    /// Marks every node the database still calls `connected` as
    /// `disconnected`, emitting `node.disconnected` for each.
    ///
    /// Runs once at start, before the listener opens: no session can be live
    /// yet, and a node whose session ended with the previous process (a
    /// restart, a crash) must not stay `connected` until it dials again, or
    /// forever if it never does.
    ///
    /// # Errors
    ///
    /// A database error.
    pub async fn reset_stale_connections(&self) -> Result<(), String> {
        let stale: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM nodes WHERE status = 'connected'")
                .fetch_all(self.0.store.pool())
                .await
                .map_err(|e| format!("cannot read the node list: {e}"))?;
        for (node_id,) in stale {
            disconnect_node(self, &node_id, "session_closed").await?;
        }
        Ok(())
    }

    /// Ends every live session, for a clean shutdown.
    pub fn shutdown(&self) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        for (_, handle) in sessions.drain() {
            let _ = handle.closed.send(true);
            handle.pending.lock().expect("pending lock").fail_all();
        }
    }

    /// Registers a new session for `node_id`, closing the older one first.
    fn attach(
        &self,
        node_id: &str,
    ) -> (
        u64,
        mpsc::UnboundedReceiver<String>,
        watch::Receiver<bool>,
        Arc<Notify>,
    ) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        if let Some(old) = sessions.remove(node_id) {
            // Section 3.1: one session per node; the older one closes and must
            // not mark the node disconnected.
            let _ = old.closed.send(true);
            old.pending.lock().expect("pending lock").fail_all();
        }
        let generation = self.0.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        let (closed, closed_rx) = watch::channel(false);
        let changed = Arc::new(Notify::new());
        sessions.insert(
            node_id.to_string(),
            SessionHandle {
                generation,
                tx,
                closed,
                changed: Arc::clone(&changed),
                pending: Arc::new(Mutex::new(Pending::default())),
            },
        );
        (generation, rx, closed_rx, changed)
    }

    /// Removes the session if it is still the current generation, returning
    /// its pending commands and streams so the caller can fail them.
    fn detach(&self, node_id: &str, generation: u64) -> Option<Arc<Mutex<Pending>>> {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        if sessions
            .get(node_id)
            .is_some_and(|handle| handle.generation == generation)
        {
            sessions.remove(node_id).map(|handle| handle.pending)
        } else {
            None
        }
    }

    /// Whether `generation` is still the live session of `node_id`.
    fn is_current(&self, node_id: &str, generation: u64) -> bool {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(node_id)
            .is_some_and(|handle| handle.generation == generation)
    }

    /// Queues one text frame to `generation` of the node's session.
    fn send(&self, node_id: &str, generation: u64, text: String) -> bool {
        let tx = {
            let sessions = self.0.sessions.lock().expect("session lock");
            sessions
                .get(node_id)
                .filter(|handle| handle.generation == generation)
                .map(|handle| handle.tx.clone())
        };
        match tx {
            Some(tx) => tx.send(text).is_ok(),
            None => false,
        }
    }

    /// Records `vm -> node` for every VM of a built snapshot, so a later
    /// deletion can still find the node after the row is gone.
    fn remember_vms(&self, node_id: &str, spec: &wire::Spec) {
        let mut map = self.0.vm_nodes.lock().expect("vm map lock");
        for vm in &spec.vms {
            map.insert(vm.vm_id.clone(), node_id.to_string());
        }
    }
}

impl HubInner {
    async fn vm_changed(&self, vm_id: &str) {
        let node_id = match sqlx::query_scalar::<_, String>("SELECT node_id FROM vms WHERE id = ?")
            .bind(vm_id)
            .fetch_optional(self.store.pool())
            .await
        {
            Ok(Some(node_id)) => Some(node_id),
            Ok(None) => self
                .vm_nodes
                .lock()
                .expect("vm map lock")
                .get(vm_id)
                .cloned(),
            Err(e) => {
                tracing::warn!(%e, vm_id, "cannot look up the VM's node");
                None
            }
        };
        if let Some(node_id) = node_id {
            let changed = {
                let sessions = self.sessions.lock().expect("session lock");
                sessions
                    .get(&node_id)
                    .map(|handle| Arc::clone(&handle.changed))
            };
            if let Some(changed) = changed {
                changed.notify_one();
            }
        }
        // The edge's `spec.edge` lists this VM's address and port, whichever
        // node runs it (ADR 0143).
        self.edge_changed().await;
    }

    /// Tells every edge machine with a live session to rebuild its snapshot
    /// and to reconcile its tunnel, since `spec.edge` reads `vms`, `domains`
    /// and `apps.replicas` of the whole cluster.
    async fn edge_changed(&self) {
        let rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT id, roles FROM nodes WHERE status <> 'removed' ORDER BY id",
        )
        .fetch_all(self.store.pool())
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(%e, "cannot read the edge machines");
                return;
            }
        };
        for (node_id, roles) in rows {
            let holds_role = match serde_json::from_str::<Vec<String>>(&roles) {
                Ok(roles) => roles.iter().any(|role| role == "edge"),
                Err(e) => {
                    tracing::warn!(%e, node_id, "the stored roles are not a JSON array");
                    false
                }
            };
            if !holds_role {
                continue;
            }
            let changed = {
                let sessions = self.sessions.lock().expect("session lock");
                sessions
                    .get(&node_id)
                    .map(|handle| Arc::clone(&handle.changed))
            };
            let Some(changed) = changed else {
                // A machine without a session gets the new snapshot when it
                // dials.
                continue;
            };
            changed.notify_one();
            let kick = {
                let tunnels = self.tunnels.lock().expect("tunnel handle lock");
                tunnels.clone()
            };
            if let Some(kick) = kick {
                kick.kick(&node_id);
            }
        }
    }

    /// The pending maps of `node_id`'s session, when it is this generation.
    fn pending_of(&self, node_id: &str, generation: u64) -> Option<Arc<Mutex<Pending>>> {
        let sessions = self.sessions.lock().expect("session lock");
        sessions
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .map(|handle| Arc::clone(&handle.pending))
    }

    /// Routes one `command_result` to its waiter, or ends a stream whose
    /// `stream_open` failed (sections 5.9 and 5.15).
    fn resolve_command_result(
        &self,
        node_id: &str,
        generation: u64,
        re: &str,
        result: quoro_proto::CommandResult,
    ) {
        let Some(pending) = self.pending_of(node_id, generation) else {
            return;
        };
        let mut pending = pending.lock().expect("pending lock");
        if let Some(sender) = pending.commands.remove(re) {
            let _ = sender.send(Ok(result));
            return;
        }
        let Some(stream_id) = pending.opens.remove(re) else {
            return;
        };
        if result.status == quoro_proto::CommandStatus::Error
            && let Some(slot) = pending.streams.remove(&stream_id)
        {
            let code = result.error.as_ref().map(|error| error.code);
            let message = result
                .error
                .map(|error| error.message)
                .or(result.detail)
                .unwrap_or_else(|| "the node could not open the stream".to_string());
            let event = if is_byte_stream(&slot.kind) {
                StreamEvent::SessionEnd {
                    reason: SessionEndReason::Error,
                    message,
                    exit_code: None,
                    transcript_sha256: None,
                    transcript_bytes: None,
                    node_code: code,
                }
            } else {
                StreamEvent::End {
                    reason: "error".to_string(),
                    message,
                }
            };
            let _ = slot.tx.send(event);
        }
    }

    /// Routes one `stream_data` to its stream (section 5.7). Data for an
    /// unknown stream id is dropped, as is reserved byte data.
    fn resolve_stream_data(&self, node_id: &str, generation: u64, data: wire::StreamData) {
        let Some(pending) = self.pending_of(node_id, generation) else {
            return;
        };
        let mut pending = pending.lock().expect("pending lock");
        let Some(slot) = pending.streams.get_mut(&data.stream_id) else {
            return;
        };
        if data.seq <= slot.last_seq {
            tracing::warn!(
                stream_id = %data.stream_id,
                seq = data.seq,
                last_seq = slot.last_seq,
                "stream_data is out of order"
            );
        }
        slot.last_seq = data.seq;
        if let Some(lines) = data.lines
            && !lines.is_empty()
        {
            let _ = slot.tx.send(StreamEvent::Lines(lines));
        }
        if let Some(metrics) = data.metrics {
            let _ = slot.tx.send(StreamEvent::Metrics(metrics));
        }
        if let Some(encoded) = data.data {
            match base64::engine::general_purpose::STANDARD.decode(encoded.as_bytes()) {
                Ok(bytes) if bytes.len() > MAX_STREAM_DATA_BYTES => {
                    tracing::warn!(
                        stream_id = %data.stream_id,
                        bytes = bytes.len(),
                        "stream_data carries more than 64 KiB"
                    );
                }
                Ok(bytes) if !bytes.is_empty() => {
                    let _ = slot.tx.send(StreamEvent::Data(bytes));
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    stream_id = %data.stream_id,
                    error = %e,
                    "stream_data is not base64"
                ),
            }
        }
    }

    /// Routes one `stream_end` to its stream (section 5.8).
    fn resolve_stream_end(&self, node_id: &str, generation: u64, end: wire::StreamEnd) {
        let Some(pending) = self.pending_of(node_id, generation) else {
            return;
        };
        let mut pending = pending.lock().expect("pending lock");
        if let Some(slot) = pending.streams.remove(&end.stream_id) {
            pending
                .opens
                .retain(|_, stream_id| stream_id != &end.stream_id);
            let message = end.message.unwrap_or_default();
            let event = if is_byte_stream(&slot.kind) {
                StreamEvent::SessionEnd {
                    reason: session_end_reason(end.reason),
                    message,
                    exit_code: end.exit_code,
                    transcript_sha256: end.transcript_sha256,
                    transcript_bytes: end.transcript_bytes,
                    node_code: None,
                }
            } else {
                StreamEvent::End {
                    reason: stream_reason(end.reason).to_string(),
                    message,
                }
            };
            let _ = slot.tx.send(event);
        }
    }

    /// Unregisters a dropped stream and tells the node to stop producing it
    /// (section 5.16).
    fn close_stream(&self, node_id: &str, generation: u64, stream_id: &str) {
        let Some(pending) = self.pending_of(node_id, generation) else {
            return;
        };
        let removed = {
            let mut pending = pending.lock().expect("pending lock");
            pending
                .opens
                .retain(|_, open_stream| open_stream != stream_id);
            pending.streams.remove(stream_id).is_some()
        };
        if removed {
            let text = ControlMessage::StreamClose(wire::StreamClose {
                stream_id: stream_id.to_string(),
            })
            .to_frame(&new_msg_id(), None)
            .to_text();
            self.send_text(node_id, generation, text);
        }
    }

    /// Queues one text frame to `generation` of the node's session.
    fn send_text(&self, node_id: &str, generation: u64, text: String) -> bool {
        let tx = {
            let sessions = self.sessions.lock().expect("session lock");
            sessions
                .get(node_id)
                .filter(|handle| handle.generation == generation)
                .map(|handle| handle.tx.clone())
        };
        match tx {
            Some(tx) => tx.send(text).is_ok(),
            None => false,
        }
    }
}

/// The wire form of a [`wire::StreamEndReason`].
fn stream_reason(reason: wire::StreamEndReason) -> &'static str {
    match reason {
        wire::StreamEndReason::Completed => "completed",
        wire::StreamEndReason::Error => "error",
        wire::StreamEndReason::Cancelled => "cancelled",
        wire::StreamEndReason::VmStopped => "vm_stopped",
        wire::StreamEndReason::Limit => "limit",
    }
}

/// The byte-stream form of a [`wire::StreamEndReason`].
fn session_end_reason(reason: wire::StreamEndReason) -> SessionEndReason {
    match reason {
        wire::StreamEndReason::Completed => SessionEndReason::Completed,
        wire::StreamEndReason::Error => SessionEndReason::Error,
        wire::StreamEndReason::Cancelled => SessionEndReason::Cancelled,
        wire::StreamEndReason::VmStopped => SessionEndReason::VmStopped,
        wire::StreamEndReason::Limit => SessionEndReason::Limit,
    }
}

/// A log stream opened through a node session (sections 5.7, 5.8 and 5.16).
///
/// Dropping a stream that has not ended sends `stream_close` for it, so the
/// node stops producing as soon as the HTTP consumer of the stream is gone.
pub struct NodeStream {
    inner: Arc<HubInner>,
    node_id: String,
    generation: u64,
    stream_id: String,
    rx: mpsc::UnboundedReceiver<StreamEvent>,
    ended: bool,
}

impl NodeStream {
    /// Waits for the next event, or `None` once the stream ended.
    pub async fn next(&mut self) -> Option<StreamEvent> {
        if self.ended {
            return None;
        }
        match self.rx.recv().await {
            Some(event) => {
                if matches!(
                    event,
                    StreamEvent::End { .. } | StreamEvent::SessionEnd { .. }
                ) {
                    self.ended = true;
                }
                Some(event)
            }
            None => {
                self.ended = true;
                None
            }
        }
    }

    /// The `msg_` id the node knows this stream by.
    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Whether this stream already ended (`stream_end` received, or the
    /// session's channel closed).
    #[must_use]
    pub fn is_ended(&self) -> bool {
        self.ended
    }

    /// Asks the node to stop producing this stream (section 5.16) without
    /// unregistering its routing, so a following `stream_end` — and the
    /// transcript fields it carries — still reaches [`Self::next`]. Unlike
    /// dropping an unended stream, this leaves the cleanup to `Drop`.
    /// Returns whether the frame reached the session's outbox.
    #[must_use]
    pub fn request_close(&self) -> bool {
        self.inner.send_text(
            &self.node_id,
            self.generation,
            ControlMessage::StreamClose(wire::StreamClose {
                stream_id: self.stream_id.clone(),
            })
            .to_frame(&new_msg_id(), None)
            .to_text(),
        )
    }

    /// Sends one `stream_input` (section 5.24): input bytes, a new window size,
    /// or the end of input. `seq` is gap-free from 1 per stream. Returns whether
    /// the frame reached the session's outbox.
    #[must_use]
    pub fn send_input(
        &self,
        seq: u64,
        data: Option<&[u8]>,
        resize: Option<wire::WindowSize>,
        eof: bool,
    ) -> bool {
        let input = wire::StreamInput {
            stream_id: self.stream_id.clone(),
            seq,
            data: data.map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
            resize,
            eof: eof.then_some(true),
        };
        self.inner.send_text(
            &self.node_id,
            self.generation,
            ControlMessage::StreamInput(input)
                .to_frame(&new_msg_id(), None)
                .to_text(),
        )
    }

    /// Acknowledges `acked_bytes` cumulative decoded bytes of a `data` stream
    /// (section 5.25), granting the node more window.
    #[must_use]
    pub fn send_ack(&self, acked_bytes: u64) -> bool {
        let ack = wire::StreamAck {
            stream_id: self.stream_id.clone(),
            acked_bytes,
        };
        self.inner.send_text(
            &self.node_id,
            self.generation,
            ControlMessage::StreamAck(ack)
                .to_frame(&new_msg_id(), None)
                .to_text(),
        )
    }
}

impl Drop for NodeStream {
    fn drop(&mut self) {
        if !self.ended {
            self.inner
                .close_stream(&self.node_id, self.generation, &self.stream_id);
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP entry point
// ---------------------------------------------------------------------------

/// `GET /api/v1/nodes/session` (section 3.1).
///
/// Authentication is the node credential itself, so this route does not use
/// the cookie/bearer human identity at all: the `node` scope has no other
/// consumer.
///
/// # Errors
///
/// `unauthenticated` for an unknown or revoked credential, or `forbidden` for
/// a node that was removed.
#[utoipa::path(
    get,
    path = "/api/v1/nodes/session",
    tag = "nodes",
    operation_id = "openNodeSession",
    security(("nodeCredential" = [])),
    extensions(("x-quoro-scope" = json!("node"))),
    responses(
        (status = 101, description = "Switching protocols; the connection is now a WebSocket.",
            headers(
                ("Quoro-Request-Id" = String, description = "Correlation id of this call."),
                ("Upgrade" = String, description = "Always `websocket`."),
            )),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn open_node_session(
    State(state): State<AppState>,
    node: NodeId,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let node_id = node.id;
    // The peer address is absent when the router runs without connect info
    // (tests); the mesh then has no endpoint for this machine (ADR 0144).
    let peer_ip = node.peer_ip;
    let hub = state.hub.clone();
    Ok(ws
        .max_message_size(wire::MAX_MESSAGE_BYTES)
        .max_frame_size(wire::MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_session(hub, node_id, socket, peer_ip))
        .into_response())
}

/// The node id a credential authenticates, and the address its session came
/// from. It is a request-parts extractor so authentication runs before the
/// WebSocket extractor: a bad credential is a problem+json `401`, not an
/// upgrade rejection. The peer address is absent when the router runs without
/// connect info (tests).
pub struct NodeId {
    /// The authenticated node.
    id: String,
    /// The source address of the connection, when the listener reports it.
    peer_ip: Option<IpAddr>,
}

impl FromRequestParts<AppState> for NodeId {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let id = authenticate_node(state, &parts.headers).await?;
        let peer_ip = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(address)| address.ip());
        Ok(Self { id, peer_ip })
    }
}

/// Resolves `Authorization: Bearer <credential>` to a node id.
async fn authenticate_node(state: &AppState, headers: &HeaderMap) -> Result<String, ApiError> {
    let credential = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::unauthenticated)?;
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT c.node_id, n.status FROM node_credentials c JOIN nodes n ON n.id = c.node_id \
         WHERE c.credential_hash = ? AND c.revoked_at IS NULL",
    )
    .bind(sha256_hex(credential.as_bytes()))
    .fetch_optional(state.store.pool())
    .await?;
    let (node_id, status) = row.ok_or_else(ApiError::unauthenticated)?;
    if status == "removed" {
        return Err(ApiError::new(
            Code::Forbidden,
            "the node was removed from the cluster",
        ));
    }
    Ok(node_id)
}

// ---------------------------------------------------------------------------
// Session task
// ---------------------------------------------------------------------------

struct Session {
    hub: NodeHub,
    node_id: String,
    /// Which session of this node this task is.
    generation: u64,
    connected: bool,
    desired_rev: u64,
    last_spec: Option<wire::Spec>,
    notified_deleted: HashSet<String>,
    retry_pending: bool,
    /// Consecutive builds in which an app VM's image did not resolve.
    digest_failures: HashMap<String, u32>,
}

async fn run_session(
    hub: NodeHub,
    node_id: String,
    mut socket: WebSocket,
    peer_ip: Option<IpAddr>,
) {
    let (generation, mut rx, mut closed, changed) = hub.attach(&node_id);
    // ADR 0144: the edge's WireGuard endpoint is the source address of its
    // session, so a changed address changes every node's peer entry.
    if hub.remember_session_ip(&node_id, peer_ip) {
        hub.nodes_changed_all();
    }
    let timeout = Duration::from_millis(hub.0.config.session_timeout_ms.max(1));
    let mut deadline = Instant::now() + timeout;
    let mut outbox: Vec<String> = Vec::new();
    let mut session = Session {
        hub,
        node_id,
        generation,
        connected: false,
        desired_rev: 0,
        last_spec: None,
        notified_deleted: HashSet::new(),
        retry_pending: false,
        digest_failures: HashMap::new(),
    };
    let mut superseded = false;
    let mut reason = "session_closed";
    let mut close_after_error = false;
    let retry = tokio::time::sleep(SNAPSHOT_RETRY);
    tokio::pin!(retry);

    'session: loop {
        while let Ok(text) = rx.try_recv() {
            outbox.push(text);
        }
        for text in outbox.drain(..) {
            if socket.send(Message::Text(text.into())).await.is_err() {
                break 'session;
            }
        }
        if close_after_error {
            break;
        }

        let sleep = tokio::time::sleep_until(deadline);
        tokio::select! {
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        deadline = Instant::now() + timeout;
                        let was_pending = session.retry_pending;
                        if session.handle_text(text.as_str(), &mut outbox).await {
                            close_after_error = true;
                        }
                        if session.retry_pending && !was_pending {
                            retry.as_mut().reset(Instant::now() + SNAPSHOT_RETRY);
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        deadline = Instant::now() + timeout;
                        outbox.push(protocol_error(
                            quoro_proto::ErrorCode::BadMessage,
                            "binary frames are not used in v1",
                            false,
                            None,
                        ));
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        deadline = Instant::now() + timeout;
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                }
            }
            queued = rx.recv() => {
                match queued {
                    Some(text) => outbox.push(text),
                    None => break,
                }
            }
            changed_result = closed.changed() => {
                if changed_result.is_err() || *closed.borrow() {
                    superseded = true;
                    break;
                }
            }
            () = changed.notified(), if session.connected => {
                let was_pending = session.retry_pending;
                match session.refresh(0, false).await {
                    Ok(()) => session.retry_pending = false,
                    Err(e) => {
                        tracing::warn!(%e, node_id = %session.node_id, "cannot build the desired state");
                        session.retry_pending = true;
                    }
                }
                if session.retry_pending && !was_pending {
                    retry.as_mut().reset(Instant::now() + SNAPSHOT_RETRY);
                }
            }
            () = &mut retry, if session.retry_pending => {
                match session.refresh(0, false).await {
                    Ok(()) => session.retry_pending = false,
                    Err(e) => {
                        tracing::warn!(%e, node_id = %session.node_id, "cannot build the desired state");
                        retry.as_mut().reset(Instant::now() + SNAPSHOT_RETRY);
                    }
                }
            }
            () = sleep => {
                if session.connected {
                    reason = "heartbeat_timeout";
                }
                break;
            }
        }
    }

    if let Some(pending) = session.hub.detach(&session.node_id, generation) {
        pending.lock().expect("pending lock").fail_all();
    }
    if session.connected
        && !superseded
        && let Err(e) = disconnect_node(&session.hub, &session.node_id, reason).await
    {
        tracing::warn!(%e, node_id = %session.node_id, "cannot record the disconnect");
    }
    // A close frame tells the node the session ended; the client may already
    // be gone, which is fine.
    let _ = socket.send(Message::Close(None)).await;
}

impl Session {
    /// Handles one text frame. Returns whether the session must end.
    async fn handle_text(&mut self, text: &str, outbox: &mut Vec<String>) -> bool {
        let raw = match wire::RawFrame::parse(text) {
            Ok(raw) => raw,
            Err(error) => return decode_failure(&error, outbox),
        };

        if !self.connected {
            // Section 4.2 rule 1: `hello` is the first message of every
            // session.
            if raw.kind != "hello" {
                outbox.push(protocol_error(
                    quoro_proto::ErrorCode::BadMessage,
                    "the first frame must be hello",
                    false,
                    Some(&raw.id),
                ));
                return false;
            }
            let message = match NodeMessage::from_raw(&raw) {
                Ok(message) => message,
                Err(error) => return decode_failure(&error, outbox),
            };
            let NodeMessage::Hello(hello) = message else {
                return false;
            };
            if hello.node_id != self.node_id {
                outbox.push(protocol_error(
                    quoro_proto::ErrorCode::Unauthenticated,
                    "the hello node_id does not match the credential",
                    true,
                    Some(&raw.id),
                ));
                return true;
            }
            match connect_node(&self.hub, &self.node_id, &hello).await {
                Ok(changed) => {
                    if changed {
                        self.hub.nodes_changed_all();
                    }
                }
                Err(e) => {
                    tracing::warn!(%e, node_id = %self.node_id, "cannot record the session start");
                    outbox.push(protocol_error(
                        quoro_proto::ErrorCode::BadMessage,
                        "the primary could not record this session",
                        false,
                        Some(&raw.id),
                    ));
                    return false;
                }
            }
            self.connected = true;

            let term = cluster_term(&self.hub).await.unwrap_or_else(|e| {
                tracing::warn!(%e, "cannot read the cluster term");
                1
            });
            let primary_node_id = primary_node_id(&self.hub, &self.node_id)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!(%e, "cannot read the primary node");
                    self.node_id.clone()
                });
            outbox.push(
                ControlMessage::Welcome(wire::Welcome {
                    cluster_id: self.hub.0.cluster_id.clone(),
                    term,
                    server_time: now_rfc3339(),
                    heartbeat_interval_ms: self.hub.0.config.heartbeat_interval_ms,
                    session_timeout_ms: self.hub.0.config.session_timeout_ms,
                    primary_node_id,
                    primary_urls: self.hub.advertised_urls(),
                })
                .to_frame(&new_msg_id(), Some(&raw.id))
                .to_text(),
            );

            // Section 4.2 rule 3: stale or superseded leases are revoked
            // before anything else; then the desired state (rule 5), then the
            // app leases this node holds (section 5.12).
            self.revoke_stale_leases(&hello.leases, outbox).await;
            match self.refresh(hello.stored_rev, true).await {
                Ok(()) => self.retry_pending = false,
                Err(e) => {
                    tracing::warn!(%e, node_id = %self.node_id, "cannot build the desired state");
                    self.retry_pending = true;
                }
            }
            self.send_grants(outbox).await;
            return false;
        }

        if raw.kind == "hello" {
            outbox.push(protocol_error(
                quoro_proto::ErrorCode::BadMessage,
                "hello was already received on this session",
                false,
                Some(&raw.id),
            ));
            return false;
        }
        let message = match NodeMessage::from_raw(&raw) {
            Ok(message) => message,
            Err(error) => return decode_failure(&error, outbox),
        };
        match message {
            NodeMessage::Heartbeat(_) | NodeMessage::Pong => {}
            NodeMessage::Ping => outbox.push(
                ControlMessage::Pong
                    .to_frame(&new_msg_id(), Some(&raw.id))
                    .to_text(),
            ),
            NodeMessage::StatusReport(report) => {
                if let Err(e) = apply_status_report(
                    &self.hub,
                    &self.node_id,
                    &report,
                    &mut self.notified_deleted,
                )
                .await
                {
                    tracing::warn!(%e, node_id = %self.node_id, "cannot apply a status report");
                }
            }
            NodeMessage::Event(event) => {
                if event.source.get("node_id").and_then(Value::as_str)
                    != Some(self.node_id.as_str())
                {
                    outbox.push(protocol_error(
                        quoro_proto::ErrorCode::BadMessage,
                        "the event source does not name this node",
                        false,
                        Some(&raw.id),
                    ));
                } else {
                    match insert_node_event(&self.hub, &self.node_id, &event).await {
                        Ok(()) => {
                            if event.kind == "deployment.failed" {
                                match deploy::on_node_deployment_failed(
                                    &self.hub.0.store,
                                    &self.node_id,
                                    &event.data,
                                )
                                .await
                                {
                                    Ok(vms) => {
                                        for vm_id in vms {
                                            self.hub.vm_changed(&vm_id);
                                        }
                                    }
                                    Err(e) => tracing::warn!(
                                        node_id = %self.node_id,
                                        code = %e.code.as_str(),
                                        "cannot fail the deployment"
                                    ),
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(%e, event_id = %event.id, "cannot store a node event");
                        }
                    }
                }
            }
            NodeMessage::CommandResult(result) => {
                if let Some(re) = raw.re.as_deref() {
                    self.hub
                        .0
                        .resolve_command_result(&self.node_id, self.generation, re, result);
                } else {
                    tracing::debug!(
                        node_id = %self.node_id,
                        status = ?result.status,
                        "command_result without re"
                    );
                }
            }
            NodeMessage::StreamData(data) => {
                self.hub
                    .0
                    .resolve_stream_data(&self.node_id, self.generation, data);
            }
            NodeMessage::StreamEnd(end) => {
                self.hub
                    .0
                    .resolve_stream_end(&self.node_id, self.generation, end);
            }
            NodeMessage::Error(error) => {
                tracing::warn!(
                    node_id = %self.node_id,
                    code = ?error.code,
                    message = %error.message,
                    "node reported an error"
                );
            }
            NodeMessage::Hello(_) => outbox.push(protocol_error(
                quoro_proto::ErrorCode::BadMessage,
                "hello was already received on this session",
                false,
                Some(&raw.id),
            )),
            // No change-log follow exists yet.
            NodeMessage::LeaseRenew(_) | NodeMessage::LogAck(_) => {}
            NodeMessage::DnsChallenge(challenge) => {
                let acme_routes: HashSet<String> = self
                    .last_spec
                    .as_ref()
                    .map(|spec| {
                        spec.routes
                            .iter()
                            .filter(|route| route.tls_mode == "acme")
                            .map(|route| route.hostname.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                self.start_dns_challenge(challenge, raw.id.clone(), acme_routes);
            }
            // Storing inventories happens in a later PR (section 5.26).
            NodeMessage::ImageInventory(inventory) => tracing::debug!(
                node_id = %self.node_id,
                image_digest = %inventory.image_digest,
                inventory_id = %inventory.inventory_id,
                part = inventory.part,
                parts = inventory.parts,
                "ignoring an image inventory part"
            ),
        }
        false
    }

    /// Revokes every lease `hello.leases` reports that this primary does not
    /// know, already revoked, holds on another node, or holds with a lower
    /// token than the node's (sections 5.1 and 4.2 rule 3).
    async fn revoke_stale_leases(&self, reported: &[wire::HelloLease], outbox: &mut Vec<String>) {
        if reported.is_empty() {
            return;
        }
        let mut conn = match self.hub.0.store.pool().acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read the leases of hello");
                return;
            }
        };
        for claimed in reported {
            let row = match leases::by_id(&mut conn, &claimed.lease_id).await {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(error = %e.code.as_str(), lease_id = %claimed.lease_id, "cannot read a lease");
                    continue;
                }
            };
            let reason = match &row {
                None => quoro_proto::RevokeReason::Moved,
                Some(lease) if lease.is_revoked() => quoro_proto::RevokeReason::Deleted,
                Some(lease) if lease.holder_node_id != self.node_id => {
                    quoro_proto::RevokeReason::Moved
                }
                Some(lease) if lease.fencing_token.max(1) as u64 > claimed.fencing_token.max(1) => {
                    quoro_proto::RevokeReason::Superseded
                }
                Some(_) => continue,
            };
            let token = match &row {
                Some(lease) => {
                    (lease.fencing_token.max(1) as u64).max(claimed.fencing_token.max(1))
                }
                None => claimed.fencing_token.max(1),
            };
            let revoke = quoro_proto::LeaseRevoke {
                lease_id: quoro_proto::LeaseId(claimed.lease_id.clone()),
                fencing_token: token,
                reason,
            };
            outbox.push(
                ControlMessage::LeaseRevoke(revoke)
                    .to_frame(&new_msg_id(), None)
                    .to_text(),
            );
        }
    }

    /// Sends a `lease_grant` for every unrevoked lease this node holds
    /// (section 5.12). A `local` lease carries no `duration_ms`.
    async fn send_grants(&self, outbox: &mut Vec<String>) {
        let mut conn = match self.hub.0.store.pool().acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read the leases of the node");
                return;
            }
        };
        match leases::held_by_node(&mut conn, &self.node_id).await {
            Ok(leases) => {
                for lease in leases {
                    outbox.push(
                        ControlMessage::LeaseGrant(lease.grant())
                            .to_frame(&new_msg_id(), None)
                            .to_text(),
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e.code.as_str(),
                    node_id = %self.node_id,
                    "cannot read the leases of the node"
                );
            }
        }
    }

    /// Answers one `dns_challenge` off the session loop (section 5.22). The
    /// result goes back through the session's outbox with `re` set to the
    /// challenge's id.
    fn start_dns_challenge(
        &self,
        challenge: wire::DnsChallenge,
        re: String,
        acme_routes: HashSet<String>,
    ) {
        let hub = self.hub.clone();
        let node_id = self.node_id.clone();
        let generation = self.generation;
        tokio::spawn(async move {
            let result = dns_challenge(&hub, &node_id, &challenge, &acme_routes).await;
            let text = ControlMessage::DnsChallengeResult(result)
                .to_frame(&new_msg_id(), Some(&re))
                .to_text();
            hub.0.send_text(&node_id, generation, text);
        });
    }

    /// Builds the snapshot, persists the new `rev` and queues the signed frame.
    /// `floor` is the node's stored revision on the first send (section 4.2),
    /// `0` afterwards.
    async fn refresh(&mut self, floor: u64, force: bool) -> Result<(), String> {
        let session_ip = |node_id: &str| self.hub.session_ip(node_id);
        let mesh_lookup = mesh::MeshLookup {
            session_ip: &session_ip,
            advertised_urls: self.hub.advertised_urls(),
        };
        let built = desired::build_spec(
            &self.hub.0.store,
            &self.node_id,
            self.hub.0.resolver.as_ref(),
            &self.hub.0.digests,
            &self.hub.0.data_key,
            self.last_spec.as_ref(),
            &mesh_lookup,
        )
        .await?;
        self.note_holdbacks(&built.held_back).await;
        let spec = built.spec;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(());
        }
        self.hub.remember_vms(&self.node_id, &spec);
        if !force && self.last_spec.as_ref() == Some(&spec) {
            return Ok(());
        }

        let stored: i64 = sqlx::query_scalar("SELECT desired_rev FROM nodes WHERE id = ?")
            .bind(&self.node_id)
            .fetch_one(self.hub.0.store.pool())
            .await
            .map_err(|e| format!("cannot read nodes.desired_rev: {e}"))?;
        let rev = u64::try_from(stored.max(0))
            .unwrap_or(0)
            .max(self.desired_rev)
            .max(floor)
            + 1;
        persist_desired_rev(&self.hub, &self.node_id, rev).await?;

        let state = wire::DesiredState {
            rev,
            issued_at: now_rfc3339(),
            term: cluster_term(&self.hub).await?,
            spec: spec.clone(),
            signature: String::new(),
        };
        let mut body = state.to_unsigned_value();
        quoro_proto::sign::sign_body(self.hub.0.keys.signing_key(), &mut body)
            .map_err(|e| format!("cannot sign the desired state: {e}"))?;
        let text = ControlMessage::DesiredState(body)
            .to_frame(&new_msg_id(), None)
            .to_text();
        if !self.hub.send(&self.node_id, self.generation, text) {
            // A newer session took over while this one was building; it will
            // send its own snapshot and must not receive this one.
            return Ok(());
        }
        let app_vms: Vec<(String, String)> = spec
            .vms
            .iter()
            .filter(|vm| vm.app_id.is_some())
            .map(|vm| (vm.vm_id.clone(), vm.image_digest.clone()))
            .collect();
        if !app_vms.is_empty()
            && let Err(e) = deploy::on_spec_sent(&self.hub.0.store, &self.node_id, &app_vms).await
        {
            tracing::warn!(
                node_id = %self.node_id,
                code = %e.code.as_str(),
                "cannot record the sent snapshot"
            );
        }
        self.desired_rev = rev;
        self.last_spec = Some(spec);
        self.hub.tunnel_changed(&self.node_id);
        Ok(())
    }

    /// Counts the builds in which each app VM's image did not resolve and
    /// fails the deployment after [`MAX_DIGEST_FAILURES`] in a row.
    async fn note_holdbacks(&mut self, held_back: &[desired::HeldBack]) {
        let current: HashSet<&str> = held_back.iter().map(|held| held.vm_id.as_str()).collect();
        self.digest_failures
            .retain(|vm_id, _| current.contains(vm_id.as_str()));
        for held in held_back {
            if held.app_id.is_none() {
                continue;
            }
            let count = self.digest_failures.entry(held.vm_id.clone()).or_insert(0);
            *count += 1;
            if *count < MAX_DIGEST_FAILURES {
                continue;
            }
            match deploy::on_image_unresolvable(&self.hub.0.store, &held.vm_id, &held.error).await {
                Ok(vms) => {
                    for vm_id in vms {
                        self.hub.vm_changed(&vm_id);
                    }
                }
                Err(e) => tracing::warn!(
                    vm_id = %held.vm_id,
                    code = %e.code.as_str(),
                    "cannot fail the unresolvable deployment"
                ),
            }
            self.digest_failures.remove(&held.vm_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Database work
// ---------------------------------------------------------------------------

/// A failed `dns_challenge_result` (section 5.23).
fn failed_challenge(message: &str) -> wire::DnsChallengeResult {
    wire::DnsChallengeResult {
        ok: false,
        record_id: None,
        message: Some(message.to_string()),
    }
}

/// Handles one `dns_challenge` (section 5.22): the node names a hostname it
/// serves as an `acme` route, and the primary writes or removes the
/// `_acme-challenge.<hostname>` TXT record with the domain's `dns` provider.
async fn dns_challenge(
    hub: &NodeHub,
    node_id: &str,
    challenge: &wire::DnsChallenge,
    acme_routes: &HashSet<String>,
) -> wire::DnsChallengeResult {
    match challenge.action {
        wire::DnsChallengeAction::Set => {
            if !acme_routes.contains(&challenge.hostname) {
                return failed_challenge("the hostname is not an acme route of this node");
            }
            // A DNS-01 value is a base64url SHA-256 digest (RFC 8555 section
            // 8.4): 43 characters, nothing else may be written into the zone.
            let Some(value) = challenge.value.as_deref().filter(|value| {
                value.len() == 43
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            }) else {
                return failed_challenge("set needs a DNS-01 digest value");
            };
            let provider_id: Option<String> = match sqlx::query_scalar(
                "SELECT provider_id FROM domains WHERE hostname = ?",
            )
            .bind(&challenge.hostname)
            .fetch_optional(hub.0.store.pool())
            .await
            {
                Ok(provider_id) => provider_id,
                Err(e) => {
                    tracing::warn!(node_id, error = %e, "cannot read the domain of a dns challenge");
                    return failed_challenge("the domain could not be read");
                }
            };
            let Some(provider_id) = provider_id else {
                return failed_challenge("the hostname has no domain provider");
            };
            let dns = match dns_client(hub, &provider_id).await {
                Ok(dns) => dns,
                Err(message) => return failed_challenge(&message),
            };
            let name = format!("_acme-challenge.{}", challenge.hostname);
            match dns.create_txt(&name, value).await {
                Ok(record_id) => {
                    hub.remember_dns_record(
                        node_id,
                        &record_id.0,
                        DnsRecord {
                            hostname: challenge.hostname.clone(),
                            provider_id,
                        },
                    );
                    wire::DnsChallengeResult {
                        ok: true,
                        record_id: Some(record_id.0),
                        message: None,
                    }
                }
                Err(e) => {
                    tracing::warn!(node_id, hostname = %challenge.hostname, error = %e, "cannot write a dns-01 record");
                    failed_challenge("the provider refused the record")
                }
            }
        }
        wire::DnsChallengeAction::Clear => {
            let Some(record_id) = challenge.record_id.as_deref() else {
                return failed_challenge("clear needs a record_id");
            };
            let Some(record) = hub.dns_record(node_id, record_id) else {
                return failed_challenge(
                    "the record was not created by this primary for this node",
                );
            };
            let dns = match dns_client(hub, &record.provider_id).await {
                Ok(dns) => dns,
                Err(message) => return failed_challenge(&message),
            };
            match dns
                .delete_record(&quoro_providers::RecordId(record_id.to_string()))
                .await
            {
                Ok(()) => {
                    hub.take_dns_record(node_id, record_id);
                    wire::DnsChallengeResult {
                        ok: true,
                        record_id: None,
                        message: None,
                    }
                }
                Err(e) => {
                    tracing::warn!(node_id, hostname = %record.hostname, error = %e, "cannot remove a dns-01 record");
                    failed_challenge("the provider refused the deletion")
                }
            }
        }
    }
}

/// The DNS client of the domain's provider. Messages never carry a credential.
async fn dns_client(
    hub: &NodeHub,
    provider_id: &str,
) -> Result<quoro_providers::cloudflare::CloudflareDns, String> {
    let (record, secret) =
        crate::api::providers::provider_secret_in(&hub.0.store, &hub.0.data_key, provider_id)
            .await
            .map_err(|e| {
                if e.code == crate::error::Code::NotFound {
                    "the domain's provider is gone".to_string()
                } else {
                    "the domain's provider could not be read".to_string()
                }
            })?;
    if record.kind != "dns" {
        return Err("the domain's provider is not a dns provider".to_string());
    }
    let zone_id = record
        .config
        .get("zone_id")
        .and_then(Value::as_str)
        .filter(|zone| !zone.is_empty())
        .ok_or_else(|| "the domain's provider has no zone_id".to_string())?;
    let endpoints = hub.cloudflare_endpoints();
    let cf = quoro_providers::cloudflare::Cloudflare::with_endpoints(
        quoro_providers::Token::new(secret.expose().to_string()),
        &endpoints.api_base,
        &endpoints.doh_base,
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "cannot build the cloudflare client");
        "the provider client could not be built".to_string()
    })?;
    Ok(cf.dns(zone_id))
}

fn i64_of(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Records the start of a session and emits `node.connected` (section 5.1).
///
/// It also stores the mesh public key the `hello` carried and assigns the
/// node's mesh address when it has none (ADR 0144). The `bool` says
/// whether either was newly stored, so the caller can rebuild every snapshot.
async fn connect_node(hub: &NodeHub, node_id: &str, hello: &wire::Hello) -> Result<bool, String> {
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    let capacity = serde_json::to_string(&hello.capacity).map_err(|e| e.to_string())?;
    let kvm = serde_json::to_string(&hello.kvm).map_err(|e| e.to_string())?;
    let volume_snapshot = hello.volume_snapshot.map(|method| match method {
        wire::SnapshotMethod::Reflink => "reflink",
        wire::SnapshotMethod::PauseCopy => "pause_copy",
    });
    let now = now_rfc3339();
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    sqlx::query(
        "UPDATE nodes SET status = 'connected', agent_version = ?, engine_version = ?, \
         capacity = ?, kvm = ?, volume_snapshot = ?, applied_rev = ?, last_seen_at = ? WHERE id = ?",
    )
    .bind(&hello.agent_version)
    .bind(&hello.engine_version)
    .bind(&capacity)
    .bind(&kvm)
    .bind(volume_snapshot)
    .bind(i64_of(hello.applied_rev))
    .bind(&now)
    .bind(node_id)
    .execute(&mut *w.tx)
    .await
    .map_err(|e| e.to_string())?;
    let mesh_changed =
        mesh::store_hello(&mut w.tx, &actor, node_id, hello.mesh_public_key.as_deref()).await?;
    let payload = driver::node_payload(&mut w.tx, node_id)
        .await
        .map_err(|e| format!("{e:?}"))?;
    changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload)
        .await
        .map_err(|e| e.to_string())?;
    events::emit(
        &mut w.tx,
        events::NewEvent {
            kind: "node.connected",
            source: events::Source::ControlPlane,
            subject_type: "node",
            subject_id: node_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({
                "node_id": node_id,
                "agent_version": hello.agent_version,
                "log_seq": i64_of(hello.log_seq),
            }),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    w.commit().await.map_err(|e| e.to_string())?;
    Ok(mesh_changed)
}

/// Records the end of a session and emits `node.disconnected`.
async fn disconnect_node(hub: &NodeHub, node_id: &str, reason: &str) -> Result<(), String> {
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    let now = now_rfc3339();
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    let updated = sqlx::query(
        "UPDATE nodes SET status = 'disconnected', last_seen_at = ? WHERE id = ? AND status = 'connected'",
    )
    .bind(&now)
    .bind(node_id)
    .execute(&mut *w.tx)
    .await
    .map_err(|e| e.to_string())?
    .rows_affected();
    if updated == 0 {
        return Ok(());
    }
    let payload = driver::node_payload(&mut w.tx, node_id)
        .await
        .map_err(|e| format!("{e:?}"))?;
    changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload)
        .await
        .map_err(|e| e.to_string())?;
    events::emit(
        &mut w.tx,
        events::NewEvent {
            kind: "node.disconnected",
            source: events::Source::ControlPlane,
            subject_type: "node",
            subject_id: node_id,
            project_id: None,
            request_id: None,
            actor: None,
            data: json!({ "node_id": node_id, "last_seen_at": now, "reason": reason }),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    w.commit().await.map_err(|e| e.to_string())
}

/// Persists a new `nodes.desired_rev` with its change-log entry, before the
/// snapshot is sent (section 5.11).
async fn persist_desired_rev(hub: &NodeHub, node_id: &str, rev: u64) -> Result<(), String> {
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    sqlx::query("UPDATE nodes SET desired_rev = ? WHERE id = ?")
        .bind(i64_of(rev))
        .bind(node_id)
        .execute(&mut *w.tx)
        .await
        .map_err(|e| e.to_string())?;
    let payload = driver::node_payload(&mut w.tx, node_id)
        .await
        .map_err(|e| format!("{e:?}"))?;
    changelog::append(
        &mut w.tx,
        &Actor::system("session"),
        "node",
        node_id,
        Op::Update,
        payload,
    )
    .await
    .map_err(|e| e.to_string())?;
    w.commit().await.map_err(|e| e.to_string())
}

/// Updates `nodes.applied_rev` when the node reports a new revision.
async fn set_applied_rev(hub: &NodeHub, node_id: &str, rev: u64) -> Result<(), String> {
    let current: i64 = sqlx::query_scalar("SELECT applied_rev FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_one(hub.0.store.pool())
        .await
        .map_err(|e| format!("cannot read nodes.applied_rev: {e}"))?;
    let rev = i64_of(rev);
    if current == rev {
        return Ok(());
    }
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    sqlx::query("UPDATE nodes SET applied_rev = ? WHERE id = ?")
        .bind(rev)
        .bind(node_id)
        .execute(&mut *w.tx)
        .await
        .map_err(|e| e.to_string())?;
    let payload = driver::node_payload(&mut w.tx, node_id)
        .await
        .map_err(|e| format!("{e:?}"))?;
    changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload)
        .await
        .map_err(|e| e.to_string())?;
    w.commit().await.map_err(|e| e.to_string())
}

/// The fields of a VM row [`apply_status_report`] reads before it applies an
/// observation: its node, app, address, observed status and the app's replica
/// count, so a change can refresh the VM's node and the edge.
struct ObservedVm {
    node_id: String,
    app_id: Option<String>,
    ip: Option<String>,
    observed_status: String,
    replicas: Option<i64>,
}

/// Applies a `status_report` (section 5.3): observed VM state goes through the
/// same [`driver`] functions the in-process driver used, so the `vm.*` events
/// stay in one place; reported builds, volumes and route certificates are
/// applied here. VMs of other nodes are ignored.
async fn apply_status_report(
    hub: &NodeHub,
    node_id: &str,
    report: &wire::StatusReport,
    notified_deleted: &mut HashSet<String>,
) -> Result<(), String> {
    for vm in &report.vms {
        let row = sqlx::query(
            "SELECT node_id, app_id, ip, observed_status, \
                    (SELECT replicas FROM apps WHERE apps.id = vms.app_id) AS replicas \
             FROM vms WHERE id = ?",
        )
        .bind(&vm.vm_id)
        .fetch_optional(hub.0.store.pool())
        .await
        .map_err(|e| format!("cannot read vms.node_id: {e}"))?;
        let owner = match row {
            Some(row) => Some(ObservedVm {
                node_id: row.try_get("node_id").map_err(|e| e.to_string())?,
                app_id: row.try_get("app_id").map_err(|e| e.to_string())?,
                ip: row.try_get("ip").map_err(|e| e.to_string())?,
                observed_status: row.try_get("observed_status").map_err(|e| e.to_string())?,
                replicas: row.try_get("replicas").map_err(|e| e.to_string())?,
            }),
            None => None,
        };
        match owner {
            Some(owner) if owner.node_id == node_id => {
                let observation = driver::Observation {
                    status: driver_observed(vm.observed_status),
                    ip: vm.ip.clone(),
                    exit_code: vm.last_exit_code,
                    restart_count: i64::try_from(vm.restart_count).ok(),
                };
                driver::report_observed(&hub.0.store, node_id, &vm.vm_id, &observation)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                // `spec.edge` lists the backend's address, and a node's
                // `spec.mesh.allow` its address and port: a change to either
                // refreshes both (ADR 0143, ADR 0144).
                let ip_changed = vm
                    .ip
                    .as_deref()
                    .is_some_and(|ip| owner.ip.as_deref() != Some(ip));
                let status_changed =
                    driver_observed(vm.observed_status).as_str() != owner.observed_status.as_str();
                if ip_changed || status_changed {
                    if owner.replicas.is_some_and(|replicas| replicas > 1) {
                        hub.node_changed(node_id);
                    }
                    hub.edge_changed();
                }
                if owner.app_id.is_some() {
                    match deploy::on_vm_observed(&hub.0.store, node_id, vm).await {
                        Ok(vms) => {
                            for changed in vms {
                                hub.vm_changed(&changed);
                            }
                        }
                        Err(e) => tracing::warn!(
                            vm_id = %vm.vm_id,
                            code = %e.code.as_str(),
                            "cannot apply the observed VM to its deployment"
                        ),
                    }
                }
            }
            Some(_) => {}
            None => {
                if vm.observed_status == wire::ObservedStatus::Deleted
                    && notified_deleted.insert(vm.vm_id.clone())
                {
                    driver::report_deleted(&hub.0.store, node_id, &vm.vm_id)
                        .await
                        .map_err(|e| format!("{e:?}"))?;
                }
            }
        }
    }
    if let Err(e) = apply_edge_report(hub, node_id, &report.edge_backends).await {
        tracing::warn!(%e, node_id, "cannot apply the edge's backend report");
    }
    for build in &report.builds {
        match crate::build::apply_build_status(&hub.0.store, node_id, build).await {
            Ok(true) => hub.node_changed(node_id),
            Ok(false) => {}
            Err(e) => tracing::warn!(
                build_id = %build.build_id,
                code = %e.code.as_str(),
                "cannot apply the reported build status"
            ),
        }
    }
    // `disk_free_gb` is what `createVolume` checks a size against; it is the
    // node's last report, so it lives on the hub rather than in a table.
    hub.0
        .disk_free_gb
        .lock()
        .expect("disk free lock")
        .insert(node_id.to_string(), report.disk_free_gb);
    match crate::backups::restore::apply_status(&hub.0.store, node_id, &report.volumes).await {
        Ok(applied) => {
            // The grant reaches the node before the snapshot that starts the
            // app there (section 8.3).
            for lease in &applied.grants {
                hub.send_control(
                    node_id,
                    &quoro_proto::wire::ControlMessage::LeaseGrant(lease.grant()),
                );
            }
            if applied.desired_changed {
                hub.node_changed(node_id);
            }
        }
        Err(e) => tracing::warn!(
            node_id,
            code = %e.code.as_str(),
            "cannot apply the reported restore statuses"
        ),
    }
    match crate::service::volumes::apply_status(&hub.0.store, node_id, &report.volumes).await {
        Ok(true) => hub.node_changed(node_id),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            node_id,
            code = %e.code.as_str(),
            "cannot apply the reported volume statuses"
        ),
    }
    match crate::backups::apply_status(
        &hub.0.store,
        &hub.0.data_key,
        &hub.0.keys,
        &hub.0.cluster_id,
        &hub.0.backups,
        node_id,
        &report.backups,
    )
    .await
    {
        Ok(true) => hub.node_changed(node_id),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            node_id,
            code = %e.code.as_str(),
            "cannot apply the reported backup statuses"
        ),
    }
    set_applied_rev(hub, node_id, report.applied_rev).await?;
    apply_route_statuses(hub, node_id, &report.routes).await
}

/// Keeps the newest `edge_backends` of an edge machine in memory and turns a
/// change of an app's rotation state into `app.degraded`, `app.down` or
/// `app.up` (section 5.3, ADR 0143). Nothing is written to the
/// database except the events; a machine without the `edge` role is ignored.
async fn apply_edge_report(
    hub: &NodeHub,
    node_id: &str,
    report: &[wire::EdgeBackendStatus],
) -> Result<(), String> {
    if !has_edge_role(hub, node_id).await? {
        return Ok(());
    }
    {
        let mut reports = hub.0.edge_reports.lock().expect("edge report lock");
        if report.is_empty() {
            reports.remove(node_id);
        } else {
            reports.insert(node_id.to_string(), report.to_vec());
        }
    }
    if report.is_empty() {
        return Ok(());
    }

    let mut app_ids: Vec<String> = report.iter().map(|entry| entry.app_id.clone()).collect();
    app_ids.sort_unstable();
    app_ids.dedup();
    let mut apps = BTreeMap::new();
    for app_id in app_ids {
        let row: Option<(String, i64)> =
            sqlx::query_as("SELECT project_id, replicas FROM apps WHERE id = ?")
                .bind(&app_id)
                .fetch_optional(hub.0.store.pool())
                .await
                .map_err(|e| format!("cannot read the app of an edge backend: {e}"))?;
        if let Some((project_id, replicas)) = row {
            apps.insert(
                app_id,
                crate::edge_health::AppFacts {
                    project_id,
                    replicas,
                },
            );
        }
    }
    let events = {
        let mut state = hub.0.edge_health.lock().expect("edge health lock");
        crate::edge_health::apply(&mut state, node_id, report, &apps)
    };
    if events.is_empty() {
        return Ok(());
    }

    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    for event in events {
        let Some(facts) = apps.get(&event.app_id) else {
            continue;
        };
        let id = events::emit(
            &mut w.tx,
            events::NewEvent {
                kind: event.kind,
                source: events::Source::ControlPlane,
                subject_type: "app",
                subject_id: &event.app_id,
                project_id: Some(&facts.project_id),
                request_id: None,
                actor: None,
                data: event.data.clone(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        // `app.down` and `app.up` move the app's incident exactly as a node's
        // detection does; `app.degraded` opens none (ADR 0143).
        if event.kind != "app.degraded" {
            let detection = wire::EventBody {
                schema_version: 1,
                id,
                kind: event.kind.to_string(),
                time: now_rfc3339(),
                source: json!({ "kind": "control_plane" }),
                subject: json!({ "type": "app", "id": event.app_id }),
                project_id: Some(facts.project_id.clone()),
                incident_id: None,
                request_id: None,
                actor: None,
                data: event.data,
            };
            crate::service::incidents::apply_node_event(&mut w.tx, &detection)
                .await
                .map_err(|e| format!("{e:?}"))?;
        }
    }
    w.commit().await.map_err(|e| e.to_string())
}

/// Whether `node_id` is a non-removed machine with the `edge` role.
async fn has_edge_role(hub: &NodeHub, node_id: &str) -> Result<bool, String> {
    let roles: Option<String> =
        sqlx::query_scalar("SELECT roles FROM nodes WHERE id = ? AND status <> 'removed'")
            .bind(node_id)
            .fetch_optional(hub.0.store.pool())
            .await
            .map_err(|e| format!("cannot read the roles of {node_id}: {e}"))?;
    Ok(roles
        .and_then(|roles| serde_json::from_str::<Vec<String>>(&roles).ok())
        .is_some_and(|roles| roles.iter().any(|role| role == "edge")))
}

/// The certificate state a node reported for the hostnames it serves
/// (`status_report.routes[]`, section 5.3), written to `domains` when it
/// changed.
async fn apply_route_statuses(
    hub: &NodeHub,
    node_id: &str,
    routes: &[wire::RouteStatus],
) -> Result<(), String> {
    if routes.is_empty() {
        return Ok(());
    }
    let mut changed = Vec::new();
    for route in routes {
        let Some(status) = crate::api::domains::CertStatus::parse(&route.cert_status) else {
            tracing::warn!(
                node_id,
                cert_status = %route.cert_status,
                "the node reported an unknown certificate status"
            );
            continue;
        };
        let row = sqlx::query(&format!(
            "SELECT {} FROM domains d WHERE d.hostname = ?1 \
             AND EXISTS (SELECT 1 FROM vms v WHERE v.app_id = d.app_id AND v.node_id = ?2)",
            crate::api::domains::DOMAIN_COLUMNS
        ))
        .bind(&route.hostname)
        .bind(node_id)
        .fetch_optional(hub.0.store.pool())
        .await
        .map_err(|e| format!("cannot read the domain of a route status: {e}"))?;
        let Some(row) = row else {
            continue;
        };
        let mut domain =
            crate::api::domains::domain_from_row(&row).map_err(|e| format!("{e:?}"))?;
        if domain.cert_status == status && domain.cert_expires_at == route.cert_expires_at {
            continue;
        }
        domain.cert_status = status;
        domain.cert_expires_at.clone_from(&route.cert_expires_at);
        changed.push(domain);
    }
    if changed.is_empty() {
        return Ok(());
    }
    let actor = Actor {
        kind: ActorType::Node,
        id: node_id.to_string(),
    };
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    for domain in &changed {
        sqlx::query("UPDATE domains SET cert_status = ?, cert_expires_at = ? WHERE id = ?")
            .bind(domain.cert_status.as_str())
            .bind(&domain.cert_expires_at)
            .bind(&domain.id)
            .execute(&mut *w.tx)
            .await
            .map_err(|e| e.to_string())?;
        let payload = serde_json::to_value(domain).map_err(|e| format!("domain payload: {e}"))?;
        changelog::append(&mut w.tx, &actor, "domain", &domain.id, Op::Update, payload)
            .await
            .map_err(|e| e.to_string())?;
    }
    w.commit().await.map_err(|e| e.to_string())
}

fn driver_observed(status: wire::ObservedStatus) -> DriverObservedStatus {
    match status {
        wire::ObservedStatus::Creating => DriverObservedStatus::Creating,
        wire::ObservedStatus::Stopped => DriverObservedStatus::Stopped,
        wire::ObservedStatus::Starting => DriverObservedStatus::Starting,
        wire::ObservedStatus::Running => DriverObservedStatus::Running,
        wire::ObservedStatus::Stopping => DriverObservedStatus::Stopping,
        wire::ObservedStatus::Crashed => DriverObservedStatus::Crashed,
        wire::ObservedStatus::Deleted => DriverObservedStatus::Deleted,
        wire::ObservedStatus::Unknown => DriverObservedStatus::Unknown,
    }
}

/// Stores a node event with the node's own `evt_` id, ignoring a repeat
/// (section 5.4). The event travels to replicas as a change-log entry.
async fn insert_node_event(
    hub: &NodeHub,
    node_id: &str,
    event: &wire::EventBody,
) -> Result<(), String> {
    let mut w = hub.0.store.begin_write().await.map_err(|e| e.to_string())?;
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM events WHERE id = ?")
        .bind(&event.id)
        .fetch_optional(&mut *w.tx)
        .await
        .map_err(|e| e.to_string())?;
    if exists.is_some() {
        return Ok(());
    }
    events::insert(
        &mut w.tx,
        StoredEvent {
            id: &event.id,
            time: &event.time,
            kind: &event.kind,
            source: event.source.clone(),
            subject: event.subject.clone(),
            project_id: event.project_id.as_deref(),
            incident_id: event.incident_id.as_deref(),
            request_id: event.request_id.as_deref(),
            actor: event.actor.clone(),
            data: event.data.clone(),
            change_actor: Actor {
                kind: ActorType::Node,
                id: node_id.to_string(),
            },
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    // A node report is a detection: `app.down` opens the incident for that app
    // and `app.up` resolves it, in the same transaction (D-0032,
    // permission-model.md section 8.2).
    crate::service::incidents::apply_node_event(&mut w.tx, event)
        .await
        .map_err(|e| format!("{e:?}"))?;
    w.commit().await.map_err(|e| e.to_string())
}

/// The current cluster term from `meta` (1 until elections exist).
async fn cluster_term(hub: &NodeHub) -> Result<u64, String> {
    let value: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = 'term'")
        .fetch_optional(hub.0.store.pool())
        .await
        .map_err(|e| format!("cannot read the cluster term: {e}"))?;
    Ok(value.and_then(|(value,)| value.parse().ok()).unwrap_or(1))
}

/// The node that holds the primary status, or `node_id` when nobody does.
async fn primary_node_id(hub: &NodeHub, node_id: &str) -> Result<String, String> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary' LIMIT 1")
            .fetch_optional(hub.0.store.pool())
            .await
            .map_err(|e| format!("cannot read the primary node: {e}"))?;
    Ok(row.map_or_else(|| node_id.to_string(), |(id,)| id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_statuses_map_one_to_one() {
        for (proto, driver) in [
            (
                wire::ObservedStatus::Creating,
                DriverObservedStatus::Creating,
            ),
            (wire::ObservedStatus::Stopped, DriverObservedStatus::Stopped),
            (
                wire::ObservedStatus::Starting,
                DriverObservedStatus::Starting,
            ),
            (wire::ObservedStatus::Running, DriverObservedStatus::Running),
            (
                wire::ObservedStatus::Stopping,
                DriverObservedStatus::Stopping,
            ),
            (wire::ObservedStatus::Crashed, DriverObservedStatus::Crashed),
            (wire::ObservedStatus::Deleted, DriverObservedStatus::Deleted),
            (wire::ObservedStatus::Unknown, DriverObservedStatus::Unknown),
        ] {
            assert_eq!(driver_observed(proto), driver);
        }
    }

    #[test]
    fn a_session_holds_at_most_32_streams() {
        let mut pending = Pending::default();
        for index in 0..MAX_STREAMS {
            let (tx, _rx) = mpsc::unbounded_channel();
            pending.streams.insert(
                format!("msg_{index}"),
                StreamSlot {
                    tx,
                    last_seq: 0,
                    kind: "app_logs".to_string(),
                },
            );
        }
        assert!(pending.at_stream_limit());
        pending.streams.remove("msg_0");
        assert!(!pending.at_stream_limit());
    }
}
