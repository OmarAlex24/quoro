// SPDX-License-Identifier: AGPL-3.0-only
//! The voter's side of a promotion vote (`node-agent-protocol.md` section
//! 8.9, ADR 0182): the vote listener.
//!
//! Every voter's node agent listens on its own mesh address, TCP port
//! `spec.votes.port`, and on no other address. It answers only `POST
//! /v1/vote` with a JSON body; any other path is `404` and any other method
//! `405`. The source of a request is the TCP peer address of its connection,
//! never a header: WireGuard binds each peer's mesh address to its key, so a
//! request from a voter's mesh address comes from that voter (section 8.9).
//! A caller on this host is answered only when its socket is root's, because
//! section 7.3 runs a promotion as root ([`connection_caller`]). The mesh
//! firewall has the listener take TCP only over the mesh interface or
//! loopback, and over loopback only from the machine's own mesh address
//! ([`crate::mesh::nft_ruleset`]). The listener listens only while that
//! firewall is loaded for its address
//! ([`crate::mesh::MeshManager::firewall`]), and only while votes are open:
//! `spec.votes.listen` is `true` (sections 6.12 and 8.9).
//!
//! The listener belongs to the agent process, not to a session: it answers
//! with no session up, while a promotion on this host freezes the agent
//! (section 7.3 "The freeze" does not depend on the agent's own vote), and on
//! a witness-only machine. It follows the newest verified desired state the
//! agent publishes to its other loops: it starts once that state carries
//! `spec.votes` with `listen: true` and a mesh address and the firewall for
//! them is loaded, binds again when the address or the port changes, stops
//! when `spec.votes` or its `listen` goes away or the firewall is not loaded,
//! and retries a bind that failed, such as one on a mesh address that is not
//! up yet. It reports the socket it holds to the mesh manager
//! ([`crate::mesh::ListenerLink`]), which waits for it to close before it
//! configures the peers of a state in which votes are not open (section
//! 8.9). A request on a connection accepted before votes closed is answered
//! `503` and decides nothing. Whether votes are open does not touch
//! `support_request` (section 5.27): the session answers it either way.
//!
//! Each request reads the newest `spec.votes`, decides and writes in one
//! transaction ([`crate::vote::answer_vote`]): a granted vote is durable
//! before its answer goes out. A voter that cannot tell whether it may grant
//! answers `503` with `{"error": ...}`, never `granted`. A request for a
//! term above [`MAX_TERM`] is answered `400` and decides nothing.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::header::{ALLOW, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use quoro_proto::wire::{SpecVotes, VOTE_PATH, VoteRequest};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::lease::HostClock;
use crate::mesh::ListenerLink;
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
use crate::util;
use crate::vote::{
    Received, VoteDecision, answer_vote, current_term, may_ask, mesh_ip, not_allowed,
};

/// The longest request body read, in bytes. A vote request is a few hundred
/// bytes.
pub const BODY_LIMIT: usize = 4 << 10;
/// Connections served at once, as the warm receiver's listener.
pub const MAX_CONNECTIONS: usize = 16;
/// The highest `term` a vote request may ask for: 2^53 - 1, the highest
/// integer the change log and RFC 8785 carry exactly. A request above it is
/// answered `400`, and nothing is decided or written.
pub const MAX_TERM: u64 = (1 << 53) - 1;
/// The longest error text of a `503` or `4xx` answer.
const ERROR_CHARS: usize = 300;
/// How often a bind that keeps failing is logged again at `warn`.
const BIND_WARN_EVERY: Duration = Duration::from_secs(60);
/// How often a repeated `warn` of the listener is logged again: a vote
/// request from this machine that is not root's, a caller it cannot tell
/// about, a vote it could not answer, and a failing accept.
const LOCAL_WARN_EVERY: Duration = Duration::from_secs(60);
/// Where the kernel lists this machine's TCP sockets.
const PROC_NET: &str = "/proc/net";

/// Timings; tests shorten them.
#[derive(Clone, Debug)]
pub struct VoteListenerOptions {
    /// How long the request head, and then its body, may take to arrive.
    pub read_timeout: Duration,
    /// How often a listener that could not bind is tried again.
    pub bind_retry: Duration,
    /// Where the kernel's TCP socket tables are read: `/proc/net`. Tests
    /// point it at tables of their own.
    pub proc_net: PathBuf,
}

impl Default for VoteListenerOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(5),
            bind_retry: Duration::from_secs(5),
            proc_net: PathBuf::from(PROC_NET),
        }
    }
}

impl VoteListenerOptions {
    /// Short timings for tests.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            read_timeout: Duration::from_millis(500),
            bind_retry: Duration::from_millis(50),
            ..Self::default()
        }
    }
}

/// The vote listener of section 8.9.
#[derive(Clone)]
pub struct VoteListener {
    inner: Arc<Inner>,
}

struct Inner {
    store: NodeStore,
    node_id: String,
    cluster_id: String,
    clock: Arc<dyn HostClock>,
    desired: watch::Receiver<Option<Snapshot>>,
    /// The listener address the mesh manager allows
    /// ([`crate::mesh::MeshManager::firewall`]).
    firewall: watch::Receiver<Option<SocketAddr>>,
    /// Where the listener reports the address it holds a socket on, or is
    /// about to bind ([`ListenerLink::bound`]).
    bound: Arc<watch::Sender<Option<SocketAddr>>>,
    options: VoteListenerOptions,
    connections: Arc<Semaphore>,
    /// The `warn` for a vote request from this machine that is not root's.
    local_warn: Mutex<WarnLimit>,
    /// The `warn` for a caller the listener cannot tell about (`503`).
    cannot_tell_warn: Mutex<WarnLimit>,
    /// The `warn` for a vote the listener could not answer (`503`).
    unavailable_warn: Mutex<WarnLimit>,
    /// The `warn` for a failing accept.
    accept_warn: Mutex<WarnLimit>,
}

impl VoteListener {
    /// A listener for the machine `node_id` of cluster `cluster_id`, which
    /// follows `desired`, the newest verified desired state, listens only on
    /// the address the mesh manager allows through `link` and reports its
    /// socket back through it, and reads the time a request arrived from
    /// `clock`.
    #[must_use]
    pub fn new(
        store: NodeStore,
        node_id: String,
        cluster_id: String,
        clock: Arc<dyn HostClock>,
        desired: watch::Receiver<Option<Snapshot>>,
        link: ListenerLink,
        options: VoteListenerOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                node_id,
                cluster_id,
                clock,
                desired,
                firewall: link.firewall,
                bound: link.bound,
                options,
                connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
                local_warn: Mutex::new(WarnLimit::default()),
                cannot_tell_warn: Mutex::new(WarnLimit::default()),
                unavailable_warn: Mutex::new(WarnLimit::default()),
                accept_warn: Mutex::new(WarnLimit::default()),
            }),
        }
    }

    /// Keeps the listener bound to the address the newest desired state
    /// names ([`listen_address`]) until `stop` resolves, while the mesh
    /// firewall loaded for that address stands: the listener fails closed,
    /// and stops listening when an apply of the mesh fails or the mesh is
    /// removed, until the next successful apply.
    pub async fn run(self, stop: impl Future<Output = ()> + Send) {
        let mut stop = std::pin::pin!(stop);
        let mut desired = self.inner.desired.clone();
        let mut firewall = self.inner.firewall.clone();
        let mut tick = tokio::time::interval(self.inner.options.bind_retry);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut serving: Option<(SocketAddr, JoinHandle<()>)> = None;
        let mut failing: Option<BindFailure> = None;
        loop {
            let covered = *firewall.borrow_and_update();
            let wanted = listen_address(desired.borrow_and_update().as_ref())
                .filter(|address| covered == Some(*address));
            self.pass(wanted, &mut serving, &mut failing).await;
            tokio::select! {
                () = &mut stop => break,
                changed = desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = firewall.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = tick.tick() => {}
            }
        }
        if let Some((_, task)) = serving {
            task.abort();
            let _ = task.await;
        }
        self.report(None);
        info!("vote listener stopped");
    }

    /// Reports the address the listener holds a socket on, or is about to
    /// bind, to the mesh manager ([`ListenerLink::bound`]).
    fn report(&self, address: Option<SocketAddr>) {
        self.inner.bound.send_if_modified(|current| {
            let changed = *current != address;
            *current = address;
            changed
        });
    }

    /// One pass: closes a listener on an address no longer wanted, and binds
    /// the wanted one when it has none. `failing` is the bind that has been
    /// failing, kept across passes so that it is logged at `warn` first and
    /// then every [`BIND_WARN_EVERY`] while it lasts.
    async fn pass(
        &self,
        wanted: Option<SocketAddr>,
        serving: &mut Option<(SocketAddr, JoinHandle<()>)>,
        failing: &mut Option<BindFailure>,
    ) {
        if failing
            .as_ref()
            .is_some_and(|failure| Some(failure.addr) != wanted)
        {
            *failing = None;
        }
        if serving
            .as_ref()
            .is_some_and(|(addr, task)| Some(*addr) != wanted || task.is_finished())
            && let Some((addr, task)) = serving.take()
        {
            // The listening socket closes once the aborted task is dropped:
            // awaited, so the report below never runs ahead of it.
            task.abort();
            let _ = task.await;
            info!(%addr, "vote listener closed");
        }
        if serving.is_none() {
            self.report(None);
        }
        let Some(addr) = wanted else {
            return;
        };
        if serving.is_some() {
            return;
        }
        // Reported before the bind, and the manager's address read again
        // after: a manager that took the address away either sees this
        // report and waits for the socket to close, or has already taken it
        // away, and then nothing is bound ([`crate::mesh::ListenerLink`]).
        self.report(Some(addr));
        if *self.inner.firewall.borrow() != Some(addr) {
            self.report(None);
            return;
        }
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Some(failure) = failing.take() {
                    info!(
                        %addr,
                        failed_for_s = failure.since.elapsed().as_secs(),
                        "vote listener bound after its bind failed"
                    );
                }
                info!(%addr, "vote listener ready");
                let listener_self = self.clone();
                *serving = Some((
                    addr,
                    tokio::spawn(async move { listener_self.serve(listener).await }),
                ));
            }
            Err(e) => {
                self.report(None);
                // The mesh interface may not have its address yet; the next
                // pass tries again. A bind that keeps failing must be seen.
                let now = Instant::now();
                match failing {
                    Some(failure) if now.duration_since(failure.warned) < BIND_WARN_EVERY => {
                        debug!(%addr, error = %e, "cannot listen for votes yet");
                    }
                    Some(failure) => {
                        failure.warned = now;
                        warn!(
                            %addr,
                            error = %e,
                            failing_for_s = now.duration_since(failure.since).as_secs(),
                            "still cannot listen for votes; retrying"
                        );
                    }
                    None => {
                        *failing = Some(BindFailure {
                            addr,
                            since: now,
                            warned: now,
                        });
                        warn!(%addr, error = %e, "cannot listen for votes; retrying");
                    }
                }
            }
        }
    }

    /// Serves votes on `listener` until the task is aborted. The agent's own
    /// listener comes from [`VoteListener::run`]; a test passes one on a
    /// loopback address.
    pub async fn serve(&self, listener: TcpListener) {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    let admitted = self
                        .inner
                        .accept_warn
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .admit(Instant::now());
                    match admitted {
                        Some(suppressed) => {
                            warn!(error = %e, suppressed, "could not accept a vote connection");
                        }
                        None => debug!(error = %e, "could not accept a vote connection"),
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let local = stream.local_addr();
            let Ok(permit) = Arc::clone(&self.inner.connections).try_acquire_owned() else {
                debug!(%remote, "too many vote connections; closing one");
                continue;
            };
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let _permit = permit;
                // Decided once per connection, before any request on it is
                // read and before the store is.
                let caller = connection_caller(&inner.options.proc_net, remote, local).await;
                let timeout = inner.options.read_timeout;
                let service = hyper::service::service_fn(move |request| {
                    let inner = Arc::clone(&inner);
                    let caller = caller.clone();
                    async move {
                        // Its own task: hyper drops the service future when
                        // the candidate closes the connection, and a vote is
                        // decided and written whole or not at all.
                        let task = tokio::spawn(async move { inner.handle(request, caller).await });
                        Ok::<_, Infallible>(task.await.unwrap_or_else(|e| {
                            warn!(%remote, error = %e, "a vote's task failed");
                            error(StatusCode::SERVICE_UNAVAILABLE, "the vote's task failed")
                        }))
                    }
                });
                let mut builder = hyper::server::conn::http1::Builder::new();
                builder
                    .timer(TokioTimer::new())
                    .header_read_timeout(timeout)
                    .keep_alive(false);
                if let Err(e) = builder
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    debug!(%remote, error = %e, "vote connection ended");
                }
            });
        }
    }
}

/// Who a vote connection comes from, decided once per connection
/// ([`connection_caller`]).
#[derive(Clone, Debug)]
enum Caller {
    /// Taken as a source: the TCP peer address.
    Peer(IpAddr),
    /// A process on this machine that is not root, with its uid: every
    /// request is answered `not_allowed`.
    LocalNotRoot(u32),
    /// A caller the listener cannot tell about, with why: one on this
    /// machine whose owner could not be found, or a connection whose own
    /// address could not be read. Every request is answered `503`, never
    /// `granted` nor `not_allowed` (section 8.9).
    CannotTell(String),
}

/// Lets a repeated `warn` through at most once per [`LOCAL_WARN_EVERY`],
/// counting the ones it holds back.
#[derive(Debug, Default)]
struct WarnLimit {
    /// When the last `warn` went out.
    last: Option<Instant>,
    /// The ones held back since.
    suppressed: u64,
}

impl WarnLimit {
    /// At `now`, `Some` with the number held back since the last `warn` when
    /// this one goes out, or `None` when it is held back.
    fn admit(&mut self, now: Instant) -> Option<u64> {
        match self.last {
            Some(last) if now.saturating_duration_since(last) < LOCAL_WARN_EVERY => {
                self.suppressed += 1;
                None
            }
            _ => {
                self.last = Some(now);
                Some(std::mem::take(&mut self.suppressed))
            }
        }
    }
}

/// The bind of the vote listener that has been failing.
struct BindFailure {
    addr: SocketAddr,
    /// The first failure.
    since: Instant,
    /// The last failure logged at `warn`.
    warned: Instant,
}

impl Inner {
    /// Answers one request from `caller` ([`connection_caller`]): one not
    /// taken as a source is answered `not_allowed`, and one the listener
    /// cannot tell about `503`.
    async fn handle(&self, request: Request<Incoming>, caller: Caller) -> Response<Full<Bytes>> {
        let remote = match &caller {
            Caller::Peer(ip) => ip.to_string(),
            Caller::LocalNotRoot(uid) => format!("this machine, uid {uid}"),
            Caller::CannotTell(_) => "a caller that cannot be told".to_owned(),
        };
        if request.uri().path() != VOTE_PATH {
            return error(StatusCode::NOT_FOUND, "not found");
        }
        if request.method() != Method::POST {
            let mut response = error(StatusCode::METHOD_NOT_ALLOWED, "only POST is answered");
            response
                .headers_mut()
                .insert(ALLOW, hyper::header::HeaderValue::from_static("POST"));
            return response;
        }
        let declared = request
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if declared.is_some_and(|length| length > BODY_LIMIT as u64) {
            return error(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large");
        }
        let body = Limited::new(request.into_body(), BODY_LIMIT);
        let bytes = match tokio::time::timeout(self.options.read_timeout, body.collect()).await {
            Ok(Ok(collected)) => collected.to_bytes(),
            Ok(Err(e)) if e.downcast_ref::<LengthLimitError>().is_some() => {
                return error(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large");
            }
            Ok(Err(e)) => {
                debug!(%remote, error = %e, "could not read a vote request");
                return error(StatusCode::BAD_REQUEST, "could not read the body");
            }
            Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "the body did not arrive"),
        };
        let vote: VoteRequest = match serde_json::from_slice(&bytes) {
            Ok(vote) => vote,
            Err(e) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    &format!("the body is not a vote request: {e}"),
                );
            }
        };
        // A JSON integer above `u64::MAX` already failed the parse above.
        if vote.term > MAX_TERM {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("the term is above {MAX_TERM}"),
            );
        }
        // Section 8.9: while votes are not open no vote is served. A
        // connection accepted before they closed reaches this point; its
        // request is answered `503`, which a candidate counts as no answer,
        // and decides nothing.
        if self.votes().is_some_and(|votes| !votes.open()) {
            return error(StatusCode::SERVICE_UNAVAILABLE, "votes are not open");
        }
        let source = match caller {
            Caller::Peer(ip) => Some(ip),
            Caller::LocalNotRoot(uid) => {
                self.warn_local_not_root(uid);
                None
            }
            Caller::CannotTell(e) => {
                // Its own limit: one `warn` a minute, however often it
                // connects.
                let admitted = self
                    .cannot_tell_warn
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .admit(Instant::now());
                match admitted {
                    Some(suppressed) => warn!(
                        error = %e,
                        suppressed,
                        "cannot tell who sent a vote request; answering 503"
                    ),
                    None => {
                        debug!(error = %e, "cannot tell who sent a vote request; answering 503")
                    }
                }
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cannot tell who sent the request",
                );
            }
        };
        let decision = match self.votes() {
            // A caller that cannot be a voter under the newest published
            // `spec.votes` is refused before the store's write lock is
            // taken. [`decide_vote`] refuses it first too, with the same
            // answer; the transaction below still checks everything.
            //
            // [`decide_vote`]: crate::vote::decide_vote
            Some(votes) if !may_ask(&votes, source) => match current_term(&self.store).await {
                Ok(highest) => {
                    // Any source can send these: `debug`, not `info`.
                    debug!(%remote, term = vote.term, "refused a vote from a caller that is not a voter");
                    return json(StatusCode::OK, &not_allowed(&self.node_id, highest, &votes));
                }
                Err(e) => VoteDecision::Unavailable(format!("could not read the vote state: {e}")),
            },
            _ => {
                // The clock read when the request arrived: before the
                // transaction waits for the write lock, so a wait it reports
                // is never shorter and a promise never reads as run out too
                // early. `spec.votes` is read inside the transaction instead
                // (section 8.10 rule 4).
                let received = Received::read(&*self.clock);
                answer_vote(
                    &self.store,
                    &vote,
                    &self.node_id,
                    &self.cluster_id,
                    // Read again inside the transaction, which answers a
                    // state that closed votes since the check above with
                    // `503` and decides nothing ([`answer_vote`]).
                    || self.votes(),
                    source,
                    &received,
                )
                .await
            }
        };
        match decision {
            // A refused caller on this machine was already logged, under the
            // limit above.
            VoteDecision::Answer(answer) if source.is_some() => {
                info!(
                    %remote,
                    term = vote.term,
                    candidate = %vote.candidate_node_id,
                    check = vote.is_check(),
                    vote = ?answer.vote,
                    reason = ?answer.reason,
                    "answered a promotion vote"
                );
                json(StatusCode::OK, &answer)
            }
            VoteDecision::Answer(answer) => json(StatusCode::OK, &answer),
            VoteDecision::Unavailable(reason) => {
                let admitted = self
                    .unavailable_warn
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .admit(Instant::now());
                match admitted {
                    Some(suppressed) => warn!(
                        %remote,
                        reason = %reason,
                        suppressed,
                        "could not answer a promotion vote"
                    ),
                    None => debug!(%remote, reason = %reason, "could not answer a promotion vote"),
                }
                error(StatusCode::SERVICE_UNAVAILABLE, &reason)
            }
        }
    }

    /// Logs a vote request from a process on this machine that is not root
    /// at `warn`, at most once per [`LOCAL_WARN_EVERY`], with how many were
    /// held back since; the others at `debug`.
    fn warn_local_not_root(&self, uid: u32) {
        let admitted = self
            .local_warn
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .admit(Instant::now());
        match admitted {
            Some(suppressed) => warn!(
                uid,
                suppressed,
                "a vote request from this machine that is not root's; answering not_allowed"
            ),
            None => debug!(
                uid,
                "a vote request from this machine that is not root's; answering not_allowed"
            ),
        }
    }

    /// `spec.votes` of the newest verified desired state.
    fn votes(&self) -> Option<SpecVotes> {
        self.desired
            .borrow()
            .as_ref()
            .and_then(|snapshot| snapshot.state.spec.votes.clone())
    }
}

/// Where the vote listener of `snapshot` listens: its own mesh address,
/// `spec.mesh.address` without its prefix length, on `spec.votes.port`.
/// `None` without either, while votes are not open (`spec.votes.listen` is
/// not `true`, section 8.9), or with an address that is not one.
#[must_use]
pub fn listen_address(snapshot: Option<&Snapshot>) -> Option<SocketAddr> {
    let spec = &snapshot?.state.spec;
    let votes = spec.votes.as_ref().filter(|votes| votes.open())?;
    let ip = mesh_ip(&spec.mesh.as_ref()?.address)?;
    Some(SocketAddr::new(ip, votes.port))
}

/// Who a vote connection from `remote` to the listener's `local` address
/// comes from ([`Caller`]).
///
/// A TCP connection from this machine to its own mesh address carries that
/// address as its source. Such a caller is answered only when its client
/// socket is root's ([`local_caller_uid`]), because section 7.3 has the
/// operator promote "as root on that machine".
///
/// The socket tables are read only for a peer at this machine's own address;
/// any other peer is taken as its address at once. A lookup that fails,
/// including its task, is [`Caller::CannotTell`], never root and never "not
/// root". A connection whose local address cannot be read is
/// [`Caller::CannotTell`] too.
async fn connection_caller(
    proc_net: &Path,
    remote: SocketAddr,
    local: io::Result<SocketAddr>,
) -> Caller {
    let local = match local {
        Ok(local) => local,
        Err(e) => {
            return Caller::CannotTell(format!(
                "cannot read the connection's own address from {remote}: {e}"
            ));
        }
    };
    if remote.ip().to_canonical() != local.ip().to_canonical() {
        return Caller::Peer(remote.ip());
    }
    let proc_net = proc_net.to_owned();
    match tokio::task::spawn_blocking(move || local_caller_uid(&proc_net, remote, local)).await {
        Ok(Ok(0)) => Caller::Peer(remote.ip()),
        Ok(Ok(uid)) => Caller::LocalNotRoot(uid),
        Ok(Err(e)) => Caller::CannotTell(e),
        Err(e) => Caller::CannotTell(format!("the socket lookup's task failed: {e}")),
    }
}

/// The uid that owns the client socket of a connection from this machine to
/// itself, from `peer` to the listener's `local` address.
///
/// It reads the kernel's socket tables under `proc_net` (`/proc/net` outside
/// tests): the row of `tcp`, or of `tcp6` for a dual-stack socket with an
/// IPv4-mapped address, whose local address is `peer` and whose remote
/// address is `local`, and that is established, and then that row's `uid`.
/// An established row is required because a socket the client already closed
/// can be listed with uid 0 whoever owned it.
///
/// An error when there is no such row, a table cannot be read, or a row
/// before the match does not parse. On a target other than Linux it is
/// always an error.
#[cfg(target_os = "linux")]
fn local_caller_uid(proc_net: &Path, peer: SocketAddr, local: SocketAddr) -> Result<u32, String> {
    proc_net::socket_owner(proc_net, peer, local)
}

/// Without `/proc/net`, the owner of a caller on this machine is unknown.
#[cfg(not(target_os = "linux"))]
fn local_caller_uid(
    _proc_net: &Path,
    _peer: SocketAddr,
    _local: SocketAddr,
) -> Result<u32, String> {
    Err("this system has no /proc/net socket tables".to_owned())
}

/// The kernel's TCP socket tables, `/proc/net/tcp` and `/proc/net/tcp6`.
#[cfg(target_os = "linux")]
mod proc_net {
    use std::io::BufRead;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::path::Path;

    /// `st` of an established connection.
    const ESTABLISHED: &str = "01";

    /// The uid of the established socket from `peer` to `local`, from the
    /// `tcp` table and then the `tcp6` one. Each table is read line by line
    /// and no further than the matching row.
    pub(super) fn socket_owner(
        proc_net: &Path,
        peer: SocketAddr,
        local: SocketAddr,
    ) -> Result<u32, String> {
        for name in ["tcp", "tcp6"] {
            let path = proc_net.join(name);
            let file = std::fs::File::open(&path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
            if let Some(uid) = owner_in(std::io::BufReader::new(file), peer, local)
                .map_err(|e| format!("{}: {e}", path.display()))?
            {
                return Ok(uid);
            }
        }
        Err(format!("no established socket from {peer} to {local}"))
    }

    /// The uid of the established row of `table` whose local address is
    /// `peer` and whose remote address is `local`; addresses compare in
    /// their canonical form, so an IPv4-mapped row matches its IPv4 address.
    /// It reads no further than that row: a row that does not parse, or a
    /// line that cannot be read, fails the table only when it comes before
    /// the match.
    pub(super) fn owner_in(
        table: impl BufRead,
        peer: SocketAddr,
        local: SocketAddr,
    ) -> Result<Option<u32>, String> {
        let canonical = |addr: SocketAddr| (addr.ip().to_canonical(), addr.port());
        let (peer, local) = (canonical(peer), canonical(local));
        let mut lines = table.lines();
        lines
            .next()
            .ok_or("an empty socket table")?
            .map_err(|e| format!("cannot read the table: {e}"))?;
        for line in lines {
            let line = line.map_err(|e| format!("cannot read a socket row: {e}"))?;
            if line.trim().is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            // sl local_address rem_address st tx_queue:rx_queue tr:tm->when
            // retrnsmt uid ...
            let [_, own, remote, state, _, _, _, uid, ..] = fields[..] else {
                return Err(format!("a short socket row: {line}"));
            };
            let own = address(own)?;
            let remote = address(remote)?;
            let uid = uid
                .parse::<u32>()
                .map_err(|_| format!("a socket row with a bad uid: {line}"))?;
            if own == peer && remote == local && state == ESTABLISHED {
                return Ok(Some(uid));
            }
        }
        Ok(None)
    }

    /// One address of a row: the address in hexadecimal, as the kernel
    /// stores it (each 32-bit word in the host's byte order), then `:` and
    /// the port in hexadecimal.
    fn address(text: &str) -> Result<(IpAddr, u16), String> {
        let bad = || format!("a bad socket address: {text}");
        let (ip, port) = text.split_once(':').ok_or_else(bad)?;
        let port = u16::from_str_radix(port, 16).map_err(|_| bad())?;
        let word = |hex: &str| u32::from_str_radix(hex, 16).map(u32::to_ne_bytes);
        let ip = match ip.len() {
            8 => IpAddr::V4(Ipv4Addr::from(word(ip).map_err(|_| bad())?)),
            32 => {
                let mut bytes = [0u8; 16];
                for (chunk, out) in ip.as_bytes().chunks(8).zip(bytes.chunks_mut(4)) {
                    let hex = std::str::from_utf8(chunk).map_err(|_| bad())?;
                    out.copy_from_slice(&word(hex).map_err(|_| bad())?);
                }
                IpAddr::V6(Ipv6Addr::from(bytes))
            }
            _ => return Err(bad()),
        };
        Ok((ip.to_canonical(), port))
    }
}

fn json<T: serde::Serialize>(status: StatusCode, body: &T) -> Response<Full<Bytes>> {
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    let mut response = Response::new(Full::new(Bytes::from(bytes)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

/// The body of every answer but `200`, `{"error": "<text>"}` (section 8.9),
/// with the text on one line and cut at [`ERROR_CHARS`].
fn error(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    json(
        status,
        &serde_json::json!({ "error": util::one_line(text, ERROR_CHARS) }),
    )
}

// The tests bind 127.0.0.x source addresses, which only Linux routes to the
// loopback interface without setup.
#[cfg(all(test, target_os = "linux"))]
mod tests;
