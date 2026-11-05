//! The embedded reverse proxy core (ADR 0059).
//!
//! One request path shared by every listener: the port-80 redirect listener, the
//! port-443 TLS listener and the loopback listener `cloudflared` dials into. The
//! listeners only differ in how they accept a connection and in [`ListenerMode`];
//! host validation, routing, backend selection and header rewriting live here.
//!
//! Routing reads the current [`RouteTable`] once per request, so a
//! [`ProxyCore::set_table`] swap never affects a request already in flight.
//!
//! An edge can take a backend out of rotation with [`ProxyCore::backend_down`]:
//! its pooled connections are dropped and the edge requests still waiting on
//! it are cancelled. A cancelled `GET` or `HEAD` whose request body can be sent
//! again goes once to another backend; anything else answers `502` at once. A
//! response body that is already streaming is cut, so a client never waits on
//! a backend the edge has given up on.

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::header::{
    CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri, Version};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::ServerConfig;
use rustls::server::ResolvesServerCert;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::route::{RetryPolicy, valid_hostname};
use crate::{RouteTable, TlsMode};

/// Concurrent connections a listener will hold before it stops accepting.
const MAX_CONNECTIONS: usize = 4096;
/// How long to wait for a backend TCP connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a client may take to send its request headers.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a TLS handshake may take.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long in-flight connections may finish after shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long an idle backend connection stays pooled.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// TCP keepalive on backend connections: idle time before the first probe,
/// the interval between probes and how many may go unanswered. A pooled
/// connection to a backend that vanished without closing it is dropped after
/// about 25 s instead of being handed to the next request.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const KEEPALIVE_RETRIES: u32 = 3;
/// `TCP_USER_TIMEOUT` on backend connections (Linux): request bytes the
/// backend has not acknowledged for this long fail the connection, so a
/// request written to a dead peer fails instead of waiting for the response
/// timeout. A backend that is slow to answer is not affected; it acknowledged
/// the request.
#[cfg(any(target_os = "linux", target_os = "android"))]
const USER_TIMEOUT: Duration = Duration::from_secs(20);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;
type UpstreamClient = Client<HttpConnector, UpstreamBody>;

/// Which routes a listener serves and how it labels requests.
#[derive(Clone, Copy, Debug)]
pub struct ListenerMode {
    /// Only routes with this TLS mode are served on this listener; others answer 404.
    pub serves: TlsMode,
    /// Value of X-Forwarded-Proto sent to backends ("https" for the TLS listener and the tunnel loopback listener). A trusted connector's own `http` or `https` value wins.
    pub forwarded_proto: &'static str,
    /// Trust an incoming X-Forwarded-For / CF-Connecting-IP (only for the loopback listener that cloudflared connects to, and only when the peer address is loopback). Otherwise incoming X-Forwarded-* headers are dropped and replaced.
    pub trust_forwarded: bool,
}

/// Told when a backend could not be reached or its connection was reset
/// before a response. An edge uses it to take the backend out of rotation at
/// once (`node-agent-protocol.md` section 6.8).
pub trait BackendObserver: Send + Sync {
    /// One backend of `hostname` refused a connection or was reset before it
    /// sent response headers. `error` is one line for the backend's status.
    fn connect_failed(&self, hostname: &str, backend: SocketAddr, error: &str);
}

/// The proxy's shared state: the current route table, one backend client per
/// backend address, the round-robin cursor and the optional backend observer.
pub struct ProxyCore {
    table: RwLock<Arc<RouteTable>>,
    /// The connector every backend client is built from.
    connector: HttpConnector,
    /// One client, and so one connection pool, per backend address, so the
    /// pooled connections of one backend can be dropped on their own.
    clients: Mutex<HashMap<SocketAddr, UpstreamClient>>,
    /// A counter per backend address that [`ProxyCore::backend_down`] bumps.
    /// Edge requests watch the counter of their backend and stop when it moves.
    generations: Mutex<HashMap<SocketAddr, watch::Sender<u64>>>,
    next_backend: AtomicUsize,
    permits: Arc<Semaphore>,
    observer: RwLock<Option<Arc<dyn BackendObserver>>>,
}

impl std::fmt::Debug for ProxyCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyCore")
            .field("routes", &self.table().len())
            .finish_non_exhaustive()
    }
}

impl ProxyCore {
    /// A core with an empty table. It serves 404 for every host until
    /// [`set_table`](Self::set_table) is called.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
        connector.set_nodelay(true);
        connector.set_keepalive(Some(KEEPALIVE_IDLE));
        connector.set_keepalive_interval(Some(KEEPALIVE_INTERVAL));
        connector.set_keepalive_retries(Some(KEEPALIVE_RETRIES));
        #[cfg(any(target_os = "linux", target_os = "android"))]
        connector.set_tcp_user_timeout(Some(USER_TIMEOUT));
        Arc::new(Self {
            table: RwLock::new(Arc::new(RouteTable::default())),
            connector,
            clients: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            next_backend: AtomicUsize::new(0),
            permits: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            observer: RwLock::new(None),
        })
    }

    /// Sets the observer told about backend connect failures and connection
    /// resets. Without one, a failure only reaches the request's response.
    pub fn set_observer(&self, observer: Arc<dyn BackendObserver>) {
        *self
            .observer
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    }

    /// Reports a backend failure to the observer, when there is one and the
    /// error means the connection could not be opened or was reset.
    fn observe_failure(
        &self,
        hostname: &str,
        backend: SocketAddr,
        error: &hyper_util::client::legacy::Error,
    ) {
        if !is_connect_failure(error) && !is_reset_failure(error) {
            return;
        }
        let observer = self
            .observer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer.connect_failed(hostname, backend, &error.to_string());
        }
    }

    /// Replaces the route table. Requests already in flight keep the table they
    /// started with. The pooled connections of a backend no route names any
    /// more are dropped.
    pub fn set_table(&self, table: RouteTable) {
        let served: HashSet<SocketAddr> = table
            .iter()
            .flat_map(|route| route.backends.iter().copied())
            .collect();
        {
            let mut current = self
                .table
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *current = Arc::new(table);
        }
        // A request in flight holds its own handle on its client, so it can
        // finish; the pool goes away with the last handle.
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|backend, _| served.contains(backend));
        self.generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|backend, sender| served.contains(backend) || sender.receiver_count() > 0);
    }

    /// Takes `backend` out of use for requests already in flight: its pooled
    /// connections are dropped, so no later request reuses one, and every edge
    /// request still waiting on it, or still streaming its response, is
    /// cancelled. A cancelled `GET` or `HEAD` that has not answered yet is sent
    /// once to another backend of its route; other requests answer `502`. The
    /// edge calls this when the backend leaves the rotation; new requests stop
    /// picking it with the next [`set_table`](Self::set_table).
    pub fn backend_down(&self, backend: SocketAddr) {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&backend);
        let generations = self
            .generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sender) = generations.get(&backend) {
            tracing::debug!(
                %backend,
                in_flight = sender.receiver_count(),
                "backend left the rotation; cancelling its requests"
            );
            sender.send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }

    /// The client, and so the connection pool, of one backend.
    fn client_for(&self, backend: SocketAddr) -> UpstreamClient {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(backend)
            .or_insert_with(|| {
                Client::builder(TokioExecutor::new())
                    .retry_canceled_requests(false)
                    .set_host(false)
                    .pool_idle_timeout(POOL_IDLE_TIMEOUT)
                    .pool_timer(TokioTimer::new())
                    .build(self.connector.clone())
            })
            .clone()
    }

    /// A receiver that changes when [`backend_down`](Self::backend_down) is
    /// called for `backend` from now on.
    fn watch_backend(&self, backend: SocketAddr) -> watch::Receiver<u64> {
        self.generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(backend)
            .or_insert_with(|| watch::channel(0).0)
            .subscribe()
    }

    /// The table new requests are routed with.
    #[must_use]
    pub fn table(&self) -> Arc<RouteTable> {
        Arc::clone(
            &self
                .table
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Serves plain HTTP on `listener` until `shutdown` turns true, then stops
/// accepting and lets in-flight connections finish (up to 10 s).
///
/// # Errors
///
/// Only returned if the listener task itself fails; per-connection errors are
/// logged and do not stop the listener.
pub async fn serve_http(
    listener: TcpListener,
    core: Arc<ProxyCore>,
    mode: ListenerMode,
    shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve(
        listener,
        Spec::Proxy {
            core,
            mode,
            tls: None,
        },
        shutdown,
    )
    .await
}

/// Port-80 listener of the public proxy: every request gets 308 Permanent
/// Redirect to `https://{host}{path_and_query}` when host is a served Acme
/// route; unknown host -> 404. Same shutdown semantics as [`serve_http`].
///
/// # Errors
///
/// Only returned if the listener task itself fails.
pub async fn serve_redirect(
    listener: TcpListener,
    core: Arc<ProxyCore>,
    shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve(listener, Spec::Redirect { core }, shutdown).await
}

/// TLS listener (port 443): rustls handshake with `tls`, then the same handling
/// as [`serve_http`].
///
/// # Errors
///
/// Only returned if the listener task itself fails.
pub async fn serve_https(
    listener: TcpListener,
    core: Arc<ProxyCore>,
    mode: ListenerMode,
    tls: Arc<ServerConfig>,
    shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve(
        listener,
        Spec::Proxy {
            core,
            mode,
            tls: Some(TlsAcceptor::from(tls)),
        },
        shutdown,
    )
    .await
}

/// Server config with the ring provider explicitly, no client auth, the given
/// cert resolver and ALPN `http/1.1`.
///
/// # Errors
///
/// The requested protocol versions are unsupported by the provider.
pub fn tls_server_config(
    resolver: Arc<dyn ResolvesServerCert>,
) -> Result<Arc<ServerConfig>, rustls::Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_cert_resolver(resolver);
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

#[derive(Clone)]
enum Spec {
    Proxy {
        core: Arc<ProxyCore>,
        mode: ListenerMode,
        tls: Option<TlsAcceptor>,
    },
    Redirect {
        core: Arc<ProxyCore>,
    },
}

async fn serve(
    listener: TcpListener,
    spec: Spec,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let permits = match &spec {
        Spec::Proxy { core, .. } => Arc::clone(&core.permits),
        Spec::Redirect { .. } => Arc::new(Semaphore::new(MAX_CONNECTIONS)),
    };
    let mut connections: JoinSet<()> = JoinSet::new();
    loop {
        let permit = tokio::select! {
            biased;
            _ = wait_for_shutdown(&mut shutdown) => break,
            permit = Arc::clone(&permits).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let (stream, peer) = tokio::select! {
            biased;
            _ = wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::debug!(error = %error, "accept failed");
                    // EMFILE and friends: give the process a moment to recover.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let spec = spec.clone();
        connections.spawn(async move {
            let _permit = permit;
            match spec {
                Spec::Proxy { core, mode, tls } => match tls {
                    Some(acceptor) => {
                        let stream = match tokio::time::timeout(
                            TLS_HANDSHAKE_TIMEOUT,
                            acceptor.accept(stream),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(error)) => {
                                tracing::debug!(%peer, error = %error, "tls handshake failed");
                                return;
                            }
                            Err(_) => {
                                tracing::debug!(%peer, "tls handshake timed out");
                                return;
                            }
                        };
                        let sni = stream
                            .get_ref()
                            .1
                            .server_name()
                            .map(str::to_ascii_lowercase);
                        serve_proxied(stream, core, mode, sni, peer).await;
                    }
                    None => serve_proxied(stream, core, mode, None, peer).await,
                },
                Spec::Redirect { core } => serve_redirect_connection(stream, core, peer).await,
            }
        });
        // Reap finished connections so the set does not grow without bound.
        while connections.try_join_next().is_some() {}
    }

    let drained = async { while connections.join_next().await.is_some() {} };
    if tokio::time::timeout(DRAIN_TIMEOUT, drained).await.is_err() {
        tracing::debug!("draining connections timed out");
    }
    Ok(())
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        // A closed sender means nobody can ask for shutdown any more; stop.
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn serve_proxied<I>(
    io: I,
    core: Arc<ProxyCore>,
    mode: ListenerMode,
    sni: Option<String>,
    peer: SocketAddr,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let core = Arc::clone(&core);
        let sni = sni.clone();
        async move { Ok::<_, Infallible>(handle_request(&core, mode, request, peer, sni).await) }
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .keep_alive(true);
    if let Err(error) = builder
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades()
        .await
    {
        tracing::debug!(%peer, error = %error, "connection closed with error");
    }
}

async fn serve_redirect_connection(stream: TcpStream, core: Arc<ProxyCore>, peer: SocketAddr) {
    let service = service_fn(move |request: Request<Incoming>| {
        let core = Arc::clone(&core);
        async move { Ok::<_, Infallible>(handle_redirect(&core, request)) }
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .keep_alive(true);
    if let Err(error) = builder
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(%peer, error = %error, "redirect connection closed with error");
    }
}

fn plain(status: StatusCode, body: &'static str) -> Response<ProxyBody> {
    let mut response = Response::new(box_body(Full::new(Bytes::from_static(body.as_bytes()))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn box_body<B>(body: B) -> ProxyBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

/// The host of a request, lowercased with any `:port` suffix stripped, or an
/// error message when the header is missing or not a valid hostname.
fn request_host(headers: &HeaderMap) -> Result<String, &'static str> {
    let value = headers.get(HOST).ok_or("missing host\n")?;
    let host = value.to_str().map_err(|_| "invalid host\n")?;
    let host = match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    };
    let host = host.to_ascii_lowercase();
    if !valid_hostname(&host) {
        return Err("invalid host\n");
    }
    Ok(host)
}

fn path_and_query(uri: &Uri) -> &str {
    uri.path_and_query()
        .map(hyper::http::uri::PathAndQuery::as_str)
        .unwrap_or("/")
}

fn handle_redirect(core: &ProxyCore, request: Request<Incoming>) -> Response<ProxyBody> {
    let parts = request.into_parts().0;
    let host = match request_host(&parts.headers) {
        Ok(host) => host,
        Err(message) => return plain(StatusCode::BAD_REQUEST, message),
    };
    let table = core.table();
    if table
        .get(&host)
        .is_none_or(|route| route.tls != TlsMode::Acme)
    {
        return plain(StatusCode::NOT_FOUND, "no route for this host\n");
    }
    let location = format!("https://{host}{}", path_and_query(&parts.uri));
    let mut response = plain(StatusCode::PERMANENT_REDIRECT, "");
    match HeaderValue::from_str(&location) {
        Ok(value) => {
            response
                .headers_mut()
                .insert(hyper::header::LOCATION, value);
        }
        Err(_) => return plain(StatusCode::BAD_REQUEST, "invalid host\n"),
    }
    response
}

async fn handle_request(
    core: &ProxyCore,
    mode: ListenerMode,
    mut request: Request<Incoming>,
    peer: SocketAddr,
    sni: Option<String>,
) -> Response<ProxyBody> {
    let host = match request_host(request.headers()) {
        Ok(host) => host,
        Err(message) => return plain(StatusCode::BAD_REQUEST, message),
    };
    // A certificate for one name must not be usable to reach another: when SNI
    // was sent it has to name the same host as the Host header.
    if let Some(sni) = sni.as_deref()
        && !sni.eq_ignore_ascii_case(&host)
    {
        return plain(StatusCode::MISDIRECTED_REQUEST, "misdirected request\n");
    }
    let table = core.table();
    let Some(route) = table.get(&host).filter(|route| route.tls == mode.serves) else {
        return plain(StatusCode::NOT_FOUND, "no route for this host\n");
    };

    let wants_upgrade = is_upgrade(request.headers());
    let client_upgrade = wants_upgrade.then(|| hyper::upgrade::on(&mut request));
    let query = path_and_query(request.uri()).to_string();

    let headers = request.headers_mut();
    strip_hop_by_hop(headers, wants_upgrade);
    forwarded_headers(headers, peer, mode);
    if wants_upgrade {
        // The two headers are hop-by-hop, but an upgrade needs them forwarded.
        headers.insert(CONNECTION, HeaderValue::from_static("upgrade"));
        if !headers.contains_key(UPGRADE) {
            headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
    }
    let (parts, body) = request.into_parts();
    // A body that is empty from the start can be sent again even after hyper
    // polled it; any other body only while it was never polled.
    let body_empty = body.is_end_stream();

    let state = Arc::new(BodyState {
        inner: Mutex::new(Some(body)),
        started: AtomicBool::new(false),
    });
    let attempts = route.backends.len();
    if attempts == 0 {
        return plain(StatusCode::BAD_GATEWAY, "backend unavailable\n");
    }
    // An edge retries only `GET` and `HEAD`, once, on another backend: when
    // the connection failed or was reset before a response, or when the
    // backend left the rotation while the request waited on it. Every other
    // edge request goes to one backend only. A node route keeps the older
    // policy: the next backend may be tried while the body has not started to
    // stream.
    let edge = route.retry == RetryPolicy::IdempotentOnce;
    let idempotent = edge && matches!(parts.method, Method::GET | Method::HEAD);
    let max_attempts = if idempotent {
        2
    } else if edge {
        1
    } else {
        attempts
    };
    let start = core.next_backend.fetch_add(1, Ordering::Relaxed);
    let mut response = None;
    let mut used_backend = route.backends[start % attempts];
    let mut tried: Vec<SocketAddr> = Vec::new();
    for offset in 0..max_attempts {
        let backend = if edge && offset > 0 {
            match retry_backend(core, &host, &route.backends, &tried, start) {
                Some(backend) => backend,
                None => break,
            }
        } else {
            route.backends[(start + offset) % attempts]
        };
        tried.push(backend);
        let Some(uri) = upstream_uri(backend, &query) else {
            continue;
        };
        let mut upstream = Request::new(UpstreamBody {
            state: Arc::clone(&state),
        });
        *upstream.method_mut() = parts.method.clone();
        *upstream.uri_mut() = uri;
        *upstream.version_mut() = Version::HTTP_11;
        *upstream.headers_mut() = parts.headers.clone();
        // Watch first: a backend taken down between the two calls then still
        // cancels this request rather than leaving it on a dropped pool.
        let down = edge.then(|| core.watch_backend(backend));
        let client = core.client_for(backend);
        let attempt = run_attempt(
            client.request(upstream),
            route.response_timeout,
            down.clone(),
        )
        .await;
        // Retrying is only safe while the body can be sent again.
        let replayable = body_empty || !state.started.load(Ordering::SeqCst);
        match attempt {
            Attempt::Response(backend_response) => {
                response = Some((backend_response, down));
                used_backend = backend;
                break;
            }
            Attempt::TimedOut => {
                // A backend that accepted and never answered must not hold
                // the connection; the request may have reached the app, so
                // it is not retried.
                tracing::debug!(
                    host = %host,
                    backend = %backend,
                    "backend response headers timed out"
                );
                break;
            }
            Attempt::LeftRotation => {
                tracing::debug!(
                    host = %host,
                    backend = %backend,
                    method = %parts.method,
                    retry = idempotent && replayable,
                    "backend left the rotation before it answered"
                );
                if !idempotent || !replayable {
                    break;
                }
            }
            Attempt::Failed(error) => {
                let sent = state.started.load(Ordering::SeqCst);
                tracing::debug!(host = %host, backend = %backend, sent, error = %error, "backend request failed");
                core.observe_failure(&host, backend, &error);
                match route.retry {
                    RetryPolicy::Unsent => {
                        if sent {
                            break;
                        }
                    }
                    RetryPolicy::IdempotentOnce => {
                        let retry =
                            is_connect_failure(&error) || (is_reset_failure(&error) && replayable);
                        if !idempotent || !retry {
                            break;
                        }
                    }
                }
            }
        }
    }
    let Some((mut response, down)) = response else {
        return plain(StatusCode::BAD_GATEWAY, "backend unavailable\n");
    };
    let status = response.status();
    tracing::debug!(
        host = %host,
        method = %parts.method,
        status = status.as_u16(),
        backend = %used_backend,
        "proxied request"
    );

    if wants_upgrade && status == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(client_upgrade) = client_upgrade {
            let backend_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                match tokio::try_join!(client_upgrade, backend_upgrade) {
                    Ok((client, backend)) => {
                        let mut client = TokioIo::new(client);
                        let mut backend = TokioIo::new(backend);
                        if let Err(error) =
                            tokio::io::copy_bidirectional(&mut client, &mut backend).await
                        {
                            tracing::debug!(error = %error, "upgraded stream closed with error");
                        }
                    }
                    Err(error) => tracing::debug!(error = %error, "upgrade failed"),
                }
            });
        }
        let (mut parts, _) = response.into_parts();
        parts.headers.remove(hyper::header::CONTENT_LENGTH);
        let mut out = Response::new(box_body(Full::new(Bytes::new())));
        *out.status_mut() = status;
        *out.headers_mut() = parts.headers;
        return out;
    }

    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop(&mut parts.headers, false);
    let body = match down {
        Some(down) => box_body(UntilDown {
            inner: body,
            down: Some(Box::pin(left_rotation(down))),
        }),
        None => box_body(body),
    };
    let mut out = Response::new(body);
    *out.status_mut() = status;
    *out.headers_mut() = parts.headers;
    out
}

/// How one attempt on one backend ended.
enum Attempt {
    /// The backend sent response headers.
    Response(Response<Incoming>),
    /// The client failed: connect, reset, or a broken response.
    Failed(hyper_util::client::legacy::Error),
    /// No response headers within the route's response timeout.
    TimedOut,
    /// The backend left the rotation before it sent response headers.
    LeftRotation,
}

/// Runs one attempt, bounded by `timeout` and cut short when `down` changes.
async fn run_attempt<F>(
    request: F,
    timeout: Option<Duration>,
    down: Option<watch::Receiver<u64>>,
) -> Attempt
where
    F: Future<Output = Result<Response<Incoming>, hyper_util::client::legacy::Error>>,
{
    let answered = async {
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, request).await.ok(),
            None => Some(request.await),
        }
    };
    let left = async {
        match down {
            Some(down) => left_rotation(down).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        () = left => Attempt::LeftRotation,
        answered = answered => match answered {
            Some(Ok(response)) => Attempt::Response(response),
            Some(Err(error)) => Attempt::Failed(error),
            None => Attempt::TimedOut,
        },
    }
}

/// Resolves when the backend `down` watches is taken down. Never resolves when
/// nobody can take it down any more.
async fn left_rotation(mut down: watch::Receiver<u64>) {
    if down.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// The backend an edge request is retried on: one of its route's backends in
/// the current table that it has not tried yet, or else one of the backends
/// it started with. The current table is read again because the backend that
/// failed has usually just left it.
fn retry_backend(
    core: &ProxyCore,
    host: &str,
    started_with: &[SocketAddr],
    tried: &[SocketAddr],
    start: usize,
) -> Option<SocketAddr> {
    let table = core.table();
    let current = table.get(host).map_or(&[][..], |route| &route.backends[..]);
    for backends in [current, started_with] {
        let fresh: Vec<SocketAddr> = backends
            .iter()
            .copied()
            .filter(|backend| !tried.contains(backend))
            .collect();
        if !fresh.is_empty() {
            return Some(fresh[start % fresh.len()]);
        }
    }
    None
}

/// A backend response body that fails as soon as its backend leaves the
/// rotation, so a client is never left waiting on a backend the edge gave up
/// on. The client sees a cut response; nothing is retried once bytes were
/// sent downstream.
struct UntilDown {
    inner: Incoming,
    down: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Body for UntilDown {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(down) = this.down.as_mut()
            && down.as_mut().poll(cx).is_ready()
        {
            this.down = None;
            return Poll::Ready(Some(Err("backend left the rotation".into())));
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Box::new(error)))),
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// Marks whether the outgoing request body has been polled. A body that was
/// never polled was never sent, so the request may be retried on the next
/// backend; once hyper starts reading it, retrying would duplicate it.
#[derive(Default)]
struct BodyState {
    inner: Mutex<Option<Incoming>>,
    started: AtomicBool,
}

/// The outgoing request body. It takes the client body on first poll, which lets
/// a connect failure to one backend try the next one without the body having
/// been consumed.
struct UpstreamBody {
    state: Arc<BodyState>,
}

impl Body for UpstreamBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.state.started.store(true, Ordering::SeqCst);
        let mut inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(body) = inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(body).poll_frame(cx) {
            Poll::Ready(None) => {
                *inner = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(Box::new(error)))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.state
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    let connection = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        });
    connection && headers.contains_key(UPGRADE)
}

fn header_names_in_connection(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut names = Vec::new();
    for value in headers.get_all(CONNECTION) {
        if let Ok(value) = value.to_str() {
            for token in value.split(',') {
                let token = token.trim();
                if let Ok(name) = HeaderName::from_bytes(token.as_bytes()) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Removes hop-by-hop headers: `Connection` and everything it names, the
/// proxy-only headers, `Keep-Alive`, `TE`, `Trailer` and `Transfer-Encoding`.
/// `Upgrade` and `Connection` survive only when `keep_upgrade` is set.
fn strip_hop_by_hop(headers: &mut HeaderMap, keep_upgrade: bool) {
    let mut keep: Vec<HeaderName> = Vec::new();
    if keep_upgrade {
        keep.push(CONNECTION);
        keep.push(UPGRADE);
    }
    for name in header_names_in_connection(headers) {
        // A malicious `Connection: host` must not strip the Host header the
        // route was chosen from; the backend always needs it.
        if name == HOST || keep.contains(&name) {
            continue;
        }
        headers.remove(name);
    }
    for name in [
        CONNECTION,
        HeaderName::from_static("keep-alive"),
        TE,
        TRAILER,
        TRANSFER_ENCODING,
        HeaderName::from_static("proxy-connection"),
        HeaderName::from_static("proxy-authenticate"),
        HeaderName::from_static("proxy-authorization"),
    ] {
        if !keep.contains(&name) {
            headers.remove(name);
        }
    }
    if !keep_upgrade {
        headers.remove(UPGRADE);
    }
}

fn forwarded_headers(headers: &mut HeaderMap, peer: SocketAddr, mode: ListenerMode) {
    let trusted = mode.trust_forwarded && peer.ip().is_loopback();
    if trusted {
        let existing = headers
            .get(hyper::header::HeaderName::from_static("x-forwarded-for"))
            .cloned();
        let cloudflare = headers.get("cf-connecting-ip").cloned();
        headers.remove("cf-connecting-ip");
        let value = match existing {
            Some(existing) => match existing.to_str() {
                Ok(existing) => format!("{existing}, {}", peer.ip()),
                Err(_) => peer.ip().to_string(),
            },
            None => cloudflare
                .as_ref()
                .and_then(|value| value.to_str().ok())
                .map_or_else(|| peer.ip().to_string(), str::to_string),
        };
        if let Ok(value) = HeaderValue::from_str(&value) {
            headers.insert(
                hyper::header::HeaderName::from_static("x-forwarded-for"),
                value,
            );
        }
    } else {
        let forwarded: Vec<HeaderName> = headers
            .keys()
            .filter(|name| name.as_str().starts_with("x-forwarded-"))
            .cloned()
            .collect();
        for name in forwarded {
            headers.remove(name);
        }
        headers.remove("cf-connecting-ip");
        if let Ok(value) = HeaderValue::from_str(&peer.ip().to_string()) {
            headers.insert(
                hyper::header::HeaderName::from_static("x-forwarded-for"),
                value,
            );
        }
    }
    // A trusted connector (cloudflared) knows the scheme the visitor used at
    // the edge; keep it when it is a plain `http` or `https`.
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| trusted && matches!(*value, "http" | "https"))
        .map_or(mode.forwarded_proto, |value| {
            if value == "http" { "http" } else { "https" }
        });
    headers.insert(
        hyper::header::HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static(proto),
    );
    // The host is always the one the route matched; no client value survives.
    let host = headers.get(HOST).cloned();
    headers.remove(hyper::header::HeaderName::from_static("x-forwarded-host"));
    if let Some(host) = host {
        headers.insert(
            hyper::header::HeaderName::from_static("x-forwarded-host"),
            host,
        );
    }
}

fn upstream_uri(backend: SocketAddr, path_and_query: &str) -> Option<Uri> {
    format!("http://{backend}{path_and_query}").parse().ok()
}

/// Whether a legacy client error means the backend could not be reached.
fn is_connect_failure(error: &hyper_util::client::legacy::Error) -> bool {
    error.is_connect()
}

/// Whether a legacy client error carries an I/O reset, abort or socket
/// timeout. It arrives after the connection existed, so it is not a connect
/// failure, but it still means the backend never sent a response. A socket
/// timeout is `TCP_USER_TIMEOUT` or keepalive giving up on a dead peer, not the
/// proxy's response timeout.
fn is_reset_failure(error: &hyper_util::client::legacy::Error) -> bool {
    use std::io::ErrorKind;

    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
    while let Some(error) = source {
        if let Some(io) = error.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::TimedOut
            )
        {
            return true;
        }
        source = error.source();
    }
    false
}
