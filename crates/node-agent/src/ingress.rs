// SPDX-License-Identifier: AGPL-3.0-only
//! The node's ingress supervisor: it serves the routes of the signed desired
//! state (`node-agent-protocol.md` sections 5.11 and 6.2 step 5).
//!
//! Everything that binds a port, runs `cloudflared` or holds a certificate
//! lives here, in the node agent (ADR 0096). The supervisor is fed the complete
//! route set every time the reconciler applies a snapshot: there are no deltas.
//! It runs in its own task so a slow listener, a certificate order or a
//! reconnect of the connector never blocks a reconcile pass, and it keeps
//! serving the last applied table while the primary is unreachable.
//!
//! Two backends cover the two TLS modes of `spec.routes[].tls_mode`:
//!
//! - `tunnel` routes go to [`quoro_ingress::tunnel::TunnelIngress`], which runs
//!   `cloudflared` with the node's run token and forwards every hostname to a
//!   loopback proxy. The token never appears in a log line: [`Token`]'s and
//!   [`SpecTunnel`]'s `Debug` both redact it.
//! - `acme` routes go to [`quoro_ingress::public::PublicIngress`], which binds
//!   80 and 443 and obtains certificates through ACME DNS-01. The node holds no
//!   DNS credential (ADR 0094): its [`DnsProvider`] asks the primary to write
//!   the `_acme-challenge` TXT record over the session (`dns_challenge`,
//!   section 5.22) and checks propagation itself against a public
//!   DNS-over-HTTPS resolver.
//!
//! Certificates and their keys live only under `<data_dir>/certs` (0700). The
//! per-hostname certificate state is published in `status_report.routes` and
//! the transitions are buffered as `certificate.*` node events.
//!
//! **Contract gap.** `events.schema.json` requires `data.domain_id` and a
//! `domain` subject for every `certificate.*` event, but the desired state the
//! node receives carries no domain id (`spec.routes[]` has only `app_id`,
//! `hostname`, `vm_ids`, `port` and `tls_mode`). The node therefore buffers a
//! certificate event only when a hostname-to-domain mapping is known; until
//! the contract carries that mapping the transition is logged instead of
//! emitted, rather than inventing an id.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use quoro_ingress::acme::AcmeConfig;
use quoro_ingress::cloudflared::CloudflaredConfig;
use quoro_ingress::public::{PublicConfig, PublicIngress};
use quoro_ingress::tunnel::{TunnelConfig, TunnelIngress, TunnelRun};
use quoro_ingress::{Ingress as _, IngressEvent, Route, RouteTable, TlsMode};
use quoro_proto::wire::{
    DnsChallenge, DnsChallengeAction, DnsChallengeResult, RouteStatus, SpecIngress, SpecRoute,
    SpecTunnel,
};
use quoro_providers::cloudflare::{Cloudflare, CloudflareDns};
use quoro_providers::{DnsProvider, ProviderError, RecordId, Token};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::store::NodeStore;
use crate::util;

/// The label ACME DNS-01 puts in front of the hostname (RFC 8555 section 8.4).
const ACME_CHALLENGE_PREFIX: &str = "_acme-challenge.";

/// How long the node waits for the primary's `dns_challenge_result`.
pub const DNS_CHALLENGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Plain HTTP listener of the public backend (redirects to HTTPS).
pub const PUBLIC_HTTP: &str = "0.0.0.0:80";
/// TLS listener of the public backend.
pub const PUBLIC_HTTPS: &str = "0.0.0.0:443";

/// The complete route set of one apply: `spec.routes`, `spec.ingress` and the
/// address the reconciler knows for each VM.
#[derive(Clone, Debug, Default)]
struct DesiredRoutes {
    routes: Vec<SpecRoute>,
    tunnel: Option<SpecTunnel>,
    /// Observed IPv4 address per `vm_id`. A VM without one is not a backend
    /// yet; its route comes back on the next apply.
    addresses: HashMap<String, Ipv4Addr>,
}

/// The TLS mode a route is served with, parsed from `spec.routes[].tls_mode`.
fn tls_mode(mode: &str) -> Option<TlsMode> {
    match mode {
        "acme" => Some(TlsMode::Acme),
        "tunnel" => Some(TlsMode::Tunnel),
        _ => None,
    }
}

/// The two route tables of one apply, split by TLS mode.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteTables {
    /// Hostnames served through the tunnel backend.
    pub tunnel: RouteTable,
    /// Hostnames terminated by the public backend with an ACME certificate.
    pub acme: RouteTable,
}

/// Builds the complete tables from `spec.routes` and the observed VM
/// addresses: `<vm ip>:<port>` for every listed VM that has an address.
///
/// Routes whose VMs have no address yet are skipped (they come back on the
/// next apply), a hostname is served by one mode only (the first occurrence
/// wins), and an unknown `tls_mode` or an invalid hostname is skipped rather
/// than failing the whole table.
#[must_use]
pub fn build_tables(routes: &[SpecRoute], addresses: &HashMap<String, Ipv4Addr>) -> RouteTables {
    let mut seen: HashSet<String> = HashSet::new();
    let mut tunnel = Vec::new();
    let mut acme = Vec::new();
    for spec in routes {
        let Some(mode) = tls_mode(&spec.tls_mode) else {
            warn!(
                hostname = %spec.hostname,
                tls_mode = %spec.tls_mode,
                "route has an unknown tls_mode; not served"
            );
            continue;
        };
        let hostname = spec.hostname.to_ascii_lowercase();
        if !seen.insert(hostname.clone()) {
            warn!(%hostname, "route appears more than once; keeping the first");
            continue;
        }
        let mut backends: Vec<SocketAddr> = Vec::new();
        for vm_id in &spec.vm_ids {
            if let Some(ip) = addresses.get(vm_id) {
                backends.push(SocketAddr::from((*ip, spec.port)));
            }
        }
        if backends.is_empty() {
            debug!(%hostname, "no backend has an address yet; not served this pass");
            continue;
        }
        let route = Route {
            hostname,
            backends,
            tls: mode,
        };
        match mode {
            TlsMode::Tunnel => tunnel.push(route),
            TlsMode::Acme => acme.push(route),
        }
    }
    let table = |routes| {
        RouteTable::new(routes).unwrap_or_else(|e| {
            // Individual routes were already validated; a duplicate or bad name
            // that still got here is a bug, and an empty table is the safe answer.
            error!(error = %e, "could not build a route table");
            RouteTable::default()
        })
    };
    RouteTables {
        tunnel: table(tunnel),
        acme: table(acme),
    }
}

/// The tunnel a node is running, as far as the lifecycle decision needs it.
/// The token's `Debug` is redacted; it is never printed.
#[derive(Clone, Debug)]
pub struct TunnelIdentity {
    pub tunnel_id: String,
    pub token: Token,
    pub origin_port: u16,
}

impl TunnelIdentity {
    /// Whether `other` runs the same tunnel with the same token.
    fn same_credential(&self, other: &Self) -> bool {
        self.tunnel_id == other.tunnel_id
            // `Token` has no `PartialEq` by design; compare its exposed value
            // without ever formatting it.
            && self.token.expose() == other.token.expose()
    }
}

/// What to do with the tunnel connector for one apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TunnelAction {
    /// Nothing changed.
    None,
    /// No connector runs yet: start it.
    Start,
    /// Same origin port, different tunnel id or token: replace the run.
    SetRun,
    /// Different origin port: the loopback proxy must be re-bound.
    Restart,
    /// `spec.ingress.tunnel` is gone: stop the connector.
    Stop,
}

/// The pure lifecycle decision for `desired` against the running tunnel.
#[must_use]
pub fn tunnel_action(
    running: Option<&TunnelIdentity>,
    desired: Option<&TunnelIdentity>,
) -> TunnelAction {
    match (running, desired) {
        (None, None) => TunnelAction::None,
        (None, Some(_)) => TunnelAction::Start,
        (Some(_), None) => TunnelAction::Stop,
        (Some(running), Some(desired)) => {
            if running.origin_port != desired.origin_port {
                TunnelAction::Restart
            } else if !running.same_credential(desired) {
                TunnelAction::SetRun
            } else {
                TunnelAction::None
            }
        }
    }
}

/// Certificate state of one hostname, as `status_report.routes` reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertState {
    Pending,
    Renewing,
    Issued,
    Failed,
    NotApplicable,
}

impl CertState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Renewing => "renewing",
            Self::Issued => "issued",
            Self::Failed => "failed",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// What the node knows about one hostname.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostStatus {
    pub state: CertState,
    /// `notAfter` of the served certificate, RFC 3339 UTC.
    pub expires_at: Option<String>,
}

impl HostStatus {
    fn pending() -> Self {
        Self {
            state: CertState::Pending,
            expires_at: None,
        }
    }
}

/// Where the supervisor sends what it needs from outside: the DNS-01 records
/// (through the session), the certificate events and the node store.
pub struct IngressConfig {
    /// The connector binary and its metrics address, under the artifacts dir.
    pub cloudflared: CloudflaredConfig,
    /// Node-local certificate directory (`<data_dir>/certs`).
    pub cert_dir: PathBuf,
    /// ACME account state (`<data_dir>/acme`).
    pub acme_state_dir: PathBuf,
    /// ACME directory URL (`LETS_ENCRYPT_PRODUCTION` unless configured).
    pub acme_directory_url: String,
    /// ACME account contacts (`mailto:`); may be empty.
    pub acme_contact: Vec<String>,
    /// Public backend listeners; the defaults are 80 and 443.
    pub http: SocketAddr,
    pub https: SocketAddr,
}

impl IngressConfig {
    /// The production defaults with the node's own directories.
    #[must_use]
    pub fn new(data_dir: &std::path::Path, cloudflared: CloudflaredConfig) -> Self {
        Self {
            cloudflared,
            cert_dir: data_dir.join("certs"),
            acme_state_dir: data_dir.join("acme"),
            acme_directory_url: quoro_ingress::acme::LETS_ENCRYPT_PRODUCTION.to_string(),
            acme_contact: Vec::new(),
            http: PUBLIC_HTTP
                .parse()
                .expect("PUBLIC_HTTP is a socket address"),
            https: PUBLIC_HTTPS
                .parse()
                .expect("PUBLIC_HTTPS is a socket address"),
        }
    }
}

/// The `certificate.*` node events, buffered for the primary.
///
/// A hostname is only emitted when a domain id is known, because the event
/// schema requires one and the desired state does not carry it (see the module
/// docs). `domains` is empty today and the mapping is the gap to close in the
/// contract.
#[derive(Clone)]
pub struct CertificateEvents {
    store: NodeStore,
    node_id: Option<String>,
    domains: BTreeMap<String, String>,
    logged: Arc<Mutex<HashSet<String>>>,
}

impl CertificateEvents {
    #[must_use]
    pub fn new(store: NodeStore, node_id: Option<String>) -> Self {
        Self {
            store,
            node_id,
            domains: BTreeMap::new(),
            logged: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Supplies the hostname-to-domain mapping once the protocol carries it.
    #[must_use]
    pub fn with_domains(mut self, domains: BTreeMap<String, String>) -> Self {
        self.domains = domains;
        self
    }

    fn domain_for(&self, hostname: &str) -> Option<&str> {
        self.domains
            .get(&hostname.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// Buffers one certificate event, or logs why it cannot be buffered. The
    /// caller supplies the type-specific fields without `domain_id` or
    /// `hostname`, which are added here so they always match the subject.
    async fn emit(&self, kind: &str, hostname: &str, mut data: serde_json::Map<String, Value>) {
        let Some(node_id) = self.node_id.as_deref() else {
            debug!(%hostname, kind, "no node id; not emitting a certificate event");
            return;
        };
        let Some(domain_id) = self.domain_for(hostname) else {
            let key = format!("{kind}:{hostname}");
            let first = self
                .logged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key);
            if first {
                warn!(
                    %hostname,
                    kind,
                    "the desired state carries no domain id, which a certificate event requires; logging the transition instead of emitting it"
                );
            }
            return;
        };
        let evt_id = util::event_id();
        data.insert(
            "domain_id".to_string(),
            Value::String(domain_id.to_string()),
        );
        data.insert("hostname".to_string(), Value::String(hostname.to_string()));
        let body = certificate_event_body(
            kind,
            domain_id,
            node_id,
            util::rfc3339_nanos(self.store.next_log_time()),
            Value::Object(data),
        );
        if let Err(e) = self.store.push_event(&evt_id, &body).await {
            warn!(%hostname, kind, error = %e, "could not buffer a certificate event");
        }
    }
}

/// One `certificate.*` envelope, in the shape `events.schema.json` requires.
fn certificate_event_body(
    kind: &str,
    domain_id: &str,
    node_id: &str,
    time: String,
    data: Value,
) -> Value {
    serde_json::json!({
        "schema_version": 1,
        "id": util::event_id(),
        "type": kind,
        "time": time,
        "source": {"kind": "node", "node_id": node_id},
        "subject": {"type": "domain", "id": domain_id},
        "actor": {"type": "node", "id": node_id},
        "data": data,
    })
}

/// A `dns_challenge` request handed to the live session.
#[derive(Debug)]
pub struct DnsRequest {
    pub id: String,
    pub challenge: DnsChallenge,
}

/// The bridge between the ACME worker and the session: requests go out as
/// `dns_challenge` frames and the answer comes back as `dns_challenge_result`.
pub struct DnsBridge {
    pending: Mutex<HashMap<String, oneshot::Sender<DnsChallengeResult>>>,
    outbox: Mutex<Option<mpsc::Sender<DnsRequest>>>,
    /// `record id` to the hostname it was created for, so the later `clear`
    /// names the same hostname the `set` did.
    created: Mutex<HashMap<String, String>>,
    timeout: Duration,
}

impl std::fmt::Debug for DnsBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DnsBridge")
    }
}

impl Default for DnsBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl DnsBridge {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            outbox: Mutex::new(None),
            created: Mutex::new(HashMap::new()),
            timeout: DNS_CHALLENGE_TIMEOUT,
        }
    }

    /// A bridge with a shorter timeout for tests.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        let mut bridge = Self::new();
        bridge.timeout = timeout;
        bridge
    }

    /// Called by the session once it is up; the receiver is drained into the
    /// WebSocket sink. A second call replaces the first sender, whose session
    /// is gone.
    pub fn attach(&self) -> mpsc::Receiver<DnsRequest> {
        let (tx, rx) = mpsc::channel(16);
        let old = self
            .outbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(tx);
        drop(old);
        rx
    }

    /// Called when the session ends: the next provider call fails fast and
    /// waiters wake with an error instead of hanging until their timeout.
    pub fn detach(&self) {
        let old = self
            .outbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(old);
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Resolves the waiter of the `dns_challenge` whose id is `re`.
    ///
    /// Returns whether a waiter was waiting; a result for an unknown id (a late
    /// answer after the timeout) is dropped.
    pub fn complete(&self, re: &str, result: DnsChallengeResult) -> bool {
        let waiter = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(re);
        match waiter {
            Some(waiter) => {
                let _ = waiter.send(result);
                true
            }
            None => false,
        }
    }

    async fn request(&self, challenge: DnsChallenge) -> Result<DnsChallengeResult, ProviderError> {
        let id = util::message_id();
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.clone(), tx);
        let sender = self
            .outbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            return Err(ProviderError::Transport(
                "no session with the primary is up".to_string(),
            ));
        };
        if sender
            .send(DnsRequest {
                id: id.clone(),
                challenge,
            })
            .await
            .is_err()
        {
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            return Err(ProviderError::Transport(
                "the session with the primary is not accepting messages".to_string(),
            ));
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(ProviderError::Transport(
                "the session ended before the primary answered dns_challenge".to_string(),
            )),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
                Err(ProviderError::Transport(format!(
                    "the primary did not answer dns_challenge within {:?}",
                    self.timeout
                )))
            }
        }
    }
}

/// The node's DNS-01 provider: records go through the primary (ADR 0094), and
/// propagation is checked against a public DNS-over-HTTPS resolver with no
/// credential at all.
pub struct SessionDns {
    bridge: Arc<DnsBridge>,
    /// A `CloudflareDns` only for its credential-free `txt_visible`, which
    /// queries a public DoH endpoint. The empty token is never sent: the DoH
    /// call is unauthenticated.
    doh: Result<CloudflareDns, String>,
}

impl SessionDns {
    #[must_use]
    pub fn new(bridge: Arc<DnsBridge>) -> Self {
        let doh = Cloudflare::with_endpoints(
            Token::new(String::new()),
            quoro_providers::cloudflare::API_BASE,
            quoro_providers::cloudflare::DOH_BASE,
        )
        // `txt_visible` never reads the zone id.
        .map(|client| client.dns(""))
        .map_err(|e| e.to_string());
        Self { bridge, doh }
    }

    async fn visible(&self, name: &str, value: &str) -> Result<bool, ProviderError> {
        match &self.doh {
            Ok(doh) => doh.txt_visible(name, value).await,
            Err(e) => Err(ProviderError::Transport(e.clone())),
        }
    }
}

impl DnsProvider for SessionDns {
    async fn create_txt(&self, name: &str, value: &str) -> Result<RecordId, ProviderError> {
        // The ACME manager names the record (`_acme-challenge.<hostname>`), but
        // `dns_challenge` carries the route's hostname: the primary derives the
        // record name itself, so the node cannot pick another one (section 5.22).
        let hostname = name.strip_prefix(ACME_CHALLENGE_PREFIX).ok_or_else(|| {
            ProviderError::Transport(format!(
                "{name:?} is not an ACME challenge record; the node only publishes those"
            ))
        })?;
        let result = self
            .bridge
            .request(DnsChallenge {
                action: DnsChallengeAction::Set,
                hostname: hostname.to_string(),
                value: Some(value.to_string()),
                record_id: None,
            })
            .await?;
        if !result.ok {
            return Err(ProviderError::Api {
                status: 0,
                message: result
                    .message
                    .unwrap_or_else(|| "the primary refused the TXT record".to_string()),
            });
        }
        let Some(record_id) = result.record_id else {
            return Err(ProviderError::Api {
                status: 0,
                message: "the primary accepted the TXT record without a record id".to_string(),
            });
        };
        self.bridge
            .created
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(record_id.clone(), hostname.to_string());
        Ok(RecordId(record_id))
    }

    async fn delete_record(&self, id: &RecordId) -> Result<(), ProviderError> {
        let hostname = self
            .bridge
            .created
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id.0);
        let Some(hostname) = hostname else {
            return Err(ProviderError::Transport(format!(
                "no hostname is known for record {:?}",
                id.0
            )));
        };
        let result = self
            .bridge
            .request(DnsChallenge {
                action: DnsChallengeAction::Clear,
                hostname,
                value: None,
                record_id: Some(id.0.clone()),
            })
            .await?;
        if result.ok {
            Ok(())
        } else {
            Err(ProviderError::Api {
                status: 0,
                message: result
                    .message
                    .unwrap_or_else(|| "the primary refused to remove the TXT record".to_string()),
            })
        }
    }

    async fn upsert_cname(
        &self,
        _name: &str,
        _target: &str,
        _proxied: bool,
    ) -> Result<RecordId, ProviderError> {
        Err(ProviderError::Transport(
            "the node holds no DNS credential; CNAMEs are written by the primary".to_string(),
        ))
    }

    async fn delete_cname(&self, _name: &str) -> Result<(), ProviderError> {
        Err(ProviderError::Transport(
            "the node holds no DNS credential; CNAMEs are written by the primary".to_string(),
        ))
    }

    async fn txt_visible(&self, name: &str, value: &str) -> Result<bool, ProviderError> {
        self.visible(name, value).await
    }
}

/// The supervisor the agent creates once and stops on shutdown.
pub struct IngressSupervisor {
    desired: watch::Sender<DesiredRoutes>,
    status: Arc<RwLock<BTreeMap<String, HostStatus>>>,
    dns: Arc<DnsBridge>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl IngressSupervisor {
    /// Spawns the supervisor task. It does nothing until the first
    /// [`Self::update`].
    #[must_use]
    pub fn new(config: IngressConfig, dns: Arc<DnsBridge>, events: CertificateEvents) -> Self {
        let (desired, desired_rx) = watch::channel(DesiredRoutes::default());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (event_tx, event_rx) = mpsc::channel(256);
        let status = Arc::new(RwLock::new(BTreeMap::new()));
        let task = tokio::spawn(supervise(
            config,
            Arc::clone(&dns),
            events,
            desired_rx,
            shutdown_rx,
            event_tx,
            Arc::clone(&status),
            event_rx,
        ));
        Self {
            desired,
            status,
            dns,
            shutdown,
            task: Mutex::new(Some(task)),
        }
    }

    /// Hands the supervisor the complete route set of one apply. Never blocks
    /// the reconcile loop: the newest set wins if the supervisor is busy.
    pub fn update(
        &self,
        routes: &[SpecRoute],
        ingress: Option<&SpecIngress>,
        addresses: HashMap<String, Ipv4Addr>,
    ) {
        let _ = self.desired.send_replace(DesiredRoutes {
            routes: routes.to_vec(),
            tunnel: ingress.and_then(|ingress| ingress.tunnel.clone()),
            addresses,
        });
    }

    /// The certificate state of every route, in `status_report` shape.
    #[must_use]
    pub fn route_statuses(&self, routes: &[SpecRoute]) -> Vec<RouteStatus> {
        let status = self
            .status
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes
            .iter()
            .map(|route| {
                let hostname = route.hostname.to_ascii_lowercase();
                status_for(route, status.get(&hostname))
            })
            .collect()
    }

    /// The DNS bridge the session attaches to.
    #[must_use]
    pub fn dns(&self) -> &Arc<DnsBridge> {
        &self.dns
    }

    /// Stops the listeners and the connector. Routes are not withdrawn from
    /// the provider; a restarted agent serves the same hostnames again.
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// One route's `status_report.routes` entry: its supervisor status, or the
/// pre-supervisor default of its TLS mode.
fn status_for(route: &SpecRoute, known: Option<&HostStatus>) -> RouteStatus {
    let state = known.map_or_else(
        || {
            if tls_mode(&route.tls_mode) == Some(TlsMode::Tunnel) {
                CertState::NotApplicable
            } else {
                CertState::Pending
            }
        },
        |status| status.state,
    );
    RouteStatus {
        hostname: route.hostname.clone(),
        cert_status: state.as_str().to_string(),
        cert_expires_at: known.and_then(|status| status.expires_at.clone()),
    }
}

/// The mutable state of the supervisor task.
struct Runtime {
    config: IngressConfig,
    dns: Arc<DnsBridge>,
    events: CertificateEvents,
    event_tx: mpsc::Sender<IngressEvent>,
    status: Arc<RwLock<BTreeMap<String, HostStatus>>>,
    tunnel: Option<TunnelIngress>,
    running: Option<TunnelIdentity>,
    public: Option<PublicIngress<SessionDns>>,
    /// The tables last handed to each backend, so an unchanged 2 s apply does
    /// not wake the readiness loop or the ACME worker.
    applied_tunnel: Option<RouteTable>,
    applied_acme: Option<RouteTable>,
    /// Hostnames announced as unserved, so the log line is written once.
    warned: HashSet<String>,
    missing_binary_logged: bool,
    /// Backoff for a failed public-ingress start (port 80/443 busy, bad cert
    /// directory); without it a 2 s reconcile tick would retry and log forever.
    public_retry_at: Option<tokio::time::Instant>,
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    config: IngressConfig,
    dns: Arc<DnsBridge>,
    events: CertificateEvents,
    mut desired: watch::Receiver<DesiredRoutes>,
    mut shutdown: watch::Receiver<bool>,
    event_tx: mpsc::Sender<IngressEvent>,
    status: Arc<RwLock<BTreeMap<String, HostStatus>>>,
    mut event_rx: mpsc::Receiver<IngressEvent>,
) {
    let mut runtime = Runtime {
        config,
        dns,
        events,
        event_tx,
        status,
        tunnel: None,
        running: None,
        public: None,
        applied_tunnel: None,
        applied_acme: None,
        warned: HashSet::new(),
        missing_binary_logged: false,
        public_retry_at: None,
    };
    let mut current = desired.borrow_and_update().clone();
    runtime.apply(&current).await;
    loop {
        tokio::select! {
            changed = desired.changed() => {
                if changed.is_err() {
                    break;
                }
                current = desired.borrow_and_update().clone();
                runtime.apply(&current).await;
            }
            event = event_rx.recv() => {
                if let Some(event) = event {
                    runtime.note_event(event).await;
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    if let Some(tunnel) = runtime.tunnel.take() {
        tunnel.shutdown().await;
    }
    if let Some(public) = runtime.public.take() {
        public.shutdown().await;
    }
    info!("ingress supervisor stopped");
}

impl Runtime {
    async fn apply(&mut self, desired: &DesiredRoutes) {
        let tables = build_tables(&desired.routes, &desired.addresses);
        self.apply_tunnel(desired, &tables.tunnel).await;
        self.apply_public(&tables.acme).await;
        self.record_statuses(&tables);
    }

    /// Starts, replaces or stops `cloudflared` to match `spec.ingress.tunnel`.
    async fn apply_tunnel(&mut self, desired: &DesiredRoutes, table: &RouteTable) {
        let wanted = desired.tunnel.as_ref().map(|tunnel| TunnelIdentity {
            tunnel_id: tunnel.tunnel_id.clone(),
            token: Token::new(tunnel.token.clone()),
            origin_port: tunnel.origin_port,
        });
        match tunnel_action(self.running.as_ref(), wanted.as_ref()) {
            TunnelAction::None => {}
            TunnelAction::Start | TunnelAction::Restart => {
                if let Some(tunnel) = self.tunnel.take() {
                    tunnel.shutdown().await;
                }
                self.running = None;
                let Some(wanted) = wanted else {
                    return;
                };
                if !self.config.cloudflared.binary.is_file() {
                    if !self.missing_binary_logged {
                        self.missing_binary_logged = true;
                        error!(
                            binary = %self.config.cloudflared.binary.display(),
                            "the cloudflared binary is missing; tunnel routes are reported failed until it is installed"
                        );
                    }
                    for route in table.iter() {
                        self.mark_tunnel_failed(
                            &route.hostname,
                            "the cloudflared binary is missing",
                        );
                    }
                    return;
                }
                let run = TunnelRun {
                    tunnel_id: wanted.tunnel_id.clone(),
                    token: wanted.token.clone(),
                };
                let config = TunnelConfig {
                    origin: SocketAddr::from(([127, 0, 0, 1], wanted.origin_port)),
                    cloudflared: self.config.cloudflared.clone(),
                };
                match TunnelIngress::start(config, run).await {
                    Ok(tunnel) => {
                        self.forward_events(tunnel.subscribe());
                        self.tunnel = Some(tunnel);
                        self.applied_tunnel = None;
                        info!(origin_port = wanted.origin_port, "tunnel ingress started");
                        self.running = Some(wanted);
                    }
                    Err(e) => {
                        warn!(error = %e, "could not start the tunnel ingress");
                        for route in table.iter() {
                            self.mark_tunnel_failed(&route.hostname, &e.to_string());
                        }
                        return;
                    }
                }
            }
            TunnelAction::SetRun => {
                let (Some(tunnel), Some(wanted)) = (self.tunnel.as_ref(), wanted) else {
                    return;
                };
                tunnel
                    .set_run(TunnelRun {
                        tunnel_id: wanted.tunnel_id.clone(),
                        token: wanted.token.clone(),
                    })
                    .await;
                info!("tunnel connector replaced with a new run token");
                self.running = Some(wanted);
            }
            TunnelAction::Stop => {
                if let Some(tunnel) = self.tunnel.take() {
                    tunnel.shutdown().await;
                }
                self.running = None;
                self.applied_tunnel = None;
                info!("spec.ingress.tunnel is absent; tunnel connector stopped");
            }
        }

        // `tunnel` routes without a tunnel are not served, once per hostname.
        match (&self.tunnel, desired.tunnel.is_some()) {
            (None, false) => {
                for route in table.iter() {
                    if self.warned.insert(route.hostname.clone()) {
                        warn!(
                            hostname = %route.hostname,
                            "tunnel route has no spec.ingress.tunnel on this node; not served"
                        );
                    }
                }
            }
            _ => self.warned.clear(),
        }

        if let Some(tunnel) = &self.tunnel
            && self.applied_tunnel.as_ref() != Some(table)
        {
            match tunnel.apply(table.clone()).await {
                Ok(()) => self.applied_tunnel = Some(table.clone()),
                Err(e) => {
                    warn!(error = %e, "could not apply the tunnel route table");
                    for route in table.iter() {
                        self.mark_tunnel_failed(&route.hostname, &e.to_string());
                    }
                }
            }
        }
    }

    /// Starts the public backend lazily (it binds 80 and 443), applies the
    /// `acme` table and stops it again when no `acme` route remains.
    async fn apply_public(&mut self, table: &RouteTable) {
        if table.is_empty() {
            if let Some(public) = self.public.take() {
                public.shutdown().await;
                self.applied_acme = None;
                info!("no acme route remains; public ingress stopped");
            }
            return;
        }
        if self.public.is_none() {
            if let Some(at) = self.public_retry_at
                && tokio::time::Instant::now() < at
            {
                return;
            }
            let mut acme = AcmeConfig::new(
                self.config.acme_directory_url.clone(),
                self.config.acme_state_dir.clone(),
            );
            acme.contact.clone_from(&self.config.acme_contact);
            let config = PublicConfig {
                http: self.config.http,
                https: self.config.https,
                cert_dir: self.config.cert_dir.clone(),
                acme,
            };
            match PublicIngress::start(config, Arc::new(SessionDns::new(Arc::clone(&self.dns))))
                .await
            {
                Ok(public) => {
                    self.forward_events(public.subscribe());
                    self.public = Some(public);
                    self.applied_acme = None;
                    self.public_retry_at = None;
                    info!("public ingress started on 80 and 443");
                }
                Err(e) => {
                    warn!(error = %e, "could not start the public ingress");
                    self.public_retry_at =
                        Some(tokio::time::Instant::now() + Duration::from_secs(60));
                    let mut status = self
                        .status
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for route in table.iter() {
                        status.insert(
                            route.hostname.clone(),
                            HostStatus {
                                state: CertState::Failed,
                                expires_at: None,
                            },
                        );
                    }
                    return;
                }
            }
        }
        if let Some(public) = &self.public
            && self.applied_acme.as_ref() != Some(table)
        {
            match public.apply(table.clone()).await {
                Ok(()) => self.applied_acme = Some(table.clone()),
                Err(e) => warn!(error = %e, "could not apply the acme route table"),
            }
        }
    }

    /// Seeds the status map: every `tunnel` route is `not_applicable`, every
    /// new `acme` route starts `pending`, and hostnames that left the spec are
    /// forgotten.
    fn record_statuses(&mut self, tables: &RouteTables) {
        let mut status = self
            .status
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut keep = BTreeSet::new();
        for route in tables.tunnel.iter() {
            keep.insert(route.hostname.clone());
            status.insert(
                route.hostname.clone(),
                HostStatus {
                    state: CertState::NotApplicable,
                    expires_at: None,
                },
            );
        }
        for route in tables.acme.iter() {
            keep.insert(route.hostname.clone());
            status
                .entry(route.hostname.clone())
                .or_insert_with(HostStatus::pending);
        }
        status.retain(|hostname, _| keep.contains(hostname));
    }

    fn mark_tunnel_failed(&self, hostname: &str, reason: &str) {
        warn!(%hostname, reason, "tunnel route failed");
    }

    /// Forwards one ingress backend's events into the supervisor's loop.
    fn forward_events(&self, mut events: tokio::sync::broadcast::Receiver<IngressEvent>) {
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        if tx.send(event).await.is_err() {
                            return;
                        }
                    }
                    // A lagging receiver keeps the newest events; a closed
                    // channel means the backend was shut down.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });
    }

    async fn note_event(&mut self, event: IngressEvent) {
        match event {
            IngressEvent::CertificatePending { hostname, renewal } => {
                let state = if renewal {
                    CertState::Renewing
                } else {
                    CertState::Pending
                };
                self.set_state(&hostname, state, None);
            }
            IngressEvent::CertificateIssued {
                hostname,
                not_after,
            } => {
                self.set_state(&hostname, CertState::Issued, Some(not_after.clone()));
                info!(%hostname, not_after, "certificate issued");
                let mut data = serde_json::Map::new();
                data.insert("expires_at".to_string(), Value::String(not_after));
                self.events
                    .emit("certificate.issued", &hostname, data)
                    .await;
            }
            IngressEvent::CertificateFailed { hostname, reason } => {
                self.set_state(&hostname, CertState::Failed, None);
                warn!(%hostname, reason, "certificate failed; retried with backoff");
                let mut data = serde_json::Map::new();
                data.insert("tls_mode".to_string(), Value::String("acme".to_string()));
                data.insert("error".to_string(), Value::String(reason));
                self.events
                    .emit("certificate.failed", &hostname, data)
                    .await;
            }
            IngressEvent::CertificateExpiring {
                hostname,
                not_after,
            } => {
                let days_left = days_until(&not_after).unwrap_or(0);
                warn!(%hostname, not_after, days_left, "certificate is expiring");
                let mut data = serde_json::Map::new();
                data.insert("expires_at".to_string(), Value::String(not_after));
                data.insert("days_left".to_string(), Value::from(days_left));
                self.events
                    .emit("certificate.expiring", &hostname, data)
                    .await;
            }
            IngressEvent::TunnelRouteReady { hostname } => {
                debug!(%hostname, "tunnel route ready");
                self.set_state(&hostname, CertState::NotApplicable, None);
            }
            IngressEvent::TunnelRouteFailed { hostname, reason } => {
                self.mark_tunnel_failed(&hostname, &reason);
                self.set_state(&hostname, CertState::NotApplicable, None);
            }
        }
    }

    fn set_state(&self, hostname: &str, state: CertState, expires_at: Option<String>) {
        let mut status = self
            .status
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        status.insert(
            hostname.to_ascii_lowercase(),
            HostStatus { state, expires_at },
        );
    }
}

/// Whole days until `not_after` (RFC 3339), for `certificate.expiring`.
fn days_until(not_after: &str) -> Option<u64> {
    let deadline = util::parse_rfc3339_nanos(not_after)?;
    let left = deadline.saturating_sub(util::now_ns());
    Some(u64::try_from(left / (24 * 60 * 60 * 1_000_000_000)).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(hostname: &str, vm_ids: &[&str], port: u16, tls_mode: &str) -> SpecRoute {
        SpecRoute {
            hostname: hostname.to_string(),
            app_id: "app_test".to_string(),
            vm_ids: vm_ids.iter().map(ToString::to_string).collect(),
            port,
            tls_mode: tls_mode.to_string(),
            domain_id: None,
        }
    }

    fn addresses(pairs: &[(&str, &str)]) -> HashMap<String, Ipv4Addr> {
        pairs
            .iter()
            .map(|(vm, ip)| ((*vm).to_string(), ip.parse().unwrap()))
            .collect()
    }

    #[test]
    fn backends_come_from_vm_addresses_and_skip_unaddressed_vms() {
        let routes = [route(
            "app.example.com",
            &["vm_a", "vm_b", "vm_c"],
            8080,
            "acme",
        )];
        let tables = build_tables(
            &routes,
            &addresses(&[("vm_a", "10.42.0.2"), ("vm_c", "10.42.0.4")]),
        );
        let served = tables.acme.get("app.example.com").unwrap();
        assert_eq!(
            served.backends,
            vec![
                "10.42.0.2:8080".parse::<SocketAddr>().unwrap(),
                "10.42.0.4:8080".parse::<SocketAddr>().unwrap(),
            ]
        );
        assert!(tables.tunnel.is_empty());
    }

    #[test]
    fn routes_without_a_backend_are_skipped_until_the_next_apply() {
        let routes = [route("app.example.com", &["vm_a"], 8080, "acme")];
        let tables = build_tables(&routes, &HashMap::new());
        assert!(tables.acme.is_empty());
        let tables = build_tables(&routes, &addresses(&[("vm_a", "10.42.0.2")]));
        assert_eq!(tables.acme.len(), 1);
    }

    #[test]
    fn the_table_is_split_by_tls_mode() {
        let routes = [
            route("www.example.com", &["vm_a"], 8080, "acme"),
            route("api.example.com", &["vm_b"], 3000, "tunnel"),
        ];
        let tables = build_tables(
            &routes,
            &addresses(&[("vm_a", "10.42.0.2"), ("vm_b", "10.42.0.3")]),
        );
        assert_eq!(tables.acme.len(), 1);
        assert_eq!(tables.tunnel.len(), 1);
        assert!(tables.acme.get("www.example.com").is_some());
        let tunnel = tables.tunnel.get("api.example.com").unwrap();
        assert_eq!(tunnel.backends, vec!["10.42.0.3:3000".parse().unwrap()]);
        assert_eq!(tunnel.tls, TlsMode::Tunnel);
    }

    #[test]
    fn unknown_tls_modes_and_duplicates_do_not_break_the_table() {
        let routes = [
            route("a.example.com", &["vm_a"], 80, "http"),
            route("B.Example.com", &["vm_a"], 80, "acme"),
            route("b.example.com", &["vm_a"], 80, "tunnel"),
        ];
        let tables = build_tables(&routes, &addresses(&[("vm_a", "10.42.0.2")]));
        assert_eq!(tables.acme.len(), 1);
        assert!(tables.tunnel.is_empty());
    }

    fn identity(tunnel_id: &str, token: &str, port: u16) -> TunnelIdentity {
        TunnelIdentity {
            tunnel_id: tunnel_id.to_string(),
            token: Token::new(token.to_string()),
            origin_port: port,
        }
    }

    #[test]
    fn tunnel_lifecycle_is_decided_by_presence_token_and_port() {
        let first = identity("tun_1", "run-token-one", 8081);
        assert_eq!(tunnel_action(None, None), TunnelAction::None);
        assert_eq!(tunnel_action(None, Some(&first)), TunnelAction::Start);
        assert_eq!(tunnel_action(Some(&first), None), TunnelAction::Stop);
        assert_eq!(
            tunnel_action(Some(&first), Some(&first)),
            TunnelAction::None
        );

        let rotated = identity("tun_1", "run-token-two", 8081);
        assert_eq!(
            tunnel_action(Some(&first), Some(&rotated)),
            TunnelAction::SetRun
        );
        let other_tunnel = identity("tun_2", "run-token-one", 8081);
        assert_eq!(
            tunnel_action(Some(&first), Some(&other_tunnel)),
            TunnelAction::SetRun
        );
        let other_port = identity("tun_1", "run-token-one", 8082);
        assert_eq!(
            tunnel_action(Some(&first), Some(&other_port)),
            TunnelAction::Restart
        );
        // A port change wins over a token change: the proxy must re-bind.
        let both = identity("tun_2", "run-token-two", 8082);
        assert_eq!(
            tunnel_action(Some(&first), Some(&both)),
            TunnelAction::Restart
        );
    }

    #[test]
    fn tunnel_identities_never_print_their_token() {
        let secret = "super-secret-run-token";
        let tunnel = identity("tun_1", secret, 8081);
        let rendered = format!("{tunnel:?}");
        assert!(!rendered.contains(secret), "{rendered}");
        assert!(rendered.contains("tun_1"));
        let spec = SpecTunnel {
            tunnel_id: "tun_1".to_string(),
            token: secret.to_string(),
            origin_port: 8081,
        };
        let rendered = format!("{spec:?}");
        assert!(!rendered.contains(secret), "{rendered}");
        let desired = DesiredRoutes {
            routes: Vec::new(),
            tunnel: Some(spec),
            addresses: HashMap::new(),
        };
        let rendered = format!("{desired:?}");
        assert!(!rendered.contains(secret), "{rendered}");
    }

    struct Harness {
        bridge: Arc<DnsBridge>,
        requests: mpsc::Receiver<DnsRequest>,
    }

    fn harness(timeout: Duration) -> Harness {
        let bridge = Arc::new(DnsBridge::with_timeout(timeout));
        let requests = bridge.attach();
        Harness { bridge, requests }
    }

    #[tokio::test]
    async fn a_dns_challenge_goes_out_and_resolves_on_the_matching_result() {
        let Harness {
            bridge,
            mut requests,
        } = harness(Duration::from_secs(5));
        let dns = Arc::new(SessionDns::new(Arc::clone(&bridge)));
        let session = {
            let bridge = Arc::clone(&bridge);
            tokio::spawn(async move {
                let request = requests.recv().await.unwrap();
                assert_eq!(request.challenge.action, DnsChallengeAction::Set);
                assert_eq!(request.challenge.hostname, "app.example.com");
                assert_eq!(request.challenge.value.as_deref(), Some("digest"));
                let frame = quoro_proto::wire::NodeMessage::DnsChallenge(request.challenge)
                    .to_frame(&request.id, None);
                assert_eq!(frame.kind, "dns_challenge");
                assert!(bridge.complete(
                    &frame.id,
                    DnsChallengeResult {
                        ok: true,
                        record_id: Some("record_1".to_string()),
                        message: None,
                    },
                ));
            })
        };
        let record = dns
            .create_txt("_acme-challenge.app.example.com", "digest")
            .await
            .unwrap();
        assert_eq!(record, RecordId("record_1".to_string()));
        session.await.unwrap();
    }

    #[tokio::test]
    async fn a_refused_challenge_is_an_error_with_the_primary_message() {
        let Harness {
            bridge,
            mut requests,
        } = harness(Duration::from_secs(5));
        let dns = SessionDns::new(Arc::clone(&bridge));
        let responder = {
            let bridge = Arc::clone(&bridge);
            tokio::spawn(async move {
                let request = requests.recv().await.unwrap();
                bridge.complete(
                    &request.id,
                    DnsChallengeResult {
                        ok: false,
                        record_id: None,
                        message: Some("the hostname is not a route of this node".to_string()),
                    },
                );
            })
        };
        let error = dns
            .create_txt("_acme-challenge.app.example.com", "digest")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not a route of this node"));
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn an_unanswered_challenge_times_out() {
        let Harness {
            bridge,
            requests: _requests,
        } = harness(Duration::from_millis(50));
        let dns = SessionDns::new(bridge);
        let error = dns
            .create_txt("_acme-challenge.app.example.com", "digest")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("did not answer"));
    }

    #[tokio::test]
    async fn a_challenge_without_a_session_fails_fast() {
        let bridge = Arc::new(DnsBridge::new());
        let dns = SessionDns::new(bridge);
        let error = dns
            .create_txt("_acme-challenge.app.example.com", "digest")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no session"));
    }

    #[tokio::test]
    async fn a_clear_names_the_hostname_the_set_used() {
        let Harness {
            bridge,
            mut requests,
        } = harness(Duration::from_secs(5));
        let dns = SessionDns::new(Arc::clone(&bridge));
        let responder = {
            let bridge = Arc::clone(&bridge);
            tokio::spawn(async move {
                let set = requests.recv().await.unwrap();
                bridge.complete(
                    &set.id,
                    DnsChallengeResult {
                        ok: true,
                        record_id: Some("record_9".to_string()),
                        message: None,
                    },
                );
                let clear = requests.recv().await.unwrap();
                assert_eq!(clear.challenge.action, DnsChallengeAction::Clear);
                assert_eq!(clear.challenge.hostname, "app.example.com");
                assert_eq!(clear.challenge.record_id.as_deref(), Some("record_9"));
                assert!(clear.challenge.value.is_none());
                bridge.complete(
                    &clear.id,
                    DnsChallengeResult {
                        ok: true,
                        record_id: None,
                        message: None,
                    },
                );
            })
        };
        let record = dns
            .create_txt("_acme-challenge.app.example.com", "digest")
            .await
            .unwrap();
        dns.delete_record(&record).await.unwrap();
        responder.await.unwrap();
        // A record this node never created is refused instead of guessed.
        assert!(dns.delete_record(&record).await.is_err());
    }

    #[tokio::test]
    async fn cname_writes_are_refused_on_the_node() {
        let dns = SessionDns::new(Arc::new(DnsBridge::new()));
        assert!(
            dns.upsert_cname("app.example.com", "tun.cfargotunnel.com", true)
                .await
                .is_err()
        );
        assert!(dns.delete_cname("app.example.com").await.is_err());
    }

    #[test]
    fn a_certificate_event_carries_the_envelope_the_schema_requires() {
        let body = certificate_event_body(
            "certificate.issued",
            "dom_01JC9Z0000000000000000000Q",
            "nod_01JC9Z0000000000000000000Q",
            "2026-11-02T10:15:30.000000000Z".to_string(),
            serde_json::json!({
                "domain_id": "dom_01JC9Z0000000000000000000Q",
                "hostname": "app.example.com",
                "expires_at": "2027-01-31T10:15:30Z",
            }),
        );
        assert_eq!(body["schema_version"], 1);
        assert_eq!(body["type"], "certificate.issued");
        assert_eq!(body["source"]["kind"], "node");
        assert_eq!(body["subject"]["type"], "domain");
        assert_eq!(body["data"]["hostname"], "app.example.com");
        assert!(body["id"].as_str().unwrap().starts_with("evt_"));
    }

    #[test]
    fn days_left_is_whole_days_in_the_future() {
        let now = util::now_ns();
        // A second past the boundary absorbs the sub-second truncation of the
        // timestamp text.
        let in_three = util::rfc3339_nanos(now + 3 * 24 * 60 * 60 * 1_000_000_000 + 1_000_000_000);
        assert_eq!(days_until(&in_three), Some(3));
        assert_eq!(days_until("not a time"), None);
    }

    #[test]
    fn route_statuses_fall_back_to_pending_and_not_applicable() {
        let acme = route("a.example.com", &[], 80, "acme");
        assert_eq!(status_for(&acme, None).cert_status, "pending");
        let tunnel = route("b.example.com", &[], 80, "tunnel");
        assert_eq!(status_for(&tunnel, None).cert_status, "not_applicable");
        let issued = HostStatus {
            state: CertState::Issued,
            expires_at: Some("2027-01-31T10:15:30Z".to_string()),
        };
        let status = status_for(&acme, Some(&issued));
        assert_eq!(status.cert_status, "issued");
        assert_eq!(
            status.cert_expires_at.as_deref(),
            Some("2027-01-31T10:15:30Z")
        );
    }
}
