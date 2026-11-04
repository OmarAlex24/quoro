//! Ingress: how traffic reaches an app (ADR 0024, 0059).
//!
//! One [`Ingress`] interface, two backends:
//!
//! - [`public::PublicIngress`]: the embedded reverse proxy for hosts with a
//!   public IP. It listens on 80 and 443, terminates TLS with rustls and gets
//!   its certificates from ACME through DNS-01 ([`acme`]).
//! - [`tunnel::TunnelIngress`]: Cloudflare Tunnel for hosts behind NAT. One
//!   tunnel per node; `cloudflared` dials out and forwards every hostname to
//!   the same embedded proxy on a loopback port, so routing (and, from M5a,
//!   health-aware routing, slow start and drain) lives in one place.
//!
//! The tunnel backend is split along the trust boundary of ADR 0094. The
//! account credential never leaves the primary: [`tunnel::TunnelController`]
//! creates each node's tunnel, writes its ingress rules and the proxied CNAMEs
//! there, while [`tunnel::TunnelIngress`] on the node gets only the tunnel id,
//! its run token and the origin port.
//!
//! Process placement follows the privilege split (ADR 0089, 0096): the controller
//! needs only HTTPS and the provider secret, so it runs in the unprivileged
//! control plane (`quoro serve`). Everything that binds ports, runs
//! `cloudflared` or holds node-side secrets (the run token, certificates and
//! their keys) runs in the node agent (`quoro node`), with its storage under
//! the node data directory, which the control plane cannot read.
//!
//! On a node, the [`acme::AcmeManager`]'s [`quoro_providers::DnsProvider`] is
//! expected to be backed by the primary through the `dns_challenge` message
//! (node-agent-protocol.md section 5.22): the node holds no DNS credential.
//! [`quoro_providers::cloudflare::CloudflareDns`] is used on the primary, or in
//! a single-node setup where the node also runs the primary's work.
//!
//! The caller (the node agent, epic #64) resolves `desired_state.spec.routes`
//! to backend socket addresses and calls [`Ingress::apply`] with the complete
//! table every time: there are no deltas (node-agent-protocol.md section 5.11).
//! The ingress keeps serving the last applied table until the next one.
//!
//! Certificates and their private keys live only in node-local storage
//! ([`certs::CertStore`]); they never enter replicated state or the API.

pub mod acme;
pub mod certs;
pub mod cloudflared;
pub mod proxy;
pub mod public;
pub mod route;
pub mod tunnel;

pub use route::{RetryPolicy, Route, RouteTable, TlsMode};

use std::fmt;
use std::future::Future;
use tokio::sync::broadcast;

/// Something the ingress reports to the node agent, which turns it into the
/// `certificate.*` events and the domain's `cert_status` (events.schema.json).
/// Routes carry no domain id, so events are keyed by hostname.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngressEvent {
    /// A certificate for `hostname` is being obtained or renewed
    /// (`cert_status` `pending` or `renewing`).
    CertificatePending { hostname: String, renewal: bool },
    /// A certificate was obtained or renewed (`certificate.issued`).
    /// `not_after` is RFC 3339 UTC.
    CertificateIssued { hostname: String, not_after: String },
    /// Issuance or renewal failed (`certificate.failed`); it is retried with
    /// backoff.
    CertificateFailed { hostname: String, reason: String },
    /// The current certificate expires within 14 days and renewal has not
    /// succeeded yet (`certificate.expiring`).
    CertificateExpiring { hostname: String, not_after: String },
    /// A tunnel hostname is routed: the node's connector is up and the
    /// hostname is in the proxy table. The DNS record and the tunnel's ingress
    /// rule are the primary's job (ADR 0094), so this says nothing about them.
    /// TLS terminates at the provider; the domain's `cert_status` is
    /// `not_applicable` in tunnel mode.
    TunnelRouteReady { hostname: String },
    /// Routing a tunnel hostname failed; retried on the next apply or tick.
    TunnelRouteFailed { hostname: String, reason: String },
}

/// Why an ingress operation failed.
#[derive(Debug)]
pub enum IngressError {
    /// The route table is invalid (bad hostname, duplicate, wrong TLS mode for
    /// this backend).
    InvalidRoutes(String),
    /// Binding a listener or touching node-local storage failed.
    Io(std::io::Error),
    /// An external provider failed.
    Provider(quoro_providers::ProviderError),
    /// Anything else, with a message that never contains a secret.
    Other(String),
}

impl fmt::Display for IngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoutes(m) => write!(f, "invalid routes: {m}"),
            Self::Io(e) => write!(f, "i/o: {e}"),
            Self::Provider(e) => write!(f, "{e}"),
            Self::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for IngressError {}

impl From<std::io::Error> for IngressError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<quoro_providers::ProviderError> for IngressError {
    fn from(e: quoro_providers::ProviderError) -> Self {
        Self::Provider(e)
    }
}

/// The interface both backends implement (ADR 0024).
pub trait Ingress: Send + Sync + 'static {
    /// Replaces the route table with `table`, the complete set of hostnames
    /// this node serves. Routing switches atomically; certificates and tunnel
    /// records are reconciled in the background and reported as
    /// [`IngressEvent`]s. Returns an error only for a table this backend cannot
    /// serve; the previous table then stays in force.
    fn apply(&self, table: RouteTable) -> impl Future<Output = Result<(), IngressError>> + Send;

    /// Subscribes to ingress events.
    fn subscribe(&self) -> broadcast::Receiver<IngressEvent>;

    /// Stops listeners and child processes. Routes are not withdrawn from
    /// external providers, so a restart serves the same hostnames again.
    fn shutdown(&self) -> impl Future<Output = ()> + Send;
}
