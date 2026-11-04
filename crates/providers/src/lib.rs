//! Provider interfaces: what Quoro asks of external services (ADR 0025), with
//! Cloudflare as the first implementation, not the only one.
//!
//! M2 needs two of them: [`DnsProvider`] (DNS-01 challenge records and the
//! records that point a hostname at the cluster) and the Cloudflare Tunnel API
//! ([`cloudflare::Cloudflare`] tunnel methods) for hosts behind NAT
//! (ADR 0024, 0055). Load balancers, object storage and lease arbiters come
//! with M5a/M5b (architecture.md section 11).
//!
//! Credentials reach this crate as [`Token`] values that the caller decrypted
//! from the human-only providers resource (ADR 0051); this crate never stores
//! them and never prints them.

pub mod cloudflare;
pub mod object_store;
pub mod s3;

use std::fmt;
use std::future::Future;
use zeroize::Zeroizing;

/// An API credential. It is wiped from memory on drop and never printed.
#[derive(Clone)]
pub struct Token(Zeroizing<String>);

impl Token {
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    /// The token value, for an `Authorization` header or a child process's
    /// environment. Never log it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token([redacted])")
    }
}

/// Why a provider call failed. Messages never contain a credential.
#[derive(Debug)]
pub enum ProviderError {
    /// The provider refused the credential (HTTP 401/403 or an auth error code).
    Unauthorized(String),
    /// The addressed object does not exist.
    NotFound(String),
    /// The change would touch an object Quoro did not create (for example a
    /// DNS record of the same name made by hand).
    Conflict(String),
    /// The provider answered with an error.
    Api { status: u16, message: String },
    /// The request did not complete (connect, TLS, timeout, bad body).
    Transport(String),
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized(m) => write!(f, "provider refused the credential: {m}"),
            Self::NotFound(m) => write!(f, "not found: {m}"),
            Self::Conflict(m) => write!(f, "conflict: {m}"),
            Self::Api { status, message } => write!(f, "provider error {status}: {message}"),
            Self::Transport(m) => write!(f, "provider unreachable: {m}"),
        }
    }
}

impl std::error::Error for ProviderError {}

/// Id of a DNS record at the provider.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordId(pub String);

/// DNS records Quoro manages in one zone (architecture.md section 11).
///
/// Names are fully qualified without the trailing dot
/// (`_acme-challenge.app.example.com`). Implementations tag every record they
/// create so that [`DnsProvider::delete_record`] and the CNAME methods never
/// touch a record Quoro did not create.
pub trait DnsProvider: Send + Sync + 'static {
    /// Creates a TXT record (ACME DNS-01). Several TXT records may share a name.
    fn create_txt(
        &self,
        name: &str,
        value: &str,
    ) -> impl Future<Output = Result<RecordId, ProviderError>> + Send;

    /// Deletes a record created by this provider. Deleting a record that is
    /// already gone is not an error.
    fn delete_record(
        &self,
        id: &RecordId,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send;

    /// Points `name` at `target` with a CNAME, creating or updating it.
    /// `proxied` asks the provider to proxy the traffic (Cloudflare orange
    /// cloud; required for a tunnel hostname). Refuses to overwrite a record of
    /// `name` that Quoro did not create.
    fn upsert_cname(
        &self,
        name: &str,
        target: &str,
        proxied: bool,
    ) -> impl Future<Output = Result<RecordId, ProviderError>> + Send;

    /// Removes the CNAME of `name` if Quoro created it; otherwise does nothing.
    fn delete_cname(&self, name: &str) -> impl Future<Output = Result<(), ProviderError>> + Send;

    /// Whether `name` currently resolves to a TXT record containing `value`
    /// at the provider's authoritative servers or a public resolver, used to
    /// wait for propagation before answering a DNS-01 challenge.
    fn txt_visible(
        &self,
        name: &str,
        value: &str,
    ) -> impl Future<Output = Result<bool, ProviderError>> + Send;
}
