//! Certificates for the public proxy: ACME issuance and renewal over DNS-01
//! (ADR 0024, 0059; spike #65).
//!
//! One [`AcmeManager`] holds the account, the hostnames that need certificates
//! and the renewal policy. Certificates and their keys live only in the
//! node-local [`CertStore`]; the account key lives in `state_dir/account-<directory>.json`
//! (`0600` in a `0700` directory). Nothing here is replicated or reachable
//! through the API, and no secret is ever logged or put in an error.
//!
//! Issuance runs one order at a time: the worker walks the hostnames, decides
//! with [`renewal_due`] whether each one needs a certificate, and calls
//! [`AcmeManager::ensure`]. A failed hostname waits [`backoff`] before the next
//! attempt instead of hammering the CA.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, CertificateIdentifier, ChallengeType,
    Error as AcmeError, Identifier, NewAccount, NewOrder, Order, OrderStatus, RetryPolicy,
};
use quoro_providers::{DnsProvider, RecordId};
use tokio::sync::{Notify, broadcast, watch};

use crate::IngressEvent;
use crate::certs::{CertStore, StoredCert};

/// Let's Encrypt production directory.
pub const LETS_ENCRYPT_PRODUCTION: &str = "https://acme-v02.api.letsencrypt.org/directory";
/// Let's Encrypt staging directory, for tests and dry runs.
pub const LETS_ENCRYPT_STAGING: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";

/// Where the worker runs and how patient it is with DNS propagation.
#[derive(Clone, Debug)]
pub struct AcmeConfig {
    /// The ACME directory URL ([`LETS_ENCRYPT_PRODUCTION`] or a test server).
    pub directory_url: String,
    /// Account contacts (`mailto:...`); may be empty.
    pub contact: Vec<String>,
    /// Node-local state; holds `account-<directory>.json` (`0600` in a `0700` directory).
    pub state_dir: PathBuf,
    /// An extra CA to trust for the directory connection (tests with Pebble).
    pub root_pem: Option<PathBuf>,
    /// How often the worker reconsiders every hostname.
    pub check_interval: Duration,
    /// How long a DNS-01 TXT record may take to become visible.
    pub propagation_timeout: Duration,
    /// How often to look for the TXT record while waiting.
    pub propagation_poll: Duration,
}

impl AcmeConfig {
    /// A configuration with the production defaults: renew every 6 h, wait up
    /// to 180 s for DNS with a 5 s poll.
    #[must_use]
    pub fn new(directory_url: impl Into<String>, state_dir: PathBuf) -> Self {
        Self {
            directory_url: directory_url.into(),
            contact: Vec::new(),
            state_dir,
            root_pem: None,
            check_interval: Duration::from_secs(6 * 60 * 60),
            propagation_timeout: Duration::from_secs(180),
            propagation_poll: Duration::from_secs(5),
        }
    }
}

/// Per-hostname retry state of the worker.
#[derive(Clone, Copy, Debug, Default)]
struct HostState {
    consecutive_failures: u32,
    /// Unix seconds before which the hostname is not attempted again.
    next_attempt_at: i64,
}

struct Inner<D: DnsProvider> {
    config: AcmeConfig,
    dns: Arc<D>,
    store: CertStore,
    events: broadcast::Sender<IngressEvent>,
    /// The hostnames that need certificates. A std lock, never held across an
    /// `await`.
    hostnames: Mutex<BTreeSet<String>>,
    notify: Notify,
    account: tokio::sync::Mutex<Option<Account>>,
    state: Mutex<BTreeMap<String, HostState>>,
}

/// Obtains and renews certificates for this node. Cheap to clone.
pub struct AcmeManager<D: DnsProvider> {
    inner: Arc<Inner<D>>,
}

impl<D: DnsProvider> Clone for AcmeManager<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: DnsProvider> std::fmt::Debug for AcmeManager<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcmeManager")
            .field("directory_url", &self.inner.config.directory_url)
            .field("state_dir", &self.inner.config.state_dir)
            .finish_non_exhaustive()
    }
}

impl<D: DnsProvider> AcmeManager<D> {
    /// Builds a manager. The worker answers DNS-01 challenges through `dns`,
    /// writes certificates through `store` and reports events through `events`.
    #[must_use]
    pub fn new(
        config: AcmeConfig,
        dns: Arc<D>,
        store: CertStore,
        events: broadcast::Sender<IngressEvent>,
    ) -> Self {
        // The directory connection (and the account key) use rustls with the
        // `ring` provider (ADR 0080). Installing it twice is not an error.
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            inner: Arc::new(Inner {
                config,
                dns,
                store,
                events,
                hostnames: Mutex::new(BTreeSet::new()),
                notify: Notify::new(),
                account: tokio::sync::Mutex::new(None),
                state: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Replaces the set of hostnames that need certificates and wakes the
    /// worker so new names are issued right away.
    pub fn set_hostnames(&self, hostnames: BTreeSet<String>) {
        {
            let mut current = self
                .inner
                .hostnames
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *current = hostnames;
        }
        self.inner.notify.notify_one();
    }

    /// Runs the worker until `shutdown` turns true: on every wake-up or every
    /// `check_interval`, each hostname is considered and issued or renewed
    /// sequentially, one order at a time.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        if *shutdown.borrow() {
            return;
        }
        loop {
            let hostnames = {
                let current = self
                    .inner
                    .hostnames
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                current.iter().cloned().collect::<Vec<_>>()
            };
            self.forget_removed(&hostnames);
            for hostname in &hostnames {
                if *shutdown.borrow() {
                    return;
                }
                if !self.due(hostname).await {
                    continue;
                }
                match self.ensure(hostname).await {
                    Ok(_) => self.record_success(hostname),
                    // The failure was reported as `CertificateFailed` already.
                    Err(_) => self.record_failure(hostname),
                }
            }
            let sleep = self.next_wake(&hostnames);
            tokio::select! {
                () = self.inner.notify.notified() => {}
                () = tokio::time::sleep(sleep) => {}
                _ = shutdown.changed() => {}
            }
            if *shutdown.borrow() {
                return;
            }
        }
    }

    /// Issues or renews one hostname now (used by [`Self::run`] and by tests).
    ///
    /// # Errors
    ///
    /// The hostname is not a valid DNS name, the account cannot be created or
    /// loaded, the CA refuses the order, DNS propagation times out, or the
    /// certificate cannot be stored.
    pub async fn ensure(&self, hostname: &str) -> Result<StoredCert, crate::IngressError> {
        if !crate::route::valid_hostname(hostname) {
            return Err(crate::IngressError::InvalidRoutes(format!(
                "invalid hostname {hostname:?}"
            )));
        }
        let stored = self.inner.store.get(hostname);
        let renewal = stored.is_some();
        self.emit(IngressEvent::CertificatePending {
            hostname: hostname.to_string(),
            renewal,
        });
        match self.issue(hostname, stored.as_ref()).await {
            Ok(cert) => {
                self.emit(IngressEvent::CertificateIssued {
                    hostname: hostname.to_string(),
                    not_after: rfc3339(cert.not_after),
                });
                Ok(cert)
            }
            Err(e) => {
                let reason = e.to_string();
                self.emit(IngressEvent::CertificateFailed {
                    hostname: hostname.to_string(),
                    reason: reason.clone(),
                });
                if let Some(old) = &stored
                    && old.not_after - unix_now() < 14 * 24 * 60 * 60
                {
                    self.emit(IngressEvent::CertificateExpiring {
                        hostname: hostname.to_string(),
                        not_after: rfc3339(old.not_after),
                    });
                }
                Err(crate::IngressError::Other(reason))
            }
        }
    }

    /// Walks the authorizations, answers DNS-01, and returns the stored
    /// certificate. Every TXT record created is deleted on every path.
    async fn issue(
        &self,
        hostname: &str,
        stored: Option<&StoredCert>,
    ) -> Result<StoredCert, crate::IngressError> {
        let account = self.account().await?;
        let replaces = stored.and_then(|cert| {
            CertificateIdentifier::try_from(&cert.leaf_der)
                .ok()
                .map(CertificateIdentifier::into_owned)
        });
        let identifiers = [Identifier::Dns(hostname.to_string())];
        let mut order = match &replaces {
            Some(id) => {
                let request = NewOrder::new(&identifiers).replaces(id.clone());
                match account.new_order(&request).await {
                    Ok(order) => order,
                    Err(e) => {
                        // A CA that does not know `replaces` (or refuses this
                        // particular one) still gets one plain order.
                        tracing::warn!(%hostname, error = %e, "order naming the replaced certificate was refused; retrying without it");
                        account
                            .new_order(&NewOrder::new(&identifiers))
                            .await
                            .map_err(acme_error)?
                    }
                }
            }
            None => account
                .new_order(&NewOrder::new(&identifiers))
                .await
                .map_err(acme_error)?,
        };

        let mut records = Vec::new();
        let answered = self
            .answer_challenges(hostname, &mut order, &mut records)
            .await;
        let ready = match answered {
            Ok(()) => order.poll_ready(&retry_policy()).await.map_err(acme_error),
            Err(e) => Err(e),
        };
        self.delete_records(hostname, &records).await;
        let status = ready?;
        if status != OrderStatus::Ready {
            return Err(crate::IngressError::Other(format!(
                "acme: order for {hostname} ended {status:?}"
            )));
        }

        // A fresh key for every issuance; it never leaves this function except
        // into the store.
        let key = rcgen::KeyPair::generate()
            .map_err(|e| crate::IngressError::Other(format!("key generation: {e}")))?;
        let mut params = rcgen::CertificateParams::new(vec![hostname.to_string()])
            .map_err(|e| crate::IngressError::Other(format!("certificate request: {e}")))?;
        // rcgen's default subject is CN "rcgen self signed cert", which Let's
        // Encrypt rejects as an identifier; the name belongs in the SAN only.
        params.distinguished_name = rcgen::DistinguishedName::new();
        let csr = params
            .serialize_request(&key)
            .map_err(|e| crate::IngressError::Other(format!("certificate request: {e}")))?;
        order.finalize_csr(csr.der()).await.map_err(acme_error)?;
        let chain_pem = order
            .poll_certificate(&retry_policy())
            .await
            .map_err(acme_error)?;
        let key_pem = key.serialize_pem();
        let cert = self
            .inner
            .store
            .store(hostname, &chain_pem, &key_pem)
            .map_err(|e| crate::IngressError::Other(format!("storing the certificate: {e}")))?;
        tracing::info!(%hostname, not_after = cert.not_after, "certificate issued");
        Ok(cert)
    }

    /// Creates one TXT record per pending authorization, waits for it to be
    /// visible and tells the CA the challenge is ready.
    async fn answer_challenges(
        &self,
        hostname: &str,
        order: &mut Order,
        records: &mut Vec<RecordId>,
    ) -> Result<(), crate::IngressError> {
        let mut authorizations = order.authorizations();
        while let Some(authz) = authorizations.next().await {
            let mut authz = authz.map_err(acme_error)?;
            match authz.status {
                AuthorizationStatus::Pending => {}
                AuthorizationStatus::Valid => continue,
                other => {
                    return Err(crate::IngressError::Other(format!(
                        "acme: authorization for {hostname} is {other:?}"
                    )));
                }
            }
            let mut challenge = authz.challenge(ChallengeType::Dns01).ok_or_else(|| {
                crate::IngressError::Other(format!(
                    "acme: the CA offered no DNS-01 challenge for {hostname}"
                ))
            })?;
            let name = format!("_acme-challenge.{hostname}");
            let value = challenge.key_authorization().dns_value();
            let record = self.inner.dns.create_txt(&name, &value).await?;
            records.push(record);
            self.wait_visible(&name, &value).await?;
            challenge.set_ready().await.map_err(acme_error)?;
        }
        Ok(())
    }

    /// Polls the provider until the TXT record is visible or the propagation
    /// timeout expires.
    async fn wait_visible(&self, name: &str, value: &str) -> Result<(), crate::IngressError> {
        let started = tokio::time::Instant::now();
        loop {
            match self.inner.dns.txt_visible(name, value).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e) => tracing::debug!(%name, error = %e, "TXT lookup failed; retrying"),
            }
            let timeout = self.inner.config.propagation_timeout;
            if started.elapsed() >= timeout {
                return Err(crate::IngressError::Other(format!(
                    "dns: {name} did not become visible within {timeout:?}"
                )));
            }
            tokio::time::sleep(self.inner.config.propagation_poll).await;
        }
    }

    async fn delete_records(&self, hostname: &str, records: &[RecordId]) {
        for id in records {
            if let Err(e) = self.inner.dns.delete_record(id).await {
                tracing::warn!(
                    %hostname,
                    record = %id.0,
                    error = %e,
                    "could not delete the DNS-01 TXT record"
                );
            }
        }
    }

    /// The account from `state_dir/account-<directory>.json`, created and saved on first
    /// use.
    async fn account(&self) -> Result<Account, crate::IngressError> {
        let mut slot = self.inner.account.lock().await;
        if let Some(account) = slot.as_ref() {
            return Ok(account.clone());
        }
        let config = &self.inner.config;
        let builder = match &config.root_pem {
            Some(root) => Account::builder_with_root(root).map_err(acme_error)?,
            None => Account::builder().map_err(acme_error)?,
        };
        let path = config
            .state_dir
            .join(account_file_name(&config.directory_url));
        let account = if path.exists() {
            let bytes = tokio::fs::read(&path).await?;
            let credentials: AccountCredentials = serde_json::from_slice(&bytes)
                .map_err(|e| crate::IngressError::Other(format!("account credentials: {e}")))?;
            builder
                .from_credentials(credentials)
                .await
                .map_err(acme_error)?
        } else {
            let contact = config
                .contact
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let (account, credentials) = builder
                .create(
                    &NewAccount {
                        contact: &contact,
                        terms_of_service_agreed: true,
                        only_return_existing: false,
                    },
                    config.directory_url.clone(),
                    None,
                )
                .await
                .map_err(acme_error)?;
            let json = serde_json::to_vec(&credentials)
                .map_err(|e| crate::IngressError::Other(format!("account credentials: {e}")))?;
            save_account(&path, &json)?;
            account
        };
        tracing::debug!(account = %account.id(), "ACME account ready");
        *slot = Some(account.clone());
        Ok(account)
    }

    /// Whether the worker should call [`Self::ensure`] for `hostname` now.
    async fn due(&self, hostname: &str) -> bool {
        let now = unix_now();
        let waiting = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.get(hostname).is_some_and(|s| now < s.next_attempt_at)
        };
        if waiting {
            return false;
        }
        let Some(cert) = self.inner.store.get(hostname) else {
            return true;
        };
        let window = self.ari_window_start(&cert).await;
        renewal_due(now, cert.not_before, cert.not_after, window)
    }

    /// The start of the CA's suggested renewal window, when it offers one.
    async fn ari_window_start(&self, cert: &StoredCert) -> Option<i64> {
        let id = CertificateIdentifier::try_from(&cert.leaf_der)
            .ok()?
            .into_owned();
        let account = match self.account().await {
            Ok(account) => account,
            Err(e) => {
                tracing::debug!(error = %e, "no ACME account for renewal information");
                return None;
            }
        };
        match account.renewal_info(&id).await {
            Ok((info, _)) => Some(info.suggested_window.start.unix_timestamp()),
            Err(e) => {
                tracing::debug!(error = %e, "no ARI window; using the lifetime rule");
                None
            }
        }
    }

    fn record_success(&self, hostname: &str) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.remove(hostname);
    }

    fn record_failure(&self, hostname: &str) {
        let now = unix_now();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.entry(hostname.to_string()).or_default();
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.next_attempt_at =
            now.saturating_add(backoff(entry.consecutive_failures).as_secs() as i64);
    }

    /// Drops the retry state of hostnames no longer served. Certificates on
    /// disk stay; a name that comes back is reconsidered from scratch.
    /// How long the worker sleeps: until the earliest backoff of a failing
    /// hostname expires, but never longer than `check_interval` and never less
    /// than one second.
    fn next_wake(&self, hostnames: &[String]) -> Duration {
        let now = unix_now();
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let earliest = hostnames
            .iter()
            .filter_map(|h| state.get(h))
            .map(|s| s.next_attempt_at.saturating_sub(now).max(1))
            .min();
        let interval = self.inner.config.check_interval;
        earliest.map_or(interval, |secs| {
            Duration::from_secs(u64::try_from(secs).unwrap_or(1)).min(interval)
        })
    }

    fn forget_removed(&self, hostnames: &[String]) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.retain(|hostname, _| hostnames.iter().any(|h| h == hostname));
    }

    fn emit(&self, event: IngressEvent) {
        // No subscribers is not an error: events are advisory.
        let _ = self.inner.events.send(event);
    }
}

/// Whether a certificate needs renewal at `now` (Unix seconds).
///
/// With an ARI window, renewal is due once the window has started. Without one,
/// it is due when less than a third of the lifetime remains. An expired
/// certificate, or one within 24 h of `not_after`, is always due.
#[must_use]
pub fn renewal_due(
    now: i64,
    not_before: i64,
    not_after: i64,
    ari_window_start: Option<i64>,
) -> bool {
    if now >= not_after {
        return true;
    }
    if not_after - now < 24 * 60 * 60 {
        return true;
    }
    if let Some(start) = ari_window_start {
        return now >= start;
    }
    let lifetime = not_after - not_before;
    (not_after - now) * 3 < lifetime
}

/// How long to wait after `consecutive_failures` failures: 60 s, doubling, up
/// to 6 h.
#[must_use]
pub fn backoff(consecutive_failures: u32) -> Duration {
    const CAP_SECS: u64 = 6 * 60 * 60;
    let doublings = consecutive_failures.saturating_sub(1).min(16);
    let secs = 60u64 << doublings;
    Duration::from_secs(secs.min(CAP_SECS))
}

/// `not_after` as RFC 3339 UTC, for events.
fn rfc3339(unix: i64) -> String {
    let Ok(t) = time::OffsetDateTime::from_unix_timestamp(unix) else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// One account file per ACME directory, so switching from staging to
/// production (or to another CA) creates a new account instead of reusing
/// credentials bound to the old directory: `account-<host and path>.json`.
fn account_file_name(directory_url: &str) -> String {
    let rest = directory_url
        .split_once("://")
        .map_or(directory_url, |(_, rest)| rest);
    let slug: String = rest
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("account-{slug}.json")
}

fn unix_now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn retry_policy() -> RetryPolicy {
    RetryPolicy::default().timeout(Duration::from_secs(300))
}

fn acme_error(e: AcmeError) -> crate::IngressError {
    crate::IngressError::Other(format!("acme: {e}"))
}

/// Writes the account credentials (which hold the account private key)
/// atomically, `0600`, in a `0700` directory.
fn save_account(path: &Path, contents: &[u8]) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let tmp = path.with_extension("json.tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod account_name_tests {
    #[test]
    fn account_files_differ_per_directory() {
        let staging = super::account_file_name(super::LETS_ENCRYPT_STAGING);
        let production = super::account_file_name(super::LETS_ENCRYPT_PRODUCTION);
        assert_ne!(staging, production);
        assert_eq!(
            staging,
            "account-acme-staging-v02.api.letsencrypt.org_directory.json"
        );
        assert!(!super::account_file_name("https://x/../../etc").contains('/'));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_providers::ProviderError;

    const DAY: i64 = 24 * 60 * 60;

    /// A provider that fails if it is ever asked for a record.
    struct NoDns;

    impl DnsProvider for NoDns {
        async fn create_txt(&self, _: &str, _: &str) -> Result<RecordId, ProviderError> {
            Err(ProviderError::Transport("no DNS in this test".into()))
        }

        async fn delete_record(&self, _: &RecordId) -> Result<(), ProviderError> {
            Ok(())
        }

        async fn upsert_cname(&self, _: &str, _: &str, _: bool) -> Result<RecordId, ProviderError> {
            Err(ProviderError::Transport("no DNS in this test".into()))
        }

        async fn delete_cname(&self, _: &str, _: &str) -> Result<(), ProviderError> {
            Ok(())
        }

        async fn txt_visible(&self, _: &str, _: &str) -> Result<bool, ProviderError> {
            Ok(false)
        }
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// An ACME directory nothing listens on: the account cannot be created.
    fn unreachable_manager(
        dir: &Path,
        events: broadcast::Sender<IngressEvent>,
    ) -> AcmeManager<NoDns> {
        let mut config = AcmeConfig::new("http://127.0.0.1:9/directory", dir.join("state"));
        config.check_interval = Duration::from_millis(50);
        AcmeManager::new(
            config,
            Arc::new(NoDns),
            CertStore::open(&dir.join("certs")).unwrap(),
            events,
        )
    }

    #[tokio::test]
    async fn an_unreachable_directory_reports_a_failure_and_stores_nothing() {
        let dir = test_dir("acme-unreachable");
        let (events, mut receiver) = broadcast::channel(16);
        let manager = unreachable_manager(&dir, events);
        let result = manager.ensure("app.example.com").await;
        assert!(result.is_err());
        let seen: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
        assert!(
            matches!(&seen[0], IngressEvent::CertificatePending { hostname, renewal: false } if hostname == "app.example.com"),
            "{seen:?}"
        );
        assert!(
            matches!(&seen[1], IngressEvent::CertificateFailed { hostname, .. } if hostname == "app.example.com"),
            "{seen:?}"
        );
        assert!(manager.inner.store.get("app.example.com").is_none());
        assert!(
            std::fs::read_dir(dir.join("state")).map_or(true, |mut d| d.next().is_none()),
            "no account file is written when the directory is unreachable"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn the_worker_stops_promptly_on_shutdown() {
        let dir = test_dir("acme-run-shutdown");
        let (events, _receiver) = broadcast::channel(16);
        let manager = unreachable_manager(&dir, events);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.run(shutdown_rx).await })
        };
        manager.set_hostnames(BTreeSet::from(["app.example.com".to_string()]));
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("the worker exits when shutdown turns true")
            .unwrap();
        // The first attempt failed, so the hostname is backing off.
        let state = manager
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            state
                .get("app.example.com")
                .is_some_and(|s| s.consecutive_failures >= 1)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ari_window_decides_when_the_server_offers_one() {
        let start = 1_000_000;
        // Before the window: not due, even though the lifetime rule would be.
        assert!(!renewal_due(
            start - 1,
            start - 80 * DAY,
            start + 10 * DAY,
            Some(start)
        ));
        // Inside the window: due.
        assert!(renewal_due(
            start,
            start - 80 * DAY,
            start + 10 * DAY,
            Some(start)
        ));
        assert!(renewal_due(
            start + 5,
            start - 80 * DAY,
            start + 10 * DAY,
            Some(start)
        ));
    }

    #[test]
    fn without_a_window_a_third_of_the_lifetime_remains() {
        let not_before = 2_000_000;
        let not_after = not_before + 90 * DAY;
        // 60 days remain of 90: exactly one third, still fine.
        assert!(!renewal_due(
            not_after - 60 * DAY,
            not_before,
            not_after,
            None
        ));
        // 29 days remain: below one third.
        assert!(renewal_due(
            not_after - 29 * DAY,
            not_before,
            not_after,
            None
        ));
        // Just issued.
        assert!(!renewal_due(not_before, not_before, not_after, None));
    }

    #[test]
    fn expired_or_within_a_day_is_always_due() {
        let not_before = 3_000_000;
        let not_after = not_before + 90 * DAY;
        // A healthy lifetime, but inside the last 24 h.
        assert!(renewal_due(
            not_after - DAY + 1,
            not_before,
            not_after,
            None
        ));
        // An ARI window far in the future does not postpone it.
        assert!(renewal_due(
            not_after - 3600,
            not_before,
            not_after,
            Some(not_after + DAY)
        ));
        // Expired.
        assert!(renewal_due(not_after, not_before, not_after, None));
        assert!(renewal_due(
            not_after + 1,
            not_before,
            not_after,
            Some(not_after + DAY)
        ));
    }

    #[test]
    fn backoff_doubles_from_a_minute_and_caps_at_six_hours() {
        assert_eq!(backoff(0), Duration::from_secs(60));
        assert_eq!(backoff(1), Duration::from_secs(60));
        assert_eq!(backoff(2), Duration::from_secs(120));
        assert_eq!(backoff(3), Duration::from_secs(240));
        assert_eq!(backoff(4), Duration::from_secs(480));
        assert_eq!(backoff(5), Duration::from_secs(960));
        assert_eq!(backoff(6), Duration::from_secs(1920));
        assert_eq!(backoff(7), Duration::from_secs(3840));
        assert_eq!(backoff(8), Duration::from_secs(7680));
        assert_eq!(backoff(9), Duration::from_secs(15_360));
        assert_eq!(backoff(10), Duration::from_secs(21_600));
        assert_eq!(backoff(11), Duration::from_secs(21_600));
        assert_eq!(backoff(u32::MAX), Duration::from_secs(21_600));
    }

    #[test]
    fn not_after_is_formatted_as_rfc3339() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
