// SPDX-License-Identifier: AGPL-3.0-only
//! Backup repositories, volume backups and their seals (`backup-format.md`,
//! ADR 0147).
//!
//! The primary owns everything in this module except the data path: it writes
//! the repository config object ([`repo`]), queues and times out jobs
//! ([`dispatch`]), seals what a node reports ([`seal`]) and prunes or garbage
//! collects the objects a repository no longer needs ([`prune`]). The node
//! reads the volume and uploads chunks and the manifest itself; the crypto and
//! the object layout live in `quoro-backup`.
//!
//! Key handling is the security-critical part: `repo_key_enc` is the only
//! stored copy of the repository key, encrypted under the cluster
//! data-encryption key, and the recovery key is never stored at all. The
//! encrypted column holds the 64 hex characters of the key rather than its raw
//! bytes, because [`crate::crypto::DataKey::decrypt`] returns text; the hex is
//! decoded back to the 32 bytes at the one place that derives source keys.

pub mod cost;
pub mod dispatch;
pub mod prune;
pub mod repo;
pub mod restore;
pub mod seal;

use crate::crypto::DataKey;
use crate::error::ApiError;
use quoro_backup::RepoKey;
use quoro_providers::object_store::{ObjectInfo, ObjectStore, ObjectStoreError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) use seal::apply_status;

/// The `aad_context` of `backup_repositories.repo_key_enc` (D-0050).
pub(crate) const REPO_KEY_CONTEXT: &str = "backup_repositories.repo_key_enc";

/// The `aad_context` of `providers.secret_enc`.
pub(crate) const PROVIDER_SECRET_CONTEXT: &str = "providers.secret_enc";

/// How long a reported age the garbage collector may still delete: 48 hours
/// (`backup-format.md` section 9), longer than the 24-hour backup timeout.
pub const GC_GRACE: Duration = Duration::from_secs(48 * 60 * 60);

/// How often a garbage-collection sweep may run: once a day.
pub const GC_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a backup may stay non-terminal: 24 hours
/// (`backup-format.md` section 7.1).
pub const BACKUP_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// The four object-store operations the backups code needs.
///
/// It exists because [`ObjectStore`] returns `impl Future`, which cannot be
/// boxed into one shared handle; the blanket impl through [`AnyStore`] keeps
/// production on `S3Client` and tests on `MemoryStore`.
pub trait BucketStore: Send + Sync {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>>;

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>>;

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>>;

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>>;
}

/// The adapter from [`ObjectStore`] to [`BucketStore`].
pub struct AnyStore<S>(pub S);

impl<S: ObjectStore> BucketStore for AnyStore<S> {
    fn put_new<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(async move { self.0.put_new(key, body, None).await.map(|_| ()) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.get(key))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.list(prefix))
    }

    fn delete<'a>(
        &'a self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObjectStoreError>> + Send + 'a>> {
        Box::pin(self.0.delete(key))
    }
}

/// One repository's bucket, as the backups code addresses it.
pub type Bucket = Arc<dyn BucketStore>;

/// Everything needed to open the bucket of one repository.
///
/// `secret_access_key` is decrypted from the provider row for this call only;
/// it is never logged and never stored in a struct that outlives the request.
pub struct RepositoryTarget {
    /// `brp_` id of the repository.
    pub repository_id: String,
    /// `<prefix>/<cluster_id>/`, the prefix every repository key is relative
    /// to (`backup-format.md` section 2).
    pub root: String,
    /// `https://` origin of the provider.
    pub endpoint: String,
    /// S3 region, `auto` when the provider does not name one.
    pub region: String,
    /// Bucket name.
    pub bucket: String,
    /// Access key id of the provider.
    pub access_key_id: String,
    /// Secret access key of the provider.
    pub secret_access_key: crate::crypto::Secret,
}

/// Opens the bucket of a repository. Production builds an `S3Client`; tests
/// install a factory that answers a `MemoryStore`.
pub trait BucketFactory: Send + Sync {
    /// The bucket for `target`.
    ///
    /// # Errors
    ///
    /// An internal error when the client cannot be built (a bad endpoint, for
    /// example).
    fn open(&self, target: &RepositoryTarget) -> Result<Bucket, ApiError>;
}

/// The production factory: one `S3Client` per repository (`backup-format.md`
/// section 10).
pub struct S3Buckets;

impl BucketFactory for S3Buckets {
    fn open(&self, target: &RepositoryTarget) -> Result<Bucket, ApiError> {
        let config = quoro_providers::s3::S3Config {
            endpoint: target.endpoint.clone(),
            region: target.region.clone(),
            bucket: target.bucket.clone(),
            access_key_id: target.access_key_id.clone(),
            secret_access_key: zeroize::Zeroizing::new(
                target.secret_access_key.expose().to_string(),
            ),
        };
        let client = quoro_providers::s3::S3Client::new(config)
            .map_err(|error| ApiError::internal(&error))?;
        Ok(Arc::new(AnyStore(client)))
    }
}

/// The in-memory state every backup loop and handler shares: the bucket
/// factory, the garbage-collection flag and the last sweep time.
///
/// It is cloned into [`crate::AppState`] and the session hub, so a test that
/// replaces the factory before or after start reaches both the HTTP handlers
/// and the node session.
#[derive(Clone)]
pub struct Backups(Arc<BackupsInner>);

struct BackupsInner {
    buckets: Mutex<Arc<dyn BucketFactory>>,
    sweeping: AtomicBool,
    last_sweep: Mutex<Option<time::OffsetDateTime>>,
}

impl Default for Backups {
    fn default() -> Self {
        Self::s3()
    }
}

impl Backups {
    /// The production runtime, with an `S3Client` per repository.
    #[must_use]
    pub fn s3() -> Self {
        Self(Arc::new(BackupsInner {
            buckets: Mutex::new(Arc::new(S3Buckets)),
            sweeping: AtomicBool::new(false),
            last_sweep: Mutex::new(None),
        }))
    }

    /// Replaces the bucket factory (tests install a `MemoryStore`).
    ///
    /// # Panics
    ///
    /// Panics if the factory lock is poisoned.
    pub fn set_factory(&self, factory: Arc<dyn BucketFactory>) {
        *self.0.buckets.lock().expect("bucket factory lock") = factory;
    }

    /// Whether a garbage-collection sweep is running (`backup-format.md`
    /// section 9: no dispatch while one does).
    #[must_use]
    pub fn is_sweeping(&self) -> bool {
        self.0.sweeping.load(Ordering::SeqCst)
    }

    pub(crate) fn begin_sweep(&self) -> SweepGuard<'_> {
        self.0.sweeping.store(true, Ordering::SeqCst);
        SweepGuard(self)
    }

    fn last_sweep(&self) -> Option<time::OffsetDateTime> {
        *self.0.last_sweep.lock().expect("sweep time lock")
    }

    fn note_sweep(&self, at: time::OffsetDateTime) {
        *self.0.last_sweep.lock().expect("sweep time lock") = Some(at);
    }

    pub(crate) fn bucket_factory(&self) -> Arc<dyn BucketFactory> {
        Arc::clone(&self.0.buckets.lock().expect("bucket factory lock"))
    }
}

/// Clears the sweeping flag however the sweep ends.
pub(crate) struct SweepGuard<'a>(&'a Backups);

impl Drop for SweepGuard<'_> {
    fn drop(&mut self) {
        self.0.0.sweeping.store(false, Ordering::SeqCst);
    }
}

/// The repository key of `repository_id`, decoded from the encrypted column.
///
/// # Errors
///
/// An internal error when the blob does not decrypt or does not hold 64 hex
/// characters.
pub(crate) fn decrypt_repo_key(
    data_key: &DataKey,
    repository_id: &str,
    blob: &[u8],
) -> Result<RepoKey, ApiError> {
    let secret = data_key
        .decrypt(REPO_KEY_CONTEXT, repository_id, blob)
        .map_err(|error| ApiError::internal(&error))?;
    let bytes = decode_hex32(secret.expose())
        .ok_or_else(|| ApiError::internal(&"the stored repository key is not 64 hex characters"))?;
    Ok(RepoKey::from_bytes(bytes))
}

/// The blob of `backup_repositories.repo_key_enc` for `key`.
pub(crate) fn encrypt_repo_key(data_key: &DataKey, repository_id: &str, key: &RepoKey) -> Vec<u8> {
    let hex = zeroize::Zeroizing::new(hex(key.as_bytes()));
    data_key.encrypt(REPO_KEY_CONTEXT, repository_id, hex.as_bytes())
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

/// The 32 bytes of 64 lowercase or uppercase hex characters.
fn decode_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(bytes)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Parses an RFC 3339 timestamp written by [`crate::store::now_rfc3339`].
pub(crate) fn parse_time(text: &str) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
}

/// `at` as the fixed-width timestamp the store keeps (the same layout as
/// [`crate::store::now_rfc3339`], so string comparison orders correctly).
pub(crate) fn timestamp(at: time::OffsetDateTime) -> String {
    let at = at.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        at.year(),
        u8::from(at.month()),
        at.day(),
        at.hour(),
        at.minute(),
        at.second(),
        at.millisecond()
    )
}

/// `now + seconds` as the fixed-width timestamp the store keeps.
pub(crate) fn time_after(at: time::OffsetDateTime, span: Duration) -> time::OffsetDateTime {
    at + time::Duration::seconds(i64::try_from(span.as_secs()).unwrap_or(i64::MAX))
}

/// Joins the repository root and a relative key (`backup-format.md` section 2).
pub(crate) fn object_key(root: &str, relative: &str) -> String {
    format!("{root}{relative}")
}

/// The root-relative part of `key`, or `None` when it is outside this
/// repository.
pub(crate) fn relative_key<'a>(root: &str, key: &'a str) -> Option<&'a str> {
    key.strip_prefix(root)
}

/// The `audit-log-format.md` section 3.5 system record of the backups
/// subsystem: actor `backups`, channel `system`, no scope.
pub(crate) fn system_record(
    operation: &str,
    target_type: &str,
    target_id: &str,
    params: serde_json::Value,
    result: crate::audit::Outcome,
    result_detail: Option<String>,
    change_seq: Option<i64>,
) -> crate::audit::Record {
    crate::audit::Record {
        actor: crate::changelog::Actor::system("backups"),
        agent_model: None,
        on_behalf_of: None,
        channel: crate::audit::Channel::System,
        operation: operation.to_string(),
        scope: None,
        target_type: Some(target_type.to_string()),
        target_id: Some(target_id.to_string()),
        project_id: None,
        params,
        justification: None,
        trigger_event_id: None,
        incident_id: None,
        grant_id: None,
        approval_id: None,
        request_id: quoro_types::new_id(quoro_types::Prefix::Request),
        result,
        result_detail,
        change_seq,
    }
}
