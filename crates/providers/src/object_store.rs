// SPDX-License-Identifier: AGPL-3.0-only
//! The object-store operations Quoro needs from an S3-compatible bucket, the
//! lock probe of `docs/contracts/audit-log-format.md` section 8.2, and an
//! in-memory implementation for other crates' tests.
//!
//! Backups write chunks and manifests with `If-None-Match: *`, read them back
//! and delete them while pruning (`docs/contracts/backup-format.md` sections 5
//! and 9). The off-site audit copy writes every segment once and asks the
//! bucket how strongly it can refuse a rewrite (section 8 of the audit-log
//! contract). The trait keeps both callers independent of the client in
//! [`crate::s3`].

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// One object as `ListObjectsV2` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// RFC 3339, as the server sent it.
    pub last_modified: String,
}

/// One version as `ListObjectVersions` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub is_delete_marker: bool,
    pub size: u64,
    /// RFC 3339, as the server sent it.
    pub last_modified: String,
}

/// What `HEAD` says about one object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectHead {
    pub size: u64,
    pub version_id: Option<String>,
    /// `COMPLIANCE` or `GOVERNANCE` when the object carries a lock.
    pub lock_mode: Option<String>,
    /// RFC 3339, as the server sent it.
    pub retain_until: Option<String>,
}

/// The compliance lock a write asks the bucket for
/// (`docs/contracts/audit-log-format.md` section 8.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComplianceLock {
    /// RFC 3339.
    pub retain_until: String,
}

/// Why an object-store call failed. Messages never contain the secret or a
/// signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectStoreError {
    /// The addressed object or version does not exist.
    NotFound,
    /// `If-None-Match: *` was refused because the key already exists (412).
    AlreadyExists,
    /// A lock refused the change: `409 ObjectLockedByBucketPolicy`, or a lock
    /// on the version being deleted.
    Locked(String),
    /// The credential was refused (401/403 for a reason other than a lock).
    Denied(String),
    /// The store answered with an error Quoro does not map.
    Status {
        status: u16,
        code: String,
        message: String,
    },
    /// The request did not complete (connect, TLS, timeout, body).
    Transport(String),
    /// The response could not be decoded as the protocol requires.
    Protocol(String),
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::Locked(message) => write!(f, "locked: {message}"),
            Self::Denied(message) => write!(f, "denied: {message}"),
            Self::Status {
                status,
                code,
                message,
            } => write!(f, "s3 error {status} {code}: {message}"),
            Self::Transport(message) => write!(f, "transport: {message}"),
            Self::Protocol(message) => write!(f, "protocol: {message}"),
        }
    }
}

impl std::error::Error for ObjectStoreError {}

/// The operations the primary, the node agent and the audit exporter ask of a
/// bucket.
pub trait ObjectStore: Send + Sync {
    /// `PUT` with `If-None-Match: *`. `Ok(version_id)` when created;
    /// `Err(AlreadyExists)` on 412.
    fn put_new(
        &self,
        key: &str,
        body: Vec<u8>,
        lock: Option<&ComplianceLock>,
    ) -> impl Future<Output = Result<Option<String>, ObjectStoreError>> + Send;

    /// Plain `PUT` (overwrites). Only the lock probe uses it.
    fn put_overwrite(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<Option<String>, ObjectStoreError>> + Send;

    fn get(&self, key: &str) -> impl Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send;

    fn get_version(
        &self,
        key: &str,
        version_id: &str,
    ) -> impl Future<Output = Result<Vec<u8>, ObjectStoreError>> + Send;

    fn head(&self, key: &str) -> impl Future<Output = Result<ObjectHead, ObjectStoreError>> + Send;

    /// `ListObjectsV2` over every page (continuation tokens are followed);
    /// keys in the order the store returns them.
    fn list(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<ObjectInfo>, ObjectStoreError>> + Send;

    fn list_versions(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<ObjectVersion>, ObjectStoreError>> + Send;

    /// `DELETE`; a missing object counts as success.
    fn delete(&self, key: &str) -> impl Future<Output = Result<(), ObjectStoreError>> + Send;

    fn delete_version(
        &self,
        key: &str,
        version_id: &str,
    ) -> impl Future<Output = Result<(), ObjectStoreError>> + Send;
}

/// How strongly a bucket can refuse a rewrite of an exported object
/// (`docs/contracts/audit-log-format.md` section 8.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockStrength {
    None,
    BucketRule,
    Compliance,
}

/// The outcome of [`probe_lock`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockProbe {
    pub strength: LockStrength,
    /// What each step returned, for `audit_exports.probe_detail`.
    pub detail: String,
}

/// The probe of `docs/contracts/audit-log-format.md` section 8.2.
///
/// `probe_prefix` is the full key prefix ending in `/`
/// (`<prefix>/<cluster_id>/audit/probe/`). `now_rfc3339` is the time of the
/// probe and `retain_until_rfc3339` the one-day retention asked of a
/// compliance bucket. The result never reports a stronger strength than the
/// store showed: any error yields [`LockStrength::None`] with the error in the
/// detail.
pub async fn probe_lock<S: ObjectStore>(
    store: &S,
    probe_prefix: &str,
    now_rfc3339: &str,
    retain_until_rfc3339: &str,
) -> LockProbe {
    let key = format!("{probe_prefix}{}", probe_suffix());
    let original = b"quoro lock probe".to_vec();
    let replacement = b"quoro lock probe overwritten".to_vec();
    let lock = ComplianceLock {
        retain_until: retain_until_rfc3339.to_string(),
    };

    let mut steps: Vec<String> = Vec::new();

    // (1) write the probe object, falling back without the lock headers when
    // the store says it does not take them.
    let put = match store.put_new(&key, original.clone(), Some(&lock)).await {
        Err(ObjectStoreError::Status { status, code, .. })
            if status == 400 || status == 501 || code == "NotImplemented" =>
        {
            steps.push(format!("put {status} without lock"));
            store.put_new(&key, original.clone(), None).await
        }
        other => other,
    };
    if let Err(err) = put {
        steps.push(format!("put failed ({err})"));
        return finish(now_rfc3339, steps, LockStrength::None);
    }
    steps.push("put 200".to_string());

    // (2) remember the version and the lock the store reports.
    let head = match store.head(&key).await {
        Ok(head) => head,
        Err(err) => {
            steps.push(format!("head failed ({err})"));
            return finish(now_rfc3339, steps, LockStrength::None);
        }
    };
    steps.push(match &head.version_id {
        Some(_) => "head versioned".to_string(),
        None => "head unversioned".to_string(),
    });

    // (3) and (4) try to rewrite and to delete the object.
    let overwrite = store.put_overwrite(&key, replacement).await;
    steps.push(format!("overwrite {}", outcome(&overwrite)));
    let deleted = store.delete(&key).await;
    steps.push(format!("delete {}", outcome(&deleted)));

    let both_locked = matches!(overwrite, Err(ObjectStoreError::Locked(_)))
        && matches!(deleted, Err(ObjectStoreError::Locked(_)));
    if both_locked {
        // A bucket rule refuses the change itself; the object must be intact.
        return match store.get(&key).await {
            Ok(bytes) if bytes == original => {
                steps.push("readback ok".to_string());
                finish(now_rfc3339, steps, LockStrength::BucketRule)
            }
            Ok(_) => {
                steps.push("readback differs".to_string());
                finish(now_rfc3339, steps, LockStrength::None)
            }
            Err(err) => {
                steps.push(format!("readback {}", err));
                finish(now_rfc3339, steps, LockStrength::None)
            }
        };
    }

    // A compliance lock keeps the original version even though the plain
    // write and delete succeeded.
    let Some(version_id) = head.version_id.as_deref() else {
        return finish(now_rfc3339, steps, LockStrength::None);
    };
    if head.lock_mode.as_deref() != Some("COMPLIANCE") {
        return finish(now_rfc3339, steps, LockStrength::None);
    }
    match store.get_version(&key, version_id).await {
        Ok(bytes) if bytes == original => steps.push("version readback ok".to_string()),
        Ok(_) => {
            steps.push("version readback differs".to_string());
            return finish(now_rfc3339, steps, LockStrength::None);
        }
        Err(err) => {
            steps.push(format!("version readback {}", err));
            return finish(now_rfc3339, steps, LockStrength::None);
        }
    }
    match store.delete_version(&key, version_id).await {
        Err(ObjectStoreError::Locked(_) | ObjectStoreError::Denied(_)) => {
            steps.push("version delete refused".to_string());
            finish(now_rfc3339, steps, LockStrength::Compliance)
        }
        Ok(()) => {
            steps.push("version delete 204".to_string());
            finish(now_rfc3339, steps, LockStrength::None)
        }
        Err(err) => {
            steps.push(format!("version delete {err}"));
            finish(now_rfc3339, steps, LockStrength::None)
        }
    }
}

/// A random-enough suffix so two probes never share a key. Randomness is not
/// security here; a key that is never reused is enough. `getrandom` is
/// deliberately not a dependency of this crate.
fn probe_suffix() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("{nanos:x}-{:x}", SEQUENCE.fetch_add(1, Ordering::Relaxed))
}

fn outcome<T>(result: &Result<T, ObjectStoreError>) -> String {
    match result {
        Ok(_) => "200".to_string(),
        Err(err) => match err {
            ObjectStoreError::NotFound => "404".to_string(),
            ObjectStoreError::AlreadyExists => "412".to_string(),
            ObjectStoreError::Locked(_) => "409".to_string(),
            ObjectStoreError::Denied(_) => "403".to_string(),
            ObjectStoreError::Status { status, .. } => status.to_string(),
            ObjectStoreError::Transport(_) => "transport error".to_string(),
            ObjectStoreError::Protocol(_) => "protocol error".to_string(),
        },
    }
}

fn finish(now_rfc3339: &str, steps: Vec<String>, strength: LockStrength) -> LockProbe {
    tracing::debug!(
        probe_at = now_rfc3339,
        ?strength,
        detail = %steps.join(", "),
        "lock probe result"
    );
    LockProbe {
        strength,
        detail: steps.join(", "),
    }
}

/// An in-memory [`ObjectStore`] for other crates' tests (no feature gate).
///
/// It honours `If-None-Match: *`, keeps versions when it emulates a versioned
/// bucket, and can refuse changes the way a bucket lock rule or a compliance
/// lock does. It has no clock: every object reports
/// `1970-01-01T00:00:00Z` as its last-modified time.
#[derive(Debug)]
pub struct MemoryStore {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    emulation: Emulation,
    objects: BTreeMap<String, Vec<StoredVersion>>,
    next_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Emulation {
    None,
    /// Refuses an overwrite or a delete under this prefix with 409.
    BucketRule(String),
    /// Versioned, with compliance locks on versions written with a lock.
    Compliance,
}

#[derive(Clone, Debug)]
struct StoredVersion {
    version_id: Option<String>,
    body: Vec<u8>,
    lock_mode: Option<String>,
    retain_until: Option<String>,
    delete_marker: bool,
}

/// `MemoryStore` has no clock; every object reports this time.
const MEMORY_LAST_MODIFIED: &str = "1970-01-01T00:00:00Z";

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// An unversioned bucket without a lock.
    #[must_use]
    pub fn new() -> Self {
        Self::with(Emulation::None)
    }

    /// A bucket whose lock rule refuses overwrite and delete under `prefix`
    /// with 409 (`bucket_rule`).
    #[must_use]
    pub fn bucket_rule(prefix: impl Into<String>) -> Self {
        Self::with(Emulation::BucketRule(prefix.into()))
    }

    /// A versioned bucket that reports compliance locks.
    #[must_use]
    pub fn compliance() -> Self {
        Self::with(Emulation::Compliance)
    }

    fn with(emulation: Emulation) -> Self {
        Self {
            state: Mutex::new(State {
                emulation,
                objects: BTreeMap::new(),
                next_version: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl State {
    fn versioned(&self) -> bool {
        matches!(self.emulation, Emulation::Compliance)
    }

    fn next_id(&mut self) -> String {
        self.next_version += 1;
        format!("v{}", self.next_version)
    }

    fn current(&self, key: &str) -> Option<&StoredVersion> {
        self.objects.get(key).and_then(|versions| versions.last())
    }

    fn exists(&self, key: &str) -> bool {
        match self.current(key) {
            Some(version) => !version.delete_marker,
            None => false,
        }
    }

    fn rule_blocks(&self, key: &str) -> bool {
        match &self.emulation {
            Emulation::BucketRule(prefix) => key.starts_with(prefix.as_str()),
            _ => false,
        }
    }
}

impl ObjectStore for MemoryStore {
    async fn put_new(
        &self,
        key: &str,
        body: Vec<u8>,
        lock: Option<&ComplianceLock>,
    ) -> Result<Option<String>, ObjectStoreError> {
        let mut state = self.lock();
        if state.exists(key) {
            return Err(ObjectStoreError::AlreadyExists);
        }
        let version_id = if state.versioned() {
            let id = state.next_id();
            let version = StoredVersion {
                version_id: Some(id.clone()),
                body,
                lock_mode: lock.map(|_| "COMPLIANCE".to_string()),
                retain_until: lock.map(|lock| lock.retain_until.clone()),
                delete_marker: false,
            };
            state
                .objects
                .entry(key.to_string())
                .or_default()
                .push(version);
            Some(id)
        } else {
            let version = StoredVersion {
                version_id: None,
                body,
                lock_mode: None,
                retain_until: None,
                delete_marker: false,
            };
            state.objects.insert(key.to_string(), vec![version]);
            None
        };
        Ok(version_id)
    }

    async fn put_overwrite(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<Option<String>, ObjectStoreError> {
        let mut state = self.lock();
        if state.rule_blocks(key) {
            return Err(ObjectStoreError::Locked(
                "the bucket lock rule refuses the overwrite".to_string(),
            ));
        }
        if state.versioned() {
            let id = state.next_id();
            let version = StoredVersion {
                version_id: Some(id.clone()),
                body,
                lock_mode: None,
                retain_until: None,
                delete_marker: false,
            };
            state
                .objects
                .entry(key.to_string())
                .or_default()
                .push(version);
            Ok(Some(id))
        } else {
            let version = StoredVersion {
                version_id: None,
                body,
                lock_mode: None,
                retain_until: None,
                delete_marker: false,
            };
            state.objects.insert(key.to_string(), vec![version]);
            Ok(None)
        }
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        let state = self.lock();
        match state.current(key) {
            Some(version) if !version.delete_marker => Ok(version.body.clone()),
            _ => Err(ObjectStoreError::NotFound),
        }
    }

    async fn get_version(&self, key: &str, version_id: &str) -> Result<Vec<u8>, ObjectStoreError> {
        let state = self.lock();
        let Some(versions) = state.objects.get(key) else {
            return Err(ObjectStoreError::NotFound);
        };
        versions
            .iter()
            .find(|version| {
                version.version_id.as_deref() == Some(version_id) && !version.delete_marker
            })
            .map(|version| version.body.clone())
            .ok_or(ObjectStoreError::NotFound)
    }

    async fn head(&self, key: &str) -> Result<ObjectHead, ObjectStoreError> {
        let state = self.lock();
        match state.current(key) {
            Some(version) if !version.delete_marker => Ok(ObjectHead {
                size: version.body.len() as u64,
                version_id: version.version_id.clone(),
                lock_mode: version.lock_mode.clone(),
                retain_until: version.retain_until.clone(),
            }),
            _ => Err(ObjectStoreError::NotFound),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectStoreError> {
        let state = self.lock();
        Ok(state
            .objects
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter_map(|(key, versions)| {
                let version = versions.last()?;
                (!version.delete_marker).then(|| ObjectInfo {
                    key: key.clone(),
                    size: version.body.len() as u64,
                    last_modified: MEMORY_LAST_MODIFIED.to_string(),
                })
            })
            .collect())
    }

    async fn list_versions(&self, prefix: &str) -> Result<Vec<ObjectVersion>, ObjectStoreError> {
        let state = self.lock();
        let mut out = Vec::new();
        for (key, versions) in &state.objects {
            if !key.starts_with(prefix) {
                continue;
            }
            let latest = versions.len().saturating_sub(1);
            for (index, version) in versions.iter().enumerate().rev() {
                out.push(ObjectVersion {
                    key: key.clone(),
                    version_id: version
                        .version_id
                        .clone()
                        .unwrap_or_else(|| "null".to_string()),
                    is_latest: index == latest,
                    is_delete_marker: version.delete_marker,
                    size: version.body.len() as u64,
                    last_modified: MEMORY_LAST_MODIFIED.to_string(),
                });
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let mut state = self.lock();
        if state.rule_blocks(key) {
            return Err(ObjectStoreError::Locked(
                "the bucket lock rule refuses the delete".to_string(),
            ));
        }
        if state.versioned() {
            if !state.objects.contains_key(key) {
                return Ok(());
            }
            let id = state.next_id();
            let marker = StoredVersion {
                version_id: Some(id),
                body: Vec::new(),
                lock_mode: None,
                retain_until: None,
                delete_marker: true,
            };
            state
                .objects
                .entry(key.to_string())
                .or_default()
                .push(marker);
        } else {
            state.objects.remove(key);
        }
        Ok(())
    }

    async fn delete_version(&self, key: &str, version_id: &str) -> Result<(), ObjectStoreError> {
        let mut state = self.lock();
        let Some(versions) = state.objects.get_mut(key) else {
            return Err(ObjectStoreError::NotFound);
        };
        let Some(index) = versions
            .iter()
            .position(|version| version.version_id.as_deref() == Some(version_id))
        else {
            return Err(ObjectStoreError::NotFound);
        };
        if versions[index].lock_mode.as_deref() == Some("COMPLIANCE") {
            return Err(ObjectStoreError::Locked(
                "the object version is locked".to_string(),
            ));
        }
        versions.remove(index);
        if versions.is_empty() {
            state.objects.remove(key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-11-05T02:00:00Z";
    const RETAIN: &str = "2026-11-06T02:00:00Z";

    #[tokio::test]
    async fn a_plain_store_puts_lists_and_deletes() {
        let store = MemoryStore::new();
        assert_eq!(
            store
                .put_new("chunks/00/00a1", b"one".to_vec(), None)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store.put_new("chunks/00/00a1", b"two".to_vec(), None).await,
            Err(ObjectStoreError::AlreadyExists)
        );
        assert_eq!(store.get("chunks/00/00a1").await.unwrap(), b"one");
        assert_eq!(store.head("chunks/00/00a1").await.unwrap().size, 3);
        assert_eq!(
            store
                .list("chunks/")
                .await
                .unwrap()
                .into_iter()
                .map(|object| object.key)
                .collect::<Vec<_>>(),
            vec!["chunks/00/00a1".to_string()]
        );
        assert_eq!(
            store
                .put_overwrite("chunks/00/00a1", b"three".to_vec())
                .await
                .unwrap(),
            None
        );
        assert_eq!(store.get("chunks/00/00a1").await.unwrap(), b"three");
        store.delete("chunks/00/00a1").await.unwrap();
        assert_eq!(
            store.get("chunks/00/00a1").await,
            Err(ObjectStoreError::NotFound)
        );
        // Deleting again is a success, like S3.
        store.delete("chunks/00/00a1").await.unwrap();
    }

    #[tokio::test]
    async fn a_bucket_rule_refuses_overwrite_and_delete_under_its_prefix() {
        let store = MemoryStore::bucket_rule("audit/");
        store
            .put_new("audit/0001.jsonl", b"segment".to_vec(), None)
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_overwrite("audit/0001.jsonl", b"other".to_vec())
                .await,
            Err(ObjectStoreError::Locked(_))
        ));
        assert!(matches!(
            store.delete("audit/0001.jsonl").await,
            Err(ObjectStoreError::Locked(_))
        ));
        assert_eq!(store.get("audit/0001.jsonl").await.unwrap(), b"segment");
        // Outside the prefix the rule does not apply.
        store
            .put_new("other/key", b"one".to_vec(), None)
            .await
            .unwrap();
        store
            .put_overwrite("other/key", b"two".to_vec())
            .await
            .unwrap();
        store.delete("other/key").await.unwrap();
    }

    #[tokio::test]
    async fn a_compliance_store_keeps_locked_versions() {
        let store = MemoryStore::compliance();
        let lock = ComplianceLock {
            retain_until: RETAIN.to_string(),
        };
        let version = store
            .put_new("audit/0001.jsonl", b"segment".to_vec(), Some(&lock))
            .await
            .unwrap()
            .expect("a version id");
        let head = store.head("audit/0001.jsonl").await.unwrap();
        assert_eq!(head.version_id.as_deref(), Some(version.as_str()));
        assert_eq!(head.lock_mode.as_deref(), Some("COMPLIANCE"));

        // A plain overwrite adds a version and a delete adds a marker.
        store
            .put_overwrite("audit/0001.jsonl", b"rewritten".to_vec())
            .await
            .unwrap();
        store.delete("audit/0001.jsonl").await.unwrap();
        assert_eq!(
            store.get("audit/0001.jsonl").await,
            Err(ObjectStoreError::NotFound)
        );
        assert!(store.list("audit/").await.unwrap().is_empty());
        let versions = store.list_versions("audit/").await.unwrap();
        assert_eq!(versions.len(), 3);
        assert!(versions[0].is_delete_marker && versions[0].is_latest);
        versions
            .iter()
            .for_each(|version| assert_eq!(version.key, "audit/0001.jsonl"));

        // The locked version still reads back and cannot be deleted.
        assert_eq!(
            store
                .get_version("audit/0001.jsonl", &version)
                .await
                .unwrap(),
            b"segment"
        );
        assert!(matches!(
            store.delete_version("audit/0001.jsonl", &version).await,
            Err(ObjectStoreError::Locked(_))
        ));
    }

    #[tokio::test]
    async fn probe_reports_none_for_an_unlocked_store() {
        let store = MemoryStore::new();
        let probe = probe_lock(&store, "audit/probe/", NOW, RETAIN).await;
        assert_eq!(probe.strength, LockStrength::None);
        assert!(probe.detail.contains("put 200"), "{}", probe.detail);
        assert!(probe.detail.contains("overwrite 200"), "{}", probe.detail);
        assert!(probe.detail.contains("delete 200"), "{}", probe.detail);
    }

    #[tokio::test]
    async fn probe_reports_bucket_rule_and_the_readback() {
        let store = MemoryStore::bucket_rule("audit/probe/");
        let probe = probe_lock(&store, "audit/probe/", NOW, RETAIN).await;
        assert_eq!(probe.strength, LockStrength::BucketRule);
        assert!(probe.detail.contains("overwrite 409"), "{}", probe.detail);
        assert!(probe.detail.contains("delete 409"), "{}", probe.detail);
        assert!(probe.detail.contains("readback ok"), "{}", probe.detail);
    }

    #[tokio::test]
    async fn probe_reports_compliance_when_the_version_survives() {
        let store = MemoryStore::compliance();
        let probe = probe_lock(&store, "audit/probe/", NOW, RETAIN).await;
        assert_eq!(probe.strength, LockStrength::Compliance);
        assert!(probe.detail.contains("head versioned"), "{}", probe.detail);
        assert!(
            probe.detail.contains("version readback ok"),
            "{}",
            probe.detail
        );
        assert!(
            probe.detail.contains("version delete refused"),
            "{}",
            probe.detail
        );
    }

    #[test]
    fn lock_strength_serializes_as_the_contract_spells_it() {
        assert_eq!(
            serde_json::to_string(&LockStrength::None).unwrap(),
            "\"none\""
        );
        assert_eq!(
            serde_json::to_string(&LockStrength::BucketRule).unwrap(),
            "\"bucket_rule\""
        );
        assert_eq!(
            serde_json::to_string(&LockStrength::Compliance).unwrap(),
            "\"compliance\""
        );
        assert_eq!(
            serde_json::from_str::<LockStrength>("\"compliance\"").unwrap(),
            LockStrength::Compliance
        );
    }
}
