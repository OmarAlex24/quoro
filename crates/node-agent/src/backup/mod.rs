// SPDX-License-Identifier: AGPL-3.0-only
//! Cold backups and restores in the node agent (`node-agent-protocol.md`
//! sections 6.7 and 12, `backup-format.md` sections 7.1, 8.1 and 10).
//!
//! The node moves bytes and checks them; it writes no crypto. `quoro-backup`
//! chunks the snapshot, seals every chunk and the manifest, and checks every
//! restore object; `quoro-providers` puts and gets them. [`manager`] runs every
//! job of `spec.backups` in its own task beside the apply loop, and every
//! `spec.volumes[].restore` that has not finished yet.
//!
//! Two rules of the contract shape this module:
//!
//! - The repository credential and the job's `id_key` and `data_key` live only
//!   in the memory of the task that carries them (section 5.11). Nothing here
//!   logs them, and no row of the local store holds them: the desired state
//!   itself is sealed with the node-local key at rest.
//! - The host never mounts a volume file (section 6.7). A backup reads the
//!   engine's snapshot as bytes; a restore writes bytes into
//!   `restore-staging/`, and `import_volume` renames the file into place
//!   (`engine-trait.md` section 3.20).

pub mod manager;

use std::future::Future;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use quoro_backup::{Key, SourceKeys};
use quoro_proto::wire::SpecBackupRepository;
use quoro_providers::object_store::{ObjectStore, ObjectStoreError};
use quoro_providers::s3::{S3Client, S3Config};
use tracing::debug;

/// One gibibyte, the unit of `spec.volumes[].size_gb`.
pub const GIB: u64 = 1 << 30;

/// The longest error a status report carries (section 5.3).
pub const MAX_ERROR_CHARS: usize = 2_000;

/// The directory under the node's data directory that holds a restore in
/// progress (`backup-format.md` section 8.1).
pub const STAGING_DIR: &str = "restore-staging";

/// How a transport failure is retried before the job fails
/// (`backup-format.md` section 7.1 step 4).
pub const TRANSPORT_RETRIES: u32 = 3;

/// The first wait before a failed transport request is retried.
pub const RETRY_BASE: std::time::Duration = std::time::Duration::from_secs(1);

/// Opens the object store a `spec.backup_repository` names.
///
/// The production implementation builds an [`S3Client`] per job, so the
/// credential lives no longer than the snapshot that carries it. A test
/// implementation hands back one [`quoro_providers::object_store::MemoryStore`]
/// (or a failing wrapper around it).
pub trait Repository: Send + Sync + 'static {
    /// The store every job of this repository writes through.
    type Store: ObjectStore + 'static;

    /// Opens the store from the repository entry. The error is one line and
    /// never contains the secret.
    fn open(&self, repository: &SpecBackupRepository) -> Result<Arc<Self::Store>, String>;
}

/// The production repository: an S3-compatible bucket (`backup-format.md`
/// section 10).
#[derive(Clone, Copy, Debug, Default)]
pub struct S3Repository;

impl Repository for S3Repository {
    type Store = S3Client;

    fn open(&self, repository: &SpecBackupRepository) -> Result<Arc<S3Client>, String> {
        let config = S3Config {
            endpoint: repository.endpoint.clone(),
            region: repository.region.clone(),
            bucket: repository.bucket.clone(),
            access_key_id: repository.access_key_id.clone(),
            secret_access_key: repository.secret_access_key.clone().into(),
        };
        S3Client::new(config)
            .map(Arc::new)
            .map_err(|error| format!("could not open the backup bucket: {error}"))
    }
}

/// The two chunk keys of one source, decoded from the base64 forms of a job or
/// a restore (section 5.11).
///
/// # Errors
///
/// One line naming the field when it is not base64 of 32 bytes; the value
/// itself is never echoed.
pub(crate) fn source_keys(id_key: &str, data_key: &str) -> Result<SourceKeys, String> {
    fn one(field: &str, text: &str) -> Result<Key, String> {
        let bytes = STANDARD
            .decode(text)
            .map_err(|_| format!("{field} is not base64"))?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| format!("{field} is not 32 bytes"))?;
        Ok(Key::from_bytes(bytes))
    }
    Ok(SourceKeys {
        id_key: one("id_key", id_key)?,
        data_key: one("data_key", data_key)?,
    })
}

/// The object key of one layout key inside the repository: `root + "/" + key`
/// (section 5.11).
pub(crate) fn object_key(root: &str, key: &str) -> String {
    format!("{}/{key}", root.trim_end_matches('/'))
}

/// One line of at most [`MAX_ERROR_CHARS`] characters, with no newline
/// (section 5.3).
pub(crate) fn one_line(error: &str) -> String {
    crate::util::one_line(error, MAX_ERROR_CHARS)
}

/// The wait before retry number `attempt` (1-based): the base, then doubled.
pub(crate) fn retry_delay(base: std::time::Duration, attempt: u32) -> std::time::Duration {
    let factor = 1u32
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u32::MAX);
    base.checked_mul(factor).unwrap_or(std::time::Duration::MAX)
}

/// Runs `call`, retrying a transport failure up to `retries` times with the
/// given back-off (section 7.1 step 4).
///
/// A refusal, a missing object or a protocol error is returned at once: only a
/// request that did not complete is worth repeating.
pub(crate) async fn retry_transport<T, F, Fut>(
    retries: u32,
    base: std::time::Duration,
    mut call: F,
) -> Result<T, ObjectStoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ObjectStoreError>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Err(ObjectStoreError::Transport(message)) if attempt < retries => {
                attempt += 1;
                debug!(
                    attempt,
                    error = %message,
                    "retrying a backup request after a transport error"
                );
                tokio::time::sleep(retry_delay(base, attempt)).await;
            }
            other => return other,
        }
    }
}
