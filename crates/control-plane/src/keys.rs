// SPDX-License-Identifier: AGPL-3.0-only
//! Cluster identity and the key that signs desired state
//! (`node-agent-protocol.md` sections 2.4 and 5.11, ADR 0050).
//!
//! The cluster signing key is an Ed25519 key separate from the cluster
//! data-encryption key. Its 32-byte seed lives in
//! `<data_dir>/keys/signing.key`, mode `0600` in the `0700` directory that also
//! holds the data-encryption key, outside the SQLite file, so the keys can be
//! backed up on their own. Nodes pin its public key at enrollment and
//! verify every `desired_state` with it.
//!
//! [`ensure_cluster_meta`] completes `meta` with the cluster id and the term.
//! `meta` is not part of the change-log projection, so neither write appears
//! in it.

use crate::error::ApiError;
use crate::store::{Store, random_secret};
use quoro_proto::sign;
use quoro_types::{Prefix, new_id};
use sha2::{Digest as _, Sha256};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// Name of the file, inside `<data_dir>/keys`, that holds the 32-byte
/// Ed25519 seed.
pub const SIGNING_KEY_FILE: &str = "signing.key";

/// The cluster signing key.
#[derive(Clone, Debug)]
pub struct ClusterKeys {
    signing: sign::SigningKey,
    key_id: String,
}

fn keys_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("keys")
}

fn key_path(data_dir: &Path) -> PathBuf {
    keys_dir(data_dir).join(SIGNING_KEY_FILE)
}

impl ClusterKeys {
    /// Loads `<data_dir>/keys/signing.key`, generating it on first start.
    ///
    /// # Errors
    ///
    /// An I/O error reading or writing the file, or a file that is not
    /// exactly 32 bytes.
    pub fn load_or_generate(data_dir: &Path) -> std::io::Result<Self> {
        let path = key_path(data_dir);
        let seed = match std::fs::read(&path) {
            Ok(bytes) => {
                let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{} is not a 32-byte Ed25519 seed", path.display()),
                    )
                })?;
                seed
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut seed = [0u8; 32];
                getrandom::fill(&mut seed).map_err(std::io::Error::other)?;
                write_seed(data_dir, &seed)?;
                seed
            }
            Err(e) => return Err(e),
        };
        Ok(Self::from_seed(seed))
    }

    fn from_seed(seed: [u8; 32]) -> Self {
        let signing = sign::signing_key_from_seed(&seed);
        let digest = Sha256::digest(signing.verifying_key().as_bytes());
        let key_id = digest
            .iter()
            .fold(String::with_capacity(64), |mut out, byte| {
                use std::fmt::Write as _;
                let _ = write!(out, "{byte:02x}");
                out
            });
        Self { signing, key_id }
    }

    /// The Ed25519 key that signs `desired_state`.
    #[must_use]
    pub fn signing_key(&self) -> &sign::SigningKey {
        &self.signing
    }

    /// The public key, base64, as `cluster_public_key` carries it.
    #[must_use]
    pub fn public_key_base64(&self) -> String {
        sign::public_key_to_base64(&self.signing.verifying_key())
    }

    /// Lowercase hex SHA-256 of the 32 raw public-key bytes.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The 32-byte Ed25519 seed, wiped when the returned value is dropped.
    /// Only the custody seals read it (`node-agent-protocol.md` section 7.2).
    pub(crate) fn seed(&self) -> zeroize::Zeroizing<[u8; 32]> {
        zeroize::Zeroizing::new(self.signing.to_bytes())
    }
}

/// Writes `seed` to the key file atomically and mode `0600`.
fn write_seed(data_dir: &Path, seed: &[u8; 32]) -> std::io::Result<()> {
    let dir = keys_dir(data_dir);
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(&dir)?;
    let tmp = dir.join(format!("{SIGNING_KEY_FILE}.tmp.{}", &random_secret()[..16]));
    let _ = std::fs::remove_file(&tmp);
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(seed)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, key_path(data_dir))?;
    Ok(())
}

/// Inserts the `cluster_id` and `term` keys when they are missing and returns
/// the cluster id. Idempotent: an existing cluster keeps its identity.
///
/// # Errors
///
/// A database error.
pub async fn ensure_cluster_meta(store: &Store) -> Result<String, ApiError> {
    let mut w = store.begin_write().await?;
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT value FROM meta WHERE key = 'cluster_id'")
            .fetch_optional(&mut *w.tx)
            .await?;
    let cluster_id = match existing {
        Some((id,)) => id,
        None => {
            let id = new_id(Prefix::Cluster);
            sqlx::query("INSERT INTO meta (key, value) VALUES ('cluster_id', ?)")
                .bind(&id)
                .execute(&mut *w.tx)
                .await?;
            id
        }
    };
    sqlx::query("INSERT OR IGNORE INTO meta (key, value) VALUES ('term', '1')")
        .execute(&mut *w.tx)
        .await?;
    w.commit().await?;
    Ok(cluster_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-keys-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_key_is_generated_once_and_reloaded() {
        let dir = temp_dir();
        let first = ClusterKeys::load_or_generate(&dir).unwrap();
        let second = ClusterKeys::load_or_generate(&dir).unwrap();
        assert_eq!(first.public_key_base64(), second.public_key_base64());
        assert_eq!(first.key_id(), second.key_id());
        assert_eq!(first.key_id().len(), 64);
        assert!(first.key_id().bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(std::fs::read(key_path(&dir)).unwrap().len(), 32);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(key_path(&dir))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_key_of_the_wrong_length_is_refused() {
        let dir = temp_dir();
        std::fs::create_dir_all(keys_dir(&dir)).unwrap();
        std::fs::write(key_path(&dir), [0u8; 31]).unwrap();
        let error = ClusterKeys::load_or_generate(&dir).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
