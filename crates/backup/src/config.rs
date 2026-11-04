// SPDX-License-Identifier: AGPL-3.0-only
//! The repository config object `config-<n>.json` (`backup-format.md` section
//! 4).
//!
//! It is plain JSON, written by the primary when it creates the repository
//! (`n = 1`) and each time the recovery key is replaced (`n + 1`). It carries
//! `repo_key` only wrapped under the recovery key, so a reader without the
//! primary can still open the repository.

use serde::{Deserialize, Serialize};

use crate::WrappedKey;

/// The repository config (`config-<n>.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoConfig {
    /// [`crate::FORMAT`]; a reader refuses a version it does not know.
    pub format: u32,
    /// `brp_<ULID>`, the repository the config belongs to.
    pub repository_id: String,
    /// `cls_<ULID>`, the cluster the repository belongs to.
    pub cluster_id: String,
    /// Base64 public half of the cluster signing key, a hint for the seals.
    pub cluster_public_key: String,
    /// The first 16 hex of the recovery key's fingerprint (section 3).
    pub recovery_key_fingerprint: String,
    /// `repo_key` wrapped under the recovery key (section 4).
    pub wrapped_repo_key: WrappedKey,
    /// RFC 3339 UTC time the config was written.
    pub created_at: String,
}
