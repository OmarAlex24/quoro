// SPDX-License-Identifier: AGPL-3.0-only
//! The repository config object `config-<n>.json` (`backup-format.md` section
//! 4).
//!
//! It is plain JSON, written by the primary when it creates the repository
//! (`n = 1`) and each time the recovery key is replaced (`n + 1`). It carries
//! `repo_key` only wrapped under the recovery key, so a reader without the
//! primary can still open the repository.
//!
//! [`open_newest_config`] is the one reader of section 4: `quoro restore`
//! (section 8.2 step 1) and a primary that restores the repository key
//! (section 4, "Restoring the key on a primary") both pick the config with it.

use std::fmt;
use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::{FORMAT, RecoveryKey, RepoKey, WrappedKey, layout, unwrap_repo_key};

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

/// Why [`open_newest_config`] returned no repository key. Neither variant
/// carries key material or the content of an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenConfigError {
    /// The newest config object the recovery key opens names a format
    /// version this reader does not know (section 11).
    UnknownFormat(u32),
    /// The recovery key opens no config object of this repository.
    NoneOpens,
}

impl fmt::Display for OpenConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFormat(format) => {
                write!(f, "the config object names unknown format {format}")
            }
            Self::NoneOpens => {
                f.write_str("the recovery key opens no config object of this repository")
            }
        }
    }
}

impl std::error::Error for OpenConfigError {}

/// The `config-<n>.json` keys among `listed`, highest `n` first.
///
/// `listed` holds full object keys, as a listing of `<root>config-` returns
/// them. A key is a config object when the part after `root` is
/// `config-<n>.json` without padding (section 2, [`layout::parse_config_n`]);
/// every other key is dropped.
#[must_use]
pub fn config_keys_newest_first(
    root: &str,
    listed: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut counters: Vec<(u64, String)> = listed
        .into_iter()
        .filter_map(|key| {
            let name = key.strip_prefix(root).unwrap_or(&key);
            layout::parse_config_n(name).map(|counter| (counter, key))
        })
        .collect();
    counters.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));
    counters.into_iter().map(|(_, key)| key).collect()
}

/// Unwraps `repo_key` from the newest config object `recovery` opens
/// (sections 4 and 8.2 step 1).
///
/// `listed` holds the full keys a listing of `<root>config-` returned, and
/// `fetch` reads one object by its full key. The config objects are tried
/// from the highest `n` down. One is skipped, and the next tried, when
/// `fetch` cannot read it, it is not a config object, it names another
/// cluster than `cluster_id`, it names another repository than
/// `repository_id` (when given), or `recovery` does not open it. The first
/// one `recovery` opens is the answer, unless it names a format other than
/// [`FORMAT`], which ends the search.
///
/// # Errors
///
/// [`OpenConfigError::UnknownFormat`] when the newest config the key opens
/// names an unknown format, and [`OpenConfigError::NoneOpens`] when the key
/// opens none.
pub async fn open_newest_config<F, Fut, E>(
    root: &str,
    listed: impl IntoIterator<Item = String>,
    cluster_id: &str,
    repository_id: Option<&str>,
    recovery: &RecoveryKey,
    mut fetch: F,
) -> Result<(RepoConfig, RepoKey), OpenConfigError>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    for key in config_keys_newest_first(root, listed) {
        // A config object that cannot be read is not one this key opens.
        let Ok(bytes) = fetch(key).await else {
            continue;
        };
        let Ok(config) = serde_json::from_slice::<RepoConfig>(&bytes) else {
            continue;
        };
        if config.cluster_id != cluster_id {
            continue;
        }
        if repository_id.is_some_and(|id| config.repository_id != id) {
            continue;
        }
        // The recovery key does not open this one: try the next config.
        let Ok(repo_key) =
            unwrap_repo_key(&config.wrapped_repo_key, recovery, &config.repository_id)
        else {
            continue;
        };
        if config.format != FORMAT {
            return Err(OpenConfigError::UnknownFormat(config.format));
        }
        return Ok((config, repo_key));
    }
    Err(OpenConfigError::NoneOpens)
}
