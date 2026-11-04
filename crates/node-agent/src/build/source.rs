// SPDX-License-Identifier: AGPL-3.0-only
//! The build source: one commit fetched with the host's Git client into a
//! bare repository, written as a tar stream onto the raw `quoro-src` disk
//! (`node-agent-protocol.md` section 6.6).
//!
//! No working tree of the repository ever exists on the host, and Git runs
//! hardened: HTTPS only, no prompts, no submodules, no hooks or templates, a
//! wall-clock timeout.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// Fetches `git_ref` of `url` (a branch, a tag or a full commit SHA) into a
/// new bare repository at `repo` with depth 1, and returns the full commit
/// SHA it resolved to (40 lowercase hex).
///
/// # Errors
///
/// A one-line reason when the URL is not `https://`, Git fails, or the
/// timeout passes.
pub async fn fetch_commit(
    _git: &Path,
    _url: &str,
    _git_ref: &str,
    _repo: &Path,
    _timeout: Duration,
) -> Result<String, String> {
    todo!("source fetch")
}

/// Writes the source tar onto `disk` (created, or truncated, then padded with
/// zeros to a whole MiB): the tree of `commit` under `src/`, and one file per
/// secret under `secrets/<id>` (mode `0400`). Returns the size of `disk`.
///
/// # Errors
///
/// A one-line reason when Git or a write fails.
pub async fn write_source_disk(
    _git: &Path,
    _repo: &Path,
    _commit: &str,
    _secrets: &BTreeMap<String, String>,
    _disk: &Path,
) -> Result<u64, String> {
    todo!("source disk")
}
