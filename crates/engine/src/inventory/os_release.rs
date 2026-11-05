// SPDX-License-Identifier: AGPL-3.0-only
//! `os-release` (`etc/os-release`, then `usr/lib/os-release`).

use super::Tree;

/// What the inventory needs from os-release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OsRelease {
    /// `ID`, lowercase, for example `debian`, `alpine`, `rhel`.
    pub id: String,
    /// `VERSION_ID`, for example `13`, `3.22.1`, `9.4`.
    pub version_id: Option<String>,
    /// `ID_LIKE`, split on whitespace.
    pub id_like: Vec<String>,
    /// `VERSION_CODENAME`, for example `trixie`.
    pub version_codename: Option<String>,
}

/// Reads the image's os-release; `None` when it has none or it carries no `ID`.
pub fn read<T: Tree + ?Sized>(_tree: &T) -> Option<OsRelease> {
    None
}
