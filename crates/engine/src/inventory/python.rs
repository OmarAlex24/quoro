// SPDX-License-Identifier: AGPL-3.0-only
//! Installed Python distributions (`*.dist-info/METADATA`, `*.egg-info/PKG-INFO`).

use super::{Collector, Tree};

/// Whether a directory named `name` holds a distribution's metadata.
pub fn is_metadata_dir(_name: &str) -> bool {
    false
}

/// Adds the distribution whose metadata directory is `dir`.
pub fn collect_dir<T: Tree + ?Sized>(_tree: &T, _dir: &str, _out: &mut Collector) {}
