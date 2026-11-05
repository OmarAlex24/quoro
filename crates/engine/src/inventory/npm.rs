// SPDX-License-Identifier: AGPL-3.0-only
//! npm lockfiles (`package-lock.json`, `node_modules/.package-lock.json`).

use super::{Collector, Tree};

/// Whether the walk should read `path` as an npm lockfile.
pub fn is_lockfile(_path: &str) -> bool {
    false
}

/// Adds the packages of the lockfile at `path`.
pub fn collect_file<T: Tree + ?Sized>(_tree: &T, _path: &str, _out: &mut Collector) {}
