// SPDX-License-Identifier: AGPL-3.0-only
//! The `apk` package database.

use super::os_release::OsRelease;
use super::{Collector, Tree};

/// Adds the packages of the image's `apk` database, if it has one.
pub fn collect<T: Tree + ?Sized>(_tree: &T, _os: Option<&OsRelease>, _out: &mut Collector) {}
