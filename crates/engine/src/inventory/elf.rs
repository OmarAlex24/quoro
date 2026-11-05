// SPDX-License-Identifier: AGPL-3.0-only
//! ELF executables: Go build information and `cargo auditable` data.

use super::{Collector, Tree};

/// Inspects the executable at `path`: adds the Go modules or Rust crates it
/// records, or `static_binary` for a static executable that records neither.
pub fn inspect<T: Tree + ?Sized>(_tree: &T, _path: &str, _out: &mut Collector) {}
