// SPDX-License-Identifier: AGPL-3.0-only
//! Vulnerability data on the primary.
//!
//! The primary keeps its copy of the OSV records, the KEV catalog and the image
//! inventories in `<data_dir>/vulns-cache.db`, a cache file next to the
//! database: it is large, it can always be fetched again, and it is not
//! replicated and not part of a control-plane backup. Only the data's age
//! (`osv_mirror`) and the findings stay in `quoro.db`.
//!
//! - [`ecosystem`] maps an inventory's ecosystem name to an OSV bucket.
//! - [`cache`] is the cache file: records, affected rows, KEV, inventories.
//! - [`osv`] reads an archive one record at a time, within fixed bounds.
//! - [`changes`] reads a bucket's `modified_id.csv` change list.
//! - [`fetch`] streams a download to disk and bounds every small body.
//! - [`mirror`] is the loop that downloads and refreshes, plus [`Vulns`].
//! - [`import`] feeds the cache from a directory without the network.
//! - [`kev`] holds everything KEV-specific.
//! - [`version`] orders versions per ecosystem and evaluates OSV ranges.
//! - [`status`] says what is known about an app's current image.
//! - [`severity`] turns a record's CVSS vector into a score and a band.
//! - [`ingest`] assembles the inventory parts nodes send and stores them.
//! - [`matcher`] matches one inventory against the cached OSV records.
//! - [`findings`] writes the findings and runs the matcher loop.
//! - [`components`] is the component list the running release embeds.
//! - [`subjects`] matches the release's components and the nodes' kernels.

pub mod cache;
pub mod changes;
pub mod components;
pub mod ecosystem;
pub mod fetch;
pub mod findings;
pub mod import;
pub mod ingest;
pub mod kev;
pub mod matcher;
pub mod mirror;
pub mod osv;
pub mod severity;
pub mod status;
pub mod subjects;
pub mod version;

pub use cache::{AffectedRow, BucketState, Cache};
pub use fetch::Endpoints;
pub use import::import_dir;
pub use mirror::Vulns;
