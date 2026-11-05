// SPDX-License-Identifier: AGPL-3.0-only
//! Which OSV bucket an inventory ecosystem name maps to.
//!
//! An inventory names what the image reports, for example `Debian:13`,
//! `Alpine:v3.22`, `openSUSE:Leap 15.5` or `npm`. The OSV bucket is the name
//! before the first `:`. Only the buckets in [`BUCKETS`] are supported; any
//! other name maps to `None`, so no URL is ever built for it.

/// Ecosystems the primary always keeps, whatever the inventories say.
///
/// `Linux` carries the kernel records, and no inventory names the kernel.
pub const ALWAYS: &[&str] = &["Linux"];

/// Every OSV bucket this primary knows how to fetch.
///
/// The distribution names are what an inventory's `os-release` reports, and
/// the package ecosystems are what a manifest reports. OSV serves one bucket
/// per name, at `<base>/<Bucket>/all.zip`.
pub const BUCKETS: &[&str] = &[
    "AlmaLinux",
    "Alpine",
    "Azure Linux",
    "Chainguard",
    "Debian",
    "Go",
    "Hex",
    "Linux",
    "Mageia",
    "Maven",
    "NuGet",
    "Packagist",
    "Pub",
    "PyPI",
    "Red Hat",
    "Rocky Linux",
    "RubyGems",
    "SUSE",
    "Ubuntu",
    "Wolfi",
    "crates.io",
    "npm",
    "openEuler",
    "openSUSE",
];

/// The bucket holding `ecosystem`'s records, or `None` when it is unsupported.
///
/// Distribution names carry a version after a colon (`Debian:13`), which the
/// bucket does not; package ecosystems have no colon. A name with a `/` is a
/// path fragment, never an ecosystem, and is refused.
#[must_use]
pub fn bucket_for(ecosystem: &str) -> Option<&'static str> {
    if ecosystem.is_empty() || ecosystem.contains('/') {
        return None;
    }
    let head = ecosystem
        .split_once(':')
        .map_or(ecosystem, |(head, _)| head);
    BUCKETS.iter().copied().find(|bucket| *bucket == head)
}

/// Whether `name` is a bucket this primary fetches.
#[must_use]
pub fn is_bucket(name: &str) -> bool {
    BUCKETS.contains(&name)
}

/// Percent-encodes one URL path segment, or `None` when it cannot be one.
///
/// Only unreserved characters stay as they are, so `Red Hat` becomes
/// `Red%20Hat`. Control characters, an empty segment, `.` and `..` are
/// refused: they could escape the bucket's directory.
#[must_use]
pub fn encode_segment(segment: &str) -> Option<String> {
    if segment.is_empty() || segment == "." || segment == ".." {
        return None;
    }
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte < 0x20 || byte == 0x7f {
            return None;
        }
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    Some(out)
}

/// Whether `id` is a record id a URL may name.
///
/// Record ids are `GHSA-…`, `CVE-…`, `RUSTSEC-…` and the like. A name that is
/// a file the bucket also serves (`all.zip`, `modified_id.csv`) is refused, so
/// a delta can never download the bucket archive as if it were a record.
#[must_use]
pub fn valid_record_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 200 || id.contains('/') {
        return false;
    }
    if id == "." || id == ".." || id == "all" {
        return false;
    }
    if id.ends_with(".zip") || id.ends_with(".csv") {
        return false;
    }
    id.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'+'))
}

/// `<osv_base>/<Bucket>/all.zip`, the whole bucket.
///
/// The top-level `<osv_base>/all.zip` holds every ecosystem and must never be
/// requested; refusing a name that is not in [`BUCKETS`] guarantees that no
/// URL this function builds can be it.
#[must_use]
pub fn all_zip_url(osv_base: &str, bucket: &str) -> Option<String> {
    let bucket = bucket_segment(bucket)?;
    Some(format!(
        "{}/{bucket}/all.zip",
        osv_base.trim_end_matches('/')
    ))
}

/// `<osv_base>/<Bucket>/modified_id.csv`, the bucket's change list.
#[must_use]
pub fn modified_csv_url(osv_base: &str, bucket: &str) -> Option<String> {
    let bucket = bucket_segment(bucket)?;
    Some(format!(
        "{}/{bucket}/modified_id.csv",
        osv_base.trim_end_matches('/')
    ))
}

/// `<osv_base>/<Bucket>/<id>.json`, one record.
#[must_use]
pub fn record_url(osv_base: &str, bucket: &str, id: &str) -> Option<String> {
    let bucket = bucket_segment(bucket)?;
    if !valid_record_id(id) {
        return None;
    }
    let id = encode_segment(id)?;
    Some(format!(
        "{}/{bucket}/{id}.json",
        osv_base.trim_end_matches('/')
    ))
}

fn bucket_segment(bucket: &str) -> Option<String> {
    if !is_bucket(bucket) {
        return None;
    }
    encode_segment(bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_allowlist_maps_versions_and_packages_to_their_bucket() {
        assert_eq!(bucket_for("Debian:13"), Some("Debian"));
        assert_eq!(bucket_for("Debian"), Some("Debian"));
        assert_eq!(bucket_for("Alpine:v3.22"), Some("Alpine"));
        assert_eq!(bucket_for("Red Hat"), Some("Red Hat"));
        assert_eq!(bucket_for("Rocky Linux:9"), Some("Rocky Linux"));
        assert_eq!(
            bucket_for("openSUSE:Leap 15.5"),
            Some("openSUSE"),
            "the inventory sends the product after the colon"
        );
        assert_eq!(
            bucket_for("SUSE:Linux Enterprise Server 15.4"),
            Some("SUSE")
        );
        assert_eq!(bucket_for("npm"), Some("npm"));
        assert_eq!(bucket_for("crates.io"), Some("crates.io"));
        assert_eq!(bucket_for("Go"), Some("Go"));
    }

    #[test]
    fn unsupported_names_build_no_bucket() {
        for name in [
            "",
            "RPM",
            "Fedora",
            "all",
            ".",
            "..",
            "../x",
            "a/b",
            "Debian/13",
            "Kernel",
        ] {
            assert_eq!(bucket_for(name), None, "{name:?} must be unsupported");
        }
    }

    #[test]
    fn a_bucket_with_a_space_is_percent_encoded() {
        let url = all_zip_url("https://example.test/base", "Red Hat").unwrap();
        assert_eq!(url, "https://example.test/base/Red%20Hat/all.zip");
        let url = record_url("https://example.test/base", "Red Hat", "CVE-2024-1").unwrap();
        assert_eq!(url, "https://example.test/base/Red%20Hat/CVE-2024-1.json");
        let url = modified_csv_url("https://example.test/base/", "Rocky Linux").unwrap();
        assert_eq!(
            url,
            "https://example.test/base/Rocky%20Linux/modified_id.csv"
        );
    }

    #[test]
    fn no_buildable_url_is_the_top_level_archive() {
        let base = "https://osv-vulnerabilities.storage.googleapis.com";
        let top = format!("{base}/all.zip");
        for bucket in BUCKETS {
            assert_ne!(all_zip_url(base, bucket).unwrap(), top);
        }
        for refused in ["", ".", "..", "all", "../x", "a/b"] {
            assert_eq!(all_zip_url(base, refused), None, "{refused:?}");
        }
    }

    #[test]
    fn a_record_id_cannot_name_a_file_of_the_bucket() {
        for id in [
            "",
            ".",
            "..",
            "all",
            "all.zip",
            "modified_id.csv",
            "../x",
            "a/b",
        ] {
            assert_eq!(record_url("https://x.test", "Debian", id), None, "{id:?}");
        }
        assert!(record_url("https://x.test", "Debian", "GHSA-v653-r55g-hcmg").is_some());
    }

    #[test]
    fn the_always_set_is_linux() {
        assert_eq!(ALWAYS, &["Linux"]);
        assert!(is_bucket("Linux"));
    }
}
