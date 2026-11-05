// SPDX-License-Identifier: AGPL-3.0-only
//! CISA's Known Exploited Vulnerabilities catalog.
//!
//! KEV is held like one more ecosystem: `osv_mirror` has a `KEV` row, it is
//! refreshed with the daily pass, and an offline import can feed it from
//! `known_exploited_vulnerabilities.json`. Everything KEV-specific lives in
//! this module behind [`KEV`], so the reading can move if the contract changes.
//! The data's age is the catalog's `dateReleased`.

use super::cache::Cache;
use serde::Deserialize;
use std::io::Read as _;
use std::path::Path;

/// The name of the KEV row in `osv_mirror`.
pub const KEV: &str = "KEV";

/// The file an offline import reads.
pub const KEV_FILE: &str = "known_exploited_vulnerabilities.json";

/// The largest catalog accepted, in bytes.
const MAX_BYTES: u64 = 32 * 1024 * 1024;

/// The entries of one catalog: CVE id and the date it was added.
pub type Entries = Vec<(String, Option<String>)>;

/// The catalog as the feed wraps it.
#[derive(Deserialize)]
struct Catalog {
    #[serde(rename = "dateReleased", default)]
    date_released: Option<String>,
    #[serde(default)]
    vulnerabilities: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    #[serde(rename = "cveID", default)]
    cve_id: String,
    #[serde(rename = "dateAdded", default)]
    date_added: Option<String>,
}

/// Parses the catalog body.
///
/// Returns the entries and the catalog's `dateReleased`. An entry without a
/// `cveID` is skipped.
///
/// # Errors
///
/// The body is larger than the cap or not the catalog's JSON.
pub fn parse(body: &[u8]) -> Result<(Entries, Option<String>), String> {
    if body.len() as u64 > MAX_BYTES {
        return Err(format!(
            "the KEV catalog is larger than the {MAX_BYTES} byte cap"
        ));
    }
    let catalog: Catalog = serde_json::from_slice(body)
        .map_err(|error| format!("cannot read the KEV catalog: {error}"))?;
    let entries = catalog
        .vulnerabilities
        .into_iter()
        .filter(|entry| !entry.cve_id.is_empty() && entry.cve_id.len() <= 200)
        .map(|entry| (entry.cve_id, entry.date_added))
        .collect();
    Ok((entries, catalog.date_released))
}

/// Reads `path` and replaces the cache's KEV catalog with it.
///
/// Returns the number of entries and the catalog's `dateReleased`.
///
/// # Errors
///
/// The file cannot be read, or is over the cap or not the catalog's JSON.
pub async fn import_kev(cache: &Cache, path: &Path) -> Result<(u64, Option<String>), String> {
    let file = std::fs::File::open(path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let mut body = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if body.len() as u64 > MAX_BYTES {
        return Err(format!(
            "{} is larger than the {MAX_BYTES} byte cap",
            path.display()
        ));
    }
    let (entries, date) = parse(&body)?;
    let count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    cache
        .replace_kev(entries)
        .await
        .map_err(|error| format!("cannot write the KEV catalog: {error}"))?;
    Ok((count, date))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{
        "title": "CISA Catalog of Known Exploited Vulnerabilities",
        "catalogVersion": "2026.11.04",
        "dateReleased": "2026-11-04T20:00:05.774Z",
        "count": 2,
        "vulnerabilities": [
            {"cveID": "CVE-2024-27834", "dateAdded": "2026-11-04", "vendorProject": "Apple"},
            {"cveID": "CVE-2023-0001", "dateAdded": "2026-10-01"}
        ]
    }"#;

    #[test]
    fn the_catalog_keeps_its_ids_and_release_date() {
        let (entries, date) = parse(BODY.as_bytes()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "CVE-2024-27834");
        assert_eq!(entries[0].1.as_deref(), Some("2026-11-04"));
        assert_eq!(date.as_deref(), Some("2026-11-04T20:00:05.774Z"));
    }

    #[test]
    fn a_body_that_is_not_the_catalog_is_an_error() {
        assert!(parse(b"not json").is_err());
    }

    #[tokio::test]
    async fn an_import_replaces_the_catalog() {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(KEV_FILE);
        std::fs::write(&path, BODY).unwrap();
        let (cache, _) = Cache::open(&dir.join("vulns-cache.db")).await.unwrap();
        let (count, date) = import_kev(&cache, &path).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(date.as_deref(), Some("2026-11-04T20:00:05.774Z"));
        assert!(cache.is_kev(&["CVE-2024-27834"]).await.unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
