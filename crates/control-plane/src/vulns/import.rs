// SPDX-License-Identifier: AGPL-3.0-only
//! The offline import: `quoro vuln import <dir>`.
//!
//! The directory mirrors the bucket layout, so
//! `curl -o Debian/all.zip <base>/Debian/all.zip` builds one:
//!
//! - `<dir>/<Bucket>/all.zip` for any bucket in the allowlist;
//! - `<dir>/known_exploited_vulnerabilities.json` for KEV, optional.
//!
//! A zip is read in place, never copied and never unpacked to disk. Unknown
//! subdirectories are reported on stderr and ignored; a directory with nothing
//! recognized is an error, so a mistyped path does not quietly import nothing.

use super::cache::Cache;
use super::ecosystem;
use super::kev::{self, KEV, KEV_FILE};
use super::osv::{self, ZipReader};
use std::path::Path;

/// What one import brought in: the bucket (or `KEV`), its record count and the
/// newest record modification it held.
pub type Imported = Vec<(String, u64, Option<String>)>;

/// Imports every archive and the KEV catalog `dir` holds.
///
/// The cache buckets are replaced in place and the KEV catalog swapped; the
/// `osv_mirror` rows are the caller's job, because they say who imported the
/// data and when.
///
/// # Errors
///
/// `dir` cannot be read, an archive or the catalog cannot be read, the cache
/// cannot be written, or nothing in `dir` is recognized.
pub async fn import_dir(cache: &Cache, dir: &Path) -> Result<Imported, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("cannot read {}: {error}", dir.display()))?;
    let mut buckets: Vec<(String, std::path::PathBuf)> = Vec::new();
    let mut kev_path: Option<std::path::PathBuf> = None;
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot read {}: {error}", dir.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        if path.is_dir() {
            buckets.push((name, path));
        } else if name == KEV_FILE {
            kev_path = Some(path);
        }
    }
    buckets.sort_by(|left, right| left.0.cmp(&right.0));

    let mut imported: Imported = Vec::new();
    for (name, path) in buckets {
        if !ecosystem::is_bucket(&name) {
            eprintln!(
                "quoro vuln import: ignoring {}: not an OSV ecosystem Quoro knows",
                path.display()
            );
            continue;
        }
        let zip = path.join("all.zip");
        if !zip.is_file() {
            eprintln!(
                "quoro vuln import: ignoring {}: it holds no all.zip",
                path.display()
            );
            continue;
        }
        let (count, modified) = import_zip(cache, &name, &zip).await?;
        imported.push((name, count, modified));
    }
    if let Some(path) = kev_path {
        let (count, date) = kev::import_kev(cache, &path).await?;
        imported.push((KEV.to_string(), count, date));
    }

    if imported.is_empty() {
        return Err(format!(
            "no OSV data found in {}: expected <Bucket>/all.zip or {KEV_FILE}",
            dir.display()
        ));
    }
    Ok(imported)
}

/// Reads one bucket archive into the cache.
async fn import_zip(
    cache: &Cache,
    bucket: &str,
    path: &Path,
) -> Result<(u64, Option<String>), String> {
    let mut reader = ZipReader::open(path)?;
    let count = cache
        .replace_bucket(bucket, osv::records(&mut reader))
        .await
        .map_err(|error| format!("cannot write the cache: {error}"))?;
    let stats = reader.finish()?;
    if stats.skipped > 0 {
        tracing::debug!(
            bucket,
            skipped = stats.skipped,
            "records skipped in archive"
        );
    }
    Ok((count, stats.newest_modified.map(|(_, modified)| modified)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use zip::write::SimpleFileOptions;

    fn write_zip(path: &Path, entries: &[(&str, &str)]) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        for (name, body) in entries {
            writer
                .start_file(*name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(body.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    fn temp_dir() -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const KEV_BODY: &str = r#"{
        "dateReleased": "2026-11-04T20:00:05.774Z",
        "vulnerabilities": [
            {"cveID": "CVE-2024-27834", "dateAdded": "2026-11-04"}
        ]
    }"#;

    #[tokio::test]
    async fn a_directory_imports_its_buckets_and_kev() {
        let dir = temp_dir();
        let debian = dir.join("Debian");
        std::fs::create_dir_all(&debian).unwrap();
        write_zip(
            &debian.join("all.zip"),
            &[
                (
                    "CVE-2024-1.json",
                    r#"{"id":"CVE-2024-1","modified":"2026-10-01T00:00:00Z",
                        "affected":[{"package":{"ecosystem":"Debian:13","name":"openssl"},
                        "ranges":[{"type":"ECOSYSTEM","events":[{"introduced":"0"},{"fixed":"3.0"}]}]}]}"#,
                ),
                (
                    "CVE-2024-2.json",
                    r#"{"id":"CVE-2024-2","modified":"2026-11-01T00:00:00Z"}"#,
                ),
            ],
        );
        std::fs::write(dir.join(KEV_FILE), KEV_BODY).unwrap();
        let (cache, _) = Cache::open(&dir.join("vulns-cache.db")).await.unwrap();

        let imported = import_dir(&cache, &dir).await.unwrap();
        assert_eq!(
            imported,
            vec![
                (
                    "Debian".to_string(),
                    2,
                    Some("2026-11-01T00:00:00Z".to_string())
                ),
                (
                    "KEV".to_string(),
                    1,
                    Some("2026-11-04T20:00:05.774Z".to_string())
                ),
            ]
        );
        assert_eq!(
            cache
                .affected_for("Debian:13", "openssl")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(cache.is_kev(&["CVE-2024-27834"]).await.unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_empty_directory_is_an_error() {
        let dir = temp_dir();
        let (cache, _) = Cache::open(&dir.join("vulns-cache.db")).await.unwrap();
        let error = import_dir(&cache, &dir).await.unwrap_err();
        assert!(error.contains("no OSV data found"), "{error}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_unknown_subdirectory_is_ignored() {
        let dir = temp_dir();
        let unknown = dir.join("Fedora");
        std::fs::create_dir_all(&unknown).unwrap();
        std::fs::write(unknown.join("all.zip"), b"not a zip").unwrap();
        let debian = dir.join("Debian");
        std::fs::create_dir_all(&debian).unwrap();
        write_zip(
            &debian.join("all.zip"),
            &[(
                "CVE-2024-1.json",
                r#"{"id":"CVE-2024-1","modified":"2026-10-01T00:00:00Z"}"#,
            )],
        );
        let (cache, _) = Cache::open(&dir.join("vulns-cache.db")).await.unwrap();
        let imported = import_dir(&cache, &dir).await.unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].0, "Debian");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
