// SPDX-License-Identifier: AGPL-3.0-only
//! Reading OSV records and the ecosystem archives that hold them.
//!
//! One archive is `<base>/<Bucket>/all.zip`, a zip of `<id>.json` files. It is
//! walked one entry at a time, so the archive and its records are never held
//! in memory together, and every entry is bounded: a header that lies about
//! its size is caught while reading. A record that is too large, malformed or
//! lacks its `id` or `modified` is skipped and counted, never a fatal error.
//!
//! The record shape is the OSV schema (<https://ossf.github.io/osv-schema/>).
//! Only the fields the matcher needs are kept: `details` and every unknown
//! field are dropped, and `GIT` ranges are dropped because a commit cannot be
//! ordered against a package version.

use serde::{Deserialize, Serialize};
use std::io::Read as _;
use std::path::Path;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The largest single record this primary accepts, in bytes.
pub const MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;

/// The most entries one archive may hold.
pub const MAX_ENTRIES: usize = 2_000_000;

/// The longest summary kept, in characters.
const MAX_SUMMARY_CHARS: usize = 1_000;

/// The most aliases or upstream ids kept per record.
const MAX_IDS: usize = 200;

/// The longest alias or upstream id kept, in characters.
const MAX_ID_CHARS: usize = 200;

/// One vulnerability record, in the shape the cache stores.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub modified: String,
    pub published: Option<String>,
    pub withdrawn: Option<String>,
    pub summary: Option<String>,
    pub aliases: Vec<String>,
    pub upstream: Vec<String>,
    pub severity: Vec<Severity>,
    /// `database_specific.severity` when it is a string, for example `HIGH`.
    pub db_severity: Option<String>,
    pub affected: Vec<Affected>,
}

/// One severity entry: the scoring system and its vector.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Severity {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub score: String,
}

/// One package an affected entry names, with its version ranges.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Affected {
    pub ecosystem: String,
    pub package: String,
    pub ranges: Vec<Range>,
    pub versions: Vec<String>,
}

/// One version range. Only `ECOSYSTEM` and `SEMVER` ranges are kept.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Range {
    #[serde(rename = "type", default)]
    pub kind: String,
    pub events: Vec<Event>,
}

/// One event of a range: where a vulnerable or fixed version starts or ends.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Event {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub introduced: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affected: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,
}

/// What one archive read produced.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportStats {
    /// Records kept.
    pub records: u64,
    /// Entries that were `.json` but unusable.
    pub skipped: u64,
    /// The newest `modified` held: its unix nanos and its original string.
    pub newest_modified: Option<(i128, String)>,
}

/// The raw OSV shape: unknown fields are ignored, absent ones defaulted.
#[derive(Deserialize)]
struct RawRecord {
    id: Option<String>,
    modified: Option<String>,
    #[serde(default)]
    published: Option<String>,
    #[serde(default)]
    withdrawn: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    aliases: Vec<String>,
    #[serde(default)]
    upstream: Vec<String>,
    #[serde(default)]
    severity: Vec<Severity>,
    #[serde(default)]
    affected: Vec<RawAffected>,
    #[serde(default)]
    database_specific: Option<RawDatabaseSpecific>,
}

#[derive(Deserialize)]
struct RawDatabaseSpecific {
    #[serde(default)]
    severity: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawAffected {
    #[serde(default)]
    package: Option<RawPackage>,
    #[serde(default)]
    ranges: Vec<RawRange>,
    #[serde(default)]
    versions: Vec<String>,
}

#[derive(Deserialize)]
struct RawPackage {
    #[serde(default)]
    ecosystem: String,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct RawRange {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    events: Vec<Event>,
}

/// The unix timestamp of an RFC 3339 `modified` value, in nanoseconds.
///
/// OSV times carry between zero and nine fractional digits, so they are never
/// compared as strings: `…:04.3Z` sorts before `…:04Z` as text but after it in
/// time. `None` when the value is not RFC 3339.
#[must_use]
pub fn modified_nanos(value: &str) -> Option<i128> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(|time| time.unix_timestamp_nanos())
}

/// Parses one record body, applying the size and field bounds.
///
/// `None` means the record is unusable: malformed JSON, no `id`, no `modified`
/// or a `modified` that is not RFC 3339. The caller counts it as skipped.
#[must_use]
pub fn parse_record(body: &[u8]) -> Option<Record> {
    if body.len() as u64 > MAX_ENTRY_BYTES {
        return None;
    }
    let raw: RawRecord = serde_json::from_slice(body).ok()?;
    raw.into_record()
}

impl RawRecord {
    fn into_record(self) -> Option<Record> {
        let id = self.id.filter(|id| valid_identifier(id))?;
        let modified = self.modified.filter(|m| !m.is_empty())?;
        modified_nanos(&modified)?;
        let summary = self.summary.map(|summary| {
            if summary.chars().count() > MAX_SUMMARY_CHARS {
                summary.chars().take(MAX_SUMMARY_CHARS).collect()
            } else {
                summary
            }
        });
        let aliases = clean_ids(self.aliases);
        let upstream = clean_ids(self.upstream);
        let db_severity = self
            .database_specific
            .and_then(|specific| specific.severity)
            .and_then(|value| value.as_str().map(str::to_string));
        let mut affected = Vec::with_capacity(self.affected.len());
        for entry in self.affected {
            let Some(package) = entry.package else {
                continue;
            };
            if package.name.is_empty() || package.ecosystem.is_empty() {
                continue;
            }
            let ranges = entry
                .ranges
                .into_iter()
                .filter(|range| range.kind == "ECOSYSTEM" || range.kind == "SEMVER")
                .map(|range| Range {
                    kind: range.kind,
                    events: range.events,
                })
                .collect();
            affected.push(Affected {
                ecosystem: package.ecosystem,
                package: package.name,
                ranges,
                versions: entry.versions,
            });
        }
        Some(Record {
            id,
            modified,
            published: self.published,
            withdrawn: self.withdrawn,
            summary,
            aliases,
            upstream,
            severity: self.severity,
            db_severity,
            affected,
        })
    }
}

/// Keeps the ids that fit the bounds, in order, at most [`MAX_IDS`] of them.
fn clean_ids(ids: Vec<String>) -> Vec<String> {
    ids.into_iter()
        .filter(|id| valid_identifier(id))
        .take(MAX_IDS)
        .collect()
}

fn valid_identifier(id: &str) -> bool {
    !id.is_empty() && id.chars().count() <= MAX_ID_CHARS && !id.chars().any(char::is_control)
}

/// Walks a bucket archive one `.json` entry at a time.
pub struct ZipReader {
    archive: zip::ZipArchive<std::fs::File>,
    index: usize,
    stats: ImportStats,
    error: Option<String>,
}

impl ZipReader {
    /// Opens `path` and reads its central directory.
    ///
    /// # Errors
    ///
    /// The file cannot be opened or is not a zip, or it declares more entries
    /// than [`MAX_ENTRIES`].
    pub fn open(path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path)
            .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        let archive = zip::ZipArchive::new(file)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        if archive.len() > MAX_ENTRIES {
            return Err(format!(
                "{} holds {} entries, more than the {MAX_ENTRIES} allowed",
                path.display(),
                archive.len()
            ));
        }
        Ok(Self {
            archive,
            index: 0,
            stats: ImportStats::default(),
            error: None,
        })
    }

    /// The next usable record, or `None` at the end or after a fatal error.
    ///
    /// Directories and entries that are not `.json` are passed over without
    /// being counted; a `.json` entry that is too large, malformed or lacks
    /// `id`/`modified` is counted as skipped.
    pub fn next_record(&mut self) -> Option<Record> {
        if self.error.is_some() {
            return None;
        }
        while self.index < self.archive.len() {
            let index = self.index;
            self.index += 1;
            let mut entry = match self.archive.by_index(index) {
                Ok(entry) => entry,
                Err(error) => {
                    self.error = Some(format!("cannot read entry {index}: {error}"));
                    return None;
                }
            };
            if entry.is_dir() {
                continue;
            }
            let name = match entry.name() {
                Ok(name) => name.to_string(),
                Err(_) => {
                    self.stats.skipped += 1;
                    continue;
                }
            };
            if !name.ends_with(".json") {
                continue;
            }
            if entry.size() > MAX_ENTRY_BYTES {
                self.stats.skipped += 1;
                continue;
            }
            let mut body = Vec::new();
            if let Err(error) = entry
                .by_ref()
                .take(MAX_ENTRY_BYTES + 1)
                .read_to_end(&mut body)
            {
                self.error = Some(format!("cannot read {name}: {error}"));
                return None;
            }
            // The header may lie about the size; the bytes read are the truth.
            if body.len() as u64 > MAX_ENTRY_BYTES {
                self.stats.skipped += 1;
                continue;
            }
            let Some(record) = parse_record(&body) else {
                self.stats.skipped += 1;
                continue;
            };
            count_stat(&mut self.stats, &record);
            return Some(record);
        }
        None
    }

    /// The counts of the whole read.
    ///
    /// # Errors
    ///
    /// The archive failed part-way through; the records read before it are
    /// already handed to the caller.
    pub fn finish(self) -> Result<ImportStats, String> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.stats),
        }
    }
}

/// Counts one kept record into `stats`.
fn count_stat(stats: &mut ImportStats, record: &Record) {
    stats.records += 1;
    if let Some(nanos) = modified_nanos(&record.modified) {
        let newer = stats
            .newest_modified
            .as_ref()
            .is_none_or(|(current, _)| nanos > *current);
        if newer {
            stats.newest_modified = Some((nanos, record.modified.clone()));
        }
    }
}

/// Reads every record of the archive at `path`, applying `replace_bucket`'s
/// batching in the caller.
///
/// The iterator borrows the reader; call [`ZipReader::finish`] afterwards for
/// the counts.
pub fn records(reader: &mut ZipReader) -> impl Iterator<Item = Record> + Send + '_ {
    std::iter::from_fn(|| reader.next_record())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use zip::write::SimpleFileOptions;

    fn nanos(value: &str) -> i128 {
        modified_nanos(value).unwrap()
    }

    #[test]
    fn mixed_fractional_precision_compares_in_time_not_as_text() {
        let whole = nanos("2026-11-04T06:45:04Z");
        let half = nanos("2026-11-04T06:45:04.5Z");
        let fine = nanos("2026-11-04T06:45:04.325296987Z");
        assert!(half > whole);
        assert!(fine > whole);
        assert!(half > fine, "a longer fraction is more precise, not larger");
        // As strings the fraction would sort first.
        assert!("2026-11-04T06:45:04.3Z" < "2026-11-04T06:45:04Z");
        assert!(modified_nanos("not a time").is_none());
    }

    #[test]
    fn a_debian_record_keeps_the_cve_in_upstream() {
        let body = br#"{
            "id": "DSA-1234-1",
            "modified": "2026-10-02T01:29:11.123456789Z",
            "published": "2026-09-01T00:00:00Z",
            "summary": "openssl update",
            "details": "long text that is not stored",
            "upstream": ["CVE-2026-1111"],
            "affected": [{
                "package": {"ecosystem": "Debian:13", "name": "openssl"},
                "ranges": [{"type": "ECOSYSTEM", "events": [
                    {"introduced": "0"}, {"fixed": "3.0.11-1"}
                ]}]
            }]
        }"#;
        let record = parse_record(body).unwrap();
        assert_eq!(record.id, "DSA-1234-1");
        assert_eq!(record.upstream, vec!["CVE-2026-1111"]);
        assert!(record.aliases.is_empty());
        assert_eq!(record.affected.len(), 1);
        assert_eq!(record.affected[0].ecosystem, "Debian:13");
        assert_eq!(record.affected[0].package, "openssl");
        assert_eq!(record.affected[0].ranges[0].kind, "ECOSYSTEM");
        assert_eq!(
            record.affected[0].ranges[0].events[1].fixed.as_deref(),
            Some("3.0.11-1")
        );
    }

    #[test]
    fn an_npm_record_keeps_semver_and_drops_git() {
        let body = br#"{
            "id": "GHSA-v653-r55g-hcmg",
            "modified": "2026-11-04T06:45:04.325296987Z",
            "affected": [{
                "package": {"ecosystem": "npm", "name": "left-pad"},
                "ranges": [
                    {"type": "SEMVER", "events": [{"introduced": "0"}, {"fixed": "1.3.0"}]},
                    {"type": "GIT", "events": [{"introduced": "abc123"}, {"fixed": "def456"}]}
                ],
                "versions": ["1.0.0"]
            }]
        }"#;
        let record = parse_record(body).unwrap();
        assert_eq!(record.affected[0].ranges.len(), 1);
        assert_eq!(record.affected[0].ranges[0].kind, "SEMVER");
        assert_eq!(record.affected[0].versions, vec!["1.0.0"]);
    }

    #[test]
    fn a_withdrawn_record_is_kept_with_its_withdrawal() {
        let body = br#"{
            "id": "GHSA-aaaa",
            "modified": "2026-01-02T00:00:00Z",
            "withdrawn": "2026-01-03T00:00:00Z"
        }"#;
        let record = parse_record(body).unwrap();
        assert_eq!(record.withdrawn.as_deref(), Some("2026-01-03T00:00:00Z"));
        assert!(record.affected.is_empty());
    }

    #[test]
    fn database_specific_severity_is_kept_when_it_is_a_string() {
        let body = br#"{
            "id": "CVE-2024-1",
            "modified": "2026-01-02T00:00:00Z",
            "database_specific": {"severity": "HIGH", "other": 1},
            "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L"}]
        }"#;
        let record = parse_record(body).unwrap();
        assert_eq!(record.db_severity.as_deref(), Some("HIGH"));
        assert_eq!(record.severity[0].kind, "CVSS_V3");
        assert_eq!(record.severity[0].score, "CVSS:3.1/AV:N/AC:L");
    }

    #[test]
    fn bounds_apply_to_summary_and_ids() {
        let long_summary = "x".repeat(2_000);
        let mut aliases: Vec<String> = (0..250).map(|n| format!("CVE-{n}")).collect();
        aliases.push("y".repeat(300));
        let body = format!(
            r#"{{"id":"CVE-1","modified":"2026-01-02T00:00:00Z","summary":{},
                "aliases":{},"upstream":[]}}"#,
            serde_json::to_string(&long_summary).unwrap(),
            serde_json::to_string(&aliases).unwrap()
        );
        let record = parse_record(body.as_bytes()).unwrap();
        assert_eq!(record.summary.unwrap().chars().count(), 1_000);
        assert_eq!(record.aliases.len(), MAX_IDS);
    }

    fn write_zip(path: &Path, entries: &[(&str, &[u8])]) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        let options = SimpleFileOptions::default();
        for (name, body) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(body).unwrap();
        }
        writer.finish().unwrap();
    }

    #[test]
    fn an_archive_is_walked_entry_by_entry_and_bad_entries_are_counted() {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bucket.zip");
        let good = br#"{"id":"CVE-2024-1","modified":"2026-01-02T00:00:00Z"}"#;
        let withdrawn = br#"{"id":"CVE-2024-2","modified":"2026-02-02T00:00:00Z","withdrawn":"2026-02-03T00:00:00Z"}"#;
        write_zip(
            &path,
            &[
                ("records/", b""),
                ("notes.txt", b"ignored, not json"),
                ("broken.json", b"{ this is not json"),
                ("no-id.json", br#"{"modified":"2026-01-02T00:00:00Z"}"#),
                ("no-modified.json", br#"{"id":"CVE-2024-9"}"#),
                ("CVE-2024-1.json", good),
                ("CVE-2024-2.json", withdrawn),
            ],
        );
        let mut reader = ZipReader::open(&path).unwrap();
        let mut ids = Vec::new();
        while let Some(record) = reader.next_record() {
            ids.push(record.id);
        }
        let stats = reader.finish().unwrap();
        assert_eq!(ids, vec!["CVE-2024-1", "CVE-2024-2"]);
        assert_eq!(stats.records, 2);
        assert_eq!(stats.skipped, 3);
        assert_eq!(stats.newest_modified.unwrap().1, "2026-02-02T00:00:00Z");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_oversized_entry_is_skipped_not_read() {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bucket.zip");
        let big = vec![b' '; (MAX_ENTRY_BYTES + 1) as usize];
        write_zip(&path, &[("big.json", &big)]);
        let mut reader = ZipReader::open(&path).unwrap();
        assert!(reader.next_record().is_none());
        assert_eq!(reader.finish().unwrap().skipped, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
