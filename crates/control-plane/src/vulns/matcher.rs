// SPDX-License-Identifier: AGPL-3.0-only
//! Matching an image inventory against the local OSV data.
//!
//! For every package the matcher looks up the records that describe it: OSV
//! keys a distribution's records by SOURCE package, so `libssl3t64` is looked
//! up as `openssl`; a PyPI package is also looked up under its PEP 503
//! normalized name. [`crate::vulns::version::evaluate`] decides whether the
//! installed version is affected.
//!
//! OSV publishes one vulnerability several times (a Debian `DEBIAN-CVE-…`, a
//! `DSA-…` advisory listing it, Linux's `CVE-…`/`GSD-…`/`UVI-…`). [`collapse`]
//! keeps the most useful record per package: one with a CVSS score and a CVE
//! id first, then drops a record whose CVE ids are already covered.

use crate::vulns::cache::{AffectedRow, Cache};
use crate::vulns::severity::{band, cvss};
use crate::vulns::version::{Order, evaluate, order_for};
use quoro_proto::inventory::InventoryObject;
use quoro_proto::wire::InventoryPackageWire;
use std::collections::HashSet;

/// Most candidates one inventory may produce.
const MAX_CANDIDATES: usize = 5_000;

/// Most aliases one candidate keeps.
const MAX_ALIASES: usize = 200;

/// One vulnerability found in one package of an inventory.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// The OSV record id.
    pub vuln_id: String,
    /// Other ids of the record, sorted and deduplicated.
    pub aliases: Vec<String>,
    pub summary: Option<String>,
    pub ecosystem: String,
    pub package: String,
    /// The source package the record was looked up under, when it differs.
    pub source_package: Option<String>,
    pub installed_version: String,
    pub fixed_version: Option<String>,
    /// The band derived from `cvss_score`.
    pub severity: &'static str,
    pub cvss_score: Option<f64>,
    pub cvss_vector: Option<String>,
    /// Whether the id or an alias is in the KEV catalog.
    pub kev: bool,
}

/// All findings of one inventory.
///
/// # Errors
///
/// Any database error reading the cache.
pub async fn match_inventory(
    cache: &Cache,
    inventory: &InventoryObject,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let mut candidates = Vec::new();
    for package in &inventory.packages {
        let Some(ecosystem_name) = package_ecosystem(package) else {
            continue;
        };
        candidates.extend(
            match_package_parts(
                cache,
                ecosystem_name,
                &package.name,
                &package.version,
                package.source_name.as_deref(),
            )
            .await?,
        );
    }
    if candidates.len() > MAX_CANDIDATES {
        candidates.sort_by(most_severe);
        tracing::debug!(
            candidates = candidates.len(),
            kept = MAX_CANDIDATES,
            "image inventory has more findings than the cap"
        );
        candidates.truncate(MAX_CANDIDATES);
    }
    Ok(candidates)
}

/// The candidates of one package whose ecosystem is known, matched exactly the
/// way [`match_inventory`] matches an inventory's packages: the same range
/// evaluation, withdrawn records, alias collapsing, KEV and CVSS.
///
/// # Errors
///
/// Any database error reading the cache.
pub async fn match_package(
    cache: &Cache,
    ecosystem: &str,
    package: &str,
    version: &str,
) -> Result<Vec<Candidate>, sqlx::Error> {
    match_package_parts(cache, ecosystem, package, version, None).await
}

/// One package's candidates. `source_name` is the source package an inventory
/// names, which only a distribution's records are keyed by.
async fn match_package_parts(
    cache: &Cache,
    ecosystem: &str,
    package: &str,
    version: &str,
    source_name: Option<&str>,
) -> Result<Vec<Candidate>, sqlx::Error> {
    if order_for(ecosystem).is_none() {
        return Ok(Vec::new());
    }
    let names = lookup_names(ecosystem, package, source_name);
    // Only a distribution's source package is a different package; a
    // PyPI normalized name is the same one spelled differently.
    let by_source = is_distro(ecosystem);
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    for lookup in names {
        for row in cache.affected_for(ecosystem, &lookup).await? {
            if seen.insert(row.id.clone()) {
                found.push((lookup.clone(), row));
            }
        }
    }
    let mut package_candidates = Vec::new();
    for (lookup, row) in found {
        if row.withdrawn.is_some() {
            continue;
        }
        let Some(hit) = evaluate(ecosystem, version, &row.ranges, &row.versions) else {
            continue;
        };
        let aliases = aliases(&row);
        let (cvss_score, cvss_vector) = match cvss(&row.severity) {
            Some((score, vector)) => (Some(score), Some(vector)),
            None => (None, None),
        };
        let kev = cache.is_kev(&kev_ids(&row.id, &aliases)).await?;
        package_candidates.push(Candidate {
            vuln_id: row.id,
            aliases,
            summary: row.summary,
            ecosystem: ecosystem.to_string(),
            package: package.to_string(),
            source_package: (by_source && lookup != package).then_some(lookup),
            installed_version: version.to_string(),
            fixed_version: hit.fixed,
            severity: band(cvss_score),
            cvss_score,
            cvss_vector,
            kev,
        });
    }
    Ok(collapse(package_candidates))
}

/// The order `trusted` decides for a package, or `None` when its ecosystem
/// cannot be ordered (and therefore cannot be matched).
fn package_ecosystem(package: &InventoryPackageWire) -> Option<&str> {
    (!package.ecosystem.is_empty())
        .then_some(package.ecosystem.as_str())
        .filter(|name| !name.contains('/'))
}

/// Whether `ecosystem` is a distribution, whose OSV records name source
/// packages.
fn is_distro(ecosystem: &str) -> bool {
    order_for(ecosystem)
        .is_some_and(|order| matches!(order, Order::Debian | Order::Apk | Order::Rpm))
}

/// The package names to look up, most specific first.
fn lookup_names(ecosystem: &str, package: &str, source_name: Option<&str>) -> Vec<String> {
    if is_distro(ecosystem) {
        // OSV keys a distribution's records by source package, so the binary
        // name is never looked up when a source package is named.
        return match source_name.filter(|source| !source.is_empty()) {
            Some(source) => vec![source.to_owned()],
            None => vec![package.to_owned()],
        };
    }
    let mut names = Vec::new();
    if ecosystem == "PyPI" {
        let normalized = normalize_pypi(package);
        if normalized != package && !normalized.is_empty() {
            names.push(normalized);
        }
    }
    names.push(package.to_owned());
    names
}

/// PEP 503: lowercase, runs of `-_.` become one `-`.
fn normalize_pypi(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut separator = false;
    for character in name.chars() {
        if matches!(character, '-' | '_' | '.') {
            if !out.is_empty() && !separator {
                out.push('-');
                separator = true;
            }
        } else {
            out.push(character.to_ascii_lowercase());
            separator = false;
        }
    }
    let trimmed = out.trim_end_matches('-');
    trimmed.to_string()
}

/// The record's other ids: aliases plus upstream, without the id, sorted,
/// deduplicated, at most [`MAX_ALIASES`].
fn aliases(row: &AffectedRow) -> Vec<String> {
    let mut aliases: Vec<String> = row
        .aliases
        .iter()
        .chain(row.upstream.iter())
        .filter(|alias| **alias != row.id)
        .cloned()
        .collect();
    aliases.sort();
    aliases.dedup();
    aliases.truncate(MAX_ALIASES);
    aliases
}

/// The ids the KEV catalog is checked against: the record id and its aliases.
fn kev_ids<'a>(id: &'a str, aliases: &'a [String]) -> Vec<&'a str> {
    let mut ids = Vec::with_capacity(aliases.len() + 1);
    ids.push(id);
    ids.extend(aliases.iter().map(String::as_str));
    ids
}

/// `cvss` returns `(score, vector)`; both are kept or both are absent.
/// Keeps the most useful record per CVE set for one package.
fn collapse(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut candidates = candidates;
    candidates.sort_by(|left, right| {
        right
            .cvss_score
            .is_some()
            .cmp(&left.cvss_score.is_some())
            .then_with(|| cve_first(right).cmp(&cve_first(left)))
            .then_with(|| left.vuln_id.cmp(&right.vuln_id))
    });
    let mut kept: Vec<Candidate> = Vec::with_capacity(candidates.len());
    let mut covered: HashSet<String> = HashSet::new();
    for candidate in candidates {
        let ids = cve_ids(&candidate);
        // A record whose CVE ids are already covered is the same bug said
        // again; one with no CVE id cannot be compared and is kept.
        if !ids.is_empty() && ids.is_subset(&covered) {
            continue;
        }
        covered.extend(ids);
        kept.push(candidate);
    }
    kept
}

/// Whether `candidate`'s id looks like a CVE record.
fn cve_first(candidate: &Candidate) -> bool {
    candidate.vuln_id.starts_with("CVE-") || candidate.vuln_id.contains("-CVE-")
}

/// The `CVE-…` strings among a candidate's id and aliases.
fn cve_ids(candidate: &Candidate) -> HashSet<String> {
    let mut ids = HashSet::new();
    if candidate.vuln_id.starts_with("CVE-") {
        ids.insert(candidate.vuln_id.clone());
    }
    for alias in &candidate.aliases {
        if alias.starts_with("CVE-") {
            ids.insert(alias.clone());
        }
    }
    ids
}

/// Orders candidates for truncation: most severe first, KEV first within a
/// band, then by id.
fn most_severe(left: &Candidate, right: &Candidate) -> std::cmp::Ordering {
    severity_rank(right.severity)
        .cmp(&severity_rank(left.severity))
        .then_with(|| right.kev.cmp(&left.kev))
        .then_with(|| left.vuln_id.cmp(&right.vuln_id))
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vulns::osv::{Affected, Event, Range, Record, Severity};
    use quoro_proto::wire::InventoryPackageWire;
    use std::path::PathBuf;

    fn temp_path() -> (PathBuf, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("quoro-matcher-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vulns-cache.db");
        (dir, path)
    }

    fn range(fixed: Option<&str>) -> Range {
        Range {
            kind: "ECOSYSTEM".to_owned(),
            events: vec![
                Event {
                    introduced: Some("0".to_owned()),
                    ..Default::default()
                },
                Event {
                    fixed: fixed.map(str::to_owned),
                    ..Default::default()
                },
            ],
        }
    }

    fn affected(ecosystem: &str, package: &str, fixed: Option<&str>) -> Affected {
        Affected {
            ecosystem: ecosystem.to_owned(),
            package: package.to_owned(),
            ranges: vec![range(fixed)],
            versions: Vec::new(),
        }
    }

    fn critical() -> Vec<Severity> {
        vec![Severity {
            kind: "CVSS_V3".to_owned(),
            score: "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H".to_owned(),
        }]
    }

    fn package(
        ecosystem: &str,
        name: &str,
        source: Option<&str>,
        version: &str,
    ) -> InventoryPackageWire {
        InventoryPackageWire {
            ecosystem: ecosystem.to_owned(),
            name: name.to_owned(),
            source_name: source.map(str::to_owned),
            version: version.to_owned(),
            purl: format!("pkg:generic/{name}@{version}"),
            path: None,
        }
    }

    fn inventory(packages: Vec<InventoryPackageWire>) -> InventoryObject {
        InventoryObject {
            image_digest: "sha256:aaaa".to_owned(),
            inventoried_at: "2026-11-05T06:09:41Z".to_owned(),
            os: None,
            ecosystems: vec!["Debian:13".to_owned()],
            not_inventoried: Vec::new(),
            package_count: packages.len() as u64,
            packages,
        }
    }

    #[tokio::test]
    async fn a_binary_package_is_looked_up_by_its_source_name() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "CVE-2024-1".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    summary: Some("openssl bug".to_owned()),
                    severity: critical(),
                    affected: vec![affected("Debian:13", "openssl", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package(
            "Debian:13",
            "libssl3t64",
            Some("openssl"),
            "3.5.1-1",
        )]);
        let found = match_inventory(&cache, &object).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].package, "libssl3t64");
        assert_eq!(found[0].source_package.as_deref(), Some("openssl"));
        assert_eq!(found[0].fixed_version.as_deref(), Some("3.5.4-1"));
        assert_eq!(found[0].severity, "critical");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_record_on_the_binary_name_alone_is_not_found() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "CVE-2024-1".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    affected: vec![affected("Debian:13", "libssl3t64", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package(
            "Debian:13",
            "libssl3t64",
            Some("openssl"),
            "3.5.1-1",
        )]);
        assert!(match_inventory(&cache, &object).await.unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_pypi_name_is_also_looked_up_normalized() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "PyPI",
                vec![Record {
                    id: "GHSA-xxxx".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    affected: vec![affected("PyPI", "my-package", Some("2.0"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package("PyPI", "My_Package", None, "1.0")]);
        let found = match_inventory(&cache, &object).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].package, "My_Package");
        assert_eq!(found[0].source_package, None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_withdrawn_record_is_skipped() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "CVE-2024-1".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    withdrawn: Some("2026-02-01T00:00:00Z".to_owned()),
                    affected: vec![affected("Debian:13", "openssl", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package("Debian:13", "openssl", None, "3.5.1-1")]);
        assert!(match_inventory(&cache, &object).await.unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_unaffected_version_is_skipped() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "CVE-2024-1".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    affected: vec![affected("Debian:13", "openssl", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package("Debian:13", "openssl", None, "3.5.4-1")]);
        assert!(match_inventory(&cache, &object).await.unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn kev_is_found_through_an_alias() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "DSA-5000".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    upstream: vec!["CVE-2024-27834".to_owned()],
                    severity: critical(),
                    affected: vec![affected("Debian:13", "openssl", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        cache
            .replace_kev(vec![(
                "CVE-2024-27834".to_owned(),
                Some("2026-01-01".to_owned()),
            )])
            .await
            .unwrap();
        let object = inventory(vec![package("Debian:13", "openssl", None, "3.5.1-1")]);
        let found = match_inventory(&cache, &object).await.unwrap();
        assert_eq!(found.len(), 1);
        assert!(found[0].kev);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_ecosystem_without_an_order_is_skipped() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![Record {
                    id: "CVE-2024-1".to_owned(),
                    modified: "2026-01-01T00:00:00Z".to_owned(),
                    affected: vec![affected("Fedora", "openssl", Some("3.5.4-1"))],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let object = inventory(vec![package("Fedora", "openssl", None, "3.5.1-1")]);
        assert!(match_inventory(&cache, &object).await.unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn candidate(id: &str, score: Option<f64>, aliases: &[&str]) -> Candidate {
        Candidate {
            vuln_id: id.to_owned(),
            aliases: aliases.iter().map(|alias| (*alias).to_owned()).collect(),
            summary: None,
            ecosystem: "Debian:13".to_owned(),
            package: "openssl".to_owned(),
            source_package: None,
            installed_version: "3.5.1-1".to_owned(),
            fixed_version: Some("3.5.4-1".to_owned()),
            severity: band(score),
            cvss_score: score,
            cvss_vector: None,
            kev: false,
        }
    }

    #[test]
    fn a_debian_cve_drops_the_advisory_that_lists_it() {
        let kept = collapse(vec![
            candidate("DSA-5000", None, &["CVE-2024-1", "CVE-2024-2"]),
            candidate("DEBIAN-CVE-2024-1", Some(9.8), &["CVE-2024-1"]),
            candidate("DEBIAN-CVE-2024-2", Some(7.5), &["CVE-2024-2"]),
        ]);
        let ids: Vec<&str> = kept.iter().map(|c| c.vuln_id.as_str()).collect();
        assert_eq!(ids, vec!["DEBIAN-CVE-2024-1", "DEBIAN-CVE-2024-2"]);
    }

    #[test]
    fn a_gsd_duplicate_of_a_cve_is_dropped() {
        let kept = collapse(vec![
            candidate("GSD-2024-1", None, &["CVE-2024-1"]),
            candidate("CVE-2024-1", Some(9.8), &[]),
        ]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].vuln_id, "CVE-2024-1");
    }

    #[test]
    fn a_candidate_without_a_cve_id_is_kept() {
        let kept = collapse(vec![
            candidate("CVE-2024-1", Some(9.8), &[]),
            candidate("GSD-2024-2", None, &[]),
        ]);
        assert_eq!(kept.len(), 2);
    }
}
