// SPDX-License-Identifier: AGPL-3.0-only
//! Version order per ecosystem and OSV range evaluation.
//!
//! An inventory writes a version the way its package manager does
//! (`1:2.36-9+deb12u4`, `3.5.4-r0`, `1.1.1k-12.el8_9`, `2.31.0`, `v1.2.3`),
//! and an OSV record states its ranges in that same order. [`order_for`] maps
//! an ecosystem name to the [`Order`] that compares its versions, [`compare`]
//! compares two versions, and [`evaluate`] walks an `affected` entry's ranges
//! and version list against one installed version.
//!
//! Everything here reads untrusted strings from images and from downloaded
//! records: no function panics, every input is bounded by [`MAX_LEN`], and a
//! string the order cannot parse yields `None` instead of a guess.

mod apk;
mod kernel;
mod rpm;
mod semver;

use crate::vulns::osv::{Event, Range};
use std::cmp::Ordering;
use std::str::FromStr as _;

/// The longest version string that is parsed, in bytes.
///
/// Longer strings come from a hostile image or record and are treated as
/// unparseable rather than given more per-version work.
const MAX_LEN: usize = 256;

/// How one ecosystem orders its versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Debian and Ubuntu, dpkg's `[epoch:]upstream[-revision]` order.
    Debian,
    /// Alpine's apk-tools order (`3.5.4-r0`, `1.2_rc1-r0`).
    Apk,
    /// RPM's `[epoch:]version[-release]` order.
    Rpm,
    /// Python packages, PEP 440.
    Pep440,
    /// The SemVer-like order of the package ecosystems, with a generic
    /// fallback for strings that are not SemVer.
    Semver,
    /// The Linux kernel's leading dotted numeric release.
    Kernel,
}

/// The order for an OSV ecosystem name as records and inventories write it
/// (the part before the first ':' decides for distributions). None = Quoro
/// cannot order it.
#[must_use]
pub fn order_for(ecosystem: &str) -> Option<Order> {
    let head = ecosystem
        .split_once(':')
        .map_or(ecosystem, |(head, _)| head);
    const DEBIAN: &[&str] = &["Debian", "Ubuntu"];
    const APK: &[&str] = &["Alpine", "Wolfi", "Chainguard"];
    const RPM: &[&str] = &[
        "Red Hat",
        "Rocky Linux",
        "AlmaLinux",
        "openSUSE",
        "SUSE",
        "Azure Linux",
        "Mageia",
        "openEuler",
    ];
    const SEMVER: &[&str] = &[
        "npm",
        "crates.io",
        "Go",
        "NuGet",
        "Hex",
        "Pub",
        "Packagist",
        "RubyGems",
        "Maven",
    ];
    if DEBIAN.iter().any(|name| head.starts_with(name)) {
        return Some(Order::Debian);
    }
    if APK.iter().any(|name| head.starts_with(name)) {
        return Some(Order::Apk);
    }
    if RPM.iter().any(|name| head.starts_with(name)) {
        return Some(Order::Rpm);
    }
    if head == "PyPI" {
        return Some(Order::Pep440);
    }
    if SEMVER.contains(&head) {
        return Some(Order::Semver);
    }
    if head == "Linux" {
        return Some(Order::Kernel);
    }
    None
}

/// Compares two versions in `order`. None when either cannot be parsed.
#[must_use]
pub fn compare(order: Order, a: &str, b: &str) -> Option<Ordering> {
    if !within_bounds(a) || !within_bounds(b) {
        return None;
    }
    match order {
        Order::Debian => {
            let left = debversion::Version::from_str(a).ok()?;
            let right = debversion::Version::from_str(b).ok()?;
            Some(left.cmp(&right))
        }
        Order::Apk => apk::compare(a, b),
        Order::Rpm => rpm::compare(a, b),
        Order::Pep440 => {
            let left = pep440_rs::Version::from_str(a).ok()?;
            let right = pep440_rs::Version::from_str(b).ok()?;
            Some(left.cmp(&right))
        }
        Order::Semver => Some(semver::compare(a, b)),
        Order::Kernel => kernel::compare(a, b),
    }
}

/// Whether a version string is worth parsing at all.
fn within_bounds(version: &str) -> bool {
    !version.is_empty() && version.len() <= MAX_LEN
}

/// Compares two digit runs as integers, without letting them overflow.
fn cmp_digits(a: &[u8], b: &[u8]) -> Ordering {
    let a = strip_zeros(a);
    let b = strip_zeros(b);
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// Drops the leading zeros of a digit run; an all-zero run becomes empty.
fn strip_zeros(digits: &[u8]) -> &[u8] {
    let mut start = 0;
    while digits.get(start) == Some(&b'0') {
        start += 1;
    }
    digits.get(start..).unwrap_or(&[])
}

/// Why a package is affected: the range or version list that matched, and the
/// lowest fix after the installed version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hit {
    /// The lowest `fixed` value above the installed version, if there is one.
    pub fixed: Option<String>,
}

/// Evaluates one OSV `affected` entry against an installed version.
///
/// `ecosystem` picks the order for `ECOSYSTEM` ranges; a `SEMVER` range always
/// uses [`Order::Semver`]. Some(hit) when affected, None when not affected OR
/// when the version cannot be ordered (the caller counts that separately via
/// [`order_for`]/[`compare`]).
#[must_use]
pub fn evaluate(
    ecosystem: &str,
    installed: &str,
    ranges: &[Range],
    versions: &[String],
) -> Option<Hit> {
    if !within_bounds(installed) {
        return None;
    }
    let ecosystem_order = order_for(ecosystem);
    let ranges = series_ranges(ecosystem_order, installed, ranges);
    let mut affected = false;
    let mut fix: Option<&str> = None;
    for range in ranges {
        let order = match range.kind.as_str() {
            "SEMVER" => Order::Semver,
            "ECOSYSTEM" => match ecosystem_order {
                Some(order) => order,
                None => continue,
            },
            // A GIT range names commits, which Quoro cannot order; `limit`
            // is only meaningful there.
            _ => continue,
        };
        let Some(result) = evaluate_range(order, installed, &range.events) else {
            continue;
        };
        if !result.affected {
            continue;
        }
        affected = true;
        for candidate in result.fixed {
            // Only a fix above the installed version answers "what do I
            // upgrade to"; the lowest of them wins across ranges.
            if compare(order, installed, candidate) != Some(Ordering::Less) {
                continue;
            }
            let lower = match fix {
                // A fix from another order cannot be compared with this one;
                // keep the first answer in that case.
                Some(current) => compare(order, candidate, current) == Some(Ordering::Less),
                None => true,
            };
            if lower {
                fix = Some(candidate);
            }
        }
    }
    let listed = versions.iter().any(|version| {
        version == installed
            || ecosystem_order.and_then(|order| compare(order, installed, version))
                == Some(Ordering::Equal)
    });
    if !affected && !listed {
        return None;
    }
    Some(Hit {
        fixed: fix.map(str::to_owned),
    })
}

/// The ranges that decide for `installed`.
///
/// A Linux kernel record writes one range per stable series, and the ranges
/// overlap: `introduced 6.2.0, fixed 6.12.24` next to `introduced 6.12.0,
/// fixed 6.13.12` says "6.12 is fixed in 6.12.24 and 6.13 in 6.13.12", yet
/// read as plain OSV ranges the second one flags every 6.12 release after the
/// fix. So for the Linux ecosystem (whose only package is the kernel), when
/// any `ECOSYSTEM` range names a fix in the installed version's own
/// `major.minor` series, only the ranges that name such a fix are read: the
/// version is affected when it is below that fix and at or above the range's
/// introduced version. Without a fix in its series, every range is read as
/// usual. Other ecosystems always read every range.
fn series_ranges<'a>(
    ecosystem_order: Option<Order>,
    installed: &str,
    ranges: &'a [Range],
) -> Vec<&'a Range> {
    let own_series = match ecosystem_order {
        Some(Order::Kernel) => kernel::series(installed),
        _ => None,
    };
    let Some(own_series) = own_series else {
        return ranges.iter().collect();
    };
    let in_series: Vec<&Range> = ranges
        .iter()
        .filter(|range| {
            range.kind == "ECOSYSTEM"
                && range.events.iter().any(|event| {
                    event
                        .fixed
                        .as_deref()
                        .is_some_and(|fixed| kernel::series(fixed) == Some(own_series))
                })
        })
        .collect();
    if in_series.is_empty() {
        ranges.iter().collect()
    } else {
        in_series
    }
}

/// What one usable range said.
struct RangeResult<'a> {
    /// Whether the installed version is inside the range at the end of the walk.
    affected: bool,
    /// Every `fixed` value the range names.
    fixed: Vec<&'a str>,
}

/// Which of the three version-carrying event fields one event names.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Introduced,
    Fixed,
    LastAffected,
}

/// Evaluates one range, or None when it cannot be ordered (a version in it
/// does not parse, or the installed version does not).
fn evaluate_range<'a>(
    order: Order,
    installed: &str,
    events: &'a [Event],
) -> Option<RangeResult<'a>> {
    // The installed version must be orderable in this range's order first.
    compare(order, installed, installed)?;
    let mut walked: Vec<(Kind, &'a str)> = Vec::with_capacity(events.len());
    for event in events {
        let named = [
            (Kind::Introduced, event.introduced.as_deref()),
            (Kind::Fixed, event.fixed.as_deref()),
            (Kind::LastAffected, event.last_affected.as_deref()),
        ];
        let mut picked: Option<(Kind, &'a str)> = None;
        for (kind, value) in named {
            let Some(value) = value else { continue };
            // One event names one version; anything else is malformed.
            if picked.is_some() {
                return None;
            }
            picked = Some((kind, value));
        }
        // An event that only carries `limit` (or nothing) says nothing here.
        let Some((kind, version)) = picked else {
            continue;
        };
        // `introduced: "0"` is the lowest of all and need not parse; every
        // other version must, or the whole range is guessed work.
        if !(kind == Kind::Introduced && version == "0")
            && compare(order, version, version).is_none()
        {
            return None;
        }
        walked.push((kind, version));
    }
    walked.sort_by(|left, right| event_key(order, *left, *right));
    let fixed = walked
        .iter()
        .filter(|(kind, _)| *kind == Kind::Fixed)
        .map(|(_, version)| *version)
        .collect();
    let mut affected = false;
    for (kind, version) in walked {
        let reached = match kind {
            Kind::Introduced => {
                version == "0" || compare(order, installed, version) != Some(Ordering::Less)
            }
            Kind::Fixed => compare(order, installed, version) != Some(Ordering::Less),
            Kind::LastAffected => compare(order, installed, version) == Some(Ordering::Greater),
        };
        match kind {
            Kind::Introduced => {
                if reached {
                    affected = true;
                }
            }
            Kind::Fixed | Kind::LastAffected => {
                if reached {
                    affected = false;
                }
            }
        }
    }
    Some(RangeResult { affected, fixed })
}

/// Orders two events by their version, with `introduced: "0"` first.
///
/// The sort is stable, so two events at the same version keep the record's
/// order, the same way the reference evaluator walks them.
fn event_key(order: Order, left: (Kind, &str), right: (Kind, &str)) -> Ordering {
    let left_low = left.0 == Kind::Introduced && left.1 == "0";
    let right_low = right.0 == Kind::Introduced && right.1 == "0";
    match (left_low, right_low) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => compare(order, left.1, right.1).unwrap_or(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vulns::osv::{Event, Range};

    fn event(introduced: Option<&str>, fixed: Option<&str>, last_affected: Option<&str>) -> Event {
        Event {
            introduced: introduced.map(str::to_owned),
            fixed: fixed.map(str::to_owned),
            last_affected: last_affected.map(str::to_owned),
            limit: None,
        }
    }

    fn ecosystem_range(events: Vec<Event>) -> Range {
        Range {
            kind: "ECOSYSTEM".to_owned(),
            events,
        }
    }

    fn semver_range(events: Vec<Event>) -> Range {
        Range {
            kind: "SEMVER".to_owned(),
            events,
        }
    }

    #[test]
    fn order_for_maps_every_ecosystem() {
        let cases = [
            ("Debian", Some(Order::Debian)),
            ("Debian:13", Some(Order::Debian)),
            ("Ubuntu:24.04", Some(Order::Debian)),
            ("Alpine", Some(Order::Apk)),
            ("Alpine:v3.22", Some(Order::Apk)),
            ("Wolfi", Some(Order::Apk)),
            ("Chainguard", Some(Order::Apk)),
            ("Red Hat", Some(Order::Rpm)),
            ("Rocky Linux:9", Some(Order::Rpm)),
            ("AlmaLinux:9", Some(Order::Rpm)),
            ("openSUSE:Leap 15.5", Some(Order::Rpm)),
            ("SUSE", Some(Order::Rpm)),
            ("Azure Linux:3.0", Some(Order::Rpm)),
            ("Mageia:9", Some(Order::Rpm)),
            ("openEuler:24.03", Some(Order::Rpm)),
            ("PyPI", Some(Order::Pep440)),
            ("npm", Some(Order::Semver)),
            ("crates.io", Some(Order::Semver)),
            ("Go", Some(Order::Semver)),
            ("NuGet", Some(Order::Semver)),
            ("Hex", Some(Order::Semver)),
            ("Pub", Some(Order::Semver)),
            ("Packagist", Some(Order::Semver)),
            ("RubyGems", Some(Order::Semver)),
            ("Maven", Some(Order::Semver)),
            ("Linux", Some(Order::Kernel)),
            ("Fedora", None),
            ("RPM", None),
            ("", None),
            ("GIT", None),
        ];
        for (name, expected) in cases {
            assert_eq!(order_for(name), expected, "{name}");
        }
    }

    #[test]
    fn compare_rejects_impossible_inputs() {
        let long = "1.".repeat(200);
        let cases = [
            (Order::Debian, ""),
            (Order::Apk, ""),
            (Order::Rpm, ""),
            (Order::Pep440, ""),
            (Order::Semver, ""),
            (Order::Kernel, ""),
        ];
        for (order, empty) in cases {
            assert_eq!(compare(order, empty, "1.0"), None, "{order:?} empty");
            assert_eq!(compare(order, "1.0", empty), None, "{order:?} empty");
            assert_eq!(compare(order, long.as_str(), "1.0"), None, "{order:?}");
            assert_eq!(compare(order, "1.0", long.as_str()), None, "{order:?}");
        }
        // Garbage that no order can read is None where the order rejects it.
        assert_eq!(compare(Order::Kernel, "no-digits", "1.0"), None);
        assert_eq!(compare(Order::Debian, "1.0 2.0", "1.0"), None);
        assert_eq!(compare(Order::Apk, "1.0 junk", "1.0"), None);
    }

    #[test]
    fn compare_equal_and_ordering_per_order() {
        let cases = [
            (Order::Debian, "1:2.36-9+deb12u4", "1:2.36-9+deb12u4", 0),
            (Order::Debian, "2.3.1-2~bpo12+1", "2.3.1-2", -1),
            (Order::Apk, "2.40_rc1-r0", "2.40-r0", -1),
            (Order::Rpm, "0:1.13.1-3.el9_3.3", "1.13.1-3.el9_3.3", 0),
            (Order::Pep440, "1.0", "1.0.0", 0),
            (Order::Semver, "v1.2.3", "1.2.3", 0),
            (Order::Kernel, "6.12.113+deb13-amd64", "6.12.113", 0),
        ];
        for (order, a, b, expected) in cases {
            let ordering = compare(order, a, b).expect("parses");
            let expected = match expected {
                -1 => Ordering::Less,
                0 => Ordering::Equal,
                _ => Ordering::Greater,
            };
            assert_eq!(ordering, expected, "{order:?} {a} {b}");
            assert_eq!(compare(order, b, a), Some(expected.reverse()));
        }
    }

    #[test]
    fn debian_and_pep440_tables() {
        let cases = [
            (Order::Debian, "3.5.1-1", "3.5.4-1", Ordering::Less),
            (
                Order::Debian,
                "1:2.36-9+deb12u4",
                "2.36-10",
                Ordering::Greater,
            ),
            (Order::Debian, "2.3.1-2~bpo12+1", "2.3.1-2", Ordering::Less),
            (Order::Debian, "3.5.4-1", "3.5.4-1", Ordering::Equal),
            (Order::Debian, "1.0", "1.0-1", Ordering::Less),
            (Order::Pep440, "1.0rc1", "1.0", Ordering::Less),
            (Order::Pep440, "1.0", "1.0.post1", Ordering::Less),
            (Order::Pep440, "1.0.dev1", "1.0a1", Ordering::Less),
            (Order::Pep440, "2!1.0", "1.5", Ordering::Greater),
            (Order::Pep440, "1.0", "1.0.0", Ordering::Equal),
        ];
        for (order, a, b, expected) in cases {
            assert_eq!(compare(order, a, b), Some(expected), "{order:?} {a} {b}");
            assert_eq!(compare(order, b, a), Some(expected.reverse()));
        }
        // Unparseable in the order's own grammar.
        for text in ["not a version", "1.0_", "1.0 2.0", "é"] {
            assert_eq!(compare(Order::Debian, text, "1.0"), None, "{text}");
        }
        for text in ["", "not a version", "1.0-", "1..0", "rc1"] {
            assert_eq!(compare(Order::Pep440, text, "1.0"), None, "{text}");
        }
    }

    #[test]
    fn evaluate_unorderable_installed_is_none() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("2.0"), None),
        ])];
        assert_eq!(evaluate("Debian", "not a version", &ranges, &[]), None);
        assert_eq!(evaluate("Linux", "quoro", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_introduced_and_fixed() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("2.0"), None),
        ])];
        assert_eq!(
            evaluate("PyPI", "1.5", &ranges, &[]),
            Some(Hit {
                fixed: Some("2.0".to_owned())
            })
        );
        assert!(evaluate("PyPI", "1.9.9", &ranges, &[]).is_some());
        assert_eq!(evaluate("PyPI", "2.0", &ranges, &[]), None);
        assert_eq!(evaluate("PyPI", "2.1", &ranges, &[]), None);
        // Exactly at `introduced` is affected.
        let ranges = [ecosystem_range(vec![
            event(Some("1.5"), None, None),
            event(None, Some("2.0"), None),
        ])];
        assert_eq!(
            evaluate("PyPI", "1.5", &ranges, &[]),
            Some(Hit {
                fixed: Some("2.0".to_owned())
            })
        );
        assert_eq!(evaluate("PyPI", "1.4", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_several_pairs_in_one_range() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("1.0"), None),
            event(Some("2.0"), None, None),
            event(None, Some("2.5"), None),
        ])];
        assert_eq!(evaluate("PyPI", "1.5", &ranges, &[]), None);
        assert_eq!(
            evaluate("PyPI", "2.1", &ranges, &[]),
            Some(Hit {
                fixed: Some("2.5".to_owned())
            })
        );
        assert_eq!(evaluate("PyPI", "3.0", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_last_affected_is_inclusive() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, None, Some("1.9")),
        ])];
        assert_eq!(
            evaluate("npm", "1.9", &ranges, &[]),
            Some(Hit { fixed: None })
        );
        assert_eq!(evaluate("npm", "1.9.1", &ranges, &[]), None);
        assert_eq!(
            evaluate("npm", "0.1", &ranges, &[]),
            Some(Hit { fixed: None })
        );
    }

    #[test]
    fn evaluate_version_list_without_ranges() {
        let versions = vec!["1.0.0".to_owned(), "2.0".to_owned()];
        assert_eq!(
            evaluate("npm", "2.0.0", &[], &versions),
            Some(Hit { fixed: None })
        );
        assert_eq!(evaluate("npm", "3.0.0", &[], &versions), None);
        // Unknown ecosystem: only string equality can match.
        assert_eq!(
            evaluate("Unknown", "2.0", &[], &versions),
            Some(Hit { fixed: None })
        );
        assert_eq!(evaluate("Unknown", "2.0.0", &[], &versions), None);
    }

    #[test]
    fn evaluate_skips_a_range_with_an_unparseable_event() {
        let ranges = [
            ecosystem_range(vec![
                event(Some("0"), None, None),
                event(None, Some("not a version"), None),
            ]),
            ecosystem_range(vec![
                event(Some("9.0"), None, None),
                event(None, Some("10.0"), None),
            ]),
        ];
        // The first range is skipped, the second does not cover 1.0.
        assert_eq!(evaluate("PyPI", "1.0", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_ignores_git_ranges_and_limits() {
        let ranges = [Range {
            kind: "GIT".to_owned(),
            events: vec![event(Some("0"), None, None)],
        }];
        assert_eq!(evaluate("npm", "1.0.0", &ranges, &[]), None);
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            Event {
                introduced: None,
                fixed: None,
                last_affected: None,
                limit: Some("5.0".to_owned()),
            },
        ])];
        assert_eq!(
            evaluate("PyPI", "1.0", &ranges, &[]),
            Some(Hit { fixed: None })
        );
    }

    #[test]
    fn evaluate_semver_range_under_npm() {
        let ranges = [semver_range(vec![
            event(Some("1.2.3"), None, None),
            event(None, Some("1.3.0"), None),
        ])];
        assert_eq!(
            evaluate("npm", "1.2.4", &ranges, &[]),
            Some(Hit {
                fixed: Some("1.3.0".to_owned())
            })
        );
        assert_eq!(evaluate("npm", "1.3.0", &ranges, &[]), None);
        // SEMVER does not consult the ecosystem's order.
        assert_eq!(
            evaluate("PyPI", "1.2.4", &ranges, &[]),
            Some(Hit {
                fixed: Some("1.3.0".to_owned())
            })
        );
    }

    #[test]
    fn evaluate_debian_openssl() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("3.5.4-1"), None),
        ])];
        assert_eq!(
            evaluate("Debian:13", "3.5.1-1", &ranges, &[]),
            Some(Hit {
                fixed: Some("3.5.4-1".to_owned())
            })
        );
        assert_eq!(evaluate("Debian:13", "3.5.4-1", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_alpine_openssl() {
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("3.5.4-r0"), None),
        ])];
        assert_eq!(evaluate("Alpine:v3.22", "3.5.4-r0", &ranges, &[]), None);
        assert_eq!(
            evaluate("Alpine:v3.22", "3.5.3-r1", &ranges, &[]),
            Some(Hit {
                fixed: Some("3.5.4-r0".to_owned())
            })
        );
    }

    #[test]
    fn evaluate_kernel_ranges() {
        let ranges = [
            ecosystem_range(vec![
                event(Some("4.4.0"), None, None),
                event(None, Some("5.10.272"), None),
            ]),
            ecosystem_range(vec![
                event(Some("6.19.0"), None, None),
                event(None, Some("7.2.10"), None),
            ]),
        ];
        assert_eq!(evaluate("Linux", "7.2.15", &ranges, &[]), None);
        assert_eq!(
            evaluate("Linux", "7.2.8", &ranges, &[]),
            Some(Hit {
                fixed: Some("7.2.10".to_owned())
            })
        );
        assert_eq!(evaluate("Linux", "5.10.272", &ranges, &[]), None);
        assert_eq!(evaluate("Linux", "4.3.9", &ranges, &[]), None);
        // The installed string may carry a distribution suffix.
        assert!(evaluate("Linux", "7.2.8-quoro", &ranges, &[]).is_some());
    }

    /// A Linux record's ranges, each an introduced version and its fixes.
    fn kernel_record(ranges: &[(&str, &[&str])]) -> Vec<Range> {
        ranges
            .iter()
            .map(|(introduced, fixes)| {
                let mut events = vec![event(Some(introduced), None, None)];
                events.extend(fixes.iter().map(|fixed| event(None, Some(fixed), None)));
                ecosystem_range(events)
            })
            .collect()
    }

    fn fixed_in(version: &str) -> Option<Hit> {
        Some(Hit {
            fixed: Some(version.to_owned()),
        })
    }

    #[test]
    fn evaluate_kernel_reads_the_fix_of_its_own_series() {
        // CVE-2025-37750 as OSV publishes it: one range per stable series.
        let record = kernel_record(&[
            ("0", &["6.1.191"]),
            ("6.2.0", &["6.12.24"]),
            ("6.12.0", &["6.13.12"]),
            ("6.13.0", &["6.14.3"]),
        ]);
        // 6.12.115 is past the 6.12 fix, although `6.12.0..6.13.12` contains it.
        assert_eq!(evaluate("Linux", "6.12.115", &record, &[]), None);
        assert_eq!(
            evaluate("Linux", "6.12.115+deb13-amd64", &record, &[]),
            None
        );
        // A 6.12 release below the 6.12 fix is still affected, and told that fix.
        assert_eq!(
            evaluate("Linux", "6.12.20", &record, &[]),
            fixed_in("6.12.24")
        );
        assert_eq!(evaluate("Linux", "6.12.24", &record, &[]), None);
        assert_eq!(
            evaluate("Linux", "6.13.5", &record, &[]),
            fixed_in("6.13.12")
        );
        assert_eq!(
            evaluate("Linux", "6.1.190", &record, &[]),
            fixed_in("6.1.191")
        );
        assert_eq!(evaluate("Linux", "6.1.191", &record, &[]), None);
        // No fix in 6.15, and no range contains it: not affected.
        assert_eq!(evaluate("Linux", "6.15.2", &record, &[]), None);
    }

    #[test]
    fn evaluate_kernel_guest_release_past_its_series_fix() {
        // Three records from the Linux data whose plain reading flags 7.2.15
        // through the range that opens the next series.
        let records = [
            kernel_record(&[
                ("0", &["5.10.273"]),
                ("5.11.0", &["5.15.224"]),
                ("5.16.0", &["6.1.191"]),
                ("6.2.0", &["6.6.161"]),
                ("6.7.0", &["6.12.115"]),
                ("6.13.0", &["6.18.60"]),
                ("6.19.0", &["7.2.14"]),
                ("7.2.0", &["7.3.1"]),
            ]),
            kernel_record(&[
                ("0", &["5.15.224"]),
                ("5.16.0", &["6.1.191", "6.6.161"]),
                ("6.2.0", &["6.12.115"]),
                ("6.7.0", &["6.18.60"]),
                ("6.13.0", &["7.2.14"]),
                ("6.19.0", &["7.3.1"]),
            ]),
            kernel_record(&[
                ("0", &["6.1.191"]),
                ("6.2.0", &["6.6.161"]),
                ("6.7.0", &["6.12.115"]),
                ("6.10.0", &["6.18.60"]),
                ("6.13.0", &["7.2.14"]),
                ("6.19.0", &["7.3.1"]),
            ]),
        ];
        for record in &records {
            assert_eq!(evaluate("Linux", "7.2.15", record, &[]), None);
            assert_eq!(evaluate("Linux", "6.12.115", record, &[]), None);
            assert_eq!(evaluate("Linux", "7.2.13", record, &[]), fixed_in("7.2.14"));
            assert_eq!(
                evaluate("Linux", "6.12.100", record, &[]),
                fixed_in("6.12.115")
            );
        }
    }

    #[test]
    fn evaluate_kernel_without_a_fix_in_its_series_reads_every_range() {
        // Introduced in 6.7 and fixed only in 6.18: 6.12 has no fix of its own.
        let record = kernel_record(&[("6.7.0", &["6.18.60"]), ("6.19.0", &["7.3.1"])]);
        assert_eq!(
            evaluate("Linux", "6.12.115", &record, &[]),
            fixed_in("6.18.60")
        );
        assert_eq!(evaluate("Linux", "7.2.15", &record, &[]), fixed_in("7.3.1"));
        assert_eq!(evaluate("Linux", "6.6.1", &record, &[]), None);
    }

    #[test]
    fn evaluate_series_rule_is_only_for_linux() {
        // The same overlapping shape under a package ecosystem keeps the plain
        // OSV reading.
        let record = kernel_record(&[("6.2.0", &["6.12.24"]), ("6.12.0", &["6.13.12"])]);
        assert_eq!(
            evaluate("crates.io", "6.12.115", &record, &[]),
            fixed_in("6.13.12")
        );
    }

    #[test]
    fn evaluate_picks_the_lowest_fix_above_installed() {
        let ranges = [
            ecosystem_range(vec![
                event(Some("0"), None, None),
                event(None, Some("2.0"), None),
            ]),
            ecosystem_range(vec![
                event(Some("1.0"), None, None),
                event(None, Some("1.5"), None),
            ]),
        ];
        assert_eq!(
            evaluate("PyPI", "1.2", &ranges, &[]),
            Some(Hit {
                fixed: Some("1.5".to_owned())
            })
        );
    }

    #[test]
    fn evaluate_malformed_event_skips_the_range() {
        let ranges = [ecosystem_range(vec![Event {
            introduced: Some("1.0".to_owned()),
            fixed: Some("2.0".to_owned()),
            last_affected: None,
            limit: None,
        }])];
        assert_eq!(evaluate("PyPI", "1.5", &ranges, &[]), None);
    }

    #[test]
    fn evaluate_does_not_panic_on_long_input() {
        let long = "9".repeat(1024);
        let ranges = [ecosystem_range(vec![
            event(Some("0"), None, None),
            event(None, Some("2.0"), None),
        ])];
        assert_eq!(evaluate("Debian", long.as_str(), &ranges, &[]), None);
        assert_eq!(evaluate("PyPI", long.as_str(), &ranges, &[]), None);
    }
}
