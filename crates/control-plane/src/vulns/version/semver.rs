// SPDX-License-Identifier: AGPL-3.0-only
//! The SemVer-like order of the package ecosystems.
//!
//! npm, crates.io, Go, NuGet, Hex, Pub, Packagist, RubyGems and Maven all
//! order their releases the way SemVer 2.0 does: one leading `v` is dropped
//! (Go writes `v1.2.3`), build metadata after `+` is ignored, a prerelease
//! after `-` sorts before the release, and the core takes one to four numeric
//! components with missing ones counting as zero.
//!
//! A string that is not SemVer does not fail: it falls back to a generic
//! order that compares runs of digits as numbers and the rest as text, so a
//! vendor's odd version (`1.0.0.git20240101`) still sorts stably.

use std::cmp::Ordering;

/// One parsed SemVer-like version.
struct Version<'a> {
    /// The core components, padded to four.
    core: [u64; 4],
    /// The dot-separated prerelease identifiers.
    pre: Option<Vec<&'a str>>,
}

/// Parses a SemVer-like version, or None when the core is not SemVer.
fn parse(text: &str) -> Option<Version<'_>> {
    // Go writes a leading `v`; exactly one is dropped.
    let text = match text.as_bytes().first() {
        Some(b'v' | b'V') => text.get(1..)?,
        _ => text,
    };
    // Build metadata never takes part in the order.
    let text = match text.split_once('+') {
        Some((before, _)) => before,
        None => text,
    };
    let (core, pre) = match text.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (text, None),
    };
    let mut numbers = [0_u64; 4];
    let mut len = 0;
    for part in core.split('.') {
        if len == numbers.len() || part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let number = part.parse::<u64>().ok()?;
        numbers[len] = number;
        len += 1;
    }
    if len == 0 {
        return None;
    }
    Some(Version {
        core: numbers,
        pre: pre.map(|pre| pre.split('.').collect()),
    })
}

/// Compares two prerelease identifiers the way SemVer 2.0 section 11 does.
fn cmp_identifier(left: &str, right: &str) -> Ordering {
    let left_numeric = !left.is_empty() && left.bytes().all(|b| b.is_ascii_digit());
    let right_numeric = !right.is_empty() && right.bytes().all(|b| b.is_ascii_digit());
    match (left_numeric, right_numeric) {
        // A numeric identifier always sorts before an alphanumeric one.
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => super::cmp_digits(left.as_bytes(), right.as_bytes()),
        (false, false) => left.as_bytes().cmp(right.as_bytes()),
    }
}

/// Compares two parsed versions.
fn cmp_parsed(left: &Version<'_>, right: &Version<'_>) -> Ordering {
    match left.core.cmp(&right.core) {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    match (&left.pre, &right.pre) {
        (None, None) => Ordering::Equal,
        // A prerelease sorts before the release it leads to.
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => {
            let count = left.len().max(right.len());
            for index in 0..count {
                let ordering = match (left.get(index), right.get(index)) {
                    (None, None) => return Ordering::Equal,
                    // A shorter list sorts first when all its identifiers match.
                    (None, Some(_)) => return Ordering::Less,
                    (Some(_), None) => return Ordering::Greater,
                    (Some(left), Some(right)) => cmp_identifier(left, right),
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        }
    }
}

/// One run of the generic order: digits or everything else.
enum Run<'a> {
    Numeric(&'a [u8]),
    Text(&'a [u8]),
}

/// Splits a string into digit runs and text runs.
fn runs(text: &str) -> Vec<Run<'_>> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let numeric = bytes.get(at).is_some_and(u8::is_ascii_digit);
        let mut end = at;
        while end < bytes.len() && bytes.get(end).is_some_and(u8::is_ascii_digit) == numeric {
            end += 1;
        }
        let run = bytes.get(at..end).unwrap_or(&[]);
        out.push(if numeric {
            Run::Numeric(run)
        } else {
            Run::Text(run)
        });
        at = end;
    }
    out
}

/// The generic fallback: digit runs as numbers, the rest as text.
fn generic(left: &str, right: &str) -> Ordering {
    let left = runs(left);
    let right = runs(right);
    let count = left.len().max(right.len());
    for index in 0..count {
        let empty = Run::Text(&[]);
        let a = left.get(index).unwrap_or(&empty);
        let b = right.get(index).unwrap_or(&empty);
        let ordering = match (a, b) {
            (Run::Numeric(a), Run::Numeric(b)) => super::cmp_digits(a, b),
            // A numeric run sorts after a text run, in both directions.
            (Run::Numeric(_), Run::Text(_)) => Ordering::Greater,
            (Run::Text(_), Run::Numeric(_)) => Ordering::Less,
            (Run::Text(a), Run::Text(b)) => a.cmp(b),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Compares two versions, falling back to the generic order.
pub(super) fn compare(left: &str, right: &str) -> Ordering {
    match (parse(left), parse(right)) {
        (Some(left), Some(right)) => cmp_parsed(&left, &right),
        _ => generic(left, right),
    }
}

#[cfg(test)]
mod tests {
    use super::compare;
    use std::cmp::Ordering;

    #[test]
    fn go_pseudo_versions_and_leading_v() {
        let cases = [
            (
                "v0.0.0-20240101000000-abcdef123456",
                "v0.1.0",
                Ordering::Less,
            ),
            ("v1.2.3", "1.2.3", Ordering::Equal),
            ("V1.2.3", "v1.2.3", Ordering::Equal),
            ("v1.2.3", "v1.2.4", Ordering::Less),
            (
                "v0.0.0-20240101000000-abcdef123456",
                "v0.0.0-20240102000000-abcdef123456",
                Ordering::Less,
            ),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), expected, "{a} vs {b}");
        }
    }

    #[test]
    fn prereleases() {
        let cases = [
            ("1.0.0-alpha", "1.0.0-alpha.1", Ordering::Less),
            ("1.0.0-alpha.1", "1.0.0-beta", Ordering::Less),
            ("1.0.0-beta", "1.0.0", Ordering::Less),
            ("1.0.0", "1.0.0+build.5", Ordering::Equal),
            ("1.0.0-rc.1", "1.0.0-rc.1", Ordering::Equal),
            ("1.0.0-1", "1.0.0-alpha", Ordering::Less),
            ("1.0.0-alpha.10", "1.0.0-alpha.9", Ordering::Greater),
            ("1.0.0-alpha", "1.0.0-alpha.beta", Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), expected, "{a} vs {b}");
        }
    }

    #[test]
    fn core_components() {
        let cases = [
            ("1.10.0", "1.9.0", Ordering::Greater),
            ("1.2", "1.2.0", Ordering::Equal),
            ("1", "1.0.0.0", Ordering::Equal),
            ("1.2.3.4", "1.2.3.3", Ordering::Greater),
            ("01.2.3", "1.2.3", Ordering::Equal),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), expected, "{a} vs {b}");
        }
    }

    #[test]
    fn generic_fallback() {
        let cases = [
            ("1.0.0.git20240101", "1.0.0.git20240102", Ordering::Less),
            ("1.0.0.git9", "1.0.0.git10", Ordering::Less),
            ("nightly", "nightly", Ordering::Equal),
            ("nightly-2", "nightly-10", Ordering::Less),
            ("abc", "abd", Ordering::Less),
            ("2.0", "10.0", Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), expected, "{a} vs {b}");
        }
        // The fallback is a total order: comparing any two strings still
        // answers something and never panics.
        let long = "9".repeat(2000);
        assert_eq!(compare(&long, &long), Ordering::Equal);
        assert_eq!(compare(&long, "1.0"), Ordering::Greater);
    }

    #[test]
    fn mixed_semver_and_odd_strings() {
        // A SemVer core on one side and an odd string on the other takes the
        // generic order, so a 30-digit component does not fail the compare.
        let huge = "1.999999999999999999999999999999";
        assert_eq!(compare(huge, "1.2"), Ordering::Greater);
        assert_eq!(compare(huge, huge), Ordering::Equal);
        assert_eq!(compare("1.0.0.0.0", "1.0.0.0.1"), Ordering::Less);
    }
}
