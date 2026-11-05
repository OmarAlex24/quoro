// SPDX-License-Identifier: AGPL-3.0-only
//! Alpine's version order, as apk-tools compares versions.
//!
//! apk reads a version as numeric components separated by `.`, an optional
//! trailing letter (`1.0b`), suffixes such as `_rc1` or `_p2`, and the `-rN`
//! revision. The suffixes `alpha`, `beta`, `pre` and `rc` sort before a
//! version without one, and `cvs`, `svn`, `git`, `hg` and `p` after it.
//! Components are integers, so `3.5.10` is newer than `3.5.4`.

use super::{cmp_digits, within_bounds};
use std::cmp::Ordering;

/// How much a suffix moves a version relative to "no suffix".
fn suffix_rank(name: &str) -> Option<i8> {
    let rank = match name {
        "alpha" => -4,
        "beta" => -3,
        "pre" => -2,
        "rc" => -1,
        "cvs" => 1,
        "svn" => 2,
        "git" => 3,
        "hg" => 4,
        "p" => 5,
        _ => return None,
    };
    Some(rank)
}

/// One parsed apk version.
struct Version<'a> {
    /// The numeric components, leading zeros kept.
    parts: Vec<&'a [u8]>,
    /// The single trailing letter, if any.
    letter: Option<u8>,
    /// One or more suffixes with their optional number.
    suffixes: Vec<(i8, &'a [u8])>,
    /// The `-rN` revision.
    revision: Option<&'a [u8]>,
}

/// Reads one digit run starting at `at`; an empty run is None.
fn digits_at(bytes: &[u8], at: usize) -> Option<usize> {
    let mut end = at;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    (end > at).then_some(end)
}

/// Reads one run of ASCII letters starting at `at`; an empty run is None.
fn letters_at(bytes: &[u8], at: usize) -> Option<usize> {
    let mut end = at;
    while bytes.get(end).is_some_and(u8::is_ascii_alphabetic) {
        end += 1;
    }
    (end > at).then_some(end)
}

/// Parses an apk version, or None when the string is not one.
fn parse(text: &str) -> Option<Version<'_>> {
    if !within_bounds(text) {
        return None;
    }
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let mut at = 0;
    loop {
        let end = digits_at(bytes, at)?;
        parts.push(bytes.get(at..end)?);
        at = end;
        if bytes.get(at) == Some(&b'.') {
            at += 1;
            continue;
        }
        break;
    }
    // An optional trailing letter, as in `0.9.8b`.
    let mut letter = None;
    if let Some(end) = letters_at(bytes, at) {
        letter = bytes.get(at..end).and_then(|run| run.first().copied());
        // apk allows exactly one letter here.
        if end - at != 1 {
            return None;
        }
        at = end;
    }
    // Suffixes, each optionally followed by a number, may repeat.
    let mut suffixes = Vec::new();
    while bytes.get(at) == Some(&b'_') {
        at += 1;
        let end = letters_at(bytes, at)?;
        let name = std::str::from_utf8(bytes.get(at..end)?).ok()?;
        let rank = suffix_rank(name)?;
        at = end;
        let (number, next) = match digits_at(bytes, at) {
            Some(end) => (bytes.get(at..end)?, end),
            None => (b"0".as_slice(), at),
        };
        suffixes.push((rank, number));
        at = next;
    }
    // The `-rN` revision.
    let mut revision = None;
    if bytes.get(at) == Some(&b'-') {
        at += 1;
        if bytes.get(at) != Some(&b'r') {
            return None;
        }
        at += 1;
        let end = digits_at(bytes, at)?;
        revision = Some(bytes.get(at..end)?);
        at = end;
    }
    if at != bytes.len() {
        return None;
    }
    Some(Version {
        parts,
        letter,
        suffixes,
        revision,
    })
}

/// Compares the numeric components, a missing one counting as zero.
fn cmp_parts(a: &[&[u8]], b: &[&[u8]]) -> Ordering {
    let count = a.len().max(b.len());
    for index in 0..count {
        let left = a.get(index).copied().unwrap_or(b"0");
        let right = b.get(index).copied().unwrap_or(b"0");
        match cmp_digits(left, right) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

/// Compares the suffix lists, a missing suffix counting as "no suffix".
fn cmp_suffixes(a: &[(i8, &[u8])], b: &[(i8, &[u8])]) -> Ordering {
    let count = a.len().max(b.len());
    for index in 0..count {
        let (left_rank, left_number) = a.get(index).copied().unwrap_or((0, b"0"));
        let (right_rank, right_number) = b.get(index).copied().unwrap_or((0, b"0"));
        match left_rank.cmp(&right_rank) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        match cmp_digits(left_number, right_number) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

/// Compares two apk versions, or None when either is not one.
pub(super) fn compare(a: &str, b: &str) -> Option<Ordering> {
    let left = parse(a)?;
    let right = parse(b)?;
    let ordering = cmp_parts(&left.parts, &right.parts)
        .then_with(|| match (left.letter, right.letter) {
            (None, None) => Ordering::Equal,
            // A letter sorts after no letter: `1.0 < 1.0a`.
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => left.cmp(&right),
        })
        .then_with(|| cmp_suffixes(&left.suffixes, &right.suffixes))
        .then_with(|| {
            let left = left.revision.unwrap_or(b"0");
            let right = right.revision.unwrap_or(b"0");
            cmp_digits(left, right)
        });
    Some(ordering)
}

#[cfg(test)]
mod tests {
    use super::compare;
    use std::cmp::Ordering;

    #[test]
    fn revision_and_parts() {
        let cases = [
            ("1.2.3-r0", "1.2.3-r1", Ordering::Less),
            ("1.2.3-r1", "1.2.4-r0", Ordering::Less),
            ("3.5.4-r0", "3.5.10-r0", Ordering::Less),
            ("1.2.3-r0", "1.2.3", Ordering::Equal),
            ("1.2", "1.2.0", Ordering::Equal),
            ("01.2.3", "1.2.3", Ordering::Equal),
            ("1.36.1-r29", "1.36.1-r30", Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn suffixes() {
        let cases = [
            ("1.2_rc1-r0", "1.2-r0", Ordering::Less),
            ("1.2-r0", "1.2_p1-r0", Ordering::Less),
            ("1.2_alpha1", "1.2_beta1", Ordering::Less),
            ("1.2_beta1", "1.2_pre1", Ordering::Less),
            ("1.2_pre1", "1.2_rc1", Ordering::Less),
            ("1.2_rc1", "1.2", Ordering::Less),
            ("1.2", "1.2_cvs", Ordering::Less),
            ("1.2_cvs", "1.2_svn", Ordering::Less),
            ("1.2_svn", "1.2_git", Ordering::Less),
            ("1.2_git", "1.2_hg", Ordering::Less),
            ("1.2_hg", "1.2_p", Ordering::Less),
            ("1.2_p1", "1.2_p2", Ordering::Less),
            ("2.40_rc1-r0", "2.40-r0", Ordering::Less),
            ("1.2.3_p4-r1", "1.2.3-r1", Ordering::Greater),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn letters() {
        let cases = [
            ("1.0a", "1.0b", Ordering::Less),
            ("1.0b", "1.1", Ordering::Less),
            ("1.0", "1.0a", Ordering::Less),
            ("0.9.8b-r2", "0.9.8a-r9", Ordering::Greater),
            ("0.9.8b-r2", "0.9.8b-r2", Ordering::Equal),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn unparseable_is_none() {
        let cases = [
            "",
            "v1.2.3",
            "1.2.3 junk",
            "1.2.3-r",
            "1.2.3-rc1",
            "1.2.3_unknown1",
            "abc",
            ".1.2",
            "1..2",
            "1.2.3ab",
        ];
        for text in cases {
            assert_eq!(compare(text, "1.2.3"), None, "{text}");
            assert_eq!(compare("1.2.3", text), None, "{text}");
        }
    }

    #[test]
    fn long_components_do_not_overflow() {
        let big = "9".repeat(80);
        let bigger = format!("1{big}");
        assert_eq!(compare(&big, &bigger), Some(Ordering::Less));
        assert_eq!(compare(&big, &big), Some(Ordering::Equal));
        let absurd = "9".repeat(400);
        assert_eq!(compare(&absurd, "1.0"), None);
    }
}
