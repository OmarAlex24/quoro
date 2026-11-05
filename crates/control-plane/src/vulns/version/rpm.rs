// SPDX-License-Identifier: AGPL-3.0-only
//! RPM's version order, a port of rpm's `rpmvercmp`.
//!
//! An RPM version is `[epoch:]version[-release]`; the epoch is a number that
//! defaults to zero, and the release is everything after the last `-`. The
//! version and the release are compared segment by segment, where a numeric
//! segment is compared as a number and sorts after an alphabetic one. `~`
//! sorts before everything, even the end of the string, and `^` sorts after
//! the end but before any other character.

use super::{cmp_digits, within_bounds};
use std::cmp::Ordering;

/// One split version: epoch, version and the optional release.
struct Split<'a> {
    epoch: &'a [u8],
    version: &'a str,
    release: Option<&'a str>,
}

/// Splits `[epoch:]version[-release]`, the way rpm does.
fn split(text: &str) -> Option<Split<'_>> {
    if !within_bounds(text) {
        return None;
    }
    let (epoch, rest) = match text.split_once(':') {
        // An epoch is a number; anything else keeps the colon in the version.
        Some((epoch, rest))
            if !epoch.is_empty() && epoch.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            (epoch.as_bytes(), rest)
        }
        _ => (b"0".as_slice(), text),
    };
    let (version, release) = match rest.rsplit_once('-') {
        Some((version, release)) if !release.is_empty() => (version, Some(release)),
        // A trailing `-` names an empty release, which is malformed.
        Some(_) => return None,
        None => (rest, None),
    };
    if version.is_empty() {
        return None;
    }
    Some(Split {
        epoch,
        version,
        release,
    })
}

/// Whether a byte is one of rpm's alphanumeric segment characters.
fn is_alnum(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

/// Compares two strings with rpm's rules, byte by byte.
fn rpmvercmp(a: &[u8], b: &[u8]) -> Ordering {
    let mut one = 0;
    let mut two = 0;
    loop {
        while one < a.len() && !is_alnum(a[one]) && a[one] != b'~' && a[one] != b'^' {
            one += 1;
        }
        while two < b.len() && !is_alnum(b[two]) && b[two] != b'~' && b[two] != b'^' {
            two += 1;
        }
        let left = a.get(one).copied();
        let right = b.get(two).copied();
        // `~` sorts before everything, even the end of the string.
        if left == Some(b'~') || right == Some(b'~') {
            return match (left, right) {
                (Some(b'~'), Some(b'~')) => {
                    one += 1;
                    two += 1;
                    continue;
                }
                (Some(b'~'), _) => Ordering::Less,
                _ => Ordering::Greater,
            };
        }
        // `^` sorts after the end of the string but before any other
        // character, which rpm has already skipped over.
        if left == Some(b'^') || right == Some(b'^') {
            return match (left, right) {
                (None, _) => Ordering::Less,
                (_, None) => Ordering::Greater,
                (Some(b'^'), Some(b'^')) => {
                    one += 1;
                    two += 1;
                    continue;
                }
                (Some(b'^'), _) => Ordering::Less,
                _ => Ordering::Greater,
            };
        }
        let (Some(left), Some(_)) = (left, right) else {
            break;
        };
        // One segment is all digits or all letters; a numeric segment sorts
        // after an alphabetic one.
        let numeric = left.is_ascii_digit();
        let start_a = one;
        let start_b = two;
        if numeric {
            while one < a.len() && a[one].is_ascii_digit() {
                one += 1;
            }
            while two < b.len() && b[two].is_ascii_digit() {
                two += 1;
            }
        } else {
            while one < a.len() && a[one].is_ascii_alphabetic() {
                one += 1;
            }
            while two < b.len() && b[two].is_ascii_alphabetic() {
                two += 1;
            }
        }
        let left = a.get(start_a..one).unwrap_or(&[]);
        let right = b.get(start_b..two).unwrap_or(&[]);
        if right.is_empty() {
            return if numeric {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        let ordering = if numeric {
            cmp_digits(left, right)
        } else {
            left.cmp(right)
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    match (one >= a.len(), two >= b.len()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        // One of the two still has a separator before the end, and the other
        // does not: the one with characters left wins, as rpm decides it.
        (false, false) => Ordering::Equal,
    }
}

/// Compares two RPM versions, or None when either is not one.
pub(super) fn compare(a: &str, b: &str) -> Option<Ordering> {
    let left = split(a)?;
    let right = split(b)?;
    let ordering = cmp_digits(left.epoch, right.epoch)
        .then_with(|| rpmvercmp(left.version.as_bytes(), right.version.as_bytes()))
        .then_with(|| match (left.release, right.release) {
            // rpm compares the release only when both name one; a version
            // without a release therefore matches any release.
            (Some(left), Some(right)) => rpmvercmp(left.as_bytes(), right.as_bytes()),
            _ => Ordering::Equal,
        });
    Some(ordering)
}

#[cfg(test)]
mod tests {
    use super::compare;
    use std::cmp::Ordering;

    #[test]
    fn epoch_version_release() {
        let cases = [
            ("1.0-1", "1.0-2", Ordering::Less),
            ("1:0.9-1", "2.0-1", Ordering::Greater),
            ("0:1.13.1-3.el9_3.3", "1.13.1-3.el9_3.3", Ordering::Equal),
            ("1.0~rc1-1", "1.0-1", Ordering::Less),
            ("1.0^git1-1", "1.0-1", Ordering::Greater),
            ("1.1.1k-12.el8_9", "1.1.1k-9.el8", Ordering::Greater),
            ("2.28-251.el9_2", "2.28-251.el9_10", Ordering::Less),
            ("2.28-251.el9_10", "2.28-251.el9_10", Ordering::Equal),
            ("1:1.1.1k-12.el8_9", "1:1.1.1k-12.el8_9", Ordering::Equal),
            ("1.0", "1.0-5", Ordering::Equal),
            ("1.0-5", "1.0", Ordering::Equal),
            ("1.0", "1.1", Ordering::Less),
            ("0:1.0-1", "1.0-1", Ordering::Equal),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn numeric_segments_sort_after_letters() {
        let cases = [
            ("1.0a", "1.0.1", Ordering::Less),
            ("1.0a", "1.0b", Ordering::Less),
            ("1.0", "1.0a", Ordering::Less),
            ("1.01", "1.1", Ordering::Equal),
            ("1.0~rc1", "1.0~rc2", Ordering::Less),
            ("1.0~rc1~git1", "1.0~rc1", Ordering::Less),
            ("1.0^git1", "1.0^git2", Ordering::Less),
            ("1.0^git1", "1.0.1", Ordering::Less),
            ("1.0^git1", "1.0a", Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn unparseable_is_none() {
        for text in ["", "-1", "1.0-"] {
            assert_eq!(compare(text, "1.0"), None, "{text}");
            assert_eq!(compare("1.0", text), None, "{text}");
        }
        let absurd = "1".repeat(400);
        assert_eq!(compare(&absurd, "1.0"), None);
    }

    #[test]
    fn long_numbers_do_not_overflow() {
        let big = "9".repeat(90);
        assert_eq!(
            compare(&format!("1:{big}-1"), &format!("1:{big}0-1")),
            Some(Ordering::Less)
        );
        assert_eq!(compare(&big, &big), Some(Ordering::Equal));
    }
}
