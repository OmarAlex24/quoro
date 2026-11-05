// SPDX-License-Identifier: AGPL-3.0-only
//! The Linux kernel's version order.
//!
//! Kernel records name upstream releases (`7.2.15`, `6.12.113`, `6.1`) and an
//! inventory names the running kernel with its distribution's suffix
//! (`6.12.113+deb13-amd64`, `7.2.15-quoro`, `6.8.0-48-generic`). Only the
//! leading dotted numeric part is the upstream release; everything from the
//! first character after it is packaging and does not take part in the order.
//! Missing components count as zero, so `6.8` equals `6.8.0`.

use super::within_bounds;
use std::cmp::Ordering;

/// Reads the leading `\d+(\.\d+)*` of a kernel version.
fn parse(text: &str) -> Option<Vec<u64>> {
    if !within_bounds(text) {
        return None;
    }
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let mut at = 0;
    loop {
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        let digits = text.get(start..at)?;
        // A component that does not fit is not a kernel version Quoro can
        // order; refusing is better than guessing.
        parts.push(digits.parse::<u64>().ok()?);
        if bytes.get(at) == Some(&b'.') && bytes.get(at + 1).is_some_and(u8::is_ascii_digit) {
            at += 1;
            continue;
        }
        break;
    }
    Some(parts)
}

/// Compares two kernel versions, or None when either has no leading number.
pub(super) fn compare(a: &str, b: &str) -> Option<Ordering> {
    let left = parse(a)?;
    let right = parse(b)?;
    let count = left.len().max(right.len());
    for index in 0..count {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        match left.cmp(&right) {
            Ordering::Equal => {}
            ordering => return Some(ordering),
        }
    }
    Some(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::compare;
    use std::cmp::Ordering;

    #[test]
    fn suffixes_are_ignored() {
        let cases = [
            ("6.12.113+deb13-amd64", "6.12.113", Ordering::Equal),
            ("7.2.15-quoro", "7.2.15", Ordering::Equal),
            ("6.8.0-48-generic", "6.8", Ordering::Equal),
            ("6.8.0-48-generic", "6.8.0", Ordering::Equal),
            ("6.12.113", "7.2.15", Ordering::Less),
            ("7.2.15", "7.2.10", Ordering::Greater),
            ("6.1", "6.1.0", Ordering::Equal),
            ("6.19.0", "6.12.113", Ordering::Greater),
            ("6.1", "6.1.1", Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(a, b), Some(expected), "{a} vs {b}");
        }
    }

    #[test]
    fn no_leading_number_is_none() {
        for text in ["", "quoro", "-6.1", "v6.1"] {
            assert_eq!(compare(text, "6.1"), None, "{text}");
            assert_eq!(compare("6.1", text), None, "{text}");
        }
        let absurd = "9".repeat(400);
        assert_eq!(compare(&absurd, "6.1"), None);
    }

    #[test]
    fn oversized_component_is_none() {
        let huge = "6.999999999999999999999999";
        assert_eq!(compare(huge, "6.1"), None);
    }
}
