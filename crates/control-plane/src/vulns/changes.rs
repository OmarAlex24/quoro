// SPDX-License-Identifier: AGPL-3.0-only
//! Reading an ecosystem's `modified_id.csv` change list.
//!
//! The file is one `<modified>,<id>` line per record, newest first, and it is
//! what a daily refresh reads instead of the whole archive. Times carry mixed
//! fractional precision, so they are compared as unix nanoseconds, never as
//! strings.
//!
//! A delta starts at the bucket's newest held `modified`. Several records can
//! share one timestamp, so a line *at* the cursor is fetched again too; the
//! first line older than it ends the delta.

use crate::vulns::osv;

/// The ids a delta must fetch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delta {
    /// Record ids at or newer than the cursor, newest first.
    pub ids: Vec<String>,
    /// Lines that were not a time and an id, or held a bad id.
    pub malformed: u64,
    /// The list held more ids than the caller allows; it should fetch the
    /// whole archive instead.
    pub too_many: bool,
}

/// Reads the lines of `csv` from the top down to `cursor`.
///
/// Stops at the first line older than `cursor`, since the file is sorted
/// newest first. A malformed line is skipped and counted.
#[must_use]
pub fn scan(csv: &str, cursor: i128, limit: usize) -> Delta {
    let mut delta = Delta::default();
    for line in csv.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (modified, id) = match line.split_once(',') {
            Some((modified, id)) => (modified.trim(), id.trim()),
            None => {
                delta.malformed += 1;
                continue;
            }
        };
        let Some(nanos) = osv::modified_nanos(modified) else {
            delta.malformed += 1;
            continue;
        };
        if nanos < cursor {
            break;
        }
        if !crate::vulns::ecosystem::valid_record_id(id) {
            delta.malformed += 1;
            continue;
        }
        delta.ids.push(id.to_string());
        if delta.ids.len() > limit {
            delta.too_many = true;
            return delta;
        }
    }
    delta
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nanos(value: &str) -> i128 {
        osv::modified_nanos(value).unwrap()
    }

    const CSV: &str = "\
2026-11-04T06:45:04.5Z,CVE-2026-0002
2026-11-04T06:45:04.325296987Z,GHSA-v653-r55g-hcmg
2026-11-04T06:45:04Z,DSA-1
2026-11-03T01:00:00Z,CVE-2026-0001
2020-10-02T01:29:11Z,RUSTSEC-2020-0020
";

    #[test]
    fn a_cursor_of_mixed_precision_fetches_the_line_at_it_and_newer() {
        // The cursor is the newest held `modified`, `…:04Z`. The `.5Z` line of
        // the same second is newer than it, and the line exactly at it is
        // fetched again.
        let delta = scan(CSV, nanos("2026-11-04T06:45:04Z"), 5_000);
        assert_eq!(
            delta.ids,
            vec!["CVE-2026-0002", "GHSA-v653-r55g-hcmg", "DSA-1"]
        );
        assert!(!delta.too_many);
    }

    #[test]
    fn a_cursor_with_a_fraction_stops_before_older_lines() {
        let delta = scan(CSV, nanos("2026-11-04T06:45:04.5Z"), 5_000);
        assert_eq!(delta.ids, vec!["CVE-2026-0002"]);
    }

    #[test]
    fn the_limit_asks_for_the_whole_archive() {
        let delta = scan(CSV, nanos("2026-11-04T06:45:04Z"), 1);
        assert!(delta.too_many);
        assert_eq!(delta.ids.len(), 2);
    }

    #[test]
    fn malformed_lines_are_skipped_and_counted() {
        let csv = "\
not a line
2026-11-04T06:45:04Z,
2026-11-04T06:45:04Z,../x
2026-11-04T06:45:04Z,GHSA-ok
";
        let delta = scan(csv, 0, 5_000);
        assert_eq!(delta.ids, vec!["GHSA-ok"]);
        assert_eq!(delta.malformed, 3);
    }
}
