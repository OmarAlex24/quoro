// SPDX-License-Identifier: AGPL-3.0-only
//! CVSS severity: the base score a record's vector carries and its band.
//!
//! An OSV record lists severities as vectors (`CVSS_V3`, `CVSS_V4`, older
//! `CVSS_V2`). [`cvss`] turns the best of them into a score rounded to one
//! decimal, and [`band`] maps a score to the four words the contract uses.
//! A vector comes from downloaded data, so a malformed one yields `None`
//! instead of an error.

use crate::vulns::osv::Severity;
use cvss::v3::MetricType as V3Metric;
use cvss::v4::MetricType as V4Metric;
use std::str::FromStr as _;

/// The CVSS base score and vector of a record's `severity` list: the first
/// CVSS_V3 entry that parses, else the first CVSS_V4, else none (CVSS_V2 is
/// ignored). The score is rounded to one decimal.
#[must_use]
pub fn cvss(severity: &[Severity]) -> Option<(f64, String)> {
    severity
        .iter()
        .filter(|entry| entry.kind == "CVSS_V3")
        .find_map(v3)
        .or_else(|| {
            severity
                .iter()
                .filter(|entry| entry.kind == "CVSS_V4")
                .find_map(v4)
        })
}

/// Scores one CVSS v3 vector.
fn v3(entry: &Severity) -> Option<(f64, String)> {
    let vector = cvss::v3::Vector::from_str(&entry.score).ok()?;
    // The crate scores a vector with missing base metrics as 0.0; a record
    // that does not state all of them states no usable score.
    let named = vector.metrics().map(|(kind, _)| kind).collect::<Vec<_>>();
    let complete = [
        V3Metric::AV,
        V3Metric::AC,
        V3Metric::PR,
        V3Metric::UI,
        V3Metric::S,
        V3Metric::C,
        V3Metric::I,
        V3Metric::A,
    ]
    .into_iter()
    .all(|metric| named.contains(&metric));
    if !complete {
        return None;
    }
    Some((round(vector.score().value()), entry.score.clone()))
}

/// Scores one CVSS v4 vector.
fn v4(entry: &Severity) -> Option<(f64, String)> {
    let vector = cvss::v4::Vector::from_str(&entry.score).ok()?;
    // `score()` unwraps the base metrics, which the parser does not require:
    // an incomplete vector must be refused here or it would panic inside the
    // crate (a panic on untrusted data).
    let named = vector.metrics().map(|(kind, _)| kind).collect::<Vec<_>>();
    let complete = [
        V4Metric::AV,
        V4Metric::AC,
        V4Metric::AT,
        V4Metric::PR,
        V4Metric::UI,
        V4Metric::VC,
        V4Metric::VI,
        V4Metric::VA,
        V4Metric::SC,
        V4Metric::SI,
        V4Metric::SA,
    ]
    .into_iter()
    .all(|metric| named.contains(&metric));
    if !complete {
        return None;
    }
    Some((round(vector.score().value()), entry.score.clone()))
}

/// Rounds a score to the one decimal CVSS publishes.
fn round(score: f64) -> f64 {
    (score * 10.0).round() / 10.0
}

/// The band the contract derives from a score: `critical` >= 9.0,
/// `high` >= 7.0, `medium` >= 4.0, `low` > 0.0, `unknown` otherwise
/// (including None and 0.0).
#[must_use]
pub fn band(score: Option<f64>) -> &'static str {
    match score {
        Some(score) if score >= 9.0 => "critical",
        Some(score) if score >= 7.0 => "high",
        Some(score) if score >= 4.0 => "medium",
        Some(score) if score > 0.0 => "low",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{band, cvss};
    use crate::vulns::osv::Severity;

    fn severity(kind: &str, score: &str) -> Severity {
        Severity {
            kind: kind.to_owned(),
            score: score.to_owned(),
        }
    }

    #[test]
    fn v3_scores() {
        let cases = [
            (
                "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
                9.8,
                "critical",
            ),
            ("CVSS:3.1/AV:L/AC:L/PR:L/UI:R/S:U/C:H/I:H/A:H", 7.3, "high"),
            ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N", 7.5, "high"),
        ];
        for (vector, score, expected_band) in cases {
            let (got, got_vector) = cvss(&[severity("CVSS_V3", vector)]).expect("scores");
            assert_eq!(got, score, "{vector}");
            assert_eq!(got_vector, vector);
            assert_eq!(band(Some(got)), expected_band, "{vector}");
        }
    }

    #[test]
    fn v4_scores() {
        let vector = "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N";
        let (score, got_vector) = cvss(&[severity("CVSS_V4", vector)]).expect("scores");
        assert_eq!(score, 9.3);
        assert_eq!(got_vector, vector);
        assert_eq!(band(Some(score)), "critical");
    }

    #[test]
    fn v3_is_preferred_over_v4() {
        let entries = [
            severity(
                "CVSS_V4",
                "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N",
            ),
            severity("CVSS_V3", "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"),
        ];
        let (score, vector) = cvss(&entries).expect("scores");
        assert_eq!(score, 9.8);
        assert_eq!(vector, "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H");
    }

    #[test]
    fn first_parsing_v3_wins() {
        let severity = [
            severity("CVSS_V3", "garbage"),
            severity("CVSS_V3", "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"),
            severity("CVSS_V3", "CVSS:3.1/AV:L/AC:L/PR:L/UI:R/S:U/C:H/I:H/A:H"),
        ];
        let (score, _) = cvss(&severity).expect("scores");
        assert_eq!(score, 9.8);
    }

    #[test]
    fn v2_is_ignored() {
        let entries = [
            severity("CVSS_V2", "AV:N/AC:L/Au:N/C:C/I:C/A:C"),
            severity("CVSS_V3", "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"),
        ];
        let (score, _) = cvss(&entries).expect("scores");
        assert_eq!(score, 9.8);
        let only_v2 = [severity("CVSS_V2", "AV:N/AC:L/Au:N/C:C/I:C/A:C")];
        assert_eq!(cvss(&only_v2), None);
    }

    #[test]
    fn garbage_is_none() {
        let cases = [
            "",
            "not a vector",
            "CVSS:3.1",
            "CVSS:9.9/AV:N",
            "CVSS:3.1/AV:Q/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            "CVSS:4.0/AV:Q",
            "CVSS:3.1/AV:N/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H",
        ];
        for vector in cases {
            assert_eq!(cvss(&[severity("CVSS_V3", vector)]), None, "v3 {vector}");
            assert_eq!(cvss(&[severity("CVSS_V4", vector)]), None, "v4 {vector}");
        }
        let long = format!("CVSS:3.1/AV:N/AC:L/{}", "X:Y/".repeat(5000));
        assert_eq!(cvss(&[severity("CVSS_V3", &long)]), None);
        let long_v4 = format!(
            "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N/{}",
            "X:Y/".repeat(5000)
        );
        assert_eq!(cvss(&[severity("CVSS_V4", &long_v4)]), None);
    }

    #[test]
    fn incomplete_v4_does_not_panic() {
        // The crate's parser accepts this and its score() unwraps the missing
        // metrics; refusing it here keeps the function panic-free.
        for vector in [
            "CVSS:4.0/AV:N",
            "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N",
            "CVSS:4.0",
        ] {
            assert_eq!(cvss(&[severity("CVSS_V4", vector)]), None, "{vector}");
        }
    }

    #[test]
    fn band_edges() {
        let cases = [
            (Some(9.0), "critical"),
            (Some(8.99), "high"),
            (Some(10.0), "critical"),
            (Some(7.0), "high"),
            (Some(6.99), "medium"),
            (Some(4.0), "medium"),
            (Some(3.99), "low"),
            (Some(0.1), "low"),
            (Some(0.0), "unknown"),
            (None, "unknown"),
        ];
        for (score, expected) in cases {
            assert_eq!(band(score), expected, "{score:?}");
        }
    }
}
