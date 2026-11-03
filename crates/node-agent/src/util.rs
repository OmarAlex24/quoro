// SPDX-License-Identifier: AGPL-3.0-only
//! Small helpers shared by the node agent: random bytes, hexadecimal and
//! RFC 3339 timestamps.
//!
//! The crate deliberately has no `chrono`, `time` or `hex` dependency: the
//! three functions here are the whole need, and a date-library dependency
//! would have to be justified for two format calls.

use std::time::{SystemTime, UNIX_EPOCH};

/// Fills `N` bytes from the OS generator.
///
/// # Panics
///
/// Panics when the operating system cannot provide randomness, which on a
/// running Unix host means the kernel entropy source is broken; there is no
/// safe way to continue without unique ids and keys.
#[must_use]
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    if getrandom::fill(&mut bytes).is_err() {
        panic!("the operating system did not provide random bytes");
    }
    bytes
}

/// A fresh `msg_` id (section 3.3).
#[must_use]
pub fn message_id() -> String {
    quoro_proto::wire::msg_id(random_bytes::<16>())
}

/// Lowercase hexadecimal, the form `key_id`, `log_hash` and fingerprints use.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// The current wall-clock time as an RFC 3339 UTC string with second
/// precision.
#[must_use]
pub fn now_rfc3339() -> String {
    rfc3339(SystemTime::now())
}

/// An RFC 3339 UTC string for `time`.
#[must_use]
pub fn rfc3339(time: SystemTime) -> String {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
    let days = seconds.div_euclid(86_400);
    let rest = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        (rest % 3_600) / 60,
        rest % 60
    )
}

/// The civil date for a count of days since 1970-01-01, by the algorithm in
/// Howard Hinnant's `chrono`-compatible `days_from_civil` note.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = u32::try_from(day_of_year - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (
        if month <= 2 { year + 1 } else { year },
        u32::try_from(month).unwrap_or(1),
        day,
    )
}

/// A random fraction in `[0, 1)`, for full-jitter backoff (section 4.4).
#[must_use]
pub fn random_fraction() -> f64 {
    let value = getrandom::u64().unwrap_or(0);
    (value % 1_000_000) as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_rfc3339_utc() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + std::time::Duration::from_secs(1_767_225_600)),
            "2026-01-01T00:00:00Z"
        );
    }

    #[test]
    fn hex_is_lowercase_and_padded() {
        assert_eq!(hex(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }

    #[test]
    fn ids_have_their_prefixes() {
        assert!(message_id().starts_with("msg_"));
    }
}
