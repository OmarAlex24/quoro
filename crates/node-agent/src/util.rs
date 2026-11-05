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

/// `text` on one line, at most `max_chars` characters: the form a status
/// report's `error` takes (section 5.3).
#[must_use]
pub fn one_line(text: &str, max_chars: usize) -> String {
    let one_line: String = text
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    one_line.chars().take(max_chars).collect()
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

/// The current wall-clock time as whole seconds since the Unix epoch.
#[must_use]
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// The current wall-clock time as nanoseconds since the Unix epoch.
#[must_use]
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Nanoseconds since the Unix epoch as an RFC 3339 UTC string with exactly
/// nine fractional digits, for example `2026-11-02T10:16:30.123456789Z`.
///
/// The control plane turns this string back into an integer and uses it as
/// the SSE event id and the `since` cursor, so the width is fixed.
#[must_use]
pub fn rfc3339_nanos(time_ns: i64) -> String {
    let seconds = time_ns.div_euclid(1_000_000_000);
    let nanos = time_ns.rem_euclid(1_000_000_000);
    let days = seconds.div_euclid(86_400);
    let rest = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        rest / 3_600,
        (rest % 3_600) / 60,
        rest % 60
    )
}

/// Parses an RFC 3339 UTC timestamp into nanoseconds since the Unix epoch.
///
/// Fractional seconds of one to nine digits are accepted; `Z` and numeric
/// offsets are accepted. Returns `None` for anything that is not a complete
/// timestamp, so a malformed cursor is a `bad_message` rather than silently
/// treated as zero.
#[must_use]
pub fn parse_rfc3339_nanos(text: &str) -> Option<i64> {
    let (date, rest) = text.split_once('T')?;
    let (time, offset) = match rest.strip_suffix('Z') {
        Some(time) => (time, 0i64),
        None => {
            let (time, offset) = rest.split_once(['+', '-'])?;
            let sign = if rest[time.len()..].starts_with('-') {
                -1
            } else {
                1
            };
            let (hours, minutes) = offset.split_once(':')?;
            let hours: i64 = hours.parse().ok()?;
            let minutes: i64 = minutes.parse().ok()?;
            (time, sign * (hours * 3_600 + minutes * 60))
        }
    };
    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time, None),
    };
    let mut parts = clock.split(':');
    let hour: i64 = parts.next()?.parse().ok()?;
    let minute: i64 = parts.next()?.parse().ok()?;
    let second: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let nanos = match fraction {
        None => 0,
        Some(digits) => {
            if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let value: i64 = digits.parse().ok()?;
            value * 10i64.pow(u32::try_from(9 - digits.len()).ok()?)
        }
    };
    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?
        .checked_sub(offset)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

/// Days since 1970-01-01 for a civil date, the inverse of
/// [`civil_from_days`].
fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

/// A fresh `evt_` id, built exactly like [`message_id`] but with the event
/// prefix (section 5.4).
#[must_use]
pub fn event_id() -> String {
    id_with_prefix("evt_", random_bytes::<16>())
}

/// A fresh `msg_` id with an arbitrary prefix, in the same 26-character
/// Crockford base32 form as [`quoro_proto::wire::msg_id`].
fn id_with_prefix(prefix: &str, random: [u8; 16]) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let value = u128::from_be_bytes(random);
    let mut out = String::with_capacity(prefix.len() + 26);
    out.push_str(prefix);
    for i in (0..26).rev() {
        let index = ((value >> (i * 5)) & 0x1f) as usize;
        out.push(char::from(ALPHABET[index]));
    }
    out
}

/// A random fraction in `[0, 1)`, for full-jitter backoff (section 4.4).
#[must_use]
pub fn random_fraction() -> f64 {
    let value = getrandom::u64().unwrap_or(0);
    (value % 1_000_000) as f64 / 1_000_000.0
}

/// The space an unprivileged write can still use on the filesystem that holds
/// `path`, in GiB (`node-agent-protocol.md` section 5.3, `disk_free_gb`). An
/// error is logged and reported as 0, so the primary refuses a volume rather
/// than over-commit the disk.
#[must_use]
pub fn disk_free_gb(path: &std::path::Path) -> f64 {
    match rustix::fs::statvfs(path) {
        Ok(stat) => {
            let bytes = u128::from(stat.f_bavail) * u128::from(stat.f_frsize);
            // Precision loss is irrelevant at GiB scale.
            #[allow(clippy::cast_precision_loss)]
            let gib = bytes as f64 / f64::from(1u32 << 30);
            gib
        }
        Err(error) => {
            tracing::warn!(path = %path.display(), error = %error, "cannot read the free disk space");
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_free_reads_the_filesystem_and_reports_zero_on_error() {
        let free = disk_free_gb(&std::env::temp_dir());
        assert!(free > 0.0, "the temp directory has free space: {free}");
        assert!(
            disk_free_gb(std::path::Path::new("/nonexistent/quoro/disk-free")).abs() < f64::EPSILON,
            "an unreadable path reports 0"
        );
    }

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
        assert!(event_id().starts_with("evt_"));
        assert_eq!(event_id().len(), 30);
    }

    #[test]
    fn nanosecond_timestamps_round_trip_with_nine_digits() {
        let value = 1_761_008_190_123_456_789i64;
        let text = rfc3339_nanos(value);
        assert_eq!(text, "2025-10-21T00:56:30.123456789Z");
        assert_eq!(parse_rfc3339_nanos(&text), Some(value));
        assert_eq!(
            rfc3339_nanos(0),
            "1970-01-01T00:00:00.000000000Z",
            "the fraction is always present"
        );
        assert_eq!(
            parse_rfc3339_nanos("2026-11-02T10:16:30Z"),
            Some(1_793_614_590_000_000_000)
        );
        assert_eq!(
            parse_rfc3339_nanos("2026-11-02T10:16:30.5Z"),
            Some(1_793_614_590_500_000_000)
        );
        assert_eq!(
            parse_rfc3339_nanos("2026-11-02T11:16:30+01:00"),
            Some(1_793_614_590_000_000_000)
        );
    }

    #[test]
    fn malformed_timestamps_are_rejected() {
        for text in [
            "",
            "2026-11-02",
            "2026-11-02T10:16",
            "2026-13-02T10:16:30Z",
            "2026-11-02T10:16:30.Z",
            "2026-11-02T10:16:30.1234567890Z",
        ] {
            assert!(parse_rfc3339_nanos(text).is_none(), "{text:?}");
        }
    }
}
