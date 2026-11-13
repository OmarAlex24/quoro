// SPDX-License-Identifier: AGPL-3.0-only
//! RFC 8785 JSON Canonicalization Scheme for the values Quoro hashes.
//!
//! The change log and the audit log hash `SHA-256(JCS(entry))`
//! (`audit-log-format.md` section 5). Object members are sorted by the UTF-16
//! code units of their names, strings use the minimal JSON escaping of RFC
//! 8785 section 3.2.2.2, and there is no whitespace. Numbers follow RFC 8785
//! section 3.2.2.3 (ECMAScript `Number.prototype.toString`) for the values
//! Quoro's records carry: an integer below 2^53 serializes as its shortest
//! decimal form, and a float whose magnitude is 0 or in `[1e-6, 1e21)` as its
//! shortest round-trip decimal without an exponent (a CVSS score `9.8` stays
//! `9.8`, `10.0` becomes `10`, `-0.0` becomes `0`). Any other number has an
//! exponent form in ECMAScript and is refused.
//!
//! When two decimals of that shortest length both read back as the float and
//! it lies exactly halfway between them, the one whose last digit is even is
//! written, as ECMAScript does: the double 18983552425072.5625 is written
//! `18983552425072.562` (`audit-log-format.md` section 4.1).
//!
//! [`canonicalize_lenient`] writes every value, for the text an audit record's
//! parameters digest covers (`audit-log-format.md` section 4.1): the same
//! form, plus the numbers [`canonicalize`] refuses.

use serde_json::Value;
use std::fmt::Write as _;

/// Error for a value that has no canonical form in Quoro's records.
#[derive(Debug, PartialEq, Eq)]
pub struct NotCanonical(pub String);

/// Canonical JSON text of `value`.
///
/// # Errors
///
/// An integer outside the IEEE-754 safe range, or a float that is not finite
/// or whose magnitude is outside `[1e-6, 1e21)` (other than 0), is refused.
pub fn canonicalize(value: &Value) -> Result<String, NotCanonical> {
    let mut out = String::new();
    canonicalize_into(&mut out, value)?;
    Ok(out)
}

/// Appends the canonical JSON text of `value` to `out`: exactly what
/// [`canonicalize`] returns, into a buffer the caller owns, such as one sized
/// up front and wiped after use.
///
/// # Errors
///
/// As [`canonicalize`]; `out` may then hold part of the text.
pub fn canonicalize_into(out: &mut String, value: &Value) -> Result<(), NotCanonical> {
    write_value(out, value, Numbers::Strict)
}

/// The text of `value` that an audit record's `params_sha256` covers
/// (`audit-log-format.md` section 4.1): its RFC 8785 form, written for every
/// value, so it never fails.
///
/// It is [`canonicalize`] wherever that succeeds. The numbers that one
/// refuses are written as well: an integer beyond ±(2^53 − 1) that was read
/// exactly (`serde_json` holds it as an `i64` or a `u64`) as its exact decimal
/// integer, and a float whose magnitude is outside `[1e-6, 1e21)` in
/// ECMAScript's exponent form (`1e+21`, `1.5e-7`), as RFC 8785 section
/// 3.2.2.3 writes it. A `serde_json` value holds no float that is not finite.
#[must_use]
pub fn canonicalize_lenient(value: &Value) -> String {
    let mut out = String::new();
    // Lenient writing refuses no value.
    let _ = write_value(&mut out, value, Numbers::Lenient);
    out
}

/// Which numbers [`write_value`] writes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Numbers {
    /// Only those [`canonicalize`] accepts.
    Strict,
    /// Every number, as [`canonicalize_lenient`] says.
    Lenient,
}

fn write_value(out: &mut String, value: &Value, numbers: Numbers) -> Result<(), NotCanonical> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => {
            const SAFE: i64 = (1 << 53) - 1;
            if let Some(i) = number.as_i64().filter(|i| (-SAFE..=SAFE).contains(i)) {
                let _ = write!(out, "{i}");
            } else if let Some(u) = number.as_u64().filter(|u| *u <= SAFE as u64) {
                let _ = write!(out, "{u}");
            } else if let Some(f) = number
                .as_f64()
                .filter(|f| number.is_f64() && plain_float(*f))
            {
                // ECMAScript writes a float in this range without an exponent.
                write_float(out, f);
            } else if numbers == Numbers::Lenient {
                write_lenient_number(out, number);
            } else {
                return Err(NotCanonical(format!(
                    "number {number} has no canonical form in Quoro's records"
                )));
            }
        }
        Value::String(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item, numbers)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, key);
                out.push(':');
                write_value(out, &map[key.as_str()], numbers)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// A number [`canonicalize`] refuses, as [`canonicalize_lenient`] writes it:
/// an exact integer beyond ±(2^53 − 1) as its decimal integer, and any other
/// (a float outside `[1e-6, 1e21)`) in ECMAScript's exponent form.
fn write_lenient_number(out: &mut String, number: &serde_json::Number) {
    if let Some(i) = number.as_i64().filter(|_| !number.is_f64()) {
        let _ = write!(out, "{i}");
    } else if let Some(u) = number.as_u64().filter(|_| !number.is_f64()) {
        let _ = write!(out, "{u}");
    } else if let Some(f) = number.as_f64() {
        write_float(out, f);
    }
}

/// Whether ECMAScript prints `f` without an exponent: finite, and 0 or with a
/// magnitude in `[1e-6, 1e21)`. A float outside it is refused.
fn plain_float(f: f64) -> bool {
    f.is_finite() && (f == 0.0 || (1e-6..1e21).contains(&f.abs()))
}

/// Writes the finite float `f` as ECMAScript's `Number.prototype.toString`
/// does (RFC 8785 section 3.2.2.3): the digits of [`shortest`], without an
/// exponent when `f` is 0 or its magnitude is in `[1e-6, 1e21)` (`9.8`,
/// `0.000001`, `10000000000000000`), and otherwise as `d[.ddd]` with a signed
/// exponent (`1e+21`, `1.5e-7`). `-0.0` is written `0`.
fn write_float(out: &mut String, f: f64) {
    if f == 0.0 {
        out.push('0');
        return;
    }
    if f < 0.0 {
        out.push('-');
    }
    let Some((digits, point)) = shortest(out, f.abs()) else {
        // Not reached for a finite float: Rust's own form is left in place.
        return;
    };
    // `f` reads back from `0.d₁d₂…dₖ × 10^point`, with `k` digits.
    let start = out.len();
    let k = digits.ilog10() + 1;
    if (1..=21).contains(&point) {
        let _ = write!(out, "{digits}");
        let n = point.unsigned_abs();
        if n >= k {
            out.extend(std::iter::repeat_n('0', (n - k) as usize));
        } else {
            out.insert(start + n as usize, '.');
        }
    } else if (-5..=0).contains(&point) {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', point.unsigned_abs() as usize));
        let _ = write!(out, "{digits}");
    } else {
        let _ = write!(out, "{digits}");
        if k > 1 {
            out.insert(start + 1, '.');
        }
        let exponent = point - 1;
        let sign = if exponent < 0 { '-' } else { '+' };
        let _ = write!(out, "e{sign}{}", exponent.unsigned_abs());
    }
}

/// ECMAScript's digits for the finite `f > 0`: the integer `d` whose `k`
/// digits are the shortest decimal that reads back as `f`, and `point`, with
/// `f` read back from `0.d₁…dₖ × 10^point`. Of two such decimals equally near
/// `f`, `d` is the one whose last digit is even.
///
/// Rust's shortest formatting finds the digits but takes the upper of two
/// equally near ones. So when its last digit is odd and `f` is exactly the
/// midpoint between it and a neighbour at the same scale, the neighbour (even)
/// is taken if it also reads back as `f`. Uses `out` past its end as scratch
/// and leaves it as it was, or with Rust's text when that cannot be read
/// (`None`, never for a finite `f`).
fn shortest(out: &mut String, f: f64) -> Option<(u64, i32)> {
    let start = out.len();
    let _ = write!(out, "{f:e}");
    let (mantissa, exponent) = out[start..].split_once('e')?;
    let exponent: i32 = exponent.parse().ok()?;
    let mut digits = 0u64;
    let mut k = 0i32;
    for c in mantissa.bytes().filter(u8::is_ascii_digit) {
        digits = digits * 10 + u64::from(c - b'0');
        k += 1;
    }
    out.truncate(start);
    // `f` reads back from `digits × 10^scale`.
    let scale = exponent + 1 - k;
    if digits % 2 == 1 {
        for (midpoint, neighbour) in [(10 * digits - 5, digits - 1), (10 * digits + 5, digits + 1)]
        {
            if exactly(f, midpoint, scale - 1) {
                let same_length = neighbour.checked_ilog10() == digits.checked_ilog10();
                let _ = write!(out, "{neighbour}e{scale}");
                let reads_back = out[start..].parse::<f64>() == Ok(f);
                out.truncate(start);
                if same_length && reads_back {
                    digits = neighbour;
                }
                break;
            }
        }
    }
    Some((digits, scale + k))
}

/// Whether the finite `f > 0` equals `midpoint × 10^power` exactly, for an odd
/// `midpoint` (one ending in 5). `f` is `m × 2^e` with `m` odd, and
/// `midpoint × 10^power` is `midpoint × 5^power × 2^power` with
/// `midpoint × 5^power` odd, so they are equal only when `e` is `power` and
/// `m` is `midpoint × 5^power`: compared in `u128`, as `m × 5^-power` against
/// `midpoint` when `power` is negative. A product too large for `u128` cannot
/// equal the other side, which fits.
fn exactly(f: f64, midpoint: u64, power: i32) -> bool {
    let bits = f.to_bits();
    let biased = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let fraction = bits & ((1 << 52) - 1);
    let (m, e) = if biased == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1 << 52), biased - 1075)
    };
    if m == 0 {
        return false;
    }
    let zeros = m.trailing_zeros();
    let (m, e) = (u128::from(m >> zeros), e + zeros.cast_signed());
    if e != power {
        return false;
    }
    let midpoint = u128::from(midpoint);
    let fives = 5u128.checked_pow(power.unsigned_abs());
    if power >= 0 {
        fives.and_then(|p| p.checked_mul(midpoint)) == Some(m)
    } else {
        fives.and_then(|p| p.checked_mul(m)) == Some(midpoint)
    }
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{09}' => out.push_str("\\t"),
            '\u{0A}' => out.push_str("\\n"),
            '\u{0C}' => out.push_str("\\f"),
            '\u{0D}' => out.push_str("\\r"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_members_and_drops_whitespace() {
        let value = json!({ "b": 1, "a": [true, null, "x"], "c": { "z": 0, "y": -3 } });
        assert_eq!(
            canonicalize(&value).unwrap(),
            r#"{"a":[true,null,"x"],"b":1,"c":{"y":-3,"z":0}}"#
        );
    }

    #[test]
    fn sorts_by_utf16_code_units() {
        // RFC 8785 section 3.2.3: U+1F600 is the surrogate pair 0xD83D 0xDE00
        // in UTF-16, so it sorts before U+E000 even though its code point is
        // larger; a code-point sort would get this wrong.
        let value = json!({ "\u{E000}": 2, "\u{1F600}": 1 });
        assert_eq!(
            canonicalize(&value).unwrap(),
            "{\"\u{1F600}\":1,\"\u{E000}\":2}"
        );
    }

    #[test]
    fn escapes_minimally() {
        let value = json!("a\"b\\c\nd\u{1f}e/\u{7f}é");
        assert_eq!(
            canonicalize(&value).unwrap(),
            "\"a\\\"b\\\\c\\nd\\u001fe/\u{7f}é\""
        );
    }

    #[test]
    fn writes_floats_like_ecmascript() {
        for (value, text) in [
            (9.8, "9.8"),
            (10.0, "10"),
            (0.1, "0.1"),
            (-0.0, "0"),
            (0.0, "0"),
            (-2.5, "-2.5"),
            (0.000_001, "0.000001"),
            (123_456.789, "123456.789"),
        ] {
            assert_eq!(canonicalize(&json!(value)).unwrap(), text, "{value}");
        }
        // A parsed `10.0` is a float and still canonicalizes as `10`.
        let parsed: Value = serde_json::from_str("[10.0,9.80]").unwrap();
        assert_eq!(canonicalize(&parsed).unwrap(), "[10,9.8]");
    }

    #[test]
    fn a_float_s_canonical_text_reads_back_as_the_same_double() {
        // `serde_json` parses floats exactly (`float_roundtrip`, ADR 0315):
        // each text reads as the double nearest to it, so the canonical text
        // of a parsed float reads back as that double, and a record hashed
        // over that text re-hashes from it. An approximate parser reads each
        // of these texts back as a neighbouring double.
        for (body, text) in [
            ("1548.2162860866162", "1548.2162860866163"),
            ("18983552425072.563", "18983552425072.562"),
            ("72096104873994100000", "72096104873994100000"),
        ] {
            let parsed: Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                parsed.as_f64(),
                Some(body.parse::<f64>().unwrap()),
                "{body}"
            );
            assert_eq!(canonicalize(&parsed).unwrap(), text, "{body}");
            let read_back: Value = serde_json::from_str(text).unwrap();
            assert_eq!(read_back, parsed, "{body}");
            assert_eq!(canonicalize(&read_back).unwrap(), text, "{body}");
        }
    }

    #[test]
    fn refuses_numbers_with_an_exponent_form() {
        assert!(canonicalize(&json!(1e21)).is_err());
        assert!(canonicalize(&json!(1e-7)).is_err());
        assert!(canonicalize(&json!(u64::MAX)).is_err());
    }

    fn parse(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn the_lenient_form_writes_the_three_cases_of_section_4_1() {
        // `audit-log-format.md` section 4.1, as the primary reads each
        // request's parameters.
        let whole_float = parse(r#"{"n": 1e16}"#);
        assert!(whole_float["n"].is_f64());
        assert_eq!(
            canonicalize_lenient(&whole_float),
            r#"{"n":10000000000000000}"#
        );
        assert_eq!(canonicalize_lenient(&whole_float).len(), 23);

        let exact = parse(r#"{"n": 18446744073709551615}"#);
        assert!(canonicalize(&exact).is_err());
        assert_eq!(
            canonicalize_lenient(&exact),
            r#"{"n":18446744073709551615}"#
        );
        assert_eq!(canonicalize_lenient(&exact).len(), 26);

        // The worked float case, kept in `params`: the nearest double is
        // exactly 18983552425072.5625, written with the even last digit.
        let float = parse(r#"{"x": 18983552425072.563}"#);
        assert_eq!(canonicalize_lenient(&float), canonicalize(&float).unwrap());
        assert_eq!(canonicalize_lenient(&float), r#"{"x":18983552425072.562}"#);
        assert_eq!(canonicalize_lenient(&float).len(), 24);
        assert_eq!(parse(&canonicalize(&float).unwrap()), float);
    }

    /// The tie the contract's worked case holds (`audit-log-format.md`
    /// section 4.1).
    const TIE: &str = "18983552425072.5625";

    #[test]
    fn the_worked_tie_is_written_with_its_even_last_digit() {
        let tie: f64 = TIE.parse().unwrap();
        // Both 17-digit decimals read back as the double, which lies exactly
        // halfway between them.
        assert_eq!("18983552425072.562".parse::<f64>(), Ok(tie));
        assert_eq!("18983552425072.563".parse::<f64>(), Ok(tie));

        let params = json!({ "x": tie });
        let text = canonicalize(&params).unwrap();
        assert_eq!(text, r#"{"x":18983552425072.562}"#);
        assert_eq!(text.len(), 24);
        assert_eq!(canonicalize_lenient(&params), text);
        let read_back: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(read_back["x"].as_f64(), Some(tie));
        assert_eq!(canonicalize(&read_back).unwrap(), text);

        // Its neighbouring doubles are no tie, and keep their shortest form.
        let below = f64::from_bits(tie.to_bits() - 1);
        let above = f64::from_bits(tie.to_bits() + 1);
        assert_eq!(canonicalize(&json!(below)).unwrap(), "18983552425072.56");
        assert_eq!(canonicalize(&json!(above)).unwrap(), "18983552425072.566");
    }

    #[test]
    fn an_exact_tie_is_written_with_its_even_last_digit() {
        // (the double's exact value, its text, the other decimal of the same
        // length that also reads back as it). Each double lies exactly
        // halfway between the two; ECMAScript's `String` writes the first.
        for (exact, written, other) in [
            (
                "0.00000107288360595703125",
                "0.0000010728836059570312",
                "0.0000010728836059570313",
            ),
            (
                "0.123477935791015625",
                "0.12347793579101562",
                "0.12347793579101563",
            ),
            (
                "1.00000762939453125",
                "1.0000076293945312",
                "1.0000076293945313",
            ),
            (
                "16777215.6103515625",
                "16777215.610351562",
                "16777215.610351563",
            ),
            (TIE, "18983552425072.562", "18983552425072.563"),
            (
                "1125899906842524.25",
                "1125899906842524.2",
                "1125899906842524.3",
            ),
            (
                "-18983552425072.5625",
                "-18983552425072.562",
                "-18983552425072.563",
            ),
            // Below 1e-6, in the exponent form only the lenient form writes.
            (
                "2.98023223876953125e-8",
                "2.9802322387695312e-8",
                "2.9802322387695313e-8",
            ),
            (
                "5.9604644775390625e-7",
                "5.960464477539062e-7",
                "5.960464477539063e-7",
            ),
            (
                "-5.9604644775390625e-7",
                "-5.960464477539062e-7",
                "-5.960464477539063e-7",
            ),
            // Ties whose upper decimal is the even one.
            (
                "1.00002288818359375",
                "1.0000228881835938",
                "1.0000228881835937",
            ),
            (
                "1.78813934326171875e-7",
                "1.7881393432617188e-7",
                "1.7881393432617187e-7",
            ),
        ] {
            let value: f64 = exact.parse().unwrap();
            assert_eq!(written.parse::<f64>(), Ok(value), "{exact}");
            assert_eq!(other.parse::<f64>(), Ok(value), "{exact}");
            assert_eq!(canonicalize_lenient(&json!(value)), written, "{exact}");
            if plain_float(value) {
                assert_eq!(canonicalize(&json!(value)).unwrap(), written, "{exact}");
            } else {
                assert!(canonicalize(&json!(value)).is_err(), "{exact}");
            }
        }
    }

    /// The significant digits of a number's text: no sign, point, exponent,
    /// or leading or trailing zero.
    fn significant(text: &str) -> String {
        let mantissa = text.split(['e', 'E']).next().unwrap();
        let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
        digits.trim_matches('0').to_owned()
    }

    #[test]
    fn every_float_is_written_with_the_shortest_digits_that_read_back() {
        // A fixed spread of doubles: any bit pattern, and few fractional bits
        // at every magnitude, where ties are.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut values = Vec::new();
        for _ in 0..20_000 {
            values.push(f64::from_bits(next()));
            let bits = next();
            let whole = bits >> (11 + bits % 53);
            values.push(whole as f64 / f64::from(1u32 << (bits >> 58).min(30)));
        }
        for value in values.into_iter().filter(|v| v.is_finite() && *v != 0.0) {
            let text = canonicalize_lenient(&json!(value));
            assert_eq!(text.parse::<f64>(), Ok(value), "{text}");
            // Rust's shortest digits, which differ only at a tie, by one in
            // the last digit, where the even one is written.
            let ours = significant(&text);
            let rusts = significant(&format!("{value:e}"));
            if ours != rusts {
                assert_eq!(ours.len(), rusts.len(), "{value:e} as {text}");
                let (a, b): (u64, u64) = (ours.parse().unwrap(), rusts.parse().unwrap());
                assert_eq!(a.abs_diff(b), 1, "{value:e} as {text}");
                assert_eq!(a % 2, 0, "{value:e} as {text}");
            }
        }
    }

    #[test]
    fn the_lenient_form_writes_exact_integers_beyond_2_53() {
        for (text, written) in [
            ("9007199254740992", "9007199254740992"),
            ("-9007199254740992", "-9007199254740992"),
            ("9223372036854775807", "9223372036854775807"),
            ("-9223372036854775808", "-9223372036854775808"),
            ("18446744073709551615", "18446744073709551615"),
        ] {
            let value = parse(text);
            assert!(!value.is_f64(), "{text} is read exactly");
            assert_eq!(canonicalize_lenient(&value), written);
        }
        // An integer beyond u64 is not read exactly: it is read as a float,
        // and written as one.
        let beyond = parse("100000000000000000000000000000");
        assert!(beyond.is_f64());
        assert!(canonicalize_lenient(&beyond).contains("e+2"), "{beyond}");
    }

    #[test]
    fn the_lenient_form_writes_floats_like_ecmascript() {
        for (value, text) in [
            (1e21, "1e+21"),
            (1.5e300, "1.5e+300"),
            (-1.5e300, "-1.5e+300"),
            (1e-7, "1e-7"),
            (1.2345e-7, "1.2345e-7"),
            (-2.5e-10, "-2.5e-10"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (123e20, "1.23e+22"),
        ] {
            assert_eq!(canonicalize_lenient(&json!(value)), text, "{value}");
        }
        // Inside an object, members still sort, and floats in the plain range
        // keep their plain form.
        assert_eq!(
            canonicalize_lenient(&json!({ "b": 1e21, "a": [9.8, 10.0, -0.0] })),
            r#"{"a":[9.8,10,0],"b":1e+21}"#
        );
    }

    #[test]
    fn the_lenient_form_is_the_strict_form_wherever_that_exists() {
        for value in [
            json!({ "b": 1, "a": [true, null, "x"], "c": { "z": 0, "y": -3 } }),
            json!({ "\u{E000}": 2, "\u{1F600}": 1 }),
            json!("a\"b\\c\nd\u{1f}e/\u{7f}é"),
            json!([9.8, 10.0, 0.1, -0.0, 0.000_001, 123_456.789]),
            json!({ "big": 9_007_199_254_740_991_i64, "neg": -9_007_199_254_740_991_i64 }),
        ] {
            assert_eq!(
                canonicalize_lenient(&value),
                canonicalize(&value).unwrap(),
                "{value}"
            );
        }
    }
}
