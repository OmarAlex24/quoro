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
                // Rust's `Display` for f64 is the shortest round-trip decimal
                // and never uses an exponent, which is what ECMAScript prints
                // in this range. `-0.0` prints as `0` in ECMAScript.
                if f == 0.0 {
                    out.push('0');
                } else {
                    let _ = write!(out, "{f}");
                }
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
        // Rust's `LowerExp` writes the shortest round-trip digits, `d[.ddd]`,
        // then `e` and the exponent; ECMAScript writes the same digits with a
        // sign on the exponent (`1e+21`, `1.5e-7`).
        let exp = format!("{f:e}");
        match exp.split_once('e') {
            Some((digits, exponent)) if !exponent.starts_with('-') => {
                let _ = write!(out, "{digits}e+{exponent}");
            }
            _ => out.push_str(&exp),
        }
    }
}

/// Whether ECMAScript prints `f` without an exponent: finite, and 0 or with a
/// magnitude in `[1e-6, 1e21)`. A float outside it is refused.
fn plain_float(f: f64) -> bool {
    f.is_finite() && (f == 0.0 || (1e-6..1e21).contains(&f.abs()))
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

        let float = parse(r#"{"x": 18983552425072.563}"#);
        assert_eq!(canonicalize_lenient(&float), canonicalize(&float).unwrap());
        assert_eq!(canonicalize_lenient(&float), r#"{"x":18983552425072.563}"#);
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
