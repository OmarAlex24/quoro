//! RFC 8785 JSON Canonicalization Scheme for the values Quoro hashes.
//!
//! The change log and the audit log hash `SHA-256(JCS(entry))`
//! (`audit-log-format.md` section 5). Object members are sorted by the UTF-16
//! code units of their names, strings use the minimal JSON escaping of RFC
//! 8785 section 3.2.2.2, and there is no whitespace. Numbers: Quoro's hashed
//! records only carry integers; an integer serializes as its shortest decimal
//! form, which is what ECMAScript produces for integers below 2^53. A float is
//! refused, because no hashed record is allowed to carry one.

use serde_json::Value;
use std::fmt::Write as _;

/// Error for a value that has no canonical form in Quoro's records.
#[derive(Debug, PartialEq, Eq)]
pub struct NotCanonical(pub String);

/// Canonical JSON text of `value`.
///
/// # Errors
///
/// A non-integer number, or an integer outside the IEEE-754 safe range, has
/// no canonical form that every implementation agrees on, so it is refused.
pub fn canonicalize(value: &Value) -> Result<String, NotCanonical> {
    let mut out = String::new();
    write_value(&mut out, value)?;
    Ok(out)
}

fn write_value(out: &mut String, value: &Value) -> Result<(), NotCanonical> {
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
            } else {
                return Err(NotCanonical(format!(
                    "number {number} is not a safe integer"
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
                write_value(out, item)?;
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
                write_value(out, &map[key.as_str()])?;
            }
            out.push('}');
        }
    }
    Ok(())
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
    fn refuses_floats() {
        assert!(canonicalize(&json!(1.5)).is_err());
        assert!(canonicalize(&json!(u64::MAX)).is_err());
    }
}
