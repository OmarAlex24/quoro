//! Shape validation of tool arguments against a tool's input schema
//! (permission-model.md section 10: the adapter validates the tool arguments).
//!
//! The tool schemas are derived from `docs/contracts/openapi.yaml` (the
//! operations behind the MCP tools), so [`validate`] implements the subset of
//! JSON Schema those schemas use, by hand and without a new dependency.
//!
//! # Supported patterns
//!
//! `pattern` is not a general regex engine. [`supports`] accepts exactly this
//! shape and [`validate`] matches it by hand:
//!
//! - `^<prefix>_[0-9A-HJKMNP-TV-Z]{26}$`, where `<prefix>` is one or more
//!   lowercase ASCII letters. This is the id shape the contract uses
//!   (`^app_...$`, `^prj_...$`, `^vm_...$`, `^inc_...$`, `^nod_...$`,
//!   `^evt_...$`, `^req_...$`, ...). The value is `<prefix>_` followed by
//!   exactly 26 Crockford base32 characters: uppercase `0-9`, `A`-`H`, `J`,
//!   `K`, `M`, `N`, `P`-`T`, `V`-`Z` (no `I`, `L`, `O` or `U`).
//!
//! The operations behind the MCP tools (`listApps`, `getAppLogs`, `listEvents`,
//! `listAudit`, `listIncidents`, `createApproval` and `restartApp`) use no
//! other `pattern`: `Scope` and `EventType` are closed enums, and the
//! `Target`, `Limit`, `Cursor`, `AfterSeq`, `QuoroJustification` and
//! `QuoroIncident` schemas contribute no pattern beyond the id shape above.

use serde_json::{Map, Value};

/// Type names [`validate`] understands.
const TYPES: [&str; 7] = [
    "object", "string", "integer", "number", "boolean", "array", "null",
];

/// Crockford base32 alphabet used by ids (no `I`, `L`, `O` or `U`).
const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Arguments whose presence the service layer enforces, not the adapter.
///
/// `justification` at the top level is deliberately not required here: a call
/// without it is refused by the service layer with `validation_failed` and
/// that refusal is recorded in the audit log (permission-model.md section 10).
/// The adapter must let such a call through so the refusal is audited; a
/// present but empty or over-long justification is still a shape error.
const SERVICE_REQUIRED: [&str; 1] = ["justification"];

/// Checks `arguments` against `schema` (the subset of JSON Schema the tool
/// schemas use). Returns a human-readable reason on the first violation.
///
/// # Errors
///
/// The reason the arguments do not match.
pub fn validate(schema: &Map<String, Value>, arguments: &Map<String, Value>) -> Result<(), String> {
    check_object(schema, arguments, "")
}

/// Checks that `schema` uses only keywords and patterns [`validate`]
/// understands, so a schema can never be silently under-validated.
///
/// # Errors
///
/// The first unsupported keyword or pattern.
pub fn supports(schema: &Map<String, Value>) -> Result<(), String> {
    check_supported(schema, "")
}

fn check_value(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    check_type(schema, value, path)?;
    check_enum(schema, value, path)?;
    check_const(schema, value, path)?;
    match value {
        Value::String(text) => check_string(schema, text, path),
        Value::Number(number) => check_number(schema, number, path),
        Value::Array(items) => check_array(schema, items, path),
        Value::Object(map) => check_object(schema, map, path),
        Value::Bool(_) | Value::Null => Ok(()),
    }
}

fn check_type(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    let Some(expected) = schema.get("type") else {
        return Ok(());
    };
    let names: Vec<&str> = match expected {
        Value::String(name) => vec![name.as_str()],
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        // Malformed schemas are rejected by `supports`; ignore them here.
        _ => return Ok(()),
    };
    if names.is_empty() || names.iter().any(|name| matches_type(name, value)) {
        return Ok(());
    }
    Err(format!("{path}: must be of type {}", names.join(" or ")))
}

fn matches_type(name: &str, value: &Value) -> bool {
    match name {
        "object" => value.is_object(),
        "string" => value.is_string(),
        // `integer` is a JSON number with no fractional part, so a float such
        // as `1.5` does not qualify.
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn check_enum(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    if let Some(Value::Array(allowed)) = schema.get("enum")
        && !allowed.contains(value)
    {
        let allowed: Vec<String> = allowed.iter().map(render).collect();
        return Err(format!("{path}: must be one of {}", allowed.join(", ")));
    }
    Ok(())
}

fn check_const(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    if let Some(expected) = schema.get("const")
        && value != expected
    {
        return Err(format!("{path}: must be {}", render(expected)));
    }
    Ok(())
}

fn check_string(schema: &Map<String, Value>, text: &str, path: &str) -> Result<(), String> {
    let length = text.chars().count() as u64;
    if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64)
        && length < minimum
    {
        return Err(format!("{path}: must be at least {minimum} characters"));
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64)
        && length > maximum
    {
        return Err(format!("{path}: must be at most {maximum} characters"));
    }
    if let Some(Value::String(pattern)) = schema.get("pattern")
        && !matches_pattern(pattern, text)
    {
        return Err(format!("{path}: does not match {pattern}"));
    }
    if schema.get("format").and_then(Value::as_str) == Some("date-time") && !is_date_time(text) {
        return Err(format!("{path}: must be an RFC 3339 date-time"));
    }
    Ok(())
}

fn check_number(
    schema: &Map<String, Value>,
    number: &serde_json::Number,
    path: &str,
) -> Result<(), String> {
    if let Some(limit) = schema.get("minimum")
        && let (Some(value), Some(minimum)) = (number.as_f64(), limit.as_f64())
        && value < minimum
    {
        return Err(format!("{path}: must be at least {limit}"));
    }
    if let Some(limit) = schema.get("maximum")
        && let (Some(value), Some(maximum)) = (number.as_f64(), limit.as_f64())
        && value > maximum
    {
        return Err(format!("{path}: must be at most {limit}"));
    }
    Ok(())
}

fn check_array(schema: &Map<String, Value>, items: &[Value], path: &str) -> Result<(), String> {
    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64)
        && (items.len() as u64) < minimum
    {
        return Err(format!("{path}: must have at least {minimum} items"));
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64)
        && (items.len() as u64) > maximum
    {
        return Err(format!("{path}: must have at most {maximum} items"));
    }
    if let Some(item_schema) = schema.get("items").and_then(Value::as_object) {
        for (index, item) in items.iter().enumerate() {
            check_value(item_schema, item, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn check_object(
    schema: &Map<String, Value>,
    map: &Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            // The service layer enforces and audits a missing top-level
            // `justification`; nested `justification` fields are still shape
            // checked (permission-model.md section 10).
            if path.is_empty() && SERVICE_REQUIRED.contains(&name) {
                continue;
            }
            if !map.contains_key(name) {
                return Err(format!("{}: is required", child(path, name)));
            }
        }
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    let allows_others = !matches!(schema.get("additionalProperties"), Some(Value::Bool(false)));
    for (name, value) in map {
        match properties.and_then(|properties| properties.get(name)) {
            Some(Value::Object(property_schema)) => {
                check_value(property_schema, value, &child(path, name))?;
            }
            Some(_) => {}
            None if !allows_others => {
                return Err(if path.is_empty() {
                    format!("unknown argument {name}")
                } else {
                    format!("{path}: unknown argument {name}")
                });
            }
            None => {}
        }
    }
    Ok(())
}

fn check_supported(schema: &Map<String, Value>, path: &str) -> Result<(), String> {
    for (keyword, value) in schema {
        match keyword.as_str() {
            "type" => check_supported_type(value, path)?,
            "properties" => {
                let Some(properties) = value.as_object() else {
                    return Err(format!("{}: properties must be an object", location(path)));
                };
                for (name, property) in properties {
                    let Some(property) = property.as_object() else {
                        return Err(format!(
                            "{}: property {name} must be a schema object",
                            location(path)
                        ));
                    };
                    check_supported(property, &child(&child(path, "properties"), name))?;
                }
            }
            "items" => {
                let Some(items) = value.as_object() else {
                    return Err(format!("{}: items must be a schema object", location(path)));
                };
                check_supported(items, &child(path, "items"))?;
            }
            "required" => {
                let valid = value
                    .as_array()
                    .is_some_and(|names| names.iter().all(Value::is_string));
                if !valid {
                    return Err(format!(
                        "{}: required must be an array of strings",
                        location(path)
                    ));
                }
            }
            "additionalProperties" => {
                if !value.is_boolean() {
                    return Err(format!(
                        "{}: additionalProperties must be a boolean",
                        location(path)
                    ));
                }
            }
            "enum" => {
                if !value.is_array() {
                    return Err(format!("{}: enum must be an array", location(path)));
                }
            }
            "minLength" | "maxLength" | "minimum" | "maximum" | "minItems" | "maxItems" => {
                if !value.is_number() {
                    return Err(format!("{}: {keyword} must be a number", location(path)));
                }
            }
            "pattern" => {
                let Some(pattern) = value.as_str() else {
                    return Err(format!("{}: unsupported pattern {value}", location(path)));
                };
                if !supports_pattern(pattern) {
                    return Err(format!("{}: unsupported pattern {pattern}", location(path)));
                }
            }
            // Enforced by `validate`, including `date-time`; other formats are
            // annotations.
            "const" | "format" => {}
            // Annotations only: accepted and ignored.
            "description" | "title" | "default" | "examples" | "writeOnly" | "readOnly"
            | "deprecated" => {}
            other => {
                return Err(format!("{}: unsupported keyword {other}", location(path)));
            }
        }
    }
    Ok(())
}

fn check_supported_type(value: &Value, path: &str) -> Result<(), String> {
    let names: Vec<&str> = match value {
        Value::String(name) => vec![name.as_str()],
        Value::Array(items) => {
            let mut names = Vec::with_capacity(items.len());
            for item in items {
                let Some(name) = item.as_str() else {
                    return Err(format!(
                        "{}: type must be a string or an array of strings",
                        location(path)
                    ));
                };
                names.push(name);
            }
            names
        }
        _ => {
            return Err(format!(
                "{}: type must be a string or an array of strings",
                location(path)
            ));
        }
    };
    for name in names {
        if !TYPES.contains(&name) {
            return Err(format!("{}: unsupported type {name}", location(path)));
        }
    }
    Ok(())
}

/// Whether `pattern` is one of the shapes [`validate`] matches by hand.
fn supports_pattern(pattern: &str) -> bool {
    id_prefix(pattern).is_some()
}

/// The `<prefix>` of `^<prefix>_[0-9A-HJKMNP-TV-Z]{26}$`, if `pattern` has that
/// exact shape.
fn id_prefix(pattern: &str) -> Option<&str> {
    const SUFFIX: &str = "_[0-9A-HJKMNP-TV-Z]{26}$";
    let rest = pattern.strip_prefix('^')?;
    let prefix = rest.strip_suffix(SUFFIX)?;
    if !prefix.is_empty() && prefix.bytes().all(|byte| byte.is_ascii_lowercase()) {
        Some(prefix)
    } else {
        None
    }
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    id_prefix(pattern).is_some_and(|prefix| matches_id(prefix, value))
}

/// Whether `value` is `<prefix>_` followed by exactly 26 Crockford characters.
fn matches_id(prefix: &str, value: &str) -> bool {
    let Some(rest) = value.strip_prefix(prefix) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('_') else {
        return false;
    };
    rest.len() == 26 && rest.bytes().all(|byte| CROCKFORD.contains(&byte))
}

/// RFC 3339 shape: `YYYY-MM-DDTHH:MM:SS`, an optional fraction and `Z` or
/// `±HH:MM`. `T`/`t` and `Z`/`z` are both accepted.
fn is_date_time(text: &str) -> bool {
    fn parse(text: &str) -> Option<bool> {
        let bytes = text.as_bytes();
        digits(bytes.get(0..4)?)?;
        if bytes.get(4)? != &b'-' {
            return Some(false);
        }
        let month = digits(bytes.get(5..7)?)?;
        if bytes.get(7)? != &b'-' {
            return Some(false);
        }
        let day = digits(bytes.get(8..10)?)?;
        if !matches!(*bytes.get(10)?, b'T' | b't') {
            return Some(false);
        }
        let hour = digits(bytes.get(11..13)?)?;
        if bytes.get(13)? != &b':' {
            return Some(false);
        }
        let minute = digits(bytes.get(14..16)?)?;
        if bytes.get(16)? != &b':' {
            return Some(false);
        }
        let second = digits(bytes.get(17..19)?)?;
        if !(1..=12).contains(&month)
            || !(1..=31).contains(&day)
            || hour > 23
            || minute > 59
            || second > 60
        {
            return Some(false);
        }
        let mut rest = &bytes[19..];
        if rest.first() == Some(&b'.') {
            rest = &rest[1..];
            let fraction = rest.iter().take_while(|byte| byte.is_ascii_digit()).count();
            if fraction == 0 {
                return Some(false);
            }
            rest = &rest[fraction..];
        }
        match rest {
            [b'Z' | b'z'] => Some(true),
            [sign, hour_1, hour_2, b':', minute_1, minute_2] if *sign == b'+' || *sign == b'-' => {
                let offset_hour = digits(&[*hour_1, *hour_2])?;
                let offset_minute = digits(&[*minute_1, *minute_2])?;
                Some(offset_hour <= 23 && offset_minute <= 59)
            }
            _ => Some(false),
        }
    }
    parse(text).unwrap_or(false)
}

fn digits(bytes: &[u8]) -> Option<u32> {
    let mut value = 0_u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
    }
    Some(value)
}

fn render(value: &Value) -> String {
    match value {
        Value::String(text) => format!("\"{text}\""),
        other => other.to_string(),
    }
}

/// `name` below `path`, as an argument path such as `target.id`.
fn child(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_owned()
    } else {
        format!("{path}.{name}")
    }
}

/// Where a schema keyword lives, for [`supports`] errors.
fn location(path: &str) -> String {
    if path.is_empty() {
        "schema".to_owned()
    } else {
        format!("schema {path}")
    }
}

#[cfg(test)]
mod tests {
    use super::{supports, validate};
    use serde_json::{Map, Value, json};

    const APP_ID: &str = "app_01JC9Z6Q5W8N3K4T7R2M1XHB0D";
    const APP_ID_PATTERN: &str = "^app_[0-9A-HJKMNP-TV-Z]{26}$";

    /// The shape of `getAppLogs`, with `justification`/`incident_id` added as
    /// the MCP tool carries them.
    fn logs_schema() -> Map<String, Value> {
        schema(json!({
            "type": "object",
            "properties": {
                "app_id": { "type": "string", "pattern": APP_ID_PATTERN },
                "since": { "type": "string", "format": "date-time" },
                "until": { "type": ["string", "null"], "format": "date-time" },
                "tail": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5000,
                    "default": 200
                },
                "instance": {
                    "type": ["string", "null"],
                    "pattern": "^vm_[0-9A-HJKMNP-TV-Z]{26}$"
                },
                "justification": { "type": "string", "minLength": 1, "maxLength": 2000 },
                "incident_id": {
                    "type": ["string", "null"],
                    "pattern": "^inc_[0-9A-HJKMNP-TV-Z]{26}$"
                }
            },
            "required": ["app_id", "justification"],
            "additionalProperties": false
        }))
    }

    /// The shape of `listEvents`' `type` filter: an array of `EventType`.
    fn events_schema() -> Map<String, Value> {
        schema(json!({
            "type": "object",
            "properties": {
                "type": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["app.down", "app.up", "vm.crashed"]
                    },
                    "minItems": 1,
                    "maxItems": 3
                }
            },
            "additionalProperties": false
        }))
    }

    fn schema(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .cloned()
            .expect("the schema is a JSON object")
    }

    fn arguments(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .cloned()
            .expect("the arguments are a JSON object")
    }

    fn error(schema: &Map<String, Value>, arguments: &Map<String, Value>) -> String {
        validate(schema, arguments).expect_err("the arguments are invalid")
    }

    /// Checks one value against a schema that is not an object schema, with an
    /// empty argument path.
    fn check(schema: &Map<String, Value>, value: Value) -> Result<(), String> {
        super::check_value(schema, &value, "")
    }

    fn check_error(schema: &Map<String, Value>, value: Value) -> String {
        check(schema, value).expect_err("the value is invalid")
    }

    #[test]
    fn accepts_a_valid_object() {
        let arguments = arguments(json!({
            "app_id": APP_ID,
            "since": "2026-11-03T10:20:30Z",
            "tail": 200,
            "instance": null,
            "justification": "investigating app.down"
        }));
        assert_eq!(validate(&logs_schema(), &arguments), Ok(()));
    }

    #[test]
    fn accepts_an_empty_object_when_nothing_but_justification_is_required() {
        let schema = schema(json!({
            "type": "object",
            "properties": { "justification": { "type": "string" } },
            "required": ["justification"],
            "additionalProperties": false
        }));
        assert_eq!(validate(&schema, &arguments(json!({}))), Ok(()));
    }

    #[test]
    fn rejects_an_unknown_argument() {
        let message = error(
            &logs_schema(),
            &arguments(json!({ "app_id": APP_ID, "foo": 1 })),
        );
        assert_eq!(message, "unknown argument foo");
    }

    #[test]
    fn rejects_a_wrong_type() {
        let message = error(
            &logs_schema(),
            &arguments(json!({ "app_id": APP_ID, "tail": "200" })),
        );
        assert_eq!(message, "tail: must be of type integer");
    }

    #[test]
    fn rejects_a_float_for_an_integer() {
        let message = error(
            &logs_schema(),
            &arguments(json!({ "app_id": APP_ID, "tail": 1.5 })),
        );
        assert_eq!(message, "tail: must be of type integer");
    }

    #[test]
    fn accepts_an_integer_for_a_number() {
        let schema = schema(json!({ "type": "number" }));
        assert_eq!(check(&schema, json!(3)), Ok(()));
        assert_eq!(check(&schema, json!(3.5)), Ok(()));
    }

    #[test]
    fn enforces_minimum_and_maximum() {
        assert_eq!(
            error(
                &logs_schema(),
                &arguments(json!({ "app_id": APP_ID, "tail": 0 })),
            ),
            "tail: must be at least 1"
        );
        assert_eq!(
            error(
                &logs_schema(),
                &arguments(json!({ "app_id": APP_ID, "tail": 6000 })),
            ),
            "tail: must be at most 5000"
        );
    }

    #[test]
    fn enforces_min_length_and_max_length_in_characters() {
        assert_eq!(
            error(
                &logs_schema(),
                &arguments(json!({ "app_id": APP_ID, "justification": "" })),
            ),
            "justification: must be at least 1 characters"
        );
        let long = "é".repeat(2001);
        let message = error(
            &logs_schema(),
            &arguments(json!({ "app_id": APP_ID, "justification": long })),
        );
        assert_eq!(message, "justification: must be at most 2000 characters");
        // 2000 multi-byte characters are 4000 bytes but within `maxLength`.
        let accepted = "é".repeat(2000);
        assert_eq!(
            validate(
                &logs_schema(),
                &arguments(json!({ "app_id": APP_ID, "justification": accepted })),
            ),
            Ok(())
        );
    }

    #[test]
    fn enforces_enum() {
        let schema = schema(json!({
            "type": "object",
            "properties": {
                "status": { "type": "string", "enum": ["open", "resolved"] }
            },
            "additionalProperties": false
        }));
        assert_eq!(
            validate(&schema, &arguments(json!({ "status": "open" }))),
            Ok(())
        );
        assert_eq!(
            error(&schema, &arguments(json!({ "status": "closed" }))),
            "status: must be one of \"open\", \"resolved\""
        );
    }

    #[test]
    fn enforces_array_items_and_bounds() {
        let schema = events_schema();
        assert_eq!(
            validate(
                &schema,
                &arguments(json!({ "type": ["app.down", "vm.crashed"] })),
            ),
            Ok(())
        );
        assert_eq!(
            error(
                &schema,
                &arguments(json!({ "type": ["app.down", "app.exploded"] })),
            ),
            "type[1]: must be one of \"app.down\", \"app.up\", \"vm.crashed\""
        );
        assert_eq!(
            error(&schema, &arguments(json!({ "type": [] }))),
            "type: must have at least 1 items"
        );
        assert_eq!(
            error(
                &schema,
                &arguments(json!({ "type": ["app.down", "app.up", "vm.crashed", "app.down"] })),
            ),
            "type: must have at most 3 items"
        );
        assert_eq!(
            error(&schema, &arguments(json!({ "type": ["app.down", 7] }))),
            "type[1]: must be of type string"
        );
    }

    #[test]
    fn accepts_rfc_3339_date_times() {
        for value in [
            "2026-11-03T10:20:30Z",
            "2026-11-03t10:20:30z",
            "2026-11-03T10:20:30.123Z",
            "2026-11-03T10:20:30.5+02:00",
            "2026-11-03T10:20:30-07:30",
            "2026-11-03T10:20:60Z",
        ] {
            let schema = schema(json!({ "type": "string", "format": "date-time" }));
            assert_eq!(check(&schema, json!(value)), Ok(()), "{value}");
        }
    }

    #[test]
    fn rejects_bad_date_times() {
        for value in [
            "2026-11-03 10:20:30Z",
            "2026-11-03T10:20:30",
            "2026-11-03T10:20:30.Z",
            "2026-11-03T10:20:30+0200",
            "2026-13-03T10:20:30Z",
            "2026-11-03T24:00:00Z",
            "2026-11-03T10:60:30Z",
            "26-11-03T10:20:30Z",
        ] {
            let schema = schema(json!({ "type": "string", "format": "date-time" }));
            let message = check_error(&schema, json!(value));
            assert!(message.contains("date-time"), "{value}: {message}");
        }
    }

    #[test]
    fn accepts_an_id() {
        let schema = schema(json!({ "type": "string", "pattern": APP_ID_PATTERN }));
        assert_eq!(check(&schema, json!(APP_ID)), Ok(()));
    }

    #[test]
    fn rejects_an_id_with_the_wrong_prefix() {
        let schema = schema(json!({ "type": "string", "pattern": APP_ID_PATTERN }));
        let message = check_error(&schema, json!("prj_01JC9Z6Q5W8N3K4T7R2M1XHB0D"));
        assert_eq!(message, format!(": does not match {APP_ID_PATTERN}"));
    }

    #[test]
    fn rejects_a_lowercase_or_forbidden_letter_id() {
        let schema = schema(json!({ "type": "string", "pattern": APP_ID_PATTERN }));
        for value in [
            "app_01jc9z6q5w8n3k4t7r2m1xhb0d",
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0I",
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0L",
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0O",
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0U",
        ] {
            assert_eq!(
                check_error(&schema, json!(value)),
                format!(": does not match {APP_ID_PATTERN}"),
                "{value}"
            );
        }
    }

    #[test]
    fn rejects_an_id_of_the_wrong_length() {
        let schema = schema(json!({ "type": "string", "pattern": APP_ID_PATTERN }));
        for value in [
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0",
            "app_01JC9Z6Q5W8N3K4T7R2M1XHB0DD",
            "app_",
        ] {
            assert_eq!(
                check_error(&schema, json!(value)),
                format!(": does not match {APP_ID_PATTERN}"),
                "{value}"
            );
        }
    }

    #[test]
    fn rejects_a_missing_required_argument() {
        let message = error(
            &logs_schema(),
            &arguments(json!({ "justification": "because" })),
        );
        assert_eq!(message, "app_id: is required");
    }

    #[test]
    fn accepts_a_missing_top_level_justification() {
        assert_eq!(
            validate(&logs_schema(), &arguments(json!({ "app_id": APP_ID }))),
            Ok(())
        );
    }

    #[test]
    fn enforces_a_nested_justification() {
        let schema = schema(json!({
            "type": "object",
            "properties": {
                "request": {
                    "type": "object",
                    "properties": {
                        "justification": { "type": "string", "minLength": 1 }
                    },
                    "required": ["justification"]
                }
            }
        }));
        assert_eq!(
            error(&schema, &arguments(json!({ "request": {} }))),
            "request.justification: is required"
        );
    }

    #[test]
    fn accepts_null_for_a_nullable_type() {
        let schema = schema(json!({ "type": ["string", "null"] }));
        assert_eq!(check(&schema, Value::Null), Ok(()));
        assert_eq!(check(&schema, json!("x")), Ok(()));
        assert_eq!(
            check_error(&schema, json!(1)),
            ": must be of type string or null"
        );
    }

    #[test]
    fn a_nullable_id_still_accepts_null() {
        assert_eq!(
            validate(
                &logs_schema(),
                &arguments(json!({ "app_id": APP_ID, "incident_id": null })),
            ),
            Ok(())
        );
    }

    #[test]
    fn enforces_const() {
        let schema = schema(json!({ "const": "restart_app" }));
        assert_eq!(check(&schema, json!("restart_app")), Ok(()));
        assert_eq!(
            check_error(&schema, json!("delete_app")),
            ": must be \"restart_app\""
        );
    }

    #[test]
    fn supports_accepts_the_tool_schemas() {
        assert_eq!(supports(&logs_schema()), Ok(()));
        assert_eq!(supports(&events_schema()), Ok(()));
    }

    #[test]
    fn supports_accepts_annotation_keywords() {
        let schema = schema(json!({
            "type": "object",
            "title": "Thing",
            "description": "A thing.",
            "properties": {
                "id": {
                    "type": "string",
                    "format": "uuid",
                    "default": "x",
                    "examples": ["x"],
                    "deprecated": false,
                    "readOnly": false,
                    "writeOnly": false
                }
            },
            "additionalProperties": false
        }));
        assert_eq!(supports(&schema), Ok(()));
    }

    #[test]
    fn supports_accepts_each_supported_pattern() {
        for prefix in [
            "app", "prj", "vm", "inc", "nod", "evt", "req", "dep", "dom", "usr", "agt", "akey",
        ] {
            let pattern = format!("^{prefix}_[0-9A-HJKMNP-TV-Z]{{26}}$");
            let schema = schema(json!({ "type": "string", "pattern": pattern }));
            assert_eq!(supports(&schema), Ok(()), "{pattern}");
            assert_eq!(
                check(
                    &schema,
                    json!(format!("{prefix}_01JC9Z6Q5W8N3K4T7R2M1XHB0D"))
                ),
                Ok(()),
                "{pattern}"
            );
        }
    }

    #[test]
    fn supports_rejects_an_unknown_keyword() {
        let schema = schema(json!({
            "type": "object",
            "properties": {
                "tail": { "type": "integer", "exclusiveMaximum": 5000 }
            }
        }));
        let message = supports(&schema).expect_err("the keyword is unsupported");
        assert_eq!(
            message,
            "schema properties.tail: unsupported keyword exclusiveMaximum"
        );
    }

    #[test]
    fn supports_rejects_an_unknown_pattern() {
        let schema = schema(json!({
            "type": "object",
            "properties": {
                "hash": { "type": "string", "pattern": "^[0-9a-f]{64}$" }
            }
        }));
        let message = supports(&schema).expect_err("the pattern is unsupported");
        assert_eq!(
            message,
            "schema properties.hash: unsupported pattern ^[0-9a-f]{64}$"
        );
    }

    #[test]
    fn supports_rejects_an_unknown_type() {
        let schema = schema(json!({ "type": "timestamp" }));
        let message = supports(&schema).expect_err("the type is unsupported");
        assert_eq!(message, "schema: unsupported type timestamp");
    }

    #[test]
    fn supports_rejects_a_reference() {
        let schema = schema(json!({
            "type": "object",
            "properties": { "target": { "$ref": "#/components/schemas/Target" } }
        }));
        let message = supports(&schema).expect_err("the reference is unsupported");
        assert_eq!(
            message,
            "schema properties.target: unsupported keyword $ref"
        );
    }
}
