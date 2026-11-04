//! D-0048: the hand-written `docs/contracts/openapi.yaml` is the source of
//! truth. This test generates the document from the handlers' utoipa
//! annotations and fails on any semantic difference for every implemented
//! operation, and for every component schema those operations reference.
//!
//! Compared: operation id, `x-quoro-scope`, `x-quoro-mcp-tool`, effective
//! security, parameters (name, location, required, schema), request body
//! (required, media types, schema), responses (status codes, response header
//! names, media types, schema) and, per schema: type with nullability,
//! format, enum, const, pattern, numeric and length bounds, `minItems`,
//! `uniqueItems`, `default`, `writeOnly`, items, properties, `required` and
//! `additionalProperties`. Not compared: descriptions, titles, examples,
//! tags and numeric width formats (`int32`/`int64`/`float`/`double`), which
//! are prose or representation hints. Every response must still carry a
//! non-empty description (F-0042).

use quoro_control_plane::openapi::ApiDoc;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use utoipa::OpenApi;

fn frozen() -> Value {
    let text = include_str!("../../../docs/contracts/openapi.yaml");
    serde_norway::from_str(text).expect("frozen openapi.yaml parses")
}

fn generated() -> Value {
    serde_json::to_value(ApiDoc::openapi()).expect("generated document serializes")
}

/// Follows a local `$ref` to `#/components/<kind>/<name>`.
fn resolve<'a>(doc: &'a Value, node: &'a Value) -> &'a Value {
    match node.get("$ref").and_then(Value::as_str) {
        Some(r) => {
            let parts: Vec<&str> = r.trim_start_matches("#/").split('/').collect();
            let mut cur = doc;
            for p in parts {
                cur = &cur[p];
            }
            resolve(doc, cur)
        }
        None => node,
    }
}

fn schema_ref_name(node: &Value) -> Option<String> {
    node.get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix("#/components/schemas/"))
        .map(str::to_string)
}

/// Normalized schema; collects referenced component names into `refs`.
fn norm_schema(node: &Value, refs: &mut BTreeSet<String>) -> Value {
    if let Some(name) = schema_ref_name(node) {
        refs.insert(name.clone());
        return json!({ "ref": name });
    }
    let Some(obj) = node.as_object() else {
        return node.clone();
    };
    let mut out = Map::new();
    let mut types: BTreeSet<String> = BTreeSet::new();
    match obj.get("type") {
        Some(Value::String(t)) => {
            types.insert(t.clone());
        }
        Some(Value::Array(ts)) => {
            for t in ts {
                if let Some(t) = t.as_str() {
                    types.insert(t.to_string());
                }
            }
        }
        _ => {}
    }
    // `oneOf`/`anyOf` with a `null` member is nullability, however written.
    for key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(members)) = obj.get(key) {
            let (nulls, rest): (Vec<&Value>, Vec<&Value>) = members.iter().partition(|m| {
                m.get("type").and_then(Value::as_str) == Some("null")
                    && m.as_object().is_some_and(|o| o.len() <= 2)
            });
            if !nulls.is_empty() {
                types.insert("null".to_string());
            }
            if rest.len() == 1 {
                let inner = norm_schema(rest[0], refs);
                if let Value::Object(inner) = inner {
                    for (k, v) in inner {
                        if k == "type" {
                            if let Value::Array(ts) = v {
                                for t in ts {
                                    types.insert(t.as_str().unwrap_or_default().to_string());
                                }
                            }
                        } else {
                            out.insert(k, v);
                        }
                    }
                }
            } else if !rest.is_empty() {
                out.insert(
                    key.to_string(),
                    Value::Array(rest.iter().map(|m| norm_schema(m, refs)).collect()),
                );
            }
        }
    }
    if !types.is_empty() {
        out.insert(
            "type".into(),
            Value::Array(types.iter().cloned().map(Value::String).collect()),
        );
    }
    for key in [
        "format",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "uniqueItems",
        "default",
        "writeOnly",
        "const",
    ] {
        if let Some(v) = obj.get(key) {
            if key == "format"
                && matches!(
                    v.as_str(),
                    Some(
                        "int32"
                            | "int64"
                            | "uint32"
                            | "uint64"
                            | "int8"
                            | "uint8"
                            | "int16"
                            | "uint16"
                            | "float"
                            | "double"
                    )
                )
            {
                continue;
            }
            if key == "writeOnly" && v == &Value::Bool(false) {
                continue;
            }
            // Bounds compare numerically: YAML `1` and JSON `1.0` are equal.
            if let Some(n) = v.as_f64().filter(|_| {
                key.starts_with("min") || key.starts_with("max") || key.starts_with("exclusive")
            }) {
                out.insert(key.into(), json!(n));
                continue;
            }
            out.insert(key.into(), v.clone());
        }
    }
    if let Some(Value::Array(values)) = obj.get("enum") {
        let set: BTreeSet<String> = values
            .iter()
            .filter(|v| !v.is_null())
            .map(Value::to_string)
            .collect();
        if values.iter().any(Value::is_null)
            && let Some(Value::Array(ts)) = out.get_mut("type")
            && !ts.contains(&json!("null"))
        {
            ts.push(json!("null"));
        }
        out.insert("enum".into(), json!(set));
    }
    if let Some(items) = obj.get("items") {
        out.insert("items".into(), norm_schema(items, refs));
    }
    if let Some(Value::Object(props)) = obj.get("properties") {
        let mut p = Map::new();
        for (k, v) in props {
            p.insert(k.clone(), norm_schema(v, refs));
        }
        out.insert("properties".into(), Value::Object(p));
    }
    if let Some(Value::Array(req)) = obj.get("required") {
        let set: BTreeSet<String> = req
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        if !set.is_empty() {
            out.insert("required".into(), json!(set));
        }
    }
    if let Some(ap) = obj.get("additionalProperties") {
        match ap {
            Value::Bool(true) => {}
            Value::Bool(false) => {
                out.insert("additionalProperties".into(), json!(false));
            }
            other => {
                out.insert("additionalProperties".into(), norm_schema(other, refs));
            }
        }
    }
    if let Some(Value::Array(all)) = obj.get("allOf") {
        out.insert(
            "allOf".into(),
            Value::Array(all.iter().map(|m| norm_schema(m, refs)).collect()),
        );
    }
    Value::Object(out)
}

fn norm_content(doc: &Value, content: Option<&Value>, refs: &mut BTreeSet<String>) -> Value {
    let mut out = Map::new();
    if let Some(Value::Object(content)) = content {
        for (media, body) in content {
            out.insert(
                media.clone(),
                norm_schema(body.get("schema").unwrap_or(&Value::Null), refs),
            );
        }
    }
    let _ = doc;
    Value::Object(out)
}

fn effective_security(doc: &Value, op: &Value) -> Value {
    let sec = op
        .get("security")
        .or_else(|| doc.get("security"))
        .cloned()
        .unwrap_or(json!([]));
    // `[{}]` (anonymous allowed) and `[]` both mean no authentication here.
    let mut set: BTreeSet<String> = BTreeSet::new();
    if let Value::Array(reqs) = sec {
        for r in reqs {
            if let Value::Object(r) = r {
                let mut names: Vec<&String> = r.keys().collect();
                names.sort();
                if !names.is_empty() {
                    set.insert(
                        names
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join("+"),
                    );
                }
            }
        }
    }
    json!(set)
}

fn norm_operation(
    doc: &Value,
    op: &Value,
    refs: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
    label: &str,
) -> Value {
    let mut out = Map::new();
    for key in ["operationId", "x-quoro-scope", "x-quoro-mcp-tool"] {
        if let Some(v) = op.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    out.insert("security".into(), effective_security(doc, op));
    let mut params = BTreeMap::new();
    if let Some(Value::Array(ps)) = op.get("parameters") {
        for p in ps {
            let p = resolve(doc, p);
            let name = p["name"].as_str().unwrap_or_default();
            let location = p["in"].as_str().unwrap_or_default();
            // Header names are case-insensitive.
            let name = if location == "header" {
                name.to_ascii_lowercase()
            } else {
                name.to_string()
            };
            params.insert(
                format!("{location}:{name}"),
                json!({
                    "required": p.get("required").and_then(Value::as_bool).unwrap_or(false),
                    "schema": norm_schema(p.get("schema").unwrap_or(&Value::Null), refs),
                }),
            );
        }
    }
    out.insert("parameters".into(), json!(params));
    if let Some(body) = op.get("requestBody") {
        let body = resolve(doc, body);
        out.insert(
            "requestBody".into(),
            json!({
                "required": body.get("required").and_then(Value::as_bool).unwrap_or(false),
                "content": norm_content(doc, body.get("content"), refs),
            }),
        );
    }
    let mut responses = BTreeMap::new();
    if let Some(Value::Object(rs)) = op.get("responses") {
        for (status, r) in rs {
            let r = resolve(doc, r);
            if r.get("description")
                .and_then(Value::as_str)
                .is_none_or(|d| d.trim().is_empty())
            {
                problems.push(format!(
                    "{label}: response {status} has an empty description (F-0042)"
                ));
            }
            let headers: BTreeSet<String> = r
                .get("headers")
                .and_then(Value::as_object)
                .map(|h| h.keys().map(|k| k.to_ascii_lowercase()).collect())
                .unwrap_or_default();
            responses.insert(
                status.clone(),
                json!({ "headers": headers, "content": norm_content(doc, r.get("content"), refs) }),
            );
        }
    }
    out.insert("responses".into(), json!(responses));
    Value::Object(out)
}

fn diff(path: &str, a: &Value, b: &Value, out: &mut Vec<String>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let keys: BTreeSet<&String> = x.keys().chain(y.keys()).collect();
            for k in keys {
                match (x.get(k), y.get(k)) {
                    (Some(p), Some(q)) => diff(&format!("{path}.{k}"), p, q, out),
                    (Some(p), None) => {
                        out.push(format!("{path}.{k}: frozen {p}, generated absent"))
                    }
                    (None, Some(q)) => {
                        out.push(format!("{path}.{k}: frozen absent, generated {q}"))
                    }
                    (None, None) => {}
                }
            }
        }
        _ if a != b => out.push(format!("{path}: frozen {a}, generated {b}")),
        _ => {}
    }
}

#[test]
fn generated_document_matches_the_frozen_contract() {
    let frozen = frozen();
    let generated = generated();
    let mut problems = Vec::new();
    let mut frozen_refs = BTreeSet::new();
    let mut generated_refs = BTreeSet::new();
    let mut operations = 0;

    let paths = generated["paths"].as_object().expect("generated paths");
    assert!(!paths.is_empty(), "no operation is annotated");
    for (path, item) in paths {
        for (method, op) in item.as_object().expect("path item") {
            if !["get", "post", "put", "patch", "delete"].contains(&method.as_str()) {
                continue;
            }
            operations += 1;
            let label = format!("{} {path}", method.to_uppercase());
            let Some(frozen_op) = frozen["paths"].get(path).and_then(|i| i.get(method)) else {
                problems.push(format!("{label}: not in the frozen contract"));
                continue;
            };
            let f = norm_operation(
                &frozen,
                frozen_op,
                &mut frozen_refs,
                &mut problems,
                &format!("frozen {label}"),
            );
            let g = norm_operation(
                &generated,
                op,
                &mut generated_refs,
                &mut problems,
                &format!("generated {label}"),
            );
            diff(&label, &f, &g, &mut problems);
        }
    }

    // Compare every component schema either side references, transitively.
    let mut done = BTreeSet::new();
    let mut queue: Vec<String> = frozen_refs.union(&generated_refs).cloned().collect();
    while let Some(name) = queue.pop() {
        if !done.insert(name.clone()) {
            continue;
        }
        let f = frozen["components"]["schemas"].get(&name);
        let g = generated["components"]["schemas"].get(&name);
        match (f, g) {
            (Some(f), Some(g)) => {
                let mut more = BTreeSet::new();
                let fs = norm_schema(f, &mut more);
                let gs = norm_schema(g, &mut more);
                diff(&format!("schema {name}"), &fs, &gs, &mut problems);
                queue.extend(more);
            }
            (None, _) => problems.push(format!("schema {name}: not in the frozen contract")),
            (_, None) => problems.push(format!("schema {name}: referenced but not generated")),
        }
    }

    assert!(
        problems.is_empty(),
        "{} semantic differences across {operations} operations:\n{}",
        problems.len(),
        problems.join("\n")
    );
    println!(
        "{operations} operations and {} schemas match the frozen contract",
        done.len()
    );
}
