//! Contract tests: the MCP crate against the frozen contracts.
//!
//! Every tool of [`quoro_mcp::tools`] must match the "MCP tools" table of
//! `docs/contracts/README.md`, the annotated operation of
//! `docs/contracts/openapi.yaml` and the derivation rules that produce its
//! input schema. The last test is the static half of ADR 0031: the adapter
//! depends on no database crate and never builds an audit record.

use quoro_mcp::tools::{ToolSpec, list, specs};
use serde_json::{Map, Value, json};
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A repository file, located from this crate's manifest directory.
fn repo_file(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

fn read(relative: &str) -> String {
    let path = repo_file(relative);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

fn openapi() -> Value {
    let path = repo_file("docs/contracts/openapi.yaml");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    serde_norway::from_str(&text)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", path.display()))
}

/// One row of the README "MCP tools" table.
struct TableRow {
    tool: String,
    method: String,
    path: String,
    scope: String,
    destructive: bool,
}

/// Parses the README "### MCP tools" table. The scope cell of `delete_app`
/// reads `` `app:delete` (destructive) ``, so the scope is the first backticked
/// token and the flag is the parenthetical.
fn table() -> Vec<TableRow> {
    let text = read("docs/contracts/README.md");
    let mut rows = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if line.trim() == "### MCP tools" {
            inside = true;
            continue;
        }
        if inside && line.starts_with("### ") {
            break;
        }
        if !inside {
            continue;
        }
        let trimmed = line.trim();
        if !trimmed.starts_with("| `") {
            continue;
        }
        let cells: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if cells.len() < 4 {
            continue;
        }
        let tool = cells[1].trim_matches('`').to_string();
        let endpoint = cells[2].trim_matches('`');
        let (method, path) = endpoint
            .split_once(' ')
            .unwrap_or_else(|| panic!("{tool}: endpoint cell {endpoint:?} is not 'METHOD /path'"));
        let scope_cell = cells[3];
        let scope = scope_cell
            .split('`')
            .nth(1)
            .unwrap_or_else(|| panic!("{tool}: no backticked scope in {scope_cell:?}"))
            .to_string();
        rows.push(TableRow {
            tool,
            method: method.to_string(),
            path: path.to_string(),
            scope,
            destructive: scope_cell.contains("(destructive)"),
        });
    }
    assert!(!rows.is_empty(), "the README has no MCP tools table rows");
    rows
}

/// One operation of `openapi.yaml` with the method and path it lives under.
struct Operation<'a> {
    method: &'a str,
    path: &'a str,
    value: &'a Value,
}

const METHODS: [&str; 5] = ["get", "post", "put", "patch", "delete"];

fn operations(doc: &Value) -> Vec<Operation<'_>> {
    let Some(paths) = doc.get("paths").and_then(Value::as_object) else {
        panic!("openapi.yaml has no paths object");
    };
    let mut operations = Vec::new();
    for (path, item) in paths {
        let Some(item) = item.as_object() else {
            continue;
        };
        for method in METHODS {
            if let Some(value) = item.get(method) {
                operations.push(Operation {
                    method,
                    path,
                    value,
                });
            }
        }
    }
    operations
}

fn operation<'a>(doc: &'a Value, operation_id: &str) -> Operation<'a> {
    operations(doc)
        .into_iter()
        .find(|operation| {
            operation.value.get("operationId").and_then(Value::as_str) == Some(operation_id)
        })
        .unwrap_or_else(|| panic!("openapi.yaml has no operation with operationId {operation_id}"))
}

// ---------------------------------------------------------------------------
// Reference resolution and schema derivation
// ---------------------------------------------------------------------------

fn resolve_pointer<'a>(doc: &'a Value, reference: &str, section: &str) -> &'a Value {
    let prefix = format!("#/components/{section}/");
    let name = reference
        .strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("unsupported reference {reference} (expected {prefix}...)"));
    doc.get("components")
        .and_then(|components| components.get(section))
        .and_then(|entries| entries.get(name))
        .unwrap_or_else(|| panic!("unresolved reference {reference}"))
}

fn resolve_parameter<'a>(doc: &'a Value, parameter: &'a Value) -> &'a Value {
    if let Some(reference) = parameter.get("$ref").and_then(Value::as_str) {
        resolve_pointer(doc, reference, "parameters")
    } else {
        parameter
    }
}

fn resolve_component<'a>(doc: &'a Value, value: &'a Value, section: &str) -> &'a Value {
    if let Some(reference) = value.get("$ref").and_then(Value::as_str) {
        resolve_pointer(doc, reference, section)
    } else {
        value
    }
}

/// Replaces every `$ref: '#/components/schemas/Y'` at any depth with a deep
/// copy of the (recursively resolved) component.
fn resolve_schema(doc: &Value, schema: &Value) -> Value {
    resolve_schema_at(doc, schema, 0)
}

fn resolve_schema_at(doc: &Value, schema: &Value, depth: usize) -> Value {
    assert!(depth < 64, "schema references nest too deeply at {schema}");
    match schema {
        Value::Object(map) => {
            if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                let target = resolve_pointer(doc, reference, "schemas");
                return resolve_schema_at(doc, target, depth + 1);
            }
            Value::Object(
                map.iter()
                    .map(|(key, value)| (key.clone(), resolve_schema_at(doc, value, depth)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| resolve_schema_at(doc, item, depth))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A `|` block description: lines trimmed, empty ones dropped, joined by single
/// spaces.
fn normalize_description(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The property of a path or query parameter: its resolved schema plus the
/// parameter's description (which wins over any description in the schema).
fn property_from_parameter(doc: &Value, parameter: &Value, name: &str) -> Value {
    let mut schema = resolve_schema(doc, parameter.get("schema").unwrap_or(&Value::Null));
    let description = normalize_description(
        parameter
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or(""),
    );
    schema
        .as_object_mut()
        .unwrap_or_else(|| panic!("parameter {name} has no schema object"))
        .insert("description".to_string(), json!(description));
    schema
}

fn push_parameter(
    doc: &Value,
    parameter: &Value,
    properties: &mut Vec<(String, Value)>,
    required: &mut Vec<String>,
) {
    let name = parameter
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("a parameter has no name"));
    let schema = property_from_parameter(doc, parameter, name);
    properties.push((name.to_string(), schema));
    if parameter
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        required.push(name.to_string());
    }
}

/// Builds the input schema the way `tools.rs` documents: path parameters, then
/// query parameters, then the two MCP headers, then a JSON request body.
fn expected_schema(doc: &Value, operation: &Value) -> Value {
    let parameters: Vec<&Value> = operation
        .get("parameters")
        .and_then(Value::as_array)
        .map(|parameters| {
            parameters
                .iter()
                .map(|parameter| resolve_parameter(doc, parameter))
                .collect()
        })
        .unwrap_or_default();

    let mut properties: Vec<(String, Value)> = Vec::new();
    let mut required: Vec<String> = Vec::new();

    for group in ["path", "query"] {
        for parameter in &parameters {
            if parameter.get("in").and_then(Value::as_str) == Some(group) {
                push_parameter(doc, parameter, &mut properties, &mut required);
            }
        }
    }

    for parameter in &parameters {
        if parameter.get("in").and_then(Value::as_str) != Some("header") {
            continue;
        }
        match parameter.get("name").and_then(Value::as_str) {
            Some("Quoro-Justification") => {
                properties.push((
                    "justification".to_string(),
                    property_from_parameter(doc, parameter, "justification"),
                ));
                required.push("justification".to_string());
            }
            Some("Quoro-Incident") => {
                if properties.iter().any(|(name, _)| name == "incident_id") {
                    continue;
                }
                properties.push((
                    "incident_id".to_string(),
                    property_from_parameter(doc, parameter, "incident_id"),
                ));
            }
            _ => {}
        }
    }

    if let Some(body) = operation.get("requestBody") {
        let body = resolve_component(doc, body, "requestBodies");
        if let Some(schema) = body
            .get("content")
            .and_then(|content| content.get("application/json"))
            .and_then(|media| media.get("schema"))
        {
            let schema = resolve_schema(doc, schema);
            if let Some(schema) = schema.as_object() {
                if let Some(body_properties) = schema.get("properties").and_then(Value::as_object) {
                    for (name, value) in body_properties {
                        if properties.iter().any(|(existing, _)| existing == name) {
                            continue;
                        }
                        properties.push((name.clone(), value.clone()));
                    }
                }
                if let Some(body_required) = schema.get("required").and_then(Value::as_array) {
                    for name in body_required.iter().filter_map(Value::as_str) {
                        if !required.iter().any(|existing| existing == name) {
                            required.push(name.to_string());
                        }
                    }
                }
            }
        }
    }

    let mut object = Map::new();
    object.insert("type".to_string(), json!("object"));
    object.insert(
        "properties".to_string(),
        Value::Object(properties.into_iter().collect()),
    );
    if !required.is_empty() {
        object.insert("required".to_string(), json!(required));
    }
    object.insert("additionalProperties".to_string(), json!(false));
    Value::Object(object)
}

/// Sorts `required` so the comparison is order-insensitive.
fn sorted_required(mut schema: Value) -> Value {
    if let Some(object) = schema.as_object_mut()
        && let Some(Value::Array(names)) = object.get_mut("required")
    {
        names.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
    schema
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|error| format!("<unprintable: {error}>"))
}

fn schema_of(spec: &ToolSpec) -> Map<String, Value> {
    let Value::Object(schema) = (spec.input_schema)() else {
        panic!("{}: the input schema is not a JSON object", spec.name);
    };
    schema
}

fn required_names(schema: &Map<String, Value>) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn has_justification(doc: &Value, operation: &Value) -> bool {
    let parameters = operation.get("parameters").and_then(Value::as_array);
    parameters.is_some_and(|parameters| {
        parameters.iter().any(|parameter| {
            let parameter = resolve_parameter(doc, parameter);
            parameter.get("name").and_then(Value::as_str) == Some("Quoro-Justification")
        })
    })
}

/// The `required` flag of the path or query parameter named `name`, or `None`
/// when the operation declares no such parameter (so the property can only
/// come from the `Quoro-Incident` header).
fn parameter(doc: &Value, operation: &Value, name: &str) -> Option<bool> {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .and_then(|parameters| {
            parameters.iter().find_map(|parameter| {
                let parameter = resolve_parameter(doc, parameter);
                let is_named = matches!(
                    parameter.get("in").and_then(Value::as_str),
                    Some("path" | "query")
                ) && parameter.get("name").and_then(Value::as_str) == Some(name);
                is_named.then(|| {
                    parameter
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
            })
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn tool_set_matches_the_contract_table() {
    let rows = table();
    let mut expected: Vec<&str> = rows.iter().map(|row| row.tool.as_str()).collect();
    let mut actual: Vec<&str> = specs().iter().map(|spec| spec.name).collect();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(
        actual, expected,
        "specs() must equal the README MCP tools table"
    );

    for spec in specs() {
        let row = rows
            .iter()
            .find(|row| row.tool == spec.name)
            .unwrap_or_else(|| panic!("{} is missing from the README table", spec.name));
        assert_eq!(spec.scope, row.scope, "{}: scope disagrees", spec.name);
        assert_eq!(
            spec.destructive, row.destructive,
            "{}: destructive flag disagrees with the README",
            spec.name
        );
    }
}

#[test]
fn every_tool_maps_to_its_annotated_operation() {
    let doc = openapi();
    let rows = table();
    let known: Vec<&str> = specs().iter().map(|spec| spec.name).collect();

    for operation in operations(&doc) {
        if let Some(name) = operation
            .value
            .get("x-quoro-mcp-tool")
            .and_then(Value::as_str)
        {
            assert!(
                known.contains(&name),
                "{} {} is annotated with tool {name}, which is not a wired tool",
                operation.method,
                operation.path
            );
        }
    }

    for spec in specs() {
        let operation = operation(&doc, spec.operation_id);
        let annotated = operation
            .value
            .get("x-quoro-mcp-tool")
            .and_then(Value::as_str);
        assert_eq!(
            annotated,
            Some(spec.name),
            "{}: operation {} annotations disagree",
            spec.name,
            spec.operation_id
        );
        assert_eq!(
            operation.value.get("x-quoro-scope").and_then(Value::as_str),
            Some(spec.scope),
            "{}: x-quoro-scope disagrees",
            spec.name
        );
        assert_eq!(
            operation.value.get("summary").and_then(Value::as_str),
            Some(spec.title),
            "{}: summary disagrees with the tool title",
            spec.name
        );

        let row = rows
            .iter()
            .find(|row| row.tool == spec.name)
            .unwrap_or_else(|| panic!("{} is missing from the README table", spec.name));
        assert_eq!(
            operation.method.to_ascii_uppercase(),
            row.method,
            "{}: method disagrees with the README",
            spec.name
        );
        assert_eq!(
            operation.path, row.path,
            "{}: path disagrees with the README",
            spec.name
        );
        assert_eq!(
            spec.mutating,
            has_justification(&doc, operation.value),
            "{}: mutating flag disagrees with the Quoro-Justification parameter",
            spec.name
        );
    }
}

#[test]
fn input_schemas_are_derived_from_openapi() {
    let doc = openapi();
    for spec in specs() {
        let operation = operation(&doc, spec.operation_id);
        let expected = sorted_required(expected_schema(&doc, operation.value));
        let actual = sorted_required((spec.input_schema)());
        assert!(
            expected == actual,
            "{}: input schema does not match the OpenAPI derivation\n expected:\n{}\n actual:\n{}",
            spec.name,
            pretty(&expected),
            pretty(&actual)
        );
    }
}

#[test]
fn every_schema_is_fully_validated() {
    for spec in specs() {
        let schema = schema_of(spec);
        if let Err(reason) = quoro_mcp::args::supports(&schema) {
            panic!(
                "{}: the args validator does not fully support the schema: {reason}",
                spec.name
            );
        }
    }
}

#[test]
fn mutating_tools_require_a_justification_and_accept_an_incident() {
    let doc = openapi();
    for spec in specs() {
        let schema = schema_of(spec);
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{}: no properties", spec.name));
        let required = required_names(&schema);
        let operation = operation(&doc, spec.operation_id);
        if spec.mutating {
            assert!(
                properties.contains_key("justification"),
                "{}: a mutating tool has no justification property",
                spec.name
            );
            assert!(
                required.iter().any(|name| name == "justification"),
                "{}: a mutating tool does not require justification",
                spec.name
            );
            assert!(
                properties.contains_key("incident_id"),
                "{}: a mutating tool has no incident_id property",
                spec.name
            );
            // `resolve_incident` names the incident in its path, so it is
            // required there. Everywhere else `incident_id` is the
            // `Quoro-Incident` header context and stays optional.
            assert_eq!(
                required.iter().any(|name| name == "incident_id"),
                parameter(&doc, operation.value, "incident_id") == Some(true),
                "{}: incident_id is required exactly when the operation declares it so",
                spec.name
            );
        } else {
            assert!(
                !properties.contains_key("justification"),
                "{}: a read tool must not take justification",
                spec.name
            );
            assert!(
                !required.iter().any(|name| name == "justification"),
                "{}: a read tool must not require justification",
                spec.name
            );
            // `list_audit` filters by `incident_id` and `get_incident` names
            // the incident in its path, so a read tool may carry the property —
            // but only when the operation declares it as a path or query
            // parameter, never as the `Quoro-Incident` header context.
            assert_eq!(
                properties.contains_key("incident_id"),
                parameter(&doc, operation.value, "incident_id").is_some(),
                "{}: a read tool takes incident_id only as a path or query parameter",
                spec.name
            );
        }
    }

    let tools = list();
    assert_eq!(tools.len(), specs().len(), "list() and specs() disagree");
    let mut destructive = Vec::new();
    for spec in specs() {
        let tool = tools
            .iter()
            .find(|tool| tool.name.as_ref() == spec.name)
            .unwrap_or_else(|| panic!("{} is missing from list()", spec.name));
        let annotations = tool
            .annotations
            .as_ref()
            .unwrap_or_else(|| panic!("{}: no annotations", spec.name));
        assert_eq!(
            annotations.read_only_hint,
            Some(!spec.mutating),
            "{}: read_only_hint disagrees",
            spec.name
        );
        assert_eq!(
            annotations.destructive_hint,
            Some(spec.destructive),
            "{}: destructive_hint disagrees",
            spec.name
        );
        if annotations.destructive_hint == Some(true) {
            destructive.push(tool.name.as_ref());
        }
    }
    assert_eq!(
        destructive,
        vec!["delete_app"],
        "delete_app is the only destructive tool"
    );
}

#[test]
fn no_tool_named_after_approval_decide() {
    let tools = list();
    let listed: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    let names: Vec<&str> = specs().iter().map(|spec| spec.name).collect();

    for name in names.iter().chain(listed.iter()) {
        let lower = name.to_ascii_lowercase();
        // `request_approval` merely files a request; deciding is what MCP must
        // never expose.
        for forbidden in ["approve", "deny", "decid"] {
            assert!(
                !lower.contains(forbidden),
                "{name} is named after an approval decision; approval:decide has no MCP tool \
                 (permission-model.md section 10)"
            );
        }
    }

    for spec in specs() {
        assert_ne!(
            spec.scope, "approval:decide",
            "{}: an agent cannot decide approvals through MCP",
            spec.name
        );
    }
}

#[test]
fn the_adapter_never_touches_sql_or_builds_audit_records() {
    // [dependencies], not [dev-dependencies]: only the tests may touch sqlite.
    let manifest = read("crates/mcp/Cargo.toml");
    let mut in_dependencies = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_dependencies = trimmed == "[dependencies]";
            continue;
        }
        if !in_dependencies || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        for forbidden in ["sqlx", "rusqlite", "libsqlite3-sys"] {
            assert!(
                !line.contains(forbidden),
                "crates/mcp/Cargo.toml: [dependencies] names {forbidden}: {line}"
            );
        }
    }

    const FORBIDDEN: [&str; 18] = [
        "sqlx",
        "rusqlite",
        "SELECT ",
        "INSERT ",
        "UPDATE ",
        "DELETE FROM",
        ".pool()",
        "begin_write",
        "audit::append",
        "audit::Record",
        "Record {",
        ".record(",
        "append_ok",
        "audit_refusal",
        "audit_error",
        "audit_replay",
        "finish_read",
        "authorize",
    ];

    let mut files = Vec::new();
    collect_rust_files(&repo_file("crates/mcp/src"), &mut files);
    assert!(!files.is_empty(), "crates/mcp/src has no Rust sources");

    for file in files {
        let text = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
        for (index, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            // Comments explain the rule; they are not the rule.
            if trimmed.starts_with("//") {
                continue;
            }
            for forbidden in FORBIDDEN {
                if contains_token(line, forbidden) {
                    panic!(
                        "{}:{}: the adapter must never reach SQL, authorization or an audit \
                         record, but the line contains {forbidden:?}: {line}",
                        file.display(),
                        index + 1
                    );
                }
            }
        }
    }
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| {
                panic!("cannot read an entry of {}: {error}", directory.display())
            })
            .path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

/// `authorize` is matched as a whole word so the server's human-readable
/// instructions ("Calls are authorized against your grants") do not trip the
/// static check; the other needles are substrings.
fn contains_token(line: &str, needle: &str) -> bool {
    if needle != "authorize" {
        return line.contains(needle);
    }
    line.match_indices(needle).any(|(index, _)| {
        let before = line[..index].chars().next_back();
        let after = line[index + needle.len()..].chars().next();
        let is_word = |character: Option<char>| {
            character.is_some_and(|character| character.is_alphanumeric() || character == '_')
        };
        !is_word(before) && !is_word(after)
    })
}
