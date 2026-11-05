// SPDX-License-Identifier: AGPL-3.0-only
//! The component list a build embeds.
//!
//! With `QUORO_SBOM_COMPONENTS` set to a JSON file, as the release build does,
//! that file's component list is read and written to `$OUT_DIR/components.json`.
//! Without it, a developer build and the tests embed the pinned artifacts only,
//! read from the three `pins.txt` files of the workspace.
//!
//! Either way the list is written sorted by kind, name, version and arch, and
//! with only the documented fields. The embedded text therefore changes only
//! when the list does, which is what makes the hash a finding's fingerprint can
//! use. A file this script cannot read fails the build with a message naming
//! it.

use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

/// The variable the release build sets to the SBOM's component list.
const COMPONENTS: &str = "QUORO_SBOM_COMPONENTS";

/// The optional fields a component may carry.
const OPTIONAL: [&str; 4] = ["license", "source", "arch", "sha256"];

fn main() {
    println!("cargo::rerun-if-env-changed={COMPONENTS}");
    let output = out_dir().join("components.json");
    let text = match std::env::var(COMPONENTS) {
        Ok(path) if !path.trim().is_empty() => from_release(Path::new(path.trim())),
        _ => from_pins(),
    };
    std::fs::write(&output, text)
        .unwrap_or_else(|error| fail(&output.display().to_string(), error));
}

/// The text of a release build's list, sorted for a stable hash.
fn from_release(path: &Path) -> String {
    println!("cargo::rerun-if-changed={}", path.display());
    let origin = path.display().to_string();
    let text = read(path);
    let file: Value = serde_json::from_str(&text)
        .unwrap_or_else(|error| fail(&origin, format!("not JSON: {error}")));
    let schema = file.get("schema").and_then(Value::as_u64).unwrap_or(1);
    let list = file
        .get("components")
        .and_then(Value::as_array)
        .unwrap_or_else(|| fail(&origin, "no \"components\" array"));
    let entries = list.iter().map(|entry| normalize(&origin, entry)).collect();
    render(schema, entries)
}

/// The text of a developer build's list: the pinned artifacts only.
fn from_pins() -> String {
    let root = manifest_dir();
    let mut entries = Vec::new();
    for relative in ["../engine/pins.txt", "../ingress/pins.txt"] {
        let path = root.join(relative);
        println!("cargo::rerun-if-changed={}", path.display());
        entries.extend(artifact_pins(&path));
    }
    let path = root.join("../node-agent/pins.txt");
    println!("cargo::rerun-if-changed={}", path.display());
    entries.extend(buildkit_pin(&path));
    render(1, entries)
}

/// One normalised component object: the required fields and the optional ones
/// the entry names, nothing else.
fn normalize(origin: &str, entry: &Value) -> Map<String, Value> {
    let object = entry
        .as_object()
        .unwrap_or_else(|| fail(origin, "a component is not an object"));
    let mut out = Map::new();
    for key in ["kind", "name", "version", "purl"] {
        let value = object
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| fail(origin, format!("a component has no {key:?}")));
        out.insert(key.to_owned(), Value::String(value.to_owned()));
    }
    for key in OPTIONAL {
        let Some(value) = object.get(key) else {
            continue;
        };
        // An absent optional field is absent, never null in the output.
        if value.is_null() {
            continue;
        }
        let text = value
            .as_str()
            .unwrap_or_else(|| fail(origin, format!("a component's {key:?} is not a string")));
        out.insert(key.to_owned(), Value::String(text.to_owned()));
    }
    out
}

/// The pins of the engine and the ingress: `name version arch file url sha256`.
fn artifact_pins(path: &Path) -> Vec<Map<String, Value>> {
    let origin = path.display().to_string();
    let mut out = Vec::new();
    for (index, line) in read(path).lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 6 {
            fail(
                &origin,
                format!(
                    "line {}: expected 6 fields (name version arch file url sha256), found {}",
                    index + 1,
                    fields.len()
                ),
            );
        }
        let mut entry = Map::new();
        entry.insert("kind".to_owned(), json!("pin"));
        entry.insert("name".to_owned(), json!(fields[0]));
        entry.insert("version".to_owned(), json!(fields[1]));
        entry.insert("arch".to_owned(), json!(fields[2]));
        entry.insert("sha256".to_owned(), json!(fields[5]));
        entry.insert(
            "purl".to_owned(),
            json!(format!("pkg:generic/{}@{}", fields[0], fields[1])),
        );
        out.push(entry);
    }
    out
}

/// The node agent's BuildKit pin: `buildkit-image version reference`. The
/// component is the image, named `buildkit`.
fn buildkit_pin(path: &Path) -> Vec<Map<String, Value>> {
    let origin = path.display().to_string();
    let mut out = Vec::new();
    for (index, line) in read(path).lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 3 {
            fail(
                &origin,
                format!(
                    "line {}: expected 3 fields (name version reference), found {}",
                    index + 1,
                    fields.len()
                ),
            );
        }
        if fields[0] != "buildkit-image" {
            fail(
                &origin,
                format!(
                    "line {}: unknown pin {:?}, expected \"buildkit-image\"",
                    index + 1,
                    fields[0]
                ),
            );
        }
        let mut entry = Map::new();
        entry.insert("kind".to_owned(), json!("pin"));
        entry.insert("name".to_owned(), json!("buildkit"));
        entry.insert("version".to_owned(), json!(fields[1]));
        entry.insert(
            "purl".to_owned(),
            json!(format!("pkg:generic/buildkit@{}", fields[1])),
        );
        out.push(entry);
    }
    out
}

/// The document, with the components sorted by kind, name, version and arch.
fn render(schema: u64, mut entries: Vec<Map<String, Value>>) -> String {
    entries.sort_by_key(sort_key);
    let document = json!({ "schema": schema, "components": entries });
    serde_json::to_string(&document)
        .unwrap_or_else(|error| fail("components.json", format!("cannot be written: {error}")))
}

/// The sort key of one component. The last two fields only break a tie so the
/// order never depends on the input's.
fn sort_key(entry: &Map<String, Value>) -> [String; 6] {
    ["kind", "name", "version", "arch", "purl", "sha256"].map(|key| {
        entry
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    })
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| fail(&path.display().to_string(), error))
}

fn manifest_dir() -> PathBuf {
    std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|error| fail("CARGO_MANIFEST_DIR", error))
}

fn out_dir() -> PathBuf {
    std::env::var("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|error| fail("OUT_DIR", error))
}

/// Build scripts report a bad input by panicking, which cargo shows with the
/// script's own output.
fn fail(origin: &str, error: impl std::fmt::Display) -> ! {
    panic!("{origin}: {error}");
}
