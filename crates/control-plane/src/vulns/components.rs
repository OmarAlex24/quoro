// SPDX-License-Identifier: AGPL-3.0-only
//! The list of components the running Quoro release is made of.
//!
//! `build.rs` writes `components.json` into `$OUT_DIR`: the release's SBOM
//! component list when `QUORO_SBOM_COMPONENTS` names one, the pinned artifacts
//! otherwise. [`embedded`] parses that text once, and [`embedded_hash`] is its
//! SHA-256, which is what a finding's fingerprint uses to tell whether the
//! release changed.
//!
//! The list is untrusted only in the sense that it arrives from the build: a
//! text this code cannot read logs and yields an empty list, never a panic, so
//! a broken embed stops the matcher instead of the primary.

use serde::Deserialize;
use std::sync::OnceLock;

/// The embedded text `build.rs` wrote.
const EMBEDDED: &str = include_str!(concat!(env!("OUT_DIR"), "/components.json"));

/// One component of the running release (packaging/README.md).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Component {
    /// `crate`, `npm` or `pin`.
    pub kind: String,
    /// The crate's, npm package's or pinned artifact's name.
    pub name: String,
    /// The version the release ships.
    pub version: String,
    /// The component's package URL.
    pub purl: String,
    /// The license the SBOM states, when it states one.
    #[serde(default)]
    pub license: Option<String>,
    /// Where the component came from (`crates.io`, `path`, `git`, …).
    #[serde(default)]
    pub source: Option<String>,
    /// The architecture of a pinned artifact.
    #[serde(default)]
    pub arch: Option<String>,
    /// The pinned artifact's digest, when the pin has one.
    #[serde(default)]
    pub sha256: Option<String>,
}

/// The document `build.rs` writes.
#[derive(Deserialize)]
struct Document {
    components: Vec<Component>,
}

/// Every component of the running release, parsed once.
#[must_use]
pub fn embedded() -> &'static [Component] {
    static COMPONENTS: OnceLock<Vec<Component>> = OnceLock::new();
    COMPONENTS.get_or_init(|| parse(EMBEDDED))
}

/// The SHA-256 of the embedded component text, as lowercase hex.
///
/// It changes only when the component list changes, so a pass can compare it
/// with the one it last reconciled.
#[must_use]
pub fn embedded_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| crate::store::sha256_hex(EMBEDDED.as_bytes()))
}

/// Reads the component list, or logs and yields nothing.
fn parse(text: &str) -> Vec<Component> {
    match serde_json::from_str::<Document>(text) {
        Ok(document) => document.components,
        Err(error) => {
            tracing::warn!(%error, "the embedded component list cannot be read");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    /// The pins a test build embeds, read from the engine's pins file.
    fn engine_pins() -> BTreeMap<String, String> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../engine/pins.txt");
        let text = std::fs::read_to_string(&path).expect("engine pins");
        let mut pins = BTreeMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(fields.len(), 6, "{line}");
            pins.insert(fields[0].to_owned(), fields[1].to_owned());
        }
        pins
    }

    #[test]
    fn a_test_build_embeds_the_pinned_artifacts() {
        let names: Vec<&str> = embedded()
            .iter()
            .map(|component| component.name.as_str())
            .collect();
        for name in [
            "guest-kernel",
            "cloud-hypervisor",
            "cloudflared",
            "buildkit",
        ] {
            assert!(names.contains(&name), "{name} among {names:?}");
        }
        let pins = engine_pins();
        let kernel = embedded()
            .iter()
            .find(|component| component.name == "guest-kernel")
            .expect("the guest kernel pin");
        assert_eq!(
            Some(&kernel.version),
            pins.get("guest-kernel"),
            "the embedded version is the pins file's, not a literal"
        );
        assert!(
            embedded()
                .iter()
                .all(|component| component.kind == "pin" && !component.purl.is_empty()),
            "a test build embeds the pins only"
        );
    }

    #[test]
    fn the_embedded_list_is_sorted_and_has_a_stable_hash() {
        let components = embedded();
        let mut sorted = components.to_vec();
        sorted.sort_by(|left, right| {
            (&left.kind, &left.name, &left.version, &left.arch).cmp(&(
                &right.kind,
                &right.name,
                &right.version,
                &right.arch,
            ))
        });
        assert_eq!(components, sorted.as_slice());

        let hash = embedded_hash();
        assert_eq!(hash.len(), 64);
        assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(hash, embedded_hash());
    }

    #[test]
    fn a_text_that_is_not_a_component_list_yields_nothing() {
        assert!(parse("not JSON").is_empty());
        assert!(parse("{}").is_empty());
        assert!(parse(r#"{"components": "nope"}"#).is_empty());
    }

    #[test]
    fn the_optional_fields_are_read_when_present() {
        let parsed = parse(
            r#"{"schema": 1, "components": [
                {"kind": "crate", "name": "tokio", "version": "1.47.1",
                 "purl": "pkg:cargo/tokio@1.47.1", "license": "MIT", "source": "crates.io",
                 "arch": null}
            ]}"#,
        );
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].license.as_deref(), Some("MIT"));
        assert_eq!(parsed[0].source.as_deref(), Some("crates.io"));
        assert_eq!(parsed[0].arch, None);
    }
}
