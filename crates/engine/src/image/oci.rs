// SPDX-License-Identifier: AGPL-3.0-only
//! OCI manifest, image index and image config JSON.
//!
//! The registry client returns raw bytes, so the engine parses the documents
//! itself: that is what lets it verify the manifest digest against
//! `ImageRef::digest`, pick the host's platform out of an index, and turn the
//! image config into the [`ImageConfig`] the rest of the engine uses
//! (engine-trait.md §3.4).

use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

use crate::{EngineError, ImageConfig};

/// OCI image manifest media type.
pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
/// Docker schema 2 image manifest media type.
pub const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// OCI image index media type.
pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
/// Docker manifest list media type.
pub const DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";

/// Everything the client announces it understands in the `Accept` header.
pub const ACCEPTED_MANIFEST_TYPES: &[&str] =
    &[OCI_MANIFEST, DOCKER_MANIFEST, OCI_INDEX, DOCKER_LIST];

/// A descriptor, used for the config blob, layers and index entries.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    /// The media type of the described content.
    #[serde(default)]
    pub media_type: String,
    /// `sha256:<hex>` of the described content.
    pub digest: String,
    /// Size of the described content in bytes; negative when absent.
    #[serde(default)]
    pub size: i64,
}

/// An image manifest: one platform's config and layers.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// The image config blob.
    pub config: Descriptor,
    /// The layer blobs, in application order.
    #[serde(default)]
    pub layers: Vec<Descriptor>,
}

/// The platform an index entry targets.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Platform {
    /// `amd64`, `arm64`, `unknown` for attestations, ...
    pub architecture: String,
    /// `linux`, `unknown` for attestations, ...
    pub os: String,
    /// `v8` on arm64, when present.
    #[serde(default)]
    pub variant: Option<String>,
}

/// One entry of an image index.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexEntry {
    /// Media type of the entry's manifest.
    #[serde(default)]
    pub media_type: String,
    /// `sha256:<hex>` of the entry's manifest bytes.
    pub digest: String,
    /// Size of the entry's manifest bytes; negative when absent.
    #[serde(default)]
    pub size: i64,
    /// The platform the entry targets.
    #[serde(default)]
    pub platform: Option<Platform>,
}

/// A multi-platform image index (or Docker manifest list).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    /// The manifests the index points at.
    #[serde(default)]
    pub manifests: Vec<IndexEntry>,
}

/// Which of the two manifest documents a JSON body is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A single-platform image manifest.
    Manifest,
    /// A multi-platform image index.
    Index,
}

/// The image config's fields the engine keeps, plus its layer diff_ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigDoc {
    /// What the image says about how to run it.
    pub config: ImageConfig,
    /// One uncompressed-layer digest per layer, in application order.
    pub diff_ids: Vec<String>,
}

impl Default for ConfigDoc {
    fn default() -> Self {
        Self {
            config: ImageConfig {
                entrypoint: Vec::new(),
                cmd: Vec::new(),
                env: Vec::new(),
                user: None,
                workdir: None,
                exposed_ports: Vec::new(),
            },
            diff_ids: Vec::new(),
        }
    }
}

/// The host's OCI architecture name (`x86_64` → `amd64`).
pub fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// Decides whether `bytes` is a manifest or an index.
///
/// # Errors
///
/// `Image` when the body is not a JSON object with either `manifests` or
/// `layers`.
pub fn classify(bytes: &[u8]) -> Result<Kind, EngineError> {
    let value = json(bytes, "manifest")?;
    let object = value
        .as_object()
        .ok_or_else(|| EngineError::Image("manifest is not a JSON object".into()))?;
    if object.contains_key("manifests") {
        Ok(Kind::Index)
    } else if object.contains_key("layers") {
        Ok(Kind::Manifest)
    } else {
        Err(EngineError::Image(
            "manifest has neither a `manifests` nor a `layers` field".into(),
        ))
    }
}

/// Parses an image manifest.
///
/// # Errors
///
/// `Image` when the document is malformed.
pub fn parse_manifest(bytes: &[u8]) -> Result<Manifest, EngineError> {
    serde_json::from_slice(bytes)
        .map_err(|err| EngineError::Image(format!("image manifest is malformed: {err}")))
}

/// Parses an image index.
///
/// # Errors
///
/// `Image` when the document is malformed.
pub fn parse_index(bytes: &[u8]) -> Result<Index, EngineError> {
    serde_json::from_slice(bytes)
        .map_err(|err| EngineError::Image(format!("image index is malformed: {err}")))
}

/// Picks the entry for this host out of an index.
///
/// Attestation manifests (`os: unknown`) never match. On arm64 a `v8` variant
/// is preferred over a variant-less entry.
///
/// # Errors
///
/// `Image("no linux/<arch> manifest in index …")` when nothing matches.
pub fn select_platform<'a>(
    index: &'a Index,
    index_digest: &str,
) -> Result<&'a IndexEntry, EngineError> {
    let arch = host_arch();
    let matches: Vec<&IndexEntry> = index
        .manifests
        .iter()
        .filter(|entry| {
            entry
                .platform
                .as_ref()
                .is_some_and(|platform| platform.os == "linux" && platform.architecture == arch)
        })
        .collect();
    if arch == "arm64"
        && let Some(v8) = matches.iter().copied().find(|entry| {
            entry
                .platform
                .as_ref()
                .and_then(|platform| platform.variant.as_deref())
                == Some("v8")
        })
    {
        return Ok(v8);
    }
    matches.first().copied().ok_or_else(|| {
        EngineError::Image(format!("no linux/{arch} manifest in index {index_digest}"))
    })
}

/// Maps an image config document to [`ImageConfig`] plus its `diff_ids`.
///
/// A missing `config` object yields empty fields; a missing `rootfs` yields no
/// diff_ids. Field types are checked so a malformed config names the field.
///
/// # Errors
///
/// `Image` when the document is not JSON or a field has the wrong type.
pub fn parse_image_config(bytes: &[u8]) -> Result<ConfigDoc, EngineError> {
    let value = json(bytes, "image config")?;

    // A single-platform manifest carries no platform of its own; the config
    // does. Refuse a foreign image here instead of failing at boot.
    let os = value.get("os").and_then(Value::as_str);
    let architecture = value.get("architecture").and_then(Value::as_str);
    if os.is_some_and(|os| os != "linux") || architecture.is_some_and(|arch| arch != host_arch()) {
        return Err(EngineError::Image(format!(
            "image is for {}/{}, this host runs linux/{}",
            os.unwrap_or("?"),
            architecture.unwrap_or("?"),
            host_arch()
        )));
    }

    let diff_ids = match value
        .get("rootfs")
        .and_then(|rootfs| rootfs.get("diff_ids"))
    {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_owned).ok_or_else(|| {
                    EngineError::Image(
                        "image config `rootfs.diff_ids` has a non-string entry".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => {
            return Err(EngineError::Image(
                "image config `rootfs.diff_ids` is not an array".into(),
            ));
        }
    };

    let empty = serde_json::Map::new();
    let config = match value.get("config") {
        None | Some(Value::Null) => &empty,
        Some(Value::Object(object)) => object,
        Some(_) => {
            return Err(EngineError::Image(
                "image config `config` is not an object".into(),
            ));
        }
    };

    let entrypoint = strings(config, "Entrypoint")?;
    let cmd = strings(config, "Cmd")?;
    let env = strings(config, "Env")?
        .into_iter()
        .map(|entry| match entry.split_once('=') {
            Some((name, value)) => (name.to_owned(), value.to_owned()),
            None => (entry, String::new()),
        })
        .collect();
    let user = string(config, "User")?.filter(|user| !user.is_empty());
    let workdir = string(config, "WorkingDir")?.filter(|dir| !dir.is_empty());
    let exposed_ports = exposed_ports(config)?;

    Ok(ConfigDoc {
        config: ImageConfig {
            entrypoint,
            cmd,
            env,
            user,
            workdir,
            exposed_ports,
        },
        diff_ids,
    })
}

fn json(bytes: &[u8], what: &str) -> Result<Value, EngineError> {
    serde_json::from_slice(bytes)
        .map_err(|err| EngineError::Image(format!("{what} is not valid JSON: {err}")))
}

fn strings(config: &serde_json::Map<String, Value>, key: &str) -> Result<Vec<String>, EngineError> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_owned).ok_or_else(|| {
                    EngineError::Image(format!("image config `{key}` is not an array of strings"))
                })
            })
            .collect(),
        Some(_) => Err(EngineError::Image(format!(
            "image config `{key}` is not an array"
        ))),
    }
}

fn string(
    config: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>, EngineError> {
    match config.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(EngineError::Image(format!(
            "image config `{key}` is not a string"
        ))),
    }
}

/// `ExposedPorts` keys such as `80/tcp` → `80`, sorted and deduplicated.
fn exposed_ports(config: &serde_json::Map<String, Value>) -> Result<Vec<u16>, EngineError> {
    let Some(ports) = config.get("ExposedPorts") else {
        return Ok(Vec::new());
    };
    let object = match ports {
        Value::Null => return Ok(Vec::new()),
        Value::Object(object) => object,
        _ => {
            return Err(EngineError::Image(
                "image config `ExposedPorts` is not an object".into(),
            ));
        }
    };
    let mut parsed = BTreeSet::new();
    for key in object.keys() {
        let port = key.split('/').next().unwrap_or(key).trim();
        if let Ok(port) = port.parse::<u16>() {
            parsed.insert(port);
        }
    }
    Ok(parsed.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:aaaa",
            "size": 1234
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:bbbb",
                "size": 10
            }
        ]
    }"#;

    const INDEX: &str = r#"{
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:attest",
                "size": 1,
                "platform": { "architecture": "unknown", "os": "unknown" }
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:amd64",
                "size": 2,
                "platform": { "architecture": "amd64", "os": "linux" }
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:arm64",
                "size": 3,
                "platform": { "architecture": "arm64", "os": "linux", "variant": "v8" }
            }
        ]
    }"#;

    #[test]
    fn classify_tells_manifest_from_index() {
        assert_eq!(
            classify(MANIFEST.as_bytes()).expect("manifest"),
            Kind::Manifest
        );
        assert_eq!(classify(INDEX.as_bytes()).expect("index"), Kind::Index);
        assert!(classify(b"{}").is_err());
        assert!(classify(b"not json").is_err());
    }

    #[test]
    fn manifest_parses_config_and_layers() {
        let manifest = parse_manifest(MANIFEST.as_bytes()).expect("parse");
        assert_eq!(manifest.config.digest, "sha256:aaaa");
        assert_eq!(manifest.config.size, 1234);
        assert_eq!(manifest.layers.len(), 1);
        assert_eq!(
            manifest.layers[0].media_type,
            "application/vnd.oci.image.layer.v1.tar+gzip"
        );
    }

    #[test]
    fn index_selects_the_host_platform() {
        let index = parse_index(INDEX.as_bytes()).expect("parse");
        let chosen = select_platform(&index, "sha256:index").expect("select");
        let expected = if host_arch() == "arm64" {
            "sha256:arm64"
        } else {
            "sha256:amd64"
        };
        assert_eq!(chosen.digest, expected);
    }

    #[test]
    fn index_without_host_platform_names_linux_and_arch() {
        let only_arm = r#"{"manifests":[{"digest":"sha256:a","size":1,
            "platform":{"architecture":"arm64","os":"linux"}}]}"#;
        let index = parse_index(only_arm.as_bytes()).expect("parse");
        let err = select_platform(&index, "sha256:idx").expect_err("no match");
        let message = err.to_string();
        assert!(message.contains("no linux/"), "{message}");
        assert!(
            message.contains("manifest in index sha256:idx"),
            "{message}"
        );
        if host_arch() == "amd64" {
            assert!(message.contains("no linux/amd64 manifest"), "{message}");
        }
    }

    #[test]
    fn config_maps_every_field() {
        let json = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Entrypoint": ["/docker-entrypoint.sh"],
                "Cmd": ["nginx", "-g", "daemon off;"],
                "Env": ["PATH=/usr/bin", "NO_EQUALS", "A=b=c"],
                "User": "",
                "WorkingDir": "/srv",
                "ExposedPorts": { "443/tcp": {}, "80/tcp": {}, "80/udp": {}, "nope": {} }
            },
            "rootfs": { "type": "layers", "diff_ids": ["sha256:one", "sha256:two"] }
        }"#;
        let doc = parse_image_config(json.as_bytes()).expect("parse");
        assert_eq!(doc.config.entrypoint, vec!["/docker-entrypoint.sh"]);
        assert_eq!(doc.config.cmd, vec!["nginx", "-g", "daemon off;"]);
        assert_eq!(
            doc.config.env,
            vec![
                ("PATH".to_owned(), "/usr/bin".to_owned()),
                ("NO_EQUALS".to_owned(), String::new()),
                ("A".to_owned(), "b=c".to_owned()),
            ]
        );
        assert_eq!(doc.config.user, None);
        assert_eq!(doc.config.workdir.as_deref(), Some("/srv"));
        assert_eq!(doc.config.exposed_ports, vec![80, 443]);
        assert_eq!(doc.diff_ids, vec!["sha256:one", "sha256:two"]);
    }

    #[test]
    fn config_without_config_object_is_empty() {
        let doc = parse_image_config(r#"{"rootfs":{"diff_ids":[]}}"#.as_bytes()).expect("parse");
        assert_eq!(doc, ConfigDoc::default());
    }

    #[test]
    fn config_rejects_wrong_types() {
        assert!(parse_image_config(br#"{"config":{"Cmd":"ls"}}"#).is_err());
        assert!(parse_image_config(br#"{"config":{"Env":[1]}}"#).is_err());
        assert!(parse_image_config(br#"{"rootfs":{"diff_ids":"x"}}"#).is_err());
        assert!(parse_image_config(b"nope").is_err());
        assert!(parse_image_config(br#"{"os":"windows","architecture":"amd64"}"#).is_err());
        assert!(parse_image_config(br#"{"os":"linux","architecture":"s390x"}"#).is_err());
    }

    #[test]
    fn null_cmd_and_entrypoint_are_empty() {
        let doc =
            parse_image_config(br#"{"config":{"Cmd":null,"Entrypoint":null}}"#).expect("parse");
        assert!(doc.config.cmd.is_empty());
        assert!(doc.config.entrypoint.is_empty());
    }
}
