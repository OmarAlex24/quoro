// SPDX-License-Identifier: AGPL-3.0-only
//! `prepare_layout`: an OCI image layout built on a node (engine-trait.md
//! §3.17; ADR 0117, ADR 0118).
//!
//! A builder microVM writes an OCI image layout onto its work disk. The node
//! hands that directory, or a tarball of it, to [`ImageStore::prepare_layout`],
//! which materializes the blobs in a scratch directory, verifies every digest,
//! resolves the manifest the index names, and then reuses the same
//! layer-to-ext4 pipeline `prepare_image` uses (§3.4).
//!
//! The layout is hostile input: it was written by an untrusted guest. Only
//! regular files named `oci-layout`, `index.json` or `blobs/sha256/<hex>` are
//! accepted from a tarball, every blob is hashed before it is used, the
//! extracted size is bounded, and the engine never follows a symbolic link out
//! of the layout directory.

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader, Read, Write as _};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::image::cache;
use crate::image::layer::compression_for;
use crate::image::oci::{
    DOCKER_LIST, DOCKER_MANIFEST, Kind, OCI_INDEX, OCI_MANIFEST, classify, parse_image_config,
    parse_index, parse_manifest,
};
use crate::image::registry::{self, PulledLayer};
use crate::image::{ImageStore, ImageStoreConfig, hex};
use crate::{EngineError, ImageConfig, ImageDigest, ImageRef, OciLayout, PreparedImage};

/// The most a layout may cost on disk: the blobs the engine extracts, or the
/// tar entries it accepts. Never more than the layout file's own size.
const MAX_LAYOUT_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// The most `index.json`, a manifest or an image config may be.
const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;

/// The buffer one blob is copied through.
const COPY_BUFFER: usize = 64 * 1024;

impl ImageStore {
    /// Converts a local OCI image layout into a prepared rootfs (§3.17).
    ///
    /// Idempotent by manifest digest: a digest already in the cache is served
    /// without opening the layout, and concurrent calls for the same digest are
    /// serialized like [`ImageStore::prepare`]. The layout itself is never
    /// removed.
    ///
    /// # Errors
    ///
    /// `Image` for a malformed digest or layout, an entry type or path the
    /// engine does not accept, a blob whose content does not match its name, a
    /// manifest the index does not name, or a media type the pipeline does not
    /// support; `Io` for local failures.
    pub async fn prepare_layout(&self, layout: &OciLayout) -> Result<PreparedImage, EngineError> {
        let hex = registry::digest_hex(&layout.manifest_digest.0)?;
        if let Some(cached) = self.cached(&layout.manifest_digest)? {
            return Ok(cached);
        }
        let lock = self.lock_for(hex);
        let _guard = lock.lock().await;
        // Another caller may have finished while we waited for the lock.
        if let Some(cached) = self.cached(&layout.manifest_digest)? {
            return Ok(cached);
        }

        let tmp = cache::tmp_dir(&self.config.root, hex);
        if let Err(err) = convert_layout(&self.config, layout, &tmp).await {
            crate::image::remove_dir(&tmp).await;
            return Err(err);
        }
        if let Err(err) = cache::commit(&self.config.root, &tmp, hex) {
            crate::image::remove_dir(&tmp).await;
            return Err(err);
        }
        self.cached(&layout.manifest_digest)?.ok_or_else(|| {
            EngineError::Io(format!(
                "cache entry for {} disappeared after commit",
                layout.manifest_digest.0
            ))
        })
    }
}

/// Where the blobs of one layout come from while they are materialized.
enum Source {
    /// A tarball whose accepted entries were already written to `blobs`.
    Extracted,
    /// An OCI image-layout directory the blobs are copied from on demand.
    Directory(PathBuf),
}

/// Materializes the layout in `tmp` and turns it into the shared conversion
/// tail.
async fn convert_layout(
    config: &ImageStoreConfig,
    layout: &OciLayout,
    tmp: &Path,
) -> Result<(), EngineError> {
    let path = layout.path.clone();
    let requested = layout.manifest_digest.clone();
    let scratch = tmp.to_path_buf();
    let (image_config, layers) =
        tokio::task::spawn_blocking(move || materialize(&path, &requested, &scratch))
            .await
            .map_err(|err| EngineError::Io(format!("layout import task failed: {err}")))??;

    let image = ImageRef {
        reference: format!("oci-layout:{}", layout.path.display()),
        digest: layout.manifest_digest.clone(),
    };
    crate::image::finalize(config, &image, &image_config, layers, tmp).await
}

/// Reads the layout into `tmp/blobs` and resolves it to the image config and
/// its layers.
fn materialize(
    layout: &Path,
    requested: &ImageDigest,
    tmp: &Path,
) -> Result<(ImageConfig, Vec<PulledLayer>), EngineError> {
    let blobs = tmp.join("blobs");
    std::fs::create_dir_all(&blobs).map_err(|err| io_error(&blobs, &err))?;

    let metadata = match std::fs::metadata(layout) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(EngineError::Image(format!(
                "layout {}: {err}",
                layout.display()
            )));
        }
        Err(err) => return Err(io_error(layout, &err)),
    };
    let mut total = 0_u64;
    let source = if metadata.is_dir() {
        materialize_directory(layout, &blobs, &mut total)?;
        Source::Directory(layout.to_path_buf())
    } else if metadata.is_file() {
        extract_tar(layout, &blobs, metadata.len())?;
        Source::Extracted
    } else {
        return Err(EngineError::Image(format!(
            "layout {} is neither a directory nor a regular file",
            layout.display()
        )));
    };

    resolve(&source, requested, &blobs, &mut total)
}

/// Reads an OCI image-layout directory: `oci-layout` and a bounded
/// `index.json`.
fn materialize_directory(dir: &Path, blobs: &Path, total: &mut u64) -> Result<(), EngineError> {
    let marker = dir.join("oci-layout");
    let marker_meta = std::fs::symlink_metadata(&marker).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            EngineError::Image(format!("layout {} has no oci-layout file", dir.display()))
        } else {
            io_error(&marker, &err)
        }
    })?;
    if !marker_meta.file_type().is_file() {
        return Err(EngineError::Image(format!(
            "layout {}: oci-layout is not a regular file",
            dir.display()
        )));
    }

    let index = dir.join("index.json");
    let index_meta = std::fs::symlink_metadata(&index).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            EngineError::Image(format!("layout {} has no index.json file", dir.display()))
        } else {
            io_error(&index, &err)
        }
    })?;
    if !index_meta.file_type().is_file() {
        return Err(EngineError::Image(format!(
            "layout {}: index.json is not a regular file",
            dir.display()
        )));
    }
    if index_meta.len() > MAX_DOCUMENT_BYTES {
        return Err(EngineError::Image(format!(
            "layout {}: index.json is {} bytes, more than the {MAX_DOCUMENT_BYTES}-byte limit",
            dir.display(),
            index_meta.len()
        )));
    }

    let mut bytes = Vec::with_capacity(usize::try_from(index_meta.len()).unwrap_or(0));
    File::open(&index)
        .and_then(|file| file.take(MAX_DOCUMENT_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|err| io_error(&index, &err))?;
    *total = total.saturating_add(bytes.len() as u64);
    write_file(&blobs.join("index.json"), &bytes)
}

/// Accepts a tar of one OCI image layout into `blobs`.
///
/// Only regular files named `oci-layout`, `index.json` or
/// `blobs/sha256/<64 lowercase hex>`, and the directory entries `blobs` and
/// `blobs/sha256`, are accepted. Every other type or name, an absolute path, a
/// `..` component or a duplicate name is `Image`. The tar may be followed by
/// zero padding; the archive's end marker ends it.
fn extract_tar(path: &Path, blobs: &Path, file_len: u64) -> Result<(), EngineError> {
    let budget = file_len.min(MAX_LAYOUT_BYTES);
    let file = File::open(path).map_err(|err| io_error(path, &err))?;
    let mut archive = tar::Archive::new(BufReader::new(file));
    let entries = archive.entries().map_err(|err| tar_error(path, &err))?;

    let mut seen = HashSet::new();
    let mut total = 0_u64;
    for entry in entries {
        let mut entry = entry.map_err(|err| tar_error(path, &err))?;
        let entry_type = entry.header().entry_type();
        let raw = entry.path_bytes().into_owned();
        let name = normalize_entry(&raw, entry_type.is_dir())?;
        if !seen.insert(name.clone()) {
            return Err(EngineError::Image(format!(
                "layout tar has a duplicate entry {name:?}"
            )));
        }

        if entry_type.is_dir() {
            if !is_allowed_dir(&name) {
                return Err(EngineError::Image(format!(
                    "layout tar has an unexpected directory entry {name:?}"
                )));
            }
            continue;
        }
        if !entry_type.is_file() {
            return Err(EngineError::Image(format!(
                "layout tar entry {name:?} is not a regular file"
            )));
        }
        if !is_allowed_file(&name) {
            return Err(EngineError::Image(format!(
                "layout tar has an unexpected entry {name:?}"
            )));
        }

        let size = entry.header().size().map_err(|err| tar_error(path, &err))?;
        if (name == "index.json" || name == "oci-layout") && size > MAX_DOCUMENT_BYTES {
            return Err(EngineError::Image(format!(
                "layout tar entry {name:?} is {size} bytes, more than the {MAX_DOCUMENT_BYTES}-byte limit"
            )));
        }
        let remaining = budget.saturating_sub(total);
        if size > remaining {
            return Err(EngineError::Image(format!(
                "layout tar is larger than the {budget}-byte limit"
            )));
        }

        if name == "index.json" {
            let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
            entry
                .read_to_end(&mut bytes)
                .map_err(|err| tar_error(path, &err))?;
            write_file(&blobs.join("index.json"), &bytes)?;
        } else if name == "oci-layout" {
            copy_to_sink(&mut entry).map_err(|err| tar_error(path, &err))?;
        } else {
            let blob_hex = name
                .strip_prefix("blobs/sha256/")
                .expect("is_allowed_file checked the prefix");
            let dest = blobs.join(blob_hex);
            let (got, digest) = match copy_hashed(&mut entry, &dest, budget.saturating_sub(total)) {
                Ok(copied) => copied,
                Err(err) => {
                    let _ = std::fs::remove_file(&dest);
                    return Err(tar_error(path, &err));
                }
            };
            if got != size {
                let _ = std::fs::remove_file(&dest);
                return Err(EngineError::Image(format!(
                    "layout tar entry {name:?} is {got} bytes, its header says {size}"
                )));
            }
            if digest != format!("sha256:{blob_hex}") {
                let _ = std::fs::remove_file(&dest);
                return Err(EngineError::Image(format!(
                    "blob {blob_hex} does not match the digest in its name"
                )));
            }
        }
        total = total.saturating_add(size);
    }

    if !blobs.join("index.json").is_file() {
        return Err(EngineError::Image(
            "layout tar has no index.json entry".to_string(),
        ));
    }
    Ok(())
}

/// Resolves the manifest `requested` names and parses its config and layers.
fn resolve(
    source: &Source,
    requested: &ImageDigest,
    blobs: &Path,
    total: &mut u64,
) -> Result<(ImageConfig, Vec<PulledLayer>), EngineError> {
    let requested_hex = registry::digest_hex(&requested.0)?;
    let index = read_index(blobs)?;

    let entry = match find_entry(&index, &requested.0) {
        Some(entry) => entry.clone(),
        None => {
            // One level of nesting only: BuildKit may write an index of one
            // platform index. Nothing deeper is followed.
            if index.manifests.len() != 1 {
                return Err(EngineError::Image(format!(
                    "layout index does not name manifest {}",
                    requested.0
                )));
            }
            let outer = &index.manifests[0];
            if !is_index_media(&outer.media_type) {
                return Err(EngineError::Image(format!(
                    "layout index does not name manifest {}",
                    requested.0
                )));
            }
            let outer_hex = registry::digest_hex(&outer.digest)?;
            let outer_path = blob_path(source, outer_hex, blobs, total)?;
            let outer_bytes = read_bounded(&outer_path, MAX_DOCUMENT_BYTES)?;
            if registry::sha256_digest(&outer_bytes) != outer.digest {
                return Err(EngineError::Image(format!(
                    "nested index {} does not match its digest",
                    outer.digest
                )));
            }
            if classify(&outer_bytes)? != Kind::Index {
                return Err(EngineError::Image(format!(
                    "layout entry {} is not an image index",
                    outer.digest
                )));
            }
            let nested = parse_index(&outer_bytes)?;
            find_entry(&nested, &requested.0).cloned().ok_or_else(|| {
                EngineError::Image(format!(
                    "layout index does not name manifest {}",
                    requested.0
                ))
            })?
        }
    };

    if !is_manifest_media(&entry.media_type) {
        return Err(EngineError::Image(format!(
            "layout index names {} with {}, not an image manifest",
            entry.digest, entry.media_type
        )));
    }

    let manifest_path = blob_path(source, requested_hex, blobs, total)?;
    let manifest_bytes = read_bounded(&manifest_path, MAX_DOCUMENT_BYTES)?;
    let got = registry::sha256_digest(&manifest_bytes);
    if got != requested.0 {
        return Err(EngineError::Image(format!(
            "manifest digest mismatch: expected {}, got {got}",
            requested.0
        )));
    }
    let manifest = parse_manifest(&manifest_bytes)?;

    let config_hex = registry::digest_hex(&manifest.config.digest)?;
    let config_path = blob_path(source, config_hex, blobs, total)?;
    let config_bytes = read_bounded(&config_path, MAX_DOCUMENT_BYTES)?;
    let config_doc = parse_image_config(&config_bytes)?;
    if config_doc.diff_ids.len() != manifest.layers.len() {
        return Err(EngineError::Image(format!(
            "image config has {} diff_ids for {} layers",
            config_doc.diff_ids.len(),
            manifest.layers.len()
        )));
    }

    let mut layers = Vec::with_capacity(manifest.layers.len());
    for (index, layer) in manifest.layers.iter().enumerate() {
        let layer_hex = registry::digest_hex(&layer.digest)?;
        let compression = compression_for(&layer.media_type)?;
        let path = blob_path(source, layer_hex, blobs, total)?;
        layers.push(PulledLayer {
            path,
            compression,
            diff_id: config_doc.diff_ids[index].clone(),
        });
    }

    Ok((config_doc.config, layers))
}

/// Reads and parses `index.json` from the materialized layout.
fn read_index(blobs: &Path) -> Result<crate::image::oci::Index, EngineError> {
    let path = blobs.join("index.json");
    let bytes = read_bounded(&path, MAX_DOCUMENT_BYTES)?;
    parse_index(&bytes)
}

/// The index entry that names `digest`, if any.
fn find_entry<'a>(
    index: &'a crate::image::oci::Index,
    digest: &str,
) -> Option<&'a crate::image::oci::IndexEntry> {
    index.manifests.iter().find(|entry| entry.digest == digest)
}

/// The scratch path of one blob, copying it out of a layout directory when it
/// is not there yet.
fn blob_path(
    source: &Source,
    blob_hex: &str,
    blobs: &Path,
    total: &mut u64,
) -> Result<PathBuf, EngineError> {
    let dest = blobs.join(blob_hex);
    match source {
        Source::Extracted => match std::fs::symlink_metadata(&dest) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(dest),
            _ => Err(EngineError::Image(format!(
                "layout is missing blob sha256:{blob_hex}"
            ))),
        },
        Source::Directory(dir) => {
            if let Ok(metadata) = std::fs::symlink_metadata(&dest)
                && metadata.file_type().is_file()
            {
                return Ok(dest);
            }
            let source_path = dir.join("blobs").join("sha256").join(blob_hex);
            let metadata = std::fs::symlink_metadata(&source_path).map_err(|err| {
                if err.kind() == io::ErrorKind::NotFound {
                    EngineError::Image(format!("layout is missing blob sha256:{blob_hex}"))
                } else {
                    io_error(&source_path, &err)
                }
            })?;
            if !metadata.file_type().is_file() {
                return Err(EngineError::Image(format!(
                    "layout blob sha256:{blob_hex} is not a regular file"
                )));
            }
            let remaining = MAX_LAYOUT_BYTES.saturating_sub(*total);
            if metadata.len() > remaining {
                return Err(EngineError::Image(format!(
                    "layout is larger than the {MAX_LAYOUT_BYTES}-byte limit"
                )));
            }
            let mut file = File::open(&source_path).map_err(|err| io_error(&source_path, &err))?;
            let (got, digest) = match copy_hashed(&mut file, &dest, remaining) {
                Ok(copied) => copied,
                Err(err) => {
                    let _ = std::fs::remove_file(&dest);
                    return Err(io_error(&source_path, &err));
                }
            };
            if digest != format!("sha256:{blob_hex}") {
                let _ = std::fs::remove_file(&dest);
                return Err(EngineError::Image(format!(
                    "blob {blob_hex} does not match the digest in its name"
                )));
            }
            *total = total.saturating_add(got);
            Ok(dest)
        }
    }
}

/// Reads at most `limit` bytes from `path`.
fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, EngineError> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(limit + 1).read_to_end(&mut bytes))
        .map_err(|err| io_error(path, &err))?;
    if bytes.len() as u64 > limit {
        return Err(EngineError::Image(format!(
            "{} is larger than the {limit}-byte limit",
            path.display()
        )));
    }
    Ok(bytes)
}

/// Copies a reader into a new file, hashing as it writes, and refuses to write
/// more than `limit` bytes.
///
/// Returns the byte count and the `sha256:<hex>` digest of what was written.
fn copy_hashed(reader: &mut impl Read, dest: &Path, limit: u64) -> io::Result<(u64, String)> {
    let mut file = File::create(dest)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; COPY_BUFFER];
    let mut written = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if written + read as u64 > limit {
            let _ = std::fs::remove_file(dest);
            return Err(io::Error::other(format!(
                "entry is larger than the {limit}-byte limit"
            )));
        }
        file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        written += read as u64;
    }
    file.flush()?;
    Ok((written, format!("sha256:{}", hex(&hasher.finalize()))))
}

/// Copies a reader to `/dev/null`, for entries whose content is not kept.
fn copy_to_sink(reader: &mut impl Read) -> io::Result<()> {
    io::copy(reader, &mut io::sink()).map(|_| ())
}

/// Writes `bytes` to `path`, creating the parent directory.
fn write_file(path: &Path, bytes: &[u8]) -> Result<(), EngineError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| io_error(parent, &err))?;
    }
    std::fs::write(path, bytes).map_err(|err| io_error(path, &err))
}

/// Normalizes one tar path and refuses anything that could escape the layout.
fn normalize_entry(raw: &[u8], is_dir: bool) -> Result<String, EngineError> {
    let invalid = |reason: &str| {
        EngineError::Image(format!(
            "invalid layout tar entry {:?}: {reason}",
            String::from_utf8_lossy(raw)
        ))
    };
    if raw.is_empty() {
        return Err(invalid("the path is empty"));
    }
    if raw[0] == b'/' {
        return Err(invalid("the path is absolute"));
    }
    let text = std::str::from_utf8(raw).map_err(|_| invalid("the path is not UTF-8"))?;
    let text = if is_dir {
        text.trim_end_matches('/')
    } else {
        text
    };
    // A leading `./` is what `tar -C dir .` writes; it is harmless.
    let text = text.strip_prefix("./").unwrap_or(text);
    for component in text.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(invalid("the path has an empty, `.` or `..` component"));
        }
    }
    Ok(text.to_string())
}

/// True for the files an OCI image layout may contain.
fn is_allowed_file(name: &str) -> bool {
    name == "oci-layout"
        || name == "index.json"
        || name
            .strip_prefix("blobs/sha256/")
            .is_some_and(is_lower_hex_64)
}

/// True for the directories an OCI image layout may contain.
fn is_allowed_dir(name: &str) -> bool {
    name == "blobs" || name == "blobs/sha256"
}

/// True for 64 lowercase hexadecimal digits.
fn is_lower_hex_64(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// True for a media type that names a single-platform image manifest.
fn is_manifest_media(media_type: &str) -> bool {
    media_type == OCI_MANIFEST || media_type == DOCKER_MANIFEST
}

/// True for a media type that names an index.
fn is_index_media(media_type: &str) -> bool {
    media_type == OCI_INDEX || media_type == DOCKER_LIST
}

fn io_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

fn tar_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Image(format!("layout tar {}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;

    use super::*;
    use crate::image::cache::tests_support::temp_dir;
    use crate::image::layer::OCI_LAYER_TAR_GZIP;
    use crate::image::oci::host_arch;
    use crate::image::registry::sha256_digest;

    /// The binaries `mkfs.ext4` may live at on the test host.
    fn mkfs() -> Option<PathBuf> {
        ["/usr/sbin/mkfs.ext4", "/sbin/mkfs.ext4"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
    }

    /// One image layout, as a build would have written it.
    struct Fixture {
        oci_layout: Vec<u8>,
        index: Vec<u8>,
        manifest: Vec<u8>,
        config: Vec<u8>,
        layer: Vec<u8>,
        manifest_digest: String,
        layer_digest: String,
        config_digest: String,
    }

    impl Fixture {
        /// Every file of the layout, by its path inside it.
        fn files(&self) -> Vec<(String, Vec<u8>)> {
            vec![
                ("oci-layout".to_string(), self.oci_layout.clone()),
                ("index.json".to_string(), self.index.clone()),
                (
                    format!(
                        "blobs/sha256/{}",
                        self.manifest_digest.trim_start_matches("sha256:")
                    ),
                    self.manifest.clone(),
                ),
                (
                    format!(
                        "blobs/sha256/{}",
                        self.config_digest.trim_start_matches("sha256:")
                    ),
                    self.config.clone(),
                ),
                (
                    format!(
                        "blobs/sha256/{}",
                        self.layer_digest.trim_start_matches("sha256:")
                    ),
                    self.layer.clone(),
                ),
            ]
        }

        fn request(&self) -> OciLayout {
            OciLayout {
                path: PathBuf::new(),
                manifest_digest: ImageDigest(self.manifest_digest.clone()),
            }
        }
    }

    fn fixture() -> Fixture {
        let mut layer_tar = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut layer_tar);
            let content = b"hello from a layout";
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            header.set_cksum();
            builder
                .append_data(&mut header, "etc/hello", &content[..])
                .expect("layer entry");
            builder.finish().expect("layer tar");
        }
        let diff_id = sha256_digest(&layer_tar);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&layer_tar).expect("gzip");
        let layer = encoder.finish().expect("gzip");

        let config = serde_json::to_vec(&json!({
            "architecture": host_arch(),
            "os": "linux",
            "config": {
                "Entrypoint": ["/bin/hello"],
                "Cmd": ["--say", "hi"],
                "Env": ["PATH=/usr/bin"],
                "WorkingDir": "/srv"
            },
            "rootfs": {"type": "layers", "diff_ids": [diff_id]}
        }))
        .expect("config json");

        let layer_digest = sha256_digest(&layer);
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": sha256_digest(&config),
                "size": config.len()
            },
            "layers": [{
                "mediaType": OCI_LAYER_TAR_GZIP,
                "digest": layer_digest,
                "size": layer.len()
            }]
        }))
        .expect("manifest json");

        let manifest_digest = sha256_digest(&manifest);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [{
                "mediaType": OCI_MANIFEST,
                "digest": manifest_digest,
                "size": manifest.len(),
                "platform": {"architecture": host_arch(), "os": "linux"}
            }]
        }))
        .expect("index json");

        Fixture {
            oci_layout: br#"{"imageLayoutVersion":"1.0.0"}"#.to_vec(),
            index,
            manifest,
            config_digest: sha256_digest(&config),
            config,
            layer_digest,
            layer,
            manifest_digest,
        }
    }

    fn store(root: &Path) -> ImageStore {
        ImageStore::new(ImageStoreConfig {
            root: root.to_path_buf(),
            mkfs_ext4: mkfs().expect("mkfs.ext4"),
            preserve_ownership: false,
        })
        .expect("store")
    }

    fn write_dir(fixture: &Fixture, dir: &Path) {
        std::fs::create_dir_all(dir).expect("layout dir");
        for (name, bytes) in fixture.files() {
            let path = dir.join(name);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("parent dir");
            std::fs::write(&path, bytes).expect("layout file");
        }
    }

    fn write_tar(files: &[(String, Vec<u8>)], path: &Path) {
        let mut builder = tar_builder(path);
        for (name, bytes) in files {
            append_file(&mut builder, name, bytes);
        }
        builder.finish().expect("tar");
    }

    /// Opens a tar builder that callers may append special entries to.
    fn tar_builder(path: &Path) -> tar::Builder<File> {
        tar::Builder::new(File::create(path).expect("tar file"))
    }

    /// Appends one regular file entry.
    fn append_file(builder: &mut tar::Builder<File>, name: &str, bytes: &[u8]) {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        builder
            .append_data(&mut header, name, bytes)
            .expect("tar entry");
    }

    async fn prepared(fixture: &Fixture, layout: OciLayout) -> PreparedImage {
        let root = temp_dir("layout-prepare");
        let store = store(&root);
        let image = store.prepare_layout(&layout).await.expect("prepare layout");
        assert_eq!(image.digest, ImageDigest(fixture.manifest_digest.clone()));
        assert_eq!(image.config.entrypoint, vec!["/bin/hello"]);
        assert_eq!(image.config.cmd, vec!["--say", "hi"]);
        assert_eq!(
            image.config.env,
            vec![("PATH".to_string(), "/usr/bin".to_string())]
        );
        assert_eq!(image.config.workdir.as_deref(), Some("/srv"));
        assert!(image.size_bytes > 0);
        assert!(image.rootfs_path.exists());
        std::fs::remove_dir_all(&root).expect("cleanup");
        image
    }

    #[tokio::test]
    async fn prepares_a_directory_layout() {
        if mkfs().is_none() {
            eprintln!("skipping: no mkfs.ext4 on this host");
            return;
        }
        let fixture = fixture();
        let root = temp_dir("layout-dir");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        let mut request = fixture.request();
        request.path = dir;
        prepared(&fixture, request).await;
    }

    #[tokio::test]
    async fn prepares_a_tar_layout() {
        if mkfs().is_none() {
            eprintln!("skipping: no mkfs.ext4 on this host");
            return;
        }
        let fixture = fixture();
        let root = temp_dir("layout-tar");
        let tar_path = root.join("layout.tar");
        write_tar(&fixture.files(), &tar_path);
        let mut request = fixture.request();
        request.path = tar_path;
        prepared(&fixture, request).await;
    }

    #[tokio::test]
    async fn a_tar_may_be_followed_by_zero_padding() {
        if mkfs().is_none() {
            eprintln!("skipping: no mkfs.ext4 on this host");
            return;
        }
        let fixture = fixture();
        let root = temp_dir("layout-zero");
        let tar_path = root.join("layout.tar");
        write_tar(&fixture.files(), &tar_path);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&tar_path)
            .expect("open tar");
        file.write_all(&vec![0_u8; 1024 * 1024]).expect("padding");
        drop(file);
        let mut request = fixture.request();
        request.path = tar_path;
        prepared(&fixture, request).await;
    }

    #[tokio::test]
    async fn a_cached_digest_is_served_without_the_layout() {
        if mkfs().is_none() {
            eprintln!("skipping: no mkfs.ext4 on this host");
            return;
        }
        let fixture = fixture();
        let root = temp_dir("layout-cached");
        let tar_path = root.join("layout.tar");
        write_tar(&fixture.files(), &tar_path);
        let store = store(&root);
        let mut request = fixture.request();
        request.path = tar_path.clone();
        let first = store.prepare_layout(&request).await.expect("first prepare");
        std::fs::remove_file(&tar_path).expect("remove the layout file");
        let second = store
            .prepare_layout(&request)
            .await
            .expect("cached prepare");
        assert_eq!(first, second);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_blob_that_does_not_match_its_name_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-bad-blob");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        let layer = dir.join(format!(
            "blobs/sha256/{}",
            fixture.layer_digest.trim_start_matches("sha256:")
        ));
        std::fs::write(&layer, b"not the layer").expect("corrupt layer");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = dir;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_directory_blob_symlink_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-dir-symlink");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        let outside = root.join("outside");
        std::fs::write(&outside, &fixture.layer).expect("outside file");
        let layer = dir.join(format!(
            "blobs/sha256/{}",
            fixture.layer_digest.trim_start_matches("sha256:")
        ));
        std::fs::remove_file(&layer).expect("remove layer");
        std::os::unix::fs::symlink(&outside, &layer).expect("symlink layer");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = dir;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_missing_blob_is_an_image_error_and_not_a_fetch() {
        let fixture = fixture();
        let root = temp_dir("layout-missing-blob");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        std::fs::remove_file(dir.join(format!(
            "blobs/sha256/{}",
            fixture.layer_digest.trim_start_matches("sha256:")
        )))
        .expect("remove layer");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = dir;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_symlink_entry_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-symlink");
        let tar_path = root.join("layout.tar");
        let mut files = fixture.files();
        files.retain(|(name, _)| name != "oci-layout");
        let mut builder = tar_builder(&tar_path);
        for (name, bytes) in &files {
            append_file(&mut builder, name, bytes);
        }
        let mut header = tar::Header::new_gnu();
        header.set_path("oci-layout").expect("entry name");
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_link_name("index.json").expect("link name");
        header.set_cksum();
        builder.append(&header, io::empty()).expect("symlink entry");
        builder.finish().expect("tar");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = tar_path;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_parent_directory_entry_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-parent");
        let tar_path = root.join("layout.tar");
        let mut builder = tar_builder(&tar_path);
        for (name, bytes) in &fixture.files() {
            append_file(&mut builder, name, bytes);
        }
        let body = b"nope";
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        {
            let bytes = header.as_mut_bytes();
            bytes[..7].copy_from_slice(b"../esca");
        }
        header.set_cksum();
        builder.append(&header, &body[..]).expect("parent entry");
        builder.finish().expect("tar");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = tar_path;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_duplicate_entry_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-duplicate");
        let tar_path = root.join("layout.tar");
        let files = fixture.files();
        let mut with_duplicate = files.clone();
        with_duplicate.push(files[1].clone());
        write_tar(&with_duplicate, &tar_path);
        let store = store(&root);
        let mut request = fixture.request();
        request.path = tar_path;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn an_index_without_the_requested_digest_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-index");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        let index = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [{
                "mediaType": OCI_MANIFEST,
                "digest": fixture.config_digest,
                "size": 1,
                "platform": {"architecture": host_arch(), "os": "linux"}
            }]
        }))
        .expect("index");
        std::fs::write(dir.join("index.json"), index).expect("index");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = dir;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn a_manifest_that_does_not_match_the_requested_digest_is_refused() {
        let fixture = fixture();
        let root = temp_dir("layout-manifest");
        let dir = root.join("layout");
        write_dir(&fixture, &dir);
        let manifest_path = dir.join(format!(
            "blobs/sha256/{}",
            fixture.manifest_digest.trim_start_matches("sha256:")
        ));
        std::fs::write(&manifest_path, b"{}").expect("corrupt manifest");
        let store = store(&root);
        let mut request = fixture.request();
        request.path = dir;
        assert!(matches!(
            store.prepare_layout(&request).await,
            Err(EngineError::Image(_))
        ));
        std::fs::remove_dir_all(&root).expect("cleanup");
    }
}
