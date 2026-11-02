// SPDX-License-Identifier: AGPL-3.0-only
//! Pull an OCI image by digest and verify every byte against its descriptor.
//!
//! The registry client is used only as a transport: the engine parses the
//! documents itself and hashes every blob, the manifest and the decompressed
//! layer streams. Only anonymous authentication is implemented; private
//! registries are a later item, so a registry that asks for credentials
//! surfaces its own error message.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use oci_client::client::ClientConfig;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWrite;

use crate::image::layer::{Compression, compression_for};
use crate::image::oci::{
    ACCEPTED_MANIFEST_TYPES, ConfigDoc, Descriptor, Kind, Manifest, classify, parse_image_config,
    parse_index, parse_manifest, select_platform,
};
use crate::{EngineError, ImageConfig, ImageRef};

/// One layer, downloaded and ready to apply.
pub(crate) struct PulledLayer {
    /// Scratch file holding the compressed blob.
    pub path: PathBuf,
    /// How the blob is compressed.
    pub compression: Compression,
    /// The uncompressed digest from the image config.
    pub diff_id: String,
}

/// Everything a pull produced that the conversion needs.
pub(crate) struct PulledImage {
    /// The image config, mapped to what the engine keeps.
    pub config: ImageConfig,
    /// Layers in application order.
    pub layers: Vec<PulledLayer>,
}

/// Checks an `ImageDigest` and returns its 64 hex characters.
///
/// # Errors
///
/// `Image` when the value is not `sha256:` followed by 64 lowercase hex digits.
pub(crate) fn digest_hex(digest: &str) -> Result<&str, EngineError> {
    if let Some(hex) = digest.strip_prefix("sha256:")
        && hex.len() == 64
        && hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Ok(hex);
    }
    Err(EngineError::Image(format!(
        "invalid image digest {digest:?}: expected sha256: followed by 64 lowercase hex digits"
    )))
}

/// `sha256:<lowercase hex>` of `bytes`.
pub(crate) fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", crate::image::hex(&Sha256::digest(bytes)))
}

/// Resolves the reference to pull by digest.
///
/// # Errors
///
/// `Image` when the digest is malformed, the reference cannot be parsed, or the
/// reference carries a different digest than the one requested.
pub(crate) fn by_digest_reference(image: &ImageRef) -> Result<Reference, EngineError> {
    digest_hex(&image.digest.0)?;
    let reference: Reference = Reference::try_from(image.reference.as_str()).map_err(|err| {
        EngineError::Image(format!(
            "invalid image reference {:?}: {err}",
            image.reference
        ))
    })?;
    if let Some(carried) = reference.digest()
        && carried != image.digest.0.as_str()
    {
        return Err(EngineError::Image(format!(
            "reference {} carries digest {carried}, expected {}",
            image.reference, image.digest.0
        )));
    }
    Ok(Reference::with_digest(
        reference.registry().to_owned(),
        reference.repository().to_owned(),
        image.digest.0.clone(),
    ))
}

/// Pulls the manifest, config and every layer for `image` into `blobs`.
///
/// # Errors
///
/// `Image` for a digest or size mismatch, an unsupported platform or layer, or
/// a registry error; `Io` for local file failures.
pub(crate) async fn pull(image: &ImageRef, blobs: &Path) -> Result<PulledImage, EngineError> {
    let reference = by_digest_reference(image)?;
    let client = Client::new(ClientConfig::default());

    let manifest = fetch_manifest(&client, &reference, &image.digest.0).await?;

    let mut layers = Vec::with_capacity(manifest.layers.len());
    // Reject an unsupported layer before downloading gigabytes.
    for layer in &manifest.layers {
        compression_for(&layer.media_type)?;
    }

    let config_path = blobs.join("config.json");
    pull_blob(&client, &reference, &manifest.config, &config_path).await?;
    let config_bytes = tokio::fs::read(&config_path)
        .await
        .map_err(|err| io_error(&config_path, &err))?;
    let ConfigDoc { config, diff_ids } = parse_image_config(&config_bytes)?;
    if diff_ids.len() != manifest.layers.len() {
        return Err(EngineError::Image(format!(
            "image config has {} diff_ids for {} layers",
            diff_ids.len(),
            manifest.layers.len()
        )));
    }

    for (index, layer) in manifest.layers.iter().enumerate() {
        let path = blobs.join(format!("layer-{index:03}.tar"));
        pull_blob(&client, &reference, layer, &path).await?;
        layers.push(PulledLayer {
            path,
            compression: compression_for(&layer.media_type)?,
            diff_id: diff_ids[index].clone(),
        });
    }

    Ok(PulledImage { config, layers })
}

/// Fetches a single-platform manifest, resolving an index for this host.
async fn fetch_manifest(
    client: &Client,
    reference: &Reference,
    digest: &str,
) -> Result<Manifest, EngineError> {
    let (body, _) = client
        .pull_manifest_raw(reference, &RegistryAuth::Anonymous, ACCEPTED_MANIFEST_TYPES)
        .await
        .map_err(|err| EngineError::Image(format!("pulling manifest for {digest}: {err}")))?;
    verify_bytes(&body, digest)?;

    match classify(&body)? {
        Kind::Manifest => parse_manifest(&body),
        Kind::Index => {
            let index = parse_index(&body)?;
            let entry = select_platform(&index, digest)?;
            let platform_reference = reference.clone_with_digest(entry.digest.clone());
            let (platform_body, _) = client
                .pull_manifest_raw(
                    &platform_reference,
                    &RegistryAuth::Anonymous,
                    ACCEPTED_MANIFEST_TYPES,
                )
                .await
                .map_err(|err| {
                    EngineError::Image(format!(
                        "pulling manifest {} from index {digest}: {err}",
                        entry.digest
                    ))
                })?;
            verify_bytes(&platform_body, &entry.digest)?;
            if classify(&platform_body)? != Kind::Manifest {
                return Err(EngineError::Image(format!(
                    "index {digest} points at another index ({}, {})",
                    entry.digest, entry.media_type
                )));
            }
            if entry.size >= 0 && platform_body.len() as i64 != entry.size {
                return Err(EngineError::Image(format!(
                    "manifest size mismatch for {}: expected {} bytes, got {}",
                    entry.digest,
                    entry.size,
                    platform_body.len()
                )));
            }
            parse_manifest(&platform_body)
        }
    }
}

/// Checks `bytes` against a sha256 digest.
fn verify_bytes(bytes: &[u8], expected: &str) -> Result<(), EngineError> {
    let got = sha256_digest(bytes);
    if got != expected {
        return Err(EngineError::Image(format!(
            "manifest digest mismatch: expected {expected}, got {got}"
        )));
    }
    Ok(())
}

/// Downloads one blob into `dest` and verifies its size and digest.
async fn pull_blob(
    client: &Client,
    reference: &Reference,
    descriptor: &Descriptor,
    dest: &Path,
) -> Result<u64, EngineError> {
    digest_hex(&descriptor.digest).map_err(|_| {
        EngineError::Image(format!(
            "blob {} has an unsupported descriptor digest; only sha256 is supported",
            descriptor.digest
        ))
    })?;
    let limit = u64::try_from(descriptor.size).ok();
    let mut writer = HashingWriter::create(dest, limit).await?;
    client
        .pull_blob(reference, descriptor.digest.as_str(), &mut writer)
        .await
        .map_err(|err| EngineError::Image(format!("pulling blob {}: {err}", descriptor.digest)))?;
    let (digest, bytes) = writer.finish().await?;

    if digest != descriptor.digest {
        return Err(EngineError::Image(format!(
            "blob digest mismatch: expected {}, got {digest}",
            descriptor.digest
        )));
    }
    if descriptor.size >= 0 && bytes != descriptor.size as u64 {
        return Err(EngineError::Image(format!(
            "blob size mismatch for {}: expected {} bytes, got {bytes}",
            descriptor.digest, descriptor.size
        )));
    }
    Ok(bytes)
}

/// An async file writer that hashes and counts what it writes, and refuses to
/// write past the descriptor's size so a registry cannot fill the disk.
struct HashingWriter {
    file: tokio::fs::File,
    hasher: Sha256,
    bytes: u64,
    limit: Option<u64>,
}

impl HashingWriter {
    async fn create(path: &Path, limit: Option<u64>) -> Result<Self, EngineError> {
        let file = tokio::fs::File::create(path)
            .await
            .map_err(|err| io_error(path, &err))?;
        Ok(Self {
            file,
            hasher: Sha256::new(),
            bytes: 0,
            limit,
        })
    }

    /// Flushes and returns the digest and byte count.
    async fn finish(self) -> Result<(String, u64), EngineError> {
        let Self {
            mut file,
            hasher,
            bytes,
            limit: _,
        } = self;
        tokio::io::AsyncWriteExt::flush(&mut file)
            .await
            .map_err(|err| EngineError::Io(err.to_string()))?;
        Ok((
            format!("sha256:{}", crate::image::hex(&hasher.finalize())),
            bytes,
        ))
    }
}

impl AsyncWrite for HashingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(limit) = this.limit
            && this.bytes + buf.len() as u64 > limit
        {
            return Poll::Ready(Err(io::Error::other(format!(
                "blob is larger than its descriptor size of {limit} bytes"
            ))));
        }
        match Pin::new(&mut this.file).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.hasher.update(&buf[..written]);
                this.bytes += written as u64;
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

fn io_error(path: &Path, err: &io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageDigest;

    fn image(reference: &str, digest: &str) -> ImageRef {
        ImageRef {
            reference: reference.to_owned(),
            digest: ImageDigest(digest.to_owned()),
        }
    }

    #[test]
    fn digest_validation_accepts_the_canonical_form_only() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(digest_hex(&format!("sha256:{hex}")).expect("valid"), hex);
        for bad in [
            "",
            hex,
            "sha256:",
            "sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0",
        ] {
            assert!(
                matches!(digest_hex(bad), Err(EngineError::Image(_))),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn reference_is_pulled_by_digest() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let reference = by_digest_reference(&image(
            "docker.io/library/alpine:3.22",
            &format!("sha256:{hex}"),
        ))
        .expect("reference");
        assert_eq!(reference.registry(), "docker.io");
        assert_eq!(reference.repository(), "library/alpine");
        let expected = format!("sha256:{hex}");
        assert_eq!(reference.digest(), Some(expected.as_str()));
        assert_eq!(reference.tag(), None);
    }

    #[test]
    fn a_reference_with_a_different_digest_is_refused() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let other = "1111111111111111111111111111111111111111111111111111111111111111";
        let err = by_digest_reference(&image(
            &format!("docker.io/library/alpine@sha256:{other}"),
            &format!("sha256:{hex}"),
        ))
        .expect_err("conflict");
        assert!(err.to_string().contains(other), "{err}");
    }

    #[test]
    fn a_reference_with_the_same_digest_is_accepted() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        by_digest_reference(&image(
            &format!("docker.io/library/alpine@sha256:{hex}"),
            &format!("sha256:{hex}"),
        ))
        .expect("same digest");
    }

    #[test]
    fn a_malformed_reference_is_an_image_error() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(matches!(
            by_digest_reference(&image("", &format!("sha256:{hex}"))),
            Err(EngineError::Image(_))
        ));
    }

    #[test]
    fn manifest_bytes_are_verified_against_the_requested_digest() {
        let body = br#"{"layers":[]}"#;
        verify_bytes(body, &sha256_digest(body)).expect("match");
        let err = verify_bytes(body, &format!("sha256:{}", "0".repeat(64))).expect_err("mismatch");
        let message = err.to_string();
        assert!(message.contains("manifest digest mismatch"), "{message}");
        assert!(message.contains("expected sha256:"), "{message}");
        assert!(message.contains("got sha256:"), "{message}");
    }
}
