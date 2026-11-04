// SPDX-License-Identifier: AGPL-3.0-only
//! The manifest and its encrypted object (`backup-format.md` section 6.1).
//!
//! The machine that took the backup writes one manifest per backup, after its
//! last chunk is acknowledged. Its plaintext is JSON; the object is that JSON
//! compressed with zstd and encrypted like a chunk under `data_key`, with
//! `manifest:` followed by the backup id as associated data, so a manifest
//! cannot be passed off as another backup's. [`manifest_digest`] is the
//! unkeyed BLAKE3 of the encrypted bytes, which the node reports to the
//! primary.

use blake3::Hash;
use serde::{Deserialize, Serialize};

use crate::{
    CHUNK_AVG, CHUNK_MAX, CHUNK_MIN, ChunkId, Error, FORMAT, Result, SourceKeys, aead, util,
};

/// The zstd level a manifest is compressed at, like a chunk (section 6.1).
const ZSTD_LEVEL: i32 = 3;
/// The largest plaintext a manifest may decompress to, in bytes (256 MiB).
const MANIFEST_PLAINTEXT_MAX: usize = 256 * 1024 * 1024;
/// The only chunker `algorithm` format version 1 knows (section 5).
const CHUNKER_ALGORITHM: &str = "fastcdc2020";

/// What a backup was taken of (section 6.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// A volume of an app; `source_id` is the app id.
    Volume,
    /// The control-plane database; `source_id` is the cluster id.
    ControlPlane,
}

/// The chunker parameters format version 1 fixes (section 5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunker {
    /// `fastcdc2020`.
    pub algorithm: String,
    /// 256 KiB.
    pub min: u32,
    /// 1 MiB.
    pub avg: u32,
    /// 4 MiB.
    pub max: u32,
}

impl Chunker {
    /// The one set of parameters format version 1 accepts.
    #[must_use]
    pub fn fixed() -> Self {
        Self {
            algorithm: CHUNKER_ALGORITHM.to_owned(),
            min: CHUNK_MIN,
            avg: CHUNK_AVG,
            max: CHUNK_MAX,
        }
    }

    /// Whether these are exactly the fixed parameters of section 5.
    fn is_fixed(&self) -> bool {
        self.algorithm == CHUNKER_ALGORITHM
            && self.min == CHUNK_MIN
            && self.avg == CHUNK_AVG
            && self.max == CHUNK_MAX
    }
}

/// One entry of a manifest's `chunks` array: an id, an offset and a length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestChunk {
    /// 64 lowercase hex.
    pub id: String,
    /// Byte offset of the chunk in the file.
    pub offset: u64,
    /// Plaintext length, more than zero and at most [`CHUNK_MAX`].
    pub length: u32,
}

/// The plaintext manifest of one backup (section 6.1).
///
/// `volume_id`, `fencing_token` and `snapshot_method` are present for a volume
/// backup and absent for a control-plane backup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// [`FORMAT`].
    pub format: u32,
    /// `bkp_<ULID>`.
    pub backup_id: String,
    /// `cls_<ULID>`.
    pub cluster_id: String,
    /// `brp_<ULID>`.
    pub repository_id: String,
    /// A volume or the control-plane database.
    pub source_kind: SourceKind,
    /// The app id for a volume, the cluster id for the control plane.
    pub source_id: String,
    /// The volume the backup was taken of.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    /// The fencing token of the app's lease when the backup was recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// `reflink` or `pause_copy`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_method: Option<String>,
    /// The size of the file the chunks cover.
    pub logical_bytes: u64,
    /// The fixed FastCDC parameters.
    pub chunker: Chunker,
    /// The chunks in file order, with no gaps and no overlaps.
    pub chunks: Vec<ManifestChunk>,
    /// The Quoro version that wrote it.
    pub quoro_version: String,
    /// RFC 3339 UTC time.
    pub created_at: String,
}

impl Manifest {
    /// Checks everything section 6.1 fixes about a manifest.
    ///
    /// # Errors
    ///
    /// [`Error::Format`] for an unknown format, chunker parameters that are
    /// not exactly section 5's, a bad id, chunks not in order from zero, a gap,
    /// an overlap, a length outside `0 < length ≤ CHUNK_MAX`, lengths that do
    /// not add up to `logical_bytes`, or volume fields that do not match
    /// `source_kind`.
    pub fn validate(&self) -> Result<()> {
        if self.format != FORMAT {
            return Err(Error::Format(format!(
                "unknown manifest format {}",
                self.format
            )));
        }
        if !self.chunker.is_fixed() {
            return Err(Error::Format(
                "the chunker parameters are not fastcdc2020 with 256 KiB, 1 MiB and 4 MiB".into(),
            ));
        }
        let mut expected: u64 = 0;
        for entry in &self.chunks {
            ChunkId::from_hex(&entry.id)?;
            if entry.length == 0 || entry.length > CHUNK_MAX {
                return Err(Error::Format(format!(
                    "chunk length {} is outside 1..={CHUNK_MAX}",
                    entry.length
                )));
            }
            if entry.offset != expected {
                return Err(Error::Format(format!(
                    "chunk at offset {} is expected at {expected}",
                    entry.offset
                )));
            }
            expected = expected
                .checked_add(u64::from(entry.length))
                .ok_or_else(|| Error::Format("the chunk offsets overflow".into()))?;
        }
        if expected != self.logical_bytes {
            return Err(Error::Format(format!(
                "the chunks cover {expected} bytes, not {}",
                self.logical_bytes
            )));
        }
        if let Some(method) = &self.snapshot_method
            && method != "reflink"
            && method != "pause_copy"
        {
            return Err(Error::Format(format!("unknown snapshot method {method:?}")));
        }
        match self.source_kind {
            SourceKind::Volume => {
                if self.volume_id.is_none()
                    || self.fencing_token.is_none()
                    || self.snapshot_method.is_none()
                {
                    return Err(Error::Format(
                        "a volume backup needs a volume id, a fencing token and a snapshot method"
                            .into(),
                    ));
                }
            }
            SourceKind::ControlPlane => {
                if self.volume_id.is_some()
                    || self.fencing_token.is_some()
                    || self.snapshot_method.is_some()
                {
                    return Err(Error::Format(
                        "a control-plane backup has no volume id, fencing token or snapshot method"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The encrypted manifest object of `manifest` (section 6.1).
///
/// # Errors
///
/// [`Error::Format`] when [`Manifest::validate`] refuses the manifest.
pub fn encrypt_manifest(keys: &SourceKeys, manifest: &Manifest) -> Result<Vec<u8>> {
    manifest.validate()?;
    let json = serde_json::to_vec(manifest)
        .map_err(|error| Error::Format(format!("the manifest does not serialize: {error}")))?;
    let compressed =
        zstd::bulk::compress(&json, ZSTD_LEVEL).map_err(|error| Error::Io(error.to_string()))?;
    Ok(aead::encrypt(
        keys.data_key.as_bytes(),
        manifest_aad(&manifest.backup_id).as_bytes(),
        &compressed,
    ))
}

/// Opens a manifest object and checks it belongs to `backup_id` and
/// `source_id` (section 6.1, section 8.1 step 1).
///
/// # Errors
///
/// [`Error::Corrupt`] when the object does not authenticate, does not
/// decompress within 256 MiB or is not JSON, and [`Error::Format`] when it
/// names another backup or source, or [`Manifest::validate`] refuses it.
pub fn decrypt_manifest(
    keys: &SourceKeys,
    backup_id: &str,
    source_id: &str,
    object: &[u8],
) -> Result<Manifest> {
    let compressed = aead::decrypt(
        keys.data_key.as_bytes(),
        manifest_aad(backup_id).as_bytes(),
        object,
    )?;
    let json = zstd::bulk::decompress(&compressed, MANIFEST_PLAINTEXT_MAX)
        .map_err(|_| Error::Corrupt("the manifest does not decompress within 256 MiB".into()))?;
    let manifest: Manifest = serde_json::from_slice(&json)
        .map_err(|error| Error::Format(format!("the manifest is not valid JSON: {error}")))?;
    if manifest.backup_id != backup_id {
        return Err(Error::Format(format!(
            "the manifest belongs to backup {:?}",
            manifest.backup_id
        )));
    }
    if manifest.source_id != source_id {
        return Err(Error::Format(format!(
            "the manifest belongs to source {:?}",
            manifest.source_id
        )));
    }
    manifest.validate()?;
    Ok(manifest)
}

/// The unkeyed BLAKE3 of the encrypted manifest object, as 64 lowercase hex
/// (section 6.1).
/// Checks the encrypted manifest's digest against the one the seal (or the
/// job) gives, then decrypts and checks it (`backup-format.md` section 8.1
/// step 1). Readers use this, not [`decrypt_manifest`] alone, so the digest
/// check cannot be skipped.
///
/// # Errors
///
/// `Error::Corrupt` when the digest differs, then any error of
/// [`decrypt_manifest`].
pub fn open_manifest(
    keys: &SourceKeys,
    backup_id: &str,
    source_id: &str,
    expected_digest: &str,
    object: &[u8],
) -> Result<Manifest> {
    if manifest_digest(object) != expected_digest {
        return Err(Error::Corrupt(
            "the manifest digest does not match the seal".into(),
        ));
    }
    decrypt_manifest(keys, backup_id, source_id, object)
}

#[must_use]
pub fn manifest_digest(object: &[u8]) -> String {
    let digest: Hash = blake3::hash(object);
    util::to_hex(digest.as_bytes())
}

/// `manifest:` followed by the backup id, the manifest's associated data.
fn manifest_aad(backup_id: &str) -> String {
    format!("manifest:{backup_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepoKey;

    fn keys() -> SourceKeys {
        RepoKey::generate().source_keys("app_one")
    }

    fn manifest() -> Manifest {
        Manifest {
            format: FORMAT,
            backup_id: "bkp_one".into(),
            cluster_id: "cls_one".into(),
            repository_id: "brp_one".into(),
            source_kind: SourceKind::Volume,
            source_id: "app_one".into(),
            volume_id: Some("vol_one".into()),
            fencing_token: Some(3),
            snapshot_method: Some("reflink".into()),
            logical_bytes: 1_048_576 + 731_402,
            chunker: Chunker::fixed(),
            chunks: vec![
                ManifestChunk {
                    id: "9c4e".to_owned() + &"0".repeat(60),
                    offset: 0,
                    length: 1_048_576,
                },
                ManifestChunk {
                    id: "51a0".to_owned() + &"1".repeat(60),
                    offset: 1_048_576,
                    length: 731_402,
                },
            ],
            quoro_version: "0.6.0".into(),
            created_at: "2026-11-05T03:00:41Z".into(),
        }
    }

    #[test]
    fn round_trips_and_binds_backup_and_source() {
        let keys = keys();
        let manifest = manifest();
        manifest.validate().unwrap();
        let object = encrypt_manifest(&keys, &manifest).unwrap();
        assert_eq!(manifest_digest(&object).len(), 64);
        assert_eq!(
            decrypt_manifest(&keys, "bkp_one", "app_one", &object).unwrap(),
            manifest
        );
        assert!(decrypt_manifest(&keys, "bkp_two", "app_one", &object).is_err());
        assert!(decrypt_manifest(&keys, "bkp_one", "app_two", &object).is_err());

        let mut tampered = object.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(matches!(
            decrypt_manifest(&keys, "bkp_one", "app_one", &tampered),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn optional_volume_fields_are_omitted_for_the_control_plane() {
        let mut manifest = manifest();
        manifest.source_kind = SourceKind::ControlPlane;
        manifest.source_id = "cls_one".into();
        manifest.volume_id = None;
        manifest.fencing_token = None;
        manifest.snapshot_method = None;
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(!json.contains("volume_id"));
        assert!(!json.contains("fencing_token"));
        assert!(!json.contains("snapshot_method"));
        manifest.validate().unwrap();
    }

    #[test]
    fn a_gap_an_overlap_and_wrong_bytes_are_refused() {
        let mut gap = manifest();
        gap.chunks[1].offset = 1_048_577;
        assert!(matches!(gap.validate(), Err(Error::Format(_))));

        let mut overlap = manifest();
        overlap.chunks[1].offset = 1;
        assert!(overlap.validate().is_err());

        let mut wrong = manifest();
        wrong.logical_bytes += 1;
        assert!(wrong.validate().is_err());

        let mut zero = manifest();
        zero.chunks[0].length = 0;
        assert!(zero.validate().is_err());

        let mut huge = manifest();
        huge.chunks[0].length = CHUNK_MAX + 1;
        assert!(huge.validate().is_err());

        let mut bad_id = manifest();
        bad_id.chunks[0].id = "zz".repeat(32);
        assert!(bad_id.validate().is_err());

        let mut format = manifest();
        format.format = 2;
        assert!(format.validate().is_err());

        let mut chunker = manifest();
        chunker.chunker.avg = 1024;
        assert!(chunker.validate().is_err());
    }

    #[test]
    fn volume_fields_must_match_the_source_kind() {
        let mut missing = manifest();
        missing.volume_id = None;
        assert!(missing.validate().is_err());

        let mut control = manifest();
        control.source_kind = SourceKind::ControlPlane;
        assert!(control.validate().is_err());
    }

    #[test]
    fn open_manifest_checks_the_digest_first() {
        let keys = keys();
        let manifest = manifest();
        let object = encrypt_manifest(&keys, &manifest).unwrap();
        let digest = manifest_digest(&object);
        let opened =
            open_manifest(&keys, &manifest.backup_id, "app_one", &digest, &object).unwrap();
        assert_eq!(opened, manifest);
        assert!(matches!(
            open_manifest(
                &keys,
                &manifest.backup_id,
                "app_one",
                &"0".repeat(64),
                &object
            ),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn an_unknown_snapshot_method_is_refused() {
        let mut manifest = manifest();
        manifest.snapshot_method = Some("lvm".into());
        assert!(matches!(manifest.validate(), Err(Error::Format(_))));
    }
}
