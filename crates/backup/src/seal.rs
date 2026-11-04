// SPDX-License-Identifier: AGPL-3.0-only
//! The seal (`backup-format.md` section 6.2).
//!
//! The primary writes the seal, and only then does a backup exist for a
//! restore. The signature is the Ed25519 signature, by the cluster signing key,
//! over the RFC 8785 canonical JSON of the object without `signature`; both
//! halves come from `quoro_proto::sign`, so there is one canonicalizer in the
//! workspace.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result, SourceKind};

/// The signed seal of a succeeded backup (`seals/<source_id>/<backup_id>.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seal {
    /// [`crate::FORMAT`].
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
    /// The fencing token of the app's lease, checked before sealing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// The manifest object's key, `manifests/<source_id>/<backup_id>`.
    pub manifest_key: String,
    /// The unkeyed BLAKE3 of the manifest object (section 6.1).
    pub manifest_digest: String,
    /// The size of the file the manifest's chunks cover.
    pub logical_bytes: u64,
    /// The number of chunks in the manifest.
    pub chunk_count: u64,
    /// RFC 3339 UTC time the backup was recorded.
    pub created_at: String,
    /// RFC 3339 UTC time the seal was written.
    pub sealed_at: String,
    /// Base64 Ed25519 signature over the canonical JSON without `signature`.
    #[serde(default)]
    pub signature: String,
}

/// Signs `seal` with the cluster signing key, replacing any earlier signature
/// (section 6.2).
///
/// # Errors
///
/// [`Error::Format`] when the seal does not serialize to a JSON object, and
/// [`Error::Signature`] when it has no canonical form.
pub fn sign_seal(seal: &mut Seal, key: &quoro_proto::sign::SigningKey) -> Result<()> {
    let mut body = body(seal)?;
    quoro_proto::sign::sign_body(key, &mut body)
        .map_err(|error| Error::Signature(error.to_string()))?;
    let signature = body
        .get("signature")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Signature("the signed seal lost its signature".into()))?;
    seal.signature = signature.to_owned();
    Ok(())
}

/// Verifies `seal` against the cluster public key (section 6.2).
///
/// # Errors
///
/// [`Error::Format`] when the seal does not serialize to a JSON object, and
/// [`Error::Signature`] when `signature` is missing or malformed, or does not
/// verify: another key, or any changed field.
pub fn verify_seal(seal: &Seal, key: &quoro_proto::sign::VerifyingKey) -> Result<()> {
    let body = body(seal)?;
    quoro_proto::sign::verify_body(key, &body).map_err(|error| Error::Signature(error.to_string()))
}

/// The seal as the JSON object the signature covers.
fn body(seal: &Seal) -> Result<Value> {
    serde_json::to_value(seal)
        .map_err(|error| Error::Format(format!("the seal does not serialize: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FORMAT;

    fn seal() -> Seal {
        Seal {
            format: FORMAT,
            backup_id: "bkp_one".into(),
            cluster_id: "cls_one".into(),
            repository_id: "brp_one".into(),
            source_kind: SourceKind::Volume,
            source_id: "app_one".into(),
            volume_id: Some("vol_one".into()),
            fencing_token: Some(3),
            manifest_key: "manifests/app_one/bkp_one".into(),
            manifest_digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .into(),
            logical_bytes: 10_737_418_240,
            chunk_count: 10_240,
            created_at: "2026-11-05T03:00:00Z".into(),
            sealed_at: "2026-11-05T03:00:42Z".into(),
            signature: String::new(),
        }
    }

    fn signing_key(seed: u8) -> quoro_proto::sign::SigningKey {
        quoro_proto::sign::signing_key_from_seed(&[seed; 32])
    }

    #[test]
    fn signs_and_verifies() {
        let key = signing_key(7);
        let mut seal = seal();
        sign_seal(&mut seal, &key).unwrap();
        assert!(!seal.signature.is_empty());
        verify_seal(&seal, &key.verifying_key()).unwrap();
    }

    #[test]
    fn a_changed_field_or_another_key_fails() {
        let key = signing_key(7);
        let mut seal = seal();
        sign_seal(&mut seal, &key).unwrap();

        let mut changed = seal.clone();
        changed.chunk_count += 1;
        assert!(matches!(
            verify_seal(&changed, &key.verifying_key()),
            Err(Error::Signature(_))
        ));

        changed = seal.clone();
        changed.volume_id = None;
        assert!(verify_seal(&changed, &key.verifying_key()).is_err());

        assert!(verify_seal(&seal, &signing_key(8).verifying_key()).is_err());
    }

    #[test]
    fn an_unsigned_or_malformed_seal_fails() {
        let seal = seal();
        assert!(matches!(
            verify_seal(&seal, &signing_key(1).verifying_key()),
            Err(Error::Signature(_))
        ));
        let mut malformed = seal.clone();
        malformed.signature = "!!".into();
        assert!(verify_seal(&malformed, &signing_key(1).verifying_key()).is_err());
    }
}
