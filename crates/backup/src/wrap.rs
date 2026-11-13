// SPDX-License-Identifier: AGPL-3.0-only
//! The wrapped repository key of the config object (`backup-format.md`
//! sections 3 and 4).
//!
//! `repo_key` is encrypted under `wrap_key`, which is derived from the
//! recovery key, with the ASCII bytes of `repository_id` as associated data.
//! The wrapped form is what the config object carries in the bucket, so a
//! reader with the recovery key can open the repository without the primary.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{Error, RecoveryKey, RepoKey, Result, aead};

/// The `alg` of a wrapped repo key (section 4).
const ALG: &str = "xchacha20poly1305";

/// `repo_key` encrypted under the recovery key, as `config-<n>.json` carries
/// it (section 4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedKey {
    /// Always `xchacha20poly1305` for format version 1.
    pub alg: String,
    /// Base64 of the 24-byte nonce.
    pub nonce: String,
    /// Base64 of the 32-byte `repo_key` and the 16-byte tag.
    pub ciphertext: String,
}

/// Wraps `repo` under `recovery` for `repository_id` (section 4).
///
/// # Panics
///
/// Panics if the operating system's random generator fails.
#[must_use]
pub fn wrap_repo_key(repo: &RepoKey, recovery: &RecoveryKey, repository_id: &str) -> WrappedKey {
    let object = aead::encrypt(
        recovery.wrap_key().as_bytes(),
        repository_id.as_bytes(),
        repo.as_bytes(),
    );
    let (nonce, ciphertext) = object.split_at(aead::NONCE_LEN);
    WrappedKey {
        alg: ALG.to_owned(),
        nonce: STANDARD.encode(nonce),
        ciphertext: STANDARD.encode(ciphertext),
    }
}

/// Opens the `repo_key` from a config object with `recovery` (section 4).
///
/// # Errors
///
/// [`Error::Format`] when `alg` is unknown or a field is not base64 of the
/// right shape, and [`Error::Corrupt`] when the object does not authenticate,
/// which is what a wrong recovery key or a wrong `repository_id` produces.
pub fn unwrap_repo_key(
    wrapped: &WrappedKey,
    recovery: &RecoveryKey,
    repository_id: &str,
) -> Result<RepoKey> {
    if wrapped.alg != ALG {
        return Err(Error::Format(format!(
            "unknown wrapped key alg {:?}",
            wrapped.alg
        )));
    }
    let nonce = STANDARD
        .decode(&wrapped.nonce)
        .map_err(|_| Error::Format("the wrapped key nonce is not base64".into()))?;
    if nonce.len() != aead::NONCE_LEN {
        return Err(Error::Format(
            "the wrapped key nonce is not 24 bytes".into(),
        ));
    }
    let ciphertext = STANDARD
        .decode(&wrapped.ciphertext)
        .map_err(|_| Error::Format("the wrapped key ciphertext is not base64".into()))?;
    let mut object = Vec::with_capacity(nonce.len() + ciphertext.len());
    object.extend_from_slice(&nonce);
    object.extend_from_slice(&ciphertext);
    // The plaintext is the repository key: both buffers that hold it here
    // are wiped when they drop.
    let plaintext = Zeroizing::new(aead::decrypt(
        recovery.wrap_key().as_bytes(),
        repository_id.as_bytes(),
        &object,
    )?);
    if plaintext.len() != 32 {
        return Err(Error::Corrupt(
            "the wrapped repo key is not 32 bytes".into(),
        ));
    }
    let mut bytes = Zeroizing::new([0u8; 32]);
    bytes.copy_from_slice(&plaintext);
    Ok(RepoKey::from_bytes(*bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPOSITORY: &str = "brp_01JCA0A2B3C4D5E6F7G8H9J0KM";

    #[test]
    fn round_trips_under_the_recovery_key() {
        let repo = RepoKey::generate();
        let recovery = RecoveryKey::generate();
        let wrapped = wrap_repo_key(&repo, &recovery, REPOSITORY);
        assert_eq!(wrapped.alg, "xchacha20poly1305");
        let opened = unwrap_repo_key(&wrapped, &recovery, REPOSITORY).unwrap();
        assert_eq!(opened.as_bytes(), repo.as_bytes());
    }

    #[test]
    fn a_wrong_recovery_key_or_repository_is_refused() {
        let repo = RepoKey::generate();
        let recovery = RecoveryKey::generate();
        let wrapped = wrap_repo_key(&repo, &recovery, REPOSITORY);
        let other = RecoveryKey::generate();
        assert!(matches!(
            unwrap_repo_key(&wrapped, &other, REPOSITORY),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            unwrap_repo_key(&wrapped, &recovery, "brp_other"),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn malformed_fields_are_format_errors() {
        let recovery = RecoveryKey::generate();
        let mut wrapped = wrap_repo_key(&RepoKey::generate(), &recovery, REPOSITORY);
        wrapped.alg = "aes-gcm".into();
        assert!(matches!(
            unwrap_repo_key(&wrapped, &recovery, REPOSITORY),
            Err(Error::Format(_))
        ));
        let mut wrapped = wrap_repo_key(&RepoKey::generate(), &recovery, REPOSITORY);
        wrapped.nonce = "not base64".into();
        assert!(matches!(
            unwrap_repo_key(&wrapped, &recovery, REPOSITORY),
            Err(Error::Format(_))
        ));
        let mut wrapped = wrap_repo_key(&RepoKey::generate(), &recovery, REPOSITORY);
        wrapped.ciphertext = STANDARD.encode([0u8; 8]);
        assert!(matches!(
            unwrap_repo_key(&wrapped, &recovery, REPOSITORY),
            Err(Error::Corrupt(_))
        ));
    }
}
