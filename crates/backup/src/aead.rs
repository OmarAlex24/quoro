// SPDX-License-Identifier: AGPL-3.0-only
//! XChaCha20-Poly1305 objects: `nonce ‖ ciphertext ‖ tag` (sections 4, 5, 6.1).
//!
//! Every object of this format uses one 24-byte random nonce under one key and
//! a context string as associated data, so an object cannot be moved to
//! another id, backup or repository.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

use crate::{Error, Result, util};

/// Length of the XChaCha20-Poly1305 nonce, in bytes.
pub(crate) const NONCE_LEN: usize = 24;
/// Length of the Poly1305 tag, in bytes.
pub(crate) const TAG_LEN: usize = 16;

/// Encrypts `plaintext` under `key` with `aad`, returning `nonce ‖ ciphertext ‖
/// tag`.
///
/// # Panics
///
/// Panics if the operating system's random generator fails.
pub(crate) fn encrypt(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let nonce_bytes: [u8; NONCE_LEN] = util::random_bytes();
    let cipher = XChaCha20Poly1305::new_from_slice(key).expect("a 32-byte key is valid");
    let ciphertext = cipher
        .encrypt(
            &XNonce::from(nonce_bytes),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("XChaCha20-Poly1305 encrypts any small message");
    let mut object = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    object.extend_from_slice(&nonce_bytes);
    object.extend_from_slice(&ciphertext);
    object
}

/// Decrypts an object produced by [`encrypt`] under the same key and `aad`.
///
/// # Errors
///
/// [`Error::Corrupt`] when the object is shorter than a nonce and a tag, or
/// fails authentication.
pub(crate) fn decrypt(key: &[u8; 32], aad: &[u8], object: &[u8]) -> Result<Vec<u8>> {
    if object.len() < NONCE_LEN + TAG_LEN {
        return Err(Error::Corrupt(
            "the object is shorter than a nonce and a tag".into(),
        ));
    }
    let (nonce, ciphertext) = object.split_at(NONCE_LEN);
    let mut nonce_bytes = [0u8; NONCE_LEN];
    nonce_bytes.copy_from_slice(nonce);
    let cipher = XChaCha20Poly1305::new_from_slice(key).expect("a 32-byte key is valid");
    cipher
        .decrypt(
            &XNonce::from(nonce_bytes),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| Error::Corrupt("the object does not authenticate".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_binds_the_associated_data() {
        let key = [7u8; 32];
        let object = encrypt(&key, b"aad", b"plaintext");
        assert_eq!(object.len(), NONCE_LEN + TAG_LEN + b"plaintext".len());
        assert_eq!(decrypt(&key, b"aad", &object).unwrap(), b"plaintext");
        assert!(decrypt(&key, b"other", &object).is_err());
        assert!(decrypt(&[8u8; 32], b"aad", &object).is_err());
        assert!(decrypt(&key, b"aad", &object[..NONCE_LEN + TAG_LEN - 1]).is_err());
    }
}
