// SPDX-License-Identifier: AGPL-3.0-only
//! Chunk ids and chunk objects (`backup-format.md` section 5).
//!
//! A chunk id is `BLAKE3-keyed(id_key, plaintext)` as 64 lowercase hex. A
//! plain content hash would let anyone who can read the bucket test whether a
//! known file is in a backup, so the id is keyed.
//!
//! A chunk object is the plaintext compressed with zstd at level 3 and then
//! encrypted with XChaCha20-Poly1305 under `data_key`, with a random 24-byte
//! nonce and the chunk id's ASCII bytes as associated data. The object is
//! `nonce ‖ ciphertext ‖ tag`.

use std::fmt;

use blake3::Hasher;

use crate::{CHUNK_MAX, Error, Key, Result, SourceKeys, aead, util};

/// The zstd level every chunk is compressed at (section 5).
const ZSTD_LEVEL: i32 = 3;

/// A 256-bit chunk id, the keyed BLAKE3 of the chunk's plaintext.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    /// The 64 lowercase hex characters the layout keys and the manifest use.
    #[must_use]
    pub fn to_hex(&self) -> String {
        util::to_hex(&self.0)
    }

    /// Parses exactly 64 lowercase hex characters.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] for a wrong length or any character that is not a
    /// lowercase hex digit.
    pub fn from_hex(text: &str) -> Result<Self> {
        util::from_hex_32(text)
            .map(Self)
            .ok_or_else(|| Error::Corrupt(format!("{text:?} is not 64 lowercase hex")))
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChunkId").field(&self.to_hex()).finish()
    }
}

/// `BLAKE3-keyed(id_key, plaintext)` (section 5).
#[must_use]
pub fn chunk_id(id_key: &Key, plaintext: &[u8]) -> ChunkId {
    let mut hasher = Hasher::new_keyed(id_key.as_bytes());
    hasher.update(plaintext);
    ChunkId(*hasher.finalize().as_bytes())
}

/// The id and the object of `plaintext`: compress, then encrypt (section 5).
///
/// # Panics
///
/// Panics if compression fails, which zstd does not do for a buffer in memory,
/// or if the operating system's random generator fails.
#[must_use]
pub fn seal_chunk(keys: &SourceKeys, plaintext: &[u8]) -> (ChunkId, Vec<u8>) {
    let id = chunk_id(&keys.id_key, plaintext);
    let compressed =
        zstd::bulk::compress(plaintext, ZSTD_LEVEL).expect("zstd compresses any in-memory buffer");
    let object = aead::encrypt(
        keys.data_key.as_bytes(),
        id.to_hex().as_bytes(),
        &compressed,
    );
    (id, object)
}

/// Opens a chunk object: decrypt, decompress within the 4 MiB bound, then
/// recompute the keyed id (section 5, section 8.1).
///
/// # Errors
///
/// [`Error::Corrupt`] when the object does not authenticate, decompresses
/// beyond [`CHUNK_MAX`], or does not hash to `id` under `keys.id_key`.
pub fn open_chunk(keys: &SourceKeys, id: &ChunkId, object: &[u8]) -> Result<Vec<u8>> {
    let compressed = aead::decrypt(keys.data_key.as_bytes(), id.to_hex().as_bytes(), object)?;
    let plaintext = zstd::bulk::decompress(&compressed, CHUNK_MAX as usize)
        .map_err(|_| Error::Corrupt("the chunk does not decompress within its maximum".into()))?;
    if chunk_id(&keys.id_key, &plaintext) != *id {
        return Err(Error::Corrupt(
            "the chunk id does not match its plaintext".into(),
        ));
    }
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepoKey;

    fn keys() -> SourceKeys {
        RepoKey::generate().source_keys("app_one")
    }

    #[test]
    fn an_all_zero_chunk_round_trips_and_compresses() {
        let keys = keys();
        let plaintext = vec![0u8; CHUNK_MAX as usize];
        let (id, object) = seal_chunk(&keys, &plaintext);
        assert!(object.len() < 1024, "{} compressed bytes", object.len());
        assert_eq!(open_chunk(&keys, &id, &object).unwrap(), plaintext);
    }

    #[test]
    fn a_tampered_object_or_wrong_id_is_refused() {
        let keys = keys();
        let (id, mut object) = seal_chunk(&keys, b"hello");
        object[aead::NONCE_LEN] ^= 0x01;
        assert!(matches!(
            open_chunk(&keys, &id, &object),
            Err(Error::Corrupt(_))
        ));

        let (id, object) = seal_chunk(&keys, b"hello");
        let other = ChunkId([0xff; 32]);
        assert!(matches!(
            open_chunk(&keys, &other, &object),
            Err(Error::Corrupt(_))
        ));
        assert!(open_chunk(&keys, &id, &object[..8]).is_err());
    }

    #[test]
    fn another_sources_keys_are_refused() {
        let other = RepoKey::generate().source_keys("app_two");
        let (id, object) = seal_chunk(&keys(), b"hello");
        assert!(matches!(
            open_chunk(&other, &id, &object),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn ids_are_64_lowercase_hex_and_parse_back() {
        let keys = keys();
        let (id, _) = seal_chunk(&keys, b"hello");
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(ChunkId::from_hex(&hex).unwrap(), id);
        assert!(ChunkId::from_hex(&hex.to_uppercase()).is_err());
        assert!(ChunkId::from_hex(&hex[..63]).is_err());
    }

    #[test]
    fn the_id_is_keyed_by_the_id_key() {
        let keys = keys();
        let mut hasher = Hasher::new();
        hasher.update(b"hello");
        assert_ne!(
            chunk_id(&keys.id_key, b"hello").0,
            *hasher.finalize().as_bytes()
        );
        assert_eq!(
            chunk_id(&keys.id_key, b"hello"),
            chunk_id(&keys.id_key, b"hello")
        );
    }
}
