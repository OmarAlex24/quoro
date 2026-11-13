// SPDX-License-Identifier: AGPL-3.0-only
//! The node-local key that encrypts this node's copy of desired-state secrets
//! (`node-agent-protocol.md` section 12).
//!
//! One 32-byte key lives in `<data_dir>/local.key`, mode `0600`, created on
//! first start and never leaves the node. Bodies are sealed with
//! XChaCha20-Poly1305, a random 24-byte nonce and the associated data
//! `b"desired_state" || rev`, so a ciphertext copied to another revision does
//! not decrypt.
//!
//! The file is written atomically: a temporary file in the same directory is
//! written, fsynced and renamed, so a crash never leaves a half-written key
//! behind. An existing file of the wrong length is refused rather than
//! replaced, because replacing it would silently make every stored body
//! unreadable.

use std::io;
use std::path::Path;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};

use crate::AgentError;
use crate::util;

/// Length of the node-local key, in bytes.
pub const KEY_LEN: usize = 32;
/// Length of the XChaCha20-Poly1305 nonce, in bytes.
pub const NONCE_LEN: usize = 24;
/// Version byte in front of every sealed body.
const VERSION: u8 = 1;
/// The file name, inside the data directory.
const KEY_FILE: &str = "local.key";

/// The node-local cipher.
pub struct LocalKey {
    cipher: XChaCha20Poly1305,
}

impl std::fmt::Debug for LocalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocalKey([redacted])")
    }
}

impl LocalKey {
    /// Loads `<data_dir>/local.key`, creating it on first start.
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when the file cannot be written or the directory
    /// cannot be created, and [`AgentError::LocalKey`] when an existing file
    /// is not exactly [`KEY_LEN`] bytes.
    pub fn load_or_create(data_dir: &Path) -> Result<Self, AgentError> {
        let path = data_dir.join(KEY_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => Self::from_bytes(&bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let key: [u8; KEY_LEN] = util::random_bytes();
                write_atomic(data_dir, &path, &key)?;
                Ok(Self::from_key(&key))
            }
            Err(error) => Err(AgentError::io("read the node-local key", error)),
        }
    }

    /// A random key held only in memory, which no file holds: the key of a
    /// store opened read-only ([`crate::store::NodeStore::open_read_only`]),
    /// which neither reads nor creates `local.key`. A body sealed with the
    /// node's key does not open with it.
    pub(crate) fn in_memory() -> Self {
        let key: [u8; KEY_LEN] = util::random_bytes();
        Self::from_key(&key)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, AgentError> {
        let key: [u8; KEY_LEN] = bytes.try_into().map_err(|_| {
            AgentError::LocalKey(format!(
                "the node-local key file has {} bytes, expected {KEY_LEN}",
                bytes.len()
            ))
        })?;
        Ok(Self::from_key(&key))
    }

    fn from_key(key: &[u8; KEY_LEN]) -> Self {
        let key = Key::from(*key);
        Self {
            cipher: XChaCha20Poly1305::new(&key),
        }
    }

    /// Seals `plaintext` for the desired-state revision `rev`.
    ///
    /// The result is `version || nonce || ciphertext-with-tag`.
    ///
    /// # Panics
    ///
    /// Panics when the operating system's random generator fails.
    #[must_use]
    pub fn seal(&self, rev: u64, plaintext: &[u8]) -> Vec<u8> {
        let nonce_bytes: [u8; NONCE_LEN] = util::random_bytes();
        let nonce = XNonce::from(nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad(rev),
                },
            )
            .expect("XChaCha20-Poly1305 encrypts any small message");
        let mut blob = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        blob.push(VERSION);
        blob.extend_from_slice(&nonce_bytes);
        blob.extend_from_slice(&ciphertext);
        blob
    }

    /// Opens a blob produced by [`LocalKey::seal`] for revision `rev`.
    ///
    /// # Errors
    ///
    /// [`AgentError::LocalKey`] when the blob is truncated, has another
    /// version, is bound to another revision or fails authentication.
    pub fn open(&self, rev: u64, blob: &[u8]) -> Result<Vec<u8>, AgentError> {
        if blob.len() < 1 + NONCE_LEN {
            return Err(AgentError::LocalKey(
                "the stored desired state is truncated".to_owned(),
            ));
        }
        if blob[0] != VERSION {
            return Err(AgentError::LocalKey(format!(
                "the stored desired state has unknown version {}",
                blob[0]
            )));
        }
        let nonce = XNonce::try_from(&blob[1..1 + NONCE_LEN]).map_err(|_| {
            AgentError::LocalKey("the stored desired state is truncated".to_owned())
        })?;
        self.cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &blob[1 + NONCE_LEN..],
                    aad: &aad(rev),
                },
            )
            .map_err(|_| {
                AgentError::LocalKey(
                    "the stored desired state is corrupt or was tampered with".to_owned(),
                )
            })
    }
}

/// The associated data binding a body to `desired_state` and its revision.
fn aad(rev: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(12 + 20);
    aad.extend_from_slice(b"desired_state");
    aad.extend_from_slice(rev.to_string().as_bytes());
    aad
}

/// Writes `bytes` to `path` with mode `0600`, through a temporary file in
/// `dir` and a rename.
fn write_atomic(dir: &Path, path: &Path, bytes: &[u8]) -> Result<(), AgentError> {
    let temp = dir.join(format!(
        ".local.key.{}.tmp",
        util::hex(&util::random_bytes::<8>())
    ));
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .map_err(|e| AgentError::io("create the node-local key", e))?;
        file.write_all(bytes)
            .map_err(|e| AgentError::io("write the node-local key", e))?;
        file.sync_all()
            .map_err(|e| AgentError::io("sync the node-local key", e))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&temp, bytes).map_err(|e| AgentError::io("write the node-local key", e))?;
    }
    std::fs::rename(&temp, path).map_err(|e| {
        let _ = std::fs::remove_file(&temp);
        AgentError::io("install the node-local key", e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-localkey-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn the_key_is_created_once_and_reloaded() {
        let dir = TempDir::new();
        let key = LocalKey::load_or_create(&dir.0).unwrap();
        let sealed = key.seal(4, b"{\"a\":1}");
        let bytes = std::fs::read(dir.0.join(KEY_FILE)).unwrap();
        assert_eq!(bytes.len(), KEY_LEN);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.0.join(KEY_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        let reloaded = LocalKey::load_or_create(&dir.0).unwrap();
        assert_eq!(reloaded.open(4, &sealed).unwrap(), b"{\"a\":1}");
    }

    #[test]
    fn ciphertexts_are_bound_to_their_revision() {
        let key = LocalKey::from_key(&[7; KEY_LEN]);
        let sealed = key.seal(2, b"body");
        assert!(key.open(2, &sealed).is_ok());
        assert!(key.open(3, &sealed).is_err());

        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(key.open(2, &tampered).is_err());
        assert!(key.open(2, &sealed[..3]).is_err());
    }

    #[test]
    fn a_wrong_length_key_file_is_refused() {
        let dir = TempDir::new();
        std::fs::write(dir.0.join(KEY_FILE), [0u8; 5]).unwrap();
        assert!(matches!(
            LocalKey::load_or_create(&dir.0),
            Err(AgentError::LocalKey(_))
        ));
    }
}
