//! The cluster data-encryption key (DEK, D-0050).
//!
//! One current key lives in `<data_dir>/keys/data.key`: 48 raw bytes, a
//! 16-byte key id followed by the 32-byte key. Every `*_enc` BLOB in the
//! database is `key_id || nonce || ciphertext-with-tag` (see
//! `docs/contracts/db-schema.sql`), so rotating the key can re-encrypt rows
//! lazily and a blob can never be decrypted under the wrong key. The key id is
//! also bound into the AEAD's associated data together with the column context
//! and the row id, so a blob copied to another row fails to decrypt.
//!
//! The file is created once with `create_new` and mode `0600`, and the
//! directory with mode `0700`; a file other users can read is refused rather
//! than silently used.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use std::io;
use std::path::Path;
use zeroize::{Zeroize, Zeroizing};

/// Length of a key id, in bytes.
const KEY_ID_LEN: usize = 16;
/// Length of the XChaCha20-Poly1305 nonce, in bytes.
const NONCE_LEN: usize = 24;
/// Length of the Poly1305 tag, in bytes.
const TAG_LEN: usize = 16;
/// Length of `data.key`: key id then key.
const FILE_LEN: usize = KEY_ID_LEN + 32;

/// Why a stored secret could not be decrypted. Deliberately carries no
/// plaintext, key material or blob contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CryptoError {
    /// The blob names a key id this keyring does not hold.
    UnknownKey,
    /// The blob is truncated or fails authentication.
    Corrupt,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKey => f.write_str("the blob was encrypted with an unknown data key"),
            Self::Corrupt => f.write_str("the encrypted blob is corrupt or truncated"),
        }
    }
}

impl std::error::Error for CryptoError {}

/// A secret held in memory. It is zeroed when dropped, prints as
/// `Secret([redacted])` and is never serialized.
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Takes ownership of the plaintext.
    #[must_use]
    pub fn new(secret: String) -> Self {
        Self(Zeroizing::new(secret))
    }

    /// The plaintext, for the few places that must use it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

/// The cluster data-encryption keyring: one current key, ready for more.
pub struct DataKey {
    key_id: [u8; KEY_ID_LEN],
    /// Built once from the key bytes, which are wiped right after; the cipher
    /// zeroizes its own key schedule on drop (`zeroize` feature).
    cipher: XChaCha20Poly1305,
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DataKey").field(&hex(&self.key_id)).finish()
    }
}

impl DataKey {
    /// Loads `<data_dir>/keys/data.key`, creating it on first start.
    ///
    /// # Errors
    ///
    /// An I/O error creating the directory or writing the key, a random-number
    /// failure, or a key file that is the wrong length or readable by group or
    /// others.
    pub fn load_or_create(data_dir: &Path) -> io::Result<Self> {
        let dir = data_dir.join("keys");
        create_private_dir(&dir)?;
        let path = dir.join("data.key");
        match std::fs::metadata(&path) {
            Ok(_) => read_key(&path),
            Err(error) if error.kind() == io::ErrorKind::NotFound => write_new_key(&path),
            Err(error) => Err(error),
        }
    }

    /// A random in-memory key, for tests and for a process without a data
    /// directory.
    ///
    /// # Panics
    ///
    /// Panics if the operating system's random generator fails.
    #[must_use]
    pub fn ephemeral() -> Self {
        let key_id = random_bytes().expect("OS random generator");
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(random_bytes().expect("OS random generator"));
        Self::from_parts(key_id, &key)
    }

    fn from_parts(key_id: [u8; KEY_ID_LEN], key: &[u8; 32]) -> Self {
        let mut key = Key::from(*key);
        let cipher = XChaCha20Poly1305::new(&key);
        key.as_mut_slice().zeroize();
        Self { key_id, cipher }
    }

    /// The id stored in front of every blob.
    #[must_use]
    pub fn key_id(&self) -> [u8; KEY_ID_LEN] {
        self.key_id
    }

    /// The key id as lowercase hex, for logs.
    #[must_use]
    pub fn key_id_hex(&self) -> String {
        hex(&self.key_id)
    }

    /// Encrypts `plaintext` for the column `aad_context` and the row `row_id`.
    ///
    /// # Panics
    ///
    /// Panics if the operating system's random generator fails.
    #[must_use]
    pub fn encrypt(&self, aad_context: &str, row_id: &str, plaintext: &[u8]) -> Vec<u8> {
        let nonce_bytes: [u8; NONCE_LEN] = random_bytes().expect("OS random generator");
        let nonce = XNonce::from(nonce_bytes);
        let aad = self.aad(aad_context, row_id);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .expect("XChaCha20-Poly1305 encrypts any small message");
        let mut blob = Vec::with_capacity(KEY_ID_LEN + NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(&self.key_id);
        blob.extend_from_slice(&nonce_bytes);
        blob.extend_from_slice(&ciphertext);
        blob
    }

    /// Decrypts a blob produced by [`DataKey::encrypt`] for the same column and
    /// row, under this keyring's current key.
    ///
    /// # Errors
    ///
    /// [`CryptoError::UnknownKey`] when the blob's key id is not this key, and
    /// [`CryptoError::Corrupt`] when it is truncated, tampered with or bound to
    /// a different column or row.
    pub fn decrypt(
        &self,
        aad_context: &str,
        row_id: &str,
        blob: &[u8],
    ) -> Result<Secret, CryptoError> {
        if blob.len() < KEY_ID_LEN + NONCE_LEN + TAG_LEN {
            return Err(CryptoError::Corrupt);
        }
        if blob[..KEY_ID_LEN] != self.key_id {
            return Err(CryptoError::UnknownKey);
        }
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&blob[KEY_ID_LEN..KEY_ID_LEN + NONCE_LEN]);
        let nonce = XNonce::from(nonce_bytes);
        let aad = self.aad(aad_context, row_id);
        let plaintext = self
            .cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &blob[KEY_ID_LEN + NONCE_LEN..],
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::Corrupt)?;
        match String::from_utf8(plaintext) {
            Ok(secret) => Ok(Secret::new(secret)),
            Err(error) => {
                let mut bytes = error.into_bytes();
                bytes.zeroize();
                Err(CryptoError::Corrupt)
            }
        }
    }

    /// `key_id || aad_context || 0x00 || row_id`, the associated data.
    fn aad(&self, aad_context: &str, row_id: &str) -> Vec<u8> {
        let mut aad = Vec::with_capacity(KEY_ID_LEN + aad_context.len() + 1 + row_id.len());
        aad.extend_from_slice(&self.key_id);
        aad.extend_from_slice(aad_context.as_bytes());
        aad.push(0);
        aad.extend_from_slice(row_id.as_bytes());
        aad
    }
}

/// Creates `dir` and restricts it to the owner.
fn create_private_dir(dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Reads and validates an existing key file.
fn read_key(path: &Path) -> io::Result<DataKey> {
    let metadata = std::fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} is readable by group or others; fix permissions to 0600",
                    path.display()
                ),
            ));
        }
    }
    let bytes = Zeroizing::new(std::fs::read(path)?);
    if bytes.len() != FILE_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is {} bytes; expected {FILE_LEN}",
                path.display(),
                bytes.len()
            ),
        ));
    }
    let mut key_id = [0u8; KEY_ID_LEN];
    key_id.copy_from_slice(&bytes[..KEY_ID_LEN]);
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&bytes[KEY_ID_LEN..]);
    Ok(DataKey::from_parts(key_id, &key))
}

/// Generates and writes a new key file. The key is written and synced to a
/// private temporary file first and then hard-linked into place: the link
/// either creates `data.key` complete or fails because another start won the
/// race, in which case the winner's file is read. A crash never leaves a
/// truncated `data.key`.
fn write_new_key(path: &Path) -> io::Result<DataKey> {
    use std::io::Write as _;

    let key_id: [u8; KEY_ID_LEN] = random_bytes()?;
    let key: Zeroizing<[u8; 32]> = Zeroizing::new(random_bytes()?);
    let mut bytes = Zeroizing::new([0u8; FILE_LEN]);
    bytes[..KEY_ID_LEN].copy_from_slice(&key_id);
    bytes[KEY_ID_LEN..].copy_from_slice(&*key);

    let tmp = path.with_extension(format!("tmp-{}", hex(&random_bytes::<8>()?)));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let written = (|| {
        let mut file = options.open(&tmp)?;
        file.write_all(&*bytes)?;
        file.sync_all()?;
        std::fs::hard_link(&tmp, path)
    })();
    let _ = std::fs::remove_file(&tmp);
    match written {
        Ok(()) => {
            if let Some(dir) = path.parent() {
                std::fs::File::open(dir)?.sync_all()?;
            }
            Ok(DataKey::from_parts(key_id, &key))
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => read_key(path),
        Err(error) => Err(error),
    }
}

/// `n` random bytes from the operating-system generator.
fn random_bytes<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(bytes)
}

/// Lowercase hex.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const CONTEXT: &str = "providers.secret_enc";

    /// A throwaway directory under the crate's `target/` (never `/tmp`).
    fn test_dir() -> PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("crypto-{}", hex(&random_bytes::<8>().unwrap())));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn round_trips_and_binds_column_and_row() {
        let key = DataKey::ephemeral();
        let blob = key.encrypt(CONTEXT, "prv_one", b"hunter2");
        assert_eq!(&blob[..KEY_ID_LEN], &key.key_id()[..]);
        assert_eq!(
            key.decrypt(CONTEXT, "prv_one", &blob).unwrap().expose(),
            "hunter2"
        );
        assert_eq!(
            key.decrypt(CONTEXT, "prv_two", &blob).unwrap_err(),
            CryptoError::Corrupt
        );
        assert_eq!(
            key.decrypt("webhooks.secret_enc", "prv_one", &blob)
                .unwrap_err(),
            CryptoError::Corrupt
        );
    }

    #[test]
    fn rejects_tampering_truncation_and_other_keys() {
        let key = DataKey::ephemeral();
        let mut blob = key.encrypt(CONTEXT, "prv_one", b"hunter2");
        let last = blob.len() - 1;
        blob[last] ^= 0x01;
        assert_eq!(
            key.decrypt(CONTEXT, "prv_one", &blob).unwrap_err(),
            CryptoError::Corrupt
        );

        let blob = key.encrypt(CONTEXT, "prv_one", b"hunter2");
        assert_eq!(
            key.decrypt(CONTEXT, "prv_one", &blob[..blob.len() - 1])
                .unwrap_err(),
            CryptoError::Corrupt
        );
        assert_eq!(
            key.decrypt(CONTEXT, "prv_one", &blob[..KEY_ID_LEN + NONCE_LEN])
                .unwrap_err(),
            CryptoError::Corrupt
        );

        let other = DataKey::ephemeral();
        assert_eq!(
            other.decrypt(CONTEXT, "prv_one", &blob).unwrap_err(),
            CryptoError::UnknownKey
        );
    }

    #[test]
    fn load_or_create_keeps_one_key_and_private_modes() {
        let dir = test_dir();
        let first = DataKey::load_or_create(&dir).unwrap();
        let second = DataKey::load_or_create(&dir).unwrap();
        assert_eq!(first.key_id(), second.key_id());
        assert_eq!(
            first
                .decrypt(CONTEXT, "prv_one", &first.encrypt(CONTEXT, "prv_one", b"x"))
                .unwrap()
                .expose(),
            "x"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.join("keys/data.key"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
            let mode = std::fs::metadata(dir.join("keys"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_group_readable_key_file() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = test_dir();
        let keys = dir.join("keys");
        std::fs::create_dir_all(&keys).unwrap();
        std::fs::write(keys.join("data.key"), [0u8; FILE_LEN]).unwrap();
        std::fs::set_permissions(
            keys.join("data.key"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        let error = DataKey::load_or_create(&dir).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("fix permissions to 0600"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn debug_never_prints_the_key() {
        let key = DataKey::ephemeral();
        let rendered = format!("{key:?}");
        assert_eq!(rendered, format!("DataKey({:?})", key.key_id_hex()));
        assert_eq!(
            format!("{:?}", Secret::new("hunter2".into())),
            "Secret([redacted])"
        );
    }
}
