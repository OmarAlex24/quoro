//! The one-time setup token for `POST /api/v1/setup` (D-0054).
//!
//! `quoro setup-token` issues a token and prints it once. The data directory
//! keeps only its SHA-256 hash and its expiry, in a `0600` file; issuing a new
//! token replaces the previous unused one. A token is valid for one hour and
//! is consumed by a successful setup.

use crate::store::{now_rfc3339, random_secret, rfc3339_in, sha256_hex};
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// Lifetime of a setup token.
pub const TTL_SECONDS: i64 = 60 * 60;

const FILE_NAME: &str = "setup-token.json";

#[derive(Serialize, Deserialize)]
struct Stored {
    hash: String,
    expires_at: String,
}

fn path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

/// Issues a fresh token, replacing any previous one, and returns the secret.
/// The caller must refuse when an owner already exists.
///
/// # Errors
///
/// An I/O error writing the file.
pub fn issue(data_dir: &Path) -> std::io::Result<String> {
    let secret = format!("qst_{}", random_secret());
    let stored = Stored {
        hash: sha256_hex(secret.as_bytes()),
        expires_at: rfc3339_in(TTL_SECONDS),
    };
    let body = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
    let final_path = path(data_dir);
    let tmp = data_dir.join(format!("{FILE_NAME}.tmp"));
    // A stale temporary file would keep its old mode; start from nothing.
    let _ = std::fs::remove_file(&tmp);
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&body)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    Ok(secret)
}

/// Whether `presented` is the current, unexpired token. Comparison is over
/// the hashes in constant time.
#[must_use]
pub fn is_valid(data_dir: &Path, presented: &str) -> bool {
    let Ok(bytes) = std::fs::read(path(data_dir)) else {
        return false;
    };
    let Ok(stored) = serde_json::from_slice::<Stored>(&bytes) else {
        return false;
    };
    if stored.expires_at <= now_rfc3339() {
        return false;
    }
    let presented = sha256_hex(presented.as_bytes());
    constant_time_eq(presented.as_bytes(), stored.hash.as_bytes())
}

/// Removes the token after a successful setup, so it is single use.
///
/// # Errors
///
/// An I/O error other than the file already being gone.
pub fn consume(data_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path(data_dir)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-setup-{}", random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn issue_validate_replace_consume() {
        let dir = temp_dir();
        assert!(!is_valid(&dir, "qst_nothing"));
        let first = issue(&dir).unwrap();
        assert!(is_valid(&dir, &first));
        assert!(!is_valid(&dir, "qst_wrong"));
        let second = issue(&dir).unwrap();
        assert!(
            !is_valid(&dir, &first),
            "a new token invalidates the old one"
        );
        assert!(is_valid(&dir, &second));
        consume(&dir).unwrap();
        assert!(!is_valid(&dir, &second), "single use");
        consume(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            issue(&dir).unwrap();
            let mode = std::fs::metadata(path(&dir)).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn expired_tokens_are_refused() {
        let dir = temp_dir();
        let secret = issue(&dir).unwrap();
        let stored = Stored {
            hash: sha256_hex(secret.as_bytes()),
            expires_at: rfc3339_in(-1),
        };
        std::fs::write(path(&dir), serde_json::to_vec(&stored).unwrap()).unwrap();
        assert!(!is_valid(&dir, &secret));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
