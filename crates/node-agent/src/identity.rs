// SPDX-License-Identifier: AGPL-3.0-only
//! The node's identity files (`node-agent-protocol.md` sections 2.2-2.5).
//!
//! Everything lives in the agent's data directory, which is created `0700` and
//! holds files written `0600` and renamed into place, so a half-written file
//! never becomes the one the agent reads:
//!
//! - `node-ed25519.seed` — the node's private key. It never leaves the
//!   machine; enrollment sends only the public key.
//! - `credential` — the session credential returned once by enrollment.
//! - `identity.json` — the identity the primary gave us and the cluster key
//!   that signs desired state.
//!
//! [`IdentityHandle`] is the in-process view the session keeps: it answers with
//! the current pin and can persist a `signing_key_rotate` atomically.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use quoro_proto::sign::{self, VerifyingKey};
use sha2::{Digest as _, Sha256};

use crate::AgentError;
use crate::util;

/// File name of the node's Ed25519 private-key seed.
pub const SEED_FILE: &str = "node-ed25519.seed";
/// File name of the node session credential.
pub const CREDENTIAL_FILE: &str = "credential";
/// File name of the enrolled identity.
pub const IDENTITY_FILE: &str = "identity.json";

/// The identity the node received at enrollment (section 2.4), as
/// `identity.json` holds it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Identity {
    /// `nod_` id of this node.
    pub node_id: String,
    /// `cls_` id of the cluster it joined.
    pub cluster_id: String,
    /// Base URL the node dials for this and later sessions.
    pub primary_url: String,
    /// `sha256:<64 hex>` pin of the primary's TLS certificate.
    pub fingerprint: String,
    /// Base64 Ed25519 public key that signs desired state.
    pub cluster_public_key: String,
    /// Lowercase hex SHA-256 of the 32 raw bytes of `cluster_public_key`.
    pub cluster_key_id: String,
}

impl Identity {
    /// The `key_id` of a base64 Ed25519 public key: the lowercase hex SHA-256
    /// of its raw 32 bytes (section 5.21).
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the text is not a base64 Ed25519 key.
    pub fn key_id_for(public_key: &str) -> Result<String, AgentError> {
        let key = sign::public_key_from_base64(public_key)
            .map_err(|e| AgentError::Identity(format!("cluster public key: {e}")))?;
        Ok(util::hex(&Sha256::digest(key.as_bytes())))
    }

    /// The pinned cluster key, parsed.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when `cluster_public_key` is malformed.
    pub fn verifying_key(&self) -> Result<VerifyingKey, AgentError> {
        sign::public_key_from_base64(&self.cluster_public_key)
            .map_err(|e| AgentError::Identity(format!("cluster public key: {e}")))
    }
}

/// Whether a data directory holds a complete enrollment.
#[must_use]
pub fn is_enrolled(data_dir: &Path) -> bool {
    [IDENTITY_FILE, CREDENTIAL_FILE, SEED_FILE]
        .iter()
        .all(|name| data_dir.join(name).is_file())
}

/// Creates the data directory `0700` when it does not exist yet.
///
/// # Errors
///
/// [`AgentError::Io`] when the directory cannot be created or its mode set.
pub fn ensure_data_dir(data_dir: &Path) -> Result<(), AgentError> {
    fs::create_dir_all(data_dir).map_err(|e| AgentError::io("create data directory", e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700))
            .map_err(|e| AgentError::io("set data directory mode", e))?;
    }
    Ok(())
}

/// Reads the node's private-key seed.
///
/// # Errors
///
/// [`AgentError::Identity`] when the file is missing, unreadable or not exactly
/// 32 bytes.
pub fn read_seed(data_dir: &Path) -> Result<[u8; 32], AgentError> {
    let path = data_dir.join(SEED_FILE);
    let bytes = fs::read(&path).map_err(|e| AgentError::Identity(missing(&path, &e)))?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| AgentError::Identity(format!("{} is not 32 bytes", path.display())))
}

/// Writes the private-key seed `0600`.
///
/// # Errors
///
/// [`AgentError::Io`] when the file cannot be written.
pub fn write_seed(data_dir: &Path, seed: &[u8; 32]) -> Result<(), AgentError> {
    write_private(data_dir, SEED_FILE, seed)
}

/// Reads the session credential.
///
/// # Errors
///
/// [`AgentError::Identity`] when the file is missing or unreadable.
pub fn read_credential(data_dir: &Path) -> Result<String, AgentError> {
    let path = data_dir.join(CREDENTIAL_FILE);
    let text = fs::read_to_string(&path).map_err(|e| AgentError::Identity(missing(&path, &e)))?;
    let text = text.trim().to_owned();
    if text.is_empty() {
        return Err(AgentError::Identity(format!("{} is empty", path.display())));
    }
    Ok(text)
}

/// Writes the session credential `0600`.
///
/// # Errors
///
/// [`AgentError::Io`] when the file cannot be written.
pub fn write_credential(data_dir: &Path, credential: &str) -> Result<(), AgentError> {
    write_private(data_dir, CREDENTIAL_FILE, credential.as_bytes())
}

/// Loads `identity.json`.
///
/// # Errors
///
/// [`AgentError::Identity`] when the file is missing, unreadable, malformed or
/// has an empty required member.
pub fn load_identity(data_dir: &Path) -> Result<Identity, AgentError> {
    let path = data_dir.join(IDENTITY_FILE);
    let text = fs::read_to_string(&path).map_err(|e| AgentError::Identity(missing(&path, &e)))?;
    let identity: Identity = serde_json::from_str(&text)
        .map_err(|e| AgentError::Identity(format!("{} is malformed: {e}", path.display())))?;
    if identity.node_id.is_empty()
        || identity.cluster_id.is_empty()
        || identity.primary_url.is_empty()
        || identity.fingerprint.is_empty()
        || identity.cluster_public_key.is_empty()
        || identity.cluster_key_id.is_empty()
    {
        return Err(AgentError::Identity(format!(
            "{} has an empty required member",
            path.display()
        )));
    }
    identity.verifying_key()?;
    Ok(identity)
}

/// Writes `identity.json` `0600`, replacing the previous one atomically.
///
/// # Errors
///
/// [`AgentError::Io`] or [`AgentError::Identity`] when serialization or the
/// write fails.
pub fn save_identity(data_dir: &Path, identity: &Identity) -> Result<(), AgentError> {
    let text = serde_json::to_vec_pretty(identity)
        .map_err(|e| AgentError::Identity(format!("could not serialize identity: {e}")))?;
    write_private(data_dir, IDENTITY_FILE, &text)
}

/// Writes `bytes` to `data_dir/name` through a temporary file in the same
/// directory, `0600`, then renames it into place.
///
/// # Errors
///
/// [`AgentError::Io`] when the temporary file, its mode, the write or the
/// rename fails.
pub fn write_private(data_dir: &Path, name: &str, bytes: &[u8]) -> Result<(), AgentError> {
    ensure_data_dir(data_dir)?;
    let temp = data_dir.join(format!(
        ".{name}.{}.tmp",
        util::hex(&util::random_bytes::<8>())
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let write = || -> std::io::Result<()> {
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, data_dir.join(name))
    };
    let result = write();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(|e| AgentError::io("write identity file", e))
}

/// The in-process identity: the current pin plus the path it is persisted to.
///
/// `signing_key_rotate` replaces the key through [`IdentityHandle::rotate`], so
/// later `desired_state` messages are verified against the new key without a
/// restart.
#[derive(Clone)]
pub struct IdentityHandle {
    data_dir: PathBuf,
    inner: Arc<Mutex<Identity>>,
}

impl IdentityHandle {
    /// Loads the enrolled identity from `data_dir`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the node is not enrolled or the files are
    /// malformed.
    pub fn load(data_dir: &Path) -> Result<Self, AgentError> {
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            inner: Arc::new(Mutex::new(load_identity(data_dir)?)),
        })
    }

    /// A copy of the current identity.
    #[must_use]
    pub fn get(&self) -> Identity {
        self.lock().clone()
    }

    /// The session credential from `credential`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the file is missing or empty.
    pub fn credential(&self) -> Result<String, AgentError> {
        read_credential(&self.data_dir)
    }

    /// The pinned cluster key.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the stored key is malformed.
    pub fn verifying_key(&self) -> Result<VerifyingKey, AgentError> {
        self.lock().verifying_key()
    }

    /// Persists a new cluster signing key from a verified `signing_key_rotate`
    /// (section 5.21) and starts using it immediately.
    ///
    /// The `key_id` is recomputed from the key rather than trusted from the
    /// message, so the stored id always names the stored key.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when the key is malformed, [`AgentError::Io`]
    /// when `identity.json` cannot be written.
    pub fn rotate(&self, public_key: &str) -> Result<Identity, AgentError> {
        let key_id = Identity::key_id_for(public_key)?;
        let mut identity = self.lock();
        let mut updated = identity.clone();
        updated.cluster_public_key = public_key.to_owned();
        updated.cluster_key_id = key_id;
        updated.verifying_key()?;
        save_identity(&self.data_dir, &updated)?;
        *identity = updated.clone();
        Ok(updated)
    }

    fn lock(&self) -> MutexGuard<'_, Identity> {
        // A panic before a write could poison the lock; the identity is still
        // complete, so recover rather than propagate.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The message for a missing identity file, which is the common case on a node
/// that was never enrolled.
fn missing(path: &Path, error: &std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        format!("{} is missing; the node is not enrolled", path.display())
    } else {
        format!("could not read {}: {error}", path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_id_is_the_sha256_of_the_raw_key() {
        let key = sign::signing_key_from_seed(&[9; 32]);
        let public = sign::public_key_to_base64(&key.verifying_key());
        let id = Identity::key_id_for(&public).unwrap();
        assert_eq!(id.len(), 64);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
        assert_eq!(
            id,
            util::hex(&Sha256::digest(key.verifying_key().as_bytes()))
        );
    }
}
