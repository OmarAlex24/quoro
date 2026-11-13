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
//! - `log-mark` — on a machine with the `node` role, the change-log mark: the
//!   highest `seq` the node has acknowledged (section 7.1, section 12, ADR
//!   0261), read and written by [`crate::follow`].
//! - `custody.key` — on a machine with the `node` role, the X25519 custody
//!   private key the primary seals the cluster keys to (section 7.2, ADR
//!   0180). It never leaves the machine; `hello` and the enroll request carry
//!   its public half. It is written once and linked into place rather than
//!   renamed, so it never replaces a file. See [`load_or_create_custody_key`].
//!
//! [`IdentityHandle`] is the in-process view the session keeps: it answers with
//! the current pin and can persist a `signing_key_rotate` atomically.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use quoro_proto::custody::{self, CustodyKey, Zeroizing};
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
/// File name of the change-log mark: the decimal `seq` of the highest entry
/// the node has acknowledged, mode `0600` (section 7.1 "The node's mark",
/// section 12, ADR 0261). It sits next to [`SEED_FILE`] and
/// [`CREDENTIAL_FILE`], where `custody.key` joins them (section 7.2), so it
/// survives a lost `node.db`. [`crate::follow::read_mark`] reads it.
pub const LOG_MARK_FILE: &str = "log-mark";
/// File name of the custody private key on a machine with the `node` role:
/// the 32 raw bytes of an X25519 private key, in the format of
/// [`SEED_FILE`], mode `0600` (section 7.2, section 12, ADR 0180). It sits
/// next to the credential. [`load_or_create_custody_key`] reads it and
/// writes it once.
pub const CUSTODY_KEY_FILE: &str = "custody.key";

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
    /// Other base URLs the primary advertised in `welcome.primary_urls`
    /// (section 5.10), tried in order when `primary_url` fails to dial.
    /// Older identity files do not have it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_urls: Vec<String>,
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

/// Loads the custody key from [`CUSTODY_KEY_FILE`], or generates one and
/// writes it `0600` when the file is missing (section 7.2). The machine
/// calls it when it enrolls with the `node` role and before every `hello`
/// it sends with that role, so a machine upgraded to M5b gets its key
/// before its first `hello` reports it.
///
/// A key that exists is never replaced: the primary binds the first key a
/// machine reports and refuses another (ADR 0231). A file that exists but
/// cannot be read or is not exactly 32 bytes is therefore an error, and the
/// file is left as it is. A new key never replaces a file either, even one
/// that appears while it is written: it is linked into place, which fails
/// when the file exists, and the file that won is read and used.
///
/// # Errors
///
/// [`AgentError::Identity`] when the file cannot be read or is not 32
/// bytes; [`AgentError::Io`] when a new key cannot be written.
pub fn load_or_create_custody_key(data_dir: &Path) -> Result<CustodyKey, AgentError> {
    let path = data_dir.join(CUSTODY_KEY_FILE);
    match fs::read(&path) {
        Ok(bytes) => parse_custody_key(&path, bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_custody_key(data_dir),
        Err(error) => Err(unreadable_custody_key(&path, &error)),
    }
}

/// Loads the custody key from [`CUSTODY_KEY_FILE`] and never creates one.
/// `quoro cluster promote` opens its seals with the key the agent made
/// (section 7.3): a machine without the file holds no seal it could open, and
/// a key made then would be one the primary never bound (ADR 0231).
///
/// # Errors
///
/// [`AgentError::Identity`] when the file is missing, cannot be read or is
/// not 32 bytes. Nothing is written in any case.
pub fn load_custody_key(data_dir: &Path) -> Result<CustodyKey, AgentError> {
    let path = data_dir.join(CUSTODY_KEY_FILE);
    match fs::read(&path) {
        Ok(bytes) => parse_custody_key(&path, bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(AgentError::Identity(format!(
                "{} is missing; this machine has no custody key",
                path.display()
            )))
        }
        Err(error) => Err(unreadable_custody_key(&path, &error)),
    }
}

/// Generates a custody key and writes it to [`CUSTODY_KEY_FILE`] only if no
/// file is there ([`write_private_new`]). When one is, it was created after
/// [`load_or_create_custody_key`] found none: it is read once and used, and
/// the generated key is dropped.
///
/// # Errors
///
/// As [`load_or_create_custody_key`].
fn create_custody_key(data_dir: &Path) -> Result<CustodyKey, AgentError> {
    let key = CustodyKey::generate(&mut custody::os_rng());
    if write_private_new(data_dir, CUSTODY_KEY_FILE, &key.to_bytes()[..])? {
        return Ok(key);
    }
    let path = data_dir.join(CUSTODY_KEY_FILE);
    let bytes = fs::read(&path).map_err(|error| unreadable_custody_key(&path, &error))?;
    parse_custody_key(&path, bytes)
}

/// Parses the bytes of `custody.key`, naming the file when they are not a
/// key.
fn parse_custody_key(path: &Path, bytes: Vec<u8>) -> Result<CustodyKey, AgentError> {
    let bytes = Zeroizing::new(bytes);
    CustodyKey::from_bytes(&bytes)
        .map_err(|_| AgentError::Identity(format!("{} is not 32 bytes", path.display())))
}

fn unreadable_custody_key(path: &Path, error: &std::io::Error) -> AgentError {
    AgentError::Identity(format!("could not read {}: {error}", path.display()))
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
/// directory, `0600`, then renames it into place, durably: the file is synced
/// before the rename and the directory after it, so once this returns the new
/// contents survive a crash. The change-log mark depends on it (section 7.1,
/// ADR 0261).
///
/// # Errors
///
/// [`AgentError::Io`] when the temporary file, its mode, the write, the
/// rename or the directory sync fails.
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
        fs::rename(&temp, data_dir.join(name))?;
        // The rename is a change to the directory: without this sync a crash
        // can bring back the old file, or none.
        fs::File::open(data_dir)?.sync_all()
    };
    let result = write();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(|e| AgentError::io("write identity file", e))
}

/// Writes `bytes` to `data_dir/name`, `0600`, only when no file of that name
/// exists, so it never replaces one, even one created while it runs. Like
/// [`write_private`] it writes and syncs a temporary file first, but it
/// hard-links it to `name`, which fails when `name` exists, instead of
/// renaming it over `name`. The temporary file is then removed and the
/// directory synced, so a new file survives a crash.
///
/// Returns `false`, having written nothing, when `name` already exists.
///
/// # Errors
///
/// [`AgentError::Io`] when the temporary file, its mode, the write, the
/// link, the removal or the directory sync fails.
fn write_private_new(data_dir: &Path, name: &str, bytes: &[u8]) -> Result<bool, AgentError> {
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
    let write = || -> std::io::Result<bool> {
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&temp, data_dir.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(error) => return Err(error),
        }
        fs::remove_file(&temp)?;
        // The link and the removal change the directory: without this sync
        // a crash can lose the new file.
        fs::File::open(data_dir)?.sync_all()?;
        Ok(true)
    };
    let result = write();
    // Gone already after a link; removed here on every other path.
    let _ = fs::remove_file(&temp);
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

    /// Stores the base URLs a `welcome` advertised (section 5.10).
    ///
    /// Entries that are not `https://` URLs are dropped, as are duplicates; at
    /// most `MAX_PRIMARY_URLS` are kept. The entry equal to the current
    /// [`Identity::primary_url`] is kept too: when `primary_url` is later
    /// changed to an address that does not answer (by hand, or by
    /// `quoro node set-primary` to a wrong host that happens to hold the key),
    /// the address that worked is still in the list. The dial loop skips it
    /// while it is the primary URL. The file is written only when the result
    /// differs from the stored list.
    ///
    /// # Errors
    ///
    /// [`AgentError::Io`] when `identity.json` cannot be written.
    pub fn set_primary_urls(&self, urls: &[String]) -> Result<bool, AgentError> {
        let mut identity = self.lock();
        let mut kept: Vec<String> = Vec::new();
        for url in urls {
            let Some(url) = normalize_primary_url(url) else {
                continue;
            };
            if kept.contains(&url) {
                continue;
            }
            kept.push(url);
            if kept.len() == quoro_proto::wire::MAX_PRIMARY_URLS {
                break;
            }
        }
        if kept == identity.primary_urls {
            return Ok(false);
        }
        let mut updated = identity.clone();
        updated.primary_urls = kept;
        save_identity(&self.data_dir, &updated)?;
        *identity = updated;
        Ok(true)
    }

    /// Makes `url` the address the node dials (section 5.10) after a session
    /// reached it through `welcome.primary_urls`.
    ///
    /// The previous [`Identity::primary_url`] goes to the front of
    /// `primary_urls`, so the old address is still tried when the new one
    /// fails; `url` itself leaves the list.
    ///
    /// # Errors
    ///
    /// [`AgentError::Identity`] when `url` is not an `https://` URL,
    /// [`AgentError::Io`] when `identity.json` cannot be written.
    pub fn promote(&self, url: &str) -> Result<(), AgentError> {
        let url = normalize_primary_url(url)
            .ok_or_else(|| AgentError::Identity(format!("{url:?} is not an https primary URL")))?;
        let mut identity = self.lock();
        if identity.primary_url == url {
            return Ok(());
        }
        let mut urls = Vec::with_capacity(identity.primary_urls.len() + 1);
        urls.push(identity.primary_url.clone());
        for stored in &identity.primary_urls {
            if stored != &url && !urls.contains(stored) {
                urls.push(stored.clone());
            }
        }
        urls.truncate(quoro_proto::wire::MAX_PRIMARY_URLS);
        let mut updated = identity.clone();
        updated.primary_url = url;
        updated.primary_urls = urls;
        save_identity(&self.data_dir, &updated)?;
        *identity = updated;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Identity> {
        // A panic before a write could poison the lock; the identity is still
        // complete, so recover rather than propagate.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Normalizes one `welcome.primary_urls` entry to an `https://<host>:<port>`
/// base URL with no path, or `None` when it is not an `https://` URL.
fn normalize_primary_url(url: &str) -> Option<String> {
    if !url.starts_with("https://") {
        return None;
    }
    let endpoint = crate::tls::Endpoint::parse(url).ok()?;
    Some(if endpoint.host.contains(':') {
        format!("https://[{}]:{}", endpoint.host, endpoint.port)
    } else {
        format!("https://{}:{}", endpoint.host, endpoint.port)
    })
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

    /// `write_private` replaces the file through a temporary one, `0600`, and
    /// leaves no temporary file behind, also when it replaces an existing
    /// file.
    #[test]
    fn write_private_leaves_no_temporary_file() {
        let dir = std::env::temp_dir().join(format!(
            "quoro-identity-unit-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        write_private(&dir, "file", b"first").unwrap();
        write_private(&dir, "file", b"second").unwrap();
        let names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["file"]);
        assert_eq!(fs::read(dir.join("file")).unwrap(), b"second");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(dir.join("file")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Section 7.2, ADR 0231: creating the custody key never replaces a
    /// `custody.key` that is already there, as when one appears after the
    /// loader found none. The file is left byte for byte and the key it holds
    /// is returned; a malformed one is left too and refused with the file
    /// named. With no file the new key is written `0600`, and no temporary
    /// file is ever left.
    #[test]
    fn creating_the_custody_key_never_replaces_an_existing_file() {
        let dir = std::env::temp_dir().join(format!(
            "quoro-identity-unit-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        ensure_data_dir(&dir).unwrap();
        let path = dir.join(CUSTODY_KEY_FILE);
        let names = || -> Vec<String> {
            fs::read_dir(&dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        };

        let existing = CustodyKey::generate(&mut custody::os_rng());
        fs::write(&path, &existing.to_bytes()[..]).unwrap();
        let created = create_custody_key(&dir);
        assert_eq!(
            fs::read(&path).unwrap(),
            &existing.to_bytes()[..],
            "custody.key is left byte for byte"
        );
        assert_eq!(
            created.unwrap().public_key(),
            existing.public_key(),
            "the key in the file is the one returned"
        );
        assert_eq!(names(), [CUSTODY_KEY_FILE]);

        fs::write(&path, [7u8; 33]).unwrap();
        let error = create_custody_key(&dir).unwrap_err().to_string();
        assert_eq!(fs::read(&path).unwrap(), [7u8; 33]);
        assert!(error.contains(CUSTODY_KEY_FILE), "{error}");
        assert_eq!(names(), [CUSTODY_KEY_FILE]);

        fs::remove_file(&path).unwrap();
        let new = create_custody_key(&dir).unwrap();
        assert_eq!(fs::read(&path).unwrap(), &new.to_bytes()[..]);
        assert_ne!(new.public_key(), existing.public_key());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "custody.key is root-only");
        }
        assert_eq!(names(), [CUSTODY_KEY_FILE]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Section 7.3: the load-only path reads the key the agent made and
    /// never creates one. A missing file is an error that leaves the
    /// directory as it was; a malformed one is refused with the file named.
    #[test]
    fn loading_the_custody_key_never_creates_one() {
        let dir = std::env::temp_dir().join(format!(
            "quoro-identity-unit-{}",
            util::hex(&util::random_bytes::<8>())
        ));
        ensure_data_dir(&dir).unwrap();
        let path = dir.join(CUSTODY_KEY_FILE);

        let error = load_custody_key(&dir).unwrap_err().to_string();
        assert!(error.contains(CUSTODY_KEY_FILE), "{error}");
        assert!(!path.exists(), "no custody key is created");
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);

        let made = load_or_create_custody_key(&dir).unwrap();
        assert_eq!(
            load_custody_key(&dir).unwrap().public_key(),
            made.public_key()
        );

        fs::write(&path, [7u8; 31]).unwrap();
        let error = load_custody_key(&dir).unwrap_err().to_string();
        assert!(error.contains(CUSTODY_KEY_FILE), "{error}");
        assert_eq!(fs::read(&path).unwrap(), [7u8; 31]);
        let _ = fs::remove_dir_all(&dir);
    }

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
