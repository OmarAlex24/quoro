//! The keys of step 6 of `quoro cluster promote` (`node-agent-protocol.md`
//! section 7.3): "It opens its seals and writes the keys where a primary
//! keeps them". [`install_keys`] writes the cluster keys that `keys::open`
//! opened from this machine's custody seals (section 7.2) into the control
//! plane's data directory, each file as a primary's first start writes it
//! and as its reader reads it:
//!
//! | File | What it holds | Read by |
//! | --- | --- | --- |
//! | `keys/signing.key` | The 32-byte Ed25519 seed of the current signing key, raw | `quoro_control_plane::keys::ClusterKeys::load_or_generate` |
//! | `keys/data.key` | 48 raw bytes: the 16-byte key id, whose 32 lowercase hex characters are the seal's `key_id`, then the 32-byte key | `quoro_control_plane::crypto::DataKey::load_or_create` |
//! | `tls/cert.pem` | The certificate chain, the PEM text the `tls` seal holds | `crate::tls::load_existing` (and `load_or_generate`) |
//! | `tls/key.pem` | The private key, the PEM text the `tls` seal holds | `crate::tls::load_existing` (and `load_or_generate`) |
//!
//! These are the four key files `prepare::PRIMARY_FILES` requires before
//! the control plane starts. The command runs this as root after it has
//! written the database (steps 4 and 5) and before `prepare::prepare_host`,
//! which then hands the data directory to `quoro-api`.
//!
//! Before it writes anything, it refuses, listing every problem:
//!
//! - a signing key in use other than the one `keys/signing.key` holds,
//!   which is the key of the `current` row of `signing_keys` or, when the
//!   followed log holds no `current` row, the one signing key in use. During
//!   a rotation the next key is in use too (section 7.5), but the control
//!   plane of this binary reads one signing key and keeps no file for
//!   another, so a primary promoted then would lose it;
//! - a signing seed whose public key is not the key its id names, and a
//!   data key id that is not 32 lowercase hex characters;
//! - a TLS pair the listener cannot load, or whose certificate's
//!   fingerprint is not the one this machine pins: nodes would refuse a
//!   primary that serves it;
//! - a data directory that is missing, not a directory or a symbolic link;
//!   a `keys/` or `tls/` that is a symbolic link or not a directory; and a
//!   key file that is a symbolic link, is not a regular file, or holds
//!   anything other than what would be written. A key file is never
//!   overwritten.
//!
//! A key file that already holds exactly what would be written is kept, so
//! a run stopped part-way can be run again. `keys/` and `tls/` are created
//! `0700`, or made `0700`. Each new file is written under a partial name
//! next to it, `.<name>.promote-partial`, created exclusively `0600`,
//! written and synced, then renamed into place without replacing any file
//! ([`KeyFs`]), and its directory synced. A kept file is made `0600`. A
//! partial file a stopped run left is removed first. Modes are set through
//! descriptors opened without following a link.
//!
//! The rename into place goes through [`KeyFs`], so a test can stop a run
//! between two files. Nothing here needs root: the files are the running
//! process's, which is root's when the command runs.

use std::fs;
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use quoro_control_plane::custody::TlsIdentity;
use quoro_control_plane::store::sha256_hex;
use quoro_proto::custody::KeyKind;
use quoro_proto::sign;
use rustix::fs::{Mode, OFlags};
use zeroize::Zeroizing;

use super::keys::{Opened, TlsPair};
use crate::host::Out;

/// The directory of the signing and data-encryption keys.
const KEYS_DIR: &str = "keys";
/// The directory of the TLS pair.
const TLS_DIR: &str = "tls";
/// The signing key, as `quoro_control_plane::keys` reads it.
pub(crate) const SIGNING_FILE: &str = "keys/signing.key";
/// The data-encryption key, as `quoro_control_plane::crypto` reads it.
pub(crate) const DATA_FILE: &str = "keys/data.key";
/// The TLS certificate chain, as `crate::tls` reads it.
pub(crate) const CERT_FILE: &str = "tls/cert.pem";
/// The TLS private key, as `crate::tls` reads it.
pub(crate) const KEY_FILE: &str = "tls/key.pem";
/// The length of `keys/data.key`: the 16-byte key id, then the key.
const DATA_FILE_LEN: usize = 16 + 32;
/// The mode of each key file.
const FILE_MODE: u32 = 0o600;
/// The mode of `keys/` and `tls/`.
const DIR_MODE: u32 = 0o700;
/// The prefix of a pinned fingerprint.
const FINGERPRINT_PREFIX: &str = "sha256:";
/// What every refusal ends with.
const UNCHANGED: &str = "nothing was changed";

/// A signing key opened from its seal.
pub(crate) struct SigningSeed {
    /// The key id its seal names: the lowercase hex SHA-256 of its 32-byte
    /// public key.
    pub(crate) key_id: String,
    /// The 32-byte Ed25519 seed, wiped when dropped.
    pub(crate) seed: Zeroizing<[u8; 32]>,
}

/// The data-encryption key opened from its seal.
pub(crate) struct DataKeyBytes {
    /// The 32 lowercase hex characters of its 16-byte key id.
    pub(crate) key_id: String,
    /// The 32-byte key, wiped when dropped.
    pub(crate) key: Zeroizing<[u8; 32]>,
}

/// The keys a promoted primary keeps, opened from this machine's seals.
pub(crate) struct PrimaryKeys {
    /// Every signing key in use: the identity's and every `signing_keys`
    /// row's.
    pub(crate) signing: Vec<SigningSeed>,
    /// The key id of the `current` row of `signing_keys` in the followed
    /// log, when it holds one.
    pub(crate) current_signing: Option<String>,
    /// The one data-encryption key in use.
    pub(crate) data: DataKeyBytes,
    /// The TLS certificate chain and private key.
    pub(crate) tls: TlsIdentity,
}

impl PrimaryKeys {
    /// The keys `keys::open` opened, from what each seal holds.
    ///
    /// # Errors
    ///
    /// A message when a seal does not hold a key of its kind, or when the
    /// opened keys are not one data key and one TLS key.
    pub(crate) fn from_opened(opened: Opened) -> Result<Self, String> {
        let mut signing = Vec::new();
        let mut data = None;
        let mut tls = None;
        for key in opened.keys {
            match key.kind {
                KeyKind::Signing => {
                    let seed = thirty_two(&key.plaintext).ok_or_else(|| {
                        format!(
                            "the signing key {}: its seal does not hold a 32-byte seed",
                            key.key_id
                        )
                    })?;
                    signing.push(SigningSeed {
                        key_id: key.key_id,
                        seed,
                    });
                }
                KeyKind::Dek => {
                    if data.is_some() {
                        return Err("more than one data-encryption key was opened, and a \
                                    primary keeps exactly one"
                            .to_string());
                    }
                    let bytes = thirty_two(&key.plaintext).ok_or_else(|| {
                        format!(
                            "the data-encryption key {}: its seal does not hold a 32-byte key",
                            key.key_id
                        )
                    })?;
                    data = Some(DataKeyBytes {
                        key_id: key.key_id,
                        key: bytes,
                    });
                }
                KeyKind::Tls => {
                    if tls.is_some() {
                        return Err("more than one TLS key was opened".to_string());
                    }
                    let pair: TlsPair = serde_json::from_slice(&key.plaintext).map_err(|_| {
                        format!(
                            "the TLS key {}: its seal does not hold a certificate and a key",
                            key.key_id
                        )
                    })?;
                    let identity = TlsIdentity::from_pem(pair.cert_pem, pair.key_pem)
                        .map_err(|e| format!("the TLS key {}: {e}", key.key_id))?;
                    tls = Some(identity);
                }
            }
        }
        Ok(Self {
            signing,
            current_signing: opened.current_signing,
            data: data.ok_or("no data-encryption key was opened")?,
            tls: tls.ok_or("no TLS key was opened")?,
        })
    }
}

/// What [`install_keys`] did with each key file, named as under the data
/// directory.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Installed {
    /// The files it wrote.
    pub(crate) written: Vec<&'static str>,
    /// The files that already held what it would write, kept.
    pub(crate) kept: Vec<&'static str>,
}

/// The rename that puts a key file in place. A test replaces it to stop a
/// run between two files.
pub(crate) trait KeyFs {
    /// Renames `from` to `to`, failing with `AlreadyExists` when `to`
    /// exists: no key file is ever replaced, even one that appears after the
    /// checks.
    ///
    /// # Errors
    ///
    /// The error of the rename.
    fn rename_new(&self, from: &Path, to: &Path) -> io::Result<()>;
}

/// The real rename.
pub(crate) struct RealKeyFs;

impl KeyFs for RealKeyFs {
    fn rename_new(&self, from: &Path, to: &Path) -> io::Result<()> {
        rename_noreplace(from, to)
    }
}

/// `renameat2` with `RENAME_NOREPLACE`.
#[cfg(target_os = "linux")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE).map_err(io::Error::from)
}

/// Elsewhere, for a build that compiles: the command runs on Linux only.
#[cfg(not(target_os = "linux"))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    if fs::symlink_metadata(to).is_ok() {
        return Err(io::Error::from(io::ErrorKind::AlreadyExists));
    }
    fs::rename(from, to)
}

/// A key file and what it must hold.
struct KeyFile {
    /// Its directory under the data directory.
    dir: &'static str,
    /// Its path under the data directory.
    name: &'static str,
    /// What it holds, wiped when dropped.
    bytes: Zeroizing<Vec<u8>>,
}

/// What the writes do with a key file.
#[derive(Clone, Copy)]
enum Action {
    /// There is none: write it.
    Write,
    /// It already holds what would be written: keep it.
    Keep,
}

/// Writes `keys` into `data_dir` where a primary keeps them (module
/// documentation). `pinned` is the fingerprint this machine pins,
/// `sha256:<hex>`. Every check runs before anything is written.
///
/// # Errors
///
/// A refusal that lists every problem and says that nothing was changed
/// (module documentation), or a failed write, after which a rerun goes on
/// where this run stopped.
pub(crate) fn install_keys(
    keys: &PrimaryKeys,
    pinned: &str,
    data_dir: &Path,
    fs_calls: &dyn KeyFs,
    out: &mut Out,
) -> Result<Installed, String> {
    let files = key_files(keys, pinned).map_err(|problems| refusal(&problems))?;
    let actions = check_tree(data_dir, &files).map_err(|problems| refusal(&problems))?;

    let mut created = false;
    for dir in [KEYS_DIR, TLS_DIR] {
        let path = data_dir.join(dir);
        if private_dir(&path)? {
            created = true;
            out.say(format!("created {} (0700)", path.display()));
        }
    }
    if created {
        sync_dir(data_dir)?;
    }
    let mut installed = Installed::default();
    for (file, action) in files.iter().zip(actions) {
        let path = data_dir.join(file.name);
        let partial = partial_of(&path);
        remove_if_present(&partial)?;
        match action {
            Action::Keep => {
                if keep_private(&path)? {
                    out.say(format!(
                        "kept {}, which already holds this key, and made it 0600",
                        path.display()
                    ));
                } else {
                    out.say(format!(
                        "kept {}, which already holds this key (0600)",
                        path.display()
                    ));
                }
                installed.kept.push(file.name);
            }
            Action::Write => {
                let dir = data_dir.join(file.dir);
                place(fs_calls, &partial, &path, &dir, &file.bytes)?;
                out.say(format!("wrote {} (0600)", path.display()));
                installed.written.push(file.name);
            }
        }
    }
    Ok(installed)
}

/// The refusal for `problems`.
fn refusal(problems: &[String]) -> String {
    format!("{}; {UNCHANGED}", problems.join("; "))
}

/// The four key files and what each holds, in the order they are written.
///
/// # Errors
///
/// Every problem with the keys (module documentation).
fn key_files(keys: &PrimaryKeys, pinned: &str) -> Result<Vec<KeyFile>, Vec<String>> {
    let mut problems = Vec::new();
    let seed = signing_seed(keys, &mut problems);
    let data = match data_key_bytes(&keys.data) {
        Ok(bytes) => Some(bytes),
        Err(problem) => {
            problems.push(problem);
            None
        }
    };
    if let Err(problem) = check_tls(&keys.tls, pinned) {
        problems.push(problem);
    }
    match (seed, data) {
        (Some(seed), Some(data)) if problems.is_empty() => Ok(vec![
            KeyFile {
                dir: KEYS_DIR,
                name: SIGNING_FILE,
                bytes: Zeroizing::new(seed.to_vec()),
            },
            KeyFile {
                dir: KEYS_DIR,
                name: DATA_FILE,
                bytes: data,
            },
            KeyFile {
                dir: TLS_DIR,
                name: CERT_FILE,
                bytes: Zeroizing::new(keys.tls.cert_pem().as_bytes().to_vec()),
            },
            KeyFile {
                dir: TLS_DIR,
                name: KEY_FILE,
                bytes: Zeroizing::new(keys.tls.key_pem().as_bytes().to_vec()),
            },
        ]),
        _ => Err(problems),
    }
}

/// The seed `keys/signing.key` holds: the key of the `current` row of
/// `signing_keys` or, when the followed log holds no `current` row, the one
/// signing key in use. A seed that is not the key its id names, and every
/// other signing key in use, is pushed to `problems`, and then there is
/// none. `None` always comes with a problem.
fn signing_seed<'k>(
    keys: &'k PrimaryKeys,
    problems: &mut Vec<String>,
) -> Option<&'k Zeroizing<[u8; 32]>> {
    let before = problems.len();
    for key in &keys.signing {
        let opened = signing_key_id(&key.seed);
        if opened != key.key_id {
            problems.push(format!(
                "the signing key {}: its seed is the key {opened}",
                key.key_id
            ));
        }
    }
    let current = match &keys.current_signing {
        Some(current) => keys.signing.iter().find(|key| key.key_id == *current),
        None => match keys.signing.as_slice() {
            [one] => Some(one),
            _ => None,
        },
    };
    let Some(current) = current else {
        problems.push(match &keys.current_signing {
            Some(id) => {
                format!("the current signing key {id} was not opened from this machine's seals")
            }
            None => format!(
                "the followed log holds no current signing key, and {} signing keys are in use",
                keys.signing.len()
            ),
        });
        return None;
    };
    for key in &keys.signing {
        if key.key_id != current.key_id {
            problems.push(format!(
                "the signing key {} is in use beside the current key {}, as the next key of a \
                 rotation (section 7.5); the control plane of this binary reads one signing key, \
                 {SIGNING_FILE}, and keeps no file for another, so a primary promoted now would \
                 lose it",
                key.key_id, current.key_id
            ));
        }
    }
    (problems.len() == before).then_some(&current.seed)
}

/// The key id of the signing key of `seed`: the lowercase hex SHA-256 of
/// its 32-byte public key, as `quoro_control_plane::keys` computes it.
fn signing_key_id(seed: &[u8; 32]) -> String {
    sha256_hex(sign::signing_key_from_seed(seed).verifying_key().as_bytes())
}

/// What `keys/data.key` holds: the key id's 16 bytes, then the key.
///
/// # Errors
///
/// A key id that is not 32 lowercase hex characters.
fn data_key_bytes(data: &DataKeyBytes) -> Result<Zeroizing<Vec<u8>>, String> {
    let key_id = key_id_bytes(&data.key_id).ok_or_else(|| {
        format!(
            "the data-encryption key {}: its key id is not 32 lowercase hex characters",
            data.key_id
        )
    })?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(DATA_FILE_LEN));
    bytes.extend_from_slice(&key_id);
    bytes.extend_from_slice(data.key.as_slice());
    Ok(bytes)
}

/// The 16 bytes of a data key id: 32 lowercase hex characters.
fn key_id_bytes(text: &str) -> Option<[u8; 16]> {
    let digits = text.as_bytes();
    if digits.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (byte, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(bytes)
}

/// The value of a lowercase hex digit.
fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

/// `plaintext` as a 32-byte key, wiped when dropped.
fn thirty_two(plaintext: &[u8]) -> Option<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0u8; 32]);
    if plaintext.len() != key.len() {
        return None;
    }
    key.copy_from_slice(plaintext);
    Some(key)
}

/// Checks that the listener can load the TLS pair, as `crate::tls` loads
/// the files, and that its certificate is the one `pinned` names.
///
/// # Errors
///
/// A pair the listener cannot load, or another certificate.
fn check_tls(tls: &TlsIdentity, pinned: &str) -> Result<(), String> {
    let material = crate::tls::material_from_pem(tls.cert_pem(), tls.key_pem()).map_err(|e| {
        format!(
            "the TLS key {}: the listener cannot load its certificate and key ({e})",
            tls.key_id()
        )
    })?;
    let served = bare_fingerprint(&material.fingerprint);
    let pin = bare_fingerprint(pinned);
    if served == pin {
        Ok(())
    } else {
        Err(format!(
            "the TLS certificate's fingerprint is {FINGERPRINT_PREFIX}{served}, but this machine \
             pins {FINGERPRINT_PREFIX}{pin}: nodes would refuse a primary that serves it"
        ))
    }
}

/// A fingerprint's lowercase hex, without its `sha256:` prefix.
fn bare_fingerprint(fingerprint: &str) -> String {
    fingerprint
        .strip_prefix(FINGERPRINT_PREFIX)
        .unwrap_or(fingerprint)
        .to_ascii_lowercase()
}

/// Checks `data_dir` and what it holds before anything is written, and
/// returns what to do with each of `files`.
///
/// # Errors
///
/// Every problem found (module documentation). A data directory or a
/// `keys/` or `tls/` that cannot be used stops the checks there.
fn check_tree(data_dir: &Path, files: &[KeyFile]) -> Result<Vec<Action>, Vec<String>> {
    let shown = data_dir.display();
    match fs::symlink_metadata(data_dir) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(metadata) if metadata.is_symlink() => {
            return Err(vec![format!(
                "the data directory {shown} is a symbolic link, and the keys are never written \
                 through a link"
            )]);
        }
        Ok(_) => {
            return Err(vec![format!(
                "the data directory {shown} is not a directory"
            )]);
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(vec![format!(
                "the data directory {shown} does not exist; the promotion writes the database \
                 there before the keys"
            )]);
        }
        Err(e) => return Err(vec![format!("cannot read {shown}: {e}")]),
    }
    let mut problems = Vec::new();
    for dir in [KEYS_DIR, TLS_DIR] {
        let path = data_dir.join(dir);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(metadata) if metadata.is_symlink() => problems.push(format!(
                "{} is a symbolic link, and the keys are never written through a link",
                path.display()
            )),
            Ok(_) => problems.push(format!("{} exists and is not a directory", path.display())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => problems.push(format!("cannot read {}: {e}", path.display())),
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let mut actions = Vec::with_capacity(files.len());
    for file in files {
        let path = data_dir.join(file.name);
        match existing(&path) {
            Ok(None) => actions.push(Action::Write),
            Ok(Some(bytes)) if bytes.as_slice() == file.bytes.as_slice() => {
                actions.push(Action::Keep);
            }
            Ok(Some(_)) => problems.push(format!(
                "{} exists and holds something else, and a key file is never overwritten: move \
                 it away if it is stale and run this again",
                path.display()
            )),
            Err(problem) => problems.push(problem),
        }
        let partial = partial_of(&path);
        if fs::symlink_metadata(&partial).is_ok_and(|metadata| metadata.is_dir()) {
            problems.push(format!(
                "{} is a directory, where {} is written before it is renamed into place",
                partial.display(),
                file.name
            ));
        }
    }
    if problems.is_empty() {
        Ok(actions)
    } else {
        Err(problems)
    }
}

/// What the key file at `path` holds, or `None` when there is none.
///
/// # Errors
///
/// A symbolic link, a name that is not a regular file, or a read that
/// failed.
fn existing(path: &Path) -> Result<Option<Zeroizing<Vec<u8>>>, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    if metadata.is_symlink() {
        return Err(format!(
            "{} is a symbolic link, and a key file is never written or read through a link",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!(
            "{} exists and is not a regular file",
            path.display()
        ));
    }
    let unreadable = |e: io::Error| format!("cannot read {}: {e}", path.display());
    let mut file = open_no_follow(path, OFlags::empty()).map_err(unreadable)?;
    let capacity = usize::try_from(metadata.len())
        .unwrap_or(0)
        .saturating_add(1);
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    file.read_to_end(&mut bytes).map_err(unreadable)?;
    Ok(Some(bytes))
}

/// Opens `path` read-only with `flags` added, without following a link at
/// its last component and without blocking on a FIFO.
fn open_no_follow(path: &Path, flags: OFlags) -> io::Result<fs::File> {
    let flags = flags | OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
    Ok(fs::File::from(rustix::fs::open(
        path,
        flags,
        Mode::empty(),
    )?))
}

/// Creates the directory `path` `0700` when it is missing, and makes it
/// `0700` through a descriptor opened without following a link. Returns
/// whether it created it.
///
/// # Errors
///
/// A failed creation, open or mode change.
fn private_dir(path: &Path) -> Result<bool, String> {
    let created = match fs::DirBuilder::new().mode(DIR_MODE).create(path) {
        Ok(()) => true,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
        Err(e) => return Err(format!("cannot create {}: {e}", path.display())),
    };
    let dir = open_no_follow(path, OFlags::DIRECTORY).map_err(|e| {
        format!(
            "cannot open {} as a directory without following a link: {e}",
            path.display()
        )
    })?;
    dir.set_permissions(fs::Permissions::from_mode(DIR_MODE))
        .map_err(|e| format!("cannot make {} 0700: {e}", path.display()))?;
    Ok(created)
}

/// Makes the kept key file at `path` `0600` when it is not, through a
/// descriptor opened without following a link. Returns whether its mode
/// changed.
///
/// # Errors
///
/// A failed open or mode change, or a name that is no longer a regular
/// file.
fn keep_private(path: &Path) -> Result<bool, String> {
    let file = open_no_follow(path, OFlags::empty())
        .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    if metadata.permissions().mode() & 0o7777 == FILE_MODE {
        return Ok(false);
    }
    file.set_permissions(fs::Permissions::from_mode(FILE_MODE))
        .map_err(|e| format!("cannot make {} 0600: {e}", path.display()))?;
    Ok(true)
}

/// The partial name a key file is written under: `.<name>.promote-partial`
/// next to it.
fn partial_of(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.promote-partial"))
}

/// Writes `bytes` under `partial`, created exclusively `0600`, synced, then
/// renamed to `target` without replacing anything, and syncs `dir`. The
/// partial file is removed when the write or the rename fails.
///
/// # Errors
///
/// A failed write, rename or sync, or a `target` that appeared after the
/// checks.
fn place(
    fs_calls: &dyn KeyFs,
    partial: &Path,
    target: &Path,
    dir: &Path,
    bytes: &[u8],
) -> Result<(), String> {
    if let Err(e) = write_partial(partial, bytes) {
        let _ = fs::remove_file(partial);
        return Err(format!("cannot write {}: {e}", partial.display()));
    }
    if let Err(e) = fs_calls.rename_new(partial, target) {
        let _ = fs::remove_file(partial);
        return Err(if e.kind() == io::ErrorKind::AlreadyExists {
            format!(
                "{} appeared while the keys were written, and a key file is never replaced; run \
                 this again to compare it",
                target.display()
            )
        } else {
            format!(
                "cannot rename {} to {}: {e}",
                partial.display(),
                target.display()
            )
        });
    }
    sync_dir(dir)
}

/// Creates `path` exclusively `0600`, writes `bytes` and syncs it.
fn write_partial(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(FILE_MODE))?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Removes `path` when it exists.
fn remove_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("cannot remove {}: {e}", path.display())),
    }
}

/// Syncs a directory, so a new entry or a rename in it is durable.
fn sync_dir(dir: &Path) -> Result<(), String> {
    open_no_follow(dir, OFlags::DIRECTORY)
        .and_then(|opened| opened.sync_all())
        .map_err(|e| format!("cannot sync {}: {e}", dir.display()))
}

#[cfg(test)]
mod tests;
