//! The key installation of `quoro cluster promote` in a temporary
//! directory: each key written as a primary's own first start writes it and
//! read back through the reader a primary uses, the modes, the refusals and
//! a rerun. Nothing needs root.

use super::*;
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt as _, symlink};
use std::sync::atomic::{AtomicU32, Ordering};

use quoro_control_plane::crypto::DataKey;
use quoro_control_plane::keys::{ClusterKeys, SIGNING_KEY_FILE};
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::identity::{self, CUSTODY_KEY_FILE, Identity};
use quoro_proto::custody::{self, CustodyKey, CustodyPublicKey, SealContext};
use serde_json::{Value, json};

use crate::promote::keys::open as open_seals;
use crate::promote::prepare::PRIMARY_FILES;

const NODE_ID: &str = "nod_01JC9Z0000000000000000000A";
const CLUSTER_ID: &str = "cls_01JC9Z0000000000000000000Q";
const TIME: &str = "2026-11-13T10:00:00.000Z";
/// The current signing key's seed.
const SEED: [u8; 32] = [7; 32];
/// The next signing key's seed, during a rotation.
const NEXT_SEED: [u8; 32] = [8; 32];
const DEK_ID: &str = "00112233445566778899aabbccddeeff";
const DEK: [u8; 32] = [9; 32];
/// The four key files, in the order they are written.
const FILES: [&str; 4] = [SIGNING_FILE, DATA_FILE, CERT_FILE, KEY_FILE];

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A directory of its own under the system's temporary directory, removed
/// on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "quoro-test-install-keys-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// The control plane's data directory, as the promotion leaves it
    /// before the keys: it exists.
    fn data_dir(&self) -> PathBuf {
        let dir = self.0.join("quoro");
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A rename that stops the run at `0`.
struct StopAt(PathBuf);

impl KeyFs for StopAt {
    fn rename_new(&self, from: &Path, to: &Path) -> io::Result<()> {
        if to == self.0.as_path() {
            return Err(io::Error::other("the run stopped here"));
        }
        RealKeyFs.rename_new(from, to)
    }
}

/// A self-signed certificate and its key, as `crate::tls` generates them.
fn tls_pair() -> TlsIdentity {
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = rcgen::CertificateParams::new(vec!["node-1".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    TlsIdentity::from_pem(certificate.pem(), key.serialize_pem()).unwrap()
}

fn signing(seed: [u8; 32]) -> SigningSeed {
    SigningSeed {
        key_id: signing_key_id(&seed),
        seed: Zeroizing::new(seed),
    }
}

/// The keys outside a rotation: the current signing key, a data key and a
/// TLS pair.
fn primary_keys() -> PrimaryKeys {
    PrimaryKeys {
        signing: vec![signing(SEED)],
        current_signing: Some(signing_key_id(&SEED)),
        data: DataKeyBytes {
            key_id: DEK_ID.to_string(),
            key: Zeroizing::new(DEK),
        },
        tls: tls_pair(),
    }
}

/// The fingerprint this machine pins: the certificate of `keys`.
fn pin(keys: &PrimaryKeys) -> String {
    format!("sha256:{}", keys.tls.key_id())
}

fn install(keys: &PrimaryKeys, data_dir: &Path) -> Result<Installed, String> {
    install_keys(keys, &pin(keys), data_dir, &RealKeyFs, &mut Out::quiet())
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

/// Every entry under `root`, by path: a link's target, a directory's mode,
/// a file's mode and bytes. Links are not followed.
fn snapshot(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        for entry in fs::read_dir(dir).unwrap() {
            let owned = entry.unwrap().path();
            let path = owned.as_path();
            let metadata = fs::symlink_metadata(path).unwrap();
            let name = path.strip_prefix(root).unwrap().display().to_string();
            let mode = metadata.mode() & 0o7777;
            if metadata.is_symlink() {
                let target = fs::read_link(path).unwrap();
                out.insert(name, format!("link to {}", target.display()));
            } else if metadata.is_dir() {
                out.insert(name, format!("directory {mode:o}"));
                walk(root, path, out);
            } else {
                let bytes = fs::read(path).unwrap();
                out.insert(name, format!("file {mode:o} {bytes:?}"));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// One change-log entry before its place in the log.
struct Change {
    entity_type: &'static str,
    entity_id: String,
    payload: Value,
}

/// `changes` as the followed log's `create` entries from `seq` 1.
fn log(changes: Vec<Change>) -> Vec<LogEntry> {
    changes
        .into_iter()
        .zip(1u64..)
        .map(|(change, seq)| LogEntry {
            seq,
            term: 1,
            time: TIME.to_string(),
            actor_type: "system".to_string(),
            actor_id: "test".to_string(),
            entity_type: change.entity_type.to_string(),
            entity_id: change.entity_id,
            op: "create".to_string(),
            payload: change.payload.to_string(),
            prev_hash: format!("{:064x}", seq - 1),
            hash: format!("{seq:064x}"),
        })
        .collect()
}

/// The `signing_keys` row of `keys` in `state`.
fn signing_row(keys: &ClusterKeys, state: &str) -> Change {
    Change {
        entity_type: "signing_key",
        entity_id: keys.key_id().to_string(),
        payload: json!({
            "key_id": keys.key_id(),
            "public_key": keys.public_key_base64(),
            "state": state,
            "created_at": TIME,
        }),
    }
}

/// A `node_key_custody` row: `plaintext` sealed to `recipient` for this
/// machine, as the primary writes it.
fn sealed(recipient: &CustodyPublicKey, kind: KeyKind, key_id: &str, plaintext: &[u8]) -> Change {
    let context = SealContext {
        cluster_id: CLUSTER_ID,
        node_id: NODE_ID,
        key_kind: kind,
        key_id,
    };
    let sealed = custody::seal(recipient, &context, plaintext, &mut custody::os_rng()).unwrap();
    Change {
        entity_type: "node_key_custody",
        entity_id: format!("{NODE_ID}/{}/{key_id}", kind.as_str()),
        payload: json!({
            "node_id": NODE_ID,
            "key_kind": kind.as_str(),
            "key_id": key_id,
            "sealed": base64(&sealed),
            "created_at": TIME,
        }),
    }
}

/// Standard base64 with padding, as a seal's payload carries `sealed`.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let padded = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(padded[0]) << 16) | (u32::from(padded[1]) << 8) | u32::from(padded[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The files are the four key files `prepare_host` requires before the
/// control plane starts, and the signing key's is the control plane's.
#[test]
fn the_key_files_are_those_the_host_preparation_requires() {
    assert_eq!(PRIMARY_FILES[1..], FILES);
    assert_eq!(SIGNING_FILE, format!("keys/{SIGNING_KEY_FILE}"));
}

/// A primary's first start writes its keys with the control plane's own
/// writers and seals them to this machine. Opened from those seals and
/// installed, each file is byte for byte the primary's, and each reader a
/// primary uses reads the same key from it.
#[tokio::test]
async fn each_key_is_written_as_the_primary_wrote_it_and_reads_back() {
    let scratch = Scratch::new();
    let primary = scratch.path().join("primary");
    let signing_key = ClusterKeys::load_or_generate(&primary).unwrap();
    let data_key = DataKey::load_or_create(&primary).unwrap();
    let listen = "127.0.0.1:8443".parse().unwrap();
    let tls = crate::tls::load_or_generate(&primary, "primary-1", listen).unwrap();
    let data_file = fs::read(primary.join(DATA_FILE)).unwrap();
    assert_eq!(data_file.len(), DATA_FILE_LEN);

    let node_dir = scratch.path().join("quoro-node");
    identity::ensure_data_dir(&node_dir).unwrap();
    let custody_key = CustodyKey::generate(&mut custody::os_rng());
    fs::write(node_dir.join(CUSTODY_KEY_FILE), &custody_key.to_bytes()[..]).unwrap();
    let identity = Identity {
        node_id: NODE_ID.to_string(),
        cluster_id: CLUSTER_ID.to_string(),
        primary_url: "https://192.0.2.10:8443".to_string(),
        primary_urls: Vec::new(),
        fingerprint: tls.fingerprint.clone(),
        cluster_public_key: signing_key.public_key_base64(),
        cluster_key_id: signing_key.key_id().to_string(),
    };
    let recipient = custody_key.public_key();
    let tls_plaintext = json!({
        "cert_pem": tls.identity.cert_pem(),
        "key_pem": tls.identity.key_pem(),
    })
    .to_string();
    let entries = log(vec![
        signing_row(&signing_key, "current"),
        sealed(
            recipient,
            KeyKind::Signing,
            signing_key.key_id(),
            &signing_key.signing_key().to_bytes(),
        ),
        // The seal holds the key the file holds after its key id.
        sealed(
            recipient,
            KeyKind::Dek,
            &data_key.key_id_hex(),
            &data_file[16..],
        ),
        sealed(
            recipient,
            KeyKind::Tls,
            tls.identity.key_id(),
            tls_plaintext.as_bytes(),
        ),
    ]);
    let opened = open_seals(&identity, &entries, &node_dir).await.unwrap();
    let keys = PrimaryKeys::from_opened(opened).unwrap();

    let data_dir = scratch.data_dir();
    let mut out = Out::quiet();
    let installed = install_keys(
        &keys,
        &identity.fingerprint,
        &data_dir,
        &RealKeyFs,
        &mut out,
    )
    .unwrap();
    assert_eq!(installed.written, FILES);
    assert!(installed.kept.is_empty());
    assert_eq!(out.lines.len(), 6, "{:#?}", out.lines);

    for name in FILES {
        assert_eq!(
            fs::read(data_dir.join(name)).unwrap(),
            fs::read(primary.join(name)).unwrap(),
            "{name} is not the primary's"
        );
    }

    let read = ClusterKeys::load_or_generate(&data_dir).unwrap();
    assert_eq!(read.key_id(), signing_key.key_id());
    assert_eq!(
        read.signing_key().to_bytes(),
        signing_key.signing_key().to_bytes()
    );

    let read = DataKey::load_or_create(&data_dir).unwrap();
    assert_eq!(read.key_id_hex(), data_key.key_id_hex());
    let blob = data_key.encrypt("app_env.value_enc", "env_1", b"hunter2");
    assert_eq!(
        read.decrypt("app_env.value_enc", "env_1", &blob)
            .unwrap()
            .expose(),
        "hunter2"
    );
    let blob = read.encrypt("webhooks.secret_enc", "whk_1", b"s3cret");
    assert_eq!(
        data_key
            .decrypt("webhooks.secret_enc", "whk_1", &blob)
            .unwrap()
            .expose(),
        "s3cret"
    );

    let read = crate::tls::load_existing(&data_dir).unwrap();
    assert_eq!(read.fingerprint, tls.fingerprint);
    assert_eq!(read.identity.cert_pem(), tls.identity.cert_pem());
    assert_eq!(read.identity.key_pem(), tls.identity.key_pem());
}

/// Each file is `0600` and `keys/` and `tls/` are `0700`, also when a run
/// before left a directory open to others or a kept file with another mode.
/// The modes are read before any reader runs: the data key's reader makes
/// its directory `0700` itself.
#[test]
fn the_files_are_0600_and_their_directories_0700() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();
    let keys = primary_keys();
    fs::create_dir(data_dir.join(KEYS_DIR)).unwrap();
    fs::set_permissions(data_dir.join(KEYS_DIR), fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(data_dir.join(SIGNING_FILE), SEED).unwrap();
    fs::set_permissions(
        data_dir.join(SIGNING_FILE),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();

    let installed = install(&keys, &data_dir).unwrap();
    assert_eq!(installed.kept, [SIGNING_FILE]);
    assert_eq!(installed.written, [DATA_FILE, CERT_FILE, KEY_FILE]);
    for dir in [KEYS_DIR, TLS_DIR] {
        assert_eq!(mode(&data_dir.join(dir)), 0o700, "{dir}");
    }
    for name in FILES {
        assert_eq!(mode(&data_dir.join(name)), 0o600, "{name}");
    }
}

/// An existing key file that holds anything else refuses, names the file,
/// and nothing changes: no directory, mode or file.
#[test]
fn an_existing_file_that_holds_something_else_refuses_and_changes_nothing() {
    for name in FILES {
        let scratch = Scratch::new();
        let data_dir = scratch.data_dir();
        let path = data_dir.join(name);
        fs::create_dir(path.parent().unwrap()).unwrap();
        fs::write(&path, b"another key").unwrap();
        let before = snapshot(scratch.path());

        let refusal = install(&primary_keys(), &data_dir).unwrap_err();
        assert!(
            refusal.contains(&format!(
                "{} exists and holds something else, and a key file is never overwritten",
                path.display()
            )) && refusal.ends_with("nothing was changed"),
            "{refusal}"
        );
        assert_eq!(snapshot(scratch.path()), before, "{name}");
    }
}

/// A key file that already holds what would be written is kept, as it is:
/// a rerun writes nothing and replaces no file.
#[test]
fn an_identical_file_is_kept_and_a_rerun_changes_nothing() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();
    let keys = primary_keys();
    install(&keys, &data_dir).unwrap();
    let inodes = |dir: &Path| -> Vec<u64> {
        FILES
            .iter()
            .map(|name| fs::symlink_metadata(dir.join(name)).unwrap().ino())
            .collect()
    };
    let first = inodes(&data_dir);
    let before = snapshot(scratch.path());

    let rerun = install(&keys, &data_dir).unwrap();
    assert_eq!(
        rerun,
        Installed {
            written: Vec::new(),
            kept: FILES.to_vec(),
        }
    );
    assert_eq!(snapshot(scratch.path()), before);
    assert_eq!(inodes(&data_dir), first);
}

/// A run stopped between two files leaves the files it wrote and no
/// partial file. A rerun keeps those, removes a partial file a crash left,
/// and writes the rest.
#[test]
fn a_run_stopped_between_two_files_is_finished_by_a_rerun() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();
    let keys = primary_keys();
    let stop = StopAt(data_dir.join(CERT_FILE));
    let error = install_keys(&keys, &pin(&keys), &data_dir, &stop, &mut Out::quiet()).unwrap_err();
    assert!(error.contains("the run stopped here"), "{error}");
    assert!(data_dir.join(SIGNING_FILE).is_file());
    assert!(data_dir.join(DATA_FILE).is_file());
    assert!(!data_dir.join(CERT_FILE).exists());
    let partial = data_dir.join("tls/.cert.pem.promote-partial");
    assert!(!partial.exists(), "the partial key file is removed");

    // A crash between the write and the rename leaves it.
    fs::write(&partial, b"half a certificate").unwrap();
    let rerun = install(&keys, &data_dir).unwrap();
    assert_eq!(rerun.kept, [SIGNING_FILE, DATA_FILE]);
    assert_eq!(rerun.written, [CERT_FILE, KEY_FILE]);
    assert!(!partial.exists());
    assert_eq!(
        fs::read_to_string(data_dir.join(CERT_FILE)).unwrap(),
        keys.tls.cert_pem()
    );
}

/// A symbolic link at the data directory, at `keys/` or `tls/`, or at a key
/// file refuses, even one to a file that holds the same key, and nothing is
/// written through it or anywhere else.
#[test]
fn a_symbolic_link_on_the_way_refuses_and_nothing_is_written() {
    let keys = primary_keys();

    // The data directory.
    let scratch = Scratch::new();
    let link = scratch.path().join("link");
    symlink(scratch.data_dir(), &link).unwrap();
    let before = snapshot(scratch.path());
    let refusal = install(&keys, &link).unwrap_err();
    assert!(
        refusal.contains(&format!(
            "the data directory {} is a symbolic link",
            link.display()
        )),
        "{refusal}"
    );
    assert_eq!(snapshot(scratch.path()), before);

    // `keys/` and `tls/`.
    for dir in [KEYS_DIR, TLS_DIR] {
        let scratch = Scratch::new();
        let data_dir = scratch.data_dir();
        let elsewhere = scratch.path().join("elsewhere");
        fs::create_dir(&elsewhere).unwrap();
        symlink(elsewhere, data_dir.join(dir)).unwrap();
        let before = snapshot(scratch.path());
        let refusal = install(&keys, &data_dir).unwrap_err();
        assert!(
            refusal.contains(&format!(
                "{} is a symbolic link",
                data_dir.join(dir).display()
            )),
            "{refusal}"
        );
        assert_eq!(snapshot(scratch.path()), before, "{dir}");
    }

    // A key file: a link to a file holding the same key, and a dangling one.
    for (name, target_holds) in [(KEY_FILE, Some(keys.tls.key_pem())), (DATA_FILE, None)] {
        let scratch = Scratch::new();
        let data_dir = scratch.data_dir();
        let path = data_dir.join(name);
        fs::create_dir(path.parent().unwrap()).unwrap();
        let target = scratch.path().join("target");
        if let Some(text) = target_holds {
            fs::write(&target, text).unwrap();
        }
        symlink(target, &path).unwrap();
        let before = snapshot(scratch.path());
        let refusal = install(&keys, &data_dir).unwrap_err();
        assert!(
            refusal.contains(&format!("{} is a symbolic link", path.display())),
            "{refusal}"
        );
        assert_eq!(snapshot(scratch.path()), before, "{name}");
    }
}

/// A certificate other than the one this machine pins refuses before
/// anything is written: nodes would refuse the promoted primary.
#[test]
fn a_certificate_other_than_the_pinned_one_refuses() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();
    let keys = primary_keys();
    let other = tls_pair();
    let pinned = format!("sha256:{}", other.key_id());
    let before = snapshot(scratch.path());

    let refusal =
        install_keys(&keys, &pinned, &data_dir, &RealKeyFs, &mut Out::quiet()).unwrap_err();
    assert!(
        refusal.contains(&format!(
            "the TLS certificate's fingerprint is sha256:{}, but this machine pins {pinned}",
            keys.tls.key_id()
        )) && refusal.ends_with("nothing was changed"),
        "{refusal}"
    );
    assert_eq!(snapshot(scratch.path()), before);
}

/// During a rotation the next signing key is in use too, and this binary's
/// control plane keeps no file for it: the installation refuses, names the
/// key, and writes nothing. Without the next key the same keys install.
#[test]
fn a_rotation_s_next_signing_key_refuses_and_changes_nothing() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();
    let mut keys = primary_keys();
    keys.signing.push(signing(NEXT_SEED));
    let before = snapshot(scratch.path());

    let refusal = install(&keys, &data_dir).unwrap_err();
    assert!(
        refusal.contains(&format!(
            "the signing key {} is in use beside the current key {}, as the next key of a \
             rotation",
            signing_key_id(&NEXT_SEED),
            signing_key_id(&SEED)
        )) && refusal.ends_with("nothing was changed"),
        "{refusal}"
    );
    assert_eq!(snapshot(scratch.path()), before);

    keys.signing.pop();
    install(&keys, &data_dir).unwrap();
    assert_eq!(fs::read(data_dir.join(SIGNING_FILE)).unwrap(), SEED);
}

/// The signing key file holds the `current` row's key; without a `current`
/// row, the one key in use. A `current` row whose key was not opened, a
/// seed that is not the key its id names, and a data key id that is not
/// hex refuse.
#[test]
fn the_signing_key_file_holds_the_current_key_and_bad_keys_refuse() {
    let scratch = Scratch::new();
    let data_dir = scratch.data_dir();

    let mut keys = primary_keys();
    keys.current_signing = None;
    install(&keys, &data_dir).unwrap();
    assert_eq!(fs::read(data_dir.join(SIGNING_FILE)).unwrap(), SEED);

    let mut keys = primary_keys();
    keys.current_signing = Some(signing_key_id(&NEXT_SEED));
    let refusal = install(&keys, &scratch.data_dir()).unwrap_err();
    assert!(
        refusal.contains(&format!(
            "the current signing key {} was not opened",
            signing_key_id(&NEXT_SEED)
        )),
        "{refusal}"
    );

    let mut keys = primary_keys();
    keys.signing[0].key_id = signing_key_id(&NEXT_SEED);
    keys.current_signing = Some(signing_key_id(&NEXT_SEED));
    let refusal = install(&keys, &scratch.data_dir()).unwrap_err();
    assert!(
        refusal.contains(&format!(
            "the signing key {}: its seed is the key {}",
            signing_key_id(&NEXT_SEED),
            signing_key_id(&SEED)
        )),
        "{refusal}"
    );

    let mut keys = primary_keys();
    keys.data.key_id = DEK_ID.to_ascii_uppercase();
    let refusal = install(&keys, &scratch.data_dir()).unwrap_err();
    assert!(
        refusal.contains("its key id is not 32 lowercase hex characters"),
        "{refusal}"
    );
}
