//! `quoro restore` (`docs/contracts/backup-format.md` section 8.2): a
//! control-plane backup becomes a usable database, and every refusal leaves
//! nothing installed.

#[allow(dead_code)]
#[path = "../src/restore.rs"]
mod restore;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use quoro_backup::{
    BackupWriter, Chunker, FORMAT, Manifest, RecoveryKey, RepoConfig, RepoKey, Seal, SourceKind,
    chunks, layout, sign_seal, wrap_repo_key,
};
use quoro_control_plane::restore_pending::{self, Head, Marker};
use quoro_control_plane::store::{GENESIS_HASH, random_secret};
use quoro_proto::sign;
use quoro_providers::object_store::{MemoryStore, ObjectStore};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

const CLUSTER: &str = "cls_01JC9Z1A2B3C4D5E6F7G8H9J0K";
const REPOSITORY: &str = "brp_01JCA0A2B3C4D5E6F7G8H9J0KM";
const PREFIX: &str = "backups";
const SIGNING_SEED: [u8; 32] = [7; 32];
const DATA_KEY: [u8; 48] = [0x11; 48];
const OLD: &str = "2026-11-05T03:00:42Z";
const NEW: &str = "2026-11-05T09:00:00Z";

struct Fixture {
    store: MemoryStore,
    root: String,
    recovery: RecoveryKey,
    repo_key: RepoKey,
    signing: sign::SigningKey,
    base: PathBuf,
    signing_file: PathBuf,
    data_key_file: PathBuf,
    database: Vec<u8>,
}

impl Fixture {
    async fn new() -> Self {
        let base =
            Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("restore-{}", random_secret()));
        std::fs::create_dir_all(&base).unwrap();
        let signing_file = base.join("signing.key");
        let data_key_file = base.join("data.key");
        write_private(&signing_file, &SIGNING_SEED);
        write_private(&data_key_file, &DATA_KEY);

        let signing = sign::signing_key_from_seed(&SIGNING_SEED);
        let repo_key = RepoKey::generate();
        let recovery = RecoveryKey::generate();
        let root = layout::root(PREFIX, CLUSTER);

        let store = MemoryStore::new();
        let config = RepoConfig {
            format: FORMAT,
            repository_id: REPOSITORY.into(),
            cluster_id: CLUSTER.into(),
            cluster_public_key: sign::public_key_to_base64(&signing.verifying_key()),
            recovery_key_fingerprint: recovery.fingerprint(),
            wrapped_repo_key: wrap_repo_key(&repo_key, &recovery, REPOSITORY),
            created_at: "2026-11-05T02:00:00Z".into(),
        };
        put_json(
            &store,
            &format!("{root}{}", layout::config(1)),
            serde_json::to_vec(&config).unwrap(),
        )
        .await;

        let database = make_database(&base.join("source.db"), "written before the backup").await;

        Self {
            store,
            root,
            recovery,
            repo_key,
            signing,
            base,
            signing_file,
            data_key_file,
            database,
        }
    }

    /// Adds one sealed control-plane backup of `data`.
    async fn add_backup(&self, backup_id: &str, data: &[u8], time: &str) -> Seal {
        let header = Manifest {
            format: FORMAT,
            backup_id: backup_id.into(),
            cluster_id: CLUSTER.into(),
            repository_id: REPOSITORY.into(),
            source_kind: SourceKind::ControlPlane,
            source_id: CLUSTER.into(),
            volume_id: None,
            fencing_token: None,
            snapshot_method: None,
            logical_bytes: 0,
            chunker: Chunker::fixed(),
            chunks: Vec::new(),
            quoro_version: "0.1.0".into(),
            created_at: time.into(),
        };
        let mut writer = BackupWriter::new(self.repo_key.source_keys(CLUSTER), header);
        let mut written: BTreeMap<String, ()> = BTreeMap::new();
        for chunk in chunks(data) {
            let (id, object, _) = writer.push(chunk.unwrap());
            let key = format!("{}{}", self.root, layout::chunk(&id));
            if written.insert(key.clone(), ()).is_none() {
                put_json(&self.store, &key, object).await;
            }
        }
        let (manifest, object, digest) = writer.finish().unwrap();
        put_json(
            &self.store,
            &format!("{}{}", self.root, layout::manifest(CLUSTER, backup_id)),
            object,
        )
        .await;

        let mut seal = Seal {
            format: FORMAT,
            backup_id: backup_id.into(),
            cluster_id: CLUSTER.into(),
            repository_id: REPOSITORY.into(),
            source_kind: SourceKind::ControlPlane,
            source_id: CLUSTER.into(),
            volume_id: None,
            fencing_token: None,
            manifest_key: layout::manifest(CLUSTER, backup_id),
            manifest_digest: digest,
            logical_bytes: manifest.logical_bytes,
            chunk_count: manifest.chunks.len() as u64,
            created_at: time.into(),
            sealed_at: time.into(),
            signature: String::new(),
        };
        sign_seal(&mut seal, &self.signing).unwrap();
        put_json(
            &self.store,
            &format!("{}{}", self.root, layout::seal(CLUSTER, backup_id)),
            serde_json::to_vec(&seal).unwrap(),
        )
        .await;
        seal
    }

    fn options(&self, data_dir: &Path, backup: Option<&str>) -> restore::Options {
        restore::Options {
            prefix: PREFIX.into(),
            cluster_id: CLUSTER.into(),
            data_dir: data_dir.to_path_buf(),
            signing_key_file: self.signing_file.clone(),
            data_key_file: self.data_key_file.clone(),
            recovery_key: self.recovery.clone(),
            backup: backup.map(str::to_string),
            node_id: None,
            // No node agent on this host unless a test writes one.
            identity_file: self.base.join("no-node-agent").join("identity.json"),
            dry_run: false,
        }
    }

    /// Writes a node agent identity file of `cluster_id` naming `node_id` and
    /// returns its path.
    fn identity_file(&self, name: &str, cluster_id: &str, node_id: &str) -> PathBuf {
        let path = self.base.join(name);
        let identity = serde_json::json!({
            "node_id": node_id,
            "cluster_id": cluster_id,
            "primary_url": "https://primary.example:8443",
            "fingerprint": format!("sha256:{}", "0".repeat(64)),
            "cluster_public_key": sign::public_key_to_base64(&self.signing.verifying_key()),
            "cluster_key_id": "0".repeat(64),
        });
        write_private(&path, &serde_json::to_vec(&identity).unwrap());
        path
    }

    fn data_dir(&self, name: &str) -> PathBuf {
        self.base.join(name)
    }
}

async fn put_json(store: &MemoryStore, key: &str, body: Vec<u8>) {
    match store.put_new(key, body, None).await {
        Ok(_) | Err(quoro_providers::object_store::ObjectStoreError::AlreadyExists) => {}
        Err(error) => panic!("put {key}: {error}"),
    }
}

async fn make_database(path: &Path, value: &str) -> Vec<u8> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE restored (value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    create_change_log(&pool, &[]).await;
    sqlx::query("INSERT INTO restored (value) VALUES (?)")
        .bind(value)
        .execute(&pool)
        .await
        .unwrap();
    let _ = sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(&pool)
        .await;
    pool.close().await;
    std::fs::read(path).unwrap()
}

/// A database with a `nodes` table holding `(id, status, cp_status)` rows,
/// the columns `quoro restore` reads, and an empty change log.
async fn make_database_with_nodes(path: &Path, nodes: &[(&str, &str, Option<&str>)]) -> Vec<u8> {
    make_database_with(path, nodes, &[]).await
}

/// [`make_database_with_nodes`] with the change-log entries `log`, as
/// `(seq, hash)`.
async fn make_database_with(
    path: &Path,
    nodes: &[(&str, &str, Option<&str>)],
    log: &[(i64, &str)],
) -> Vec<u8> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE nodes (id TEXT PRIMARY KEY, status TEXT NOT NULL, cp_status TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, status, cp_status) in nodes {
        sqlx::query("INSERT INTO nodes (id, status, cp_status) VALUES (?, ?, ?)")
            .bind(id)
            .bind(status)
            .bind(cp_status)
            .execute(&pool)
            .await
            .unwrap();
    }
    create_change_log(&pool, log).await;
    let _ = sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(&pool)
        .await;
    pool.close().await;
    std::fs::read(path).unwrap()
}

/// The `change_log` columns `quoro restore` reads, with the entries `log`
/// as `(seq, hash)`.
async fn create_change_log(pool: &sqlx::SqlitePool, log: &[(i64, &str)]) {
    sqlx::query("CREATE TABLE change_log (seq INTEGER PRIMARY KEY, hash TEXT NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
    for (seq, hash) in log {
        sqlx::query("INSERT INTO change_log (seq, hash) VALUES (?, ?)")
            .bind(seq)
            .bind(hash)
            .execute(pool)
            .await
            .unwrap();
    }
}

fn write_private(path: &Path, bytes: &[u8]) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    use std::io::Write as _;
    options.open(path).unwrap().write_all(bytes).unwrap();
}

fn write_with_mode(path: &Path, bytes: &[u8], mode: u32) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(mode);
    }
    use std::io::Write as _;
    options.open(path).unwrap().write_all(bytes).unwrap();
}

#[cfg(unix)]
fn mode_of(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn assert_nothing_installed(dir: &Path) {
    assert!(!dir.join("quoro.db").exists(), "no database");
    assert!(!dir.join("primary-node").exists(), "no primary-node file");
    assert!(
        !dir.join(restore_pending::MARKER_FILE).exists(),
        "no restore-pending marker"
    );
    assert!(!dir.join(".restore-quoro.db.tmp").exists(), "no temp file");
    assert!(!dir.join("keys/signing.key").exists(), "no signing key");
    assert!(!dir.join("keys/data.key").exists(), "no data key");
}

async fn stored_value(database: &Path) -> String {
    let options = SqliteConnectOptions::new()
        .filename(database)
        .read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let value: String = sqlx::query_scalar("SELECT value FROM restored")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    value
}

#[tokio::test]
async fn restores_the_newest_backup_and_installs_private_files() {
    let fixture = Fixture::new().await;
    let seal = fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let dir = fixture.data_dir("restored");

    let report = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap();
    assert!(report.installed);
    assert_eq!(report.backup_id, seal.backup_id);
    assert_eq!(report.backup_time, NEW);
    assert_eq!(report.logical_bytes, fixture.database.len() as u64);
    assert_eq!(report.skipped_seals, 0);

    assert_eq!(
        std::fs::read(dir.join("quoro.db")).unwrap(),
        fixture.database
    );
    assert_eq!(
        std::fs::read(dir.join("keys/signing.key")).unwrap(),
        SIGNING_SEED
    );
    assert_eq!(std::fs::read(dir.join("keys/data.key")).unwrap(), DATA_KEY);
    assert_eq!(
        stored_value(&dir.join("quoro.db")).await,
        "written before the backup"
    );

    #[cfg(unix)]
    {
        assert_eq!(mode_of(&dir.join("quoro.db")), 0o600);
        assert_eq!(mode_of(&dir.join("keys")), 0o700);
        assert_eq!(mode_of(&dir.join("keys/signing.key")), 0o600);
        assert_eq!(mode_of(&dir.join("keys/data.key")), 0o600);
    }
}

#[tokio::test]
async fn a_wrong_recovery_key_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let dir = fixture.data_dir("wrong-recovery");
    let mut options = fixture.options(&dir, None);
    options.recovery_key = RecoveryKey::generate();

    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("opens no config object"),
        "{error}"
    );
    assert_nothing_installed(&dir);
}

#[tokio::test]
async fn a_signing_key_that_does_not_match_the_config_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let other = fixture.base.join("other-signing.key");
    write_private(&other, &[8u8; 32]);
    let dir = fixture.data_dir("wrong-signing");
    let mut options = fixture.options(&dir, None);
    options.signing_key_file = other;

    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match the cluster's public key"),
        "{error}"
    );
    assert_nothing_installed(&dir);
}

#[tokio::test]
async fn a_tampered_seal_is_skipped_and_the_older_valid_one_is_used() {
    let fixture = Fixture::new().await;
    let old = fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0M", &fixture.database, OLD)
        .await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0N", &fixture.database, NEW)
        .await;

    let key = format!(
        "{}{}",
        fixture.root,
        layout::seal(CLUSTER, "bkp_01JC9Z1A2B3C4D5E6F7G8H9J0N")
    );
    let mut tampered: Seal =
        serde_json::from_slice(&fixture.store.get(&key).await.unwrap()).unwrap();
    tampered.chunk_count += 1;
    fixture
        .store
        .put_overwrite(&key, serde_json::to_vec(&tampered).unwrap())
        .await
        .unwrap();

    let dir = fixture.data_dir("tampered-seal");
    let report = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap();
    assert_eq!(report.backup_id, old.backup_id);
    assert_eq!(report.skipped_seals, 1);
}

#[tokio::test]
async fn only_a_tampered_seal_leaves_no_valid_seal() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let key = format!(
        "{}{}",
        fixture.root,
        layout::seal(CLUSTER, "bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K")
    );
    let mut tampered: Seal =
        serde_json::from_slice(&fixture.store.get(&key).await.unwrap()).unwrap();
    tampered.chunk_count += 1;
    fixture
        .store
        .put_overwrite(&key, serde_json::to_vec(&tampered).unwrap())
        .await
        .unwrap();

    let dir = fixture.data_dir("no-valid-seal");
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no valid seal"), "{error}");
    assert_nothing_installed(&dir);
}

#[tokio::test]
async fn a_manifest_whose_digest_does_not_match_the_seal_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let key = format!(
        "{}{}",
        fixture.root,
        layout::manifest(CLUSTER, "bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K")
    );
    fixture
        .store
        .put_overwrite(&key, b"not the manifest".to_vec())
        .await
        .unwrap();

    let dir = fixture.data_dir("bad-digest");
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("digest"), "{error}");
    assert_nothing_installed(&dir);
}

#[tokio::test]
async fn a_tampered_chunk_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let chunks: Vec<String> = fixture
        .store
        .list(&format!("{}chunks/", fixture.root))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert!(!chunks.is_empty());
    let mut object = fixture.store.get(&chunks[0]).await.unwrap();
    object[24] ^= 0x01;
    fixture
        .store
        .put_overwrite(&chunks[0], object)
        .await
        .unwrap();

    let dir = fixture.data_dir("bad-chunk");
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("chunk"), "{error}");
    assert_nothing_installed(&dir);
}

#[tokio::test]
async fn an_existing_database_in_the_data_dir_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let dir = fixture.data_dir("live-primary");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("quoro.db"), b"live").unwrap();

    // The live primary's own marker, from the restore it was installed by.
    std::fs::write(dir.join(restore_pending::MARKER_FILE), b"the live one's").unwrap();

    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");
    assert_eq!(std::fs::read(dir.join("quoro.db")).unwrap(), b"live");
    assert!(!dir.join(".restore-quoro.db.tmp").exists());
    assert_eq!(
        std::fs::read(dir.join(restore_pending::MARKER_FILE)).unwrap(),
        b"the live one's",
        "a refused restore leaves a live primary's marker alone"
    );
}

#[tokio::test]
async fn backup_picks_an_older_backup_and_the_newest_is_the_default() {
    let fixture = Fixture::new().await;
    let older = make_database(&fixture.base.join("older.db"), "older").await;
    let newer = make_database(&fixture.base.join("newer.db"), "newer").await;
    let old = fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0M", &older, OLD)
        .await;
    let new = fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0N", &newer, NEW)
        .await;

    let named = fixture.data_dir("named");
    let report = restore::execute(
        &fixture.store,
        &fixture.options(&named, Some(&old.backup_id)),
    )
    .await
    .unwrap();
    assert_eq!(report.backup_id, old.backup_id);
    assert_eq!(std::fs::read(named.join("quoro.db")).unwrap(), older);

    let newest = fixture.data_dir("newest");
    let report = restore::execute(&fixture.store, &fixture.options(&newest, None))
        .await
        .unwrap();
    assert_eq!(report.backup_id, new.backup_id);
    assert_eq!(std::fs::read(newest.join("quoro.db")).unwrap(), newer);
}

#[tokio::test]
async fn a_dry_run_installs_nothing() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let dir = fixture.data_dir("dry-run");
    let mut options = fixture.options(&dir, None);
    options.dry_run = true;

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(!report.installed);
    assert_nothing_installed(&dir);
}

#[test]
fn a_credentials_file_other_users_can_read_is_refused() {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("restore-{}", random_secret()));
    std::fs::create_dir_all(&base).unwrap();
    let path = base.join("credentials");
    write_with_mode(
        &path,
        b"access_key_id=id\nsecret_access_key=secret\n",
        0o644,
    );

    let error = restore::read_credentials(&path).unwrap_err();
    assert!(
        error.to_string().contains("readable by group or others"),
        "{error}"
    );

    let private = base.join("private-credentials");
    write_with_mode(
        &private,
        b"access_key_id=id\nsecret_access_key=secret\n",
        0o600,
    );
    let credentials = restore::read_credentials(&private).unwrap();
    assert_eq!(credentials.access_key_id, "id");
    assert_eq!(credentials.secret_access_key.as_str(), "secret");
    assert_eq!(format!("{credentials:?}"), "Credentials([redacted])");
}

#[cfg(unix)]
#[test]
fn the_command_refuses_a_readable_credentials_file_before_touching_the_data_dir() {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("restore-{}", random_secret()));
    std::fs::create_dir_all(&base).unwrap();
    let credentials = base.join("credentials");
    write_with_mode(
        &credentials,
        b"access_key_id=id\nsecret_access_key=secret\n",
        0o644,
    );
    let data_dir = base.join("data");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args([
            "restore",
            "--endpoint",
            "https://example.invalid",
            "--region",
            "auto",
            "--bucket",
            "backups",
            "--prefix",
            "quoro",
            "--cluster-id",
            CLUSTER,
            "--credentials-file",
        ])
        .arg(&credentials)
        .args(["--recovery-key-file", "/nonexistent/recovery.key"])
        .args(["--signing-key-file", "/nonexistent/signing.key"])
        .args(["--data-key-file", "/nonexistent/data.key"])
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("readable by group or others"), "{stderr}");
    assert!(!data_dir.exists());
}

const OLD_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0A";
const NEW_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0B";
const OTHER_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0C";
const REMOVED_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0D";
const UNKNOWN_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0E";
const OTHER_CLUSTER: &str = "cls_01JC9Z1A2B3C4D5E6F7G8H9J0Z";

/// A fixture whose one backup holds a `nodes` table: the old host's primary,
/// the new host's node, another node and a removed one.
async fn node_fixture() -> Fixture {
    let fixture = Fixture::new().await;
    let database = make_database_with_nodes(
        &fixture.base.join("nodes.db"),
        &[
            (OLD_NODE, "connected", Some("primary")),
            (NEW_NODE, "pending", None),
            (OTHER_NODE, "connected", None),
            (REMOVED_NODE, "removed", None),
        ],
    )
    .await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    fixture
}

#[tokio::test]
async fn the_identity_file_of_this_cluster_names_the_new_primary_node() {
    let fixture = node_fixture().await;
    let dir = fixture.data_dir("identity");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert_eq!(
        report.primary,
        restore::PrimaryPick::IdentityFile(NEW_NODE.into())
    );
    let marker = dir.join("primary-node");
    assert_eq!(std::fs::read_to_string(&marker).unwrap().trim(), NEW_NODE);
    #[cfg(unix)]
    assert_eq!(mode_of(&marker), 0o600);
}

#[tokio::test]
async fn an_identity_file_of_another_cluster_writes_no_marker() {
    let fixture = node_fixture().await;
    let dir = fixture.data_dir("other-cluster");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", OTHER_CLUSTER, NEW_NODE);

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(dir.join("quoro.db").exists());
    assert!(!dir.join("primary-node").exists());
    let restore::PrimaryPick::Unchanged(reason) = &report.primary else {
        panic!("no node is picked: {:?}", report.primary);
    };
    assert!(reason.contains(OTHER_CLUSTER), "{reason}");
}

#[tokio::test]
async fn an_identity_file_naming_an_unknown_or_removed_node_writes_no_marker() {
    let fixture = node_fixture().await;
    for (name, node_id, expected) in [
        ("unknown", UNKNOWN_NODE, "holds no node"),
        ("removed", REMOVED_NODE, "is removed"),
    ] {
        let dir = fixture.data_dir(&format!("identity-{name}"));
        let mut options = fixture.options(&dir, None);
        options.identity_file =
            fixture.identity_file(&format!("identity-{name}.json"), CLUSTER, node_id);

        let report = restore::execute(&fixture.store, &options).await.unwrap();
        assert!(report.installed);
        assert!(!dir.join("primary-node").exists(), "{name}");
        let restore::PrimaryPick::Unchanged(reason) = &report.primary else {
            panic!("no node is picked: {:?}", report.primary);
        };
        assert!(reason.contains(expected), "{reason}");
    }
}

#[tokio::test]
async fn no_identity_file_writes_no_marker() {
    let fixture = node_fixture().await;
    let dir = fixture.data_dir("no-identity");

    let report = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap();
    assert!(report.installed);
    assert!(!dir.join("primary-node").exists());
    let restore::PrimaryPick::Unchanged(reason) = &report.primary else {
        panic!("no node is picked: {:?}", report.primary);
    };
    assert!(reason.contains("no node agent identity file"), "{reason}");
}

#[tokio::test]
async fn a_node_id_the_database_does_not_hold_or_holds_removed_is_refused() {
    let fixture = node_fixture().await;
    for (name, node_id, expected) in [
        ("unknown", UNKNOWN_NODE, "holds no such node"),
        ("removed", REMOVED_NODE, "is removed"),
    ] {
        let dir = fixture.data_dir(&format!("node-id-{name}"));
        let mut options = fixture.options(&dir, None);
        options.node_id = Some(node_id.into());
        // The flag wins even over a usable identity file.
        options.identity_file =
            fixture.identity_file(&format!("identity-{name}.json"), CLUSTER, NEW_NODE);

        let error = restore::execute(&fixture.store, &options)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_nothing_installed(&dir);
    }
}

#[tokio::test]
async fn node_id_overrides_the_identity_file() {
    let fixture = node_fixture().await;
    let dir = fixture.data_dir("node-id");
    let mut options = fixture.options(&dir, None);
    options.node_id = Some(OTHER_NODE.into());
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert_eq!(
        report.primary,
        restore::PrimaryPick::NodeIdFlag(OTHER_NODE.into())
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("primary-node"))
            .unwrap()
            .trim(),
        OTHER_NODE
    );
}

#[tokio::test]
async fn a_dry_run_reports_the_node_and_writes_no_marker() {
    let fixture = node_fixture().await;
    let dir = fixture.data_dir("dry-run-node");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);
    options.dry_run = true;

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(!report.installed);
    assert_eq!(
        report.primary,
        restore::PrimaryPick::IdentityFile(NEW_NODE.into())
    );
    assert_nothing_installed(&dir);
}

#[cfg(unix)]
#[test]
fn the_command_refuses_a_malformed_node_id() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
        .args([
            "restore",
            "--endpoint",
            "https://example.invalid",
            "--region",
            "auto",
            "--bucket",
            "backups",
            "--prefix",
            "quoro",
            "--cluster-id",
            CLUSTER,
            "--credentials-file",
            "/nonexistent/credentials",
            "--recovery-key-file",
            "/nonexistent/recovery.key",
            "--signing-key-file",
            "/nonexistent/signing.key",
            "--data-key-file",
            "/nonexistent/data.key",
            "--data-dir",
            "/nonexistent/data",
            "--node-id",
            "nod_lowercase",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--node-id"), "{stderr}");
}

// ---------------------------------------------------------------------------
// The restore-pending marker (section 8.2, "Detecting the first start")
// ---------------------------------------------------------------------------

/// Three entry hashes of a restored change log.
fn hash(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}

#[cfg(unix)]
fn owner_of(path: &Path) -> u32 {
    use std::os::unix::fs::MetadataExt as _;
    std::fs::metadata(path).unwrap().uid()
}

/// `quoro restore` writes `restore-pending` next to the database and to
/// `primary-node`, mode `0600` and with their owner, naming the restored
/// log's head: the newest entry's `seq` and hash. Nothing else is in it: no
/// `--accept-loss`, no statement.
#[tokio::test]
async fn the_marker_names_the_restored_logs_head() {
    let fixture = Fixture::new().await;
    let (h1, h2, h3) = (hash(1), hash(2), hash(3));
    let database = make_database_with(
        &fixture.base.join("logged.db"),
        &[
            (OLD_NODE, "connected", Some("primary")),
            (NEW_NODE, "pending", None),
        ],
        &[(1, h1.as_str()), (2, h2.as_str()), (3, h3.as_str())],
    )
    .await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    let dir = fixture.data_dir("marker");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    let path = dir.join(restore_pending::MARKER_FILE);
    let bytes = std::fs::read(&path).unwrap();
    let marker = Marker::parse(&bytes).unwrap();
    assert_eq!(marker, Marker::plain(Head { seq: 3, hash: h3 }));
    assert_eq!(
        bytes,
        marker.to_bytes(),
        "the file is the marker's own form"
    );
    #[cfg(unix)]
    {
        assert_eq!(mode_of(&path), 0o600);
        let primary_node = dir.join("primary-node");
        assert!(primary_node.exists());
        assert_eq!(owner_of(&path), owner_of(&primary_node));
        assert_eq!(owner_of(&path), owner_of(&dir.join("quoro.db")));
    }
}

/// An empty change log has no head entry: the marker names `seq` 0 and the
/// genesis hash, which the primary reads for an empty log too.
#[tokio::test]
async fn an_empty_change_log_is_named_by_the_genesis_head() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;
    let dir = fixture.data_dir("genesis");

    restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap();
    let marker =
        Marker::parse(&std::fs::read(dir.join(restore_pending::MARKER_FILE)).unwrap()).unwrap();
    assert_eq!(
        marker,
        Marker::plain(Head {
            seq: 0,
            hash: GENESIS_HASH.to_string()
        })
    );
}

/// A database without a change log has no head to name, so no marker can
/// be written, and a database is never installed without one.
#[tokio::test]
async fn a_backup_without_a_change_log_is_refused() {
    let fixture = Fixture::new().await;
    let path = fixture.base.join("no-log.db");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE restored (value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let _ = sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(&pool)
        .await;
    pool.close().await;
    let database = std::fs::read(&path).unwrap();
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    let dir = fixture.data_dir("no-log");

    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("change log"), "{error}");
    assert_nothing_installed(&dir);
}

/// A restore that stops before the database is installed deletes the
/// marker: one an interrupted earlier run left in the data directory, when
/// a chunk fails or a key file cannot be read, and its own, when the install
/// fails after writing it (a `primary-node` that cannot be replaced).
#[tokio::test]
async fn a_restore_that_stops_before_installing_deletes_the_marker() {
    let fixture = Fixture::new().await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &fixture.database, NEW)
        .await;

    // The install fails after the marker is written: `primary-node` is a
    // directory, which the install cannot remove.
    let dir = fixture.data_dir("install-fails");
    std::fs::create_dir_all(dir.join("primary-node").join("in-the-way")).unwrap();
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("primary-node"), "{error}");
    assert!(!dir.join("quoro.db").exists(), "no database");
    assert!(
        !dir.join(restore_pending::MARKER_FILE).exists(),
        "the marker is deleted"
    );
    assert!(!dir.join("keys/signing.key").exists(), "no signing key");

    // A marker left by an earlier run, and a chunk that does not open.
    let chunks: Vec<String> = fixture
        .store
        .list(&format!("{}chunks/", fixture.root))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .collect();
    let mut object = fixture.store.get(&chunks[0]).await.unwrap();
    object[24] ^= 0x01;
    fixture
        .store
        .put_overwrite(&chunks[0], object)
        .await
        .unwrap();
    let dir = fixture.data_dir("left-behind");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(restore_pending::MARKER_FILE),
        Marker::plain(Head {
            seq: 7,
            hash: hash(7),
        })
        .to_bytes(),
    )
    .unwrap();
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("chunk"), "{error}");
    assert_nothing_installed(&dir);

    // A marker left by an earlier run, and a signing key file that cannot be
    // read: the restore stops before it reads the repository.
    let dir = fixture.data_dir("no-signing-key");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(restore_pending::MARKER_FILE),
        Marker::plain(Head {
            seq: 7,
            hash: hash(7),
        })
        .to_bytes(),
    )
    .unwrap();
    let mut options = fixture.options(&dir, None);
    options.signing_key_file = fixture.base.join("no-such-signing.key");
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("signing key"), "{error}");
    assert_nothing_installed(&dir);
}
