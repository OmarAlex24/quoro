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
/// The statement of `--accept-loss`.
const STATEMENT: &str =
    "no other machine with the node role survives, and no control plane of this cluster runs";
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
            node_data_dir: self.base.join("no-node-agent"),
            accept_loss: None,
            dry_run: false,
        }
    }

    /// [`Fixture::options`] with `--accept-loss` and [`STATEMENT`].
    fn forced_options(&self, data_dir: &Path) -> restore::Options {
        let mut options = self.options(data_dir, None);
        options.accept_loss = Some(STATEMENT.into());
        options
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
    create_nodes(&pool, &[]).await;
    create_change_log(&pool, &[]).await;
    create_leases(&pool, &[]).await;
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

/// A database with a `nodes` table holding `(id, status, cp_status)` rows of
/// machines with the `node` role, the columns `quoro restore` reads, and an
/// empty change log.
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
    let machines: Vec<(&str, &str, &str, Option<&str>)> = nodes
        .iter()
        .map(|(id, status, cp_status)| (*id, r#"["node"]"#, *status, *cp_status))
        .collect();
    make_database_of(path, &machines, log).await
}

/// A database whose `nodes` table holds `(id, roles, status, cp_status)`
/// rows, `roles` as JSON, with the change-log entries `log`.
async fn make_database_of(
    path: &Path,
    machines: &[(&str, &str, &str, Option<&str>)],
    log: &[(i64, &str)],
) -> Vec<u8> {
    make_database_with_leases(path, machines, log, &[]).await
}

/// [`make_database_of`] with the `leases` rows `leases`, as [`LeaseRow`]s.
async fn make_database_with_leases(
    path: &Path,
    machines: &[(&str, &str, &str, Option<&str>)],
    log: &[(i64, &str)],
    leases: &[LeaseRow<'_>],
) -> Vec<u8> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    create_nodes(&pool, machines).await;
    create_change_log(&pool, log).await;
    create_leases(&pool, leases).await;
    let _ = sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        .execute(&pool)
        .await;
    pool.close().await;
    std::fs::read(path).unwrap()
}

/// The `nodes` columns `quoro restore` reads, with the rows `machines` as
/// `(id, roles, status, cp_status)`.
async fn create_nodes(pool: &sqlx::SqlitePool, machines: &[(&str, &str, &str, Option<&str>)]) {
    sqlx::query(
        "CREATE TABLE nodes (id TEXT PRIMARY KEY, roles TEXT NOT NULL, status TEXT NOT NULL, \
         cp_status TEXT)",
    )
    .execute(pool)
    .await
    .unwrap();
    for (id, roles, status, cp_status) in machines {
        sqlx::query("INSERT INTO nodes (id, roles, status, cp_status) VALUES (?, ?, ?, ?)")
            .bind(id)
            .bind(roles)
            .bind(status)
            .bind(cp_status)
            .execute(pool)
            .await
            .unwrap();
    }
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

/// A row of the restored `leases` table: `(id, holder_node_id,
/// fencing_token, revoked)`.
type LeaseRow<'a> = (&'a str, &'a str, i64, bool);

/// The `leases` columns `quoro restore --accept-loss` reads, with the rows
/// `leases`.
async fn create_leases(pool: &sqlx::SqlitePool, leases: &[LeaseRow<'_>]) {
    sqlx::query(
        "CREATE TABLE leases (id TEXT PRIMARY KEY, holder_node_id TEXT NOT NULL, \
         fencing_token INTEGER NOT NULL, revoked_at TEXT)",
    )
    .execute(pool)
    .await
    .unwrap();
    for (id, holder, token, revoked) in leases {
        sqlx::query(
            "INSERT INTO leases (id, holder_node_id, fencing_token, revoked_at) VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(holder)
        .bind(token)
        .bind(revoked.then_some(OLD))
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
/// the new host's node, another node and a removed one. The two other live
/// nodes refuse a restore without `--accept-loss` (ADR 0261), so the tests of
/// the pick that restore give the flag.
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
    let mut options = fixture.forced_options(&dir);
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
    let mut options = fixture.forced_options(&dir);
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
        let mut options = fixture.forced_options(&dir);
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

    let report = restore::execute(&fixture.store, &fixture.forced_options(&dir))
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
    let mut options = fixture.forced_options(&dir);
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
    let mut options = fixture.forced_options(&dir);
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
        // A cluster whose only node is the new host's own restores without
        // --accept-loss.
        &[(NEW_NODE, "connected", Some("primary"))],
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

// ---------------------------------------------------------------------------
// The refusal and --accept-loss (section 8.2, "When a restore is refused" and
// "Override"; ADR 0261)
// ---------------------------------------------------------------------------

/// A node enrolled after the backup: only this host's node agent knows it.
const LATE_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0F";
/// A machine with the `edge` role only.
const EDGE_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0G";
/// A machine with the `witness` role only.
const WITNESS_NODE: &str = "nod_01JC9Z1A2B3C4D5E6F7G8H9J0H";

/// A machine of the restored `nodes` table: `(id, roles, status, cp_status)`.
type Machine<'a> = (&'a str, &'a str, &'a str, Option<&'a str>);

/// The new host's node as the only live machine with the `node` role, next
/// to a removed node, an edge-only and a witness-only machine.
const ONLY_THE_HOSTS_NODE: [Machine<'static>; 4] = [
    (NEW_NODE, r#"["node"]"#, "connected", Some("primary")),
    (REMOVED_NODE, r#"["node"]"#, "removed", None),
    (EDGE_NODE, r#"["edge"]"#, "connected", None),
    (WITNESS_NODE, r#"["witness"]"#, "connected", None),
];

/// A fixture whose one backup holds `machines` and an empty change log.
async fn machines_fixture(machines: &[Machine<'_>]) -> Fixture {
    let fixture = Fixture::new().await;
    let database = make_database_of(&fixture.base.join("machines.db"), machines, &[]).await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    fixture
}

/// One stored desired state: its `rev` and the `(node_id, roles)` of its
/// `spec.votes.voters`.
type Revision<'a> = (u64, &'a [(&'a str, &'a [&'a str])]);

/// Writes a node agent store into the new directory `name`, as the agent
/// stores desired state (`quoro_node_agent::store::NodeStore`, bodies sealed
/// with its node-local key), one per revision, and returns the directory.
async fn agent_store(fixture: &Fixture, name: &str, revisions: &[Revision<'_>]) -> PathBuf {
    let dir = fixture.base.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let store = quoro_node_agent::store::NodeStore::open(&dir)
        .await
        .unwrap();
    for (rev, voters) in revisions {
        let voters: Vec<serde_json::Value> = voters
            .iter()
            .map(|(node_id, roles)| {
                serde_json::json!({ "node_id": node_id, "roles": roles, "mesh_address": null })
            })
            .collect();
        let body = serde_json::json!({
            "rev": rev,
            "issued_at": NEW,
            "term": 1,
            "spec": { "votes": { "version": rev, "port": 51821, "voters": voters } },
            "signature": "",
        });
        store.store_desired(*rev, &body).await.unwrap();
    }
    store.pool().close().await;
    dir
}

fn forced_marker(head: Head) -> Marker {
    Marker {
        head,
        accept_loss: true,
        statement: Some(STATEMENT.to_string()),
    }
}

fn genesis() -> Head {
    Head {
        seq: 0,
        hash: GENESIS_HASH.to_string(),
    }
}

/// Another live machine with the `node` role in the restored database
/// refuses the restore before anything is installed. The new host's node, a
/// removed node and machines without the `node` role do not count.
#[tokio::test]
async fn a_restored_database_with_another_live_node_is_refused() {
    let mut machines = ONLY_THE_HOSTS_NODE.to_vec();
    machines.push((OTHER_NODE, r#"["node","edge"]"#, "disconnected", None));
    let fixture = machines_fixture(&machines).await;
    let dir = fixture.data_dir("another-node");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(matches!(error, restore::Error::Failed(_)), "{error:?}");
    let text = error.to_string();
    assert!(text.contains("refusing to restore"), "{text}");
    assert!(
        text.contains(&format!("restored database holds {OTHER_NODE}")),
        "{text}"
    );
    for id in [REMOVED_NODE, EDGE_NODE, WITNESS_NODE] {
        assert!(!text.contains(id), "{id} does not count: {text}");
    }
    assert!(text.contains("--accept-loss --statement"), "{text}");
    assert_nothing_installed(&dir);
}

/// A cluster whose only node is the new host's own restores as before, also
/// when the host's node agent stored voters: a removed node and a
/// witness-only machine among them do not count either. The marker is a
/// plain one.
#[tokio::test]
async fn only_the_new_hosts_node_restores_as_before() {
    let fixture = machines_fixture(&ONLY_THE_HOSTS_NODE).await;
    let dir = fixture.data_dir("only-the-host");
    let mut options = fixture.options(&dir, None);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);
    options.node_data_dir = agent_store(
        &fixture,
        "agent",
        &[(
            3,
            &[
                (NEW_NODE, &["node"]),
                (REMOVED_NODE, &["node"]),
                (WITNESS_NODE, &["witness"]),
            ],
        )],
    )
    .await;

    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.survivors.is_empty(), "{:?}", report.survivors);
    assert!(!report.accept_loss);
    let marker =
        Marker::parse(&std::fs::read(dir.join(restore_pending::MARKER_FILE)).unwrap()).unwrap();
    assert_eq!(marker, Marker::plain(genesis()));
}

/// A node enrolled after the backup is not in the restored database, but the
/// newest `spec.votes.voters` this host's node agent stored names it: the
/// restore is refused. Only the newest stored desired state counts.
#[tokio::test]
async fn the_agents_newest_voters_refuse_a_node_enrolled_after_the_backup() {
    let fixture =
        machines_fixture(&[(NEW_NODE, r#"["node"]"#, "connected", Some("primary"))]).await;
    let identity = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);
    let host: (&str, &[&str]) = (NEW_NODE, &["node"]);
    let late: (&str, &[&str]) = (LATE_NODE, &["node"]);

    let dir = fixture.data_dir("late-newest");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity.clone();
    options.node_data_dir = agent_store(
        &fixture,
        "agent-late-newest",
        &[(4, &[host]), (5, &[host, late])],
    )
    .await;
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains(&format!("node agent last stored {LATE_NODE}")),
        "{text}"
    );
    assert_nothing_installed(&dir);

    // The same node in an older revision only: the newest one counts.
    let dir = fixture.data_dir("late-older");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity;
    options.node_data_dir = agent_store(
        &fixture,
        "agent-late-older",
        &[(4, &[host, late]), (5, &[host])],
    )
    .await;
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.survivors.is_empty(), "{:?}", report.survivors);
}

/// A node agent store that exists but cannot be read leaves the restore
/// unable to tell which nodes enrolled after the backup, so it refuses;
/// `--accept-loss` does not read it, and restores.
#[tokio::test]
async fn an_agent_store_that_cannot_be_read_refuses_without_the_flag() {
    let fixture =
        machines_fixture(&[(NEW_NODE, r#"["node"]"#, "connected", Some("primary"))]).await;
    let agent = fixture.base.join("broken-agent");
    std::fs::create_dir_all(&agent).unwrap();
    std::fs::write(agent.join("node.db"), b"not a sqlite file at all").unwrap();
    let identity = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let dir = fixture.data_dir("broken-agent-plain");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity.clone();
    options.node_data_dir = agent.clone();
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot read the newest desired state"),
        "{error}"
    );
    assert_nothing_installed(&dir);

    let dir = fixture.data_dir("broken-agent-forced");
    let mut options = fixture.forced_options(&dir);
    options.identity_file = identity;
    options.node_data_dir = agent;
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.accept_loss);
}

/// A single-node cluster restored onto a fresh host needs the flag: the dead
/// node is not the new host's node, which has none.
#[tokio::test]
async fn a_single_node_cluster_restored_onto_a_fresh_host_needs_the_flag() {
    let fixture =
        machines_fixture(&[(OLD_NODE, r#"["node"]"#, "connected", Some("primary"))]).await;

    let dir = fixture.data_dir("fresh-host-plain");
    let error = restore::execute(&fixture.store, &fixture.options(&dir, None))
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(text.contains(OLD_NODE), "{text}");
    assert!(
        text.contains("this host has no node of the cluster"),
        "{text}"
    );
    assert_nothing_installed(&dir);

    let dir = fixture.data_dir("fresh-host-forced");
    let report = restore::execute(&fixture.store, &fixture.forced_options(&dir))
        .await
        .unwrap();
    assert!(report.installed);
    assert_eq!(report.survivors.in_database, vec![OLD_NODE.to_string()]);
    assert!(!dir.join("primary-node").exists());
}

/// `--accept-loss --statement` restores a backup whose other nodes would
/// refuse it. The marker carries both, and the report lists the machines the
/// primary's first write removes: every live one with the `node` role other
/// than the new host's node.
#[tokio::test]
async fn accept_loss_restores_and_the_marker_carries_the_statement() {
    let fixture = node_fixture().await;
    let identity = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    let dir = fixture.data_dir("plain");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity.clone();
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains(&format!("holds {OLD_NODE}, {OTHER_NODE}")),
        "{text}"
    );
    assert_nothing_installed(&dir);

    let dir = fixture.data_dir("forced");
    let mut options = fixture.forced_options(&dir);
    options.identity_file = identity;
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.accept_loss);
    assert_eq!(
        report.survivors.in_database,
        vec![OLD_NODE.to_string(), OTHER_NODE.to_string()]
    );
    let bytes = std::fs::read(dir.join(restore_pending::MARKER_FILE)).unwrap();
    let marker = Marker::parse(&bytes).unwrap();
    assert_eq!(marker, forced_marker(genesis()));
    assert_eq!(bytes, marker.to_bytes());
    assert_eq!(
        std::fs::read_to_string(dir.join("primary-node"))
            .unwrap()
            .trim(),
        NEW_NODE
    );
}

/// A blank statement is refused as a usage error before anything is
/// touched, also when `execute` is called without the command line.
#[tokio::test]
async fn a_blank_statement_is_refused() {
    let fixture = Fixture::new().await;
    let dir = fixture.data_dir("blank-statement");
    let mut options = fixture.options(&dir, None);
    options.accept_loss = Some(" \n".into());
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(matches!(error, restore::Error::Usage(_)), "{error:?}");
    assert!(!dir.exists());
}

/// `--accept-loss` needs a non-empty `--statement`, `--statement` needs
/// `--accept-loss`, and a statement that starts with `--` is a flag the shell
/// passed in its place: each is a usage error, exit `2`, before any file is
/// read.
#[cfg(unix)]
#[test]
fn the_command_checks_accept_loss_and_its_statement() {
    let base = [
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
    ];
    let cases: [(&[&str], &str); 4] = [
        (&["--accept-loss"], "--accept-loss needs --statement"),
        (
            &["--statement", "nothing survives"],
            "--statement needs --accept-loss",
        ),
        (
            &["--accept-loss", "--statement", "   "],
            "non-empty statement",
        ),
        (
            &["--accept-loss", "--statement", "--dry-run"],
            "starts with --",
        ),
    ];
    for (extra, expected) in cases {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_quoro"))
            .args(base)
            .args(extra)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{extra:?}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{extra:?}: {stderr}");
    }
}

/// The highest fencing token (node-agent-protocol.md section 8.6, "The cap").
const MAX_TOKEN: i64 = (1 << 53) - 1;

/// With `--accept-loss`, a lease the primary's first write would raise that
/// is already at the token cap refuses the restore before anything is
/// installed, a dry run too (node-agent-protocol.md section 8.6, ADR 0325).
/// The refusal names each such lease with its token. A revoked lease, which
/// the raise skips, a lease of the new host's node, which is not removed,
/// and a lease below the cap are not named; without them the restore goes
/// on.
#[tokio::test]
async fn accept_loss_refuses_a_lease_at_the_token_cap_before_installing() {
    const AT_CAP: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0A";
    const ALSO_AT_CAP: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0B";
    const REVOKED_AT_CAP: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0C";
    const HOSTS_AT_CAP: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0D";
    const BELOW: &str = "lse_01JC9Z1A2B3C4D5E6F7G8H9J0E";
    let machines: [Machine<'_>; 3] = [
        (OLD_NODE, r#"["node"]"#, "connected", Some("primary")),
        (NEW_NODE, r#"["node"]"#, "connected", None),
        (OTHER_NODE, r#"["node"]"#, "disconnected", None),
    ];
    let leases: [LeaseRow<'_>; 5] = [
        (AT_CAP, OLD_NODE, MAX_TOKEN, false),
        (ALSO_AT_CAP, OTHER_NODE, MAX_TOKEN, false),
        (REVOKED_AT_CAP, OLD_NODE, MAX_TOKEN, true),
        (HOSTS_AT_CAP, NEW_NODE, MAX_TOKEN, false),
        (BELOW, OTHER_NODE, MAX_TOKEN - 1, false),
    ];
    let fixture = Fixture::new().await;
    let database =
        make_database_with_leases(&fixture.base.join("capped.db"), &machines, &[], &leases).await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    let identity = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);

    for dry_run in [false, true] {
        let dir = fixture.data_dir(&format!("capped-{dry_run}"));
        let mut options = fixture.forced_options(&dir);
        options.identity_file = identity.clone();
        options.dry_run = dry_run;
        let error = restore::execute(&fixture.store, &options)
            .await
            .unwrap_err();
        assert!(matches!(error, restore::Error::Failed(_)), "{error:?}");
        let text = error.to_string();
        assert!(text.contains("--accept-loss"), "{text}");
        for id in [AT_CAP, ALSO_AT_CAP] {
            assert!(
                text.contains(&format!("lease {id} is at fencing token {MAX_TOKEN}")),
                "{id}: {text}"
            );
        }
        for id in [REVOKED_AT_CAP, HOSTS_AT_CAP, BELOW] {
            assert!(!text.contains(id), "{id} is not raised: {text}");
        }
        assert!(text.contains("nothing was installed"), "{text}");
        assert_nothing_installed(&dir);
    }

    // The same cluster without the capped leases restores.
    let fixture = Fixture::new().await;
    let database = make_database_with_leases(
        &fixture.base.join("uncapped.db"),
        &machines,
        &[],
        &leases[2..],
    )
    .await;
    fixture
        .add_backup("bkp_01JC9Z1A2B3C4D5E6F7G8H9J0K", &database, NEW)
        .await;
    let dir = fixture.data_dir("uncapped");
    let mut options = fixture.forced_options(&dir);
    options.identity_file = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert_eq!(
        report.survivors.in_database,
        vec![OLD_NODE.to_string(), OTHER_NODE.to_string()]
    );
}

/// The host's node agent store is not read when its identity file names
/// another cluster: that agent's voters are machines of the other cluster.
/// Here the store cannot even be read, and the restore, which `--node-id`
/// points at this cluster's node, goes on without the flag.
#[tokio::test]
async fn the_agent_store_of_another_cluster_is_not_read() {
    let fixture =
        machines_fixture(&[(NEW_NODE, r#"["node"]"#, "connected", Some("primary"))]).await;
    let agent = fixture.base.join("other-cluster-agent");
    std::fs::create_dir_all(&agent).unwrap();
    std::fs::write(agent.join("node.db"), b"not a sqlite file at all").unwrap();

    let dir = fixture.data_dir("other-cluster-agent-restore");
    let mut options = fixture.options(&dir, None);
    options.node_id = Some(NEW_NODE.to_string());
    options.identity_file = fixture.identity_file("identity.json", OTHER_CLUSTER, NEW_NODE);
    options.node_data_dir = agent.clone();
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.survivors.is_empty(), "{:?}", report.survivors);

    // The same store under an identity file of this cluster is read, and
    // refuses.
    let dir = fixture.data_dir("this-cluster-agent-restore");
    let mut options = fixture.options(&dir, None);
    options.node_id = Some(NEW_NODE.to_string());
    options.identity_file = fixture.identity_file("identity-this.json", CLUSTER, NEW_NODE);
    options.node_data_dir = agent;
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot read the newest desired state"),
        "{error}"
    );
    assert_nothing_installed(&dir);
}

/// The node of an identity file of this cluster is the new host's node even
/// when the restored database does not hold it: it is no survivor among the
/// agent's voters, and no `primary-node` is written. A removed node among
/// those voters does not count either, and another node there still refuses.
#[tokio::test]
async fn an_identity_node_the_database_does_not_hold_is_not_a_surviving_voter() {
    let fixture = machines_fixture(&[
        (REMOVED_NODE, r#"["node"]"#, "removed", None),
        (WITNESS_NODE, r#"["witness"]"#, "connected", None),
    ])
    .await;
    let identity = fixture.identity_file("identity.json", CLUSTER, UNKNOWN_NODE);
    let host: (&str, &[&str]) = (UNKNOWN_NODE, &["node"]);
    let removed: (&str, &[&str]) = (REMOVED_NODE, &["node"]);
    let late: (&str, &[&str]) = (LATE_NODE, &["node"]);

    let dir = fixture.data_dir("unknown-host-node");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity.clone();
    options.node_data_dir =
        agent_store(&fixture, "agent-unknown-host", &[(2, &[host, removed])]).await;
    let report = restore::execute(&fixture.store, &options).await.unwrap();
    assert!(report.installed);
    assert!(report.survivors.is_empty(), "{:?}", report.survivors);
    assert_eq!(report.primary.node_id(), None);
    assert!(!dir.join("primary-node").exists());

    let dir = fixture.data_dir("unknown-host-node-late");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity;
    options.node_data_dir =
        agent_store(&fixture, "agent-unknown-host-late", &[(2, &[host, late])]).await;
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains(&format!("node agent last stored {LATE_NODE}")),
        "{text}"
    );
    assert!(
        text.contains(&format!("other than this host's node {UNKNOWN_NODE}")),
        "{text}"
    );
    assert!(!text.contains(&format!("stored {UNKNOWN_NODE}")), "{text}");
    assert_nothing_installed(&dir);
}

/// The node-local key that seals the agent's stored desired states is
/// loaded, never created: a missing key refuses the restore without the flag
/// and is not created, and a key that cannot be read refuses with its error
/// instead of being taken as missing.
#[cfg(unix)]
#[tokio::test]
async fn the_agents_node_local_key_is_never_created_and_an_unreadable_one_is_an_error() {
    use std::os::unix::fs::PermissionsExt as _;
    let fixture =
        machines_fixture(&[(NEW_NODE, r#"["node"]"#, "connected", Some("primary"))]).await;
    let identity = fixture.identity_file("identity.json", CLUSTER, NEW_NODE);
    let host: (&str, &[&str]) = (NEW_NODE, &["node"]);

    let agent = agent_store(&fixture, "agent-no-key", &[(2, &[host])]).await;
    let key = agent.join("local.key");
    std::fs::remove_file(&key).unwrap();
    let dir = fixture.data_dir("agent-no-key-restore");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity.clone();
    options.node_data_dir = agent;
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("local.key is missing"),
        "{error}"
    );
    assert!(!key.exists(), "the key is not created");
    assert_nothing_installed(&dir);

    let agent = agent_store(&fixture, "agent-unreadable-key", &[(2, &[host])]).await;
    let key = agent.join("local.key");
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&key).is_ok() {
        // Root opens any file: the unreadable case cannot be made here.
        return;
    }
    let dir = fixture.data_dir("agent-unreadable-key-restore");
    let mut options = fixture.options(&dir, None);
    options.identity_file = identity;
    options.node_data_dir = agent;
    let error = restore::execute(&fixture.store, &options)
        .await
        .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("local.key cannot be read"), "{text}");
    assert!(!text.contains("missing"), "{text}");
    assert_nothing_installed(&dir);
}
