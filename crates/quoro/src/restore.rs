//! `quoro restore`: an offline restore of the control-plane database
//! (`docs/contracts/backup-format.md` section 8.2, ADR 0148).
//!
//! The command reads a backup repository straight from its bucket, verifies
//! the seal with the cluster signing key, opens the manifest and the chunks
//! with the repository key the recovery key unwraps, writes the database to a
//! temporary file, checks it with SQLite's `PRAGMA integrity_check`, and only
//! then installs it with the two cluster key files into a fresh data
//! directory. It writes no audit record: a restore is offline
//! (`audit-log-format.md` section 3.5).
//!
//! It also names the node on this host that takes the primary status: the one
//! `--node-id` gives, otherwise the one in this host's node agent identity
//! file. Its id goes to `<data_dir>/primary-node`, which the primary applies
//! and deletes at its next start (section 8.2 step 6).
//!
//! Before it installs the database, it writes the `restore-pending` marker
//! next to it (section 8.2, "Detecting the first start", ADR 0285): the
//! restored log's head `seq` and that entry's hash, and whether
//! `--accept-loss` was given with its statement
//! ([`quoro_control_plane::restore_pending::Marker`]). The primary's first
//! start makes the restored primary's first write from it and deletes it. A
//! restore that stops before the database is installed deletes the marker.
//!
//! **The refusal (section 8.2, "When a restore is refused", ADR 0261).**
//! Before anything is installed, the restore refuses when a machine with the
//! `node` role, not `removed` and other than the new host's node, appears in
//! the restored database or in the newest `spec.votes.voters` this host's
//! node agent stored ([`Survivors`]). Such a cluster recovers by promoting a
//! surviving node. `--accept-loss --statement <text>` restores anyway: the
//! statement goes into the marker, and the primary's first write removes
//! every machine of the first list. The new host's node is the node
//! `primary-node` names; when none is named, the node of this host's node
//! agent identity file when that file names this cluster, even one the
//! restored database does not hold; otherwise the host has no node.
//!
//! With `--accept-loss`, the restore also refuses, before anything is
//! installed, when a lease that the first write would raise is already at the
//! token cap (node-agent-protocol.md section 8.6, ADR 0325), naming each
//! lease and its token ([`leases_at_cap`]).
//!
//! Every secret is read from a file whose mode refuses group and other access,
//! never from a flag or the environment, and no error message carries a key, a
//! credential or the recovery key. [`execute`] is generic over
//! [`ObjectStore`] so the tests drive it against [`MemoryStore`] while the
//! command line builds an [`S3Client`].
//!
//! [`MemoryStore`]: quoro_providers::object_store::MemoryStore

use std::collections::BTreeSet;
use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use quoro_backup::layout;
use quoro_backup::{
    ChunkId, FORMAT, Manifest, OpenConfigError, RecoveryKey, RepoConfig, RepoKey, Restorer, Seal,
    SourceKind, open_manifest, open_newest_config, verify_seal,
};
use quoro_control_plane::primary_status::MARKER_FILE;
use quoro_control_plane::restore_pending::{self, Head, Marker};
use quoro_providers::object_store::ObjectStore;
use quoro_providers::s3::{S3Client, S3Config};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zeroize::Zeroizing;

/// The length of the cluster signing key file: a 32-byte Ed25519 seed
/// (`control-plane/src/keys.rs`).
const SIGNING_KEY_BYTES: usize = 32;
/// The length of `data.key`: a 16-byte key id and a 32-byte key
/// (`control-plane/src/crypto.rs`).
const DATA_KEY_BYTES: usize = 48;
/// The database file inside the data directory.
const DATABASE_FILE: &str = "quoro.db";
/// The temporary database a restore writes before it installs anything.
const TEMP_FILE: &str = ".restore-quoro.db.tmp";
/// The name of the cluster signing key file inside `<data_dir>/keys`.
const SIGNING_KEY_FILE: &str = "signing.key";
/// The name of the cluster data-encryption key file inside `<data_dir>/keys`.
const DATA_KEY_FILE: &str = "data.key";
/// The node agent's store inside its data directory (node-agent-protocol.md
/// section 12; `quoro_node_agent::store`).
const NODE_STORE_FILE: &str = "node.db";
/// The node-local key that seals the desired-state bodies of
/// [`NODE_STORE_FILE`] (`quoro_node_agent::localkey`).
const NODE_LOCAL_KEY_FILE: &str = "local.key";

/// How a restore failed. A usage error exits `2`, any other failure exits `1`
/// (the exit codes of the other commands).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The command line was wrong.
    Usage(String),
    /// The repository, a local file or the database could not be used.
    Failed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) | Self::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

fn failed(message: impl Into<String>) -> Error {
    Error::Failed(message.into())
}

/// What [`execute`] needs. Secrets are values already read from their files.
#[derive(Clone, Debug)]
pub struct Options {
    /// The repository prefix in the bucket.
    pub prefix: String,
    /// The cluster the repository and the seals belong to.
    pub cluster_id: String,
    /// The fresh host's data directory.
    pub data_dir: PathBuf,
    /// The cluster's `keys/signing.key` on this machine.
    pub signing_key_file: PathBuf,
    /// The cluster's `keys/data.key` on this machine.
    pub data_key_file: PathBuf,
    /// The recovery key, parsed from its `qrk1-…` text.
    pub recovery_key: RecoveryKey,
    /// Restore this backup instead of the newest valid one.
    pub backup: Option<String>,
    /// `--node-id`: the node that takes the primary status. The restored
    /// database must hold it, not `removed`, or the restore is refused.
    pub node_id: Option<String>,
    /// This host's node agent identity file, read when `node_id` is `None`.
    /// [`run`] sets `/var/lib/quoro-node/identity.json`.
    pub identity_file: PathBuf,
    /// This host's node agent data directory, whose store holds the newest
    /// desired state the agent stored. [`run`] sets `/var/lib/quoro-node`.
    pub node_data_dir: PathBuf,
    /// `--accept-loss --statement <text>`: the statement, which says that no
    /// other machine with the `node` role survives and that no control plane
    /// of this cluster runs. `None` without `--accept-loss`.
    pub accept_loss: Option<String>,
    /// Do every check and write the database to a scratch file, install
    /// nothing.
    pub dry_run: bool,
}

/// What a successful restore did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// `bkp_<ULID>` of the restored backup.
    pub backup_id: String,
    /// The seal's time.
    pub backup_time: String,
    /// The size of the restored database.
    pub logical_bytes: u64,
    /// The number of chunks it was restored from.
    pub chunks: usize,
    /// Seals of this cluster that did not verify or did not fit.
    pub skipped_seals: usize,
    /// Whether the files were installed. False for `--dry-run`.
    pub installed: bool,
    /// Which node takes the primary status, or why none was named.
    pub primary: PrimaryPick,
    /// The machines that refuse a restore without `--accept-loss`. With it,
    /// the primary's first write removes those of
    /// [`Survivors::in_database`].
    pub survivors: Survivors,
    /// Whether `--accept-loss` was given.
    pub accept_loss: bool,
}

/// The machines with the `node` role, not `removed` and other than the new
/// host's node, that refuse a restore without `--accept-loss` (section 8.2,
/// "When a restore is refused", ADR 0261). Each list is sorted by id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Survivors {
    /// Those the restored database holds. The first write of a restore with
    /// `--accept-loss` removes exactly these.
    pub in_database: Vec<String>,
    /// Those in the newest `spec.votes.voters` this host's node agent stored
    /// that the restored database does not list above and does not hold as
    /// `removed`: nodes enrolled after the backup. Not read with
    /// `--accept-loss`, whose statement covers them, so that an unreadable
    /// agent store cannot stop the override.
    pub in_agent: Vec<String>,
}

impl Survivors {
    /// Whether no machine refuses the restore.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.in_database.is_empty() && self.in_agent.is_empty()
    }
}

/// The node `primary-node` names (section 8.2 step 6), or why no file was
/// written. A dry run reports what it would pick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimaryPick {
    /// The node `--node-id` named.
    NodeIdFlag(String),
    /// The node in this host's node agent identity file.
    IdentityFile(String),
    /// No `primary-node` file. Without `--accept-loss` the primary status
    /// stays where the backup had it; with it, every machine loses the status
    /// in the primary's first write, and the next local enrollment of this
    /// host's node agent takes it. The text says why no file was written.
    Unchanged(String),
}

impl PrimaryPick {
    /// The picked `nod_` id, if any.
    #[must_use]
    pub fn node_id(&self) -> Option<&str> {
        match self {
            Self::NodeIdFlag(id) | Self::IdentityFile(id) => Some(id),
            Self::Unchanged(_) => None,
        }
    }
}

/// The access key and secret of the repository's bucket. Its [`fmt::Debug`]
/// never prints either value.
pub struct Credentials {
    /// The `access_key_id=` line.
    pub access_key_id: String,
    /// The `secret_access_key=` line, wiped on drop.
    pub secret_access_key: Zeroizing<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials([redacted])")
    }
}

/// `quoro restore --endpoint URL --region REGION --bucket NAME --prefix PREFIX
/// --cluster-id CLS --credentials-file PATH --recovery-key-file PATH
/// --signing-key-file PATH --data-key-file PATH --data-dir DIR
/// [--backup BKP] [--node-id NOD] [--accept-loss --statement TEXT]
/// [--dry-run]`.
///
/// `node_data_dir` is this host's node agent data directory
/// (`/var/lib/quoro-node`); its identity file names the node that takes the
/// primary status when `--node-id` is absent, and its store holds the voters
/// the refusal reads.
///
/// # Errors
///
/// [`Error::Usage`] for a bad command line and [`Error::Failed`] for every
/// refusal or failed check of section 8.2.
pub async fn run(args: &[String], node_data_dir: &Path) -> Result<(), Error> {
    let command = parse_args(args)?;
    // The two secret files are checked before the network is touched.
    let credentials = read_credentials(&command.credentials_file)?;
    let recovery_key = read_recovery_key(&command.recovery_key_file)?;

    let client = S3Client::new(S3Config {
        endpoint: command.endpoint,
        region: command.region,
        bucket: command.bucket,
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
    })
    .map_err(|error| failed(format!("cannot use the object store: {error}")))?;

    let report = execute(
        &client,
        &Options {
            prefix: command.prefix,
            cluster_id: command.cluster_id,
            data_dir: command.data_dir,
            signing_key_file: command.signing_key_file,
            data_key_file: command.data_key_file,
            recovery_key,
            backup: command.backup,
            node_id: command.node_id,
            identity_file: node_data_dir.join(quoro_node_agent::identity::IDENTITY_FILE),
            node_data_dir: node_data_dir.to_path_buf(),
            accept_loss: command.accept_loss,
            dry_run: command.dry_run,
        },
    )
    .await?;

    let verb = if report.installed {
        "restored"
    } else {
        "verified"
    };
    println!(
        "{verb} backup {} ({}), {} bytes in {} chunks",
        report.backup_id, report.backup_time, report.logical_bytes, report.chunks
    );
    if report.skipped_seals > 0 {
        println!("skipped {} invalid seals", report.skipped_seals);
    }
    println!("{}", primary_line(&report));
    if report.accept_loss {
        let removes = if report.installed {
            "removes"
        } else {
            "would remove"
        };
        if report.survivors.in_database.is_empty() {
            println!(
                "--accept-loss: the primary's first write {removes} no machine with the node role"
            );
        } else {
            println!(
                "--accept-loss: the primary's first write {removes} these machines with the node role: {}",
                report.survivors.in_database.join(", ")
            );
        }
    }
    if !report.installed {
        println!("dry run: nothing was installed");
    }
    println!(
        "the restored database is as old as the backup; audit verification against the off-site copy will report tail_truncated"
    );
    Ok(())
}

/// The line `quoro restore` prints about the primary status: the node that
/// takes it, or why no `primary-node` file was written and what becomes of the
/// status then.
#[must_use]
pub fn primary_line(report: &Report) -> String {
    let (takes, loses) = if report.installed {
        ("takes", "loses")
    } else {
        ("would take", "would lose")
    };
    match &report.primary {
        PrimaryPick::NodeIdFlag(node_id) => format!(
            "node {node_id} (--node-id) {takes} the primary status at the primary's next start"
        ),
        PrimaryPick::IdentityFile(node_id) => format!(
            "node {node_id} (this host's node agent) {takes} the primary status at the primary's next start"
        ),
        PrimaryPick::Unchanged(reason) if report.accept_loss => format!(
            "no {MARKER_FILE} file: {reason}; every machine {loses} the primary status in the primary's first write, and the next local enrollment of this host's node agent {takes} it"
        ),
        PrimaryPick::Unchanged(reason) => format!(
            "no {MARKER_FILE} file: {reason}; the primary status stays where the backup had it"
        ),
    }
}

/// The restore of section 8.2 over any object store.
///
/// # Errors
///
/// [`Error::Usage`] for a statement [`check_statement`] refuses;
/// [`Error::Failed`] when a refused input, a seal or manifest check, a chunk,
/// the integrity check, the refusal of ADR 0261, with `--accept-loss` a lease
/// at the token cap, or the install fails.
pub async fn execute<S: ObjectStore>(store: &S, options: &Options) -> Result<Report, Error> {
    if let Some(statement) = &options.accept_loss {
        check_statement(statement)?;
    }
    ensure_fresh_data_dir(&options.data_dir)?;
    // From here on, a restore that stops before the database is installed
    // deletes the marker, also one an interrupted earlier run left in this
    // data directory, which holds no database (section 8.2), and also when
    // the key files cannot be read. A dry run installs nothing and leaves the
    // marker alone.
    let mut pending = PendingMarker {
        path: options.data_dir.join(restore_pending::MARKER_FILE),
        armed: !options.dry_run,
    };
    let signing_seed = read_signing_seed(&options.signing_key_file)?;
    let data_key = read_exact(&options.data_key_file, DATA_KEY_BYTES, "data key")?;
    ensure_data_dir(&options.data_dir)?;

    let root = layout::root(&options.prefix, &options.cluster_id);
    let (config, repo_key) =
        choose_config(store, &root, &options.cluster_id, &options.recovery_key).await?;

    let signing = quoro_proto::sign::signing_key_from_seed(&signing_seed);
    check_signing_key(&config, &signing)?;

    let (seal, skipped_seals) = choose_seal(
        store,
        &root,
        &options.cluster_id,
        &signing.verifying_key(),
        options.backup.as_deref(),
    )
    .await?;
    if seal.repository_id != config.repository_id {
        return Err(failed(
            "the seal belongs to another backup repository than the config object",
        ));
    }

    let manifest_key = format!("{root}{}", seal.manifest_key);
    let manifest_object = store
        .get(&manifest_key)
        .await
        .map_err(|error| failed(format!("cannot read the manifest: {error}")))?;
    let source_keys = repo_key.source_keys(&options.cluster_id);
    let manifest = open_manifest(
        &source_keys,
        &seal.backup_id,
        &options.cluster_id,
        &seal.manifest_digest,
        &manifest_object,
    )
    .map_err(|error| failed(format!("the manifest: {error}")))?;
    check_manifest(&manifest, &seal, &config, &options.cluster_id)?;

    let temp = options.data_dir.join(TEMP_FILE);
    // A killed earlier run may have left the scratch file behind.
    remove_temp_files(&temp);
    let mut guard = TempFile {
        path: temp.clone(),
        keep: false,
    };
    let mut restorer = Restorer::create(source_keys.clone(), &temp, manifest.logical_bytes)
        .map_err(|error| failed(format!("cannot write {TEMP_FILE}: {error}")))?;
    restore_chunks(store, &root, &manifest, &mut restorer).await?;
    restorer
        .finish()
        .map_err(|error| failed(format!("cannot finish the database: {error}")))?;

    check_integrity(&temp).await?;
    // An unusable `--node-id` refuses the restore here, before anything is
    // installed (section 8.2).
    let primary = pick_primary(&temp, options).await?;
    // The marker names the database by its log's head (section 8.2, ADR
    // 0285); a file without a change log is no control-plane database.
    let head = read_head(&temp).await?;
    // Section 8.2, "When a restore is refused" (ADR 0261), before anything is
    // installed; a dry run checks it too.
    let host_node = new_host_node(options, &primary);
    let survivors = find_survivors(&temp, options, host_node.as_deref()).await?;
    if options.accept_loss.is_none() && !survivors.is_empty() {
        return Err(refusal(&survivors, host_node.as_deref()));
    }
    // The token cap (node-agent-protocol.md section 8.6, ADR 0325): the
    // first write of `--accept-loss` raises the lease tokens of the machines
    // it removes, so a lease already at the cap refuses here, before anything
    // is installed, as `promote --ack-fence` checks before it changes
    // anything. The first write refuses it too, as a backstop.
    if options.accept_loss.is_some() {
        let at_cap = leases_at_cap(&temp, primary.node_id()).await?;
        if !at_cap.is_empty() {
            return Err(failed(format!(
                "refusing to restore with --accept-loss: the primary's first write raises the \
                 lease tokens of the machines it removes, and {}; nothing was installed",
                restore_pending::token_cap_refusal(&at_cap)
            )));
        }
    }

    let installed = !options.dry_run;
    if installed {
        // SQLite may have touched these while checking the file; they must not
        // be installed next to the database.
        remove_sidecars(&temp);
        let marker = Marker {
            head,
            accept_loss: options.accept_loss.is_some(),
            statement: options.accept_loss.clone(),
        };
        if let Err(error) = install(
            options,
            &temp,
            &signing_seed,
            &data_key,
            &marker,
            primary.node_id(),
        ) {
            remove_install(&options.data_dir);
            return Err(error);
        }
        guard.keep = true;
        pending.armed = false;
    }

    Ok(Report {
        backup_id: seal.backup_id,
        backup_time: seal.sealed_at,
        logical_bytes: manifest.logical_bytes,
        chunks: manifest.chunks.len(),
        skipped_seals,
        installed,
        primary,
        survivors,
        accept_loss: options.accept_loss.is_some(),
    })
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Reads a `key=value` credentials file that only its owner may read.
///
/// # Errors
///
/// [`Error::Failed`] when the file is readable by group or others, cannot be
/// read, or does not hold exactly one `access_key_id` and one
/// `secret_access_key`.
pub fn read_credentials(path: &Path) -> Result<Credentials, Error> {
    check_private(path, "credentials")?;
    let text = Zeroizing::new(
        std::fs::read_to_string(path)
            .map_err(|error| failed(format!("cannot read the credentials file: {error}")))?,
    );
    let mut access_key_id = None;
    let mut secret_access_key = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            return Err(failed(
                "the credentials file has a line without '='; expected access_key_id=… and secret_access_key=…",
            ));
        };
        let value = value.trim();
        if value.is_empty() {
            return Err(failed(format!(
                "the credentials file has an empty {}",
                name.trim()
            )));
        }
        match name.trim() {
            "access_key_id" => access_key_id = Some(value.to_string()),
            "secret_access_key" => secret_access_key = Some(Zeroizing::new(value.to_string())),
            _ => {
                return Err(failed(
                    "the credentials file names a field other than access_key_id or secret_access_key",
                ));
            }
        }
    }
    let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) else {
        return Err(failed(
            "the credentials file needs access_key_id and secret_access_key",
        ));
    };
    Ok(Credentials {
        access_key_id,
        secret_access_key,
    })
}

fn read_recovery_key(path: &Path) -> Result<RecoveryKey, Error> {
    check_private(path, "recovery key")?;
    let text = Zeroizing::new(
        std::fs::read_to_string(path)
            .map_err(|error| failed(format!("cannot read the recovery key file: {error}")))?,
    );
    RecoveryKey::parse(&text)
        .map_err(|_| failed("the recovery key file does not hold a valid qrk1- key"))
}

fn read_signing_seed(path: &Path) -> Result<[u8; SIGNING_KEY_BYTES], Error> {
    let bytes = read_exact(path, SIGNING_KEY_BYTES, "signing key")?;
    bytes
        .try_into()
        .map_err(|_| failed("the signing key file is not 32 bytes"))
}

fn read_exact(path: &Path, expected: usize, what: &str) -> Result<Vec<u8>, Error> {
    let bytes = std::fs::read(path)
        .map_err(|error| failed(format!("cannot read the {what} file: {error}")))?;
    if bytes.len() != expected {
        return Err(failed(format!(
            "the {what} file is {} bytes, not {expected}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Refuses to overwrite a live primary: the database or either key file must
/// not exist yet (section 8.2).
fn ensure_fresh_data_dir(data_dir: &Path) -> Result<(), Error> {
    let mut candidates = vec![data_dir.join(DATABASE_FILE)];
    for name in [SIGNING_KEY_FILE, DATA_KEY_FILE] {
        candidates.push(data_dir.join("keys").join(name));
    }
    for path in candidates {
        let exists = path
            .try_exists()
            .map_err(|error| failed(format!("cannot check {}: {error}", path.display())))?;
        if exists {
            return Err(failed(format!(
                "{} already exists; refusing to overwrite a live primary",
                path.display()
            )));
        }
    }
    Ok(())
}

fn ensure_data_dir(dir: &Path) -> Result<(), Error> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .map_err(|error| failed(format!("cannot create {}: {error}", dir.display())))
}

fn check_private(path: &Path, what: &str) -> Result<(), Error> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| failed(format!("cannot read the {what} file: {error}")))?;
    if !is_private(&metadata.permissions()) {
        return Err(failed(format!(
            "the {what} file is readable by group or others; chmod 600 it"
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn is_private(permissions: &std::fs::Permissions) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    permissions.mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn is_private(_: &std::fs::Permissions) -> bool {
    true
}

// ---------------------------------------------------------------------------
// Config and seal
// ---------------------------------------------------------------------------

/// Unwraps `repo_key` from the newest config the recovery key opens
/// (section 8.2 step 1), with the reader a primary also uses to restore the
/// key ([`open_newest_config`]).
async fn choose_config<S: ObjectStore>(
    store: &S,
    root: &str,
    cluster_id: &str,
    recovery: &RecoveryKey,
) -> Result<(RepoConfig, RepoKey), Error> {
    let listed = store
        .list(&format!("{root}config-"))
        .await
        .map_err(|error| failed(format!("cannot list the config objects: {error}")))?;
    open_newest_config(
        root,
        listed.into_iter().map(|object| object.key),
        cluster_id,
        None,
        recovery,
        |key| async move { store.get(&key).await },
    )
    .await
    .map_err(|error| match error {
        // `quoro restore` reports config objects it cannot read as ones the
        // key does not open, as it always has.
        OpenConfigError::Unreadable(_) => failed(OpenConfigError::NoneOpens.to_string()),
        other => failed(other.to_string()),
    })
}

/// The config's `cluster_public_key` must be the public half of the given
/// signing key (section 8.2 step 2).
fn check_signing_key(
    config: &RepoConfig,
    signing: &quoro_proto::sign::SigningKey,
) -> Result<(), Error> {
    let expected = quoro_proto::sign::public_key_from_base64(&config.cluster_public_key)
        .map_err(|_| failed("the config object has no valid cluster public key"))?;
    if expected != signing.verifying_key() {
        return Err(failed(
            "the signing key file does not match the cluster's public key",
        ));
    }
    Ok(())
}

struct Candidate {
    seal: Seal,
    time: OffsetDateTime,
}

/// Picks the newest valid seal, or the one `wanted` names (section 8.2 step 3).
/// Returns it and how many seals did not verify or did not fit.
async fn choose_seal<S: ObjectStore>(
    store: &S,
    root: &str,
    cluster_id: &str,
    verifying: &quoro_proto::sign::VerifyingKey,
    wanted: Option<&str>,
) -> Result<(Seal, usize), Error> {
    let prefix = format!("{root}seals/{cluster_id}/");
    let listed = store
        .list(&prefix)
        .await
        .map_err(|error| failed(format!("cannot list the seals: {error}")))?;
    let mut skipped = 0;
    let mut candidates: Vec<Candidate> = Vec::new();
    for object in listed {
        let seal = match read_seal(store, &object.key, verifying).await {
            Some(seal) => seal,
            None => {
                skipped += 1;
                continue;
            }
        };
        if seal.format != FORMAT
            || seal.cluster_id != cluster_id
            || seal.source_kind != SourceKind::ControlPlane
            || seal.source_id != cluster_id
            || seal.manifest_key != layout::manifest(cluster_id, &seal.backup_id)
            || object.key != format!("{root}{}", layout::seal(cluster_id, &seal.backup_id))
        {
            skipped += 1;
            continue;
        }
        let time = match OffsetDateTime::parse(seal.sealed_at.trim(), &Rfc3339) {
            Ok(time) => time,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        candidates.push(Candidate { seal, time });
    }

    if let Some(backup_id) = wanted {
        return match candidates
            .into_iter()
            .find(|candidate| candidate.seal.backup_id == backup_id)
        {
            Some(candidate) => Ok((candidate.seal, skipped)),
            None => Err(failed(format!(
                "the repository has no valid seal for backup {backup_id}"
            ))),
        };
    }
    match candidates
        .into_iter()
        .max_by_key(|candidate| candidate.time)
    {
        Some(candidate) => Ok((candidate.seal, skipped)),
        None => Err(failed("the repository has no valid seal for this cluster")),
    }
}

async fn read_seal<S: ObjectStore>(
    store: &S,
    key: &str,
    verifying: &quoro_proto::sign::VerifyingKey,
) -> Option<Seal> {
    let bytes = store.get(key).await.ok()?;
    let seal: Seal = serde_json::from_slice(&bytes).ok()?;
    verify_seal(&seal, verifying).ok()?;
    Some(seal)
}

fn check_manifest(
    manifest: &Manifest,
    seal: &Seal,
    config: &RepoConfig,
    cluster_id: &str,
) -> Result<(), Error> {
    if manifest.source_kind != SourceKind::ControlPlane {
        return Err(failed(
            "the manifest does not describe a control-plane backup",
        ));
    }
    if manifest.cluster_id != cluster_id {
        return Err(failed("the manifest belongs to another cluster"));
    }
    if manifest.repository_id != config.repository_id {
        return Err(failed("the manifest belongs to another repository"));
    }
    if manifest.logical_bytes != seal.logical_bytes {
        return Err(failed("the manifest's size does not match the seal"));
    }
    if manifest.chunks.len() as u64 != seal.chunk_count {
        return Err(failed("the manifest's chunk count does not match the seal"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Chunks, integrity and install
// ---------------------------------------------------------------------------

/// Reads, opens and writes every chunk of the manifest in order
/// (`backup-format.md` section 8.1 step 2). All-zero chunks are stored like
/// any other (section 5); [`Restorer::apply`] writes them back as holes.
async fn restore_chunks<S: ObjectStore>(
    store: &S,
    root: &str,
    manifest: &Manifest,
    restorer: &mut Restorer,
) -> Result<(), Error> {
    for entry in &manifest.chunks {
        let id = ChunkId::from_hex(&entry.id)
            .map_err(|error| failed(format!("the manifest's chunk id: {error}")))?;
        let key = format!("{root}{}", layout::chunk(&id));
        let object = store
            .get(&key)
            .await
            .map_err(|error| failed(format!("cannot read chunk {id}: {error}")))?;
        restorer
            .apply(entry, &object)
            .map_err(|error| failed(format!("the chunk at {}: {error}", entry.offset)))?;
    }
    Ok(())
}

/// SQLite's own opinion of the restored file: exactly one `ok` (section 8.2
/// step 5).
async fn check_integrity(path: &Path) -> Result<(), Error> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| failed(format!("cannot open the restored database: {error}")))?;
    let rows: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_all(&pool)
        .await
        .map_err(|error| failed(format!("cannot check the restored database: {error}")))?;
    pool.close().await;
    if rows.len() != 1 || rows[0] != "ok" {
        return Err(failed(
            "the restored database failed SQLite's integrity check",
        ));
    }
    Ok(())
}

/// Copies the two key files into `<data_dir>/keys` with mode `0600`, writes
/// the `restore-pending` marker `0600` and syncs it into the directory,
/// writes `primary-node` `0600` when a node was picked, and only then renames
/// the checked database into place (section 8.2 steps 5 and 6, "Detecting
/// the first start"). Every file is this process's, so the marker has the
/// owner of `primary-node` and of the database.
fn install(
    options: &Options,
    temp: &Path,
    signing_seed: &[u8; SIGNING_KEY_BYTES],
    data_key: &[u8],
    marker: &Marker,
    primary_node: Option<&str>,
) -> Result<(), Error> {
    let keys_dir = options.data_dir.join("keys");
    create_private_dir(&keys_dir)?;
    write_private_file(&keys_dir.join(SIGNING_KEY_FILE), signing_seed)?;
    write_private_file(&keys_dir.join(DATA_KEY_FILE), data_key)?;

    // Files an interrupted earlier run left behind must not outlive this
    // restore: no database was installed then, so they named nothing real.
    let pending = options.data_dir.join(restore_pending::MARKER_FILE);
    remove_if_present(&pending)?;
    write_private_file(&pending, &marker.to_bytes())?;
    // No restored database is installed without its marker, also after a
    // power loss (section 8.2).
    sync_dir(&options.data_dir)?;

    let primary = options.data_dir.join(MARKER_FILE);
    remove_if_present(&primary)?;
    if let Some(node_id) = primary_node {
        write_private_file(&primary, format!("{node_id}\n").as_bytes())?;
    }

    let database = options.data_dir.join(DATABASE_FILE);
    std::fs::rename(temp, &database)
        .map_err(|error| failed(format!("cannot install {}: {error}", database.display())))?;
    set_private_mode(&database)?;
    sync_dir(&options.data_dir)?;
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(failed(format!("cannot remove {}: {error}", path.display()))),
    }
}

fn remove_install(data_dir: &Path) {
    let _ = std::fs::remove_file(data_dir.join(DATABASE_FILE));
    let _ = std::fs::remove_file(data_dir.join(MARKER_FILE));
    let _ = std::fs::remove_file(data_dir.join(restore_pending::MARKER_FILE));
    let _ = std::fs::remove_file(data_dir.join("keys").join(SIGNING_KEY_FILE));
    let _ = std::fs::remove_file(data_dir.join("keys").join(DATA_KEY_FILE));
}

// ---------------------------------------------------------------------------
// The node that takes the primary status
// ---------------------------------------------------------------------------

/// The two members of the node agent's `identity.json` the pick needs
/// (`quoro_node_agent::identity::Identity`).
#[derive(serde::Deserialize)]
struct HostIdentity {
    node_id: String,
    cluster_id: String,
}

/// Picks the node `primary-node` names (section 8.2 step 6) from the checked
/// database at `database`.
///
/// # Errors
///
/// [`Error::Failed`] when `--node-id` names a node the database does not hold
/// or holds as `removed`, or the database cannot be read. Every problem with
/// the identity file is a [`PrimaryPick::Unchanged`] instead.
async fn pick_primary(database: &Path, options: &Options) -> Result<PrimaryPick, Error> {
    if let Some(node_id) = &options.node_id {
        return match node_status(database, node_id).await? {
            None => Err(failed(format!(
                "--node-id {node_id}: the restored database holds no such node"
            ))),
            Some(status) if status == "removed" => Err(failed(format!(
                "--node-id {node_id}: the node is removed in the restored database"
            ))),
            Some(_) => Ok(PrimaryPick::NodeIdFlag(node_id.clone())),
        };
    }

    let path = &options.identity_file;
    let identity = match read_identity(path) {
        Ok(identity) => identity,
        Err(reason) => return Ok(PrimaryPick::Unchanged(reason)),
    };
    if identity.cluster_id != options.cluster_id {
        return Ok(PrimaryPick::Unchanged(format!(
            "this host's node agent belongs to cluster {}, not {}",
            identity.cluster_id, options.cluster_id
        )));
    }
    if !quoro_types::is_id(quoro_types::Prefix::Node, &identity.node_id) {
        return Ok(PrimaryPick::Unchanged(format!(
            "the node agent identity file {} does not hold a nod_ id",
            path.display()
        )));
    }
    Ok(match node_status(database, &identity.node_id).await? {
        None => PrimaryPick::Unchanged(format!(
            "the restored database holds no node {} (this host's node agent)",
            identity.node_id
        )),
        Some(status) if status == "removed" => PrimaryPick::Unchanged(format!(
            "node {} (this host's node agent) is removed in the restored database",
            identity.node_id
        )),
        Some(_) => PrimaryPick::IdentityFile(identity.node_id),
    })
}

/// This host's node agent identity file at `path`, or why it names no node:
/// it is missing, cannot be read or is malformed.
fn read_identity(path: &Path) -> Result<HostIdentity, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "no --node-id and no node agent identity file at {}",
                path.display()
            ));
        }
        Err(error) => {
            return Err(format!(
                "cannot read the node agent identity file {}: {error}",
                path.display()
            ));
        }
    };
    serde_json::from_str(&text).map_err(|error| {
        format!(
            "the node agent identity file {} is malformed: {error}",
            path.display()
        )
    })
}

// ---------------------------------------------------------------------------
// The refusal of ADR 0261
// ---------------------------------------------------------------------------

/// The new host's node (section 8.2): the node `primary-node` names, the one
/// `--node-id` or the identity file gave; otherwise the node of this host's
/// node agent identity file when it names this cluster, even one the
/// restored database does not hold or holds as `removed`, since that agent is
/// still this host's own. `None` when the host has no node of the cluster.
fn new_host_node(options: &Options, primary: &PrimaryPick) -> Option<String> {
    if let Some(node_id) = primary.node_id() {
        return Some(node_id.to_string());
    }
    read_identity(&options.identity_file)
        .ok()
        .filter(|identity| {
            identity.cluster_id == options.cluster_id
                && quoro_types::is_id(quoro_types::Prefix::Node, &identity.node_id)
        })
        .map(|identity| identity.node_id)
}

/// Whether `roles` hold the `node` role.
fn has_node_role(roles: &[String]) -> bool {
    roles.iter().any(|role| role == "node")
}

/// The machines that refuse the restore (section 8.2, "When a restore is
/// refused"), from the restored database at `database` and, without
/// `--accept-loss`, from this host's node agent store. `host_node` is the new
/// host's node ([`new_host_node`]), which never counts.
///
/// The agent store is not read when the identity file names another
/// cluster: that agent's voters are machines of the other cluster.
///
/// # Errors
///
/// [`Error::Failed`] when the restored nodes or, without `--accept-loss`,
/// the agent's newest desired state cannot be read.
async fn find_survivors(
    database: &Path,
    options: &Options,
    host_node: Option<&str>,
) -> Result<Survivors, Error> {
    let machines = restored_machines(database).await?;
    let removed: BTreeSet<&str> = machines
        .iter()
        .filter(|machine| machine.status == "removed")
        .map(|machine| machine.id.as_str())
        .collect();
    let in_database: Vec<String> = machines
        .iter()
        .filter(|machine| {
            machine.status != "removed"
                && has_node_role(&machine.roles)
                && Some(machine.id.as_str()) != host_node
        })
        .map(|machine| machine.id.clone())
        .collect();

    let other_cluster = read_identity(&options.identity_file)
        .is_ok_and(|identity| identity.cluster_id != options.cluster_id);
    let voters = if options.accept_loss.is_some() || other_cluster {
        Vec::new()
    } else {
        agent_voters(&options.node_data_dir).await?
    };
    let in_agent: BTreeSet<String> = voters
        .into_iter()
        .filter(|voter| {
            has_node_role(&voter.roles)
                && Some(voter.node_id.as_str()) != host_node
                && !removed.contains(voter.node_id.as_str())
                && !in_database.contains(&voter.node_id)
        })
        .map(|voter| voter.node_id)
        .collect();
    Ok(Survivors {
        in_database,
        in_agent: in_agent.into_iter().collect(),
    })
}

/// The refusal of a restore that [`Survivors`] lists, with the way out.
fn refusal(survivors: &Survivors, host_node: Option<&str>) -> Error {
    let mut found = Vec::new();
    if !survivors.in_database.is_empty() {
        found.push(format!(
            "the restored database holds {}",
            survivors.in_database.join(", ")
        ));
    }
    if !survivors.in_agent.is_empty() {
        found.push(format!(
            "this host's node agent last stored {} among the voters",
            survivors.in_agent.join(", ")
        ));
    }
    let host = match host_node {
        Some(node_id) => format!("other than this host's node {node_id}"),
        None => "and this host has no node of the cluster".to_string(),
    };
    failed(format!(
        "refusing to restore: machines with the node role that are not removed may survive \
         ({host}): {}. A restored log may lack writes every node already holds, so such a \
         cluster recovers by promoting a surviving node (quoro cluster promote). If no other \
         machine with the node role survives and no control plane of this cluster runs, \
         restore with --accept-loss --statement <text>; the primary's first write then \
         removes the machines the restored database holds",
        found.join("; ")
    ))
}

/// One machine of the restored database's `nodes` table.
struct Machine {
    id: String,
    roles: Vec<String>,
    status: String,
}

/// Every machine of the restored database at `path`, by id, read-only.
async fn restored_machines(path: &Path) -> Result<Vec<Machine>, Error> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| failed(format!("cannot open the restored database: {error}")))?;
    let rows: Result<Vec<(String, String, String)>, sqlx::Error> =
        sqlx::query_as("SELECT id, roles, status FROM nodes ORDER BY id")
            .fetch_all(&pool)
            .await;
    pool.close().await;
    let rows = rows.map_err(|error| failed(format!("cannot read the restored nodes: {error}")))?;
    rows.into_iter()
        .map(|(id, roles, status)| -> Result<Machine, Error> {
            let roles: Vec<String> = serde_json::from_str(&roles).map_err(|error| {
                failed(format!(
                    "the roles of the restored node {id} are not a list: {error}"
                ))
            })?;
            Ok(Machine { id, roles, status })
        })
        .collect()
}

/// The members of a stored desired state the refusal reads: one voter of
/// `spec.votes.voters` (node-agent-protocol.md section 6.12).
#[derive(serde::Deserialize)]
struct StoredVoter {
    node_id: String,
    roles: Vec<String>,
}

#[derive(serde::Deserialize)]
struct StoredVotes {
    voters: Vec<StoredVoter>,
}

#[derive(serde::Deserialize)]
struct StoredSpec {
    #[serde(default)]
    votes: Option<StoredVotes>,
}

#[derive(serde::Deserialize)]
struct StoredDesiredState {
    spec: StoredSpec,
}

/// A `desired_state` row of the node agent store: `rev`, the plaintext
/// `body` of a row written before bodies were sealed, and the sealed
/// `body_enc`.
type StoredRow = (i64, String, Option<Vec<u8>>);

/// The newest `spec.votes.voters` this host's node agent stored: the
/// highest `rev` of the `desired_state` table of its store in
/// `node_data_dir` (node-agent-protocol.md section 12), opened read-only and
/// opened with the agent's node-local key when sealed. No store, no stored
/// desired state, or one without `spec.votes`, gives no voter.
///
/// # Errors
///
/// [`Error::Failed`] when the store exists but its newest desired state
/// cannot be read: the refusal cannot then tell which nodes enrolled after
/// the backup. The node-local key is never created here.
async fn agent_voters(node_data_dir: &Path) -> Result<Vec<StoredVoter>, Error> {
    let path = node_data_dir.join(NODE_STORE_FILE);
    let exists = path
        .try_exists()
        .map_err(|error| failed(format!("cannot check {}: {error}", path.display())))?;
    if !exists {
        return Ok(Vec::new());
    }
    let unreadable = |reason: String| {
        failed(format!(
            "cannot read the newest desired state this host's node agent stored in {}: \
             {reason}; without it the restore cannot tell which nodes enrolled after the backup",
            path.display()
        ))
    };
    let options = SqliteConnectOptions::new()
        .filename(path.as_path())
        .read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| unreadable(error.to_string()))?;
    let row: Result<Option<StoredRow>, sqlx::Error> =
        sqlx::query_as("SELECT rev, body, body_enc FROM desired_state ORDER BY rev DESC LIMIT 1")
            .fetch_optional(&pool)
            .await;
    pool.close().await;
    let Some((rev, body, sealed)) = row.map_err(|error| unreadable(error.to_string()))? else {
        return Ok(Vec::new());
    };
    let body = match sealed {
        None => body,
        Some(blob) => {
            let key = load_local_key(node_data_dir).map_err(unreadable)?;
            let rev =
                u64::try_from(rev).map_err(|_| unreadable(format!("its rev {rev} is negative")))?;
            let plaintext = key
                .open(rev, &blob)
                .map_err(|error| unreadable(error.to_string()))?;
            String::from_utf8(plaintext).map_err(|_| unreadable("it is not UTF-8".to_string()))?
        }
    };
    let state: StoredDesiredState = serde_json::from_str(&body)
        .map_err(|error| unreadable(format!("it is not a desired state: {error}")))?;
    Ok(state
        .spec
        .votes
        .map(|votes| votes.voters)
        .unwrap_or_default())
}

/// The agent's node-local key in `node_data_dir`, which seals the stored
/// desired states. A restore must not create it: an agent without one stored
/// no sealed state this restore can read.
///
/// The key file is opened first, so a missing file is reported missing and
/// one that cannot be opened, such as for a permission error, is reported
/// with its error, never taken as missing. The agent's own loader then reads
/// the file just opened. node-agent offers no loader that only loads, so a
/// key deleted between the two calls would be created again.
///
/// # Errors
///
/// A one-line reason when the file is missing, cannot be read or is not a
/// key.
fn load_local_key(node_data_dir: &Path) -> Result<quoro_node_agent::localkey::LocalKey, String> {
    let key_file = node_data_dir.join(NODE_LOCAL_KEY_FILE);
    match std::fs::File::open(&key_file) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "it is sealed and {} is missing",
                key_file.display()
            ));
        }
        Err(error) => {
            return Err(format!(
                "it is sealed and {} cannot be read: {error}",
                key_file.display()
            ));
        }
    }
    quoro_node_agent::localkey::LocalKey::load_or_create(node_data_dir)
        .map_err(|error| format!("{}: {error}", key_file.display()))
}

/// Every lease the first write of a restore with `--accept-loss` would raise
/// that is already at the token cap, as `(lease id, token)`, read-only from
/// the restored database at `path`: the leases held, not revoked, by the
/// machines that write removes when `primary-node` names `host_node`
/// ([`restore_pending::to_remove`], [`restore_pending::leases_at_cap`], the
/// same reads the primary's start makes).
///
/// # Errors
///
/// [`Error::Failed`] when the restored nodes or leases cannot be read.
async fn leases_at_cap(path: &Path, host_node: Option<&str>) -> Result<Vec<(String, i64)>, Error> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| failed(format!("cannot open the restored database: {error}")))?;
    let at_cap = match pool.acquire().await {
        Ok(mut conn) => match restore_pending::to_remove(&mut conn, host_node).await {
            Ok(lost) => restore_pending::leases_at_cap(&mut conn, &lost)
                .await
                .map_err(|error| format!("cannot read the restored lease tokens: {error}")),
            Err(reason) => Err(reason),
        },
        Err(error) => Err(format!("cannot read the restored database: {error}")),
    };
    pool.close().await;
    at_cap.map_err(failed)
}

/// The head of the change log in the database at `path`, read-only: the
/// newest entry's `seq` and hash, or `seq` 0 and the genesis hash for an
/// empty log.
async fn read_head(path: &Path) -> Result<Head, Error> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| failed(format!("cannot open the restored database: {error}")))?;
    let head = match pool.acquire().await {
        Ok(mut conn) => restore_pending::log_head(&mut conn).await,
        Err(error) => Err(error),
    };
    pool.close().await;
    head.map_err(|error| {
        failed(format!(
            "cannot read the restored change log's head: {error}"
        ))
    })
}

/// The `status` of node `node_id` in the database at `path`, read-only.
async fn node_status(path: &Path, node_id: &str) -> Result<Option<String>, Error> {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| failed(format!("cannot open the restored database: {error}")))?;
    let status: Result<Option<String>, sqlx::Error> =
        sqlx::query_scalar("SELECT status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&pool)
            .await;
    pool.close().await;
    status.map_err(|error| failed(format!("cannot read the restored nodes: {error}")))
}

struct TempFile {
    path: PathBuf,
    keep: bool,
}

/// Deletes `<data_dir>/restore-pending` on drop while armed: the restore
/// stopped before the database was installed (section 8.2).
struct PendingMarker {
    path: PathBuf,
    armed: bool,
}

impl Drop for PendingMarker {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.keep {
            remove_temp_files(&self.path);
        }
    }
}

fn remove_temp_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    remove_sidecars(path);
}

fn remove_sidecars(path: &Path) {
    for suffix in ["-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

fn create_private_dir(dir: &Path) -> Result<(), Error> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .map_err(|error| failed(format!("cannot create {}: {error}", dir.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(|error| {
            failed(format!("cannot set the mode of {}: {error}", dir.display()))
        })?;
    }
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| failed(format!("cannot write {}: {error}", path.display())))?;
    file.write_all(bytes)
        .map_err(|error| failed(format!("cannot write {}: {error}", path.display())))?;
    file.sync_all()
        .map_err(|error| failed(format!("cannot sync {}: {error}", path.display())))
}

fn set_private_mode(path: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(
            |error| {
                failed(format!(
                    "cannot set the mode of {}: {error}",
                    path.display()
                ))
            },
        )?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)
            .and_then(|file| file.sync_all())
            .map_err(|error| failed(format!("cannot sync {}: {error}", dir.display())))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

struct Command {
    endpoint: String,
    region: String,
    bucket: String,
    prefix: String,
    cluster_id: String,
    credentials_file: PathBuf,
    recovery_key_file: PathBuf,
    signing_key_file: PathBuf,
    data_key_file: PathBuf,
    data_dir: PathBuf,
    backup: Option<String>,
    node_id: Option<String>,
    accept_loss: Option<String>,
    dry_run: bool,
}

/// The value flags of `quoro restore`; `--dry-run` and `--accept-loss` carry
/// no value.
const VALUE_FLAGS: &[&str] = &[
    "endpoint",
    "region",
    "bucket",
    "prefix",
    "cluster-id",
    "credentials-file",
    "recovery-key-file",
    "signing-key-file",
    "data-key-file",
    "data-dir",
    "backup",
    "node-id",
    "statement",
];

fn parse_args(args: &[String]) -> Result<Command, Error> {
    let mut flags: Vec<(String, String)> = Vec::new();
    let mut dry_run = false;
    let mut accept_loss = false;
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let Some(name) = argument.strip_prefix("--") else {
            return Err(Error::Usage(format!("unexpected argument '{argument}'")));
        };
        if name == "dry-run" {
            dry_run = true;
            continue;
        }
        if name == "accept-loss" {
            accept_loss = true;
            continue;
        }
        if !VALUE_FLAGS.contains(&name) {
            return Err(Error::Usage(format!("unknown option '{argument}'")));
        }
        let Some(value) = args.next() else {
            return Err(Error::Usage(format!("'{argument}' needs a value")));
        };
        flags.push((name.to_string(), value.clone()));
    }
    let value = |name: &str| {
        flags
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    let required =
        |name: &str| value(name).ok_or_else(|| Error::Usage(format!("restore needs --{name}")));
    let node_id = value("node-id");
    if let Some(node_id) = &node_id
        && !quoro_types::is_id(quoro_types::Prefix::Node, node_id)
    {
        return Err(Error::Usage(format!(
            "--node-id '{node_id}' is not nod_ followed by 26 Crockford base32 characters"
        )));
    }
    let accept_loss = match (accept_loss, value("statement")) {
        (true, None) => {
            return Err(Error::Usage(
                "restore --accept-loss needs --statement \"<text>\"".to_string(),
            ));
        }
        (false, Some(_)) => {
            return Err(Error::Usage(
                "restore --statement needs --accept-loss".to_string(),
            ));
        }
        (true, Some(statement)) => {
            check_statement(&statement)?;
            Some(statement)
        }
        (false, None) => None,
    };
    Ok(Command {
        endpoint: required("endpoint")?,
        region: required("region")?,
        bucket: required("bucket")?,
        prefix: required("prefix")?,
        cluster_id: required("cluster-id")?,
        credentials_file: PathBuf::from(required("credentials-file")?),
        recovery_key_file: PathBuf::from(required("recovery-key-file")?),
        signing_key_file: PathBuf::from(required("signing-key-file")?),
        data_key_file: PathBuf::from(required("data-key-file")?),
        data_dir: PathBuf::from(required("data-dir")?),
        backup: value("backup"),
        node_id,
        accept_loss,
        dry_run,
    })
}

/// The statement of `--accept-loss` (section 8.2, "Override"): not blank,
/// and not a value that starts with `--`, which is a flag the shell passed
/// in its place, such as `--statement --dry-run`.
///
/// # Errors
///
/// [`Error::Usage`] for such a statement.
fn check_statement(statement: &str) -> Result<(), Error> {
    if statement.trim().is_empty() {
        return Err(Error::Usage(
            "restore --statement needs a non-empty statement".to_string(),
        ));
    }
    if statement.starts_with("--") {
        return Err(Error::Usage(format!(
            "restore --statement '{statement}' starts with --; give the statement itself, quoted"
        )));
    }
    Ok(())
}
