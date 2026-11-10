// SPDX-License-Identifier: AGPL-3.0-only
//! The backup repository: its row, the repository key and the config object
//! (`backup-format.md` sections 3 and 4, `openapi.yaml`
//! `createBackupRepository`).
//!
//! One repository exists per cluster in M5a. Creating it generates the
//! repository key and the owner's recovery key, writes `config-1.json` with
//! the repository key wrapped under the recovery key, and stores the same
//! repository key under the cluster data-encryption key. The recovery key is
//! answered once and never stored; rotating it writes `config-<n+1>.json`.

use crate::AppState;
use crate::api::apps::BackupSchedule;
use crate::api::backups::{CreateBackupRepositoryRequest, UpdateBackupRepositoryRequest};
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::request::CallCtx;
use crate::store::now_rfc3339;
use quoro_backup::{FORMAT, RecoveryKey, RepoConfig, RepoKey, layout, wrap_repo_key};
use quoro_providers::object_store::ObjectStoreError;
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use super::{BucketFactory, RepositoryTarget, decrypt_repo_key, encrypt_repo_key, object_key};

/// Columns of [`Repository`], in the order [`repository_from_row`] expects
/// them.
pub(crate) const REPOSITORY_COLUMNS: &str = "id, name, provider_id, prefix, repo_key_enc, \
     recovery_key_fingerprint, recovery_key_rotated_at, control_plane_schedule, created_by, \
     created_at";

/// One `backup_repositories` row, with its encrypted repository key.
pub(crate) struct Repository {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub prefix: String,
    pub repo_key_enc: Vec<u8>,
    pub recovery_key_fingerprint: String,
    pub recovery_key_rotated_at: Option<String>,
    pub control_plane_schedule: BackupSchedule,
    pub created_by: String,
    pub created_at: String,
}

impl Repository {
    /// The change-log payload of the row, without `repo_key_enc`. Every
    /// machine with the `node` role can open its own seal of the cluster
    /// data-encryption key (`node-agent-protocol.md` section 7.2), so the
    /// encrypted repository key in the log would put the repository key on
    /// every follower's disk, and the contract says a node never gets it. The
    /// key stays out of the log until the maintainer decides how a promoted
    /// primary gets it back.
    #[must_use]
    pub(crate) fn payload(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "provider_id": self.provider_id,
            "prefix": self.prefix,
            "recovery_key_fingerprint": self.recovery_key_fingerprint,
            "recovery_key_rotated_at": self.recovery_key_rotated_at,
            "control_plane_schedule": self.control_plane_schedule,
            "created_by": self.created_by,
            "created_at": self.created_at,
        })
    }
}

/// Builds a [`Repository`] from a row selected with [`REPOSITORY_COLUMNS`].
///
/// # Errors
///
/// An internal error for a stored schedule that is not JSON, or a database
/// error.
pub(crate) fn repository_from_row(row: &SqliteRow) -> Result<Repository, ApiError> {
    let schedule: String = row.try_get("control_plane_schedule")?;
    let control_plane_schedule = parse_schedule(&schedule)?;
    Ok(Repository {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        provider_id: row.try_get("provider_id")?,
        prefix: row.try_get("prefix")?,
        repo_key_enc: row.try_get("repo_key_enc")?,
        recovery_key_fingerprint: row.try_get("recovery_key_fingerprint")?,
        recovery_key_rotated_at: row.try_get("recovery_key_rotated_at")?,
        control_plane_schedule,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Parses a stored `BackupSchedule`.
///
/// # Errors
///
/// An internal error for JSON that is not a schedule.
pub(crate) fn parse_schedule(text: &str) -> Result<BackupSchedule, ApiError> {
    serde_json::from_str(text)
        .map_err(|error| ApiError::internal(&format!("stored backup schedule: {error}")))
}

/// The cluster's repository, if it has one (at most one in M5a).
///
/// # Errors
///
/// A database error.
pub(crate) async fn any(store: &crate::store::Store) -> Result<Option<Repository>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {REPOSITORY_COLUMNS} FROM backup_repositories ORDER BY id LIMIT 1"
    ))
    .fetch_optional(store.pool())
    .await?;
    row.as_ref().map(repository_from_row).transpose()
}

/// The cluster's repository as the pre-upgrade backup may read it, before the
/// pending migrations ran (`backup-format.md` section 7.2, "Before an upgrade").
///
/// A database too old to have the table, or to have every column this reads,
/// has no repository the pre-upgrade backup could write to.
///
/// # Errors
///
/// A database error.
pub(crate) async fn any_before_upgrade(
    store: &crate::store::Store,
) -> Result<Option<Repository>, ApiError> {
    let table: Option<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'backup_repositories'",
    )
    .fetch_optional(store.pool())
    .await?;
    if table.is_none() {
        return Ok(None);
    }
    let columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('backup_repositories') WHERE name IN \
         ('id', 'name', 'provider_id', 'prefix', 'repo_key_enc', 'recovery_key_fingerprint', \
          'recovery_key_rotated_at', 'control_plane_schedule', 'created_by', 'created_at')",
    )
    .fetch_one(store.pool())
    .await?;
    if columns < 10 {
        return Ok(None);
    }
    any(store).await
}

/// One repository by id.
///
/// # Errors
///
/// A database error.
pub(crate) async fn by_id(
    store: &crate::store::Store,
    repository_id: &str,
) -> Result<Option<Repository>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {REPOSITORY_COLUMNS} FROM backup_repositories WHERE id = ?"
    ))
    .bind(repository_id)
    .fetch_optional(store.pool())
    .await?;
    row.as_ref().map(repository_from_row).transpose()
}

/// The cluster id from `meta`.
///
/// # Errors
///
/// An internal error when `meta` has no cluster id.
pub(crate) async fn cluster_id(store: &crate::store::Store) -> Result<String, ApiError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = 'cluster_id'")
        .fetch_optional(store.pool())
        .await?;
    row.map(|row| row.0)
        .ok_or_else(|| ApiError::internal(&"the cluster has no id"))
}

/// Where a repository lives and how to reach it: its root, the provider's
/// bucket and the decrypted provider secret.
///
/// # Errors
///
/// `not_found` for an unknown provider, a database error, or an internal error
/// for a stored config or secret that cannot be read.
pub(crate) async fn target(
    store: &crate::store::Store,
    data_key: &crate::crypto::DataKey,
    repository: &Repository,
) -> Result<RepositoryTarget, ApiError> {
    target_of_provider(
        store,
        data_key,
        &repository.id,
        &repository.provider_id,
        &repository.prefix,
    )
    .await
}

/// [`target`] from the parts, for `createBackupRepository` before the row
/// exists.
///
/// # Errors
///
/// As [`target`].
pub(crate) async fn target_of_provider(
    store: &crate::store::Store,
    data_key: &crate::crypto::DataKey,
    repository_id: &str,
    provider_id: &str,
    prefix: &str,
) -> Result<RepositoryTarget, ApiError> {
    let row = sqlx::query("SELECT config_json, secret_enc FROM providers WHERE id = ?")
        .bind(provider_id)
        .fetch_optional(store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("provider"))?;
    let config_json: String = row.try_get("config_json")?;
    let config: Value = serde_json::from_str(&config_json)
        .map_err(|error| ApiError::internal(&format!("stored provider config: {error}")))?;
    let secret_enc: Vec<u8> = row.try_get("secret_enc")?;
    let secret_access_key = data_key
        .decrypt(super::PROVIDER_SECRET_CONTEXT, provider_id, &secret_enc)
        .map_err(|error| ApiError::internal(&error))?;
    let field = |name: &str| {
        config
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ApiError::validation(format!("the provider config has no {name}")))
    };
    let cluster_id = cluster_id(store).await?;
    Ok(RepositoryTarget {
        repository_id: repository_id.to_string(),
        root: layout::root(prefix, &cluster_id),
        endpoint: field("endpoint")?,
        region: config
            .get("region")
            .and_then(Value::as_str)
            .unwrap_or("auto")
            .to_string(),
        bucket: field("bucket")?,
        access_key_id: field("access_key_id")?,
        secret_access_key,
    })
}

/// Checks the static rules of a `CreateBackupRepositoryRequest`
/// (`openapi.yaml` `CreateBackupRepositoryRequest`).
///
/// # Errors
///
/// `validation_failed` for a bad name, prefix or schedule.
pub(crate) fn validate_create(body: &CreateBackupRepositoryRequest) -> Result<(), ApiError> {
    validate_name(&body.name)?;
    let prefix = body.prefix.as_deref().unwrap_or(DEFAULT_PREFIX);
    validate_prefix(prefix)?;
    if let Some(schedule) = &body.control_plane_schedule {
        validate_schedule(schedule)?;
    }
    Ok(())
}

/// The contract's default repository prefix.
pub(crate) const DEFAULT_PREFIX: &str = "quoro-backups";

fn validate_name(name: &str) -> Result<(), ApiError> {
    let len = name.chars().count();
    if !(1..=200).contains(&len) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    Ok(())
}

/// `^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$`, 1 to 200 characters.
fn validate_prefix(prefix: &str) -> Result<(), ApiError> {
    if !(1..=200).contains(&prefix.len()) {
        return Err(ApiError::validation(
            "prefix must be between 1 and 200 characters",
        ));
    }
    if prefix.split('/').any(|segment| {
        segment.is_empty()
            || !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    }) {
        return Err(ApiError::validation(
            "prefix may only hold letters, digits, `.`, `_`, `-` and `/`",
        ));
    }
    Ok(())
}

pub(crate) fn validate_schedule(schedule: &BackupSchedule) -> Result<(), ApiError> {
    if !(1..=168).contains(&schedule.interval_h) {
        return Err(ApiError::validation(
            "control_plane_schedule.interval_h is outside 1-168",
        ));
    }
    if !(1..=100).contains(&schedule.keep) {
        return Err(ApiError::validation(
            "control_plane_schedule.keep is outside 1-100",
        ));
    }
    Ok(())
}

/// Creates the cluster's repository: the config object in the bucket, then the
/// row, its change log and the audit record.
///
/// Returns the repository and the recovery key's text form, the only time it
/// exists outside the owner's hands.
///
/// # Errors
///
/// `conflict` when a repository exists or the prefix already holds a config
/// object, `validation_failed` for a provider that is not `object_store`, is
/// the audit export's, or whose bucket refuses the probe write, or a database
/// error.
pub(crate) async fn create(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateBackupRepositoryRequest,
    params: &Value,
) -> Result<(Repository, String), ApiError> {
    validate_create(body)?;
    let prefix = body
        .prefix
        .clone()
        .unwrap_or_else(|| DEFAULT_PREFIX.to_string());
    let schedule = body.control_plane_schedule.unwrap_or(BackupSchedule {
        interval_h: 6,
        keep: 7,
    });

    if any(&state.store).await?.is_some() {
        return Err(ApiError::conflict(
            "the cluster already has a backup repository",
        ));
    }
    // The provider must be object storage and must not be the audit copy's
    // entry: backup credentials delete, the audit credential does not
    // (`backup-format.md` section 10).
    let provider = sqlx::query("SELECT kind FROM providers WHERE id = ?")
        .bind(&body.provider_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::validation("the provider does not exist"))?;
    let kind: String = provider.try_get("kind")?;
    if kind != "object_store" {
        return Err(ApiError::validation(
            "the provider is not an object_store provider",
        ));
    }
    let exported: Option<(String,)> =
        sqlx::query_as("SELECT id FROM audit_exports WHERE provider_id = ?")
            .bind(&body.provider_id)
            .fetch_optional(state.store.pool())
            .await?;
    if exported.is_some() {
        return Err(ApiError::validation("the provider holds the audit export"));
    }

    let id = crate::deploy::new_id_with_prefix("brp");
    let target = target_of_provider(
        &state.store,
        &state.data_key,
        &id,
        &body.provider_id,
        &prefix,
    )
    .await?;
    let bucket = state.backups.open_bucket(&target)?;

    let repo_key = RepoKey::generate();
    let recovery = RecoveryKey::generate();
    let config = RepoConfig {
        format: FORMAT,
        repository_id: id.clone(),
        cluster_id: state.cluster_id.clone(),
        cluster_public_key: state.keys.public_key_base64(),
        recovery_key_fingerprint: recovery.fingerprint(),
        wrapped_repo_key: wrap_repo_key(&repo_key, &recovery, &id),
        created_at: now_rfc3339(),
    };
    let body_bytes = serde_json::to_vec(&config)
        .map_err(|error| ApiError::internal(&format!("repository config: {error}")))?;
    let config_key = object_key(&target.root, &layout::config(1));
    // A request that passed the guard before a demotion writes no object
    // after it (`node-agent-protocol.md` section 7.4).
    if state.store.is_demoted() {
        return Err(ApiError::new(
            crate::error::Code::NotPrimary,
            crate::store::DEMOTED_REFUSAL,
        ));
    }
    match bucket.put_new(&config_key, body_bytes).await {
        Ok(()) => {}
        Err(ObjectStoreError::AlreadyExists) => {
            return Err(ApiError::conflict(
                "the prefix already holds a config object",
            ));
        }
        Err(error) => {
            return Err(ApiError::validation(format!(
                "the probe write to the bucket failed: {}",
                short(&error)
            )));
        }
    }

    let repo_key_enc = encrypt_repo_key(&state.data_key, &id, &repo_key);
    let now = now_rfc3339();
    let created_by = ctx.identity.id().to_string();
    let mut w = state.store.begin_write().await?;
    // A concurrent create may have won while the probe ran.
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM backup_repositories LIMIT 1")
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(
            "the cluster already has a backup repository",
        ));
    }
    let schedule_json = serde_json::to_string(&schedule)
        .map_err(|error| ApiError::internal(&format!("backup schedule: {error}")))?;
    sqlx::query(
        "INSERT INTO backup_repositories (id, name, provider_id, prefix, repo_key_enc, \
         recovery_key_fingerprint, recovery_key_rotated_at, control_plane_schedule, created_by, \
         created_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&body.name)
    .bind(&body.provider_id)
    .bind(&prefix)
    .bind(&repo_key_enc)
    .bind(recovery.fingerprint())
    .bind(&schedule_json)
    .bind(&created_by)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let repository = Repository {
        id: id.clone(),
        name: body.name.clone(),
        provider_id: body.provider_id.clone(),
        prefix,
        repo_key_enc,
        recovery_key_fingerprint: recovery.fingerprint(),
        recovery_key_rotated_at: None,
        control_plane_schedule: schedule,
        created_by,
        created_at: now,
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "backup_repository",
        &id,
        Op::Create,
        repository.payload(),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(quoro_types::Scope::BackupManage),
        Some(&repository_target(&id)),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok((repository, recovery.to_text()))
}

/// Rotates the recovery key: `config-<n+1>.json` in the bucket, then the new
/// fingerprint.
///
/// # Errors
///
/// `not_found` for an unknown repository, `conflict` when the next config
/// object already exists, or a database error.
pub(crate) async fn rotate(
    state: &AppState,
    ctx: &CallCtx,
    repository_id: &str,
    params: &Value,
) -> Result<(Repository, String), ApiError> {
    if !crate::deploy::is_id_with_prefix("brp", repository_id) {
        return Err(ApiError::not_found("backup repository"));
    }
    let mut repository = by_id(&state.store, repository_id)
        .await?
        .ok_or_else(|| ApiError::not_found("backup repository"))?;
    let repo_key = decrypt_repo_key(&state.data_key, repository_id, &repository.repo_key_enc)?;
    let target = target(&state.store, &state.data_key, &repository).await?;
    let bucket = state.backups.open_bucket(&target)?;
    let next = highest_config(&bucket, &target.root)
        .await?
        .saturating_add(1);
    let recovery = RecoveryKey::generate();
    let fingerprint = recovery.fingerprint();
    let config = RepoConfig {
        format: FORMAT,
        repository_id: repository_id.to_string(),
        cluster_id: state.cluster_id.clone(),
        cluster_public_key: state.keys.public_key_base64(),
        recovery_key_fingerprint: fingerprint.clone(),
        wrapped_repo_key: wrap_repo_key(&repo_key, &recovery, repository_id),
        created_at: now_rfc3339(),
    };
    let body = serde_json::to_vec(&config)
        .map_err(|error| ApiError::internal(&format!("repository config: {error}")))?;
    let key = object_key(&target.root, &layout::config(next));
    if state.store.is_demoted() {
        return Err(ApiError::new(
            crate::error::Code::NotPrimary,
            crate::store::DEMOTED_REFUSAL,
        ));
    }
    match bucket.put_new(&key, body).await {
        Ok(()) => {}
        Err(ObjectStoreError::AlreadyExists) => {
            return Err(ApiError::conflict("the next config object already exists"));
        }
        Err(error) => {
            return Err(ApiError::validation(format!(
                "the config write to the bucket failed: {}",
                short(&error)
            )));
        }
    }

    let now = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "UPDATE backup_repositories SET recovery_key_fingerprint = ?, \
         recovery_key_rotated_at = ? WHERE id = ?",
    )
    .bind(&fingerprint)
    .bind(&now)
    .bind(repository_id)
    .execute(&mut *w.tx)
    .await?;
    repository.recovery_key_fingerprint = fingerprint;
    repository.recovery_key_rotated_at = Some(now);
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "backup_repository",
        repository_id,
        Op::Update,
        repository.payload(),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(quoro_types::Scope::BackupManage),
        Some(&repository_target(repository_id)),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok((repository, recovery.to_text()))
}

/// The highest existing `config-<n>.json` counter, or 0.
///
/// # Errors
///
/// `validation_failed` when the bucket cannot be listed.
async fn highest_config(bucket: &super::Bucket, root: &str) -> Result<u64, ApiError> {
    let prefix = object_key(root, "config-");
    let objects = bucket.list(&prefix).await.map_err(|error| {
        ApiError::validation(format!("cannot list the bucket: {}", short(&error)))
    })?;
    Ok(objects
        .iter()
        .filter_map(|object| super::relative_key(root, &object.key))
        .filter_map(layout::parse_config_n)
        .max()
        .unwrap_or(0))
}

/// Changes the name and the control-plane schedule, nothing else
/// (`openapi.yaml` `updateBackupRepository`).
///
/// # Errors
///
/// `not_found` for an unknown repository, `validation_failed` for a bad name or
/// schedule, or a database error.
pub(crate) async fn update(
    state: &AppState,
    ctx: &CallCtx,
    repository_id: &str,
    body: &UpdateBackupRepositoryRequest,
    params: &Value,
) -> Result<Repository, ApiError> {
    if !crate::deploy::is_id_with_prefix("brp", repository_id) {
        return Err(ApiError::not_found("backup repository"));
    }
    let mut repository = by_id(&state.store, repository_id)
        .await?
        .ok_or_else(|| ApiError::not_found("backup repository"))?;
    if let Some(name) = &body.name {
        validate_name(name)?;
        repository.name.clone_from(name);
    }
    if let Some(schedule) = &body.control_plane_schedule {
        validate_schedule(schedule)?;
        repository.control_plane_schedule = *schedule;
    }
    let schedule_json = serde_json::to_string(&repository.control_plane_schedule)
        .map_err(|error| ApiError::internal(&format!("backup schedule: {error}")))?;
    let mut w = state.store.begin_write().await?;
    sqlx::query("UPDATE backup_repositories SET name = ?, control_plane_schedule = ? WHERE id = ?")
        .bind(&repository.name)
        .bind(&schedule_json)
        .bind(repository_id)
        .execute(&mut *w.tx)
        .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "backup_repository",
        repository_id,
        Op::Update,
        repository.payload(),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(quoro_types::Scope::BackupManage),
        Some(&repository_target(repository_id)),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(repository)
}

/// The audit target of a backup repository operation.
#[must_use]
pub(crate) fn repository_target(repository_id: &str) -> Target {
    Target {
        kind: "backup_repository".to_string(),
        id: repository_id.to_string(),
    }
}

/// A one-line reason from an object-store error, for a `422`.
fn short(error: &ObjectStoreError) -> String {
    error.to_string().chars().take(200).collect()
}

impl super::Backups {
    /// The bucket `target` names, through the installed [`BucketFactory`].
    ///
    /// # Errors
    ///
    /// An internal error from the factory.
    pub fn open_bucket(&self, target: &RepositoryTarget) -> Result<super::Bucket, ApiError> {
        let factory: std::sync::Arc<dyn BucketFactory> = self.bucket_factory();
        factory.open(target)
    }
}
