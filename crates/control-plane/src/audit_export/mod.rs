// SPDX-License-Identifier: AGPL-3.0-only
//! The off-site copy of the audit log (`audit-log-format.md` section 8,
//! ADR 0149).
//!
//! A configured export writes canonical JSON Lines segments and their head
//! objects to an `object_store` provider's bucket with `If-None-Match: *`, so
//! Quoro never creates a second version of an object it already wrote
//! (section 8.1). [`probe`] observes how strongly the bucket refuses a
//! rewrite, [`exporter`] copies new records in segments of 1,000 records or
//! five minutes, and [`verify`] compares the local chain with the copy
//! (section 8.3). Every function is generic over
//! [`quoro_providers::object_store::ObjectStore`], so the tests drive them
//! with `MemoryStore` and production drives them with the S3 client built
//! from the provider row.
//!
//! The credential is the provider entry's, decrypted with the cluster
//! data-encryption key; it may put, get and list and is never the backup
//! repository's entry (`backup-format.md` section 10).

pub mod exporter;
pub mod probe;
pub mod verify;

use crate::AppState;
use crate::api::audit_exports::{AuditExport, CreateAuditExportRequest};
use crate::api::providers::provider_secret;
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::request::CallCtx;
use crate::store::Store;
use quoro_providers::object_store::{
    LockStrength as StoreLockStrength, ObjectStore, ObjectStoreError,
};
use quoro_providers::s3::{S3Client, S3Config};
use quoro_types::Scope;
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use std::time::Duration;
use zeroize::Zeroizing;

/// Records in one segment (section 8.1).
pub const SEGMENT_MAX_RECORDS: i64 = 1_000;

/// How long the oldest unexported record may wait before its segment closes
/// (section 8.1).
pub const SEGMENT_MAX_AGE: Duration = Duration::from_secs(5 * 60);

/// How often the exporter wakes (section 8).
pub const EXPORT_INTERVAL: Duration = Duration::from_secs(30);

/// How often the probe and the copy comparison run by themselves
/// (section 8.2 and 8.3).
pub const DAILY_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a `compliance` write retains its object. The contract fixes no
/// period; one year is long enough for an auditor to collect the copy and
/// short enough that the bucket does not grow forever.
pub const COMPLIANCE_RETENTION_DAYS: i64 = 365;

/// The largest object the copy reads back. The S3 client refuses a larger
/// `GET` body itself; the in-memory store is held to the same limit.
pub const MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;

/// Every column of `audit_exports` after migration 0006.
pub const COLUMNS: &str = "id, target, provider_id, lock_strength, probed_at, probe_detail, \
     last_exported_seq, last_exported_hash, last_error, updated_at";

/// One `audit_exports` row (`openapi.yaml` `AuditExport`).
#[derive(Clone, Debug)]
pub struct ExportRow {
    pub id: String,
    /// The key prefix of the copy inside the bucket.
    pub target: String,
    pub provider_id: Option<String>,
    pub lock_strength: StoreLockStrength,
    pub probed_at: Option<String>,
    pub probe_detail: Option<String>,
    pub last_exported_seq: i64,
    pub last_exported_hash: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: String,
}

/// Parses `audit_exports.lock_strength`; an unknown value is an internal
/// inconsistency.
fn lock_strength(value: &str) -> Result<StoreLockStrength, ApiError> {
    match value {
        "none" => Ok(StoreLockStrength::None),
        "bucket_rule" => Ok(StoreLockStrength::BucketRule),
        "compliance" => Ok(StoreLockStrength::Compliance),
        other => Err(ApiError::internal(&format!(
            "unknown audit export lock strength {other:?}"
        ))),
    }
}

/// Builds an [`ExportRow`] from a row selected with [`COLUMNS`].
///
/// # Errors
///
/// A missing or mistyped column, or an unknown lock strength.
pub fn export_from_row(row: &SqliteRow) -> Result<ExportRow, ApiError> {
    let strength: String = row.try_get("lock_strength")?;
    Ok(ExportRow {
        id: row.try_get("id")?,
        target: row.try_get("target")?,
        provider_id: row.try_get("provider_id")?,
        lock_strength: lock_strength(&strength)?,
        probed_at: row.try_get("probed_at")?,
        probe_detail: row.try_get("probe_detail")?,
        last_exported_seq: row.try_get("last_exported_seq")?,
        last_exported_hash: row.try_get("last_exported_hash")?,
        last_error: row.try_get("last_error")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The cluster's export, of which M5a has at most one (section 8).
///
/// # Errors
///
/// A database error or a malformed stored row.
pub async fn load(store: &Store) -> Result<Option<ExportRow>, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM audit_exports ORDER BY id LIMIT 1"
    ))
    .fetch_optional(store.pool())
    .await?;
    row.as_ref().map(export_from_row).transpose()
}

/// One export by id.
///
/// # Errors
///
/// A database error or a malformed stored row.
pub async fn load_by_id(store: &Store, id: &str) -> Result<Option<ExportRow>, ApiError> {
    let row = sqlx::query(&format!("SELECT {COLUMNS} FROM audit_exports WHERE id = ?"))
        .bind(id)
        .fetch_optional(store.pool())
        .await?;
    row.as_ref().map(export_from_row).transpose()
}

/// `<prefix>/<cluster_id>/audit/`: the root of every object of the copy
/// (section 8.1).
#[must_use]
pub fn key_root(state: &AppState, row: &ExportRow) -> String {
    format!(
        "{}/{}/audit/",
        row.target.trim_matches('/'),
        state.cluster_id
    )
}

/// The segment key of a `first`-`last` range (section 8.1).
#[must_use]
pub fn segment_key(root: &str, first: i64, last: i64) -> String {
    format!("{root}{first:020}-{last:020}.jsonl")
}

/// The head key that accompanies the segment ending at `last` (section 8.1).
#[must_use]
pub fn head_key(root: &str, last: i64) -> String {
    format!("{root}head-{last:020}.json")
}

/// Parses a segment key back into its range.
#[must_use]
pub fn parse_segment_key(root: &str, key: &str) -> Option<(i64, i64)> {
    let name = key.strip_prefix(root)?;
    let name = name.strip_suffix(".jsonl")?;
    let (first, last) = name.split_once('-')?;
    if first.len() != 20 || last.len() != 20 {
        return None;
    }
    Some((first.parse().ok()?, last.parse().ok()?))
}

/// Parses a head key back into its `last_seq`.
#[must_use]
pub fn parse_head_key(root: &str, key: &str) -> Option<i64> {
    let name = key.strip_prefix(root)?;
    let name = name.strip_prefix("head-")?;
    let name = name.strip_suffix(".json")?;
    if name.len() != 20 {
        return None;
    }
    name.parse().ok()
}

/// The fixed-width RFC 3339 form the store writes, so stored timestamps
/// compare correctly as strings.
#[must_use]
pub fn format_rfc3339(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

/// The record the copy retains a `compliance` object until (section 8.1).
#[must_use]
pub fn compliance_lock(now: time::OffsetDateTime) -> quoro_providers::object_store::ComplianceLock {
    quoro_providers::object_store::ComplianceLock {
        retain_until: format_rfc3339(now + time::Duration::days(COMPLIANCE_RETENTION_DAYS)),
    }
}

/// Reads one object of the copy. On a `compliance` bucket the first version
/// is read, so a newer version or a delete marker cannot hide the original;
/// on `bucket_rule` and `none` the current object is read (section 8.3).
///
/// # Errors
///
/// The store's error, or a protocol error when the object is larger than
/// [`MAX_OBJECT_BYTES`].
pub async fn read_object<S: ObjectStore>(
    store: &S,
    key: &str,
    strength: StoreLockStrength,
) -> Result<Vec<u8>, ObjectStoreError> {
    let bytes = if strength == StoreLockStrength::Compliance {
        read_first_version(store, key).await?
    } else {
        store.get(key).await?
    };
    if bytes.len() > MAX_OBJECT_BYTES {
        return Err(ObjectStoreError::Protocol(
            "the object is larger than the download limit".to_string(),
        ));
    }
    Ok(bytes)
}

/// Reads the oldest non-delete version of `key`, by `last_modified` and then
/// by version id.
async fn read_first_version<S: ObjectStore>(
    store: &S,
    key: &str,
) -> Result<Vec<u8>, ObjectStoreError> {
    let mut versions: Vec<_> = store
        .list_versions(key)
        .await?
        .into_iter()
        .filter(|version| version.key == key && !version.is_delete_marker)
        .collect();
    versions.sort_by(|a, b| {
        a.last_modified
            .cmp(&b.last_modified)
            .then_with(|| a.version_id.cmp(&b.version_id))
    });
    let Some(first) = versions.first() else {
        return Err(ObjectStoreError::NotFound);
    };
    store.get_version(key, &first.version_id).await
}

/// Builds the S3 client of an export's provider entry from its stored config
/// (`backup-format.md` section 10).
///
/// # Errors
///
/// `not_found` for an unknown provider, `validation_failed` when the entry is
/// not a usable `object_store`, or an internal error when the secret cannot be
/// decrypted.
pub async fn store_for_provider(state: &AppState, provider_id: &str) -> Result<S3Client, ApiError> {
    let (provider, secret) = provider_secret(state, provider_id).await?;
    if provider.kind != "object_store" {
        return Err(ApiError::validation(format!(
            "provider {provider_id} is not object_store"
        )));
    }
    let config = &provider.config;
    let field = |name: &str| {
        config
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| ApiError::validation(format!("provider {provider_id} has no {name}")))
    };
    let endpoint = field("endpoint")?;
    let bucket = field("bucket")?;
    let access_key_id = field("access_key_id")?;
    let region = config
        .get("region")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_string();
    S3Client::new(S3Config {
        endpoint,
        region,
        bucket,
        access_key_id,
        secret_access_key: Zeroizing::new(secret.expose().to_string()),
    })
    .map_err(|error| {
        ApiError::validation(format!(
            "provider {provider_id} is not a usable object store: {error}"
        ))
    })
}

/// [`store_for_provider`] for an export row.
///
/// # Errors
///
/// As [`store_for_provider`], or an internal error for an export without a
/// provider.
pub async fn store_for_export(state: &AppState, row: &ExportRow) -> Result<S3Client, ApiError> {
    let provider_id = row
        .provider_id
        .as_deref()
        .ok_or_else(|| ApiError::internal(&"audit export has no provider"))?;
    store_for_provider(state, provider_id).await
}

/// Truncates a stored error to the 2,000 characters the contract allows.
#[must_use]
pub fn short_error(message: &str) -> String {
    message.chars().take(2_000).collect()
}

/// Records `last_error` on the export, unless it is already the same message.
///
/// # Errors
///
/// A database error.
pub async fn set_last_error(
    state: &AppState,
    export_id: &str,
    message: &str,
) -> Result<(), ApiError> {
    let message = short_error(message);
    let unchanged: Option<(Option<String>,)> =
        sqlx::query_as("SELECT last_error FROM audit_exports WHERE id = ? AND last_error = ?")
            .bind(export_id)
            .bind(&message)
            .fetch_optional(state.store.pool())
            .await?;
    if unchanged.is_some() {
        return Ok(());
    }
    let mut w = state.store.begin_write().await?;
    set_last_error_in(&mut w.tx, export_id, &message).await?;
    w.commit().await?;
    Ok(())
}

/// [`set_last_error`] in the caller's transaction, with its change-log entry.
///
/// # Errors
///
/// A database error.
pub(crate) async fn set_last_error_in(
    conn: &mut sqlx::SqliteConnection,
    export_id: &str,
    message: &str,
) -> Result<(), ApiError> {
    let now = crate::store::now_rfc3339();
    sqlx::query("UPDATE audit_exports SET last_error = ?, updated_at = ? WHERE id = ?")
        .bind(message)
        .bind(&now)
        .bind(export_id)
        .execute(&mut *conn)
        .await?;
    crate::changelog::append(
        conn,
        &crate::changelog::Actor::system("audit_export"),
        "audit_export",
        export_id,
        crate::changelog::Op::Update,
        serde_json::json!({ "id": export_id, "last_error": message, "updated_at": now }),
    )
    .await?;
    Ok(())
}

/// Starts the exporter task (see [`exporter::spawn`]).
pub fn spawn(state: AppState) {
    exporter::spawn(state);
}

/// The operation string of `createAuditExport`.
pub const CREATE_OPERATION: &str = "POST /api/v1/audit-exports";

/// Whether `prefix` matches `CreateAuditExportRequest.prefix`: one or more
/// slash-separated segments of `A-Za-z0-9._-`, at most 200 characters.
fn valid_prefix(prefix: &str) -> bool {
    let len = prefix.chars().count();
    (1..=200).contains(&len)
        && prefix.split('/').all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        })
}

/// Validates the provider of a new export and returns its id: an
/// `object_store` entry that is not the backup repository's
/// (`openapi.yaml` `createAuditExport`, `backup-format.md` section 10).
async fn check_provider(
    conn: &mut sqlx::SqliteConnection,
    provider_id: &str,
) -> Result<(), ApiError> {
    let kind: Option<(String,)> = sqlx::query_as("SELECT kind FROM providers WHERE id = ?")
        .bind(provider_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some((kind,)) = kind else {
        return Err(ApiError::validation(format!(
            "provider {provider_id} does not exist"
        )));
    };
    if kind != "object_store" {
        return Err(ApiError::validation(format!(
            "provider {provider_id} is not object_store"
        )));
    }
    let (backup,): (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM backup_repositories WHERE provider_id = ?)")
            .bind(provider_id)
            .fetch_one(&mut *conn)
            .await?;
    if backup {
        return Err(ApiError::validation(format!(
            "provider {provider_id} is the backup repository's"
        )));
    }
    Ok(())
}

/// Configures the off-site copy (`POST /api/v1/audit-exports`): stores the
/// row, then probes the bucket and stores what it observed (sections 8.1 and
/// 8.2). One export per cluster, human only.
///
/// # Errors
///
/// `conflict` when an export already exists, `validation_failed` for a bad
/// prefix or provider, or a database error.
pub async fn create(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateAuditExportRequest,
) -> Result<AuditExport, ApiError> {
    let store = store_for_provider(state, &body.provider_id).await?;
    create_with(state, ctx, body, &store, time::OffsetDateTime::now_utc()).await
}

/// [`create`] against an injected store, for the tests.
///
/// # Errors
///
/// As [`create`].
pub async fn create_with<S: ObjectStore>(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateAuditExportRequest,
    store: &S,
    now: time::OffsetDateTime,
) -> Result<AuditExport, ApiError> {
    if !valid_prefix(&body.prefix) {
        return Err(ApiError::validation(
            "prefix must be slash-separated segments of A-Za-z0-9._-",
        ));
    }
    let params = json!({
        "provider_id": body.provider_id,
        "prefix": body.prefix,
    });
    let mut w = state.store.begin_write().await?;
    let (exists,): (bool,) = sqlx::query_as("SELECT EXISTS (SELECT 1 FROM audit_exports)")
        .fetch_one(&mut *w.tx)
        .await?;
    if exists {
        return Err(ApiError::conflict(
            "the cluster already has an audit export",
        ));
    }
    check_provider(&mut w.tx, &body.provider_id).await?;
    let id = crate::deploy::new_id_with_prefix("aex");
    let at = format_rfc3339(now);
    let row = ExportRow {
        id: id.clone(),
        target: body.prefix.clone(),
        provider_id: Some(body.provider_id.clone()),
        lock_strength: StoreLockStrength::None,
        probed_at: None,
        probe_detail: None,
        last_exported_seq: 0,
        last_exported_hash: None,
        last_error: None,
        updated_at: at.clone(),
    };
    sqlx::query(
        "INSERT INTO audit_exports (id, target, provider_id, lock_strength, probed_at, \
         probe_detail, last_exported_seq, last_exported_hash, last_error, updated_at) \
         VALUES (?, ?, ?, 'none', NULL, NULL, 0, NULL, NULL, ?)",
    )
    .bind(&id)
    .bind(&row.target)
    .bind(&row.provider_id)
    .bind(&at)
    .execute(&mut *w.tx)
    .await?;
    let change_seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "audit_export",
        &id,
        Op::Create,
        json!({
            "id": id,
            "provider_id": row.provider_id,
            "prefix": row.target,
            "lock_strength": "none",
            "last_exported_seq": 0,
            "updated_at": at,
        }),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ProviderManage),
        Some(&Target {
            kind: "audit_export".to_string(),
            id: id.clone(),
        }),
        None,
        &params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;

    match probe::run_with(state, &row, store, now).await {
        Ok(export) => Ok(export),
        Err(error) => {
            // The row is committed; report it with the strength it has rather
            // than fail a create that already happened.
            tracing::warn!(error = ?error, "audit export probe failed after create");
            Ok(AuditExport::from_row(&row))
        }
    }
}
