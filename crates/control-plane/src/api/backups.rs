// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/backups`, `/api/v1/backups*` and
//! `/api/v1/backup-repositories*`: encrypted backups of volume and
//! control-plane data (ADR 0147, ADR 0148, `backup-format.md`).
//!
//! No key material is ever in a response except the recovery key of
//! `createBackupRepository` and `rotateBackupRecoveryKey`, which appears once
//! and is then forgotten. The repository key stays encrypted in
//! `backup_repositories.repo_key_enc`; backups are listed and read without it.
//! A control-plane backup is listed only to a caller whose selector is
//! `{all: true}`.

use crate::AppState;
use crate::api::apps::{BackupSchedule, SnapshotMethod, app_target, idempotency_key};
use crate::api::volumes::Volume;
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId, Resource};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use quoro_proto::wire::ControlMessage;
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// Columns of [`Backup`], in the order [`backup_from_row`] expects them.
pub(crate) const BACKUP_COLUMNS: &str = "id, repository_id, source_kind, app_id, volume_id, \
     node_id, status, \"trigger\", fencing_token, snapshot_method, pause_ms, logical_bytes, \
     uploaded_bytes, chunk_count, manifest_key, manifest_digest, error, created_by_type, \
     created_by_id, created_at, started_at, finished_at, pruned_at";

/// The audit operation of `createBackup`.
const CREATE_BACKUP: &str = "POST /api/v1/apps/{app_id}/backups";
/// The audit operation of `createBackupRepository`.
const CREATE_REPOSITORY: &str = "POST /api/v1/backup-repositories";
/// The audit operation of `updateBackupRepository`.
const UPDATE_REPOSITORY: &str = "PATCH /api/v1/backup-repositories/{repository_id}";
/// The audit operation of `rotateBackupRecoveryKey`.
const ROTATE_RECOVERY_KEY: &str = "POST /api/v1/backup-repositories/{repository_id}/recovery-key";
/// The audit operation of `restoreBackup`.
const RESTORE_BACKUP: &str = "POST /api/v1/backups/{backup_id}/restore";

/// What a backup was taken of (`Backup.source_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackupSourceKind {
    Volume,
    ControlPlane,
}

impl BackupSourceKind {
    /// The wire form stored in `backups.source_kind`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Volume => "volume",
            Self::ControlPlane => "control_plane",
        }
    }

    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "volume" => Some(Self::Volume),
            "control_plane" => Some(Self::ControlPlane),
            _ => None,
        }
    }
}

/// `backups.status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl BackupStatus {
    /// The wire form stored in `backups.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Whether the backup reached a terminal status.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

/// `backups.trigger`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackupTrigger {
    Schedule,
    Api,
    Upgrade,
}

impl BackupTrigger {
    /// The wire form stored in `backups.trigger`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Api => "api",
            Self::Upgrade => "upgrade",
        }
    }

    /// Parses the wire form.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "schedule" => Some(Self::Schedule),
            "api" => Some(Self::Api),
            "upgrade" => Some(Self::Upgrade),
            _ => None,
        }
    }
}

/// One backup (`openapi.yaml` `Backup`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[schema(
    title = "Backup",
    description = "One point-in-time copy of a volume, or of the control-plane database, in a backup repository (ADR 0147, ADR 0148)."
)]
pub struct Backup {
    /// Backup id.
    #[schema(pattern = "^bkp_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// The backup repository it is stored in.
    #[schema(pattern = "^brp_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub repository_id: String,
    /// What was backed up.
    #[schema(inline)]
    pub source_kind: BackupSourceKind,
    /// The app whose volume was backed up; null for `control_plane`.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: Option<String>,
    /// The backed-up volume; null for `control_plane`.
    #[schema(pattern = "^vol_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub volume_id: Option<String>,
    /// The node that took it, the volume's node or the primary.
    #[schema(required = false, pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Backup status.
    #[schema(inline)]
    pub status: BackupStatus,
    /// Why the backup was taken.
    #[schema(inline)]
    pub trigger: BackupTrigger,
    /// How the volume was snapshotted; null for `control_plane` and before
    /// `running`.
    #[schema(inline)]
    pub snapshot_method: Option<SnapshotMethod>,
    /// How long the app was paused for a `pause_copy` snapshot.
    #[schema(minimum = 0)]
    pub pause_ms: Option<i64>,
    /// Size of the snapshot that was chunked, on success.
    #[schema(minimum = 0)]
    pub logical_bytes: Option<i64>,
    /// Bytes of new chunk objects this backup uploaded, on success.
    #[schema(minimum = 0)]
    pub uploaded_bytes: Option<i64>,
    /// Chunks its manifest lists, on success.
    #[schema(minimum = 0)]
    pub chunk_count: Option<i64>,
    /// One-line reason, on `failed`.
    #[schema(max_length = 2000)]
    pub error: Option<String>,
    /// When the backup was requested or scheduled.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When it left `queued`.
    #[schema(format = "date-time")]
    pub started_at: Option<String>,
    /// When it reached a terminal status.
    #[schema(format = "date-time")]
    pub finished_at: Option<String>,
    /// When retention removed its seal and manifest.
    #[schema(format = "date-time")]
    pub pruned_at: Option<String>,
}

/// One page of backups, newest first (`openapi.yaml` `BackupList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "BackupList",
    description = "One page of backups, newest first."
)]
pub struct BackupList {
    pub items: Vec<Backup>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// One backup repository (`openapi.yaml` `BackupRepository`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
#[schema(
    title = "BackupRepository",
    description = "An encrypted, content-addressed store of backups in the bucket of an `object_store` provider (ADR 0147). No key is ever returned."
)]
pub struct BackupRepository {
    /// Backup repository id.
    #[schema(pattern = "^brp_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Label of the repository.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// The `object_store` provider whose bucket holds it.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// Key prefix of the repository inside the bucket.
    pub prefix: String,
    /// Identifies the current recovery key without revealing it.
    #[schema(pattern = "^[0-9a-f]{16}$")]
    pub recovery_key_fingerprint: String,
    /// When the recovery key was last replaced, or null if never.
    #[schema(format = "date-time")]
    pub recovery_key_rotated_at: Option<String>,
    /// Whether this primary holds the repository key (ADR 0322).
    pub repository_key_present: bool,
    /// Schedule of the control-plane database backups.
    pub control_plane_schedule: BackupSchedule,
    /// Owner who created the repository.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub created_by: String,
    /// When the repository was created.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// The repositories of the cluster (`openapi.yaml` `BackupRepositoryList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(
    title = "BackupRepositoryList",
    description = "The backup repositories of the cluster."
)]
pub struct BackupRepositoryList {
    pub items: Vec<BackupRepository>,
}

/// A repository together with its new recovery key, shown only here
/// (`openapi.yaml` `BackupRepositoryWithRecoveryKey`).
#[derive(Debug, Serialize)]
pub struct BackupRepositoryWithRecoveryKey {
    #[serde(flatten)]
    pub repository: BackupRepository,
    /// The recovery key (`backup-format.md` section 3); shown only in this
    /// response.
    pub recovery_key: String,
}

impl utoipa::PartialSchema for BackupRepositoryWithRecoveryKey {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::Ref;
        use utoipa::openapi::schema::{AllOfBuilder, ObjectBuilder, Type};
        let extra = ObjectBuilder::new()
            .schema_type(Type::Object)
            .property(
                "recovery_key",
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .pattern(Some("^qrk1(-[0-9A-HJKMNP-TV-Z]{4}){13}$")),
            )
            .required("recovery_key");
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .item(Ref::from_schema_name("BackupRepository"))
            .item(extra)
            .into()
    }
}

impl ToSchema for BackupRepositoryWithRecoveryKey {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("BackupRepositoryWithRecoveryKey")
    }
}

/// Create the cluster's backup repository (`openapi.yaml`
/// `CreateBackupRepositoryRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "CreateBackupRepositoryRequest",
    description = "Create the cluster's backup repository in a provider's bucket."
)]
pub struct CreateBackupRepositoryRequest {
    /// Label of the repository.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// An `object_store` provider whose `config` names the bucket.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// Key prefix inside the bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(
        value_type = String,
        min_length = 1,
        max_length = 200,
        pattern = "^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$",
        default = "quoro-backups",
        required = false
    )]
    pub prefix: Option<String>,
    /// Schedule of the control-plane database backups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = BackupSchedule, required = false)]
    pub control_plane_schedule: Option<BackupSchedule>,
}

/// Change a backup repository's settings (`openapi.yaml`
/// `UpdateBackupRepositoryRequest`).
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "UpdateBackupRepositoryRequest",
    description = "Change a backup repository's settings; absent fields stay unchanged."
)]
pub struct UpdateBackupRepositoryRequest {
    /// New label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String, min_length = 1, max_length = 200, required = false)]
    pub name: Option<String>,
    /// New control-plane backup schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = BackupSchedule, required = false)]
    pub control_plane_schedule: Option<BackupSchedule>,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageParams {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    cursor: Option<String>,
}

impl PageParams {
    fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// `limit`, `cursor`, `source_kind` and `app_id` of `listBackups`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListBackupsParams {
    /// Only backups of this source kind.
    #[param(rename = "source_kind", value_type = BackupSourceKind, inline, required = false)]
    source_kind: Option<String>,
    /// Only backups of this app's volumes.
    #[param(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    app_id: Option<String>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    cursor: Option<String>,
}

impl ListBackupsParams {
    fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// One `backups` row with every column, including the two the API does not
/// expose (`manifest_key`, `manifest_digest`, which only the seal and the
/// pruning code read).
#[derive(Clone, Debug)]
pub(crate) struct BackupRecord {
    pub id: String,
    pub repository_id: String,
    pub source_kind: BackupSourceKind,
    pub app_id: Option<String>,
    pub volume_id: Option<String>,
    pub node_id: String,
    pub status: BackupStatus,
    pub trigger: BackupTrigger,
    pub fencing_token: Option<i64>,
    pub snapshot_method: Option<SnapshotMethod>,
    pub pause_ms: Option<i64>,
    pub logical_bytes: Option<i64>,
    pub uploaded_bytes: Option<i64>,
    pub chunk_count: Option<i64>,
    pub manifest_key: Option<String>,
    pub manifest_digest: Option<String>,
    pub error: Option<String>,
    pub created_by_type: String,
    pub created_by_id: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub pruned_at: Option<String>,
}

impl BackupRecord {
    /// The contract shape, without the manifest key and digest.
    #[must_use]
    pub(crate) fn api(&self) -> Backup {
        Backup {
            id: self.id.clone(),
            repository_id: self.repository_id.clone(),
            source_kind: self.source_kind,
            app_id: self.app_id.clone(),
            volume_id: self.volume_id.clone(),
            node_id: self.node_id.clone(),
            status: self.status,
            trigger: self.trigger,
            snapshot_method: self.snapshot_method,
            pause_ms: self.pause_ms,
            logical_bytes: self.logical_bytes,
            uploaded_bytes: self.uploaded_bytes,
            chunk_count: self.chunk_count,
            error: self.error.clone(),
            created_at: self.created_at.clone(),
            started_at: self.started_at.clone(),
            finished_at: self.finished_at.clone(),
            pruned_at: self.pruned_at.clone(),
        }
    }
}

/// Builds a [`BackupRecord`] from a row selected with [`BACKUP_COLUMNS`].
///
/// # Errors
///
/// An internal error for a status, trigger or snapshot method outside the
/// contract, or a database error.
pub(crate) fn backup_from_row(row: &SqliteRow) -> Result<BackupRecord, ApiError> {
    let kind: String = row.try_get("source_kind")?;
    let source_kind = BackupSourceKind::parse(&kind)
        .ok_or_else(|| ApiError::internal(&format!("unknown backup source_kind {kind:?}")))?;
    let status: String = row.try_get("status")?;
    let status = BackupStatus::parse(&status)
        .ok_or_else(|| ApiError::internal(&format!("unknown backup status {status:?}")))?;
    let trigger: String = row.try_get("trigger")?;
    let trigger = BackupTrigger::parse(&trigger)
        .ok_or_else(|| ApiError::internal(&format!("unknown backup trigger {trigger:?}")))?;
    let method: Option<String> = row.try_get("snapshot_method")?;
    let snapshot_method = match method.as_deref() {
        None => None,
        Some("reflink") => Some(SnapshotMethod::Reflink),
        Some("pause_copy") => Some(SnapshotMethod::PauseCopy),
        Some(other) => {
            return Err(ApiError::internal(&format!(
                "unknown snapshot method {other:?}"
            )));
        }
    };
    Ok(BackupRecord {
        id: row.try_get("id")?,
        repository_id: row.try_get("repository_id")?,
        source_kind,
        app_id: row.try_get("app_id")?,
        volume_id: row.try_get("volume_id")?,
        node_id: row.try_get("node_id")?,
        status,
        trigger,
        fencing_token: row.try_get("fencing_token")?,
        snapshot_method,
        pause_ms: row.try_get("pause_ms")?,
        logical_bytes: row.try_get("logical_bytes")?,
        uploaded_bytes: row.try_get("uploaded_bytes")?,
        chunk_count: row.try_get("chunk_count")?,
        manifest_key: row.try_get("manifest_key")?,
        manifest_digest: row.try_get("manifest_digest")?,
        error: row.try_get("error")?,
        created_by_type: row.try_get("created_by_type")?,
        created_by_id: row.try_get("created_by_id")?,
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        pruned_at: row.try_get("pruned_at")?,
    })
}

/// The change-log payload of a backup row.
#[must_use]
pub(crate) fn backup_payload(backup: &BackupRecord) -> Value {
    json!({
        "id": backup.id,
        "repository_id": backup.repository_id,
        "source_kind": backup.source_kind.as_str(),
        "app_id": backup.app_id,
        "volume_id": backup.volume_id,
        "node_id": backup.node_id,
        "status": backup.status.as_str(),
        "trigger": backup.trigger.as_str(),
        "fencing_token": backup.fencing_token,
        "snapshot_method": backup.snapshot_method.map(snapshot_name),
        "pause_ms": backup.pause_ms,
        "logical_bytes": backup.logical_bytes,
        "uploaded_bytes": backup.uploaded_bytes,
        "chunk_count": backup.chunk_count,
        "manifest_key": backup.manifest_key,
        "manifest_digest": backup.manifest_digest,
        "error": backup.error,
        "created_by_type": backup.created_by_type,
        "created_by_id": backup.created_by_id,
        "created_at": backup.created_at,
        "started_at": backup.started_at,
        "finished_at": backup.finished_at,
        "pruned_at": backup.pruned_at,
    })
}

/// The audit target of a backup operation.
#[must_use]
pub(crate) fn backup_target(backup_id: &str) -> Target {
    Target {
        kind: "backup".to_string(),
        id: backup_id.to_string(),
    }
}

/// The stored form of a snapshot method.
fn snapshot_name(method: SnapshotMethod) -> &'static str {
    match method {
        SnapshotMethod::Reflink => "reflink",
        SnapshotMethod::PauseCopy => "pause_copy",
    }
}

/// Converts a repository row to the contract shape.
pub(crate) fn repository_to_api(repository: &crate::backups::repo::Repository) -> BackupRepository {
    BackupRepository {
        id: repository.id.clone(),
        name: repository.name.clone(),
        provider_id: repository.provider_id.clone(),
        prefix: repository.prefix.clone(),
        recovery_key_fingerprint: repository.recovery_key_fingerprint.clone(),
        recovery_key_rotated_at: repository.recovery_key_rotated_at.clone(),
        repository_key_present: repository.holds_key(),
        control_plane_schedule: repository.control_plane_schedule,
        created_by: repository.created_by.clone(),
        created_at: repository.created_at.clone(),
    }
}

/// The project of a backup's app, when it has one.
async fn backup_project(state: &AppState, app_id: Option<&str>) -> Option<String> {
    let app_id = app_id?;
    crate::api::volumes::app_project(state, app_id).await
}

/// Describes a backup row for an agent's selector filter.
fn backup_resource<'a>(backup: &'a Backup, project_id: Option<&'a str>) -> Resource<'a> {
    Resource {
        kind: "backup",
        id: &backup.id,
        app_id: backup.app_id.as_deref(),
        project_id,
        node_id: Some(&backup.node_id),
    }
}

/// The backups of one app's volumes, newest first, failed and pruned ones
/// included.
///
/// # Errors
///
/// `forbidden` without `backup:read`, `not_found` for an unknown or malformed
/// app id, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/backups",
    tag = "backups",
    operation_id = "listAppBackups",
    extensions(("x-quoro-scope" = json!("backup:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        PageParams,
    ),
    responses(
        (status = 200, description = "One page of backups.", body = BackupList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_app_backups(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(page): Query<PageParams>,
) -> Result<Json<BackupList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/backups",
        &headers,
    );
    let params = json!({ "app_id": app_id });
    let target = app_target(&app_id);
    let access = ctx
        .authorize_read(&state, Scope::BackupRead, Some(&target), &params)
        .await?;
    let result: Result<Json<BackupList>, ApiError> = async {
        if !crate::deploy::is_id_with_prefix("app", &app_id) {
            return Err(ApiError::not_found("app"));
        }
        let project_id = crate::api::volumes::app_project(&state, &app_id)
            .await
            .ok_or_else(|| ApiError::not_found("app"))?;
        let limit = page.limit()?;
        let rows = sqlx::query(&format!(
            "SELECT {BACKUP_COLUMNS} FROM backups WHERE app_id = ? AND (? = '' OR id < ?) \
             ORDER BY id DESC LIMIT ?"
        ))
        .bind(&app_id)
        .bind(page.after())
        .bind(page.after())
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
        let records = rows
            .iter()
            .map(backup_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let backups: Vec<Backup> = records.iter().map(BackupRecord::api).collect();
        let (items, next_cursor) = crate::api::page::finish(backups, limit, |b| b.id.clone());
        let entries: Vec<(Backup, String)> = items
            .into_iter()
            .map(|backup| (backup, project_id.clone()))
            .collect();
        let items = access
            .filter(&state, entries, |(backup, project_id)| {
                backup_resource(backup, Some(project_id))
            })
            .await?
            .into_iter()
            .map(|(backup, _)| backup)
            .collect();
        Ok(Json(BackupList { items, next_cursor }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Backups of every source the caller may read, newest first. Control-plane
/// backups are listed only to an `{all: true}` selector.
///
/// # Errors
///
/// `forbidden` without `backup:read`, `validation_failed` for a bad filter, or
/// a database error.
#[utoipa::path(
    get,
    path = "/api/v1/backups",
    tag = "backups",
    operation_id = "listBackups",
    extensions(("x-quoro-scope" = json!("backup:read"))),
    params(ListBackupsParams),
    responses(
        (status = 200, description = "One page of backups.", body = BackupList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_backups(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Query(page): Query<ListBackupsParams>,
) -> Result<Json<BackupList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, "GET /api/v1/backups", &headers);
    let mut params = Map::new();
    if let Some(kind) = &page.source_kind {
        params.insert("source_kind".to_string(), json!(kind));
    }
    if let Some(app_id) = &page.app_id {
        params.insert("app_id".to_string(), json!(app_id));
    }
    let access = ctx
        .authorize_read(
            &state,
            Scope::BackupRead,
            None,
            &Value::Object(params.clone()),
        )
        .await?;
    let result: Result<Json<BackupList>, ApiError> = async {
        if access.reads_nothing() {
            return Err(ApiError::forbidden(
                crate::error::Reason::SelectorMismatch,
                Some(Scope::BackupRead.as_str()),
            ));
        }
        let kind = match &page.source_kind {
            None => None,
            Some(value) => Some(BackupSourceKind::parse(value).ok_or_else(|| {
                ApiError::validation("source_kind is not volume or control_plane")
            })?),
        };
        if let Some(app_id) = &page.app_id
            && !crate::deploy::is_id_with_prefix("app", app_id)
        {
            return Err(ApiError::validation("app_id is not an app id"));
        }
        let limit = page.limit()?;
        let mut sql = format!("SELECT {BACKUP_COLUMNS} FROM backups WHERE (? = '' OR id < ?)");
        if kind.is_some() {
            sql.push_str(" AND source_kind = ?");
        }
        if page.app_id.is_some() {
            sql.push_str(" AND app_id = ?");
        }
        // A control-plane backup holds the whole cluster state: only
        // `{all: true}` sees it (permission-model.md section 5).
        if !access.sees_all() {
            sql.push_str(" AND source_kind = 'volume'");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        let mut query = sqlx::query(&sql).bind(page.after()).bind(page.after());
        if let Some(kind) = kind {
            query = query.bind(kind.as_str());
        }
        if let Some(app_id) = &page.app_id {
            query = query.bind(app_id);
        }
        let rows = query.bind(limit + 1).fetch_all(state.store.pool()).await?;
        let records = rows
            .iter()
            .map(backup_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let backups: Vec<Backup> = records.iter().map(BackupRecord::api).collect();
        let (items, next_cursor) = crate::api::page::finish(backups, limit, |b| b.id.clone());
        let mut kept = Vec::with_capacity(items.len());
        for backup in items {
            let project_id = backup_project(&state, backup.app_id.as_deref()).await;
            let visible = access
                .filter(
                    &state,
                    vec![(backup, project_id)],
                    |(backup, project_id)| backup_resource(backup, project_id.as_deref()),
                )
                .await?;
            kept.extend(visible.into_iter().map(|(backup, _)| backup));
        }
        Ok(Json(BackupList {
            items: kept,
            next_cursor,
        }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// One backup with its status, sizes, snapshot method and error.
///
/// # Errors
///
/// `forbidden` without `backup:read`, `not_found` for an unknown or malformed
/// id, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/backups/{backup_id}",
    tag = "backups",
    operation_id = "getBackup",
    extensions(("x-quoro-scope" = json!("backup:read"))),
    params(
        ("backup_id" = String, Path, description = "Backup id (`bkp_...`).",
            pattern = "^bkp_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The backup.", body = Backup,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_backup(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(backup_id): Path<String>,
) -> Result<Json<Backup>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/backups/{backup_id}",
        &headers,
    );
    let params = json!({ "backup_id": backup_id });
    let access = ctx
        .authorize_read(&state, Scope::BackupRead, None, &params)
        .await?;
    let result: Result<Json<Backup>, ApiError> = async {
        if !crate::deploy::is_id_with_prefix("bkp", &backup_id) {
            return Err(ApiError::not_found("backup"));
        }
        let row = sqlx::query(&format!(
            "SELECT {BACKUP_COLUMNS} FROM backups WHERE id = ?"
        ))
        .bind(&backup_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("backup"))?;
        let backup = backup_from_row(&row)?.api();
        let project_id = backup_project(&state, backup.app_id.as_deref()).await;
        let kept = access
            .filter(
                &state,
                vec![(backup, project_id)],
                |(backup, project_id)| backup_resource(backup, project_id.as_deref()),
            )
            .await?;
        kept.into_iter()
            .next()
            .map(|(backup, _)| Json(backup))
            .ok_or_else(|| ApiError::not_found("backup"))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Queues a backup of the app's volume with `trigger: api`.
///
/// # Errors
///
/// `forbidden` without `backup:write` or a justification for an agent,
/// `not_found` for an unknown app, `conflict` when the app has no `ready`
/// volume, the cluster has no repository, this primary does not hold the
/// repository key after a promotion, the volume already has an active backup
/// or a garbage-collection sweep runs, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/backups",
    tag = "backups",
    operation_id = "createBackup",
    extensions(("x-quoro-scope" = json!("backup:write"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original backup.",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 202, description = "The backup was accepted and is `queued`.", body = Backup,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_backup(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CREATE_BACKUP, &headers);
    let params = json!({ "app_id": app_id });
    let target = app_target(&app_id);
    let project_id = crate::api::volumes::app_project(&state, &app_id).await;
    ctx.authorize(
        &state,
        Scope::BackupWrite,
        Some(&target),
        project_id.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_BACKUP, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let backup: Backup = serde_json::from_value(stored)
                    .map_err(|error| ApiError::internal(&format!("replayed backup: {error}")))?;
                ctx.audit_replay(
                    &state,
                    Scope::BackupWrite,
                    &target,
                    project_id.as_deref(),
                    &params,
                )
                .await?;
                return Ok((status, Json(backup)).into_response());
            }
            Begin::Conflict => return Err(ApiError::conflict("Idempotency-Key reused")),
        }
    }

    match queue_backup(&state, &ctx, &app_id, project_id.as_deref(), &params).await {
        Ok(backup) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_BACKUP,
                    key,
                    StatusCode::ACCEPTED,
                    serde_json::to_value(&backup).unwrap_or(Value::Null),
                );
            }
            state.hub.node_changed(&backup.node_id);
            Ok((StatusCode::ACCEPTED, Json(backup)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_BACKUP, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::BackupWrite),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Queues the backup after the volume and repository checks.
async fn queue_backup(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    project_id: Option<&str>,
    params: &Value,
) -> Result<Backup, ApiError> {
    if !crate::deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    if state.backups.is_sweeping() {
        return Err(ApiError::conflict("a garbage-collection sweep is running"));
    }
    let repository = crate::backups::repo::any(&state.store)
        .await?
        .ok_or_else(|| ApiError::conflict("the cluster has no backup repository"))?;
    // `node-agent-protocol.md` section 7.3, step 4: no backup until the key is
    // back on this primary.
    if !repository.holds_key() {
        return Err(crate::backups::key_absent_conflict());
    }
    let volume = sqlx::query(
        "SELECT id, node_id FROM volumes WHERE app_id = ? AND state = 'ready' \
         ORDER BY id DESC LIMIT 1",
    )
    .bind(app_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or_else(|| ApiError::conflict("the app has no ready volume"))?;
    let volume_id: String = volume.try_get("id")?;
    let node_id: String = volume.try_get("node_id")?;
    let fencing_token = crate::backups::dispatch::current_lease(&state.store, app_id)
        .await?
        .ok_or_else(|| ApiError::conflict("the app has no local lease"))?;
    let actor = ctx.identity.actor();
    crate::backups::dispatch::queue(
        &state.store,
        crate::backups::dispatch::NewBackup {
            repository_id: &repository.id,
            source_kind: BackupSourceKind::Volume,
            app_id: Some(app_id),
            volume_id: Some(&volume_id),
            node_id: &node_id,
            fencing_token: Some(fencing_token),
            trigger: BackupTrigger::Api,
            created_by_type: actor.kind.as_str(),
            created_by_id: &actor.id,
            created_at: &crate::store::now_rfc3339(),
            project_id,
            request: Some((ctx, params)),
            dispatch_audit: false,
        },
    )
    .await
    .map(|record| record.api())
}

/// The cluster's backup repositories; at most one in M5a. No key is ever
/// returned.
///
/// # Errors
///
/// `forbidden` without `backup:read` or with a selector that reads nothing, or
/// a database error.
#[utoipa::path(
    get,
    path = "/api/v1/backup-repositories",
    tag = "backups",
    operation_id = "listBackupRepositories",
    extensions(("x-quoro-scope" = json!("backup:read"))),
    responses(
        (status = 200, description = "The backup repositories.", body = BackupRepositoryList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_backup_repositories(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
) -> Result<Json<BackupRepositoryList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/backup-repositories",
        &headers,
    );
    let access = ctx
        .authorize_read(&state, Scope::BackupRead, None, &json!({}))
        .await?;
    let result: Result<Json<BackupRepositoryList>, ApiError> = async {
        if access.reads_nothing() {
            return Err(ApiError::forbidden(
                crate::error::Reason::SelectorMismatch,
                Some(Scope::BackupRead.as_str()),
            ));
        }
        if !access.sees_all() {
            // A repository is cluster-level: `{all: true}` only.
            return Ok(Json(BackupRepositoryList { items: Vec::new() }));
        }
        let rows = sqlx::query(&format!(
            "SELECT {} FROM backup_repositories ORDER BY id",
            crate::backups::repo::REPOSITORY_COLUMNS
        ))
        .fetch_all(state.store.pool())
        .await?;
        let items = rows
            .iter()
            .map(crate::backups::repo::repository_from_row)
            .map(|row| row.map(|repository| repository_to_api(&repository)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Json(BackupRepositoryList { items }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Creates the cluster's backup repository and answers its recovery key once.
///
/// Human-only and owner-only (`backup:manage`). The `Idempotency-Key` header
/// is accepted for the contract but deliberately not cached: the body holds
/// the recovery key, and the primary must not keep a copy of it.
///
/// # Errors
///
/// `forbidden` for an agent (`reason: human_only`) or without
/// `backup:manage`, `conflict` when a repository exists or the prefix already
/// holds a config object, `validation_failed` for a provider that is not
/// `object_store`, is the audit export's, or whose probe write fails, or a
/// database error.
#[utoipa::path(
    post,
    path = "/api/v1/backup-repositories",
    tag = "backups",
    operation_id = "createBackupRepository",
    extensions(("x-quoro-scope" = json!("backup:manage"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create.",
            nullable = false, min_length = 1, max_length = 128),
    ),
    request_body(content = CreateBackupRepositoryRequest, content_type = "application/json",
        description = "Name, provider, prefix and the control-plane schedule."),
    responses(
        (status = 201, description = "The repository was created; the recovery key is in this response only.",
            body = BackupRepositoryWithRecoveryKey,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_backup_repository(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    ApiJson(body): ApiJson<CreateBackupRepositoryRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, CREATE_REPOSITORY, &headers);
    let params = serde_json::to_value(&body).unwrap_or(Value::Null);
    // Authored before the repository exists: the target is the cluster.
    ctx.authorize_human(&state, Scope::BackupManage, None, None, &params)
        .await?;
    match crate::backups::repo::create(&state, &ctx, &body, &params).await {
        Ok((repository, recovery_key)) => {
            let response = BackupRepositoryWithRecoveryKey {
                repository: repository_to_api(&repository),
                recovery_key,
            };
            Ok((StatusCode::CREATED, Json(response)).into_response())
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::BackupManage),
                None,
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// One backup repository with its recovery-key fingerprint and schedule.
///
/// # Errors
///
/// `forbidden` without `backup:read`, `not_found` for an unknown or malformed
/// id or a caller that may not see it, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/backup-repositories/{repository_id}",
    tag = "backups",
    operation_id = "getBackupRepository",
    extensions(("x-quoro-scope" = json!("backup:read"))),
    params(
        ("repository_id" = String, Path, description = "Backup repository id (`brp_...`).",
            pattern = "^brp_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The backup repository.", body = BackupRepository,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_backup_repository(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<BackupRepository>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/backup-repositories/{repository_id}",
        &headers,
    );
    let params = json!({ "repository_id": repository_id });
    let access = ctx
        .authorize_read(&state, Scope::BackupRead, None, &params)
        .await?;
    let result: Result<Json<BackupRepository>, ApiError> = async {
        if access.reads_nothing() {
            return Err(ApiError::forbidden(
                crate::error::Reason::SelectorMismatch,
                Some(Scope::BackupRead.as_str()),
            ));
        }
        if !access.sees_all() {
            return Err(ApiError::not_found("backup repository"));
        }
        if !crate::deploy::is_id_with_prefix("brp", &repository_id) {
            return Err(ApiError::not_found("backup repository"));
        }
        let repository = crate::backups::repo::by_id(&state.store, &repository_id)
            .await?
            .ok_or_else(|| ApiError::not_found("backup repository"))?;
        Ok(Json(repository_to_api(&repository)))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Changes the name or the control-plane schedule. Human-only and owner-only.
///
/// # Errors
///
/// `forbidden` for an agent (`reason: human_only`) or without
/// `backup:manage`, `not_found` for an unknown repository, `validation_failed`
/// for a bad field, or a database error.
#[utoipa::path(
    patch,
    path = "/api/v1/backup-repositories/{repository_id}",
    tag = "backups",
    operation_id = "updateBackupRepository",
    extensions(("x-quoro-scope" = json!("backup:manage"))),
    params(
        ("repository_id" = String, Path, description = "Backup repository id (`brp_...`).",
            pattern = "^brp_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = UpdateBackupRepositoryRequest, content_type = "application/json",
        description = "Fields to change; absent fields stay unchanged."),
    responses(
        (status = 200, description = "The updated backup repository.", body = BackupRepository,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn update_backup_repository(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
    ApiJson(body): ApiJson<UpdateBackupRepositoryRequest>,
) -> Result<Json<BackupRepository>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, UPDATE_REPOSITORY, &headers);
    let params = serde_json::to_value(&body).unwrap_or(Value::Null);
    let target = crate::backups::repo::repository_target(&repository_id);
    ctx.authorize_human(&state, Scope::BackupManage, Some(&target), None, &params)
        .await?;
    match crate::backups::repo::update(&state, &ctx, &repository_id, &body, &params).await {
        Ok(repository) => Ok(Json(repository_to_api(&repository))),
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::BackupManage),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Replaces the recovery key and answers the new one once. Human-only and
/// owner-only.
///
/// # Errors
///
/// `forbidden` for an agent (`reason: human_only`) or without
/// `backup:manage`, `not_found` for an unknown repository, `conflict` while
/// this primary does not hold the repository key (ADR 0322) or when the next
/// config object exists, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/backup-repositories/{repository_id}/recovery-key",
    tag = "backups",
    operation_id = "rotateBackupRecoveryKey",
    extensions(("x-quoro-scope" = json!("backup:manage"))),
    params(
        ("repository_id" = String, Path, description = "Backup repository id (`brp_...`).",
            pattern = "^brp_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The repository with its new recovery key, in this response only.",
            body = BackupRepositoryWithRecoveryKey,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn rotate_backup_recovery_key(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<BackupRepositoryWithRecoveryKey>, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, ROTATE_RECOVERY_KEY, &headers);
    let params = json!({});
    let target = crate::backups::repo::repository_target(&repository_id);
    ctx.authorize_human(&state, Scope::BackupManage, Some(&target), None, &params)
        .await?;
    match crate::backups::repo::rotate(&state, &ctx, &repository_id, &params).await {
        Ok((repository, recovery_key)) => Ok(Json(BackupRepositoryWithRecoveryKey {
            repository: repository_to_api(&repository),
            recovery_key,
        })),
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::BackupManage),
                Some(&target),
                None,
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Restores a succeeded, unpruned volume backup into a new volume on
/// `node_id` and moves the app there (`openapi.yaml` `restoreBackup`). A human
/// with `backup:restore` calls it directly; an agent reaches it through an
/// approval grant on the backup (permission-model.md section 9), otherwise the
/// call is refused with `approval_required`.
///
/// # Errors
///
/// `forbidden` without `backup:restore` or with `approval_required` for an
/// agent, `not_found` for an unknown backup, `validation_failed` for a
/// `node_id` that is not a node with the `node` role, `conflict` for a backup
/// that cannot be restored right now, including while this primary does not
/// hold the repository key, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/backups/{backup_id}/restore",
    tag = "backups",
    operation_id = "restoreBackup",
    extensions(("x-quoro-scope" = json!("backup:restore"))),
    params(
        ("backup_id" = String, Path, description = "Backup id (`bkp_...`).",
            pattern = "^bkp_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original volume instead of starting a second restore.",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = crate::backups::restore::RestoreBackupRequest, content_type = "application/json",
        description = "The node to restore onto."),
    responses(
        (status = 202, description = "The restore was accepted; the answer is the new volume in `restoring`.", body = Volume,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::ApprovalRequired),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn restore_backup(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(backup_id): Path<String>,
    ApiJson(body): ApiJson<crate::backups::restore::RestoreBackupRequest>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(identity, request_id.0, RESTORE_BACKUP, &headers);
    let params = json!({ "node_id": body.node_id });
    let target = backup_target(&backup_id);
    let project_id = backup_app_project(&state, &backup_id).await;
    ctx.authorize(
        &state,
        Scope::BackupRestore,
        Some(&target),
        project_id.as_deref(),
        &params,
    )
    .await?;

    let key = idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), RESTORE_BACKUP, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let volume: Volume = serde_json::from_value(stored)
                    .map_err(|error| ApiError::internal(&format!("replayed volume: {error}")))?;
                ctx.audit_replay(
                    &state,
                    Scope::BackupRestore,
                    &target,
                    project_id.as_deref(),
                    &params,
                )
                .await?;
                return Ok((status, Json(volume)).into_response());
            }
            Begin::Conflict => return Err(ApiError::conflict("Idempotency-Key reused")),
        }
    }

    match crate::backups::restore::accept(&state, &ctx, &backup_id, &body, &params).await {
        Ok(accepted) => {
            // The revoke reaches the old holder before the snapshot that
            // stops the app there (section 8.3).
            if let Some(old_node_id) = &accepted.old_node_id {
                if let Some(revoke) = &accepted.revoke {
                    state
                        .hub
                        .send_control(old_node_id, &ControlMessage::LeaseRevoke(revoke.clone()));
                }
                state.hub.node_changed(old_node_id);
            }
            // Every node that holds an instance hears that it stopped or, on
            // a move, that it left (issue #287).
            for node_id in &accepted.vm_node_ids {
                state.hub.node_changed(node_id);
            }
            state.hub.node_changed(&accepted.volume.node_id);
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    RESTORE_BACKUP,
                    key,
                    StatusCode::ACCEPTED,
                    serde_json::to_value(&accepted.volume).unwrap_or(Value::Null),
                );
            }
            Ok((StatusCode::ACCEPTED, Json(accepted.volume)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), RESTORE_BACKUP, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::BackupRestore),
                Some(&target),
                project_id.as_deref(),
                &params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// The project of the app a backup belongs to, for the selector and the audit
/// record; `None` for a control-plane backup or an unknown backup.
async fn backup_app_project(state: &AppState, backup_id: &str) -> Option<String> {
    let row: Option<(Option<String>,)> = sqlx::query_as("SELECT app_id FROM backups WHERE id = ?")
        .bind(backup_id)
        .fetch_optional(state.store.pool())
        .await
        .ok()
        .flatten();
    match row {
        Some((Some(app_id),)) => crate::api::volumes::app_project(state, &app_id).await,
        _ => None,
    }
}

/// Routes of the backup operations.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/apps/{app_id}/backups",
            get(list_app_backups).post(create_backup),
        )
        .route("/api/v1/backups", get(list_backups))
        .route("/api/v1/backups/{backup_id}", get(get_backup))
        .route("/api/v1/backups/{backup_id}/restore", post(restore_backup))
        .route(
            "/api/v1/backup-repositories",
            get(list_backup_repositories).post(create_backup_repository),
        )
        .route(
            "/api/v1/backup-repositories/{repository_id}",
            get(get_backup_repository).patch(update_backup_repository),
        )
        .route(
            "/api/v1/backup-repositories/{repository_id}/recovery-key",
            post(rotate_backup_recovery_key),
        )
}

/// OpenAPI paths and schemas of the backup operations.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list_app_backups,
        create_backup,
        list_backups,
        get_backup,
        list_backup_repositories,
        create_backup_repository,
        get_backup_repository,
        update_backup_repository,
        rotate_backup_recovery_key,
        restore_backup,
    ),
    components(
        schemas(
            Backup,
            BackupList,
            BackupRepository,
            BackupRepositoryList,
            BackupRepositoryWithRecoveryKey,
            CreateBackupRepositoryRequest,
            UpdateBackupRepositoryRequest,
            crate::backups::restore::RestoreBackupRequest,
        ),
        responses(
            crate::openapi::Unauthenticated,
            crate::openapi::Forbidden,
            crate::openapi::ApprovalRequired,
            crate::openapi::NotFound,
            crate::openapi::Conflict,
            crate::openapi::ValidationFailed,
            crate::openapi::NotPrimary,
            crate::openapi::RateLimited,
        )
    )
)]
pub struct BackupsDoc;
