// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/audit-exports`: the off-site copy of the audit log
//! (`audit-log-format.md` section 8, ADR 0149).
//!
//! `listAuditExports` needs `audit:read`; `createAuditExport` and
//! `probeAuditExport` need `provider:manage` and are human only, because the
//! copy is what makes the operator's own log tamper-evident (section 8.2).
//! [`routes`] mounts the three operations and [`AuditExportsDoc`] carries
//! their OpenAPI paths and schemas.

use crate::AppState;
use crate::audit_export::{self, ExportRow};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId, Resource};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use quoro_providers::object_store::LockStrength as StoreLockStrength;
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

/// What the bucket does against an overwrite or delete of an exported object
/// (`LockStrength`, section 8.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(
    title = "LockStrength",
    description = "What the bucket of an audit export does against an overwrite or delete of an exported object, as the probe observed it (ADR 0149, audit-log-format.md section 8.2). `compliance`: S3 Object Lock in compliance mode (AWS S3, Backblaze B2); the locked version survives every overwrite and delete. `bucket_rule`: a Cloudflare R2 bucket lock rule refuses the overwrite and the delete, and anyone who may edit the bucket's settings can remove the rule. `none`: no lock, or the probe failed."
)]
pub enum LockStrength {
    None,
    BucketRule,
    Compliance,
}

impl From<StoreLockStrength> for LockStrength {
    fn from(value: StoreLockStrength) -> Self {
        match value {
            StoreLockStrength::None => Self::None,
            StoreLockStrength::BucketRule => Self::BucketRule,
            StoreLockStrength::Compliance => Self::Compliance,
        }
    }
}

/// The off-site copy target of the audit log, with the lock strength its
/// probe observed (`openapi.yaml` `AuditExport`).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "AuditExport",
    description = "The off-site copy target of the audit log (ADR 0031, ADR 0149), with the lock strength its probe observed. One per cluster in M5a."
)]
pub struct AuditExport {
    /// Audit export id.
    #[schema(pattern = "^aex_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// The put-only `object_store` provider of the copy.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// Key prefix of the copy inside the bucket.
    pub prefix: String,
    /// What the bucket does against an overwrite or delete.
    pub lock_strength: LockStrength,
    /// When the probe last ran.
    #[schema(format = "date-time")]
    pub probed_at: Option<String>,
    /// What the probe saw, for example the status of the refused overwrite.
    #[schema(max_length = 2000)]
    pub probe_detail: Option<String>,
    /// The last audit `seq` in an acknowledged segment; 0 before the first.
    #[schema(minimum = 0)]
    pub last_exported_seq: i64,
    /// The `hash` of that record, as written in its head object.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub last_exported_hash: Option<String>,
    /// The last export or verification error, or null.
    #[schema(max_length = 2000)]
    pub last_error: Option<String>,
    /// When the record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

impl AuditExport {
    /// [`AuditExport`] from an `audit_exports` row.
    ///
    /// # Errors
    ///
    /// A row without a provider, which an M5a export always has.
    pub fn from_row(row: &ExportRow) -> Self {
        Self {
            id: row.id.clone(),
            provider_id: row.provider_id.clone().unwrap_or_default(),
            prefix: row.target.clone(),
            lock_strength: row.lock_strength.into(),
            probed_at: row.probed_at.clone(),
            probe_detail: row.probe_detail.clone(),
            last_exported_seq: row.last_exported_seq,
            last_exported_hash: row.last_exported_hash.clone(),
            last_error: row.last_error.clone(),
            updated_at: row.updated_at.clone(),
        }
    }
}

/// The audit exports of the cluster (`openapi.yaml` `AuditExportList`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "AuditExportList")]
pub struct AuditExportList {
    pub items: Vec<AuditExport>,
}

/// Configure the off-site copy of the audit log
/// (`openapi.yaml` `CreateAuditExportRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateAuditExportRequest")]
pub struct CreateAuditExportRequest {
    /// An `object_store` provider whose key may put and get, and should not
    /// be able to delete. It must not be the backup repository's provider.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// Key prefix of the copy inside the bucket.
    #[serde(default = "default_prefix")]
    #[schema(
        default = "quoro-audit",
        required = false,
        min_length = 1,
        max_length = 200,
        pattern = "^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$"
    )]
    pub prefix: String,
}

fn default_prefix() -> String {
    "quoro-audit".to_string()
}

/// Every audit export of the cluster.
///
/// # Errors
///
/// `forbidden` without `audit:read`, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/audit-exports",
    tag = "audit",
    operation_id = "listAuditExports",
    extensions(("x-quoro-scope" = json!("audit:read"))),
    responses(
        (status = 200, description = "The audit exports.", body = AuditExportList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_audit_exports(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
) -> Result<Json<AuditExportList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/audit-exports", &headers);
    let params = json!({});
    let access = ctx
        .authorize_read(&state, Scope::AuditRead, None, &params)
        .await?;
    let result: Result<_, ApiError> = async {
        let rows = sqlx::query(&format!(
            "SELECT {} FROM audit_exports ORDER BY id",
            audit_export::COLUMNS
        ))
        .fetch_all(state.store.pool())
        .await?;
        let items = rows
            .iter()
            .map(|row| audit_export::export_from_row(row).map(|row| AuditExport::from_row(&row)))
            .collect::<Result<Vec<_>, _>>()?;
        let items = access
            .filter(&state, items, |export| Resource {
                kind: "audit_export",
                id: &export.id,
                app_id: None,
                project_id: None,
                node_id: None,
            })
            .await?;
        Ok(AuditExportList { items })
    }
    .await;
    ctx.finish_read(&state, access, result).await.map(Json)
}

/// Configures the off-site audit copy and probes the bucket once.
///
/// # Errors
///
/// `forbidden` without `provider:manage` or for an agent, `conflict` when the
/// cluster already has an export, `validation_failed` for a bad prefix or
/// provider, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/audit-exports",
    tag = "audit",
    operation_id = "createAuditExport",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
    ),
    request_body(content = CreateAuditExportRequest, content_type = "application/json",
        description = "Provider and prefix."),
    responses(
        (status = 201, description = "The audit export was created and probed.", body = AuditExport,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_audit_export(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateAuditExportRequest>,
) -> Result<(StatusCode, Json<AuditExport>), ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        audit_export::CREATE_OPERATION,
        &headers,
    );
    let params: Value = json!({
        "provider_id": body.provider_id,
        "prefix": body.prefix,
    });
    ctx.authorize_human(&state, Scope::ProviderManage, None, None, &params)
        .await?;
    let key = crate::api::apps::idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state.idempotency.begin(
            ctx.identity.id(),
            audit_export::CREATE_OPERATION,
            key,
            &params,
        ) {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let export: AuditExport = serde_json::from_value(stored).map_err(|error| {
                    ApiError::internal(&format!("replayed audit export: {error}"))
                })?;
                ctx.audit_replay(
                    &state,
                    Scope::ProviderManage,
                    &target(&export.id),
                    None,
                    &params,
                )
                .await?;
                return Ok((status, Json(export)));
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::ProviderManage),
                    None,
                    None,
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match audit_export::create(&state, &ctx, &body).await {
        Ok(export) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    audit_export::CREATE_OPERATION,
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&export).unwrap_or(Value::Null),
                );
            }
            Ok((StatusCode::CREATED, Json(export)))
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), audit_export::CREATE_OPERATION, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::ProviderManage),
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

/// Runs the lock probe again and stores what it observed.
///
/// # Errors
///
/// `forbidden` without `provider:manage` or for an agent, `not_found` for an
/// unknown or malformed id, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/audit-exports/{export_id}/probe",
    tag = "audit",
    operation_id = "probeAuditExport",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("export_id" = String, Path, description = "Audit export id (`aex_...`).",
            pattern = "^aex_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The audit export with the new probe result.", body = AuditExport,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn probe_audit_export(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(export_id): Path<String>,
) -> Result<Json<AuditExport>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "POST /api/v1/audit-exports/{export_id}/probe",
        &headers,
    );
    let params = json!({ "export_id": export_id });
    let target = target(&export_id);
    ctx.authorize_human(&state, Scope::ProviderManage, Some(&target), None, &params)
        .await?;
    if !crate::deploy::is_id_with_prefix("aex", &export_id) {
        return Err(ApiError::not_found("audit export"));
    }
    match audit_export::probe::run_for_call(&state, &ctx, &export_id, &params).await {
        Ok(export) => Ok(Json(export)),
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::ProviderManage),
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

fn target(export_id: &str) -> Target {
    Target {
        kind: "audit_export".to_string(),
        id: export_id.to_string(),
    }
}

/// Every implemented audit-export route.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/audit-exports",
            get(list_audit_exports).post(create_audit_export),
        )
        .route(
            "/api/v1/audit-exports/{export_id}/probe",
            post(probe_audit_export),
        )
}

/// OpenAPI paths and schemas of the audit-export operations.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(list_audit_exports, create_audit_export, probe_audit_export),
    components(
        schemas(AuditExport, AuditExportList, CreateAuditExportRequest, LockStrength),
        responses(
            crate::openapi::Unauthenticated,
            crate::openapi::Forbidden,
            crate::openapi::NotFound,
            crate::openapi::Conflict,
            crate::openapi::ValidationFailed,
            crate::openapi::NotPrimary,
            crate::openapi::RateLimited,
        )
    )
)]
pub struct AuditExportsDoc;
