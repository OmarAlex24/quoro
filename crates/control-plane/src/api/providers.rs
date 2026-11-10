//! `/api/v1/providers`: encrypted credentials for external providers
//! (D-0025, D-0050, D-0051).
//!
//! The secret is write-only. It is encrypted with the cluster data-encryption
//! key before it touches the database, the change log only ever holds the
//! ciphertext, and every audit record carries `"[redacted]"` in its place. The
//! idempotency cache keys on the secret's SHA-256, never on the secret.

use crate::AppState;
use crate::api::page::finish;

use crate::auth::Identity;
use crate::changelog::{self, Op};
use crate::crypto::Secret;
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::{now_rfc3339, sha256_hex};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// The operation string recorded in the audit log and used as the
/// idempotency scope.
const CREATE_OPERATION: &str = "POST /api/v1/providers";
const LIST_OPERATION: &str = "GET /api/v1/providers";
const GET_OPERATION: &str = "GET /api/v1/providers/{provider_id}";
const DELETE_OPERATION: &str = "DELETE /api/v1/providers/{provider_id}";
const ROTATE_OPERATION: &str = "POST /api/v1/providers/{provider_id}/rotate";

/// The `aad_context` that binds a blob to the `providers.secret_enc` column.
const SECRET_CONTEXT: &str = "providers.secret_enc";

/// What a provider entry is used for (`ProviderKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(
    title = "ProviderKind",
    description = "What the provider entry is used for: DNS records, tunnels, load balancing or object storage (D-0025, D-0051)."
)]
pub enum ProviderKind {
    Dns,
    Tunnel,
    Lb,
    ObjectStore,
}

impl ProviderKind {
    /// The wire form stored in `providers.kind`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Tunnel => "tunnel",
            Self::Lb => "lb",
            Self::ObjectStore => "object_store",
        }
    }

    /// Parses `providers.kind`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "dns" => Some(Self::Dns),
            "tunnel" => Some(Self::Tunnel),
            "lb" => Some(Self::Lb),
            "object_store" => Some(Self::ObjectStore),
            _ => None,
        }
    }
}

/// Credentials for one external provider (`Provider`). The secret is never part
/// of this shape.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "Provider",
    description = "Credentials for one external provider (D-0025, D-0051). The secret is write-only: it is stored encrypted with the cluster data-encryption key (D-0050), never returned by the API and redacted from audit params. The off-site audit-copy credential is a separate `object_store` provider entry whose key is put-only (D-0031)."
)]
pub struct Provider {
    /// Provider id.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    pub kind: ProviderKind,
    /// Label of the provider entry; unique in the cluster.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Non-secret settings such as zone id, account id, bucket, endpoint or
    /// region.
    #[schema(value_type = Object)]
    pub config: Value,
    /// Whether a secret is stored for this provider; the secret itself is never
    /// returned.
    pub has_secret: bool,
    /// User who created the provider entry.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub created_by: String,
    /// When the provider entry was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the secret was last replaced, or null if never.
    #[schema(format = "date-time")]
    pub rotated_at: Option<String>,
}

/// One page of providers.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "ProviderList", description = "One page of providers.")]
pub struct ProviderList {
    pub items: Vec<Provider>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Add credentials for an external provider.
#[derive(Clone, Deserialize, ToSchema)]
#[schema(
    title = "CreateProviderRequest",
    description = "Add credentials for an external provider."
)]
pub struct CreateProviderRequest {
    pub kind: ProviderKind,
    /// Label of the provider entry; unique in the cluster.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Non-secret settings such as zone id, account id, bucket, endpoint or
    /// region.
    #[schema(value_type = Object)]
    pub config: Value,
    /// API token, tunnel token or access key secret. Write-only; never
    /// returned.
    #[schema(write_only = true, min_length = 1)]
    pub secret: String,
}

/// Replace the stored secret of a provider.
#[derive(Clone, Deserialize, ToSchema)]
#[schema(
    title = "RotateProviderSecretRequest",
    description = "Replace the stored secret of a provider."
)]
pub struct RotateProviderSecretRequest {
    /// API token, tunnel token or access key secret. Write-only; never
    /// returned.
    #[schema(write_only = true, min_length = 1)]
    pub secret: String,
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
    /// The validated limit.
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

    /// The cursor, or the empty string, which sorts before every id.
    fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// Builds a [`Provider`] from a `providers` row.
fn provider_from_row(row: &SqliteRow) -> Result<Provider, ApiError> {
    let kind: String = row.try_get("kind")?;
    let kind = ProviderKind::parse(&kind)
        .ok_or_else(|| ApiError::internal(&format!("unknown provider kind {kind:?}")))?;
    let config_json: String = row.try_get("config_json")?;
    let config = serde_json::from_str(&config_json)
        .map_err(|error| ApiError::internal(&format!("stored provider config: {error}")))?;
    let secret_enc: Vec<u8> = row.try_get("secret_enc")?;
    Ok(Provider {
        id: row.try_get("id")?,
        kind,
        name: row.try_get("name")?,
        config,
        has_secret: !secret_enc.is_empty(),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        rotated_at: row.try_get("rotated_at")?,
    })
}

/// A provider without its secret, for in-process services.
#[derive(Clone, Debug)]
pub struct ProviderRecord {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub config: Value,
}

fn record_from_row(row: &SqliteRow) -> Result<ProviderRecord, ApiError> {
    let config_json: String = row.try_get("config_json")?;
    let config = serde_json::from_str(&config_json)
        .map_err(|error| ApiError::internal(&format!("stored provider config: {error}")))?;
    Ok(ProviderRecord {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        name: row.try_get("name")?,
        config,
    })
}

/// Validates the body before it is authorized. `kind` is already constrained to
/// the four contract values by [`ProviderKind`]'s deserializer.
fn validate(name: &str, config: &Value, secret: &Secret) -> Result<(), ApiError> {
    let len = name.chars().count();
    if !(1..=200).contains(&len) {
        return Err(ApiError::validation(
            "name must be between 1 and 200 characters",
        ));
    }
    if !config.is_object() {
        return Err(ApiError::validation("config must be a JSON object"));
    }
    let len = secret.expose().len();
    if !(1..=16384).contains(&len) {
        return Err(ApiError::validation(
            "secret must be between 1 and 16384 bytes",
        ));
    }
    Ok(())
}

/// Lowercase hex, for the ciphertext the change log may carry.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

/// The audit params shared by the create call: the secret is always redacted.
fn audit_params(kind: ProviderKind, name: &str, config: &Value) -> Value {
    json!({
        "kind": kind.as_str(),
        "name": name,
        "config": config,
        "secret": "[redacted]",
    })
}

/// Every provider entry of the cluster, without their secrets. Managing
/// providers is human-only (D-0029, D-0051).
#[utoipa::path(
    get,
    path = "/api/v1/providers",
    tag = "providers",
    operation_id = "listProviders",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(PageParams),
    responses(
        (status = 200, description = "One page of providers.", body = ProviderList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `provider:manage`, or a database error.
pub async fn list_providers(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<PageParams>,
) -> Result<Json<ProviderList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, LIST_OPERATION, &headers);
    let mut params = serde_json::Map::new();
    if let Some(limit) = page.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &page.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    let access = ctx
        .authorize_read(&state, Scope::ProviderManage, None, &Value::Object(params))
        .await?;
    let result: Result<_, ApiError> = async {
        let limit = page.limit()?;
        let rows = sqlx::query("SELECT * FROM providers WHERE id > ? ORDER BY id LIMIT ?")
            .bind(page.after())
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let providers = rows
            .iter()
            .map(provider_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_cursor) = finish(providers, limit, |provider| provider.id.clone());
        let items = access
            .filter(&state, items, |provider| crate::request::Resource {
                kind: "provider",
                id: &provider.id,
                app_id: None,
                project_id: None,
                node_id: None,
            })
            .await?;
        Ok(Json(ProviderList { items, next_cursor }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Stores credentials for an external provider. The secret is encrypted with
/// the cluster data-encryption key and never returned (D-0050).
#[utoipa::path(
    post,
    path = "/api/v1/providers",
    tag = "providers",
    operation_id = "createProvider",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
    ),
    request_body = CreateProviderRequest,
    responses(
        (status = 201, description = "The provider entry was created; its secret is not in the response.", body = Provider,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `validation_failed` for a bad name, config or secret, `forbidden` without
/// `provider:manage`, `conflict` for a duplicate name, or a database error.
pub async fn create_provider(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateProviderRequest>,
) -> Result<(StatusCode, Json<Provider>), ApiError> {
    // Move the plaintext into a `Secret` before anything else touches it.
    let CreateProviderRequest {
        kind,
        name,
        config,
        secret,
    } = body;
    let secret = Secret::new(secret);
    validate(&name, &config, &secret)?;
    let identity_id = identity.id().to_string();
    let audit_params = audit_params(kind, &name, &config);
    // The plaintext never reaches the idempotency cache; its hash is enough to
    // tell a replay from a reused key with a different body.
    let idem_params = json!({
        "kind": kind.as_str(),
        "name": &name,
        "config": &config,
        "secret_sha256": sha256_hex(secret.expose().as_bytes()),
    });
    // Authorize before claiming the idempotency slot: a refused call must not
    // leave the key in flight.
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    ctx.authorize(&state, Scope::ProviderManage, None, None, &audit_params)
        .await?;
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if let Some(key) = &key {
        match state
            .idempotency
            .begin(&identity_id, CREATE_OPERATION, key, &idem_params)
        {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let provider: Provider = serde_json::from_value(stored)
                    .map_err(|e| ApiError::internal(&format!("replayed provider: {e}")))?;
                let target = Target {
                    kind: "provider".to_string(),
                    id: provider.id.clone(),
                };
                ctx.audit_replay(&state, Scope::ProviderManage, &target, None, &audit_params)
                    .await?;
                return Ok((status, Json(provider)));
            }
            Begin::Conflict => return Err(ApiError::conflict("Idempotency-Key reused")),
        }
    }

    match insert_provider(&state, &ctx, kind, &name, &config, &secret, &audit_params).await {
        Ok(provider) => {
            if let Some(key) = &key {
                let stored = serde_json::to_value(&provider)
                    .map_err(|e| ApiError::internal(&format!("provider serialization: {e}")))?;
                state.idempotency.finish(
                    &identity_id,
                    CREATE_OPERATION,
                    key,
                    StatusCode::CREATED,
                    stored,
                );
            }
            Ok((StatusCode::CREATED, Json(provider)))
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(&identity_id, CREATE_OPERATION, key);
            }
            ctx.audit_error(
                &state,
                Some(Scope::ProviderManage),
                None,
                None,
                &audit_params,
                &error,
            )
            .await?;
            Err(error)
        }
    }
}

/// Inserts the provider, its change-log entry and the audit record in one
/// transaction.
async fn insert_provider(
    state: &AppState,
    ctx: &CallCtx,
    kind: ProviderKind,
    name: &str,
    config: &Value,
    secret: &Secret,
    params: &Value,
) -> Result<Provider, ApiError> {
    let mut w = state.store.begin_write().await?;
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM providers WHERE name = ?")
        .bind(name)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!("name {name} is taken")));
    }
    let id = new_id(Prefix::Provider);
    let now = now_rfc3339();
    let created_by = ctx.identity.id().to_string();
    let secret_enc = state
        .data_key
        .encrypt(SECRET_CONTEXT, &id, secret.expose().as_bytes());
    let config_json = serde_json::to_string(config)
        .map_err(|error| ApiError::internal(&format!("provider config: {error}")))?;
    sqlx::query(
        "INSERT INTO providers (id, kind, name, config_json, secret_enc, created_by, created_at, rotated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
    )
    .bind(&id)
    .bind(kind.as_str())
    .bind(name)
    .bind(&config_json)
    .bind(&secret_enc)
    .bind(&created_by)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let provider = Provider {
        id: id.clone(),
        kind,
        name: name.to_string(),
        config: config.clone(),
        has_secret: true,
        created_by,
        created_at: now.clone(),
        rotated_at: None,
    };
    // Replicas only ever see the ciphertext, never the secret.
    let payload = json!({
        "id": provider.id,
        "kind": provider.kind.as_str(),
        "name": provider.name,
        "config_json": provider.config,
        "secret_enc": hex(&secret_enc),
        "created_by": provider.created_by,
        "created_at": provider.created_at,
        "rotated_at": null,
    });
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "provider",
        &id,
        Op::Create,
        payload,
    )
    .await?;
    let target = Target {
        kind: "provider".to_string(),
        id: id.clone(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ProviderManage),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(provider)
}

/// One provider entry, its non-secret config and whether a secret is stored.
#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider_id}",
    tag = "providers",
    operation_id = "getProvider",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("provider_id" = String, Path, description = "Provider id (`prv_...`).", pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The provider entry.", body = Provider,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `provider:manage`, `not_found` for an unknown id, or a
/// database error.
pub async fn get_provider(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(provider_id): Path<String>,
) -> Result<Json<Provider>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, GET_OPERATION, &headers);
    let target = Target {
        kind: "provider".to_string(),
        id: provider_id.clone(),
    };
    let access = ctx
        .authorize_read(
            &state,
            Scope::ProviderManage,
            Some(&target),
            &json!({ "provider_id": provider_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        if !is_id(Prefix::Provider, &provider_id) {
            return Err(ApiError::not_found("provider"));
        }
        let row = sqlx::query("SELECT * FROM providers WHERE id = ?")
            .bind(&provider_id)
            .fetch_optional(state.store.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("provider"))?;
        Ok(Json(provider_from_row(&row)?))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Deletes the provider entry and its stored secret.
#[utoipa::path(
    delete,
    path = "/api/v1/providers/{provider_id}",
    tag = "providers",
    operation_id = "deleteProvider",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("provider_id" = String, Path, description = "Provider id (`prv_...`).", pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 204, description = "The provider entry was deleted.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `provider:manage`, `not_found` for an unknown or
/// malformed id, `conflict` while the provider is referenced, or a database
/// error.
pub async fn delete_provider(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(provider_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(identity, request_id, DELETE_OPERATION, &headers);
    let params = json!({ "provider_id": &provider_id });
    let target = Target {
        kind: "provider".to_string(),
        id: provider_id.clone(),
    };
    ctx.authorize(&state, Scope::ProviderManage, Some(&target), None, &params)
        .await?;
    if !is_id(Prefix::Provider, &provider_id) {
        return Err(ApiError::not_found("provider"));
    }
    match delete_provider_tx(&state, &ctx, &provider_id, &params).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
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

/// Deletes the row, its change-log entry and the audit record in one
/// transaction.
async fn delete_provider_tx(
    state: &AppState,
    ctx: &CallCtx,
    provider_id: &str,
    params: &Value,
) -> Result<(), ApiError> {
    let mut w = state.store.begin_write().await?;
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM providers WHERE id = ?")
        .bind(provider_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_none() {
        return Err(ApiError::not_found("provider"));
    }
    // ADR 0095: a provider still referenced by a domain, a node's tunnel
    // mapping or the off-site audit copy is refused (the foreign keys refuse
    // it too, but with a storage error instead of `conflict`).
    let (referenced,): (i64,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM domains WHERE provider_id = ?1)
             OR EXISTS (SELECT 1 FROM node_tunnels WHERE provider_id = ?1)
             OR EXISTS (SELECT 1 FROM audit_exports WHERE provider_id = ?1)",
    )
    .bind(provider_id)
    .fetch_one(&mut *w.tx)
    .await?;
    if referenced != 0 {
        return Err(ApiError::conflict(
            "provider is still referenced by a domain, a node tunnel or the audit copy",
        ));
    }
    sqlx::query("DELETE FROM providers WHERE id = ?")
        .bind(provider_id)
        .execute(&mut *w.tx)
        .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "provider",
        provider_id,
        Op::Delete,
        json!({ "id": provider_id }),
    )
    .await?;
    let target = Target {
        kind: "provider".to_string(),
        id: provider_id.to_string(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ProviderManage),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// Replaces the stored secret and sets `rotated_at`; the old secret stops being
/// used immediately.
#[utoipa::path(
    post,
    path = "/api/v1/providers/{provider_id}/rotate",
    tag = "providers",
    operation_id = "rotateProviderSecret",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("provider_id" = String, Path, description = "Provider id (`prv_...`).", pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body = RotateProviderSecretRequest,
    responses(
        (status = 200, description = "The secret was replaced; `rotated_at` is set.", body = Provider,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `validation_failed` for a bad secret, `forbidden` without
/// `provider:manage`, `not_found` for an unknown or malformed id, or a database
/// error.
pub async fn rotate_provider_secret(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(provider_id): Path<String>,
    ApiJson(body): ApiJson<RotateProviderSecretRequest>,
) -> Result<Json<Provider>, ApiError> {
    let secret = Secret::new(body.secret);
    let len = secret.expose().len();
    if !(1..=16384).contains(&len) {
        return Err(ApiError::validation(
            "secret must be between 1 and 16384 bytes",
        ));
    }
    let ctx = CallCtx::new(identity, request_id, ROTATE_OPERATION, &headers);
    let params = json!({ "secret": "[redacted]" });
    let target = Target {
        kind: "provider".to_string(),
        id: provider_id.clone(),
    };
    ctx.authorize(&state, Scope::ProviderManage, Some(&target), None, &params)
        .await?;
    if !is_id(Prefix::Provider, &provider_id) {
        return Err(ApiError::not_found("provider"));
    }
    match rotate_provider_tx(&state, &ctx, &provider_id, &secret, &params).await {
        Ok(provider) => Ok(Json(provider)),
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

/// Re-encrypts under the same row id, updates the row, the change log and the
/// audit record in one transaction.
async fn rotate_provider_tx(
    state: &AppState,
    ctx: &CallCtx,
    provider_id: &str,
    secret: &Secret,
    params: &Value,
) -> Result<Provider, ApiError> {
    let mut w = state.store.begin_write().await?;
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM providers WHERE id = ?")
        .bind(provider_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("provider"));
    }
    let now = now_rfc3339();
    let secret_enc =
        state
            .data_key
            .encrypt(SECRET_CONTEXT, provider_id, secret.expose().as_bytes());
    sqlx::query("UPDATE providers SET secret_enc = ?, rotated_at = ? WHERE id = ?")
        .bind(&secret_enc)
        .bind(&now)
        .bind(provider_id)
        .execute(&mut *w.tx)
        .await?;
    let row = sqlx::query("SELECT * FROM providers WHERE id = ?")
        .bind(provider_id)
        .fetch_one(&mut *w.tx)
        .await?;
    let provider = provider_from_row(&row)?;
    let payload = json!({
        "id": provider.id,
        "secret_enc": hex(&secret_enc),
        "rotated_at": provider.rotated_at,
    });
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "provider",
        provider_id,
        Op::Update,
        payload,
    )
    .await?;
    let target = Target {
        kind: "provider".to_string(),
        id: provider_id.to_string(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::ProviderManage),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(provider)
}

/// The provider row and its decrypted secret, for in-process services only
/// (ingress wiring). Never exposed over HTTP.
///
/// # Errors
///
/// `not_found` for an unknown id, a database error, or an internal error when
/// the stored blob cannot be decrypted.
pub async fn provider_secret(
    state: &AppState,
    provider_id: &str,
) -> Result<(ProviderRecord, Secret), ApiError> {
    provider_secret_in(&state.store, &state.data_key, provider_id).await
}

/// [`provider_secret`] from a store and the data-encryption key alone, for
/// in-process callers that do not build a full [`AppState`].
///
/// # Errors
///
/// As [`provider_secret`].
pub async fn provider_secret_in(
    store: &crate::store::Store,
    data_key: &crate::crypto::DataKey,
    provider_id: &str,
) -> Result<(ProviderRecord, Secret), ApiError> {
    let row =
        sqlx::query("SELECT id, kind, name, config_json, secret_enc FROM providers WHERE id = ?")
            .bind(provider_id)
            .fetch_optional(store.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("provider"))?;
    let record = record_from_row(&row)?;
    let secret_enc: Vec<u8> = row.try_get("secret_enc")?;
    let secret = data_key
        .decrypt(SECRET_CONTEXT, provider_id, &secret_enc)
        .map_err(|error| ApiError::internal(&error))?;
    Ok((record, secret))
}

/// Every provider of one kind, without secrets, for in-process services only
/// (ingress wiring). Never exposed over HTTP.
///
/// # Errors
///
/// A database error, or an internal error for a row that cannot be read.
pub async fn find_providers(state: &AppState, kind: &str) -> Result<Vec<ProviderRecord>, ApiError> {
    let rows =
        sqlx::query("SELECT id, kind, name, config_json FROM providers WHERE kind = ? ORDER BY id")
            .bind(kind)
            .fetch_all(state.store.pool())
            .await?;
    rows.iter().map(record_from_row).collect()
}
