// SPDX-License-Identifier: AGPL-3.0-only
//! `/api/v1/apps/{app_id}/domains` and `/api/v1/domains/{domain_id}`.
//!
//! A domain routes one hostname to an app; TLS is obtained with ACME for a
//! public-IP host or terminated by a Cloudflare Tunnel behind NAT (D-0024).

use crate::api::page::PageQuery;
use crate::error::ApiError;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use utoipa::{IntoParams, ToSchema};

/// Columns of [`Domain`], in the order [`domain_from_row`] expects them.
pub(crate) const DOMAIN_COLUMNS: &str =
    "id, app_id, hostname, tls_mode, cert_status, cert_expires_at, provider_id, created_at";

/// `domains.tls_mode` (`Domain.tls_mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    Acme,
    Tunnel,
}

impl TlsMode {
    /// The wire form stored in `domains.tls_mode`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acme => "acme",
            Self::Tunnel => "tunnel",
        }
    }
}

/// `domains.cert_status` (`Domain.cert_status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CertStatus {
    Pending,
    Issued,
    Renewing,
    Failed,
    NotApplicable,
}

impl CertStatus {
    /// The wire form stored in `domains.cert_status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Issued => "issued",
            Self::Renewing => "renewing",
            Self::Failed => "failed",
            Self::NotApplicable => "not_applicable",
        }
    }

    /// Parses `domains.cert_status`; `None` is an internal inconsistency.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "issued" => Some(Self::Issued),
            "renewing" => Some(Self::Renewing),
            "failed" => Some(Self::Failed),
            "not_applicable" => Some(Self::NotApplicable),
            _ => None,
        }
    }
}

/// A hostname routed to one app (`openapi.yaml` `Domain`).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "Domain",
    description = "A hostname routed to one app. TLS is obtained with ACME for public-IP hosts or terminated by a Cloudflare Tunnel for hosts behind NAT (D-0024)."
)]
pub struct Domain {
    /// Domain id.
    #[schema(pattern = "^dom_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// App the hostname routes to.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Hostname served for the app.
    #[schema(format = "hostname")]
    pub hostname: String,
    /// How TLS is provided.
    #[schema(inline)]
    pub tls_mode: TlsMode,
    /// Certificate state; `not_applicable` in tunnel mode.
    #[schema(inline)]
    pub cert_status: CertStatus,
    /// Expiry of the current certificate, when one exists.
    #[schema(required = false)]
    pub cert_expires_at: Option<String>,
    /// The provider whose credential manages the hostname's DNS records: a
    /// `dns` provider for `acme` or a `tunnel` provider for `tunnel` (D-0095).
    /// Null only for domains created before the field existed.
    #[schema(required = false, pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: Option<String>,
    /// When the domain was added.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// One page of domains (`openapi.yaml` `DomainList`).
#[derive(Debug, Serialize, ToSchema)]
#[schema(title = "DomainList", description = "One page of domains.")]
pub struct DomainList {
    pub items: Vec<Domain>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required = true)]
    pub next_cursor: Option<String>,
}

/// Body of `createAppDomain` (`openapi.yaml` `CreateDomainRequest`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(
    title = "CreateDomainRequest",
    description = "Route a hostname to an app."
)]
pub struct CreateDomainRequest {
    /// Hostname to serve for the app.
    #[schema(format = "hostname")]
    pub hostname: String,
    /// How TLS is provided (D-0024).
    #[schema(inline)]
    pub tls_mode: TlsMode,
    /// The provider that manages the hostname's DNS records: kind `dns` for
    /// `acme`, kind `tunnel` for `tunnel`. Optional when exactly one provider
    /// of that kind exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false, pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: Option<String>,
}

/// Query of `listAppDomains`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListDomainsQuery {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListDomainsQuery {
    /// `limit` and `cursor` in the shared page shape.
    #[must_use]
    pub fn page(&self) -> PageQuery {
        PageQuery {
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Builds a [`Domain`] from a `domains` row.
///
/// # Errors
///
/// An internal error for a stored value outside the contract.
pub(crate) fn domain_from_row(row: &SqliteRow) -> Result<Domain, ApiError> {
    let tls_mode: String = row.try_get("tls_mode")?;
    let tls_mode = match tls_mode.as_str() {
        "acme" => TlsMode::Acme,
        "tunnel" => TlsMode::Tunnel,
        other => return Err(ApiError::internal(&format!("unknown tls_mode {other:?}"))),
    };
    let cert_status: String = row.try_get("cert_status")?;
    let cert_status = CertStatus::parse(&cert_status)
        .ok_or_else(|| ApiError::internal(&format!("unknown cert_status {cert_status:?}")))?;
    Ok(Domain {
        id: row.try_get("id")?,
        app_id: row.try_get("app_id")?,
        hostname: row.try_get("hostname")?,
        tls_mode,
        cert_status,
        cert_expires_at: row.try_get("cert_expires_at")?,
        provider_id: row.try_get("provider_id")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Whether `hostname` is a lowercase DNS name: 1-253 characters, labels 1-63
/// of `[a-z0-9-]` that do not start or end with `-`.
#[must_use]
pub(crate) fn valid_hostname(hostname: &str) -> bool {
    if hostname.is_empty() || hostname.len() > 253 {
        return false;
    }
    hostname.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    })
}

use crate::AppState;

use crate::changelog::{self, Op};
use crate::deploy;
use crate::error::Target;
use crate::idempotency::Begin;
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::store::now_rfc3339;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use quoro_types::Scope;
use serde_json::{Value, json};

fn domain_target(domain_id: &str) -> Target {
    Target {
        kind: "domain".to_string(),
        id: domain_id.to_string(),
    }
}

/// Hostnames routed to the app, with their TLS mode and certificate state.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed app
/// id, `validation_failed` for a bad page bound, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/domains",
    tag = "domains",
    operation_id = "listAppDomains",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ListDomainsQuery,
    ),
    responses(
        (status = 200, description = "One page of domains.", body = DomainList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_app_domains(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(query): Query<ListDomainsQuery>,
) -> Result<Json<DomainList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/domains",
        &headers,
    );
    let mut params = serde_json::Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    let access = ctx
        .authorize_read(&state, Scope::AppRead, None, &Value::Object(params))
        .await?;
    let result: Result<_, ApiError> = async {
        if !deploy::is_id_with_prefix("app", &app_id) {
            return Err(ApiError::not_found("app"));
        }
        let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
            .bind(&app_id)
            .fetch_optional(state.store.pool())
            .await?;
        let Some((project_id,)) = project else {
            return Err(ApiError::not_found("app"));
        };
        let page_query = query.page();
        let limit = page_query.limit()?;
        let rows = sqlx::query(&format!(
            "SELECT {DOMAIN_COLUMNS} FROM domains WHERE app_id = ? AND id > ? ORDER BY id LIMIT ?"
        ))
        .bind(&app_id)
        .bind(page_query.after())
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
        let domains = rows
            .iter()
            .map(domain_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let entries: Vec<(Domain, String)> = domains
            .into_iter()
            .map(|domain| (domain, project_id.clone()))
            .collect();
        let (entries, next_cursor) =
            crate::api::page::finish(entries, limit, |(domain, _)| domain.id.clone());
        let entries = access
            .filter(&state, entries, |(domain, project_id)| {
                crate::request::Resource {
                    kind: "domain",
                    id: &domain.id,
                    app_id: Some(&domain.app_id),
                    project_id: Some(project_id),
                    node_id: None,
                }
            })
            .await?;
        let items = entries.into_iter().map(|(domain, _)| domain).collect();
        Ok(Json(DomainList { items, next_cursor }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// Routes a hostname to an app. TLS is obtained with ACME for a public-IP host
/// or provided by a Cloudflare Tunnel for a host behind NAT (D-0024).
///
/// # Errors
///
/// `forbidden` without `app:update`, `not_found` for an unknown or malformed
/// app id, `conflict` for a hostname already routed elsewhere or an idempotency
/// clash, `validation_failed` for a bad hostname, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/apps/{app_id}/domains",
    tag = "domains",
    operation_id = "createAppDomain",
    extensions(("x-quoro-scope" = json!("app:update"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).",
            pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Idempotency-Key" = Option<String>, Header,
            description = "Client-chosen key for a create. A repeated call with the same key and the same body returns the original result instead of creating a second resource (D-0031 records the call either way).",
            nullable = false, min_length = 1, max_length = 128),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = CreateDomainRequest, content_type = "application/json",
        description = "Hostname and TLS mode."),
    responses(
        (status = 201, description = "The domain was added and its certificate is `pending`.", body = Domain,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn create_app_domain(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    ApiJson(body): ApiJson<CreateDomainRequest>,
) -> Result<axum::response::Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "POST /api/v1/apps/{app_id}/domains",
        &headers,
    );
    let params = json!({
        "app_id": app_id,
        "hostname": body.hostname,
        "tls_mode": body.tls_mode.as_str(),
        "provider_id": body.provider_id,
    });
    let target = Target {
        kind: "app".to_string(),
        id: app_id.clone(),
    };
    ctx.authorize(&state, Scope::AppUpdate, Some(&target), None, &params)
        .await?;
    let key = crate::api::apps::idempotency_key(&headers)?;
    if let Some(key) = &key {
        match state.idempotency.begin(
            ctx.identity.id(),
            "POST /api/v1/apps/{app_id}/domains",
            key,
            &params,
        ) {
            Begin::Proceed => {}
            Begin::Replay(status, stored) => {
                let domain: Domain = serde_json::from_value(stored)
                    .map_err(|error| ApiError::internal(&format!("replayed domain: {error}")))?;
                ctx.audit_replay(
                    &state,
                    Scope::AppUpdate,
                    &domain_target(&domain.id),
                    None,
                    &params,
                )
                .await?;
                return Ok((status, Json(domain)).into_response());
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    &state,
                    Some(Scope::AppUpdate),
                    Some(&target),
                    None,
                    &params,
                    &error,
                )
                .await?;
                return Err(error);
            }
        }
    }

    match create_app_domain_tx(&state, &ctx, &app_id, &body, &params).await {
        Ok((domain, vm_ids)) => {
            if let Some(key) = &key {
                state.idempotency.finish(
                    ctx.identity.id(),
                    "POST /api/v1/apps/{app_id}/domains",
                    key,
                    StatusCode::CREATED,
                    serde_json::to_value(&domain).unwrap_or(Value::Null),
                );
            }
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok((StatusCode::CREATED, Json(domain)).into_response())
        }
        Err(error) => {
            if let Some(key) = &key {
                state.idempotency.abandon(
                    ctx.identity.id(),
                    "POST /api/v1/apps/{app_id}/domains",
                    key,
                );
            }
            ctx.audit_error(
                &state,
                Some(Scope::AppUpdate),
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

async fn create_app_domain_tx(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    body: &CreateDomainRequest,
    params: &Value,
) -> Result<(Domain, Vec<String>), ApiError> {
    if !valid_hostname(&body.hostname) {
        return Err(ApiError::validation(
            "hostname must be a lowercase DNS name of 1-253 characters",
        ));
    }
    if !deploy::is_id_with_prefix("app", app_id) {
        return Err(ApiError::not_found("app"));
    }
    let mut w = state.store.begin_write().await?;
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("app"));
    }
    let taken: Option<(String,)> = sqlx::query_as("SELECT id FROM domains WHERE hostname = ?")
        .bind(&body.hostname)
        .fetch_optional(&mut *w.tx)
        .await?;
    if taken.is_some() {
        return Err(ApiError::conflict(format!(
            "hostname {} is already routed",
            body.hostname
        )));
    }
    let provider_id =
        domain_provider(&mut w.tx, body.tls_mode, body.provider_id.as_deref()).await?;
    let cert_status = match body.tls_mode {
        TlsMode::Acme => CertStatus::Pending,
        TlsMode::Tunnel => CertStatus::NotApplicable,
    };
    let domain = Domain {
        id: deploy::new_id_with_prefix("dom"),
        app_id: app_id.to_string(),
        hostname: body.hostname.clone(),
        tls_mode: body.tls_mode,
        cert_status,
        cert_expires_at: None,
        provider_id: Some(provider_id),
        created_at: now_rfc3339(),
    };
    sqlx::query(
        "INSERT INTO domains (id, app_id, hostname, tls_mode, cert_status, cert_expires_at, provider_id, created_at) \
         VALUES (?, ?, ?, ?, ?, NULL, ?, ?)",
    )
    .bind(&domain.id)
    .bind(&domain.app_id)
    .bind(&domain.hostname)
    .bind(domain.tls_mode.as_str())
    .bind(domain.cert_status.as_str())
    .bind(&domain.provider_id)
    .bind(&domain.created_at)
    .execute(&mut *w.tx)
    .await?;
    let actor = ctx.identity.actor();
    let payload = serde_json::to_value(&domain)
        .map_err(|error| ApiError::internal(&format!("domain payload: {error}")))?;
    let change_seq =
        changelog::append(&mut w.tx, &actor, "domain", &domain.id, Op::Create, payload).await?;
    let vm_ids = crate::api::apps::app_vm_ids(&mut w.tx, app_id).await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppUpdate),
        Some(&domain_target(&domain.id)),
        None,
        params,
        Some(change_seq),
    )
    .await?;
    w.commit().await?;
    Ok((domain, vm_ids))
}

/// The provider a new domain records (`CreateDomainRequest.provider_id`,
/// D-0095): kind `dns` for `acme`, kind `tunnel` for `tunnel`. A named
/// provider must exist and be of that kind; without one, the only provider of
/// that kind is used.
async fn domain_provider(
    conn: &mut sqlx::SqliteConnection,
    tls_mode: TlsMode,
    requested: Option<&str>,
) -> Result<String, ApiError> {
    let kind = match tls_mode {
        TlsMode::Acme => "dns",
        TlsMode::Tunnel => "tunnel",
    };
    if let Some(requested) = requested {
        let found: Option<(String,)> = sqlx::query_as("SELECT kind FROM providers WHERE id = ?")
            .bind(requested)
            .fetch_optional(&mut *conn)
            .await?;
        return match found {
            Some((found,)) if found == kind => Ok(requested.to_string()),
            Some(_) => Err(ApiError::validation(format!(
                "provider_id must name a provider of kind {kind} for tls_mode {}",
                tls_mode.as_str()
            ))),
            None => Err(ApiError::validation("provider_id names no provider")),
        };
    }
    let found: Vec<(String,)> =
        sqlx::query_as("SELECT id FROM providers WHERE kind = ? ORDER BY id LIMIT 2")
            .bind(kind)
            .fetch_all(&mut *conn)
            .await?;
    match found.as_slice() {
        [(id,)] => Ok(id.clone()),
        [] => Err(ApiError::validation(format!(
            "tls_mode {} needs a provider of kind {kind}; add one first",
            tls_mode.as_str()
        ))),
        _ => Err(ApiError::validation(format!(
            "several providers of kind {kind} exist; name one in provider_id"
        ))),
    }
}

/// Removes the hostname and stops serving it.
///
/// # Errors
///
/// `forbidden` without `app:update`, `not_found` for an unknown or malformed
/// id, or a database error.
#[utoipa::path(
    delete,
    path = "/api/v1/domains/{domain_id}",
    tag = "domains",
    operation_id = "deleteDomain",
    extensions(("x-quoro-scope" = json!("app:update"))),
    params(
        ("domain_id" = String, Path, description = "Domain id (`dom_...`).",
            pattern = "^dom_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header,
            description = "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it.",
            nullable = false, min_length = 1, max_length = 2000),
        ("Quoro-Incident" = Option<String>, Header,
            description = "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032).",
            nullable = false, pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 204, description = "The domain was removed.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn delete_domain(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(domain_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "DELETE /api/v1/domains/{domain_id}",
        &headers,
    );
    let params = json!({ "domain_id": domain_id });
    let target = domain_target(&domain_id);
    ctx.authorize(&state, Scope::AppUpdate, Some(&target), None, &params)
        .await?;

    match delete_domain_tx(&state, &ctx, &domain_id, &params).await {
        Ok(vm_ids) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok(StatusCode::NO_CONTENT)
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::AppUpdate),
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

async fn delete_domain_tx(
    state: &AppState,
    ctx: &CallCtx,
    domain_id: &str,
    params: &Value,
) -> Result<Vec<String>, ApiError> {
    if !deploy::is_id_with_prefix("dom", domain_id) {
        return Err(ApiError::not_found("domain"));
    }
    let mut w = state.store.begin_write().await?;
    let row = sqlx::query(&format!(
        "SELECT {DOMAIN_COLUMNS} FROM domains WHERE id = ?"
    ))
    .bind(domain_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("domain"))?;
    let domain = domain_from_row(&row)?;
    sqlx::query("DELETE FROM domains WHERE id = ?")
        .bind(domain_id)
        .execute(&mut *w.tx)
        .await?;
    let actor = ctx.identity.actor();
    changelog::append(
        &mut w.tx,
        &actor,
        "domain",
        domain_id,
        Op::Delete,
        serde_json::to_value(&domain)
            .map_err(|error| ApiError::internal(&format!("domain payload: {error}")))?,
    )
    .await?;
    let vm_ids = crate::api::apps::app_vm_ids(&mut w.tx, &domain.app_id).await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::AppUpdate),
        Some(&domain_target(domain_id)),
        None,
        params,
        None,
    )
    .await?;
    w.commit().await?;
    Ok(vm_ids)
}
