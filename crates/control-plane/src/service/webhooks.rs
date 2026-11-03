//! Webhook operations (`/api/v1/webhooks*`, D-0032).
//!
//! A webhook is a replicated table: its change-log entry carries the encrypted
//! secret (`secret_enc`, ciphertext only), never the plaintext. The signing
//! secret is generated server-side and returned once by `createWebhook`.
//! Deliveries are LOCAL (`webhook_deliveries`) and written by
//! [`crate::dispatcher`].

use crate::AppState;
use crate::api::webhooks::{
    CREATE_OPERATION, CreateWebhookRequest, DeliveryStatus, EventType, UpdateWebhookRequest,
    Webhook, WebhookCreated, WebhookDelivery, WebhookDeliveryList, WebhookList, WebhookPage,
    WebhookWake,
};
use crate::authz::Selector;
use crate::changelog::{self, Op};
use crate::dispatcher::{self, SendOutcome};
use crate::error::{ApiError, Target};
use crate::idempotency::Begin;
use crate::request::CallCtx;
use crate::store::{now_rfc3339, random_secret};
use quoro_types::{Prefix, Scope, is_id, new_id};
use serde_json::{Value, json};
use sqlx::{Row, SqliteConnection};

/// The `aad_context` that binds a blob to the `webhooks.secret_enc` column.
pub(crate) const SECRET_CONTEXT: &str = "webhooks.secret_enc";

/// Every column of [`WebhookRow`], in the order [`webhook_row_from_row`] reads
/// them.
const WEBHOOK_COLUMNS: &str = "id, name, url, event_types, selector, secret_enc, enabled, wake, created_by, created_at, \
     updated_at";

/// Every column of [`WebhookDelivery`].
const DELIVERY_COLUMNS: &str = "id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, \
     test, created_at";

/// A `webhooks` row, including the encrypted secret. Only the dispatcher and
/// the service layer read the secret.
#[derive(Clone, Debug)]
pub(crate) struct WebhookRow {
    pub id: String,
    pub name: String,
    pub url: String,
    pub event_types: Vec<EventType>,
    pub selector: Selector,
    pub secret_enc: Vec<u8>,
    pub enabled: bool,
    pub wake: Option<WebhookWake>,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

impl WebhookRow {
    /// The wire form, without the secret.
    #[must_use]
    pub(crate) fn webhook(&self) -> Webhook {
        Webhook {
            id: self.id.clone(),
            name: self.name.clone(),
            url: self.url.clone(),
            event_types: self.event_types.clone(),
            selector: self.selector.clone(),
            enabled: self.enabled,
            wake: self.wake.clone(),
            secret: None,
            created_by: self.created_by.clone(),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
        }
    }
}

/// Builds a [`WebhookRow`] from a row selected with [`WEBHOOK_COLUMNS`].
///
/// # Errors
///
/// An internal error for a row whose JSON columns do not parse.
pub(crate) fn webhook_row_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookRow, ApiError> {
    let event_types: String = row.try_get("event_types")?;
    let selector: String = row.try_get("selector")?;
    let wake: Option<String> = row.try_get("wake")?;
    let raw_event_types: Vec<String> = serde_json::from_str(&event_types)
        .map_err(|e| ApiError::internal(&format!("webhook event_types: {e}")))?;
    let event_types = raw_event_types
        .iter()
        .map(|kind| {
            EventType::parse(kind)
                .ok_or_else(|| ApiError::internal(&format!("unknown webhook event type {kind}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(WebhookRow {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        url: row.try_get("url")?,
        event_types,
        selector: serde_json::from_str(&selector).unwrap_or_default(),
        secret_enc: row.try_get("secret_enc")?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        wake: wake
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| ApiError::internal(&format!("webhook wake: {e}")))
            })
            .transpose()?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The change-log payload of a webhook: the whole row, with the secret as
/// ciphertext only, so a replica rebuilds the table by replaying the log.
fn webhook_payload(row: &WebhookRow) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "url": row.url,
        "event_types": row.event_types.iter().map(|kind| kind.as_str()).collect::<Vec<_>>(),
        "selector": row.selector,
        "secret_enc": hex(&row.secret_enc),
        "enabled": row.enabled,
        "wake": row.wake,
        "created_by": row.created_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}

fn delivery_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookDelivery, ApiError> {
    let status: String = row.try_get("status")?;
    let status = match status.as_str() {
        "pending" => DeliveryStatus::Pending,
        "delivered" => DeliveryStatus::Delivered,
        "failed" => DeliveryStatus::Failed,
        other => {
            return Err(ApiError::internal(&format!(
                "unknown delivery status {other}"
            )));
        }
    };
    Ok(WebhookDelivery {
        id: row.try_get("id")?,
        webhook_id: row.try_get("webhook_id")?,
        event_id: row.try_get("event_id")?,
        attempt: row.try_get("attempt")?,
        status,
        response_code: row.try_get("response_code")?,
        error: row.try_get("error")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        test: row.try_get("test")?,
        created_at: row.try_get("created_at")?,
    })
}

fn webhook_target(webhook_id: &str) -> Target {
    Target {
        kind: "webhook".to_string(),
        id: webhook_id.to_string(),
    }
}

fn valid_url(url: &str) -> bool {
    // Parsed, not prefix-matched: `http://localhost:80@elsewhere` names
    // another host, and credentials never belong in a receiver URL.
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return false;
    }
    match (parsed.scheme(), parsed.host_str()) {
        ("https", Some(host)) => !host.is_empty(),
        ("http", Some(host)) => host == "localhost" && url.starts_with("http://localhost"),
        _ => false,
    }
}

fn validate_selector(selector: &Selector) -> Result<(), ApiError> {
    for slug in selector.projects.iter().flatten() {
        if !valid_slug(slug) {
            return Err(ApiError::validation(format!(
                "selector project {slug:?} is not a valid project slug"
            )));
        }
    }
    for app_id in selector.apps.iter().flatten() {
        if !crate::deploy::is_id_with_prefix("app", app_id) {
            return Err(ApiError::validation(format!(
                "selector app {app_id:?} is not a valid app id"
            )));
        }
    }
    for node_id in selector.nodes.iter().flatten() {
        if !is_id(Prefix::Node, node_id) {
            return Err(ApiError::validation(format!(
                "selector node {node_id:?} is not a valid node id"
            )));
        }
    }
    Ok(())
}

fn valid_slug(slug: &str) -> bool {
    (1..=40).contains(&slug.len())
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Section 8.1: the wake scopes are validated when the webhook is saved, so a
/// webhook can never route around the destructive and human-only rules.
fn validate_wake(wake: &WebhookWake) -> Result<(), ApiError> {
    if !is_id(Prefix::Agent, &wake.agent_id) {
        return Err(ApiError::validation(
            "wake.agent_id is not a valid agent id",
        ));
    }
    for scope in &wake.grant_scopes {
        if scope.is_human_only() {
            return Err(ApiError::validation(format!(
                "wake scope {} is human-only and cannot be granted to an agent",
                scope.as_str()
            )));
        }
        if scope.is_destructive() {
            return Err(ApiError::validation(format!(
                "wake scope {} is destructive: an incident grant cannot carry it",
                scope.as_str()
            )));
        }
    }
    if wake.grant_ttl_s < 1 {
        return Err(ApiError::validation("wake.grant_ttl_s must be at least 1"));
    }
    if wake.grant_max_uses.is_some_and(|max| max < 1) {
        return Err(ApiError::validation(
            "wake.grant_max_uses must be at least 1",
        ));
    }
    Ok(())
}

fn validate(
    name: &str,
    url: &str,
    selector: &Selector,
    wake: Option<&WebhookWake>,
) -> Result<(), ApiError> {
    if name.trim().is_empty() {
        return Err(ApiError::validation("name must not be empty"));
    }
    if name.len() > 200 {
        return Err(ApiError::validation("name is longer than 200 characters"));
    }
    if !valid_url(url) {
        return Err(ApiError::validation(
            "url must start with https:// or http://localhost (for testing)",
        ));
    }
    validate_selector(selector)?;
    if let Some(wake) = wake {
        validate_wake(wake)?;
    }
    Ok(())
}

async fn read_webhook_row(
    conn: &mut SqliteConnection,
    webhook_id: &str,
) -> Result<WebhookRow, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE id = ?"
    ))
    .bind(webhook_id)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| ApiError::not_found("webhook"))?;
    webhook_row_from_row(&row)
}

async fn read_webhook(state: &AppState, webhook_id: &str) -> Result<Webhook, ApiError> {
    let mut conn = state.store.pool().acquire().await?;
    let row = read_webhook_row(&mut conn, webhook_id).await?;
    Ok(row.webhook())
}

async fn read_delivery(state: &AppState, delivery_id: &str) -> Result<WebhookDelivery, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {DELIVERY_COLUMNS} FROM webhook_deliveries WHERE id = ?"
    ))
    .bind(delivery_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or_else(|| ApiError::not_found("webhook delivery"))?;
    delivery_from_row(&row)
}

/// Creates a webhook. The secret is generated here, encrypted with the cluster
/// data-encryption key and returned only in the response.
///
/// # Errors
///
/// `validation_failed` for the URL, selector, event types or wake
/// configuration, `forbidden` without `webhook:write` (agents get
/// `human_only`), `conflict` for a reused idempotency key, or a database
/// error.
pub async fn create_webhook(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateWebhookRequest,
    key: Option<String>,
) -> Result<WebhookCreated, ApiError> {
    let params = serde_json::to_value(body)
        .map_err(|e| ApiError::internal(&format!("webhook params: {e}")))?;
    ctx.authorize_human(state, Scope::WebhookWrite, None, None, &params)
        .await?;

    if let Some(key) = &key {
        match state
            .idempotency
            .begin(ctx.identity.id(), CREATE_OPERATION, key, &params)
        {
            Begin::Proceed => {}
            Begin::Replay(_, stored) => {
                let id = stored["id"]
                    .as_str()
                    .ok_or_else(|| ApiError::internal(&"replayed webhook has no id"))?
                    .to_string();
                let webhook = read_webhook(state, &id).await?;
                ctx.audit_replay(
                    state,
                    Scope::WebhookWrite,
                    &webhook_target(&id),
                    None,
                    &params,
                )
                .await?;
                let secret = stored["secret"]
                    .as_str()
                    .ok_or_else(|| ApiError::internal(&"replayed webhook has no secret"))?
                    .to_string();
                return Ok(WebhookCreated { webhook, secret });
            }
            Begin::Conflict => {
                let error = ApiError::conflict("Idempotency-Key reused");
                ctx.audit_error(
                    state,
                    Some(Scope::WebhookWrite),
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

    if let Err(error) = validate(&body.name, &body.url, &body.selector, body.wake.as_ref()) {
        ctx.audit_error(
            state,
            Some(Scope::WebhookWrite),
            None,
            None,
            &params,
            &error,
        )
        .await?;
        return Err(error);
    }

    match insert_webhook_tx(state, ctx, body, &params).await {
        Ok(created) => {
            if let Some(key) = &key {
                let stored = serde_json::json!({
                    "id": created.webhook.id,
                    "secret": created.secret,
                });
                state.idempotency.finish(
                    ctx.identity.id(),
                    CREATE_OPERATION,
                    key,
                    axum::http::StatusCode::CREATED,
                    stored,
                );
            }
            Ok(created)
        }
        Err(error) => {
            if let Some(key) = &key {
                state
                    .idempotency
                    .abandon(ctx.identity.id(), CREATE_OPERATION, key);
            }
            ctx.audit_error(
                state,
                Some(Scope::WebhookWrite),
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

async fn insert_webhook_tx(
    state: &AppState,
    ctx: &CallCtx,
    body: &CreateWebhookRequest,
    params: &Value,
) -> Result<WebhookCreated, ApiError> {
    let mut w = state.store.begin_write().await?;
    if let Some(wake) = &body.wake {
        let agent: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE id = ?")
            .bind(&wake.agent_id)
            .fetch_optional(&mut *w.tx)
            .await?;
        if agent.is_none() {
            return Err(ApiError::validation(format!(
                "wake.agent_id {} names no agent",
                wake.agent_id
            )));
        }
    }
    let id = new_id(Prefix::Webhook);
    let now = now_rfc3339();
    let secret = random_secret();
    let secret_enc = state
        .data_key
        .encrypt(SECRET_CONTEXT, &id, secret.as_bytes());
    let event_types: Vec<&str> = body.event_types.iter().map(|kind| kind.as_str()).collect();
    let event_types = serde_json::to_string(&event_types)
        .map_err(|e| ApiError::internal(&format!("webhook event types: {e}")))?;
    let selector = serde_json::to_string(&body.selector)
        .map_err(|e| ApiError::internal(&format!("webhook selector: {e}")))?;
    let wake = body
        .wake
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| ApiError::internal(&format!("webhook wake: {e}")))?;
    sqlx::query(
        "INSERT INTO webhooks (id, name, url, event_types, selector, secret_enc, enabled, wake, created_by, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&body.name)
    .bind(&body.url)
    .bind(&event_types)
    .bind(&selector)
    .bind(&secret_enc)
    .bind(i64::from(body.enabled))
    .bind(&wake)
    .bind(ctx.identity.id())
    .bind(&now)
    .bind(&now)
    .execute(&mut *w.tx)
    .await?;
    let row = WebhookRow {
        id: id.clone(),
        name: body.name.clone(),
        url: body.url.clone(),
        event_types: body.event_types.clone(),
        selector: body.selector.clone(),
        secret_enc,
        enabled: body.enabled,
        wake: body.wake.clone(),
        created_by: ctx.identity.id().to_string(),
        created_at: now.clone(),
        updated_at: now,
    };
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "webhook",
        &id,
        Op::Create,
        webhook_payload(&row),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::WebhookWrite),
        Some(&webhook_target(&id)),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(WebhookCreated {
        webhook: Webhook {
            id: row.id,
            name: row.name,
            url: row.url,
            event_types: row.event_types,
            selector: row.selector,
            enabled: row.enabled,
            wake: row.wake,
            secret: None,
            created_by: row.created_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
        },
        secret,
    })
}

/// Lists webhooks one page at a time, keeping only the rows the caller may see.
///
/// # Errors
///
/// `forbidden` without `webhook:read`, `validation_failed` for a bad page, or
/// a database error.
pub async fn list_webhooks(
    state: &AppState,
    ctx: &CallCtx,
    page: &WebhookPage,
) -> Result<WebhookList, ApiError> {
    let params = json!({ "limit": page.limit });
    let access = ctx
        .authorize_read(state, Scope::WebhookRead, None, &params)
        .await?;
    let result: Result<WebhookList, ApiError> = async {
        let limit = page.limit()?;
        if access.reads_nothing() {
            return Ok(WebhookList {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let rows = sqlx::query(&format!(
            "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE id > ? ORDER BY id"
        ))
        .bind(page.after())
        .fetch_all(state.store.pool())
        .await?;
        let webhooks = rows
            .iter()
            .map(webhook_row_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        access
            .filter(state, webhooks, |webhook| crate::request::Resource {
                kind: "webhook",
                id: &webhook.id,
                app_id: None,
                project_id: None,
                node_id: None,
            })
            .await
            .map(|webhooks| {
                let webhooks: Vec<Webhook> = webhooks
                    .into_iter()
                    .map(|webhook| webhook.webhook())
                    .collect();
                let (items, next_cursor) =
                    crate::api::page::finish(webhooks, limit, |webhook| webhook.id.clone());
                WebhookList { items, next_cursor }
            })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One webhook by id.
///
/// # Errors
///
/// `forbidden` without `webhook:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_webhook(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
) -> Result<Webhook, ApiError> {
    let params = json!({ "webhook_id": webhook_id });
    let target = webhook_target(webhook_id);
    let access = ctx
        .authorize_read(state, Scope::WebhookRead, Some(&target), &params)
        .await?;
    let result: Result<Webhook, ApiError> = async {
        if !is_id(Prefix::Webhook, webhook_id) {
            return Err(ApiError::not_found("webhook"));
        }
        read_webhook(state, webhook_id).await
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Changes a webhook; absent fields stay unchanged and the secret is untouched.
///
/// # Errors
///
/// `validation_failed` for the URL, selector, event types or wake
/// configuration, `forbidden` without `webhook:write` (agents get
/// `human_only`), `not_found` for an unknown id, or a database error.
pub async fn update_webhook(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
    body: &UpdateWebhookRequest,
) -> Result<Webhook, ApiError> {
    let params = serde_json::to_value(body)
        .map_err(|e| ApiError::internal(&format!("webhook params: {e}")))?;
    let target = webhook_target(webhook_id);
    ctx.authorize_human(state, Scope::WebhookWrite, Some(&target), None, &params)
        .await?;
    match update_webhook_tx(state, ctx, webhook_id, body, &target, &params).await {
        Ok(webhook) => Ok(webhook),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::WebhookWrite),
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

async fn update_webhook_tx(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
    body: &UpdateWebhookRequest,
    target: &Target,
    params: &Value,
) -> Result<Webhook, ApiError> {
    if !is_id(Prefix::Webhook, webhook_id) {
        return Err(ApiError::not_found("webhook"));
    }
    let mut w = state.store.begin_write().await?;
    let mut row = read_webhook_row(&mut w.tx, webhook_id).await?;
    if let Some(name) = &body.name {
        row.name = name.clone();
    }
    if let Some(url) = &body.url {
        row.url = url.clone();
    }
    if let Some(event_types) = &body.event_types {
        row.event_types = event_types.clone();
    }
    if let Some(selector) = &body.selector {
        row.selector = selector.clone();
    }
    if let Some(enabled) = body.enabled {
        row.enabled = enabled;
    }
    if let Some(wake) = &body.wake {
        row.wake = Some(wake.clone());
    }
    validate(&row.name, &row.url, &row.selector, row.wake.as_ref())?;
    if let Some(wake) = &row.wake {
        let agent: Option<(String,)> = sqlx::query_as("SELECT id FROM agents WHERE id = ?")
            .bind(&wake.agent_id)
            .fetch_optional(&mut *w.tx)
            .await?;
        if agent.is_none() {
            return Err(ApiError::validation(format!(
                "wake.agent_id {} names no agent",
                wake.agent_id
            )));
        }
    }
    row.updated_at = now_rfc3339();
    let event_types: Vec<&str> = row.event_types.iter().map(|kind| kind.as_str()).collect();
    let event_types = serde_json::to_string(&event_types)
        .map_err(|e| ApiError::internal(&format!("webhook event types: {e}")))?;
    let selector = serde_json::to_string(&row.selector)
        .map_err(|e| ApiError::internal(&format!("webhook selector: {e}")))?;
    let wake = row
        .wake
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| ApiError::internal(&format!("webhook wake: {e}")))?;
    sqlx::query(
        "UPDATE webhooks SET name = ?, url = ?, event_types = ?, selector = ?, enabled = ?, wake = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&row.name)
    .bind(&row.url)
    .bind(&event_types)
    .bind(&selector)
    .bind(i64::from(row.enabled))
    .bind(&wake)
    .bind(&row.updated_at)
    .bind(webhook_id)
    .execute(&mut *w.tx)
    .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "webhook",
        webhook_id,
        Op::Update,
        webhook_payload(&row),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::WebhookWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(row.webhook())
}

/// Deletes a webhook.
///
/// # Errors
///
/// `forbidden` without `webhook:write` (agents get `human_only`), `not_found`
/// for an unknown id, or a database error.
pub async fn delete_webhook(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
) -> Result<(), ApiError> {
    let params = json!({ "webhook_id": webhook_id });
    let target = webhook_target(webhook_id);
    ctx.authorize_human(state, Scope::WebhookWrite, Some(&target), None, &params)
        .await?;
    match delete_webhook_tx(state, ctx, webhook_id, &target, &params).await {
        Ok(()) => Ok(()),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::WebhookWrite),
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

async fn delete_webhook_tx(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
    target: &Target,
    params: &Value,
) -> Result<(), ApiError> {
    if !is_id(Prefix::Webhook, webhook_id) {
        return Err(ApiError::not_found("webhook"));
    }
    let mut w = state.store.begin_write().await?;
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM webhooks WHERE id = ?")
        .bind(webhook_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("webhook"));
    }
    sqlx::query("DELETE FROM webhooks WHERE id = ?")
        .bind(webhook_id)
        .execute(&mut *w.tx)
        .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "webhook",
        webhook_id,
        Op::Delete,
        json!({ "id": webhook_id }),
    )
    .await?;
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::WebhookWrite),
        Some(target),
        None,
        params,
        Some(seq),
    )
    .await?;
    w.commit().await?;
    Ok(())
}

/// Sends one signed test delivery and records it in the delivery history. The
/// test body is the most recent event envelope, so a receiver verifies
/// `Quoro-Signature` against a real payload.
///
/// # Errors
///
/// `forbidden` without `webhook:write` (agents get `human_only`), `not_found`
/// for an unknown id, `validation_failed` when the event log is empty, or a
/// database error.
pub async fn test_webhook(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
) -> Result<WebhookDelivery, ApiError> {
    let params = json!({ "webhook_id": webhook_id });
    let target = webhook_target(webhook_id);
    ctx.authorize_human(state, Scope::WebhookWrite, Some(&target), None, &params)
        .await?;
    match test_webhook_tx(state, ctx, webhook_id, &target, &params).await {
        Ok(delivery) => Ok(delivery),
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::WebhookWrite),
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

async fn test_webhook_tx(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
    target: &Target,
    params: &Value,
) -> Result<WebhookDelivery, ApiError> {
    if !is_id(Prefix::Webhook, webhook_id) {
        return Err(ApiError::not_found("webhook"));
    }
    let mut conn = state.store.pool().acquire().await?;
    let webhook = read_webhook_row(&mut conn, webhook_id).await?;
    let event_row = sqlx::query(&format!(
        "SELECT {} FROM events ORDER BY seq DESC LIMIT 1",
        dispatcher::EVENT_COLUMNS
    ))
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| {
        ApiError::validation("there is no event to send yet; a test delivery needs one")
    })?;
    let event = dispatcher::event_from_row(&event_row)?;
    drop(conn);

    let delivery_id = new_id(Prefix::WebhookDelivery);
    let created_at = now_rfc3339();
    let mut w = state.store.begin_write().await?;
    // A test delivery is its own row (`test = 1`, attempt 1, never retried), so
    // it neither collides with nor blocks a real delivery of the same event
    // (migration 0004).
    sqlx::query(
        "INSERT INTO webhook_deliveries (id, webhook_id, event_id, attempt, status, response_code, error, next_attempt_at, created_at, test) \
         VALUES (?, ?, ?, 1, 'pending', NULL, NULL, NULL, ?, 1)",
    )
    .bind(&delivery_id)
    .bind(webhook_id)
    .bind(&event.id)
    .bind(&created_at)
    .execute(&mut *w.tx)
    .await?;
    // Deliveries are LOCAL (db-schema.sql): no change-log entry.
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::WebhookWrite),
        Some(target),
        None,
        params,
        None,
    )
    .await?;
    w.commit().await?;

    let secret = state
        .data_key
        .decrypt(SECRET_CONTEXT, &webhook.id, &webhook.secret_enc)
        .map_err(|error| ApiError::internal(&format!("webhook secret: {error}")))?
        .expose()
        .to_string();
    let client = reqwest::Client::builder()
        .timeout(dispatcher::DELIVERY_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| ApiError::internal(&format!("HTTP client: {error}")))?;
    // A test delivery never carries `data.wake` and never issues a grant.
    let outcome: SendOutcome =
        dispatcher::send_once(&client, &webhook, &event, &delivery_id, &secret, None).await;
    let mut w = state.store.begin_write().await?;
    sqlx::query(
        "UPDATE webhook_deliveries SET status = ?, response_code = ?, error = ? WHERE id = ?",
    )
    .bind(if outcome.delivered() {
        DeliveryStatus::Delivered.as_str()
    } else {
        DeliveryStatus::Failed.as_str()
    })
    .bind(outcome.response_code)
    .bind(outcome.error.as_deref())
    .bind(&delivery_id)
    .execute(&mut *w.tx)
    .await?;
    w.commit().await?;
    read_delivery(state, &delivery_id).await
}

/// Lists one webhook's deliveries, newest first.
///
/// # Errors
///
/// `forbidden` without `webhook:read`, `not_found` for an unknown id,
/// `validation_failed` for a bad page, or a database error.
pub async fn list_deliveries(
    state: &AppState,
    ctx: &CallCtx,
    webhook_id: &str,
    page: &WebhookPage,
) -> Result<WebhookDeliveryList, ApiError> {
    let params = json!({ "webhook_id": webhook_id, "limit": page.limit });
    let target = webhook_target(webhook_id);
    let access = ctx
        .authorize_read(state, Scope::WebhookRead, Some(&target), &params)
        .await?;
    let result: Result<WebhookDeliveryList, ApiError> = async {
        if !is_id(Prefix::Webhook, webhook_id) {
            return Err(ApiError::not_found("webhook"));
        }
        let limit = page.limit()?;
        let mut sql =
            format!("SELECT {DELIVERY_COLUMNS} FROM webhook_deliveries WHERE webhook_id = ?");
        let after = page.cursor.clone();
        if after.is_some() {
            sql.push_str(" AND id < ?");
        }
        sql.push_str(" ORDER BY id DESC");
        let mut statement = sqlx::query(&sql).bind(webhook_id);
        if let Some(after) = &after {
            statement = statement.bind(after);
        }
        let rows = statement.fetch_all(state.store.pool()).await?;
        let deliveries = rows
            .iter()
            .map(delivery_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_cursor) =
            crate::api::page::finish(deliveries, limit, |delivery| delivery.id.clone());
        Ok(WebhookDeliveryList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

#[cfg(test)]
mod tests {
    use super::valid_url;

    #[test]
    fn receiver_urls_are_https_or_localhost_only() {
        assert!(valid_url("https://hooks.example.com/quoro"));
        assert!(valid_url("http://localhost:9000/hook"));
        assert!(valid_url("http://localhost"));
        assert!(!valid_url("http://example.com/hook"));
        assert!(!valid_url("http://localhost:80@example.com/"));
        assert!(!valid_url("http://localhost.example.com/"));
        assert!(!valid_url("https://user:pass@example.com/"));
        assert!(!valid_url("ftp://localhost/"));
        assert!(!valid_url("not a url"));
    }
}
