//! Webhooks: `/api/v1/webhooks*` (D-0032).
//!
//! A webhook subscribes a receiver to events. Delivering an `incident.opened`
//! event to a webhook with a `wake` object also issues the incident grant the
//! woken agent acts under ([`crate::dispatcher`], permission-model.md section
//! 8). Managing webhooks is human-only (`webhook:write`), so create, update,
//! delete and test authorize through [`CallCtx::authorize_human`]; reading one
//! uses [`CallCtx::authorize_read`].

use crate::AppState;
use crate::api::auth::{ScopeSchema, Selector as SelectorSchema};
use crate::authz;
use crate::error::ApiError;
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use quoro_types::Scope;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Operation strings recorded in the audit log and used as idempotency
/// scopes.
pub const CREATE_OPERATION: &str = "POST /api/v1/webhooks";
pub const LIST_OPERATION: &str = "GET /api/v1/webhooks";
pub const GET_OPERATION: &str = "GET /api/v1/webhooks/{webhook_id}";
pub const UPDATE_OPERATION: &str = "PATCH /api/v1/webhooks/{webhook_id}";
pub const DELETE_OPERATION: &str = "DELETE /api/v1/webhooks/{webhook_id}";
pub const TEST_OPERATION: &str = "POST /api/v1/webhooks/{webhook_id}/test";
pub const DELIVERIES_OPERATION: &str = "GET /api/v1/webhooks/{webhook_id}/deliveries";

// ---------------------------------------------------------------------------
// Schemas (components of the frozen `openapi.yaml`)
// ---------------------------------------------------------------------------

/// Event type, `<subject>.<what>`. The list is closed by the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "EventType",
    description = "Event type, `<subject>.<what>` in lowercase with underscores."
)]
pub enum EventType {
    #[serde(rename = "node.enrolled")]
    NodeEnrolled,
    #[serde(rename = "node.connected")]
    NodeConnected,
    #[serde(rename = "node.disconnected")]
    NodeDisconnected,
    #[serde(rename = "node.fenced")]
    NodeFenced,
    #[serde(rename = "node.fence_failed")]
    NodeFenceFailed,
    #[serde(rename = "node.disk_low")]
    NodeDiskLow,
    #[serde(rename = "vm.created")]
    VmCreated,
    #[serde(rename = "vm.started")]
    VmStarted,
    #[serde(rename = "vm.stopped")]
    VmStopped,
    #[serde(rename = "vm.crashed")]
    VmCrashed,
    #[serde(rename = "vm.deleted")]
    VmDeleted,
    #[serde(rename = "app.created")]
    AppCreated,
    #[serde(rename = "app.updated")]
    AppUpdated,
    #[serde(rename = "app.deleted")]
    AppDeleted,
    #[serde(rename = "app.down")]
    AppDown,
    #[serde(rename = "app.up")]
    AppUp,
    #[serde(rename = "app.restarted")]
    AppRestarted,
    #[serde(rename = "deployment.started")]
    DeploymentStarted,
    #[serde(rename = "deployment.succeeded")]
    DeploymentSucceeded,
    #[serde(rename = "deployment.failed")]
    DeploymentFailed,
    #[serde(rename = "build.started")]
    BuildStarted,
    #[serde(rename = "build.succeeded")]
    BuildSucceeded,
    #[serde(rename = "build.failed")]
    BuildFailed,
    #[serde(rename = "build.cancelled")]
    BuildCancelled,
    #[serde(rename = "backup.succeeded")]
    BackupSucceeded,
    #[serde(rename = "backup.failed")]
    BackupFailed,
    #[serde(rename = "restore.succeeded")]
    RestoreSucceeded,
    #[serde(rename = "restore.failed")]
    RestoreFailed,
    #[serde(rename = "certificate.issued")]
    CertificateIssued,
    #[serde(rename = "certificate.expiring")]
    CertificateExpiring,
    #[serde(rename = "certificate.failed")]
    CertificateFailed,
    #[serde(rename = "grant.created")]
    GrantCreated,
    #[serde(rename = "grant.revoked")]
    GrantRevoked,
    #[serde(rename = "grant.expired")]
    GrantExpired,
    #[serde(rename = "approval.requested")]
    ApprovalRequested,
    #[serde(rename = "approval.approved")]
    ApprovalApproved,
    #[serde(rename = "approval.denied")]
    ApprovalDenied,
    #[serde(rename = "approval.expired")]
    ApprovalExpired,
    #[serde(rename = "incident.opened")]
    IncidentOpened,
    #[serde(rename = "incident.resolved")]
    IncidentResolved,
    #[serde(rename = "audit.chain_broken")]
    AuditChainBroken,
}

impl EventType {
    /// The wire form stored in `webhooks.event_types`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeEnrolled => "node.enrolled",
            Self::NodeConnected => "node.connected",
            Self::NodeDisconnected => "node.disconnected",
            Self::NodeFenced => "node.fenced",
            Self::NodeFenceFailed => "node.fence_failed",
            Self::NodeDiskLow => "node.disk_low",
            Self::VmCreated => "vm.created",
            Self::VmStarted => "vm.started",
            Self::VmStopped => "vm.stopped",
            Self::VmCrashed => "vm.crashed",
            Self::VmDeleted => "vm.deleted",
            Self::AppCreated => "app.created",
            Self::AppUpdated => "app.updated",
            Self::AppDeleted => "app.deleted",
            Self::AppDown => "app.down",
            Self::AppUp => "app.up",
            Self::AppRestarted => "app.restarted",
            Self::DeploymentStarted => "deployment.started",
            Self::DeploymentSucceeded => "deployment.succeeded",
            Self::DeploymentFailed => "deployment.failed",
            Self::BuildStarted => "build.started",
            Self::BuildSucceeded => "build.succeeded",
            Self::BuildFailed => "build.failed",
            Self::BuildCancelled => "build.cancelled",
            Self::BackupSucceeded => "backup.succeeded",
            Self::BackupFailed => "backup.failed",
            Self::RestoreSucceeded => "restore.succeeded",
            Self::RestoreFailed => "restore.failed",
            Self::CertificateIssued => "certificate.issued",
            Self::CertificateExpiring => "certificate.expiring",
            Self::CertificateFailed => "certificate.failed",
            Self::GrantCreated => "grant.created",
            Self::GrantRevoked => "grant.revoked",
            Self::GrantExpired => "grant.expired",
            Self::ApprovalRequested => "approval.requested",
            Self::ApprovalApproved => "approval.approved",
            Self::ApprovalDenied => "approval.denied",
            Self::ApprovalExpired => "approval.expired",
            Self::IncidentOpened => "incident.opened",
            Self::IncidentResolved => "incident.resolved",
            Self::AuditChainBroken => "audit.chain_broken",
        }
    }

    /// Parses a stored event type; `None` is not a type of the contract.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|event_type| event_type.as_str() == value)
    }

    /// Every event type of the contract, in contract order.
    pub const ALL: [Self; 41] = [
        Self::NodeEnrolled,
        Self::NodeConnected,
        Self::NodeDisconnected,
        Self::NodeFenced,
        Self::NodeFenceFailed,
        Self::NodeDiskLow,
        Self::VmCreated,
        Self::VmStarted,
        Self::VmStopped,
        Self::VmCrashed,
        Self::VmDeleted,
        Self::AppCreated,
        Self::AppUpdated,
        Self::AppDeleted,
        Self::AppDown,
        Self::AppUp,
        Self::AppRestarted,
        Self::DeploymentStarted,
        Self::DeploymentSucceeded,
        Self::DeploymentFailed,
        Self::BuildStarted,
        Self::BuildSucceeded,
        Self::BuildFailed,
        Self::BuildCancelled,
        Self::BackupSucceeded,
        Self::BackupFailed,
        Self::RestoreSucceeded,
        Self::RestoreFailed,
        Self::CertificateIssued,
        Self::CertificateExpiring,
        Self::CertificateFailed,
        Self::GrantCreated,
        Self::GrantRevoked,
        Self::GrantExpired,
        Self::ApprovalRequested,
        Self::ApprovalApproved,
        Self::ApprovalDenied,
        Self::ApprovalExpired,
        Self::IncidentOpened,
        Self::IncidentResolved,
        Self::AuditChainBroken,
    ];
}

/// Default lifetime of an incident grant, in seconds.
#[must_use]
pub const fn default_grant_ttl_s() -> i64 {
    3600
}

const fn default_enabled() -> bool {
    true
}

/// Present when the webhook wakes an agent: the delivery starts the agent with
/// an incident grant over the non-destructive scopes listed here.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "WebhookWake")]
pub struct WebhookWake {
    /// Agent to wake.
    #[schema(pattern = "^agt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub agent_id: String,
    /// Non-destructive scopes the incident grant carries.
    #[serde(default)]
    #[schema(value_type = Vec<ScopeSchema>)]
    pub grant_scopes: Vec<Scope>,
    /// TTL of the incident grant in seconds.
    #[serde(default = "default_grant_ttl_s")]
    #[schema(minimum = 1, default = 3600)]
    pub grant_ttl_s: i64,
    /// Optional maximum number of uses of the incident grant.
    #[serde(default)]
    #[schema(minimum = 1, nullable = true)]
    pub grant_max_uses: Option<i64>,
}

/// A subscription that delivers events to an external receiver.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Webhook")]
pub struct Webhook {
    /// Webhook id.
    #[schema(pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Label of the subscription.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Receiver URL. HTTPS only, except `http://localhost` for testing.
    #[schema(format = "uri")]
    pub url: String,
    /// Event types that fire this webhook; empty means all.
    pub event_types: Vec<EventType>,
    /// Where the event subject must be for the webhook to fire.
    #[schema(value_type = SelectorSchema)]
    pub selector: authz::Selector,
    /// Whether deliveries are attempted.
    pub enabled: bool,
    /// Present when the webhook wakes an agent.
    #[schema(nullable = false)]
    pub wake: Option<WebhookWake>,
    /// The signing secret. Write-only: present only in `WebhookCreated`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(write_only, nullable = false)]
    pub secret: Option<String>,
    /// User who created it; also the `on_behalf_of` of the grants it wakes.
    #[schema(pattern = "^usr_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub created_by: String,
    /// When the webhook was created.
    #[schema(format = "date-time")]
    pub created_at: String,
    /// When the webhook record last changed.
    #[schema(format = "date-time")]
    pub updated_at: String,
}

/// One page of webhooks.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "WebhookList")]
pub struct WebhookList {
    pub items: Vec<Webhook>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// The new webhook with its signing secret. The secret is shown once and
/// cannot be retrieved again.
#[derive(Clone, Debug, Serialize)]
pub struct WebhookCreated {
    /// The webhook fields.
    #[serde(flatten)]
    pub webhook: Webhook,
    /// The signing secret. Shown only in this response.
    pub secret: String,
}

impl utoipa::PartialSchema for WebhookCreated {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::Ref;
        use utoipa::openapi::schema::{AllOfBuilder, ObjectBuilder, Type};
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .title(Some("WebhookCreated".to_string()))
            .item(Ref::from_schema_name("Webhook"))
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Object)
                    .property(
                        "secret",
                        ObjectBuilder::new()
                            .schema_type(Type::String)
                            .read_only(Some(true)),
                    )
                    .required("secret"),
            )
            .into()
    }
}

impl ToSchema for WebhookCreated {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("WebhookCreated")
    }
}

/// Subscribe a receiver to events.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[schema(title = "CreateWebhookRequest")]
pub struct CreateWebhookRequest {
    /// Label of the subscription.
    #[schema(min_length = 1, max_length = 200)]
    pub name: String,
    /// Receiver URL; HTTPS only, except `http://localhost`.
    #[schema(format = "uri")]
    pub url: String,
    /// Event types that fire the webhook; empty means all.
    #[serde(default)]
    pub event_types: Vec<EventType>,
    /// Where the event subject must be for the webhook to fire.
    #[serde(default)]
    #[schema(value_type = SelectorSchema)]
    pub selector: authz::Selector,
    /// Whether deliveries are attempted.
    #[serde(default = "default_enabled")]
    #[schema(default = true)]
    pub enabled: bool,
    /// Present when the webhook wakes an agent.
    #[serde(default)]
    #[schema(nullable = false)]
    pub wake: Option<WebhookWake>,
}

/// Change a webhook. Absent fields stay unchanged. The signing secret cannot
/// be changed: delete and recreate the webhook to rotate it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "UpdateWebhookRequest")]
pub struct UpdateWebhookRequest {
    /// New label.
    #[serde(default)]
    #[schema(min_length = 1, max_length = 200, nullable = false)]
    pub name: Option<String>,
    /// New receiver URL.
    #[serde(default)]
    #[schema(format = "uri", nullable = false)]
    pub url: Option<String>,
    /// New event types; empty means all.
    #[serde(default)]
    #[schema(nullable = false)]
    pub event_types: Option<Vec<EventType>>,
    /// New selector.
    #[serde(default)]
    #[schema(value_type = SelectorSchema)]
    pub selector: Option<authz::Selector>,
    /// Whether deliveries are attempted.
    #[serde(default)]
    #[schema(nullable = false)]
    pub enabled: Option<bool>,
    /// New wake configuration.
    #[serde(default)]
    #[schema(nullable = false)]
    pub wake: Option<WebhookWake>,
}

/// One delivery attempt of an event to a webhook.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "WebhookDelivery")]
pub struct WebhookDelivery {
    /// Delivery id; also sent as `Quoro-Delivery-Id`.
    #[schema(pattern = "^whd_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Webhook the delivery belongs to.
    #[schema(pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub webhook_id: String,
    /// Event that was delivered.
    #[schema(pattern = "^evt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub event_id: String,
    /// Attempt number, starting at 1.
    #[schema(minimum = 1)]
    pub attempt: i64,
    /// Delivery status.
    #[schema(inline)]
    pub status: DeliveryStatus,
    /// HTTP status the receiver answered, when it answered.
    #[schema(minimum = 100, maximum = 599, nullable = true)]
    pub response_code: Option<i64>,
    /// Transport or receiver error.
    #[schema(nullable = true)]
    pub error: Option<String>,
    /// When the next attempt is due.
    #[schema(nullable = true)]
    pub next_attempt_at: Option<String>,
    /// True for a test delivery sent by `testWebhook`, false for every real
    /// delivery. A test delivery is always attempt 1 with no next attempt.
    pub test: bool,
    /// When the attempt was made.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// Delivery status (`WebhookDelivery.status`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryStatus {
    Pending,
    Delivered,
    Failed,
}

impl DeliveryStatus {
    /// The wire form stored in `webhook_deliveries.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }
}

/// One page of deliveries, newest first.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "WebhookDeliveryList")]
pub struct WebhookDeliveryList {
    pub items: Vec<WebhookDelivery>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// `limit` and `cursor` (`openapi.yaml` `Limit` and `Cursor`).
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WebhookPage {
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub(crate) limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub(crate) cursor: Option<String>,
}

impl WebhookPage {
    pub(crate) fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    pub(crate) fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// List webhooks
#[utoipa::path(
    get,
    path = "/api/v1/webhooks",
    tag = "webhooks",
    operation_id = "listWebhooks",
    extensions(("x-quoro-scope" = json!("webhook:read"))),
    params(WebhookPage),
    responses(
        (status = 200, description = "One page of webhooks.", body = WebhookList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `webhook:read`, `validation_failed` for a bad page, or
/// a database error.
pub async fn list_webhooks(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<WebhookPage>,
) -> Result<Json<WebhookList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, LIST_OPERATION, &headers);
    let list = crate::service::webhooks::list_webhooks(&state, &ctx, &page).await?;
    Ok(Json(list))
}

/// Create a webhook
#[utoipa::path(
    post,
    path = "/api/v1/webhooks",
    tag = "webhooks",
    operation_id = "createWebhook",
    extensions(("x-quoro-scope" = json!("webhook:write"))),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Client-chosen key for a create.", min_length = 1, max_length = 128, nullable = false),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = CreateWebhookRequest,
    responses(
        (status = 201, description = "The webhook was created; the secret is in this response only.", body = WebhookCreated,
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
/// `validation_failed` for a bad URL, scope or wake configuration,
/// `forbidden` without `webhook:write` (agents get `human_only`), `conflict`
/// for a reused idempotency key, or a database error.
pub async fn create_webhook(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiJson(body): ApiJson<CreateWebhookRequest>,
) -> Result<(StatusCode, Json<WebhookCreated>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, CREATE_OPERATION, &headers);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let created = crate::service::webhooks::create_webhook(&state, &ctx, &body, key).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

/// Read one webhook
#[utoipa::path(
    get,
    path = "/api/v1/webhooks/{webhook_id}",
    tag = "webhooks",
    operation_id = "getWebhook",
    extensions(("x-quoro-scope" = json!("webhook:read"))),
    params(
        ("webhook_id" = String, Path, description = "Webhook id (`whk_...`).", pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The webhook.", body = Webhook,
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
/// `forbidden` without `webhook:read`, `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn get_webhook(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(webhook_id): Path<String>,
) -> Result<Json<Webhook>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, GET_OPERATION, &headers);
    let webhook = crate::service::webhooks::get_webhook(&state, &ctx, &webhook_id).await?;
    Ok(Json(webhook))
}

/// Change a webhook
#[utoipa::path(
    patch,
    path = "/api/v1/webhooks/{webhook_id}",
    tag = "webhooks",
    operation_id = "updateWebhook",
    extensions(("x-quoro-scope" = json!("webhook:write"))),
    params(
        ("webhook_id" = String, Path, description = "Webhook id (`whk_...`).", pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    request_body = UpdateWebhookRequest,
    responses(
        (status = 200, description = "The updated webhook.", body = Webhook,
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
/// `validation_failed` for a bad URL, scope or wake configuration,
/// `forbidden` without `webhook:write` (agents get `human_only`), `not_found`
/// for an unknown id, or a database error.
pub async fn update_webhook(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(webhook_id): Path<String>,
    ApiJson(body): ApiJson<UpdateWebhookRequest>,
) -> Result<Json<Webhook>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, UPDATE_OPERATION, &headers);
    let webhook =
        crate::service::webhooks::update_webhook(&state, &ctx, &webhook_id, &body).await?;
    Ok(Json(webhook))
}

/// Delete a webhook
#[utoipa::path(
    delete,
    path = "/api/v1/webhooks/{webhook_id}",
    tag = "webhooks",
    operation_id = "deleteWebhook",
    extensions(("x-quoro-scope" = json!("webhook:write"))),
    params(
        ("webhook_id" = String, Path, description = "Webhook id (`whk_...`).", pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    responses(
        (status = 204, description = "The webhook was deleted.",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
///
/// # Errors
///
/// `forbidden` without `webhook:write` (agents get `human_only`), `not_found`
/// for an unknown id, or a database error.
pub async fn delete_webhook(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(webhook_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ctx = CallCtx::new(identity, request_id, DELETE_OPERATION, &headers);
    crate::service::webhooks::delete_webhook(&state, &ctx, &webhook_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Send a signed test delivery
#[utoipa::path(
    post,
    path = "/api/v1/webhooks/{webhook_id}/test",
    tag = "webhooks",
    operation_id = "testWebhook",
    extensions(("x-quoro-scope" = json!("webhook:write"))),
    params(
        ("webhook_id" = String, Path, description = "Webhook id (`whk_...`).", pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
    ),
    responses(
        (status = 202, description = "The test delivery was attempted and recorded.", body = WebhookDelivery,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 422, response = crate::openapi::ValidationFailed),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `webhook:write` (agents get `human_only`), `not_found`
/// for an unknown id, `validation_failed` when the event log is still empty,
/// or a database error.
pub async fn test_webhook(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(webhook_id): Path<String>,
) -> Result<(StatusCode, Json<WebhookDelivery>), ApiError> {
    let ctx = CallCtx::new(identity, request_id, TEST_OPERATION, &headers);
    let delivery = crate::service::webhooks::test_webhook(&state, &ctx, &webhook_id).await?;
    Ok((StatusCode::ACCEPTED, Json(delivery)))
}

/// List a webhook's deliveries
#[utoipa::path(
    get,
    path = "/api/v1/webhooks/{webhook_id}/deliveries",
    tag = "webhooks",
    operation_id = "listWebhookDeliveries",
    extensions(("x-quoro-scope" = json!("webhook:read"))),
    params(
        ("webhook_id" = String, Path, description = "Webhook id (`whk_...`).", pattern = "^whk_[0-9A-HJKMNP-TV-Z]{26}$"),
        WebhookPage,
    ),
    responses(
        (status = 200, description = "One page of deliveries.", body = WebhookDeliveryList,
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
/// `forbidden` without `webhook:read`, `not_found` for an unknown id,
/// `validation_failed` for a bad page, or a database error.
pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(webhook_id): Path<String>,
    Query(page): Query<WebhookPage>,
) -> Result<Json<WebhookDeliveryList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, DELIVERIES_OPERATION, &headers);
    let list = crate::service::webhooks::list_deliveries(&state, &ctx, &webhook_id, &page).await?;
    Ok(Json(list))
}
