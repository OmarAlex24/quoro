//! Incidents: `/api/v1/incidents*` (D-0030, D-0032).
//!
//! An incident is opened by a detection and scoped to one subject; it is the
//! context an agent receives when an event wakes it. `incident:read` lists and
//! reads them, `incident:resolve` closes one and revokes its incident grants
//! (permission-model.md sections 8.2 and 8.3).

use crate::AppState;
use crate::api::events::{Event, Subject};
use crate::api::grants::Grant;
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::openapi::schema::{AllOfBuilder, ArrayBuilder, ObjectBuilder, Type};
use utoipa::openapi::{Ref, RefOr, Schema};
use utoipa::{IntoParams, PartialSchema, ToSchema};

/// Target of an incident operation.
#[must_use]
pub fn incident_target(incident_id: &str) -> Target {
    Target {
        kind: "incident".into(),
        id: incident_id.to_string(),
    }
}

/// Incident status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    Open,
    Resolved,
}

impl IncidentStatus {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    /// Parses the stored status.
    ///
    /// # Errors
    ///
    /// An internal error for a status the contract does not define.
    pub fn parse(value: &str) -> Result<Self, ApiError> {
        match value {
            "open" => Ok(Self::Open),
            "resolved" => Ok(Self::Resolved),
            other => Err(ApiError::internal(&format!(
                "unknown incident status {other}"
            ))),
        }
    }
}

/// Kind of identity that resolved an incident.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum IncidentResolver {
    User,
    Agent,
    System,
}

impl IncidentResolver {
    /// The wire form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::System => "system",
        }
    }

    /// Parses the stored resolver kind.
    ///
    /// # Errors
    ///
    /// An internal error for a resolver kind the contract does not define.
    pub fn parse(value: &str) -> Result<Self, ApiError> {
        match value {
            "user" => Ok(Self::User),
            "agent" => Ok(Self::Agent),
            "system" => Ok(Self::System),
            other => Err(ApiError::internal(&format!(
                "unknown incident resolver {other}"
            ))),
        }
    }
}

/// An incident, scoped to one subject (`Incident`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Incident")]
pub struct Incident {
    /// Incident id.
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Incident status.
    #[schema(inline)]
    pub status: IncidentStatus,
    /// One-line summary of the incident.
    pub title: String,
    /// Event that opened the incident.
    #[schema(pattern = "^evt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub trigger_event_id: String,
    /// The resource the incident is about.
    #[schema(value_type = Subject)]
    pub subject: Subject,
    /// Project of the subject, when it has one.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// When the incident opened.
    #[schema(format = "date-time")]
    pub opened_at: String,
    /// When the incident was resolved.
    pub resolved_at: Option<String>,
    /// Kind of identity that resolved the incident.
    #[schema(inline)]
    pub resolved_by_type: Option<IncidentResolver>,
    /// Id of the identity that resolved the incident.
    pub resolved_by_id: Option<String>,
    /// What was done to resolve it.
    pub resolution_note: Option<String>,
}

/// One page of incidents (`IncidentList`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "IncidentList")]
pub struct IncidentList {
    pub items: Vec<Incident>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// An incident with the event that opened it and its grants
/// (`IncidentDetail`).
#[derive(Clone, Debug)]
pub struct IncidentDetail {
    pub incident: Incident,
    pub trigger_event: Event,
    pub grants: Vec<Grant>,
}

impl Serialize for IncidentDetail {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.incident.id)?;
        map.serialize_entry("status", &self.incident.status)?;
        map.serialize_entry("title", &self.incident.title)?;
        map.serialize_entry("trigger_event_id", &self.incident.trigger_event_id)?;
        map.serialize_entry("subject", &self.incident.subject)?;
        map.serialize_entry("project_id", &self.incident.project_id)?;
        map.serialize_entry("opened_at", &self.incident.opened_at)?;
        map.serialize_entry("resolved_at", &self.incident.resolved_at)?;
        map.serialize_entry("resolved_by_type", &self.incident.resolved_by_type)?;
        map.serialize_entry("resolved_by_id", &self.incident.resolved_by_id)?;
        map.serialize_entry("resolution_note", &self.incident.resolution_note)?;
        map.serialize_entry("trigger_event", &self.trigger_event)?;
        map.serialize_entry("grants", &self.grants)?;
        map.end()
    }
}

impl PartialSchema for IncidentDetail {
    fn schema() -> RefOr<Schema> {
        let extra = ObjectBuilder::new()
            .schema_type(Type::Object)
            .property("trigger_event", Ref::from_schema_name("Event"))
            .property(
                "grants",
                ArrayBuilder::new().items(Ref::from_schema_name("Grant")),
            )
            .required("trigger_event")
            .required("grants");
        AllOfBuilder::new()
            .schema_type(Type::Object)
            .item(Ref::from_schema_name("Incident"))
            .item(extra)
            .into()
    }
}

impl ToSchema for IncidentDetail {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("IncidentDetail")
    }
}

/// Close an incident and record what was done (`ResolveIncidentRequest`).
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[schema(title = "ResolveIncidentRequest")]
pub struct ResolveIncidentRequest {
    /// What was done to resolve it. Recorded in the audit log.
    #[schema(max_length = 2000, nullable = false)]
    pub resolution_note: Option<String>,
}

/// `status`, `limit` and `cursor` of `listIncidents`.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListIncidentsQuery {
    /// Only incidents in this status.
    #[param(inline)]
    pub status: Option<IncidentStatus>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListIncidentsQuery {
    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(crate::api::page::DEFAULT_LIMIT),
            Some(n) if (1..=crate::api::page::MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{}",
                crate::api::page::MAX_LIMIT
            ))),
        }
    }

    /// The cursor, or the empty string, which sorts after every id.
    #[must_use]
    pub fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }

    /// The `status` filter as the stored string.
    #[must_use]
    pub fn status(&self) -> Option<&'static str> {
        self.status.map(IncidentStatus::as_str)
    }
}

/// Incidents opened by a detection, newest first.
///
/// # Errors
///
/// `forbidden` without `incident:read`, `validation_failed` for a bad filter,
/// or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/incidents",
    tag = "incidents",
    operation_id = "listIncidents",
    extensions(
        ("x-quoro-scope" = json!("incident:read")),
        ("x-quoro-mcp-tool" = json!("list_incidents")),
    ),
    params(ListIncidentsQuery),
    responses(
        (status = 200, description = "One page of incidents.", body = IncidentList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_incidents(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(query): Query<ListIncidentsQuery>,
) -> Result<Json<IncidentList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/incidents", &headers);
    let list = crate::service::incidents::list_incidents(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// One incident with its trigger event and grants.
///
/// # Errors
///
/// `forbidden` without `incident:read`, `not_found` for an unknown or
/// malformed id, or a database error.
#[utoipa::path(
    get,
    path = "/api/v1/incidents/{incident_id}",
    tag = "incidents",
    operation_id = "getIncident",
    extensions(
        ("x-quoro-scope" = json!("incident:read")),
        ("x-quoro-mcp-tool" = json!("get_incident")),
    ),
    params(
        ("incident_id" = String, Path, description = "Incident id (`inc_...`).", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The incident with its trigger event and grants.", body = IncidentDetail,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn get_incident(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(incident_id): Path<String>,
) -> Result<Json<IncidentDetail>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/incidents/{incident_id}",
        &headers,
    );
    let detail = crate::service::incidents::get_incident(&state, &ctx, &incident_id).await?;
    Ok(Json(detail))
}

/// Resolves an incident: records who did it and why, emits
/// `incident.resolved` and revokes the incident grants.
///
/// # Errors
///
/// `forbidden` without `incident:resolve`, `not_found` for an unknown id,
/// `conflict` when it is already resolved, or a database error.
#[utoipa::path(
    post,
    path = "/api/v1/incidents/{incident_id}/resolve",
    tag = "incidents",
    operation_id = "resolveIncident",
    extensions(
        ("x-quoro-scope" = json!("incident:resolve")),
        ("x-quoro-mcp-tool" = json!("resolve_incident")),
    ),
    params(
        ("incident_id" = String, Path, description = "Incident id (`inc_...`).", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Quoro-Justification" = Option<String>, Header, description = "The agent's stated reason for the call.", min_length = 1, max_length = 2000, nullable = false),
        ("Quoro-Incident" = Option<String>, Header, description = "The incident the call belongs to.", pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$", nullable = false),
    ),
    request_body = ResolveIncidentRequest,
    responses(
        (status = 200, description = "The resolved incident.", body = Incident,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 409, response = crate::openapi::Conflict),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 503, response = crate::openapi::NotPrimary),
    )
)]
pub async fn resolve_incident(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(incident_id): Path<String>,
    body: Option<ApiJson<ResolveIncidentRequest>>,
) -> Result<Json<Incident>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "POST /api/v1/incidents/{incident_id}/resolve",
        &headers,
    );
    let note = body.and_then(|ApiJson(body)| body.resolution_note);
    let incident =
        crate::service::incidents::resolve_incident(&state, &ctx, &incident_id, note).await?;
    Ok(Json(incident))
}

/// The `Incident` component of the generated document.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(list_incidents, get_incident, resolve_incident),
    components(schemas(
        Incident,
        IncidentList,
        IncidentDetail,
        IncidentStatus,
        IncidentResolver,
        ResolveIncidentRequest,
    ))
)]
pub struct IncidentsDoc;
