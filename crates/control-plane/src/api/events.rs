//! Events: `/api/v1/events` and the SSE stream `/api/v1/events/stream`
//! (D-0032, D-0057).
//!
//! The event log is the notify step of the AI-native loop. `GET /api/v1/events`
//! reads it in ascending `seq` order and the stream delivers new events with
//! `id:` set to the `seq`, so a client resumes with `Last-Event-ID` without
//! losing anything. Agents read under `event:read`; an agent's results are
//! filtered by its grant selectors (permission-model.md section 5.1), never
//! refused.

use crate::AppState;
use crate::error::ApiError;
use crate::request::{CallCtx, ReadFilter, RequestId, Resource};
use crate::service::events::{self, EventFilters};
use axum::Json;
use axum::extract::{Extension, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::stream::{self, Stream};
use quoro_types::Scope;
use serde::Serialize;
use serde_json::{Value, json};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;
use utoipa::openapi::schema::{ObjectBuilder, Type};
use utoipa::openapi::{RefOr, Schema};
use utoipa::{IntoParams, PartialSchema, ToSchema};

/// How often the stream polls the event log for new rows.
const POLL: Duration = Duration::from_secs(1);

/// How often a quiet stream sends a keep-alive comment.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// The closed set of event types (`events.schema.json`, `EventType`).
pub const EVENT_TYPES: [&str; 49] = [
    "node.enrolled",
    "node.connected",
    "node.disconnected",
    "node.fenced",
    "node.fence_failed",
    "node.disk_low",
    "vm.created",
    "vm.started",
    "vm.stopped",
    "vm.crashed",
    "vm.deleted",
    "app.created",
    "app.updated",
    "app.deleted",
    "app.down",
    "app.degraded",
    "app.up",
    "app.restarted",
    "app.vulnerable",
    "app.failed_over",
    "app.switched_over",
    "app.switchover_failed",
    "deployment.started",
    "deployment.succeeded",
    "deployment.failed",
    "build.started",
    "build.succeeded",
    "build.failed",
    "build.cancelled",
    "backup.succeeded",
    "backup.failed",
    "restore.succeeded",
    "restore.failed",
    "volume.quarantined",
    "certificate.issued",
    "certificate.expiring",
    "certificate.failed",
    "domain.unserved",
    "domain.served",
    "grant.created",
    "grant.revoked",
    "grant.expired",
    "approval.requested",
    "approval.approved",
    "approval.denied",
    "approval.expired",
    "incident.opened",
    "incident.resolved",
    "audit.chain_broken",
];

/// The `EventType` component: one of the closed event types.
pub struct EventType;

impl PartialSchema for EventType {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .title(Some("EventType"))
            .description(Some(
                "Event type, `<subject>.<what>` in lowercase with underscores. The list is closed.",
            ))
            .enum_values(Some(EVENT_TYPES.iter().copied()))
            .into()
    }
}

impl ToSchema for EventType {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("EventType")
    }
}

impl EventType {
    /// Whether `value` is one of the closed set of event types.
    #[must_use]
    pub fn is_known(value: &str) -> bool {
        EVENT_TYPES.contains(&value)
    }
}

/// What produced the event (`EventSource.kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventSourceKind {
    ControlPlane,
    Node,
}

/// What produced the event (`EventSource`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "EventSource")]
pub struct EventSource {
    /// Whether the control plane or a node produced it.
    #[schema(inline)]
    pub kind: EventSourceKind,
    /// Node that produced the event, when `kind` is `node`.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: Option<String>,
}

/// Kind of identity behind the action (`EventActor.type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventActorKind {
    User,
    Agent,
    Node,
    System,
}

/// The identity behind the action the event reports, when there is one.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "EventActor")]
pub struct EventActor {
    /// Kind of identity.
    #[serde(rename = "type")]
    #[schema(inline)]
    pub kind: EventActorKind,
    /// Id of the identity.
    pub id: String,
}

/// The resource an event is about (`Subject`).
#[derive(Clone, Debug, Serialize)]
pub struct Subject {
    /// Resource kind, in the same words the id prefixes use.
    #[serde(rename = "type")]
    pub kind: String,
    /// Id of that resource.
    pub id: String,
}

impl PartialSchema for Subject {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Object)
            .title(Some("Subject"))
            .description(Some(
                "The resource the event is about: its kind and id in the same words the id prefixes use.",
            ))
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .description(Some(
                        "Resource kind, for example `app`, `vm`, `node` or `cluster`.",
                    )),
            )
            .property(
                "id",
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .description(Some("Id of the resource.")),
            )
            .required("type")
            .required("id")
            .into()
    }
}

impl ToSchema for Subject {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("Subject")
    }
}

/// One event in the append-only event log (`Event`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Event")]
pub struct Event {
    /// Event id.
    #[schema(pattern = "^evt_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Gap-free sequence number of the event log.
    #[schema(minimum = 1)]
    pub seq: i64,
    /// Event type.
    #[serde(rename = "type")]
    #[schema(value_type = EventType)]
    pub kind: String,
    /// When the event was recorded.
    #[schema(format = "date-time")]
    pub time: String,
    /// What produced the event.
    pub source: EventSource,
    /// The resource the event is about.
    #[schema(value_type = Subject)]
    pub subject: Subject,
    /// Project of the subject, when it has one.
    #[schema(pattern = "^prj_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub project_id: Option<String>,
    /// Incident the event belongs to, if any.
    #[schema(pattern = "^inc_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub incident_id: Option<String>,
    /// API or MCP call that caused the event.
    #[schema(pattern = "^req_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub request_id: Option<String>,
    /// The identity behind the action, when there is one.
    #[schema(nullable = false)]
    pub actor: Option<EventActor>,
    /// Type-specific payload, defined per event type in `events.schema.json`.
    #[schema(value_type = Object)]
    pub data: Value,
}

/// One page of events in ascending `seq` order (`EventList`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "EventList")]
pub struct EventList {
    pub items: Vec<Event>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
}

/// Query of `listEvents`: `type` (repeatable), `subject_id`, `after_seq`,
/// `limit` and `cursor`.
#[derive(Debug, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListEventsQuery {
    /// Only events of this type. Repeat the parameter to select several.
    #[param(rename = "type", value_type = Vec<EventType>, required = false)]
    pub types: Option<Vec<String>>,
    /// Only events about this resource id.
    pub subject_id: Option<String>,
    /// Return only entries whose `seq` is greater than this value.
    #[param(minimum = 0)]
    pub after_seq: Option<i64>,
    /// Maximum number of items to return (default 50, maximum 500).
    #[param(minimum = 1, maximum = 500, default = 50)]
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl ListEventsQuery {
    /// Reads the query from the raw request pairs, so `type` may repeat.
    ///
    /// # Errors
    ///
    /// `validation_failed` for an unknown event type or a malformed cursor.
    pub fn parse(pairs: &[(String, String)]) -> Result<Self, ApiError> {
        let mut query = Self {
            types: None,
            subject_id: None,
            after_seq: None,
            limit: None,
            cursor: None,
        };
        for (key, value) in pairs {
            match key.as_str() {
                "type" => {
                    if !EventType::is_known(value) {
                        return Err(ApiError::validation(format!("unknown event type {value}")));
                    }
                    query.types.get_or_insert_with(Vec::new).push(value.clone());
                }
                "subject_id" => query.subject_id = Some(value.clone()),
                "after_seq" => {
                    let seq: i64 = value
                        .parse()
                        .map_err(|_| ApiError::validation("after_seq is not a sequence number"))?;
                    if seq < 0 {
                        return Err(ApiError::validation("after_seq must be at least 0"));
                    }
                    query.after_seq = Some(seq);
                }
                "limit" => {
                    query.limit = Some(
                        value
                            .parse()
                            .map_err(|_| ApiError::validation("limit is not a number"))?,
                    );
                }
                "cursor" => query.cursor = Some(value.clone()),
                _ => {}
            }
        }
        Ok(query)
    }

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

    /// The filters both the page and its cursor apply.
    #[must_use]
    pub fn filters(&self) -> EventFilters {
        EventFilters {
            types: self.types.clone().unwrap_or_default(),
            subject_id: self.subject_id.clone(),
        }
    }

    /// The first `seq` the page returns: the larger of `after_seq` and the
    /// cursor, which is the last `seq` of the previous page.
    ///
    /// # Errors
    ///
    /// `validation_failed` for a cursor that is not a sequence number.
    pub fn after(&self) -> Result<i64, ApiError> {
        let mut after = self.after_seq.unwrap_or(0);
        if let Some(cursor) = &self.cursor {
            let from_cursor: i64 = cursor
                .parse()
                .map_err(|_| ApiError::validation("cursor is not a sequence number"))?;
            after = after.max(from_cursor);
        }
        Ok(after)
    }
}

/// Query of `streamEvents`: the `type` filter and `subject_id`.
#[derive(Debug, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct StreamEventsQuery {
    /// Only stream events of this type. Repeat to select several.
    #[param(rename = "type", value_type = Vec<EventType>, required = false)]
    pub types: Option<Vec<String>>,
    /// Only stream events about this resource id.
    pub subject_id: Option<String>,
}

impl StreamEventsQuery {
    /// Reads the query from the raw request pairs, so `type` may repeat.
    ///
    /// # Errors
    ///
    /// `validation_failed` for an unknown event type.
    pub fn parse(pairs: &[(String, String)]) -> Result<Self, ApiError> {
        let mut query = Self {
            types: None,
            subject_id: None,
        };
        for (key, value) in pairs {
            match key.as_str() {
                "type" => {
                    if !EventType::is_known(value) {
                        return Err(ApiError::validation(format!("unknown event type {value}")));
                    }
                    query.types.get_or_insert_with(Vec::new).push(value.clone());
                }
                "subject_id" => query.subject_id = Some(value.clone()),
                _ => {}
            }
        }
        Ok(query)
    }

    /// The filters the stream applies to every poll.
    #[must_use]
    pub fn filters(&self) -> EventFilters {
        EventFilters {
            types: self.types.clone().unwrap_or_default(),
            subject_id: self.subject_id.clone(),
        }
    }
}

/// `GET /api/v1/events`.
///
/// # Errors
///
/// `forbidden` without `event:read`, `validation_failed` for an unknown type,
/// a negative `after_seq`, a bad `limit` or a malformed cursor, or a database
/// error.
#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    operation_id = "listEvents",
    extensions(
        ("x-quoro-scope" = json!("event:read")),
        ("x-quoro-mcp-tool" = json!("list_events")),
    ),
    params(ListEventsQuery),
    responses(
        (status = 200, description = "One page of events.", body = EventList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn list_events(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<EventList>, ApiError> {
    let query = ListEventsQuery::parse(&pairs)?;
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/events", &headers);
    let list = events::list_events(&state, &ctx, &query).await?;
    Ok(Json(list))
}

/// `GET /api/v1/events/stream`.
///
/// # Errors
///
/// `forbidden` without `event:read`, `validation_failed` for an unknown type,
/// or a database error while the stream starts.
#[utoipa::path(
    get,
    path = "/api/v1/events/stream",
    tag = "events",
    operation_id = "streamEvents",
    extensions(("x-quoro-scope" = json!("event:read"))),
    params(
        StreamEventsQuery,
        ("Last-Event-ID" = Option<String>, Header, description = "Last event `seq` the client received; the stream resumes after it.", nullable = false),
    ),
    responses(
        (status = 200, description = "The stream started; events follow until the client disconnects.",
            content_type = "text/event-stream", body = String,
            headers(
                ("Quoro-Request-Id" = String, description = "Correlation id of this call."),
                ("Cache-Control" = String, description = "Always `no-cache`; the stream must not be cached."),
            )),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
pub async fn stream_events(
    State(state): State<AppState>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Response, ApiError> {
    let query = StreamEventsQuery::parse(&pairs)?;
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/events/stream", &headers);
    let mut params = json!({ "type": query.types, "subject_id": query.subject_id });
    if let Some(last) = headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
        params["after_seq"] = json!(last);
    }
    let access = ctx
        .authorize_read(&state, Scope::EventRead, None, &params)
        .await?;
    let result: Result<Response, ApiError> = async {
        let filters = query.filters();
        // Without `Last-Event-ID` the stream is live: it starts at the current
        // head. With it, it resumes right after the last `seq` the client saw.
        let after = match headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
            Some(last) => last
                .trim()
                .parse::<i64>()
                .map_err(|_| ApiError::validation("Last-Event-ID is not a sequence number"))?,
            None => events::head_seq(&state).await?,
        };
        let kept = access.filter.clone();
        let feed = StreamState {
            state: state.clone(),
            filters,
            filter: kept,
            last_seq: after,
            pending: VecDeque::new(),
        };
        Ok(sse(stream::unfold(feed, |mut feed| async move {
            feed.next_event()
                .await
                .map(|event| (Ok::<_, Infallible>(event), feed))
        })))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

/// The live part of the event stream: the filters, the caller's row filter and
/// the last `seq` already read.
struct StreamState {
    state: AppState,
    filters: EventFilters,
    filter: ReadFilter,
    last_seq: i64,
    pending: VecDeque<Event>,
}

impl StreamState {
    /// The next frame, polling the event log once a second while quiet.
    async fn next_event(&mut self) -> Option<SseEvent> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(frame(&event));
            }
            let rows = match events::query(&self.state, &self.filters, self.last_seq, 500).await {
                Ok(rows) => rows,
                Err(_) => {
                    tokio::time::sleep(POLL).await;
                    continue;
                }
            };
            if rows.is_empty() {
                tokio::time::sleep(POLL).await;
                continue;
            }
            for event in &rows {
                self.last_seq = self.last_seq.max(event.seq);
            }
            if let Ok(kept) = events::filter_events(&self.state, &self.filter, rows).await {
                self.pending.extend(kept);
            }
        }
    }
}

/// One SSE frame: `id:` the `seq`, `event:` the type, `data:` the envelope.
fn frame(event: &Event) -> SseEvent {
    let data = serde_json::to_string(event).unwrap_or_default();
    SseEvent::default()
        .id(event.seq.to_string())
        .event(event.kind.clone())
        .data(data)
}

/// The SSE response with `Cache-Control: no-cache` and a 15 s keep-alive.
fn sse<S>(body: S) -> Response
where
    S: Stream<Item = Result<SseEvent, Infallible>> + Send + 'static,
{
    let mut response = Sse::new(body)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

/// OpenAPI paths and schemas of the event operations.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(list_events, stream_events),
    components(schemas(
        Event,
        EventList,
        EventType,
        EventSource,
        EventSourceKind,
        EventActor,
        EventActorKind,
        Subject,
    ))
)]
pub struct EventsDoc;

/// The `Resource` of an event, for selector filtering: the subject's own app,
/// node or project, plus the event's `project_id`.
#[must_use]
pub fn resource_of(event: &Event) -> Resource<'_> {
    Resource {
        kind: "event",
        id: &event.id,
        app_id: (event.subject.kind == "app").then_some(event.subject.id.as_str()),
        project_id: event.project_id.as_deref(),
        node_id: (event.subject.kind == "node").then_some(event.subject.id.as_str()),
    }
}
