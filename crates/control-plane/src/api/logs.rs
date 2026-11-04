// SPDX-License-Identifier: AGPL-3.0-only
//! On-demand log reads (`getAppLogs`, `getDeploymentLogs`) and the live-log
//! SSE stream (`streamAppLogs`, ADR 0057). Logs stay on the node (ADR 0014):
//! every handler asks the node through its session.
//!
//! A line's `time` is RFC 3339 UTC with 9 fractional digits and strictly
//! increasing per node, so the SSE `id` of a line is that instant in
//! nanoseconds since the Unix epoch. A client resumes with `Last-Event-ID` set
//! to the last `id` it saw; the handler replays the gap from `logs_query` and
//! then drops streamed lines at or before the last emitted instant.

use crate::AppState;
use crate::error::{ApiError, Code, Problem};
use crate::request::{CallCtx, RequestId};
use crate::session::{HubError, NodeStream, StreamEvent};
use axum::Json;
use axum::Router;
use axum::extract::{Extension, Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::{self, Stream};
use quoro_proto::wire::{self, LogStream};
use quoro_types::{Prefix, Scope, is_id};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use utoipa::{OpenApi, ToResponse, ToSchema};

/// How long one node gets to answer a log query or finish a stream.
pub(crate) const NODE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the SSE response keeps a quiet connection alive.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// Routes of the log reads and the live-log stream.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/apps/{app_id}/logs", get(get_app_logs))
        .route("/api/v1/apps/{app_id}/logs/stream", get(stream_app_logs))
        .route(
            "/api/v1/deployments/{deployment_id}/logs",
            get(get_deployment_logs),
        )
}

// ---------------------------------------------------------------------------
// Response shapes (`openapi.yaml` `LogEntry`, `LogList`, ...)
// ---------------------------------------------------------------------------

/// Which output stream a log line came from (`LogStreamName`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[schema(
    title = "LogStreamName",
    description = "Which output stream a log line came from."
)]
pub enum LogStreamName {
    Stdout,
    Stderr,
}

impl From<LogStream> for LogStreamName {
    fn from(value: LogStream) -> Self {
        match value {
            LogStream::Stdout => Self::Stdout,
            LogStream::Stderr => Self::Stderr,
        }
    }
}

/// One line of app output (`LogEntry`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "LogEntry",
    description = "One line of app output, as read from the node that runs the instance. Logs are not replicated; they are read on demand (D-0014)."
)]
pub struct LogEntry {
    /// When the node recorded the line.
    #[schema(format = "date-time")]
    pub time: String,
    /// Instance that produced the line.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub vm_id: String,
    /// Which stream the line came from.
    pub stream: LogStreamName,
    /// The line, without its trailing newline.
    pub line: String,
}

/// Log lines in time order (`LogList`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "LogList", description = "Log lines in time order.")]
pub struct LogList {
    pub items: Vec<LogEntry>,
}

/// Phase a deployment progress line belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentPhase {
    Pull,
    Rootfs,
    Boot,
    Health,
    Build,
}

impl DeploymentPhase {
    const ALL: [Self; 4] = [Self::Pull, Self::Rootfs, Self::Boot, Self::Health];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Pull => "pull",
            Self::Rootfs => "rootfs",
            Self::Boot => "boot",
            Self::Health => "health",
            Self::Build => "build",
        }
    }
}

/// One progress line of a deployment (`DeploymentLogEntry`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "DeploymentLogEntry",
    description = "One progress line of a deployment."
)]
pub struct DeploymentLogEntry {
    /// When the line was produced.
    #[schema(format = "date-time")]
    pub time: String,
    /// Phase the line belongs to.
    #[schema(inline)]
    pub phase: DeploymentPhase,
    /// The progress line.
    pub message: String,
}

/// Progress lines of a deployment in order (`DeploymentLogList`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "DeploymentLogList",
    description = "Progress lines of a deployment in order."
)]
pub struct DeploymentLogList {
    pub items: Vec<DeploymentLogEntry>,
}

/// On-demand telemetry could not reach the node (`code: node_unreachable`,
/// 504). Defined here because the frozen contract has it as a response that no
/// other handler uses yet.
#[derive(ToResponse)]
#[allow(dead_code)] // The newtype exists only for its schema.
#[response(
    description = "On-demand telemetry could not reach the node (`code: node_unreachable`, 504). Telemetry stays on the node and is read through the control plane on demand (D-0014).",
    content_type = "application/problem+json",
    headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))
)]
pub struct NodeUnreachable(Problem);

/// OpenAPI paths and schemas of the log operations.
#[derive(OpenApi)]
#[openapi(
    paths(get_app_logs, stream_app_logs, get_deployment_logs),
    components(
        schemas(
            LogEntry,
            LogList,
            LogStreamName,
            DeploymentLogEntry,
            DeploymentLogList,
        ),
        responses(NodeUnreachable)
    )
)]
pub struct LogsDoc;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

/// Query of `getAppLogs`.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AppLogsQuery {
    /// Only lines recorded at or after this time.
    #[param(format = "date-time")]
    pub since: Option<String>,
    /// Only lines recorded at or before this time.
    #[param(format = "date-time")]
    pub until: Option<String>,
    /// Maximum number of trailing lines to return.
    #[param(minimum = 1, maximum = 5000, default = 200)]
    pub tail: Option<i64>,
    /// Only lines of this VM (an instance of the app).
    #[param(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub instance: Option<String>,
}

/// Query of `streamAppLogs`.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AppLogsStreamQuery {
    /// Only stream this VM (an instance of the app).
    #[param(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub instance: Option<String>,
    /// Number of existing lines to replay before live output.
    #[param(minimum = 0, maximum = 5000, default = 0)]
    pub tail: Option<i64>,
}

// ---------------------------------------------------------------------------
// Time cursor
// ---------------------------------------------------------------------------

/// Nanoseconds since the Unix epoch of an RFC 3339 `time`, or `None` when it
/// does not parse.
#[must_use]
pub fn line_ns(time: &str) -> Option<i128> {
    let parsed =
        time::OffsetDateTime::parse(time, &time::format_description::well_known::Rfc3339).ok()?;
    Some(parsed.unix_timestamp_nanos())
}

/// RFC 3339 UTC with exactly 9 fractional digits, the inverse of [`line_ns`].
#[must_use]
pub fn ns_to_rfc3339(ns: i128) -> String {
    const EPOCH: &str = "1970-01-01T00:00:00.000000000Z";
    let Ok(parsed) = time::OffsetDateTime::from_unix_timestamp_nanos(ns) else {
        return EPOCH.to_string();
    };
    parsed
        .format(timestamp_format())
        .unwrap_or_else(|_| EPOCH.to_string())
}

/// `[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z`.
fn timestamp_format() -> &'static time::format_description::OwnedFormatItem {
    static FORMAT: OnceLock<time::format_description::OwnedFormatItem> = OnceLock::new();
    FORMAT.get_or_init(|| {
        time::format_description::parse_owned::<2>(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z",
        )
        .expect("the log timestamp format is valid")
    })
}

// ---------------------------------------------------------------------------
// getAppLogs
// ---------------------------------------------------------------------------

/// `GET /api/v1/apps/{app_id}/logs`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:logs`, `not_found` for an unknown
/// app, `validation_failed` for a bad `tail` or an `instance` outside the app,
/// or `node_unreachable` when a node holding the app does not answer.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/logs",
    tag = "logs",
    operation_id = "getAppLogs",
    extensions(
        ("x-quoro-scope" = json!("app:logs")),
        ("x-quoro-mcp-tool" = json!("get_app_logs")),
    ),
    params(
        AppLogsQuery,
        ("app_id" = String, Path, description = "App id (`app_...`).", pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The requested log lines.", body = LogList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = NodeUnreachable),
    )
)]
pub async fn get_app_logs(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(query): Query<AppLogsQuery>,
) -> Result<Json<LogList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/logs",
        &headers,
    );
    let logs = crate::service::logs::get_app_logs(&state, &ctx, &app_id, &query).await?;
    Ok(Json(logs))
}

// ---------------------------------------------------------------------------
// streamAppLogs
// ---------------------------------------------------------------------------

/// `GET /api/v1/apps/{app_id}/logs/stream`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:logs`, `not_found` for an unknown
/// app, `validation_failed` for a bad `tail` or an `instance` outside the app,
/// or `node_unreachable` when a node holding the app has no live session.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/logs/stream",
    tag = "logs",
    operation_id = "streamAppLogs",
    extensions(("x-quoro-scope" = json!("app:logs"))),
    params(
        AppLogsStreamQuery,
        ("app_id" = String, Path, description = "App id (`app_...`).", pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
        ("Last-Event-ID" = Option<String>, Header, description = "Last log sequence number the client received; the stream resumes after it.", nullable = false),
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
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = NodeUnreachable),
    )
)]
pub async fn stream_app_logs(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(app_id): Path<String>,
    Query(query): Query<AppLogsStreamQuery>,
) -> Result<Response, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/logs/stream",
        &headers,
    );
    let mut params = serde_json::Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(instance) = &query.instance {
        params.insert("instance".to_string(), json!(instance));
    }
    if let Some(tail) = query.tail {
        params.insert("tail".to_string(), json!(tail));
    }
    let target = crate::api::apps::app_target(&app_id);
    let access = ctx
        .authorize_read(
            &state,
            Scope::AppLogs,
            Some(&target),
            &Value::Object(params),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        require_app(&state, &app_id).await?;
        let tail = tail_arg(query.tail, 0, 0)?;
        let instance = validate_instance(&state, &app_id, query.instance.as_deref()).await?;
        let vms = app_vms(&state, &app_id).await?;
        let selected: Vec<&(String, String)> = match &instance {
            Some(instance) => vms.iter().filter(|(id, _)| id == instance).collect(),
            None => vms.iter().collect(),
        };
        if selected.is_empty() {
            return Ok(sse(stream::once(async {
                Ok::<_, Infallible>(end_event())
            })));
        }
        let fallback = if selected.len() == 1 {
            Some(selected[0].0.clone())
        } else {
            None
        };
        let nodes = distinct_nodes(&selected);
        for node in &nodes {
            if !state.hub.is_connected(node) {
                return Err(hub_error(HubError::NotConnected));
            }
        }

        // `Last-Event-ID` is the nanosecond instant of the last line the client
        // saw. An unparsable value is ignored and the stream starts live.
        let resume = headers
            .get("last-event-id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<i128>().ok());

        let mut streams: Vec<(NodeStream, i128)> = Vec::with_capacity(nodes.len());
        let mut pending: VecDeque<(usize, wire::LogLine)> = VecDeque::new();
        let mut min_ns: Vec<i128> = Vec::with_capacity(nodes.len());

        for (index, node) in nodes.iter().enumerate() {
            let stream = open_app_stream(&state, node, &app_id, instance.as_deref(), tail)?;
            let floor = match resume {
                Some(last) => {
                    // Open the live stream first so nothing produced during the
                    // gap query is lost, then fill the gap from history.
                    let mut params = selector(&app_id, instance.as_deref());
                    params.insert("since".to_string(), json!(ns_to_rfc3339(last)));
                    let result = state
                        .hub
                        .command(node, "logs_query", Value::Object(params), NODE_TIMEOUT)
                        .await
                        .map_err(hub_error)?;
                    if result.status == quoro_proto::CommandStatus::Error {
                        return Err(command_error(result));
                    }
                    let mut history: Vec<wire::LogLine> = result
                        .lines
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|line| line_ns(&line.time).is_some_and(|ns| ns > last))
                        .collect();
                    history.sort_by(|a, b| a.time.cmp(&b.time));
                    pending.extend(history.into_iter().map(|line| (index, line)));
                    last
                }
                None => -1,
            };
            min_ns.push(floor);
            streams.push((stream, floor));
        }

        let feed = MergedFeed::start(streams, pending, min_ns, instance.clone(), fallback.clone());
        Ok(sse(stream::unfold(feed, |mut feed| async move {
            feed.next_event()
                .await
                .map(|event| (Ok::<_, Infallible>(event), feed))
        })))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

// ---------------------------------------------------------------------------
// getDeploymentLogs
// ---------------------------------------------------------------------------

/// `GET /api/v1/deployments/{deployment_id}/logs`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:logs`, `not_found` for an unknown
/// deployment, or `node_unreachable` when the node holding its VM does not
/// answer.
#[utoipa::path(
    get,
    path = "/api/v1/deployments/{deployment_id}/logs",
    tag = "deployments",
    operation_id = "getDeploymentLogs",
    extensions(("x-quoro-scope" = json!("app:logs"))),
    params(
        ("deployment_id" = String, Path, description = "Deployment id (`dep_...`).", pattern = "^dep_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The progress lines.", body = DeploymentLogList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = NodeUnreachable),
    )
)]
pub async fn get_deployment_logs(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    Path(deployment_id): Path<String>,
) -> Result<Json<DeploymentLogList>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/deployments/{deployment_id}/logs",
        &headers,
    );
    let target = crate::error::Target {
        kind: "deployment".to_string(),
        id: deployment_id.clone(),
    };
    let mut access = ctx
        .authorize_read(
            &state,
            Scope::AppLogs,
            Some(&target),
            &json!({ "deployment_id": deployment_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        if !deployment_id.starts_with("dep_") {
            return Err(ApiError::not_found("deployment"));
        }
        let app_id: Option<(String,)> =
            sqlx::query_as("SELECT app_id FROM deployments WHERE id = ?")
                .bind(&deployment_id)
                .fetch_optional(state.store.pool())
                .await?;
        let Some((app_id,)) = app_id else {
            return Err(ApiError::not_found("deployment"));
        };
        let project: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
            .bind(&app_id)
            .fetch_optional(state.store.pool())
            .await?;
        access.set_project(project.as_ref().map(|(project_id,)| project_id.as_str()));

        // The link rule of `deploy` (module documentation): the deployment's VM
        // shares the app and `created_at`. If that row is gone, any VM of the app
        // still names the node that holds its history.
        let node: Option<(String,)> = sqlx::query_as(
            "SELECT v.node_id FROM vms v JOIN deployments d \
             ON d.app_id = v.app_id AND d.created_at = v.created_at \
             WHERE d.id = ? LIMIT 1",
        )
        .bind(&deployment_id)
        .fetch_optional(state.store.pool())
        .await?;
        let node = match node {
            Some((node,)) => Some(node),
            None => sqlx::query_as::<_, (String,)>(
                "SELECT node_id FROM vms WHERE app_id = ? ORDER BY id LIMIT 1",
            )
            .bind(&app_id)
            .fetch_optional(state.store.pool())
            .await?
            .map(|(node,)| node),
        };
        let Some(node) = node else {
            return Ok(Json(DeploymentLogList { items: Vec::new() }));
        };
        if !state.hub.is_connected(&node) {
            return Err(hub_error(HubError::NotConnected));
        }

        let mut streams: Vec<(DeploymentPhase, NodeStream)> = Vec::new();
        for phase in DeploymentPhase::ALL {
            let stream = state
                .hub
                .open_stream(
                    &node,
                    "deployment_logs",
                    json!({
                        "deployment_id": deployment_id,
                        "phase": phase.as_str(),
                    }),
                )
                .map_err(hub_error)?;
            streams.push((phase, stream));
        }

        let deadline = Instant::now() + NODE_TIMEOUT;
        let mut items = Vec::new();
        for (phase, mut stream) in streams {
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(hub_error(HubError::Timeout));
                }
                let event = tokio::time::timeout(remaining, stream.next())
                    .await
                    .map_err(|_| hub_error(HubError::Timeout))?;
                match event {
                    None | Some(StreamEvent::End { .. }) => break,
                    Some(StreamEvent::Lines(lines)) => {
                        for line in lines {
                            items.push(DeploymentLogEntry {
                                time: line.time,
                                phase,
                                message: line.line,
                            });
                        }
                    }
                    Some(StreamEvent::Metrics(_)) => {}
                    Some(StreamEvent::Data(_) | StreamEvent::SessionEnd { .. }) => {}
                }
            }
        }
        items.sort_by(|a, b| a.time.cmp(&b.time));
        Ok(Json(DeploymentLogList { items }))
    }
    .await;
    ctx.finish_read(&state, access, result).await
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The merged live-log feed of one or several nodes.
struct MergedFeed {
    /// History lines to emit before live output.
    pending: VecDeque<(usize, wire::LogLine)>,
    rx: mpsc::UnboundedReceiver<(usize, StreamEvent)>,
    /// Last instant emitted per node; streamed lines at or before it are
    /// duplicates of replayed history.
    min_ns: Vec<i128>,
    ended: Vec<bool>,
    instance: Option<String>,
    fallback: Option<String>,
    done: bool,
}

impl MergedFeed {
    /// Sends every node's stream into one channel, so the SSE body reads them
    /// in arrival order and one slow node never blocks another.
    fn start(
        streams: Vec<(NodeStream, i128)>,
        pending: VecDeque<(usize, wire::LogLine)>,
        mut min_ns: Vec<i128>,
        instance: Option<String>,
        fallback: Option<String>,
    ) -> Self {
        let count = streams.len();
        let (tx, rx) = mpsc::unbounded_channel();
        for (index, (mut stream, _)) in streams.into_iter().enumerate() {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    let end = matches!(event, StreamEvent::End { .. });
                    if tx.send((index, event)).is_err() {
                        break;
                    }
                    if end {
                        break;
                    }
                }
            });
        }
        // `min_ns` arrives aligned with `streams`; keep the length in step.
        min_ns.resize(count, -1);
        Self {
            pending,
            rx,
            min_ns,
            ended: vec![false; count],
            instance,
            fallback,
            done: false,
        }
    }

    /// The next SSE frame, or `None` when the client is done reading.
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some((index, line)) = self.pending.pop_front() {
                if let Some(event) = self.line_event(index, &line) {
                    return Some(event);
                }
                continue;
            }
            if self.done {
                return None;
            }
            let Some((index, event)) = self.rx.recv().await else {
                self.done = true;
                return None;
            };
            match event {
                StreamEvent::Lines(lines) => {
                    for line in lines {
                        self.pending.push_back((index, line));
                    }
                }
                StreamEvent::Metrics(_) => {}
                StreamEvent::End { reason, message } => {
                    if let Some(ended) = self.ended.get_mut(index) {
                        *ended = true;
                    }
                    if reason == "error" {
                        self.done = true;
                        return Some(error_event(&message));
                    }
                    if self.ended.iter().all(|ended| *ended) {
                        self.done = true;
                        return Some(end_event());
                    }
                }
                StreamEvent::Data(_) | StreamEvent::SessionEnd { .. } => {}
            }
        }
    }

    /// One `log` frame, or `None` for a duplicate or an unattributable line.
    fn line_event(&mut self, index: usize, line: &wire::LogLine) -> Option<Event> {
        let ns = line_ns(&line.time)?;
        if self.min_ns.get(index).is_none_or(|last| ns <= *last) {
            return None;
        }
        let vm_id = line
            .vm_id
            .clone()
            .or_else(|| self.instance.clone())
            .or_else(|| self.fallback.clone())?;
        if let Some(last) = self.min_ns.get_mut(index) {
            *last = ns;
        }
        let entry = LogEntry {
            time: line.time.clone(),
            vm_id,
            stream: line.stream.into(),
            line: line.line.clone(),
        };
        let data = serde_json::to_string(&entry).unwrap_or_default();
        Some(Event::default().id(ns.to_string()).event("log").data(data))
    }
}

/// Opens one `app_logs` stream on `node`.
fn open_app_stream(
    state: &AppState,
    node: &str,
    app_id: &str,
    instance: Option<&str>,
    tail: i64,
) -> Result<NodeStream, ApiError> {
    let mut params = selector(app_id, instance);
    params.insert("tail".to_string(), json!(tail));
    state
        .hub
        .open_stream(node, "app_logs", Value::Object(params))
        .map_err(hub_error)
}

/// The `app_id` or `vm_id` selector of `logs_query`/`app_logs` (exactly one).
pub(crate) fn selector(app_id: &str, instance: Option<&str>) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    match instance {
        Some(instance) => {
            params.insert("vm_id".to_string(), json!(instance));
        }
        None => {
            params.insert("app_id".to_string(), json!(app_id));
        }
    }
    params
}

/// The `tail` bound of a log read.
pub(crate) fn tail_arg(tail: Option<i64>, default: i64, minimum: i64) -> Result<i64, ApiError> {
    match tail {
        None => Ok(default),
        Some(value) if (minimum..=5000).contains(&value) => Ok(value),
        Some(value) => Err(ApiError::validation(format!(
            "tail {value} is outside {minimum}-5000"
        ))),
    }
}

/// Rejects an unknown or malformed app id (404).
pub(crate) async fn require_app(state: &AppState, app_id: &str) -> Result<(), ApiError> {
    if !app_id.starts_with("app_") {
        return Err(ApiError::not_found("app"));
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?;
    row.map(|_| ()).ok_or_else(|| ApiError::not_found("app"))
}

/// `instance` must name a VM of this app (422 when it does not).
pub(crate) async fn validate_instance(
    state: &AppState,
    app_id: &str,
    instance: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let Some(instance) = instance else {
        return Ok(None);
    };
    if !is_id(Prefix::Vm, instance) {
        return Err(ApiError::validation(
            "instance is not an instance of this app",
        ));
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM vms WHERE id = ? AND app_id = ?")
        .bind(instance)
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?;
    if row.is_none() {
        return Err(ApiError::validation(
            "instance is not an instance of this app",
        ));
    }
    Ok(Some(instance.to_string()))
}

/// `(vm_id, node_id)` of every VM of the app, ordered by VM id.
pub(crate) async fn app_vms(
    state: &AppState,
    app_id: &str,
) -> Result<Vec<(String, String)>, ApiError> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, node_id FROM vms WHERE app_id = ? ORDER BY id")
            .bind(app_id)
            .fetch_all(state.store.pool())
            .await?;
    Ok(rows)
}

/// The distinct nodes of the selected VMs, sorted for a deterministic order.
pub(crate) fn distinct_nodes(vms: &[&(String, String)]) -> Vec<String> {
    let mut nodes: Vec<String> = vms.iter().map(|(_, node)| node.clone()).collect();
    nodes.sort();
    nodes.dedup();
    nodes
}

/// The `LogEntry` of one line, with the instance or the app's only VM filling
/// a missing `vm_id`; `None` when the line cannot be attributed.
pub(crate) fn log_entry(
    line: &wire::LogLine,
    instance: Option<&str>,
    fallback: Option<&str>,
) -> Option<LogEntry> {
    let vm_id = line.vm_id.clone().or_else(|| {
        instance
            .map(str::to_string)
            .or_else(|| fallback.map(str::to_string))
    })?;
    Some(LogEntry {
        time: line.time.clone(),
        vm_id,
        stream: line.stream.into(),
        line: line.line.clone(),
    })
}

/// Maps a [`HubError`] to the contract's `node_unreachable` (504).
pub(crate) fn hub_error(error: HubError) -> ApiError {
    ApiError::new(Code::NodeUnreachable, error.to_string())
}

/// A `command_result` with `status: error` is a node problem for a log read.
pub(crate) fn command_error(result: quoro_proto::CommandResult) -> ApiError {
    let detail = result
        .error
        .map(|error| error.message)
        .or(result.detail)
        .unwrap_or_else(|| "the node could not read the logs".to_string());
    ApiError::new(Code::NodeUnreachable, detail)
}

/// An SSE `end` frame.
fn end_event() -> Event {
    Event::default().event("end").data("{}")
}

/// An SSE `error` frame carrying a `node_unreachable` `Problem`.
fn error_event(message: &str) -> Event {
    let problem = Problem {
        kind: "about:blank".to_string(),
        title: "Node unreachable".to_string(),
        status: 504,
        detail: Some(if message.is_empty() {
            "the node reported an error".to_string()
        } else {
            message.to_string()
        }),
        instance: None,
        code: Code::NodeUnreachable,
        reason: None,
        scope: None,
        target: None,
    };
    let data = serde_json::to_string(&problem).unwrap_or_default();
    Event::default().event("error").data(data)
}

/// The SSE response with `Cache-Control: no-cache` and a 15 s keep-alive. The
/// `Quoro-Request-Id` header is added by the request-id middleware.
fn sse<S>(body: S) -> Response
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_time_round_trips_through_nanoseconds() {
        for ns in [0, 1, 1_700_000_000_123_456_789, 1_700_000_000_000_000_001] {
            let text = ns_to_rfc3339(ns);
            assert_eq!(text.len(), 30, "{text}");
            assert!(text.ends_with('Z'));
            assert_eq!(line_ns(&text), Some(ns), "{text}");
        }
    }

    #[test]
    fn nine_fractional_digits_are_always_written() {
        assert_eq!(ns_to_rfc3339(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            ns_to_rfc3339(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20.000000000Z"
        );
    }

    #[test]
    fn a_bad_time_is_not_a_cursor() {
        assert_eq!(line_ns("yesterday"), None);
        assert_eq!(line_ns(""), None);
    }
}
