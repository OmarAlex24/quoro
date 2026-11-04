// SPDX-License-Identifier: AGPL-3.0-only
//! Metrics reads (`getNodeMetrics`, `getAppMetrics`) and their live SSE
//! streams (`streamNodeMetrics`, `streamAppMetrics`, ADR 0057). Telemetry stays
//! on the node (ADR 0014): every handler asks the nodes through their sessions
//! and nothing is stored, cached or replicated here.

use crate::AppState;
use crate::api::logs::{NODE_TIMEOUT, app_vms, hub_error, require_app};
use crate::error::{ApiError, Code, Problem};
use crate::request::{CallCtx, RequestId};
use crate::session::{NodeStream, StreamEvent};
use axum::Json;
use axum::Router;
use axum::extract::rejection::PathRejection;
use axum::extract::{Extension, Path, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::{self, Stream};
use quoro_types::{Prefix, Scope, is_id};
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::mpsc;
use utoipa::ToSchema;

/// How often the SSE response keeps a quiet connection alive.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// Routes of the metrics reads and live streams.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/nodes/{node_id}/metrics", get(get_node_metrics))
        .route(
            "/api/v1/nodes/{node_id}/metrics/stream",
            get(stream_node_metrics),
        )
        .route("/api/v1/apps/{app_id}/metrics", get(get_app_metrics))
        .route(
            "/api/v1/apps/{app_id}/metrics/stream",
            get(stream_app_metrics),
        )
}

// ---------------------------------------------------------------------------
// Response shapes (`openapi.yaml` `NodeMetrics`, `AppMetrics`, ...)
// ---------------------------------------------------------------------------

/// One sample of a node's CPU and memory (`NodeMetricSample`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "NodeMetricSample",
    description = "One sample of a node's CPU and memory, taken by the node agent every `interval_s`."
)]
pub struct NodeMetricSample {
    /// When the node took the sample.
    #[schema(format = "date-time")]
    pub time: String,
    /// Busy fraction of all host CPUs over the sampling interval.
    #[schema(minimum = 0, maximum = 1)]
    pub cpu_usage: f64,
    /// Number of host CPUs.
    #[schema(minimum = 1)]
    pub cpu_count: u32,
    /// Host memory.
    #[schema(minimum = 0)]
    pub memory_total_bytes: u64,
    /// Host memory in use, `MemTotal - MemAvailable`.
    #[schema(minimum = 0)]
    pub memory_used_bytes: u64,
}

impl From<quoro_proto::metrics::NodeSample> for NodeMetricSample {
    fn from(value: quoro_proto::metrics::NodeSample) -> Self {
        Self {
            time: value.time,
            cpu_usage: value.cpu_usage,
            cpu_count: value.cpu_count,
            memory_total_bytes: value.memory_total_bytes,
            memory_used_bytes: value.memory_used_bytes,
        }
    }
}

/// A node's recent CPU and memory samples (`NodeMetrics`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "NodeMetrics",
    description = "A node's recent CPU and memory samples, read from the node on demand and never stored by the control plane (ADR 0014)."
)]
pub struct NodeMetrics {
    /// The node.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Seconds between samples (5).
    #[schema(minimum = 1)]
    pub interval_s: u32,
    /// Samples, oldest first.
    pub samples: Vec<NodeMetricSample>,
}

/// One sample of a VM's CPU and memory (`VmMetricSample`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "VmMetricSample",
    description = "One sample of a VM's CPU and memory as the host sees its VMM process; no guest agent is involved."
)]
pub struct VmMetricSample {
    /// When the node took the sample.
    #[schema(format = "date-time")]
    pub time: String,
    /// CPU time of the VM's VMM process over the interval, divided by the
    /// interval and by `vcpus`.
    #[schema(minimum = 0)]
    pub cpu_usage: f64,
    /// The VM's virtual CPUs.
    #[schema(minimum = 1)]
    pub vcpus: u32,
    /// Resident memory of the VMM process on the host.
    #[schema(minimum = 0)]
    pub memory_rss_bytes: u64,
    /// The VM's configured memory.
    #[schema(minimum = 0)]
    pub memory_bytes: u64,
}

impl From<quoro_proto::metrics::VmSample> for VmMetricSample {
    fn from(value: quoro_proto::metrics::VmSample) -> Self {
        Self {
            time: value.time,
            cpu_usage: value.cpu_usage,
            vcpus: value.vcpus,
            memory_rss_bytes: value.memory_rss_bytes,
            memory_bytes: value.memory_bytes,
        }
    }
}

/// Recent samples of one instance of an app (`AppInstanceMetrics`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "AppInstanceMetrics",
    description = "Recent samples of one instance of an app."
)]
pub struct AppInstanceMetrics {
    /// The instance.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub vm_id: String,
    /// Node that runs it.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// Samples, oldest first; empty when the instance is not running.
    pub samples: Vec<VmMetricSample>,
}

/// Recent CPU and memory samples of an app's instances (`AppMetrics`).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(
    title = "AppMetrics",
    description = "Recent CPU and memory samples of an app's instances, read from their nodes on demand and never stored by the control plane (ADR 0014)."
)]
pub struct AppMetrics {
    /// The app.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// Seconds between samples (5).
    #[schema(minimum = 1)]
    pub interval_s: u32,
    /// One entry per instance.
    pub instances: Vec<AppInstanceMetrics>,
}

/// A streamed VM sample: `VmMetricSample` plus `vm_id` and `node_id`
/// (`streamAppMetrics`).
#[derive(Serialize)]
struct StreamedVmSample<'a> {
    #[serde(flatten)]
    sample: &'a VmMetricSample,
    vm_id: &'a str,
    node_id: &'a str,
}

// ---------------------------------------------------------------------------
// getNodeMetrics
// ---------------------------------------------------------------------------

/// `GET /api/v1/nodes/{node_id}/metrics`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `node:read`, `not_found` for an
/// unknown node, or `node_unreachable` when the node has no live session.
#[utoipa::path(
    get,
    path = "/api/v1/nodes/{node_id}/metrics",
    tag = "metrics",
    operation_id = "getNodeMetrics",
    extensions(("x-quoro-scope" = json!("node:read"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The node's recent samples.", body = NodeMetrics,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn get_node_metrics(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    node_id: Result<Path<String>, PathRejection>,
) -> Result<Json<NodeMetrics>, ApiError> {
    let node_id = path_id(node_id, "node")?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/nodes/{node_id}/metrics",
        &headers,
    );
    let target = crate::error::Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let access = ctx
        .authorize_read(
            &state,
            Scope::NodeRead,
            Some(&target),
            &json!({ "node_id": node_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        require_node(&state, &node_id).await?;
        if !state.hub.is_connected(&node_id) {
            return Err(hub_error(crate::session::HubError::NotConnected));
        }
        let answer = state
            .hub
            .command(
                &node_id,
                "metrics_query",
                json!({ "node": true }),
                NODE_TIMEOUT,
            )
            .await
            .map_err(hub_error)?;
        if answer.status == quoro_proto::CommandStatus::Error {
            return Err(metrics_command_error(answer));
        }
        let report = answer.metrics.unwrap_or_default();
        Ok(NodeMetrics {
            node_id,
            interval_s: interval_or_default(report.interval_s),
            samples: report
                .node
                .unwrap_or_default()
                .into_iter()
                .map(NodeMetricSample::from)
                .collect(),
        })
    }
    .await;
    ctx.finish_read(&state, access, result).await.map(Json)
}

// ---------------------------------------------------------------------------
// streamNodeMetrics
// ---------------------------------------------------------------------------

/// `GET /api/v1/nodes/{node_id}/metrics/stream`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `node:read`, `not_found` for an
/// unknown node, or `node_unreachable` when the node has no live session.
#[utoipa::path(
    get,
    path = "/api/v1/nodes/{node_id}/metrics/stream",
    tag = "metrics",
    operation_id = "streamNodeMetrics",
    extensions(("x-quoro-scope" = json!("node:read"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
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
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn stream_node_metrics(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    node_id: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let node_id = path_id(node_id, "node")?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/nodes/{node_id}/metrics/stream",
        &headers,
    );
    let target = crate::error::Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let access = ctx
        .authorize_read(
            &state,
            Scope::NodeRead,
            Some(&target),
            &json!({ "node_id": node_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        require_node(&state, &node_id).await?;
        if !state.hub.is_connected(&node_id) {
            return Err(hub_error(crate::session::HubError::NotConnected));
        }
        let stream = state
            .hub
            .open_stream(&node_id, "metrics", json!({ "node": true }))
            .map_err(hub_error)?;
        let feed = NodeMetricsFeed {
            stream,
            pending: VecDeque::new(),
            done: false,
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

// ---------------------------------------------------------------------------
// getAppMetrics
// ---------------------------------------------------------------------------

/// `GET /api/v1/apps/{app_id}/metrics`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:read`, `not_found` for an
/// unknown app, or `node_unreachable` when a node holding an instance has no
/// live session.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/metrics",
    tag = "metrics",
    operation_id = "getAppMetrics",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).", pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The instances' recent samples.", body = AppMetrics,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 404, response = crate::openapi::NotFound),
        (status = 429, response = crate::openapi::RateLimited),
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn get_app_metrics(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    app_id: Result<Path<String>, PathRejection>,
) -> Result<Json<AppMetrics>, ApiError> {
    let app_id = path_id(app_id, "app")?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/metrics",
        &headers,
    );
    let target = crate::api::apps::app_target(&app_id);
    let access = ctx
        .authorize_read(
            &state,
            Scope::AppRead,
            Some(&target),
            &json!({ "app_id": app_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        require_app(&state, &app_id).await?;
        let vms = app_vms(&state, &app_id).await?;
        let nodes = group_by_node(&vms);
        for node in nodes.keys() {
            if !state.hub.is_connected(node) {
                return Err(hub_error(crate::session::HubError::NotConnected));
            }
        }

        let mut interval_s = 5;
        let mut samples: BTreeMap<String, Vec<VmMetricSample>> = BTreeMap::new();
        for (node, vm_ids) in &nodes {
            let answer = state
                .hub
                .command(
                    node,
                    "metrics_query",
                    json!({ "vm_ids": vm_ids }),
                    NODE_TIMEOUT,
                )
                .await
                .map_err(hub_error)?;
            if answer.status == quoro_proto::CommandStatus::Error {
                return Err(metrics_command_error(answer));
            }
            let report = answer.metrics.unwrap_or_default();
            interval_s = interval_or_default(report.interval_s);
            for entry in report.vms.unwrap_or_default() {
                samples
                    .entry(entry.vm_id)
                    .or_default()
                    .extend(entry.samples.into_iter().map(VmMetricSample::from));
            }
        }

        let instances = vms
            .into_iter()
            .map(|(vm_id, node_id)| {
                let samples = samples.remove(&vm_id).unwrap_or_default();
                AppInstanceMetrics {
                    vm_id,
                    node_id,
                    samples,
                }
            })
            .collect();
        Ok(AppMetrics {
            app_id,
            interval_s,
            instances,
        })
    }
    .await;
    ctx.finish_read(&state, access, result).await.map(Json)
}

// ---------------------------------------------------------------------------
// streamAppMetrics
// ---------------------------------------------------------------------------

/// `GET /api/v1/apps/{app_id}/metrics/stream`.
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:read`, `not_found` for an
/// unknown app, or `node_unreachable` when a node holding an instance has no
/// live session.
#[utoipa::path(
    get,
    path = "/api/v1/apps/{app_id}/metrics/stream",
    tag = "metrics",
    operation_id = "streamAppMetrics",
    extensions(("x-quoro-scope" = json!("app:read"))),
    params(
        ("app_id" = String, Path, description = "App id (`app_...`).", pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$"),
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
        (status = 504, response = crate::api::logs::NodeUnreachable),
    )
)]
pub async fn stream_app_metrics(
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    identity: crate::auth::Identity,
    headers: HeaderMap,
    app_id: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let app_id = path_id(app_id, "app")?;
    let ctx = CallCtx::new(
        identity,
        request_id.0,
        "GET /api/v1/apps/{app_id}/metrics/stream",
        &headers,
    );
    let target = crate::api::apps::app_target(&app_id);
    let access = ctx
        .authorize_read(
            &state,
            Scope::AppRead,
            Some(&target),
            &json!({ "app_id": app_id }),
        )
        .await?;
    let result: Result<_, ApiError> = async {
        require_app(&state, &app_id).await?;
        let vms = app_vms(&state, &app_id).await?;
        if vms.is_empty() {
            // No instance to sample: the SSE body is empty and ends at once.
            return Ok(sse(stream::empty()));
        }
        let nodes = group_by_node(&vms);
        for node in nodes.keys() {
            if !state.hub.is_connected(node) {
                return Err(hub_error(crate::session::HubError::NotConnected));
            }
        }

        let mut streams = Vec::with_capacity(nodes.len());
        for (node, vm_ids) in &nodes {
            let stream = state
                .hub
                .open_stream(node, "metrics", json!({ "vm_ids": vm_ids }))
                .map_err(hub_error)?;
            streams.push((node.clone(), stream));
        }
        let feed = AppMetricsFeed::start(streams);
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
// Shared helpers
// ---------------------------------------------------------------------------

/// The merged live-metrics feed of one or several nodes.
struct AppMetricsFeed {
    pending: VecDeque<(String, String, quoro_proto::metrics::VmSample)>,
    rx: mpsc::UnboundedReceiver<(String, StreamEvent)>,
    done: bool,
}

impl AppMetricsFeed {
    /// Sends every node's stream into one channel, so the SSE body reads them
    /// in arrival order and one slow node never blocks another.
    fn start(streams: Vec<(String, NodeStream)>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for (node_id, mut stream) in streams {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    let end = matches!(event, StreamEvent::End { .. });
                    if tx.send((node_id.clone(), event)).is_err() {
                        break;
                    }
                    if end {
                        break;
                    }
                }
            });
        }
        Self {
            pending: VecDeque::new(),
            rx,
            done: false,
        }
    }

    /// The next SSE frame, or `None` when the client is done reading.
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some((node_id, vm_id, sample)) = self.pending.pop_front() {
                let sample = VmMetricSample::from(sample);
                let data = StreamedVmSample {
                    sample: &sample,
                    vm_id: &vm_id,
                    node_id: &node_id,
                };
                return Some(sample_event(&data));
            }
            if self.done {
                return None;
            }
            let Some((node_id, event)) = self.rx.recv().await else {
                self.done = true;
                return Some(error_event("the node session ended"));
            };
            match event {
                StreamEvent::Metrics(report) => {
                    for entry in report.vms.unwrap_or_default() {
                        for sample in entry.samples {
                            self.pending
                                .push_back((node_id.clone(), entry.vm_id.clone(), sample));
                        }
                    }
                }
                StreamEvent::Lines(_) | StreamEvent::Data(_) | StreamEvent::SessionEnd { .. } => {}
                StreamEvent::End { message, .. } => {
                    self.done = true;
                    return Some(error_event(&message));
                }
            }
        }
    }
}

/// The live samples of one node's `metrics` stream.
struct NodeMetricsFeed {
    stream: NodeStream,
    pending: VecDeque<quoro_proto::metrics::NodeSample>,
    done: bool,
}

impl NodeMetricsFeed {
    /// The next SSE frame, or `None` when the client is done reading.
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some(sample) = self.pending.pop_front() {
                return Some(sample_event(&NodeMetricSample::from(sample)));
            }
            if self.done {
                return None;
            }
            match self.stream.next().await {
                Some(StreamEvent::Metrics(report)) => {
                    self.pending.extend(report.node.unwrap_or_default());
                }
                Some(
                    StreamEvent::Lines(_) | StreamEvent::Data(_) | StreamEvent::SessionEnd { .. },
                ) => {}
                Some(StreamEvent::End { message, .. }) => {
                    self.done = true;
                    return Some(error_event(&message));
                }
                None => {
                    self.done = true;
                    return Some(error_event("the node session ended"));
                }
            }
        }
    }
}

/// The id of a path extractor; a malformed id is a `not_found` (404).
fn path_id(path: Result<Path<String>, PathRejection>, what: &str) -> Result<String, ApiError> {
    path.map(|Path(id)| id)
        .map_err(|_| ApiError::not_found(what))
}

/// Rejects an unknown or malformed node id (404).
async fn require_node(state: &AppState, node_id: &str) -> Result<(), ApiError> {
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let row: Option<(String,)> = sqlx::query_as("SELECT id FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_optional(state.store.pool())
        .await?;
    row.map(|_| ()).ok_or_else(|| ApiError::not_found("node"))
}

/// The VMs of an app grouped by their node, in node-id order.
fn group_by_node(vms: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    let mut nodes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (vm_id, node_id) in vms {
        nodes
            .entry(node_id.clone())
            .or_default()
            .push(vm_id.clone());
    }
    nodes
}

/// The sampling interval a report states, or the node's fixed 5 s when it
/// states none.
fn interval_or_default(interval_s: u32) -> u32 {
    if interval_s == 0 { 5 } else { interval_s }
}

/// A `command_result` with `status: error` is a node problem for a metrics
/// read.
fn metrics_command_error(result: quoro_proto::CommandResult) -> ApiError {
    let detail = result
        .error
        .map(|error| error.message)
        .or(result.detail)
        .unwrap_or_else(|| "the node could not read the metrics".to_string());
    ApiError::new(Code::NodeUnreachable, detail)
}

/// An SSE `sample` frame; unlike a log line it carries no `id`.
fn sample_event<T: Serialize>(sample: &T) -> Event {
    let data = serde_json::to_string(sample).unwrap_or_default();
    Event::default().event("sample").data(data)
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
