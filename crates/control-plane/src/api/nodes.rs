//! `GET /api/v1/nodes` and `GET /api/v1/nodes/{node_id}`.
//!
//! Rows come from enrollment (`crate::api::enroll`). JSON columns are parsed
//! into the contract's component schemas.

use crate::AppState;
use crate::api::page::finish;
use crate::auth::Identity;
use crate::error::ApiError;
use crate::request::{CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::HeaderMap;
use quoro_types::Scope;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use std::collections::BTreeMap;
use utoipa::{IntoParams, ToSchema};

/// A machine running the Quoro binary (D-0012).
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "Node")]
pub struct Node {
    /// Node id.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Node name, unique in the cluster.
    pub name: String,
    /// Roles this node runs; they combine (D-0018).
    #[schema(inline)]
    pub roles: Vec<NodeRole>,
    /// Control-plane status, or null before this node is a primary/replica.
    #[schema(inline, required)]
    pub cp_status: Option<NodeCpStatus>,
    /// Session lifecycle status of the node.
    #[schema(inline)]
    pub status: NodeStatus,
    /// Ed25519 public key of the node, base64.
    pub public_key: String,
    /// Reachable address if the node is public, else null.
    pub address: Option<String>,
    /// Whether the node dials out instead of accepting inbound sessions.
    pub behind_nat: bool,
    /// Operator labels used by placement.
    pub labels: BTreeMap<String, String>,
    pub capacity: NodeCapacity,
    pub allocated: NodeCapacity,
    pub kvm: KvmInfo,
    /// Version of the engine implementation on the node.
    pub engine_version: Option<String>,
    /// Version of the node agent on the node.
    pub agent_version: Option<String>,
    /// Revision of the desired state the control plane wants applied.
    #[schema(minimum = 0)]
    pub desired_rev: i64,
    /// Revision the node last reported as applied.
    #[schema(minimum = 0)]
    pub applied_rev: i64,
    /// Last change-log seq the node has verified.
    #[schema(minimum = 0)]
    pub log_seq: i64,
    /// Unacknowledged failed fences of this node, or null.
    #[schema(required)]
    pub fence_failure: Option<NodeFenceFailure>,
    /// Last heartbeat or message from the node.
    pub last_seen_at: Option<String>,
    /// When the node first enrolled.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// One role a node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Node,
    Witness,
    Edge,
}

/// Control-plane status of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum NodeCpStatus {
    Primary,
    Replica,
}

/// Session lifecycle status of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Pending,
    Connected,
    Disconnected,
    Fenced,
    Removed,
}

/// Capacity of a node, and the part of it allocated to VMs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "NodeCapacity")]
pub struct NodeCapacity {
    /// Number of logical CPUs.
    #[schema(minimum = 1)]
    pub cpus: u32,
    /// Memory in mebibytes, as reported by the kernel.
    pub memory_mb: u64,
    /// Disk in gibibytes available to VM disks.
    pub disk_gb: u64,
}

/// Result of the KVM probe a node runs at enrollment and on request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "KvmInfo")]
pub struct KvmInfo {
    /// Whether `/dev/kvm` is usable.
    pub available: bool,
    /// Kernel release reported by the probe.
    pub kernel: String,
    /// Whether the node itself runs as a guest with nested virtualization.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub nested: Option<bool>,
}

/// Unacknowledged failed fences of a node: a forced stop could not confirm
/// that these VMs stopped, so they may still run (D-0074).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "NodeFenceFailure")]
pub struct NodeFenceFailure {
    /// When the primary recorded the first of these failures.
    #[schema(format = "date-time")]
    pub detected_at: String,
    /// Leases whose fence failed.
    #[schema(pattern = "^lse_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub lease_ids: Vec<String>,
    /// Apps bound to those leases.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_ids: Vec<String>,
    /// VMs the node could not confirm stopped.
    #[schema(pattern = "^vm_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub vm_ids: Vec<String>,
}

/// One page of nodes.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(title = "NodeList")]
pub struct NodeList {
    pub items: Vec<Node>,
    /// Cursor for the next page, or null on the last page.
    #[schema(required)]
    pub next_cursor: Option<String>,
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

/// A JSON column parsed into `T`.
fn json_column<T: DeserializeOwned>(row: &SqliteRow, column: &str) -> Result<T, ApiError> {
    let text: String = row.try_get(column)?;
    serde_json::from_str(&text)
        .map_err(|e| ApiError::internal(&format!("nodes.{column} is invalid: {e}")))
}

/// A nullable JSON column parsed into `T`.
fn optional_json_column<T: DeserializeOwned>(
    row: &SqliteRow,
    column: &str,
) -> Result<Option<T>, ApiError> {
    match row.try_get::<Option<String>, _>(column)? {
        None => Ok(None),
        Some(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| ApiError::internal(&format!("nodes.{column} is invalid: {e}"))),
    }
}

/// Builds a [`Node`] from a `nodes` row.
fn node_from_row(row: &SqliteRow) -> Result<Node, ApiError> {
    let cp_status: Option<NodeCpStatus> = row
        .try_get::<Option<String>, _>("cp_status")?
        .map(|text| {
            serde_json::from_value(Value::String(text))
                .map_err(|e| ApiError::internal(&format!("nodes.cp_status is invalid: {e}")))
        })
        .transpose()?;
    let status: NodeStatus = serde_json::from_value(Value::String(row.try_get("status")?))
        .map_err(|e| ApiError::internal(&format!("nodes.status is invalid: {e}")))?;
    Ok(Node {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        roles: json_column(row, "roles")?,
        cp_status,
        status,
        public_key: row.try_get("public_key")?,
        address: row.try_get("address")?,
        behind_nat: row.try_get::<i64, _>("behind_nat")? != 0,
        labels: json_column(row, "labels")?,
        capacity: json_column(row, "capacity")?,
        allocated: json_column(row, "allocated")?,
        kvm: json_column(row, "kvm")?,
        engine_version: row.try_get("engine_version")?,
        agent_version: row.try_get("agent_version")?,
        desired_rev: row.try_get("desired_rev")?,
        applied_rev: row.try_get("applied_rev")?,
        log_seq: row.try_get("log_seq")?,
        fence_failure: optional_json_column(row, "fence_failure")?,
        last_seen_at: row.try_get("last_seen_at")?,
        created_at: row.try_get("created_at")?,
    })
}

/// All nodes of the cluster.
#[utoipa::path(
    get,
    path = "/api/v1/nodes",
    tag = "nodes",
    operation_id = "listNodes",
    extensions(
        ("x-quoro-scope" = json!("node:read")),
        ("x-quoro-mcp-tool" = json!("list_nodes")),
    ),
    params(PageParams),
    responses(
        (status = 200, description = "One page of nodes.", body = NodeList,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `node:read`, or a database error.
pub async fn list_nodes(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Query(page): Query<PageParams>,
) -> Result<Json<NodeList>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/nodes", &headers);
    ctx.authorize_read(Scope::NodeRead)?;
    let limit = page.limit()?;
    let rows = sqlx::query("SELECT * FROM nodes WHERE id > ? ORDER BY id LIMIT ?")
        .bind(page.after())
        .bind(limit + 1)
        .fetch_all(state.store.pool())
        .await?;
    let nodes = rows
        .iter()
        .map(node_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let (items, next_cursor) = finish(nodes, limit, |node| node.id.clone());
    Ok(Json(NodeList { items, next_cursor }))
}

/// One node.
#[utoipa::path(
    get,
    path = "/api/v1/nodes/{node_id}",
    tag = "nodes",
    operation_id = "getNode",
    extensions(
        ("x-quoro-scope" = json!("node:read")),
        ("x-quoro-mcp-tool" = json!("get_node")),
    ),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The node.", body = Node,
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
/// `forbidden` without `node:read`, `not_found` for an unknown id, or a
/// database error.
pub async fn get_node(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/nodes/{node_id}",
        &headers,
    );
    ctx.authorize_read(Scope::NodeRead)?;
    if !quoro_types::is_id(quoro_types::Prefix::Node, &node_id) {
        return Err(ApiError::not_found("node"));
    }
    let row = sqlx::query("SELECT * FROM nodes WHERE id = ?")
        .bind(&node_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("node"))?;
    Ok(Json(node_from_row(&row)?))
}
