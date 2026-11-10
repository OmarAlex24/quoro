//! `GET /api/v1/nodes` and `GET /api/v1/nodes/{node_id}`.
//!
//! Rows come from enrollment (`crate::api::enroll`). JSON columns are parsed
//! into the contract's component schemas.

use crate::AppState;

use crate::auth::Identity;
use crate::changelog::{self, Op};
use crate::error::{ApiError, Target};
use crate::request::{ApiJson, CallCtx, RequestId};
use crate::session::NodeHub;
use crate::store::now_rfc3339;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use quoro_types::{Prefix, Scope, is_id};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::sqlite::SqliteRow;
use std::collections::{BTreeMap, HashMap};
use utoipa::{IntoParams, ToSchema};

/// The operation string recorded in the audit log.
const SET_TUNNEL_OPERATION: &str = "PUT /api/v1/nodes/{node_id}/tunnel";
const DELETE_TUNNEL_OPERATION: &str = "DELETE /api/v1/nodes/{node_id}/tunnel";
const FENCE_ACK_OPERATION: &str = "POST /api/v1/nodes/{node_id}/fence-ack";

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
    /// Last change-log seq the node has verified. From M5b the newest
    /// acknowledgement the primary holds in memory (`node-agent-protocol.md`
    /// section 7.1).
    #[schema(minimum = 0)]
    pub log_seq: i64,
    /// How many change-log entries the node is behind the primary: the
    /// primary's newest seq minus `log_seq`. Null for a machine that does not
    /// follow, one without the `node` role (`node-agent-protocol.md` section
    /// 7.1, ADR 0227, ADR 0301).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<i64>, minimum = 0, required = false)]
    pub log_lag: Option<Option<i64>>,
    /// Whether the cluster keys are sealed to this machine: `sealed`,
    /// `pending`, `none` or `key_changed`, or null without the `node` role
    /// (`node-agent-protocol.md` section 7.2). Always present.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<NodeCustody>, inline, required = false)]
    pub custody: Option<Option<NodeCustody>>,
    /// Unacknowledged failed fences of this node, or null.
    #[schema(required)]
    pub fence_failure: Option<NodeFenceFailure>,
    /// The node's tunnel mapping (D-0094, D-0095), or null.
    #[schema(required)]
    pub tunnel: Option<NodeTunnel>,
    /// Last heartbeat or message from the node.
    pub last_seen_at: Option<String>,
    /// When an operator declared this node lost (ADR 0183), or null.
    #[schema(format = "date-time", required = true)]
    pub lost_at: Option<String>,
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

/// Whether the cluster keys are sealed to a machine (`Node.custody`, ADR 0180).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeCustody {
    Sealed,
    Pending,
    None,
    KeyChanged,
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

/// Which `tunnel` provider runs a node's tunnel (D-0094, D-0095). The run
/// token is never returned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(title = "NodeTunnel")]
pub struct NodeTunnel {
    /// The `tunnel` provider whose account holds the tunnel.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// The provider's id of the tunnel, or null until the primary created it.
    #[schema(required)]
    pub tunnel_id: Option<String>,
    /// Loopback port on the node where its proxy accepts the tunnel's traffic.
    #[schema(minimum = 1, maximum = 65535)]
    pub origin_port: u16,
}

/// Body of `setNodeTunnel` (`openapi.yaml` `SetNodeTunnelRequest`).
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(
    title = "SetNodeTunnelRequest",
    description = "Map a node to the `tunnel` provider that runs its tunnel."
)]
pub struct SetNodeTunnelRequest {
    /// A provider of kind `tunnel`; any other kind is refused with
    /// `validation_failed`.
    #[schema(pattern = "^prv_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub provider_id: String,
    /// Loopback port on the node where its proxy accepts the tunnel's
    /// traffic.
    #[serde(default)]
    #[schema(value_type = u16, minimum = 1, maximum = 65535, default = 8081, required = false)]
    pub origin_port: Option<u16>,
}

/// Body of `ackNodeFence` (`openapi.yaml` `FenceAckRequest`).
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(
    title = "FenceAckRequest",
    description = "The failed fences an operator verified out of band."
)]
pub struct FenceAckRequest {
    /// Lease ids of the failed fences being acknowledged.
    #[schema(schema_with = fence_ack_lease_ids_schema)]
    pub lease_ids: Vec<String>,
}

/// `FenceAckRequest.lease_ids`: at least one unique `lse_` id.
fn fence_ack_lease_ids_schema() -> utoipa::openapi::schema::Array {
    use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, Type};
    ArrayBuilder::new()
        .items(
            ObjectBuilder::new()
                .schema_type(Type::String)
                .pattern(Some("^lse_[0-9A-HJKMNP-TV-Z]{26}$")),
        )
        .min_items(Some(1))
        .unique_items(true)
        .build()
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
    pub limit: Option<i64>,
    /// Opaque cursor returned as `next_cursor` by the previous page.
    pub cursor: Option<String>,
}

impl PageParams {
    /// The validated limit.
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

    /// The cursor, or the empty string, which sorts before every id.
    pub(crate) fn after(&self) -> &str {
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

/// The `SELECT` list of a node row: the `nodes` columns plus the capacity the
/// VMs placed on the node use, summed from `vms` at read time so every node
/// (not only the primary) always reports a current allocation.
pub(crate) const NODE_COLUMNS: &str = "n.*, \
     (SELECT IFNULL(SUM(vms.vcpus), 0) FROM vms WHERE vms.node_id = n.id) AS allocated_cpus, \
     (SELECT IFNULL(SUM(vms.memory_mb), 0) FROM vms WHERE vms.node_id = n.id) AS allocated_memory_mb, \
     (SELECT IFNULL(SUM(vms.disk_gb), 0) FROM vms WHERE vms.node_id = n.id) AS allocated_disk_gb";

/// Builds a [`Node`] from a row selected with [`NODE_COLUMNS`].
pub(crate) fn node_from_row(row: &SqliteRow) -> Result<Node, ApiError> {
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
        allocated: NodeCapacity {
            cpus: u32::try_from(row.try_get::<i64, _>("allocated_cpus")?)
                .map_err(|e| ApiError::internal(&format!("allocated cpus: {e}")))?,
            memory_mb: u64::try_from(row.try_get::<i64, _>("allocated_memory_mb")?)
                .map_err(|e| ApiError::internal(&format!("allocated memory_mb: {e}")))?,
            disk_gb: u64::try_from(row.try_get::<i64, _>("allocated_disk_gb")?)
                .map_err(|e| ApiError::internal(&format!("allocated disk_gb: {e}")))?,
        },
        kvm: json_column(row, "kvm")?,
        engine_version: row.try_get("engine_version")?,
        agent_version: row.try_get("agent_version")?,
        desired_rev: row.try_get("desired_rev")?,
        applied_rev: row.try_get("applied_rev")?,
        // Both filled by `attach_log_position` (section 7.1, "Lag").
        log_seq: row.try_get("log_seq")?,
        log_lag: None,
        // Filled by `attach_custody` (section 7.2, "Status").
        custody: None,
        fence_failure: optional_json_column(row, "fence_failure")?,
        // Filled by `attach_tunnel` from `node_tunnels` (ADR 0095).
        tunnel: None,
        last_seen_at: row.try_get("last_seen_at")?,
        lost_at: row.try_get("lost_at")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Fills `log_seq` and `log_lag` of nodes built by [`node_from_row`]
/// (`node-agent-protocol.md` section 7.1, "Lag", ADR 0227, ADR 0301):
///
/// - `log_seq` is the newest acknowledgement the primary holds in memory: for
///   a node that follows on a live session, that session's
///   ([`NodeHub::log_seq`]); otherwise the higher of the row's
///   `nodes.log_seq` and the newest acknowledgement held from an earlier
///   session ([`NodeHub::newest_log_ack`]). The row alone would lag between a
///   session's detach and its end write, or after a failed end write; and
///   after a restart of the primary, which holds none, the row is the
///   position.
/// - `log_lag` is the change log's newest `seq` minus `log_seq`, never below
///   0, for a machine with the `node` role, and null for any other machine.
///   It is never absent.
///
/// The newest `seq` is read once, through `executor`, after the positions in
/// memory, so it is at least every acknowledgement read.
///
/// # Errors
///
/// A database error.
pub(crate) async fn attach_log_position<'e, E>(
    executor: E,
    hub: &NodeHub,
    nodes: &mut [Node],
) -> Result<(), ApiError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    for node in nodes.iter_mut() {
        let seq = hub.log_seq(&node.id).unwrap_or_else(|| {
            u64::try_from(node.log_seq)
                .unwrap_or(0)
                .max(hub.newest_log_ack(&node.id))
        });
        node.log_seq = i64::try_from(seq).unwrap_or(i64::MAX);
    }
    let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
        .fetch_one(executor)
        .await?;
    let head = head.unwrap_or(0);
    for node in nodes {
        let follows = node.roles.contains(&NodeRole::Node);
        node.log_lag = Some(follows.then(|| head.saturating_sub(node.log_seq).max(0)));
    }
    Ok(())
}

/// A node's id, whether it has a bound custody key and a refused change, and
/// one of its seals' `key_kind` and `key_id`, or none ([`attach_custody`]).
type CustodyRow = (String, bool, bool, Option<String>, Option<String>);

/// Fills `custody` of nodes built by [`node_from_row`]
/// (`node-agent-protocol.md` section 7.2, "Status", ADR 0231):
///
/// - null for a machine without the `node` role;
/// - `key_changed` once the machine reported a key other than the bound one;
/// - `none` before it reports a custody key;
/// - `sealed` when every key in use has a seal row for it;
/// - `pending` otherwise.
///
/// It is never absent. The keys in use are the primary's own
/// ([`crate::custody::Keyring::key_ids_in_use`]), computed once per call; the
/// bound key, the refused change and the seal rows of `nodes` alone are read
/// in one query, through `executor`.
///
/// # Errors
///
/// A database error.
pub(crate) async fn attach_custody<'e, E>(
    executor: E,
    keyring: &crate::custody::Keyring<'_>,
    nodes: &mut [Node],
) -> Result<(), ApiError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    if nodes.is_empty() {
        return Ok(());
    }
    let placeholders = vec!["?"; nodes.len()].join(", ");
    let sql = format!(
        "SELECT n.id, n.custody_public_key IS NOT NULL, n.custody_key_changed_at IS NOT NULL, \
         k.key_kind, k.key_id \
         FROM nodes n LEFT JOIN node_key_custody k ON k.node_id = n.id \
         WHERE n.id IN ({placeholders})"
    );
    let mut query = sqlx::query_as::<_, CustodyRow>(&sql);
    for node in nodes.iter() {
        query = query.bind(&node.id);
    }
    let rows = query.fetch_all(executor).await?;
    let mut read: HashMap<&str, CustodyRead<'_>> = HashMap::new();
    for (id, has_key, has_changed, key_kind, key_id) in &rows {
        let entry = read.entry(id.as_str()).or_insert_with(|| CustodyRead {
            bound: *has_key,
            changed: *has_changed,
            sealed: Vec::new(),
        });
        if let (Some(key_kind), Some(key_id)) = (key_kind, key_id) {
            entry.sealed.push((key_kind.as_str(), key_id.as_str()));
        }
    }
    let in_use = keyring.key_ids_in_use();
    for node in nodes {
        let (bound, changed, sealed) = read
            .get(node.id.as_str())
            .map_or((false, false, &[][..]), |read| {
                (read.bound, read.changed, read.sealed.as_slice())
            });
        node.custody = Some(custody_status(
            node.roles.contains(&NodeRole::Node),
            bound,
            changed,
            sealed,
            &in_use,
        ));
    }
    Ok(())
}

/// What [`attach_custody`] read for one node.
struct CustodyRead<'a> {
    /// Whether a custody key is bound.
    bound: bool,
    /// Whether a changed key was refused.
    changed: bool,
    /// The `(key_kind, key_id)` of each of its seals.
    sealed: Vec<(&'a str, &'a str)>,
}

/// `Node.custody` from what [`attach_custody`] read (section 7.2, "Status").
fn custody_status(
    follows: bool,
    bound: bool,
    changed: bool,
    sealed: &[(&str, &str)],
    in_use: &[(quoro_proto::custody::KeyKind, String)],
) -> Option<NodeCustody> {
    if !follows {
        return None;
    }
    let status = if changed {
        NodeCustody::KeyChanged
    } else if !bound {
        NodeCustody::None
    } else if in_use
        .iter()
        .all(|(kind, key_id)| sealed.contains(&(kind.as_str(), key_id.as_str())))
    {
        NodeCustody::Sealed
    } else {
        NodeCustody::Pending
    };
    Some(status)
}

/// The node's tunnel mapping, without the run token (`NodeTunnel`).
pub(crate) async fn tunnel_of<'e, E>(
    executor: E,
    node_id: &str,
) -> Result<Option<NodeTunnel>, ApiError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT provider_id, tunnel_id, origin_port FROM node_tunnels WHERE node_id = ?",
    )
    .bind(node_id)
    .fetch_optional(executor)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let origin_port: i64 = row.try_get("origin_port")?;
    Ok(Some(NodeTunnel {
        provider_id: row.try_get("provider_id")?,
        tunnel_id: row.try_get("tunnel_id")?,
        origin_port: u16::try_from(origin_port)
            .map_err(|e| ApiError::internal(&format!("node_tunnels.origin_port: {e}")))?,
    }))
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
    let list = crate::service::nodes::list_nodes(&state, &ctx, &page).await?;
    Ok(Json(list))
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
    let node = crate::service::nodes::get_node(&state, &ctx, &node_id).await?;
    Ok(Json(node))
}

/// Maps a node to the `tunnel` provider that runs its tunnel (ADR 0094,
/// 0095). The account credential never leaves the primary; the node later
/// receives only its own tunnel id, run token and origin port.
#[utoipa::path(
    put,
    path = "/api/v1/nodes/{node_id}/tunnel",
    tag = "nodes",
    operation_id = "setNodeTunnel",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body = SetNodeTunnelRequest,
    responses(
        (status = 200, description = "The mapping was recorded; the node is returned with its `tunnel`.", body = Node,
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
///
/// # Errors
///
/// `forbidden` for an agent or without `provider:manage`, `not_found` for an
/// unknown node, `validation_failed` for a provider that is missing or is not
/// a `tunnel` provider with `account_id` and `zone_id`, `conflict` when the
/// node still serves `tunnel` domains under another provider, or a database
/// error.
pub async fn set_node_tunnel(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(node_id): Path<String>,
    ApiJson(body): ApiJson<SetNodeTunnelRequest>,
) -> Result<Json<Node>, ApiError> {
    let origin_port = body.origin_port.unwrap_or(8081);
    if origin_port == 0 {
        return Err(ApiError::validation(
            "origin_port must be between 1 and 65535",
        ));
    }
    let params = json!({ "provider_id": &body.provider_id, "origin_port": origin_port });
    let target = Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let ctx = CallCtx::new(identity, request_id, SET_TUNNEL_OPERATION, &headers);
    ctx.authorize_human(&state, Scope::ProviderManage, Some(&target), None, &params)
        .await?;
    match set_tunnel_tx(
        &state,
        &ctx,
        &node_id,
        &body.provider_id,
        origin_port,
        &params,
    )
    .await
    {
        Ok(node) => {
            state.hub.node_changed(&node_id);
            state.tunnel.kick(&node_id);
            Ok(Json(node))
        }
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

/// Records a human's acknowledgement of a node's failed fences
/// (`node-agent-protocol.md` section 8.5, D-0074, D-0079).
#[utoipa::path(
    post,
    path = "/api/v1/nodes/{node_id}/fence-ack",
    tag = "nodes",
    operation_id = "ackNodeFence",
    extensions(("x-quoro-scope" = json!("node:fence_ack"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = FenceAckRequest, description = "The lease ids of the failed fences being acknowledged."),
    responses(
        (status = 200, description = "The acknowledgement was recorded; the node is returned with `fence_failure` null.", body = Node,
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
///
/// # Errors
///
/// `forbidden` for an agent or without `node:fence_ack`, `validation_failed`
/// for an empty list, a repeated id or one that is not an `lse_` id,
/// `not_found` for an unknown node, `conflict` when the node has no failed
/// fence or `lease_ids` differs from its outstanding ones, or a database
/// error.
pub async fn ack_node_fence(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(node_id): Path<String>,
    ApiJson(body): ApiJson<FenceAckRequest>,
) -> Result<Json<Node>, ApiError> {
    let params = json!({ "lease_ids": &body.lease_ids });
    let target = Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let ctx = CallCtx::new(identity, request_id, FENCE_ACK_OPERATION, &headers);
    ctx.authorize_human(&state, Scope::NodeFenceAck, Some(&target), None, &params)
        .await?;
    match fence_ack_tx(&state, &ctx, &node_id, &body.lease_ids, &params).await {
        Ok(node) => {
            // The node's session rebuilds its snapshot and sends the
            // `fence_ack` at once; a node without one gets it at its next
            // session start (section 8.5 step 4).
            state.hub.node_changed(&node_id);
            Ok(Json(node))
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::NodeFenceAck),
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

/// A lease row's `fencing_token` as the token a `fence_ack` carries.
fn stored_token(token: i64) -> u64 {
    u64::try_from(token).unwrap_or(0)
}

/// Section 8.5 step 3 in one transaction: checks the list against the node's
/// outstanding failed fences, increments each listed lease's token and
/// records it revoked as when declaring a node lost (section 8.3), clears
/// `fence_failure`, queues the acknowledgement in `fence_ack_pending`, and
/// writes the change-log entries and the audit record.
///
/// Only the leases this node still holds change: declaring a node lost
/// touches the leases the node holds (section 8.3), and a lease another node
/// holds already carries a higher token than this node's, so revoking it
/// here would only end the other node's grant without telling it (ADR 0246).
/// Every listed lease is still acknowledged to the node, each with a token
/// (ADR 0245): a lease the node holds, its token after the increment; one
/// another node holds, that lease's token plus one, so the node clears a
/// failed fence it recorded at that token (ADR 0247); and one with no row,
/// [`crate::fences::UNKNOWN_LEASE_TOKEN`] (P-0273). The node clears only the
/// failed fences below its lease's token.
async fn fence_ack_tx(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    lease_ids: &[String],
    params: &Value,
) -> Result<Node, ApiError> {
    if lease_ids.is_empty() {
        return Err(ApiError::validation(
            "lease_ids must name at least one lease",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for lease_id in lease_ids {
        if !is_id(Prefix::Lease, lease_id) {
            return Err(ApiError::validation(format!(
                "lease_ids: {lease_id} is not a lease id"
            )));
        }
        if !seen.insert(lease_id.as_str()) {
            return Err(ApiError::validation(format!(
                "lease_ids: {lease_id} is listed twice"
            )));
        }
    }
    let mut w = state.store.begin_write().await?;
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("node"));
    }
    let Some(failure) = crate::fences::failure_of(&mut w.tx, node_id).await? else {
        return Err(ApiError::conflict(
            "the node has no failed fence to acknowledge",
        ));
    };
    let mut asked: Vec<&str> = lease_ids.iter().map(String::as_str).collect();
    let mut outstanding: Vec<&str> = failure.lease_ids.iter().map(String::as_str).collect();
    asked.sort_unstable();
    outstanding.sort_unstable();
    outstanding.dedup();
    if asked != outstanding {
        return Err(ApiError::conflict(format!(
            "lease_ids must equal the node's outstanding failed fences: {}",
            outstanding.join(", ")
        )));
    }

    let actor = ctx.identity.actor();
    let bumped = crate::leases::bump_tokens(&mut w.tx, lease_ids, node_id, &actor).await?;
    let mut pending = crate::fences::pending_of(&mut w.tx, node_id)
        .await?
        .unwrap_or_default();
    pending.acked_at = now_rfc3339();
    for lease_id in lease_ids {
        let token = match bumped.iter().find(|lease| &lease.id == lease_id) {
            Some(lease) => stored_token(lease.fencing_token),
            None => match crate::leases::by_id(&mut w.tx, lease_id).await? {
                // Another node holds it (ADR 0247).
                Some(lease) => stored_token(lease.fencing_token).saturating_add(1),
                None => crate::fences::UNKNOWN_LEASE_TOKEN,
            },
        };
        if !pending.lease_ids.contains(lease_id) {
            pending.lease_ids.push(lease_id.clone());
        }
        pending.fencing_tokens.insert(lease_id.clone(), token);
    }
    sqlx::query("UPDATE nodes SET fence_failure = NULL, fence_ack_pending = ? WHERE id = ?")
        .bind(crate::fences::pending_text(Some(&pending))?)
        .bind(node_id)
        .execute(&mut *w.tx)
        .await?;
    let payload = crate::driver::node_payload(&mut w.tx, node_id).await?;
    let seq = changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload).await?;
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
    };
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::NodeFenceAck),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    let row = sqlx::query(&format!(
        "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?"
    ))
    .bind(node_id)
    .fetch_one(&mut *w.tx)
    .await?;
    let mut node = node_from_row(&row)?;
    node.tunnel = tunnel_of(&mut *w.tx, node_id).await?;
    attach_log_position(&mut *w.tx, &state.hub, std::slice::from_mut(&mut node)).await?;
    attach_custody(
        &mut *w.tx,
        &state.keyring(),
        std::slice::from_mut(&mut node),
    )
    .await?;
    w.commit().await?;
    // The operator checked that the failed fence's VMs are stopped, and the
    // token moved as when declaring the node lost (section 8.5 step 3), so a
    // restore may grant the lease elsewhere (issue #392).
    for lease in &bumped {
        state
            .hub
            .record_release(&lease.id, node_id, lease.fencing_token);
    }
    Ok(node)
}

/// Validates the provider, writes the row, its change-log entry and the audit
/// record in one transaction.
async fn set_tunnel_tx(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    provider_id: &str,
    origin_port: u16,
    params: &Value,
) -> Result<Node, ApiError> {
    let mut w = state.store.begin_write().await?;
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let node = sqlx::query(&format!(
        "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?"
    ))
    .bind(node_id)
    .fetch_optional(&mut *w.tx)
    .await?
    .ok_or_else(|| ApiError::not_found("node"))?;
    tunnel_provider(&mut w.tx, provider_id).await?;
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT provider_id FROM node_tunnels WHERE node_id = ?")
            .bind(node_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    let now = now_rfc3339();
    let op = match &existing {
        Some((current,)) if current == provider_id => {
            sqlx::query(
                "UPDATE node_tunnels SET origin_port = ?, updated_at = ? WHERE node_id = ?",
            )
            .bind(i64::from(origin_port))
            .bind(&now)
            .bind(node_id)
            .execute(&mut *w.tx)
            .await?;
            Op::Update
        }
        Some(_) => {
            if serves_tunnel_domains(&mut w.tx, node_id).await? {
                return Err(ApiError::conflict(
                    "the node still serves tunnel domains; remove them before changing its provider",
                ));
            }
            sqlx::query(
                "INSERT INTO node_tunnels (node_id, provider_id, tunnel_id, token_enc, origin_port, created_at, updated_at) \
                 VALUES (?, ?, NULL, NULL, ?, ?, ?) \
                 ON CONFLICT (node_id) DO UPDATE SET provider_id = excluded.provider_id, \
                 tunnel_id = NULL, token_enc = NULL, origin_port = excluded.origin_port, \
                 updated_at = excluded.updated_at",
            )
            .bind(node_id)
            .bind(provider_id)
            .bind(i64::from(origin_port))
            .bind(&now)
            .bind(&now)
            .execute(&mut *w.tx)
            .await?;
            Op::Update
        }
        None => {
            sqlx::query(
                "INSERT INTO node_tunnels (node_id, provider_id, tunnel_id, token_enc, origin_port, created_at, updated_at) \
                 VALUES (?, ?, NULL, NULL, ?, ?, ?)",
            )
            .bind(node_id)
            .bind(provider_id)
            .bind(i64::from(origin_port))
            .bind(&now)
            .bind(&now)
            .execute(&mut *w.tx)
            .await?;
            Op::Create
        }
    };
    // The change log is the replicated state: it carries the ciphertext as
    // hex and never the plaintext run token.
    let (tunnel_id, token_enc): (Option<String>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT tunnel_id, token_enc FROM node_tunnels WHERE node_id = ?")
            .bind(node_id)
            .fetch_one(&mut *w.tx)
            .await?;
    let payload = json!({
        "node_id": node_id,
        "provider_id": provider_id,
        "tunnel_id": tunnel_id,
        "token_enc": token_enc.map(|blob| crate::tunnel::hex(&blob)),
        "origin_port": origin_port,
        "updated_at": now,
    });
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "node_tunnel",
        node_id,
        op,
        payload,
    )
    .await?;
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
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
    let mut node = node_from_row(&node)?;
    node.tunnel = tunnel_of(&mut *w.tx, node_id).await?;
    attach_log_position(&mut *w.tx, &state.hub, std::slice::from_mut(&mut node)).await?;
    attach_custody(
        &mut *w.tx,
        &state.keyring(),
        std::slice::from_mut(&mut node),
    )
    .await?;
    w.commit().await?;
    Ok(node)
}

/// Checks that `provider_id` is a `tunnel` provider whose `config` carries a
/// string `account_id` and `zone_id`.
async fn tunnel_provider(conn: &mut SqliteConnection, provider_id: &str) -> Result<(), ApiError> {
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT kind, config_json FROM providers WHERE id = ?")
            .bind(provider_id)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((kind, config_json)) = row else {
        return Err(ApiError::validation("provider_id names no provider"));
    };
    if kind != "tunnel" {
        return Err(ApiError::validation(
            "provider_id must name a provider of kind tunnel",
        ));
    }
    let config: Value = serde_json::from_str(&config_json)
        .map_err(|e| ApiError::internal(&format!("stored provider config: {e}")))?;
    for field in ["account_id", "zone_id"] {
        if config
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(ApiError::validation(format!(
                "the tunnel provider needs a string {field} in its config"
            )));
        }
    }
    Ok(())
}

/// Whether any `tunnel` domain of an app with a VM on the node still needs
/// this node's tunnel.
async fn serves_tunnel_domains(
    conn: &mut SqliteConnection,
    node_id: &str,
) -> Result<bool, ApiError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM domains d JOIN vms v ON v.app_id = d.app_id \
         WHERE d.tls_mode = 'tunnel' AND v.node_id = ?1)",
    )
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(row.0 != 0)
}

/// Removes a node's tunnel mapping (ADR 0094, 0095). The manager then removes
/// the tunnel at the provider and the node's next snapshot stops carrying
/// `spec.ingress`.
#[utoipa::path(
    delete,
    path = "/api/v1/nodes/{node_id}/tunnel",
    tag = "nodes",
    operation_id = "deleteNodeTunnel",
    extensions(("x-quoro-scope" = json!("provider:manage"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 204, description = "The mapping was removed.",
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
/// `forbidden` for an agent or without `provider:manage`, `not_found` for an
/// unknown node or a node without a mapping, `conflict` while the node still
/// serves `tunnel` domains, or a database error.
pub async fn delete_node_tunnel(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let params = json!({ "node_id": &node_id });
    let target = Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let ctx = CallCtx::new(identity, request_id, DELETE_TUNNEL_OPERATION, &headers);
    ctx.authorize_human(&state, Scope::ProviderManage, Some(&target), None, &params)
        .await?;
    match delete_tunnel_tx(&state, &ctx, &node_id, &params).await {
        Ok(()) => {
            state.hub.node_changed(&node_id);
            state.tunnel.kick(&node_id);
            Ok(StatusCode::NO_CONTENT)
        }
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
async fn delete_tunnel_tx(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    params: &Value,
) -> Result<(), ApiError> {
    let mut w = state.store.begin_write().await?;
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let node: Option<(String,)> = sqlx::query_as("SELECT id FROM nodes WHERE id = ?")
        .bind(node_id)
        .fetch_optional(&mut *w.tx)
        .await?;
    if node.is_none() {
        return Err(ApiError::not_found("node"));
    }
    let mapping: Option<(String,)> =
        sqlx::query_as("SELECT provider_id FROM node_tunnels WHERE node_id = ?")
            .bind(node_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    if mapping.is_none() {
        return Err(ApiError::not_found("node tunnel"));
    }
    if serves_tunnel_domains(&mut w.tx, node_id).await? {
        return Err(ApiError::conflict(
            "the node still serves tunnel domains; remove them first",
        ));
    }
    sqlx::query("DELETE FROM node_tunnels WHERE node_id = ?")
        .bind(node_id)
        .execute(&mut *w.tx)
        .await?;
    let seq = changelog::append(
        &mut w.tx,
        &ctx.identity.actor(),
        "node_tunnel",
        node_id,
        Op::Delete,
        json!({ "node_id": node_id }),
    )
    .await?;
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
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

// ---------------------------------------------------------------------------
// Roles (`updateNodeRoles`, ADR 0170)
// ---------------------------------------------------------------------------

/// The operation string recorded in the audit log.
const ROLES_OPERATION: &str = "POST /api/v1/nodes/{node_id}/roles";

/// The only role `updateNodeRoles` changes in M5b (`NodeRolesRequest`).
const WITNESS: &str = "witness";

/// Body of `updateNodeRoles` (`openapi.yaml` `NodeRolesRequest`).
///
/// The handler reads the body as JSON and validates it after the caller is
/// authorized, so an agent is refused `human_only` whatever it sends, and
/// every refusal is audited.
#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[schema(
    title = "NodeRolesRequest",
    description = "Add or remove the `witness` role of an enrolled machine (ADR 0170)."
)]
pub struct NodeRolesRequest {
    /// Roles to add.
    #[serde(default)]
    #[schema(schema_with = roles_change_schema)]
    pub add: Option<Vec<String>>,
    /// Roles to remove.
    #[serde(default)]
    #[schema(schema_with = roles_change_schema)]
    pub remove: Option<Vec<String>>,
    /// Remove a vote even though fewer than three voters would remain while
    /// `expiring` leases exist.
    #[serde(default)]
    #[schema(default = false)]
    pub force: bool,
}

/// `NodeRolesRequest.add` and `.remove`: at least one unique role, and only
/// `witness` can change in M5b.
fn roles_change_schema() -> utoipa::openapi::schema::Array {
    use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, Type};
    ArrayBuilder::new()
        .items(
            ObjectBuilder::new()
                .schema_type(Type::String)
                .enum_values(Some([WITNESS])),
        )
        .min_items(Some(1))
        .unique_items(true)
        .build()
}

/// One validated change of `updateNodeRoles`: add or remove `witness`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RolesChange {
    AddWitness,
    RemoveWitness,
}

/// Reads a `NodeRolesRequest`: exactly one of `add` and `remove`, each a
/// non-empty list of unique roles of which only `witness` can change.
fn roles_change(body: &Value) -> Result<(RolesChange, bool), ApiError> {
    let request: NodeRolesRequest = serde_json::from_value(body.clone())
        .map_err(|e| ApiError::validation(format!("the body is not a NodeRolesRequest: {e}")))?;
    let (change, roles) = match (&request.add, &request.remove) {
        (Some(roles), None) => (RolesChange::AddWitness, roles),
        (None, Some(roles)) => (RolesChange::RemoveWitness, roles),
        (Some(_), Some(_)) => {
            return Err(ApiError::validation(
                "give exactly one of add and remove, not both",
            ));
        }
        (None, None) => {
            return Err(ApiError::validation("give exactly one of add and remove"));
        }
    };
    if roles.is_empty() {
        return Err(ApiError::validation("the list of roles must not be empty"));
    }
    if roles.len() > 1 {
        return Err(ApiError::validation(
            "the list of roles contains a duplicate or a role that cannot change",
        ));
    }
    if roles[0] != WITNESS {
        return Err(ApiError::validation(format!(
            "only the witness role can be added or removed; {} is chosen at enrollment",
            roles[0]
        )));
    }
    Ok((change, request.force))
}

/// Adds or removes the `witness` role of an enrolled machine without
/// enrolling it again (ADR 0170, `node-agent-protocol.md` sections 6.12 and
/// 8.10). The machine acts on its next `desired_state`.
#[utoipa::path(
    post,
    path = "/api/v1/nodes/{node_id}/roles",
    tag = "nodes",
    operation_id = "updateNodeRoles",
    extensions(("x-quoro-scope" = json!("node:roles"))),
    params(
        ("node_id" = String, Path, description = "Node id (`nod_...`).", pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    request_body(content = NodeRolesRequest, description = "The role to add or to remove."),
    responses(
        (status = 200, description = "The node with its roles after the change.", body = Node,
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
///
/// # Errors
///
/// In this order: `forbidden` for an agent (`human_only`) or without
/// `node:roles`; `validation_failed` for a body with both or neither of
/// `add` and `remove` or a role other than `witness`; `not_found` for an
/// unknown node. Adding a role the machine has, or removing one it lacks,
/// then answers `200` and changes nothing. After that: `validation_failed`
/// when the machine would be left with no role; `conflict` for a removed
/// node; `conflict` while the previous voter-set change has not reached a
/// majority of voters (section 8.10 rule 2); `conflict` when removing the
/// vote would leave fewer than three voters while any `expiring` lease
/// exists, unless `force` is set (D-0170's guard). Or a database error.
pub async fn update_node_roles(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    Path(node_id): Path<String>,
    ApiJson(body): ApiJson<Value>,
) -> Result<Json<Node>, ApiError> {
    // `params` is the request body (audit-log-format.md section 3.7).
    let params = body.clone();
    let target = Target {
        kind: "node".to_string(),
        id: node_id.clone(),
    };
    let ctx = CallCtx::new(identity, request_id, ROLES_OPERATION, &headers);
    ctx.authorize_human(&state, Scope::NodeRoles, Some(&target), None, &params)
        .await?;
    match roles_tx(&state, &ctx, &node_id, &body, &params).await {
        Ok((node, changed)) => {
            if changed {
                // Every voter's `spec.votes` names the roles of every voter,
                // and a new witness changes the mesh's peers (sections 6.9
                // and 6.12): every machine builds its desired state again.
                state.hub.nodes_changed_all();
            }
            Ok(Json(node))
        }
        Err(error) => {
            ctx.audit_error(
                &state,
                Some(Scope::NodeRoles),
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

/// The checks and the write of `updateNodeRoles`, in one write transaction
/// so rule 2 counts against the version this write moves (section 8.10).
/// Returns the node after the call and whether anything changed.
async fn roles_tx(
    state: &AppState,
    ctx: &CallCtx,
    node_id: &str,
    body: &Value,
    params: &Value,
) -> Result<(Node, bool), ApiError> {
    let (change, force) = roles_change(body)?;
    let mut w = state.store.begin_write().await?;
    if !is_id(Prefix::Node, node_id) {
        return Err(ApiError::not_found("node"));
    }
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT roles, status FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *w.tx)
            .await?;
    let Some((roles, status)) = row else {
        return Err(ApiError::not_found("node"));
    };
    let roles: Vec<String> = serde_json::from_str(&roles).map_err(|e| ApiError::internal(&e))?;
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
    };
    let has_witness = roles.iter().any(|role| role == WITNESS);

    // Adding a role the machine has, or removing one it lacks, changes
    // nothing and answers `200` before any other check; it is still
    // recorded, with no `change_seq` (audit-log-format.md section 3.7).
    let unchanged = match change {
        RolesChange::AddWitness => has_witness,
        RolesChange::RemoveWitness => !has_witness,
    };
    if unchanged {
        ctx.append_ok(
            &mut w.tx,
            Some(Scope::NodeRoles),
            Some(&target),
            None,
            params,
            None,
        )
        .await?;
        let node = node_in(&mut w.tx, &state.hub, node_id).await?;
        w.commit().await?;
        return Ok((node, false));
    }

    let new_roles: Vec<String> = match change {
        RolesChange::AddWitness => roles
            .iter()
            .cloned()
            .chain(std::iter::once(WITNESS.to_string()))
            .collect(),
        RolesChange::RemoveWitness => roles
            .iter()
            .filter(|role| *role != WITNESS)
            .cloned()
            .collect(),
    };
    if new_roles.is_empty() {
        return Err(ApiError::validation(
            "removing witness would leave the machine with no role",
        ));
    }
    if status == "removed" {
        return Err(ApiError::conflict("the node is removed"));
    }

    // One machine has one vote: only a change that adds or removes it
    // changes the voter set (sections 6.12 and 8.10).
    let votes_before = crate::voters::is_voter_roles(&roles);
    let votes_after = crate::voters::is_voter_roles(&new_roles);
    let changes_vote = votes_before != votes_after;
    if changes_vote {
        let (_, voters) = state.hub.voter_change_gate(&mut w.tx).await?;
        if votes_before && !force && voters.saturating_sub(1) < 3 {
            let expiring: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM leases WHERE mode = 'expiring')")
                    .fetch_one(&mut *w.tx)
                    .await?;
            if expiring {
                return Err(ApiError::conflict(
                    "removing this vote would leave fewer than three voters while expiring leases exist; set force to remove it anyway",
                ));
            }
        }
    }

    let actor = ctx.identity.actor();
    // A machine that gains the witness role gets a mesh address if it has
    // none (section 6.12), before the entry that changes its roles, so that
    // entry is the one that moves the voter-set version.
    if change == RolesChange::AddWitness {
        crate::session::mesh::assign_address(&mut w.tx, &actor, node_id)
            .await
            .map_err(|e| ApiError::internal(&e))?;
    }
    let roles_json = serde_json::to_string(&new_roles).map_err(|e| ApiError::internal(&e))?;
    sqlx::query("UPDATE nodes SET roles = ? WHERE id = ?")
        .bind(&roles_json)
        .bind(node_id)
        .execute(&mut *w.tx)
        .await?;
    let payload = crate::driver::node_payload(&mut w.tx, node_id).await?;
    let seq = changelog::append(&mut w.tx, &actor, "node", node_id, Op::Update, payload).await?;
    if changes_vote {
        // The change moved the voter-set version to its own entry, or
        // nothing is written (section 8.10 rules 1 and 2).
        state.hub.voter_change_landed(&mut w.tx, seq).await?;
    }
    ctx.append_ok(
        &mut w.tx,
        Some(Scope::NodeRoles),
        Some(&target),
        None,
        params,
        Some(seq),
    )
    .await?;
    let node = node_in(&mut w.tx, &state.hub, node_id).await?;
    w.commit().await?;
    Ok((node, true))
}

/// The node `node_id` as the API returns it, read on `conn`.
async fn node_in(
    conn: &mut SqliteConnection,
    hub: &NodeHub,
    node_id: &str,
) -> Result<Node, ApiError> {
    let row = sqlx::query(&format!(
        "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?"
    ))
    .bind(node_id)
    .fetch_one(&mut *conn)
    .await?;
    let mut node = node_from_row(&row)?;
    node.tunnel = tunnel_of(&mut *conn, node_id).await?;
    attach_log_position(&mut *conn, hub, std::slice::from_mut(&mut node)).await?;
    Ok(node)
}
