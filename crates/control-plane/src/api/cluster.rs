//! Cluster identity and change-log position: `GET /api/v1/cluster`
//! (MCP tool `get_cluster`); the N+1 indicator: `GET /api/v1/cluster/capacity`
//! (MCP tool `get_capacity`).

use crate::AppState;
use crate::auth::Identity;
use crate::error::ApiError;
use crate::request::{CallCtx, RequestId};
use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Extension, Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The single installation (D-0012). The primary is the source of truth; the
/// change log is the state (D-0015).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "Cluster")]
pub struct Cluster {
    /// Cluster id.
    #[schema(pattern = "^cls_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub id: String,
    /// Cluster name chosen at setup.
    pub name: String,
    /// Version of the running binary.
    pub version: String,
    /// Node that currently holds the primary status, or null while no
    /// machine holds it (`backup-format.md` section 8.2).
    #[schema(required = true, pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub primary_node_id: Option<String>,
    /// Sequence number of the last change-log entry.
    #[schema(minimum = 0)]
    pub log_seq: i64,
    /// Hash of the last change-log entry.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub log_head_hash: String,
    /// Term of the current primary: 1 until the first promotion (ADR 0169).
    #[schema(minimum = 1)]
    pub term: i64,
    /// The voters of the cluster and whether HA is ready (ADR 0170).
    pub votes: ClusterVotes,
    /// Id of the current cluster signing key: the lowercase hex SHA-256 of
    /// its public key (`node-agent-protocol.md` section 5.21).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(pattern = "^[0-9a-f]{64}$", required = false, nullable = false)]
    pub signing_key_id: Option<String>,
    /// The running signing-key rotation, or null; absent until signing-key
    /// rotation ships (`node-agent-protocol.md` section 7.5).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(required = false)]
    pub signing_key_rotation: Option<Option<SigningKeyRotation>>,
    /// When the cluster was created.
    #[schema(format = "date-time")]
    pub created_at: String,
}

/// A running rotation of the cluster signing key (`openapi.yaml`
/// `SigningKeyRotation`, ADR 0154).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "SigningKeyRotation",
    description = "A running rotation of the cluster signing key (ADR 0154, node-agent-protocol.md section 7.5)."
)]
pub struct SigningKeyRotation {
    /// Id of the next signing key.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub next_key_id: String,
    /// When the rotation started.
    #[schema(format = "date-time")]
    pub started_at: String,
    /// Machines that still pin the current key, oldest enrolled first.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub pending_node_ids: Vec<String>,
}

/// The voters of the cluster (`openapi.yaml` `ClusterVotes`, ADR 0170).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "ClusterVotes")]
pub struct ClusterVotes {
    /// Every voter, oldest enrolled first.
    pub voters: Vec<ClusterVoter>,
    /// Number of voters.
    #[schema(minimum = 0)]
    pub count: i64,
    /// Voters that HA needs; 3 in M5b (ADR 0017).
    #[schema(minimum = 1)]
    pub required: i64,
    /// Whether `count` reaches `required` and the primary holds the support of
    /// a majority of voters (ADR 0169).
    pub ha_ready: bool,
}

/// One voter of the cluster (`openapi.yaml` `ClusterVoter`).
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[schema(title = "ClusterVoter")]
pub struct ClusterVoter {
    /// The voting machine.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub node_id: String,
    /// The machine's roles; at least one of them is `node` or `witness`.
    #[schema(inline)]
    pub roles: Vec<crate::api::nodes::NodeRole>,
    /// Whether the machine's node-agent session is up now.
    pub connected: bool,
}

/// Read cluster identity and change-log position.
#[utoipa::path(
    get,
    path = "/api/v1/cluster",
    tag = "cluster",
    operation_id = "getCluster",
    extensions(
        ("x-quoro-scope" = json!("cluster:read")),
        ("x-quoro-mcp-tool" = json!("get_cluster")),
    ),
    responses(
        (status = 200, description = "The cluster.", body = Cluster,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 401, response = crate::openapi::Unauthenticated),
        (status = 403, response = crate::openapi::Forbidden),
        (status = 503, response = crate::openapi::NotPrimary),
        (status = 429, response = crate::openapi::RateLimited),
    )
)]
///
/// # Errors
///
/// `forbidden` without `cluster:read`, or a database error.
pub async fn get_cluster(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
) -> Result<Json<Cluster>, ApiError> {
    let ctx = CallCtx::new(identity, request_id, "GET /api/v1/cluster", &headers);
    let cluster = crate::service::cluster::get_cluster(&state, &ctx).await?;
    Ok(Json(cluster))
}

/// The N+1 indicator (`openapi.yaml` `ClusterCapacity`, ADR 0021, ADR 0178).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "ClusterCapacity",
    description = "The N+1 indicator: whether the apps of one node would fit on the others if that node were lost, or which resources are missing (ADR 0021, ADR 0178). The primary simulates the loss with the allocation it holds now; nothing is reserved."
)]
pub struct ClusterCapacity {
    /// The node whose loss was simulated.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub lost_node_id: String,
    /// Whether every listed app that needs a node got one, that is no app has
    /// `reason` `no_capacity`.
    pub fits: bool,
    /// The resources of the apps left without a node for lack of capacity.
    pub missing: CapacityShortfall,
    /// One entry per app with an instance meant to run on the lost node, or
    /// whose volume is there; in placement order.
    pub apps: Vec<ClusterCapacityApp>,
    /// When the primary computed the answer.
    #[schema(format = "date-time")]
    pub computed_at: String,
}

/// The resources of the apps left without a node for lack of capacity,
/// summed (`openapi.yaml` `CapacityShortfall`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "CapacityShortfall",
    description = "The resources of the apps left without a node for lack of capacity, summed; all zero when `fits` is true."
)]
pub struct CapacityShortfall {
    /// vCPUs.
    #[schema(minimum = 0)]
    pub cpus: i64,
    /// Memory in mebibytes.
    #[schema(minimum = 0)]
    pub memory_mb: i64,
    /// Disk in gibibytes.
    #[schema(minimum = 0)]
    pub disk_gb: i64,
}

/// What one app of the lost node would need (`openapi.yaml`
/// `ClusterCapacityApp`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(
    title = "ClusterCapacityApp",
    description = "What one app of the lost node would need."
)]
pub struct ClusterCapacityApp {
    /// The app.
    #[schema(pattern = "^app_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub app_id: String,
    /// The app's `ha_policy`.
    #[schema(inline)]
    pub policy: crate::api::apps::HaPolicy,
    /// `keep`, `start`, `failover` or `restore`.
    #[schema(inline)]
    pub action: CapacityAction,
    /// Where the app would go; the node it keeps running on for `keep`. Null
    /// when it gets no node.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$", required = true)]
    pub node_id: Option<String>,
    /// Why `node_id` is null, or null.
    #[schema(inline, required = true)]
    pub reason: Option<CapacityReason>,
}

/// What a lost node's app would do (`ClusterCapacityApp.action`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CapacityAction {
    /// A stateless app that still has an instance on another node.
    Keep,
    /// A stateless app whose only instance was on the lost node.
    Start,
    /// A `warm` app whose source is the lost node.
    Failover,
    /// A stateful `none` or `cold` app, restored from its latest backup.
    Restore,
}

/// Why an app gets no node (`ClusterCapacityApp.reason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CapacityReason {
    /// No remaining node has room.
    NoCapacity,
    /// A stateful app with no succeeded backup.
    NoBackup,
    /// The warm node holds no complete copy.
    NoWarmCopy,
}

/// Query of `getClusterCapacity`. `lost_node_id` is required by the contract;
/// it is optional here so a missing one answers `validation_failed` like a
/// malformed one, through the service, on HTTP and MCP alike.
#[derive(Debug, Default, Deserialize)]
pub struct ClusterCapacityQuery {
    /// The node whose loss is simulated (`nod_...`).
    pub lost_node_id: Option<String>,
}

/// Simulate the loss of a node (the N+1 indicator).
#[utoipa::path(
    get,
    path = "/api/v1/cluster/capacity",
    tag = "cluster",
    operation_id = "getClusterCapacity",
    extensions(
        ("x-quoro-scope" = json!("cluster:read")),
        ("x-quoro-mcp-tool" = json!("get_capacity")),
    ),
    params(
        ("lost_node_id" = String, Query,
            description = "The node whose loss the N+1 indicator simulates (`nod_...`).",
            pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$"),
    ),
    responses(
        (status = 200, description = "The simulated loss.", body = ClusterCapacity,
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
/// `forbidden` without `cluster:read`, `validation_failed` without a
/// well-formed `lost_node_id`, `not_found` for an unknown or removed node, or
/// a database error.
pub async fn get_cluster_capacity(
    State(state): State<AppState>,
    identity: Identity,
    headers: HeaderMap,
    Extension(RequestId(request_id)): Extension<RequestId>,
    query: Result<Query<ClusterCapacityQuery>, QueryRejection>,
) -> Result<Json<ClusterCapacity>, ApiError> {
    let ctx = CallCtx::new(
        identity,
        request_id,
        "GET /api/v1/cluster/capacity",
        &headers,
    );
    let Query(query) = query.map_err(|rejection| ApiError::validation(rejection.body_text()))?;
    let capacity =
        crate::service::cluster::get_cluster_capacity(&state, &ctx, query.lost_node_id.as_deref())
            .await?;
    Ok(Json(capacity))
}
