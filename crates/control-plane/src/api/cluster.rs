//! Cluster identity and change-log position: `GET /api/v1/cluster`
//! (MCP tool `get_cluster`).

use crate::AppState;
use crate::auth::Identity;
use crate::error::ApiError;
use crate::request::{CallCtx, RequestId};
use axum::Json;
use axum::extract::{Extension, State};
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
    /// Node that currently holds the primary status.
    #[schema(pattern = "^nod_[0-9A-HJKMNP-TV-Z]{26}$")]
    pub primary_node_id: String,
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
    /// When the cluster was created.
    #[schema(format = "date-time")]
    pub created_at: String,
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
