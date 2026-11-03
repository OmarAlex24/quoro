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
    /// Term of the current primary; 1 until elections exist (M6).
    #[schema(minimum = 1)]
    pub term: i64,
    /// When the cluster was created.
    #[schema(format = "date-time")]
    pub created_at: String,
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
