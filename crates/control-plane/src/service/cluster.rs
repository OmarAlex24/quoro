//! Cluster read (`GET /api/v1/cluster`; MCP tool `get_cluster`).

use crate::AppState;
use crate::api::cluster::{Cluster, ClusterVoter, ClusterVotes};
use crate::api::nodes::NodeRole;
use crate::error::ApiError;
use crate::request::CallCtx;
use crate::store::{GENESIS_HASH, now_rfc3339};
use quoro_types::Scope;
use serde_json::json;

/// Reads cluster identity and the change-log position. A human read is not
/// audited; an agent read is (audit-log-format.md section 3).
///
/// # Errors
///
/// `forbidden` without `cluster:read`, or a database error.
pub async fn get_cluster(state: &AppState, ctx: &CallCtx) -> Result<Cluster, ApiError> {
    let access = ctx
        .authorize_read(state, Scope::ClusterRead, None, &json!({}))
        .await?;
    let result: Result<_, ApiError> = async {
        // Section 5.1: matched by any non-empty selector, so `{}` alone reads nothing.
        if access.reads_nothing() {
            return Err(ApiError::forbidden(
                crate::error::Reason::SelectorMismatch,
                Some(Scope::ClusterRead.as_str()),
            ));
        }
        let name: Option<(String,)> =
            sqlx::query_as("SELECT value FROM meta WHERE key = 'cluster_name'")
                .fetch_optional(state.store.pool())
                .await?;
        // Setup writes no `cluster_name` key yet: fall back to the default
        // name rather than failing the endpoint.
        let name = name.map_or_else(|| "quoro".to_string(), |(value,)| value);
        let created_at: Option<(String,)> =
            sqlx::query_as("SELECT value FROM meta WHERE key = 'created_at'")
                .fetch_optional(state.store.pool())
                .await?;
        // The cluster is born at setup, so the first user's creation time is
        // the best available creation time when `meta` does not record one.
        let created_at = match created_at {
            Some((value,)) => value,
            None => {
                let first: Option<(String,)> = sqlx::query_as("SELECT MIN(created_at) FROM users")
                    .fetch_optional(state.store.pool())
                    .await?;
                first.map(|(value,)| value).unwrap_or_else(now_rfc3339)
            }
        };
        // The primary marks itself in `nodes.cp_status` at first start. If no
        // node is marked yet, fall back to the first enrolled node; a cluster
        // with no nodes at all reports an empty id.
        let primary: Option<(String,)> =
            sqlx::query_as("SELECT id FROM nodes WHERE cp_status = 'primary'")
                .fetch_optional(state.store.pool())
                .await?;
        let primary_node_id = match primary {
            Some((id,)) => id,
            None => {
                let first: Option<(String,)> =
                    sqlx::query_as("SELECT id FROM nodes ORDER BY created_at, id LIMIT 1")
                        .fetch_optional(state.store.pool())
                        .await?;
                first.map_or_else(String::new, |(id,)| id)
            }
        };
        let head: Option<(i64, String)> =
            sqlx::query_as("SELECT seq, hash FROM change_log ORDER BY seq DESC LIMIT 1")
                .fetch_optional(state.store.pool())
                .await?;
        let (log_seq, log_head_hash) = head.unwrap_or_else(|| (0, GENESIS_HASH.to_string()));
        Ok(Cluster {
            id: state.cluster_id.clone(),
            name,
            version: env!("CARGO_PKG_VERSION").to_string(),
            primary_node_id,
            log_seq,
            log_head_hash,
            // No promotion exists before the M5b promotion work; the only
            // term so far is 1.
            term: 1,
            votes: votes(state).await?,
            // The current signing key (section 5.21). No rotation runs
            // before signing-key rotation ships (#76), so its field stays
            // absent.
            signing_key_id: Some(state.keys.key_id().to_string()),
            signing_key_rotation: None,
            created_at,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Voters HA needs (ADR 0017, ADR 0170).
const REQUIRED_VOTES: i64 = 3;

/// The voters of the cluster (`Cluster.votes`): every machine with the `node`
/// or `witness` role that is not removed, connected or not, oldest enrolled
/// first (ADR 0170). `spec.votes.voters` lists them in the same order
/// ([`crate::voters`]).
///
/// `ha_ready` is true while `count` is at least `required` and a majority of
/// the voters supports this primary for `D` right now, so it can grant and
/// renew `expiring` leases (`node-agent-protocol.md` section 8.8).
///
/// # Errors
///
/// A database error, or a `roles` column that is not a list of roles.
async fn votes(state: &AppState) -> Result<ClusterVotes, ApiError> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT id, roles, status FROM nodes WHERE status <> 'removed' ORDER BY created_at, id",
    )
    .fetch_all(state.store.pool())
    .await?;
    let mut voters = Vec::new();
    for (node_id, roles, status) in rows {
        let roles: Vec<NodeRole> = serde_json::from_str(&roles)
            .map_err(|e| ApiError::internal(&format!("nodes.roles is invalid: {e}")))?;
        if roles
            .iter()
            .any(|role| matches!(role, NodeRole::Node | NodeRole::Witness))
        {
            voters.push(ClusterVoter {
                node_id,
                roles,
                connected: status == "connected",
            });
        }
    }
    let count = i64::try_from(voters.len())
        .map_err(|e| ApiError::internal(&format!("voter count: {e}")))?;
    let ha_ready = count >= REQUIRED_VOTES
        && state
            .hub
            .authority()
            .supports(crate::authority::DEFAULT_DURATION_MS);
    Ok(ClusterVotes {
        voters,
        count,
        required: REQUIRED_VOTES,
        ha_ready,
    })
}
