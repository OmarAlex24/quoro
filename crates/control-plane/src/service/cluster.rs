//! Cluster read (`GET /api/v1/cluster`; MCP tool `get_cluster`) and the N+1
//! indicator (`GET /api/v1/cluster/capacity`; MCP tool `get_capacity`).

use crate::AppState;
use crate::api::cluster::{Cluster, ClusterCapacity, ClusterVoter, ClusterVotes};
use crate::api::nodes::NodeRole;
use crate::capacity::{self, Loaded};
use crate::error::ApiError;
use crate::request::{CallCtx, Resource};
use crate::store::{GENESIS_HASH, now_rfc3339, read_term};
use quoro_types::{Prefix, Scope, is_id};
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
        // The term `meta` stores, read as every other reader of the term
        // reads it. A stored value that holds no term is an internal error,
        // so the answer never carries a term above the ceiling
        // (`node-agent-protocol.md` section 8.8).
        let term = read_term(state.store.pool())
            .await
            .map_err(|error| ApiError::internal(&error))?;
        Ok(Cluster {
            id: state.cluster_id.clone(),
            name,
            version: env!("CARGO_PKG_VERSION").to_string(),
            primary_node_id,
            log_seq,
            log_head_hash,
            term,
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

/// Simulates the loss of `lost_node_id` (the N+1 indicator, ADR 0178): whether
/// its apps fit on the other nodes, or which resources are missing
/// ([`capacity`]). A human read is not audited; an agent read is.
///
/// Under `cluster:read` with a narrower selector than `{"all": true}`, the
/// answer lists only the apps the selector matches (permission-model.md
/// section 5.1), and `fits` and `missing` are worked out from those entries,
/// so an app the caller may not see never shows through them. The simulation
/// itself places every app, so a visible app gets the node it would really
/// get.
///
/// # Errors
///
/// `forbidden` without `cluster:read` or under a `{}` selector,
/// `validation_failed` without a well-formed `lost_node_id`, `not_found` for
/// an unknown or removed node, or a database error.
pub async fn get_cluster_capacity(
    state: &AppState,
    ctx: &CallCtx,
    lost_node_id: Option<&str>,
) -> Result<ClusterCapacity, ApiError> {
    let params = lost_node_id.map_or_else(|| json!({}), |id| json!({ "lost_node_id": id }));
    let access = ctx
        .authorize_read(state, Scope::ClusterRead, None, &params)
        .await?;
    let result: Result<_, ApiError> = async {
        if access.reads_nothing() {
            return Err(ApiError::forbidden(
                crate::error::Reason::SelectorMismatch,
                Some(Scope::ClusterRead.as_str()),
            ));
        }
        let lost = match lost_node_id {
            None => return Err(ApiError::validation("lost_node_id is required")),
            Some(id) if !is_id(Prefix::Node, id) => {
                return Err(ApiError::validation(
                    "lost_node_id is not a node id (`nod_...`)",
                ));
            }
            Some(id) => id,
        };
        let mut tx = state.store.pool().begin().await?;
        let loaded = capacity::load(&mut tx, state, lost).await?;
        drop(tx);
        let Loaded::Inputs { rooms, apps } = loaded else {
            return Err(ApiError::not_found("node"));
        };
        let entries = capacity::simulate(lost, &rooms, &apps);
        let entries = access
            .filter(state, entries, |entry| Resource {
                kind: "app",
                id: &entry.app.app_id,
                app_id: Some(&entry.app.app_id),
                project_id: Some(&entry.project_id),
                node_id: None,
            })
            .await?;
        let (fits, missing) = capacity::summarize(&entries);
        Ok(ClusterCapacity {
            lost_node_id: lost.to_string(),
            fits,
            missing,
            apps: entries.into_iter().map(|entry| entry.app).collect(),
            computed_at: now_rfc3339(),
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
