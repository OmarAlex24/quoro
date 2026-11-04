// SPDX-License-Identifier: AGPL-3.0-only
//! Instance placement for apps with replicas (ADR 0142).
//!
//! An app runs at most one instance per node. The scheduler chooses the nodes
//! an app's instances go to and never under-places: when fewer eligible nodes
//! are available than a rollout or a scale needs, it refuses with
//! `validation_failed` instead of silently placing fewer instances.
//!
//! * **Eligible node.** A row that is not `removed` and whose `roles` array
//!   contains `node`, narrowed to `placement.node_ids` when that list is not
//!   empty. Eligibility does not depend on the node's status, so validation can
//!   count nodes a disconnected one would not serve.
//! * **Candidate node.** An eligible node whose status is `connected`.
//! * **Order.** Nodes that already run a VM of the app come first, so a
//!   rollout replaces the instances of the current deployment in a stable
//!   order; inside each group nodes are ordered by id.

use crate::api::apps::AppPlacement;
use crate::error::ApiError;
use sqlx::{Row, SqliteConnection};
use std::collections::HashSet;

/// The nodes a placement makes eligible for an app, ordered by id.
///
/// # Errors
///
/// A database error.
pub(crate) async fn eligible_node_ids(
    conn: &mut SqliteConnection,
    placement: &AppPlacement,
) -> Result<Vec<String>, ApiError> {
    let rows: Vec<(String,)> = if placement.node_ids.is_empty() {
        sqlx::query_as(
            "SELECT id FROM nodes WHERE status != 'removed' \
             AND EXISTS (SELECT 1 FROM json_each(nodes.roles) WHERE json_each.value = 'node') \
             ORDER BY id",
        )
        .fetch_all(&mut *conn)
        .await?
    } else {
        let placeholders = vec!["?"; placement.node_ids.len()].join(", ");
        let sql = format!(
            "SELECT id FROM nodes WHERE status != 'removed' AND id IN ({placeholders}) \
             AND EXISTS (SELECT 1 FROM json_each(nodes.roles) WHERE json_each.value = 'node') \
             ORDER BY id"
        );
        let mut query = sqlx::query_as::<_, (String,)>(&sql);
        for node_id in &placement.node_ids {
            query = query.bind(node_id);
        }
        query.fetch_all(&mut *conn).await?
    };
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// The nodes an app's instances may be placed on now: connected eligible
/// nodes, those already running a VM of the app first, each group by id.
///
/// # Errors
///
/// A database error.
pub(crate) async fn candidate_nodes(
    conn: &mut SqliteConnection,
    app_id: &str,
    placement: &AppPlacement,
) -> Result<Vec<String>, ApiError> {
    let eligible = eligible_node_ids(conn, placement).await?;
    let connected: HashSet<String> = sqlx::query("SELECT id FROM nodes WHERE status = 'connected'")
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("id"))
        .collect();
    let hosting: HashSet<String> = sqlx::query("SELECT DISTINCT node_id FROM vms WHERE app_id = ?")
        .bind(app_id)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("node_id"))
        .collect();

    let mut with_app = Vec::new();
    let mut without_app = Vec::new();
    for node_id in eligible {
        if !connected.contains(&node_id) {
            continue;
        }
        if hosting.contains(&node_id) {
            with_app.push(node_id);
        } else {
            without_app.push(node_id);
        }
    }
    with_app.extend(without_app);
    Ok(with_app)
}

/// The nodes to place `replicas` instances of `app_id` on, in order.
///
/// # Errors
///
/// `validation_failed` when fewer candidate nodes are available than
/// `replicas`, or a database error.
pub(crate) async fn place_instances(
    conn: &mut SqliteConnection,
    app_id: &str,
    placement: &AppPlacement,
    replicas: i64,
) -> Result<Vec<String>, ApiError> {
    let candidates = candidate_nodes(conn, app_id, placement).await?;
    if candidates.len() < usize::try_from(replicas).unwrap_or(usize::MAX) {
        return Err(ApiError::validation(format!(
            "replicas {replicas} is above the {} eligible nodes",
            candidates.len()
        )));
    }
    Ok(candidates
        .into_iter()
        .take(usize::try_from(replicas).unwrap_or(usize::MAX))
        .collect())
}
