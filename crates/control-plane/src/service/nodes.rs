//! Node reads (`GET /api/v1/nodes`, `GET /api/v1/nodes/{node_id}`; MCP tools `list_nodes`, `get_node`).

use crate::AppState;
use crate::api::nodes::{Node, NodeList, PageParams, node_from_row, tunnel_of};
use crate::api::page::finish;
use crate::error::{ApiError, Target};
use crate::request::{CallCtx, Resource};
use quoro_types::{Prefix, Scope, is_id};
use serde_json::{Map, Value, json};

/// The query fields a list call was given, as audit params.
fn list_params(page: &PageParams) -> Value {
    let mut params = Map::new();
    if let Some(limit) = page.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &page.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    Value::Object(params)
}

/// All nodes of the cluster.
///
/// # Errors
///
/// `forbidden` without `node:read`, or a database error.
pub async fn list_nodes(
    state: &AppState,
    ctx: &CallCtx,
    page: &PageParams,
) -> Result<NodeList, ApiError> {
    let params = list_params(page);
    let access = ctx
        .authorize_read(state, Scope::NodeRead, None, &params)
        .await?;
    let result: Result<NodeList, ApiError> = async {
        let limit = page.limit()?;
        let rows = sqlx::query("SELECT * FROM nodes WHERE id > ? ORDER BY id LIMIT ?")
            .bind(page.after())
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let mut nodes = rows
            .iter()
            .map(node_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        for node in &mut nodes {
            node.tunnel = tunnel_of(state.store.pool(), &node.id).await?;
        }
        let (items, next_cursor) = finish(nodes, limit, |node| node.id.clone());
        let items = access
            .filter(state, items, |node| Resource {
                kind: "node",
                id: &node.id,
                app_id: None,
                project_id: None,
                node_id: Some(&node.id),
            })
            .await?;
        Ok(NodeList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One node.
///
/// # Errors
///
/// `forbidden` without `node:read`, `not_found` for an unknown id, or a
/// database error.
pub async fn get_node(state: &AppState, ctx: &CallCtx, node_id: &str) -> Result<Node, ApiError> {
    let params = json!({ "node_id": node_id });
    let target = Target {
        kind: "node".to_string(),
        id: node_id.to_string(),
    };
    let access = ctx
        .authorize_read(state, Scope::NodeRead, Some(&target), &params)
        .await?;
    let result: Result<Node, ApiError> = async {
        if !is_id(Prefix::Node, node_id) {
            return Err(ApiError::not_found("node"));
        }
        let row = sqlx::query("SELECT * FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(state.store.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("node"))?;
        let mut node = node_from_row(&row)?;
        node.tunnel = tunnel_of(state.store.pool(), node_id).await?;
        Ok(node)
    }
    .await;
    ctx.finish_read(state, access, result).await
}
