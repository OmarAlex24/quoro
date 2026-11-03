//! Deployment reads (`GET /api/v1/apps/{app_id}/deployments`, `GET /api/v1/deployments/{deployment_id}`; MCP tools `list_deployments`, `get_deployment`).

use crate::AppState;
use crate::api::deployments::{
    DEPLOYMENT_COLUMNS, Deployment, DeploymentList, ListDeploymentsQuery, deployment_from_row,
};
use crate::api::page;
use crate::deploy;
use crate::error::{ApiError, Target};
use crate::request::{CallCtx, Resource};
use quoro_types::Scope;
use serde_json::{Map, Value, json};

/// The query fields a list call was given, with the path id, as audit params.
fn list_params(app_id: &str, query: &ListDeploymentsQuery) -> Value {
    let mut params = Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    Value::Object(params)
}

/// The project of an app, or `None` when the app does not exist.
async fn app_project(state: &AppState, app_id: &str) -> Result<Option<String>, ApiError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT project_id FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(state.store.pool())
        .await?;
    Ok(row.map(|(project_id,)| project_id))
}

/// Deployments of one app, newest first.
///
/// The cursor is the last id of the previous page and means "ids smaller than
/// the cursor", because the page is ordered `id DESC` (ULIDs sort by time).
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed
/// app id, `validation_failed` for a bad page bound, or a database error.
pub async fn list_deployments(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    query: &ListDeploymentsQuery,
) -> Result<DeploymentList, ApiError> {
    let params = list_params(app_id, query);
    let target = Target {
        kind: "app".to_string(),
        id: app_id.to_string(),
    };
    let access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<DeploymentList, ApiError> = async {
        if !deploy::is_id_with_prefix("app", app_id) {
            return Err(ApiError::not_found("app"));
        }
        let Some(project_id) = app_project(state, app_id).await? else {
            return Err(ApiError::not_found("app"));
        };
        let page_query = query.page();
        let limit = page_query.limit()?;

        let mut sql = format!("SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE app_id = ?");
        if page_query.cursor.is_some() {
            sql.push_str(" AND id < ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        let mut statement = sqlx::query(&sql).bind(app_id);
        if let Some(cursor) = &page_query.cursor {
            statement = statement.bind(cursor);
        }
        let rows = statement
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let deployments = rows
            .iter()
            .map(deployment_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let entries: Vec<(Deployment, String)> = deployments
            .into_iter()
            .map(|deployment| (deployment, project_id.clone()))
            .collect();
        let (entries, next_cursor) =
            page::finish(entries, limit, |(deployment, _)| deployment.id.clone());
        let entries = access
            .filter(state, entries, |(row, project_id)| Resource {
                kind: "deployment",
                id: &row.id,
                app_id: Some(&row.app_id),
                project_id: Some(project_id),
                node_id: None,
            })
            .await?;
        let items = entries
            .into_iter()
            .map(|(deployment, _)| deployment)
            .collect();
        Ok(DeploymentList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One deployment with its status, phase, resolved digest and timestamps.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
pub async fn get_deployment(
    state: &AppState,
    ctx: &CallCtx,
    deployment_id: &str,
) -> Result<Deployment, ApiError> {
    let params = json!({ "deployment_id": deployment_id });
    let target = Target {
        kind: "deployment".to_string(),
        id: deployment_id.to_string(),
    };
    let mut access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<Deployment, ApiError> = async {
        if !deploy::is_id_with_prefix("dep", deployment_id) {
            return Err(ApiError::not_found("deployment"));
        }
        let row = sqlx::query(&format!(
            "SELECT {DEPLOYMENT_COLUMNS} FROM deployments WHERE id = ?"
        ))
        .bind(deployment_id)
        .fetch_optional(state.store.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("deployment"))?;
        let deployment = deployment_from_row(&row)?;
        let project_id = app_project(state, &deployment.app_id).await?;
        access.set_project(project_id.as_deref());
        Ok(deployment)
    }
    .await;
    ctx.finish_read(state, access, result).await
}
