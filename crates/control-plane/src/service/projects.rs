//! Project reads (`GET /api/v1/projects`, `GET /api/v1/projects/{project_id}`; MCP tool `list_projects`).

use crate::AppState;
use crate::api::page::finish;
use crate::api::projects::{PageParams, Project, ProjectList, project_from_row};
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

/// Projects group apps and are the unit a grant's selector can name by slug
/// (D-0030).
///
/// # Errors
///
/// `forbidden` without `project:read`, or a database error.
pub async fn list_projects(
    state: &AppState,
    ctx: &CallCtx,
    page: &PageParams,
) -> Result<ProjectList, ApiError> {
    let params = list_params(page);
    let access = ctx
        .authorize_read(state, Scope::ProjectRead, None, &params)
        .await?;
    let result: Result<ProjectList, ApiError> = async {
        let limit = page.limit()?;
        let rows = sqlx::query("SELECT * FROM projects WHERE id > ? ORDER BY id LIMIT ?")
            .bind(page.after())
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let projects = rows
            .iter()
            .map(project_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_cursor) = finish(projects, limit, |project| project.id.clone());
        let items = access
            .filter(state, items, |project| Resource {
                kind: "project",
                id: &project.id,
                app_id: None,
                project_id: Some(&project.id),
                node_id: None,
            })
            .await?;
        Ok(ProjectList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One project by id.
///
/// # Errors
///
/// `forbidden` without `project:read`, `not_found` for an unknown id, or a
/// database error.
pub async fn get_project(
    state: &AppState,
    ctx: &CallCtx,
    project_id: &str,
) -> Result<Project, ApiError> {
    let params = json!({ "project_id": project_id });
    let target = Target {
        kind: "project".to_string(),
        id: project_id.to_string(),
    };
    let mut access = ctx
        .authorize_read(state, Scope::ProjectRead, Some(&target), &params)
        .await?;
    let result: Result<Project, ApiError> = async {
        if !is_id(Prefix::Project, project_id) {
            return Err(ApiError::not_found("project"));
        }
        let row = sqlx::query("SELECT * FROM projects WHERE id = ?")
            .bind(project_id)
            .fetch_optional(state.store.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("project"))?;
        let project = project_from_row(&row)?;
        access.set_project(Some(&project.id));
        Ok(project)
    }
    .await;
    ctx.finish_read(state, access, result).await
}
