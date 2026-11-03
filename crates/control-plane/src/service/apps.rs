//! App operations (`/api/v1/apps*`; MCP tools `list_apps`, `get_app`,
//! `restart_app`, `redeploy_app`, `delete_app`).

use crate::AppState;
use crate::api::apps::{
    APP_COLUMNS, App, AppDetail, AppList, ListAppsQuery, app_from_row, app_target, app_vms,
    delete_app_tx, redeploy_app_tx, restart_app_tx,
};
use crate::api::deployments::Deployment;
use crate::api::domains::{DOMAIN_COLUMNS, domain_from_row};
use crate::api::page;
use crate::deploy;
use crate::error::ApiError;
use crate::request::{CallCtx, Resource};
use quoro_types::{Prefix, Scope, is_id};
use serde_json::{Map, Value, json};

/// The query fields a list call was given, as audit params.
fn list_params(query: &ListAppsQuery) -> Value {
    let mut params = Map::new();
    if let Some(project_id) = &query.project_id {
        params.insert("project_id".to_string(), json!(project_id));
    }
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    if let Some(cursor) = &query.cursor {
        params.insert("cursor".to_string(), json!(cursor));
    }
    Value::Object(params)
}

/// Apps, optionally filtered by project. Reads are not audited for humans.
///
/// # Errors
///
/// `forbidden` without `app:read`, `validation_failed` for a malformed filter
/// or page bound, or a database error.
pub async fn list_apps(
    state: &AppState,
    ctx: &CallCtx,
    query: &ListAppsQuery,
) -> Result<AppList, ApiError> {
    let params = list_params(query);
    let access = ctx
        .authorize_read(state, Scope::AppRead, None, &params)
        .await?;
    let result: Result<AppList, ApiError> = async {
        if let Some(project_id) = &query.project_id
            && !is_id(Prefix::Project, project_id)
        {
            return Err(ApiError::validation("project_id is not a valid project id"));
        }
        let page_query = query.page();
        let limit = page_query.limit()?;
        let after = page_query.after();

        let mut sql = format!("SELECT {APP_COLUMNS} FROM apps WHERE id > ?");
        if query.project_id.is_some() {
            sql.push_str(" AND project_id = ?");
        }
        sql.push_str(" ORDER BY id LIMIT ?");
        let mut statement = sqlx::query(&sql).bind(after);
        if let Some(project_id) = &query.project_id {
            statement = statement.bind(project_id);
        }
        let rows = statement
            .bind(limit + 1)
            .fetch_all(state.store.pool())
            .await?;
        let apps = rows
            .iter()
            .map(app_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_cursor) = page::finish(apps, limit, |app| app.id.clone());
        let items = access
            .filter(state, items, |app| Resource {
                kind: "app",
                id: &app.id,
                app_id: Some(&app.id),
                project_id: Some(&app.project_id),
                node_id: None,
            })
            .await?;
        Ok(AppList { items, next_cursor })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// One app with its instances (its VMs) and its domains.
///
/// # Errors
///
/// `forbidden` without `app:read`, `not_found` for an unknown or malformed id,
/// or a database error.
pub async fn get_app(state: &AppState, ctx: &CallCtx, app_id: &str) -> Result<AppDetail, ApiError> {
    let params = json!({ "app_id": app_id });
    let target = app_target(app_id);
    let mut access = ctx
        .authorize_read(state, Scope::AppRead, Some(&target), &params)
        .await?;
    let result: Result<AppDetail, ApiError> = async {
        if !deploy::is_id_with_prefix("app", app_id) {
            return Err(ApiError::not_found("app"));
        }
        let mut conn = state.store.pool().acquire().await?;
        let row = sqlx::query(&format!("SELECT {APP_COLUMNS} FROM apps WHERE id = ?"))
            .bind(app_id)
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| ApiError::not_found("app"))?;
        let app = app_from_row(&row)?;
        access.set_project(Some(&app.project_id));
        let instances = app_vms(&mut conn, app_id).await?;
        let rows = sqlx::query(&format!(
            "SELECT {DOMAIN_COLUMNS} FROM domains WHERE app_id = ? ORDER BY id"
        ))
        .bind(app_id)
        .fetch_all(&mut *conn)
        .await?;
        let domains = rows
            .iter()
            .map(domain_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AppDetail {
            app,
            instances,
            domains,
        })
    }
    .await;
    ctx.finish_read(state, access, result).await
}

/// Deletes an app and its data. The authorization path refuses an agent
/// without a matching approval grant with `approval_required`
/// (permission-model.md section 7.2, steps d and g).
///
/// # Errors
///
/// `forbidden` without `app:delete` or with `approval_required` for an agent,
/// `not_found` for an unknown or malformed id, or a database error.
pub async fn delete_app(state: &AppState, ctx: &CallCtx, app_id: &str) -> Result<App, ApiError> {
    let params = json!({ "app_id": app_id });
    let target = app_target(app_id);
    ctx.authorize(state, Scope::AppDelete, Some(&target), None, &params)
        .await?;
    match delete_app_tx(state, ctx, app_id, &params).await {
        Ok((app, vm_ids)) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok(app)
        }
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AppDelete),
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

/// Restarts an app: bumps `restart_generation`, so the node restarts the
/// instances that carry the new generation, and emits `app.restarted`.
///
/// # Errors
///
/// `forbidden` without `app:restart` (or the agent refusals of
/// permission-model.md section 7.4), `not_found` for an unknown or malformed
/// id, or a database error.
pub async fn restart_app(state: &AppState, ctx: &CallCtx, app_id: &str) -> Result<App, ApiError> {
    let params = json!({ "app_id": app_id });
    let target = app_target(app_id);
    ctx.authorize(state, Scope::AppRestart, Some(&target), None, &params)
        .await?;
    match restart_app_tx(state, ctx, app_id, &params).await {
        Ok((app, vm_ids)) => {
            for vm_id in vm_ids {
                state.notify_driver(&vm_id);
            }
            Ok(app)
        }
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AppRestart),
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

/// Rolls out the app's current image again with `trigger: redeploy`.
///
/// # Errors
///
/// `forbidden` without `app:redeploy`, `not_found` for an unknown or malformed
/// id, `validation_failed` when no node is available, or a database error.
pub async fn redeploy_app(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
) -> Result<Deployment, ApiError> {
    let params = json!({ "app_id": app_id });
    let target = app_target(app_id);
    ctx.authorize(state, Scope::AppRedeploy, Some(&target), None, &params)
        .await?;
    match redeploy_app_tx(state, ctx, app_id, &params).await {
        Ok((deployment, created)) => {
            for vm_id in std::iter::once(&created.vm_id).chain(created.retired_vm_ids.iter()) {
                state.notify_driver(vm_id);
            }
            Ok(deployment)
        }
        Err(error) => {
            ctx.audit_error(
                state,
                Some(Scope::AppRedeploy),
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
