//! App log reads (`GET /api/v1/apps/{app_id}/logs`; MCP tool `get_app_logs`).

use crate::AppState;
use crate::api::apps::app_target;
use crate::api::logs::{
    AppLogsQuery, LogList, NODE_TIMEOUT, app_vms, command_error, distinct_nodes, hub_error,
    log_entry, require_app, selector, tail_arg, validate_instance,
};
use crate::error::ApiError;
use crate::request::CallCtx;
use quoro_types::Scope;
use serde_json::{Map, Value, json};

/// The path and query arguments of a log read, as audit params.
fn read_params(app_id: &str, query: &AppLogsQuery) -> Value {
    let mut params = Map::new();
    params.insert("app_id".to_string(), json!(app_id));
    if let Some(since) = &query.since {
        params.insert("since".to_string(), json!(since));
    }
    if let Some(until) = &query.until {
        params.insert("until".to_string(), json!(until));
    }
    if let Some(tail) = query.tail {
        params.insert("tail".to_string(), json!(tail));
    }
    if let Some(instance) = &query.instance {
        params.insert("instance".to_string(), json!(instance));
    }
    Value::Object(params)
}

/// The requested log lines of an app, read on demand from the nodes that run
/// its instances (ADR 0014).
///
/// # Errors
///
/// `unauthenticated`/`forbidden` without `app:logs`, `not_found` for an unknown
/// app, `validation_failed` for a bad `tail` or an `instance` outside the app,
/// or `node_unreachable` when a node holding the app does not answer.
pub async fn get_app_logs(
    state: &AppState,
    ctx: &CallCtx,
    app_id: &str,
    query: &AppLogsQuery,
) -> Result<LogList, ApiError> {
    let params = read_params(app_id, query);
    let target = app_target(app_id);
    let access = ctx
        .authorize_read(state, Scope::AppLogs, Some(&target), &params)
        .await?;
    let result: Result<LogList, ApiError> = async {
        require_app(state, app_id).await?;
        let tail = tail_arg(query.tail, 200, 1)?;
        let instance = validate_instance(state, app_id, query.instance.as_deref()).await?;
        let vms = app_vms(state, app_id).await?;
        let selected: Vec<&(String, String)> = match &instance {
            Some(instance) => vms.iter().filter(|(id, _)| id == instance).collect(),
            None => vms.iter().collect(),
        };
        if selected.is_empty() {
            return Ok(LogList { items: Vec::new() });
        }
        let fallback = if selected.len() == 1 {
            Some(selected[0].0.clone())
        } else {
            None
        };
        let nodes = distinct_nodes(&selected);

        let mut params = selector(app_id, instance.as_deref());
        params.insert("tail".to_string(), json!(tail));
        if let Some(since) = &query.since {
            params.insert("since".to_string(), json!(since));
        }
        if let Some(until) = &query.until {
            params.insert("until".to_string(), json!(until));
        }

        let mut items = Vec::new();
        for node in &nodes {
            let result = state
                .hub
                .command(
                    node,
                    "logs_query",
                    Value::Object(params.clone()),
                    NODE_TIMEOUT,
                )
                .await
                .map_err(hub_error)?;
            if result.status == quoro_proto::CommandStatus::Error {
                return Err(command_error(result));
            }
            for line in result.lines.unwrap_or_default() {
                if let Some(entry) = log_entry(&line, instance.as_deref(), fallback.as_deref()) {
                    items.push(entry);
                }
            }
        }
        items.sort_by(|a, b| a.time.cmp(&b.time));
        let tail = usize::try_from(tail).unwrap_or(usize::MAX);
        if items.len() > tail {
            items.drain(0..items.len() - tail);
        }
        Ok(LogList { items })
    }
    .await;
    ctx.finish_read(state, access, result).await
}
