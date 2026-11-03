//! The tool table and dispatch.
//!
//! Each [`ToolSpec`] mirrors one operation of the frozen `openapi.yaml`: its
//! name, operation id, the scope of the README "MCP tools" table, the summary
//! as title and an input schema derived from the operation's parameters. The
//! server is a thin adapter: [`call`] validates the shape of the arguments,
//! builds a [`CallCtx`] with `Channel::Mcp` and hands the call to the same
//! service function as the HTTP endpoint. Authorization, the justification
//! rule and the audit record all happen there.

use crate::args;
use crate::problem;
use quoro_control_plane::AppState;
use quoro_control_plane::api;
use quoro_control_plane::audit::Channel;
use quoro_control_plane::auth::Identity;
use quoro_control_plane::error::ApiError;
use quoro_control_plane::request::CallCtx;
use quoro_control_plane::service;
use quoro_types::{Prefix, new_id};
use rmcp::ErrorData;
use rmcp::model::{CallToolResult, JsonObject, MetaObject, Tool, ToolAnnotations};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::sync::Arc;

/// One wired tool: the contract operation behind it and how it behaves.
pub struct ToolSpec {
    /// The tool name clients call (`restart_app`).
    pub name: &'static str,
    /// The OpenAPI `operationId` (`restartApp`).
    pub operation_id: &'static str,
    /// The scope the matching endpoint needs (`app:restart`).
    pub scope: &'static str,
    /// Whether the operation carries the `Quoro-Justification` header.
    pub mutating: bool,
    /// Whether the operation is destructive (`delete_app` only).
    pub destructive: bool,
    /// The operation's OpenAPI `summary`.
    pub title: &'static str,
    /// Builds the tool's input schema from the operation's parameters.
    pub input_schema: fn() -> Value,
}

/// Tools whose service functions land later. Calling one is an unknown tool.
pub const PENDING: &[&str] = &[
    "list_events",
    "list_incidents",
    "get_incident",
    "resolve_incident",
    "request_approval",
    "get_approval",
    "list_audit",
];

/// The wired tools, in the order of the README "MCP tools" table.
const SPECS: &[ToolSpec] = &[
    ToolSpec {
        name: "get_cluster",
        operation_id: "getCluster",
        scope: "cluster:read",
        mutating: false,
        destructive: false,
        title: "Read cluster identity and change-log position",
        input_schema: schema_get_cluster,
    },
    ToolSpec {
        name: "list_nodes",
        operation_id: "listNodes",
        scope: "node:read",
        mutating: false,
        destructive: false,
        title: "List nodes",
        input_schema: schema_list_nodes,
    },
    ToolSpec {
        name: "get_node",
        operation_id: "getNode",
        scope: "node:read",
        mutating: false,
        destructive: false,
        title: "Read one node",
        input_schema: schema_get_node,
    },
    ToolSpec {
        name: "list_projects",
        operation_id: "listProjects",
        scope: "project:read",
        mutating: false,
        destructive: false,
        title: "List projects",
        input_schema: schema_list_projects,
    },
    ToolSpec {
        name: "list_apps",
        operation_id: "listApps",
        scope: "app:read",
        mutating: false,
        destructive: false,
        title: "List apps",
        input_schema: schema_list_apps,
    },
    ToolSpec {
        name: "get_app",
        operation_id: "getApp",
        scope: "app:read",
        mutating: false,
        destructive: false,
        title: "Read one app with its instances and domains",
        input_schema: schema_get_app,
    },
    ToolSpec {
        name: "list_deployments",
        operation_id: "listAppDeployments",
        scope: "app:read",
        mutating: false,
        destructive: false,
        title: "List deployments of an app",
        input_schema: schema_list_deployments,
    },
    ToolSpec {
        name: "get_deployment",
        operation_id: "getDeployment",
        scope: "app:read",
        mutating: false,
        destructive: false,
        title: "Read one deployment",
        input_schema: schema_get_deployment,
    },
    ToolSpec {
        name: "get_app_logs",
        operation_id: "getAppLogs",
        scope: "app:logs",
        mutating: false,
        destructive: false,
        title: "Read an app's logs",
        input_schema: schema_get_app_logs,
    },
    ToolSpec {
        name: "restart_app",
        operation_id: "restartApp",
        scope: "app:restart",
        mutating: true,
        destructive: false,
        title: "Restart an app's instances",
        input_schema: schema_restart_app,
    },
    ToolSpec {
        name: "redeploy_app",
        operation_id: "redeployApp",
        scope: "app:redeploy",
        mutating: true,
        destructive: false,
        title: "Roll out the current image again",
        input_schema: schema_redeploy_app,
    },
    ToolSpec {
        name: "delete_app",
        operation_id: "deleteApp",
        scope: "app:delete",
        mutating: true,
        destructive: true,
        title: "Delete an app and its data",
        input_schema: schema_delete_app,
    },
];

/// Every wired tool spec.
#[must_use]
pub fn specs() -> &'static [ToolSpec] {
    SPECS
}

// ---------------------------------------------------------------------------
// Input schemas
//
// Derived from `openapi.yaml`: the operation's path parameters, then its query
// parameters, with each parameter's schema copied verbatim plus its
// description. `Quoro-Justification` becomes the required `justification`
// property of a mutating tool, `Quoro-Incident` the optional `incident_id`;
// every other header is skipped. `required` lists the path parameters, the
// required query parameters and `justification`.
// ---------------------------------------------------------------------------

/// Assembles an input schema object. `required` is omitted when empty.
fn schema(properties: Vec<(&str, Value)>, required: &[&str]) -> Value {
    let mut props = JsonObject::new();
    for (name, property) in properties {
        props.insert(name.to_string(), property);
    }
    let mut object = JsonObject::new();
    object.insert("type".to_string(), json!("object"));
    object.insert("properties".to_string(), Value::Object(props));
    if !required.is_empty() {
        object.insert("required".to_string(), json!(required));
    }
    object.insert("additionalProperties".to_string(), json!(false));
    Value::Object(object)
}

/// The path parameter `node_id` (`components/parameters/NodeId`).
fn node_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^nod_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "Node id (`nod_...`)."
    })
}

/// The path parameter `app_id` (`components/parameters/AppId`).
fn app_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^app_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "App id (`app_...`)."
    })
}

/// The path parameter `deployment_id` (`components/parameters/DeploymentId`).
fn deployment_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^dep_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "Deployment id (`dep_...`)."
    })
}

/// The query parameter `limit` (`components/parameters/Limit`).
fn limit() -> Value {
    json!({
        "type": "integer",
        "minimum": 1,
        "maximum": 500,
        "default": 50,
        "description": "Maximum number of items to return (default 50, maximum 500)."
    })
}

/// The query parameter `cursor` (`components/parameters/Cursor`).
fn cursor() -> Value {
    json!({
        "type": "string",
        "description": "Opaque cursor returned as `next_cursor` by the previous page."
    })
}

/// The `Quoro-Justification` header as the required `justification` property.
fn justification() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 2000,
        "description": "The agent's stated reason for the call. Required when the caller is an agent; it is recorded in the audit log (D-0031). Human callers may omit it."
    })
}

/// The `Quoro-Incident` header as the optional `incident_id` property.
fn incident_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^inc_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "The incident (`inc_...`) the call belongs to, when an agent acts on an incident (D-0032). Recorded in the audit log and used to evaluate incident grants (D-0030)."
    })
}

fn schema_get_cluster() -> Value {
    schema(vec![], &[])
}

fn schema_list_nodes() -> Value {
    schema(vec![("limit", limit()), ("cursor", cursor())], &[])
}

fn schema_get_node() -> Value {
    schema(vec![("node_id", node_id())], &["node_id"])
}

fn schema_list_projects() -> Value {
    schema(vec![("limit", limit()), ("cursor", cursor())], &[])
}

fn schema_list_apps() -> Value {
    schema(
        vec![
            (
                "project_id",
                json!({
                    "type": "string",
                    "pattern": "^prj_[0-9A-HJKMNP-TV-Z]{26}$",
                    "description": "Only apps of this project."
                }),
            ),
            ("limit", limit()),
            ("cursor", cursor()),
        ],
        &[],
    )
}

fn schema_get_app() -> Value {
    schema(vec![("app_id", app_id())], &["app_id"])
}

fn schema_list_deployments() -> Value {
    schema(
        vec![
            ("app_id", app_id()),
            ("limit", limit()),
            ("cursor", cursor()),
        ],
        &["app_id"],
    )
}

fn schema_get_deployment() -> Value {
    schema(vec![("deployment_id", deployment_id())], &["deployment_id"])
}

fn schema_get_app_logs() -> Value {
    schema(
        vec![
            ("app_id", app_id()),
            (
                "since",
                json!({
                    "type": "string",
                    "format": "date-time",
                    "description": "Only lines recorded at or after this time."
                }),
            ),
            (
                "until",
                json!({
                    "type": "string",
                    "format": "date-time",
                    "description": "Only lines recorded at or before this time."
                }),
            ),
            (
                "tail",
                json!({
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5000,
                    "default": 200,
                    "description": "Maximum number of trailing lines to return."
                }),
            ),
            (
                "instance",
                json!({
                    "type": "string",
                    "pattern": "^vm_[0-9A-HJKMNP-TV-Z]{26}$",
                    "description": "Only lines of this VM (an instance of the app)."
                }),
            ),
        ],
        &["app_id"],
    )
}

fn schema_restart_app() -> Value {
    schema(
        vec![
            ("app_id", app_id()),
            ("justification", justification()),
            ("incident_id", incident_id()),
        ],
        &["app_id", "justification"],
    )
}

fn schema_redeploy_app() -> Value {
    schema(
        vec![
            ("app_id", app_id()),
            ("justification", justification()),
            ("incident_id", incident_id()),
        ],
        &["app_id", "justification"],
    )
}

fn schema_delete_app() -> Value {
    schema(
        vec![
            ("app_id", app_id()),
            ("justification", justification()),
            ("incident_id", incident_id()),
        ],
        &["app_id", "justification"],
    )
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

/// The tools as MCP sees them: name, title, description, schema and hints.
#[must_use]
pub fn list() -> Vec<Tool> {
    specs()
        .iter()
        .map(|spec| {
            let Value::Object(schema) = (spec.input_schema)() else {
                unreachable!("a tool schema is always a JSON object")
            };
            Tool::new(spec.name, description(spec), Arc::new(schema))
                .with_title(spec.title)
                .with_annotations(annotations(spec))
        })
        .collect()
}

/// `"<summary>. Scope <scope>."`, plus the justification reminder for a
/// mutating tool.
fn description(spec: &ToolSpec) -> String {
    let mut description = format!("{}. Scope {}.", spec.title, spec.scope);
    if spec.mutating {
        description
            .push_str(" Requires justification; pass incident_id when acting on an incident.");
    }
    description
}

/// What MCP clients may assume about a call: reads are read-only and
/// idempotent; restart and redeploy are neither; delete is idempotent and
/// destructive. Every tool stays within Quoro's closed world.
fn annotations(spec: &ToolSpec) -> ToolAnnotations {
    if spec.mutating {
        ToolAnnotations::new()
            .read_only(false)
            .destructive(spec.destructive)
            .idempotent(spec.destructive)
            .open_world(false)
    } else {
        ToolAnnotations::new()
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false)
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Runs one tool call.
///
/// The tool arguments are validated for shape only; authorization, the
/// justification rule and the audit record belong to the service function.
///
/// # Errors
///
/// A JSON-RPC error for an unknown tool (including a [`PENDING`] one).
pub async fn call(
    state: &AppState,
    identity: Identity,
    name: &str,
    arguments: Option<JsonObject>,
) -> Result<CallToolResult, ErrorData> {
    let spec = specs()
        .iter()
        .find(|spec| spec.name == name)
        .ok_or_else(|| ErrorData::invalid_params(format!("unknown tool {name}"), None))?;
    let arguments = arguments.unwrap_or_default();
    let Value::Object(schema) = (spec.input_schema)() else {
        unreachable!("a tool schema is always a JSON object")
    };
    if let Err(reason) = args::validate(&schema, &arguments) {
        return Ok(problem::tool_error(&ApiError::validation(reason)));
    }

    let (justification, incident_id) = if spec.mutating {
        (
            text(&arguments, "justification"),
            text(&arguments, "incident_id"),
        )
    } else {
        (None, None)
    };
    let request_id = new_id(Prefix::Request);
    let ctx = CallCtx::with_channel(
        identity,
        Channel::Mcp,
        request_id.clone(),
        name,
        justification,
        incident_id,
    );

    let result = dispatch(state, &ctx, name, &arguments).await;
    // Attach the correlation id to both kinds of result, so a client can line
    // a call up with its audit record.
    let mut meta = JsonObject::new();
    meta.insert("quoro/request_id".to_string(), json!(request_id));
    let meta = Some(MetaObject(meta));
    Ok(match result {
        Ok(value) => CallToolResult::structured(value).with_meta(meta),
        Err(error) => problem::tool_error(&error).with_meta(meta),
    })
}

/// Calls the service function of `name` and serializes its value.
async fn dispatch(
    state: &AppState,
    ctx: &CallCtx,
    name: &str,
    arguments: &JsonObject,
) -> Result<Value, ApiError> {
    match name {
        "get_cluster" => to_value(service::cluster::get_cluster(state, ctx).await),
        "list_nodes" => {
            let page = query::<api::nodes::PageParams>(arguments, &[])?;
            to_value(service::nodes::list_nodes(state, ctx, &page).await)
        }
        "get_node" => {
            let node_id = id(arguments, "node_id");
            to_value(service::nodes::get_node(state, ctx, node_id).await)
        }
        "list_projects" => {
            let page = query::<api::projects::PageParams>(arguments, &[])?;
            to_value(service::projects::list_projects(state, ctx, &page).await)
        }
        "list_apps" => {
            let query = query::<api::apps::ListAppsQuery>(arguments, &[])?;
            to_value(service::apps::list_apps(state, ctx, &query).await)
        }
        "get_app" => {
            let app_id = id(arguments, "app_id");
            to_value(service::apps::get_app(state, ctx, app_id).await)
        }
        "list_deployments" => {
            let app_id = id(arguments, "app_id");
            let query = query::<api::deployments::ListDeploymentsQuery>(arguments, &["app_id"])?;
            to_value(service::deployments::list_deployments(state, ctx, app_id, &query).await)
        }
        "get_deployment" => {
            let deployment_id = id(arguments, "deployment_id");
            to_value(service::deployments::get_deployment(state, ctx, deployment_id).await)
        }
        "get_app_logs" => {
            let app_id = id(arguments, "app_id");
            let query = query::<api::logs::AppLogsQuery>(arguments, &["app_id"])?;
            to_value(service::logs::get_app_logs(state, ctx, app_id, &query).await)
        }
        "restart_app" => {
            let app_id = id(arguments, "app_id");
            to_value(service::apps::restart_app(state, ctx, app_id).await)
        }
        "redeploy_app" => {
            let app_id = id(arguments, "app_id");
            to_value(service::apps::redeploy_app(state, ctx, app_id).await)
        }
        "delete_app" => {
            let app_id = id(arguments, "app_id");
            to_value(service::apps::delete_app(state, ctx, app_id).await)
        }
        _ => Err(ApiError::internal(&format!("tool {name} has no dispatch"))),
    }
}

/// Serializes a service value, mapping a serialization failure to an internal
/// error.
fn to_value<T: serde::Serialize>(result: Result<T, ApiError>) -> Result<Value, ApiError> {
    let value = result?;
    serde_json::to_value(value).map_err(|e| ApiError::internal(&e))
}

/// A string argument, or `None` when absent.
fn text(arguments: &JsonObject, name: &str) -> Option<String> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// A required string argument. Shape validation already guarantees it; an
/// empty id simply has no target and the service answers `not_found`.
fn id<'a>(arguments: &'a JsonObject, name: &str) -> &'a str {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Deserializes the query parameters from the arguments, dropping the
/// properties that are not fields of `T`.
fn query<T: DeserializeOwned>(arguments: &JsonObject, omit: &[&str]) -> Result<T, ApiError> {
    let mut map = arguments.clone();
    map.remove("justification");
    map.remove("incident_id");
    for name in omit {
        map.remove(*name);
    }
    serde_json::from_value(Value::Object(map)).map_err(|e| ApiError::validation(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_of(spec: &ToolSpec) -> JsonObject {
        let Value::Object(schema) = (spec.input_schema)() else {
            panic!("{} is not an object", spec.name);
        };
        schema
    }

    #[test]
    fn every_schema_is_closed() {
        for spec in specs() {
            assert_eq!(
                schema_of(spec).get("additionalProperties"),
                Some(&json!(false)),
                "{}",
                spec.name
            );
        }
    }

    #[test]
    fn mutating_tools_require_justification() {
        for spec in specs() {
            let schema = schema_of(spec);
            let required = schema.get("required");
            if spec.mutating {
                let required = required
                    .and_then(Value::as_array)
                    .unwrap_or_else(|| panic!("{} has no required list", spec.name));
                assert!(
                    required.iter().any(|name| name == "justification"),
                    "{} does not require justification",
                    spec.name
                );
            } else {
                assert!(
                    required.is_none_or(|r| !r
                        .as_array()
                        .is_some_and(|list| list.iter().any(|name| name == "justification"))),
                    "{} requires justification but is not mutating",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn specs_and_pending_cover_the_readme_table() {
        let expected = [
            "get_cluster",
            "list_nodes",
            "get_node",
            "list_projects",
            "list_apps",
            "get_app",
            "list_deployments",
            "get_deployment",
            "get_app_logs",
            "list_events",
            "list_incidents",
            "get_incident",
            "resolve_incident",
            "restart_app",
            "redeploy_app",
            "delete_app",
            "request_approval",
            "get_approval",
            "list_audit",
        ];
        let mut actual: Vec<&str> = specs().iter().map(|spec| spec.name).collect();
        actual.extend_from_slice(PENDING);
        let mut expected = expected.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn list_matches_the_specs() {
        assert_eq!(list().len(), specs().len());
    }
}
