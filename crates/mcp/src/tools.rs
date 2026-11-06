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
        name: "list_events",
        operation_id: "listEvents",
        scope: "event:read",
        mutating: false,
        destructive: false,
        title: "List events",
        input_schema: schema_list_events,
    },
    ToolSpec {
        name: "list_incidents",
        operation_id: "listIncidents",
        scope: "incident:read",
        mutating: false,
        destructive: false,
        title: "List incidents",
        input_schema: schema_list_incidents,
    },
    ToolSpec {
        name: "get_incident",
        operation_id: "getIncident",
        scope: "incident:read",
        mutating: false,
        destructive: false,
        title: "Read one incident with its trigger event and grants",
        input_schema: schema_get_incident,
    },
    ToolSpec {
        name: "resolve_incident",
        operation_id: "resolveIncident",
        scope: "incident:resolve",
        mutating: true,
        destructive: false,
        title: "Resolve an incident",
        input_schema: schema_resolve_incident,
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
    ToolSpec {
        name: "request_approval",
        operation_id: "createApproval",
        scope: "approval:create",
        mutating: true,
        destructive: false,
        title: "File an approval request",
        input_schema: schema_request_approval,
    },
    ToolSpec {
        name: "get_approval",
        operation_id: "getApproval",
        scope: "approval:read",
        mutating: false,
        destructive: false,
        title: "Read one approval request",
        input_schema: schema_get_approval,
    },
    ToolSpec {
        name: "list_audit",
        operation_id: "listAudit",
        scope: "audit:read",
        mutating: false,
        destructive: false,
        title: "List audit records",
        input_schema: schema_list_audit,
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
// required query parameters and `justification`. A JSON request body follows,
// its properties appended in YAML order after the parameter-derived ones and
// skipping names that already exist; its `required` names are added. The body's
// `$ref`s are inlined verbatim (the args validator does not resolve them).
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

/// The path parameter `approval_id` (`components/parameters/ApprovalId`).
fn approval_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^apr_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "Approval request id (`apr_...`)."
    })
}

/// The path parameter `incident_id` (`components/parameters/IncidentId`).
fn incident_path_id() -> Value {
    json!({
        "type": "string",
        "pattern": "^inc_[0-9A-HJKMNP-TV-Z]{26}$",
        "description": "Incident id (`inc_...`)."
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

/// The query parameter `after_seq` (`components/parameters/AfterSeq`).
fn after_seq() -> Value {
    json!({
        "type": "integer",
        "minimum": 0,
        "description": "Return only entries whose `seq` is greater than this value."
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

/// The `CreateApprovalRequest` body's `scope` (`components/schemas/Scope`),
/// inlined because the args validator does not resolve `$ref`.
fn scope() -> Value {
    json!({
        "type": "string",
        "title": "Scope",
        "description": "One scope, `<resource>:<verb>` (D-0030). Wildcards are not allowed in\ngrants; scopes are listed explicitly.\n",
        "enum": [
            "cluster:read",
            "node:read",
            "node:enroll",
            "node:remove",
            "node:fence_ack",
            "project:read",
            "project:write",
            "project:delete",
            "vm:read",
            "vm:create",
            "vm:start",
            "vm:stop",
            "vm:delete",
            "vm:console",
            "app:read",
            "app:create",
            "app:update",
            "app:restart",
            "app:redeploy",
            "app:deploy",
            "app:start",
            "app:stop",
            "app:delete",
            "app:logs",
            "app:terminal",
            "backup:read",
            "backup:write",
            "backup:restore",
            "backup:manage",
            "agent:read",
            "agent:write",
            "grant:read",
            "grant:write",
            "approval:read",
            "approval:create",
            "approval:decide",
            "incident:read",
            "incident:resolve",
            "audit:read",
            "event:read",
            "webhook:read",
            "webhook:write",
            "provider:manage",
            "user:read",
            "user:write"
        ]
    })
}

/// The `CreateApprovalRequest` body's `target` (`components/schemas/Target`),
/// inlined.
fn target() -> Value {
    json!({
        "type": "object",
        "title": "Target",
        "description": "One addressed resource.",
        "required": ["type", "id"],
        "properties": {
            "type": {
                "type": "string",
                "description": "Resource kind, for example `app`, `vm`, `project` or `node`."
            },
            "id": {
                "type": "string",
                "description": "Id of the resource."
            }
        }
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

/// The `EventType` component (`components/schemas/EventType`), inlined because
/// the args validator does not resolve `$ref`.
///
/// The `|` block description keeps its raw YAML line breaks: only the
/// parameter-level description is normalized, so the inlined `items` schema
/// carries the component's text as `resolve_schema` produces it.
fn event_type() -> Value {
    json!({
        "type": "string",
        "title": "EventType",
        "description": "Event type, `<subject>.<what>` in lowercase with underscores. The list is\nclosed; new types are added to the contract in a standalone change.\n",
        "enum": [
            "node.enrolled",
            "node.connected",
            "node.disconnected",
            "node.fenced",
            "node.fence_failed",
            "node.disk_low",
            "vm.created",
            "vm.started",
            "vm.stopped",
            "vm.crashed",
            "vm.deleted",
            "app.created",
            "app.updated",
            "app.deleted",
            "app.down",
            "app.degraded",
            "app.up",
            "app.restarted",
            "app.vulnerable",
            "deployment.started",
            "deployment.succeeded",
            "deployment.failed",
            "build.started",
            "build.succeeded",
            "build.failed",
            "build.cancelled",
            "backup.succeeded",
            "backup.failed",
            "restore.succeeded",
            "restore.failed",
            "certificate.issued",
            "certificate.expiring",
            "certificate.failed",
            "domain.unserved",
            "domain.served",
            "grant.created",
            "grant.revoked",
            "grant.expired",
            "approval.requested",
            "approval.approved",
            "approval.denied",
            "approval.expired",
            "incident.opened",
            "incident.resolved",
            "audit.chain_broken"
        ]
    })
}

fn schema_list_events() -> Value {
    schema(
        vec![
            (
                "type",
                json!({
                    "type": "array",
                    "items": event_type(),
                    "description": "Only events of this type. Repeat the parameter to select several."
                }),
            ),
            (
                "subject_id",
                json!({
                    "type": "string",
                    "description": "Only events about this resource id."
                }),
            ),
            ("after_seq", after_seq()),
            ("limit", limit()),
            ("cursor", cursor()),
        ],
        &[],
    )
}

fn schema_list_incidents() -> Value {
    schema(
        vec![
            (
                "status",
                json!({
                    "type": "string",
                    "enum": ["open", "resolved"],
                    "description": "Only incidents in this status."
                }),
            ),
            ("limit", limit()),
            ("cursor", cursor()),
        ],
        &[],
    )
}

fn schema_get_incident() -> Value {
    schema(vec![("incident_id", incident_path_id())], &["incident_id"])
}

/// The tool carries one `incident_id`: the path parameter, which is both the
/// incident to resolve and the call's context incident. `call()` reads it into
/// the [`CallCtx`] for every mutating tool, so `Quoro-Incident` adds no second
/// property and the same id feeds the audit record. The body's
/// `resolution_note` follows the header-derived properties.
fn schema_resolve_incident() -> Value {
    schema(
        vec![
            ("incident_id", incident_path_id()),
            ("justification", justification()),
            (
                "resolution_note",
                json!({
                    "type": "string",
                    "maxLength": 2000,
                    "description": "What was done to resolve it. Recorded in the audit log."
                }),
            ),
        ],
        &["incident_id", "justification"],
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

/// The tool carries one `justification` and one `incident_id`: the MCP
/// equivalents of the `Quoro-Justification` and `Quoro-Incident` headers and,
/// for `request_approval`, of the body fields of the same names. The body's
/// properties are appended after the header ones, so the duplicates are
/// skipped and `justification` stays first.
fn schema_request_approval() -> Value {
    schema(
        vec![
            ("justification", justification()),
            ("incident_id", incident_id()),
            ("scope", scope()),
            ("target", target()),
            (
                "operation",
                json!({
                    "type": "string",
                    "description": "Endpoint template or MCP tool name the agent intends to call."
                }),
            ),
            (
                "params",
                json!({
                    "type": "object",
                    "additionalProperties": true,
                    "description": "The intended call parameters."
                }),
            ),
        ],
        &["justification", "scope", "target", "operation", "params"],
    )
}

fn schema_get_approval() -> Value {
    schema(vec![("approval_id", approval_id())], &["approval_id"])
}

fn schema_list_audit() -> Value {
    schema(
        vec![
            (
                "actor_id",
                json!({
                    "type": "string",
                    "description": "Only records of this actor (user, agent or node id)."
                }),
            ),
            (
                "target_id",
                json!({
                    "type": "string",
                    "description": "Only records about this resource id."
                }),
            ),
            (
                "result",
                json!({
                    "type": "string",
                    "enum": ["ok", "refused", "error"],
                    "description": "Only records with this result."
                }),
            ),
            (
                "incident_id",
                json!({
                    "type": "string",
                    "pattern": "^inc_[0-9A-HJKMNP-TV-Z]{26}$",
                    "description": "Only records that belong to this incident."
                }),
            ),
            ("after_seq", after_seq()),
            ("limit", limit()),
            ("cursor", cursor()),
        ],
        &[],
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
/// idempotent; restart, redeploy and filing an approval request are neither;
/// delete is idempotent and destructive. Every tool stays within Quoro's
/// closed world.
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
/// A JSON-RPC error for an unknown tool.
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
        "list_events" => {
            let query = events_query(arguments)?;
            to_value(service::events::list_events(state, ctx, &query).await)
        }
        "list_incidents" => {
            let query = query::<api::incidents::ListIncidentsQuery>(arguments, &[])?;
            to_value(service::incidents::list_incidents(state, ctx, &query).await)
        }
        "get_incident" => {
            let incident_id = id(arguments, "incident_id");
            to_value(service::incidents::get_incident(state, ctx, incident_id).await)
        }
        "resolve_incident" => {
            let incident_id = id(arguments, "incident_id");
            let resolution_note = text(arguments, "resolution_note");
            to_value(
                service::incidents::resolve_incident(state, ctx, incident_id, resolution_note)
                    .await,
            )
        }
        "list_audit" => {
            let query = query::<api::audit::ListAuditQuery>(arguments, &[])?;
            to_value(service::audit::list_audit(state, ctx, &query).await)
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
        "request_approval" => {
            let body = approval_request(arguments)?;
            to_value(service::approvals::create_approval(state, ctx, &body, None).await)
        }
        "get_approval" => {
            let approval_id = id(arguments, "approval_id");
            to_value(service::approvals::get_approval(state, ctx, approval_id).await)
        }
        _ => Err(ApiError::internal(&format!("tool {name} has no dispatch"))),
    }
}

/// Builds the body of `request_approval`. The tool has one `justification` and
/// one `incident_id`, the MCP equivalents of the header and of the body field,
/// so the argument feeds both the [`CallCtx`] and the body. No idempotency key
/// travels over MCP: a repeated call files a second request.
///
/// A missing `justification` is refused here instead of by the service: the
/// body type requires it, and over HTTP a body without it is rejected before
/// the service, unaudited.
fn approval_request(
    arguments: &JsonObject,
) -> Result<api::approvals::CreateApprovalRequest, ApiError> {
    let justification = text(arguments, "justification")
        .ok_or_else(|| ApiError::validation("justification is required"))?;
    Ok(api::approvals::CreateApprovalRequest {
        scope: text(arguments, "scope").unwrap_or_default(),
        target: arguments.get("target").cloned().unwrap_or(Value::Null),
        operation: text(arguments, "operation").unwrap_or_default(),
        params: arguments.get("params").cloned().unwrap_or(Value::Null),
        justification,
        incident_id: text(arguments, "incident_id"),
    })
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

/// Builds the `list_events` query from the arguments. `ListEventsQuery` has no
/// `Deserialize`, because `type` repeats, so the arguments become the raw
/// query pairs HTTP would carry and [`api::events::ListEventsQuery::parse`]
/// validates them (unknown event type, negative `after_seq`, bad cursor).
fn events_query(arguments: &JsonObject) -> Result<api::events::ListEventsQuery, ApiError> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    if let Some(Value::Array(types)) = arguments.get("type") {
        for value in types {
            if let Some(name) = value.as_str() {
                pairs.push(("type".to_string(), name.to_string()));
            }
        }
    }
    for name in ["subject_id", "after_seq", "limit", "cursor"] {
        if let Some(value) = arguments.get(name) {
            pairs.push((name.to_string(), scalar(value)));
        }
    }
    api::events::ListEventsQuery::parse(&pairs)
}

/// A scalar argument as it would appear in a query string.
fn scalar(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
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
/// properties in `omit` (path parameters, which are not fields of `T`).
fn query<T: DeserializeOwned>(arguments: &JsonObject, omit: &[&str]) -> Result<T, ApiError> {
    let mut map = arguments.clone();
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
    fn specs_cover_the_readme_table() {
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
        let mut expected = expected.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn list_matches_the_specs() {
        assert_eq!(list().len(), specs().len());
    }

    /// A syntactically valid incident id (`inc_` plus 26 Crockford characters).
    const INCIDENT: &str = "inc_01JC9Z6Q5W8N3K4T7R2M1XHB0D";

    fn spec(name: &str) -> &'static ToolSpec {
        specs()
            .iter()
            .find(|spec| spec.name == name)
            .unwrap_or_else(|| panic!("{name} is not a wired tool"))
    }

    #[test]
    fn list_events_arguments_build_the_query() {
        let mut arguments = JsonObject::new();
        arguments.insert("type".to_string(), json!(["app.down", "vm.crashed"]));
        arguments.insert("after_seq".to_string(), json!(42));

        let query = events_query(&arguments).expect("the arguments are valid");
        assert_eq!(
            query.types,
            Some(vec!["app.down".to_string(), "vm.crashed".to_string()])
        );
        assert_eq!(query.after_seq, Some(42));
    }

    #[test]
    fn the_audit_query_keeps_the_incident_filter() {
        let mut arguments = JsonObject::new();
        arguments.insert("incident_id".to_string(), json!(INCIDENT));
        arguments.insert("result".to_string(), json!("refused"));

        let query =
            query::<api::audit::ListAuditQuery>(&arguments, &[]).expect("the arguments are valid");
        assert_eq!(query.incident_id.as_deref(), Some(INCIDENT));
        assert_eq!(query.result.as_deref(), Some("refused"));
    }

    #[test]
    fn validate_rejects_an_unknown_event_type() {
        let schema = schema_of(spec("list_events"));
        let mut arguments = JsonObject::new();
        arguments.insert("type".to_string(), json!(["app.exploded"]));

        let reason = args::validate(&schema, &arguments).expect_err("the event type is unknown");
        assert!(reason.starts_with("type[0]: must be one of"), "{reason}");
    }

    #[test]
    fn validate_requires_the_incident_id_to_resolve() {
        let schema = schema_of(spec("resolve_incident"));
        let mut arguments = JsonObject::new();
        arguments.insert("justification".to_string(), json!("J"));

        assert_eq!(
            args::validate(&schema, &arguments),
            Err("incident_id: is required".to_string())
        );
    }
}
