# MCP tools

Generated from the "MCP tools" table of `docs/contracts/README.md` by `crates/cli/tests/skill.rs`; do not edit. Regenerate with `QUORO_BLESS=1 cargo test -p quoro-cli --test skill`.

The quoro MCP server exposes exactly these tools. Each calls the same operation as its endpoint and needs the same scope. Every mutating tool takes a required `justification` string and an optional `incident_id`. No tool decides an approval: `approval:decide` is human-only.

| Tool | Endpoint | Scope |
| --- | --- | --- |
| `get_cluster` | `GET /api/v1/cluster` | `cluster:read` |
| `get_capacity` | `GET /api/v1/cluster/capacity` | `cluster:read` |
| `list_nodes` | `GET /api/v1/nodes` | `node:read` |
| `get_node` | `GET /api/v1/nodes/{node_id}` | `node:read` |
| `list_projects` | `GET /api/v1/projects` | `project:read` |
| `list_apps` | `GET /api/v1/apps` | `app:read` |
| `get_app` | `GET /api/v1/apps/{app_id}` | `app:read` |
| `list_deployments` | `GET /api/v1/apps/{app_id}/deployments` | `app:read` |
| `get_deployment` | `GET /api/v1/deployments/{deployment_id}` | `app:read` |
| `get_app_logs` | `GET /api/v1/apps/{app_id}/logs` | `app:logs` |
| `list_events` | `GET /api/v1/events` | `event:read` |
| `list_incidents` | `GET /api/v1/incidents` | `incident:read` |
| `get_incident` | `GET /api/v1/incidents/{incident_id}` | `incident:read` |
| `resolve_incident` | `POST /api/v1/incidents/{incident_id}/resolve` | `incident:resolve` |
| `restart_app` | `POST /api/v1/apps/{app_id}/restart` | `app:restart` |
| `redeploy_app` | `POST /api/v1/apps/{app_id}/redeploy` | `app:redeploy` |
| `delete_app` | `DELETE /api/v1/apps/{app_id}` | `app:delete` (destructive) |
| `request_approval` | `POST /api/v1/approvals` | `approval:create` |
| `get_approval` | `GET /api/v1/approvals/{approval_id}` | `approval:read` |
| `list_audit` | `GET /api/v1/audit` | `audit:read` |
