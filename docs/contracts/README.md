# Quoro contracts

Status: M0 draft for review.

These files are the interfaces that Quoro's workstreams build against in parallel. They are written before any code (D-0040) and are the M0 deliverable named in D-0033.

**Contracts freeze at the end of M0.** After the freeze, a contract change is a standalone pull request that touches only the contract and is reviewed on its own; it is never mixed into feature work (D-0036). If an implementation needs something a contract does not allow, stop and open an issue proposing the change.

## Index

| File | What it fixes | Main consumers |
| --- | --- | --- |
| [openapi.yaml](openapi.yaml) | HTTP API for the M1-M3 surface (OpenAPI 3.1): auth, nodes, projects, VMs, apps, deployments, domains, logs, agents, grants, approvals, incidents, audit, events, webhooks | control-plane, panel, mcp, installer |
| [events.schema.json](events.schema.json) | JSON Schema (draft 2020-12) of the event envelope and every event type | control-plane, node-agent, panel, mcp, webhook receivers |
| [node-agent-protocol.md](node-agent-protocol.md) | Enrollment, the dial-out session, messages, desired-state reconciliation, change-log follow, leases and self-fencing (D-0019) | node-agent, control-plane, engine |
| [audit-log-format.md](audit-log-format.md) | Audit record format, hash chaining, verification, off-site copy with object lock | control-plane, mcp, panel |
| [permission-model.md](permission-model.md) | Identities, scopes, resource selectors, grants (TTL, max uses), incident grants, JIT approval | control-plane, mcp, panel |
| [db-schema.sql](db-schema.sql) | Control-plane SQLite schema: numbered change log and the state tables derived from it (D-0015) | control-plane |
| [engine-trait.md](engine-trait.md) | The Rust `Engine` trait, its types and semantics; Cloud Hypervisor is the first implementation (D-0007) | engine, node-agent |

The architecture these contracts implement is described in [../architecture/architecture.md](../architecture/architecture.md); threats and mitigations are in [../architecture/threat-model.md](../architecture/threat-model.md). Terms follow [../glossary.md](../glossary.md).

## Validation

Two checks run on every contract change.

- `scripts/check_contracts.py` (Python 3 standard library only; run `python3 scripts/check_contracts.py`): `events.schema.json` parses as JSON, its event-type list equals the Event types table, and every example's `type` is in that list and carries the schema's required top-level keys; `db-schema.sql` loads into an in-memory SQLite database and its tables equal the Database tables list; in `openapi.yaml` (checked line by line, without a YAML parser) the document declares OpenAPI 3.1, every operation has `x-quoro-scope` with a scope from the table or a special value, every response has a `description` or a `$ref`, every `x-quoro-mcp-tool` matches the MCP tools table (endpoint and scope), and every event type is mentioned; every backticked scope in the markdown contracts exists in the Scopes table; the node-agent message names listed below equal the message headings of node-agent-protocol.md section 5, and every JSON example block in that file parses.
- Full semantic validation of `openapi.yaml` against OpenAPI 3.1 and of `events.schema.json` against JSON Schema 2020-12 needs third-party validators; it is not in the standard-library script and is added to CI with the cargo workspace in M0.

`openapi.yaml` is hand-written and is the frozen source of truth for the HTTP API (D-0048). The implementation's utoipa output is generated and diffed against it in CI, and the panel client is generated from it with orval.

## Shared vocabulary

Every contract uses exactly the names below. If a contract needs a name that is not here, add it here in the same pull request.

### Roles and statuses

- **Roles** of the single `quoro` binary (D-0012): `node`, `witness`, `edge`. They combine (for example `witness`+`edge`, D-0018).
- **Control-plane status** of a node: `primary` (single writer and source of truth) or `replica` (holds a copy of the change log, serves reads, forwards writes) (D-0012). Primary and replica are statuses, not roles.
- The witness role stores no data (D-0017). In M1-M4 the cluster has exactly one primary and no witness (D-0016).

### Identifiers

All ids are strings `<prefix>_<ULID>` (ULID in Crockford base32, 26 characters), for example `app_01JC9Z6Q5W8N3K4T7R2M1XHB0D`. Ids are never reused.

| Resource | Prefix | Notes |
| --- | --- | --- |
| cluster | `cls_` | One per installation |
| node | `nod_` | A machine running the binary |
| node join token | `njt_` | One-time enrollment token |
| project | `prj_` | Groups apps; also has a unique `slug` (`[a-z0-9-]{1,40}`) used in selectors |
| VM | `vm_` | One microVM managed by the engine (M1). An app's instances are VMs whose `app_id` is set |
| app | `app_` | A deployable service; runs as one microVM per instance (D-0008) |
| deployment | `dep_` | One rollout of an image to an app |
| domain | `dom_` | A hostname routed to an app |
| volume | `vol_` | Persistent disk of a stateful app (shape prepared, used from M5) |
| user (human) | `usr_` | A human identity |
| API token (human) | `tok_` | Personal token of a human |
| agent | `agt_` | An AI identity: name, model, owner (D-0029) |
| agent key | `akey_` | Long-lived agent credential that authorizes nothing by itself |
| access token (agent) | `atk_` | Short-lived token minted from an agent key |
| grant | `grt_` | Scope + selector + TTL (+ max uses) (D-0030) |
| approval request | `apr_` | JIT approval for a destructive action (D-0030) |
| incident | `inc_` | Opened by a detection; scopes incident grants (D-0030, D-0032) |
| event | `evt_` | One event; also has a numeric `seq` |
| webhook | `whk_` | Subscription that delivers events |
| webhook delivery | `whd_` | One delivery attempt record |
| provider | `prv_` | Credentials of one external provider (D-0051) |
| lease | `lse_` | Lease on an app or on the primary status (D-0019) |
| request | `req_` | Correlation id of one API or MCP call |

Change-log entries, audit records and events also carry a numeric, gap-free `seq` per log.

Node-agent protocol messages carry `msg_` ids. They are message-local (they pair a request with its answer inside one session), are never stored, and are not resource ids.

### Keys

The cluster has three keys (D-0050):

- **cluster signing key**: Ed25519. It signs `desired_state` and lease messages, and nodes pin it at enrollment. Rotation uses the node-agent message `signing_key_rotate`, signed by the old key.
- **cluster data-encryption key**: 256-bit key for XChaCha20-Poly1305. It encrypts every `*_enc` column; a `*_enc` BLOB stores the key id, then the 24-byte nonce, then the ciphertext with its tag, so that entries encrypted under different keys can coexist and are selected by key id, which is how rotation works.
- **node-local key**: encrypts a node's local copy of secrets and never leaves the node.

The cluster keys are generated at the primary's first start and stored as `0600` files under `/var/lib/quoro`, outside SQLite. They are backed up separately from the database: losing the data-encryption key loses every secret it protects. Custody of the cluster keys on promotion is open (Q-0024): the candidate is to replicate both keys wrapped to each replica's node key, against re-keying on promotion with an operator-confirmed re-pin.

### Resource relationships

- A **project** contains **apps**. A **VM** may belong to an app (`app_id` set) or stand alone (M1 lifecycle, `app_id` null). Standalone VMs belong to a project too.
- An **app** has **deployments**; the current deployment's image runs as one or more **VMs** (instances) placed on **nodes**. In M1-M4 an app has one instance.
- An app is `stateful` or not. Stateful apps have **volumes** and may run only on their **lease holder** (D-0019).
- An app has an HA policy: `none`, `cold`, `warm`, `hot` (D-0020). Only `none` is implemented before M5a; the field exists from M0.
- A **domain** belongs to one app and has a TLS mode: `acme` (public-IP hosts) or `tunnel` (Cloudflare Tunnel) (D-0024).

### Status enums

- Node `status`: `pending` (enrolled, never connected), `connected`, `disconnected`, `fenced`, `removed`.
- VM `desired_status`: `running`, `stopped`. VM `observed_status`: `creating`, `stopped`, `starting`, `running`, `stopping`, `crashed`, `deleted`, `unknown`.
- App `status`: `deploying`, `running`, `degraded`, `down`, `stopped`.
- Deployment `status`: `pending`, `preparing` (pulling image, building rootfs), `starting`, `succeeded`, `failed`, `superseded`.
- Domain `cert_status`: `pending`, `issued`, `renewing`, `failed`, `not_applicable` (tunnel mode).
- Grant `status`: `active`, `expired`, `revoked`, `exhausted`.
- Approval `status`: `pending`, `approved`, `denied`, `expired`.
- Incident `status`: `open`, `resolved`.
- Lease `mode`: `local` (HA policy `none`/`cold`: never expires when the arbiter is unreachable; ends by `lease_revoke` or an operator declaring the node lost) or `expiring` (HA policy `warm`/`hot`, from M5b: `held` -> `uncertain` -> `expired`, self-fence at `expired`; revoked explicitly) (D-0047). Lease state as seen by a node: `held`, `uncertain`, `expired`, `revoked`. See node-agent-protocol.md.
- Webhook delivery `status`: `pending`, `delivered`, `failed`.

### Scopes

A scope is `<resource>:<verb>` (D-0030). The complete list:

| Scope | Allows | Flags |
| --- | --- | --- |
| `cluster:read` | Read cluster info, version, log position | |
| `node:read` | List and read nodes | |
| `node:enroll` | Create node join tokens | human-only |
| `node:remove` | Remove a node from the cluster | destructive, human-only |
| `project:read` | List and read projects | |
| `project:write` | Create and update projects | |
| `project:delete` | Delete an empty project | destructive |
| `vm:read` | List and read VMs | |
| `vm:create` | Create a VM | |
| `vm:start` | Start a VM | |
| `vm:stop` | Stop a VM | |
| `vm:delete` | Delete a VM and its disks | destructive |
| `app:read` | List and read apps, deployments, domains, env var names | |
| `app:create` | Create an app | |
| `app:update` | Change app settings, env vars, domains | |
| `app:restart` | Restart an app's instances | |
| `app:redeploy` | Roll out the current image again | |
| `app:deploy` | Deploy a new image (new deployment) | |
| `app:start` | Start a stopped app | |
| `app:stop` | Stop an app | |
| `app:delete` | Delete an app and its data | destructive |
| `app:logs` | Read app logs, including live streams | |
| `agent:read` | List and read agents | |
| `agent:write` | Create, update, disable agents; rotate agent keys | human-only |
| `grant:read` | List and read grants | |
| `grant:write` | Create and revoke grants | human-only |
| `approval:read` | List and read approval requests | |
| `approval:create` | File an approval request | |
| `approval:decide` | Approve or deny an approval request | human-only |
| `incident:read` | List and read incidents | |
| `incident:resolve` | Resolve an incident | |
| `audit:read` | Read the audit log and run verification | |
| `event:read` | Read events and the event stream | |
| `webhook:read` | List and read webhooks and deliveries | |
| `webhook:write` | Create, update, delete, test webhooks | human-only |
| `provider:manage` | Create, list, read, delete providers and rotate their secrets | human-only |
| `user:write` | Manage human users and their roles | human-only |

- **destructive** scopes need JIT approval when an agent uses them: the agent files an approval request, a human approves it, and a short-lived extra grant is issued (D-0030).
- **human-only** scopes can never appear in an agent's grant, so an agent cannot widen its own power or approve its own requests.
- The wildcard `app:*` style is not allowed in grants; scopes are listed explicitly.
- Human roles are fixed scope bundles: `owner` (every scope), `operator` (every scope except `user:write`), `viewer` (every `:read` scope plus `app:logs`).

### Resource selectors

A grant's selector says which resources its scopes apply to. JSON form:

```json
{ "projects": ["blog"], "apps": ["app_01J..."], "nodes": ["nod_01J..."] }
```

A resource matches if it matches **any** listed entry (projects by slug, others by id); an app matches `projects` through its project. `{"all": true}` matches everything. An empty selector `{}` matches nothing. String form for CLI and docs: `project=blog`, `app=app_01J...`, `node=nod_01J...`, `all`.

### Event types

Event `type` is `<subject>.<what>` in lowercase with underscores. The complete list:

| Type | Emitted when | D-0032 example |
| --- | --- | --- |
| `node.enrolled` | A node completed enrollment | |
| `node.connected` | A node's session started | |
| `node.disconnected` | A node's session ended or missed heartbeats | |
| `node.fenced` | A node self-fenced stateful apps after losing a lease | |
| `node.disk_low` | A node's free disk fell below its threshold | disk full |
| `vm.created` | A VM was created | |
| `vm.started` | A VM reached `running` | |
| `vm.stopped` | A VM stopped on request | |
| `vm.crashed` | A VM exited without being asked to stop | VM down |
| `vm.deleted` | A VM was deleted | |
| `app.created` | An app was created | |
| `app.updated` | An app's settings changed | |
| `app.deleted` | An app was deleted | |
| `app.down` | An app's health check failed past its threshold | VM down |
| `app.up` | An app recovered | |
| `app.restarted` | An app's instances were restarted | |
| `deployment.started` | A deployment began | |
| `deployment.succeeded` | A deployment's instances are healthy | |
| `deployment.failed` | A deployment failed (image pull, rootfs, boot, health) | build failed (M4 builds report here with `phase: build`) |
| `certificate.issued` | A TLS certificate was obtained or renewed | |
| `certificate.expiring` | A certificate expires within its warning window | certificate expiring |
| `certificate.failed` | Obtaining or renewing a certificate failed | |
| `grant.created` | A grant was issued | |
| `grant.revoked` | A grant was revoked | |
| `grant.expired` | A grant's TTL passed or its uses ran out | |
| `approval.requested` | An agent filed an approval request | |
| `approval.approved` | A human approved a request | |
| `approval.denied` | A human denied a request | |
| `approval.expired` | A request expired undecided | |
| `incident.opened` | A detection opened an incident | |
| `incident.resolved` | An incident was resolved | |
| `audit.chain_broken` | Audit verification found a broken link | |

### Errors

HTTP errors use `application/problem+json` (RFC 9457) with an extra `code` field. `code` values: `unauthenticated` (401), `forbidden` (403), `approval_required` (403), `not_found` (404), `conflict` (409), `validation_failed` (422), `rate_limited` (429), `not_primary` (503, write reached a replica that cannot forward), `node_unreachable` (504, on-demand telemetry could not reach the node), `internal` (500).

For `forbidden`, the field `reason` is one of: `scope_missing`, `selector_mismatch`, `grant_expired`, `grant_revoked`, `grant_exhausted`, `human_only`, `agent_disabled`. For `approval_required`, the body carries `scope` and the target so the agent can file an approval request.

### Request headers

- `Authorization: Bearer <token>` (human API token or agent access token); the panel uses a session cookie.
- `Idempotency-Key` on every `POST` that creates something.
- `Quoro-Justification`: the agent's stated reason for the call, required on every mutating call by an agent, recorded in the audit log (D-0031).
- `Quoro-Setup-Token`: the one-time setup token, required by `POST /api/v1/setup` only (D-0054).
- `Quoro-Incident`: the incident id the call belongs to, when an agent acts on an incident.
- `Last-Event-ID` on SSE streams (`GET /api/v1/events/stream`, `GET /api/v1/apps/{app_id}/logs/stream`): resume after the given `seq`. One-way streams use SSE; bidirectional sessions use WebSockets (D-0057).
- Every response carries `Quoro-Request-Id` (`req_...`).
- Webhook deliveries carry `Quoro-Event-Id`, `Quoro-Event-Type`, `Quoro-Delivery-Id` and `Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<raw body>" with the webhook secret>`. Receivers reject a signature older than 5 minutes.

### Operation annotations

Every operation in `openapi.yaml` carries `x-quoro-scope`: one scope from the table above, or a list when the request needs several, or one of four special values: `public` (no authentication, `security: []`), `authenticated` (any valid human or agent identity, no scope), `human` (any authenticated human identity, session or personal API token; an agent caller is refused with `403 forbidden`, `reason: human_only`), `node` (node credential; only the node-agent session and enrollment). Operations that an MCP tool also exposes carry `x-quoro-mcp-tool` with the tool name.

### MCP tools

M3 exposes these tools (D-0033: read plus restart and redeploy; delete exists so the approval flow can be exercised). Each calls the same service operation as the listed endpoint and needs the same scope (D-0031).

| Tool | Endpoint | Scope |
| --- | --- | --- |
| `get_cluster` | `GET /api/v1/cluster` | `cluster:read` |
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

Every mutating tool takes a required `justification` string and an optional `incident_id`, the MCP equivalents of the `Quoro-Justification` and `Quoro-Incident` headers.

### Channels

Every action reaches the same service layer through one of these channels, recorded in the audit log as `channel`: `api` (HTTP API), `panel` (the web panel, which uses the API with a session), `mcp` (MCP server, D-0031), `cli` (the `quoro` command), `system` (Quoro itself: reconciliation, schedulers, incident automation). MCP tools map one-to-one onto service operations and require the same scope as the matching endpoint.

### Node-agent message types

Defined in node-agent-protocol.md. Node to control plane: `hello`, `heartbeat`, `status_report`, `event`, `lease_renew`, `log_ack`, `stream_data`, `stream_end`, `command_result`. Control plane to node: `welcome`, `desired_state`, `lease_grant`, `lease_revoke`, `log_entries`, `stream_open`, `stream_close`, `command`, `signing_key_rotate`, `error`. Either side: `ping`, `pong`.

### Database tables

Defined in db-schema.sql: `meta`, `change_log`, `nodes`, `node_join_tokens`, `projects`, `vms`, `apps`, `app_env`, `deployments`, `domains`, `volumes`, `leases`, `users`, `api_tokens`, `sessions`, `agents`, `agent_keys`, `access_tokens`, `grants`, `approvals`, `incidents`, `events`, `webhooks`, `webhook_deliveries`, `providers`, `audit_log`, `audit_exports`. Tables marked local in that file are not replicated through the change log.

## Open items before the freeze

| Element | Recommended | Open alternative |
| --- | --- | --- |
| Cluster key custody on promotion | Replicate both keys wrapped to each replica's node key | Re-key on promotion with an operator-confirmed re-pin (open, Q-0024) |
| OCI image to rootfs conversion and minimal init | Own Rust init and own OCI-to-ext4 pipeline, settled by the M0 spike (engine-trait.md) | Reuse an existing guest agent and tooling (open, Q-0003) |
| Snapshot primitive under the chunk engine | Reflink snapshots with a full chunk scan for M5a (engine-trait.md) | lvm-thin, ZFS or btrfs, or own dirty tracking (open, Q-0004) |

None of these rows blocks the contract freeze: promotion arrives in M5 and the key files and message shapes it needs already exist; the `Engine` trait shape is fixed and only the implementation behind `prepare_image` and `snapshot_volume` is open. Every other element formerly listed here is decided (D-0047, D-0048, D-0050, D-0051, D-0052, D-0053, D-0054, D-0056, D-0057).
