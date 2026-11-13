# Permission model

Status: M0 draft for review.

This contract fixes how Quoro decides whether a caller may perform an operation. It implements agents as first-class identities (D-0029), grants made of a scope, a resource selector, a TTL and an optional maximum number of uses (D-0030), one authorization path shared by the HTTP API and MCP (D-0031), and the incident-grant and just-in-time approval flows (D-0030, D-0032). The M3 scope it must satisfy is the M3 Definition of Done in [../product/roadmap.md](../product/roadmap.md) (D-0033).

Contracts freeze at the end of M0; after the freeze a change to this file is a standalone pull request (D-0036). If an implementation needs something this contract does not allow, stop and open an issue proposing the change (D-0036).

Vocabulary, ids, scopes, reasons, roles, channels and MCP tool names are defined once in [README.md](README.md), where the tool list is the table in section MCP tools, and are used here unchanged. The audit record this contract produces is specified in [audit-log-format.md](audit-log-format.md).

## 1. Purpose and principles

Quoro is operated by humans and by AI agents over the same state (D-0002). This contract exists so that an agent can be given real power over infrastructure without that power being open-ended, unnamed or permanent (D-0030).

The principles, each of which the rest of this file turns into a rule:

1. **An agent is an identity, not a shared password.** It has a name, a model and an owning human, and every action records who acted, on behalf of whom and why (D-0029).
2. **Authority is a grant, never a property of a key.** An agent key authorizes nothing by itself; it can only mint short-lived access tokens, and those tokens carry no scopes. What an agent may do is read from its grants on every call (D-0030).
3. **Every grant is bounded in time.** A TTL is mandatory; there is no grant without an expiry (D-0030).
4. **Every grant is bounded in space.** A grant names the resources its scopes apply to through a selector; an empty selector matches nothing (D-0030).
5. **Least privilege by construction.** Agents hold the scopes an incident or a task needs, nothing more; incident grants are issued with exactly the scopes the webhook declared (D-0030, D-0032).
6. **Destructive actions are off by default.** An agent that needs one asks a human, and the human's approval produces a separate, single-use grant for that one call (D-0030).
7. **One gate for every channel.** The API, the panel, the CLI, MCP and Quoro's own automation reach the same service layer and the same authorization check; MCP has no second path (D-0031).
8. **Refusals are evidence.** A refused call is audited with the reason, not dropped (D-0031).

The threat this model addresses is a capable agent acting with more authority than the situation warrants, and nobody being able to reconstruct afterwards what it did and why (D-0029, D-0031). Threats and their mitigations are listed in [../architecture/threat-model.md](../architecture/threat-model.md).

### 1.1 Channels

Every action arrives through one of the channels in the vocabulary: `api`, `panel`, `mcp`, `cli`, `system` (README.md, "Channels"). The channel is recorded in the audit record but does not change authorization: the same identity with the same grant gets the same answer through any of them (D-0031).

A `system` action is taken by Quoro itself, for example reconciliation, a scheduler, incident automation or the audit exporter. It is not a caller with a credential; it is an internal actor whose actions are audited like any other (D-0029).

## 2. Identities and credentials

There are four identity types: `user`, `agent`, `node` and `system` (README.md, "Identifiers"; the audit record uses the same four values in `actor_type`). They are separate: an agent is not a human session with a different name (D-0029).

| Identity | Credential | Lifetime | What it authorizes | Stored as |
| --- | --- | --- | --- | --- |
| human (`usr_`) | panel session cookie | 24 h absolute expiry, no sliding renewal | the panel and its API calls, as that human's role | `sessions.token_hash` |
| human (`usr_`) | personal API token (`tok_`) | optional `expires_at`; valid until expiry or `revoked_at` | the HTTP API and the CLI, as that human's role | `api_tokens.token_hash` |
| agent (`agt_`) | agent key (`akey_`) | long-lived until `revoked_at` | **nothing else**: it may only mint access tokens | `agent_keys.key_hash` |
| agent (`agt_`) | access token (`atk_`) | 5 minutes | API and MCP calls, subject to the agent's grants | `access_tokens.token_hash` |
| node (`nod_`) | node credential (enrollment key pair, `njt_` join token at enrollment) | as long as the node is enrolled and not `removed` | the node-agent session only (`x-quoro-scope: node`) | `nodes.public_key`; `node_join_tokens.token_hash` |
| system | none | — | internal actions through the service layer | — |

Rules on credentials:

- **Secrets are shown once.** A node join token, a personal API token, an agent key and a webhook secret are returned by the call that creates them and never again; only a hash (or, for the webhook secret, an encrypted value) is stored (README.md, "Identifiers"; D-0030). A backup repository's recovery key is returned by the call that creates the repository or replaces the key, and never again; nothing on the server stores it, not even a hash, only a short fingerprint (ADR 0147, [backup-format.md](backup-format.md) section 3).
- **Personal API tokens (`tok_`) can be listed, created and revoked only by a human; an agent's access token is refused with `human_only`, so an agent can never obtain a credential that escapes its grants (D-0029, D-0030).**
- **Provider credentials are managed only by humans.** Providers hold the secrets of external services (DNS, tunnel, load balancer, object storage), so every operation needs `provider:manage`, which is human-only: an agent is refused with `human_only` (D-0051). A provider secret is write-only, encrypted with the cluster data-encryption key, never returned by a read and redacted from audit parameters (D-0050, D-0051).
- **Access tokens are opaque.** An access token is 32 random bytes encoded as an `atk_`-prefixed string. It is not a JWT and carries no claims: scopes, selectors and expiry of authority live in the grant rows, not in the token (D-0030).
- **Access tokens expire in 5 minutes** and are minted against an agent key on demand. A long-running agent re-mints; nothing about the re-mint widens its authority, because the grants are read per call (D-0030).
- **An expired or unknown access token is `unauthenticated` (401).** The caller re-mints from its agent key; the agent key itself is never sent to an ordinary endpoint.
- **Disabling an agent invalidates its access tokens immediately.** `agents.disabled` is read as part of the identity lookup on every request, before any grant is consulted, so the next call after `disabled` is set answers `forbidden` with reason `agent_disabled` (D-0029, D-0030).
- **Revoking an agent key stops new access tokens** (`agent_keys.revoked_at`); already-minted access tokens die at their 5-minute expiry, and any grant check they reach still applies (D-0030).
- **The node credential never authorizes an API call.** It authenticates the node-agent session and enrollment only; every operation annotated `node` in [openapi.yaml](openapi.yaml) is reachable with it and nothing else is (README.md, "Operation annotations"; D-0011). A node therefore cannot read the audit log, change a grant or act as a human.
- **A human's session cookie is not a bearer token.** It is accepted only by the panel origin and never by the MCP endpoint; an MCP client authenticates with an access token (D-0031).

## 3. Human roles

A human's authority is their role. Roles are fixed bundles of scopes, not grant rows: a human holds every scope in their bundle for as long as the role is theirs (README.md, "Scopes").

| Role | Holds |
| --- | --- |
| `owner` | every scope |
| `operator` | every scope except `user:write`, `backup:manage` and `cluster:manage` |
| `viewer` | every `:read` scope plus `app:logs` |

Consequences of the bundles:

- A `viewer` can read apps, deployments, nodes, events, incidents, grants, approvals and the audit log, and can stream app logs, but cannot mutate anything (README.md, "Scopes").
- An `operator` cannot create or change human users; that is `user:write` and owner-only (README.md, "Scopes").
- An `operator` cannot create a backup repository, replace its recovery key or restore its repository key; that is `backup:manage` and owner-only, because the recovery key opens every backup in the repository, including those of the control-plane database and its secrets (ADR 0147). An `operator` takes backups and restores them (`backup:write`, `backup:restore`).
- An `operator` cannot rotate the cluster signing key; that is `cluster:manage` and owner-only, because the signing key decides what every node runs (node-agent-protocol.md section 7.5).
- Every role holds `user:read`, so an `operator` or `viewer` can list users and turn a user id (`owner_user_id`, `on_behalf_of`, `decided_by`, `created_by`) into a name. `user:read` is human-only: agents never list users.
- `app:terminal` and `vm:console` are in the `owner` and `operator` bundles and not in `viewer`, because neither is a `:read` scope (ADR 0114). A `viewer` can read a build's log (`app:logs`) but cannot start a build.
- Human roles are not grantable to agents. An agent never receives a "role"; it receives grants (D-0029, D-0030).
- `role` is a column on the user (`users.role`), so changing a role changes every subsequent decision for that human with no re-issue step (D-0029).

A human may hold personal API tokens; a token inherits the human's role at call time, so demoting or disabling the human takes effect on the next call (D-0029).

**Own password.** Every human may change their own password, whatever their role, with `POST /api/v1/auth/password` ([openapi.yaml](openapi.yaml), `changePassword`; ADR 0135). It is not a scope: it needs no `user:write`, acts only on the caller and proves the current password. It is accepted only with the panel session cookie; a request with an `Authorization` header is `unauthenticated`, so a personal API token cannot turn into a new password and an agent never reaches it. A successful change ends the user's other panel sessions; the calling session stays valid and personal API tokens are not touched (revoke them with `DELETE /api/v1/tokens/{token_id}`). Interactive sessions opened from an ended panel session close with `close_reason` `credential_expired` at their next re-check (section 7.5). Changing another user's password stays `updateUser` with `user:write`.

## 4. Scopes

A scope is `<resource>:<verb>` and is the unit of authority in both human roles and agent grants (D-0030). The complete list, with the flags this contract depends on:

| Scope | Allows | Flags |
| --- | --- | --- |
| `cluster:read` | Read cluster info, version, log position | |
| `cluster:manage` | Rotate the cluster signing key (ADR 0154, ADR 0187) | human-only |
| `node:read` | List and read nodes and node metrics; with `app:read`, the Quoro and host-kernel parts of the vulnerability summary | |
| `node:enroll` | Create node join tokens | human-only |
| `node:remove` | Remove a node from the cluster | destructive, human-only |
| `node:fence_ack` | Acknowledge a node's failed fence after verifying its VMs are stopped, so its leases may be granted again | human-only |
| `node:roles` | Add or remove the `witness` role of an enrolled machine, which changes the voters (ADR 0170) | human-only |
| `node:declare_lost` | Declare a disconnected node lost, so its leases may be granted elsewhere (ADR 0183) | human-only |
| `project:read` | List and read projects | |
| `project:write` | Create and update projects | |
| `project:delete` | Delete an empty project | destructive |
| `vm:read` | List and read VMs | |
| `vm:create` | Create a VM | |
| `vm:start` | Start a VM | |
| `vm:stop` | Stop a VM | |
| `vm:delete` | Delete a VM and its disks | destructive |
| `vm:console` | Read a VM's serial console, live and its recent output (ADR 0116) | |
| `app:read` | List and read apps, their Git sources, builds, deployments, domains, volumes, env var names, app metrics and vulnerability reports | |
| `app:create` | Create an app | |
| `app:update` | Change app settings, the Git source, env vars, domains, the backup schedule; create volumes | |
| `app:restart` | Restart an app's instances | |
| `app:redeploy` | Roll out the current image again | |
| `app:deploy` | Deploy a new image (new deployment); start and cancel builds, whose success deploys, including a build that refreshes its base images | |
| `app:start` | Start a stopped app | |
| `app:stop` | Stop an app | |
| `app:delete` | Delete an app and its data, or one volume of an app | destructive |
| `app:logs` | Read app logs and build logs, including live streams | |
| `app:terminal` | Open an interactive terminal in an app's VM (ADR 0113, ADR 0114) | human-only |
| `app:failover` | Fail a `warm` app over to its warm node, or run a planned switchover to it (ADR 0175, ADR 0177) | destructive |
| `app:delete_quarantine` | Delete a quarantined copy of a volume, the divergent writes a node kept after a failover (ADR 0176) | destructive, human-only |
| `backup:read` | List and read backups and backup repositories (ADR 0147) | |
| `backup:write` | Take a backup of an app's volume now | |
| `backup:restore` | Restore a backup onto a node, which replaces the app's data and moves the app there | destructive |
| `backup:manage` | Create a backup repository, rotate its recovery key, restore its repository key on a promoted primary, set the control-plane backup schedule (ADR 0147, ADR 0148, ADR 0322) | human-only |
| `agent:read` | List and read agents | |
| `agent:write` | Create, update, disable agents; rotate agent keys | human-only |
| `grant:read` | List and read grants | |
| `grant:write` | Create and revoke grants | human-only |
| `approval:read` | List and read approval requests | |
| `approval:create` | File an approval request | |
| `approval:decide` | Approve or deny an approval request | human-only |
| `incident:read` | List and read incidents | |
| `incident:resolve` | Resolve an incident | |
| `audit:read` | Read the audit log and run verification, and read the status of its off-site export; humans also fetch session transcripts (section 7.6) | |
| `event:read` | Read events and the event stream | |
| `webhook:read` | List and read webhooks and deliveries | |
| `webhook:write` | Create, update, delete, test webhooks | human-only |
| `provider:manage` | Create, list, read, delete providers and rotate their secrets; configure and probe the off-site audit export (ADR 0149) | human-only |
| `user:read` | List human users, to resolve user ids to names | human-only |
| `user:write` | Manage human users and their roles | human-only |

### 4.1 Destructive scopes

Seven scopes are destructive: `node:remove`, `project:delete`, `vm:delete`, `app:delete`, `backup:restore`, and from M5b `app:failover` and `app:delete_quarantine` (README.md, "Scopes"). They remove data or capacity and cannot be undone by repeating a call. `app:delete` also covers deleting one volume (`deleteVolume`), and `backup:restore` replaces an app's data with an older copy. `app:failover` starts a `warm` app from its last shipped snapshot, which drops the writes made since then on the old node from the app's data (they survive only as a quarantined copy), and it also covers the planned switchover, which moves the app (ADR 0175, ADR 0177). `app:delete_quarantine` deletes those kept writes for good (ADR 0176).

This contract fixes their M3 behaviour:

- **A standard grant can never contain a destructive scope.** Attempting to create one is `validation_failed` at the grant endpoint.
- **An incident grant can never contain a destructive scope.** `webhook.wake.grant_scopes` is validated when the webhook is saved, so an incident can never hand an agent the power to delete (D-0032).
- **An agent reaches a destructive scope only through an approval grant**, that is a grant with `kind` `approval`, created by a human's decision on an approval request (D-0030). Section 9 defines the flow.
- **A destructive call with no matching approval grant answers `approval_required` (403)**, not `forbidden`: the agent is not told "never", it is told "ask first" (README.md, "Errors"; D-0030).

Making destructive scopes grantable in standing grants would be a change to this contract and therefore to an accepted decision (D-0030 says destructive actions are off by default), not an implementation detail (D-0036).

`node:remove` is destructive **and** human-only, so no agent can delete a node at all: an approval grant can carry a destructive scope, never a human-only one (README.md, "Scopes"). `app:delete_quarantine` is both for the same reason: no agent deletes divergent writes, even with an approval.

### 4.2 Human-only scopes

Sixteen scopes are human-only: `cluster:manage`, `node:enroll`, `node:remove`, `node:fence_ack`, `node:roles`, `node:declare_lost`, `app:terminal`, `app:delete_quarantine`, `backup:manage`, `agent:write`, `grant:write`, `approval:decide`, `webhook:write`, `provider:manage`, `user:read` and `user:write` (README.md, "Scopes").

- A grant whose `scopes` contains a human-only scope is refused at creation with `validation_failed`, and a hypothetical row that contained one is refused at call time with reason `human_only` (README.md, "Errors").
- The set is chosen so an agent can never widen its own power (`agent:write`, `grant:write`, `webhook:write`), approve its own request (`approval:decide`), take over cluster membership or the voters (`node:enroll`, `node:remove`, `node:roles`), declare a possibly running VM stopped so its lease moves (`node:fence_ack`, D-0079, and `node:declare_lost`, ADR 0183), delete the divergent writes a failover left behind (`app:delete_quarantine`, ADR 0176) or hold external credentials (`provider:manage`) or the key that opens every backup (`backup:manage`) or replace the key every node trusts (`cluster:manage`) (D-0030, D-0051, ADR 0147). `user:read` is human-only so an agent never reads the list of human identities and their emails. `app:terminal` is human-only in M4 because a shell in the app's VM reads every secret the app holds and acts outside single audited calls (ADR 0114); a later, non-interactive exec scope reachable only through an approval grant is a separate decision.
- Because `approval:decide` is human-only, "an agent approves its own destructive request" is not a policy that could be misconfigured; it is unrepresentable (D-0030).

### 4.3 Wildcards

Grants list scopes explicitly. `app:*`, `*` and any other wildcard form are rejected with `validation_failed` (README.md, "Scopes"). The only wildcard in the model is the selector form `{"all": true}`, which is a resource selector, not a scope (section 5).

## 5. Resource selectors

A grant's selector says which resources its scopes apply to. JSON form:

```json
{ "projects": ["blog"], "apps": ["app_01J..."], "nodes": ["nod_01J..."] }
```

and the string form used by the CLI and by documentation: `project=blog`, `app=app_01J...`, `node=nod_01J...`, `all` (README.md, "Resource selectors").

Matching rules:

- A resource matches if it matches **any** listed entry. Entries are combined with OR, not AND.
- Projects are matched by `slug`, never by `prj_` id; apps and nodes are matched by id (README.md, "Resource selectors").
- An app matches `projects` through its own `project_id`'s slug, so `project=blog` covers every app in the `blog` project.
- `{"all": true}` matches everything and is the only way to say "the whole cluster".
- `{}` (the empty selector) matches nothing. A grant with an empty selector is valid to store and useless to use; it is refused at call time with `selector_mismatch`.

Matching per target type:

| Target type | Matched by |
| --- | --- |
| app | `apps` entry equal to its id, or `projects` entry equal to the slug of its project, or `{"all": true}` |
| VM | `apps` entry equal to the VM's `app_id`, or `projects` entry equal to the slug of the VM's project, or `nodes` entry equal to the VM's `node_id`, or `{"all": true}` |
| deployment | the app it belongs to (as above), or `{"all": true}` |
| build | the app it belongs to (as above), or `{"all": true}` |
| interactive session | its VM (as above; a terminal session's VM is an instance of its app), or `{"all": true}` |
| domain | the app it belongs to (as above), or `{"all": true}` |
| volume | the app it belongs to (as above), or `{"all": true}` |
| backup of a volume | the app it belongs to (as above), or `{"all": true}` |
| backup of the control-plane database | `{"all": true}` only: it holds the whole cluster state |
| backup repository, audit export | `{"all": true}` only (section 5.1) |
| vulnerability finding of an app | the app (as above), or `{"all": true}` |
| vulnerability finding of a Quoro component or a host kernel | `{"all": true}` only, together with `node:read`: it is node data (openapi.yaml `getVulnerabilitySummary`) |
| project | `projects` entry equal to its slug, or `{"all": true}` |
| node | `nodes` entry equal to its id, or `{"all": true}` |
| incident | its `subject` (resolved as the subject's own type above), or `{"all": true}` |
| approval request | its `target` (resolved as the target's own type above), or `{"all": true}` |

### 5.1 Cluster-level scopes

`cluster:read`, `event:read` and `audit:read` have no resource of their own, and neither do the backup repositories under `backup:read`:

- **They are matched by any non-empty selector.** A grant carrying `event:read` with selector `{"projects":["blog"]}` may read the event stream, but only records whose subject or project belongs to `blog` (constraint below).
- **Results are filtered, never refused.** A list call under a selector returns the matching items; it does not answer `forbidden` because some rows in the table are out of scope. Filtered lists are the normal outcome for agents holding narrow grants (D-0030).
- **Events are filtered by `subject`** (and by `project_id` when set).
- **Audit records are filtered by `target_type`/`target_id` and `project_id`,** and by `actor_id` when the selector names apps or nodes the actor belongs to. A record with no target (for example a cluster-level write) is visible only to a selector of `{"all": true}`.
- **A grant whose selector is `{}` reads nothing** under these scopes, consistently with section 5.

The filtering above is a read-path rule; it does not create a second permission check. An implementation that returns everything to a narrowly scoped agent and relies on the panel to hide rows violates this contract (D-0030).

## 6. Grants

A grant is the only carrier of agent authority (D-0030). Its fields:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | `grt_` id | yes | identity of the grant |
| `agent_id` | `agt_` id | yes | the agent it empowers |
| `kind` | `standard` \| `incident` \| `approval` | yes | why the grant exists (section 6.1) |
| `scopes` | array of scope strings | yes | what it allows; never human-only, destructive only for `approval` |
| `selector` | selector object | yes | where it applies (section 5) |
| `expires_at` | RFC 3339 timestamp | yes | TTL is mandatory; no grant is open-ended |
| `max_uses` | integer or null | no | maximum authorized mutating calls; null means unlimited within the TTL |
| `uses` | integer | yes | how many mutating calls it has authorized |
| `status` | `active` \| `expired` \| `revoked` \| `exhausted` | yes | lifecycle state (section 6.2) |
| `incident_id` | `inc_` id or null | for `kind=incident` | the incident the grant was issued for |
| `approval_id` | `apr_` id or null | for `kind=approval` | the approval request that produced it |
| `on_behalf_of` | `usr_` id | yes | the human whose authority is being lent |
| `created_by_type` | `user` \| `system` | yes | who issued it |
| `created_by_id` | id | yes | that human or subsystem |
| `reason` | text | yes | why it was issued, in words |
| `created_at` | RFC 3339 timestamp | yes | when it was issued |
| `revoked_at` | RFC 3339 timestamp or null | no | when a human or the system revoked it |
| `revoked_by` | id or null | no | who revoked it |

The field names match the shared entity model and the `grants` table in [db-schema.sql](db-schema.sql).

### 6.1 Kinds

| Kind | Issued by | Typical TTL | May carry destructive scopes | Ends when |
| --- | --- | --- | --- | --- |
| `standard` | a human with `grant:write` | chosen by that human | no | TTL, max uses, or revocation |
| `incident` | the system, when an incident opens | `wake.grant_ttl_s` (default 3600 s) | no | TTL, max uses, the incident resolving, or revocation |
| `approval` | the system, on a human's approval | 10 minutes | yes, exactly one | one use, TTL, or revocation |

`standard` is the everyday grant: a human lends an agent a few scopes on a few resources for a shift or a task (D-0030). `incident` and `approval` are the two automatic kinds, defined in sections 8 and 9 (D-0030, D-0032).

### 6.2 Lifecycle

`status` moves in one direction:

```
active ──TTL passed──────────▶ expired
   │
   ├──── revoked by a human──▶ revoked
   │
   └──── last use consumed────▶ exhausted
```

- A grant is created `active` (event `grant.created`).
- The transition to `expired`, `revoked` or `exhausted` emits `grant.expired` or `grant.revoked`; `grant.expired` carries `reason` `ttl` or `max_uses` in its `data` (README.md, "Event types").
- `revoked` and `expired` are terminal; there is no re-activation of a grant id. A new grant is a new row with a new id (README.md, "Identifiers": ids are never reused).
- `exhausted` is computed from `uses >= max_uses`, so a grant with `max_uses` 1 that authorized one call is `exhausted` even if its TTL has not passed.

### 6.3 TTL

`expires_at` is mandatory on every grant. There is no default of "forever" and no way to omit it: the grant endpoint answers `validation_failed` without it (D-0030).

- The TTL is absolute from `created_at`; it is not refreshed by use. An agent that needs longer asks for a longer grant (D-0030).
- Expiry is evaluated against the control plane's clock at decision time, not against any node's clock.
- A grant at or past `expires_at` is `expired` and authorizes nothing, even for a read (D-0030).

### 6.4 Maximum uses

- `max_uses` counts **authorized mutating calls**, that is calls that pass authorization and are executed with result `ok`.
- **Reads do not consume uses.** An agent can poll `get_app` under a one-use grant as often as it likes (D-0030).
- A refused call does not consume a use; a failed call (result `error` after authorization) does not consume a use either, because the use is incremented with the action in the same transaction (section 7.3) and an action that did not commit did not happen.
- `max_uses` of 1 is the shape of every approval grant (section 9).

### 6.5 Revocation

- A human with `grant:write` revokes any grant; the system revokes incident grants when their incident resolves (section 8).
- **Revocation is checked on every request**, not cached and not attached to the token. The next call after revocation is therefore refused with `grant_revoked`, which is how "locks the agent out within seconds" (D-0030) holds: the bound is the next request, not a refresh interval.
- Revoking a grant does not revoke the agent key or delete audit records; it removes the authority the grant lent (D-0030).
- Revocation emits `grant.revoked` and is itself audited.

### 6.6 Issuing grants

`POST /api/v1/grants` requires `grant:write` and is human-only. The service refuses with `validation_failed`:

- a scope that does not exist;
- a wildcard scope;
- a human-only scope;
- a destructive scope on a `standard` grant (section 4.1);
- a missing `expires_at`, an `expires_at` in the past, or a `max_uses` below 1;
- a selector that is not the documented shape.

The created grant emits `grant.created` and is audited with the issuing human as `actor` (`created_by_type` `user`) (D-0030, D-0031).

## 7. Authorization

Authorization is one function called by the service layer. The API, MCP, the panel, the CLI and Quoro's own automation call it; none of them has a bypass (D-0031).

### 7.1 Inputs

```
authorize(
  identity,        -- resolved from the credential: {type, id, role?, disabled?, model?}
  operation,       -- endpoint name or MCP tool name, e.g. "restart_app"
  required_scopes, -- one scope or a list, from x-quoro-scope
  target,          -- {type, id} or null for list calls
  params,          -- the request body/query as the caller sent it
  channel,         -- api | panel | mcp | cli | system
  justification,   -- agent's stated reason (required for mutating agent calls)
  incident_id,     -- optional, from Quoro-Incident
  approval_id      -- optional, only for destructive calls
) -> Decision { allow, grant?, reason?, approval? }
```

`operation` is the endpoint or MCP tool name from [openapi.yaml](openapi.yaml); `required_scopes` comes from the operation's `x-quoro-scope`, so a tool exposed over MCP inherits exactly the scope of its endpoint (README.md, "Operation annotations"; D-0031).

### 7.2 Decision procedure

```
1. resolve identity
   - unknown/expired credential            -> 401 unauthenticated
   - node credential on a non-`node` op    -> 403 scope_missing
   - system identity                       -> allow (internal actor, audited)

2. if identity.type == "user":
     if required_scopes ⊆ role_bundle(identity.role) -> allow
     else -> 403 scope_missing

3. if identity.type == "agent":
     a. if human_only ∩ required_scopes ≠ ∅         -> 403 human_only
     b. if identity.disabled                        -> 403 agent_disabled
     c. candidates = grants where
          agent_id == identity.id
          AND status == active
          AND expires_at > now
          AND (max_uses is null OR uses < max_uses)
          AND required_scopes ⊆ scopes
     d. if candidates is empty :
          if destructive ∩ required_scopes ≠ ∅        -> 403 approval_required
          else -> 403 scope_missing
     e. candidates = candidates where selector matches `target`
        (for list calls: filter the result rows by selector, never refuse)
     f. if candidates is empty                       -> 403 selector_mismatch
     g. if destructive ∩ required_scopes ≠ ∅ :
          keep only grants with kind == "approval"
            whose approval_id's target == target
            and whose approval_id's params_hash == sha256_jcs(params)
          if none                                   -> 403 approval_required
     h. grant = candidate with the earliest expires_at
        (ties broken by earliest created_at, then by lowest id)
     i. mutating call: in one transaction
          re-read grant FOR UPDATE; re-check expiry, revocation, uses, status
          perform the action
          uses += 1
          if max_uses is not null and uses == max_uses -> status = exhausted, emit grant.expired(reason=max_uses)
          append the audit record with result "ok" and change_seq of the action
          commit
        read call: no use is consumed, no transaction beyond the read
     j. allow, carry grant.id into the audit record
```

Step (a) before step (c) is what makes `human_only` the answer even when a malformed grant row exists and even for a disabled agent. Step (b) before the grant query is what makes disabling an agent immediate.

### 7.3 The transaction

An authorized mutating call writes three things atomically: the state change (through the change log, D-0013), the increment of `uses` and the audit record (D-0031). Either all three exist or none does. This is why "the action exists but its audit record does not" is not a state the system can reach (see [audit-log-format.md](audit-log-format.md)).

The transaction re-reads the grant after step (h) so that a revocation or expiry that lands between selection and commit still refuses the call. The bound on "within seconds" (D-0030) is then stricter than seconds: it is the next commit.

### 7.4 Refusal reasons

Every refusal maps to an HTTP `application/problem+json` body with `code` and, for `forbidden`, a `reason`:

| Reason | Status | When |
| --- | --- | --- |
| `human_only` | 403 `forbidden` | the call needs a scope that can never be granted to an agent |
| `agent_disabled` | 403 `forbidden` | the agent is disabled |
| `scope_missing` | 403 `forbidden` | no active grant contains the required scope, or the human's role lacks it |
| `selector_mismatch` | 403 `forbidden` | a grant has the scope but its selector does not cover the target |
| `grant_expired` | 403 `forbidden` | the only grants with the scope have passed `expires_at` |
| `grant_revoked` | 403 `forbidden` | the only grants with the scope were revoked |
| `grant_exhausted` | 403 `forbidden` | the only grants with the scope have no uses left |
| `approval_required` | 403 `approval_required` | a destructive scope is required and no matching approval grant exists |

**Order.** When several reasons could apply, the answer is the first that applies in this order: `human_only`, `agent_disabled`, `scope_missing`, `selector_mismatch`, `grant_expired`, `grant_revoked`, `grant_exhausted`. The order is part of the contract because the M3 demo prints the reason (docs/product/roadmap.md, M3 Definition of Done): an agent that reads `grant_expired` learns something different from an agent that reads `selector_mismatch`.

Implementation note: the candidate query in step (c) filters on status and expiry for speed, so reasons `grant_expired`, `grant_revoked` and `grant_exhausted` are produced by a second, unfiltered lookup over the agent's grants when step (d) or (f) would otherwise answer `scope_missing` or `selector_mismatch`. The unfiltered lookup is only consulted to choose the reason; it never authorizes.

**`approval_required` body.** It carries `scope` (the single destructive scope) and `target` (`type`, `id`), so the agent can file an approval request without guessing (README.md, "Errors"). It may also carry `operation` and `params_hash` as convenience; the approval endpoint recomputes both from the request body it receives.

**Every refusal is audited** with `result` `refused` and `result_detail` set to the reason (`approval_required` included). A refusal names no `change_seq` because nothing changed (D-0031).

**Authentication failures are not refusals.** An `unauthenticated` (401) call has no identity to attribute, so it is written to the server log, not the audit chain (D-0031; section 11).

### 7.5 Interactive sessions

A web terminal or a VM console is opened by one authorized call and then stays open for minutes or hours over a WebSocket (ADR 0057, ADR 0114). The open is authorized like any call: `app:terminal` for `POST /api/v1/apps/{app_id}/terminal-sessions` (human-only, so an agent is refused with `human_only` at step 3a), `vm:console` for `POST /api/v1/vms/{vm_id}/console-sessions` (a human's role, or an agent's grant whose selector covers the VM). The answer is a single-use ticket valid for 30 seconds; the WebSocket upgrade presents the ticket instead of a credential ([openapi.yaml](openapi.yaml), `connectInteractiveSession`).

Because one authorization would otherwise cover hours of activity, the primary re-checks an open session and closes it, with the `close_reason` in brackets, when:

| Condition | Checked | `close_reason` |
| --- | --- | --- |
| the human is disabled or deleted, or the agent is disabled | on change, and at least every 30 s | `access_revoked` |
| the human's role no longer holds the scope | on change, and at least every 30 s | `access_revoked` |
| the agent's grant that authorized the open is revoked, expires or is exhausted, and no other active grant covers the scope and the VM | on change, and at least every 30 s | `access_revoked` |
| the panel session cookie, the personal API token or the agent access token that opened it expires or is revoked | at its expiry | `credential_expired` |
| no input from the client for 15 minutes (a console has no input: no WebSocket message from the client) | continuously | `idle_timeout` |
| the session has been open for 8 hours | continuously | `max_duration` |

An agent access token lives 5 minutes (section 2), so an agent's console session ends at that token's expiry unless the agent opens a new one; console sessions are meant for short reads of boot output, not for permanent tails. The re-check consumes no grant use: only the open is a call. Every close, for any reason, writes the `session.end` audit record ([audit-log-format.md](audit-log-format.md), section 3.3).

### 7.6 Session transcripts

A terminal session's transcript holds every byte typed and printed, so it holds whatever secret appeared on the screen (ADR 0115). `GET /api/v1/interactive-sessions/{session_id}/transcript` therefore needs `audit:read` **and** a human caller: an agent holding `audit:read` is refused with `human_only`, because an agent may not read through a transcript what it may not obtain through a terminal (ADR 0114). Every transcript fetch is audited, also when a human makes it ([audit-log-format.md](audit-log-format.md), section 3).

## 8. Incident grants and the AI-native loop

D-0032 defines the loop: detect, notify, wake, act. This section fixes the permissions half of the wake and act steps.

### 8.1 The webhook's wake configuration

A webhook may carry a `wake` object (shared entity model; [events.schema.json](events.schema.json)):

| Field | Type | Meaning |
| --- | --- | --- |
| `agent_id` | `agt_` id | the agent the webhook wakes |
| `grant_scopes` | array of scope strings | the scopes the incident grant will carry; non-destructive only |
| `grant_ttl_s` | integer, default 3600 | lifetime of the incident grant |
| `grant_max_uses` | integer or null | optional cap on how many mutating calls the incident grant allows |

- `grant_scopes` is validated when the webhook is saved: a human-only scope or a destructive scope receives `validation_failed`. A webhook therefore cannot be used to route around section 4.1 (D-0032).
- `webhook:write` is human-only, so the agent that will be woken cannot configure its own future grant (README.md, "Scopes").
- `created_by` is the human who saved the webhook and becomes `on_behalf_of` of every incident grant the webhook causes, which is how "on whose behalf" is answered for a wake nobody was present for (D-0029).
- `wake` can be changed with `PATCH /api/v1/webhooks/{webhook_id}` but not removed. To stop a webhook from waking an agent, delete the webhook and create it again without `wake`; the new webhook gets a new signing secret.

### 8.2 When the grant is issued

1. A detection (node agent report, health check, certificate state, a vulnerability finding, or a switchover stopped after its revoke) opens an incident with `subject` `{type, id}` (D-0032). An app has at most one open incident per detection kind: `app.down` opens one unless an `app.down` incident of that app is open, and `app.up` resolves only that one; from M5a `app.vulnerable` opens its own, alongside an open `app.down` incident, and the system resolves it once the app has no open finding in the KEV catalog or with a CVSS score of 9.0 or more (ADR 0150); from M5b `app.switchover_failed` with `step` `move` opens one unless such an incident of that app is open, and `app.failed_over` of that app resolves only that one, since the lease that the switchover left revoked has then moved (node-agent-protocol.md section 6.13). The incident's `trigger_event_id` tells them apart. `app.degraded` and, from M5b, `domain.unserved` open no incident (ADR 0167).
2. `incident.opened` is emitted.
3. For each enabled webhook whose `event_types` matches and whose `selector` covers the incident's subject, the system delivers the event and, if the webhook has `wake`, issues an incident grant:
   - `kind` `incident`;
   - `agent_id` = `wake.agent_id`;
   - `scopes` = `wake.grant_scopes`, copied verbatim;
   - `selector` = **the incident subject only** — an app id, a node id or a project slug, never a broader selector;
   - `expires_at` = now + `wake.grant_ttl_s`;
   - `max_uses` = `wake.grant_max_uses`;
   - `incident_id` = the incident;
   - `on_behalf_of` = `webhook.created_by`;
   - `created_by_type` `system`, `reason` naming the incident and the webhook.
4. `grant.created` is emitted and the issuance is audited with `actor_type` `system`.
5. The delivery body is the `incident.opened` event with `data.wake` = `{agent_id, grant_id}` added: the woken agent and the incident grant just issued ([events.schema.json](events.schema.json), `data_incident_opened`). The signature covers it like the rest of the body, and every retry of that delivery carries the same `data.wake`. A delivery whose webhook issued no grant (no `wake`, or the incident was resolved before the delivery) carries no `data.wake`. The event log itself never carries it.

### 8.3 When the grant ends

- **Revoked by the system when the incident resolves.** `POST /api/v1/incidents/{incident_id}/resolve` revokes every incident grant with that `incident_id`, emitting `grant.revoked` (D-0030, D-0032).
- Otherwise it ends by TTL or by `max_uses`, like any grant (section 6).
- A resolved incident does not delete its grants; they stay visible as `revoked` so the audit trail can be read later (D-0031).

### 8.4 Worked example

The D-0030 example, in this contract's terms: an app on one node goes down.

- Detection opens `inc_01J...` with subject `{ "type": "app", "id": "app_01J..." }`.
- A webhook with `wake` = `{ "agent_id": "agt_01J...", "grant_scopes": ["app:read", "app:logs", "app:restart", "incident:read", "incident:resolve", "approval:create"], "grant_ttl_s": 3600 }` fires.
- The agent receives `grt_01J...`: those six scopes on `{"apps":["app_01J..."]}`, one hour.
- The agent calls `get_app`, then `restart_app` with a justification naming the incident. Both pass; only the restart consumes a use, and `max_uses` is null so the hour is the only bound.
- The app comes back; the agent calls `resolve_incident` (it holds `incident:resolve` because the webhook declared it; without it, the agent reports and a human resolves). On resolve, the grant becomes `revoked`.

A grant for one hour on one app, and nothing else, is the whole point: the agent could not have restarted a different app, read a project it was not woken for, or deleted anything (D-0030).

## 9. Just-in-time approval for destructive actions

D-0030 requires that destructive actions be off by default and reachable only after a human approves a specific call. The flow, with the exact endpoints and events:

1. **The agent attempts the destructive call.** For example `DELETE /api/v1/apps/{app_id}` (scope `app:delete`) or the `delete_app` MCP tool (D-0031, README.md "MCP tools"). From M5a the same flow covers `DELETE /api/v1/volumes/{volume_id}` (scope `app:delete`, target `{type: volume}`) and `POST /api/v1/backups/{backup_id}/restore` (scope `backup:restore`, target `{type: backup}`, with the target node in `params`). From M5b it covers `POST /api/v1/apps/{app_id}/failover` and `POST /api/v1/apps/{app_id}/switchover` (scope `app:failover`, target `{type: app}`, with the warm node in `params`): an agent that fails over a warm app is the "agent with human approval" of ADR 0175.
2. **The service refuses with `approval_required`** (403) carrying `scope` and `target`. Nothing is deleted, and the refusal is audited with `result` `refused`, `result_detail` `approval_required` (D-0030).
3. **The agent files a request:** `POST /api/v1/approvals` with `approval:create`, body `{scope, target, operation, params, justification, incident_id?}`. The service computes `params_hash` as the SHA-256 hex of the RFC 8785 canonical serialization of `params`, stores `status` `pending` and `expires_at` = now + 1 hour, and emits `approval.requested` (D-0030).
4. **A human decides in the panel** with `approval:decide`, which is human-only: the panel shows the operation, the target, the parameters and the agent's justification, and the human approves or denies (D-0030; README.md "Scopes").
5. **On approve** the system, in one transaction:
   - sets `status` `approved`, `decided_by`, `decided_at`, optional `decision_note`;
   - creates an approval grant: `kind` `approval`, scopes = exactly the one requested scope, selector = that target only, `max_uses` 1, `expires_at` = now + 10 minutes, `approval_id` set, `on_behalf_of` = the deciding human;
   - emits `approval.approved` and `grant.created`;
   - audits the decision with `actor_type` `user`.
6. **The agent repeats the same call with the same params.** Authorization matches the approval grant by target and `params_hash`; the call executes; `uses` reaches 1; the grant becomes `exhausted` and emits `grant.expired` with `reason` `max_uses` (section 6.2). The audit record carries `approval_id` and `grant_id` (D-0030, D-0031).

The same call with different params is a different call: `params_hash` differs, no approval grant matches, and the answer is `approval_required` again. This is what stops "approve a harmless restart, then send a delete" (D-0030).

Denial and expiry:

- **Denied:** `POST /api/v1/approvals/{approval_id}/deny` with `approval:decide` and a note; `status` `denied`, `approval.denied`, no grant. The agent's retry answers `approval_required` (D-0030).
- **Expired:** a pending request past its `expires_at` (default 1 hour) becomes `expired` and emits `approval.expired`; no grant is created and a late decision answers `conflict` (409) (D-0030).
- **Already decided:** a second decision on the same request answers `conflict` (409); approval is not idempotent in the "make it work twice" sense, it is idempotent in the "cannot double-issue a grant" sense.

Who may approve:

- Any human holding `approval:decide`: `owner` and `operator` (README.md, "Scopes").
- Never the agent that filed it, never another agent, and never a caller reaching the endpoint through MCP: `approval:decide` is human-only, so an MCP client cannot call the decide operation at all (D-0030).
- The deciding human is recorded as `decided_by` and as the approval grant's `on_behalf_of`, so the audit record of the destructive call names the agent that did it and the human who allowed it (D-0029).

What approval does not do: it does not widen the agent's other grants, it does not create a reusable permission, and it does not pre-approve a class of future calls (D-0030).

## 10. MCP mapping

M3 exposes the tools listed in [README.md](README.md) (D-0033: read plus restart and redeploy; delete exists so the approval flow can be exercised). The tool list is the table in README.md, section MCP tools. Each calls the same service operation as the listed endpoint and requires the same scope (D-0031).

Rules that follow from D-0031 and are not optional:

- **Every mutating tool takes a required `justification` string and an optional `incident_id`.** They are the MCP equivalents of the `Quoro-Justification` and `Quoro-Incident` headers (README.md, "MCP tools"; D-0031). A mutating tool call without `justification` is `validation_failed`, not an unaudited success.
- **MCP clients authenticate with the same bearer access token** as the API: `Authorization: Bearer atk_...`. There is no MCP-specific identity, and a human session cookie is not accepted by the MCP endpoint (D-0031).
- **`approval:decide` has no MCP tool.** The absence is deliberate: an agent cannot approve anything through MCP (D-0030).
- **Tool and endpoint annotations must agree.** Each operation in [openapi.yaml](openapi.yaml) carries `x-quoro-scope` and, when a tool exposes it, `x-quoro-mcp-tool`; a tool whose annotation disagrees with the README table fails the contract check.
- **The MCP server is a thin adapter.** It validates the tool arguments, calls the service operation and returns its result; it never queries the database directly and never constructs an audit record (D-0031).
- **`rmcp` is pinned to an exact minor version**, because the SDK shipped three semver-major releases between March and July 2026 and upgrades are scheduled work rather than a range bump (F-0041; `docs/research/rust-api-and-mcp-tooling.md`). The transport is Streamable HTTP mounted as a Tower service; no session state is assumed, since recent spec revisions remove protocol sessions (F-0041).
- **Inbound token validation is Quoro's own middleware**, not something the SDK is documented to provide: server-side OAuth middleware is not documented in rmcp, so the bearer check is implemented on the Axum side (F-0041; `docs/research/rust-api-and-mcp-tooling.md`, labelled a **Hypothesis** there for the absence of documentation).

## 11. Accountability

Every action, allowed or refused, records the same accountability fields (D-0029, D-0031). The full record is specified in [audit-log-format.md](audit-log-format.md); this section fixes what each identity type puts in them.

| Question | Field | Where it comes from |
| --- | --- | --- |
| who acted | `actor_type`, `actor_id` | the resolved identity |
| which model | `agent_model` | `agents.model`, agents only |
| on behalf of whom | `on_behalf_of` | the grant's `on_behalf_of`, or the deciding human for an approval |
| why | `justification` | `Quoro-Justification` or the MCP `justification` argument, required for mutating agent calls |
| what woke it | `trigger_event_id`, `incident_id` | the incident and the event that opened it |
| what it did | `operation`, `scope`, `target_type`, `target_id`, `params` | the call |
| what happened | `result`, `result_detail`, `change_seq` | the outcome |
| which authority | `grant_id`, `approval_id` | the grant that allowed it, the approval that produced it |

Per identity type:

| Identity | `actor_type` / `actor_id` | `agent_model` | `on_behalf_of` |
| --- | --- | --- | --- |
| human | `user` / `usr_` | omitted | omitted (a human acting for themselves); set to the human who decided, on an approval decision record |
| agent | `agent` / `agt_` | the agent's `model` | the matched grant's `on_behalf_of` |
| node | `node` / `nod_` | omitted | omitted |
| system | `system` / the subsystem name | omitted | set when the system acts on an incident grant it issued |

Two consequences worth stating plainly:

- An agent action always names three parties: the agent, its model and the human whose authority it borrowed (D-0029).
- A `system` action that a webhook caused still names the human who created the webhook, through the incident grant and `trigger_event_id`. There is no unattributed automation (D-0029, D-0032).

A human's read calls are not recorded; an agent's reads are (D-0031). That asymmetry is deliberate: the log exists to make agent behaviour reconstructable, and the volume of human browsing would bury it (see [audit-log-format.md](audit-log-format.md), "What is recorded").

## 12. Rate limits

- Rate limits are enforced per identity: a human token and an agent access token each get their own bucket, and sharing an agent key does not pool two agents into one bucket (D-0029).
- Exceeding a limit answers `rate_limited` (429) in `application/problem+json`.
- **The values are implementation defaults, not contract.** This contract fixes only that the limit is per identity and that the answer is `rate_limited`; an implementation may choose the numbers and change them without a contract change (D-0030).
- A rate-limited call is audited as `refused` with `result_detail` `rate_limited` when the identity is known, so a burst that an agent produced is visible in the log (D-0031).
- A request body is at most 256 KiB. A larger one answers `payload_too_large` (413) and is audited as `refused` with `result_detail` `payload_too_large` and `params` `{}` when the identity is known (README.md, "Request body limit", ADR 0306).
- `POST /api/v1/auth/password` is limited per user, counting attempts with a wrong current password, so a stolen session cannot be used to guess the password (ADR 0135).

## 13. Worked examples

The scenarios cover the M3 Definition of Done in [../product/roadmap.md](../product/roadmap.md) (D-0033). "Audit `result_detail`" is the field on the record this call produces.

| # | Identity | Authority | Call | Result | Audit `result_detail` |
| --- | --- | --- | --- | --- | --- |
| 1 | agent `agt_A` | incident grant, `app:read` + `app:restart`, selector `{"apps":["app_1"]}` | `restart_app` on `app_1` | ok; app restarts | `ok`, with `change_seq`, `incident_id`, `grant_id` |
| 2 | agent `agt_A` | standard grant, `app:read` only | `restart_app` on `app_1` | 403 `forbidden` | `scope_missing` |
| 3 | agent `agt_A` | standard grant, `app:restart`, `expires_at` 10 minutes ago | `restart_app` on `app_1` | 403 `forbidden` | `grant_expired` |
| 4 | agent `agt_A` | standard grant, `app:restart`, revoked by a human a second ago | `restart_app` on `app_1` | 403 `forbidden` | `grant_revoked` |
| 5 | agent `agt_A` | standard grant, `app:restart`, selector `{"projects":["blog"]}` | `restart_app` on an app in project `shop` | 403 `forbidden` | `selector_mismatch` |
| 6 | agent `agt_A` | standard grant, `app:restart`, `max_uses` 1, one use already spent | `restart_app` on `app_1` | 403 `forbidden` | `grant_exhausted` |
| 7 | agent `agt_A` | any grant (even `{"all": true}`) | `delete_app` on `app_1` | 403 `approval_required`, body carries `scope` and `target` | `approval_required` |
| 8 | agent `agt_A`, after the human approves | approval grant, `app:delete`, selector `{"apps":["app_1"]}`, `max_uses` 1, TTL 10 min | `delete_app` on `app_1` with the approved params | ok; app deleted; grant `exhausted` | `ok`, with `approval_id` and `grant_id` |
| 9 | agent `agt_A` | grant minus the destructive scope | `delete_app` on `app_1`, unchanged params, expired approval grant | 403 `approval_required` again | `approval_required` |
| 10 | agent `agt_A` | any grant containing a human-only scope (malformed row) | `POST /api/v1/grants` (`grant:write`) | 403 `forbidden` | `human_only` |
| 11 | agent `agt_A` | valid grant, but the agent was disabled a moment ago | `get_app` on `app_1` | 403 `forbidden` | `agent_disabled` |
| 12 | human `usr_H`, role `viewer` | role bundle | `restart_app` on `app_1` | 403 `forbidden` | `scope_missing` |
| 13 | human `usr_H`, role `operator` | role bundle | `delete_app` on `app_1` | ok; no approval needed for a human | `ok` |
| 14 | agent `agt_A` | incident grant, `event:read`, selector `{"projects":["blog"]}` | `list_events` | ok; only `blog` subjects returned | `ok` |
| 15 | agent `agt_A` | standard grant, `app:restart`, TTL 1 h, `max_uses` null | `get_app` on `app_1` twenty times, then `restart_app` | all ok; only the restart consumes a use | `ok` on each |
| 16 | agent `agt_A` | standard grant, `app:terminal` (malformed row), selector `{"all": true}` | `POST /api/v1/apps/{app_id}/terminal-sessions` | 403 `forbidden` | `human_only` |
| 17 | agent `agt_A` | standard grant, `vm:console`, selector `{"apps":["app_1"]}` | `POST /api/v1/vms/{vm_id}/console-sessions` on `app_1`'s VM | 201 with a ticket; the session ends when the access token expires | `ok`; a later `session.end` with `credential_expired` |
| 18 | human `usr_H`, role `viewer` | role bundle | `POST /api/v1/vms/{vm_id}/console-sessions` | 403 `forbidden` | `scope_missing` |

Two rows are worth reading together. Row 7 and row 8 are the same call: the second one succeeds only because a human approved exactly that call with exactly those params. Row 9 shows what happens when the approval is not there any more — the agent does not inherit a permanent right to delete from having once been approved (D-0030).

## 14. What this contract does not decide

- **Whether destructive scopes may ever go into standing grants.** This contract says no for M3 (D-0030: destructive actions are off by default). Changing it needs a decision, not a patch (D-0036).
- **The exact numeric rate limits.** Implementation defaults (section 12).
- **How long a human session lives beyond the 24 h absolute expiry**, and whether refresh tokens are added. The contract fixes that a session expires absolutely; anything more is a later decision (D-0029).
- **Grant approval by two humans (four-eyes).** Not in D-0030 and not in this contract.
- **An external identity provider.** Quoro authenticates its own humans with a password checked against an Argon2id hash and with its own tokens; no SSO path exists in M0-M6 (D-0029; `users.password_hash` in [db-schema.sql](db-schema.sql)).
- **The off-site copy of the audit log**, which the audit contract fixes, with the lock strength its probe reports per provider (ADR 0149; [audit-log-format.md](audit-log-format.md) section 8). Configuring it needs `provider:manage`; reading its status needs `audit:read`.
