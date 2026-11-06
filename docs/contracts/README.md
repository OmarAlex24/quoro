# Quoro contracts

Status: M0 draft for review.

These files are the interfaces that Quoro's workstreams build against in parallel. They are written before any code (D-0040) and are the M0 deliverable named in D-0033.

**Contracts freeze at the end of M0.** After the freeze, a contract change is a standalone pull request that touches only the contract and is reviewed on its own; it is never mixed into feature work (D-0036). If an implementation needs something a contract does not allow, stop and open an issue proposing the change.

## Index

| File | What it fixes | Main consumers |
| --- | --- | --- |
| [openapi.yaml](openapi.yaml) | HTTP API (OpenAPI 3.1): auth, nodes, projects, VMs, apps, Git sources and builds, deployments, domains, logs, metrics, terminal and console sessions, volumes, backups and backup repositories, agents, grants, approvals, incidents, audit and its off-site exports, events, webhooks | control-plane, panel, mcp, installer |
| [events.schema.json](events.schema.json) | JSON Schema (draft 2020-12) of the event envelope and every event type | control-plane, node-agent, panel, mcp, webhook receivers |
| [node-agent-protocol.md](node-agent-protocol.md) | Enrollment, the dial-out session, messages, desired-state reconciliation, change-log follow, leases and self-fencing (D-0019) | node-agent, control-plane, engine |
| [audit-log-format.md](audit-log-format.md) | Audit record format, hash chaining, verification, off-site copy with a per-provider lock strength | control-plane, mcp, panel |
| [backup-format.md](backup-format.md) | Backup repository layout in object storage: chunks, manifests, seals, the repository key and the owner recovery key (ADR 0147, ADR 0148) | control-plane, node-agent, installer |
| [permission-model.md](permission-model.md) | Identities, scopes, resource selectors, grants (TTL, max uses), incident grants, JIT approval | control-plane, mcp, panel |
| [db-schema.sql](db-schema.sql) | Control-plane SQLite schema: numbered change log and the state tables derived from it (D-0015) | control-plane |
| [migrations/](migrations/) | Numbered schema migrations applied in order on top of the frozen `db-schema.sql`; each bumps `schema_version` | control-plane |
| [engine-trait.md](engine-trait.md) | The Rust `Engine` trait, its types and semantics; Cloud Hypervisor is the first implementation (D-0007) | engine, node-agent |

The architecture these contracts implement is described in [../architecture/architecture.md](../architecture/architecture.md); threats and mitigations are in [../architecture/threat-model.md](../architecture/threat-model.md). Terms follow [../glossary.md](../glossary.md).

## Validation

Two checks run on every contract change.

- `scripts/check_contracts.py` (Python 3 standard library only; run `python3 scripts/check_contracts.py`): `events.schema.json` parses as JSON, its event-type list equals the Event types table, and every example's `type` is in that list and carries the schema's required top-level keys; `db-schema.sql` loads into an in-memory SQLite database, the migrations in `migrations/` apply in order on top of it with `schema_version` ending at one plus their count, and its tables equal the Database tables list; in `openapi.yaml` (checked line by line, without a YAML parser) the document declares OpenAPI 3.1, every operation has `x-quoro-scope` with a scope from the table or a special value, every response has a `description` or a `$ref`, every `x-quoro-mcp-tool` matches the MCP tools table (endpoint and scope), and every event type is mentioned; every backticked scope in the markdown contracts exists in the Scopes table; the node-agent message names listed below equal the message headings of node-agent-protocol.md section 5, and every JSON example block in that file parses.
- Full semantic validation of `openapi.yaml` against OpenAPI 3.1 and of `events.schema.json` against JSON Schema 2020-12 needs third-party validators; it is not in the standard-library script and is added to CI with the cargo workspace in M0.

`openapi.yaml` is hand-written and is the frozen source of truth for the HTTP API (D-0048). The implementation's utoipa output is generated and diffed against it in CI, and the panel client is generated from it with orval.

## Shared vocabulary

Every contract uses exactly the names below. If a contract needs a name that is not here, add it here in the same pull request.

### Roles and statuses

- **Roles** of the single `quoro` binary (D-0012): `node`, `witness`, `edge`. They combine (for example `witness`+`edge`, D-0018).
- **Control-plane status** of a node: `primary` (single writer and source of truth) or `replica` (holds a copy of the change log, serves reads, forwards writes) (D-0012). Primary and replica are statuses, not roles.
- The witness role stores no data (D-0017). In M1-M4 the cluster has exactly one primary and no witness (D-0016).
- From M5b every machine with the `node` or `witness` role is a **voter**; an edge-only machine is not, and one machine has one vote whatever its roles (ADR 0170). Each voter gives the primary its **support** for a short window over its session, and the primary grants and renews `expiring` leases only while a majority of voters supports it (ADR 0169). The cluster **term** goes up by one with each promotion, which takes it from a majority of voters; every machine refuses a primary with a lower term than it has seen (`stale_term`, node-agent-protocol.md section 8.8). An enrolled machine gains or loses the `witness` role through `POST /api/v1/nodes/{node_id}/roles`, without enrolling again; a witness stores only its vote state.
- The **edge** role (from M5a, ADR 0143) runs the embedded proxy for the domains of apps with more than one replica, balancing over their instances on other nodes and checking each one itself. An edge-only machine runs no engine and needs no KVM. Edges and nodes are joined by the **mesh**, a kernel WireGuard star with the edge as hub (ADR 0144).

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
| build | `bld_` | One image build of an app's Git source, run on the app's node (ADR 0117, ADR 0118) |
| domain | `dom_` | A hostname routed to an app |
| volume | `vol_` | Local persistent disk of a stateful app, on one node (ADR 0145) |
| quarantined copy | `qrn_` | A volume's divergent writes kept by a node that lost the app's lease to a failover (ADR 0176); the node generates the id |
| backup | `bkp_` | One point-in-time copy of a volume or of the control-plane database in a backup repository (ADR 0147, ADR 0148) |
| backup repository | `brp_` | An encrypted, content-addressed store of backups in an `object_store` provider's bucket (ADR 0147) |
| audit export | `aex_` | One off-site copy target of the audit log (ADR 0149) |
| vulnerability finding | `vln_` | One known vulnerability in one package of an app's image, a Quoro component or a host kernel (ADR 0150) |
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
| interactive session | `trm_` | One web terminal or VM console session (ADR 0114); not a panel login session |

Change-log entries, audit records and events also carry a numeric, gap-free `seq` per log.

Node-agent protocol messages carry `msg_` ids. They are message-local (they pair a request with its answer inside one session), are never stored, and are not resource ids.

### Keys

The cluster has three keys (D-0050):

- **cluster signing key**: Ed25519. It signs `desired_state` and lease messages, and nodes pin it at enrollment. Rotation uses the node-agent message `signing_key_rotate`, signed by the old key.
- **cluster data-encryption key**: 256-bit key for XChaCha20-Poly1305. It encrypts every `*_enc` column; a `*_enc` BLOB stores the key id, then the 24-byte nonce, then the ciphertext with its tag, so that entries encrypted under different keys can coexist and are selected by key id, which is how rotation works.
- **node-local key**: encrypts a node's local copy of secrets and never leaves the node.

Backups add two keys per backup repository (ADR 0147, [backup-format.md](backup-format.md)):

- **repository key**: 256 random bits, generated when the repository is created. The keys that name and encrypt chunks are derived from it per backup source. It is stored only wrapped: under the cluster data-encryption key in `backup_repositories.repo_key_enc`, for unattended backups, and under the recovery key, in the repository's config object in the bucket.
- **recovery key**: 256 random bits, shown once to the owner who creates the repository (or rotates it) and never stored by Quoro in plaintext. With it, the repository can be read on a fresh host after the primary and its data-encryption key are lost.

The cluster keys are generated at the primary's first start and stored as `0600` files under `/var/lib/quoro`, outside SQLite. They are backed up separately from the database: losing the data-encryption key loses every secret it protects. Custody of the cluster keys on promotion is decided (ADR 0078, read by ADR 0180): every machine with the `node` role generates a custody key pair of its own, and the primary seals the signing key, the data-encryption key and its own TLS key to each such machine, so a promoted node can open them and keeps the certificate nodes pin (ADR 0181). The node-local key above is not used for this, because the primary never sees it. The fields and the table that carry the seals are specified with manual promotion in M5b; until then the keys do not leave the primary.

### Resource relationships

- A **project** contains **apps**. A **VM** may belong to an app (`app_id` set) or stand alone (M1 lifecycle, `app_id` null). Standalone VMs belong to a project too.
- An **app** has **deployments**; the current deployment's image runs as one or more **VMs** (instances) placed on **nodes**. In M1-M4 an app has one instance. From M5a a stateless app runs `replicas` instances, at most one per node (ADR 0142); every instance gets `QUORO_INSTANCE_ID` (its `vm_` id) and `QUORO_NODE_ID` (its `nod_` id) in its environment, and those two names are reserved.
- Each node with the `node` role has its own guest **subnet**, a `/24` of `10.42.0.0/16` assigned at enrollment and never reused while a VM from it is recorded (the oldest node of a cluster from before M5a keeps the configured guest network, and no other subnet overlaps it); each node and edge has a **mesh address** in `10.43.0.0/16` (ADR 0144).
- An app is `stateful` or not. An app is stateful exactly when it has a **volume**: its first volume makes it stateful (ADR 0145). A stateful app runs one instance, on the node that holds its volume and its `local` **lease** (D-0019, D-0047).
- A **backup** copies one volume, or the control-plane database, into a **backup repository** (ADR 0147, ADR 0148). Restoring a volume backup creates a new volume on the chosen node and moves the app there.
- An app has an HA policy: `none`, `cold`, `warm`, `hot` (D-0020). `none` and `cold` (scheduled backups of a stateful app) are accepted from M5a, `warm` from M5b; `hot` is refused until M6. The field exists from M0.
- A `warm` app keeps a second copy of its volume on its **warm node**, shipped from the node that holds its lease every `interval_s` (30 s by default) and kept with its prepared image and a standby VM that starts nothing (ADR 0172 to ADR 0174). An operator, or an agent with a human's approval, **fails over** to the warm node once the old node's lease is lost, or runs a planned **switchover** that loses no data (ADR 0175, ADR 0177). A node that lost the lease keeps its divergent writes as a **quarantined copy** of the volume and becomes the warm copy (ADR 0176).
- A **domain** belongs to one app and has a TLS mode: `acme` (public-IP hosts) or `tunnel` (Cloudflare Tunnel) (D-0024). Its serving status says whether a machine can serve the hostname at all, apart from its certificate (ADR 0167).
- An app either deploys images given by reference or has a **Git source** (`source`): then its images come from **builds** of that source, and each successful build starts a deployment with `trigger: build` (ADR 0117). A build runs on the node that runs the app (ADR 0118).
- From M5a the node that prepares an image sends its package **inventory** to the primary, which matches it against a local copy of OSV data and records **vulnerability findings** for the apps that run the image, for the running Quoro release's own components and for host kernels (ADR 0138, ADR 0150). The image never leaves the node. A finding in CISA's KEV catalog or with a CVSS score of 9.0 or more raises `app.vulnerable`, which opens an incident. An app with a Git source is patched by a build with `refresh_base`; Quoro never upgrades a host or an image it did not build.
- An **interactive session** is a web terminal into an app's VM or a read-only console of a VM (ADR 0114). It lives on the primary while it is open and leaves two audit records, its open and its `session.end` (ADR 0115).

### Built images

The image a build produces never leaves the node that built it (ADR 0118), so it has no registry reference. Every contract names it with the reference

```
build.quoro.invalid/<app_id>:<build_id>
```

and the build's manifest digest as its digest, for example `build.quoro.invalid/app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E:bld_01JC9ZA2B3C4D5E6F7G8H9J0KM` with `sha256:...`. The host is under the reserved `.invalid` top-level domain (RFC 6761), so the reference parses as an ordinary OCI reference but can never be pulled: a node prepares it from the build's OCI layout and never contacts a registry for it. `App.image`, `Deployment.image`, `vms.image` and `desired_state` `spec.vms[].image` all carry it verbatim.

### Status enums

- Node `status`: `pending` (enrolled, never connected), `connected`, `disconnected`, `fenced`, `removed`.
- VM `desired_status`: `running`, `stopped`. VM `observed_status`: `creating`, `stopped`, `starting`, `running`, `stopping`, `crashed`, `deleted`, `unknown`.
- App `status`: `deploying`, `running`, `degraded`, `down`, `stopped`.
- Deployment `status`: `pending`, `preparing` (pulling image, building rootfs), `starting`, `succeeded`, `failed`, `superseded`.
- Build `status`: `queued` (waiting for the node), `preparing` (builder VM and source checkout), `building`, `exporting` (writing the OCI layout), `importing` (turning the layout into a root disk), `succeeded`, `failed`, `cancelled` (ADR 0117). The last three are terminal.
- Domain `cert_status`: `pending`, `issued`, `renewing`, `failed`, `not_applicable` (tunnel mode).
- Grant `status`: `active`, `expired`, `revoked`, `exhausted`.
- Approval `status`: `pending`, `approved`, `denied`, `expired`.
- Incident `status`: `open`, `resolved`.
- Lease `mode`: `local` (HA policy `none`/`cold`: never expires when the arbiter is unreachable; ends by `lease_revoke` or an operator declaring the node lost) or `expiring` (HA policy `warm`/`hot`, from M5b: `held` -> `uncertain` -> `expired`, self-fence at `expired`; revoked explicitly) (D-0047). Lease state as seen by a node: `held`, `uncertain`, `expired`, `revoked`. See node-agent-protocol.md. From M5b the API shows each app's lease (`App.lease`), as the primary sees it.
- Webhook delivery `status`: `pending`, `delivered`, `failed`.
- Volume `state`: `creating` (the node has not reported the file yet), `ready`, `restoring` (being written from a backup), `failed`, `detached` (replaced by a restore, its file kept on its node until deleted), `deleting` (ADR 0145).
- Warm copy `state` (`App.warm`, M5b): `syncing`, `in_sync` (a lag of at most two intervals), `lagging`, `failed` (ADR 0173). Quarantined copy `state`: `kept`, `deleting` (ADR 0176).
- Backup `status`: `queued`, `running`, `succeeded`, `failed` (ADR 0147). The last two are terminal.
- Volume snapshot method of a node: `reflink` (native or in the node's XFS volume pool) or `pause_copy` (ADR 0156).
- Audit export `lock_strength`: `none`, `bucket_rule` (R2 lock rules), `compliance` (S3 Object Lock in compliance mode) (ADR 0149).

### Scopes

A scope is `<resource>:<verb>` (D-0030). The complete list:

| Scope | Allows | Flags |
| --- | --- | --- |
| `cluster:read` | Read cluster info, version, log position | |
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
| `backup:read` | List and read backups and backup repositories | |
| `backup:write` | Take a backup of an app's volume now | |
| `backup:restore` | Restore a backup onto a node, which moves the app there | destructive |
| `backup:manage` | Create a backup repository, rotate its recovery key, set the control-plane backup schedule | human-only |
| `agent:read` | List and read agents | |
| `agent:write` | Create, update, disable agents; rotate agent keys | human-only |
| `grant:read` | List and read grants | |
| `grant:write` | Create and revoke grants | human-only |
| `approval:read` | List and read approval requests | |
| `approval:create` | File an approval request | |
| `approval:decide` | Approve or deny an approval request | human-only |
| `incident:read` | List and read incidents | |
| `incident:resolve` | Resolve an incident | |
| `audit:read` | Read the audit log and run verification, and read the status of its off-site export; humans also fetch session transcripts | |
| `event:read` | Read events and the event stream | |
| `webhook:read` | List and read webhooks and deliveries | |
| `webhook:write` | Create, update, delete, test webhooks | human-only |
| `provider:manage` | Create, list, read, delete providers and rotate their secrets; map nodes to tunnel providers; configure and probe the off-site audit export | human-only |
| `user:read` | List human users, to resolve user ids to names | human-only |
| `user:write` | Manage human users and their roles | human-only |

- **destructive** scopes need JIT approval when an agent uses them: the agent files an approval request, a human approves it, and a short-lived extra grant is issued (D-0030).
- **human-only** scopes can never appear in an agent's grant, so an agent cannot widen its own power or approve its own requests.
- The wildcard `app:*` style is not allowed in grants; scopes are listed explicitly.
- Human roles are fixed scope bundles: `owner` (every scope), `operator` (every scope except `user:write` and `backup:manage`), `viewer` (every `:read` scope plus `app:logs`). `user:read` is a `:read` scope, so every role holds it. `app:terminal` and `vm:console` are not `:read` scopes, so a `viewer` holds neither (ADR 0114). `backup:manage` is owner-only because it shows the recovery key, which reads every backup (ADR 0147).
- Volumes add no scope of their own (ADR 0145): creating one changes an app's settings (`app:update`), listing them reads the app (`app:read`), and deleting one deletes app data (`app:delete`, destructive, so an agent needs a JIT approval). From M5b, deleting a quarantined copy needs `app:delete_quarantine`, which only a human holds, and the N+1 indicator reads the cluster (`cluster:read`).
- Builds and metrics add no scope of their own: a build is authorized as a deployment of its app (`app:deploy`), its record as app data (`app:read`), its log as app output (`app:logs`); node metrics need `node:read` and app metrics `app:read`.

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
| `node.fenced` | A node self-fenced stateful apps after losing a lease, every bound VM confirmed stopped | |
| `node.fence_failed` | A node could not confirm a fence stopped its stateful VMs; it marked itself failed | |
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
| `app.degraded` | An app with more than one replica has at least one instance out of the edge's rotation and at least one in it (ADR 0143) | |
| `app.up` | An app recovered, from down or from degraded | |
| `app.restarted` | An app's instances were restarted | |
| `app.vulnerable` | The image an app runs has a new finding in the KEV catalog or with a CVSS score of 9.0 or more (ADR 0150); it opens an incident of its own, resolved when no such finding is open | critical CVE |
| `app.failed_over` | The lease of a `warm` app moved to its warm node, which starts the app from its last complete copy (ADR 0175). It opens no incident | |
| `app.switched_over` | A planned switchover of a `warm` app completed with no data lost; the former node is now its warm copy (ADR 0177). It opens no incident | |
| `app.switchover_failed` | A planned switchover stopped before the lease moved; the app resumed on its node and nothing moved. It opens no incident | |
| `deployment.started` | A deployment began | |
| `deployment.succeeded` | A deployment's instances are healthy | |
| `deployment.failed` | A deployment failed (image pull, rootfs, boot, health) | |
| `build.started` | A build was created and queued | |
| `build.succeeded` | A build produced its image; the deployment it starts follows | |
| `build.failed` | A build failed (source checkout, a build step, export, import, timeout) | build failed |
| `build.cancelled` | A build was cancelled before it finished | |
| `backup.succeeded` | A backup finished and its seal was written | |
| `backup.failed` | A backup failed (snapshot, upload, seal) | backup failed |
| `restore.succeeded` | A restored volume is ready on its node and the app was moved there | |
| `restore.failed` | A restore failed; the new volume is `failed` and the app stays stopped until another restore | |
| `volume.quarantined` | A node that lost a `warm` app's lease kept its divergent volume as a quarantined copy, recorded in the audit log (ADR 0176). It opens no incident | |
| `certificate.issued` | A TLS certificate was obtained or renewed | |
| `certificate.expiring` | A certificate expires within its warning window | certificate expiring |
| `certificate.failed` | Obtaining or renewing a certificate failed. A primary that does not yet emit `domain.unserved` also emits it with `tls_mode` `tunnel` for a `tunnel` domain no tunnel serves | |
| `domain.unserved` | No machine can serve a domain's hostname although its app has an instance meant to run; the domain's `serving_status` became `unserved` (ADR 0167). It opens no incident | |
| `domain.served` | A machine serves an `unserved` domain's hostname again | |
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

For `forbidden`, the field `reason` is one of: `scope_missing`, `selector_mismatch`, `grant_expired`, `grant_revoked`, `grant_exhausted`, `human_only`, `agent_disabled`. From M5b `reason` also says why a failover or a switchover was refused with `conflict` (`not_warm`, `in_progress`, `no_copy`, `not_in_sync`, `node_disconnected`, `fence_failed`, `no_support`, `lease_held`), and why a delete was (`has_quarantines`). For `approval_required`, the body carries `scope` and the target so the agent can file an approval request.

### Request headers

- `Authorization: Bearer <token>` (human API token or agent access token); the panel uses a session cookie.
- `Idempotency-Key` on every `POST` that creates something.
- `Quoro-Justification`: the agent's stated reason for the call, required on every mutating call by an agent, recorded in the audit log (D-0031).
- `Quoro-Setup-Token`: the one-time setup token, required by `POST /api/v1/setup` only (D-0054).
- `Quoro-Incident`: the incident id the call belongs to, when an agent acts on an incident.
- `Last-Event-ID` on SSE streams (`GET /api/v1/events/stream`, `GET /api/v1/apps/{app_id}/logs/stream`, `GET /api/v1/builds/{build_id}/logs/stream`): resume after the given `seq`. The metrics streams (`GET /api/v1/nodes/{node_id}/metrics/stream`, `GET /api/v1/apps/{app_id}/metrics/stream`) carry no resumable position and ignore it. One-way streams use SSE; bidirectional sessions use WebSockets (D-0057): the web terminal and the VM console (`GET /api/v1/interactive-sessions/{session_id}/ws`).
- Every response carries `Quoro-Request-Id` (`req_...`).
- Webhook deliveries carry `Quoro-Event-Id`, `Quoro-Event-Type`, `Quoro-Delivery-Id` and `Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<raw body>" with the webhook secret>`. Receivers reject a signature older than 5 minutes.
- Webhook delivery is at-least-once, also across a restart of the primary (ADR 0140). The dispatcher keeps a durable cursor, the `seq` of the last event it has dispatched, in the local `meta` table; it advances the cursor in the same transaction that records that event's first delivery attempts, and at start-up it resumes after the cursor, so an event committed before a stop or during start-up is still delivered. On the very first start there is no cursor and the dispatcher begins at the current highest `seq`, without replaying older history. A first attempt that was recorded but not sent before a stop is sent after the restart. Because a stop between sending a delivery and recording its outcome sends it again, a receiver can get the same event more than once: the event `id` (also sent as `Quoro-Event-Id`) is unique and is the dedupe key; `Quoro-Delivery-Id` names one attempt and differs between them. Catch-up is bounded: after a long stop, events whose `time` is more than 24 hours old are skipped, not delivered, and the dispatcher logs a `dispatcher.gap` line with the skipped `seq` range.

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

M4 adds no MCP tool. Builds, metrics and console sessions are reachable by agents through the HTTP API with a grant; the web terminal is human-only (ADR 0114); a later tool is a contract change of its own.

M5a adds no MCP tool either. Volumes, backups and restores are reachable by agents through the HTTP API with a grant (a volume delete and a restore only through a JIT approval); backup repositories, their recovery key and the off-site audit export are human-only. Replicas are a field of `updateApp`, with no tool of their own. Vulnerability reports are read through the HTTP API (`app:read`), and an agent woken by `app.vulnerable` rebuilds through `startBuild` with `refresh_base` (`app:deploy`), also with no tool of their own (ADR 0155 defers automatic rebuilds to M5b). A later tool is a contract change of its own.

M5b's lease and witness contract adds no MCP tool: the roles operation and declaring a node lost are human-only, and an agent reads the voters through `get_cluster` and an app's lease through `get_app`.

M5b's warm contract adds no MCP tool yet. An agent reads an app's warm copy through `get_app` and fails over or switches over through the HTTP API with a JIT approval (`app:failover`); deleting a quarantined copy is human-only. ADR 0178 names an MCP tool `get_capacity` for the N+1 indicator (`getClusterCapacity`, `cluster:read`); it joins this table, and its annotation joins `openapi.yaml`, with the change that implements the operation, because the MCP server's tool list must match this table.

### Channels

Every action reaches the same service layer through one of these channels, recorded in the audit log as `channel`: `api` (HTTP API), `panel` (the web panel, which uses the API with a session), `mcp` (MCP server, D-0031), `cli` (the `quoro` command), `system` (Quoro itself: reconciliation, schedulers, incident automation). MCP tools map one-to-one onto service operations and require the same scope as the matching endpoint.

### Node-agent message types

Defined in node-agent-protocol.md. Node to control plane: `hello`, `heartbeat`, `status_report`, `event`, `lease_renew`, `log_ack`, `stream_data`, `stream_end`, `command_result`, `dns_challenge`, `image_inventory`, `support_grant`. Control plane to node: `welcome`, `desired_state`, `lease_grant`, `lease_revoke`, `log_entries`, `stream_open`, `stream_close`, `stream_input`, `stream_ack`, `command`, `signing_key_rotate`, `dns_challenge_result`, `support_request`, `error`. Either side: `ping`, `pong`.

### Database tables

Defined in db-schema.sql and changed only by the numbered files in `migrations/`: `meta`, `change_log`, `nodes`, `node_join_tokens`, `node_credentials`, `node_tunnels`, `projects`, `vms`, `apps`, `app_env`, `deployments`, `builds`, `domains`, `volumes`, `volume_quarantines`, `leases`, `users`, `api_tokens`, `sessions`, `agents`, `agent_keys`, `access_tokens`, `grants`, `approvals`, `incidents`, `events`, `webhooks`, `webhook_deliveries`, `providers`, `audit_log`, `audit_exports`, `interactive_sessions`, `backup_repositories`, `backups`, `osv_mirror`, `vulnerability_findings`. Tables marked local in that file are not replicated through the change log.

## Open items before the freeze

None. The last open element, custody of the cluster keys on promotion, is decided: the keys are replicated to every machine with the `node` role, sealed to a custody key of each machine (ADR 0078, read by ADR 0180), together with the primary's TLS key (ADR 0181). The fields and the table that carry the seals are specified with manual promotion in M5b. The image pipeline and init are decided (D-0061), and so is the snapshot primitive: reflink snapshots, natively or in a node-managed XFS volume pool, for the M5a `cold` backups (ADR 0156, which supersedes D-0062 and ADR 0146), and the same snapshots compared by their extent maps (FIEMAP) to find the changed ranges for `warm` replication in M5b, with no lvm-thin and no btrfs (ADR 0172). Every other element formerly listed here is decided (D-0047, D-0048, D-0050, D-0051, D-0052, D-0053, D-0054, D-0056, D-0057, D-0061, D-0062, D-0067, D-0071, D-0072, D-0073, D-0074).
