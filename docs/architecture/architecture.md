# Quoro architecture

Status: M0 draft for review. Where an accepted decision settles a choice, the text cites its D-id; a recommendation is marked pending maintainer decision only where its question is still open.

Quoro is one self-hosted binary that unifies a hypervisor with application deployment: it runs one microVM per app on Cloud Hypervisor and KVM, and the same interface deploys, exposes and observes those apps. This document describes how the pieces fit together: the roles of the binary and the control-plane statuses, the split between control plane and data plane, how nodes stay autonomous when the control plane is unreachable, how state is replicated through a hash-chained change log, where telemetry lives, the engine layer, networking, ingress, providers, identities and grants, the realtime transport (D-0057), the high-availability groundwork laid in the MVP, the key flows, the confirmation of the baseline stack (D-0056, superseding D-0028), the workspace layout, and every open question that this document depends on.

## 1. Scope and reading guide

This document is the architecture reference named in D-0040 and the M0 architecture deliverable of D-0033. It covers component boundaries, process model, data flow, failure behaviour and the interfaces between workstreams. It does not restate contracts: those are separate files that freeze at the end of M0 (D-0036).

In scope:

- The components inside the single `quoro` binary and which of them run under which role and control-plane status (D-0012).
- The control-plane and data-plane split, and what still works when the control plane is down (D-0010, D-0011).
- State storage, the change log and replication (D-0013, D-0015).
- Telemetry locality (D-0014), the engine layer (D-0007, D-0008, D-0009), networking (D-0010, D-0034), ingress (D-0024) and provider interfaces (D-0025).
- Identities, grants, audit and the AI-native loop (D-0029 to D-0032).
- The realtime transport (D-0057), the HA groundwork (D-0016 to D-0023, D-0033), the key flows, the stack confirmation (D-0056, superseding D-0028) and the workspace layout.

Out of scope: product requirements (see `docs/product/roadmap.md`), the threat model (see [../architecture/threat-model.md](../architecture/threat-model.md)), and the contents of each contract, which are linked but not copied.

### Contracts this document depends on

| Contract | What it fixes |
| --- | --- |
| [../contracts/openapi.yaml](../contracts/openapi.yaml) | The HTTP API, its operations, scopes and error codes |
| [../contracts/events.schema.json](../contracts/events.schema.json) | Event envelope and every event type |
| [../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md) | Enrollment, the dial-out session, messages, reconciliation, leases and self-fencing |
| [../contracts/audit-log-format.md](../contracts/audit-log-format.md) | Audit record format, chaining, verification, off-site copy |
| [../contracts/permission-model.md](../contracts/permission-model.md) | Identities, scopes, selectors, grants, approvals |
| [../contracts/db-schema.sql](../contracts/db-schema.sql) | Control-plane SQLite schema: change log and derived tables |
| [../contracts/engine-trait.md](../contracts/engine-trait.md) | The `Engine` trait, its types and semantics |

Vocabulary is fixed by [../contracts/README.md](../contracts/README.md); terms follow [../glossary.md](../glossary.md). Every name used here for roles, statuses, ids, scopes, event types, errors, headers, channels, node-agent messages and tables comes from those two files.

### Labelling convention

Every design statement below carries one of these labels:

- **Decision** (`D-xxxx`): accepted and binding. An accepted decision is law; changing one requires a new decision approved by the maintainer (D-0036).
- **Finding** (`F-xxxx`, or a `docs/research/<file>.md` path): a sourced fact about external software, gathered before it entered a document (D-0040).
- **Pending maintainer decision**: a recommendation or an undecided design choice. Written as "recommended, pending maintainer decision"; it is not a decision and must not be implemented as one.
- **Open** (`Q-xxxx`): an unanswered question. Options are listed and the recommendation, if any, is marked pending maintainer decision; the question stays open.
- **Hypothesis**: a claim about external software, or about behaviour this project has not yet run, that is not established by the findings. Each one is collected in section 19 with what would verify it.

Anything stated without one of these labels and without a citation is a **Hypothesis**.

## 2. System overview

There is one binary, `quoro`, on every machine (D-0012). The binary bundles the components below; which of them run depends on the role (`node`, `witness`, `edge`) and, for a node, on its control-plane status (`primary`, `replica`) (D-0012).

| Component | What it does | Runs on (role; milestone for M5 items) |
| --- | --- | --- |
| HTTP API | The REST surface of [../contracts/openapi.yaml](../contracts/openapi.yaml), served by Axum | node |
| MCP server | Streamable HTTP at `/mcp` on the same server, plus a stdio mode in the CLI; calls the same service layer as the API (D-0031) | node |
| Embedded panel assets | The React panel, embedded with rust-embed | node |
| State store | SQLite: the change log and the tables derived from it (D-0015) | node (writable on the primary) |
| Scheduler | Chooses the node for a deployment and writes desired state (D-0011, D-0012) | primary |
| Event bus | Turns state changes and node reports into events (D-0032) | primary |
| Webhook dispatcher | Delivers signed events to subscribers, with retries and delivery records (D-0031, D-0032) | primary |
| Audit log | Append-only, hash-chained record of every action (D-0031) | primary writes; replicas hold a copy |
| Node agent | Holds desired state locally, reconciles it, dials out, self-fences (D-0011, D-0019) | node |
| Engine | The `Engine` trait and its Cloud Hypervisor implementation (D-0007) | node |
| Ingress | The `Ingress` interface: reverse proxy with ACME, or Cloudflare Tunnel (D-0024) | node |
| Witness | Breaks ties; stores no data (D-0017) | M5b |
| Edge | Traffic entry point and load balancer; can also be the third voter (D-0012, D-0018) | M5a |

```
   humans (panel, session cookie)        agents (MCP, access token)
                 \                              /
                  \                            /
            +------------------------------------------+
            |   primary  (role: node)                  |
            |   HTTP API  ----  MCP server  (/mcp)     |
            |       |                |                 |
            |   service layer (auth, grants, audit)    |
            |       |                |                 |
            |   state store (SQLite: change_log +      |
            |   derived tables)   audit log  scheduler |
            |   event bus  webhook dispatcher          |
            +------------------------------------------+
               ^   ^   ^                   |
   dial-out    |   |   |                   | providers (D-0025)
   WebSocket   |   |   |                   v
   sessions    |   |   |            +----------------------+
   (TLS, node  |   |   |            |  DNS   LB   object  |
    opens them)|   |   |            |  provider  store    |
               |   |   |            +----------------------+
     +---------+   |   +---------+
     |             |             |
+----------+  +----------+  +----------+
|  node    |  |  node    |  |  node    |
| agent    |  | agent    |  | agent    |
| engine   |  | engine   |  | engine   |
| ingress  |  | ingress  |  | ingress  |
| telemetry|  | telemetry|  | telemetry|
+----+-----+  +----+-----+  +----+-----+
     |             |             |
  Cloud Hypervisor microVMs (one per app, KVM)

  - - - - - - - - - - - - - - - - - - - - - - - - -
  witness  (M5b: tie-break vote, no data)
  edge     (M5a: traffic entry, load balancer;
            can combine with witness, D-0018)
  - - - - - - - - - - - - - - - - - - - - - - - - -
```

The arrows into the primary are inbound *sessions*, not inbound connections: each node agent opened that session outward (D-0011). Nothing in the design dials into a node.

### Process model

There is one OS process per machine, not one per component (D-0012). Inside it:

| Concern | How it is arranged |
| --- | --- |
| HTTP and MCP | One Axum server, one listener; `/mcp` is a Tower service mounted beside the REST routes (F-0041) |
| Long-running work | Tokio tasks: the reconciler, the session reader and writer, the scheduler tick, the webhook dispatcher |
| Blocking work | Image pulls, rootfs builds and snapshot copies run on a bounded blocking pool, so a slow pull cannot starve the API |
| MicroVMs | One VMM child process per microVM, owned by the engine; Quoro never shares memory with a guest |
| Licensing boundary | The VMM is a separate process under its own license (F-0036). That a separate process keeps AGPL-3.0 Quoro (D-0004) and Apache-2.0/BSD-3-Clause Cloud Hypervisor cleanly separated is a legal reading, not a reviewed conclusion: **Hypothesis** (`docs/research/cloud-hypervisor.md`) |
| Panel | Static assets compiled into the binary with rust-embed; no Node process at runtime |
| State | One writer connection to SQLite on the primary; readers get their own connections |

The process model follows from two constraints. Nothing that can block a request may run on the request path, because a deploy that waits on a registry pull must not make the panel unresponsive. And nothing that runs a guest may share an address space with the control plane, because a compromised app must not reach Quoro's memory (D-0008).

Privileges are split the same way (ADR 0089, which replaces the M1 root service of ADR 0088). The control plane (`quoro serve`, unit `quoro.service`) runs as the `quoro-api` system user with an empty capability bounding set, `NoNewPrivileges` and a read-only system except its data directory `/var/lib/quoro`: it signs desired state and never touches a hypervisor, a bridge or a firewall. The node agent (`quoro node`, unit `quoro-node.service`) is the only privileged process: it runs the engine as root, keeps its state in `/var/lib/quoro-node`, and dials the control plane like any other node, over loopback on a primary. Its unit uses `KillMode=process`, so restarting the agent leaves the VMMs running and the next agent adopts them. A development host may still run both in one process with `[node] local = true`, which then needs root.

### Host layout, ports and CLI

Decided (D-0054):

- **One listener.** One HTTPS listener, default port 8443 and configurable, serves the API, the panel and MCP. `ingress` owns 80 and 443 on the same host, so the control plane never competes with a customer's domain for those ports.
- **Certificate.** The primary's certificate is self-signed at install, and the join command carries its SHA-256 fingerprint, which is the pin model of section 5. Rotating that certificate therefore invalidates every outstanding join command. An operator may later replace it with an ACME certificate.
- **Paths.** Control-plane data lives in `/var/lib/quoro`, node-agent data in `/var/lib/quoro-node` (siblings, never nested), configuration in `/etc/quoro/quoro.toml` (TOML), and logs go to journald.
- **CLI.** `quoro serve --role node|witness|edge`, `quoro version`, `quoro join <url> --token --fingerprint`, `quoro node`, `quoro mcp stdio`, `quoro setup-token`. `quoro join` only enrolls; `quoro node` runs the node agent.
- **First run.** The installer prints a one-time setup token that `POST /api/v1/setup` requires in the `Quoro-Setup-Token` header: single use, valid for one hour. The primary keeps only the token's SHA-256 hash and its expiry in the data directory. `quoro setup-token` issues a fresh token, invalidating the previous unused one, and refuses once an owner exists.

Alternatives not chosen: ACME for the listener from install, which needs a public name and fails behind NAT; and binding `/setup` to localhost, which breaks remote installs.

## 3. Roles and control-plane status

Roles are what a machine does; statuses are what a node's control-plane component is. They are different axes (D-0012).

Roles (combinable, D-0012, D-0018):

| Role | What it runs | Since |
| --- | --- | --- |
| `node` | Node agent, engine, ingress, local telemetry, and — when it is not a replica — the control plane | M1 |
| `witness` | The tie-break vote only; stores no data (D-0017) | M5b (D-0033) |
| `edge` | Traffic entry point and load balancer; can combine with `witness` (D-0018) | M5a (D-0033) |

Combining roles is a deployment choice, not a code branch: a machine that is `witness`+`edge` runs the witness vote and the load balancer from the same binary (D-0018). A machine can be `node` and `edge` at once, which is what a two-machine cluster with a load balancer on one of them looks like.

**A single edge is a traffic single point of failure (D-0018).** With the edge also a voter, losing it removes both the traffic path and one vote. D-0018's mitigations are Cloudflare as the traffic edge with the small VPS acting only as witness, or two edges with DNS failover. Verify in M5b that the chosen mitigation keeps both traffic and quorum when the edge dies.

Control-plane status of a node:

| Status | Runs | Accepts |
| --- | --- | --- |
| `primary` | API + MCP + panel assets, state store, scheduler, event bus, webhook dispatcher, audit log, node agent | All reads and all writes; it is the single source of truth (D-0012) |
| `replica` | API + MCP + panel assets (read paths), a verified copy of the change log and state tables, node agent | Reads; writes are forwarded to the primary, and fail with `not_primary` (503) when forwarding is not possible (D-0012) |

What exists per milestone:

- M1 to M4 have exactly one primary, which is also a node, plus nodes that run the node agent and dial out to it (D-0016, D-0033).
- Witness, edge and leases with expiry arrive with M5 (D-0033): `witness` in M5b, `edge` in M5a.
- Replica nodes serve reads and forward writes to the primary (D-0012). Turning that serving on requires promoting a replica to primary; manual promotion ships in M5b, alongside leases and fencing, and refuses while the old primary may still hold expiring leases unless an operator acknowledges the fence (D-0067).

Design choice, stated as such: from M2 every enrolled node already follows the change log over its session and keeps a verified copy, so a later manual promotion needs no protocol change (D-0013). The M2 node agent therefore implements the follow path even though nothing else in M2 uses a replica. This costs storage on each node and buys the ability to promote without a redesign.

## 4. Control plane and data plane

The **control plane** is the API, the state store and the audit log on the primary: the layer that decides what should run and records who asked for it. The **data plane** is the node agents, the engine, the ingress and the apps themselves: the layer that actually runs things (D-0010, D-0011, D-0012).

The principle is that the two fail independently. If the control plane dies, apps keep running and the node agents keep restarting them; only changes are blocked (D-0010, D-0011, D-0012).

| Without the control plane | State |
| --- | --- |
| Running apps | Keep running (D-0011) |
| Local restarts, health checks and crash backoff | Keep working on each node (D-0011, D-0019) |
| Ingress routing | Keeps serving the last applied route table (D-0024) |
| Certificates already issued | Keep being served (D-0024) |
| Local logs and metrics | Keep being written and kept on the node (D-0014) |
| Deploys, config changes, new grants and access tokens | Blocked (D-0012, D-0030) |
| Approvals | Blocked: a human cannot approve in the panel, and the API that records the decision is down (D-0030) |
| Panel and MCP actions | Blocked (D-0012) |
| Webhooks | Not dispatched (D-0031, D-0032) |

One row depends on the ingress backend (D-0024): a reverse proxy on a public-IP host renews certificates locally with ACME, so renewal continues while the primary is down; a host behind a Cloudflare Tunnel needs nothing renewed locally because the tunnel terminates TLS at the provider.

The data plane never infers intent. A node with no session to the primary applies the last desired state it persisted and nothing else. This is what makes "the control plane is down" a change freeze rather than an outage (D-0011).

### Failure behaviour by component

| If this fails | What happens | What does not happen |
| --- | --- | --- |
| The primary | Apps keep running; replicas serve reads and refuse writes they cannot forward (`not_primary`) | No automatic election before M6 (D-0016) |
| A replica | Nothing user-visible; the primary keeps serving | No state is lost, because the primary is the only writer (D-0012) |
| A node | Its apps stop; its session drops and `node.disconnected` fires; the app is reported `down` or `degraded` | Nothing is started elsewhere automatically before M6 (D-0016, D-0033) |
| The engine on one node | That node cannot create or restart microVMs; running microVMs continue unless the VMM died with it | The cluster does not rebalance; placement is explicit |
| The ingress on one node | Its apps are unreachable through that node; `certificate.failed` or `app.down` may fire | Traffic is not moved automatically before M5a, and never without a healthy endpoint to move to |
| The witness | Losing it removes the third vote: with two remaining voters no automatic decision (M6) can be taken safely, and `expiring` leases stay renewable while the authority itself is reachable (D-0017) | Nodes do not self-fence merely because the witness is down (D-0047) |
| A provider (DNS, LB, object store) | The operation that needed it fails with a clear error; state stays consistent | Backups do not half-write: a failed upload leaves no chunk referenced by a manifest |

The third column is as important as the second. Every failure mode that could be "helpfully" automated is left manual until the milestone that owns it, because the alternative in a partition is two writers (D-0016, D-0019).

## 5. Nodes: autonomy and dial-out

Enrollment is initiated by a human: `node:enroll` creates a one-time join token, and the node redeems it to receive its identity (D-0011; scope and prefix from [../contracts/README.md](../contracts/README.md)). The node generates its own keypair; the private key never leaves the machine.

The sequence:

1. A human with `node:enroll` calls `POST /api/v1/nodes/join-tokens` and gets a one-time `njt_` token (1 h default) and a join command.
2. The join command carries the primary URL, the token and the SHA-256 fingerprint of the primary's TLS certificate, which the node pins: a certificate whose digest differs is refused, so a self-signed certificate is safe to accept.
3. The node generates an Ed25519 keypair, keeps the private key locally, and calls `POST /api/v1/nodes/enroll` with the token, its name, its public key, its roles, its capacity and the KVM probe result.
4. The primary marks the token used (it is one-time), records the node with status `pending`, emits `node.enrolled`, and returns `node_id`, `cluster_id`, `primary_url`, a node credential (shown once, stored hashed) and the cluster public key that signs desired state.
5. The node stores these and opens the session with `GET /api/v1/nodes/session` using the credential; `hello` and `welcome` follow on that session.
6. `pending` becomes `connected` when the first session starts, and `node.connected` is emitted.

Reconnects reuse steps 5 and 6 only and never re-enroll: a lost credential is recovered by removing and re-enrolling the node by a human. That is deliberate: enrollment and reconnect must not be two implementations of the session. The flow is fixed by [../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md) section 2.

After enrollment the node opens a long-lived WebSocket-over-TLS session to the primary and keeps it open. The direction is fixed: the node dials out, the control plane never dials in (D-0011). That is what lets the control plane live anywhere — including behind NAT itself — and lets nodes work from networks that accept no inbound connections.

On its side the node keeps:

- **Desired state** in a local SQLite store, written from `desired_state` messages and persisted before it is applied (D-0011).
- **A reconciler** that compares desired state with observed state continuously, not only on message arrival. This is what makes a node converge after a crash, a reboot or a long disconnect.
- **Health checks** per instance, whose results become `status_report` messages and, on failure, `event` messages (D-0032).
- **Crash restart** with backoff: a VM that exits without being asked to stop is restarted locally (D-0011). Restarts are requested by bumping `restart_generation` in desired state, so a repeated request is idempotent and a disconnected node still performs the restart it was last told to perform.
- **An event buffer**, so events raised while disconnected are replayed on reconnect; the control plane deduplicates them by their event ids.

Desired state carries a monotonic `rev`. A node applies a `desired_state` only if its `rev` is newer than the one it holds, which makes re-delivery harmless and makes the "everything after N" catch-up in section 6 well defined.

Message types on the session, named exactly as in [../contracts/README.md](../contracts/README.md) and specified in [../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md):

| Direction | Messages |
| --- | --- |
| Node to control plane | `hello`, `heartbeat`, `status_report`, `event`, `lease_renew`, `log_ack`, `stream_data`, `stream_end`, `command_result` |
| Control plane to node | `welcome`, `desired_state`, `lease_grant`, `lease_revoke`, `log_entries`, `stream_open`, `stream_close`, `command`, `error` |
| Either | `ping`, `pong` |

The session is a WebSocket. That is a transport choice for a machine-to-machine protocol and is independent of the panel realtime split in section 13 (D-0057).

## 6. State: SQLite and the change log

Control-plane state lives in SQLite on the primary (D-0015). SQLite is chosen because "one binary, one command" rules out a database server, and because the MVP has a single writer anyway (D-0015).

Every state change is one numbered, hash-chained entry in the change log, appended in the same SQLite transaction that updates the state tables (D-0013, D-0015). The state tables are derived: they can be rebuilt by replaying the log from genesis. That property is what makes replication, verification and recovery one mechanism instead of three.

Change-log entry fields (schema in [../contracts/db-schema.sql](../contracts/db-schema.sql)):

| Field | Meaning |
| --- | --- |
| `seq` | Gap-free sequence number, starting at 1 |
| `term` | Leader epoch; always 1 until elections exist, prepared for D-0016 |
| `time` | Timestamp of the change |
| `actor` | The identity that caused it: human, agent, or `system` for reconciliation |
| `entity_type`, `entity_id` | What changed |
| `op` | The operation on that entity |
| `payload` | The change itself, as JSON |
| `prev_hash` | Hash of the previous entry; 64 zeros for genesis |
| `hash` | SHA-256 over the RFC 8785 canonical JSON of the entry without `hash` |

A replica asks "everything after N" and verifies the chain as it applies entries (D-0013). Verification is the same check the audit log uses for its own chain (D-0031), so tamper evidence on state comes for free.

How replication runs:

1. The replica sends its last verified `seq` N when it connects, and after that as it advances.
2. The primary streams entries after N in order, each with its hash and `prev_hash`.
3. The replica verifies each hash before applying it, and stops at the first mismatch rather than applying past it. A mismatch is an incident, not a retry.
4. Entries are applied in the same order and in transactions, so a crash mid-batch leaves a consistent state at some `seq` rather than a torn one.
5. The replica records its verified `seq`, which is what it reports on reconnect.

Because state tables are derived, recovery from a bug in a state table's logic is "fix the code, replay the log", not "restore a backup and lose the tail" (D-0015). The log is the durable artifact; the tables are a materialised view of it.

`term` is part of the entry from M0 even though it is constant at 1 while there is one primary: it is prepared for the election work of D-0016 and cannot be added later without rewriting history.

Some tables are local and are not replicated: `sessions`, `access_tokens` and `webhook_deliveries` (marked local in [../contracts/db-schema.sql](../contracts/db-schema.sql)). `api_tokens` and `agent_keys` are replicated, but only as hashes, so a replica holds no usable credential. Two reasons:

- **Secrets.** Session tokens and agent access tokens are the local secrets. Copying them to every replica node multiplies the places they can leak from, and a replica does not need them to serve reads on behalf of a caller who authenticated to that replica.
- **Churn.** Webhook delivery attempts change on every retry and are of interest only to the node that dispatched them. Replicating them would put the noisiest table on the replication path.

Backing up the primary's database off-site is not in the MVP (D-0033), but the design has to name a path. Litestream was suggested in D-0015; Litestream 0.5 has no client-side encryption, which conflicts with the encrypted-backup expectation of D-0023 (F-0040). D-0069 settles the default: SQLite and catalog-template backups go through Quoro's own chunk engine with client-side encryption, taken from consistent snapshots via the SQLite backup API or `VACUUM INTO`. Litestream is not used. The rejected and optional paths, kept for the record:

| Option | What it means | Consequence |
| --- | --- | --- |
| 1. Accept SSE-C | Litestream encrypts nothing; the bucket does it, with a key Quoro holds and sends per request | Provider stores ciphertext at rest, but the key travels with each request |
| 2. Pin Litestream 0.3.x with age | The older line still has client-side encryption | Its 0.3 line is unmaintained (hypothesis); not used (D-0069) |
| 3. Quoro's own chunk engine | Periodic consistent snapshots via `VACUUM INTO` or the SQLite backup API, encrypted client-side like every other chunk (D-0023) | Loses Litestream's ~1 s RPO. This is the decided path (D-0069) |
| 4. Encrypting S3 proxy | An encrypting proxy in front of the bucket | One more component on the backup path |

Litestream with SSE-C may be added later as an optional backend for a lower RPO (D-0069).

### Cluster keys

Decided (D-0050), three keys:

- **Cluster signing key** (Ed25519). Signs `desired_state` and lease messages; nodes pin its public half at enrollment, so losing or rotating it is a cluster-wide re-pin.
- **Cluster data-encryption key** (DEK, 256-bit XChaCha20-Poly1305). Encrypts every `*_enc` column; a `*_enc` column stores the key id, the nonce and the ciphertext, so a row records which key wrote it.
- **Node-local key.** Encrypts a node's local copy of the secrets for the apps placed on it.

Storage and rotation: the keys are generated at first start and stored as 0600 files in `/var/lib/quoro`, outside the SQLite file, and backed up separately, because losing the data-encryption key loses every secret. The signing key rotates through the node-agent message `signing_key_rotate`, signed by the old key. The data-encryption key rotates by re-encrypting rows under a new key id. Custody of both keys when a replica is promoted is decided (ADR 0078, read by ADR 0180): each machine with the `node` role generates a custody key pair, and the primary seals the signing key, the data-encryption key and its own TLS key to each such machine's public custody key, so a promoted node opens them and keeps the certificate nodes pin (ADR 0181). The node-local key is not used for this, because the primary never sees it. The fields and the table that carry the seals are specified with manual promotion in M5b (D-0067); until then the keys do not leave the primary.

## 7. Telemetry locality

App logs and metrics stay on the node (D-0014). Shipping them all to the primary would make the primary the bottleneck and the single point of failure for observability, which is exactly where it is least affordable.

**Logs.** The minimal init forwards the app's stdout and stderr over the microVM's virtio-console to the node agent, which writes size-capped local files (F-0034 covers which console types Cloud Hypervisor offers). The panel and MCP read logs through the primary, which opens a `stream_open` on the node's session on demand and relays `stream_data` back; the primary never stores app logs (D-0014). A bounded pull of app logs is a `command` of kind `logs_query` answered by `command_result`; a live view is `stream_open` (kind `app_logs`), `stream_data`, and `stream_end` / `stream_close`. `log_entries` and `log_ack` are not app-log reads: they are change-log follow messages (section 6).

**Metrics (M4).** Per-VM counters come from the engine, which reads them from Cloud Hypervisor (`docs/research/cloud-hypervisor.md`); host metrics come from the host. They are sampled and kept on the node, and the panel reads them through the primary on the same on-demand path as logs (D-0014).

Optional archiving of telemetry to S3-compatible storage arrives later (D-0014) and is not assigned to a milestone.

If the node is unreachable when a read is attempted, the API returns `node_unreachable` (504), which says what happened: the state is known to the primary, the telemetry is not reachable right now ([../contracts/README.md](../contracts/README.md)).

### Log pipeline and retention

- The minimal init is the app's parent process inside the microVM and forwards stdout and stderr over the virtio-console (F-0034). Line boundaries are preserved; partial lines are flushed after a short timeout so a chatty app cannot hold the last line forever.
- The node agent writes size-capped local files and rotates them. The cap is what keeps a log flood from filling the node's disk, which would take the node down and its apps with it.
- Reads are on demand. A pull of a bounded range is a `command` of kind `logs_query` answered by `command_result`; a live view uses `stream_open` (kind `app_logs`), `stream_data` and `stream_end` / `stream_close` on the node's existing session, so a log view needs no inbound connection to the node (D-0011, D-0014).
- The primary relays and forgets. It holds no copy after the stream ends, which is why the primary's disk usage does not scale with app traffic (D-0014).
- Archiving to S3-compatible storage is optional and later (D-0014). When it exists, it is a node-side upload from the node that holds the logs, not a transfer through the primary.

## 8. Engine layer

The engine sits behind an `Engine` trait (D-0007, D-0009), specified in [../contracts/engine-trait.md](../contracts/engine-trait.md). The trait is the only way the node agent touches virtualization: create, start, stop, delete, and — for M5 — snapshot and volume diff hooks. Cloud Hypervisor is the first implementation; a container runtime is a possible later implementation (D-0009); a fake in-memory engine ships for tests on machines without KVM.

Cloud Hypervisor specifics:

- **One VMM process per microVM**, driven over its HTTP/JSON API on a UNIX socket (`docs/research/cloud-hypervisor.md`). The engine owns the process lifecycle; a crashed VMM surfaces as `vm.crashed` in the node's `event` stream.
- **License**: Apache-2.0 AND BSD-3-Clause (F-0036). Cloud Hypervisor is Apache-2.0 AND BSD-3-Clause; compatibility with an AGPL-3.0 Quoro that runs it as a separate process is a **Hypothesis** (section 19).
- **Host requirements**: a KVM-capable host (D-0009) and host kernel >= 6.8 (F-0036).
- **Nested virtualization**: rare on cheap VPS. Hetzner Cloud, Hostinger, Contabo VPS and netcup VPS state that they do not support it (F-0044, `docs/research/vps-nested-virt.md`). The installer therefore probes `/dev/kvm`, the vmx/svm CPU flag and a real microVM boot instead of trusting provider marketing. A host that fails the probe is refused during install rather than at first deploy.
- **Image path**: OCI image -> root disk -> boot with a minimal init (D-0008). Quoro ships its own minimal Rust init and its own OCI-to-ext4 pipeline (D-0061), validated by the M0 spike (`spikes/oci-microvm`); the pipeline resolves layer paths inside the rootfs with `openat2` and `RESOLVE_IN_ROOT`, and also covers zstd layers and config delivery through a config disk or the kernel command line. The guest kernel is built by Quoro and pinned reproducibly (D-0027).
- **Consoles are text-only.** Cloud Hypervisor has no VNC, virtual GPU or framebuffer device (F-0034). The microVM console is the serial or virtio-console UNIX socket, relayed by the node agent, so consoles are xterm.js over serial or virtio-console only.
- **Disks and snapshots.** Cloud Hypervisor snapshots hold guest RAM and device state, not disk contents, and there is no incremental or dirty-bitmap API (F-0035). The chunk engine (D-0023) therefore needs its own disk snapshot primitive. The primitive is reflink snapshots, natively or in a node-managed XFS volume pool, plus a full content-defined chunk scan for M5a cold backups (ADR 0156, which supersedes D-0062). Warm replication in M5b keeps the same snapshots and finds the ranges that changed between two of them by comparing their extent maps (FIEMAP), with a full hashed scan as the fallback; neither lvm-thin nor btrfs is used (ADR 0172).
- **virtio-fs** needs an external virtiofsd (`docs/research/cloud-hypervisor.md`). The engine therefore does not depend on it: apps boot from a disk image, and virtio-fs is at most an optimisation for a later milestone.
- **No live migration** (D-0034). Moving a running microVM between nodes is not a goal; moving an app is a stop/start with a lease, not a migration.

The engine is the component with the largest surface for a mistake that costs data, which is why its contract is frozen at M0 and why the fake engine exists: engine behaviour must be testable without KVM, and CI must be able to run the node agent's reconciliation logic on an ordinary machine.

## 9. Networking

Networking is deliberately simple: no SDN overlay, no VLANs, no distributed switch (D-0034). Each node is its own L2 island with a small amount of local plumbing, and cross-node traffic is carried by a mesh VPN (D-0010).

Per node:

- **A Linux bridge** that carries the node's microVMs and the ingress.
- **One TAP device per microVM**, created by the engine and attached to the bridge.
- **A private subnet per node**, carved from one cluster-wide range. No two nodes hand out the same addresses, which keeps routes unambiguous when traffic crosses the mesh.
- **nftables rules** in two groups: egress NAT, so a microVM can reach the internet through the node's address; and isolation between microVMs, so no microVM reaches another project's microVMs by default. Flows *into* a microVM exist only from the ingress on the same bridge.
- **Ingress forwarding** to the microVM's private address. The ingress is the only component that talks to the public side.

The bridge, the subnet carving and the address plan are this document's proposal for the engine workstream, not a decision: D-0011 and D-0008 fix that microVMs exist and are isolated, but not how the host network is laid out. Two properties are requirements rather than taste: a microVM must not reach another project's microVMs by default, and the address plan must survive a node being added or removed without renumbering existing microVMs.

A worked proposal for the address plan: one cluster range (for example `10.42.0.0/16`), one `/24` per node, the bridge on the first address of that `/24`, and microVMs allocated upward from it. The point of writing it down is that the mesh needs stable peer addresses, and a per-node `/24` gives them without a central allocator after enrollment.

| Object | Scope | Purpose | Removed when |
| --- | --- | --- | --- |
| Bridge | One per node | Carries microVMs and the ingress on one L2 segment | Node removed |
| Node subnet | One `/24` per node | Stable private addresses without a central allocator | Node removed, and never reused while any microVM from it is recorded |
| TAP device | One per microVM | Attaches the guest's virtio-net to the bridge | MicroVM deleted |
| nftables NAT rules | One per node | Egress from microVMs to the internet through the node's address | Node removed |
| nftables isolation rules | One per node | No microVM reaches another project's microVMs by default | Node removed |
| Forward rules | One per domain | Ingress to the microVM's private address, per route table entry | Domain removed or instance replaced |

Two rules keep this from drifting. Addresses are recorded in state, so a reinstall of a node does not hand out an address that a recorded volume or VM still refers to. And isolation is a default that a project can widen for itself, never one that another project can widen for it.

**Node mesh.** Nodes are independent machines, often at different providers (D-0010), so cross-node traffic — edge-to-replica traffic in M5a, node-to-node volume replication for `warm` in M5b, the chunk engine, and any future node-to-node call — runs over WireGuard (D-0010). The mesh is kernel WireGuard configured by Quoro over netlink, with no `wg-quick`, per-node keypairs and peers plus allowed IPs in desired state; userspace boringtun is only a fallback, and an own implementation is rejected for crypto risk (D-0063). It is first needed in M5a, for cross-node replica traffic, and M2 does not need it; the control-plane session does not depend on it, because that is TLS dial-out (D-0011), so a single-node M2 works with no mesh at all.

Two nodes behind NAT cannot reach each other directly. The mesh therefore assumes at least one publicly reachable peer — a public node or the edge — as a relay hub, or falls back to the dial-out path (D-0063). This is a limitation of the mesh, not of the control plane.

## 10. Ingress

Ingress is one `Ingress` interface with two backends (D-0024):

| Backend | For | How TLS happens |
| --- | --- | --- |
| Reverse proxy with ACME | Hosts with a public IP | ACME (HTTP-01 or TLS-ALPN-01) obtains and renews certificates locally |
| Cloudflare Tunnel | Hosts behind NAT | cloudflared dials out; TLS terminates at the provider |

The reverse proxy is an embedded Rust proxy — hyper + rustls + an ACME client crate — behind the same interface, which keeps one binary and full control of health-aware routing and slow start at the cost of owning the ACME edge cases (D-0059). Caddy driven through its admin API stays a fallback backend, to be used if the M2 spike shows that owning ACME is too costly; crate maturity is a research task before M2.

The route table is derived from desired state: each domain maps to the app instances that may serve it (D-0024). It is written by the control plane and applied by the node, so a route change is a state change with a change-log entry, and a node that cannot reach the primary keeps serving the last applied table (section 4).

Findings to design around (`docs/research/cloudflare-lb-and-tunnel.md`):

- cloudflared replicas of one tunnel are identical copies, routed to the nearest replica, with no app health check.
- The load balancer treats all replicas of one tunnel as a single endpoint, and has no TCP monitors for tunnel origins (F-0046).
- An HTTPS monitor whose `Host` is the app hostname *does* check the app through the tunnel; it is proxied by cloudflared to the app (F-0046, which corrects the broader reading of F-0043).

The consequence is a design rule: per-node failover needs **one tunnel per node behind a load balancer**, not several replicas of one tunnel. Replicas of one tunnel fail over the connector, not the node: if the node behind the tunnel dies, every replica of that tunnel dies with it, and the load balancer cannot see the difference because it sees one endpoint. One tunnel per node turns each node into a distinct endpoint that a monitor can mark down.

Load balancing with health monitors is a paid add-on of the tunnel provider (`docs/research/cloudflare-lb-and-tunnel.md`); the tunnel backend without it gives connector redundancy only, which is why the tunnel path is documented as best-effort until M5a.

Certificates live on the node that serves them (D-0024). There is no central certificate store to synchronise and no certificate secret in the replicated state: an ACME certificate stays in the node's own storage, and a tunnel host has no local certificate at all.

## 11. Providers

Anything Quoro asks of an external service goes through a provider interface (D-0025). Cloudflare is the first implementation, not the only one: the interface exists so the community does not read Quoro as vendor lock-in.

| Interface | Used for | Since |
| --- | --- | --- |
| `DnsProvider` | Creating and updating the records that point a domain at the cluster | M2 |
| `LoadBalancerProvider` | Registering and deregistering endpoints, health monitors | M5a |
| `ObjectStore` (S3-compatible) | Cold backups (M5a), telemetry archive, off-site audit copy from M5a (D-0053) | M5a |
| `LeaseArbiter` | Leases when an external arbiter is used; M5b uses its own voters instead (ADR 0169) | Later |

`ObjectStore` probes capabilities at setup instead of assuming them:

- **Conditional PUT** (`If-Match` / `If-None-Match`). Not portable: R2 and AWS S3 support it, MinIO AIStor documents it, Backblaze B2 apparently does not (F-0038). Anything that depends on it — the conditional-write lease arbiter in section 14 — must be probed per provider at setup.
- **Object lock.** Not yet researched: whether a given provider supports it, and in which mode, is a **Hypothesis** per provider. The off-site audit copy depends on it (D-0031) and is gated by an object-lock probe per provider (D-0053); a research task verifies compliance-mode object lock for R2, S3, B2 and MinIO before M5a.
- **SSE-C.** Presence and behaviour are recorded by the probe, because an optional Litestream backend with SSE-C would depend on it (D-0069, F-0040).

R2 specifics: one write per second per key, and no conditional delete (F-0037). Those two limits rule out using a single object as a high-frequency lease register and rule out a delete-as-lock-release pattern; any arbiter built on R2 must renew at a rate below the write limit and must treat lease release as an overwrite, not a delete.

Backups target any S3-compatible store — R2, B2, MinIO (D-0025) — because a self-hosted tool that only backs up to one vendor is not self-hosted in the part that matters when a machine dies.

### Provider credentials

Provider API tokens, tunnel tokens and S3 keys reach Quoro through a human-only `providers` resource (D-0051):

| Method and path | Purpose |
| --- | --- |
| `POST /api/v1/providers` | Create a provider entry (kind `dns`, `tunnel`, `lb` or `object_store`) |
| `GET /api/v1/providers` | List entries; secrets are never returned |
| `GET /api/v1/providers/{provider_id}` | Read one entry; secrets are never returned |
| `DELETE /api/v1/providers/{provider_id}` | Remove an entry |
| `POST /api/v1/providers/{provider_id}/rotate` | Replace the secret of an entry |

The resource needs the human-only scope `provider:manage`. Entries live in the replicated `providers` table, with the secret in `secret_enc` under the data-encryption key (D-0050). Secrets are write-only and are redacted from audit parameters, so neither a read call nor an audit record returns one. The audit-copy credential is a separate, put-only `object_store` entry, so backup credentials cannot delete the audit copy.

## 12. Identities, grants, audit and the AI-native loop

### Identities

Humans (`usr_`, roles `owner`, `operator`, `viewer`) and agents (`agt_`: name, model, owner) are separate identities (D-0029). They are separate because the questions are different: for a human, "who"; for an agent, "which agent, on whose behalf, and because of what" (D-0029, D-0031). Role bundles fix which scopes a human has; the full mapping is in [../contracts/README.md](../contracts/README.md) and [../contracts/permission-model.md](../contracts/permission-model.md).

These are kept apart on purpose:

| Thing | Id | Authorizes | Lifetime | Stored where |
| --- | --- | --- | --- | --- |
| A human | `usr_` | What the role bundle allows | Until the user is removed | Replicated |
| A human's API token | `tok_` | What that human's role allows | Until revoked | Replicated (hash only) |
| An agent | `agt_` | Nothing; it is only an identity | Until disabled | Replicated |
| An agent key | `akey_` | Nothing by itself; it can only be exchanged for an access token (D-0030) | Until rotated | Replicated (hash only) |
| An agent access token | `atk_` | Whatever the agent's active grants allow *at request time* | Minutes | Local (hash only) |
| A grant | `grt_` | Scopes on selected resources until TTL or uses run out (D-0030) | TTL, optional max uses | Replicated |

The split is what makes revocation immediate: the access token carries identity, not permission, and permission is read from the grants on every call. A token that is still valid but whose grant is gone authorizes nothing (D-0030). It is also why access tokens and sessions are local and unreplicated (section 6): they are short-lived secrets, and a replica does not need them to answer a read for a caller who authenticated to that replica. The token and key tables that do replicate carry hashes only, never a usable secret.

### Grants

Agents hold an agent key (`akey_`) that authorizes nothing by itself. They exchange it for a short-lived access token (`atk_`), and every request is checked against the agent's active grants at request time, so revoking a grant locks the agent out on its next call rather than when a token happens to expire (D-0030).

A grant (`grt_`) is: scopes + resource selector + TTL + optional max uses (D-0030). Selectors are a set of projects, apps and nodes, matched by "any"; `{"all": true}` matches everything and `{}` matches nothing ([../contracts/README.md](../contracts/README.md)).

Three properties of the model matter architecturally:

- **Incident grants.** When an event wakes an agent, the grant issued with it carries only the scopes the binding names, selects only the affected resource, and expires with the incident or its TTL (D-0030, D-0032). The agent cannot widen it.
- **Destructive scopes need JIT approval.** The agent files an approval request (`apr_`), a human approves in the panel, and a short-lived extra grant is issued (D-0030). Approvals are checked in the service layer, not in the MCP layer, so no channel can bypass them.
- **Human-only scopes are never grantable.** `node:enroll`, `node:remove`, `agent:write`, `grant:write`, `approval:decide`, `webhook:write` and `user:write` cannot appear in an agent's grant, so an agent cannot widen its own power, approve its own request, or create another agent ([../contracts/README.md](../contracts/README.md)).

Refusals are part of the contract, not an afterthought: a refused request carries `forbidden` with a `reason` (`scope_missing`, `selector_mismatch`, `grant_expired`, `grant_revoked`, `grant_exhausted`, `human_only`, `agent_disabled`), and an action that needs approval carries `approval_required` with the scope and target so the agent can file the request itself ([../contracts/README.md](../contracts/README.md)).

### Audit

The audit log is a separate append-only hash chain, with each record appended in the same transaction as the action it records (D-0031). Refusals are recorded too: a denied restart is as interesting as a granted one, and the M3 demo relies on it (D-0033). A record holds who, on behalf of whom, the triggering event, the tool or endpoint, the parameters, the result and the agent's justification (D-0031); the format and its verification are fixed in [../contracts/audit-log-format.md](../contracts/audit-log-format.md). The log can be copied off-site to object storage with object lock, out of reach of whoever operates the server (D-0031).

| Record part | Answers | Source |
| --- | --- | --- |
| Actor | Who acted | The authenticated identity: `usr_`, `agt_`, `nod_` (node) or `system` |
| On behalf of | Whose authority | For an agent, the human who created the grant or the binding (D-0029) |
| Trigger | Why now | The `evt_` or `inc_` that woke the agent, when there is one |
| Channel and operation | What was called | `api`, `panel`, `mcp`, `cli` or `system`, plus endpoint or tool name |
| Parameters | With what | The request parameters, with secrets redacted |
| Result | What happened | Success, or the error code and `forbidden` reason |
| Justification | In the actor's words | `Quoro-Justification`, or the tool's `justification` argument |

The log is a chain, not a table with a timestamp column: each record includes the previous record's hash, so removing or editing an entry breaks every later link. `audit.chain_broken` is the event that says a verification found one (D-0031; [../contracts/README.md](../contracts/README.md)).

The channel of every action is recorded: `api`, `panel`, `mcp`, `cli`, `system` ([../contracts/README.md](../contracts/README.md)). That is what makes "the agent and the human are logged the same way" a property of the design rather than a promise (D-0002).

### MCP

The MCP server is rmcp's Streamable HTTP transport mounted on the same Axum server at `/mcp`, plus a stdio mode through the CLI (F-0041). It calls the same service layer as the HTTP API, with the same scopes and the same audit log; there is no second path (D-0031). Authentication is the same bearer access token as the API, and every mutating tool takes a required `justification` and an optional `incident_id`, the MCP equivalents of the `Quoro-Justification` and `Quoro-Incident` headers ([../contracts/README.md](../contracts/README.md)).

The tool set is the one fixed in [../contracts/README.md](../contracts/README.md): reads for cluster, nodes, projects, apps, deployments, logs, events, incidents, approvals and audit, plus `restart_app`, `redeploy_app`, `delete_app`, `resolve_incident`, `request_approval` and `get_approval`. Each tool maps one-to-one onto an endpoint and needs the same scope.

### The AI-native loop

D-0032 defines a four-step loop — detect, notify, wake, act. Each step has a named mechanism here; the incident sits between detection and notification.

1. **Detect.** Node agents run health checks and emit `event` messages; the control plane also derives detections from state (`node.disconnected`, `certificate.expiring`).
2. **Event.** The control plane records the event, which can be one of the types in [../contracts/README.md](../contracts/README.md) — `app.down` for a health check past its threshold, `vm.crashed` for an unexplained exit, `node.disk_low`, `deployment.failed`.
3. **Incident.** An event that a webhook binding is interested in opens an incident (`inc_`), and the incident scopes the grant the agent will get (D-0030, D-0032).
4. **Wake.** The webhook dispatcher delivers a signed event to the configured endpoint (`Quoro-Signature`, five-minute freshness window, retries with `webhook_deliveries` records). The receiver — an agent runner — starts the agent with Quoro's MCP endpoint and the incident context.
5. **Act.** The agent acts under its incident grant. Anything destructive stops at an approval request and waits for a human (D-0030).

Events are defined in [../contracts/events.schema.json](../contracts/events.schema.json). The loop is the M3 deliverable (D-0033).

**Agent runner.** The wake step needs a runner that receives the webhook delivery, verifies `Quoro-Signature`, reads `WebhookWake.agent_id` and the incident id from the payload, holds that agent's own agent key (`akey_`) configured by the operator at the runner (Quoro never sends credentials in a webhook), mints a 5-minute access token via `POST /api/v1/auth/agent-token`, and acts through Quoro's MCP endpoint within the incident grant, passing `incident_id` and `justification` on every mutating tool. The runner is a reference example in `examples/agent-runner/`, outside the `quoro` binary, owned by the mcp workstream and delivered in M3 (D-0049); the runner contract is the one in this section.

## 13. Realtime transport (D-0057)

D-0057 decides the split, superseding D-0044: SSE for one-way streams and WebSockets for bidirectional sessions:

| Traffic | Transport | Endpoints |
| --- | --- | --- |
| Server-to-client streams: event stream, live app logs, deployment progress | SSE (`text/event-stream`) | `GET /api/v1/events/stream`, `GET /api/v1/apps/{app_id}/logs/stream` |
| Bidirectional interactive sessions: web terminal, VM console (M4, xterm.js) | WebSocket | terminal and console endpoints |
| Node agent session | WebSocket | [../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md), outside D-0057's scope |

The reasons for SSE on the read paths: it is plain HTTP, so it passes reverse proxies and Cloudflare without upgrade handling; it reconnects automatically; and `Last-Event-ID` maps directly onto the `seq` cursor of events and logs, which gives a resume point for free. The reason for WebSockets on the interactive paths is that a terminal needs both directions on one connection.

The claim that Axum supports both natively — an `sse` module and a `ws` feature — is a **Hypothesis**; it was not re-verified in the research stage. The cost of using two mechanisms is one more client-side code path; the benefit is that the common read-only case never needs a WebSocket.

## 14. High availability: prepared for in the MVP

HA and backups are out of the MVP but prepared for (D-0033). "Prepared for" is a specific claim: each item below is a field, message or path that exists in M0 to M4 and is exercised in M5 or M6, not a design that would have to be retrofitted.

| Item | Prepared in M0-M4 | Delivered in |
| --- | --- | --- |
| Change log with `term` (leader epoch) | M0 schema; constant 1 until elections (D-0013, D-0016) | M6 |
| Lease messages and the self-fencing path in the node-agent protocol | M0 protocol (D-0019) | M5b for expiry; the self-fence path ships from M0 |
| Lease modes `local` and `expiring` | M0 protocol (D-0047) | M5b |
| Lease states `held`, `uncertain`, `expired`, `revoked` | M0 vocabulary | M5b |
| Fencing tokens | M0 protocol field | M5b |
| Concurrent fences and a mid-fence crash | — | M5b, mandatory before expiring leases ship (D-0075) |
| HA policy field on apps (`none`, `cold`, `warm`, `hot`) | M0 schema; only `none` implemented (D-0020) | M5a (`cold`), M5b (`warm`), M6 (`hot`) |
| Node resource reports feeding the N+1 indicator | Node agent reports from M2 (D-0021) | M5b |
| Manual promotion | Replica nodes follow the log from M2 (D-0013) | M5b (D-0067) |
| Witness and three votes | — | M5b (D-0017) |
| Edge as third voter | — | M5b (D-0018; a single edge is a traffic single point of failure) |
| Failback sequence | — | M5b (D-0022) |
| Chunk engine | — | M5a cold, M5b warm (D-0023) |

### Lease modes

Two modes (D-0047), because D-0019 (self-fencing from M0) and D-0011 (apps keep running when the control plane is gone) would otherwise conflict during M1-M4, when there is only one control plane:

- **`local`**: issued for HA policy `none` or `cold`, where no other copy can start. It never expires when the arbiter is unreachable; it ends only by an explicit `lease_revoke` (move, delete, planned switchover) or when an operator declares the node lost, which requires a manual fence acknowledgement before a cold backup is restored elsewhere. That keeps D-0011 and the M2 demo true: stopping the control plane does not stop apps.
- **`expiring`**: issued for `warm` and `hot` from M5b. Duration-based, measured on the node's monotonic clock from when the renewal was sent, minus a safety margin. The state path is `held` -> `uncertain` -> `expired` -> self-fence, and a lease is also revoked explicitly.

Fencing tokens are carried on every write to shared storage (D-0047), not only in the lease messages.

The distinction between two lease states is the core safety rule:

- **`uncertain`** means "the authority cannot be reached": the arbiter, or in M5b a primary that is unreachable or has lost the support of a majority of voters. The node keeps running what it runs and starts nothing new. It has not lost the lease; it cannot prove it still holds it.
- **`expired`** means "lease lost": the node self-fences, stopping its stateful VMs (D-0019).

Conflating the two produces one of two failures: fencing on every network blip, or continuing to run a stateful app that another node has already been allowed to start. Treating them as separate states is what lets a partition be survivable on one side and safe on the other.

For an `expiring` lease the deadline is `valid_until = t_send + D - M`, where `t_send` is the node's monotonic time when it sent the renewal the current grant answers, `D` is the grant's `duration_ms` and `M = max(1000 ms, D / 10)` ([../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md) section 8.4).

| From | Event | To | What the node does |
| --- | --- | --- | --- |
| (none) | `lease_grant` | `held` | Starts the stateful app |
| `held` | renewal failing, authority unreachable | `uncertain` | Keeps running, starts nothing new |
| `held` | `lease_revoke` | `revoked` | Stops the stateful app |
| `held` | monotonic clock reaches `valid_until` | `expired` | Self-fences: stops the stateful app |
| `uncertain` | renewal succeeds | `held` | Resumes normal operation |
| `uncertain` | monotonic clock reaches `valid_until` | `expired` | Self-fences: stops the stateful app |
| `expired` | new `lease_grant` | `held` | Restarts only under the new grant's token |

Two properties matter in the table. Recovery from `uncertain` back to `held` is silent, because nothing was given up; recovery from `expired` is not, because the app was stopped and the fencing token moved on. And no transition ever goes directly from `uncertain` to starting something, which is the whole point of the state.

The authority never grants the lease to another holder before `D + 2M` after its last grant, so the old holder's fencing completes first ([../contracts/node-agent-protocol.md](../contracts/node-agent-protocol.md) section 8.4).

In M5b the authority is the primary, backed by its voters (ADR 0169, ADR 0170): every machine with the `node` or `witness` role gives the primary a support window over its session, and the primary grants and renews `expiring` leases only while a majority supports it. Voters never see leases. What they prevent is a second primary granting at the same time: a promotion takes term `t + 1` from a majority, a voter agrees only after its support for the old primary ran out, and the candidate waits until the old primary's last possible grant has passed its gate. Votes travel directly between voters over the mesh, which gains voter-to-voter peers in M5b (node-agent-protocol.md sections 8.8 to 8.10).

Fencing tokens increase on every ownership change and stay the same across renewals (D-0073); they travel on every write to shared storage — chunk store, object store, replication stream — and receivers reject stale tokens (D-0019). A token is what makes "the old holder is definitely not writing" enforceable by the storage layer rather than by good behaviour.

### Lease arbiter options

| Option | Evidence | Consequence |
| --- | --- | --- |
| Quoro's own voters, with a witness as the third vote | A witness is a free daemon in a different failure domain that breaks ties and stores no data (D-0017); the voters support the primary for short windows and vote at promotion (ADR 0169) | No external dependency; needs a third machine or a managed plan. The M5b choice |
| R2 conditional PUT | Atomic `If-Match` / `If-None-Match` PUT with a single winner, but no TTL, one write per second per key, no conditional delete, and a 404 does not prove an in-flight PUT failed (`docs/research/r2-conditional-writes.md`, F-0037, F-0038) | Single winner is real; TTL must be emulated by timestamps, so fencing tokens are still required |
| Cloudflare Durable Objects | A strongly consistent single-threaded object with alarms for a server-side TTL; fits the free plan at a 5 s renewal per F-0039; acquire can be ambiguous when an object restarts; needs a Worker in the user's account (`docs/research/durable-objects.md`) | Closest to a real lease; adds a Worker and a provider account to the critical path |

Both external arbiters still require fencing tokens (D-0019), and conditional-write support must be probed per provider at setup (F-0038). Neither is required for the self-hosted path: the witness is free and first-class (D-0017), and the external arbiters are what a managed offering can add.

### Failback

Failback is never automatic and never immediate (D-0022). The six steps of the planned sequence:

1. The returning node rejoins fenced, checks its leases and starts nothing stateful.
2. It joins as a replica of the current lease holder, which is the source of truth.
3. Its divergent writes are kept in a quarantined snapshot and recorded in the audit log, never silently dropped.
4. Incremental resync runs from the last common point, and the node becomes the `warm` copy.
5. By default there is no failback: the roles simply swap.
6. If the user prefers the original node, a planned switchover waits for replication lag near zero and a low-traffic window, freezes writes for seconds, does a final sync, moves the lease, shifts traffic at the load balancer and drains connections.

A planned switchover loses no data. Only a surprise failure can lose seconds, and the amount is bounded by the replication interval (D-0020, D-0022). Stateless apps need none of this: the load balancer slow-starts traffic back to the returning node (D-0022).

## 15. Key flows

Each flow is written as the sequence of calls, records and messages it produces, using the exact names from [../contracts/README.md](../contracts/README.md). "Change-log entry" means one numbered, hash-chained entry appended in the same transaction as the state table update (D-0013, D-0015); "audit record" means one entry in the audit chain, appended in the same transaction as the action (D-0031).

Four rules hold in every flow:

- **Authenticate, then authorize, then act.** The scope check happens in the service layer, against the grants as they are at that instant (D-0030). No channel is exempt.
- **One transaction, one record.** The state change, its change-log entry and its audit record commit together, so a crash cannot leave an action without a record or a record without an action (D-0013, D-0031).
- **Idempotent by key.** Every creating `POST` carries an `Idempotency-Key`; retries return the first result instead of creating a second object ([../contracts/README.md](../contracts/README.md)).
- **Correlatable end to end.** Every request has a `req_` id, echoed as `Quoro-Request-Id`; event ids `evt_` and incident ids `inc_` carry the thread from detection to action (D-0029, D-0031).

### (a) Deploy (M2)

1. The caller sends `POST /api/v1/apps/{app_id}/deployments` with scope `app:deploy`, an `Idempotency-Key`, and — if it is an agent — `Quoro-Justification`. The channel is `api`, `panel` or `mcp` depending on the caller, and is recorded.
2. Authorization checks the caller's identity, the scope against the app's project through the grant's selector, and whether a destructive scope needs approval (it does not; `app:deploy` is not destructive). A refusal is audited with its `forbidden` reason.
3. Change-log entries: the new `deployments` row with status `pending`, and the app's current deployment pointer. Audit record: who, scope, parameters, result.
4. Event `deployment.started`.
5. The scheduler chooses a node and builds that node's desired state at rev N+1, which now contains the new instance, its image reference and digest, and its domain.
6. The primary sends `desired_state` with rev N+1 on the node's session.
7. The node persists rev N+1 before applying it (D-0011), then prepares the image: pull, verify the digest, build the rootfs, cache by digest so a redeploy of the same image is free.
8. The node creates and starts the microVM. The deployment status moves `preparing`, then `starting`; the VM's observed status is a separate enum and follows its own transitions. A stateful app starts only while its lease is `held` (D-0019); otherwise the deployment stops here.
9. The health check passes, the node sends `status_report` and `event` `vm.started`.
10. The primary records `deployment.succeeded`, applies the route to the route table, and obtains a certificate through ACME or registers the tunnel route (D-0024).
11. The previous deployment becomes `superseded` and its microVM is removed. For a stateless app the new instance starts before the old one stops; for a stateful app the old one stops first, because there is a single writer (D-0019).
12. Any failure in steps 7 to 10 ends as `deployment.failed` with the failing phase, which is one of `pull`, `rootfs`, `boot` or `health` (the deployment `phase` field, not a status), and the node keeps the previous instance running where it still can.

### (b) Restart by an agent (the M3 demo)

1. A node health check fails past its threshold, and the node sends `event` `app.down`.
2. The primary records the event and opens an incident `inc_`, emitting `incident.opened` (D-0032).
3. The webhook binding that matches the event delivers a signed event to its endpoint: headers `Quoro-Event-Id`, `Quoro-Event-Type`, `Quoro-Delivery-Id` and `Quoro-Signature`; a receiver rejects a signature older than five minutes.
4. An incident grant is issued and `grant.created` is emitted. Its scopes come from the binding, its selector is the affected app, and its TTL comes from the binding (D-0030).
5. The agent runner starts the agent with the incident context and Quoro's MCP endpoint at `/mcp`.
6. The agent exchanges its agent key (`akey_`) for a short-lived access token (`atk_`). The key alone authorizes nothing (D-0030).
7. The agent reads state and logs through MCP: `get_app`, `get_app_logs` (scope `app:logs`), `list_incidents` and `get_incident` (scope `incident:read`).
8. The agent calls `restart_app` with a justification and the incident id (scope `app:restart`, endpoint `POST /api/v1/apps/{app_id}/restart`).
9. The service layer checks the grant: scope, selector, TTL, uses remaining, and `agent_disabled`. A refusal would be recorded with its reason (`scope_missing`, `selector_mismatch`, `grant_expired`, `grant_revoked`, `grant_exhausted`).
10. Change-log entry: the app's `restart_generation` is bumped. Audit record: channel `mcp`, the agent, the human who configured the binding as "on behalf of", the triggering event, the tool, the parameters, the result and the justification (D-0029, D-0031).
11. Event `app.restarted`.
12. The primary sends the new `desired_state`; the node restarts the microVM and reports `status_report` when it is running again.
13. Event `app.up` when the health check passes again.
14. The incident is resolved by the agent (`resolve_incident`, scope `incident:resolve`) or by a human; `incident.resolved` is emitted, and the incident grant ends with the incident or when its TTL passes.

### (c) Node loss

1. The primary notices the session drop or missed heartbeats and emits `node.disconnected`. The node's status becomes `disconnected`.
2. An incident can be opened and an agent woken, exactly as in (b), with scopes limited to the affected node and apps.
3. The node itself keeps running and reconciling its last desired state; local leases stay `held`, because a `local` lease does not expire while the arbiter is unreachable (D-0047). It redials with backoff and jitter.
4. Nothing is started elsewhere automatically before M6 (D-0016, D-0033). The panel shows the app as degraded and the N+1 indicator answers whether its apps would fit on the remaining nodes (D-0021).
5. To restore a stateful app elsewhere from a `cold` backup (M5a), an operator must first declare the node lost, as part of the `local` lease mode (D-0047), and acknowledge the manual fence. That revokes its leases and increments the fencing token, so a node that comes back cannot keep writing (D-0019).
6. From M5b, with `expiring` leases: the node's renewals start failing, it goes `uncertain` (keep running, start nothing new), then `expired`, self-fences its stateful apps, and buffers `node.fenced` for replay on reconnect; if a forced stop cannot confirm a VM stopped, it emits `node.fence_failed` instead, marks itself failed, and the lease is not granted elsewhere until an operator acknowledges the fence (D-0074).
7. A `warm` copy may start only after the old lease has passed its gate on the primary, `T_grant + D + 2M` after its last grant (D-0019). That ordering, not the detection time, is what prevents two writers.

### (d) Node return

1. The node redials and sends `hello` carrying its applied desired-state rev, its change-log position and the leases it believes it holds.
2. The primary sends `lease_revoke` for any lease that is no longer the node's, before anything else.
3. The node stops those stateful VMs first, before applying anything else (D-0022).
4. Divergent writes, if any, are kept as a quarantined snapshot and recorded in the audit log (D-0022; M5b). Nothing is discarded silently.
5. The primary sends the full `desired_state`; the node reconciles, and no duplicate app appears — this is the M2 demonstration that reconciliation is idempotent (D-0011).
6. Change-log follow resumes from the node's last verified `seq` (D-0013).
7. There is no automatic failback: a planned switchover is a separate, explicit action (D-0022).

## 16. Stack confirmation (D-0056, superseding D-0028)

D-0056 fixes the baseline stack, keeping the same items D-0028 listed and replacing the graphical console with text-only consoles. This section confirms it item by item, with the refinements the research forced; D-0057 adds the realtime split.

| Item | Baseline stack | Status here | Notes |
| --- | --- | --- | --- |
| Axum + Tokio | Control plane and API in Rust on Axum + Tokio | Confirmed | One server hosts the API, the MCP endpoint and the embedded panel |
| sqlx + SQLite | sqlx with SQLite | Confirmed (D-0015) | One writer, no database server; the change log and derived tables are the schema |
| Node agent | Rust static binary | Confirmed | It is part of the single binary per D-0012, so it is a role rather than a separate artifact |
| MCP | rmcp, the official Rust SDK | Confirmed (D-0048) | rmcp 3.8.0, three semver-major releases between March and July 2026 (F-0041): pinned to a minor version and majors are budgeted |
| API contract | OpenAPI via utoipa -> TS client (orval or openapi-ts) | Confirmed (D-0048) | The hand-written `docs/contracts/openapi.yaml` is frozen; CI diffs utoipa's output against it; every response needs a non-empty description because of F-0042; orval generates the panel client |
| Panel | React + TanStack Router/Query/Table, shadcn/ui + Tailwind, rust-embed | Confirmed (D-0048) | The panel is built against a client generated from the frozen spec by orval |
| Consoles | xterm.js; no noVNC (D-0056) | Confirmed (D-0056) | There is no VNC (F-0034): Cloud Hypervisor has no display device, so consoles are text-only, xterm.js over serial or virtio-console |
| Metrics | uPlot | Confirmed | Fed by on-demand node telemetry (D-0014) |
| Realtime | SSE for one-way streams, WebSockets for bidirectional sessions (D-0057) | Confirmed (D-0057) | Section 13: SSE for the event stream, live logs and deployment progress; WebSockets for terminal and VM console |
| Builds | Dockerfile only, run by BuildKit inside an ephemeral builder microVM, output to a node-local OCI layout (D-0060) | Confirmed (D-0060) | The OCI layout feeds the normal OCI-to-rootfs path; that BuildKit runs under Cloud Hypervisor is a **Hypothesis** (section 19); Railpack/Nixpacks are re-verified before M4, and a zero-config builder may be added later as a second `Builder` implementation |
| Docker client | bollard | Not needed | Confirmed: there is no Docker on nodes (D-0008) |
| Backup | Quoro's own chunk engine with client-side encryption (D-0069) | Confirmed (D-0069) | Litestream 0.5 has no client-side encryption (F-0040), so SQLite snapshots come from the SQLite backup API or `VACUUM INTO` and are chunked and encrypted locally; a Litestream backend for a lower RPO may be added later as an option (D-0069) |

Two of the "confirmed" rows carry a caveat about the dependency rather than about the choice. rmcp's release cadence means the MCP layer must be isolated behind the service-layer boundary so a minor-version upgrade is a contained change (D-0048). utoipa's empty-description behaviour (F-0042) means the API contract has a lint rule, not just a convention: every response must carry a non-empty description. Every dependency in the list must still be justified individually (D-0056).

### Dependency justification

D-0056 requires that every dependency be justified, so each one above carries a reason rather than a precedent:

| Dependency | Why it is there | What would remove it |
| --- | --- | --- |
| Axum + Tokio | The API, MCP and panel all need one async HTTP server; Tokio is its runtime | Nothing realistic in Rust |
| sqlx + SQLite | Compile-time-checked queries against the one database the design has (D-0015) | A different database, which D-0015 rules out for the MVP |
| rmcp | The official MCP SDK (F-0041); writing the protocol by hand would be more surface, not less | A smaller, stable SDK |
| utoipa + orval | Type-safe client generation from the frozen spec; hand-written clients drift | A generator with no empty-description bug (F-0042) |
| React + TanStack + shadcn/ui + Tailwind | The panel; the TanStack pieces are the data and routing layer | Nothing planned |
| rust-embed | One binary, no asset directory to lose (D-0012) | Nothing planned |
| xterm.js | Text consoles over the serial or virtio-console (F-0034) | Nothing planned |
| uPlot | Metrics rendering without a large charting stack | Nothing planned |
| hyper + rustls + an ACME client | The embedded reverse proxy that serves ingress on hosts with a public IP (D-0059) | The M2 spike showing that owning ACME is too costly, in which case Caddy's admin API becomes the backend |
| BuildKit, in an ephemeral builder microVM | Dockerfile-only builds in M4 (D-0060) | A different builder backend behind the same `Builder` implementation |

Not on the list: bollard, because there is no Docker on nodes (D-0008); noVNC, because consoles are text-only (D-0056, F-0034); a container runtime, because a container Engine is a later implementation (D-0009).

## 17. Workspace layout

This is the planned M0 cargo workspace layout. It follows the rule that each workstream owns its paths (D-0036), one crate or package per workstream, so that parallel work rarely touches the same file.

| Path | Owner workstream |
| --- | --- |
| `crates/quoro/` | control-plane — the binary: CLI, role wiring, startup |
| `crates/types/` | control-plane — ids, scopes, event types and shared domain types generated from the contracts |
| `crates/control-plane/` | control-plane — API, services, auth, grants, audit, store, scheduler, webhooks |
| `crates/proto/` | node-agent — node-agent protocol messages |
| `crates/node-agent/` | node-agent — local store, reconciler, session, leases |
| `crates/engine/` | engine — `Engine` trait, Cloud Hypervisor implementation, fake engine, networking, OCI-to-rootfs |
| `crates/init/` | engine — the guest minimal init |
| `crates/ingress/` | ingress — the `Ingress` interface and the proxy backend |
| `crates/providers/` | ingress — DNS, load balancer, object store, Cloudflare |
| `crates/mcp/` | mcp — rmcp tools over the service layer |
| `panel/` | panel — React application and its generated client |
| `installer/`, `packaging/`, `.github/workflows/`, `xtask/` | installer — install, release, CI, and contract validation |

Two notes on the table. `docs/contracts/` is not owned by a workstream after M0: it is frozen, and a change there is a standalone pull request that touches only the contract (D-0036). And `crates/proto/` is separate from `crates/node-agent/` so that the control plane can depend on the message types without depending on the agent implementation, which is what keeps the protocol contract reviewable on its own.

### Build and checks

The workspace exists to make the contracts checkable in CI, not only readable:

- `xtask` validates the contracts: the OpenAPI document parses, every response has a description, the event schema validates its examples, the SQL schema runs in a fresh in-memory SQLite database ([../contracts/README.md](../contracts/README.md)).
- `xtask` also generates the utoipa document and diffs it against the frozen `openapi.yaml`, failing on any semantic difference (D-0048).
- The panel builds against a client that orval generates from the frozen spec, so a panel change that needs a new field needs a contract change first (D-0048; D-0036 for the contract-change rule).
- Releases are signed and reproducible, because the installer is `curl | sh` (D-0027).

The dependency direction is one-way: `crates/quoro` depends on every role crate, `crates/control-plane` and `crates/node-agent` both depend on `crates/types` and `crates/proto`, and `crates/engine` depends on neither. That is what allows the engine and the control plane to be developed and tested in parallel, which is the reason parallel workstreams can run at the same time at all (D-0036).

### Release and verification

D-0027 sets the requirements, and this document restates them so the installer workstream has one list:

- Every release binary is signed.
- The `curl | sh` installer verifies the signature against a published key before it runs anything.
- Builds are reproducible, including the panel's JavaScript bundle embedded with rust-embed.
- Provenance is published.

The scheme is not chosen — for example minisign, or Sigstore plus SLSA provenance — and the installer workstream proposes it before M1, pending maintainer decision.

## 18. Open questions and recommendations

Every open item this document depends on. None of them is decided here.

No question is open any more.

Decided since the first draft, and therefore no longer listed here:

- Ingress proxy: D-0059.
- Git builds: D-0060.
- Image path, rootfs conversion and minimal init: D-0061.
- Disk snapshot primitive: D-0062, superseded by ADR 0146 and ADR 0156; changed ranges for warm volumes: ADR 0172.
- Custody of the cluster keys on promotion: D-0078, read by ADR 0180 and ADR 0181.
- Node mesh: D-0063.
- Release milestones: D-0064.
- Manual promotion: D-0067.
- SQLite and catalog-template backups: D-0069 (answers Q-0026; a Litestream backend with SSE-C for a lower RPO may be added later as an option).
- Curated catalog: format, owner, location and signature scheme are decided at M4 planning, before the first template ships: D-0070 (answers Q-0060).
- HA policy costs for `none` and `cold`: D-0068 (answers Q-0025).
- Docs-site timing: D-0065 (answers Q-0009).
- Prerequisites for shipping expiring leases in M5b: D-0075.
- Realtime transport: D-0057 (supersedes D-0044).
- Graphical console dropped from the baseline stack: D-0056 (supersedes D-0028).
- Lease modes `local` and `expiring`: D-0047.
- API source of truth, generator and pinning: D-0048.
- Agent runner: D-0049.
- Cluster key algorithms, storage and rotation: D-0050.
- Provider credentials and the `providers` resource: D-0051.
- Node-local volumes in M5a: D-0052.
- Off-site audit copy in M5a: D-0053.
- Host layout, CLI and setup token: D-0054.
- Lab TLS for the M2 demonstration: D-0055.

Custody of the cluster keys on promotion was the last open row. Q-0024 fixed when promotion ships (M5b, D-0067); D-0078 chose to replicate the keys to replica nodes, and ADR 0180 reads that as seals to a custody key of each node, because the node-local key is never seen by the primary. Until promotion exists the keys do not leave the primary.

Q-0001 to Q-0006, Q-0009, Q-0024, Q-0025, Q-0026 and Q-0060 are decided (D-0059 to D-0065, D-0067 to D-0070) and are therefore absent from the table.

## 19. Hypotheses to verify

Every claim marked **Hypothesis** in this document, and what would settle it.

- **Axum supports SSE and WebSockets natively.** Used in section 13. Verify by building one `text/event-stream` route and one WebSocket route in the current Axum version and running them in CI; settle before the M3 event stream ships.
- **An object store can support object lock on every provider we target.** Used in section 11 for the off-site audit copy. Verify per provider with a real bucket: create a locked object, try to overwrite and delete it, and record the result.
- **The available Rust ACME and TLS crates are mature enough to own the proxy path.** Used in section 10 and D-0059. Verify with a spike before M2: obtain and renew one certificate for a real domain through the embedded proxy.
- **An encrypting S3 proxy or SSE-C is operationally acceptable as a backup path.** Used in section 6, options 1 and 4, and D-0069's optional Litestream backend. Verify by restoring a primary snapshot from the bucket in a drill, not by reading documentation.
- **BuildKit inside a builder microVM works under Cloud Hypervisor.** Used in section 16 and D-0060. Verify with an M4 spike that runs one Dockerfile build end to end and produces an OCI layout.
- **Railpack/Nixpacks are viable zero-config builders, with Nixpacks in maintenance and Railpack as its successor.** Used in D-0060, which re-verifies them before M4. Verify before M4 by building one real repository with each.
- **Running Cloud Hypervisor as a separate process keeps the AGPL-3.0 and Apache-2.0/BSD-3-Clause licenses cleanly separated.** Used in section 2. This is a legal reading, not a reviewed conclusion. Verify with a license review before the first public release, which is the M3 preview (D-0064): a lawyer confirms that process separation is enough, or the distribution model changes.
- **A crash-consistent block copy of a running database is not sufficient, so native replication is needed.** Used in section 14 and D-0023's exception for databases. Verify by restoring a chunk-engine copy of a running database and measuring what it contains.
- **A `local` lease that never expires does not let two writers exist.** Used in section 14 and D-0047. Verify with the M5b partition test: with the control plane unreachable, confirm that no other node can be told to start the stateful app.
- **The witness can be small enough to run on any cheap machine in a different failure domain.** Used in section 3 and D-0017, which puts the witness in a different failure domain from the nodes. Verify in M5b by running it on a third machine and cutting each link in turn.
- **Decision to verify, D-0018: a single edge is a traffic single point of failure.** With the edge also a voter, losing it removes both the traffic path and one vote. D-0018's mitigations are Cloudflare as the traffic edge with the small VPS acting only as witness, or two edges with DNS failover. Verify in M5b that the chosen mitigation keeps both traffic and quorum when the edge dies.
- **One tunnel per node with an HTTPS monitor on the app hostname gives per-node failover.** Used in section 10. Verify in M5a by powering off a node behind its own tunnel and confirming the load balancer removes that endpoint and keeps serving from the other.
- **The load-balancer health check interval is short enough for the RTO the `warm` policy promises.** Used in section 14. Verify by measuring detection-to-recovery time in M5b.
- **`mkfs.ext4 -d` plus a Rust OCI registry client is a workable rootfs pipeline.** Used in section 8 and D-0061. Verify in the M0 spike by booting a microVM from a converted image.
- **virtio-fs is only an optimisation and never a requirement.** Used in section 8. Verify by confirming the M2 deploy path boots from a disk image with no virtiofsd present.
