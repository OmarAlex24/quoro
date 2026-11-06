# Node-agent protocol

Status: M0 draft for review. Contracts freeze at the end of M0 (D-0036); after the freeze a change to this file is a standalone contract pull request, never part of feature work.

This file fixes the wire protocol between a node agent and the control plane: enrollment, the dial-out session, the message set, desired-state reconciliation, change-log follow, and leases with self-fencing. It is the contract named in `README.md` for the node-agent, control-plane and engine workstreams.

Names used here — roles, node status, VM observed status, lease modes and node-side lease states, scopes, event types, message types — come from `README.md` and are used exactly as defined there. Field names come from the shared entity model used by `openapi.yaml`, `db-schema.sql` and `events.schema.json`. Terminology follows `../glossary.md`; in particular a **replica node** is not an **app replica**, and a **lease holder** is the only node that may run a stateful app (D-0019).

## 1. Purpose and principles

### 1.1 What this contract fixes

- The enrollment flow and the credential a node receives.
- The transport, the envelope and the message set in both directions.
- How desired state is delivered, verified, persisted and applied.
- How a node follows the change log (D-0013) and what it does when its copy diverges.
- The lease lifecycle and the self-fence procedure that D-0019 requires from M0.
- The order of operations when a node returns after a failure (D-0022).
- What the node stores locally, so the data plane can run without the control plane (D-0011).

It does not fix: the HTTP API around it (`openapi.yaml`), the event payloads (`events.schema.json`), how the engine drives Cloud Hypervisor (`engine-trait.md`), or the panel's realtime transport.

### 1.2 Principles

| Principle | Consequence in this protocol | Source |
| --- | --- | --- |
| Nodes dial out, never are dialed into | The node opens one outbound WebSocket to the primary; the primary never connects to the node. Works behind NAT and lets the control plane live anywhere. | D-0011 |
| The node is autonomous | The node stores signed desired state locally and reconciles it without the control plane. Losing the session blocks changes, not workloads. | D-0011 |
| Self-fencing exists from M0 | The lease messages, the node-side lease state machine and the self-fence path ship before any HA feature is used, so the protocol is not redesigned later. | D-0019 |
| One writer | Only the primary issues desired state, leases and commands. A replica node forwards to the primary; it never speaks this protocol as an authority. | D-0012 |
| State is replicated, telemetry is not | Logs, console bytes and metrics travel on demand over this session and are not stored by the primary. | D-0014 |
| Every change is numbered | Desired state carries a monotonic `rev`; the change log carries a gap-free `seq` with a hash chain. | D-0011, D-0013 |

## 2. Enrollment

Enrollment is a one-time exchange over the HTTP API. It happens before any session exists and uses the API's `application/problem+json` error codes on failure (`README.md`).

### 2.1 Join token

A human with the `node:enroll` scope creates a join token:

```
POST /api/v1/nodes/join-tokens
```

The token is a one-time secret with a default lifetime of 1 hour; the API returns the secret once and stores only its hash. `node:enroll` is human-only, so an agent can never enroll a machine (`README.md`).

### 2.2 Join command

The panel and the CLI render a join command (the API returns it as `join_command`) that carries exactly three values:

| Value | Purpose |
| --- | --- |
| primary URL | Where to open the session, for example `wss://quoro.example.com` |
| join token | The one-time secret from `POST /api/v1/nodes/join-tokens` |
| certificate fingerprint | SHA-256 of the primary's TLS certificate (DER), so a self-signed certificate is safe to accept |

The fingerprint is a pin, not a trust-on-first-use decision: the node refuses any certificate whose digest differs, and a certificate rotation requires a new join command.

### 2.3 Enroll request

The node generates an Ed25519 keypair, keeps the private key locally and sends the public key:

```
POST /api/v1/nodes/enroll
```

The call is `x-quoro-scope: public`: the node has no credential yet and the join token is the proof. A token can be consumed once; a used or expired token is `409` or `422` in the API's error format (`openapi.yaml`).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `join_token` | string | yes | The join token secret |
| `name` | string | yes | Operator-visible node name, unique in the cluster |
| `public_key` | string | yes | Base64 Ed25519 public key of the node |
| `roles` | array of string | yes | Any of `node`, `witness`, `edge` (`README.md`) |
| `capacity` | object | yes | `cpus`, `memory_mb`, `disk_gb` |
| `kvm` | object | yes | `available` (bool), `kernel` (string), `nested` (bool, optional) |

**Edge-only machines (M5a, ADR 0143).** A machine that enrolls with `roles: ["edge"]` runs no engine and starts no VM, so it needs no KVM: it reports the probe as it finds it (usually `available: false`), and the primary never places a VM on it. A machine with the `node` role still needs KVM to run VMs; `node` and `edge` may combine on one machine.

The `kvm` object is filled from a probe at enrollment: `/dev/kvm` exists and is writable, a `vmx`/`svm` flag is present in the CPU flags, and the kernel release is recorded. The probe procedure follows `docs/research/vps-nested-virt.md`; presence of `/dev/kvm` alone does not prove that a guest can start, so the first real boot is the confirmation and a failed boot is reported as a deployment failure in the `boot` phase.

### 2.4 Enroll response

| Field | Type | Meaning |
| --- | --- | --- |
| `node_id` | string (`nod_`) | Identity of this node |
| `cluster_id` | string (`cls_`) | The cluster it joined |
| `primary_url` | string | Base URL the node dials for this and later sessions |
| `credential` | string | Node session credential, returned once |
| `cluster_public_key` | string | Base64 Ed25519 public key that signs desired state |

The credential is 32 random bytes, base64-encoded, and stored root-only on the node. The primary stores only its SHA-256 hash, in the local table `node_credentials` (`db-schema.sql` migration 0002), so a database read does not yield a usable credential and the hash never travels in the change log.

Desired state and lease messages are signed with the cluster signing key (Ed25519), whose public key the node pins at enrollment; that key is separate from the cluster data-encryption key that protects data at rest (D-0050). The signing key is Ed25519; data at rest uses a 256-bit cluster data-encryption key with XChaCha20-Poly1305, plus a node-local key that encrypts this node's copy of desired-state secrets. All keys are files mode 0600 in `/var/lib/quoro` on the primary, outside the SQLite file, generated at first start and backed up separately from the database. Custody of the keys when a replica is promoted is decided (ADR 0078, read by ADR 0180): every machine with the `node` role will hold the signing key, the data-encryption key and the primary's TLS key sealed to a custody key of its own, never to its node-local key, which the primary never sees. The fields that carry the seals are specified with manual promotion in M5b; until then the keys do not leave the primary.

### 2.5 After enrollment

The primary records the node with status `pending` and emits `node.enrolled`. The node writes the credential, the cluster public key and its `node_id` to its local store, then opens a session. `pending` becomes `connected` when the first session starts; the node never re-enrolls on its own — a lost credential is recovered by removing and re-enrolling the node by a human.

## 3. Transport

### 3.1 Connection

```
GET /api/v1/nodes/session
Authorization: Bearer <node credential>
```

- WebSocket over TLS, upgraded from the same API listener. The node is the client; the primary is the server.
- The node verifies the TLS certificate against the pinned fingerprint from the join command.
- One session per node. A new session for the same `node_id` closes the older one, so a stale connection can never deliver messages alongside a fresh one.
- The operation is annotated `x-quoro-scope: node` in `openapi.yaml` (`README.md`): a node credential authorizes the session and nothing else.

### 3.2 Frames

- Text frames only, one JSON message per frame.
- Maximum message size 1 MiB. A larger frame is a protocol violation: the receiver answers `error` with code `bad_message` and `fatal: false`, and ignores the frame.
- Binary frames are not used in v1.

### 3.3 Envelope

```json
{
  "v": 1,
  "type": "heartbeat",
  "id": "msg_01JC9Z7A4M8Q2V6B3N5T9R1KXW",
  "re": "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
  "body": { "time": "2026-11-02T10:15:32Z" }
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `v` | integer | yes | Envelope major version; `1` in v1 |
| `type` | string | yes | One of the message types in `README.md` |
| `id` | string (`msg_`) | yes | Identity of this message |
| `re` | string (`msg_`) | no | `id` of the message this one answers |
| `body` | object | yes | Message-specific body; may be `{}` |

`msg_` ids are message-local. They exist to pair a request with its answer inside one session, they are never persisted, never appear in the API, the database, the change log, the audit log or an event, and they are not part of the identifier table in `README.md`. A message id may be reused after a reconnect.

### 3.4 Versioning, unknown fields and unknown types

- Unknown fields inside a known body are ignored. This allows additive changes without a new version.
- An unknown `type` is answered with `error`, code `bad_message`, `fatal: false`; the session continues.
- A `v` whose major version the receiver does not implement is answered with `error`, code `unsupported_version`, `fatal: true`; the session ends.
- Minor changes (new optional fields, new message bodies of the same type) do not change `v`. A new required field or a changed meaning is a major change and a standalone contract pull request (D-0036).

### 3.5 Relation to the panel's realtime transport

This WebSocket is the node protocol and is independent of how the panel receives live data. The panel's transport is decided (D-0057): SSE for one-way streams and WebSockets for interactive sessions. The node session is a WebSocket regardless, so nothing in this file depends on that choice.

## 4. Session lifecycle

### 4.1 Sequence

```
node agent                                          primary
    |                                                  |
    |-- TLS + GET /api/v1/nodes/session (credential) -->|
    |-- hello ---------------------------------------->|
    |<---------------------------------------- welcome  |
    |<----------------------------------- lease_revoke  |  0..n stale or superseded leases
    |-- command_result ------------------------------>|
    |<----------------------------- command fence_ack  |  0..1 when an acknowledgement is pending
    |-- command_result ------------------------------>|
    |<---------------------------------- desired_state |
    |-- command_result (stored) --------------------->|
    |<----------------------------------- log_entries  |
    |-- log_ack ------------------------------------->|
    |== reconcile: network, images, stop, start, routes ==|
    |-- status_report (applied_rev) ------------------>|
    |-- heartbeat every 5 s -------------------------->|
    |-- status_report on change and every 30 s ------->|
    |-- event (buffered while disconnected) ---------->|
    |<--------------------------------------- command  |  on demand
    |-- command_result ------------------------------>|
    |<------------------------------------- stream_open |
    |-- stream_data ... stream_end ------------------>|
    |<------------------------------------ lease_grant |
    |-- lease_renew every duration/3 (expiring only) ->|
    |                                                  |
```

### 4.2 Rules of the order

1. `hello` is the first message the node sends on every session.
2. `welcome` is the first message the primary sends.
3. The primary sends any `lease_revoke` before anything else. The node stops the stateful VMs bound to those leases and answers `command_result` for each revoke before it processes the next message.
4. If an operator acknowledged a failed fence of this node and the node has not yet confirmed it, the primary then sends `command` kind `fence_ack` (sections 5.17 and 8.5) and waits for its `command_result`.
5. Only then does the primary send `desired_state`. The node verifies, persists, answers `command_result` with `ok`, and reconciles.
6. `log_entries` follow the desired state; the node acknowledges with `log_ack` as it verifies the chain.
7. Steady state begins: `heartbeat` every 5 s, `status_report` on any observed change and at least every 30 s, `event` whenever something happens, streams on demand.

### 4.3 Timers

| Timer | Value | Owner | Source |
| --- | --- | --- | --- |
| Heartbeat interval | 5 000 ms | node sends, both expect | this file |
| Session timeout | 15 000 ms without any frame | primary | this file |
| Status report interval | 30 000 ms | node | this file |
| Redial backoff | 1 s doubling to 60 s, full jitter | node | this file |
| Event buffer | 10 000 entries | node | D-0011, this file |
| Concurrent streams | 32 per session | primary enforces | this file |
| Stream window | 256 KiB of unacknowledged `data` bytes per stream | node enforces, primary acknowledges | ADR 0116, section 5.25 |
| Maximum message | 1 MiB | both | this file |

The values above are carried in `welcome` (`heartbeat_interval_ms`, `session_timeout_ms`), so the primary can change them without a protocol change; the node uses the values from the current `welcome` and falls back to the defaults above when a session has not yet received one.

### 4.4 Disconnect and redial

The primary marks the node `disconnected` after 15 s without any frame from it and emits `node.disconnected` with `data.reason` either `heartbeat_timeout` (no frame within the timeout) or `session_closed` (the socket closed or the server ended the session). A node that reconnects is `connected` again and the primary emits `node.connected`.

The node redials with exponential backoff from 1 s doubling to 60 s with full jitter, and resets the backoff after a session that stayed up for at least 60 s. One redial attempt dials `primary_url` and, when that fails, each URL the primary advertised in `welcome.primary_urls` in order (section 5.10); the backoff applies between attempts, not between the URLs of one attempt. A `fatal` error stops redialing until an operator acts, except for `unsupported_version`, where the node keeps dialing (an upgraded primary may accept it) but logs the mismatch locally.

## 5. Message reference

All bodies are objects. Field names are snake_case. Timestamps are RFC 3339 UTC strings. Ids use the prefixes in `README.md`.

### 5.1 `hello`

**Direction:** node to control plane.
**When:** first message of every session, before any other message.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `node_id` | string (`nod_`) | yes | The node's identity; must match the credential |
| `agent_version` | string | yes | Version of the node agent binary |
| `engine_version` | string | yes | Version of the engine backend, for example the pinned Cloud Hypervisor release (`docs/research/cloud-hypervisor.md`) |
| `boot_id` | string | yes | Random value generated at agent process start; identifies this run of the agent |
| `stored_rev` | integer | yes | Newest desired-state `rev` persisted locally |
| `applied_rev` | integer | yes | Newest desired-state `rev` fully reconciled |
| `log_seq` | integer | yes | Last change-log `seq` verified locally; 0 if none |
| `log_hash` | string | yes | Hex SHA-256 the node holds at `log_seq`; 64 zeros when `log_seq` is 0 |
| `leases` | array of object | yes | Leases known locally, see below |
| `capacity` | object | yes | `cpus`, `memory_mb`, `disk_gb` |
| `allocated` | object | yes | Same shape, currently allocated |
| `kvm` | object | yes | `available`, `kernel`, `nested` (optional) |
| `dropped_events` | integer | yes | Local events dropped from the buffer since the last successful delivery; 0 when none |
| `mesh_public_key` | string | no | Base64 WireGuard public key of this machine (32 bytes), from M5a (ADR 0144). The machine generates the key pair on its first M5a start and keeps the private key next to its credential; only the public key is ever sent. The primary stores it in `nodes.mesh_public_key` and uses it in other machines' `spec.mesh.peers` |
| `volume_snapshot` | string | no | How this node snapshots a volume (ADR 0156): `reflink` when its data directory supports `FICLONE` or its volumes live in the node's XFS volume pool, otherwise `pause_copy`. Probed once at agent start (`engine-trait.md` section 3.16). Absent from an agent older than M5a, which the primary treats as a node that cannot hold a volume |

Each lease entry:

| Field | Type | Meaning |
| --- | --- | --- |
| `lease_id` | string (`lse_`) | Lease identity |
| `resource_type` | string | `app` or `primary` |
| `resource_id` | string | `app_` or `nod_` id |
| `mode` | string | `local` or `expiring` |
| `fencing_token` | integer | Last token this node saw for the lease |
| `state` | string | `held`, `uncertain`, `expired` or `revoked` |

`leases` lets the primary detect a node that believes it holds a lease the primary has already moved, before any desired state is sent. A lease the primary does not know, or one whose `fencing_token` is lower than the primary's, is revoked immediately after `welcome`.

```json
{
  "v": 1,
  "type": "hello",
  "id": "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
  "body": {
    "node_id": "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP",
    "agent_version": "0.1.0",
    "engine_version": "cloud-hypervisor v54.0",
    "boot_id": "8f1c2a4e-6b3d-4f5a-9c7e-1a2b3c4d5e6f",
    "stored_rev": 412,
    "applied_rev": 412,
    "log_seq": 902,
    "log_hash": "3b1f0c9a5e7d2b8f4a6c1d9e0f3a5b7c9d1e3f5a7b9c1d3e5f7a9b1c3d5e7f90",
    "leases": [
      {
        "lease_id": "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D",
        "resource_type": "app",
        "resource_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
        "mode": "local",
        "fencing_token": 7,
        "state": "held"
      }
    ],
    "capacity": { "cpus": 8, "memory_mb": 16384, "disk_gb": 200 },
    "allocated": { "cpus": 2, "memory_mb": 2048, "disk_gb": 20 },
    "kvm": { "available": true, "kernel": "6.11.2", "nested": true },
    "dropped_events": 0,
    "mesh_public_key": "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=",
    "volume_snapshot": "pause_copy"
  }
}
```

### 5.2 `heartbeat`

**Direction:** node to control plane.
**When:** every `heartbeat_interval_ms`, from the end of the handshake for as long as the session is open.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `time` | string | yes | Node wall-clock time when the heartbeat was sent |
| `busy` | boolean | no | True while a long apply loop is running; purely informational |

```json
{
  "v": 1,
  "type": "heartbeat",
  "id": "msg_01JC9Z9C4P6R8T0V2X4Z6B8D0F",
  "body": { "time": "2026-11-02T10:15:32Z", "busy": false }
}
```

### 5.3 `status_report`

**Direction:** node to control plane.
**When:** after the first reconciliation of a session, on any observed change, and at least every 30 s.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `applied_rev` | integer | yes | Newest desired-state `rev` fully reconciled |
| `vms` | array of object | yes | One entry per VM the node runs or is changing |
| `routes` | array of object | yes | One entry per hostname the node serves |
| `capacity` | object | yes | `cpus`, `memory_mb`, `disk_gb` |
| `allocated` | object | yes | Same shape, currently allocated |
| `disk_free_gb` | number | yes | Free space on the volume filesystem |
| `builds` | array of object | no | One entry per build in the node's current `spec.builds` (section 5.11); absent or empty when there is none (ADR 0117) |
| `volumes` | array of object | no | One entry per volume in the node's current `spec.volumes`, plus one per volume file the node holds that `spec.volumes` does not name (ADR 0145); absent or empty when there is none |
| `backups` | array of object | no | One entry per job in the node's current `spec.backups` (ADR 0147); absent or empty when there is none |
| `edge_backends` | array of object | no | On a machine with the `edge` role: one entry per backend of `spec.edge.routes` (ADR 0143); absent or empty otherwise |
| `images` | array of object | no | From M5a: one `{ "image_digest", "inventory_sha256" }` per prepared image the node holds, where `inventory_sha256` is the hex SHA-256 of the image's inventory (section 6.11) or absent while the node is still building it (ADR 0150) |

VM entry: `vm_id` (string, `vm_`), `observed_status` (one of `creating`, `stopped`, `starting`, `running`, `stopping`, `crashed`, `deleted`, `unknown`), `health` (`passing`, `failing`, `unknown`), `ip` (string, optional), `restart_count` (integer), `last_exit_code` (integer, optional).

Route entry: `hostname` (string), `cert_status` (one of `pending`, `issued`, `renewing`, `failed`, `not_applicable`), `cert_expires_at` (string, optional).

Build entry:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `build_id` | string (`bld_`) | yes | The build |
| `status` | string | yes | `queued`, `preparing`, `building`, `exporting`, `importing`, `succeeded`, `failed` or `cancelled` (README.md) |
| `commit` | string | no | Full commit SHA the ref resolved to, once the checkout finished |
| `image_digest` | string | no | Manifest digest of the built image, on `succeeded`; the node has already prepared it as a root disk |
| `failed_phase` | string | no | On `failed`: `preparing`, `building`, `exporting` or `importing` |
| `error` | string | no | On `failed`: one-line reason, at most 2 000 characters |
| `failed_step` | object | no | On `failed` in a build step: `vertex` (integer), `name` (string), `error` (string), `dockerfile_line` (integer, optional), `excerpt` (array of at most 20 strings, each at most 500 characters: the step's last log lines and the Dockerfile excerpt) |
| `started_at` | string | no | When the node left `queued` |
| `finished_at` | string | no | When the build reached a terminal status |

The primary owns the `builds` table and the `build.*` events: it applies each reported status change to the build row and emits the event, so the node emits no `build.*` event of its own. A status report may skip intermediate statuses; only the terminal one matters.

Volume entry:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `volume_id` | string (`vol_`) | yes | The volume |
| `state` | string | yes | `creating`, `ready`, `restoring`, `failed`, `deleted` (the file was removed after a `deleted` entry in `spec.volumes`) or `orphaned` (the node holds the file and `spec.volumes` does not name it) |
| `allocated_bytes` | integer | no | Bytes the sparse file uses on disk; present when the file exists |
| `error` | string | no | On `failed`: one-line reason, at most 2 000 characters |

Backup entry:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `backup_id` | string (`bkp_`) | yes | The backup |
| `status` | string | yes | `queued`, `running`, `succeeded` (the node wrote the manifest; the backup is not sealed yet) or `failed` |
| `snapshot_method` | string | no | From `running`: `reflink` or `pause_copy` |
| `pause_ms` | integer | no | From `running`, for `pause_copy`: how long the VM was paused |
| `manifest_key` | string | no | On `succeeded`: key of the manifest object, relative to the repository root (`backup-format.md` section 2) |
| `manifest_digest` | string | no | On `succeeded`: BLAKE3 of the encrypted manifest object, 64 lowercase hex |
| `logical_bytes` | integer | no | On `succeeded`: size of the snapshot |
| `uploaded_bytes` | integer | no | On `succeeded`: bytes of chunk objects this backup created |
| `chunk_count` | integer | no | On `succeeded`: chunks the manifest lists |
| `error` | string | no | On `failed`: one-line reason, at most 2 000 characters; `lease_mismatch` when the job's token is not the node's (section 6.7) |
| `started_at` | string | no | When the node left `queued` |
| `finished_at` | string | no | When the node finished its part |

Edge backend entry:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `app_id` | string (`app_`) | yes | App of the backend |
| `vm_id` | string (`vm_`) | yes | The instance |
| `node_id` | string (`nod_`) | yes | Node that runs it |
| `in_rotation` | boolean | yes | Whether the edge sends it requests |
| `since` | string | yes | When `in_rotation` last changed, or when the edge first saw the backend |
| `reason` | string | no | When out of rotation: `health_check` (its probe failed `failures` times in a row) or `connect_failed` (a request could not connect or was reset) |
| `last_error` | string | no | One line from the last failed probe or connection, at most 500 characters |

The primary does not store edge backend health. It keeps the newest report, shows it in `AppDetail.edge_backends` (`openapi.yaml`), and turns changes into events: `app.degraded` while some but not all instances of an app are out of rotation, `app.down` with `check: edge_health` when none is left, and `app.up` when every instance is back (`events.schema.json`).

The primary owns the `volumes` and `backups` tables and the `backup.*` and `restore.*` events, as it does for builds: it applies each reported change to the row, seals a reported `succeeded` backup (`backup-format.md` section 6.2), and emits the events. A node emits no `backup.*` or `restore.*` event of its own. A volume reported `deleted` is removed from the `volumes` table and leaves `spec.volumes` in the next revision. An `orphaned` volume is kept and never deleted by the protocol (section 6.7).

The primary treats a VM it knows and the node does not report as `unknown` rather than assuming it is gone; only an explicit `deleted` or a successful `inspect` removes it.

```json
{
  "v": 1,
  "type": "status_report",
  "id": "msg_01JC9ZA0D5Q7S9Z1W3Y5A7C9E1",
  "body": {
    "applied_rev": 412,
    "vms": [
      {
        "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
        "observed_status": "running",
        "health": "passing",
        "ip": "10.42.1.11",
        "restart_count": 0
      }
    ],
    "routes": [
      {
        "hostname": "blog.example.com",
        "cert_status": "issued",
        "cert_expires_at": "2027-01-31T09:00:00Z"
      }
    ],
    "capacity": { "cpus": 8, "memory_mb": 16384, "disk_gb": 200 },
    "allocated": { "cpus": 2, "memory_mb": 2048, "disk_gb": 20 },
    "disk_free_gb": 128.4
  }
}
```

### 5.4 `event`

**Direction:** node to control plane.
**When:** when something the cluster should know about happens on the node — a VM crash, an app going down, a node fencing itself, free disk crossing its threshold.

The body is the event envelope from `events.schema.json` **without** `seq`, which the primary assigns:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `schema_version` | integer | yes | Envelope version; `1` in v1. New optional payload fields do not change it |
| `id` | string (`evt_`) | yes | Event identity, generated by the node |
| `type` | string | yes | One of the event types in `README.md` |
| `time` | string | yes | When it happened, node clock |
| `source` | object | yes | `{ "kind": "node", "node_id": <nod_> }`, `node_id` must match the session |
| `subject` | object | yes | `{ "type": ..., "id": ... }` |
| `project_id` | string (`prj_`) | no | Project the subject belongs to |
| `incident_id` | string (`inc_`) | no | Set only when the primary correlates it |
| `request_id` | string (`req_`) | no | Present when the event answers an API call |
| `actor` | object | no | `{ "type": "node", "id": <nod_> }` for node-originated events |
| `data` | object | yes | Type-specific payload; may be `{}` |

The primary assigns `seq` on receipt and deduplicates by `id`, so a node that resends an event after a reconnect cannot produce two rows. The node buffers events while disconnected (section 6.5).

```json
{
  "v": 1,
  "type": "event",
  "id": "msg_01JC9ZB1E6R8T0V2X4Z6B8D0F2",
  "body": {
    "schema_version": 1,
    "id": "evt_01JC9Z91A2B3C4D5E6F7G8H9J0",
    "type": "app.down",
    "time": "2026-11-02T10:16:05Z",
    "source": { "kind": "node", "node_id": "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP" },
    "subject": { "type": "app", "id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E" },
    "project_id": "prj_01JC9Z5A1B2C3D4E5F6G7H8J9K",
    "actor": { "type": "node", "id": "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP" },
    "data": { "failures": 3, "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F" }
  }
}
```

### 5.5 `lease_renew`

**Direction:** node to control plane.
**When:** `expiring` leases only, every `duration_ms / 3` while the lease is `held` or `uncertain`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `lease_id` | string (`lse_`) | yes | Lease being renewed |
| `fencing_token` | integer | yes | Token the node believes it holds |

The primary answers with a `lease_grant` that carries the same `fencing_token` when the node still holds the lease — renewals keep the token, they never change it (D-0073) — with `re` set to this message's `id`. A renewal whose token is lower than the primary's current token, or for a lease that no longer exists, is answered with `lease_revoke`. The node also sends one `lease_renew` immediately after a first grant (section 8.4). `local` leases are never renewed.

```json
{
  "v": 1,
  "type": "lease_renew",
  "id": "msg_01JC9ZC2F7S9Z1W3Y5A7C9E1G3",
  "body": { "lease_id": "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D", "fencing_token": 7 }
}
```

### 5.6 `log_ack`

**Direction:** node to control plane.
**When:** after the node has verified and stored a batch of `log_entries`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `seq` | integer | yes | Highest contiguous `seq` verified |
| `hash` | string | yes | The `hash` of that entry, hex SHA-256 |

The primary keeps sending from `seq + 1`. An ack that skips a sequence is ignored: the node must verify every entry in order, because the point of the log is that a gap or a broken link is detectable (D-0013).

```json
{
  "v": 1,
  "type": "log_ack",
  "id": "msg_01JC9ZD3G8T0V2X4Z6B8D0F2H4",
  "body": {
    "seq": 950,
    "hash": "9c7e1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f6a7b8c9d0e1"
  }
}
```

### 5.7 `stream_data`

**Direction:** node to control plane.
**When:** while a stream opened by `stream_open` is producing data.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | Stream the data belongs to, chosen by the primary in `stream_open` |
| `seq` | integer | yes | Per-stream sequence, gap-free from 1 |
| `lines` | array of object | no | Log lines; present for `app_logs`, `deployment_logs` and `build_logs` |
| `data` | string | no | Base64 bytes, at most 64 KiB decoded per message; present for `terminal`, `console` and `transcript` |
| `metrics` | object | no | One sample; present for `metrics` (section 5.17, `metrics_query`, for its shape) |

Exactly one of `lines`, `data` and `metrics` is present. A log line is `{ "time": <RFC 3339>, "stream": "stdout"|"stderr", "line": <string without trailing newline>, "vm_id": <vm_> }`; `vm_id` is optional and names the VM that produced the line, so a stream opened for an app with several VMs stays attributable. Lines come from the engine's output stream, which the minimal init tags by stream on the guest side (`engine-trait.md`); the node does not guess. A build log line also carries `seq` (integer, gap-free from 1 per build, the position used by the API's cursor and `Last-Event-ID`) and, when BuildKit names one, `vertex` (integer, the step number); it never carries `vm_id`.

Byte streams (`data`) coalesce output over a few milliseconds, so one echoed keystroke is not one message, and obey the stream window: the node stops reading its source while more than 256 KiB it sent on the stream are unacknowledged (section 5.25). On `terminal` and `console` streams the bytes are what the guest wrote, unparsed; the node never interprets them.

```json
{
  "v": 1,
  "type": "stream_data",
  "id": "msg_01JC9ZE4H9Z1W3Y5A7C9E1G3J5",
  "body": {
    "stream_id": "msg_01JC9ZD9K2M4P6R8T0V2X4Z6B8",
    "seq": 17,
    "lines": [
      { "time": "2026-11-02T10:16:30Z", "stream": "stdout", "line": "listening on :8080" }
    ]
  }
}
```

### 5.8 `stream_end`

**Direction:** node to control plane.
**When:** the stream finished, failed, or was closed by either side.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | Stream that ended |
| `reason` | string | yes | `completed`, `error`, `cancelled`, `vm_stopped` or `limit` |
| `message` | string | no | Human-readable detail, for example the engine error |
| `exit_code` | integer | no | `terminal` only: the program's exit code, or `128 + signal`, when it exited |
| `transcript_sha256` | string | no | `terminal` only: lowercase hex SHA-256 of the session transcript as stored on the node |
| `transcript_bytes` | integer | no | `terminal` only: size of that transcript |

The primary closes the matching API or panel consumer. `cancelled` is the answer to `stream_close`; `error` carries whatever `EngineError` the node saw. `completed` ends a `terminal` whose program exited (with `exit_code`), a `build_logs` stream once the build is terminal and every line was sent, and a `transcript` stream once the file was sent. `vm_stopped` ends a `terminal` whose VM stopped and a `console` whose VM was deleted; a console stream outlives VM restarts, so a viewer sees the next boot. `limit` ends a `terminal` whose transcript reached its cap (16 MiB, `audit-log-format.md` section 3.3). A `terminal` stream_end always carries the transcript fields when a transcript file exists, whatever the reason, so the primary can write `session.end`.

```json
{
  "v": 1,
  "type": "stream_end",
  "id": "msg_01JC9ZF5J0V2X4Z6B8D0F2H4K6",
  "body": {
    "stream_id": "msg_01JC9ZD9K2M4P6R8T0V2X4Z6B8",
    "reason": "completed"
  }
}
```

A terminal whose shell exited:

```json
{
  "v": 1,
  "type": "stream_end",
  "id": "msg_01JC9ZF6K1W3Y5A7C9E1G3J5M7",
  "body": {
    "stream_id": "msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9",
    "reason": "completed",
    "exit_code": 0,
    "transcript_sha256": "4f2a9c1e7b3d5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8",
    "transcript_bytes": 18234
  }
}
```

### 5.9 `command_result`

**Direction:** node to control plane.
**When:** as the answer to `desired_state`, `lease_revoke`, `command`, `signing_key_rotate`, and a `stream_open` that could not start.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `status` | string | yes | `ok` or `error` |
| `detail` | string | no | Short human-readable detail |
| `error` | object | no | `{ "code": ..., "message": ... }` using the `error` codes in section 5.18 |
| `lines` | array of object | no | Log lines, oldest first, in the line shape of section 5.7; present only in the answer to `command` kind `logs_query` or `build_logs_query` |
| `metrics` | object | no | Present only in the answer to `command` kind `metrics_query` (section 5.17) |

`re` identifies the message being answered. For `desired_state` the answer means **stored**: the signature was verified and the snapshot was persisted atomically. It does not mean applied — that is `applied_rev` in `status_report`. For `lease_revoke` the answer means the stateful VMs bound to the lease are stopped and recorded. A `lease_revoke` whose self-fence could not confirm that every bound VM stopped is answered with `status: error` and `error.code` `fence_failed` (sections 5.18 and 8.5); `detail` names the VMs that may still run.

```json
{
  "v": 1,
  "type": "command_result",
  "id": "msg_01JC9ZG6K1W3Y5A7C9E1G3J5M7",
  "re": "msg_01JC9ZF9W2M4P6R8T0V2X4Z6B8",
  "body": { "status": "ok", "detail": "stored rev 412" }
}
```

A `lease_revoke` whose fence failed:

```json
{
  "v": 1,
  "type": "command_result",
  "id": "msg_01JC9ZG7M2X4Z6B8D0F2H4K6N8",
  "re": "msg_01JC9ZF0X3N5Q7S9V1W3Y5A7C9",
  "body": {
    "status": "error",
    "detail": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F neither stopped nor gone after the forced stop",
    "error": {
      "code": "fence_failed",
      "message": "self-fence of lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D could not confirm every bound VM stopped"
    }
  }
}
```

### 5.10 `welcome`

**Direction:** control plane to node.
**When:** immediately after a valid `hello`, as the first message from the primary.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `cluster_id` | string (`cls_`) | yes | Cluster the session belongs to |
| `term` | integer | yes | Cluster term; `1` until elections exist |
| `server_time` | string | yes | Primary's wall-clock time |
| `heartbeat_interval_ms` | integer | yes | 5 000 in v1 |
| `session_timeout_ms` | integer | yes | 15 000 in v1 |
| `primary_node_id` | string (`nod_`) | yes | The node holding the primary status |
| `primary_urls` | array of string | no | Other base URLs where the primary's listener can be reached, best first; at most 8 (ADR 0139) |

**`primary_urls`.** The primary fills the list with its `public_url` when one is configured, then `https://<address>:<port>` for each non-loopback, non-link-local address of its host (IPv6 in brackets), then `https://<host name>.local:<port>` when that name resolves on the host, where `<port>` is the listener's port. Addresses of the node bridge are left out. Every entry is an `https://` base URL with no path. The field is optional and additive (section 3.4): a primary that does not send it, or sends an empty list, is valid, and a node that does not know it ignores it.

The node keeps the list next to `primary_url` in its identity (section 12). It drops every entry that is not an `https://` URL and every duplicate, keeps the first 8, and replaces the stored list only when the result differs; a `welcome` without the field leaves the stored list as it is. When a dial to `primary_url` fails (the TCP connection, the TLS handshake against the pinned fingerprint, or the WebSocket upgrade), the node tries the stored URLs in order, with the same pinned fingerprint and the same credential, before it waits out the redial backoff (section 4.4). The first one whose session reaches `welcome` becomes the node's `primary_url` and is persisted. The pin and the credential never change: a host that does not hold the primary's private key cannot complete the handshake, so a stale or hostile entry costs one failed dial and nothing else. A `401` or `403` on the upgrade from a host that passed the pin is fatal as on `primary_url`. An operator can also point a node at a new address by hand with `quoro node set-primary <url>`, which completes a pinned handshake before it writes anything.

```json
{
  "v": 1,
  "type": "welcome",
  "id": "msg_01JC9ZH7W2X4Z6B8D0F2H4K6N8",
  "re": "msg_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
  "body": {
    "cluster_id": "cls_01JC9Z0000000000000000000Q",
    "term": 1,
    "server_time": "2026-11-02T10:15:30Z",
    "heartbeat_interval_ms": 5000,
    "session_timeout_ms": 15000,
    "primary_node_id": "nod_01JC9Z0000000000000000000Q",
    "primary_urls": [
      "https://quoro.example.com:8443",
      "https://192.0.2.10:8443",
      "https://[2001:db8::10]:8443"
    ]
  }
}
```

### 5.11 `desired_state`

**Direction:** control plane to node.
**When:** once per session after `welcome` and any `lease_revoke`, and again whenever the desired state for this node changes.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `rev` | integer | yes | Monotonic revision of the node's desired state |
| `issued_at` | string | yes | When the primary produced it |
| `term` | integer | yes | Cluster term at issue time |
| `spec` | object | yes | `vms`, `routes`, `network`, `ingress` when the node runs a tunnel, `builds` when the node has builds to run (ADR 0117), `volumes` when the node holds volumes (ADR 0145), `backups` with `backup_repository` when it has backup jobs (ADR 0147), `mesh` once the machine is on the mesh, and `edge` on a machine with the `edge` role (ADR 0143, ADR 0144). An edge-only machine gets `vms` and `routes` as empty lists and no `network` |
| `signature` | string | yes | Base64 Ed25519 signature by the cluster signing key |

**Signature.** The signature covers the RFC 8785 (JSON Canonicalization Scheme) serialization of the whole `body` object with the `signature` member removed. The node verifies it against `cluster_public_key` before it parses anything else, persists the verified snapshot atomically, answers `command_result` with `ok`, and only then reconciles. An invalid signature is answered with `error` code `bad_signature` and the snapshot is discarded; the node keeps running on its previously stored snapshot.

**Full snapshot only.** v1 has no deltas. Every `desired_state` is the complete set of VMs and routes for this node, so a lost message is harmless and a node that missed several revisions converges on the next snapshot.

**`spec.vms[]`:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `vm_id` | string (`vm_`) | yes | VM identity |
| `project_id` | string (`prj_`) | yes | Owning project |
| `app_id` | string (`app_`) | no | Set when this VM is an instance of an app; absent for a standalone VM |
| `deployment_id` | string (`dep_`) | no | The deployment that produced this VM, when it is an app instance; selects the VM for `stream_open` kind `deployment_logs` |
| `name` | string | yes | Operator-visible name |
| `image` | string | yes | OCI reference as requested; a built image's reference is `build.quoro.invalid/<app_id>:<build_id>` (README.md, "Built images") |
| `image_digest` | string | yes | Resolved digest |
| `build_id` | string (`bld_`) | no | Set when the image is a build's output on this node; the node prepares it from that build and never pulls it (section 6.6) |
| `vcpus` | integer | yes | vCPUs |
| `memory_mb` | integer | yes | Memory in MiB |
| `disk_gb` | integer | yes | Root disk size |
| `port` | integer | no | Container port the app listens on |
| `env` | object | yes | Environment values, decrypted for this node; `{}` when none. It never holds `QUORO_INSTANCE_ID` or `QUORO_NODE_ID`: the node adds those (section 6.10) |
| `health_check` | object | no | `path`, `interval_s`, `timeout_s`, `failures`, and from M5b `start_period_s` and `restart` (section 6.3) |
| `desired_status` | string | yes | `running` or `stopped` |
| `restart_generation` | integer | yes | Incremented by the primary on an explicit restart |
| `stateful` | boolean | yes | Whether the VM may run only while its lease is held |
| `lease_id` | string (`lse_`) | no | Required when `stateful` is true |
| `fencing_token` | integer | no | Required when `stateful` is true |
| `volumes` | array of object | yes | Volumes to attach, at most one in M5a; `[]` for a stateless VM. Each `{ "volume_id": <vol_>, "serial": <string>, "mount_path": <string> }`: the volume must be `ready` on this node and named in `spec.volumes`; `serial` is the block device serial the guest init finds it by, `quoro-vol-0` for the first (`engine-trait.md` section 3.6) |

**`spec.routes[]`:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `hostname` | string | yes | Host to serve |
| `app_id` | string (`app_`) | yes | App behind the route |
| `vm_ids` | array of string | yes | Candidate backends on this node. From M5a a node gets routes only for apps with one replica, so this holds one VM; the routes of apps with more replicas go to the edge (`spec.edge`). The one exception is a route with `draining_until`, which a node keeps for a short time after its app moved to the edge |
| `port` | integer | yes | Backend port |
| `tls_mode` | string | yes | `acme` or `tunnel` |
| `domain_id` | string (`dom_`) | no | The domain this route serves (`domains.id`); filled by the primary for every route it builds from a domain |
| `draining_until` | string (RFC 3339) | no | From M5b, on `tunnel` routes only: the hostname has left this machine and is kept here until this time (see "Draining tunnel routes" below, ADR 0168) |

**Draining tunnel routes (ADR 0168).** When a `tunnel` hostname moves from one machine's tunnel to another's (an app going from one replica to more moves it from its node to the edge, and back when it returns to one), the primary repoints the hostname's CNAME at once, but Cloudflare keeps sending some requests to the old tunnel for up to about two minutes. So the primary keeps the departed route in the losing machine's desired state, and the hostname in the losing tunnel's ingress rules, for a grace window of 120 s after it repointed the CNAME, and marks the route with `draining_until`, the end of the window. The route keeps only backends that still run: on a node, its own VM, which stays as one of the app's instances; on the edge, the remaining instance. A machine left with no such backend gets no draining route. The machine serves a draining route exactly like any other route and does not act on the time itself: the primary drops the route, and the hostname from the losing tunnel's ingress, in the first desired state it issues after `draining_until`. While draining, the hostname is in the desired state of both machines, each serving it from its own tunnel. `draining_until` is additive: a node that does not know it serves the route as usual, which is the intended behaviour.

**Domain ids and certificate events.** The node names the domain of a route in every `certificate.*` event it emits (`certificate.issued`, `certificate.expiring`, `certificate.failed`; section 5.4): `subject` is `{ "type": "domain", "id": <domain_id> }` and `data.domain_id` carries the same id, as `events.schema.json` requires. `domain_id` is an additive optional field: a node that does not know it ignores it, and a node that receives a route without it (from an older primary) reports the certificate state in `status_report.routes` only and emits no `certificate.*` event for that hostname, because it never invents an id.

**`spec.builds[]`** (optional; ADR 0117, ADR 0118):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `build_id` | string (`bld_`) | yes | Build identity |
| `app_id` | string (`app_`) | yes | App the build belongs to; also the build cache key, so caches never mix between apps |
| `project_id` | string (`prj_`) | yes | Owning project |
| `repo_url` | string | yes | `https://` URL of a public Git repository |
| `ref` | string | yes | Branch, tag or full commit SHA to build; the node resolves it to a commit |
| `dockerfile_path` | string | yes | Path of the Dockerfile, relative to `context_dir` |
| `context_dir` | string | yes | Build context, relative to the repository root; `.` is the root |
| `target` | string | no | Stage of a multi-stage Dockerfile to build |
| `build_args` | object | yes | `--build-arg` name to value; `{}` when none |
| `secrets` | object | yes | BuildKit secret id to plaintext value, taken from the app's environment variables named in the source; `{}` when none |
| `limits` | object | yes | `vcpus`, `memory_mb`, `timeout_s`, `cache_gb`, `output_gb` |
| `cancelled` | boolean | yes | True once a cancel was requested; the node stops the build and reports `cancelled` |
| `refresh_base` | boolean | no | From M5a: when true, the builder resolves every base image again from its registry (`image-resolve-mode=pull`) instead of using the cached one, so a patched base is picked up (ADR 0150). Absent means false |

A build stays in `spec.builds` until the primary has recorded the terminal status the node reported. A succeeded build leaves `spec.builds` in the same revision that references its image in `spec.vms` (the deployment it started), so the apply loop never sees the built root disk unreferenced and removes it (section 6.2). `secrets` values are handled like `env`: plaintext only inside the signed snapshot, encrypted with the node-local key at rest, never logged and never written to the build's log.

**`spec.volumes[]`** (optional; ADR 0145):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `volume_id` | string (`vol_`) | yes | Volume identity; also the name of its file in the node's data directory |
| `app_id` | string (`app_`) | yes | App that owns it |
| `size_gb` | integer | yes | Size of the sparse file, in gibibytes |
| `state` | string | yes | `present`: the file must exist; `deleted`: the node removes the file once no VM uses it, then reports `deleted` |
| `restore` | object | no | Present only while the volume is `restoring`: write the file from this backup instead of creating it empty |

**`spec.volumes[].restore`:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `backup_id` | string (`bkp_`) | yes | Backup to restore |
| `manifest_key` | string | yes | Key of its manifest object, relative to the repository root |
| `manifest_digest` | string | yes | BLAKE3 of the encrypted manifest object, from the backup's seal record; the node refuses a manifest that does not match |
| `logical_bytes` | integer | yes | Size the chunks must cover exactly |
| `id_key` | string | yes | Base64 of the app's 32-byte chunk-id key (`backup-format.md` section 3) |
| `data_key` | string | yes | Base64 of the app's 32-byte chunk-data key |

**`spec.backups[]`** (optional; ADR 0147):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `backup_id` | string (`bkp_`) | yes | Backup identity; also the name of its manifest object |
| `app_id` | string (`app_`) | yes | App whose volume is backed up; the backup's `source_id` |
| `volume_id` | string (`vol_`) | yes | Volume to back up; `ready` on this node and named in `spec.volumes` |
| `vm_id` | string (`vm_`) | no | The app's VM on this node, which a `pause_copy` snapshot pauses; absent when the app has no VM here |
| `fencing_token` | integer | yes | Token of the app's `local` lease when the backup was dispatched |
| `id_key` | string | yes | Base64 of the app's 32-byte chunk-id key |
| `data_key` | string | yes | Base64 of the app's 32-byte chunk-data key |

**`spec.backup_repository`** (optional; present while `spec.backups` or a `spec.volumes[].restore` is not empty):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `repository_id` | string (`brp_`) | yes | The backup repository |
| `endpoint` | string | yes | `https://` URL of the S3-compatible endpoint, from the provider's `config` |
| `region` | string | yes | Signing region, from the provider's `config`; `auto` when it names none |
| `bucket` | string | yes | Bucket name, from the provider's `config` |
| `root` | string | yes | `<prefix>/<cluster_id>`, the key prefix every repository key is relative to |
| `access_key_id` | string | yes | From the provider's `config` |
| `secret_access_key` | string | yes | The provider's secret, plaintext |

**Backup keys and credentials.** The node gets the chunk keys of one app per job and never the repository key, so it can read and write only that app's backups (`backup-format.md` section 3). `id_key`, `data_key` and `secret_access_key` are handled like `env`: plaintext only inside the signed snapshot on the session, encrypted with the node-local key at rest, never logged, and dropped from the node's store when the field leaves desired state. The primary sends the bucket credential only while the node has a job; the credential of the off-site audit copy never reaches a node.

**`spec.network`:** `subnet` (CIDR for the node bridge) and `gateway` (address on that bridge). The addresses in `status_report` must fall inside the subnet. From M5a the subnet is the node's own (`nodes.subnet`, ADR 0144), so two nodes never share one; absent on a machine without the `node` role. A changed subnet is applied as section 6.9 describes.

**`spec.mesh`** (optional; ADR 0144):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `address` | string | yes | This machine's mesh address, `nodes.mesh_address`, with prefix length `/16` |
| `listen_port` | integer | yes | UDP port the interface listens on; 51820 unless the operator changed it |
| `peers` | array of object | yes | The other end of each tunnel; on a node only the edge, on the edge every node with the `node` role (a star in M5a) |
| `allow` | array of object | yes | On a node: what the edge may reach through the interface, one `{ "source": <edge mesh address>, "address": <guest address>, "port": <integer> }` per backend of this node; `[]` on the edge |

Each peer:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `node_id` | string (`nod_`) | yes | The peer machine |
| `public_key` | string | yes | Its `mesh_public_key` |
| `endpoint` | string | no | `host:port` to dial; absent for a peer behind NAT, which dials this machine instead |
| `allowed_ips` | array of string | yes | CIDRs routed to the peer: on the edge, a node's mesh address `/32` and its guest subnet; on a node, the edge's mesh address `/32` |
| `persistent_keepalive_s` | integer | no | Keepalive interval, set by the primary for a machine behind NAT; 25 by default |

**`spec.edge`** (optional; on a machine with the `edge` role; ADR 0143):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `routes` | array of object | yes | One per hostname the edge serves |
| `tunnel` | object | no | The edge's own tunnel, same shape and handling as `spec.ingress.tunnel`; present when the edge serves `tunnel` routes |

Each edge route:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `hostname` | string | yes | Host to serve |
| `domain_id` | string (`dom_`) | yes | The domain, for `certificate.*` events as in `spec.routes` |
| `app_id` | string (`app_`) | yes | App behind the route |
| `tls_mode` | string | yes | `acme` or `tunnel` |
| `health_check` | object | yes | The app's `path`, `interval_s`, `timeout_s`, `failures`. The edge ignores `start_period_s` and `restart`, which only the node acts on (section 6.3) |
| `backends` | array of object | yes | One per instance: `{ "vm_id", "node_id", "address", "port" }`, where `address` is the instance's guest address in its node's subnet and `port` the app port |
| `draining_until` | string (RFC 3339) | no | From M5b, on `tunnel` routes only: the app is back to one replica and its hostname moved to that node's tunnel; the edge keeps serving it from its own tunnel until this time ("Draining tunnel routes" above, ADR 0168) |

**`spec.ingress`** (optional; absent when the node runs no tunnel):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `tunnel` | object | no | The node's Cloudflare Tunnel (D-0024, D-0094); present when the node is mapped to a `tunnel` provider (`node_tunnels`, migration 0003) and the tunnel exists |

**`spec.ingress.tunnel`:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `tunnel_id` | string | yes | The provider's id of this node's tunnel |
| `token` | string | yes | The tunnel's run token, plaintext; it lets the holder run this one tunnel and nothing else |
| `origin_port` | integer | yes | Loopback port where the node's proxy accepts the tunnel's traffic |

**Tunnels (D-0094).** One tunnel per node. The primary creates or looks up the node's tunnel with the account credential of the `tunnel` provider, writes the tunnel's ingress rules (each `tunnel` route's hostname to `http://127.0.0.1:<origin_port>`) and the proxied DNS records itself, and sends the node only `tunnel_id`, `token` and `origin_port`. The node runs the tunnel connector with the token and serves the `tunnel` routes of `spec.routes` from its proxy on `127.0.0.1:<origin_port>`. The account credential never leaves the primary. A changed `token` (rotation) replaces the running connector; an absent `tunnel` stops it.

**Environment values.** `env` (and `spec.builds[].secrets`) carries plaintext values because the node must be able to start the process. It is sent only over the authenticated, pinned session, never over a webhook or an API read, and the node stores it encrypted with the node-local key. The primary stores the same values encrypted with the cluster data-encryption key (AEAD), and the API returns only names (`README.md`, `db-schema.sql`). `spec.ingress.tunnel.token` and the backup fields above are handled the same way: plaintext only inside the signed snapshot on the session, encrypted with the node-local key at rest on the node, encrypted with the cluster data-encryption key on the primary (`node_tunnels.token_enc`), never returned by the API.

```json
{
  "v": 1,
  "type": "desired_state",
  "id": "msg_01JC9ZJ8M3Y5A7C9E1G3J5M7P9",
  "body": {
    "rev": 413,
    "issued_at": "2026-11-02T10:15:31Z",
    "term": 1,
    "spec": {
      "vms": [
        {
          "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
          "project_id": "prj_01JC9Z5A1B2C3D4E5F6G7H8J9K",
          "app_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
          "name": "blog-web",
          "image": "ghcr.io/example/blog:1.4.2",
          "image_digest": "sha256:5c1f0d2a9b8e7f6a5d4c3b2a1908f7e6d5c4b3a29180f7e6d5c4b3a291807f6e",
          "vcpus": 2,
          "memory_mb": 512,
          "disk_gb": 4,
          "port": 8080,
          "env": { "DATABASE_URL": "postgres://blog@10.42.1.5/blog" },
          "health_check": { "path": "/", "interval_s": 10, "timeout_s": 2, "failures": 3 },
          "desired_status": "running",
          "restart_generation": 1,
          "stateful": false,
          "volumes": []
        }
      ],
      "routes": [
        {
          "hostname": "blog.example.com",
          "app_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
          "vm_ids": ["vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F"],
          "port": 8080,
          "tls_mode": "acme",
          "domain_id": "dom_01JC9Z8D5Q7S9V1X3Z5B7D9F1H"
        }
      ],
      "network": { "subnet": "10.42.1.0/24", "gateway": "10.42.1.1" }
    },
    "signature": "MEUCIQDf3k...c2lnbmF0dXJl"
  }
}
```

A replica node that holds a copy of the change log but is not the primary never sends `desired_state`; it can only be the primary's job, because the primary is the single writer (D-0012).

### 5.12 `lease_grant`

**Direction:** control plane to node.
**When:** when the primary (or, from M5b, the authority behind it) grants a lease, and as the answer to every successful `lease_renew`. From M5a the primary grants a `local` lease on an app when it places a stateful app: when the app's first volume is created, and on the target node when a restore completes (section 8.3).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `lease_id` | string (`lse_`) | yes | Lease identity |
| `resource_type` | string | yes | `app` or `primary` |
| `resource_id` | string | yes | `app_` or `nod_` id |
| `mode` | string | yes | `local` or `expiring` |
| `fencing_token` | integer | yes | Changes only when ownership changes: the same value as the node's current token when the grant answers a renewal by the current holder; a strictly higher value than any earlier token for the resource when the lease goes to a new holder or is granted again after expiry or revoke. Never reused (D-0073). |
| `duration_ms` | integer | no | Present for `expiring`, absent for `local` |
| `granted_at` | string | yes | Authority's wall-clock time of the grant |

A grant whose token is lower than the highest token the node has seen for that resource, or equal to it while the node does not hold that lease (state `held` or `uncertain`) under that token, is a bug in the authority and is refused with `error` code `bad_message`, non-fatal: the node keeps its current state and starts or stops nothing.

```json
{
  "v": 1,
  "type": "lease_grant",
  "id": "msg_01JC9ZK9N4Z6B8D0F2H4K6N8Q0",
  "re": "msg_01JC9ZC2F7S9Z1W3Y5A7C9E1G3",
  "body": {
    "lease_id": "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D",
    "resource_type": "app",
    "resource_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
    "mode": "expiring",
    "fencing_token": 7,
    "duration_ms": 15000,
    "granted_at": "2026-11-02T10:15:33Z"
  }
}
```

### 5.13 `lease_revoke`

**Direction:** control plane to node.
**When:** the lease moved to another node, the resource was deleted, an operator declared this node lost, or a higher token superseded it.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `lease_id` | string (`lse_`) | yes | Lease to revoke |
| `fencing_token` | integer | yes | Token of the revoke; at least the node's current token for the lease |
| `reason` | string | yes | `moved`, `deleted`, `declared_lost` or `superseded` |

A revoke whose token is lower than the node's current token predates the node's latest grant; it is refused with `error` code `bad_message`, non-fatal, and the node keeps the lease and its VMs as they are (D-0073).

On receipt the node stops the stateful VMs bound to the lease, records the stop locally, moves the lease to `revoked`, and answers `command_result` **before** it processes any later message of the session. If the self-fence cannot confirm that every bound VM stopped, the answer is `command_result` with status `error` and the node follows section 8.5's failed-fence path.

```json
{
  "v": 1,
  "type": "lease_revoke",
  "id": "msg_01JC9ZM0P5B8D0F2H4K6N8Q0S2",
  "body": {
    "lease_id": "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D",
    "fencing_token": 8,
    "reason": "moved"
  }
}
```

### 5.14 `log_entries`

**Direction:** control plane to node.
**When:** after the desired state of a session, then continuously as the change log grows.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `from_seq` | integer | yes | `seq` of the first entry in `entries` |
| `entries` | array of object | yes | Change-log entries, at most 500 |

Each entry has exactly the change-log fields from the shared entity model: `seq` (gap-free from 1), `term`, `time`, `actor_type`, `actor_id`, `entity_type`, `entity_id`, `op` (`create`, `update`, `delete`), `payload` (full new row for create and update, `{}` for delete), `prev_hash` (64 hex, 64 zeros for `seq` 1) and `hash` (hex SHA-256 over the RFC 8785 serialization of the entry without `hash`).

```json
{
  "v": 1,
  "type": "log_entries",
  "id": "msg_01JC9ZN1Q6C8D0F2H4K6N8Q0S2",
  "body": {
    "from_seq": 903,
    "entries": [
      {
        "seq": 903,
        "term": 1,
        "time": "2026-11-02T10:15:31Z",
        "actor_type": "agent",
        "actor_id": "agt_01JC9Z7D5Q7S9Z1W3Y5A7C9E1G",
        "entity_type": "app",
        "entity_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
        "op": "update",
        "payload": { "id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E", "name": "blog" },
        "prev_hash": "3b1f0c9a5e7d2b8f4a6c1d9e0f3a5b7c9d1e3f5a7b9c1d3e5f7a9b1c3d5e7f90",
        "hash": "9c7e1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f6a7b8c9d0e1"
      }
    ]
  }
}
```

### 5.15 `stream_open`

**Direction:** control plane to node.
**When:** a human or an agent reads live logs, or the panel opens a stream.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | Stream identity chosen by the primary |
| `kind` | string | yes | `app_logs`, `deployment_logs`, `build_logs`, `metrics`, `terminal`, `console` or `transcript` |
| `params` | object | yes | Kind-specific selector |

| Kind | `params` | Data | Ends |
| --- | --- | --- | --- |
| `app_logs` | `app_id` or `vm_id` (exactly one), optional `tail` (integer, default 0) | `lines` | on `stream_close` |
| `deployment_logs` | `deployment_id`, optional `phase` | `lines` | on `stream_close` |
| `build_logs` | `build_id`; optional `tail` (integer: start with the last `tail` lines; default 0) or `after_seq` (integer: start after that line), not both | `lines` with `seq` | `completed` once the build is terminal and every line was sent (ADR 0119) |
| `metrics` | `node` (boolean) and `vm_ids` (array of `vm_` ids), at least one of them set | `metrics`, one sample per sampling interval | on `stream_close` |
| `terminal` | `session_id` (`trm_`), `vm_id`, `rows` and `cols` (integers, 1 to 1 000), `argv` (array of strings, at least one), optional `user` | `data`; input through `stream_input` | `completed` with `exit_code`, `vm_stopped`, `limit`, or `cancelled` on `stream_close` (ADR 0113) |
| `console` | `vm_id`, optional `tail_bytes` (integer: replay at most that many bytes of the VM's console ring first; default the whole ring) | `data`; `stream_input` is refused | `vm_stopped` when the VM is deleted, or `cancelled` (ADR 0116) |
| `transcript` | `session_id` (`trm_`) | `data`: the transcript file, from its first byte | `completed` |

A kind this node cannot serve right now is answered with `command_result` status `error`: code `not_found` when the build, VM or transcript is not on this node, `not_available` when the VM exists but cannot serve the kind (a `terminal` on a VM started before the M4 guest init, or on a stopped VM). A `terminal` stream is one PTY session of its own: several terminal streams on one VM are several independent sessions. Any number of `console` streams may follow one VM; the node fans its one console reader out to all of them and none of them ever connects to the VM's serial socket (ADR 0116).

The primary enforces 32 concurrent streams per session. A 33rd open is answered with `command_result` status `error`, code `bad_message`, detail `stream_limit`, without touching the node's streams.

```json
{
  "v": 1,
  "type": "stream_open",
  "id": "msg_01JC9ZP2R7D0F2H4K6N8Q0S2T4",
  "body": {
    "stream_id": "msg_01JC9ZD9K2M4P6R8T0V2X4Z6B8",
    "kind": "app_logs",
    "params": { "app_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E", "tail": 100 }
  }
}
```

A web terminal:

```json
{
  "v": 1,
  "type": "stream_open",
  "id": "msg_01JC9ZP3S8E1G3J5M7P9R1T3W5",
  "body": {
    "stream_id": "msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9",
    "kind": "terminal",
    "params": {
      "session_id": "trm_01JC9ZA3C4D5E6F7G8H9J0K1M2",
      "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
      "rows": 24,
      "cols": 80,
      "argv": ["/bin/sh"]
    }
  }
}
```

### 5.16 `stream_close`

**Direction:** control plane to node.
**When:** the consumer went away — the HTTP request ended, the panel tab closed, the agent's token expired.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | Stream to stop |

The node stops producing, releases the stream slot, and answers `stream_end` with reason `cancelled`. Closing an unknown stream id is ignored.

```json
{
  "v": 1,
  "type": "stream_close",
  "id": "msg_01JC9ZQ3S8F2H4K6N8Q0S2T4Z6",
  "body": { "stream_id": "msg_01JC9ZD9K2M4P6R8T0V2X4Z6B8" }
}
```

### 5.17 `command`

**Direction:** control plane to node.
**When:** the primary needs a one-off action that is not a change of desired state.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `kind` | string | yes | `logs_query`, `probe`, `fence_ack`, `build_logs_query`, `metrics_query` or `inventory_resend` |
| `params` | object | yes | Kind-specific parameters |

`logs_query` params: `app_id` or `vm_id` (exactly one), `since` (RFC 3339, optional), `until` (RFC 3339, optional), `tail` (integer, optional). The answer is one `command_result` with `status: ok` and the matching lines in `lines` (section 5.9), each carrying its `vm_id`. The answer must fit the 1 MiB message limit: the node keeps the newest lines that fit and says in `detail` that older ones were left out.

`probe` params: `{}`. The node re-runs the KVM probe (`/dev/kvm`, CPU flags, kernel release) and answers with a `command_result` whose `detail` summarizes it; the primary updates the node's `kvm` record and `engine_version`.

`fence_ack` params: `lease_ids` (array of `lse_` ids, at least one). It carries an operator's acknowledgement of failed fences, recorded through `POST /api/v1/nodes/{node_id}/fence-ack` (section 8.5). The node clears the failed fence it recorded for each listed lease, and clears its failed mark once no failed fence remains. It answers `command_result` with `status: ok`; when failed fences of other leases remain, the node stays failed and `detail` names them. A listed lease with no recorded failed fence is not an error, so a redelivered `fence_ack` is harmless. The listed leases stay `revoked` or `expired` on the node: it starts nothing stateful for them until a `lease_grant` arrives with a higher `fencing_token` (section 8.5 step 4).

`build_logs_query` params: `build_id`, `after_seq` (integer, optional, default 0), `limit` (integer, optional, default 500, at most 1 000). The answer is one `command_result` with `status: ok` and the build's log lines after `after_seq` in `lines`, in the build-log line shape of section 5.7, oldest first, at most `limit` and within the 1 MiB message limit (the node returns fewer lines rather than a larger message). A build the node does not have is answered with `status: error`, code `not_found`.

`inventory_resend` params: `image_digest`. The node sends that image's inventory again as `image_inventory` messages (section 5.26) and answers one `command_result` with `status: ok` once it has queued them. An image the node does not hold is answered with `status: error`, code `not_found`. The primary sends it when `status_report.images` names an inventory it does not hold, or holds with another `inventory_sha256` (ADR 0150).

`metrics_query` params: `node` (boolean) and `vm_ids` (array of `vm_` ids), at least one of them set. The answer is one `command_result` with `status: ok` and `metrics`:

| Field | Type | Meaning |
| --- | --- | --- |
| `interval_s` | integer | The node's sampling interval, 5 s |
| `node` | array of object | Present when `node` was asked: the node's samples, oldest first |
| `vms` | array of object | One `{ "vm_id", "samples" }` per asked VM the node runs; a VM it does not run is left out |

The node samples every `interval_s` into an in-memory ring of 10 minutes per node and per running VM (ADR 0014): the ring is never written to disk, never sent unless asked and lost on an agent restart. A **node sample** is `{ "time", "cpu_usage", "cpu_count", "memory_total_bytes", "memory_used_bytes" }`: `cpu_usage` is the busy fraction of all host CPUs over the interval (0 to 1, from `/proc/stat`), `memory_used_bytes` is `MemTotal - MemAvailable` (from `/proc/meminfo`). A **VM sample** is `{ "time", "cpu_usage", "vcpus", "memory_rss_bytes", "memory_bytes" }`: `cpu_usage` is the CPU time of the VM's VMM process over the interval divided by the interval and by `vcpus` (0 to 1; it can exceed 1 briefly because the VMM's own threads count), `memory_rss_bytes` is the VMM process's resident memory on the host, which counts the guest pages the guest has touched and does not shrink when the guest frees memory, and `memory_bytes` is the VM's configured memory. Both come from the engine's `counters` and the host's process accounting (`engine-trait.md` section 3.15); no guest agent is involved. The `metrics` stream sends one object per interval in the same shape, with one sample per array.

```json
{
  "v": 1,
  "type": "command_result",
  "id": "msg_01JC9ZG8N3Y5A7C9E1G3J5M7P9",
  "re": "msg_01JC9ZR6W1K6N8Q0S2T4V6X8Z0",
  "body": {
    "status": "ok",
    "metrics": {
      "interval_s": 5,
      "node": [
        { "time": "2026-11-04T10:15:30Z", "cpu_usage": 0.12, "cpu_count": 8, "memory_total_bytes": 17179869184, "memory_used_bytes": 6442450944 }
      ],
      "vms": [
        {
          "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
          "samples": [
            { "time": "2026-11-04T10:15:30Z", "cpu_usage": 0.04, "vcpus": 2, "memory_rss_bytes": 201326592, "memory_bytes": 536870912 }
          ]
        }
      ]
    }
  }
}
```

Restarts and stops are **never** commands. They are changes of desired state — `restart_generation` for a restart, `desired_status` for a stop — so they are idempotent, survive a reconnect, and cannot be lost with a dropped message (D-0011).

```json
{
  "v": 1,
  "type": "command",
  "id": "msg_01JC9ZR4T9H4K6N8Q0S2T4Z6V8",
  "body": {
    "kind": "logs_query",
    "params": {
      "vm_id": "vm_01JC9Z7C4P6R8T0V2X4Z6B8D0F",
      "since": "2026-11-02T10:00:00Z",
      "tail": 200
    }
  }
}
```

```json
{
  "v": 1,
  "type": "command",
  "id": "msg_01JC9ZR5V0J5M7P9R1T3W5Y7A9",
  "body": {
    "kind": "fence_ack",
    "params": {
      "lease_ids": ["lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D"]
    }
  }
}
```

### 5.18 `error`

**Direction:** either side.
**When:** a message could not be accepted, or the session must end.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `code` | string | yes | One of the codes below |
| `message` | string | yes | Human-readable detail |
| `fatal` | boolean | yes | Whether the session ends |
| `re` | string (`msg_`) | no | The message that caused it, in the envelope |

| Code | Fatal | Meaning |
| --- | --- | --- |
| `unsupported_version` | yes | Envelope major version not implemented |
| `unauthenticated` | yes | Credential rejected; the node stops dialing |
| `node_removed` | yes | The node was removed from the cluster (section 10) |
| `bad_signature` | no | A `desired_state` or `signing_key_rotate` signature did not verify; the message was discarded |
| `bad_message` | no | Malformed frame, unknown type, unknown command kind, oversized frame |
| `log_diverged` | no | The node's change-log copy does not match the primary's (section 7) |
| `not_found` | no | A `stream_open` or `command` named a build, VM or transcript this node does not have. Carried in the `error` of a `command_result` |
| `not_available` | no | The VM exists but cannot serve the requested stream now: a `terminal` on a stopped VM or on a VM started before the M4 guest init. Carried in the `error` of a `command_result` |
| `fence_failed` | no | A self-fence could not confirm that every stateful VM bound to the lease stopped, so a VM may still run (section 8.5). Carried in the `error` of the `command_result` that answers a `lease_revoke`; the node has marked itself failed |

A fatal error ends the session immediately; the node applies its redial rule (section 4.4). A non-fatal error leaves the session usable.

```json
{
  "v": 1,
  "type": "error",
  "id": "msg_01JC9ZS5Z0J6N8Q0S2T4Z6V8W0",
  "re": "msg_01JC9ZJ8M3Y5A7C9E1G3J5M7P9",
  "body": {
    "code": "bad_signature",
    "message": "desired_state signature does not verify against the cluster signing key",
    "fatal": false
  }
}
```

### 5.19 `ping`

**Direction:** either side.
**When:** to test liveness outside the heartbeat cadence, or to keep a connection warm through an intermediary that closes idle sockets.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| (none) | — | — | Body is `{}` |

The receiver answers `pong` with `re` set to the `ping` id. A `ping` is not a heartbeat and does not reset the primary's 15 s timer by itself; any frame does.

```json
{
  "v": 1,
  "type": "ping",
  "id": "msg_01JC9ZT6V1K8Q0S2T4Z6V8W0X2",
  "body": {}
}
```

### 5.20 `pong`

**Direction:** either side.
**When:** as the answer to a `ping`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| (none) | — | — | Body is `{}` |

```json
{
  "v": 1,
  "type": "pong",
  "id": "msg_01JC9ZZ7W2W0S2T4Z6V8W0X2Y4",
  "re": "msg_01JC9ZT6V1K8Q0S2T4Z6V8W0X2",
  "body": {}
}
```

### 5.21 `signing_key_rotate`

**Direction:** control plane to node.
**When:** when the cluster signing key is rotated (D-0050).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `key_id` | string | yes | Id of the new cluster signing key: the lowercase hex SHA-256 of its raw 32-byte public key |
| `public_key` | string | yes | Base64 Ed25519 public key of the new signing key |
| `signature` | string | yes | Base64 Ed25519 signature by the current pinned signing key |

**Signature.** As for `desired_state` (section 5.11), the signature covers the RFC 8785 serialization of the whole `body` object with the `signature` member removed. The node verifies it against the cluster public key it currently has pinned.

**Node behaviour.** On a valid signature the node persists the new public key and the `key_id` in its local store (`node_meta`, section 12), and from then on accepts only signatures by the new key. It answers with a `command_result` (`re` set to this message's `id`) meaning persisted, the same acknowledgement `desired_state` and `lease_revoke` use. A signature that does not verify is answered with `error` code `bad_signature`, and the node keeps the old pin and the old key.

```json
{
  "v": 1,
  "type": "signing_key_rotate",
  "id": "msg_01JC9ZV9X3Y0S2T4Z6V8W0X2Y4A6",
  "body": {
    "key_id": "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35",
    "public_key": "9pQ2m0Yb1s7Tq3Vx6Kd8Lw5Rc4Nh2Jf0Gz1Xe7Ua3Ik=",
    "signature": "MEUCIQDf3k2m9Qx1s7Tq3Vx6Kd8Lw5Rc4Nh2Jf0Gz1Xe7Ua3I..."
  }
}
```

### 5.22 `dns_challenge`

**Direction:** node to control plane.
**When:** while the node obtains or renews an ACME certificate for an `acme` route (D-0094). The node holds no DNS credential: the primary writes the DNS-01 TXT record on its behalf with the domain's `dns` provider (`domains.provider_id`).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `action` | string | yes | `set` to publish the TXT record, `clear` to remove it |
| `hostname` | string | yes | The route's hostname the certificate is for |
| `value` | string | `set` only | The DNS-01 key authorization digest (base64url, RFC 8555 section 8.4) |
| `record_id` | string | `clear` only | The id returned by the `dns_challenge_result` that answered the `set` |

**Authorization.** The primary accepts a `set` only for a hostname that is an `acme` route in the node's current desired state and whose domain has a `dns` provider. The record name is always `_acme-challenge.<hostname>`; the node cannot choose another name. A `clear` removes only a record the primary created for this node. Anything else is answered with `dns_challenge_result` `ok: false`.

```json
{
  "v": 1,
  "type": "dns_challenge",
  "id": "msg_01JC9ZW2A4C6E8G0J2M4P6R8T0",
  "body": {
    "action": "set",
    "hostname": "blog.example.com",
    "value": "LoqXcYV8q5ONbJQxbmR7SCTNo3tiAXDfowyjxAjEuX0"
  }
}
```

### 5.23 `dns_challenge_result`

**Direction:** control plane to node.
**When:** answers each `dns_challenge`, with `re` set to its `id`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `ok` | boolean | yes | Whether the record was written (`set`) or removed (`clear`) |
| `record_id` | string | no | Present on a successful `set`: the id to pass in the later `clear` |
| `message` | string | no | Present when `ok` is false: human-readable reason, never a credential |

A successful `set` means the provider accepted the record, not that resolvers already see it: the node checks propagation itself before it tells the ACME server the challenge is ready. A failed `set` fails that issuance attempt, which the node retries with backoff and reports as `certificate.failed`.

```json
{
  "v": 1,
  "type": "dns_challenge_result",
  "id": "msg_01JC9ZW3B5D7F9H1K3N5Q7S9V1",
  "re": "msg_01JC9ZW2A4C6E8G0J2M4P6R8T0",
  "body": { "ok": true, "record_id": "372e67954025e0ba6aaa6d586b9e0b59" }
}
```

### 5.24 `stream_input`

**Direction:** control plane to node.
**When:** the client of a `terminal` stream typed, pasted, resized its window or closed its input (ADR 0116).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | The `terminal` stream |
| `seq` | integer | yes | Per-stream input sequence, gap-free from 1 |
| `data` | string | no | Base64 bytes for the program's input, at most 64 KiB decoded |
| `resize` | object | no | `rows` and `cols` (integers, 1 to 1 000): the new window size |
| `eof` | boolean | no | True to close the program's input |

At least one of `data`, `resize` and `eof` is present; the node applies them in that order. The node writes input in `seq` order and records it in the transcript before it reaches the guest (`i` and `r` events, `audit-log-format.md` section 3.3). A `stream_input` for a `console`, `build_logs`, `metrics`, `transcript` or log stream is answered with `error` code `bad_message`, non-fatal, and dropped: a console is read-only (ADR 0116). Input for an unknown or ended stream is ignored. Input has no window of its own; the primary forwards at most one client WebSocket message per `stream_input`, and the WebSocket's own back-pressure bounds it.

```json
{
  "v": 1,
  "type": "stream_input",
  "id": "msg_01JC9ZX4C6E8G0J2M4P6R8T0V2",
  "body": {
    "stream_id": "msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9",
    "seq": 3,
    "data": "dW5hbWUgLWEK"
  }
}
```

```json
{
  "v": 1,
  "type": "stream_input",
  "id": "msg_01JC9ZX5D7F9H1K3N5Q7S9V1X3",
  "body": {
    "stream_id": "msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9",
    "seq": 4,
    "resize": { "rows": 50, "cols": 132 }
  }
}
```

### 5.25 `stream_ack`

**Direction:** control plane to node.
**When:** after the primary has handed bytes of a `data` stream (`terminal`, `console`, `transcript`) to their consumer, at the latest every 64 KiB.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `stream_id` | string (`msg_`) | yes | The stream |
| `acked_bytes` | integer | yes | Cumulative decoded bytes of the stream's `data` that the consumer has taken, from the stream's start |

Credit flow control (ADR 0116): the node keeps at most 256 KiB of decoded `data` sent on a stream and not yet covered by `acked_bytes`. At the window it stops reading the stream's source: a terminal's vsock connection, whose own credit flow control then stops the guest program, or a console's ring, which keeps filling, so a slow console viewer loses the oldest bytes it has not seen and never slows the guest. Without the window, one busy terminal could fill the session's send queue and delay heartbeats and lease messages on the same WebSocket. An `acked_bytes` lower than an earlier one is ignored; one beyond the bytes sent is a primary bug, answered with `error` code `bad_message`, non-fatal.

```json
{
  "v": 1,
  "type": "stream_ack",
  "id": "msg_01JC9ZY6E8G0J2M4P6R8T0V2X4",
  "body": { "stream_id": "msg_01JC9ZD0L3N5Q7S9V1X3Z5B7D9", "acked_bytes": 196608 }
}
```

### 5.26 `image_inventory`

**Direction:** node to control plane.
**When:** once per image the node prepares, pulled or built, after the engine has inventoried it (section 6.11); once per image the node already held without an inventory, after the engine has inventoried it from its root disk (section 6.11); again when the primary asks with `command` kind `inventory_resend`. From M5a (ADR 0150).

An inventory may hold up to 20 000 packages and 4 MiB of JSON, which is larger than one message (1 MiB, section 3). The node therefore splits it into parts: every part carries the same `image_digest`, `inventory_id` and `parts`, the metadata fields, and a slice of `packages` in order, and each part's serialized message stays below 1 MiB. The primary keeps the parts of one `inventory_id` until all `parts` have arrived, then replaces the image's previous inventory with the joined one and matches it. Parts of an `inventory_id` that are still incomplete after 10 minutes are dropped; the node's next `status_report` then shows the primary that the inventory is missing, and the primary asks again. A part that arrives twice is ignored.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `image_digest` | string | yes | Manifest digest of the image, as in `spec.vms[].image_digest` |
| `inventory_id` | string | yes | Identity of this send, the node's `msg_` id of part 1; parts of different sends never mix |
| `part` | integer | yes | 1-based number of this part |
| `parts` | integer | yes | Number of parts of this send, 1 to 8 |
| `inventory_sha256` | string | yes | Hex SHA-256 of the whole inventory (the JSON object of section 6.11), the same value `status_report.images` carries |
| `inventoried_at` | string | yes | When the engine built the inventory |
| `os` | object | no | `{ "id", "version_id" }` from the image's `/etc/os-release` or `/usr/lib/os-release`; absent when the image has neither |
| `ecosystems` | array of string | yes | OSV ecosystems the inventory covers, for example `Debian:13`, `npm`; `[]` when none |
| `not_inventoried` | array of string | yes | Kinds of content the engine saw and could not read, for example `static_binary`, `rpm_bdb`; `[]` when none |
| `package_count` | integer | yes | Packages in the whole inventory, at most 20 000 |
| `packages` | array of object | yes | This part's slice of the packages |

Each package:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `ecosystem` | string | yes | OSV ecosystem with its release where OSV uses one: `Debian:13`, `Alpine:v3.22`, `Red Hat`, `npm`, `PyPI`, `Go`, `crates.io` |
| `name` | string | yes | The installed (binary) package's name |
| `source_name` | string | no | Its source package, when the package database records one (`dpkg`'s `Source:`, `apk`'s `o:`); OSV's distribution records are keyed by it |
| `version` | string | yes | Installed version, in the ecosystem's own form |
| `purl` | string | yes | Package URL |
| `path` | string | no | For a language package, the manifest or binary it was read from, relative to the image root |

The primary acknowledges nothing: completeness is checked through `status_report.images`.

```json
{
  "v": 1,
  "type": "image_inventory",
  "id": "msg_01JCA2C4E6G8J0M2P4R6T8V0X2",
  "body": {
    "image_digest": "sha256:5f0c9a2e4b6d8f1a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a",
    "inventory_id": "msg_01JCA2C4E6G8J0M2P4R6T8V0X2",
    "part": 1,
    "parts": 1,
    "inventory_sha256": "9b2f6c1d8e4a7b3c5d9e1f2a4b6c8d0e2f4a6b8c0d2e4f6a8b0c2d4e6f8a0b2c",
    "inventoried_at": "2026-11-05T06:09:41Z",
    "os": { "id": "debian", "version_id": "13" },
    "ecosystems": ["Debian:13", "npm"],
    "not_inventoried": [],
    "package_count": 2,
    "packages": [
      {
        "ecosystem": "Debian:13",
        "name": "libssl3t64",
        "source_name": "openssl",
        "version": "3.5.1-1",
        "purl": "pkg:deb/debian/libssl3t64@3.5.1-1?arch=amd64&distro=debian-13"
      },
      {
        "ecosystem": "npm",
        "name": "express",
        "version": "4.21.2",
        "purl": "pkg:npm/express@4.21.2",
        "path": "app/package-lock.json"
      }
    ]
  }
}
```

## 6. Reconciliation

### 6.1 Source of truth on the node

The node always acts on the newest verified snapshot in its local store. It never acts on a message that is still in memory: `desired_state` is persisted before `command_result` is sent, so a crash between receipt and apply cannot lose the revision.

- `rev` only increases. A `desired_state` with a `rev` lower than or equal to the stored one is acknowledged with `command_result` status `ok` and ignored.
- An older revision arriving after a newer one is a primary bug; the node keeps the newer snapshot and reports it.

### 6.2 Apply loop order

An apply run always follows this order, whatever changed:

1. **Network.** Ensure the mesh interface, its peers and its `allow` rules from `spec.mesh` (section 6.9), then the bridge, subnet and gateway from `spec.network`, and the per-VM TAP devices. Each VM is attached with its `project_id` as its isolation group, so the VMs of one project reach each other and VMs of different projects do not (`engine-trait.md` section 3.3, D-0084). The DNS servers handed to guests come from the node's own configuration, not from `spec.network`.
2. **Images.** Prepare every image referenced by a VM whose rootfs is missing or has a different digest, and remove rootfs images no longer referenced. An image whose VM carries `build_id` is never pulled: it was prepared by the build's `importing` step (section 6.6); if it is gone, the deployment fails in phase `pull` with the error that the built image is no longer on this node. A root disk is referenced while a VM in `spec.vms` or a build in `spec.builds` names it.
3. **Stop.** Stop VMs that are no longer desired, or whose spec requires a recreate (image digest, disk size, network identity, attached volumes).
4. **Volumes, then start.** Create the file of every `present` volume that has none, or write it from its `restore`, and remove the file of every `deleted` volume that no VM uses any more (section 6.7). Then start desired VMs that are not running. A VM whose volume is not `ready` on this node stays `stopped`; volume work never delays a VM that attaches no volume.
5. **Builds and backups.** Start the next `queued` build of `spec.builds` when the node has no build running (one at a time per node by default, ADR 0118), stop a build marked `cancelled`, and report every change in `status_report.builds` (section 6.6). Builds run alongside the other steps, never block them, and are not restarted by a later apply run. Start every `spec.backups` job that is not running (section 6.7); backups also run alongside, one per volume.
6. **Routes.** Update the reverse proxy for the hostnames in `spec.routes` and, on an edge, `spec.edge.routes` (section 6.8), and start, replace or stop the tunnel connectors to match `spec.ingress.tunnel` and `spec.edge.tunnel`. A route with `draining_until` is applied like any other: the node serves exactly the routes of the snapshot and drops one only when a snapshot no longer carries it (section 5.11). Certificates for `acme` routes are obtained in the background through `dns_challenge` (section 5.22); a route keeps serving its previous certificate until a new one is issued.

The order exists so that a VM never starts on a network that is not ready, and no route points at a VM that is being replaced. It is the reconciliation described in D-0011 and the deployments in D-0033.

### 6.3 Restart, crash and health

- A VM that exits without being asked to stop is `crashed`; the node restarts it with backoff from 1 s doubling to 5 min. The backoff resets after the VM has been healthy for 10 minutes. `restart_generation` in the spec is what a human-initiated restart changes; the crash path does not increment it.
- Health checks use the app's `health_check` (`path` default `/`, `interval_s` default 10, `timeout_s` default 2, `failures` default 3, `start_period_s` default 30, `restart` default true). Failed probes within `start_period_s` after the VM started do not count. After `failures` consecutive failures the node reports the app as `failing` and emits `app.down`; on recovery it reports `passing` and emits `app.up`.
- From M5b a VM that stays unhealthy is restarted (ADR 0184). With `restart` true, after `failures` consecutive failures outside the start period the node, besides reporting `failing` and emitting `app.down`, stops the VM and starts it again in place, with the crash backoff above (1 s doubling to 5 min, reset after 10 minutes healthy). The start period begins again at each start. A stateful VM is started again only under the rule of section 6.4: its node holds the app's lease in state `held` with the matching token; otherwise it stays stopped, as for any stateful start. A health restart is not a human restart: it does not change `restart_generation`. With `restart` false the node only reports, as before M5b. A snapshot without `restart` means true.
- Image preparation is cached by digest. Two VMs with the same digest share one prepared rootfs; the digest, not the tag, is the key.
- An agent restart does not restart VMs. VMMs are separate processes that outlive the agent, and the agent rediscovers them on start (`engine-trait.md`, D-0011). This is what makes an agent upgrade invisible to running apps.

### 6.4 Stateful starts

A stateful VM starts only while the node holds that app's lease in state `held` **and** the lease's `fencing_token` equals the `fencing_token` in the VM spec. If either condition fails the VM stays `stopped`, the fact is recorded locally, and the node reports the VM as `stopped` with `health: unknown`; it does not report an error to the primary, because the primary is the one that moved the lease (D-0019). Because renewals keep the token (D-0073), the equality holds for the whole time one holder owns the lease; it changes only when ownership changes, and then the primary issues a new `desired_state` carrying the new token. A node that marked itself failed (section 8.5) starts no stateful VM at all. In M5a a VM is stateful exactly when it attaches a volume (ADR 0145), and its volume must be `ready` on this node.

### 6.5 While disconnected

Everything continues: running VMs keep running, restarts still happen, health checks still run, and the node keeps reconciling against its stored snapshot. Events are buffered in the local store, bounded at 10 000 entries. On overflow the oldest entries are dropped, a counter is incremented, and the next `hello` reports it as `dropped_events` so that a primary never assumes silence means "nothing happened".

### 6.6 Builds

A build runs on the node in an ephemeral builder microVM, one per build, created through the engine like any VM (ADR 0117, ADR 0118):

- **Builder VM.** Root disk: the BuildKit image pinned by digest in the node agent's build (`pins.txt`, like Cloud Hypervisor and the kernel) and prepared with `prepare_image`; it runs the rootful, daemonless `buildkitd` pattern with `--oci-worker-net=host`. Size: `limits.vcpus` and `limits.memory_mb`. Network: on the node bridge with no isolation group and `egress_only` (`engine-trait.md` section 3.3), so it reaches the outside through the gateway and no VM and no service on the host. The builder VM counts against the node's capacity like any VM; a build whose limits do not fit waits in `queued`.
- **Disks.** The builder VM gets three work disks (`engine-trait.md` section 3.6), found in the guest by serial:
  - `quoro-work`: one persistent ext4 cache disk per app on this node, at most `limits.cache_gb` gibibytes, created by the engine on the app's first build. It holds the BuildKit state and the extracted build context. Only the builder VM ever mounts it; the node never mounts, reads or writes inside it. After each build the builder runs `buildctl prune --keep-storage` down to below the cap. The node deletes the file once the app appears in neither `spec.vms` nor `spec.builds`.
  - `quoro-src`: an ephemeral raw disk with no filesystem, written by the node before boot: one tar stream holding the source tree at the resolved commit and the build's secret files. The guest extracts it and never writes to it.
  - `quoro-out`: an ephemeral raw disk with no filesystem of `limits.output_gb` gibibytes, sparse and zeroed by the node. The builder writes the OCI layout to it as one tar stream; the node reads it back as a tarball in user space.

  The node deletes both ephemeral disks when the build ends. A disk the builder VM wrote is never mounted on the host: a guest-written filesystem would be parsed by the host kernel, and the guest is untrusted.
- **Phases.** `preparing`: the node resolves `ref` to a commit with the host's Git client, fetching only that commit (shallow, public HTTPS only in M4, no submodules) into a bare repository, so no working tree of the repository ever exists on the host; writes the tree of that commit and the secret files to `quoro-src`; and boots the builder VM. `building`: `buildctl build` with the Dockerfile frontend, `--progress=rawjson`, `build_args` as `--build-arg` and `secrets` as `--secret` (extracted into guest memory, never onto `quoro-work`). `exporting`: `--output type=oci` onto `quoro-out`, at most `limits.output_gb`. `importing`: after the builder VM powered off, the node finds the manifest digest in the layout's `index.json`, verifies the layout and prepares it as a root disk with `prepare_layout` (`engine-trait.md` section 3.17), cached by manifest digest, then reports `succeeded` with `image_digest`.
- **Failure.** A step that fails ends the build `failed` in phase `building`, with `failed_step` from BuildKit's rawjson output (the failing vertex, its name and error, the Dockerfile line) and the excerpt from the plain log. The wall-clock limit `limits.timeout_s` stops the builder VM and fails the build in the phase it was in, with the error `timeout`. A failed checkout fails `preparing`. A node agent restart while a build runs fails the build in its current phase with the error `node agent restarted`; builds are not resumed.
- **Cancel.** `cancelled: true` stops the builder VM with a forced stop and reports `cancelled`.
- **Log.** The builder VM's tagged output lines (the same path as app logs, `engine-trait.md` section 6) are parsed: rawjson objects become build events and plain lines are appended to the build's log next to its work directory, numbered from 1 (ADR 0119). The log is served by `build_logs` streams and `build_logs_query`, capped at 8 MiB per build (later lines are counted and dropped with one marker line), and kept for the last 20 builds of each app on this node and until the app's work disk is deleted. It is never sent unasked and never replicated; only the status and `failed_step` reach the primary (ADR 0014).

**Defaults.** Until a contract change makes them configurable, the primary sends `limits` `{ "vcpus": 2, "memory_mb": 2048, "timeout_s": 1800, "cache_gb": 10, "output_gb": 10 }`.

```json
{
  "build_id": "bld_01JC9ZA2B3C4D5E6F7G8H9J0KM",
  "app_id": "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E",
  "project_id": "prj_01JC9Z5A1B2C3D4E5F6G7H8J9K",
  "repo_url": "https://github.com/example/blog.git",
  "ref": "main",
  "dockerfile_path": "Dockerfile",
  "context_dir": ".",
  "build_args": { "NODE_ENV": "production" },
  "secrets": {},
  "limits": { "vcpus": 2, "memory_mb": 2048, "timeout_s": 1800, "cache_gb": 10, "output_gb": 10 },
  "cancelled": false
}
```

### 6.7 Volumes and backups

A volume is a sparse raw file in the node's data directory, named by its `volume_id`, holding an ext4 filesystem the engine creates empty (`engine-trait.md` section 3.6, ADR 0145). The host never mounts it: a guest has written it, so it is untrusted input to the host kernel.

- **Create.** A `present` entry with no file and no `restore`: the node creates the file through the engine (`ensure_volume`) and reports `creating`, then `ready`. A size larger than the node's free disk fails the volume with the reason.
- **Restore.** A `present` entry with `restore`: the node reports `restoring`, then writes the file from the backup as `backup-format.md` section 8.1 describes, and reports `ready`, or `failed` with the reason after deleting the partial file. A restore is not resumed after an agent restart: the node deletes the partial file and starts it again.
- **Delete.** A `deleted` entry: once no VM of `spec.vms` attaches the volume and no running VM uses it, the node removes the file (`delete_volume`) and reports `deleted`. The primary sends one for a volume deleted on its own and, from M5b, for every volume of a deleted app, whose record it keeps in `deleting` until this report arrives (ADR 0165).
- **Never by absence.** The node never removes a volume file because `spec.volumes` does not name it. Such a file is reported `orphaned` and kept until a `deleted` entry names it or someone with root on the node removes it, as section 10 does for a removed node.
- **Backup.** For each job of `spec.backups`, the node first checks that it holds the app's lease in state `held` with the job's `fencing_token`; otherwise it reports the backup `failed` with `lease_mismatch`. It then takes the snapshot with its method (`engine-trait.md` section 3.16), pausing `vm_id` for `pause_copy`, reports `running`, chunks and uploads the snapshot, writes the manifest and deletes the snapshot file (`backup-format.md` section 7.1). It reports `succeeded` with the manifest's key and digest, or `failed` with the reason. A job leaves `spec.backups` once the primary has recorded its terminal status; an agent restart while a backup runs fails it with `node agent restarted`, and the snapshot file is deleted at start.
- **Writes carry the token.** Every object a backup job writes is part of a backup that the primary seals only if the job's token is still the app's current token (`backup-format.md` section 6.2). A node that lost its lease while uploading therefore wastes storage, never a valid backup (section 8.6).

### 6.8 The edge

A machine with the `edge` role runs the embedded proxy for `spec.edge.routes` (ADR 0143). It needs no engine; on an edge-only machine the apply loop runs only steps 1 and 6.

- **Health.** For each backend the edge probes `health_check.path` over the mesh every `interval_s`, with `timeout_s`. A backend leaves the rotation after `failures` consecutive failed probes and returns after the same number of consecutive passes. A request that cannot connect, or whose connection is reset before a response, takes the backend out at once with reason `connect_failed`; it returns through the probes. A backend the edge has not probed yet starts in rotation.
- **Balancing.** Round robin over the backends in rotation, with no stickiness. A `GET` or `HEAD` request that fails to connect, or whose connection is reset before response headers arrive, is retried once on another backend in rotation; no other request is ever sent twice. With no backend in rotation the edge answers `502`.
- **Leaving the rotation.** When a backend leaves the rotation for any reason in this section, the edge drops its pooled connections to it and cancels the requests still in flight to it. A cancelled `GET` or `HEAD` that has no response headers yet, and whose request body can be sent again, is retried once on another backend in rotation; any other cancelled request answers `502` at once; a response that is already streaming to the client is cut and never retried. A backend that only leaves `spec.edge.routes` (for example during a deploy) is not cancelled: its requests finish normally.
- **Timeouts.** A connect timeout of 5 s and a response-header timeout of 60 s per request, so a backend that accepts and hangs cannot hold a connection forever. Backend connections use TCP keepalive and, on Linux, `TCP_USER_TIMEOUT`, so a connection to a backend that disappeared fails within seconds instead of waiting for the response-header timeout.
- **Without the primary.** The edge keeps its last verified desired state on disk like a node (section 6.1, ADR 0011) and keeps probing and balancing. Its health decisions are its own: they change no replicated state, and they reach the primary only as `status_report.edge_backends`.
- **TLS.** `acme` routes take certificates through `dns_challenge` (section 5.22), exactly as on a node; `tunnel` routes are served from the edge's tunnel.
- **Draining routes.** A route with `draining_until` (section 5.11, ADR 0168) is balanced, probed and served like any other. It names the hostname of an app that went back to one replica and is now also served by that node's tunnel; the edge keeps it until a snapshot no longer carries it. A backend that leaves a draining route because the route itself left `spec.edge.routes` finishes its requests normally, as in "Leaving the rotation" above.

### 6.9 The mesh

The mesh is kernel WireGuard driven over netlink, with no `wg` or `wg-quick` process (ADR 0063, ADR 0144). In M5a it is a star: the edge has one peer per node, and every node has the edge as its only peer.

- **Keys.** The machine generates its WireGuard key pair on its first M5a start, keeps the private key root-only next to its credential, and sends the public key in `hello`. A machine never receives another machine's private key.
- **Interface.** The node agent creates the WireGuard interface `quoro-wg0` with `address`, `listen_port` and `peers`, and keeps it equal to them on every apply. A peer that disappears from `peers` is removed. On the edge, the kernel routes each node's guest subnet to that node's peer through `allowed_ips`.
- **Firewall.** On a node, traffic that arrives on the mesh interface is dropped unless it comes from an `allow` entry's `source` and goes to its `address` and `port` over TCP. The mesh carries no traffic between guests and none to the host's own services; the isolation of guests from each other (ADR 0084) is unchanged.
- **NAT.** A machine behind NAT has no `endpoint` in the other side's peer entry; it dials the peer that has one and keeps the tunnel open with `persistent_keepalive_s`. WireGuard follows a peer whose address changed (roaming), which also covers a node whose address changed after a reboot.
- **A changed subnet.** When `spec.network.subnet` differs from the subnet of the node's bridge, the node does not reconfigure that bridge (`engine-trait.md` section 3.3 refuses it with `Conflict`). It ensures a second bridge with the new subnet and gateway, keeps its running VMs on the old bridge with their old addresses, and creates every new or redeployed VM on the new one. The old bridge stays, without VMs, until the host restarts: the engine has no call that removes a network in M5a. VMs that only restart in place keep their address; a redeploy moves them (ADR 0144, like ADR 0136).

### 6.10 Instance identity

When it creates an app instance, the node adds two variables to its environment, after the app's own `env`: `QUORO_INSTANCE_ID` (the VM's `vm_` id) and `QUORO_NODE_ID` (the node's `nod_` id). The names are reserved: the API refuses them in an app's environment, and if a snapshot from an older primary carries either one, the node's value wins. They are not secrets and may appear in logs.

### 6.11 Image inventories

From M5a every image the engine prepares is inventoried in the same pass that unpacks its layers, before the root disk is built (`engine-trait.md` section 3.4, ADR 0151). An image the node already held from an earlier release has no inventory; the engine inventories it once from its root disk, read-only, after the node agent starts (`engine-trait.md` section 3.21). No image leaves the node, and no separate scanner runs.

- **What is read.** `os-release`; the `dpkg` database (`/var/lib/dpkg/status` and `status.d/`), the `apk` database (`/lib/apk/db/installed`) and the `rpm` SQLite database; npm `package-lock.json`, Python `*.dist-info/METADATA`, the build information of Go binaries and the `cargo auditable` section of Rust binaries. Anything the engine sees and cannot read (a static binary without build information, an `rpm` Berkeley DB) is listed in `not_inventoried`, so the panel never shows "no vulnerabilities" for something nobody read.
- **Source names.** Distribution packages carry their source package in `source_name`: OSV's Debian and Alpine records are keyed by it, and a match on the binary name finds nothing.
- **Bounds.** At most 20 000 packages and 4 MiB of JSON. A larger inventory is cut at the bound and `not_inventoried` gains `truncated`.
- **Storage on the node.** The inventory is kept next to the prepared image in the engine's cache and removed with it. Its JSON object is the fields of section 5.26 other than `inventory_id`, `part` and `parts`, with all packages, serialized with RFC 8785; `inventory_sha256` is the SHA-256 of those bytes.
- **Sending.** After it prepares an image, the node sends the inventory once (section 5.26) and lists the image in `status_report.images`. It sends it again only on `inventory_resend`.
- **Images held before M5a.** Once after it starts, in the background, the node agent asks the engine for every cached image without an inventory and has the engine inventory them one at a time from their root disks (`engine-trait.md` section 3.21). Each inventory it gets is sent exactly as for a freshly prepared image: once (section 5.26), and listed in `status_report.images` with its `inventory_sha256`. Until then the image is listed without `inventory_sha256`, as one whose inventory is still being built. The pass needs no network and no deployment, covers pulled and built images alike, and never delays the node's start or its VMs. An image the engine cannot inventory is logged and stays without an inventory until the next start of the node agent tries again; the other images go on.
- **On the primary.** The primary keeps the inventories it received, and its copy of the OSV data, in a cache file of its own next to its database, not in the database: the file is large, can always be fetched again, and is left out of control-plane backups (ADR 0148). Only the data age (`osv_mirror`) and the findings (`vulnerability_findings`) are in the database (`migrations/0008-vulnerabilities.sql`). The primary fetches an OSV ecosystem the first time an inventory needs it, never OSV's whole-bucket archive, refreshes it daily from that ecosystem's change list, and can be fed offline with `quoro vuln import <dir>`. It matches on the package's (or source package's) name and the ecosystem's own version order, and records a finding only when it appears, changes or is resolved.

## 7. Change-log follow

Every change to cluster state is a numbered entry in an append-only, hash-chained log (D-0013, D-0015). Replica nodes follow it so that a manual promotion (D-0016) needs no protocol change: a node that already holds a current copy can serve reads and forward writes.

- **Which nodes follow:** every node with the role `node`, from M2 on. Nothing in this protocol distinguishes a future primary from any other follower, which is what keeps promotion cheap. When promotion happens (manual promotion in M5b, D-0067) the promoted node stops being a follower and starts issuing `desired_state` for its own nodes.
- **Delivery:** `log_entries` with `from_seq` and up to 500 entries. The node verifies each entry's `hash` over the RFC 8785 serialization of the entry without `hash`, and that `prev_hash` equals the previous entry's `hash`, then stores it and sends `log_ack`.
- **On `hello`:** the primary compares the node's `log_seq` and `log_hash` with its own hash at that sequence. If they differ, the primary sends `error` with code `log_diverged`, non-fatal. The node moves its local copy aside as a quarantined copy — never deleting it, because divergent local writes must never be silently dropped (D-0022) — and refollows from `seq` 1.
- **Secrets in the log:** `app_env` values, webhook secrets and provider secrets are stored encrypted with the cluster data-encryption key (AEAD, `db-schema.sql`), so a follower holds ciphertext it cannot read. A promoted replica obtains the cluster data-encryption key, and the signing key, from seals made for it: every machine with the `node` role generates a custody key pair and the primary seals both cluster keys, and its own TLS key, to that machine's public custody key (ADR 0078, read by ADR 0180 and ADR 0181). Edge-only and witness-only machines get no seal. The fields and the table that carry the seals are specified with manual promotion in M5b (D-0067); until then the keys do not leave the primary.

## 8. Leases and self-fencing

This section is the core of the contract. It exists because starting a stateful app on node B while it is still running on node A produces two diverging copies of the same data (D-0019).

The lease modes are decided (D-0047): `local` for HA policy `none` and `cold`, `expiring` (M5b and later) for `warm` and `hot`. The node-side states `held`, `uncertain`, `expired` and `revoked` are fixed.

### 8.1 Who arbitrates

| Period | Lease authority | Consequence |
| --- | --- | --- |
| M0 to M5a | The primary, in process | One control plane; a `local` lease survives its absence (D-0011) |
| M5b and later | The primary backed by a quorum with the witness, or an external arbiter | Same messages; the authority behind them is stronger (D-0017) |

The external arbiter options are an S3/R2 conditional write or a Cloudflare Durable Object. Both are researched and both require the fencing token to stay (F-0037, F-0038, `docs/research/r2-conditional-writes.md`, `docs/research/durable-objects.md`): object storage has no server-side expiry, and a Durable Object restart can kill an in-flight write, so "the lease expired" is always inferred and always enforced by the node and by the token.

The same mechanism also serves primary election (D-0019). Leases with `resource_type: primary` appear when automatic failover exists (M6); in M0-M5 the only leases are `resource_type: app`.

### 8.2 Messages

| Message | Direction | Purpose |
| --- | --- | --- |
| `lease_grant` | control plane to node | Grant a lease, with mode, token and duration; also the answer to `lease_renew` |
| `lease_renew` | node to control plane | Ask to extend an `expiring` lease every `duration_ms / 3` |
| `lease_revoke` | control plane to node | End a lease: `moved`, `deleted`, `declared_lost`, `superseded` |

### 8.3 `local` mode

Issued for the HA policies where no other copy of the app can start (`none`, `cold`, D-0047), so nobody is waiting to take the lease.

- It never expires because the session is gone or the control plane is unreachable. This is what keeps D-0011 true: an app keeps running when the panel is down.
- It ends only by:
  - `lease_revoke` with reason `moved`, `deleted`, `superseded`; or
  - an operator declaring the node lost (part of the `local` lease mode, D-0047), which is a manual action from M5a and needs an endpoint that a later contract pull request adds. Declaring a node lost increments the lease's `fencing_token` (D-0019), so the old holder's token is stale even though the old holder never received a message.
- A `local` lease on a node whose session is gone is a normal state, not an incident. The primary reports the node `disconnected`, not `fenced`.
- **Granted at placement (M5a).** The primary grants a `local` lease on an app to the node that holds its volume when the volume is created, and records it in `leases`. A restore revokes the lease on the old node with reason `moved` and, once the new volume is `ready`, grants a new one to the target node with a strictly higher token (ADR 0147). A restore is refused while the old node is disconnected: an operator removes that node first (section 10), which increments the token.
- **Released at deletion.** Deleting the app's attached volume, or the app, revokes the lease with reason `deleted`.

### 8.4 `expiring` mode timing

Used for `warm` and `hot`, where a second copy is ready and will start as soon as the lease moves (D-0047).

Definitions, all on the node's monotonic clock:

| Symbol | Value |
| --- | --- |
| `D` | `duration_ms` from the grant |
| `M` | `margin = max(1000 ms, D / 10)` |
| `t_send` | Node monotonic time at which it sent the `lease_renew` that the current grant answers. A grant that answers no outstanding renewal has no `t_send` and never sets or extends `valid_until`. |
| Renewal interval | `D / 3`; a renewal left unanswered for one interval is late |
| `valid_until` | `t_send + D - M` |
| Fence deadline | `valid_until + M = t_send + D` |
| `T_grace` | Graceful-stop timeout used when fencing |
| `T_force` | Upper bound for the forced stop that follows |

```
valid_until = t_send + D - M
M           = max(1000, D / 10)
```

The authority never grants a `duration_ms` below 3 000 ms: with the floor margin of 1 000 ms that keeps `valid_until` at least 2 000 ms after the renewal, so the node has a real window to renew before it must fence. A grant below that floor is refused with `error` code `bad_message`, non-fatal.

**First grant.** An `expiring` grant that does not answer an outstanding renewal of the node — the first grant of a lease to this node, a grant after `expired` or `revoked`, or a higher-token grant while the node already holds the lease — records the token, leaves the lease (or puts it) in `uncertain`, and makes the node send a `lease_renew` at once. The lease becomes `held` only when the authority answers that renewal with the same token; `valid_until` is then computed from that renewal's send time. Until then the node starts nothing stateful for it, and an existing `valid_until` (a higher-token grant while held) is kept, never extended. This removes any assumption about how long the grant took to arrive (D-0072). A `local` grant needs no renewal and is `held` on receipt.

**Node state machine.**

| From | Trigger | To | Action |
| --- | --- | --- | --- |
| — , `held`, `expired`, `revoked` | `lease_grant`, `local` mode, token higher than any seen | `held` | Record the token |
| — , `expired`, `revoked` | `lease_grant`, `expiring` mode, token higher than any seen | `uncertain` | Record the token, send `lease_renew` at once |
| `held`, `uncertain` | `expiring` `lease_grant` with a higher token that answers no renewal (ownership changed) | `uncertain` | Record the token, keep `valid_until`, send `lease_renew` at once |
| `uncertain` | `lease_grant` with the same token answering an outstanding renewal | `held` | Compute `valid_until` from that renewal's `t_send` |
| `held` | a renewal is answered with the same token | `held` | Recompute `valid_until` from the new `t_send` |
| `held` | a renewal is late, or the session is lost | `uncertain` | Keep running; start nothing new stateful |
| `held`, `uncertain` | monotonic clock reaches `valid_until` | `expired` | Self-fence (section 8.5) |
| any | `lease_revoke` with a token at least the current one | `revoked` | Self-fence, answer `command_result` |

A `local` grant repeated with the same token while `held` changes nothing.

`uncertain` is deliberate. It is "the arbiter is unreachable", not "the lease is lost": stopping every stateful app during a control-plane or arbiter outage would break D-0011, while starting a new stateful VM in that window could produce the double-run D-0019 forbids. The distinction between the two situations is exactly what the lease research asks for (`docs/research/r2-conditional-writes.md`, `docs/research/durable-objects.md`).

**Why the node finishes fencing before another holder may start.** Let `T_grant` be the authority's send time of the grant the node is currently holding. The authority grants the same lease to another holder no earlier than:

```
T_gate = T_grant + D + 2M
```

The node starts fencing at `valid_until = t_send + D - M` and must be done by:

```
F = valid_until + M = t_send + D
T_grace + T_force <= M
```

`T_grant >= t_send` (the grant is sent after the renewal it answers arrived; a held lease always has a `t_send`, because only an answered renewal makes a lease `held`), so on a shared time base:

```
F - T_gate <= (t_send + D) - (t_send + D + 2M) = -2M
```

The fence therefore completes at least `2M` before the gate opens. The two clocks are not shared, so let `δ` be the bound on their drift and offset over the lease lifetime: the fence completion, expressed on the authority's clock, is at most `t_send + D + δ`, and the gate is at least `t_send + D + 2M - δ`. The remaining gap is `2M - 2δ`, which is strictly positive whenever `δ < M`. The margin `M` must therefore be larger than the bounded drift and the messaging delay between the machines; that the drift is bounded by such a value is a **Hypothesis** to be measured on the lab, and a node whose clock steps backwards or forwards by more than `M` must be treated as a failed node.

Worked example with `D = 15000`:

| Quantity | Value |
| --- | --- |
| `M` | 1 500 ms |
| `valid_until` | `t_send + 13 500 ms` |
| Fence deadline `F` | `t_send + 15 000 ms` |
| Gate `T_gate` | not before `t_send + 18 000 ms` |
| Chosen `T_grace` / `T_force` | 1 000 ms / 300 ms, sum 1 300 ms ≤ `M` |
| Actual fence completion, worst case | `t_send + 14 800 ms` |
| Slack before the gate | at least 3 200 ms, minus 2δ |

**What the authority must enforce.** The gate is the authority's obligation, not the node's: the primary (or the arbiter) records the send time of the last grant to a holder and refuses to grant the same lease to anyone else before `D + 2M` have passed on its own clock. An authority that grants earlier can hand the same app to two nodes. That obligation is why an arbiter with a real single-writer critical section (a Durable Object, or a conditional write on strongly consistent storage) matters at M5b (`docs/research/durable-objects.md`, `docs/research/r2-conditional-writes.md`).

**Agent restart.** `expiring` leases that were `held` or `uncertain` survive an agent restart within the same host boot. The node persists, for each such lease, the remaining validity measured on `CLOCK_BOOTTIME` together with the host's kernel boot id (`/proc/sys/kernel/random/boot_id`, stored as `host_boot_id` in `node_meta`, section 12; not the per-process `boot_id` of `hello`). On restart, if the kernel boot id is unchanged and the remaining validity has not run out on `CLOCK_BOOTTIME`, the lease comes back `uncertain` with that remaining `valid_until` and renews at once; otherwise it comes back `expired` and is fenced. `CLOCK_BOOTTIME` keeps counting while the machine is suspended, so a suspend can only shorten the remaining validity. The VMMs outlive the agent (section 6.3), so an agent upgrade does not stop warm apps. Required before `expiring` leases ship in M5b (D-0072); M0 restores such leases as `expired` and fences them, which is the conservative behaviour.

### 8.5 Self-fence procedure

When a lease reaches `expired` or `revoked`, the node:

1. Stops every stateful VM bound to that lease with `StopMode::Graceful { timeout: T_grace }`, then with `StopMode::Force` if the timeout passes, so the whole stop finishes within `T_grace + T_force <= M` (`engine-trait.md`). The forced stop escalates to `SIGKILL` on the VMM process inside the engine (`engine-trait.md` section 3.8).
2. Records the stop locally: lease id, token, VM ids, time and the reason (`expired` or `revoked`), so the fact survives an agent restart.
3. Emits `node.fenced` with the lease, the token and the VM ids — only when every bound VM is confirmed stopped or gone. If the session is down the event is buffered (section 6.5) and delivered later.
4. Starts nothing stateful for that resource until a new `lease_grant` arrives with a higher `fencing_token`.

The node also emits `node.fenced` when an operator declaration invalidated its token without a message reaching it — it learns that when the primary refuses its next `lease_renew` or when a `hello` is answered with a `lease_revoke`.

**Failed fence.** If a forced stop fails or times out and the node cannot confirm the VM stopped (the engine reports it neither stopped nor gone), the VM may still be running. The node then never emits `node.fenced` for that fence; it records the failed fence locally, emits `node.fence_failed` with the lease, the apps and the VM ids that may still run, marks itself failed in its local store (`node_meta`, section 12) so the mark survives an agent restart, starts no stateful VM for any lease while failed, and answers a `lease_revoke` with `command_result` status `error` and error code `fence_failed` (section 5.18). The lease stays unstartable on this node. The primary treats `node.fence_failed` like the loss of the node: it does not grant the lease to anyone else until an operator acknowledges the fence, as when declaring a node lost (section 8.3) (D-0074).

**Acknowledging a failed fence** (D-0079). Only a human clears a failed fence, after checking out of band that the VMs it names are stopped (for example by power-cycling the host):

1. When the primary receives `node.fence_failed`, or a `lease_revoke` answered with `fence_failed`, it records the failure on the node: the lease, app and VM ids are added to the node's `fence_failure` (`db-schema.sql` migration 0001; `Node.fence_failure` in `openapi.yaml`). While a lease is listed there, the primary grants it to no node.
2. An operator calls `POST /api/v1/nodes/{node_id}/fence-ack` with the lease ids they verified. The scope `node:fence_ack` is human-only, so no agent can clear a fence, and the call is audited (D-0031). The list must equal the node's outstanding `fence_failure` lease ids, so an acknowledgement cannot cover a failure the operator has not seen.
3. In the same transaction the primary increments the `fencing_token` of each listed lease, exactly as when declaring a node lost (section 8.3), clears the node's `fence_failure`, and records the acknowledgement as pending delivery (`fence_ack_pending`). From then on the leases may be granted again, to this node or another, with the new token.
4. The primary sends `command` kind `fence_ack` with those lease ids (section 5.17): at once when the node is connected, otherwise at its next session after the stale-lease revokes and before `desired_state` (section 4.2). A `command_result` with `status: ok` clears `fence_ack_pending`; until then the primary sends it again at each session start.
5. The node clears the listed failed fences and, when none remains, its failed mark. A node that never receives the command stays failed, which is the safe side: it starts nothing stateful.

**Concurrency.** Fences of different leases must run concurrently, and an agent crash in the middle of a fence must leave the lease fenced on restart; both are covered by mandatory tests before `expiring` leases ship (D-0075). M0 fences one lease at a time, which is acceptable only because no `expiring` lease exists outside tests.

### 8.6 Fencing tokens

- A token is an integer per leased resource that increases strictly on every ownership change — a grant to a new holder, or a grant after expiry or revoke — and stays the same across renewals by the same holder (D-0073). It is never reused. The current token lives on the lease row (`fencing_token` in the shared entity model) and is echoed in every `lease_grant`, `lease_revoke`, `lease_renew`, `hello` and stateful VM spec.
- Every write to shared storage carries the token: the chunk store and object store from M5, and the replication stream (D-0023). A receiver rejects any write whose token is lower than the highest it has seen for that resource, and records the rejection. For the backups of M5a the receiver is the primary, not the bucket: the bucket accepts any chunk or manifest the credential may write, and the primary seals a backup only when its token is the app's current one (`backup-format.md` section 6.2).
- Tokens are what makes the lease safe against a paused node: a node that was frozen past its expiry can still act on stale intent, and only the token at the storage layer stops it (`docs/research/r2-conditional-writes.md`).
- Arbiters do not remove the need for tokens. A Durable Object can bump a token inside its critical section, and R2 has no server-side expiry at all, but in both cases the node and the storage must still enforce the token (`docs/research/durable-objects.md`).

### 8.7 What exists in M0, and the milestone table

M0 ships the protocol, not the HA feature: the messages, the four node-side states, the self-fence code path, and tests — revoking a `local` lease, expiring a test-only short `expiring` lease, and a failed forced stop that ends in `node.fence_failed` instead of `node.fenced`, among others. They run against the in-process primary and the `FakeEngine` (`engine-trait.md`), so the path is exercised on machines without KVM. This is the "self-fencing from M0" requirement of D-0019.

| Milestone | Authority | Modes in use | What the node does |
| --- | --- | --- | --- |
| M0 | In-process primary | `local` (message-complete), `expiring` only in tests | Runs the state machine and the self-fence path against the fake engine |
| M1 | Primary | `local` | Single node; the engine lifecycle runs, no stateful app exists yet |
| M2 | Primary | `local` | Enrolls, follows the change log, reconciles desired state, emits events |
| M3 | Primary | `local` | Same; the agent-driven calls arrive as desired-state changes from the API and MCP |
| M4 | Primary | `local` | Adds builds in desired state, build-log, metrics, terminal, console and transcript streams, `stream_input` and `stream_ack`; leases unchanged |
| M5a | Primary | `local` | Local volumes, with a `local` lease granted at placement; backup jobs carrying the lease token; restores that move the lease to the target node (section 6.7); operator declaration of a lost node adds a lease revoke with a new token. Replicas take no lease: they are stateless. The edge and the mesh (sections 6.8 and 6.9) hold no lease either |
| M5b | Primary plus witness quorum or external arbiter | `local`, `expiring` (`warm`) | Warm: renewal loop, `uncertain` handling, replication writes carrying tokens, restart persistence of held leases (section 8.4), concurrent fences (section 8.5) |
| M6 | Quorum | `local`, `expiring` (`warm`, `hot`) | Automatic failover and `hot`; the same messages also carry `resource_type: primary` leases |

## 9. Node return

A node that comes back after a failure may hold writes nobody else has and may be missing everything that happened while it was gone, which is why failback is never automatic (D-0022).

Order on reconnect:

1. `hello`, carrying `stored_rev`, `applied_rev`, `log_seq`, `log_hash` and every lease the node believes it holds.
2. The primary revokes every stale lease: held by a node the primary no longer considers the holder, or carrying a lower token than the primary's. The node fences those first, before anything else (section 4.2).
3. Only after fencing, the primary sends a full `desired_state`. A node that returned fenced starts nothing stateful, exactly as D-0022 step 1 requires.
4. The node reconciles. Because the snapshot is complete and `rev` is monotonic, reconciliation cannot start a duplicate app: a stateful VM appears in the snapshot only if the node holds the matching lease token, which it does not until the primary grants it again.
5. Log follow resumes. If the hash chain no longer matches, the divergent copy is quarantined and the node refollows from `seq` 1 (section 7).

Divergent local writes are kept as a quarantined snapshot and recorded in the audit log from M5b (D-0022 step 3, D-0031). There is no automatic failback: the default is that roles simply swap, and a return to the previous node is a planned switchover with a write freeze, a final sync and a lease move (D-0022).

## 10. Node removal

```
DELETE /api/v1/nodes/{node_id}
```

- The scope is `node:remove`, destructive and human-only (`README.md`), so an agent cannot remove a node.
- If the node is connected, the primary first sends `lease_revoke` for every lease the node holds and waits for each `command_result`, so the node fences its stateful VMs; then it ends the session with a fatal `error` code `node_removed` and marks the node `removed`. If the node is not connected, removal increments the fencing token of each of its leases, exactly like declaring the node lost (section 8.3), so the old copy can no longer write to shared storage. Only then may the app start elsewhere; the node itself is not asked to destroy anything.
- The node stops dialing permanently. It leaves its stateless VMs running and its fenced stateful VMs stopped, because deleting them is a data decision, not a protocol step.
- Removing a node never destroys data remotely. Purging local state — disks, rootfs images, the node's store — is a local CLI command run on that machine by whoever has root there. A node that is gone for good therefore keeps its data until someone with access deletes it, which is the safe default.

## 11. Security notes

Threats and their mitigations are in `../architecture/threat-model.md`; this section states only what the protocol does.

- **Credential storage.** The node credential and the node's private key are stored root-only on the node. The primary stores only a hash of the credential.
- **Desired-state signature.** A node accepts a snapshot only if the Ed25519 signature verifies against the cluster public key it received at enrollment. A compromised intermediate or a different cluster cannot make a node start an arbitrary microVM. The signature does not protect against the primary itself: whatever the primary signs, the node runs. The signing key rotates with a `signing_key_rotate` signed by the old key (D-0050), so a new key is accepted only from the holder of the old one.
- **Certificate pinning.** The node refuses a TLS certificate whose SHA-256 differs from the fingerprint in the join command, so a self-signed primary is as safe as a CA-issued one, and a stolen DNS name does not redirect a node.
- **Provider credentials stay on the primary (D-0094).** A node never receives a provider's account credential. For a tunnel it receives only that tunnel's run token, which cannot create tunnels, write DNS records or reach another node's tunnel. For ACME it asks the primary to write `_acme-challenge.<hostname>` TXT records, and only for the `acme` routes of its own desired state (section 5.22), so a compromised node cannot write arbitrary records in the zone or obtain certificates for hostnames it does not serve.
- **A compromised primary** can read everything the cluster stores — state, ciphertext it can decrypt with the cluster data-encryption key, and the telemetry that nodes stream on demand — and can issue desired state, lease grants and commands to every node. Until M5b it is also the lease authority, so it can grant the same lease to two nodes and run a stateful app twice; only a storage receiver that tracks the highest fencing token on its own side limits the damage. From M5b a quorum or external arbiter takes that power away from a single primary (D-0017). It cannot read a node's local store or its private key.
- **A compromised node** can lie about observed state, emit false events and, within its lease, run whatever its stored desired state allows. It cannot forge desired state for other nodes, cannot mint credentials, and cannot write to shared storage with a stale token. Its blast radius is its own workloads, which is why stateful data stays protected by the lease and the token rather than by the honesty of the node.
- **Terminal and console relay (M4).** The primary never touches a VM socket: it authorizes, audits and relays bytes between a client WebSocket and this session (ADR 0113, ADR 0114). The node agent, already root and already the only reader of VM sockets, is the only process that connects to a VM's vsock socket and its serial socket; both live in the VM's runtime directory, which only root can enter. The node never creates a `<socket>_<port>` listener, so a guest cannot open a vsock connection to the host. The guest is untrusted: the node bounds every frame it reads from it and treats its output as bytes, never as protocol. A `terminal` stream exists only because the primary opened it; a node never starts an exec session on its own.
- **Builds (M4).** A build runs untrusted code: the Dockerfile and everything it fetches. It runs only inside a builder microVM, with `egress_only` networking and no isolation group (section 6.6), never on the host. Build secrets reach the node only inside the signed desired state, reach the builder VM only on its ephemeral source disk, which the node deletes when the build ends, and live in the guest only in memory as BuildKit secret files; the node never writes them to a build log, a `stream_data` or an event. The node never mounts a disk the builder VM wrote: it reads the output layout as a tar stream in user space and never touches the cache disk's contents. The built image never leaves the node: it is imported locally and never pushed.

- **Image inventories (M5a).** The inventory is built by parsing package databases and manifests inside images the operator did not write, as root on the host. The parsers are Rust, in the engine's process, bounded in size (section 6.11), and read files of the unpacked tree only: they never execute anything from the image. What leaves the node is the package list, over the session, never the image.
- **Edge and mesh (M5a).** Besides the certificates of the routes it serves, the edge holds its tunnel token and its WireGuard private key, and it reaches a node only through the mesh, where the node's firewall admits only the edge's mesh address and only to the guest addresses and ports of that node's backends (section 6.9). A compromised edge can read and alter the traffic of the apps it serves and probe their backends; it cannot reach other guests, the node's host services or other nodes, and it cannot change desired state. With one edge in M5a, the edge is a single point of failure for the traffic of apps with more than one replica.
- **Volumes and backups (M5a).** A volume file is guest-written and never mounted on the host; snapshots, backups and restores handle it as bytes (section 6.7). A node receives the chunk keys of the apps it has jobs for and the bucket credential only while it has a job, never the repository key and never the audit-copy credential. A compromised node can read and overwrite nothing in the repository that it could not already read on its own disk: it can upload garbage chunks or manifests, which no seal covers and a restore refuses (`backup-format.md` section 8.1).

## 12. Local node store

The node keeps a SQLite file, owned by root, that holds everything it needs to run without the control plane (D-0011, D-0015 for the choice of SQLite elsewhere). The tables below are described, not specified: they are local to the node, are not part of `db-schema.sql`, are never replicated, and their exact columns may change without a contract change.

| Table | Holds | Why |
| --- | --- | --- |
| `node_meta` | `node_id`, `cluster_id`, `boot_id`, `host_boot_id` (the kernel boot id), the failed mark and the failed fences it covers (cleared by `command` kind `fence_ack`, section 5.17), agent and engine versions, the cluster public key and its `key_id`, a reference to the credential | Identity across restarts |
| `desired_state` | One row per accepted `rev`: the full signed body, its hash, `issued_at`, `term` | The snapshot to reconcile from, and the signature to re-verify |
| `applied_state` | Per VM the last applied spec digest, observed status, restart count, last exit code | Detects drift without the primary |
| `leases` | Lease id, resource, mode, token, state, `valid_until`, duration, remaining validity on `CLOCK_BOOTTIME` for `held` and `uncertain` expiring leases | Survives an agent restart: a fenced lease stays fenced, a held one keeps its remaining validity within the same host boot |
| `event_buffer` | Buffered events with their `evt_` id and time, plus the dropped counter | Bounded at 10 000; delivered after reconnect |
| `change_log` | Followed entries in `seq` order with `prev_hash` and `hash`, and the last acked `seq` | Verifies the chain and resumes follow |
| `quarantine` | Metadata for each quarantined divergent copy: path, time, reason | Nothing divergent is deleted (D-0022) |
| `cmd_results` | `msg_` id and outcome of the last answered intrusive messages | Makes a redelivered `lease_revoke` idempotent |
| `builds` (M4) | Per build: `bld_` id, app, status, phase, resolved commit, image digest, failed step, the path of its log file, start and end times | Reports build progress in `status_report` across an agent restart; keeps the logs of the last 20 builds of each app (section 6.6) |
| `transcripts` (M4) | Per terminal session: `trm_` id, VM, file path, size, SHA-256, start and end times | Serves `stream_open` kind `transcript` and enforces retention: 30 days and 1 GiB per node, oldest removed first (`audit-log-format.md` section 3.3) |
| `volumes` (M5a) | Per volume: `vol_` id, app, file path, size, state, last error | Reports volume states across an agent restart, and tells an `orphaned` file from one in `spec.volumes` |
| `edge_backends` (M5a, edge) | Per backend of `spec.edge.routes`: in rotation or not, since when, reason, consecutive probe results | Lets a restarted edge resume with its last view of backend health instead of sending requests to backends it had taken out |
| `backups` (M5a) | Per backup job: `bkp_` id, status, snapshot method, pause, manifest key and digest, sizes, error, start and end times; optionally the chunk ids the node knows the repository holds, per app | Reports backup progress across an agent restart and skips uploads of known chunks (`backup-format.md` section 5) |

Outside the SQLite file, next to the credential and the node's private key, the node keeps its WireGuard private key (from M5a, mode `0600`) and its identity file: `node_id`, `cluster_id`, `primary_url`, the URLs advertised in `welcome.primary_urls` (at most 8, section 5.10), the pinned certificate fingerprint, and the cluster public key with its `key_id`. It is written `0600` through a temporary file renamed into place, so a half-written identity is never read. Also outside the SQLite file, in the VM's runtime directory, each VM also has its serial ring: two segments of 512 KiB that keep the newest serial output across VMM restarts (`engine-trait.md` section 3.19). The ring is removed with the VM. From M5a each prepared image's inventory is a file next to the image in the engine's cache (section 6.11), so the node answers `inventory_resend` without inventorying the image again. Metric samples are never stored: they live in memory only (section 5.17).

The store is the reason a node can be restarted, upgraded or disconnected without stopping apps: it holds the signed intent, the applied reality, and the fencing state, and nothing in it depends on the control plane being reachable.
