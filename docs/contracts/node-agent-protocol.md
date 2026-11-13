# Node-agent protocol

Status: M0 draft for review. Contracts freeze at the end of M0 (D-0036); after the freeze a change to this file is a standalone contract pull request, never part of feature work.

This file fixes the wire protocol between a node agent and the control plane: enrollment, the dial-out session, the message set, desired-state reconciliation, change-log follow, and leases with self-fencing. It is the contract named in `README.md` for the node-agent, control-plane and engine workstreams.

Names used here — roles, node status, VM observed status, lease modes and node-side lease states, scopes, event types, message types — come from `README.md` and are used exactly as defined there. Field names come from the shared entity model used by `openapi.yaml`, `db-schema.sql` and `events.schema.json`. Terminology follows `../glossary.md`; in particular a **replica node** is not an **app replica**, and a **lease holder** is the only node that may run a stateful app (D-0019).

## 1. Purpose and principles

### 1.1 What this contract fixes

- The enrollment flow and the credential a node receives.
- The transport, the envelope and the message set in both directions.
- How desired state is delivered, verified, persisted and applied.
- How a node follows the change log (D-0013) and what it does when its copy diverges; from M5b, how a machine with the `node` role holds the cluster keys and is promoted to primary (ADR 0179 to ADR 0182).
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

The call is `x-quoro-scope: public`: the node has no credential yet and the join token is the proof. A token can be consumed once; a used or expired token is `409` or `422` in the API's error format (`openapi.yaml`). From M5b, enrolling a machine with the `node` or `witness` role changes the voter set, and is also refused with `409`, without consuming the token, while the previous voter-set change has not reached a majority of voters (section 8.10); `quoro join` says so and the operator runs it again.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `join_token` | string | yes | The join token secret |
| `name` | string | yes | Operator-visible node name, unique in the cluster |
| `public_key` | string | yes | Base64 Ed25519 public key of the node |
| `roles` | array of string | yes | Any of `node`, `witness`, `edge` (`README.md`) |
| `capacity` | object | yes | `cpus`, `memory_mb`, `disk_gb` |
| `kvm` | object | yes | `available` (bool), `kernel` (string), `nested` (bool, optional) |
| `custody_public_key` | string | no | From M5b, with the `node` role: base64 X25519 public key of the machine's custody key (section 7.2). The primary binds the machine to the first custody key it reports, here or in `hello` |

**Edge-only machines (M5a, ADR 0143).** A machine that enrolls with `roles: ["edge"]` runs no engine and starts no VM, so it needs no KVM: it reports the probe as it finds it (usually `available: false`), and the primary never places a VM on it. A machine with the `node` role still needs KVM to run VMs; `node` and `edge` may combine on one machine.

**Witness-only machines (M5b, ADR 0170).** A machine that enrolls with `roles: ["witness"]` runs no engine either and needs no KVM; it only votes (section 6.12). `witness` may combine with `edge` (ADR 0018) and with `node`, where it adds nothing, since one machine has one vote.

The `kvm` object is filled from a probe at enrollment: `/dev/kvm` exists and is writable, a `vmx`/`svm` flag is present in the CPU flags, and the kernel release is recorded. The probe procedure follows `docs/research/vps-nested-virt.md`; presence of `/dev/kvm` alone does not prove that a guest can start, so the first real boot is the confirmation and a failed boot is reported as a deployment failure in the `boot` phase.

### 2.4 Enroll response

| Field | Type | Meaning |
| --- | --- | --- |
| `node_id` | string (`nod_`) | Identity of this node |
| `cluster_id` | string (`cls_`) | The cluster it joined |
| `primary_url` | string | Base URL the node dials for this and later sessions |
| `credential` | string | Node session credential, returned once |
| `cluster_public_key` | string | Base64 Ed25519 public key that signs desired state |

The credential is 32 random bytes, base64-encoded, and stored root-only on the node. The primary stores only its SHA-256 hash, in the table `node_credentials` (`db-schema.sql` migration 0002), so a database read does not yield a usable credential. Until M5b the table was local. From M5b its rows travel in the change log like the other hash-only tables (migration 0012, ADR 0226), so that a promoted primary accepts every node's session (section 7.3).

Desired state and lease messages are signed with the cluster signing key (Ed25519), whose public key the node pins at enrollment; that key is separate from the cluster data-encryption key that protects data at rest (D-0050). The signing key is Ed25519; data at rest uses a 256-bit cluster data-encryption key with XChaCha20-Poly1305, plus a node-local key that encrypts this node's copy of desired-state secrets. All keys are files mode 0600 in `/var/lib/quoro` on the primary, outside the SQLite file, generated at first start and backed up separately from the database. From M5b every machine with the `node` role holds the signing key, the data-encryption key and the primary's TLS key sealed to a custody key of its own, never to its node-local key, which the primary never sees (ADR 0078, read by ADR 0180 and ADR 0181; section 7.2).

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
    |<---------------------------------------- welcome  |  a lower term than the node has seen: stale_term
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
    |<------------------------------- support_request  |  voters only, every support_ms/3 (M5b)
    |-- support_grant -------------------------------->|
    |                                                  |
```

### 4.2 Rules of the order

1. `hello` is the first message the node sends on every session.
2. `welcome` is the first message the primary sends. From M5b the node compares its `term` with the highest term it has seen and ends the session with `error` code `stale_term` when the `welcome` is lower (section 8.8).
3. The primary sends any `lease_revoke` before anything else. The node stops the stateful VMs bound to those leases and answers `command_result` for each revoke. While a revoke's self-fence runs, the node keeps processing `lease_grant` for other leases, including the answers to its renewals, liveness traffic, and on a voter `support_request`, which it answers with `support_grant`; it holds back every other message, such as `desired_state`, a `lease_revoke` of any lease, `command` and `stream_open`, until it has answered the revoke (section 5.13, ADR 0222, ADR 0237).
4. If an operator acknowledged a failed fence of this node and the node has not yet confirmed it, the primary then sends `command` kind `fence_ack`, once the node has answered every `lease_revoke` of rule 3 (sections 5.17 and 8.5, ADR 0239), and waits for its `command_result`. From M5b, while a signing-key rotation runs and the node still pins the current key, the primary then sends `signing_key_rotate` (sections 5.21 and 7.5) and waits for its `command_result`, so the `desired_state` that follows is signed with the key the node pins.
5. Only then does the primary send `desired_state`. The node verifies, persists, answers `command_result` with `ok`, and reconciles.
6. `log_entries` follow the desired state; the node acknowledges with `log_ack` as it verifies the chain.
7. Steady state begins: `heartbeat` every 5 s, `status_report` on any observed change and at least every 30 s, `event` whenever something happens, streams on demand. On a voter the primary sends its first `support_request` after the voter acknowledged the `desired_state`, so the voter knows its voter-set version (section 8.10).

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
| Support window `S` | 15 000 ms, asked again every `S / 3` | primary asks, voter promises | ADR 0169, section 8.8 |
| Connect timeout of a stored `primary_urls` entry | 2 000 ms in the first redial attempt, doubling with each failed attempt up to 10 000 ms | node | ADR 0139, section 5.10 |

The values above are carried in `welcome` (`heartbeat_interval_ms`, `session_timeout_ms`), so the primary can change them without a protocol change; the node uses the values from the current `welcome` and falls back to the defaults above when a session has not yet received one.

### 4.4 Disconnect and redial

The primary marks the node `disconnected` after 15 s without any frame from it and emits `node.disconnected` with `data.reason` either `heartbeat_timeout` (no frame within the timeout) or `session_closed` (the socket closed or the server ended the session). A node that reconnects is `connected` again and the primary emits `node.connected`.

The node redials with exponential backoff from 1 s doubling to 60 s with full jitter, and resets the backoff after a session that stayed up for at least 60 s. One redial attempt dials `primary_url` and, when that fails, each URL the primary advertised in `welcome.primary_urls` in order (section 5.10); the backoff applies between attempts, not between the URLs of one attempt. A `fatal` error stops redialing until an operator acts, with three exceptions. After `unsupported_version` the node keeps dialing (an upgraded primary may accept it) but logs the mismatch locally. After `stale_term` (section 8.8), sent by the node or received by it, the node keeps dialing too, through `primary_url` and every stored `primary_urls` entry, because the primary of the higher term may be reached at another address or start later; while it has no session it keeps running its stored desired state (section 6.5). After `not_primary` (section 7.4), from a primary that learned of a higher term, it keeps dialing in the same way: `primary_url` is now a demoted primary, and the new one is reached through the stored `primary_urls`. An agent that a promotion on its own host has frozen does not redial while the freeze lasts (section 7.3).

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
| `log_seq` | integer | yes | Last change-log `seq` verified and durably stored locally (section 7.1); 0 if none |
| `log_hash` | string | yes | Hex SHA-256 the node holds at `log_seq`; 64 zeros when `log_seq` is 0 |
| `leases` | array of object | yes | Leases known locally, see below |
| `capacity` | object | yes | `cpus`, `memory_mb`, `disk_gb` |
| `allocated` | object | yes | Same shape, currently allocated |
| `kvm` | object | yes | `available`, `kernel`, `nested` (optional) |
| `dropped_events` | integer | yes | Local events dropped from the buffer since the last successful delivery; 0 when none |
| `mesh_public_key` | string | no | Base64 WireGuard public key of this machine (32 bytes), from M5a (ADR 0144). The machine generates the key pair on its first M5a start and keeps the private key next to its credential; only the public key is ever sent. The primary stores it in `nodes.mesh_public_key` and uses it in other machines' `spec.mesh.peers` |
| `volume_snapshot` | string | no | How this node snapshots a volume (ADR 0156): `reflink` when its data directory supports `FICLONE` or its volumes live in the node's XFS volume pool, otherwise `pause_copy`. Probed once at agent start (`engine-trait.md` section 3.16). Absent from an agent older than M5a, which the primary treats as a node that cannot hold a volume |
| `custody_public_key` | string | no | From M5b, on a machine with the `node` role: base64 X25519 public key (32 bytes) of its custody key (section 7.2). Generated at the machine's first M5b start; the private half never leaves it |
| `signing_key_id` | string | no | From M5b: `key_id` of the cluster signing key this machine pins (section 5.21). The primary signs this machine's messages with that key while a rotation runs (section 7.5) |
| `control_plane_urls` | array of string | no | From M5b, on a machine with the `node` role: base URLs where this machine's control-plane listener would answer if it were promoted, best first, at most 4. Built like `welcome.primary_urls` from this machine's own addresses and the listener port of the installed configuration (8443 by default). The primary lists them in `welcome.primary_urls` (section 5.10) |
| `mesh_rules` | integer | no | From M5b: the version of the mesh firewall rules (section 6.9) that this machine's node agent applies whenever its desired state carries `spec.mesh`; `2` for the rules of section 6.9 with its "New connections" bullet. An agent older than that sends none. An integer from 1 to 2^53 - 1; a `hello` with another value is answered `bad_message` (section 5.18). The primary records it in `nodes.mesh_rules` (`db-schema.sql` migration 0015), `NULL` when the field is absent, and opens votes from it (section 6.12) |

Each lease entry:

| Field | Type | Meaning |
| --- | --- | --- |
| `lease_id` | string (`lse_`) | Lease identity |
| `resource_type` | string | `app` or `primary` |
| `resource_id` | string | `app_` or `nod_` id |
| `mode` | string | `local` or `expiring` |
| `fencing_token` | integer | Last token this node saw for the lease |
| `state` | string | `held`, `uncertain`, `expired` or `revoked` |

`leases` lets the primary detect a node that believes it holds a lease the primary has already moved, before any desired state is sent. A lease the primary does not know, or one whose `fencing_token` is lower than the primary's, is revoked immediately after `welcome`. The one exception is a lease whose row carries the unsettled mark and whose newest write gave the lease a new token and names that node as holder, in either mode: the row a regrant wrote after the node fenced, or the row a failover or a restore wrote whose grant a restarted primary never sent. When the node lists that lease at a lower token, or does not list it, the primary sends that grant instead of the revoke, under the conditions of section 8.4, "The holder fenced first" step 4. When the node lists it `held` or `uncertain` in the other mode, the primary sends the revoke first and that grant after it (ADR 0261, ADR 0280, ADR 0295, ADR 0299).

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
    "volume_snapshot": "pause_copy",
    "custody_public_key": "hSDwCYkwp1R0i33ctD73Wg2/Og0mOBr066SpjqqbTmo=",
    "signing_key_id": "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35",
    "control_plane_urls": ["https://192.0.2.21:8443"],
    "mesh_rules": 2
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
| `warm` | array of object | no | From M5b: one entry per volume of `spec.volumes` that carries `warm`, on the source and on the target (section 6.13, ADR 0173). On a target, also one entry with `role: target` for every warm copy the node still holds that `spec.volumes` does not name as a `present` target entry, until the copy is removed (ADR 0267). A volume the node holds as its own is not such a copy and is never reported that way. A returning source that still holds the volume as its own, before its hand-over (section 9), may leave its target entry out until it has durably recorded that it is returning, so the primary never sends a stale-copy `deleted` entry (section 5.11) for a file the node has not marked. Absent or empty when there is none |
| `quarantines` | array of object | no | From M5b: one entry per quarantined copy of a volume the node keeps (section 9, ADR 0176); absent or empty when there is none |

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
| `guest_flush` | boolean | no | From `running` (M5b, ADR 0282). `true` when the engine reported `Flushed` or `NoRunningVm` (`engine-trait.md` section 3.16): the snapshot then holds every write the guest completed before the flush, or no running guest held unwritten data when it began. `false` for any other outcome: an init that predates ADR 0282, a VM without vsock, a paused VM, or a flush that failed or timed out. A `false` backup is crash-consistent only: data written without `fsync` in about the last 35 s may be missing. It never fails the backup. Absent from an agent older than M5b |
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

Warm entry (M5b, section 6.13):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `volume_id` | string (`vol_`) | yes | The volume |
| `app_id` | string (`app_`) | yes | Its app |
| `role` | string | yes | `source` or `target`, as in `spec.volumes[].warm.role` |
| `snapshot_seq` | integer | no | On the target: the newest complete copy it holds. On the source: the newest snapshot the target confirmed. Absent before the first one |
| `snapshot_taken_at` | string | no | When the source took that snapshot, from the stream's header |
| `completed_at` | string | no | On the target: when it swapped that copy in. On the source: when the target's confirmation arrived |
| `allocated_bytes` | integer | no | On the target: bytes its complete copy uses on disk |
| `extent_count` | integer | no | On the source: the extents of the newest snapshot it took. A diff costs time in proportion to it (`docs/research/m5b-warm-and-leases.md`) |
| `frozen` | boolean | no | On the source: true while a freeze of `switchover_freeze` is recorded for the volume: its VM stays paused, or stopped when its VMM exited or a change of desired state stopped or replaced it (section 5.17); absent otherwise |
| `rejected_token` | integer | no | On the target: the token of the last stream it refused as stale (section 6.13) |
| `rejected_at` | string | no | On the target: when it refused that stream |
| `error` | string | no | One line on the last failed cycle or stream, at most 500 characters; absent once a cycle succeeds |

Quarantine entry (M5b, section 9):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `quarantine_id` | string (`qrn_`) | yes | The quarantined copy; the node generates the id |
| `volume_id` | string (`vol_`) | yes | The volume it is a copy of |
| `app_id` | string (`app_`) | yes | The volume's app |
| `created_at` | string | yes | When the node took the copy |
| `size_bytes` | integer | yes | Bytes the copy uses on the node's disk |
| `sha256` | string | no | Hex SHA-256 of the copy's content, 64 lowercase characters; absent while the node is still computing it |

The primary does not store edge backend health. It keeps the newest report, shows it in `AppDetail.edge_backends` (`openapi.yaml`), and turns changes into events: `app.degraded` while some but not all instances of an app are out of rotation, `app.down` with `check: edge_health` when none is left, and `app.up` when every instance is back (`events.schema.json`).

The primary owns the `volumes` and `backups` tables and the `backup.*` and `restore.*` events, as it does for builds: it applies each reported change to the row, seals a reported `succeeded` backup (`backup-format.md` section 6.2), and emits the events. A node emits no `backup.*` or `restore.*` event of its own. A volume reported `deleted` is removed from the `volumes` table and leaves `spec.volumes` in the next revision. An `orphaned` volume is kept and never deleted by the protocol (section 6.7).

The primary does not store `warm` entries either: it keeps the newest of each node in memory, and the progress fields of `App.warm` (`openapi.yaml`) come from them, so they are null after a primary restart until the nodes report again. Writing them would append a change-log entry per warm app every interval (ADR 0203 keeps renewals out of the log for the same reason). It does store quarantines, which are rare and must survive: the first time a report lists a quarantine with its `sha256`, the primary records it (`volume_quarantines`), writes the system audit record `volume.quarantine` (`audit-log-format.md` section 3.8) and emits `volume.quarantined`. A quarantine stays recorded until the node no longer reports it after `spec.quarantine_deletes` named it. A copy of a volume that is gone, or whose state is `deleting`, gets no row: the row needs the volume, and would hold back its deletion. The primary still writes its `volume.quarantine` record and emits `volume.quarantined`, once per `quarantine_id` (section 9, ADR 0316).

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

The primary answers with a `lease_grant` that carries the same `fencing_token` when the node still holds the lease — renewals keep the token, they never change it (D-0073) — with `re` set to this message's `id`. From M5b it answers only while a majority of voters supports it for the duration it grants (section 8.8); without that majority it leaves the renewal unanswered, so the renewal is late and the node follows section 8.4. A renewal whose token is lower than the primary's current token, or for a lease that no longer exists, is answered with `lease_revoke`. The node also sends one `lease_renew` immediately after a first grant and after a grant that converts a `local` lease to `expiring` (section 8.4). `local` leases are never renewed.

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
**When:** after the node has verified and durably stored a batch of `log_entries` (section 7.1).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `seq` | integer | yes | Highest contiguous `seq` verified |
| `hash` | string | yes | The `hash` of that entry, hex SHA-256 |

The primary keeps sending from `seq + 1`. An ack that skips a sequence is ignored: the node must verify every entry in order, because the point of the log is that a gap or a broken link is detectable (D-0013). From M5b the node acknowledges each `log_entries` message once, with its highest entry, and the primary keeps the newest ack of each node in memory (section 7.1).

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
| `snapshot_seq` | integer | no | From M5b: present only in an `ok` answer to `command` kind `switchover_freeze`: the final snapshot the target confirmed (section 5.17) |

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
| `term` | integer | yes | Cluster term: `1` until the first promotion, then higher with each promotion (section 8.9) and with each restore (section 8.8). A node that has seen a higher term refuses this session with `stale_term`. At most 2^53 - 1 (section 8.8) |
| `server_time` | string | yes | Primary's wall-clock time |
| `heartbeat_interval_ms` | integer | yes | 5 000 in v1 |
| `session_timeout_ms` | integer | yes | 15 000 in v1 |
| `primary_node_id` | string (`nod_`) | yes | The node holding the primary status |
| `primary_urls` | array of string | no | Other base URLs where the primary's listener can be reached, best first, and from M5b the URLs of the machines that could be promoted; at most 16 (ADR 0139, ADR 0235, section 7.3) |
| `schema_version` | integer | no | From M5b: the schema version of the primary's database (`db-schema.sql` `meta`). The node keeps the newest it saw; `quoro cluster promote` refuses to rebuild a database from a log newer than its binary (section 7.3, ADR 0229) |

**`primary_urls`.** The primary fills the list with its `public_url` when one is configured, then `https://<address>:<port>` for each non-loopback, non-link-local address of its host (IPv6 in brackets), then `https://<host name>.local:<port>` when that name resolves on the host, where `<port>` is the listener's port. Addresses of the node bridge are left out. From M5b the list goes on with the `control_plane_urls` of every other connected machine whose `Node.custody` is `sealed` (section 7.2), the machines a promotion could choose: the first URL of each such machine, oldest enrolled first, then the second of each, and so on. Every entry is an `https://` base URL with no path. The field is optional and additive (section 3.4): a primary that does not send it, or sends an empty list, is valid, and a node that does not know it ignores it.

The node keeps the list next to `primary_url` in its identity (section 12). It drops every entry that is not an `https://` URL and every duplicate, keeps the first 16, and replaces the stored list only when the result differs; a `welcome` without the field leaves the stored list as it is. When a dial to `primary_url` fails (the TCP connection, the TLS handshake against the pinned fingerprint, or the WebSocket upgrade), the node tries the stored URLs in order, with the same pinned fingerprint and the same credential, before it waits out the redial backoff (section 4.4). Each connection to a stored entry gives up after the connect timeout of section 4.3, so a list of unreachable entries costs seconds per attempt. A `503` on the upgrade, which a demoted primary answers (section 7.4), is a failed dial like any other. The first one whose session reaches `welcome` becomes the node's `primary_url` and is persisted. The pin and the credential never change: a host that does not hold the primary's private key cannot complete the handshake, so a stale or hostile entry costs one failed dial and nothing else. A `401` or `403` on the upgrade from a host that passed the pin is fatal as on `primary_url`. An operator can also point a node at a new address by hand with `quoro node set-primary <url>`, which completes a pinned handshake before it writes anything.

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
      "https://[2001:db8::10]:8443",
      "https://192.0.2.21:8443"
    ],
    "schema_version": 14
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
| `term` | integer | yes | Cluster term at issue time. A node refuses a snapshot whose term is lower than the highest term it has seen with `error` code `stale_term`. At most 2^53 - 1 (section 8.8) |
| `spec` | object | yes | `vms`, `routes`, `network`, `ingress` when the node runs a tunnel, `builds` when the node has builds to run (ADR 0117), `volumes` when the node holds volumes (ADR 0145), `backups` with `backup_repository` when it has backup jobs (ADR 0147), `mesh` once the machine is on the mesh, `edge` on a machine with the `edge` role (ADR 0143, ADR 0144), and from M5b `votes` on every voter and `witness` on a machine with the `witness` role (section 6.12, ADR 0170), and `quarantine_deletes` when the operator deleted quarantined copies the node keeps (section 9). An edge-only or witness-only machine gets `vms` and `routes` as empty lists and no `network` |
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
| `standby` | boolean | no | From M5b, on the warm node of a `warm` app (ADR 0174): the node prepares the VM's image and creates nothing else, so the VM is reported `stopped`. A VM of a `standby` entry that already exists on the node (the old source after a failover or a switchover) is deleted, keeping its volumes (section 9). `desired_status` is `stopped`. The VM is created and started only when a later snapshot drops `standby`, sets `desired_status` to `running` and carries a token of a lease the node holds (sections 6.4 and 6.13). Absent means false |
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
| `vm_ids` | array of string | yes | Candidate backends on this node. From M5a a node gets routes only for apps with one replica, so this holds one VM; the routes of apps with more replicas go to the edge (`spec.edge`), and from M5b so do those of `warm` apps while the cluster has an edge (ADR 0174). The one exception is a route with `draining_until`, which a node keeps for a short time after its app moved to the edge |
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
| `warm` | object | no | From M5b, on both nodes of a `warm` app: this node's part in replicating the volume (section 6.13, ADR 0173) |

**`spec.volumes[].warm`** (M5b):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `role` | string | yes | `source`: this node holds the app's lease, runs it and ships snapshots. `target`: this node holds the warm copy and receives them |
| `peer_node_id` | string (`nod_`) | yes | The other node of the pair |
| `peer_address` | string | yes | The other node's mesh address; the source dials the target's |
| `port` | integer | yes | TCP port of the target's replication listener on its own mesh address; 51822 unless the operator changed it |
| `interval_s` | integer | yes | Seconds between two snapshots on the source: 30 by default, 5 to 3 600 (ADR 0173, `docs/research/m5b-warm-and-leases.md`) |
| `lease_id` | string (`lse_`) | yes | The app's lease |
| `fencing_token` | integer | yes | The lease's current token. The source stamps it on every stream; the target refuses a stream with a lower one (section 6.13) |

An entry with `warm.role` `target` is the warm copy, not a second volume: the node creates its file only from a complete stream, reports it in `status_report.warm` and never in `status_report.volumes`, and removes it on a `deleted` entry like any volume. `size_gb` is the same on both nodes.

**A warm copy the node is no longer the target of (ADR 0267).** The app may have left `warm`, been given another warm node, or been deleted. The node keeps reporting such a copy, with `role: target`, until it is removed (section 5.3). For each copy reported that way, the primary sends a `deleted` entry without `warm`, once no failover can need the copy any more. That is the case when the volume or its app is being deleted or is gone. It is also the case when the app left `warm` or has another warm node, as long as the volume is `ready` on the node that holds the app's lease. The node removes the copy as its warm copy, because it holds that copy and no volume of its own. The entry stays in `spec.volumes` while the node reports the copy and no failover can need it. A delete that failed is therefore asked again, and after a primary restart the node's first report brings the copy back into view. A copy that a failover may still need gets no entry, and the node keeps it. A node never removes, through this rule, a file that is a volume of its own.

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
| `vm_id` | string (`vm_`) | no | The app's VM on this node as the primary placed it; absent when the app has no VM here. The node does not use it to choose what to pause: a `pause_copy` snapshot pauses the VM that attaches the volume (`engine-trait.md` section 3.16), which a redeploy or a stale VM row can make another one (ADR 0272) |
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
| `peers` | array of object | yes | The other end of each tunnel; on a node the edge, on the edge every node with the `node` role (a star in M5a). From M5b a voter also has every other voter, except that two witness-only machines are not peers (section 6.9) |
| `allow` | array of object | yes | On a node: what the edge may reach through the interface, one `{ "source": <edge mesh address>, "address": <guest address>, "port": <integer> }` per backend of this node. From M5b the target of a warm volume also gets `{ "source": <the source's mesh address>, "address": <this machine's own mesh address>, "port": <the warm entry's port> }` (section 6.13), on any machine, the edge included: a machine with both `edge` and `node` can be the warm node (ADR 0275), and a failover makes the source the target. On the edge `allow` is otherwise `[]` (ADR 0296) |

Each peer:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `node_id` | string (`nod_`) | yes | The peer machine |
| `public_key` | string | yes | Its `mesh_public_key` |
| `endpoint` | string | no | `host:port` to dial: the source address of the peer's session with `listen_port`, with the loopback fallback of section 6.9 (ADR 0144, ADR 0266); absent for a peer whose session the primary has not seen since it started, which dials this machine instead. On a machine whose only role is `edge`, the entries for nodes carry no `endpoint` and no `persistent_keepalive_s`, as in M5a: that machine is not a voter, and the nodes dial it (section 6.9, ADR 0296) |
| `allowed_ips` | array of string | yes | CIDRs routed to the peer: on the edge, a node's mesh address `/32` and its guest subnet; on a node, the edge's mesh address `/32`; between two voters, the other voter's mesh address `/32` |
| `persistent_keepalive_s` | integer | no | Keepalive interval, 25: on a node's entry for the edge, since any node may be behind NAT (ADR 0144), and from M5b on every entry between two voters (ADR 0266) |

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
| `backends` | array of object | yes | One per instance: `{ "vm_id", "node_id", "address", "port" }`, where `address` is the instance's guest address in its node's subnet and `port` the app port. A `warm` app has one backend, the instance on the node that holds its lease; its standby VM is never a backend (section 6.8) |
| `draining_until` | string (RFC 3339) | no | From M5b, on `tunnel` routes only: the app is back to one replica and its hostname moved to that node's tunnel; the edge keeps serving it from its own tunnel until this time ("Draining tunnel routes" above, ADR 0168) |

**`spec.quarantine_deletes`** (optional; M5b): an array of `qrn_` ids, the quarantined copies on this node that an operator deleted with `DELETE /api/v1/volumes/{volume_id}/quarantines/{quarantine_id}` (ADR 0176). The node removes each copy and stops reporting it in `status_report.quarantines`; an id it does not hold is ignored. The primary leaves an id out of the next revision once the node no longer reports it. Nothing else removes a quarantined copy: the node never removes one because a snapshot does not name it. Forgetting the record of a copy whose clone is already gone, which section 9 step 2 allows only for a record with no SHA-256, removes no copy.

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

**Tunnels (ADR 0094).** One tunnel per node. The primary creates or looks up the node's tunnel with the account credential of the `tunnel` provider, writes the tunnel's ingress rules (each `tunnel` route's hostname to `http://127.0.0.1:<origin_port>`) and the proxied DNS records itself, and sends the node only `tunnel_id`, `token` and `origin_port`. The node runs the tunnel connector with the token and serves the `tunnel` routes of `spec.routes` from its proxy on `127.0.0.1:<origin_port>`. Only the primary uses the account credential and no desired state carries it, but every machine with the `node` role can read it from the change log (sections 7.1 and 11, ADR 0320). A changed `token` (rotation) replaces the running connector; an absent `tunnel` stops it.

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
| `duration_ms` | integer | no | Present for `expiring`, absent for `local`. At least 3 000; `D` (15 000 by default, ADR 0171), or `W` while a maintenance window is open (section 8.11), at most 600 000 |
| `granted_at` | string | yes | Authority's wall-clock time of the grant |

A grant whose token is lower than the highest token the node has seen for that resource, or equal to it while the node does not hold that lease (state `held` or `uncertain`) under that token, is a bug in the authority and is refused with `error` code `bad_message`, non-fatal: the node keeps its current state and starts or stops nothing. So is a grant with a higher token whose `mode` differs from the lease the node holds (`held` or `uncertain`): a lease changes mode only with the same token (section 8.4, "Changing the mode", ADR 0258). After `expired` or `revoked`, a grant with a higher token may carry either mode. A same-token `local` grant that reaches the node after the lease expired there is refused the same way, although it is a race, not a bug: the primary then gives the lease back (section 8.5, "Regrant from `hello`", ADR 0281).

A grant with the same token as a lease the node holds is accepted whatever its `mode`. Its `re` decides how it is read (ADR 0256):

- An `expiring` grant that carries `re` is an answer to a renewal. When it answers no outstanding renewal of the node, it changes nothing, whatever the lease's mode: it neither converts a `local` lease nor sets `valid_until`.
- An `expiring` grant without `re` over a `local` lease is a conversion grant: it changes the lease's mode to `expiring` (section 8.4, "Changing the mode", ADR 0241).
- A `local` grant over an `expiring` lease changes the lease's mode to `local`. Renewals the node sent before it are no longer outstanding, so their answers change nothing.

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

On receipt the node starts nothing more for the lease: from then on no VM bound to it starts, whatever arrives later (section 6.4), until a grant with a higher token (section 8.5 step 4). It stops the stateful VMs bound to the lease, records the stop locally, moves the lease to `revoked`, and answers `command_result`. If the self-fence cannot confirm that every bound VM stopped, the answer is `command_result` with status `error` and the node follows section 8.5's failed-fence path. An agent that restarted without a verified desired-state snapshot cannot tell which VMs the lease binds; when it holds no recorded fence for the lease, it stops every app VM instead (section 6.3).

**Order while the fence runs** (ADR 0222, ADR 0237). Until it has answered the revoke, the node processes only:

- `lease_grant` for any other lease, including the answers to its `lease_renew`;
- liveness traffic: it keeps sending `heartbeat`, answers `ping` with `pong`, and accepts the `pong` that answers its own `ping`;
- on a voter, `support_request`, which it answers with `support_grant` as at any other time (section 8.8). A request with a lower term ends the session with `stale_term`, as at any other time, and the fence goes on.

It also keeps sending `lease_renew` for its other `expiring` leases. Every other message of the session is held back and processed in the order received once the revoke is answered: for example `desired_state`, a `lease_revoke` of any lease, the one being fenced or another, `command` and `stream_open`. An `expiring` lease whose revoke is held back still fences at its own `valid_until` (section 8.4), so holding the revoke back delays only its answer.

This is safe. Section 4.2 rule 3 exists so that nothing received after a revoke can start the revoked lease's VMs before they are stopped. None of the messages let through can: a grant names another lease, liveness traffic starts nothing, a support request changes only the voter's own vote state (section 8.8), and the start gate above keeps every VM of the revoked lease stopped while its fence runs and afterwards. Holding back every message would hold back renewal answers too. A `local` lease's fence has no deadline and can take longer than a third of `D`, so an `expiring` lease of another app could reach `valid_until` and fence although the primary renewed it. Holding back support requests would cost the primary this voter's support for as long as the fence lasts, and with it, when the other voters cannot make up for it, the majority it needs to grant and renew `expiring` leases.

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
| `head_seq` | integer | no | From M5b: the `seq` of the primary's newest entry when it sent this message, so the node knows how far behind it is (section 7.1, ADR 0227) |

Each entry has exactly the change-log fields from the shared entity model: `seq` (gap-free from 1), `term`, `time`, `actor_type`, `actor_id`, `entity_type`, `entity_id`, `op` (`create`, `update`, `delete`), `payload` (below), `prev_hash` (64 hex, 64 zeros for `seq` 1) and `hash` (hex SHA-256 over the RFC 8785 serialization of the entry without `hash`).

The `payload` depends on `op` (ADR 0297):

- **`create`:** the whole new row.
- **`update`:** the entity's key and every field the change writes. A field the change does not write may be absent, and it keeps its value: a rebuild merges the payload over the current row (section 7.3 step 4).
- **`delete`:** `{}` or the deleted object. No reader applies it: the row removed is the one `entity_id` names.

A column with no field of its own in the API object travels under the column's name. An app's `warm_node_id`, `warm_node_pinned` and `warm_interval_s` do so in a whole-App payload, and a partial app payload omits them (`db-schema.sql` header, ADR 0286). A payload field with no column is ignored, whether it is a field the primary derives on read, such as `App.warm`, which is null, or a column a later migration dropped.

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
    ],
    "head_seq": 903
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
| `kind` | string | yes | `logs_query`, `probe`, `fence_ack`, `build_logs_query`, `metrics_query`, `inventory_resend`, and from M5b `switchover_freeze` or `switchover_thaw` |
| `params` | object | yes | Kind-specific parameters |

`logs_query` params: `app_id` or `vm_id` (exactly one), `since` (RFC 3339, optional), `until` (RFC 3339, optional), `tail` (integer, optional). The answer is one `command_result` with `status: ok` and the matching lines in `lines` (section 5.9), each carrying its `vm_id`. The answer must fit the 1 MiB message limit: the node keeps the newest lines that fit and says in `detail` that older ones were left out.

`probe` params: `{}`. The node re-runs the KVM probe (`/dev/kvm`, CPU flags, kernel release) and answers with a `command_result` whose `detail` summarizes it; the primary updates the node's `kvm` record and `engine_version`.

`fence_ack` params: `lease_ids` (array of `lse_` ids, at least one) and, from M5b, `fencing_tokens` (object keyed by lease id, with an integer for every id in `lease_ids`: the lease's `fencing_token` once the acknowledgement was recorded, that token plus one for a lease another node holds, or 9007199254740991 (2^53 - 1) for a lease the primary has no row for, section 8.5 step 3; ADR 0245, ADR 0247, ADR 0262). It carries an operator's acknowledgement of failed fences, recorded through `POST /api/v1/nodes/{node_id}/fence-ack` (section 8.5). The primary sends it only once the node has answered every `lease_revoke` sent on the session, and leaves out a lease whose fence failed again after the acknowledgement (section 8.5 step 4, ADR 0238, ADR 0239). For each listed lease, the node clears the failed fences it recorded with a token below the lease's entry in `fencing_tokens` (section 8.5 step 2). A failed fence at or above that token is one the acknowledgement does not cover: it stays, and the primary lists its lease again from the failure's report (section 8.5 step 1, ADR 0238). A `fence_ack` whose `fencing_tokens` misses a listed lease is answered with `command_result` status `error`, and the node clears nothing. Without `fencing_tokens`, as a primary before M5b sends it, the node clears every failed fence of each listed lease, as before. The node clears its failed mark once no failed fence remains. It answers `command_result` with `status: ok`; when failed fences remain, of other leases or at or above an acknowledged token, the node stays failed and `detail` names them. A listed lease with no recorded failed fence is not an error, so a redelivered `fence_ack` is harmless. The listed leases stay `revoked` or `expired` on the node: it starts nothing stateful for them until a `lease_grant` arrives with a higher `fencing_token` (section 8.5 step 4).

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

`switchover_freeze` params (M5b): `app_id`, `vm_id`, `volume_id`, `lease_id`, `fencing_token`. The first step of a planned switchover (ADR 0177, section 6.13). The node checks that it is the source of `volume_id` and holds `lease_id` in state `held` with `fencing_token`; otherwise it answers `status: error` with code `not_available`. It first asks the engine to flush the volume in the guest (`flush_volume`, bounded at 2 s, `engine-trait.md` section 3.22, ADR 0282) while the app keeps serving, so the final snapshot holds every write the app completed before the flush, `fsync` or not. Whatever the flush returns, an error included, the switchover goes on: a flush that failed, timed out or is unsupported leaves the final snapshot crash-consistent, as before ADR 0282, and is not a failed step. Writes between the flush and the pause are crash-consistent only. Then it pauses the VM through the engine (`engine-trait.md` section 3.22), takes a snapshot, ships it to the target as one cycle of section 6.13, and waits for the target's answer. When the target answers `complete`, the node answers `status: ok` with `snapshot_seq`, and the VM **stays paused**: the app's writes are frozen until the freeze ends ("How a freeze ends" below). If any step fails, the node ends the freeze at once under the thaw check below and answers `status: error` with code `replication_failed` and the reason. While the VM is paused this way, the source's `status_report.warm` entry carries `frozen: true`. The node records the freeze in its store, with the freeze's `lease_id` and `fencing_token`, before it pauses the VM (section 12). The record survives a restart of the node agent and of the host, so a node agent that restarts keeps the VM paused and still knows the lease that froze it.

`switchover_thaw` params (M5b): `app_id`, `vm_id`. Ends the freeze that `switchover_freeze` recorded for the app, under the thaw check below, and answers `status: ok`, also when the check leaves the freeze in place. `vm_id` names the app's VM as the primary knows it; the thaw ends the freeze also when a change of desired state stopped or replaced the VM it froze ("How a freeze ends" below). An app with no freeze recorded is left as it is, and the answer is still `ok`, so a repeated thaw is harmless. The primary sends it when it gives up a switchover before the revoke, and whenever a report shows `frozen: true` for an app with no switchover it is running, for example after the primary restarted. It sends none while a failed fence covers the app's lease: a node's `fence_failure` lists the lease, or a node reported a failed fence of it that the primary could not record (section 8.5). Nor does it send one while the lease is revoked in its own view (`leases.revoked_at` is set), as after step 3 of a switchover committed (section 6.13): the switchover can then only go forward, and the revoke that reaches the source ends the freeze, on its current session or at its next session start, where the primary revokes a lease it records revoked (section 9 step 2).

**The thaw check.** The node ends a freeze only while it holds the freeze's lease, the `lease_id` at the `fencing_token` recorded with the freeze, exactly that token, `held` or `uncertain`, and is not failed (section 8.5). A lease held at a higher token does not pass. Otherwise it leaves the freeze recorded, and the VM paused, or stopped when it was stopped meanwhile (below). The check and the resume are one step for the lease's state, so a fence and a thaw never interleave: a fence that starts after the check stops the resumed VM as it stops any VM bound to the lease, and a thaw checked after a fence started finds the lease `expired` or `revoked` and resumes nothing. A thaw sent after a `lease_revoke` is processed only once the revoke is answered (section 5.13), so it finds the lease `revoked`. A resume is not a start, but under an `expiring` lease it needs the start margin of section 6.4 (ADR 0242), `valid_until - now >= start bound + M` on the lease clock, because the resume takes the VM's operation lock (`engine-trait.md` section 4), which a fence at `valid_until` needs. Without that margin the node leaves the VM paused and the freeze recorded, as when the check fails. The primary's next thaw tries again, and for a freeze whose warm entry a `desired_state` dropped (below), the node tries again once the margin is back. A late renewal or a lost session can make the lease `uncertain` without ending the freeze, and a VM that a thaw resumes then runs as any VM does under an `uncertain` lease (section 8.4).

**How a freeze ends.** Only in these ways:

- a thaw that passes the check;
- a `desired_state` in which the frozen VM's volume no longer carries `warm` with `role: source` (the app left `warm`, or the volume moved or is gone) ends the freeze under the same check, as a thaw does. From then on the node reports no `frozen: true` for the VM, so no thaw follows: a node that is failed at that moment ends the freeze under the check once its failed mark clears (section 8.5, "Acknowledging a failed fence" step 5);
- a `switchover_freeze` whose step failed, under the same check (above);
- a `lease_revoke` of the lease, whatever its token (section 5.13), or the fence when the lease expires (section 8.5). No stop resumes the VM first (`engine-trait.md` section 3.8). After the revoke or the fence, failed or not, the node drops the freeze's record once the engine reports the VM stopped or gone, and the VM stays stopped;
- a `lease_grant` of the freeze's lease with a token above the freeze's, which drops a record that still stands: the switchover that froze it can no longer go forward. A VM still paused then is stopped without a resume, and the node starts the VM its desired state runs under section 6.4.

Nothing else ends a freeze. A stop that a change of desired state asks for, a restart (`restart_generation`) or a new deployment that replaces the VM included, keeps the record, unless the same `desired_state` drops the warm entry, which ends the freeze as the second item above says. So does a VM whose VMM exits while it is frozen, a restart of the host included. The node keeps reporting `frozen: true`, and while the record stands it starts no VM that attaches the frozen volume (section 6.4): not the frozen VM after a crash (section 6.3), not a restart of it, and not a VM that replaces it. When a thaw, or a `desired_state` that ends the freeze as above, passes the check, the node drops the record. It then resumes the frozen VM if it is still paused and its desired state still runs it; a paused VM that its desired state stops, replaces or removes is stopped without a resume (`engine-trait.md` section 3.8). Otherwise it starts the VM that its desired state runs, the frozen one or its replacement, under section 6.4. So the app makes no write to the volume after the final snapshot while the switchover can still go forward (section 6.13).

A fence that cannot confirm the stop ends nothing at once. The VM may then still be paused, with its freeze recorded, under a lease that is `revoked` or `expired` on a node that is failed (section 8.5, "Failed fence"). The check keeps it paused, the primary sends no thaw for it, and the failed node starts no stateful VM (section 6.4). The case ends as any failed fence does: an operator checks that the VM stopped and acknowledges the fence. The lease stays `revoked` or `expired` on the node after that acknowledgement (`fence_ack` above), so no later thaw resumes the VM either. The record stays until the engine reports the VM stopped or gone, as the operator's check makes it, or until the next `lease_revoke` of the lease, which the node's next session start sends (section 5.1) and which drops it the same way. Until then, its failed mark cleared or not, the node starts no VM that attaches the frozen volume (section 6.4).

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
      "lease_ids": ["lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D"],
      "fencing_tokens": { "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D": 8 }
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
| `term` | integer | no | From M5b: the sender's term, the highest it has seen. Carried with code `stale_term` (section 8.8, ADR 0261), and with the `bad_message` a voter sends to a `support_request` because it voted in that term for another candidate (section 5.27, ADR 0280). At most 2^53 - 1 (section 8.8) |
| `re` | string (`msg_`) | no | The message that caused it, in the envelope |

| Code | Fatal | Meaning |
| --- | --- | --- |
| `unsupported_version` | yes | Envelope major version not implemented |
| `unauthenticated` | yes | Credential rejected; the node stops dialing |
| `node_removed` | yes | The node was removed from the cluster (section 10) |
| `bad_signature` | no | A `desired_state` or `signing_key_rotate` signature did not verify; the message was discarded |
| `bad_message` | no | Malformed frame, unknown type, unknown command kind, oversized frame |
| `log_diverged` | no | The node's change-log copy does not match the primary's (section 7.1) |
| `not_found` | no | A `stream_open` or `command` named a build, VM or transcript this node does not have. Carried in the `error` of a `command_result` |
| `not_available` | no | The VM exists but cannot serve the requested stream now: a `terminal` on a stopped VM or on a VM started before the M4 guest init. Carried in the `error` of a `command_result` |
| `fence_failed` | no | A self-fence could not confirm that every stateful VM bound to the lease stopped, so a VM may still run (section 8.5). Carried in the `error` of the `command_result` that answers a `lease_revoke`; the node has marked itself failed |
| `replication_failed` | no | From M5b: a `switchover_freeze` could not ship its final snapshot, or the target refused it (section 6.13). Carried in the `error` of its `command_result`; the node ended the freeze under the thaw check of section 5.17, so the VM may stay paused |
| `stale_term` | yes | From M5b: the other side's term is lower than the highest term the sender has seen (section 8.8). A node sends it on a `welcome`, `desired_state` or `support_request` from an older primary and ends the session; it keeps redialing (section 4.4). It carries `term`. A primary that receives it stops granting and renewing `expiring` leases at once, and demotes itself (section 7.4). The one exception is a primary restored with `quoro restore --accept-loss` that has sent no `lease_grant` yet: it starts again at one above that term instead (`backup-format.md` section 8.2, ADR 0261) |
| `not_primary` | yes | From M5b: the sender was the primary and has learned of a higher term (section 7.4). It ends every session with this code; the node keeps redialing through its other URLs (section 4.4) |
| `custody_key_changed` | no | From M5b: the `custody_public_key` of a `hello` differs from the key the primary bound to the machine (section 7.2). The session goes on; the primary makes no seal to the new key |

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
**When:** to test liveness outside the heartbeat cadence, or to keep a connection warm through an intermediary that closes idle sockets. From M5b the primary also sends one after a same-token `local` grant, and sends no conversion grant for that lease until its `pong` arrives (section 8.4, ADR 0257), and one after the renewal answer that settles a lease, whose row it writes only once the `pong` arrives (section 8.4, "Settling a lease", ADR 0261).

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

**When, from M5b.** During a rotation (section 7.5) the primary sends it to every machine of any role, edge-only and witness-only machines included, because they verify `desired_state` too: at once to the connected machines, and to every other one on its next session, after any `lease_revoke` and `fence_ack` and before its `desired_state`.

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

### 5.27 `support_request`

**Direction:** control plane to node.
**When:** from M5b, to every connected voter (a machine with `spec.votes`, section 6.12): once after `desired_state` at the start of each session, then every `support_ms / 3` (ADR 0169, section 8.8).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `term` | integer | yes | The primary's term, at most 2^53 - 1 (section 8.8) |
| `primary_node_id` | string (`nod_`) | yes | The node holding the primary status, as in `welcome` |
| `support_ms` | integer | yes | `S`, the support window asked for; 15 000 by default, at least 3 000 |
| `max_lease_ms` | integer | yes | `L`, the longest `duration_ms` the primary may grant while this support lasts: `D` normally, `W` while a maintenance window is open (section 8.11) |
| `voters_version` | integer | yes | The voter-set version the primary counts against (section 8.10) |

The voter answers with `support_grant` (section 5.28) once its promise is stored, or with `error`: `stale_term`, fatal, when `term` is lower than the highest term it has seen (section 8.8); `bad_message`, non-fatal, when it voted in this term for another candidate than `primary_node_id`, when `support_ms` is below the floor, or when `term` is above 2^53 - 1 (section 8.8). The `bad_message` for a vote in this term carries the voter's `term` (section 5.18), so a restored primary learns of that term (section 8.8, ADR 0280). A machine that is not a voter ignores the message.

```json
{
  "v": 1,
  "type": "support_request",
  "id": "msg_01JCA0B2C3D4E5F6G7H8J9K0MN",
  "body": {
    "term": 1,
    "primary_node_id": "nod_01JC9Z0000000000000000000Q",
    "support_ms": 15000,
    "max_lease_ms": 15000,
    "voters_version": 3120
  }
}
```

### 5.28 `support_grant`

**Direction:** node to control plane.
**When:** in answer to a `support_request`, with `re` set to the request's `id`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `term` | integer | yes | The term supported, equal to the request's, at most 2^53 - 1 (section 8.8) |
| `support_ms` | integer | yes | The window granted, equal to the request's |
| `max_lease_ms` | integer | yes | The `L` recorded with the promise, equal to the request's |
| `voters_version` | integer | yes | The voter-set version of the voter's stored desired state; the primary counts the support only when it equals its own (section 8.10) |

The voter sends it only after it has stored its promise and its lease horizon (sections 8.8 and 12). The primary measures the support from the time it sent the request, never from the time the answer arrived. An answer that does not pair with an outstanding request is ignored.

```json
{
  "v": 1,
  "type": "support_grant",
  "id": "msg_01JCA0B3D4E5F6G7H8J9K0MNPQ",
  "re": "msg_01JCA0B2C3D4E5F6G7H8J9K0MN",
  "body": {
    "term": 1,
    "support_ms": 15000,
    "max_lease_ms": 15000,
    "voters_version": 3120
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
2. **Images.** Prepare every image referenced by a VM whose rootfs is missing or has a different digest, and remove rootfs images no longer referenced. An image whose VM carries `build_id` is never pulled: it was prepared by the build's `importing` step (section 6.6); if it is gone, the deployment fails in phase `pull` with the error that the built image is no longer on this node. A root disk is referenced while a VM in `spec.vms` or a build in `spec.builds` names it. A `standby` VM's image is prepared like any other, so a failover does not wait for a pull (ADR 0174).
3. **Stop.** Stop VMs that are no longer desired, or whose spec requires a recreate (image digest, disk size, network identity, attached volumes).
4. **Volumes, then start.** Create the file of every `present` volume that has none, or write it from its `restore`, and remove the file of every `deleted` volume that no VM uses any more (section 6.7). The file of a warm target comes only from a complete stream (section 6.13). Then start desired VMs that are not running. A VM whose volume is not `ready` on this node stays `stopped`; volume work never delays a VM that attaches no volume.
5. **Builds and backups.** Start the next `queued` build of `spec.builds` when the node has no build running (one at a time per node by default, ADR 0118), stop a build marked `cancelled`, and report every change in `status_report.builds` (section 6.6). Builds run alongside the other steps, never block them, and are not restarted by a later apply run. Start every `spec.backups` job that is not running (section 6.7); backups also run alongside, one per volume. From M5b the warm cycle of every volume the node is the source of runs alongside too (section 6.13).
6. **Routes.** Update the reverse proxy for the hostnames in `spec.routes` and, on an edge, `spec.edge.routes` (section 6.8), and start, replace or stop the tunnel connectors to match `spec.ingress.tunnel` and `spec.edge.tunnel`. A route with `draining_until` is applied like any other: the node serves exactly the routes of the snapshot and drops one only when a snapshot no longer carries it (section 5.11). Certificates for `acme` routes are obtained in the background through `dns_challenge` (section 5.22); a route keeps serving its previous certificate until a new one is issued.

The order exists so that a VM never starts on a network that is not ready, and no route points at a VM that is being replaced. It is the reconciliation described in D-0011 and the deployments in D-0033.

### 6.3 Restart, crash and health

- A VM that exits without being asked to stop is `crashed`; the node restarts it with backoff from 1 s doubling to 5 min. A VM that exits while `switchover_freeze` keeps it frozen is not restarted until the freeze ends (section 5.17). The backoff resets after the VM has been healthy for 10 minutes. `restart_generation` in the spec is what a human-initiated restart changes; the crash path does not increment it.
- A start that fails is not tried again at once (ADR 0250). After the engine answers a start with an error, `Timeout` included (`engine-trait.md` section 3.7), the node waits the crash backoff above before it starts the VM again: 1 s, doubling to 5 min, with the same reset. The wait counts from when the failed start returned, so a start that used its whole bound still waits a full backoff, and a crash restart whose start fails waits the same way. Otherwise a VM whose boot always takes longer than its bound, 3 s under an `expiring` lease (section 6.4), would be booted and stopped by force every few seconds. A successful start clears the wait; the backoff keeps its length until that reset. A new `restart_generation` also clears the wait, and the node starts the VM at once. The first start of a VM never waits, because only a failure sets the wait, and a failed start does not count as a restart (`restart_count` in `status_report`, section 5.3).
- Health checks use the app's `health_check` (`path` default `/`, `interval_s` default 10, `timeout_s` default 2, `failures` default 3, `start_period_s` default 30, `restart` default true). Failed probes within `start_period_s` after the VM started do not count. After `failures` consecutive failures the node reports the app as `failing` and emits `app.down`; on recovery it reports `passing` and emits `app.up`.
- From M5b a VM that stays unhealthy is restarted (ADR 0184). With `restart` true, after `failures` consecutive failures outside the start period the node, besides reporting `failing` and emitting `app.down`, stops the VM and starts it again in place, with the crash backoff above (1 s doubling to 5 min, reset after 10 minutes healthy). The start period begins again at each start. A stateful VM is started again only under the rule of section 6.4: its node holds the app's lease in state `held` with the matching token; otherwise it stays stopped, as for any stateful start. A health restart is not a human restart: it does not change `restart_generation`. With `restart` false the node only reports, as before M5b. A snapshot without `restart` means true.
- A health restart stops a VM only when the VM may start again (ADR 0259). For a stateful VM, that means section 6.4 would let it start now: its node holds the lease `held` with the spec's token and, under an `expiring` lease, has the start margin of ADR 0242 left. Under an `uncertain` lease the VM keeps running (section 8.4), and the next failure counted asks for the restart again. If the lease leaves `held` between the stop and the start, the VM stays stopped. A VM that a fence of its lease is stopping is left to the fence (section 8.5). A VM that a backup has paused (a `pause_copy` snapshot, ADR 0156) is not restarted for its health while it is paused. The pause lasts the whole copy, and the engine reports the VM `running` meanwhile, so failed probes while it is paused count toward no failure, and the count starts from zero when the VM is resumed. A VM that `switchover_freeze` paused (section 5.17) is treated the same way for as long as it stays frozen, across a restart of the node agent too. Failed probes while it is frozen count toward no failure: they do not restart it for its health, make it `failing` or emit `app.down`. The count starts from zero when the node resumes it (section 5.17). A `lease_revoke` or a fence stops it as it stops any VM.
- Image preparation is cached by digest. Two VMs with the same digest share one prepared rootfs; the digest, not the tag, is the key.
- An agent restart does not restart VMs. VMMs are separate processes that outlive the agent, and the agent rediscovers them on start (`engine-trait.md`, D-0011). This is what makes an agent upgrade invisible to running apps.
- One exception (ADR 0224). An agent can restart with no verified desired-state snapshot in its store: none was persisted, or none verifies. If it then receives a `lease_revoke` for a lease with no recorded fence, it cannot tell which VMs that lease binds. It fences by stopping every app VM on the node, as section 8.5 describes, records the stop for that lease, and answers the revoke. It stops no builder VM. It starts nothing until the first `desired_state` verifies. Then it starts again the VMs of other leases and of stateless apps that the snapshot lists as running, a stateful VM only under the rule of section 6.4; the revoked lease's VMs stay stopped. The case needs both a missing snapshot and a revoke with no recorded fence, so it is rare, and stopping every app VM is the safe side against two copies of one app.

### 6.4 Stateful starts

A stateful VM starts only while the node holds that app's lease in state `held` **and** the lease's `fencing_token` equals the `fencing_token` in the VM spec. If either condition fails the VM stays `stopped`, the fact is recorded locally, and the node reports the VM as `stopped` with `health: unknown`; it does not report an error to the primary, because the primary is the one that moved the lease (D-0019). Because renewals keep the token (D-0073), the equality holds for the whole time one holder owns the lease; it changes only when ownership changes, and then the primary issues a new `desired_state` carrying the new token. A node that marked itself failed (section 8.5) starts no stateful VM at all. From M5b no VM starts while a freeze of `switchover_freeze` is recorded for a volume it attaches (section 5.17). In M5a a VM is stateful exactly when it attaches a volume (ADR 0145), and its volume must be `ready` on this node. From M5b a `standby` VM is never created or started, whatever lease the node holds (section 5.11); on a warm node the VM starts only from a snapshot that drops `standby`, after the lease was granted to this node (section 6.13).

From M5b a stateful start under an `expiring` lease also needs time left on the lease (ADR 0242). The node starts the VM only when `valid_until - now >= start bound + M`, on the lease clock of section 8.4 (for a converted lease that has no `valid_until` yet, its provisional deadline stands in), and passes the start bound, 3 000 ms, to the engine, which gives the start up when the bound passes (`engine-trait.md` section 3.7). Otherwise a start that took its permit just before `valid_until` could still hold the VM when the fence must stop it. Right after a renewal with `D` = 15 000 ms about 13 500 ms remain, and about 8 500 ms just before the next renewal is answered, so the check holds back no start in steady state. A lease granted for 5 000 ms or less never leaves enough time, and starts no stateful VM.

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
- **Delete.** A `deleted` entry: once no VM of `spec.vms` attaches the volume and no running VM uses it, the node removes the file (`delete_volume`) and reports `deleted`. The primary sends one for a volume deleted on its own and, from M5b, for every volume of a deleted app, whose record it keeps in `deleting` until this report arrives (ADR 0165). A `deleted` entry that reaches a node only through the stale-copy rule of section 5.11 never removes a volume file of the node's own. That is the case of a returning source before its hand-over, which has recorded that it is returning (section 5.3): it keeps the file, its row and its kept snapshots, and reports the file `orphaned` once the entry leaves `spec.volumes`, because the file may hold divergent writes (section 9).
- **Never by absence.** The node never removes a volume file because `spec.volumes` does not name it. Such a file is reported `orphaned` and kept until a `deleted` entry names it, other than a stale-copy one (Delete above), or someone with root on the node removes it, as section 10 does for a removed node.
- **Backup.** For each job of `spec.backups`, the node first checks that it holds the app's lease in state `held` with the job's `fencing_token`; otherwise it reports the backup `failed` with `lease_mismatch`. It then takes the snapshot with its method (`engine-trait.md` section 3.16, ADR 0272). The engine first asks the guest to flush the volume (ADR 0282), and for `pause_copy` it then pauses the VM that attaches the volume. The node reports `running` with `guest_flush`, chunks and uploads the snapshot, writes the manifest and deletes the snapshot file (`backup-format.md` section 7.1). It reports `succeeded` with the manifest's key and digest, or `failed` with the reason. A job leaves `spec.backups` once the primary has recorded its terminal status; an agent restart while a backup runs fails it with `node agent restarted`, and the snapshot file is deleted at start.
- **Writes carry the token.** Every object a backup job writes is part of a backup that the primary seals only if the job's token is still the app's current token (`backup-format.md` section 6.2). A node that lost its lease while uploading therefore wastes storage, never a valid backup (section 8.6).
- **Warm volumes (M5b).** A volume entry with `warm` is replicated as section 6.13 describes. A backup of a `warm` app's volume runs on the source, which holds the lease. A `deleted` entry removes the copy on each node. A quarantined copy is removed only through `spec.quarantine_deletes` (section 9).

### 6.8 The edge

A machine with the `edge` role runs the embedded proxy for `spec.edge.routes` (ADR 0143). It needs no engine; on an edge-only machine the apply loop runs only steps 1 and 6.

- **Health.** For each backend the edge probes `health_check.path` over the mesh every `interval_s`, with `timeout_s`. A backend leaves the rotation after `failures` consecutive failed probes and returns after the same number of consecutive passes. A request that cannot connect, or whose connection is reset before a response, takes the backend out at once with reason `connect_failed`; it returns through the probes. A backend the edge has not probed yet starts in rotation.
- **Balancing.** Round robin over the backends in rotation, with no stickiness. A `GET` or `HEAD` request that fails to connect, or whose connection is reset before response headers arrive, is retried once on another backend in rotation; no other request is ever sent twice. With no backend in rotation the edge answers `502`.
- **Leaving the rotation.** When a backend leaves the rotation for any reason in this section, the edge drops its pooled connections to it and cancels the requests still in flight to it. A cancelled `GET` or `HEAD` that has no response headers yet, and whose request body can be sent again, is retried once on another backend in rotation; any other cancelled request answers `502` at once; a response that is already streaming to the client is cut and never retried. A backend that only leaves `spec.edge.routes` (for example during a deploy) is not cancelled: its requests finish normally.
- **Timeouts.** A connect timeout of 5 s and a response-header timeout of 60 s per request, so a backend that accepts and hangs cannot hold a connection forever. Backend connections use TCP keepalive and, on Linux, `TCP_USER_TIMEOUT`, so a connection to a backend that disappeared fails within seconds instead of waiting for the response-header timeout.
- **Without the primary.** The edge keeps its last verified desired state on disk like a node (section 6.1, ADR 0011) and keeps probing and balancing. Its health decisions are its own: they change no replicated state, and they reach the primary only as `status_report.edge_backends`.
- **TLS.** `acme` routes take certificates through `dns_challenge` (section 5.22), exactly as on a node; `tunnel` routes are served from the edge's tunnel.
- **Warm apps (M5b).** A `warm` app's route has one backend: the instance on the node that holds the app's lease (ADR 0174). A failover or a switchover replaces it in the next `desired_state` of the edge, which the edge applies like any change of backends; the old backend's requests finish as in "Leaving the rotation" above. So moving a warm app needs no DNS or tunnel change.
- **Draining routes.** A route with `draining_until` (section 5.11, ADR 0168) is balanced, probed and served like any other. It names the hostname of an app that went back to one replica and is now also served by that node's tunnel; the edge keeps it until a snapshot no longer carries it. A backend that leaves a draining route because the route itself left `spec.edge.routes` finishes its requests normally, as in "Leaving the rotation" above.

### 6.9 The mesh

The mesh is kernel WireGuard driven over netlink, with no `wg` or `wg-quick` process (ADR 0063, ADR 0144). In M5a it is a star: the edge has one peer per node, and every node has the edge as its only peer. From M5b voters also peer with each other, so a candidate for promotion reaches every voter directly (section 8.9): every two voters are peers unless both are witness-only, and a peer the star already has is not repeated. The two nodes of a warm volume both have the `node` role, so they are voters and already peers; warm replication adds only the `allow` entry of section 6.13.

- **Keys.** The machine generates its WireGuard key pair on its first M5a start, keeps the private key root-only next to its credential, and sends the public key in `hello`. A machine never receives another machine's private key.
- **Interface.** The node agent creates the WireGuard interface `quoro-wg0` with `address`, `listen_port` and `peers`, and keeps it equal to them on every apply. On every apply it puts the mesh firewall table (below) in place before it configures any peer, and configures no peer while it cannot. When `spec.mesh` goes away, it removes the interface before the table. A peer that disappears from `peers` is removed. On the edge, the kernel routes each node's guest subnet to that node's peer through `allowed_ips`.
- **Firewall.** On a node, traffic that arrives on the mesh interface is dropped unless it comes from an `allow` entry's `source` and goes to its `address` and `port` over TCP. The mesh carries no traffic between guests, and none to the host's own services except, on a voter, the vote listener: TCP to its own mesh address on `spec.votes.port`, from the mesh address of each voter with the `node` role in `spec.votes.voters` (section 8.9). That rule follows from `spec.votes` and is not repeated in `allow`. From M5b the target of a warm volume also admits the replication listener: TCP to its own mesh address on the warm entry's `port`, from the source's mesh address, through the `allow` entry the primary sends it (section 6.13). The isolation of guests from each other (ADR 0084) is unchanged.
- **New connections.** On every machine with `spec.mesh`, the edge included, a new connection leaves through the mesh interface only when one of that machine's own root processes opens it: nothing forwarded opens one there.
- **Version.** The rules of this section, the "New connections" bullet included, are version 2 of the mesh rules. A machine reports the version its node agent applies in `hello` (`mesh_rules`, section 5.1).
- **NAT.** A machine without an `endpoint` in the other side's peer entry dials the peer that has one and keeps the tunnel open with `persistent_keepalive_s`. WireGuard follows a peer whose address changed (roaming), which also covers a node whose address changed after a reboot.
- **Endpoints (ADR 0144, ADR 0266).** The primary takes a machine's `endpoint` from the source address of its session, with `listen_port` as the port: for the edge from M5a, and for every voter from M5b. When that address is loopback or unspecified, the machine runs on the primary's host, and the primary takes the first non-loopback IPv4 host of `welcome.primary_urls` instead. A machine whose session the primary has not seen since it started has no endpoint. The primary keeps endpoints in memory only and does not detect NAT. A voter behind NAT therefore gets its NAT's public address as its endpoint: handshakes sent there fail, which is harmless, because WireGuard follows the voter's real address once the voter's own keepalive arrives, as long as the other side is reachable. Every peer entry between two voters carries `persistent_keepalive_s` 25.
- **Voters that cannot reach each other.** Two voters that both have no endpoint get no peer entry for each other. Two voters that are both really behind NAT get entries that never carry traffic, and the primary cannot tell that case apart. In both cases a candidate on one of them cannot count the other's vote (section 8.9), and the two cannot replicate a warm volume to each other: a warm copy between them stays `syncing` or goes `failed` (`openapi.yaml`, `App.warm`).
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

### 6.12 Voters and the witness

From M5b every machine with the `node` or `witness` role is a voter (ADR 0170). An edge-only machine is not, and one machine has one vote whatever its roles. What a voter does is section 8.8; this section says what the primary sends it and what a witness runs.

- **Roles in desired state.** A machine learns its roles from its desired state: `spec.witness` is present on a machine with the `witness` role, and `spec.votes` on every voter. A machine gains or loses the `witness` role through `POST /api/v1/nodes/{node_id}/roles` without enrolling again, and acts on the next `desired_state`. A machine whose desired state loses `spec.votes` stops answering `support_request` and votes, and keeps its vote state (section 12) until it gets `spec.votes` again or is removed.
- **A witness-only machine** (enrolled with `roles: ["witness"]`, or an edge that gained the role) runs the node agent with no engine and starts no VM, so it needs no KVM, like an edge-only machine (section 2.3). It gets `vms` and `routes` as empty lists, no `network`, and `spec.mesh` with its peers. It answers `support_request`, serves votes on the mesh (section 8.9) and stores only its vote state: no volumes, no images and no application data. The primary assigns it a mesh address when it enrolls or gains the role.
- **The voter set** is every enrolled machine with the `node` or `witness` role that is not `removed`, connected or not. A node declared lost (section 8.3) stays a voter until it is removed.

**`spec.witness`** (optional; on a machine with the `witness` role): an empty object in M5b. Its presence is the role.

**`spec.votes`** (optional; on every voter):

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `version` | integer | yes | The voter-set version: the change-log `seq` of the entry that last changed the voter set (section 8.10) |
| `port` | integer | yes | TCP port of the vote listener on the voter's mesh address; 51821 unless the operator changed it |
| `voters` | array of object | yes | Every voter, this machine included: `{ "node_id": <nod_>, "roles": [...], "mesh_address": <address or null> }` |
| `listen` | boolean | no | `true` once votes are open (below): the voter serves votes on `port` (section 8.9). Absent until then; the primary never sends `false` |

```json
{
  "witness": {},
  "votes": {
    "version": 3120,
    "port": 51821,
    "listen": true,
    "voters": [
      { "node_id": "nod_01JC9Z0000000000000000000Q", "roles": ["node"], "mesh_address": "10.43.0.2" },
      { "node_id": "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP", "roles": ["node"], "mesh_address": "10.43.0.3" },
      { "node_id": "nod_01JCA0A1B2C3D4E5F6G7H8J9KM", "roles": ["edge", "witness"], "mesh_address": "10.43.0.1" }
    ]
  }
}
```

The example shows the two fields as they appear in `spec` on a machine that is both edge and witness.

**Opening votes.** The primary sets `listen` in every voter's `spec.votes` once every voter with the `node` role that is not `removed` has a `nodes.mesh_rules` of 2 or above (section 5.1), and leaves it out otherwise. It checks this at its start, a promoted primary's included, and again whenever a `hello` changes a node's `mesh_rules` or the voter set changes (section 8.10), and sends every voter a new desired state when the result changes. A machine with the `node` role that has never connected, or whose last `hello` reported a lower version or none, keeps votes closed until it reports version 2 or is removed (section 10). The primary follows each machine's last `hello`: a node agent replaced by one that applies a lower version runs those rules while votes stay open, until its `hello` reaches the primary and the voters apply the new desired state, and for as long as it cannot reach the primary. Replacing a voter's node agent with one older than version 2 while votes are open is not supported. This is a stated limit. While `listen` is not `true` a voter serves no vote (section 8.9), and it still answers `support_request` (section 5.27).

### 6.13 Warm replication

From M5b an app with `ha_policy: warm` keeps a second copy of its volume on another node, its warm node, at most a few intervals behind (ADR 0020, ADR 0172 to ADR 0177). The node that holds the app's lease is the **source**: it runs the app, snapshots the volume every `interval_s` and ships what changed. The warm node is the **target**: it holds the last complete copy and the app's prepared image (`standby`, section 5.11), and starts nothing until a failover or a switchover grants it the lease. The primary sends both sides `spec.volumes[].warm` (section 5.11). A node that snapshots with `pause_copy` is never either side (ADR 0172). Only a reported `pause_copy` counts: a warm node that has not reported a method yet (`volume_snapshot` absent, for example before its first `hello` after an upgrade) keeps its place, while a new warm node is picked only among nodes that report `reflink` (`openapi.yaml`, `WarmCopy.node_id`, ADR 0287).

**Snapshots and sequence numbers.**

- Each complete copy of a warm volume has a `snapshot_seq`. The source numbers each snapshot one above the newest copy the target completed, so a number names one complete copy, and a node that becomes the source numbers its snapshots after the copy it started from. The first snapshot of a volume is 1. A snapshot that never completed on the target is discarded, and its number may be used again.
- The source keeps its **base**, the newest snapshot the target confirmed, until the target confirms a newer one; then the base moves and the old one is deleted. After a `root_mismatch` answer the source deletes it and has no base until the target confirms a full copy ("A cycle" below, ADR 0248). It also keeps the snapshot it is shipping. Both survive a node-agent restart: the node records them in its store (section 12), and the start-up sweep keeps them (`engine-trait.md` section 3.16).
- **A cycle.** The source takes a snapshot `next`, which the engine takes after a guest flush of the volume (`engine-trait.md` section 3.16, ADR 0282). A cycle whose flush failed still ships its snapshot. Then the source asks the engine for the ranges that changed since the base (`volume_changed_ranges`, `engine-trait.md` section 3.22): the ranges whose extents no longer share physical blocks with the base's (ADR 0172). When there is no base (the first copy, a new target, a base lost to a crash or dropped after a `root_mismatch`) or the engine cannot compare extents, the source ships every allocated range of `next`, a full copy. Then it streams `next` to the target. A failed cycle keeps the base, deletes `next` and runs again at the next interval, and `status_report.warm` carries the error. The one exception is a `root_mismatch` answer: the target's copy and the source's base no longer match, so every later cycle from that base would fail the same way. The source therefore treats its base as lost, as after a crash: it deletes the base and `next`, and the next cycle ships a full copy (ADR 0248). The app's live volume is untouched.
- **Interval.** `interval_s` is 30 by default, at least 5 and at most 3 600 (ADR 0173). The lab measurements behind the default: the extent comparison found exactly the written blocks over 50 cycles in about 20 ms, and a cycle's cost follows the changed bytes and the volume's extent count, not the interval (`docs/research/m5b-warm-and-leases.md`). The source reports the extent count in `status_report.warm`.

**The stream.**

- **Transport.** One TCP connection from the source's mesh address to the target's mesh address on `port`, through the mesh (section 6.9); the target's firewall admits it only from the source's mesh address (the `allow` entry, section 5.11). HTTP/1.1, `POST /v1/replicate`, one stream per request: the request body is a sequence of frames and the answer is JSON. WireGuard authenticates and encrypts the link; the stream adds no TLS and no credential of its own.
- **Frames.** `[u8 type][u32 BE length][payload]`, as on the terminal exec channel (ADR 0126): one header, then blocks and zero ranges in increasing offset order with no overlap, then one manifest.

| Type | Frame | Payload |
| --- | --- | --- |
| 1 | header | JSON: `app_id`, `volume_id`, `lease_id`, `fencing_token`, `snapshot_seq`, `base_seq` (the target's copy the ranges apply to, or null for a full copy), `taken_at` (RFC 3339, when the source took the snapshot) and `size_bytes` (the volume's size, `size_gb` GiB) |
| 2 | block | `offset` (u64 BE), `length` (u32 BE, 1 to 1 048 576), `blake3` (32 bytes, the BLAKE3 hash of the data), then `length` bytes of data. The range lies inside one cell |
| 3 | zero range | `offset` (u64 BE), `length` (u64 BE): the range reads as zeros in the snapshot; the target writes zeros or punches a hole |
| 4 | manifest | JSON: `block_count` (blocks and zero ranges sent) and `root_hash` (64 lowercase hex) |

- **Cells and the root hash.** A volume is divided into cells of 1 MiB at offsets `k × 1 048 576`; a volume's size is a whole number of GiB, so every cell is full. A cell's hash is the BLAKE3 hash of its 1 MiB. The **root hash** of a copy is the BLAKE3 hash of the concatenation of every cell's hash in offset order. Each side keeps the cell hashes of its copy (the source those of its base and of `next`, the target those of its complete copy) and updates only the cells a cycle touched, so neither side reads the whole volume per cycle. A side that lost its cell hashes computes them again by reading the copy once. The hashing and IO code is the chunk engine's (ADR 0023); the blocks are not FastCDC chunks, because they apply in place (ADR 0173).
- **The target's checks**, before it writes anything:
  - it is the target of `volume_id` with the same `lease_id` in its desired state, and the connection comes from `peer_address`;
  - `fencing_token` is at least the higher of the warm entry's `fencing_token` and the highest token it has seen in a stream of this volume;
  - `base_seq` is null, or names its complete copy, or, on a returning source, one of the snapshots it kept (section 9);
  - `snapshot_seq` is above `base_seq`, and for a full copy above its complete copy;
  - no other stream of this volume is open, and the target is not resolving its copy (a copy left pending by a crash or a failed swap, whose hash reads the whole volume; "Applying" below, ADR 0273).

  A stream that fails a check is refused at once with its reason.
- **Applying.** The target writes into a staging file: a reflink clone of the copy `base_seq` names, or for a full copy an empty sparse file of `size_bytes` (`engine-trait.md` section 3.22). It checks each block's `blake3`, writes it, and updates the cell hashes. At the manifest it checks `block_count`, computes the root hash of the staging file from its cell hashes and compares it with `root_hash`. If they are equal, it syncs the staging file, renames it over its copy in one step, and answers `complete`. Otherwise it deletes the staging file and refuses with `root_mismatch`, and its complete copy is unchanged. So the target never holds a half-applied copy: its copy is always one complete snapshot of the source.
- **Answer.** `{ "status": "complete" | "refused", "snapshot_seq": <the target's newest complete copy, or null>, "reason": <string> }`. `reason` is present on `refused` and is one of `stale_token`, `wrong_lease`, `not_target`, `not_newer`, `base_mismatch`, `root_mismatch`, `bad_frame` or `busy`. On `base_mismatch` the source ships again from the copy the answer names if it still holds that snapshot, and as a full copy otherwise. On `root_mismatch` it deletes its base, and its next cycle ships a full copy ("A cycle" above, ADR 0248). On `busy` the source keeps its base and drops only the snapshot it was shipping; the answer's `snapshot_seq` names the copy the target's store names as complete, which a copy being resolved may replace, so the source does not act on it (ADR 0273).
- **Tokens.** A stream is a write to shared storage, so it carries the lease's token, and the target refuses a lower one (section 8.6, ADR 0173). The target records the refusal and reports it (`rejected_token`, `rejected_at`). So a source that lost its lease while it was paused or cut off cannot overwrite the copy that a failover started from.

**What the primary derives.** From the two sides' `status_report.warm` entries the primary shows `App.warm` (`openapi.yaml`):

- `last_snapshot_seq` and `last_shipped_at` are the target's `snapshot_seq` and `completed_at`;
- `lag_s` is the time since the source took that copy's snapshot (`snapshot_taken_at`), on the primary's clock;
- `state` is `syncing` until the target holds a first complete copy, `in_sync` while `lag_s` is at most twice `interval_s`, `lagging` above that, and `failed` while either side reports an error or is not connected.

The switchover's condition, "in sync with a lag below two intervals" (ADR 0177), is that `in_sync`.

**Failover (ADR 0175).** When the primary grants the target the lease (`openapi.yaml`, `failoverApp`), it sends the target a `desired_state` in which this node is the source of the volume and the standby VM is running with the new token, and the other node is the target. Before it creates the VM, the node keeps its complete copy as its base: a snapshot numbered with that copy's `snapshot_seq`, the common base of the resync (section 9). Then it creates and starts the VM on its copy (section 6.4). Writes the old source made after its last complete copy are not on the target; they are kept, never dropped, when the old source returns (section 9).

**Planned switchover (ADR 0177).** The primary (`openapi.yaml`, `switchoverApp`):

1. refuses the call unless `App.warm.state` is `in_sync` and both nodes are connected;
2. sends the source `command` kind `switchover_freeze` (section 5.17): the source asks the guest to flush the volume (ADR 0282), pauses the VM, ships a final snapshot and keeps the VM paused;
3. sends the source `lease_revoke` with reason `moved` and waits for its `command_result`: the VM is stopped and the lease revoked;
4. grants the target the lease with a higher token and sends both nodes their new roles, as for a failover;
5. switches the edge's backend to the new instance in the edge's next `desired_state`; the old backend leaves the route, and the edge lets its requests finish (section 6.8);
6. emits `app.switched_over`.

If step 2 fails, or the primary gives up before step 3, it sends `switchover_thaw`: the source resumes, nothing moved, and the primary emits `app.switchover_failed`. Once the revoke of step 3 was answered, the switchover goes forward only: the final snapshot is on the target, and the old source's volume holds the same data, so it becomes the target with no quarantine (section 9). If the flush before the pause succeeded (section 5.17, ADR 0282), that data includes every write the app completed before the flush. Any write the guest had not written back to the volume when it was paused stayed in its memory: one made after the flush or, if the flush did not succeed, one the guest's own writeback had not reached yet. No stop resumes a VM that `pause` froze (`engine-trait.md` section 3.8, ADR 0270), so that memory is lost when the VM is stopped. Such a write is in neither copy, and the two copies stay the same. A primary that restarts during a switchover thaws a source that reports `frozen: true` (section 5.17); a frozen source cut off from the primary fences when its lease ends.

## 7. Change-log follow, key custody and promotion

Every change to cluster state is a numbered entry in an append-only, hash-chained log (D-0013, D-0015). Every machine with the `node` role follows it and holds the cluster keys sealed to a key of its own, so that an operator can promote it when the primary is gone (D-0067, ADR 0179 to ADR 0182). In M5b a follower serves no reads and forwards no writes: `cp_status` `replica` and read serving stay for M6 (ADR 0179).

### 7.1 Follow

- **Which machines follow:** every machine with the `node` role, the primary's own host included. Nothing in this protocol distinguishes a future primary from any other follower, which is what keeps promotion cheap. Edge-only and witness-only machines do not follow and get no `log_entries`.
- **Delivery (ADR 0227):** after the `desired_state` of a session (section 4.2), the primary sends `log_entries` from the node's `log_seq + 1`, up to 500 entries per message, and then each new entry as it is written. It keeps at most one `log_entries` message per session unacknowledged, and sends the next once the `log_ack` of the previous one arrives. Each message carries `head_seq`, the primary's newest `seq`, so the node knows how far behind it is (section 5.14).
- **Verification:** the node verifies each entry's `hash` over the RFC 8785 serialization of the entry without `hash`, and that `prev_hash` equals the previous entry's `hash`, then stores the entries durably (fsync) and only then sends one `log_ack` for the highest one. `hello.log_seq` likewise reports only entries stored durably (ADR 0261). A message whose entries do not verify is a primary bug: the node stores nothing from it and answers `error` code `bad_message`, and the primary sends again from the node's last ack.
- **Entries a node can receive (ADR 0302):** the primary never writes a change-log entry that cannot be sent alone in one `log_entries` message and verified by a node. That is an entry whose serialization alone, with the fields of the `log_entries` message around it, exceeds the message limit of section 3 (1 MiB), or one whose payload, as read back from the text the primary stores, has no RFC 8785 serialization, such as a number that reads back as an integer above 2^53. The write that would add such an entry fails like any other write error, so nothing it carries enters the log. The primary checks with its own JSON parser, and nodes verify with the same one: floats parsed exactly, each as the IEEE 754 double nearest to its text (ADR 0315). A node on an older parser that rounds floats only approximately can fail to verify an entry that holds a float. It answers `bad_message` and stays at its last acknowledgement until it is upgraded, so nodes are upgraded promptly after the primary. An audit record's entry is not refused for its `params`: the record keeps their digest and size instead (audit-log-format.md section 4.1, ADR 0306).
- **The node's mark (ADR 0261):** a node's log can lose entries while its identity and custody key survive, because section 12 keeps those outside the SQLite file: `node.db` deleted after corruption, or the host rolled back to a disk snapshot. The node therefore keeps a high-water mark of the highest `seq` it has acknowledged, in a file next to `custody.key` (section 12). It writes and fsyncs the mark before it sends the `log_ack` or the `hello` that reports a higher `seq`. The mark moves down only as "Refollowing" below says. `quoro cluster promote` refuses while the log is below the mark (section 7.3).
- **On `hello` (ADR 0227):** the primary compares the node's `log_seq` and `log_hash` with its own entry at that `seq`. If they differ, or the node's `log_seq` is above the primary's newest `seq` (a node that followed an older primary further than the promoted one, section 7.3), or below the newest acknowledgement the primary holds for that node, in memory or in `nodes.log_seq` (a log that went back, ADR 0261), the primary sends `error` with code `log_diverged`, non-fatal, before any `log_entries`. The node moves its local copy aside as a quarantined copy, never deleting it, because divergent local writes must never be silently dropped (D-0022), and refollows from `seq` 1. In the same step the primary sets its in-memory acknowledgement for that node and `nodes.log_seq` to 0, so that the node's next `hello` does not read as a log that went back.
- **Refollowing (ADR 0261):** a node whose log the primary declared diverged is refollowing from the quarantine until its new log reaches the `head_seq` of the first `log_entries` message it receives after the quarantine. That primary holds every entry replicated over the machines with the `node` role (section 8.4), so the node then holds them all again. At the quarantine, the node first writes and fsyncs its mark as 9007199254740991 (2^53 - 1, the highest `seq` the change log carries) and records "refollowing, head unknown" in its local store, and only then moves the log aside. An agent that finds its mark at 2^53 - 1 at start, with no refollowing state recorded, stopped between those two writes, takes the state as "refollowing, head unknown", and finishes moving its log aside before it follows (ADR 0285). Once it learns the `head_seq` it refollows to, it records that `head_seq` and lowers its mark to it, before it stores any new entry (ADR 0280). A `node.db` lost while the node refollows therefore leaves a log below the mark. The refollowing state survives an agent crash at any point (section 12). While it lasts, `quoro cluster promote` on that node refuses, as it does for a quarantined log (section 7.3).
- **What neither check catches (ADR 0261):** three cases are stated limits of the mark and of the primary's check. A host rolled back as a whole to a disk snapshot takes the mark back with its log, so neither the node nor the primary can tell. A primary that crashed since the node's session began lost the acknowledgements it held only in memory, because it writes `nodes.log_seq` only at session start and end (ADR 0227), so the node's mark is then the only guard. A primary host reverted to a VM snapshot is unsupported: it comes back at the same term with a shorter log, its nodes refollow from it a history that may lack replicated entries, and promoting or refollowing from it can lose them (ADR 0280). Section 7.3 says how a promotion treats such hosts.
- **Lag (ADR 0227):** the primary keeps each node's newest acknowledged `seq` in memory and reports it as `Node.log_seq`, with `Node.log_lag`, its own newest `seq` minus that value (`openapi.yaml`). It writes `nodes.log_seq` only together with the writes it already makes to the node's row when a session starts (from `hello.log_seq`) and when it ends, and when it answers `log_diverged` (above). Every write to `nodes` is itself a change-log entry that every follower must then receive and acknowledge, so writing each acknowledgement would never settle.
- **Secrets in the log:** `app_env` values, webhook secrets, provider secrets and every machine's tunnel run token (`node_tunnels.token_enc`) are stored encrypted with the cluster data-encryption key (AEAD, `db-schema.sql`). Every machine with the `node` role holds that ciphertext and a seal of that key that root on the machine can open at any time (section 7.2). Each such machine can therefore read every one of these secrets, not only after a promotion (ADR 0320). Section 11 says what follows from that, and what to rotate when such a machine leaves. From M5b the hashes in `node_credentials` travel in the log too (`db-schema.sql` migration 0012, ADR 0226), so a promoted primary accepts every node's session. So do `api_tokens.token_hash`, `agent_keys.key_hash` and `node_join_tokens.token_hash` (ADR 0317). A SHA-256 of a random secret gives a follower no usable credential, and personal API tokens and agent keys then still work after a promotion, which a recovery run by agents needs. Session and access-token hashes stay on the primary (`sessions` and `access_tokens` are local).
- **The repository key stays out of the log:** `backup_repositories.repo_key_enc` is in no change-log payload, neither a `create` nor an `update`. Every machine with the `node` role can open its own seal of the data-encryption key (section 7.2), so the wrapped key in the log would let anyone holding a follower's disk read every backup, for good, because the repository key never rotates. A promoted primary therefore starts without the key. The owner restores it from the recovery key, whose wrap is in the repository's config object, with `restoreBackupRepositoryKey` (`backup-format.md` section 4, ADR 0322). Re-keying a repository is not served yet (section 7.3, step 4).

### 7.2 Key custody

Every machine with the `node` role holds the cluster keys sealed to a key of its own (ADR 0078, read by ADR 0180, and ADR 0181). Edge-only and witness-only machines get no seal.

- **Custody key.** At its first M5b start a machine with the `node` role generates an X25519 key pair, its custody key, and keeps the private half next to its credential as `custody.key`, mode `0600` (section 12). It is neither the WireGuard key nor the node's Ed25519 key, so no key serves two protocols. The machine sends the public half in every `hello` (`custody_public_key`, section 5.1), and in its enroll request when it enrolls with the `node` role (section 2.3).
- **Binding (ADR 0231).** The primary stores the first custody key a machine reports in `nodes.custody_public_key` and never replaces it from a later report. A `hello` with a different key is answered with `error` code `custody_key_changed`, non-fatal. The session goes on, the primary sets `nodes.custody_key_changed_at`, writes the audit record `node.custody_key_changed` once (`audit-log-format.md` section 3.9), and makes no seal to the new key; `Node.custody` shows `key_changed`. A machine loses its custody key only with its data directory, which also holds its credential, so the way back is to remove the node and enroll the machine again.
- **Seals.** The primary seals each key it holds to each such machine and keeps the seals in `node_key_custody` (`db-schema.sql` migration 0012). The table is replicated, so every follower holds every seal and can open only its own:

| `key_kind` | Plaintext | `key_id` |
| --- | --- | --- |
| `signing` | The 32-byte Ed25519 seed of a cluster signing key | Lowercase hex SHA-256 of its 32-byte public key, as in `signing_key_rotate` (section 5.21) |
| `dek` | The 32-byte cluster data-encryption key | The 32 lowercase hex characters of the 16-byte key id that starts every blob the key encrypted. One seal per key id the primary holds |
| `tls` | The RFC 8785 serialization of `{"cert_pem": ..., "key_pem": ...}`: the primary's certificate chain and private key, as PEM text | Lowercase hex SHA-256 of the leaf certificate's DER, the fingerprint every node pins (section 2.2) |

- **Construction (ADR 0231).** HPKE (RFC 9180) in base mode, with DHKEM(X25519, HKDF-SHA256), HKDF-SHA256 and ChaCha20-Poly1305 (KEM `0x0020`, KDF `0x0001`, AEAD `0x0003`). `info` is the ASCII text `quoro custody v1`, `cluster_id`, `node_id`, `key_kind` and `key_id`, each followed by one zero byte, and the AAD is empty. `info` binds a seal to its cluster, machine, kind and key, so a seal cannot be presented as another one. `sealed` holds the 32-byte encapsulated key followed by the ciphertext and its tag.
- **When.** The primary seals every key in use to a machine when it first stores the machine's custody key, and seals a new key to every such machine before it uses the key (a signing key before its rotation starts, section 7.5). A machine's first seals therefore come in the log after every entry the primary wrote before it stored that key. The log is hash-chained from `seq` 1, so a machine that holds its seals holds every such entry, the write that ends an `expiring` lease's renewals included (section 8.4, ADR 0261). It deletes a machine's seals when the machine is removed (section 10), and a key's seals when the key is retired. These writes go through the change log like any other. At its first M5b start the primary seals its keys to every machine that has reported a custody key.
- **Status.** `Node.custody` (`openapi.yaml`) is `sealed` when every key in use is sealed to the machine, `pending` while a seal is missing, `none` before the machine reports a custody key, and `key_changed` after a refused change. It is null for a machine without the `node` role. Only a `sealed` machine can be promoted.

### 7.3 Promotion

An operator promotes a machine with the `node` role when the primary is gone (ADR 0182, ADR 0232). As root on that machine:

```
quoro cluster promote [--dry-run] [--ack-fence] [--accept-loss] [--statement <text>]
quoro cluster promote --abort
```

Before it changes anything, the command refuses when:

- the machine does not have the `node` role, or its agent has no identity;
- this host already has a control-plane database or a control-plane unit. A demoted primary's data must be moved aside first (section 7.4);
- the agent's followed log is empty or quarantined;
- the agent's followed log is below the agent's mark, or the agent is refollowing (section 7.1, ADR 0261). `--accept-loss` lifts these two refusals (below);
- a `restore-pending` marker exists on this host (`backup-format.md` section 8.2, ADR 0285). The operator finishes the restore, or deletes the marker after checking that no `quoro restore` runs on the host;
- the newest `welcome.schema_version` the agent stored is above the newest schema version this binary knows;
- it cannot open a seal of every key in use: the current signing key and, during a rotation, the next one (section 7.5), every data-encryption key id, and the TLS key.

Then it:

1. **Shows the log position:** the agent's last verified `seq`, the `head_seq` the primary last reported with its time, and the difference. Entries the old primary wrote and this machine never received are lost to the cluster. That holds only for entries that some machine with the `node` role never acknowledged: a machine that passes the refusals above holds every entry the old primary counted as replicated (section 8.4, ADR 0261), unless it promotes with `--accept-loss`. With `--dry-run` the command stops after the vote pre-check of step 2, having changed nothing, so an operator can compare candidates.
2. **Takes the term.** It asks every voter for a term `T` over the mesh (section 8.9): first with `check`, then for real. A voter that does not listen, because votes are closed (section 6.12), gives no answer. `T` is one above the highest term this machine has seen or that any vote answer carries, `check` answers included (ADR 0261). When a `check` answer carries a term at or above `T`, the command raises `T` to one above that term and checks again at the new `T` before any real vote. When `T` would be above 2^53 - 1, the term ceiling (section 8.8), the command stops there, before any real vote request and so before the freeze, and changes nothing. A rerun of the command starts from the highest term it then sees (ADR 0280). Only the command uses that term: the agent stores a term only when it grants a real vote. Before the first real vote request, it freezes its own agent (below). It goes on only when a majority answers `granted`; otherwise it prints each voter's answer and stops, and no voter moved, because only the pre-check ran.
3. **Waits for the old primary's grants:** until the latest receipt time plus `lease_wait_ms` over the granting answers (section 8.9), printing the time left. `--ack-fence` skips this wait (below).
4. **Builds the database (ADR 0228, ADR 0229, ADR 0297).** In a new database file it creates the schema of this binary (`db-schema.sql` and every migration), then applies the followed entries in `seq` order, folded per entity (ADR 0317):
   - In memory, each entity has one row. A `create` replaces it with the payload's row. An `update` merges the payload over it, so a field absent from the payload keeps its value. A `delete` removes it and ignores its payload.
   - An `update` for an entity that has no row merges into an empty row, so a log that holds an update before its create still rebuilds.
   - Once every entry is folded, an entity that never received a `create` stops the build, with an error that names the entity and the `seq` of its first entry. No row made only of updates is ever inserted.
   - The final rows are inserted in one transaction with foreign keys deferred. The command applies the schema's `ON DELETE CASCADE` deletes itself, removing with each deleted row the rows its cascade would have removed.
   - Each entry is still copied into `change_log` in `seq` order.

   A whole-App payload's `warm_node_id`, `warm_node_pinned` and `warm_interval_s` write those columns, and a partial app payload, which omits them, leaves them as they are, so the new primary keeps each `warm` app's warm node, pin and interval (ADR 0286). It builds from exactly the frozen log: it checks that the freeze held, and stops if the log has changed (ADR 0261). The agent may have quarantined or refollowed its log between the refusals above and the freeze, so the command also runs every refusal on the log again against the frozen log, and stops when one fails: the log is empty or quarantined, below the agent's mark, or the agent is refollowing. `--accept-loss` lifts the last two here as above (ADR 0280). It verifies the change-log chain and the audit chain (`audit-log-format.md` section 6) and stops on a break.
   - A `create` payload written before a column existed lacks that column, and the row takes the column's default. An `update` that lacks a column keeps the row's value; a column's default applies only to a `create` (ADR 0229, narrowed by ADR 0297). Every migration therefore gives a new column a default or allows NULL. A payload field with no column in this schema is ignored, such as a field the primary derives on read (`App.warm`) or a column a later migration dropped.
   - Each `entity_type` names one table (`db-schema.sql` and its migrations). An `update` or a `delete` finds its row by `entity_id`, which for a table with a composite key joins the key's columns with `/`.
   - Tables marked LOCAL in `db-schema.sql` start empty: browser sessions and access tokens, so users log in again; webhook delivery attempts; interactive sessions; and the vulnerability mirror, which the new primary fetches again (section 6.11).
   - `meta` is not in the log. `cluster_id` comes from the agent's identity, `term` is `T` and `schema_version` is this binary's. `webhook_cursor` is the `seq` of the newest event in the rebuilt event log, so the new primary delivers only events it writes itself. An event the old primary never delivered is lost rather than delivered twice: a second delivery of `incident.opened` to a webhook with `wake` would issue a second grant.
   - `audit_exports` is in the log, so the off-site export resumes from its row as after a restore (`audit-log-format.md` section 8.5).
   - `backup_repositories.repo_key_enc` is not in the log (section 7.1), so the rebuilt row holds an empty BLOB there. It is the one column a `create` payload lacks that takes no default. An empty `repo_key_enc` means that this primary does not hold the repository key; a wrapped key is never empty (`db-schema.sql`, encrypted columns). Until the key is back on this primary:
     - `BackupRepository.repository_key_present` is false (ADR 0322);
     - `createBackup` and `restoreBackup` answer `conflict`, as when the cluster has no repository, and so does `rotateBackupRecoveryKey`;
     - a scheduled backup, of a volume or of the control-plane database, is recorded `failed` at once with the error `repository key absent` and emits `backup.failed`; the next interval tries again;
     - the backup before an upgrade fails, so `backup-format.md` section 7.2 step 4 applies;
     - garbage collection does not run, because it reads the manifests with the key (`backup-format.md` section 9).

     The owner restores the key with `restoreBackupRepositoryKey`, which takes the recovery key (`backup-format.md` section 4, ADR 0322). Restoring writes only this primary's row, never the log, so after a further promotion the key is absent again. Re-keying a repository is not served yet (ADR 0324). Until it is, an owner who has lost the recovery key takes no new backup after a promotion, and the backups already taken stay readable only with that key. An operation that replaces the repository with a new one, with a new key, is planned after M5b.
5. **Moves the primary status.** In one change-log write at term `T` it clears `cp_status` on the old primary's node, sets `cp_status = 'primary'` on its own node and records `cluster.promoted` (`audit-log-format.md` section 3.9). With `--ack-fence` the same write declares the old primary's node lost, as `declareNodeLost` does (section 8.3). With `--accept-loss` the same write marks every `expiring` lease row unsettled (section 8.4, "The mark").
6. **Installs the keys and starts.** It opens its seals and writes the keys where a primary keeps them, and prepares the host as the installer does for a primary: the control plane's system user, its data and configuration directories, and its unit. It then starts the control plane, ends the freeze and points this machine's agent at it, as `quoro node set-primary` does (section 5.10). The certificate is the old primary's (ADR 0181), so the pin holds.

**The freeze (ADR 0261).** Before any real vote request (not `check`), the command freezes its own agent through a local call. The freeze is stored in the agent's local store (section 12), so it survives an agent restart. While it lasts, the agent ends its session with the primary, does not redial (section 4.4), stores no more `log_entries` and keeps its log as it stands. The freeze does not depend on the agent's own vote, which the agent may refuse while it still supports a primary. Without the freeze, the agent could acknowledge an entry to the old primary after the command read its log, and the old primary would count that entry replicated while the new database lacks it. A frozen agent acknowledges nothing, so it holds back the end of every `expiring` lease's renewals in the cluster, and every grant of a marked row, until the freeze ends (section 8.4, ADR 0280).

**`--abort` (ADR 0280).** `quoro cluster promote --abort`, run as root on the same host, ends a freeze that a killed or interrupted command left, a reboot included. It is refused while a promotion runs on the host. The agent then dials again as below. When the command stopped after it wrote the new database (steps 4 and 5), a rerun is refused because the host already has a control-plane database (above): the operator first moves that database aside, as for a demoted primary (section 7.4, ADR 0285).

**If the promotion stops** after the freeze, the freeze ends and the agent dials again:

- If its own agent did not grant the real vote, the agent resumes following.
- If its own agent granted it, the agent is at term `T`. It answers the old primary's `welcome` with `stale_term`, which demotes that primary (section 8.8). The promotion must then be finished or retried at term `T` or above. The `check` before the real vote keeps this case rare.

Taking `T` from the vote answers matters here: a promotion that stopped after its real votes leaves voters at its term, and they refuse with `stale_term` every other candidate that asks for that same term.

**`--ack-fence` (ADR 0236).** The flag requires `--statement`, the operator's statement that the old primary's host is down and stays down, which the record keeps as its `justification`. It skips step 3 only, and declares the old primary's node lost in step 5, so the `local` leases that node held may be granted elsewhere. Declaring the node lost raises the token of every lease it holds, so before any real vote request, and so before the freeze, the command checks those tokens against the cap: when one is already at 2^53 - 1, it stops with `token_exhausted` and changes nothing, with no term taken and no freeze (section 8.6, ADR 0325). It does not skip any lease's own gate: before it grants an `expiring` lease to another node, the new primary waits until that lease's gate has passed (section 8.4, ADR 0203), counted from its own first start with the lease's stored `duration_ms`. That is safe, because a primary raises `duration_ms` before a longer grant and sends that grant only once every machine with the `node` role holds the raise, this one included (section 8.4, ADR 0268). What the flag gives up is the case the voters' wait covers: an `expiring` grant the old primary made and this machine never received. A token issued in that grant is unknown to the new primary, which may issue the same token for the same lease again.

**`--accept-loss` (ADR 0261).** The flag lifts the refusals for a log below the mark and for a refollowing agent, for when no other candidate is left. The command then builds from the current log only, and every entry past that log's head is lost, replicated or not. The flag requires `--statement`; when `--ack-fence` is given too, one statement serves both flags. The statement says that no other candidate holds a newer log, and that the operator accepts that every entry past this log's head, replicated or not, is lost (ADR 0285). The record keeps the statement as its `justification` and sets `params.accept_loss` (`audit-log-format.md` section 3.9). Step 5 marks every `expiring` lease row unsettled, because the log may lack the writes that end a lease's renewals (section 8.4). Such a promotion may lack a raise of `duration_ms` too (ADR 0268), as may a control-plane restore, but both mark every `expiring` row, and a marked row never moves on a gate, only as a `local` lease does until its holder reports (section 8.4, "The mark"; `backup-format.md` section 8.2). A token issued in lost entries may be issued again to another node, as for `--ack-fence`.

**A host whose log went back unseen.** A host rolled back as a whole to a disk snapshot, or one whose acknowledgements the primary lost in a crash, passes the refusals above with a stale log (section 7.1). Promoting from such a host is a forced recovery by operator rule, so it needs `--accept-loss`. A primary host reverted to a VM snapshot is unsupported: promoting a node that refollowed from it can lose replicated entries (section 7.1, ADR 0280).

**A node enrolled in lost entries.** A node whose enrollment is in entries the new primary lacks is unknown to the new primary, which neither revokes its leases nor counts it in the required set of section 8.4. This is a stated limit, like the cases that neither check catches (section 7.1, ADR 0285).

**The new primary.** It starts at term `T` with no voter support (section 8.8), so it asks its voters before it grants or renews any `expiring` lease. Nodes and the edge find it through `welcome.primary_urls` (section 5.10) or an operator's `quoro node set-primary`, with the pin unchanged, and every machine that sees its `welcome` adopts the higher term (section 8.8). It sends each node a `desired_state` whose `rev` is above both its own `nodes.desired_rev` and the `stored_rev` of the node's `hello`, so no node ignores it as old (section 6.1, ADR 0234). A `warm` app whose lease expired is granted again only to its last holder, which has the newest copy; a promotion never moves it (ADR 0182).

### 7.4 The old primary

A primary learns that a newer term exists when a machine answers it with `stale_term` (section 8.8): a node that reached it after seeing the new primary, or a voter answering its `support_request`. A primary restored with `quoro restore --accept-loss` that has sent no `lease_grant` yet starts again at one above the error's `term` instead (`backup-format.md` section 8.2, ADR 0261). Any other primary then (ADR 0230):

1. stops granting and renewing `expiring` leases at once (section 8.8);
2. records the higher term in `meta` (`demoted_by_term`), so a restart keeps it demoted;
3. ends every node session with `error` code `not_primary`, fatal, and answers every later session upgrade and every API, panel and MCP request with `503` and code `not_primary`.

A node keeps redialing after `not_primary` and tries its other URLs (section 4.4), so it reaches the new primary. Nothing on the old primary is deleted, and it writes nothing more, grants nothing, and no node accepts its term. To make the host a node again, the operator runs, as root on it:

```
quoro node rejoin
```

Before it stops anything, the command checks that this host's control plane is no longer the primary: its database records `demoted_by_term`, or the host's agent has seen a term above the database's `term`. The second case covers a control plane stopped by hand before a promotion elsewhere, which never learned of the newer term. When neither holds it refuses and changes nothing, because run by mistake on a live primary it would take the cluster's control plane down. `--force --statement "<why>"` overrides the refusal. It is needed after a promotion that stopped once it had written its new database (`--abort`, section 7.3): that host was never demoted, and its agent has seen no term above the one of that database. A forced run records the statement, with the time, in the node agent's journal and in a file `rejoin-statement` inside the moved-aside copy (below).

Next, the command creates the moved-aside copy, a directory named with the term and the time, and writes a file `rejoin-started` in it before anything else, then the `rejoin-statement` of a forced run. It stops and disables the control-plane unit, when the host has one, and confirms that it is inactive. It then moves the control-plane data directory, with its database and its key files, into the copy as a quarantined copy, then the unit file with any local edit or drop-in, and writes `rejoin-done` in the copy last. It deletes nothing: the copy may hold writes the new primary lacks (ADR 0022), and the unit file keeps the operator's edits. A rejoin interrupted before `rejoin-done` leaves at most the unit stopped and disabled and some files not yet moved. Running it again finds the copy it started, the one with `rejoin-started` and no `rejoin-done`, so it never takes a copy left by an earlier rejoin for its own, and finishes the moves into it. Its check reads the database where it then is, in place or in that copy, and that copy's `rejoin-statement` lets a forced run's rerun go on without `--force`. A host that rejoined has neither a control-plane database nor a control-plane unit, so section 7.3 does not refuse it, and it can be promoted later like any machine with the `node` role. The host's agent kept running and reaches the new primary through its stored URLs. Its followed log is ahead of the new primary's or differs from it, so the new primary answers its `hello` with `log_diverged`, and the agent quarantines its copy and follows from `seq` 1 (section 7.1). Terminal and console sessions that were open on the old primary get no `session.end` record.

### 7.5 Signing-key rotation

An owner rotates the cluster signing key with `POST /api/v1/cluster/signing-key/rotate`, under the human-only scope `cluster:manage` (ADR 0154, ADR 0187, ADR 0233). One rotation runs at a time; another request answers `409` `conflict` with reason `in_progress` until it completes.

1. The primary generates the next key, writes it next to the current one in its key directory, seals it to every machine with a custody key (section 7.2) and records it in `signing_keys` as `next`, in one change-log write.
2. It sends `signing_key_rotate`, signed by the current key, to every machine that is not removed, of any role (section 5.21).
3. A machine persists the new pin and answers `command_result` `ok`. The primary records the key the machine pins in `nodes.signing_key_id`, from that answer and from `hello.signing_key_id` on every session, and signs each machine's messages with the key that machine pins.
4. Once every machine that is not removed pins the next key, the primary makes it the current one: in one change-log write it deletes the old key's row and its seals, records the next key as `current`, and writes the audit record `cluster.signing_key_rotated`. Then it deletes the old key file.

A machine that never returns holds the rotation open until an operator removes it (section 10); `Cluster.signing_key_rotation` lists the machines still on the old key. A machine that enrolls during a rotation receives the current key at enrollment and the next one on its first session. A promotion during a rotation opens the seals of both keys and the new primary goes on with the rotation where it was.

## 8. Leases and self-fencing

This section is the core of the contract. It exists because starting a stateful app on node B while it is still running on node A produces two diverging copies of the same data (D-0019).

The lease modes are decided (D-0047): `local` for HA policy `none` and `cold`, `expiring` (M5b and later) for `warm` and `hot`. The node-side states `held`, `uncertain`, `expired` and `revoked` are fixed.

### 8.1 Who arbitrates

| Period | Lease authority | Consequence |
| --- | --- | --- |
| M0 to M5a | The primary, in process | One control plane; a `local` lease survives its absence (D-0011) |
| M5b | The primary, while a majority of voters supports it (section 8.8, ADR 0169) | Same lease messages; `expiring` grants and renewals stop when the primary loses that majority, and a promotion cannot overlap the old primary's grants (D-0017) |
| M6 and later | An elected primary | Automatic failover; the same messages also carry `resource_type: primary` leases |

The voters do not see leases: they give the primary a short support window, and the primary grants only inside it. An external arbiter is not used in M5b (ADR 0169 keeps it as a later option). The external arbiter options are an S3/R2 conditional write or a Cloudflare Durable Object. Both are researched and both require the fencing token to stay (F-0037, F-0038, `docs/research/r2-conditional-writes.md`, `docs/research/durable-objects.md`): object storage has no server-side expiry, and a Durable Object restart can kill an in-flight write, so "the lease expired" is always inferred and always enforced by the node and by the token.

The same mechanism also serves primary election (D-0019). Leases with `resource_type: primary` appear when automatic failover exists (M6); in M0-M5 the only leases are `resource_type: app`.

### 8.2 Messages

| Message | Direction | Purpose |
| --- | --- | --- |
| `lease_grant` | control plane to node | Grant a lease, with mode, token and duration; also the answer to `lease_renew` |
| `lease_renew` | node to control plane | Ask to extend an `expiring` lease every `duration_ms / 3` |
| `lease_revoke` | control plane to node | End a lease: `moved`, `deleted`, `declared_lost`, `superseded` |
| `support_request` | control plane to voter | From M5b: ask for a support window, every `support_ms / 3` (section 8.8) |
| `support_grant` | voter to control plane | The promise not to support another candidate for a higher term until the window has passed |

### 8.3 `local` mode

Issued for the HA policies where no other copy of the app can start (`none`, `cold`, D-0047), so nobody is waiting to take the lease.

- It never expires because the session is gone or the control plane is unreachable. This is what keeps D-0011 true: an app keeps running when the panel is down.
- It ends only by:
  - `lease_revoke` with reason `moved`, `deleted`, `superseded`; or
  - an operator declaring the node lost (part of the `local` lease mode, D-0047): `POST /api/v1/nodes/{node_id}/declare-lost`, scope `node:declare_lost`, human-only and audited (ADR 0183). Declaring a node lost increments the lease's `fencing_token` (D-0019), so the old holder's token is stale even though the old holder never received a message.
- A `local` lease on a node whose session is gone is a normal state, not an incident. The primary reports the node `disconnected`, not `fenced`.
- **Granted at placement (M5a).** The primary grants a `local` lease on an app to the node that holds its volume when the volume is created, and records it in `leases`. A restore revokes the lease on the old node with reason `moved` and, once the new volume is `ready`, grants a new one to the target node with a strictly higher token (ADR 0147). A restore is refused while the old node is disconnected: an operator removes that node first (section 10), which increments the token. From M5b, while the app's HA policy is `warm`, the restore's write that names the target node `local` sets the row's unsettled mark, as R does, whatever the row's mode: the row can read `local` while the old holder still holds the lease `expiring`, between a conversion grant and the answer to its first renewal (ADR 0244). The same holds for any row that reads `expiring` (section 8.4, "The mark", ADR 0285, ADR 0288). While the lease row carries the mark, the restore's grant to its target (`restoreBackup`) waits until the row's newest write is replicated over the machines with the `node` role and the primary holds majority support; the restore is not refused for it. A failover (`failoverApp`) of such a lease answers `lease_held` while its holder is neither connected nor declared lost, and its grant waits in the same way (section 8.4, "Grants of a marked row", ADR 0280).
- **Released at deletion.** Deleting the app's attached volume, or the app, revokes the lease with reason `deleted`.

**Declaring a node lost (M5b, ADR 0183).** The operator states that a disconnected node is gone, so the apps it held may start elsewhere. In one transaction the primary:

1. increments the `fencing_token` of every lease the node holds, `local` and `expiring`, and records each one revoked in its own view (`leases.revoked_at`), with the node still named as the last holder;
2. sets `nodes.lost_at`; the node stays enrolled, and stays a voter until it is removed (section 6.12);
3. writes one audit record, `declareNodeLost`.

The call is refused with `409 conflict` while the node is connected, because the primary can then revoke over the session (section 5.13), and for a `removed` node. It changes nothing in `fence_failure`: a lease listed there stays ungrantable until an operator acknowledges that fence (section 8.5), so declaring a node lost never covers a failed fence the operator has not seen. An `expiring` lease of a lost node also keeps its gate (section 8.4): a new holder gets it only after the old holder's last grant has passed `T_grant + D + 2M`. When the node returns, its `hello` names leases with tokens lower than the primary's, and the primary revokes them with reason `declared_lost` before anything else (section 9); the primary clears `lost_at` once those revokes are answered. Cold restores of a disconnected node and a promotion that acknowledges the fence for the old primary's own node use this call (ADR 0182).

### 8.4 `expiring` mode timing

Used for `warm` and `hot`, where a second copy is ready and will start as soon as the lease moves (D-0047).

**Changing the mode (M5b).** An app's lease changes mode without changing holder when its HA policy changes. When the policy becomes `warm`, the primary sends the holder a `lease_grant` with `mode` `expiring`, the current token and a duration; when it leaves `warm`, a `lease_grant` with `mode` `local` and the current token, once every machine with the `node` role holds the row's `local` write ("Ending the renewals" below, ADR 0255, ADR 0261). Neither is an ownership change, so the token stays (section 8.6), and the node accepts a same-token grant whose mode differs from the lease it holds and adopts that mode (section 5.12, ADR 0241). This is the only way a lease changes mode. While the node holds the lease (`held` or `uncertain`), a grant with a new token carries the mode the node holds it in, and the node refuses one with the other mode. After `expired` or `revoked`, a new token may carry either mode (section 5.12, ADR 0258). The primary sends the `expiring` grant only while it holds majority support (section 8.8). Until then the lease stays `local`: the app runs on, and a failover needs the source connected or declared lost (section 8.3).

The `expiring` grant that converts a lease answers no renewal and carries no `re` (ADR 0256), so it sets no `valid_until` and counts toward no gate (ADR 0241, which completes ADR 0218):

- The node keeps the lease `held` and its VMs running, and sends a `lease_renew` at once. From the moment that first renewal is sent, the lease has a provisional deadline, that renewal's `t_send + D - M`, and the node self-fences at it unless an answer sets a real `valid_until` first. An answer computes `valid_until` from the `t_send` of the renewal it answers, never earlier than the first, so the deadline moves later only on proof from the authority.
- Until its first renewal is sent, the converted lease behaves as `local` on the node: a lost session neither makes it `uncertain` nor stops its VMs. An agent restart in that window is the exception: the lease comes back `expired` and is fenced, and the primary gives it back ("Agent restart" below, ADR 0281). The authority has set no gate for it then, so the app runs on as before the conversion.
- On the primary, the converted lease has no gate until a grant answers one of its renewals (`T_grant >= t_send` of that renewal). Until then it is `local` for failover: the source must be connected or declared lost, and `failoverApp` answers `lease_held` (section 8.3).
- The gate that the first answer opens counts the longer duration: `T_grant + D_1 + 2M`, where `D_1` is the longer of the conversion grant's `duration_ms` and the answer's, and `M` is computed from `D_1`. Later answers count as any other (ADR 0254). The node's provisional deadline comes from the conversion grant's duration, and an answer may carry a shorter one, so a gate from the answer's duration alone could open before a node that never received that answer has fenced. Before it sends a conversion grant, the primary raises `leases.duration_ms` to at least that grant's duration, so the first gate of a restarted primary covers it too ("What the authority must enforce"). A conversion grant longer than the duration held before the raise is sent only once that raise is replicated ("When R is replicated" below, ADR 0268). Until then the conversion grant carries the duration held before the raise; a row that held none, as a `local` row may not, has no shorter duration to fall back to, so its conversion grant waits for the raise (ADR 0289).
- The renewal the primary answers as the first one must be one the node sent after it took the conversion, because the node's provisional deadline starts there (ADR 0257). After it sends a same-token `local` grant for a lease, the primary therefore sends the node a `ping` (section 5.19), and it sends no conversion grant for that lease until the node's `pong` arrives. The session is ordered both ways (section 4.2), so by then every renewal the node sent before it took the `local` grant has arrived. A renewal that arrives after the conversion grant was sent is then one the node sent after it took the conversion, and the primary answers it as the first one although the row is still `local`, unless the policy has left `warm` since (below). After a session cut, the holder's next `hello` stands in for the `pong`, because no renewal of the old session can arrive on the new one (ADR 0261).
- The lease row keeps `mode` `local` until then (ADR 0244). On the session that sent the conversion grant, the primary writes `expiring` in a transaction that it commits before it sends its answer to the holder's first renewal after the conversion. A restarted primary therefore sees the lease as `local`, so a failover answers `lease_held`. When the holder's `hello` reports the lease as `local`, the primary sends the conversion grant again. When it reports it as `expiring` and `held` or `uncertain`, the primary settles the lease ("Settling a lease" below, ADR 0261): it answers the holder's next renewal as the first one, and writes the row `expiring` once the `pong` that follows arrives. A row that the end of the renewals wrote `local` while the holder still held the lease `expiring` carries the unsettled mark, so the primary tells it apart ("The mark" below, ADR 0280): it settles such a row in the same way while the policy is `warm`, and sends the same-token `local` grant again while the policy is off `warm`. When it reports it `expired`, the node has fenced it, and the primary gives it back with a higher token (section 8.5, "Regrant from `hello`", ADR 0281). A lease the node reports `revoked` at or above the row's token is never given back (ADR 0281).
- A policy change off `warm` (R, "Ending the renewals" below) ends a conversion in flight: the primary answers the holder's renewals as "From R until the holder has taken the grant" says, and writes no `expiring` for them. The row stays `local` with the mark that R set, so what the API shows matches the policy, and the app keeps running. The lease then ends as any lease does after R: with the same-token `local` grant once R is replicated.

A `local` grant with the same token ends the renewals: the node drops `valid_until` and keeps the lease `held` as `local`. A renewal it sent before is no longer outstanding, so an answer to it changes nothing (ADR 0256).

**Ending the renewals (M5b, ADR 0255, ADR 0261).** A successor primary must see the lease as `local` once its holder may run it `local`. A successor that sees the row `expiring` would, once the gate passes, start the app on another node while the holder keeps running it. A majority of the voters does not give that: the vote carries no log position (section 8.9), a promotion loses the entries the candidate never received (section 7.3), and a witness holds no log (section 7.1). A new token does not avoid it either, because a successor that lacks the revoke and the new grant sees the old `expiring` lease. When an app's HA policy leaves `warm`, the primary therefore:

1. writes the lease row `local` and sets its unsettled mark ("The mark" below), in the same transaction as the policy change (ADR 0280). This write, called R below, is the write that ends the lease's renewals. It leaves `leases.duration_ms` as it was;
2. once R is replicated (below), while the policy is still off `warm` and while it holds majority support (section 8.8), sends the holder the same-token `local` grant. It sends it only on a session on which the holder showed that it holds the lease at the row's token: its `hello` listed the lease `held` or `uncertain` at that token, in either mode, or it renewed the lease at that token on that session. Otherwise a node whose highest token for the app is lower would take the grant as a new grant (section 5.12), so the primary waits for such a session;
3. then sends the `ping` of "Changing the mode" (ADR 0257). The write that records its `pong` clears the mark ("The mark" below).

Until the lease row is `local` and replicated, the lease stays `expiring` and keeps renewing (ADR 0255). `Lease.local_pending` is true from R until the primary knows that the holder took the grant: its `pong`, or its report of the lease `local` (`openapi.yaml`, ADR 0280).

When R turns a row that reads `expiring` into `local`, a grant with a new token that the primary holds for that row and has not sent, such as a failover's `expiring` grant to the warm node ("Grants of a marked row" below), is dropped when R commits. R gives the row no new token, so step 4 of "The holder fenced first" does not send that grant either. The node it named holds no lease for the app, so the app stays stopped until an operator starts it. The primary emits `app.left_stopped` as it drops the grant (`events.schema.json`). A primary that restarted after R committed holds no such grant, so it drops nothing and emits nothing. A row that already reads `local` with the mark, as after a restore's grant, R leaves as it is, and the grant held for that row stays.

**When R is replicated.** R is replicated when every machine of the required set has acknowledged a `seq` at or above R's in the primary's log.

- **Acknowledgements.** A `log_ack` on the session, or a `hello.log_seq` once the hash check of section 7.1 has matched it. For the grant of a marked row ("Grants of a marked row" below), a `hello.log_seq` counts only once the node has answered every `lease_revoke` the primary sent at that session start, and every token raise those revokes cause is committed, as a `fence_ack` waits for those revokes (section 8.5, ADR 0239, ADR 0299). A node that lists a lease the row gives another node therefore counts only after it has answered that lease's revoke. A later `log_ack` needs no such wait, because the node holds it back until it has answered those revokes (section 4.2 rule 3). The primary's own host counts through its own agent, like any node. An acknowledgement no longer counts once the primary answers that node `log_diverged` (section 7.1).
- **The required set.** Every enrolled machine with the `node` role, from its enrollment entry on. A primary host without the `node` role is not counted: it holds R in its own database.
- **A removed machine** leaves the set only once its removal entry is itself replicated over the machines of the set that are not removed; until then it still counts (sections 8.10 and 10, ADR 0280). So every later primary holds the removal: there the machine's credential is revoked and its seals are deleted. Two removals pending at once do not hold each other in the set, because neither counts for the other's removal entry.

| Machines | R is replicated when |
| --- | --- |
| Node A (the primary's host), node B and a witness | A and B have acknowledged |
| Two nodes, no witness | Both have acknowledged |
| Three nodes, with or without witnesses | All three have acknowledged |
| One node and one or more witnesses | The node has acknowledged |
| One node, no other voter | The node has acknowledged |

**Why every later primary holds R.**

- A promotion runs only on a machine with the `node` role whose custody is `sealed` (section 7.3).
- A machine that was in the required set when R was replicated acknowledged R durably and keeps it. If its log went back or diverged, a promotion refuses it until it holds R again (sections 7.1 and 7.3).
- A machine that enrolls later is sealed only after it reports a custody key, and its seals come after R in the log (section 7.2).
- A removed machine cannot be elected. Every later primary holds its removal, so it is no voter of the newer voter set, whose listeners refuse it (section 8.9, "Listener"). Nor can it win a majority of the older set: the `local` grant needs majority support at the primary's current voter-set version (section 8.10 rule 3), the voters that gave that support never vote at an older version, and a majority of the older set includes one of them, because the two sets differ by one voter.
- Witnesses and edge-only machines are never candidates (section 8.9, "Listener").

The rule needs no change to `POST /v1/vote` and no majority of acknowledgements. It is full replication over the machines with the `node` role for this one write, not a log-commit quorum, which stays in M6 (ADR 0169). A witness-heavy voter set and the term of a promotion do not affect it.

**While R waits.** R waits only while a machine of the required set is unreachable or not following: a node that is down, a node declared lost but not yet removed, or a node on a binary that does not follow. Meanwhile the policy change is in the database and `Lease.local_pending` is true. The holder still holds the lease `expiring`: it renews and the app keeps running, and like any `warm` app it fences only if the primary loses majority support. The operator waits, or removes the node (section 10). Removal is not refused for leaving fewer than three voters, and the HA view then reports that HA needs a third vote. No setup is stuck for good. When the policy returns to `warm` on a live session while R waits, the primary sends no `local` grant and settles the lease ("Settling a lease" below), as from `hello` (ADR 0280).

**From R until the holder has taken the grant,** the row reads `local` and carries the mark while the holder may still hold the lease `expiring`:

- **Renewals.** The primary answers the holder's renewals under majority support (section 8.8), as for an `expiring` lease, until it sends the `local` grant on the session. From then on it answers no renewal of the lease on that session (ADR 0256) until a conversion grant follows ("Changing the mode"). These answers write nothing. That holds also for the first renewal after a conversion that was in flight at R: no `expiring` is written for it ("Changing the mode").
- **Moving the lease.** A marked row, whatever its mode, moves only as a `local` lease does: through a revoke its holder answered, a fence acknowledgement, or a node declared lost (sections 8.3 and 8.5). `failoverApp` answers `lease_held` while the holder is neither connected nor declared lost. A marked row never moves on a gate, and the mark is in the row, so a primary restart that loses the gates kept in memory loses nothing.
- **No conversion grant without the mode.** The primary sends no conversion grant to a holder whose mode it has not learned on the current session. A same-token `expiring` grant without `re` over an `expiring` lease has no row in the node state machine below, and the primary never sends one. For a holder that reports `local`, a return to `warm` uses the conversion above (ADR 0244, ADR 0254).

**The holder's next `hello`** tells the primary which case a marked row is in, whichever write set the mark ("The mark" below, ADR 0280). A `local` or `expiring` report below means that the `hello` lists the lease `held` or `uncertain` in that mode (ADR 0285):

- **`local` at the row's token, or at a higher token while the row names that holder:** the primary writes the row `local` at the holder's token and clears the mark.
- **`expiring` at the row's token, or at a higher token while the row names that holder, and the policy on `warm`:** the primary settles the lease (below) at the holder's token, which writes the row `expiring` and clears the mark.
- **`expiring` at the row's token, or at a higher token while the row names that holder, and the policy off `warm`:** the primary keeps the mark and sends the same-token `local` grant again, under "Grants of a marked row" below. When the holder's token is above the row's, it first writes the row `local` at the holder's token. This is the resend of ADR 0255; a restarted or promoted primary does the same.
- **`expired`:** "The holder fenced first" below.
- **`revoked` at or above the row's token:** the primary gives nothing back, and the mark stays (ADR 0281). The app stays stopped until an operator acts.
- **Not listed:** the node lists no token for the lease. When the row's newest write gave the lease a new token, the primary sends that grant as "The holder fenced first" step 4 states, under its conditions; otherwise it gives nothing back. The mark stays (ADR 0295).
- **`revoked`, `held` or `uncertain` at a token below the row's:** the primary revokes the lease as section 5.1 says, except as "The holder fenced first" step 4 states.

**Writing the row `local`.** When a holder's `hello` reports the lease `local` at the row's token, or at a higher token while the row names that holder, and the row reads `expiring`, the primary always writes the row `local` at the holder's token, whether or not the row is marked (ADR 0280). It writes the row at the holder's token too when the row reads `local`, carries no mark and names that holder, and the holder's `hello` reports the lease `local` at a higher token, as it can after a restore of the control-plane database (`backup-format.md` section 8.2) whose row predates the holder's latest grant. The primary writes that row before it sends the node its first desired state or any grant of the lease on the session, so both carry the holder's token: a grant at the row's lower token would be refused (section 5.12). The write sets no mark. The primary records a holder's `local` report, under this paragraph or "The holder's next `hello`" above, only while the row is not revoked in its own view (`leases.revoked_at` is null, which declaring a node lost sets, section 8.3), the lease has no failed fence (no node's `fence_failure` lists it, and no node reported a failed fence of it that the primary could not record, section 8.5), and the reported token is at most 2^53 - 1 (section 8.6). Otherwise it leaves the row as it is.

**Settling a lease.** The primary answers a renewal of the holder on the current session, then sends a `ping`. Once the `pong` arrives, it writes the row with the token, with mode `expiring`, and with the larger of the row's stored `duration_ms` and the longest one it granted on that session, so a settle never lowers `duration_ms`; that write clears the mark. The answer that settles the lease counts for its gate at the longer of its own duration and the stored `duration_ms`, so the first gate after a settle counts the stored duration, which covers any longer grant an earlier primary may have sent ("What the authority must enforce" below). The answer replaces the node's deadline with one computed from this primary's grant ("Recompute `valid_until` from the new `t_send`" in the node state machine below), but only once the node has received it. The `pong` to a `ping` sent after the answer proves that it has. Without the `pong`, a node that an earlier primary had granted a longer duration, such as a maintenance window's `W` (section 8.11), could run past a gate counted at `D`. Sending the same-token `local` grant clears any pending settle for that lease, whether planned from a `hello` or waiting for its `pong`: none of them writes the row afterwards, so a renewal the holder sent before it took the grant never writes the row `expiring`.

**The holder fenced first.** The holder may list the lease `expired` in `hello`. It may also refuse the same-token `local` grant with `bad_message` because `valid_until` passed while the grant was in flight, under the conditions of section 8.5 (section 5.12). Either way the node holds no lease for the app, whatever its VMs are doing. The primary gives the lease back to that same node, so no second node runs the app. This is the regrant of section 8.5 ("Regrant from `hello`", ADR 0281), under its conditions, for a marked row. It happens only in these two cases (ADR 0281). Otherwise:

- A holder that lists the lease `revoked` at or above the row's token gets nothing back: the revoke can be the first half of a move whose grant to another node is in entries this primary lacks (section 8.5).
- A lease the holder lists `revoked` at a token below the row's is revoked as section 5.1 says, except as step 4 states.
- A lease the holder does not list gets only the grant of step 4, under its conditions (ADR 0295).

The cost when nothing is given back: the app stays stopped until an operator acts. For a regrant, the primary:

1. waits until the lease's gate has passed, as for any grant with a new token after an expiry ("What the authority must enforce" below). After a restart of the primary, that is the start gate of a marked row ("The mark" below);
2. writes the row with a token above both the row's token and the one the node reports (section 8.5), in the mode the app's HA policy wants, as at placement. That write keeps the mark ("The mark" below);
3. sends the grant under "Grants of a marked row" below: once that write is replicated, and while it holds majority support. While a node of the required set is down, the regrant waits for it as R does ("While R waits" above), and the app stays stopped meanwhile;
4. at each later session start where a marked row's newest write gave the lease a new token and names the node as holder, in either mode, and the node reports a lower token or does not list the lease, sends that grant instead of the revoke of section 5.1, under the same rule. It checks this step's conditions again when it sends the grant, not only at the session start (ADR 0299). Besides the row this regrant wrote, this covers every new-token grant held back under "Grants of a marked row" that a restarted primary no longer holds, such as a failover's `expiring` grant to the warm node or a restore's grant (ADR 0295). This applies only to a marked row whose `revoked_at` is null and whose lease is in neither `fence_failure` nor `fence_ack_pending`. A row that declaring a node lost or a fence acknowledgement wrote keeps its revoke (sections 8.3 and 8.5). At a session start without majority support, the primary sends neither the revoke nor the grant until support returns. The node holds no lease for the app meanwhile, which is safe (ADR 0280). A node that lists the lease `held` or `uncertain` at a lower token in the other mode would refuse that grant (section 5.12, ADR 0258). The primary therefore first revokes the lease at the token the node reports, as section 5.1 says, and then sends the grant. The node holds the grant back until it has answered the revoke (section 4.2 rule 3), as for a lease listed `expired` at a lower token (section 8.5), and after the revoke a grant with a higher token may carry either mode (ADR 0299). Only a node whose store went back lists a lease so (section 7.1), because while a node holds a lease, the primary gives it a new token only in the mode it holds it in.

**The mark (ADR 0280).** The lease row's unsettled mark (`leases.unsettled`, `db-schema.sql` migration 0013) says that the row may disagree with its holder's mode, so the lease moves only as a `local` lease does, whatever the row's mode. The mark is a column of the row, so it is in the log for every later primary, and a restarted primary still has it. It is set by:

- R, on the row it writes `local` ("Ending the renewals" above), and any other write that turns a row that reads `expiring` into `local`, at any token, as R does;
- any write that names a node `local` at a new token, to grant it the lease, while the app's HA policy is `warm`, whatever the row's mode (ADR 0285, ADR 0288). The row can read `local` while the holder still holds the lease `expiring`: between a conversion grant and the answer to its first renewal ("Changing the mode" above, ADR 0244). A volume restore of a `warm` app is such a write: it revokes the holder and names the target node `local` at a new token (section 8.3). A write that grants nothing, as declaring a node lost, a fence acknowledgement or a node removal does, is not one;
- a failover's write whenever its `expiring` grant to the warm node must wait for replication, a raise included: when the row does not read `expiring` with a `duration_ms` of at least the grant's, the write raises `leases.duration_ms`, and the grant waits until that raise is replicated ("What the authority must enforce" below, ADR 0268). A warm app's lease whose conversion is not done, or whose first renewal after the conversion was not answered, reads so. The grant then waits under "Grants of a marked row", where `grant_pending`, `grant_waiting_on`, the `in_progress` refusal and step 4 of "The holder fenced first" cover it, and the settle at the warm node's first renewal clears the mark (below, ADR 0292, ADR 0295, ADR 0303);
- any other write that grants the lease with a new token while that grant must wait, whoever makes it: a regrant, a restore or `createVolume` (ADR 0321). Such an `expiring` grant must wait when its write raises `leases.duration_ms`, as a failover's may, or when the primary may not send it yet for lack of majority support (section 8.8). Marking more rows can only delay a move, never let an app run twice, because a marked row moves only as a `local` lease does;
- the first write of any restore, plain or forced (`backup-format.md` section 8.2), and of any `--accept-loss` promotion (section 7.3), on every `expiring` row, because these may lack R or the writes after it.

The write that records the holder's own `local` report is not one of these: it clears the mark (below).

Other writes of the row keep the mark, the regrant's write of "The holder fenced first" included: no write with a new token clears it. Only a write that records what the holder holds clears it:

- **A `local` report:** the holder reports the lease `local` (`held` or `uncertain`, as "The holder's next `hello`" says) at the row's token, or at a higher token while the row names that holder. The primary writes the row `local` at the holder's token and clears the mark ("The holder's next `hello`" above).
- **The `pong` after the `local` grant, on a live session.** The `pong` to the `ping` that follows the same-token `local` grant (ADR 0257) proves that the node received the grant. The session is ordered both ways (section 4.2), so a `bad_message` refusal of the grant would arrive before that `pong`. When the `pong` arrives with no refusal before it, the primary writes the row `local` and clears the mark, as for a `local` report. After a session cut, the holder's next `hello` stands in for the `pong`, as for the conversion ("Changing the mode").
- **A settle:** the holder holds the lease `expiring` and the policy is `warm`. The primary settles the lease ("Settling a lease" above), from the holder's `hello`, on a live session after a return to `warm` ("While R waits" above), or at any renewal of the holder at the row's token on the current session while the row reads `expiring` and the policy is `warm`, such as the first renewal after a regrant or a failover's grant. Such a renewal proves that the holder ran the lease `expiring` when it sent it, the same evidence its `hello` gives. The settle's row write clears the mark.

A holder that reports the lease `expiring` while the policy is off `warm` leaves the mark, and the primary sends the same-token `local` grant again ("The holder's next `hello`" above).

After a `local` grant with a new token, such as a restore's grant to its target node or a regrant in `local` mode, the mark stays, and so does `Lease.local_pending`, until the node's next session, whose `hello` reports the lease `local` at the row's token. The `pong` above clears the mark only after a same-token `local` grant (ADR 0285).

A restarted primary keeps no gate in memory for a marked row. While it waits for the holder's report, it moves a marked row only as a `local` lease does, whatever its mode, and a regrant after the holder fenced first waits for the start gate `start + duration_ms + 2M`, as for an `expiring` lease ("What the authority must enforce" below). R leaves `leases.duration_ms` as it was (step 1 of "Ending the renewals"), so that gate covers the holder's last `expiring` grant.

When a node that a marked row does not name lists the lease `held` or `uncertain`, the primary revokes it at the token the node reports, which section 5.13 accepts because it is at least the node's current one, as for an unmarked row (section 9 step 2). When the row's token is not already above that, the primary then raises the row's token above it, keeping the mark, and the same write records the row revoked in its own view (`leases.revoked_at`), as declaring a node lost does (section 8.3). Step 4 of "The holder fenced first" therefore never sends a grant for that row (ADR 0285). The raise has a cost: the holder that the row still names, at the old token, is revoked at its next `hello` and stays stopped until an operator acts. The raise does not declare that holder lost: it sets no `lost_at` and emits no event (ADR 0285). Only a marked row's token is raised this way. The cost of the mark itself: after any restore, and from R until the holder has taken the grant, no `warm` app moves on its gate alone until its holder reports.

**Grants of a marked row (ADR 0280).** While the row carries the mark, the primary sends a grant of the lease, other than the answer to a renewal, only once the row's newest change-log write is replicated over the required set ("When R is replicated" above), and only while it holds majority support (section 8.8). Every grant with a new token writes the row first, so its own write is the one that must be replicated. This covers:

- the same-token `local` grant that ends the renewals, and its resend;
- every grant with a new token: the regrant after the holder fenced first, a restore's grant to its target node (`restoreBackup`, section 8.3), a failover's grant (`failoverApp`) and any placement grant.

No operation is refused for it: the grant waits. A failover of a marked row whose holder is neither connected nor declared lost answers `lease_held`, as for a `local` lease ("Moving the lease" above). The grant of a failover of a marked row whose holder is declared lost waits until that holder is removed (section 10), or reports again and has acknowledged the row's newest write, because a node declared lost stays in the required set until it is removed ("While R waits" above). `failoverApp` is neither refused for it nor held open by it: it answers `202` once the change that moves the lease commits, and the grant follows (ADR 0292). Until the grant is sent, the primary reports the wait in `Lease.grant_pending` and `Lease.grant_waiting_on`, and a second failover or a switchover of the app answers `in_progress`. A primary that restarts while a grant with a new token waits sends it at the next session start of the node the row names, under this same rule, as "The holder fenced first" step 4 states (ADR 0295). `grant_pending` is true from the primary's start until that grant is sent, and `grant_waiting_on` also lists that node until its session starts. The same-token `local` grant that ends the renewals is not reported as a wait while it waits for a session on which its holder shows that it holds the lease at the row's token ("Ending the renewals" step 2): `grant_pending` stays false for it, `grant_waiting_on` lists nothing for it, and it makes no failover or switchover answer `in_progress`. The mark still shows in `Lease.unsettled` and `Lease.local_pending`. While the grant waits for such a session, `Lease` does not show whether R is replicated either: `grant_waiting_on` stays empty, and `local_pending` stays true as it does until the holder takes the grant. While the holder's current session is such a session, `grant_pending` is true until the grant is sent on it, and `grant_waiting_on` lists the machines of the required set that have not acknowledged R. When that session ends first, the grant again waits for such a session and is not pending. When the policy returns to `warm`, the grant is dropped. Declaring a node lost and removing a node grant nothing, so they stay as they are (sections 8.3 and 10). Grants of an unmarked row are unchanged: a `local` one needs no support, as before. Under the voters' promise of section 8.8, no voter of that majority can meanwhile have voted for a higher term, so such a grant never races a later primary.

Definitions, all on the node's lease clock: a monotonic clock that keeps counting while the host is suspended, `CLOCK_BOOTTIME` on Linux (ADR 0240):

| Symbol | Value |
| --- | --- |
| `D` | `duration_ms` from the grant |
| `M` | `margin = max(1000 ms, D / 10)` |
| `t_send` | Node lease-clock time at which it handed to the session the `lease_renew` that the current grant answers. A grant that answers no outstanding renewal has no `t_send` and never sets or extends `valid_until`. |
| Renewal interval | `D / 3`; a renewal left unanswered for one interval is late |
| `valid_until` | `t_send + D - M` |
| Fence deadline | `valid_until + M = t_send + D` |
| `T_grace` | Graceful-stop timeout used when fencing |
| `T_force` | Bound of the forced stop that follows, passed to the engine as `force_within` (`engine-trait.md` section 3.8, ADR 0243) |
| Start bound | 3 000 ms: the bound the node passes to the engine for a stateful start under an `expiring` lease (section 6.4, ADR 0242) |

```
valid_until = t_send + D - M
M           = max(1000, D / 10)
```

The authority never grants a `duration_ms` below 3 000 ms: with the floor margin of 1 000 ms that keeps `valid_until` at least 2 000 ms after the renewal, so the node has a real window to renew before it must fence. A grant below that floor is refused with `error` code `bad_message`, non-fatal. A grant that answers a renewal may carry any duration at or above the floor, up to 600 000 ms; the node computes `M` and `valid_until` from the duration of the grant it holds. In M5b the duration is `D` = 15 000 ms, or `W` while a maintenance window is open (section 8.11, ADR 0171).

**Suspend (ADR 0240).** A host suspend must shorten the validity, because the authority's gate keeps running meanwhile. The lease clock counts the suspend, but timers commonly run on a clock that stops during it (`CLOCK_MONOTONIC` on Linux). The node therefore checks, at least every `M / 4` and at once when the host wakes, how far the lease clock has moved beyond its timers' clock. When it has, the host was suspended for the difference, and the node moves each `expiring` lease's timer for `valid_until` earlier by that amount, so a lease whose `valid_until` passed during the suspend expires and self-fences at once. A suspend is not a failure of the node: fencing what passed its deadline is enough, and no failed mark is set.

**First grant.** An `expiring` grant without `re` that does not answer an outstanding renewal of the node — the first grant of a lease to this node, a grant after `expired` or `revoked`, or a higher-token grant while the node already holds the lease as `expiring` (ADR 0258) — records the token, leaves the lease (or puts it) in `uncertain`, and makes the node send a `lease_renew` at once. The lease becomes `held` only when the authority answers that renewal with the same token; `valid_until` is then computed from that renewal's send time. Until then the node starts nothing stateful for it, and an existing `valid_until` (a higher-token grant while held) is kept, never extended. This removes any assumption about how long the grant took to arrive (D-0072). A `local` grant needs no renewal and is `held` on receipt.

**Node state machine.**

| From | Trigger | To | Action |
| --- | --- | --- | --- |
| — , `held` (`local`), `expired`, `revoked` | `lease_grant`, `local` mode, token higher than any seen | `held` | Record the token |
| — , `expired`, `revoked` | `lease_grant`, `expiring` mode, token higher than any seen | `uncertain` | Record the token, send `lease_renew` at once |
| `held`, `uncertain` (`expiring`) | `expiring` `lease_grant` with a higher token that answers no renewal (ownership changed) | `uncertain` | Record the token, keep `valid_until`, send `lease_renew` at once |
| `held`, `uncertain` | `lease_grant` with a higher token and the other mode | unchanged | Refuse with `bad_message` (section 5.12, ADR 0258) |
| `uncertain` | `lease_grant` with the same token answering an outstanding renewal | `held` | Compute `valid_until` from that renewal's `t_send` |
| `held` | a renewal is answered with the same token | `held` | Recompute `valid_until` from the new `t_send` |
| `held` (`local`) | `expiring` `lease_grant` with the same token and no `re` (a conversion) | `held` | Adopt `expiring`, send `lease_renew` at once, keep the provisional deadline of that renewal ("Changing the mode", ADR 0256) |
| any | `expiring` `lease_grant` with `re` that answers no outstanding renewal | unchanged | Nothing (ADR 0256) |
| `held`, `uncertain` (`expiring`) | `local` `lease_grant` with the same token | `held` | Adopt `local`, stop renewing, drop `valid_until` and the outstanding renewals (ADR 0256) |
| `held` | a renewal is late, or the session is lost, except for a converted lease whose first renewal was not sent yet | `uncertain` | Keep running; start nothing new stateful |
| `held`, `uncertain` | the lease clock reaches `valid_until`, or a converted lease's provisional deadline | `expired` | Self-fence (section 8.5) |
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

The fence therefore completes at least `2M` before the gate opens. The two clocks are not shared, so let `δ` be the bound on their drift and offset over the lease lifetime: the fence completion, expressed on the authority's clock, is at most `t_send + D + δ`, and the gate is at least `t_send + D + 2M - δ`. The remaining gap is `2M - 2δ`, which is strictly positive whenever `δ < M`. The margin `M` must therefore be larger than the bounded drift and the messaging delay between the machines. That bound was measured on the lab before M5b (`docs/research/m5b-warm-and-leases.md`): the monotonic clocks of the three machines differed in rate by at most 4.2 ppm (the lease clock runs at the same rate; it differs from the monotonic clock only by the time spent suspended), 0.063 ms over a 15 s lease, and the session path's round trip had a p99 of 0.75 ms, against `M` = 1 500 ms. The lab's machines share one physical host; machines on separate hosts can drift more, but still by parts per million, microseconds per lease. A suspend is detected and handled as above. Any other step of the lease clock, backwards or forwards, by more than `M` must still be treated as a failed node.

Worked example with `D = 15000`:

| Quantity | Value |
| --- | --- |
| `M` | 1 500 ms |
| `valid_until` | `t_send + 13 500 ms` |
| Fence deadline `F` | `t_send + 15 000 ms` |
| Gate `T_gate` | not before `t_send + 18 000 ms` |
| Chosen `T_grace` / `T_force` | 1 000 ms / 300 ms, sum 1 300 ms ≤ `M` |
| Actual fence completion, worst case | `t_send + 14 800 ms` |
| Slack before the gate | at least 3 200 ms, minus 2δ (2δ measured at about 0.13 ms on the lab) |
| A stateful start needs | `valid_until - now` of at least 3 000 + 1 500 = 4 500 ms; about 8 500 ms remain just before a renewal is answered |

**What the authority must enforce.** The gate is the authority's obligation, not the node's. An authority that grants earlier can hand the same app to two nodes. In M5b the authority is the primary, and it:

- sends an `expiring` grant, a first grant or the answer to a renewal, only while a majority of voters supports it for that duration (section 8.8);
- while a lease row carries the unsettled mark, sends a grant of the lease other than the answer to a renewal only once the row's newest change-log write is replicated over the machines with the `node` role, and only while a majority of voters supports it: the same-token `local` grant that ends an `expiring` lease's renewals, its resend, and every grant with a new token ("Grants of a marked row", ADR 0261, ADR 0280). It sends that same-token `local` grant only on a session on which the holder showed that it holds the lease at the row's token ("Ending the renewals" step 2);
- keeps, for each `expiring` lease, the gate of its current holder: the latest `T_grant + duration_ms + 2M` over every grant it sent that holder except a conversion grant, with the first answer after a conversion counted at the longer of its own and the conversion grant's duration (ADR 0254), and the answer that settles a lease at the longer of its own and the row's stored `duration_ms` ("Settling a lease"), so a shorter grant after a longer one (the end of a maintenance window) never brings the gate forward. A lease converted from `local` has no gate until a grant answers one of its renewals, and until then it is moved only as a `local` lease is, with its row still `local` ("Changing the mode", ADR 0241, ADR 0244). A lease whose row carries the unsettled mark is also moved only as a `local` lease is, whatever the row's mode ("The mark", ADR 0261, ADR 0280);
- changes a lease's mode only with the same token: while the node holds the lease (`held` or `uncertain`), a grant with a new token carries the mode the node holds it in, and after `expired` or `revoked` either mode (ADR 0258);
- after a same-token `local` grant, sends the node a `ping`, and sends no conversion grant for that lease until the node's `pong` arrives, so the first renewal it answers after a conversion is one the node sent after it took the conversion ("Changing the mode", ADR 0257);
- grants the lease with a new token, to another node or to the same one after expiry or revoke, only after that gate has passed on its own monotonic clock; renewals by the current holder are not gated;
- keeps no monotonic time across its own restart: at start it treats every `expiring` lease as granted at that moment for `leases.duration_ms`, the longest duration it may have granted the holder, which it raises before it sends a longer grant and lowers only once the longer grant's gate has passed. It sends the longer grant only once the raise is replicated over the machines with the `node` role ("When R is replicated", ADR 0268). Only such a machine can be promoted, so every possible next primary counts the longer gate, also one promoted with `--ack-fence`, which skips the voters' wait (section 7.3, ADR 0236). Lowering `duration_ms` needs no wait: a primary that lacks the lowering only waits longer. This wait is not the one of "Grants of a marked row": that one holds a marked row's grants other than the answer to a renewal, and this one holds any grant longer than the duration held before the raise, answers to renewals included. A grant that both cover waits for both. A conversion grant counts here: the primary raises `duration_ms` to at least its duration before it sends it, and lowers it only once the gate of the first answer after it has passed (ADR 0254). The first gate after a restart is therefore `start + duration_ms + 2M`. A converted lease whose first renewal was never answered gets no such gate, because its row is still `local` (ADR 0244). A row that carries the unsettled mark gets it whatever its mode, so a regrant after the holder fenced first waits for it ("The mark", ADR 0280);
- writes nothing for a renewal: a renewal changes no state and appends no change-log entry, except the answer to a converted lease's first renewal while the policy is `warm`, which writes the row's `mode` `expiring` before it is sent (ADR 0244), and the answer that settles a lease, whose row write follows the `pong` ("Settling a lease", ADR 0261). `Lease.renewed_at`, `expires_at` and `gate_at` in the API are the primary's in-memory view.

The primary's single-writer critical section is its own database transaction. What a single primary cannot guarantee alone is that no second primary grants at the same time after a promotion; the voters' support and the terms provide that (sections 8.8 and 8.9). A Durable Object or a conditional write on strongly consistent storage remains a later option for the same role (`docs/research/durable-objects.md`, `docs/research/r2-conditional-writes.md`).

**Agent restart.** `expiring` leases that were `held` or `uncertain` survive an agent restart within the same host boot. The node persists, for each such lease, its deadline on `CLOCK_BOOTTIME`, `CLOCK_BOOTTIME(t_send) + D - M`, with `t_send` read when the renewal is sent, not when its answer arrives, so a suspend between the two cannot make the stored deadline later (ADR 0240); a converted lease's provisional deadline is persisted the same way. It stores the deadline together with the host's kernel boot id (`/proc/sys/kernel/random/boot_id`, stored as `host_boot_id` in `node_meta`, section 12; not the per-process `boot_id` of `hello`). On restart, if the kernel boot id is unchanged and the deadline has not passed on `CLOCK_BOOTTIME`, the lease comes back `uncertain` with that `valid_until` and renews at once; otherwise it comes back `expired` and is fenced. A converted lease whose first renewal had not been sent has no stored deadline, so it comes back `expired` too: the node stores the provisional deadline only after the send, and cannot tell whether a renewal left. The primary gives it back from the node's next `hello` (section 8.5, "Regrant from `hello`", ADR 0281). `CLOCK_BOOTTIME` keeps counting while the machine is suspended, so a suspend can only shorten the remaining validity. The VMMs outlive the agent (section 6.3), so an agent upgrade does not stop warm apps. Required before `expiring` leases ship in M5b (D-0072); M0 restores such leases as `expired` and fences them, which is the conservative behaviour. The vote state of a voter survives restarts the same way (section 8.8).

### 8.5 Self-fence procedure

When a lease reaches `expired` or `revoked`, the node:

1. Stops every stateful VM bound to that lease (after an agent restart with no verified desired-state snapshot, every app VM, section 6.3) with `StopMode::Graceful { timeout: T_grace, force_within: T_force }`, which forces the stop when the graceful timeout passes, so the whole stop finishes within `T_grace + T_force <= M` (`engine-trait.md` section 3.8, ADR 0243). The forced stop escalates to `SIGKILL` on the VMM process inside the engine (`engine-trait.md` section 3.8).
2. Records the stop locally: lease id, token, VM ids, time and the reason (`expired` or `revoked`), so the fact survives an agent restart. The token is the highest the node has seen for the lease when the fence starts: for a fence that a `lease_revoke` started, the revoke's token, which is at least the node's current one (section 5.13, ADR 0246). A failed fence is recorded with the same token (ADR 0245).
3. Emits `node.fenced` with the lease, the token and the VM ids (`data.lease_ids`, `data.fencing_tokens` and `data.vm_ids` in `events.schema.json`) — only when every bound VM is confirmed stopped or gone. If the session is down the event is buffered (section 6.5) and delivered later.
4. Starts nothing stateful for that resource until a new `lease_grant` arrives with a higher `fencing_token`.

The node also emits `node.fenced` when an operator declaration invalidated its token without a message reaching it — it learns that when the primary refuses its next `lease_renew` or when a `hello` is answered with a `lease_revoke`.

**Regrant from `hello`** (M5b, ADR 0281, which supersedes ADR 0264). A node can fence a lease that the primary's row still gives it: a converted lease after an agent restart before its first renewal was sent (section 8.4, "Agent restart"), or an `expiring` lease whose deadline passed while the session was down or while a same-token `local` grant was on its way. Each is a deadline expiry. Nothing else gives such a lease back, so the app would stay down until an operator acted. The primary therefore gives the lease back when the node reports such an expiry, and only then:

- the node's `hello` lists the lease as `expired`; or
- the node refuses a same-token `local` grant of the lease with `error` code `bad_message` (section 5.12), on a session whose `hello` listed the lease `held` or `uncertain` at that token and on which the primary sent no `lease_revoke` of it, so the lease expired while the grant was on its way;

and both of these hold:

- the lease's row gives it to that node: the row names the node as its holder and is not revoked in the primary's own view (`leases.revoked_at` is null, which declaring a node lost sets, section 8.3);
- the node has reported no failed fence of it: the lease is in neither the node's `fence_failure` nor its `fence_ack_pending` ("Acknowledging a failed fence" below).

**Never on `revoked` (ADR 0281).** The primary never gives back a lease that the node lists as `revoked` at or above the row's token. A revoke can be the first half of a move, such as a restore (section 8.3), whose grant to another node sits in change-log entries that a promoted primary may lack. Such a primary cannot tell that revoke from a crash between a revoke and its grant, so a regrant could hand the lease back to the old node while the other node runs the app. A lease the node lists at a token below the row's is revoked as section 5.1 says, unless the row carries the unsettled mark and its newest write gave the lease a new token and names the node: the primary then sends that write's grant instead, after the revoke when the node lists the lease `held` or `uncertain` in the other mode (section 8.4, "The holder fenced first" step 4, ADR 0295, ADR 0299). The cost: an app whose node a revoke stopped, with no grant after it, stays stopped until an operator acts.

The primary then grants that node the lease with a token above both the row's token and the one the node reports (ADR 0285), in the mode the app's HA policy wants. It is an ownership change like any grant after an expiry (section 8.6):

- the primary writes the row with the new token and mode, then sends the grant;
- for a row in `expiring` mode, the grant waits until the lease's gate has passed (section 8.4, "What the authority must enforce"). A converted lease whose first renewal was never answered has no gate, because its row is still `local` (ADR 0244);
- an `expiring` grant waits for majority support (section 8.8);
- the node takes it as a first grant (section 8.4): a `local` grant is `held` at once, and an `expiring` one puts the lease in `uncertain`, so the app starts only once the first renewal is answered and the start margin is left (section 6.4, ADR 0242).

For a row that carries the unsettled mark (section 8.4, "The mark", ADR 0280), the primary does more ("The holder fenced first", ADR 0261): it waits for the lease's gate whatever the row's mode, and after its own restart for the start gate `start + duration_ms + 2M`; the row it writes keeps the mark; and it sends the grant only once that write is replicated over the machines with the `node` role, and while it holds majority support (section 8.4, "Grants of a marked row").

A lease the node lists `expired` at a token below the row's is revoked first (section 5.1); the node holds back the grant until it has answered that revoke (section 4.2 rule 3). The exception is a marked row whose newest write names the node as holder, in either mode, at a higher token, such as the one a regrant or a failover wrote: at each later session start the primary sends that grant instead of the revoke (section 8.4, "The holder fenced first" step 4, ADR 0295). The node stopped the app's VMs when it fenced, so the cost is one cold start of the app on the same node.

**Failed fence.** If a forced stop fails or times out and the node cannot confirm the VM stopped (the engine reports it neither stopped nor gone), the VM may still be running. The node then never emits `node.fenced` for that fence; it records the failed fence locally, emits `node.fence_failed` with the lease and the token of step 2, the apps and the VM ids that may still run, marks itself failed in its local store (`node_meta`, section 12) so the mark survives an agent restart, starts no stateful VM for any lease while failed, and answers a `lease_revoke` with `command_result` status `error` and error code `fence_failed` (section 5.18). The lease stays unstartable on this node. The primary treats `node.fence_failed` like the loss of the node: it does not grant the lease to anyone else until an operator acknowledges the fence, as when declaring a node lost (section 8.3) (D-0074).

**Acknowledging a failed fence** (D-0079). Only a human clears a failed fence, after checking out of band that the VMs it names are stopped (for example by power-cycling the host):

1. When the primary receives `node.fence_failed`, or a `lease_revoke` answered with `fence_failed`, it records the failure on the node: the lease, app and VM ids are added to the node's `fence_failure` (`db-schema.sql` migration 0001; `Node.fence_failure` in `openapi.yaml`). While a lease is listed there, the primary grants it to no node.
2. An operator calls `POST /api/v1/nodes/{node_id}/fence-ack` with the lease ids they verified. The scope `node:fence_ack` is human-only, so no agent can clear a fence, and the call is audited (D-0031). The list must equal the node's outstanding `fence_failure` lease ids, so an acknowledgement cannot cover a failure the operator has not seen.
3. In the same transaction the primary increments the `fencing_token` of each listed lease that the node still holds and records it revoked in its own view (`leases.revoked_at`), exactly as when declaring a node lost (section 8.3). When such a lease is already at 2^53 - 1, the token cap (section 8.6), the primary writes nothing and answers `409 conflict` with `reason: token_exhausted` (ADR 0321). A listed lease whose row names another node as its holder keeps its token, and the primary leaves its row as it is (ADR 0246). The primary clears the node's `fence_failure`, and records the acknowledgement as pending delivery (`fence_ack_pending`), adding its lease ids to any acknowledgement still pending, each with a token (ADR 0245): for a lease the node still holds, the lease's `fencing_token` once this transaction commits; for a lease another node holds, that lease's `fencing_token` plus one, so that the acknowledgement covers a failed fence the node recorded at that token (ADR 0247); for a lease the primary has no row for, 9007199254740991 (2^53 - 1), the highest integer the change log's canonical JSON carries, so that the node clears every failed fence of that lease below it (ADR 0262). The stored token stays as it is when the lease is granted again before the acknowledgement is delivered. From then on the leases may be granted again, to this node or another, with a higher token.
4. The primary sends `command` kind `fence_ack` with the lease ids in `fence_ack_pending` and the tokens stored with them, the same on every delivery (section 5.17, ADR 0245): at once when the node is connected and has answered every `lease_revoke` sent on the session, otherwise at its next session once the node has answered the stale-lease revokes, and before `desired_state` (section 4.2). The node processes no `command` before it has answered a revoke (section 4.2 rule 3), so a `fence_ack` sent while a revoke is unanswered would be applied after that revoke's fence and clear a failure that fence records; waiting for the answer lets a `fence_failed` answer take its lease out of the acknowledgement first (ADR 0239). A `command_result` with `status: ok` clears the delivered ids from `fence_ack_pending`, so an acknowledgement recorded after the command went out stays pending; until then the primary sends it again at each session start. A new failure of one of the pending leases (step 1) also clears it from `fence_ack_pending`, in the same transaction that lists it again in `fence_failure`, so a pending acknowledgement never clears a failure nobody saw: the new failure needs a new acknowledgement (ADR 0238). Once no lease is left, `fence_ack_pending` is null and no `fence_ack` goes out.
5. For each listed lease, the node clears the failed fences recorded below its acknowledged token and, when none remains, its failed mark (ADR 0245). A failed fence at or above that token stays: the operator has not seen it, and the primary lists it again (step 1, ADR 0238). A node that never receives the command stays failed, which is the safe side: it starts nothing stateful.

**Why the token is enough** (ADR 0245). A node can fence one lease more than once at the same token: a `lease_revoke` with a token at least its current one fences again (section 8.4), whatever came before. That never lets an acknowledgement clear a failure the operator has not seen:

- For a lease the node still holds, every message the primary sends about the lease after step 3 carries the acknowledged token or a higher one (section 8.6). A fence that such a message starts is recorded at or above it (self-fence step 2, ADR 0246) and stays.
- For a lease another node holds, the acknowledged token is one above that lease's token (ADR 0247), so the node clears a failed fence it recorded at that token. The node never held the lease at that token, so only a `lease_revoke` from the primary starts a fence recorded there, or an agent restart after one. On a live session after step 3 the primary sends the node nothing about the lease: the node does not renew it, because the lease is `revoked` or `expired` there (sections 5.5 and 5.17), and a later move of the lease revokes its holder, not this node. A revoke sent before step 3 and one sent at a session start are both answered before the `fence_ack` goes out (the next two bullets). A later grant of the lease to this node is an ownership change, so its token is at or above the acknowledged one (section 8.6), and a fence it starts stays.
- For a lease the primary has no row for, the acknowledged token is 2^53 - 1 (ADR 0262). The primary grants such a lease to no node, and the only thing it sends the node about it is the revoke of section 5.1 at each session start, which the node answers before the `fence_ack` goes out (step 4). A failed fence of that revoke takes the lease out of the acknowledgement (ADR 0238). Every failed fence the node records for the lease before the acknowledgement is delivered is therefore one the operator saw or one listed again. No primary can issue a token above 2^53 - 1: every grant and every token increment writes the lease row, `fencing_token` included, to the change log, whose canonical JSON refuses larger integers. A node that reports a higher token in `hello` therefore holds a corrupted or forged store. The node records the revoke's failed fence at that token, the acknowledgement does not clear it, and the node stays failed and starts nothing stateful for the lease. That is the safe side, as for a lease with a row. A failed fence at exactly 2^53 - 1 is not cleared either; reaching it takes 2^53 - 1 ownership changes of one resource.
- A revoke sent before step 3 is answered before the `fence_ack` goes out (step 4). If its fence failed, the answer took the lease out of the acknowledgement (ADR 0238).
- At a session start, a node whose token for the lease is below the primary's, or whose lease another node holds, reports it in `hello`. The primary therefore revokes the lease at its current token first (section 5.1, section 9 step 2) and sends the `fence_ack` only once that revoke is answered. The revoke's fence either fails, which takes the lease out of the acknowledgement, or confirms that the lease's VMs are stopped. This covers a fence the node ran at the older token while disconnected, an agent restart and a redelivered `fence_ack`.
- On a live session, the node is still failed from the acknowledged failure until it applies the `fence_ack`, so it has started no stateful VM since, and a lease cannot become `held` again at a token it already fenced (section 5.12). A fence of the lease below the acknowledged token therefore names only VMs that the acknowledged failure's fence already covered: those the operator verified, or those the start gate has kept stopped since (section 5.13).

**Concurrency.** Fences of different leases must run concurrently, and an agent crash in the middle of a fence must leave the lease fenced on restart; both are covered by mandatory tests before `expiring` leases ship (D-0075). M0 fences one lease at a time, which is acceptable only because no `expiring` lease exists outside tests. Fences that answer revokes run one at a time, because the node holds back a second revoke until it has answered the first (section 4.2 rule 3); a fence at an `expiring` lease's `valid_until` runs alongside them, so the timing of section 8.4 holds however long a revoke's fence takes (D-0075).

### 8.6 Fencing tokens

- A token is an integer per leased resource that increases strictly on every ownership change — a grant to a new holder, or a grant after expiry or revoke — and stays the same across renewals by the same holder (ADR 0073). It is never reused.
- **The cap (ADR 0321, ADR 0325).** A token never exceeds 2^53 - 1, the highest integer the change log's canonical JSON carries (section 8.5). A lease row at that token stays revoked and is never granted again. Every operation that would raise a token past it writes nothing and is refused with `reason: token_exhausted`, not an internal error: a fence acknowledgement (section 8.5), declaring a node lost (section 8.3) and removing a node that is not connected (section 10) answer `409 conflict`, and `quoro cluster promote --ack-fence`, whose write declares the old primary's node lost, checks the cap before its real votes and its freeze, so its refusal changes nothing (section 7.3). Recovery stays a human action. The current token lives on the lease row (`fencing_token` in the shared entity model) and is echoed in every `lease_grant`, `lease_revoke`, `lease_renew`, `hello` and stateful VM spec.
- Every write to shared storage carries the token: the chunk store and object store from M5, and the replication stream of warm volumes (D-0023, section 6.13). A receiver rejects any write whose token is lower than the highest it has seen for that resource, and records the rejection. For the backups of M5a the receiver is the primary, not the bucket: the bucket accepts any chunk or manifest the credential may write, and the primary seals a backup only when its token is the app's current one (`backup-format.md` section 6.2).
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
| M5b | Primary, while a majority of voters supports it (section 8.8) | `local`, `expiring` (`warm`) | Warm: renewal loop, `uncertain` handling, restart persistence of held leases (section 8.4), concurrent fences (section 8.5); replication streams carry the token and the target refuses a lower one (section 6.13). Voters answer `support_request`, keep their vote state and serve promotion votes (sections 8.8 and 8.9); every machine refuses a lower term |
| M6 | Quorum | `local`, `expiring` (`warm`, `hot`) | Automatic failover and `hot`; the same messages also carry `resource_type: primary` leases |

### 8.8 Voter support and terms (M5b)

From M5b the primary is the lease authority only while a majority of voters supports it (ADR 0169). A voter is a machine with the `node` or `witness` role (section 6.12). Support works like an `expiring` lease with the roles swapped: the primary asks over the session it already has with each voter, the voter promises, and each side measures from the moment that keeps it safe.

Definitions, each on the clock of the machine that uses it:

| Symbol | Value |
| --- | --- |
| `S` | `support_ms` of the request; 15 000 ms by default, at least 3 000 ms |
| `M_S` | `max(1000 ms, S / 10)`: 1 500 ms with the default |
| `L` | `max_lease_ms` of the request: the longest `duration_ms` the primary may grant while this support lasts |
| `M_L` | `max(1000 ms, L / 10)`, the margin of a grant of duration `L` (section 8.4) |
| `p_send` | The primary's time on `CLOCK_BOOTTIME` when it sent the `support_request` that a `support_grant` answers (ADR 0263) |
| Support, in the primary's view | Until `p_send + S - M_S` |
| `v_recv` | The voter's monotonic time when it received that request |
| Promise | The voter supports no other candidate for a higher term before `v_recv + S` |
| Lease horizon | `v_recv + S + L + 2 M_L`: until then a grant made under this support may not yet have passed its gate |

**Exchange.** The primary sends `support_request` to every connected voter (section 5.27) and keeps, per voter, the newest answered support. The voter stores the promise and raises its lease horizon to the new value when it is later than the stored one, then answers `support_grant` (section 5.28). The voter never lowers the horizon within a term.

**Majority.** More than half of the voter set of the primary's `voters_version` (section 8.10), counting every voter, connected or not. The primary's own host is a voter like any other when it has the `node` or `witness` role, and it counts only through the support its own node agent grants over its own session: the vote state lives with the agent (section 12), so a candidate can ask it like any voter, and a host whose agent does not run gives no support. A voter counts only when its `support_grant` carries the primary's `voters_version`. With fewer than three voters the primary still needs a majority of the voters that exist (both of two, the one of one), and `Cluster.votes.ha_ready` is false: a two-voter cluster loses its `expiring` leases when either machine is cut off.

**What support allows.** At the moment it sends an `expiring` `lease_grant`, a first grant or the answer to a renewal, of duration `d`, the primary must hold support from a majority whose `L` is at least `d`. It checks `p_send + S - M_S` against `CLOCK_BOOTTIME` read at that moment, the clock the voter measures its promise on and the node its deadlines on (section 8.4, ADR 0240, ADR 0263). On Linux `CLOCK_MONOTONIC` stops while the host is suspended, so a support window measured on it would outlast the voters' promises by the length of a suspend of the primary's host, and the primary could renew a lease after a candidate has won and waited the lease horizon (section 8.9). The other times of sections 8.4 and 8.9 that the primary or a candidate measures, the authority's gate and the candidate's wait for the lease horizon, only run long across a suspend, which is the safe side. Without it the primary leaves renewals unanswered: the node's renewals go late, the lease goes `uncertain` and at `valid_until` `expired`, and the node fences, on the primary's own host too. That is the price of ADR 0169: a primary cut off from the majority stops its warm apps instead of keeping them while a promotion may start them elsewhere. `local` leases need no support; they are granted, revoked and moved as before. The exception is every grant of a lease whose row carries the unsettled mark, other than the answer to a renewal: the `local` grant that ends an `expiring` lease's renewals, its resend, and every grant with a new token each need majority support at the moment they are sent (section 8.4, "Grants of a marked row", ADR 0261, ADR 0280). Placement grants of an unmarked row need none.

**Terms.** `term` starts at 1 and goes up only through a promotion, which takes a higher term from a majority of voters (section 8.9), and through a restore, whose primary starts above every term it knows of (`backup-format.md` section 8.2, ADR 0261). Every machine, voter or not, keeps the highest term it has seen, from a `welcome`, a `desired_state`, a `support_request` or a vote it granted, in its local store (section 12):

- A `welcome`, `desired_state` or `support_request` with a lower term is answered with `error` code `stale_term`, fatal, and the machine ends the session and redials (section 4.4). It keeps running its stored desired state meanwhile.
- A higher term in a `welcome` or `desired_state` is stored and adopted. The session is authenticated by the pinned certificate, which a promoted primary keeps (ADR 0181).
- A voter supports, at its current term, only the primary it voted for when it voted in that term; for a term it did not vote in, the primary of its session.
- A primary that receives `stale_term` stops granting and renewing `expiring` leases at once, and demotes itself: it ends its sessions with `not_primary` and answers every later request with `503` `not_primary` (section 7.4, ADR 0182). The error carries the sender's `term` (section 5.18). A primary restored with `quoro restore --accept-loss` that has sent no `lease_grant` yet starts again at one above that term instead of demoting itself (`backup-format.md` section 8.2, ADR 0261).
- A voter that voted in the primary's term for another candidate answers its `support_request` with `bad_message`, which carries the voter's `term` (section 5.27). A primary restored with `--accept-loss` that has sent no `lease_grant` yet treats it like `stale_term`: it starts again at one above that term. A plain restore lacks that voter's support, and `backup-format.md` section 8.2 gives the way out (ADR 0280).

**The term ceiling.** A term never exceeds 2^53 - 1, the highest integer the change log's canonical JSON carries (section 8.5). No machine sends, stores or adopts a higher term: not as its own term, a vote, `demoted_by_term` (section 7.4) or the highest term it has seen.

- After a `welcome` whose `term` is above it, the node adopts nothing, answers `bad_message` and ends the session, then dials again (section 4.4).
- A `desired_state` or `support_request` whose `term` is above it is answered with `error` code `bad_message` and changes nothing (section 5.18).
- An `error` whose `term` is above it is malformed. The primary does not demote itself on it, records nothing from it and logs it, and a primary restored with `--accept-loss` does not start again from it.
- A vote request whose `term` is above it is answered `400` before any write, and a vote answer whose `term` is above it counts as no answer (section 8.9).
- A promotion whose `T` would pass it stops before any real vote request and changes nothing (section 7.3).
- A primary restored with `--accept-loss` whose new term, one above an error's `term`, would pass it has no exception left, and demotes itself as any other primary does (section 7.4).
- A restored primary whose first term would pass it writes nothing (`backup-format.md` section 8.2).

A cluster whose term has reached the ceiling has no recovery path: no promotion and no restore can take a higher term. This is a stated limit, as is the demotion of a primary restored with `--accept-loss` above.

**Restarts.** Before it answers, the voter stores:

- its term and the candidate it voted for;
- the `S`, `L` and `v_recv` of the last request it granted;
- the promise and the lease horizon.

`v_recv`, the promise and the horizon are measured on `CLOCK_BOOTTIME` next to the host's kernel boot id, as for held leases (section 8.4). After an agent restart within the same boot they keep counting.

After a reboot the voter cannot tell how long it was down, so it starts both again from its first start, `now`, and neither is shorter than the longest window it may still owe:

- the promise until `now + max(S, promise - v_recv)`;
- the horizon until `now + max(S + L + 2 M_L, horizon - v_recv)`;
- `S`, `L`, `v_recv`, the promise and the horizon are the stored values, and the voter then stores `now` as its `v_recv` (ADR 0305).

An earlier grant with a longer window can have set the stored promise or horizon, and the primary keeps the newest support it got an answer for. So when the answer to the last request was lost, the primary may still count that earlier support. An example is a lost answer after a maintenance window's `W` ended (section 8.11), when `L` went back to `D` while a grant of up to `W` may still be before its gate. When the last grant set both, the two spans are exactly `S` and `S + L + 2 M_L`. Both can only end later than before, which is the safe side. The primary keeps support in memory only: after it restarts it has none, and it asks again before it grants or renews any `expiring` lease.

### 8.9 Promotion votes (M5b)

Manual promotion (ADR 0182) takes a term `T` from a majority of voters. `T` is above the highest term the candidate has seen or any vote answer carries, `check` answers included (section 7.3, ADR 0261). This section fixes how a candidate asks and what a voter answers; the promotion's own steps, the CLI and the custody seals are in section 7.

**Where votes travel: directly over the mesh.** The M5a mesh is a star: nodes peer only with the edge (section 6.9). A candidate could reach the other voters either through the edge as a relay or through peers of its own, and M5b uses peers:

- A relay makes the edge a single point of failure for promotion, which is the operation used when a machine is gone, and a cluster whose witness is a separate machine may have no edge at all.
- WireGuard authenticates each peer by its key and binds its mesh address to it, so a request that arrives from a voter's mesh address comes from that voter.
- Warm replication needs node-to-node peers anyway (ADR 0144, ADR 0173).

Every two voters are therefore peers unless both are witness-only (section 6.9). Two voters that both have no endpoint get no peer entry for each other, and two voters both really behind NAT get entries that carry no traffic (section 6.9). A candidate on one of them cannot count the other, and needs its majority among the voters it reaches.

**Listener.** The node agent of every voter listens on its own mesh address, TCP port `spec.votes.port` (51821 by default), and on no other address. It listens only while `spec.votes.listen` is `true` (section 6.12). A voter that applies a desired state without `listen` stops listening before it configures that state's mesh peers. While votes are closed, no voter answers a vote, so no promotion can take a term (section 7.3 step 2). Requests are HTTP/1.1 with JSON bodies inside the WireGuard tunnel, which encrypts and authenticates them. The voter answers only requests that come from the mesh address of a voter with the `node` role in its `spec.votes.voters`, because only such a machine holds custody seals and can be promoted (ADR 0180); any other source is answered `not_allowed`. The candidate asks every voter in the same way, its own agent included. A connection from the voter's own host to its mesh address carries that address as its source, whichever process opened it. The voter takes such a connection as coming from that address only when its client socket belongs to root (uid 0), as the promotion command's does (section 7.3). It answers any other owner `not_allowed`, and answers `503` when it cannot read the owner (below).

**Request.** `POST /v1/vote`:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `cluster_id` | string (`cls_`) | yes | The cluster |
| `term` | integer | yes | The term asked for, `T`: above the highest term the candidate has seen or any vote answer carried, `check` answers included (section 7.3). At most 2^53 - 1 (section 8.8) |
| `candidate_node_id` | string (`nod_`) | yes | The candidate; must be the node whose mesh address sent the request, otherwise the request is refused `not_allowed` |
| `voters_version` | integer | yes | The voter-set version the candidate counts against (section 8.10) |
| `check` | boolean | no | `true`: answer as for a real vote and change nothing. A candidate checks first and votes only when a majority would grant, so a promotion that cannot win moves no voter to a higher term |

**Answer.** `200` with:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `node_id` | string (`nod_`) | yes | The voter |
| `term` | integer | yes | The voter's current term after this request. An answer whose `term` is above 2^53 - 1 counts as no answer (section 8.8) |
| `vote` | string | yes | `granted` or `refused` (with `check`, what a real vote would answer) |
| `reason` | string | no | With `refused`: `stale_term` (the term asked for is not above the voter's term, unless the voter already granted that term to this candidate), `support_held` (the voter's promise to the primary it supports has not run out), `voters_changed` (the request's `voters_version` differs from the voter's), `not_allowed` (another cluster, the source is not a voter with the `node` role, `candidate_node_id` is not the node the request comes from, or the request comes from the voter's own host through a socket that is not root's) |
| `retry_after_ms` | integer | no | With `support_held`: time until the promise runs out |
| `lease_wait_ms` | integer | no | With `granted`: `max(0, lease horizon - now)` on the voter's clock |
| `voters_version` | integer | yes | The voter's voter-set version |
| `voters` | array of string | no | With `voters_changed`: the node ids of the voter's voter set |

```json
{
  "node_id": "nod_01JCA0A1B2C3D4E5F6G7H8J9KM",
  "term": 2,
  "vote": "granted",
  "lease_wait_ms": 16480,
  "voters_version": 3120
}
```

**Voter rules.** A voter grants term `T` to candidate `C` only when all of these hold:

1. `T` is above its current term, or equals it and it already granted `T` to `C`, which makes a repeated request harmless;
2. its promise to the primary it supports has run out (`now >= v_recv + S` of its last `support_grant`), or `C` is that same primary;
3. the request's `voters_version` equals its own.

It then stores `T` and `C` (section 12) before it answers `granted`, and from then on refuses every lower term: its session with an older primary ends with `stale_term`.

**Order of the refusals.** The voter checks the reasons in this order, and the first that applies decides the answer: `not_allowed`, `stale_term`, `voters_changed`, `support_held`. A `check` follows the same order.

**When the voter cannot tell.** A voter that cannot tell whether it may grant answers `503` with the body `{"error": "<text>"}`: when its store fails or the work that answers the request fails, when its stored desired state carries no `spec.votes`, when it cannot read its boot clock or its boot id, when its stored support is from another boot and it has not yet restarted that support on this one (section 8.8, "Restarts"), or when it cannot read the owner of a connection from its own host (above). It grants nothing and writes nothing. A `503` may come before a refusal that would otherwise apply. The candidate counts it as no answer, neither granted nor refused, and may ask again.

**A request outside this section.** A body that is not a vote request, or whose `term` is above 2^53 - 1 (section 8.8), is answered `400` with the same body, before any write. A candidate never asks for such a term (section 7.3).

Any other path is answered `404`, any other method `405`, a body above 4 KiB `413`, and a body that does not arrive in time `408`, each with the same body and before any write.

**Candidate rules.** The candidate may act as primary at term `T` only when voters forming a majority of the voter set of its `voters_version` answered `granted`, and it has then waited until the latest `(receipt time of the answer) + lease_wait_ms` over those answers, on its own monotonic clock. An operator can skip that wait by acknowledging the fence (`--ack-fence`, section 7.3, ADR 0182), which declares the old primary's node lost (section 8.3) and leaves every lease's own gate in place.

**Why a promoted primary never overlaps the old one's grants.** Let `P` be the primary of term `t` and `Q` the candidate's majority for a term `T` above `t`.

1. Every `expiring` grant `P` sends needs, at that moment, the support of a majority `R` in `P`'s view. `R` and `Q` share a voter `v`: they are majorities of one voter set, or of two sets one change apart (section 8.10).
2. `v` granted `T` only at or after `v_recv + S` on its clock, where `v_recv` is the receipt of the last `support_request` from `P` it answered. That request was sent at `p_send <= v_recv`, so `P`'s view of `v`'s support ended at `p_send + S - M_S`, earlier by at least `M_S - δ > 0` (section 8.4: `δ < M`). After granting `T`, `v` answers `P` only with `stale_term`. So `P` held no support from `v` once `v` had voted, and every grant `P` made was sent while an earlier support of `v` was valid.
3. Such a grant, sent at `T_grant < p_send + S - M_S` with `d <= L`, passes its gate at `T_grant + d + 2M_d < p_send + S + L + 2M_L`, which is before `v`'s lease horizon.
4. `v` reported `lease_wait_ms` as the rest of that horizon when it answered; the candidate counts it from the answer's receipt, which is later. So when the candidate acts, every grant `P` made has passed its gate: every holder has fenced (section 8.4), and the new primary may grant again with a higher token.

With the defaults (`S` = `D` = 15 s), a voter's wait after the old primary's last support request is at most `S + D + 2M` = 33 s, the bound of ADR 0169; during a maintenance window with `W` = 180 s it is 15 + 180 + 36 = 231 s.

### 8.10 Voter set changes (M5b)

The voter set changes when a machine with the `node` or `witness` role enrolls, when `POST /api/v1/nodes/{node_id}/roles` adds or removes a vote, and when a voter is removed (section 10). Its version, `spec.votes.version`, is the change-log `seq` of the entry that made the last such change, so it only grows and a promoted primary inherits it with the log. Two majorities must share a voter for section 8.9's argument, and that holds between one set and the next only when they differ by one voter. So:

1. Every change adds or removes exactly one voter.
2. The primary makes the next change only after a majority of the current voters has acknowledged, with `command_result` `ok`, a `desired_state` that carries the current version. Until then the roles operation and the removal of a voter answer `409 conflict`, and the enrollment of a voter answers `409` without consuming its token (section 2.3).
3. The primary counts a voter's support only when its `support_grant` carries the primary's current version.
4. A voter grants a vote only for its own version (`voters_changed` otherwise), so a candidate's majority holds one version.

A machine's version only grows, because it comes with its newest stored `desired_state`, so a voter that supported the primary at `v + 1` can never vote at `v` afterwards. A candidate whose majority holds version `v` therefore faces a primary that counts version `v` or `v + 1`: rule 2 keeps the primary from moving to `v + 2` while a majority of the `v + 1` set still holds `v`, and a majority of `v` voters would need some of them. The `voters` list of a `voters_changed` answer lets the candidate count against the newer set instead.

A removed machine with the `node` role leaves the required set of section 8.4 only once its removal entry is itself replicated over the machines of that set that are not removed (ADR 0261, ADR 0280). Until then it still counts for the write that ends an `expiring` lease's renewals and for every grant of a marked row.

The first write of a restore with `--accept-loss` removes the dead machines with the `node` role, so it may change the voter set by more than one voter. It is exempt from rules 1 and 2 (`backup-format.md` section 8.2, ADR 0280). What is left after it: the restored primary needs a majority of the remaining voters, as any primary does. A new host without the `node` role leaves a cluster with no node.

ADR 0170's guard applies to the roles operation: removing a vote is refused with `409 conflict` when it would leave fewer than three voters while any `expiring` lease exists, unless the request sets `force`. An `expiring` lease row counts here while it is not revoked, or while it is revoked and its gate has not passed; a revoked row whose gate has passed does not count (ADR 0321). `force` bypasses only this guard, never rule 2.

**When the rows and the log disagree (ADR 0321).** The primary checks the voters of the `nodes` rows against the voters the change log gives at the current version, because counting the rows' voters at the log's version could let two changes pass on the same acknowledgements and leave the voters two changes away from the last acknowledged set (rules 1 and 2). While the two disagree on a machine, the primary keeps refusing every voter change under rule 2, with `409 conflict`. A machine that connects again ends the disagreement itself, because its `hello` writes its row to the log. For a machine that never connects again, the repair is removing it (section 10): its removal entry ends the disagreement, and the gate lets that one change through. There is no other bypass, and no `force` for rule 2.

### 8.11 Maintenance window (M5b)

A planned restart of the primary, for an upgrade, would otherwise fence every `warm` app whose node cannot renew within about `D` (ADR 0171). Before it, the operator or the installer's upgrade path runs, as root on the primary's host:

```
quoro cluster maintenance [--duration <seconds>]
```

The command reaches the running primary over a local control socket, `/run/quoro/control.sock`, which only root and the service user may open; nothing about it is exposed on the network. The primary then:

1. asks its voters for support with `max_lease_ms` = `W`, where `W` is the duration given (default 180 s, at least `D`, at most 600 s);
2. once a majority supports `W`, raises `leases.duration_ms` to `W` for every `expiring` lease, and once that raise is replicated (section 8.4, "When R is replicated", ADR 0268), answers every renewal with `duration_ms` = `W` while the window is open;
3. writes one audit record, `cluster.maintenance` (audit-log-format.md section 3.7).

The command returns success once every `expiring` lease has had a renewal answered with `W`, and prints until when the last of them is covered. It fails when no majority supports `W`, when the raise is not replicated within `D / 3` plus one margin, because a machine with the `node` role is down or not following (ADR 0289), or when a holder has not renewed within `D / 3` plus one margin; the leases already answered keep their longer validity. Until the raise is replicated, renewals are answered with `D`; when it is not replicated in time, the window does not open, as when no majority supports `W`. The window closes at the primary's next start, with `quoro cluster maintenance --end`, or `W` after it opened, whichever comes first; renewals are then answered with `D` again, and the gates follow section 8.4, so the longer grants keep their longer gates.

The installer runs the command before it restarts `quoro.service`. When it fails, the installer says that `warm` apps fence if the restart takes longer than about `D`, and goes on with the upgrade, because fencing is the safe outcome. An unplanned outage of the primary longer than `D` still fences `warm` apps by design; the HA view says so, and M6's election removes it.

## 9. Node return

A node that comes back after a failure may hold writes nobody else has and may be missing everything that happened while it was gone, which is why failback is never automatic (D-0022).

Order on reconnect:

1. `hello`, carrying `stored_rev`, `applied_rev`, `log_seq`, `log_hash` and every lease the node believes it holds.
2. The primary revokes every stale lease: held by a node the primary no longer considers the holder, or carrying a lower token than the primary's, except a lease whose marked row's newest write gave it a new token and names the node as holder: the primary sends that grant instead, or after the revoke when the node lists the lease `held` or `uncertain` in the other mode, and sends it too for such a lease the node does not list (section 5.1, section 8.4 "The holder fenced first" step 4, ADR 0280, ADR 0295, ADR 0299). When a node that the lease row does not name lists the lease `held` or `uncertain`, the primary revokes it at the token the node reports, which section 5.13 accepts, whether or not the row carries the mark. It does not raise an unmarked row's token for it; only a marked row's token is raised (section 8.4, "The mark", ADR 0285). So when the node reports a token above an unmarked row's, a later grant with a new token may issue that same token again, as section 7.3 says for `--ack-fence`. The node fenced before it answered the revoke, so no second copy runs, but storage cannot reject a write of that node still in flight at the same token. This is a stated limit (ADR 0285). The holder that the row names is another case: when it lists the lease `held` or `uncertain` in `local` mode at a higher token, the primary writes the row at that token, while the row is not revoked and the lease has no failed fence (section 8.4, "Writing the row `local`"). A node declared lost gets every lease it names revoked here with reason `declared_lost`, and its `lost_at` is cleared once the revokes are answered (section 8.3). The node fences those first, before anything else (section 4.2).
3. Only after fencing, the primary sends a full `desired_state`. A node that returned fenced starts nothing stateful, exactly as D-0022 step 1 requires.
4. The node reconciles. Because the snapshot is complete and `rev` is monotonic, reconciliation cannot start a duplicate app: a stateful VM appears in the snapshot only if the node holds the matching lease token, which it does not until the primary grants it again.
5. Log follow resumes. If the hash chain no longer matches, the divergent copy is quarantined and the node refollows from `seq` 1 (section 7.1).

Divergent local writes are kept as a quarantined snapshot and recorded in the audit log from M5b (D-0022 step 3, D-0031). There is no automatic failback: the default is that roles simply swap, and a return to the previous node is a planned switchover with a write freeze, a final sync and a lease move (D-0022).

**A returning source (M5b, ADR 0176).** A node that was the source of a warm volume and lost the lease (a failover while it was cut off, or a switchover) finds, in its first `desired_state` after the stale-lease revokes, that it is now the target of that volume. Before it accepts a stream for it, the node:

1. compares its live volume file with its base, the newest snapshot the target answered `complete` for, through `volume_changed_ranges` (section 6.13, `engine-trait.md` section 3.22). Only a comparison that is not `full` and finds no changed range proves that the file holds no divergent write: no block changed since the base, so every later snapshot, and the target's copy whichever it is, holds the same data. Then it deletes the file (ADR 0314);
2. otherwise keeps the live file as a **quarantined copy**. That includes a file that shows no change only against the snapshot it was shipping: the target may never have completed that snapshot, and a lost answer looks the same, so the file can hold writes the target lacks. It includes a `full` comparison and a missing or unusable base too. A file the target does hold, because it completed that snapshot and only its answer was lost, is then quarantined as well, and an operator deletes it. The copy is a reflink clone, which costs only the diverged blocks, recorded in its store (section 12) with a new `qrn_` id and reported in `status_report.quarantines` with its size and, once computed, its SHA-256. The primary records it, writes the audit record and emits `volume.quarantined` (section 5.3), also when the volume is already gone (below). The node keeps the copy's record until `spec.quarantine_deletes` names it, with one exception. It may forget a record, with its kept snapshot, only when all three hold: the engine answers `NotFound` for the clone, looked up as a snapshot of the volume in the volume's current directory (`engine-trait.md` section 3.22; a pool that does not mount answers `Unsupported` instead, section 3.16); another file the node records for the volume, or the volume's live file, is present in that directory, so the directory is the real one; and the record has no SHA-256, so the primary never recorded the copy and no operator can see or delete it. Forgetting it loses nothing, because the clone is already gone. A record with a SHA-256 keeps waiting for `quarantine_deletes`;
3. keeps its snapshots as candidate copies. The new source ships from its base, the copy it started from, and names it in `base_seq`: when the target holds that snapshot, only the ranges changed since are shipped, an incremental resync from the last common point. The other candidate is deleted once that stream completes. That drops no write, because step 2 kept as a quarantined copy every file that changed since the base, whichever snapshot the new source started from. When the target holds neither, it answers `base_mismatch` and the new source ships a full copy.

A returning source therefore starts nothing stateful, keeps its divergent writes and becomes the `warm` copy (D-0022 steps 1 to 4). Nothing deletes a quarantined copy automatically: only an operator does, with `DELETE /api/v1/volumes/{volume_id}/quarantines/{quarantine_id}`, a human-only call that reaches the node as `spec.quarantine_deletes` (section 5.11). A switchover's old source holds no divergent write, because its VM was paused before the final snapshot and stopped after it, so step 2 does not happen. A quarantined copy on a node that never reconnects is removed from the primary's records only when an operator removes the node (section 10); until then `deleteVolumeQuarantine` leaves it `deleting`.

**A stale-copy `deleted` entry before the hand-over.** When one reaches a returning source before its hand-over, the node keeps its own file `orphaned`, with its kept snapshots (section 6.7). If the node is named the volume's target again, the hand-over compares the file against those snapshots, as above.

**A copy of a volume that is gone (ADR 0316).** A returning source can report a quarantined copy after its volume was deleted, by `deleteVolume` or `deleteApp` while the node was away, or while the volume reads `deleting`. Its divergent writes are still recorded, never dropped (ADR 0022, ADR 0176):

- The first time a report lists such a copy with its `sha256`, the primary writes the system audit record `volume.quarantine` and emits `volume.quarantined`, with the same fields as for any copy.
- It does both once per `quarantine_id`. A later report of the same copy, also after a restart of the primary, finds that record and writes nothing.
- It writes no `volume_quarantines` row, and the schema does not change. The copy is therefore not listed in `Volume.quarantines`, `deleteVolumeQuarantine` cannot reach it, and it never blocks a delete (`has_quarantines`).
- The node keeps it and keeps reporting it, as it does a removed node's copies (section 10). The operator removes its file on the node.

The old source still has the app's VM, fenced or stopped by the revoke. Its desired state now lists that VM as `standby` (section 5.11), so the node deletes it before step 1, keeping its volume (`engine-trait.md` section 3.9): the engine compares and replaces a volume file only while no VM that attaches it exists (`engine-trait.md` section 3.22).

## 10. Node removal

```
DELETE /api/v1/nodes/{node_id}
```

- The scope is `node:remove`, destructive and human-only (`README.md`), so an agent cannot remove a node.
- If the node is connected, the primary first sends `lease_revoke` for every lease the node holds and waits for each `command_result`, so the node fences its stateful VMs; then it ends the session with a fatal `error` code `node_removed` and marks the node `removed`. If the node is not connected, removal increments the fencing token of each of its leases, exactly like declaring the node lost (section 8.3), so the old copy can no longer write to shared storage. Only then may the app start elsewhere; the node itself is not asked to destroy anything.
- The node stops dialing permanently. It leaves its stateless VMs running and its fenced stateful VMs stopped, because deleting them is a data decision, not a protocol step.
- From M5b, removing a voter changes the voter set: it follows section 8.10, so it answers `409 conflict` while the previous change has not reached a majority of voters. While the `nodes` rows and the change log disagree on a machine that never connects again, every voter change is refused, and removing that machine is the one change the gate lets through, because its removal entry ends the disagreement (section 8.10, ADR 0321). Removal is not refused for leaving fewer than three voters; the HA view then reports that HA needs a third vote. A removed machine with the `node` role still counts in the required set of section 8.4 until its removal entry is replicated over the machines of that set that are not removed (ADR 0261, ADR 0280), so removing a node that holds back the end of an `expiring` lease's renewals lets that end go ahead only once the nodes that are not removed hold the removal.
- From M5b, removing a node deletes the custody seals made for it (section 7.2) in the same transaction, and no longer counts it for a signing-key rotation, which may then complete (section 7.5).
- From M5b, removing a node drops the records of the quarantined copies it keeps (section 9, `volume_quarantines`) in the same transaction, so a node that is gone for good no longer blocks `deleteVolume` or `deleteApp` (`has_quarantines`). The copies themselves stay on its disk with the rest of its data. A copy of a volume that is gone has no row to drop: only its audit record and its event exist (section 9, ADR 0316).
- Removing a node never destroys data remotely. Purging local state — disks, rootfs images, the node's store — is a local CLI command run on that machine by whoever has root there. A node that is gone for good therefore keeps its data until someone with access deletes it, which is the safe default.

## 11. Security notes

Threats and their mitigations are in `../architecture/threat-model.md`; this section states only what the protocol does.

- **Credential storage.** The node credential and the node's private key are stored root-only on the node. The primary stores only a hash of the credential.
- **Key custody and promotion (M5b).** Every machine with the `node` role can open its seals and so holds, once it opens them, the keys a primary holds: the signing key, the data-encryption key and the primary's TLS key (ADR 0078, ADR 0180). A seal opens only with that machine's custody key, which never leaves it, and its `info` binds it to one cluster, machine, kind and key (section 7.2). A machine cannot swap its custody key for one an attacker holds: the primary keeps the first key it saw and refuses a change. Promotion needs root on the candidate and a majority of voters, and a promoted primary keeps the certificate every node pins, so nodes follow it without a re-pin; a returning old primary learns the higher term and demotes itself (sections 7.3 and 7.4). The node credential hashes are on every follower; they are hashes of 32 random bytes, so they do not yield a credential.
- **Desired-state signature.** A node accepts a snapshot only if the Ed25519 signature verifies against the cluster public key it received at enrollment. A compromised intermediate or a different cluster cannot make a node start an arbitrary microVM. The signature does not protect against the primary itself: whatever the primary signs, the node runs. The signing key rotates with a `signing_key_rotate` signed by the old key (D-0050), so a new key is accepted only from the holder of the old one.
- **Certificate pinning.** The node refuses a TLS certificate whose SHA-256 differs from the fingerprint in the join command, so a self-signed primary is as safe as a CA-issued one, and a stolen DNS name does not redirect a node.
- **Provider credentials and tunnel tokens (ADR 0094, ADR 0320).** A node's desired state carries no DNS or tunnel provider's account credential, and the backup bucket credential only while the node has a backup job (below). For a tunnel it carries only that node's tunnel run token, which cannot create tunnels or write DNS records. For ACME the node asks the primary to write `_acme-challenge.<hostname>` TXT records, and only for the `acme` routes of its own desired state (section 5.22). The change log reaches further: it carries every provider's account credential and every machine's tunnel run token, and every machine with the `node` role can read them (section 7.1). A compromised machine with the `node` role can therefore read the DNS and tunnel provider's API token, the backup bucket credential and the audit-copy credential. With them it can create tunnels, write any record in the zone, obtain certificates for any hostname in it and run any machine's tunnel. A machine with only the `edge` or `witness` role follows no log, so it holds none of them beyond its own desired state: an edge holds its own tunnel run token ("Edge and mesh" below).
- **A compromised primary** can read everything the cluster stores — state, ciphertext it can decrypt with the cluster data-encryption key, and the telemetry that nodes stream on demand — and can issue desired state, lease grants and commands to every node. It is also the lease authority, so it can grant the same lease to two nodes and run a stateful app twice; only a storage receiver that tracks the highest fencing token on its own side limits the damage. The voters of M5b do not change that: they see no leases, and they only stop a primary that has lost their majority, or an old primary after a promotion, from granting (section 8.8). It cannot read a node's local store or its private key.
- **Voters (M5b).** A voter's power is its support and its vote. A compromised voter can withhold support, which with the other voters' help can stop `expiring` renewals (availability, never a double run), and can vote for any candidate, but one vote is never a majority. The vote listener is reachable only through the mesh, only from voters with the `node` role, and its requests carry no secret. A witness holds no data, no seals and no credential of other machines.
- **A compromised node** can lie about observed state, emit false events and, within its lease, run whatever its stored desired state allows. Through its session it cannot forge desired state for other nodes, cannot mint credentials, and cannot write to shared storage with a stale token. That is why stateful data stays protected by the lease and the token rather than by the honesty of the node. Its own workloads are not all it holds, though. A machine with the `node` role also holds the keys a primary holds (key custody above) and every secret in the change log (above), so for secrets its blast radius is the whole cluster (ADR 0320). The rotation below applies as soon as one is suspected.
- **When a machine with the `node` role leaves or is suspected compromised (ADR 0320).** Treat every secret the change log carries as known to it. Once the machine is removed (section 10), or as soon as it is suspected, the operator rotates:
  1. every provider credential: a new credential issued at the provider, stored with `rotateProviderSecret`, and the old one revoked at the provider, since `rotateProviderSecret` only stops Quoro from using it. This covers the DNS and tunnel provider's token, the backup bucket credential and the audit-copy credential;
  2. every machine's tunnel: `deleteNodeTunnel`, then `setNodeTunnel`, which creates a new tunnel with a new run token. `deleteNodeTunnel` refuses while the node serves `tunnel` domains, so those domains are removed first and their traffic stops until they are added back;
  3. every webhook's signing secret, by deleting the webhook and creating it again, because `updateWebhook` cannot change it;
  4. the apps' secrets: new values at their source, then `replaceAppEnv`;
  5. the users' passwords: every user changes their own with `changePassword`, because the change log carries `users.password_hash` (ADR 0326). An Argon2id hash cannot be read, but a weak password can be guessed offline.

  The machine keeps the keys it opened. No operation rotates the data-encryption key, so the ciphertext it copied stays readable to it; the rotation makes what it can read worthless. The signing key rotates with `rotateSigningKey` (section 7.5) once that operation is served. Until then the machine keeps a key that nodes accept, and it reaches only the nodes that dial it (threat model T-NODE-16).
- **Terminal and console relay (M4).** The primary never touches a VM socket: it authorizes, audits and relays bytes between a client WebSocket and this session (ADR 0113, ADR 0114). The node agent, already root and already the only reader of VM sockets, is the only process that connects to a VM's vsock socket and its serial socket; both live in the VM's runtime directory, which only root can enter. The node never creates a `<socket>_<port>` listener, so a guest cannot open a vsock connection to the host. The guest is untrusted: the node bounds every frame it reads from it and treats its output as bytes, never as protocol. A `terminal` stream exists only because the primary opened it; a node never starts an exec session on its own.
- **Builds (M4).** A build runs untrusted code: the Dockerfile and everything it fetches. It runs only inside a builder microVM, with `egress_only` networking and no isolation group (section 6.6), never on the host. Build secrets reach the node only inside the signed desired state, reach the builder VM only on its ephemeral source disk, which the node deletes when the build ends, and live in the guest only in memory as BuildKit secret files; the node never writes them to a build log, a `stream_data` or an event. The node never mounts a disk the builder VM wrote: it reads the output layout as a tar stream in user space and never touches the cache disk's contents. The built image never leaves the node: it is imported locally and never pushed.

- **Image inventories (M5a).** The inventory is built by parsing package databases and manifests inside images the operator did not write, as root on the host. The parsers are Rust, in the engine's process, bounded in size (section 6.11), and read files of the unpacked tree only: they never execute anything from the image. What leaves the node is the package list, over the session, never the image.
- **Edge and mesh (M5a).** Besides the certificates of the routes it serves, the edge holds its tunnel token and its WireGuard private key, and it reaches a node only through the mesh, where the node's firewall admits only the edge's mesh address and only to the guest addresses and ports of that node's backends (section 6.9). A compromised edge can read and alter the traffic of the apps it serves and probe their backends; it cannot reach other guests, the node's host services or other nodes, and it cannot change desired state. With one edge in M5a, the edge is a single point of failure for the traffic of apps with more than one replica.
- **Warm replication (M5b).** The target holds a full copy of the volume, so it can read the app's data like the source; the warm node is chosen among the nodes the app may run on (`openapi.yaml`, `App.warm`). The replication listener is reachable only through the mesh and only from the source's mesh address, and its streams carry no secret. A compromised source can ship any content, which it could also write through the app; it cannot ship with a token lower than the target has seen, so a source that lost its lease cannot overwrite the copy a failover started from. A compromised target can refuse streams, which shows as lag in `App.warm`, or hold a different copy than it reports; a failover to it starts from whatever it holds. A quarantined copy holds the app's data and stays on its node's disk until an operator deletes it.
- **Volumes and backups (M5a).** A volume file is guest-written and never mounted on the host; snapshots, backups and restores handle it as bytes (section 6.7). A node's desired state carries the chunk keys of the apps it has jobs for, and the bucket credential only while it has a job; it never carries the repository key or the audit-copy credential. The repository key is in no change-log payload either, so a compromised node reads no backup of an app it was never given a job for. A machine with the `node` role can still read the bucket credential and the audit-copy credential from the log (above). With the bucket credential it can delete objects in the repository, which destroys backups. It can also upload chunks or manifests, which no seal covers and a restore refuses (`backup-format.md` section 8.1). What it can do to the audit copy is bounded by the bucket's lock (`audit-log-format.md` section 8).

## 12. Local node store

The node keeps a SQLite file, owned by root, that holds everything it needs to run without the control plane (D-0011, D-0015 for the choice of SQLite elsewhere). The tables below are described, not specified: they are local to the node, are not part of `db-schema.sql`, are never replicated, and their exact columns may change without a contract change.

| Table | Holds | Why |
| --- | --- | --- |
| `node_meta` | `node_id`, `cluster_id`, `boot_id`, `host_boot_id` (the kernel boot id), the failed mark and the failed fences it covers (cleared by `command` kind `fence_ack`, section 5.17), agent and engine versions, the cluster public key and its `key_id`, a reference to the credential; from M5b the highest term seen, the newest `welcome.schema_version`, and on a voter its vote state: the candidate it voted for in that term, the primary it supports with the `S`, `L` and `v_recv` of the last request it granted (its first start after a reboot), and the promise and lease horizon, the instants on `CLOCK_BOOTTIME` next to the kernel boot id (section 8.8); on a machine with the `node` role, whether a promotion on this host froze the agent (section 7.3) | Identity across restarts; a voter's promise survives an agent restart and errs late after a reboot; a frozen agent stays frozen across a restart |
| `desired_state` | One row per accepted `rev`: the full signed body, its hash, `issued_at`, `term` | The snapshot to reconcile from, and the signature to re-verify |
| `applied_state` | Per VM the last applied spec digest, observed status, restart count, last exit code | Detects drift without the primary |
| `leases` | Lease id, resource, mode, token, state, `valid_until`, duration, remaining validity on `CLOCK_BOOTTIME` for `held` and `uncertain` expiring leases | Survives an agent restart: a fenced lease stays fenced, a held one keeps its remaining validity within the same host boot |
| `event_buffer` | Buffered events with their `evt_` id and time, plus the dropped counter | Bounded at 10 000; delivered after reconnect |
| `change_log` | Followed entries in `seq` order with `prev_hash` and `hash`, the last acked `seq`, and from M5b the primary's `head_seq` as last reported with its time, and while the node refollows after a quarantine, the refollowing state with the `head_seq` it refollows to, or "head unknown" until it has one (section 7.1) | Verifies the chain, resumes follow, and is what `quoro cluster promote` rebuilds the database from (section 7.3); the refollowing state survives an agent crash at any point |
| `quarantine` | Metadata for each quarantined divergent copy, of the change log (section 7) and, from M5b, of a volume (section 9): path, time, reason, and for a volume its `qrn_` id, volume, size and SHA-256 | Nothing divergent is deleted (D-0022) |
| `warm` (M5b) | Per warm volume: role, peer, the base and the snapshot being shipped with their `snapshot_seq`, the cell hashes of the base and of the complete copy, the highest token seen in a stream, whether the VM is frozen by a switchover and the freeze's `lease_id` and `fencing_token`, and on a returning source its candidate snapshots | Resumes replication after an agent restart without a full copy, and keeps a frozen VM frozen, or stopped, across an agent or host restart (section 5.17) |
| `cmd_results` | `msg_` id and outcome of the last answered intrusive messages | Makes a redelivered `lease_revoke` idempotent |
| `builds` (M4) | Per build: `bld_` id, app, status, phase, resolved commit, image digest, failed step, the path of its log file, start and end times | Reports build progress in `status_report` across an agent restart; keeps the logs of the last 20 builds of each app (section 6.6) |
| `transcripts` (M4) | Per terminal session: `trm_` id, VM, file path, size, SHA-256, start and end times | Serves `stream_open` kind `transcript` and enforces retention: 30 days and 1 GiB per node, oldest removed first (`audit-log-format.md` section 3.3) |
| `volumes` (M5a) | Per volume: `vol_` id, app, file path, size, state, last error | Reports volume states across an agent restart, and tells an `orphaned` file from one in `spec.volumes` |
| `edge_backends` (M5a, edge) | Per backend of `spec.edge.routes`: in rotation or not, since when, reason, consecutive probe results | Lets a restarted edge resume with its last view of backend health instead of sending requests to backends it had taken out |
| `backups` (M5a) | Per backup job: `bkp_` id, status, snapshot method, pause, from M5b whether the guest flushed (ADR 0282), manifest key and digest, sizes, error, start and end times; optionally the chunk ids the node knows the repository holds, per app | Reports backup progress across an agent restart and skips uploads of known chunks (`backup-format.md` section 5) |

Outside the SQLite file, next to the credential and the node's private key, the node keeps its WireGuard private key (from M5a, mode `0600`), on a machine with the `node` role its custody private key `custody.key` (from M5b, mode `0600`, section 7.2) and, next to it, the mark of section 7.1: a file holding the highest change-log `seq` the node has acknowledged, fsynced before the `log_ack` or `hello` that reports it, set to 2^53 - 1 at a quarantine of the log before the log is moved aside, and lowered to the `head_seq` the node refollows to once it learns it (section 7.1; from M5b, ADR 0261, ADR 0280), and its identity file: `node_id`, `cluster_id`, `primary_url`, the URLs advertised in `welcome.primary_urls` (at most 16, section 5.10), the pinned certificate fingerprint, and the cluster public key with its `key_id`. It is written `0600` through a temporary file renamed into place, so a half-written identity is never read. Also outside the SQLite file, in the VM's runtime directory, each VM also has its serial ring: two segments of 512 KiB that keep the newest serial output across VMM restarts (`engine-trait.md` section 3.19). The ring is removed with the VM. From M5a each prepared image's inventory is a file next to the image in the engine's cache (section 6.11), so the node answers `inventory_resend` without inventorying the image again. Metric samples are never stored: they live in memory only (section 5.17).

The store is the reason a node can be restarted, upgraded or disconnected without stopping apps: it holds the signed intent, the applied reality, and the fencing state, and nothing in it depends on the control plane being reachable.
