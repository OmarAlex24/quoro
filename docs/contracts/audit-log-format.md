# Audit log format

Status: M0 draft for review.

This contract fixes the shape of the audit log, how records chain, how the chain is verified and how a copy is written off-site. It implements D-0031: the audit log is append-only, hash-chained, optionally copied to S3-compatible storage with object lock, and MCP writes through the same path as the HTTP API. It also implements the "everything must be reconstructable" requirement of D-0029 and rides on the replicated change log of D-0013.

The permission rules that decide *whether* a call may happen are in [permission-model.md](permission-model.md). This file assumes that decision has been made and describes what is written once it has.

Contracts freeze at the end of M0; a change to this file after the freeze is a standalone pull request (D-0036).

## 1. Purpose and principles

An operator of a self-hosted cluster has to be able to answer, months later: which agent restarted which app, on whose authority, because of which event, with what parameters, and did it work (D-0029, D-0031). The audit log is the only place that answer exists. It is not a debugging log and not a metrics stream; it is the accountability record of every action taken against the cluster.

The principles:

1. **Append-only.** Records are inserted, never updated and never deleted (D-0031). There is no update endpoint, no delete endpoint and no retention job in M0-M6 (section 9).
2. **One record per action, in the action's transaction.** A record is written in the same SQLite transaction as the change it describes, so an action without its record cannot exist (D-0031; D-0013).
3. **Hash-chained.** Each record names the hash of its predecessor, so a modified or reordered record is detectable (D-0031; D-0013).
4. **Same path for every channel.** API, panel, CLI, MCP and Quoro's own automation write through one service layer and therefore one log (D-0031).
5. **Copyable out of reach.** An optional off-site copy with object lock exists so that the person operating the server cannot silently rewrite the record (D-0031; section 8).
6. **Records are not events.** The audit log and the event stream are different logs with different consumers (section 10).

## 2. Record format

An audit record has no prefixed id; it is identified by its `seq`, which is gap-free from 1 within the cluster's audit log (shared entity model; [db-schema.sql](db-schema.sql), table `audit_log`).

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `seq` | integer | yes | position in the audit log, gap-free from 1 |
| `time` | RFC 3339 UTC string | yes | when the record was appended |
| `actor_type` | `user` \| `agent` \| `node` \| `system` | yes | which kind of identity acted |
| `actor_id` | string | yes | the acting id (`usr_`, `agt_`, `nod_`) or the system subsystem name |
| `agent_model` | string | no | the agent's `model`, on agent records only |
| `on_behalf_of` | `usr_` id | no | the human whose authority was lent |
| `channel` | `api` \| `panel` \| `mcp` \| `cli` \| `system` | yes | how the action arrived |
| `operation` | string | yes | the endpoint (`DELETE /api/v1/apps/{app_id}`), the MCP tool name (`delete_app`), or `session.end` for the close of an interactive session (section 3.3) |
| `scope` | scope string | no | the scope the call was authorized under; absent when the call needed none |
| `target_type` | string | no | `app`, `vm`, `deployment`, `build`, `domain`, `project`, `node`, `incident`, `approval`, `grant`, `webhook`, `user`, `cluster` |
| `target_id` | string | no | id of that target |
| `project_id` | `prj_` id | no | project the target belongs to, when it has one |
| `params` | JSON object | yes | the request parameters, secrets redacted (section 4) |
| `justification` | string | no | the agent's stated reason; required on mutating agent calls, omitted for humans |
| `trigger_event_id` | `evt_` id | no | the event that woke the agent |
| `incident_id` | `inc_` id | no | the incident the call belongs to |
| `grant_id` | `grt_` id | no | the grant that authorized the call |
| `approval_id` | `apr_` id | no | the approval request behind an approval grant |
| `request_id` | `req_` id | yes | correlation id of the call, echoed as `Quoro-Request-Id` |
| `result` | `ok` \| `refused` \| `error` | yes | outcome of the call |
| `result_detail` | string | no | the refusal reason or the error code |
| `change_seq` | integer | no | the change-log seq this action produced, when it changed state |
| `prev_hash` | 64 lowercase hex chars | yes | `hash` of record `seq - 1`; 64 zeros for `seq` 1 |
| `hash` | 64 lowercase hex chars | yes | hash of this record (section 5) |

Absent optional fields are **omitted from the JSON**, not set to `null`. This matters because the hash covers the serialized object: `null` and "absent" would otherwise be two encodings of the same record with different hashes (section 5).

### 2.1 Example: the M3 demo restart, by an agent, through MCP

An app on a node went down, an incident opened, the webhook woke an agent, and the agent restarted the app through the `restart_app` MCP tool (D-0033 M3 demo; D-0032).

```json
{
  "seq": 1041,
  "time": "2026-11-02T15:42:07Z",
  "actor_type": "agent",
  "actor_id": "agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
  "agent_model": "example-model-id",
  "on_behalf_of": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E",
  "channel": "mcp",
  "operation": "restart_app",
  "scope": "app:restart",
  "target_type": "app",
  "target_id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0F",
  "project_id": "prj_01JC9Z6Q5W8N3K4T7R2M1XHB0G",
  "params": { "app_id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0F" },
  "justification": "app down per incident inc_01JC9Z6Q5W8N3K4T7R2M1XHB0H; restarting its instance",
  "trigger_event_id": "evt_01JC9Z6Q5W8N3K4T7R2M1XHB0J",
  "incident_id": "inc_01JC9Z6Q5W8N3K4T7R2M1XHB0H",
  "grant_id": "grt_01JC9Z6Q5W8N3K4T7R2M1XHB0K",
  "request_id": "req_01JC9Z6Q5W8N3K4T7R2M1XHB0M",
  "result": "ok",
  "change_seq": 4077,
  "prev_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "hash": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

Reading it: the actor is an agent (`agt_...`), its model is recorded, it acted on behalf of `usr_...`, the channel was `mcp`, the operation was the `restart_app` tool under `app:restart`, the incident and the triggering event are both named, the grant that allowed it is named, the agent said why, the result was `ok`, and change-log entry 4077 is the restart itself (D-0029, D-0031).

The two hash values above are **illustrative placeholders, not computed hashes**; the `prev_hash` is drawn from a fixed pattern only so the example is readable.

### 2.2 Example: a refusal

An agent holding a read-only grant attempted `delete_app`; the call was refused before anything happened (D-0030, D-0031).

```json
{
  "seq": 1042,
  "time": "2026-11-02T15:43:11Z",
  "actor_type": "agent",
  "actor_id": "agt_01JC9Z6Q5W8N3K4T7R2M1XHB0D",
  "agent_model": "example-model-id",
  "on_behalf_of": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E",
  "channel": "mcp",
  "operation": "delete_app",
  "scope": "app:delete",
  "target_type": "app",
  "target_id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0F",
  "project_id": "prj_01JC9Z6Q5W8N3K4T7R2M1XHB0G",
  "params": { "app_id": "app_01JC9Z6Q5W8N3K4T7R2M1XHB0F" },
  "justification": "asked to remove the app; checking whether that is allowed",
  "grant_id": "grt_01JC9Z6Q5W8N3K4T7R2M1XHB0N",
  "request_id": "req_01JC9Z6Q5W8N3K4T7R2M1XHB0P",
  "result": "refused",
  "result_detail": "approval_required",
  "prev_hash": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
  "hash": "f0e1d2c3b4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0f"
}
```

A refusal has no `change_seq`, because nothing changed. `result_detail` carries the reason from the permission contract's refusal table, so the M3 demo can print "grant expired" or "approval required" straight from the audit record (docs/product/roadmap.md, M3 Definition of Done; D-0030).

### 2.3 Example: an approval decision by a human

The same agent filed an approval request, and a human approved it in the panel. The approval created a grant, so the record has a `change_seq` (D-0030, D-0029).

```json
{
  "seq": 1043,
  "time": "2026-11-02T15:47:52Z",
  "actor_type": "user",
  "actor_id": "usr_01JC9Z6Q5W8N3K4T7R2M1XHB0E",
  "channel": "panel",
  "operation": "POST /api/v1/approvals/{approval_id}/approve",
  "scope": "approval:decide",
  "target_type": "approval",
  "target_id": "apr_01JC9Z6Q5W8N3K4T7R2M1XHB0Q",
  "project_id": "prj_01JC9Z6Q5W8N3K4T7R2M1XHB0G",
  "params": { "approval_id": "apr_01JC9Z6Q5W8N3K4T7R2M1XHB0Q", "decision": "approved", "decision_note": "confirmed with the app owner" },
  "approval_id": "apr_01JC9Z6Q5W8N3K4T7R2M1XHB0Q",
  "request_id": "req_01JC9Z6Q5W8N3K4T7R2M1XHB0R",
  "result": "ok",
  "change_seq": 4078,
  "prev_hash": "f0e1d2c3b4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0f",
  "hash": "6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c"
}
```

A human record has no `agent_model` and no `on_behalf_of` (the human acted for themselves), and no `justification` (the `decision_note` is in `params`). The grant produced by this decision is a separate change-log entry, and the audit record for the destructive call that follows names both `approval_id` and `grant_id` (permission-model.md, section 9).

### 2.4 Reading the log

- `GET /api/v1/audit` requires `audit:read`, takes `limit` (default 50, max 500), `cursor` and `after_seq`, and returns `{ "items": [...], "next_cursor": ... }` (shared entity model, "Pagination and lists").
- Records are returned in `seq` order, oldest first (D-0031).
- An agent's selector filters the rows it may see; the list is filtered, never refused (permission-model.md, section 5.1; D-0030).
- There is no endpoint that updates or deletes a record. `GET /api/v1/audit` and `POST /api/v1/audit/verify` are the only audit operations in [openapi.yaml](openapi.yaml) (D-0031).

## 3. What is recorded

Each call or action in the rows marked "yes" below produces exactly one record (D-0031):

| Event | Recorded | `result` |
| --- | --- | --- |
| every mutating call by any identity, allowed | yes | `ok` |
| every mutating call that is refused | yes | `refused`, `result_detail` = reason |
| every mutating call that is authorized and then fails | yes | `error`, `result_detail` = error code |
| every call by an agent, including reads (`get_app`, `list_audit`, `get_app_logs`) | yes | `ok` / `refused` / `error` |
| reads by humans (panel browsing, API reads) | no | — |
| approval requests filed, approved, denied, expired | yes | `ok` |
| system actions: incident opened and resolved, incident grant issued and revoked, fencing-related lease revocations, node enrollment and removal | yes | `ok` / `error` |
| audit verification runs | yes | `ok` / `error` |
| the open of a terminal or console session (its `POST`) | yes, for every identity, as a mutating call | `ok` / `refused` / `error` |
| the close of a terminal or console session, for any reason | yes: one `session.end` record (section 3.3) | `ok` / `error` |
| a session transcript fetch, by any identity | yes, also for humans, because a transcript holds secrets (section 3.3) | `ok` / `refused` / `error` |
| bytes, keystrokes and control frames inside a session | no; the transcript holds them on the node, and `session.end` carries its hash | — |
| authentication failures (no resolvable identity) | no audit record; server log only | — |

Why the split falls where it does:

- **Agents are recorded even on reads** because an agent's reading pattern is part of the risk the log exists for: what it looked at before it acted is evidence (D-0029, D-0031). The M3 demo reconstructs a whole incident from these records.
- **Human reads are not recorded** because the volume would bury the signal; a human's mutating calls and their approval decisions are recorded (D-0031).
- **Authentication failures have no identity**, so there is nobody to attribute the record to. They are counted in the server log and in metrics, not in the chain (D-0031).

System records exist so that "the system did it" is not a blank space in the story: an incident grant that appears from nowhere, or a lease revoked because a node lost it, would otherwise be an unexplained state change (D-0029; D-0013). Node enrollment and removal are recorded because they change who is in the cluster (D-0011, D-0031).

### 3.1 Atomic with the change

An authorized mutating call writes, in one SQLite transaction:

1. the change-log entry (the append-only state change, D-0013);
2. the audit record, with `change_seq` set to that entry's `seq`;
3. any grant bookkeeping, such as `uses += 1` (permission-model.md, section 7.3).

If the transaction rolls back, none of the three exists. An audit record with a `change_seq` therefore always points at a change that happened, and a change-log entry produced by an API or MCP call always has an audit record pointing at it (D-0031).

`change_seq` is absent for calls that changed nothing: refusals, reads, and errors before any write.

### 3.2 Replication of audit records

The audit chain is separate from the change-log chain, but audit records travel on the change log: each audit record is also replicated to replica nodes as a change-log entry of `entity_type` `audit` (D-0013). A replica node therefore verifies two chains, and the audit chain reaches replicas only through a verified change-log stream (D-0013; [node-agent-protocol.md](node-agent-protocol.md)).

The `audit_log` table itself is the primary's copy; it is not a local table (see the "local" markings in [db-schema.sql](db-schema.sql)).

### 3.3 Interactive sessions (M4)

A web terminal or VM console session lasts minutes, and one record per call does not describe it (ADR 0115). A session therefore leaves exactly two records, the open and the end, paired by the session id.

**The open** is the normal record of `POST /api/v1/apps/{app_id}/terminal-sessions` (scope `app:terminal`, `target_type` `app`) or `POST /api/v1/vms/{vm_id}/console-sessions` (scope `vm:console`, `target_type` `vm`). Its `params` are the request body plus the values the primary resolved: `session_id` (the `trm_` id the call minted, so the end record can be paired with it), `vm_id`, and for a terminal `rows`, `cols`, `command` (the argv that runs, after the default is applied) and `user` (absent when the app's own user applies); for a console `tail_bytes`. The single-use ticket is never in `params`. A refused or failed open writes its record and no session exists.

**The end** is a record the primary writes when an opened session closes for any reason, including a ticket that was never used and a primary restart that lost the session (`interactive_sessions` in [db-schema.sql](db-schema.sql) migration 0005 lets it close those at start-up):

| Field | Value |
| --- | --- |
| `actor_type`, `actor_id`, `agent_model`, `on_behalf_of`, `channel` | the same as the open's |
| `operation` | `session.end` |
| `scope` | the open's scope (`app:terminal` or `vm:console`) |
| `target_type`, `target_id`, `project_id` | the open's |
| `grant_id` | the open's, when an agent opened it |
| `request_id` | the `req_` id of the `DELETE /api/v1/interactive-sessions/{session_id}` that ended it, or a new `req_` id minted by the primary for any other close; never the open's id, which belongs to the open call |
| `result` | `ok`, or `error` when `close_reason` is `error` or `node_disconnected` |
| `result_detail` | absent, or the error code when `result` is `error` |
| `change_seq` | absent; a session changes no replicated state |
| `params` | the object below |

| `params` field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `session_id` | `trm_` id | yes | the session, as in the open's `params` |
| `kind` | `terminal` \| `console` | yes | |
| `node_id` | `nod_` id | yes | the node that ran the session and holds its transcript |
| `vm_id` | `vm_` id | yes | the VM the session was attached to |
| `opened_at` | RFC 3339 | yes | when the open was recorded |
| `connected` | boolean | yes | whether a WebSocket ever attached; false when the ticket expired unused |
| `duration_ms` | integer | yes | from the open to the close |
| `bytes_in` | integer | yes | bytes the client sent into the session (always 0 for a console) |
| `bytes_out` | integer | yes | bytes the session sent to the client |
| `exit_code` | integer | no | the program's exit code, terminal sessions whose program exited |
| `close_reason` | string | yes | one of `exited`, `ended_by_client`, `ended_by_request`, `ticket_expired`, `idle_timeout`, `max_duration`, `access_revoked`, `credential_expired`, `vm_stopped`, `node_disconnected`, `transcript_limit`, `primary_restarted`, `error` (meanings in [openapi.yaml](openapi.yaml), `InteractiveSessionCloseReason`) |
| `ended_by` | `usr_` or `agt_` id | no | who called the `DELETE`, when `close_reason` is `ended_by_request` |
| `transcript_sha256` | 64 lowercase hex chars | no | SHA-256 of the transcript file exactly as the node stored it; terminal sessions that connected |
| `transcript_bytes` | integer | no | size of that file |

Because `session.end` is chained like every record, a transcript edited after the session ended no longer matches `transcript_sha256`, and the edit is evident (ADR 0031, ADR 0115).

**Transcripts.** A terminal session's transcript is recorded by the node that runs it, never sent to the primary unless a human fetches it, and never part of the audit log, the change log or the off-site copy (ADR 0014, ADR 0115):

- **Format:** asciicast v2 (newline-delimited JSON): a header line `{"version": 2, "width": <cols>, "height": <rows>, "timestamp": <unix seconds>, "env": {"TERM": ...}}`, then one event line per chunk, `[<seconds since start>, "i" | "o" | "r", <data>]`: `i` input, `o` output, `r` resize as `"<cols>x<rows>"`. Bytes that are not valid UTF-8 are written with U+FFFD replacement in `<data>`; the hash covers the file as written.
- **Cap:** 16 MiB per session. When the next chunk would pass the cap the node ends the session with `close_reason` `transcript_limit`, so no part of a session is unrecorded.
- **Console sessions have no transcript.** They are read-only and show the VM's own serial output, which the node already keeps in its console ring (ADR 0116); their `session.end` carries no transcript fields.
- **Retention:** 30 days on the node, and at most 1 GiB of transcripts per node, the oldest deleted first. Deleting the app or its VM does not delete them; removing the node or purging its data does. A deleted transcript leaves its `session.end` record, whose hash then has nothing to verify.
- **Access:** `GET /api/v1/interactive-sessions/{session_id}/transcript`, which needs `audit:read` and a human caller (permission-model.md, section 7.6). The primary relays the file from the node over the node session, on demand; it answers `node_unreachable` (504) when the node is not connected and `not_found` (404) when the node no longer has the file. Every fetch is recorded, with `target_type` `vm`, the session's VM as `target_id` and `params` `{session_id}`.

### 3.4 Password changes (M4)

`POST /api/v1/auth/password` (ADR 0135, [openapi.yaml](openapi.yaml), `changePassword`) writes one record per call that reached an identity, with the user as actor and as target (`target_type` `user`, `target_id` the user's id), no `scope` and `channel` `panel`:

| Outcome | `result` | `result_detail` | `change_seq` |
| --- | --- | --- | --- |
| password changed | `ok` | absent | the change-log seq of the `users` update |
| `current_password` does not match | `refused` | `wrong_password` | absent |
| attempt rate limited | `refused` | `rate_limited` | absent |
| `new_password` breaks the password rule or equals the current one | `error` | `validation_failed` | absent |

`params` carries both keys, redacted (section 4): `{"current_password": "[redacted]", "new_password": "[redacted]"}`. The `wrong_password` records are the trail of someone guessing a password with a stolen session. A request without a session cookie, or with an `Authorization` header, has no identity to attribute and is not recorded (section 7).

## 4. Redaction

Requests can carry secrets. The audit record stores the request parameters, so it must not become a second secret store (D-0031).

The redaction rule:

- The values of the keys `value`, `values`, `secret`, `password`, `current_password`, `new_password`, `token` and `key` are replaced by the string `"[redacted]"`, at any depth of the `params` object.
- **Every environment variable value** is replaced by `"[redacted]"`, whatever the key is called. Environment variable *names* are kept, because a name is not a secret and the record of which variable changed is the useful part (D-0031; [openapi.yaml](openapi.yaml), `app_env`).
- Everything else is stored as sent.

Build arguments (`source.build_args[].value` in [openapi.yaml](openapi.yaml)) are not secrets, since they end up in the image's history, but they sit under the key `value` and are therefore redacted like every other `value`. Build secrets are passed by name only (the names of app environment variables), so their values never reach a request.

Provider credentials are covered by the `secret` key: the requests that carry them (`POST /api/v1/providers` and `POST /api/v1/providers/{provider_id}/rotate` in [openapi.yaml](openapi.yaml)) put the credential under `secret`, so it is redacted like any other secret (D-0051).

This list is part of the contract: adding a key to it is a contract change, because a verifier or a consumer that expects an unredacted field would otherwise silently see a different record.

```json
{ "name": "DATABASE_URL", "value": "[redacted]" }
```

Redaction happens before serialization and therefore before hashing: the hash covers the redacted record, which is what is stored and what the chain verifies (section 5). This is deliberate — the record must be verifiable by anyone who can read it, including an auditor holding only the exported copy.

A parameter that is a secret but whose key is not on the list is an implementation bug to fix in the redaction list, not a reason to hash the unredacted value (D-0031).

## 5. The hash chain

Each record's `hash` is:

```
hash = lowercase_hex( SHA-256( JCS( record_object_without_hash ) ) )
```

where `JCS` is the JSON Canonicalization Scheme (RFC 8785) and `record_object_without_hash` contains every field of the record except `hash`, with absent optional fields omitted rather than null (shared entity model, "Audit record"; the change log uses the same construction, D-0013).

- `prev_hash` of `seq` 1 is 64 zeros (`"0000000000000000000000000000000000000000000000000000000000000000"`).
- For `seq` > 1, `prev_hash` equals the `hash` of `seq - 1`.
- `seq` is gap-free from 1; there is no hole and no duplicate (D-0031).

Because the serialization is canonical, a record can be re-hashed by any implementation that has the record's JSON: field order, whitespace and number formatting do not affect the result (RFC 8785, cited by the shared entity model).

### 5.1 Worked chain (illustrative)

Two records, with hashes shortened for reading. The real values are 64 hex characters.

| `seq` | Key fields | `prev_hash` | `hash` |
| --- | --- | --- | --- |
| 1 | `actor_type` `system`, `operation` `POST /api/v1/incidents/{incident_id}/resolve` | `0000…0000` (64 zeros) | `H1` |
| 2 | `actor_type` `agent`, `operation` `restart_app`, `grant_id` `grt_A` | `H1` | `H2` |

To verify record 2:

1. Take the JSON object of record 2 without `hash`.
2. Canonicalize it with RFC 8785.
3. SHA-256 it; the lowercase hex must equal `hash`.
4. Check that `prev_hash` equals record 1's `hash`.
5. Check that `seq` is 2, one more than record 1's `seq`.

Editing any field of record 2 changes `H2`; editing any field of record 1 changes `H1`, which breaks record 2's `prev_hash` as well. That second effect is the point of the chain: one edited record makes every later record fail.

**The `H1`/`H2` values above are placeholders, not computed hashes.**

### 5.2 What the chain is separate from

- **The change-log chain** (D-0013) chains state changes; the audit chain chains records about actions. They have independent `seq` spaces and independent head hashes, and a verifier must not mix them.
- The two are joined per record by `change_seq`, which points from an audit record to the change it produced, not the other way round (section 3.1).

## 6. Verification

`POST /api/v1/audit/verify` requires `audit:read` and re-walks the chain. The algorithm:

```
prev = 64 zeros if first_seq == 1, else the hash of record first_seq - 1
expect_seq = first_seq
checked = 0
for record in audit_log ordered by seq, from first_seq to last_seq:
    if record.seq != expect_seq          -> broken: reason "seq_gap", broken_at_seq = record.seq
    h = sha256_hex(jcs(record without hash))
    if h != record.hash                  -> broken: reason "hash_mismatch", broken_at_seq = record.seq
    if record.prev_hash != prev          -> broken: reason "prev_mismatch", broken_at_seq = record.seq
    prev = record.hash
    expect_seq += 1
    checked += 1
```

The result object:

```json
{
  "ok": true,
  "checked": 1043,
  "first_seq": 1,
  "last_seq": 1043,
  "head_hash": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
}
```

and on failure:

```json
{
  "ok": false,
  "checked": 517,
  "first_seq": 1,
  "last_seq": 1043,
  "head_hash": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
  "broken_at_seq": 518,
  "reason": "hash_mismatch"
}
```

| Field | Meaning |
| --- | --- |
| `ok` | true only if every record verified |
| `checked` | records verified before the walk stopped |
| `first_seq`, `last_seq` | the range walked |
| `head_hash` | `hash` of `last_seq` |
| `broken_at_seq` | the first record that failed; absent when `ok` is true |
| `reason` | `hash_mismatch`, `prev_mismatch` or `seq_gap`; absent when `ok` is true |

On failure the service emits `audit.chain_broken`, the only audit-related event type (README.md, "Event types"; section 10). The verification run itself is audited: a record with `operation` `POST /api/v1/audit/verify`, `result` `ok` or `error`, and the outcome in `result_detail` when it failed.

This matches the M3 Definition of Done: after one entry is edited in the local copy, verification reports that exact entry as broken (docs/product/roadmap.md, M3 Definition of Done; D-0013, D-0031).

### 6.1 Offline verification

`quoro audit verify --file <jsonl>` verifies an exported file with no server, no database and no network:

- the file is JSON Lines, one record per line, in `seq` order (the same format the off-site copy uses, section 8);
- the first record's `prev_hash` is checked against the 64-zero seed unless a starting `prev_hash` is supplied for a segment that does not begin at `seq` 1;
- the same three checks apply: recomputed `hash`, `prev_hash` link, `seq` continuity;
- it exits non-zero and names the failing `seq` and reason when the file is broken.

This is the tool an auditor runs against a downloaded segment without trusting the server that produced it (D-0031).

## 7. Limits, stated honestly

A hash chain is a tamper-*evident* structure, not a tamper-*proof* one. It detects:

- a modified record;
- a reordered record;
- an inserted record in the middle;
- a deleted record in the middle (the following record's `prev_hash` no longer matches).

It does **not** detect:

- **Truncation of the tail.** Deleting the last N records leaves a chain that verifies perfectly, because nothing after them points back. Only a head hash recorded somewhere else catches this.
- **A complete rewrite.** Someone with write access to the database and the code can recompute every hash from `seq` 1 and produce a consistent, valid chain.

The only defence against both is that the head is known outside the server that produced it: the off-site copy (section 8) plus anchoring the head hash somewhere the operator does not control (D-0031). This is precisely why D-0031 asks for a copy out of reach of whoever operates the server, and why the design does not claim that the local chain alone is enough.

Two further honest limits:

- The chain proves internal consistency, not that the recorded parameters and results are true; a lying service writes a consistent lie.
- It proves nothing about calls that never reached the audit gate. Authentication failures are not recorded (section 3), so a burst of failed logins is not visible in the chain.

## 8. Off-site copy

The off-site copy ships in M5a with the S3-compatible path (D-0053), gated by an object-lock probe per provider. Object-lock support per provider is unverified (Hypothesis) until the research task before M5a and the probe.

### 8.1 Segments

- Records are exported as **canonical JSON Lines**: one audit record per line, serialized with RFC 8785, in `seq` order, redacted as stored (section 4).
- A segment is closed every **1,000 records or 5 minutes**, whichever comes first.
- The object key is:

  ```
  <prefix>/<cluster_id>/audit/<first_seq 20-digit zero-padded>-<last_seq 20-digit>.jsonl
  ```

- Alongside each segment, a small head object is written:

  ```
  <prefix>/<cluster_id>/audit/head-<last_seq 20-digit>.json
  ```

  containing `{ "last_seq": ..., "head_hash": "...", "time": "..." }`.

- Each object is written **once**. Segment keys are unique (the seq range is in the key), so an export never overwrites a previous segment.
- Each object is written with an **object-lock retention in compliance mode**, so the store itself refuses deletion or overwrite until the retention passes (D-0031).
- The exporter's credentials are limited to put: no delete, no overwrite of a locked object. A credential that can delete defeats the purpose of the copy (D-0031).
- Progress is recorded in `audit_exports` (`target`, `last_exported_seq`, `object_lock`, `updated_at`), so a restarted exporter resumes after the last closed segment (shared entity model; [db-schema.sql](db-schema.sql)).

### 8.2 Provider caveats

- **Object lock and conditional writes are not portable across S3-compatible providers** (F-0038; `docs/research/r2-conditional-writes.md`). The provider adapter probes the capability at setup, and **refuses to report the copy as "locked" if the probe fails**: `audit_exports.object_lock` stays false and the panel says so, rather than promising a lock the store does not enforce.
- Whether a given provider implements object lock in the form this contract needs is **Hypothesis**, not covered by `docs/research/`; F-0038 covers conditional writes only, and object-lock support per provider must be probed the same way.
- **R2 limits concurrent writes to the same key to one per second** (F-0037). Segment keys are unique and each object is written once, so the limit does not bind this design. The head object has one key per `last_seq`, also unique, so it does not bind either.
- A failed put is retried; a segment is only recorded as exported in `audit_exports` after the put is acknowledged. After a crash between put and bookkeeping, the exporter lists the keys after `last_exported_seq` before cutting a new segment and adopts an existing segment (and its head object) whose content verifies, instead of writing again: a locked object cannot be overwritten, and a re-cut range would overlap.

### 8.3 What the copy does and does not solve

It solves tail truncation and full rewrite *on the server*, because the segments and the head hashes live somewhere the operator of the server cannot rewrite (D-0031). It does not solve a compromised provider account, and it does not make the local log immutable. The honest statement is: the local chain plus an out-of-reach copy make silent rewriting detectable by anyone who compares the two heads (section 7).

## 9. Retention

- **Audit records are never deleted in M0-M6.** There is no retention job, no vacuum of `audit_log` and no export-then-prune step (D-0031).
- A retention policy would be a decision of its own, and this contract does not anticipate its shape. It would have to reconcile "append-only" with deletion and say what happens to the chain when a middle segment is removed (D-0031, D-0036).
- Growth is bounded in practice by the fact that only mutating calls, agent reads and system actions are recorded, not human reads (section 3). That is an expectation, not a limit this contract fixes.

## 10. Relationship to events

- **Audit records are not events.** They are not in `events`, they are not delivered by webhooks and they do not appear in the event stream. A consumer that wants to react to agent activity uses the audit API with `audit:read` (D-0031; [events.schema.json](events.schema.json)).
- **`audit.chain_broken` is the only audit event type.** It is emitted when verification fails and is the one way the audit log reaches the event stream (README.md, "Event types").
- A mutating call may produce both an audit record and a domain event, for example a restart that produces an `app.restarted` event and an audit record with `change_seq`. They are different records for different consumers, and neither replaces the other (D-0031, D-0032).
